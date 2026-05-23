// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// State is the daemon-managed sidecar that holds runtime-derived state
// that should not pollute the user-editable ~/.mnemo/config.json but
// must survive daemon restarts (🎯T64.2).
//
// Schema is additive — new fields appear over time, old ones never
// disappear silently. UnknownKeys preserves any top-level keys the
// current daemon does not recognise so a newer daemon's additions
// survive a downgrade-then-upgrade cycle.
//
// File location: ~/.mnemo/state.json. Owned exclusively by the daemon;
// users edit ~/.mnemo/config.json, never this file.
type State struct {
	// Version governs schema-shape changes that go beyond
	// additive-field. Current version: 1.
	Version int `json:"version"`

	// VaultPath records the path the most recent sync wrote against.
	// Used by the "vault_path change semantics" rule to detect a path
	// change and reset the soak-TTL counters.
	VaultPath string `json:"vault_path,omitempty"`

	// VaultLayoutFirstSeen records the wall-clock time the daemon first
	// observed each layout mode active for the current vault. Keys are
	// the layout constants (v1/both/v2); a nil pointer means "not yet
	// observed". hours_in_<mode> for the recommendation state machine
	// is derived from the relevant entry.
	VaultLayoutFirstSeen map[string]*time.Time `json:"vault_layout_first_seen,omitempty"`

	// VaultLayoutLastSoakWarn records the most recent time a
	// "vault_layout=both past soak window" warning was emitted, so the
	// daemon can rate-limit the warning to a weekly cadence after the
	// initial trip. Nil pointer means "never warned".
	VaultLayoutLastSoakWarn *time.Time `json:"vault_layout_last_soak_warn,omitempty"`

	// IndexingScopeFirstSeen mirrors VaultLayoutFirstSeen for the
	// indexing-scope axis. Keys: "_mnemo_only", "full", "includes".
	IndexingScopeFirstSeen map[string]*time.Time `json:"indexing_scope_first_seen,omitempty"`

	// VaultMigrationDocWritten records that mnemo has, at some point
	// in this vault's history, written _mnemo/MIGRATION.md. The
	// write-once contract uses this to honour deletion as "I have
	// read this; move on" — if the file is gone but this flag is
	// true, the sync path will not recreate it. The user-initiated
	// WriteMigrationDoc (mnemo_vault_migration_doc write:true) sets
	// this flag too so subsequent syncs continue to respect it.
	VaultMigrationDocWritten bool `json:"vault_migration_doc_written,omitempty"`

	// UnknownKeys captures any top-level JSON keys the current daemon
	// does not recognise. Re-emitted verbatim on write so a future
	// daemon's additions survive a roundtrip through this binary.
	UnknownKeys map[string]json.RawMessage `json:"-"`
}

// CurrentStateVersion is the schema version this binary writes. A
// state.json with a higher version triggers read-only state mode.
const CurrentStateVersion = 1

// knownStateKeys is the closed set of top-level JSON keys the current
// daemon writes. Any incoming key not on this list is captured into
// State.UnknownKeys and re-emitted unchanged on the next write.
var knownStateKeys = map[string]struct{}{
	"version":                      {},
	"vault_path":                   {},
	"vault_layout_first_seen":      {},
	"vault_layout_last_soak_warn":  {},
	"indexing_scope_first_seen":    {},
	"vault_migration_doc_written":  {},
}

// ErrStateVersionAhead signals that the on-disk state.json was written
// by a newer daemon than this binary. The caller should run in
// read-only state mode: counters report against recorded data but no
// new state writes happen for this boot. Restoring this invariant
// (i.e. discarding fields the older binary cannot represent) requires
// an explicit user action.
var ErrStateVersionAhead = errors.New("state.json version is ahead of this daemon")

// stateDirOverride is set by SetStateDirForTesting to redirect
// state.json reads/writes into a per-test temp directory. The empty
// string (zero value) means "resolve via os.UserHomeDir as usual".
var stateDirOverride string

// SetStateDirForTesting redirects state.json read/write into dir
// (typically a t.TempDir() value). Returns a restore function the
// caller defers to undo the override. Must not be used concurrently
// with itself.
func SetStateDirForTesting(dir string) func() {
	prev := stateDirOverride
	stateDirOverride = dir
	return func() { stateDirOverride = prev }
}

// StatePath returns the absolute path to ~/.mnemo/state.json (or
// <override>/state.json when SetStateDirForTesting is active).
func StatePath() (string, error) {
	if stateDirOverride != "" {
		return filepath.Join(stateDirOverride, "state.json"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".mnemo", "state.json"), nil
}

// LoadState reads ~/.mnemo/state.json. Returns a fresh
// State{Version: CurrentStateVersion} when the file does not exist
// (first daemon boot). Returns ErrStateVersionAhead alongside the
// partially-populated State when the recorded version is higher than
// CurrentStateVersion so the caller can flip into read-only state mode
// instead of overwriting unknown fields.
//
// Sweeps any leftover .state.json.tmp from a prior crashed write before
// returning so subsequent atomic writes have a clean tmp slot.
func LoadState() (State, error) {
	path, err := StatePath()
	if err != nil {
		return State{Version: CurrentStateVersion}, err
	}
	return loadStateFrom(path)
}

func loadStateFrom(path string) (State, error) {
	// Sweep any stale tempfile from a previous crashed write before
	// reading the canonical file. The tmp lives next to the target
	// with a leading dot, matching the in-vault tmp convention.
	_ = os.Remove(stateTmpPath(path))

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return State{Version: CurrentStateVersion}, nil
		}
		return State{Version: CurrentStateVersion}, err
	}

	// Decode twice: once into the typed State, once into a raw map so
	// unknown keys are preserved verbatim on the next write.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return State{Version: CurrentStateVersion}, fmt.Errorf("parse state.json: %w", err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return State{Version: CurrentStateVersion}, fmt.Errorf("decode state.json: %w", err)
	}

	// Capture unknown top-level keys for round-trip preservation.
	for k, v := range raw {
		if _, known := knownStateKeys[k]; known {
			continue
		}
		if s.UnknownKeys == nil {
			s.UnknownKeys = make(map[string]json.RawMessage)
		}
		s.UnknownKeys[k] = v
	}

	if s.Version > CurrentStateVersion {
		return s, ErrStateVersionAhead
	}
	if s.Version == 0 {
		s.Version = CurrentStateVersion
	}
	return s, nil
}

// WriteState persists s to ~/.mnemo/state.json atomically (sibling tmp
// + fsync + rename). The parent directory is created if missing.
//
// Returns ErrStateVersionAhead without touching the file if s.Version
// is higher than CurrentStateVersion. This keeps the older binary from
// silently dropping fields it does not understand.
func WriteState(s State) error {
	path, err := StatePath()
	if err != nil {
		return err
	}
	return writeStateTo(path, s)
}

func writeStateTo(path string, s State) error {
	if s.Version > CurrentStateVersion {
		return ErrStateVersionAhead
	}
	if s.Version == 0 {
		s.Version = CurrentStateVersion
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	// Marshal known fields then splice unknown top-level keys back in
	// so a newer daemon's additions survive a roundtrip through this
	// binary.
	body, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	var merged map[string]json.RawMessage
	if err := json.Unmarshal(body, &merged); err != nil {
		return fmt.Errorf("re-decode state for merge: %w", err)
	}
	for k, v := range s.UnknownKeys {
		if _, known := knownStateKeys[k]; known {
			continue
		}
		merged[k] = v
	}
	data, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal merged state: %w", err)
	}
	data = append(data, '\n')

	tmp := stateTmpPath(path)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// stateTmpPath returns the sibling tempfile path used by the atomic
// write protocol. Leading dot matches the in-vault tmp convention so
// PKM tools that skip dotfiles also skip a half-written state file.
func stateTmpPath(path string) string {
	dir, base := filepath.Split(path)
	return filepath.Join(dir, "."+base+".tmp")
}

// LayoutFirstSeen returns the recorded first-seen time for layout
// mode, or nil if the mode has not yet been observed under this state.
func (s State) LayoutFirstSeen(mode string) *time.Time {
	if s.VaultLayoutFirstSeen == nil {
		return nil
	}
	return s.VaultLayoutFirstSeen[mode]
}

// RecordLayoutFirstSeen sets the first-seen time for mode to now if it
// is not already recorded. Returns true when a new entry was written
// (i.e. the caller should persist the updated state).
//
// Existing entries are never overwritten so a daemon restart does not
// reset the soak-TTL counter.
func (s *State) RecordLayoutFirstSeen(mode string, now time.Time) bool {
	if s.VaultLayoutFirstSeen == nil {
		s.VaultLayoutFirstSeen = make(map[string]*time.Time)
	}
	if existing := s.VaultLayoutFirstSeen[mode]; existing != nil {
		return false
	}
	t := now
	s.VaultLayoutFirstSeen[mode] = &t
	return true
}

// ResetLayoutCounters clears all VaultLayoutFirstSeen entries and the
// last-warn timestamp. Called by the "vault_path change semantics"
// rule when the configured vault_path no longer matches the recorded
// State.VaultPath, since soak counters from the previous vault have
// no meaning against the new one.
func (s *State) ResetLayoutCounters() {
	s.VaultLayoutFirstSeen = nil
	s.VaultLayoutLastSoakWarn = nil
	s.VaultMigrationDocWritten = false
}
