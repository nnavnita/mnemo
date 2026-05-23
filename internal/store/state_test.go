// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadState_Missing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	s, err := loadStateFrom(path)
	if err != nil {
		t.Fatalf("loadStateFrom missing file: %v", err)
	}
	if s.Version != CurrentStateVersion {
		t.Errorf("missing-file should produce CurrentStateVersion, got %d", s.Version)
	}
	if s.VaultLayoutFirstSeen != nil {
		t.Errorf("VaultLayoutFirstSeen should be nil, got %+v", s.VaultLayoutFirstSeen)
	}
}

func TestWriteState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	now := time.Date(2026, 5, 24, 14, 0, 0, 0, time.UTC)
	in := State{
		Version:   1,
		VaultPath: "/home/u/vault",
	}
	if !in.RecordLayoutFirstSeen("both", now) {
		t.Fatal("RecordLayoutFirstSeen should report true on first record")
	}
	if in.RecordLayoutFirstSeen("both", now.Add(time.Hour)) {
		t.Fatal("RecordLayoutFirstSeen should not overwrite existing entry")
	}

	if err := writeStateTo(path, in); err != nil {
		t.Fatalf("writeStateTo: %v", err)
	}

	out, err := loadStateFrom(path)
	if err != nil {
		t.Fatalf("loadStateFrom after write: %v", err)
	}
	if out.VaultPath != in.VaultPath {
		t.Errorf("VaultPath: got %q, want %q", out.VaultPath, in.VaultPath)
	}
	if got := out.LayoutFirstSeen("both"); got == nil || !got.Equal(now) {
		t.Errorf("LayoutFirstSeen(both): got %v, want %v", got, now)
	}
}

func TestWriteState_PreservesUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	// Pretend a future daemon wrote state.json at the current version
	// with extra top-level keys this daemon does not know about.
	raw := []byte(`{
  "version": 1,
  "vault_path": "/v",
  "future_key": {"nested": true},
  "another_extra": "preserved"
}`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := loadStateFrom(path)
	if err != nil {
		t.Fatalf("loadStateFrom: %v", err)
	}
	if _, ok := s.UnknownKeys["future_key"]; !ok {
		t.Errorf("future_key not captured; got %+v", s.UnknownKeys)
	}

	// Write back; unknown keys should still be present in the file.
	if err := writeStateTo(path, s); err != nil {
		t.Fatalf("writeStateTo: %v", err)
	}
	data, _ := os.ReadFile(path)
	var rt map[string]json.RawMessage
	if err := json.Unmarshal(data, &rt); err != nil {
		t.Fatalf("re-decode: %v", err)
	}
	if _, ok := rt["future_key"]; !ok {
		t.Errorf("future_key dropped on write; got %s", data)
	}
	if _, ok := rt["another_extra"]; !ok {
		t.Errorf("another_extra dropped on write; got %s", data)
	}
}

func TestLoadState_VersionAhead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	raw := []byte(`{"version": 99}`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := loadStateFrom(path)
	if !errors.Is(err, ErrStateVersionAhead) {
		t.Errorf("expected ErrStateVersionAhead, got %v", err)
	}
}

func TestWriteState_RefusesFutureVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	err := writeStateTo(path, State{Version: 99})
	if !errors.Is(err, ErrStateVersionAhead) {
		t.Errorf("expected ErrStateVersionAhead, got %v", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("file should not exist after refused write; stat: %v", statErr)
	}
}

func TestWriteState_SweepsStaleTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	tmp := stateTmpPath(path)
	if err := os.WriteFile(tmp, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	// loadStateFrom should sweep the stale tmp before returning.
	if _, err := loadStateFrom(path); err != nil {
		t.Fatalf("loadStateFrom: %v", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Errorf("stale tmp not swept; stat: %v", err)
	}
}

func TestResetLayoutCounters(t *testing.T) {
	now := time.Now().UTC()
	s := State{Version: 1}
	s.RecordLayoutFirstSeen("both", now)
	warn := now.Add(time.Hour)
	s.VaultLayoutLastSoakWarn = &warn

	s.ResetLayoutCounters()
	if s.VaultLayoutFirstSeen != nil {
		t.Errorf("VaultLayoutFirstSeen not cleared: %+v", s.VaultLayoutFirstSeen)
	}
	if s.VaultLayoutLastSoakWarn != nil {
		t.Errorf("VaultLayoutLastSoakWarn not cleared: %v", s.VaultLayoutLastSoakWarn)
	}
}
