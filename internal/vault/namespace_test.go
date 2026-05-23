// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package vault

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcelocantos/mnemo/internal/store"
	"github.com/marcelocantos/mnemo/internal/storetest"
)

// TestSyncWritesMnemoNamespace verifies that a fresh sync creates
// <vault>/_mnemo/index.md and README.md with the standard fence
// contract (🎯T64.2 acceptance: "_mnemo/index.md and _mnemo/README.md
// exist with the standard fence contract").
func TestSyncWritesMnemoNamespace(t *testing.T) {
	vaultDir, exp := newVaultForTest(t)
	exp.SetLayoutResolver(func() string { return store.VaultLayoutV2 })

	if err := exp.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	for _, name := range []string{"index.md", "README.md"} {
		path := filepath.Join(vaultDir, mnemoNamespaceDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
		if !strings.Contains(string(data), generatedFence) {
			t.Errorf("%s lacks generated fence", name)
		}
	}
}

// TestMigrationDocWriteOnce verifies the MIGRATION.md write-once
// contract: created on v1-detection, never regenerated even after
// deletion (🎯T64.2 acceptance: "MIGRATION.md is written once...
// deletion is respected").
func TestMigrationDocWriteOnce(t *testing.T) {
	vaultDir, exp := newVaultForTest(t)
	exp.SetLayoutResolver(func() string { return store.VaultLayoutBoth })

	// Simulate a v1-populated vault by creating one of the v1 marker
	// dirs before the first sync.
	if err := os.MkdirAll(filepath.Join(vaultDir, "sessions"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := exp.Sync(context.Background()); err != nil {
		t.Fatalf("Sync 1: %v", err)
	}
	migPath := filepath.Join(vaultDir, mnemoNamespaceDir, migrationDocName)
	first, err := os.ReadFile(migPath)
	if err != nil {
		t.Fatalf("MIGRATION.md missing after first sync: %v", err)
	}
	if !strings.Contains(string(first), "v1 → v2 layout migration") {
		t.Errorf("MIGRATION.md body unexpected: %q", first)
	}

	// Bump mtime then sync again; the doc must not be re-rendered.
	if err := os.Chtimes(migPath, time.Now().Add(-time.Hour), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	stat1, _ := os.Stat(migPath)

	if err := exp.Sync(context.Background()); err != nil {
		t.Fatalf("Sync 2: %v", err)
	}
	stat2, _ := os.Stat(migPath)
	if !stat1.ModTime().Equal(stat2.ModTime()) {
		t.Errorf("MIGRATION.md re-written on second sync; mtime moved %v → %v",
			stat1.ModTime(), stat2.ModTime())
	}

	// Delete and resync — must not be recreated.
	if err := os.Remove(migPath); err != nil {
		t.Fatal(err)
	}
	if err := exp.Sync(context.Background()); err != nil {
		t.Fatalf("Sync 3: %v", err)
	}
	if _, err := os.Stat(migPath); !os.IsNotExist(err) {
		t.Errorf("MIGRATION.md recreated after deletion; stat: %v", err)
	}
}

// TestMigrationDocOnDemand verifies that WriteMigrationDoc
// (mnemo_vault_migration_doc write: true) recreates the file after
// deletion — the user-initiated escape hatch.
func TestMigrationDocOnDemand(t *testing.T) {
	vaultDir, exp := newVaultForTest(t)
	exp.SetLayoutResolver(func() string { return store.VaultLayoutBoth })

	if err := os.MkdirAll(filepath.Join(vaultDir, "decisions"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := exp.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	migPath := filepath.Join(vaultDir, mnemoNamespaceDir, migrationDocName)
	if err := os.Remove(migPath); err != nil {
		t.Fatal(err)
	}

	got, err := exp.WriteMigrationDoc()
	if err != nil {
		t.Fatalf("WriteMigrationDoc: %v", err)
	}
	if got != migPath {
		t.Errorf("returned path %q, want %q", got, migPath)
	}
	if _, err := os.Stat(migPath); err != nil {
		t.Errorf("MIGRATION.md not present after WriteMigrationDoc: %v", err)
	}

	snap := exp.MigrationDocSnapshot()
	if !strings.Contains(snap, "decisions") {
		t.Errorf("snapshot missing detected v1 dir: %q", snap)
	}
}

// TestLayoutGating verifies the v2/both/v1 layout gates the right
// writers (🎯T64.2 + design's "what each mode writes" rule).
func TestLayoutGating(t *testing.T) {
	cases := []struct {
		layout      string
		wantMnemo   bool
		wantV1Root  bool // sessions/ or repos/ should exist
		wantV1Index bool // root index.md
	}{
		{store.VaultLayoutV2, true, false, false},
		{store.VaultLayoutBoth, true, true, true},
		{store.VaultLayoutV1, false, true, true},
	}
	for _, c := range cases {
		t.Run(c.layout, func(t *testing.T) {
			vaultDir, exp := newVaultForTest(t)
			exp.SetLayoutResolver(func() string { return c.layout })

			if err := exp.Sync(context.Background()); err != nil {
				t.Fatalf("Sync: %v", err)
			}
			_, errMnemo := os.Stat(filepath.Join(vaultDir, mnemoNamespaceDir, "index.md"))
			gotMnemo := errMnemo == nil
			if gotMnemo != c.wantMnemo {
				t.Errorf("_mnemo/index.md present=%v, want %v (err: %v)", gotMnemo, c.wantMnemo, errMnemo)
			}

			_, errIdx := os.Stat(filepath.Join(vaultDir, "index.md"))
			gotV1Index := errIdx == nil
			if gotV1Index != c.wantV1Index {
				t.Errorf("root index.md present=%v, want %v", gotV1Index, c.wantV1Index)
			}
		})
	}
}

// TestSoakWarningEmitsThenRateLimits verifies the soak-window
// warning fires once after the threshold and then respects the
// weekly cadence (🎯T64.2 acceptance: "emits a weekly structured
// warning; the layout never auto-narrows").
func TestSoakWarningEmitsThenRateLimits(t *testing.T) {
	// Use a tight soak window so the test does not depend on the
	// 720h default. State is isolated by TestMain.
	dir, _ := os.MkdirTemp("", "mnemo-soak-state-")
	defer os.RemoveAll(dir)
	restore := store.SetStateDirForTesting(dir)
	defer restore()

	vaultDir, exp := newVaultForTest(t)
	exp.SetLayoutResolver(func() string { return store.VaultLayoutBoth })
	// 1ns soak window → already tripped from first observation.
	exp.SetSoakWarnAfterResolver(func() time.Duration { return time.Nanosecond })

	// Seed state with vault_path and a first-seen time so the
	// computed hours_in_both is non-zero.
	seed := store.State{
		Version:   1,
		VaultPath: vaultDir,
		VaultLayoutFirstSeen: map[string]*time.Time{
			store.VaultLayoutBoth: ptrTime(time.Now().Add(-time.Hour).UTC()),
		},
	}
	if err := store.WriteState(seed); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	// First sync — warning should fire, last-warn timestamp recorded.
	if err := exp.Sync(context.Background()); err != nil {
		t.Fatalf("Sync 1: %v", err)
	}
	s1, _ := store.LoadState()
	if s1.VaultLayoutLastSoakWarn == nil {
		t.Fatal("expected VaultLayoutLastSoakWarn set after threshold-tripped sync")
	}

	// Second sync immediately after — last-warn timestamp must not
	// advance (weekly cadence not yet elapsed).
	firstWarn := *s1.VaultLayoutLastSoakWarn
	if err := exp.Sync(context.Background()); err != nil {
		t.Fatalf("Sync 2: %v", err)
	}
	s2, _ := store.LoadState()
	if s2.VaultLayoutLastSoakWarn == nil || !s2.VaultLayoutLastSoakWarn.Equal(firstWarn) {
		t.Errorf("warn timestamp advanced within cadence; before=%v after=%v",
			firstWarn, s2.VaultLayoutLastSoakWarn)
	}
}

// TestVaultPathChangeResetsCounters verifies the "vault_path change
// semantics" rule: when state.json records a different vault path
// from the active one, the soak counters are reset.
func TestVaultPathChangeResetsCounters(t *testing.T) {
	dir, _ := os.MkdirTemp("", "mnemo-reset-state-")
	defer os.RemoveAll(dir)
	restore := store.SetStateDirForTesting(dir)
	defer restore()

	// Pre-seed state.json with a different vault path and a recorded
	// first-seen time.
	oldFirstSeen := time.Now().Add(-24 * time.Hour).UTC()
	seed := store.State{
		Version:   1,
		VaultPath: "/some/other/vault",
		VaultLayoutFirstSeen: map[string]*time.Time{
			store.VaultLayoutBoth: ptrTime(oldFirstSeen),
		},
	}
	if err := store.WriteState(seed); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	vaultDir, exp := newVaultForTest(t)
	exp.SetLayoutResolver(func() string { return store.VaultLayoutBoth })
	if err := exp.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	s, _ := store.LoadState()
	if s.VaultPath != vaultDir {
		t.Errorf("VaultPath: got %q, want %q", s.VaultPath, vaultDir)
	}
	got := s.LayoutFirstSeen(store.VaultLayoutBoth)
	if got == nil {
		t.Fatal("first-seen entry should have been re-recorded for new vault")
	}
	if got.Equal(oldFirstSeen) {
		t.Errorf("first-seen entry was not reset; still %v from previous vault", oldFirstSeen)
	}
}

// newVaultForTest builds an Exporter against a fresh temp store and
// vault dir. No layout resolver is set — callers configure their own.
func newVaultForTest(t *testing.T) (string, *Exporter) {
	t.Helper()
	projDir := t.TempDir()
	storetest.WriteJSONL(t, projDir, "-Users-x-dev-app", "sess-ns-01", []map[string]any{
		storetest.MetaMsg("user", "hi", "2026-05-10T10:00:00Z", "/Users/x/dev/app", "main"),
		storetest.Msg("assistant", "hi back", "2026-05-10T10:00:01Z"),
	})
	s := storetest.NewStore(t, projDir)
	if err := s.IngestAll(); err != nil {
		t.Fatal(err)
	}
	vaultDir := t.TempDir()
	exp, err := New(s, vaultDir)
	if err != nil {
		t.Fatal(err)
	}
	return vaultDir, exp
}

func ptrTime(t time.Time) *time.Time { return &t }
