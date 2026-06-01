// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package tools

import (
	"strings"
	"testing"

	"github.com/marcelocantos/mnemo/internal/store"
)

// fakeCtl is a minimal ConfigController for handler-level tests; the
// real adapter (configController in main.go) talks to a live Registry.
type fakeCtl struct {
	cur    store.Config
	last   store.Config
	report ConfigReport
	err    error
}

func (f *fakeCtl) Get() store.Config { return f.cur }
func (f *fakeCtl) Put(c store.Config) (ConfigReport, error) {
	f.last = c
	if f.err != nil {
		return ConfigReport{}, f.err
	}
	f.cur = c
	return f.report, nil
}

func TestMergeConfigPatchOnlyOverlaysProvidedKeys(t *testing.T) {
	current := store.Config{
		WorkspaceRoots: []string{"/a"},
		VaultPath:      "/old",
	}
	merged, err := mergeConfigPatch(current, map[string]any{
		"vault_path": "/new",
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if merged.VaultPath != "/new" {
		t.Errorf("vault_path: got %q want /new", merged.VaultPath)
	}
	// Untouched key must survive.
	if len(merged.WorkspaceRoots) != 1 || merged.WorkspaceRoots[0] != "/a" {
		t.Errorf("workspace_roots dropped: %v", merged.WorkspaceRoots)
	}
}

func TestMergeConfigPatchClearWithEmptyString(t *testing.T) {
	current := store.Config{VaultPath: "/old"}
	merged, err := mergeConfigPatch(current, map[string]any{
		"vault_path": "",
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if merged.VaultPath != "" {
		t.Errorf("vault_path should be cleared, got %q", merged.VaultPath)
	}
}

func TestMergeConfigPatchRejectsUnknownKeys(t *testing.T) {
	_, err := mergeConfigPatch(store.Config{}, map[string]any{
		"vaultpath":        "/x", // missing underscore
		"random_other_key": "y",
		"workspace_roots":  []any{"/a"}, // valid; should still error overall
	})
	if err == nil {
		t.Fatal("expected error for unknown keys")
	}
	if !strings.Contains(err.Error(), "vaultpath") || !strings.Contains(err.Error(), "random_other_key") {
		t.Errorf("error should mention both typos, got: %v", err)
	}
}

func TestConfigToolReadReturnsJSON(t *testing.T) {
	ctl := &fakeCtl{cur: store.Config{VaultPath: "/v"}}
	ch := &callHandler{}
	out, isErr, err := ch.config(map[string]any{"op": "read"}, ctl)
	if err != nil || isErr {
		t.Fatalf("read: isErr=%v err=%v", isErr, err)
	}
	if !strings.Contains(out, `"vault_path": "/v"`) {
		t.Errorf("expected vault_path in output, got: %s", out)
	}
}

func TestConfigToolWriteAppliesPatch(t *testing.T) {
	ctl := &fakeCtl{
		cur: store.Config{
			WorkspaceRoots: []string{"/a"},
			VaultPath:      "/old",
		},
		report: ConfigReport{
			Changed: []string{"vault_path"},
			Adopted: []string{"vault_path"},
		},
	}
	ch := &callHandler{}
	out, isErr, err := ch.config(map[string]any{
		"op":    "write",
		"patch": map[string]any{"vault_path": "/new"},
	}, ctl)
	if err != nil || isErr {
		t.Fatalf("write: isErr=%v err=%v out=%s", isErr, err, out)
	}
	if ctl.last.VaultPath != "/new" {
		t.Errorf("controller saw vault_path %q want /new", ctl.last.VaultPath)
	}
	if len(ctl.last.WorkspaceRoots) != 1 || ctl.last.WorkspaceRoots[0] != "/a" {
		t.Errorf("workspace_roots clobbered: %v", ctl.last.WorkspaceRoots)
	}
	if !strings.Contains(out, "Adopted live:") || !strings.Contains(out, "vault_path") {
		t.Errorf("missing adopted line: %s", out)
	}
}

func TestConfigToolWriteRequiresPatch(t *testing.T) {
	ctl := &fakeCtl{}
	ch := &callHandler{}
	_, isErr, err := ch.config(map[string]any{"op": "write"}, ctl)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !isErr {
		t.Errorf("expected tool-level error for missing patch")
	}
}

func TestConfigToolUnknownOp(t *testing.T) {
	ctl := &fakeCtl{}
	ch := &callHandler{}
	_, isErr, err := ch.config(map[string]any{"op": "drop"}, ctl)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !isErr {
		t.Errorf("expected error for unknown op")
	}
}

func TestConfigToolNoControllerAvailable(t *testing.T) {
	ch := &callHandler{}
	_, isErr, err := ch.config(map[string]any{}, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !isErr {
		t.Errorf("expected error when controller not wired")
	}
}

// TestConfigToolWriteRejectsMalformedSoakWarnAfter ensures the
// mnemo_config write path surfaces a malformed vault_layout
// configuration synchronously instead of letting it fall through to
// the resolver, where it would have silently degraded to the package
// default. The user sees the error at write time, not as a vanished
// override on next sync.
func TestConfigToolWriteRejectsMalformedSoakWarnAfter(t *testing.T) {
	ctl := &fakeCtl{cur: store.Config{}}
	ch := &callHandler{}
	out, isErr, err := ch.config(map[string]any{
		"op": "write",
		"patch": map[string]any{
			"vault_layout": map[string]any{
				"mode":            "both",
				"soak_warn_after": "thirty days",
			},
		},
	}, ctl)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !isErr {
		t.Errorf("expected tool-level error for malformed soak_warn_after, got: %s", out)
	}
	if !strings.Contains(out, "soak_warn_after") {
		t.Errorf("error should mention soak_warn_after, got: %s", out)
	}
	if ctl.last.VaultLayout.SoakWarnAfter != "" {
		t.Errorf("controller should not have received the bad config; got %q", ctl.last.VaultLayout.SoakWarnAfter)
	}
}
