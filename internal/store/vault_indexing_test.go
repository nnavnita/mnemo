// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResolvedVaultIndexingScopeExplicit(t *testing.T) {
	cases := []string{
		VaultIndexingScopeMnemoOnly,
		VaultIndexingScopeFull,
		VaultIndexingScopeIncludes,
	}
	for _, want := range cases {
		t.Run(want, func(t *testing.T) {
			cfg := Config{VaultIndexingScope: want}
			if got := cfg.ResolvedVaultIndexingScope(t.TempDir()); got != want {
				t.Errorf("explicit scope %q lost in resolution: got %q", want, got)
			}
		})
	}
}

func TestResolvedVaultIndexingScopeDefaultsNewVault(t *testing.T) {
	vault := t.TempDir() // empty
	cfg := Config{}
	if got := cfg.ResolvedVaultIndexingScope(vault); got != VaultIndexingScopeMnemoOnly {
		t.Errorf("empty vault should default to %q, got %q",
			VaultIndexingScopeMnemoOnly, got)
	}
}

func TestResolvedVaultIndexingScopeDefaultsV1Vault(t *testing.T) {
	vault := t.TempDir()
	// Plant a v1 marker dir; _mnemo/ absent → "full" for continuity.
	if err := os.MkdirAll(filepath.Join(vault, "sessions"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cfg := Config{}
	if got := cfg.ResolvedVaultIndexingScope(vault); got != VaultIndexingScopeFull {
		t.Errorf("v1 vault should default to %q, got %q",
			VaultIndexingScopeFull, got)
	}
}

func TestResolvedVaultIndexingScopeDefaultsV2Vault(t *testing.T) {
	vault := t.TempDir()
	// _mnemo/ present (with or without v1 dirs) → "_mnemo_only".
	if err := os.MkdirAll(filepath.Join(vault, "_mnemo"), 0o755); err != nil {
		t.Fatalf("seed _mnemo: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(vault, "sessions"), 0o755); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}
	cfg := Config{}
	if got := cfg.ResolvedVaultIndexingScope(vault); got != VaultIndexingScopeMnemoOnly {
		t.Errorf("v2 vault (_mnemo/ present) should default to %q, got %q",
			VaultIndexingScopeMnemoOnly, got)
	}
}

func TestResolvedVaultIndexingIgnoreFileDefault(t *testing.T) {
	if got := (Config{}).ResolvedVaultIndexingIgnoreFile(); got != defaultVaultIgnoreFile {
		t.Errorf("default ignore file: got %q want %q", got, defaultVaultIgnoreFile)
	}
	if got := (Config{VaultIndexingIgnoreFile: "custom.ignore"}).ResolvedVaultIndexingIgnoreFile(); got != "custom.ignore" {
		t.Errorf("custom ignore file: got %q", got)
	}
}

func TestResolveVaultIndexingRoots(t *testing.T) {
	// Use a host-absolute vault root and assemble expected paths via
	// filepath.Join so the test runs identically on POSIX and Windows
	// (filepath.Join emits "\" on Windows).
	vault := filepath.Join(t.TempDir(), "vault")
	// A host-absolute path the includes-rejects test can pass —
	// "/etc/passwd" would not trip filepath.IsAbs on Windows, which
	// requires either a drive letter or a UNC path.
	absInclude := filepath.Join(t.TempDir(), "absolute")
	cases := []struct {
		name     string
		scope    string
		includes []string
		want     []string
		wantErr  bool
	}{
		{"mnemo only", VaultIndexingScopeMnemoOnly, nil, []string{filepath.Join(vault, "_mnemo")}, false},
		{"empty scope defaults", "", nil, []string{filepath.Join(vault, "_mnemo")}, false},
		{"full", VaultIndexingScopeFull, nil, []string{vault}, false},
		{
			"includes",
			VaultIndexingScopeIncludes,
			[]string{"areas/knowledge", "projects"},
			[]string{
				filepath.Join(vault, "_mnemo"),
				filepath.Join(vault, "areas", "knowledge"),
				filepath.Join(vault, "projects"),
			},
			false,
		},
		{
			"includes rejects absolute paths",
			VaultIndexingScopeIncludes,
			[]string{absInclude},
			nil,
			true,
		},
		{
			"includes rejects escape",
			VaultIndexingScopeIncludes,
			[]string{"../outside"},
			nil,
			true,
		},
		{
			"unknown scope errors",
			"nonsense",
			nil,
			nil,
			true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := resolveVaultIndexingRoots(vault, c.scope, c.includes)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if err != nil {
				return
			}
			if len(got) != len(c.want) {
				t.Fatalf("roots len: got %v want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("roots[%d]: got %q want %q", i, got[i], c.want[i])
				}
			}
		})
	}
}

// 🎯T64.2 — vault_layout resolution defaults and explicit override.
func TestResolvedVaultLayoutExplicit(t *testing.T) {
	for _, want := range []string{VaultLayoutV2, VaultLayoutBoth, VaultLayoutV1} {
		t.Run(want, func(t *testing.T) {
			cfg := Config{VaultLayout: VaultLayoutConfig{Mode: want}}
			if got := cfg.ResolvedVaultLayout(t.TempDir()); got != want {
				t.Errorf("explicit layout %q lost: got %q", want, got)
			}
		})
	}
}

func TestResolvedVaultLayoutDefaultsNewVault(t *testing.T) {
	cfg := Config{}
	if got := cfg.ResolvedVaultLayout(t.TempDir()); got != VaultLayoutV2 {
		t.Errorf("empty vault should default to %q, got %q", VaultLayoutV2, got)
	}
}

func TestResolvedVaultLayoutDefaultsV1Vault(t *testing.T) {
	vault := t.TempDir()
	if err := os.MkdirAll(filepath.Join(vault, "sessions"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := Config{}
	if got := cfg.ResolvedVaultLayout(vault); got != VaultLayoutBoth {
		t.Errorf("v1 vault should default to %q for migration continuity, got %q",
			VaultLayoutBoth, got)
	}
}

func TestVaultLayoutEffectiveSoakWarnAfter(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"default", "", defaultVaultLayoutSoakWarnAfter, false},
		{"valid", "168h", 168 * time.Hour, false},
		{"malformed", "thirty days", 0, true},
		{"negative", "-1h", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lc := VaultLayoutConfig{SoakWarnAfter: c.in}
			got, err := lc.EffectiveSoakWarnAfter()
			if c.wantErr {
				if err == nil {
					t.Errorf("expected error for %q, got %v", c.in, got)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}
