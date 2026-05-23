// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package vault

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/marcelocantos/mnemo/internal/store"
)

// TestMain isolates state.json (🎯T64.2) into a per-process temp
// directory so the test suite never reads or writes the daemon user's
// real ~/.mnemo/state.json. Vault Sync calls maintainStateAndWarn,
// which would otherwise touch the user's home.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mnemo-vault-state-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// Mirror the daemon's layout: state.json lives under .mnemo/
	// relative to the override root.
	sub := filepath.Join(dir, ".mnemo")
	_ = os.MkdirAll(sub, 0o755)
	restore := store.SetStateDirForTesting(sub)
	defer restore()

	os.Exit(m.Run())
}
