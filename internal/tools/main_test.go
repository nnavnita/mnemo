// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package tools

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/marcelocantos/mnemo/internal/store"
)

// TestMain isolates state.json (🎯T64.2) into a per-process temp
// directory so tests exercising mnemo_vault_status do not read or
// write the daemon user's real ~/.mnemo/state.json.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mnemo-tools-state-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	sub := filepath.Join(dir, ".mnemo")
	_ = os.MkdirAll(sub, 0o755)
	restore := store.SetStateDirForTesting(sub)
	defer restore()

	os.Exit(m.Run())
}
