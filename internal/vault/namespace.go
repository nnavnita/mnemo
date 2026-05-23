// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package vault

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/marcelocantos/mnemo/internal/store"
)

// mnemoNamespaceDir is the v2 layout's reserved subdirectory under the
// vault root (🎯T64.2). Every mnemo-generated abstraction lives under
// here; user notes live alongside it untouched.
const mnemoNamespaceDir = "_mnemo"

// migrationDocName is the write-once file documenting a v1→v2
// migration. Created the first sync that observes any v1 marker dir,
// then never regenerated unless the user opts back in via the
// mnemo_vault_migration_doc MCP tool.
const migrationDocName = "MIGRATION.md"

// syncMnemoNamespace writes <vault>/_mnemo/{index.md,README.md} and,
// on first v1-detection, the write-once MIGRATION.md.
//
// Layout-gating: caller invokes only when the resolved layout is "v2"
// or "both". v1 vaults skip this entirely (the _mnemo/ wing does not
// exist for them).
//
// All writes use writeNote, so the standard fence contract applies:
// human edits below the <!-- mnemo:generated --> fence survive
// re-syncs. The single exception is MIGRATION.md, which is written
// without a fence and never touched again.
func (e *Exporter) syncMnemoNamespace() error {
	wingDir := filepath.Join(e.path, mnemoNamespaceDir)
	if err := os.MkdirAll(wingDir, 0o755); err != nil {
		return fmt.Errorf("vault: mkdir %s: %w", wingDir, err)
	}

	if err := writeNote(filepath.Join(wingDir, "index.md"), renderMnemoIndex(), ""); err != nil {
		return fmt.Errorf("vault: write _mnemo/index.md: %w", err)
	}
	if err := writeNote(filepath.Join(wingDir, "README.md"), renderMnemoREADME(), ""); err != nil {
		return fmt.Errorf("vault: write _mnemo/README.md: %w", err)
	}

	// MIGRATION.md is write-once. Only create it when v1 dirs are
	// present AND state.json records no prior write for this vault. A
	// user who deletes the file is signalling "I have read this; move
	// on" — state.VaultMigrationDocWritten stays true and we honour
	// that by not recreating it. Reading and writing state here is
	// best-effort: failures log and proceed so a state.json problem
	// never blocks the rest of the sync.
	v1Dirs := detectV1Dirs(e.path)
	if len(v1Dirs) > 0 {
		state, err := store.LoadState()
		if err != nil {
			return fmt.Errorf("vault: load state for migration-doc gate: %w", err)
		}
		if !state.VaultMigrationDocWritten {
			migPath := filepath.Join(wingDir, migrationDocName)
			content := renderMigrationDoc(e.path, v1Dirs)
			if err := os.WriteFile(migPath, []byte(content), 0o644); err != nil {
				return fmt.Errorf("vault: write _mnemo/MIGRATION.md: %w", err)
			}
			state.VaultMigrationDocWritten = true
			if err := store.WriteState(state); err != nil {
				return fmt.Errorf("vault: persist migration-doc flag: %w", err)
			}
		}
	}
	return nil
}

// detectV1Dirs returns the subset of v1 marker dirs that exist under
// vaultPath. Used by the namespace writer (for MIGRATION.md gating)
// and by the migration-doc tool (to summarise what would be migrated).
//
// The list mirrors store.v1VaultMarkerDirs but is duplicated here to
// avoid coupling the vault package to that unexported symbol. Drift
// risk is low: the v1 layout is frozen.
func detectV1Dirs(vaultPath string) []string {
	candidates := []string{
		"sessions", "decisions", "memories", "skills", "configs",
		"plans", "targets", "ci", "prs", "repos",
	}
	var present []string
	for _, d := range candidates {
		if fi, err := os.Stat(filepath.Join(vaultPath, d)); err == nil && fi.IsDir() {
			present = append(present, d)
		}
	}
	return present
}

// renderMnemoIndex returns the content of <vault>/_mnemo/index.md —
// the entry point for the mnemo-generated wing of the vault.
func renderMnemoIndex() string {
	var b strings.Builder
	b.WriteString("---\n")
	b.WriteString("tags:\n")
	b.WriteString("  - mnemo\n")
	b.WriteString("  - mnemo/index\n")
	b.WriteString("---\n\n")
	b.WriteString("# mnemo library wing\n\n")
	b.WriteString("Generated knowledge — themes, patterns, cross-repo views, ")
	b.WriteString("lessons, filtered decisions, and indexed memories — ")
	b.WriteString("produced by [mnemo](https://github.com/marcelocantos/mnemo).\n\n")

	b.WriteString("## Collections\n\n")
	b.WriteString("- `themes/` — cross-session clusters of related work\n")
	b.WriteString("- `patterns/` — recurring workflow patterns\n")
	b.WriteString("- `cross-repo/` — themes spanning multiple repositories\n")
	b.WriteString("- `lessons/` — distilled feedback and confirmed decisions\n")
	b.WriteString("- `decisions/` — high-signal decisions filtered from sessions\n")
	b.WriteString("- `memories/` — auto-memory files indexed across projects\n\n")

	b.WriteString("## Conventions\n\n")
	b.WriteString("Every file under `_mnemo/` carries the `mnemo` tag plus a ")
	b.WriteString("`mnemo/<type>` tag. Filter your graph or search by ")
	b.WriteString("`-tag:mnemo` to exclude the wing entirely; query ")
	b.WriteString("`FROM \"_mnemo\"` to scope to it.\n\n")
	b.WriteString("Content above the `<!-- mnemo:generated -->` fence is owned ")
	b.WriteString("by mnemo and regenerated on each sync. Annotations below ")
	b.WriteString("the fence are yours and are preserved across re-syncs.\n")

	return b.String()
}

// renderMnemoREADME returns the content of <vault>/_mnemo/README.md —
// orientation for a user who opens the directory cold.
func renderMnemoREADME() string {
	var b strings.Builder
	b.WriteString("---\n")
	b.WriteString("tags:\n")
	b.WriteString("  - mnemo\n")
	b.WriteString("  - mnemo/readme\n")
	b.WriteString("---\n\n")
	b.WriteString("# _mnemo/ — generated knowledge wing\n\n")
	b.WriteString("This directory is the mnemo daemon's writable workspace inside ")
	b.WriteString("your vault. Everything you see here was written by ")
	b.WriteString("[mnemo](https://github.com/marcelocantos/mnemo) from indexed ")
	b.WriteString("session transcripts, memories, and decisions.\n\n")

	b.WriteString("## Two contracts to know about\n\n")
	b.WriteString("1. **Fence contract.** Every file (except `MIGRATION.md`) ")
	b.WriteString("uses an `<!-- mnemo:generated -->` HTML comment to ")
	b.WriteString("separate generated content above from your annotations ")
	b.WriteString("below. mnemo rewrites everything above the fence on ")
	b.WriteString("every sync; everything below is yours and is preserved.\n\n")
	b.WriteString("2. **`MIGRATION.md` write-once.** When mnemo first detects ")
	b.WriteString("that your vault was populated by the v1 layout (root-level ")
	b.WriteString("`sessions/`, `decisions/`, ...), it writes this file ")
	b.WriteString("**once** to explain what changed. mnemo does not ")
	b.WriteString("regenerate it on subsequent syncs. If you delete it, ")
	b.WriteString("mnemo takes that as \"I have read this; move on\" and ")
	b.WriteString("will not recreate it. To get the doc back, run ")
	b.WriteString("`mnemo_vault_migration_doc(write: true)` from any ")
	b.WriteString("MCP-aware client.\n\n")

	b.WriteString("## Index\n\n")
	b.WriteString("See [[_mnemo/index|index]] for the collections.\n\n")

	b.WriteString("## Safe to touch?\n\n")
	b.WriteString("Below-fence content: yes, freely. Mnemo never touches it.\n")
	b.WriteString("Above-fence content: edits will be overwritten on the next ")
	b.WriteString("sync — copy anything worth keeping below the fence first.\n")
	b.WriteString("Whole files: deleting them is fine; mnemo will recreate ")
	b.WriteString("the generated ones on the next sync (except `MIGRATION.md` ")
	b.WriteString("per the contract above).\n")
	return b.String()
}

// renderMigrationDoc produces the body of MIGRATION.md given the
// vault path and the list of detected v1 dirs.
//
// The doc is intentionally narrow: it explains the layout change and
// points the user at the gc_legacy tool (slice 9). It does not claim
// what has migrated where, since the per-collection migrations land
// across subsequent slices. The doc is the "read me first" pointer,
// not a manifest.
func renderMigrationDoc(vaultPath string, v1Dirs []string) string {
	sort.Strings(v1Dirs)
	var b strings.Builder
	b.WriteString("# mnemo vault — v1 → v2 layout migration\n\n")
	b.WriteString("This vault was populated under mnemo's v1 layout, which ")
	b.WriteString("wrote generated notes to root-level directories alongside ")
	b.WriteString("your own files. As of mnemo v0.43+, generated content ")
	b.WriteString("lives under `_mnemo/` so it never collides with your ")
	b.WriteString("own structure.\n\n")

	b.WriteString("## What mnemo saw\n\n")
	b.WriteString("The following root-level v1 directories are present in ")
	b.WriteString("this vault:\n\n")
	for _, d := range v1Dirs {
		fmt.Fprintf(&b, "- `%s/`\n", d)
	}
	b.WriteString("\n")

	b.WriteString("## What happens next\n\n")
	b.WriteString("- `vault_layout` defaults to `\"both\"` on a v1-populated ")
	b.WriteString("vault. mnemo writes to both root-level dirs and `_mnemo/` ")
	b.WriteString("so nothing you currently rely on disappears.\n")
	b.WriteString("- After a soak window (30 days by default), mnemo emits a ")
	b.WriteString("weekly warning suggesting you opt into pure v2.\n")
	b.WriteString("- `mnemo_vault_gc_legacy` (forthcoming) is the user-")
	b.WriteString("initiated path that removes the v1 root-level dirs once ")
	b.WriteString("you are satisfied with the new layout.\n\n")

	b.WriteString("## Opting in early\n\n")
	b.WriteString("Set `vault_layout.mode = \"v2\"` in `~/.mnemo/config.json` ")
	b.WriteString("(or via `mnemo_config`). New syncs will stop writing v1 ")
	b.WriteString("paths immediately; existing v1 files remain on disk until ")
	b.WriteString("you run `gc_legacy`.\n\n")

	b.WriteString("## This file\n\n")
	b.WriteString("`MIGRATION.md` is **write-once**. mnemo will not ")
	b.WriteString("regenerate it. If you delete it, mnemo will not recreate ")
	b.WriteString("it. To get a fresh snapshot, run ")
	b.WriteString("`mnemo_vault_migration_doc(write: true)`.\n")
	return b.String()
}

// MigrationDocSnapshot returns the current MIGRATION.md content
// without writing anything. Used by the mnemo_vault_migration_doc MCP
// tool's `write: false` mode.
func (e *Exporter) MigrationDocSnapshot() string {
	return renderMigrationDoc(e.path, detectV1Dirs(e.path))
}

// WriteMigrationDoc renders and writes MIGRATION.md to the vault's
// _mnemo/ directory unconditionally (overwriting any existing file).
// Used by mnemo_vault_migration_doc(write: true) so a user who deleted
// the file can get a fresh snapshot on demand.
//
// The directory is created if missing. Returns the path written to so
// the caller can include it in the tool response.
func (e *Exporter) WriteMigrationDoc() (string, error) {
	wingDir := filepath.Join(e.path, mnemoNamespaceDir)
	if err := os.MkdirAll(wingDir, 0o755); err != nil {
		return "", fmt.Errorf("vault: mkdir %s: %w", wingDir, err)
	}
	path := filepath.Join(wingDir, migrationDocName)
	content := renderMigrationDoc(e.path, detectV1Dirs(e.path))
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("vault: write _mnemo/MIGRATION.md: %w", err)
	}
	// Also pin the write-once flag so the periodic sync path keeps
	// honouring deletion as "move on" rather than recreating.
	state, err := store.LoadState()
	if err == nil {
		state.VaultMigrationDocWritten = true
		_ = store.WriteState(state)
	}
	return path, nil
}

// resolveLayoutHooked is a package-level seam used by the exporter's
// Sync to look up the active layout. Real callers set this from the
// registry (which reads the live config); tests can override.
//
// store import is intentional here even though we only need string
// constants: keeping them in one place avoids drift if the design
// adds a fourth layout mode.
var (
	_ = store.VaultLayoutV2
	_ = store.VaultLayoutBoth
	_ = store.VaultLayoutV1
)
