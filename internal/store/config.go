// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config holds runtime configuration loaded from ~/.mnemo/config.json.
//
// This file is optional. If absent, sensible defaults apply. Its purpose
// is to let the daemon discover repos and project directories that live
// outside the places mnemo would guess on its own (~/work for repos,
// ~/.claude/projects for transcripts).
type Config struct {
	// WorkspaceRoots are filesystem roots under which repo-level streams
	// (targets, audit logs, plans, CLAUDE.md, CI) discover repositories.
	// Each root is walked for .git entries to identify repos. An empty
	// list falls back to DefaultWorkspaceRoots.
	WorkspaceRoots []string `json:"workspace_roots,omitempty"`

	// ExtraProjectDirs lists extra Claude Code project directories to
	// index beyond ~/.claude/projects/. Used for cross-platform
	// transcript ingest (🎯T15) — e.g. a Windows VM's Claude projects
	// dir exposed via SMB mount. Missing or unavailable entries are
	// skipped at ingest/watch time rather than failing.
	ExtraProjectDirs []string `json:"extra_project_dirs,omitempty"`

	// SynthesisRoots are filesystem roots walked by the synthesis-doc
	// indexer (🎯T34) to index analysis/research/design/planning docs
	// under docs/{papers,design,analysis,plans}/ plus docs/audit-log.md
	// and docs/convergence-report.md. Unlike WorkspaceRoots, these roots
	// do not require a .git marker — suitable for non-repo planning
	// spaces such as ~/think. Entries support ~ for the user's home.
	// An empty list disables synthesis-doc ingest (repo-level docs are
	// still indexed via WorkspaceRoots + IngestDocs).
	SynthesisRoots []string `json:"synthesis_roots,omitempty"`

	// VaultPath is the directory where mnemo writes its Markdown knowledge
	// graph. When set, mnemo continuously exports sessions, decisions,
	// memories, skills, configs, plans, targets, CI runs, and PRs as
	// Markdown files compatible with Obsidian and Logseq. Human edits
	// below the <!-- mnemo:generated --> fence are preserved on re-sync.
	// Supports ~ for the user's home directory.
	// When absent or empty, vault export is completely disabled.
	VaultPath string `json:"vault_path,omitempty"`

	// VaultIndexingScope selects which subtree of the vault mnemo reads
	// during IngestVaultAnnotations (🎯T64.1 — consent fix). One of:
	//   - "_mnemo_only" — only <vault>/_mnemo/ is walked. Below-fence
	//     annotations on generated pages plus user Markdown placed
	//     inside the wing surface in mnemo_search; nothing outside the
	//     wing is touched.
	//   - "full"        — the entire vault is walked (hidden dirs like
	//     .obsidian/, .git/, .trash/ excluded). Matches v1 behaviour.
	//   - "includes"    — <vault>/_mnemo/ plus each path listed in
	//     VaultIndexingIncludes is walked.
	//
	// Empty resolves via ResolvedVaultIndexingScope at runtime: new
	// vaults default to "_mnemo_only" (the safest scope), pre-existing
	// v1-populated vaults default to "full" for continuity. The detection
	// is documented under "Indexing scope" in docs/design/vault-library-wing.md.
	VaultIndexingScope string `json:"vault_indexing_scope,omitempty"`

	// VaultIndexingIncludes lists vault-relative paths walked in
	// addition to <vault>/_mnemo/ when VaultIndexingScope == "includes".
	// Ignored under "_mnemo_only" or "full". Paths use forward slashes;
	// `..` and absolute paths are rejected at validation.
	VaultIndexingIncludes []string `json:"vault_indexing_includes,omitempty"`

	// VaultIndexingIgnoreFile is the vault-relative path to a
	// gitignore-syntax file applied across the configured scope
	// (including inside _mnemo/). Empty defaults to ".mnemoignore"; an
	// absent file means no extra exclusions. Only this single file is
	// consulted — nested .mnemoignore files are not honoured.
	VaultIndexingIgnoreFile string `json:"vault_indexing_ignore_file,omitempty"`

	// VaultLayout controls which directory tree(s) the vault exporter
	// writes (🎯T64.2). The zero value resolves at runtime via
	// ResolvedVaultLayout against the live vault: a vault with any
	// root-level v1 marker dir (sessions/, decisions/, ...) defaults to
	// "both" for migration continuity, anything else defaults to "v2".
	VaultLayout VaultLayoutConfig `json:"vault_layout,omitempty"`

	// LinkedInstances declares peer mnemo endpoints to federate with
	// (🎯T15). Each peer is identified by a https URL and a trusted
	// peer certificate (either a name resolved under ~/.mnemo/peers/
	// or an inline PEM). An absent or empty list disables federation
	// entirely; the daemon makes no outbound peer calls.
	LinkedInstances []LinkedInstance `json:"linked_instances,omitempty"`

	// Backup controls the daemon's periodic backup worker (🎯T61).
	// Absent in config.json → all defaults apply, backup enabled.
	Backup BackupConfig `json:"backup,omitempty"`

	// CostReconciliation controls the Anthropic Admin API reconciler
	// (🎯T63). Disabled by default: even with ANTHROPIC_ADMIN_API_KEY
	// set in the environment, no outbound Admin API call is made until
	// the user explicitly opts in via this config block. This protects
	// users in security-reviewed environments where unsolicited egress
	// to hosted APIs is undesirable. The estimated-cost path (derived
	// from transcript tokens) is always on and requires zero external
	// calls.
	CostReconciliation CostReconciliationConfig `json:"cost_reconciliation,omitempty"`

	// ConnectionSweep controls the daemon_connections sweeper
	// (🎯T60). Absent in config.json → defaults apply, sweeper
	// enabled. Set {"connection_sweep": {"disabled": true}} to opt
	// out (the open-row count will then grow unbounded as it did
	// before — accepted only if some external mechanism reaps).
	ConnectionSweep ConnectionSweepConfig `json:"connection_sweep,omitempty"`
}

// ConnectionSweepConfig controls how often the daemon checks for
// stale daemon_connections rows and how long a row must be idle
// before being marked closed. A zero-value ConnectionSweepConfig
// (i.e. the section absent from config.json) means enabled, sweep
// every minute, idle threshold 10 minutes.
type ConnectionSweepConfig struct {
	// Disabled opts out of the sweeper. Negated form so the zero
	// value (absent section) means enabled.
	Disabled bool `json:"disabled,omitempty"`

	// Interval between sweep ticks. Format: a Go time.ParseDuration
	// string. Empty → "1m".
	Interval string `json:"interval,omitempty"`

	// StaleAfter is the duration since last_seen_at after which a
	// connection is considered stale and marked closed. Format: a
	// Go time.ParseDuration string. Empty → "10m".
	StaleAfter string `json:"stale_after,omitempty"`
}

// IsEnabled reports whether the sweeper should run. Defaults to true;
// only an explicit `"disabled": true` opts out.
func (c ConnectionSweepConfig) IsEnabled() bool { return !c.Disabled }

// EffectiveInterval returns the parsed sweep interval, or 1 minute
// when unset.
func (c ConnectionSweepConfig) EffectiveInterval() (time.Duration, error) {
	if c.Interval == "" {
		return time.Minute, nil
	}
	d, err := time.ParseDuration(c.Interval)
	if err != nil {
		return 0, fmt.Errorf("interval: %w", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("interval must be positive, got %v", d)
	}
	return d, nil
}

// EffectiveStaleAfter returns the parsed idle threshold, or 10 minutes
// when unset.
func (c ConnectionSweepConfig) EffectiveStaleAfter() (time.Duration, error) {
	if c.StaleAfter == "" {
		return 10 * time.Minute, nil
	}
	d, err := time.ParseDuration(c.StaleAfter)
	if err != nil {
		return 0, fmt.Errorf("stale_after: %w", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("stale_after must be positive, got %v", d)
	}
	return d, nil
}

// CostReconciliationConfig gates the Anthropic Admin API reconciler.
// A zero-value CostReconciliationConfig (i.e. the section omitted from
// config.json) means disabled — opposite of BackupConfig's safety
// default, because the safe default for unsolicited outbound API calls
// is "do not call".
type CostReconciliationConfig struct {
	// Enabled opts in to the reconciler. When false (the zero value
	// and documented default), StartReconciler exits immediately and
	// makes zero Admin API calls regardless of whether
	// ANTHROPIC_ADMIN_API_KEY is set in the daemon's environment.
	Enabled bool `json:"enabled,omitempty"`
}

// IsEnabled reports whether the reconciler should run. False by
// default (zero-value config = no Admin API calls).
func (c CostReconciliationConfig) IsEnabled() bool { return c.Enabled }

// BackupConfig controls the periodic backup worker. Field defaults are
// resolved via the Effective* methods so a zero-value BackupConfig (the
// state when config.json omits the "backup" section entirely) gets
// sensible behaviour: enabled, ~/.mnemo/backups, 7 dailies, 03:00–04:00
// local window, 15 min quiescence threshold.
type BackupConfig struct {
	// Disabled opts out of periodic backups. Negated form chosen so
	// the zero value (BackupConfig{}, what you get when config.json
	// omits the "backup" key entirely) means enabled — backups are the
	// safe default.
	Disabled bool `json:"disabled,omitempty"`

	// Dir is the backup directory. Supports ~ for the user's home.
	// Empty → ~/.mnemo/backups.
	Dir string `json:"dir,omitempty"`

	// KeepDailies caps the number of snapshots retained. Older
	// backups beyond this count are deleted after each successful run.
	// 0 or unset → 7.
	KeepDailies int `json:"keep_dailies,omitempty"`

	// WindowStart and WindowEnd bound the local time-of-day during
	// which the worker may take its daily snapshot. Format "HH:MM"
	// (24h). Empty → "03:00" / "04:00".
	WindowStart string `json:"window_start,omitempty"`
	WindowEnd   string `json:"window_end,omitempty"`

	// QuiescenceMin is the minimum time since the last recorded write
	// activity (Store.NoteActivity) before the worker will snapshot.
	// Format: a Go time.ParseDuration string. Empty → "15m".
	QuiescenceMin string `json:"quiescence_min,omitempty"`
}

// IsEnabled reports whether the periodic backup worker should run.
// Defaults to true; only an explicit `"disabled": true` opts out.
func (b BackupConfig) IsEnabled() bool { return !b.Disabled }

// EffectiveDir returns Dir with ~ expanded, or ~/.mnemo/backups when
// Dir is empty. userHome is the home directory used for expansion.
func (b BackupConfig) EffectiveDir(userHome string) string {
	d := b.Dir
	if d == "" {
		return filepath.Join(userHome, ".mnemo", "backups")
	}
	if userHome != "" {
		switch {
		case d == "~":
			return userHome
		case strings.HasPrefix(d, "~/"):
			return filepath.Join(userHome, d[2:])
		}
	}
	return d
}

// EffectiveKeepDailies returns KeepDailies or 7 when unset.
func (b BackupConfig) EffectiveKeepDailies() int {
	if b.KeepDailies > 0 {
		return b.KeepDailies
	}
	return 7
}

// EffectiveWindow returns the [start, end) local time-of-day window for
// the daily backup attempt. Returns parsed time.Duration offsets from
// midnight rather than parsed times, since the worker needs to compute
// "next 03:17 today or tomorrow" against the wall clock.
//
// Returns an error if WindowStart/WindowEnd are set but malformed.
// Defaults: 3h, 4h (03:00, 04:00).
func (b BackupConfig) EffectiveWindow() (start, end time.Duration, err error) {
	start, err = parseHHMM(b.WindowStart, 3*time.Hour)
	if err != nil {
		return 0, 0, fmt.Errorf("window_start: %w", err)
	}
	end, err = parseHHMM(b.WindowEnd, 4*time.Hour)
	if err != nil {
		return 0, 0, fmt.Errorf("window_end: %w", err)
	}
	if end <= start {
		return 0, 0, fmt.Errorf("window_end (%v) must be > window_start (%v)", end, start)
	}
	return start, end, nil
}

// EffectiveQuiescenceMin returns the parsed quiescence threshold, or
// 15 minutes when unset. Errors are surfaced so config validation can
// catch a typo'd value at write time.
func (b BackupConfig) EffectiveQuiescenceMin() (time.Duration, error) {
	if b.QuiescenceMin == "" {
		return 15 * time.Minute, nil
	}
	d, err := time.ParseDuration(b.QuiescenceMin)
	if err != nil {
		return 0, fmt.Errorf("quiescence_min: %w", err)
	}
	if d < 0 {
		return 0, fmt.Errorf("quiescence_min: must be non-negative, got %v", d)
	}
	return d, nil
}

// parseHHMM parses "HH:MM" (24-hour, leading zeros optional) and
// returns the offset from midnight. Empty input returns dflt.
func parseHHMM(s string, dflt time.Duration) (time.Duration, error) {
	if s == "" {
		return dflt, nil
	}
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("expected HH:MM, got %q", s)
	}
	h, err := parseUintBounded(parts[0], 0, 23)
	if err != nil {
		return 0, fmt.Errorf("hour: %w", err)
	}
	m, err := parseUintBounded(parts[1], 0, 59)
	if err != nil {
		return 0, fmt.Errorf("minute: %w", err)
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, nil
}

func parseUintBounded(s string, lo, hi int) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("non-digit in %q", s)
		}
		n = n*10 + int(c-'0')
	}
	if n < lo || n > hi {
		return 0, fmt.Errorf("%d out of range [%d,%d]", n, lo, hi)
	}
	return n, nil
}

// LinkedInstance is one peer mnemo endpoint the daemon may query.
type LinkedInstance struct {
	// Name uniquely identifies the peer. Used for log lines and to
	// attribute federated query results back to the source peer.
	Name string `json:"name"`

	// URL is the peer's MCP endpoint, https only.
	URL string `json:"url"`

	// PeerCert is either the bare basename of a file under
	// ~/.mnemo/peers/ (e.g. "alice" → ~/.mnemo/peers/alice.pem) or an
	// inline PEM-encoded X.509 certificate. The first form is the
	// usual case; inline PEM is for small deployments that want
	// everything in one config file.
	PeerCert string `json:"peer_cert"`
}

// LoadConfig reads ~/.mnemo/config.json. Returns a zero Config if the
// file doesn't exist. Federation peers (LinkedInstances) are validated
// against ~/.mnemo/peers/; any structural problem (duplicate name,
// non-https URL, unresolvable peer cert) returns an error so startup
// fails loud rather than silently disabling federation.
func LoadConfig() (Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Config{}, err
	}
	cfg, err := loadConfigFrom(filepath.Join(home, ".mnemo", "config.json"))
	if err != nil {
		return Config{}, err
	}
	if err := cfg.validateLinkedInstances(filepath.Join(home, ".mnemo", "peers")); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func loadConfigFrom(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, nil
		}
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ConfigPath returns the absolute path to ~/.mnemo/config.json for the
// current process user. Returns an error only when the home directory
// cannot be resolved.
func ConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".mnemo", "config.json"), nil
}

// WriteConfig persists cfg to ~/.mnemo/config.json atomically and after
// passing the same federation-peer validation that LoadConfig applies on
// startup. The write is atomic in the rename-into-place sense: a tmp
// file is written next to the target and renamed once fsync'd, so a
// crashed writer cannot leave a half-formed config visible to a
// subsequent LoadConfig call.
//
// vault_path is trial-balloon validated (see validateVaultPath) before
// the rename so the persisted config is always loadable cleanly — a
// path that vault.New would reject is rejected here too, leaving the
// previous on-disk config intact.
//
// Used by the mnemo_config MCP tool to apply runtime configuration
// changes (chiefly vault_path) without requiring a daemon restart.
func WriteConfig(cfg Config) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	if err := cfg.validateLinkedInstances(filepath.Join(home, ".mnemo", "peers")); err != nil {
		return err
	}
	if err := cfg.validateVaultPath(home); err != nil {
		return err
	}
	path := filepath.Join(home, ".mnemo", "config.json")
	return writeConfigTo(path, cfg)
}

// validateVaultPath mirrors the only fallible step of vault.New
// (os.MkdirAll on the resolved root) so a bad vault_path is rejected
// before WriteConfig commits the new config to disk. Without this
// check, a write of e.g. {"vault_path": "/dev/null"} succeeds and
// persists; the subsequent Reload's swapVault fails and surfaces a
// Warning, but the on-disk config is already wrong and the next
// daemon start re-hits the failure during initial setup.
//
// home is the daemon's home directory — used to ~-expand vault_path.
// In the common single-user deployment this matches the per-user
// homeDir that Reload's swapVault uses. On a multi-user Windows
// Service install where different users may resolve ~ differently,
// trial-balloon coverage against the daemon home is still enough to
// catch the typical "garbage path" mistake; user-specific resolution
// failures continue to surface as Reload Warnings.
//
// Empty VaultPath is the documented "vault disabled" state and skips
// validation.
func (c Config) validateVaultPath(home string) error {
	resolved := c.ResolvedVaultPath(home)
	if resolved == "" {
		return nil
	}
	if err := os.MkdirAll(resolved, 0o755); err != nil {
		return fmt.Errorf("vault_path %q is not usable: %w", c.VaultPath, err)
	}
	return nil
}

// writeConfigTo is the testable core of WriteConfig: it writes cfg to
// path using a sibling tmp file + rename so concurrent readers always
// observe either the previous or the new file, never a partial write.
// The parent directory is created if missing (mode 0o755).
func writeConfigTo(path string, cfg Config) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(path), ".config.json.*")
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("sync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// validateLinkedInstances enforces the rules documented on
// LinkedInstance: unique names, https-only URLs, and resolvable
// peer certificates (either as a name under peersDir or as inline
// PEM that parses as an X.509 certificate). Returns the first
// violation encountered; the error message names the offending entry
// (or pair) so the user can correct config.json without grep.
func (c Config) validateLinkedInstances(peersDir string) error {
	seen := map[string]int{}
	for i, li := range c.LinkedInstances {
		if li.Name == "" {
			return fmt.Errorf("linked_instances[%d]: name is required", i)
		}
		if prev, dup := seen[li.Name]; dup {
			return fmt.Errorf("linked_instances: duplicate name %q at indexes %d and %d", li.Name, prev, i)
		}
		seen[li.Name] = i

		if li.URL == "" {
			return fmt.Errorf("linked_instances[%q]: url is required", li.Name)
		}
		u, err := url.Parse(li.URL)
		if err != nil {
			return fmt.Errorf("linked_instances[%q]: parse url %q: %w", li.Name, li.URL, err)
		}
		if u.Scheme != "https" {
			return fmt.Errorf("linked_instances[%q]: url scheme must be https, got %q", li.Name, u.Scheme)
		}

		if li.PeerCert == "" {
			return fmt.Errorf("linked_instances[%q]: peer_cert is required", li.Name)
		}
		if err := validatePeerCert(li.Name, li.PeerCert, peersDir); err != nil {
			return err
		}
	}
	return nil
}

// validatePeerCert resolves and parses li.PeerCert via the same logic
// as ResolvePeerCert; the validation pass is just "did this resolve".
func validatePeerCert(name, value, peersDir string) error {
	li := LinkedInstance{Name: name, PeerCert: value}
	if _, err := li.ResolvePeerCert(peersDir); err != nil {
		return err
	}
	return nil
}

// ResolvePeerCert returns the parsed X.509 certificate for
// li.PeerCert. If PeerCert contains a "-----BEGIN" marker it is
// treated as inline PEM; otherwise it is treated as a basename to
// resolve under peersDir/<name>.pem. Errors include the instance
// name so they make sense in startup logs.
func (li LinkedInstance) ResolvePeerCert(peersDir string) (*x509.Certificate, error) {
	if looksLikeInlinePEM(li.PeerCert) {
		block, _ := pem.Decode([]byte(li.PeerCert))
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("linked_instances[%q]: inline peer_cert is not a CERTIFICATE PEM block", li.Name)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("linked_instances[%q]: parse inline peer_cert: %w", li.Name, err)
		}
		return cert, nil
	}

	path := filepath.Join(peersDir, li.PeerCert+".pem")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("linked_instances[%q]: peer_cert %q not found at %s: %w", li.Name, li.PeerCert, path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("linked_instances[%q]: peer_cert %s: no CERTIFICATE PEM block", li.Name, path)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("linked_instances[%q]: parse peer_cert %s: %w", li.Name, path, err)
	}
	return cert, nil
}

func looksLikeInlinePEM(s string) bool {
	return strings.Contains(s, "-----BEGIN")
}

// DefaultWorkspaceRoots returns the default workspace roots: [~/work].
// This matches the convention used across the global CLAUDE.md for
// Go-style repo layouts (~/work/github.com/org/repo).
func DefaultWorkspaceRoots() []string {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	return []string{filepath.Join(home, "work")}
}

// ResolvedWorkspaceRoots returns the WorkspaceRoots as configured, or
// DefaultWorkspaceRoots if none are set.
func (c Config) ResolvedWorkspaceRoots() []string {
	if len(c.WorkspaceRoots) == 0 {
		return DefaultWorkspaceRoots()
	}
	return c.WorkspaceRoots
}

// ResolvedVaultPath returns VaultPath with ~ expanded using userHome.
// Returns "" when VaultPath is not set (vault export disabled).
// userHome is passed in rather than looked up here so ForUser can
// expand ~ relative to the target user's home directory, not the
// daemon's own home (relevant on Windows where LocalSystem runs the
// daemon but a named user's vault path is configured).
func (c Config) ResolvedVaultPath(userHome string) string {
	p := c.VaultPath
	if p == "" {
		return ""
	}
	if userHome != "" {
		switch {
		case p == "~":
			return userHome
		case strings.HasPrefix(p, "~/"):
			return filepath.Join(userHome, p[2:])
		}
	}
	return p
}

// Vault indexing scope constants. Use these instead of bare string
// literals so a typo surfaces at compile time.
const (
	VaultIndexingScopeMnemoOnly = "_mnemo_only"
	VaultIndexingScopeFull      = "full"
	VaultIndexingScopeIncludes  = "includes"
)

// Vault layout constants. v2 writes only to <vault>/_mnemo/; v1 writes
// only to the root-level v1 dirs (sessions/, decisions/, ...); both
// writes to both trees during a migration window.
const (
	VaultLayoutV2   = "v2"
	VaultLayoutBoth = "both"
	VaultLayoutV1   = "v1"
)

// defaultVaultLayoutSoakWarnAfter is the soak window after which a
// vault stuck on layout="both" begins emitting a structured warning on
// each sync (weekly cadence once tripped). 720h = 30 days. The user
// can lengthen via VaultLayoutConfig.SoakWarnAfter.
const defaultVaultLayoutSoakWarnAfter = 720 * time.Hour

// VaultLayoutConfig governs which vault tree(s) the exporter writes.
// Modelled as a struct (rather than a bare string) so the soak-warn
// window can be tuned alongside the mode without inventing a sibling
// top-level config key.
type VaultLayoutConfig struct {
	// Mode is one of VaultLayoutV2, VaultLayoutBoth, VaultLayoutV1, or
	// empty. Empty resolves at runtime per ResolvedVaultLayout.
	Mode string `json:"mode,omitempty"`

	// SoakWarnAfter is a Go time.ParseDuration string controlling when
	// the daemon begins warning about a vault stuck on Mode == "both".
	// Empty defaults to 720h (30 days). Only consulted while Mode
	// resolves to "both"; ignored otherwise.
	SoakWarnAfter string `json:"soak_warn_after,omitempty"`
}

// EffectiveSoakWarnAfter parses SoakWarnAfter and returns the duration,
// or defaultVaultLayoutSoakWarnAfter when unset. A malformed value
// surfaces as an error so config validation can flag it at write time.
func (l VaultLayoutConfig) EffectiveSoakWarnAfter() (time.Duration, error) {
	if l.SoakWarnAfter == "" {
		return defaultVaultLayoutSoakWarnAfter, nil
	}
	d, err := time.ParseDuration(l.SoakWarnAfter)
	if err != nil {
		return 0, fmt.Errorf("vault_layout.soak_warn_after: %w", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("vault_layout.soak_warn_after must be positive, got %v", d)
	}
	return d, nil
}

// ResolvedVaultLayout returns the effective layout mode for the vault
// at resolvedVaultPath. When VaultLayout.Mode is set explicitly, it
// wins (after type-check). Otherwise:
//
//   - any v1 marker dir present (sessions/, decisions/, ...) → "both"
//     (migration continuity — keep writing v1 while user opts in to v2)
//   - otherwise (new or already-v2 vault)                   → "v2"
//
// An empty resolvedVaultPath returns "v2" — the call site is
// responsible for not invoking the exporter when vault is disabled.
func (c Config) ResolvedVaultLayout(resolvedVaultPath string) string {
	switch c.VaultLayout.Mode {
	case VaultLayoutV2, VaultLayoutBoth, VaultLayoutV1:
		return c.VaultLayout.Mode
	}
	if resolvedVaultPath == "" {
		return VaultLayoutV2
	}
	for _, d := range v1VaultMarkerDirs {
		if fi, err := os.Stat(filepath.Join(resolvedVaultPath, d)); err == nil && fi.IsDir() {
			return VaultLayoutBoth
		}
	}
	return VaultLayoutV2
}

// defaultVaultIgnoreFile is the conventional name of the gitignore-
// syntax exclude file at the vault root.
const defaultVaultIgnoreFile = ".mnemoignore"

// v1VaultMarkerDirs are the root-level subdirectories the v1 vault
// layout writes to. The presence of any of these without a sibling
// `_mnemo/` indicates a pre-Slice 1 vault that should default to
// "full" indexing scope for continuity.
var v1VaultMarkerDirs = []string{
	"sessions", "decisions", "memories", "skills", "configs",
	"plans", "targets", "ci", "prs", "repos",
}

// ResolvedVaultIndexingScope returns the effective indexing scope for
// the vault at resolvedVaultPath. When VaultIndexingScope is set, it
// wins. Otherwise the default is computed by inspecting the vault:
//
//   - <vault>/_mnemo/ exists                       → "_mnemo_only"
//   - any v1 marker dir exists (sessions/, ...)    → "full"  (continuity)
//   - otherwise (empty or missing directory)       → "_mnemo_only"
//
// An empty resolvedVaultPath returns "_mnemo_only" — the call site is
// responsible for not invoking the walker when vault is disabled.
func (c Config) ResolvedVaultIndexingScope(resolvedVaultPath string) string {
	switch c.VaultIndexingScope {
	case VaultIndexingScopeMnemoOnly, VaultIndexingScopeFull, VaultIndexingScopeIncludes:
		return c.VaultIndexingScope
	}
	if resolvedVaultPath == "" {
		return VaultIndexingScopeMnemoOnly
	}
	if fi, err := os.Stat(filepath.Join(resolvedVaultPath, "_mnemo")); err == nil && fi.IsDir() {
		return VaultIndexingScopeMnemoOnly
	}
	for _, d := range v1VaultMarkerDirs {
		if fi, err := os.Stat(filepath.Join(resolvedVaultPath, d)); err == nil && fi.IsDir() {
			return VaultIndexingScopeFull
		}
	}
	return VaultIndexingScopeMnemoOnly
}

// ResolvedVaultIndexingIgnoreFile returns the configured ignore-file
// basename, or ".mnemoignore" when unset.
func (c Config) ResolvedVaultIndexingIgnoreFile() string {
	if c.VaultIndexingIgnoreFile != "" {
		return c.VaultIndexingIgnoreFile
	}
	return defaultVaultIgnoreFile
}

// ResolvedSynthesisRoots returns SynthesisRoots with ~ expanded to the
// user's home directory. Unset entries return an empty slice (the
// indexer skips synthesis ingest entirely when no roots are configured;
// there is no default, unlike WorkspaceRoots).
func (c Config) ResolvedSynthesisRoots() []string {
	if len(c.SynthesisRoots) == 0 {
		return nil
	}
	home, _ := os.UserHomeDir()
	out := make([]string, 0, len(c.SynthesisRoots))
	for _, r := range c.SynthesisRoots {
		if r == "" {
			continue
		}
		if home != "" {
			switch {
			case r == "~":
				r = home
			case strings.HasPrefix(r, "~/"):
				r = filepath.Join(home, r[2:])
			}
		}
		out = append(out, r)
	}
	return out
}
