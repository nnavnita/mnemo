// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

// Package tools defines MCP tool schemas and handlers for the mnemo server.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/marcelocantos/mnemo/internal/store"
	"github.com/marcelocantos/mnemo/internal/vault"
)

// VaultSyncer is satisfied by *vault.Exporter when vault is configured.
// Defined as an interface so test code can substitute a fake. The tools
// package does import the vault package solely for vault.ErrSyncInFlight
// — a one-symbol sentinel needed to distinguish coalesced calls from
// real errors in the MCP response.
type VaultSyncer interface {
	Sync(ctx context.Context) error
	Path() string
	MigrationDocSnapshot() string
	WriteMigrationDoc() (string, error)
}

// CompactorHealthReporter is satisfied by *compact.Watcher.
// Surfaced as an interface so this package doesn't need to import
// the compact package (which would create a circular dependency
// hierarchy in tests and main). Backed by Watcher.Health() — see
// internal/compact/watcher.go.
type CompactorHealthReporter interface {
	Health() CompactorHealth
}

// CompactorHealth is the externally-visible snapshot returned by the
// mnemo_compactor_status MCP tool (🎯T67). Mirrors
// compact.HealthSnapshot field-for-field; duplicated here to keep
// the tools→compact import direction clean.
type CompactorHealth struct {
	LastScanAt            time.Time
	LastScanCount         int
	Backlog               int
	LastTickAt            time.Time
	LastTickOutcome       string
	InFlightSession       string
	Counts                map[string]int64
	ScanInterval          time.Duration
	IdleTimeout           time.Duration
	TickTimeout           time.Duration
	MinDeltaMessages      int
	MaxCompactionsPerScan int
	MaxTokenRatio         float64
}

// ConfigReport mirrors registry.ReloadReport without importing the
// registry package. The mnemo_config write handler returns these four
// slices verbatim so the caller can see which fields were applied live,
// which require a restart, and which adoption attempts failed despite
// the config write itself succeeding.
type ConfigReport struct {
	Changed         []string
	Adopted         []string
	RequiresRestart []string
	Warnings        []string
}

// ConfigController is the dependency the mnemo_config tool uses to read
// and atomically apply config changes. Injected from main so the tools
// package stays free of any direct dependency on the registry or
// filesystem layout. Get returns a snapshot of the live Config. Put
// validates+persists newCfg to disk and applies in-process adoption
// across every per-user Store.
type ConfigController interface {
	Get() store.Config
	Put(newCfg store.Config) (ConfigReport, error)
}

// Handler handles tool calls, dispatching each incoming call to the
// per-user Store resolved from the call's Username. The resolver is
// injected so the tools package does not need to import the
// registry package (which would create an awkward dependency
// hierarchy in tests and future refactors). seen deduplicates the
// first-call RecordConnectionOpen per (username, MCP session) pair.
type Handler struct {
	resolve          func(username string) (store.Backend, error)
	resolveVault     func(username string) VaultSyncer             // nil when vault disabled
	resolveCompactor func(username string) CompactorHealthReporter // nil when compactor health not wired
	cfgCtl           ConfigController                              // nil when mnemo_config disabled
	seen             sync.Map
}

// NewHandler creates a tool handler that resolves each call's
// backing Store by username via the supplied resolver. Main wires
// this up to Registry.ForUser.
func NewHandler(resolve func(string) (store.Backend, error)) *Handler {
	return &Handler{resolve: resolve}
}

// SetVaultResolver configures a per-user vault syncer resolver. Calling
// this is optional; when not called (or when the resolver returns nil)
// the vault tools report "vault not configured".
func (h *Handler) SetVaultResolver(fn func(string) VaultSyncer) {
	h.resolveVault = fn
}

// SetCompactorResolver configures a per-user compactor health
// resolver. Calling this is optional; when not called (or when the
// resolver returns nil) mnemo_compactor_status reports that the
// watcher's runtime state is not available (typically the daemon's
// startup hasn't completed yet for the calling user).
func (h *Handler) SetCompactorResolver(fn func(string) CompactorHealthReporter) {
	h.resolveCompactor = fn
}

// SetConfigController wires the mnemo_config tool to a live config
// source. Calling this is optional; when not called, mnemo_config
// reports that runtime reconfiguration is not available.
func (h *Handler) SetConfigController(c ConfigController) {
	h.cfgCtl = c
}

// callHandler is the per-call delegate that owns the user-resolved
// Store for the lifetime of one tool invocation. Every per-tool
// method is defined on *callHandler so the method bodies read
// naturally as `h.mem.Search(...)` without threading the store
// through every signature. Call() builds the callHandler once and
// dispatches into the switch.
type callHandler struct {
	mem   store.Backend
	cc    CallContext
	vault VaultSyncer     // nil when vault is not configured for this user
	ctx   context.Context // request context; honours MCP-caller cancellation
}

// Definitions returns the MCP tool definitions.
// These are served to the proxy via the ListTools RPC method.
func Definitions() []mcp.Tool {
	return []mcp.Tool{
		mcp.NewTool("mnemo_search",
			mcp.WithDescription(`Search across Claude Code session transcripts. Uses FTS5 full-text search with fuzzy matching.

Plain word queries use OR matching — "QR code pairing protocol" finds messages containing ANY of those words, ranked by how many match (BM25). This means partial matches surface instead of returning nothing. Messages matching more/rarer terms rank higher.

For exact matching, use explicit FTS5 operators:
- Require all terms: "QR AND transfer"
- Exact phrase: "\"QR transfer\""
- Exclude terms: "QR NOT test"
- Proximity: NEAR(QR transfer, 5)

By default searches only interactive sessions (excludes subagents, worktrees, ephemeral). Noise messages (interrupts, compaction summaries, tool-loaded markers) are excluded from the index.`),
			mcp.WithString("query", mcp.Required(), mcp.Description("Search query — plain words use OR (fuzzy). Use AND/NOT/NEAR/quotes for precise control.")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
			mcp.WithString("session_type", mcp.Description(`Filter by session type (default "interactive"). Values: "interactive", "subagent", "worktree", "ephemeral", "all"`)),
			mcp.WithString("repo", mcp.Description(`Filter by repo. Flexible matching against session working directory and extracted repo name. Accepts: bare name ("mnemo"), org/repo ("marcelocantos/mnemo"), host/org/repo ("github.com/marcelocantos/mnemo"), or a path fragment ("~/work/myproject").`)),
			mcp.WithNumber("context_before", mcp.Description("Number of messages before each hit to include (default 3)")),
			mcp.WithNumber("context_after", mcp.Description("Number of messages after each hit to include (default 3)")),
			mcp.WithString("context_filter", mcp.Description(`Filter for context messages. "substantive" (default): only non-noise user/assistant messages. "all": include everything (tool calls, system messages, noise).`)),
		),
		mcp.NewTool("mnemo_sessions",
			mcp.WithDescription("List transcript sessions, sorted by most recent activity. By default shows only interactive sessions with at least 6 substantive messages."),
			mcp.WithString("session_type", mcp.Description(`Filter by session type (default "interactive"). Values: "interactive", "subagent", "worktree", "ephemeral", "all"`)),
			mcp.WithNumber("min_messages", mcp.Description("Minimum substantive (non-noise) messages to include (default 6)")),
			mcp.WithNumber("limit", mcp.Description("Max sessions to return (default 30)")),
			mcp.WithString("project", mcp.Description("Filter by project name substring")),
			mcp.WithString("repo", mcp.Description("Filter by repo (org/name substring, e.g. \"marcelocantos/mnemo\")")),
			mcp.WithString("work_type", mcp.Description(`Filter by work type: "development", "feature", "bugfix", "refactor", "chore", "docs", "test", "ci", "release", "review", "branch-work"`)),
		),
		mcp.NewTool("mnemo_read_session",
			mcp.WithDescription("Read messages from a specific session transcript. Returns messages ordered chronologically."),
			mcp.WithString("session_id", mcp.Required(), mcp.Description("Session ID (the JSONL filename stem, or a prefix)")),
			mcp.WithString("role", mcp.Description(`Filter by role: "user" or "assistant". Omit for all roles.`)),
			mcp.WithNumber("offset", mcp.Description("Skip first N messages (default 0)")),
			mcp.WithNumber("limit", mcp.Description("Max messages to return (default 50)")),
		),
		mcp.NewTool("mnemo_query",
			mcp.WithDescription(`Run a read-only query against the transcript database.

Accepts plain SQL (SELECT/WITH) or sqldeep nested syntax for hierarchical JSON output.

sqldeep example — repos with their recent sessions:
  FROM session_meta sm
  JOIN session_summary ss ON ss.session_id = sm.session_id
  WHERE ss.last_msg >= datetime('now', '-7 days')
    AND ss.session_type = 'interactive'
  SELECT {
    sm.repo,
    sessions: FROM session_summary s
      WHERE s.session_id = sm.session_id
      SELECT { s.session_id, s.last_msg, s.substantive_msgs, },
  }
  GROUP BY sm.repo

Tables:
  entries (id, session_id, project, type, timestamp, raw)
    — every JSONL line stored as JSONB in 'raw'. Virtual columns:
      model, stop_reason, input_tokens, output_tokens,
      cache_read_tokens, cache_creation_tokens, agent_id, version,
      slug, is_sidechain, data_type, data_command, data_hook_event,
      top_tool_use_id, parent_tool_use_id
    — entry types: user, assistant, progress, system, file-history-snapshot
    — use json_extract(raw, '$.path') for fields without virtual columns
  messages (id, entry_id, session_id, project, role, text, timestamp, type, is_noise)
    — content blocks from user/assistant entries. entry_id links to entries.
    — tool_use fields: tool_name, tool_use_id, tool_input (JSONB), content_type
    — virtual columns from tool_input: tool_file_path, tool_command, etc.
  messages_fts — FTS5 virtual table (excludes noise). Use: WHERE messages_fts MATCH 'terms'
  snapshot_files (id, entry_id, session_id, file_path, backup_time)
    — auto-extracted from file-history-snapshot entries via trigger
  snapshot_files_fts — FTS5 on file_path. Use: WHERE snapshot_files_fts MATCH 'pattern'
  sessions — view: session_id, project, session_type, total_msgs, substantive_msgs, first_msg, last_msg
  session_meta (session_id, repo, cwd, git_branch, work_type, topic)
  session_summary (session_id, project, session_type, total_msgs, substantive_msgs, first_msg, last_msg)
  memories (id, project, file_path, name, description, memory_type, content, updated_at)
    — auto-memory files from ~/.claude/projects/*/memory/*.md
    — memory_type: user, feedback, project, reference
  memories_fts — FTS5 on name, description, content, project
  skills (id, file_path, name, description, content, updated_at)
    — skill files from ~/.claude/skills/*.md
  skills_fts — FTS5 on name, description, content
  claude_configs (id, repo, file_path, content, updated_at)
    — CLAUDE.md project instruction files from all repo roots
  claude_configs_fts — FTS5 on content, repo
  audit_entries (id, repo, file_path, date, skill, version, summary, raw_text)
    — parsed entries from docs/audit-log.md in each repo
    — skill: release, audit, docs, etc. version: vN.N.N if present
  audit_entries_fts — FTS5 on summary, raw_text, repo
  ci_runs (id, repo, run_id, workflow, branch, commit_sha, status, conclusion, started_at, completed_at, log_summary, url)
    — GitHub Actions runs polled from repos seen in session history
    — status: completed, in_progress, queued; conclusion: success, failure, cancelled, skipped
  ci_runs_fts — FTS5 on repo, workflow, branch, log_summary, conclusion

Join pattern — message with its entry metadata:
  SELECT m.text, e.model, e.input_tokens FROM messages m JOIN entries e ON e.id = m.entry_id

Token usage query:
  SELECT date(timestamp) AS day, SUM(input_tokens) AS input, SUM(output_tokens) AS output
  FROM entries WHERE type = 'assistant' GROUP BY day ORDER BY day DESC

File history — which sessions touched a file:
  SELECT sf.session_id, sf.backup_time, sm.repo
  FROM snapshot_files sf JOIN session_meta sm ON sm.session_id = sf.session_id
  WHERE sf.file_path LIKE '%store.go'

Session types (derived from project path): interactive, subagent, worktree, ephemeral.
is_noise = 1 for interrupts, compaction summaries, tool-loaded markers, slash command markup.
Results capped at 100 rows.

Tip: If you find yourself running the same complex query pattern repeatedly, save it as a template with mnemo_define for reuse.`),
			mcp.WithString("query", mcp.Required(), mcp.Description("SQL SELECT/WITH query, or sqldeep nested syntax (FROM ... SELECT { ... })")),
		),
		mcp.NewTool("mnemo_repos",
			mcp.WithDescription(`List repositories that have been worked on in Claude Code sessions. Returns, per repo: name, filesystem path, session count, last session activity, last git commit date, and a one-line summary derived from the repo's root CLAUDE.md (first non-blank, non-heading sentence, capped at ~120 chars).

This is the canonical at-a-glance "what repos do I have and what is each one about?" view — sufficient to replace an externally-generated active-projects.md or similar overview file. Summaries refresh automatically when CLAUDE.md is re-indexed.`),
			mcp.WithString("filter", mcp.Description(`Optional filter. Supports: bare name ("mnemo"), org/repo ("marcelocantos/mnemo"), path fragment ("/work/github"), or glob ("marcelocantos/sql*"). Omit to list all repos.`)),
		),
		mcp.NewTool("mnemo_stats",
			mcp.WithDescription("Show transcript index statistics — sessions and messages broken down by session type, with noise vs substantive counts."),
		),
		mcp.NewTool("mnemo_recent_activity",
			mcp.WithDescription("Recent session activity grouped by repo. Returns per-repo JSON with session count, message count, last activity time, work types, and key topics. Useful for understanding where active work is happening across projects."),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 7)")),
			mcp.WithString("repo", mcp.Description(`Filter by repo. Accepts: bare name ("mnemo"), org/repo ("marcelocantos/mnemo"), or path fragment.`)),
		),
		mcp.NewTool("mnemo_status",
			mcp.WithDescription(`Rich status report of recent work across repos. Returns repos → sessions → conversation excerpts with drill-down offsets.

User messages are shown in full. Assistant messages are truncated (default 200 chars). Each message includes its database ID — use mnemo_read_session with offset to retrieve the full text.

Use this when you need context about recent work: the user references prior discussions, you need to understand project history before making decisions, or you want to know what's been happening across repos. Don't dump the output to the user — use it to inform your own understanding.`),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 7)")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment")),
			mcp.WithNumber("max_sessions", mcp.Description("Max sessions per repo (default 3)")),
			mcp.WithNumber("max_excerpts", mcp.Description("Max message excerpts per session (default 20, most recent kept)")),
			mcp.WithNumber("truncate_len", mcp.Description("Truncate assistant messages to this length (default 200)")),
		),
		mcp.NewTool("mnemo_memories",
			mcp.WithDescription(`Search across Claude Code auto-memory files from all projects. Memories are structured notes with frontmatter (name, description, type) that agents save across sessions.

Memory types: "user" (role/preferences), "feedback" (corrections/confirmations), "project" (ongoing work context), "reference" (pointers to external systems).

Use this to find decisions, preferences, and context captured in any project — even when working in a different repo. Also queryable via mnemo_query against the memories table.`),
			mcp.WithString("query", mcp.Description("Search query (uses same fuzzy OR matching as mnemo_search). Omit to list all.")),
			mcp.WithString("type", mcp.Description(`Filter by memory type: "user", "feedback", "project", "reference"`)),
			mcp.WithString("project", mcp.Description("Filter by project name substring")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_get_memory",
			mcp.WithDescription(`Return the raw markdown content of a named memory file, or list available memories when name is omitted.

Replaces the cat ~/.claude/projects/<project>/memory/<name>.md workaround. Project is matched as a substring (consistent with mnemo_memories). Name is matched case-insensitively against the frontmatter name field and the file base name (without .md extension).

When name is omitted, returns a list of all memories for the project with their name, type, and description.`),
			mcp.WithString("project", mcp.Required(), mcp.Description(`Project to look up. Accepts bare name ("mnemo"), org/repo fragment ("marcelocantos/mnemo"), or path fragment ("-Users-marcelo-work-github-com-marcelocantos-mnemo").`)),
			mcp.WithString("name", mcp.Description("Memory name to retrieve (matches frontmatter name or file stem). Omit to list available memories.")),
		),
		mcp.NewTool("mnemo_usage",
			mcp.WithDescription(`Token usage analytics across sessions. Aggregates input, output, cache read, and cache creation tokens with cost estimates.

Returns per-period breakdown and totals. Cost estimates use published Anthropic pricing (Opus, Sonnet, Haiku families). Unknown models use Sonnet pricing as fallback.

Each row includes a "source" field: "estimated" (computed from token counts), "reconciled" (authoritative cost from Anthropic Admin API), or "mixed" (aggregation spans both). Reconciliation requires ANTHROPIC_ADMIN_API_KEY env var; absent by default (all rows report "estimated").

A top-level "freshness" field reports the RFC3339 timestamp of the most recently ingested assistant message, bounding indexer lag for real-time consumers.`),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 30). Ignored when since/until are supplied.")),
			mcp.WithString("since", mcp.Description("RFC3339 timestamp lower bound (inclusive). Overrides days when set.")),
			mcp.WithString("until", mcp.Description("RFC3339 timestamp upper bound (inclusive). Defaults to now when only since is set.")),
			mcp.WithString("repo", mcp.Description(`Filter by repo. Accepts: bare name ("mnemo"), org/repo ("marcelocantos/mnemo"), or path fragment.`)),
			mcp.WithString("model", mcp.Description(`Filter by model prefix (e.g. "claude-opus-4", "claude-sonnet-4")`)),
			mcp.WithString("group_by", mcp.Description(`Group results by: "day" (default), "model", "repo", "session" (one row per Claude Code session ID), or "block" (one row per 5-hour Anthropic billing block, boundaries aligned to UTC and matching what /cost and ccusage report).`)),
		),
		mcp.NewTool("mnemo_skills",
			mcp.WithDescription(`Search across Claude Code skill files (~/.claude/skills/). Skills define reusable workflows — release processes, audit procedures, documentation generation, etc. Use this to discover relevant skills or understand what workflows are available.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching). Omit to list all.")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_configs",
			mcp.WithDescription(`Search across CLAUDE.md project instruction files from all repos. These files contain build instructions, conventions, delivery definitions, and project-specific agent guidance. Use this to understand how other projects are configured or to find cross-project patterns.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching). Omit to list all.")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_audit",
			mcp.WithDescription(`Search across audit logs (docs/audit-log.md) from all repos. Audit logs record maintenance activities: releases, audits, documentation runs. Use this to check when a project was last released, find maintenance patterns across repos, or review past audit findings.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching). Omit to list all.")),
			mcp.WithString("repo", mcp.Description("Filter by repo name")),
			mcp.WithString("skill", mcp.Description("Filter by skill name (e.g. 'release', 'audit')")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_targets",
			mcp.WithDescription(`Search across convergence targets (docs/targets.md) from all repos. Targets track desired states — features to build, bugs to fix, quality gaps to close. Use this to find targets across projects, check what's active/achieved, or discover cross-project priorities.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching). Omit to list all.")),
			mcp.WithString("repo", mcp.Description("Filter by repo name")),
			mcp.WithString("status", mcp.Description("Filter by status: identified, converging, achieved")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_plans",
			mcp.WithDescription(`Search across implementation plans (.planning/ directories) from all repos. Plans contain architectural decisions, task breakdowns, and implementation reasoning from GSD workflows. Use this to find past design decisions or understand how features were planned.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching). Omit to list all.")),
			mcp.WithString("repo", mcp.Description("Filter by repo name")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_docs",
			mcp.WithDescription(`Search across project documentation files (markdown, plain-text, PDF) indexed from all tracked repos. Covers README, CHANGELOG, design notes, and any files under docs/, design/, notes/, papers/ directories. Deduplicates .md/.pdf pairs with same stem — always prefers .md. Use this to find project documentation, design decisions, and release notes across repos.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching). Omit to list recent.")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment")),
			mcp.WithString("kind", mcp.Description("Filter by file kind: md, txt, pdf")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_synthesis",
			mcp.WithDescription(`Search across synthesis documents — analysis, research, design, and planning artifacts that follow the four-dir taxonomy (docs/{papers,design,analysis,plans}) plus docs/audit-log.md and docs/convergence-report.md. Indexed from workspace repos and additional synthesis roots (e.g. ~/think).

Use this instead of mnemo_docs when you want a global view of the user's thinking: cross-repo research themes, recurring design decisions, target retros, external-material summaries. Results include the inferred taxonomy, inline metadata (Date, Status, Target, Source) when present, and the full document content.

Taxonomy values: paper | design | analysis | plans | audit-log | convergence-report.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching). Omit to list recent.")),
			mcp.WithString("taxonomy", mcp.Description("Filter by taxonomy: paper, design, analysis, plans, audit-log, convergence-report")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_who_ran",
			mcp.WithDescription(`Find sessions that ran a specific shell command. Searches Bash tool_use entries by command pattern, returning session ID, repo, matched command, and timestamp. Useful for tracing when and where a command was last executed across all sessions.`),
			mcp.WithString("pattern", mcp.Required(), mcp.Description("Command substring to match (LIKE match, case-insensitive)")),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 30)")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_permissions",
			mcp.WithDescription(`Analyze tool usage patterns across sessions to suggest allowedTools rules for settings.json.

Returns the most frequently used tools with counts and concrete suggestions for permission rules. Also analyzes Bash command patterns to suggest fine-grained Bash permissions (e.g., "Bash(go *)", "Bash(git *)").

Use this to understand which tools agents use most and to tighten permissions without blocking common workflows.`),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 30)")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment")),
			mcp.WithNumber("limit", mcp.Description("Max results per category (default 20)")),
		),
		mcp.NewTool("mnemo_backup_status",
			mcp.WithDescription(`List existing backups of ~/.mnemo/mnemo.db with size, age, and tag (daily / pre-migration / manual).

Backups are taken by the daemon's periodic worker (03:00–04:00 local during a quiescent window), by the migration path (before any sqlift.Apply), or manually via mnemo_backup_now. The retention pool is shared across all tags — older backups are GC'd after each daily run.

Returns the newest first. Restore is manual:
  gunzip -c ~/.mnemo/backups/<file>.db.gz > ~/.mnemo/mnemo.db.restored
  brew services stop marcelocantos/tap/mnemo
  mv ~/.mnemo/mnemo.db ~/.mnemo/mnemo.db.bak
  mv ~/.mnemo/mnemo.db.restored ~/.mnemo/mnemo.db
  brew services start marcelocantos/tap/mnemo`),
		),
		mcp.NewTool("mnemo_backup_now",
			mcp.WithDescription(`Trigger an immediate backup of ~/.mnemo/mnemo.db. Tagged "manual" so retention GC distinguishes it from automatic dailies.

Idempotency: skips if any backup was taken within the last hour, unless force=true. The VACUUM INTO + gzip step takes ~1-2 minutes on a multi-GB DB; the call blocks until the snapshot is on disk.

Useful before risky operations (manual schema edits, large deletes via mnemo_query, etc.).`),
			mcp.WithBoolean("force", mcp.Description("Force a new snapshot even if a recent backup exists (default false)")),
		),
		mcp.NewTool("mnemo_prs",
			mcp.WithDescription(`Search GitHub PRs and issues across all indexed repos. Uses FTS5 for keyword search on titles and bodies. Data is polled from GitHub repos that appear in session history and backfilled at startup.

Supports filtering by state, author, and recency. Results include both PRs and issues unless filtered by type.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching on title/body). Omit to list recent.")),
			mcp.WithString("repo", mcp.Description("Filter by repo (e.g. 'mnemo', 'marcelocantos/mnemo')")),
			mcp.WithString("state", mcp.Description("Filter by state: open, closed, merged (PRs only), all (default)")),
			mcp.WithString("author", mcp.Description("Filter by author username")),
			mcp.WithString("type", mcp.Description("Filter by type: pr, issue, all (default)")),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 30)")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_ci",
			mcp.WithDescription(`Search CI/CD run history across repos. Indexes GitHub Actions runs from all repos that appear in session history.

Supports FTS search across workflow names, branches, and failure logs. Use this to:
- Find recent CI failures across projects
- Check if a specific repo's CI is green
- Search failure logs for error patterns
- Correlate CI runs with development sessions

Runs are polled incrementally from GitHub Actions. Failed run logs are indexed for full-text search.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching against workflow, branch, logs). Omit to list recent runs.")),
			mcp.WithString("repo", mcp.Description("Filter by repo (e.g. 'mnemo', 'marcelocantos/mnemo')")),
			mcp.WithString("conclusion", mcp.Description("Filter by conclusion: success, failure, cancelled, skipped")),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 30)")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_commits",
			mcp.WithDescription(`Search git commits across all indexed repos. Uses FTS5 for keyword search on commit messages. Commits are indexed automatically from repos that appear in session history. Supports cross-repo queries with date range filtering.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching on subject/body). Omit to list recent.")),
			mcp.WithString("repo", mcp.Description("Filter by repo (e.g. 'mnemo', 'marcelocantos/mnemo')")),
			mcp.WithString("author", mcp.Description("Filter by author name or email substring")),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 30)")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_decisions",
			mcp.WithDescription(`Search past decisions across all sessions. Decisions are automatically detected from proposal + confirmation patterns in conversations (e.g., assistant proposes an approach, user confirms with "yes", "go ahead", "lgtm"). Use this to recall what was decided and why.`),
			mcp.WithString("query", mcp.Description("Search query (fuzzy OR matching). Omit to list recent.")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment")),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 30)")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
		),
		mcp.NewTool("mnemo_restore",
			mcp.WithDescription(`Return the compacted context for a session chain — all compaction summaries across the full /clear-bounded chain, oldest first.

Use this at the start of a session to restore context from a previous run. Given any session ID in a chain (including the current one), it returns the structured summaries (targets, decisions, files touched, open threads) produced by the background compactor across all segments of that chain.

Returns nothing if no compactions have been produced yet (the background compactor runs every 5 minutes on active sessions).`),
			mcp.WithString("session_id", mcp.Required(), mcp.Description("Any session ID in the chain (or a prefix).")),
		),
		mcp.NewTool("mnemo_chain",
			mcp.WithDescription(`Retrieve the /clear-bounded session chain for any session ID.

Session chain detection has two layers:
  - DEFINITIVE: rows in session_chains written live by the daemon
    when a proxy connection observes successive sessions. These
    carry mechanism='mcp_connection', confidence='definitive'.
  - HEURISTIC: query-time inference for sessions the daemon never
    saw live (first installs, daemon downtime, sessions that never
    called a mnemo tool). Uses the cwd-most-recent rule.

mode:
  - "auto" (default) — returns the definitive chain; if the query
    session has no definitive predecessor, surfaces heuristic
    candidates.
  - "strict" — definitive only; empty when no connection observed
    the rollover.
  - "candidates" — always returns both definitive rows and
    heuristic candidates, labelled by mechanism, so the caller can
    see ambiguity explicitly.`),
			mcp.WithString("session_id", mcp.Required(), mcp.Description("Any session ID in the chain (or a prefix)")),
			mcp.WithString("mode", mcp.Description(`"auto" (default), "strict", or "candidates".`)),
		),
		mcp.NewTool("mnemo_whatsup",
			mcp.WithDescription(`Report which active Claude Code sessions are doing expensive work right now.

Shows per-session CPU%, RSS memory, CPU time, cwd, and resolved transcript path alongside system-wide memory pressure. Cross-references live session PIDs with session metadata (repo, topic, work type) and reads PWD from each process's environment. Results are sorted by CPU% descending so the busiest session appears first.

Use postmortem=true when no live sessions are detected (e.g. after a machine crash) to recover which directories had recent Claude activity based on transcript file mtimes within the last 24 hours.

Use this to answer "what is Claude doing right now?" — especially useful when the machine is hot or fans are spinning.`),
			mcp.WithBoolean("postmortem", mcp.Description("When true and no live sessions exist, report directories with recent Claude activity from transcript mtimes (last 24h).")),
		),
		mcp.NewTool("mnemo_self",
			mcp.WithDescription(`Discover the calling session's ID. Two-phase protocol:

Phase 1: Call with no arguments. Returns a unique nonce. This nonce appears in your transcript as the tool response.
Phase 2: Call again with the nonce. mnemo searches for the session containing it and returns your session ID.

Example: call mnemo_self → get nonce "mnemo:abc123". Call mnemo_self with nonce "mnemo:abc123" → get your session ID. Then use mnemo_read_session to read your own transcript.`),
			mcp.WithString("nonce", mcp.Description("The nonce returned by a previous mnemo_self call. Omit on first call to generate a new nonce.")),
		),
		mcp.NewTool("mnemo_define",
			mcp.WithDescription(`Define a reusable parameterised query template. Templates persist across sessions in SQLite. Use {{param_name}} placeholders in the query. If a template with the same name exists, it is updated.`),
			mcp.WithString("name", mcp.Required(), mcp.Description("Template name (unique identifier)")),
			mcp.WithString("description", mcp.Description("What this template does")),
			mcp.WithString("query", mcp.Required(), mcp.Description("SQL query with {{param}} placeholders")),
			mcp.WithArray("params", mcp.Description("List of parameter names referenced in the query (e.g. [\"days\", \"repo\"])")),
		),
		mcp.NewTool("mnemo_evaluate",
			mcp.WithDescription(`Execute a named query template with parameters. Returns results in the same format as mnemo_query.`),
			mcp.WithString("name", mcp.Required(), mcp.Description("Template name")),
			mcp.WithObject("params", mcp.Description(`Parameter values as key-value pairs (e.g. {"days": "7", "repo": "mnemo"})`)),
		),
		mcp.NewTool("mnemo_list_templates",
			mcp.WithDescription(`List all saved query templates with their names, descriptions, and parameter definitions.`),
		),
		mcp.NewTool("mnemo_discover_patterns",
			mcp.WithDescription(`Analyze transcript history to discover workaround patterns that suggest missing mnemo features.

Detects:
- direct_jsonl_read: Bash commands that read JSONL transcript files directly (bypassing mnemo)
- transcript_grep: grep/rg over transcript directories instead of using mnemo_search
- repeated_query: mnemo_query shapes repeated across 3+ sessions (candidates for templates)
- repeated_search: mnemo_search patterns repeated across 3+ sessions (may warrant dedicated tools)

Returns candidate features with evidence counts, example sessions, and suggested actions.`),
			mcp.WithNumber("days", mcp.Description("Recency window in days (default 90)")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment")),
			mcp.WithNumber("min_occurrences", mcp.Description("Minimum pattern occurrences to report (default 3)")),
		),
		mcp.NewTool("mnemo_session_structure",
			mcp.WithDescription(`Return a structural summary of a session's entry types and content-block shapes.

Answers "what is in this session?" without reading full transcript text. Returns:
- entry_types: count per JSONL entry type (user, assistant, system, progress, ...)
- assistant_stop_reasons: count per stop_reason (end_turn, tool_use, max_tokens, ...)
- system_subtypes: count per $.subtype for system entries
- content_block_kinds: count per content-block type (text, tool_use, tool_result, thinking)
- tool_names: count per tool name in tool_use blocks

Use this to quickly understand a session's shape before deep-reading it, to compare session structures, or to verify that a session contains the content type you expect.`),
			mcp.WithString("session_id", mcp.Required(), mcp.Description("Session ID (exact or prefix, consistent with mnemo_read_session)")),
		),
		mcp.NewTool("mnemo_locate_uuid",
			mcp.WithDescription(`Locate an entry by UUID across all sessions. Searches every UUID field in the transcript index and returns the session, entry, and which field matched.

UUID fields searched:
  - entry_uuid       — the entry's own $.uuid
  - parent_uuid      — the parent entry's UUID ($.parentUuid)
  - top_tool_use_id  — entry-level tool use ID ($.toolUseID, for progress/result entries)
  - parent_tool_use_id — entry-level parent tool use ID ($.parentToolUseID)
  - tool_use_id      — a tool_use content block's id inside $.message.content
  - tool_result_id   — a tool_result content block's tool_use_id inside $.message.content

Supports partial UUID prefixes — the first 8 characters are usually enough to uniquely identify an entry.

Returns a structured result with session_id, entry_id, entry type, timestamp, match_kind (which field matched), the full matched UUID, and surrounding context messages. Returns "not found" when no entry matches.`),
			mcp.WithString("uuid", mcp.Required(), mcp.Description("Full UUID or prefix to locate (e.g. \"abc12345\" or the full UUID)")),
			mcp.WithNumber("context_before", mcp.Description("Number of messages before the entry to include (default 3)")),
			mcp.WithNumber("context_after", mcp.Description("Number of messages after the entry to include (default 3)")),
		),
		mcp.NewTool("mnemo_images",
			mcp.WithDescription(`Search images captured from Claude Code transcripts. Three search modes: (1) text (default) — FTS5 over AI descriptions and OCR text; (2) semantic — embed the query text and find images by meaning using CLIP k-NN (requires embed backend); (3) similar — find visually similar images given an image ID. Use 'text' to find images by paraphrase, 'semantic' for conceptual matches like "architecture diagram", and 'similar' to browse related screenshots.`),
			mcp.WithString("query", mcp.Description("Search query. Used in 'text' and 'semantic' modes. Omit to list recent (text mode).")),
			mcp.WithString("mode", mcp.Description(`Search mode: "text" (FTS5 over descriptions + OCR, default), "semantic" (embed query, k-NN on CLIP vectors), or "similar" (visual similarity to another image, requires similar_to).`)),
			mcp.WithNumber("similar_to", mcp.Description("Image ID to find visually similar images (used with mode='similar').")),
			mcp.WithString("repo", mcp.Description("Filter by repo (session's repo)")),
			mcp.WithString("session", mcp.Description("Filter by session ID prefix")),
			mcp.WithNumber("days", mcp.Description("Recency window (default 90)")),
			mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
			mcp.WithString("search_fields", mcp.Description(`For text mode: which indexes to search: "both" (default), "description" (AI descriptions only), or "ocr" (extracted text only).`)),
		),
		mcp.NewTool("mnemo_tool_result",
			mcp.WithDescription(`Return the raw tool-result payload for a specific tool invocation.

Looks up the tool-result body stored at ingest time, identified by its tool_use_id within a session. Replaces the cat ~/.claude/projects/<project>/<session>/tool-results/<id>.txt workaround.

Returns the full text, is_error flag, and total byte length. Use offset and truncate_len to page through large payloads.`),
			mcp.WithString("session_id", mcp.Required(), mcp.Description("Session ID (or unique prefix) containing the tool invocation.")),
			mcp.WithString("tool_use_id", mcp.Required(), mcp.Description("The tool_use_id of the tool invocation whose result you want.")),
			mcp.WithNumber("offset", mcp.Description("Skip the first N bytes of the result text (default 0).")),
			mcp.WithNumber("truncate_len", mcp.Description("Maximum bytes to return (default: no limit). Use with offset to page.")),
		),
		mcp.NewTool("mnemo_rework_history",
			mcp.WithDescription(`Return prior rework attempts for a bullseye target, ordered most-recent first.

Each result is one compaction span (a background-summarised session segment) in which the target appeared as actively worked on (in targets_active or targets_progressed). Provides: session ID, timestamp, repo, the per-target progress note if the compactor recorded one, the prose summary of the span, and any open threads left unresolved.

Use this to build a rework diagnosis context: the bullseye_rework tool accepts the output as its mnemo_history parameter so the rework agent sees what was tried before, what failed, and what was left open — avoiding repeating the same failed approaches.`),
			mcp.WithString("target_id", mcp.Required(), mcp.Description("Bullseye target ID (e.g. \"T1.5\"). Exact match against targets_active and targets_progressed keys.")),
			mcp.WithString("repo", mcp.Description("Filter by repo name or path fragment (optional).")),
			mcp.WithNumber("limit", mcp.Description("Max attempts to return (default 20).")),
		),
		mcp.NewTool("mnemo_vault_sync",
			mcp.WithDescription(`Synchronise the vault: write or update Markdown notes for every session, decision, memory, plan, target, CI run, and PR in the knowledge graph, then re-ingest the vault directory so human-added notes and edits are searchable.

Notes whose vault file is already up to date (file mtime > entity timestamp) are skipped, making repeated syncs fast.

Human content added below the <!-- mnemo:generated --> fence in any vault note is preserved across re-syncs and is indexed by mnemo's FTS5 search, feeding back into mnemo's associations.

Vault must be configured via vault_path in ~/.mnemo/config.json.`),
		),
		mcp.NewTool("mnemo_vault_status",
			mcp.WithDescription("Report vault configuration: whether vault is enabled, the vault root path, the active indexing scope (vault_indexing_scope) with its includes and .mnemoignore file state, the resolved vault_layout (v1/both/v2) with soak counter and migration recommendation, and a count of notes on disk by section."),
		),
		mcp.NewTool("mnemo_vault_migration_doc",
			mcp.WithDescription(`Return or rewrite the v1→v2 layout MIGRATION.md snapshot.

Modes:
  - write=false (default): return the current state-of-vault snapshot as text without touching the filesystem. Use this to inspect what would be written without persisting.
  - write=true: idempotently render the snapshot to <vault>/_mnemo/MIGRATION.md, overwriting any existing file. Use this to restore the doc after deleting it, or to refresh it after the v1 layout state changes.

MIGRATION.md is normally write-once: mnemo creates it the first sync that observes a v1 layout and never regenerates it. Deleting it tells mnemo "I have read this; move on." This tool is the user-initiated escape hatch when you want the doc back.

Requires vault_path to be configured.`),
			mcp.WithBoolean("write", mcp.Description("If true, persist the snapshot to <vault>/_mnemo/MIGRATION.md. Default: false (snapshot only).")),
		),
		mcp.NewTool("mnemo_compactor_status",
			mcp.WithDescription(`Report the live state of mnemo's background session compactor (🎯T67). Returns:

  - Last scan timestamp and number of candidates the scan returned.
  - Last tick timestamp and its outcome (one of: compacted,
    nothing_to_compact, budget_exceeded, failed, timeout, skipped_self).
  - In-flight session ID, if a tick is currently running.
  - Lifetime counts of each tick outcome since the daemon started.
  - Configuration in effect: scan interval, idle timeout, per-tick
    timeout, minimum delta-messages trigger, per-scan compaction cap,
    and the configured max token-budget ratio.
  - Backlog: the count of owed-but-uncompacted sessions (the gap to
    the compactor's fixed point).

Use this to answer "is the compactor working?" without grepping the
daemon log. A LastScanAt that is older than ScanInterval × 2 is the
clearest "watcher is wedged" signal.`),
		),
		mcp.NewTool("mnemo_divergence",
			mcp.WithDescription(`Report, per derived data stream, the gap between desired and actual state — how far each stream is from its convergence fixed point (🎯T68.4).

For each stream returns: whether a gap metric is known, the gap (in the stream's unit; 0 = converged), when it last reconciled, and a note. Streams with a cheap metric today: compactions (owed-but-uncompacted sessions), transcript_index (un-ingested transcript bytes), and the repo-level document streams (docs:* — files on disk not yet indexed). Streams not yet instrumented (images, vault, github_mirrors) report known=false rather than a fabricated number; each becomes known as its reconciler slice lands.

Use this to see what derived state is stale and by how much — the single surface for "is anything behind?" across the data plane.`),
		),
		mcp.NewTool("mnemo_source_drift",
			mcp.WithDescription(`Report indexed transcript sources that have been pruned or truncated out from under the index (🎯T68.6).

Returns counts of "deleted" (the source .jsonl no longer exists) and "truncated" (its current size is below the ingested offset — pruned or rewritten shorter), plus example paths. Under mnemo's durable-tier model this is NOT an error: Claude Code prunes transcripts, and the index is the authoritative durable copy of that content. This surface exists so you can see how much indexed content no longer has a live source — informational, not a reconcile gap.`),
		),
		mcp.NewTool("mnemo_vault_gc",
			mcp.WithDescription(`Inspect (and optionally clean up) vault GC orphans (🎯T68.6).

Two orphan classes are reported, both via exact set-difference over the vault_outputs manifest:
  - manifest_path_missing: manifest rows whose note_path is not on disk anymore (the user or another process removed the file). With confirm=true, the GC removes these manifest rows (no filesystem action).
  - disk_not_in_manifest: *.md files under the vault with no manifest entry. INFORMATIONAL ONLY in this version — the tool reports them but never deletes them (user content lives here; deletion needs higher-level policy + below-fence checks).

Dry-run by default. Setting confirm=true is required to act on manifest_path_missing.`),
			mcp.WithString("vault_path", mcp.Required(), mcp.Description("Absolute path to the vault root.")),
			mcp.WithBoolean("confirm", mcp.Description("If true, remove manifest rows for manifest_path_missing orphans. Default false (dry-run; reports candidates only).")),
		),
		mcp.NewTool("mnemo_config",
			mcp.WithDescription(`Read or update mnemo's runtime configuration (~/.mnemo/config.json).

Modes:
  - op=read (default): return the current effective config as JSON, plus a list of resolved-paths (workspace_roots, vault_path, synthesis_roots) with ~ expanded.
  - op=write: merge "patch" into the current config, validate, persist to disk, and adopt the change in the running daemon.

Patch semantics: patch is a JSON object with the same shape as ~/.mnemo/config.json. Only keys present in the patch are changed; unset keys are left untouched. Array fields are replaced wholesale — to add or remove a single entry, read the current config first and write the full updated array. To clear a field, set it to its zero value (empty string for vault_path, empty array for the slices).

Hot-reload coverage:
  - vault_path: applied live. The existing vault workers stop, a fresh exporter is built at the new path, and an initial sync starts in the background. Set vault_path to "" to disable vault export entirely.
  - workspace_roots, extra_project_dirs, synthesis_roots: applied live; subsequent ingest passes pick up the new roots.
  - linked_instances: persisted to disk but requires a daemon restart to take effect (the federation client is built once at startup).

Response includes which fields changed, which were adopted live, and which require a restart.`),
			mcp.WithString("op", mcp.Description("Operation: \"read\" (default) or \"write\".")),
			mcp.WithObject("patch", mcp.Description("For op=write: object with the keys to update. Same shape as ~/.mnemo/config.json. Omitted keys are left unchanged.")),
		),
	}
}

// Call executes a tool by name with the given arguments.
// Returns (text, isError, err) where isError means a tool-level error
// (returned to the user) vs err which is a transport/system error.
//
// The CallContext carries the MCP session ID (from the Mcp-Session-Id
// header). Most tools ignore it; mnemo_self uses it to bind a Claude
// Code session to its owning MCP session, which the compactor and
// mnemo_restore rely on for /clear-boundary context preservation.
func (h *Handler) Call(ctx context.Context, cc CallContext, name string, args map[string]any) (string, bool, error) {
	mem, err := h.resolve(cc.Username)
	if err != nil {
		return fmt.Sprintf("resolve user %q: %v", cc.Username, err), true, nil
	}
	if cc.MCPSessionID != "" {
		key := cc.Username + "\x00" + cc.MCPSessionID
		if _, loaded := h.seen.LoadOrStore(key, struct{}{}); !loaded {
			mem.RecordConnectionOpen(cc.MCPSessionID, 0, time.Now())
		}
	}
	// Resolve vault syncer for this user (nil when vault not configured).
	var vs VaultSyncer
	if h.resolveVault != nil {
		vs = h.resolveVault(cc.Username)
	}
	ch := &callHandler{mem: mem, cc: cc, vault: vs, ctx: ctx}
	switch name {
	case "mnemo_search":
		return ch.search(args)
	case "mnemo_sessions":
		return ch.sessions(args)
	case "mnemo_read_session":
		return ch.readSession(args)
	case "mnemo_query":
		return ch.query(args)
	case "mnemo_repos":
		return ch.repos(args)
	case "mnemo_recent_activity":
		return ch.recentActivity(args)
	case "mnemo_status":
		return ch.status(args)
	case "mnemo_stats":
		return ch.stats()
	case "mnemo_memories":
		return ch.memories(args)
	case "mnemo_get_memory":
		return ch.getMemory(args)
	case "mnemo_skills":
		return ch.skills(args)
	case "mnemo_usage":
		return ch.usage(args)
	case "mnemo_configs":
		return ch.configs(args)
	case "mnemo_audit":
		return ch.auditLogs(args)
	case "mnemo_targets":
		return ch.targets(args)
	case "mnemo_plans":
		return ch.plans(args)
	case "mnemo_docs":
		return ch.docs(args)
	case "mnemo_synthesis":
		return ch.synthesis(args)
	case "mnemo_backup_status":
		return ch.backupStatus()
	case "mnemo_backup_now":
		return ch.backupNow(args)
	case "mnemo_who_ran":
		return ch.whoRan(args)
	case "mnemo_permissions":
		return ch.permissions(args)
	case "mnemo_prs":
		return ch.prs(args)
	case "mnemo_ci":
		return ch.ci(args)
	case "mnemo_commits":
		return ch.commits(args)
	case "mnemo_decisions":
		return ch.decisions(args)
	case "mnemo_restore":
		return ch.restore(args)
	case "mnemo_chain":
		return ch.chain(args)
	case "mnemo_self":
		return ch.self(args)
	case "mnemo_whatsup":
		return ch.whatsup(args)
	case "mnemo_define":
		return ch.defineTemplate(args)
	case "mnemo_evaluate":
		return ch.evaluateTemplate(args)
	case "mnemo_list_templates":
		return ch.listTemplates()
	case "mnemo_discover_patterns":
		return ch.discoverPatterns(args)
	case "mnemo_locate_uuid":
		return ch.locateUUID(args)
	case "mnemo_images":
		return ch.images(args)
	case "mnemo_session_structure":
		return ch.sessionStructure(args)
	case "mnemo_tool_result":
		return ch.toolResult(args)
	case "mnemo_rework_history":
		return ch.reworkHistory(args)
	case "mnemo_vault_sync":
		return ch.vaultSync()
	case "mnemo_vault_status":
		return ch.vaultStatus(h.cfgCtl)
	case "mnemo_compactor_status":
		return ch.compactorStatus(h.resolveCompactor)
	case "mnemo_divergence":
		return ch.divergence()
	case "mnemo_source_drift":
		return ch.sourceDrift()
	case "mnemo_vault_gc":
		return ch.vaultGC(args)
	case "mnemo_vault_migration_doc":
		return ch.vaultMigrationDoc(args)
	case "mnemo_config":
		return ch.config(args, h.cfgCtl)
	default:
		return "", false, fmt.Errorf("unknown tool: %s", name)
	}
}

func (h *callHandler) search(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	sessionType, _ := args["session_type"].(string)
	repoFilter, _ := args["repo"].(string)
	contextBefore := 3
	if cb, ok := args["context_before"].(float64); ok && cb >= 0 {
		contextBefore = int(cb)
	}
	contextAfter := 3
	if ca, ok := args["context_after"].(float64); ok && ca >= 0 {
		contextAfter = int(ca)
	}
	substantiveOnly := true
	if cf, ok := args["context_filter"].(string); ok && cf == "all" {
		substantiveOnly = false
	}
	if query == "" {
		return "query is required", true, nil
	}

	results, err := h.mem.Search(query, limit, sessionType, repoFilter, contextBefore, contextAfter, substantiveOnly)
	if err != nil {
		return fmt.Sprintf("search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No results found. Try different terms — the content may use different vocabulary than expected.", false, nil
	}

	var b strings.Builder
	for _, r := range results {
		// Vault annotation hits use SessionID to carry the file path; render
		// them as a single "[vault] <path>" header rather than the message
		// format with empty Project/Timestamp/sessionID fields.
		if r.Role == "vault" {
			fmt.Fprintf(&b, ">> [vault] %s\n>> %s\n\n", r.SessionID, r.Text)
			continue
		}
		sid := r.SessionID
		if len(sid) > 8 {
			sid = sid[:8]
		}
		for _, cm := range r.Before {
			fmt.Fprintf(&b, "  [%s] %s\n", cm.Role, cm.Text)
		}
		fmt.Fprintf(&b, ">> [%s] %s | %s | %s | msg:%d\n>> %s\n",
			r.Role, r.Project, sid, r.Timestamp, r.MessageID, r.Text)
		for _, cm := range r.After {
			fmt.Fprintf(&b, "  [%s] %s\n", cm.Role, cm.Text)
		}
		b.WriteByte('\n')
	}
	return b.String(), false, nil
}

func (h *callHandler) sessions(args map[string]any) (string, bool, error) {
	sessionType, _ := args["session_type"].(string)
	minMessages := 6
	if m, ok := args["min_messages"].(float64); ok && m >= 0 {
		minMessages = int(m)
	}
	limit := 30
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	projectFilter, _ := args["project"].(string)
	repoFilter, _ := args["repo"].(string)
	workTypeFilter, _ := args["work_type"].(string)

	sessions, err := h.mem.ListSessions(sessionType, minMessages, limit, projectFilter, repoFilter, workTypeFilter)
	if err != nil {
		return fmt.Sprintf("list sessions failed: %v", err), true, nil
	}
	if len(sessions) == 0 {
		return "No sessions found.", false, nil
	}

	live := h.mem.LiveSessions()

	var b strings.Builder
	for _, si := range sessions {
		sid := si.SessionID
		if len(sid) > 10 {
			sid = sid[:10]
		}
		repo := si.Repo
		if repo == "" {
			repo = "-"
		}
		workType := si.WorkType
		if workType == "" {
			workType = "-"
		}
		lastMsg := si.LastMsg
		if len(lastMsg) > 19 {
			lastMsg = lastMsg[:19]
		}
		topic := si.Topic
		if len(topic) > 80 {
			topic = topic[:77] + "..."
		}
		liveness := ""
		if pid, ok := live[si.SessionID]; ok {
			liveness = fmt.Sprintf("  [LIVE pid=%d]", pid)
		}
		fmt.Fprintf(&b, "%s  %s  %s  %s  %d/%d msgs  %s%s\n",
			sid, repo, workType, lastMsg, si.SubstantiveMsgs, si.TotalMsgs, topic, liveness)
	}
	return b.String(), false, nil
}

func (h *callHandler) readSession(args map[string]any) (string, bool, error) {
	sessionID, _ := args["session_id"].(string)
	if sessionID == "" {
		return "session_id is required", true, nil
	}
	role, _ := args["role"].(string)
	offset := 0
	if o, ok := args["offset"].(float64); ok && o >= 0 {
		offset = int(o)
	}
	limit := 50
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	messages, err := h.mem.ReadSession(sessionID, role, offset, limit)
	if err != nil {
		return fmt.Sprintf("read session failed: %v", err), true, nil
	}
	if len(messages) == 0 {
		return "No messages found for session " + sessionID, false, nil
	}

	var b strings.Builder
	for _, m := range messages {
		marker := ""
		if m.IsNoise {
			marker = " [noise]"
		}
		fmt.Fprintf(&b, "[%s]%s %s\n%s\n\n", m.Role, marker, m.Timestamp, m.Text)
	}
	return b.String(), false, nil
}

func (h *callHandler) query(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return "query is required", true, nil
	}

	rows, err := h.mem.Query(query)
	if err != nil {
		return fmt.Sprintf("query failed: %v", err), true, nil
	}
	if len(rows) == 0 {
		return "No rows returned.", false, nil
	}

	var b strings.Builder
	for _, row := range rows {
		for k, v := range row {
			fmt.Fprintf(&b, "%s: %v  ", k, v)
		}
		b.WriteByte('\n')
	}
	return b.String(), false, nil
}

func (h *callHandler) repos(args map[string]any) (string, bool, error) {
	filter, _ := args["filter"].(string)

	repos, err := h.mem.ListRepos(filter)
	if err != nil {
		return fmt.Sprintf("list repos failed: %v", err), true, nil
	}
	if len(repos) == 0 {
		return "No repos found.", false, nil
	}

	var b strings.Builder
	for _, r := range repos {
		lastActivity := r.LastActivity
		if len(lastActivity) > 19 {
			lastActivity = lastActivity[:19]
		}
		// Date column shows last_commit when available (truer signal
		// for "is this repo alive?"), falling back to last_activity.
		dateCol := r.LastCommit
		dateLabel := "commit"
		if dateCol == "" {
			dateCol = lastActivity
			dateLabel = "session"
		}
		fmt.Fprintf(&b, "%-45s  %4d sessions  %s %s  %s\n",
			r.Repo, r.Sessions, dateLabel, dateCol, r.Path)
		if r.Summary != "" {
			marker := ""
			switch r.SummaryVerdict {
			case "stale":
				marker = "  [stale, reviewed " + r.SummaryReviewedAt + "]"
			case "rewritten":
				marker = "  [needs rewrite, reviewed " + r.SummaryReviewedAt + "]"
			}
			fmt.Fprintf(&b, "    %s%s\n", r.Summary, marker)
		}
	}
	return b.String(), false, nil
}

func (h *callHandler) stats() (string, bool, error) {
	stats, err := h.mem.Stats()
	if err != nil {
		return fmt.Sprintf("stats failed: %v", err), true, nil
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Total: %d sessions, %d messages\n\n", stats.TotalSessions, stats.TotalMessages)
	fmt.Fprintf(&b, "%-12s %8s %10s %12s %8s\n", "Type", "Sessions", "Total Msgs", "Substantive", "Noise")
	fmt.Fprintf(&b, "%s\n", strings.Repeat("-", 55))
	for _, ts := range stats.ByType {
		fmt.Fprintf(&b, "%-12s %8d %10d %12d %8d\n",
			ts.SessionType, ts.Sessions, ts.TotalMsgs, ts.SubstantiveMsgs, ts.NoiseMsgs)
	}

	if len(stats.Streams) > 0 {
		fmt.Fprintf(&b, "\n%-16s %8s %8s %6s  %s\n", "Stream", "Indexed", "On Disk", "Drift", "Last Backfill")
		fmt.Fprintf(&b, "%s\n", strings.Repeat("-", 70))
		for _, st := range stats.Streams {
			drift := st.FilesOnDisk - st.FilesIndexed
			fmt.Fprintf(&b, "%-16s %8d %8d %6d  %s\n",
				st.Stream, st.FilesIndexed, st.FilesOnDisk, drift, st.LastBackfill)
		}
	}

	return b.String(), false, nil
}

func (h *callHandler) status(args map[string]any) (string, bool, error) {
	days := 7
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	repoFilter, _ := args["repo"].(string)
	maxSessions := 2
	if m, ok := args["max_sessions"].(float64); ok && m > 0 {
		maxSessions = int(m)
	}
	maxExcerpts := 6
	if m, ok := args["max_excerpts"].(float64); ok && m > 0 {
		maxExcerpts = int(m)
	}
	truncateLen := 160
	if t, ok := args["truncate_len"].(float64); ok && t > 0 {
		truncateLen = int(t)
	}

	result, err := h.mem.Status(days, repoFilter, maxSessions, maxExcerpts, truncateLen)
	if err != nil {
		return fmt.Sprintf("status failed: %v", err), true, nil
	}
	if len(result.Repos) == 0 && len(result.Streams) == 0 {
		return "No recent activity found.", false, nil
	}

	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) recentActivity(args map[string]any) (string, bool, error) {
	days := 7
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	repoFilter, _ := args["repo"].(string)

	results, err := h.mem.RecentActivity(days, repoFilter)
	if err != nil {
		return fmt.Sprintf("recent activity failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No recent activity found.", false, nil
	}

	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) memories(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	memType, _ := args["type"].(string)
	project, _ := args["project"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results, err := h.mem.SearchMemories(query, memType, project, limit)
	if err != nil {
		return fmt.Sprintf("memory search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No memories found.", false, nil
	}

	var b strings.Builder
	for _, m := range results {
		proj := m.Project
		if len(proj) > 30 {
			// Trim project path prefix for readability.
			parts := strings.Split(proj, "-")
			if len(parts) > 1 {
				proj = parts[len(parts)-1]
			}
		}
		fmt.Fprintf(&b, "## %s [%s] (%s)\n%s\n\n%s\n\n",
			m.Name, m.MemoryType, proj, m.Description, m.Content)
	}
	return b.String(), false, nil
}

func (h *callHandler) getMemory(args map[string]any) (string, bool, error) {
	project, _ := args["project"].(string)
	name, _ := args["name"].(string)

	if project == "" {
		return "project is required", true, nil
	}

	// When name is omitted, list all memories for the project.
	if name == "" {
		results, err := h.mem.SearchMemories("", "", project, 100)
		if err != nil {
			return fmt.Sprintf("memory list failed: %v", err), true, nil
		}
		if len(results) == 0 {
			return fmt.Sprintf("No memories found for project %q.", project), false, nil
		}
		var b strings.Builder
		fmt.Fprintf(&b, "Memories for project %q:\n\n", project)
		for _, m := range results {
			fmt.Fprintf(&b, "- **%s** [%s]: %s\n", m.Name, m.MemoryType, m.Description)
		}
		return b.String(), false, nil
	}

	m, err := h.mem.GetMemory(project, name)
	if err != nil {
		return fmt.Sprintf("get memory failed: %v", err), true, nil
	}
	if m == nil {
		// Check whether the project itself exists.
		all, listErr := h.mem.SearchMemories("", "", project, 1)
		if listErr == nil && len(all) == 0 {
			return fmt.Sprintf("Project %q not found.", project), false, nil
		}
		return fmt.Sprintf("Memory %q not found in project %q.", name, project), false, nil
	}

	return m.Content, false, nil
}

func (h *callHandler) skills(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results, err := h.mem.SearchSkills(query, limit)
	if err != nil {
		return fmt.Sprintf("skill search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No skills found.", false, nil
	}

	var b strings.Builder
	for _, sk := range results {
		fmt.Fprintf(&b, "## %s\n%s\n\n%s\n\n", sk.Name, sk.Description, sk.Content)
	}
	return b.String(), false, nil
}

func (h *callHandler) configs(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repoFilter, _ := args["repo"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results, err := h.mem.SearchClaudeConfigs(query, repoFilter, limit)
	if err != nil {
		return fmt.Sprintf("config search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No CLAUDE.md configs found.", false, nil
	}

	var b strings.Builder
	for _, c := range results {
		fmt.Fprintf(&b, "## %s\n**Path:** %s\n\n%s\n\n---\n\n", c.Repo, c.FilePath, c.Content)
	}
	return b.String(), false, nil
}

func (h *callHandler) usage(args map[string]any) (string, bool, error) {
	p := store.UsageParams{}
	if d, ok := args["days"].(float64); ok && d > 0 {
		p.Days = int(d)
	}
	p.Since, _ = args["since"].(string)
	p.Until, _ = args["until"].(string)
	p.RepoFilter, _ = args["repo"].(string)
	p.Model, _ = args["model"].(string)
	p.GroupBy, _ = args["group_by"].(string)

	result, err := h.mem.Usage(p)
	if err != nil {
		return fmt.Sprintf("usage query failed: %v", err), true, nil
	}
	if len(result.Rows) == 0 {
		return "No usage data found.", false, nil
	}

	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) auditLogs(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repo, _ := args["repo"].(string)
	skill, _ := args["skill"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results, err := h.mem.SearchAuditLogs(query, repo, skill, limit)
	if err != nil {
		return fmt.Sprintf("audit log search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No audit log entries found.", false, nil
	}

	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) targets(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repo, _ := args["repo"].(string)
	status, _ := args["status"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results, err := h.mem.SearchTargets(query, repo, status, limit)
	if err != nil {
		return fmt.Sprintf("targets search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No targets found.", false, nil
	}

	var b strings.Builder
	for _, t := range results {
		statusStr := t.Status
		if statusStr == "" {
			statusStr = "unknown"
		}
		weightStr := ""
		if t.Weight != 0 {
			weightStr = fmt.Sprintf(" weight=%.1f", t.Weight)
		}
		fmt.Fprintf(&b, "## %s %s [%s%s] (%s)\n", t.TargetID, t.Name, statusStr, weightStr, t.Repo)
		if t.Description != "" {
			fmt.Fprintf(&b, "%s\n", t.Description)
		}
		b.WriteByte('\n')
	}
	return b.String(), false, nil
}

func (h *callHandler) plans(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repoFilter, _ := args["repo"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results, err := h.mem.SearchPlans(query, repoFilter, limit)
	if err != nil {
		return fmt.Sprintf("plan search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No plans found.", false, nil
	}

	var b strings.Builder
	for _, p := range results {
		phase := p.Phase
		if phase == "" {
			phase = "(root)"
		}
		fmt.Fprintf(&b, "## %s [phase: %s] (%s)\n\n%s\n\n", p.FilePath, phase, p.Repo, p.Content)
	}
	return b.String(), false, nil
}

func (h *callHandler) docs(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repoFilter, _ := args["repo"].(string)
	kind, _ := args["kind"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results, err := h.mem.SearchDocs(query, repoFilter, kind, limit)
	if err != nil {
		return fmt.Sprintf("doc search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No docs found.", false, nil
	}

	var b strings.Builder
	for _, d := range results {
		title := d.Title
		if title == "" {
			title = filepath.Base(d.FilePath)
		}
		fmt.Fprintf(&b, "## %s [%s] (%s)\n", title, d.Kind, d.Repo)
		fmt.Fprintf(&b, "**Path**: %s\n\n", d.FilePath)
		// Truncate very long content for display.
		content := d.Content
		if len(content) > 2000 {
			content = content[:2000] + "\n…(truncated)"
		}
		fmt.Fprintf(&b, "%s\n\n", content)
	}
	return b.String(), false, nil
}

func (h *callHandler) synthesis(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	taxonomy, _ := args["taxonomy"].(string)
	repoFilter, _ := args["repo"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results, err := h.mem.SearchSynthesis(query, taxonomy, repoFilter, limit)
	if err != nil {
		return fmt.Sprintf("synthesis search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No synthesis docs found.", false, nil
	}

	var b strings.Builder
	for _, d := range results {
		title := d.Title
		if title == "" {
			title = filepath.Base(d.FilePath)
		}
		fmt.Fprintf(&b, "## %s [%s] (%s)\n", title, d.Taxonomy, d.Repo)
		fmt.Fprintf(&b, "**Path**: %s\n", d.FilePath)
		if d.DocDate != "" {
			fmt.Fprintf(&b, "**Date**: %s  ", d.DocDate)
		}
		if d.DocStatus != "" {
			fmt.Fprintf(&b, "**Status**: %s  ", d.DocStatus)
		}
		if d.DocTarget != "" {
			fmt.Fprintf(&b, "**Target**: %s  ", d.DocTarget)
		}
		if d.DocSource != "" {
			fmt.Fprintf(&b, "**Source**: %s", d.DocSource)
		}
		fmt.Fprintf(&b, "\n\n")
		content := d.Content
		if len(content) > 2000 {
			content = content[:2000] + "\n…(truncated)"
		}
		fmt.Fprintf(&b, "%s\n\n", content)
	}
	return b.String(), false, nil
}

func (h *callHandler) permissions(args map[string]any) (string, bool, error) {
	days := 30
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	repoFilter, _ := args["repo"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	result, err := h.mem.Permissions(days, repoFilter, limit)
	if err != nil {
		return fmt.Sprintf("permissions analysis failed: %v", err), true, nil
	}
	if len(result.TopTools) == 0 {
		return "No tool usage data found.", false, nil
	}

	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) prs(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repo, _ := args["repo"].(string)
	state, _ := args["state"].(string)
	author, _ := args["author"].(string)
	activityType, _ := args["type"].(string)
	days := 30
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	results, err := h.mem.SearchGitHubActivity(query, repo, state, author, activityType, days, limit)
	if err != nil {
		return fmt.Sprintf("GitHub activity search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No PRs or issues found.", false, nil
	}
	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) ci(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repo, _ := args["repo"].(string)
	conclusion, _ := args["conclusion"].(string)
	days := 30
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	results, err := h.mem.SearchCI(query, repo, conclusion, days, limit)
	if err != nil {
		return fmt.Sprintf("CI search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No CI runs found.", false, nil
	}
	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) commits(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repo, _ := args["repo"].(string)
	author, _ := args["author"].(string)
	days := 30
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	results, err := h.mem.SearchCommits(query, repo, author, days, limit)
	if err != nil {
		return fmt.Sprintf("commits search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No commits found.", false, nil
	}
	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) decisions(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	repo, _ := args["repo"].(string)
	days := 30
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	results, err := h.mem.SearchDecisions(query, repo, days, limit)
	if err != nil {
		return fmt.Sprintf("decisions search failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No decisions found.", false, nil
	}
	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) self(args map[string]any) (string, bool, error) {
	cc := h.cc
	nonce, _ := args["nonce"].(string)

	if nonce == "" {
		nonce = store.NoncePrefix + uuid.NewString()
		return nonce, false, nil
	}

	if !strings.HasPrefix(nonce, store.NoncePrefix) {
		return "invalid nonce — must be a value returned by a previous mnemo_self call", true, nil
	}

	sessionID, err := h.mem.ResolveNonce(nonce)
	if err != nil {
		return fmt.Sprintf("Nonce not found. The transcript may not be ingested yet — wait a moment and retry. Error: %v", err), true, nil
	}

	// Record the (MCP session, Claude Code session) binding so the
	// daemon has an authoritative record of which MCP session is
	// currently driving this transcript. This is the signal the
	// compactor / mnemo_restore / chain detection all build on.
	// No-op if MCPSessionID is empty (e.g. stateless test calls).
	h.mem.RecordConnectionSession(cc.MCPSessionID, sessionID)

	return fmt.Sprintf("session_id: %s", sessionID), false, nil
}

func (h *callHandler) whoRan(args map[string]any) (string, bool, error) {
	pattern, _ := args["pattern"].(string)
	if pattern == "" {
		return "pattern is required", true, nil
	}
	days := 30
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	repoFilter, _ := args["repo"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	results, err := h.mem.WhoRan(pattern, days, repoFilter, limit)
	if err != nil {
		return fmt.Sprintf("who_ran query failed: %v", err), true, nil
	}
	if len(results) == 0 {
		return "No matching commands found.", false, nil
	}
	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) restore(args map[string]any) (string, bool, error) {
	sessionID, _ := args["session_id"].(string)
	if sessionID == "" {
		return "session_id is required", true, nil
	}

	// Walk the session chain and union compactions across every
	// session in the chain. Under the activity-driven compactor
	// (🎯T59), compactions are tagged with connection_id only
	// best-effort: a session that never had an MCP binding still
	// produces compactions, just with NULL connection_id. So we
	// resolve via session_id (which always exists on a compaction
	// row) rather than via connection_id (which may not).
	//
	// session_chains drives the chain walk, so /clear boundaries
	// are honoured regardless of whether any connection ever
	// observed both halves.
	var sessionIDs []string
	if chain, err := h.mem.Chain(sessionID); err == nil && len(chain) > 0 {
		for _, link := range chain {
			sessionIDs = append(sessionIDs, link.SessionID)
		}
	} else {
		sessionIDs = []string{sessionID}
	}

	var compactions []store.Compaction
	seen := map[int64]bool{}
	for _, sid := range sessionIDs {
		cc, err := h.mem.ListCompactions(sid, 0)
		if err != nil {
			continue
		}
		for _, c := range cc {
			if seen[c.ID] {
				continue
			}
			seen[c.ID] = true
			compactions = append(compactions, c)
		}
	}

	if len(compactions) == 0 {
		return "No compactions available yet for this session. The background compactor scans session activity periodically; a session below the substantive-message threshold or outside the recency window will not yet have a compaction.", false, nil
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Compacted context for this connection (%d span(s)):\n\n", len(compactions))

	// Token-budget footer data: measure the running summariser cost
	// against the session's own cost for this chain leaf. Surfaces the
	// 🎯T10 AC6 invariant live.
	var compIn, compOut, sessIn, sessOut int64
	if in, out, err := h.mem.CompactionTokens(sessionID); err == nil {
		compIn, compOut = in, out
	}
	if in, out, err := h.mem.SessionTokens(sessionID); err == nil {
		sessIn, sessOut = in, out
	}

	for i, c := range compactions {
		sid := c.SessionID
		if len(sid) > 10 {
			sid = sid[:10]
		}
		fmt.Fprintf(&b, "── Span %d  [%s]  entries %d..%d  %s ──\n",
			i+1, sid, c.EntryIDFrom, c.EntryIDTo,
			c.GeneratedAt.Format("2006-01-02 15:04"))
		if c.Summary != "" {
			fmt.Fprintf(&b, "Summary: %s\n", c.Summary)
		}
		if c.PayloadJSON != "" && c.PayloadJSON != "{}" {
			var payload struct {
				Targets           []string          `json:"targets"`
				TargetsActive     []string          `json:"targets_active"`
				TargetsProgressed map[string]string `json:"targets_progressed"`
				TargetsNext       string            `json:"targets_next"`
				Files             []string          `json:"files"`
				OpenThreads       []string          `json:"open_threads"`
				Decisions         []struct {
					What string `json:"what"`
					Why  string `json:"why"`
				} `json:"decisions"`
			}
			if err := json.Unmarshal([]byte(c.PayloadJSON), &payload); err == nil {
				if len(payload.TargetsActive) > 0 {
					fmt.Fprintf(&b, "Targets active: %s\n", strings.Join(payload.TargetsActive, ", "))
				} else if len(payload.Targets) > 0 {
					fmt.Fprintf(&b, "Targets: %s\n", strings.Join(payload.Targets, ", "))
				}
				if len(payload.TargetsProgressed) > 0 {
					ids := make([]string, 0, len(payload.TargetsProgressed))
					for id := range payload.TargetsProgressed {
						ids = append(ids, id)
					}
					sort.Strings(ids)
					b.WriteString("Targets progressed:\n")
					for _, id := range ids {
						fmt.Fprintf(&b, "  - %s: %s\n", id, payload.TargetsProgressed[id])
					}
				}
				if payload.TargetsNext != "" {
					fmt.Fprintf(&b, "Next target: %s\n", payload.TargetsNext)
				}
				if len(payload.Files) > 0 {
					fmt.Fprintf(&b, "Files: %s\n", strings.Join(payload.Files, ", "))
				}
				for _, d := range payload.Decisions {
					fmt.Fprintf(&b, "Decision: %s — %s\n", d.What, d.Why)
				}
				if len(payload.OpenThreads) > 0 {
					fmt.Fprintf(&b, "Open threads: %s\n", strings.Join(payload.OpenThreads, "; "))
				}
			}
		}
		b.WriteByte('\n')
	}

	compTotal := compIn + compOut
	sessTotal := sessIn + sessOut
	if compTotal > 0 || sessTotal > 0 {
		fmt.Fprintf(&b, "── Budget ──\n")
		fmt.Fprintf(&b, "Compaction tokens: %d (prompt %d + output %d)\n", compTotal, compIn, compOut)
		if sessTotal > 0 {
			ratio := 100.0 * float64(compTotal) / float64(sessTotal)
			fmt.Fprintf(&b, "Session tokens: %d  |  Compaction/session: %.2f%%  (target < 10%%)\n", sessTotal, ratio)
		} else {
			fmt.Fprintf(&b, "Session tokens: unknown yet  |  ratio unmeasurable\n")
		}
	}

	return b.String(), false, nil
}

func (h *callHandler) chain(args map[string]any) (string, bool, error) {
	sessionID, _ := args["session_id"].(string)
	if sessionID == "" {
		return "session_id is required", true, nil
	}
	mode, _ := args["mode"].(string)
	if mode == "" {
		mode = "auto"
	}

	links, err := h.mem.Chain(sessionID)
	if err != nil {
		return fmt.Sprintf("chain lookup failed: %v", err), true, nil
	}
	if len(links) == 0 {
		return fmt.Sprintf("No session found for ID %s", sessionID), true, nil
	}

	// Definitive chain has a predecessor if the head of the chain is
	// not the queried session. Otherwise, the queried session has no
	// definitive predecessor — heuristic fallback may be relevant.
	hasDefinitivePred := len(links) > 1 && links[0].SessionID != sessionID

	var candidates []store.ChainCandidate
	runHeuristic := mode == "candidates" || (mode == "auto" && !hasDefinitivePred)
	if runHeuristic {
		if cc, err := h.mem.InferChainHeuristic(sessionID, 3); err == nil {
			candidates = cc
		}
	}

	var b strings.Builder
	if len(links) == 1 {
		fmt.Fprintf(&b, "Single session (no chain links detected):\n")
	} else {
		fmt.Fprintf(&b, "Chain of %d sessions (oldest → newest):\n", len(links))
	}
	for i, link := range links {
		sid := link.SessionID
		if len(sid) > 10 {
			sid = sid[:10]
		}
		repo := link.Repo
		if repo == "" {
			repo = link.Project
		}
		topic := link.Topic
		if len(topic) > 80 {
			topic = topic[:77] + "..."
		}
		first := link.FirstMsg
		if len(first) > 19 {
			first = first[:19]
		}
		last := link.LastMsg
		if len(last) > 19 {
			last = last[:19]
		}
		marker := "  "
		if link.SessionID == sessionID {
			marker = ">>"
		}
		fmt.Fprintf(&b, "%s [%d] %s  %s  %s→%s  %s\n",
			marker, i+1, sid, repo, first, last, topic)
		if i < len(links)-1 && link.Confidence != "" {
			fmt.Fprintf(&b, "       ↓ gap=%dms confidence=%s\n", link.GapMs, link.Confidence)
		}
	}
	if len(candidates) > 0 {
		fmt.Fprintf(&b, "\nHeuristic candidates (cwd_most_recent):\n")
		for _, c := range candidates {
			pid := c.PredecessorID
			if len(pid) > 10 {
				pid = pid[:10]
			}
			fmt.Fprintf(&b, "  ? %s  gap=%dms  confidence=%s  mechanism=%s\n",
				pid, c.GapMs, c.Confidence, c.Mechanism)
		}
	}
	return b.String(), false, nil
}

func (h *callHandler) whatsup(args map[string]any) (string, bool, error) {
	postmortem, _ := args["postmortem"].(bool)
	result, err := h.mem.Whatsup(postmortem)
	if err != nil {
		return fmt.Sprintf("whatsup failed: %v", err), true, nil
	}

	var b strings.Builder

	if len(result.Sessions) == 0 {
		fmt.Fprintf(&b, "No live Claude Code sessions detected.\n")
	} else {
		fmt.Fprintf(&b, "%-12s %-6s %7s %10s %-12s %-20s %s\n",
			"Session", "PID", "CPU%", "RSS", "WorkType", "Repo", "Topic")
		fmt.Fprintf(&b, "%s\n", strings.Repeat("-", 90))
		for _, s := range result.Sessions {
			sid := s.SessionID
			if len(sid) > 12 {
				sid = sid[:12]
			}
			rss := fmt.Sprintf("%dMB", s.RSSBytes/1024/1024)
			repo := s.Repo
			if len(repo) > 20 {
				repo = repo[:17] + "..."
			}
			topic := s.Topic
			if len(topic) > 40 {
				topic = topic[:37] + "..."
			}
			workType := s.WorkType
			if workType == "" {
				workType = "-"
			}
			fmt.Fprintf(&b, "%-12s %-6d %6.1f%% %10s %-12s %-20s %s\n",
				sid, s.PID, s.CPUPct, rss, workType, repo, topic)
			if s.Cwd != "" {
				fmt.Fprintf(&b, "  cwd: %s\n", s.Cwd)
			}
			switch len(s.Transcripts) {
			case 0:
				// no transcript found — omit
			case 1:
				fmt.Fprintf(&b, "  transcript: %s\n", s.Transcripts[0].Path)
			default:
				fmt.Fprintf(&b, "  transcripts (multiple — disambiguate by mtime/size):\n")
				for _, t := range s.Transcripts {
					fmt.Fprintf(&b, "    %s  mtime=%s size=%d\n",
						t.Path, t.MTime.Format("2006-01-02T15:04:05"), t.Size)
				}
			}
		}
	}

	// Postmortem section.
	if len(result.Postmortem) > 0 {
		fmt.Fprintf(&b, "\nPostmortem (recent claude activity, no live processes):\n")
		for _, e := range result.Postmortem {
			fmt.Fprintf(&b, "  cwd: %s\n", e.Cwd)
			for _, t := range e.Transcripts {
				fmt.Fprintf(&b, "    %s  mtime=%s size=%d\n",
					t.Path, t.MTime.Format("2006-01-02T15:04:05"), t.Size)
			}
		}
	}

	// System metrics section.
	sys := result.System
	if sys.MemPagesFree+sys.MemPagesActive+sys.MemPagesInactive+sys.MemPagesWired > 0 {
		total := sys.MemPagesFree + sys.MemPagesActive + sys.MemPagesInactive + sys.MemPagesWired
		pageSize := int64(4096) // macOS default page size
		fmt.Fprintf(&b, "\nSystem memory (4K pages, pressure=%.1f%%):\n", sys.MemPressurePct)
		fmt.Fprintf(&b, "  Free:     %d pages (%dMB)\n", sys.MemPagesFree, sys.MemPagesFree*pageSize/1024/1024)
		fmt.Fprintf(&b, "  Active:   %d pages (%dMB)\n", sys.MemPagesActive, sys.MemPagesActive*pageSize/1024/1024)
		fmt.Fprintf(&b, "  Inactive: %d pages (%dMB)\n", sys.MemPagesInactive, sys.MemPagesInactive*pageSize/1024/1024)
		fmt.Fprintf(&b, "  Wired:    %d pages (%dMB)\n", sys.MemPagesWired, sys.MemPagesWired*pageSize/1024/1024)
		fmt.Fprintf(&b, "  Total:    %d pages (%dMB)\n", total, total*pageSize/1024/1024)
	}

	return b.String(), false, nil
}

func (h *callHandler) defineTemplate(args map[string]any) (string, bool, error) {
	name, _ := args["name"].(string)
	if name == "" {
		return "name is required", true, nil
	}
	query, _ := args["query"].(string)
	if query == "" {
		return "query is required", true, nil
	}
	description, _ := args["description"].(string)

	var paramNames []string
	if raw, ok := args["params"]; ok && raw != nil {
		switch v := raw.(type) {
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok {
					paramNames = append(paramNames, s)
				}
			}
		case []string:
			paramNames = v
		}
	}

	if err := h.mem.DefineTemplate(name, description, query, paramNames); err != nil {
		return fmt.Sprintf("define template failed: %v", err), true, nil
	}
	return fmt.Sprintf("Template %q saved.", name), false, nil
}

func (h *callHandler) evaluateTemplate(args map[string]any) (string, bool, error) {
	name, _ := args["name"].(string)
	if name == "" {
		return "name is required", true, nil
	}

	params := make(map[string]string)
	if raw, ok := args["params"]; ok && raw != nil {
		if m, ok := raw.(map[string]any); ok {
			for k, v := range m {
				params[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	rows, err := h.mem.EvaluateTemplate(name, params)
	if err != nil {
		return fmt.Sprintf("evaluate template failed: %v", err), true, nil
	}
	if len(rows) == 0 {
		return "No rows returned.", false, nil
	}

	var b strings.Builder
	for _, row := range rows {
		for k, v := range row {
			fmt.Fprintf(&b, "%s: %v  ", k, v)
		}
		b.WriteByte('\n')
	}
	return b.String(), false, nil
}

func (h *callHandler) listTemplates() (string, bool, error) {
	templates, err := h.mem.ListTemplates()
	if err != nil {
		return fmt.Sprintf("list templates failed: %v", err), true, nil
	}
	if len(templates) == 0 {
		return "No templates defined.", false, nil
	}
	out, err := json.MarshalIndent(templates, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) discoverPatterns(args map[string]any) (string, bool, error) {
	days := 90
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	repoFilter, _ := args["repo"].(string)
	minOccurrences := 3
	if m, ok := args["min_occurrences"].(float64); ok && m > 0 {
		minOccurrences = int(m)
	}

	candidates, err := h.mem.DiscoverPatterns(days, repoFilter, minOccurrences)
	if err != nil {
		return fmt.Sprintf("discover patterns failed: %v", err), true, nil
	}
	if len(candidates) == 0 {
		return fmt.Sprintf("No workaround patterns found in the last %d days (min_occurrences=%d). The transcript index may not have enough data yet, or agents are already using mnemo tools effectively.", days, minOccurrences), false, nil
	}

	var b strings.Builder
	fmt.Fprintf(&b, "# Discovered Workaround Patterns (%d days, min_occurrences=%d)\n\n", days, minOccurrences)
	for _, c := range candidates {
		fmt.Fprintf(&b, "## %s (%d sessions)\n", c.PatternType, c.Occurrences)
		fmt.Fprintf(&b, "**Description:** %s\n\n", c.Description)
		fmt.Fprintf(&b, "**Suggestion:** %s\n\n", c.Suggestion)
		if c.Evidence != "" {
			fmt.Fprintf(&b, "**Example evidence:**\n```\n%s\n```\n\n", c.Evidence)
		}
		if len(c.Sessions) > 0 {
			shown := c.Sessions
			if len(shown) > 5 {
				shown = shown[:5]
			}
			fmt.Fprintf(&b, "**Sessions (showing %d of %d):** %s\n\n", len(shown), len(c.Sessions), strings.Join(shown, ", "))
		}
		b.WriteString("---\n\n")
	}
	return b.String(), false, nil
}

func (h *callHandler) images(args map[string]any) (string, bool, error) {
	query, _ := args["query"].(string)
	mode, _ := args["mode"].(string)
	repo, _ := args["repo"].(string)
	session, _ := args["session"].(string)
	searchFields, _ := args["search_fields"].(string)
	days := 90
	if d, ok := args["days"].(float64); ok && d > 0 {
		days = int(d)
	}
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	similarTo := 0
	if st, ok := args["similar_to"].(float64); ok && st > 0 {
		similarTo = int(st)
	}

	if mode == "" {
		mode = "text"
	}

	var results []store.ImageSearchResult
	var err error

	switch mode {
	case "semantic":
		if query == "" {
			return "query is required for semantic mode", true, nil
		}
		results, err = h.mem.SearchImagesSemantic(query, repo, session, days, limit)
		if err != nil {
			return fmt.Sprintf("semantic image search failed: %v", err), true, nil
		}
	case "similar":
		if similarTo <= 0 {
			return "similar_to (image ID) is required for similar mode", true, nil
		}
		results, err = h.mem.SearchImagesSimilar(similarTo, repo, session, days, limit)
		if err != nil {
			return fmt.Sprintf("similar image search failed: %v", err), true, nil
		}
	default: // "text"
		results, err = h.mem.SearchImagesFiltered(query, repo, session, days, limit, searchFields)
		if err != nil {
			return fmt.Sprintf("search images failed: %v", err), true, nil
		}
	}

	if len(results) == 0 {
		switch mode {
		case "semantic":
			return "No images found via semantic search. Ensure embeddings are populated (embed backend requires uv + sentence-transformers).", false, nil
		case "similar":
			return fmt.Sprintf("No similar images found for image ID %d. The image may not have an embedding yet.", similarTo), false, nil
		default:
			if query != "" {
				return "No images found matching query. Descriptions require ANTHROPIC_API_KEY; OCR requires Apple Vision (macOS) or tesseract.", false, nil
			}
			return "No images indexed yet. Images are extracted from transcripts during ingest.", false, nil
		}
	}

	var b strings.Builder
	for _, r := range results {
		img := r.Image
		sid := ""
		if len(r.Occurrences) > 0 {
			sid = r.Occurrences[0].SessionID
			if len(sid) > 8 {
				sid = sid[:8]
			}
		}
		fmt.Fprintf(&b, "[image id=%d] %s %dx%d %s (%.1f KB)",
			img.ID, img.MimeType, img.Width, img.Height, img.PixelFormat,
			float64(img.ByteSize)/1024)
		if img.OriginalPath != "" {
			fmt.Fprintf(&b, " path=%s", img.OriginalPath)
		}
		if sid != "" {
			fmt.Fprintf(&b, " session=%s", sid)
		}
		if r.MatchSource != "" {
			fmt.Fprintf(&b, " match=%s", r.MatchSource)
		}
		if r.Score > 0 {
			fmt.Fprintf(&b, " score=%.3f", r.Score)
		}
		b.WriteByte('\n')
		if r.Description != "" {
			fmt.Fprintf(&b, "  [desc] %s\n", r.Description)
		} else {
			b.WriteString("  [desc] (pending)\n")
		}
		if r.OCRText != "" {
			// Truncate long OCR text for display.
			ocrDisplay := r.OCRText
			if len(ocrDisplay) > 300 {
				ocrDisplay = ocrDisplay[:300] + "…"
			}
			fmt.Fprintf(&b, "  [ocr]  %s\n", ocrDisplay)
		}
		for _, occ := range r.Occurrences {
			occSID := occ.SessionID
			if len(occSID) > 8 {
				occSID = occSID[:8]
			}
			fmt.Fprintf(&b, "  seen in %s (%s) at %s\n", occSID, occ.SourceType, occ.OccurredAt)
		}
		b.WriteByte('\n')
	}
	return b.String(), false, nil
}

func (h *callHandler) sessionStructure(args map[string]any) (string, bool, error) {
	sessionID, _ := args["session_id"].(string)
	if sessionID == "" {
		return "session_id is required", true, nil
	}
	result, err := h.mem.SessionStructure(sessionID)
	if err != nil {
		return fmt.Sprintf("session_structure failed: %v", err), true, nil
	}
	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) toolResult(args map[string]any) (string, bool, error) {
	sessionID, _ := args["session_id"].(string)
	if sessionID == "" {
		return "session_id is required", true, nil
	}
	toolUseID, _ := args["tool_use_id"].(string)
	if toolUseID == "" {
		return "tool_use_id is required", true, nil
	}
	offset := 0
	if o, ok := args["offset"].(float64); ok && o >= 0 {
		offset = int(o)
	}
	truncateLen := 0
	if t, ok := args["truncate_len"].(float64); ok && t > 0 {
		truncateLen = int(t)
	}

	payload, err := h.mem.ToolResult(sessionID, toolUseID, offset, truncateLen)
	if err != nil {
		return fmt.Sprintf("tool_result lookup failed: %v", err), true, nil
	}

	var b strings.Builder
	if payload.IsError {
		b.WriteString("[error] ")
	}
	fmt.Fprintf(&b, "total_len=%d", payload.TotalLen)
	if offset > 0 {
		fmt.Fprintf(&b, " offset=%d", offset)
	}
	if payload.Truncated {
		fmt.Fprintf(&b, " truncated=true")
	}
	b.WriteString("\n\n")
	b.WriteString(payload.Text)
	return b.String(), false, nil
}

func (h *callHandler) locateUUID(args map[string]any) (string, bool, error) {
	prefix, _ := args["uuid"].(string)
	if prefix == "" {
		return "uuid is required", true, nil
	}
	contextBefore := 3
	if cb, ok := args["context_before"].(float64); ok && cb >= 0 {
		contextBefore = int(cb)
	}
	contextAfter := 3
	if ca, ok := args["context_after"].(float64); ok && ca >= 0 {
		contextAfter = int(ca)
	}

	matches, err := h.mem.LocateUUID(prefix, contextBefore, contextAfter)
	if err != nil {
		return fmt.Sprintf("locate_uuid failed: %v", err), true, nil
	}
	if len(matches) == 0 {
		return fmt.Sprintf("UUID %q not found in any session.", prefix), false, nil
	}

	out, err := json.MarshalIndent(matches, "", "  ")
	if err != nil {
		return fmt.Sprintf("marshal failed: %v", err), true, nil
	}
	return string(out), false, nil
}

func (h *callHandler) reworkHistory(args map[string]any) (string, bool, error) {
	targetID, _ := args["target_id"].(string)
	if targetID == "" {
		return "target_id is required", true, nil
	}
	repo, _ := args["repo"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	attempts, err := h.mem.ReworkHistory(targetID, repo, limit)
	if err != nil {
		return fmt.Sprintf("rework_history failed: %v", err), true, nil
	}
	if len(attempts) == 0 {
		return fmt.Sprintf("No prior rework attempts found for target %s.", targetID), false, nil
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Prior rework attempts for %s (%d span(s)):\n\n", targetID, len(attempts))
	for i, a := range attempts {
		sid := a.SessionID
		if len(sid) > 10 {
			sid = sid[:10]
		}
		fmt.Fprintf(&b, "── Attempt %d  [%s]  %s", i+1, sid, a.GeneratedAt)
		if a.Repo != "" {
			fmt.Fprintf(&b, "  (%s)", a.Repo)
		}
		b.WriteString(" ──\n")
		if a.Progress != "" {
			fmt.Fprintf(&b, "Progress: %s\n", a.Progress)
		}
		if a.Summary != "" {
			fmt.Fprintf(&b, "Summary: %s\n", a.Summary)
		}
		if len(a.OpenThreads) > 0 {
			fmt.Fprintf(&b, "Open threads: %s\n", strings.Join(a.OpenThreads, "; "))
		}
		b.WriteByte('\n')
	}
	return b.String(), false, nil
}

// vaultNotConfigured is the standard response when vault_path is absent.
const vaultNotConfigured = `Vault export is not configured. Mnemo runs fine without it — vault export is an optional Obsidian/Logseq integration that materialises sessions, decisions, memories, plans, and targets as Markdown notes in a directory you choose, and re-ingests human annotations you add below the <!-- mnemo:generated --> fence so they become searchable across all your transcripts.

To enable now without restarting the daemon:
  mnemo_config(op="write", patch={"vault_path": "~/Documents/mnemo-vault"})

Or edit ~/.mnemo/config.json and restart the daemon.`

func (h *callHandler) vaultSync() (string, bool, error) {
	if h.vault == nil {
		return vaultNotConfigured, false, nil
	}
	start := time.Now()
	if err := h.vault.Sync(h.ctx); err != nil {
		if errors.Is(err, vault.ErrSyncInFlight) {
			// Another sync (periodic ticker or initial pass) is already
			// running. Reporting this honestly is important: a falsely
			// successful 0s response would mislead callers into thinking
			// their sync request ran.
			return fmt.Sprintf("vault sync skipped: another sync is already in flight; this call did not run.\nVault path: %s",
				h.vault.Path()), false, nil
		}
		return fmt.Sprintf("vault sync failed: %v", err), true, nil
	}
	return fmt.Sprintf("vault sync complete in %s.\nVault path: %s",
		time.Since(start).Round(time.Millisecond), h.vault.Path()), false, nil
}

func (h *callHandler) vaultStatus(ctl ConfigController) (string, bool, error) {
	if h.vault == nil {
		return vaultNotConfigured, false, nil
	}
	vaultPath := h.vault.Path()
	sections := []string{"sessions", "decisions", "memories", "skills", "configs", "plans", "targets", "ci", "prs", "repos"}
	var b strings.Builder
	fmt.Fprintf(&b, "vault path: %s\n\n", vaultPath)

	// Indexing scope (🎯T64.1): report the configured surface so the
	// user can audit what mnemo can see. When ctl is wired (the normal
	// runtime), resolve against the live config; otherwise fall back
	// to the defaults so the section is never silently missing.
	var cfg store.Config
	if ctl != nil {
		cfg = ctl.Get()
	}
	scope := cfg.ResolvedVaultIndexingScope(vaultPath)
	ignoreFile := cfg.ResolvedVaultIndexingIgnoreFile()
	b.WriteString("Indexing scope:\n")
	fmt.Fprintf(&b, "  scope:       %s", scope)
	if cfg.VaultIndexingScope == "" {
		b.WriteString("  (auto-default)")
	}
	b.WriteString("\n")
	if scope == store.VaultIndexingScopeIncludes {
		fmt.Fprintf(&b, "  includes:    %v\n", cfg.VaultIndexingIncludes)
	}
	ignorePath := filepath.Join(vaultPath, ignoreFile)
	ignoreState := "absent"
	if _, err := os.Stat(ignorePath); err == nil {
		ignoreState = "present"
	}
	fmt.Fprintf(&b, "  ignore_file: %s (%s)\n\n", ignoreFile, ignoreState)

	// Vault layout (🎯T64.2). Surfaces the resolved mode, soak counter
	// (days_in_both), and recommendation per the design's state
	// machine so the user can audit whether a "both" vault is still
	// within its migration window.
	layout := cfg.ResolvedVaultLayout(vaultPath)
	b.WriteString("Layout:\n")
	fmt.Fprintf(&b, "  mode:        %s", layout)
	if cfg.VaultLayout.Mode == "" {
		b.WriteString("  (auto-default)")
	}
	b.WriteString("\n")

	state, _ := store.LoadState()
	soakAfter, _ := cfg.VaultLayout.EffectiveSoakWarnAfter()
	hoursInBoth, daysInBoth := computeHoursInBoth(state, layout)
	if layout == store.VaultLayoutBoth {
		fmt.Fprintf(&b, "  days_in_both: %d (soak_warn_after: %s)\n", daysInBoth, soakAfter)
	}
	rec := vaultLayoutRecommendation(layout, vaultPath, hoursInBoth, soakAfter)
	if rec != "" {
		fmt.Fprintf(&b, "  recommendation: %s\n", rec)
	}
	b.WriteString("\n")

	b.WriteString("Notes on disk:\n")
	total := 0
	for _, sec := range sections {
		count := countMDFiles(filepath.Join(vaultPath, sec))
		total += count
		fmt.Fprintf(&b, "  %-12s %d\n", sec, count)
	}
	fmt.Fprintf(&b, "  %-12s %d\n", "total", total)
	return b.String(), false, nil
}

// compactorStatus implements mnemo_compactor_status (🎯T67). It
// returns a snapshot of the compactor watcher's runtime state so
// callers can answer "is the compactor working?" without grepping
// the daemon log.
//
// The resolver may be nil (daemon started without compactor health
// wired) or may return nil (the user's workers haven't started
// yet); both surface as a helpful "not available" message rather
// than an error.
func (h *callHandler) compactorStatus(resolve func(username string) CompactorHealthReporter) (string, bool, error) {
	if resolve == nil {
		return "Compactor status not available (daemon was started without the compactor health resolver wired).", false, nil
	}
	reporter := resolve(h.cc.Username)
	if reporter == nil {
		return "Compactor status not available (the watcher hasn't started for this user yet — usually the daemon is still booting).", false, nil
	}
	hs := reporter.Health()

	var b strings.Builder
	b.WriteString("Compactor watcher status:\n\n")

	now := time.Now()
	formatAge := func(t time.Time) string {
		if t.IsZero() {
			return "never"
		}
		return fmt.Sprintf("%s (%s ago)", t.Format(time.RFC3339), now.Sub(t).Round(time.Second))
	}

	fmt.Fprintf(&b, "  last_scan_at:        %s\n", formatAge(hs.LastScanAt))
	fmt.Fprintf(&b, "  last_scan_count:     %d\n", hs.LastScanCount)
	fmt.Fprintf(&b, "  backlog:             %d (owed-but-uncompacted sessions)\n", hs.Backlog)
	fmt.Fprintf(&b, "  last_tick_at:        %s\n", formatAge(hs.LastTickAt))
	if hs.LastTickOutcome != "" {
		fmt.Fprintf(&b, "  last_tick_outcome:   %s\n", hs.LastTickOutcome)
	}
	if hs.InFlightSession != "" {
		fmt.Fprintf(&b, "  in_flight_session:   %s\n", hs.InFlightSession)
	} else {
		b.WriteString("  in_flight_session:   (idle)\n")
	}

	b.WriteString("\nLifetime tick counts:\n")
	outcomes := []string{"compacted", "nothing_to_compact", "budget_exceeded", "failed", "timeout", "skipped_self"}
	for _, o := range outcomes {
		fmt.Fprintf(&b, "  %-20s %d\n", o+":", hs.Counts[o])
	}

	b.WriteString("\nConfiguration:\n")
	fmt.Fprintf(&b, "  scan_interval:           %s\n", hs.ScanInterval)
	fmt.Fprintf(&b, "  idle_timeout:            %s\n", hs.IdleTimeout)
	fmt.Fprintf(&b, "  tick_timeout:            %s\n", hs.TickTimeout)
	fmt.Fprintf(&b, "  min_delta_messages:      %d\n", hs.MinDeltaMessages)
	fmt.Fprintf(&b, "  max_compactions_per_scan: %d\n", hs.MaxCompactionsPerScan)
	fmt.Fprintf(&b, "  max_token_ratio:         %.2f\n", hs.MaxTokenRatio)

	// Health heuristic (🎯T71): the watcher is genuinely stuck only when
	// neither the per-scan loop NOR the per-tick loop has progressed in
	// a while. A single scan can return up to MaxCompactionsPerScan
	// candidates and then spend many minutes ticking through them
	// (TickTimeout per LLM call), so last_scan_at can legitimately sit
	// well past 2× scan_interval on a busy daemon. Use last_tick_at as
	// the proof-of-life signal alongside last_scan_at — if either is
	// recent, we're working, not wedged.
	if !hs.LastScanAt.IsZero() {
		stale := 2 * hs.ScanInterval
		scanStale := now.Sub(hs.LastScanAt) > stale
		tickStale := hs.LastTickAt.IsZero() || now.Sub(hs.LastTickAt) > stale
		if scanStale && tickStale {
			tickAge := "never"
			if !hs.LastTickAt.IsZero() {
				tickAge = now.Sub(hs.LastTickAt).Round(time.Second).String() + " ago"
			}
			fmt.Fprintf(&b, "\n⚠ Watcher appears stuck: last scan was %s ago and last tick was %s (> 2× scan_interval).\n",
				now.Sub(hs.LastScanAt).Round(time.Second),
				tickAge)
		}
	}

	return b.String(), false, nil
}

// divergence implements mnemo_divergence (🎯T68.4): a uniform
// per-stream actual-vs-desired gap report across the derived data
// plane. Streams without a cheap gap metric are shown as "unknown"
// rather than a fabricated number.
func (h *callHandler) divergence() (string, bool, error) {
	rows := h.mem.StreamDivergences()

	var b strings.Builder
	b.WriteString("Derived-stream divergence (gap to fixed point):\n\n")
	if len(rows) == 0 {
		b.WriteString("  (no streams reported)\n")
		return b.String(), false, nil
	}

	converged := 0
	for _, d := range rows {
		if !d.Known {
			fmt.Fprintf(&b, "  %-22s unknown — %s\n", d.Stream+":", d.Note)
			continue
		}
		if d.Gap == 0 {
			converged++
		}
		last := d.LastReconciled
		if last == "" {
			last = "never"
		}
		fmt.Fprintf(&b, "  %-22s gap=%d %s (last reconciled: %s)\n",
			d.Stream+":", d.Gap, d.Unit, last)
		if d.Note != "" {
			fmt.Fprintf(&b, "  %-22s   %s\n", "", d.Note)
		}
	}
	fmt.Fprintf(&b, "\n%d stream(s) reported; %d converged (gap=0).\n", len(rows), converged)
	return b.String(), false, nil
}

// sourceDrift implements mnemo_source_drift (🎯T68.6): a read-only
// report of indexed transcript sources pruned/truncated out from under
// the index. Informational under the durable-tier model — the index
// retains the content.
func (h *callHandler) sourceDrift() (string, bool, error) {
	rep := h.mem.SourceDrift()

	var b strings.Builder
	b.WriteString("Source drift (indexed transcripts whose source is gone or shrank):\n\n")
	fmt.Fprintf(&b, "  deleted:    %d (source .jsonl no longer exists)\n", rep.Deleted)
	fmt.Fprintf(&b, "  truncated:  %d (current size below ingested offset)\n", rep.Truncated)
	fmt.Fprintf(&b, "  rewritten:  %d (same size, mtime moved — in-place edit)\n", rep.Rewritten)
	if rep.Deleted == 0 && rep.Truncated == 0 && rep.Rewritten == 0 {
		b.WriteString("\nNo drift — every indexed source is still present and intact.\n")
		return b.String(), false, nil
	}
	b.WriteString("\nThe index retains this content (durable tier); this is informational, not a reconcile gap.\n")
	if len(rep.Examples) > 0 {
		b.WriteString("\nExamples:\n")
		for _, e := range rep.Examples {
			fmt.Fprintf(&b, "  %-10s offset=%d size=%d  %s\n", e.Kind+":", e.Offset, e.Size, e.Path)
		}
	}
	return b.String(), false, nil
}

// vaultGC implements mnemo_vault_gc (🎯T68.6): inspect orphans and
// optionally clean up manifest rows whose file is gone. Never deletes
// files on disk — that path needs higher-level policy and is not in
// this version.
func (h *callHandler) vaultGC(args map[string]any) (string, bool, error) {
	vaultPath, _ := args["vault_path"].(string)
	if vaultPath == "" {
		return "", false, fmt.Errorf("vault_path is required")
	}
	confirm, _ := args["confirm"].(bool)

	rep, err := h.mem.ScanVaultOrphans(vaultPath)
	if err != nil {
		return "", false, err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Vault GC scan (%s):\n\n", vaultPath)
	fmt.Fprintf(&b, "  manifest_path_missing: %d (manifest rows whose file is gone)\n",
		len(rep.ManifestPathMissing))
	fmt.Fprintf(&b, "  disk_not_in_manifest:  %d (*.md files with no manifest entry — informational only)\n",
		len(rep.DiskNotInManifest))

	if len(rep.ManifestPathMissing) > 0 {
		b.WriteString("\nManifest rows pointing at missing files:\n")
		for i, m := range rep.ManifestPathMissing {
			if i >= 20 {
				fmt.Fprintf(&b, "  … and %d more\n", len(rep.ManifestPathMissing)-i)
				break
			}
			fmt.Fprintf(&b, "  %s [%s/%s]\n", m.NotePath, m.EntityKind, m.EntityID)
		}
	}
	if len(rep.DiskNotInManifest) > 0 {
		b.WriteString("\nDisk files with no manifest entry (informational — never auto-deleted):\n")
		for i, p := range rep.DiskNotInManifest {
			if i >= 20 {
				fmt.Fprintf(&b, "  … and %d more\n", len(rep.DiskNotInManifest)-i)
				break
			}
			fmt.Fprintf(&b, "  %s\n", p)
		}
	}

	if !confirm {
		if len(rep.ManifestPathMissing) > 0 {
			b.WriteString("\n[dry-run] Re-run with confirm=true to remove the manifest rows above.\n")
		} else {
			b.WriteString("\nNothing to clean up.\n")
		}
		return b.String(), false, nil
	}

	removed := 0
	for _, m := range rep.ManifestPathMissing {
		if err := h.mem.RemoveVaultManifestRow(m.NotePath); err != nil {
			fmt.Fprintf(&b, "\n⚠ failed to remove manifest row for %s: %v", m.NotePath, err)
			continue
		}
		removed++
	}
	fmt.Fprintf(&b, "\nRemoved %d manifest row(s). Disk side untouched.\n", removed)
	return b.String(), false, nil
}

// vaultMigrationDoc implements mnemo_vault_migration_doc. The default
// (write: false) returns the current state-of-vault snapshot without
// touching the filesystem; write: true overwrites _mnemo/MIGRATION.md
// with the same content, returning the path written to.
func (h *callHandler) vaultMigrationDoc(args map[string]any) (string, bool, error) {
	if h.vault == nil {
		return vaultNotConfigured, false, nil
	}
	write, _ := args["write"].(bool)
	if !write {
		return h.vault.MigrationDocSnapshot(), false, nil
	}
	path, err := h.vault.WriteMigrationDoc()
	if err != nil {
		return fmt.Sprintf("write migration doc failed: %v", err), true, nil
	}
	return fmt.Sprintf("wrote %s", path), false, nil
}

// computeHoursInBoth returns (hours, days) elapsed since the daemon
// first observed layout="both" for the current vault, or (0, 0) when
// the current resolved layout is not "both" or no first-seen entry
// has been recorded yet.
func computeHoursInBoth(state store.State, layout string) (hours, days int64) {
	if layout != store.VaultLayoutBoth {
		return 0, 0
	}
	t := state.LayoutFirstSeen(store.VaultLayoutBoth)
	if t == nil {
		return 0, 0
	}
	delta := time.Since(*t)
	if delta < 0 {
		return 0, 0
	}
	h := int64(delta / time.Hour)
	d := int64((delta + 12*time.Hour) / (24 * time.Hour))
	return h, d
}

// vaultLayoutRecommendation implements the state machine from
// docs/design/vault-library-wing.md. Pure function of observable
// state, so the recommendation always matches what the user sees.
//
// Returns "" (empty) when no migration action is owed.
func vaultLayoutRecommendation(layout, vaultPath string, hoursInBoth int64, soakAfter time.Duration) string {
	soakAfterHours := int64(soakAfter / time.Hour)
	switch layout {
	case store.VaultLayoutBoth:
		if hoursInBoth >= soakAfterHours {
			return "opt into v2"
		}
		return "still within soak"
	case store.VaultLayoutV2:
		if hasV1MarkerDirs(vaultPath) {
			return "run gc_legacy"
		}
		return ""
	default:
		return ""
	}
}

// hasV1MarkerDirs reports whether any of the v1 root-level marker
// directories (sessions/, decisions/, ...) exist under vaultPath.
// Used by the recommendation state machine and by the migration-doc
// snapshotter.
func hasV1MarkerDirs(vaultPath string) bool {
	for _, d := range store.V1VaultMarkerDirs {
		if fi, err := os.Stat(filepath.Join(vaultPath, d)); err == nil && fi.IsDir() {
			return true
		}
	}
	return false
}

// countMDFiles counts *.md files recursively under dir.
func countMDFiles(dir string) int {
	count := 0
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".md") {
			count++
		}
		return nil
	})
	return count
}

// config implements the mnemo_config tool. ctl is nil when main did not
// wire up the controller; in that case both modes report unavailability
// rather than crashing.
//
// The write path merges patch onto a fresh snapshot of the live config.
// Only keys present in the patch JSON are applied — unspecified keys
// preserve their current value. This makes "configure vault_path"
// safe to call without re-stating workspace_roots/etc.
func (h *callHandler) config(args map[string]any, ctl ConfigController) (string, bool, error) {
	if ctl == nil {
		return "mnemo_config not available (server started without config controller)", true, nil
	}
	op, _ := args["op"].(string)
	if op == "" {
		op = "read"
	}
	switch op {
	case "read":
		return renderConfigRead(ctl.Get(), h.callerHome()), false, nil
	case "write":
		patch, _ := args["patch"].(map[string]any)
		if len(patch) == 0 {
			return "op=write requires a non-empty \"patch\" object", true, nil
		}
		current := ctl.Get()
		merged, err := mergeConfigPatch(current, patch)
		if err != nil {
			return fmt.Sprintf("patch invalid: %v", err), true, nil
		}
		if _, err := merged.VaultLayout.EffectiveSoakWarnAfter(); err != nil {
			return fmt.Sprintf("patch invalid: %v", err), true, nil
		}
		report, err := ctl.Put(merged)
		if err != nil {
			return fmt.Sprintf("write failed: %v", err), true, nil
		}
		return renderConfigWrite(merged, report), false, nil
	default:
		return fmt.Sprintf("unknown op %q: expected \"read\" or \"write\"", op), true, nil
	}
}

// callerHome resolves the home directory for the request's
// Username, falling back to the daemon's own home if the user is
// unset or unresolvable. The read path uses this only for ~
// expansion in the displayed "Resolved paths" block; on Windows
// Service deployments the daemon runs as LocalSystem, and a per-
// user mnemo_config call should see vault_path resolved against the
// caller's home, not the service account's.
func (h *callHandler) callerHome() string {
	if h.cc.Username != "" {
		if home, err := store.ResolveHomeFor(h.cc.Username); err == nil {
			return home
		}
	}
	home, _ := osUserHome()
	return home
}

// knownConfigKeys is the closed set of JSON keys mnemo_config accepts
// in a patch. Anything else is rejected up-front so a typo like
// "vaultpath" produces an error rather than being silently dropped by
// json.Unmarshal's unknown-field handling.
var knownConfigKeys = map[string]struct{}{
	"workspace_roots":            {},
	"extra_project_dirs":         {},
	"synthesis_roots":            {},
	"vault_path":                 {},
	"vault_indexing_scope":       {},
	"vault_indexing_includes":    {},
	"vault_indexing_ignore_file": {},
	"vault_layout":               {},
	"linked_instances":           {},
}

// mergeConfigPatch round-trips current through JSON so the patch's
// keys overlay only the fields the user actually specified. This is
// simpler and safer than reflective field-by-field merging: any new
// Config field added later participates automatically as long as it
// has a json tag, and the resulting Config goes through json.Unmarshal
// which catches obvious type mismatches early.
//
// CONTRACT: every exported Config field must carry a `json:"name"`
// tag. A field tagged `json:"-"` (runtime-only / derived) is silently
// zeroed on every patch round-trip, even when the patch does not
// touch it. If a future Config field needs to survive merges without
// being patchable, switch this function to reflective field-by-field
// merge.
//
// Patch keys are validated against knownConfigKeys before merging so
// typos surface as tool errors instead of silent no-ops. Add a new
// entry to knownConfigKeys when adding a Config field.
func mergeConfigPatch(current store.Config, patch map[string]any) (store.Config, error) {
	var unknown []string
	for k := range patch {
		if _, ok := knownConfigKeys[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return store.Config{}, fmt.Errorf("unknown config keys: %s", strings.Join(unknown, ", "))
	}
	curJSON, err := json.Marshal(current)
	if err != nil {
		return store.Config{}, fmt.Errorf("marshal current: %w", err)
	}
	var asMap map[string]any
	if err := json.Unmarshal(curJSON, &asMap); err != nil {
		return store.Config{}, fmt.Errorf("unmarshal current: %w", err)
	}
	if asMap == nil {
		asMap = map[string]any{}
	}
	for k, v := range patch {
		asMap[k] = v
	}
	mergedJSON, err := json.Marshal(asMap)
	if err != nil {
		return store.Config{}, fmt.Errorf("marshal merged: %w", err)
	}
	var merged store.Config
	if err := json.Unmarshal(mergedJSON, &merged); err != nil {
		return store.Config{}, fmt.Errorf("decode merged: %w", err)
	}
	return merged, nil
}

func renderConfigRead(cfg store.Config, home string) string {
	var b strings.Builder
	b.WriteString("Current mnemo config (~/.mnemo/config.json):\n\n")
	data, _ := json.MarshalIndent(cfg, "", "  ")
	b.Write(data)
	b.WriteString("\n\n")
	b.WriteString("Resolved paths:\n")
	fmt.Fprintf(&b, "  workspace_roots:    %v\n", cfg.ResolvedWorkspaceRoots())
	fmt.Fprintf(&b, "  synthesis_roots:    %v\n", cfg.ResolvedSynthesisRoots())
	vp := cfg.ResolvedVaultPath(home)
	if vp == "" {
		b.WriteString("  vault_path:         (vault disabled)\n")
	} else {
		fmt.Fprintf(&b, "  vault_path:         %s\n", vp)
	}
	return b.String()
}

func renderConfigWrite(merged store.Config, report ConfigReport) string {
	var b strings.Builder
	b.WriteString("mnemo config updated and persisted to ~/.mnemo/config.json.\n\n")
	if len(report.Changed) == 0 {
		b.WriteString("No field values changed (patch matched the existing config).\n")
	} else {
		fmt.Fprintf(&b, "Changed fields:          %s\n", strings.Join(report.Changed, ", "))
		if len(report.Adopted) > 0 {
			fmt.Fprintf(&b, "Adopted live:            %s\n", strings.Join(report.Adopted, ", "))
		}
		if len(report.RequiresRestart) > 0 {
			fmt.Fprintf(&b, "Requires daemon restart: %s\n", strings.Join(report.RequiresRestart, ", "))
		}
		if len(report.Warnings) > 0 {
			b.WriteString("\nAdoption warnings (config persisted but live adoption failed):\n")
			for _, w := range report.Warnings {
				fmt.Fprintf(&b, "  - %s\n", w)
			}
		}
	}
	b.WriteString("\nNew config:\n")
	data, _ := json.MarshalIndent(merged, "", "  ")
	b.Write(data)
	b.WriteString("\n")
	return b.String()
}

// osUserHome is split into a tiny helper so tests can stub home
// resolution if needed; the current callers only need a best-effort
// path for read-side rendering.
func osUserHome() (string, error) {
	return os.UserHomeDir()
}
