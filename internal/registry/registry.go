// Copyright 2026 Marcelo Cantos
// SPDX-License-Identifier: Apache-2.0

// Package registry owns the per-user Store lifecycle.
//
// Each incoming MCP request carries an implicit or explicit user
// identity (via ?user=<name> or the process owner). Registry maps
// those identities to lazily-created Store instances and per-user
// background workers (ingest, watcher, compactor, CI polling).
//
// Registry lives in its own package rather than inside internal/store
// because it imports internal/compact, which imports internal/store —
// a store-owned Registry would create a dependency cycle.
package registry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/marcelocantos/mnemo/internal/backup"
	"github.com/marcelocantos/mnemo/internal/compact"
	"github.com/marcelocantos/mnemo/internal/reviewer"
	"github.com/marcelocantos/mnemo/internal/store"
	"github.com/marcelocantos/mnemo/internal/vault"
)

// llmAdapter bridges compact.LLMCaller to reviewer.LLMCaller. The
// two interfaces have the same shape; the type alias would create
// an import cycle since reviewer can't import compact.
type llmAdapter struct {
	c *compact.ClaudiaCaller
}

func (a llmAdapter) Call(ctx context.Context, sys, user string) (reviewer.LLMResult, error) {
	res, err := a.c.Call(ctx, sys, user)
	if err != nil {
		return reviewer.LLMResult{}, err
	}
	return reviewer.LLMResult{
		Text:         res.Text,
		Model:        res.Model,
		PromptTokens: res.PromptTokens,
		OutputTokens: res.OutputTokens,
		CostUSD:      res.CostUSD,
	}, nil
}

// Registry holds per-user Store instances plus their background
// workers. Stores are created lazily on first access via ForUser —
// this keeps a Windows-Service mnemo daemon (running as LocalSystem)
// idle until a request arrives carrying `?user=<name>`, at which
// point that user's transcript tree, database, and workers spin up.
//
// Multiple concurrent requests for the same user share a single
// Store instance. Registry.Close waits for every user's workers to
// drain and closes every Store.
type Registry struct {
	mu sync.Mutex
	// reloadMu serializes Reload calls. Holding it across the entire
	// Reload flow (snapshot → adopt → swap) prevents two concurrent
	// reloads from racing through swapVault and orphaning workers /
	// leaving the registry with two live exporters per user. mu is
	// still acquired in fine-grained sections inside Reload; reloadMu
	// is the coarse-grained guard.
	reloadMu       sync.Mutex
	baseCtx        context.Context
	cancel         context.CancelFunc
	stores         map[string]*userEntry
	cfg            store.Config
	mnemoRepoDir   string
	compactorModel string
}

// userEntry tracks one user's Store, optional vault Exporter, and
// background goroutines. workers lets Close wait for them to drain
// before the Store is closed.
//
// Vault workers are tracked separately (vaultCancel + vaultWorkers) so
// the mnemo_config tool can hot-swap vault_path: cancel the old vault
// sub-context, wait for its goroutines to drain, then start fresh ones
// against the new vault path. Non-vault workers (ingest, compactor, CI
// poller, reconciler) all continue uninterrupted, since the Store and
// transcript ingest pipeline are unaffected by a vault path change.
type userEntry struct {
	store             *store.Store
	vault             *vault.Exporter  // nil when vault_path is not configured
	compactWatcher    *compact.Watcher // background compaction watcher; nil before startWorkers
	workers           sync.WaitGroup
	vaultCancel       context.CancelFunc // cancels the vault sub-context; nil when vault disabled
	vaultWorkers      sync.WaitGroup     // tracks only vault goroutines, so reload can wait for them
	reconcilerCancel  context.CancelFunc // cancels the reconciler sub-context; nil when disabled
	reconcilerWorkers sync.WaitGroup     // tracks reconciler goroutine for hot-reload
	homeDir           string             // remembered for Reload's ~/ expansion
}

// NewRegistry builds an empty Registry. The baseCtx is cancelled on
// Close and is the parent of every per-user worker context.
// mnemoRepoDir is passed to the compactor watcher (it's the same for
// every user — the compactor spawns claudia against mnemo's source
// tree regardless of whose transcripts are being compacted).
func NewRegistry(parent context.Context, cfg store.Config, mnemoRepoDir string) *Registry {
	ctx, cancel := context.WithCancel(parent)
	return &Registry{
		baseCtx:        ctx,
		cancel:         cancel,
		stores:         map[string]*userEntry{},
		cfg:            cfg,
		mnemoRepoDir:   mnemoRepoDir,
		compactorModel: "sonnet",
	}
}

// ForUser returns the Store for the given username, creating it on
// first access. The empty username resolves to the process's home
// directory (useful for foreground / brew-services runs where the
// default identity is implicit).
//
// Callers that must never silently index SYSTEM's profile should
// reject the empty username up-front via DefaultUsername.
func (r *Registry) ForUser(username string) (*store.Store, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stores == nil {
		return nil, fmt.Errorf("registry is closed")
	}
	if e, ok := r.stores[username]; ok {
		return e.store, nil
	}

	home, err := store.ResolveHomeFor(username)
	if err != nil {
		return nil, err
	}

	projectDir := filepath.Join(home, ".claude", "projects")
	dbPath := filepath.Join(home, ".mnemo", "mnemo.db")

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	s, err := store.New(dbPath, projectDir)
	if err != nil {
		return nil, fmt.Errorf("open store for %q: %w", username, err)
	}
	s.SetWorkspaceRoots(r.cfg.ResolvedWorkspaceRoots())
	s.SetExtraProjectDirs(r.cfg.ExtraProjectDirs)

	synthRoots := r.cfg.ResolvedSynthesisRoots()
	var vaultExp *vault.Exporter
	if vaultPath := r.cfg.ResolvedVaultPath(home); vaultPath != "" {
		// Exclude the vault path from ingest walkers before any
		// Ingest* call runs. Without this, a vault sitting inside a
		// synthesis root or repo docs/ tree would have its generated
		// content re-ingested on every Sync, growing the docs index
		// without bound.
		s.RegisterExcludedPath(vaultPath, "vault_path")
		s.SetVaultPath(vaultPath) // 🎯T68.6: vault divergence + GC machinery needs the path
		exp, err := vault.New(s, vaultPath)
		if err != nil {
			slog.Warn("vault: exporter creation failed", "path", vaultPath, "err", err)
		} else {
			vp := vaultPath
			exp.SetLayoutResolver(func() string {
				return r.CurrentConfig().ResolvedVaultLayout(vp)
			})
			exp.SetSoakWarnAfterResolver(r.soakWarnAfterResolver())
			vaultExp = exp
		}
	}
	s.SetSynthesisRoots(synthRoots)

	e := &userEntry{store: s, vault: vaultExp, homeDir: home}
	r.stores[username] = e
	r.startWorkers(username, projectDir, e)
	return s, nil
}

// VaultFor returns the vault Exporter for username, or nil when vault is
// not configured or the user has not yet been initialised. Safe to call
// concurrently.
func (r *Registry) VaultFor(username string) *vault.Exporter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.stores[username]; ok {
		return e.vault
	}
	return nil
}

// CompactWatcherFor returns the compaction Watcher for username, or
// nil when the user has not yet been initialised. Used by the
// mnemo_compactor_status MCP tool (🎯T67) to surface watcher health
// — last scan / tick timestamps, in-flight session, lifetime tick
// counts — without grepping the daemon log.
func (r *Registry) CompactWatcherFor(username string) *compact.Watcher {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.stores[username]; ok {
		return e.compactWatcher
	}
	return nil
}

// startWorkers kicks off the per-user ingest / watcher / compactor /
// CI-poll goroutines. Each goroutine runs until r.baseCtx is
// cancelled (Registry.Close) or until it hits a terminal error.
func (r *Registry) startWorkers(username, projectDir string, e *userEntry) {
	logger := slog.Default().With("user", username)

	// Ingest + watcher + image workers + repo-level ingest streams.
	e.workers.Add(1)
	go func() {
		defer e.workers.Done()
		logger.Info("ingesting transcripts", "dir", projectDir)
		if err := e.store.IngestAll(); err != nil {
			logger.Error("initial ingest failed", "err", err)
		}
		if stats, err := e.store.Stats(); err == nil {
			logger.Info("ingest complete",
				"sessions", stats.TotalSessions,
				"messages", stats.TotalMessages)
		}
		e.store.StartImageDescriber()
		e.store.StartImageOCR()
		e.store.StartImageEmbedder()
		if err := e.store.IngestMemories(); err != nil {
			logger.Error("memory ingest failed", "err", err)
		}
		if err := e.store.IngestSkills(); err != nil {
			logger.Error("skill ingest failed", "err", err)
		}
		if err := e.store.IngestClaudeConfigs(); err != nil {
			logger.Error("claude config ingest failed", "err", err)
		}
		if err := e.store.IngestAuditLogs(); err != nil {
			logger.Error("audit log ingest failed", "err", err)
		}
		if err := e.store.IngestTargets(); err != nil {
			logger.Error("target ingest failed", "err", err)
		}
		if err := e.store.IngestPlans(); err != nil {
			logger.Error("plan ingest failed", "err", err)
		}
		if err := e.store.IngestDocs(); err != nil {
			logger.Error("doc ingest failed", "err", err)
		}
		if err := e.store.IngestSynthesis(); err != nil {
			logger.Error("synthesis ingest failed", "err", err)
		}
		// Initial vault sync: materialise all knowledge-graph entities as
		// Markdown notes. Spawned in its own goroutine so Watch() starts
		// immediately and live JSONL ingestion is not delayed. The SQLite
		// index is fully populated at this point (all Ingest* calls above
		// have completed), so the sync goroutine reads a consistent snapshot.
		//
		// Tracked under vaultWorkers (not workers) so a concurrent
		// mnemo_config vault_path swap waits for it to drain before
		// starting the new exporter, guaranteeing the old exporter
		// finishes writing to the old path before workers spin up
		// against the new one.
		r.mu.Lock()
		vp := e.vault
		if vp != nil {
			e.vaultWorkers.Add(1)
		}
		r.mu.Unlock()
		if vp != nil {
			go func() {
				defer e.vaultWorkers.Done()
				logger.Info("vault: initial sync starting")
				if err := vp.Sync(r.baseCtx); err != nil && !errors.Is(err, vault.ErrSyncInFlight) {
					logger.Warn("vault: initial sync failed", "err", err)
				}
			}()
		}
		if err := e.store.Watch(); err != nil {
			logger.Error("watcher failed", "err", err)
		}
	}()

	r.startVaultWorkers(username, e)

	// Compaction watcher.
	e.workers.Add(1)
	go func() {
		defer e.workers.Done()
		caller := compact.NewClaudiaCaller(r.mnemoRepoDir, r.compactorModel)
		compactor := compact.New(e.store, caller, compact.Config{})
		watcher := compact.NewWatcher(e.store, compactor, compact.WatcherConfig{}, r.mnemoRepoDir)
		e.compactWatcher = watcher
		logger.Info("compact: watcher starting")
		watcher.Run(r.baseCtx)
	}()

	// CLAUDE.md summary review worker (🎯T41). Same claudia.Task
	// path as the compactor but a different cadence and trigger
	// (cheap-signal entry-count gate, see store.ShouldReview).
	e.workers.Add(1)
	go func() {
		defer e.workers.Done()
		caller := compact.NewClaudiaCaller(r.mnemoRepoDir, r.compactorModel)
		rev := reviewer.New(e.store, llmAdapter{caller})
		reviewer.Run(r.baseCtx, rev)
	}()

	// External mirror reconciler (🎯T68.5): divergence-driven reconcile
	// of the mirror streams (CI today; GitHub/commits as they convert).
	// Ticks every minute but reconciles a repo's stream only when its
	// mirror_status cursor is missing or older than the stream's
	// interval, so a newly-seen repo is picked up promptly while fresh
	// repos are skipped. Replaces the fixed 5-minute PollCI loop.
	e.workers.Add(1)
	go func() {
		defer e.workers.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			now := time.Now()
			// 🎯T68.7 capstone: drive every registered periodic stream
			// through the StreamReconciler abstraction. Adding a new
			// stream is one entry in Store.StreamReconcilers(); this
			// loop stays the same.
			for _, sr := range e.store.StreamReconcilers() {
				if n, err := sr.Reconcile(r.baseCtx, now); err != nil {
					logger.Warn("reconcile failed", "stream", sr.Name(), "err", err)
				} else if n > 0 {
					logger.Info("reconciled", "stream", sr.Name(), "count", n)
				}
			}
			select {
			case <-r.baseCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// Anthropic Admin API cost reconciler (🎯T45, 🎯T63). Opt-in via
	// config.cost_reconciliation.enabled — disabled by default so that
	// no outbound Admin API call is made unless the operator
	// explicitly says so. Tracked per-userEntry so Reload can start
	// and stop the goroutine when the flag flips.
	r.startReconcilerWorker(e)

	// Periodic backup worker (🎯T61). Opted in by default; opt out via
	// {"backup": {"disabled": true}} in config.json.
	r.startBackupWorker(username, e, logger)

	// Daemon connection sweeper (🎯T60). Marks daemon_connections rows
	// closed once last_seen_at falls outside the idle threshold. The
	// HTTP MCP transport has no reliable disconnect signal, so this
	// sweep is the authoritative reaper. Opted in by default; opt out
	// via {"connection_sweep": {"disabled": true}}.
	r.startConnectionSweeper(e, logger)
}

// startConnectionSweeper spawns the per-user daemon_connections
// sweeper goroutine. On each tick it calls
// Store.MarkStaleConnectionsClosed; rows whose last_seen_at fell
// outside the idle threshold are marked closed. No-ops when the
// sweeper is disabled in config.
func (r *Registry) startConnectionSweeper(e *userEntry, logger *slog.Logger) {
	cfg := r.cfg.ConnectionSweep
	if !cfg.IsEnabled() {
		logger.Info("connection sweeper: disabled by config")
		return
	}
	interval, err := cfg.EffectiveInterval()
	if err != nil {
		logger.Warn("connection sweeper: bad interval, falling back to 1m", "err", err)
		interval = time.Minute
	}
	stale, err := cfg.EffectiveStaleAfter()
	if err != nil {
		logger.Warn("connection sweeper: bad stale_after, falling back to 10m", "err", err)
		stale = 10 * time.Minute
	}

	e.workers.Add(1)
	go func() {
		defer e.workers.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			n, err := e.store.MarkStaleConnectionsClosed(stale, time.Now())
			if err != nil {
				logger.Warn("connection sweeper: failed", "err", err)
			} else if n > 0 {
				logger.Info("connection sweeper: closed stale rows",
					"count", n, "stale_after", stale)
			}
			select {
			case <-r.baseCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// startReconcilerWorker spawns (or no-ops) the per-user Anthropic
// Admin API reconciler goroutine. Reads the latest cfg.CostReconciliation
// flag and tracks the cancel func + waitgroup on e so a subsequent
// Reload can stop the goroutine cleanly when the flag flips off.
//
// Caller must hold no locks; this method serialises against r.mu only
// when writing the cancel func into e. Safe to call from both
// startWorkers (initial bring-up) and Reload (config flip).
func (r *Registry) startReconcilerWorker(e *userEntry) {
	enabled := r.cfg.CostReconciliation.IsEnabled()
	if !enabled {
		// Run the gated entry-point once anyway to surface the
		// "disabled" log line at the same level/cadence as before.
		// StartReconciler is a synchronous no-op in this branch.
		e.store.StartReconciler(r.baseCtx, false)
		return
	}
	ctx, cancel := context.WithCancel(r.baseCtx)
	r.mu.Lock()
	e.reconcilerCancel = cancel
	r.mu.Unlock()
	e.reconcilerWorkers.Add(1)
	go func() {
		defer e.reconcilerWorkers.Done()
		e.store.StartReconciler(ctx, true)
		// StartReconciler spawns its own inner goroutine and returns;
		// keep this outer goroutine alive until ctx is cancelled so
		// reconcilerWorkers.Wait() in Reload covers both layers.
		<-ctx.Done()
	}()
}

// stopReconcilerWorker cancels the per-user reconciler goroutine (if
// any) and waits for it to drain. Idempotent — safe to call when the
// reconciler is already stopped.
func (r *Registry) stopReconcilerWorker(e *userEntry) {
	r.mu.Lock()
	cancel := e.reconcilerCancel
	e.reconcilerCancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
		e.reconcilerWorkers.Wait()
	}
}

// startBackupWorker resolves the backup config and launches the daily
// snapshot goroutine if backups are enabled. Misconfiguration (bad
// window times, bad quiescence duration) logs a warning and skips the
// worker — backup failures should never block the rest of the daemon.
func (r *Registry) startBackupWorker(username string, e *userEntry, logger *slog.Logger) {
	bcfg := r.cfg.Backup
	if !bcfg.IsEnabled() {
		logger.Info("backup: disabled by config")
		return
	}
	winStart, winEnd, err := bcfg.EffectiveWindow()
	if err != nil {
		logger.Warn("backup: invalid window, worker not started", "err", err)
		return
	}
	quiescence, err := bcfg.EffectiveQuiescenceMin()
	if err != nil {
		logger.Warn("backup: invalid quiescence_min, worker not started", "err", err)
		return
	}
	dir := bcfg.EffectiveDir(e.homeDir)
	keep := bcfg.EffectiveKeepDailies()

	w, err := backup.NewWorker(backup.Config{
		SrcPath:     e.store.DBPath(),
		Dir:         dir,
		Keep:        keep,
		WindowStart: winStart,
		WindowEnd:   winEnd,
		Quiescence:  quiescence,
		Activity:    e.store,
	})
	if err != nil {
		logger.Warn("backup: NewWorker failed, worker not started", "err", err)
		return
	}
	logger.Info("backup: worker starting",
		"dir", dir, "keep", keep,
		"window_start", winStart, "window_end", winEnd,
		"quiescence_min", quiescence)
	e.workers.Add(1)
	go func() {
		defer e.workers.Done()
		w.Run(r.baseCtx)
	}()
}

// startVaultWorkers launches the per-user vault periodic-sync and
// file-watcher goroutines under a vault-specific sub-context, so the
// mnemo_config tool can stop just those goroutines when vault_path
// changes without disturbing transcript ingest or the compactor.
//
// Returns the vault sub-context so callers wanting to spawn additional
// vault-scoped goroutines (e.g. the post-reload initial sync) can tie
// them to the same cancellation as the periodic-sync/watcher pair.
// Returns nil when e.vault is nil (no workers started).
//
// PRECONDITION: caller MUST hold r.mu. ForUser owns it via its defer
// for the entire Store-construction path; swapVault re-acquires it
// after building the new exporter. Re-acquiring inside this function
// would self-deadlock the ForUser path.
//
// The vault pointer is captured locally so a concurrent hot-swap that
// replaces e.vault does not race with the goroutines already running
// against the previous exporter.
func (r *Registry) startVaultWorkers(username string, e *userEntry) context.Context {
	vp := e.vault
	if vp == nil {
		return nil
	}
	vctx, vcancel := context.WithCancel(r.baseCtx)
	e.vaultCancel = vcancel

	logger := slog.Default().With("user", username)

	// Vault periodic sync: materialise new transcript entities as
	// Markdown every 5 minutes. Does NOT call IngestSynthesis here —
	// the file watcher below picks up those writes within ~2 seconds.
	e.vaultWorkers.Add(1)
	go func() {
		defer e.vaultWorkers.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-vctx.Done():
				return
			case <-ticker.C:
				if err := vp.Sync(vctx); err != nil && !errors.Is(err, vault.ErrSyncInFlight) {
					logger.Warn("vault: periodic sync failed", "err", err)
				}
			}
		}
	}()

	// Vault file watcher: re-indexes human annotations (content below the
	// <!-- mnemo:generated --> fence) within ~2 seconds of any .md save.
	// IngestVaultAnnotations extracts only below-fence content, so
	// generated blocks are never re-ingested and there is no feedback loop.
	e.vaultWorkers.Add(1)
	go func() {
		defer e.vaultWorkers.Done()
		vaultPath := vp.Path()
		// vault.New already called os.MkdirAll; the directory exists.

		fw, err := fsnotify.NewWatcher()
		if err != nil {
			logger.Warn("vault: file watcher init failed", "err", err)
			return
		}
		defer fw.Close()

		// Add vault root and all existing subdirectories.
		// fsnotify v1.9 does not expose a public WithRecursive option
		// on all platforms, so we walk and add manually, then
		// re-add any newly created subdirectory on CREATE events.
		// Hidden dirs (.obsidian/, .git/, .trash/) are skipped to
		// avoid wasting inotify slots on Linux and to skip Obsidian
		// internal-state churn that has no signal for mnemo.
		addVaultDirs := func(root string) {
			_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
				if err != nil || !d.IsDir() {
					return nil
				}
				if p != root && strings.HasPrefix(d.Name(), ".") {
					return filepath.SkipDir
				}
				_ = fw.Add(p)
				return nil
			})
		}
		addVaultDirs(vaultPath)
		logger.Info("vault: file watcher started", "path", vaultPath)

		const quietPeriod = 2 * time.Second
		debounce := time.NewTimer(quietPeriod)
		debounce.Stop()
		defer debounce.Stop()

		for {
			select {
			case <-vctx.Done():
				return
			case ev, ok := <-fw.Events:
				if !ok {
					return
				}
				// Watch newly created non-hidden subdirectories so notes
				// written into new sections are also picked up.
				if ev.Has(fsnotify.Create) {
					if fi, err := os.Stat(ev.Name); err == nil && fi.IsDir() &&
						!strings.HasPrefix(filepath.Base(ev.Name), ".") {
						_ = fw.Add(ev.Name)
					}
				}
				if strings.HasSuffix(ev.Name, ".md") {
					debounce.Reset(quietPeriod)
				}
			case err, ok := <-fw.Errors:
				if !ok {
					return
				}
				logger.Warn("vault: watcher error", "err", err)
			case <-debounce.C:
				opts := r.vaultIndexingOptionsFor(vaultPath)
				if err := e.store.IngestVaultAnnotations(vaultPath, opts); err != nil {
					logger.Warn("vault: annotation ingest failed", "err", err)
				}
				logger.Info("vault: annotations indexed from file change", "scope", opts.Scope)
			}
		}
	}()

	return vctx
}

// CurrentConfig returns a snapshot of the live Config. Safe to call
// concurrently.
func (r *Registry) CurrentConfig() store.Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg
}

// soakWarnAfterResolver returns a closure suitable for
// vault.Exporter.SetSoakWarnAfterResolver. The closure reads the live
// config on every call and, on the first malformed value it observes,
// emits a structured warning so a misconfigured soak window does not
// silently degrade to the package default. Subsequent calls with the
// same malformed string stay quiet to avoid log spam at sync cadence;
// a different malformed value re-arms the warn.
//
// mnemo_config validates SoakWarnAfter at write time (see tools.go)
// so the resolver path is defence-in-depth: users who hand-edit
// config.json still see the misconfiguration in the daemon log.
func (r *Registry) soakWarnAfterResolver() func() time.Duration {
	var (
		mu         sync.Mutex
		lastWarned string
	)
	return func() time.Duration {
		raw := r.CurrentConfig().VaultLayout.SoakWarnAfter
		d, err := r.CurrentConfig().VaultLayout.EffectiveSoakWarnAfter()
		if err != nil {
			mu.Lock()
			fire := raw != lastWarned
			lastWarned = raw
			mu.Unlock()
			if fire {
				slog.Warn("vault_layout.soak_warn_after malformed; falling back to default",
					"value", raw, "err", err)
			}
			return 0
		}
		return d
	}
}

// vaultIndexingOptionsFor builds the VaultIndexingOptions struct
// IngestVaultAnnotations expects, resolving any auto-default scope
// against the live vault tree (🎯T64.1). Reads the live config under
// the registry mutex so a concurrent Reload that swaps the indexing
// fields doesn't observe a half-updated struct.
func (r *Registry) vaultIndexingOptionsFor(resolvedVaultPath string) store.VaultIndexingOptions {
	r.mu.Lock()
	cfg := r.cfg
	r.mu.Unlock()
	return store.VaultIndexingOptions{
		Scope:      cfg.ResolvedVaultIndexingScope(resolvedVaultPath),
		Includes:   cfg.VaultIndexingIncludes,
		IgnoreFile: cfg.ResolvedVaultIndexingIgnoreFile(),
	}
}

// ReloadReport summarises what changed during a Reload call and which
// of those changes were adopted in-process versus deferred to the next
// daemon restart. The MCP tool surfaces this verbatim so the caller
// (most often a Claude Code agent on behalf of the user) can see at a
// glance whether the running daemon already reflects the new config.
type ReloadReport struct {
	// Changed lists the JSON keys whose values differ between the
	// previous and incoming Config (e.g. "vault_path").
	Changed []string
	// Adopted lists keys whose new values were applied to the running
	// daemon without a restart (a subset of Changed).
	Adopted []string
	// RequiresRestart lists keys whose values changed but cannot be
	// applied in-process (currently: "linked_instances"). These will
	// take effect only after the daemon is restarted.
	RequiresRestart []string
	// Warnings lists per-user adoption failures that happened despite
	// the config write itself succeeding. The classic case: vault.New
	// fails because the new vault_path points at a regular file (not
	// a directory). The config-on-disk is the new value, the old
	// vault workers are torn down, but the new exporter never came
	// up. Surfacing this here lets the MCP caller see the divergence
	// instead of believing the field was cleanly adopted.
	Warnings []string
}

// Reload swaps the Registry's active config for newCfg and adopts the
// changes across every already-initialised per-user entry. The caller
// is responsible for having validated newCfg (mnemo_config delegates to
// store.WriteConfig, which runs the same validation as LoadConfig).
//
// Adoption per field:
//   - workspace_roots, extra_project_dirs, synthesis_roots — applied
//     via the matching Store setters. New ingest passes will pick up
//     the new roots; already-indexed content is untouched.
//   - vault_path — the per-user vault sub-context is cancelled, its
//     goroutines drain, and fresh vault workers are started against
//     the new path (or vault is fully disabled if the new path is
//     empty). The initial sync against the new vault is kicked off in
//     the background; this call returns once the swap is complete.
//   - linked_instances — flagged as requires-restart. Federation peers
//     are wired up at startup against a process-wide http.Client; a
//     mid-run swap would need to tear down and rebuild every fan-out
//     handler, which is out of scope for this tool.
func (r *Registry) Reload(newCfg store.Config) ReloadReport {
	// Serialize reloads end-to-end. Two concurrent Reload calls would
	// otherwise both pass through the swapVault stages, with one
	// racing the other's vaultCancel/vault assignment under r.mu and
	// the result depending on goroutine interleavings. The MCP entry
	// point is the only caller in practice (single agent), but
	// nothing in the type signature enforces that, so guard it here.
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()

	r.mu.Lock()
	old := r.cfg
	r.cfg = newCfg
	entries := make(map[string]*userEntry, len(r.stores))
	for u, e := range r.stores {
		entries[u] = e
	}
	r.mu.Unlock()

	report := ReloadReport{}

	if !stringSlicesEqual(old.WorkspaceRoots, newCfg.WorkspaceRoots) {
		report.Changed = append(report.Changed, "workspace_roots")
		for _, e := range entries {
			e.store.SetWorkspaceRoots(newCfg.ResolvedWorkspaceRoots())
		}
		report.Adopted = append(report.Adopted, "workspace_roots")
	}
	if !stringSlicesEqual(old.ExtraProjectDirs, newCfg.ExtraProjectDirs) {
		report.Changed = append(report.Changed, "extra_project_dirs")
		for _, e := range entries {
			e.store.SetExtraProjectDirs(newCfg.ExtraProjectDirs)
		}
		report.Adopted = append(report.Adopted, "extra_project_dirs")
	}
	if !stringSlicesEqual(old.SynthesisRoots, newCfg.SynthesisRoots) {
		report.Changed = append(report.Changed, "synthesis_roots")
		for _, e := range entries {
			e.store.SetSynthesisRoots(newCfg.ResolvedSynthesisRoots())
		}
		report.Adopted = append(report.Adopted, "synthesis_roots")
	}
	if old.VaultPath != newCfg.VaultPath {
		report.Changed = append(report.Changed, "vault_path")
		anyFailure := false
		for username, e := range entries {
			if err := r.swapVault(username, e, newCfg.ResolvedVaultPath(e.homeDir)); err != nil {
				report.Warnings = append(report.Warnings,
					fmt.Sprintf("vault_path: user %q: %v", username, err))
				anyFailure = true
			}
		}
		// Even one failure means at least one Store does not have
		// the new vault active; refrain from claiming live adoption
		// in that case. The warning carries the detail.
		if !anyFailure {
			report.Adopted = append(report.Adopted, "vault_path")
		}
	}
	if !linkedInstancesEqual(old.LinkedInstances, newCfg.LinkedInstances) {
		report.Changed = append(report.Changed, "linked_instances")
		report.RequiresRestart = append(report.RequiresRestart, "linked_instances")
	}
	if old.CostReconciliation.IsEnabled() != newCfg.CostReconciliation.IsEnabled() {
		report.Changed = append(report.Changed, "cost_reconciliation.enabled")
		for _, e := range entries {
			// Tear down the existing goroutine (no-op when previously
			// disabled) then start a fresh one if the new state opts
			// in. startReconcilerWorker reads r.cfg, which was already
			// swapped above under r.mu.
			r.stopReconcilerWorker(e)
			r.startReconcilerWorker(e)
		}
		report.Adopted = append(report.Adopted, "cost_reconciliation.enabled")
	}
	return report
}

// swapVault tears down e's current vault workers, swaps in a fresh
// Exporter at newPath (or nil when newPath is ""), and starts new vault
// workers. Safe to call on a userEntry that currently has no vault: it
// will simply build one and start workers. Logs warnings rather than
// returning errors — partial success (e.g. the exporter built but a
// later sync failed) should not roll back the on-disk config.
//
// Reload serializes calls to swapVault via reloadMu; without that, two
// concurrent reloads could clear each other's vaultCancel funcs and
// leave the entry with stale workers running against an abandoned
// exporter.
func (r *Registry) swapVault(username string, e *userEntry, newPath string) error {
	logger := slog.Default().With("user", username)

	r.mu.Lock()
	oldCancel := e.vaultCancel
	e.vaultCancel = nil
	oldVault := e.vault
	e.vault = nil
	r.mu.Unlock()

	if oldCancel != nil {
		oldCancel()
		e.vaultWorkers.Wait()
		logger.Info("vault: workers stopped for reload", "previous_path", safePath(oldVault))
	}

	if newPath == "" {
		e.store.SetVaultPath("") // 🎯T68.6 clear so the vault divergence gatherer reports unknown
		logger.Info("vault: disabled by reload (vault_path cleared)")
		return nil
	}

	exp, err := vault.New(e.store, newPath)
	if err != nil {
		logger.Warn("vault: exporter creation failed on reload", "path", newPath, "err", err)
		return fmt.Errorf("vault.New(%q): %w", newPath, err)
	}
	e.store.SetVaultPath(newPath) // 🎯T68.6 mirror new vault path for divergence + GC
	exp.SetLayoutResolver(func() string {
		return r.CurrentConfig().ResolvedVaultLayout(newPath)
	})
	exp.SetSoakWarnAfterResolver(r.soakWarnAfterResolver())
	r.mu.Lock()
	e.vault = exp
	vctx := r.startVaultWorkers(username, e)
	// Track the post-reload initial sync under vaultWorkers so a
	// subsequent swap waits for it (no two syncs against the same
	// exporter racing through writeNote) and Close blocks on its
	// completion before closing the Store. Bound to vctx (not
	// r.baseCtx) so cascaded reloads (A→B→C) abort the in-flight
	// B-sync via oldCancel() at the next swap instead of forcing C
	// to block on B-sync's natural completion against a path the
	// user has already moved away from.
	e.vaultWorkers.Add(1)
	r.mu.Unlock()

	logger.Info("vault: workers restarted with new path", "path", newPath)

	go func() {
		defer e.vaultWorkers.Done()
		if err := exp.Sync(vctx); err != nil && !errors.Is(err, vault.ErrSyncInFlight) {
			logger.Warn("vault: post-reload sync failed", "err", err)
		}
	}()
	return nil
}

func safePath(v *vault.Exporter) string {
	if v == nil {
		return ""
	}
	return v.Path()
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// linkedInstancesEqual compares element-by-element with ==. All
// LinkedInstance fields must remain comparable for this to work: adding
// a slice field would surface as a compile error (caught), but adding a
// map field would compile and panic at runtime when both sides are
// non-nil. If LinkedInstance ever gains a non-comparable field, switch
// to reflect.DeepEqual on the element (or slices.EqualFunc with an
// explicit comparator updated alongside the struct).
func linkedInstancesEqual(a, b []store.LinkedInstance) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Close cancels every worker context and closes every Store. Safe to
// call once.
//
// Acquires reloadMu before r.mu so that an in-flight swapVault — which
// drops r.mu between teardown and the post-Wait re-entry that spawns
// new vault workers — cannot interleave with Close. Without this guard
// Close could observe vaultWorkers at zero, return from Wait, and then
// see swapVault's re-entry Add() new workers against a Store that
// Close is about to close (closed-DB log noise on shutdown, and a
// WaitGroup contract that depends on Wait's previous return value
// being final).
func (r *Registry) Close() {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	r.mu.Lock()
	r.cancel()
	entries := make([]*userEntry, 0, len(r.stores))
	for _, e := range r.stores {
		entries = append(entries, e)
	}
	r.stores = nil
	r.mu.Unlock()

	for _, e := range entries {
		e.workers.Wait()
		e.vaultWorkers.Wait()
		_ = e.store.Close()
	}
}
