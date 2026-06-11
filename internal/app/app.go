package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hjongedijk/drakkar/internal/api"
	"github.com/hjongedijk/drakkar/internal/blocklist"
	"github.com/hjongedijk/drakkar/internal/cache"
	"github.com/hjongedijk/drakkar/internal/catalog"
	"github.com/hjongedijk/drakkar/internal/config"
	"github.com/hjongedijk/drakkar/internal/dav"
	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/fuse"
	"github.com/hjongedijk/drakkar/internal/hydra"
	"github.com/hjongedijk/drakkar/internal/library"
	"github.com/hjongedijk/drakkar/internal/maintenance"
	"github.com/hjongedijk/drakkar/internal/metrics"
	"github.com/hjongedijk/drakkar/internal/nntp"
	"github.com/hjongedijk/drakkar/internal/nzb"
	"github.com/hjongedijk/drakkar/internal/opensubtitles"
	"github.com/hjongedijk/drakkar/internal/plex"
	"github.com/hjongedijk/drakkar/internal/policy"
	"github.com/hjongedijk/drakkar/internal/probe"
	"github.com/hjongedijk/drakkar/internal/queue"
	"github.com/hjongedijk/drakkar/internal/seerr"
	"github.com/hjongedijk/drakkar/internal/stream"
	"github.com/hjongedijk/drakkar/internal/subdl"
	"github.com/hjongedijk/drakkar/internal/subtitles"
	"github.com/hjongedijk/drakkar/internal/tmdb"
	"github.com/hjongedijk/drakkar/internal/tvdb"
	"github.com/hjongedijk/drakkar/internal/workflow"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/rs/zerolog"
)

const (
	maintenanceRecentTVTask    = "hydra_recent_tv"
	maintenanceRecentMovieTask = "hydra_recent_movie"
	taskSeerrSync              = "seerr_sync"
	taskPendingQueuePush       = "pending_queue_push"
	taskRetryFailedQueue       = "retry_failed_queue"
	taskRepublishPending       = "republish_pending"
	taskHealthCheck            = "health_check"
	taskCachePrune             = "cache_prune"
	taskOrphanedContent        = "orphaned-content"
	taskBrokenSymlinks         = "broken-media-symlinks"
	taskOrphanedCompleted      = "orphaned-completed-symlinks"
	taskFillMissingEpisodes    = "fill_missing_episodes"
)

type runtimeStatus struct {
	status api.Status
}

type taskScheduleStatusService struct {
	db *database.DB
}

func (s *runtimeStatus) Status() api.Status {
	return s.status
}

func (s *taskScheduleStatusService) ListTaskSchedules(ctx context.Context) ([]api.TaskSchedule, error) {
	cursorRows, err := s.db.ListMaintenanceCursors(ctx)
	if err != nil {
		return nil, err
	}
	lastRuns := make(map[string]time.Time, len(cursorRows))
	for _, row := range cursorRows {
		if ts, err := time.Parse(time.RFC3339, strings.TrimSpace(row.Cursor)); err == nil {
			lastRuns[row.TaskName] = ts
			continue
		}
		lastRuns[row.TaskName] = row.UpdatedAt
	}
	defs := []api.TaskSchedule{
		{ID: taskSeerrSync, Label: "Sync Seerr Requests", Group: "Indexing", Interval: "10m", Automated: true, LastRunState: "idle"},
		{ID: taskPendingQueuePush, Label: "Dispatch Pending Queue", Group: "Indexing", Interval: "5m", Automated: true, LastRunState: "idle"},
		{ID: maintenanceRecentTVTask, Label: "Recent TV Feed", Group: "Indexing", Interval: "15m", Automated: true, LastRunState: "idle"},
		{ID: maintenanceRecentMovieTask, Label: "Recent Movie Feed", Group: "Indexing", Interval: "15m", Automated: true, LastRunState: "idle"},
		{ID: taskRetryFailedQueue, Label: "Retry Failed Queue", Group: "Indexing", Interval: "15m", Automated: true, LastRunState: "idle"},
		{ID: taskRepublishPending, Label: "Republish Pending", Group: "Publishing", Interval: "30m", Automated: true, LastRunState: "idle"},
		{ID: taskHealthCheck, Label: "Run Health Check", Group: "Maintenance", Interval: "60m", Automated: true, LastRunState: "idle"},
		{ID: taskCachePrune, Label: "Prune Block Cache", Group: "Maintenance", Interval: "6h", Automated: true, LastRunState: "idle"},
		{ID: taskOrphanedContent, Label: "Remove Orphaned Content", Group: "Maintenance", Interval: "6h", Automated: true, LastRunState: "idle"},
		{ID: taskBrokenSymlinks, Label: "Remove Broken Media Symlinks", Group: "Maintenance", Interval: "6h", Automated: true, LastRunState: "idle"},
		{ID: taskOrphanedCompleted, Label: "Remove Orphaned History", Group: "Maintenance", Interval: "6h", Automated: true, LastRunState: "idle"},
		{ID: taskFillMissingEpisodes, Label: "Fill Missing Episodes", Group: "Indexing", Interval: "6h", Automated: true, LastRunState: "idle"},
	}
	for i := range defs {
		if runAt, ok := lastRuns[defs[i].ID]; ok {
			ts := runAt
			defs[i].LastRunAt = &ts
		}
	}
	return defs, nil
}

func Run(ctx context.Context, logger zerolog.Logger) error {
	rt := config.DefaultRuntime()
	if env := os.Getenv("DRAKKAR_SETTINGS_PATH"); env != "" {
		rt.SettingsPath = env
	}
	if env := os.Getenv("DRAKKAR_HTTP_ADDR"); env != "" {
		rt.HTTPAddress = env
	}
	if env := os.Getenv("DRAKKAR_WEBDAV_ADDR"); env != "" {
		rt.WebDAVAddress = env
	}

	cfg, err := config.Load(rt.SettingsPath)
	if err != nil {
		return err
	}
	if err := config.ValidatePaths(rt); err != nil {
		return err
	}
	for _, dir := range []string{
		rt.BlockCachePath,
		rt.HeaderCachePath,
		rt.RepairWorkspacePath,
		rt.StagingNZBPath,
		rt.FailedDiagnosticsPath,
		rt.LogsPath,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	db, err := database.Open(cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		return err
	}
	if err := db.ApplyMigrations(ctx, filepath.Join(".", "migrations")); err != nil {
		return err
	}
	var scheduledSrc *nntp.ScheduledSource
	db.ReadAhead = stream.NewReadAheadManager(rt.ReadAheadLimitBytes)
	db.ReadAhead.SetArticleBufferSize(cfg.Usenet.ArticleBufferSize)
	var (
		articleSources []nntp.NamedArticleSource
		pooledSources  []*nntp.PooledSource
		totalWorkers   int
	)
	for _, provider := range cfg.Usenet.Providers {
		if !provider.Enabled || provider.Host == "" {
			continue
		}
		client := nntp.NewArticleClient(provider)
		pooled := nntp.NewPooledSource(client.NewSession, provider.MaxConnections)
		pooledSources = append(pooledSources, pooled)
		articleSources = append(articleSources, nntp.NamedArticleSource{
			Name:   provider.Name,
			Source: pooled,
		})
		totalWorkers += max(provider.MaxConnections, 1)
	}
	if len(articleSources) > 0 {
		maxDownloadConnections := cfg.Usenet.MaxDownloadConnections
		if maxDownloadConnections <= 0 {
			maxDownloadConnections = totalWorkers
		}
		if maxDownloadConnections > totalWorkers {
			maxDownloadConnections = totalWorkers
		}
		fallback := nntp.NewFallbackSource(articleSources, 1)
		// Wrap with a 24-hour missing-article cache so known-expired (430) IDs
		// are never re-fetched from NNTP within the TTL window.
		cachedFallback := nntp.NewCachedFallbackSource(fallback)
		scheduled := nntp.NewScheduledSource(cachedFallback, maxDownloadConnections*3, maxDownloadConnections*8)
		// No separate background budget (matches nzbdav behaviour) — all priorities share the pool
		diskDecoded := nntp.NewDiskCachedDecodedSource(scheduled, rt.BlockCachePath, rt.DiskCacheLimitBytes)
		decoded := nntp.NewCachedDecodedSource(diskDecoded, rt.MemoryHotCacheMaxBytes)
		db.SegmentFetcher = nntp.NewSegmentFetcher(decoded)
		db.ReadAhead.SetConnectionBudget(maxDownloadConnections, cfg.Usenet.StreamingPriorityPct)
		scheduledSrc = scheduled
		// Evict stale missing-article cache entries hourly.
		go func() {
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					cachedFallback.Evict()
				}
			}
		}()
	}

	valkey := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Valkey.Host, cfg.Valkey.Port),
		Password: cfg.Valkey.Password,
		DB:       0,
		// Valkey 8 does not implement Redis maint_notifications; disable the
		// go-redis auto probe so startup stays quiet and deterministic.
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer valkey.Close()
	if err := valkey.Ping(ctx).Err(); err != nil {
		return err
	}

	startedAt := time.Now().UTC()
	statusSvc := &runtimeStatus{status: api.StatusFromConfig(rt, cfg, startedAt, true)}
	queueSvc := queue.NewService(db, nzb.NewImporter(rt.StagingNZBPath, rt.NZBUploadLimitBytes))
	seerrClient := seerr.NewClient(cfg.Seerr)
	hydraClient := hydra.NewClient(cfg.NZBHydra2)
	workflowSvc := workflow.NewService(db, seerrClient, hydraClient)
	if strings.TrimSpace(cfg.Metadata.TMDB.APIKey) != "" {
		workflowSvc.SetTMDBClient(tmdb.NewClient(cfg.Metadata))
	}
	if strings.TrimSpace(cfg.Metadata.TVDB.APIKey) != "" {
		workflowSvc.SetTVDBClient(tvdb.NewClient(cfg.Metadata))
	}
	publicationSvc := library.NewPublisher(db, rt)
	maintenanceSvc := maintenance.NewService(db, rt)
	cacheSvc := cache.NewService(cache.NewFileCache(rt.BlockCachePath, rt.DiskCacheLimitBytes))
	catalogSvc := catalog.NewService(db, tmdb.NewClient(cfg.Metadata))
	var subtitleProviders []subtitles.Provider
	var probeProviders []probe.NamedProber
	if strings.TrimSpace(cfg.Seerr.URL) != "" && strings.TrimSpace(cfg.Seerr.APIKey) != "" {
		probeProviders = append(probeProviders, seerrClient)
	}
	if strings.TrimSpace(cfg.NZBHydra2.URL) != "" && strings.TrimSpace(cfg.NZBHydra2.APIKey) != "" {
		probeProviders = append(probeProviders, hydraClient)
	}
	if cfg.Subtitles.Enabled {
		if auth, ok := cfg.Subtitles.Providers["subdl"]; ok && auth.Enabled && strings.TrimSpace(auth.APIKey) != "" {
			client := subdl.NewClient(auth)
			subtitleProviders = append(subtitleProviders, client)
			probeProviders = append(probeProviders, client)
		}
		if auth, ok := cfg.Subtitles.Providers["opensubtitles"]; ok && auth.Enabled && strings.TrimSpace(auth.APIKey) != "" && strings.TrimSpace(auth.Username) != "" && strings.TrimSpace(auth.Password) != "" {
			client := opensubtitles.NewClient(auth)
			subtitleProviders = append(subtitleProviders, client)
			probeProviders = append(probeProviders, client)
		}
	}
	for _, provider := range cfg.Usenet.Providers {
		if !provider.Enabled || strings.TrimSpace(provider.Host) == "" || strings.TrimSpace(provider.Username) == "" || strings.TrimSpace(provider.Password) == "" {
			continue
		}
		probeProviders = append(probeProviders, nntp.NewArticleClient(provider))
	}
	subtitleSvc := subtitles.NewService(db, cfg.Subtitles.Languages, subtitleProviders...)
	policySvc := policy.NewService(db)
	blocklistSvc := blocklist.NewService(db)
	probeSvc := probe.NewService(probeProviders...)
	plexClient := plex.NewClient(cfg.Plex.URL, cfg.Plex.Token)
	publicationSvc.SetPostPublishHook(func(ctx context.Context, libraryItemID int64) error {
		// Run all post-publish work in a goroutine so nothing blocks the queue pipeline.
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_ = subtitleSvc.RepublishStoredSubtitles(bgCtx, libraryItemID)
			subtitleSvc.TriggerAutomaticSearch(libraryItemID)
			if plexClient.Enabled() {
				plexCtx, plexCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer plexCancel()
				if err := plexClient.RefreshSection(plexCtx, cfg.Plex.SectionKey); err != nil {
					logger.Warn().Err(err).Msg("plex library refresh failed")
				}
			}
		}()
		return nil
	})
	postImport := func(ctx context.Context, item database.QueueSnapshot) error {
		if item.SelectedRelease == nil {
			return nil
		}
		return publicationSvc.PublishSelectedRelease(ctx, *item.SelectedRelease)
	}



	queueSvc.SetPostImportHook(postImport)
	workflowSvc.SetPostImportHook(postImport)
	workflowSvc.SetQueuePolicyProvider(policySvc)
	// Attempt lazy unmount in case a previous process left a stale FUSE socket.
	_ = fuse.LazyUnmount(rt.FuseMountPath)
	fuseServer, fuseErr := fuse.Mount(rt.FuseMountPath, rt.StagingNZBPath, rt.NZBUploadLimitBytes, queueSvc)
	if fuseErr != nil {
		logger.Warn().Err(fuseErr).Str("path", rt.FuseMountPath).Msg("fuse mount failed — FUSE VFS unavailable, use WebDAV on port 8888 for file access")
	} else {
		defer fuseServer.Unmount()
	}
	broker := api.NewEventBroker()

	// live metrics collector — reads NNTP pool + scheduler + disk cache at query time
	var pooledSrcs []*nntp.PooledSource
	if len(pooledSources) > 0 {
		pooledSrcs = pooledSources
	}
	blockCache := cache.NewFileCache(rt.BlockCachePath, rt.DiskCacheLimitBytes)
	metricsColl := &liveMetricsCollector{
		readAhead:  db.ReadAhead,
		pools:      pooledSrcs,
		scheduled:  scheduledSrc,
		blockCache: blockCache,
	}
	taskScheduleSvc := &taskScheduleStatusService{db: db}

	server := &http.Server{
		Addr:              rt.HTTPAddress,
		Handler:           api.Router(statusSvc, queueSvc, workflowSvc, publicationSvc, maintenanceSvc, cacheSvc, subtitleSvc, blocklistSvc, probeSvc, catalogSvc, broker, db, db.ReadAhead, db, taskScheduleSvc, policySvc, plexClient, metricsColl),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Reset queue items that were left in transitional states by a previous crash.
	if n, err := db.ResetStuckQueueItems(ctx); err != nil {
		logger.Error().Err(err).Msg("could not reset stuck queue items")
	} else if n > 0 {
		logger.Info().Int("reset", n).Msg("reset stuck queue items to failed")
	}

	// Start the work queue — searches library items immediately when pushed.
	workflowSvc.WorkQueue.Start(ctx, func(qCtx context.Context, libraryItemID int64) {
		if _, err := workflowSvc.SearchLibrary(qCtx, libraryItemID); err != nil {
			logger.Error().Err(err).Int64("libraryItemId", libraryItemID).Msg("workqueue: search failed")
		}
	})

	runRecentPass := func(mediaType string) {
		sr, err := workflowSvc.SearchRecentPending(ctx, mediaType)
		if err != nil {
			logger.Error().Err(err).Str("mediaType", mediaType).Msg("monitoring: recent search failed")
			return
		}
		if err := db.TouchMaintenanceCursor(ctx, maintenanceRecentTaskName(mediaType), time.Now().UTC().Format(time.RFC3339)); err != nil {
			logger.Error().Err(err).Str("mediaType", mediaType).Msg("monitoring: could not persist recent cursor")
		}
		if sr.Searched > 0 || sr.Selected > 0 {
			broker.Publish(map[string]any{"kind": "library.reconcile_background", "mediaType": mediaType, "searched": sr.Searched, "selected": sr.Selected})
			logger.Info().Str("mediaType", mediaType).Int("processed", sr.Processed).Int("searched", sr.Searched).Int("selected", sr.Selected).Msg("monitoring: recent search complete")
		}
	}

	// runStaleReset resets items that have been stuck in a transitional state
	// for longer than 10 minutes. This catches workers that died mid-job without
	// cleaning up state — something that only the startup reset can't handle.
	runStaleReset := func() {
		n, err := db.ResetStaleQueueItems(ctx, 10*time.Minute)
		if err != nil {
			logger.Error().Err(err).Msg("monitoring: stale reset error")
			return
		}
		if n > 0 {
			logger.Warn().Int("reset", n).Msg("monitoring: stale queue items reset")
			broker.Publish(map[string]any{"kind": "queue.stale_reset", "reset": n})
		}
	}

	// runRetryPass retries failed queue items.
	runRetryPass := func() {
		rr, err := workflowSvc.RetryFailedQueue(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("monitoring: retry failed queue error")
			return
		}
		_ = db.TouchMaintenanceCursor(ctx, taskRetryFailedQueue, time.Now().UTC().Format(time.RFC3339))
		if rr.Retried > 0 {
			broker.Publish(map[string]any{"kind": "queue.retry_background", "retried": rr.Retried})
		}
		logger.Info().Int("retried", rr.Retried).Msg("monitoring: retry failed queue complete")
	}

	// runSyncOnce syncs Seerr requests.
	runSyncOnce := func() {
		result, err := workflowSvc.SyncRequests(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("seerr sync failed")
			return
		}
		_ = db.TouchMaintenanceCursor(ctx, taskSeerrSync, time.Now().UTC().Format(time.RFC3339))
		if result.Created > 0 {
			broker.Publish(map[string]any{"kind": "requests.sync_background", "seen": result.Seen, "created": result.Created})
		}
		logger.Info().Int("seen", result.Seen).Int("created", result.Created).Msg("seerr sync complete")
	}

	// runPendingDispatch pushes ALL pending items to the WorkQueue for concurrent
	// 3-worker processing. Using SearchPendingLibrary() (WorkQueue) instead of
	// SearchPendingBatch() (sequential) increases throughput from ~45 items/hour
	// to ~3000+ items/hour — same approach as Radarr's parallel indexer dispatch.
	runPendingDispatch := func() {
		result, err := workflowSvc.SearchPendingLibrary(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("monitoring: pending dispatch error")
			return
		}
		_ = db.TouchMaintenanceCursor(ctx, taskPendingQueuePush, time.Now().UTC().Format(time.RFC3339))
		if result.Searched > 0 {
			broker.Publish(map[string]any{"kind": "library.pending_background", "searched": result.Searched, "selected": result.Selected, "failed": result.Failed})
		}
		logger.Info().Int("pending", result.Processed).Int("dispatched", result.Searched).Msg("monitoring: pending dispatch complete")
	}

	runRepublishPass := func() {
		result, err := publicationSvc.RepublishPendingLibrary(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("monitoring: republish pending error")
			return
		}
		_ = db.TouchMaintenanceCursor(ctx, taskRepublishPending, time.Now().UTC().Format(time.RFC3339))
		if result.Republished > 0 {
			broker.Publish(map[string]any{"kind": "library.republish_background", "republished": result.Republished})
		}
		logger.Info().Int("republished", result.Republished).Msg("monitoring: republish pending complete")
	}

	runHealthCheck := func() {
		entries, err := db.ListHealthEntries(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("monitoring: health check list error")
			return
		}
		var checked, healthy int
		for _, e := range entries {
			isOK := database.CheckSymlinkHealth(e.LibraryPath, e.TargetPath)
			_ = db.RecordHealthCheck(ctx, e.ID, isOK)
			checked++
			if isOK {
				healthy++
			}
		}
		_ = db.TouchMaintenanceCursor(ctx, taskHealthCheck, time.Now().UTC().Format(time.RFC3339))
		broker.Publish(map[string]any{"kind": "health.check_background", "checked": checked, "healthy": healthy})
		logger.Info().Int("checked", checked).Int("healthy", healthy).Msg("monitoring: health check complete")
	}

	runCachePrune := func() {
		result, err := cacheSvc.Prune(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("monitoring: cache prune error")
			return
		}
		_ = db.TouchMaintenanceCursor(ctx, taskCachePrune, time.Now().UTC().Format(time.RFC3339))
		if result.DeletedFiles > 0 {
			broker.Publish(map[string]any{"kind": "cache.prune_background", "deletedFiles": result.DeletedFiles})
		}
		logger.Info().Int("deletedFiles", result.DeletedFiles).Msg("monitoring: cache prune complete")
	}

	startRecurring := func(name string, interval time.Duration, runOnStartup bool, fn func()) {
		go func() {
			if runOnStartup {
				fn()
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					fn()
				}
			}
		}()
		logger.Info().Str("task", name).Dur("interval", interval).Bool("startup", runOnStartup).Msg("scheduler: task started")
	}

	// background worker: Seerr sync every 10 min. Sync imports requests only;
	// discovery happens via recent-feed polling or explicit/manual search.
	startRecurring(taskSeerrSync, 10*time.Minute, true, runSyncOnce)
	startRecurring(taskPendingQueuePush, 5*time.Minute, true, runPendingDispatch) // was 20m; push all pending every 5m for Radarr-like throughput

	// background worker: TV recent feed, Servarr-style 15 minute floor.
	startRecurring(maintenanceRecentTVTask, 15*time.Minute, shouldRunRecentOnStartup(ctx, db, maintenanceRecentTVTask, 15*time.Minute, time.Duration(cfg.NZBHydra2.FeedCacheTTLSeconds)*time.Second, time.Now().UTC()), func() {
		runRecentPass("tv")
	})

	// background worker: movie recent feed — now 15 min (was 60m) to match TV and
	// Radarr/Sonarr RSS-style fast detection of new releases on indexers.
	startRecurring(maintenanceRecentMovieTask, 15*time.Minute, shouldRunRecentOnStartup(ctx, db, maintenanceRecentMovieTask, 15*time.Minute, time.Duration(cfg.NZBHydra2.FeedCacheTTLSeconds)*time.Second, time.Now().UTC()), func() {
		runRecentPass("movie")
	})

	startRecurring("stale-queue-reset", 5*time.Minute, true, runStaleReset)
	startRecurring(taskRetryFailedQueue, 15*time.Minute, true, runRetryPass) // was 30m
	startRecurring(taskRepublishPending, 30*time.Minute, true, runRepublishPass)
	startRecurring(taskHealthCheck, 60*time.Minute, true, runHealthCheck)
	startRecurring(taskCachePrune, 6*time.Hour, true, runCachePrune)
	startRecurring(taskOrphanedContent, 6*time.Hour, true, func() { _, _ = maintenanceSvc.RemoveOrphanedContent(ctx) })
	startRecurring(taskBrokenSymlinks, 6*time.Hour, true, func() { _, _ = maintenanceSvc.RemoveBrokenMediaSymlinks(ctx) })
	startRecurring(taskOrphanedCompleted, 6*time.Hour, true, func() { _, _ = maintenanceSvc.RemoveOrphanedCompletedSymlinks(ctx) })
	startRecurring(taskFillMissingEpisodes, 6*time.Hour, false, func() {
		if _, err := workflowSvc.FillMissingEpisodes(ctx); err != nil {
			logger.Error().Err(err).Msg("fill missing episodes failed")
		}
	})

	webdavServer := &http.Server{
		Addr:              rt.WebDAVAddress,
		Handler:           dav.Handler(db, rt.MovieLibraryPath, rt.TVLibraryPath),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", rt.HTTPAddress).Msg("http server starting")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		logger.Info().Str("addr", rt.WebDAVAddress).Msg("webdav server starting")
		if err := webdavServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("webdav server error")
		}
	}()
	go func() {
		if err := publicationSvc.RebuildPublications(ctx); err != nil {
			logger.Error().Err(err).Msg("rebuild publications failed")
		}
	}()
	go func() {
		if err := db.CalibrateAllNZBOffsets(ctx); err != nil {
			logger.Error().Err(err).Msg("calibrate nzb offsets failed")
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = webdavServer.Shutdown(shutdownCtx)
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		if fuseErr == nil {
			return fuseServer.Unmount()
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func maintenanceRecentTaskName(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "tv", "episode":
		return maintenanceRecentTVTask
	default:
		return maintenanceRecentMovieTask
	}
}

func shouldRunRecentOnStartup(ctx context.Context, db *database.DB, taskName string, floor time.Duration, ttl time.Duration, now time.Time) bool {
	if ttl <= 0 {
		ttl = floor
	}
	cursor, err := db.GetMaintenanceCursor(ctx, taskName)
	if err != nil || strings.TrimSpace(cursor) == "" {
		return true
	}
	lastRun, err := time.Parse(time.RFC3339, cursor)
	if err != nil {
		return true
	}
	age := now.Sub(lastRun.UTC())
	skipWindow := floor
	if ttl > skipWindow {
		skipWindow = ttl
	}
	return age >= skipWindow
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type liveMetricsCollector struct {
	readAhead  *stream.ReadAheadManager
	pools      []*nntp.PooledSource
	scheduled  *nntp.ScheduledSource
	blockCache *cache.FileCache
}

func (c *liveMetricsCollector) Collect() metrics.Snapshot {
	var nntpStats metrics.NNTPStats
	for _, p := range c.pools {
		open, idle := p.Stats()
		inUse := open - idle
		if inUse < 0 {
			inUse = 0
		}
		nntpStats.Active += int64(inUse)
		nntpStats.Idle += int64(idle)
	}
	var queueStats metrics.QueueStats
	if c.scheduled != nil {
		interactive, readAhead, background := c.scheduled.QueueDepths()
		queueStats.Interactive = int64(interactive)
		queueStats.Background = int64(readAhead + background)
	}
	var cacheStats metrics.CacheStats
	if c.blockCache != nil {
		if stats, err := c.blockCache.Stats(); err == nil {
			cacheStats.DiskBytes = stats.Bytes
		}
	}
	if c.readAhead != nil {
		metrics.M.ActiveStreams.Store(int64(c.readAhead.ActiveCount()))
	}
	return metrics.M.Collect(nntpStats, cacheStats, queueStats)
}
