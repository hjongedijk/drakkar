package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"os"

	"github.com/go-chi/chi/v5"
	"github.com/hjongedijk/drakkar/internal/auth"
	"github.com/hjongedijk/drakkar/internal/cache"
	"github.com/hjongedijk/drakkar/internal/catalog"
	"github.com/hjongedijk/drakkar/internal/config"
	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/frontend"
	"github.com/hjongedijk/drakkar/internal/jellyfin"
	"github.com/hjongedijk/drakkar/internal/library"
	"github.com/hjongedijk/drakkar/internal/maintenance"
	"github.com/hjongedijk/drakkar/internal/metrics"
	"github.com/hjongedijk/drakkar/internal/nzb"
	"github.com/hjongedijk/drakkar/internal/plex"
	"github.com/hjongedijk/drakkar/internal/policy"
	"github.com/hjongedijk/drakkar/internal/probe"
	"github.com/hjongedijk/drakkar/internal/seerr"
	"github.com/hjongedijk/drakkar/internal/stream"
	intsub "github.com/hjongedijk/drakkar/internal/subtitles"
	"github.com/hjongedijk/drakkar/internal/version"
	"github.com/hjongedijk/drakkar/internal/workflow"
)

type StatusService interface {
	Status() Status
}

type MetricsProvider interface {
	Collect() metrics.Snapshot
}

type StreamsProvider interface {
	ActiveSessions() []stream.SessionSnapshot
	Stop(sessionID string)
}

type HealthRepository interface {
	HealthSummary(ctx context.Context) (database.HealthSummary, error)
	ListHealthEntries(ctx context.Context) ([]database.HealthEntry, error)
	ListHealthEntriesPage(ctx context.Context, filter string, limit, offset int) (database.HealthEntriesPage, error)
	ListConsistencyIssues(ctx context.Context) ([]database.ConsistencyIssue, error)
	RecordHealthCheck(ctx context.Context, publicationID int64, ok bool) error
}

type ProfilesRepository interface {
	ListQualityProfiles(ctx context.Context) ([]database.QualityProfile, error)
	UpsertQualityProfile(ctx context.Context, p database.QualityProfile) (database.QualityProfile, error)
	DeleteQualityProfile(ctx context.Context, id int64) error
	ListQualityDefinitions(ctx context.Context) ([]database.QualityDefinition, error)
	UpdateQualityDefinition(ctx context.Context, d database.QualityDefinition) (database.QualityDefinition, error)
	GetLibraryItemQualityProfile(ctx context.Context, libraryItemID int64) (*database.QualityProfile, error)
	SetLibraryItemQualityProfile(ctx context.Context, libraryItemID int64, profileID *int64) error
	SetMediaRequestQualityProfile(ctx context.Context, requestID int64, profileID *int64) (int64, error)
	GetGrabHistory(ctx context.Context, libraryItemID int64) ([]database.GrabHistoryEntry, error)
	ListCustomFormats(ctx context.Context) ([]database.CustomFormat, error)
	UpdateCustomFormat(ctx context.Context, f database.CustomFormat) (database.CustomFormat, error)
	DeleteCustomFormat(ctx context.Context, id int64) error
	UpsertCustomFormat(ctx context.Context, f database.CustomFormat) (database.CustomFormat, error)
	UpsertCustomFormatByName(ctx context.Context, f database.CustomFormat) (database.CustomFormat, error)
	ListReleaseBlockRules(ctx context.Context) ([]database.ReleaseBlockRule, error)
	UpsertReleaseBlockRule(ctx context.Context, r database.ReleaseBlockRule) (database.ReleaseBlockRule, error)
	UpdateReleaseBlockRule(ctx context.Context, r database.ReleaseBlockRule) (database.ReleaseBlockRule, error)
	DeleteReleaseBlockRule(ctx context.Context, id int64) error
	ListIndexerPolicies(ctx context.Context) ([]database.IndexerPolicy, error)
	UpsertIndexerPolicy(ctx context.Context, p database.IndexerPolicy) (database.IndexerPolicy, error)
	UpdateIndexerPolicy(ctx context.Context, p database.IndexerPolicy) (database.IndexerPolicy, error)
	DeleteIndexerPolicy(ctx context.Context, id int64) error
	ListSubtitleProfiles(ctx context.Context) ([]database.SubtitleProfile, error)
	CreateSubtitleProfile(ctx context.Context, p database.SubtitleProfile) (database.SubtitleProfile, error)
	UpdateSubtitleProfile(ctx context.Context, p database.SubtitleProfile) (database.SubtitleProfile, error)
	DeleteSubtitleProfile(ctx context.Context, id int64) error
	SetTVShowMonitoringMode(ctx context.Context, tvShowID int64, mode string) error
	ListSabQueueItems(ctx context.Context, category string, start, limit int) ([]database.SabQueueItem, int, error)
	ListSabHistoryItems(ctx context.Context, category string, start, limit int) ([]database.SabHistoryItem, int, error)
	DismissSabItems(ctx context.Context, libraryItemIDs []int64) error
}

type QueueService interface {
	ListQueue(ctx context.Context) ([]database.QueueSnapshot, error)
	ListLibraryItems(ctx context.Context) ([]database.LibraryItemSummary, error)
	ListReleaseSummaries(ctx context.Context, libraryItemID int64) ([]database.ReleaseSummary, error)
	ImportNZB(ctx context.Context, fileName string, src io.Reader) (database.QueueSnapshot, error)
	CancelNZB(ctx context.Context, nzbDocumentID int64) error
}

type CatalogService interface {
	Dashboard(ctx context.Context) (catalog.DashboardHome, error)
	ListLibraryCards(ctx context.Context) ([]catalog.MediaCard, error)
	SearchLibraryCards(ctx context.Context, query string) ([]catalog.MediaCard, error)
	DiscoverSearch(ctx context.Context, query string) (catalog.DiscoverSearchResult, error)
	DiscoverList(ctx context.Context, mediaType string, page int) (catalog.DiscoverListResult, error)
	DiscoverDetails(ctx context.Context, lookup catalog.DiscoverLookup) (catalog.DiscoverDetails, error)
	LibraryDetail(ctx context.Context, libraryItemID int64) (catalog.LibraryDetail, error)
	ReleaseCalendar(ctx context.Context, month string) ([]catalog.CalendarEntry, error)
}

type WorkflowService interface {
	ListRequests(ctx context.Context) ([]database.MediaRequestSummary, error)
	SyncRequests(ctx context.Context) (workflow.SyncResult, error)
	CreateSeerrRequest(ctx context.Context, mediaType string, tmdbID int64) (workflow.SyncResult, error)
	CreateSeerrSeasonRequest(ctx context.Context, tmdbID int64, seasons []int) (workflow.SyncResult, error)
	SearchPendingLibrary(ctx context.Context) (workflow.BulkSearchResult, error)
	WorkQueueStatus(ctx context.Context) (workflow.WorkQueueStatus, error)
	PauseWorkQueue(ctx context.Context) (workflow.WorkQueueStatus, error)
	ResumeWorkQueue(ctx context.Context) (workflow.WorkQueueStatus, error)
	RetryFailedQueue(ctx context.Context) (workflow.BulkQueueRetryResult, error)
	ClearFailedQueue(ctx context.Context) (int, error)
	SearchLibrary(ctx context.Context, libraryItemID int64) (workflow.SearchResult, error)
	SelectRelease(ctx context.Context, releaseCandidateID int64) (workflow.ReleaseActionResult, error)
	RejectRelease(ctx context.Context, releaseCandidateID int64, reason string) (workflow.ReleaseActionResult, error)
	RestoreRelease(ctx context.Context, releaseCandidateID int64) (workflow.ReleaseActionResult, error)
	RestoreRejectedReleases(ctx context.Context, libraryItemID int64) (database.RejectedReleaseRestoreResult, error)
	SkipRelease(ctx context.Context, releaseCandidateID int64) (workflow.ReleaseActionResult, error)
	RetryQueueItem(ctx context.Context, queueItemID int64) (workflow.QueueRetryResult, error)
	ManageQueueItem(ctx context.Context, queueItemID int64, action string) (workflow.QueueManageResult, error)
	ManageQueueItems(ctx context.Context, queueItemIDs []int64, action string) (workflow.BulkQueueRetryResult, error)
	ManageFailedQueue(ctx context.Context, action string) (workflow.BulkQueueRetryResult, error)
	BackfillMetadata(ctx context.Context) (workflow.BackfillMetadataResult, error)
	FillMissingEpisodes(ctx context.Context) (workflow.FillMissingEpisodesResult, error)
	PrioritizeTVShowMissing(ctx context.Context, tvShowID int64) (workflow.PrioritizeTVShowResult, error)
	SearchUpgrades(ctx context.Context) (workflow.UpgradeSearchResult, error)
	ManualSearch(ctx context.Context, query string) ([]workflow.ManualSearchItem, error)
	ImportNZBFromPush(ctx context.Context, content []byte, filename, mediaType string) (string, error)
	ResetLibraryItem(ctx context.Context, libraryItemID int64) error
	ResetOrphanedAvailableItems(ctx context.Context) (workflow.ResetOrphanedAvailableItemsResult, error)
	PushMissingLibraryItemsToSeerr(ctx context.Context) (workflow.PushMissingToSeerrResult, error)
	SyncPlexDetectedShows(ctx context.Context) (workflow.SyncPlexDetectedResult, error)
}

type PublicationService interface {
	RepublishLibraryItem(ctx context.Context, libraryItemID int64) error
	RepublishPendingLibrary(ctx context.Context) (library.BulkRepublishResult, error)
}

type MaintenanceService interface {
	DeepNZBHealthCheck(ctx context.Context) (maintenance.Result, error)
}

type CacheService interface {
	Prune(ctx context.Context) (cache.PruneResult, error)
}

type SubtitleService interface {
	ListSubtitles(ctx context.Context, libraryItemID int64) ([]database.SubtitleFileSummary, error)
	ListCandidates(ctx context.Context, libraryItemID int64) ([]database.SubtitleCandidateSummary, error)
	SearchCandidates(ctx context.Context, libraryItemID int64, languages []string) (intsub.SearchResult, error)
	DownloadCandidate(ctx context.Context, candidateID int64) (intsub.UploadResult, error)
	UploadSubtitle(ctx context.Context, libraryItemID int64, language, fileName string, src io.Reader) (intsub.UploadResult, error)
	DeleteSubtitle(ctx context.Context, subtitleID int64) error
}

type BlocklistService interface {
	List(ctx context.Context) ([]database.BlocklistItemSummary, error)
	ListPaged(ctx context.Context, f database.BlocklistFilter) (database.BlocklistPage, error)
	Stats(ctx context.Context) (database.BlocklistStats, error)
	Create(ctx context.Context, item database.BlocklistMutation) (database.BlocklistItemSummary, error)
	Update(ctx context.Context, id int64, item database.BlocklistMutation) (database.BlocklistItemSummary, error)
	Clear(ctx context.Context, id int64) error
	ClearAll(ctx context.Context) (database.BlocklistClearResult, error)
	ClearByReason(ctx context.Context, reason string) (database.BlocklistClearResult, error)
}

type IntegrationProbeService interface {
	Probe(ctx context.Context) (probe.Report, error)
}

type TaskSchedule struct {
	ID           string     `json:"id"`
	Label        string     `json:"label"`
	Group        string     `json:"group"`
	Interval     string     `json:"interval"`
	Automated    bool       `json:"automated"`
	LastRunAt    *time.Time `json:"lastRunAt,omitempty"`
	LastRunState string     `json:"lastRunState"`
}

type TaskScheduleProvider interface {
	ListTaskSchedules(ctx context.Context) ([]TaskSchedule, error)
}

type PolicyService interface {
	Settings(ctx context.Context) (policy.Settings, error)
	Update(ctx context.Context, input policy.Settings) (policy.Settings, error)
}

type SettingsService interface {
	GetSettings(ctx context.Context) (config.Settings, error)
	UpdateSettings(ctx context.Context, cfg config.Settings) (config.Settings, error)
}

type libraryPageResponse struct {
	Items          []catalog.MediaCard `json:"items"`
	Page           int                 `json:"page"`
	PageSize       int                 `json:"pageSize"`
	Total          int                 `json:"total"`
	TotalPages     int                 `json:"totalPages"`
	TotalMonitored int                 `json:"totalMonitored"`
	SumAvailable   int                 `json:"sumAvailable"`
	SumMissing     int                 `json:"sumMissing"`
	CountActive    int                 `json:"countActive"`
}

type EventBroker struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

type Status struct {
	Service              string         `json:"service"`
	Version              string         `json:"version"`
	Healthy              bool           `json:"healthy"`
	StartedAt            time.Time      `json:"startedAt"`
	Settings             map[string]any `json:"settings"`
	Integrations         Integrations   `json:"integrations"`
	FuseMountPath        string         `json:"fuseMountPath"`
	DiskCacheLimitBytes  int64          `json:"diskCacheLimitBytes"`
	ReadAheadLimitBytes  int64          `json:"readAheadLimitBytes"`
	MemoryHotCacheBytes  int64          `json:"memoryHotCacheBytes"`
	BackgroundQueueDepth int            `json:"backgroundQueueDepth"`
}

type Integrations struct {
	Seerr             IntegrationStatus            `json:"seerr"`
	NZBHydra2         IntegrationStatus            `json:"nzbhydra2"`
	Usenet            IntegrationStatus            `json:"usenet"`
	TMDB              IntegrationStatus            `json:"tmdb"`
	TVDB              IntegrationStatus            `json:"tvdb"`
	Subtitles         IntegrationStatus            `json:"subtitles"`
	SubtitleProviders map[string]IntegrationStatus `json:"subtitleProviders"`
}

type IntegrationStatus struct {
	Enabled    bool   `json:"enabled"`
	Configured bool   `json:"configured"`
	Detail     string `json:"detail,omitempty"`
	Count      int    `json:"count,omitempty"`
}

func NewEventBroker() *EventBroker {
	return &EventBroker{clients: make(map[chan []byte]struct{})}
}

func (b *EventBroker) Publish(event map[string]any) {
	if b == nil {
		return
	}
	raw, _ := json.Marshal(event)
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- raw:
		default:
		}
	}
}

func Router(status StatusService, queue QueueService, workflowSvc WorkflowService, publication PublicationService, maintenance MaintenanceService, cacheSvc CacheService, subtitleSvc SubtitleService, blocklistSvc BlocklistService, probeSvc IntegrationProbeService, catalogSvc CatalogService, broker *EventBroker, healthRepo HealthRepository, streamsProvider StreamsProvider, profilesRepo ProfilesRepository, taskSchedules TaskScheduleProvider, policySvc PolicyService, plexClient *plex.Client, jellyfinClient *jellyfin.Client, settingsSvc SettingsService, userRepo UserRepository, metricsProvider ...MetricsProvider) chi.Router {
	r := chi.NewRouter()
	r.Use(corsMiddleware)
	r.Use(authMiddlewareFor(userRepo))
	publishMutation := func(kind string, fields map[string]any) {
		if broker == nil {
			return
		}
		event := map[string]any{"kind": kind, "at": time.Now().UTC()}
		for key, value := range fields {
			event[key] = value
		}
		broker.Publish(event)
	}
	filterLibraryCards := func(items []catalog.MediaCard, kind, state, query string) []catalog.MediaCard {
		kind = strings.TrimSpace(strings.ToLower(kind))
		state = strings.TrimSpace(strings.ToLower(state))
		query = strings.TrimSpace(strings.ToLower(query))
		activeStates := map[string]struct{}{
			"searching": {}, "ranking": {}, "selected": {}, "fetching_nzb": {},
			"indexing": {}, "preflight": {}, "publishing": {}, "downloading": {},
		}
		filtered := make([]catalog.MediaCard, 0, len(items))
		for _, item := range items {
			mappedType := item.MediaType
			if mappedType == "episode" {
				mappedType = "tv"
			}
			if kind != "" && kind != "all" && mappedType != kind {
				continue
			}
			_, isActive := activeStates[strings.ToLower(item.QueueState)]
			switch state {
			case "", "all":
			case "available":
				if !item.Available {
					continue
				}
			case "active":
				if !isActive {
					continue
				}
			case "failed":
				if item.QueueState != "failed" {
					continue
				}
			case "missing":
				if item.Available || isActive {
					continue
				}
			}
			if query != "" {
				hay := strings.ToLower(item.Title)
				if item.Year > 0 {
					hay += " " + strconv.Itoa(item.Year)
				}
				if !strings.Contains(hay, query) {
					continue
				}
			}
			filtered = append(filtered, item)
		}
		return filtered
	}

	// SABnzbd-compatible API endpoint — allows Radarr/Sonarr to use Drakkar as a download client.
	if workflowSvc != nil {
		sabH := &sabHandler{
			importFn: workflowSvc.ImportNZBFromPush,
			repo:     profilesRepo,
			fuseMountPath: func() string {
				mp := status.Status().FuseMountPath
				if mp == "" {
					mp = config.DefaultFuseMountPath
				}
				return mp
			}(),
		}
		r.HandleFunc("/sabnzbd/api", sabH.ServeHTTP)
		r.HandleFunc("/api/sabnzbd/api", sabH.ServeHTTP)
		r.HandleFunc("/dav/api", sabH.ServeHTTP)
	}

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		// Browsers request text/html — let the SvelteKit health page handle those.
		// Health checkers (wget, curl, Docker) send no Accept or */*, get JSON.
		if strings.Contains(r.Header.Get("Accept"), "text/html") {
			frontend.Handler().ServeHTTP(w, r)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"service": "drakkar", "healthy": status.Status().Healthy})
	})
	r.Get("/api/status", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, http.StatusOK, status.Status())
	})
	r.Post("/api/integrations/probe", func(w http.ResponseWriter, r *http.Request) {
		if probeSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("integration probe unavailable"))
			return
		}
		report, err := probeSvc.Probe(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusAccepted, report)
	})
	r.Get("/api/queue", func(w http.ResponseWriter, r *http.Request) {
		items, err := queue.ListQueue(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		workQueue := workflow.WorkQueueStatus{}
		if workflowSvc != nil {
			workQueue, err = workflowSvc.WorkQueueStatus(r.Context())
			if err != nil {
				respondError(w, http.StatusInternalServerError, err)
				return
			}
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items, "workQueue": workQueue})
	})
	r.Post("/api/queue/pause", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		result, err := workflowSvc.PauseWorkQueue(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("queue.pause", map[string]any{"paused": result.Paused, "depth": result.Depth})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/queue/resume", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		result, err := workflowSvc.ResumeWorkQueue(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("queue.resume", map[string]any{"paused": result.Paused, "depth": result.Depth})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Get("/api/dashboard/home", func(w http.ResponseWriter, r *http.Request) {
		if catalogSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("catalog unavailable"))
			return
		}
		result, err := catalogSvc.Dashboard(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, result)
	})
	r.Get("/api/library/search", func(w http.ResponseWriter, r *http.Request) {
		if catalogSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("catalog unavailable"))
			return
		}
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		if query == "" {
			respondJSON(w, http.StatusOK, map[string]any{"items": []catalog.MediaCard{}})
			return
		}
		items, err := catalogSvc.SearchLibraryCards(r.Context(), query)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	r.Get("/api/discover/search", func(w http.ResponseWriter, r *http.Request) {
		if catalogSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("catalog unavailable"))
			return
		}
		query := strings.TrimSpace(r.URL.Query().Get("query"))
		if query == "" {
			respondJSON(w, http.StatusOK, catalog.DiscoverSearchResult{})
			return
		}
		result, err := catalogSvc.DiscoverSearch(r.Context(), query)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, result)
	})
	r.Get("/api/discover/{mediaType}", func(w http.ResponseWriter, r *http.Request) {
		if catalogSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("catalog unavailable"))
			return
		}
		mediaType := strings.ToLower(strings.TrimSpace(chi.URLParam(r, "mediaType")))
		page, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("page")))
		result, err := catalogSvc.DiscoverList(r.Context(), mediaType, page)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, result)
	})
	r.Get("/api/discover/details/{mediaType}", func(w http.ResponseWriter, r *http.Request) {
		if catalogSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("catalog unavailable"))
			return
		}
		mediaType := strings.ToLower(strings.TrimSpace(chi.URLParam(r, "mediaType")))
		title := strings.TrimSpace(r.URL.Query().Get("title"))
		year, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("year")))
		tmdbID, _ := strconv.ParseInt(strings.TrimSpace(r.URL.Query().Get("tmdbId")), 10, 64)
		result, err := catalogSvc.DiscoverDetails(r.Context(), catalog.DiscoverLookup{
			MediaType: mediaType,
			Title:     title,
			Year:      year,
			TMDBID:    tmdbID,
			IMDbID:    strings.TrimSpace(r.URL.Query().Get("imdbId")),
		})
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, result)
	})
	r.Post("/api/queue/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := workflowSvc.RetryQueueItem(workflow.WithAsyncDownload(r.Context()), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("queue.retry", map[string]any{"queueItemId": id, "action": result.Action})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/queue/retry-failed", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		result, err := workflowSvc.RetryFailedQueue(workflow.WithAsyncDownload(r.Context()))
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("queue.retry_failed", map[string]any{"processed": result.Processed, "retried": result.Retried, "failed": result.Failed})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/queue/{id}/action", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		var body struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := workflowSvc.ManageQueueItem(workflow.WithAsyncDownload(r.Context()), id, body.Action)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("queue.action", map[string]any{"queueItemId": id, "action": result.Action})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/queue/bulk-action", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		var body struct {
			QueueItemIDs []int64 `json:"queueItemIds"`
			Action       string  `json:"action"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		if len(body.QueueItemIDs) == 0 {
			respondError(w, http.StatusBadRequest, errors.New("queueItemIds required"))
			return
		}
		result, err := workflowSvc.ManageQueueItems(workflow.WithAsyncDownload(r.Context()), body.QueueItemIDs, body.Action)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("queue.bulk_action", map[string]any{"queueItemIds": body.QueueItemIDs, "action": body.Action, "processed": result.Processed, "retried": result.Retried, "failed": result.Failed})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/queue/failed/action", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		var body struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := workflowSvc.ManageFailedQueue(workflow.WithAsyncDownload(r.Context()), body.Action)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("queue.failed_action", map[string]any{"action": body.Action, "processed": result.Processed, "retried": result.Retried, "failed": result.Failed})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/queue/clear-failed", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusServiceUnavailable, errors.New("workflow service unavailable"))
			return
		}
		n, err := workflowSvc.ClearFailedQueue(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("queue.clear_failed", map[string]any{"cleared": n})
		respondJSON(w, http.StatusOK, map[string]any{"cleared": n})
	})
	r.Get("/api/nzbs", func(w http.ResponseWriter, r *http.Request) {
		items, err := queue.ListQueue(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	r.Post("/api/nzbs/import", func(w http.ResponseWriter, r *http.Request) {
		item, err := importNZBRequest(r, queue)
		if err != nil {
			switch {
			case errors.Is(err, nzb.ErrUploadTooLarge):
				respondError(w, http.StatusInsufficientStorage, err)
			case errors.Is(err, nzb.ErrEmptyDocument):
				respondError(w, http.StatusBadRequest, err)
			default:
				respondError(w, http.StatusBadRequest, err)
			}
			return
		}
		publishMutation("nzb.import", map[string]any{"queueItemId": item.QueueItemID, "libraryItemId": item.LibraryItemID})
		respondJSON(w, http.StatusCreated, item)
	})
	r.Post("/api/nzbs/import-url", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.URL == "" {
			respondError(w, http.StatusBadRequest, errors.New("url required"))
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, body.URL, nil)
		if err != nil {
			respondError(w, http.StatusBadRequest, fmt.Errorf("invalid url: %w", err))
			return
		}
		req.Header.Set("User-Agent", "Drakkar/1.0")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Errorf("fetch url: %w", err))
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			respondError(w, http.StatusInternalServerError, fmt.Errorf("remote returned HTTP %d", resp.StatusCode))
			return
		}
		fileName := path.Base(body.URL)
		if fileName == "" || fileName == "." {
			fileName = "import.nzb"
		}
		item, err := queue.ImportNZB(r.Context(), fileName, resp.Body)
		if err != nil {
			switch {
			case errors.Is(err, nzb.ErrUploadTooLarge):
				respondError(w, http.StatusInsufficientStorage, err)
			case errors.Is(err, nzb.ErrEmptyDocument):
				respondError(w, http.StatusBadRequest, err)
			default:
				respondError(w, http.StatusBadRequest, err)
			}
			return
		}
		publishMutation("nzb.import", map[string]any{"queueItemId": item.QueueItemID, "libraryItemId": item.LibraryItemID})
		respondJSON(w, http.StatusCreated, item)
	})
	r.Delete("/api/nzbs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		if err := queue.CancelNZB(r.Context(), id); err != nil {
			respondError(w, http.StatusNotFound, err)
			return
		}
		publishMutation("nzb.cancel", map[string]any{"nzbDocumentId": id})
		respondJSON(w, http.StatusOK, map[string]any{"status": "cancelled", "nzbDocumentId": id})
	})
	r.Get("/api/requests", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondJSON(w, http.StatusOK, map[string]any{"requests": []any{}})
			return
		}
		items, err := workflowSvc.ListRequests(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"requests": items})
	})
	r.Put("/api/requests/{id}/profile", func(w http.ResponseWriter, r *http.Request) {
		if profilesRepo == nil {
			respondError(w, http.StatusNotImplemented, errors.New("profiles unavailable"))
			return
		}
		requestID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		var body struct {
			ProfileID *int64 `json:"profileId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		libraryItemID, err := profilesRepo.SetMediaRequestQualityProfile(r.Context(), requestID, body.ProfileID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				respondError(w, http.StatusNotFound, errors.New("request is not linked to a library item"))
				return
			}
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("request.profile", map[string]any{"requestId": requestID, "libraryItemId": libraryItemID, "profileId": body.ProfileID})
		respondJSON(w, http.StatusOK, map[string]any{"requestId": requestID, "libraryItemId": libraryItemID, "profileId": body.ProfileID})
	})
	r.Get("/api/library", func(w http.ResponseWriter, r *http.Request) {
		if catalogSvc != nil {
			items, err := catalogSvc.ListLibraryCards(r.Context())
			if err != nil {
				respondError(w, http.StatusInternalServerError, err)
				return
			}
			// Compute aggregate counts from full unfiltered list for the metric band.
			activeStateSet := map[string]struct{}{
				"searching": {}, "ranking": {}, "selected": {}, "fetching_nzb": {},
				"indexing": {}, "preflight": {}, "publishing": {}, "downloading": {},
			}
			var sumAvailable, sumMissing, countActive int
			for _, item := range items {
				if item.AvailableCount > 0 {
					sumAvailable += item.AvailableCount
				} else if item.Available {
					sumAvailable++
				}
				sumMissing += item.MissingCount
				if _, isActive := activeStateSet[strings.ToLower(item.QueueState)]; isActive {
					countActive++
				}
			}
			q := r.URL.Query()
			filtered := filterLibraryCards(items, q.Get("kind"), q.Get("state"), q.Get("q"))
			sort.SliceStable(filtered, func(i, j int) bool {
				pi, pj := libStatusPriority(filtered[i]), libStatusPriority(filtered[j])
				if pi != pj {
					return pi < pj
				}
				return strings.ToLower(filtered[i].Title) < strings.ToLower(filtered[j].Title)
			})
			page, _ := strconv.Atoi(q.Get("page"))
			pageSize, _ := strconv.Atoi(q.Get("pageSize"))
			if page < 1 {
				page = 1
			}
			if pageSize <= 0 {
				pageSize = 40
			}
			if pageSize > 200 {
				pageSize = 200
			}
			total := len(filtered)
			totalPages := 1
			if total > 0 {
				totalPages = (total + pageSize - 1) / pageSize
			}
			if page > totalPages {
				page = totalPages
			}
			start := (page - 1) * pageSize
			if start < 0 {
				start = 0
			}
			end := start + pageSize
			if end > total {
				end = total
			}
			if start > end {
				start = end
			}
			respondJSON(w, http.StatusOK, libraryPageResponse{
				Items:          filtered[start:end],
				Page:           page,
				PageSize:       pageSize,
				Total:          total,
				TotalPages:     totalPages,
				TotalMonitored: len(items),
				SumAvailable:   sumAvailable,
				SumMissing:     sumMissing,
				CountActive:    countActive,
			})
			return
		}
		items, err := queue.ListLibraryItems(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items, "page": 1, "pageSize": len(items), "total": len(items), "totalPages": 1})
	})
	r.Get("/api/library/{id}/details", func(w http.ResponseWriter, r *http.Request) {
		if catalogSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("catalog unavailable"))
			return
		}
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		item, err := catalogSvc.LibraryDetail(r.Context(), libraryItemID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, item)
	})
	r.Post("/api/library/search-upgrades", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		go func() {
			result, err := workflowSvc.SearchUpgrades(context.Background())
			if err != nil {
				slog.Error("search upgrades background", "err", err)
				return
			}
			publishMutation("library.search_upgrades", map[string]any{"checked": result.Checked, "upgraded": result.Upgraded, "failed": result.Failed})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Get("/api/library/missing", func(w http.ResponseWriter, r *http.Request) {
		items, err := queue.ListLibraryItems(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		missing := make([]database.LibraryItemSummary, 0, len(items))
		for _, item := range items {
			if !item.Available {
				missing = append(missing, item)
			}
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": missing})
	})
	r.Post("/api/library/{id}/replacements", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		// Start search in background; return existing candidates immediately so the
		// release picker opens without waiting for the full NZB search round-trip.
		go func() {
			search, searchErr := workflowSvc.SearchLibrary(context.Background(), libraryItemID)
			if searchErr != nil {
				slog.Warn("search replacements background", "library_item_id", libraryItemID, "err", searchErr)
			}
			items, err := queue.ListReleaseSummaries(context.Background(), libraryItemID)
			if err != nil {
				slog.Error("replacements list background", "library_item_id", libraryItemID, "err", err)
				return
			}
			publishMutation("library.replacements", map[string]any{"libraryItemId": libraryItemID, "candidateCount": len(items), "selectedReleaseId": search.SelectedReleaseID})
		}()
		items, err := queue.ListReleaseSummaries(r.Context(), libraryItemID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusAccepted, map[string]any{
			"libraryItemId":  libraryItemID,
			"candidateCount": len(items),
			"items":          items,
			"searching":      true,
		})
	})
	r.Get("/api/releases/{libraryItemId}", func(w http.ResponseWriter, r *http.Request) {
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "libraryItemId"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		items, err := queue.ListReleaseSummaries(r.Context(), libraryItemID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	r.Get("/api/subtitles/{libraryItemId}", func(w http.ResponseWriter, r *http.Request) {
		if subtitleSvc == nil {
			respondJSON(w, http.StatusOK, map[string]any{"items": []any{}})
			return
		}
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "libraryItemId"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		items, err := subtitleSvc.ListSubtitles(r.Context(), libraryItemID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	r.Get("/api/subtitle-candidates/{libraryItemId}", func(w http.ResponseWriter, r *http.Request) {
		if subtitleSvc == nil {
			respondJSON(w, http.StatusOK, map[string]any{"items": []any{}})
			return
		}
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "libraryItemId"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		items, err := subtitleSvc.ListCandidates(r.Context(), libraryItemID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	r.Post("/api/subtitles/{libraryItemId}/search", func(w http.ResponseWriter, r *http.Request) {
		if subtitleSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("subtitles unavailable"))
			return
		}
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "libraryItemId"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		var payload struct {
			Languages []string `json:"languages"`
		}
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&payload)
		}
		languages := payload.Languages
		go func() {
			result, err := subtitleSvc.SearchCandidates(context.Background(), libraryItemID, languages)
			if err != nil {
				slog.Warn("subtitle search background", "library_item_id", libraryItemID, "err", err)
				return
			}
			publishMutation("subtitle.search", map[string]any{"libraryItemId": libraryItemID, "candidateCount": result.CandidateCount})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true, "libraryItemId": libraryItemID})
	})
	r.Post("/api/subtitles/{libraryItemId}/upload", func(w http.ResponseWriter, r *http.Request) {
		if subtitleSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("subtitles unavailable"))
			return
		}
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "libraryItemId"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := uploadSubtitleRequest(r, subtitleSvc, libraryItemID)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		publishMutation("subtitle.upload", map[string]any{"libraryItemId": libraryItemID, "language": result.Language, "provider": result.Provider})
		respondJSON(w, http.StatusCreated, result)
	})
	r.Post("/api/subtitle-candidates/{id}/download", func(w http.ResponseWriter, r *http.Request) {
		if subtitleSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("subtitles unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := subtitleSvc.DownloadCandidate(r.Context(), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("subtitle.download", map[string]any{"libraryItemId": result.LibraryItemID, "language": result.Language, "provider": result.Provider})
		respondJSON(w, http.StatusCreated, result)
	})
	r.Delete("/api/subtitle-files/{id}", func(w http.ResponseWriter, r *http.Request) {
		if subtitleSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("subtitles unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		if err := subtitleSvc.DeleteSubtitle(r.Context(), id); err != nil {
			respondError(w, http.StatusNotFound, err)
			return
		}
		publishMutation("subtitle.delete", map[string]any{"subtitleFileId": id})
		respondJSON(w, http.StatusOK, map[string]any{"status": "deleted", "subtitleFileId": id})
	})
	r.Get("/api/blocklist", func(w http.ResponseWriter, r *http.Request) {
		if blocklistSvc == nil {
			respondJSON(w, http.StatusOK, database.BlocklistPage{Items: []database.BlocklistItemSummary{}, Page: 1, PageSize: 50, Total: 0, TotalPages: 1})
			return
		}
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		pageSize, _ := strconv.Atoi(q.Get("pageSize"))
		result, err := blocklistSvc.ListPaged(r.Context(), database.BlocklistFilter{
			Q:        q.Get("q"),
			Reason:   q.Get("reason"),
			Page:     page,
			PageSize: pageSize,
			Sort:     q.Get("sort"),
			Dir:      q.Get("dir"),
		})
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, result)
	})
	r.Get("/api/blocklist/stats", func(w http.ResponseWriter, r *http.Request) {
		if blocklistSvc == nil {
			respondJSON(w, http.StatusOK, database.BlocklistStats{ByReason: map[string]int{}})
			return
		}
		stats, err := blocklistSvc.Stats(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, stats)
	})
	r.Post("/api/blocklist/manual", func(w http.ResponseWriter, r *http.Request) {
		if blocklistSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("blocklist unavailable"))
			return
		}
		item, err := parseManualBlocklistMutation(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		created, err := blocklistSvc.Create(r.Context(), item)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("blocklist.create", map[string]any{"blocklistItemId": created.ID, "reason": created.Reason})
		respondJSON(w, http.StatusCreated, created)
	})
	r.Put("/api/blocklist/{id}", func(w http.ResponseWriter, r *http.Request) {
		if blocklistSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("blocklist unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		item, err := parseManualBlocklistMutation(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		updated, err := blocklistSvc.Update(r.Context(), id, item)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("blocklist.update", map[string]any{"blocklistItemId": updated.ID, "reason": updated.Reason})
		respondJSON(w, http.StatusOK, updated)
	})
	r.Delete("/api/blocklist/{id}", func(w http.ResponseWriter, r *http.Request) {
		if blocklistSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("blocklist unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		if err := blocklistSvc.Clear(r.Context(), id); err != nil {
			respondError(w, http.StatusNotFound, err)
			return
		}
		publishMutation("blocklist.clear", map[string]any{"blocklistItemId": id})
		respondJSON(w, http.StatusOK, map[string]any{"status": "cleared", "blocklistItemId": id})
	})
	r.Delete("/api/blocklist", func(w http.ResponseWriter, r *http.Request) {
		if blocklistSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("blocklist unavailable"))
			return
		}
		if reason := r.URL.Query().Get("reason"); reason != "" {
			result, err := blocklistSvc.ClearByReason(r.Context(), reason)
			if err != nil {
				respondError(w, http.StatusInternalServerError, err)
				return
			}
			publishMutation("blocklist.clear_reason", map[string]any{"reason": reason, "cleared": result.Cleared})
			respondJSON(w, http.StatusOK, result)
			return
		}
		result, err := blocklistSvc.ClearAll(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("blocklist.clear_all", map[string]any{"cleared": result.Cleared})
		respondJSON(w, http.StatusOK, result)
	})
	// Seerr webhook — Seerr calls this URL when a request is approved/available.
	// Configure in Seerr → Settings → Notifications → Webhook with URL:
	//   http://<drakkar-host>:8080/api/webhooks/seerr
	r.Post("/api/webhooks/seerr", func(w http.ResponseWriter, r *http.Request) {
		var payload seerr.WebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		// Acknowledge immediately — never let Seerr time out waiting.
		respondJSON(w, http.StatusOK, map[string]any{"received": true})
		if !payload.IsActionable() {
			return
		}
		if workflowSvc == nil {
			return
		}
		// Trigger a sync in the background so this request returns fast.
		go func() {
			bgCtx := context.Background()
			result, err := workflowSvc.SyncRequests(bgCtx)
			if err != nil {
				slog.Error("seerr webhook: SyncRequests failed", "error", err)
				return
			}
			publishMutation("requests.sync_webhook", map[string]any{
				"notification": payload.NotificationType,
				"seen":         result.Seen,
				"created":      result.Created,
			})
			// Items created from webhook get priority 0 (highest) so they
			// jump ahead of normal monitoring items.
			if result.Created > 0 {
				if wq, ok := workflowSvc.(interface {
					PushLibraryItemsToQueue(ids []int64, priority int)
				}); ok {
					wq.PushLibraryItemsToQueue(result.CreatedLibraryItemIDs, 0)
				} else {
					for _, id := range result.CreatedLibraryItemIDs {
						workflowSvc.SearchLibrary(bgCtx, id) //nolint:errcheck
					}
				}
			}
		}()
	})
	r.Get("/api/streams", func(w http.ResponseWriter, r *http.Request) {
		var sessions []stream.SessionSnapshot
		if streamsProvider != nil {
			sessions = streamsProvider.ActiveSessions()
		}
		if sessions == nil {
			sessions = []stream.SessionSnapshot{}
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": sessions})
	})
	r.Post("/api/requests/sync", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		result, err := workflowSvc.SyncRequests(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("requests.sync", map[string]any{"seen": result.Seen, "created": result.Created})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/requests/push-library", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		go func() {
			result, err := workflowSvc.PushMissingLibraryItemsToSeerr(context.Background())
			if err != nil {
				slog.Error("push missing to seerr background", "err", err)
				return
			}
			publishMutation("library.push_library", map[string]any{"moviesPushed": result.MoviesPushed, "showsPushed": result.ShowsPushed, "moviesSkipped": result.MoviesSkipped, "showsSkipped": result.ShowsSkipped})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Post("/api/requests/sync-plex-detected", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		result, err := workflowSvc.SyncPlexDetectedShows(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, result)
	})
	r.Post("/api/discover/request", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		var body struct {
			MediaType string `json:"mediaType"`
			TmdbID    int64  `json:"tmdbId"`
			Seasons   []int  `json:"seasons"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.TmdbID == 0 {
			respondError(w, http.StatusBadRequest, errors.New("tmdbId and mediaType required"))
			return
		}
		var (
			result workflow.SyncResult
			err    error
		)
		if body.MediaType == "tv" && len(body.Seasons) > 0 {
			result, err = workflowSvc.CreateSeerrSeasonRequest(r.Context(), body.TmdbID, body.Seasons)
		} else {
			result, err = workflowSvc.CreateSeerrRequest(r.Context(), body.MediaType, body.TmdbID)
		}
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("requests.sync", map[string]any{"seen": result.Seen, "created": result.Created})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/library/search-pending", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		go func() {
			result, err := workflowSvc.SearchPendingLibrary(context.Background())
			if err != nil {
				slog.Error("search pending background", "err", err)
				return
			}
			publishMutation("library.search_pending", map[string]any{"processed": result.Processed, "searched": result.Searched, "selected": result.Selected, "failed": result.Failed})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Post("/api/library/{id}/search", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		go func() {
			result, err := workflowSvc.SearchLibrary(context.Background(), id)
			if err != nil {
				slog.Warn("search library background", "library_item_id", id, "err", err)
				return
			}
			publishMutation("library.search", map[string]any{"libraryItemId": id, "candidateCount": result.CandidateCount, "selectedReleaseId": result.SelectedReleaseID})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Post("/api/tv-shows/{id}/prioritize-missing", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := workflowSvc.PrioritizeTVShowMissing(r.Context(), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("tv.prioritize_missing", map[string]any{"tvShowId": id, "queued": result.Queued, "itemsCreated": result.ItemsCreated})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/library/{id}/reset", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		if err := workflowSvc.ResetLibraryItem(r.Context(), id); err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("library.reset", map[string]any{"libraryItemId": id})
		respondJSON(w, http.StatusOK, map[string]any{"libraryItemId": id})
	})
	r.Post("/api/releases/{id}/select", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := workflowSvc.SelectRelease(workflow.WithAsyncDownload(r.Context()), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		if result.SelectedReleaseID == nil && result.Action == "selected" {
			// Candidate was replaced by a newer search — tell the client to refresh.
			respondError(w, http.StatusGone, errors.New("release candidate no longer available — please refresh and try again"))
			return
		}
		publishMutation("release.select", map[string]any{"releaseCandidateId": id, "selectedReleaseId": result.SelectedReleaseID})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/releases/{id}/reject", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		var payload struct {
			Reason string `json:"reason"`
		}
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&payload)
		}
		result, err := workflowSvc.RejectRelease(workflow.WithAsyncDownload(r.Context()), id, payload.Reason)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("release.reject", map[string]any{"releaseCandidateId": id, "selectedReleaseId": result.SelectedReleaseID})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/releases/{id}/restore", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := workflowSvc.RestoreRelease(r.Context(), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("release.restore", map[string]any{"releaseCandidateId": id})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/releases/{id}/skip", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := workflowSvc.SkipRelease(workflow.WithAsyncDownload(r.Context()), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("release.skip", map[string]any{"releaseCandidateId": id, "selectedReleaseId": result.SelectedReleaseID})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/library/{id}/republish", func(w http.ResponseWriter, r *http.Request) {
		if publication == nil {
			respondError(w, http.StatusNotImplemented, errors.New("publication unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		if err := publication.RepublishLibraryItem(r.Context(), id); err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("library.republish", map[string]any{"libraryItemId": id})
		respondJSON(w, http.StatusAccepted, map[string]any{"status": "republished", "libraryItemId": id})
	})
	r.Post("/api/library/{id}/restore-rejected", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		result, err := workflowSvc.RestoreRejectedReleases(r.Context(), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("library.restore_rejected", map[string]any{"libraryItemId": id, "restored": result.Restored})
		respondJSON(w, http.StatusAccepted, result)
	})
	r.Post("/api/library/republish-pending", func(w http.ResponseWriter, r *http.Request) {
		if publication == nil {
			respondError(w, http.StatusNotImplemented, errors.New("publication unavailable"))
			return
		}
		go func() {
			result, err := publication.RepublishPendingLibrary(context.Background())
			if err != nil {
				slog.Error("republish pending background", "err", err)
				return
			}
			publishMutation("library.republish_pending", map[string]any{"processed": result.Processed, "republished": result.Republished, "failed": result.Failed})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Post("/api/library/reset-orphaned-available", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		go func() {
			result, err := workflowSvc.ResetOrphanedAvailableItems(context.Background())
			if err != nil {
				slog.Error("reset orphaned available background", "err", err)
				return
			}
			publishMutation("library.reset_orphaned", map[string]any{"found": result.Found, "reset": result.Reset, "failed": result.Failed})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Post("/api/library/backfill-metadata", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		go func() {
			result, err := workflowSvc.BackfillMetadata(context.Background())
			if err != nil {
				slog.Error("backfill metadata background", "err", err)
				return
			}
			publishMutation("library.backfill_metadata", map[string]any{"processedMovies": result.ProcessedMovies, "processedShows": result.ProcessedShows, "enriched": result.Enriched, "failed": result.Failed})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Post("/api/library/fill-missing-episodes", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		go func() {
			result, err := workflowSvc.FillMissingEpisodes(context.Background())
			if err != nil {
				slog.Error("fill missing episodes background", "err", err)
				return
			}
			publishMutation("library.fill_missing_episodes", map[string]any{"showsProcessed": result.ShowsProcessed, "episodesFound": result.EpisodesFound, "itemsCreated": result.ItemsCreated})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Post("/api/cache/prune", func(w http.ResponseWriter, r *http.Request) {
		if cacheSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("cache pruning unavailable"))
			return
		}
		go func() {
			result, err := cacheSvc.Prune(context.Background())
			if err != nil {
				slog.Error("cache prune background", "err", err)
				return
			}
			publishMutation("cache.prune", map[string]any{"deletedFiles": result.DeletedFiles, "deletedBytes": result.DeletedBytes})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Post("/api/maintenance/nzb-health-check", func(w http.ResponseWriter, r *http.Request) {
		if maintenance == nil {
			respondError(w, http.StatusNotImplemented, errors.New("maintenance unavailable"))
			return
		}
		go func() {
			result, err := maintenance.DeepNZBHealthCheck(context.Background())
			if err != nil {
				slog.Error("nzb health check background", "err", err)
				return
			}
			publishMutation("maintenance.nzb_health_check", map[string]any{"scannedRows": result.ScannedRows, "resetItems": result.ResetItems})
		}()
		respondJSON(w, http.StatusAccepted, map[string]any{"queued": true})
	})
	r.Get("/api/events", broker.ServeHTTP)
	r.Get("/api/health/summary", func(w http.ResponseWriter, r *http.Request) {
		if healthRepo == nil {
			respondJSON(w, http.StatusOK, database.HealthSummary{})
			return
		}
		summary, err := healthRepo.HealthSummary(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, summary)
	})
	r.Get("/api/health/entries", func(w http.ResponseWriter, r *http.Request) {
		if healthRepo == nil {
			respondJSON(w, http.StatusOK, database.HealthEntriesPage{Items: []database.HealthEntry{}})
			return
		}
		filter := r.URL.Query().Get("filter") // "all" | "broken" | "unchecked"
		limit := 100
		offset := 0
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
				limit = n
			}
		}
		if v := r.URL.Query().Get("offset"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				offset = n
			}
		}
		page, err := healthRepo.ListHealthEntriesPage(r.Context(), filter, limit, offset)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, page)
	})
	r.Post("/api/health/check", func(w http.ResponseWriter, r *http.Request) {
		if healthRepo == nil {
			respondJSON(w, http.StatusOK, map[string]any{"checked": 0, "healthy": 0})
			return
		}
		entries, err := healthRepo.ListHealthEntries(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		var checked, healthy int
		for _, e := range entries {
			isOK := database.CheckSymlinkHealth(e.LibraryPath, e.TargetPath)
			_ = healthRepo.RecordHealthCheck(r.Context(), e.ID, isOK)
			checked++
			if isOK {
				healthy++
			}
		}
		publishMutation("health.check", map[string]any{"checked": checked, "healthy": healthy})
		respondJSON(w, http.StatusOK, map[string]any{"checked": checked, "healthy": healthy})
	})
	r.Get("/api/health/consistency", func(w http.ResponseWriter, r *http.Request) {
		if healthRepo == nil {
			respondJSON(w, http.StatusOK, map[string]any{"items": []any{}})
			return
		}
		issues, err := healthRepo.ListConsistencyIssues(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		if issues == nil {
			issues = []database.ConsistencyIssue{}
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": issues})
	})
	r.Get("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
		var snap metrics.Snapshot
		if len(metricsProvider) > 0 && metricsProvider[0] != nil {
			snap = metricsProvider[0].Collect()
		} else {
			snap = metrics.M.Collect(metrics.NNTPStats{}, metrics.CacheStats{}, metrics.QueueStats{})
		}
		respondJSON(w, http.StatusOK, snap)
	})
	r.Get("/api/tasks/schedules", func(w http.ResponseWriter, r *http.Request) {
		if taskSchedules == nil {
			respondJSON(w, http.StatusOK, map[string]any{"items": []TaskSchedule{}})
			return
		}
		items, err := taskSchedules.ListTaskSchedules(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		if items == nil {
			items = []TaskSchedule{}
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	r.Get("/api/policies", func(w http.ResponseWriter, r *http.Request) {
		if policySvc == nil {
			respondJSON(w, http.StatusOK, policy.DefaultSettings())
			return
		}
		settings, err := policySvc.Settings(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, settings)
	})
	r.Put("/api/policies", func(w http.ResponseWriter, r *http.Request) {
		if policySvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("policy service unavailable"))
			return
		}
		var input policy.Settings
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		settings, err := policySvc.Update(r.Context(), input)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("policies.update", map[string]any{})
		respondJSON(w, http.StatusOK, settings)
	})
	r.Get("/api/settings", func(w http.ResponseWriter, r *http.Request) {
		if settingsSvc == nil {
			respondError(w, http.StatusServiceUnavailable, errors.New("settings service unavailable"))
			return
		}
		cfg, err := settingsSvc.GetSettings(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, cfg)
	})
	r.Put("/api/settings", func(w http.ResponseWriter, r *http.Request) {
		if settingsSvc == nil {
			respondError(w, http.StatusServiceUnavailable, errors.New("settings service unavailable"))
			return
		}
		var input config.Settings
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			respondError(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
			return
		}
		cfg, err := settingsSvc.UpdateSettings(r.Context(), input)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		publishMutation("settings.update", map[string]any{})
		respondJSON(w, http.StatusOK, cfg)
	})

	// Recent structured log lines from the application log file.
	r.Get("/api/logs", func(w http.ResponseWriter, r *http.Request) {
		limitStr := r.URL.Query().Get("limit")
		limit := 200
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 && n <= 2000 {
			limit = n
		}
		levelFilter := strings.ToLower(r.URL.Query().Get("level"))
		logFile := config.DefaultLogsPath + "/drakkar.log"
		data, err := os.ReadFile(logFile)
		if err != nil {
			respondJSON(w, http.StatusOK, map[string]any{"lines": []any{}})
			return
		}
		rawLines := strings.Split(strings.TrimSpace(string(data)), "\n")
		// Take the last `limit` lines, apply level filter.
		type LogLine struct {
			Raw string `json:"raw"`
		}
		var out []LogLine
		for i := len(rawLines) - 1; i >= 0 && len(out) < limit; i-- {
			line := rawLines[i]
			if line == "" {
				continue
			}
			if levelFilter != "" && !strings.Contains(strings.ToLower(line), `"level":"`+levelFilter+`"`) {
				continue
			}
			out = append(out, LogLine{Raw: line})
		}
		// Reverse so newest is last.
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
		respondJSON(w, http.StatusOK, map[string]any{"lines": out})
	})
	// Quality profiles — CRUD for user-configurable release ranking preferences.
	r.Get("/api/profiles", func(w http.ResponseWriter, r *http.Request) {
		if profilesRepo == nil {
			respondJSON(w, http.StatusOK, map[string]any{"profiles": []any{}})
			return
		}
		profiles, err := profilesRepo.ListQualityProfiles(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		if profiles == nil {
			profiles = []database.QualityProfile{}
		}
		respondJSON(w, http.StatusOK, map[string]any{"profiles": profiles})
	})
	r.Post("/api/profiles", func(w http.ResponseWriter, r *http.Request) {
		if profilesRepo == nil {
			respondError(w, http.StatusNotImplemented, errors.New("profiles unavailable"))
			return
		}
		var p database.QualityProfile
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		saved, err := profilesRepo.UpsertQualityProfile(r.Context(), p)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, saved)
	})
	r.Delete("/api/profiles/{id}", func(w http.ResponseWriter, r *http.Request) {
		if profilesRepo == nil {
			respondError(w, http.StatusNotImplemented, errors.New("profiles unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		if err := profilesRepo.DeleteQualityProfile(r.Context(), id); err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"deleted": id})
	})
	r.Get("/api/quality-definitions", func(w http.ResponseWriter, r *http.Request) {
		if profilesRepo == nil {
			respondError(w, http.StatusNotImplemented, errors.New("profiles unavailable"))
			return
		}
		defs, err := profilesRepo.ListQualityDefinitions(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"definitions": defs})
	})
	r.Put("/api/quality-definitions/{id}", func(w http.ResponseWriter, r *http.Request) {
		if profilesRepo == nil {
			respondError(w, http.StatusNotImplemented, errors.New("profiles unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		var d database.QualityDefinition
		if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		d.ID = id
		updated, err := profilesRepo.UpdateQualityDefinition(r.Context(), d)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, updated)
	})
	// Manual search — proxy a free-text Hydra query and return scored candidates.
	r.Get("/api/search/manual", func(w http.ResponseWriter, r *http.Request) {
		if workflowSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("workflow unavailable"))
			return
		}
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		if q == "" {
			respondJSON(w, http.StatusOK, map[string]any{"items": []any{}})
			return
		}
		candidates, err := workflowSvc.ManualSearch(r.Context(), q)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": candidates})
	})

	// Release calendar — upcoming releases from TMDB.
	r.Get("/api/release-calendar", func(w http.ResponseWriter, r *http.Request) {
		if catalogSvc == nil {
			respondError(w, http.StatusNotImplemented, errors.New("catalog unavailable"))
			return
		}
		month := r.URL.Query().Get("month") // "YYYY-MM", defaults to current
		entries, err := catalogSvc.ReleaseCalendar(r.Context(), month)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"entries": entries})
	})

	r.Post("/api/streams/{sessionId}/stop", func(w http.ResponseWriter, r *http.Request) {
		if streamsProvider == nil {
			respondError(w, http.StatusNotImplemented, errors.New("streams unavailable"))
			return
		}
		sessionID := chi.URLParam(r, "sessionId")
		streamsProvider.Stop(sessionID)
		respondJSON(w, http.StatusOK, map[string]any{"stopped": true})
	})

	// Per-library-item quality profile override.
	r.Get("/api/library/{id}/profile", func(w http.ResponseWriter, r *http.Request) {
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		p, err := profilesRepo.GetLibraryItemQualityProfile(r.Context(), libraryItemID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"profile": p})
	})
	r.Put("/api/library/{id}/profile", func(w http.ResponseWriter, r *http.Request) {
		libraryItemID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		var body struct {
			ProfileID *int64 `json:"profileId"` // null = clear override
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		if err := profilesRepo.SetLibraryItemQualityProfile(r.Context(), libraryItemID, body.ProfileID); err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"libraryItemId": libraryItemID, "profileId": body.ProfileID})
	})

	// VFS browser — returns empty; content is served via HTTP at /content/{id}/{filename}.
	r.Get("/api/vfs", func(w http.ResponseWriter, r *http.Request) {
		base := status.Status().FuseMountPath
		if base == "" {
			base = config.DefaultFuseMountPath
		}
		reqPath := strings.TrimSpace(r.URL.Query().Get("path"))
		if reqPath == "" {
			reqPath = "/"
		}
		// Prevent directory traversal by cleaning the path within the virtual root.
		clean := "/" + strings.Trim(strings.ReplaceAll(reqPath, "..", ""), "/")
		fullPath := base + clean
		entries, err := os.ReadDir(fullPath)
		if err != nil {
			if os.IsNotExist(err) {
				respondJSON(w, http.StatusOK, map[string]any{"path": clean, "entries": []any{}})
				return
			}
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		type VFSEntry struct {
			Name  string `json:"name"`
			Path  string `json:"path"`
			IsDir bool   `json:"isDir"`
			Size  int64  `json:"size"`
		}
		result := make([]VFSEntry, 0, len(entries))
		for _, e := range entries {
			entryPath := clean
			if entryPath == "/" {
				entryPath = "/" + e.Name()
			} else {
				entryPath = entryPath + "/" + e.Name()
			}
			size := int64(0)
			if !e.IsDir() {
				if info, err := e.Info(); err == nil {
					size = info.Size()
				}
			}
			result = append(result, VFSEntry{Name: e.Name(), Path: entryPath, IsDir: e.IsDir(), Size: size})
		}
		respondJSON(w, http.StatusOK, map[string]any{"path": clean, "entries": result})
	})
	// Plex integration
	r.Post("/api/plex/test", func(w http.ResponseWriter, r *http.Request) {
		if plexClient == nil || !plexClient.Enabled() {
			respondJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "plex not configured"})
			return
		}
		result := plexClient.Test(r.Context())
		respondJSON(w, http.StatusOK, result)
	})
	r.Post("/api/plex/refresh", func(w http.ResponseWriter, r *http.Request) {
		if plexClient == nil || !plexClient.Enabled() {
			respondError(w, http.StatusNotImplemented, errors.New("plex not configured"))
			return
		}
		if err := plexClient.RefreshSection(r.Context(), ""); err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"status": "refreshed"})
	})
	r.Get("/api/plex/libraries", func(w http.ResponseWriter, r *http.Request) {
		if plexClient == nil || !plexClient.Enabled() {
			respondJSON(w, http.StatusOK, map[string]any{"libraries": []any{}})
			return
		}
		libs, err := plexClient.Libraries(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"libraries": libs})
	})
	// Plex OAuth PIN flow
	r.Post("/api/plex/oauth/start", func(w http.ResponseWriter, r *http.Request) {
		pin, err := plex.StartOAuth(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, pin)
	})
	r.Post("/api/plex/oauth/poll", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			PinID int64 `json:"pinId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PinID <= 0 {
			respondError(w, http.StatusBadRequest, errors.New("pinId required"))
			return
		}
		result, err := plex.PollOAuth(r.Context(), body.PinID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, result)
	})

	// Jellyfin integration
	r.Post("/api/jellyfin/test", func(w http.ResponseWriter, r *http.Request) {
		if jellyfinClient == nil || !jellyfinClient.Enabled() {
			respondJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "jellyfin not configured"})
			return
		}
		result := jellyfinClient.Test(r.Context())
		respondJSON(w, http.StatusOK, result)
	})
	r.Post("/api/jellyfin/refresh", func(w http.ResponseWriter, r *http.Request) {
		if jellyfinClient == nil || !jellyfinClient.Enabled() {
			respondError(w, http.StatusNotImplemented, errors.New("jellyfin not configured"))
			return
		}
		if err := jellyfinClient.RefreshLibraries(r.Context()); err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"status": "refreshed"})
	})

	// ── Grab history ────────────────────────────────────────────────────────────
	r.Get("/api/library/{id}/grab-history", func(w http.ResponseWriter, r *http.Request) {
		if profilesRepo == nil {
			respondJSON(w, http.StatusOK, map[string]any{"items": []any{}})
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		entries, err := profilesRepo.GetGrabHistory(r.Context(), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		if entries == nil {
			entries = []database.GrabHistoryEntry{}
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": entries})
	})

	// ── Custom formats, release block rules, indexer policies, subtitle profiles ──
	registerProfileRoutes(r, profilesRepo)
	// ── TV show monitoring mode ──────────────────────────────────────────────────
	r.Put("/api/tv-shows/{id}/monitoring", func(w http.ResponseWriter, r *http.Request) {
		if profilesRepo == nil {
			respondError(w, http.StatusNotImplemented, errors.New("profiles unavailable"))
			return
		}
		id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		var body struct {
			Mode string `json:"mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondError(w, http.StatusBadRequest, err)
			return
		}
		validModes := map[string]bool{"all": true, "future": true, "missing": true, "recent": true, "pilot": true, "none": true}
		if !validModes[body.Mode] {
			respondError(w, http.StatusBadRequest, fmt.Errorf("invalid monitoring mode: %q", body.Mode))
			return
		}
		if err := profilesRepo.SetTVShowMonitoringMode(r.Context(), id, body.Mode); err != nil {
			respondError(w, http.StatusInternalServerError, err)
			return
		}
		publishMutation("tv_show.monitoring_mode", map[string]any{"tvShowId": id, "mode": body.Mode})
		respondJSON(w, http.StatusOK, map[string]any{"tvShowId": id, "mode": body.Mode})
	})

	// ── Auth, setup and user management ─────────────────────────────────────────
	mountSetupRoutes(r, userRepo)
	mountAuthRoutes(r, userRepo)
	mountUserRoutes(r, userRepo)

	// OpenAPI spec and Scalar docs UI (public).
	r.Get("/openapi.json", serveOpenAPISpec)
	r.Get("/docs", serveScalarDocs)

	// Serve the embedded SvelteKit SPA for all non-API routes, with SPA fallback.
	r.Mount("/", frontend.Handler())
	return r
}

// authMiddlewareFor builds the auth middleware using the user repo.
// Public prefixes (setup, login/logout, webhooks, sabnzbd) pass through unauthenticated.
// When repo is nil (e.g. in tests) all requests pass through unauthenticated.
func authMiddlewareFor(repo UserRepository) func(http.Handler) http.Handler {
	if repo == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	exempt := []string{
		"/api/setup/",
		"/api/auth/login",
		"/api/auth/logout",
		"/api/webhooks/",
		"/dav/api",
		"/api/sabnzbd/",
		"/sabnzbd/",
		"/openapi.json",
		"/docs",
	}
	return auth.Middleware(repo, exempt)
}

func (b *EventBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", http.StatusInternalServerError)
		return
	}
	headers := w.Header()
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("Connection", "keep-alive")

	ch := make(chan []byte, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.clients, ch)
		b.mu.Unlock()
	}()

	fmt.Fprintf(w, "event: ready\ndata: {\"service\":\"drakkar\"}\n\n")
	flusher.Flush()
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, "event: heartbeat\ndata: {}\n\n")
			flusher.Flush()
		}
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Api-Key, X-API-KEY")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func emptyList(key string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, http.StatusOK, map[string]any{key: []any{}})
	}
}

func accepted(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, http.StatusAccepted, map[string]any{"status": kind})
	}
}

// libStatusPriority mirrors the frontend itemStatus/STATUS_ORDER for server-side sort:
// 0=available, 1=partial, 2=active/unreleased, 3=missing/failed.
func libStatusPriority(item catalog.MediaCard) int {
	switch strings.ToLower(item.QueueState) {
	case "searching", "ranking", "selected", "fetching_nzb", "indexing", "preflight", "publishing", "downloading":
		return 2
	case "failed":
		return 3
	case "requested":
		return 2
	}
	if item.Available {
		if (item.MediaType == "tv" || item.MediaType == "episode") && item.MissingCount > 0 {
			return 1
		}
		return 0
	}
	if item.MissingCount > 0 && item.AvailableCount > 0 {
		return 1
	}
	return 3
}

func respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, status int, err error) {
	respondJSON(w, status, map[string]any{
		"error": err.Error(),
	})
}

func StatusFromConfig(rt config.Runtime, cfg config.Settings, startedAt time.Time, healthy bool) Status {
	return Status{
		Service:             "drakkar",
		Version:             version.Version,
		Healthy:             healthy,
		StartedAt:           startedAt,
		Settings:            config.RedactedSettings(cfg),
		Integrations:        integrationStatusFromConfig(cfg),
		FuseMountPath:       rt.FuseMountPath,
		DiskCacheLimitBytes: rt.DiskCacheLimitBytes,
		ReadAheadLimitBytes: rt.ReadAheadLimitBytes,
		MemoryHotCacheBytes: rt.MemoryHotCacheMaxBytes,
	}
}

func integrationStatusFromConfig(cfg config.Settings) Integrations {
	subtitleProviders := make(map[string]IntegrationStatus, len(cfg.Subtitles.Providers))
	configuredSubtitleProviders := 0
	enabledSubtitleProviders := 0
	for name, provider := range cfg.Subtitles.Providers {
		status := subtitleProviderStatus(provider)
		subtitleProviders[name] = status
		if status.Enabled {
			enabledSubtitleProviders++
		}
		if status.Configured {
			configuredSubtitleProviders++
		}
	}

	usenetEnabled := 0
	usenetConfigured := 0
	for _, provider := range cfg.Usenet.Providers {
		if provider.Enabled {
			usenetEnabled++
		}
		if provider.Enabled && provider.Host != "" && provider.Username != "" && provider.Password != "" {
			usenetConfigured++
		}
	}

	subtitles := IntegrationStatus{
		Enabled:    cfg.Subtitles.Enabled,
		Configured: cfg.Subtitles.Enabled && len(cfg.Subtitles.Languages) > 0 && configuredSubtitleProviders > 0,
		Count:      configuredSubtitleProviders,
	}
	switch {
	case !cfg.Subtitles.Enabled:
		subtitles.Detail = "disabled"
	case len(cfg.Subtitles.Languages) == 0:
		subtitles.Detail = "no subtitle languages configured"
	case enabledSubtitleProviders == 0:
		subtitles.Detail = "no subtitle providers enabled"
	case configuredSubtitleProviders == 0:
		subtitles.Detail = "subtitle providers enabled but credentials missing"
	default:
		subtitles.Detail = fmt.Sprintf("%d subtitle provider(s) configured", configuredSubtitleProviders)
	}

	return Integrations{
		Seerr: integrationStatus(
			cfg.Seerr.URL != "",
			cfg.Seerr.URL != "" && cfg.Seerr.APIKey != "",
			"URL and API key required",
			"configured",
		),
		NZBHydra2: integrationStatus(
			cfg.NZBHydra2.URL != "",
			cfg.NZBHydra2.URL != "" && cfg.NZBHydra2.APIKey != "",
			"URL and API key required",
			"configured",
		),
		Usenet: IntegrationStatus{
			Enabled:    usenetEnabled > 0,
			Configured: usenetConfigured > 0,
			Count:      usenetConfigured,
			Detail:     usenetDetail(usenetEnabled, usenetConfigured),
		},
		TMDB: integrationStatus(
			true,
			cfg.Metadata.TMDB.APIKey != "",
			"API key required",
			"configured",
		),
		TVDB: integrationStatus(
			true,
			cfg.Metadata.TVDB.APIKey != "",
			"API key required",
			"configured",
		),
		Subtitles:         subtitles,
		SubtitleProviders: subtitleProviders,
	}
}

func integrationStatus(enabled, configured bool, missingDetail, configuredDetail string) IntegrationStatus {
	status := IntegrationStatus{
		Enabled:    enabled,
		Configured: configured,
	}
	if configured {
		status.Detail = configuredDetail
	} else {
		status.Detail = missingDetail
	}
	return status
}

func subtitleProviderStatus(provider config.SubtitleAuth) IntegrationStatus {
	switch {
	case !provider.Enabled:
		return IntegrationStatus{Enabled: false, Configured: false, Detail: "disabled"}
	case provider.APIKey == "":
		return IntegrationStatus{Enabled: true, Configured: false, Detail: "API key required"}
	case provider.Username == "" && provider.Password != "":
		return IntegrationStatus{Enabled: true, Configured: false, Detail: "username required"}
	case provider.Username != "" && provider.Password == "":
		return IntegrationStatus{Enabled: true, Configured: false, Detail: "password required"}
	default:
		return IntegrationStatus{Enabled: true, Configured: true, Detail: "configured"}
	}
}

func usenetDetail(enabled, configured int) string {
	switch {
	case enabled == 0:
		return "no enabled providers"
	case configured == 0:
		return "enabled providers missing host or credentials"
	default:
		return fmt.Sprintf("%d provider(s) configured", configured)
	}
}

func importNZBRequest(r *http.Request, queue QueueService) (database.QueueSnapshot, error) {
	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		return importMultipartNZB(r, queue)
	}
	fileName := nzb.ImportRawBodyName(r.Header.Get("Content-Disposition"))
	return queue.ImportNZB(r.Context(), fileName, r.Body)
}

func importMultipartNZB(r *http.Request, queue QueueService) (database.QueueSnapshot, error) {
	if err := r.ParseMultipartForm(2 << 20); err != nil {
		return database.QueueSnapshot{}, err
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		return database.QueueSnapshot{}, err
	}
	defer file.Close()
	return queue.ImportNZB(r.Context(), multipartFileName(header), file)
}

func multipartFileName(header *multipart.FileHeader) string {
	if header == nil {
		return "imported.nzb"
	}
	return nzb.ImportHTTPFileName(header.Filename)
}

func uploadSubtitleRequest(r *http.Request, subtitles SubtitleService, libraryItemID int64) (intsub.UploadResult, error) {
	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		if err := r.ParseMultipartForm(4 << 20); err != nil {
			return intsub.UploadResult{}, err
		}
		language := r.FormValue("language")
		file, header, err := r.FormFile("file")
		if err != nil {
			return intsub.UploadResult{}, err
		}
		defer file.Close()
		return subtitles.UploadSubtitle(r.Context(), libraryItemID, language, multipartFileName(header), file)
	}
	language := r.URL.Query().Get("language")
	fileName := r.URL.Query().Get("fileName")
	if fileName == "" {
		fileName = "subtitle.srt"
	}
	return subtitles.UploadSubtitle(r.Context(), libraryItemID, language, fileName, r.Body)
}

func parseManualBlocklistMutation(r *http.Request) (database.BlocklistMutation, error) {
	var body struct {
		Key          string     `json:"key"`
		KeyType      string     `json:"keyType"`
		ExternalURL  string     `json:"externalUrl"`
		ReleaseTitle string     `json:"releaseTitle"`
		IndexerName  string     `json:"indexerName"`
		SizeMB       int64      `json:"sizeMb"`
		PostedDate   string     `json:"postedDate"`
		Reason       string     `json:"reason"`
		ExpiresAt    *time.Time `json:"expiresAt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return database.BlocklistMutation{}, err
	}
	key, err := manualBlocklistKey(body.KeyType, body.Key, body.ExternalURL, body.ReleaseTitle, body.IndexerName, body.SizeMB, body.PostedDate)
	if err != nil {
		return database.BlocklistMutation{}, err
	}
	return database.BlocklistMutation{
		Key:       key,
		Reason:    strings.TrimSpace(body.Reason),
		ExpiresAt: body.ExpiresAt,
	}, nil
}

func manualBlocklistKey(keyType, rawKey, externalURL, releaseTitle, indexerName string, sizeMB int64, postedDate string) (string, error) {
	switch strings.TrimSpace(keyType) {
	case "", "raw":
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return "", errors.New("key is required")
		}
		return key, nil
	case "external_url":
		if strings.TrimSpace(externalURL) == "" {
			return "", errors.New("externalUrl is required")
		}
		return "external_url:" + strings.TrimSpace(externalURL), nil
	case "release_signature":
		titleKey := normalizeBlocklistTitle(strings.TrimSpace(releaseTitle))
		if titleKey == "" {
			return "", errors.New("releaseTitle is required")
		}
		indexerKey := normalizeBlocklistTitle(strings.TrimSpace(indexerName))
		sizeBucket := "0"
		if sizeMB > 0 {
			sizeBucket = strconv.FormatInt(sizeMB, 10)
		}
		dateBucket := "none"
		if strings.TrimSpace(postedDate) != "" {
			value, err := time.Parse("2006-01-02", strings.TrimSpace(postedDate))
			if err != nil {
				return "", fmt.Errorf("invalid postedDate: %w", err)
			}
			dateBucket = value.UTC().Format("2006-01-02")
		}
		return "release_signature:" + strings.Join([]string{titleKey, indexerKey, sizeBucket, dateBucket}, "|"), nil
	default:
		return "", fmt.Errorf("unsupported keyType %q", keyType)
	}
}

func normalizeBlocklistTitle(value string) string {
	replacer := strings.NewReplacer(".", " ", "_", " ", "-", " ", "[", " ", "]", " ", "(", " ", ")", " ", "{", " ", "}", " ")
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(strings.TrimSpace(value)))), " ")
}

func validateReleaseBlockRule(r database.ReleaseBlockRule) error {
	validTypes := map[string]bool{"release_group": true, "title_pattern": true, "regex": true, "missing_release_group": true}
	if !validTypes[r.Type] {
		return fmt.Errorf("type must be one of: release_group, title_pattern, regex, missing_release_group")
	}
	validMediaTypes := map[string]bool{"movie": true, "tv": true, "both": true}
	if !validMediaTypes[r.MediaType] {
		return fmt.Errorf("mediaType must be one of: movie, tv, both")
	}
	validActions := map[string]bool{"block": true, "penalty": true}
	if !validActions[r.Action] {
		return fmt.Errorf("action must be one of: block, penalty")
	}
	if r.ScorePenalty < 0 {
		return fmt.Errorf("scorePenalty must be >= 0")
	}
	if r.Type != "missing_release_group" && strings.TrimSpace(r.Pattern) == "" {
		return fmt.Errorf("pattern is required for type %q", r.Type)
	}
	if r.Type == "regex" {
		if _, err := regexp.Compile(r.Pattern); err != nil {
			return fmt.Errorf("invalid regex pattern: %w", err)
		}
	}
	return nil
}
