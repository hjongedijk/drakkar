package workflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"database/sql"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"

	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/hydra"
	"github.com/hjongedijk/drakkar/internal/library"
	"github.com/hjongedijk/drakkar/internal/metrics"
	"github.com/hjongedijk/drakkar/internal/nzb"
	"github.com/hjongedijk/drakkar/internal/policy"
	"github.com/hjongedijk/drakkar/internal/ranking"
	"github.com/hjongedijk/drakkar/internal/seerr"
	"github.com/hjongedijk/drakkar/internal/tmdb"
	"github.com/hjongedijk/drakkar/internal/tvdb"
)

type Repository interface {
	ListMediaRequests(ctx context.Context) ([]database.MediaRequestSummary, error)
	UpsertMovieRequest(ctx context.Context, externalID string, tmdbID int64, title string, year int) (int64, bool, error)
	UpsertEpisodeRequest(ctx context.Context, externalID string, tvdbID, tmdbID int64, show string, year, season, episode int, episodeTitle string) (int64, bool, error)
	EnrichMovieMetadata(ctx context.Context, libraryItemID, tmdbID int64, title string, year int, imdbID string) error
	EnrichMovieFull(ctx context.Context, libraryItemID int64, e database.MovieEnrichment) error
	EnrichEpisodeMetadata(ctx context.Context, libraryItemID, tmdbID int64, show string, year int, imdbID, episodeTitle string) error
	EnrichTVFull(ctx context.Context, libraryItemID int64, episodeTitle string, e database.TVShowEnrichment) error
	DetectMovieSearchConflict(ctx context.Context, libraryItemID int64) (string, error)
	GetDefaultQualityProfile(ctx context.Context) (database.QualityProfile, error)
	GetQualityProfileByName(ctx context.Context, name string) (database.QualityProfile, error)
	GetLibraryItemQualityProfile(ctx context.Context, libraryItemID int64) (*database.QualityProfile, error)
	ListQualityDefinitions(ctx context.Context) ([]database.QualityDefinition, error)
	GetLibrarySearchInput(ctx context.Context, libraryItemID int64) (database.LibrarySearchInput, error)
	LookupCandidateHistory(ctx context.Context, libraryItemID int64) (map[string]database.CandidateHistory, error)
	ListPendingLibrarySearchTargets(ctx context.Context) ([]database.PendingLibrarySearchTarget, error)
	CountActiveSearchBacklog(ctx context.Context) (int, error)
	CountSelectedQueueBacklog(ctx context.Context) (int, error)
	GetShowWithMissingEpisodes(ctx context.Context, tvShowID int64) (*database.ShowWithMissingEpisodes, error)
	ListPendingTVShowLibraryItemIDs(ctx context.Context, tvShowID int64) ([]int64, error)
	ListFailedQueueRetryTargets(ctx context.Context, limit int) ([]database.FailedQueueRetryTarget, error)
	ListSelectedQueueRetryTargets(ctx context.Context, limit int) ([]database.SelectedQueueRetryTarget, error)
	ListUpgradableLibraryItems(ctx context.Context) ([]int64, error)
	ClearFailedQueueItems(ctx context.Context) (int, error)
	GetQueueRetryTarget(ctx context.Context, queueItemID int64) (database.QueueRetryTarget, error)
	BlocklistQueueSelectedRelease(ctx context.Context, queueItemID int64, reason string, ttlDays int) error
	ClearQueueSelectedRelease(ctx context.Context, queueItemID int64) error
	RequeueSelectedRelease(ctx context.Context, queueItemID int64) error
	ReplaceSearchCandidates(ctx context.Context, libraryItemID int64, candidates []database.SearchCandidateRecord) (*int64, error)
	MarkLibrarySearchFailed(ctx context.Context, libraryItemID int64, reason string) error
	GetSelectedReleaseSummary(ctx context.Context, selectedReleaseID int64) (database.ReleaseSummary, error)
	GetLatestSelectedReleaseSummaryByLibraryItem(ctx context.Context, libraryItemID int64) (*database.ReleaseSummary, error)
	GetStoredNZBDocument(ctx context.Context, selectedReleaseID int64) (database.StoredNZBDocument, error)
	PromoteBestRetryCandidate(ctx context.Context, libraryItemID int64) (*database.ReleaseSummary, error)
	PromoteAlternativeRetryCandidate(ctx context.Context, libraryItemID int64, excludeReleaseCandidateID int64) (*database.ReleaseSummary, error)
	SelectReleaseCandidate(ctx context.Context, releaseCandidateID int64) (*database.ReleaseSummary, error)
	RejectReleaseCandidate(ctx context.Context, releaseCandidateID int64, reason string) (*database.ReleaseSummary, error)
	RestoreReleaseCandidate(ctx context.Context, releaseCandidateID int64) error
	RestoreRejectedReleaseCandidates(ctx context.Context, libraryItemID int64) (database.RejectedReleaseRestoreResult, error)
	SkipReleaseCandidate(ctx context.Context, releaseCandidateID int64) (*database.ReleaseSummary, error)
	MarkSelectedReleaseFetching(ctx context.Context, selectedReleaseID int64) error
	StoreRawNZBDocument(ctx context.Context, selectedReleaseID int64, fileName string, xml []byte, externalURL string) error
	ImportSelectedReleaseNZB(ctx context.Context, selectedReleaseID int64, imported database.ImportedNZB) (database.QueueSnapshot, error)
	CalibrateNZBOffsets(ctx context.Context, nzbDocumentID int64) error
	SetImportedNZBIndexed(ctx context.Context, queueItemID int64) error
	FailSelectedReleaseAndPromoteNext(ctx context.Context, selectedReleaseID int64, reason string) (*database.ReleaseSummary, error)
	ShouldAttemptSeasonPack(ctx context.Context, tvShowID int64, season int) (bool, error)
	RecordSeasonPackAttempt(ctx context.Context, tvShowID int64, season int, outcome string) error
	ListMetadataBackfillTargets(ctx context.Context) ([]database.MetadataBackfillTarget, error)
	ListShowsWithMissingEpisodes(ctx context.Context) ([]database.ShowWithMissingEpisodes, error)
	EnsureEpisodeLibraryItem(ctx context.Context, tvShowID int64, showTitle string, seasonNum, episodeNum int, episodeTitle, airDate string) (created bool, err error)
	ListCustomFormats(ctx context.Context) ([]database.CustomFormat, error)
	ListReleaseBlockRules(ctx context.Context) ([]database.ReleaseBlockRule, error)
	LoadIndexerPolicyMap(ctx context.Context) (map[string]int, error)
	CreateImportedNZB(ctx context.Context, imported database.ImportedNZB) (database.QueueSnapshot, error)
	ListSabQueueItems(ctx context.Context, category string, start, limit int) ([]database.SabQueueItem, int, error)
	ListSabHistoryItems(ctx context.Context, category string, start, limit int) ([]database.SabHistoryItem, int, error)
	DismissSabItems(ctx context.Context, libraryItemIDs []int64) error
	DeleteSymlinkPublicationsForLibraryItem(ctx context.Context, libraryItemID int64) ([]string, error)
	ResetLibraryItemState(ctx context.Context, libraryItemID int64) error
	ListUnrecoverableLibraryItems(ctx context.Context) ([]int64, error)
	ListMovieTmdbIDs(ctx context.Context) ([]int64, error)
	ListTVShowTmdbIDsWithSeasons(ctx context.Context) ([]database.TVShowSeerrInfo, error)
	ListPublishedDirectNzbSegments(ctx context.Context) ([]database.PublishedDirectNzbSegment, error)
	TouchQueueItemSearched(ctx context.Context, libraryItemID int64) error
}

type SeerrClient interface {
	PendingRequests(ctx context.Context) ([]seerr.Request, error)
	CreateRequest(ctx context.Context, mediaType string, tmdbID int64) error
	CreateTVSeasonRequest(ctx context.Context, tmdbID int64, seasons []int) error
	CreateTVSeasonRequestNoWait(ctx context.Context, tmdbID int64, seasons []int) error
	PartialTVItems(ctx context.Context) ([]seerr.PartialTVItem, error)
}

type HydraClient interface {
	Search(ctx context.Context, request hydra.SearchRequest) ([]hydra.SearchResult, error)
	SearchRecent(ctx context.Context, mediaType string) ([]hydra.SearchResult, error)
}

type IndexerLimits struct {
	MinimumAgeMinutes int
	RetentionDays     int
	MaximumSizeMB     int
}

type Service struct {
	repo             Repository
	seerr            SeerrClient
	hydra            HydraClient
	tmdb             TMDBClient
	tvdb             TVDBClient
	fetcher          NZBFetcher
	postImportHook   func(context.Context, database.QueueSnapshot) error
	preflightChecker func(context.Context, database.QueueSnapshot) error
	// earlyChecker is called with a single message ID immediately after NZB parsing,
	// before archive inspection and DB import. A non-nil error rejects the candidate
	// fast, avoiding expensive segment downloads for expired releases.
	earlyChecker   func(context.Context, string) error
	articleChecker func(ctx context.Context, messageID string) error
	queuePolicy    QueuePolicyProvider
	indexerLimits IndexerLimits
	logger        zerolog.Logger
	// WorkQueue accepts individual library item IDs for immediate dispatch.
	// Push items here from webhooks or sync to bypass the 30-min tick.
	WorkQueue WorkQueuer

	// default profile names per media type (set from config at startup).
	movieProfileName string
	tvProfileName    string

	// profileCacheMu guards the cached quality profiles (keyed by media type).
	profileCacheMu     sync.Mutex
	profileCacheAt     map[string]time.Time
	profileCachedPrefs map[string]ranking.Preferences

	// importSem is kept for ImportNZBFromPush (SABnzbd push path) only.
	// The main download path uses downloader instead.
	importSem chan struct{}

	// downloader is a priority download queue processed by dedicated workers
	// (started in app.go). Replaces the importSem lottery so downloads execute
	// in priority/FIFO order — like SABnzbd's sequential download queue.
	// Priority 0 = fast-lane (HTTP retry, BullMQ workers), 1 = normal.
	downloader *downloadDispatcher

	// dispatchC is a buffered wake signal for the pending-dispatch loop in app.go.
	// Send a value (non-blocking) after creating new library items so the loop
	// re-scans and pushes them to BullMQ immediately instead of waiting for the
	// next scheduled tick.
	dispatchC chan struct{}

	// searchInflight deduplicates concurrent SearchLibrary calls for the same
	// library item ID (O-05). Keyed by int64 library item ID.
	searchInflight     sync.Map
	recentURLMu        sync.Mutex
	recentURLHits      map[string]time.Time
	searchAttemptMu    sync.Mutex
	searchAttempts     map[string]searchAttemptRecord

	// TTL caches for policy data, guarded by profileCacheMu (O-04).
	// These are loaded from the DB at most once per 5 minutes.
	customFormatsCache   []ranking.CustomFormat
	customFormatsCacheAt time.Time
	blockRulesCache      []ranking.BlockRule
	blockRulesCacheAt    time.Time
	indexerPolicyCache   map[string]int
	indexerPolicyCacheAt time.Time
	tierSizeCache        map[string]map[string][2]int
	tierSizeCacheAt      map[string]time.Time
}

type WorkQueueStatus struct {
	Paused bool  `json:"paused"`
	Depth  int64 `json:"depth"`
}

type QueuePolicyProvider interface {
	Settings(ctx context.Context) (policy.Settings, error)
}

type TMDBClient interface {
	Enabled() bool
	MovieDetails(ctx context.Context, tmdbID int64) (tmdb.MovieDetails, error)
	TVDetails(ctx context.Context, tmdbID int64) (tmdb.TVDetails, error)
	TVSeasonNumbers(ctx context.Context, tmdbID int64) ([]int, error)
	TVSeason(ctx context.Context, tmdbID int64, seasonNumber int) (tmdb.TVSeason, error)
}

type TVDBClient interface {
	Enabled() bool
	SeriesDetails(ctx context.Context, tvdbID int64) (tvdb.SeriesDetails, error)
}

type NZBFetcher interface {
	Fetch(ctx context.Context, rawURL string) (string, []byte, error)
}

type SyncResult struct {
	Seen                  int     `json:"seen"`
	Created               int     `json:"created"`
	CreatedLibraryItemIDs []int64 `json:"createdLibraryItemIds,omitempty"`
}

type SearchResult struct {
	LibraryItemID     int64  `json:"libraryItemId"`
	Query             string `json:"query"`
	CandidateCount    int    `json:"candidateCount"`
	SelectedReleaseID *int64 `json:"selectedReleaseId,omitempty"`
}

type BulkSearchResult struct {
	Processed      int     `json:"processed"`
	Searched       int     `json:"searched"`
	Selected       int     `json:"selected"`
	Failed         int     `json:"failed"`
	ProcessedItems []int64 `json:"processedItems,omitempty"`
	FailedItems    []int64 `json:"failedItems,omitempty"`
}

type BulkQueueRetryResult struct {
	Processed       int     `json:"processed"`
	Retried         int     `json:"retried"`
	Failed          int     `json:"failed"`
	ProcessedQueues []int64 `json:"processedQueues,omitempty"`
	FailedQueues    []int64 `json:"failedQueues,omitempty"`
}

type ReleaseActionResult struct {
	ReleaseCandidateID int64  `json:"releaseCandidateId"`
	Action             string `json:"action"`
	SelectedReleaseID  *int64 `json:"selectedReleaseId,omitempty"`
}

type QueueRetryResult struct {
	QueueItemID        int64  `json:"queueItemId"`
	Action             string `json:"action"`
	SelectedReleaseID  *int64 `json:"selectedReleaseId,omitempty"`
	SearchCandidateCnt int    `json:"searchCandidateCount,omitempty"`
}

type QueueManageResult struct {
	QueueItemID        int64  `json:"queueItemId"`
	Action             string `json:"action"`
	SelectedReleaseID  *int64 `json:"selectedReleaseId,omitempty"`
	SearchCandidateCnt int    `json:"searchCandidateCount,omitempty"`
}

const pendingQueueBatchSize = 1000 // process up to 1000 items per scheduler tick

const (
	defaultInlineFallbackDepth  = 3
	busyInlineFallbackDepth     = 1
	fastLaneInlineFallbackDepth = 3
	busyQueueDepthThreshold     = 150
	selectedResumeCooldown      = 5 * time.Minute
	selectedURLCooldown         = 30 * time.Minute
	searchRequestCooldown       = 20 * time.Minute
)

type searchAttemptRecord struct {
	at        time.Time
	outcome   string
	queryText string
}

type completionFastLaneKey struct{}

// asyncDownloadKey marks a context as HTTP-initiated: fetchAndImportSelectedRelease
// submits the job and returns immediately, keeping HTTP handlers responsive regardless
// of queue depth.  Use WithAsyncDownload in router call sites; background workers omit it.
type asyncDownloadKey struct{}

// WithAsyncDownload returns ctx marked for non-blocking download submission.
func WithAsyncDownload(ctx context.Context) context.Context {
	return context.WithValue(ctx, asyncDownloadKey{}, true)
}

func isAsyncDownload(ctx context.Context) bool {
	v, _ := ctx.Value(asyncDownloadKey{}).(bool)
	return v
}

// downloadJob is a unit of work submitted to the download dispatcher.
type downloadJob struct {
	ctx               context.Context
	selectedReleaseID int64
	priority          int // 0 = highest (fast-lane), 1 = normal
	enqueuedAt        time.Time
	resultCh          chan downloadJobResult
}

// downloadJobResult carries the outcome back to the caller.
type downloadJobResult struct {
	selectedReleaseID *int64
	err               error
}

// downloadDispatcher is a priority queue processed by N dedicated worker goroutines.
// Items with lower priority value execute first; within a priority, oldest first.
// This replaces the importSem goroutine lottery — callers submit a job and block
// until the worker completes it, just like SABnzbd's sequential download queue.
// When workerCount==0 (e.g. in unit tests), callers fall back to inline execution.
//
// inFlight deduplicates by selectedReleaseID: if the same release is already
// queued or being processed, a second submit is a no-op. This prevents the
// duplicate-download race when BullMQ jobs complete quickly (due to WithAsyncDownload)
// and the same selected item is re-pushed before the download worker changes its state.
type downloadDispatcher struct {
	mu          sync.Mutex
	queue       []downloadJob
	signal      chan struct{} // non-blocking notify: item added
	workerCount int
	inFlight    map[int64]bool // selectedReleaseID → true while queued or in progress
}

func newDownloadDispatcher() *downloadDispatcher {
	return &downloadDispatcher{
		signal:   make(chan struct{}, 1),
		inFlight: make(map[int64]bool),
	}
}

// submit enqueues a download job. Returns false (no-op) if the same
// selectedReleaseID is already queued or being processed by a worker.
func (d *downloadDispatcher) submit(job downloadJob) bool {
	d.mu.Lock()
	if d.inFlight[job.selectedReleaseID] {
		d.mu.Unlock()
		return false
	}
	d.inFlight[job.selectedReleaseID] = true
	d.queue = append(d.queue, job)
	sort.SliceStable(d.queue, func(i, j int) bool {
		if d.queue[i].priority != d.queue[j].priority {
			return d.queue[i].priority < d.queue[j].priority
		}
		return d.queue[i].enqueuedAt.Before(d.queue[j].enqueuedAt)
	})
	d.mu.Unlock()
	select {
	case d.signal <- struct{}{}:
	default:
	}
	return true
}

// markDone removes a selectedReleaseID from the in-flight set so a future
// submit for the same release is accepted again.
func (d *downloadDispatcher) markDone(selectedReleaseID int64) {
	d.mu.Lock()
	delete(d.inFlight, selectedReleaseID)
	d.mu.Unlock()
}

func (d *downloadDispatcher) next(ctx context.Context) (downloadJob, bool) {
	for {
		d.mu.Lock()
		if len(d.queue) > 0 {
			job := d.queue[0]
			d.queue = d.queue[1:]
			d.mu.Unlock()
			return job, true
		}
		d.mu.Unlock()
		select {
		case <-d.signal:
		case <-ctx.Done():
			return downloadJob{}, false
		}
	}
}

func (d *downloadDispatcher) depth() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.queue)
}

func (d *downloadDispatcher) incWorker() { d.mu.Lock(); d.workerCount++; d.mu.Unlock() }
func (d *downloadDispatcher) decWorker() { d.mu.Lock(); d.workerCount--; d.mu.Unlock() }
func (d *downloadDispatcher) hasWorkers() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.workerCount > 0
}

func NewService(repo Repository, seerr SeerrClient, hydra HydraClient) *Service {
	return &Service{
		repo:               repo,
		seerr:              seerr,
		hydra:              hydra,
		fetcher:            HTTPNZBFetcher{},
		importSem:          make(chan struct{}, 2), // kept for ImportNZBFromPush only
		downloader:         newDownloadDispatcher(),
		dispatchC:          make(chan struct{}, 1),
		recentURLHits:      make(map[string]time.Time),
		searchAttempts:     make(map[string]searchAttemptRecord),
	}
}

// wakeDispatch sends a non-blocking signal to the pending-dispatch loop so it
// re-scans and pushes new items immediately instead of waiting for the next tick.
func (s *Service) wakeDispatch() {
	select {
	case s.dispatchC <- struct{}{}:
	default:
	}
}

// DispatchWakeCh returns the channel that app.go should select on to wake the
// pending-dispatch loop as soon as new library items are created.
func (s *Service) DispatchWakeCh() <-chan struct{} {
	return s.dispatchC
}

func (s *Service) SetIndexerLimits(limits IndexerLimits) {
	s.indexerLimits = limits
}

func (s *Service) SetTMDBClient(client TMDBClient) {
	s.tmdb = client
}

func (s *Service) SetTVDBClient(client TVDBClient) {
	s.tvdb = client
}

func (s *Service) SetPostImportHook(fn func(context.Context, database.QueueSnapshot) error) {
	s.postImportHook = fn
}

func (s *Service) SetPreflightChecker(fn func(context.Context, database.QueueSnapshot) error) {
	s.preflightChecker = fn
}

func (s *Service) SetImportConcurrency(workers int) {
	if s == nil {
		return
	}
	if workers < 1 {
		workers = 1
	}
	s.importSem = make(chan struct{}, workers)
}

func (s *Service) SetEarlyChecker(fn func(context.Context, string) error) {
	s.earlyChecker = fn
}

func (s *Service) SetArticleChecker(fn func(ctx context.Context, messageID string) error) {
	s.articleChecker = fn
}

// ValidatePublishedArticles checks the first NNTP segment of every published
// direct_nzb library item. Items whose segment returns 430 (No Such Article)
// are reset so Drakkar can search for a working replacement. Returns the number
// of items reset.
func (s *Service) ValidatePublishedArticles(ctx context.Context) (int, error) {
	if s.articleChecker == nil {
		return 0, nil
	}
	segments, err := s.repo.ListPublishedDirectNzbSegments(ctx)
	if err != nil {
		return 0, err
	}
	reset := 0
	for _, seg := range segments {
		if ctx.Err() != nil {
			break
		}
		if seg.FirstMsgID == "" {
			continue
		}
		checkErr := s.articleChecker(ctx, seg.FirstMsgID)
		if checkErr == nil {
			continue
		}
		msg := strings.ToLower(checkErr.Error())
		if !strings.Contains(msg, "430") && !strings.Contains(msg, "article not found") && !strings.Contains(msg, "article missing") {
			continue
		}
		s.logger.Warn().Int64("libraryItemId", seg.LibraryItemID).Str("msgID", seg.FirstMsgID).Err(checkErr).Msg("article health check: first segment unavailable — blocklisting release")
		reason := "article health check: " + checkErr.Error()
		if seg.SelectedReleaseID > 0 {
			if blockErr := s.FailAndBlocklistRelease(ctx, seg.SelectedReleaseID, reason); blockErr != nil {
				s.logger.Warn().Int64("libraryItemId", seg.LibraryItemID).Err(blockErr).Msg("article health check: blocklist failed")
				continue
			}
		} else {
			if resetErr := s.ResetLibraryItem(ctx, seg.LibraryItemID); resetErr != nil {
				s.logger.Warn().Int64("libraryItemId", seg.LibraryItemID).Err(resetErr).Msg("article health check: reset failed")
				continue
			}
		}
		reset++
	}
	return reset, nil
}

func (s *Service) SetQueuePolicyProvider(provider QueuePolicyProvider) {
	s.queuePolicy = provider
}

func (s *Service) SetLogger(l zerolog.Logger) {
	s.logger = l
}

func (s *Service) ListRequests(ctx context.Context) ([]database.MediaRequestSummary, error) {
	return s.repo.ListMediaRequests(ctx)
}

func (s *Service) SyncRequests(ctx context.Context) (SyncResult, error) {
	if s == nil || s.seerr == nil {
		return SyncResult{}, fmt.Errorf("seerr client unavailable")
	}
	requests, err := s.seerr.PendingRequests(ctx)
	if err != nil {
		return SyncResult{}, err
	}
	result := SyncResult{Seen: len(requests)}
	for _, request := range requests {
		switch strings.ToLower(request.Type) {
		case "movie":
			libraryItemID, created, err := s.repo.UpsertMovieRequest(ctx, fmt.Sprintf("%d", request.ID), request.TMDBID, request.MediaTitle, request.MediaYear)
			if err != nil {
				return result, err
			}
			// Enrich in background so TMDB calls don't block the sync loop.
			// New items get queued for search immediately; metadata arrives shortly after.
			if created {
				result.Created++
				result.CreatedLibraryItemIDs = append(result.CreatedLibraryItemIDs, libraryItemID)
			}
			lid, tmdbID := libraryItemID, request.TMDBID
			go func() {
				enrichCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = s.enrichMovieRequest(enrichCtx, lid, tmdbID)
			}()
		case "tv":
			if len(request.Seasons) > 0 {
				// Season-level request: expand into per-episode library items via TMDB.
				created, ids, err := s.syncSeasonRequest(ctx, request)
				if err != nil {
					s.logger.Warn().Err(err).Int64("seerrID", request.ID).Msg("seerr sync: season request expand failed")
					continue
				}
				result.Created += created
				result.CreatedLibraryItemIDs = append(result.CreatedLibraryItemIDs, ids...)
				continue
			}
			libraryItemID, created, err := s.repo.UpsertEpisodeRequest(
				ctx,
				fmt.Sprintf("%d", request.ID),
				request.TVDBID,
				request.TMDBID,
				request.MediaTitle,
				request.MediaYear,
				request.SeasonNumber,
				request.EpisodeNumber,
				request.EpisodeTitle,
			)
			if err != nil {
				return result, err
			}
			if created {
				result.Created++
				result.CreatedLibraryItemIDs = append(result.CreatedLibraryItemIDs, libraryItemID)
			}
			lid, tmdbID, tvdbID, epTitle := libraryItemID, request.TMDBID, request.TVDBID, request.EpisodeTitle
			go func() {
				enrichCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = s.enrichEpisodeRequest(enrichCtx, lid, tmdbID, tvdbID, epTitle)
			}()
		}
	}
	if result.Created > 0 {
		s.wakeDispatch()
	}
	return result, nil
}

// syncSeasonRequest expands a Seerr season-level TV request into per-episode library items.
// It fetches episode data from TMDB for each requested season and calls UpsertEpisodeRequest
// for each episode, which is idempotent so re-runs are safe.
func (s *Service) syncSeasonRequest(ctx context.Context, req seerr.Request) (int, []int64, error) {
	if s.tmdb == nil {
		return 0, nil, fmt.Errorf("tmdb client unavailable")
	}
	details, err := s.tmdb.TVDetails(ctx, req.TMDBID)
	if err != nil {
		return 0, nil, fmt.Errorf("tmdb lookup failed: %w", err)
	}
	title := strings.TrimSpace(details.Name)
	if title == "" {
		title = req.MediaTitle
	}
	year := req.MediaYear
	if year == 0 && len(details.FirstAirDate) >= 4 {
		if y, err2 := strconv.Atoi(details.FirstAirDate[:4]); err2 == nil {
			year = y
		}
	}
	var created int
	var ids []int64
	for _, seasonNum := range req.Seasons {
		season, err := s.tmdb.TVSeason(ctx, req.TMDBID, seasonNum)
		if err != nil {
			s.logger.Warn().Err(err).Int64("tmdbID", req.TMDBID).Int("season", seasonNum).Msg("seerr sync: tmdb season fetch failed")
			continue
		}
		for _, ep := range season.Episodes {
			if ep.EpisodeNumber <= 0 {
				continue
			}
			externalID := fmt.Sprintf("%d-s%de%d", req.ID, seasonNum, ep.EpisodeNumber)
			lid, wasCreated, upsertErr := s.repo.UpsertEpisodeRequest(
				ctx, externalID, req.TVDBID, req.TMDBID, title, year,
				seasonNum, ep.EpisodeNumber, ep.Name,
			)
			if upsertErr != nil {
				s.logger.Warn().Err(upsertErr).Str("externalID", externalID).Msg("seerr sync: episode upsert failed")
				continue
			}
			if wasCreated {
				created++
				ids = append(ids, lid)
			}
		}
	}
	if created > 0 {
		tmdbID, tvdbID := req.TMDBID, req.TVDBID
		go func() {
			enrichCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = s.enrichEpisodeRequest(enrichCtx, ids[0], tmdbID, tvdbID, "")
		}()
	}
	return created, ids, nil
}

func (s *Service) CreateSeerrRequest(ctx context.Context, mediaType string, tmdbID int64) (SyncResult, error) {
	if s == nil || s.seerr == nil {
		return SyncResult{}, fmt.Errorf("seerr client unavailable")
	}
	if err := s.seerr.CreateRequest(ctx, mediaType, tmdbID); err != nil {
		return SyncResult{}, err
	}
	return s.syncRequestsWithRetry(ctx)
}

func (s *Service) CreateSeerrSeasonRequest(ctx context.Context, tmdbID int64, seasons []int) (SyncResult, error) {
	if s == nil || s.seerr == nil {
		return SyncResult{}, fmt.Errorf("seerr client unavailable")
	}
	if len(seasons) == 0 {
		return SyncResult{}, fmt.Errorf("at least one season is required")
	}
	if err := s.seerr.CreateTVSeasonRequest(ctx, tmdbID, seasons); err != nil {
		return SyncResult{}, err
	}
	return s.syncRequestsWithRetry(ctx)
}

type PushMissingToSeerrResult struct {
	MoviesPushed  int `json:"moviesPushed"`
	ShowsPushed   int `json:"showsPushed"`
	MoviesSkipped int `json:"moviesSkipped"`
	ShowsSkipped  int `json:"showsSkipped"`
}

// PushMissingLibraryItemsToSeerr pushes any movie or TV show that is in Drakkar's
// library but absent from Seerr's current request list.
func (s *Service) PushMissingLibraryItemsToSeerr(ctx context.Context) (PushMissingToSeerrResult, error) {
	if s == nil || s.seerr == nil {
		return PushMissingToSeerrResult{}, fmt.Errorf("seerr client unavailable")
	}

	// Fetch what Seerr currently has.
	existing, err := s.seerr.PendingRequests(ctx)
	if err != nil {
		return PushMissingToSeerrResult{}, fmt.Errorf("fetch seerr requests: %w", err)
	}
	seerrMovies := make(map[int64]struct{}, len(existing))
	seerrTV := make(map[int64]struct{}, len(existing))
	for _, r := range existing {
		switch strings.ToLower(r.Type) {
		case "movie":
			seerrMovies[r.TMDBID] = struct{}{}
		case "tv":
			seerrTV[r.TMDBID] = struct{}{}
		}
	}

	var result PushMissingToSeerrResult

	// Push missing movies.
	movieIDs, err := s.repo.ListMovieTmdbIDs(ctx)
	if err != nil {
		return result, fmt.Errorf("list movie tmdb ids: %w", err)
	}
	for _, tmdbID := range movieIDs {
		if _, ok := seerrMovies[tmdbID]; ok {
			result.MoviesSkipped++
			continue
		}
		if err := s.seerr.CreateRequest(ctx, "movie", tmdbID); err != nil {
			s.logger.Warn().Err(err).Int64("tmdbID", tmdbID).Msg("push to seerr: movie create failed")
			result.MoviesSkipped++
			continue
		}
		result.MoviesPushed++
	}

	// Push missing TV shows.
	shows, err := s.repo.ListTVShowTmdbIDsWithSeasons(ctx)
	if err != nil {
		return result, fmt.Errorf("list tv show tmdb ids: %w", err)
	}
	for _, show := range shows {
		if _, ok := seerrTV[show.TMDBID]; ok {
			result.ShowsSkipped++
			continue
		}
		if err := s.seerr.CreateTVSeasonRequest(ctx, show.TMDBID, show.Seasons); err != nil {
			s.logger.Warn().Err(err).Int64("tmdbID", show.TMDBID).Msg("push to seerr: tv create failed")
			result.ShowsSkipped++
			continue
		}
		result.ShowsPushed++
	}

	return result, nil
}

// SyncPlexDetectedResult is returned by SyncPlexDetectedShows.
type SyncPlexDetectedResult struct {
	Found     int `json:"found"`
	Requested int `json:"requested"`
	Skipped   int `json:"skipped"`
}

// SyncPlexDetectedShows creates Seerr requests for TV shows that Seerr has
// detected via a Plex scan (status=PARTIAL) but that have no download request
// yet. Only seasons not already fully available in Plex are requested.
// After posting all requests it runs SyncRequests to immediately import them.
func (s *Service) SyncPlexDetectedShows(ctx context.Context) (SyncPlexDetectedResult, error) {
	if s == nil || s.seerr == nil {
		return SyncPlexDetectedResult{}, fmt.Errorf("seerr client unavailable")
	}

	partialItems, err := s.seerr.PartialTVItems(ctx)
	if err != nil {
		return SyncPlexDetectedResult{}, fmt.Errorf("fetch partial tv items: %w", err)
	}

	existing, err := s.seerr.PendingRequests(ctx)
	if err != nil {
		return SyncPlexDetectedResult{}, fmt.Errorf("fetch seerr requests: %w", err)
	}
	requestedTMDB := make(map[int64]struct{}, len(existing))
	for _, r := range existing {
		if strings.EqualFold(r.Type, "tv") {
			requestedTMDB[r.TMDBID] = struct{}{}
		}
	}

	result := SyncPlexDetectedResult{Found: len(partialItems)}
	for _, item := range partialItems {
		if _, exists := requestedTMDB[item.TMDBID]; exists {
			result.Skipped++
			continue
		}
		if err := s.seerr.CreateTVSeasonRequestNoWait(ctx, item.TMDBID, item.PartialSeasons); err != nil {
			s.logger.Warn().Err(err).Int64("tmdbID", item.TMDBID).Msg("sync plex detected: season request failed")
			result.Skipped++
			continue
		}
		result.Requested++
	}

	// Import the newly created requests immediately.
	if result.Requested > 0 {
		if _, syncErr := s.SyncRequests(ctx); syncErr != nil {
			s.logger.Warn().Err(syncErr).Msg("sync plex detected: SyncRequests after request creation failed")
		}
	}

	return result, nil
}

func (s *Service) enrichMovieRequest(ctx context.Context, libraryItemID, tmdbID int64) error {
	if s == nil || s.tmdb == nil || !s.tmdb.Enabled() || tmdbID <= 0 {
		return nil
	}
	item, err := s.tmdb.MovieDetails(ctx, tmdbID)
	if err != nil {
		return nil
	}
	return s.repo.EnrichMovieFull(ctx, libraryItemID, database.MovieEnrichment{
		TMDBID:              tmdbID,
		Title:               item.Title,
		OriginalTitle:       item.OriginalTitle,
		Year:                item.Year,
		ReleaseDate:         item.ReleaseDate,
		IMDbID:              item.IMDbID,
		Overview:            item.Overview,
		Tagline:             item.Tagline,
		Status:              item.Status,
		ContentRating:       item.ContentRating,
		OriginalLanguage:    item.OriginalLanguage,
		RuntimeMinutes:      item.RuntimeMinutes,
		PosterURL:           item.PosterURL,
		BackdropURL:         item.BackdropURL,
		TrailerURL:          item.TrailerURL,
		Genres:              item.Genres,
		AlternativeTitles:   item.AlternativeTitles,
		ProductionCompanies: item.ProductionCompanies,
		Popularity:          item.Popularity,
		VoteAverage:         item.VoteAverage,
		VoteCount:           item.VoteCount,
		Budget:              item.Budget,
		Revenue:             item.Revenue,
	})
}

func (s *Service) enrichEpisodeRequest(ctx context.Context, libraryItemID, tmdbID, tvdbID int64, episodeTitle string) error {
	if s != nil && s.tmdb != nil && s.tmdb.Enabled() && tmdbID > 0 {
		item, err := s.tmdb.TVDetails(ctx, tmdbID)
		if err == nil {
			return s.repo.EnrichTVFull(ctx, libraryItemID, episodeTitle, database.TVShowEnrichment{
				TMDBID:              tmdbID,
				ShowTitle:           item.Name,
				OriginalName:        item.OriginalName,
				Year:                item.Year,
				FirstAirDate:        item.FirstAirDate,
				LastAirDate:         item.LastAirDate,
				IMDbID:              item.IMDbID,
				Overview:            item.Overview,
				Tagline:             item.Tagline,
				Status:              item.Status,
				ContentRating:       item.ContentRating,
				OriginalLanguage:    item.OriginalLanguage,
				Network:             item.Network,
				EpisodeRunTime:      item.EpisodeRunTime,
				NumberOfSeasons:     item.NumberOfSeasons,
				NumberOfEpisodes:    item.NumberOfEpisodes,
				InProduction:        item.InProduction,
				PosterURL:           item.PosterURL,
				BackdropURL:         item.BackdropURL,
				TrailerURL:          item.TrailerURL,
				Genres:              item.Genres,
				AlternativeTitles:   item.AlternativeTitles,
				ProductionCompanies: item.ProductionCompanies,
				Popularity:          item.Popularity,
				VoteAverage:         item.VoteAverage,
				VoteCount:           item.VoteCount,
			})
		}
	}
	if s == nil || s.tvdb == nil || !s.tvdb.Enabled() || tvdbID <= 0 {
		return nil
	}
	item, err := s.tvdb.SeriesDetails(ctx, tvdbID)
	if err != nil {
		return nil
	}
	return s.repo.EnrichEpisodeMetadata(ctx, libraryItemID, tmdbID, item.Name, item.Year, item.IMDbID, episodeTitle)
}

// PushPendingToQueue fetches all pending library items and pushes them to the
// work queue. Items that already have a selected release always get priority 0
// (fast-lane) regardless of the caller-supplied priority; items needing a new
// search get the caller-supplied priority.
// Called from the webhook handler so newly approved requests are processed
// immediately with high priority.
func (s *Service) PushPendingToQueue(priority int) {
	if s.WorkQueue == nil {
		return
	}
	ctx := context.Background()
	targets, err := s.repo.ListPendingLibrarySearchTargets(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("PushPendingToQueue: ListPendingLibrarySearchTargets failed")
		return
	}
	for _, target := range s.filterPendingSearchTargets(targets, time.Now()) {
		p := priority
		if target.Selected {
			p = 0 // already-selected items always jump ahead of new searches
		}
		s.WorkQueue.Push(ctx, target.LibraryItemID, p)
	}
}

func (s *Service) PushLibraryItemsToQueue(ids []int64, priority int) {
	if s == nil || s.WorkQueue == nil {
		return
	}
	ctx := context.Background()
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		s.WorkQueue.Push(ctx, id, priority)
	}
}

func (s *Service) SearchPendingBatch(ctx context.Context, limit int) (BulkSearchResult, error) {
	targets, err := s.repo.ListPendingLibrarySearchTargets(ctx)
	if err != nil {
		return BulkSearchResult{}, err
	}
	if limit > 0 && len(targets) > limit {
		targets = targets[:limit]
	}
	result := BulkSearchResult{Processed: len(targets)}
	for _, target := range targets {
		result.ProcessedItems = append(result.ProcessedItems, target.LibraryItemID)
		search, err := s.SearchLibrary(ctx, target.LibraryItemID)
		if err != nil {
			result.Failed++
			result.FailedItems = append(result.FailedItems, target.LibraryItemID)
			continue
		}
		result.Searched++
		if search.SelectedReleaseID != nil {
			result.Selected++
		}
	}
	return result, nil
}

func (s *Service) SearchPendingLibrary(ctx context.Context) (BulkSearchResult, error) {
	targets, err := s.repo.ListPendingLibrarySearchTargets(ctx)
	if err != nil {
		return BulkSearchResult{}, err
	}
	targets = s.filterPendingSearchTargets(targets, time.Now())
	result := BulkSearchResult{Processed: len(targets)}
	for _, target := range targets {
		result.ProcessedItems = append(result.ProcessedItems, target.LibraryItemID)
		// If the work queue is running, push items for concurrent processing
		// rather than searching sequentially.
		// Resume items (selected release already chosen) bypass BullMQ entirely:
		// submitting via BullMQ adds 30-min lock latency for a <1ms async dispatch.
		// Direct submission to the downloadDispatcher is idempotent — submit()
		// returns false if the release is already in-flight, so this is safe to
		// call every dispatch cycle.
		// Items needing a Hydra search get priority=10 (BullMQ lower-priority).
		if s.WorkQueue != nil {
			if target.SelectedReleaseID > 0 {
				// Cooldown only for requested+selected resume items. Newly promoted
				// selected candidates need to advance quickly under backlog; the
				// downloadDispatcher already deduplicates true in-flight duplicates.
				now := time.Now()
				if !s.shouldDispatchSelectedTarget(target, now) {
					continue
				}
				resultCh := make(chan downloadJobResult, 1)
				if s.downloader.submit(downloadJob{
					ctx:               context.Background(),
					selectedReleaseID: target.SelectedReleaseID,
					priority:          0,
					enqueuedAt:        time.Now(),
					resultCh:          resultCh,
				}) {
					s.markSelectedReleaseURLDispatched(target.ExternalURL, now)
					go func() { <-resultCh }()
				}
				result.Searched++
				continue
			}
			s.WorkQueue.Push(ctx, target.LibraryItemID, 10)
			result.Searched++
			continue
		}
		search, err := s.SearchLibrary(ctx, target.LibraryItemID)
		if err != nil {
			result.Failed++
			result.FailedItems = append(result.FailedItems, target.LibraryItemID)
			continue
		}
		result.Searched++
		if search.SelectedReleaseID != nil {
			result.Selected++
		}
	}
	return result, nil
}

func (s *Service) DispatchAutomaticPending(ctx context.Context) (BulkSearchResult, error) {
	targets, err := s.repo.ListPendingLibrarySearchTargets(ctx)
	if err != nil {
		return BulkSearchResult{}, err
	}
	filtered := make([]database.PendingLibrarySearchTarget, 0, len(targets))
	for _, target := range targets {
		if target.SelectedReleaseID > 0 || target.Selected {
			filtered = append(filtered, target)
		}
	}
	result := BulkSearchResult{Processed: len(filtered)}
	for _, target := range filtered {
		result.ProcessedItems = append(result.ProcessedItems, target.LibraryItemID)
		now := time.Now()
		if !s.shouldDispatchSelectedTarget(target, now) {
			continue
		}
		resultCh := make(chan downloadJobResult, 1)
		if s.downloader.submit(downloadJob{
			ctx:               context.Background(),
			selectedReleaseID: target.SelectedReleaseID,
			priority:          0,
			enqueuedAt:        now,
			resultCh:          resultCh,
		}) {
			s.markSelectedReleaseURLDispatched(target.ExternalURL, now)
			go func() { <-resultCh }()
			result.Searched++
		}
	}
	return result, nil
}

func (s *Service) filterPendingSearchTargets(targets []database.PendingLibrarySearchTarget, now time.Time) []database.PendingLibrarySearchTarget {
	if len(targets) == 0 {
		return nil
	}
	selectedTargets := make([]database.PendingLibrarySearchTarget, 0, len(targets))
	searchTargets := make([]database.PendingLibrarySearchTarget, 0, len(targets))
	// Sonarr/Radarr group missing episodes by show+season: one search per season,
	// not one per episode. The season pack attempt inside the BullMQ worker covers
	// all missing episodes from that season in a single Hydra2 query.
	type showSeason struct{ tvShowID int64; season int }
	seenShowSeasons := make(map[showSeason]bool)
	for _, target := range targets {
		if target.SelectedReleaseID > 0 || target.Selected {
			selectedTargets = append(selectedTargets, target)
			continue
		}
		if strings.EqualFold(target.MediaType, "episode") && target.TVShowID > 0 {
			key := showSeason{target.TVShowID, target.SeasonNumber}
			if seenShowSeasons[key] {
				continue
			}
			seenShowSeasons[key] = true
		}
		searchTargets = append(searchTargets, target)
		if len(searchTargets) >= pendingQueueBatchSize {
			break
		}
	}
	return append(selectedTargets, searchTargets...)
}


func (s *Service) shouldDispatchSelectedTarget(target database.PendingLibrarySearchTarget, now time.Time) bool {
	if target.SelectedReleaseID <= 0 {
		return false
	}
	if target.State != database.QueueRequested {
		return true
	}
	rawURL := strings.TrimSpace(target.ExternalURL)
	if rawURL != "" && s != nil {
		s.recentURLMu.Lock()
		lastAt, ok := s.recentURLHits[rawURL]
		if ok && now.Sub(lastAt) < selectedURLCooldown {
			s.recentURLMu.Unlock()
			return false
		}
		// Drop stale URL entries opportunistically to keep the map bounded.
		for url, hitAt := range s.recentURLHits {
			if now.Sub(hitAt) >= selectedURLCooldown {
				delete(s.recentURLHits, url)
			}
		}
		s.recentURLMu.Unlock()
	}
	return now.Sub(target.UpdatedAt) >= selectedResumeCooldown
}

func (s *Service) markSelectedReleaseURLDispatched(rawURL string, now time.Time) {
	if s == nil {
		return
	}
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return
	}
	s.recentURLMu.Lock()
	s.recentURLHits[rawURL] = now
	s.recentURLMu.Unlock()
}

func searchRequestFingerprint(libraryItemID int64, request hydra.SearchRequest) string {
	query := normalizeSearchText(request.Query)
	return fmt.Sprintf("%d|%s|%s|%d|%s|%d|%d", libraryItemID,
		strings.ToLower(strings.TrimSpace(request.MediaType)),
		query,
		request.TMDBID,
		strings.ToLower(strings.TrimSpace(request.IMDbID)),
		request.TVDBID,
		request.SeasonNumber*1000+request.EpisodeNumber,
	)
}

func (s *Service) shouldSkipSearchRequest(libraryItemID int64, request hydra.SearchRequest, now time.Time) bool {
	if s == nil {
		return false
	}
	key := searchRequestFingerprint(libraryItemID, request)
	s.searchAttemptMu.Lock()
	defer s.searchAttemptMu.Unlock()
	for attemptKey, record := range s.searchAttempts {
		if now.Sub(record.at) >= searchRequestCooldown {
			delete(s.searchAttempts, attemptKey)
		}
	}
	record, ok := s.searchAttempts[key]
	if !ok {
		return false
	}
	switch record.outcome {
	case "selected", "usable", "rejected_only", "empty":
		return now.Sub(record.at) < searchRequestCooldown
	default:
		return false
	}
}

func (s *Service) rememberSearchRequest(libraryItemID int64, request hydra.SearchRequest, outcome string, now time.Time) {
	if s == nil {
		return
	}
	key := searchRequestFingerprint(libraryItemID, request)
	s.searchAttemptMu.Lock()
	s.searchAttempts[key] = searchAttemptRecord{
		at:        now,
		outcome:   outcome,
		queryText: searchRequestLabel(request),
	}
	s.searchAttemptMu.Unlock()
}

func searchCandidateOutcome(candidates []database.SearchCandidateRecord, selectedReleaseID *int64) string {
	if selectedReleaseID != nil {
		return "selected"
	}
	if len(candidates) == 0 {
		return "empty"
	}
	if hasNonRejectedCandidate(candidates) {
		return "usable"
	}
	return "rejected_only"
}

func withCompletionFastLane(ctx context.Context) context.Context {
	return context.WithValue(ctx, completionFastLaneKey{}, true)
}

func isCompletionFastLane(ctx context.Context) bool {
	value, _ := ctx.Value(completionFastLaneKey{}).(bool)
	return value
}

// ProcessLibraryItem handles a queue-dispatched library item.
// If the item already has a selected release, submit to the download queue
// asynchronously and return — blocking the BullMQ worker on the download
// would starve the other search workers.
// For items needing a search, the search runs synchronously (BullMQ workers
// are search workers) but the resulting download is kicked off asynchronously.
func (s *Service) ProcessLibraryItem(ctx context.Context, libraryItemID int64) error {
	if s == nil {
		return nil
	}
	current, err := s.repo.GetLatestSelectedReleaseSummaryByLibraryItem(ctx, libraryItemID)
	if err != nil {
		return err
	}
	if current != nil && current.SelectedReleaseID != 0 {
		_, err := s.fetchAndImportSelectedRelease(WithAsyncDownload(ctx), current.SelectedReleaseID)
		return err
	}
	_, err = s.SearchLibrary(WithAsyncDownload(ctx), libraryItemID)
	// Record search time regardless of outcome so the backlog scheduler skips
	// this item until the per-item cooldown expires (matches Sonarr's LastSearchTime).
	_ = s.repo.TouchQueueItemSearched(ctx, libraryItemID)
	return err
}

func (s *Service) WorkQueueStatus(ctx context.Context) (WorkQueueStatus, error) {
	if s == nil || s.WorkQueue == nil {
		return WorkQueueStatus{}, nil
	}
	paused, err := s.WorkQueue.IsPaused(ctx)
	if err != nil {
		return WorkQueueStatus{}, err
	}
	return WorkQueueStatus{
		Paused: paused,
		Depth:  s.WorkQueue.Depth(ctx),
	}, nil
}

func (s *Service) PauseWorkQueue(ctx context.Context) (WorkQueueStatus, error) {
	if s == nil || s.WorkQueue == nil {
		return WorkQueueStatus{}, errors.New("work queue unavailable")
	}
	if err := s.WorkQueue.Pause(ctx); err != nil {
		return WorkQueueStatus{}, err
	}
	return s.WorkQueueStatus(ctx)
}

func (s *Service) ResumeWorkQueue(ctx context.Context) (WorkQueueStatus, error) {
	if s == nil || s.WorkQueue == nil {
		return WorkQueueStatus{}, errors.New("work queue unavailable")
	}
	if err := s.WorkQueue.Resume(ctx); err != nil {
		return WorkQueueStatus{}, err
	}
	return s.WorkQueueStatus(ctx)
}

func (s *Service) SearchRecentPending(ctx context.Context, mediaType string) (BulkSearchResult, error) {
	if s == nil || s.hydra == nil {
		return BulkSearchResult{}, fmt.Errorf("nzbhydra2 client unavailable")
	}
	recent, err := s.hydra.SearchRecent(ctx, mediaType)
	if err != nil {
		return BulkSearchResult{}, err
	}
	targets, err := s.repo.ListPendingLibrarySearchTargets(ctx)
	if err != nil {
		return BulkSearchResult{}, err
	}
	result := BulkSearchResult{}
	for _, target := range targets {
		input, err := s.repo.GetLibrarySearchInput(ctx, target.LibraryItemID)
		if err != nil {
			continue
		}
		if reason, err := s.repo.DetectMovieSearchConflict(ctx, target.LibraryItemID); err == nil && strings.TrimSpace(reason) != "" {
			_ = s.repo.MarkLibrarySearchFailed(ctx, target.LibraryItemID, reason)
			result.Failed++
			result.FailedItems = append(result.FailedItems, target.LibraryItemID)
			continue
		} else if err != nil {
			result.Failed++
			result.FailedItems = append(result.FailedItems, target.LibraryItemID)
			continue
		}
		if !matchesRecentMediaType(input, mediaType) {
			continue
		}
		result.Processed++
		result.ProcessedItems = append(result.ProcessedItems, target.LibraryItemID)
		history, err := s.repo.LookupCandidateHistory(ctx, target.LibraryItemID)
		if err != nil {
			result.Failed++
			result.FailedItems = append(result.FailedItems, target.LibraryItemID)
			continue
		}
		profilePrefs := s.profilePreferencesForItem(ctx, target.LibraryItemID, input.MediaType)
		candidates := buildSearchCandidates(recent, searchRequirements(input), history, profilePrefs, s.indexerLimits, s.loadIndexerPolicyMap(ctx))
		// Only store candidates when at least one matches this show's title.
		// If the recent feed has no match (all wrong_title), skip rather than
		// replacing existing valid candidates with a batch of rejections.
		if !hasNonRejectedCandidate(candidates) {
			continue
		}
		selectedReleaseID, err := s.repo.ReplaceSearchCandidates(ctx, target.LibraryItemID, candidates)
		if err != nil {
			result.Failed++
			result.FailedItems = append(result.FailedItems, target.LibraryItemID)
			continue
		}
		result.Searched++
		if selectedReleaseID == nil {
			continue
		}
		finalSelected, err := s.fetchAndImportSelectedRelease(ctx, *selectedReleaseID)
		if err != nil {
			result.Failed++
			result.FailedItems = append(result.FailedItems, target.LibraryItemID)
			continue
		}
		if finalSelected != nil {
			result.Selected++
		}
	}
	return result, nil
}

// ClearFailedQueue resets all failed queue items back to 'requested' state,
// removing them from the history view and re-queuing them for the next search pass.
func (s *Service) ClearFailedQueue(ctx context.Context) (int, error) {
	return s.repo.ClearFailedQueueItems(ctx)
}

func (s *Service) RetryFailedQueue(ctx context.Context) (BulkQueueRetryResult, error) {
	// Fetch up to 500 items: restart-interrupted items (stale_worker, interrupted_by_restart)
	// don't call Hydra and are much faster to process. Hydra calls are capped at 100
	// per run so we don't flood the indexer while still clearing restart backlogs quickly.
	targets, err := s.repo.ListFailedQueueRetryTargets(ctx, 500)
	if err != nil {
		return BulkQueueRetryResult{}, err
	}

	// Load user-configured policy (same source as AutoManageFailedQueue).
	settings := policy.DefaultSettings()
	if s.queuePolicy != nil {
		if loaded, loadErr := s.queuePolicy.Settings(ctx); loadErr == nil {
			settings = loaded
		}
	}
	ttl := settings.BlocklistTTLDays

	const maxHydraCalls = 100
	hydraCallCount := 0

	result := BulkQueueRetryResult{Processed: len(targets)}
	for _, target := range targets {
		result.ProcessedQueues = append(result.ProcessedQueues, target.QueueItemID)

		// User-configured policy takes precedence over the hardcoded matrix,
		// EXCEPT for items that already have a valid selected release: those
		// should be retried via RetryQueueItem (NZB re-fetch) rather than
		// discarding the release and doing a fresh NZBHydra2 search.
		userAction := policy.ActionForReason(settings, target.FailureReason)
		if userAction != policy.QueueActionDoNothing &&
			!(target.HasSelectedRelease && target.CandidateFailureCount == 0) {
			switch userAction {
			case policy.QueueActionRemoveBlocklistAndSearch, policy.QueueActionRemoveAndBlocklist:
				if hydraCallCount >= maxHydraCalls && userAction == policy.QueueActionRemoveBlocklistAndSearch {
					continue // skip Hydra-dependent items when cap is reached
				}
				if err := s.repo.BlocklistQueueSelectedRelease(ctx, target.QueueItemID, target.FailureReason, ttl); err != nil {
					s.logger.Warn().Err(err).Int64("queueItemId", target.QueueItemID).Msg("retry: blocklist failed")
				}
				if err := s.repo.ClearQueueSelectedRelease(ctx, target.QueueItemID); err != nil {
					result.Failed++
					result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
					continue
				}
				if userAction == policy.QueueActionRemoveBlocklistAndSearch {
					hydraCallCount++
					if _, err := s.SearchLibrary(ctx, target.LibraryItemID); err == nil {
						result.Retried++
					} else {
						result.Failed++
						result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
					}
				}
			case policy.QueueActionSearchAgain:
				if hydraCallCount >= maxHydraCalls {
					continue
				}
				hydraCallCount++
				if _, err := s.SearchLibrary(ctx, target.LibraryItemID); err == nil {
					result.Retried++
				} else {
					result.Failed++
					result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
				}
			case policy.QueueActionRemove:
				_ = s.repo.ClearQueueSelectedRelease(ctx, target.QueueItemID)
			}
			continue
		}

		// Fall back to the hardcoded recovery matrix for failure reasons
		// that have no user-configured policy entry.
		action := policy.DecideFromReason(target.FailureReason)
		// Escalate ActionRetryLater to blocklist+search after 3 failures on the
		// same candidate, preventing an infinite throttle-retry loop.
		if action == policy.ActionRetryLater && target.CandidateFailureCount >= 3 {
			action = policy.ActionBlocklistAndSearch
		}
		switch action {
		case policy.ActionBlocklistAndSearch:
			if hydraCallCount >= maxHydraCalls {
				continue
			}
			hydraCallCount++
			if err := s.repo.BlocklistQueueSelectedRelease(ctx, target.QueueItemID, target.FailureReason, ttl); err != nil {
				s.logger.Warn().Err(err).Int64("queueItemId", target.QueueItemID).Msg("bulk retry: blocklist failed")
			}
			if sr, err := s.SearchLibrary(ctx, target.LibraryItemID); err == nil && sr.SelectedReleaseID != nil {
				result.Retried++
			} else {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			}
			continue
		case policy.ActionDoNothing:
			continue
		default:
			// ActionSearchAgain, ActionRetryLater (under cap) → standard retry flow.
		}

		isRestartInterruption := strings.Contains(strings.ToLower(target.FailureReason), "interrupted_by_restart") ||
			strings.Contains(strings.ToLower(target.FailureReason), "stale_worker")
		if isRestartInterruption && target.HasSelectedRelease && target.CandidateFailureCount == 0 {
			if err := s.repo.RequeueSelectedRelease(ctx, target.QueueItemID); err != nil {
				s.logger.Warn().Err(err).Int64("queueItemId", target.QueueItemID).Msg("retry: RequeueSelectedRelease failed")
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
				continue
			}
			result.Retried++
			continue
		}

		if _, err := s.RetryQueueItem(ctx, target.QueueItemID); err != nil {
			s.logger.Warn().Err(err).Int64("queueItemId", target.QueueItemID).Msg("retry: RetryQueueItem failed")
			result.Failed++
			result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			continue
		}
		result.Retried++
	}
	return result, nil
}

// isDeadlock returns true when err is a PostgreSQL deadlock (SQLSTATE 40P01).
func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.SQLState() == "40P01"
}

// isFKViolation returns true when err is a PostgreSQL foreign-key constraint violation (SQLSTATE 23503).
func isFKViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.SQLState() == "23503"
}

func (s *Service) SearchLibrary(ctx context.Context, libraryItemID int64) (SearchResult, error) {
	// O-05: skip if another goroutine is already searching this item.
	if _, loaded := s.searchInflight.LoadOrStore(libraryItemID, struct{}{}); loaded {
		return SearchResult{LibraryItemID: libraryItemID}, nil
	}
	defer s.searchInflight.Delete(libraryItemID)

	const maxDeadlockRetries = 3
	for attempt := 0; attempt < maxDeadlockRetries; attempt++ {
		result, err := s.searchLibraryOnceWithMode(ctx, libraryItemID, false)
		if isDeadlock(err) {
			time.Sleep(time.Duration(50+attempt*50) * time.Millisecond)
			continue
		}
		return result, err
	}
	return SearchResult{}, fmt.Errorf("searchLibrary: too many deadlock retries for item %d", libraryItemID)
}

func (s *Service) searchLibraryOnce(ctx context.Context, libraryItemID int64) (SearchResult, error) {
	return s.searchLibraryOnceWithMode(ctx, libraryItemID, false)
}

func (s *Service) searchLibraryOnceWithMode(ctx context.Context, libraryItemID int64, upgradeSearch bool) (SearchResult, error) {
	if s == nil || s.hydra == nil {
		return SearchResult{}, fmt.Errorf("nzbhydra2 client unavailable")
	}
	if reason, err := s.repo.DetectMovieSearchConflict(ctx, libraryItemID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SearchResult{LibraryItemID: libraryItemID}, nil // item deleted or missing, skip
		}
		return SearchResult{}, err
	} else if strings.TrimSpace(reason) != "" {
		if markErr := s.repo.MarkLibrarySearchFailed(ctx, libraryItemID, reason); markErr != nil {
			return SearchResult{}, markErr
		}
		return SearchResult{LibraryItemID: libraryItemID}, nil
	}
	input, err := s.repo.GetLibrarySearchInput(ctx, libraryItemID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SearchResult{LibraryItemID: libraryItemID}, nil // item deleted between queue push and processing
		}
		return SearchResult{}, err
	}
	history, err := s.repo.LookupCandidateHistory(ctx, libraryItemID)
	if err != nil {
		return SearchResult{}, err
	}
	s.logger.Debug().Int64("libraryItemId", libraryItemID).Str("title", input.Title).Str("mediaType", input.MediaType).Msg("workqueue: searching item")
	profilePrefs := s.profilePreferencesForItem(ctx, libraryItemID, input.MediaType)
	var currentSelected *database.ReleaseSummary
	if upgradeSearch && profilePrefs.MinimumUpgradeCustomFormatScore > 0 {
		currentSelected, err = s.repo.GetLatestSelectedReleaseSummaryByLibraryItem(ctx, libraryItemID)
		if err != nil {
			return SearchResult{}, err
		}
	}

	plan := buildSearchRequests(input)
	query := ""
	var (
		results            []hydra.SearchResult
		candidates         []database.SearchCandidateRecord
		combinedCandidates []database.SearchCandidateRecord
		selectedReleaseID  *int64
		lastSearchErr      error
	)

	// For TV episodes, try the full season pack first if the rate limit allows.
	// A season pack covers all episodes in the season and avoids many separate downloads.
	if isEpisodeSearch(input) && input.TVShowID > 0 && input.SeasonNumber > 0 {
		if ok, _ := s.repo.ShouldAttemptSeasonPack(ctx, input.TVShowID, input.SeasonNumber); ok {
			packResult, packSelected, packCandidates, packErr := s.trySeasonPack(ctx, input, history, libraryItemID)
			outcome := database.SeasonPackOutcomeFailed
			if packSelected != nil {
				outcome = database.SeasonPackOutcomeSelected
			}
			_ = s.repo.RecordSeasonPackAttempt(ctx, input.TVShowID, input.SeasonNumber, outcome)
			if packSelected != nil || packErr != nil {
				return packResult, packErr
			}
			// Pack found nothing usable — pre-seed combinedCandidates with any pack
			// results so they remain visible in the picker alongside episode candidates.
			combinedCandidates = packCandidates
		}
	}

	// searchTier runs all requests in a tier and returns true if the caller
	// should stop (selected a release or found good candidates).
	// trustSource=true for tier1 (ID-based): skips title check since indexer
	// guarantees correctness and NZB subjects may be obfuscated.
	searchTier := func(tierRequests []hydra.SearchRequest, trustSource bool) bool {
		req := searchRequirements(input)
		req.TrustSource = trustSource
		for _, candidateRequest := range tierRequests {
			if s.shouldSkipSearchRequest(libraryItemID, candidateRequest, time.Now()) {
				s.logger.Debug().
					Int64("libraryItemId", libraryItemID).
					Str("query", searchRequestLabel(candidateRequest)).
					Msg("workqueue: skipping recent identical Hydra request")
				continue
			}
			query = searchRequestLabel(candidateRequest)
			results, err = s.searchHydraWithRetry(ctx, candidateRequest)
			if err != nil {
				lastSearchErr = err
				err = nil // search errors don't exit early; only DB errors should
				continue
			}
			lastSearchErr = nil
			candidates = buildSearchCandidates(results, req, history, profilePrefs, s.indexerLimits, s.loadIndexerPolicyMap(ctx))
			if upgradeSearch {
				candidates = applyUpgradeCustomFormatMinimum(candidates, currentSelected, profilePrefs.MinimumUpgradeCustomFormatScore)
			}
			combinedCandidates = mergeSearchCandidates(combinedCandidates, candidates)
			selectedReleaseID, err = s.repo.ReplaceSearchCandidates(ctx, libraryItemID, combinedCandidates)
			if err != nil {
				return true // propagate error via outer err variable
			}
			s.rememberSearchRequest(libraryItemID, candidateRequest, searchCandidateOutcome(candidates, selectedReleaseID), time.Now())
			if selectedReleaseID != nil && !shouldContinueSearch(combinedCandidates, input) {
				return true
			}
		}
		return false
	}

	// Tier 1: ID-based queries (tmdbid / imdbid / tvdbid).
	// If these return a usable candidate we skip title queries entirely —
	// same logic as Radarr/Sonarr's IndexerPageableRequestChain.AddTier().
	tier1Done := searchTier(plan.Tier1, true) // ID-based: trust indexer
	if err != nil {
		return SearchResult{}, err
	}

	// Tier 2: title-based fallback — run unless tier 1 definitively stopped early
	// (i.e. found a good, non-failed candidate). Also runs when tier 1 found only
	// rejected candidates, or found a failure-history candidate that warrants
	// checking for a fresher alternative.
	if !tier1Done {
		searchTier(plan.Tier2, false) // title-based: verify title
		if err != nil {
			return SearchResult{}, err
		}
	}
	if selectedReleaseID == nil && len(combinedCandidates) == 0 && lastSearchErr != nil {
		reason := classifySearchFailureReason(lastSearchErr)
		s.logger.Warn().
			Int64("libraryItemId", libraryItemID).
			Str("title", input.Title).
			Str("reason", reason).
			Err(lastSearchErr).
			Msg("workqueue: search failed — indexer unreachable, nothing to blocklist")
		if markErr := s.repo.MarkLibrarySearchFailed(ctx, libraryItemID, reason); markErr != nil {
			return SearchResult{}, markErr
		}
		return SearchResult{}, lastSearchErr
	}
	if selectedReleaseID == nil && len(combinedCandidates) == 0 {
		s.logger.Info().Int64("libraryItemId", libraryItemID).Str("title", input.Title).Msg("workqueue: no matching releases found")
	} else if selectedReleaseID == nil {
		rejected := 0
		for _, c := range combinedCandidates {
			if c.Rejected {
				rejected++
			}
		}
		s.logger.Info().Int64("libraryItemId", libraryItemID).Str("title", input.Title).Int("candidates", len(combinedCandidates)).Int("rejected", rejected).Msg("workqueue: candidates found but none selected")
	}
	if selectedReleaseID != nil {
		finalSelected, err := s.fetchAndImportSelectedRelease(ctx, *selectedReleaseID)
		if err != nil {
			return SearchResult{}, err
		}
		selectedReleaseID = finalSelected
	}
	return SearchResult{
		LibraryItemID:     libraryItemID,
		Query:             query,
		CandidateCount:    len(combinedCandidates),
		SelectedReleaseID: selectedReleaseID,
	}, nil
}

func isEpisodeSearch(input database.LibrarySearchInput) bool {
	t := strings.ToLower(input.MediaType)
	return t == "episode" || t == "tv"
}

func matchesRecentMediaType(input database.LibrarySearchInput, mediaType string) bool {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "movie":
		return strings.EqualFold(input.MediaType, "movie")
	case "episode", "tv":
		return isEpisodeSearch(input)
	default:
		return true
	}
}

// trySeasonPack searches for the full season pack and selects it if found.
// Returns (result, selectedID, packCandidates, err). packCandidates contains
// any season-pack candidates found, filtered to exclude individual episodes —
// the caller should pre-seed its combinedCandidates with these so pack options
// remain visible in the picker even when the individual episode search runs.
// When selectedID is nil and err is nil, no usable pack was selected.
func (s *Service) trySeasonPack(ctx context.Context, input database.LibrarySearchInput, history map[string]database.CandidateHistory, libraryItemID int64) (SearchResult, *int64, []database.SearchCandidateRecord, error) {
	packInput := input
	packInput.EpisodeNumber = 0 // season pack: no episode

	packRequests := buildSeasonPackRequests(packInput)
	var (
		combinedCandidates []database.SearchCandidateRecord
		selectedReleaseID  *int64
	)
	profilePrefs := s.profilePreferencesForItem(ctx, libraryItemID, "episode")
	for _, req := range packRequests {
		if s.shouldSkipSearchRequest(libraryItemID, req, time.Now()) {
			continue
		}
		results, err := s.searchHydraWithRetry(ctx, req)
		if err != nil || len(results) == 0 {
			if err == nil {
				s.rememberSearchRequest(libraryItemID, req, "empty", time.Now())
			}
			continue
		}
		candidates := buildSearchCandidates(results, searchRequirements(packInput), history, profilePrefs, s.indexerLimits, s.loadIndexerPolicyMap(ctx))
		// Filter to season-pack-only: drop individual episode results that
		// Hydra returns when querying by TVDB ID + season without an episode
		// number. Keeps season packs and obfuscated NZBs (no episode token).
		candidates = filterToPacksOnly(candidates, input.SeasonNumber)
		combinedCandidates = mergeSearchCandidates(combinedCandidates, candidates)
		selectedReleaseID, err = s.repo.ReplaceSearchCandidates(ctx, libraryItemID, combinedCandidates)
		if err != nil {
			return SearchResult{}, nil, nil, err
		}
		s.rememberSearchRequest(libraryItemID, req, searchCandidateOutcome(candidates, selectedReleaseID), time.Now())
		if selectedReleaseID != nil {
			break
		}
	}
	if selectedReleaseID == nil {
		// No pack selected — return candidates so the caller can pre-seed the
		// individual episode search and keep pack options visible in the picker.
		return SearchResult{}, nil, combinedCandidates, nil
	}
	final, err := s.fetchAndImportSelectedRelease(ctx, *selectedReleaseID)
	if err != nil {
		return SearchResult{}, nil, nil, err
	}
	return SearchResult{
		LibraryItemID:     libraryItemID,
		CandidateCount:    len(combinedCandidates),
		SelectedReleaseID: final,
	}, final, combinedCandidates, nil
}

// buildSeasonPackRequests produces Hydra queries for a full season (no episode number).
// Mirrors buildSearchRequests: Tier 1 is ID-based (no title), Tier 2 is title-only (no IDs).
func buildSeasonPackRequests(input database.LibrarySearchInput) []hydra.SearchRequest {
	show := input.ShowTitle
	if show == "" {
		show = input.Title
	}
	var requests []hydra.SearchRequest

	seen := func(req hydra.SearchRequest) bool {
		for _, ex := range requests {
			if sameSearchRequest(ex, req) {
				return true
			}
		}
		return false
	}

	// Tier 1: ID-based request (no query string) — consistent with buildSearchRequests.
	if input.ShowTMDBID > 0 || input.ShowTVDBID > 0 || input.ShowIMDbID != "" {
		req := hydra.SearchRequest{
			MediaType:    input.MediaType,
			TMDBID:       input.ShowTMDBID,
			IMDbID:       input.ShowIMDbID,
			TVDBID:       input.ShowTVDBID,
			SeasonNumber: input.SeasonNumber,
		}
		requests = append(requests, req)
	}

	// Tier 2: title-based fallbacks (no IDs) — EpisodeNumber 0 = season pack.
	addTitle := func(q string) {
		q = strings.TrimSpace(q)
		if q == "" {
			return
		}
		req := hydra.SearchRequest{
			MediaType:    input.MediaType,
			Query:        q,
			SeasonNumber: input.SeasonNumber,
		}
		if !seen(req) {
			requests = append(requests, req)
		}
	}
	if input.SeasonNumber > 0 {
		addTitle(fmt.Sprintf("%s S%02d", show, input.SeasonNumber))
		if input.ShowYear > 0 {
			addTitle(fmt.Sprintf("%s %d S%02d", show, input.ShowYear, input.SeasonNumber))
		}
	}
	addTitle(show)
	return requests
}

func (s *Service) searchHydraWithRetry(ctx context.Context, request hydra.SearchRequest) ([]hydra.SearchResult, error) {
	results, err := s.hydra.Search(ctx, request)
	if err == nil {
		return results, nil
	}
	if !isRetryableSearchFailure(err) {
		return nil, err
	}
	return s.hydra.Search(ctx, request)
}

func (s *Service) syncRequestsWithRetry(ctx context.Context) (SyncResult, error) {
	backoff := []time.Duration{0, 1 * time.Second, 2 * time.Second}
	var lastResult SyncResult
	var lastErr error
	for _, delay := range backoff {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return SyncResult{}, ctx.Err()
			case <-time.After(delay):
			}
		}
		lastResult, lastErr = s.SyncRequests(ctx)
		if lastErr == nil {
			return lastResult, nil
		}
		if !isRetryableSeerrSyncFailure(lastErr) {
			return SyncResult{}, lastErr
		}
	}
	if lastErr != nil {
		return SyncResult{}, lastErr
	}
	return lastResult, nil
}

func classifySearchFailureReason(err error) string {
	if err == nil {
		return "search_error"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "search_timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "search_cancelled"
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "deadline exceeded"),
		strings.Contains(message, "timeout"):
		return "search_timeout"
	case strings.Contains(message, "status 401"),
		strings.Contains(message, "status 403"),
		strings.Contains(message, "unauthorized"),
		strings.Contains(message, "forbidden"):
		return "search_auth_error"
	case strings.Contains(message, "status 429"),
		strings.Contains(message, "rate limit"):
		return "search_rate_limited"
	case strings.Contains(message, "status 500"),
		strings.Contains(message, "status 502"),
		strings.Contains(message, "status 503"),
		strings.Contains(message, "status 504"),
		strings.Contains(message, "status 520"),
		strings.Contains(message, "status 521"),
		strings.Contains(message, "status 522"),
		strings.Contains(message, "status 523"),
		strings.Contains(message, "cloudflare unavailable"),
		strings.Contains(message, "connection refused"),
		strings.Contains(message, "no such host"),
		strings.Contains(message, "server misbehaving"):
		return "search_unavailable"
	case strings.Contains(message, "status 524"),
		strings.Contains(message, "cloudflare timeout"),
		strings.Contains(message, "gateway timeout"):
		return "search_timeout"
	default:
		return "search_error"
	}
}

func isRetryableSearchFailure(err error) bool {
	switch classifySearchFailureReason(err) {
	case "search_timeout", "search_unavailable":
		return true
	default:
		return false
	}
}

func isRetryableSeerrSyncFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "timeout"),
		strings.Contains(message, "deadline exceeded"),
		strings.Contains(message, "status 500"),
		strings.Contains(message, "status 502"),
		strings.Contains(message, "status 503"),
		strings.Contains(message, "status 504"),
		strings.Contains(message, "status 520"),
		strings.Contains(message, "status 521"),
		strings.Contains(message, "status 522"),
		strings.Contains(message, "status 523"),
		strings.Contains(message, "status 524"),
		strings.Contains(message, "cloudflare"),
		strings.Contains(message, "bad gateway"),
		strings.Contains(message, "gateway timeout"),
		strings.Contains(message, "connection refused"),
		strings.Contains(message, "no such host"):
		return true
	default:
		return false
	}
}

func (s *Service) fetchAndImportSelectedRelease(ctx context.Context, selectedReleaseID int64) (*int64, error) {
	// If no worker is running (e.g. in unit tests), execute inline — same as the
	// old importSem path but without semaphore contention.
	if !s.downloader.hasWorkers() {
		result, importedRelease, err := s.fetchIndexAndRelease(ctx, selectedReleaseID)
		if err != nil || importedRelease == nil {
			return result, err
		}
		return s.publishImportedRelease(ctx, *importedRelease)
	}
	// Priority 0 (fast-lane) for user-triggered and HTTP-initiated downloads.
	priority := 1
	if isCompletionFastLane(ctx) || isAsyncDownload(ctx) {
		priority = 0
	}
	// HTTP-initiated downloads use a detached context so the download survives
	// when the HTTP request is cancelled or times out after we return.
	jobCtx := ctx
	if isAsyncDownload(ctx) {
		jobCtx = context.Background()
	}
	resultCh := make(chan downloadJobResult, 1)
	submitted := s.downloader.submit(downloadJob{
		ctx:               jobCtx,
		selectedReleaseID: selectedReleaseID,
		priority:          priority,
		enqueuedAt:        time.Now(),
		resultCh:          resultCh,
	})
	if isAsyncDownload(ctx) {
		if submitted {
			// Fire-and-forget: drain the result channel in the background so the
			// download worker is never blocked sending its result.
			go func() { <-resultCh }()
		}
		return nil, nil
	}
	if !submitted {
		// Already queued or in progress — caller doesn't need to wait.
		return nil, nil
	}
	select {
	case result := <-resultCh:
		return result.selectedReleaseID, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// RunDownloadWorker processes download jobs from the dispatcher sequentially.
// Start N goroutines running this in app.go to allow N concurrent downloads
// (each with ~10 dedicated NNTP connections), matching SABnzbd's queue model.
func (s *Service) RunDownloadWorker(ctx context.Context) {
	s.downloader.incWorker()
	defer s.downloader.decWorker()
	for {
		job, ok := s.downloader.next(ctx)
		if !ok {
			return
		}
		// Skip jobs whose caller context already expired while queued.
		select {
		case <-job.ctx.Done():
			s.downloader.markDone(job.selectedReleaseID)
			job.resultCh <- downloadJobResult{nil, job.ctx.Err()}
			continue
		default:
		}
		result, importedRelease, err := s.fetchIndexAndRelease(job.ctx, job.selectedReleaseID)
		// Release the in-flight slot before sending the result so that a
		// promoted release or a re-queued retry can be accepted immediately.
		s.downloader.markDone(job.selectedReleaseID)
		if err != nil || importedRelease == nil {
			job.resultCh <- downloadJobResult{result, err}
			continue
		}
		selectedReleaseID, pubErr := s.publishImportedRelease(job.ctx, *importedRelease)
		job.resultCh <- downloadJobResult{selectedReleaseID, pubErr}
	}
}

// pendingPublish holds data needed to publish an already-indexed release.
type pendingPublish struct {
	current database.ReleaseSummary
	item    database.QueueSnapshot
}

// fetchIndexAndRelease fetches the NZB, bulk-inserts segments, and returns the
// release ready for publishing. Runs under the import semaphore.
func (s *Service) fetchIndexAndRelease(ctx context.Context, selectedReleaseID int64) (*int64, *pendingPublish, error) {
	current, err := s.repo.GetSelectedReleaseSummary(ctx, selectedReleaseID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Concurrent worker already processed or deleted this release — skip quietly.
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if current.FailureCount >= 5 {
		result, err := s.promoteNextAfterFailureDepth(ctx, current, "too_many_failures", 0)
		return result, nil, err
	}
	if current.NZBDocumentID != nil {
		// Already indexed — skip straight to publish.
		return nil, &pendingPublish{current: current}, nil
	}
	if err := s.repo.MarkSelectedReleaseFetching(ctx, current.SelectedReleaseID); err != nil {
		return nil, nil, err
	}
	fileName, raw, err := s.fetcher.Fetch(ctx, current.ExternalURL)
	if err != nil {
		result, err := s.promoteNextAfterFailureDepth(ctx, current, err.Error(), 0)
		return result, nil, err
	}
	// Persist raw NZB bytes immediately after download so that a crash or restart
	// between here and ImportSelectedReleaseNZB does not cause a re-download.
	// NZBDocumentID != nil check at the top of this function will reuse the stored
	// bytes on the next attempt instead of calling NZBFinder again.
	if storeErr := s.repo.StoreRawNZBDocument(ctx, current.SelectedReleaseID, fileName, raw, current.ExternalURL); storeErr != nil {
		if isFKViolation(storeErr) {
			return nil, nil, nil
		}
		s.logger.Warn().Err(storeErr).Int64("srId", current.SelectedReleaseID).Msg("early NZB store failed — will re-download on next attempt if restart occurs")
	}
	imported, err := nzb.BuildImportedNZB(fileName, raw, fmt.Sprintf("selected-release:%d", current.SelectedReleaseID), current.ExternalURL)
	if err != nil {
		result, err := s.promoteNextAfterFailureDepth(ctx, current, err.Error(), 0)
		return result, nil, err
	}
	if s.earlyChecker != nil {
		if msgID := largestFileFirstSegment(imported.Files); msgID != "" {
			if err := s.earlyChecker(ctx, msgID); err != nil {
				if shouldIgnoreEarlyPreflightFailure(err) {
					s.logger.Info().Err(err).Int64("srId", current.SelectedReleaseID).Msg("early preflight advisory only; continuing import")
				} else {
					result, err := s.promoteNextAfterFailureDepth(ctx, current, fmt.Sprintf("early preflight: %s", err), 0)
					return result, nil, err
				}
			}
		}
	}
	item, err := s.repo.ImportSelectedReleaseNZB(ctx, current.SelectedReleaseID, imported)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil
		}
		result, err := s.promoteNextAfterFailure(ctx, current, err.Error())
		return result, nil, err
	}
	if item.NZBDocumentID != nil {
		if calErr := s.repo.CalibrateNZBOffsets(ctx, *item.NZBDocumentID); calErr != nil {
			s.logger.Warn().Err(calErr).Int64("nzbDocumentID", *item.NZBDocumentID).Msg("calibrate: yEnc offset prefetch failed")
		}
	}
	if err := s.repo.SetImportedNZBIndexed(ctx, item.QueueItemID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil
		}
		result, err := s.promoteNextAfterFailure(ctx, current, err.Error())
		return result, nil, err
	}
	item.State = database.QueuePreflight
	if s.preflightChecker != nil {
		if err := s.preflightChecker(ctx, item); err != nil {
			if shouldIgnorePreflightFailure(err) {
				s.logger.Info().Err(err).Int64("srId", current.SelectedReleaseID).Msg("preflight advisory only; continuing publish")
			} else {
				result, err := s.promoteNextAfterFailureDepth(ctx, current, err.Error(), 0)
				return result, nil, err
			}
		}
	}
	return nil, &pendingPublish{current: current, item: item}, nil
}

// publishImportedRelease runs postImportHook (symlinks, episode items, Plex)
// without holding the import semaphore so other fetches can proceed in parallel.
func (s *Service) publishImportedRelease(ctx context.Context, p pendingPublish) (*int64, error) {
	if p.current.NZBDocumentID != nil && p.item.QueueItemID == 0 {
		// Already indexed in a previous run — re-import from stored NZB.
		return s.retrySelectedReleaseFromStoredNZB(ctx, p.current, 0)
	}
	updated, err := s.repo.GetSelectedReleaseSummary(ctx, p.current.SelectedReleaseID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err == nil && updated.VirtualFileCount == 0 && strings.TrimSpace(updated.ArchiveRejects) != "" {
		return s.promoteNextAfterFailure(ctx, p.current, updated.ArchiveRejects)
	}
	if p.item.QueueItemID > 0 {
		if publisher, ok := s.repo.(interface {
			MarkQueueItemPublishing(context.Context, int64) error
		}); ok {
			if err := publisher.MarkQueueItemPublishing(ctx, p.item.QueueItemID); err != nil {
				return s.promoteNextAfterFailure(ctx, p.current, err.Error())
			}
			p.item.State = database.QueuePublishing
		}
	}
	if s.postImportHook != nil {
		if err := s.postImportHook(ctx, p.item); err != nil {
			failureReason := err.Error()
			if errors.Is(err, library.ErrNoVirtualFiles) {
				if updated, lookupErr := s.repo.GetSelectedReleaseSummary(ctx, p.current.SelectedReleaseID); lookupErr == nil && strings.TrimSpace(updated.ArchiveRejects) != "" {
					failureReason = updated.ArchiveRejects
				}
			}
			return s.promoteNextAfterFailure(ctx, p.current, failureReason)
		}
	}
	value := p.current.SelectedReleaseID
	return &value, nil
}

func (s *Service) fetchAndImportSelectedReleaseDepth(ctx context.Context, selectedReleaseID int64, depth int) (*int64, error) {
	current, err := s.repo.GetSelectedReleaseSummary(ctx, selectedReleaseID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Concurrent worker already processed or deleted this release — skip quietly.
			return nil, nil
		}
		return nil, err
	}
	// Hard guard: if this release has already failed 5+ times, blocklist it
	// immediately and promote the next candidate. This prevents infinite retry
	// loops (e.g. 403 from NZBFinder causing 870 identical attempts).
	if current.FailureCount >= 5 {
		return s.promoteNextAfterFailureDepth(ctx, current, "too_many_failures", depth)
	}
	// If the NZB was already downloaded in a previous attempt (e.g. stuck in
	// preflight then reset), re-use the stored document instead of fetching again.
	// This prevents duplicate entries in NZBHydra's download history.
	if current.NZBDocumentID != nil {
		return s.retrySelectedReleaseFromStoredNZB(ctx, current, depth)
	}
	if err := s.repo.MarkSelectedReleaseFetching(ctx, current.SelectedReleaseID); err != nil {
		return nil, err
	}
	fileName, raw, err := s.fetcher.Fetch(ctx, current.ExternalURL)
	if err != nil {
		return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
	}
	if storeErr := s.repo.StoreRawNZBDocument(ctx, current.SelectedReleaseID, fileName, raw, current.ExternalURL); storeErr != nil {
		if isFKViolation(storeErr) {
			return nil, nil
		}
		s.logger.Warn().Err(storeErr).Int64("srId", current.SelectedReleaseID).Msg("early NZB store failed — will re-download on next attempt if restart occurs")
	}
	imported, err := nzb.BuildImportedNZB(fileName, raw, fmt.Sprintf("selected-release:%d", current.SelectedReleaseID), current.ExternalURL)
	if err != nil {
		return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
	}
	// Quick NNTP STAT check before the expensive archive inspection + DB import.
	// Pick the first segment of the largest NZB file (proxy for the main content file).
	if s.earlyChecker != nil {
		if msgID := largestFileFirstSegment(imported.Files); msgID != "" {
			if err := s.earlyChecker(ctx, msgID); err != nil {
				if shouldIgnoreEarlyPreflightFailure(err) {
					s.logger.Info().Err(err).Int64("srId", current.SelectedReleaseID).Msg("early preflight advisory only; continuing import")
				} else {
					return s.promoteNextAfterFailureDepth(ctx, current, fmt.Sprintf("early preflight: %s", err), depth)
				}
			}
		}
	}
	return s.importSelectedRelease(ctx, current, imported, depth)
}

// largestFileFirstSegment returns the first segment message ID of the largest
// likely-payload file in the NZB, skipping files with no segments plus obvious
// support files such as PAR2/NFO/JPG and sample clips. If every file is a
// support file, it falls back to the largest file overall.
func largestFileFirstSegment(files []database.ImportedNZBFile) string {
	best := pickLargestEarlyProbeFile(files, false)
	if len(best.Segments) == 0 {
		best = pickLargestEarlyProbeFile(files, true)
		if len(best.Segments) == 0 {
			return ""
		}
	}
	return best.Segments[0].MessageID
}

func pickLargestEarlyProbeFile(files []database.ImportedNZBFile, includeSupport bool) database.ImportedNZBFile {
	var best database.ImportedNZBFile
	for _, f := range files {
		if len(f.Segments) == 0 {
			continue
		}
		if !includeSupport && isEarlyProbeSupportFile(f.FileName) {
			continue
		}
		if f.FileSizeBytes > best.FileSizeBytes {
			best = f
		}
	}
	return best
}

func isEarlyProbeSupportFile(name string) bool {
	base := strings.ToLower(filepath.Base(strings.TrimSpace(name)))
	if base == "" {
		return false
	}
	switch filepath.Ext(base) {
	case ".par2", ".sfv", ".nfo", ".jpg", ".jpeg", ".png":
		return true
	}
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	return stem == "sample" ||
		strings.HasPrefix(stem, "sample-") ||
		strings.HasPrefix(stem, "sample_") ||
		strings.HasSuffix(stem, "-sample") ||
		strings.HasSuffix(stem, "_sample")
}

func shouldIgnoreEarlyPreflightFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "article missing") ||
		strings.Contains(msg, "article not found") ||
		strings.Contains(msg, "status 430") ||
		strings.Contains(msg, " 430")
}

func shouldIgnorePreflightFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if !strings.HasPrefix(msg, "preflight:") {
		return false
	}
	return strings.Contains(msg, "article missing") ||
		strings.Contains(msg, "article not found") ||
		strings.Contains(msg, "status 430") ||
		strings.Contains(msg, " 430")
}

// dedupeSearchResults removes true duplicates — the same release reported more
// than once by the same indexer. Results from different indexers for the same
// release are intentionally kept as separate candidates so that the fallback
// chain (FailSelectedReleaseAndPromoteNext) can try every available source
// before giving up. Within a (title, size, indexer) group the entry with the
// highest IndexerScore wins; Grabs breaks ties.
func dedupeSearchResults(results []hydra.SearchResult) []hydra.SearchResult {
	type seenKey struct {
		title   string
		indexer string
	}
	type sizeBucket struct {
		size int64
		best hydra.SearchResult
	}
	// Map by (normalized title, indexer) → slice of per-size buckets.
	// Different indexers for the same release are distinct candidates.
	seen := make(map[seenKey][]sizeBucket, len(results))
	for _, r := range results {
		key := seenKey{normReleaseTitle(r.Title), r.Indexer}
		matched := false
		for i := range seen[key] {
			if sizesClose(r.SizeBytes, seen[key][i].size) {
				if r.IndexerScore > seen[key][i].best.IndexerScore ||
					(r.IndexerScore == seen[key][i].best.IndexerScore && r.Grabs > seen[key][i].best.Grabs) {
					seen[key][i].best = r
				}
				matched = true
				break
			}
		}
		if !matched {
			seen[key] = append(seen[key], sizeBucket{size: r.SizeBytes, best: r})
		}
	}
	out := make([]hydra.SearchResult, 0, len(results))
	for _, buckets := range seen {
		for _, b := range buckets {
			out = append(out, b.best)
		}
	}
	return out
}

// normReleaseTitle lowercases a release title and collapses all separators
// (dots, dashes, underscores, brackets) to single spaces so that e.g.
// "Show.S01E01.1080p" and "Show S01E01 1080p" compare equal.
func normReleaseTitle(title string) string {
	replacer := strings.NewReplacer(".", " ", "_", " ", "-", " ", "[", " ", "]", " ", "(", " ", ")", " ")
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(title))), " ")
}

// sizesClose returns true when two sizes are within 5% of each other.
func sizesClose(a, b int64) bool {
	if a == 0 || b == 0 {
		return a == b
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	max := a
	if b > a {
		max = b
	}
	return diff*20 <= max
}

func buildSearchCandidates(results []hydra.SearchResult, required ranking.Requirements, history map[string]database.CandidateHistory, prefs ranking.Preferences, limits IndexerLimits, indexerPolicies map[string]int) []database.SearchCandidateRecord {
	results = dedupeSearchResults(results)
	now := time.Now()
	candidates := make([]database.SearchCandidateRecord, 0, len(results))
	for _, result := range results {
		if result.Passworded {
			continue
		}
		// Minimum Age: skip releases posted too recently (Sonarr/Radarr behaviour).
		if limits.MinimumAgeMinutes > 0 && !result.PublishedAt.IsZero() {
			age := now.Sub(result.PublishedAt)
			if age < time.Duration(limits.MinimumAgeMinutes)*time.Minute {
				continue
			}
		}
		// Retention: skip releases older than provider retention window.
		if limits.RetentionDays > 0 && !result.PublishedAt.IsZero() {
			cutoff := now.AddDate(0, 0, -limits.RetentionDays)
			if result.PublishedAt.Before(cutoff) {
				continue
			}
		}
		// Maximum Size: reject oversized releases.
		if limits.MaximumSizeMB > 0 && result.SizeBytes > int64(limits.MaximumSizeMB)*1024*1024 {
			continue
		}
		// Episode mismatch: reject candidates belonging to a different episode.
		// Season-pack titles (no SxxEnn token) are allowed through.
		if required.EpisodeNumber > 0 && hasWrongEpisodeToken(strings.ToLower(result.Title), required.SeasonNumber, required.EpisodeNumber) {
			candidates = append(candidates, database.SearchCandidateRecord{
				Title:        result.Title,
				ExternalURL:  result.Link,
				IndexerName:  result.Indexer,
				SizeBytes:    result.SizeBytes,
				PostedAt:     result.PublishedAt,
				Score:        0,
				Rejected:     true,
				RejectReason: "wrong_episode",
			})
			continue
		}

		known := history[strings.TrimSpace(result.Link)]
		effectiveFailureCount, degraded, durableRejectThreshold := candidateFailurePenaltyProfile(known)
		// nzbdav-style tolerance: a prior failed download should penalize a
		// candidate, not immediately disqualify it. Only URLs that have failed
		// repeatedly for a non-transient reason are durably rejected up front.
		// Transient exceptions:
		//   - interrupted_by_restart / stale_worker: process died mid-download.
		//   - status 403: indexer quota exhausted temporarily.
		if known.FailureCount >= durableRejectThreshold {
			lr := strings.ToLower(known.LastFailureReason)
			isRestartInterruption := strings.Contains(lr, "interrupted_by_restart") || strings.Contains(lr, "stale_worker")
			isTemporaryQuota := strings.Contains(lr, "status 403")
			if !isRestartInterruption && !isTemporaryQuota {
				candidates = append(candidates, database.SearchCandidateRecord{
					Title:             result.Title,
					ExternalURL:       result.Link,
					IndexerName:       result.Indexer,
					SizeBytes:         result.SizeBytes,
					PostedAt:          result.PublishedAt,
					Score:             0,
					Explanations:      []string{"Rejected before ranking: this exact release URL previously failed and is durably blocked."},
					Rejected:          true,
					RejectReason:      "previously_failed",
					FailureCount:      known.FailureCount,
					LastFailureReason: known.LastFailureReason,
				})
				continue
			}
		}
		parsed := parseCandidate(result, known, effectiveFailureCount, degraded, indexerPolicies)
		score := ranking.ScoreWithPreferences(parsed, required, prefs)
		candidates = append(candidates, database.SearchCandidateRecord{
			Title:                 result.Title,
			ExternalURL:           result.Link,
			IndexerName:           result.Indexer,
			SizeBytes:             result.SizeBytes,
			PostedAt:              result.PublishedAt,
			Score:                 score.Score,
			CustomFormatScore:     score.CustomFormatScore,
			Explanations:          score.Explanations,
			CompatibilityWarnings: score.CompatibilityWarnings,
			Rejected:              score.Rejected,
			RejectReason:          score.RejectReason,
			FailureCount:          known.FailureCount,
			LastFailureReason:     known.LastFailureReason,
			Resolution:            parsed.Resolution,
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Rejected != candidates[j].Rejected {
			return !candidates[i].Rejected
		}
		return candidates[i].Score > candidates[j].Score
	})
	return candidates
}

func candidateFailurePenaltyProfile(history database.CandidateHistory) (effectiveFailureCount int, degraded bool, durableRejectThreshold int) {
	effectiveFailureCount = history.FailureCount
	degraded = history.FailureCount > 0
	// Sonarr blocks on first failure. Allow 1 retry only for truly transient
	// reasons (process restart mid-download, indexer quota). Everything else
	// is rejected after the first attempt.
	durableRejectThreshold = 1
	if isSoftCandidateFailureReason(history.LastFailureReason) {
		degraded = false
		durableRejectThreshold = 3
		if effectiveFailureCount <= 1 {
			effectiveFailureCount = 0
		} else {
			effectiveFailureCount -= 1
		}
	}
	if effectiveFailureCount < 0 {
		effectiveFailureCount = 0
	}
	return effectiveFailureCount, degraded, durableRejectThreshold
}

func isSoftCandidateFailureReason(reason string) bool {
	reason = strings.TrimSpace(strings.ToLower(reason))
	if reason == "" {
		return false
	}
	if strings.Contains(reason, "interrupted_by_restart") || strings.Contains(reason, "stale_worker") || strings.Contains(reason, "status 403") || strings.Contains(reason, "status 430") {
		return true
	}
	if isRetryablePreflightCandidateReason(reason) {
		return true
	}
	switch reason {
	case "archive_headers_invalid", "archive_video_not_found", "no_publishable_files":
		return true
	}
	return false
}

func isRetryablePreflightCandidateReason(reason string) bool {
	if !(strings.HasPrefix(reason, "early preflight:") || strings.HasPrefix(reason, "preflight:")) {
		return false
	}
	return strings.Contains(reason, "article missing") ||
		strings.Contains(reason, "article not found") ||
		strings.Contains(reason, "430")
}

func hasNonRejectedCandidate(candidates []database.SearchCandidateRecord) bool {
	for _, c := range candidates {
		if !c.Rejected {
			return true
		}
	}
	return false
}

func applyUpgradeCustomFormatMinimum(candidates []database.SearchCandidateRecord, current *database.ReleaseSummary, minimumIncrement int) []database.SearchCandidateRecord {
	if current == nil || minimumIncrement <= 0 {
		return candidates
	}
	requiredScore := current.CustomFormatScore + minimumIncrement
	for i := range candidates {
		if candidates[i].Rejected {
			continue
		}
		if candidates[i].CustomFormatScore < requiredScore {
			candidates[i].Rejected = true
			candidates[i].RejectReason = "upgrade_custom_format_score"
		}
	}
	return candidates
}

func searchRequirements(input database.LibrarySearchInput) ranking.Requirements {
	mediaType := input.MediaType
	if isWholeShowRequest(input) {
		mediaType = "tv"
	}
	required := ranking.Requirements{
		MediaType:       mediaType,
		Year:            input.MovieYear,
		SeasonNumber:    input.SeasonNumber,
		EpisodeNumber:   input.EpisodeNumber,
		Title:           input.Title,
		AlternateTitles: input.AlternateTitles,
		RuntimeMinutes:  input.RuntimeMinutes,
	}
	if input.ShowTitle != "" {
		required.Title = input.ShowTitle
		required.Year = input.ShowYear
	}
	return required
}

func mergeSearchCandidates(existing, incoming []database.SearchCandidateRecord) []database.SearchCandidateRecord {
	merged := make(map[string]database.SearchCandidateRecord, len(existing)+len(incoming))
	order := make([]string, 0, len(existing)+len(incoming))
	for _, candidate := range existing {
		key := candidateIdentity(candidate)
		if _, ok := merged[key]; !ok {
			order = append(order, key)
		}
		merged[key] = candidate
	}
	for _, candidate := range incoming {
		key := candidateIdentity(candidate)
		current, ok := merged[key]
		if !ok {
			order = append(order, key)
			merged[key] = candidate
			continue
		}
		if betterSearchCandidate(candidate, current) {
			merged[key] = candidate
		}
	}
	out := make([]database.SearchCandidateRecord, 0, len(order))
	for _, key := range order {
		out = append(out, merged[key])
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Rejected != out[j].Rejected {
			return !out[i].Rejected
		}
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if out[i].FailureCount != out[j].FailureCount {
			return out[i].FailureCount < out[j].FailureCount
		}
		return out[i].PostedAt.After(out[j].PostedAt)
	})
	return out
}

func candidateIdentity(candidate database.SearchCandidateRecord) string {
	if strings.TrimSpace(candidate.ExternalURL) != "" {
		return "url:" + strings.TrimSpace(candidate.ExternalURL)
	}
	return "title:" + strings.ToLower(strings.TrimSpace(candidate.Title))
}

func betterSearchCandidate(left, right database.SearchCandidateRecord) bool {
	if left.Rejected != right.Rejected {
		return !left.Rejected
	}
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	if left.FailureCount != right.FailureCount {
		return left.FailureCount < right.FailureCount
	}
	if !left.PostedAt.Equal(right.PostedAt) {
		return left.PostedAt.After(right.PostedAt)
	}
	return false
}

func shouldContinueSearch(candidates []database.SearchCandidateRecord, input database.LibrarySearchInput) bool {
	for _, candidate := range candidates {
		if candidate.Rejected {
			continue
		}
		if shouldKeepSearchingPastCandidate(candidate, input) {
			continue // season pack — keep scanning for a specific-episode candidate
		}
		if candidate.FailureCount == 0 {
			return false // untried, non-season-pack candidate exists
		}
	}
	return true // all non-rejected candidates are season packs or have failures
}

func shouldKeepSearchingPastCandidate(candidate database.SearchCandidateRecord, input database.LibrarySearchInput) bool {
	if isWholeShowRequest(input) {
		return false
	}
	if !strings.EqualFold(input.MediaType, "episode") && !strings.EqualFold(input.MediaType, "tv") {
		return false
	}
	title := normalizeSearchText(candidate.Title)
	if hasExactEpisodeToken(title, input.SeasonNumber, input.EpisodeNumber) {
		return false
	}
	return hasSeasonPackToken(title, input.SeasonNumber)
}

// hasWrongEpisodeToken returns true when the title contains an episode token for
// the right season but a different episode. Handles SxxEnn and NxMM formats.
// Season-pack titles (no episode token) return false so they pass through.
func hasWrongEpisodeToken(titleLower string, seasonNumber, episodeNumber int) bool {
	if seasonNumber <= 0 || episodeNumber <= 0 {
		return false
	}
	// SxxEnn format: e.g. s05e01
	prefix := fmt.Sprintf("s%02de", seasonNumber)
	if idx := strings.Index(titleLower, prefix); idx >= 0 {
		rest := titleLower[idx+len(prefix):]
		ep, digits := 0, 0
		for _, ch := range rest {
			if ch >= '0' && ch <= '9' {
				ep = ep*10 + int(ch-'0')
				digits++
			} else {
				break
			}
		}
		if digits > 0 && ep != episodeNumber {
			return true
		}
	}
	// NxMM format: e.g. 5x01
	for e := 1; e <= 99; e++ {
		if e == episodeNumber {
			continue
		}
		if strings.Contains(titleLower, fmt.Sprintf("%dx%02d", seasonNumber, e)) {
			return true
		}
	}
	return false
}

// filterToPacksOnly removes individual-episode candidates from a season-pack
// search result set. Keeps season packs and obfuscated NZBs (no episode token).
// This prevents Hydra's "TVDB ID + season" responses from polluting the picker
// with wrong-episode results when no episode number was sent in the request.
func filterToPacksOnly(candidates []database.SearchCandidateRecord, seasonNumber int) []database.SearchCandidateRecord {
	out := make([]database.SearchCandidateRecord, 0, len(candidates))
	for _, c := range candidates {
		titleLower := strings.ToLower(c.Title)
		if c.Rejected || hasSeasonPackToken(normalizeSearchText(titleLower), seasonNumber) || !hasAnyIndividualEpisodeToken(titleLower) {
			out = append(out, c)
		}
	}
	return out
}

// hasAnyIndividualEpisodeToken returns true when a title contains a SxxEnn or
// NxMM episode token for any season/episode (indicating a single-episode release).
func hasAnyIndividualEpisodeToken(titleLower string) bool {
	for s := 1; s <= 40; s++ {
		prefix := fmt.Sprintf("s%02de", s)
		if idx := strings.Index(titleLower, prefix); idx >= 0 {
			rest := titleLower[idx+len(prefix):]
			if len(rest) > 0 && rest[0] >= '0' && rest[0] <= '9' {
				return true
			}
		}
		for e := 1; e <= 99; e++ {
			if strings.Contains(titleLower, fmt.Sprintf("%dx%02d", s, e)) {
				return true
			}
		}
	}
	return false
}

func hasExactEpisodeToken(title string, seasonNumber, episodeNumber int) bool {
	if seasonNumber <= 0 || episodeNumber <= 0 {
		return false
	}
	for _, token := range []string{
		fmt.Sprintf("s%02de%02d", seasonNumber, episodeNumber),
		fmt.Sprintf("%dx%02d", seasonNumber, episodeNumber),
		fmt.Sprintf("%d x %02d", seasonNumber, episodeNumber),
	} {
		if strings.Contains(title, token) {
			return true
		}
	}
	return false
}

func hasSeasonPackToken(title string, seasonNumber int) bool {
	if seasonNumber <= 0 {
		return false
	}
	for _, token := range []string{
		fmt.Sprintf("season %d", seasonNumber),
		fmt.Sprintf("s%02d", seasonNumber),
	} {
		if strings.Contains(title, token) && (strings.Contains(title, "complete") || strings.Contains(title, "pack")) {
			return true
		}
	}
	return false
}

func normalizeSearchText(value string) string {
	replacer := strings.NewReplacer(".", " ", "_", " ", "-", " ", "[", " ", "]", " ", "(", " ", ")", " ")
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(value))), " ")
}

func (s *Service) importSelectedRelease(ctx context.Context, current database.ReleaseSummary, imported database.ImportedNZB, depth int) (*int64, error) {
	item, err := s.repo.ImportSelectedReleaseNZB(ctx, current.SelectedReleaseID, imported)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
	}
	if item.NZBDocumentID != nil {
		if calErr := s.repo.CalibrateNZBOffsets(ctx, *item.NZBDocumentID); calErr != nil {
			s.logger.Warn().Err(calErr).Int64("nzbDocumentID", *item.NZBDocumentID).Msg("calibrate: yEnc offset prefetch failed")
		}
	}
	if err := s.repo.SetImportedNZBIndexed(ctx, item.QueueItemID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
	}
	item.State = database.QueuePreflight
	// Preflight: verify first segments are reachable on NNTP before publishing.
	// Mirrors nzbdav's FetchFirstSegmentsStep — catches expired/incomplete NZBs
	// early and falls back to the next search candidate instead of publishing dead content.
	if s.preflightChecker != nil {
		if err := s.preflightChecker(ctx, item); err != nil {
			if shouldIgnorePreflightFailure(err) {
				s.logger.Info().Err(err).Int64("srId", current.SelectedReleaseID).Msg("preflight advisory only; continuing publish")
			} else {
				return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
			}
		}
	}
	// Fast-fail: if no virtual files were created, skip publish immediately.
	// Don't call postImportHook (FUSE symlinks, subtitles, Plex) for an empty release.
	updated, err := s.repo.GetSelectedReleaseSummary(ctx, current.SelectedReleaseID)
	if err == nil && updated.VirtualFileCount == 0 {
		reason := strings.TrimSpace(updated.ArchiveRejects)
		if reason == "" {
			reason = "no_publishable_files"
		}
		return s.promoteNextAfterFailureDepth(ctx, current, reason, depth)
	}
	if s.postImportHook != nil {
		if err := s.postImportHook(ctx, item); err != nil {
			failureReason := err.Error()
			if errors.Is(err, library.ErrNoVirtualFiles) {
				if updated2, lookupErr := s.repo.GetSelectedReleaseSummary(ctx, current.SelectedReleaseID); lookupErr == nil && strings.TrimSpace(updated2.ArchiveRejects) != "" {
					failureReason = updated2.ArchiveRejects
				}
			}
			return s.promoteNextAfterFailureDepth(ctx, current, failureReason, depth)
		}
	}
	value := current.SelectedReleaseID
	return &value, nil
}

func (s *Service) promoteNextAfterFailure(ctx context.Context, current database.ReleaseSummary, reason string) (*int64, error) {
	return s.promoteNextAfterFailureDepth(ctx, current, reason, 0)
}

// cleanupCtx returns a non-canceled context for DB state cleanup. If the
// caller's context is already canceled (e.g. BullMQ stalled-job detection),
// DB calls to record the failure would fail immediately, leaving the item
// stuck in a transitional state. Using context.Background() as the fallback
// is safe: FailSelectedReleaseAndPromoteNext wraps with its own 90s timeout.
func cleanupCtx(ctx context.Context) context.Context {
	if ctx.Err() == nil {
		return ctx
	}
	return context.Background()
}

func (s *Service) maxInlineFallbackDepth(ctx context.Context) int {
	if s == nil {
		return defaultInlineFallbackDepth
	}
	if isCompletionFastLane(ctx) {
		return fastLaneInlineFallbackDepth
	}
	if s.repo != nil {
		if backlog, err := s.repo.CountActiveSearchBacklog(ctx); err == nil && backlog >= busyQueueDepthThreshold {
			return busyInlineFallbackDepth
		}
	}
	if s.WorkQueue == nil {
		return defaultInlineFallbackDepth
	}
	if s.WorkQueue.Depth(ctx) >= busyQueueDepthThreshold {
		return busyInlineFallbackDepth
	}
	return defaultInlineFallbackDepth
}

// promoteNextAfterFailureDepth adds a depth counter to prevent infinite recursive
// promotion chains (e.g. all candidates fail with 403 from the indexer).
// Radarr/Sonarr never recurse here — they let the scheduler re-try later.
// Under heavy backlog, cap inline churn aggressively so one bad episode does
// not monopolize workers for minutes while hundreds of other items wait.
func (s *Service) promoteNextAfterFailureDepth(ctx context.Context, current database.ReleaseSummary, reason string, depth int) (*int64, error) {
	s.logger.Warn().
		Int64("libraryItemId", current.LibraryItemID).
		Str("release", current.Title).
		Str("reason", reason).
		Msg("workqueue: release failed — checking for next candidate")
	// Use a fresh context for DB cleanup if the caller's context was already
	// canceled — prevents the item from remaining stuck in a transitional state.
	dbCtx := cleanupCtx(ctx)
	if depth >= s.maxInlineFallbackDepth(ctx) {
		// Stop inline candidate churn and leave the next candidate selected for a
		// later queue pass. This keeps throughput fair when backlog is large.
		if _, depthErr := s.repo.FailSelectedReleaseAndPromoteNext(context.Background(), current.SelectedReleaseID, reason); depthErr != nil {
			s.logger.Error().Err(depthErr).Int64("selectedReleaseId", current.SelectedReleaseID).Msg("workqueue: depth-limit fail failed")
			_ = s.repo.MarkLibrarySearchFailed(context.Background(), current.LibraryItemID, reason)
		}
		return nil, nil
	}
	metrics.M.FallbackReleaseAttempts.Add(1)
	next, promoteErr := s.repo.FailSelectedReleaseAndPromoteNext(dbCtx, current.SelectedReleaseID, reason)
	if promoteErr != nil {
		// Transient DB errors (e.g. "driver: bad connection") must not leave the item
		// stuck in fetching_nzb. Retry with increasing backoff; use a fresh
		// context.Background() for each attempt so a stale/canceled caller context
		// does not permanently block cleanup.
		for attempt, delay := range []time.Duration{300 * time.Millisecond, 1 * time.Second, 3 * time.Second} {
			time.Sleep(delay)
			next, promoteErr = s.repo.FailSelectedReleaseAndPromoteNext(context.Background(), current.SelectedReleaseID, reason)
			if promoteErr == nil {
				break
			}
			s.logger.Warn().Err(promoteErr).Int("attempt", attempt+1).Int64("libraryItemId", current.LibraryItemID).Msg("workqueue: promote retry failed")
		}
		if promoteErr != nil {
			s.logger.Error().Err(promoteErr).Int64("libraryItemId", current.LibraryItemID).Msg("workqueue: promote failed — falling back to direct fail")
			_ = s.repo.MarkLibrarySearchFailed(context.Background(), current.LibraryItemID, reason)
			return nil, nil
		}
	}
	if next == nil {
		return nil, nil
	}
	if next.FailureCount >= 5 {
		// This candidate has already failed many times — skip it and promote again.
		return s.promoteNextAfterFailureDepth(ctx, *next, "too_many_failures", depth+1)
	}
	if strings.TrimSpace(next.ExternalURL) == "" {
		return s.retrySelectedReleaseFromStoredNZB(ctx, *next, depth+1)
	}
	// Recursively try the next candidate, but track depth to prevent stack overflow.
	result, err := s.fetchAndImportSelectedReleaseDepth(ctx, next.SelectedReleaseID, depth+1)
	return result, err
}

func (s *Service) retrySelectedReleaseFromStoredNZB(ctx context.Context, current database.ReleaseSummary, depth int) (*int64, error) {
	if err := s.repo.MarkSelectedReleaseFetching(ctx, current.SelectedReleaseID); err != nil {
		return nil, err
	}
	doc, err := s.repo.GetStoredNZBDocument(ctx, current.SelectedReleaseID)
	if err != nil {
		return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
	}
	imported, err := nzb.BuildImportedNZB(doc.FileName, doc.XML, fmt.Sprintf("selected-release:%d:stored", current.SelectedReleaseID), doc.ExternalURL)
	if err != nil {
		return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
	}
	return s.importSelectedRelease(ctx, current, imported, depth)
}

func (s *Service) SelectRelease(ctx context.Context, releaseCandidateID int64) (ReleaseActionResult, error) {
	current, err := s.repo.SelectReleaseCandidate(ctx, releaseCandidateID)
	if err != nil {
		return ReleaseActionResult{}, err
	}
	if current == nil {
		return ReleaseActionResult{ReleaseCandidateID: releaseCandidateID, Action: "selected"}, nil
	}
	finalSelected, err := s.fetchAndImportSelectedRelease(ctx, current.SelectedReleaseID)
	if err != nil {
		return ReleaseActionResult{}, err
	}
	return ReleaseActionResult{
		ReleaseCandidateID: releaseCandidateID,
		Action:             "selected",
		SelectedReleaseID:  finalSelected,
	}, nil
}

func (s *Service) RejectRelease(ctx context.Context, releaseCandidateID int64, reason string) (ReleaseActionResult, error) {
	if strings.TrimSpace(reason) == "" {
		reason = "manual_reject"
	}
	next, err := s.repo.RejectReleaseCandidate(ctx, releaseCandidateID, reason)
	if err != nil {
		return ReleaseActionResult{}, err
	}
	if next == nil {
		return ReleaseActionResult{
			ReleaseCandidateID: releaseCandidateID,
			Action:             "rejected",
		}, nil
	}
	finalSelected, err := s.fetchAndImportSelectedRelease(ctx, next.SelectedReleaseID)
	if err != nil {
		return ReleaseActionResult{}, err
	}
	return ReleaseActionResult{
		ReleaseCandidateID: releaseCandidateID,
		Action:             "rejected",
		SelectedReleaseID:  finalSelected,
	}, nil
}

func (s *Service) RestoreRelease(ctx context.Context, releaseCandidateID int64) (ReleaseActionResult, error) {
	if err := s.repo.RestoreReleaseCandidate(ctx, releaseCandidateID); err != nil {
		return ReleaseActionResult{}, err
	}
	return ReleaseActionResult{
		ReleaseCandidateID: releaseCandidateID,
		Action:             "restored",
	}, nil
}

func (s *Service) RestoreRejectedReleases(ctx context.Context, libraryItemID int64) (database.RejectedReleaseRestoreResult, error) {
	return s.repo.RestoreRejectedReleaseCandidates(ctx, libraryItemID)
}

func (s *Service) SkipRelease(ctx context.Context, releaseCandidateID int64) (ReleaseActionResult, error) {
	next, err := s.repo.SkipReleaseCandidate(ctx, releaseCandidateID)
	if err != nil {
		return ReleaseActionResult{}, err
	}
	if next == nil {
		return ReleaseActionResult{
			ReleaseCandidateID: releaseCandidateID,
			Action:             "skipped",
		}, nil
	}
	finalSelected, err := s.fetchAndImportSelectedRelease(ctx, next.SelectedReleaseID)
	if err != nil {
		return ReleaseActionResult{}, err
	}
	return ReleaseActionResult{
		ReleaseCandidateID: releaseCandidateID,
		Action:             "skipped",
		SelectedReleaseID:  finalSelected,
	}, nil
}

// buildSearchRequests creates a tiered list of Hydra search requests matching
// Radarr/Sonarr's NewznabRequestGenerator strategy:
//
//	Tier 1 (ID-based):  tmdbid, imdbid, tvdbid — sent first to NZBHydra2.
//	                    If any tier-1 query returns candidates the search
//	                    stops (title queries are NOT sent).  This is identical
//	                    to Radarr calling chain.Add() then chain.AddTier().
//	Tier 2 (title):     title+year variants — only used when ID queries return
//	                    nothing or when no IDs are available.
//
// The caller's search loop breaks as soon as a selectedRelease is chosen, so
// the tiers naturally prevent unnecessary Hydra calls.
func buildSearchRequests(input database.LibrarySearchInput) SearchRequestPlan {
	var tier1, tier2 []hydra.SearchRequest

	dedup := func(list *[]hydra.SearchRequest, req hydra.SearchRequest) {
		for _, ex := range *list {
			if sameSearchRequest(ex, req) {
				return
			}
		}
		// Also check cross-tier duplicates
		for _, ex := range tier1 {
			if sameSearchRequest(ex, req) {
				return
			}
		}
		*list = append(*list, req)
	}

	baseMovie := func(query string, tmdbID int64, imdbID string) hydra.SearchRequest {
		return hydra.SearchRequest{
			MediaType: input.MediaType,
			Query:     query,
			TMDBID:    tmdbID,
			IMDbID:    imdbID,
		}
	}

	baseTV := func(query string, tmdbID int64, imdbID string, tvdbID int64) hydra.SearchRequest {
		return hydra.SearchRequest{
			MediaType:     input.MediaType,
			Query:         query,
			TMDBID:        tmdbID,
			IMDbID:        imdbID,
			TVDBID:        tvdbID,
			SeasonNumber:  input.SeasonNumber,
			EpisodeNumber: input.EpisodeNumber,
		}
	}
	addTVTitleVariant := func(show string, season, episode int, showYear int, episodeTitle string) {
		show = strings.TrimSpace(show)
		if show == "" {
			return
		}
		add := func(query string) {
			query = strings.TrimSpace(query)
			if query == "" {
				return
			}
			dedup(&tier2, baseTV(query, 0, "", 0))
		}
		if season > 0 && episode > 0 {
			// Servarr-style fallback: keep q= focused on the series title and let
			// the structured season/episode parameters do the narrowing. This
			// avoids spraying Hydra with many near-duplicate text searches.
			add(show)
			if showYear > 0 {
				add(fmt.Sprintf("%s %d", show, showYear))
			}
			if strings.TrimSpace(episodeTitle) != "" {
				add(fmt.Sprintf("%s %s", show, strings.TrimSpace(episodeTitle)))
			}
			return
		}
		if season > 0 {
			add(fmt.Sprintf("%s S%02d", show, season))
			add(fmt.Sprintf("%s Season %d", show, season))
			if showYear > 0 {
				add(fmt.Sprintf("%s %d S%02d", show, showYear, season))
			}
			add(show)
			if showYear > 0 {
				add(fmt.Sprintf("%s %d", show, showYear))
			}
			return
		}
		add(show)
		if showYear > 0 {
			add(fmt.Sprintf("%s %d", show, showYear))
		}
	}

	switch strings.ToLower(input.MediaType) {
	case "movie":
		// Tier 1: IDs — send TMDB+IMDb together (Radarr's aggregated approach).
		// A separate IMDb-only query is redundant: the combined request already
		// includes the IMDb ID, so NZBHydra2 routes it to IMDb-capable indexers.
		if input.MovieTMDBID > 0 {
			r := baseMovie("", input.MovieTMDBID, input.IMDbID)
			dedup(&tier1, r)
		} else if input.IMDbID != "" {
			// Only fallback to IMDb-only when there is no TMDB ID at all.
			r := baseMovie("", 0, input.IMDbID)
			dedup(&tier1, r)
		}
		// Tier 2: title fallbacks
		if input.MovieYear > 0 {
			r := baseMovie(fmt.Sprintf("%s %d", input.Title, input.MovieYear), 0, "")
			dedup(&tier2, r)
		}
		r := baseMovie(input.Title, 0, "")
		dedup(&tier2, r)

	case "episode", "tv":
		show := input.ShowTitle
		if show == "" {
			show = input.Title
		}
		// Tier 1: IDs — send tvdbid+tmdbid+imdbid (Sonarr aggregates these)
		if input.ShowTMDBID > 0 || input.ShowTVDBID > 0 || input.ShowIMDbID != "" {
			r := baseTV("", input.ShowTMDBID, input.ShowIMDbID, input.ShowTVDBID)
			dedup(&tier1, r)
		}
		if isWholeShowRequest(input) {
			// Whole-show request: title fallbacks only
			addTVTitleVariant(show, input.SeasonNumber, input.EpisodeNumber, input.ShowYear, input.EpisodeTitle)
			break
		}
		// Tier 2: broader TV title variants. Old TV backlog often needs multiple
		// naming styles because indexers vary between SxxExx, 1x02, season, and
		// episode-title conventions.
		addTVTitleVariant(show, input.SeasonNumber, input.EpisodeNumber, input.ShowYear, input.EpisodeTitle)

	default:
		return SearchRequestPlan{Tier2: []hydra.SearchRequest{{
			MediaType: input.MediaType,
			Query:     strings.TrimSpace(input.Title),
			IMDbID:    input.IMDbID,
		}}}
	}

	if len(tier1) == 0 && len(tier2) == 0 {
		return SearchRequestPlan{Tier2: []hydra.SearchRequest{{
			MediaType: input.MediaType,
			Query:     strings.TrimSpace(input.Title),
		}}}
	}
	return SearchRequestPlan{Tier1: tier1, Tier2: tier2}
}

// SearchRequestPlan holds ID-based (tier 1) and title-based (tier 2) search
// requests.  Tier 1 is tried first; tier 2 is only used if tier 1 found no
// usable candidates (matching Radarr/Sonarr's IndexerPageableRequestChain tiers).
type SearchRequestPlan struct {
	Tier1 []hydra.SearchRequest
	Tier2 []hydra.SearchRequest
}

func sameSearchRequest(left, right hydra.SearchRequest) bool {
	return strings.EqualFold(left.Query, right.Query) &&
		strings.EqualFold(left.IMDbID, right.IMDbID) &&
		left.TMDBID == right.TMDBID &&
		left.TVDBID == right.TVDBID &&
		left.SeasonNumber == right.SeasonNumber &&
		left.EpisodeNumber == right.EpisodeNumber &&
		strings.EqualFold(left.MediaType, right.MediaType)
}

func isWholeShowRequest(input database.LibrarySearchInput) bool {
	return (strings.EqualFold(input.MediaType, "episode") || strings.EqualFold(input.MediaType, "tv")) &&
		input.SeasonNumber <= 0 &&
		input.EpisodeNumber <= 0
}

func searchRequestLabel(request hydra.SearchRequest) string {
	if strings.TrimSpace(request.Query) != "" {
		return request.Query
	}
	if strings.TrimSpace(request.IMDbID) != "" {
		return request.IMDbID
	}
	if request.TVDBID > 0 {
		return fmt.Sprintf("tvdb:%d", request.TVDBID)
	}
	return ""
}

func (s *Service) RetryQueueItem(ctx context.Context, queueItemID int64) (QueueRetryResult, error) {
	target, err := s.repo.GetQueueRetryTarget(ctx, queueItemID)
	if err != nil {
		return QueueRetryResult{}, err
	}
	if target.SelectedReleaseID != nil {
		summary, err := s.repo.GetSelectedReleaseSummary(ctx, *target.SelectedReleaseID)
		if err != nil {
			return QueueRetryResult{}, err
		}
		if summary.FailureCount > 0 {
			if alternative, err := s.repo.PromoteAlternativeRetryCandidate(ctx, target.LibraryItemID, summary.ReleaseCandidateID); err != nil {
				return QueueRetryResult{}, err
			} else if alternative != nil {
				selectedReleaseID, err := s.fetchAndImportSelectedRelease(ctx, alternative.SelectedReleaseID)
				if err != nil {
					return QueueRetryResult{}, err
				}
				return QueueRetryResult{
					QueueItemID:       queueItemID,
					Action:            "retried_alternative_candidate",
					SelectedReleaseID: selectedReleaseID,
				}, nil
			}
		}
		if strings.TrimSpace(summary.ExternalURL) == "" {
			selectedReleaseID, err := s.retrySelectedReleaseFromStoredNZB(ctx, summary, 0)
			if err != nil {
				return QueueRetryResult{}, err
			}
			return QueueRetryResult{
				QueueItemID:       queueItemID,
				Action:            "retried_stored_nzb",
				SelectedReleaseID: selectedReleaseID,
			}, nil
		}
		selectedReleaseID, err := s.fetchAndImportSelectedRelease(ctx, *target.SelectedReleaseID)
		if err != nil {
			return QueueRetryResult{}, err
		}
		return QueueRetryResult{
			QueueItemID:       queueItemID,
			Action:            "retried_selected_release",
			SelectedReleaseID: selectedReleaseID,
		}, nil
	}
	if existing, err := s.repo.PromoteBestRetryCandidate(ctx, target.LibraryItemID); err != nil {
		return QueueRetryResult{}, err
	} else if existing != nil {
		selectedReleaseID, err := s.fetchAndImportSelectedRelease(ctx, existing.SelectedReleaseID)
		if err != nil {
			return QueueRetryResult{}, err
		}
		return QueueRetryResult{
			QueueItemID:       queueItemID,
			Action:            "retried_existing_candidate",
			SelectedReleaseID: selectedReleaseID,
		}, nil
	}
	search, err := s.SearchLibrary(ctx, target.LibraryItemID)
	if err != nil {
		return QueueRetryResult{}, err
	}
	return QueueRetryResult{
		QueueItemID:        queueItemID,
		Action:             "researched_library_item",
		SelectedReleaseID:  search.SelectedReleaseID,
		SearchCandidateCnt: search.CandidateCount,
	}, nil
}

func (s *Service) ManageQueueItem(ctx context.Context, queueItemID int64, action string) (QueueManageResult, error) {
	target, err := s.repo.GetQueueRetryTarget(ctx, queueItemID)
	if err != nil {
		return QueueManageResult{}, err
	}
	settings := policy.DefaultSettings()
	if s.queuePolicy != nil {
		if loaded, loadErr := s.queuePolicy.Settings(ctx); loadErr == nil {
			settings = loaded
		}
	}
	switch strings.TrimSpace(action) {
	case string(policy.QueueActionRemove):
		if err := s.repo.ClearQueueSelectedRelease(ctx, queueItemID); err != nil {
			return QueueManageResult{}, err
		}
		return QueueManageResult{QueueItemID: queueItemID, Action: string(policy.QueueActionRemove)}, nil
	case string(policy.QueueActionRemoveAndBlocklist):
		if err := s.repo.BlocklistQueueSelectedRelease(ctx, queueItemID, "manual_reject", settings.BlocklistTTLDays); err != nil {
			return QueueManageResult{}, err
		}
		if err := s.repo.ClearQueueSelectedRelease(ctx, queueItemID); err != nil {
			return QueueManageResult{}, err
		}
		return QueueManageResult{QueueItemID: queueItemID, Action: string(policy.QueueActionRemoveAndBlocklist)}, nil
	case string(policy.QueueActionRemoveBlocklistAndSearch):
		if err := s.repo.BlocklistQueueSelectedRelease(ctx, queueItemID, "manual_reject", settings.BlocklistTTLDays); err != nil {
			return QueueManageResult{}, err
		}
		if err := s.repo.ClearQueueSelectedRelease(ctx, queueItemID); err != nil {
			return QueueManageResult{}, err
		}
		search, err := s.SearchLibrary(ctx, target.LibraryItemID)
		if err != nil {
			return QueueManageResult{}, err
		}
		return QueueManageResult{
			QueueItemID:        queueItemID,
			Action:             string(policy.QueueActionRemoveBlocklistAndSearch),
			SelectedReleaseID:  search.SelectedReleaseID,
			SearchCandidateCnt: search.CandidateCount,
		}, nil
	default:
		return QueueManageResult{}, fmt.Errorf("unsupported queue action: %q", action)
	}
}

func (s *Service) ManageFailedQueue(ctx context.Context, action string) (BulkQueueRetryResult, error) {
	targets, err := s.repo.ListFailedQueueRetryTargets(ctx, 0)
	if err != nil {
		return BulkQueueRetryResult{}, err
	}
	result := BulkQueueRetryResult{Processed: len(targets)}
	for _, target := range targets {
		result.ProcessedQueues = append(result.ProcessedQueues, target.QueueItemID)
		item, err := s.ManageQueueItem(ctx, target.QueueItemID, action)
		if err != nil {
			result.Failed++
			result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			continue
		}
		if item.Action == string(policy.QueueActionRemoveBlocklistAndSearch) {
			if item.SelectedReleaseID != nil || item.SearchCandidateCnt > 0 {
				result.Retried++
			}
			continue
		}
		result.Retried++
	}
	return result, nil
}

func (s *Service) ManageQueueItems(ctx context.Context, queueItemIDs []int64, action string) (BulkQueueRetryResult, error) {
	result := BulkQueueRetryResult{Processed: len(queueItemIDs)}
	for _, queueItemID := range queueItemIDs {
		result.ProcessedQueues = append(result.ProcessedQueues, queueItemID)
		item, err := s.ManageQueueItem(ctx, queueItemID, action)
		if err != nil {
			result.Failed++
			result.FailedQueues = append(result.FailedQueues, queueItemID)
			continue
		}
		if item.Action == string(policy.QueueActionRemoveBlocklistAndSearch) {
			if item.SelectedReleaseID != nil || item.SearchCandidateCnt > 0 {
				result.Retried++
			}
			continue
		}
		result.Retried++
	}
	return result, nil
}

type UpgradeSearchResult struct {
	Checked  int `json:"checked"`
	Upgraded int `json:"upgraded"`
	Failed   int `json:"failed"`
}

// SearchUpgrades re-searches library items whose quality profile has
// allow_upgrade=true and that are currently in state 'available'. If a
// higher-scored release is found it replaces the existing one.
func (s *Service) SearchUpgrades(ctx context.Context) (UpgradeSearchResult, error) {
	items, err := s.repo.ListUpgradableLibraryItems(ctx)
	if err != nil {
		return UpgradeSearchResult{}, err
	}
	result := UpgradeSearchResult{Checked: len(items)}
	for _, libraryItemID := range items {
		res, err := s.searchLibraryOnceWithMode(ctx, libraryItemID, true)
		if err != nil {
			s.logger.Warn().Int64("libraryItemId", libraryItemID).Err(err).Msg("upgrade search failed")
			result.Failed++
			continue
		}
		if res.SelectedReleaseID != nil {
			result.Upgraded++
		}
	}
	return result, nil
}

func parseCandidate(item hydra.SearchResult, history database.CandidateHistory, effectiveFailureCount int, degraded bool, indexerPolicies map[string]int) ranking.Candidate {
	titleLower := strings.ToLower(item.Title)
	policyScore := 0
	if indexerPolicies != nil {
		policyScore = indexerPolicies[item.Indexer]
	}
	return ranking.Candidate{
		Title:              item.Title,
		SizeBytes:          item.SizeBytes,
		Resolution:         detectOne(titleLower, "2160p", "1080p", "720p"),
		Source:             detectOne(titleLower, "web-dl", "webrip", "bluray", "remux", "hdtv", "cam", "camrip", "hdcam", "telesync", "telecine", "ts", "tc"),
		Codec:              detectOne(titleLower, "x265", "h265", "x264", "h264", "av1"),
		Language:           detectLanguage(titleLower),
		Indexer:            item.Indexer,
		ReleaseGroup:       detectReleaseGroup(item.Title),
		UploadedAt:         item.PublishedAt,
		FailureCount:       effectiveFailureCount,
		Degraded:           degraded,
		Grabs:              item.Grabs,
		IndexerScore:       item.IndexerScore,
		IndexerPolicyScore: policyScore,
	}
}

// SetDefaultProfileNames configures which quality profile names are used for
// movies and TV episodes. Call this once at startup from the app config.
func (s *Service) SetDefaultProfileNames(movie, tv string) {
	s.movieProfileName = movie
	s.tvProfileName = tv
}

// profilePreferencesForItem returns the ranking preferences for a specific
// library item, using its per-item profile override if set, falling back to
// the media-type default.
func (s *Service) profilePreferencesForItem(ctx context.Context, libraryItemID int64, mediaType string) ranking.Preferences {
	if libraryItemID > 0 {
		if p, err := s.repo.GetLibraryItemQualityProfile(ctx, libraryItemID); err == nil && p != nil {
			return ranking.Preferences{
				Resolutions:                     append([]string(nil), p.Resolutions...),
				Sources:                         append([]string(nil), p.Sources...),
				Codecs:                          append([]string(nil), p.Codecs...),
				Languages:                       append([]string(nil), p.Languages...),
				AudioFormats:                    append([]string(nil), p.AudioFormats...),
				HdrFormats:                      append([]string(nil), p.HdrFormats...),
				ExcludePatterns:                 append([]string(nil), p.ExcludePatterns...),
				PreferProper:                    p.PreferProper,
				PreferRepack:                    p.PreferRepack,
				RejectCam:                       p.RejectCam,
				MinimumUpgradeCustomFormatScore: p.MinimumUpgradeCustomFormatScore,
				MinMBPerMinute:                  p.MinMBPerMinute,
				MaxMBPerMinute:                  p.MaxMBPerMinute,
				TierMBPerMinuteLimits:           s.loadTierSizeLimits(ctx, mediaType),
				MinimumAgeHours:                 p.MinimumAgeHours,
				CutoffResolution:                p.CutoffResolution,
				CustomFormats:                   s.loadCustomFormats(ctx),
				BlockRules:                      s.loadBlockRules(ctx),
			}
		}
	}
	return s.defaultProfilePreferences(ctx, mediaType)
}

// defaultProfilePreferences returns the ranking preferences for the given
// media type ("movie" or "episode"). Results are cached for 5 minutes.
func (s *Service) defaultProfilePreferences(ctx context.Context, mediaType string) ranking.Preferences {
	if s == nil || s.repo == nil {
		return ranking.Preferences{}
	}
	const cacheTTL = 5 * time.Minute

	s.profileCacheMu.Lock()
	if s.profileCacheAt == nil {
		s.profileCacheAt = make(map[string]time.Time)
		s.profileCachedPrefs = make(map[string]ranking.Preferences)
	}
	if time.Since(s.profileCacheAt[mediaType]) < cacheTTL {
		prefs := s.profileCachedPrefs[mediaType]
		s.profileCacheMu.Unlock()
		return prefs
	}
	s.profileCacheMu.Unlock()

	// Pick the configured profile name for this media type.
	name := s.movieProfileName
	if mediaType == "episode" && s.tvProfileName != "" {
		name = s.tvProfileName
	}

	var profile database.QualityProfile
	var err error
	if name != "" {
		profile, err = s.repo.GetQualityProfileByName(ctx, name)
	}
	if err != nil || name == "" {
		profile, err = s.repo.GetDefaultQualityProfile(ctx)
	}
	if err != nil {
		return ranking.Preferences{}
	}

	prefs := ranking.Preferences{
		Resolutions:                     append([]string(nil), profile.Resolutions...),
		Sources:                         append([]string(nil), profile.Sources...),
		Codecs:                          append([]string(nil), profile.Codecs...),
		Languages:                       append([]string(nil), profile.Languages...),
		AudioFormats:                    append([]string(nil), profile.AudioFormats...),
		HdrFormats:                      append([]string(nil), profile.HdrFormats...),
		ExcludePatterns:                 append([]string(nil), profile.ExcludePatterns...),
		PreferProper:                    profile.PreferProper,
		PreferRepack:                    profile.PreferRepack,
		RejectCam:                       profile.RejectCam,
		MinimumUpgradeCustomFormatScore: profile.MinimumUpgradeCustomFormatScore,
		MinMBPerMinute:                  profile.MinMBPerMinute,
		MaxMBPerMinute:                  profile.MaxMBPerMinute,
		TierMBPerMinuteLimits:           s.loadTierSizeLimits(ctx, mediaType),
		MinimumAgeHours:                 profile.MinimumAgeHours,
		CutoffResolution:                profile.CutoffResolution,
		CustomFormats:                   s.loadCustomFormats(ctx),
		BlockRules:                      s.loadBlockRules(ctx),
	}

	s.profileCacheMu.Lock()
	s.profileCachedPrefs[mediaType] = prefs
	s.profileCacheAt[mediaType] = time.Now()
	s.profileCacheMu.Unlock()
	return prefs
}

// loadTierSizeLimits returns a map from resolution → [minMB, maxMB] built from
// the quality_definitions table. Results are cached for 5 minutes (O-04).
func (s *Service) loadTierSizeLimits(ctx context.Context, mediaType string) map[string][2]int {
	const cacheTTL = 5 * time.Minute
	s.profileCacheMu.Lock()
	if s.tierSizeCache != nil {
		if at, ok := s.tierSizeCacheAt[mediaType]; ok && time.Since(at) < cacheTTL {
			cached := s.tierSizeCache[mediaType]
			s.profileCacheMu.Unlock()
			return cached
		}
	}
	s.profileCacheMu.Unlock()

	defs, err := s.repo.ListQualityDefinitions(ctx)
	if err != nil || len(defs) == 0 {
		return nil
	}
	// Map DB quality keys (no dashes) to resolution strings used in ranking.
	keyToResolution := map[string]string{
		"bluray2160p": "2160p", "webdl2160p": "2160p", "webrip2160p": "2160p",
		"bluray1080p": "1080p", "webdl1080p": "1080p", "webrip1080p": "1080p", "hdtv1080p": "1080p",
		"bluray720p": "720p", "webdl720p": "720p", "webrip720p": "720p", "hdtv720p": "720p",
		"dvd": "576p", "sdtv": "480p",
	}
	out := make(map[string][2]int)
	for _, d := range defs {
		if d.MediaType != mediaType {
			continue
		}
		if d.MinMBPerMinute == 0 && d.MaxMBPerMinute == 0 {
			continue
		}
		res, ok := keyToResolution[d.QualityKey]
		if !ok {
			continue
		}
		existing := out[res]
		if d.MinMBPerMinute > 0 && (existing[0] == 0 || d.MinMBPerMinute < existing[0]) {
			existing[0] = d.MinMBPerMinute
		}
		if d.MaxMBPerMinute > 0 && d.MaxMBPerMinute > existing[1] {
			existing[1] = d.MaxMBPerMinute
		}
		out[res] = existing
	}
	var result map[string][2]int
	if len(out) > 0 {
		result = out
	}

	s.profileCacheMu.Lock()
	if s.tierSizeCache == nil {
		s.tierSizeCache = make(map[string]map[string][2]int)
		s.tierSizeCacheAt = make(map[string]time.Time)
	}
	s.tierSizeCache[mediaType] = result
	s.tierSizeCacheAt[mediaType] = time.Now()
	s.profileCacheMu.Unlock()

	return result
}

// loadCustomFormats fetches custom formats from the DB and converts them to
// ranking.CustomFormat values. Results are cached for 5 minutes (O-04).
func (s *Service) loadCustomFormats(ctx context.Context) []ranking.CustomFormat {
	if s == nil || s.repo == nil {
		return nil
	}
	const cacheTTL = 5 * time.Minute
	s.profileCacheMu.Lock()
	if s.customFormatsCache != nil && time.Since(s.customFormatsCacheAt) < cacheTTL {
		out := make([]ranking.CustomFormat, len(s.customFormatsCache))
		copy(out, s.customFormatsCache)
		s.profileCacheMu.Unlock()
		return out
	}
	s.profileCacheMu.Unlock()

	dbFormats, err := s.repo.ListCustomFormats(ctx)
	if err != nil || len(dbFormats) == 0 {
		return nil
	}
	out := make([]ranking.CustomFormat, len(dbFormats))
	for i, f := range dbFormats {
		out[i] = ranking.CustomFormat{
			Name:    f.Name,
			Pattern: f.Pattern,
			Score:   f.Score,
			Enabled: f.Enabled,
			Source:  f.Source,
		}
	}

	cached := make([]ranking.CustomFormat, len(out))
	copy(cached, out)
	s.profileCacheMu.Lock()
	s.customFormatsCache = cached
	s.customFormatsCacheAt = time.Now()
	s.profileCacheMu.Unlock()

	return out
}

// loadBlockRules fetches release block rules from the DB and converts them to
// ranking.BlockRule values. Results are cached for 5 minutes (O-04).
func (s *Service) loadBlockRules(ctx context.Context) []ranking.BlockRule {
	if s == nil || s.repo == nil {
		return nil
	}
	const cacheTTL = 5 * time.Minute
	s.profileCacheMu.Lock()
	if s.blockRulesCache != nil && time.Since(s.blockRulesCacheAt) < cacheTTL {
		out := make([]ranking.BlockRule, len(s.blockRulesCache))
		copy(out, s.blockRulesCache)
		s.profileCacheMu.Unlock()
		return out
	}
	s.profileCacheMu.Unlock()

	dbRules, err := s.repo.ListReleaseBlockRules(ctx)
	if err != nil || len(dbRules) == 0 {
		return nil
	}
	out := make([]ranking.BlockRule, len(dbRules))
	for i, r := range dbRules {
		out[i] = ranking.BlockRule{
			ID:           r.ID,
			Type:         r.Type,
			Pattern:      r.Pattern,
			MediaType:    r.MediaType,
			Action:       r.Action,
			ScorePenalty: r.ScorePenalty,
			Enabled:      r.Enabled,
			Source:       r.Source,
			Note:         r.Note,
		}
	}

	cached := make([]ranking.BlockRule, len(out))
	copy(cached, out)
	s.profileCacheMu.Lock()
	s.blockRulesCache = cached
	s.blockRulesCacheAt = time.Now()
	s.profileCacheMu.Unlock()

	return out
}

// loadIndexerPolicyMap fetches the enabled indexer policy score modifiers.
// Results are cached for 5 minutes (O-04). Returns nil on error.
func (s *Service) loadIndexerPolicyMap(ctx context.Context) map[string]int {
	if s == nil || s.repo == nil {
		return nil
	}
	const cacheTTL = 5 * time.Minute
	s.profileCacheMu.Lock()
	if s.indexerPolicyCache != nil && time.Since(s.indexerPolicyCacheAt) < cacheTTL {
		out := make(map[string]int, len(s.indexerPolicyCache))
		for k, v := range s.indexerPolicyCache {
			out[k] = v
		}
		s.profileCacheMu.Unlock()
		return out
	}
	s.profileCacheMu.Unlock()

	m, err := s.repo.LoadIndexerPolicyMap(ctx)
	if err != nil {
		return nil
	}

	cached := make(map[string]int, len(m))
	for k, v := range m {
		cached[k] = v
	}
	s.profileCacheMu.Lock()
	s.indexerPolicyCache = cached
	s.indexerPolicyCacheAt = time.Now()
	s.profileCacheMu.Unlock()

	return m
}

// ImportNZBFromPush imports an NZB file received via the SABnzbd-compatible API
// (pushed by Radarr/Sonarr). It bypasses the search/ranking pipeline and
// immediately starts the import → preflight → publish sequence asynchronously.
// Returns the nzo_id (e.g. "item-42") that Radarr/Sonarr use to poll status.
func (s *Service) ImportNZBFromPush(ctx context.Context, content []byte, filename, mediaType string) (string, error) {
	jobName := strings.TrimSuffix(filename, ".nzb")
	idempotencyKey := fmt.Sprintf("sabnzbd-push:%s", filename)
	imported, err := nzb.BuildImportedNZB(filename, content, idempotencyKey, "")
	if err != nil {
		return "", fmt.Errorf("parse nzb: %w", err)
	}
	imported.MediaType = mediaType

	select {
	case s.importSem <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	semReleased := false
	defer func() {
		if !semReleased {
			<-s.importSem
		}
	}()
	item, err := s.repo.CreateImportedNZB(ctx, imported)
	<-s.importSem
	semReleased = true
	if err != nil {
		return "", fmt.Errorf("create imported nzb: %w", err)
	}

	current, err := s.repo.GetSelectedReleaseSummary(ctx, *item.SelectedRelease)
	if err != nil {
		return "", fmt.Errorf("get release summary: %w", err)
	}
	_ = jobName

	go func() {
		bgCtx := context.Background()
		if err := s.repo.SetImportedNZBIndexed(bgCtx, item.QueueItemID); err != nil {
			s.logger.Error().Err(err).Int64("queueItemId", item.QueueItemID).Msg("sabnzbd: SetImportedNZBIndexed failed")
			return
		}
		item.State = database.QueuePreflight
		if s.preflightChecker != nil {
			if err := s.preflightChecker(bgCtx, item); err != nil {
				if shouldIgnorePreflightFailure(err) {
					s.logger.Info().Err(err).Int64("srId", current.SelectedReleaseID).Msg("sabnzbd preflight advisory only; continuing publish")
				} else {
					if _, promoteErr := s.promoteNextAfterFailure(bgCtx, current, err.Error()); promoteErr != nil {
						s.logger.Error().Err(promoteErr).Msg("sabnzbd: promoteNextAfterFailure failed (preflight)")
					}
					return
				}
			}
		}
		updated, lookupErr := s.repo.GetSelectedReleaseSummary(bgCtx, current.SelectedReleaseID)
		if lookupErr == nil && updated.VirtualFileCount == 0 {
			reason := strings.TrimSpace(updated.ArchiveRejects)
			if reason == "" {
				reason = "no_publishable_files"
			}
			if _, promoteErr := s.promoteNextAfterFailure(bgCtx, current, reason); promoteErr != nil {
				s.logger.Error().Err(promoteErr).Msg("sabnzbd: promoteNextAfterFailure failed (no files)")
			}
			return
		}
		if publisher, ok := s.repo.(interface {
			MarkQueueItemPublishing(context.Context, int64) error
		}); ok {
			if err := publisher.MarkQueueItemPublishing(bgCtx, item.QueueItemID); err != nil {
				if _, promoteErr := s.promoteNextAfterFailure(bgCtx, current, err.Error()); promoteErr != nil {
					s.logger.Error().Err(promoteErr).Msg("sabnzbd: promoteNextAfterFailure failed (publishing state)")
				}
				return
			}
			item.State = database.QueuePublishing
		}
		if s.postImportHook != nil {
			if err := s.postImportHook(bgCtx, item); err != nil {
				if _, promoteErr := s.promoteNextAfterFailure(bgCtx, current, err.Error()); promoteErr != nil {
					s.logger.Error().Err(promoteErr).Msg("sabnzbd: promoteNextAfterFailure failed (post-import)")
				}
			}
		}
	}()

	return fmt.Sprintf("item-%d", item.LibraryItemID), nil
}

func detectOne(title string, options ...string) string {
	for _, option := range options {
		if strings.Contains(title, option) {
			return option
		}
	}
	return ""
}

func detectLanguage(title string) string {
	switch {
	case strings.Contains(title, ".nl.") || strings.Contains(title, " dutch"):
		return "nl"
	case strings.Contains(title, " multi") || strings.Contains(title, ".multi."):
		return "multi"
	case strings.Contains(title, " german") || strings.Contains(title, ".ger.") || strings.Contains(title, " french") || strings.Contains(title, " truefrench") || strings.Contains(title, ".vostfr.") || strings.Contains(title, " spanish") || strings.Contains(title, " latino") || strings.Contains(title, " italian"):
		return "foreign"
	case strings.Contains(title, ".en.") || strings.Contains(title, " english"):
		return "en"
	default:
		return "unknown"
	}
}

func detectReleaseGroup(title string) string {
	if idx := strings.LastIndex(title, "-"); idx >= 0 && idx < len(title)-1 {
		return strings.TrimSpace(title[idx+1:])
	}
	return ""
}

type HTTPNZBFetcher struct {
	Client *http.Client
}

// nzbFetchInterval enforces a minimum gap between NZB downloads to prevent
// burst floods. Radarr/Sonarr cap indexer grabs at 2/sec; we use a simpler
// fixed interval of 2 seconds between any two NZB fetches.
var (
	nzbFetchMu       sync.Mutex
	nzbFetchLastTime time.Time
)

func (f HTTPNZBFetcher) Fetch(ctx context.Context, rawURL string) (string, []byte, error) {
	// Rate-limit NZB fetches globally: wait if less than 2 seconds since last fetch.
	nzbFetchMu.Lock()
	wait := time.Second - time.Since(nzbFetchLastTime) // was 2s; 1s is safe with 5-failure hard cap
	if wait > 0 {
		nzbFetchMu.Unlock()
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
		nzbFetchMu.Lock()
	}
	nzbFetchLastTime = time.Now()
	nzbFetchMu.Unlock()

	client := f.Client
	client = prepareNZBHTTPClient(client)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; Drakkar/1.0; +https://github.com/hjongedijk/drakkar)")
	req.Header.Set("Accept", "application/x-nzb,application/xml,text/xml,application/octet-stream,*/*;q=0.9")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		initialHost := req.URL.Host
		finalHost := ""
		if resp.Request != nil && resp.Request.URL != nil {
			finalHost = resp.Request.URL.Host
		}
		if resp.StatusCode == http.StatusForbidden && finalHost != "" && !strings.EqualFold(initialHost, finalHost) {
			return "", nil, fmt.Errorf("nzb fetch status 403 after redirect to indexer host %s; direct indexer download was forbidden", finalHost)
		}
		return "", nil, fmt.Errorf("nzb fetch status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}
	name := path.Base(req.URL.Path)
	if name == "" || name == "." || name == "/" {
		name = "selected.nzb"
	}
	return name, raw, nil
}

func prepareNZBHTTPClient(base *http.Client) *http.Client {
	if base == nil {
		base = &http.Client{Timeout: 60 * time.Second}
	} else {
		cloned := *base
		base = &cloned
	}
	if base.Jar == nil {
		if jar, err := cookiejar.New(nil); err == nil {
			base.Jar = jar
		}
	}
	prevRedirect := base.CheckRedirect
	base.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 0 {
			prev := via[len(via)-1]
			copyRedirectHeader(req, prev, "User-Agent")
			copyRedirectHeader(req, prev, "Accept")
			copyRedirectHeader(req, prev, "Accept-Language")
			copyRedirectHeader(req, prev, "Cache-Control")
			copyRedirectHeader(req, prev, "Pragma")
			if req.Header.Get("Referer") == "" {
				req.Header.Set("Referer", prev.URL.String())
			}
		}
		if prevRedirect != nil {
			return prevRedirect(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return base
}

func copyRedirectHeader(req, prev *http.Request, name string) {
	if req.Header.Get(name) != "" {
		return
	}
	if value := prev.Header.Get(name); value != "" {
		req.Header.Set(name, value)
	}
}

// BackfillMetadataResult summarises a metadata re-enrichment pass.
type BackfillMetadataResult struct {
	ProcessedMovies int `json:"processedMovies"`
	ProcessedShows  int `json:"processedShows"`
	Enriched        int `json:"enriched"`
	Failed          int `json:"failed"`
}

// BackfillMetadata re-fetches TMDB metadata for all movies and TV shows that
// already have a tmdb_id, filling newly-added columns (tagline, status,
// content_rating, release_date, trailer_url, etc.). Safe to call repeatedly.
func (s *Service) BackfillMetadata(ctx context.Context) (BackfillMetadataResult, error) {
	if s == nil || s.repo == nil || s.tmdb == nil || !s.tmdb.Enabled() {
		return BackfillMetadataResult{}, nil
	}
	targets, err := s.repo.ListMetadataBackfillTargets(ctx)
	if err != nil {
		return BackfillMetadataResult{}, err
	}

	var result BackfillMetadataResult
	seen := map[int64]bool{}
	for _, t := range targets { //nolint:govet
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
		if t.MediaType == "movie" {
			result.ProcessedMovies++
			if seen[t.TMDBID] {
				continue
			}
			seen[t.TMDBID] = true
			if err := s.enrichMovieRequest(ctx, t.LibraryItemID, t.TMDBID); err != nil {
				result.Failed++
			} else {
				result.Enriched++
			}
		} else {
			result.ProcessedShows++
			key := t.TVDBID*1_000_000 + t.TMDBID
			if seen[key] {
				continue
			}
			seen[key] = true
			if err := s.enrichEpisodeRequest(ctx, t.LibraryItemID, t.TMDBID, t.TVDBID, t.EpisodeTitle); err != nil {
				result.Failed++
			} else {
				result.Enriched++
			}
		}
	}
	return result, nil
}

// FillMissingEpisodesResult summarises how many episode items were created.
type FillMissingEpisodesResult struct {
	ShowsProcessed int `json:"showsProcessed"`
	EpisodesFound  int `json:"episodesFound"`
	ItemsCreated   int `json:"itemsCreated"`
	Queued         int `json:"queued"`
}

type PrioritizeTVShowResult struct {
	TVShowID      int64 `json:"tvShowId"`
	EpisodesFound int   `json:"episodesFound"`
	ItemsCreated  int   `json:"itemsCreated"`
	Queued        int   `json:"queued"`
}

type missingEpisodeBatchEnsurer interface {
	EnsureEpisodeLibraryItemsBatch(ctx context.Context, tvShowID int64, showTitle string, episodes []database.MissingEpisodeBatchInput) ([]int64, error)
}

// FillMissingEpisodes queries TMDB for the episode list of every TV show that
// has missing episodes, then creates library_item + queue_item rows for each
// episode not yet in the local database. To mirror Sonarr's episode-refresh
// behavior, only newly created episodes that aired recently are auto-searched.
func (s *Service) FillMissingEpisodes(ctx context.Context) (FillMissingEpisodesResult, error) {
	if s == nil || s.repo == nil || s.tmdb == nil || !s.tmdb.Enabled() {
		return FillMissingEpisodesResult{}, nil
	}
	// Find TV shows where we have fewer available episodes than the TMDB total.
	targets, err := s.repo.ListShowsWithMissingEpisodes(ctx)
	if err != nil {
		return FillMissingEpisodesResult{}, err
	}

	var result FillMissingEpisodesResult
	autoSearchIDs := make([]int64, 0)
	now := time.Now().UTC()
	batchRepo, hasBatchRepo := s.repo.(missingEpisodeBatchEnsurer)
	for _, show := range targets {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
		if show.TMDBID <= 0 {
			continue
		}
		result.ShowsProcessed++

		seasonNums, err := s.tmdb.TVSeasonNumbers(ctx, show.TMDBID)
		if err != nil {
			continue
		}
		for _, seasonNum := range seasonNums {
			season, err := s.tmdb.TVSeason(ctx, show.TMDBID, seasonNum)
			if err != nil {
				continue
			}

			batch := make([]database.MissingEpisodeBatchInput, 0, len(season.Episodes))
			for _, ep := range season.Episodes {
				if ep.EpisodeNumber <= 0 {
					continue
				}
				result.EpisodesFound++
				batch = append(batch, database.MissingEpisodeBatchInput{
					SeasonNumber:  seasonNum,
					EpisodeNumber: ep.EpisodeNumber,
					Title:         ep.Name,
					AirDate:       ep.AirDate,
				})
			}
			if len(batch) == 0 {
				continue
			}

			recentBatch := make([]database.MissingEpisodeBatchInput, 0, len(batch))
			olderBatch := make([]database.MissingEpisodeBatchInput, 0, len(batch))
			for _, ep := range batch {
				if shouldAutoSearchNewlyAddedEpisode(ep.AirDate, now) {
					recentBatch = append(recentBatch, ep)
				} else {
					olderBatch = append(olderBatch, ep)
				}
			}

			if hasBatchRepo {
				if len(recentBatch) > 0 {
					createdRecentIDs, err := batchRepo.EnsureEpisodeLibraryItemsBatch(ctx, show.TVShowID, show.ShowTitle, recentBatch)
					if err == nil {
						result.ItemsCreated += len(createdRecentIDs)
						autoSearchIDs = append(autoSearchIDs, createdRecentIDs...)
					}
				}
				if len(olderBatch) > 0 {
					createdOlderIDs, err := batchRepo.EnsureEpisodeLibraryItemsBatch(ctx, show.TVShowID, show.ShowTitle, olderBatch)
					if err == nil {
						result.ItemsCreated += len(createdOlderIDs)
					}
				}
				continue
			}

			for _, ep := range batch {
				created, err := s.repo.EnsureEpisodeLibraryItem(ctx, show.TVShowID, show.ShowTitle, ep.SeasonNumber, ep.EpisodeNumber, ep.Title, ep.AirDate)
				if err != nil || !created {
					continue
				}
				result.ItemsCreated++
			}
		}
	}
	if len(autoSearchIDs) > 0 {
		autoSearchIDs = dedupeInt64s(autoSearchIDs)
		s.PushLibraryItemsToQueue(autoSearchIDs, 10)
		result.Queued = len(autoSearchIDs)
	}
	return result, nil
}

func shouldAutoSearchNewlyAddedEpisode(airDate string, now time.Time) bool {
	airDate = strings.TrimSpace(airDate)
	if airDate == "" {
		return false
	}
	parsed, err := time.Parse("2006-01-02", airDate)
	if err != nil {
		return false
	}
	airDateUTC := parsed.UTC()
	return !airDateUTC.Before(now.AddDate(0, 0, -14)) &&
		!airDateUTC.After(now.AddDate(0, 0, 1))
}

func dedupeInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (s *Service) PrioritizeTVShowMissing(ctx context.Context, tvShowID int64) (PrioritizeTVShowResult, error) {
	if s == nil || s.repo == nil || s.tmdb == nil || !s.tmdb.Enabled() || tvShowID <= 0 {
		return PrioritizeTVShowResult{}, nil
	}
	show, err := s.repo.GetShowWithMissingEpisodes(ctx, tvShowID)
	if err != nil {
		return PrioritizeTVShowResult{}, err
	}
	if show == nil || show.TMDBID <= 0 {
		return PrioritizeTVShowResult{TVShowID: tvShowID}, nil
	}

	result := PrioritizeTVShowResult{TVShowID: tvShowID}
	seasonNums, err := s.tmdb.TVSeasonNumbers(ctx, show.TMDBID)
	if err != nil {
		return result, err
	}
	queuedSet := make(map[int64]struct{})
	for _, seasonNum := range seasonNums {
		season, err := s.tmdb.TVSeason(ctx, show.TMDBID, seasonNum)
		if err != nil {
			continue
		}
		batch := make([]database.MissingEpisodeBatchInput, 0, len(season.Episodes))
		for _, ep := range season.Episodes {
			if ep.EpisodeNumber <= 0 {
				continue
			}
			result.EpisodesFound++
			batch = append(batch, database.MissingEpisodeBatchInput{
				SeasonNumber:  seasonNum,
				EpisodeNumber: ep.EpisodeNumber,
				Title:         ep.Name,
				AirDate:       ep.AirDate,
			})
		}
		if len(batch) == 0 {
			continue
		}
		if batchRepo, ok := s.repo.(missingEpisodeBatchEnsurer); ok {
			createdIDs, err := batchRepo.EnsureEpisodeLibraryItemsBatch(ctx, show.TVShowID, show.ShowTitle, batch)
			if err != nil {
				continue
			}
			result.ItemsCreated += len(createdIDs)
			for _, id := range createdIDs {
				queuedSet[id] = struct{}{}
			}
			continue
		}
		for _, ep := range batch {
			created, err := s.repo.EnsureEpisodeLibraryItem(ctx, show.TVShowID, show.ShowTitle, ep.SeasonNumber, ep.EpisodeNumber, ep.Title, ep.AirDate)
			if err != nil || !created {
				continue
			}
			result.ItemsCreated++
		}
	}

	pendingIDs, err := s.repo.ListPendingTVShowLibraryItemIDs(ctx, tvShowID)
	if err != nil {
		return result, err
	}
	for _, id := range pendingIDs {
		queuedSet[id] = struct{}{}
	}
	if s.WorkQueue != nil {
		for id := range queuedSet {
			s.WorkQueue.Push(ctx, id, 10)
			result.Queued++
		}
		return result, nil
	}
	for id := range queuedSet {
		if _, err := s.SearchLibrary(ctx, id); err == nil {
			result.Queued++
		}
	}
	return result, nil
}

// ManualSearchItem is one result from a free-text Hydra search.
type ManualSearchItem struct {
	Title       string `json:"title"`
	ExternalURL string `json:"externalUrl"`
	Indexer     string `json:"indexer"`
	SizeBytes   int64  `json:"sizeBytes"`
	Score       int    `json:"score"`
	Resolution  string `json:"resolution,omitempty"`
	Source      string `json:"source,omitempty"`
	Codec       string `json:"codec,omitempty"`
	Audio       string `json:"audio,omitempty"`
	HDR         string `json:"hdr,omitempty"`
}

// ManualSearch queries NZBHydra2 with a free-text query and returns scored candidates.
func (s *Service) ManualSearch(ctx context.Context, query string) ([]ManualSearchItem, error) {
	if s == nil || s.hydra == nil || query == "" {
		return nil, nil
	}
	results, err := s.searchHydraWithRetry(ctx, hydra.SearchRequest{
		Query:     query,
		MediaType: "search",
	})
	if err != nil {
		return nil, err
	}
	out := make([]ManualSearchItem, 0, len(results))
	for _, r := range results {
		// hydra.SearchResult has limited fields — extract resolution/source/codec
		// from the title using the same parser the ranking engine uses.
		titleLower := strings.ToLower(r.Title)
		resolution := detectOne(titleLower, "2160p", "1080p", "720p", "576p", "480p")
		source := detectOne(titleLower, "bluray", "remux", "web-dl", "webrip", "hdtv", "dvdrip")
		codec := detectOne(titleLower, "x265", "hevc", "x264", "avc", "av1")
		audio := ranking.ParseAudioFormat(titleLower)
		hdr := ranking.ParseHDRFormat(titleLower)

		candidate := ranking.Candidate{
			Title:      r.Title,
			SizeBytes:  r.SizeBytes,
			Resolution: resolution,
			Source:     source,
			Codec:      codec,
			Indexer:    r.Indexer,
			UploadedAt: r.PublishedAt,
		}
		result := ranking.ScoreWithPreferences(candidate, ranking.Requirements{}, ranking.Preferences{
			CustomFormats: s.loadCustomFormats(ctx),
			BlockRules:    s.loadBlockRules(ctx),
		})
		out = append(out, ManualSearchItem{
			Title:       r.Title,
			ExternalURL: r.Link,
			Indexer:     r.Indexer,
			SizeBytes:   r.SizeBytes,
			Score:       result.Score,
			Resolution:  resolution,
			Source:      source,
			Codec:       codec,
			Audio:       audio,
			HDR:         hdr,
		})
	}
	return out, nil
}

// ResetLibraryItem removes the symlinks for a library item from the filesystem,
// deletes the associated NZB data, and resets the queue entry to 'requested' so
// the item re-enters the normal search cycle as if it were newly added.
func (s *Service) ResetLibraryItem(ctx context.Context, libraryItemID int64) error {
	paths, err := s.repo.DeleteSymlinkPublicationsForLibraryItem(ctx, libraryItemID)
	if err != nil {
		return err
	}
	for _, p := range paths {
		if removeErr := os.Remove(p); removeErr != nil && !os.IsNotExist(removeErr) {
			s.logger.Warn().Str("path", p).Err(removeErr).Msg("reset: could not remove symlink")
		}
	}
	return s.repo.ResetLibraryItemState(ctx, libraryItemID)
}

// FailAndBlocklistRelease blocklists the given selected release (so it is never
// re-selected for this library item) and promotes the next ranked candidate if
// one exists, or marks the item failed so the retry task can trigger a fresh
// search. Use this instead of ResetLibraryItem when the failure reason should
// be persisted — e.g. from the NZB health check when segments are confirmed
// missing on the NNTP server.
func (s *Service) FailAndBlocklistRelease(ctx context.Context, selectedReleaseID int64, reason string) error {
	next, err := s.repo.FailSelectedReleaseAndPromoteNext(ctx, selectedReleaseID, reason)
	if err != nil {
		return err
	}
	if next != nil && s.WorkQueue != nil {
		s.WorkQueue.Push(ctx, next.LibraryItemID, 10)
	}
	return nil
}

// ResetOrphanedAvailableItemsResult summarises a bulk orphan-reset pass.
type ResetOrphanedAvailableItemsResult struct {
	Found  int `json:"found"`
	Reset  int `json:"reset"`
	Failed int `json:"failed"`
}

// ResetOrphanedAvailableItems finds library items that are available=true but
// have no symlink and no recoverable virtual-file path, then resets each one
// back to 'requested' so it re-enters the normal search cycle.
func (s *Service) ResetOrphanedAvailableItems(ctx context.Context) (ResetOrphanedAvailableItemsResult, error) {
	ids, err := s.repo.ListUnrecoverableLibraryItems(ctx)
	if err != nil {
		return ResetOrphanedAvailableItemsResult{}, err
	}
	result := ResetOrphanedAvailableItemsResult{Found: len(ids)}
	for _, id := range ids {
		if err := s.ResetLibraryItem(ctx, id); err != nil {
			result.Failed++
			continue
		}
		result.Reset++
	}
	return result, nil
}
