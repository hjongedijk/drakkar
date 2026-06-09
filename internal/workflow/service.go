package workflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

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
	GetLibrarySearchInput(ctx context.Context, libraryItemID int64) (database.LibrarySearchInput, error)
	LookupCandidateHistory(ctx context.Context, libraryItemID int64) (map[string]database.CandidateHistory, error)
	ListPendingLibrarySearchTargets(ctx context.Context) ([]database.PendingLibrarySearchTarget, error)
	ListFailedQueueRetryTargets(ctx context.Context) ([]database.FailedQueueRetryTarget, error)
	ClearFailedQueueItems(ctx context.Context) (int, error)
	GetQueueRetryTarget(ctx context.Context, queueItemID int64) (database.QueueRetryTarget, error)
	BlocklistQueueSelectedRelease(ctx context.Context, queueItemID int64, reason string) error
	ClearQueueSelectedRelease(ctx context.Context, queueItemID int64) error
	ReplaceSearchCandidates(ctx context.Context, libraryItemID int64, candidates []database.SearchCandidateRecord) (*int64, error)
	MarkLibrarySearchFailed(ctx context.Context, libraryItemID int64, reason string) error
	GetSelectedReleaseSummary(ctx context.Context, selectedReleaseID int64) (database.ReleaseSummary, error)
	GetStoredNZBDocument(ctx context.Context, selectedReleaseID int64) (database.StoredNZBDocument, error)
	PromoteBestRetryCandidate(ctx context.Context, libraryItemID int64) (*database.ReleaseSummary, error)
	PromoteAlternativeRetryCandidate(ctx context.Context, libraryItemID int64, excludeReleaseCandidateID int64) (*database.ReleaseSummary, error)
	SelectReleaseCandidate(ctx context.Context, releaseCandidateID int64) (*database.ReleaseSummary, error)
	RejectReleaseCandidate(ctx context.Context, releaseCandidateID int64, reason string) (*database.ReleaseSummary, error)
	RestoreReleaseCandidate(ctx context.Context, releaseCandidateID int64) error
	RestoreRejectedReleaseCandidates(ctx context.Context, libraryItemID int64) (database.RejectedReleaseRestoreResult, error)
	SkipReleaseCandidate(ctx context.Context, releaseCandidateID int64) (*database.ReleaseSummary, error)
	MarkSelectedReleaseFetching(ctx context.Context, selectedReleaseID int64) error
	ImportSelectedReleaseNZB(ctx context.Context, selectedReleaseID int64, imported database.ImportedNZB) (database.QueueSnapshot, error)
	SetImportedNZBIndexed(ctx context.Context, queueItemID int64) error
	FailSelectedReleaseAndPromoteNext(ctx context.Context, selectedReleaseID int64, reason string) (*database.ReleaseSummary, error)
	ShouldAttemptSeasonPack(ctx context.Context, tvShowID int64, season int) (bool, error)
	RecordSeasonPackAttempt(ctx context.Context, tvShowID int64, season int, outcome string) error
	ListMetadataBackfillTargets(ctx context.Context) ([]database.MetadataBackfillTarget, error)
	ListShowsWithMissingEpisodes(ctx context.Context) ([]database.ShowWithMissingEpisodes, error)
	EnsureEpisodeLibraryItem(ctx context.Context, tvShowID int64, showTitle string, seasonNum, episodeNum int, episodeTitle, airDate string) (created bool, err error)
}

type SeerrClient interface {
	PendingRequests(ctx context.Context) ([]seerr.Request, error)
}

type HydraClient interface {
	Search(ctx context.Context, request hydra.SearchRequest) ([]hydra.SearchResult, error)
	SearchRecent(ctx context.Context, mediaType string) ([]hydra.SearchResult, error)
}

type Service struct {
	repo           Repository
	seerr          SeerrClient
	hydra          HydraClient
	tmdb           TMDBClient
	tvdb           TVDBClient
	fetcher        NZBFetcher
	postImportHook   func(context.Context, database.QueueSnapshot) error
	preflightChecker func(context.Context, database.QueueSnapshot) error
	queuePolicy      QueuePolicyProvider
	// WorkQueue accepts individual library item IDs for immediate dispatch.
	// Push items here from webhooks or sync to bypass the 30-min tick.
	WorkQueue *WorkQueue

	// profileCacheMu guards the cached quality profile.
	profileCacheMu  sync.Mutex
	profileCacheAt  time.Time
	profileCachedPrefs ranking.Preferences

	// importSem limits fetchAndImportSelectedRelease to 1 concurrent execution.
	// Like nzbdav's SemaphoreSlim(1,1): parallel searching is fine, but only one
	// NZB may go through preflight + publish at a time to avoid exhausting the
	// NNTP connection pool.
	importSem chan struct{}
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
	Seen    int `json:"seen"`
	Created int `json:"created"`
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

const pendingQueueBatchSize = 200 // process up to 200 items per scheduler tick (was 50)

func NewService(repo Repository, seerr SeerrClient, hydra HydraClient) *Service {
	return &Service{
		repo:      repo,
		seerr:     seerr,
		hydra:     hydra,
		fetcher:   HTTPNZBFetcher{},
		WorkQueue: NewWorkQueue(1), // 1 worker = 1 active item at a time (nzbdav parity)
		importSem: make(chan struct{}, 1), // 1 concurrent import/preflight/publish (nzbdav parity)
	}
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

func (s *Service) SetQueuePolicyProvider(provider QueuePolicyProvider) {
	s.queuePolicy = provider
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
				lid, tmdbID := libraryItemID, request.TMDBID
				go func() {
					enrichCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					_ = s.enrichMovieRequest(enrichCtx, lid, tmdbID)
				}()
			} else {
				// Still enrich existing items (noop if already complete).
				_ = s.enrichMovieRequest(ctx, libraryItemID, request.TMDBID)
			}
		case "tv":
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
				lid, tmdbID, tvdbID, epTitle := libraryItemID, request.TMDBID, request.TVDBID, request.EpisodeTitle
				go func() {
					enrichCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					_ = s.enrichEpisodeRequest(enrichCtx, lid, tmdbID, tvdbID, epTitle)
				}()
			} else {
				_ = s.enrichEpisodeRequest(ctx, libraryItemID, request.TMDBID, request.TVDBID, request.EpisodeTitle)
			}
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
// work queue with the given priority instead of searching synchronously.
// Called from the webhook handler so newly approved requests are processed
// immediately with high priority.
func (s *Service) PushPendingToQueue(priority int) {
	if s.WorkQueue == nil {
		return
	}
	ctx := context.Background()
	targets, err := s.repo.ListPendingLibrarySearchTargets(ctx)
	if err != nil {
		return
	}
	for _, target := range targets {
		s.WorkQueue.Push(target.LibraryItemID, priority)
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
	result := BulkSearchResult{Processed: len(targets)}
	for _, target := range targets {
		result.ProcessedItems = append(result.ProcessedItems, target.LibraryItemID)
		// If the work queue is running, push items for concurrent processing
		// rather than searching sequentially. Limit one bulk dispatch batch so
		// a giant import does not hammer Hydra all at once.
		if s.WorkQueue != nil {
			// Push ALL pending items to WorkQueue — no artificial cap.
			// WorkQueue workers process at the natural Hydra throttle rate (500ms).
			// Matches Radarr's "Search All Missing" which queries all missing items
			// in parallel without a hard batch limit.
			s.WorkQueue.Push(target.LibraryItemID, 0)
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
	profilePrefs := s.defaultProfilePreferences(ctx)
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
		candidates := buildSearchCandidates(recent, searchRequirements(input), history, profilePrefs)
		if len(candidates) == 0 {
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
	targets, err := s.repo.ListFailedQueueRetryTargets(ctx)
	if err != nil {
		return BulkQueueRetryResult{}, err
	}
	result := BulkQueueRetryResult{Processed: len(targets)}
	for _, target := range targets {
		result.ProcessedQueues = append(result.ProcessedQueues, target.QueueItemID)

		// Use policy engine to decide the recovery action for this failure.
		action := policy.DecideFromReason(target.FailureReason)
		switch action {
		case policy.ActionBlocklistAndSearch:
			// Blocklist the current release and trigger a fresh search.
			if sr, err := s.SearchLibrary(ctx, target.LibraryItemID); err == nil && sr.SelectedReleaseID != nil {
				result.Retried++
			} else {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			}
			continue
		case policy.ActionDoNothing:
			// Hard failures (no releases, wrong title) — don't retry until
			// the next sync brings fresh candidates.
			result.Failed++
			result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			continue
		default:
			// ActionSearchAgain, ActionRetryLater → standard retry flow.
		}

		if _, err := s.RetryQueueItem(ctx, target.QueueItemID); err != nil {
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
	return err != nil && strings.Contains(err.Error(), "40P01")
}

func (s *Service) SearchLibrary(ctx context.Context, libraryItemID int64) (SearchResult, error) {
	const maxDeadlockRetries = 3
	for attempt := 0; attempt < maxDeadlockRetries; attempt++ {
		result, err := s.searchLibraryOnce(ctx, libraryItemID)
		if isDeadlock(err) {
			time.Sleep(time.Duration(50+attempt*50) * time.Millisecond)
			continue
		}
		return result, err
	}
	return SearchResult{}, fmt.Errorf("searchLibrary: too many deadlock retries for item %d", libraryItemID)
}

func (s *Service) searchLibraryOnce(ctx context.Context, libraryItemID int64) (SearchResult, error) {
	if s == nil || s.hydra == nil {
		return SearchResult{}, fmt.Errorf("nzbhydra2 client unavailable")
	}
	if reason, err := s.repo.DetectMovieSearchConflict(ctx, libraryItemID); err != nil {
		if strings.Contains(err.Error(), "no rows") {
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
		if strings.Contains(err.Error(), "no rows") {
			return SearchResult{LibraryItemID: libraryItemID}, nil // item deleted between queue push and processing
		}
		return SearchResult{}, err
	}
	history, err := s.repo.LookupCandidateHistory(ctx, libraryItemID)
	if err != nil {
		return SearchResult{}, err
	}
	profilePrefs := s.defaultProfilePreferences(ctx)

	// For TV episodes, try the full season pack first if the rate limit allows.
	// A season pack covers all episodes in the season and avoids many separate downloads.
	if isEpisodeSearch(input) && input.TVShowID > 0 && input.SeasonNumber > 0 {
		if ok, _ := s.repo.ShouldAttemptSeasonPack(ctx, input.TVShowID, input.SeasonNumber); ok {
			packResult, packSelected, packErr := s.trySeasonPack(ctx, input, history, libraryItemID)
			outcome := database.SeasonPackOutcomeFailed
			if packSelected != nil {
				outcome = database.SeasonPackOutcomeSelected
			}
			_ = s.repo.RecordSeasonPackAttempt(ctx, input.TVShowID, input.SeasonNumber, outcome)
			if packSelected != nil || packErr != nil {
				return packResult, packErr
			}
			// Pack found nothing usable — fall through to individual episode search.
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

	// searchTier runs all requests in a tier and returns true if the caller
	// should stop (selected a release or found good candidates).
	searchTier := func(tierRequests []hydra.SearchRequest) bool {
		for _, candidateRequest := range tierRequests {
			query = searchRequestLabel(candidateRequest)
			results, err = s.searchHydraWithRetry(ctx, candidateRequest)
			if err != nil {
				lastSearchErr = err
				continue
			}
			lastSearchErr = nil
			candidates = buildSearchCandidates(results, searchRequirements(input), history, profilePrefs)
			combinedCandidates = mergeSearchCandidates(combinedCandidates, candidates)
			selectedReleaseID, err = s.repo.ReplaceSearchCandidates(ctx, libraryItemID, combinedCandidates)
			if err != nil {
				return true // propagate error via outer err variable
			}
			if selectedReleaseID != nil && !shouldContinueSearch(combinedCandidates, input) {
				return true
			}
		}
		return false
	}

	// Tier 1: ID-based queries (tmdbid / imdbid / tvdbid).
	// If these return a usable candidate we skip title queries entirely —
	// same logic as Radarr/Sonarr's IndexerPageableRequestChain.AddTier().
	tier1Done := searchTier(plan.Tier1)
	if err != nil {
		return SearchResult{}, err
	}

	// Tier 2: title-based fallback — only run if tier 1 produced no candidates.
	if !tier1Done && selectedReleaseID == nil && len(combinedCandidates) == 0 {
		searchTier(plan.Tier2)
		if err != nil {
			return SearchResult{}, err
		}
	}
	if selectedReleaseID == nil && len(combinedCandidates) == 0 && lastSearchErr != nil {
		if markErr := s.repo.MarkLibrarySearchFailed(ctx, libraryItemID, classifySearchFailureReason(lastSearchErr)); markErr != nil {
			return SearchResult{}, markErr
		}
		return SearchResult{}, lastSearchErr
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
// Returns (result, selectedID, err). When selectedID is nil and err is nil,
// no usable pack was found and the caller should fall back to episode search.
func (s *Service) trySeasonPack(ctx context.Context, input database.LibrarySearchInput, history map[string]database.CandidateHistory, libraryItemID int64) (SearchResult, *int64, error) {
	packInput := input
	packInput.EpisodeNumber = 0 // season pack: no episode

	packRequests := buildSeasonPackRequests(packInput)
	var (
		combinedCandidates []database.SearchCandidateRecord
		selectedReleaseID  *int64
	)
	profilePrefs := s.defaultProfilePreferences(ctx)
	for _, req := range packRequests {
		results, err := s.searchHydraWithRetry(ctx, req)
		if err != nil || len(results) == 0 {
			continue
		}
		candidates := buildSearchCandidates(results, searchRequirements(packInput), history, profilePrefs)
		combinedCandidates = mergeSearchCandidates(combinedCandidates, candidates)
		selectedReleaseID, err = s.repo.ReplaceSearchCandidates(ctx, libraryItemID, combinedCandidates)
		if err != nil {
			return SearchResult{}, nil, err
		}
		if selectedReleaseID != nil {
			break
		}
	}
	if selectedReleaseID == nil {
		return SearchResult{}, nil, nil
	}
	final, err := s.fetchAndImportSelectedRelease(ctx, *selectedReleaseID)
	if err != nil {
		return SearchResult{}, nil, err
	}
	return SearchResult{
		LibraryItemID:     libraryItemID,
		CandidateCount:    len(combinedCandidates),
		SelectedReleaseID: final,
	}, final, nil
}

// buildSeasonPackRequests produces Hydra queries for a full season (no episode number).
func buildSeasonPackRequests(input database.LibrarySearchInput) []hydra.SearchRequest {
	show := input.ShowTitle
	if show == "" {
		show = input.Title
	}
	var requests []hydra.SearchRequest
	add := func(q string) {
		q = strings.TrimSpace(q)
		if q == "" {
			return
		}
		req := hydra.SearchRequest{
			MediaType:    input.MediaType,
			Query:        q,
			IMDbID:       input.ShowIMDbID,
			TVDBID:       input.ShowTVDBID,
			SeasonNumber: input.SeasonNumber,
			// EpisodeNumber intentionally 0 = season pack
		}
		if strings.EqualFold(q, req.IMDbID) {
			req.Query = ""
		}
		for _, ex := range requests {
			if sameSearchRequest(ex, req) {
				return
			}
		}
		requests = append(requests, req)
	}
	add(input.ShowIMDbID)
	if input.SeasonNumber > 0 {
		add(fmt.Sprintf("%s S%02d", show, input.SeasonNumber))
		if input.ShowYear > 0 {
			add(fmt.Sprintf("%s %d S%02d", show, input.ShowYear, input.SeasonNumber))
		}
	}
	add(show)
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
		strings.Contains(message, "connection refused"),
		strings.Contains(message, "no such host"),
		strings.Contains(message, "server misbehaving"):
		return "search_unavailable"
	default:
		return "search_error"
	}
}

func isRetryableSearchFailure(err error) bool {
	switch classifySearchFailureReason(err) {
	case "search_timeout", "search_rate_limited", "search_unavailable":
		return true
	default:
		return false
	}
}

func (s *Service) fetchAndImportSelectedRelease(ctx context.Context, selectedReleaseID int64) (*int64, error) {
	// Semaphore: 1 NZB fetch+index at a time to avoid DB write contention.
	// Released before publish so season packs don't block the next fetch.
	select {
	case s.importSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	result, importedRelease, err := s.fetchIndexAndRelease(ctx, selectedReleaseID)
	<-s.importSem // release before publish — next NZB can be fetched while this one publishes
	if err != nil || importedRelease == nil {
		return result, err
	}
	return s.publishImportedRelease(ctx, *importedRelease)
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
	imported, err := nzb.BuildImportedNZB(fileName, raw, fmt.Sprintf("selected-release:%d", current.SelectedReleaseID), current.ExternalURL)
	if err != nil {
		result, err := s.promoteNextAfterFailureDepth(ctx, current, err.Error(), 0)
		return result, nil, err
	}
	item, err := s.repo.ImportSelectedReleaseNZB(ctx, current.SelectedReleaseID, imported)
	if err != nil {
		result, err := s.promoteNextAfterFailure(ctx, current, err.Error())
		return result, nil, err
	}
	if err := s.repo.SetImportedNZBIndexed(ctx, item.QueueItemID); err != nil {
		result, err := s.promoteNextAfterFailure(ctx, current, err.Error())
		return result, nil, err
	}
	item.State = database.QueuePreflight
	return nil, &pendingPublish{current: current, item: item}, nil
}

// publishImportedRelease runs postImportHook (symlinks, episode items, Plex)
// without holding the import semaphore so other fetches can proceed in parallel.
func (s *Service) publishImportedRelease(ctx context.Context, p pendingPublish) (*int64, error) {
	if p.current.NZBDocumentID != nil && p.item.QueueItemID == 0 {
		// Already indexed in a previous run — re-import from stored NZB.
		return s.retrySelectedReleaseFromStoredNZB(ctx, p.current)
	}
	updated, err := s.repo.GetSelectedReleaseSummary(ctx, p.current.SelectedReleaseID)
	if err == nil && updated.VirtualFileCount == 0 && strings.TrimSpace(updated.ArchiveRejects) != "" {
		return s.promoteNextAfterFailure(ctx, p.current, updated.ArchiveRejects)
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
		return s.retrySelectedReleaseFromStoredNZB(ctx, current)
	}
	for {
		if err := s.repo.MarkSelectedReleaseFetching(ctx, current.SelectedReleaseID); err != nil {
			return nil, err
		}
		fileName, raw, err := s.fetcher.Fetch(ctx, current.ExternalURL)
		if err != nil {
			return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
		}
		imported, err := nzb.BuildImportedNZB(fileName, raw, fmt.Sprintf("selected-release:%d", current.SelectedReleaseID), current.ExternalURL)
		if err != nil {
			return s.promoteNextAfterFailureDepth(ctx, current, err.Error(), depth)
		}
		return s.importSelectedRelease(ctx, current, imported)
	}
}

func buildSearchCandidates(results []hydra.SearchResult, required ranking.Requirements, history map[string]database.CandidateHistory, prefs ranking.Preferences) []database.SearchCandidateRecord {
	candidates := make([]database.SearchCandidateRecord, 0, len(results))
	for _, result := range results {
		known := history[strings.TrimSpace(result.Link)]
		score := ranking.ScoreWithPreferences(parseCandidate(result, known), required, prefs)
		candidates = append(candidates, database.SearchCandidateRecord{
			Title:             result.Title,
			ExternalURL:       result.Link,
			IndexerName:       result.Indexer,
			SizeBytes:         result.SizeBytes,
			PostedAt:          result.PublishedAt,
			Score:             score.Score,
			Rejected:          score.Rejected,
			RejectReason:      score.RejectReason,
			FailureCount:      known.FailureCount,
			LastFailureReason: known.LastFailureReason,
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

func searchRequirements(input database.LibrarySearchInput) ranking.Requirements {
	mediaType := input.MediaType
	if isWholeShowRequest(input) {
		mediaType = "tv"
	}
	required := ranking.Requirements{
		MediaType:     mediaType,
		Year:          input.MovieYear,
		SeasonNumber:  input.SeasonNumber,
		EpisodeNumber: input.EpisodeNumber,
		Title:         input.Title,
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
			return true
		}
		return candidate.FailureCount > 0
	}
	return true
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

func (s *Service) importSelectedRelease(ctx context.Context, current database.ReleaseSummary, imported database.ImportedNZB) (*int64, error) {
	item, err := s.repo.ImportSelectedReleaseNZB(ctx, current.SelectedReleaseID, imported)
	if err != nil {
		return s.promoteNextAfterFailure(ctx, current, err.Error())
	}
	if err := s.repo.SetImportedNZBIndexed(ctx, item.QueueItemID); err != nil {
		return s.promoteNextAfterFailure(ctx, current, err.Error())
	}
	item.State = database.QueuePreflight
	// Preflight: verify first segments are reachable on NNTP before publishing.
	// Mirrors nzbdav's FetchFirstSegmentsStep — catches expired/incomplete NZBs
	// early and falls back to the next search candidate instead of publishing dead content.
	if s.preflightChecker != nil {
		if err := s.preflightChecker(ctx, item); err != nil {
			return s.promoteNextAfterFailure(ctx, current, err.Error())
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
		return s.promoteNextAfterFailure(ctx, current, reason)
	}
	if s.postImportHook != nil {
		if err := s.postImportHook(ctx, item); err != nil {
			failureReason := err.Error()
			if errors.Is(err, library.ErrNoVirtualFiles) {
				if updated2, lookupErr := s.repo.GetSelectedReleaseSummary(ctx, current.SelectedReleaseID); lookupErr == nil && strings.TrimSpace(updated2.ArchiveRejects) != "" {
					failureReason = updated2.ArchiveRejects
				}
			}
			return s.promoteNextAfterFailure(ctx, current, failureReason)
		}
	}
	value := current.SelectedReleaseID
	return &value, nil
}

func (s *Service) promoteNextAfterFailure(ctx context.Context, current database.ReleaseSummary, reason string) (*int64, error) {
	return s.promoteNextAfterFailureDepth(ctx, current, reason, 0)
}

// promoteNextAfterFailureDepth adds a depth counter to prevent infinite recursive
// promotion chains (e.g. all candidates fail with 403 from the indexer).
// Radarr/Sonarr never recurse here — they let the scheduler re-try later.
// We cap at 5 hops so we can try a few alternatives without stack-overflowing.
func (s *Service) promoteNextAfterFailureDepth(ctx context.Context, current database.ReleaseSummary, reason string, depth int) (*int64, error) {
	if depth >= 5 {
		// Safety valve: stop recursing and leave the item failed.
		// The next scheduler cycle will pick it up with a fresh search.
		return nil, nil
	}
	metrics.M.FallbackReleaseAttempts.Add(1)
	next, promoteErr := s.repo.FailSelectedReleaseAndPromoteNext(ctx, current.SelectedReleaseID, reason)
	if promoteErr != nil {
		return nil, promoteErr
	}
	if next == nil {
		return nil, nil
	}
	if next.FailureCount >= 5 {
		// This candidate has already failed many times — skip it and promote again.
		return s.promoteNextAfterFailureDepth(ctx, *next, "too_many_failures", depth+1)
	}
	if strings.TrimSpace(next.ExternalURL) == "" {
		return s.retrySelectedReleaseFromStoredNZB(ctx, *next)
	}
	// Recursively try the next candidate, but track depth to prevent stack overflow.
	result, err := s.fetchAndImportSelectedReleaseDepth(ctx, next.SelectedReleaseID, depth+1)
	return result, err
}

func (s *Service) retrySelectedReleaseFromStoredNZB(ctx context.Context, current database.ReleaseSummary) (*int64, error) {
	if err := s.repo.MarkSelectedReleaseFetching(ctx, current.SelectedReleaseID); err != nil {
		return nil, err
	}
	doc, err := s.repo.GetStoredNZBDocument(ctx, current.SelectedReleaseID)
	if err != nil {
		return s.promoteNextAfterFailure(ctx, current, err.Error())
	}
	imported, err := nzb.BuildImportedNZB(doc.FileName, doc.XML, fmt.Sprintf("selected-release:%d:stored", current.SelectedReleaseID), doc.ExternalURL)
	if err != nil {
		return s.promoteNextAfterFailure(ctx, current, err.Error())
	}
	return s.importSelectedRelease(ctx, current, imported)
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
//   Tier 1 (ID-based):  tmdbid, imdbid, tvdbid — sent first to NZBHydra2.
//                       If any tier-1 query returns candidates the search
//                       stops (title queries are NOT sent).  This is identical
//                       to Radarr calling chain.Add() then chain.AddTier().
//   Tier 2 (title):     title+year variants — only used when ID queries return
//                       nothing or when no IDs are available.
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

	switch strings.ToLower(input.MediaType) {
	case "movie":
		// Tier 1: IDs (Radarr sends tmdbid first, then imdbid — both aggregated if supported)
		if input.MovieTMDBID > 0 {
			r := baseMovie("", input.MovieTMDBID, input.IMDbID)
			dedup(&tier1, r)
		}
		if input.IMDbID != "" {
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
			if input.ShowYear > 0 {
				dedup(&tier2, baseTV(fmt.Sprintf("%s %d", show, input.ShowYear), 0, "", 0))
			}
			dedup(&tier2, baseTV(show, 0, "", 0))
			break
		}
		// Tier 2: title-based episode variants
		if input.SeasonNumber > 0 && input.EpisodeNumber > 0 {
			dedup(&tier2, baseTV(fmt.Sprintf("%s S%02dE%02d", show, input.SeasonNumber, input.EpisodeNumber), 0, "", 0))
			dedup(&tier2, baseTV(fmt.Sprintf("%s %dx%02d", show, input.SeasonNumber, input.EpisodeNumber), 0, "", 0))
			if input.ShowYear > 0 {
				dedup(&tier2, baseTV(fmt.Sprintf("%s %d S%02dE%02d", show, input.ShowYear, input.SeasonNumber, input.EpisodeNumber), 0, "", 0))
			}
			if strings.TrimSpace(input.EpisodeTitle) != "" {
				dedup(&tier2, baseTV(fmt.Sprintf("%s %s", show, input.EpisodeTitle), 0, "", 0))
			}
		}
		if input.SeasonNumber > 0 {
			dedup(&tier2, baseTV(fmt.Sprintf("%s S%02d", show, input.SeasonNumber), 0, "", 0))
			if input.ShowYear > 0 {
				dedup(&tier2, baseTV(fmt.Sprintf("%s %d S%02d", show, input.ShowYear, input.SeasonNumber), 0, "", 0))
			}
		}
		dedup(&tier2, baseTV(show, 0, "", 0))

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
			selectedReleaseID, err := s.retrySelectedReleaseFromStoredNZB(ctx, summary)
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

func (s *Service) AutoManageFailedQueue(ctx context.Context) (BulkQueueRetryResult, error) {
	targets, err := s.repo.ListFailedQueueRetryTargets(ctx)
	if err != nil {
		return BulkQueueRetryResult{}, err
	}
	settings := policy.DefaultSettings()
	if s.queuePolicy != nil {
		if loaded, loadErr := s.queuePolicy.Settings(ctx); loadErr == nil {
			settings = loaded
		}
	}
	result := BulkQueueRetryResult{Processed: len(targets)}
	for _, target := range targets {
		result.ProcessedQueues = append(result.ProcessedQueues, target.QueueItemID)
		action := policy.ActionForReason(settings, target.FailureReason)
		switch action {
		case policy.QueueActionSearchAgain:
			if _, err := s.SearchLibrary(ctx, target.LibraryItemID); err != nil {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			} else {
				result.Retried++
			}
		case policy.QueueActionRemove:
			if err := s.repo.ClearQueueSelectedRelease(ctx, target.QueueItemID); err != nil {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			}
		case policy.QueueActionRemoveAndBlocklist:
			if err := s.repo.BlocklistQueueSelectedRelease(ctx, target.QueueItemID, target.FailureReason); err != nil {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
				continue
			}
			if err := s.repo.ClearQueueSelectedRelease(ctx, target.QueueItemID); err != nil {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			}
		case policy.QueueActionRemoveBlocklistAndSearch:
			if err := s.repo.BlocklistQueueSelectedRelease(ctx, target.QueueItemID, target.FailureReason); err != nil {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
				continue
			}
			if err := s.repo.ClearQueueSelectedRelease(ctx, target.QueueItemID); err != nil {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
				continue
			}
			if _, err := s.SearchLibrary(ctx, target.LibraryItemID); err != nil {
				result.Failed++
				result.FailedQueues = append(result.FailedQueues, target.QueueItemID)
			} else {
				result.Retried++
			}
		default:
			continue
		}
	}
	return result, nil
}

func parseCandidate(item hydra.SearchResult, history database.CandidateHistory) ranking.Candidate {
	titleLower := strings.ToLower(item.Title)
	return ranking.Candidate{
		Title:        item.Title,
		SizeBytes:    item.SizeBytes,
		Resolution:   detectOne(titleLower, "2160p", "1080p", "720p"),
		Source:       detectOne(titleLower, "web-dl", "webrip", "bluray", "remux", "hdtv", "cam", "camrip", "hdcam", "telesync", "telecine", "ts", "tc"),
		Codec:        detectOne(titleLower, "x265", "h265", "x264", "h264", "av1"),
		Language:     detectLanguage(titleLower),
		Indexer:      item.Indexer,
		ReleaseGroup: detectReleaseGroup(item.Title),
		UploadedAt:   item.PublishedAt,
		FailureCount: history.FailureCount,
		Degraded:     history.FailureCount > 0,
	}
}

// defaultProfilePreferences returns the active quality profile as ranking
// preferences. The result is cached for 5 minutes so batch searches don't
// hit the database for every individual library item.
func (s *Service) defaultProfilePreferences(ctx context.Context) ranking.Preferences {
	if s == nil || s.repo == nil {
		return ranking.Preferences{}
	}
	const cacheTTL = 5 * time.Minute
	s.profileCacheMu.Lock()
	if time.Since(s.profileCacheAt) < cacheTTL {
		prefs := s.profileCachedPrefs
		s.profileCacheMu.Unlock()
		return prefs
	}
	s.profileCacheMu.Unlock()

	profile, err := s.repo.GetDefaultQualityProfile(ctx)
	if err != nil {
		return ranking.Preferences{}
	}
	prefs := ranking.Preferences{
		Resolutions:  append([]string(nil), profile.Resolutions...),
		Sources:      append([]string(nil), profile.Sources...),
		Codecs:       append([]string(nil), profile.Codecs...),
		Languages:    append([]string(nil), profile.Languages...),
		AudioFormats: append([]string(nil), profile.AudioFormats...),
		HdrFormats:   append([]string(nil), profile.HdrFormats...),
		PreferProper: profile.PreferProper,
		PreferRepack: profile.PreferRepack,
		RejectCam:    profile.RejectCam,
		MinSizeMB:    profile.MinSizeMB,
		MaxSizeMB:    profile.MaxSizeMB,
	}
	s.profileCacheMu.Lock()
	s.profileCachedPrefs = prefs
	s.profileCacheAt = time.Now()
	s.profileCacheMu.Unlock()
	return prefs
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
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
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

// BackfillMetadataResult summarises a metadata re-enrichment pass.
type BackfillMetadataResult struct {
	ProcessedMovies   int `json:"processedMovies"`
	ProcessedShows    int `json:"processedShows"`
	Enriched          int `json:"enriched"`
	Failed            int `json:"failed"`
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
}

// FillMissingEpisodes queries TMDB for the episode list of every TV show that
// has missing episodes, then creates library_item + queue_item rows for each
// episode not yet in the local database. Those new items enter the normal
// search queue and will be picked up by the next SearchPendingLibrary pass.
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
			for _, ep := range season.Episodes {
				if ep.EpisodeNumber <= 0 {
					continue
				}
				result.EpisodesFound++
				created, err := s.repo.EnsureEpisodeLibraryItem(ctx, show.TVShowID, show.ShowTitle, seasonNum, ep.EpisodeNumber, ep.Name, ep.AirDate)
				if err != nil || !created {
					continue
				}
				result.ItemsCreated++
			}
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
	results, err := s.hydra.Search(ctx, hydra.SearchRequest{
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
		result := ranking.Score(candidate, ranking.Requirements{})
		out = append(out, ManualSearchItem{
			Title:      r.Title,
			ExternalURL: r.Link,
			Indexer:    r.Indexer,
			SizeBytes:  r.SizeBytes,
			Score:      result.Score,
			Resolution: resolution,
			Source:     source,
			Codec:      codec,
			Audio:      audio,
			HDR:        hdr,
		})
	}
	return out, nil
}
