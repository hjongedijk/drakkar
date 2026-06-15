package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/hydra"
	"github.com/hjongedijk/drakkar/internal/library"
	"github.com/hjongedijk/drakkar/internal/seerr"
	"github.com/hjongedijk/drakkar/internal/tmdb"
	"github.com/hjongedijk/drakkar/internal/tvdb"
)

type repoStub struct {
	requests       []database.MediaRequestSummary
	searchInput    database.LibrarySearchInput
	history        map[string]database.CandidateHistory
	defaultProfile database.QualityProfile
	itemProfile    *database.QualityProfile
	conflict       string
	searchApplied  []database.SearchCandidateRecord
	searchFailed   []string
	pending        []database.PendingLibrarySearchTarget
	failedQueues   []database.FailedQueueRetryTarget
	upgradable     []int64
	movieCalls     int
	tvCalls        int
	fetching       int64
	imported       database.ImportedNZB
	indexed        int64
	selected       database.ReleaseSummary
	selectedByID   map[int64]database.ReleaseSummary
	promoted       *database.ReleaseSummary
	alternative    *database.ReleaseSummary
	next           *database.ReleaseSummary
	failed         []string
	rejected       []string
	retryTarget    database.QueueRetryTarget
	skipped        []int64
	stored         database.StoredNZBDocument
	restoredGroup  []int64
	movieMeta      struct {
		libraryItemID int64
		tmdbID        int64
		title         string
		year          int
		imdbID        string
	}
	episodeMeta struct {
		libraryItemID int64
		tmdbID        int64
		show          string
		year          int
		imdbID        string
		episodeTitle  string
	}
}

func (r *repoStub) ListMediaRequests(ctx context.Context) ([]database.MediaRequestSummary, error) {
	return r.requests, nil
}
func (r *repoStub) UpsertMovieRequest(ctx context.Context, externalID string, tmdbID int64, title string, year int) (int64, bool, error) {
	r.movieCalls++
	return 11, true, nil
}
func (r *repoStub) UpsertEpisodeRequest(ctx context.Context, externalID string, tvdbID, tmdbID int64, show string, year, season, episode int, episodeTitle string) (int64, bool, error) {
	r.tvCalls++
	return 12, true, nil
}
func (r *repoStub) EnrichMovieMetadata(ctx context.Context, libraryItemID, tmdbID int64, title string, year int, imdbID string) error {
	r.movieMeta = struct {
		libraryItemID int64
		tmdbID        int64
		title         string
		year          int
		imdbID        string
	}{libraryItemID, tmdbID, title, year, imdbID}
	return nil
}
func (r *repoStub) EnrichEpisodeMetadata(ctx context.Context, libraryItemID, tmdbID int64, show string, year int, imdbID, episodeTitle string) error {
	r.episodeMeta = struct {
		libraryItemID int64
		tmdbID        int64
		show          string
		year          int
		imdbID        string
		episodeTitle  string
	}{libraryItemID, tmdbID, show, year, imdbID, episodeTitle}
	return nil
}

func (r *repoStub) EnrichMovieFull(_ context.Context, _ int64, e database.MovieEnrichment) error {
	r.movieMeta = struct {
		libraryItemID int64
		tmdbID        int64
		title         string
		year          int
		imdbID        string
	}{0, e.TMDBID, e.Title, e.Year, e.IMDbID}
	return nil
}

func (r *repoStub) EnrichTVFull(_ context.Context, _ int64, episodeTitle string, e database.TVShowEnrichment) error {
	r.episodeMeta = struct {
		libraryItemID int64
		tmdbID        int64
		show          string
		year          int
		imdbID        string
		episodeTitle  string
	}{0, e.TMDBID, e.ShowTitle, e.Year, e.IMDbID, episodeTitle}
	return nil
}
func (r *repoStub) DetectMovieSearchConflict(ctx context.Context, libraryItemID int64) (string, error) {
	return r.conflict, nil
}
func (r *repoStub) GetDefaultQualityProfile(ctx context.Context) (database.QualityProfile, error) {
	return r.defaultProfile, nil
}
func (r *repoStub) GetLibrarySearchInput(ctx context.Context, libraryItemID int64) (database.LibrarySearchInput, error) {
	return r.searchInput, nil
}
func (r *repoStub) LookupCandidateHistory(ctx context.Context, libraryItemID int64) (map[string]database.CandidateHistory, error) {
	return r.history, nil
}
func (r *repoStub) ListPendingLibrarySearchTargets(ctx context.Context) ([]database.PendingLibrarySearchTarget, error) {
	return r.pending, nil
}
func (r *repoStub) ListFailedQueueRetryTargets(ctx context.Context, limit int) ([]database.FailedQueueRetryTarget, error) {
	return r.failedQueues, nil
}
func (r *repoStub) GetQueueRetryTarget(ctx context.Context, queueItemID int64) (database.QueueRetryTarget, error) {
	return r.retryTarget, nil
}
func (r *repoStub) BlocklistQueueSelectedRelease(ctx context.Context, queueItemID int64, reason string, ttlDays int) error {
	r.failed = append(r.failed, "blocklist:"+reason)
	return nil
}
func (r *repoStub) ClearQueueSelectedRelease(ctx context.Context, queueItemID int64) error {
	r.skipped = append(r.skipped, queueItemID)
	return nil
}
func (r *repoStub) ReplaceSearchCandidates(ctx context.Context, libraryItemID int64, candidates []database.SearchCandidateRecord) (*int64, error) {
	r.searchApplied = candidates
	if len(candidates) == 0 || candidates[0].Rejected {
		return nil, nil
	}
	value := int64(88)
	r.selected = database.ReleaseSummary{
		SelectedReleaseID:  value,
		ReleaseCandidateID: 1,
		LibraryItemID:      libraryItemID,
		Title:              candidates[0].Title,
		ExternalURL:        candidates[0].ExternalURL,
	}
	return &value, nil
}

func (r *repoStub) MarkLibrarySearchFailed(ctx context.Context, libraryItemID int64, reason string) error {
	r.searchFailed = append(r.searchFailed, fmt.Sprintf("%d:%s", libraryItemID, reason))
	return nil
}

func (r *repoStub) GetSelectedReleaseSummary(ctx context.Context, selectedReleaseID int64) (database.ReleaseSummary, error) {
	if r.selectedByID != nil {
		if item, ok := r.selectedByID[selectedReleaseID]; ok {
			return item, nil
		}
	}
	return r.selected, nil
}

func (r *repoStub) GetLatestSelectedReleaseSummaryByLibraryItem(_ context.Context, libraryItemID int64) (*database.ReleaseSummary, error) {
	if r.selected.LibraryItemID == libraryItemID && r.selected.SelectedReleaseID != 0 {
		item := r.selected
		return &item, nil
	}
	return nil, nil
}

func (r *repoStub) GetStoredNZBDocument(ctx context.Context, selectedReleaseID int64) (database.StoredNZBDocument, error) {
	return r.stored, nil
}

func (r *repoStub) PromoteBestRetryCandidate(ctx context.Context, libraryItemID int64) (*database.ReleaseSummary, error) {
	if r.promoted == nil {
		return nil, nil
	}
	next := *r.promoted
	r.selected = next
	return &next, nil
}

func (r *repoStub) PromoteAlternativeRetryCandidate(ctx context.Context, libraryItemID int64, excludeReleaseCandidateID int64) (*database.ReleaseSummary, error) {
	if r.alternative == nil {
		return nil, nil
	}
	next := *r.alternative
	r.selected = next
	return &next, nil
}

func (r *repoStub) SelectReleaseCandidate(ctx context.Context, releaseCandidateID int64) (*database.ReleaseSummary, error) {
	r.selected = database.ReleaseSummary{
		SelectedReleaseID:  101,
		ReleaseCandidateID: releaseCandidateID,
		LibraryItemID:      42,
		Title:              "Manual.Select.Release",
		ExternalURL:        "http://example/manual.nzb",
	}
	return &r.selected, nil
}

func (r *repoStub) RejectReleaseCandidate(ctx context.Context, releaseCandidateID int64, reason string) (*database.ReleaseSummary, error) {
	r.rejected = append(r.rejected, reason)
	if r.next == nil {
		return nil, nil
	}
	next := *r.next
	r.selected = next
	r.next = nil
	return &next, nil
}

func (r *repoStub) RestoreReleaseCandidate(ctx context.Context, releaseCandidateID int64) error {
	r.rejected = append(r.rejected, "restored")
	return nil
}

func (r *repoStub) RestoreRejectedReleaseCandidates(ctx context.Context, libraryItemID int64) (database.RejectedReleaseRestoreResult, error) {
	r.restoredGroup = append(r.restoredGroup, libraryItemID)
	return database.RejectedReleaseRestoreResult{LibraryItemID: libraryItemID, Restored: 2}, nil
}

func (r *repoStub) SkipReleaseCandidate(ctx context.Context, releaseCandidateID int64) (*database.ReleaseSummary, error) {
	r.skipped = append(r.skipped, releaseCandidateID)
	if r.next == nil {
		return nil, nil
	}
	next := *r.next
	r.selected = next
	r.next = nil
	return &next, nil
}

func (r *repoStub) MarkSelectedReleaseFetching(ctx context.Context, selectedReleaseID int64) error {
	r.fetching = selectedReleaseID
	return nil
}

func (r *repoStub) ImportSelectedReleaseNZB(ctx context.Context, selectedReleaseID int64, imported database.ImportedNZB) (database.QueueSnapshot, error) {
	r.imported = imported
	// Simulate real import creating virtual files so VirtualFileCount==0 fast-fail doesn't trigger.
	// Only update r.selected when selectedByID has no explicit override for this release.
	if _, hasOverride := r.selectedByID[selectedReleaseID]; !hasOverride {
		r.selected.VirtualFileCount = 1
	}
	return database.QueueSnapshot{
		QueueItemID:     99,
		LibraryItemID:   42,
		LibraryTitle:    "Dune",
		State:           database.QueueIndexing,
		SelectedRelease: &selectedReleaseID,
		NZBDocumentID:   func() *int64 { value := int64(123); return &value }(),
	}, nil
}

func (r *repoStub) SetImportedNZBIndexed(ctx context.Context, queueItemID int64) error {
	r.indexed = queueItemID
	return nil
}

func (r *repoStub) FailSelectedReleaseAndPromoteNext(ctx context.Context, selectedReleaseID int64, reason string) (*database.ReleaseSummary, error) {
	r.failed = append(r.failed, reason)
	if r.next == nil {
		return nil, nil
	}
	next := *r.next
	r.selected = next
	r.next = nil
	return &next, nil
}

func (r *repoStub) ShouldAttemptSeasonPack(_ context.Context, _ int64, _ int) (bool, error) {
	return false, nil // tests skip season pack logic by default
}

func (r *repoStub) RecordSeasonPackAttempt(_ context.Context, _ int64, _ int, _ string) error {
	return nil
}

func (r *repoStub) ClearFailedQueueItems(_ context.Context) (int, error) { return 0, nil }
func (r *repoStub) ListMetadataBackfillTargets(_ context.Context) ([]database.MetadataBackfillTarget, error) {
	return nil, nil
}
func (r *repoStub) ListShowsWithMissingEpisodes(_ context.Context) ([]database.ShowWithMissingEpisodes, error) {
	return nil, nil
}
func (r *repoStub) EnsureEpisodeLibraryItem(_ context.Context, _ int64, _ string, _, _ int, _, _ string) (bool, error) {
	return false, nil
}
func (r *repoStub) ListCustomFormats(_ context.Context) ([]database.CustomFormat, error) {
	return nil, nil
}
func (r *repoStub) GetLibraryItemQualityProfile(_ context.Context, _ int64) (*database.QualityProfile, error) {
	return r.itemProfile, nil
}
func (r *repoStub) GetQualityProfileByName(_ context.Context, _ string) (database.QualityProfile, error) {
	return database.QualityProfile{}, nil
}
func (r *repoStub) ListQualityDefinitions(_ context.Context) ([]database.QualityDefinition, error) {
	return nil, nil
}
func (r *repoStub) ListUpgradableLibraryItems(_ context.Context) ([]int64, error) {
	return r.upgradable, nil
}
func (r *repoStub) CreateImportedNZB(_ context.Context, _ database.ImportedNZB) (database.QueueSnapshot, error) {
	return database.QueueSnapshot{}, nil
}
func (r *repoStub) ListSabQueueItems(_ context.Context, _ string, _, _ int) ([]database.SabQueueItem, int, error) {
	return nil, 0, nil
}
func (r *repoStub) ListSabHistoryItems(_ context.Context, _ string, _, _ int) ([]database.SabHistoryItem, int, error) {
	return nil, 0, nil
}
func (r *repoStub) DismissSabItems(_ context.Context, _ []int64) error { return nil }
func (r *repoStub) DeleteSymlinkPublicationsForLibraryItem(_ context.Context, _ int64) ([]string, error) {
	return nil, nil
}
func (r *repoStub) ResetLibraryItemState(_ context.Context, _ int64) error { return nil }
func (r *repoStub) ListUnrecoverableLibraryItems(_ context.Context) ([]int64, error) {
	return nil, nil
}
func (r *repoStub) ListReleaseBlockRules(_ context.Context) ([]database.ReleaseBlockRule, error) {
	return nil, nil
}

func (r *repoStub) LoadIndexerPolicyMap(_ context.Context) (map[string]int, error) {
	return nil, nil
}

type seerrStub struct {
	requests []seerr.Request
}

func (s seerrStub) PendingRequests(ctx context.Context) ([]seerr.Request, error) {
	return s.requests, nil
}
func (s seerrStub) CreateRequest(_ context.Context, _ string, _ int64) error        { return nil }
func (s seerrStub) CreateTVSeasonRequest(_ context.Context, _ int64, _ []int) error { return nil }

type seasonRequestSeerrStub struct {
	seerrStub
	seasonRequestID int64
	seasonNumbers   []int
}

func (s *seasonRequestSeerrStub) CreateTVSeasonRequest(_ context.Context, tmdbID int64, seasons []int) error {
	s.seasonRequestID = tmdbID
	s.seasonNumbers = append([]int(nil), seasons...)
	return nil
}

type hydraStub struct {
	results    []hydra.SearchResult
	recent     map[string][]hydra.SearchResult
	recentErr  map[string]error
	byQuery    map[string][]hydra.SearchResult
	seqByQuery map[string][]hydraReply
	errByQuery map[string]error
	queries    *[]string
	requests   *[]hydra.SearchRequest
}

type hydraReply struct {
	results []hydra.SearchResult
	err     error
}

func (h hydraStub) Search(ctx context.Context, request hydra.SearchRequest) ([]hydra.SearchResult, error) {
	query := request.Query
	if query == "" {
		if request.IMDbID != "" {
			query = request.IMDbID
		} else if request.TVDBID > 0 {
			query = fmt.Sprintf("tvdb:%d", request.TVDBID)
		}
	}
	if h.queries != nil {
		*h.queries = append(*h.queries, query)
	}
	if h.requests != nil {
		*h.requests = append(*h.requests, request)
	}
	if h.seqByQuery != nil {
		if replies, ok := h.seqByQuery[query]; ok && len(replies) > 0 {
			reply := replies[0]
			h.seqByQuery[query] = replies[1:]
			return reply.results, reply.err
		}
	}
	if h.errByQuery != nil {
		if err, ok := h.errByQuery[query]; ok {
			return nil, err
		}
	}
	if h.byQuery != nil {
		return h.byQuery[query], nil
	}
	return h.results, nil
}

func (h hydraStub) SearchRecent(ctx context.Context, mediaType string) ([]hydra.SearchResult, error) {
	if h.recentErr != nil {
		if err, ok := h.recentErr[mediaType]; ok {
			return nil, err
		}
	}
	if h.recent != nil {
		return h.recent[mediaType], nil
	}
	return h.results, nil
}

type fetcherStub struct {
	fileName string
	raw      []byte
}

func (f fetcherStub) Fetch(ctx context.Context, rawURL string) (string, []byte, error) {
	return f.fileName, f.raw, nil
}

type tmdbStub struct{}

func (tmdbStub) Enabled() bool { return true }
func (tmdbStub) MovieDetails(ctx context.Context, tmdbID int64) (tmdb.MovieDetails, error) {
	return tmdb.MovieDetails{Title: "Dune", Year: 2021, IMDbID: "tt1160419"}, nil
}
func (tmdbStub) TVDetails(ctx context.Context, tmdbID int64) (tmdb.TVDetails, error) {
	return tmdb.TVDetails{Name: "Loki", Year: 2021, IMDbID: "tt9140554"}, nil
}
func (tmdbStub) TVSeasonNumbers(_ context.Context, _ int64) ([]int, error) { return nil, nil }
func (tmdbStub) TVSeason(_ context.Context, _ int64, _ int) (tmdb.TVSeason, error) {
	return tmdb.TVSeason{}, nil
}

type tmdbDisabledStub struct{}

func (tmdbDisabledStub) Enabled() bool { return false }
func (tmdbDisabledStub) MovieDetails(ctx context.Context, tmdbID int64) (tmdb.MovieDetails, error) {
	return tmdb.MovieDetails{}, nil
}
func (tmdbDisabledStub) TVDetails(ctx context.Context, tmdbID int64) (tmdb.TVDetails, error) {
	return tmdb.TVDetails{}, nil
}
func (tmdbDisabledStub) TVSeasonNumbers(_ context.Context, _ int64) ([]int, error) { return nil, nil }
func (tmdbDisabledStub) TVSeason(_ context.Context, _ int64, _ int) (tmdb.TVSeason, error) {
	return tmdb.TVSeason{}, nil
}

type tvdbStub struct{}

func (tvdbStub) Enabled() bool { return true }
func (tvdbStub) SeriesDetails(ctx context.Context, tvdbID int64) (tvdb.SeriesDetails, error) {
	return tvdb.SeriesDetails{Name: "The Bear", Year: 2022, IMDbID: "tt14452776"}, nil
}

func TestSyncRequests(t *testing.T) {
	repo := &repoStub{}
	service := NewService(repo, seerrStub{requests: []seerr.Request{
		{ID: 1, Type: "movie", MediaTitle: "Dune", MediaYear: 2021, TMDBID: 438631},
		{ID: 2, Type: "tv", MediaTitle: "Loki", MediaYear: 2021, TVDBID: 362472, TMDBID: 84958, SeasonNumber: 2, EpisodeNumber: 1},
	}}, hydraStub{})
	service.SetTMDBClient(tmdbStub{})

	result, err := service.SyncRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Created != 2 || repo.movieCalls != 1 || repo.tvCalls != 1 {
		t.Fatalf("unexpected sync result %+v repo=%+v", result, repo)
	}
	// Enrichment for new items runs in a goroutine — give it a moment.
	time.Sleep(50 * time.Millisecond)
	if repo.movieMeta.tmdbID != 438631 || repo.movieMeta.imdbID != "tt1160419" || repo.movieMeta.title != "Dune" {
		t.Fatalf("unexpected movie metadata %+v", repo.movieMeta)
	}
	if repo.episodeMeta.tmdbID != 84958 || repo.episodeMeta.show != "Loki" || repo.episodeMeta.imdbID != "tt9140554" {
		t.Fatalf("unexpected episode metadata %+v", repo.episodeMeta)
	}
}

func TestSyncRequestsTVDBFallback(t *testing.T) {
	repo := &repoStub{}
	service := NewService(repo, seerrStub{requests: []seerr.Request{
		{ID: 3, Type: "tv", MediaTitle: "The Bear", MediaYear: 2022, TVDBID: 412567, SeasonNumber: 1, EpisodeNumber: 1, EpisodeTitle: "System"},
	}}, hydraStub{})
	service.SetTMDBClient(tmdbDisabledStub{})
	service.SetTVDBClient(tvdbStub{})

	result, err := service.SyncRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Created != 1 || repo.tvCalls != 1 {
		t.Fatalf("unexpected sync result %+v repo=%+v", result, repo)
	}
	// Enrichment for new items runs in a goroutine — give it a moment.
	time.Sleep(50 * time.Millisecond)
	if repo.episodeMeta.tmdbID != 0 || repo.episodeMeta.show != "The Bear" || repo.episodeMeta.imdbID != "tt14452776" || repo.episodeMeta.year != 2022 {
		t.Fatalf("unexpected episode metadata %+v", repo.episodeMeta)
	}
}

func TestCreateSeerrSeasonRequest(t *testing.T) {
	seerrClient := &seasonRequestSeerrStub{
		seerrStub: seerrStub{
			requests: []seerr.Request{
				{ID: 2, Type: "tv", MediaTitle: "Loki", MediaYear: 2021, TVDBID: 362472, TMDBID: 84958, SeasonNumber: 2, EpisodeNumber: 1},
			},
		},
	}
	service := NewService(&repoStub{}, seerrClient, hydraStub{})

	result, err := service.CreateSeerrSeasonRequest(context.Background(), 84958, []int{2})
	if err != nil {
		t.Fatal(err)
	}
	if result.Seen != 1 || result.Created != 1 {
		t.Fatalf("unexpected sync result %+v", result)
	}
	if seerrClient.seasonRequestID != 84958 {
		t.Fatalf("unexpected tmdb id %d", seerrClient.seasonRequestID)
	}
	if len(seerrClient.seasonNumbers) != 1 || seerrClient.seasonNumbers[0] != 2 {
		t.Fatalf("unexpected seasons %+v", seerrClient.seasonNumbers)
	}
}

func TestSearchLibrary(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
		{Title: "Other.Movie.2021.720p", Link: "http://example/other", Indexer: "hydra", SizeBytes: 4567, PublishedAt: time.Now()},
	}})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 2 || result.SelectedReleaseID == nil {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.searchApplied) != 2 || repo.searchApplied[0].Rejected {
		t.Fatalf("unexpected candidates %+v", repo.searchApplied)
	}
	if repo.fetching != 88 || repo.indexed != 99 || repo.imported.FileName != "dune.nzb" {
		t.Fatalf("unexpected import state fetching=%d indexed=%d imported=%+v", repo.fetching, repo.indexed, repo.imported)
	}
}

func TestSearchLibraryFallsBackToLaterQuery(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
	}
	var queries []string
	service := NewService(repo, seerrStub{}, hydraStub{byQuery: map[string][]hydra.SearchResult{
		"tt1160419": {},
		"Dune 2021": {
			{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
		},
	}, queries: &queries})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil || result.Query != "Dune 2021" {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(queries) < 2 || queries[0] != "tt1160419" || queries[1] != "Dune 2021" {
		t.Fatalf("unexpected queries %+v", queries)
	}
}

func TestSearchLibraryUsesStructuredIMDbSearchRequest(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
	}
	var requests []hydra.SearchRequest
	service := NewService(repo, seerrStub{}, hydraStub{
		byQuery: map[string][]hydra.SearchResult{
			"tt1160419": {
				{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
			},
		},
		requests: &requests,
	})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	if len(requests) == 0 || requests[0].IMDbID != "tt1160419" || requests[0].MediaType != "movie" {
		t.Fatalf("expected structured movie request, got %+v", requests)
	}
	if requests[0].Query != "" {
		t.Fatalf("expected id-only lookup to omit free-text query, got %+v", requests[0])
	}
}

func TestSearchLibraryMarksMetadataConflictWithoutHydraCall(t *testing.T) {
	repo := &repoStub{conflict: "metadata_conflict"}
	var requests []hydra.SearchRequest
	hydraClient := hydraStub{requests: &requests}
	service := NewService(repo, nil, hydraClient)

	result, err := service.SearchLibrary(context.Background(), 34)
	if err != nil {
		t.Fatalf("SearchLibrary error = %v", err)
	}
	if result.LibraryItemID != 34 {
		t.Fatalf("expected library item 34, got %+v", result)
	}
	if len(repo.searchFailed) != 1 || repo.searchFailed[0] != "34:metadata_conflict" {
		t.Fatalf("expected metadata_conflict failure, got %#v", repo.searchFailed)
	}
	if len(requests) != 0 {
		t.Fatalf("expected no hydra search, got %#v", requests)
	}
}

func TestSearchLibraryFallsBackWhenEarlierQueryOnlyReturnsRejectedCandidates(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
	}
	var queries []string
	service := NewService(repo, seerrStub{}, hydraStub{byQuery: map[string][]hydra.SearchResult{
		"tt1160419": {
			{Title: "Other.Movie.2022.720p", Link: "http://example/bad", Indexer: "hydra", SizeBytes: 555, PublishedAt: time.Now()},
		},
		"Dune 2021": {
			{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/good", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
		},
	}, queries: &queries})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil || result.Query != "Dune 2021" {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(queries) < 2 || queries[0] != "tt1160419" || queries[1] != "Dune 2021" {
		t.Fatalf("unexpected queries %+v", queries)
	}
	if len(repo.searchApplied) != 2 || repo.searchApplied[0].Rejected || repo.searchApplied[0].ExternalURL != "http://example/good" {
		t.Fatalf("unexpected final candidates %+v", repo.searchApplied)
	}
}

func TestSearchLibraryPrefersExactEpisodeOverSeasonPack(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "episode",
			Title:         "Loki S01E02",
			ShowTitle:     "Loki",
			ShowYear:      2021,
			SeasonNumber:  1,
			EpisodeNumber: 2,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Loki.Season.1.Complete.1080p.WEB-DL", Link: "http://example/pack", Indexer: "hydra", SizeBytes: 5000, PublishedAt: time.Now()},
		{Title: "Loki.S01E02.1080p.WEB-DL", Link: "http://example/exact", Indexer: "hydra", SizeBytes: 1200, PublishedAt: time.Now()},
	}})
	service.fetcher = fetcherStub{
		fileName: "loki.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Loki S01E02.mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.tv</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	if len(repo.searchApplied) != 2 {
		t.Fatalf("unexpected candidates %+v", repo.searchApplied)
	}
	if repo.searchApplied[0].Title != "Loki.S01E02.1080p.WEB-DL" {
		t.Fatalf("expected exact episode ranked first, got %+v", repo.searchApplied)
	}
}

func TestSearchLibraryPenalizesPreviouslyFailedCandidateAcrossRefresh(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
		history: map[string]database.CandidateHistory{
			"http://example/old": {
				ExternalURL:       "http://example/old",
				FailureCount:      2,
				LastFailureReason: "context deadline exceeded",
			},
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/old", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
		{Title: "Dune.2021.1080p.WEB-DL.x265-NEW", Link: "http://example/new", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
	}})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	if len(repo.searchApplied) != 2 {
		t.Fatalf("unexpected candidates %+v", repo.searchApplied)
	}
	if repo.searchApplied[0].ExternalURL != "http://example/new" {
		t.Fatalf("expected clean candidate ranked first, got %+v", repo.searchApplied)
	}
	if repo.searchApplied[1].FailureCount != 2 || repo.searchApplied[1].LastFailureReason != "context deadline exceeded" {
		t.Fatalf("expected history carried forward, got %+v", repo.searchApplied[1])
	}
}

func TestSearchLibraryContinuesPastSelectedCandidateWithFailureHistory(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
		history: map[string]database.CandidateHistory{
			"http://example/old": {
				ExternalURL:       "http://example/old",
				FailureCount:      1,
				LastFailureReason: "context deadline exceeded",
			},
		},
	}
	var queries []string
	service := NewService(repo, seerrStub{}, hydraStub{byQuery: map[string][]hydra.SearchResult{
		"tt1160419": {
			{Title: "Dune.2021.1080p.WEB-DL.x265-OLD", Link: "http://example/old", Indexer: "hydra", SizeBytes: 1200, PublishedAt: time.Now()},
		},
		"Dune 2021": {
			{Title: "Dune.2021.1080p.WEB-DL.x265-NEW", Link: "http://example/new", Indexer: "hydra", SizeBytes: 1200, PublishedAt: time.Now()},
		},
	}, queries: &queries})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	if len(queries) < 2 || queries[0] != "tt1160419" || queries[1] != "Dune 2021" {
		t.Fatalf("expected search to continue to later query, got %+v", queries)
	}
	if repo.searchApplied[0].ExternalURL != "http://example/new" {
		t.Fatalf("expected fresh candidate ranked first after later query, got %+v", repo.searchApplied)
	}
}

func TestSearchLibraryUsesOnlyOneEpisodeTitleQuery(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "episode",
			Title:         "Loki S01E02",
			ShowTitle:     "Loki",
			ShowYear:      2021,
			SeasonNumber:  1,
			EpisodeNumber: 2,
		},
	}
	var queries []string
	service := NewService(repo, seerrStub{}, hydraStub{byQuery: map[string][]hydra.SearchResult{
		"Loki S01E02": {
			{Title: "Loki.S01E02.1080p.WEB-DL", Link: "http://example/episode", Indexer: "hydra", SizeBytes: 1200, PublishedAt: time.Now()},
		},
	}, queries: &queries})
	service.fetcher = fetcherStub{
		fileName: "loki.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Loki S01E02.mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.tv</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	// Only the canonical SxxExx query should be sent — no year variants, 3x02, episode title, etc.
	if len(queries) != 1 || queries[0] != "Loki S01E02" {
		t.Fatalf("expected exactly 1 episode query 'Loki S01E02', got %+v", queries)
	}
}

func TestSearchLibraryEpisodeNoExtraTitleVariants(t *testing.T) {
	// Episode title, year, and 3x02 variants are NOT sent as separate queries.
	// NZBHydra2 handles per-indexer format adaptation internally.
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "episode",
			Title:         "Loki S01E02",
			ShowTitle:     "Loki",
			EpisodeTitle:  "The Variant",
			ShowYear:      2021,
			SeasonNumber:  1,
			EpisodeNumber: 2,
		},
	}
	var queries []string
	service := NewService(repo, seerrStub{}, hydraStub{byQuery: map[string][]hydra.SearchResult{
		"Loki S01E02": {
			{Title: "Loki.S01E02.1080p.WEB-DL", Link: "http://example/episode", Indexer: "hydra", SizeBytes: 1200, PublishedAt: time.Now()},
		},
	}, queries: &queries})
	service.fetcher = fetcherStub{
		fileName: "loki.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Loki S01E02.mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.tv</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	for _, q := range queries {
		if q == "Loki The Variant" || q == "Loki 1x02" || q == "Loki 2021 S01E02" {
			t.Fatalf("unexpected extra query variant sent: %q (all queries: %v)", q, queries)
		}
	}
}

func TestSearchLibraryUsesStructuredTVDBSearchRequest(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "episode",
			Title:         "Loki S02E03",
			ShowTitle:     "Loki",
			ShowIMDbID:    "tt9140554",
			ShowTVDBID:    362472,
			ShowYear:      2021,
			SeasonNumber:  2,
			EpisodeNumber: 3,
		},
	}
	var requests []hydra.SearchRequest
	service := NewService(repo, seerrStub{}, hydraStub{
		byQuery: map[string][]hydra.SearchResult{
			"Loki S02E03": {
				{Title: "Loki.S02E03.1080p.WEB-DL", Link: "http://example/episode", Indexer: "hydra", SizeBytes: 1200, PublishedAt: time.Now()},
			},
		},
		requests: &requests,
	})
	service.fetcher = fetcherStub{
		fileName: "loki.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Loki S02E03.mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.tv</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	if len(requests) == 0 || requests[0].TVDBID != 362472 || requests[0].SeasonNumber != 2 || requests[0].EpisodeNumber != 3 {
		t.Fatalf("expected structured tv request, got %+v", requests)
	}
}

func TestSearchLibraryUsesWholeShowRequestsWhenEpisodeMissing(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "episode",
			Title:         "Wednesday",
			ShowTitle:     "Wednesday",
			ShowIMDbID:    "tt13443470",
			ShowTVDBID:    397060,
			ShowYear:      2022,
		},
	}
	var requests []hydra.SearchRequest
	service := NewService(repo, seerrStub{}, hydraStub{
		byQuery: map[string][]hydra.SearchResult{
			"tt13443470": {
				{Title: "Wednesday.2022.S01.1080p.NF.WEB-DL", Link: "http://example/show", Indexer: "hydra", SizeBytes: 1200, PublishedAt: time.Now()},
			},
		},
		requests: &requests,
	})
	service.fetcher = fetcherStub{
		fileName: "wednesday.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Wednesday S01E01.mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.tv</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	if len(requests) == 0 {
		t.Fatal("expected at least one hydra request")
	}
	if requests[0].SeasonNumber != 0 || requests[0].EpisodeNumber != 0 {
		t.Fatalf("whole-show request should not send season/episode, got %+v", requests[0])
	}
	if requests[0].TVDBID != 397060 {
		t.Fatalf("expected tvdb id, got %+v", requests[0])
	}
}

func TestSearchLibraryKeepsEarlierUsableCandidatesWhenLaterQueryErrors(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
		history: map[string]database.CandidateHistory{
			"http://example/good": {
				ExternalURL:       "http://example/good",
				FailureCount:      1,
				LastFailureReason: "interrupted_by_restart",
			},
		},
	}
	var queries []string
	service := NewService(repo, seerrStub{}, hydraStub{
		byQuery: map[string][]hydra.SearchResult{
			"tt1160419": {
				{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/good", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
			},
		},
		errByQuery: map[string]error{
			"Dune 2021": errors.New("temporary hydra failure"),
		},
		queries: &queries,
	})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	if len(queries) < 2 || queries[0] != "tt1160419" || queries[1] != "Dune 2021" {
		t.Fatalf("expected both queries to be attempted, got %+v", queries)
	}
	if repo.searchApplied[0].ExternalURL != "http://example/good" {
		t.Fatalf("expected earlier good candidate to survive later error, got %+v", repo.searchApplied)
	}
}

func TestSearchLibraryReturnsErrorWhenAllQueriesFail(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{
		errByQuery: map[string]error{
			"tt1160419": errors.New("hydra unavailable"),
			"Dune 2021": errors.New("hydra unavailable"),
			"Dune":      errors.New("hydra unavailable"),
		},
	})

	if _, err := service.SearchLibrary(context.Background(), 42); err == nil {
		t.Fatal("expected hydra error when all queries fail")
	}
	if len(repo.searchFailed) != 1 || repo.searchFailed[0] != "42:search_error" {
		t.Fatalf("expected durable search failure state, got %+v", repo.searchFailed)
	}
}

func TestSearchLibraryClassifiesTimeoutFailure(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{
		errByQuery: map[string]error{
			"tt1160419": context.DeadlineExceeded,
			"Dune 2021": context.DeadlineExceeded,
			"Dune":      context.DeadlineExceeded,
		},
	})

	if _, err := service.SearchLibrary(context.Background(), 42); err == nil {
		t.Fatal("expected hydra timeout")
	}
	if len(repo.searchFailed) != 1 || repo.searchFailed[0] != "42:search_timeout" {
		t.Fatalf("expected timeout failure state, got %+v", repo.searchFailed)
	}
}

func TestClassifySearchFailureReason(t *testing.T) {
	cases := map[string]string{
		"nzbhydra2 search status 401":  "search_auth_error",
		"nzbhydra2 search status 429":  "search_rate_limited",
		"nzbhydra2 search status 503":  "search_unavailable",
		"dial tcp: connection refused": "search_unavailable",
		"something else":               "search_error",
	}
	for input, expected := range cases {
		if got := classifySearchFailureReason(errors.New(input)); got != expected {
			t.Fatalf("for %q expected %q, got %q", input, expected, got)
		}
	}
}

func TestSearchLibraryRetriesTransientHydraFailure(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
	}
	var queries []string
	service := NewService(repo, seerrStub{}, hydraStub{
		seqByQuery: map[string][]hydraReply{
			"tt1160419": {
				{err: context.DeadlineExceeded},
				{results: []hydra.SearchResult{
					{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/good", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
				}},
			},
		},
		queries: &queries,
	})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil {
		t.Fatalf("expected selected release, got %+v", result)
	}
	if len(queries) != 2 || queries[0] != "tt1160419" || queries[1] != "tt1160419" {
		t.Fatalf("expected one retry of same query, got %+v", queries)
	}
}

func TestSearchLibraryDoesNotRetryAuthFailure(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
	}
	var queries []string
	service := NewService(repo, seerrStub{}, hydraStub{
		errByQuery: map[string]error{
			"tt1160419": errors.New("nzbhydra2 search status 401"),
			"Dune 2021": errors.New("nzbhydra2 search status 401"),
			"Dune":      errors.New("nzbhydra2 search status 401"),
		},
		queries: &queries,
	})

	if _, err := service.SearchLibrary(context.Background(), 42); err == nil {
		t.Fatal("expected auth failure")
	}
	if len(queries) != 3 || queries[0] != "tt1160419" || queries[1] != "Dune 2021" || queries[2] != "Dune" {
		t.Fatalf("expected no per-query retry on auth failure, got %+v", queries)
	}
}

func TestSearchPendingLibrary(t *testing.T) {
	repo := &repoStub{
		pending: []database.PendingLibrarySearchTarget{
			{LibraryItemID: 42},
			{LibraryItemID: 43},
		},
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
	}})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	// Disable the work queue so SearchPendingLibrary searches synchronously in tests.
	service.WorkQueue = nil
	result, err := service.SearchPendingLibrary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed != 2 || result.Searched != 2 || result.Selected != 2 || result.Failed != 0 {
		t.Fatalf("unexpected bulk result %+v", result)
	}
	if len(result.ProcessedItems) != 2 || result.ProcessedItems[0] != 42 || result.ProcessedItems[1] != 43 {
		t.Fatalf("unexpected processed items %+v", result.ProcessedItems)
	}
}

func TestSearchPendingLibraryQueuesAllItems(t *testing.T) {
	const total = pendingQueueBatchSize + 10
	pending := make([]database.PendingLibrarySearchTarget, 0, total)
	for i := range total {
		pending = append(pending, database.PendingLibrarySearchTarget{LibraryItemID: int64(i + 1)})
	}
	repo := &repoStub{pending: pending}
	service := NewService(repo, seerrStub{}, hydraStub{})
	service.WorkQueue = newWorkQueueStub()

	result, err := service.SearchPendingLibrary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// BullMQ queue is Redis-backed — all items are pushed regardless of queue depth.
	if result.Processed != total || result.Searched != total {
		t.Fatalf("expected all %d items queued, got %+v", total, result)
	}
	if depth := service.WorkQueue.Depth(context.Background()); depth != total {
		t.Fatalf("expected workqueue depth=%d got %d", total, depth)
	}
}

func TestSearchRecentPendingMovieSelectsWithoutActiveHydraSearch(t *testing.T) {
	repo := &repoStub{
		pending: []database.PendingLibrarySearchTarget{{LibraryItemID: 42}},
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			IMDbID:        "tt1160419",
			MovieYear:     2021,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{
		recent: map[string][]hydra.SearchResult{
			"movie": {
				{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
			},
		},
	})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SearchRecentPending(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed != 1 || result.Searched != 1 || result.Selected != 1 || result.Failed != 0 {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestSearchRecentPendingSkipsNonMatchingMediaType(t *testing.T) {
	repo := &repoStub{
		pending: []database.PendingLibrarySearchTarget{{LibraryItemID: 42}},
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{
		recent: map[string][]hydra.SearchResult{
			"tv": {
				{Title: "Loki.S02E03.1080p.WEB-DL.x265-GRP", Link: "http://example/nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
			},
		},
	})
	result, err := service.SearchRecentPending(context.Background(), "tv")
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed != 0 || result.Searched != 0 || result.Selected != 0 {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestSyncRequestsDoesNotQueueCreatedItems(t *testing.T) {
	service := NewService(&repoStub{}, seerrStub{
		requests: []seerr.Request{
			{ID: 1, Type: "movie", TMDBID: 11, MediaTitle: "Dune", MediaYear: 2021},
		},
	}, hydraStub{})
	service.WorkQueue = newWorkQueueStub()
	if _, err := service.SyncRequests(context.Background()); err != nil {
		t.Fatal(err)
	}
	if depth := service.WorkQueue.Depth(context.Background()); depth != 0 {
		t.Fatalf("expected no auto-queued items after sync, got depth=%d", depth)
	}
}

func TestRestoreRejectedReleases(t *testing.T) {
	repo := &repoStub{}
	service := NewService(repo, seerrStub{}, hydraStub{})

	result, err := service.RestoreRejectedReleases(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.LibraryItemID != 42 || result.Restored != 2 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.restoredGroup) != 1 || repo.restoredGroup[0] != 42 {
		t.Fatalf("unexpected restored groups %+v", repo.restoredGroup)
	}
}

func TestRetryFailedQueue(t *testing.T) {
	repo := &repoStub{
		failedQueues: []database.FailedQueueRetryTarget{
			{QueueItemID: 55, LibraryItemID: 42, FailureReason: "interrupted_by_restart", HasSelectedRelease: true, CandidateFailureCount: 0},
			{QueueItemID: 56, LibraryItemID: 43, FailureReason: "interrupted_by_restart", HasSelectedRelease: true, CandidateFailureCount: 0},
		},
		retryTarget: database.QueueRetryTarget{
			QueueItemID:       55,
			LibraryItemID:     42,
			SelectedReleaseID: func() *int64 { v := int64(303); return &v }(),
		},
		selected: database.ReleaseSummary{
			SelectedReleaseID: 303,
			LibraryItemID:     42,
			ExternalURL:       "http://example/retry.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{})
	service.fetcher = fetcherStub{
		fileName: "retry.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Retry (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.RetryFailedQueue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed != 2 || result.Retried != 2 || result.Failed != 0 {
		t.Fatalf("unexpected bulk retry result %+v", result)
	}
	if len(result.ProcessedQueues) != 2 || result.ProcessedQueues[0] != 55 || result.ProcessedQueues[1] != 56 {
		t.Fatalf("unexpected processed queues %+v", result.ProcessedQueues)
	}
}

func TestSearchLibraryFallsBackToNextCandidate(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
		next: &database.ReleaseSummary{
			SelectedReleaseID:  89,
			ReleaseCandidateID: 2,
			LibraryItemID:      42,
			Title:              "Dune.2021.720p.WEB-DL.x264-GRP2",
			ExternalURL:        "http://example/next.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/bad.nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
		{Title: "Dune.2021.720p.WEB-DL.x264-GRP2", Link: "http://example/next.nzb", Indexer: "hydra", SizeBytes: 4567, PublishedAt: time.Now()},
	}})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}
	service.fetcher = &sequenceFetcher{
		results: []fetchResult{
			{err: context.DeadlineExceeded},
			{fileName: "next.nzb", raw: []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`)},
		},
	}

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil || *result.SelectedReleaseID != 89 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.failed) != 1 || repo.fetching != 89 || repo.indexed != 99 {
		t.Fatalf("unexpected fallback state failed=%v fetching=%d indexed=%d", repo.failed, repo.fetching, repo.indexed)
	}
}

func TestSearchLibraryFallsBackWhenPublishFails(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
		next: &database.ReleaseSummary{
			SelectedReleaseID:  89,
			ReleaseCandidateID: 2,
			LibraryItemID:      42,
			Title:              "Dune.2021.720p.WEB-DL.x264-GRP2",
			ExternalURL:        "http://example/next.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/first.nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
		{Title: "Dune.2021.720p.WEB-DL.x264-GRP2", Link: "http://example/next.nzb", Indexer: "hydra", SizeBytes: 4567, PublishedAt: time.Now()},
	}})
	service.fetcher = &sequenceFetcher{
		results: []fetchResult{
			{fileName: "first.nzb", raw: []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`)},
			{fileName: "next.nzb", raw: []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`)},
		},
	}
	calls := 0
	service.SetPostImportHook(func(ctx context.Context, item database.QueueSnapshot) error {
		calls++
		if calls == 1 {
			return library.ErrNoVirtualFiles
		}
		return nil
	})

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil || *result.SelectedReleaseID != 89 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.failed) != 1 || repo.failed[0] != library.ErrNoVirtualFiles.Error() {
		t.Fatalf("unexpected failed reasons %+v", repo.failed)
	}
}

func TestSearchLibraryFallsBackWithArchiveRejectReason(t *testing.T) {
	repo := &repoStub{
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
		next: &database.ReleaseSummary{
			SelectedReleaseID:  89,
			ReleaseCandidateID: 2,
			LibraryItemID:      42,
			Title:              "Dune.2021.720p.WEB-DL.x264-GRP2",
			ExternalURL:        "http://example/next.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/first.nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
		{Title: "Dune.2021.720p.WEB-DL.x264-GRP2", Link: "http://example/next.nzb", Indexer: "hydra", SizeBytes: 4567, PublishedAt: time.Now()},
	}})
	service.fetcher = &sequenceFetcher{
		results: []fetchResult{
			{fileName: "first.nzb", raw: []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Movie.part01.rar&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`)},
			{fileName: "next.nzb", raw: []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`)},
		},
	}
	calls := 0
	service.SetPostImportHook(func(ctx context.Context, item database.QueueSnapshot) error {
		calls++
		if calls == 1 {
			repo.selected.ArchiveRejects = "archive_video_not_found"
			return library.ErrNoVirtualFiles
		}
		return nil
	})

	result, err := service.SearchLibrary(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil || *result.SelectedReleaseID != 89 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.failed) != 1 || repo.failed[0] != "archive_video_not_found" {
		t.Fatalf("unexpected failed reasons %+v", repo.failed)
	}
}

func TestImportSelectedReleaseFallsBackBeforePublishHookWhenArchiveRejectHasNoVirtualFiles(t *testing.T) {
	repo := &repoStub{
		selectedByID: map[int64]database.ReleaseSummary{
			88: {
				SelectedReleaseID:  88,
				ReleaseCandidateID: 1,
				LibraryItemID:      42,
				Title:              "First.Release",
				ExternalURL:        "http://example/first.nzb",
				ArchiveRejects:     "archive_video_not_found",
				VirtualFileCount:   0,
			},
		},
		next: &database.ReleaseSummary{
			SelectedReleaseID:  89,
			ReleaseCandidateID: 2,
			LibraryItemID:      42,
			Title:              "Next.Release",
			ExternalURL:        "http://example/next.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{})
	service.fetcher = &sequenceFetcher{
		results: []fetchResult{
			{fileName: "next.nzb", raw: []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`)},
		},
	}
	postImportCalls := 0
	service.SetPostImportHook(func(ctx context.Context, item database.QueueSnapshot) error {
		postImportCalls++
		return nil
	})

	result, err := service.importSelectedRelease(context.Background(), database.ReleaseSummary{
		SelectedReleaseID:  88,
		ReleaseCandidateID: 1,
		LibraryItemID:      42,
		Title:              "First.Release",
		ExternalURL:        "http://example/first.nzb",
	}, database.ImportedNZB{
		FileName: "first.nzb",
		XML:      []byte(`<nzb/>`),
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || *result != 89 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.failed) != 1 || repo.failed[0] != "archive_video_not_found" {
		t.Fatalf("unexpected failed reasons %+v", repo.failed)
	}
	if postImportCalls != 1 {
		t.Fatalf("expected post import only for promoted candidate, got %d", postImportCalls)
	}
}

func TestSelectRelease(t *testing.T) {
	repo := &repoStub{}
	service := NewService(repo, seerrStub{}, hydraStub{})
	service.fetcher = fetcherStub{
		fileName: "manual.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Manual (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SelectRelease(context.Background(), 77)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil || *result.SelectedReleaseID != 101 {
		t.Fatalf("unexpected result %+v", result)
	}
	if repo.fetching != 101 || repo.indexed != 99 {
		t.Fatalf("unexpected state fetching=%d indexed=%d", repo.fetching, repo.indexed)
	}
}

func TestRejectReleasePromotesNext(t *testing.T) {
	repo := &repoStub{
		next: &database.ReleaseSummary{
			SelectedReleaseID:  202,
			ReleaseCandidateID: 9,
			LibraryItemID:      42,
			Title:              "Promoted.Release",
			ExternalURL:        "http://example/promoted.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{})
	service.fetcher = fetcherStub{
		fileName: "promoted.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Promoted (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.RejectRelease(context.Background(), 8, "manual_reject")
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedReleaseID == nil || *result.SelectedReleaseID != 202 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.rejected) != 1 || repo.fetching != 202 {
		t.Fatalf("unexpected state rejected=%v fetching=%d", repo.rejected, repo.fetching)
	}
}

func TestRestoreRelease(t *testing.T) {
	repo := &repoStub{}
	service := NewService(repo, seerrStub{}, hydraStub{})

	result, err := service.RestoreRelease(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "restored" || result.ReleaseCandidateID != 8 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.rejected) != 1 || repo.rejected[0] != "restored" {
		t.Fatalf("unexpected restore state %+v", repo.rejected)
	}
}

func TestSkipReleasePromotesNext(t *testing.T) {
	repo := &repoStub{
		next: &database.ReleaseSummary{
			SelectedReleaseID:  202,
			ReleaseCandidateID: 9,
			LibraryItemID:      42,
			Title:              "Promoted.Release",
			ExternalURL:        "http://example/promoted.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{})
	service.fetcher = fetcherStub{
		fileName: "promoted.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Promoted (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.SkipRelease(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "skipped" || result.SelectedReleaseID == nil || *result.SelectedReleaseID != 202 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.skipped) != 1 || repo.skipped[0] != 8 || repo.fetching != 202 {
		t.Fatalf("unexpected skip state skipped=%v fetching=%d", repo.skipped, repo.fetching)
	}
}

func TestRetryQueueItemSelectedRelease(t *testing.T) {
	repo := &repoStub{
		retryTarget: database.QueueRetryTarget{
			QueueItemID:       55,
			LibraryItemID:     42,
			SelectedReleaseID: func() *int64 { v := int64(303); return &v }(),
		},
		selected: database.ReleaseSummary{
			SelectedReleaseID: 303,
			LibraryItemID:     42,
			ExternalURL:       "http://example/retry.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{})
	service.fetcher = fetcherStub{
		fileName: "retry.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Retry (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.RetryQueueItem(context.Background(), 55)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "retried_selected_release" || result.SelectedReleaseID == nil || *result.SelectedReleaseID != 303 {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestRetryQueueItemStoredNZB(t *testing.T) {
	repo := &repoStub{
		retryTarget: database.QueueRetryTarget{
			QueueItemID:       58,
			LibraryItemID:     42,
			SelectedReleaseID: func() *int64 { v := int64(304); return &v }(),
		},
		selected: database.ReleaseSummary{
			SelectedReleaseID: 304,
			LibraryItemID:     42,
			ExternalURL:       "",
		},
		stored: database.StoredNZBDocument{
			SelectedReleaseID: 304,
			FileName:          "stored-retry.nzb",
			XML:               []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Stored Retry (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{})

	result, err := service.RetryQueueItem(context.Background(), 58)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "retried_stored_nzb" || result.SelectedReleaseID == nil || *result.SelectedReleaseID != 304 {
		t.Fatalf("unexpected result %+v", result)
	}
	if repo.fetching != 304 || repo.imported.FileName != "stored-retry.nzb" || repo.indexed != 99 {
		t.Fatalf("unexpected stored retry state fetching=%d imported=%+v indexed=%d", repo.fetching, repo.imported, repo.indexed)
	}
}

func TestRetryQueueItemUsesAlternativeCandidateWhenSelectedHasFailureHistory(t *testing.T) {
	repo := &repoStub{
		retryTarget: database.QueueRetryTarget{
			QueueItemID:       59,
			LibraryItemID:     42,
			SelectedReleaseID: func() *int64 { v := int64(305); return &v }(),
		},
		selected: database.ReleaseSummary{
			SelectedReleaseID:  305,
			ReleaseCandidateID: 9001,
			LibraryItemID:      42,
			ExternalURL:        "http://example/old-retry.nzb",
			FailureCount:       2,
		},
		alternative: &database.ReleaseSummary{
			SelectedReleaseID: 406,
			LibraryItemID:     42,
			ExternalURL:       "http://example/alt.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{})
	service.fetcher = fetcherStub{
		fileName: "alt.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Alternative (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.RetryQueueItem(context.Background(), 59)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "retried_alternative_candidate" || result.SelectedReleaseID == nil || *result.SelectedReleaseID != 406 {
		t.Fatalf("unexpected result %+v", result)
	}
	if repo.fetching != 406 {
		t.Fatalf("expected alternative release fetch, got %d", repo.fetching)
	}
}

func TestRetryQueueItemResearchesLibrary(t *testing.T) {
	repo := &repoStub{
		retryTarget: database.QueueRetryTarget{
			QueueItemID:   56,
			LibraryItemID: 42,
		},
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Dune.2021.1080p.WEB-DL.x265-GRP", Link: "http://example/nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
	}})
	service.fetcher = fetcherStub{
		fileName: "dune.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.RetryQueueItem(context.Background(), 56)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "researched_library_item" || result.SearchCandidateCnt != 1 {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestRetryQueueItemUsesExistingCandidateBeforeResearch(t *testing.T) {
	repo := &repoStub{
		retryTarget: database.QueueRetryTarget{
			QueueItemID:   57,
			LibraryItemID: 42,
		},
		promoted: &database.ReleaseSummary{
			SelectedReleaseID: 404,
			LibraryItemID:     42,
			ExternalURL:       "http://example/existing.nzb",
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Should.Not.Be.Used", Link: "http://example/new.nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
	}})
	service.fetcher = fetcherStub{
		fileName: "existing.nzb",
		raw:      []byte(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="&quot;Existing (2021).mkv&quot;" poster="poster" date="1710000000"><groups><group>alt.binaries.movies</group></groups><segments><segment bytes="1000" number="1">&lt;msg1&gt;</segment></segments></file></nzb>`),
	}

	result, err := service.RetryQueueItem(context.Background(), 57)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "retried_existing_candidate" || result.SelectedReleaseID == nil || *result.SelectedReleaseID != 404 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(repo.searchApplied) != 0 {
		t.Fatalf("did not expect fresh search, got candidates %+v", repo.searchApplied)
	}
}

func TestSearchUpgradesRequiresMinimumCustomFormatScoreIncrement(t *testing.T) {
	repo := &repoStub{
		upgradable: []int64{42},
		itemProfile: &database.QualityProfile{
			Name:                            "Upgrade Gate",
			AllowUpgrade:                    true,
			MinimumUpgradeCustomFormatScore: 100,
		},
		searchInput: database.LibrarySearchInput{
			LibraryItemID: 42,
			MediaType:     "movie",
			Title:         "Dune",
			MovieYear:     2021,
		},
		selected: database.ReleaseSummary{
			SelectedReleaseID:  90,
			LibraryItemID:      42,
			CustomFormatScore:  50,
			ReleaseCandidateID: 9,
		},
	}
	service := NewService(repo, seerrStub{}, hydraStub{results: []hydra.SearchResult{
		{Title: "Dune.2021.1080p.WEB-DL.Atmos-GRP", Link: "http://example/nzb", Indexer: "hydra", SizeBytes: 1234, PublishedAt: time.Now()},
	}})

	result, err := service.SearchUpgrades(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Upgraded != 0 {
		t.Fatalf("expected upgrade to be blocked by CF threshold, got %+v", result)
	}
	if len(repo.searchApplied) != 1 {
		t.Fatalf("expected one candidate, got %+v", repo.searchApplied)
	}
	if !repo.searchApplied[0].Rejected || repo.searchApplied[0].RejectReason != "upgrade_custom_format_score" {
		t.Fatalf("expected upgrade_custom_format_score reject, got %+v", repo.searchApplied[0])
	}
}

type fetchResult struct {
	fileName string
	raw      []byte
	err      error
}

type sequenceFetcher struct {
	results []fetchResult
	index   int
}

func (f *sequenceFetcher) Fetch(ctx context.Context, rawURL string) (string, []byte, error) {
	result := f.results[f.index]
	if f.index < len(f.results)-1 {
		f.index++
	}
	return result.fileName, result.raw, result.err
}
