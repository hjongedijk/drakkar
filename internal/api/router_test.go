package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hjongedijk/drakkar/internal/cache"
	"github.com/hjongedijk/drakkar/internal/config"
	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/library"
	"github.com/hjongedijk/drakkar/internal/maintenance"
	"github.com/hjongedijk/drakkar/internal/nzb"
	"github.com/hjongedijk/drakkar/internal/probe"
	"github.com/hjongedijk/drakkar/internal/queue"
	intsub "github.com/hjongedijk/drakkar/internal/subtitles"
	"github.com/hjongedijk/drakkar/internal/workflow"
)

type statusStub struct{}

type workflowStub struct {
	requests   []database.MediaRequestSummary
	sync       workflow.SyncResult
	pending    workflow.BulkSearchResult
	workQueue  workflow.WorkQueueStatus
	retryAll   workflow.BulkQueueRetryResult
	search     workflow.SearchResult
	upgrades   workflow.UpgradeSearchResult
	selectr    workflow.ReleaseActionResult
	reject     workflow.ReleaseActionResult
	restore    workflow.ReleaseActionResult
	restoreAll database.RejectedReleaseRestoreResult
	skip       workflow.ReleaseActionResult
	retry      workflow.QueueRetryResult
	queueAct   workflow.QueueManageResult
	queueBulk  workflow.BulkQueueRetryResult
	importCall *sabImportCall
}

type sabImportCall struct {
	filename  string
	mediaType string
	size      int
}

type publicationStub struct {
	republished int64
	pending     library.BulkRepublishResult
}

type maintenanceStub struct{}
type cacheStub struct{}
type probeStub struct {
	report probe.Report
}
type subtitleStub struct {
	items      []database.SubtitleFileSummary
	candidates []database.SubtitleCandidateSummary
	search     intsub.SearchResult
	download   intsub.UploadResult
	upload     intsub.UploadResult
	deleted    int64
}
type blocklistStub struct {
	items    []database.BlocklistItemSummary
	cleared  int64
	all      database.BlocklistClearResult
	created  database.BlocklistItemSummary
	updated  database.BlocklistItemSummary
	lastBody database.BlocklistMutation
}

type profilesStub struct {
	profiles         []database.QualityProfile
	requestLibraryID int64
	lastRequestID    int64
	lastProfileID    *int64
}

func (statusStub) Status() Status {
	return Status{Service: "drakkar", Healthy: true, StartedAt: time.Now().UTC()}
}

func (w workflowStub) ListRequests(ctx context.Context) ([]database.MediaRequestSummary, error) {
	return w.requests, nil
}

func (w workflowStub) SyncRequests(ctx context.Context) (workflow.SyncResult, error) {
	return w.sync, nil
}

func (w workflowStub) CreateSeerrRequest(ctx context.Context, mediaType string, tmdbID int64) (workflow.SyncResult, error) {
	return w.sync, nil
}

func (w workflowStub) CreateSeerrSeasonRequest(ctx context.Context, tmdbID int64, seasons []int) (workflow.SyncResult, error) {
	return w.sync, nil
}

func (w workflowStub) SearchPendingLibrary(ctx context.Context) (workflow.BulkSearchResult, error) {
	return w.pending, nil
}
func (w workflowStub) PrioritizeTVShowMissing(ctx context.Context, tvShowID int64) (workflow.PrioritizeTVShowResult, error) {
	return workflow.PrioritizeTVShowResult{TVShowID: tvShowID, Queued: 3}, nil
}

func (w workflowStub) WorkQueueStatus(ctx context.Context) (workflow.WorkQueueStatus, error) {
	return w.workQueue, nil
}

func (w workflowStub) PauseWorkQueue(ctx context.Context) (workflow.WorkQueueStatus, error) {
	status := w.workQueue
	status.Paused = true
	return status, nil
}

func (w workflowStub) ResumeWorkQueue(ctx context.Context) (workflow.WorkQueueStatus, error) {
	status := w.workQueue
	status.Paused = false
	return status, nil
}

func (w workflowStub) RetryFailedQueue(ctx context.Context) (workflow.BulkQueueRetryResult, error) {
	return w.retryAll, nil
}

func (w workflowStub) SearchLibrary(ctx context.Context, libraryItemID int64) (workflow.SearchResult, error) {
	return w.search, nil
}

func (w workflowStub) SelectRelease(ctx context.Context, releaseCandidateID int64) (workflow.ReleaseActionResult, error) {
	return w.selectr, nil
}

func (w workflowStub) RejectRelease(ctx context.Context, releaseCandidateID int64, reason string) (workflow.ReleaseActionResult, error) {
	return w.reject, nil
}

func (w workflowStub) RetryQueueItem(ctx context.Context, queueItemID int64) (workflow.QueueRetryResult, error) {
	return w.retry, nil
}

func (w workflowStub) ManageQueueItem(ctx context.Context, queueItemID int64, action string) (workflow.QueueManageResult, error) {
	return w.queueAct, nil
}

func (w workflowStub) ManageQueueItems(ctx context.Context, queueItemIDs []int64, action string) (workflow.BulkQueueRetryResult, error) {
	return w.queueBulk, nil
}

func (w workflowStub) ManageFailedQueue(ctx context.Context, action string) (workflow.BulkQueueRetryResult, error) {
	return w.retryAll, nil
}

func (w workflowStub) RestoreRelease(ctx context.Context, releaseCandidateID int64) (workflow.ReleaseActionResult, error) {
	return w.restore, nil
}

func (w workflowStub) RestoreRejectedReleases(ctx context.Context, libraryItemID int64) (database.RejectedReleaseRestoreResult, error) {
	return w.restoreAll, nil
}

func (w workflowStub) SkipRelease(ctx context.Context, releaseCandidateID int64) (workflow.ReleaseActionResult, error) {
	return w.skip, nil
}
func (w workflowStub) BackfillMetadata(_ context.Context) (workflow.BackfillMetadataResult, error) {
	return workflow.BackfillMetadataResult{}, nil
}
func (w workflowStub) ClearFailedQueue(_ context.Context) (int, error) { return 0, nil }
func (w workflowStub) FillMissingEpisodes(_ context.Context) (workflow.FillMissingEpisodesResult, error) {
	return workflow.FillMissingEpisodesResult{}, nil
}
func (w workflowStub) SearchUpgrades(_ context.Context) (workflow.UpgradeSearchResult, error) {
	return w.upgrades, nil
}
func (w workflowStub) ManualSearch(_ context.Context, _ string) ([]workflow.ManualSearchItem, error) {
	return nil, nil
}
func (w workflowStub) ResetLibraryItem(_ context.Context, _ int64) error { return nil }
func (w workflowStub) ResetOrphanedAvailableItems(_ context.Context) (workflow.ResetOrphanedAvailableItemsResult, error) {
	return workflow.ResetOrphanedAvailableItemsResult{}, nil
}

func (w workflowStub) PushMissingLibraryItemsToSeerr(_ context.Context) (workflow.PushMissingToSeerrResult, error) {
	return workflow.PushMissingToSeerrResult{}, nil
}
func (w workflowStub) SyncPlexDetectedShows(_ context.Context) (workflow.SyncPlexDetectedResult, error) {
	return workflow.SyncPlexDetectedResult{}, nil
}

func (w workflowStub) ImportNZBFromPush(_ context.Context, content []byte, filename, mediaType string) (string, error) {
	if w.importCall != nil {
		w.importCall.filename = filename
		w.importCall.mediaType = mediaType
		w.importCall.size = len(content)
	}
	return "item-42", nil
}

type sabRepoStub struct {
	lastQueueCategory   string
	lastHistoryCategory string
}

func (s *sabRepoStub) ListSabQueueItems(_ context.Context, category string, _, _ int) ([]database.SabQueueItem, int, error) {
	s.lastQueueCategory = category
	return nil, 0, nil
}

func (s *sabRepoStub) ListSabHistoryItems(_ context.Context, category string, _, _ int) ([]database.SabHistoryItem, int, error) {
	s.lastHistoryCategory = category
	return nil, 0, nil
}

func (s *sabRepoStub) DismissSabItems(_ context.Context, _ []int64) error { return nil }

func (p *publicationStub) RepublishLibraryItem(ctx context.Context, libraryItemID int64) error {
	p.republished = libraryItemID
	return nil
}

func (p *publicationStub) RepublishPendingLibrary(ctx context.Context) (library.BulkRepublishResult, error) {
	return p.pending, nil
}

func (maintenanceStub) DeepNZBHealthCheck(ctx context.Context) (maintenance.Result, error) {
	return maintenance.Result{TaskName: "nzb-health-check", ScannedRows: 4, ResetItems: 2}, nil
}

func (cacheStub) Prune(ctx context.Context) (cache.PruneResult, error) {
	return cache.PruneResult{Root: "/mnt/drakkar/cache/blocks", FilesBefore: 5, FilesAfter: 3, DeletedFiles: 2}, nil
}

func (p probeStub) Probe(ctx context.Context) (probe.Report, error) {
	return p.report, nil
}

func (s subtitleStub) ListSubtitles(ctx context.Context, libraryItemID int64) ([]database.SubtitleFileSummary, error) {
	return s.items, nil
}

func (s subtitleStub) ListCandidates(ctx context.Context, libraryItemID int64) ([]database.SubtitleCandidateSummary, error) {
	return s.candidates, nil
}

func (s subtitleStub) SearchCandidates(ctx context.Context, libraryItemID int64, languages []string) (intsub.SearchResult, error) {
	return s.search, nil
}

func (s subtitleStub) DownloadCandidate(ctx context.Context, candidateID int64) (intsub.UploadResult, error) {
	return s.download, nil
}

func (s subtitleStub) UploadSubtitle(ctx context.Context, libraryItemID int64, language, fileName string, src io.Reader) (intsub.UploadResult, error) {
	body, _ := io.ReadAll(src)
	if len(body) == 0 {
		return intsub.UploadResult{}, io.EOF
	}
	if s.upload.Language != "" || len(s.upload.CreatedPaths) > 0 || s.upload.Provider != "" {
		return s.upload, nil
	}
	return intsub.UploadResult{
		LibraryItemID: libraryItemID,
		Language:      language,
		Provider:      "manual",
		CreatedPaths:  []string{fileName},
	}, nil
}

func (b *blocklistStub) List(ctx context.Context) ([]database.BlocklistItemSummary, error) {
	return b.items, nil
}

func (b *blocklistStub) Clear(ctx context.Context, id int64) error {
	b.cleared = id
	return nil
}

func (b *blocklistStub) ClearAll(ctx context.Context) (database.BlocklistClearResult, error) {
	return b.all, nil
}

func (b *blocklistStub) ClearByReason(ctx context.Context, reason string) (database.BlocklistClearResult, error) {
	return database.BlocklistClearResult{Cleared: 0}, nil
}

func (b *blocklistStub) ListPaged(ctx context.Context, f database.BlocklistFilter) (database.BlocklistPage, error) {
	return database.BlocklistPage{Items: b.items}, nil
}

func (b *blocklistStub) Stats(ctx context.Context) (database.BlocklistStats, error) {
	return database.BlocklistStats{ByReason: map[string]int{}}, nil
}

func (b *blocklistStub) Create(ctx context.Context, item database.BlocklistMutation) (database.BlocklistItemSummary, error) {
	b.lastBody = item
	if b.created.ID != 0 {
		return b.created, nil
	}
	return database.BlocklistItemSummary{ID: 12, Key: item.Key, Reason: item.Reason}, nil
}

func (b *blocklistStub) Update(ctx context.Context, id int64, item database.BlocklistMutation) (database.BlocklistItemSummary, error) {
	b.lastBody = item
	if b.updated.ID != 0 {
		return b.updated, nil
	}
	return database.BlocklistItemSummary{ID: id, Key: item.Key, Reason: item.Reason}, nil
}

func (p *profilesStub) ListQualityProfiles(ctx context.Context) ([]database.QualityProfile, error) {
	return p.profiles, nil
}

func (p *profilesStub) UpsertQualityProfile(ctx context.Context, profile database.QualityProfile) (database.QualityProfile, error) {
	return profile, nil
}

func (p *profilesStub) DeleteQualityProfile(ctx context.Context, id int64) error { return nil }

func (p *profilesStub) ListQualityDefinitions(ctx context.Context) ([]database.QualityDefinition, error) {
	return nil, nil
}

func (p *profilesStub) UpdateQualityDefinition(ctx context.Context, d database.QualityDefinition) (database.QualityDefinition, error) {
	return d, nil
}

func (p *profilesStub) GetLibraryItemQualityProfile(ctx context.Context, libraryItemID int64) (*database.QualityProfile, error) {
	return nil, nil
}

func (p *profilesStub) SetLibraryItemQualityProfile(ctx context.Context, libraryItemID int64, profileID *int64) error {
	return nil
}

func (p *profilesStub) SetMediaRequestQualityProfile(ctx context.Context, requestID int64, profileID *int64) (int64, error) {
	p.lastRequestID = requestID
	p.lastProfileID = profileID
	return p.requestLibraryID, nil
}

func (p *profilesStub) GetGrabHistory(ctx context.Context, libraryItemID int64) ([]database.GrabHistoryEntry, error) {
	return nil, nil
}

func (p *profilesStub) ListCustomFormats(ctx context.Context) ([]database.CustomFormat, error) {
	return nil, nil
}

func (p *profilesStub) UpdateCustomFormat(ctx context.Context, f database.CustomFormat) (database.CustomFormat, error) {
	return f, nil
}

func (p *profilesStub) DeleteCustomFormat(ctx context.Context, id int64) error { return nil }

func (p *profilesStub) UpsertCustomFormat(ctx context.Context, f database.CustomFormat) (database.CustomFormat, error) {
	return f, nil
}

func (p *profilesStub) UpsertCustomFormatByName(ctx context.Context, f database.CustomFormat) (database.CustomFormat, error) {
	return f, nil
}

func (p *profilesStub) ListReleaseBlockRules(ctx context.Context) ([]database.ReleaseBlockRule, error) {
	return nil, nil
}

func (p *profilesStub) UpsertReleaseBlockRule(ctx context.Context, r database.ReleaseBlockRule) (database.ReleaseBlockRule, error) {
	return r, nil
}

func (p *profilesStub) UpdateReleaseBlockRule(ctx context.Context, r database.ReleaseBlockRule) (database.ReleaseBlockRule, error) {
	return r, nil
}

func (p *profilesStub) DeleteReleaseBlockRule(ctx context.Context, id int64) error { return nil }

func (p *profilesStub) ListIndexerPolicies(ctx context.Context) ([]database.IndexerPolicy, error) {
	return nil, nil
}
func (p *profilesStub) UpsertIndexerPolicy(ctx context.Context, pol database.IndexerPolicy) (database.IndexerPolicy, error) {
	return pol, nil
}
func (p *profilesStub) UpdateIndexerPolicy(ctx context.Context, pol database.IndexerPolicy) (database.IndexerPolicy, error) {
	return pol, nil
}
func (p *profilesStub) DeleteIndexerPolicy(ctx context.Context, id int64) error { return nil }

func (p *profilesStub) ListSubtitleProfiles(ctx context.Context) ([]database.SubtitleProfile, error) {
	return nil, nil
}
func (p *profilesStub) CreateSubtitleProfile(ctx context.Context, sp database.SubtitleProfile) (database.SubtitleProfile, error) {
	return sp, nil
}
func (p *profilesStub) UpdateSubtitleProfile(ctx context.Context, sp database.SubtitleProfile) (database.SubtitleProfile, error) {
	return sp, nil
}
func (p *profilesStub) DeleteSubtitleProfile(ctx context.Context, id int64) error { return nil }

func (p *profilesStub) SetTVShowMonitoringMode(ctx context.Context, tvShowID int64, mode string) error {
	return nil
}

func (p *profilesStub) ListSabQueueItems(ctx context.Context, category string, start, limit int) ([]database.SabQueueItem, int, error) {
	return nil, 0, nil
}

func (p *profilesStub) ListSabHistoryItems(ctx context.Context, category string, start, limit int) ([]database.SabHistoryItem, int, error) {
	return nil, 0, nil
}

func (p *profilesStub) DismissSabItems(ctx context.Context, libraryItemIDs []int64) error { return nil }

const sampleNZB = `<?xml version="1.0" encoding="UTF-8"?>
<nzb>
  <file subject="&quot;Dune (2021).mkv&quot;" poster="poster" date="1710000000">
    <groups><group>alt.binaries.movies</group></groups>
    <segments>
      <segment bytes="1000" number="1">&lt;msg1&gt;</segment>
    </segments>
  </file>
</nzb>`

func TestImportNZBEndpoint(t *testing.T) {
	queueSvc := queue.NewService(queue.NewMemoryRepository(), nzb.NewImporter(t.TempDir(), 1024*1024))
	router := Router(statusStub{}, queueSvc, nil, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/nzbs/import", strings.NewReader(sampleNZB))
	req.Header.Set("Content-Disposition", `attachment; filename="dune.nzb"`)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var item database.QueueSnapshot
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&item); err != nil {
		t.Fatal(err)
	}
	if item.State != database.QueuePreflight {
		t.Fatalf("unexpected state %s", item.State)
	}
}

func (s *subtitleStub) DeleteSubtitle(ctx context.Context, subtitleID int64) error {
	s.deleted = subtitleID
	return nil
}

func TestCancelNZBEndpoint(t *testing.T) {
	queueSvc := queue.NewService(queue.NewMemoryRepository(), nzb.NewImporter(t.TempDir(), 1024*1024))
	item, err := queueSvc.ImportNZB(context.Background(), "dune.nzb", strings.NewReader(sampleNZB))
	if err != nil {
		t.Fatal(err)
	}
	router := Router(statusStub{}, queueSvc, nil, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/nzbs/"+itoa(*item.NZBDocumentID), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	listReq := httptest.NewRequest(http.MethodGet, "/api/queue", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	body, _ := io.ReadAll(listRec.Body)
	if !strings.Contains(string(body), `"failureReason":"cancelled"`) {
		t.Fatalf("unexpected queue body %s", string(body))
	}
}

func TestQueueEndpointIncludesWorkQueueStatus(t *testing.T) {
	queueSvc := queue.NewService(queue.NewMemoryRepository(), nzb.NewImporter(t.TempDir(), 1024*1024))
	workflowSvc := workflowStub{workQueue: workflow.WorkQueueStatus{Paused: true, Depth: 7}}
	router := Router(statusStub{}, queueSvc, workflowSvc, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/queue", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"workQueue":{"paused":true,"depth":7}`) {
		t.Fatalf("unexpected queue payload %s", rec.Body.String())
	}
}

func TestQueuePauseResumeEndpoints(t *testing.T) {
	workflowSvc := workflowStub{workQueue: workflow.WorkQueueStatus{Paused: false, Depth: 3}}
	router := Router(statusStub{}, nil, workflowSvc, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	pauseReq := httptest.NewRequest(http.MethodPost, "/api/queue/pause", nil)
	pauseRec := httptest.NewRecorder()
	router.ServeHTTP(pauseRec, pauseReq)
	if pauseRec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 from pause, got %d: %s", pauseRec.Code, pauseRec.Body.String())
	}
	if !strings.Contains(pauseRec.Body.String(), `"paused":true`) {
		t.Fatalf("unexpected pause payload %s", pauseRec.Body.String())
	}

	resumeReq := httptest.NewRequest(http.MethodPost, "/api/queue/resume", nil)
	resumeRec := httptest.NewRecorder()
	router.ServeHTTP(resumeRec, resumeReq)
	if resumeRec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 from resume, got %d: %s", resumeRec.Code, resumeRec.Body.String())
	}
	if !strings.Contains(resumeRec.Body.String(), `"paused":false`) {
		t.Fatalf("unexpected resume payload %s", resumeRec.Body.String())
	}
}

func TestLibraryEndpoints(t *testing.T) {
	queueSvc := queue.NewService(queue.NewMemoryRepository(), nzb.NewImporter(t.TempDir(), 1024*1024))
	item, err := queueSvc.ImportNZB(context.Background(), "dune.nzb", strings.NewReader(sampleNZB))
	if err != nil {
		t.Fatal(err)
	}
	router := Router(statusStub{}, queueSvc, nil, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	libraryReq := httptest.NewRequest(http.MethodGet, "/api/library", nil)
	libraryRec := httptest.NewRecorder()
	router.ServeHTTP(libraryRec, libraryReq)
	if libraryRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", libraryRec.Code)
	}
	if !strings.Contains(libraryRec.Body.String(), `"title":"dune.nzb"`) {
		t.Fatalf("unexpected library body %s", libraryRec.Body.String())
	}

	releasesReq := httptest.NewRequest(http.MethodGet, "/api/releases/"+itoa(item.LibraryItemID), nil)
	releasesRec := httptest.NewRecorder()
	router.ServeHTTP(releasesRec, releasesReq)
	if releasesRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", releasesRec.Code)
	}
	if !strings.Contains(releasesRec.Body.String(), `"selected":true`) {
		t.Fatalf("unexpected releases body %s", releasesRec.Body.String())
	}

}

func TestStatusFromConfigIncludesIntegrationReadiness(t *testing.T) {
	rt := config.DefaultRuntime()
	cfg := config.Settings{
		Database: config.DatabaseConfig{Host: "postgres", Port: 5432, Name: "drakkar", Username: "drakkar", Password: "secret"},
		Valkey:   config.ValkeyConfig{Host: "valkey", Port: 6379},
		NZBHydra2: config.ServiceConfig{
			URL: "http://nzbhydra2:5076",
		},
		Seerr: config.ServiceConfig{
			URL: "http://seerr:5055",
		},
		Usenet: config.UsenetConfig{
			Providers: []config.UsenetProvider{
				{Name: "primary", Enabled: true, Host: "", Username: "", Password: "", Port: 563, MaxConnections: 20},
			},
		},
		Metadata: config.MetadataConfig{
			TMDB: config.APIKeyConfig{APIKey: "tmdb-key"},
		},
		Subtitles: config.SubtitlesConfig{
			Enabled:   true,
			Languages: []string{"en"},
			Providers: map[string]config.SubtitleAuth{
				"subdl": {Enabled: true},
			},
		},
	}

	status := StatusFromConfig(rt, cfg, time.Unix(1710000000, 0).UTC(), true)

	if !status.Integrations.TMDB.Configured {
		t.Fatalf("expected tmdb configured")
	}
	if status.Integrations.Seerr.Configured {
		t.Fatalf("expected seerr unconfigured without api key")
	}
	if status.Integrations.NZBHydra2.Configured {
		t.Fatalf("expected hydra unconfigured without api key")
	}
	if status.Integrations.Subtitles.Configured {
		t.Fatalf("expected subtitles unconfigured without provider credentials")
	}
	if status.Integrations.SubtitleProviders["subdl"].Configured {
		t.Fatalf("expected subdl unconfigured without api key")
	}
	if status.Integrations.Usenet.Configured {
		t.Fatalf("expected usenet unconfigured without host and credentials")
	}
}

func TestWorkflowEndpoints(t *testing.T) {
	queueSvc := queue.NewService(queue.NewMemoryRepository(), nzb.NewImporter(t.TempDir(), 1024*1024))
	workflowSvc := workflowStub{
		requests:   []database.MediaRequestSummary{{ID: 1, ExternalID: "123", RequestType: "movie", Title: "Dune", MediaType: "movie"}},
		sync:       workflow.SyncResult{Seen: 2, Created: 1},
		pending:    workflow.BulkSearchResult{Processed: 2, Searched: 2, Selected: 1, Failed: 0},
		retryAll:   workflow.BulkQueueRetryResult{Processed: 3, Retried: 2, Failed: 1},
		search:     workflow.SearchResult{LibraryItemID: 42, Query: "Dune 2021", CandidateCount: 3},
		selectr:    workflow.ReleaseActionResult{ReleaseCandidateID: 7, Action: "selected", SelectedReleaseID: func() *int64 { v := int64(88); return &v }()},
		reject:     workflow.ReleaseActionResult{ReleaseCandidateID: 8, Action: "rejected", SelectedReleaseID: func() *int64 { v := int64(89); return &v }()},
		restore:    workflow.ReleaseActionResult{ReleaseCandidateID: 8, Action: "restored"},
		restoreAll: database.RejectedReleaseRestoreResult{LibraryItemID: 42, Restored: 2},
		skip:       workflow.ReleaseActionResult{ReleaseCandidateID: 8, Action: "skipped", SelectedReleaseID: func() *int64 { v := int64(91); return &v }()},
		retry:      workflow.QueueRetryResult{QueueItemID: 4, Action: "retried_selected_release", SelectedReleaseID: func() *int64 { v := int64(90); return &v }()},
	}
	pub := &publicationStub{}
	pub.pending = library.BulkRepublishResult{Processed: 2, Republished: 1, Failed: 1}
	maint := maintenanceStub{}
	cacheSvc := cacheStub{}
	subtitles := &subtitleStub{
		items:      []database.SubtitleFileSummary{{ID: 1, LibraryItemID: 42, Provider: "manual", Language: "en", Path: "/mnt/drakkar/media/movies/Dune (2021) {tmdb-438631}/Dune (2021).en.srt", CreatedAt: time.Now().UTC()}},
		candidates: []database.SubtitleCandidateSummary{{ID: 3, LibraryItemID: 42, Provider: "subdl", Language: "en", Title: "Dune.2021.en.srt", ReleaseName: "Dune.2021.1080p.WEB-DL", Format: "srt", Score: 155, ExternalID: "file123", CreatedAt: time.Now().UTC()}},
		search: intsub.SearchResult{
			LibraryItemID:  42,
			CandidateCount: 1,
		},
		download: intsub.UploadResult{
			LibraryItemID: 42,
			Language:      "en",
			Provider:      "subdl",
			CreatedPaths:  []string{"/mnt/drakkar/media/movies/Dune (2021) {tmdb-438631}/Dune (2021).en.srt"},
		},
		upload: intsub.UploadResult{
			LibraryItemID: 42,
			Language:      "en",
			Provider:      "manual",
			CreatedPaths:  []string{"/mnt/drakkar/media/movies/Dune (2021) {tmdb-438631}/Dune (2021).en.srt"},
		},
	}
	blocklist := &blocklistStub{
		items: []database.BlocklistItemSummary{{ID: 9, Key: "external_url:http://example/blocked.nzb", Reason: "manual_reject"}},
		all:   database.BlocklistClearResult{Cleared: 1},
	}
	probes := probeStub{report: probe.Report{
		CheckedAt: time.Now().UTC(),
		Results: []probe.Result{
			{Name: "seerr", OK: true, Detail: "ok", CheckedAt: time.Now().UTC(), DurationMS: 12},
		},
	}}
	router := Router(statusStub{}, queueSvc, workflowSvc, pub, maint, cacheSvc, subtitles, blocklist, probes, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	requestsReq := httptest.NewRequest(http.MethodGet, "/api/requests", nil)
	requestsRec := httptest.NewRecorder()
	router.ServeHTTP(requestsRec, requestsReq)
	if requestsRec.Code != http.StatusOK || !strings.Contains(requestsRec.Body.String(), `"externalId":"123"`) {
		t.Fatalf("unexpected requests response %d %s", requestsRec.Code, requestsRec.Body.String())
	}

	syncReq := httptest.NewRequest(http.MethodPost, "/api/requests/sync", nil)
	syncRec := httptest.NewRecorder()
	router.ServeHTTP(syncRec, syncReq)
	if syncRec.Code != http.StatusAccepted || !strings.Contains(syncRec.Body.String(), `"created":1`) {
		t.Fatalf("unexpected sync response %d %s", syncRec.Code, syncRec.Body.String())
	}

	requestReq := httptest.NewRequest(http.MethodPost, "/api/discover/request", strings.NewReader(`{"mediaType":"tv","tmdbId":84958,"seasons":[2]}`))
	requestRec := httptest.NewRecorder()
	router.ServeHTTP(requestRec, requestReq)
	if requestRec.Code != http.StatusAccepted || !strings.Contains(requestRec.Body.String(), `"created":1`) {
		t.Fatalf("unexpected season request response %d %s", requestRec.Code, requestRec.Body.String())
	}

	pendingReq := httptest.NewRequest(http.MethodPost, "/api/library/search-pending", nil)
	pendingRec := httptest.NewRecorder()
	router.ServeHTTP(pendingRec, pendingReq)
	if pendingRec.Code != http.StatusAccepted || !strings.Contains(pendingRec.Body.String(), `"queued":true`) {
		t.Fatalf("unexpected pending search response %d %s", pendingRec.Code, pendingRec.Body.String())
	}

	retryReq := httptest.NewRequest(http.MethodPost, "/api/queue/4/retry", nil)
	retryRec := httptest.NewRecorder()
	router.ServeHTTP(retryRec, retryReq)
	if retryRec.Code != http.StatusAccepted || !strings.Contains(retryRec.Body.String(), `"action":"retried_selected_release"`) {
		t.Fatalf("unexpected retry response %d %s", retryRec.Code, retryRec.Body.String())
	}

	retryAllReq := httptest.NewRequest(http.MethodPost, "/api/queue/retry-failed", nil)
	retryAllRec := httptest.NewRecorder()
	router.ServeHTTP(retryAllRec, retryAllReq)
	if retryAllRec.Code != http.StatusAccepted || !strings.Contains(retryAllRec.Body.String(), `"processed":3`) {
		t.Fatalf("unexpected bulk retry response %d %s", retryAllRec.Code, retryAllRec.Body.String())
	}

	nzbHealthReq := httptest.NewRequest(http.MethodPost, "/api/maintenance/nzb-health-check", nil)
	nzbHealthRec := httptest.NewRecorder()
	router.ServeHTTP(nzbHealthRec, nzbHealthReq)
	if nzbHealthRec.Code != http.StatusAccepted || !strings.Contains(nzbHealthRec.Body.String(), `"queued":true`) {
		t.Fatalf("unexpected nzb health response %d %s", nzbHealthRec.Code, nzbHealthRec.Body.String())
	}

	searchReq := httptest.NewRequest(http.MethodPost, "/api/library/42/search", nil)
	searchRec := httptest.NewRecorder()
	router.ServeHTTP(searchRec, searchReq)
	if searchRec.Code != http.StatusAccepted || !strings.Contains(searchRec.Body.String(), `"queued":true`) {
		t.Fatalf("unexpected search response %d %s", searchRec.Code, searchRec.Body.String())
	}

	selectReq := httptest.NewRequest(http.MethodPost, "/api/releases/7/select", nil)
	selectRec := httptest.NewRecorder()
	router.ServeHTTP(selectRec, selectReq)
	if selectRec.Code != http.StatusAccepted || !strings.Contains(selectRec.Body.String(), `"action":"selected"`) {
		t.Fatalf("unexpected select response %d %s", selectRec.Code, selectRec.Body.String())
	}

	rejectReq := httptest.NewRequest(http.MethodPost, "/api/releases/8/reject", strings.NewReader(`{"reason":"bad_release"}`))
	rejectRec := httptest.NewRecorder()
	router.ServeHTTP(rejectRec, rejectReq)
	if rejectRec.Code != http.StatusAccepted || !strings.Contains(rejectRec.Body.String(), `"action":"rejected"`) {
		t.Fatalf("unexpected reject response %d %s", rejectRec.Code, rejectRec.Body.String())
	}

	restoreReq := httptest.NewRequest(http.MethodPost, "/api/releases/8/restore", nil)
	restoreRec := httptest.NewRecorder()
	router.ServeHTTP(restoreRec, restoreReq)
	if restoreRec.Code != http.StatusAccepted || !strings.Contains(restoreRec.Body.String(), `"action":"restored"`) {
		t.Fatalf("unexpected restore response %d %s", restoreRec.Code, restoreRec.Body.String())
	}

	restoreAllReq := httptest.NewRequest(http.MethodPost, "/api/library/42/restore-rejected", nil)
	restoreAllRec := httptest.NewRecorder()
	router.ServeHTTP(restoreAllRec, restoreAllReq)
	if restoreAllRec.Code != http.StatusAccepted || !strings.Contains(restoreAllRec.Body.String(), `"restored":2`) {
		t.Fatalf("unexpected restore-all response %d %s", restoreAllRec.Code, restoreAllRec.Body.String())
	}

	skipReq := httptest.NewRequest(http.MethodPost, "/api/releases/8/skip", nil)
	skipRec := httptest.NewRecorder()
	router.ServeHTTP(skipRec, skipReq)
	if skipRec.Code != http.StatusAccepted || !strings.Contains(skipRec.Body.String(), `"action":"skipped"`) {
		t.Fatalf("unexpected skip response %d %s", skipRec.Code, skipRec.Body.String())
	}

	republishReq := httptest.NewRequest(http.MethodPost, "/api/library/42/republish", nil)
	republishRec := httptest.NewRecorder()
	router.ServeHTTP(republishRec, republishReq)
	if republishRec.Code != http.StatusAccepted || pub.republished != 42 {
		t.Fatalf("unexpected republish response %d %s", republishRec.Code, republishRec.Body.String())
	}

	republishPendingReq := httptest.NewRequest(http.MethodPost, "/api/library/republish-pending", nil)
	republishPendingRec := httptest.NewRecorder()
	router.ServeHTTP(republishPendingRec, republishPendingReq)
	if republishPendingRec.Code != http.StatusAccepted || !strings.Contains(republishPendingRec.Body.String(), `"queued":true`) {
		t.Fatalf("unexpected bulk republish response %d %s", republishPendingRec.Code, republishPendingRec.Body.String())
	}

	cacheReq := httptest.NewRequest(http.MethodPost, "/api/cache/prune", nil)
	cacheRec := httptest.NewRecorder()
	router.ServeHTTP(cacheRec, cacheReq)
	if cacheRec.Code != http.StatusAccepted || !strings.Contains(cacheRec.Body.String(), `"queued":true`) {
		t.Fatalf("unexpected cache response %d %s", cacheRec.Code, cacheRec.Body.String())
	}

	subtitleListReq := httptest.NewRequest(http.MethodGet, "/api/subtitles/42", nil)
	subtitleListRec := httptest.NewRecorder()
	router.ServeHTTP(subtitleListRec, subtitleListReq)
	if subtitleListRec.Code != http.StatusOK || !strings.Contains(subtitleListRec.Body.String(), `"language":"en"`) {
		t.Fatalf("unexpected subtitle list response %d %s", subtitleListRec.Code, subtitleListRec.Body.String())
	}

	subtitleCandidateListReq := httptest.NewRequest(http.MethodGet, "/api/subtitle-candidates/42", nil)
	subtitleCandidateListRec := httptest.NewRecorder()
	router.ServeHTTP(subtitleCandidateListRec, subtitleCandidateListReq)
	if subtitleCandidateListRec.Code != http.StatusOK || !strings.Contains(subtitleCandidateListRec.Body.String(), `"provider":"subdl"`) {
		t.Fatalf("unexpected subtitle candidate list response %d %s", subtitleCandidateListRec.Code, subtitleCandidateListRec.Body.String())
	}

	subtitleSearchReq := httptest.NewRequest(http.MethodPost, "/api/subtitles/42/search", strings.NewReader(`{"languages":["en"]}`))
	subtitleSearchRec := httptest.NewRecorder()
	router.ServeHTTP(subtitleSearchRec, subtitleSearchReq)
	if subtitleSearchRec.Code != http.StatusAccepted || !strings.Contains(subtitleSearchRec.Body.String(), `"queued":true`) {
		t.Fatalf("unexpected subtitle search response %d %s", subtitleSearchRec.Code, subtitleSearchRec.Body.String())
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("language", "en")
	part, err := writer.CreateFormFile("file", "subtitle.srt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(part, strings.NewReader("1\n00:00:01,000 --> 00:00:02,000\nHello\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	subtitleUploadReq := httptest.NewRequest(http.MethodPost, "/api/subtitles/42/upload", &body)
	subtitleUploadReq.Header.Set("Content-Type", writer.FormDataContentType())
	subtitleUploadRec := httptest.NewRecorder()
	router.ServeHTTP(subtitleUploadRec, subtitleUploadReq)
	if subtitleUploadRec.Code != http.StatusCreated || !strings.Contains(subtitleUploadRec.Body.String(), `"provider":"manual"`) {
		t.Fatalf("unexpected subtitle upload response %d %s", subtitleUploadRec.Code, subtitleUploadRec.Body.String())
	}

	subtitleDownloadReq := httptest.NewRequest(http.MethodPost, "/api/subtitle-candidates/3/download", nil)
	subtitleDownloadRec := httptest.NewRecorder()
	router.ServeHTTP(subtitleDownloadRec, subtitleDownloadReq)
	if subtitleDownloadRec.Code != http.StatusCreated || !strings.Contains(subtitleDownloadRec.Body.String(), `"provider":"subdl"`) {
		t.Fatalf("unexpected subtitle download response %d %s", subtitleDownloadRec.Code, subtitleDownloadRec.Body.String())
	}

	subtitleDeleteReq := httptest.NewRequest(http.MethodDelete, "/api/subtitle-files/1", nil)
	subtitleDeleteRec := httptest.NewRecorder()
	router.ServeHTTP(subtitleDeleteRec, subtitleDeleteReq)
	if subtitleDeleteRec.Code != http.StatusOK || !strings.Contains(subtitleDeleteRec.Body.String(), `"status":"deleted"`) || subtitles.deleted != 1 {
		t.Fatalf("unexpected subtitle delete response %d %s deleted=%d", subtitleDeleteRec.Code, subtitleDeleteRec.Body.String(), subtitles.deleted)
	}

	blocklistReq := httptest.NewRequest(http.MethodGet, "/api/blocklist", nil)
	blocklistRec := httptest.NewRecorder()
	router.ServeHTTP(blocklistRec, blocklistReq)
	if blocklistRec.Code != http.StatusOK || !strings.Contains(blocklistRec.Body.String(), `"manual_reject"`) {
		t.Fatalf("unexpected blocklist response %d %s", blocklistRec.Code, blocklistRec.Body.String())
	}

	blocklistClearAllReq := httptest.NewRequest(http.MethodDelete, "/api/blocklist", nil)
	blocklistClearAllRec := httptest.NewRecorder()
	router.ServeHTTP(blocklistClearAllRec, blocklistClearAllReq)
	if blocklistClearAllRec.Code != http.StatusOK || !strings.Contains(blocklistClearAllRec.Body.String(), `"cleared":1`) {
		t.Fatalf("unexpected blocklist clear-all response %d %s", blocklistClearAllRec.Code, blocklistClearAllRec.Body.String())
	}

	clearReq := httptest.NewRequest(http.MethodDelete, "/api/blocklist/9", nil)
	clearRec := httptest.NewRecorder()
	router.ServeHTTP(clearRec, clearReq)
	if clearRec.Code != http.StatusOK || blocklist.cleared != 9 {
		t.Fatalf("unexpected blocklist clear response %d %s", clearRec.Code, clearRec.Body.String())
	}

	probeReq := httptest.NewRequest(http.MethodPost, "/api/integrations/probe", nil)
	probeRec := httptest.NewRecorder()
	router.ServeHTTP(probeRec, probeReq)
	if probeRec.Code != http.StatusAccepted || !strings.Contains(probeRec.Body.String(), `"name":"seerr"`) {
		t.Fatalf("unexpected probe response %d %s", probeRec.Code, probeRec.Body.String())
	}
}

func TestSABAPIAddFileAliasAcceptsLowercaseFieldAndNzbname(t *testing.T) {
	importCall := &sabImportCall{}
	workflowSvc := workflowStub{importCall: importCall}
	router := Router(statusStub{}, nil, workflowSvc, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("mode", "addfile"); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteField("nzbname", "Expected Folder"); err != nil {
		t.Fatal(err)
	}
	part, err := writer.CreateFormFile("nzbfile", "ignored-upload-name.nzb")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(part, strings.NewReader(sampleNZB)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/dav/api?category=movies", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"item-42"`) {
		t.Fatalf("expected nzo id in response, got %s", rec.Body.String())
	}
	if importCall.filename != "Expected Folder.nzb" {
		t.Fatalf("expected nzbname override, got %q", importCall.filename)
	}
	if importCall.mediaType != "movie" {
		t.Fatalf("expected movie media type, got %q", importCall.mediaType)
	}
	if importCall.size == 0 {
		t.Fatal("expected uploaded nzb content to be passed through")
	}
}

func TestSABAPIHistoryAcceptsCategoryAlias(t *testing.T) {
	repo := &sabRepoStub{}
	handler := &sabHandler{repo: repo}

	req := httptest.NewRequest(http.MethodGet, "/dav/api?mode=history&category=movies", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if repo.lastHistoryCategory != "movies" {
		t.Fatalf("expected history category movies, got %q", repo.lastHistoryCategory)
	}
}

func TestQueueActionEndpoint(t *testing.T) {
	workflowSvc := workflowStub{
		queueAct: workflow.QueueManageResult{QueueItemID: 4, Action: "remove_blocklist_and_search"},
	}
	router := Router(statusStub{}, nil, workflowSvc, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/queue/4/action", strings.NewReader(`{"action":"remove_blocklist_and_search"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"remove_blocklist_and_search"`) {
		t.Fatalf("unexpected response %s", rec.Body.String())
	}
}

func TestQueueBulkActionEndpoint(t *testing.T) {
	workflowSvc := workflowStub{
		queueBulk: workflow.BulkQueueRetryResult{Processed: 2, Retried: 2, Failed: 0, ProcessedQueues: []int64{4, 5}},
	}
	router := Router(statusStub{}, nil, workflowSvc, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/queue/bulk-action", strings.NewReader(`{"queueItemIds":[4,5],"action":"remove_and_blocklist"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"processed":2`) {
		t.Fatalf("unexpected response %s", rec.Body.String())
	}
}

func TestRequestProfileEndpoint(t *testing.T) {
	profiles := &profilesStub{requestLibraryID: 42}
	router := Router(statusStub{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, profiles, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPut, "/api/requests/7/profile", strings.NewReader(`{"profileId":3}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if profiles.lastRequestID != 7 {
		t.Fatalf("expected request id 7, got %d", profiles.lastRequestID)
	}
	if profiles.lastProfileID == nil || *profiles.lastProfileID != 3 {
		t.Fatalf("expected profile id 3, got %#v", profiles.lastProfileID)
	}
}

func TestSearchUpgradesEndpoint(t *testing.T) {
	workflowSvc := workflowStub{
		upgrades: workflow.UpgradeSearchResult{Checked: 4, Upgraded: 2, Failed: 1},
	}
	router := Router(statusStub{}, nil, workflowSvc, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/library/search-upgrades", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"queued":true`) {
		t.Fatalf("unexpected response %s", rec.Body.String())
	}
}

func TestManualBlocklistCreateEndpoint(t *testing.T) {
	blocklist := &blocklistStub{
		created: database.BlocklistItemSummary{ID: 21, Key: "external_url:https://example.invalid/a.nzb", Reason: "manual"},
	}
	router := Router(statusStub{}, nil, nil, nil, nil, nil, nil, blocklist, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/blocklist/manual", strings.NewReader(`{"keyType":"external_url","externalUrl":"https://example.invalid/a.nzb","reason":"manual"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if blocklist.lastBody.Key != "external_url:https://example.invalid/a.nzb" {
		t.Fatalf("expected normalized external_url key, got %q", blocklist.lastBody.Key)
	}
}

func TestManualBlocklistUpdateEndpoint(t *testing.T) {
	blocklist := &blocklistStub{
		updated: database.BlocklistItemSummary{ID: 9, Key: "release_signature:dune 2021|nzb finder|7000|2026-06-14", Reason: "manual"},
	}
	router := Router(statusStub{}, nil, nil, nil, nil, nil, nil, blocklist, nil, nil, NewEventBroker(), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPut, "/api/blocklist/9", strings.NewReader(`{"keyType":"raw","key":"release_signature:dune 2021|nzb finder|7000|2026-06-14","reason":"manual"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if blocklist.lastBody.Key == "" {
		t.Fatal("expected update payload to include a key")
	}
}

func itoa(value int64) string {
	return strconv.FormatInt(value, 10)
}

func TestCustomFormatsImportEndpoint(t *testing.T) {
	profiles := &profilesStub{}
	router := Router(statusStub{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, profiles, nil, nil, nil, nil, nil, nil)

	body := `[{"name":"BluRay","pattern":"(?i)bluray","score":50,"enabled":true}]`
	req := httptest.NewRequest(http.MethodPost, "/api/custom-formats/import", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestIndexerPoliciesEndpoints(t *testing.T) {
	profiles := &profilesStub{}
	router := Router(statusStub{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, profiles, nil, nil, nil, nil, nil, nil)

	// GET list
	req := httptest.NewRequest(http.MethodGet, "/api/indexer-policies", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/indexer-policies expected 200, got %d", rec.Code)
	}

	// POST create
	req = httptest.NewRequest(http.MethodPost, "/api/indexer-policies", strings.NewReader(`{"indexerName":"NZBGeek","scoreModifier":25,"enabled":true,"note":""}`))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/indexer-policies expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// DELETE
	req = httptest.NewRequest(http.MethodDelete, "/api/indexer-policies/1", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE /api/indexer-policies/1 expected 200, got %d", rec.Code)
	}
}

func TestSubtitleProfilesEndpoints(t *testing.T) {
	profiles := &profilesStub{}
	router := Router(statusStub{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, NewEventBroker(), nil, nil, profiles, nil, nil, nil, nil, nil, nil)

	// GET list
	req := httptest.NewRequest(http.MethodGet, "/api/subtitle-profiles", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/subtitle-profiles expected 200, got %d", rec.Code)
	}

	// POST create
	req = httptest.NewRequest(http.MethodPost, "/api/subtitle-profiles",
		strings.NewReader(`{"name":"English Only","languages":["en"],"preferHearingImpaired":false,"requireExactLanguage":false,"isDefault":false}`))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/subtitle-profiles expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}
