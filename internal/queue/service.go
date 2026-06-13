package queue

import (
	"context"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/nzb"
	"github.com/hjongedijk/drakkar/internal/stream"
)

type Repository interface {
	ListQueue(ctx context.Context) ([]database.QueueSnapshot, error)
	ListLibraryItems(ctx context.Context) ([]database.LibraryItemSummary, error)
	ListReleaseSummaries(ctx context.Context, libraryItemID int64) ([]database.ReleaseSummary, error)
	ListNZBMountEntries(ctx context.Context) ([]database.NZBMountEntry, error)
	ListContentMountEntries(ctx context.Context) ([]database.ContentMountEntry, error)
	ListContentMountEntriesForRelease(ctx context.Context, selectedReleaseID int64) ([]database.ContentMountEntry, error)
	ListCompletedSymlinkEntries(ctx context.Context) ([]database.CompletedSymlinkEntry, error)
	OpenVirtualMediaFile(ctx context.Context, virtualFileID int64) (stream.VirtualMediaFile, error)
	CreateImportedNZB(ctx context.Context, imported database.ImportedNZB) (database.QueueSnapshot, error)
	SetImportedNZBIndexed(ctx context.Context, queueItemID int64) error
	CancelNZBDocument(ctx context.Context, nzbDocumentID int64) error
}

type Service struct {
	repo           Repository
	importer       *nzb.Importer
	postImportHook func(context.Context, database.QueueSnapshot) error
}

func NewService(repo Repository, importer *nzb.Importer) *Service {
	return &Service{
		repo:     repo,
		importer: importer,
	}
}

func (s *Service) SetPostImportHook(fn func(context.Context, database.QueueSnapshot) error) {
	s.postImportHook = fn
}

func (s *Service) ListQueue(ctx context.Context) ([]database.QueueSnapshot, error) {
	return s.repo.ListQueue(ctx)
}

func (s *Service) ListLibraryItems(ctx context.Context) ([]database.LibraryItemSummary, error) {
	return s.repo.ListLibraryItems(ctx)
}

func (s *Service) ListReleaseSummaries(ctx context.Context, libraryItemID int64) ([]database.ReleaseSummary, error) {
	return s.repo.ListReleaseSummaries(ctx, libraryItemID)
}

func (s *Service) ListNZBMountEntries(ctx context.Context) ([]database.NZBMountEntry, error) {
	return s.repo.ListNZBMountEntries(ctx)
}

func (s *Service) ListContentMountEntries(ctx context.Context) ([]database.ContentMountEntry, error) {
	return s.repo.ListContentMountEntries(ctx)
}

func (s *Service) ListContentMountEntriesForRelease(ctx context.Context, selectedReleaseID int64) ([]database.ContentMountEntry, error) {
	return s.repo.ListContentMountEntriesForRelease(ctx, selectedReleaseID)
}

func (s *Service) OpenVirtualMediaFile(ctx context.Context, virtualFileID int64) (stream.VirtualMediaFile, error) {
	return s.repo.OpenVirtualMediaFile(ctx, virtualFileID)
}

func (s *Service) ListCompletedSymlinkEntries(ctx context.Context) ([]database.CompletedSymlinkEntry, error) {
	return s.repo.ListCompletedSymlinkEntries(ctx)
}

func (s *Service) ImportNZB(ctx context.Context, fileName string, src io.Reader) (database.QueueSnapshot, error) {
	item, err := s.importer.Import(ctx, s.repo, fileName, src)
	if err != nil {
		return database.QueueSnapshot{}, err
	}
	if s.postImportHook != nil {
		if err := s.postImportHook(ctx, item); err != nil {
			return database.QueueSnapshot{}, err
		}
	}
	return item, nil
}

func (s *Service) ImportNZBPath(ctx context.Context, fileName, path string) (database.QueueSnapshot, error) {
	item, err := s.importer.ImportPath(ctx, s.repo, fileName, path)
	if err != nil {
		return database.QueueSnapshot{}, err
	}
	if s.postImportHook != nil {
		if err := s.postImportHook(ctx, item); err != nil {
			return database.QueueSnapshot{}, err
		}
	}
	return item, nil
}

func (s *Service) CancelNZB(ctx context.Context, nzbDocumentID int64) error {
	return s.repo.CancelNZBDocument(ctx, nzbDocumentID)
}

func (s *Service) CancelNZBDocument(ctx context.Context, nzbDocumentID int64) error {
	return s.repo.CancelNZBDocument(ctx, nzbDocumentID)
}

type MemoryRepository struct {
	items       []database.QueueSnapshot
	mountByID   map[int64]database.NZBMountEntry
	content     []database.ContentMountEntry
	contentData map[int64][]byte
	completed   []database.CompletedSymlinkEntry
	requests    []database.MediaRequestSummary
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		mountByID:   make(map[int64]database.NZBMountEntry),
		contentData: make(map[int64][]byte),
	}
}

func (m *MemoryRepository) ListQueue(ctx context.Context) ([]database.QueueSnapshot, error) {
	out := make([]database.QueueSnapshot, len(m.items))
	copy(out, m.items)
	slices.SortStableFunc(out, func(a, b database.QueueSnapshot) int {
		if ra, rb := queueStateRank(a.State), queueStateRank(b.State); ra != rb {
			return ra - rb
		}
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			if a.UpdatedAt.After(b.UpdatedAt) {
				return -1
			}
			return 1
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			if a.CreatedAt.After(b.CreatedAt) {
				return -1
			}
			return 1
		}
		switch {
		case a.QueueItemID > b.QueueItemID:
			return -1
		case a.QueueItemID < b.QueueItemID:
			return 1
		default:
			return 0
		}
	})
	return out, nil
}

func queueStateRank(state database.QueueState) int {
	switch state {
	case database.QueueFetchingNZB:
		return 0
	case database.QueueIndexing:
		return 1
	case database.QueuePreflight:
		return 2
	case database.QueuePublishing:
		return 3
	case database.QueueSelected:
		return 4
	case database.QueueRanking:
		return 5
	case database.QueueSearching:
		return 6
	case database.QueueRequested:
		return 7
	case database.QueueAvailable:
		return 8
	case database.QueueFailed:
		return 9
	default:
		return 10
	}
}

func (m *MemoryRepository) ListLibraryItems(ctx context.Context) ([]database.LibraryItemSummary, error) {
	out := make([]database.LibraryItemSummary, 0, len(m.items))
	for _, item := range m.items {
		out = append(out, database.LibraryItemSummary{
			ID:                item.LibraryItemID,
			MediaType:         "manual_nzb",
			Title:             item.LibraryTitle,
			Available:         item.State == database.QueueAvailable,
			RequestedAt:       item.CreatedAt,
			QueueState:        item.State,
			FailureReason:     item.FailureReason,
			SelectedReleaseID: item.SelectedRelease,
		})
	}
	return out, nil
}

func (m *MemoryRepository) ListMediaRequests(ctx context.Context) ([]database.MediaRequestSummary, error) {
	out := make([]database.MediaRequestSummary, len(m.requests))
	copy(out, m.requests)
	return out, nil
}

func (m *MemoryRepository) UpsertMovieRequest(ctx context.Context, externalID string, tmdbID int64, title string, year int) (int64, bool, error) {
	for _, item := range m.requests {
		if item.ExternalID == externalID && item.RequestType == "movie" && item.LibraryItemID != nil {
			return *item.LibraryItemID, false, nil
		}
	}
	id := int64(len(m.items) + len(m.requests) + 4000)
	m.requests = append(m.requests, database.MediaRequestSummary{
		ID:          int64(len(m.requests) + 1),
		ExternalID:  externalID,
		RequestType: "movie",
		Title:       title,
		MediaType:   "movie",
		LibraryItemID: func() *int64 {
			value := id
			return &value
		}(),
		QueueState: database.QueueRequested,
	})
	m.items = append(m.items, database.QueueSnapshot{
		QueueItemID:    int64(len(m.items) + 1),
		LibraryItemID:  id,
		LibraryTitle:   title,
		State:          database.QueueRequested,
		IdempotencyKey: "seerr-movie-" + externalID,
	})
	return id, true, nil
}

func (m *MemoryRepository) UpsertEpisodeRequest(ctx context.Context, externalID string, tvdbID, tmdbID int64, show string, year, season, episode int, episodeTitle string) (int64, bool, error) {
	for _, item := range m.requests {
		if item.ExternalID == externalID && item.RequestType == "tv" && item.LibraryItemID != nil {
			return *item.LibraryItemID, false, nil
		}
	}
	id := int64(len(m.items) + len(m.requests) + 5000)
	title := show
	if season > 0 && episode > 0 {
		title = show + " S" + twoDigit(season) + "E" + twoDigit(episode)
	}
	m.requests = append(m.requests, database.MediaRequestSummary{
		ID:          int64(len(m.requests) + 1),
		ExternalID:  externalID,
		RequestType: "tv",
		Title:       title,
		MediaType:   "episode",
		LibraryItemID: func() *int64 {
			value := id
			return &value
		}(),
		QueueState: database.QueueRequested,
	})
	m.items = append(m.items, database.QueueSnapshot{
		QueueItemID:    int64(len(m.items) + 1),
		LibraryItemID:  id,
		LibraryTitle:   title,
		State:          database.QueueRequested,
		IdempotencyKey: "seerr-tv-" + externalID,
	})
	return id, true, nil
}

func (m *MemoryRepository) GetLibrarySearchInput(ctx context.Context, libraryItemID int64) (database.LibrarySearchInput, error) {
	for _, item := range m.items {
		if item.LibraryItemID != libraryItemID {
			continue
		}
		return database.LibrarySearchInput{
			LibraryItemID: libraryItemID,
			MediaType:     "movie",
			Title:         item.LibraryTitle,
		}, nil
	}
	return database.LibrarySearchInput{}, errors.New("library item not found")
}

func (m *MemoryRepository) ReplaceSearchCandidates(ctx context.Context, libraryItemID int64, candidates []database.SearchCandidateRecord) (*int64, error) {
	for i := range m.items {
		if m.items[i].LibraryItemID != libraryItemID {
			continue
		}
		selectedIndex := -1
		for j, candidate := range candidates {
			if !candidate.Rejected {
				selectedIndex = j
				break
			}
		}
		if len(candidates) == 0 || selectedIndex < 0 {
			m.items[i].State = database.QueueFailed
			m.items[i].FailureReason = "no_releases"
			m.items[i].SelectedRelease = nil
			return nil, nil
		}
		selected := libraryItemID + 9000
		m.items[i].State = database.QueueSelected
		m.items[i].SelectedRelease = &selected
		return &selected, nil
	}
	return nil, errors.New("library item not found")
}

func (m *MemoryRepository) ListReleaseSummaries(ctx context.Context, libraryItemID int64) ([]database.ReleaseSummary, error) {
	for _, item := range m.items {
		if item.LibraryItemID != libraryItemID || item.SelectedRelease == nil {
			continue
		}
		return []database.ReleaseSummary{{
			SelectedReleaseID:  *item.SelectedRelease,
			ReleaseCandidateID: *item.SelectedRelease - 1000,
			LibraryItemID:      item.LibraryItemID,
			Title:              item.LibraryTitle,
			Score:              0,
			Selected:           true,
			Rejected:           false,
			RejectReason:       "",
			FailureCount:       0,
			LastFailureReason:  "",
			CreatedAt:          item.CreatedAt,
			NZBDocumentID:      item.NZBDocumentID,
			NZBFileName:        item.NZBFileName,
		}}, nil
	}
	return []database.ReleaseSummary{}, nil
}

func (m *MemoryRepository) ListNZBMountEntries(ctx context.Context) ([]database.NZBMountEntry, error) {
	out := make([]database.NZBMountEntry, 0, len(m.items))
	for _, item := range m.items {
		if item.NZBDocumentID == nil || item.State == database.QueueFailed || item.State == database.QueueAvailable {
			continue
		}
		out = append(out, m.mountByID[*item.NZBDocumentID])
	}
	return out, nil
}

func (m *MemoryRepository) ListContentMountEntries(ctx context.Context) ([]database.ContentMountEntry, error) {
	out := make([]database.ContentMountEntry, len(m.content))
	copy(out, m.content)
	return out, nil
}

func (m *MemoryRepository) ListContentMountEntriesForRelease(ctx context.Context, selectedReleaseID int64) ([]database.ContentMountEntry, error) {
	var out []database.ContentMountEntry
	for _, item := range m.content {
		if item.SelectedReleaseID == selectedReleaseID {
			out = append(out, item)
		}
	}
	return out, nil
}

func (m *MemoryRepository) OpenVirtualMediaFile(ctx context.Context, virtualFileID int64) (stream.VirtualMediaFile, error) {
	for _, item := range m.content {
		if item.VirtualFileID == virtualFileID {
			return stream.NewByteVirtualFile(item.FileName, m.contentData[virtualFileID]), nil
		}
	}
	return nil, errors.New("virtual file not found")
}

func (m *MemoryRepository) ListCompletedSymlinkEntries(ctx context.Context) ([]database.CompletedSymlinkEntry, error) {
	out := make([]database.CompletedSymlinkEntry, len(m.completed))
	copy(out, m.completed)
	return out, nil
}

func (m *MemoryRepository) CreateImportedNZB(ctx context.Context, imported database.ImportedNZB) (database.QueueSnapshot, error) {
	for _, item := range m.items {
		if item.IdempotencyKey == imported.IdempotencyKey {
			return item, nil
		}
	}
	id := int64(len(m.items) + 1)
	selected := id + 1000
	document := id + 2000
	item := database.QueueSnapshot{
		QueueItemID:     id,
		LibraryItemID:   id + 3000,
		LibraryTitle:    imported.FileName,
		State:           database.QueueIndexing,
		IdempotencyKey:  imported.IdempotencyKey,
		SelectedRelease: &selected,
		NZBDocumentID:   &document,
		NZBFileName:     imported.FileName,
		NZBFileCount:    imported.FileCount,
		NZBSegmentCount: imported.SegmentCount,
	}
	m.items = append(m.items, item)
	m.mountByID[document] = database.NZBMountEntry{
		DocumentID: document,
		FileName:   imported.FileName,
		XML:        imported.XML,
		State:      item.State,
	}
	return item, nil
}

func (m *MemoryRepository) SetImportedNZBIndexed(ctx context.Context, queueItemID int64) error {
	for i := range m.items {
		if m.items[i].QueueItemID == queueItemID {
			m.items[i].State = database.QueuePreflight
			if m.items[i].NZBDocumentID != nil {
				entry := m.mountByID[*m.items[i].NZBDocumentID]
				entry.State = database.QueuePreflight
				m.mountByID[*m.items[i].NZBDocumentID] = entry
			}
			return nil
		}
	}
	return errors.New("queue item not found")
}

func (m *MemoryRepository) CancelNZBDocument(ctx context.Context, nzbDocumentID int64) error {
	for i := range m.items {
		if m.items[i].NZBDocumentID != nil && *m.items[i].NZBDocumentID == nzbDocumentID {
			m.items[i].State = database.QueueFailed
			m.items[i].FailureReason = "cancelled"
			delete(m.mountByID, nzbDocumentID)
			return nil
		}
	}
	return errors.New("nzb document not found")
}

func DetectUploadName(headerName, contentDisposition string) string {
	if strings.TrimSpace(headerName) != "" {
		return nzb.ImportHTTPFileName(headerName)
	}
	return nzb.ImportRawBodyName(contentDisposition)
}

func (m *MemoryRepository) AddInlineContent(selectedReleaseID, virtualFileID int64, path, fileName string, data []byte) {
	m.content = append(m.content, database.ContentMountEntry{
		VirtualFileID:     virtualFileID,
		SelectedReleaseID: selectedReleaseID,
		Path:              path,
		FileName:          fileName,
		SizeBytes:         int64(len(data)),
		ReaderKind:        "inline",
	})
	clone := make([]byte, len(data))
	copy(clone, data)
	m.contentData[virtualFileID] = clone
}

func (m *MemoryRepository) AddCompletedSymlink(publicationID int64, name, targetPath string) {
	m.completed = append(m.completed, database.CompletedSymlinkEntry{
		PublicationID: publicationID,
		Name:          name,
		TargetPath:    targetPath,
	})
}

func twoDigit(value int) string {
	if value < 10 {
		return "0" + strconv.Itoa(value)
	}
	return strconv.Itoa(value)
}
