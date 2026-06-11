package database

import "time"

type QueueState string

const (
	QueueRequested   QueueState = "requested"
	QueueSearching   QueueState = "searching"
	QueueRanking     QueueState = "ranking"
	QueueSelected    QueueState = "selected"
	QueueFetchingNZB QueueState = "fetching_nzb"
	QueueIndexing    QueueState = "indexing"
	QueuePreflight   QueueState = "preflight"
	QueuePublishing  QueueState = "publishing"
	QueueAvailable   QueueState = "available"
	QueueDegraded    QueueState = "degraded"
	QueueFailed      QueueState = "failed"
)

type QueueItem struct {
	ID              int64
	LibraryItemID   int64
	State           QueueState
	FailureReason   string
	IdempotencyKey  string
	SelectedRelease *int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type QueueSnapshot struct {
	QueueItemID     int64      `json:"queueItemId"`
	LibraryItemID   int64      `json:"libraryItemId"`
	LibraryTitle    string     `json:"libraryTitle"`
	State           QueueState `json:"state"`
	FailureReason   string     `json:"failureReason"`
	IdempotencyKey  string     `json:"idempotencyKey"`
	SelectedRelease *int64     `json:"selectedReleaseId,omitempty"`
	NZBDocumentID   *int64     `json:"nzbDocumentId,omitempty"`
	NZBFileName     string     `json:"nzbFileName,omitempty"`
	NZBFileCount    int        `json:"nzbFileCount"`
	NZBSegmentCount int        `json:"nzbSegmentCount"`
	CreatedAt       time.Time  `json:"createdAt"`
	UpdatedAt       time.Time  `json:"updatedAt"`
}

type ImportedNZB struct {
	FileName       string
	XML            []byte
	ExternalURL    string
	IdempotencyKey string
	FileCount      int
	SegmentCount   int
	Files          []ImportedNZBFile
	Archives       []ImportedArchive
}

type ImportedNZBFile struct {
	FileName      string
	Subject       string
	Poster        string
	PostedUnix    int64
	FileSizeBytes int64
	Segments      []ImportedNZBSegment
}

type ImportedNZBSegment struct {
	Number             int
	MessageID          string
	EncodedSizeBytes   int64
	DecodedStartOffset int64
	DecodedEndOffset   int64
}

type ImportedArchive struct {
	Kind         string
	Status       string
	RejectReason string
	Volumes      []ImportedArchiveVolume
	Entries      []ImportedArchiveEntry
}

type ImportedArchiveVolume struct {
	Path        string
	VolumeIndex int
}

type ImportedArchiveEntry struct {
	Path              string
	SizeBytes         int64
	PackedSizeBytes   int64
	CompressionMethod string
	Encrypted         bool
	Solid             bool
	VolumeIndex       int
	ArchiveOffset     int64
	Ranges            []ImportedArchiveRange
}

type ImportedArchiveRange struct {
	VolumeIndex   int
	EntryOffset   int64
	ArchiveOffset int64
	LengthBytes   int64
}

type NZBMountEntry struct {
	DocumentID int64
	FileName   string
	XML        []byte
	State      QueueState
}

type ContentMountEntry struct {
	VirtualFileID     int64
	SelectedReleaseID int64
	Path              string
	FileName          string
	SizeBytes         int64
	ReaderKind        string
}

type ReleaseVirtualFile struct {
	VirtualFileID     int64
	SelectedReleaseID int64
	LibraryItemID     int64
	MediaType         string
	Path              string
	FileName          string
	MovieTitle        string
	MovieYear         int
	MovieTMDBID       int64
	ShowTitle         string
	ShowYear          int
	ShowTVDBID        int64
	SeasonNumber      int
	EpisodeNumber     int
}

type CompletedSymlinkEntry struct {
	PublicationID int64
	Name          string
	TargetPath    string
}

type LibraryItemSummary struct {
	ID                int64      `json:"id"`
	MediaType         string     `json:"mediaType"`
	Title             string     `json:"title"`
	Available         bool       `json:"available"`
	RequestedAt       time.Time  `json:"requestedAt"`
	QueueState        QueueState `json:"queueState"`
	FailureReason     string     `json:"failureReason"`
	SelectedReleaseID *int64     `json:"selectedReleaseId,omitempty"`
}

type ReleaseSummary struct {
	SelectedReleaseID  int64                   `json:"selectedReleaseId"`
	ReleaseCandidateID int64                   `json:"releaseCandidateId"`
	LibraryItemID      int64                   `json:"libraryItemId"`
	Title              string                  `json:"title"`
	ExternalURL        string                  `json:"externalUrl,omitempty"`
	IndexerName        string                  `json:"indexerName,omitempty"`
	SizeBytes          int64                   `json:"sizeBytes"`
	PostedAt           time.Time               `json:"postedAt,omitempty"`
	Score              int                     `json:"score"`
	Selected           bool                    `json:"selected"`
	Rejected           bool                    `json:"rejected"`
	RejectReason       string                  `json:"rejectReason"`
	FailureCount       int                     `json:"failureCount"`
	LastFailureReason  string                  `json:"lastFailureReason"`
	ArchiveCount       int                     `json:"archiveCount"`
	ArchiveVolumeCount int                     `json:"archiveVolumeCount"`
	ArchiveStatuses    string                  `json:"archiveStatuses"`
	ArchiveRejects     string                  `json:"archiveRejects"`
	VirtualFileCount   int                     `json:"virtualFileCount"`
	Archives           []ReleaseArchiveSummary `json:"archives,omitempty"`
	FailedAttempts     []FailedReleaseAttempt  `json:"failedAttempts,omitempty"`
	CreatedAt          time.Time               `json:"createdAt"`
	NZBDocumentID      *int64                  `json:"nzbDocumentId,omitempty"`
	NZBFileName        string                  `json:"nzbFileName,omitempty"`
}

type FailedReleaseAttempt struct {
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"createdAt"`
}

type ReleaseArchiveSummary struct {
	Kind         string                `json:"kind"`
	Status       string                `json:"status"`
	RejectReason string                `json:"rejectReason"`
	VolumeCount  int                   `json:"volumeCount"`
	Entries      []ReleaseArchiveEntry `json:"entries,omitempty"`
}

type ReleaseArchiveEntry struct {
	Path                string `json:"path"`
	SizeBytes           int64  `json:"sizeBytes"`
	PackedSizeBytes     int64  `json:"packedSizeBytes"`
	CompressionMethod   string `json:"compressionMethod"`
	Encrypted           bool   `json:"encrypted"`
	Solid               bool   `json:"solid"`
	SourceVolumeIndex   int    `json:"sourceVolumeIndex"`
	SourceArchiveOffset int64  `json:"sourceArchiveOffset"`
}

type MediaRequestSummary struct {
	ID            int64      `json:"id"`
	ExternalID    string     `json:"externalId"`
	RequestType   string     `json:"requestType"`
	Title         string     `json:"title"`
	MediaType     string     `json:"mediaType"`
	LibraryItemID *int64     `json:"libraryItemId,omitempty"`
	QueueState    QueueState `json:"queueState"`
	CreatedAt     time.Time  `json:"createdAt"`
}

type SubtitleFileSummary struct {
	ID            int64     `json:"id"`
	LibraryItemID int64     `json:"libraryItemId"`
	Provider      string    `json:"provider"`
	Language      string    `json:"language"`
	Path          string    `json:"path"`
	CreatedAt     time.Time `json:"createdAt"`
}

type SubtitleCandidateSummary struct {
	ID              int64     `json:"id"`
	LibraryItemID   int64     `json:"libraryItemId"`
	Provider        string    `json:"provider"`
	Language        string    `json:"language"`
	Title           string    `json:"title"`
	ReleaseName     string    `json:"releaseName"`
	Format          string    `json:"format"`
	HearingImpaired bool      `json:"hearingImpaired"`
	Score           int       `json:"score"`
	ExternalID      string    `json:"externalId"`
	DownloadURL     string    `json:"-"`
	CreatedAt       time.Time `json:"createdAt"`
}

type BlocklistItemSummary struct {
	ID        int64      `json:"id"`
	Key       string     `json:"key"`
	Reason    string     `json:"reason"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

type SearchCandidateRecord struct {
	Title             string
	ExternalURL       string
	IndexerName       string
	SizeBytes         int64
	PostedAt          time.Time
	Score             int
	Rejected          bool
	RejectReason      string
	FailureCount      int
	LastFailureReason string
}

type CandidateHistory struct {
	ExternalURL       string
	FailureCount      int
	LastFailureReason string
}

type StoredNZBDocument struct {
	SelectedReleaseID int64
	FileName          string
	ExternalURL       string
	XML               []byte
}

type SubtitleDeleteGroup struct {
	LibraryItemID int64
	Provider      string
	Language      string
	Paths         []string
}

type SubtitleSearchInput struct {
	LibraryItemID int64
	MediaType     string
	Title         string
	ShowTitle     string
	MovieYear     int
	ShowYear      int
	SeasonNumber  int
	EpisodeNumber int
	TMDBID        int64
	TVDBID        int64
}

type SubtitleCandidateRecord struct {
	Provider        string
	Language        string
	Title           string
	ReleaseName     string
	Format          string
	HearingImpaired bool
	Score           int
	ExternalID      string
	DownloadURL     string
}

type LibrarySearchInput struct {
	LibraryItemID   int64
	MediaType       string
	Title           string
	IMDbID          string
	MovieYear       int
	MovieTMDBID     int64  // used in tmdbid= query parameter (Radarr approach)
	ShowTitle       string
	EpisodeTitle    string
	ShowIMDbID      string
	ShowTVDBID      int64
	ShowTMDBID      int64  // used in tmdbid= query parameter for TV (Sonarr approach)
	ShowYear        int
	SeasonNumber    int
	EpisodeNumber   int
	TVShowID        int64  // DB primary key of tv_shows row, used for season pack tracking
	AlternateTitles []string // mirrors Radarr/Sonarr AlternativeTitles; checked as fallback
}

type SymlinkPublicationRecord struct {
	ID          int64
	LibraryPath string
	TargetPath  string
}

type QueueRetryTarget struct {
	QueueItemID       int64
	LibraryItemID     int64
	SelectedReleaseID *int64
	MediaType         string
	IdempotencyKey    string
}

type PendingLibrarySearchTarget struct {
	LibraryItemID int64 `json:"libraryItemId"`
}

type FailedQueueRetryTarget struct {
	QueueItemID   int64  `json:"queueItemId"`
	LibraryItemID int64  `json:"libraryItemId"`
	FailureReason string `json:"failureReason"`
}

type PendingRepublishTarget struct {
	LibraryItemID int64 `json:"libraryItemId"`
}

type BlocklistClearResult struct {
	Cleared int `json:"cleared"`
}

type RejectedReleaseRestoreResult struct {
	LibraryItemID int64 `json:"libraryItemId"`
	Restored      int   `json:"restored"`
}
