package library

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/hjongedijk/drakkar/internal/config"
	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/metrics"
	"github.com/hjongedijk/drakkar/internal/stream"
	"github.com/hjongedijk/drakkar/internal/symlink"
)

var ErrNoVirtualFiles = errors.New("selected release has no publishable virtual files")
var ErrInvalidMediaPayload = errors.New("selected release has unreadable or invalid media payload")

type Repository interface {
	ListVirtualFilesForRelease(ctx context.Context, selectedReleaseID int64) ([]database.ReleaseVirtualFile, error)
	ListSelectedReleasesForPublication(ctx context.Context) ([]int64, error)
	ListSelectedReleasesByLibraryItem(ctx context.Context, libraryItemID int64) ([]int64, error)
	FindSourceSelectedReleaseForItem(ctx context.Context, libraryItemID int64) (int64, error)
	GetEpisodeMetadataForLibraryItem(ctx context.Context, libraryItemID int64) (database.EpisodeMetadata, error)
	ListPendingRepublishTargets(ctx context.Context) ([]database.PendingRepublishTarget, error)
	UpsertSymlinkPublication(ctx context.Context, libraryItemID, virtualFileID int64, libraryPath, targetPath string) error
	MarkReleaseAvailable(ctx context.Context, selectedReleaseID int64) error
	FindSeasonPackMatches(ctx context.Context, selectedReleaseID, triggeringLibraryItemID int64) ([]database.SeasonPackEpisodeMatch, error)
	FulfillEpisodeLibraryItem(ctx context.Context, libraryItemID, sourceSelectedReleaseID, virtualFileID int64) error
	CreateSeasonPackEpisodeItems(ctx context.Context, selectedReleaseID, triggeringLibraryItemID int64) error
	OpenVirtualMediaFile(ctx context.Context, virtualFileID int64) (stream.VirtualMediaFile, error)
}

type Publisher struct {
	repo            Repository
	syml            *symlink.Publisher
	runtime         config.Runtime
	postPublishHook func(context.Context, int64) error
}

type BulkRepublishResult struct {
	Processed        int     `json:"processed"`
	Republished      int     `json:"republished"`
	Failed           int     `json:"failed"`
	ProcessedLibrary []int64 `json:"processedLibrary,omitempty"`
	FailedLibrary    []int64 `json:"failedLibrary,omitempty"`
}

func NewPublisher(repo Repository, runtime config.Runtime) *Publisher {
	return &Publisher{
		repo:    repo,
		syml:    symlink.NewPublisher(),
		runtime: runtime,
	}
}

func (p *Publisher) SetPostPublishHook(fn func(context.Context, int64) error) {
	p.postPublishHook = fn
}

// PublishSelectedRelease publishes virtual files for a selected release.
// isNew should be true for fresh publishes (creates per-episode items for season
// packs) and false for startup rebuilds (skip redundant episode item creation).
func (p *Publisher) publishSelectedRelease(ctx context.Context, selectedReleaseID int64, isNew bool) error {
	files, err := p.repo.ListVirtualFilesForRelease(ctx, selectedReleaseID)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return ErrNoVirtualFiles
	}
	if err := p.validateReleaseMedia(ctx, files); err != nil {
		return err
	}
	libraryItemIDs := make(map[int64]struct{})
	for _, file := range files {
		// Season-pack guard: when a specific episode is expected (season > 0,
		// episode > 0) and the pack contains multiple files, only publish the file
		// whose filename parses to the expected episode. The other files are handled
		// by fulfillSeasonPackEpisodes. Without this guard every file in the pack
		// overwrites the same library path (e.g. S01E01.mkv) and the last file
		// alphabetically wins — typically the season finale.
		if strings.EqualFold(file.MediaType, "episode") &&
			file.SeasonNumber > 0 && file.EpisodeNumber > 0 && len(files) > 1 {
			fs, fe := database.ParseEpisodeFromFilename(file.FileName)
			if fs > 0 && fe > 0 && (fs != file.SeasonNumber || fe != file.EpisodeNumber) {
				libraryItemIDs[file.LibraryItemID] = struct{}{}
				continue
			}
		}
		target := filepath.Join(p.runtime.FuseMountPath, "content", file.Path)
		libraryPath := p.libraryPathFor(file)
		if libraryPath == "" {
			slog.Warn("skipping host symlink: insufficient metadata", "virtual_file_id", file.VirtualFileID, "file", file.FileName)
		} else {
			if err := p.syml.Publish(libraryPath, target); err != nil {
				return err
			}
			if err := p.repo.UpsertSymlinkPublication(ctx, file.LibraryItemID, file.VirtualFileID, libraryPath, target); err != nil {
				return err
			}
		}
		libraryItemIDs[file.LibraryItemID] = struct{}{}
	}
	// Only call the post-publish hook (subtitle search/publish) for new publications.
	// During startup RebuildPublications, subtitles are already in place.
	if isNew && p.postPublishHook != nil {
		for libraryItemID := range libraryItemIDs {
			if err := p.postPublishHook(ctx, libraryItemID); err != nil {
				return err
			}
		}
	}
	metrics.M.PublishedVirtualFiles.Add(int64(len(files)))
	if err := p.repo.MarkReleaseAvailable(ctx, selectedReleaseID); err != nil {
		return err
	}
	// For season packs: fulfil any other episode library items that are covered
	// by virtual files in this release but were searched as separate items.
	// Runs on rebuild too — fills in symlinks for episodes created after the
	// initial publish (e.g. by CreateSeasonPackEpisodeItems).
	if len(libraryItemIDs) == 1 {
		for triggeringID := range libraryItemIDs {
			p.fulfillSeasonPackEpisodes(ctx, selectedReleaseID, triggeringID, files)
		}
	}
	// Create per-episode library items for whole-show imports. Skip on rebuild —
	// those items were already created on the initial publish.
	if isNew && len(libraryItemIDs) == 1 {
		for triggeringID := range libraryItemIDs {
			if err := p.repo.CreateSeasonPackEpisodeItems(ctx, selectedReleaseID, triggeringID); err != nil {
				_ = err // non-fatal
			}
			p.fulfillSeasonPackEpisodes(ctx, selectedReleaseID, triggeringID, files)
		}
	}
	return nil
}

func (p *Publisher) validateReleaseMedia(ctx context.Context, files []database.ReleaseVirtualFile) error {
	for _, file := range files {
		if !isPublishableMediaType(file.MediaType) {
			continue
		}
		vf, err := p.repo.OpenVirtualMediaFile(ctx, file.VirtualFileID)
		if err != nil {
			return fmt.Errorf("%w: open virtual file %d: %v", ErrInvalidMediaPayload, file.VirtualFileID, err)
		}
		if err := validateMediaHeader(ctx, vf, file.FileName); err != nil {
			return fmt.Errorf("%w: virtual file %d %s: %v", ErrInvalidMediaPayload, file.VirtualFileID, file.FileName, err)
		}
	}
	return nil
}

func isPublishableMediaType(mediaType string) bool {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "movie", "episode", "tv":
		return true
	default:
		return false
	}
}

func validateMediaHeader(ctx context.Context, vf stream.VirtualMediaFile, fileName string) error {
	buf := make([]byte, 4096)
	n, err := vf.ReadAt(ctx, buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n <= 0 {
		return errors.New("file returned no readable bytes")
	}
	buf = buf[:n]
	if isAllZero(buf) {
		return errors.New("file header is all zero bytes")
	}
	if !matchesKnownContainer(fileName, buf) {
		return errors.New("file header does not match expected media container")
	}
	return nil
}

func isAllZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return len(buf) > 0
}

func matchesKnownContainer(fileName string, buf []byte) bool {
	ext := strings.ToLower(filepath.Ext(fileName))
	switch ext {
	case ".mkv", ".webm":
		return hasPrefix(buf, 0x1a, 0x45, 0xdf, 0xa3)
	case ".mp4", ".m4v", ".mov":
		return containsFTYP(buf)
	case ".avi":
		return len(buf) >= 12 && string(buf[0:4]) == "RIFF" && string(buf[8:12]) == "AVI "
	case ".ts", ".m2ts":
		return len(buf) >= 188 && buf[0] == 0x47
	default:
		// Unknown extension: allow if it looks like one of our common containers
		// or at least has non-zero readable data.
		return hasPrefix(buf, 0x1a, 0x45, 0xdf, 0xa3) ||
			containsFTYP(buf) ||
			(len(buf) >= 12 && string(buf[0:4]) == "RIFF") ||
			(len(buf) >= 188 && buf[0] == 0x47)
	}
}

func hasPrefix(buf []byte, want ...byte) bool {
	if len(buf) < len(want) {
		return false
	}
	for i := range want {
		if buf[i] != want[i] {
			return false
		}
	}
	return true
}

func containsFTYP(buf []byte) bool {
	limit := len(buf) - 8
	if limit < 0 {
		return false
	}
	for i := 0; i <= limit; i++ {
		if string(buf[i:i+4]) == "ftyp" {
			return true
		}
	}
	return false
}

// PublishSelectedRelease publishes a new release (creates per-episode items for season packs).
func (p *Publisher) PublishSelectedRelease(ctx context.Context, selectedReleaseID int64) error {
	return p.publishSelectedRelease(ctx, selectedReleaseID, true)
}

func (p *Publisher) RebuildPublications(ctx context.Context) error {
	selectedReleaseIDs, err := p.repo.ListSelectedReleasesForPublication(ctx)
	if err != nil {
		return err
	}
	for _, selectedReleaseID := range selectedReleaseIDs {
		// isNew=false: skip per-episode item creation; those were already done on initial publish.
		if err := p.publishSelectedRelease(ctx, selectedReleaseID, false); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) RepublishLibraryItem(ctx context.Context, libraryItemID int64) error {
	selectedReleaseIDs, err := p.repo.ListSelectedReleasesByLibraryItem(ctx, libraryItemID)
	if err != nil {
		return err
	}
	if len(selectedReleaseIDs) == 0 {
		// Season-pack episode: virtual files live under the pack's selected release.
		// Rather than re-publishing the whole pack (which may have no show metadata),
		// use the episode item's own metadata to find and publish the matching VF directly.
		sourceID, err := p.repo.FindSourceSelectedReleaseForItem(ctx, libraryItemID)
		if err != nil || sourceID == 0 {
			return nil
		}
		return p.republishEpisodeFromSourceRelease(ctx, libraryItemID, sourceID)
	}
	for _, selectedReleaseID := range selectedReleaseIDs {
		if err := p.PublishSelectedRelease(ctx, selectedReleaseID); err != nil {
			return err
		}
	}
	return nil
}

// republishEpisodeFromSourceRelease creates the missing symlink for an episode
// library item whose virtual files live under a season pack selected release.
// It queries the episode item's own metadata (show title, season, episode) and
// matches the corresponding virtual file by parsing the filename.
func (p *Publisher) republishEpisodeFromSourceRelease(ctx context.Context, libraryItemID, sourceReleaseID int64) error {
	meta, err := p.repo.GetEpisodeMetadataForLibraryItem(ctx, libraryItemID)
	if err != nil || meta.ShowTitle == "" || meta.SeasonNumber <= 0 || meta.EpisodeNumber <= 0 {
		return nil
	}
	files, err := p.repo.ListVirtualFilesForRelease(ctx, sourceReleaseID)
	if err != nil {
		return err
	}
	for _, f := range files {
		s, e := database.ParseEpisodeFromFilename(f.FileName)
		if s != meta.SeasonNumber || e != meta.EpisodeNumber {
			continue
		}
		enriched := f
		enriched.LibraryItemID = libraryItemID
		enriched.MediaType = "episode"
		enriched.ShowTitle = meta.ShowTitle
		enriched.ShowYear = meta.ShowYear
		enriched.ShowTVDBID = meta.ShowTVDBID
		enriched.SeasonNumber = meta.SeasonNumber
		enriched.EpisodeNumber = meta.EpisodeNumber
		target := filepath.Join(p.runtime.FuseMountPath, "content", enriched.Path)
		libraryPath := p.libraryPathFor(enriched)
		if libraryPath == "" {
			return nil
		}
		if err := p.syml.Publish(libraryPath, target); err != nil {
			return err
		}
		return p.repo.UpsertSymlinkPublication(ctx, libraryItemID, f.VirtualFileID, libraryPath, target)
	}
	return nil
}

func (p *Publisher) RepublishPendingLibrary(ctx context.Context) (BulkRepublishResult, error) {
	targets, err := p.repo.ListPendingRepublishTargets(ctx)
	if err != nil {
		return BulkRepublishResult{}, err
	}
	result := BulkRepublishResult{Processed: len(targets)}
	for _, target := range targets {
		result.ProcessedLibrary = append(result.ProcessedLibrary, target.LibraryItemID)
		if err := p.RepublishLibraryItem(ctx, target.LibraryItemID); err != nil {
			slog.Warn("republish pending: item failed", "library_item_id", target.LibraryItemID, "err", err)
			result.Failed++
			result.FailedLibrary = append(result.FailedLibrary, target.LibraryItemID)
			continue
		}
		result.Republished++
	}
	slog.Info("republish pending: done", "processed", result.Processed, "republished", result.Republished, "failed", result.Failed)
	return result, nil
}

// fulfillSeasonPackEpisodes matches virtual files in a season pack to their
// individual episode library items and marks each one as available.
// This runs after a season pack is published so all episodes are fulfilled
// without each needing its own separate NZB download.
func (p *Publisher) fulfillSeasonPackEpisodes(ctx context.Context, selectedReleaseID, triggeringLibraryItemID int64, files []database.ReleaseVirtualFile) {
	matches, err := p.repo.FindSeasonPackMatches(ctx, selectedReleaseID, triggeringLibraryItemID)
	if err != nil || len(matches) == 0 {
		return
	}
	// Build a fast lookup: (season, episode) → virtual file
	type epKey struct{ season, episode int }
	fileByEpisode := map[epKey]database.ReleaseVirtualFile{}
	for _, f := range files {
		s, e := database.ParseEpisodeFromFilename(f.FileName)
		if s > 0 && e > 0 {
			fileByEpisode[epKey{s, e}] = f
		}
	}

	for _, m := range matches {
		vf, ok := fileByEpisode[epKey{m.SeasonNumber, m.EpisodeNumber}]
		if !ok {
			vf.VirtualFileID = m.VirtualFileID
		}
		// Prefer the file path from fileByEpisode (ordered by vf.path, so deterministic
		// and alphabetically last — proper "ShowName.SxxExx.Title.mkv" files sort after
		// bonus content like "Behind The Story - SxxExx.mkv"). Fall back to m.VirtualFilePath
		// only when fileByEpisode had no entry for this episode.
		enrichedPath := m.VirtualFilePath
		enrichedFileName := m.FileName
		if ok && vf.Path != "" {
			enrichedPath = vf.Path
			enrichedFileName = vf.FileName
		}
		// Publish the host symlink for this episode using its proper library item metadata.
		// We reuse the existing virtual file — no new NNTP fetching needed.
		enriched := database.ReleaseVirtualFile{
			VirtualFileID:     vf.VirtualFileID,
			SelectedReleaseID: selectedReleaseID,
			LibraryItemID:     m.LibraryItemID,
			MediaType:         "episode",
			Path:              enrichedPath,
			FileName:          enrichedFileName,
		}
		if meta, metaErr := p.repo.GetEpisodeMetadataForLibraryItem(ctx, m.LibraryItemID); metaErr == nil {
			enriched.ShowTitle = meta.ShowTitle
			enriched.ShowYear = meta.ShowYear
			enriched.ShowTVDBID = meta.ShowTVDBID
			enriched.SeasonNumber = meta.SeasonNumber
			enriched.EpisodeNumber = meta.EpisodeNumber
		}
		if enriched.SeasonNumber <= 0 || enriched.EpisodeNumber <= 0 {
			enriched.SeasonNumber = m.SeasonNumber
			enriched.EpisodeNumber = m.EpisodeNumber
		}
		target := filepath.Join(p.runtime.FuseMountPath, "content", enriched.Path)
		libraryPath := p.libraryPathFor(enriched)
		if libraryPath != "" {
			if symlinkErr := p.syml.Publish(libraryPath, target); symlinkErr == nil {
				_ = p.repo.UpsertSymlinkPublication(ctx, m.LibraryItemID, m.VirtualFileID, libraryPath, target)
			}
		}
		_ = p.repo.FulfillEpisodeLibraryItem(ctx, m.LibraryItemID, selectedReleaseID, m.VirtualFileID)
		slog.Debug("season pack: fulfilled episode",
			"library_item_id", m.LibraryItemID,
			"season", m.SeasonNumber, "episode", m.EpisodeNumber,
			"file", m.FileName)
	}
}

func (p *Publisher) libraryPathFor(file database.ReleaseVirtualFile) string {
	switch strings.ToLower(file.MediaType) {
	case "movie":
		if file.MovieTitle != "" {
			return symlink.MoviePath(p.runtime.MovieLibraryPath, file.MovieTitle, file.MovieYear, int(file.MovieTMDBID), file.FileName)
		}
	case "episode", "tv":
		season := file.SeasonNumber
		episode := file.EpisodeNumber
		// Season packs: the library item may have season=0/episode=0 when imported
		// as a whole-show request. Fall back to parsing the filename.
		if (season <= 0 || episode <= 0) && file.FileName != "" {
			season, episode = database.ParseEpisodeFromFilename(file.FileName)
		}
		if file.ShowTitle != "" && season > 0 && episode > 0 {
			return symlink.EpisodePath(
				p.runtime.TVLibraryPath,
				file.ShowTitle,
				file.ShowYear,
				int(file.ShowTVDBID),
				season,
				episode,
				file.FileName,
			)
		}
	}
	return ""
}

func CompletedTargetVirtualPath(selectedReleaseID int64, fileName string) string {
	return filepath.Join("/content", "releases", fmt.Sprintf("%d", selectedReleaseID), fileName)
}
