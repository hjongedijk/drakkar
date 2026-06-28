package library

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hjongedijk/drakkar/internal/config"
	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/stream"
)

type repoStub struct {
	files       []database.ReleaseVirtualFile
	publicated  []database.CompletedSymlinkEntry
	available   int64
	selected    []int64
	byLibrary   []int64
	pending     []database.PendingRepublishTarget
	matches     []database.SeasonPackEpisodeMatch
	episodeMeta map[int64]database.EpisodeMetadata
	createCalls int
	fulfilled   []int64
	virtualData map[int64][]byte
	virtualErr  map[int64]error
}

func (r *repoStub) ListVirtualFilesForRelease(ctx context.Context, selectedReleaseID int64) ([]database.ReleaseVirtualFile, error) {
	return r.files, nil
}

func (r *repoStub) UpsertSymlinkPublication(ctx context.Context, libraryItemID, virtualFileID int64, libraryPath, targetPath string) error {
	r.publicated = append(r.publicated, database.CompletedSymlinkEntry{
		PublicationID: virtualFileID,
		Name:          filepath.Base(libraryPath),
		TargetPath:    targetPath,
	})
	return nil
}

func (r *repoStub) MarkReleaseAvailable(ctx context.Context, selectedReleaseID int64) error {
	r.available = selectedReleaseID
	return nil
}

func (r *repoStub) ListSelectedReleasesForPublication(ctx context.Context) ([]int64, error) {
	return r.selected, nil
}

func (r *repoStub) ListSelectedReleasesByLibraryItem(ctx context.Context, libraryItemID int64) ([]int64, error) {
	return r.byLibrary, nil
}

func (r *repoStub) ListPendingRepublishTargets(ctx context.Context) ([]database.PendingRepublishTarget, error) {
	return r.pending, nil
}

func (r *repoStub) FindSourceSelectedReleaseForItem(_ context.Context, _ int64) (int64, error) {
	return 0, nil
}
func (r *repoStub) GetEpisodeMetadataForLibraryItem(_ context.Context, libraryItemID int64) (database.EpisodeMetadata, error) {
	if r.episodeMeta == nil {
		return database.EpisodeMetadata{}, nil
	}
	return r.episodeMeta[libraryItemID], nil
}

func (r *repoStub) FindSeasonPackMatches(_ context.Context, _, _ int64) ([]database.SeasonPackEpisodeMatch, error) {
	return r.matches, nil
}

func (r *repoStub) FulfillEpisodeLibraryItem(_ context.Context, libraryItemID, _, _ int64) error {
	r.fulfilled = append(r.fulfilled, libraryItemID)
	return nil
}
func (r *repoStub) CreateSeasonPackEpisodeItems(_ context.Context, _, _ int64) error {
	r.createCalls++
	return nil
}

func (r *repoStub) OpenVirtualMediaFile(_ context.Context, virtualFileID int64) (stream.VirtualMediaFile, error) {
	if err := r.virtualErr[virtualFileID]; err != nil {
		return nil, err
	}
	return testVF{name: "vf", data: r.virtualData[virtualFileID]}, nil
}

type testVF struct {
	name string
	data []byte
}

func (f testVF) Name() string { return f.name }
func (f testVF) Size() int64  { return int64(len(f.data)) }
func (f testVF) ReadAt(_ context.Context, dst []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(dst, f.data[off:])
	if int(off)+n >= len(f.data) {
		return n, io.EOF
	}
	return n, nil
}

func TestPublishSelectedReleaseUnknownMediaType(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{
		files: []database.ReleaseVirtualFile{
			{
				VirtualFileID:     11,
				SelectedReleaseID: 77,
				LibraryItemID:     22,
				MediaType:         "manual_nzb",
				Path:              "releases/77/Dune.mkv",
				FileName:          "Dune.mkv",
			},
		},
		virtualData: map[int64][]byte{11: []byte("not-media")},
	}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	if err := publisher.PublishSelectedRelease(context.Background(), 77); err != nil {
		t.Fatal(err)
	}
	// No host symlink should be created when metadata is insufficient.
	if _, err := os.Stat(filepath.Join(rt.MovieLibraryPath)); err == nil {
		entries, _ := os.ReadDir(rt.MovieLibraryPath)
		if len(entries) > 0 {
			t.Fatalf("expected no host symlink directories, found %v", entries)
		}
	}
	// Release must still be marked available so the FUSE virtual file is accessible.
	if repo.available != 77 {
		t.Fatalf("release not marked available")
	}
}

func TestPublishSelectedReleaseMoviePath(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{
		files: []database.ReleaseVirtualFile{
			{
				VirtualFileID:     11,
				SelectedReleaseID: 77,
				LibraryItemID:     22,
				MediaType:         "movie",
				Path:              "releases/77/Dune (2021).mkv",
				FileName:          "Dune (2021).mkv",
				MovieTitle:        "Dune",
				MovieYear:         2021,
				MovieTMDBID:       438631,
			},
		},
		virtualData: map[int64][]byte{11: append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x01}, 32)...)},
	}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	if err := publisher.PublishSelectedRelease(context.Background(), 77); err != nil {
		t.Fatal(err)
	}
	finalPath := filepath.Join(rt.MovieLibraryPath, "Dune (2021) {tmdb-438631}", "Dune (2021).mkv")
	target, err := os.Readlink(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if target != filepath.Join(rt.FuseMountPath, "content", "releases/77/Dune (2021).mkv") {
		t.Fatalf("unexpected target %s", target)
	}
}

func TestPublishSelectedReleaseEpisodePath(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{
		files: []database.ReleaseVirtualFile{
			{
				VirtualFileID:     12,
				SelectedReleaseID: 88,
				LibraryItemID:     23,
				MediaType:         "episode",
				Path:              "releases/88/Loki (2021) - S02E01.mkv",
				FileName:          "Loki (2021) - S02E01.mkv",
				ShowTitle:         "Loki",
				ShowYear:          2021,
				ShowTVDBID:        362472,
				SeasonNumber:      2,
				EpisodeNumber:     1,
			},
		},
		virtualData: map[int64][]byte{12: append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x01}, 32)...)},
	}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.TVLibraryPath = filepath.Join(root, "tv")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	if err := publisher.PublishSelectedRelease(context.Background(), 88); err != nil {
		t.Fatal(err)
	}
	finalPath := filepath.Join(rt.TVLibraryPath, "Loki (2021) {tvdb-362472}", "Season 02", "Loki - S02E01.mkv")
	target, err := os.Readlink(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if target != filepath.Join(rt.FuseMountPath, "content", "releases/88/Loki (2021) - S02E01.mkv") {
		t.Fatalf("unexpected target %s", target)
	}
}

func TestPublishSelectedReleaseWholeShowPackPublishesEpisodeSymlink(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{
		files: []database.ReleaseVirtualFile{
			{
				VirtualFileID:     40,
				SelectedReleaseID: 99,
				LibraryItemID:     548,
				MediaType:         "tv",
				Path:              "releases/99/Yellowstone.S04E01.mkv",
				FileName:          "Yellowstone.S04E01.mkv",
			},
		},
		matches: []database.SeasonPackEpisodeMatch{{
			VirtualFileID:   40,
			VirtualFilePath: "releases/99/Yellowstone.S04E01.mkv",
			FileName:        "Yellowstone.S04E01.mkv",
			LibraryItemID:   25894,
			SeasonNumber:    4,
			EpisodeNumber:   1,
		}},
		episodeMeta: map[int64]database.EpisodeMetadata{
			25894: {
				ShowTitle:     "Yellowstone",
				ShowYear:      2018,
				ShowTVDBID:    341164,
				SeasonNumber:  4,
				EpisodeNumber: 1,
			},
		},
		virtualData: map[int64][]byte{40: append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x01}, 32)...)},
	}
	rt := config.DefaultRuntime()
	rt.TVLibraryPath = filepath.Join(root, "tv")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	if err := publisher.PublishSelectedRelease(context.Background(), 99); err != nil {
		t.Fatal(err)
	}
	finalPath := filepath.Join(rt.TVLibraryPath, "Yellowstone (2018) {tvdb-341164}", "Season 04", "Yellowstone - S04E01.mkv")
	target, err := os.Readlink(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if target != filepath.Join(rt.FuseMountPath, "content", "releases/99/Yellowstone.S04E01.mkv") {
		t.Fatalf("unexpected target %s", target)
	}
	if repo.createCalls != 1 {
		t.Fatalf("expected create pass once, got %d", repo.createCalls)
	}
	if len(repo.fulfilled) != 2 || repo.fulfilled[0] != 25894 || repo.fulfilled[1] != 25894 {
		t.Fatalf("expected initial + post-create fulfill passes, got %+v", repo.fulfilled)
	}
}

func TestRebuildPublications(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{
		selected: []int64{77},
		files: []database.ReleaseVirtualFile{
			{
				VirtualFileID:     11,
				SelectedReleaseID: 77,
				LibraryItemID:     22,
				MediaType:         "movie",
				Path:              "releases/77/Dune (2021).mkv",
				FileName:          "Dune (2021).mkv",
				MovieTitle:        "Dune",
				MovieYear:         2021,
				MovieTMDBID:       438631,
			},
		},
		virtualData: map[int64][]byte{11: append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x01}, 32)...)},
	}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	if err := publisher.RebuildPublications(context.Background()); err != nil {
		t.Fatal(err)
	}
	finalPath := filepath.Join(rt.MovieLibraryPath, "Dune (2021) {tmdb-438631}", "Dune (2021).mkv")
	if _, err := os.Readlink(finalPath); err != nil {
		t.Fatal(err)
	}
	if repo.available != 77 {
		t.Fatalf("unexpected available release %d", repo.available)
	}
}

func TestRepublishLibraryItem(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{
		byLibrary: []int64{77},
		files: []database.ReleaseVirtualFile{
			{
				VirtualFileID:     11,
				SelectedReleaseID: 77,
				LibraryItemID:     22,
				MediaType:         "movie",
				Path:              "releases/77/Dune (2021).mkv",
				FileName:          "Dune (2021).mkv",
				MovieTitle:        "Dune",
				MovieYear:         2021,
				MovieTMDBID:       438631,
			},
		},
		virtualData: map[int64][]byte{11: append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x01}, 32)...)},
	}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	if err := publisher.RepublishLibraryItem(context.Background(), 22); err != nil {
		t.Fatal(err)
	}
	finalPath := filepath.Join(rt.MovieLibraryPath, "Dune (2021) {tmdb-438631}", "Dune (2021).mkv")
	if _, err := os.Readlink(finalPath); err != nil {
		t.Fatal(err)
	}
}

func TestRepublishPendingLibrary(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{
		pending:   []database.PendingRepublishTarget{{LibraryItemID: 22}, {LibraryItemID: 23}},
		byLibrary: []int64{77},
		files: []database.ReleaseVirtualFile{
			{
				VirtualFileID:     11,
				SelectedReleaseID: 77,
				LibraryItemID:     22,
				MediaType:         "movie",
				Path:              "releases/77/Dune (2021).mkv",
				FileName:          "Dune (2021).mkv",
				MovieTitle:        "Dune",
				MovieYear:         2021,
				MovieTMDBID:       438631,
			},
		},
		virtualData: map[int64][]byte{11: append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x01}, 32)...)},
	}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	result, err := publisher.RepublishPendingLibrary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed != 2 || result.Republished != 2 || result.Failed != 0 {
		t.Fatalf("unexpected result %+v", result)
	}
}

// TestRestartReconstructionIdempotent simulates two publisher lifetimes sharing
// the same repository state — the canonical "after restart" scenario.
// RebuildPublications must produce identical results on every call without
// duplicating, corrupting, or failing on already-published symlinks.
func TestRestartReconstructionIdempotent(t *testing.T) {
	root := t.TempDir()
	file := database.ReleaseVirtualFile{
		VirtualFileID:     11,
		SelectedReleaseID: 77,
		LibraryItemID:     22,
		MediaType:         "movie",
		Path:              "releases/77/Dune (2021).mkv",
		FileName:          "Dune (2021).mkv",
		MovieTitle:        "Dune",
		MovieYear:         2021,
		MovieTMDBID:       438631,
	}
	repo := &repoStub{
		selected:    []int64{77},
		files:       []database.ReleaseVirtualFile{file},
		virtualData: map[int64][]byte{11: append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x01}, 32)...)},
	}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.FuseMountPath = filepath.Join(root, "vfs")

	finalPath := filepath.Join(rt.MovieLibraryPath, "Dune (2021) {tmdb-438631}", "Dune (2021).mkv")
	want := filepath.Join(rt.FuseMountPath, "content", "releases/77/Dune (2021).mkv")

	// First publisher lifetime: initial publication.
	p1 := NewPublisher(repo, rt, "")
	if err := p1.RebuildPublications(context.Background()); err != nil {
		t.Fatalf("first rebuild failed: %v", err)
	}
	target1, err := os.Readlink(finalPath)
	if err != nil {
		t.Fatalf("symlink missing after first rebuild: %v", err)
	}
	if target1 != want {
		t.Fatalf("unexpected target after first rebuild: %s", target1)
	}

	// Second publisher lifetime: simulates a restart with the same persisted state.
	// Must overwrite the existing symlink atomically without error.
	p2 := NewPublisher(repo, rt, "")
	if err := p2.RebuildPublications(context.Background()); err != nil {
		t.Fatalf("second rebuild (restart) failed: %v", err)
	}
	target2, err := os.Readlink(finalPath)
	if err != nil {
		t.Fatalf("symlink missing after second rebuild: %v", err)
	}
	if target2 != want {
		t.Fatalf("unexpected target after second rebuild: %s", target2)
	}
	if target1 != target2 {
		t.Fatalf("targets differ between rebuilds: %s vs %s", target1, target2)
	}
}

func TestPublishSelectedReleaseFailsWithoutVirtualFiles(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	err := publisher.PublishSelectedRelease(context.Background(), 77)
	if !errors.Is(err, ErrNoVirtualFiles) {
		t.Fatalf("expected ErrNoVirtualFiles, got %v", err)
	}
	if repo.available != 0 {
		t.Fatalf("release should not be marked available, got %d", repo.available)
	}
}

func TestPublishSelectedReleaseRunsPostPublishHook(t *testing.T) {
	root := t.TempDir()
	repo := &repoStub{
		files: []database.ReleaseVirtualFile{
			{
				VirtualFileID:     11,
				SelectedReleaseID: 77,
				LibraryItemID:     22,
				MediaType:         "movie",
				Path:              "releases/77/Dune (2021).mkv",
				FileName:          "Dune (2021).mkv",
				MovieTitle:        "Dune",
				MovieYear:         2021,
				MovieTMDBID:       438631,
			},
		},
		virtualData: map[int64][]byte{11: append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x01}, 32)...)},
	}
	rt := config.DefaultRuntime()
	rt.MovieLibraryPath = filepath.Join(root, "movies")
	rt.FuseMountPath = filepath.Join(root, "vfs")
	publisher := NewPublisher(repo, rt, "")

	var hooked int64
	publisher.SetPostPublishHook(func(ctx context.Context, libraryItemID int64) error {
		hooked = libraryItemID
		return nil
	})

	if err := publisher.PublishSelectedRelease(context.Background(), 77); err != nil {
		t.Fatal(err)
	}
	if hooked != 22 {
		t.Fatalf("unexpected hooked library item %d", hooked)
	}
}

