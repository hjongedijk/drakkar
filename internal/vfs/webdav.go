package vfs

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/stream"
	"golang.org/x/net/webdav"
)

// VirtualFileLister lists all published virtual files.
type VirtualFileLister interface {
	ListAllVirtualFiles(ctx context.Context) ([]database.VirtualFileEntry, error)
}

// newDAVFS returns a webdav.FileSystem that mirrors nzbdav's exact directory
// structure. Top-level directories:
//
//	/nzbs/                               — empty (NZB watch folder, nzbdav compat)
//	/content/                            — all virtual files listed flat
//	/completed-symlinks/                 — mirrors /content with .rclonelink files
//	/.ids/{c0}/{c1}/{c2}/{c3}/{c4}/{id}/{filename} — 5-char prefix tree, exact nzbdav format
//
// Symlinks in the media library point to {rcloneMount}/.ids/... — matching
// nzbdav's GetTargetPath() which uses the same 5-level prefix hierarchy.
func newDAVFS(lister VirtualFileLister, opener FileOpener) webdav.FileSystem {
	return &davFS{lister: lister, opener: opener}
}

// idPrefix returns the 5-character prefix string used for the .ids tree.
// Maps integer IDs to a 12-digit zero-padded decimal string and takes the first 5 chars.
// Example: ID 456 → "000000000456" → prefix "00000"
// Example: ID 123456789 → "000123456789" → prefix "00012"
func idPrefix(id int64) string {
	s := fmt.Sprintf("%012d", id)
	return s[:5]
}

// IdsPath returns the full /.ids path for a virtual file — the path that
// rclone-mounted symlinks will point to.
// Matches nzbdav's DatabaseStoreSymlinkFile.GetTargetPath() exactly.
func IdsPath(id int64, filename string) string {
	p := idPrefix(id)
	return fmt.Sprintf("/.ids/%c/%c/%c/%c/%c/%012d/%s",
		p[0], p[1], p[2], p[3], p[4], id, filename)
}

type davFS struct {
	lister    VirtualFileLister
	opener    FileOpener
	cacheMu   sync.Mutex
	cacheAt   time.Time
	cacheData []database.VirtualFileEntry
}

const listCacheTTL = 2 * time.Minute

var epoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// listFiles returns all virtual files, cached for listCacheTTL to avoid
// hammering the DB on every rclone poll interval.
func (d *davFS) listFiles(ctx context.Context) ([]database.VirtualFileEntry, error) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if time.Since(d.cacheAt) < listCacheTTL && d.cacheData != nil {
		return d.cacheData, nil
	}
	files, err := d.lister.ListAllVirtualFiles(ctx)
	if err != nil {
		return nil, err
	}
	d.cacheData = files
	d.cacheAt = time.Now()
	return files, nil
}

func (d *davFS) Mkdir(_ context.Context, _ string, _ os.FileMode) error { return os.ErrPermission }
func (d *davFS) RemoveAll(_ context.Context, _ string) error             { return os.ErrPermission }
func (d *davFS) Rename(_ context.Context, _, _ string) error             { return os.ErrPermission }

func (d *davFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	f, err := d.OpenFile(ctx, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Stat()
}

func (d *davFS) OpenFile(ctx context.Context, name string, _ int, _ os.FileMode) (webdav.File, error) {
	name = "/" + strings.TrimPrefix(strings.TrimSuffix(name, "/"), "/")

	// ── root ──────────────────────────────────────────────────────────────────
	if name == "/" {
		return staticDir("/", []os.FileInfo{
			dirInfo("nzbs"),
			dirInfo("content"),
			dirInfo("completed-symlinks"),
			dirInfo(".ids"),
		}), nil
	}

	// ── /nzbs ─────────────────────────────────────────────────────────────────
	if name == "/nzbs" {
		return staticDir("/nzbs", nil), nil
	}

	// ── /content ──────────────────────────────────────────────────────────────
	if name == "/content" {
		files, err := d.listFiles(ctx)
		if err != nil {
			return nil, err
		}
		entries := make([]os.FileInfo, 0, len(files))
		for _, f := range files {
			entries = append(entries, &fileInfo{name: f.FileName, size: f.Size})
		}
		return staticDir("/content", entries), nil
	}
	// /content/{filename} — look up by filename
	if strings.HasPrefix(name, "/content/") {
		fname := path.Base(name)
		files, err := d.listFiles(ctx)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if f.FileName == fname {
				vf, err := d.opener.OpenVirtualMediaFile(ctx, f.ID)
				if err != nil {
					return nil, os.ErrNotExist
				}
				return &davFile{ctx: ctx, id: f.ID, name: fname, file: vf}, nil
			}
		}
		return nil, os.ErrNotExist
	}

	// ── /completed-symlinks ───────────────────────────────────────────────────
	// Mirrors /content but serves .rclonelink files (plain-text path to /.ids/...)
	if name == "/completed-symlinks" {
		files, err := d.listFiles(ctx)
		if err != nil {
			return nil, err
		}
		entries := make([]os.FileInfo, 0, len(files))
		for _, f := range files {
			entries = append(entries, &fileInfo{name: f.FileName + ".rclonelink", size: int64(len(IdsPath(f.ID, f.FileName)) + 1)})
		}
		return staticDir("/completed-symlinks", entries), nil
	}
	if strings.HasPrefix(name, "/completed-symlinks/") {
		linkName := path.Base(name)
		fname := strings.TrimSuffix(linkName, ".rclonelink")
		files, err := d.listFiles(ctx)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if f.FileName == fname {
				content := IdsPath(f.ID, f.FileName) + "\n"
				return &bytesFile{name: linkName, data: []byte(content)}, nil
			}
		}
		return nil, os.ErrNotExist
	}

	// ── /.ids prefix tree ─────────────────────────────────────────────────────
	// Structure: /.ids/{c0}/{c1}/{c2}/{c3}/{c4}/{12-digit-id}/{filename}
	if name == "/.ids" {
		// List all unique first characters of the prefix
		files, err := d.listFiles(ctx)
		if err != nil {
			return nil, err
		}
		seen := map[string]struct{}{}
		for _, f := range files {
			c := string(idPrefix(f.ID)[0])
			seen[c] = struct{}{}
		}
		entries := make([]os.FileInfo, 0, len(seen))
		for c := range seen {
			entries = append(entries, dirInfo(c))
		}
		return staticDir("/.ids", entries), nil
	}
	if strings.HasPrefix(name, "/.ids/") {
		return d.openIdsPath(ctx, name)
	}

	return nil, os.ErrNotExist
}

func (d *davFS) openIdsPath(ctx context.Context, name string) (webdav.File, error) {
	// Strip /.ids/ prefix and split path
	rest := strings.TrimPrefix(name, "/.ids/")
	parts := strings.SplitN(rest, "/", 7) // max: c0/c1/c2/c3/c4/id/filename

	files, err := d.listFiles(ctx)
	if err != nil {
		return nil, err
	}

	depth := len(parts)

	// Depth 1-5: prefix character directory levels
	if depth <= 5 {
		prefix := strings.Join(parts, "")
		seen := map[string]struct{}{}
		for _, f := range files {
			fp := idPrefix(f.ID)
			if strings.HasPrefix(fp, prefix) && len(fp) > len(prefix) {
				c := string(fp[len(prefix)])
				seen[c] = struct{}{}
			}
		}
		if len(seen) == 0 {
			return nil, os.ErrNotExist
		}
		entries := make([]os.FileInfo, 0, len(seen))
		for c := range seen {
			entries = append(entries, dirInfo(c))
		}
		return staticDir(name, entries), nil
	}

	// Depth 6: /.ids/c0/c1/c2/c3/c4/{12-digit-id} — list files for this ID
	if depth == 6 {
		prefix := strings.Join(parts[:5], "")
		idStr := parts[5]
		var matchID int64
		fmt.Sscanf(idStr, "%d", &matchID)
		if fmt.Sprintf("%012d", matchID) != idStr || idPrefix(matchID) != prefix {
			return nil, os.ErrNotExist
		}
		for _, f := range files {
			if f.ID == matchID {
				return staticDir(name, []os.FileInfo{&fileInfo{name: f.FileName, size: f.Size}}), nil
			}
		}
		return nil, os.ErrNotExist
	}

	// Depth 7: /.ids/c0/c1/c2/c3/c4/{id}/{filename} — the actual file
	if depth == 7 {
		idStr := parts[5]
		filename := parts[6]
		var id int64
		fmt.Sscanf(idStr, "%d", &id)
		if fmt.Sprintf("%012d", id) != idStr {
			return nil, os.ErrNotExist
		}
		vf, err := d.opener.OpenVirtualMediaFile(ctx, id)
		if err != nil {
			return nil, os.ErrNotExist
		}
		if vf.Name() != filename {
			return nil, os.ErrNotExist
		}
		return &davFile{ctx: ctx, id: id, name: filename, file: vf}, nil
	}

	return nil, os.ErrNotExist
}

// ── helpers ────────────────────────────────────────────────────────────────

func staticDir(name string, entries []os.FileInfo) webdav.File {
	return &davDir{name: path.Base(name), entries: entries}
}

func dirInfo(name string) os.FileInfo { return &davDirInfo{name: name} }

// ── directory node ─────────────────────────────────────────────────────────

type davDir struct {
	name    string
	entries []os.FileInfo
	pos     int
}

func (d *davDir) Close() error                  { return nil }
func (d *davDir) Write(_ []byte) (int, error)   { return 0, os.ErrPermission }
func (d *davDir) Seek(_ int64, _ int) (int64, error) { return 0, os.ErrInvalid }
func (d *davDir) Read(_ []byte) (int, error)     { return 0, os.ErrInvalid }
func (d *davDir) Stat() (os.FileInfo, error)     { return &davDirInfo{name: d.name}, nil }
func (d *davDir) Readdir(count int) ([]os.FileInfo, error) {
	if d.pos >= len(d.entries) {
		return nil, nil
	}
	if count <= 0 || d.pos+count > len(d.entries) {
		count = len(d.entries) - d.pos
	}
	out := d.entries[d.pos : d.pos+count]
	d.pos += count
	return out, nil
}

type davDirInfo struct{ name string }

func (i *davDirInfo) Name() string       { return i.name }
func (i *davDirInfo) Size() int64        { return 0 }
func (i *davDirInfo) Mode() fs.FileMode  { return fs.ModeDir | 0o555 }
func (i *davDirInfo) ModTime() time.Time { return epoch }
func (i *davDirInfo) IsDir() bool        { return true }
func (i *davDirInfo) Sys() any           { return nil }

// ── file node (virtual media) ──────────────────────────────────────────────

type davFile struct {
	ctx  context.Context
	id   int64
	name string
	file stream.VirtualMediaFile
	pos  int64
}

func (f *davFile) Close() error                        { return nil }
func (f *davFile) Write(_ []byte) (int, error)         { return 0, os.ErrPermission }
func (f *davFile) Readdir(_ int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }
func (f *davFile) Stat() (os.FileInfo, error) {
	return &fileInfo{name: f.name, size: f.file.Size()}, nil
}
func (f *davFile) Read(p []byte) (int, error) {
	n, err := f.file.ReadAt(f.ctx, p, f.pos)
	f.pos += int64(n)
	return n, err
}
func (f *davFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		f.pos = offset
	case 1:
		f.pos += offset
	case 2:
		f.pos = f.file.Size() + offset
	}
	if f.pos < 0 {
		f.pos = 0
	}
	return f.pos, nil
}

type fileInfo struct {
	name string
	size int64
}

func (i *fileInfo) Name() string       { return i.name }
func (i *fileInfo) Size() int64        { return i.size }
func (i *fileInfo) Mode() fs.FileMode  { return 0o444 }
func (i *fileInfo) ModTime() time.Time { return epoch }
func (i *fileInfo) IsDir() bool        { return false }
func (i *fileInfo) Sys() any           { return nil }

// ── bytes file (rclonelink) ────────────────────────────────────────────────

type bytesFile struct {
	name string
	data []byte
	pos  int
}

func (f *bytesFile) Close() error                        { return nil }
func (f *bytesFile) Write(_ []byte) (int, error)         { return 0, os.ErrPermission }
func (f *bytesFile) Readdir(_ int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }
func (f *bytesFile) Stat() (os.FileInfo, error) {
	return &fileInfo{name: f.name, size: int64(len(f.data))}, nil
}
func (f *bytesFile) Read(p []byte) (int, error) {
	if f.pos >= len(f.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}
func (f *bytesFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		f.pos = int(offset)
	case 1:
		f.pos += int(offset)
	case 2:
		f.pos = len(f.data) + int(offset)
	}
	return int64(f.pos), nil
}

func parseInt64(s string) (int64, error) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
