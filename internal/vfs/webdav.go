package vfs

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/hjongedijk/drakkar/internal/stream"
	"github.com/hjongedijk/drakkar/internal/database"
	"golang.org/x/net/webdav"
)

// davFS implements webdav.FileSystem so rclone can mount Drakkar's content
// via WebDAV (PROPFIND + GET). The tree is flat:
//
//	/                          → root directory
//	/content/                  → all virtual files listed here
//	/content/{id}/{filename}   → individual streamable file
type davFS struct {
	lister  VirtualFileLister
	opener  FileOpener
}

// VirtualFileLister lists all published virtual files.
type VirtualFileLister interface {
	ListAllVirtualFiles(ctx context.Context) ([]database.VirtualFileEntry, error)
}

func newDAVFS(lister VirtualFileLister, opener FileOpener) webdav.FileSystem {
	return &davFS{lister: lister, opener: opener}
}

func (d *davFS) Mkdir(_ context.Context, _ string, _ os.FileMode) error {
	return os.ErrPermission
}
func (d *davFS) RemoveAll(_ context.Context, _ string) error { return os.ErrPermission }
func (d *davFS) Rename(_ context.Context, _, _ string) error  { return os.ErrPermission }

func (d *davFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	f, err := d.OpenFile(ctx, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Stat()
}

func (d *davFS) OpenFile(ctx context.Context, name string, flag int, _ os.FileMode) (webdav.File, error) {
	// Normalise path.
	name = "/" + strings.TrimPrefix(name, "/")

	switch name {
	case "/":
		return &davDir{name: "/", entries: []os.FileInfo{&davDirInfo{name: "content"}}}, nil
	case "/content", "/content/":
		files, err := d.lister.ListAllVirtualFiles(ctx)
		if err != nil {
			return nil, err
		}
		entries := make([]os.FileInfo, 0, len(files))
		for _, f := range files {
			entries = append(entries, &davDirInfo{name: fmt.Sprintf("%d", f.ID)})
		}
		return &davDir{name: "/content", entries: entries}, nil
	}

	// /content/{id} — sub-directory per file ID
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) == 2 && parts[0] == "content" {
		id, err := parseInt64(parts[1])
		if err != nil {
			return nil, os.ErrNotExist
		}
		f, err := d.opener.OpenVirtualMediaFile(ctx, id)
		if err != nil {
			return nil, os.ErrNotExist
		}
		return &davDir{name: name, entries: []os.FileInfo{
			&davFileInfo{name: f.Name(), size: f.Size()},
		}}, nil
	}

	// /content/{id}/{filename}
	if len(parts) == 3 && parts[0] == "content" {
		id, err := parseInt64(parts[1])
		if err != nil {
			return nil, os.ErrNotExist
		}
		f, err := d.opener.OpenVirtualMediaFile(ctx, id)
		if err != nil {
			return nil, os.ErrNotExist
		}
		return &davFile{ctx: ctx, id: id, name: parts[2], file: f}, nil
	}

	return nil, os.ErrNotExist
}

// ── directory node ─────────────────────────────────────────────────────────

type davDir struct {
	name    string
	entries []os.FileInfo
	pos     int
}

func (d *davDir) Close() error                                    { return nil }
func (d *davDir) Write(_ []byte) (int, error)                     { return 0, os.ErrPermission }
func (d *davDir) Seek(_ int64, _ int) (int64, error)              { return 0, os.ErrInvalid }
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
func (d *davDir) Read(_ []byte) (int, error) { return 0, os.ErrInvalid }
func (d *davDir) Stat() (os.FileInfo, error)  { return &davDirInfo{name: path.Base(d.name)}, nil }

type davDirInfo struct{ name string }

func (i *davDirInfo) Name() string      { return i.name }
func (i *davDirInfo) Size() int64       { return 0 }
func (i *davDirInfo) Mode() fs.FileMode { return fs.ModeDir | 0o555 }
func (i *davDirInfo) ModTime() time.Time { return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) }
func (i *davDirInfo) IsDir() bool       { return true }
func (i *davDirInfo) Sys() any          { return nil }

// ── file node ──────────────────────────────────────────────────────────────

type davFile struct {
	ctx  context.Context
	id   int64
	name string
	file stream.VirtualMediaFile
	pos  int64
}

func (f *davFile) Close() error                   { return nil }
func (f *davFile) Write(_ []byte) (int, error)     { return 0, os.ErrPermission }
func (f *davFile) Readdir(_ int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }
func (f *davFile) Stat() (os.FileInfo, error) {
	return &davFileInfo{name: f.name, size: f.file.Size()}, nil
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

type davFileInfo struct {
	name string
	size int64
}

func (i *davFileInfo) Name() string      { return i.name }
func (i *davFileInfo) Size() int64       { return i.size }
func (i *davFileInfo) Mode() fs.FileMode { return 0o444 }
func (i *davFileInfo) ModTime() time.Time { return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) }
func (i *davFileInfo) IsDir() bool       { return false }
func (i *davFileInfo) Sys() any          { return nil }

// ── helpers ────────────────────────────────────────────────────────────────

func parseInt64(s string) (int64, error) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
