package dav

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/stream"
	"golang.org/x/net/webdav"
)

func init() {
	// Register media MIME types missing from Go's standard library so that
	// http.ServeContent identifies them by extension instead of sniffing the
	// first 512 bytes (which would trigger a needless NNTP segment fetch).
	for ext, typ := range map[string]string{
		".mkv":  "video/x-matroska",
		".mp4":  "video/mp4",
		".avi":  "video/x-msvideo",
		".mov":  "video/quicktime",
		".wmv":  "video/x-ms-wmv",
		".m4v":  "video/x-m4v",
		".ts":   "video/mp2t",
		".m2ts": "video/mp2t",
		".flac": "audio/flac",
		".mp3":  "audio/mpeg",
		".aac":  "audio/aac",
		".ac3":  "audio/ac3",
		".dts":  "audio/vnd.dts",
	} {
		_ = mime.AddExtensionType(ext, typ)
	}
}

var contentModTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// ContentProvider is the database interface needed by the WebDAV handler.
type ContentProvider interface {
	ListContentMountEntries(ctx context.Context) ([]database.ContentMountEntry, error)
	ListContentMountEntriesForRelease(ctx context.Context, selectedReleaseID int64) ([]database.ContentMountEntry, error)
	OpenVirtualMediaFile(ctx context.Context, virtualFileID int64) (stream.VirtualMediaFile, error)
	ListSymlinkPublications(ctx context.Context) ([]database.SymlinkPublication, error)
}

// Handler returns an HTTP handler serving virtual files over WebDAV.
//
// Directory structure mirrors the FUSE mount so existing library symlinks
// continue to work when rclone replaces FUSE at the same mount point:
//
//	/content/releases/{selectedReleaseID}/{filename}   — streaming content
//	/completed-symlinks/movies/{path}/{file}.rclonelink — rclone symlink files
//	/completed-symlinks/tv/{path}/{file}.rclonelink
func Handler(db ContentProvider, movieLibPath, tvLibPath string) http.Handler {
	h := &webdav.Handler{
		FileSystem: &contentFS{
			db:           db,
			movieLibPath: strings.TrimSuffix(movieLibPath, "/"),
			tvLibPath:    strings.TrimSuffix(tvLibPath, "/"),
		},
		LockSystem: webdav.NewMemLS(),
	}
	// Content-Encoding: identity tells rclone that Content-Length is accurate
	// and the stream is not encoded, enabling direct Range-request pass-through
	// without requiring vfs-cache-mode=full.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "identity")
		h.ServeHTTP(w, r)
	})
}

type contentFS struct {
	db           ContentProvider
	movieLibPath string
	tvLibPath    string
}

// parsedPath is the result of decomposing a WebDAV path.
type parsedPath struct {
	section   string // "content", "completed-symlinks", or ""
	rest      string // everything after the section
}

func splitPath(name string) parsedPath {
	name = strings.Trim(name, "/")
	if name == "" {
		return parsedPath{}
	}
	slash := strings.IndexByte(name, '/')
	if slash < 0 {
		return parsedPath{section: name}
	}
	return parsedPath{
		section: name[:slash],
		rest:    name[slash+1:],
	}
}

// --- os.FileInfo helpers ---

type dirInfo struct{ name string }

func (d *dirInfo) Name() string       { return d.name }
func (d *dirInfo) Size() int64        { return 0 }
func (d *dirInfo) Mode() os.FileMode  { return os.ModeDir | 0o555 }
func (d *dirInfo) ModTime() time.Time { return contentModTime }
func (d *dirInfo) IsDir() bool        { return true }
func (d *dirInfo) Sys() any           { return nil }

type fileInfo struct {
	name string
	size int64
}

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() os.FileMode  { return 0o444 }
func (fi *fileInfo) ModTime() time.Time { return contentModTime }
func (fi *fileInfo) IsDir() bool        { return false }
func (fi *fileInfo) Sys() any           { return nil }

// --- webdav.File implementations ---

// dirFile is a read-only directory.
type dirFile struct {
	fi       os.FileInfo
	children []os.FileInfo
	pos      int
}

func (d *dirFile) Close() error                       { return nil }
func (d *dirFile) Read(_ []byte) (int, error)         { return 0, io.EOF }
func (d *dirFile) Write(_ []byte) (int, error)        { return 0, os.ErrPermission }
func (d *dirFile) Seek(_ int64, _ int) (int64, error) { return 0, os.ErrInvalid }
func (d *dirFile) Stat() (os.FileInfo, error)         { return d.fi, nil }

func (d *dirFile) Readdir(count int) ([]os.FileInfo, error) {
	if count <= 0 {
		result := d.children[d.pos:]
		d.pos = len(d.children)
		return result, nil
	}
	if d.pos >= len(d.children) {
		return nil, io.EOF
	}
	end := d.pos + count
	if end > len(d.children) {
		end = len(d.children)
	}
	result := d.children[d.pos:end]
	d.pos = end
	return result, nil
}

// bytesFile serves a static byte slice (used for .rclonelink files).
type bytesFile struct {
	fi  os.FileInfo
	buf []byte
	pos int64
}

func (f *bytesFile) Close() error                       { return nil }
func (f *bytesFile) Write(_ []byte) (int, error)        { return 0, os.ErrPermission }
func (f *bytesFile) Readdir(_ int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }
func (f *bytesFile) Stat() (os.FileInfo, error)         { return f.fi, nil }

func (f *bytesFile) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = f.pos + offset
	case io.SeekEnd:
		newPos = int64(len(f.buf)) + offset
	default:
		return 0, os.ErrInvalid
	}
	if newPos < 0 {
		return 0, os.ErrInvalid
	}
	f.pos = newPos
	return newPos, nil
}

func (f *bytesFile) Read(p []byte) (int, error) {
	if f.pos >= int64(len(f.buf)) {
		return 0, io.EOF
	}
	n := copy(p, f.buf[f.pos:])
	f.pos += int64(n)
	return n, nil
}

// virtualFile streams a VirtualMediaFile over HTTP.
// It mirrors the FUSE handle session lifecycle so read-ahead works identically:
// StartSession on open, NotifyRead after each read, Seek on non-sequential
// access, StopSession on close.
type virtualFile struct {
	ctx         context.Context
	vf          stream.VirtualMediaFile
	fi          os.FileInfo
	pos         int64
	sessionFile stream.SessionVirtualMediaFile // nil for inline/byte files
	sessionID   string
	hasRead     bool  // true after the first actual data read
	lastEnd     int64 // position after the last read, for seek detection
}

func (f *virtualFile) Close() error {
	if f.sessionFile != nil {
		f.sessionFile.StopSession(f.sessionID)
	}
	return nil
}

func (f *virtualFile) Write(_ []byte) (int, error)          { return 0, os.ErrPermission }
func (f *virtualFile) Stat() (os.FileInfo, error)           { return f.fi, nil }
func (f *virtualFile) Readdir(_ int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }

func (f *virtualFile) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = f.pos + offset
	case io.SeekEnd:
		newPos = f.vf.Size() + offset
	default:
		return 0, os.ErrInvalid
	}
	if newPos < 0 {
		return 0, os.ErrInvalid
	}
	// Only cancel read-ahead on a genuine seek (after first real data read).
	// Pre-read Seeks for size detection (Seek(0,End) → Seek(0,Start)) are
	// ignored because hasRead is still false at that point.
	if f.sessionFile != nil && f.hasRead && newPos != f.lastEnd {
		f.sessionFile.Seek(f.sessionID, newPos)
	}
	f.pos = newPos
	return newPos, nil
}

func (f *virtualFile) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	size := f.vf.Size()
	if f.pos >= size {
		return 0, io.EOF
	}
	remaining := size - f.pos
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := f.vf.ReadAt(f.ctx, p, f.pos)
	if n > 0 {
		f.pos += int64(n)
		f.hasRead = true
		f.lastEnd = f.pos
		if f.sessionFile != nil {
			f.sessionFile.NotifyRead(f.sessionID, f.pos)
		}
		if err == io.EOF {
			err = nil // don't mix data with EOF; next Read returns EOF
		}
	}
	return n, err
}

// --- Filesystem read/write stubs ---

func (f *contentFS) Mkdir(_ context.Context, _ string, _ os.FileMode) error {
	return os.ErrPermission
}
func (f *contentFS) RemoveAll(_ context.Context, _ string) error {
	return os.ErrPermission
}
func (f *contentFS) Rename(_ context.Context, _, _ string) error {
	return os.ErrPermission
}

// --- Stat ---

func (f *contentFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	pp := splitPath(name)
	switch pp.section {
	case "":
		return &dirInfo{name: "/"}, nil
	case "content":
		return f.statContent(ctx, pp.rest)
	case "completed-symlinks":
		return f.statCompleted(ctx, pp.rest)
	}
	return nil, os.ErrNotExist
}

func (f *contentFS) statContent(ctx context.Context, rest string) (os.FileInfo, error) {
	rest = strings.Trim(rest, "/")
	if rest == "" || rest == "releases" {
		return &dirInfo{name: "content"}, nil
	}
	if !strings.HasPrefix(rest, "releases/") {
		return nil, os.ErrNotExist
	}
	rest = strings.TrimPrefix(rest, "releases/")
	rest = strings.Trim(rest, "/")

	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		// /content/releases/{id}
		rid, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return nil, os.ErrNotExist
		}
		entries, err := f.db.ListContentMountEntriesForRelease(ctx, rid)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			return nil, os.ErrNotExist
		}
		return &dirInfo{name: rest}, nil
	}
	// /content/releases/{id}/{filename}
	rid, err := strconv.ParseInt(rest[:slash], 10, 64)
	if err != nil {
		return nil, os.ErrNotExist
	}
	filename := rest[slash+1:]
	entries, err := f.db.ListContentMountEntriesForRelease(ctx, rid)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.FileName == filename {
			return &fileInfo{name: e.FileName, size: e.SizeBytes}, nil
		}
	}
	return nil, os.ErrNotExist
}

func (f *contentFS) statCompleted(ctx context.Context, rest string) (os.FileInfo, error) {
	rest = strings.Trim(rest, "/")
	if rest == "" {
		return &dirInfo{name: "completed-symlinks"}, nil
	}
	pubs, err := f.db.ListSymlinkPublications(ctx)
	if err != nil {
		return nil, err
	}
	tree := f.buildTree(pubs)
	node := treeNodeAt(tree, rest)
	if node == nil {
		return nil, os.ErrNotExist
	}
	if node.isFile {
		return &fileInfo{name: filepath.Base(rest), size: int64(len(node.content))}, nil
	}
	return &dirInfo{name: filepath.Base(rest)}, nil
}

// --- OpenFile ---

func (f *contentFS) OpenFile(ctx context.Context, name string, _ int, _ os.FileMode) (webdav.File, error) {
	pp := splitPath(name)
	switch pp.section {
	case "":
		return f.openRoot(ctx)
	case "content":
		return f.openContent(ctx, pp.rest)
	case "completed-symlinks":
		return f.openCompleted(ctx, pp.rest)
	}
	return nil, os.ErrNotExist
}

func (f *contentFS) openRoot(ctx context.Context) (webdav.File, error) {
	children := []os.FileInfo{
		&dirInfo{name: "content"},
		&dirInfo{name: "completed-symlinks"},
	}
	return &dirFile{fi: &dirInfo{name: "/"}, children: children}, nil
}

func (f *contentFS) openContent(ctx context.Context, rest string) (webdav.File, error) {
	rest = strings.Trim(rest, "/")
	if rest == "" {
		// /content/ → one child: releases/
		return &dirFile{
			fi:       &dirInfo{name: "content"},
			children: []os.FileInfo{&dirInfo{name: "releases"}},
		}, nil
	}
	if rest == "releases" {
		// /content/releases/ → list all release IDs
		entries, err := f.db.ListContentMountEntries(ctx)
		if err != nil {
			return nil, err
		}
		seen := make(map[int64]bool)
		var kids []os.FileInfo
		for _, e := range entries {
			if !seen[e.SelectedReleaseID] {
				seen[e.SelectedReleaseID] = true
				kids = append(kids, &dirInfo{name: strconv.FormatInt(e.SelectedReleaseID, 10)})
			}
		}
		return &dirFile{fi: &dirInfo{name: "releases"}, children: kids}, nil
	}
	if !strings.HasPrefix(rest, "releases/") {
		return nil, os.ErrNotExist
	}
	rest = strings.TrimPrefix(rest, "releases/")
	rest = strings.Trim(rest, "/")

	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		// /content/releases/{id}/ → list files
		rid, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return nil, os.ErrNotExist
		}
		entries, err := f.db.ListContentMountEntriesForRelease(ctx, rid)
		if err != nil {
			return nil, err
		}
		var kids []os.FileInfo
		for _, e := range entries {
			kids = append(kids, &fileInfo{name: e.FileName, size: e.SizeBytes})
		}
		if len(kids) == 0 {
			return nil, os.ErrNotExist
		}
		return &dirFile{
			fi:       &dirInfo{name: rest},
			children: kids,
		}, nil
	}
	// /content/releases/{id}/{filename}
	rid, err := strconv.ParseInt(rest[:slash], 10, 64)
	if err != nil {
		return nil, os.ErrNotExist
	}
	filename := rest[slash+1:]
	entries, err := f.db.ListContentMountEntriesForRelease(ctx, rid)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.FileName == filename {
			vf, err := f.db.OpenVirtualMediaFile(ctx, e.VirtualFileID)
			if err != nil {
				return nil, err
			}
			file := &virtualFile{
				ctx: ctx,
				vf:  vf,
				fi:  &fileInfo{name: e.FileName, size: e.SizeBytes},
			}
			if sf, ok := vf.(stream.SessionVirtualMediaFile); ok {
				file.sessionFile = sf
				file.sessionID = fmt.Sprintf("dav-%d-%d", e.VirtualFileID, time.Now().UnixNano())
				sf.StartSession(file.sessionID)
				sf.RegisterMeta(file.sessionID, stream.SessionMeta{
					VirtualFileID: e.VirtualFileID,
					FileName:      e.FileName,
					FileSizeBytes: e.SizeBytes,
					OpenedAt:      time.Now().UTC(),
				})
				sf.NotifyRead(file.sessionID, 0)
			}
			return file, nil
		}
	}
	return nil, os.ErrNotExist
}

func (f *contentFS) openCompleted(ctx context.Context, rest string) (webdav.File, error) {
	rest = strings.Trim(rest, "/")
	pubs, err := f.db.ListSymlinkPublications(ctx)
	if err != nil {
		return nil, err
	}
	tree := f.buildTree(pubs)

	if rest == "" {
		// /completed-symlinks/ → list top-level children
		return dirFileFromNode(&dirInfo{name: "completed-symlinks"}, tree), nil
	}
	node := treeNodeAt(tree, rest)
	if node == nil {
		return nil, os.ErrNotExist
	}
	name := filepath.Base(rest)
	if node.isFile {
		return &bytesFile{
			fi:  &fileInfo{name: name, size: int64(len(node.content))},
			buf: node.content,
		}, nil
	}
	return dirFileFromNode(&dirInfo{name: name}, node), nil
}

// --- Completed-symlinks tree ---

type treeNode struct {
	isFile   bool
	content  []byte
	children map[string]*treeNode
}

func (f *contentFS) buildTree(pubs []database.SymlinkPublication) *treeNode {
	root := &treeNode{children: make(map[string]*treeNode)}
	for _, pub := range pubs {
		relPath := f.relPath(pub.LibraryPath)
		if relPath == "" {
			continue
		}
		parts := strings.Split(filepath.ToSlash(relPath), "/")
		node := root
		for i, part := range parts {
			if i == len(parts)-1 {
				linkName := part + ".rclonelink"
				node.children[linkName] = &treeNode{
					isFile:  true,
					content: []byte(pub.TargetPath),
				}
			} else {
				if _, ok := node.children[part]; !ok {
					node.children[part] = &treeNode{children: make(map[string]*treeNode)}
				}
				node = node.children[part]
			}
		}
	}
	return root
}

func (f *contentFS) relPath(libraryPath string) string {
	if f.movieLibPath != "" && strings.HasPrefix(libraryPath, f.movieLibPath+"/") {
		return "movies/" + strings.TrimPrefix(libraryPath, f.movieLibPath+"/")
	}
	if f.tvLibPath != "" && strings.HasPrefix(libraryPath, f.tvLibPath+"/") {
		return "tv/" + strings.TrimPrefix(libraryPath, f.tvLibPath+"/")
	}
	base := filepath.Base(libraryPath)
	if base == "" || base == "." {
		return ""
	}
	return base
}

func treeNodeAt(root *treeNode, path string) *treeNode {
	if path == "" {
		return root
	}
	parts := strings.Split(filepath.ToSlash(path), "/")
	node := root
	for _, part := range parts {
		if part == "" {
			continue
		}
		child, ok := node.children[part]
		if !ok {
			return nil
		}
		node = child
	}
	return node
}

func dirFileFromNode(fi os.FileInfo, node *treeNode) *dirFile {
	kids := make([]os.FileInfo, 0, len(node.children))
	for name, child := range node.children {
		if child.isFile {
			kids = append(kids, &fileInfo{name: name, size: int64(len(child.content))})
		} else {
			kids = append(kids, &dirInfo{name: name})
		}
	}
	return &dirFile{fi: fi, children: kids}
}
