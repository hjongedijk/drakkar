package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/nzb"
	"github.com/hjongedijk/drakkar/internal/stream"
)

var syntheticTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

type Provider interface {
	ListNZBMountEntries(ctx context.Context) ([]database.NZBMountEntry, error)
	ListContentMountEntries(ctx context.Context) ([]database.ContentMountEntry, error)
	ListCompletedSymlinkEntries(ctx context.Context) ([]database.CompletedSymlinkEntry, error)
	OpenVirtualMediaFile(ctx context.Context, virtualFileID int64) (stream.VirtualMediaFile, error)
	CancelNZBDocument(ctx context.Context, nzbDocumentID int64) error
	ImportNZBPath(ctx context.Context, fileName, path string) (database.QueueSnapshot, error)
}

type DirNode struct {
	fs.Inode
	name     string
	children map[string]fs.InodeEmbedder
}

type ContentDirNode struct {
	fs.Inode
	provider Provider
}

type CompletedSymlinksDirNode struct {
	fs.Inode
	provider Provider
}

type ReleasesDirNode struct {
	fs.Inode
	provider Provider
}

type ReleaseDirNode struct {
	fs.Inode
	provider  Provider
	releaseID int64
}

type VirtualFileNode struct {
	fs.Inode
	provider      Provider
	virtualFileID int64
	fileName      string
	sizeBytes     int64
}

type NZBsDirNode struct {
	fs.Inode
	provider       Provider
	stagingDir     string
	maxUploadBytes int64
}

type visibleNZBEntry struct {
	VisibleName string
	Entry       database.NZBMountEntry
}

type UploadNode struct {
	fs.Inode
	name     string
	path     string
	size     int64
	mu       sync.Mutex
	imported bool
	dir      *NZBsDirNode
}

type uploadHandle struct {
	file *os.File
	node *UploadNode
}

type virtualFileHandle struct {
	reader      stream.VirtualMediaFile
	sessionID   string
	sessionFile stream.SessionVirtualMediaFile
	lastEnd     int64
	hasRead     bool
}

// LazyUnmount tries to detach a stale FUSE mount left by a crashed process.
// It never returns an error — failure just means there was nothing to unmount.
func LazyUnmount(path string) error {
	// go-fuse exposes no lazy-unmount API; exec fusermount3 which is present in the container.
	// Failure is intentionally silenced.
	cmd := exec.Command("fusermount3", "-u", "-z", path)
	_ = cmd.Run()
	return nil
}

func Mount(path, stagingDir string, maxUploadBytes int64, provider Provider) (*gofuse.Server, error) {
	root := newRootNode(provider, stagingDir, maxUploadBytes)
	server, err := fs.Mount(path, root, &fs.Options{
		MountOptions: gofuse.MountOptions{
			FsName:       "drakkar",
			Name:         "drakkar",
			MaxWrite:     4 * 1024 * 1024, // 4 MiB read/write size — reduces round-trips for streaming
			MaxReadAhead: 4 * 1024 * 1024, // 4 MiB kernel read-ahead
		},
	})
	if err != nil {
		return nil, err
	}
	return server, nil
}

func newRootNode(provider Provider, stagingDir string, maxUploadBytes int64) *DirNode {
	root := &DirNode{name: ""}
	root.children = map[string]fs.InodeEmbedder{
		".ids":               &DirNode{name: ".ids"},
		"completed-symlinks": &CompletedSymlinksDirNode{provider: provider},
		"content":            &ContentDirNode{provider: provider},
		"nzbs":               &NZBsDirNode{provider: provider, stagingDir: stagingDir, maxUploadBytes: maxUploadBytes},
	}
	return root
}

func (n *DirNode) OnAdd(ctx context.Context) {
	for name, child := range n.children {
		n.AddChild(name, n.NewPersistentInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), false)
	}
}

func (n *DirNode) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	out.Mode = 0o755 | syscall.S_IFDIR
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return 0
}

func (n *DirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]gofuse.DirEntry, 0, len(n.children))
	names := make([]string, 0, len(n.children))
	for name := range n.children {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entries = append(entries, gofuse.DirEntry{Name: name, Mode: syscall.S_IFDIR})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *DirNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if _, ok := n.children[name]; !ok {
		return nil, syscall.ENOENT
	}
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return n.GetChild(name), 0
}

func (n *NZBsDirNode) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	out.Mode = 0o755 | syscall.S_IFDIR
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return 0
}

func (n *NZBsDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	visible, errno := n.visibleEntries(ctx)
	if errno != 0 {
		return nil, errno
	}
	entries := make([]gofuse.DirEntry, 0, len(visible))
	for _, item := range visible {
		entries = append(entries, gofuse.DirEntry{
			Name: item.VisibleName,
			Mode: syscall.S_IFREG,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *NZBsDirNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	visible, errno := n.visibleEntries(ctx)
	if errno != 0 {
		return nil, errno
	}
	for _, item := range visible {
		if item.VisibleName != name {
			continue
		}
		node := &fs.MemRegularFile{
			Data: item.Entry.XML,
			Attr: gofuse.Attr{
				Mode:  0o644 | syscall.S_IFREG,
				Mtime: uint64(syntheticTime.Unix()),
				Atime: uint64(syntheticTime.Unix()),
				Ctime: uint64(syntheticTime.Unix()),
			},
		}
		child := n.NewPersistentInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
		n.AddChild(name, child, true)
		out.Size = uint64(len(item.Entry.XML))
		out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
		return child, 0
	}
	return nil, syscall.ENOENT
}

func (n *NZBsDirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	visible, errno := n.visibleEntries(ctx)
	if errno != 0 {
		return errno
	}
	for _, item := range visible {
		if item.VisibleName == name {
			if err := n.provider.CancelNZBDocument(ctx, item.Entry.DocumentID); err != nil {
				return syscall.EIO
			}
			return 0
		}
	}
	return syscall.ENOENT
}

func (n *NZBsDirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if err := os.MkdirAll(n.stagingDir, 0o755); err != nil {
		return nil, nil, 0, syscall.EIO
	}
	tempFile, err := os.CreateTemp(n.stagingDir, "*.nzb")
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	node := &UploadNode{
		name: name,
		path: tempFile.Name(),
		dir:  n,
	}
	child := n.NewPersistentInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	n.AddChild(name, child, true)
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return child, &uploadHandle{file: tempFile, node: node}, 0, 0
}

func (n *NZBsDirNode) visibleEntries(ctx context.Context) ([]visibleNZBEntry, syscall.Errno) {
	items, err := n.provider.ListNZBMountEntries(ctx)
	if err != nil {
		return nil, syscall.EIO
	}
	return makeVisibleNZBEntries(items), 0
}

func makeVisibleNZBEntries(items []database.NZBMountEntry) []visibleNZBEntry {
	counts := make(map[string]int)
	out := make([]visibleNZBEntry, 0, len(items))
	for _, item := range items {
		base := sanitizeNZBFileName(item.FileName)
		if base == "" {
			base = "document.nzb"
		}
		counts[base]++
		name := base
		if counts[base] > 1 {
			ext := filepath.Ext(base)
			stem := strings.TrimSuffix(base, ext)
			name = stem + "." + strconv.FormatInt(item.DocumentID, 10) + ext
		}
		out = append(out, visibleNZBEntry{
			VisibleName: name,
			Entry:       item,
		})
	}
	return out
}

func sanitizeNZBFileName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "." || name == "" || name == "/" {
		return "document.nzb"
	}
	if !strings.HasSuffix(strings.ToLower(name), ".nzb") {
		name += ".nzb"
	}
	return name
}

func (n *CompletedSymlinksDirNode) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	out.Mode = 0o755 | syscall.S_IFDIR
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return 0
}

func (n *CompletedSymlinksDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	items, err := n.provider.ListCompletedSymlinkEntries(ctx)
	if err != nil {
		return nil, syscall.EIO
	}
	entries := make([]gofuse.DirEntry, 0, len(items))
	for _, item := range items {
		entries = append(entries, gofuse.DirEntry{Name: item.Name, Mode: syscall.S_IFLNK})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *CompletedSymlinksDirNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	items, err := n.provider.ListCompletedSymlinkEntries(ctx)
	if err != nil {
		return nil, syscall.EIO
	}
	for _, item := range items {
		if item.Name != name {
			continue
		}
		child := n.NewPersistentInode(ctx, &fs.MemSymlink{
			Data: []byte(item.TargetPath),
			Attr: gofuse.Attr{
				Mode: 0o777 | syscall.S_IFLNK,
				Size: uint64(len(item.TargetPath)),
			},
		}, fs.StableAttr{Mode: syscall.S_IFLNK})
		n.AddChild(name, child, true)
		out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
		return child, 0
	}
	return nil, syscall.ENOENT
}

func (n *ContentDirNode) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	out.Mode = 0o755 | syscall.S_IFDIR
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return 0
}

func (n *ContentDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewListDirStream([]gofuse.DirEntry{{Name: "releases", Mode: syscall.S_IFDIR}}), 0
}

func (n *ContentDirNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name != "releases" {
		return nil, syscall.ENOENT
	}
	child := n.NewPersistentInode(ctx, &ReleasesDirNode{provider: n.provider}, fs.StableAttr{Mode: syscall.S_IFDIR})
	n.AddChild(name, child, true)
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return child, 0
}

func (n *ReleasesDirNode) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	out.Mode = 0o755 | syscall.S_IFDIR
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return 0
}

func (n *ReleasesDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, errno := n.releaseDirEntries(ctx)
	if errno != 0 {
		return nil, errno
	}
	return fs.NewListDirStream(entries), 0
}

func (n *ReleasesDirNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	releaseID, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, syscall.ENOENT
	}
	items, errno := n.contentItems(ctx)
	if errno != 0 {
		return nil, errno
	}
	for _, item := range items {
		if item.SelectedReleaseID != releaseID {
			continue
		}
		child := n.NewPersistentInode(ctx, &ReleaseDirNode{provider: n.provider, releaseID: releaseID}, fs.StableAttr{Mode: syscall.S_IFDIR})
		n.AddChild(name, child, true)
		out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
		return child, 0
	}
	return nil, syscall.ENOENT
}

func (n *ReleasesDirNode) releaseDirEntries(ctx context.Context) ([]gofuse.DirEntry, syscall.Errno) {
	items, errno := n.contentItems(ctx)
	if errno != 0 {
		return nil, errno
	}
	seen := make(map[int64]struct{})
	var entries []gofuse.DirEntry
	for _, item := range items {
		if _, ok := seen[item.SelectedReleaseID]; ok {
			continue
		}
		seen[item.SelectedReleaseID] = struct{}{}
		entries = append(entries, gofuse.DirEntry{
			Name: strconv.FormatInt(item.SelectedReleaseID, 10),
			Mode: syscall.S_IFDIR,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, 0
}

func (n *ReleasesDirNode) contentItems(ctx context.Context) ([]database.ContentMountEntry, syscall.Errno) {
	items, err := n.provider.ListContentMountEntries(ctx)
	if err != nil {
		return nil, syscall.EIO
	}
	return items, 0
}

func (n *ReleaseDirNode) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	out.Mode = 0o755 | syscall.S_IFDIR
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return 0
}

func (n *ReleaseDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	items, errno := n.releaseItems(ctx)
	if errno != 0 {
		return nil, errno
	}
	entries := make([]gofuse.DirEntry, 0, len(items))
	for _, item := range items {
		entries = append(entries, gofuse.DirEntry{Name: item.FileName, Mode: syscall.S_IFREG})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *ReleaseDirNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	items, errno := n.releaseItems(ctx)
	if errno != 0 {
		return nil, errno
	}
	for _, item := range items {
		if item.FileName != name {
			continue
		}
		child := n.NewPersistentInode(ctx, &VirtualFileNode{
			provider:      n.provider,
			virtualFileID: item.VirtualFileID,
			fileName:      item.FileName,
			sizeBytes:     item.SizeBytes,
		}, fs.StableAttr{Mode: syscall.S_IFREG})
		n.AddChild(name, child, true)
		out.Size = uint64(item.SizeBytes)
		out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
		return child, 0
	}
	return nil, syscall.ENOENT
}

func (n *ReleaseDirNode) releaseItems(ctx context.Context) ([]database.ContentMountEntry, syscall.Errno) {
	items, err := n.provider.ListContentMountEntries(ctx)
	if err != nil {
		return nil, syscall.EIO
	}
	var out []database.ContentMountEntry
	for _, item := range items {
		if item.SelectedReleaseID == n.releaseID {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FileName < out[j].FileName })
	return out, 0
}

func (n *VirtualFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	out.Mode = 0o444 | syscall.S_IFREG
	out.Size = uint64(n.sizeBytes)
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return 0
}

func (n *VirtualFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	reader, err := n.provider.OpenVirtualMediaFile(ctx, n.virtualFileID)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	handle := &virtualFileHandle{reader: reader}
	if sessionFile, ok := reader.(stream.SessionVirtualMediaFile); ok {
		handle.sessionFile = sessionFile
		handle.sessionID = fmt.Sprintf("%d-%d", n.virtualFileID, time.Now().UnixNano())
		sessionFile.StartSession(handle.sessionID)
		sessionFile.RegisterMeta(handle.sessionID, stream.SessionMeta{
			VirtualFileID: n.virtualFileID,
			FileName:      n.fileName,
			FileSizeBytes: n.sizeBytes,
			OpenedAt:      time.Now().UTC(),
		})
		// Prime first read-ahead window immediately so players do not wait
		// for the second/third article fetch before startup settles.
		sessionFile.NotifyRead(handle.sessionID, 0)
	}
	return handle, gofuse.FOPEN_KEEP_CACHE, 0
}

func (n *VirtualFileNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	if handle, ok := f.(*virtualFileHandle); ok {
		return handle.Read(ctx, dest, off)
	}
	reader, err := n.provider.OpenVirtualMediaFile(ctx, n.virtualFileID)
	if err != nil {
		return nil, syscall.EIO
	}
	data := make([]byte, len(dest))
	read, readErr := reader.ReadAt(ctx, data, off)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, syscall.EIO
	}
	return gofuse.ReadResultData(data[:read]), 0
}

func (h *virtualFileHandle) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	if h == nil || h.reader == nil {
		return nil, syscall.EIO
	}
	if h.sessionFile != nil && h.hasRead && off != h.lastEnd {
		h.sessionFile.Seek(h.sessionID, off)
	}
	data := make([]byte, len(dest))
	read, readErr := h.reader.ReadAt(ctx, data, off)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, syscall.EIO
	}
	h.hasRead = true
	h.lastEnd = off + int64(read)
	if h.sessionFile != nil {
		h.sessionFile.NotifyRead(h.sessionID, h.lastEnd)
	}
	return gofuse.ReadResultData(data[:read]), 0
}

func (h *virtualFileHandle) Release(ctx context.Context) syscall.Errno {
	if h != nil && h.sessionFile != nil {
		h.sessionFile.StopSession(h.sessionID)
	}
	return 0
}

func (n *UploadNode) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()
	out.Mode = 0o644 | syscall.S_IFREG
	out.Size = uint64(n.size)
	out.SetTimes(&syntheticTime, &syntheticTime, &syntheticTime)
	return 0
}

func (n *UploadNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	file, err := os.OpenFile(n.path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &uploadHandle{file: file, node: n}, 0, 0
}

func (h *uploadHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	info, err := h.file.Stat()
	if err != nil {
		return 0, syscall.EIO
	}
	newSize := max64(info.Size(), off+int64(len(data)))
	if newSize > h.node.dir.maxUploadBytes {
		return 0, syscall.ENOSPC
	}
	written, err := h.file.WriteAt(data, off)
	if err != nil {
		return uint32(written), syscall.EIO
	}
	h.node.mu.Lock()
	if end := off + int64(written); end > h.node.size {
		h.node.size = end
	}
	h.node.mu.Unlock()
	return uint32(written), 0
}

func (h *uploadHandle) Flush(ctx context.Context) syscall.Errno {
	if err := h.commit(ctx); err != nil {
		if errors.Is(err, errAlreadyImported) {
			return 0
		}
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		if errors.Is(err, nzb.ErrUploadTooLarge) {
			return syscall.ENOSPC
		}
		return syscall.EIO
	}
	return 0
}

func (h *uploadHandle) Release(ctx context.Context) syscall.Errno {
	defer h.file.Close()
	if err := h.commit(ctx); err != nil && !errors.Is(err, errAlreadyImported) {
		if errors.Is(err, nzb.ErrUploadTooLarge) {
			return syscall.ENOSPC
		}
		return syscall.EIO
	}
	return 0
}

var errAlreadyImported = errors.New("already imported")

func (h *uploadHandle) commit(ctx context.Context) error {
	h.node.mu.Lock()
	if h.node.imported {
		h.node.mu.Unlock()
		return errAlreadyImported
	}
	// Mark imported while holding the lock to prevent a concurrent Flush
	// from also calling ImportNZBPath (TOCTOU). Reset on failure.
	h.node.imported = true
	h.node.mu.Unlock()

	if err := h.file.Sync(); err != nil {
		h.node.mu.Lock()
		h.node.imported = false
		h.node.mu.Unlock()
		return err
	}
	if _, err := h.file.Seek(0, 0); err != nil {
		h.node.mu.Lock()
		h.node.imported = false
		h.node.mu.Unlock()
		return err
	}
	if _, err := h.node.dir.provider.ImportNZBPath(ctx, h.node.name, h.node.path); err != nil {
		h.node.mu.Lock()
		h.node.imported = false
		h.node.mu.Unlock()
		if errors.Is(err, syscall.ENOSPC) {
			return err
		}
		return err
	}

	_ = os.Remove(h.node.path)
	if h.node.dir != nil {
		h.node.dir.RmChild(h.node.name)
	}
	return nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
