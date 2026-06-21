package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"

	"github.com/hjongedijk/drakkar/internal/metrics"
)

type FileCache struct {
	root     string
	maxBytes int64
}

type DirStats struct {
	Files int   `json:"files"`
	Bytes int64 `json:"bytes"`
}

type PruneResult struct {
	Root         string `json:"root"`
	FilesBefore  int    `json:"filesBefore"`
	FilesAfter   int    `json:"filesAfter"`
	BytesBefore  int64  `json:"bytesBefore"`
	BytesAfter   int64  `json:"bytesAfter"`
	DeletedFiles int    `json:"deletedFiles"`
	DeletedBytes int64  `json:"deletedBytes"`
	LimitBytes   int64  `json:"limitBytes"`
}

func NewFileCache(root string, maxBytes int64) *FileCache {
	return &FileCache{root: root, maxBytes: maxBytes}
}

func (c *FileCache) Get(key string) ([]byte, bool, error) {
	path := c.pathFor(key)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	now := unixNow()
	_ = os.Chtimes(path, now, now)
	return data, true, nil
}

func (c *FileCache) Put(key string, value []byte) error {
	if err := os.MkdirAll(c.root, 0o755); err != nil {
		return err
	}
	path := c.pathFor(key)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, value, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return c.Trim()
}

func (c *FileCache) Stats() (DirStats, error) {
	entries, err := os.ReadDir(c.root)
	if err != nil {
		if os.IsNotExist(err) {
			return DirStats{}, nil
		}
		return DirStats{}, err
	}
	var stats DirStats
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return DirStats{}, err
		}
		stats.Files++
		stats.Bytes += info.Size()
	}
	return stats, nil
}

func (c *FileCache) Trim() error {
	if c.maxBytes <= 0 {
		return nil
	}
	entries, err := os.ReadDir(c.root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	type item struct {
		path    string
		size    int64
		modTime int64
	}
	var items []item
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		items = append(items, item{
			path:    filepath.Join(c.root, entry.Name()),
			size:    info.Size(),
			modTime: info.ModTime().UnixNano(),
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].modTime < items[j].modTime })
	for total > c.maxBytes && len(items) > 0 {
		target := items[0]
		items = items[1:]
		if err := os.Remove(target.path); err == nil {
			total -= target.size
			metrics.M.CacheEvictions.Add(1)
		}
	}
	return nil
}

func (c *FileCache) Prune() (PruneResult, error) {
	before, err := c.Stats()
	if err != nil {
		return PruneResult{}, err
	}
	if err := c.Trim(); err != nil {
		return PruneResult{}, err
	}
	after, err := c.Stats()
	if err != nil {
		return PruneResult{}, err
	}
	return PruneResult{
		Root:         c.root,
		FilesBefore:  before.Files,
		FilesAfter:   after.Files,
		BytesBefore:  before.Bytes,
		BytesAfter:   after.Bytes,
		DeletedFiles: max(0, before.Files-after.Files),
		DeletedBytes: max64(0, before.Bytes-after.Bytes),
		LimitBytes:   c.maxBytes,
	}, nil
}

func (c *FileCache) pathFor(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(c.root, hex.EncodeToString(sum[:])+".bin")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
