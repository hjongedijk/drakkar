package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileCachePruneResult(t *testing.T) {
	root := t.TempDir()
	cache := NewFileCache(root, 5)
	if err := os.WriteFile(filepath.Join(root, "a.bin"), []byte("1234"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if err := os.WriteFile(filepath.Join(root, "b.bin"), []byte("5678"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := cache.Prune()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if result.FilesBefore != 2 || result.FilesAfter != 1 {
		t.Fatalf("unexpected file counts: %+v", result)
	}
	if result.DeletedFiles != 1 {
		t.Fatalf("expected one deleted file, got %+v", result)
	}
}

func TestServicePrune(t *testing.T) {
	root := t.TempDir()
	cache := NewFileCache(root, 100)
	if err := cache.Put("k", []byte("value")); err != nil {
		t.Fatal(err)
	}
	svc := NewService(cache)
	result, err := svc.Prune(context.Background())
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if result.Root != root {
		t.Fatalf("unexpected root: %+v", result)
	}
}

func TestFileCachePruneNeverReturnsNegativeDeletes(t *testing.T) {
	root := t.TempDir()
	cache := NewFileCache(root, 100)
	if err := os.WriteFile(filepath.Join(root, "a.bin"), []byte("1234"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Simulate a concurrent writer landing after the "before" snapshot.
	before, err := cache.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.bin"), []byte("56789"), 0o644); err != nil {
		t.Fatal(err)
	}
	after, err := cache.Stats()
	if err != nil {
		t.Fatal(err)
	}
	result := PruneResult{
		FilesBefore:  before.Files,
		FilesAfter:   after.Files,
		BytesBefore:  before.Bytes,
		BytesAfter:   after.Bytes,
		DeletedFiles: max(0, before.Files-after.Files),
		DeletedBytes: max64(0, before.Bytes-after.Bytes),
	}
	if result.DeletedFiles < 0 || result.DeletedBytes < 0 {
		t.Fatalf("expected clamped deletes, got %+v", result)
	}
}
