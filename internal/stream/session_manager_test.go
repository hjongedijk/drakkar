package stream

import (
	"context"
	"testing"
	"time"
)

type priorityFetchCall struct {
	segment  SegmentRange
	priority FetchPriority
	ctx      context.Context
}

type priorityFetcherStub struct {
	calls chan priorityFetchCall
}

func (s *priorityFetcherStub) FetchRange(ctx context.Context, segment SegmentRange) ([]byte, error) {
	return s.FetchRangePriority(ctx, segment, PriorityInteractive)
}

func (s *priorityFetcherStub) FetchRangePriority(ctx context.Context, segment SegmentRange, priority FetchPriority) ([]byte, error) {
	if s.calls != nil {
		s.calls <- priorityFetchCall{segment: segment, priority: priority, ctx: ctx}
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestReadAheadManagerNotifyReadUsesReadAheadPriority(t *testing.T) {
	manager := NewReadAheadManager(32)
	fetcher := &priorityFetcherStub{calls: make(chan priorityFetchCall, 1)}
	manager.Register("stream-1", []SegmentSpan{{SegmentID: 1, MessageID: "<msg1>", Start: 0, End: 64}}, fetcher)

	manager.NotifyRead("stream-1", 16)

	select {
	case call := <-fetcher.calls:
		if call.priority != PriorityReadAhead {
			t.Fatalf("expected read-ahead priority, got %d", call.priority)
		}
		if call.segment.RangeStart != 16 || call.segment.RangeEnd != 48 {
			t.Fatalf("unexpected range %#v", call.segment)
		}
		manager.Stop("stream-1")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for read-ahead fetch")
	}
}

func TestReadAheadManagerSeekCancelsPreviousRequest(t *testing.T) {
	manager := NewReadAheadManager(32)
	fetcher := &priorityFetcherStub{calls: make(chan priorityFetchCall, 2)}
	manager.Register("stream-2", []SegmentSpan{{SegmentID: 1, MessageID: "<msg1>", Start: 0, End: 128}}, fetcher)

	manager.NotifyRead("stream-2", 0)
	first := <-fetcher.calls
	manager.Seek("stream-2", 64)

	select {
	case <-first.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected first request context to cancel on seek")
	}

	// Seek only cancels — it does not schedule a new window immediately.
	// The FUSE handle calls NotifyRead after the interactive read completes.
	manager.NotifyRead("stream-2", 64)

	select {
	case second := <-fetcher.calls:
		if second.segment.RangeStart != 64 || second.segment.RangeEnd != 96 {
			t.Fatalf("unexpected second range %#v", second.segment)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for read-ahead after seek+NotifyRead")
	}
	manager.Stop("stream-2")
}

func TestActiveSessionsTracking(t *testing.T) {
	manager := NewReadAheadManager(32)
	fetcher := &priorityFetcherStub{}

	if n := manager.ActiveCount(); n != 0 {
		t.Fatalf("expected 0 sessions initially, got %d", n)
	}
	if ss := manager.ActiveSessions(); len(ss) != 0 {
		t.Fatalf("expected empty sessions, got %v", ss)
	}

	meta := SessionMeta{
		VirtualFileID: 42,
		FileName:      "Dune (2021).mkv",
		FileSizeBytes: 8_000_000_000,
		OpenedAt:      time.Now().UTC(),
	}
	manager.Register("s1", []SegmentSpan{{SegmentID: 1, MessageID: "<m1>", Start: 0, End: 100}}, fetcher, meta)

	if n := manager.ActiveCount(); n != 1 {
		t.Fatalf("expected 1 session, got %d", n)
	}
	ss := manager.ActiveSessions()
	if len(ss) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(ss))
	}
	snap := ss[0]
	if snap.SessionID != "s1" {
		t.Fatalf("unexpected session ID %q", snap.SessionID)
	}
	if snap.VirtualFileID != 42 {
		t.Fatalf("unexpected virtualFileID %d", snap.VirtualFileID)
	}
	if snap.FileName != "Dune (2021).mkv" {
		t.Fatalf("unexpected fileName %q", snap.FileName)
	}
	if snap.FileSizeBytes != 8_000_000_000 {
		t.Fatalf("unexpected size %d", snap.FileSizeBytes)
	}

	// Simulate a read notification updating the current offset.
	manager.NotifyRead("s1", 4096)
	ss2 := manager.ActiveSessions()
	if ss2[0].CurrentOffset != 4096 {
		t.Fatalf("expected offset 4096, got %d", ss2[0].CurrentOffset)
	}

	// RegisterMeta after the fact also updates the snapshot.
	manager.RegisterMeta("s1", SessionMeta{VirtualFileID: 99, FileName: "updated.mkv"})
	ss3 := manager.ActiveSessions()
	if ss3[0].FileName != "updated.mkv" {
		t.Fatalf("expected updated fileName, got %q", ss3[0].FileName)
	}

	// Stop removes the session.
	manager.Stop("s1")
	if n := manager.ActiveCount(); n != 0 {
		t.Fatalf("expected 0 sessions after stop, got %d", n)
	}
}

func TestReadAheadManagerStopCancelsRequest(t *testing.T) {
	manager := NewReadAheadManager(32)
	fetcher := &priorityFetcherStub{calls: make(chan priorityFetchCall, 1)}
	manager.Register("stream-3", []SegmentSpan{{SegmentID: 1, MessageID: "<msg1>", Start: 0, End: 128}}, fetcher)

	manager.NotifyRead("stream-3", 0)
	call := <-fetcher.calls
	manager.Stop("stream-3")

	select {
	case <-call.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected stop to cancel request context")
	}
}

func TestReadAheadManagerCapsArticleBuffer(t *testing.T) {
	manager := NewReadAheadManager(1024)
	manager.SetArticleBufferSize(2)
	fetcher := &priorityFetcherStub{calls: make(chan priorityFetchCall, 8)}
	manager.Register("stream-4", []SegmentSpan{
		{SegmentID: 1, MessageID: "<msg1>", Start: 0, End: 64},
		{SegmentID: 2, MessageID: "<msg2>", Start: 64, End: 128},
		{SegmentID: 3, MessageID: "<msg3>", Start: 128, End: 192},
		{SegmentID: 4, MessageID: "<msg4>", Start: 192, End: 256},
	}, fetcher)

	manager.NotifyRead("stream-4", 0)
	for i := 0; i < 2; i++ {
		select {
		case <-fetcher.calls:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for buffered article fetch")
		}
	}
	manager.Stop("stream-4")
	select {
	case call := <-fetcher.calls:
		t.Fatalf("expected only 2 buffered article fetches, got extra %#v", call.segment)
	case <-time.After(100 * time.Millisecond):
	}
}
