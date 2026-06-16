package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	gobullmq "go.codycody31.dev/gobullmq"
)

const bullmqQueueName = "drakkar:search"

// WorkQueuer is the interface consumed by Service. The real implementation
// is backed by BullMQ/Redis; tests can provide a lightweight stub.
type WorkQueuer interface {
	Push(ctx context.Context, libraryItemID int64, priority int)
	Depth(ctx context.Context) int64
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	IsPaused(ctx context.Context) (bool, error)
	Start(ctx context.Context, fn func(ctx context.Context, libraryItemID int64)) error
}

type searchJob struct {
	LibraryItemID int64 `json:"libraryItemId"`
}

// WorkQueue wraps a BullMQ-backed queue for library item search jobs.
// Jobs are deduplicated by library item ID — pushing an item already waiting
// in the queue is a no-op (BullMQ ignores duplicate job IDs).
//
// Per gobullmq docs, the queue client and worker client must be separate
// Redis connections to avoid CLIENT SETNAME collisions.
type WorkQueue struct {
	queue        *gobullmq.Queue[searchJob]
	queueClient  redis.Cmdable // dedicated to Queue.Add calls
	workerClient redis.Cmdable // dedicated to Worker (separate connection)
	workers      int
}

// NewWorkQueue creates a BullMQ-backed work queue.
// queueClient is used for enqueueing; workerClient is used by the worker pool.
// Pass two separate *redis.Client instances to avoid CLIENT SETNAME collisions.
func NewWorkQueue(workers int, queueClient, workerClient redis.Cmdable) (*WorkQueue, error) {
	if workers < 1 {
		workers = 1
	}
	q, err := gobullmq.NewQueue[searchJob](bullmqQueueName, queueClient, &gobullmq.QueueOptions{
		Prefix: "bull",
	})
	if err != nil {
		return nil, fmt.Errorf("workqueue: create queue: %w", err)
	}
	return &WorkQueue{
		queue:        q,
		queueClient:  queueClient,
		workerClient: workerClient,
		workers:      workers,
	}, nil
}

// Push enqueues a library item search job. High-priority items (priority > 0)
// are placed ahead of normal items. Duplicate job IDs are ignored by BullMQ,
// so pushing an already-queued item is safe and cheap.
func (q *WorkQueue) Push(ctx context.Context, libraryItemID int64, priority int) {
	bullPriority := 1
	if priority > 0 {
		bullPriority = 10
	}
	_, _ = q.queue.Add(ctx, "search", searchJob{LibraryItemID: libraryItemID},
		gobullmq.AddWithJobID(fmt.Sprintf("%d", libraryItemID)),
		gobullmq.AddWithPriority(bullPriority),
		gobullmq.AddWithRemoveOnComplete(),
		gobullmq.AddWithRemoveOnFail(),
	)
}

// Depth returns the number of jobs currently waiting in the queue.
func (q *WorkQueue) Depth(ctx context.Context) int64 {
	n, _ := q.queue.GetWaitingCount(ctx)
	return n
}

func (q *WorkQueue) Pause(ctx context.Context) error {
	return q.queue.Pause(ctx)
}

func (q *WorkQueue) Resume(ctx context.Context) error {
	return q.queue.Resume(ctx)
}

func (q *WorkQueue) IsPaused(ctx context.Context) (bool, error) {
	return q.queue.IsPaused(ctx)
}

// Start launches the BullMQ worker pool. Blocks until ctx is cancelled.
func (q *WorkQueue) Start(ctx context.Context, fn func(ctx context.Context, libraryItemID int64)) error {
	processor := func(ctx context.Context, job *gobullmq.Job[searchJob]) (struct{}, error) {
		fn(ctx, job.Data().LibraryItemID)
		return struct{}{}, nil
	}
	worker, err := gobullmq.NewWorker[searchJob, struct{}](
		bullmqQueueName,
		q.workerClient,
		processor,
		&gobullmq.WorkerOptions{
			Concurrency:      q.workers,
			RemoveOnComplete: &gobullmq.KeepJobs{Count: 0},
			RemoveOnFail:     &gobullmq.KeepJobs{Count: 0},
			// A single library item can chain through many expired-NZB candidates and
			// large file downloads (2-4 GB). 30s default lock causes stalled-check
			// re-queues mid-processing, racing two workers on the same item (FK violations).
			// Lock renewal runs every LockDuration/4 so 10-min lock = renewal every 2.5 min.
			LockDuration:    10 * time.Minute,
			StalledInterval: 5 * time.Minute,
			// Allow one recovery attempt before failing: if the stalled check fires
			// while the lock is briefly between renewals, the job is moved back to
			// wait instead of immediately to failed (MaxStalledCount=0 default).
			MaxStalledCount: 1,
		},
	)
	if err != nil {
		return fmt.Errorf("workqueue: create worker: %w", err)
	}
	return worker.Run(ctx)
}
