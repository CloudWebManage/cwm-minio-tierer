package updater

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orihoch/cwm-minio-tierer/internal/redisstore"
)

var (
	ErrNotRunning      = errors.New("batcher is not accepting work")
	ErrQueueFull       = errors.New("batcher queue is full")
	ErrRequestTooLarge = errors.New("request exceeds batch limits")
	ErrInvalidRequest  = errors.New("invalid batch request")
	ErrBatcherStopped  = errors.New("batcher stopped before Redis execution")
)

type CounterStore interface {
	Apply(context.Context, []redisstore.Increment) error
}

type BatchObserver interface {
	SetQueueDepth(int)
	ObserveBatch(events, uniqueKeys int, latency time.Duration, err error)
}

type BatcherConfig struct {
	QueueSize        int
	MaxEvents        int
	MaxKeys          int
	MaxWait          time.Duration
	OperationTimeout time.Duration
	Logger           *slog.Logger
}

type BatchRequest struct {
	Events     int
	Increments []redisstore.Increment
}

type Batcher struct {
	config   BatcherConfig
	store    CounterStore
	observer BatchObserver
	logger   *slog.Logger

	queue     chan *batchJob
	stop      chan struct{}
	done      chan struct{}
	runCtx    context.Context
	cancelRun context.CancelFunc

	stateMu   sync.Mutex
	started   bool
	accepting bool
	stopOnce  sync.Once
	ready     atomic.Bool
	depth     atomic.Int64
}

type batchJob struct {
	request BatchRequest
	result  chan error
}

type pendingBatch struct {
	jobs       []*batchJob
	events     int
	increments map[string]redisstore.Increment
}

func NewBatcher(config BatcherConfig, store CounterStore, observer BatchObserver) *Batcher {
	runCtx, cancelRun := context.WithCancel(context.Background())
	return &Batcher{
		config:    config,
		store:     store,
		observer:  observer,
		logger:    config.Logger,
		queue:     make(chan *batchJob, config.QueueSize),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		runCtx:    runCtx,
		cancelRun: cancelRun,
	}
}

func (b *Batcher) Start() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.started {
		return
	}
	b.started = true
	b.accepting = true
	b.ready.Store(true)
	go b.run()
}

func (b *Batcher) Submit(ctx context.Context, request BatchRequest) error {
	if err := b.validateRequest(request); err != nil {
		return err
	}
	job := &batchJob{request: request, result: make(chan error, 1)}

	b.stateMu.Lock()
	if !b.accepting {
		b.stateMu.Unlock()
		return ErrNotRunning
	}
	depth := int(b.depth.Add(1))
	b.observeQueueDepth(depth)
	select {
	case b.queue <- job:
		b.stateMu.Unlock()
	default:
		depth = int(b.depth.Add(-1))
		b.observeQueueDepth(depth)
		b.stateMu.Unlock()
		return ErrQueueFull
	}

	select {
	case err := <-job.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher) Stop(ctx context.Context) error {
	b.stateMu.Lock()
	if !b.started {
		b.stateMu.Unlock()
		return nil
	}
	b.accepting = false
	b.ready.Store(false)
	b.stopOnce.Do(func() { close(b.stop) })
	b.stateMu.Unlock()

	select {
	case <-b.done:
		b.cancelRun()
		return nil
	case <-ctx.Done():
		b.cancelRun()
		<-b.done
		return ctx.Err()
	}
}

func (b *Batcher) Ready() bool {
	return b.ready.Load()
}

func (b *Batcher) QueueDepth() int {
	return int(b.depth.Load())
}

func (b *Batcher) validateRequest(request BatchRequest) error {
	if request.Events <= 0 || len(request.Increments) == 0 {
		return ErrInvalidRequest
	}
	if request.Events > b.config.MaxEvents || len(request.Increments) > b.config.MaxKeys {
		return ErrRequestTooLarge
	}
	seen := make(map[string]struct{}, len(request.Increments))
	var total int64
	for _, increment := range request.Increments {
		if increment.Key == "" || increment.Delta <= 0 || increment.ExpireAt.IsZero() {
			return ErrInvalidRequest
		}
		if _, duplicate := seen[increment.Key]; duplicate {
			return fmt.Errorf("%w: duplicate key", ErrInvalidRequest)
		}
		seen[increment.Key] = struct{}{}
		total += increment.Delta
	}
	if total != int64(request.Events) {
		return fmt.Errorf("%w: event count does not match increments", ErrInvalidRequest)
	}
	return nil
}

func (b *Batcher) run() {
	defer close(b.done)
	var pending *pendingBatch
	var timer *time.Timer
	var timerC <-chan time.Time

	for {
		if pending == nil {
			select {
			case job := <-b.queue:
				b.dequeued()
				pending = newPendingBatch(job)
				if pending.full(b.config) {
					b.flush(pending)
					pending = nil
					continue
				}
				timer = time.NewTimer(b.config.MaxWait)
				timerC = timer.C
			case <-b.stop:
				b.drain(nil)
				return
			case <-b.runCtx.Done():
				b.abort(nil)
				return
			}
			continue
		}

		select {
		case job := <-b.queue:
			b.dequeued()
			if pending.canAdd(job, b.config) {
				pending.add(job)
				if pending.full(b.config) {
					stopTimer(timer)
					b.flush(pending)
					pending = nil
					timerC = nil
				}
			} else {
				stopTimer(timer)
				b.flush(pending)
				pending = newPendingBatch(job)
				if pending.full(b.config) {
					b.flush(pending)
					pending = nil
					timerC = nil
				} else {
					timer = time.NewTimer(b.config.MaxWait)
					timerC = timer.C
				}
			}
		case <-timerC:
			b.flush(pending)
			pending = nil
			timerC = nil
		case <-b.stop:
			stopTimer(timer)
			b.drain(pending)
			return
		case <-b.runCtx.Done():
			stopTimer(timer)
			b.abort(pending)
			return
		}
	}
}

func (b *Batcher) drain(pending *pendingBatch) {
	for {
		if b.runCtx.Err() != nil {
			b.abort(pending)
			return
		}
		select {
		case job := <-b.queue:
			b.dequeued()
			if pending == nil {
				pending = newPendingBatch(job)
			} else if pending.canAdd(job, b.config) {
				pending.add(job)
			} else {
				b.flush(pending)
				pending = newPendingBatch(job)
			}
			if pending.full(b.config) {
				b.flush(pending)
				pending = nil
			}
		default:
			if pending != nil {
				b.flush(pending)
			}
			return
		}
	}
}

func (b *Batcher) abort(pending *pendingBatch) {
	if pending != nil {
		b.resolve(pending.jobs, ErrBatcherStopped)
	}
	for {
		select {
		case job := <-b.queue:
			b.dequeued()
			job.result <- ErrBatcherStopped
		default:
			return
		}
	}
}

func newPendingBatch(job *batchJob) *pendingBatch {
	batch := &pendingBatch{increments: make(map[string]redisstore.Increment)}
	batch.add(job)
	return batch
}

func (b *pendingBatch) canAdd(job *batchJob, config BatcherConfig) bool {
	if b.events+job.request.Events > config.MaxEvents {
		return false
	}
	unique := len(b.increments)
	for _, increment := range job.request.Increments {
		if _, exists := b.increments[increment.Key]; !exists {
			unique++
		}
	}
	return unique <= config.MaxKeys
}

func (b *pendingBatch) add(job *batchJob) {
	b.jobs = append(b.jobs, job)
	b.events += job.request.Events
	for _, increment := range job.request.Increments {
		current, exists := b.increments[increment.Key]
		if !exists {
			b.increments[increment.Key] = increment
			continue
		}
		current.Delta += increment.Delta
		if increment.ExpireAt.After(current.ExpireAt) {
			current.ExpireAt = increment.ExpireAt
		}
		b.increments[increment.Key] = current
	}
}

func (b *pendingBatch) full(config BatcherConfig) bool {
	return b.events == config.MaxEvents || len(b.increments) == config.MaxKeys
}

func (b *Batcher) flush(batch *pendingBatch) {
	increments := make([]redisstore.Increment, 0, len(batch.increments))
	for _, increment := range batch.increments {
		increments = append(increments, increment)
	}
	sort.Slice(increments, func(i, j int) bool { return increments[i].Key < increments[j].Key })
	started := time.Now()
	ctx, cancel := context.WithTimeout(b.runCtx, b.config.OperationTimeout)
	err := b.store.Apply(ctx, increments)
	cancel()
	duration := time.Since(started)
	if b.logger != nil {
		b.logger.Debug("updater batch flushed", "operation", "batch_flush", "records", batch.events, "keys", len(increments), "duration", duration, "error", err)
	}
	if b.observer != nil {
		b.observer.ObserveBatch(batch.events, len(increments), duration, err)
	}
	b.resolve(batch.jobs, err)
}

func (b *Batcher) resolve(jobs []*batchJob, err error) {
	for _, job := range jobs {
		job.result <- err
	}
}

func (b *Batcher) dequeued() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	depth := int(b.depth.Add(-1))
	b.observeQueueDepth(depth)
}

func (b *Batcher) observeQueueDepth(depth int) {
	if b.observer != nil {
		b.observer.SetQueueDepth(depth)
	}
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
