package updater

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orihoch/cwm-minio-tierer/internal/redisstore"
)

func TestBatcherCombinesRequestsAndAggregatesDuplicateKeys(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	batcher := NewBatcher(BatcherConfig{QueueSize: 4, MaxEvents: 10, MaxKeys: 10, MaxWait: 20 * time.Millisecond, OperationTimeout: time.Second}, store, nil)
	batcher.Start()
	t.Cleanup(func() { _ = batcher.Stop(context.Background()) })
	expiry := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)

	errorsByRequest := submitConcurrently(t, batcher,
		BatchRequest{Events: 2, Increments: []redisstore.Increment{{Key: "b", Delta: 1, ExpireAt: expiry}, {Key: "a", Delta: 1, ExpireAt: expiry}}},
		BatchRequest{Events: 2, Increments: []redisstore.Increment{{Key: "a", Delta: 2, ExpireAt: expiry}}},
	)
	for i, err := range errorsByRequest {
		if err != nil {
			t.Fatalf("Submit(request %d) error = %v", i, err)
		}
	}
	batches := store.snapshot()
	if len(batches) != 1 {
		t.Fatalf("store batches = %d, want 1", len(batches))
	}
	if len(batches[0]) != 2 || batches[0][0].Key != "a" || batches[0][0].Delta != 3 || batches[0][1].Key != "b" {
		t.Fatalf("aggregated batch = %#v", batches[0])
	}
}

func TestBatcherDebugLogsBatchFlush(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	batcher := NewBatcher(BatcherConfig{QueueSize: 1, MaxEvents: 1, MaxKeys: 1, MaxWait: time.Hour, OperationTimeout: time.Second, Logger: logger}, &recordingStore{}, nil)
	batcher.Start()
	t.Cleanup(func() { _ = batcher.Stop(context.Background()) })

	if err := batcher.Submit(context.Background(), BatchRequest{Events: 1, Increments: testIncrement("a")}); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	text := output.String()
	if !strings.Contains(text, `"operation":"batch_flush"`) || !strings.Contains(text, `"records":1`) || !strings.Contains(text, `"keys":1`) {
		t.Fatalf("debug batch log = %s", text)
	}
}

func TestBatcherNeverSplitsARequestAcrossBatches(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	batcher := NewBatcher(BatcherConfig{QueueSize: 4, MaxEvents: 3, MaxKeys: 3, MaxWait: 50 * time.Millisecond, OperationTimeout: time.Second}, store, nil)
	batcher.Start()
	t.Cleanup(func() { _ = batcher.Stop(context.Background()) })
	expiry := time.Now().Add(time.Hour)

	errorsByRequest := submitConcurrently(t, batcher,
		BatchRequest{Events: 2, Increments: []redisstore.Increment{{Key: "a", Delta: 2, ExpireAt: expiry}}},
		BatchRequest{Events: 2, Increments: []redisstore.Increment{{Key: "b", Delta: 2, ExpireAt: expiry}}},
	)
	for _, err := range errorsByRequest {
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}
	if batches := store.snapshot(); len(batches) != 2 || len(batches[0]) != 1 || len(batches[1]) != 1 {
		t.Fatalf("store batches = %#v, want two whole requests", batches)
	}
}

func TestBatcherRejectsOversizedRequestBeforeEnqueue(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	batcher := NewBatcher(BatcherConfig{QueueSize: 1, MaxEvents: 1, MaxKeys: 1, MaxWait: time.Millisecond, OperationTimeout: time.Second}, store, nil)
	batcher.Start()
	t.Cleanup(func() { _ = batcher.Stop(context.Background()) })

	err := batcher.Submit(context.Background(), BatchRequest{Events: 2, Increments: []redisstore.Increment{{Key: "a", Delta: 2, ExpireAt: time.Now().Add(time.Hour)}}})
	if !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("Submit() error = %v, want ErrRequestTooLarge", err)
	}
	if batches := store.snapshot(); len(batches) != 0 {
		t.Fatalf("oversized request reached store: %#v", batches)
	}
}

func TestBatcherPropagatesStoreErrorToEveryWaiter(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("Redis unavailable")
	store := &recordingStore{err: wantErr}
	batcher := NewBatcher(BatcherConfig{QueueSize: 2, MaxEvents: 10, MaxKeys: 10, MaxWait: 10 * time.Millisecond, OperationTimeout: time.Second}, store, nil)
	batcher.Start()
	t.Cleanup(func() { _ = batcher.Stop(context.Background()) })

	got := submitConcurrently(t, batcher,
		BatchRequest{Events: 1, Increments: []redisstore.Increment{{Key: "a", Delta: 1, ExpireAt: time.Now().Add(time.Hour)}}},
		BatchRequest{Events: 1, Increments: []redisstore.Increment{{Key: "b", Delta: 1, ExpireAt: time.Now().Add(time.Hour)}}},
	)
	for i, err := range got {
		if !errors.Is(err, wantErr) {
			t.Fatalf("Submit(request %d) error = %v, want %v", i, err, wantErr)
		}
	}
}

func TestBatcherBoundsQueueAndDrainsAcceptedWorkOnStop(t *testing.T) {
	t.Parallel()

	store := &blockingStore{started: make(chan struct{}), release: make(chan struct{})}
	batcher := NewBatcher(BatcherConfig{QueueSize: 1, MaxEvents: 1, MaxKeys: 1, MaxWait: time.Hour, OperationTimeout: time.Second}, store, nil)
	batcher.Start()
	request := BatchRequest{Events: 1, Increments: []redisstore.Increment{{Key: "a", Delta: 1, ExpireAt: time.Now().Add(time.Hour)}}}
	firstDone := make(chan error, 1)
	go func() { firstDone <- batcher.Submit(context.Background(), request) }()
	<-store.started

	secondDone := make(chan error, 1)
	go func() { secondDone <- batcher.Submit(context.Background(), request) }()
	deadline := time.Now().Add(time.Second)
	for batcher.QueueDepth() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := batcher.Submit(context.Background(), request); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third Submit() error = %v, want ErrQueueFull", err)
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- batcher.Stop(context.Background()) }()
	close(store.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Submit() error = %v", err)
	}
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if batcher.Ready() {
		t.Fatal("Ready() = true after Stop")
	}
}

func TestBatcherQueueDepthNeverBecomesNegativeUnderConcurrentHandoff(t *testing.T) {
	observer := &depthObserver{}
	batcher := NewBatcher(BatcherConfig{QueueSize: 1, MaxEvents: 1, MaxKeys: 1, MaxWait: time.Millisecond, OperationTimeout: time.Second}, &recordingStore{}, observer)
	batcher.Start()
	request := BatchRequest{Events: 1, Increments: testIncrement("a")}

	const workers = 32
	const requestsPerWorker = 500
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for range requestsPerWorker {
				for {
					err := batcher.Submit(context.Background(), request)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrQueueFull) {
						t.Errorf("Submit() error = %v", err)
						return
					}
					runtime.Gosched()
				}
			}
		}()
	}
	wait.Wait()
	if err := batcher.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if minimum := observer.minimum.Load(); minimum < 0 {
		t.Fatalf("minimum observed queue depth = %d, want >= 0", minimum)
	}
	if depth := batcher.QueueDepth(); depth != 0 {
		t.Fatalf("final queue depth = %d, want 0", depth)
	}
}

func TestBatcherStopDeadlineCancelsResolvesAndJoins(t *testing.T) {
	store := &cancellationStore{
		started:        make(chan struct{}),
		canceled:       make(chan struct{}),
		releaseCleanup: make(chan struct{}),
	}
	batcher := NewBatcher(BatcherConfig{QueueSize: 1, MaxEvents: 1, MaxKeys: 1, MaxWait: time.Hour, OperationTimeout: 30 * time.Millisecond}, store, nil)
	batcher.Start()
	request := BatchRequest{Events: 1, Increments: testIncrement("a")}
	firstDone := make(chan error, 1)
	go func() { firstDone <- batcher.Submit(context.Background(), request) }()
	<-store.started
	secondDone := make(chan error, 1)
	go func() { secondDone <- batcher.Submit(context.Background(), request) }()
	waitForQueueDepth(t, batcher, 1)

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- batcher.Stop(stopCtx) }()
	select {
	case <-store.canceled:
	case <-time.After(time.Second):
		t.Fatal("store operation was not canceled")
	}
	returnedBeforeCleanup := false
	var stopErr error
	select {
	case stopErr = <-stopDone:
		returnedBeforeCleanup = true
	default:
	}
	close(store.releaseCleanup)
	if !returnedBeforeCleanup {
		stopErr = <-stopDone
	}
	firstErr := <-firstDone
	secondErr := <-secondDone

	if returnedBeforeCleanup {
		t.Fatal("Stop() returned before its goroutine completed cancellation cleanup")
	}
	if !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want deadline exceeded", stopErr)
	}
	if firstErr == nil || secondErr == nil {
		t.Fatalf("resolved waiter errors = first:%v second:%v, want shutdown failures", firstErr, secondErr)
	}
	select {
	case <-batcher.done:
	default:
		t.Fatal("batcher goroutine still running after Stop() returned")
	}
	if depth := batcher.QueueDepth(); depth != 0 {
		t.Fatalf("queue depth after canceled drain = %d, want 0", depth)
	}
}

func submitConcurrently(t *testing.T, batcher *Batcher, requests ...BatchRequest) []error {
	t.Helper()
	start := make(chan struct{})
	results := make([]error, len(requests))
	var wait sync.WaitGroup
	wait.Add(len(requests))
	for i := range requests {
		go func(i int) {
			defer wait.Done()
			<-start
			results[i] = batcher.Submit(context.Background(), requests[i])
		}(i)
	}
	close(start)
	wait.Wait()
	return results
}

type recordingStore struct {
	mu      sync.Mutex
	batches [][]redisstore.Increment
	err     error
}

func (s *recordingStore) Apply(_ context.Context, increments []redisstore.Increment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, append([]redisstore.Increment(nil), increments...))
	return s.err
}

func (s *recordingStore) snapshot() [][]redisstore.Increment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]redisstore.Increment(nil), s.batches...)
}

type blockingStore struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type depthObserver struct {
	minimum atomic.Int64
}

func (o *depthObserver) SetQueueDepth(depth int) {
	for {
		minimum := o.minimum.Load()
		if int64(depth) >= minimum || o.minimum.CompareAndSwap(minimum, int64(depth)) {
			return
		}
	}
}

func (*depthObserver) ObserveBatch(int, int, time.Duration, error) {}

type cancellationStore struct {
	started        chan struct{}
	canceled       chan struct{}
	releaseCleanup chan struct{}
	startOnce      sync.Once
	cancelOnce     sync.Once
}

func (s *cancellationStore) Apply(ctx context.Context, _ []redisstore.Increment) error {
	s.startOnce.Do(func() { close(s.started) })
	<-ctx.Done()
	s.cancelOnce.Do(func() { close(s.canceled) })
	<-s.releaseCleanup
	return ctx.Err()
}

func waitForQueueDepth(t *testing.T, batcher *Batcher, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for batcher.QueueDepth() != want && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := batcher.QueueDepth(); got != want {
		t.Fatalf("QueueDepth() = %d, want %d", got, want)
	}
}

func (s *blockingStore) Apply(ctx context.Context, _ []redisstore.Increment) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
