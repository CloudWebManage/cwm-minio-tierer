package updater

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orihoch/cwm-minio-tierer/internal/redisstore"
	"github.com/redis/go-redis/v9"
)

func TestLogRiskWarningsOnlyForActiveUnsafeStatuses(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	config := testConfig()
	logRiskWarnings(config, logger)
	if output.Len() != 0 {
		t.Fatalf("safe status warning output = %q, want empty", output.String())
	}

	config.FailureStatus = http.StatusOK
	config.DataLossRisk = true
	logRiskWarnings(config, logger)
	if !strings.Contains(output.String(), `"risk_override":"data_loss"`) {
		t.Fatalf("active unsafe status warning output = %q", output.String())
	}
}

func TestRedisOptionsConfigureOnlyStandaloneClientAndSafeTLS(t *testing.T) {
	t.Parallel()

	config := testConfig()
	config.RedisAddress = "redis.internal:6380"
	config.RedisUsername = "updater"
	config.RedisPassword = "secret"
	config.RedisDB = 3
	config.RedisTLS = true
	options, err := RedisOptions(config)
	if err != nil {
		t.Fatalf("RedisOptions() error = %v", err)
	}
	if options.Addr != config.RedisAddress || options.Username != "updater" || options.Password != "secret" || options.DB != 3 {
		t.Fatalf("Redis options = %#v", options)
	}
	if options.TLSConfig == nil || options.TLSConfig.MinVersion != tls.VersionTLS12 || options.TLSConfig.ServerName != "redis.internal" {
		t.Fatalf("TLS config = %#v", options.TLSConfig)
	}
	if options.DialTimeout != config.RedisOperationTimeout || options.ReadTimeout != config.RedisOperationTimeout || options.WriteTimeout != config.RedisOperationTimeout {
		t.Fatalf("Redis timeouts = dial:%s read:%s write:%s", options.DialTimeout, options.ReadTimeout, options.WriteTimeout)
	}
}

func TestRedisOptionsDisableTransparentCommandRetries(t *testing.T) {
	t.Parallel()

	options, err := RedisOptions(testConfig())
	if err != nil {
		t.Fatalf("RedisOptions() error = %v", err)
	}
	if options.MaxRetries != -1 {
		t.Errorf("RedisOptions() MaxRetries = %d, want -1 disable sentinel", options.MaxRetries)
	}

	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if client.Options().MaxRetries != 0 {
		t.Errorf("effective client MaxRetries = %d, want 0 retries", client.Options().MaxRetries)
	}
}

func TestRunServiceStopsHTTPThenDrainsBatcherAndClosesRedis(t *testing.T) {
	store := &blockingStore{started: make(chan struct{}), release: make(chan struct{})}
	batcher := NewBatcher(BatcherConfig{QueueSize: 2, MaxEvents: 1, MaxKeys: 1, MaxWait: time.Hour, OperationTimeout: time.Second}, store, nil)
	server := newLifecycleServer()
	closed := false
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- runService(ctx, server, batcher, time.Second, func() error {
			closed = true
			return nil
		})
	}()
	<-server.started

	requestDone := make(chan error, 1)
	go func() {
		requestDone <- batcher.Submit(context.Background(), BatchRequest{Events: 1, Increments: testIncrement("a")})
	}()
	<-store.started
	cancel()
	close(store.release)

	if err := <-requestDone; err != nil {
		t.Fatalf("accepted request error = %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("runService() error = %v", err)
	}
	if batcher.Ready() {
		t.Fatal("batcher remained ready after shutdown")
	}
	if !closed {
		t.Fatal("Redis client was not closed")
	}
	if !server.wasShutdown() {
		t.Fatal("HTTP server was not shut down")
	}
}

func TestRunServiceGivesBatchDrainItsOwnShutdownWindow(t *testing.T) {
	store := &completionStore{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		completed: make(chan struct{}),
	}
	batcher := NewBatcher(BatcherConfig{QueueSize: 1, MaxEvents: 1, MaxKeys: 1, MaxWait: time.Hour, OperationTimeout: time.Second}, store, nil)
	server := newSlowShutdownServer()
	ctx, cancel := context.WithCancel(context.Background())
	closedBeforeDrain := false
	runDone := make(chan error, 1)
	go func() {
		runDone <- runService(ctx, server, batcher, 20*time.Millisecond, func() error {
			select {
			case <-store.completed:
			default:
				closedBeforeDrain = true
			}
			return nil
		})
	}()
	<-server.started
	requestDone := make(chan error, 1)
	go func() {
		requestDone <- batcher.Submit(context.Background(), BatchRequest{Events: 1, Increments: testIncrement("a")})
	}()
	<-store.started
	cancel()
	time.AfterFunc(30*time.Millisecond, func() { close(store.release) })

	if err := <-runDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runService() error = %v, want HTTP shutdown deadline error", err)
	}
	if err := <-requestDone; err != nil {
		t.Fatalf("accepted request error = %v", err)
	}
	if closedBeforeDrain {
		t.Fatal("Redis was closed before the accepted batch drained")
	}
}

func TestRunServiceWaitsForCanceledBatcherGoroutineBeforeClosingRedis(t *testing.T) {
	store := &cancellationStore{
		started:        make(chan struct{}),
		canceled:       make(chan struct{}),
		releaseCleanup: make(chan struct{}),
	}
	batcher := NewBatcher(BatcherConfig{QueueSize: 1, MaxEvents: 1, MaxKeys: 1, MaxWait: time.Hour, OperationTimeout: 30 * time.Millisecond}, store, nil)
	server := newLifecycleServer()
	ctx, cancel := context.WithCancel(context.Background())
	closed := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- runService(ctx, server, batcher, 5*time.Millisecond, func() error {
			close(closed)
			return nil
		})
	}()
	<-server.started
	requestDone := make(chan error, 1)
	go func() {
		requestDone <- batcher.Submit(context.Background(), BatchRequest{Events: 1, Increments: testIncrement("a")})
	}()
	<-store.started
	cancel()
	<-store.canceled
	select {
	case <-closed:
		t.Fatal("Redis closed before batcher cancellation cleanup completed")
	default:
	}
	select {
	case <-runDone:
		t.Fatal("runService returned before batcher goroutine joined")
	default:
	}
	close(store.releaseCleanup)
	if err := <-runDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runService() error = %v, want drain deadline error", err)
	}
	if err := <-requestDone; err == nil {
		t.Fatal("accepted request error = nil after shutdown cancellation")
	}
	select {
	case <-closed:
	default:
		t.Fatal("Redis was not closed after batcher goroutine joined")
	}
}

func testIncrement(key string) []redisstore.Increment {
	return []redisstore.Increment{{Key: key, Delta: 1, ExpireAt: time.Now().Add(time.Hour)}}
}

type lifecycleServer struct {
	started  chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
	mu       sync.Mutex
	shutdown bool
}

type slowShutdownServer struct {
	*lifecycleServer
}

func newSlowShutdownServer() *slowShutdownServer {
	return &slowShutdownServer{lifecycleServer: newLifecycleServer()}
}

func (s *slowShutdownServer) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopped) })
	<-ctx.Done()
	return ctx.Err()
}

type completionStore struct {
	started   chan struct{}
	release   chan struct{}
	completed chan struct{}
	once      sync.Once
}

func (s *completionStore) Apply(ctx context.Context, _ []redisstore.Increment) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		close(s.completed)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newLifecycleServer() *lifecycleServer {
	return &lifecycleServer{started: make(chan struct{}), stopped: make(chan struct{})}
}

func (s *lifecycleServer) ListenAndServe() error {
	close(s.started)
	<-s.stopped
	return http.ErrServerClosed
}

func (s *lifecycleServer) Shutdown(context.Context) error {
	s.mu.Lock()
	s.shutdown = true
	s.mu.Unlock()
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

func (s *lifecycleServer) wasShutdown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdown
}
