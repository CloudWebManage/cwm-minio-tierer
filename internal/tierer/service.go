package tierer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type readiness interface{ Ready(context.Context) error }

func NewHTTPHandler(check readiness, metrics *Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := check.Ready(r.Context()); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /metrics", metrics.Handler())
	return mux
}

type dependencyReadiness struct {
	redis interface{ Ping(context.Context) error }
	minio interface {
		Buckets(context.Context) ([]string, error)
	}
	timeout time.Duration
	metrics *Metrics
	logger  *slog.Logger
}

func (r dependencyReadiness) Ready(ctx context.Context) error {
	checkCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	if err := r.redis.Ping(checkCtx); err != nil {
		r.metrics.DependencyFailure("redis")
		r.logFailure("redis", err)
		return fmt.Errorf("Redis readiness: %w", err)
	}
	if _, err := r.minio.Buckets(checkCtx); err != nil {
		r.metrics.DependencyFailure("minio")
		r.logFailure("minio", err)
		return fmt.Errorf("MinIO readiness: %w", err)
	}
	return nil
}

func (r dependencyReadiness) logFailure(dependency string, err error) {
	if r.logger != nil {
		r.logger.Error("tierer readiness dependency failed", "dependency", dependency, "operation", "readiness", "error", err)
	}
}

func RedisOptions(config Config) (*redis.Options, error) {
	// Budget reservations are non-idempotent; surface ambiguous replies to the scanner.
	options := &redis.Options{Addr: config.RedisAddress, Username: config.RedisUsername, Password: config.RedisPassword, DB: config.RedisDB, DialTimeout: config.RedisOperationTimeout, ReadTimeout: config.RedisOperationTimeout, WriteTimeout: config.RedisOperationTimeout, PoolTimeout: config.RedisOperationTimeout, MaxRetries: -1}
	if config.RedisTLS {
		host, _, err := net.SplitHostPort(config.RedisAddress)
		if err != nil {
			return nil, err
		}
		options.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	}
	return options, nil
}

func MinIOTransport(config Config) (*http.Transport, error) {
	transport, err := minio.DefaultTransport(config.MinIOSecure)
	if err != nil {
		return nil, err
	}
	timeout := config.MinIOOperationTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialTimeout := min(timeout, 10*time.Second)
	transport.DialContext = (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = timeout
	transport.TLSHandshakeTimeout = min(timeout, 10*time.Second)
	transport.ExpectContinueTimeout = min(timeout, time.Second)
	transport.IdleConnTimeout = 60 * time.Second
	return transport, nil
}

func Run(ctx context.Context, config Config, logger *slog.Logger) error {
	redisOptions, err := RedisOptions(config)
	if err != nil {
		return err
	}
	rawRedis := redis.NewClient(redisOptions)
	defer rawRedis.Close()
	redisClient := NewRedisClient(rawRedis)
	minioTransport, err := MinIOTransport(config)
	if err != nil {
		return fmt.Errorf("create MinIO transport: %w", err)
	}
	defer minioTransport.CloseIdleConnections()
	minioClient, err := minio.New(config.MinIOEndpoint, &minio.Options{Creds: credentials.NewStaticV4(config.MinIOAccessKey, config.MinIOSecretKey, ""), Secure: config.MinIOSecure, Region: config.MinIORegion, Transport: minioTransport, MaxRetries: 1})
	if err != nil {
		return fmt.Errorf("create MinIO client: %w", err)
	}
	minioAdapter := NewMinIOAdapter(minioClient)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	ready := dependencyReadiness{redis: redisClient, minio: minioAdapter, timeout: config.RedisOperationTimeout, metrics: metrics, logger: logger}
	if err := ready.Ready(ctx); err != nil {
		return err
	}
	store := NewRedisStore(redisClient, config.InstanceID, config.CoverageTemplate, config.CoverageValue)
	scope := ConfigScopeHash(config)
	scanner := NewScanner(ScannerConfig{Apply: config.Apply, Policy: config.Policy, RestoreDays: config.RestoreDays, MarkerKey: config.MarkerKey, MarkerValue: config.MarkerValue, ChunkSize: config.ChunkSize, CompletionDelay: config.CompletionDelay, RetryDelay: config.RetryDelay, ExcludedBuckets: config.ExcludedBuckets, ExcludedPrefixes: config.ExcludedPrefixes, Scope: scope, TransitionBudget: config.TransitionBudget, RestoreBudget: config.RestoreBudget, OperationTimeout: config.MinIOOperationTimeout, Logger: logger}, minioAdapter, store, store, metrics, time.Now)
	httpServer := &http.Server{Addr: config.ListenAddress, Handler: NewHTTPHandler(ready, metrics), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}

	mode := "audit"
	if config.Apply {
		mode = "apply"
	}
	logger.Info("starting MinIO tierer", "mode", mode, "listen_address", config.ListenAddress, "minio_endpoint", config.MinIOEndpoint, "redis_address", config.RedisAddress, "instance_id", config.InstanceID, "scope_hash", scope)
	err = runServiceLoops(ctx, httpServer, scanner.Run, config.ShutdownTimeout)
	if ctx.Err() != nil && err == nil {
		logger.Info("MinIO tierer stopped")
		return nil
	}
	return err
}

type serviceLifecycle interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

func runServiceLoops(ctx context.Context, server serviceLifecycle, worker func(context.Context) error, shutdownTimeout time.Duration) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	serverDone := make(chan error, 1)
	workerDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe() }()
	go func() { workerDone <- worker(runCtx) }()
	var result error
	serverFinished := false
	workerFinished := false
	select {
	case <-ctx.Done():
	case err := <-serverDone:
		serverFinished = true
		if !errors.Is(err, http.ErrServerClosed) {
			result = errors.Join(result, fmt.Errorf("http service: %w", err))
		}
	case err := <-workerDone:
		workerFinished = true
		if err != nil && !errors.Is(err, context.Canceled) {
			result = errors.Join(result, fmt.Errorf("scanner service: %w", err))
		}
	}
	cancel()
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer deadlineCancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(deadlineCtx) }()
	shutdownFinished := false
	for !serverFinished || !workerFinished || !shutdownFinished {
		select {
		case err := <-serverDone:
			serverFinished = true
			if !errors.Is(err, http.ErrServerClosed) {
				result = errors.Join(result, err)
			}
		case err := <-workerDone:
			workerFinished = true
			if err != nil && !errors.Is(err, context.Canceled) {
				result = errors.Join(result, err)
			}
		case err := <-shutdownDone:
			shutdownFinished = true
			if err != nil {
				result = errors.Join(result, err)
			}
		case <-deadlineCtx.Done():
			return errors.Join(result, fmt.Errorf("service shutdown deadline: %w", deadlineCtx.Err()))
		}
	}
	return result
}
