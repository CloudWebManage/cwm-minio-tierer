package updater

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/orihoch/cwm-minio-tierer/internal/redisstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type serverLifecycle interface {
	ListenAndServe(context.Context) error
	Shutdown(context.Context) error
	Close() error
}

type httpService struct {
	*http.Server
}

func (s *httpService) ListenAndServe(ctx context.Context) error {
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", s.Addr)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return http.ErrServerClosed
		}
		return err
	}
	defer listener.Close()
	// Resolution/binding can finish concurrently with shutdown, before Serve
	// has registered the listener. Retain ownership until it is handed over.
	if ctx.Err() != nil {
		return http.ErrServerClosed
	}
	return s.Server.Serve(listener)
}

func RedisOptions(config Config) (*redis.Options, error) {
	options := &redis.Options{
		Addr:         config.RedisAddress,
		Username:     config.RedisUsername,
		Password:     config.RedisPassword,
		DB:           config.RedisDB,
		DialTimeout:  config.RedisOperationTimeout,
		ReadTimeout:  config.RedisOperationTimeout,
		WriteTimeout: config.RedisOperationTimeout,
		// Counter updates are non-idempotent; surface ambiguous replies to Vector.
		MaxRetries: -1,
	}
	if config.RedisTLS {
		host, _, err := net.SplitHostPort(config.RedisAddress)
		if err != nil {
			return nil, fmt.Errorf("REDIS_ADDR for TLS must include host and port: %w", err)
		}
		options.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: host,
		}
	}
	return options, nil
}

func Run(ctx context.Context, config Config, logger *slog.Logger) error {
	options, err := RedisOptions(config)
	if err != nil {
		return err
	}
	redisClient := redis.NewClient(options)
	client := redisstore.NewClient(redisClient)
	startupCtx, cancel := context.WithTimeout(ctx, config.RedisOperationTimeout)
	defer cancel()
	if err := client.Ping(startupCtx); err != nil {
		_ = client.Close()
		return fmt.Errorf("initial Redis ping: %w", err)
	}
	store := redisstore.NewStore(client)
	if err := store.Load(startupCtx); err != nil {
		_ = client.Close()
		return err
	}

	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, config.DataLossRisk, config.DuplicateRisk)
	batcher := NewBatcher(BatcherConfig{
		QueueSize:        config.QueueSize,
		MaxEvents:        config.BatchMaxEvents,
		MaxKeys:          config.BatchMaxKeys,
		MaxWait:          config.BatchMaxWait,
		OperationTimeout: config.RedisOperationTimeout,
		Logger:           logger,
	}, store, metrics)
	handler := NewHTTPHandler(config, batcher, client, metrics, time.Now, logger)
	server := &httpService{Server: NewHTTPServer(config, handler)}
	var additionalServers []serverLifecycle
	if config.MetricsListenAddress != "" {
		metricsServer := NewHTTPServer(config, NewMetricsHTTPHandler(config, batcher, client, metrics))
		metricsServer.Addr = config.MetricsListenAddress
		additionalServers = append(additionalServers, &httpService{Server: metricsServer})
	}

	logRiskWarnings(config, logger)
	logger.Info("starting Redis updater", "listen_address", config.ListenAddress, "metrics_listen_address", config.MetricsListenAddress, "redis_address", config.RedisAddress, "instance_id", config.InstanceID)
	err = runService(ctx, server, batcher, config.ShutdownTimeout, client.Close, additionalServers...)
	if err != nil {
		return err
	}
	logger.Info("Redis updater stopped")
	return nil
}

func logRiskWarnings(config Config, logger *slog.Logger) {
	if config.DataLossRisk {
		logger.Warn("non-5xx updater failure status accepts data-loss risk", "failure_status", config.FailureStatus, "risk_override", "data_loss")
	}
	if config.DuplicateRisk {
		logger.Warn("non-2xx updater success status accepts duplicate-counting risk", "success_status", config.SuccessStatus, "risk_override", "duplicate")
	}
}

func runService(ctx context.Context, server serverLifecycle, batcher *Batcher, shutdownTimeout time.Duration, closeRedis func() error, additionalServers ...serverLifecycle) error {
	servers := append([]serverLifecycle{server}, additionalServers...)
	listenCtx, cancelListen := context.WithCancel(ctx)
	defer cancelListen()
	batcher.Start()
	serveErrors := make(chan error, len(servers))
	for _, server := range servers {
		go func() {
			err := server.ListenAndServe(listenCtx)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			serveErrors <- err
		}()
	}

	var serveErr error
	received := 0
	select {
	case <-ctx.Done():
	case serveErr = <-serveErrors:
		received++
	}
	// Cancel pending hostname resolution on any exit, including a peer's bind
	// failure. HTTP Shutdown cannot see listeners that are still being acquired.
	cancelListen()

	// Stop accepting on every listener concurrently, under one HTTP shutdown
	// window. Force-close active connections if graceful shutdown times out.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	shutdownErrors := make(chan error, len(servers))
	for _, server := range servers {
		go func() {
			err := server.Shutdown(shutdownCtx)
			if err != nil {
				err = errors.Join(err, server.Close())
			}
			shutdownErrors <- err
		}()
	}
	var shutdownErr error
	for range servers {
		shutdownErr = errors.Join(shutdownErr, <-shutdownErrors)
	}
	cancelShutdown()
	// Accepted work retains its own drain window, even if HTTP timed out.
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), shutdownTimeout)
	batchErr := batcher.Stop(drainCtx)
	cancelDrain()
	closeErr := closeRedis()
	for ; received < len(servers); received++ {
		serveErr = errors.Join(serveErr, <-serveErrors)
	}
	return errors.Join(serveErr, shutdownErr, batchErr, closeErr)
}
