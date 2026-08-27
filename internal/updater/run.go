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
	ListenAndServe() error
	Shutdown(context.Context) error
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
	server := NewHTTPServer(config, handler)

	logRiskWarnings(config, logger)
	logger.Info("starting Redis updater", "listen_address", config.ListenAddress, "redis_address", config.RedisAddress, "instance_id", config.InstanceID)
	err = runService(ctx, server, batcher, config.ShutdownTimeout, client.Close)
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

func runService(ctx context.Context, server serverLifecycle, batcher *Batcher, shutdownTimeout time.Duration, closeRedis func() error) error {
	batcher.Start()
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serveErrors:
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	shutdownErr := server.Shutdown(shutdownCtx)
	cancelShutdown()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), shutdownTimeout)
	batchErr := batcher.Stop(drainCtx)
	cancelDrain()
	closeErr := closeRedis()
	if serveErr == nil {
		serveErr = <-serveErrors
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr, batchErr, closeErr)
}
