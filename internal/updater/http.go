package updater

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/orihoch/cwm-minio-tierer/internal/contracts"
	"github.com/orihoch/cwm-minio-tierer/internal/redisstore"
)

type HealthChecker interface {
	Ping(context.Context) error
}

type HTTPHandler struct {
	config  Config
	batcher *Batcher
	health  HealthChecker
	metrics *Metrics
	clock   func() time.Time
	logger  *slog.Logger
	handler http.Handler
}

func NewHTTPHandler(config Config, batcher *Batcher, health HealthChecker, metrics *Metrics, clock func() time.Time, logger *slog.Logger) http.Handler {
	h := &HTTPHandler{config: config, batcher: batcher, health: health, metrics: metrics, clock: clock, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{$}", h.ingest)
	mux.HandleFunc("GET /livez", h.liveness)
	mux.HandleFunc("GET /readyz", h.readiness)
	mux.Handle("GET /metrics", metrics.Handler())
	h.handler = mux
	return h
}

func (h *HTTPHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	h.handler.ServeHTTP(response, request)
}

func (h *HTTPHandler) ingest(response http.ResponseWriter, request *http.Request) {
	if _, err := validateContentType(request.Header.Get("Content-Type")); err != nil {
		h.reject(response, err, 0)
		return
	}
	acceptedAt := h.clock().UTC()
	request.Body = http.MaxBytesReader(response, request.Body, h.config.MaxBodyBytes+1)
	records, err := DecodeRequest(request.Body, request.Header.Get("Content-Type"), DecodeLimits{
		MaxBodyBytes: h.config.MaxBodyBytes,
		MaxRecords:   h.config.MaxRecords,
	})
	if err != nil {
		h.reject(response, err, 0)
		return
	}

	increments := make(map[string]redisstore.Increment, len(records))
	expiry := contracts.CounterExpiry(acceptedAt, h.config.AccessRetention)
	for _, record := range records {
		key, err := contracts.AccessKey(h.config.InstanceID, record.Bucket, record.Object, acceptedAt)
		if err != nil {
			h.logger.Error("validated access identity could not be encoded", "error", err)
			h.failBatch(response, len(records), "identity_error", err)
			return
		}
		increment := increments[key]
		increment.Key = key
		increment.Delta++
		increment.ExpireAt = expiry
		increments[key] = increment
	}
	requestIncrements := make([]redisstore.Increment, 0, len(increments))
	for _, increment := range increments {
		requestIncrements = append(requestIncrements, increment)
	}
	err = h.batcher.Submit(request.Context(), BatchRequest{Events: len(records), Increments: requestIncrements})
	if err != nil {
		reason := "batch_error"
		switch {
		case errors.Is(err, ErrQueueFull):
			reason = "queue_full"
		case errors.Is(err, ErrNotRunning):
			reason = "not_ready"
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			reason = "request_context"
		}
		h.failBatch(response, len(records), reason, err)
		return
	}
	h.metrics.ObserveHTTPResult("success", len(records))
	response.WriteHeader(h.config.SuccessStatus)
}

func (h *HTTPHandler) reject(response http.ResponseWriter, err error, records int) {
	reason := RejectionReason(err)
	h.metrics.ObserveRejection(reason)
	h.metrics.ObserveHTTPResult("rejected", records)
	h.logger.Warn("updater request rejected", "reason", reason, "error", err)
	response.WriteHeader(h.config.FailureStatus)
}

func (h *HTTPHandler) failBatch(response http.ResponseWriter, records int, reason string, err error) {
	h.metrics.ObserveRejection(reason)
	h.metrics.ObserveHTTPResult("failed", records)
	h.logger.Error("updater request batch failed", "reason", reason, "records", records, "error", err)
	response.WriteHeader(h.config.FailureStatus)
}

func (h *HTTPHandler) liveness(response http.ResponseWriter, _ *http.Request) {
	response.WriteHeader(http.StatusOK)
}

func (h *HTTPHandler) readiness(response http.ResponseWriter, request *http.Request) {
	if !h.batcher.Ready() {
		response.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), h.config.RedisOperationTimeout)
	defer cancel()
	if err := h.health.Ping(ctx); err != nil {
		h.metrics.ObserveRedisFailure()
		response.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	response.WriteHeader(http.StatusOK)
}

func NewHTTPServer(config Config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              config.ListenAddress,
		Handler:           handler,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       config.IdleTimeout,
		MaxHeaderBytes:    config.MaxHeaderBytes,
	}
}
