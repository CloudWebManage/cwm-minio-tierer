package updater

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestHTTPHandlerValidatesWholeRequestBeforeEnqueue(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	batcher := testBatcher(store)
	defer func() { _ = batcher.Stop(context.Background()) }()
	handler := NewHTTPHandler(testConfig(), batcher, &healthStub{}, NewMetrics(prometheus.NewRegistry(), false, false), fixedClock(), discardLogger())
	body := "{\"bucket\":\"b\",\"object\":\"o\"}\n{\"bucket\":\"b\",\"object\":\"o\",\"bucket\":\"again\"}"
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-ndjson")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != 500 {
		t.Fatalf("status = %d, want configured failure 500", response.Code)
	}
	if batches := store.snapshot(); len(batches) != 0 {
		t.Fatalf("invalid request reached Redis store: %#v", batches)
	}
}

func TestHTTPHandlerUsesOneAcceptanceHourAndAcknowledgesRedis(t *testing.T) {
	t.Parallel()

	store := &recordingStore{}
	batcher := testBatcher(store)
	defer func() { _ = batcher.Stop(context.Background()) }()
	config := testConfig()
	config.SuccessStatus = http.StatusNoContent
	handler := NewHTTPHandler(config, batcher, &healthStub{}, NewMetrics(prometheus.NewRegistry(), false, false), fixedClock(), discardLogger())
	body := "{\"bucket\":\"b\",\"object\":\"a/b\"}\n{\"bucket\":\"b\",\"object\":\"a/b\"}"
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-ndjson")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	batches := store.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("Redis batches = %#v", batches)
	}
	increment := batches[0][0]
	const wantKey = "cwm-minio-tierer:v1:site-a:access:2026:08:25:14:Yg:YS9i"
	if increment.Key != wantKey || increment.Delta != 2 {
		t.Fatalf("increment = %#v, want key %q delta 2", increment, wantKey)
	}
	wantExpiry := time.Date(2026, 9, 1, 15, 0, 0, 0, time.UTC)
	if !increment.ExpireAt.Equal(wantExpiry) {
		t.Fatalf("expiry = %s, want %s", increment.ExpireAt, wantExpiry)
	}
}

func TestHTTPHandlerWaitsForContainingBatchResult(t *testing.T) {
	store := &blockingStore{started: make(chan struct{}), release: make(chan struct{})}
	batcher := testBatcher(store)
	defer func() { _ = batcher.Stop(context.Background()) }()
	handler := NewHTTPHandler(testConfig(), batcher, &healthStub{}, NewMetrics(prometheus.NewRegistry(), false, false), fixedClock(), discardLogger())
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"bucket":"b","object":"o"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()
	<-store.started
	select {
	case <-done:
		t.Fatal("handler returned before Redis batch completed")
	default:
	}
	close(store.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after Redis batch completed")
	}
	if response.Code != 200 {
		t.Fatalf("status = %d, want 200", response.Code)
	}
}

func TestHTTPHealthReadinessAndMetrics(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, true, true)
	store := &recordingStore{}
	batcher := testBatcherWithObserver(store, metrics)
	defer func() { _ = batcher.Stop(context.Background()) }()
	health := &healthStub{err: errors.New("Redis unavailable")}
	handler := NewHTTPHandler(testConfig(), batcher, health, metrics, fixedClock(), discardLogger())

	for path, want := range map[string]int{"/livez": 200, "/readyz": 503} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want {
			t.Errorf("GET %s status = %d, want %d", path, response.Code, want)
		}
	}
	health.err = nil
	readyResponse := httptest.NewRecorder()
	handler.ServeHTTP(readyResponse, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readyResponse.Code != 200 {
		t.Fatalf("ready status after Redis recovery = %d", readyResponse.Code)
	}

	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := metricsResponse.Body.String()
	for _, signal := range []string{
		`cwm_minio_tierer_updater_risk_override{risk="data_loss"} 1`,
		`cwm_minio_tierer_updater_risk_override{risk="duplicate"} 1`,
		"cwm_minio_tierer_updater_queue_depth",
		"cwm_minio_tierer_updater_batch_duration_seconds",
		"cwm_minio_tierer_updater_redis_failures_total 1",
	} {
		if !strings.Contains(body, signal) {
			t.Errorf("metrics output missing %q", signal)
		}
	}
}

func TestRiskMetricsRemainInactiveForSafeStatusesDespiteAcknowledgementFlags(t *testing.T) {
	t.Parallel()

	config, err := LoadConfig(mapLookup(map[string]string{
		"INSTANCE_ID":                   "site-a",
		"ACCESS_RETENTION":              "168h",
		"UPDATER_ACCEPT_DATA_LOSS":      "true",
		"UPDATER_ACCEPT_DUPLICATE_RISK": "true",
	}))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	metrics := NewMetrics(prometheus.NewRegistry(), config.DataLossRisk, config.DuplicateRisk)
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, signal := range []string{
		`cwm_minio_tierer_updater_risk_override{risk="data_loss"} 0`,
		`cwm_minio_tierer_updater_risk_override{risk="duplicate"} 0`,
	} {
		if !strings.Contains(response.Body.String(), signal) {
			t.Errorf("metrics output missing %q", signal)
		}
	}
}

func TestHTTPServerHasBoundedTimeoutsAndHeaders(t *testing.T) {
	t.Parallel()

	config := testConfig()
	server := NewHTTPServer(config, http.NotFoundHandler())
	if server.ReadHeaderTimeout != config.ReadHeaderTimeout || server.ReadTimeout != config.ReadTimeout || server.WriteTimeout != config.WriteTimeout || server.IdleTimeout != config.IdleTimeout {
		t.Fatalf("server timeouts = header:%s read:%s write:%s idle:%s", server.ReadHeaderTimeout, server.ReadTimeout, server.WriteTimeout, server.IdleTimeout)
	}
	if server.MaxHeaderBytes != config.MaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", server.MaxHeaderBytes, config.MaxHeaderBytes)
	}
}

func testBatcher(store CounterStore) *Batcher {
	return testBatcherWithObserver(store, nil)
}

func testBatcherWithObserver(store CounterStore, observer BatchObserver) *Batcher {
	batcher := NewBatcher(BatcherConfig{QueueSize: 4, MaxEvents: 10, MaxKeys: 10, MaxWait: time.Millisecond, OperationTimeout: time.Second}, store, observer)
	batcher.Start()
	return batcher
}

func testConfig() Config {
	return Config{
		InstanceID:            "site-a",
		AccessRetention:       168 * time.Hour,
		SuccessStatus:         200,
		FailureStatus:         500,
		MaxBodyBytes:          1024,
		MaxRecords:            10,
		RedisOperationTimeout: time.Second,
		ListenAddress:         ":8080",
		ReadHeaderTimeout:     time.Second,
		ReadTimeout:           2 * time.Second,
		WriteTimeout:          3 * time.Second,
		IdleTimeout:           4 * time.Second,
		MaxHeaderBytes:        4096,
	}
}

func fixedClock() func() time.Time {
	return func() time.Time {
		return time.Date(2026, 8, 25, 17, 42, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

type healthStub struct {
	err error
}

func (h *healthStub) Ping(context.Context) error {
	return h.err
}
