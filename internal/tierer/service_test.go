package tierer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func TestHealthHandlerKeepsLivenessLocalAndReadinessChecksDependencies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	checker := &fakeReadiness{err: errors.New("redis unavailable")}
	handler := NewHTTPHandler(checker, metrics)

	live := httptest.NewRecorder()
	handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if live.Code != http.StatusOK || checker.calls != 0 {
		t.Fatalf("/livez status=%d readiness calls=%d", live.Code, checker.calls)
	}
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable || checker.calls != 1 {
		t.Fatalf("/readyz status=%d readiness calls=%d", ready.Code, checker.calls)
	}
	checker.err = nil
	ready = httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("healthy /readyz status=%d", ready.Code)
	}
}

func TestMinIOTransportBoundsResponseHeaderWait(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(func() { close(release); server.Close() })
	transport, err := MinIOTransport(Config{MinIOSecure: false, MinIOOperationTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("MinIOTransport() error = %v", err)
	}
	client := &http.Client{Transport: transport}
	started := time.Now()
	_, err = client.Get(server.URL)
	if err == nil || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("client.Get() error=%v duration=%s, want bounded header timeout", err, time.Since(started))
	}
}

func TestRedisOptionsDisableTransparentCommandRetries(t *testing.T) {
	t.Parallel()

	options, err := RedisOptions(Config{RedisAddress: "redis:6379", RedisOperationTimeout: time.Second})
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

func TestRunServiceLoopsBoundsEndToEndShutdownWhenComponentsIgnoreCancellation(t *testing.T) {
	t.Parallel()
	server := &blockingService{serveRelease: make(chan struct{}), shutdownRelease: make(chan struct{})}
	workerRelease := make(chan struct{})
	t.Cleanup(func() { close(server.serveRelease); close(server.shutdownRelease); close(workerRelease) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := runServiceLoops(ctx, server, func(context.Context) error { <-workerRelease; return nil }, 20*time.Millisecond)
	if err == nil || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("runServiceLoops() error=%v duration=%s, want bounded shutdown error", err, time.Since(started))
	}
}

func TestMetricsExposeRequiredBoundedSignalsWithoutObjectLabels(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.ScanStarted()
	metrics.ObjectHandled("marked")
	metrics.CoverageSkipped()
	metrics.CursorSaved(time.Unix(100, 0))
	metrics.BudgetObserved(BudgetRestore, BudgetReservation{Allowed: true, UsedAttempts: 2, UsedBytes: 3})
	metrics.BudgetExhausted(BudgetTransition)
	metrics.MarkerObserved(MarkerAdded, nil)
	beforeApply := metricValue(t, registry, "cwm_minio_tierer_last_successful_mutation_timestamp_seconds")
	if beforeApply != 0 {
		t.Fatalf("marker planning advanced mutation timestamp to %f", beforeApply)
	}
	metrics.MarkerApplied(MarkerAdded)
	metrics.TransitionState(true)
	metrics.RestoreObserved(ActionRenew, false, nil)
	metrics.RestoreApplied(ActionRenew)
	metrics.ScanFinished(time.Second, nil)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, request)
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	text := string(body)
	for _, name := range []string{
		"cwm_minio_tierer_scans_total", "cwm_minio_tierer_objects_total", "cwm_minio_tierer_coverage_skips_total",
		"cwm_minio_tierer_cursor_age_seconds", "cwm_minio_tierer_budget_used", "cwm_minio_tierer_budget_exhaustions_total",
		"cwm_minio_tierer_marker_outcomes_total", "cwm_minio_tierer_transitioned_state_total", "cwm_minio_tierer_restore_outcomes_total",
		"cwm_minio_tierer_last_successful_scan_timestamp_seconds",
		"cwm_minio_tierer_redis_failures_total",
	} {
		if !strings.Contains(text, name) {
			t.Errorf("metrics missing %s", name)
		}
	}
	if strings.Contains(text, "bucket=") || strings.Contains(text, "object=") {
		t.Fatalf("metrics contain high-cardinality identity labels:\n%s", text)
	}
}

func TestCursorAgeTracksPersistedUpdateContinuouslyAndResets(t *testing.T) {
	t.Parallel()

	updated := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	now := updated.Add(10 * time.Second)
	registry := prometheus.NewRegistry()
	metrics := newMetrics(registry, func() time.Time { return now })

	metrics.CursorLoaded(updated)
	if got := metricValue(t, registry, "cwm_minio_tierer_cursor_age_seconds"); got != 10 {
		t.Fatalf("loaded cursor age = %v, want 10", got)
	}
	now = now.Add(5 * time.Second)
	if got := metricValue(t, registry, "cwm_minio_tierer_cursor_age_seconds"); got != 15 {
		t.Fatalf("advancing cursor age = %v, want 15", got)
	}

	metrics.CursorSaved(now.Add(time.Second))
	if got := metricValue(t, registry, "cwm_minio_tierer_cursor_age_seconds"); got != 0 {
		t.Fatalf("future cursor age = %v, want 0", got)
	}
	metrics.CursorReset()
	if got := metricValue(t, registry, "cwm_minio_tierer_cursor_age_seconds"); got != 0 {
		t.Fatalf("reset cursor age = %v, want 0", got)
	}
}

func TestDependencyReadinessLogsStructuredFailure(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	metrics := NewMetrics(prometheus.NewRegistry())
	ready := dependencyReadiness{redis: failingPinger{}, minio: emptyBuckets{}, timeout: time.Second, metrics: metrics, logger: logger}
	if err := ready.Ready(context.Background()); err == nil {
		t.Fatal("Ready() error = nil")
	}
	if text := output.String(); !strings.Contains(text, `"dependency":"redis"`) || !strings.Contains(text, `"operation":"readiness"`) {
		t.Fatalf("readiness log = %s", text)
	}
}

func TestAuditRestoreObservationDoesNotAdvanceMutationTimestamp(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.RestoreObserved(ActionRestore, false, nil)
	if got := metricValue(t, registry, "cwm_minio_tierer_last_successful_mutation_timestamp_seconds"); got != 0 {
		t.Fatalf("audit restore observation advanced mutation timestamp to %f", got)
	}
}

func metricValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.Metric) == 1 {
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

func gatherText(t *testing.T, metrics *Metrics) string {
	t.Helper()
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return response.Body.String()
}

type fakeReadiness struct {
	err   error
	calls int
}

type failingPinger struct{}

func (failingPinger) Ping(context.Context) error { return errors.New("ping failed") }

type emptyBuckets struct{}

func (emptyBuckets) Buckets(context.Context) ([]string, error) { return nil, nil }

type blockingService struct {
	serveRelease    chan struct{}
	shutdownRelease chan struct{}
	once            sync.Once
}

func (s *blockingService) ListenAndServe() error { <-s.serveRelease; return http.ErrServerClosed }
func (s *blockingService) Shutdown(context.Context) error {
	<-s.shutdownRelease
	return nil
}

func (f *fakeReadiness) Ready(context.Context) error {
	f.calls++
	return f.err
}
