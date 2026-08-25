package tierer

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsNamespace = "cwm_minio_tierer"

type Metrics struct {
	registry               *prometheus.Registry
	scans                  *prometheus.CounterVec
	scanDuration           prometheus.Histogram
	objects                *prometheus.CounterVec
	coverageSkips          prometheus.Counter
	cursorAge              prometheus.GaugeFunc
	cursorMu               sync.RWMutex
	cursorUpdatedAt        time.Time
	now                    func() time.Time
	cursorErrors           prometheus.Counter
	budgetUsed             *prometheus.GaugeVec
	budgetExhausted        *prometheus.CounterVec
	markers                *prometheus.CounterVec
	transitioned           *prometheus.CounterVec
	restores               *prometheus.CounterVec
	dependencyFailures     *prometheus.CounterVec
	redisFailures          prometheus.Counter
	lastSuccessfulScan     prometheus.Gauge
	lastSuccessfulMutation prometheus.Gauge
}

func NewMetrics(registry *prometheus.Registry) *Metrics {
	return newMetrics(registry, time.Now)
}

func newMetrics(registry *prometheus.Registry, now func() time.Time) *Metrics {
	if now == nil {
		now = time.Now
	}
	m := &Metrics{
		registry:               registry,
		scans:                  prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "scans_total", Help: "Full scan traversals by result."}, []string{"result"}),
		scanDuration:           prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: metricsNamespace, Name: "scan_duration_seconds", Help: "Full scan traversal duration.", Buckets: prometheus.DefBuckets}),
		objects:                prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "objects_total", Help: "Objects handled by bounded outcome."}, []string{"outcome"}),
		coverageSkips:          prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "coverage_skips_total", Help: "Low decisions skipped because coverage was incomplete."}),
		now:                    now,
		cursorErrors:           prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "cursor_errors_total", Help: "Cursor load, save, or reset failures."}),
		budgetUsed:             prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "budget_used", Help: "Current UTC daily budget use."}, []string{"kind", "resource"}),
		budgetExhausted:        prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "budget_exhaustions_total", Help: "Actions skipped due to daily budget exhaustion."}, []string{"kind"}),
		markers:                prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "marker_outcomes_total", Help: "Marker planning and mutation outcomes."}, []string{"outcome", "result"}),
		transitioned:           prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "transitioned_state_total", Help: "Objects evaluated by authoritative transition state."}, []string{"state"}),
		restores:               prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "restore_outcomes_total", Help: "Restore starts and renewals by bounded result."}, []string{"action", "result"}),
		dependencyFailures:     prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "dependency_failures_total", Help: "Dependency operation failures."}, []string{"dependency"}),
		redisFailures:          prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "redis_failures_total", Help: "Redis operation failures observed by the tierer."}),
		lastSuccessfulScan:     prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "last_successful_scan_timestamp_seconds", Help: "Unix timestamp of the last completed full traversal."}),
		lastSuccessfulMutation: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "last_successful_mutation_timestamp_seconds", Help: "Unix timestamp of the last successful MinIO mutation."}),
	}
	m.cursorAge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "cursor_age_seconds", Help: "Current age of the persisted cursor update; zero when no cursor is persisted."}, m.cursorAgeSeconds)
	registry.MustRegister(m.scans, m.scanDuration, m.objects, m.coverageSkips, m.cursorAge, m.cursorErrors, m.budgetUsed, m.budgetExhausted, m.markers, m.transitioned, m.restores, m.dependencyFailures, m.redisFailures, m.lastSuccessfulScan, m.lastSuccessfulMutation)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
func (m *Metrics) ScanStarted() {}
func (m *Metrics) ScanFinished(duration time.Duration, err error) {
	m.scanDuration.Observe(duration.Seconds())
	if err != nil {
		m.scans.WithLabelValues("error").Inc()
		return
	}
	m.scans.WithLabelValues("success").Inc()
	m.lastSuccessfulScan.SetToCurrentTime()
}
func (m *Metrics) ObjectHandled(outcome string) { m.objects.WithLabelValues(outcome).Inc() }
func (m *Metrics) CoverageSkipped()             { m.coverageSkips.Inc() }
func (m *Metrics) CursorSaved(updated time.Time) {
	m.setCursorUpdatedAt(updated)
}
func (m *Metrics) CursorLoaded(updated time.Time) {
	m.setCursorUpdatedAt(updated)
}
func (m *Metrics) CursorReset() {
	m.setCursorUpdatedAt(time.Time{})
}
func (m *Metrics) setCursorUpdatedAt(updated time.Time) {
	m.cursorMu.Lock()
	m.cursorUpdatedAt = updated
	m.cursorMu.Unlock()
}
func (m *Metrics) cursorAgeSeconds() float64 {
	m.cursorMu.RLock()
	updated := m.cursorUpdatedAt
	m.cursorMu.RUnlock()
	if updated.IsZero() {
		return 0
	}
	age := m.now().Sub(updated).Seconds()
	if age < 0 {
		return 0
	}
	return age
}
func (m *Metrics) CursorError() { m.cursorErrors.Inc() }
func (m *Metrics) BudgetObserved(kind BudgetKind, reservation BudgetReservation) {
	m.budgetUsed.WithLabelValues(string(kind), "attempts").Set(float64(reservation.UsedAttempts))
	m.budgetUsed.WithLabelValues(string(kind), "bytes").Set(float64(reservation.UsedBytes))
}
func (m *Metrics) BudgetExhausted(kind BudgetKind) {
	m.budgetExhausted.WithLabelValues(string(kind)).Inc()
}
func (m *Metrics) MarkerObserved(outcome MarkerOutcome, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	m.markers.WithLabelValues(string(outcome), result).Inc()
}
func (m *Metrics) MarkerApplied(MarkerOutcome) { m.lastSuccessfulMutation.SetToCurrentTime() }
func (m *Metrics) TransitionState(transitioned bool) {
	state := "local"
	if transitioned {
		state = "transitioned"
	}
	m.transitioned.WithLabelValues(state).Inc()
}
func (m *Metrics) RestoreObserved(action Action, pending bool, err error) {
	result := "success"
	if pending {
		result = "pending"
	}
	if err != nil {
		result = "error"
	}
	m.restores.WithLabelValues(string(action), result).Inc()
}
func (m *Metrics) RestoreApplied(Action) { m.lastSuccessfulMutation.SetToCurrentTime() }
func (m *Metrics) DependencyFailure(dependency string) {
	m.dependencyFailures.WithLabelValues(dependency).Inc()
	if dependency == "redis" {
		m.redisFailures.Inc()
	}
}
