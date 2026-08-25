package updater

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricNamespace = "cwm_minio_tierer_updater"

type Metrics struct {
	registry *prometheus.Registry

	httpRequests        *prometheus.CounterVec
	httpRecords         *prometheus.CounterVec
	httpRejections      *prometheus.CounterVec
	queueDepth          prometheus.Gauge
	batchEvents         prometheus.Histogram
	batchUniqueKeys     prometheus.Histogram
	batchDuration       prometheus.Histogram
	batchErrors         prometheus.Counter
	redisFailures       prometheus.Counter
	riskOverride        *prometheus.GaugeVec
	lastSuccessfulBatch prometheus.Gauge
}

func NewMetrics(registry *prometheus.Registry, dataLossRisk, duplicateRisk bool) *Metrics {
	metrics := &Metrics{
		registry: registry,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "http_requests_total",
			Help:      "Updater ingestion requests by bounded result.",
		}, []string{"result"}),
		httpRecords: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "http_records_total",
			Help:      "Valid access records by containing batch result.",
		}, []string{"result"}),
		httpRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "http_rejections_total",
			Help:      "Rejected updater requests by bounded reason.",
		}, []string{"reason"}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "queue_depth",
			Help:      "Current number of accepted requests waiting for batching.",
		}),
		batchEvents: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "batch_events",
			Help:      "Access records represented by each Redis batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 14),
		}),
		batchUniqueKeys: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "batch_unique_keys",
			Help:      "Unique Redis keys represented by each batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 14),
		}),
		batchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "batch_duration_seconds",
			Help:      "Redis batch execution latency in seconds.",
			Buckets:   prometheus.DefBuckets,
		}),
		batchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "batch_errors_total",
			Help:      "Redis batches that returned an error.",
		}),
		redisFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "redis_failures_total",
			Help:      "Redis operation failures observed by the updater.",
		}),
		riskOverride: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "risk_override",
			Help:      "Whether a configured status-code risk override is active.",
		}, []string{"risk"}),
		lastSuccessfulBatch: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "last_successful_batch_timestamp_seconds",
			Help:      "Unix timestamp of the last Redis-acknowledged updater batch.",
		}),
	}
	registry.MustRegister(
		metrics.httpRequests,
		metrics.httpRecords,
		metrics.httpRejections,
		metrics.queueDepth,
		metrics.batchEvents,
		metrics.batchUniqueKeys,
		metrics.batchDuration,
		metrics.batchErrors,
		metrics.redisFailures,
		metrics.riskOverride,
		metrics.lastSuccessfulBatch,
	)
	metrics.riskOverride.WithLabelValues("data_loss").Set(boolFloat(dataLossRisk))
	metrics.riskOverride.WithLabelValues("duplicate").Set(boolFloat(duplicateRisk))
	return metrics
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

func (m *Metrics) SetQueueDepth(depth int) {
	m.queueDepth.Set(float64(depth))
}

func (m *Metrics) ObserveBatch(events, uniqueKeys int, latency time.Duration, err error) {
	m.batchEvents.Observe(float64(events))
	m.batchUniqueKeys.Observe(float64(uniqueKeys))
	m.batchDuration.Observe(latency.Seconds())
	if err != nil {
		m.batchErrors.Inc()
		m.ObserveRedisFailure()
		return
	}
	m.lastSuccessfulBatch.SetToCurrentTime()
}

func (m *Metrics) ObserveRedisFailure() {
	m.redisFailures.Inc()
}

func (m *Metrics) ObserveHTTPResult(result string, records int) {
	m.httpRequests.WithLabelValues(result).Inc()
	if records > 0 {
		m.httpRecords.WithLabelValues(result).Add(float64(records))
	}
}

func (m *Metrics) ObserveRejection(reason string) {
	m.httpRejections.WithLabelValues(reason).Inc()
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
