package tierer

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestScanDurationHistogramObservesMinutesHoursAndDay(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics(prometheus.NewRegistry())
	metrics.ScanFinished(45*time.Second, nil)
	metrics.ScanFinished(8*time.Minute, errors.New("scan failed"))
	metrics.ScanFinished(90*time.Minute, nil)
	metrics.ScanFinished(20*time.Hour, nil)
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, sample := range []string{
		`cwm_minio_tierer_scan_duration_seconds_bucket{le="10"} 0`,
		`cwm_minio_tierer_scan_duration_seconds_bucket{le="60"} 1`,
		`cwm_minio_tierer_scan_duration_seconds_bucket{le="600"} 2`,
		`cwm_minio_tierer_scan_duration_seconds_bucket{le="7200"} 3`,
		`cwm_minio_tierer_scan_duration_seconds_bucket{le="86400"} 4`,
		`cwm_minio_tierer_scan_duration_seconds_bucket{le="+Inf"} 4`,
		`cwm_minio_tierer_scan_duration_seconds_count 4`,
		`cwm_minio_tierer_scan_duration_seconds_sum 77925`,
		`cwm_minio_tierer_scans_total{result="success"} 3`,
		`cwm_minio_tierer_scans_total{result="error"} 1`,
	} {
		if !strings.Contains(response.Body.String(), sample+"\n") {
			t.Errorf("scrape missing %q", sample)
		}
	}
}
