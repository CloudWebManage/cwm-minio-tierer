package updater

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
)

func TestRunMetricsListenerIsolatesIngestion(t *testing.T) {
	config, redis, ingestion, monitoring := listenerTestConfig(t, true)
	stop := startTestUpdater(t, config)
	client := listenerTestClient(t)
	waitForUpdaterListeners(t, client, ingestion, monitoring)
	for _, address := range []string{ingestion, monitoring} {
		for _, path := range []string{"/metrics", "/livez", "/readyz"} {
			status, _ := requestHTTP(t, client, http.MethodGet, address+path)
			if status != http.StatusOK {
				t.Fatalf("GET %s%s status = %d", address, path, status)
			}
		}
	}
	for _, path := range []string{"/", "/?source=vector", "/metrics", "/livez", "/readyz", "/ingest", "/other"} {
		status, _ := requestHTTP(t, client, http.MethodPost, monitoring+path)
		if status < 400 || status >= 500 {
			t.Fatalf("monitoring POST %s status = %d, want 4xx", path, status)
		}
	}
	if keys := redis.Keys(); len(keys) != 0 {
		t.Fatalf("monitoring requests wrote Redis keys: %v", keys)
	}
	_, metrics := requestHTTP(t, client, http.MethodGet, monitoring+"/metrics")
	for _, name := range []string{"http_requests_total{", "http_records_total{", "http_rejections_total{"} {
		if strings.Contains(metrics, "cwm_minio_tierer_updater_"+name) {
			t.Fatalf("monitoring requests changed ingestion accounting: %s", name)
		}
	}
	if !strings.Contains(metrics, "cwm_minio_tierer_updater_batch_events_count 0\n") {
		t.Fatal("monitoring requests caused a Redis batch")
	}
	status, _ := requestHTTP(t, client, http.MethodPost, ingestion+"/")
	if status != http.StatusOK {
		t.Fatalf("ingestion status = %d", status)
	}
	keys := redis.Keys()
	if len(keys) != 1 {
		t.Fatalf("ingestion Redis keys = %v, want one access counter", keys)
	}
	if value, err := redis.Get(keys[0]); err != nil || value != "1" {
		t.Fatalf("ingestion counter = %q, %v, want 1", value, err)
	}
	_, metrics = requestHTTP(t, client, http.MethodGet, monitoring+"/metrics")
	if !strings.Contains(metrics, `cwm_minio_tierer_updater_http_records_total{result="success"} 1`) {
		t.Fatal("separate metrics listener did not expose ingestion result")
	}
	redis.SetError("Redis unavailable")
	status, _ = requestHTTP(t, client, http.MethodGet, monitoring+"/readyz")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("monitoring readiness with failed Redis = %d", status)
	}
	redis.SetError("")
	if err := stop(); err != nil {
		t.Fatalf("Run() shutdown error = %v", err)
	}
	assertRedisDisconnected(t, redis)
	assertAddressReleased(t, strings.TrimPrefix(ingestion, "http://"))
	assertAddressReleased(t, strings.TrimPrefix(monitoring, "http://"))
}

func TestRunLegacyListenerWithoutMetricsAddress(t *testing.T) {
	config, _, ingestion, _ := listenerTestConfig(t, false)
	stop := startTestUpdater(t, config)
	client := listenerTestClient(t)
	waitForHTTP(t, client, ingestion+"/readyz")
	for _, methodPath := range [][2]string{{http.MethodGet, "/metrics"}, {http.MethodPost, "/"}} {
		status, _ := requestHTTP(t, client, methodPath[0], ingestion+methodPath[1])
		if status != http.StatusOK {
			t.Fatalf("legacy %v status = %d", methodPath, status)
		}
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestListenerReadinessWaitsForDelayedIngestion(t *testing.T) {
	monitoringReady := make(chan struct{})
	var monitoringOnce sync.Once
	monitoring := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/livez" {
			monitoringOnce.Do(func() { close(monitoringReady) })
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(monitoring.Close)
	reservation := reserveTestAddress(t)
	ingestionAddress := reservation.Addr().String()
	_ = reservation.Close()
	ingestion := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })}
	t.Cleanup(func() { _ = ingestion.Close() })
	client := listenerTestClient(t)
	transport := client.Transport
	var ingestionOnce sync.Once
	// Hold the ingestion bind until monitoring is serving and the readiness
	// helper actually probes ingestion. A metrics assertion without that probe
	// deterministically gets connection refused, rather than a scheduling flake.
	client.Transport = listenerRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == ingestionAddress && request.URL.Path == "/livez" {
			select {
			case <-monitoringReady:
			default:
				return nil, errors.New("ingestion probed before monitoring was serving")
			}
			var listenErr error
			ingestionOnce.Do(func() {
				listener, err := net.Listen("tcp", ingestionAddress)
				listenErr = err
				if err == nil {
					go func() { _ = ingestion.Serve(listener) }()
				}
			})
			if listenErr != nil {
				return nil, listenErr
			}
		}
		return transport.RoundTrip(request)
	})
	url := "http://" + ingestionAddress
	waitForUpdaterListeners(t, client, url, monitoring.URL)
	for _, address := range []string{url, monitoring.URL} {
		if status, _ := requestHTTP(t, client, http.MethodGet, address+"/metrics"); status != http.StatusOK {
			t.Fatalf("metrics status after readiness wait = %d", status)
		}
	}
}

type listenerRoundTripFunc func(*http.Request) (*http.Response, error)

func (f listenerRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestRunListenerBindFailureCleansUp(t *testing.T) {
	for _, occupied := range []string{"ingestion", "metrics"} {
		t.Run(occupied, func(t *testing.T) {
			config, redis, ingestion, monitoring := listenerTestConfig(t, true)
			failed, healthy := strings.TrimPrefix(ingestion, "http://"), strings.TrimPrefix(monitoring, "http://")
			if occupied == "metrics" {
				failed, healthy = healthy, failed
			}
			listener, err := net.Listen("tcp", failed)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Run(ctx, config, discardLogger()) }()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), failed) {
					t.Fatalf("Run() error = %v, want bind error for %s", err, failed)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Run hung after listener bind failure")
			}
			assertAddressReleased(t, healthy)
			assertRedisDisconnected(t, redis)
		})
	}
}

func TestRunServiceClosesActiveConnectionsOnShutdownTimeout(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	listener := reserveTestAddress(t)
	server := &boundTestServer{
		Server: &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			close(entered)
			_, _ = io.Copy(io.Discard, request.Body)
			close(exited)
		})},
		listener: listener,
	}
	t.Cleanup(func() { _ = server.Close() })
	batcher := NewBatcher(BatcherConfig{QueueSize: 1}, &recordingStore{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- runService(ctx, server, batcher, 20*time.Millisecond, func() error { return nil }) }()
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := io.WriteString(connection, "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\nx"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("partial request did not enter handler")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("service hung on partial request")
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("active HTTP connection survived the shutdown deadline")
	}
}

type boundTestServer struct {
	*http.Server
	listener net.Listener
}

func (s *boundTestServer) ListenAndServe(context.Context) error { return s.Serve(s.listener) }

func TestRunServiceDualListenersDrainAcceptedRequest(t *testing.T) {
	for _, stopReason := range []string{"cancel", "ingestion failure", "metrics failure"} {
		t.Run(stopReason, func(t *testing.T) {
			store := &completionStore{started: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(store.release) }) }
			metrics := NewMetrics(prometheus.NewRegistry(), false, false)
			batcher := NewBatcher(BatcherConfig{QueueSize: 1, MaxEvents: 1, MaxKeys: 1, OperationTimeout: 3 * time.Second}, store, metrics)
			config := testConfig()
			ingestion := &boundTestServer{Server: NewHTTPServer(config, NewHTTPHandler(config, batcher, &healthStub{}, metrics, fixedClock(), discardLogger())), listener: reserveTestAddress(t)}
			monitoring := &boundTestServer{Server: NewHTTPServer(config, NewMetricsHTTPHandler(config, batcher, &healthStub{}, metrics)), listener: reserveTestAddress(t)}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var runErr error
			redisClosed := false
			go func() {
				runErr = runService(ctx, ingestion, batcher, time.Second, func() error {
					redisClosed = true
					select {
					case <-store.completed:
						return nil
					default:
						return errors.New("Redis closed before accepted batch completed")
					}
				}, monitoring)
				close(done)
			}()
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("dual-listener service hung during cleanup")
				}
			})
			client := listenerTestClient(t)
			waitForUpdaterListeners(t, client, "http://"+ingestion.listener.Addr().String(), "http://"+monitoring.listener.Addr().String())
			requestDone := make(chan error, 1)
			go func() {
				response, err := client.Post("http://"+ingestion.listener.Addr().String()+"/", "application/json", strings.NewReader(`{"bucket":"b","object":"o"}`))
				if err == nil {
					_ = response.Body.Close()
					if response.StatusCode != http.StatusOK {
						err = errors.New("accepted request was not acknowledged successfully")
					}
				}
				requestDone <- err
			}()
			select {
			case <-store.started:
			case <-time.After(time.Second):
				t.Fatal("HTTP request did not reach batch execution")
			}
			switch stopReason {
			case "cancel":
				cancel()
			case "ingestion failure":
				_ = ingestion.listener.Close()
			case "metrics failure":
				_ = monitoring.listener.Close()
			}
			waitForListenerClosed(t, ingestion.listener.Addr().String())
			waitForListenerClosed(t, monitoring.listener.Addr().String())
			select {
			case <-done:
				t.Fatal("service returned before accepted batch completed")
			default:
			}
			release()
			select {
			case err := <-requestDone:
				if err != nil {
					t.Fatalf("accepted request failed: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("accepted request hung")
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("dual-listener shutdown hung")
			}
			if stopReason == "cancel" && runErr != nil || stopReason != "cancel" && !errors.Is(runErr, net.ErrClosed) {
				t.Fatalf("RunService() error = %v for %s", runErr, stopReason)
			}
			if !redisClosed || batcher.Ready() {
				t.Fatalf("cleanup state: Redis closed=%t batcher ready=%t", redisClosed, batcher.Ready())
			}
		})
	}
}

func TestRunServiceDualListenersCanceledAtStartup(t *testing.T) {
	for range 20 {
		first := &boundTestServer{Server: &http.Server{Handler: http.NotFoundHandler()}, listener: reserveTestAddress(t)}
		second := &boundTestServer{Server: &http.Server{Handler: http.NotFoundHandler()}, listener: reserveTestAddress(t)}
		batcher := NewBatcher(BatcherConfig{QueueSize: 1}, &recordingStore{}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		done := make(chan error, 1)
		go func() { done <- runService(ctx, first, batcher, time.Second, func() error { return nil }, second) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("immediate cancellation hung during listener startup")
		}
		assertAddressReleased(t, first.listener.Addr().String())
		assertAddressReleased(t, second.listener.Addr().String())
		if batcher.Ready() {
			t.Fatal("batcher remained ready after immediate cancellation")
		}
	}
}

func waitForListenerClosed(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err != nil {
			return
		}
		_ = connection.Close()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("listener %s still accepts connections during shutdown", address)
}

func assertRedisDisconnected(t *testing.T, server *miniredis.Miniredis) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if server.CurrentConnectionCount() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Redis connections after shutdown = %d, want zero", server.CurrentConnectionCount())
}

func listenerTestConfig(t *testing.T, metrics bool) (Config, *miniredis.Miniredis, string, string) {
	t.Helper()
	redis := miniredis.RunT(t)
	ingestion := reserveTestAddress(t)
	monitoring := reserveTestAddress(t)
	env := map[string]string{
		"INSTANCE_ID": "site-a", "ACCESS_RETENTION": "168h", "REDIS_ADDR": redis.Addr(),
		"UPDATER_LISTEN_ADDR":     ingestion.Addr().String(),
		"REDIS_OPERATION_TIMEOUT": "250ms", "UPDATER_SHUTDOWN_TIMEOUT": "1s",
		"UPDATER_BATCH_MAX_WAIT": "1ms",
	}
	if metrics {
		env["UPDATER_METRICS_LISTEN_ADDR"] = monitoring.Addr().String()
	}
	config, err := LoadConfig(mapLookup(env))
	if err != nil {
		t.Fatal(err)
	}
	_ = ingestion.Close()
	_ = monitoring.Close()
	return config, redis, "http://" + ingestion.Addr().String(), "http://" + monitoring.Addr().String()
}

func reserveTestAddress(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func assertAddressReleased(t *testing.T, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listener %s not released: %v", address, err)
	}
	_ = listener.Close()
}

func startTestUpdater(t *testing.T, config Config) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = Run(ctx, config, discardLogger())
		close(done)
	}()
	stop := func() error {
		cancel()
		select {
		case <-done:
			return runErr
		case <-time.After(3 * time.Second):
			t.Fatal("Run hung during shutdown")
			return nil
		}
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func listenerTestClient(t *testing.T) *http.Client {
	t.Helper()
	transport := &http.Transport{DisableKeepAlives: true}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func waitForHTTP(t *testing.T, client *http.Client, url string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("listener did not become ready: %s", url)
}

func waitForUpdaterListeners(t *testing.T, client *http.Client, ingestion, monitoring string) {
	t.Helper()
	waitForHTTP(t, client, monitoring+"/livez")
	waitForHTTP(t, client, ingestion+"/livez")
}

func requestHTTP(t *testing.T, client *http.Client, method, url string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(`{"bucket":"b","object":"o"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(body)
}
