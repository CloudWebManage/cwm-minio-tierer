package updater

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestRunServiceCancelsPendingHostnameStartup(t *testing.T) {
	// DefaultResolver is process-wide, so this test and its subtests must not
	// run in parallel. The resolver never sends traffic outside the process.
	for _, reason := range []string{"parent cancellation", "peer bind failure"} {
		t.Run(reason, func(t *testing.T) {
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var startOnce, cancelOnce sync.Once
			originalResolver := net.DefaultResolver
			net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				startOnce.Do(func() { close(started) })
				select {
				case <-ctx.Done():
					cancelOnce.Do(func() { close(canceled) })
					return nil, ctx.Err()
				case <-release:
					return nil, errors.New("test resolver released for cleanup")
				}
			}}
			t.Cleanup(func() { net.DefaultResolver = originalResolver })
			pending := &httpService{Server: &http.Server{Addr: "pending-updater.invalid:8922", Handler: http.NotFoundHandler()}}
			reservation := reserveTestAddress(t)
			peerAddress := reservation.Addr().String()
			if reason == "parent cancellation" {
				_ = reservation.Close()
			}
			peer := &startupBarrierServer{
				httpService: &httpService{Server: &http.Server{Addr: peerAddress, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })}},
				ready:       started,
			}
			batcher := NewBatcher(BatcherConfig{QueueSize: 1}, &recordingStore{}, nil)
			redisClosed := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var runErr error
			go func() {
				runErr = runService(ctx, pending, batcher, 50*time.Millisecond, func() error {
					close(redisClosed)
					return nil
				}, peer)
				close(done)
			}()
			t.Cleanup(func() {
				cancel()
				close(release)
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("service did not exit after test resolver was released")
				}
			})
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("hostname resolution did not start")
			}
			if reason == "parent cancellation" {
				waitForHTTP(t, listenerTestClient(t), "http://"+peerAddress+"/livez")
				cancel()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("pending hostname startup prevented service shutdown")
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("pending resolver was not canceled")
			}
			if reason == "parent cancellation" && runErr != nil {
				t.Fatalf("canceled startup error = %v", runErr)
			}
			if reason == "peer bind failure" {
				var opErr *net.OpError
				if !errors.As(runErr, &opErr) || opErr.Op != "listen" {
					t.Fatalf("service error = %v, want peer bind error", runErr)
				}
				_ = reservation.Close()
			}
			assertAddressReleased(t, peerAddress)
			select {
			case <-batcher.done:
			default:
				t.Fatal("batcher goroutine was not joined")
			}
			if batcher.Ready() {
				t.Fatal("batcher remained ready")
			}
			select {
			case <-redisClosed:
			default:
				t.Fatal("Redis was not closed")
			}
		})
	}
}

type startupBarrierServer struct {
	*httpService
	ready <-chan struct{}
}

func (s *startupBarrierServer) ListenAndServe(ctx context.Context) error {
	<-s.ready
	return s.httpService.ListenAndServe(ctx)
}
