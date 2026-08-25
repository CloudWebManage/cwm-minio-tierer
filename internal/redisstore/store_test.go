package redisstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStoreAppliesWholeBatchWithAbsoluteExpiry(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewStore(NewClient(client))
	if err := store.Load(context.Background()); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	expires := now.Add(48 * time.Hour)
	err := store.Apply(context.Background(), []Increment{
		{Key: "access:a", Delta: 3, ExpireAt: expires},
		{Key: "access:b", Delta: 2, ExpireAt: expires.Add(time.Hour)},
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if got := mustGet(t, server, "access:a"); got != "3" {
		t.Fatalf("access:a = %q, want 3", got)
	}
	if got := server.TTL("access:a"); got != 48*time.Hour {
		t.Fatalf("TTL(access:a) = %s, want 48h", got)
	}

	if err := store.Apply(context.Background(), []Increment{{Key: "access:a", Delta: 4, ExpireAt: expires.Add(2 * time.Hour)}}); err != nil {
		t.Fatalf("second Apply() error = %v", err)
	}
	if got := mustGet(t, server, "access:a"); got != "7" {
		t.Fatalf("access:a after second batch = %q, want 7", got)
	}
	if got := server.TTL("access:a"); got != 50*time.Hour {
		t.Fatalf("TTL(access:a) after second batch = %s, want 50h", got)
	}
}

func TestStorePrevalidatesEveryKeyBeforeWriting(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	server.SetTime(now)
	server.Set("good", "5")
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.RPush(context.Background(), "wrong-type", "value").Err(); err != nil {
		t.Fatalf("RPush() error = %v", err)
	}
	store := NewStore(NewClient(client))

	err := store.Apply(context.Background(), []Increment{
		{Key: "good", Delta: 2, ExpireAt: now.Add(time.Hour)},
		{Key: "wrong-type", Delta: 1, ExpireAt: now.Add(time.Hour)},
	})
	if err == nil || IsAmbiguous(err) {
		t.Fatalf("Apply() error = %v, want deterministic validation error", err)
	}
	if got := mustGet(t, server, "good"); got != "5" {
		t.Fatalf("good was partially updated to %q", got)
	}
}

func TestStoreRejectsMalformedNegativeAndOverflowingCountersBeforeWriting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "malformed", value: "01"},
		{name: "negative", value: "-1"},
		{name: "overflow", value: "9223372036854775807"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
			server.SetTime(now)
			server.Set("bad", tt.value)
			server.Set("good", "8")
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			store := NewStore(NewClient(client))
			err := store.Apply(context.Background(), []Increment{
				{Key: "good", Delta: 1, ExpireAt: now.Add(time.Hour)},
				{Key: "bad", Delta: 1, ExpireAt: now.Add(time.Hour)},
			})
			if err == nil || IsAmbiguous(err) {
				t.Fatalf("Apply() error = %v, want deterministic validation error", err)
			}
			if got := mustGet(t, server, "good"); got != "8" {
				t.Fatalf("good was partially updated to %q", got)
			}
		})
	}
}

func TestStoreReloadsScriptAfterNOSCRIPT(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewStore(NewClient(client))
	if err := store.Load(context.Background()); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := client.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatalf("ScriptFlush() error = %v", err)
	}

	err := store.Apply(context.Background(), []Increment{{Key: "access:a", Delta: 1, ExpireAt: time.Now().Add(time.Hour)}})
	if err != nil {
		t.Fatalf("Apply() after SCRIPT FLUSH error = %v", err)
	}
	if got := mustGet(t, server, "access:a"); got != "1" {
		t.Fatalf("access:a = %q, want 1", got)
	}
}

func TestStoreMarksUnknownExecutionErrorsAmbiguous(t *testing.T) {
	t.Parallel()

	client := &failingClient{evalErr: errors.New("connection reset")}
	store := NewStore(client)
	err := store.Apply(context.Background(), []Increment{{Key: "access:a", Delta: 1, ExpireAt: time.Now().Add(time.Hour)}})
	if err == nil || !IsAmbiguous(err) {
		t.Fatalf("Apply() error = %v, want ambiguous error", err)
	}
}

func TestStoreClassifiesRepeatedNOSCRIPTAsDeterministicNonWriteFailure(t *testing.T) {
	t.Parallel()

	client := &failingClient{evalErr: errors.New("NOSCRIPT No matching script. Please use EVAL")}
	store := NewStore(client)
	err := store.Apply(context.Background(), []Increment{{Key: "access:a", Delta: 1, ExpireAt: time.Now().Add(time.Hour)}})
	if err == nil {
		t.Fatal("Apply() error = nil after repeated NOSCRIPT")
	}
	if IsAmbiguous(err) {
		t.Fatalf("Apply() error = %v, repeated NOSCRIPT cannot have written", err)
	}
}

type failingClient struct {
	evalErr error
}

func (f *failingClient) ScriptLoad(context.Context, string) (string, error) {
	return "sha", nil
}

func (f *failingClient) EvalSHA(context.Context, string, []string, ...any) (any, error) {
	return nil, f.evalErr
}

func mustGet(t *testing.T, server *miniredis.Miniredis, key string) string {
	t.Helper()
	value, err := server.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	return value
}
