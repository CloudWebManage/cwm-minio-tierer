package tierer

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisStoreReadsMissingAsZeroButRejectsMalformedAndWrongType(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(NewRedisClient(client), "site-a", "coverage:2006:01:02:15", "complete", true)
	hours := []time.Time{
		time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC),
	}
	first := mustAccessKey(t, "site-a", "bucket", "object", hours[0])
	server.Set(first, "7")

	counts, err := store.ReadCounts(context.Background(), "bucket", "object", hours)
	if err != nil {
		t.Fatalf("ReadCounts() error = %v", err)
	}
	if len(counts) != 2 || counts[0] != 7 || counts[1] != 0 {
		t.Fatalf("ReadCounts() = %v, want [7 0]", counts)
	}

	second := mustAccessKey(t, "site-a", "bucket", "object", hours[1])
	server.Set(second, "01")
	if _, err := store.ReadCounts(context.Background(), "bucket", "object", hours); err == nil {
		t.Fatal("ReadCounts() error = nil for malformed integer")
	}
	server.Del(second)
	if err := client.RPush(context.Background(), second, "x").Err(); err != nil {
		t.Fatalf("RPush() error = %v", err)
	}
	if _, err := store.ReadCounts(context.Background(), "bucket", "object", hours); err == nil {
		t.Fatal("ReadCounts() error = nil for wrong type")
	}
}

func TestRedisStoreCoverageRequiresExactConfiguredRecords(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(NewRedisClient(client), "site-a", "coverage:2006:01:02:15", "complete:g1", true)
	hours := []time.Time{
		time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC),
	}
	server.Set("coverage:2026:08:25:12", "complete:g1")
	server.Set("coverage:2026:08:25:13", "complete:g2")
	covered, err := store.ReadCoverage(context.Background(), hours)
	if err != nil {
		t.Fatalf("ReadCoverage() error = %v", err)
	}
	if len(covered) != 2 || !covered[0] || covered[1] {
		t.Fatalf("ReadCoverage() = %v, want [true false]", covered)
	}
	server.Del("coverage:2026:08:25:13")
	if err := client.RPush(context.Background(), "coverage:2026:08:25:13", "complete:g1").Err(); err != nil {
		t.Fatalf("RPush() error = %v", err)
	}
	if _, err := store.ReadCoverage(context.Background(), hours); err == nil {
		t.Fatal("ReadCoverage() error = nil for wrong type")
	}
}

func TestRedisStoreDisabledCoverageReturnsCompleteWithoutRedisRead(t *testing.T) {
	t.Parallel()
	commands := &countingRedisCommands{}
	store := NewRedisStore(commands, "site-a", "", "", false)
	hours := []time.Time{
		time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC),
	}
	covered, err := store.ReadCoverage(context.Background(), hours)
	if err != nil {
		t.Fatalf("ReadCoverage() error = %v", err)
	}
	if commands.evals != 0 || len(covered) != 2 || !covered[0] || !covered[1] {
		t.Fatalf("ReadCoverage() evals=%d coverage=%v, want no Redis read and complete coverage", commands.evals, covered)
	}
}

func TestRedisStoreReadsWholeChunkWithOneCountAndOneCoverageRequest(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	commands := &countingRedisCommands{redisCommands: NewRedisClient(client)}
	store := NewRedisStore(commands, "site-a", "coverage:2006:01:02:15", "complete", true)
	lowHours := []time.Time{time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC)}
	highHours := []time.Time{time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)}
	objects := []Object{{Bucket: "b", Name: "one"}, {Bucket: "b", Name: "two"}}
	server.Set(mustAccessKey(t, "site-a", "b", "one", lowHours[0]), "2")
	server.Set(mustAccessKey(t, "site-a", "b", "two", highHours[0]), "3")
	server.Set("coverage:2026:08:25:12", "complete")
	server.Set("coverage:2026:08:25:13", "complete")

	usage, coverage, err := store.ReadChunk(context.Background(), objects, lowHours, highHours)
	if err != nil {
		t.Fatalf("ReadChunk() error = %v", err)
	}
	if commands.evals != 2 {
		t.Fatalf("ReadChunk() Redis Eval calls = %d, want 2", commands.evals)
	}
	if len(usage) != 2 || usage[0].Low[0] != 2 || usage[0].High[0] != 0 || usage[1].Low[0] != 0 || usage[1].High[0] != 3 || len(coverage) != 2 || !coverage[0] || !coverage[1] {
		t.Fatalf("ReadChunk() usage=%+v coverage=%v", usage, coverage)
	}

	bad := mustAccessKey(t, "site-a", "b", "two", lowHours[1])
	if err := client.RPush(context.Background(), bad, "wrong-type").Err(); err != nil {
		t.Fatalf("RPush() error = %v", err)
	}
	if _, _, err := store.ReadChunk(context.Background(), objects, lowHours, highHours); err == nil {
		t.Fatal("ReadChunk() error = nil for wrong-type counter in chunk")
	}
}

func TestAggregateAccessKeyLimitHandlesBoundaryAndOverflow(t *testing.T) {
	t.Parallel()
	if err := validateAggregateAccessKeys(100, 50, 50, maxRedisAccessKeysPerChunk); err != nil {
		t.Fatalf("validateAggregateAccessKeys() boundary error = %v", err)
	}
	if err := validateAggregateAccessKeys(101, 50, 50, maxRedisAccessKeysPerChunk); err == nil {
		t.Fatal("validateAggregateAccessKeys() error = nil above boundary")
	}
	if err := validateAggregateAccessKeys(math.MaxInt, math.MaxInt, math.MaxInt, maxRedisAccessKeysPerChunk); err == nil {
		t.Fatal("validateAggregateAccessKeys() error = nil for integer overflow")
	}
}

func TestRedisStoreReadChunkRejectsAggregateKeysBeforeRedisOrAllocation(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	commands := &countingRedisCommands{redisCommands: NewRedisClient(client)}
	store := NewRedisStore(commands, "site-a", "coverage:2006:01:02:15", "complete", true)
	objects := make([]Object, 101)
	for i := range objects {
		objects[i] = Object{Bucket: "bucket", Name: "object"}
	}
	hours := make([]time.Time, 50)
	for i := range hours {
		hours[i] = time.Date(2026, 8, 25, i%24, 0, 0, 0, time.UTC).Add(time.Duration(i/24) * 24 * time.Hour)
	}
	if _, _, err := store.ReadChunk(context.Background(), objects, hours, hours); err == nil {
		t.Fatal("ReadChunk() error = nil above aggregate key limit")
	}
	if commands.evals != 0 {
		t.Fatalf("ReadChunk() Redis Eval calls = %d, want zero before guard", commands.evals)
	}
}

func TestRedisStoreAtomicallyReservesDailyAttemptAndByteBudgets(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(NewRedisClient(client), "site-a", "coverage:2006:01:02:15", "complete", true)
	now := time.Date(2026, 8, 25, 23, 59, 0, 0, time.FixedZone("UTC-4", -4*60*60))
	limit := BudgetLimit{Attempts: 1, Bytes: 10}

	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservation, err := store.Reserve(context.Background(), now, BudgetTransition, limit, 6)
			if err != nil {
				t.Errorf("Reserve() error = %v", err)
				return
			}
			results <- reservation.Allowed
		}()
	}
	wg.Wait()
	close(results)
	allowed := 0
	for result := range results {
		if result {
			allowed++
		}
	}
	if allowed != 1 {
		t.Fatalf("allowed reservations = %d, want 1", allowed)
	}
	if got := mustRedisGet(t, server, "cwm-minio-tierer:v1:site-a:budget:2026:08:26:transition-attempts"); got != "1" {
		t.Fatalf("attempt budget = %q, want 1", got)
	}
	if got := mustRedisGet(t, server, "cwm-minio-tierer:v1:site-a:budget:2026:08:26:transition-bytes"); got != "6" {
		t.Fatalf("byte budget = %q, want 6", got)
	}
}

func TestRedisStoreRenewalReservesAttemptAndZeroBytesWithoutRefund(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(NewRedisClient(client), "site-a", "coverage:2006:01:02:15", "complete", true)
	now := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)
	limit := BudgetLimit{Attempts: 2, Bytes: 5}
	for i := 0; i < 2; i++ {
		result, err := store.Reserve(context.Background(), now, BudgetRestore, limit, 0)
		if err != nil || !result.Allowed {
			t.Fatalf("Reserve() = %+v, %v", result, err)
		}
	}
	third, err := store.Reserve(context.Background(), now, BudgetRestore, limit, 0)
	if err != nil || third.Allowed {
		t.Fatalf("third Reserve() = %+v, %v, want exhausted", third, err)
	}
	if got := mustRedisGet(t, server, "cwm-minio-tierer:v1:site-a:budget:2026:08:25:restore-attempts"); got != "2" {
		t.Fatalf("attempt budget = %q, want 2", got)
	}
	if server.Exists("cwm-minio-tierer:v1:site-a:budget:2026:08:25:restore-bytes") {
		t.Fatal("zero-byte renewal created byte key")
	}
}

func TestRedisStoreAllowsUnlimitedBudgets(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	commands := &countingRedisCommands{redisCommands: NewRedisClient(client)}
	store := NewRedisStore(commands, "site-a", "coverage:2006:01:02:15", "complete", true)
	now := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)

	reservation, err := store.Reserve(context.Background(), now, BudgetTransition, BudgetLimit{}, 100)
	if err != nil || !reservation.Allowed || commands.evals != 0 {
		t.Fatalf("unlimited Reserve() = %+v, %v, evals=%d", reservation, err, commands.evals)
	}

	reservation, err = store.Reserve(context.Background(), now, BudgetTransition, BudgetLimit{Attempts: 1}, 100)
	if err != nil || !reservation.Allowed || commands.evals != 1 {
		t.Fatalf("attempt-only Reserve() = %+v, %v, evals=%d", reservation, err, commands.evals)
	}
	reservation, err = store.Reserve(context.Background(), now, BudgetTransition, BudgetLimit{Attempts: 1}, 100)
	if err != nil || reservation.Allowed {
		t.Fatalf("second attempt-only Reserve() = %+v, %v, want exhausted", reservation, err)
	}
}

func TestRedisStoreBudgetUsesExactInt64ArithmeticAndRejectsMalformedState(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(NewRedisClient(client), "site-a", "coverage:2006:01:02:15", "complete", true)
	now := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)
	attemptKey := "cwm-minio-tierer:v1:site-a:budget:2026:08:25:transition-attempts"
	byteKey := "cwm-minio-tierer:v1:site-a:budget:2026:08:25:transition-bytes"
	server.Set(byteKey, "9007199254740992")
	reservation, err := store.Reserve(context.Background(), now, BudgetTransition, BudgetLimit{Attempts: 2, Bytes: 9007199254740992}, 1)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if reservation.Allowed {
		t.Fatalf("Reserve() = %+v, want exact byte-limit exhaustion", reservation)
	}
	if server.Exists(attemptKey) || mustRedisGet(t, server, byteKey) != "9007199254740992" {
		t.Fatal("exhausted reservation modified budget keys")
	}

	server.Set(attemptKey, "01")
	if _, err := store.Reserve(context.Background(), now, BudgetTransition, BudgetLimit{Attempts: 2, Bytes: 9007199254740993}, 0); err == nil {
		t.Fatal("Reserve() error = nil for non-canonical stored budget")
	}
}

func TestRedisStorePersistsVersionedNamespacedSortedCursor(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(NewRedisClient(client), "site-a", "coverage:2006:01:02:15", "complete", true)
	scope := ScopeHash([]string{"z", "a"}, []string{"tmp-"}, "marker", "value")
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	want := Cursor{Bucket: "b", Object: "a/z", UpdatedAt: now}
	if err := store.SaveCursor(context.Background(), scope, want); err != nil {
		t.Fatalf("SaveCursor() error = %v", err)
	}
	key := "cwm-minio-tierer:v1:site-a:cursor:" + scope
	raw := mustRedisGet(t, server, key)
	var envelope map[string]any
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		t.Fatalf("cursor JSON error = %v", err)
	}
	if envelope["version"] != float64(1) {
		t.Fatalf("cursor version = %v, want 1", envelope["version"])
	}
	got, err := store.LoadCursor(context.Background(), scope)
	if err != nil || got == nil || got.Bucket != want.Bucket || got.Object != want.Object || !got.UpdatedAt.Equal(now) {
		t.Fatalf("LoadCursor() = %+v, %v", got, err)
	}
	server.Set(key, `{"version":2,"bucket":"b","object":"o","updated_at":"2026-08-25T15:00:00Z"}`)
	if _, err := store.LoadCursor(context.Background(), scope); err == nil {
		t.Fatal("LoadCursor() error = nil for unsupported version")
	}
	server.Set(key, `{"version":1,"bucket":"b","object":"o","updated_at":"2026-08-25T15:00:00Z"} {}`)
	if _, err := store.LoadCursor(context.Background(), scope); err == nil {
		t.Fatal("LoadCursor() error = nil for trailing JSON")
	}
	if err := store.ResetCursor(context.Background(), scope); err != nil || server.Exists(key) {
		t.Fatalf("ResetCursor() error = %v, exists = %v", err, server.Exists(key))
	}
}

func TestConfigScopeHashChangesForActionAffectingConfiguration(t *testing.T) {
	t.Parallel()
	base := Config{
		Apply: false, MinIOEndpoint: "minio-a:9000", Policy: Policy{LowThreshold: 1, LowWindowHours: 2, HighThreshold: 3, HighWindowHours: 4},
		RestoreDays: 5, CoverageTemplate: "coverage:2006:01:02:15", CoverageValue: "complete", CoverageEnabled: true, MarkerKey: "m", MarkerValue: "v", ChunkSize: 10,
		TransitionBudget: BudgetLimit{Attempts: 11, Bytes: 12}, RestoreBudget: BudgetLimit{Attempts: 13, Bytes: 14},
	}
	wantDifferent := []Config{
		func() Config { c := base; c.Apply = true; return c }(),
		func() Config { c := base; c.MinIOEndpoint = "minio-b:9000"; return c }(),
		func() Config { c := base; c.CoverageEnabled = false; return c }(),
		func() Config { c := base; c.ChunkSize = 20; return c }(),
		func() Config { c := base; c.RestoreBudget.Attempts = 99; return c }(),
	}
	baseHash := ConfigScopeHash(base)
	for _, changed := range wantDifferent {
		if got := ConfigScopeHash(changed); got == baseHash {
			t.Fatalf("ConfigScopeHash() did not change for config %+v", changed)
		}
	}
}

func TestConfigScopeHashSeparatesExclusionsFromTypedConfiguration(t *testing.T) {
	t.Parallel()
	left := Config{Apply: false, MinIOEndpoint: "minio:9000", ExcludedPrefixes: []string{"apply=true"}, MarkerKey: "m", MarkerValue: "v"}
	right := left
	right.Apply = true
	right.ExcludedPrefixes = []string{"apply=false"}
	if ConfigScopeHash(left) == ConfigScopeHash(right) {
		t.Fatal("ConfigScopeHash() collided between exclusion data and typed apply configuration")
	}
}

func mustAccessKey(t *testing.T, instance, bucket, object string, hour time.Time) string {
	t.Helper()
	key, err := accessKey(instance, bucket, object, hour)
	if err != nil {
		t.Fatalf("accessKey() error = %v", err)
	}
	return key
}

func mustRedisGet(t *testing.T, server *miniredis.Miniredis, key string) string {
	t.Helper()
	value, err := server.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	return value
}

type countingRedisCommands struct {
	redisCommands
	evals int
}

func (c *countingRedisCommands) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	c.evals++
	return c.redisCommands.Eval(ctx, script, keys, args...)
}
