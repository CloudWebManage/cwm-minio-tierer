package tierer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func TestScannerAuditUsesBoundedChunkHoursExclusionsAndNoMutations(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 8, 25, 15, 59, 0, 0, time.UTC)
	clock := &stepClock{times: []time.Time{base, base.Add(2 * time.Hour), base.Add(3 * time.Hour)}}
	objects := map[string][]Object{
		"data": {
			{Bucket: "data", Name: "dir/", Size: 0},
			{Bucket: "data", Name: "one", ETag: "1", Size: 1, LastModified: base.Add(-10 * time.Hour), StateKnown: true},
			{Bucket: "data", Name: "two", ETag: "2", Size: 2, LastModified: base.Add(-10 * time.Hour), StateKnown: true},
			{Bucket: "data", Name: "three", ETag: "3", Size: 3, LastModified: base.Add(-10 * time.Hour), StateKnown: true},
		},
		"skip-exact": {{Bucket: "skip-exact", Name: "x"}},
		"tmp-bucket": {{Bucket: "tmp-bucket", Name: "x"}},
	}
	minio := newFakeInventory(objects)
	usage := &fakeUsage{low: []int64{0, 0}, coverage: []bool{true, true}, high: []int64{0}}
	state := &fakeStateStore{}
	scanner := NewScanner(ScannerConfig{
		Policy:      Policy{LowThreshold: 1, LowWindowHours: 2, HighThreshold: 5, HighWindowHours: 1},
		RestoreDays: 7, MarkerKey: "marker", MarkerValue: "yes", ChunkSize: 2,
		ExcludedBuckets: []string{"skip-exact"}, ExcludedPrefixes: []string{"tmp-"}, Scope: "scope",
	}, minio, usage, state, NopObserver{}, clock.Now)
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if minio.planCalls != 3 || minio.applyCalls != 0 || len(state.reservations) != 0 {
		t.Fatalf("audit calls: plans=%d applies=%d reservations=%d", minio.planCalls, minio.applyCalls, len(state.reservations))
	}
	if usage.chunkCalls != 2 {
		t.Fatalf("usage chunk calls = %d, want 2", usage.chunkCalls)
	}
	if len(usage.firstHours) != 2 || !usage.firstHours[0].Equal(base.Truncate(time.Hour).Add(-2*time.Hour)) || !usage.firstHours[1].Equal(base.Add(2*time.Hour).Truncate(time.Hour).Add(-2*time.Hour)) {
		t.Fatalf("chunk evaluation hours = %v", usage.firstHours)
	}
	if len(state.saved) != 2 || state.saved[0].Object != "two" || state.saved[1].Object != "three" || state.resets != 1 {
		t.Fatalf("cursor saves=%+v resets=%d", state.saved, state.resets)
	}
}

func TestScannerApplyReservesBeforeMarkerAndChargesRestoreRenewalZeroBytes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	objects := map[string][]Object{"data": {
		{Bucket: "data", Name: "local", ETag: "1", Size: 11, LastModified: now.Add(-10 * time.Hour), StateKnown: true},
		{Bucket: "data", Name: "restore", ETag: "2", Size: 22, LastModified: now.Add(-10 * time.Hour), StateKnown: true, State: ObjectState{Transitioned: true}},
		{Bucket: "data", Name: "renew", ETag: "3", Size: 33, LastModified: now.Add(-10 * time.Hour), StateKnown: true, State: ObjectState{Transitioned: true, Restore: &RestoreState{Expires: now.Add(time.Hour)}}},
	}}
	minio := newFakeInventory(objects)
	usage := &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{2}}
	state := &fakeStateStore{allowed: true, events: &minio.events}
	scanner := NewScanner(ScannerConfig{
		Apply: true, Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1},
		RestoreDays: 7, MarkerKey: "marker", MarkerValue: "yes", ChunkSize: 10, Scope: "scope",
		TransitionBudget: BudgetLimit{Attempts: 10, Bytes: 100}, RestoreBudget: BudgetLimit{Attempts: 10, Bytes: 100},
	}, minio, usage, state, NopObserver{}, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if minio.applyCalls != 1 || minio.restoreCalls != 2 {
		t.Fatalf("mutation calls: marker=%d restore=%d", minio.applyCalls, minio.restoreCalls)
	}
	if len(state.reservations) != 3 || state.reservations[0].kind != BudgetTransition || state.reservations[0].bytes != 11 || state.reservations[1].kind != BudgetRestore || state.reservations[1].bytes != 22 || state.reservations[2].bytes != 0 {
		t.Fatalf("reservations = %+v", state.reservations)
	}
	wantOrder := []string{"reserve:transition", "marker", "reserve:restore", "restore", "reserve:restore", "restore"}
	if len(minio.events) != len(wantOrder) {
		t.Fatalf("events = %v", minio.events)
	}
	for i := range wantOrder {
		if minio.events[i] != wantOrder[i] {
			t.Fatalf("events = %v, want %v", minio.events, wantOrder)
		}
	}
}

func TestScannerDebugLogsChunkAndMutationDecision(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	object := Object{Bucket: "data", Name: "one", ETag: "1", Size: 11, LastModified: now.Add(-2 * time.Hour), StateKnown: true}
	minio := newFakeInventory(map[string][]Object{"data": {object}})
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", Logger: logger}, minio, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, &fakeStateStore{}, NopObserver{}, func() time.Time { return now })

	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	text := logs.String()
	if !strings.Contains(text, `"operation":"chunk"`) || !strings.Contains(text, `"objects":1`) || !strings.Contains(text, `"coverage_complete":true`) {
		t.Fatalf("chunk debug log = %s", text)
	}
	if !strings.Contains(text, `"operation":"mutation_decision"`) || !strings.Contains(text, `"action":"mark"`) || !strings.Contains(text, `"apply":false`) {
		t.Fatalf("mutation decision debug log = %s", text)
	}
}

func TestScannerSkipsChangedIdentityBeforeActionWithoutBudget(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	listed := Object{Bucket: "data", Name: "one", ETag: "listed", Size: 5, LastModified: now.Add(-2 * time.Hour), StateKnown: true}
	minio := newFakeInventory(map[string][]Object{"data": {listed}})
	minio.stats["data/one"] = Object{Bucket: "data", Name: "one", ETag: "changed", Size: 5, LastModified: listed.LastModified, StateKnown: true}
	state := &fakeStateStore{allowed: true}
	scanner := NewScanner(ScannerConfig{Apply: true, Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", TransitionBudget: BudgetLimit{Attempts: 1, Bytes: 10}, RestoreBudget: BudgetLimit{Attempts: 1, Bytes: 10}}, minio, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, state, NopObserver{}, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if minio.planCalls != 0 || minio.applyCalls != 0 || len(state.reservations) != 0 {
		t.Fatalf("changed object acted on: plans=%d applies=%d reservations=%v", minio.planCalls, minio.applyCalls, state.reservations)
	}
}

func TestScannerChargesBudgetToMutationTimeUTCDayNotEvaluationHour(t *testing.T) {
	t.Parallel()
	evaluationTime := time.Date(2026, 8, 25, 23, 59, 0, 0, time.UTC)
	mutationTime := time.Date(2026, 8, 26, 0, 1, 0, 0, time.UTC)
	object := Object{Bucket: "data", Name: "one", ETag: "1", Size: 5, LastModified: evaluationTime.Add(-2 * time.Hour), StateKnown: true}
	minio := newFakeInventory(map[string][]Object{"data": {object}})
	state := &fakeStateStore{allowed: true}
	clock := &stepClock{times: []time.Time{evaluationTime, mutationTime}}
	scanner := NewScanner(ScannerConfig{Apply: true, Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", TransitionBudget: BudgetLimit{Attempts: 1, Bytes: 10}, RestoreBudget: BudgetLimit{Attempts: 1, Bytes: 10}}, minio, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, state, NopObserver{}, clock.Now)
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if len(state.reservations) != 1 || !state.reservations[0].at.Equal(mutationTime) {
		t.Fatalf("reservation = %+v, want mutation time %s", state.reservations, mutationTime)
	}
}

func TestScannerClassifiesRestoreExpiryAtActualChunkTimeAndChargesInitialBytes(t *testing.T) {
	t.Parallel()
	chunkNow := time.Date(2026, 8, 25, 15, 45, 0, 0, time.UTC)
	object := Object{Bucket: "data", Name: "one", ETag: "1", Size: 17, LastModified: chunkNow.Add(-2 * time.Hour), StateKnown: true, State: ObjectState{Transitioned: true, Restore: &RestoreState{Expires: time.Date(2026, 8, 25, 15, 30, 0, 0, time.UTC)}}}
	minio := newFakeInventory(map[string][]Object{"data": {object}})
	state := &fakeStateStore{allowed: true}
	scanner := NewScanner(ScannerConfig{Apply: true, Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", TransitionBudget: BudgetLimit{Attempts: 1, Bytes: 100}, RestoreBudget: BudgetLimit{Attempts: 1, Bytes: 100}}, minio, &fakeUsage{low: []int64{1}, coverage: []bool{true}, high: []int64{2}}, state, NopObserver{}, func() time.Time { return chunkNow })
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if len(state.reservations) != 1 || state.reservations[0].kind != BudgetRestore || state.reservations[0].bytes != 17 {
		t.Fatalf("restore reservation = %+v, want initial restore bytes 17", state.reservations)
	}
}

func TestScannerFatalReadDoesNotAdvanceAffectedChunkButObjectMutationErrorDoes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	objects := map[string][]Object{"data": {{Bucket: "data", Name: "one", ETag: "1", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true}}}
	minio := newFakeInventory(objects)
	state := &fakeStateStore{}
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope"}, minio, &fakeUsage{err: errors.New("redis unavailable")}, state, NopObserver{}, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err == nil || len(state.saved) != 0 || state.resets != 0 {
		t.Fatalf("fatal ScanOnce() error=%v saved=%v resets=%d", err, state.saved, state.resets)
	}

	minio = newFakeInventory(objects)
	minio.planErr = errors.New("tag read failed")
	state = &fakeStateStore{}
	scanner = NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope"}, minio, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, state, NopObserver{}, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err != nil || len(state.saved) != 1 || state.resets != 1 {
		t.Fatalf("isolated ScanOnce() error=%v saved=%v resets=%d", err, state.saved, state.resets)
	}
}

func TestScannerRunNeverOverlapsAndCompletionDelayHonorsShutdown(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	minio := newFakeInventory(map[string][]Object{})
	state := &fakeStateStore{}
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", CompletionDelay: time.Hour}, minio, &fakeUsage{}, state, NopObserver{}, func() time.Time { return now })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scanner.Run(ctx) }()
	for i := 0; i < 100 && state.resetCount() == 0; i++ {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop during completion delay")
	}
	if minio.maxActive != 1 {
		t.Fatalf("maximum concurrent scans = %d, want 1", minio.maxActive)
	}
}

func TestScannerRunRetriesFatalScanFromDurableCursorUntilCanceled(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	minio := newFakeInventory(map[string][]Object{"data": {{Bucket: "data", Name: "two", ETag: "2", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true}}})
	usage := &fakeUsage{low: []int64{1}, coverage: []bool{true}, high: []int64{0}, failures: 1}
	state := &fakeStateStore{cursor: &Cursor{Bucket: "data", Object: "one", UpdatedAt: now}}
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", RetryDelay: time.Millisecond, CompletionDelay: time.Hour}, minio, usage, state, NopObserver{}, func() time.Time { return now })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scanner.Run(ctx) }()
	for i := 0; i < 1000 && state.resetCount() == 0; i++ {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	if usage.chunkCalls < 2 || len(minio.startAfters) < 2 || minio.startAfters[0] != "one" || minio.startAfters[1] != "one" {
		t.Fatalf("retry calls=%d startAfters=%v, want durable cursor one on both attempts", usage.chunkCalls, minio.startAfters)
	}
}

func TestScannerRunRetriesStalledListingFromDurableCursor(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	objects := []Object{
		{Bucket: "data", Name: "one", ETag: "1", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true},
		{Bucket: "data", Name: "two", ETag: "2", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true},
	}
	base := newFakeInventory(map[string][]Object{"data": objects})
	inventory := &scriptedListingInventory{fakeInventory: base}
	inventory.stream = func(ctx context.Context, call int, bucket string) <-chan ObjectResult {
		stream := make(chan ObjectResult)
		go func() {
			defer close(stream)
			if call == 1 {
				stream <- ObjectResult{Object: objects[0]}
				<-ctx.Done()
				return
			}
			for _, object := range objects {
				select {
				case stream <- ObjectResult{Object: object}:
				case <-ctx.Done():
					return
				}
			}
		}()
		return stream
	}
	state := &fakeStateStore{cursor: &Cursor{Bucket: "data", Object: "before", UpdatedAt: now}}
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 2, Scope: "scope", OperationTimeout: 20 * time.Millisecond, RetryDelay: time.Millisecond, CompletionDelay: time.Hour}, inventory, &fakeUsage{low: []int64{1}, coverage: []bool{true}, high: []int64{0}}, state, NopObserver{}, func() time.Time { return now })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scanner.Run(ctx) }()
	for i := 0; i < 1000 && state.resetCount() == 0; i++ {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	if inventory.calls < 2 || len(inventory.startAfters) < 2 || inventory.startAfters[0] != "before" || inventory.startAfters[1] != "before" {
		t.Fatalf("listing calls=%d startAfters=%v, want retry from durable cursor", inventory.calls, inventory.startAfters)
	}
}

func TestScannerListingInactivityTimeoutResetsOnProgress(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	objects := []Object{
		{Bucket: "data", Name: "one", ETag: "1", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true},
		{Bucket: "data", Name: "two", ETag: "2", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true},
		{Bucket: "data", Name: "three", ETag: "3", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true},
	}
	inventory := &scriptedListingInventory{fakeInventory: newFakeInventory(map[string][]Object{"data": objects})}
	inventory.stream = func(ctx context.Context, _ int, _ string) <-chan ObjectResult {
		stream := make(chan ObjectResult)
		go func() {
			defer close(stream)
			for _, object := range objects {
				timer := time.NewTimer(15 * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
				select {
				case stream <- ObjectResult{Object: object}:
				case <-ctx.Done():
					return
				}
			}
		}()
		return stream
	}
	state := &fakeStateStore{}
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 3, Scope: "scope", OperationTimeout: 20 * time.Millisecond}, inventory, &fakeUsage{low: []int64{1}, coverage: []bool{true}, high: []int64{0}}, state, NopObserver{}, func() time.Time { return now })
	started := time.Now()
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond || state.resets != 1 {
		t.Fatalf("healthy progressive listing elapsed=%s resets=%d", elapsed, state.resets)
	}
}

func TestScannerLogsObjectErrorsAndCountsRedisFailuresWithoutIdentityMetricLabels(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	object := Object{Bucket: "data", Name: "one", ETag: "1", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true}
	minio := newFakeInventory(map[string][]Object{"data": {object}})
	minio.planErr = errors.New("tag unavailable")
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", Logger: logger}, minio, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, &fakeStateStore{}, metrics, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce(object error) = %v", err)
	}
	if text := logs.String(); !strings.Contains(text, `"bucket":"data"`) || !strings.Contains(text, `"object":"one"`) || !strings.Contains(text, `"operation":"marker_plan"`) {
		t.Fatalf("structured object error log = %s", text)
	}

	logs.Reset()
	scanner = NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", Logger: logger}, newFakeInventory(map[string][]Object{"data": {object}}), &fakeUsage{err: errors.New("redis down")}, &fakeStateStore{}, metrics, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err == nil {
		t.Fatal("ScanOnce(redis error) = nil")
	}
	text := gatherText(t, metrics)
	if !strings.Contains(text, `cwm_minio_tierer_dependency_failures_total{dependency="redis"} 1`) || strings.Contains(text, "bucket=") || strings.Contains(text, "object=") {
		t.Fatalf("dependency metrics = %s", text)
	}
}

func TestScannerBoundsObjectOperationTimeoutWithoutCancelingScan(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	object := Object{Bucket: "data", Name: "one", ETag: "1", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true}
	minio := newFakeInventory(map[string][]Object{"data": {object}})
	minio.blockStat = true
	state := &fakeStateStore{}
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope", OperationTimeout: 20 * time.Millisecond}, minio, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, state, NopObserver{}, func() time.Time { return now })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- scanner.ScanOnce(ctx) }()
	select {
	case err := <-done:
		if err != nil || len(state.saved) != 1 || state.resets != 1 {
			t.Fatalf("ScanOnce() error=%v saved=%v resets=%d", err, state.saved, state.resets)
		}
	case <-time.After(200 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("ScanOnce() did not enforce object operation timeout")
	}
}

func TestScannerUsesFreshStatVersionForExactActionWhenListingVersionUnavailable(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	listed := Object{Bucket: "data", Name: "one", ETag: "same", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true}
	minio := newFakeInventory(map[string][]Object{"data": {listed}})
	current := listed
	current.VersionID = "fresh-v2"
	minio.stats["data/one"] = current
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope"}, minio, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, &fakeStateStore{}, NopObserver{}, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if len(minio.planVersions) != 1 || minio.planVersions[0] != "fresh-v2" {
		t.Fatalf("marker plan versions = %v, want fresh-v2", minio.planVersions)
	}
}

func TestScannerRealRedisBudgetTTLWrongTypeAndExhaustion(t *testing.T) {
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	newCase := func(t *testing.T) (*miniredis.Miniredis, *RedisStore, *fakeInventory, ScannerConfig) {
		server := miniredis.RunT(t)
		server.SetTime(now)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		store := NewRedisStore(NewRedisClient(client), "site-a", "coverage:2006:01:02:15", "complete", true)
		object := Object{Bucket: "data", Name: "one", ETag: "1", Size: 5, LastModified: now.Add(-2 * time.Hour), StateKnown: true}
		inventory := newFakeInventory(map[string][]Object{"data": {object}})
		config := ScannerConfig{Apply: true, Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: ScopeHash(nil, nil, "m", "v"), TransitionBudget: BudgetLimit{Attempts: 1, Bytes: 10}, RestoreBudget: BudgetLimit{Attempts: 1, Bytes: 10}}
		return server, store, inventory, config
	}
	t.Run("reservation TTL", func(t *testing.T) {
		server, store, inventory, config := newCase(t)
		scanner := NewScanner(config, inventory, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, store, NopObserver{}, func() time.Time { return now })
		if err := scanner.ScanOnce(context.Background()); err != nil {
			t.Fatalf("ScanOnce() error = %v", err)
		}
		key := "cwm-minio-tierer:v1:site-a:budget:2026:08:25:transition-attempts"
		if ttl := server.TTL(key); ttl <= 0 || inventory.applyCalls != 1 {
			t.Fatalf("budget TTL=%s apply calls=%d", ttl, inventory.applyCalls)
		}
	})
	t.Run("wrong type fails chunk", func(t *testing.T) {
		server, store, inventory, config := newCase(t)
		key := "cwm-minio-tierer:v1:site-a:budget:2026:08:25:transition-attempts"
		if _, err := server.Push(key, "bad"); err != nil {
			t.Fatalf("Push() error = %v", err)
		}
		scanner := NewScanner(config, inventory, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, store, NopObserver{}, func() time.Time { return now })
		if err := scanner.ScanOnce(context.Background()); err == nil || inventory.applyCalls != 0 {
			t.Fatalf("ScanOnce() error=%v apply calls=%d", err, inventory.applyCalls)
		}
	})
	t.Run("exhaustion advances without action", func(t *testing.T) {
		server, store, inventory, config := newCase(t)
		server.Set("cwm-minio-tierer:v1:site-a:budget:2026:08:25:transition-attempts", "1")
		scanner := NewScanner(config, inventory, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, store, NopObserver{}, func() time.Time { return now })
		if err := scanner.ScanOnce(context.Background()); err != nil || inventory.applyCalls != 0 {
			t.Fatalf("ScanOnce() error=%v apply calls=%d", err, inventory.applyCalls)
		}
	})
}

func TestScannerActionErrorAdvancesDurableCursorBeforeLaterListingFailureAndResume(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	object := Object{Bucket: "data", Name: "one", ETag: "1", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true}
	inventory := newFakeInventory(map[string][]Object{"data": {object}})
	inventory.planErr = errors.New("isolated tag error")
	inventory.listErr = errors.New("later listing failure")
	state := &fakeStateStore{}
	config := ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope"}
	scanner := NewScanner(config, inventory, &fakeUsage{low: []int64{0}, coverage: []bool{true}, high: []int64{0}}, state, NopObserver{}, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err == nil || state.cursor == nil || state.cursor.Object != "one" {
		t.Fatalf("first ScanOnce() error=%v durable cursor=%+v", err, state.cursor)
	}
	inventory.listErr = nil
	inventory.planErr = nil
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("resumed ScanOnce() error=%v", err)
	}
	if got := inventory.startAfters[len(inventory.startAfters)-1]; got != "one" {
		t.Fatalf("resume startAfter=%q, want one", got)
	}
}

func TestScannerCursorFailuresPreserveLastDurablePositionForResume(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	object := Object{Bucket: "data", Name: "two", ETag: "2", Size: 1, LastModified: now.Add(-2 * time.Hour), StateKnown: true}
	inventory := newFakeInventory(map[string][]Object{"data": {object}})
	durable := &Cursor{Bucket: "data", Object: "one", UpdatedAt: now}
	state := &fakeStateStore{cursor: durable, loadErr: errors.New("cursor read failed")}
	config := ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope"}
	scanner := NewScanner(config, inventory, &fakeUsage{low: []int64{1}, coverage: []bool{true}, high: []int64{0}}, state, NopObserver{}, func() time.Time { return now })
	if err := scanner.ScanOnce(context.Background()); err == nil || len(inventory.startAfters) != 0 {
		t.Fatalf("cursor load failure error=%v startAfters=%v", err, inventory.startAfters)
	}
	state.loadErr = nil
	state.saveErr = errors.New("cursor write failed")
	if err := scanner.ScanOnce(context.Background()); err == nil || state.cursor == nil || state.cursor.Object != "one" {
		t.Fatalf("cursor save failure error=%v durable=%+v", err, state.cursor)
	}
	state.saveErr = nil
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("resumed ScanOnce() error=%v", err)
	}
	if len(inventory.startAfters) < 2 || inventory.startAfters[0] != "one" || inventory.startAfters[1] != "one" {
		t.Fatalf("resume startAfters=%v, want durable one", inventory.startAfters)
	}
}

func TestScannerInitializesCursorAgeFromResumeAndResetsAtCompletion(t *testing.T) {
	t.Parallel()

	updated := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	now := updated.Add(10 * time.Second)
	registry := prometheus.NewRegistry()
	metrics := newMetrics(registry, func() time.Time { return now })
	object := Object{Bucket: "data", Name: "two", ETag: "2", Size: 1, LastModified: updated.Add(-2 * time.Hour), StateKnown: true}
	inventory := newFakeInventory(map[string][]Object{"data": {object}})
	usage := &fakeUsage{low: []int64{1}, coverage: []bool{true}, high: []int64{0}, err: errors.New("redis unavailable")}
	state := &fakeStateStore{cursor: &Cursor{Bucket: "data", Object: "one", UpdatedAt: updated}}
	scanner := NewScanner(ScannerConfig{Policy: Policy{LowThreshold: 1, LowWindowHours: 1, HighThreshold: 1, HighWindowHours: 1}, RestoreDays: 1, MarkerKey: "m", MarkerValue: "v", ChunkSize: 1, Scope: "scope"}, inventory, usage, state, metrics, func() time.Time { return now })

	if err := scanner.ScanOnce(context.Background()); err == nil {
		t.Fatal("resumed ScanOnce() error = nil")
	}
	if got := metricValue(t, registry, "cwm_minio_tierer_cursor_age_seconds"); got != 10 {
		t.Fatalf("resumed cursor age = %v, want 10", got)
	}

	now = now.Add(5 * time.Second)
	usage.err = nil
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("completed ScanOnce() error = %v", err)
	}
	if got := metricValue(t, registry, "cwm_minio_tierer_cursor_age_seconds"); got != 0 {
		t.Fatalf("completed traversal cursor age = %v, want 0", got)
	}
}

type stepClock struct {
	mu    sync.Mutex
	times []time.Time
	index int
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.index >= len(c.times) {
		return c.times[len(c.times)-1]
	}
	value := c.times[c.index]
	c.index++
	return value
}

type fakeUsage struct {
	low        []int64
	high       []int64
	coverage   []bool
	err        error
	calls      int
	firstHours []time.Time
	chunkCalls int
	failures   int
}

func (f *fakeUsage) ReadChunk(_ context.Context, objects []Object, lowHours, _ []time.Time) ([]ObjectUsage, []bool, error) {
	f.chunkCalls++
	if len(lowHours) > 0 {
		f.firstHours = append(f.firstHours, lowHours[0])
	}
	if f.failures > 0 {
		f.failures--
		return nil, nil, errors.New("temporary Redis failure")
	}
	if f.err != nil {
		return nil, nil, f.err
	}
	result := make([]ObjectUsage, len(objects))
	for i := range result {
		result[i] = ObjectUsage{Low: append([]int64(nil), f.low...), High: append([]int64(nil), f.high...)}
	}
	return result, append([]bool(nil), f.coverage...), nil
}

func (f *fakeUsage) ReadCounts(_ context.Context, _, _ string, hours []time.Time) ([]int64, error) {
	f.calls++
	if len(hours) > 0 {
		f.firstHours = append(f.firstHours, hours[0])
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.calls%2 == 1 {
		return append([]int64(nil), f.low...), nil
	}
	return append([]int64(nil), f.high...), nil
}

func (f *fakeUsage) ReadCoverage(context.Context, []time.Time) ([]bool, error) {
	if f.err != nil {
		return nil, f.err
	}
	return append([]bool(nil), f.coverage...), nil
}

type reservationCall struct {
	kind  BudgetKind
	bytes int64
	at    time.Time
}

type fakeStateStore struct {
	mu           sync.Mutex
	cursor       *Cursor
	loadErr      error
	saveErr      error
	reserveErr   error
	allowed      bool
	saved        []Cursor
	resets       int
	reservations []reservationCall
	events       *[]string
}

func (f *fakeStateStore) LoadCursor(context.Context, string) (*Cursor, error) {
	return f.cursor, f.loadErr
}
func (f *fakeStateStore) SaveCursor(_ context.Context, _ string, cursor Cursor) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saved = append(f.saved, cursor)
	f.cursor = &cursor
	return nil
}
func (f *fakeStateStore) ResetCursor(context.Context, string) error {
	f.mu.Lock()
	f.resets++
	f.cursor = nil
	f.mu.Unlock()
	return nil
}
func (f *fakeStateStore) Reserve(_ context.Context, at time.Time, kind BudgetKind, _ BudgetLimit, bytes int64) (BudgetReservation, error) {
	f.reservations = append(f.reservations, reservationCall{kind: kind, bytes: bytes, at: at})
	if f.events != nil {
		*f.events = append(*f.events, "reserve:"+string(kind))
	}
	return BudgetReservation{Allowed: f.allowed}, f.reserveErr
}
func (f *fakeStateStore) resetCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.resets }

type fakeInventory struct {
	objects      map[string][]Object
	stats        map[string]Object
	planErr      error
	applyErr     error
	restoreErr   error
	planCalls    int
	applyCalls   int
	restoreCalls int
	events       []string
	mu           sync.Mutex
	active       int
	maxActive    int
	startAfters  []string
	blockStat    bool
	listErr      error
	planVersions []string
}

type scriptedListingInventory struct {
	*fakeInventory
	mu          sync.Mutex
	calls       int
	startAfters []string
	stream      func(context.Context, int, string) <-chan ObjectResult
}

func (f *scriptedListingInventory) Objects(ctx context.Context, bucket, startAfter string) <-chan ObjectResult {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.startAfters = append(f.startAfters, startAfter)
	f.mu.Unlock()
	return f.stream(ctx, call, bucket)
}

func newFakeInventory(objects map[string][]Object) *fakeInventory {
	stats := map[string]Object{}
	for bucket, values := range objects {
		for _, object := range values {
			stats[bucket+"/"+object.Name] = object
		}
	}
	return &fakeInventory{objects: objects, stats: stats}
}

func (f *fakeInventory) Buckets(context.Context) ([]string, error) {
	f.mu.Lock()
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.mu.Unlock()
	result := make([]string, 0, len(f.objects))
	for bucket := range f.objects {
		result = append(result, bucket)
	}
	slicesSort(result)
	return result, nil
}
func (f *fakeInventory) Objects(_ context.Context, bucket, startAfter string) <-chan ObjectResult {
	f.startAfters = append(f.startAfters, startAfter)
	stream := make(chan ObjectResult, len(f.objects[bucket])+1)
	for _, object := range f.objects[bucket] {
		if object.Name > startAfter {
			stream <- ObjectResult{Object: object}
		}
	}
	if f.listErr != nil {
		stream <- ObjectResult{Err: f.listErr}
	}
	close(stream)
	f.mu.Lock()
	f.active--
	f.mu.Unlock()
	return stream
}
func (f *fakeInventory) Stat(ctx context.Context, object Object) (Object, error) {
	if f.blockStat {
		<-ctx.Done()
		return Object{}, ctx.Err()
	}
	return f.stats[object.Bucket+"/"+object.Name], nil
}
func (f *fakeInventory) PlanMarker(_ context.Context, object Object, _, _ string) (MarkerPlan, error) {
	f.planCalls++
	f.planVersions = append(f.planVersions, object.VersionID)
	return MarkerPlan{Required: true, Outcome: MarkerAdded, Tags: map[string]string{"m": "v"}}, f.planErr
}
func (f *fakeInventory) ApplyMarker(context.Context, Object, MarkerPlan) error {
	f.applyCalls++
	f.events = append(f.events, "marker")
	return f.applyErr
}
func (f *fakeInventory) Restore(context.Context, Object, int) (bool, error) {
	f.restoreCalls++
	f.events = append(f.events, "restore")
	return false, f.restoreErr
}
