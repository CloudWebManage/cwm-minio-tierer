package tierer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/orihoch/cwm-minio-tierer/internal/contracts"
)

type inventory interface {
	Buckets(context.Context) ([]string, error)
	Objects(context.Context, string, string) <-chan ObjectResult
	Stat(context.Context, Object) (Object, error)
	PlanMarker(context.Context, Object, string, string) (MarkerPlan, error)
	ApplyMarker(context.Context, Object, MarkerPlan) error
	Restore(context.Context, Object, int) (bool, error)
}

type usageReader interface {
	ReadChunk(context.Context, []Object, []time.Time, []time.Time) ([]ObjectUsage, []bool, error)
}

type ObjectUsage struct {
	Low  []int64
	High []int64
}

type stateStore interface {
	LoadCursor(context.Context, string) (*Cursor, error)
	SaveCursor(context.Context, string, Cursor) error
	ResetCursor(context.Context, string) error
	Reserve(context.Context, time.Time, BudgetKind, BudgetLimit, int64) (BudgetReservation, error)
}

type Observer interface {
	ScanStarted()
	ScanFinished(time.Duration, error)
	ObjectHandled(string)
	CoverageSkipped()
	CursorLoaded(time.Time)
	CursorSaved(time.Time)
	CursorReset()
	CursorError()
	BudgetObserved(BudgetKind, BudgetReservation)
	BudgetExhausted(BudgetKind)
	MarkerObserved(MarkerOutcome, error)
	MarkerApplied(MarkerOutcome)
	TransitionState(bool)
	RestoreObserved(Action, bool, error)
	RestoreApplied(Action)
	DependencyFailure(string)
}

type NopObserver struct{}

func (NopObserver) ScanStarted()                                 {}
func (NopObserver) ScanFinished(time.Duration, error)            {}
func (NopObserver) ObjectHandled(string)                         {}
func (NopObserver) CoverageSkipped()                             {}
func (NopObserver) CursorLoaded(time.Time)                       {}
func (NopObserver) CursorSaved(time.Time)                        {}
func (NopObserver) CursorReset()                                 {}
func (NopObserver) CursorError()                                 {}
func (NopObserver) BudgetObserved(BudgetKind, BudgetReservation) {}
func (NopObserver) BudgetExhausted(BudgetKind)                   {}
func (NopObserver) MarkerObserved(MarkerOutcome, error)          {}
func (NopObserver) MarkerApplied(MarkerOutcome)                  {}
func (NopObserver) TransitionState(bool)                         {}
func (NopObserver) RestoreObserved(Action, bool, error)          {}
func (NopObserver) RestoreApplied(Action)                        {}
func (NopObserver) DependencyFailure(string)                     {}

type ScannerConfig struct {
	Apply            bool
	Policy           Policy
	RestoreDays      int
	MarkerKey        string
	MarkerValue      string
	ChunkSize        int
	CompletionDelay  time.Duration
	RetryDelay       time.Duration
	ExcludedBuckets  []string
	ExcludedPrefixes []string
	Scope            string
	TransitionBudget BudgetLimit
	RestoreBudget    BudgetLimit
	OperationTimeout time.Duration
	Logger           *slog.Logger
}

type Scanner struct {
	config    ScannerConfig
	inventory inventory
	usage     usageReader
	state     stateStore
	observer  Observer
	now       func() time.Time
	logger    *slog.Logger
	scanLock  chan struct{}
}

func NewScanner(config ScannerConfig, inventory inventory, usage usageReader, state stateStore, observer Observer, now func() time.Time) *Scanner {
	if observer == nil {
		observer = NopObserver{}
	}
	if now == nil {
		now = time.Now
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 30 * time.Second
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	return &Scanner{config: config, inventory: inventory, usage: usage, state: state, observer: observer, now: now, logger: logger, scanLock: lock}
}

func (s *Scanner) Run(ctx context.Context) error {
	for {
		err := s.ScanOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		delay := s.config.CompletionDelay
		if err != nil {
			delay = s.config.RetryDelay
			s.logger.Error("tierer scan failed; retrying from durable cursor", "operation", "scan", "error", err, "retry_delay", delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Scanner) ScanOnce(ctx context.Context) (scanErr error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.scanLock:
	}
	defer func() { s.scanLock <- struct{}{} }()
	started := time.Now()
	s.observer.ScanStarted()
	defer func() { s.observer.ScanFinished(time.Since(started), scanErr) }()

	if err := s.validate(); err != nil {
		return err
	}
	opCtx, cancel := s.operationContext(ctx)
	cursor, err := s.state.LoadCursor(opCtx, s.config.Scope)
	cancel()
	if err != nil {
		s.observer.CursorError()
		s.dependencyError(ctx, "redis", "cursor_load", "", "", err)
		return fmt.Errorf("load scan cursor: %w", err)
	}
	if cursor != nil {
		s.observer.CursorLoaded(cursor.UpdatedAt)
	}
	opCtx, cancel = s.operationContext(ctx)
	buckets, err := s.inventory.Buckets(opCtx)
	cancel()
	if err != nil {
		s.dependencyError(ctx, "minio", "bucket_list", "", "", err)
		return fmt.Errorf("list scan buckets: %w", err)
	}
	slicesSort(buckets)
	for _, bucket := range buckets {
		if s.excluded(bucket) || (cursor != nil && bucket < cursor.Bucket) {
			continue
		}
		startAfter := ""
		if cursor != nil && bucket == cursor.Bucket {
			startAfter = cursor.Object
		}
		if err := s.scanBucket(ctx, bucket, startAfter); err != nil {
			return err
		}
	}
	opCtx, cancel = s.operationContext(ctx)
	err = s.state.ResetCursor(opCtx, s.config.Scope)
	cancel()
	if err != nil {
		s.observer.CursorError()
		s.dependencyError(ctx, "redis", "cursor_reset", "", "", err)
		return fmt.Errorf("reset completed scan cursor: %w", err)
	}
	s.observer.CursorReset()
	return nil
}

func (s *Scanner) validate() error {
	if s.config.ChunkSize <= 0 || s.config.Policy.LowWindowHours <= 0 || s.config.Policy.HighWindowHours <= 0 || s.config.RestoreDays <= 0 || s.config.MarkerKey == "" || s.config.MarkerValue == "" || s.config.Scope == "" {
		return errors.New("invalid scanner configuration")
	}
	if s.config.Apply && (s.config.TransitionBudget.Attempts < 0 || s.config.TransitionBudget.Bytes < 0 || s.config.RestoreBudget.Attempts < 0 || s.config.RestoreBudget.Bytes < 0) {
		return errors.New("apply scanner budgets must be non-negative")
	}
	if err := validateAggregateAccessKeys(s.config.ChunkSize, s.config.Policy.LowWindowHours, s.config.Policy.HighWindowHours, maxRedisAccessKeysPerChunk); err != nil {
		return err
	}
	return nil
}

func (s *Scanner) excluded(bucket string) bool {
	for _, exact := range s.config.ExcludedBuckets {
		if bucket == exact {
			return true
		}
	}
	for _, prefix := range s.config.ExcludedPrefixes {
		if strings.HasPrefix(bucket, prefix) {
			return true
		}
	}
	return false
}

func (s *Scanner) scanBucket(ctx context.Context, bucket, startAfter string) error {
	chunk := make([]Object, 0, s.config.ChunkSize)
	listingCtx, cancelListing := context.WithCancel(ctx)
	defer cancelListing()
	stream := s.inventory.Objects(listingCtx, bucket, startAfter)
	var inactivity *time.Timer
	var inactivityC <-chan time.Time
	if s.config.OperationTimeout > 0 {
		inactivity = time.NewTimer(s.config.OperationTimeout)
		defer inactivity.Stop()
		inactivityC = inactivity.C
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-inactivityC:
			cancelListing()
			err := fmt.Errorf("MinIO object listing inactive for %s", s.config.OperationTimeout)
			s.dependencyError(ctx, "minio", "object_list", bucket, "", err)
			return err
		case item, open := <-stream:
			stopTimer(inactivity)
			if !open {
				if len(chunk) > 0 {
					return s.processChunk(ctx, chunk)
				}
				return nil
			}
			if item.Err != nil {
				s.dependencyError(ctx, "minio", "object_list", bucket, "", item.Err)
				return item.Err
			}
			if item.Object.Name == "" {
				return errors.New("MinIO listing returned an empty object name")
			}
			if item.Object.Size == 0 && strings.HasSuffix(item.Object.Name, "/") {
				s.observer.ObjectHandled("directory_skipped")
				resetTimer(inactivity, s.config.OperationTimeout)
				continue
			}
			chunk = append(chunk, item.Object)
			if len(chunk) == s.config.ChunkSize {
				if err := s.processChunk(ctx, chunk); err != nil {
					return err
				}
				chunk = chunk[:0]
			}
			resetTimer(inactivity, s.config.OperationTimeout)
		}
	}
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if timer != nil {
		timer.Reset(duration)
	}
}

type evaluatedObject struct {
	object Object
	result Result
}

func (s *Scanner) processChunk(ctx context.Context, objects []Object) error {
	chunkNow := s.now().UTC()
	evaluationHour := chunkNow.Truncate(time.Hour)
	lowHours, err := contracts.HourWindow(evaluationHour, s.config.Policy.LowWindowHours, false)
	if err != nil {
		return err
	}
	highHours, err := contracts.HourWindow(evaluationHour, s.config.Policy.HighWindowHours, s.config.Policy.HighIncludeCurrent)
	if err != nil {
		return err
	}
	evaluated := make([]evaluatedObject, 0, len(objects))
	readStarted := time.Now()
	opCtx, cancel := s.operationContext(ctx)
	chunkUsage, coverage, err := s.usage.ReadChunk(opCtx, objects, lowHours, highHours)
	cancel()
	if err != nil {
		s.dependencyError(ctx, "redis", "usage_read", "", "", err)
		return fmt.Errorf("read chunk usage: %w", err)
	}
	if len(chunkUsage) != len(objects) {
		return errors.New("chunk usage response does not match object count")
	}
	s.logger.Debug("tierer chunk usage read", "operation", "chunk", "objects", len(objects), "evaluation_hour", evaluationHour, "low_window_hours", len(lowHours), "high_window_hours", len(highHours), "coverage_complete", allCovered(coverage), "duration", time.Since(readStarted))
	for i, object := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := Evaluate(Evaluation{Hour: evaluationHour, LastModified: object.LastModified, LowCounts: chunkUsage[i].Low, LowCoverage: coverage, HighCounts: chunkUsage[i].High}, s.config.Policy)
		if err != nil {
			return fmt.Errorf("evaluate %s/%s: %w", object.Bucket, object.Name, err)
		}
		if !result.CoverageComplete {
			s.observer.CoverageSkipped()
		}
		evaluated = append(evaluated, evaluatedObject{object: object, result: result})
	}

	for _, item := range evaluated {
		if err := s.handleObject(ctx, item.object, item.result, chunkNow); err != nil {
			return err
		}
	}
	last := objects[len(objects)-1]
	updated := chunkNow
	opCtx, cancel = s.operationContext(ctx)
	err = s.state.SaveCursor(opCtx, s.config.Scope, Cursor{Bucket: last.Bucket, Object: last.Name, UpdatedAt: updated})
	cancel()
	if err != nil {
		s.observer.CursorError()
		s.dependencyError(ctx, "redis", "cursor_save", last.Bucket, last.Name, err)
		return fmt.Errorf("save scan cursor: %w", err)
	}
	s.observer.CursorSaved(updated)
	return nil
}

func (s *Scanner) handleObject(ctx context.Context, listed Object, result Result, chunkNow time.Time) error {
	state := listed.State
	action := ActionNone
	if listed.StateKnown {
		action = DecideAction(state, result.Low, result.High, chunkNow)
	}
	if !listed.StateKnown && (result.Low || result.High) || action != ActionNone {
		opCtx, cancel := s.operationContext(ctx)
		current, err := s.inventory.Stat(opCtx, listed)
		cancel()
		if err != nil {
			s.dependencyError(ctx, "minio", "stat", listed.Bucket, listed.Name, err)
			s.observer.ObjectHandled("stat_error")
			return nil
		}
		if !SameIdentity(listed, current) {
			s.observer.ObjectHandled("identity_changed")
			return nil
		}
		listed = current
		state = current.State
		action = DecideAction(state, result.Low, result.High, chunkNow)
	}
	if action != ActionNone {
		s.logger.Debug("tierer mutation decision", "operation", "mutation_decision", "bucket", listed.Bucket, "object", listed.Name, "action", action, "low", result.Low, "high", result.High, "transitioned", state.Transitioned, "state_known", listed.StateKnown, "apply", s.config.Apply)
	}
	s.observer.TransitionState(state.Transitioned)
	switch action {
	case ActionNone:
		s.observer.ObjectHandled("no_action")
		return nil
	case ActionMark:
		return s.handleMarker(ctx, listed)
	case ActionRestore, ActionRenew:
		return s.handleRestore(ctx, listed, action)
	default:
		return fmt.Errorf("unsupported action %q", action)
	}
}

func (s *Scanner) handleMarker(ctx context.Context, object Object) error {
	opCtx, cancel := s.operationContext(ctx)
	plan, err := s.inventory.PlanMarker(opCtx, object, s.config.MarkerKey, s.config.MarkerValue)
	cancel()
	if err != nil {
		s.dependencyError(ctx, "minio", "marker_plan", object.Bucket, object.Name, err)
		s.observer.MarkerObserved("", err)
		s.observer.ObjectHandled("marker_error")
		return nil
	}
	s.observer.MarkerObserved(plan.Outcome, nil)
	if !plan.Required {
		s.observer.ObjectHandled("marker_" + string(plan.Outcome))
		return nil
	}
	if !s.config.Apply {
		s.observer.ObjectHandled("audit_mark")
		return nil
	}
	opCtx, cancel = s.operationContext(ctx)
	reservation, err := s.state.Reserve(opCtx, s.now().UTC(), BudgetTransition, s.config.TransitionBudget, object.Size)
	cancel()
	if err != nil {
		s.dependencyError(ctx, "redis", "transition_budget", object.Bucket, object.Name, err)
		return err
	}
	s.observer.BudgetObserved(BudgetTransition, reservation)
	s.logger.Debug("tierer budget decision", "operation", "budget_decision", "bucket", object.Bucket, "object", object.Name, "kind", BudgetTransition, "allowed", reservation.Allowed, "used_attempts", reservation.UsedAttempts, "used_bytes", reservation.UsedBytes, "limit_attempts", s.config.TransitionBudget.Attempts, "limit_bytes", s.config.TransitionBudget.Bytes)
	if !reservation.Allowed {
		s.observer.BudgetExhausted(BudgetTransition)
		s.observer.ObjectHandled("transition_budget_exhausted")
		return nil
	}
	opCtx, cancel = s.operationContext(ctx)
	err = s.inventory.ApplyMarker(opCtx, object, plan)
	cancel()
	if err != nil {
		s.dependencyError(ctx, "minio", "marker_apply", object.Bucket, object.Name, err)
		s.observer.MarkerObserved(plan.Outcome, err)
		s.observer.ObjectHandled("marker_error")
		return nil
	}
	s.observer.MarkerApplied(plan.Outcome)
	s.observer.ObjectHandled("marked")
	return nil
}

func (s *Scanner) handleRestore(ctx context.Context, object Object, action Action) error {
	if !s.config.Apply {
		s.observer.RestoreObserved(action, false, nil)
		s.observer.ObjectHandled("audit_" + string(action))
		return nil
	}
	bytes := object.Size
	if action == ActionRenew {
		bytes = 0
	}
	opCtx, cancel := s.operationContext(ctx)
	reservation, err := s.state.Reserve(opCtx, s.now().UTC(), BudgetRestore, s.config.RestoreBudget, bytes)
	cancel()
	if err != nil {
		s.dependencyError(ctx, "redis", "restore_budget", object.Bucket, object.Name, err)
		return err
	}
	s.observer.BudgetObserved(BudgetRestore, reservation)
	s.logger.Debug("tierer budget decision", "operation", "budget_decision", "bucket", object.Bucket, "object", object.Name, "kind", BudgetRestore, "allowed", reservation.Allowed, "used_attempts", reservation.UsedAttempts, "used_bytes", reservation.UsedBytes, "limit_attempts", s.config.RestoreBudget.Attempts, "limit_bytes", s.config.RestoreBudget.Bytes)
	if !reservation.Allowed {
		s.observer.BudgetExhausted(BudgetRestore)
		s.observer.ObjectHandled("restore_budget_exhausted")
		return nil
	}
	opCtx, cancel = s.operationContext(ctx)
	pending, err := s.inventory.Restore(opCtx, object, s.config.RestoreDays)
	cancel()
	s.observer.RestoreObserved(action, pending, err)
	if err != nil {
		s.dependencyError(ctx, "minio", "restore", object.Bucket, object.Name, err)
		s.observer.ObjectHandled("restore_error")
		return nil
	}
	if pending {
		s.observer.ObjectHandled("restore_pending")
	} else {
		s.observer.RestoreApplied(action)
		s.observer.ObjectHandled(string(action))
	}
	return nil
}

func allCovered(coverage []bool) bool {
	for _, covered := range coverage {
		if !covered {
			return false
		}
	}
	return true
}

func (s *Scanner) dependencyError(ctx context.Context, dependency, operation, bucket, object string, err error) {
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return
	}
	s.observer.DependencyFailure(dependency)
	attributes := []any{"dependency", dependency, "operation", operation, "error", err}
	if bucket != "" {
		attributes = append(attributes, "bucket", bucket)
	}
	if object != "" {
		attributes = append(attributes, "object", object)
	}
	s.logger.Error("tierer dependency operation failed", attributes...)
}

func (s *Scanner) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if s.config.OperationTimeout > 0 {
		return context.WithTimeout(parent, s.config.OperationTimeout)
	}
	return context.WithCancel(parent)
}
