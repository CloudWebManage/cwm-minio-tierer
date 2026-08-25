package tierer

import (
	"testing"
	"time"
)

func TestEvaluateUsesCompletedCoveredLowHoursAndConfigurableHighCurrentHour(t *testing.T) {
	t.Parallel()
	h := time.Date(2026, 8, 25, 15, 37, 0, 0, time.UTC)
	objectTime := time.Date(2026, 8, 25, 11, 59, 0, 0, time.UTC)

	result, err := Evaluate(Evaluation{
		Hour: h, LastModified: objectTime,
		LowCounts: []int64{1, 0, 1}, LowCoverage: []bool{true, true, true},
		HighCounts: []int64{1, 2},
	}, Policy{LowThreshold: 3, LowWindowHours: 3, HighThreshold: 2, HighWindowHours: 2})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if !result.Low || !result.High {
		t.Fatalf("Evaluate() = %+v, want low and high", result)
	}

	result, err = Evaluate(Evaluation{
		Hour: h, LastModified: objectTime,
		LowCounts: []int64{0, 0, 0}, LowCoverage: []bool{true, true, true},
		HighCounts: []int64{0, 3},
	}, Policy{LowThreshold: 3, LowWindowHours: 3, HighThreshold: 2, HighWindowHours: 2, HighIncludeCurrent: true})
	if err != nil || !result.High {
		t.Fatalf("Evaluate(include current) = %+v, %v, want high", result, err)
	}
}

func TestEvaluateEnforcesCoverageAgeAndStrictThresholds(t *testing.T) {
	t.Parallel()
	h := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   Evaluation
		want Result
	}{
		{name: "missing coverage", in: Evaluation{Hour: h, LastModified: h.Add(-3 * time.Hour), LowCounts: []int64{0, 0, 0}, LowCoverage: []bool{true, false, true}, HighCounts: []int64{0, 0}}, want: Result{CoverageComplete: false}},
		{name: "low equality is not low", in: Evaluation{Hour: h, LastModified: h.Add(-3 * time.Hour), LowCounts: []int64{1, 1, 1}, LowCoverage: []bool{true, true, true}, HighCounts: []int64{0, 0}}, want: Result{CoverageComplete: true}},
		{name: "age equality is eligible", in: Evaluation{Hour: h, LastModified: h.Add(-3 * time.Hour), LowCounts: []int64{0, 0, 0}, LowCoverage: []bool{true, true, true}, HighCounts: []int64{0, 0}}, want: Result{Low: true, CoverageComplete: true}},
		{name: "newer object is not low", in: Evaluation{Hour: h, LastModified: h.Add(-3*time.Hour + time.Nanosecond), LowCounts: []int64{0, 0, 0}, LowCoverage: []bool{true, true, true}, HighCounts: []int64{0, 0}}, want: Result{CoverageComplete: true}},
		{name: "high equality is not high", in: Evaluation{Hour: h, LastModified: h.Add(-3 * time.Hour), LowCounts: []int64{3, 0, 0}, LowCoverage: []bool{true, true, true}, HighCounts: []int64{1, 1}}, want: Result{CoverageComplete: true}},
	}
	policy := Policy{LowThreshold: 3, LowWindowHours: 3, HighThreshold: 2, HighWindowHours: 2}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Evaluate(tt.in, policy)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Evaluate() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestEvaluateRejectsShapeNegativeAndOverflow(t *testing.T) {
	t.Parallel()
	h := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	policy := Policy{LowThreshold: 3, LowWindowHours: 2, HighThreshold: 2, HighWindowHours: 2}
	for _, in := range []Evaluation{
		{Hour: h, LowCounts: []int64{0}, LowCoverage: []bool{true}, HighCounts: []int64{0, 0}},
		{Hour: h, LowCounts: []int64{-1, 0}, LowCoverage: []bool{true, true}, HighCounts: []int64{0, 0}},
		{Hour: h, LowCounts: []int64{0, 0}, LowCoverage: []bool{true, true}, HighCounts: []int64{1<<63 - 1, 1}},
	} {
		if _, err := Evaluate(in, policy); err == nil {
			t.Fatalf("Evaluate(%+v) error = nil", in)
		}
	}
}

func TestDecideActionUsesAuthoritativeTransitionAndRestoreState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		state ObjectState
		low   bool
		high  bool
		want  Action
	}{
		{name: "local low marks", state: ObjectState{Transitioned: false}, low: true, want: ActionMark},
		{name: "local high does not restore", state: ObjectState{Transitioned: false}, high: true, want: ActionNone},
		{name: "transitioned high starts restore", state: ObjectState{Transitioned: true}, high: true, want: ActionRestore},
		{name: "ongoing restore waits", state: ObjectState{Transitioned: true, Restore: &RestoreState{Ongoing: true}}, high: true, want: ActionNone},
		{name: "active restore renews", state: ObjectState{Transitioned: true, Restore: &RestoreState{Expires: now.Add(time.Hour)}}, high: true, want: ActionRenew},
		{name: "expired restore restarts", state: ObjectState{Transitioned: true, Restore: &RestoreState{Expires: now.Add(-time.Second)}}, high: true, want: ActionRestore},
		{name: "active low does not renew", state: ObjectState{Transitioned: true, Restore: &RestoreState{Expires: now.Add(time.Hour)}}, low: true, want: ActionNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DecideAction(tt.state, tt.low, tt.high, now); got != tt.want {
				t.Fatalf("DecideAction() = %q, want %q", got, tt.want)
			}
		})
	}
}
