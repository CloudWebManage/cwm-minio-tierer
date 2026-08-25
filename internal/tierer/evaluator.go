package tierer

import (
	"errors"
	"fmt"
	"math"
	"time"
)

type Policy struct {
	LowThreshold       int64
	LowWindowHours     int
	HighThreshold      int64
	HighWindowHours    int
	HighIncludeCurrent bool
}

type Evaluation struct {
	Hour         time.Time
	LastModified time.Time
	LowCounts    []int64
	LowCoverage  []bool
	HighCounts   []int64
}

type Result struct {
	Low              bool
	High             bool
	CoverageComplete bool
}

func Evaluate(input Evaluation, policy Policy) (Result, error) {
	if policy.LowThreshold < 0 || policy.HighThreshold < 0 || policy.LowWindowHours <= 0 || policy.HighWindowHours <= 0 {
		return Result{}, errors.New("invalid evaluation policy")
	}
	if len(input.LowCounts) != policy.LowWindowHours || len(input.LowCoverage) != policy.LowWindowHours || len(input.HighCounts) != policy.HighWindowHours {
		return Result{}, errors.New("usage input does not match configured windows")
	}
	lowSum, err := sumCounts(input.LowCounts)
	if err != nil {
		return Result{}, fmt.Errorf("low window: %w", err)
	}
	highSum, err := sumCounts(input.HighCounts)
	if err != nil {
		return Result{}, fmt.Errorf("high window: %w", err)
	}
	complete := true
	for _, covered := range input.LowCoverage {
		complete = complete && covered
	}
	hour := input.Hour.UTC().Truncate(time.Hour)
	oldEnough := !input.LastModified.After(hour.Add(-time.Duration(policy.LowWindowHours) * time.Hour))
	return Result{
		Low:              complete && oldEnough && lowSum < policy.LowThreshold,
		High:             highSum > policy.HighThreshold,
		CoverageComplete: complete,
	}, nil
}

func sumCounts(counts []int64) (int64, error) {
	var total int64
	for _, count := range counts {
		if count < 0 {
			return 0, errors.New("negative count")
		}
		if count > math.MaxInt64-total {
			return 0, errors.New("count sum overflows int64")
		}
		total += count
	}
	return total, nil
}

type RestoreState struct {
	Ongoing bool
	Expires time.Time
}

type ObjectState struct {
	Transitioned bool
	Restore      *RestoreState
}

type Action string

const (
	ActionNone    Action = "none"
	ActionMark    Action = "mark"
	ActionRestore Action = "restore"
	ActionRenew   Action = "renew"
)

func DecideAction(state ObjectState, low, high bool, now time.Time) Action {
	if !state.Transitioned {
		if low {
			return ActionMark
		}
		return ActionNone
	}
	if !high || (state.Restore != nil && state.Restore.Ongoing) {
		return ActionNone
	}
	if state.Restore != nil && state.Restore.Expires.After(now) {
		return ActionRenew
	}
	return ActionRestore
}
