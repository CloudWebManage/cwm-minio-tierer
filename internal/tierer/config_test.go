package tierer

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaultsToAuditAndRequiresEvaluationContracts(t *testing.T) {
	t.Parallel()
	env := validEnv()
	config, err := LoadConfig(mapLookup(env))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.Apply {
		t.Fatal("LoadConfig() Apply = true, want audit default")
	}
	if config.Policy.LowThreshold != 3 || config.Policy.LowWindowHours != 48 || config.Policy.HighThreshold != 10 || config.Policy.HighWindowHours != 2 {
		t.Fatalf("LoadConfig() policy = %+v", config.Policy)
	}
	if config.CompletionDelay != 5*time.Minute || config.ChunkSize != 100 {
		t.Fatalf("LoadConfig() operational defaults = %+v", config)
	}
	if config.RetryDelay != 30*time.Second {
		t.Fatalf("LoadConfig() RetryDelay = %s, want 30s", config.RetryDelay)
	}
	if config.MinIOOperationTimeout != 30*time.Second {
		t.Fatalf("LoadConfig() MinIOOperationTimeout = %s, want 30s", config.MinIOOperationTimeout)
	}
	if !config.CoverageEnabled {
		t.Fatal("LoadConfig() CoverageEnabled = false, want true default")
	}
	delete(env, "TIERER_COVERAGE_TEMPLATE")
	if _, err := LoadConfig(mapLookup(env)); err == nil {
		t.Fatal("LoadConfig() error = nil without coverage template")
	}
}

func TestLoadConfigAllowsDisablingCoverageRecords(t *testing.T) {
	t.Parallel()
	env := validEnv()
	env["TIERER_COVERAGE_ENABLED"] = "false"
	delete(env, "TIERER_COVERAGE_TEMPLATE")
	delete(env, "TIERER_COVERAGE_VALUE")
	config, err := LoadConfig(mapLookup(env))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.CoverageEnabled {
		t.Fatal("LoadConfig() CoverageEnabled = true, want disabled")
	}
}

func TestLoadConfigApplyDefaultsMissingBudgetsToUnlimitedAndRequiresGate(t *testing.T) {
	t.Parallel()
	env := validEnv()
	env["TIERER_MODE"] = "apply"
	if _, err := LoadConfig(mapLookup(env)); err == nil || !strings.Contains(err.Error(), "TIERER_APPLY") {
		t.Fatalf("LoadConfig() error = %v, want apply gate", err)
	}
	env["TIERER_APPLY"] = "true"
	config, err := LoadConfig(mapLookup(env))
	if err != nil || !config.Apply {
		t.Fatalf("LoadConfig() = %+v, %v, want apply", config, err)
	}
	if config.TransitionBudget != (BudgetLimit{}) || config.RestoreBudget != (BudgetLimit{}) {
		t.Fatalf("LoadConfig() budgets = %+v %+v, want unlimited", config.TransitionBudget, config.RestoreBudget)
	}
	for _, key := range []string{"TIERER_DAILY_TRANSITION_ATTEMPTS", "TIERER_DAILY_TRANSITION_BYTES", "TIERER_DAILY_RESTORE_ATTEMPTS", "TIERER_DAILY_RESTORE_BYTES"} {
		env[key] = "100"
	}
	config, err = LoadConfig(mapLookup(env))
	if err != nil || !config.Apply {
		t.Fatalf("LoadConfig() = %+v, %v, want apply", config, err)
	}
	env["TIERER_DAILY_RESTORE_BYTES"] = ""
	config, err = LoadConfig(mapLookup(env))
	if err != nil || config.RestoreBudget.Bytes != 0 {
		t.Fatalf("LoadConfig() restore bytes = %d, %v, want unlimited", config.RestoreBudget.Bytes, err)
	}
	env["TIERER_DAILY_RESTORE_BYTES"] = "0"
	if _, err := LoadConfig(mapLookup(env)); err == nil {
		t.Fatal("LoadConfig() error = nil for zero apply budget")
	}
}

func TestLoadConfigRejectsAmbiguousListsDurationsAndThresholds(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"TIERER_EXCLUDE_BUCKETS":      "one, two",
		"TIERER_LOW_WINDOW_HOURS":     "1h",
		"TIERER_HIGH_THRESHOLD":       "-1",
		"TIERER_HIGH_INCLUDE_CURRENT": "TRUE",
		"TIERER_MARKER_KEY":           strings.Repeat("k", 129),
		"TIERER_COMPLETION_DELAY":     "0s",
		"TIERER_RESTORE_DAYS":         "0",
		"TIERER_MARKER_VALUE":         "",
	}
	for key, value := range tests {
		t.Run(key, func(t *testing.T) {
			env := validEnv()
			env[key] = value
			if _, err := LoadConfig(mapLookup(env)); err == nil {
				t.Fatalf("LoadConfig() error = nil for %s=%q", key, value)
			}
		})
	}
}

func TestLoadConfigRejectsWindowHoursAboveSafeBound(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"TIERER_LOW_WINDOW_HOURS", "TIERER_HIGH_WINDOW_HOURS"} {
		env := validEnv()
		env["ACCESS_RETENTION"] = "100000h"
		env[key] = "87601"
		if _, err := LoadConfig(mapLookup(env)); err == nil {
			t.Fatalf("LoadConfig() error = nil for %s above safe bound", key)
		}
	}
}

func TestLoadConfigBoundsAggregateRedisAccessKeys(t *testing.T) {
	t.Parallel()
	env := validEnv()
	env["TIERER_LOW_WINDOW_HOURS"] = "24"
	env["TIERER_HIGH_WINDOW_HOURS"] = "24"
	env["TIERER_CHUNK_SIZE"] = "208"
	if _, err := LoadConfig(mapLookup(env)); err != nil {
		t.Fatalf("LoadConfig() rejected 9,984 aggregate access keys: %v", err)
	}
	env["TIERER_CHUNK_SIZE"] = "209"
	if _, err := LoadConfig(mapLookup(env)); err == nil || !strings.Contains(err.Error(), "aggregate Redis access-key") {
		t.Fatalf("LoadConfig() error = %v, want aggregate key limit", err)
	}
}

func validEnv() map[string]string {
	return map[string]string{
		"INSTANCE_ID":              "site-a",
		"ACCESS_RETENTION":         "72h",
		"REDIS_ADDR":               "redis:6379",
		"MINIO_ENDPOINT":           "minio:9000",
		"MINIO_ACCESS_KEY":         "access",
		"MINIO_SECRET_KEY":         "secret",
		"TIERER_LOW_THRESHOLD":     "3",
		"TIERER_LOW_WINDOW_HOURS":  "48",
		"TIERER_HIGH_THRESHOLD":    "10",
		"TIERER_HIGH_WINDOW_HOURS": "2",
		"TIERER_RESTORE_DAYS":      "7",
		"TIERER_COVERAGE_TEMPLATE": "coverage:site-a:g1:2006:01:02:15",
		"TIERER_COVERAGE_VALUE":    "complete",
		"TIERER_MARKER_KEY":        "cwm-tier",
		"TIERER_MARKER_VALUE":      "eligible",
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
