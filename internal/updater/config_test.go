package updater

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigUsesSafeBoundedDefaults(t *testing.T) {
	t.Parallel()

	config, err := LoadConfig(mapLookup(map[string]string{
		"INSTANCE_ID":      "site-a",
		"ACCESS_RETENTION": "168h",
	}))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.SuccessStatus != 200 || config.FailureStatus != 500 {
		t.Fatalf("statuses = %d/%d", config.SuccessStatus, config.FailureStatus)
	}
	if config.MaxBodyBytes != 1<<20 || config.MaxRecords != 1000 || config.QueueSize != 128 {
		t.Fatalf("limits = bytes:%d records:%d queue:%d", config.MaxBodyBytes, config.MaxRecords, config.QueueSize)
	}
	if config.BatchMaxEvents != 5000 || config.BatchMaxKeys != 5000 || config.BatchMaxWait != 50*time.Millisecond {
		t.Fatalf("batch limits = events:%d keys:%d wait:%s", config.BatchMaxEvents, config.BatchMaxKeys, config.BatchMaxWait)
	}
	if config.RedisAddress != "127.0.0.1:6379" || config.ListenAddress != ":8080" {
		t.Fatalf("addresses = Redis %q HTTP %q", config.RedisAddress, config.ListenAddress)
	}
	if config.WriteTimeout != 11*time.Minute+7*time.Second+450*time.Millisecond {
		t.Fatalf("WriteTimeout = %s, want safe default 11m7.45s", config.WriteTimeout)
	}
}

func TestLoadConfigWriteTimeoutCoversWorstCaseAcknowledgement(t *testing.T) {
	t.Parallel()

	base := map[string]string{
		"INSTANCE_ID":             "site-a",
		"ACCESS_RETENTION":        "168h",
		"UPDATER_QUEUE_SIZE":      "2",
		"UPDATER_BATCH_MAX_WAIT":  "100ms",
		"REDIS_OPERATION_TIMEOUT": "1s",
		"UPDATER_READ_TIMEOUT":    "2s",
		"UPDATER_WRITE_TIMEOUT":   "5.3s",
	}
	if _, err := LoadConfig(mapLookup(base)); err == nil || !strings.Contains(err.Error(), "queue backlog") {
		t.Fatalf("LoadConfig() error = %v, want unsafe write-timeout error", err)
	}
	base["UPDATER_WRITE_TIMEOUT"] = "5.300000001s"
	if _, err := LoadConfig(mapLookup(base)); err != nil {
		t.Fatalf("LoadConfig() with write timeout above 5.3s error = %v", err)
	}
}

func TestLoadConfigEnforcesStatusRiskGates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{name: "non-5xx failure", env: map[string]string{"UPDATER_FAILURE_STATUS": "200"}, wantErr: "UPDATER_ACCEPT_DATA_LOSS"},
		{name: "non-2xx success", env: map[string]string{"UPDATER_SUCCESS_STATUS": "500"}, wantErr: "UPDATER_ACCEPT_DUPLICATE_RISK"},
		{name: "strict risk boolean", env: map[string]string{"UPDATER_ACCEPT_DATA_LOSS": "1"}, wantErr: "true or false"},
		{name: "invalid status", env: map[string]string{"UPDATER_FAILURE_STATUS": "600"}, wantErr: "between 200 and 599"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.env["INSTANCE_ID"] = "site-a"
			tt.env["ACCESS_RETENTION"] = "168h"
			_, err := LoadConfig(mapLookup(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("LoadConfig() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}

	config, err := LoadConfig(mapLookup(map[string]string{
		"INSTANCE_ID":                   "site-a",
		"ACCESS_RETENTION":              "168h",
		"UPDATER_FAILURE_STATUS":        "200",
		"UPDATER_ACCEPT_DATA_LOSS":      "true",
		"UPDATER_SUCCESS_STATUS":        "500",
		"UPDATER_ACCEPT_DUPLICATE_RISK": "true",
	}))
	if err != nil {
		t.Fatalf("LoadConfig() with overrides error = %v", err)
	}
	if !config.DataLossRisk || !config.DuplicateRisk {
		t.Fatalf("risk flags = data loss:%t duplicate:%t", config.DataLossRisk, config.DuplicateRisk)
	}
}

func TestLoadConfigActivatesRiskSignalsOnlyForUnsafeStatuses(t *testing.T) {
	t.Parallel()

	config, err := LoadConfig(mapLookup(map[string]string{
		"INSTANCE_ID":                   "site-a",
		"ACCESS_RETENTION":              "168h",
		"UPDATER_ACCEPT_DATA_LOSS":      "true",
		"UPDATER_ACCEPT_DUPLICATE_RISK": "true",
	}))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.DataLossRisk || config.DuplicateRisk {
		t.Fatalf("safe statuses activated risk signals: data loss:%t duplicate:%t", config.DataLossRisk, config.DuplicateRisk)
	}
}

func TestLoadConfigRejectsARequestLargerThanABatch(t *testing.T) {
	t.Parallel()

	_, err := LoadConfig(mapLookup(map[string]string{
		"INSTANCE_ID":              "site-a",
		"ACCESS_RETENTION":         "168h",
		"UPDATER_MAX_RECORDS":      "20",
		"UPDATER_BATCH_MAX_KEYS":   "10",
		"UPDATER_BATCH_MAX_EVENTS": "10",
	}))
	if err == nil || !strings.Contains(err.Error(), "one request") {
		t.Fatalf("LoadConfig() error = %v, want request/batch bounds error", err)
	}
}

func TestLoadConfigRequiresIdentityAndPositiveRetention(t *testing.T) {
	t.Parallel()

	if _, err := LoadConfig(mapLookup(map[string]string{"ACCESS_RETENTION": "168h"})); err == nil {
		t.Fatal("LoadConfig() without instance error = nil")
	}
	if _, err := LoadConfig(mapLookup(map[string]string{"INSTANCE_ID": "site-a", "ACCESS_RETENTION": "0s"})); err == nil {
		t.Fatal("LoadConfig() with zero retention error = nil")
	}
	if _, err := LoadConfig(mapLookup(map[string]string{"INSTANCE_ID": "site-a", "ACCESS_RETENTION": "168h500ms"})); err == nil || !strings.Contains(err.Error(), "whole seconds") {
		t.Fatalf("LoadConfig() with fractional retention error = %v, want whole-seconds error", err)
	}
}

func TestLoadConfigRejectsMalformedNetworkAddresses(t *testing.T) {
	t.Parallel()

	for _, env := range []map[string]string{
		{"INSTANCE_ID": "site-a", "ACCESS_RETENTION": "168h", "REDIS_ADDR": ""},
		{"INSTANCE_ID": "site-a", "ACCESS_RETENTION": "168h", "REDIS_ADDR": "redis-without-port"},
		{"INSTANCE_ID": "site-a", "ACCESS_RETENTION": "168h", "UPDATER_LISTEN_ADDR": "8080"},
	} {
		if _, err := LoadConfig(mapLookup(env)); err == nil {
			t.Fatalf("LoadConfig(%v) error = nil", env)
		}
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestLoadConfigRejectsInvalidMetricsListener(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, ingestion, metrics string
	}{
		{"missing port", "127.0.0.1:8921", "127.0.0.1"},
		{"URL", "127.0.0.1:8921", "http://127.0.0.1:8922"},
		{"whitespace", "127.0.0.1:8921", " 127.0.0.1:8922"},
		{"host whitespace", "127.0.0.1:8921", "bad host:8922"},
		{"host path", "127.0.0.1:8921", "localhost/path:8922"},
		{"zero port", "127.0.0.1:8921", "127.0.0.1:0"},
		{"port out of range", "127.0.0.1:8921", ":65536"},
		{"named port", "127.0.0.1:8921", ":http"},
		{"same address", "127.0.0.1:8921", "127.0.0.1:8921"},
		{"same numeric port", "127.0.0.1:8921", "127.0.0.1:08921"},
		{"same IPv6 address", "[::1]:8921", "[0:0:0:0:0:0:0:1]:8921"},
		{"wildcard ingestion", ":8921", "0.0.0.0:8921"},
		{"wildcard metrics", "127.0.0.1:8921", "0.0.0.0:8921"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadConfig(mapLookup(map[string]string{
				"INSTANCE_ID": "site-a", "ACCESS_RETENTION": "168h",
				"UPDATER_LISTEN_ADDR": tt.ingestion, "UPDATER_METRICS_LISTEN_ADDR": tt.metrics,
			}))
			if err == nil || !strings.Contains(err.Error(), "UPDATER_METRICS_LISTEN_ADDR") {
				t.Fatalf("LoadConfig() error = %v, want metrics listen address error", err)
			}
		})
	}
}

func TestLoadConfigOptionalMetricsListener(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, ingestion, metrics string
		set                      bool
	}{
		{"unset", "127.0.0.1:8921", "", false},
		{"empty", "127.0.0.1:8921", "", true},
		{"separate public monitoring", "127.0.0.1:8921", "0.0.0.0:8922", true},
		{"IPv6", "[::1]:8921", "[::]:8922", true},
		{"distinct interfaces", "127.0.0.1:8921", "127.0.0.2:8921", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{"INSTANCE_ID": "site-a", "ACCESS_RETENTION": "168h", "UPDATER_LISTEN_ADDR": tt.ingestion}
			if tt.set {
				env["UPDATER_METRICS_LISTEN_ADDR"] = tt.metrics
			}
			config, err := LoadConfig(mapLookup(env))
			if err != nil || config.MetricsListenAddress != tt.metrics || config.ListenAddress != tt.ingestion {
				t.Fatalf("listen addresses = ingestion:%q metrics:%q, error = %v", config.ListenAddress, config.MetricsListenAddress, err)
			}
		})
	}
}
