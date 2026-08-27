package tierer

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/orihoch/cwm-minio-tierer/internal/contracts"
)

const (
	maxWindowHours             = 10 * 365 * 24
	maxRedisAccessKeysPerChunk = 10_000
)

type Config struct {
	InstanceID      string
	AccessRetention time.Duration
	Apply           bool
	Policy          Policy
	RestoreDays     int

	CoverageTemplate string
	CoverageValue    string
	CoverageEnabled  bool
	MarkerKey        string
	MarkerValue      string

	RedisAddress          string
	RedisUsername         string
	RedisPassword         string
	RedisDB               int
	RedisTLS              bool
	RedisOperationTimeout time.Duration

	MinIOEndpoint         string
	MinIOAccessKey        string
	MinIOSecretKey        string
	MinIOSecure           bool
	MinIORegion           string
	MinIOOperationTimeout time.Duration

	TransitionBudget BudgetLimit
	RestoreBudget    BudgetLimit

	ExcludedBuckets  []string
	ExcludedPrefixes []string
	ChunkSize        int
	CompletionDelay  time.Duration
	RetryDelay       time.Duration
	ListenAddress    string
	ShutdownTimeout  time.Duration
}

type BudgetLimit struct {
	Attempts int64
	Bytes    int64
}

type envLookup func(string) (string, bool)

func LoadConfig(lookup envLookup) (Config, error) {
	var c Config
	var err error
	c.InstanceID = required(lookup, "INSTANCE_ID")
	if err := contracts.ValidateInstanceID(c.InstanceID); err != nil {
		return Config{}, fmt.Errorf("INSTANCE_ID: %w", err)
	}
	if c.AccessRetention, err = duration(lookup, "ACCESS_RETENTION", "", true); err != nil {
		return Config{}, err
	}
	if err := contracts.ValidateCounterRetention(c.AccessRetention); err != nil {
		return Config{}, fmt.Errorf("ACCESS_RETENTION: %w", err)
	}

	mode := optional(lookup, "TIERER_MODE", "audit")
	switch mode {
	case "audit":
		c.Apply = false
	case "apply":
		gate, err := boolean(lookup, "TIERER_APPLY", false)
		if err != nil {
			return Config{}, err
		}
		if !gate {
			return Config{}, errors.New("TIERER_APPLY=true is required for apply mode")
		}
		c.Apply = true
	default:
		return Config{}, errors.New("TIERER_MODE must be exactly audit or apply")
	}
	if c.Policy.LowThreshold, err = integer64(lookup, "TIERER_LOW_THRESHOLD", 0, false); err != nil {
		return Config{}, err
	}
	if c.Policy.LowWindowHours, err = boundedInteger(lookup, "TIERER_LOW_WINDOW_HOURS", 0, 1, maxWindowHours, false); err != nil {
		return Config{}, err
	}
	if c.Policy.HighThreshold, err = integer64(lookup, "TIERER_HIGH_THRESHOLD", 0, false); err != nil {
		return Config{}, err
	}
	if c.Policy.HighWindowHours, err = boundedInteger(lookup, "TIERER_HIGH_WINDOW_HOURS", 0, 1, maxWindowHours, false); err != nil {
		return Config{}, err
	}
	if c.Policy.HighIncludeCurrent, err = boolean(lookup, "TIERER_HIGH_INCLUDE_CURRENT", false); err != nil {
		return Config{}, err
	}
	if c.RestoreDays, err = integer(lookup, "TIERER_RESTORE_DAYS", 1, false); err != nil {
		return Config{}, err
	}
	lowWindow, err := wholeHoursDuration(c.Policy.LowWindowHours)
	if err != nil {
		return Config{}, fmt.Errorf("TIERER_LOW_WINDOW_HOURS: %w", err)
	}
	highWindow, err := wholeHoursDuration(c.Policy.HighWindowHours)
	if err != nil {
		return Config{}, fmt.Errorf("TIERER_HIGH_WINDOW_HOURS: %w", err)
	}
	if err := contracts.ValidateRetention(c.AccessRetention, lowWindow, highWindow); err != nil {
		return Config{}, fmt.Errorf("ACCESS_RETENTION: %w", err)
	}

	if c.CoverageEnabled, err = boolean(lookup, "TIERER_COVERAGE_ENABLED", true); err != nil {
		return Config{}, err
	}
	if c.CoverageEnabled {
		c.CoverageTemplate = required(lookup, "TIERER_COVERAGE_TEMPLATE")
		if err := contracts.ValidateCoverageTemplate(c.CoverageTemplate); err != nil {
			return Config{}, fmt.Errorf("TIERER_COVERAGE_TEMPLATE: %w", err)
		}
		c.CoverageValue = required(lookup, "TIERER_COVERAGE_VALUE")
		if c.CoverageValue == "" {
			return Config{}, errors.New("TIERER_COVERAGE_VALUE is required")
		}
	}
	c.MarkerKey = required(lookup, "TIERER_MARKER_KEY")
	c.MarkerValue = required(lookup, "TIERER_MARKER_VALUE")
	if c.MarkerKey == "" || c.MarkerValue == "" {
		return Config{}, errors.New("TIERER_MARKER_KEY and TIERER_MARKER_VALUE must be non-empty")
	}
	if _, err := tags.NewTags(map[string]string{c.MarkerKey: c.MarkerValue}, true); err != nil {
		return Config{}, fmt.Errorf("marker tag: %w", err)
	}

	c.RedisAddress = optional(lookup, "REDIS_ADDR", "127.0.0.1:6379")
	if err := address("REDIS_ADDR", c.RedisAddress, false); err != nil {
		return Config{}, err
	}
	c.RedisUsername = optional(lookup, "REDIS_USERNAME", "")
	c.RedisPassword = optional(lookup, "REDIS_PASSWORD", "")
	if c.RedisDB, err = boundedInteger(lookup, "REDIS_DB", 0, 0, 1_000_000, true); err != nil {
		return Config{}, err
	}
	if c.RedisTLS, err = boolean(lookup, "REDIS_TLS", false); err != nil {
		return Config{}, err
	}
	if c.RedisOperationTimeout, err = duration(lookup, "REDIS_OPERATION_TIMEOUT", "5s", false); err != nil {
		return Config{}, err
	}

	c.MinIOEndpoint = required(lookup, "MINIO_ENDPOINT")
	if err := address("MINIO_ENDPOINT", c.MinIOEndpoint, false); err != nil {
		return Config{}, err
	}
	c.MinIOAccessKey = required(lookup, "MINIO_ACCESS_KEY")
	c.MinIOSecretKey = required(lookup, "MINIO_SECRET_KEY")
	if c.MinIOAccessKey == "" || c.MinIOSecretKey == "" {
		return Config{}, errors.New("MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required")
	}
	if c.MinIOSecure, err = boolean(lookup, "MINIO_SECURE", true); err != nil {
		return Config{}, err
	}
	c.MinIORegion = optional(lookup, "MINIO_REGION", "")
	if c.MinIOOperationTimeout, err = duration(lookup, "MINIO_OPERATION_TIMEOUT", "30s", false); err != nil {
		return Config{}, err
	}

	if c.Apply {
		if c.TransitionBudget.Attempts, err = optionalInteger64(lookup, "TIERER_DAILY_TRANSITION_ATTEMPTS", 1); err != nil {
			return Config{}, err
		}
		if c.TransitionBudget.Bytes, err = optionalInteger64(lookup, "TIERER_DAILY_TRANSITION_BYTES", 1); err != nil {
			return Config{}, err
		}
		if c.RestoreBudget.Attempts, err = optionalInteger64(lookup, "TIERER_DAILY_RESTORE_ATTEMPTS", 1); err != nil {
			return Config{}, err
		}
		if c.RestoreBudget.Bytes, err = optionalInteger64(lookup, "TIERER_DAILY_RESTORE_BYTES", 1); err != nil {
			return Config{}, err
		}
	}

	if c.ExcludedBuckets, err = exactList(lookup, "TIERER_EXCLUDE_BUCKETS"); err != nil {
		return Config{}, err
	}
	if c.ExcludedPrefixes, err = exactList(lookup, "TIERER_EXCLUDE_BUCKET_PREFIXES"); err != nil {
		return Config{}, err
	}
	if c.ChunkSize, err = boundedInteger(lookup, "TIERER_CHUNK_SIZE", 100, 1, 1_000_000, true); err != nil {
		return Config{}, err
	}
	if err := validateAggregateAccessKeys(c.ChunkSize, c.Policy.LowWindowHours, c.Policy.HighWindowHours, maxRedisAccessKeysPerChunk); err != nil {
		return Config{}, err
	}
	if c.CompletionDelay, err = duration(lookup, "TIERER_COMPLETION_DELAY", "5m", false); err != nil {
		return Config{}, err
	}
	if c.RetryDelay, err = duration(lookup, "TIERER_RETRY_DELAY", "30s", false); err != nil {
		return Config{}, err
	}
	c.ListenAddress = optional(lookup, "TIERER_LISTEN_ADDR", ":8081")
	if err := address("TIERER_LISTEN_ADDR", c.ListenAddress, true); err != nil {
		return Config{}, err
	}
	if c.ShutdownTimeout, err = duration(lookup, "TIERER_SHUTDOWN_TIMEOUT", "30s", false); err != nil {
		return Config{}, err
	}
	return c, nil
}

func validateAggregateAccessKeys(objects, lowHours, highHours, limit int) error {
	if objects <= 0 || lowHours <= 0 || highHours <= 0 || limit <= 0 {
		return errors.New("aggregate Redis access-key dimensions and limit must be positive")
	}
	maxInt := int(^uint(0) >> 1)
	if lowHours > maxInt-highHours {
		return errors.New("aggregate Redis access-key hour count overflows int")
	}
	totalHours := lowHours + highHours
	if objects > limit/totalHours {
		return fmt.Errorf("aggregate Redis access-key count exceeds fixed limit %d: chunk_size %d * (%d low + %d high hours)", limit, objects, lowHours, highHours)
	}
	return nil
}

func wholeHoursDuration(hours int) (time.Duration, error) {
	if hours <= 0 || int64(hours) > int64((time.Duration(1<<63-1))/time.Hour) {
		return 0, errors.New("hours overflow time.Duration")
	}
	return time.Duration(hours) * time.Hour, nil
}

func required(lookup envLookup, key string) string {
	value, _ := lookup(key)
	return value
}

func optional(lookup envLookup, key, fallback string) string {
	if value, ok := lookup(key); ok {
		return value
	}
	return fallback
}

func boolean(lookup envLookup, key string, fallback bool) (bool, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	if raw == "true" {
		return true, nil
	}
	if raw == "false" {
		return false, nil
	}
	return false, fmt.Errorf("%s must be exactly true or false", key)
}

func integer(lookup envLookup, key string, minimum int, allowMissing bool) (int, error) {
	raw, ok := lookup(key)
	if !ok && !allowMissing {
		return 0, fmt.Errorf("%s is required", key)
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum {
		return 0, fmt.Errorf("%s must be an integer of at least %d", key, minimum)
	}
	return value, nil
}

func integer64(lookup envLookup, key string, minimum int64, allowMissing bool) (int64, error) {
	raw, ok := lookup(key)
	if !ok && !allowMissing {
		return 0, fmt.Errorf("%s is required", key)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < minimum {
		return 0, fmt.Errorf("%s must be an integer of at least %d", key, minimum)
	}
	return value, nil
}

func optionalInteger64(lookup envLookup, key string, minimum int64) (int64, error) {
	raw, ok := lookup(key)
	if !ok || raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < minimum {
		return 0, fmt.Errorf("%s must be empty or an integer of at least %d", key, minimum)
	}
	return value, nil
}

func boundedInteger(lookup envLookup, key string, fallback, minimum, maximum int, allowMissing bool) (int, error) {
	raw, ok := lookup(key)
	if !ok && allowMissing {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, minimum, maximum)
	}
	return value, nil
}

func duration(lookup envLookup, key, fallback string, mustSet bool) (time.Duration, error) {
	raw, ok := lookup(key)
	if !ok {
		if mustSet {
			return 0, fmt.Errorf("%s is required", key)
		}
		raw = fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", key)
	}
	return value, nil
}

func exactList(lookup envLookup, key string) ([]string, error) {
	raw, ok := lookup(key)
	if !ok || raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		if part == "" || strings.TrimSpace(part) != part {
			return nil, fmt.Errorf("%s must be a comma-separated list without blanks or surrounding whitespace", key)
		}
		if _, exists := seen[part]; exists {
			return nil, fmt.Errorf("%s contains duplicate %q", key, part)
		}
		seen[part] = struct{}{}
	}
	return parts, nil
}

func address(key, raw string, allowEmptyHost bool) error {
	host, port, err := net.SplitHostPort(raw)
	if err != nil || (!allowEmptyHost && host == "") || strings.TrimSpace(raw) != raw {
		return fmt.Errorf("%s must be a host:port address", key)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%s port must be between 1 and 65535", key)
	}
	return nil
}
