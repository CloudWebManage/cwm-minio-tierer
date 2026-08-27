package tierer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/orihoch/cwm-minio-tierer/internal/contracts"
	"github.com/redis/go-redis/v9"
)

type redisCommands interface {
	Eval(context.Context, string, []string, ...any) (any, error)
	Get(context.Context, string) (string, error)
	Set(context.Context, string, any, time.Duration) error
	Del(context.Context, string) error
	Ping(context.Context) error
}

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(client *redis.Client) *RedisClient { return &RedisClient{client: client} }

func (c *RedisClient) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	return c.client.Eval(ctx, script, keys, args...).Result()
}

func (c *RedisClient) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

func (c *RedisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	return c.client.Set(ctx, key, value, expiration).Err()
}

func (c *RedisClient) Del(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}

func (c *RedisClient) Ping(ctx context.Context) error { return c.client.Ping(ctx).Err() }

type RedisStore struct {
	client           redisCommands
	instance         string
	coverageTemplate string
	coverageValue    string
	coverageEnabled  bool
}

func NewRedisStore(client redisCommands, instance, coverageTemplate, coverageValue string, coverageEnabled bool) *RedisStore {
	return &RedisStore{client: client, instance: instance, coverageTemplate: coverageTemplate, coverageValue: coverageValue, coverageEnabled: coverageEnabled}
}

func accessKey(instance, bucket, object string, hour time.Time) (string, error) {
	return contracts.AccessKey(instance, bucket, object, hour)
}

func (s *RedisStore) ReadCounts(ctx context.Context, bucket, object string, hours []time.Time) ([]int64, error) {
	if len(hours) == 0 {
		return nil, errors.New("count window must not be empty")
	}
	keys := make([]string, len(hours))
	for i, hour := range hours {
		key, err := accessKey(s.instance, bucket, object, hour)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	raw, err := s.client.Eval(ctx, readUnsignedStringsScript, keys)
	if err != nil {
		return nil, fmt.Errorf("read access counters: %w", err)
	}
	values, err := redisArray(raw, len(keys))
	if err != nil {
		return nil, fmt.Errorf("read access counters: %w", err)
	}
	counts := make([]int64, len(values))
	for i, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("read access counters: unexpected value type %T", value)
		}
		count, err := strconv.ParseInt(text, 10, 64)
		if err != nil || count < 0 || (len(text) > 1 && text[0] == '0') {
			return nil, fmt.Errorf("read access counters: malformed unsigned integer at index %d", i)
		}
		counts[i] = count
	}
	return counts, nil
}

func (s *RedisStore) ReadCoverage(ctx context.Context, hours []time.Time) ([]bool, error) {
	if len(hours) == 0 {
		return nil, errors.New("coverage window must not be empty")
	}
	if !s.coverageEnabled {
		covered := make([]bool, len(hours))
		for i := range covered {
			covered[i] = true
		}
		return covered, nil
	}
	keys := make([]string, len(hours))
	for i, hour := range hours {
		keys[i] = contracts.CoverageKey(s.coverageTemplate, hour)
	}
	raw, err := s.client.Eval(ctx, readOptionalStringsScript, keys)
	if err != nil {
		return nil, fmt.Errorf("read coverage: %w", err)
	}
	values, err := redisArray(raw, len(keys))
	if err != nil {
		return nil, fmt.Errorf("read coverage: %w", err)
	}
	covered := make([]bool, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("read coverage: unexpected value type %T", value)
		}
		covered[i] = text == s.coverageValue
	}
	return covered, nil
}

func (s *RedisStore) ReadChunk(ctx context.Context, objects []Object, lowHours, highHours []time.Time) ([]ObjectUsage, []bool, error) {
	if len(objects) == 0 || len(lowHours) == 0 || len(highHours) == 0 {
		return nil, nil, errors.New("chunk objects and hour windows must not be empty")
	}
	if err := validateAggregateAccessKeys(len(objects), len(lowHours), len(highHours), maxRedisAccessKeysPerChunk); err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, len(objects)*(len(lowHours)+len(highHours)))
	for _, object := range objects {
		for _, hours := range [][]time.Time{lowHours, highHours} {
			for _, hour := range hours {
				key, err := accessKey(s.instance, object.Bucket, object.Name, hour)
				if err != nil {
					return nil, nil, err
				}
				keys = append(keys, key)
			}
		}
	}
	raw, err := s.client.Eval(ctx, readUnsignedStringsScript, keys)
	if err != nil {
		return nil, nil, fmt.Errorf("read chunk access counters: %w", err)
	}
	values, err := redisArray(raw, len(keys))
	if err != nil {
		return nil, nil, fmt.Errorf("read chunk access counters: %w", err)
	}
	usage := make([]ObjectUsage, len(objects))
	offset := 0
	for i := range objects {
		usage[i].Low, err = decodeCounts(values[offset : offset+len(lowHours)])
		if err != nil {
			return nil, nil, fmt.Errorf("read low chunk counters for object %d: %w", i, err)
		}
		offset += len(lowHours)
		usage[i].High, err = decodeCounts(values[offset : offset+len(highHours)])
		if err != nil {
			return nil, nil, fmt.Errorf("read high chunk counters for object %d: %w", i, err)
		}
		offset += len(highHours)
	}
	coverage, err := s.ReadCoverage(ctx, lowHours)
	if err != nil {
		return nil, nil, err
	}
	return usage, coverage, nil
}

func decodeCounts(values []any) ([]int64, error) {
	counts := make([]int64, len(values))
	for i, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type %T", value)
		}
		count, err := strconv.ParseInt(text, 10, 64)
		if err != nil || count < 0 || (len(text) > 1 && text[0] == '0') {
			return nil, fmt.Errorf("malformed unsigned integer at index %d", i)
		}
		counts[i] = count
	}
	return counts, nil
}

type BudgetKind string

const (
	BudgetTransition BudgetKind = "transition"
	BudgetRestore    BudgetKind = "restore"
)

type BudgetReservation struct {
	Allowed      bool
	UsedAttempts int64
	UsedBytes    int64
}

func (s *RedisStore) Reserve(ctx context.Context, now time.Time, kind BudgetKind, limit BudgetLimit, bytes int64) (BudgetReservation, error) {
	if kind != BudgetTransition && kind != BudgetRestore {
		return BudgetReservation{}, errors.New("invalid budget kind")
	}
	if limit.Attempts < 0 || limit.Bytes < 0 || bytes < 0 {
		return BudgetReservation{}, errors.New("invalid budget reservation")
	}
	if limit.Attempts == 0 && limit.Bytes == 0 {
		return BudgetReservation{Allowed: true}, nil
	}
	attemptLimit := limit.Attempts
	if attemptLimit == 0 {
		attemptLimit = math.MaxInt64
	}
	byteLimit := limit.Bytes
	if byteLimit == 0 {
		byteLimit = math.MaxInt64
	}
	day := now.UTC().Format("2006:01:02")
	prefix := fmt.Sprintf("%s:%s:budget:%s:%s", contracts.Namespace, s.instance, day, kind)
	keys := []string{prefix + "-attempts", prefix + "-bytes"}
	expires := now.UTC().Truncate(24 * time.Hour).Add(72 * time.Hour).Unix()
	raw, err := s.client.Eval(ctx, reserveBudgetScript, keys, attemptLimit, byteLimit, bytes, expires)
	if err != nil {
		return BudgetReservation{}, fmt.Errorf("reserve %s budget: %w", kind, err)
	}
	values, err := redisArray(raw, 3)
	if err != nil {
		return BudgetReservation{}, fmt.Errorf("reserve %s budget: %w", kind, err)
	}
	allowed, err := redisInt64(values[0])
	if err != nil {
		return BudgetReservation{}, err
	}
	usedAttempts, err := redisInt64(values[1])
	if err != nil {
		return BudgetReservation{}, err
	}
	usedBytes, err := redisInt64(values[2])
	if err != nil {
		return BudgetReservation{}, err
	}
	return BudgetReservation{Allowed: allowed == 1, UsedAttempts: usedAttempts, UsedBytes: usedBytes}, nil
}

type Cursor struct {
	Bucket    string    `json:"bucket"`
	Object    string    `json:"object"`
	UpdatedAt time.Time `json:"updated_at"`
}

type cursorEnvelope struct {
	Version int `json:"version"`
	Cursor
}

func ScopeHash(excludedBuckets, excludedPrefixes []string, markerKey, markerValue string) string {
	buckets := append([]string(nil), excludedBuckets...)
	prefixes := append([]string(nil), excludedPrefixes...)
	slicesSort(buckets)
	slicesSort(prefixes)
	payload, _ := json.Marshal(struct {
		Version          int      `json:"version"`
		ExcludedBuckets  []string `json:"excluded_buckets"`
		ExcludedPrefixes []string `json:"excluded_prefixes"`
		MarkerKey        string   `json:"marker_key"`
		MarkerValue      string   `json:"marker_value"`
	}{1, buckets, prefixes, markerKey, markerValue})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func ConfigScopeHash(config Config) string {
	buckets := append([]string(nil), config.ExcludedBuckets...)
	prefixes := append([]string(nil), config.ExcludedPrefixes...)
	slicesSort(buckets)
	slicesSort(prefixes)
	payload, _ := json.Marshal(struct {
		Version          int         `json:"version"`
		Apply            bool        `json:"apply"`
		MinIOEndpoint    string      `json:"minio_endpoint"`
		Policy           Policy      `json:"policy"`
		RestoreDays      int         `json:"restore_days"`
		CoverageTemplate string      `json:"coverage_template"`
		CoverageValue    string      `json:"coverage_value"`
		CoverageEnabled  bool        `json:"coverage_enabled"`
		MarkerKey        string      `json:"marker_key"`
		MarkerValue      string      `json:"marker_value"`
		TransitionBudget BudgetLimit `json:"transition_budget"`
		RestoreBudget    BudgetLimit `json:"restore_budget"`
		ChunkSize        int         `json:"chunk_size"`
		ExcludedBuckets  []string    `json:"excluded_buckets"`
		ExcludedPrefixes []string    `json:"excluded_prefixes"`
	}{1, config.Apply, config.MinIOEndpoint, config.Policy, config.RestoreDays, config.CoverageTemplate, config.CoverageValue, config.CoverageEnabled, config.MarkerKey, config.MarkerValue, config.TransitionBudget, config.RestoreBudget, config.ChunkSize, buckets, prefixes})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func slicesSort(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func CursorBefore(left, right Cursor) bool {
	return left.Bucket < right.Bucket || (left.Bucket == right.Bucket && left.Object < right.Object)
}

func (s *RedisStore) cursorKey(scope string) (string, error) {
	if len(scope) != sha256.Size*2 {
		return "", errors.New("scope hash must be a full SHA-256 hex digest")
	}
	if _, err := hex.DecodeString(scope); err != nil {
		return "", errors.New("scope hash must be hexadecimal")
	}
	return fmt.Sprintf("%s:%s:cursor:%s", contracts.Namespace, s.instance, scope), nil
}

func (s *RedisStore) SaveCursor(ctx context.Context, scope string, cursor Cursor) error {
	key, err := s.cursorKey(scope)
	if err != nil {
		return err
	}
	if cursor.Bucket == "" || cursor.Object == "" || cursor.UpdatedAt.IsZero() {
		return errors.New("cursor bucket, object, and update time are required")
	}
	payload, err := json.Marshal(cursorEnvelope{Version: 1, Cursor: cursor})
	if err != nil {
		return err
	}
	if err := s.client.Set(ctx, key, payload, 0); err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	return nil
}

func (s *RedisStore) LoadCursor(ctx context.Context, scope string) (*Cursor, error) {
	key, err := s.cursorKey(scope)
	if err != nil {
		return nil, err
	}
	raw, err := s.client.Get(ctx, key)
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load cursor: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope cursorEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode cursor: trailing JSON")
	}
	if envelope.Version != 1 || envelope.Bucket == "" || envelope.Object == "" || envelope.UpdatedAt.IsZero() {
		return nil, errors.New("invalid or unsupported cursor")
	}
	return &envelope.Cursor, nil
}

func (s *RedisStore) ResetCursor(ctx context.Context, scope string) error {
	key, err := s.cursorKey(scope)
	if err != nil {
		return err
	}
	if err := s.client.Del(ctx, key); err != nil {
		return fmt.Errorf("reset cursor: %w", err)
	}
	return nil
}

func redisArray(value any, length int) ([]any, error) {
	values, ok := value.([]any)
	if !ok || len(values) != length {
		return nil, fmt.Errorf("unexpected Redis response %T", value)
	}
	return values, nil
}

func redisInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err
	default:
		return 0, fmt.Errorf("unexpected Redis integer type %T", value)
	}
}

const readUnsignedStringsScript = `
local MAX = "9223372036854775807"
local out = {}
for i, key in ipairs(KEYS) do
  local kind = redis.call("TYPE", key)
  if type(kind) == "table" then kind = kind["ok"] end
  if kind == "none" then
    out[i] = "0"
  elseif kind ~= "string" then
    return redis.error_reply("CWM_VALIDATION wrong counter type")
  else
    local value = redis.call("GET", key)
    if value ~= "0" and string.match(value, "^[1-9][0-9]*$") == nil then
      return redis.error_reply("CWM_VALIDATION malformed counter")
    end
    if string.len(value) > string.len(MAX) or (string.len(value) == string.len(MAX) and value > MAX) then
      return redis.error_reply("CWM_VALIDATION counter overflow")
    end
    out[i] = value
  end
end
return out
`

const readOptionalStringsScript = `
local out = {}
for i, key in ipairs(KEYS) do
  local kind = redis.call("TYPE", key)
  if type(kind) == "table" then kind = kind["ok"] end
  if kind == "none" then
    out[i] = false
  elseif kind ~= "string" then
    return redis.error_reply("CWM_VALIDATION wrong coverage type")
  else
    out[i] = redis.call("GET", key)
  end
end
return out
`

const reserveBudgetScript = `
local MAX_INT64 = "9223372036854775807"

local function valid_uint(value)
  if type(value) ~= "string" then return false end
  if value == "0" then return true end
  if string.match(value, "^[1-9][0-9]*$") == nil then return false end
  if string.len(value) < string.len(MAX_INT64) then return true end
  if string.len(value) > string.len(MAX_INT64) then return false end
  return value <= MAX_INT64
end

local function current(key)
  local kind = redis.call("TYPE", key)
  if type(kind) == "table" then kind = kind["ok"] end
  if kind == "none" then return "0" end
  if kind ~= "string" then return nil end
  local raw = redis.call("GET", key)
  if not valid_uint(raw) then return nil end
  return raw
end

local function add_uint(left, right)
  local i = string.len(left)
  local j = string.len(right)
  local carry = 0
  local result = ""
  while i > 0 or j > 0 or carry > 0 do
    local a = 0
    local b = 0
    if i > 0 then a = tonumber(string.sub(left, i, i)); i = i - 1 end
    if j > 0 then b = tonumber(string.sub(right, j, j)); j = j - 1 end
    local sum = a + b + carry
    result = tostring(sum % 10) .. result
    carry = math.floor(sum / 10)
  end
  if not valid_uint(result) then return nil end
  return result
end

local function less_or_equal(left, right)
  if string.len(left) ~= string.len(right) then return string.len(left) < string.len(right) end
  return left <= right
end

local attempts = current(KEYS[1])
local bytes = current(KEYS[2])
if attempts == nil or bytes == nil then
  return redis.error_reply("CWM_VALIDATION malformed budget")
end
local attemptLimit = ARGV[1]
local byteLimit = ARGV[2]
local byteDelta = ARGV[3]
if not valid_uint(attemptLimit) or attemptLimit == "0" or not valid_uint(byteLimit) or byteLimit == "0" or not valid_uint(byteDelta) then
  return redis.error_reply("CWM_VALIDATION invalid budget arguments")
end
local newAttemptValue = add_uint(attempts, "1")
local newByteValue = add_uint(bytes, byteDelta)
if newAttemptValue == nil or newByteValue == nil or not less_or_equal(newAttemptValue, attemptLimit) or not less_or_equal(newByteValue, byteLimit) then
  return {0, attempts, bytes}
end
local newAttempts = redis.call("INCRBY", KEYS[1], 1)
redis.call("EXPIREAT", KEYS[1], ARGV[4])
local newBytes = bytes
if byteDelta ~= "0" then
  newBytes = redis.call("INCRBY", KEYS[2], byteDelta)
  redis.call("EXPIREAT", KEYS[2], ARGV[4])
end
return {1, newAttempts, newBytes}
`
