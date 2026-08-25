package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Increment struct {
	Key      string
	Delta    int64
	ExpireAt time.Time
}

type Store struct {
	client scriptClient

	loadMu sync.Mutex
	sha    string
}

func NewStore(client scriptClient) *Store {
	return &Store{client: client}
}

func (s *Store) Load(ctx context.Context) error {
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	return s.loadLocked(ctx)
}

func (s *Store) loadLocked(ctx context.Context) error {
	sha, err := s.client.ScriptLoad(ctx, counterScript)
	if err != nil {
		return fmt.Errorf("load Redis counter script: %w", err)
	}
	s.sha = sha
	return nil
}

func (s *Store) Apply(ctx context.Context, increments []Increment) error {
	keys, args, err := commandArguments(increments)
	if err != nil {
		return err
	}
	if s.currentSHA() == "" {
		if err := s.Load(ctx); err != nil {
			return err
		}
	}

	_, err = s.client.EvalSHA(ctx, s.currentSHA(), keys, args...)
	if isNoScript(err) {
		if err := s.Load(ctx); err != nil {
			return err
		}
		_, err = s.client.EvalSHA(ctx, s.currentSHA(), keys, args...)
	}
	if err == nil {
		return nil
	}
	if isNoScript(err) {
		return fmt.Errorf("Redis counter script unavailable after reload; batch was not executed: %w", err)
	}
	if strings.Contains(err.Error(), "CWM_VALIDATION") {
		return fmt.Errorf("Redis counter batch rejected: %w", err)
	}
	return &AmbiguousError{Err: err}
}

func (s *Store) currentSHA() string {
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	return s.sha
}

func commandArguments(increments []Increment) ([]string, []any, error) {
	if len(increments) == 0 {
		return nil, nil, errors.New("counter batch must not be empty")
	}
	keys := make([]string, 0, len(increments))
	args := make([]any, 0, len(increments)*2)
	seen := make(map[string]struct{}, len(increments))
	for _, increment := range increments {
		if increment.Key == "" {
			return nil, nil, errors.New("counter key must not be empty")
		}
		if _, exists := seen[increment.Key]; exists {
			return nil, nil, fmt.Errorf("duplicate counter key %q", increment.Key)
		}
		seen[increment.Key] = struct{}{}
		if increment.Delta <= 0 {
			return nil, nil, fmt.Errorf("counter delta for %q must be positive", increment.Key)
		}
		if increment.ExpireAt.Unix() <= 0 {
			return nil, nil, fmt.Errorf("counter expiry for %q must be after the Unix epoch", increment.Key)
		}
		keys = append(keys, increment.Key)
		args = append(args, increment.Delta, increment.ExpireAt.Unix())
	}
	return keys, args, nil
}

func isNoScript(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ")
}

type AmbiguousError struct {
	Err error
}

func (e *AmbiguousError) Error() string {
	return "Redis batch result is ambiguous: " + e.Err.Error()
}

func (e *AmbiguousError) Unwrap() error {
	return e.Err
}

func IsAmbiguous(err error) bool {
	var target *AmbiguousError
	return errors.As(err, &target)
}

const counterScript = `
local MAX_INT64 = "9223372036854775807"

local function valid_uint(value)
  if type(value) ~= "string" then
    return false
  end
  if value == "0" then
    return true
  end
  if string.match(value, "^[1-9][0-9]*$") == nil then
    return false
  end
  if string.len(value) < string.len(MAX_INT64) then
    return true
  end
  if string.len(value) > string.len(MAX_INT64) then
    return false
  end
  return value <= MAX_INT64
end

local function add_uint(left, right)
  local i = string.len(left)
  local j = string.len(right)
  local carry = 0
  local result = ""
  while i > 0 or j > 0 or carry > 0 do
    local a = 0
    local b = 0
    if i > 0 then
      a = tonumber(string.sub(left, i, i))
      i = i - 1
    end
    if j > 0 then
      b = tonumber(string.sub(right, j, j))
      j = j - 1
    end
    local sum = a + b + carry
    result = tostring(sum % 10) .. result
    carry = math.floor(sum / 10)
  end
  if not valid_uint(result) then
    return nil
  end
  return result
end

local sums = {}
for i, key in ipairs(KEYS) do
  local delta = ARGV[(i - 1) * 2 + 1]
  local expiry = ARGV[(i - 1) * 2 + 2]
  if not valid_uint(delta) or delta == "0" then
    return redis.error_reply("CWM_VALIDATION invalid delta")
  end
  if not valid_uint(expiry) then
    return redis.error_reply("CWM_VALIDATION invalid expiry")
  end

  local key_type = redis.call("TYPE", key)
  if type(key_type) == "table" then
    key_type = key_type["ok"]
  end
  local current = "0"
  if key_type == "string" then
    current = redis.call("GET", key)
  elseif key_type ~= "none" then
    return redis.error_reply("CWM_VALIDATION wrong key type")
  end
  if not valid_uint(current) then
    return redis.error_reply("CWM_VALIDATION malformed or negative counter")
  end
  sums[i] = add_uint(current, delta)
  if sums[i] == nil then
    return redis.error_reply("CWM_VALIDATION counter overflow")
  end
end

for i, key in ipairs(KEYS) do
  redis.call("INCRBY", key, ARGV[(i - 1) * 2 + 1])
  redis.call("EXPIREAT", key, ARGV[(i - 1) * 2 + 2])
end
return #KEYS
`
