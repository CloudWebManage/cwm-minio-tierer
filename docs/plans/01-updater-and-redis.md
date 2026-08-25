# Shared Contracts And Redis Updater Plan

**Depends on:** `docs/specification.md`

## Status

- Completed: 2026-08-25
- Review findings closed in the shared workspace:
  `/home/ori/workspace/cwm-minio-tierer`
- Git history operations: none

## Review Closure

- The HTTP write timeout invariant is:
  `write > read + (queue_size + 1) * (batch_wait + redis_timeout)`.
  `queue_size` covers every queued request, `+1` covers the active or pending
  batch, and each request is conservatively allowed to require its own batch.
  When unset, the write timeout is the calculated bound plus one second.
- Shutdown first stops HTTP acceptance, then gracefully drains the batcher.
  If the drain deadline expires, the batch operation context is canceled,
  queued waiters receive a retryable failure, and shutdown joins the batcher
  goroutine before closing Redis.
- Queue accounting increments before channel handoff and serializes observer
  updates with dequeue accounting, preventing negative or stale depth.
- `ACCESS_RETENTION` must use whole seconds because Redis expiry uses
  `EXPIREAT`.
- A second `NOSCRIPT` after script reload is a deterministic non-write error.
- The updater configures go-redis with `MaxRetries=-1`; effective client retries
  normalize to zero. This prevents transparent replay of non-idempotent counter
  batches and returns ambiguous replies to the HTTP/Vector contract.
- Risk warnings and gauges indicate active unsafe status overrides, not merely
  acknowledgment flags supplied alongside safe statuses.
- Failed Redis readiness pings increment the Redis failure counter.

## Accepted Residuals

- Request cancellation after enqueue does not cancel the accepted batch. The
  client may receive a failure while Redis later acknowledges the batch;
  retrying can overcount as already accepted by the specification.
- On a shutdown deadline, joining a dependency call after canceling its
  context can extend slightly beyond the configured deadline. Redis is not
  closed until that goroutine exits.
- Real standalone Redis restart, persistence, and transport-failure coverage
  remains assigned to Plan 03 integration testing.

## Final Review TDD Evidence

- RED: the updater option used `MaxRetries=0`, which go-redis normalized to its
  default three retries. GREEN: focused option/client assertions observe the
  required `-1` disable sentinel and effective zero retries.

## Validation Evidence

- `gofmt -l cmd internal`: no output.
- `go test ./... -count=1`: all packages passed.
- `go test -race ./... -count=1`: all packages passed under the race detector.
- `go vet ./...`: no output.
- Both `CGO_ENABLED=0 go build -buildvcs=false -mod=readonly -trimpath`
  commands completed successfully for `redis-updater` and `minio-tierer` under
  workspace `.build`.
- `docker compose config --quiet`: no output.

1. Initialize the Go module and pin Go, Redis client, MinIO SDK, Prometheus,
   and test dependencies.
2. Test-first implement object identity encoding, UTC access keys, hour-window
   helpers, coverage-template rendering, and strict environment validation.
3. Test-first implement strict JSON/NDJSON decoding and request limits.
4. Test-first implement the bounded concurrent request batcher with waiter
   result propagation and graceful drain.
5. Test-first implement the prevalidated whole-batch Lua counter/expiry store,
   script loading, `NOSCRIPT` recovery, and ambiguous error handling.
6. Implement the HTTP server, configurable status/risk gates, health, metrics,
   structured logging, and `cmd/redis-updater`.
7. Verify focused tests after each red/green cycle, then run updater package
   race tests and binary build.
