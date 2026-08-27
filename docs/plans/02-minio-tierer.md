# MinIO Tierer Plan

**Depends on:** `docs/specification.md` and shared contracts from Plan 01.

## Status

- Completed: 2026-08-25
- Execution workspace: `/home/ori/workspace/cwm-minio-tierer`
- Branch/worktree: none; the user explicitly required direct workspace changes.
- Git history operations: none.
- Exact-release MinIO transition/restore integration remains assigned to Plan
  03 because its pinned two-MinIO Compose fixture is not available in Plan 02.
- Review findings fixed in the same direct workspace on 2026-08-25, with no
  branch, worktree, commit, or other Git history operation.
- Final two Important review findings closed on 2026-08-25 in the same direct
  workspace, again with no Git or worktree operation.

## Implemented Rulings

- Audit is the default. Apply requires both `TIERER_MODE=apply` and
  `TIERER_APPLY=true`; daily limits are unlimited when unset or empty. Audit may
  perform MinIO reads needed to report intended outcomes, but never mutates
  MinIO or reserves budget.
- Evaluation configuration is required in both modes so an audit exercises the
  exact apply policy. Thresholds are non-negative integers; window values are
  positive whole-hour integers; high current-hour inclusion defaults false.
- Required tierer environment names are `TIERER_LOW_THRESHOLD`,
  `TIERER_LOW_WINDOW_HOURS`, `TIERER_HIGH_THRESHOLD`,
  `TIERER_HIGH_WINDOW_HOURS`, `TIERER_RESTORE_DAYS`,
  `TIERER_COVERAGE_TEMPLATE`, `TIERER_COVERAGE_VALUE`,
  `TIERER_MARKER_KEY`, and `TIERER_MARKER_VALUE`. MinIO uses an endpoint
  without a URL scheme plus explicit credentials and secure-mode selection.
- Missing access counters decode as zero. A wrong Redis type, negative or
  non-canonical integer, or int64 overflow is fatal. Missing or nonmatching
  string coverage records mean incomplete coverage; wrong coverage key types
  are fatal.
- Budget checks use exact decimal int64 arithmetic in one Lua execution for
  attempt and byte keys. UTC day selection is taken at reservation/mutation
  time, not from the chunk evaluation hour. Successful reservations are never
  refunded. Renewal reserves one restore attempt and zero bytes.
- Cursor values are strict version-1 JSON under the specification namespace.
  The full SHA-256 scope includes instance-separated action-affecting policy,
  mode, endpoint, marker, coverage, exclusions, budgets, and chunking. Bucket
  and object traversal is lexicographically sorted; a non-monotonic MinIO list
  is fatal.
- A non-empty, non-`STANDARD` storage class or restore metadata means
  transitioned. Listing metadata is used when conclusive; otherwise Stat is
  the fallback. Pre-action identity validation stats the current logical key
  without a VersionID, compares ETag, LastModified, size, and any listed
  VersionID, then targets the freshly captured current VersionID exactly.
- Tag reads, writes, and verification target the exact available VersionID.
  Matching markers are no-ops, reserved-value conflicts are replaced, unrelated
  tags are retained, and ten unrelated tags produce a reported skip.
- `RestoreAlreadyInProgress` is a pending object outcome, not a fatal scan
  error. Completed active restores renew on every high-qualified traversal;
  expired or absent restores initiate with configured calendar days. Active
  versus expired is evaluated at the actual chunk time, while access windows
  continue to use the UTC-truncated evaluation hour.
- Listing, usage, coverage, budget, cursor, and malformed-state failures stop
  the affected chunk without cursor advancement. Stat, tag, and restore
  failures are isolated object outcomes and advance after the chunk completes.
  Directory markers and excluded buckets are skipped. Fatal scans retry from
  the last durable cursor after `TIERER_RETRY_DELAY`; completed traversals reset
  the cursor before the completion delay. Scans never overlap and cancellation
  interrupts scanning, retry waits, and completion waits.
- Each scanner chunk issues one fail-closed Redis count script for all object
  low/high keys and one coverage script for its evaluation hour. Wrong types,
  malformed values, and overflow anywhere in the chunk fail the entire chunk.
- Structured dependency errors use bounded dependency/operation fields;
  object logs additionally include bucket/object identity. Metrics never use
  identity labels and expose both bounded dependency failures and a dedicated
  Redis failure counter. Audit marker/restore decisions never advance the last
  successful mutation timestamp.
- MinIO uses bounded dial, TLS handshake, response-header, and per-operation
  contexts with one SDK attempt. Redis uses bounded dial/read/write/pool
  operations with transparent retries disabled (`MaxRetries=-1`) so ambiguous
  non-idempotent budget reservations reach scanner retry handling. End-to-end
  service shutdown returns at the single configured shutdown deadline even if
  a component ignores cancellation.
- MinIO object listing uses `MINIO_OPERATION_TIMEOUT` as an inactivity timeout,
  not a total listing deadline. The timer stops during chunk processing and
  resets after every received item, so indefinitely large healthy listings are
  allowed while a stalled channel fails the scan, preserves the durable cursor,
  and enters normal retry handling.
- Aggregate access reads have a fixed conservative ceiling of 10,000 Redis
  keys per chunk, calculated as `chunk_size * (low_hours + high_hours)` because
  the current script requests both windows even where hours overlap. Startup,
  scanner validation, and `ReadChunk` all enforce the same overflow-safe bound
  before allocation. Defaults use 5,000 keys; chunk 100 with two 24-hour windows
  uses 4,800.
- Marker key and value must both be non-empty. Low/high window hours have an
  explicit ten-year upper bound and checked duration conversion. Cursor scope
  hashing serializes typed fields and separate exclusion arrays, preventing
  exclusion/configuration collisions.
- Lifecycle rules and tier targets are neither created nor validated. The
  accepted risks and behavioral contract in `docs/specification.md` remain
  unchanged.
- Cursor age is collected continuously from the persisted cursor `UpdatedAt`,
  initialized on durable resume, clamped at zero for future timestamps, and
  cleared only after successful reset at full traversal completion.

## TDD Evidence

- Evaluator/config RED: missing `LoadConfig`, `Evaluate`, and decision symbols;
  focused tests then passed after implementation.
- Redis RED: missing store contracts; later exact-budget regression permitted a
  `9007199254740992 + 1` reservation and strict cursor regression accepted
  trailing JSON. Both regressions passed after decimal-string arithmetic and
  decode-to-EOF fixes.
- MinIO adapter RED: missing adapter/object contracts; focused adapter tests
  passed after implementation against a narrow SDK-compatible fake.
- Scanner RED: missing scanner contracts; later mutation-day regression charged
  the truncated prior UTC hour. Focused scanner tests passed after using
  mutation-time UTC for reservation.
- Observability RED: missing health/metrics contracts; later audit marker
  planning advanced the mutation timestamp. Focused tests passed after
  separating marker planning from confirmed mutation.
- Scope RED: audit/apply mode changes initially produced the same cursor scope;
  focused test passed after action-affecting configuration was included.

### Review Fix Red/Green Evidence

- Current-key identity RED: adapter Stat sent listed `VersionID="v1"`; overwrite
  regression observed the unsafe historical lookup. GREEN: Stat omits VersionID
  and returns the fresh current VersionID for exact tag/restore actions.
- Restore-time RED: a restore expired at 15:30 was classified active at chunk
  time 15:45 because the 15:00 evaluation hour was used, charging zero bytes.
  GREEN: actual chunk time classifies it expired and charges listed size.
- Chunk-read RED: scanner performed six count calls for three objects and Redis
  had no chunk API. GREEN: two chunks make two usage calls, each backed by one
  count Eval and one coverage Eval; wrong-type chunk data remains fatal.
- Retry RED: `Scanner.Run` returned its first fatal scan. GREEN: a transient
  Redis failure retries after the configured delay with the same durable
  `startAfter`, and cancellation exits.
- Logging/metrics RED: scanner config had no logger, readiness emitted no
  structured dependency log, and no dedicated Redis failure metric existed.
  GREEN: bounded structured logs and dependency/Redis counters are exercised
  without identity metric labels.
- Timeout RED: a blocking Stat exceeded 200ms and service/transport timeout
  APIs were absent. GREEN: per-operation timeout advances the isolated object,
  response-header waits are bounded, and uncooperative service shutdown returns
  at its deadline.
- Audit metric RED: observing an audit restore advanced successful mutation
  time. GREEN: decision observation and confirmed restore application are
  separate signals.
- Config/scope RED: empty marker values were accepted; 87,601-hour windows were
  accepted with large retention; flattened config/exclusion values collided.
  GREEN: explicit validation, checked bounded hours, and typed scope JSON close
  all three cases.
- Scanner safety coverage exercises real Redis budget TTL, wrong-type fatal
  behavior, exhaustion without mutation, cursor load/save failure preservation,
  durable resume, and action-error advancement before a later listing failure.
- Listing inactivity RED: a stream emitted one item and stalled until external
  cancellation, producing only one attempt. GREEN: it times out at the
  configured inactivity interval and retries with the unchanged `startAfter`;
  a separate listing running longer than the timeout remains healthy when each
  item arrives before the reset interval.
- Aggregate-key RED: startup accepted 10,032 keys, `ReadChunk` reached Redis for
  10,100 keys, and no overflow-safe count helper existed. GREEN: 9,984 and the
  exact 10,000 boundary are accepted, above-limit and integer-overflow inputs
  fail before allocation or Redis execution.
- Redis retry RED: tierer configured and effectively used one transparent
  retry. GREEN: option/client tests observe the `-1` disable sentinel and
  effective zero retries.
- Cursor-age RED: a resumed cursor updated ten seconds earlier still exported
  age zero, and the old gauge sampled only at save time. GREEN: fake-clock tests
  observe continuous 10-to-15-second growth without sleeps, resume
  initialization, future-time clamping, and zero after durable completion.

## Validation Evidence

- `gofmt -w cmd internal` and `gofmt -l cmd internal`: formatted with no files
  subsequently reported.
- `go test ./... -count=1`: all packages passed.
- `go test -race ./... -count=1`: all packages passed under the race detector.
- `go vet ./...`: completed with no diagnostics.
- Both `CGO_ENABLED=0 go build -buildvcs=false -mod=readonly -trimpath`
  commands for `./cmd/minio-tierer` and `./cmd/redis-updater`: completed
  successfully under workspace `.build`.
- `docker compose config --quiet`: completed with no diagnostics.
- Real transition, restore, versioned-current, and cursor-restart compatibility
  against MinIO `RELEASE.2025-07-23T15-54-02Z` is deferred to Plan 03; no claim
  of exact-release runtime compatibility is made from SDK fakes alone.
- Residual risk: the exact pinned MinIO release must still prove current-key
  Stat VersionID capture, list metadata fast paths, transition headers, restore
  expiry/renewal, and exact-version tag/restore behavior in Plan 03 Compose.

1. Spike and integration-test pinned MinIO metadata, tagging, transition, and
   restore behavior before finalizing the adapter.
2. Test-first implement the pure low/high evaluator and explicit object state
   decisions.
3. Test-first implement Redis chunk reads, coverage matching, four daily budget
   reservations, and resumable cursor storage.
4. Test-first implement the MinIO adapter for sorted current-object listing,
   stat identity, tag merge/verification, transition state, restore state,
   restore initiation, and renewal.
5. Test-first implement the sequential scanner, exclusions, bounded chunks,
   cursor rules, audit/apply modes, failure handling, and completion delay.
6. Implement metrics, logs, health, shutdown, and `cmd/minio-tierer`.
7. Run tierer package race tests, vet, and binary build.
