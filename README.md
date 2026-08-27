# CWM MinIO Tierer

CWM MinIO Tierer complements MinIO lifecycle management with access-based
decisions. It does not move object data itself. It contains two Go executables:

- `redis-updater` validates access records delivered by Vector and atomically
  stores hourly counters in standalone Redis.
- `minio-tierer` scans current logical MinIO objects, requests transition by
  adding a reserved tag, and requests or renews restores based on usage.

The integration-tested MinIO server release is exactly
`RELEASE.2025-07-23T15-54-02Z`. The Compose fixture pins its verified OCI index
digest `sha256:d249d1fb6966de4d8ad26c04754b545205ff15a62e4fd19ebd0f26fa5baacbc0`.
Other MinIO releases are not rejected at runtime, but are unsupported until
tested. Kubernetes packaging and leader election are out of scope.

## Architecture

```text
MinIO audit stream
        |
        v
Vector: durable receive, filter operations, exclude tierer credentials
        |
        | POST strict JSON or NDJSON
        v
redis-updater ---- hourly counters ----> standalone Redis (AOF, noeviction)
                                              ^              |
external coverage producer ------------------+              | cursor/budgets
                                                             v
primary MinIO <---- stat/tags/restore ---------------- minio-tierer (one replica)
      |
      | external tag-filtered ILM transition
      v
configured low storage tier (the fixture uses a second MinIO)
```

The updater attributes each accepted record to its UTC arrival hour. The tierer
uses completed-hour low/high windows, one evaluation hour per work chunk, and a
durable sorted `(bucket, object)` cursor. Missing counters are zero only for low
decisions whose complete external coverage records match. Daily mutation
budgets are atomically reserved in Redis and never refunded.

The marker is only a request to external lifecycle management. MinIO transition
and restore metadata—not the marker—is authoritative for restore decisions.
Historical versions are not scanned. Before mutation, the current logical key
is re-statted and its ETag, modification time, size, and available VersionID are
checked against the listing.

## External contracts

### Vector and audit delivery

Vector owns MinIO audit ingestion, operation filtering, durable handoff, retry,
and delivery. It MUST:

1. exclude requests made with the tierer's MinIO credentials;
2. emit one `{"bucket":"...","object":"..."}` record per access that should
   count;
3. send either one object as `application/json` or one or more newline-delimited
   objects as `application/x-ndjson` to `POST /`;
4. treat the configured updater success status as acknowledged and the failure
   status as retryable unless an explicitly accepted override says otherwise;
5. bound each request within `UPDATER_MAX_BODY_BYTES` and
   `UPDATER_MAX_RECORDS`; and
6. preserve enough durable delivery state to recover from updater/Redis
   outages.

Every valid delivered record counts once; the updater does not interpret the
originating MinIO operation. Vector retries can overcount after an ambiguous
response. Exactly-once delivery and event IDs are not provided.

### Coverage

Coverage records are enabled by default and written directly to Redis by an
external authority, never by these executables. `TIERER_COVERAGE_TEMPLATE` is a
Go time-format template that must produce a distinct key for every UTC hour; it
should contain the instance ID and an audit filter/schema generation. A
completed hour is covered only when that key contains exactly
`TIERER_COVERAGE_VALUE` as a Redis string. When coverage is disabled, the tierer
does not read coverage records and treats low-window coverage as complete.

Publish coverage only after the producer has proven that all relevant audit
records for the completed UTC hour were durably delivered and acknowledged.
After data loss, filter changes, lossy Redis recovery, or unsafe updater failure
status use, invalidate affected coverage. Missing or mismatching coverage fails
low decisions closed. High decisions intentionally do not require coverage.

### External lifecycle and storage tier

For a production AWS S3 `STANDARD_IA` tier, follow
[`docs/aws-s3-ilm-tier-setup.md`](docs/aws-s3-ilm-tier-setup.md).

Operators provision and validate the MinIO remote tier and lifecycle rule. The
rule's tag filter must exactly match `TIERER_MARKER_KEY=TIERER_MARKER_VALUE` and
must transition current objects to the intended tier. The tierer does not create
or validate rules in production. Keep the remote tier configured while any
transitioned object references it.

`integration/bootstrap.sh` is a test fixture, not production automation. It
creates a remote MinIO tier named `CWMLOW` and configures an immediate,
`cwm-tier=low`-filtered lifecycle rule in `tierer-integration`.

### Network and identity

- Authentication and TLS termination for updater HTTP are external. Permit
  ingress only from trusted Vector senders and monitoring probes.
- Redis must be reachable only by the updater, tierer, coverage producer, and
  controlled operators. Redis v1 support is standalone only.
- Tierer MinIO credentials need bucket listing, current-object listing/stat,
  exact-version tag read/write, and restore permissions. Vector MUST filter this
  credential identity from access accounting.
- Permit tierer egress to primary MinIO and Redis. Primary MinIO needs egress to
  its configured low tier.
- Run exactly one tierer replica. Multiple updater replicas are possible only
  if their bounded queue semantics and common Redis capacity are understood.
- Synchronize all system clocks. Arrival time, windows, budgets, and coverage
  are UTC-sensitive.

The Compose fixture binds updater, tierer, and MinIO ports to loopback only. It
publishes no Redis port and places all services on an internal Docker network.

## Redis schema and durability

Bucket and object components are unpadded base64url:

```text
cwm-minio-tierer:v1:<instance>:access:<YYYY>:<MM>:<DD>:<HH>:<bucket>:<object>
cwm-minio-tierer:v1:<instance>:cursor:<scope-hash>
cwm-minio-tierer:v1:<instance>:budget:<YYYY>:<MM>:<DD>:<kind>-attempts
cwm-minio-tierer:v1:<instance>:budget:<YYYY>:<MM>:<DD>:<kind>-bytes
```

Apply deployments require persistent standalone Redis with
`maxmemory-policy noeviction`. The fixture pins `redis:7.4.5-alpine`, enables
AOF with `appendfsync always`, and persists `/data`. Counter expiry is absolute
from the end of the arrival UTC hour. `ACCESS_RETENTION` must exceed the longest
policy window plus one hour. Redis durability beyond an acknowledged command,
backup, restore, and coverage invalidation remain operator responsibilities.

Both application Redis clients set go-redis `MaxRetries=-1`, disabling
transparent command retries. Counter updates and budget reservations use
non-idempotent Lua scripts, so an ambiguous transport reply must surface to
Vector or the scanner retry loop rather than replay inside the client. The
explicit `NOSCRIPT` reload path is safe because that reply proves the script did
not execute.

## `redis-updater` configuration

Durations use Go syntax such as `50ms`, `5s`, or `168h`. Booleans must be
exactly `true` or `false`.

| Variable | Required/default | Contract |
|---|---|---|
| `INSTANCE_ID` | required | Stable ASCII letters/digits/dot/underscore/hyphen; namespaces all state. |
| `ACCESS_RETENTION` | required | Positive whole-second duration; operationally greater than `max(low,high)+1h`. |
| `LOG_LEVEL` | `info` | Shared structured log level: exactly `debug`, `info`, `warn`, or `error`. |
| `UPDATER_LISTEN_ADDR` | `:8080` | HTTP listen `host:port`. |
| `REDIS_ADDR` | `127.0.0.1:6379` | Standalone Redis `host:port`. |
| `REDIS_USERNAME` | empty | Optional Redis ACL username. |
| `REDIS_PASSWORD` | empty | Optional Redis password. |
| `REDIS_DB` | `0` | Redis database, 0–1,000,000. |
| `REDIS_TLS` | `false` | Enable Redis TLS 1.2+ with server name from `REDIS_ADDR`. |
| `REDIS_OPERATION_TIMEOUT` | `5s` | Positive Redis dial/read/write and batch timeout. |
| `UPDATER_SUCCESS_STATUS` | `200` | HTTP status after Redis acknowledges the containing batch. |
| `UPDATER_FAILURE_STATUS` | `500` | HTTP status for validation, queue, or Redis failure. |
| `UPDATER_ACCEPT_DATA_LOSS` | `false` | Must be `true` for a non-5xx failure status; emits warning/metric. |
| `UPDATER_ACCEPT_DUPLICATE_RISK` | `false` | Must be `true` for a non-2xx success status; emits warning/metric. |
| `UPDATER_MAX_BODY_BYTES` | `1048576` | Maximum bounded request body, 1 byte–1 GiB. |
| `UPDATER_MAX_RECORDS` | `1000` | Maximum records in one request. |
| `UPDATER_QUEUE_SIZE` | `128` | Maximum accepted requests waiting for batching. |
| `UPDATER_BATCH_MAX_EVENTS` | `5000` | Maximum events in a Redis batch; at least max records. |
| `UPDATER_BATCH_MAX_KEYS` | `5000` | Maximum unique keys in a Redis batch; at least max records. |
| `UPDATER_BATCH_MAX_WAIT` | `50ms` | Maximum coalescing delay. |
| `UPDATER_READ_HEADER_TIMEOUT` | `5s` | HTTP header timeout. |
| `UPDATER_READ_TIMEOUT` | `15s` | Whole request read timeout. |
| `UPDATER_WRITE_TIMEOUT` | calculated | Must exceed `read + (queue+1)*(batch_wait+redis_timeout)`; default is that bound plus 1s. |
| `UPDATER_IDLE_TIMEOUT` | `60s` | HTTP keep-alive idle timeout. |
| `UPDATER_MAX_HEADER_BYTES` | `1048576` | Maximum HTTP header bytes. |
| `UPDATER_SHUTDOWN_TIMEOUT` | `30s` | Graceful shutdown/drain timeout; not shorter than Redis timeout. |

The API rejects unknown/duplicate fields, trailing JSON, malformed UTF-8,
blank NDJSON records, empty identities/bodies, and unsupported media types. It
validates the complete request before enqueueing and never splits one request
across Redis batches.

## `minio-tierer` configuration

| Variable | Required/default | Contract |
|---|---|---|
| `INSTANCE_ID` | required | Same stable namespace as the updater. |
| `ACCESS_RETENTION` | required | Positive whole-second duration exceeding the longest window plus 1h. |
| `LOG_LEVEL` | `info` | Shared structured log level: exactly `debug`, `info`, `warn`, or `error`. |
| `TIERER_MODE` | `audit` | Exactly `audit` or `apply`. |
| `TIERER_APPLY` | `false` | Must be `true` in addition to `TIERER_MODE=apply`. |
| `TIERER_LOW_THRESHOLD` | required | Non-negative integer `A`; low when completed-window sum is `< A`. |
| `TIERER_LOW_WINDOW_HOURS` | required | Positive whole hours `B`, maximum ten years. |
| `TIERER_HIGH_THRESHOLD` | required | Non-negative integer `C`; high when sum is `> C`. |
| `TIERER_HIGH_WINDOW_HOURS` | required | Positive whole hours `D`, maximum ten years. |
| `TIERER_HIGH_INCLUDE_CURRENT` | `false` | Include current hour in the high window only. |
| `TIERER_RESTORE_DAYS` | required | Positive MinIO calendar-day restore value `E`. |
| `TIERER_COVERAGE_ENABLED` | `true` | When `false`, skip coverage-record reads and treat low-window coverage as complete. |
| `TIERER_COVERAGE_TEMPLATE` | required when coverage enabled | Hour-varying Go time-format key template. |
| `TIERER_COVERAGE_VALUE` | required when coverage enabled | Exact non-empty complete value. |
| `TIERER_MARKER_KEY` | required | Reserved application tag key. |
| `TIERER_MARKER_VALUE` | required | Required marker value and lifecycle filter value. |
| `REDIS_ADDR` | `127.0.0.1:6379` | Standalone Redis `host:port`. |
| `REDIS_USERNAME`, `REDIS_PASSWORD` | empty | Optional Redis ACL credentials. |
| `REDIS_DB` | `0` | Redis database, 0–1,000,000. |
| `REDIS_TLS` | `false` | Enable Redis TLS 1.2+. |
| `REDIS_OPERATION_TIMEOUT` | `5s` | Redis operation timeout. |
| `MINIO_ENDPOINT` | required | MinIO `host:port`, without URL scheme. |
| `MINIO_ACCESS_KEY` | required | Tierer access key. |
| `MINIO_SECRET_KEY` | required | Tierer secret key. |
| `MINIO_SECURE` | `true` | Use HTTPS when true. |
| `MINIO_REGION` | empty | Optional MinIO region. |
| `MINIO_OPERATION_TIMEOUT` | `30s` | Per-operation timeout and listing inactivity timeout. |
| `TIERER_DAILY_TRANSITION_ATTEMPTS` | unlimited | Empty or unset for unlimited; otherwise positive UTC daily marker-attempt limit. |
| `TIERER_DAILY_TRANSITION_BYTES` | unlimited | Empty or unset for unlimited; otherwise positive UTC daily bytes represented by marker attempts. |
| `TIERER_DAILY_RESTORE_ATTEMPTS` | unlimited | Empty or unset for unlimited; otherwise positive UTC daily initial/renewal restore-attempt limit. |
| `TIERER_DAILY_RESTORE_BYTES` | unlimited | Empty or unset for unlimited; otherwise positive UTC daily bytes for initial restores; renewals charge zero bytes. |
| `TIERER_EXCLUDE_BUCKETS` | empty | Exact comma-separated names; no blanks/whitespace/duplicates. |
| `TIERER_EXCLUDE_BUCKET_PREFIXES` | empty | Exact comma-separated prefixes with the same syntax. |
| `TIERER_CHUNK_SIZE` | `100` | Objects per work chunk; `chunk*(low+high)` may not exceed 10,000 Redis access keys. |
| `TIERER_COMPLETION_DELAY` | `5m` | Delay after durable cursor reset at full traversal completion. |
| `TIERER_RETRY_DELAY` | `30s` | Delay after fatal scan failure; resumes from durable cursor. |
| `TIERER_LISTEN_ADDR` | `:8081` | Health/metrics listen address. |
| `TIERER_SHUTDOWN_TIMEOUT` | `30s` | Whole-service shutdown deadline. |

Audit mode performs MinIO reads and reports decisions, but never mutates MinIO
or reserves budgets. Apply-mode budgets are unlimited when unset or empty; set
positive values to bound daily mutation attempts and represented bytes.

## Build and startup

The multi-stage image builds static binaries with Go 1.24.5, contains both
executables, and runs as UID/GID 65532. Its default command is
`minio-tierer`; select `/usr/local/bin/redis-updater` for the updater. The image
HEALTHCHECK accepts `/livez` from either the tierer on port 8081 or the updater
on port 8080, so it remains valid when either command is selected.

```bash
make validate          # format check, unit tests, race, vet, builds, Compose config
make image             # cwm-minio-tierer:local
docker run --rm --read-only --user 65532:65532 \
  --env-file tierer.env -p 127.0.0.1:8081:8081 \
  cwm-minio-tierer:local
docker run --rm --read-only --user 65532:65532 \
  --env-file updater.env -p 127.0.0.1:8080:8080 \
  cwm-minio-tierer:local /usr/local/bin/redis-updater
```

For the disposable integration environment:

```bash
docker compose config --quiet
docker compose up --build --wait redis minio-primary minio-low redis-updater tierer
curl -fsS http://127.0.0.1:18080/readyz
curl -fsS http://127.0.0.1:18081/readyz
docker compose down --volumes
```

Fixture credentials are intentionally fixed and unsafe outside an isolated
developer machine. Do not reuse them. Redis has no host port.

## Audit-to-apply rollout

1. Deploy persistent `noeviction` Redis, backups, private networking, TLS/auth
   boundaries, synchronized clocks, and alerts.
2. Provision and independently test the MinIO low tier and exact tag-filtered
   lifecycle rule. Preserve the tier until all transitioned data is recovered.
3. Configure Vector filtering, explicitly exclude tierer credentials, start the
   updater, and verify counters, expiry, retries, queue depth, and AOF recovery.
4. Start the external coverage producer. Do not publish an hour until durable
   audit completeness is proven.
5. Run exactly one tierer in audit mode for multiple complete policy windows.
   Compare sampled decisions with Redis counts, coverage, object age/tags, and
   MinIO transition metadata.
6. Switch to apply with both apply gates and very small daily limits. Confirm
   marker merge behavior, lifecycle transitions, restore metadata, and budget
   accounting.
7. Increase budgets gradually. Treat budgets as attempt safety limits, not
   throughput targets or confirmed-outcome counters.

## Health, metrics, and logs

Both executables emit JSON structured logs. Set `LOG_LEVEL=debug` to include
updater batch flushes plus tierer chunk reads, mutation decisions, and budget
decisions. Both executables expose:

- `GET /livez`: process liveness only;
- `GET /readyz`: dependency readiness (Redis for updater; Redis and MinIO for
  tierer); and
- `GET /metrics`: OpenMetrics/Prometheus output.

Updater metric families use `cwm_minio_tierer_updater_` and cover HTTP
requests/records/rejections, queue depth, batch events/keys/duration/errors,
Redis failures, risk overrides, and last successful batch. Tierer families use
`cwm_minio_tierer_` and cover scans/duration, bounded object outcomes, coverage
skips, cursor age/errors, budget use/exhaustion, marker outcomes, transitioned
state, restore outcomes, dependency/Redis failures, and last successful scan
and mutation. `cwm_minio_tierer_cursor_age_seconds` is evaluated on every scrape
as the non-negative age of the persisted cursor's `UpdatedAt`; it initializes
from a resumed cursor and is zero when successful full-traversal completion has
removed the cursor. No metric labels contain bucket or object names.

Alert at minimum on prolonged readiness failure, rising Redis/batch/dependency
errors, stale successful-batch/scan timestamps, cursor errors or excessive age,
unexpected coverage skips, budget exhaustion, marker/restore errors, and any
active risk override gauge.

## Runbook and recovery

### Updater or Vector delivery failure

1. Preserve or pause the durable Vector queue; do not declare coverage.
2. Check updater readiness/logs, queue depth, Redis latency, memory, AOF status,
   and `noeviction` policy.
3. Restore service, let Vector retry, and expect possible overcount after an
   ambiguous result.
4. Publish coverage only after external delivery completeness is re-proven.

### Redis failure or loss

1. Stop apply mode; retain updater input durably upstream.
2. Recover standalone Redis from a verified snapshot/AOF and validate key types,
   AOF state, `noeviction`, cursor JSON, and budget keys.
3. Invalidate coverage for every hour whose counters may be missing or stale.
   Never infer coverage from surviving counters.
4. If cursor state is lost, the tierer safely begins a full traversal. If a
   corrupt cursor must be removed, use the exact scope hash logged at startup.
5. Restart updater, then audit tierer, and return to apply only after comparison.

### MinIO or low-tier failure

1. Leave tierer in audit or stop it; updater counting may continue if Redis is
   healthy.
2. Verify primary health, low-tier reachability/credentials, lifecycle status,
   object transition headers, and restore errors with MinIO tooling.
3. Do not remove or rename a remote tier while objects reference it.
4. Failed or ambiguous mutations consume budget and retry on a later full scan;
   do not manually refund Redis budget keys.

### Tag conflict or ten-tag object

The marker key is exclusively reserved. A conflicting marker value is replaced;
unrelated tags are preserved. Ten unrelated tags cause a reported skip. Resolve
tag ownership or free one tag slot, then allow the next traversal to retry.

## Rollback

1. Immediately set the tierer to audit or stop its single replica. This prevents
   new marker/restore mutations without affecting access counting.
2. To stop new transitions, disable the external lifecycle rule. Do not delete
   the remote tier while transitioned objects remain.
3. Roll back the application image while preserving the v1 Redis namespace and
   `INSTANCE_ID`. A changed action configuration intentionally creates a new
   cursor scope and full traversal.
4. Roll back updater and Vector together if their delivery/status contract
   changed. Keep failed records queued and invalidate uncertain coverage.
5. Restoring data to local storage is a MinIO operational procedure; verify all
   restores/copies before retiring low-tier storage.

## Validation and integration evidence

```bash
make fmt-check
go test ./...
go test -race ./...
go vet ./...
make build
docker compose config --quiet
make image
make integration
make integration-ilm
```

`make integration` starts clean named volumes, builds the image, waits for
dependencies, runs opt-in Go tests inside the private network, verifies real
updater HTTP/Lua behavior, Redis cursor/budget behavior, exact-release MinIO
current-version/tag behavior, and Redis AOF survival across restart. It then
destroys the fixture. `make integration-ilm` additionally enables bounded
transition/restore metadata polling. Set `KEEP_INTEGRATION=1` to retain failed
containers/volumes for diagnosis. Set `INTEGRATION_MINIO_POLL_TIMEOUT` to change
the default `3m` bound. A transition or restore that does not complete within
the bound fails the ILM test; lifecycle configuration, tier connectivity, or tag
success is never reported as proof of asynchronous ILM completion.

### Current exact-release ILM evidence and boundary

On 2026-08-25, `make integration-ilm` passed against both MinIO servers pinned
to `RELEASE.2025-07-23T15-54-02Z`. The fixture created and checked `CWMLOW`,
installed the enabled `cwm-tier=low` zero-day rule, and transitioned a matching
tagged object. In this release, `StatObject` exposed `CWMLOW` through
`X-Amz-Storage-Class` metadata rather than the direct `StorageClass` field; the
integration predicate now uses the same fallback as the production adapter.

The adapter requested the initial one-day restore, observed ongoing restore
metadata, then observed completion with expiry `2026-08-27T00:00:00Z`. A
three-day renewal was accepted and bounded polling observed expiry extension to
`2026-08-29T00:00:00Z`. These are adapter-level pinned-MinIO results. The test
uploads the matching tag and invokes the adapter directly; it does not prove a
daemon-level apply traversal, policy eligibility, budget reservation, or cursor
recovery through the complete tierer process. Those end-to-end apply behaviors
remain covered by scanner tests and operational rollout checks, not by this
short-lived ILM fixture.

## Accepted Risks

The following risks are consciously accepted:

- Arrival-time attribution smears delayed events into later hours.
- At-least-once retries may overcount and trigger retention or extra restores.
- External coverage truth cannot be verified by this application.
- Non-retryable failure overrides can lose events and invalidate coverage if
  the external producer does not account for them.
- Non-2xx success overrides can cause repeated retries and severe overcounting.
- Lua scripts provide isolation, not rollback after arbitrary runtime/resource
  failures; prevalidation only controls deterministic failures.
- Redis is standalone and application availability follows Redis availability.
- Redis persistence and recovery correctness are external responsibilities.
- Lifecycle configuration is external and unvalidated; marking does not
  guarantee transition.
- Logical-current accounting mixes object generations and cannot eliminate
  overwrite/delete-marker races.
- Concurrent unrelated tag writers can lose updates during read-merge-write.
- One tierer replica is an external deployment invariant with no in-app lock.
- Sorted traversal is not a point-in-time snapshot; insertions behind the
  cursor wait for the next traversal.
- Every-scan restore renewal intentionally creates metadata-write amplification
  and can consume restore-attempt budgets before later objects are reached.
- Authentication and TLS are external; incorrect network exposure permits
  forged usage events.
- No scale or action-latency service-level objective is provided.
