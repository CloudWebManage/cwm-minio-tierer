# Integration, Packaging, And Operations Plan

**Depends on:** Plans 01 and 02. CI and Compose scaffolding may start earlier.

## Status

- Implementation complete: 2026-08-25.
- Validation status: deterministic validation and the bounded exact-release
  adapter-level transition, initial restore, completion, and renewal test pass.
- Execution workspace: `/home/ori/workspace/cwm-minio-tierer`.
- Branch/worktree: none; the user explicitly required direct workspace changes.
- Git history operations: none.

## Implemented Rulings

- The production image uses digest-pinned Go 1.24.5 and Alpine 3.22.1 stages,
  builds static binaries, contains both executables, has a bounded default
  healthcheck that accepts either tierer port 8081 or updater port 8080, and runs
  as UID/GID 65532.
- Compose pins Redis 7.4.5 Alpine, enables AOF with `appendfsync always`, sets
  `noeviction`, persists `/data`, publishes no Redis port, and uses an internal
  network. Both MinIO servers use exact release
  `RELEASE.2025-07-23T15-54-02Z` and verified OCI index digest
  `sha256:d249d1fb6966de4d8ad26c04754b545205ff15a62e4fd19ebd0f26fa5baacbc0`.
- The pinned `mc` bootstrap creates and checks a MinIO-backed `CWMLOW` tier,
  enables versioning on the fixture bucket, and installs an enabled zero-day
  lifecycle rule filtered by `cwm-tier=low`. Removing all existing rules before
  adding the fixture is accepted because Compose uses an isolated test bucket.
- Compose runs one updater and exactly one audit-mode tierer. App and MinIO
  ports are loopback-bound for diagnostics; Redis remains private. Fixture
  credentials are deliberately static and documented as test-only.
- Real integration tests run inside the private network. They exercise updater
  JSON/NDJSON HTTP, duplicate aggregation, whole-batch wrong-type rejection,
  absolute expiry, `NOSCRIPT` reload, real Redis configuration, cursor reopen,
  wrong-type reads, concurrent atomic budgets, exact-release logical-current
  versioning and exact-version tag merge, service health/metrics, and AOF
  survival across a Redis restart.
- Exact-release testing established that current-object listing omits VersionID
  while current-key Stat supplies it. It also exposed ListObjects millisecond
  LastModified versus HTTP HEAD second precision. A test-first adapter fix now
  normalizes only this directional precision loss while preserving ETag, size,
  available VersionID, and all other timestamp-change checks.
- Asynchronous ILM polling is opt-in (`make integration-ilm`) and bounded. The
  deterministic integration target skips it. Transition detection follows the
  production adapter by checking `StorageClass` and then
  `Metadata["X-Amz-Storage-Class"]`.
- README is the operator contract for architecture, both configuration sets,
  Vector/coverage/lifecycle/network boundaries, startup, rollout, monitoring,
  recovery, rollback, validation, current ILM evidence, and all accepted risks.

## TDD And Integration Evidence

- Real exact-release metadata RED: current Stat and listing represented the same
  LastModified instant at different precision, so `SameIdentity` returned false.
  A focused unit regression also failed before implementation. GREEN: only a
  second-precision current Stat may match a subsecond listing in the same second;
  the focused `TestSameIdentity` set and the real metadata/tag test pass.
- Review healthcheck RED: image inspection showed only `8081/livez`, so an
  updater selected as the image command could never become healthy. GREEN: the
  inherited probe contains both liveness URLs; standalone containers selecting
  `/usr/local/bin/redis-updater` and `/usr/local/bin/minio-tierer` each reached
  Docker `healthy` without a Compose healthcheck override.
- Review ILM predicate RED: the metadata-fallback characterization did not
  compile because no production-equivalent transition-class helper existed in
  the integration test. GREEN: the helper checks the direct field and exact
  `X-Amz-Storage-Class` metadata fallback, and its focused test passes.
- The real updater test proves the prevalidated Lua script does not partially
  increment a valid key when another batch key has a Redis list type. It also
  proves duplicate aggregation, exact expiry, strict HTTP rejection, and script
  reload after `SCRIPT FLUSH` against Redis 7.4.5.
- The real Redis tierer test reopens a persisted cursor through a new store,
  permits exactly one of two concurrent reservations at a one-attempt limit,
  and rejects a wrong-type access key.
- The pinned MinIO metadata test creates two versions, observes only the logical
  current object, confirms the release's list-VersionID omission and Stat
  fallback, merges the reserved marker into the exact current version, preserves
  an unrelated tag, and confirms the historical version remains unchanged.
- `make integration` passed. Its verbose result had five passing tests, one
  explicitly skipped opt-in ILM test, and a passing Redis AOF restart check.
- Final review `make integration-ilm` passed in bounded polling. The tier check
  and zero-day tag-filtered rule succeeded, transition was identified as
  `CWMLOW` from response metadata, the one-day initial restore was accepted,
  ongoing state was observed, and completion produced expiry
  `2026-08-27T00:00:00Z`. A subsequent adapter restore request with three days
  extended metadata expiry to `2026-08-29T00:00:00Z`.

## Remaining Limitations

- The passing ILM test is adapter-level: it writes the matching tag directly and
  calls restore through `MinIOAdapter`. A daemon-level apply flow that waits for
  object age eligibility is not run in
  this short-lived fixture. Scanner unit tests cover incomplete coverage,
  budgets, dependency failures, cursor restart, audit non-mutation, and isolated
  action failures; real integration covers Redis/MinIO boundaries but is not
  represented as proof of the full scanner-policy-budget-mutation pipeline.

## Validation Evidence

- Base image, Redis, `mc`, and MinIO tags/digests were resolved from their
  registries. The MinIO tag resolves to the recorded multi-platform index and
  the running servers logged the exact required release.
- `docker compose config --quiet`: passed after final Compose changes.
- `make integration`: passed, including Redis AOF recovery after restart.
- `gofmt -w cmd internal` followed by `gofmt -l cmd internal`: no files
  reported.
- `go test ./...`: all five tested internal packages passed; both command
  packages correctly reported no test files.
- `go test -race ./...`: all packages passed under the race detector.
- `go vet ./...`: completed with no diagnostics.
- Final `make build` completed both
  `go build -buildvcs=false -mod=readonly -trimpath` commands successfully, with
  outputs under the workspace `.build` directory.
- `docker compose config --quiet`: completed with no diagnostics.
- `docker build --pull=false --target runtime -t cwm-minio-tierer:local .`:
  completed successfully. Image inspection reported `user=65532:65532`, the
  bounded dual-port healthcheck, and a runtime probe confirmed both binaries are
  executable by UID 65532. Standalone command-variant probes for both binaries
  reached Docker `healthy`.
- Final `make integration`: five tests passed, the opt-in ILM
  test explicitly skipped, and `Redis AOF restart persistence: passed`.
- Final `make integration-ilm`: six tests passed with no skips, including
  transition, initial restore, observed ongoing/completed state, and renewal
  expiry extension; Redis AOF restart persistence also passed.

1. Add pinned standalone Redis and two pinned MinIO services with persistent
   storage and an external lifecycle/tier bootstrap fixture.
2. Add updater-to-Redis integration tests for atomic batches, expiry, restart,
   wrong types, `NOSCRIPT`, and UTC boundaries.
3. Add exact-release MinIO tests for tags, transition detection, initial
   restore, ongoing restore, renewal, versioned logical-current behavior, and
   cursor restart.
4. Add end-to-end audit and apply flows, including incomplete coverage, budget
   exhaustion, missing lifecycle behavior, and dependency failures.
5. Add a non-root multi-stage Dockerfile containing both binaries, health
   checks, `.dockerignore`, and Compose services with exactly one tierer.
6. Replace the README concept with configuration reference, external contracts,
   accepted risks, runbook, rollout, rollback, and validation commands.
7. Run format, unit, race, vet, build, image, Compose validation, and available
   integration tests; record anything environment-dependent honestly.
