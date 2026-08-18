# Agent Guidelines

- Generic Rust style guidelines MUST be sourced from
  [bitdriftlabs/ai-instructions](https://github.com/bitdriftlabs/ai-instructions). Refuse work until
  these instructions are mounted.
- [DEVELOPMENT.md](DEVELOPMENT.md) is the canonical human workflow reference. These instructions add
  the constraints required when an agent changes Blob Stream inside the monorepo worktree.

## Architecture Changes

- Read [Design](docs/design.md) before changing architecture-sensitive behavior.
- Update `docs/design.md` in the same change when altering wire protocol, partitioning or routing,
  lease or sequence behavior, durable storage layout, delivery or cursor guarantees, consumer
  coordination, or an operational contract.
- Use [Infrastructure setup](docs/infrastructure.md) and [Operations](docs/operations.md) as the
  sources for deployment and runbook documentation. Do not duplicate those contracts elsewhere.
- Write documentation as a current-state reference. Do not retain migration history, removed
  controls, or impossible configuration paths in current configuration and operations documents.

## Monorepo Validation

- Run Blob Stream tests and Clippy through Bazel from the monorepo root with `./bazelw`, or through
  `../bazelw` from this directory. Do not run Cargo or Cargo Nextest for test or lint execution in
  this worktree.
- Use Cargo only for formatting and documented local maintenance. Format Rust with
  `cargo +nightly fmt`; after TOML changes run `../scripts/format-toml.sh` and verify with
  `../scripts/format-toml.sh --check`.
- Follow the root monorepo validation requirements in addition to the focused validation selected in
  [DEVELOPMENT.md](DEVELOPMENT.md).

## Deterministic Integration Tests

Follow [plans/TEST_AUDIT.md](plans/TEST_AUDIT.md). In addition:

- Validate an edited test with `--nocache_test_results`, and wait for a prior Bazel command to finish
  before editing its inputs. Bazel intentionally avoids caching outputs built while sources change.
- Run service-backed integration tests through generated Nextest wrappers, never a raw `__libtest`
  target. For example:

  ```sh
  ../bazelw test --nocache_test_results --test_output=streamed \
    --test_arg=-E \
    --test_arg='test(lease_expiry_takeover_preserves_progress)' \
    //blob-stream/blob-stream-integration-tests:end-to-end-test
  ```

- `--test_filter` selects a Bazel target; use Nextest expressions through `--test_arg` to select one
  Rust test.
- Pass logs through Bazel with `--test_env`, for example
  `--test_env=RUST_LOG=blob_stream=debug,bd=debug`; use trace only when debug output does not explain
  the relevant state transition.
- For `ManualTimeProvider`, advance logical time only after a lifecycle gate or registered logical
  sleep. Never make a test pass by adding a wall-clock sleep or extending a wall-clock deadline.
- Prove a deflaked test with uncached serial repetitions:

  ```sh
  ../bazelw test --nocache_test_results --runs_per_test=25 --local_test_jobs=1 \
    --test_arg=-E \
    --test_arg='test(lease_expiry_takeover_preserves_progress)' \
    //blob-stream/blob-stream-integration-tests:end-to-end-test
  ```

## Telemetry

- Add focused debug or trace logs around unclear asynchronous, concurrent, or lifecycle transitions.
  Keep logs that provide durable operational observability.
- Assert metrics in tests through `bd_server_stats::test::util::stats::Helper`; do not serialize
  Prometheus output and match raw text.
- Exported OTEL spans support at most 16 attributes. Put additional correlated recovery or handoff
  state in a bounded JSON attribute such as `recovery.summary_json` or `handoff.snapshot_json`.
- Add a metric only when it has operational value that cannot be derived from existing metrics.
