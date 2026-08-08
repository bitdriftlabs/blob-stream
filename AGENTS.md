# Agent Guidelines

- Read primary guidelines from rust.instructions.md. If not available tell the user to clone and
  mount https://github.com/bitdriftlabs/ai-instructions
- Read `DESIGN.md` before changing architecture-sensitive behavior. Treat it as the current
  system-design contract; `README.md` is the operator-facing runbook.
- Update `DESIGN.md` in the same change when altering the wire protocol, partitioning or routing,
  lease or sequence behavior, durable storage layout, delivery or cursor guarantees, consumer
  coordination, or an operational contract that changes how the system behaves.

## Deflaking Bazel Integration Tests

- Run Blob Stream tests from the monorepo root with `./bazelw`; do not use Cargo for test or lint
  execution in this workspace.
- To run one Rust test inside the Nextest-backed Bazel target, pass a Nextest expression through
  `--test_arg`. For example:

  ```sh
  ./bazelw test --nocache_test_results --test_output=streamed \
    --test_arg=-E \
    --test_arg='test(lease_expiry_takeover_preserves_progress)' \
    //blob-stream/blob-stream-integration-tests:end-to-end-test
  ```

  `--test_filter` filters Bazel test targets; it does not select a Rust test within this wrapper.
- Pass logging through Bazel with `--test_env`, not a shell-only `RUST_LOG=...` prefix. Start with
  `--test_env=RUST_LOG=blob_stream=debug,bd=debug`; use
  `--test_env=RUST_LOG=blob_stream=trace,bd=trace` when debug logs do not explain the state
  transition. Add `--test_env=RUST_BACKTRACE=1` for failures that need a backtrace.
- After isolating a test, prove it is stable with uncached, serial repetitions:

  ```sh
  ./bazelw test --nocache_test_results --runs_per_test=25 --local_test_jobs=1 \
    --test_arg=-E \
    --test_arg='test(lease_expiry_takeover_preserves_progress)' \
    //blob-stream/blob-stream-integration-tests:end-to-end-test
  ```

- For tests using `ManualTimeProvider`, drive time only after the relevant task has registered a
  logical-clock sleep, preferably through a lifecycle gate or
  `advance_manual_time_until_lifecycle_gate`. Do not deflake by extending wall-clock deadlines or
  adding sleeps; add a narrow lifecycle hook when the intended state transition is not observable.
- Add focused debug or trace logs around unclear asynchronous state transitions and retain logs
  that provide durable operational observability.
- Exported OTEL spans support at most 16 attributes. Put additional correlated recovery or handoff
  state in a bounded JSON attribute such as `recovery.summary_json` or `handoff.snapshot_json`
  instead of adding scalar span attributes.
- Add a metric only when it provides legitimate operational value that cannot be derived from
  existing metrics.
- Do not run `cargo nextest list` in its default paging mode; it can page and hang the session. If
  listing is necessary, disable paging and constrain the output first, otherwise run a targeted
  `cargo nextest run` command directly.
- Format Rust and TOML changes with `cargo +nightly fmt` followed by
  `../scripts/format-toml.sh`. Use `../scripts/format-toml.sh --check` to verify TOML formatting
  without modifying files.
- When working on integration tests, follow the general guidelines in plans/TEST_AUDIT.md. DO NOT
  add any sleep hacks whatsoever. All tests MUST be deterministic. Add new test lifecycle hooks as
  needed.
