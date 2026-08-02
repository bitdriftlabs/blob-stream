# Agent Guidelines

- Read primary guidelines from rust.instructions.md. If not available tell the user to clone and
  mount https://github.com/bitdriftlabs/ai-instructions
- Read `DESIGN.md` before changing architecture-sensitive behavior. Treat it as the current
  system-design contract; `README.md` is the operator-facing runbook.
- Update `DESIGN.md` in the same change when altering the wire protocol, partitioning or routing,
  lease or sequence behavior, durable storage layout, delivery or cursor guarantees, consumer
  coordination, or an operational contract that changes how the system behaves.
- When debugging integration tests in blob-stream-integration tests, run with:
  RUST_LOG=blob_stream=trace,bd=trace which will provide much more info. Add more debug/trace
  logs to product code as needed to help with debugging.
- Exported OTEL spans support at most 16 attributes. Put additional correlated recovery or handoff
  state in a bounded JSON attribute such as `recovery.summary_json` or `handoff.snapshot_json`
  instead of adding scalar span attributes.
- Add a metric only when it provides legitimate operational value that cannot be derived from
  existing metrics.
- Do not run `cargo nextest list` in its default paging mode; it can page and hang the session. If
  listing is necessary, disable paging and constrain the output first, otherwise run a targeted
  `cargo nextest run` command directly.
- When working on integration tests, follow the general guidelines in plans/TEST_AUDIT.md. DO NOT
  add any sleep hacks whatsoever. All tests MUST be deterministic. Add new test lifecycle hooks as
  needed.
