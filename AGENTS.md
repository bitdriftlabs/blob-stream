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
- Do not run `cargo nextest list` in its default paging mode; it can page and hang the session. If
  listing is necessary, disable paging and constrain the output first, otherwise run a targeted
  `cargo nextest run` command directly.
