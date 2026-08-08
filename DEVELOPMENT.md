# Development Guide

This document contains developer workflows for building, testing, and previewing docs in
`blob-stream`.

## Prerequisites

- Rust toolchain (edition 2024)
- Docker + Docker Compose (for local dependencies)
- Optional for local cloud emulation commands:
  - AWS CLI

## Build and test

From repo root:

```bash
cargo build --workspace
cargo clippy --workspace --bins --examples --tests -- --no-deps
cargo nextest run
```

Integration tests for this project specifically:

```bash
RUST_LOG=off cargo nextest run -p blob-stream-integration-tests
```

For deeper integration-test debugging, use trace logs:

```bash
RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests
```

## Local infrastructure (S3 + Dynamo emulation)

Start local dependencies:

```bash
docker compose up -d
```

This starts:
- DynamoDB Local on `http://localhost:8000`
- LocalStack S3 on `http://localhost:4566`

## Local stress runner

The opt-in stress runner exercises real in-process TCP brokers, LocalStack S3, DynamoDB Local,
the producer client, and production consumer bootstrap. Each run creates an isolated bucket and
set of DynamoDB tables, which are cleaned up when the run completes.

Start the local dependencies first:

```bash
docker compose up -d
```

Run a small smoke workload:

```bash
RUST_LOG=off cargo run -p blob-stream-integration-tests --bin blob-stream-stress -- \
  --brokers 1 --producers 1 --consumers 1 --partitions 4 --records 100
```

Smoke runs have a 10-second overall deadline covering setup, brokers, production, shutdown, and
verification. The runner reports the active stage and live record counters to stderr every five
seconds. Use `--overall-timeout-seconds`, `--startup-timeout-seconds`,
`--producer-timeout-seconds`, and `--consumer-shutdown-timeout-seconds` to diagnose or extend a
specific phase.

By default, each harness producer uses the producer library's normal batching values: up to 1,000
records or 1 MiB per batch, with a 200 ms flush deadline. The harness submits up to 64 records
concurrently per producer so the library can fill those batches. Override these with
`--producer-max-batch-records`, `--producer-max-batch-bytes`,
`--producer-flush-max-delay-ms`, `--producer-max-request-concurrency`, and
`--producer-submit-concurrency` when exploring throughput or latency tradeoffs.
The stress broker uses the normal 1-second flush delay by default; override it with
`--broker-flush-max-delay-ms`. The runner generates keys that target every configured virtual
partition, so inline partition observations reflect workload coverage rather than hash collisions.
Workload consumers commit every 100 records by default and commit their remaining staged offsets
during graceful shutdown. Use `--consumer-commit-interval-records` to change that cadence. The
runner prints the full validated configuration to stderr before every run. After producers finish,
the consumer group drains until it validates every identity across its owned partitions; the final
direct reader remains an independent storage-level verification pass.

Run a larger local workload:

```bash
RUST_LOG=off cargo run -p blob-stream-integration-tests --bin blob-stream-stress -- \
  --brokers 3 --producers 4 --consumers 3 --partitions 16 --records 100000 \
  --payload-bytes 1024 --overall-timeout-seconds 600 --producer-timeout-seconds 600 \
  --drain-timeout-seconds 120
```

The runner exits unsuccessfully if an acknowledged record is missing, duplicated, misrouted, a
producer or consumer fails, or a consumed payload does not belong to the current run. This is
intentionally stricter than blob-stream's at-least-once delivery contract: duplicate deliveries
are a stress-test correctness failure.

## Local Rust docs and doctests

Primary API examples live in crate rustdocs and are validated through doctests.

Build and open local docs:

```bash
cargo doc --workspace --no-deps --open
```

Open docs for specific crates:

```bash
cargo doc -p blob-stream-producer --no-deps --open
cargo doc -p blob-stream-consumer --no-deps --open
```

Run doctests for public crates:

```bash
cargo test -p blob-stream-producer --doc
cargo test -p blob-stream-consumer --doc
cargo test -p blob-stream-broker-discovery --doc
cargo test -p blob-stream-types --doc
cargo test -p blob-stream-blob-store --doc
cargo test -p blob-stream-metadata-store --doc
```

## Development verification checklist

- Format: `cargo +nightly fmt`
- Lint: `cargo clippy --workspace --bins --examples --tests -- --no-deps`
- License: `cargo deny check licenses`
- Test: `cargo nextest run`
- Integration tests: `cargo nextest run -p blob-stream-integration-tests`

Fault-injection tests are in:
- `blob-stream-integration-tests/tests/fault_injection_test.rs`
