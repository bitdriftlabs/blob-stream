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
- Test: `cargo nextest run`
- Integration tests: `cargo nextest run -p blob-stream-integration-tests`

Fault-injection tests are in:
- `blob-stream-integration-tests/tests/fault_injection_test.rs`
