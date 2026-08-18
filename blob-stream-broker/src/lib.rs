//! Broker server crate for `blob-stream`.
//!
//! This crate contains broker runtime wiring and server-side write-path implementation used by the
//! `blob-stream-broker` binary.
//!
//! # What Is Here
//!
//! - `config`: runtime config loading and validation for broker startup
//! - `grpc`: gRPC service/router setup and request handling
//! - `write`: write engine (lease fencing, sequence reservation, segment flush/index)
//! - `metrics`: broker metrics registry/scopes
//!
//! # Client Libraries
//!
//! End users typically interact with these crates instead of this one:
//!
//! - [`blob_stream_producer`](https://docs.rs/blob-stream-producer): produce records
//! - [`blob_stream_consumer`](https://docs.rs/blob-stream-consumer): consume records
//! - [`blob_stream_broker_discovery`](https://docs.rs/blob-stream-broker-discovery): broker
//!   discovery
//! - [`blob_stream_types`](https://docs.rs/blob-stream-types): shared wire/storage types

pub mod config;
pub mod grpc;
pub mod metrics;
pub mod read;
pub mod write;
