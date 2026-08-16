//! High-throughput producer client for `blob-stream` topics.
//!
//! This crate provides a batched, retrying producer client that routes records to broker nodes
//! using rendezvous hashing over discovery membership.
//!
//! # Quick Start
//!
//! ```no_run
//! use anyhow::Result;
//! use bd_server_stats::stats::Collector;
//! use blob_stream_producer::{
//!   ProducerClient,
//!   ProducerClientImpl,
//!   ProducerCompression,
//!   ProducerConfig,
//!   ProducerDiscoveryConfig,
//!   ProducerNodeConfig,
//!   ProducerRecord,
//!   ProducerRuntimeConfig,
//!   ProducerTopicConfig,
//! };
//! use blob_stream_proto::protos::blobstream::v1::config::StaticBrokerDiscoveryConfig;
//! use blob_stream_types::ToProtoDuration;
//! use time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!   let mut runtime = ProducerRuntimeConfig::new();
//!
//!   let mut producer = ProducerConfig::new();
//!   producer.writer_id = Some(0);
//!   producer.max_batch_records = Some(1000);
//!   producer.max_batch_bytes = Some(1_048_576);
//!   producer.flush_max_delay = Duration::milliseconds(200).into_proto();
//!   producer.retry_base_delay = Duration::milliseconds(25).into_proto();
//!   producer.retry_max_delay = Duration::seconds(1).into_proto();
//!   producer.retry_deadline = Duration::seconds(30).into_proto();
//!   producer.connect_timeout = Duration::seconds(2).into_proto();
//!   producer.request_timeout = Duration::seconds(5).into_proto();
//!   producer.max_request_concurrency = Some(64);
//!   producer.compression = Some(ProducerCompression::PRODUCER_COMPRESSION_SNAPPY.into());
//!
//!   let mut node = ProducerNodeConfig::new();
//!   node.node_id = "broker-a".into();
//!   node.address = "127.0.0.1:8080".into();
//!   let mut static_cfg = StaticBrokerDiscoveryConfig::new();
//!   static_cfg.nodes.push(node);
//!   let mut discovery = ProducerDiscoveryConfig::new();
//!   discovery.set_static(static_cfg);
//!
//!   let mut topic = ProducerTopicConfig::new();
//!   topic.name = "telemetry".into();
//!   topic.partition_count = 128;
//!   topic.num_writers = 1;
//!   topic.retention = Duration::days(7).into_proto();
//!
//!   runtime.producer = Some(producer).into();
//!   runtime.discovery = Some(discovery).into();
//!   runtime.topics.push(topic);
//!
//!   let metrics_scope = Collector::default().scope("blob_stream_producer_docs");
//!   let producer = ProducerClientImpl::from_runtime_config(runtime, metrics_scope).await?;
//!
//!   let ack = producer
//!     .produce(ProducerRecord::new(
//!       "telemetry",
//!       b"device-123".to_vec(),
//!       b"payload".to_vec(),
//!       1_700_000_000_000,
//!     ))
//!     .await?;
//!   println!(
//!     "acked partition={}, attempts={}",
//!     ack.virtual_partition_id, ack.attempts
//!   );
//!   producer.flush().await?;
//!   Ok(())
//! }
//! ```

mod admin;
mod config;
mod producer;

pub use config::{
  ProducerCompression,
  ProducerConfig,
  ProducerDiscoveryConfig,
  ProducerNodeConfig,
  ProducerRuntimeConfig,
  ProducerTopicConfig,
};
pub use producer::{
  BrokerTransport,
  GrpcBrokerTransport,
  ProducerAck,
  ProducerClient,
  ProducerClientBuilder,
  ProducerClientImpl,
  ProducerDiagnostics,
  ProducerError,
  ProducerPartitionBufferSnapshot,
  ProducerRecord,
  ProducerRetryClock,
  ProducerRetryReason,
  ProducerRetrySample,
  ProducerRetrySummary,
  ProducerStateSnapshot,
  ProducerTopicSnapshot,
};
