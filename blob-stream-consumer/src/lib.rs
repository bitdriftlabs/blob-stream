//! Pull-based consumer APIs for `blob-stream`.
//!
//! The primary production entrypoint is `ConsumerConfigFactory::build_iterator_from_proto_config`,
//! which wires blob and metadata stores from a single protobuf bootstrap config.
//!
//! # Quick Start
//!
//! ```no_run
//! use anyhow::Result;
//! use bd_server_stats::stats::Collector;
//! use blob_stream_consumer::{ConsumerConfigFactory, ConsumerIterator, NextResult};
//! use blob_stream_proto::protos::blobstream::v1::config::{
//!   BlobStoreConfig,
//!   ConsumerGroupConfig,
//!   ConsumerIteratorBootstrapConfig,
//!   ConsumerReadConfig,
//!   ConsumerRuntimeConfig,
//!   MetadataStoreConfig,
//!   TopicConfig,
//!   blob_store_config,
//!   metadata_store_config,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!   let mut runtime = ConsumerRuntimeConfig::new();
//!   let mut read = ConsumerReadConfig::new();
//!   read.topic = "telemetry".into();
//!   read.window_size_seconds = Some(300);
//!   read.lookback_windows = Some(3);
//!
//!   let mut group = ConsumerGroupConfig::new();
//!   group.topic = "telemetry".into();
//!   group.group_id = "group-a".into();
//!   group.member_id = "member-a".into();
//!   group.lease_duration_ms = Some(30_000);
//!   group.heartbeat_interval_ms = Some(10_000);
//!   group.rebalance_interval_ms = Some(10_000);
//!
//!   runtime.read = Some(read).into();
//!   runtime.group = Some(group).into();
//!
//!   let mut topic = TopicConfig::new();
//!   topic.name = "telemetry".into();
//!   topic.partition_count = 128;
//!   topic.num_writers = 1;
//!   topic.retention_days = 7;
//!
//!   let mut in_memory_blob = BlobStoreConfig::new();
//!   in_memory_blob.backend = Some(blob_store_config::Backend::InMemory(Default::default()));
//!
//!   let mut in_memory_metadata = MetadataStoreConfig::new();
//!   in_memory_metadata.backend = Some(metadata_store_config::Backend::InMemory(Default::default()));
//!
//!   let mut bootstrap = ConsumerIteratorBootstrapConfig::new();
//!   bootstrap.runtime = Some(runtime).into();
//!   bootstrap.topic = Some(topic).into();
//!   bootstrap.blob_store = Some(in_memory_blob).into();
//!   bootstrap.metadata_store = Some(in_memory_metadata).into();
//!
//!   let metrics_scope = Collector::default().scope("blob_stream_consumer_docs");
//!   let mut iter =
//!     ConsumerConfigFactory::build_iterator_from_proto_config(bootstrap, metrics_scope).await?;
//!   iter.start()?;
//!
//!   match iter.next().await? {
//!     NextResult::Record(record) => {
//!       iter.store_offset(record.virtual_partition_id, record.offset)?;
//!       let _ = iter.commit().await?;
//!     },
//!     NextResult::Revoked(revoked) => {
//!       revoked.complete().await;
//!     },
//!   }
//!   Ok(())
//! }
//! ```

#[cfg(feature = "admin")]
mod admin;
mod bootstrap;
mod config;
mod consumer;
mod coordination;
mod iterator;

pub use bootstrap::{ConsumerBootstrapConfig, ConsumerConfigFactory, MembershipCoordinationSource};
pub use config::{ConsumerGroupConfig, ConsumerReadConfig, ConsumerRuntimeConfig};
pub use consumer::{ConsumerBatch, ConsumerReader, ConsumerReaderImpl};
pub use coordination::{
  ConsumerGroupCoordinator,
  ConsumerGroupCoordinatorImpl,
  HeartbeatReport,
  RebalanceReport,
  cooperative_sticky_assignment,
};
pub use iterator::{
  ConsumerCoordinationSource,
  ConsumerDeliveryState,
  ConsumerDiagnostics,
  ConsumerIterator,
  ConsumerIteratorImpl,
  ConsumerOffsetSnapshot,
  ConsumerRecord,
  ConsumerStateSnapshot,
  CoordinationSnapshot,
  NextResult,
  RevokedPartitions,
};
