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
//! use blob_stream_consumer::ConsumerConfigFactory;
//! use blob_stream_consumer::iterator::{ConsumerIterator, NextResult};
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
//! use blob_stream_types::ToProtoDuration;
//! use time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!   let mut runtime = ConsumerRuntimeConfig::new();
//!   let mut read = ConsumerReadConfig::new();
//!   read.topic = "telemetry".into();
//!   read.window_size = Duration::seconds(300).into_proto();
//!
//!   let mut group = ConsumerGroupConfig::new();
//!   group.topic = "telemetry".into();
//!   group.group_id = "group-a".into();
//!   group.member_id = "member-a".into();
//!   group.lease_duration = Duration::seconds(30).into_proto();
//!   group.heartbeat_interval = Duration::seconds(10).into_proto();
//!   group.rebalance_interval = Duration::seconds(10).into_proto();
//!
//!   runtime.read = Some(read).into();
//!   runtime.group = Some(group).into();
//!
//!   let mut topic = TopicConfig::new();
//!   topic.name = "telemetry".into();
//!   topic.partition_count = 128;
//!   topic.num_writers = 1;
//!   topic.retention = Duration::days(7).into_proto();
//!   topic.max_metadata_publication_lag = Duration::seconds(15).into_proto();
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

mod admin;
mod bootstrap;
mod config;
pub mod consumer;
mod coordination;
mod diagnostics;
pub mod iterator;

pub use blob_stream_proto::protos::blobstream::v1::config::EventualMetadataReadsConfig;
pub use bootstrap::{
  ConsumerBootstrapConfig,
  ConsumerBootstrapIteratorBuilder,
  ConsumerConfigFactory,
  MembershipCoordinationSource,
};
pub use config::{
  ConsumerGroupConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
};
pub use coordination::{
  ConsumerGroupCoordinator,
  ConsumerGroupCoordinatorImpl,
  HeartbeatReport,
  RebalanceReport,
  RecoveredCursor,
  cooperative_sticky_assignment,
};
pub use diagnostics::{
  ConsumerAssignmentPlanSnapshot,
  ConsumerAssignmentPolicy,
  ConsumerDiagnostics,
  ConsumerGroupLeaseObservation,
  ConsumerGroupPartitionLeaseSnapshot,
  ConsumerLocalPartitionSnapshot,
  ConsumerLocalStateSnapshot,
  ConsumerMemberTopologySnapshot,
  ConsumerPartitionAssignmentSnapshot,
  ConsumerPartitionReadMode,
  ConsumerPodLoadSnapshot,
  ConsumerReaderStateSnapshot,
  ConsumerSourceCheckpointSnapshot,
  ConsumerStateResponse,
  ConsumerStateSnapshot,
};
