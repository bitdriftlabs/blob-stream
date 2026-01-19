// blob-stream - segment metadata storage abstraction
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./metadata_store_test.rs"]
mod tests;

use anyhow::Result;
use async_trait::async_trait;
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{
  BatchMetadata,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use std::collections::HashMap;

mod dynamo;
mod memory;
mod producer_partition_leases_dynamo;
mod producer_partition_leases_memory;

pub use dynamo::DynamoMetadataStore;
pub use memory::InMemoryMetadataStore;
pub use producer_partition_leases_dynamo::DynamoProducerPartitionLeaseStore;
pub use producer_partition_leases_memory::InMemoryProducerPartitionLeaseStore;

//
// SegmentMetadata
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMetadata {
  pub window: TopicWindowKey,
  pub snowflake_id: SnowflakeId,
  pub blob_key: BlobKey,
  pub segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
  pub compression: Compression,
  pub record_count: u64,
  pub min_event_ts_ms: i64,
  pub max_event_ts_ms: i64,
  pub checksum: Option<String>,
  pub created_ts_ms: i64,
}

impl SegmentMetadata {
  #[must_use]
  pub fn new(
    window: TopicWindowKey,
    snowflake_id: SnowflakeId,
    blob_key: BlobKey,
    segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
    compression: Compression,
    record_count: u64,
    min_event_ts_ms: i64,
    max_event_ts_ms: i64,
    checksum: Option<String>,
    created_ts_ms: i64,
  ) -> Self {
    Self {
      window,
      snowflake_id,
      blob_key,
      segment_index,
      compression,
      record_count,
      min_event_ts_ms,
      max_event_ts_ms,
      checksum,
      created_ts_ms,
    }
  }

  #[must_use]
  pub fn partition_key(&self) -> String {
    self.window.format()
  }

  #[must_use]
  pub fn snowflake_key(&self) -> String {
    self.snowflake_id.format_lex()
  }
}

//
// MetadataStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait MetadataStore: Send + Sync {
  /// Persist metadata for a flushed segment.
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()>;

  /// Scan a single window for segments, optionally starting at a snowflake lower bound.
  ///
  /// Results are unordered for cost and performance.
  async fn scan_window(
    &self,
    window: &TopicWindowKey,
    min_snowflake_id: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>>;
}

//
// ProducerPartitionLeaseKey
//

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProducerPartitionLeaseKey {
  pub topic: String,
  pub writer_id: u32,
  pub virtual_partition_id: VirtualPartitionId,
}

impl ProducerPartitionLeaseKey {
  #[must_use]
  pub fn format(&self) -> String {
    format!(
      "{}#{}#{}",
      self.topic, self.writer_id, self.virtual_partition_id
    )
  }
}

//
// ProducerPartitionLease
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerPartitionLease {
  pub key: ProducerPartitionLeaseKey,
  pub holder_id: String,
  pub lease_expiration_ts_ms: i64,
  pub max_allocated_seq: Option<u64>,
}

//
// SequenceReservation
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceReservation {
  pub range: SeqRange,
  pub lease: ProducerPartitionLease,
}

//
// LeaseAcquireOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseAcquireOutcome {
  Acquired(ProducerPartitionLease),
  HeldByOther(ProducerPartitionLease),
}

//
// LeaseHeartbeatOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseHeartbeatOutcome {
  Renewed(ProducerPartitionLease),
  HeldByOther(ProducerPartitionLease),
  Expired,
}

//
// SequenceReservationOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceReservationOutcome {
  Reserved(SequenceReservation),
  HeldByOther(ProducerPartitionLease),
  Expired,
}

//
// ProducerPartitionLeaseStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait ProducerPartitionLeaseStore: Send + Sync {
  /// Acquire or renew a producer partition lease.
  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseAcquireOutcome>;

  /// Heartbeat a lease to keep ownership.
  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseHeartbeatOutcome>;

  /// Reserve a sequence block for a virtual partition using Hi-Lo semantics.
  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    lease_duration_ms: i64,
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome>;
}
