//! Metadata and lease-store abstractions with in-memory and Dynamo backends.

#[cfg(test)]
#[path = "./metadata_store_test.rs"]
mod tests;

use anyhow::Result;
use async_trait::async_trait;
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{
  BatchMetadata,
  CommittedCursor,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use std::collections::HashMap;

mod consumer_group_leases_dynamo;
mod consumer_group_leases_memory;
mod consumer_group_membership_dynamo;
mod consumer_group_membership_memory;
mod dynamo;
mod memory;
mod producer_partition_leases_dynamo;
mod producer_partition_leases_memory;

pub use consumer_group_leases_dynamo::DynamoConsumerGroupLeaseStore;
pub use consumer_group_leases_memory::InMemoryConsumerGroupLeaseStore;
pub use consumer_group_membership_dynamo::DynamoConsumerGroupMembershipStore;
pub use consumer_group_membership_memory::InMemoryConsumerGroupMembershipStore;
pub use dynamo::DynamoMetadataStore;
pub use memory::InMemoryMetadataStore;
pub use producer_partition_leases_dynamo::DynamoProducerPartitionLeaseStore;
pub use producer_partition_leases_memory::InMemoryProducerPartitionLeaseStore;

//
// SegmentMetadata
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Metadata index row for one flushed segment blob.
pub struct SegmentMetadata {
  /// Topic + time window key.
  pub window: TopicWindowKey,
  /// Snowflake id for segment ordering.
  pub snowflake_id: SnowflakeId,
  /// Blob key containing this segment.
  pub blob_key: BlobKey,
  /// Per-partition batch index for byte-range and sequence lookups.
  pub segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
  /// Segment compression settings.
  pub compression: Compression,
  /// Total record count in this segment.
  pub record_count: u64,
  /// Minimum event timestamp in milliseconds.
  pub min_event_ts_ms: i64,
  /// Maximum event timestamp in milliseconds.
  pub max_event_ts_ms: i64,
  /// Optional integrity checksum.
  pub checksum: Option<String>,
  /// Creation timestamp in milliseconds.
  pub created_ts_ms: i64,
}

impl SegmentMetadata {
  /// Build a segment metadata value.
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
  /// Partition key used by Dynamo schema (`topic#window_start`).
  pub fn partition_key(&self) -> String {
    self.window.format()
  }

  #[must_use]
  /// Lexicographic sort key for snowflake id.
  pub fn snowflake_key(&self) -> String {
    self.snowflake_id.format_lex()
  }
}

//
// MetadataStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
/// Segment metadata index store.
pub trait MetadataStore: Send + Sync {
  /// Persist metadata for a flushed segment.
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()>;

  /// Scan a single window for segments. Results are unordered for cost and performance.
  async fn scan_window(&self, window: &TopicWindowKey) -> Result<Vec<SegmentMetadata>>;
}

//
// ProducerPartitionLeaseKey
//

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Producer lease key scoped by topic, writer id, and virtual partition.
pub struct ProducerPartitionLeaseKey {
  /// Topic name.
  pub topic: String,
  /// Producer writer id.
  pub writer_id: u32,
  /// Virtual partition id.
  pub virtual_partition_id: VirtualPartitionId,
}

impl ProducerPartitionLeaseKey {
  #[must_use]
  /// Format as `"{topic}#{writer_id}#{virtual_partition_id}"`.
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
/// Producer lease row.
pub struct ProducerPartitionLease {
  /// Lease key.
  pub key: ProducerPartitionLeaseKey,
  /// Current holder id.
  pub holder_id: String,
  /// Lease expiration timestamp in milliseconds.
  pub lease_expiration_ts_ms: i64,
  /// High watermark for allocated sequence numbers.
  pub max_allocated_seq: Option<u64>,
}

//
// SequenceReservation
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Successful sequence reservation and resulting lease state.
pub struct SequenceReservation {
  /// Reserved inclusive sequence range.
  pub range: SeqRange,
  /// Lease state used to reserve the range.
  pub lease: ProducerPartitionLease,
}

//
// LeaseAcquireOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of acquiring a producer lease.
pub enum LeaseAcquireOutcome {
  Acquired(ProducerPartitionLease),
  HeldByOther(ProducerPartitionLease),
}

//
// LeaseHeartbeatOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of heartbeating a producer lease.
pub enum LeaseHeartbeatOutcome {
  Renewed(ProducerPartitionLease),
  HeldByOther(ProducerPartitionLease),
  Expired,
}

//
// SequenceReservationOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of reserving producer sequence numbers.
pub enum SequenceReservationOutcome {
  Reserved(SequenceReservation),
  HeldByOther(ProducerPartitionLease),
  Expired,
}

//
// LeaseReleaseOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of releasing a producer lease.
pub enum LeaseReleaseOutcome {
  Released,
  HeldByOther(ProducerPartitionLease),
  Expired,
}

//
// ProducerPartitionLeaseStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
/// Lease store for producer writer fencing and sequence reservation.
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
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome>;

  /// Release a lease held by the caller to speed up ownership convergence.
  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
  ) -> Result<LeaseReleaseOutcome>;
}

//
// ConsumerGroupLeaseKey
//

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Consumer-group lease key scoped by topic, group id, and virtual partition.
pub struct ConsumerGroupLeaseKey {
  /// Topic name.
  pub topic: String,
  /// Consumer group id.
  pub group_id: String,
  /// Virtual partition id.
  pub virtual_partition_id: VirtualPartitionId,
}

impl ConsumerGroupLeaseKey {
  #[must_use]
  /// Partition key used by Dynamo schema (`topic#group_id`).
  pub fn partition_key(&self) -> String {
    format!("{}#{}", self.topic, self.group_id)
  }

  #[must_use]
  /// Sort key used by Dynamo schema (virtual partition id string).
  pub fn sort_key(&self) -> String {
    self.virtual_partition_id.to_string()
  }
}

//
// ConsumerGroupLease
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Consumer-group lease row.
pub struct ConsumerGroupLease {
  /// Lease key.
  pub key: ConsumerGroupLeaseKey,
  /// Owner member id.
  pub owner_id: String,
  /// Assignment generation fence.
  pub generation: u64,
  /// Lease expiration timestamp in milliseconds.
  pub lease_expiration_ts_ms: i64,
  /// Last heartbeat timestamp in milliseconds.
  pub last_heartbeat_ts_ms: i64,
  /// Last committed cursor.
  pub committed_cursor: Option<CommittedCursor>,
  /// Timestamp for committed cursor in milliseconds.
  pub committed_ts_ms: Option<i64>,
}

//
// ConsumerGroupAssignmentOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of assigning a consumer partition lease.
pub enum ConsumerGroupAssignmentOutcome {
  Assigned(ConsumerGroupLease),
  HeldByOther(ConsumerGroupLease),
}

//
// ConsumerGroupHeartbeatOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of heartbeating a consumer partition lease.
pub enum ConsumerGroupHeartbeatOutcome {
  Renewed(ConsumerGroupLease),
  HeldByOther(ConsumerGroupLease),
  Expired,
}

//
// ConsumerGroupCommitOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of committing a cursor for a consumer partition lease.
pub enum ConsumerGroupCommitOutcome {
  Committed(ConsumerGroupLease),
  HeldByOther(ConsumerGroupLease),
  Expired,
}

//
// ConsumerGroupReleaseOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of releasing a consumer partition lease.
pub enum ConsumerGroupReleaseOutcome {
  Released,
  HeldByOther(ConsumerGroupLease),
  Expired,
}

//
// ConsumerGroupLeaseStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
/// Lease store used by consumer-group coordination and commits.
pub trait ConsumerGroupLeaseStore: Send + Sync {
  /// Assign ownership of a virtual partition lease.
  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<ConsumerGroupAssignmentOutcome>;

  /// Heartbeat a partition lease and optionally commit a cursor.
  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
    committed_cursor: Option<CommittedCursor>,
  ) -> Result<ConsumerGroupHeartbeatOutcome>;

  /// Commit a cursor for a partition lease.
  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    committed_cursor: CommittedCursor,
  ) -> Result<ConsumerGroupCommitOutcome>;

  /// Release a partition lease held by this owner/generation.
  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
  ) -> Result<ConsumerGroupReleaseOutcome>;
}

//
// ConsumerGroupMembershipStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
/// Membership liveness store used by dynamic group coordination.
pub trait ConsumerGroupMembershipStore: Send + Sync {
  /// Register member liveness for this consumer group.
  async fn register_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> Result<()>;

  /// Heartbeat an existing member liveness entry.
  async fn heartbeat_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> Result<()>;

  /// Deregister a member from this consumer group.
  async fn deregister_member(&self, topic: &str, group_id: &str, member_id: &str) -> Result<()>;

  /// List active members at the provided time.
  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now_ts_ms: i64,
  ) -> Result<Vec<String>>;
}
