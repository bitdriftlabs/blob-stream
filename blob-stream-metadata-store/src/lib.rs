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
use protobuf::Chars;
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};

mod aws;
mod codec;
mod consumer_group_leases_dynamo;
mod consumer_group_leases_memory;
mod consumer_group_membership_dynamo;
mod consumer_group_membership_memory;
mod dynamo;
mod dynamo_attributes;
mod dynamo_metrics;
mod memory;
mod producer_partition_leases_dynamo;
mod producer_partition_leases_memory;

pub use aws::{aws_retry_config, aws_timeout_config};
pub use codec::{decode_segment_metadata_v1, encode_segment_metadata_v1};
pub use consumer_group_leases_dynamo::DynamoConsumerGroupLeaseStore;
pub use consumer_group_leases_memory::InMemoryConsumerGroupLeaseStore;
pub use consumer_group_membership_dynamo::DynamoConsumerGroupMembershipStore;
pub use consumer_group_membership_memory::InMemoryConsumerGroupMembershipStore;
pub use dynamo::{DynamoMetadataStore, MAX_FENCED_METADATA_PARTITIONS};
pub use dynamo_metrics::DynamoCapacityMetrics;
pub use memory::InMemoryMetadataStore;
pub use producer_partition_leases_dynamo::DynamoProducerPartitionLeaseStore;
pub use producer_partition_leases_memory::InMemoryProducerPartitionLeaseStore;

//
// MetadataWriteError
//

/// Error returned when persisting segment metadata.
#[derive(Debug, thiserror::Error)]
pub enum MetadataWriteError {
  /// A metadata publication fence no longer authorizes its producer lease.
  #[error("producer lease fence was lost")]
  ProducerLeaseFenceLost,
  /// A backend, encoding, or validation failure unrelated to producer fencing.
  #[error(transparent)]
  Other(#[from] anyhow::Error),
}

/// Result returned by a segment metadata publication.
pub type MetadataWriteResult = std::result::Result<(), MetadataWriteError>;

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
  /// Compression settings shared by every batch in the segment.
  pub compression: Compression,
  /// Per-partition batch index for byte-range and sequence lookups.
  pub segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
  /// Creation instant.
  pub created_at: OffsetDateTime,
  /// Instant immediately before the metadata row was written.
  pub metadata_published_at: OffsetDateTime,
}

impl SegmentMetadata {
  /// Build a segment metadata value.
  #[must_use]
  pub fn new(
    window: TopicWindowKey,
    snowflake_id: SnowflakeId,
    blob_key: BlobKey,
    compression: Compression,
    segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
    created_at: OffsetDateTime,
    metadata_published_at: OffsetDateTime,
  ) -> Self {
    Self {
      window,
      snowflake_id,
      blob_key,
      compression,
      segment_index,
      created_at,
      metadata_published_at,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Consistency requested for one metadata window query.
pub enum MetadataReadConsistency {
  #[default]
  /// Read from a replica when `DynamoDB` permits it.
  Eventual,
  /// Read committed metadata from `DynamoDB`'s leader.
  Strong,
}

#[async_trait]
/// Segment metadata index store.
pub trait MetadataStore: Send + Sync {
  /// Persist metadata for a flushed segment, optionally conditioned on producer lease fences.
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult;

  /// Scan a single window from an optional inclusive snowflake lower bound. Results are unordered
  /// for cost and performance.
  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Producer lease fence paired with the partition it authorizes.
pub struct ProducerPartitionFence {
  /// Producer partition lease key.
  pub key: ProducerPartitionLeaseKey,
  /// Durable lease identity.
  pub fence: ProducerLeaseFence,
}

//
// ProducerPartitionLeaseKey
//

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Producer lease key scoped by topic and virtual partition.
pub struct ProducerPartitionLeaseKey {
  /// Topic name.
  pub topic: Chars,
  /// Virtual partition id.
  pub virtual_partition_id: VirtualPartitionId,
}

impl ProducerPartitionLeaseKey {
  #[must_use]
  /// Format as `"{topic}#{virtual_partition_id}"`.
  pub fn format(&self) -> String {
    format!("{}#{}", self.topic, self.virtual_partition_id)
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
  /// Durable identity that authorizes metadata publication.
  pub fence: ProducerLeaseFence,
  /// Instant at which the lease expires.
  pub lease_expiration_at: OffsetDateTime,
  /// High watermark for allocated sequence numbers.
  pub max_allocated_seq: Option<u64>,
}

//
// ProducerLeaseFence
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Durable producer-lease identity that authorizes metadata publication.
///
/// The epoch advances when ownership is taken over, rejecting a prior owner. The session id
/// additionally rejects an earlier broker process that restarts with the same stable holder id.
pub struct ProducerLeaseFence {
  /// Stable broker identity that owns the lease.
  pub holder_id: String,
  /// Monotonic ownership epoch.
  pub lease_epoch: u64,
  /// Unique identity of the owning broker process.
  pub lease_session_id: String,
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
// LeaseAcquireAndReserveOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of atomically acquiring or renewing a producer lease and optionally reserving sequences.
pub enum LeaseAcquireAndReserveOutcome {
  Acquired {
    lease: ProducerPartitionLease,
    reservation: Option<SeqRange>,
  },
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
/// Lease store for broker fencing and virtual-partition sequence reservation.
pub trait ProducerPartitionLeaseStore: Send + Sync {
  /// Read the current lease row without changing ownership or sequence state.
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>>;

  /// Acquire or renew a producer partition lease.
  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: Duration,
  ) -> Result<LeaseAcquireOutcome>;

  /// Atomically acquire or renew a lease and optionally reserve a Hi-Lo sequence block.
  async fn acquire_lease_and_reserve_sequences(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: Duration,
    reservation_size: Option<u64>,
  ) -> Result<LeaseAcquireAndReserveOutcome>;

  /// Heartbeat a lease to keep ownership.
  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    lease_duration: Duration,
  ) -> Result<LeaseHeartbeatOutcome>;

  /// Reserve a sequence block for a virtual partition using Hi-Lo semantics.
  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome>;

  /// Release a lease held by the caller to speed up ownership convergence.
  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
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
// ConsumerGroupLeaseTransition
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Ownership transition applied by a successful consumer partition claim.
pub enum ConsumerGroupLeaseTransition {
  /// The lease row did not exist before this claim.
  Initial,
  /// The existing owner renewed or advanced its own assignment generation.
  Retained,
  /// A previous owner explicitly released the lease before this claim.
  GracefulHandoff {
    /// Previous owner member id.
    previous_owner_id: String,
    /// Previous assignment generation.
    previous_generation: u64,
    /// Previous owner heartbeat timestamp in milliseconds.
    previous_last_heartbeat_ts_ms: i64,
    /// Timestamp of the explicit release in milliseconds.
    graceful_release_ts_ms: i64,
  },
  /// A previous owner did not release before its lease expired.
  ExpiryTakeover {
    /// Previous owner member id.
    previous_owner_id: String,
    /// Previous assignment generation.
    previous_generation: u64,
    /// Previous owner heartbeat timestamp in milliseconds.
    previous_last_heartbeat_ts_ms: i64,
  },
}

//
// ConsumerGroupLeasePredecessor
//

/// Lease fields needed to classify a successful consumer lease claim.
pub(crate) struct ConsumerGroupLeasePredecessor<'a> {
  pub(crate) owner_id: &'a str,
  pub(crate) generation: u64,
  pub(crate) last_heartbeat_ts_ms: i64,
  pub(crate) graceful_release_ts_ms: Option<i64>,
}

pub(crate) fn consumer_group_lease_transition(
  previous: Option<ConsumerGroupLeasePredecessor<'_>>,
  owner_id: &str,
) -> ConsumerGroupLeaseTransition {
  let Some(previous) = previous else {
    return ConsumerGroupLeaseTransition::Initial;
  };
  if previous.owner_id == owner_id {
    return ConsumerGroupLeaseTransition::Retained;
  }
  if let Some(graceful_release_ts_ms) = previous.graceful_release_ts_ms {
    return ConsumerGroupLeaseTransition::GracefulHandoff {
      previous_owner_id: previous.owner_id.to_string(),
      previous_generation: previous.generation,
      previous_last_heartbeat_ts_ms: previous.last_heartbeat_ts_ms,
      graceful_release_ts_ms,
    };
  }

  ConsumerGroupLeaseTransition::ExpiryTakeover {
    previous_owner_id: previous.owner_id.to_string(),
    previous_generation: previous.generation,
    previous_last_heartbeat_ts_ms: previous.last_heartbeat_ts_ms,
  }
}

//
// ConsumerGroupAssignmentOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of assigning a consumer partition lease.
pub enum ConsumerGroupAssignmentOutcome {
  /// This member now owns the lease, with its prior state classified when retained.
  Assigned {
    /// Newly assigned lease state.
    lease: ConsumerGroupLease,
    /// Previous lease state, when a retained row was claimed.
    previous_lease: Option<Box<ConsumerGroupLease>>,
    /// How ownership changed.
    transition: ConsumerGroupLeaseTransition,
  },
  /// Another member currently owns an unexpired lease.
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
  /// List all retained lease rows for one consumer group, including expired rows awaiting TTL
  /// cleanup.
  async fn list_group_leases(&self, topic: &str, group_id: &str)
  -> Result<Vec<ConsumerGroupLease>>;

  /// Assign ownership of a virtual partition lease.
  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: Duration,
  ) -> Result<ConsumerGroupAssignmentOutcome>;

  /// Heartbeat a partition lease and optionally commit a cursor.
  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: Duration,
    committed_cursor: Option<CommittedCursor>,
  ) -> Result<ConsumerGroupHeartbeatOutcome>;

  /// Commit a cursor for a partition lease.
  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
  ) -> Result<ConsumerGroupCommitOutcome>;

  /// Release a partition lease held by this owner/generation.
  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
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
    pod_id: Option<String>,
    now: OffsetDateTime,
    ttl: Duration,
  ) -> Result<()>;

  /// Heartbeat an existing member liveness entry.
  async fn heartbeat_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    pod_id: Option<String>,
    now: OffsetDateTime,
    ttl: Duration,
  ) -> Result<()>;

  /// Deregister a member from this consumer group.
  async fn deregister_member(&self, topic: &str, group_id: &str, member_id: &str) -> Result<()>;

  /// List active members at the provided time.
  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now: OffsetDateTime,
  ) -> Result<Vec<ConsumerGroupMember>>;

  /// Return the last published complete assignment plan for this group.
  async fn get_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Option<ConsumerGroupAssignmentPlan>>;

  /// Return the current planner lease without changing it.
  async fn get_planner_lease(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Option<ConsumerGroupPlannerLease>>;

  /// Conditionally acquire or renew the group planner lease.
  async fn acquire_or_renew_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
    now: OffsetDateTime,
    ttl: Duration,
  ) -> Result<ConsumerGroupPlannerLeaseOutcome>;

  /// Release the planner lease when it is still held by this member.
  async fn release_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
  ) -> Result<bool>;

  /// Publish a plan only while the caller still owns an unexpired planner lease.
  async fn publish_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
    now: OffsetDateTime,
    plan: ConsumerGroupAssignmentPlan,
  ) -> Result<bool>;
}

//
// ConsumerGroupMember
//

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Active consumer-group member with optional physical topology metadata.
pub struct ConsumerGroupMember {
  /// Stable consumer process identifier.
  pub member_id: String,
  /// Stable physical pod identifier when the caller supports pod-aware assignment.
  pub pod_id: Option<String>,
}

//
// ConsumerGroupAssignment
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Desired owner of one consumer-group virtual partition.
pub struct ConsumerGroupAssignment {
  /// Virtual partition covered by this assignment.
  pub virtual_partition_id: VirtualPartitionId,
  /// Active member expected to claim the partition lease.
  pub member_id: String,
}

//
// ConsumerGroupAssignmentPlan
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Versioned, complete desired ownership map shared by all consumers in a group.
pub struct ConsumerGroupAssignmentPlan {
  /// Monotonically increasing plan generation used for consumer lease fencing.
  pub version: u64,
  /// Member that most recently published and renews the planner lease for this plan.
  pub planner_member_id: String,
  /// Canonically sorted active members used to construct this plan.
  pub members: Vec<String>,
  /// Optional canonical topology snapshot for pod-aware assignment plans.
  pub member_topology: Option<Vec<ConsumerGroupMember>>,
  /// Canonically sorted partition ownership entries.
  pub assignments: Vec<ConsumerGroupAssignment>,
  /// Millisecond timestamp when the planner published this map.
  pub published_ts_ms: i64,
}

//
// ConsumerGroupPlannerLease
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Planner ownership lease for a consumer group assignment plan.
pub struct ConsumerGroupPlannerLease {
  /// Member currently responsible for refreshing or replacing the assignment plan.
  pub member_id: String,
  /// Unique coordinator session that owns this planner lease.
  pub planner_session_id: String,
  /// Millisecond timestamp after which another member may become planner.
  pub lease_expiration_ts_ms: i64,
}

//
// ConsumerGroupPlannerLeaseOutcome
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Result of conditionally acquiring the single planner lease for a consumer group.
pub enum ConsumerGroupPlannerLeaseOutcome {
  /// The caller owns the planner lease until its configured expiration.
  Acquired,
  /// A different active member currently owns the planner lease.
  HeldByOther,
}
