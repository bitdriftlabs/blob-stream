use anyhow::Result;
use async_trait::async_trait;
use blob_stream_proto::protos::blobstream::v1::broker::ProduceStatus;
use blob_stream_types::{
  Record,
  SeqRange,
  VirtualPartitionId,
  serialize_as_string,
  serialize_optional_as_string,
};
use protobuf::Chars;
use serde::Serialize;
use std::time::Duration;
use thiserror::Error;
use time::OffsetDateTime;

//
// WriteRequest
//

#[derive(Clone, Debug)]
pub struct WriteRequest {
  pub topic: Chars,
  pub virtual_partition_id: VirtualPartitionId,
  pub records: Vec<Record>,
}

//
// WriteResponse
//

#[derive(Clone, Debug)]
pub struct WriteResponse {
  pub seq_range: SeqRange,
}

//
// BrokerStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerStateSnapshot {
  #[serde(with = "time::serde::rfc3339")]
  pub generated_at: OffsetDateTime,
  pub holder_id: String,
  pub writer_id: u32,
  pub flush_max_bytes: u64,
  pub effective_flush_max_bytes: u64,
  pub max_segment_bytes: u64,
  pub effective_max_segment_bytes: u64,
  #[serde(with = "humantime_serde")]
  pub flush_max_delay: Duration,
  #[serde(with = "humantime_serde")]
  pub effective_flush_max_delay: Duration,
  pub membership: Vec<BrokerNodeSnapshot>,
  pub ownership: Vec<BrokerPartitionOwnershipSnapshot>,
  pub topics: Vec<BrokerTopicStateSnapshot>,
  pub durable_consumer_lease_scan: DurableConsumerLeaseScanSnapshot,
  pub durable_topics: Vec<DurableTopicStateSnapshot>,
}

//
// DurableConsumerLeaseScanSnapshot
//

/// Outcome of reading active consumer leases for the durable state view.
#[derive(Clone, Debug, Serialize)]
pub struct DurableConsumerLeaseScanSnapshot {
  pub status: DurableStateLookupStatus,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<String>,
}

//
// DurableStateLookupStatus
//

/// Result of one read-only durable state lookup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableStateLookupStatus {
  Present,
  Missing,
  Unavailable,
  LookupFailed,
  TimedOut,
}

//
// DurableTopicStateSnapshot
//

/// Durable producer and consumer lease observations for one configured topic.
#[derive(Clone, Debug, Serialize)]
pub struct DurableTopicStateSnapshot {
  #[serde(serialize_with = "serialize_as_string")]
  pub name: Chars,
  pub partitions: Vec<DurablePartitionStateSnapshot>,
}

//
// DurablePartitionStateSnapshot
//

/// Durable producer and consumer state for one configured virtual partition.
#[derive(Clone, Debug, Serialize)]
pub struct DurablePartitionStateSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub producer_lease: DurableProducerLeaseObservation,
  pub consumer_leases: Vec<DurableConsumerLeaseSnapshot>,
}

//
// DurableProducerLeaseObservation
//

/// Result of looking up the durable producer lease for one virtual partition.
#[derive(Clone, Debug, Serialize)]
pub struct DurableProducerLeaseObservation {
  pub status: DurableStateLookupStatus,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub lease: Option<DurableProducerLeaseSnapshot>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<String>,
}

//
// DurableProducerLeaseSnapshot
//

/// Durable producer lease row fields that are relevant to diagnosis.
#[derive(Clone, Debug, Serialize)]
pub struct DurableProducerLeaseSnapshot {
  pub holder_id: String,
  pub lease_epoch: u64,
  pub lease_session_id: String,
  #[serde(with = "time::serde::rfc3339")]
  pub expires_at: OffsetDateTime,
  pub is_active: bool,
  /// First sequence in the current locally reserved range, when observed.
  pub reservation_start: Option<u64>,
  /// Most recent sequence handed out by the current broker process, when observed.
  pub last_handed_out_seq: Option<u64>,
  #[serde(with = "time::serde::rfc3339::option")]
  pub sequence_progress_updated_at: Option<OffsetDateTime>,
  pub max_allocated_seq: Option<u64>,
}

//
// DurableConsumerLeaseSnapshot
//

/// Active consumer-group lease row fields for one virtual partition.
#[derive(Clone, Debug, Serialize)]
pub struct DurableConsumerLeaseSnapshot {
  pub group_id: String,
  pub owner_id: String,
  pub generation: u64,
  pub lease_expiration_ts_ms: i64,
  pub last_heartbeat_ts_ms: i64,
  pub committed_seq_end: Option<u64>,
  pub committed_ts_ms: Option<i64>,
  pub committed_source_checkpoint: Option<DurableCommittedSourceCheckpointSnapshot>,
}

//
// DurableCommittedSourceCheckpointSnapshot
//

/// Source checkpoint paired with an active consumer group's committed cursor.
#[derive(Clone, Debug, Serialize)]
pub struct DurableCommittedSourceCheckpointSnapshot {
  pub window_start_unix_seconds: i64,
  pub snowflake_id: u64,
}

//
// BrokerNodeSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BrokerNodeSnapshot {
  #[serde(serialize_with = "serialize_as_string")]
  pub node_id: Chars,
  #[serde(serialize_with = "serialize_as_string")]
  pub address: Chars,
}

//
// BrokerPartitionOwnershipSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerPartitionOwnershipSnapshot {
  #[serde(serialize_with = "serialize_as_string")]
  pub topic: Chars,
  pub virtual_partition_id: VirtualPartitionId,
  pub producer_writer_id: u32,
  pub logical_partition_id: u32,
  pub assigned_broker: Option<BrokerNodeSnapshot>,
  pub assignment_is_local: bool,
  pub lease_status: BrokerLeaseStatus,
  pub observed_lease: Option<BrokerLeaseSnapshot>,
}

//
// BrokerLeaseStatus
//

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerLeaseStatus {
  LocalActive,
  RemoteActive,
  AssignedLocalPending,
  AssignedRemotePending,
  UnleasedOrExpired,
  LookupFailed,
}

//
// BrokerLeaseSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerLeaseSnapshot {
  pub holder_id: String,
  #[serde(serialize_with = "serialize_optional_as_string")]
  pub holder_address: Option<Chars>,
  #[serde(with = "time::serde::rfc3339")]
  pub expires_at: OffsetDateTime,
  pub is_active: bool,
}

//
// BrokerTopicStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerTopicStateSnapshot {
  #[serde(serialize_with = "serialize_as_string")]
  pub name: Chars,
  pub partition_count: u32,
  pub num_writers: u32,
  #[serde(with = "humantime_serde")]
  pub retention: Duration,
  pub local_partitions: Vec<BrokerPartitionStateSnapshot>,
}

//
// BrokerPartitionStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerPartitionStateSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  #[serde(with = "time::serde::rfc3339::option")]
  pub lease_expires_at: Option<OffsetDateTime>,
  pub allocation_in_flight: bool,
  #[serde(with = "time::serde::rfc3339::option")]
  pub allocation_started_at: Option<OffsetDateTime>,
  pub buffered_batch_count: usize,
  pub buffered_record_count: usize,
  pub buffered_bytes: u64,
  #[serde(with = "time::serde::rfc3339::option")]
  pub first_buffered_at: Option<OffsetDateTime>,
  pub sequence_reservation: Option<SequenceReservationSnapshot>,
  pub next_sequence: u64,
}

//
// SequenceReservationSnapshot
//

#[derive(Debug, Serialize)]
pub struct SequenceReservationSnapshot {
  pub start: u64,
  pub end: u64,
}

//
// WriteError
//

#[derive(Debug, Error)]
pub enum WriteError {
  #[error("unknown topic: {0}")]
  UnknownTopic(Chars),
  #[error("invalid virtual partition {virtual_partition_id} for topic {topic}")]
  InvalidPartition {
    topic: Chars,
    virtual_partition_id: VirtualPartitionId,
  },
  #[error("not lease holder for topic {topic} partition {virtual_partition_id}")]
  NotLeaseHolder {
    topic: Chars,
    virtual_partition_id: VirtualPartitionId,
  },
  #[error("invalid write request: {0}")]
  InvalidRequest(String),
  #[error("broker overloaded: {0}")]
  Overloaded(String),
  #[error("producer lease fence was lost")]
  LeaseFenceLost,
  #[error("write failure: {0:#}")]
  Internal(#[from] anyhow::Error),
}

impl WriteError {
  #[must_use]
  pub fn status(&self) -> ProduceStatus {
    match self {
      Self::UnknownTopic(_) => ProduceStatus::PRODUCE_STATUS_UNKNOWN_TOPIC,
      Self::NotLeaseHolder { .. } | Self::LeaseFenceLost => {
        ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER
      },
      Self::InvalidRequest(_) => ProduceStatus::PRODUCE_STATUS_BAD_REQUEST,
      Self::InvalidPartition { .. } | Self::Overloaded(_) | Self::Internal(_) => {
        ProduceStatus::PRODUCE_STATUS_OVERLOADED
      },
    }
  }
}

//
// WriteEngine
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait WriteEngine: Send + Sync {
  /// Ingest a single batch into the broker write path.
  async fn produce_batch(&self, request: WriteRequest) -> Result<WriteResponse, WriteError>;

  /// Returns the maximum time an incoming produce RPC may wait for this engine.
  fn produce_request_timeout(&self) -> Duration;

  /// Returns a best-effort snapshot of local and durable broker state.
  async fn state_snapshot(&self) -> BrokerStateSnapshot;
}
