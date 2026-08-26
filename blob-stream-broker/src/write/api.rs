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
  pub max_segment_bytes: u64,
  pub effective_max_segment_bytes: u64,
  pub shared_cross_topic_blobs_enabled: bool,
  #[serde(with = "humantime_serde")]
  pub flush_max_delay: Duration,
  pub membership: Vec<BrokerNodeSnapshot>,
  pub ownership: Vec<BrokerPartitionOwnershipSnapshot>,
  pub topics: Vec<BrokerTopicStateSnapshot>,
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

  /// Returns a best-effort snapshot of state held by this broker process.
  async fn state_snapshot(&self) -> BrokerStateSnapshot;
}
