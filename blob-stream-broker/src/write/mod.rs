// blob-stream - broker write path
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./write_test.rs"]
mod tests;

mod config;
mod flush;
mod lease_assignment;

use crate::write::flush::FlushContext;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bd_log::warn_every;
use bd_time::{OffsetDateTimeExt, SystemTimeProvider, TimeProvider};
use blob_stream_blob_store::{BlobKey, BlobStore};
use blob_stream_broker_discovery::BrokerMembership;
use blob_stream_metadata_store::{
  LeaseAcquireOutcome,
  MetadataStore,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservationOutcome,
};
use blob_stream_proto::protos::blobstream::v1::broker::ProduceStatus;
use blob_stream_types::{
  BatchMetadata,
  BatchSummary,
  Compression,
  Record,
  RecordBatch,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
pub use config::{TopicInfo, WriteConfig, build_write_engine};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use thiserror::Error;
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use tokio::sync::{Mutex, oneshot, watch};

const DEFAULT_ZSTD_LEVEL: i32 = 3;
type FlushCompletion = oneshot::Sender<Result<(), String>>;

//
// WriteRequest
//

#[derive(Clone, Debug)]
pub struct WriteRequest {
  pub topic: String,
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
// WriteError
//

#[derive(Debug, Error)]
pub enum WriteError {
  #[error("unknown topic: {0}")]
  UnknownTopic(String),
  #[error("invalid virtual partition {virtual_partition_id} for topic {topic}")]
  InvalidPartition {
    topic: String,
    virtual_partition_id: VirtualPartitionId,
  },
  #[error("not lease holder for topic {topic} partition {virtual_partition_id}")]
  NotLeaseHolder {
    topic: String,
    virtual_partition_id: VirtualPartitionId,
  },
  #[error("broker overloaded: {0}")]
  Overloaded(String),
  #[error("write failure: {0}")]
  Internal(#[from] anyhow::Error),
}

impl WriteError {
  #[must_use]
  pub fn status(&self) -> ProduceStatus {
    match self {
      Self::UnknownTopic(_) => ProduceStatus::PRODUCE_STATUS_UNKNOWN_TOPIC,
      Self::NotLeaseHolder { .. } => ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER,
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
}

//
// WriteEngineImpl
//

pub struct WriteEngineImpl {
  config: WriteConfig,
  topics: HashMap<String, TopicInfo>,
  flush_context: FlushContext,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  holder_id: String,
  time_provider: Arc<dyn TimeProvider>,
  state: Arc<Mutex<WriteState>>,
  lease_assignment_shutdown_tx: Option<oneshot::Sender<()>>,
}

impl WriteEngineImpl {
  pub fn new(
    config: WriteConfig,
    topics: HashMap<String, TopicInfo>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ProducerPartitionLeaseStore>,
    holder_id: String,
    membership_rx: Option<watch::Receiver<BrokerMembership>>,
  ) -> Result<Self> {
    Self::new_with_time_provider(
      config,
      topics,
      blob_store,
      metadata_store,
      lease_store,
      holder_id,
      membership_rx,
      Arc::new(SystemTimeProvider),
    )
  }

  pub fn new_with_time_provider(
    config: WriteConfig,
    topics: HashMap<String, TopicInfo>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ProducerPartitionLeaseStore>,
    holder_id: String,
    membership_rx: Option<watch::Receiver<BrokerMembership>>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> Result<Self> {
    let snowflake = flush::SnowflakeGenerator::new()?;
    let state = Arc::new(Mutex::new(WriteState::default()));
    let flush_context = FlushContext::new(config.clone(), blob_store, metadata_store, snowflake);

    let mut engine = Self {
      config,
      topics,
      flush_context,
      lease_store,
      holder_id,
      time_provider,
      state,
      lease_assignment_shutdown_tx: None,
    };

    engine.spawn_flush_loop();
    if let Some(membership_rx) = membership_rx {
      engine.lease_assignment_shutdown_tx =
        Some(engine.spawn_lease_self_assignment_loop(membership_rx));
    }
    Ok(engine)
  }

  async fn ensure_lease(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
    now_ts_ms: i64,
  ) -> Result<i64, WriteError> {
    let key = ProducerPartitionLeaseKey {
      topic: topic.to_string(),
      writer_id: self.config.writer_id,
      virtual_partition_id,
    };

    match self
      .lease_store
      .acquire_lease(
        key,
        self.holder_id.clone(),
        now_ts_ms,
        self.config.lease_duration_ms,
      )
      .await
      .context("acquire producer partition lease")?
    {
      LeaseAcquireOutcome::Acquired(lease) => Ok(lease.lease_expiration_ts_ms),
      LeaseAcquireOutcome::HeldByOther(_) => Err(WriteError::NotLeaseHolder {
        topic: topic.to_string(),
        virtual_partition_id,
      }),
    }
  }

  async fn reserve_sequences(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
    now_ts_ms: i64,
  ) -> Result<SeqRange, WriteError> {
    let key = ProducerPartitionLeaseKey {
      topic: topic.to_string(),
      writer_id: self.config.writer_id,
      virtual_partition_id,
    };

    match self
      .lease_store
      .reserve_sequences(
        &key,
        &self.holder_id,
        now_ts_ms,
        self.config.reservation_size,
      )
      .await
      .context("reserve sequences")?
    {
      SequenceReservationOutcome::Reserved(reservation) => Ok(reservation.range),
      SequenceReservationOutcome::HeldByOther(_) | SequenceReservationOutcome::Expired => {
        Err(WriteError::NotLeaseHolder {
          topic: topic.to_string(),
          virtual_partition_id,
        })
      },
    }
  }

  fn spawn_flush_loop(&self) {
    let interval_ms = self.config.flush_max_delay_ms.max(1).cast_unsigned();
    let interval = StdDuration::from_millis(interval_ms);
    let flush_context = self.flush_context.clone();
    let state = Arc::clone(&self.state);
    let time_provider = Arc::clone(&self.time_provider);

    tokio::spawn(async move {
      let mut ticker = tokio::time::interval(interval);
      loop {
        // Time-based flushing is driven here. Even if no new writes arrive, this loop wakes up
        // every flush_max_delay_ms and asks state for anything that has waited long enough.
        ticker.tick().await;
        let now = time_provider.now();
        let now_ts_ms = now.unix_timestamp_ms();
        let plans = {
          let mut guard = state.lock().await;
          // Build flush plans from all topic/partition buffers that are currently eligible under
          // size/time rules. This call drains eligible buffered batches from in-memory state.
          guard.collect_flush_plans(now_ts_ms, flush_context.config())
        };

        if plans.is_empty() {
          continue;
        }

        if let Err(error) = flush_plans_and_notify(&flush_context, plans, now).await {
          warn_every!(15.seconds(), "write flush failed: {}", error);
        }
      }
    });
  }
}

impl Drop for WriteEngineImpl {
  fn drop(&mut self) {
    if let Some(shutdown_tx) = self.lease_assignment_shutdown_tx.take() {
      let _ignored = shutdown_tx.send(());
    }
  }
}

#[async_trait]
impl WriteEngine for WriteEngineImpl {
  async fn produce_batch(&self, request: WriteRequest) -> Result<WriteResponse, WriteError> {
    let topic_info = self
      .topics
      .get(&request.topic)
      .ok_or_else(|| WriteError::UnknownTopic(request.topic.clone()))?;

    if !topic_info.is_valid_partition(request.virtual_partition_id) {
      return Err(WriteError::InvalidPartition {
        topic: request.topic,
        virtual_partition_id: request.virtual_partition_id,
      });
    }

    if request.records.is_empty() {
      return Err(WriteError::Overloaded("record batch is empty".to_string()));
    }

    let summary = RecordBatch::new(request.virtual_partition_id, request.records.clone())
      .summary()
      .ok_or_else(|| anyhow!("failed to summarize record batch"))?;

    let now = self.time_provider.now();
    let now_ts_ms = now.unix_timestamp_ms();

    let needs_lease = {
      let mut state = self.state.lock().await;
      let partition_state = state.partition_state_mut(&request.topic, request.virtual_partition_id);
      partition_state.needs_lease(now_ts_ms)
    };

    if needs_lease {
      let expires_at = self
        .ensure_lease(&request.topic, request.virtual_partition_id, now_ts_ms)
        .await?;
      let mut state = self.state.lock().await;
      let partition_state = state.partition_state_mut(&request.topic, request.virtual_partition_id);
      partition_state.lease_expiration_ts_ms = Some(expires_at);
    }

    let record_count = request.records.len() as u64;
    let needs_reservation = {
      let mut state = self.state.lock().await;
      let partition_state = state.partition_state_mut(&request.topic, request.virtual_partition_id);
      !partition_state.seq_allocator.can_allocate(record_count)
    };

    if needs_reservation {
      let range = self
        .reserve_sequences(&request.topic, request.virtual_partition_id, now_ts_ms)
        .await?;
      let mut state = self.state.lock().await;
      let partition_state = state.partition_state_mut(&request.topic, request.virtual_partition_id);
      partition_state.seq_allocator.set_reservation(range);
    }

    let (completion_tx, completion_rx) = oneshot::channel();

    let (seq_range, plans) = {
      let mut state = self.state.lock().await;
      let partition_state = state.partition_state_mut(&request.topic, request.virtual_partition_id);
      // We should normally have capacity because we reserve when `can_allocate(record_count)` is
      // false above, and the lease-assignment loop also proactively tops up. This can still fail
      // if ownership changed or local reservation state was invalidated concurrently.
      let seq_range = partition_state
        .seq_allocator
        .allocate(record_count)
        // TODO(mattklein123): Consider a single inline re-reserve + allocate retry here before
        // returning OVERLOADED. That would reduce transient write failures during rapid
        // reservation turnover while preserving lease fencing.
        .ok_or_else(|| WriteError::Overloaded("sequence reservation exhausted".to_string()))?;

      partition_state.buffer.push(
        BufferedBatch {
          records: request.records,
          summary,
          seq_range: seq_range.clone(),
          completion: Some(completion_tx),
        },
        now_ts_ms,
      );

      // After adding this incoming batch, we immediately run flush planning once. This enables
      // "flush on size" behavior in-line with produce: if the just-pushed data makes the buffer
      // cross flush_max_bytes, we flush now instead of waiting for the background ticker.
      //
      // If thresholds are not met yet, no plans are produced and data stays buffered until either
      // a future produce call or the periodic flush loop observes the time threshold.
      let plans = state.collect_flush_plans(now_ts_ms, &self.config);
      (seq_range, plans)
    };

    if !plans.is_empty() {
      flush_plans_and_notify(&self.flush_context, plans, now).await?;
    }

    match completion_rx.await {
      Ok(Ok(())) => {},
      Ok(Err(error)) => return Err(WriteError::Internal(anyhow!(error))),
      Err(_closed) => {
        return Err(WriteError::Internal(anyhow!(
          "flush completion channel closed before acknowledgment"
        )));
      },
    }

    Ok(WriteResponse { seq_range })
  }
}

async fn flush_plans_and_notify(
  flush_context: &FlushContext,
  mut plans: Vec<FlushPlan>,
  now: OffsetDateTime,
) -> Result<(), WriteError> {
  let mut completions = Vec::new();
  for plan in &mut plans {
    for partition in &mut plan.partitions {
      for batch in &mut partition.batches {
        if let Some(completion) = batch.completion.take() {
          completions.push(completion);
        }
      }
    }
  }

  let result = flush_context.flush_plans(plans, now).await;
  let completion_result = result
    .as_ref()
    .map_or_else(|error| Err(error.to_string()), |_ok| Ok(()));

  for completion in completions {
    let _ignored = completion.send(completion_result.clone());
  }

  result
}

//
// WriteState
//

#[derive(Debug, Default)]
struct WriteState {
  topics: HashMap<String, TopicState>,
}

impl WriteState {
  fn partition_state_mut(
    &mut self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> &mut PartitionState {
    self
      .topics
      .entry(topic.to_string())
      .or_default()
      .partitions
      .entry(virtual_partition_id)
      .or_default()
  }

  fn collect_flush_plans(&mut self, now_ts_ms: i64, config: &WriteConfig) -> Vec<FlushPlan> {
    // This method is the single place where buffered data becomes flush work. Callers (produce
    // fast-path and background flush loop) both use it so size and time triggers share one
    // consistent decision path.
    let mut plans = Vec::new();

    for (topic, topic_state) in &mut self.topics {
      let mut partitions = Vec::new();

      for (virtual_partition_id, partition_state) in &mut topic_state.partitions {
        if !partition_state.buffer.should_flush(now_ts_ms, config) {
          continue;
        }

        // Once flush is triggered, all currently buffered batches for the partition are moved as
        // one partition plan. New writes arriving later start a new buffer epoch.
        let batches = std::mem::take(&mut partition_state.buffer.batches);
        if batches.is_empty() {
          partition_state.buffer.reset();
          continue;
        }

        partition_state.buffer.reset();
        partitions.push(FlushPartition {
          virtual_partition_id: *virtual_partition_id,
          batches,
        });
      }

      if !partitions.is_empty() {
        plans.push(FlushPlan {
          topic: topic.clone(),
          partitions,
        });
      }
    }

    plans
  }
}

//
// TopicState
//

#[derive(Debug, Default)]
struct TopicState {
  partitions: HashMap<VirtualPartitionId, PartitionState>,
}

//
// PartitionState
//

#[derive(Debug, Default)]
struct PartitionState {
  buffer: BufferState,
  seq_allocator: SeqAllocator,
  lease_expiration_ts_ms: Option<i64>,
}

impl PartitionState {
  fn needs_lease(&self, now_ts_ms: i64) -> bool {
    self
      .lease_expiration_ts_ms
      .is_none_or(|expires_at| now_ts_ms >= expires_at)
  }
}

//
// BufferState
//

#[derive(Debug, Default)]
struct BufferState {
  batches: Vec<BufferedBatch>,
  buffered_bytes: u64,
  first_buffered_ts_ms: Option<i64>,
}

impl BufferState {
  fn push(&mut self, batch: BufferedBatch, now_ts_ms: i64) {
    if self.first_buffered_ts_ms.is_none() {
      self.first_buffered_ts_ms = Some(now_ts_ms);
    }
    self.buffered_bytes = self
      .buffered_bytes
      .saturating_add(batch.summary.payload_bytes);
    self.batches.push(batch);
  }

  fn should_flush(&self, now_ts_ms: i64, config: &WriteConfig) -> bool {
    // Empty buffers never flush.
    if self.batches.is_empty() {
      return false;
    }

    // Size trigger: flush immediately once accumulated payload bytes cross threshold.
    if self.buffered_bytes >= config.flush_max_bytes {
      return true;
    }

    let Some(first_ts) = self.first_buffered_ts_ms else {
      return false;
    };

    // Time trigger: once oldest buffered batch has waited long enough, flush whatever is present.
    // This ensures low-throughput partitions still make progress without waiting for size growth.
    now_ts_ms.saturating_sub(first_ts) >= config.flush_max_delay_ms
  }

  fn reset(&mut self) {
    self.batches.clear();
    self.buffered_bytes = 0;
    self.first_buffered_ts_ms = None;
  }
}

//
// BufferedBatch
//

#[derive(Debug)]
struct BufferedBatch {
  records: Vec<Record>,
  summary: BatchSummary,
  seq_range: SeqRange,
  completion: Option<FlushCompletion>,
}

//
// SeqAllocator
//

#[derive(Debug, Default)]
struct SeqAllocator {
  reservation: Option<SeqRange>,
  next_seq: u64,
}

impl SeqAllocator {
  fn can_allocate(&self, count: u64) -> bool {
    let Some(reservation) = self.reservation.as_ref() else {
      return false;
    };

    if count == 0 {
      return false;
    }

    let end = self.next_seq.saturating_add(count.saturating_sub(1));
    end <= reservation.end
  }

  fn allocate(&mut self, count: u64) -> Option<SeqRange> {
    if !self.can_allocate(count) {
      return None;
    }

    let start = self.next_seq;
    let end = start.checked_add(count.saturating_sub(1))?;
    self.next_seq = end.saturating_add(1);
    Some(SeqRange { start, end })
  }

  fn set_reservation(&mut self, range: SeqRange) {
    self.next_seq = range.start;
    self.reservation = Some(range);
  }
}

//
// FlushPlan
//

#[derive(Debug)]
struct FlushPlan {
  topic: String,
  partitions: Vec<FlushPartition>,
}

//
// FlushPartition
//

#[derive(Debug)]
struct FlushPartition {
  virtual_partition_id: VirtualPartitionId,
  batches: Vec<BufferedBatch>,
}

//
// SegmentEnvelope
//

#[derive(Debug)]
struct SegmentEnvelope {
  window: TopicWindowKey,
  snowflake_id: SnowflakeId,
  blob_key: BlobKey,
  segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
  compression: Compression,
  record_count: u64,
  min_event_ts_ms: i64,
  max_event_ts_ms: i64,
  created_ts_ms: i64,
}

impl SegmentEnvelope {
  fn into_metadata(self) -> blob_stream_metadata_store::SegmentMetadata {
    blob_stream_metadata_store::SegmentMetadata::new(
      self.window,
      self.snowflake_id,
      self.blob_key,
      self.segment_index,
      self.compression,
      self.record_count,
      self.min_event_ts_ms,
      self.max_event_ts_ms,
      None,
      self.created_ts_ms,
    )
  }
}
