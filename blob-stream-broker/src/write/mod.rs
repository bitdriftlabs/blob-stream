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
use bd_server_stats::stats::Scope;
use bd_time::{OffsetDateTimeExt, SystemTimeProvider, TimeProvider};
use blob_stream_blob_store::{BlobKey, BlobStore};
use blob_stream_broker_discovery::{
  BrokerMembership,
  BrokerNode,
  balanced_assignment,
  writer_virtual_partitions,
};
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
  format_unix_timestamp_ms,
};
pub use config::{TopicInfo, WriteConfig, build_write_engine};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{debug, trace};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use thiserror::Error;
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot, watch};

const DEFAULT_ZSTD_LEVEL: i32 = 3;
const MAX_IN_FLIGHT_FLUSH_PLANS: usize = 4;
type FlushCompletion = oneshot::Sender<Result<(), String>>;
type PartitionStateHandle = Arc<Mutex<PartitionState>>;

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
// BrokerStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerStateSnapshot {
  pub schema_version: u32,
  pub generated_at: String,
  pub holder_id: String,
  pub writer_id: u32,
  pub flush_max_bytes: u64,
  pub flush_max_delay_ms: i64,
  pub membership: Vec<BrokerNodeSnapshot>,
  pub ownership: Vec<BrokerPartitionOwnershipSnapshot>,
  pub topics: Vec<BrokerTopicStateSnapshot>,
}

//
// BrokerNodeSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BrokerNodeSnapshot {
  pub node_id: String,
  pub address: String,
}

//
// BrokerPartitionOwnershipSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerPartitionOwnershipSnapshot {
  pub topic: String,
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
  pub holder_address: Option<String>,
  pub expires_at: String,
  pub is_active: bool,
}

//
// BrokerTopicStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerTopicStateSnapshot {
  pub name: String,
  pub partition_count: u32,
  pub num_writers: u32,
  pub retention_days: u32,
  pub local_partitions: Vec<BrokerPartitionStateSnapshot>,
}

//
// BrokerPartitionStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerPartitionStateSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub lease_expires_at: Option<String>,
  pub buffered_batch_count: usize,
  pub buffered_record_count: usize,
  pub buffered_bytes: u64,
  pub first_buffered_at: Option<String>,
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

  /// Returns a best-effort snapshot of state held by this broker process.
  async fn state_snapshot(&self) -> BrokerStateSnapshot;
}

//
// WriteEngineImpl
//

#[derive(Clone)]
struct WriteMetrics {
  produce_requests_total: prometheus::IntCounter,
  produce_records_total: prometheus::IntCounter,
  produce_payload_bytes_total: prometheus::IntCounter,
  produce_ok_total: prometheus::IntCounter,
  produce_not_lease_holder_total: prometheus::IntCounter,
  produce_overloaded_total: prometheus::IntCounter,
  produce_unknown_topic_total: prometheus::IntCounter,
  produce_latency_seconds: prometheus::Histogram,
  sequence_reservations_total: prometheus::IntCounter,
  sequence_reservation_records_total: prometheus::IntCounter,
  sequence_reservation_failures_total: prometheus::IntCounter,
  sequence_reservation_latency_seconds: prometheus::Histogram,
  flush_batches_total: prometheus::IntCounter,
  flush_partitions_total: prometheus::IntCounter,
  flush_plans_total: prometheus::IntCounter,
  flush_failures_total: prometheus::IntCounter,
  flush_latency_seconds: prometheus::Histogram,
}

impl WriteMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("write");
    Self {
      produce_requests_total: scope.counter("produce_requests_total"),
      produce_records_total: scope.counter("produce_records_total"),
      produce_payload_bytes_total: scope.counter("produce_payload_bytes_total"),
      produce_ok_total: scope.counter("produce_ok_total"),
      produce_not_lease_holder_total: scope.counter("produce_not_lease_holder_total"),
      produce_overloaded_total: scope.counter("produce_overloaded_total"),
      produce_unknown_topic_total: scope.counter("produce_unknown_topic_total"),
      produce_latency_seconds: scope.histogram("produce_latency_seconds"),
      sequence_reservations_total: scope.counter("sequence_reservations_total"),
      sequence_reservation_records_total: scope.counter("sequence_reservation_records_total"),
      sequence_reservation_failures_total: scope.counter("sequence_reservation_failures_total"),
      sequence_reservation_latency_seconds: scope.histogram("sequence_reservation_latency_seconds"),
      flush_batches_total: scope.counter("flush_batches_total"),
      flush_partitions_total: scope.counter("flush_partitions_total"),
      flush_plans_total: scope.counter("flush_plans_total"),
      flush_failures_total: scope.counter("flush_failures_total"),
      flush_latency_seconds: scope.histogram("flush_latency_seconds"),
    }
  }

  fn record_produce_error(&self, error: &WriteError) {
    match error {
      WriteError::UnknownTopic(_) => self.produce_unknown_topic_total.inc(),
      WriteError::NotLeaseHolder { .. } => self.produce_not_lease_holder_total.inc(),
      WriteError::InvalidPartition { .. } | WriteError::Overloaded(_) | WriteError::Internal(_) => {
        self.produce_overloaded_total.inc();
      },
    }
  }

  fn record_flush_plan_summary(&self, plans: &[FlushPlan]) {
    self.flush_plans_total.inc_by(plans.len() as u64);
    for plan in plans {
      self
        .flush_partitions_total
        .inc_by(plan.partitions.len() as u64);
      for partition in &plan.partitions {
        self
          .flush_batches_total
          .inc_by(partition.batches.len() as u64);
      }
    }
  }

  fn record_sequence_reservation(&self, range: &SeqRange) {
    self.sequence_reservations_total.inc();
    self
      .sequence_reservation_records_total
      .inc_by(range.end.saturating_sub(range.start).saturating_add(1));
  }
}

pub struct WriteEngineImpl {
  config: WriteConfig,
  topics: HashMap<String, TopicInfo>,
  flush_context: FlushContext,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  holder_id: String,
  time_provider: Arc<dyn TimeProvider>,
  metrics: WriteMetrics,
  state: Arc<Mutex<WriteState>>,
  flush_permits: Arc<Semaphore>,
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
    metrics_scope: &Scope,
  ) -> Result<Self> {
    Self::new_with_time_provider_and_scope(
      config,
      topics,
      blob_store,
      metadata_store,
      lease_store,
      holder_id,
      membership_rx,
      Arc::new(SystemTimeProvider),
      metrics_scope,
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
    metrics_scope: &Scope,
  ) -> Result<Self> {
    Self::new_with_time_provider_and_scope(
      config,
      topics,
      blob_store,
      metadata_store,
      lease_store,
      holder_id,
      membership_rx,
      time_provider,
      metrics_scope,
    )
  }

  fn new_with_time_provider_and_scope(
    config: WriteConfig,
    topics: HashMap<String, TopicInfo>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ProducerPartitionLeaseStore>,
    holder_id: String,
    membership_rx: Option<watch::Receiver<BrokerMembership>>,
    time_provider: Arc<dyn TimeProvider>,
    metrics_scope: &Scope,
  ) -> Result<Self> {
    let snowflake = flush::SnowflakeGenerator::new()?;
    let initial_membership = membership_rx.as_ref().map_or_else(
      || {
        BrokerMembership::new(vec![BrokerNode {
          node_id: holder_id.clone(),
          address: holder_id.clone(),
        }])
      },
      |membership_rx| membership_rx.borrow().clone(),
    );
    let state = Arc::new(Mutex::new(WriteState {
      membership: initial_membership,
      ..Default::default()
    }));
    let flush_context = FlushContext::new(config.clone(), blob_store, metadata_store, snowflake);

    let mut engine = Self {
      config,
      topics,
      flush_context,
      lease_store,
      holder_id,
      time_provider,
      metrics: WriteMetrics::new(metrics_scope),
      state,
      flush_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT_FLUSH_PLANS)),
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
      virtual_partition_id,
    };
    let started = Instant::now();

    let outcome = self
      .lease_store
      .reserve_sequences(
        &key,
        &self.holder_id,
        now_ts_ms,
        self.config.reservation_size,
      )
      .await
      .context("reserve sequences");
    self
      .metrics
      .sequence_reservation_latency_seconds
      .observe(started.elapsed().as_secs_f64());

    match outcome {
      Ok(SequenceReservationOutcome::Reserved(reservation)) => {
        self.metrics.record_sequence_reservation(&reservation.range);
        Ok(reservation.range)
      },
      Ok(SequenceReservationOutcome::HeldByOther(_) | SequenceReservationOutcome::Expired) => {
        self.metrics.sequence_reservation_failures_total.inc();
        Err(WriteError::NotLeaseHolder {
          topic: topic.to_string(),
          virtual_partition_id,
        })
      },
      Err(error) => {
        self.metrics.sequence_reservation_failures_total.inc();
        Err(error.into())
      },
    }
  }

  fn spawn_flush_loop(&self) {
    let interval_ms = self.config.flush_max_delay_ms.max(1).cast_unsigned();
    let interval = StdDuration::from_millis(interval_ms);
    let flush_context = self.flush_context.clone();
    let state = Arc::clone(&self.state);
    let time_provider = Arc::clone(&self.time_provider);
    let metrics = self.metrics.clone();
    let flush_permits = Arc::clone(&self.flush_permits);

    tokio::spawn(async move {
      let mut ticker = tokio::time::interval(interval);
      let mut pending_plans = VecDeque::new();
      let mut flushes = FuturesUnordered::new();
      loop {
        while let Some(plan) = pending_plans.pop_front() {
          let permit = match Arc::clone(&flush_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
              pending_plans.push_front(plan);
              break;
            },
            Err(tokio::sync::TryAcquireError::Closed) => {
              flush_plan_and_notify(
                &flush_context,
                plan,
                time_provider.now(),
                &metrics,
                &state,
                Err(WriteError::Internal(anyhow!("flush scheduler stopped"))),
              )
              .await;
              while let Some(plan) = pending_plans.pop_front() {
                flush_plan_and_notify(
                  &flush_context,
                  plan,
                  time_provider.now(),
                  &metrics,
                  &state,
                  Err(WriteError::Internal(anyhow!("flush scheduler stopped"))),
                )
                .await;
              }
              return;
            },
          };
          let flush_context = flush_context.clone();
          let metrics = metrics.clone();
          let state = Arc::clone(&state);
          let now = time_provider.now();
          flushes.push(async move {
            flush_plan_and_notify(&flush_context, plan, now, &metrics, &state, Ok(permit)).await;
          });
        }

        tokio::select! {
          // Time-based flushing is driven here. Persisting prior plans must not stop the timer
          // from collecting a later buffer epoch that has become eligible.
          _ = ticker.tick() => {
            let now = time_provider.now();
            let now_ts_ms = now.unix_timestamp_ms();
            let plans = collect_flush_plans(&state, now_ts_ms, flush_context.config()).await;

            if !plans.is_empty() {
              metrics.record_flush_plan_summary(&plans);
              pending_plans.extend(plans);
            }
          },
          Some(()) = flushes.next(), if !flushes.is_empty() => {},
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
    let started = Instant::now();
    self.metrics.produce_requests_total.inc();
    self
      .metrics
      .produce_records_total
      .inc_by(request.records.len() as u64);
    self.metrics.produce_payload_bytes_total.inc_by(
      request
        .records
        .iter()
        .map(|record| record.payload.len() as u64)
        .sum::<u64>(),
    );

    trace!(
      "broker write request accepted: topic={}, virtual_partition_id={}, records={}",
      request.topic,
      request.virtual_partition_id,
      request.records.len()
    );

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

    let summary = RecordBatch::summary_from_records(&request.records)
      .ok_or_else(|| anyhow!("failed to summarize record batch"))?;

    let now = self.time_provider.now();
    let now_ts_ms = now.unix_timestamp_ms();
    let partition_state = {
      let mut state = self.state.lock().await;
      state.partition_state(&request.topic, request.virtual_partition_id)
    };
    let mut partition_state = partition_state.lock().await;

    let needs_lease = partition_state.needs_lease(now_ts_ms);

    if needs_lease {
      let expires_at = self
        .ensure_lease(&request.topic, request.virtual_partition_id, now_ts_ms)
        .await?;
      partition_state.lease_expiration_ts_ms = Some(expires_at);
    }

    let record_count = request.records.len() as u64;
    let needs_reservation = !partition_state.seq_allocator.can_allocate(record_count);

    if needs_reservation {
      let range = self
        .reserve_sequences(&request.topic, request.virtual_partition_id, now_ts_ms)
        .await?;
      partition_state.seq_allocator.set_reservation(range);
    }

    let (completion_tx, completion_rx) = oneshot::channel();

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
    drop(partition_state);

    // After adding this incoming batch, we immediately run flush planning once. This enables
    // "flush on size" behavior in-line with produce: if the just-pushed data makes the buffer
    // cross flush_max_bytes, we flush now instead of waiting for the background ticker.
    //
    // If thresholds are not met yet, no plans are produced and data stays buffered until either
    // a future produce call or the periodic flush loop observes the time threshold.
    let plans = collect_flush_plans(&self.state, now_ts_ms, &self.config).await;

    if !plans.is_empty() {
      self.metrics.record_flush_plan_summary(&plans);
      flush_plans_and_notify(
        &self.flush_context,
        plans,
        now,
        &self.metrics,
        &self.state,
        &self.flush_permits,
      )
      .await;

      debug!(
        "broker flushed buffered write data inline: topic={}, virtual_partition_id={}",
        request.topic, request.virtual_partition_id
      );
    }

    match completion_rx.await {
      Ok(Ok(())) => {},
      Ok(Err(error)) => {
        let write_error = WriteError::Internal(anyhow!(error));
        self.metrics.record_produce_error(&write_error);
        self
          .metrics
          .produce_latency_seconds
          .observe(started.elapsed().as_secs_f64());
        return Err(write_error);
      },
      Err(_closed) => {
        let write_error = WriteError::Internal(anyhow!(
          "flush completion channel closed before acknowledgment"
        ));
        self.metrics.record_produce_error(&write_error);
        self
          .metrics
          .produce_latency_seconds
          .observe(started.elapsed().as_secs_f64());
        return Err(write_error);
      },
    }

    self.metrics.produce_ok_total.inc();
    self
      .metrics
      .produce_latency_seconds
      .observe(started.elapsed().as_secs_f64());

    Ok(WriteResponse { seq_range })
  }

  async fn state_snapshot(&self) -> BrokerStateSnapshot {
    let generated_at_ts_ms = self.time_provider.now().unix_timestamp_ms();
    let generated_at = format_unix_timestamp_ms(generated_at_ts_ms);
    let (membership, partition_states) = {
      let state = self.state.lock().await;
      (
        state.membership.clone(),
        state.partition_states_for_all_topics(),
      )
    };
    let mut membership_snapshot = membership
      .nodes
      .iter()
      .map(|node| BrokerNodeSnapshot {
        node_id: node.node_id.clone(),
        address: node.address.clone(),
      })
      .collect::<Vec<_>>();
    membership_snapshot.sort_by(|left, right| {
      left
        .node_id
        .cmp(&right.node_id)
        .then_with(|| left.address.cmp(&right.address))
    });

    let mut local_partitions_by_topic = HashMap::new();
    for (topic, virtual_partition_id, partition_state) in partition_states {
      let partition_state = partition_state.lock().await;
      local_partitions_by_topic
        .entry(topic)
        .or_insert_with(Vec::new)
        .push(BrokerPartitionStateSnapshot {
          virtual_partition_id,
          lease_expires_at: partition_state
            .lease_expiration_ts_ms
            .map(format_unix_timestamp_ms),
          buffered_batch_count: partition_state.buffer.batches.len(),
          buffered_record_count: partition_state
            .buffer
            .batches
            .iter()
            .map(|batch| batch.records.len())
            .sum(),
          buffered_bytes: partition_state.buffer.buffered_bytes,
          first_buffered_at: partition_state
            .buffer
            .first_buffered_ts_ms
            .map(format_unix_timestamp_ms),
          sequence_reservation: partition_state.seq_allocator.reservation.as_ref().map(
            |reservation| SequenceReservationSnapshot {
              start: reservation.start,
              end: reservation.end,
            },
          ),
          next_sequence: partition_state.seq_allocator.next_seq,
        });
    }

    let mut topics = self
      .topics
      .values()
      .map(|topic| {
        let mut local_partitions = local_partitions_by_topic
          .remove(&topic.name)
          .unwrap_or_default();
        local_partitions.sort_by_key(|partition| partition.virtual_partition_id);
        BrokerTopicStateSnapshot {
          name: topic.name.clone(),
          partition_count: topic.partition_count,
          num_writers: topic.num_writers,
          retention_days: topic.retention_days,
          local_partitions,
        }
      })
      .collect::<Vec<_>>();
    topics.sort_by(|left, right| left.name.cmp(&right.name));

    let assignment = balanced_assignment(
      writer_virtual_partitions(
        self
          .topics
          .values()
          .map(|topic| (topic.name.clone(), topic.partition_count, topic.num_writers)),
        self.config.writer_id,
      ),
      &membership,
    );
    let mut ownership = Vec::new();
    for (partition, assigned_broker) in assignment {
      let topic = self
        .topics
        .get(&partition.topic)
        .expect("assignment must reference a configured topic");
      let key = ProducerPartitionLeaseKey {
        topic: partition.topic.clone(),
        virtual_partition_id: partition.virtual_partition_id,
      };
      let lease = self.lease_store.get_lease(&key).await;
      let assignment_is_local = assigned_broker.node_id == self.holder_id;
      let assigned_broker = BrokerNodeSnapshot {
        node_id: assigned_broker.node_id,
        address: assigned_broker.address,
      };

      let (lease_status, observed_lease) = match lease {
        Ok(Some(lease)) => {
          let is_active = lease.lease_expiration_ts_ms > generated_at_ts_ms;
          let holder_address = membership
            .nodes
            .iter()
            .find(|node| node.node_id == lease.holder_id)
            .map(|node| node.address.clone());
          let lease_status = if !is_active {
            BrokerLeaseStatus::UnleasedOrExpired
          } else if lease.holder_id == self.holder_id {
            BrokerLeaseStatus::LocalActive
          } else {
            BrokerLeaseStatus::RemoteActive
          };
          (
            lease_status,
            Some(BrokerLeaseSnapshot {
              holder_id: lease.holder_id,
              holder_address,
              expires_at: format_unix_timestamp_ms(lease.lease_expiration_ts_ms),
              is_active,
            }),
          )
        },
        Ok(None) => {
          let status = if assignment_is_local {
            BrokerLeaseStatus::AssignedLocalPending
          } else {
            BrokerLeaseStatus::AssignedRemotePending
          };
          (status, None)
        },
        Err(error) => {
          warn_every!(
            15.seconds(),
            "broker state lease lookup failed: topic={}, virtual_partition_id={}, error={error}",
            partition.topic,
            partition.virtual_partition_id,
          );
          (BrokerLeaseStatus::LookupFailed, None)
        },
      };

      ownership.push(BrokerPartitionOwnershipSnapshot {
        topic: partition.topic,
        virtual_partition_id: partition.virtual_partition_id,
        producer_writer_id: self.config.writer_id,
        logical_partition_id: partition.virtual_partition_id % topic.partition_count,
        assigned_broker: Some(assigned_broker),
        assignment_is_local,
        lease_status,
        observed_lease,
      });
    }
    ownership.sort_by(|left, right| {
      (&left.topic, left.virtual_partition_id).cmp(&(&right.topic, right.virtual_partition_id))
    });

    BrokerStateSnapshot {
      schema_version: 3,
      generated_at,
      holder_id: self.holder_id.clone(),
      writer_id: self.config.writer_id,
      flush_max_bytes: self.config.flush_max_bytes,
      flush_max_delay_ms: self.config.flush_max_delay_ms,
      membership: membership_snapshot,
      ownership,
      topics,
    }
  }
}

async fn flush_plans_and_notify(
  flush_context: &FlushContext,
  plans: Vec<FlushPlan>,
  now: OffsetDateTime,
  metrics: &WriteMetrics,
  state: &Arc<Mutex<WriteState>>,
  flush_permits: &Arc<Semaphore>,
) {
  let mut flushes = FuturesUnordered::new();
  for plan in plans {
    let flush_context = flush_context.clone();
    let metrics = metrics.clone();
    let state = Arc::clone(state);
    let flush_permits = Arc::clone(flush_permits);
    flushes.push(async move {
      let permit = flush_permits
        .acquire_owned()
        .await
        .map_err(|_| WriteError::Internal(anyhow!("flush scheduler stopped")));
      flush_plan_and_notify(&flush_context, plan, now, &metrics, &state, permit).await;
    });
  }

  // All plans are dispatched before this waits, so a slow durable write cannot serialize another
  // plan in the same inline flush.
  while flushes.next().await.is_some() {}
}

async fn flush_plan_and_notify(
  flush_context: &FlushContext,
  mut plan: FlushPlan,
  now: OffsetDateTime,
  metrics: &WriteMetrics,
  state: &Arc<Mutex<WriteState>>,
  permit: Result<OwnedSemaphorePermit, WriteError>,
) {
  let flushed_partitions: Vec<_> = plan
    .partitions
    .iter()
    .map(|partition| partition.virtual_partition_id)
    .collect();
  let topic = plan.topic.clone();
  let mut completions = Vec::new();
  for partition in &mut plan.partitions {
    for batch in &mut partition.batches {
      if let Some(completion) = batch.completion.take() {
        completions.push(completion);
      }
    }
  }

  let result = match permit {
    Ok(_permit) => {
      let flush_started = Instant::now();
      let result = flush_context.flush_plan(plan, now).await;
      if result.is_err() {
        metrics.flush_failures_total.inc();
      }
      metrics
        .flush_latency_seconds
        .observe(flush_started.elapsed().as_secs_f64());
      result
    },
    Err(error) => {
      metrics.flush_failures_total.inc();
      Err(error)
    },
  };
  mark_flush_complete(state, &topic, &flushed_partitions).await;
  let completion_result = result
    .as_ref()
    .map_or_else(|error| Err(error.to_string()), |_ok| Ok(()));
  for completion in completions {
    let _ignored = completion.send(completion_result.clone());
  }
}

async fn mark_flush_complete(
  state: &Arc<Mutex<WriteState>>,
  topic: &str,
  virtual_partition_ids: &[VirtualPartitionId],
) {
  let partition_states = {
    let state = state.lock().await;
    state.partition_states(topic, virtual_partition_ids)
  };
  for partition_state in partition_states {
    partition_state.lock().await.flush_in_flight = false;
  }
}

async fn collect_flush_plans(
  state: &Arc<Mutex<WriteState>>,
  now_ts_ms: i64,
  config: &WriteConfig,
) -> Vec<FlushPlan> {
  let partition_states = { state.lock().await.partition_states_for_all_topics() };
  let mut plans_by_topic: HashMap<String, Vec<FlushPartition>> = HashMap::new();

  for (topic, virtual_partition_id, partition_state) in partition_states {
    let mut partition_state = partition_state.lock().await;
    if partition_state.flush_in_flight || !partition_state.buffer.should_flush(now_ts_ms, config) {
      continue;
    }

    // Once flush is triggered, all currently buffered batches for the partition are moved as one
    // partition plan. New writes arriving later start a new buffer epoch.
    let batches = std::mem::take(&mut partition_state.buffer.batches);
    if batches.is_empty() {
      partition_state.buffer.reset();
      continue;
    }

    partition_state.buffer.reset();
    partition_state.flush_in_flight = true;
    plans_by_topic
      .entry(topic)
      .or_default()
      .push(FlushPartition {
        virtual_partition_id,
        batches,
      });
  }

  plans_by_topic
    .into_iter()
    .map(|(topic, partitions)| FlushPlan { topic, partitions })
    .collect()
}

//
// WriteState
//

#[derive(Debug, Default)]
struct WriteState {
  membership: BrokerMembership,
  topics: HashMap<String, TopicState>,
}

impl WriteState {
  fn partition_state(
    &mut self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> PartitionStateHandle {
    Arc::clone(
      self
        .topics
        .entry(topic.to_string())
        .or_default()
        .partitions
        .entry(virtual_partition_id)
        .or_insert_with(|| Arc::new(Mutex::new(PartitionState::default()))),
    )
  }

  fn partition_states(
    &self,
    topic: &str,
    virtual_partition_ids: &[VirtualPartitionId],
  ) -> Vec<PartitionStateHandle> {
    let Some(topic_state) = self.topics.get(topic) else {
      return Vec::new();
    };
    virtual_partition_ids
      .iter()
      .filter_map(|virtual_partition_id| topic_state.partitions.get(virtual_partition_id))
      .cloned()
      .collect()
  }

  fn partition_states_for_all_topics(
    &self,
  ) -> Vec<(String, VirtualPartitionId, PartitionStateHandle)> {
    self
      .topics
      .iter()
      .flat_map(|(topic, topic_state)| {
        topic_state
          .partitions
          .iter()
          .map(|(virtual_partition_id, partition_state)| {
            (
              topic.clone(),
              *virtual_partition_id,
              Arc::clone(partition_state),
            )
          })
      })
      .collect()
  }
}

//
// TopicState
//

#[derive(Debug, Default)]
struct TopicState {
  partitions: HashMap<VirtualPartitionId, PartitionStateHandle>,
}

//
// PartitionState
//

#[derive(Debug, Default)]
struct PartitionState {
  buffer: BufferState,
  seq_allocator: SeqAllocator,
  lease_expiration_ts_ms: Option<i64>,
  flush_in_flight: bool,
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
