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
use log::{error, trace};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use thiserror::Error;
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use tokio::sync::futures::OwnedNotified;
use tokio::sync::{Notify, oneshot, watch};

const DEFAULT_ZSTD_LEVEL: i32 = 3;
const MAX_IN_FLIGHT_FLUSH_PLANS: usize = 4;
const UPLOADED_OBJECT_SIZE_BUCKETS_BYTES: &[f64] = &[
  64.0 * 1024.0,
  256.0 * 1024.0,
  1024.0 * 1024.0,
  4.0 * 1024.0 * 1024.0,
  5.0 * 1024.0 * 1024.0,
  8.0 * 1024.0 * 1024.0,
  16.0 * 1024.0 * 1024.0,
  32.0 * 1024.0 * 1024.0,
  64.0 * 1024.0 * 1024.0,
  128.0 * 1024.0 * 1024.0,
  512.0 * 1024.0 * 1024.0,
];
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
// BrokerStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct BrokerStateSnapshot {
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
  pub allocation_in_flight: bool,
  pub allocation_started_at: Option<String>,
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

  /// Returns the maximum time an incoming produce RPC may wait for this engine.
  fn produce_request_timeout(&self) -> StdDuration;

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
  flush_batches_max_bytes_total: prometheus::IntCounter,
  flush_batches_max_delay_total: prometheus::IntCounter,
  flush_batches_lease_drain_total: prometheus::IntCounter,
  flush_partitions_total: prometheus::IntCounter,
  flush_plans_total: prometheus::IntCounter,
  flush_failures_total: prometheus::IntCounter,
  flush_latency_seconds: prometheus::Histogram,
  flush_uploaded_object_bytes_total: prometheus::IntCounter,
  flush_uploaded_object_bytes: prometheus::Histogram,
  lease_drain_starts_total: prometheus::IntCounter,
  lease_drain_completions_total: prometheus::IntCounter,
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
      flush_batches_max_bytes_total: scope.counter("flush_batches_max_bytes_total"),
      flush_batches_max_delay_total: scope.counter("flush_batches_max_delay_total"),
      flush_batches_lease_drain_total: scope.counter("flush_batches_lease_drain_total"),
      flush_partitions_total: scope.counter("flush_partitions_total"),
      flush_plans_total: scope.counter("flush_plans_total"),
      flush_failures_total: scope.counter("flush_failures_total"),
      flush_latency_seconds: scope.histogram("flush_latency_seconds"),
      flush_uploaded_object_bytes_total: scope.counter("flush_uploaded_object_bytes_total"),
      flush_uploaded_object_bytes: scope.histogram_with_buckets(
        "flush_uploaded_object_bytes",
        UPLOADED_OBJECT_SIZE_BUCKETS_BYTES,
      ),
      lease_drain_starts_total: scope.counter("lease_drain_starts_total"),
      lease_drain_completions_total: scope.counter("lease_drain_completions_total"),
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
        match partition.trigger {
          FlushTrigger::MaxBytes => self
            .flush_batches_max_bytes_total
            .inc_by(partition.batches.len() as u64),
          FlushTrigger::MaxDelay => self
            .flush_batches_max_delay_total
            .inc_by(partition.batches.len() as u64),
          FlushTrigger::LeaseDrain => self
            .flush_batches_lease_drain_total
            .inc_by(partition.batches.len() as u64),
        }
      }
    }
  }

  #[allow(clippy::cast_precision_loss)] // Prometheus histograms require f64 observations.
  fn record_uploaded_object(&self, payload_bytes: usize) {
    self
      .flush_uploaded_object_bytes_total
      .inc_by(payload_bytes as u64);
    self
      .flush_uploaded_object_bytes
      .observe(payload_bytes as f64);
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
  flush_notifier: Arc<Notify>,
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
      None,
      membership_rx,
      Arc::new(SystemTimeProvider),
      metrics_scope,
    )
  }

  /// Construct a broker with an explicit Sonyflake machine ID for an in-process test cluster.
  pub fn new_with_snowflake_machine_id(
    config: WriteConfig,
    topics: HashMap<String, TopicInfo>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ProducerPartitionLeaseStore>,
    holder_id: String,
    machine_id: u16,
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
      Some(machine_id),
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
      None,
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
    machine_id: Option<u16>,
    membership_rx: Option<watch::Receiver<BrokerMembership>>,
    time_provider: Arc<dyn TimeProvider>,
    metrics_scope: &Scope,
  ) -> Result<Self> {
    let snowflake = match machine_id {
      Some(machine_id) => flush::SnowflakeGenerator::with_machine_id(machine_id)?,
      None => flush::SnowflakeGenerator::new()?,
    };
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
    let flush_context = FlushContext::new(
      config.clone(),
      blob_store,
      metadata_store,
      snowflake,
      Arc::clone(&time_provider),
    );

    let mut engine = Self {
      config,
      topics,
      flush_context,
      lease_store,
      holder_id,
      time_provider,
      metrics: WriteMetrics::new(metrics_scope),
      state,
      flush_notifier: Arc::new(Notify::new()),
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
    let topics = self.topics.clone();
    let time_provider = Arc::clone(&self.time_provider);
    let metrics = self.metrics.clone();
    let flush_notifier = Arc::clone(&self.flush_notifier);

    tokio::spawn(async move {
      let mut ticker = tokio::time::interval(interval);
      let mut flushes = FuturesUnordered::new();
      loop {
        let available_slots = MAX_IN_FLIGHT_FLUSH_PLANS.saturating_sub(flushes.len());
        let now = time_provider.now();
        let plans = collect_flush_plans(
          &state,
          now.unix_timestamp_ms(),
          flush_context.config(),
          &topics,
          available_slots,
        );

        if !plans.is_empty() {
          metrics.record_flush_plan_summary(&plans);
          for plan in plans {
            let flush_context = flush_context.clone();
            let metrics = metrics.clone();
            let state = Arc::clone(&state);
            let now = time_provider.now();
            flushes.push(async move {
              flush_plan_and_notify(&flush_context, plan, now, &metrics, &state).await;
            });
          }
          continue;
        }

        tokio::select! {
          // Timer wakes time-eligible buffers; new writes wake size-eligible buffers.
          _ = ticker.tick() => {},
          () = flush_notifier.notified() => {},
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
    let topic = request.topic;
    let virtual_partition_id = request.virtual_partition_id;
    let mut records = Some(request.records);
    let mut summary = Some(summary);
    let record_count = records
      .as_ref()
      .map(|records| records.len() as u64)
      .unwrap_or_default();

    let (completion_rx, seq_range) = loop {
      let now_ts_ms = self.time_provider.now().unix_timestamp_ms();
      let buffered = {
        let mut state = self.state.lock();
        let partition_state = state.partition_state_mut(&topic, virtual_partition_id);
        if partition_state.draining {
          return Err(WriteError::NotLeaseHolder {
            topic,
            virtual_partition_id,
          });
        }

        if partition_state.allocation_in_flight
          || partition_state.needs_lease(now_ts_ms)
          || !partition_state.seq_allocator.can_allocate(record_count)
        {
          None
        } else {
          let (completion_tx, completion_rx) = oneshot::channel();
          let seq_range = partition_state
            .seq_allocator
            .allocate(record_count)
            .ok_or_else(|| WriteError::Overloaded("sequence reservation exhausted".to_string()))?;
          partition_state.buffer.push(
            BufferedBatch {
              records: records.take().expect("records are buffered only once"),
              summary: summary.take().expect("summary is buffered only once"),
              seq_range: seq_range.clone(),
              completion: Some(completion_tx),
            },
            now_ts_ms,
          );
          Some((completion_rx, seq_range))
        }
      };
      if let Some(buffered) = buffered {
        break buffered;
      }

      let decision = begin_allocation_transition(
        &self.state,
        &topic,
        virtual_partition_id,
        record_count,
        now_ts_ms,
        false,
      );
      match decision {
        AllocationTransitionDecision::Draining => {
          return Err(WriteError::NotLeaseHolder {
            topic,
            virtual_partition_id,
          });
        },
        AllocationTransitionDecision::Ready => {},
        AllocationTransitionDecision::Waiting(notified) => notified.await,
        AllocationTransitionDecision::Claimed(work) => {
          let mut lease_expiration = LeaseExpirationUpdate::Preserve;
          // Sequence reservation is conditionally accepted only for the current active lease
          // holder. When acquisition is required, the lease may be absent or expired, so it must
          // complete before reserving sequences; these calls cannot be run in parallel.
          if work.needs_lease {
            match self
              .ensure_lease(&topic, virtual_partition_id, now_ts_ms)
              .await
            {
              Ok(expires_at) => lease_expiration = LeaseExpirationUpdate::Set(Some(expires_at)),
              Err(error) => {
                work
                  .transition
                  .finish(LeaseExpirationUpdate::Preserve, None);
                return Err(error);
              },
            }
          }

          let mut reservation = None;
          if work.needs_reservation {
            match self
              .reserve_sequences(&topic, virtual_partition_id, now_ts_ms)
              .await
            {
              Ok(range) => reservation = Some(range),
              Err(error) => {
                work.transition.finish(lease_expiration, None);
                return Err(error);
              },
            }
          }
          work.transition.finish(lease_expiration, reservation);
        },
      }
    };

    self.flush_notifier.notify_one();

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

  fn produce_request_timeout(&self) -> StdDuration {
    self.config.produce_request_timeout()
  }

  async fn state_snapshot(&self) -> BrokerStateSnapshot {
    let generated_at_ts_ms = self.time_provider.now().unix_timestamp_ms();
    let generated_at = format_unix_timestamp_ms(generated_at_ts_ms);
    let (membership, mut local_partitions_by_topic) = {
      let state = self.state.lock();
      let mut local_partitions_by_topic = HashMap::new();
      for (topic, topic_state) in &state.topics {
        for (virtual_partition_id, partition_state) in &topic_state.partitions {
          local_partitions_by_topic
            .entry(topic.clone())
            .or_insert_with(Vec::new)
            .push(BrokerPartitionStateSnapshot {
              virtual_partition_id: *virtual_partition_id,
              lease_expires_at: partition_state
                .lease_expiration_ts_ms
                .map(format_unix_timestamp_ms),
              allocation_in_flight: partition_state.allocation_in_flight,
              allocation_started_at: partition_state
                .allocation_started_ts_ms
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
      }
      (state.membership.clone(), local_partitions_by_topic)
    };
    let mut membership_snapshot = membership
      .nodes()
      .unwrap_or_default()
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
            .nodes()
            .unwrap_or_default()
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

async fn flush_plan_and_notify(
  flush_context: &FlushContext,
  mut plan: FlushPlan,
  now: OffsetDateTime,
  metrics: &WriteMetrics,
  state: &Arc<Mutex<WriteState>>,
) {
  let flushed_partitions: Vec<_> = plan
    .partitions
    .iter()
    .map(|partition| partition.virtual_partition_id)
    .collect();
  let mut completions = Vec::new();
  for partition in &mut plan.partitions {
    for batch in &mut partition.batches {
      if let Some(completion) = batch.completion.take() {
        completions.push(completion);
      }
    }
  }

  let flush_started = Instant::now();
  let result = flush_context.flush_plan(&mut plan, now, metrics).await;
  let topic = plan.topic;
  if result.is_err() {
    metrics.flush_failures_total.inc();
  }
  metrics
    .flush_latency_seconds
    .observe(flush_started.elapsed().as_secs_f64());
  mark_flush_complete(state, &topic, &flushed_partitions);
  let completion_result = result
    .as_ref()
    .map_or_else(|error| Err(error.to_string()), |_ok| Ok(()));
  for completion in completions {
    let _ignored = completion.send(completion_result.clone());
  }
}

fn mark_flush_complete(
  state: &Arc<Mutex<WriteState>>,
  topic: &str,
  virtual_partition_ids: &[VirtualPartitionId],
) {
  let mut drain_notifiers = Vec::new();
  {
    let mut state = state.lock();
    for virtual_partition_id in virtual_partition_ids {
      let Some(partition_state) =
        state.partition_state_mut_if_present(topic, *virtual_partition_id)
      else {
        continue;
      };
      partition_state.flush_in_flight = false;
      drain_notifiers.push(Arc::clone(&partition_state.drain_notify));
    }
  }
  for drain_notify in drain_notifiers {
    drain_notify.notify_waiters();
  }
}

fn collect_flush_plans(
  state: &Arc<Mutex<WriteState>>,
  now_ts_ms: i64,
  config: &WriteConfig,
  topics: &HashMap<String, TopicInfo>,
  max_plans: usize,
) -> Vec<FlushPlan> {
  if max_plans == 0 {
    return Vec::new();
  }
  let mut state = state.lock();
  let mut partition_keys = state.partition_keys();
  partition_keys.sort_unstable();
  let last_flush_topic = state.last_flush_topic.clone();
  let start = last_flush_topic.as_ref().map_or(0, |last_topic| {
    partition_keys
      .iter()
      .position(|(topic, _)| topic > last_topic)
      .unwrap_or(0)
  });
  let partition_state_count = partition_keys.len();
  let mut plans_by_topic: HashMap<String, Vec<FlushPartition>> = HashMap::new();
  let mut last_planned_topic = None;

  for (topic, virtual_partition_id) in partition_keys
    .iter()
    .cycle()
    .skip(start)
    .take(partition_state_count)
  {
    if !topics.contains_key(topic) {
      error!(
        "refusing to flush state for unknown topic: topic={topic}, \
         virtual_partition_id={virtual_partition_id}"
      );
      continue;
    }
    let is_new_topic = !plans_by_topic.contains_key(topic);
    if is_new_topic && plans_by_topic.len() == max_plans {
      continue;
    }
    let Some(partition_state) = state.partition_state_mut_if_present(topic, *virtual_partition_id)
    else {
      continue;
    };
    if partition_state.flush_in_flight {
      continue;
    }
    let flush_trigger = if partition_state.draining {
      Some(FlushTrigger::LeaseDrain)
    } else {
      partition_state.buffer.flush_trigger(now_ts_ms, config)
    };
    let Some(flush_trigger) = flush_trigger else {
      continue;
    };

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
      .entry(topic.clone())
      .or_default()
      .push(FlushPartition {
        virtual_partition_id: *virtual_partition_id,
        batches,
        trigger: flush_trigger,
      });
    if is_new_topic {
      last_planned_topic = Some(topic.clone());
    }
  }

  if let Some(last_planned_topic) = last_planned_topic {
    state.last_flush_topic = Some(last_planned_topic);
  }

  plans_by_topic
    .into_iter()
    .map(|(topic, partitions)| FlushPlan {
      max_metadata_publication_lag_ms: topics
        .get(&topic)
        .expect("flush plans are created only for configured topics")
        .max_metadata_publication_lag_ms,
      topic,
      partitions,
    })
    .collect()
}

//
// WriteState
//

#[derive(Debug, Default)]
struct WriteState {
  membership: BrokerMembership,
  topics: HashMap<String, TopicState>,
  last_flush_topic: Option<String>,
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

  fn partition_state(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> Option<&PartitionState> {
    self
      .topics
      .get(topic)
      .and_then(|topic_state| topic_state.partitions.get(&virtual_partition_id))
  }

  fn partition_state_mut_if_present(
    &mut self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> Option<&mut PartitionState> {
    self
      .topics
      .get_mut(topic)
      .and_then(|topic_state| topic_state.partitions.get_mut(&virtual_partition_id))
  }

  fn partition_keys(&self) -> Vec<(String, VirtualPartitionId)> {
    self
      .topics
      .iter()
      .flat_map(|(topic, topic_state)| {
        topic_state
          .partitions
          .keys()
          .map(|virtual_partition_id| (topic.clone(), *virtual_partition_id))
      })
      .collect()
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
  flush_in_flight: bool,
  allocation_in_flight: bool,
  allocation_started_ts_ms: Option<i64>,
  allocation_notify: Arc<Notify>,
  draining: bool,
  drain_notify: Arc<Notify>,
}

impl PartitionState {
  fn needs_lease(&self, now_ts_ms: i64) -> bool {
    self
      .lease_expiration_ts_ms
      .is_none_or(|expires_at| now_ts_ms >= expires_at)
  }

  fn is_drained(&self) -> bool {
    !self.flush_in_flight && !self.allocation_in_flight && self.buffer.batches.is_empty()
  }
}

//
// AllocationTransition
//

struct AllocationTransition {
  state: Arc<Mutex<WriteState>>,
  topic: String,
  virtual_partition_id: VirtualPartitionId,
  finished: bool,
}

impl AllocationTransition {
  fn finish(
    mut self,
    lease_expiration_update: LeaseExpirationUpdate,
    reservation: Option<SeqRange>,
  ) {
    self.finish_inner(lease_expiration_update, reservation);
  }

  fn finish_inner(
    &mut self,
    lease_expiration_update: LeaseExpirationUpdate,
    reservation: Option<SeqRange>,
  ) {
    if self.finished {
      return;
    }

    let (allocation_notify, drain_notify) = {
      let mut state = self.state.lock();
      let partition_state = state.partition_state_mut(&self.topic, self.virtual_partition_id);
      if let LeaseExpirationUpdate::Set(lease_expiration_ts_ms) = lease_expiration_update {
        partition_state.lease_expiration_ts_ms = lease_expiration_ts_ms;
      }
      if let Some(reservation) = reservation {
        partition_state.seq_allocator.set_reservation(reservation);
      }
      partition_state.allocation_in_flight = false;
      partition_state.allocation_started_ts_ms = None;
      (
        Arc::clone(&partition_state.allocation_notify),
        Arc::clone(&partition_state.drain_notify),
      )
    };
    self.finished = true;
    allocation_notify.notify_waiters();
    drain_notify.notify_waiters();
  }
}

impl Drop for AllocationTransition {
  fn drop(&mut self) {
    self.finish_inner(LeaseExpirationUpdate::Preserve, None);
  }
}

//
// LeaseExpirationUpdate
//

#[derive(Clone, Copy)]
enum LeaseExpirationUpdate {
  Preserve,
  Set(Option<i64>),
}

//
// AllocationTransitionWork
//

struct AllocationTransitionWork {
  transition: AllocationTransition,
  needs_lease: bool,
  needs_reservation: bool,
}

//
// AllocationTransitionDecision
//

enum AllocationTransitionDecision {
  Draining,
  Ready,
  Waiting(OwnedNotified),
  Claimed(AllocationTransitionWork),
}

fn begin_allocation_transition(
  state: &Arc<Mutex<WriteState>>,
  topic: &str,
  virtual_partition_id: VirtualPartitionId,
  record_count: u64,
  now_ts_ms: i64,
  renew_lease: bool,
) -> AllocationTransitionDecision {
  let mut state_guard = state.lock();
  let partition_state = state_guard.partition_state_mut(topic, virtual_partition_id);
  if partition_state.draining && !renew_lease {
    return AllocationTransitionDecision::Draining;
  }
  if partition_state.allocation_in_flight {
    return AllocationTransitionDecision::Waiting(
      Arc::clone(&partition_state.allocation_notify).notified_owned(),
    );
  }

  let needs_lease = renew_lease || partition_state.needs_lease(now_ts_ms);
  let needs_reservation = !partition_state.seq_allocator.can_allocate(record_count);
  if !needs_lease && !needs_reservation {
    return AllocationTransitionDecision::Ready;
  }

  partition_state.allocation_in_flight = true;
  partition_state.allocation_started_ts_ms = Some(now_ts_ms);
  AllocationTransitionDecision::Claimed(AllocationTransitionWork {
    transition: AllocationTransition {
      state: Arc::clone(state),
      topic: topic.to_string(),
      virtual_partition_id,
      finished: false,
    },
    needs_lease,
    needs_reservation,
  })
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

  fn flush_trigger(&self, now_ts_ms: i64, config: &WriteConfig) -> Option<FlushTrigger> {
    if self.batches.is_empty() {
      return None;
    }

    if self.buffered_bytes >= config.flush_max_bytes {
      return Some(FlushTrigger::MaxBytes);
    }

    let first_ts = self.first_buffered_ts_ms?;

    (now_ts_ms.saturating_sub(first_ts) >= config.flush_max_delay_ms)
      .then_some(FlushTrigger::MaxDelay)
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
  max_metadata_publication_lag_ms: u64,
}

//
// FlushPartition
//

#[derive(Debug)]
struct FlushPartition {
  virtual_partition_id: VirtualPartitionId,
  batches: Vec<BufferedBatch>,
  trigger: FlushTrigger,
}

#[derive(Clone, Copy, Debug)]
enum FlushTrigger {
  MaxBytes,
  MaxDelay,
  LeaseDrain,
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
  record_count: u64,
  created_ts_ms: i64,
}

impl SegmentEnvelope {
  fn into_metadata(
    self,
    metadata_published_ts_ms: i64,
  ) -> blob_stream_metadata_store::SegmentMetadata {
    blob_stream_metadata_store::SegmentMetadata::new(
      self.window,
      self.snowflake_id,
      self.blob_key,
      self.segment_index,
      self.created_ts_ms,
      metadata_published_ts_ms,
    )
  }
}
