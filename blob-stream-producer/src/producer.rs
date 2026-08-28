#[cfg(test)]
#[path = "./producer_test.rs"]
mod tests;

mod diagnostics;
mod dispatch;
mod metrics;
mod protocol;
mod retry;
mod routing;
mod state;
mod transport;

use crate::config::{
  ProducerConfig,
  ProducerRuntimeConfig,
  ProducerTopicConfig,
  apply_producer_startup_overrides,
  into_discovery,
  producer_flush_max_delay,
  producer_max_batch_bytes,
  producer_max_batch_records,
  producer_max_request_concurrency,
  producer_writer_id,
  validate_producer_config,
  validate_runtime_config,
  validate_topic_config,
};
use anyhow::{Result, anyhow, bail, ensure};
use async_trait::async_trait;
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use bd_server_stats::stats::Scope;
use blob_stream_broker_discovery::{
  BrokerDiscovery,
  BrokerMembership,
  BrokerPartition,
  INITIAL_MEMBERSHIP_TIMEOUT,
  wait_for_initialized_membership,
};
use blob_stream_proto::protos::blobstream::v1::broker::Record;
use blob_stream_types::{
  MAX_PRODUCE_BATCHES_REQUEST_BYTES,
  VirtualPartitionId,
  virtual_partition_for_key,
};
use bytes::Bytes;
use diagnostics::ProducerRetryDiagnostics;
pub use diagnostics::{
  ProducerDiagnostics,
  ProducerPartitionBufferSnapshot,
  ProducerRetryReason,
  ProducerRetrySample,
  ProducerRetrySummary,
  ProducerStateSnapshot,
  ProducerTopicSnapshot,
};
use dispatch::{notify_unassigned_batches, send_grouped_batches_and_notify};
use log::{debug, trace};
use metrics::ProducerMetrics;
use parking_lot::Mutex;
use protobuf::Chars;
pub use retry::ProducerRetryClock;
use routing::{ProducerRoutes, record_wire_sizes};
use state::{BufferedRecord, BulkCompletion, ProducerState, RecordCompletion};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use thiserror::Error;
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, timeout};
pub use transport::{BrokerTransport, GrpcBrokerTransport};

// TODO(mattklein123): Consider adding disk buffering of segments.

//
// ProducerRecord
//

#[derive(Clone, Debug)]
/// A single producer input record.
pub struct ProducerRecord {
  /// Target topic name.
  pub topic: Chars,
  /// Partitioning key used to derive logical and virtual partition assignment.
  pub record_key: Vec<u8>,
  /// Opaque payload bytes.
  pub payload: Bytes,
  /// Event time in Unix milliseconds.
  pub event_ts_ms: i64,
}

impl ProducerRecord {
  /// Build a producer record.
  #[must_use]
  pub fn new(topic: Chars, record_key: Vec<u8>, payload: Bytes, event_ts_ms: i64) -> Self {
    Self {
      topic,
      record_key,
      payload,
      event_ts_ms,
    }
  }
}

//
// ProducerAck
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Acknowledgement returned from a successful `produce` call.
pub struct ProducerAck {
  /// Topic that accepted the record.
  pub topic: Chars,
  /// Virtual partition selected for this record.
  pub virtual_partition_id: VirtualPartitionId,
  /// Number of attempts used (1 means no retry).
  pub attempts: u32,
}

//
// ProducerError
//

#[derive(Clone, Debug, Error, PartialEq, Eq)]
/// Producer-specific error variants.
pub enum ProducerError {
  #[error("unknown topic: {0}")]
  UnknownTopic(Chars),
  #[error("no brokers available")]
  NoBrokersAvailable,
  #[error("producer retries exhausted: {0}")]
  RetriesExhausted(String),
  #[error("broker rejected request: {0}")]
  Rejected(String),
  #[error("producer shutdown")]
  Shutdown,
}

//
// ProducerClient
//

#[async_trait]
/// High-level producer interface.
pub trait ProducerClient: Send + Sync {
  /// Enqueue records and return terminal results in the same order as the input.
  async fn produce(&self, records: Vec<ProducerRecord>) -> Vec<Result<ProducerAck, ProducerError>>;

  /// Returns a handle for observing this producer's local runtime state.
  fn diagnostics(&self) -> Option<ProducerDiagnostics> {
    None
  }
}

//
// ProducerClientBuilder
//

/// Builder for a producer and its runtime dependencies.
pub struct ProducerClientBuilder {
  config: ProducerConfig,
  topics: Vec<ProducerTopicConfig>,
  discovery: Arc<dyn BrokerDiscovery>,
  transport: Arc<dyn BrokerTransport>,
  metrics_scope: Scope,
  retry_clock: Arc<dyn ProducerRetryClock>,
}

impl ProducerClientBuilder {
  /// Start building a producer with its default Tokio-backed retry clock.
  #[must_use]
  pub fn new(
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
    discovery: Arc<dyn BrokerDiscovery>,
    transport: Arc<dyn BrokerTransport>,
    metrics_scope: Scope,
  ) -> Self {
    Self {
      config,
      topics,
      discovery,
      transport,
      metrics_scope,
      retry_clock: Arc::new(retry::TokioProducerRetryClock),
    }
  }

  /// Use a caller-provided clock for retry deadlines and backoff sleeps.
  #[must_use]
  pub fn retry_clock(mut self, retry_clock: Arc<dyn ProducerRetryClock>) -> Self {
    self.retry_clock = retry_clock;
    self
  }

  /// Build the producer and start its background flush loop.
  pub async fn build(self) -> Result<ProducerClientImpl> {
    ProducerClientImpl::build(self).await
  }
}

//
// ProducerClientImpl
//

/// Default producer implementation with discovery, batching, and retry handling.
pub struct ProducerClientImpl {
  config: ProducerConfig,
  writer_id: u32,
  max_batch_records: usize,
  max_batch_bytes: usize,
  topics: HashMap<Chars, ProducerTopicConfig>,
  membership_rx: watch::Receiver<BrokerMembership>,
  metrics: Arc<ProducerMetrics>,
  retry_diagnostics: ProducerRetryDiagnostics,
  routes: ProducerRoutes,
  state: Arc<Mutex<ProducerState>>,
  flush_notify: Arc<Notify>,
  flush_task: JoinHandle<()>,
}

//
// ProducerDispatchContext
//

// Immutable dependencies shared by the flush coordinator and its dispatch tasks. Keeping these
// together makes each task capture one Arc rather than rebuilding its closure from every field.
pub(in crate::producer) struct ProducerDispatchContext {
  pub(in crate::producer) config: ProducerConfig,
  pub(in crate::producer) topics: HashMap<Chars, ProducerTopicConfig>,
  pub(in crate::producer) membership_rx: watch::Receiver<BrokerMembership>,
  pub(in crate::producer) transport: Arc<dyn BrokerTransport>,
  pub(in crate::producer) metrics: Arc<ProducerMetrics>,
  pub(in crate::producer) retry_diagnostics: ProducerRetryDiagnostics,
  pub(in crate::producer) retry_clock: Arc<dyn ProducerRetryClock>,
  pub(in crate::producer) routes: ProducerRoutes,
  pub(in crate::producer) request_permits: Arc<Semaphore>,
}

//
// PreparedRecord
//

// Record data that was validated and sized before the producer state mutex is acquired.
struct PreparedRecord {
  result_index: usize,
  topic: Chars,
  virtual_partition_id: VirtualPartitionId,
  proto_record: Record,
  encoded_record_size: usize,
  request_base_size: usize,
}

impl ProducerClientImpl {
  /// Construct a producer and its default gRPC transport from runtime configuration.
  pub async fn from_runtime_config(
    runtime: ProducerRuntimeConfig,
    metrics_scope: Scope,
  ) -> Result<Self> {
    validate_runtime_config(&runtime)?;
    let config = runtime
      .producer
      .as_ref()
      .ok_or_else(|| anyhow!("producer config is required"))?
      .clone();
    let discovery_config = runtime
      .discovery
      .as_ref()
      .ok_or_else(|| anyhow!("producer discovery config is required"))?;
    let discovery = into_discovery(discovery_config)?;
    let transport: Arc<dyn BrokerTransport> = Arc::new(GrpcBrokerTransport::new(config.clone()));

    ProducerClientBuilder::new(config, runtime.topics, discovery, transport, metrics_scope)
      .build()
      .await
  }

  /// Construct a producer after applying its startup-only feature flag overrides.
  pub async fn from_runtime_config_with_feature_flags(
    mut runtime: ProducerRuntimeConfig,
    metrics_scope: Scope,
    feature_flags: &FeatureFlagsWatch,
  ) -> Result<Self> {
    apply_producer_startup_overrides(feature_flags, &mut runtime)?;
    debug!("constructing producer after applying startup-only feature flag overrides");
    Self::from_runtime_config(runtime, metrics_scope).await
  }

  /// Construct a producer from explicit configuration and runtime dependencies.
  pub async fn new(
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
    discovery: Arc<dyn BrokerDiscovery>,
    transport: Arc<dyn BrokerTransport>,
    metrics_scope: Scope,
  ) -> Result<Self> {
    ProducerClientBuilder::new(config, topics, discovery, transport, metrics_scope)
      .build()
      .await
  }

  async fn build(builder: ProducerClientBuilder) -> Result<Self> {
    let ProducerClientBuilder {
      config,
      topics,
      discovery,
      transport,
      metrics_scope,
      retry_clock,
    } = builder;

    validate_producer_config(&config)?;
    let writer_id = producer_writer_id(&config);
    let max_batch_records = producer_max_batch_records(&config) as usize;
    let max_batch_bytes = producer_max_batch_bytes(&config) as usize;

    let mut topic_map = HashMap::new();
    for topic in topics {
      validate_topic_config(&topic)?;
      ensure!(
        writer_id < topic.num_writers,
        "writer_id {} must be less than num_writers {} for topic {}",
        writer_id,
        topic.num_writers,
        topic.name
      );
      if topic_map.contains_key(&topic.name) {
        bail!("duplicate topic config: {}", topic.name);
      }
      topic_map.insert(topic.name.clone(), topic);
    }

    ensure!(
      !topic_map.is_empty(),
      "at least one topic must be configured"
    );

    let mut membership_rx = discovery.watch_membership().await?;
    let initial_membership = timeout(
      INITIAL_MEMBERSHIP_TIMEOUT,
      wait_for_initialized_membership(&mut membership_rx),
    )
    .await
    .map_err(|_| anyhow!("producer initial broker membership timed out after 10 seconds"))??;
    transport.reconcile_membership(&initial_membership);
    let routes = ProducerRoutes::new(&config, &topic_map, &initial_membership);
    let state = Arc::new(Mutex::new(ProducerState::default()));
    let metrics = Arc::new(ProducerMetrics::new(&metrics_scope));
    let retry_diagnostics = ProducerRetryDiagnostics::default();
    let max_request_concurrency = usize::try_from(producer_max_request_concurrency(&config))
      .map_err(|_| anyhow!("producer max request concurrency exceeds usize"))?;
    ensure!(
      max_request_concurrency > 0,
      "producer max request concurrency must be positive"
    );
    let dispatch_task_permits = Arc::new(Semaphore::new(max_request_concurrency));
    let request_permits = Arc::new(Semaphore::new(max_request_concurrency));
    let flush_notify = Arc::new(Notify::new());
    let dispatch_context = Arc::new(ProducerDispatchContext {
      config: config.clone(),
      topics: topic_map.clone(),
      membership_rx: membership_rx.clone(),
      transport: Arc::clone(&transport),
      metrics: Arc::clone(&metrics),
      retry_diagnostics: retry_diagnostics.clone(),
      retry_clock: Arc::clone(&retry_clock),
      routes: routes.clone(),
      request_permits,
    });

    log::info!(
      "producer initialized: writer_id={}, topics={}, flush_max_delay_ms={}, \
       max_batch_records={}, max_batch_bytes={}",
      writer_id,
      topic_map.len(),
      producer_flush_max_delay(&config).whole_milliseconds(),
      producer_max_batch_records(&config),
      producer_max_batch_bytes(&config)
    );

    let flush_task = Self::spawn_flush_loop(
      Arc::clone(&state),
      dispatch_context,
      membership_rx.clone(),
      Arc::clone(&dispatch_task_permits),
      Arc::clone(&flush_notify),
    );

    Ok(Self {
      config,
      writer_id,
      max_batch_records,
      max_batch_bytes,
      topics: topic_map,
      membership_rx,
      metrics,
      retry_diagnostics,
      routes,
      state,
      flush_notify,
      flush_task,
    })
  }

  fn spawn_flush_loop(
    state: Arc<Mutex<ProducerState>>,
    dispatch_context: Arc<ProducerDispatchContext>,
    mut membership_rx: watch::Receiver<BrokerMembership>,
    dispatch_task_permits: Arc<Semaphore>,
    flush_notify: Arc<Notify>,
  ) -> JoinHandle<()> {
    // Every flush trigger drains and packs all buffered partitions, maximizing each broker-scoped
    // RPC. Membership updates refresh routing but preserve the current batching cadence.
    tokio::spawn(async move {
      let flush_delay = StdDuration::try_from(producer_flush_max_delay(&dispatch_context.config))
        .expect("producer config validation requires a positive flush max delay");
      let flush_sleep = tokio::time::sleep(flush_delay);
      tokio::pin!(flush_sleep);
      // Keep dispatches owned by the flush task so producer shutdown cancels RPCs and retries.
      // The task permit bounds dispatch lifetimes; request permits bound active transport calls.
      let mut dispatches = JoinSet::new();
      let mut membership_closed = false;

      loop {
        let flush_triggered_by_size = tokio::select! {
          changed = membership_rx.changed(), if !membership_closed => {
            if changed.is_ok() {
              let membership = membership_rx.borrow_and_update().clone();
              dispatch_context.routes.refresh(
                &dispatch_context.config,
                &dispatch_context.topics,
                &membership,
              );
              dispatch_context.transport.reconcile_membership(&membership);
            } else {
              membership_closed = true;
            }
            // Membership affects the next route selection, not when buffered work is sent.
            None
          },
          () = &mut flush_sleep => Some(false),
          () = flush_notify.notified() => Some(true),
          Some(result) = dispatches.join_next(), if !dispatches.is_empty() => {
            if let Err(error) = result {
              log::error!("producer dispatch task failed: {error}");
            }
            // A completed task may have released a permit for batches retained in shared state,
            // but must not flush an unrelated later partial batch.
            None
          },
        };

        if let Some(flush_triggered_by_size) = flush_triggered_by_size {
          let assignment = dispatch_context.routes.assignment_snapshot();
          let sealed = state
            .lock()
            .seal_ready_generation(|topic, virtual_partition_id| {
              assignment
                .get(&BrokerPartition {
                  topic: topic.clone(),
                  virtual_partition_id,
                })
                .map(|broker| broker.address.clone())
            });
          if sealed.batch_count > 0 {
            if flush_triggered_by_size {
              dispatch_context.metrics.flushes_max_size.inc();
            } else {
              dispatch_context.metrics.flushes_max_delay.inc();
            }
            trace!(
              "producer flush loop sealed {} batch(es)",
              sealed.batch_count
            );
          }
          // Unassigned batches have notified their record waiters. This background task has no
          // caller to receive the terminal error.
          let _ = notify_unassigned_batches(&dispatch_context.metrics, sealed.unassigned);

          // Each trigger-driven drain starts a new maximum-delay window for subsequent partial
          // batches.
          flush_sleep.as_mut().reset(Instant::now() + flush_delay);
        }

        // Acquire a task slot before removing the next ready group. The ready broker FIFO keeps
        // unsent work in ProducerState, so no batch must be reconstructed after admission fails.
        while let Ok(dispatch_task_permit) = Arc::clone(&dispatch_task_permits).try_acquire_owned()
        {
          let Some(group) = state.lock().take_next_ready_group() else {
            drop(dispatch_task_permit);
            break;
          };
          let dispatch_context = Arc::clone(&dispatch_context);
          dispatches.spawn(async move {
            send_grouped_batches_and_notify(dispatch_context.as_ref(), dispatch_task_permit, group)
              .await
          });
        }
      }
    })
  }
}

impl Drop for ProducerClientImpl {
  fn drop(&mut self) {
    debug!("producer dropping; aborting flush loop task");
    self.flush_task.abort();
  }
}

#[async_trait]
impl ProducerClient for ProducerClientImpl {
  async fn produce(&self, records: Vec<ProducerRecord>) -> Vec<Result<ProducerAck, ProducerError>> {
    if records.is_empty() {
      return Vec::new();
    }

    // Validate, partition, and measure before entering shared state. Invalid input occupies its
    // result slot but does not prevent valid siblings from being accepted.
    let mut results = std::iter::repeat_with(|| None)
      .take(records.len())
      .collect::<Vec<Option<Result<ProducerAck, ProducerError>>>>();
    let mut prepared_records = Vec::with_capacity(records.len());
    for (result_index, record) in records.into_iter().enumerate() {
      let ProducerRecord {
        topic,
        record_key,
        payload,
        event_ts_ms,
      } = record;
      let Some(topic_config) = self.topics.get(topic.as_str()) else {
        results[result_index] = Some(Err(ProducerError::UnknownTopic(topic)));
        continue;
      };
      let virtual_partition_id =
        compute_virtual_partition_id(&record_key, topic_config.partition_count, self.writer_id);
      let proto_record = Record {
        payload,
        event_ts_ms,
        ..Default::default()
      };
      let record_wire_sizes = record_wire_sizes(&topic, virtual_partition_id, &proto_record);
      if !record_wire_sizes.fits_grouped_request() {
        results[result_index] = Some(Err(ProducerError::Rejected(format!(
          "record exceeds the {MAX_PRODUCE_BATCHES_REQUEST_BYTES} byte request limit"
        ))));
        continue;
      }
      prepared_records.push(PreparedRecord {
        result_index,
        topic,
        virtual_partition_id,
        proto_record,
        encoded_record_size: record_wire_sizes.encoded_record_size,
        request_base_size: record_wire_sizes.request_base_size,
      });
    }

    if prepared_records.is_empty() {
      return results
        .into_iter()
        .map(|result| result.expect("invalid record result was populated"))
        .collect();
    }

    let (completion, completion_rx) = BulkCompletion::new(results, prepared_records.len());
    let completion_count = prepared_records.len();
    let size_flush_requested = {
      let mut guard = self.state.lock();
      let mut size_flush_requested = false;
      for record in prepared_records {
        size_flush_requested |= guard.push_record(
          BufferedRecord {
            topic: record.topic,
            virtual_partition_id: record.virtual_partition_id,
            proto_record: record.proto_record,
            completion: RecordCompletion::new(Arc::clone(&completion), record.result_index),
            encoded_record_size: record.encoded_record_size,
            request_base_size: record.request_base_size,
          },
          self.max_batch_records,
          self.max_batch_bytes,
        );
      }
      size_flush_requested
    };

    self
      .metrics
      .records_enqueued
      .inc_by(completion_count.try_into().unwrap_or_default());
    trace!("bulk records buffered: record_count={completion_count}");
    if size_flush_requested {
      trace!("size flush requested by bulk record admission");
      self.flush_notify.notify_one();
    }

    // The record handles own the completion sender. If producer shutdown drops every handle,
    // BulkCompletion::drop preserves immediate validation failures and fills pending slots with
    // shutdown errors.
    drop(completion);
    completion_rx
      .await
      .expect("bulk completion sends a result vector before its sender is dropped")
  }

  fn diagnostics(&self) -> Option<ProducerDiagnostics> {
    Some(ProducerDiagnostics::new(
      self.config.clone(),
      self.topics.clone(),
      self.membership_rx.clone(),
      self.routes.clone(),
      Arc::clone(&self.state),
      self.retry_diagnostics.clone(),
    ))
  }
}

fn compute_virtual_partition_id(
  record_key: &[u8],
  partition_count: u32,
  writer_id: u32,
) -> VirtualPartitionId {
  virtual_partition_for_key(record_key, partition_count, writer_id)
}
