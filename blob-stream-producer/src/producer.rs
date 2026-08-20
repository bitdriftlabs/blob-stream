#[cfg(test)]
#[path = "./producer_test.rs"]
mod tests;

mod diagnostics;
mod dispatch;
mod metrics;
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
use dispatch::{dispatch_group_and_notify, notify_unassigned_batches, remember_first_error};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{debug, trace};
use metrics::ProducerMetrics;
use parking_lot::Mutex;
use protobuf::Chars;
pub use retry::ProducerRetryClock;
use routing::{ProducerRoutes, group_batches_by_broker, record_fits_grouped_request};
use state::{BufferedRecord, ProducerState};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use thiserror::Error;
use tokio::sync::{Notify, Semaphore, oneshot, watch};
use tokio::task::JoinHandle;
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
  /// Enqueue a record and wait for broker acknowledgement.
  async fn produce(&self, record: ProducerRecord) -> Result<ProducerAck, ProducerError>;
  /// Flush any buffered records for all topics/partitions.
  async fn flush(&self) -> Result<(), ProducerError>;
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
  topics: HashMap<Chars, ProducerTopicConfig>,
  membership_rx: watch::Receiver<BrokerMembership>,
  transport: Arc<dyn BrokerTransport>,
  metrics: Arc<ProducerMetrics>,
  retry_diagnostics: ProducerRetryDiagnostics,
  retry_clock: Arc<dyn ProducerRetryClock>,
  routes: ProducerRoutes,
  state: Arc<Mutex<ProducerState>>,
  flush_notify: Arc<Notify>,
  dispatch_permits: Arc<Semaphore>,
  flush_task: JoinHandle<()>,
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
    let dispatch_permits = Arc::new(Semaphore::new(max_request_concurrency));
    let flush_notify = Arc::new(Notify::new());

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
      Arc::clone(&transport),
      config.clone(),
      topic_map.clone(),
      membership_rx.clone(),
      Arc::clone(&metrics),
      retry_diagnostics.clone(),
      Arc::clone(&retry_clock),
      routes.clone(),
      Arc::clone(&dispatch_permits),
      Arc::clone(&flush_notify),
    );

    Ok(Self {
      config,
      topics: topic_map,
      membership_rx,
      transport,
      metrics,
      retry_diagnostics,
      retry_clock,
      routes,
      state,
      flush_notify,
      dispatch_permits,
      flush_task,
    })
  }

  fn spawn_flush_loop(
    state: Arc<Mutex<ProducerState>>,
    transport: Arc<dyn BrokerTransport>,
    config: ProducerConfig,
    topics: HashMap<Chars, ProducerTopicConfig>,
    mut membership_rx: watch::Receiver<BrokerMembership>,
    metrics: Arc<ProducerMetrics>,
    retry_diagnostics: ProducerRetryDiagnostics,
    retry_clock: Arc<dyn ProducerRetryClock>,
    routes: ProducerRoutes,
    dispatch_permits: Arc<Semaphore>,
    flush_notify: Arc<Notify>,
  ) -> JoinHandle<()> {
    // Every flush trigger drains and packs all buffered partitions, maximizing each broker-scoped
    // RPC. Membership updates refresh routing but preserve the current batching cadence.
    tokio::spawn(async move {
      // Dispatches share this read-only receiver while the loop keeps its receiver for updates.
      let dispatch_membership_rx = membership_rx.clone();
      let flush_delay = StdDuration::try_from(producer_flush_max_delay(&config))
        .expect("producer config validation requires a positive flush max delay");
      let flush_sleep = tokio::time::sleep(flush_delay);
      tokio::pin!(flush_sleep);
      // Keep dispatches owned by the flush task so producer shutdown cancels queued permit waits,
      // RPCs, and retries. Their completions still need polling to advance semaphore waiters, but
      // must not define a new batching boundary.
      let mut dispatches = FuturesUnordered::new();
      let mut membership_closed = false;

      loop {
        let flush_triggered_by_size = tokio::select! {
          changed = membership_rx.changed(), if !membership_closed => {
            if changed.is_ok() {
              let membership = membership_rx.borrow_and_update().clone();
              routes.refresh(&config, &topics, &membership);
              transport.reconcile_membership(&membership);
            } else {
              membership_closed = true;
            }
            // Membership affects the next route selection, not when buffered work is sent.
            None
          },
          () = &mut flush_sleep => Some(false),
          () = flush_notify.notified() => Some(true),
          Some(_) = dispatches.next(), if !dispatches.is_empty() => {
            // Polling this completion lets another dispatch acquire a released permit. Its
            // waiters have already been notified, and it must not flush partial new batches.
            None
          },
        };

        let Some(flush_triggered_by_size) = flush_triggered_by_size else {
          continue;
        };

        // Each trigger-driven drain starts a new maximum-delay window for subsequent partial
        // batches.
        flush_sleep.as_mut().reset(Instant::now() + flush_delay);

        // Only this task owns batches after they leave shared state. A caller dropping its
        // `produce` future cannot cancel dispatch, and all batches present at this wake can pack
        // into the same broker-scoped RPC.
        let batches = {
          let mut guard = state.lock();
          guard.drain_all_batches()
        };

        if !batches.is_empty() {
          if flush_triggered_by_size {
            metrics.flushes_max_size.inc();
          } else {
            metrics.flushes_max_delay.inc();
          }
          trace!("producer flush loop drained {} batch(es)", batches.len());
        }

        let grouped = group_batches_by_broker(&routes, batches);
        // Unassigned batches have notified their record waiters. This background task has no
        // caller to receive the terminal error.
        let _ = notify_unassigned_batches(&metrics, grouped.unassigned);
        for group in grouped.groups {
          dispatches.push(dispatch_group_and_notify(
            &config,
            &topics,
            &routes,
            &dispatch_membership_rx,
            &transport,
            &metrics,
            &retry_diagnostics,
            &retry_clock,
            &dispatch_permits,
            group,
          ));
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
  async fn produce(&self, record: ProducerRecord) -> Result<ProducerAck, ProducerError> {
    let ProducerRecord {
      topic,
      record_key,
      payload,
      event_ts_ms,
    } = record;
    let topic_config = self
      .topics
      .get(topic.as_str())
      .ok_or_else(|| ProducerError::UnknownTopic(topic.clone()))?;

    let virtual_partition_id = compute_virtual_partition_id(
      &record_key,
      topic_config.partition_count,
      producer_writer_id(&self.config),
    );
    let proto_record = Record {
      payload,
      event_ts_ms,
      ..Default::default()
    };
    if !record_fits_grouped_request(&topic, virtual_partition_id, &proto_record) {
      return Err(ProducerError::Rejected(format!(
        "record exceeds the {MAX_PRODUCE_BATCHES_REQUEST_BYTES} byte request limit"
      )));
    }

    let (tx, rx) = oneshot::channel();
    let payload_len = proto_record.payload.len();

    trace!(
      "record buffered: topic={topic}, virtual_partition_id={virtual_partition_id}, \
       payload_bytes={payload_len}"
    );
    let size_flush_requested = {
      let mut guard = self.state.lock();
      guard.push_record(
        BufferedRecord {
          topic,
          virtual_partition_id,
          proto_record,
          waiter: tx,
        },
        producer_max_batch_records(&self.config) as usize,
        producer_max_batch_bytes(&self.config) as usize,
      )
    };

    self.metrics.records_enqueued.inc();

    if size_flush_requested {
      trace!("size flush requested: virtual_partition_id={virtual_partition_id}");
      self.flush_notify.notify_one();
    }

    rx.await.unwrap_or(Err(ProducerError::Shutdown))
  }

  async fn flush(&self) -> Result<(), ProducerError> {
    let batches = {
      let mut guard = self.state.lock();
      guard.drain_all_batches()
    };

    let mut dispatches = FuturesUnordered::new();
    let grouped = group_batches_by_broker(&self.routes, batches);
    let mut first_error = None;
    remember_first_error(
      &mut first_error,
      notify_unassigned_batches(&self.metrics, grouped.unassigned),
    );
    for group in grouped.groups {
      dispatches.push(dispatch_group_and_notify(
        &self.config,
        &self.topics,
        &self.routes,
        &self.membership_rx,
        &self.transport,
        &self.metrics,
        &self.retry_diagnostics,
        &self.retry_clock,
        &self.dispatch_permits,
        group,
      ));
    }

    while let Some(result) = dispatches.next().await {
      remember_first_error(&mut first_error, result);
    }
    first_error.map_or(Ok(()), Err)
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
