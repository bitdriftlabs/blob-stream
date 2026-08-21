use super::driver::{ConsumerDriver, persisted_lease_expires_at, retry_backoff};
use super::shared::ConsumerIteratorMetrics;
use super::{
  ConsumerCoordinationSource,
  ConsumerIteratorImpl,
  ConsumerLifecycleHooks,
  ConsumerSharedState,
};
use crate::config::{
  ConsumerRuntimeConfig,
  DEFAULT_MAX_CLOCK_SKEW,
  consumer_idle_poll_delay,
  consumer_lease_duration,
  consumer_max_clock_skew,
  consumer_max_idle_poll_delay,
  consumer_prefetch_max_bytes,
  validate_runtime_config,
};
use crate::consumer::{BrokerBlobRangeQuery, BrokerMetadataQuery, ConsumerReaderImpl};
use crate::coordination::ConsumerGroupCoordinatorImpl;
use crate::diagnostics::ConsumerDiagnostics;
use anyhow::{Result, anyhow, ensure};
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use bd_server_stats::stats::Scope;
use bd_time::{OffsetDateTimeExt, SystemTimeProvider, TimeProvider};
use blob_stream_blob_store::BlobStore;
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  MetadataStore,
};
use blob_stream_types::DEFAULT_METADATA_WINDOW_SIZE;
use log::info;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use time::Duration;
use tokio::sync::Notify;

//
// ConsumerIteratorBuilder
//

/// Configures a consumer iterator and optional test-only runtime dependencies.
pub struct ConsumerIteratorBuilder<'a> {
  runtime: &'a ConsumerRuntimeConfig,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  coordination_source: Arc<dyn ConsumerCoordinationSource>,
  metrics_scope: Scope,
  retention: Duration,
  maximum_metadata_publication_lag: Duration,
  metadata_window_size: Duration,
  maximum_clock_skew: Duration,
  metadata_cache_max_age: Duration,
  feature_flags: Option<FeatureFlagsWatch>,
  broker_metadata_query: Option<Arc<dyn BrokerMetadataQuery>>,
  broker_blob_range_query: Option<Arc<dyn BrokerBlobRangeQuery>>,
  time_provider: Arc<dyn TimeProvider>,
  lifecycle_hooks: Option<Arc<dyn ConsumerLifecycleHooks>>,
}

impl<'a> ConsumerIteratorBuilder<'a> {
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    runtime: &'a ConsumerRuntimeConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    membership_store: Arc<dyn ConsumerGroupMembershipStore>,
    coordination_source: Arc<dyn ConsumerCoordinationSource>,
    metrics_scope: Scope,
    retention: Duration,
    maximum_metadata_publication_lag: Duration,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Self {
    Self {
      runtime,
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      coordination_source,
      metrics_scope,
      retention,
      maximum_metadata_publication_lag,
      metadata_window_size: DEFAULT_METADATA_WINDOW_SIZE,
      maximum_clock_skew: runtime
        .read
        .as_ref()
        .map_or(DEFAULT_MAX_CLOCK_SKEW, consumer_max_clock_skew),
      metadata_cache_max_age: Duration::ZERO,
      feature_flags,
      broker_metadata_query: None,
      broker_blob_range_query: None,
      time_provider: Arc::new(SystemTimeProvider),
      lifecycle_hooks: None,
    }
  }

  #[must_use]
  pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
    self.time_provider = time_provider;
    self
  }

  #[must_use]
  /// Supply the typed clock-skew budget resolved from the topic configuration.
  pub fn maximum_clock_skew(mut self, maximum_clock_skew: Duration) -> Self {
    self.maximum_clock_skew = maximum_clock_skew;
    self
  }

  #[must_use]
  /// Supply the shared topic metadata-window contract used for segment keys and scans.
  pub fn metadata_window_size(mut self, metadata_window_size: Duration) -> Self {
    self.metadata_window_size = metadata_window_size;
    self
  }

  #[must_use]
  /// Supply the shared maximum age for retained eventual broker metadata.
  pub fn metadata_cache_max_age(mut self, metadata_cache_max_age: Duration) -> Self {
    self.metadata_cache_max_age = metadata_cache_max_age;
    self
  }

  #[must_use]
  pub fn lifecycle_hooks(mut self, lifecycle_hooks: Arc<dyn ConsumerLifecycleHooks>) -> Self {
    self.lifecycle_hooks = Some(lifecycle_hooks);
    self
  }

  #[must_use]
  pub fn broker_metadata_query(mut self, query: Arc<dyn BrokerMetadataQuery>) -> Self {
    self.broker_metadata_query = Some(query);
    self
  }

  #[must_use]
  pub fn broker_blob_range_query(mut self, query: Arc<dyn BrokerBlobRangeQuery>) -> Self {
    self.broker_blob_range_query = Some(query);
    self
  }
}

impl ConsumerIteratorImpl {
  /// Build an iterator with recovery retention and an explicit metadata publication bound.
  #[allow(clippy::too_many_arguments)]
  pub async fn from_config(
    runtime: &ConsumerRuntimeConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    membership_store: Arc<dyn ConsumerGroupMembershipStore>,
    coordination_source: Arc<dyn ConsumerCoordinationSource>,
    metrics_scope: Scope,
    retention: Duration,
    maximum_metadata_publication_lag: Duration,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Result<Self> {
    ConsumerIteratorBuilder::new(
      runtime,
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      coordination_source,
      metrics_scope,
      retention,
      maximum_metadata_publication_lag,
      feature_flags,
    )
    .build()
    .await
  }
}

impl ConsumerIteratorBuilder<'_> {
  /// Build an iterator from this configuration.
  pub async fn build(self) -> Result<ConsumerIteratorImpl> {
    let Self {
      runtime,
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      coordination_source,
      metrics_scope,
      retention,
      maximum_metadata_publication_lag,
      metadata_window_size,
      maximum_clock_skew,
      metadata_cache_max_age,
      feature_flags,
      broker_metadata_query,
      broker_blob_range_query,
      time_provider,
      lifecycle_hooks,
    } = self;
    validate_runtime_config(runtime)?;
    ensure!(
      retention.is_positive(),
      "consumer retention recovery requires topic retention greater than zero"
    );
    ensure!(
      !maximum_metadata_publication_lag.is_negative(),
      "consumer maximum metadata publication lag must not be negative"
    );
    ensure!(
      !maximum_clock_skew.is_negative(),
      "consumer maximum clock skew must not be negative"
    );
    let read_config = runtime
      .read
      .as_ref()
      .ok_or_else(|| anyhow!("consumer read config is required"))?
      .clone();
    let group_config = runtime
      .group
      .as_ref()
      .ok_or_else(|| anyhow!("consumer group config is required"))?
      .clone();
    let idle_poll_delay = consumer_idle_poll_delay(&read_config);
    let max_idle_poll_delay = Some(consumer_max_idle_poll_delay(&read_config));
    let prefetch_max_bytes = consumer_prefetch_max_bytes(&read_config);

    let active_assignment = HashSet::new();
    let assignment_callback = Arc::new(Mutex::new(None));
    let shared_state = Arc::new(Mutex::new(ConsumerSharedState::default()));
    let reader = ConsumerReaderImpl::new(
      read_config,
      Vec::new(),
      HashMap::new(),
      blob_store,
      metadata_store,
      &metrics_scope.scope("consumer"),
      retention,
      maximum_metadata_publication_lag,
      feature_flags,
    )?
    .metadata_window_size(metadata_window_size)
    .maximum_clock_skew(maximum_clock_skew)
    .metadata_cache_max_age(metadata_cache_max_age);
    let reader = if let Some(broker_metadata_query) = broker_metadata_query {
      reader.broker_metadata_query(broker_metadata_query)
    } else {
      reader
    };
    let reader = if let Some(broker_blob_range_query) = broker_blob_range_query {
      reader.broker_blob_range_query(broker_blob_range_query)
    } else {
      reader
    };
    let coordinator = ConsumerGroupCoordinatorImpl::new(
      group_config.clone(),
      Arc::clone(&lease_store),
      Arc::clone(&membership_store),
    )?;
    let now = time_provider.now();
    let now_ts_ms = now.unix_timestamp_ms();
    let membership_lease_duration = consumer_lease_duration(&group_config);
    let membership_lease_expires_at = persisted_lease_expires_at(now, membership_lease_duration)?;
    let delivery_notify = Arc::new(Notify::new());
    let prefetch_space_notify = Arc::new(Notify::new());
    let reader_command_notify = Arc::new(Notify::new());
    let revocation_notify = Arc::new(Notify::new());
    let prefetch_shutdown = Arc::new(AtomicBool::new(false));
    let diagnostics = ConsumerDiagnostics::new(
      group_config.clone(),
      Arc::clone(&shared_state),
      prefetch_max_bytes,
      Arc::clone(&lease_store),
    );
    {
      let mut state = shared_state.lock();
      state.diagnostics.next_heartbeat_at_ms = now_ts_ms;
      state.diagnostics.next_rebalance_at_ms = now_ts_ms;
    }
    let mut driver = ConsumerDriver {
      group_config,
      reader: Some(reader),
      reader_command_tx: None,
      coordinator: Box::new(coordinator),
      membership_store,
      coordination_source,
      last_coordination_snapshot: None,
      metrics: ConsumerIteratorMetrics::new(&metrics_scope.scope("consumer")),
      started: false,
      shared_state: Arc::clone(&shared_state),
      delivery_notify: Arc::clone(&delivery_notify),
      prefetch_space_notify: Arc::clone(&prefetch_space_notify),
      reader_command_notify: Arc::clone(&reader_command_notify),
      prefetch_shutdown,
      prefetch_task: None,
      prefetch_idle_base_delay: idle_poll_delay,
      prefetch_idle_max_delay: max_idle_poll_delay,
      active_assignment,
      assignment_callback: Arc::clone(&assignment_callback),
      pending_assignment: None,
      pending_revocation_completion: None,
      pending_revocation_partitions: None,
      pending_revocation_snapshot: None,
      pending_revocation_span: None,
      revocation_notify,
      lifecycle_hooks,
      time_provider,
      membership_lease_expires_at,
      active_partition_lease_expiration_deadline: membership_lease_expires_at,
      next_heartbeat_at: now,
      next_rebalance_at: now,
      heartbeat_retry_backoff: retry_backoff(),
      rebalance_retry_backoff: retry_backoff(),
      diagnostics,
    };

    driver
      .membership_store
      .register_member(
        &driver.group_config.topic,
        &driver.group_config.group_id,
        &driver.group_config.member_id,
        driver.group_config.pod_id.as_ref().map(ToString::to_string),
        now,
        consumer_lease_duration(&driver.group_config),
      )
      .await?;
    let snapshot = driver.coordination_source.snapshot().await?;
    driver.record_coordination_snapshot(&snapshot);
    driver.metrics.rebalances_total.inc();
    let report = driver
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now)
      .await;
    let report = match report {
      Ok(report) => report,
      Err(error) => {
        driver.metrics.rebalance_failures_total.inc();
        return Err(error);
      },
    };
    driver.record_rebalance_metrics(&report);
    let owned = driver.apply_rebalance_report(report)?;
    info!(
      "consumer iterator bootstrapped: topic={}, group_id={}, member_id={}, owned={}",
      driver.group_config.topic,
      driver.group_config.group_id,
      driver.group_config.member_id,
      owned.len()
    );

    Ok(ConsumerIteratorImpl {
      started: false,
      diagnostics: driver.diagnostics.clone(),
      shared_state,
      delivery_notify,
      prefetch_space_notify,
      metrics: driver.metrics.clone(),
      assignment_callback,
      command_tx: None,
      driver: Some(driver),
      driver_task: None,
      #[cfg(test)]
      next_after_delivery_state_check_hook: None,
    })
  }
}
