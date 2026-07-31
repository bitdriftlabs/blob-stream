use super::driver::{ConsumerDriver, retry_backoff};
use super::shared::ConsumerIteratorMetrics;
use super::{
  ConsumerCoordinationSource,
  ConsumerIteratorImpl,
  ConsumerLifecycleHooks,
  ConsumerSharedState,
};
use crate::config::{
  ConsumerRuntimeConfig,
  consumer_idle_poll_delay_ms,
  consumer_lease_duration_ms,
  consumer_max_idle_poll_delay_ms,
  consumer_prefetch_max_bytes,
  validate_runtime_config,
};
use crate::consumer::ConsumerReaderImpl;
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
use log::info;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
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
  retention_days: u32,
  maximum_metadata_publication_lag_ms: u64,
  feature_flags: Option<FeatureFlagsWatch>,
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
    retention_days: u32,
    maximum_metadata_publication_lag_ms: u64,
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
      retention_days,
      maximum_metadata_publication_lag_ms,
      feature_flags,
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
  pub fn lifecycle_hooks(mut self, lifecycle_hooks: Arc<dyn ConsumerLifecycleHooks>) -> Self {
    self.lifecycle_hooks = Some(lifecycle_hooks);
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
    retention_days: u32,
    maximum_metadata_publication_lag_ms: u64,
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
      retention_days,
      maximum_metadata_publication_lag_ms,
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
      retention_days,
      maximum_metadata_publication_lag_ms,
      feature_flags,
      time_provider,
      lifecycle_hooks,
    } = self;
    validate_runtime_config(runtime)?;
    ensure!(
      retention_days > 0,
      "consumer retention recovery requires topic retention_days greater than zero"
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
    let idle_poll_delay_ms = consumer_idle_poll_delay_ms(&read_config);
    let max_idle_poll_delay_ms = Some(consumer_max_idle_poll_delay_ms(&read_config));
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
      retention_days,
      maximum_metadata_publication_lag_ms,
      feature_flags,
    )?;
    let coordinator = ConsumerGroupCoordinatorImpl::new(
      group_config.clone(),
      Arc::clone(&lease_store),
      Arc::clone(&membership_store),
    )?;
    let now_ts_ms = time_provider.now().unix_timestamp_ms();
    let membership_lease_duration_ms = consumer_lease_duration_ms(&group_config);
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
      prefetch_idle_base_delay_ms: idle_poll_delay_ms,
      prefetch_idle_max_delay_ms: max_idle_poll_delay_ms,
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
      membership_lease_expires_at_ms: now_ts_ms.saturating_add(membership_lease_duration_ms),
      active_partition_lease_expiration_deadline_ms: now_ts_ms
        .saturating_add(membership_lease_duration_ms),
      next_heartbeat_at_ms: now_ts_ms,
      next_rebalance_at_ms: now_ts_ms,
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
        now_ts_ms,
        consumer_lease_duration_ms(&driver.group_config),
      )
      .await?;
    let snapshot = driver.coordination_source.snapshot().await?;
    driver.record_coordination_snapshot(&snapshot);
    driver.metrics.rebalances_total.inc();
    let report = driver
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now_ts_ms)
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
