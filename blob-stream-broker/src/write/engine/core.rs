use super::super::config::EffectiveFlushConfig;
use super::super::flush::FlushContext;
use super::super::metrics::WriteMetrics;
use super::super::scheduler::{
  begin_shutdown_drain,
  collect_next_flush_plan,
  flush_plan_and_notify,
};
use super::super::state::WriteState;
use super::super::{
  AdmissionController,
  BrokerLifecycleHooks,
  MAX_IN_FLIGHT_FLUSH_PLANS,
  TopicInfo,
  WriteConfig,
};
use super::MemoryPressureController;
use anyhow::Result;
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use bd_server_stats::stats::Scope;
use bd_shutdown::ComponentShutdownTriggerHandle;
use bd_time::{SystemTimeProvider, TimeProvider};
use blob_stream_blob_store::BlobStore;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{MetadataStore, ProducerPartitionLeaseStore};
use parking_lot::{Mutex, RwLock};
use protobuf::Chars;
use std::collections::HashMap;
use std::sync::Arc;
use time::Duration as TimeDuration;
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinSet;
use uuid::Uuid;

//
// WriteEngineImpl
//

pub struct WriteEngineImpl {
  pub(in crate::write) config: WriteConfig,
  pub(in crate::write) admission: Arc<dyn AdmissionController>,
  pub(in crate::write) topics: HashMap<Chars, TopicInfo>,
  pub(in crate::write) flush_context: FlushContext,
  pub(in crate::write) lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  pub(in crate::write) holder_id: String,
  pub(in crate::write) lease_session_id: String,
  pub(in crate::write) time_provider: Arc<dyn TimeProvider>,
  pub(in crate::write) metrics: WriteMetrics,
  pub(in crate::write) state: Arc<Mutex<WriteState>>,
  pub(in crate::write) effective_flush_config: Arc<RwLock<EffectiveFlushConfig>>,
  pub(in crate::write) flush_notifier: Arc<Notify>,
  pub(in crate::write) shutdown_trigger_handle: ComponentShutdownTriggerHandle,
  pub(in crate::write) lifecycle_hooks: Option<Arc<dyn BrokerLifecycleHooks>>,
  pub(in crate::write) feature_flags: Option<FeatureFlagsWatch>,
}

//
// WriteEngineBuilder
//

/// Configures a write engine and its optional runtime and test dependencies.
pub struct WriteEngineBuilder<'a> {
  config: WriteConfig,
  topics: HashMap<Chars, TopicInfo>,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  holder_id: String,
  lease_session_id: Option<String>,
  shutdown_trigger_handle: ComponentShutdownTriggerHandle,
  metrics_scope: &'a Scope,
  machine_id: Option<u16>,
  membership_rx: Option<watch::Receiver<BrokerMembership>>,
  admission: Option<Arc<dyn AdmissionController>>,
  time_provider: Arc<dyn TimeProvider>,
  lifecycle_hooks: Option<Arc<dyn BrokerLifecycleHooks>>,
  feature_flags: Option<FeatureFlagsWatch>,
}

impl<'a> WriteEngineBuilder<'a> {
  pub fn new(
    config: WriteConfig,
    topics: HashMap<Chars, TopicInfo>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ProducerPartitionLeaseStore>,
    holder_id: String,
    shutdown_trigger_handle: ComponentShutdownTriggerHandle,
    metrics_scope: &'a Scope,
  ) -> Self {
    Self {
      config,
      topics,
      blob_store,
      metadata_store,
      lease_store,
      holder_id,
      lease_session_id: None,
      shutdown_trigger_handle,
      metrics_scope,
      machine_id: None,
      membership_rx: None,
      admission: None,
      time_provider: Arc::new(SystemTimeProvider),
      lifecycle_hooks: None,
      feature_flags: None,
    }
  }

  #[must_use]
  pub fn machine_id(mut self, machine_id: u16) -> Self {
    self.machine_id = Some(machine_id);
    self
  }

  #[must_use]
  pub fn membership_rx(mut self, membership_rx: watch::Receiver<BrokerMembership>) -> Self {
    self.membership_rx = Some(membership_rx);
    self
  }

  #[must_use]
  pub fn admission(mut self, admission: Arc<dyn AdmissionController>) -> Self {
    self.admission = Some(admission);
    self
  }

  #[must_use]
  pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
    self.time_provider = time_provider;
    self
  }

  #[must_use]
  pub fn lifecycle_hooks(mut self, lifecycle_hooks: Arc<dyn BrokerLifecycleHooks>) -> Self {
    self.lifecycle_hooks = Some(lifecycle_hooks);
    self
  }

  #[must_use]
  pub fn feature_flags(mut self, feature_flags: Option<FeatureFlagsWatch>) -> Self {
    self.feature_flags = feature_flags;
    self
  }

  #[must_use]
  pub fn lease_session_id(mut self, lease_session_id: String) -> Self {
    self.lease_session_id = Some(lease_session_id);
    self
  }

  pub fn build(self) -> Result<WriteEngineImpl> {
    let Self {
      config,
      topics,
      blob_store,
      metadata_store,
      lease_store,
      holder_id,
      lease_session_id,
      shutdown_trigger_handle,
      metrics_scope,
      machine_id,
      membership_rx,
      admission,
      time_provider,
      lifecycle_hooks,
      feature_flags,
    } = self;
    let snowflake = match machine_id {
      Some(machine_id) => super::super::flush::SnowflakeGenerator::with_machine_id(machine_id)?,
      None => super::super::flush::SnowflakeGenerator::new()?,
    };
    let initial_membership = membership_rx.as_ref().map_or_else(
      || {
        BrokerMembership::new(vec![BrokerNode {
          node_id: holder_id.clone().into(),
          address: holder_id.clone().into(),
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
      lifecycle_hooks.clone(),
    );
    let effective_flush_config = Arc::new(RwLock::new(
      config.effective_flush_config(feature_flags.as_ref()),
    ));
    let admission = admission.unwrap_or_else(|| {
      MemoryPressureController::new(&shutdown_trigger_handle, &metrics_scope.scope("write"))
    });
    let engine = WriteEngineImpl {
      config,
      admission,
      topics,
      flush_context,
      lease_store,
      holder_id,
      lease_session_id: lease_session_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
      time_provider,
      metrics: WriteMetrics::new(metrics_scope),
      state,
      effective_flush_config,
      flush_notifier: Arc::new(Notify::new()),
      shutdown_trigger_handle,
      lifecycle_hooks,
      feature_flags,
    };

    engine.spawn_flush_loop();
    if let Some(membership_rx) = membership_rx {
      engine.spawn_lease_self_assignment_loop(membership_rx);
    }
    Ok(engine)
  }
}

impl WriteEngineImpl {
  fn spawn_flush_loop(&self) {
    let flush_context = self.flush_context.clone();
    let state = Arc::clone(&self.state);
    let topics = self.topics.clone();
    let time_provider = Arc::clone(&self.time_provider);
    let metrics = self.metrics.clone();
    let feature_flags = self.feature_flags.clone();
    let mut feature_flag_changes = feature_flags.clone();
    let effective_flush_config = Arc::clone(&self.effective_flush_config);
    let flush_notifier = Arc::clone(&self.flush_notifier);
    let mut shutdown = self.shutdown_trigger_handle.make_shutdown();
    let flush_plan_permits = Arc::new(Semaphore::new(MAX_IN_FLIGHT_FLUSH_PLANS));

    tokio::spawn(async move {
      let mut flush_config = *effective_flush_config.read();
      let mut flush_tick =
        Box::pin(time_provider.sleep(flush_config.max_delay.max(TimeDuration::milliseconds(1))));
      let mut flushes = JoinSet::new();
      let mut shutdown_requested = false;
      let mut flush_config_needs_reload = false;
      loop {
        if shutdown_requested && flushes.is_empty() {
          log::info!("broker flush loop shutdown complete");
          return;
        }

        // Reserve a slot before moving buffered batches out of WriteState. The spawned task owns
        // the permit while it merges, encodes, compresses, and persists its flush plan.
        let mut scheduled_flush = false;
        while let Ok(permit) = Arc::clone(&flush_plan_permits).try_acquire_owned() {
          let Some(plan) = collect_next_flush_plan(
            &state,
            time_provider.now(),
            flush_context.config(),
            &flush_config,
            feature_flags.as_ref(),
            &topics,
          ) else {
            drop(permit);
            break;
          };

          metrics.record_flush_plan_summary(std::slice::from_ref(&plan));
          let flush_context = flush_context.clone();
          let metrics = metrics.clone();
          let state = Arc::clone(&state);
          flushes.spawn(async move {
            let _permit = permit;
            let _active_flush =
              bd_server_stats::stats::StackAutoGauge::new(&metrics.active_flush_plans);
            flush_plan_and_notify(&flush_context, plan, &metrics, &state).await;
          });
          scheduled_flush = true;
        }
        if scheduled_flush {
          continue;
        }

        tokio::select! {
          () = shutdown.cancelled(), if !shutdown_requested => {
            shutdown_requested = true;
            begin_shutdown_drain(&state);
            flush_notifier.notify_waiters();
            log::info!("broker flush loop draining buffered writes for shutdown");
          },
          () = &mut flush_tick => {
            if flush_config_needs_reload {
              flush_config = flush_context
                .config()
                .effective_flush_config(feature_flags.as_ref());
              *effective_flush_config.write() = flush_config;
              flush_config_needs_reload = false;
            }
            flush_tick = Box::pin(time_provider.sleep(
              flush_config.max_delay.max(TimeDuration::milliseconds(1)),
            ));
          },
          feature_flag_change = async {
            let Some(feature_flags) = feature_flag_changes.as_mut() else {
              std::future::pending().await
            };
            feature_flags.changed().await
          }, if feature_flag_changes.is_some() => {
            match feature_flag_change {
              Ok(()) => flush_config_needs_reload = true,
              Err(error) => {
                feature_flag_changes = None;
                log::debug!("broker feature flag watch closed: {error}");
              },
            }
          },
          () = flush_notifier.notified() => {},
          Some(result) = flushes.join_next(), if !flushes.is_empty() => {
            if let Err(error) = result {
              log::error!("broker flush task failed: {error}");
            }
          },
        }
      }
    });
  }
}
