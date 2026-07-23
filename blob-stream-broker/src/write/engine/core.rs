use super::super::flush::FlushContext;
use super::super::metrics::WriteMetrics;
use super::super::scheduler::{begin_shutdown_drain, collect_flush_plans, flush_plan_and_notify};
use super::super::state::WriteState;
use super::super::{AdmissionController, MAX_IN_FLIGHT_FLUSH_PLANS, TopicInfo, WriteConfig};
use anyhow::Result;
use bd_server_stats::stats::Scope;
use bd_shutdown::ComponentShutdownTriggerHandle;
use bd_time::{OffsetDateTimeExt, TimeProvider};
use blob_stream_blob_store::BlobStore;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{MetadataStore, ProducerPartitionLeaseStore};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use protobuf::Chars;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::{Notify, watch};

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
  pub(in crate::write) time_provider: Arc<dyn TimeProvider>,
  pub(in crate::write) metrics: WriteMetrics,
  pub(in crate::write) state: Arc<Mutex<WriteState>>,
  pub(in crate::write) flush_notifier: Arc<Notify>,
  pub(in crate::write) shutdown_trigger_handle: ComponentShutdownTriggerHandle,
}

impl WriteEngineImpl {
  pub fn new(
    config: WriteConfig,
    topics: HashMap<Chars, TopicInfo>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ProducerPartitionLeaseStore>,
    holder_id: String,
    machine_id: Option<u16>,
    membership_rx: Option<watch::Receiver<BrokerMembership>>,
    admission: Arc<dyn AdmissionController>,
    shutdown_trigger_handle: ComponentShutdownTriggerHandle,
    time_provider: Arc<dyn TimeProvider>,
    metrics_scope: &Scope,
  ) -> Result<Self> {
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
    );
    let engine = Self {
      config,
      admission,
      topics,
      flush_context,
      lease_store,
      holder_id,
      time_provider,
      metrics: WriteMetrics::new(metrics_scope),
      state,
      flush_notifier: Arc::new(Notify::new()),
      shutdown_trigger_handle,
    };

    engine.spawn_flush_loop();
    if let Some(membership_rx) = membership_rx {
      engine.spawn_lease_self_assignment_loop(membership_rx);
    }
    Ok(engine)
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
    let mut shutdown = self.shutdown_trigger_handle.make_shutdown();

    tokio::spawn(async move {
      let mut ticker = tokio::time::interval(interval);
      let mut flushes = FuturesUnordered::new();
      let mut shutdown_requested = false;
      loop {
        if shutdown_requested && flushes.is_empty() {
          log::info!("broker flush loop shutdown complete");
          return;
        }

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
          () = shutdown.cancelled(), if !shutdown_requested => {
            shutdown_requested = true;
            begin_shutdown_drain(&state);
            flush_notifier.notify_waiters();
            log::info!("broker flush loop draining buffered writes for shutdown");
          },
          _ = ticker.tick() => {},
          () = flush_notifier.notified() => {},
          Some(()) = flushes.next(), if !flushes.is_empty() => {},
        }
      }
    });
  }
}
