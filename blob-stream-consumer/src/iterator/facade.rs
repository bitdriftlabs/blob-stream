use super::delivery::{DeliveryGap, update_total_prefetch_bytes, update_worker_prefetch_metrics};
use super::driver::{ConsumerDriver, ConsumerDriverCommand};
use super::shared::{ConsumerIteratorMetrics, DeliveredSource, PendingCommit};
use super::{
  AssignmentCallback,
  ConsumerIterator,
  ConsumerSeekTarget,
  ConsumerSharedState,
  NextResult,
};
use crate::coordination::HeartbeatReport;
use crate::diagnostics::ConsumerDiagnostics;
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use bd_log_util::WarnTracker;
use blob_stream_types::VirtualPartitionId;
use log::{trace, warn};
use parking_lot::Mutex;
use std::sync::{Arc, OnceLock};
use time::ext::NumericalDuration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

static DELIVERY_GAP_WARN_TRACKER: OnceLock<WarnTracker> = OnceLock::new();

//
// ConsumerIteratorImpl
//

/// Default consumer iterator implementation.
pub struct ConsumerIteratorImpl {
  pub(super) started: bool,
  pub(super) diagnostics: ConsumerDiagnostics,
  pub(super) shared_state: Arc<Mutex<ConsumerSharedState>>,
  pub(super) delivery_notify: Arc<Notify>,
  pub(super) prefetch_space_notify: Arc<Notify>,
  pub(super) metrics: ConsumerIteratorMetrics,
  pub(super) assignment_callback: Arc<Mutex<Option<AssignmentCallback>>>,
  pub(super) command_tx: Option<mpsc::UnboundedSender<ConsumerDriverCommand>>,
  pub(super) driver: Option<ConsumerDriver>,
  pub(super) driver_task: Option<JoinHandle<()>>,
  #[cfg(test)]
  pub(super) next_after_delivery_state_check_hook: Option<NextAfterDeliveryStateCheckHook>,
}

#[cfg(test)]
pub(super) struct NextAfterDeliveryStateCheckHook {
  pub(super) state_checked: oneshot::Sender<()>,
  pub(super) release: oneshot::Receiver<()>,
}

impl ConsumerIteratorImpl {
  /// Stops local consumer work without committing offsets or releasing group state.
  ///
  /// This intentionally models a process crash for integration tests. Production callers must
  /// use [`ConsumerIterator::shutdown`] to release ownership gracefully.
  pub async fn abort_for_test(&mut self) -> Result<()> {
    ensure!(
      self.started,
      "consumer iterator must be started before aborting"
    );

    let (response_tx, response_rx) = oneshot::channel();
    self
      .command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer iterator command queue is unavailable"))?
      .send(ConsumerDriverCommand::AbortForTest {
        response: response_tx,
      })
      .map_err(|_| anyhow!("consumer driver stopped before test abort could be queued"))?;
    response_rx
      .await
      .map_err(|_| anyhow!("consumer driver stopped before test abort completed"))?;
    if let Some(driver_task) = self.driver_task.take() {
      let _ = driver_task.await;
    }
    self.started = false;
    self.command_tx = None;
    Ok(())
  }

  #[cfg(test)]
  pub(super) fn set_next_after_delivery_state_check_hook(
    &mut self,
    state_checked: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
  ) {
    self.next_after_delivery_state_check_hook = Some(NextAfterDeliveryStateCheckHook {
      state_checked,
      release,
    });
  }
}

#[async_trait]
impl ConsumerIterator for ConsumerIteratorImpl {
  fn start(&mut self) -> Result<()> {
    ensure!(!self.started, "consumer iterator already started");

    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let driver = self
      .driver
      .take()
      .ok_or_else(|| anyhow!("consumer iterator driver is unavailable"))?;

    self.command_tx = Some(command_tx);
    self.started = true;
    {
      let mut shared_state = self.shared_state.lock();
      shared_state.diagnostics.started = true;
      shared_state.diagnostics.prefetch_worker_running = true;
    }
    self.driver_task = Some(tokio::spawn(async move {
      driver.run(command_rx).await;
    }));
    Ok(())
  }

  async fn next(&mut self) -> Result<NextResult> {
    ensure!(
      self.started,
      "consumer iterator must be started before next"
    );

    loop {
      let notified = self.delivery_notify.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      let (delivery_result, terminal_error) = {
        let mut shared_state = self.shared_state.lock();
        let pending_bytes = shared_state.diagnostics.prefetch_pending_bytes;
        let ConsumerSharedState {
          active_partitions,
          delivery_state,
          terminal_error,
          ..
        } = &mut *shared_state;
        let delivery_result = delivery_state.try_take_next(active_partitions, &self.metrics);
        update_worker_prefetch_metrics(&self.metrics, delivery_state);
        update_total_prefetch_bytes(&self.metrics, delivery_state, pending_bytes);
        (delivery_result, terminal_error.clone())
      };
      #[cfg(test)]
      if let Some(hook) = self.next_after_delivery_state_check_hook.take() {
        let _ = hook.state_checked.send(());
        let _ = hook.release.await;
      }
      if let Some(delivery_result) = delivery_result {
        if let Some(gap) = delivery_result.gap
          && should_log_delivery_gap()
        {
          let partition_state_json = self
            .diagnostics
            .state_snapshot()
            .local
            .partitions
            .into_iter()
            .find(|partition| partition.virtual_partition_id == gap.virtual_partition_id)
            .and_then(|partition| serde_json::to_string(&partition).ok());
          log_delivery_gap(&gap, partition_state_json.as_deref());
        }
        self.prefetch_space_notify.notify_waiters();
        return Ok(delivery_result.next_result);
      }

      if let Some(error) = terminal_error {
        return Err(anyhow!("consumer driver stopped: {error}"));
      }

      notified.await;
    }
  }

  fn store_offset(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()> {
    let mut shared_state = self.shared_state.lock();
    let partition_state = shared_state
      .active_partitions
      .get_mut(&virtual_partition_id)
      .ok_or_else(|| {
        anyhow!("cannot store cursor for unassigned virtual partition {virtual_partition_id}")
      })?;
    let stored_source = partition_state
      .delivered_source_ranges
      .iter()
      .find_map(|range| {
        (range.start_offset <= offset && offset <= range.end_offset).then(|| DeliveredSource {
          offset,
          source_checkpoint: range.source_checkpoint.clone(),
          source: range.source.clone(),
        })
      })
      .ok_or_else(|| {
        anyhow!(
          "cannot store offset {offset} for partition {virtual_partition_id}: offset was not \
           delivered"
        )
      })?;
    let source_checkpoint = stored_source.source_checkpoint.clone();
    if let Some(staged) = &partition_state.pending_commit {
      ensure!(
        offset >= staged.offset,
        "cannot store offset {offset} below staged offset {} for partition {virtual_partition_id}",
        staged.offset
      );
    }
    partition_state.pending_commit = Some(PendingCommit {
      offset,
      source_checkpoint,
    });
    partition_state.last_stored_source = Some(stored_source);
    trace!("consumer stored offset: partition={virtual_partition_id}, offset={offset}");
    Ok(())
  }

  async fn commit(&mut self) -> Result<HeartbeatReport> {
    ensure!(
      self.started,
      "consumer iterator must be started before commit"
    );
    let (response_tx, response_rx) = oneshot::channel();
    self
      .command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer iterator command queue is unavailable"))?
      .send(ConsumerDriverCommand::Commit {
        response: response_tx,
      })
      .map_err(|_| anyhow!("consumer driver stopped before commit could be queued"))?;
    response_rx
      .await
      .map_err(|_| anyhow!("consumer driver stopped before commit completed"))?
  }

  async fn shutdown(mut self: Box<Self>) -> Result<()> {
    if !self.started {
      return Ok(());
    }

    let (response_tx, response_rx) = oneshot::channel();
    self
      .command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer iterator command queue is unavailable"))?
      .send(ConsumerDriverCommand::Shutdown {
        response: response_tx,
      })
      .map_err(|_| anyhow!("consumer driver stopped before shutdown could be queued"))?;
    let shutdown_result = response_rx
      .await
      .map_err(|_| anyhow!("consumer driver stopped before shutdown completed"))?;
    if let Some(driver_task) = self.driver_task.take() {
      let _ = driver_task.await;
    }
    self.started = false;
    shutdown_result
  }

  async fn seek(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    target: ConsumerSeekTarget,
  ) -> Result<()> {
    ensure!(
      self.started,
      "consumer iterator must be started before seek"
    );
    let (response_tx, response_rx) = oneshot::channel();
    self
      .command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer iterator command queue is unavailable"))?
      .send(ConsumerDriverCommand::Seek {
        virtual_partition_id,
        target,
        response: response_tx,
      })
      .map_err(|_| anyhow!("consumer driver stopped before seek could be queued"))?;
    response_rx
      .await
      .map_err(|_| anyhow!("consumer driver stopped before seek completed"))?
  }

  fn set_assignment_callback(&mut self, callback: AssignmentCallback) {
    let mut active_assignment = self
      .shared_state
      .lock()
      .active_partitions
      .keys()
      .copied()
      .collect::<Vec<_>>();
    active_assignment.sort_unstable();
    *self.assignment_callback.lock() = Some(Arc::clone(&callback));
    if !active_assignment.is_empty() {
      callback(&active_assignment);
    }
  }

  fn diagnostics(&self) -> Option<ConsumerDiagnostics> {
    Some(self.diagnostics.clone())
  }
}

/// Decide whether the next gap receives full forensic serialization and a warning.
fn should_log_delivery_gap() -> bool {
  DELIVERY_GAP_WARN_TRACKER
    .get_or_init(WarnTracker::default)
    .should_warn(15.seconds())
}

/// Emit source identifiers needed to inspect both sides of a delivery discontinuity.
fn log_delivery_gap(gap: &DeliveryGap, partition_state_json: Option<&str>) {
  let admission_scan_json = gap
    .admission_scan
    .as_ref()
    .and_then(|scan| serde_json::to_string(scan).ok())
    .unwrap_or_else(|| "null".to_string());
  let previous_offset = gap.previous_source.as_ref().map(|source| source.offset);
  let previous_checkpoint = gap
    .previous_source
    .as_ref()
    .map(|source| &source.source_checkpoint);
  let previous_blob_key = gap
    .previous_source
    .as_ref()
    .map(|source| source.source.blob_key.as_str());
  let last_stored_offset = gap.last_stored_source.as_ref().map(|source| source.offset);
  let last_stored_checkpoint = gap
    .last_stored_source
    .as_ref()
    .map(|source| &source.source_checkpoint);
  let last_stored_blob_key = gap
    .last_stored_source
    .as_ref()
    .map(|source| source.source.blob_key.as_str());
  warn!(
    "consumer delivery gap: partition={}, expected_offset={}, received_offset={}, \
     missing_sequences={}, previous_delivered_offset={previous_offset:?}, \
     previous_source_checkpoint={previous_checkpoint:?}, previous_blob_key={previous_blob_key:?}, \
     last_stored_offset={last_stored_offset:?}, \
     last_stored_source_checkpoint={last_stored_checkpoint:?}, \
     last_stored_blob_key={last_stored_blob_key:?}, current_batch_range={}..={}, \
     current_source_checkpoint={:?}, current_blob_key={}, current_metadata_published_at={}, \
     admission_scan_json={admission_scan_json}, partition_state_json={partition_state_json:?}",
    gap.virtual_partition_id,
    gap.expected_offset,
    gap.received_offset,
    gap.missing_sequences,
    gap.current_batch_start_offset,
    gap.current_batch_end_offset,
    gap.current_source_checkpoint,
    gap.current_source.blob_key.as_str(),
    gap.current_source.metadata_published_at,
  );
}
