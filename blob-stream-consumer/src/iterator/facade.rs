use super::delivery::{update_total_prefetch_bytes, update_worker_prefetch_metrics};
use super::driver::{ConsumerDriver, ConsumerDriverCommand};
use super::shared::{ConsumerIteratorMetrics, PendingCommit};
use super::{AssignmentCallback, ConsumerIterator, ConsumerSharedState, NextResult};
use crate::coordination::HeartbeatReport;
use crate::diagnostics::ConsumerDiagnostics;
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use blob_stream_types::VirtualPartitionId;
use log::trace;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

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
      let (next_result, terminal_error) = {
        let mut shared_state = self.shared_state.lock();
        let pending_bytes = shared_state.diagnostics.prefetch_pending_bytes;
        let ConsumerSharedState {
          active_partitions,
          delivery_state,
          terminal_error,
          ..
        } = &mut *shared_state;
        let next_result = delivery_state.try_take_next(active_partitions, &self.metrics);
        update_worker_prefetch_metrics(&self.metrics, delivery_state);
        update_total_prefetch_bytes(&self.metrics, delivery_state, pending_bytes);
        (next_result, terminal_error.clone())
      };
      #[cfg(test)]
      if let Some(hook) = self.next_after_delivery_state_check_hook.take() {
        let _ = hook.state_checked.send(());
        let _ = hook.release.await;
      }
      if let Some(next_result) = next_result {
        self.prefetch_space_notify.notify_waiters();
        return Ok(next_result);
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
    let source_checkpoint = partition_state
      .delivered_source_ranges
      .iter()
      .find_map(|range| {
        (range.start_offset <= offset && offset <= range.end_offset)
          .then(|| range.source_checkpoint.clone())
      })
      .ok_or_else(|| {
        anyhow!(
          "cannot store offset {offset} for partition {virtual_partition_id}: offset was not \
           delivered"
        )
      })?;
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

  async fn seek(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()> {
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
        offset,
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
