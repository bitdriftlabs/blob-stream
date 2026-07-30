use super::{ConsumerDriver, ConsumerDriverCommand, HeartbeatTrigger};
use anyhow::{Result, ensure};
use bd_log::warn_every;
use blob_stream_types::format_unix_timestamp_ms;
use log::{debug, info, trace};
use std::cmp::max;
use std::sync::Arc;
use std::time::Instant;
use time::ext::NumericalDuration;
use tokio::sync::mpsc;

impl ConsumerDriver {
  pub(in crate::iterator) fn start(&mut self) -> Result<()> {
    ensure!(!self.started, "consumer iterator already started");
    self.started = true;
    self.spawn_prefetch_task();
    self.refresh_diagnostics();
    info!(
      "consumer iterator started: topic={}, group_id={}, member_id={}",
      self.group_config.topic, self.group_config.group_id, self.group_config.member_id
    );
    Ok(())
  }

  pub(in crate::iterator) async fn run(
    mut self,
    mut command_rx: mpsc::UnboundedReceiver<ConsumerDriverCommand>,
  ) {
    if let Err(error) = self.start() {
      {
        let mut shared_state = self.shared_state.lock();
        shared_state.terminal_error = Some(format!("{error:#}"));
        shared_state.diagnostics.started = false;
        shared_state.diagnostics.prefetch_worker_running = false;
      }
      self.delivery_notify.notify_waiters();
      return;
    }

    loop {
      let revocation_completed = match self.finish_pending_revocation_if_completed().await {
        Ok(completed) => completed,
        Err(error) => {
          self.shared_state.lock().terminal_error = Some(format!("{error:#}"));
          let _ = self.shutdown().await;
          self.delivery_notify.notify_waiters();
          return;
        },
      };
      let now_ts_ms = self.now_unix_millis();

      let heartbeat_failed = if now_ts_ms >= self.next_heartbeat_at_ms {
        trace!(
          "consumer scheduled heartbeat due: topic={}, group_id={}, member_id={}, generation={}, \
           now={}, due_at={}, overdue_ms={}, active_partitions={:?}, pending_commits={}",
          self.group_config.topic,
          self.group_config.group_id,
          self.group_config.member_id,
          self.coordinator.generation(),
          format_unix_timestamp_ms(now_ts_ms),
          format_unix_timestamp_ms(self.next_heartbeat_at_ms),
          now_ts_ms.saturating_sub(self.next_heartbeat_at_ms),
          self.active_assignment,
          self
            .shared_state
            .lock()
            .active_partitions
            .values()
            .filter(|state| state.pending_commit.is_some())
            .count()
        );
        if let Err(error) = self.heartbeat(now_ts_ms, HeartbeatTrigger::Scheduled).await {
          warn_every!(
            15.seconds(),
            "consumer scheduled heartbeat retrying after error: error={error:#}"
          );
          self.next_heartbeat_at_ms = now_ts_ms.saturating_add(1_000);
          self.refresh_diagnostics();
          true
        } else {
          false
        }
      } else {
        false
      };

      if revocation_completed && now_ts_ms >= self.next_rebalance_at_ms {
        if heartbeat_failed {
          debug!(
            "consumer rebalance deferred after heartbeat failure: topic={}, group_id={}, \
             member_id={}, generation={}",
            self.group_config.topic,
            self.group_config.group_id,
            self.group_config.member_id,
            self.coordinator.generation(),
          );
        } else {
          match self.maybe_rebalance(now_ts_ms).await {
            Ok(()) => {},
            Err(error) => {
              warn_every!(
                15.seconds(),
                "consumer rebalance retrying after error: error={error:#}"
              );
              self.next_rebalance_at_ms = now_ts_ms.saturating_add(1_000);
              self.refresh_rebalance_diagnostics();
            },
          }
        }
      }

      let until_heartbeat_ms = (self.next_heartbeat_at_ms - now_ts_ms).max(0);
      let until_rebalance_ms = (self.next_rebalance_at_ms - now_ts_ms).max(0);
      let wait_ms = if revocation_completed {
        max(1_i64, until_heartbeat_ms.min(until_rebalance_ms)).cast_unsigned()
      } else {
        until_heartbeat_ms.clamp(1, 100).cast_unsigned()
      };
      let time_provider = Arc::clone(&self.time_provider);
      let wait_duration = time::Duration::milliseconds(i64::try_from(wait_ms).unwrap_or(i64::MAX));
      tokio::select! {
        command = command_rx.recv() => {
          let Some(command) = command else {
            let _ = self.shutdown().await;
            self.delivery_notify.notify_waiters();
            return;
          };
          match command {
            ConsumerDriverCommand::Commit { response } => {
              let started_at = Instant::now();
              let result = self.commit().await;
              self.metrics.commit_latency_seconds.observe(started_at.elapsed().as_secs_f64());
              let _ = response.send(result);
            },
            ConsumerDriverCommand::AbortForTest { response } => {
              self.stop_prefetch_task().await;
              self.started = false;
              self.refresh_diagnostics();
              let _ = response.send(());
              self.delivery_notify.notify_waiters();
              return;
            },
            ConsumerDriverCommand::Seek {
              virtual_partition_id,
              offset,
              response,
            } => {
              self.seek(virtual_partition_id, offset, response);
            },
            ConsumerDriverCommand::Shutdown { response } => {
              let _ = response.send(self.shutdown().await);
              self.delivery_notify.notify_waiters();
              return;
            },
          }
        },
        () = self.revocation_notify.notified(), if !revocation_completed => {},
        () = time_provider.sleep(wait_duration) => {},
      }
    }
  }
}
