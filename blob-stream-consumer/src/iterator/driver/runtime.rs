use super::{ConsumerDriver, ConsumerDriverCommand, HeartbeatTrigger, RETRY_MAX_DELAY};
use anyhow::{Result, ensure};
use bd_backoff::InfiniteBackoff;
use bd_log_util::warn_every;
use log::{debug, info, trace};
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
      let mut revocation_completed = match self.finish_pending_revocation_if_completed().await {
        Ok(completed) => completed,
        Err(error) => {
          self.shared_state.lock().terminal_error = Some(format!("{error:#}"));
          let _ = self.shutdown().await;
          self.delivery_notify.notify_waiters();
          return;
        },
      };
      let now = self.time_provider.now();

      let heartbeat_failed = if now >= self.next_heartbeat_at {
        trace!(
          "consumer scheduled heartbeat due: topic={}, group_id={}, member_id={}, generation={}, \
           now={}, due_at={}, overdue_ms={}, active_partitions={:?}, pending_commits={}",
          self.group_config.topic,
          self.group_config.group_id,
          self.group_config.member_id,
          self.coordinator.generation(),
          now,
          self.next_heartbeat_at,
          (now - self.next_heartbeat_at).whole_milliseconds(),
          self.active_assignment,
          self
            .shared_state
            .lock()
            .active_partitions
            .values()
            .filter(|state| state.pending_commit.is_some())
            .count()
        );
        if let Err(error) = self.heartbeat(now, HeartbeatTrigger::Scheduled).await {
          warn_every!(
            15.seconds(),
            "consumer scheduled heartbeat retrying after error: error={error:#}"
          );
          let retry_delay = self
            .heartbeat_retry_backoff
            .next_backoff()
            .min(RETRY_MAX_DELAY);
          let heartbeat_retry_deadline = self.heartbeat_retry_deadline();
          let retry_delay = if now < heartbeat_retry_deadline {
            retry_delay.min(heartbeat_retry_deadline - now)
          } else {
            retry_delay
          };
          self.metrics.heartbeat_retry_attempts.inc();
          self.next_heartbeat_at = now.saturating_add(retry_delay);
          self.refresh_diagnostics();
          true
        } else {
          self.heartbeat_retry_backoff.reset();
          false
        }
      } else {
        false
      };

      revocation_completed = revocation_completed && self.pending_revocation_completion.is_none();

      if revocation_completed && now >= self.next_rebalance_at {
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
          match self.maybe_rebalance(now).await {
            Ok(()) => self.rebalance_retry_backoff.reset(),
            Err(error) => {
              if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
                lifecycle_hooks
                  .rebalance_failed(&self.group_config.member_id, self.coordinator.generation())
                  .await;
              }
              warn_every!(
                15.seconds(),
                "consumer rebalance retrying after error: error={error:#}"
              );
              let retry_delay = self
                .rebalance_retry_backoff
                .next_backoff()
                .min(RETRY_MAX_DELAY);
              self.metrics.rebalance_retry_attempts.inc();
              self.next_rebalance_at = now.saturating_add(retry_delay);
              self.refresh_rebalance_diagnostics();
            },
          }
        }
      }

      let until_heartbeat = self.next_heartbeat_at - now;
      let until_rebalance = self.next_rebalance_at - now;
      let wait_duration = if revocation_completed {
        until_heartbeat
          .min(until_rebalance)
          .max(time::Duration::milliseconds(1))
      } else {
        until_heartbeat.clamp(
          time::Duration::milliseconds(1),
          time::Duration::milliseconds(100),
        )
      };
      let time_provider = Arc::clone(&self.time_provider);
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
