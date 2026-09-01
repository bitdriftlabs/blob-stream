#[cfg(test)]
#[path = "./assignment_test.rs"]
mod tests;

use super::super::allocation::{
  AllocationTransitionDecision,
  LeaseExpirationUpdate,
  begin_allocation_transition,
};
use super::super::metrics::WriteMetrics;
use super::super::state::WriteState;
use super::super::{BrokerLifecycleHooks, TopicInfo, WriteEngineImpl};
use super::acquire_lease_and_reserve_sequences;
use bd_log_util::warn_every;
use bd_time::TimeProvider;
use blob_stream_broker_discovery::{
  BrokerMembership,
  balanced_assignment,
  writer_virtual_partitions,
};
use blob_stream_metadata_store::{
  LeaseAcquireAndReserveOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  ProducerPartitionLeaseKey,
};
use blob_stream_types::VirtualPartitionId;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{debug, info, trace};
use protobuf::Chars;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use tokio::sync::watch;

const LEASE_RETRY_INITIAL_DELAY: time::Duration = time::Duration::milliseconds(250);
const LEASE_RETRY_MAX_DELAY: time::Duration = time::Duration::seconds(2);

//
// LeaseRetrySchedule
//

#[derive(Clone, Copy, Debug)]
struct LeaseRetrySchedule {
  next_retry_at: OffsetDateTime,
  retry_delay: time::Duration,
}

impl LeaseRetrySchedule {
  fn new(next_retry_at: OffsetDateTime) -> Self {
    Self {
      next_retry_at,
      retry_delay: LEASE_RETRY_INITIAL_DELAY,
    }
  }

  fn schedule_retry(&mut self, now: OffsetDateTime) -> time::Duration {
    let delay = self.retry_delay;
    self.next_retry_at = now + delay;
    self.retry_delay = self
      .retry_delay
      .saturating_mul(2)
      .min(LEASE_RETRY_MAX_DELAY);
    delay
  }

  fn reset(&mut self, next_retry_at: OffsetDateTime) {
    self.next_retry_at = next_retry_at;
    self.retry_delay = LEASE_RETRY_INITIAL_DELAY;
  }
}

impl WriteEngineImpl {
  pub(super) fn owned_virtual_partitions(
    topics: &HashMap<Chars, TopicInfo>,
    writer_id: u32,
    holder_id: &str,
    membership: &BrokerMembership,
  ) -> Vec<(Chars, VirtualPartitionId)> {
    let partitions = writer_virtual_partitions(
      topics
        .values()
        .map(|topic| (topic.name.clone(), topic.partition_count, topic.num_writers)),
      writer_id,
    );

    let mut owned: Vec<(Chars, VirtualPartitionId)> = balanced_assignment(partitions, membership)
      .into_iter()
      .filter_map(|(partition, owner)| {
        (owner.node_id.as_str() == holder_id)
          .then_some((partition.topic, partition.virtual_partition_id))
      })
      .collect();

    owned.sort_unstable();
    owned
  }

  pub(in crate::write) fn spawn_lease_self_assignment_loop(
    &self,
    mut membership_rx: watch::Receiver<BrokerMembership>,
  ) {
    let heartbeat_interval = self.config.heartbeat_interval;
    let heartbeat_tick_interval =
      StdDuration::try_from(heartbeat_interval).unwrap_or(StdDuration::from_secs(1));
    let topics = self.topics.clone();
    let holder_id = self.holder_id.clone();
    let lease_session_id = self.lease_session_id.clone();
    let writer_id = self.config.writer_id;
    let lease_duration = self.config.lease_duration;
    let base_reservation_size = self.config.reservation_size;
    let lease_store = Arc::clone(&self.lease_store);
    let state = Arc::clone(&self.state);
    let time_provider = Arc::clone(&self.time_provider);
    let metrics = self.metrics.clone();
    let flush_notifier = Arc::clone(&self.flush_notifier);
    let lifecycle_hooks = self.lifecycle_hooks.clone();
    let mut shutdown = self.shutdown_trigger_handle.make_shutdown();

    tokio::spawn(async move {
      // Lease-store timestamps and takeover retries use the injected TimeProvider, but recurring
      // maintenance measures local elapsed time. Keeping it on Tokio's monotonic clock prevents
      // manual test jumps across historical windows from applying missed heartbeats at one final
      // timestamp, which would incorrectly expire and reacquire a healthy lease fence.
      let mut heartbeat_ticker = tokio::time::interval(heartbeat_tick_interval);
      // If the watch sender closes, we continue on ticker-only cadence so lease maintenance keeps
      // running instead of silently stalling.
      let mut membership_updates_open = true;
      let mut assignment_activated = false;
      let mut previously_assigned: HashSet<(Chars, VirtualPartitionId)> = HashSet::new();
      let mut pending_acquisitions: HashMap<(Chars, VirtualPartitionId), LeaseRetrySchedule> =
        HashMap::new();

      loop {
        let next_acquisition_retry = pending_acquisitions
          .values()
          .map(|retry| retry.next_retry_at)
          .min();
        let acquisition_retry_sleep = async {
          let Some(next_retry_at) = next_acquisition_retry else {
            std::future::pending().await
          };
          let delay = (next_retry_at - time_provider.now()).max(time::Duration::ZERO);
          time_provider.sleep(delay).await;
        };
        tokio::pin!(acquisition_retry_sleep);

        let shutting_down = if membership_updates_open {
          tokio::select! {
            _ = heartbeat_ticker.tick() => false,
            () = &mut acquisition_retry_sleep => false,
            changed = membership_rx.changed() => {
              if changed.is_err() {
                membership_updates_open = false;
              }
              false
            }
            () = shutdown.cancelled() => true,
          }
        } else {
          tokio::select! {
            _ = heartbeat_ticker.tick() => false,
            () = &mut acquisition_retry_sleep => false,
            () = shutdown.cancelled() => true,
          }
        };

        let membership = membership_rx.borrow().clone();
        {
          let mut guard = state.lock();
          guard.membership = membership.clone();
        }
        if shutting_down {
          let mut partitions: HashSet<_> = owned_partitions_for_shutdown(&state);
          partitions.extend(Self::owned_virtual_partitions(
            &topics,
            writer_id,
            &holder_id,
            &membership,
          ));
          let mut partitions = partitions.into_iter().collect::<Vec<_>>();
          partitions.sort_unstable();
          info!(
            "broker releasing drained leases for shutdown: holder_id={holder_id}, \
             partitions={partitions:?}"
          );
          Self::release_partition_leases(
            &lease_store,
            &state,
            &flush_notifier,
            &metrics,
            &holder_id,
            &lease_session_id,
            partitions,
            time_provider.as_ref(),
            lease_duration,
            heartbeat_interval,
            lifecycle_hooks.as_ref(),
          )
          .await;
          return;
        }

        // Kubernetes does not publish this pod to Endpoints until it is ready. Waiting here keeps
        // the server available for readiness checks without treating an incomplete snapshot as
        // authoritative ownership.
        if !assignment_activated {
          let Some(nodes) = membership.nodes() else {
            continue;
          };
          let self_is_member = nodes.iter().any(|node| node.node_id.as_str() == holder_id);
          if !self_is_member {
            continue;
          }
          assignment_activated = true;
          info!(
            "broker lease assignment activated: holder_id={holder_id}, membership_nodes={nodes:?}"
          );
        }

        let owned = Self::owned_virtual_partitions(&topics, writer_id, &holder_id, &membership);
        let currently_owned: HashSet<(Chars, VirtualPartitionId)> = owned.iter().cloned().collect();
        pending_acquisitions.retain(|partition, _| currently_owned.contains(partition));

        let gained_partitions = sorted_partition_delta(&currently_owned, &previously_assigned);
        let lost_partitions = sorted_partition_delta(&previously_assigned, &currently_owned);
        if !gained_partitions.is_empty() || !lost_partitions.is_empty() {
          info!(
            "broker partition assignment changed: holder_id={holder_id}, membership_nodes={:?}, \
             assigned_partitions={}, gained_partitions={gained_partitions:?}, \
             lost_partitions={lost_partitions:?}",
            membership.nodes().unwrap_or_default(),
            currently_owned.len(),
          );
        }

        // On membership changes (scale up/down), we release any partition that moved away from
        // this broker instead of waiting for lease TTL expiration. This shortens convergence and
        // reduces transient NOT_LEASE_HOLDER retries for producers during rebalance.
        Self::release_partition_leases(
          &lease_store,
          &state,
          &flush_notifier,
          &metrics,
          &holder_id,
          &lease_session_id,
          lost_partitions,
          time_provider.as_ref(),
          lease_duration,
          heartbeat_interval,
          lifecycle_hooks.as_ref(),
        )
        .await;

        // Record assignment after reconciling releases so the next pass can compute deltas.
        previously_assigned = currently_owned;

        for (topic, virtual_partition_id) in owned {
          let partition = (topic.clone(), virtual_partition_id);
          if pending_acquisitions
            .get(&partition)
            .is_some_and(|retry| retry.next_retry_at > time_provider.now())
          {
            continue;
          }
          let key = ProducerPartitionLeaseKey {
            topic: topic.clone(),
            virtual_partition_id,
          };

          let now = time_provider.now();
          let transition = loop {
            match begin_allocation_transition(
              &state,
              &topic,
              virtual_partition_id,
              1,
              now,
              true,
              base_reservation_size,
            ) {
              AllocationTransitionDecision::Draining => break None,
              AllocationTransitionDecision::Ready => {
                unreachable!("lease renewal always needs an allocation transition")
              },
              AllocationTransitionDecision::Waiting(notified) => notified.await,
              AllocationTransitionDecision::Claimed(transition) => break Some(transition),
            }
          };
          let Some(transition) = transition else {
            continue;
          };

          let reservation_request = transition.reservation;
          match acquire_lease_and_reserve_sequences(
            &lease_store,
            &holder_id,
            &lease_session_id,
            key,
            now,
            lease_duration,
            reservation_request.map(|request| request.size),
            &metrics,
          )
          .await
          {
            Ok(LeaseAcquireAndReserveOutcome::Acquired { lease, reservation }) => {
              pending_acquisitions.remove(&partition);
              if let Some(reservation) = reservation.as_ref() {
                metrics.record_sequence_reservation(reservation);
                debug!(
                  "broker sequence reservation coalesced with lease maintenance: topic={topic}, \
                   virtual_partition_id={virtual_partition_id}, reason={:?}, start={}, end={}",
                  reservation_request.map(|request| request.reason),
                  reservation.start,
                  reservation.end,
                );
              }
              let lease_expiration_update =
                LeaseExpirationUpdate::Set(Some(lease.lease_expiration_at));
              if transition.lease_was_expired {
                Self::log_lease_acquired(
                  &holder_id,
                  &lease_session_id,
                  &topic,
                  virtual_partition_id,
                  &lease,
                );
              }
              transition.transition.finish_lease_maintenance(
                lease_expiration_update,
                Some(lease),
                reservation,
                transition
                  .records_allocated_since_last_maintenance
                  .unwrap_or_default(),
              );
              {
                let mut state = state.lock();
                if let Some(partition_state) =
                  state.partition_state_mut_if_present(&topic, virtual_partition_id)
                {
                  partition_state.draining = false;
                }
              }
            },
            Ok(LeaseAcquireAndReserveOutcome::HeldByOther(_)) => {
              let retry = pending_acquisitions
                .entry(partition)
                .or_insert_with(|| LeaseRetrySchedule::new(now));
              let scheduled_retry_delay = retry.schedule_retry(now);
              debug!(
                "broker lease acquisition deferred: holder_id={holder_id}, topic={topic}, \
                 virtual_partition_id={virtual_partition_id}, retry_at={}, retry_delay_ms={}",
                retry.next_retry_at,
                scheduled_retry_delay.whole_milliseconds(),
              );
              transition
                .transition
                .finish(LeaseExpirationUpdate::Set(None), None, None);
            },
            Err(error) => {
              pending_acquisitions.remove(&partition);
              if reservation_request.is_some() {
                metrics.sequence_reservation_failures_total.inc();
              }
              warn_every!(
                15.seconds(),
                "lease self-assignment acquire/reserve failed: topic={topic}, \
                 virtual_partition_id={virtual_partition_id}, requested_size={:?}, error={error:#}",
                reservation_request.map(|request| request.size),
              );
              transition
                .transition
                .finish(LeaseExpirationUpdate::Preserve, None, None);
            },
          }
        }
      }
    });
  }

  async fn release_partition_lease(
    lease_store: &Arc<dyn blob_stream_metadata_store::ProducerPartitionLeaseStore>,
    state: &Arc<parking_lot::Mutex<WriteState>>,
    flush_notifier: &Arc<tokio::sync::Notify>,
    metrics: &WriteMetrics,
    holder_id: &str,
    lease_session_id: &str,
    topic: &Chars,
    virtual_partition_id: VirtualPartitionId,
    time_provider: &dyn TimeProvider,
    lease_duration: time::Duration,
    heartbeat_interval: time::Duration,
    lifecycle_hooks: Option<&Arc<dyn BrokerLifecycleHooks>>,
  ) {
    let key = ProducerPartitionLeaseKey {
      topic: topic.clone(),
      virtual_partition_id,
    };

    let next_heartbeat_at = {
      let mut state = state.lock();
      let partition_state = state.partition_state_mut(topic, virtual_partition_id);
      partition_state.draining = true;
      partition_state.lease_expiration_at.map_or_else(
        || time_provider.now(),
        |expiration| expiration - heartbeat_interval,
      )
    };
    metrics.lease_drain_starts_total.inc();
    info!(
      "broker partition drain started: holder_id={holder_id}, topic={topic}, \
       virtual_partition_id={virtual_partition_id}"
    );
    if let Some(lifecycle_hooks) = lifecycle_hooks {
      lifecycle_hooks
        .lease_drain_started(topic.as_str(), virtual_partition_id)
        .await;
    }
    flush_notifier.notify_one();

    Self::wait_for_partition_drain(
      lease_store,
      state,
      &key,
      holder_id,
      lease_session_id,
      time_provider,
      next_heartbeat_at,
      lease_duration,
      heartbeat_interval,
    )
    .await;
    metrics.lease_drain_completions_total.inc();
    info!(
      "broker partition drain complete: holder_id={holder_id}, topic={topic}, \
       virtual_partition_id={virtual_partition_id}"
    );
    if let Some(lifecycle_hooks) = lifecycle_hooks {
      lifecycle_hooks
        .partition_drained(topic.as_str(), virtual_partition_id)
        .await;
      lifecycle_hooks
        .before_lease_release(topic.as_str(), virtual_partition_id)
        .await;
    }

    match lease_store
      .release_lease(&key, holder_id, lease_session_id, time_provider.now())
      .await
    {
      Ok(
        LeaseReleaseOutcome::Released
        | LeaseReleaseOutcome::Expired
        | LeaseReleaseOutcome::HeldByOther(_),
      ) => {
        // Clear local lease/allocator state immediately to avoid accepting writes based on stale
        // in-memory lease data after ownership moved away.
        {
          let mut state = state.lock();
          if let Some(partition_state) =
            state.partition_state_mut_if_present(topic, virtual_partition_id)
          {
            partition_state.lease_expiration_at = None;
            partition_state.lease_fence = None;
            partition_state.reset_sequence_allocation();
          }
        }
        if let Some(lifecycle_hooks) = lifecycle_hooks {
          lifecycle_hooks
            .lease_released(topic.as_str(), virtual_partition_id)
            .await;
        }
      },
      Err(error) => {
        warn_every!(
          15.seconds(),
          "lease self-assignment release failed: {error}"
        );
      },
    }
  }

  async fn release_partition_leases(
    lease_store: &Arc<dyn blob_stream_metadata_store::ProducerPartitionLeaseStore>,
    state: &Arc<parking_lot::Mutex<WriteState>>,
    flush_notifier: &Arc<tokio::sync::Notify>,
    metrics: &WriteMetrics,
    holder_id: &str,
    lease_session_id: &str,
    partitions: Vec<(Chars, VirtualPartitionId)>,
    time_provider: &dyn TimeProvider,
    lease_duration: time::Duration,
    heartbeat_interval: time::Duration,
    lifecycle_hooks: Option<&Arc<dyn BrokerLifecycleHooks>>,
  ) {
    let mut releases = FuturesUnordered::new();
    for (topic, virtual_partition_id) in partitions {
      releases.push(async move {
        Self::release_partition_lease(
          lease_store,
          state,
          flush_notifier,
          metrics,
          holder_id,
          lease_session_id,
          &topic,
          virtual_partition_id,
          time_provider,
          lease_duration,
          heartbeat_interval,
          lifecycle_hooks,
        )
        .await;
      });
    }

    while releases.next().await.is_some() {}
  }

  async fn wait_for_partition_drain(
    lease_store: &Arc<dyn blob_stream_metadata_store::ProducerPartitionLeaseStore>,
    state: &Arc<parking_lot::Mutex<WriteState>>,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    time_provider: &dyn TimeProvider,
    next_heartbeat_at: OffsetDateTime,
    lease_duration: time::Duration,
    heartbeat_interval: time::Duration,
  ) {
    let mut lease_held = true;
    let mut heartbeat_retry = LeaseRetrySchedule::new(next_heartbeat_at);
    loop {
      let notified = state
        .lock()
        .partition_state(key.topic.as_str(), key.virtual_partition_id)
        .map(|partition_state| partition_state.drain_notify.clone().notified_owned());
      let Some(notified) = notified else {
        return;
      };
      tokio::pin!(notified);
      notified.as_mut().enable();
      let is_drained = {
        let state = state.lock();
        let Some(partition_state) =
          state.partition_state(key.topic.as_str(), key.virtual_partition_id)
        else {
          return;
        };
        // Register before checking state so a flush completion cannot notify between the check
        // and the await below.
        trace!(
          "broker partition drain state: topic={}, virtual_partition_id={}, \
           outstanding_flushes={}, allocation_in_flight={}, buffered_batches={}, draining={}",
          key.topic,
          key.virtual_partition_id,
          partition_state.outstanding_flushes,
          partition_state.allocation_in_flight,
          partition_state.buffer.batches.len(),
          partition_state.draining,
        );
        partition_state.is_drained()
      };
      if is_drained {
        return;
      }
      trace!(
        "broker partition drain waiting: topic={}, virtual_partition_id={}",
        key.topic, key.virtual_partition_id,
      );
      if lease_held {
        let heartbeat_delay =
          (heartbeat_retry.next_retry_at - time_provider.now()).max(time::Duration::ZERO);
        tokio::select! {
          () = notified => {
            trace!(
              "broker partition drain notified: topic={}, virtual_partition_id={}",
              key.topic,
              key.virtual_partition_id,
            );
          },
          () = time_provider.sleep(heartbeat_delay) => {
            match lease_store
              .heartbeat_lease(
                key,
                holder_id,
                lease_session_id,
                time_provider.now(),
                lease_duration,
              )
              .await
            {
              Ok(LeaseHeartbeatOutcome::Renewed(lease)) => {
                let mut state = state.lock();
                if let Some(partition_state) =
                  state.partition_state_mut_if_present(key.topic.as_str(), key.virtual_partition_id)
                {
                  partition_state.lease_expiration_at = Some(lease.lease_expiration_at);
                }
                heartbeat_retry.reset(time_provider.now() + heartbeat_interval);
              },
              Ok(LeaseHeartbeatOutcome::HeldByOther(_) | LeaseHeartbeatOutcome::Expired) => {
                lease_held = false;
                info!(
                  "broker partition drain lost lease: holder_id={holder_id}, topic={}, \
                   virtual_partition_id={}",
                  key.topic,
                  key.virtual_partition_id,
                );
              },
              Err(error) => {
                let retry_delay = heartbeat_retry.schedule_retry(time_provider.now());
                warn_every!(
                  15.seconds(),
                  "broker partition drain lease heartbeat failed: holder_id={holder_id}, \
                   topic={}, virtual_partition_id={}, retry_delay_ms={}, error={error:#}",
                  key.topic,
                  key.virtual_partition_id,
                  retry_delay.whole_milliseconds(),
                );
              },
            }
          },
        }
      } else {
        notified.await;
        trace!(
          "broker partition drain notified: topic={}, virtual_partition_id={}",
          key.topic, key.virtual_partition_id,
        );
      }
    }
  }
}

fn owned_partitions_for_shutdown(
  state: &Arc<parking_lot::Mutex<WriteState>>,
) -> HashSet<(Chars, VirtualPartitionId)> {
  state.lock().partition_keys().into_iter().collect()
}

fn sorted_partition_delta(
  left: &HashSet<(Chars, VirtualPartitionId)>,
  right: &HashSet<(Chars, VirtualPartitionId)>,
) -> Vec<(Chars, VirtualPartitionId)> {
  let mut partitions: Vec<_> = left.difference(right).cloned().collect();
  partitions.sort_unstable();
  partitions
}
