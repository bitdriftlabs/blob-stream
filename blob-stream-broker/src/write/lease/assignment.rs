#[cfg(test)]
#[path = "./assignment_test.rs"]
mod tests;

use super::super::allocation::{
  AllocationTransitionDecision,
  AllocationTransitionFinish,
  AllocationTransitionWork,
  LeaseExpirationUpdate,
  begin_allocation_transition,
};
use super::super::metrics::WriteMetrics;
use super::super::state::WriteState;
use super::super::{BrokerLifecycleHooks, TopicInfo, WriteEngineImpl};
use super::{LeaseAcquisitionOrigin, acquire_lease_and_reserve_sequences};
use anyhow::Result;
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
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
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

// A partition key is carried through supervisor completions so the coordinator can update the
// matching pending set before it recomputes effective write admission.
type PartitionKey = (Chars, VirtualPartitionId);

// An acquisition task either observes that the partition no longer needs work, or returns the
// still-owned allocation transition with its remote result. Holding the transition in the future
// keeps `allocation_in_flight` set until the coordinator applies or fences that result.
type MaintenanceAcquisitionCompletion = (
  PartitionKey,
  Option<(
    AllocationTransitionWork,
    Result<LeaseAcquireAndReserveOutcome>,
  )>,
);

//
// LeaseReleaseReason
//

#[derive(Clone, Copy, Debug)]
pub(super) enum LeaseReleaseReason {
  AssignmentLoss,
  DefensiveReconciliation,
  Shutdown,
}

impl LeaseReleaseReason {
  const fn as_str(self) -> &'static str {
    match self {
      Self::AssignmentLoss => "assignment_loss",
      Self::DefensiveReconciliation => "defensive_reconciliation",
      Self::Shutdown => "shutdown",
    }
  }
}

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

fn next_acquisition_retry_at(
  pending_acquisitions: &HashMap<PartitionKey, LeaseRetrySchedule>,
  in_flight_acquisitions: &HashSet<PartitionKey>,
) -> Option<OffsetDateTime> {
  pending_acquisitions
    .iter()
    .filter(|(partition, _)| !in_flight_acquisitions.contains(*partition))
    .map(|(_, retry)| retry.next_retry_at)
    .min()
}

impl WriteEngineImpl {
  pub(in crate::write) fn owned_virtual_partitions(
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
      // This task is the single coordinator for assignment state. It never awaits lease-store I/O
      // directly: membership changes, remote acquisition completions, and release completions
      // must all remain independently pollable.
      //
      // If the watch sender closes, we continue on ticker-only cadence so lease maintenance keeps
      // running instead of silently stalling.
      let mut membership_updates_open = true;
      // Set when a membership snapshot or release completion changes the effective admission set.
      let mut assignment_refresh_needed = true;
      // Maintenance is a periodic reconciliation pass. Completion of a fast lease call must not
      // immediately launch another pass, or a ready future can starve membership processing.
      let mut maintenance_refresh_needed = true;
      let mut assignment_activated = false;
      // Desired ownership is the newest deterministic membership plan. `owned` below is the
      // stricter effective admission set after excluding partitions with a release in progress.
      let mut previously_desired: HashSet<PartitionKey> = HashSet::new();
      let mut pending_acquisitions: HashMap<(Chars, VirtualPartitionId), LeaseRetrySchedule> =
        HashMap::new();
      // Only one maintenance transition may exist for a partition. Its future owns the
      // transition until its result is applied, which also gives release drains a reliable
      // `allocation_in_flight` dependency to wait on after a membership loss.
      let mut in_flight_acquisitions = HashSet::new();
      // A partition remains out of effective admission until this set no longer contains it.
      // This matters when a newer membership snapshot returns the partition while an old
      // conditional release request is already in flight.
      let mut pending_releases: HashSet<(Chars, VirtualPartitionId)> = HashSet::new();
      // Release tasks include drain, lease heartbeat, and conditional-release retry. They are
      // retained through a terminal result because cancelling a remote conditional mutation does
      // not prove that DynamoDB did not apply it.
      let mut release_supervisor =
        FuturesUnordered::<BoxFuture<'static, (Chars, VirtualPartitionId)>>::new();
      // Acquisition tasks may wait behind a foreground transition or perform remote I/O. Keeping
      // them here prevents either wait from delaying membership publication.
      let mut acquisition_supervisor =
        FuturesUnordered::<BoxFuture<'static, MaintenanceAcquisitionCompletion>>::new();
      let mut owned = Vec::new();

      loop {
        // Failed acquisitions retry on logical time. A missing retry becomes a permanently
        // pending future, allowing the select below to express one uniform wake-up source.
        let next_acquisition_retry =
          next_acquisition_retry_at(&pending_acquisitions, &in_flight_acquisitions);
        let acquisition_retry_sleep = async {
          let Some(next_retry_at) = next_acquisition_retry else {
            std::future::pending().await
          };
          let delay = (next_retry_at - time_provider.now()).max(time::Duration::ZERO);
          time_provider.sleep(delay).await;
        };
        tokio::pin!(acquisition_retry_sleep);

        // Each turn consumes exactly one ready source, then applies any membership snapshot
        // before installing a completed acquisition. That ordering is the local linearization
        // point between an external membership change and an external lease response.
        let mut membership_changed = false;
        let mut completed_release = None;
        let mut completed_acquisition = None;
        if membership_updates_open {
          tokio::select! {
            _ = heartbeat_ticker.tick() => maintenance_refresh_needed = true,
            () = &mut acquisition_retry_sleep => maintenance_refresh_needed = true,
            completed = release_supervisor.next(),
            if !release_supervisor.is_empty() => completed_release = completed,
            completed = acquisition_supervisor.next(),
            if !acquisition_supervisor.is_empty() => completed_acquisition = completed,
            changed = membership_rx.changed() => {
              membership_changed = true;
              if changed.is_err() {
                membership_updates_open = false;
              }
            }
            () = shutdown.cancelled() => break,
          };
        } else {
          tokio::select! {
            _ = heartbeat_ticker.tick() => maintenance_refresh_needed = true,
            () = &mut acquisition_retry_sleep => maintenance_refresh_needed = true,
            completed = release_supervisor.next(),
            if !release_supervisor.is_empty() => completed_release = completed,
            completed = acquisition_supervisor.next(),
            if !acquisition_supervisor.is_empty() => completed_acquisition = completed,
            () = shutdown.cancelled() => break,
          };
        }
        if let Some(partition) = completed_release {
          // A terminal handoff may make a returned desired partition admissible again.
          pending_releases.remove(&partition);
          assignment_refresh_needed = true;
        }

        // A lease future and membership update may become ready in the same scheduler turn.
        // Publish the update first whenever it is already waiting so the allocation transition
        // applies its result only if its captured authorization generation is still current.
        if !membership_changed && membership_updates_open {
          match membership_rx.has_changed() {
            Ok(true) => {
              membership_changed = true;
              if membership_rx.changed().await.is_err() {
                membership_updates_open = false;
              }
            },
            Ok(false) => {},
            Err(_) => membership_updates_open = false,
          }
        }

        let membership = membership_rx.borrow().clone();

        if assignment_refresh_needed || membership_changed {
          // Kubernetes does not publish this pod to Endpoints until it is ready. Lease acquisition
          // waits for an initialized snapshot that includes this broker, but every snapshot still
          // replaces the foreground admission set: pending, empty, and foreign membership must
          // fence a stale local assignment before asynchronous draining begins.
          let self_is_member = membership
            .nodes()
            .is_some_and(|nodes| nodes.iter().any(|node| node.node_id.as_str() == holder_id));
          if !assignment_activated && self_is_member {
            assignment_activated = true;
            info!(
              "broker lease assignment activated: holder_id={holder_id}, membership_nodes={:?}",
              membership.nodes().unwrap_or_default(),
            );
          }

          // A previously activated broker immediately releases its ownership when it disappears
          // from a later snapshot. Before first activation this is simply the empty admission set.
          let next_desired_owned = if assignment_activated && self_is_member {
            Self::owned_virtual_partitions(&topics, writer_id, &holder_id, &membership)
          } else {
            Vec::new()
          };
          let currently_desired: HashSet<(Chars, VirtualPartitionId)> =
            next_desired_owned.iter().cloned().collect();
          let tracked_partitions = state.lock().partition_keys();
          pending_acquisitions.retain(|partition, _| currently_desired.contains(partition));

          let gained_partitions = sorted_partition_delta(&currently_desired, &previously_desired);
          let lost_partitions = sorted_partition_delta(&previously_desired, &currently_desired);
          if !gained_partitions.is_empty() || !lost_partitions.is_empty() {
            info!(
              "broker partition assignment changed: holder_id={holder_id}, membership_nodes={:?}, \
               assigned_partitions={}, gained_partitions={gained_partitions:?}, \
               lost_partitions={lost_partitions:?}",
              membership.nodes().unwrap_or_default(),
              currently_desired.len(),
            );
          }

          // Reconcile both calculated ownership loss and locally tracked state. The latter catches
          // leases created by old versions of the inline path, which were never in the assignment
          // delta and would otherwise survive until TTL expiry.
          let lost_partitions = lost_partitions
            .into_iter()
            .map(|(topic, virtual_partition_id)| {
              (
                (topic, virtual_partition_id),
                LeaseReleaseReason::AssignmentLoss,
              )
            })
            .collect::<HashMap<_, _>>();
          let mut release_partitions = lost_partitions;
          for partition in tracked_partitions {
            if !currently_desired.contains(&partition) {
              release_partitions
                .entry(partition)
                .or_insert(LeaseReleaseReason::DefensiveReconciliation);
            }
          }
          let mut release_partitions = release_partitions
            .into_iter()
            .map(|((topic, virtual_partition_id), reason)| (topic, virtual_partition_id, reason))
            .collect::<Vec<_>>();
          release_partitions
            .sort_unstable_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));

          for (topic, virtual_partition_id, release_reason) in release_partitions {
            let partition = (topic.clone(), virtual_partition_id);
            if !pending_releases.insert(partition.clone()) {
              continue;
            }
            let lease_store = Arc::clone(&lease_store);
            let state = Arc::clone(&state);
            let flush_notifier = Arc::clone(&flush_notifier);
            let metrics = metrics.clone();
            let holder_id = holder_id.clone();
            let lease_session_id = lease_session_id.clone();
            let time_provider = Arc::clone(&time_provider);
            let lifecycle_hooks = lifecycle_hooks.clone();
            release_supervisor.push(
              async move {
                Self::release_partition_lease(
                  &lease_store,
                  &state,
                  &flush_notifier,
                  &metrics,
                  &holder_id,
                  &lease_session_id,
                  &topic,
                  virtual_partition_id,
                  release_reason,
                  time_provider.as_ref(),
                  lease_duration,
                  heartbeat_interval,
                  lifecycle_hooks.as_ref(),
                )
                .await;
                partition
              }
              .boxed(),
            );
          }

          // A returned partition must complete its prior handoff before it can be admitted
          // again. This keeps every old drain/revocation alive while the latest membership is
          // still consumed promptly for unrelated partitions.
          owned = next_desired_owned
            .iter()
            .filter(|partition| !pending_releases.contains(partition))
            .cloned()
            .collect();
          previously_desired = currently_desired;
          assignment_refresh_needed = false;
          maintenance_refresh_needed = true;
          {
            let mut guard = state.lock();
            guard.membership = membership.clone();
            guard.publish_assignment(&owned);
          }
          if let Some(lifecycle_hooks) = lifecycle_hooks.as_ref() {
            lifecycle_hooks.assignment_published().await;
          }
        }

        if let Some((partition, completion)) = completed_acquisition {
          // Membership was refreshed above when already available. `finish_*` repeats the
          // generation check under WriteState's lock, so a response racing a later publication
          // can clear its in-flight marker but cannot install stale lease or reservation state.
          in_flight_acquisitions.remove(&partition);
          let Some((transition, outcome)) = completion else {
            continue;
          };
          let (topic, virtual_partition_id) = &partition;
          match outcome {
            Ok(LeaseAcquireAndReserveOutcome::Acquired { lease, reservation }) => {
              // A stale successful remote acquisition is still useful to record in metrics. Its
              // allocation transition refuses to install local authority and wakes the release
              // task scheduled by the newer membership snapshot to conditionally revoke it.
              if let Some(reservation) = reservation.as_ref() {
                metrics.record_sequence_reservation(reservation);
                debug!(
                  "broker sequence reservation coalesced with lease maintenance: topic={topic}, \
                   virtual_partition_id={virtual_partition_id}, reason={:?}, start={}, end={}",
                  transition.reservation.map(|request| request.reason),
                  reservation.start,
                  reservation.end,
                );
              }
              let lease_expiration_update =
                LeaseExpirationUpdate::Set(Some(lease.lease_expiration_at));
              let lease_for_log = lease.clone();
              let finish = transition.transition.finish_lease_maintenance(
                lease_expiration_update,
                Some(lease),
                reservation,
                transition
                  .records_allocated_since_last_maintenance
                  .unwrap_or_default(),
              );
              {
                let mut state = state.lock();
                if finish == AllocationTransitionFinish::Applied
                  && let Some(partition_state) =
                    state.partition_state_mut_if_present(topic, *virtual_partition_id)
                {
                  partition_state.draining = false;
                }
              }
              if finish == AllocationTransitionFinish::Applied {
                pending_acquisitions.remove(&partition);
              }
              if finish == AllocationTransitionFinish::Applied && transition.lease_was_expired {
                Self::log_lease_acquired(
                  &holder_id,
                  &lease_session_id,
                  topic,
                  *virtual_partition_id,
                  &lease_for_log,
                  LeaseAcquisitionOrigin::AssignmentMaintenance,
                  transition.assignment_generation,
                );
              }
            },
            Ok(LeaseAcquireAndReserveOutcome::HeldByOther(_)) => {
              let finish =
                transition
                  .transition
                  .finish(LeaseExpirationUpdate::Set(None), None, None);
              if finish == AllocationTransitionFinish::Applied {
                let retry = pending_acquisitions
                  .entry(partition.clone())
                  .or_insert_with(|| LeaseRetrySchedule::new(time_provider.now()));
                let scheduled_retry_delay = retry.schedule_retry(time_provider.now());
                debug!(
                  "broker lease acquisition deferred: holder_id={holder_id}, topic={topic}, \
                   virtual_partition_id={virtual_partition_id}, retry_at={}, retry_delay_ms={}",
                  retry.next_retry_at,
                  scheduled_retry_delay.whole_milliseconds(),
                );
              }
            },
            Err(error) => {
              let reservation_requested = transition.reservation.is_some();
              let finish =
                transition
                  .transition
                  .finish(LeaseExpirationUpdate::Preserve, None, None);
              if finish == AllocationTransitionFinish::Applied {
                pending_acquisitions.remove(&partition);
              }
              if reservation_requested {
                metrics.sequence_reservation_failures_total.inc();
              }
              warn_every!(
                15.seconds(),
                "lease self-assignment acquire/reserve failed: topic={topic}, \
                 virtual_partition_id={virtual_partition_id}, error={error:#}",
              );
            },
          }
        }

        // Do not start another maintenance operation under a membership snapshot that has
        // already arrived. The next select consumes it without waiting on any remote I/O.
        if membership_updates_open && membership_rx.has_changed().unwrap_or(false) {
          continue;
        }

        if !maintenance_refresh_needed {
          continue;
        }
        maintenance_refresh_needed = false;

        for (topic, virtual_partition_id) in &owned {
          let topic = topic.clone();
          let virtual_partition_id = *virtual_partition_id;
          let partition = (topic.clone(), virtual_partition_id);
          if in_flight_acquisitions.contains(&partition)
            || pending_acquisitions
              .get(&partition)
              .is_some_and(|retry| retry.next_retry_at > time_provider.now())
          {
            continue;
          }
          in_flight_acquisitions.insert(partition.clone());
          let state = Arc::clone(&state);
          let lease_store = Arc::clone(&lease_store);
          let holder_id = holder_id.clone();
          let lease_session_id = lease_session_id.clone();
          let time_provider = Arc::clone(&time_provider);
          let metrics = metrics.clone();
          acquisition_supervisor.push(
            async move {
              // A foreground allocation can own the transition first. Waiting happens inside the
              // supervisor rather than this coordinator so membership changes remain responsive.
              let transition = loop {
                match begin_allocation_transition(
                  &state,
                  topic.as_str(),
                  virtual_partition_id,
                  1,
                  time_provider.now(),
                  true,
                  base_reservation_size,
                ) {
                  AllocationTransitionDecision::NotAssigned
                  | AllocationTransitionDecision::Draining => return (partition, None),
                  AllocationTransitionDecision::Ready => {
                    unreachable!("lease renewal always needs an allocation transition")
                  },
                  AllocationTransitionDecision::Waiting(notified) => notified.await,
                  AllocationTransitionDecision::Claimed(transition) => break transition,
                }
              };
              let key = ProducerPartitionLeaseKey {
                topic: topic.clone(),
                virtual_partition_id,
              };
              let outcome = acquire_lease_and_reserve_sequences(
                &lease_store,
                &holder_id,
                &lease_session_id,
                key,
                time_provider.now(),
                lease_duration,
                transition.reservation.map(|request| request.size),
                &metrics,
              )
              .await;
              (partition, Some((transition, outcome)))
            }
            .boxed(),
          );
        }
      }

      // Stop admitting immediately, then retain all existing handoffs and add any remaining
      // locally tracked partition to the concurrent shutdown release set.
      state.lock().publish_assignment(&[]);
      let membership = membership_rx.borrow().clone();
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
      for (topic, virtual_partition_id) in partitions {
        let partition = (topic.clone(), virtual_partition_id);
        if !pending_releases.insert(partition.clone()) {
          continue;
        }
        let lease_store = Arc::clone(&lease_store);
        let state = Arc::clone(&state);
        let flush_notifier = Arc::clone(&flush_notifier);
        let metrics = metrics.clone();
        let holder_id = holder_id.clone();
        let lease_session_id = lease_session_id.clone();
        let time_provider = Arc::clone(&time_provider);
        let lifecycle_hooks = lifecycle_hooks.clone();
        release_supervisor.push(
          async move {
            Self::release_partition_lease(
              &lease_store,
              &state,
              &flush_notifier,
              &metrics,
              &holder_id,
              &lease_session_id,
              &topic,
              virtual_partition_id,
              LeaseReleaseReason::Shutdown,
              time_provider.as_ref(),
              lease_duration,
              heartbeat_interval,
              lifecycle_hooks.as_ref(),
            )
            .await;
            partition
          }
          .boxed(),
        );
      }
      // The release drain waits for outstanding allocation transitions. Keep polling completed
      // acquisition futures during shutdown so their transitions clear that state and wake the
      // corresponding drain; dropping either supervisor would make remote mutation ordering
      // unknowable.
      while !release_supervisor.is_empty() || !acquisition_supervisor.is_empty() {
        tokio::select! {
          _ = release_supervisor.next(), if !release_supervisor.is_empty() => {},
          _ = acquisition_supervisor.next(), if !acquisition_supervisor.is_empty() => {},
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
    release_reason: LeaseReleaseReason,
    time_provider: &dyn TimeProvider,
    lease_duration: time::Duration,
    heartbeat_interval: time::Duration,
    lifecycle_hooks: Option<&Arc<dyn BrokerLifecycleHooks>>,
  ) {
    // Draining is performed once for this handoff. Only the conditional release itself retries;
    // repeating the drain would duplicate lifecycle hooks and make one handoff look like several.
    let key = ProducerPartitionLeaseKey {
      topic: topic.clone(),
      virtual_partition_id,
    };
    let next_heartbeat_at = {
      let mut state = state.lock();
      let Some(partition_state) = state.partition_state_mut_if_present(topic, virtual_partition_id)
      else {
        return;
      };
      partition_state.draining = true;
      partition_state.lease_expiration_at.map_or_else(
        || time_provider.now(),
        |expiration| expiration - heartbeat_interval,
      )
    };
    metrics.lease_drain_starts_total.inc();
    info!(
      "broker partition drain started: release_reason={}, holder_id={holder_id}, topic={topic}, \
       virtual_partition_id={virtual_partition_id}",
      release_reason.as_str()
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
      "broker partition drain complete: release_reason={}, holder_id={holder_id}, topic={topic}, \
       virtual_partition_id={virtual_partition_id}",
      release_reason.as_str()
    );
    if let Some(lifecycle_hooks) = lifecycle_hooks {
      lifecycle_hooks
        .partition_drained(topic.as_str(), virtual_partition_id)
        .await;
      lifecycle_hooks
        .before_lease_release(topic.as_str(), virtual_partition_id)
        .await;
    }

    // A release error leaves the remote mutation outcome unknown. Retain the task and retry the
    // same conditional operation until the store reports Released, Expired, or HeldByOther.
    let mut release_retry = LeaseRetrySchedule::new(time_provider.now());
    loop {
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
          // in-memory lease data after ownership moved away. Retire terminally released unassigned
          // state so defensive reconciliation does not perform a release on every heartbeat.
          {
            let mut state = state.lock();
            state.clear_terminally_released_partition(topic, virtual_partition_id);
          }
          if let Some(lifecycle_hooks) = lifecycle_hooks {
            lifecycle_hooks
              .lease_released(topic.as_str(), virtual_partition_id)
              .await;
          }
          info!(
            "broker partition lease released: release_reason={}, holder_id={holder_id}, \
             topic={topic}, virtual_partition_id={virtual_partition_id}",
            release_reason.as_str(),
          );
          return;
        },
        Err(error) => {
          let retry_delay = release_retry.schedule_retry(time_provider.now());
          warn_every!(
            15.seconds(),
            "lease self-assignment release failed: topic={topic}, \
             virtual_partition_id={virtual_partition_id}, retry_delay_ms={}, error={error:#}",
            retry_delay.whole_milliseconds(),
          );
          time_provider.sleep(retry_delay).await;
        },
      }
    }
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
    // Continue heartbeating while accepted writes drain. If the lease is lost, draining still
    // waits for local work so state can be retired safely, but no further heartbeat is attempted.
    let mut lease_held = true;
    let mut heartbeat_retry = LeaseRetrySchedule::new(next_heartbeat_at);
    loop {
      let notified = {
        let state = state.lock();
        state
          .partition_state(key.topic.as_str(), key.virtual_partition_id)
          .map(|partition_state| partition_state.drain_notify.clone().notified_owned())
      };
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
