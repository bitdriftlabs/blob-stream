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
use super::super::{TopicInfo, WriteEngineImpl};
use super::acquire_lease_and_reserve_sequences;
use bd_log::warn_every;
use bd_time::OffsetDateTimeExt;
use blob_stream_broker_discovery::{
  BrokerMembership,
  balanced_assignment,
  writer_virtual_partitions,
};
use blob_stream_metadata_store::{
  LeaseAcquireAndReserveOutcome,
  LeaseReleaseOutcome,
  ProducerPartitionLeaseKey,
};
use blob_stream_types::VirtualPartitionId;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{debug, info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::ext::NumericalDuration;
use tokio::sync::watch;

impl WriteEngineImpl {
  pub(super) fn owned_virtual_partitions(
    topics: &HashMap<String, TopicInfo>,
    writer_id: u32,
    holder_id: &str,
    membership: &BrokerMembership,
  ) -> Vec<(String, VirtualPartitionId)> {
    let partitions = writer_virtual_partitions(
      topics
        .values()
        .map(|topic| (topic.name.clone(), topic.partition_count, topic.num_writers)),
      writer_id,
    );

    let mut owned: Vec<(String, VirtualPartitionId)> = balanced_assignment(partitions, membership)
      .into_iter()
      .filter_map(|(partition, owner)| {
        (owner.node_id == holder_id).then_some((partition.topic, partition.virtual_partition_id))
      })
      .collect();

    owned.sort_unstable();
    owned
  }

  pub(in crate::write) fn spawn_lease_self_assignment_loop(
    &self,
    mut membership_rx: watch::Receiver<BrokerMembership>,
  ) {
    let interval_ms = (self.config.lease_duration_ms / 3)
      .max(1_000)
      .cast_unsigned();
    let interval = StdDuration::from_millis(interval_ms);
    let topics = self.topics.clone();
    let holder_id = self.holder_id.clone();
    let writer_id = self.config.writer_id;
    let lease_duration_ms = self.config.lease_duration_ms;
    let base_reservation_size = self.config.reservation_size;
    let lease_store = Arc::clone(&self.lease_store);
    let state = Arc::clone(&self.state);
    let time_provider = Arc::clone(&self.time_provider);
    let metrics = self.metrics.clone();
    let flush_notifier = Arc::clone(&self.flush_notifier);
    let mut shutdown = self.shutdown_trigger_handle.make_shutdown();

    tokio::spawn(async move {
      let mut ticker = tokio::time::interval(interval);
      // If the watch sender closes, we continue on ticker-only cadence so lease maintenance keeps
      // running instead of silently stalling.
      let mut membership_updates_open = true;
      let mut assignment_activated = false;
      let mut previously_assigned: HashSet<(String, VirtualPartitionId)> = HashSet::new();

      loop {
        let shutting_down = if membership_updates_open {
          tokio::select! {
            _ = ticker.tick() => false,
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
            _ = ticker.tick() => false,
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
            partitions,
            time_provider.now().unix_timestamp_ms(),
          )
          .await;
          return;
        }

        // Kubernetes does not publish this pod to Endpoints until it is ready. Waiting here keeps
        // the server available for readiness checks without treating an incomplete snapshot as
        // authoritative ownership.
        if !assignment_activated {
          let Some(nodes) = membership.nodes() else {
            if shutting_down {
              break;
            }
            continue;
          };
          let self_is_member = nodes.iter().any(|node| node.node_id == holder_id);
          if !self_is_member {
            if shutting_down {
              break;
            }
            continue;
          }
          assignment_activated = true;
          info!(
            "broker lease assignment activated: holder_id={holder_id}, membership_nodes={nodes:?}"
          );
        }

        let owned = Self::owned_virtual_partitions(&topics, writer_id, &holder_id, &membership);
        let currently_owned: HashSet<(String, VirtualPartitionId)> =
          owned.iter().cloned().collect();

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
          lost_partitions,
          time_provider.now().unix_timestamp_ms(),
        )
        .await;

        // Record assignment after reconciling releases so the next pass can compute deltas.
        previously_assigned = currently_owned;

        for (topic, virtual_partition_id) in owned {
          let key = ProducerPartitionLeaseKey {
            topic: topic.clone(),
            virtual_partition_id,
          };

          let now = time_provider.now();
          let now_ts_ms = now.unix_timestamp_ms();
          let transition = loop {
            match begin_allocation_transition(
              &state,
              &topic,
              virtual_partition_id,
              1,
              now_ts_ms,
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
            key,
            now_ts_ms,
            lease_duration_ms,
            reservation_request.map(|request| request.size),
            &metrics,
          )
          .await
          {
            Ok(LeaseAcquireAndReserveOutcome::Acquired { lease, reservation }) => {
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
              transition.transition.finish_lease_maintenance(
                LeaseExpirationUpdate::Set(Some(lease.lease_expiration_ts_ms)),
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
              transition
                .transition
                .finish(LeaseExpirationUpdate::Set(None), None);
            },
            Err(error) => {
              if reservation_request.is_some() {
                metrics.sequence_reservation_failures_total.inc();
              }
              warn_every!(
                15.seconds(),
                "lease self-assignment acquire/reserve failed: topic={topic}, \
                 virtual_partition_id={virtual_partition_id}, requested_size={:?}, error={error}",
                reservation_request.map(|request| request.size),
              );
              transition
                .transition
                .finish(LeaseExpirationUpdate::Preserve, None);
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
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
    now_ts_ms: i64,
  ) {
    let key = ProducerPartitionLeaseKey {
      topic: topic.to_string(),
      virtual_partition_id,
    };

    {
      let mut state = state.lock();
      let partition_state = state.partition_state_mut(topic, virtual_partition_id);
      partition_state.draining = true;
    }
    metrics.lease_drain_starts_total.inc();
    info!(
      "broker partition drain started: holder_id={holder_id}, topic={topic}, \
       virtual_partition_id={virtual_partition_id}"
    );
    flush_notifier.notify_one();

    // TODO(mattklein123): Renew the producer lease while waiting for a drain that can approach
    // the lease duration. The default 30-second lease makes this unlikely in normal operation,
    // but a slow blob upload can otherwise let a successor acquire before this drain completes.
    Self::wait_for_partition_drain(state, topic, virtual_partition_id).await;
    metrics.lease_drain_completions_total.inc();
    info!(
      "broker partition drain complete: holder_id={holder_id}, topic={topic}, \
       virtual_partition_id={virtual_partition_id}"
    );

    match lease_store.release_lease(&key, holder_id, now_ts_ms).await {
      Ok(
        LeaseReleaseOutcome::Released
        | LeaseReleaseOutcome::Expired
        | LeaseReleaseOutcome::HeldByOther(_),
      ) => {
        // Clear local lease/allocator state immediately to avoid accepting writes based on stale
        // in-memory lease data after ownership moved away.
        let mut state = state.lock();
        if let Some(partition_state) =
          state.partition_state_mut_if_present(topic, virtual_partition_id)
        {
          partition_state.lease_expiration_ts_ms = None;
          partition_state.reset_sequence_allocation();
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
    partitions: Vec<(String, VirtualPartitionId)>,
    now_ts_ms: i64,
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
          &topic,
          virtual_partition_id,
          now_ts_ms,
        )
        .await;
      });
    }

    while releases.next().await.is_some() {}
  }

  async fn wait_for_partition_drain(
    state: &Arc<parking_lot::Mutex<WriteState>>,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) {
    loop {
      let notified = {
        let state = state.lock();
        let Some(partition_state) = state.partition_state(topic, virtual_partition_id) else {
          return;
        };
        if partition_state.is_drained() {
          return;
        }
        partition_state.drain_notify.clone().notified_owned()
      };
      notified.await;
    }
  }
}

fn owned_partitions_for_shutdown(
  state: &Arc<parking_lot::Mutex<WriteState>>,
) -> HashSet<(String, VirtualPartitionId)> {
  state.lock().partition_keys().into_iter().collect()
}

fn sorted_partition_delta(
  left: &HashSet<(String, VirtualPartitionId)>,
  right: &HashSet<(String, VirtualPartitionId)>,
) -> Vec<(String, VirtualPartitionId)> {
  let mut partitions: Vec<_> = left.difference(right).cloned().collect();
  partitions.sort_unstable();
  partitions
}
