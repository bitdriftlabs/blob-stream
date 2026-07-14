#[cfg(test)]
#[path = "./lease_assignment_test.rs"]
mod tests;

use super::{PartitionLocks, TopicInfo, WriteEngineImpl, lock_partition};
use bd_log::warn_every;
use bd_time::OffsetDateTimeExt;
use blob_stream_broker_discovery::{
  BrokerMembership,
  balanced_assignment,
  writer_virtual_partitions,
};
use blob_stream_metadata_store::{
  LeaseAcquireOutcome,
  LeaseReleaseOutcome,
  ProducerPartitionLeaseKey,
  SequenceReservationOutcome,
};
use blob_stream_types::VirtualPartitionId;
use log::info;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::ext::NumericalDuration;
use tokio::sync::{oneshot, watch};

impl WriteEngineImpl {
  pub(super) fn owned_virtual_partitions(
    topics: &HashMap<String, TopicInfo>,
    writer_id: u32,
    holder_id: &str,
    membership: &BrokerMembership,
  ) -> Vec<(String, VirtualPartitionId)> {
    // Empty membership means discovery is unavailable or intentionally static; in that case we
    // self-assign all partitions so writes can continue behind lease fencing.
    let static_self = membership.nodes.is_empty();
    let partitions = writer_virtual_partitions(
      topics
        .values()
        .map(|topic| (topic.name.clone(), topic.partition_count, topic.num_writers)),
      writer_id,
    );

    let mut owned: Vec<(String, VirtualPartitionId)> = if static_self {
      partitions
        .into_iter()
        .map(|partition| (partition.topic, partition.virtual_partition_id))
        .collect()
    } else {
      balanced_assignment(partitions, membership)
        .into_iter()
        .filter_map(|(partition, owner)| {
          (owner.node_id == holder_id).then_some((partition.topic, partition.virtual_partition_id))
        })
        .collect()
    };

    owned.sort_unstable();
    owned
  }

  pub(super) fn spawn_lease_self_assignment_loop(
    &self,
    mut membership_rx: watch::Receiver<BrokerMembership>,
  ) -> oneshot::Sender<()> {
    let interval_ms = (self.config.lease_duration_ms / 3)
      .max(1_000)
      .cast_unsigned();
    let interval = StdDuration::from_millis(interval_ms);
    let topics = self.topics.clone();
    let holder_id = self.holder_id.clone();
    let writer_id = self.config.writer_id;
    let lease_duration_ms = self.config.lease_duration_ms;
    let reservation_size = self.config.reservation_size;
    let lease_store = Arc::clone(&self.lease_store);
    let state = Arc::clone(&self.state);
    let time_provider = Arc::clone(&self.time_provider);
    let metrics = self.metrics.clone();
    let partition_locks = Arc::clone(&self.partition_locks);
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
      let mut ticker = tokio::time::interval(interval);
      // If the watch sender closes, we continue on ticker-only cadence so lease maintenance keeps
      // running instead of silently stalling.
      let mut membership_updates_open = true;
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
            _ = &mut shutdown_rx => true,
          }
        } else {
          tokio::select! {
            _ = ticker.tick() => false,
            _ = &mut shutdown_rx => true,
          }
        };

        let membership = membership_rx.borrow().clone();
        {
          let mut guard = state.lock().await;
          guard.membership = membership.clone();
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
            membership.nodes,
            currently_owned.len(),
          );
        }

        // On membership changes (scale up/down), we release any partition that moved away from
        // this broker instead of waiting for lease TTL expiration. This shortens convergence and
        // reduces transient NOT_LEASE_HOLDER retries for producers during rebalance.
        for (topic, virtual_partition_id) in &lost_partitions {
          Self::release_partition_lease(
            &lease_store,
            &state,
            &partition_locks,
            &holder_id,
            topic,
            *virtual_partition_id,
            time_provider.now().unix_timestamp_ms(),
          )
          .await;
        }

        // During shutdown we release all currently owned partitions so another broker can acquire
        // immediately. If release fails, normal lease expiry still guarantees eventual progress.
        if shutting_down {
          info!(
            "broker releasing assigned leases for shutdown: holder_id={holder_id}, \
             partitions={owned:?}"
          );
          for (topic, virtual_partition_id) in owned {
            Self::release_partition_lease(
              &lease_store,
              &state,
              &partition_locks,
              &holder_id,
              &topic,
              virtual_partition_id,
              time_provider.now().unix_timestamp_ms(),
            )
            .await;
          }
          break;
        }

        // Record assignment after reconciling releases so the next pass can compute deltas.
        previously_assigned = currently_owned;

        for (topic, virtual_partition_id) in owned {
          let _partition_guard =
            lock_partition(&partition_locks, &topic, virtual_partition_id).await;
          let key = ProducerPartitionLeaseKey {
            topic: topic.clone(),
            virtual_partition_id,
          };

          let now = time_provider.now();
          let now_ts_ms = now.unix_timestamp_ms();

          let acquired = match lease_store
            .acquire_lease(key.clone(), holder_id.clone(), now_ts_ms, lease_duration_ms)
            .await
          {
            Ok(LeaseAcquireOutcome::Acquired(lease)) => {
              let mut guard = state.lock().await;
              let partition_state = guard.partition_state_mut(&topic, virtual_partition_id);
              partition_state.lease_expiration_ts_ms = Some(lease.lease_expiration_ts_ms);
              true
            },
            Ok(LeaseAcquireOutcome::HeldByOther(_)) => {
              let mut guard = state.lock().await;
              let partition_state = guard.partition_state_mut(&topic, virtual_partition_id);
              partition_state.lease_expiration_ts_ms = None;
              false
            },
            Err(error) => {
              warn_every!(
                15.seconds(),
                "lease self-assignment acquire failed: {error}"
              );
              false
            },
          };

          if !acquired {
            continue;
          }

          let needs_reservation = {
            let mut guard = state.lock().await;
            let partition_state = guard.partition_state_mut(&topic, virtual_partition_id);
            // Background maintenance only tops up when we cannot allocate even a single record.
            // This keeps steady-state churn low while ensuring foreground writes usually find
            // capacity already available.
            !partition_state.seq_allocator.can_allocate(1)
          };

          if !needs_reservation {
            continue;
          }

          let reservation_started = std::time::Instant::now();
          let reservation_outcome = lease_store
            .reserve_sequences(&key, &holder_id, now_ts_ms, reservation_size)
            .await;
          metrics
            .sequence_reservation_latency_seconds
            .observe(reservation_started.elapsed().as_secs_f64());
          match reservation_outcome {
            Ok(SequenceReservationOutcome::Reserved(reservation)) => {
              metrics.record_sequence_reservation(&reservation.range);
              let mut guard = state.lock().await;
              let partition_state = guard.partition_state_mut(&topic, virtual_partition_id);
              partition_state
                .seq_allocator
                .set_reservation(reservation.range);
            },
            Ok(
              SequenceReservationOutcome::HeldByOther(_) | SequenceReservationOutcome::Expired,
            ) => {
              metrics.sequence_reservation_failures_total.inc();
              // If reservation cannot be extended here, we do not fail the loop. Foreground
              // writes will attempt lease+reservation again in `produce_batch`; if ownership has
              // moved, those writes return NOT_LEASE_HOLDER and producers re-route.
            },
            Err(error) => {
              metrics.sequence_reservation_failures_total.inc();
              warn_every!(
                15.seconds(),
                "lease self-assignment reserve failed: {error}"
              );
            },
          }
        }
      }
    });

    shutdown_tx
  }

  async fn release_partition_lease(
    lease_store: &Arc<dyn blob_stream_metadata_store::ProducerPartitionLeaseStore>,
    state: &Arc<tokio::sync::Mutex<super::WriteState>>,
    partition_locks: &PartitionLocks,
    holder_id: &str,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
    now_ts_ms: i64,
  ) {
    let _partition_guard = lock_partition(partition_locks, topic, virtual_partition_id).await;
    let key = ProducerPartitionLeaseKey {
      topic: topic.to_string(),
      virtual_partition_id,
    };

    match lease_store.release_lease(&key, holder_id, now_ts_ms).await {
      Ok(LeaseReleaseOutcome::Released | LeaseReleaseOutcome::Expired) => {
        let mut guard = state.lock().await;
        let partition_state = guard.partition_state_mut(topic, virtual_partition_id);
        // Clear local lease/allocator state immediately to avoid accepting writes based on stale
        // in-memory lease data after ownership moved away.
        partition_state.lease_expiration_ts_ms = None;
        partition_state.seq_allocator = super::SeqAllocator::default();
      },
      Ok(LeaseReleaseOutcome::HeldByOther(_)) => {
        let mut guard = state.lock().await;
        let partition_state = guard.partition_state_mut(topic, virtual_partition_id);
        partition_state.lease_expiration_ts_ms = None;
        partition_state.seq_allocator = super::SeqAllocator::default();
      },
      Err(error) => {
        warn_every!(
          15.seconds(),
          "lease self-assignment release failed: {error}"
        );
      },
    }
  }
}

fn sorted_partition_delta(
  left: &HashSet<(String, VirtualPartitionId)>,
  right: &HashSet<(String, VirtualPartitionId)>,
) -> Vec<(String, VirtualPartitionId)> {
  let mut partitions: Vec<_> = left.difference(right).cloned().collect();
  partitions.sort_unstable();
  partitions
}
