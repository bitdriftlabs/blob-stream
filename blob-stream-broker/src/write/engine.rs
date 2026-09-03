mod core;

use super::api::{
  BrokerLeaseSnapshot,
  BrokerLeaseStatus,
  BrokerNodeSnapshot,
  BrokerPartitionOwnershipSnapshot,
  BrokerPartitionStateSnapshot,
  BrokerStateSnapshot,
  BrokerTopicStateSnapshot,
  DurableCommittedSourceCheckpointSnapshot,
  DurableConsumerLeaseScanSnapshot,
  DurableConsumerLeaseSnapshot,
  DurablePartitionStateSnapshot,
  DurableProducerLeaseObservation,
  DurableProducerLeaseSnapshot,
  DurableStateLookupStatus,
  DurableTopicStateSnapshot,
  SequenceReservationSnapshot,
  WriteEngine,
  WriteError,
  WriteRequest,
  WriteResponse,
};
use crate::write::allocation::{
  AllocationTransitionDecision,
  AllocationTransitionFinish,
  LeaseExpirationUpdate,
  begin_allocation_transition,
};
use crate::write::buffer::{BufferedBatch, FlushCompletionError};
use crate::write::lease::LeaseAcquisitionOrigin;
use crate::write::memory_pressure::MemoryPressureController;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bd_log_util::warn_every;
use blob_stream_broker_discovery::{balanced_assignment, writer_virtual_partitions};
use blob_stream_metadata_store::ProducerPartitionLeaseKey;
use blob_stream_types::RecordBatch;
pub use core::{WriteEngineBuilder, WriteEngineImpl};
use futures::{StreamExt, stream};
use log::trace;
use std::collections::HashMap;
use std::time::Duration as StdDuration;
use time::ext::NumericalDuration;
use tokio::sync::oneshot;

const MAX_CONCURRENT_DURABLE_STATE_LOOKUPS: usize = 8;
const DURABLE_STATE_LOOKUP_TIMEOUT: StdDuration = StdDuration::from_secs(5);

#[async_trait]
impl WriteEngine for WriteEngineImpl {
  async fn produce_batch(&self, request: WriteRequest) -> Result<WriteResponse, WriteError> {
    self.metrics.produce_requests_total.inc();
    let record_count = request.records.len() as u64;

    trace!(
      "broker write request accepted: topic={}, virtual_partition_id={}, records={}",
      request.topic,
      request.virtual_partition_id,
      request.records.len()
    );

    let Some(topic_info) = self.topics.get(request.topic.as_str()) else {
      let error = WriteError::UnknownTopic(request.topic.clone());
      return Err(error);
    };

    if !topic_info.is_valid_partition(request.virtual_partition_id) {
      let error = WriteError::InvalidPartition {
        topic: request.topic.clone(),
        virtual_partition_id: request.virtual_partition_id,
      };
      return Err(error);
    }

    if request.records.is_empty() {
      let error = WriteError::InvalidRequest("record batch is empty".to_string());
      return Err(error);
    }

    let Some(summary) = RecordBatch::summary_from_records(&request.records) else {
      let error = WriteError::Internal(anyhow!("failed to summarize record batch"));
      return Err(error);
    };
    if self.admission.is_overloaded() {
      self.metrics.admission_rejections_total.inc();
      let error = WriteError::Overloaded("broker admission controller is overloaded".to_string());
      return Err(error);
    }
    let topic = request.topic;
    let virtual_partition_id = request.virtual_partition_id;
    let mut records = Some(request.records);
    let mut summary = Some(summary);

    let (completion_rx, seq_range, should_notify_flush) = loop {
      let now = self.time_provider.now();
      let buffered = {
        let mut state = self.state.lock();
        // This is the fast-path counterpart to the transition-level check below. It prevents a
        // stale local lease or reservation from accepting a request after membership moved it.
        if state
          .assignment_generation_for_partition(topic.as_str(), virtual_partition_id)
          .is_none()
        {
          return Err(WriteError::NotLeaseHolder {
            topic: topic.clone(),
            virtual_partition_id,
          });
        }
        let partition_state = state.partition_state_mut(topic.as_str(), virtual_partition_id);
        if partition_state.draining {
          let error = WriteError::NotLeaseHolder {
            topic: topic.clone(),
            virtual_partition_id,
          };
          return Err(error);
        }

        if partition_state.allocation_in_flight
          || partition_state.needs_lease(now)
          || !partition_state.seq_allocator.can_allocate(record_count)
        {
          None
        } else {
          let (completion_tx, completion_rx) = oneshot::channel();
          let Some(seq_range) = partition_state.seq_allocator.allocate(record_count) else {
            let error = WriteError::Overloaded("sequence reservation exhausted".to_string());
            return Err(error);
          };
          partition_state.records_allocated_since_lease_maintenance = partition_state
            .records_allocated_since_lease_maintenance
            .saturating_add(record_count);
          trace!(
            "broker sequence allocation: topic={topic}, \
             virtual_partition_id={virtual_partition_id}, record_count={record_count}, start={}, \
             end={}, remaining_capacity={}",
            seq_range.start,
            seq_range.end,
            partition_state.seq_allocator.remaining_capacity(),
          );
          let flush_config = *self.effective_flush_config.read();
          let was_byte_due = partition_state.buffer.buffered_bytes >= flush_config.max_bytes;
          partition_state.buffer.push(
            BufferedBatch {
              records: records.take().expect("records are buffered only once"),
              summary: summary.take().expect("summary is buffered only once"),
              seq_range: seq_range.clone(),
              acceptance_fence: partition_state.lease_fence.clone(),
              completion: Some(completion_tx),
            },
            now,
          );
          let flush_became_byte_due =
            !was_byte_due && partition_state.buffer.buffered_bytes >= flush_config.max_bytes;
          let should_notify_flush =
            flush_became_byte_due || partition_state.buffer.is_time_due(now, &flush_config);
          Some((completion_rx, seq_range, should_notify_flush))
        }
      };
      if let Some(buffered) = buffered {
        break buffered;
      }

      let decision = begin_allocation_transition(
        &self.state,
        topic.as_str(),
        virtual_partition_id,
        record_count,
        now,
        false,
        self.config.reservation_size,
      );
      match decision {
        AllocationTransitionDecision::NotAssigned | AllocationTransitionDecision::Draining => {
          let error = WriteError::NotLeaseHolder {
            topic: topic.clone(),
            virtual_partition_id,
          };
          return Err(error);
        },
        AllocationTransitionDecision::Ready => {},
        AllocationTransitionDecision::Waiting(notified) => notified.await,
        AllocationTransitionDecision::Claimed(work) => {
          let mut acquired_lease = None;
          // Sequence reservation is conditionally accepted only for the current active lease
          // holder. When acquisition is required, the lease may be absent or expired, so it must
          // complete before reserving sequences; these calls cannot be run in parallel.
          let mut reservation = None;
          match (work.needs_lease, work.reservation) {
            (true, Some(request)) => match self
              .acquire_lease_and_reserve_sequences(
                &topic,
                virtual_partition_id,
                now,
                request.size,
                work.sequence_progress,
              )
              .await
            {
              Ok((lease, range)) => {
                acquired_lease = Some(lease);
                reservation = Some(range);
              },
              Err(error) => {
                let _ = work
                  .transition
                  .finish(LeaseExpirationUpdate::Preserve, None, None);
                return Err(error);
              },
            },
            (true, None) => match self
              .ensure_lease(&topic, virtual_partition_id, now, work.sequence_progress)
              .await
            {
              Ok(lease) => acquired_lease = Some(lease),
              Err(error) => {
                let _ = work
                  .transition
                  .finish(LeaseExpirationUpdate::Preserve, None, None);
                return Err(error);
              },
            },
            (false, Some(request)) => match self
              .reserve_sequences(
                &topic,
                virtual_partition_id,
                now,
                request.size,
                work.sequence_progress,
              )
              .await
            {
              Ok(range) => reservation = Some(range),
              Err(error) => {
                let _ = work
                  .transition
                  .finish(LeaseExpirationUpdate::Preserve, None, None);
                return Err(error);
              },
            },
            (false, None) => {},
          }
          let lease_expiration_update = acquired_lease
            .as_ref()
            .map_or(LeaseExpirationUpdate::Preserve, |lease| {
              LeaseExpirationUpdate::Set(Some(lease.lease_expiration_at))
            });
          let assignment_generation = work.assignment_generation;
          let lease_was_expired = work.lease_was_expired;
          let acquired_lease_for_log = acquired_lease.clone();
          let finish = work
            .transition
            .finish(lease_expiration_update, acquired_lease, reservation);
          if finish == AllocationTransitionFinish::Applied
            && lease_was_expired
            && let Some(lease) = acquired_lease_for_log.as_ref()
          {
            Self::log_lease_acquired(
              &self.holder_id,
              &self.lease_session_id,
              &topic,
              virtual_partition_id,
              lease,
              LeaseAcquisitionOrigin::InlineProduce,
              assignment_generation,
            );
          }
          if finish == AllocationTransitionFinish::StaleAssignment {
            trace!(
              "broker inline lease transition completed after assignment changed: topic={topic}, \
               virtual_partition_id={virtual_partition_id}, \
               assignment_generation={assignment_generation}"
            );
          }
        },
      }
    };

    // Timer ticks handle sub-threshold delay-bound buffers. Notify when this batch crosses the
    // byte threshold or encounters an already-time-due buffer, avoiding a complete planner pass
    // for ordinary sub-threshold batches while preserving the latency bound.
    if should_notify_flush {
      self.flush_notifier.notify_one();
    }

    match completion_rx.await {
      Ok(Ok(())) => {},
      Ok(Err(FlushCompletionError::LeaseFenceLost)) => return Err(WriteError::LeaseFenceLost),
      Ok(Err(FlushCompletionError::Internal)) => {
        return Err(WriteError::Internal(anyhow!("flush failed")));
      },
      Err(_closed) => {
        let write_error = WriteError::Internal(anyhow!(
          "flush completion channel closed before acknowledgment"
        ));
        return Err(write_error);
      },
    }

    Ok(WriteResponse { seq_range })
  }

  fn produce_request_timeout(&self) -> StdDuration {
    self.config.produce_request_timeout()
  }

  async fn state_snapshot(&self) -> BrokerStateSnapshot {
    let generated_at = self.time_provider.now();
    let (membership, mut local_partitions_by_topic) = {
      let state = self.state.lock();
      let mut local_partitions_by_topic = HashMap::new();
      for (topic, topic_state) in &state.topics {
        for (virtual_partition_id, partition_state) in &topic_state.partitions {
          local_partitions_by_topic
            .entry(topic.clone())
            .or_insert_with(Vec::new)
            .push(BrokerPartitionStateSnapshot {
              virtual_partition_id: *virtual_partition_id,
              lease_expires_at: partition_state.lease_expiration_at,
              allocation_in_flight: partition_state.allocation_in_flight,
              allocation_started_at: partition_state.allocation_started_at,
              buffered_batch_count: partition_state.buffer.batches.len(),
              buffered_record_count: partition_state
                .buffer
                .batches
                .iter()
                .map(|batch| batch.records.len())
                .sum(),
              buffered_bytes: partition_state.buffer.buffered_bytes,
              first_buffered_at: partition_state.buffer.first_buffered_at,
              sequence_reservation: partition_state.seq_allocator.reservation.as_ref().map(
                |reservation| SequenceReservationSnapshot {
                  start: reservation.start,
                  end: reservation.end,
                },
              ),
              next_sequence: partition_state.seq_allocator.next_seq,
            });
        }
      }
      (state.membership.clone(), local_partitions_by_topic)
    };
    let mut membership_snapshot = membership
      .nodes()
      .unwrap_or_default()
      .iter()
      .map(|node| BrokerNodeSnapshot {
        node_id: node.node_id.clone(),
        address: node.address.clone(),
      })
      .collect::<Vec<_>>();
    membership_snapshot.sort_by(|left, right| {
      left
        .node_id
        .cmp(&right.node_id)
        .then_with(|| left.address.cmp(&right.address))
    });

    let mut topics = self
      .topics
      .values()
      .map(|topic| {
        let mut local_partitions = local_partitions_by_topic
          .remove(topic.name.as_str())
          .unwrap_or_default();
        local_partitions.sort_by_key(|partition| partition.virtual_partition_id);
        BrokerTopicStateSnapshot {
          name: topic.name.clone(),
          partition_count: topic.partition_count,
          num_writers: topic.num_writers,
          retention: std::time::Duration::try_from(topic.retention)
            .expect("topic retention is validated as positive"),
          local_partitions,
        }
      })
      .collect::<Vec<_>>();
    topics.sort_by(|left, right| left.name.cmp(&right.name));

    let assignment = balanced_assignment(
      writer_virtual_partitions(
        self
          .topics
          .values()
          .map(|topic| (topic.name.clone(), topic.partition_count, topic.num_writers)),
        self.config.writer_id,
      ),
      &membership,
    );
    let mut ownership = Vec::new();
    for (partition, assigned_broker) in assignment {
      let topic = self
        .topics
        .get(&partition.topic)
        .expect("assignment must reference a configured topic");
      let key = ProducerPartitionLeaseKey {
        topic: partition.topic.clone(),
        virtual_partition_id: partition.virtual_partition_id,
      };
      let lease = self.lease_store.get_lease(&key).await;
      let assignment_is_local = assigned_broker.node_id.as_str() == self.holder_id;
      let assigned_broker = BrokerNodeSnapshot {
        node_id: assigned_broker.node_id,
        address: assigned_broker.address,
      };

      let (lease_status, observed_lease) = match lease {
        Ok(Some(lease)) => {
          let is_active = lease.lease_expiration_at > generated_at;
          let holder_address = membership
            .nodes()
            .unwrap_or_default()
            .iter()
            .find(|node| node.node_id.as_str() == lease.fence.holder_id)
            .map(|node| node.address.clone());
          let lease_status = if !is_active {
            BrokerLeaseStatus::UnleasedOrExpired
          } else if lease.fence.holder_id == self.holder_id {
            BrokerLeaseStatus::LocalActive
          } else {
            BrokerLeaseStatus::RemoteActive
          };
          (
            lease_status,
            Some(BrokerLeaseSnapshot {
              holder_id: lease.fence.holder_id,
              holder_address,
              expires_at: lease.lease_expiration_at,
              is_active,
            }),
          )
        },
        Ok(None) => {
          let status = if assignment_is_local {
            BrokerLeaseStatus::AssignedLocalPending
          } else {
            BrokerLeaseStatus::AssignedRemotePending
          };
          (status, None)
        },
        Err(error) => {
          warn_every!(
            15.seconds(),
            "broker state lease lookup failed: topic={}, virtual_partition_id={}, error={error}",
            partition.topic,
            partition.virtual_partition_id,
          );
          (BrokerLeaseStatus::LookupFailed, None)
        },
      };

      ownership.push(BrokerPartitionOwnershipSnapshot {
        topic: partition.topic,
        virtual_partition_id: partition.virtual_partition_id,
        producer_writer_id: self.config.writer_id,
        logical_partition_id: partition.virtual_partition_id % topic.partition_count,
        assigned_broker: Some(assigned_broker),
        assignment_is_local,
        lease_status,
        observed_lease,
      });
    }
    ownership.sort_by(|left, right| {
      (&left.topic, left.virtual_partition_id).cmp(&(&right.topic, right.virtual_partition_id))
    });

    // The durable view intentionally reads after local state is copied so slow Dynamo calls do
    // not block producers. Each observation reports failure separately from an absent lease.
    let configured_topics = self
      .topics
      .values()
      .map(|topic| topic.name.to_string())
      .collect::<Vec<_>>();
    let (durable_consumer_lease_scan, consumer_leases_by_partition) =
      if let Some(consumer_lease_store) = &self.consumer_lease_store {
        match tokio::time::timeout(
          DURABLE_STATE_LOOKUP_TIMEOUT,
          consumer_lease_store.list_active_leases(&configured_topics, generated_at),
        )
        .await
        {
          Ok(Ok(leases)) => {
            let mut consumer_leases_by_partition = HashMap::new();
            for lease in leases {
              let committed_source_checkpoint =
                lease.committed_cursor.as_ref().and_then(|cursor| {
                  cursor.source_checkpoint.as_ref().map(|checkpoint| {
                    DurableCommittedSourceCheckpointSnapshot {
                      window_start_unix_seconds: checkpoint.window_start_unix_seconds,
                      snowflake_id: checkpoint.snowflake_id,
                    }
                  })
                });
              let consumer_lease = DurableConsumerLeaseSnapshot {
                group_id: lease.key.group_id,
                owner_id: lease.owner_id,
                generation: lease.generation,
                lease_expiration_ts_ms: lease.lease_expiration_ts_ms,
                last_heartbeat_ts_ms: lease.last_heartbeat_ts_ms,
                committed_seq_end: lease.committed_cursor.as_ref().map(|cursor| cursor.seq_end),
                committed_ts_ms: lease.committed_ts_ms,
                committed_source_checkpoint,
              };
              consumer_leases_by_partition
                .entry((lease.key.topic, lease.key.virtual_partition_id))
                .or_insert_with(Vec::new)
                .push(consumer_lease);
            }
            (
              DurableConsumerLeaseScanSnapshot {
                status: DurableStateLookupStatus::Present,
                error: None,
              },
              consumer_leases_by_partition,
            )
          },
          Ok(Err(error)) => {
            warn_every!(
              15.seconds(),
              "broker durable consumer lease scan failed: error={error}",
            );
            (
              DurableConsumerLeaseScanSnapshot {
                status: DurableStateLookupStatus::LookupFailed,
                error: Some(error.to_string()),
              },
              HashMap::new(),
            )
          },
          Err(_) => (
            DurableConsumerLeaseScanSnapshot {
              status: DurableStateLookupStatus::TimedOut,
              error: Some(format!("lookup exceeded {DURABLE_STATE_LOOKUP_TIMEOUT:?}")),
            },
            HashMap::new(),
          ),
        }
      } else {
        (
          DurableConsumerLeaseScanSnapshot {
            status: DurableStateLookupStatus::Unavailable,
            error: None,
          },
          HashMap::new(),
        )
      };

    let durable_partition_keys = self
      .topics
      .values()
      .flat_map(|topic| {
        (0 .. topic.partition_count.saturating_mul(topic.num_writers)).map(|virtual_partition_id| {
          ProducerPartitionLeaseKey {
            topic: topic.name.clone(),
            virtual_partition_id,
          }
        })
      })
      .collect::<Vec<_>>();
    let durable_producer_leases =
      stream::iter(durable_partition_keys.into_iter().map(|key| async {
        let observation = match tokio::time::timeout(
          DURABLE_STATE_LOOKUP_TIMEOUT,
          self.lease_store.get_lease(&key),
        )
        .await
        {
          Ok(Ok(Some(lease))) => DurableProducerLeaseObservation {
            status: DurableStateLookupStatus::Present,
            lease: Some(DurableProducerLeaseSnapshot {
              holder_id: lease.fence.holder_id,
              lease_epoch: lease.fence.lease_epoch,
              lease_session_id: lease.fence.lease_session_id,
              expires_at: lease.lease_expiration_at,
              is_active: lease.lease_expiration_at > generated_at,
              reservation_start: lease.reservation_start,
              last_handed_out_seq: lease.last_handed_out_seq,
              sequence_progress_updated_at: lease.sequence_progress_updated_at,
              max_allocated_seq: lease.max_allocated_seq,
            }),
            error: None,
          },
          Ok(Ok(None)) => DurableProducerLeaseObservation {
            status: DurableStateLookupStatus::Missing,
            lease: None,
            error: None,
          },
          Ok(Err(error)) => {
            warn_every!(
              15.seconds(),
              "broker durable producer lease lookup failed: topic={}, virtual_partition_id={}, \
               error={error}",
              key.topic,
              key.virtual_partition_id,
            );
            DurableProducerLeaseObservation {
              status: DurableStateLookupStatus::LookupFailed,
              lease: None,
              error: Some(error.to_string()),
            }
          },
          Err(_) => DurableProducerLeaseObservation {
            status: DurableStateLookupStatus::TimedOut,
            lease: None,
            error: Some(format!("lookup exceeded {DURABLE_STATE_LOOKUP_TIMEOUT:?}")),
          },
        };
        (key, observation)
      }))
      .buffer_unordered(MAX_CONCURRENT_DURABLE_STATE_LOOKUPS)
      .collect::<HashMap<_, _>>()
      .await;

    let mut durable_topics = self
      .topics
      .values()
      .map(|topic| {
        let mut partitions = (0 .. topic.partition_count.saturating_mul(topic.num_writers))
          .map(|virtual_partition_id| {
            let mut consumer_leases = consumer_leases_by_partition
              .get(&(topic.name.to_string(), virtual_partition_id))
              .cloned()
              .unwrap_or_default();
            consumer_leases.sort_by(|left, right| left.group_id.cmp(&right.group_id));
            let producer_lease = durable_producer_leases
              .get(&ProducerPartitionLeaseKey {
                topic: topic.name.clone(),
                virtual_partition_id,
              })
              .expect("durable producer lookup must exist for every configured partition")
              .clone();
            DurablePartitionStateSnapshot {
              virtual_partition_id,
              producer_lease,
              consumer_leases,
            }
          })
          .collect::<Vec<_>>();
        partitions.sort_by_key(|partition| partition.virtual_partition_id);
        DurableTopicStateSnapshot {
          name: topic.name.clone(),
          partitions,
        }
      })
      .collect::<Vec<_>>();
    durable_topics.sort_by(|left, right| left.name.cmp(&right.name));

    let effective_flush_config = *self.effective_flush_config.read();
    BrokerStateSnapshot {
      generated_at,
      holder_id: self.holder_id.clone(),
      writer_id: self.config.writer_id,
      flush_max_bytes: self.config.flush_max_bytes,
      effective_flush_max_bytes: effective_flush_config.max_bytes,
      max_segment_bytes: self.config.max_segment_bytes,
      effective_max_segment_bytes: self.config.max_segment_bytes(self.feature_flags.as_ref()),
      flush_max_delay: StdDuration::try_from(self.config.flush_max_delay)
        .unwrap_or(StdDuration::MAX),
      effective_flush_max_delay: StdDuration::try_from(effective_flush_config.max_delay)
        .unwrap_or(StdDuration::MAX),
      membership: membership_snapshot,
      ownership,
      topics,
      durable_consumer_lease_scan,
      durable_topics,
    }
  }
}

//
// AdmissionController
//

pub trait AdmissionController: Send + Sync {
  fn is_overloaded(&self) -> bool;
}

impl AdmissionController for MemoryPressureController {
  fn is_overloaded(&self) -> bool {
    self.is_overloaded()
  }
}
