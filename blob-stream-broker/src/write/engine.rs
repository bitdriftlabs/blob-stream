mod core;

use super::api::{
  BrokerLeaseSnapshot,
  BrokerLeaseStatus,
  BrokerNodeSnapshot,
  BrokerPartitionOwnershipSnapshot,
  BrokerPartitionStateSnapshot,
  BrokerStateSnapshot,
  BrokerTopicStateSnapshot,
  SequenceReservationSnapshot,
  WriteEngine,
  WriteError,
  WriteRequest,
  WriteResponse,
};
use crate::write::allocation::{
  AllocationTransitionDecision,
  LeaseExpirationUpdate,
  begin_allocation_transition,
};
use crate::write::buffer::BufferedBatch;
use crate::write::memory_pressure::MemoryPressureController;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bd_log::warn_every;
use bd_server_stats::stats::Scope;
use bd_shutdown::ComponentShutdownTriggerHandle;
use bd_time::OffsetDateTimeExt;
use blob_stream_broker_discovery::{balanced_assignment, writer_virtual_partitions};
use blob_stream_metadata_store::ProducerPartitionLeaseKey;
use blob_stream_types::{RecordBatch, format_unix_timestamp_ms};
pub use core::WriteEngineImpl;
use log::trace;
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};
use time::ext::NumericalDuration;
use tokio::sync::oneshot;

#[async_trait]
impl WriteEngine for WriteEngineImpl {
  async fn produce_batch(&self, request: WriteRequest) -> Result<WriteResponse, WriteError> {
    let started = Instant::now();
    self.metrics.produce_requests_total.inc();
    self
      .metrics
      .produce_records_total
      .inc_by(request.records.len() as u64);
    self.metrics.produce_payload_bytes_total.inc_by(
      request
        .records
        .iter()
        .map(|record| record.payload.len() as u64)
        .sum::<u64>(),
    );

    trace!(
      "broker write request accepted: topic={}, virtual_partition_id={}, records={}",
      request.topic,
      request.virtual_partition_id,
      request.records.len()
    );

    let topic_info = self
      .topics
      .get(&request.topic)
      .ok_or_else(|| WriteError::UnknownTopic(request.topic.clone()))?;

    if !topic_info.is_valid_partition(request.virtual_partition_id) {
      return Err(WriteError::InvalidPartition {
        topic: request.topic,
        virtual_partition_id: request.virtual_partition_id,
      });
    }

    if request.records.is_empty() {
      return Err(WriteError::Overloaded("record batch is empty".to_string()));
    }

    let summary = RecordBatch::summary_from_records(&request.records)
      .ok_or_else(|| anyhow!("failed to summarize record batch"))?;
    if self.admission.is_overloaded() {
      self.metrics.admission_rejections_total.inc();
      return Err(WriteError::Overloaded(
        "broker admission controller is overloaded".to_string(),
      ));
    }
    let topic = request.topic;
    let virtual_partition_id = request.virtual_partition_id;
    let mut records = Some(request.records);
    let mut summary = Some(summary);
    let record_count = records
      .as_ref()
      .map(|records| records.len() as u64)
      .unwrap_or_default();

    let (completion_rx, seq_range) = loop {
      let now_ts_ms = self.time_provider.now().unix_timestamp_ms();
      let buffered = {
        let mut state = self.state.lock();
        let partition_state = state.partition_state_mut(&topic, virtual_partition_id);
        if partition_state.draining {
          return Err(WriteError::NotLeaseHolder {
            topic,
            virtual_partition_id,
          });
        }

        if partition_state.allocation_in_flight
          || partition_state.needs_lease(now_ts_ms)
          || !partition_state.seq_allocator.can_allocate(record_count)
        {
          None
        } else {
          let (completion_tx, completion_rx) = oneshot::channel();
          let seq_range = partition_state
            .seq_allocator
            .allocate(record_count)
            .ok_or_else(|| WriteError::Overloaded("sequence reservation exhausted".to_string()))?;
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
          partition_state.buffer.push(
            BufferedBatch {
              records: records.take().expect("records are buffered only once"),
              summary: summary.take().expect("summary is buffered only once"),
              seq_range: seq_range.clone(),
              completion: Some(completion_tx),
            },
            now_ts_ms,
          );
          Some((completion_rx, seq_range))
        }
      };
      if let Some(buffered) = buffered {
        break buffered;
      }

      let decision = begin_allocation_transition(
        &self.state,
        &topic,
        virtual_partition_id,
        record_count,
        now_ts_ms,
        false,
        self.config.reservation_size,
      );
      match decision {
        AllocationTransitionDecision::Draining => {
          return Err(WriteError::NotLeaseHolder {
            topic,
            virtual_partition_id,
          });
        },
        AllocationTransitionDecision::Ready => {},
        AllocationTransitionDecision::Waiting(notified) => notified.await,
        AllocationTransitionDecision::Claimed(work) => {
          let mut lease_expiration = LeaseExpirationUpdate::Preserve;
          // Sequence reservation is conditionally accepted only for the current active lease
          // holder. When acquisition is required, the lease may be absent or expired, so it must
          // complete before reserving sequences; these calls cannot be run in parallel.
          let mut reservation = None;
          match (work.needs_lease, work.reservation) {
            (true, Some(request)) => match self
              .acquire_lease_and_reserve_sequences(
                &topic,
                virtual_partition_id,
                now_ts_ms,
                request.size,
              )
              .await
            {
              Ok((expires_at, range)) => {
                lease_expiration = LeaseExpirationUpdate::Set(Some(expires_at));
                reservation = Some(range);
              },
              Err(error) => {
                work
                  .transition
                  .finish(LeaseExpirationUpdate::Preserve, None);
                return Err(error);
              },
            },
            (true, None) => match self
              .ensure_lease(&topic, virtual_partition_id, now_ts_ms)
              .await
            {
              Ok(expires_at) => lease_expiration = LeaseExpirationUpdate::Set(Some(expires_at)),
              Err(error) => {
                work
                  .transition
                  .finish(LeaseExpirationUpdate::Preserve, None);
                return Err(error);
              },
            },
            (false, Some(request)) => match self
              .reserve_sequences(&topic, virtual_partition_id, now_ts_ms, request.size)
              .await
            {
              Ok(range) => reservation = Some(range),
              Err(error) => {
                work.transition.finish(lease_expiration, None);
                return Err(error);
              },
            },
            (false, None) => {},
          }
          work.transition.finish(lease_expiration, reservation);
        },
      }
    };

    self.flush_notifier.notify_one();

    match completion_rx.await {
      Ok(Ok(())) => {},
      Ok(Err(error)) => {
        let write_error = WriteError::Internal(anyhow!(error));
        self.metrics.record_produce_error(&write_error);
        self
          .metrics
          .produce_latency_seconds
          .observe(started.elapsed().as_secs_f64());
        return Err(write_error);
      },
      Err(_closed) => {
        let write_error = WriteError::Internal(anyhow!(
          "flush completion channel closed before acknowledgment"
        ));
        self.metrics.record_produce_error(&write_error);
        self
          .metrics
          .produce_latency_seconds
          .observe(started.elapsed().as_secs_f64());
        return Err(write_error);
      },
    }

    self.metrics.produce_ok_total.inc();
    self
      .metrics
      .produce_latency_seconds
      .observe(started.elapsed().as_secs_f64());

    Ok(WriteResponse { seq_range })
  }

  fn produce_request_timeout(&self) -> StdDuration {
    self.config.produce_request_timeout()
  }

  async fn state_snapshot(&self) -> BrokerStateSnapshot {
    let generated_at_ts_ms = self.time_provider.now().unix_timestamp_ms();
    let generated_at = format_unix_timestamp_ms(generated_at_ts_ms);
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
              lease_expires_at: partition_state
                .lease_expiration_ts_ms
                .map(format_unix_timestamp_ms),
              allocation_in_flight: partition_state.allocation_in_flight,
              allocation_started_at: partition_state
                .allocation_started_ts_ms
                .map(format_unix_timestamp_ms),
              buffered_batch_count: partition_state.buffer.batches.len(),
              buffered_record_count: partition_state
                .buffer
                .batches
                .iter()
                .map(|batch| batch.records.len())
                .sum(),
              buffered_bytes: partition_state.buffer.buffered_bytes,
              first_buffered_at: partition_state
                .buffer
                .first_buffered_ts_ms
                .map(format_unix_timestamp_ms),
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
          .remove(&topic.name)
          .unwrap_or_default();
        local_partitions.sort_by_key(|partition| partition.virtual_partition_id);
        BrokerTopicStateSnapshot {
          name: topic.name.clone(),
          partition_count: topic.partition_count,
          num_writers: topic.num_writers,
          retention_days: topic.retention_days,
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
      let assignment_is_local = assigned_broker.node_id == self.holder_id;
      let assigned_broker = BrokerNodeSnapshot {
        node_id: assigned_broker.node_id,
        address: assigned_broker.address,
      };

      let (lease_status, observed_lease) = match lease {
        Ok(Some(lease)) => {
          let is_active = lease.lease_expiration_ts_ms > generated_at_ts_ms;
          let holder_address = membership
            .nodes()
            .unwrap_or_default()
            .iter()
            .find(|node| node.node_id == lease.holder_id)
            .map(|node| node.address.clone());
          let lease_status = if !is_active {
            BrokerLeaseStatus::UnleasedOrExpired
          } else if lease.holder_id == self.holder_id {
            BrokerLeaseStatus::LocalActive
          } else {
            BrokerLeaseStatus::RemoteActive
          };
          (
            lease_status,
            Some(BrokerLeaseSnapshot {
              holder_id: lease.holder_id,
              holder_address,
              expires_at: format_unix_timestamp_ms(lease.lease_expiration_ts_ms),
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

    BrokerStateSnapshot {
      generated_at,
      holder_id: self.holder_id.clone(),
      writer_id: self.config.writer_id,
      flush_max_bytes: self.config.flush_max_bytes,
      flush_max_delay_ms: self.config.flush_max_delay_ms,
      membership: membership_snapshot,
      ownership,
      topics,
    }
  }
}

//
// AdmissionController
//

pub trait AdmissionController: Send + Sync {
  fn is_overloaded(&self) -> bool;
}

//
// MemoryPressureAdmissionController
//

#[derive(Clone, Debug)]
pub struct MemoryPressureAdmissionController {
  memory_pressure: MemoryPressureController,
}

impl MemoryPressureAdmissionController {
  #[must_use]
  pub fn new(
    shutdown_trigger_handle: &ComponentShutdownTriggerHandle,
    metrics_scope: &Scope,
  ) -> Self {
    Self {
      memory_pressure: MemoryPressureController::new(shutdown_trigger_handle, metrics_scope),
    }
  }
}

impl AdmissionController for MemoryPressureAdmissionController {
  fn is_overloaded(&self) -> bool {
    self.memory_pressure.is_overloaded()
  }
}
