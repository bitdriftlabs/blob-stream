use super::metrics::WriteMetrics;
use super::{WriteEngineImpl, WriteError};
use anyhow::{Context, Result, anyhow};
use blob_stream_metadata_store::{
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservationOutcome,
};
use blob_stream_types::{SeqRange, VirtualPartitionId};
use log::debug;
use std::sync::Arc;
use std::time::Instant;

mod assignment;

pub(super) async fn acquire_lease_and_reserve_sequences(
  lease_store: &Arc<dyn ProducerPartitionLeaseStore>,
  holder_id: &str,
  key: ProducerPartitionLeaseKey,
  now_ts_ms: i64,
  lease_duration_ms: i64,
  reservation_size: Option<u64>,
  metrics: &WriteMetrics,
) -> Result<LeaseAcquireAndReserveOutcome> {
  let reservation_started = reservation_size.map(|_| Instant::now());
  let outcome = lease_store
    .acquire_lease_and_reserve_sequences(
      key,
      holder_id.to_string(),
      now_ts_ms,
      lease_duration_ms,
      reservation_size,
    )
    .await
    .context("acquire producer partition lease and reserve sequences");
  if let Some(reservation_started) = reservation_started {
    metrics
      .sequence_reservation_latency_seconds
      .observe(reservation_started.elapsed().as_secs_f64());
  }
  outcome
}

impl WriteEngineImpl {
  pub(super) async fn ensure_lease(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
    now_ts_ms: i64,
  ) -> Result<i64, WriteError> {
    let key = ProducerPartitionLeaseKey {
      topic: topic.to_string(),
      virtual_partition_id,
    };

    match self
      .lease_store
      .acquire_lease(
        key,
        self.holder_id.clone(),
        now_ts_ms,
        self.config.lease_duration_ms,
      )
      .await
      .context("acquire producer partition lease")?
    {
      LeaseAcquireOutcome::Acquired(lease) => Ok(lease.lease_expiration_ts_ms),
      LeaseAcquireOutcome::HeldByOther(_) => Err(WriteError::NotLeaseHolder {
        topic: topic.to_string(),
        virtual_partition_id,
      }),
    }
  }

  pub(super) async fn reserve_sequences(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
    now_ts_ms: i64,
    reservation_size: u64,
  ) -> Result<SeqRange, WriteError> {
    let key = ProducerPartitionLeaseKey {
      topic: topic.to_string(),
      virtual_partition_id,
    };
    let started = Instant::now();
    let outcome = self
      .lease_store
      .reserve_sequences(&key, &self.holder_id, now_ts_ms, reservation_size)
      .await
      .context("reserve sequences");
    self
      .metrics
      .sequence_reservation_latency_seconds
      .observe(started.elapsed().as_secs_f64());

    match outcome {
      Ok(SequenceReservationOutcome::Reserved(reservation)) => {
        self.metrics.record_sequence_reservation(&reservation.range);
        Ok(reservation.range)
      },
      Ok(SequenceReservationOutcome::HeldByOther(_) | SequenceReservationOutcome::Expired) => {
        self.metrics.sequence_reservation_failures_total.inc();
        Err(WriteError::NotLeaseHolder {
          topic: topic.to_string(),
          virtual_partition_id,
        })
      },
      Err(error) => {
        self.metrics.sequence_reservation_failures_total.inc();
        Err(error.into())
      },
    }
  }

  pub(super) async fn acquire_lease_and_reserve_sequences(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
    now_ts_ms: i64,
    reservation_size: u64,
  ) -> Result<(i64, SeqRange), WriteError> {
    let key = ProducerPartitionLeaseKey {
      topic: topic.to_string(),
      virtual_partition_id,
    };
    let outcome = acquire_lease_and_reserve_sequences(
      &self.lease_store,
      &self.holder_id,
      key,
      now_ts_ms,
      self.config.lease_duration_ms,
      Some(reservation_size),
      &self.metrics,
    )
    .await;

    match outcome {
      Ok(LeaseAcquireAndReserveOutcome::Acquired {
        lease,
        reservation: Some(reservation),
      }) => {
        self.metrics.record_sequence_reservation(&reservation);
        debug!(
          "broker sequence reservation acquired during produce with coalesced lease update: \
           topic={topic}, virtual_partition_id={virtual_partition_id}, \
           requested_size={reservation_size}, start={}, end={}",
          reservation.start, reservation.end
        );
        Ok((lease.lease_expiration_ts_ms, reservation))
      },
      Ok(LeaseAcquireAndReserveOutcome::Acquired {
        reservation: None, ..
      }) => {
        self.metrics.sequence_reservation_failures_total.inc();
        Err(WriteError::Internal(anyhow!(
          "lease store acquired lease without requested sequence reservation"
        )))
      },
      Ok(LeaseAcquireAndReserveOutcome::HeldByOther(_)) => {
        self.metrics.sequence_reservation_failures_total.inc();
        Err(WriteError::NotLeaseHolder {
          topic: topic.to_string(),
          virtual_partition_id,
        })
      },
      Err(error) => {
        self.metrics.sequence_reservation_failures_total.inc();
        Err(error.into())
      },
    }
  }
}
