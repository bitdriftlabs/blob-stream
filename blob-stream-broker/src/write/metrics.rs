use super::WriteError;
use super::buffer::{FlushPlan, FlushTrigger};
use bd_server_stats::stats::Scope;
use blob_stream_types::SeqRange;
use std::time::Instant;

const UPLOADED_OBJECT_SIZE_BUCKETS_BYTES: &[f64] = &[
  64.0 * 1024.0,
  256.0 * 1024.0,
  1024.0 * 1024.0,
  4.0 * 1024.0 * 1024.0,
  5.0 * 1024.0 * 1024.0,
  8.0 * 1024.0 * 1024.0,
  16.0 * 1024.0 * 1024.0,
  32.0 * 1024.0 * 1024.0,
  64.0 * 1024.0 * 1024.0,
  128.0 * 1024.0 * 1024.0,
  512.0 * 1024.0 * 1024.0,
];

//
// WriteMetrics
//

#[derive(Clone)]
pub(super) struct WriteMetrics {
  pub(super) produce_requests_total: prometheus::IntCounter,
  pub(super) produce_records_total: prometheus::IntCounter,
  pub(super) produce_payload_bytes_total: prometheus::IntCounter,
  pub(super) produce_ok_total: prometheus::IntCounter,
  pub(super) produce_not_lease_holder_total: prometheus::IntCounter,
  pub(super) produce_overloaded_total: prometheus::IntCounter,
  pub(super) produce_unknown_topic_total: prometheus::IntCounter,
  pub(super) admission_rejections_total: prometheus::IntCounter,
  pub(super) produce_latency_seconds: prometheus::Histogram,
  pub(super) sequence_reservations_total: prometheus::IntCounter,
  pub(super) sequence_reservation_records_total: prometheus::IntCounter,
  pub(super) sequence_reservation_failures_total: prometheus::IntCounter,
  pub(super) sequence_reservation_latency_seconds: prometheus::Histogram,
  pub(super) flush_batches_total: prometheus::IntCounter,
  pub(super) flush_batches_max_bytes_total: prometheus::IntCounter,
  pub(super) flush_batches_max_delay_total: prometheus::IntCounter,
  pub(super) flush_batches_lease_drain_total: prometheus::IntCounter,
  pub(super) flush_partitions_total: prometheus::IntCounter,
  pub(super) flush_plans_total: prometheus::IntCounter,
  pub(super) flush_failures_total: prometheus::IntCounter,
  pub(super) flush_latency_seconds: prometheus::Histogram,
  pub(super) metadata_publication_latency_seconds: prometheus::Histogram,
  pub(super) metadata_publication_deadline_exhausted_before_persistence_total:
    prometheus::IntCounter,
  pub(super) metadata_publication_deadline_exhausted_while_persisting_total: prometheus::IntCounter,
  pub(super) flush_uploaded_object_bytes_total: prometheus::IntCounter,
  pub(super) flush_uploaded_object_bytes: prometheus::Histogram,
  pub(super) lease_drain_starts_total: prometheus::IntCounter,
  pub(super) lease_drain_completions_total: prometheus::IntCounter,
}

impl WriteMetrics {
  pub(super) fn new(scope: &Scope) -> Self {
    let scope = scope.scope("write");
    Self {
      produce_requests_total: scope.counter("produce_requests_total"),
      produce_records_total: scope.counter("produce_records_total"),
      produce_payload_bytes_total: scope.counter("produce_payload_bytes_total"),
      produce_ok_total: scope.counter("produce_ok_total"),
      produce_not_lease_holder_total: scope.counter("produce_not_lease_holder_total"),
      produce_overloaded_total: scope.counter("produce_overloaded_total"),
      produce_unknown_topic_total: scope.counter("produce_unknown_topic_total"),
      admission_rejections_total: scope.counter("admission_rejections_total"),
      produce_latency_seconds: scope.histogram("produce_latency_seconds"),
      sequence_reservations_total: scope.counter("sequence_reservations_total"),
      sequence_reservation_records_total: scope.counter("sequence_reservation_records_total"),
      sequence_reservation_failures_total: scope.counter("sequence_reservation_failures_total"),
      sequence_reservation_latency_seconds: scope.histogram("sequence_reservation_latency_seconds"),
      flush_batches_total: scope.counter("flush_batches_total"),
      flush_batches_max_bytes_total: scope.counter("flush_batches_max_bytes_total"),
      flush_batches_max_delay_total: scope.counter("flush_batches_max_delay_total"),
      flush_batches_lease_drain_total: scope.counter("flush_batches_lease_drain_total"),
      flush_partitions_total: scope.counter("flush_partitions_total"),
      flush_plans_total: scope.counter("flush_plans_total"),
      flush_failures_total: scope.counter("flush_failures_total"),
      flush_latency_seconds: scope.histogram("flush_latency_seconds"),
      metadata_publication_latency_seconds: scope.histogram("metadata_publication_latency_seconds"),
      metadata_publication_deadline_exhausted_before_persistence_total: scope
        .counter("metadata_publication_deadline_exhausted_before_persistence_total"),
      metadata_publication_deadline_exhausted_while_persisting_total: scope
        .counter("metadata_publication_deadline_exhausted_while_persisting_total"),
      flush_uploaded_object_bytes_total: scope.counter("flush_uploaded_object_bytes_total"),
      flush_uploaded_object_bytes: scope.histogram_with_buckets(
        "flush_uploaded_object_bytes",
        UPLOADED_OBJECT_SIZE_BUCKETS_BYTES,
      ),
      lease_drain_starts_total: scope.counter("lease_drain_starts_total"),
      lease_drain_completions_total: scope.counter("lease_drain_completions_total"),
    }
  }

  pub(super) fn record_produce_error(&self, error: &WriteError) {
    match error {
      WriteError::UnknownTopic(_) => self.produce_unknown_topic_total.inc(),
      WriteError::NotLeaseHolder { .. } => self.produce_not_lease_holder_total.inc(),
      WriteError::InvalidPartition { .. } | WriteError::Overloaded(_) | WriteError::Internal(_) => {
        self.produce_overloaded_total.inc();
      },
    }
  }

  pub(super) fn record_flush_plan_summary(&self, plans: &[FlushPlan]) {
    self.flush_plans_total.inc_by(plans.len() as u64);
    for plan in plans {
      self
        .flush_partitions_total
        .inc_by(plan.partitions.len() as u64);
      for partition in &plan.partitions {
        self
          .flush_batches_total
          .inc_by(partition.batches.len() as u64);
        match partition.trigger {
          FlushTrigger::MaxBytes => self
            .flush_batches_max_bytes_total
            .inc_by(partition.batches.len() as u64),
          FlushTrigger::MaxDelay => self
            .flush_batches_max_delay_total
            .inc_by(partition.batches.len() as u64),
          FlushTrigger::LeaseDrain => self
            .flush_batches_lease_drain_total
            .inc_by(partition.batches.len() as u64),
        }
      }
    }
  }

  #[allow(clippy::cast_precision_loss)] // Prometheus histograms require f64 observations.
  pub(super) fn record_uploaded_object(&self, payload_bytes: usize) {
    self
      .flush_uploaded_object_bytes_total
      .inc_by(payload_bytes as u64);
    self
      .flush_uploaded_object_bytes
      .observe(payload_bytes as f64);
  }

  pub(super) fn record_metadata_publication_latency(&self, started_at: Instant) {
    self
      .metadata_publication_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  pub(super) fn record_metadata_publication_deadline_exhausted_before_persistence(&self) {
    self
      .metadata_publication_deadline_exhausted_before_persistence_total
      .inc();
  }

  pub(super) fn record_metadata_publication_deadline_exhausted_while_persisting(&self) {
    self
      .metadata_publication_deadline_exhausted_while_persisting_total
      .inc();
  }

  pub(super) fn record_sequence_reservation(&self, range: &SeqRange) {
    self.sequence_reservations_total.inc();
    self
      .sequence_reservation_records_total
      .inc_by(range.end.saturating_sub(range.start).saturating_add(1));
  }
}
