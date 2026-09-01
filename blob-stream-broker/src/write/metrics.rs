use super::buffer::{FlushPlan, FlushTrigger};
use super::{WriteError, WriteResponse};
use bd_server_stats::stats::Scope;
use blob_stream_types::SeqRange;

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
  pub(super) admission_rejections_total: prometheus::IntCounter,
  pub(super) sequence_reservations_total: prometheus::IntCounter,
  pub(super) sequence_reservation_records_total: prometheus::IntCounter,
  pub(super) sequence_reservation_failures_total: prometheus::IntCounter,
  pub(super) sequence_reservation_latency_seconds: prometheus::Histogram,
  pub(super) flush_batches_max_bytes_total: prometheus::IntCounter,
  pub(super) flush_batches_max_delay_total: prometheus::IntCounter,
  pub(super) flush_batches_lease_drain_total: prometheus::IntCounter,
  pub(super) flush_partitions_total: prometheus::IntCounter,
  pub(super) flush_max_segment_size_splits_total: prometheus::IntCounter,
  pub(super) active_flush_plans: prometheus::IntGauge,
  pub(super) flush_failures_total: prometheus::IntCounter,
  pub(super) flush_latency_seconds: prometheus::Histogram,
  pub(super) metadata_publication_deadline_exhausted_before_persistence_total:
    prometheus::IntCounter,
  pub(super) metadata_publication_deadline_exhausted_while_persisting_total: prometheus::IntCounter,
  pub(super) flush_uploaded_objects_total: prometheus::IntCounter,
  pub(super) flush_oversized_single_partition_objects_total: prometheus::IntCounter,
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
      admission_rejections_total: scope.counter("admission_rejections_total"),
      sequence_reservations_total: scope.counter("sequence_reservations_total"),
      sequence_reservation_records_total: scope.counter("sequence_reservation_records_total"),
      sequence_reservation_failures_total: scope.counter("sequence_reservation_failures_total"),
      sequence_reservation_latency_seconds: scope.histogram("sequence_reservation_latency_seconds"),
      flush_batches_max_bytes_total: scope.counter("flush_batches_max_bytes_total"),
      flush_batches_max_delay_total: scope.counter("flush_batches_max_delay_total"),
      flush_batches_lease_drain_total: scope.counter("flush_batches_lease_drain_total"),
      flush_partitions_total: scope.counter("flush_partitions_total"),
      flush_max_segment_size_splits_total: scope.counter("flush_max_segment_size_splits_total"),
      active_flush_plans: scope.gauge("active_flush_plans"),
      flush_failures_total: scope.counter("flush_failures_total"),
      flush_latency_seconds: scope.histogram("flush_latency_seconds"),
      metadata_publication_deadline_exhausted_before_persistence_total: scope
        .counter("metadata_publication_deadline_exhausted_before_persistence_total"),
      metadata_publication_deadline_exhausted_while_persisting_total: scope
        .counter("metadata_publication_deadline_exhausted_while_persisting_total"),
      flush_uploaded_objects_total: scope.counter("flush_uploaded_objects_total"),
      flush_oversized_single_partition_objects_total: scope
        .counter("flush_oversized_single_partition_objects_total"),
      flush_uploaded_object_bytes_total: scope.counter("flush_uploaded_object_bytes_total"),
      flush_uploaded_object_bytes: scope.histogram_with_buckets(
        "flush_uploaded_object_bytes",
        UPLOADED_OBJECT_SIZE_BUCKETS_BYTES,
      ),
      lease_drain_starts_total: scope.counter("lease_drain_starts_total"),
      lease_drain_completions_total: scope.counter("lease_drain_completions_total"),
    }
  }

  pub(super) fn record_flush_plan_summary(&self, plans: &[FlushPlan]) {
    for plan in plans {
      for topic_plan in &plan.topics {
        self
          .flush_partitions_total
          .inc_by(topic_plan.partitions.len() as u64);
        for partition in &topic_plan.partitions {
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
  }
}

//
// ProduceOutcomeMetrics
//

pub struct ProduceOutcomeMetrics {
  records_total: prometheus::IntCounter,
  payload_bytes_total: prometheus::IntCounter,
  rejected_records_total: prometheus::IntCounter,
  rejected_payload_bytes_total: prometheus::IntCounter,
  ok_total: prometheus::IntCounter,
  not_lease_holder_total: prometheus::IntCounter,
  overloaded_total: prometheus::IntCounter,
  unknown_topic_total: prometheus::IntCounter,
}

impl ProduceOutcomeMetrics {
  pub fn new(scope: &Scope) -> Self {
    let scope = scope.scope("write");
    Self {
      records_total: scope.counter("produce_records_total"),
      payload_bytes_total: scope.counter("produce_payload_bytes_total"),
      rejected_records_total: scope.counter("produce_rejected_records_total"),
      rejected_payload_bytes_total: scope.counter("produce_rejected_payload_bytes_total"),
      ok_total: scope.counter("produce_ok_total"),
      not_lease_holder_total: scope.counter("produce_not_lease_holder_total"),
      overloaded_total: scope.counter("produce_overloaded_total"),
      unknown_topic_total: scope.counter("produce_unknown_topic_total"),
    }
  }

  fn record_error(&self, error: &WriteError, record_count: u64, payload_bytes: u64) {
    self.rejected_records_total.inc_by(record_count);
    self.rejected_payload_bytes_total.inc_by(payload_bytes);
    match error {
      WriteError::UnknownTopic(_) => self.unknown_topic_total.inc(),
      WriteError::NotLeaseHolder { .. } | WriteError::LeaseFenceLost => {
        self.not_lease_holder_total.inc();
      },
      WriteError::InvalidRequest(_) => {},
      WriteError::InvalidPartition { .. } | WriteError::Overloaded(_) | WriteError::Internal(_) => {
        self.overloaded_total.inc();
      },
    }
  }

  pub fn record_result(
    &self,
    result: &std::result::Result<WriteResponse, WriteError>,
    record_count: u64,
    payload_bytes: u64,
  ) {
    match result {
      Ok(_) => {
        self.ok_total.inc();
        self.records_total.inc_by(record_count);
        self.payload_bytes_total.inc_by(payload_bytes);
      },
      Err(error) => self.record_error(error, record_count, payload_bytes),
    }
  }
}

impl WriteMetrics {
  #[allow(clippy::cast_precision_loss)] // Prometheus histograms require f64 observations.
  pub(super) fn record_uploaded_object(&self, payload_bytes: usize, oversized_singleton: bool) {
    self.flush_uploaded_objects_total.inc();
    if oversized_singleton {
      self.flush_oversized_single_partition_objects_total.inc();
    }
    self
      .flush_uploaded_object_bytes_total
      .inc_by(payload_bytes as u64);
    self
      .flush_uploaded_object_bytes
      .observe(payload_bytes as f64);
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
