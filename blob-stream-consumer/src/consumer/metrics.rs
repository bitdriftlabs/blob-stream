use bd_server_stats::stats::{ContributionGauge, Scope};
use prometheus::{Histogram, IntCounter};
use std::time::Instant;

//
// ConsumerReaderMetrics
//

#[derive(Clone)]
pub(in crate::consumer) struct ConsumerReaderMetrics {
  read_available_calls: IntCounter,
  read_available_empty: IntCounter,
  read_available_latency_seconds: Histogram,
  metadata_scan_requests: IntCounter,
  metadata_scan_segments: IntCounter,
  metadata_scan_latency_seconds: Histogram,
  metadata_fast_scan_requests: IntCounter,
  metadata_fast_scan_segments: IntCounter,
  metadata_recovery_scan_requests: IntCounter,
  metadata_recovery_scan_segments: IntCounter,
  pub(in crate::consumer) metadata_recovery_scan_failures: IntCounter,
  metadata_recovery_scan_hits: IntCounter,
  metadata_recovery_scan_batches_read: IntCounter,
  recovery_metadata_cache_hits: IntCounter,
  recovery_metadata_cache_misses: IntCounter,
  recovery_metadata_cache_inserts: IntCounter,
  recovery_metadata_cache_invalidations: IntCounter,
  recovery_metadata_cache_entries: ContributionGauge,
  recovery_metadata_cache_retained_bytes: ContributionGauge,
  metadata_fast_scan_frontiers: ContributionGauge,
  pub(in crate::consumer) metadata_fast_scan_without_lower_bound: IntCounter,
  pub(in crate::consumer) metadata_fast_scan_segments_below_partition_frontier: IntCounter,
  pub(in crate::consumer) metadata_fast_scan_segments_without_assigned_batches: IntCounter,
  pub(in crate::consumer) metadata_segments_deferred_by_visibility_delay: IntCounter,
  metadata_batches_scanned: IntCounter,
  metadata_batches_skipped_by_cursor: IntCounter,
  blob_range_requests: IntCounter,
  blob_range_bytes: IntCounter,
  blob_batch_ranges: IntCounter,
  blob_batch_range_bytes: IntCounter,
  blob_range_latency_seconds: Histogram,
  batches_read: IntCounter,
  records_read: IntCounter,
  record_payload_bytes: IntCounter,
}

impl ConsumerReaderMetrics {
  pub(in crate::consumer) fn new(scope: &Scope) -> Self {
    let scope = scope.scope("reader");
    Self {
      read_available_calls: scope.counter("read_available_calls"),
      read_available_empty: scope.counter("read_available_empty"),
      read_available_latency_seconds: scope.histogram("read_available_latency_seconds"),
      metadata_scan_requests: scope.counter("metadata_scan_requests"),
      metadata_scan_segments: scope.counter("metadata_scan_segments"),
      metadata_scan_latency_seconds: scope.histogram("metadata_scan_latency_seconds"),
      metadata_fast_scan_requests: scope.counter("metadata_fast_scan_requests"),
      metadata_fast_scan_segments: scope.counter("metadata_fast_scan_segments"),
      metadata_recovery_scan_requests: scope.counter("metadata_recovery_scan_requests"),
      metadata_recovery_scan_segments: scope.counter("metadata_recovery_scan_segments"),
      metadata_recovery_scan_failures: scope.counter("metadata_recovery_scan_failures"),
      metadata_recovery_scan_hits: scope.counter("metadata_recovery_scan_hits"),
      metadata_recovery_scan_batches_read: scope.counter("metadata_recovery_scan_batches_read"),
      recovery_metadata_cache_hits: scope.counter("recovery_metadata_cache_hits"),
      recovery_metadata_cache_misses: scope.counter("recovery_metadata_cache_misses"),
      recovery_metadata_cache_inserts: scope.counter("recovery_metadata_cache_inserts"),
      recovery_metadata_cache_invalidations: scope.counter("recovery_metadata_cache_invalidations"),
      recovery_metadata_cache_entries: ContributionGauge::new(
        scope.gauge("recovery_metadata_cache_entries"),
      ),
      recovery_metadata_cache_retained_bytes: ContributionGauge::new(
        scope.gauge("recovery_metadata_cache_retained_bytes"),
      ),
      metadata_fast_scan_frontiers: ContributionGauge::new(
        scope.gauge("metadata_fast_scan_frontiers"),
      ),
      metadata_fast_scan_without_lower_bound: scope
        .counter("metadata_fast_scan_without_lower_bound"),
      metadata_fast_scan_segments_below_partition_frontier: scope
        .counter("metadata_fast_scan_segments_below_partition_frontier"),
      metadata_fast_scan_segments_without_assigned_batches: scope
        .counter("metadata_fast_scan_segments_without_assigned_batches"),
      metadata_segments_deferred_by_visibility_delay: scope
        .counter("metadata_segments_deferred_by_visibility_delay"),
      metadata_batches_scanned: scope.counter("metadata_batches_scanned"),
      metadata_batches_skipped_by_cursor: scope.counter("metadata_batches_skipped_by_cursor"),
      blob_range_requests: scope.counter("blob_range_requests"),
      blob_range_bytes: scope.counter("blob_range_bytes"),
      blob_batch_ranges: scope.counter("blob_batch_ranges"),
      blob_batch_range_bytes: scope.counter("blob_batch_range_bytes"),
      blob_range_latency_seconds: scope.histogram("blob_range_latency_seconds"),
      batches_read: scope.counter("batches_read"),
      records_read: scope.counter("records_read"),
      record_payload_bytes: scope.counter("record_payload_bytes"),
    }
  }

  pub(in crate::consumer) fn record_metadata_scan(
    &self,
    started_at: Instant,
    segment_count: usize,
    recovery_scan: bool,
  ) {
    self.metadata_scan_requests.inc();
    self
      .metadata_scan_segments
      .inc_by(u64::try_from(segment_count).unwrap_or(u64::MAX));
    let (mode_requests, mode_segments) = if recovery_scan {
      (
        &self.metadata_recovery_scan_requests,
        &self.metadata_recovery_scan_segments,
      )
    } else {
      (
        &self.metadata_fast_scan_requests,
        &self.metadata_fast_scan_segments,
      )
    };
    mode_requests.inc();
    mode_segments.inc_by(u64::try_from(segment_count).unwrap_or(u64::MAX));
    self
      .metadata_scan_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  pub(in crate::consumer) fn record_fast_frontiers(&self, frontier_count: usize) {
    self
      .metadata_fast_scan_frontiers
      .set(i64::try_from(frontier_count).unwrap_or(i64::MAX));
  }

  pub(in crate::consumer) fn record_recovery_metadata_cache_hit(&self) {
    self.recovery_metadata_cache_hits.inc();
  }

  pub(in crate::consumer) fn record_recovery_metadata_cache_miss(&self) {
    self.recovery_metadata_cache_misses.inc();
  }

  pub(in crate::consumer) fn record_recovery_metadata_cache_insert(&self) {
    self.recovery_metadata_cache_inserts.inc();
  }

  pub(in crate::consumer) fn record_recovery_metadata_cache_invalidation(&self, count: usize) {
    self
      .recovery_metadata_cache_invalidations
      .inc_by(u64::try_from(count).unwrap_or(u64::MAX));
  }

  pub(in crate::consumer) fn record_recovery_metadata_cache_entries(&self, count: usize) {
    self
      .recovery_metadata_cache_entries
      .set(i64::try_from(count).unwrap_or(i64::MAX));
  }

  pub(in crate::consumer) fn record_recovery_metadata_cache_retained_bytes(&self, bytes: u64) {
    self
      .recovery_metadata_cache_retained_bytes
      .set(i64::try_from(bytes).unwrap_or(i64::MAX));
  }

  pub(in crate::consumer) fn record_blob_range(&self, started_at: Instant, bytes: usize) {
    self.blob_range_requests.inc();
    self
      .blob_range_bytes
      .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    self
      .blob_range_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  pub(in crate::consumer) fn record_blob_batch_ranges(&self, range_count: usize, bytes: u64) {
    self
      .blob_batch_ranges
      .inc_by(u64::try_from(range_count).unwrap_or(u64::MAX));
    self.blob_batch_range_bytes.inc_by(bytes);
  }

  pub(in crate::consumer) fn record_batch(&self, record_count: usize, payload_bytes: usize) {
    self.batches_read.inc();
    self
      .records_read
      .inc_by(u64::try_from(record_count).unwrap_or(u64::MAX));
    self
      .record_payload_bytes
      .inc_by(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
  }

  pub(in crate::consumer) fn record_read_available(
    &self,
    started_at: Instant,
    batch_count: usize,
    metadata_batches_scanned: usize,
    metadata_batches_skipped_by_cursor: usize,
    recovery_scan: bool,
  ) {
    self.read_available_calls.inc();
    if batch_count == 0 {
      self.read_available_empty.inc();
    }
    if recovery_scan && batch_count > 0 {
      self.metadata_recovery_scan_hits.inc();
      self
        .metadata_recovery_scan_batches_read
        .inc_by(u64::try_from(batch_count).unwrap_or(u64::MAX));
    }
    self
      .metadata_batches_scanned
      .inc_by(u64::try_from(metadata_batches_scanned).unwrap_or(u64::MAX));
    self
      .metadata_batches_skipped_by_cursor
      .inc_by(u64::try_from(metadata_batches_skipped_by_cursor).unwrap_or(u64::MAX));
    self
      .read_available_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }
}
