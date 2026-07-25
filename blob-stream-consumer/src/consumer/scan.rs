//! Metadata scan planning and blob batch decoding for `ConsumerReaderImpl`.
//!
//! This module owns the mechanics that turn reader lifecycle state into metadata requests. It
//! deliberately keeps recovery chronological, scopes fast scans to the publication horizon, and
//! leaves lifecycle transitions to the caller after an entire scan pass succeeds.

use super::{
  ConsumerBatch,
  ConsumerReaderImpl,
  MAX_RECOVERY_WINDOWS_PER_SCAN,
  VirtualPartitionState,
  format_unix_timestamp_seconds,
  metadata_availability_delay_seconds,
};
use crate::config::{
  ConsumerReadRuntimeSettings,
  consumer_candidate_window_count,
  consumer_metadata_visibility_delay_ms,
  consumer_window_size_seconds,
};
use anyhow::{Context, Error, Result, anyhow, ensure};
use blob_stream_blob_store::ByteRange;
use blob_stream_metadata_store::SegmentMetadata;
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  CommittedSourceCheckpoint,
  CompressionCodec,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  Window,
  format_unix_timestamp_ms,
  now_unix_seconds as system_now_unix_seconds,
};
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt, stream};
use log::{debug, info, trace};
use protobuf::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

//
// ScanRequest
//

/// One metadata-window request, with all reader modes that require its results.
///
/// A single window can simultaneously serve fresh partitions, retained-history recovery, and the
/// live fast path. `insert_scan_request` merges those independent demands into one metadata query,
/// so this uses fixed flags rather than a single mode or an allocating mode set.
#[derive(Clone)]
pub(super) struct ScanRequest {
  pub(super) window: TopicWindowKey,
  pub(super) min_snowflake: Option<SnowflakeId>,
  pub(super) recovery_scan: bool,
  pub(super) eligibility: ScanEligibility,
}

//
// ScanEligibility
//

/// Reader modes eligible to consume a shared metadata-window request.
///
/// These flags are intentionally not mutually exclusive: recovery can overlap the fast horizon,
/// and a newly assigned partition can share a window with either. They avoid duplicate metadata
/// queries while each partition still applies its own cursor and mode filtering.
#[derive(Clone, Copy)]
pub(super) struct ScanEligibility {
  pub(super) recovering: bool,
  pub(super) fast: bool,
  pub(super) fresh: bool,
}

//
// ReadCapacity
//

/// Payload-byte capacity available for one reader pass.
#[derive(Clone, Copy)]
pub struct ReadCapacity {
  remaining_payload_bytes: u64,
  oversized_batch_allowed: bool,
}

impl ReadCapacity {
  /// Create a capacity that admits decoded payloads up to `remaining_payload_bytes`.
  #[must_use]
  pub fn new(remaining_payload_bytes: u64) -> Self {
    Self {
      remaining_payload_bytes,
      oversized_batch_allowed: false,
    }
  }

  pub(crate) fn with_oversized_batch(
    remaining_payload_bytes: u64,
    oversized_batch_allowed: bool,
  ) -> Self {
    Self {
      remaining_payload_bytes,
      oversized_batch_allowed,
    }
  }

  /// Reserve one decoded batch. A single oversized batch may make progress from an empty buffer.
  fn reserve(&mut self, payload_bytes: u64) -> bool {
    if payload_bytes <= self.remaining_payload_bytes {
      self.remaining_payload_bytes = self.remaining_payload_bytes.saturating_sub(payload_bytes);
      self.oversized_batch_allowed = false;
      return true;
    }
    if self.oversized_batch_allowed {
      self.remaining_payload_bytes = 0;
      self.oversized_batch_allowed = false;
      return true;
    }
    false
  }
}

//
// BatchReadCandidate
//

/// One ordered, capacity-reserved batch contained in a segment read plan.
struct BatchReadCandidate {
  batch_metadata: BatchMetadata,
  virtual_partition_id: VirtualPartitionId,
}

//
// SegmentReadPlan
//

/// One consolidated segment range read and the ordered batches it supplies.
struct SegmentReadPlan {
  metadata: SegmentMetadata,
  candidates: Vec<BatchReadCandidate>,
  byte_range: ByteRange,
}

impl SegmentReadPlan {
  /// Create the smallest range containing every selected batch in this segment.
  fn new(metadata: SegmentMetadata, candidates: Vec<BatchReadCandidate>) -> Result<Self> {
    ensure!(
      !candidates.is_empty(),
      "segment read plan for {} has no batch candidates",
      metadata.blob_key.as_str()
    );

    let mut start = u64::MAX;
    let mut end = 0;
    for candidate in &candidates {
      let range = &candidate.batch_metadata.byte_range;
      ensure!(
        !range.is_empty(),
        "segment {} has an empty batch range for partition {}",
        metadata.blob_key.as_str(),
        candidate.virtual_partition_id
      );
      start = start.min(range.start);
      end = end.max(range.end);
    }

    Ok(Self {
      metadata,
      candidates,
      byte_range: ByteRange { start, end },
    })
  }
}

impl ConsumerReaderImpl {
  /// Return the bounded current-window horizon, from oldest candidate to newest.
  pub(super) fn scan_windows(&self, now_unix_seconds: i64) -> Result<Vec<TopicWindowKey>> {
    // Anchor scans to the current window and cover the enforced publication deadline plus the
    // eventual-read visibility delay. Oldest -> newest ordering keeps traversal deterministic.
    let current_window =
      Window::for_timestamp(now_unix_seconds, consumer_window_size_seconds(&self.config))
        .start_unix_seconds;

    let candidate_windows =
      consumer_candidate_window_count(&self.config, self.maximum_metadata_publication_lag_ms)?;
    let window_size_seconds = consumer_window_size_seconds(&self.config);
    let mut windows = Vec::with_capacity(candidate_windows);
    for offset in (0 .. candidate_windows).rev() {
      let offset = i64::try_from(offset).unwrap_or(i64::MAX);
      let window_start = current_window.saturating_sub(offset.saturating_mul(window_size_seconds));
      windows.push(TopicWindowKey {
        topic: self.config.topic.to_string(),
        window_start_unix_seconds: window_start,
      });
    }

    trace!(
      "consumer scan windows computed: topic={}, count={}, now={}",
      self.config.topic,
      windows.len(),
      format_unix_timestamp_seconds(now_unix_seconds)
    );

    Ok(windows)
  }

  /// Execute one metadata scan pass, retaining no progress when a batch cannot reach the caller.
  pub(super) async fn read_available_impl(
    &mut self,
    now_unix_seconds: i64,
    capacity: ReadCapacity,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<ConsumerBatch>> {
    // Cursor and frontier changes are only valid once every decoded batch is returned. A later
    // range-read failure must not make already decoded output disappear behind an advanced cursor.
    let virtual_partition_states = self.virtual_partition_states.clone();
    let fast_frontiers = self.fast_frontiers.clone();
    let result = self
      .read_available_impl_once(now_unix_seconds, capacity, runtime_settings)
      .await;
    if result.is_err() {
      self.virtual_partition_states = virtual_partition_states;
      self.fast_frontiers = fast_frontiers;
    }
    result
  }

  /// Execute one metadata scan pass, then advance scan modes only for windows fully observed.
  async fn read_available_impl_once(
    &mut self,
    now_unix_seconds: i64,
    mut capacity: ReadCapacity,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<ConsumerBatch>> {
    let read_started_at = Instant::now();
    let assigned_partition_ids = self.assigned_virtual_partition_ids();
    trace!(
      "consumer read_available start: topic={}, assigned_partitions={}",
      self.config.topic,
      assigned_partition_ids.len()
    );
    // Output contains only newly consumable batches according to per-partition cursor state.
    let mut output = Vec::new();
    let mut metadata_batches_scanned = 0_usize;
    let mut metadata_batches_skipped_by_cursor = 0_usize;
    let visibility_cutoff_ts_ms = now_unix_seconds.saturating_mul(1_000).saturating_sub(
      i64::try_from(consumer_metadata_visibility_delay_ms(&self.config)).unwrap_or(i64::MAX),
    );

    // Recovery scans catch late lower-snowflake rows; fast scans avoid rereading old metadata.
    let (scan_requests, recovery_scan) =
      self.scan_requests(now_unix_seconds, &assigned_partition_ids)?;
    for request in &scan_requests {
      trace!(
        "consumer metadata scan planned: topic={}, window_start={}, recovery={}, fast={}, \
         fresh={}, min_snowflake={:?}",
        request.window.topic,
        format_unix_timestamp_seconds(request.window.window_start_unix_seconds),
        request.eligibility.recovering,
        request.eligibility.fast,
        request.eligibility.fresh,
        request.min_snowflake.map(SnowflakeId::as_u64)
      );
    }
    let scanned_window_starts: Arc<[i64]> = scan_requests
      .iter()
      .map(|request| request.window.window_start_unix_seconds)
      .collect::<Vec<_>>()
      .into();
    let mut scan_states = assigned_partition_ids
      .iter()
      .filter_map(|&partition_id| {
        self
          .virtual_partition_states
          .get(&partition_id)
          .map(|state| {
            (
              partition_id,
              super::ConsumerReaderPartitionScanState::new(
                partition_id,
                scanned_window_starts.clone(),
                now_unix_seconds,
                state.cursor(),
              ),
            )
          })
      })
      .collect::<HashMap<_, _>>();
    self.record_fast_scan_bounds(
      &scan_requests,
      &assigned_partition_ids,
      now_unix_seconds,
      &mut scan_states,
    );
    let recovery_window_end = scan_requests
      .iter()
      .filter(|request| request.eligibility.recovering)
      .map(|request| request.window.window_start_unix_seconds)
      .max();
    let scanned_fresh_window_starts = scan_requests
      .iter()
      .filter(|request| request.eligibility.fresh)
      .map(|request| request.window.window_start_unix_seconds)
      .collect::<Vec<_>>();
    let scan_futures = scan_requests.iter().cloned().map(|request| {
      let metadata_store = Arc::clone(&self.metadata_store);
      let metrics = self.metrics.clone();
      async move {
        if !request.recovery_scan && request.eligibility.fast && request.min_snowflake.is_none() {
          metrics.metadata_fast_scan_without_lower_bound.inc();
        }
        let scan_started_at = Instant::now();
        let segments = metadata_store
          .scan_window_from_snowflake(&request.window, request.min_snowflake)
          .await
          .with_context(|| {
            format!(
              "consumer metadata scan failed: topic={}, window_start={}, recovery={}, fast={}, \
               fresh={}, min_snowflake={:?}",
              request.window.topic,
              format_unix_timestamp_seconds(request.window.window_start_unix_seconds),
              request.eligibility.recovering,
              request.eligibility.fast,
              request.eligibility.fresh,
              request.min_snowflake.map(SnowflakeId::as_u64)
            )
          })?;
        metrics.record_metadata_scan(scan_started_at, segments.len(), request.recovery_scan);
        Ok::<_, Error>((request, segments))
      }
    });

    // Run independent per-window metadata queries concurrently while preserving input order.
    let window_results = match try_join_all(scan_futures).await {
      Ok(window_results) => window_results,
      Err(error) => {
        if recovery_scan {
          self.metrics.metadata_recovery_scan_failures.inc();
        }
        return Err(error);
      },
    };

    let mut next_fast_frontiers = self.fast_frontiers.clone();
    let mut blocked_fast_sources = HashSet::new();
    // A deferred recovery window must hold back later windows for the same partition. Otherwise a
    // later sequence could advance the cursor and make the deferred batch permanently ineligible.
    let mut blocked_recovering_partitions = HashSet::new();
    let mut deferred_recovery_windows = HashSet::new();
    // Recovery can hand an active publication-horizon window to Fast. Fast retains the same
    // visibility safety check and replays its inclusive time floor once the row becomes eligible.
    let fast_horizon_windows = self
      .eligible_fast_scan_windows(now_unix_seconds)?
      .into_iter()
      .map(|(window, _)| window.window_start_unix_seconds)
      .collect::<HashSet<_>>();
    let mut deferred_fresh_partitions = HashSet::new();
    let mut segment_read_plans = Vec::new();
    let mut capacity_exhausted = false;
    'windows: for (request, mut segments) in window_results {
      let window = request.window;
      trace!(
        "consumer scanned window: topic={}, window_start={}, segments={}",
        window.topic,
        format_unix_timestamp_seconds(window.window_start_unix_seconds),
        segments.len()
      );

      // Metadata scans are unordered by contract; sorting provides deterministic processing.
      segments.sort_by_key(|metadata| metadata.snowflake_id);
      for segment in segments {
        // Restrict work to currently assigned virtual partitions only.
        let mut segment_has_assigned_batches = false;
        let mut segment_read_candidates = Vec::new();
        for &partition_id in &assigned_partition_ids {
          let partition_state = self.virtual_partition_states.get(&partition_id);
          let partition_is_eligible = match partition_state {
            Some(VirtualPartitionState::Fresh {
              initial_window_start_unix_seconds,
              ..
            }) => {
              request.eligibility.fresh
                && *initial_window_start_unix_seconds == window.window_start_unix_seconds
            },
            Some(VirtualPartitionState::Recovering { recovery_state, .. }) => {
              request.eligibility.recovering
                && recovery_state.next_window_start_unix_seconds <= window.window_start_unix_seconds
                && window.window_start_unix_seconds
                  <= recovery_state.cutover_window_start_unix_seconds
            },
            Some(VirtualPartitionState::Fast { .. }) => request.eligibility.fast,
            Some(
              VirtualPartitionState::PendingCursor { .. }
              | VirtualPartitionState::PendingRecovering { .. }
              | VirtualPartitionState::PendingFast { .. },
            )
            | None => false,
          };
          if !partition_is_eligible {
            continue;
          }
          if matches!(
            partition_state,
            Some(VirtualPartitionState::Recovering { .. })
          ) && blocked_recovering_partitions.contains(&partition_id)
          {
            continue;
          }
          let scan_state = scan_states
            .get_mut(&partition_id)
            .expect("assigned partition has scan diagnostic state");
          scan_state.metadata_segments_seen = scan_state.metadata_segments_seen.saturating_add(1);
          let Some(partition_batches) = segment.segment_index.get(&partition_id) else {
            trace!(
              "consumer metadata segment has no partition batches: topic={}, partition={}, \
               window_start={}, snowflake_id={}",
              self.config.topic,
              partition_id,
              format_unix_timestamp_seconds(window.window_start_unix_seconds),
              segment.snowflake_id.as_u64()
            );
            scan_state.metadata_segments_without_partition_batches = scan_state
              .metadata_segments_without_partition_batches
              .saturating_add(1);
            continue;
          };
          segment_has_assigned_batches = true;

          let frontier_key = (partition_id, window.window_start_unix_seconds);
          let fast_partition = matches!(partition_state, Some(VirtualPartitionState::Fast { .. }));
          if fast_partition {
            if blocked_fast_sources.contains(&frontier_key) {
              trace!(
                "consumer metadata segment blocked by earlier visibility delay: topic={}, \
                 partition={}, window_start={}, snowflake_id={}",
                self.config.topic,
                partition_id,
                format_unix_timestamp_seconds(window.window_start_unix_seconds),
                segment.snowflake_id.as_u64()
              );
              scan_state.metadata_segments_blocked_by_visibility = scan_state
                .metadata_segments_blocked_by_visibility
                .saturating_add(1);
              continue;
            }
            if next_fast_frontiers
              .get(&frontier_key)
              .is_some_and(|frontier| segment.snowflake_id < *frontier)
            {
              self
                .metrics
                .metadata_fast_scan_segments_below_partition_frontier
                .inc();
              trace!(
                "consumer metadata segment skipped by fast frontier: topic={}, partition={}, \
                 window_start={}, snowflake_id={}, frontier={}",
                self.config.topic,
                partition_id,
                format_unix_timestamp_seconds(window.window_start_unix_seconds),
                segment.snowflake_id.as_u64(),
                next_fast_frontiers
                  .get(&frontier_key)
                  .map_or(0, |frontier| frontier.as_u64())
              );
              scan_state.metadata_segments_skipped_by_frontier = scan_state
                .metadata_segments_skipped_by_frontier
                .saturating_add(1);
              continue;
            }
          }

          if segment.metadata_published_ts_ms > visibility_cutoff_ts_ms {
            self
              .metrics
              .metadata_segments_deferred_by_visibility_delay
              .inc();
            scan_state.metadata_segments_deferred_by_visibility = scan_state
              .metadata_segments_deferred_by_visibility
              .saturating_add(1);
            trace!(
              "consumer deferred metadata by visibility delay: topic={}, partition={}, \
               window_start={}, snowflake_id={}, published_at={}, visibility_cutoff={}",
              self.config.topic,
              partition_id,
              format_unix_timestamp_seconds(window.window_start_unix_seconds),
              segment.snowflake_id.as_u64(),
              format_unix_timestamp_ms(segment.metadata_published_ts_ms),
              format_unix_timestamp_ms(visibility_cutoff_ts_ms)
            );
            if fast_partition {
              blocked_fast_sources.insert(frontier_key);
            } else if matches!(partition_state, Some(VirtualPartitionState::Fresh { .. })) {
              deferred_fresh_partitions.insert(partition_id);
            } else if fast_horizon_windows.contains(&window.window_start_unix_seconds) {
              scan_state.recovery_segments_handed_to_fast_by_visibility = scan_state
                .recovery_segments_handed_to_fast_by_visibility
                .saturating_add(1);
            } else {
              scan_state.recovery_segments_blocked_by_visibility = scan_state
                .recovery_segments_blocked_by_visibility
                .saturating_add(1);
              deferred_recovery_windows.insert(window.window_start_unix_seconds);
              blocked_recovering_partitions.insert(partition_id);
            }
            continue;
          }

          metadata_batches_scanned =
            metadata_batches_scanned.saturating_add(partition_batches.len());
          scan_state.metadata_batches_seen = scan_state
            .metadata_batches_seen
            .saturating_add(partition_batches.len());
          let current_cursor = partition_state.and_then(VirtualPartitionState::cursor);

          // Avoid sorting historical batches when this partition has already consumed all of
          // them. Any late batch beyond the cursor still uses the normal sorted path below.
          if current_cursor.is_some_and(|cursor| {
            partition_batches
              .iter()
              .all(|batch| batch.seq_range.end <= cursor)
          }) {
            metadata_batches_skipped_by_cursor =
              metadata_batches_skipped_by_cursor.saturating_add(partition_batches.len());
            scan_state.metadata_batches_skipped_by_cursor = scan_state
              .metadata_batches_skipped_by_cursor
              .saturating_add(partition_batches.len());
            continue;
          }

          // Metadata scans are unordered and can arrive late. Sorting by seq_start keeps
          // processing deterministic while cursor checks prevent replay.
          let mut sorted_batches = partition_batches.iter().collect::<Vec<_>>();
          sorted_batches.sort_by_key(|batch| batch.seq_range.start);

          for batch_metadata in sorted_batches {
            // Cursor semantics: seq_end <= cursor was already consumed and can be skipped.
            let current_cursor = self
              .virtual_partition_states
              .get(&partition_id)
              .and_then(VirtualPartitionState::cursor);
            if current_cursor.is_some_and(|cursor| batch_metadata.seq_range.end <= cursor) {
              trace!(
                "consumer skipped batch by cursor: topic={}, partition={}, seq_end={}, cursor={}",
                self.config.topic,
                partition_id,
                batch_metadata.seq_range.end,
                current_cursor.unwrap_or(0)
              );
              metadata_batches_skipped_by_cursor =
                metadata_batches_skipped_by_cursor.saturating_add(1);
              scan_state.metadata_batches_skipped_by_cursor = scan_state
                .metadata_batches_skipped_by_cursor
                .saturating_add(1);
              continue;
            }

            if !capacity.reserve(batch_metadata.payload_bytes) {
              capacity_exhausted = true;
              trace!(
                "consumer deferred batch by prefetch capacity: topic={}, partition={}, \
                 seq_start={}, seq_end={}, payload_bytes={}",
                self.config.topic,
                partition_id,
                batch_metadata.seq_range.start,
                batch_metadata.seq_range.end,
                batch_metadata.payload_bytes
              );
              scan_state.metadata_batches_deferred_by_capacity = scan_state
                .metadata_batches_deferred_by_capacity
                .saturating_add(1);
              break;
            }

            segment_read_candidates.push(BatchReadCandidate {
              batch_metadata: batch_metadata.clone(),
              virtual_partition_id: partition_id,
            });
          }

          if capacity_exhausted {
            break;
          }

          if fast_partition {
            next_fast_frontiers
              .entry(frontier_key)
              .and_modify(|frontier| *frontier = (*frontier).max(segment.snowflake_id))
              .or_insert(segment.snowflake_id);
          }
        }
        if !segment_has_assigned_batches {
          self
            .metrics
            .metadata_fast_scan_segments_without_assigned_batches
            .inc();
        }
        if !segment_read_candidates.is_empty() {
          segment_read_plans.push(SegmentReadPlan::new(segment, segment_read_candidates)?);
        }
        if capacity_exhausted {
          break 'windows;
        }
      }
    }

    // `buffered` preserves segment-plan order while allowing independent object-store requests
    // and decoding work to overlap. Cursor changes occur only after every planned read succeeds.
    let mut decoded_batches = stream::iter(
      segment_read_plans
        .into_iter()
        .map(|plan| async { self.read_segment_plan(plan).await }),
    )
    .buffered(runtime_settings.max_in_flight_batch_reads)
    .try_collect::<Vec<_>>()
    .await?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    // Segment plans retain scan order, but concurrent metadata windows may not be ordered by a
    // partition's sequence range. Normalize before cursor advancement and delivery.
    decoded_batches
      .sort_by_key(|(candidate, batch)| (candidate.virtual_partition_id, batch.seq_range.start));

    for (candidate, batch) in decoded_batches {
      let current_cursor = self
        .virtual_partition_states
        .get(&candidate.virtual_partition_id)
        .and_then(VirtualPartitionState::cursor);
      if current_cursor.is_some_and(|cursor| batch.seq_range.end <= cursor) {
        metadata_batches_skipped_by_cursor = metadata_batches_skipped_by_cursor.saturating_add(1);
        if let Some(scan_state) = scan_states.get_mut(&candidate.virtual_partition_id) {
          scan_state.metadata_batches_skipped_by_cursor = scan_state
            .metadata_batches_skipped_by_cursor
            .saturating_add(1);
        }
        continue;
      }

      // Cursor always moves forward. max() keeps monotonicity if metadata ordering is odd.
      let next_cursor = batch.seq_range.end.max(current_cursor.unwrap_or(0));
      if let Some(state) = self
        .virtual_partition_states
        .get_mut(&candidate.virtual_partition_id)
      {
        state.advance_cursor(next_cursor);
      }
      trace!(
        "consumer accepted batch: topic={}, partition={}, seq_start={}, seq_end={}, records={}, \
         new_cursor={}",
        self.config.topic,
        candidate.virtual_partition_id,
        batch.seq_range.start,
        batch.seq_range.end,
        batch.records.len(),
        next_cursor
      );
      if let Some(scan_state) = scan_states.get_mut(&candidate.virtual_partition_id) {
        scan_state.batches_accepted = scan_state.batches_accepted.saturating_add(1);
        scan_state.records_accepted = scan_state
          .records_accepted
          .saturating_add(batch.records.len());
      }
      output.push(batch);
    }

    let window_size_seconds = consumer_window_size_seconds(&self.config);
    let mut initial_scans_completed = Vec::new();
    let mut recoveries_completed = Vec::new();
    if !capacity_exhausted {
      for (partition_id, state) in &mut self.virtual_partition_states {
        let mut next_state = None;
        match state {
          VirtualPartitionState::Fresh {
            cursor,
            initial_window_start_unix_seconds,
            ..
          } if scanned_fresh_window_starts.contains(initial_window_start_unix_seconds)
            && !deferred_fresh_partitions.contains(partition_id) =>
          {
            initial_scans_completed.push((*partition_id, *initial_window_start_unix_seconds));
            next_state = Some(VirtualPartitionState::Fast {
              cursor: *cursor,
              last_scan: None,
            });
          },
          VirtualPartitionState::Recovering {
            cursor,
            recovery_state,
            ..
          } => {
            let Some(recovery_window_end) = recovery_window_end else {
              continue;
            };
            let recovery_window_end = deferred_recovery_windows.iter().min().map_or(
              recovery_window_end,
              |deferred_window| {
                recovery_window_end
                  .min(deferred_window.saturating_sub(consumer_window_size_seconds(&self.config)))
              },
            );
            if recovery_state.next_window_start_unix_seconds > recovery_window_end {
              continue;
            }
            recovery_state.next_window_start_unix_seconds =
              recovery_window_end.saturating_add(window_size_seconds);
            if recovery_state.next_window_start_unix_seconds
              > recovery_state.cutover_window_start_unix_seconds
            {
              recoveries_completed.push((
                *partition_id,
                recovery_state.cutover_window_start_unix_seconds,
              ));
              next_state = Some(VirtualPartitionState::Fast {
                cursor: *cursor,
                last_scan: None,
              });
            }
          },
          VirtualPartitionState::PendingCursor { .. }
          | VirtualPartitionState::PendingRecovering { .. }
          | VirtualPartitionState::PendingFast { .. }
          | VirtualPartitionState::Fresh { .. }
          | VirtualPartitionState::Fast { .. } => {},
        }
        if let Some(next_state) = next_state {
          *state = next_state;
        }
      }
    }
    for (partition_id, initial_window_start_unix_seconds) in initial_scans_completed {
      info!(
        "consumer partition initial scan completed; fast path active: topic={}, partition={}, \
         initial_window={}",
        self.config.topic,
        partition_id,
        format_unix_timestamp_seconds(initial_window_start_unix_seconds)
      );
    }
    for (partition_id, cutover_window_start_unix_seconds) in recoveries_completed {
      info!(
        "consumer partition recovery completed; fast path active: topic={}, partition={}, \
         cutover_window={}",
        self.config.topic,
        partition_id,
        format_unix_timestamp_seconds(cutover_window_start_unix_seconds)
      );
    }

    trace!(
      "consumer read_available complete: topic={}, output_batches={}, \
       metadata_batches_scanned={}, metadata_batches_skipped_by_cursor={}",
      self.config.topic,
      output.len(),
      metadata_batches_scanned,
      metadata_batches_skipped_by_cursor
    );

    self.fast_frontiers = next_fast_frontiers;
    self.prune_fast_frontiers(now_unix_seconds)?;
    let completed_at_unix_seconds = system_now_unix_seconds();
    for ((partition_id, window_start_unix_seconds), snowflake_id) in &self.fast_frontiers {
      if let Some(scan_state) = scan_states.get_mut(partition_id) {
        scan_state
          .fast_frontiers
          .push(super::ConsumerReaderFastFrontierState {
            window_start_unix_seconds: *window_start_unix_seconds,
            snowflake_id: *snowflake_id,
          });
      }
    }
    for (partition_id, scan_state) in &mut scan_states {
      scan_state.completed_at_unix_seconds = completed_at_unix_seconds;
      scan_state.cursor_after = self
        .virtual_partition_states
        .get(partition_id)
        .and_then(VirtualPartitionState::cursor);
      scan_state
        .fast_frontiers
        .sort_by_key(|frontier| frontier.window_start_unix_seconds);
      debug!(
        "consumer partition scan summary: topic={}, partition={}, windows={:?}, \
         cursor_before={:?}, cursor_after={:?}, metadata_segments_seen={}, \
         metadata_segments_without_partition_batches={}, metadata_batches_seen={}, \
         metadata_batches_skipped_by_cursor={}, metadata_segments_skipped_by_frontier={}, \
         metadata_segments_deferred_by_visibility={}, metadata_segments_blocked_by_visibility={}, \
         recovery_segments_handed_to_fast_by_visibility={}, \
         recovery_segments_blocked_by_visibility={}, metadata_batches_deferred_by_capacity={}, \
         batches_accepted={}, records_accepted={}, fast_scan_bounds={:?}, fast_frontiers={:?}",
        self.config.topic,
        partition_id,
        scan_state.scanned_window_starts,
        scan_state.cursor_before,
        scan_state.cursor_after,
        scan_state.metadata_segments_seen,
        scan_state.metadata_segments_without_partition_batches,
        scan_state.metadata_batches_seen,
        scan_state.metadata_batches_skipped_by_cursor,
        scan_state.metadata_segments_skipped_by_frontier,
        scan_state.metadata_segments_deferred_by_visibility,
        scan_state.metadata_segments_blocked_by_visibility,
        scan_state.recovery_segments_handed_to_fast_by_visibility,
        scan_state.recovery_segments_blocked_by_visibility,
        scan_state.metadata_batches_deferred_by_capacity,
        scan_state.batches_accepted,
        scan_state.records_accepted,
        scan_state.fast_scan_bounds,
        scan_state.fast_frontiers
      );
    }
    for (partition_id, scan_state) in scan_states {
      if let Some(state) = self.virtual_partition_states.get_mut(&partition_id) {
        state.set_last_scan(scan_state);
      }
    }

    self.metrics.record_read_available(
      read_started_at,
      output.len(),
      metadata_batches_scanned,
      metadata_batches_skipped_by_cursor,
      recovery_scan,
    );

    Ok(output)
  }

  /// Combine mode-specific demand for one window into one metadata-store query.
  fn insert_scan_request(
    scan_requests: &mut BTreeMap<i64, ScanRequest>,
    topic: &str,
    window_start_unix_seconds: i64,
    min_snowflake: Option<SnowflakeId>,
    recovery_scan: bool,
    eligibility: ScanEligibility,
  ) {
    scan_requests
      .entry(window_start_unix_seconds)
      .and_modify(|request| {
        request.min_snowflake = request.min_snowflake.min(min_snowflake);
        request.recovery_scan |= recovery_scan;
        request.eligibility.recovering |= eligibility.recovering;
        request.eligibility.fast |= eligibility.fast;
        request.eligibility.fresh |= eligibility.fresh;
      })
      .or_insert_with(|| ScanRequest {
        window: TopicWindowKey {
          topic: topic.to_string(),
          window_start_unix_seconds,
        },
        min_snowflake,
        recovery_scan,
        eligibility,
      });
  }

  /// Find the least inclusive lower bound required when one query serves several fast partitions.
  fn fast_scan_min_snowflake(
    &self,
    assigned_partition_ids: &[VirtualPartitionId],
    window_start_unix_seconds: i64,
    safe_floor: SnowflakeId,
  ) -> SnowflakeId {
    let mut minimum = None;
    for &partition_id in assigned_partition_ids {
      let Some((_, partition_lower_bound)) =
        self.fast_scan_partition_lower_bound(partition_id, window_start_unix_seconds, safe_floor)
      else {
        continue;
      };
      minimum = Some(
        minimum.map_or(partition_lower_bound, |current: SnowflakeId| {
          current.min(partition_lower_bound)
        }),
      );
    }
    minimum.unwrap_or(safe_floor)
  }

  /// Return the observed and effective lower bounds for one Fast partition/window pair.
  fn fast_scan_partition_lower_bound(
    &self,
    partition_id: VirtualPartitionId,
    window_start_unix_seconds: i64,
    safe_floor: SnowflakeId,
  ) -> Option<(Option<SnowflakeId>, SnowflakeId)> {
    if !matches!(
      self.virtual_partition_states.get(&partition_id),
      Some(VirtualPartitionState::Fast { .. })
    ) {
      return None;
    }
    let observed_frontier = self
      .fast_frontiers
      .get(&(partition_id, window_start_unix_seconds))
      .copied();
    let partition_lower_bound =
      observed_frontier.map_or(safe_floor, |frontier| frontier.max(safe_floor));
    Some((observed_frontier, partition_lower_bound))
  }

  /// Record the time and frontier inputs that determined each Fast partition's shared query.
  fn record_fast_scan_bounds(
    &self,
    scan_requests: &[ScanRequest],
    assigned_partition_ids: &[VirtualPartitionId],
    now_unix_seconds: i64,
    scan_states: &mut HashMap<VirtualPartitionId, super::ConsumerReaderPartitionScanState>,
  ) {
    let safe_timestamp_unix_seconds = self.fast_scan_safe_timestamp_unix_seconds(now_unix_seconds);
    for request in scan_requests {
      if !request.eligibility.fast {
        continue;
      }
      let floor_timestamp_unix_seconds = request
        .window
        .window_start_unix_seconds
        .max(safe_timestamp_unix_seconds);
      let time_floor = Self::snowflake_floor(floor_timestamp_unix_seconds);
      for &partition_id in assigned_partition_ids {
        let Some((observed_frontier, partition_lower_bound)) = self
          .fast_scan_partition_lower_bound(
            partition_id,
            request.window.window_start_unix_seconds,
            time_floor,
          )
        else {
          continue;
        };
        if let Some(scan_state) = scan_states.get_mut(&partition_id) {
          scan_state
            .fast_scan_bounds
            .push(super::ConsumerReaderFastScanBoundState {
              window_start_unix_seconds: request.window.window_start_unix_seconds,
              floor_timestamp_unix_seconds,
              time_floor,
              observed_frontier,
              partition_lower_bound,
              query_lower_bound: request.min_snowflake,
            });
        }
      }
    }
  }

  /// Return the oldest timestamp whose metadata may still be unpublished or invisible.
  fn fast_scan_safe_timestamp_unix_seconds(&self, now_unix_seconds: i64) -> i64 {
    // This relies on the existing deployment assumption that broker and consumer clocks are
    // synchronized. Keep the two configured timing bounds together so a future skew margin has
    // one obvious place to join the safety calculation.
    now_unix_seconds.saturating_sub(metadata_availability_delay_seconds(
      &self.config,
      self.maximum_metadata_publication_lag_ms,
    ))
  }

  /// Return the lowest possible segment ID for a timestamp, preserving safety for invalid input.
  fn snowflake_floor(timestamp_unix_seconds: i64) -> SnowflakeId {
    time::OffsetDateTime::from_unix_timestamp(timestamp_unix_seconds)
      .map_or(SnowflakeId(0), SnowflakeId::minimum_for_timestamp)
  }

  /// Return Fast windows that can still contain unpublished or invisible metadata and their floors.
  fn eligible_fast_scan_windows(
    &self,
    now_unix_seconds: i64,
  ) -> Result<Vec<(TopicWindowKey, SnowflakeId)>> {
    let safe_timestamp_unix_seconds = self.fast_scan_safe_timestamp_unix_seconds(now_unix_seconds);
    let window_size_seconds = consumer_window_size_seconds(&self.config);
    let windows = self
      .scan_windows(now_unix_seconds)?
      .into_iter()
      .filter(|window| {
        window
          .window_start_unix_seconds
          .saturating_add(window_size_seconds)
          > safe_timestamp_unix_seconds
      })
      .map(|window| {
        let floor_timestamp_unix_seconds = window
          .window_start_unix_seconds
          .max(safe_timestamp_unix_seconds);
        (window, Self::snowflake_floor(floor_timestamp_unix_seconds))
      })
      .collect();
    Ok(windows)
  }

  /// Return the least lower bound that can serve every recovering partition in one window.
  fn recovery_scan_min_snowflake(&self, window_start_unix_seconds: i64) -> Option<SnowflakeId> {
    let mut minimum = None;
    for state in self
      .virtual_partition_states
      .values()
      .filter(|state| state.is_assigned())
    {
      let VirtualPartitionState::Recovering { recovery_state, .. } = state else {
        continue;
      };
      if recovery_state.next_window_start_unix_seconds > window_start_unix_seconds
        || window_start_unix_seconds > recovery_state.cutover_window_start_unix_seconds
      {
        continue;
      }
      let min_snowflake = recovery_state.first_window_min_snowflake.filter(|_| {
        recovery_state.first_window_start_unix_seconds == Some(window_start_unix_seconds)
          && recovery_state.next_window_start_unix_seconds == window_start_unix_seconds
      })?;
      minimum = Some(minimum.map_or(min_snowflake, |current: SnowflakeId| {
        current.min(min_snowflake)
      }));
    }
    minimum
  }

  /// Build a scan pass that prioritizes bounded recovery before using the fast path.
  pub(super) fn scan_requests(
    &self,
    now_unix_seconds: i64,
    assigned_partition_ids: &[VirtualPartitionId],
  ) -> Result<(Vec<ScanRequest>, bool)> {
    let mut scan_requests = BTreeMap::new();
    let mut recovery_scan = false;

    // Scan a bounded chronological slice of retention recovery before allowing this partition's
    // fast path. A failure leaves the state untouched, so the same slice is retried.
    let oldest_recovery_window = self
      .virtual_partition_states
      .values()
      .filter(|state| state.is_assigned())
      .filter_map(|state| match state {
        VirtualPartitionState::Recovering { recovery_state, .. } => {
          Some(recovery_state.next_window_start_unix_seconds)
        },
        VirtualPartitionState::PendingCursor { .. }
        | VirtualPartitionState::PendingRecovering { .. }
        | VirtualPartitionState::PendingFast { .. }
        | VirtualPartitionState::Fresh { .. }
        | VirtualPartitionState::Fast { .. } => None,
      })
      .min();
    if let Some(oldest_recovery_window) = oldest_recovery_window {
      let window_size_seconds = consumer_window_size_seconds(&self.config);
      let latest_recovery_window = self
        .virtual_partition_states
        .values()
        .filter(|state| state.is_assigned())
        .filter_map(|state| match state {
          VirtualPartitionState::Recovering { recovery_state, .. } => {
            Some(recovery_state.cutover_window_start_unix_seconds)
          },
          VirtualPartitionState::PendingCursor { .. }
          | VirtualPartitionState::PendingRecovering { .. }
          | VirtualPartitionState::PendingFast { .. }
          | VirtualPartitionState::Fresh { .. }
          | VirtualPartitionState::Fast { .. } => None,
        })
        .max()
        .unwrap_or(oldest_recovery_window);
      for offset in 0 .. MAX_RECOVERY_WINDOWS_PER_SCAN {
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        let window_start =
          oldest_recovery_window.saturating_add(offset.saturating_mul(window_size_seconds));
        if window_start > latest_recovery_window {
          break;
        }
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          window_start,
          self.recovery_scan_min_snowflake(window_start),
          true,
          ScanEligibility {
            recovering: true,
            fast: false,
            fresh: false,
          },
        );
      }
      recovery_scan = true;
    }

    for state in self
      .virtual_partition_states
      .values()
      .filter(|state| state.is_assigned())
    {
      if let VirtualPartitionState::Fresh {
        initial_window_start_unix_seconds,
        ..
      } = state
      {
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          *initial_window_start_unix_seconds,
          None,
          true,
          ScanEligibility {
            recovering: false,
            fast: false,
            fresh: true,
          },
        );
        recovery_scan = true;
      }
    }

    if self
      .virtual_partition_states
      .values()
      .any(|state| state.is_assigned() && matches!(state, VirtualPartitionState::Fast { .. }))
    {
      for (window, safe_floor) in self.eligible_fast_scan_windows(now_unix_seconds)? {
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          window.window_start_unix_seconds,
          Some(self.fast_scan_min_snowflake(
            assigned_partition_ids,
            window.window_start_unix_seconds,
            safe_floor,
          )),
          false,
          ScanEligibility {
            recovering: false,
            fast: true,
            fresh: false,
          },
        );
      }
    }

    Ok((scan_requests.into_values().collect(), recovery_scan))
  }

  /// Discard frontiers for windows that Fast scans no longer query.
  pub(super) fn prune_fast_frontiers(&mut self, now_unix_seconds: i64) -> Result<()> {
    let eligible_window_starts = self
      .eligible_fast_scan_windows(now_unix_seconds)?
      .into_iter()
      .map(|(window, _)| window.window_start_unix_seconds)
      .collect::<HashSet<_>>();
    self
      .fast_frontiers
      .retain(|(_, window_start), _| eligible_window_starts.contains(window_start));
    self
      .metrics
      .record_fast_frontiers(self.fast_frontiers.len());
    Ok(())
  }

  /// Fetch one segment range and decode all of its selected batches in planning order.
  async fn read_segment_plan(
    &self,
    plan: SegmentReadPlan,
  ) -> Result<Vec<(BatchReadCandidate, ConsumerBatch)>> {
    trace!(
      "consumer read segment range start: topic={}, blob_key={}, start={}, end={}, batches={}",
      self.config.topic,
      plan.metadata.blob_key.as_str(),
      plan.byte_range.start,
      plan.byte_range.end,
      plan.candidates.len()
    );

    let blob_read_started_at = Instant::now();
    let payload = self
      .blob_store
      .get_range(&plan.metadata.blob_key, plan.byte_range.clone())
      .await?;
    self
      .metrics
      .record_blob_range(blob_read_started_at, payload.len());
    let selected_bytes = plan.candidates.iter().fold(0_u64, |total, candidate| {
      total.saturating_add(candidate.batch_metadata.byte_range.len())
    });
    self
      .metrics
      .record_blob_batch_ranges(plan.candidates.len(), selected_bytes);

    let mut decoded_batches = Vec::with_capacity(plan.candidates.len());
    for candidate in plan.candidates {
      let batch_range = &candidate.batch_metadata.byte_range;
      let start = batch_range
        .start
        .checked_sub(plan.byte_range.start)
        .ok_or_else(|| anyhow!("batch range starts before its segment read range"))?;
      let end = batch_range
        .end
        .checked_sub(plan.byte_range.start)
        .ok_or_else(|| anyhow!("batch range ends before its segment read range"))?;
      let start =
        usize::try_from(start).map_err(|_| anyhow!("batch range start does not fit in memory"))?;
      let end =
        usize::try_from(end).map_err(|_| anyhow!("batch range end does not fit in memory"))?;
      ensure!(
        start < end && end <= payload.len(),
        "batch range is outside fetched segment range: start={start}, end={end}, fetched_bytes={}",
        payload.len()
      );
      let batch_payload = payload.slice(start .. end);
      let batch = self.decode_batch(
        &plan.metadata,
        &candidate.batch_metadata,
        candidate.virtual_partition_id,
        batch_payload,
      )?;
      decoded_batches.push((candidate, batch));
    }

    Ok(decoded_batches)
  }

  /// Decompress, validate, and decode one batch payload supplied by a segment range read.
  fn decode_batch(
    &self,
    metadata: &SegmentMetadata,
    batch_metadata: &BatchMetadata,
    virtual_partition_id: VirtualPartitionId,
    payload: bytes::Bytes,
  ) -> Result<ConsumerBatch> {
    trace!(
      "consumer decode batch: topic={}, partition={}, blob_key={}, seq_start={}, seq_end={}",
      self.config.topic,
      virtual_partition_id,
      metadata.blob_key.as_str(),
      batch_metadata.seq_range.start,
      batch_metadata.seq_range.end
    );

    // Decode in two stages: transport/storage compression first, then logical RecordBatch format.
    let decoded = match metadata.compression.codec {
      CompressionCodec::None => payload,
      CompressionCodec::Zstd => zstd::stream::decode_all(Cursor::new(payload))
        .map(bytes::Bytes::from)
        .map_err(|error| anyhow!("failed to decode zstd batch: {error}"))?,
    };

    let record_batch = StoredRecordBatch::parse_from_tokio_bytes(&decoded)
      .map_err(|error| anyhow!("failed to decode record batch protobuf: {error}"))?;

    // Defensive integrity check: segment index entry and decoded payload must agree on partition.
    ensure!(
      record_batch.virtual_partition_id == virtual_partition_id,
      "decoded batch partition {} does not match expected {}",
      record_batch.virtual_partition_id,
      virtual_partition_id
    );

    let payload_bytes = record_batch.records.iter().fold(0_usize, |total, record| {
      total.saturating_add(record.payload.len())
    });
    self
      .metrics
      .record_batch(record_batch.records.len(), payload_bytes);

    Ok(ConsumerBatch {
      virtual_partition_id,
      seq_range: batch_metadata.seq_range.clone(),
      source_checkpoint: CommittedSourceCheckpoint {
        window_start_unix_seconds: metadata.window.window_start_unix_seconds,
        snowflake_id: metadata.snowflake_id.as_u64(),
      },
      records: record_batch.records,
    })
  }
}
