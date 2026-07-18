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
};
use crate::config::{
  consumer_candidate_window_count,
  consumer_metadata_visibility_delay_ms,
  consumer_window_size_seconds,
};
use anyhow::{Error, Result, anyhow, ensure};
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
};
use futures::future::try_join_all;
use log::{info, trace};
use protobuf::Message;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

//
// ScanRequest
//

/// One metadata-window request, with all reader modes that require its results.
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

/// Reader modes allowed to consume a request's metadata results.
#[derive(Clone, Copy)]
pub(super) struct ScanEligibility {
  pub(super) recovering: bool,
  pub(super) fast: bool,
  pub(super) fresh: bool,
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

  /// Execute one metadata scan pass, then advance scan modes only for windows fully observed.
  pub(super) async fn read_available_impl(
    &mut self,
    now_unix_seconds: i64,
  ) -> Result<Vec<ConsumerBatch>> {
    let read_started_at = Instant::now();
    trace!(
      "consumer read_available start: topic={}, assigned_partitions={}",
      self.config.topic,
      self.assigned_virtual_partition_ids().len()
    );
    // Output contains only newly consumable batches according to per-partition cursor state.
    let mut output = Vec::new();
    let mut metadata_batches_scanned = 0_usize;
    let mut metadata_batches_skipped_by_cursor = 0_usize;
    let visibility_cutoff_ts_ms = now_unix_seconds.saturating_mul(1_000).saturating_sub(
      i64::try_from(consumer_metadata_visibility_delay_ms(&self.config)).unwrap_or(i64::MAX),
    );

    // Recovery scans catch late lower-snowflake rows; fast scans avoid rereading old metadata.
    let (scan_requests, recovery_scan) = self.scan_requests(now_unix_seconds)?;
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
          .await?;
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
    let mut deferred_recovery_windows = HashSet::new();
    let mut deferred_fresh_partitions = HashSet::new();
    for (request, mut segments) in window_results {
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
        for partition_id in self.assigned_virtual_partition_ids() {
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
          let Some(partition_batches) = segment.segment_index.get(&partition_id) else {
            continue;
          };
          segment_has_assigned_batches = true;

          let frontier_key = (partition_id, window.window_start_unix_seconds);
          let fast_partition = matches!(partition_state, Some(VirtualPartitionState::Fast { .. }));
          if fast_partition {
            if blocked_fast_sources.contains(&frontier_key) {
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
              continue;
            }
          }

          if segment.metadata_published_ts_ms > visibility_cutoff_ts_ms {
            self
              .metrics
              .metadata_segments_deferred_by_visibility_delay
              .inc();
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
            } else {
              deferred_recovery_windows.insert(window.window_start_unix_seconds);
            }
            continue;
          }

          metadata_batches_scanned =
            metadata_batches_scanned.saturating_add(partition_batches.len());
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
              continue;
            }

            // Decode batch payload only after passing cursor filter to avoid unnecessary I/O.
            let batch = self
              .read_batch(&segment, batch_metadata, partition_id)
              .await?;

            // Cursor always moves forward. max() keeps monotonicity if metadata ordering is odd.
            let next_cursor = batch.seq_range.end.max(current_cursor.unwrap_or(0));
            if let Some(state) = self.virtual_partition_states.get_mut(&partition_id) {
              state.advance_cursor(next_cursor);
            }
            trace!(
              "consumer accepted batch: topic={}, partition={}, seq_start={}, seq_end={}, \
               records={}, new_cursor={}",
              self.config.topic,
              partition_id,
              batch.seq_range.start,
              batch.seq_range.end,
              batch.records.len(),
              next_cursor
            );
            output.push(batch);
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
      }
    }

    let window_size_seconds = consumer_window_size_seconds(&self.config);
    let mut initial_scans_completed = Vec::new();
    let mut recoveries_completed = Vec::new();
    for (partition_id, state) in &mut self.virtual_partition_states {
      let mut next_state = None;
      match state {
        VirtualPartitionState::Fresh {
          cursor,
          initial_window_start_unix_seconds,
        } if scanned_fresh_window_starts.contains(initial_window_start_unix_seconds)
          && !deferred_fresh_partitions.contains(partition_id) =>
        {
          initial_scans_completed.push((*partition_id, *initial_window_start_unix_seconds));
          next_state = Some(VirtualPartitionState::Fast { cursor: *cursor });
        },
        VirtualPartitionState::Recovering {
          cursor,
          recovery_state,
        } => {
          let Some(recovery_window_end) = recovery_window_end else {
            continue;
          };
          let recovery_window_end =
            deferred_recovery_windows
              .iter()
              .min()
              .map_or(recovery_window_end, |deferred_window| {
                recovery_window_end
                  .min(deferred_window.saturating_sub(consumer_window_size_seconds(&self.config)))
              });
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
            next_state = Some(VirtualPartitionState::Fast { cursor: *cursor });
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

  /// Find the least inclusive frontier required when one query serves several fast partitions.
  fn fast_scan_min_snowflake(&self, window_start_unix_seconds: i64) -> Option<SnowflakeId> {
    let mut minimum = None;
    for partition_id in self.assigned_virtual_partition_ids() {
      if !matches!(
        self.virtual_partition_states.get(&partition_id),
        Some(VirtualPartitionState::Fast { .. })
      ) {
        continue;
      }
      let frontier = self
        .fast_frontiers
        .get(&(partition_id, window_start_unix_seconds))
        .copied()?;
      minimum = Some(minimum.map_or(frontier, |current: SnowflakeId| current.min(frontier)));
    }
    minimum
  }

  /// Build a scan pass that prioritizes bounded recovery before using the fast path.
  pub(super) fn scan_requests(&self, now_unix_seconds: i64) -> Result<(Vec<ScanRequest>, bool)> {
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
          None,
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
      for window in self.scan_windows(now_unix_seconds)? {
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          window.window_start_unix_seconds,
          self.fast_scan_min_snowflake(window.window_start_unix_seconds),
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

  /// Discard frontiers outside the bounded fast-scan horizon to cap reader-local state.
  pub(super) fn prune_fast_frontiers(&mut self, now_unix_seconds: i64) -> Result<()> {
    let windows = self.scan_windows(now_unix_seconds)?;
    if let Some(oldest_window) = windows.first() {
      self
        .fast_frontiers
        .retain(|(_, window_start), _| *window_start >= oldest_window.window_start_unix_seconds);
    }
    self
      .metrics
      .record_fast_frontiers(self.fast_frontiers.len());
    Ok(())
  }

  /// Fetch, decompress, validate, and decode the byte range referenced by one segment batch.
  pub(super) async fn read_batch(
    &self,
    metadata: &SegmentMetadata,
    batch_metadata: &BatchMetadata,
    virtual_partition_id: VirtualPartitionId,
  ) -> Result<ConsumerBatch> {
    trace!(
      "consumer read batch start: topic={}, partition={}, blob_key={}, seq_start={}, seq_end={}",
      self.config.topic,
      virtual_partition_id,
      metadata.blob_key.as_str(),
      batch_metadata.seq_range.start,
      batch_metadata.seq_range.end
    );
    // Pull only the referenced byte range for this batch to avoid downloading full segments.
    let blob_read_started_at = Instant::now();
    let payload = self
      .blob_store
      .get_range(
        &metadata.blob_key,
        ByteRange {
          start: batch_metadata.byte_range.start,
          end: batch_metadata.byte_range.end,
        },
      )
      .await?;
    self
      .metrics
      .record_blob_range(blob_read_started_at, payload.len());

    // Decode in two stages: transport/storage compression first, then logical RecordBatch format.
    let decompression_started_at = Instant::now();
    let decoded: Cow<'_, [u8]> = match batch_metadata.compression.codec {
      CompressionCodec::None => Cow::Borrowed(payload.as_ref()),
      CompressionCodec::Zstd => zstd::stream::decode_all(Cursor::new(payload.as_ref()))
        .map(Cow::Owned)
        .map_err(|error| anyhow!("failed to decode zstd batch: {error}"))?,
    };
    if matches!(batch_metadata.compression.codec, CompressionCodec::Zstd) {
      self
        .metrics
        .decompression_latency_seconds
        .observe(decompression_started_at.elapsed().as_secs_f64());
    }

    let protobuf_decode_started_at = Instant::now();
    let record_batch = StoredRecordBatch::parse_from_bytes(decoded.as_ref())
      .map_err(|error| anyhow!("failed to decode record batch protobuf: {error}"))?;
    self
      .metrics
      .protobuf_decode_latency_seconds
      .observe(protobuf_decode_started_at.elapsed().as_secs_f64());

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
