use super::{
  Arc,
  BatchReadCandidate,
  BatchReadResult,
  ConsumerReadRuntimeSettings,
  ConsumerReaderFastFrontierState,
  ConsumerReaderImpl,
  ConsumerReaderPartitionScanState,
  Context,
  Error,
  HashMap,
  HashSet,
  Instant,
  ReadCapacity,
  Result,
  SegmentMetadata,
  SegmentReadPlan,
  SnowflakeId,
  StreamExt,
  TryStreamExt,
  VirtualPartitionId,
  VirtualPartitionState,
  consumer_window_size_seconds,
  debug,
  format_unix_timestamp_ms,
  format_unix_timestamp_seconds,
  info,
  stream,
  system_now_unix_seconds,
  trace,
  try_join_all,
};
use crate::consumer::ConsumerReadOutcome;
use bd_log_util::warn_every;
use blob_stream_metadata_store::MetadataReadConsistency;
use time::ext::NumericalDuration;

impl ConsumerReaderImpl {
  /// Execute one metadata scan pass, retaining no progress when a batch cannot reach the caller.
  pub(in crate::consumer) async fn read_available_impl(
    &mut self,
    now_unix_seconds: i64,
    capacity: ReadCapacity,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<ConsumerReadOutcome> {
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
  pub(in crate::consumer) async fn read_available_impl_once(
    &mut self,
    now_unix_seconds: i64,
    mut capacity: ReadCapacity,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<ConsumerReadOutcome> {
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
    // Metadata publication timestamps have millisecond precision while reader passes use whole
    // seconds. Keep the visibility comparison in milliseconds, then round retry deadlines up so
    // a worker never wakes in the second before a row becomes eligible.
    let visibility_delay_ms =
      i64::try_from(runtime_settings.metadata_visibility_delay_ms).unwrap_or(i64::MAX);
    let visibility_cutoff_ts_ms = now_unix_seconds
      .saturating_mul(1_000)
      .saturating_sub(visibility_delay_ms);
    // A pass can defer several sources. The worker needs only the first safe retry, not a
    // per-source timer, because rescanning then will reconsider every deferred source.
    let mut next_visibility_eligible_unix_seconds = None;

    // Requests merge partitions sharing a metadata window. Recovery scans catch late
    // lower-snowflake rows, while fast scans avoid rereading metadata outside its visibility
    // horizon; per-partition filtering is reapplied after each shared query.
    let (scan_requests, recovery_scan) =
      self.scan_requests(now_unix_seconds, &assigned_partition_ids, runtime_settings)?;
    for request in &scan_requests {
      trace!(
        "consumer metadata scan planned: topic={}, window_start={}, recovery={}, fast={}, \
         fresh={}, min_snowflake={:?}",
        request.window.topic,
        format_unix_timestamp_seconds(request.window.window_start_unix_seconds),
        !request.eligibility.recovering_partitions.is_empty(),
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
              ConsumerReaderPartitionScanState::new(
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
      runtime_settings,
      &mut scan_states,
    );
    let mut recovery_window_ends = HashMap::<VirtualPartitionId, i64>::new();
    for request in &scan_requests {
      for &partition_id in &request.eligibility.recovering_partitions {
        recovery_window_ends
          .entry(partition_id)
          .and_modify(|window_end| {
            *window_end = (*window_end).max(request.window.window_start_unix_seconds);
          })
          .or_insert(request.window.window_start_unix_seconds);
      }
    }
    let scanned_fresh_window_starts = scan_requests
      .iter()
      .filter(|request| request.eligibility.fresh)
      .map(|request| request.window.window_start_unix_seconds)
      .collect::<Vec<_>>();
    let mut cached_window_results = Vec::new();
    let mut scan_futures = Vec::new();
    for (request_index, request) in scan_requests.iter().cloned().enumerate() {
      let cache_key =
        self.mature_recovery_metadata_cache_key(&request, now_unix_seconds, runtime_settings);
      if let Some(cache_key) = cache_key
        && let Some(segments) = self.recovery_metadata_cache.get(&cache_key)
      {
        self.metrics.record_recovery_metadata_cache_hit();
        if let Some(scan_state) = scan_states.get_mut(&cache_key.0) {
          scan_state.recovery_metadata_cache_hits =
            scan_state.recovery_metadata_cache_hits.saturating_add(1);
        }
        trace!(
          "consumer reused cached recovery metadata: topic={}, partition={}, window_start={}, \
           segments={}",
          self.config.topic,
          cache_key.0,
          format_unix_timestamp_seconds(request.window.window_start_unix_seconds),
          segments.len()
        );
        cached_window_results.push((request_index, request, Arc::clone(segments)));
        continue;
      }
      if let Some(cache_key) = cache_key {
        self.metrics.record_recovery_metadata_cache_miss();
        if let Some(scan_state) = scan_states.get_mut(&cache_key.0) {
          scan_state.recovery_metadata_cache_misses =
            scan_state.recovery_metadata_cache_misses.saturating_add(1);
        }
      }
      let metadata_store = Arc::clone(&self.metadata_store);
      let metrics = self.metrics.clone();
      let metadata_read_consistency = runtime_settings.metadata_read_consistency;
      scan_futures.push(async move {
        if !request.recovery_scan && request.eligibility.fast && request.min_snowflake.is_none() {
          metrics.metadata_fast_scan_without_lower_bound.inc();
        }
        let scan_started_at = Instant::now();
        let segments = metadata_store
          .scan_window_from_snowflake(
            &request.window,
            request.min_snowflake,
            metadata_read_consistency,
          )
          .await
          .with_context(|| {
            format!(
              "consumer metadata scan failed: topic={}, window_start={}, recovery={}, fast={}, \
               fresh={}, min_snowflake={:?}",
              request.window.topic,
              format_unix_timestamp_seconds(request.window.window_start_unix_seconds),
              !request.eligibility.recovering_partitions.is_empty(),
              request.eligibility.fast,
              request.eligibility.fresh,
              request.min_snowflake.map(SnowflakeId::as_u64)
            )
          })?;
        metrics.record_metadata_scan(scan_started_at, segments.len(), request.recovery_scan);
        Ok::<_, Error>((request_index, request, segments, cache_key))
      });
    }

    // Run independent per-window metadata queries concurrently while preserving input order.
    let queried_window_results = match try_join_all(scan_futures).await {
      Ok(window_results) => window_results,
      Err(error) => {
        if recovery_scan {
          self.metrics.metadata_recovery_scan_failures.inc();
        }
        return Err(error);
      },
    };
    for (request_index, request, segments, cache_key) in queried_window_results {
      let segments = if let Some(cache_key) = cache_key {
        let partition_id = cache_key.0;
        let has_visibility_deferred_segment = segments.iter().any(|segment| {
          segment.segment_index.contains_key(&partition_id)
            && segment.metadata_published_ts_ms > visibility_cutoff_ts_ms
        });
        if has_visibility_deferred_segment {
          let mut segments = segments;
          segments.sort_by_key(|metadata| metadata.snowflake_id);
          segments.into()
        } else {
          let mut cached_segments = segments
            .into_iter()
            .filter_map(|mut segment| {
              let partition_batches = segment.segment_index.remove(&partition_id)?;
              segment.segment_index.clear();
              segment
                .segment_index
                .insert(partition_id, partition_batches);
              Some(segment)
            })
            .collect::<Vec<_>>();
          cached_segments.sort_by_key(|metadata| metadata.snowflake_id);
          let cached_segments: Arc<[SegmentMetadata]> = cached_segments.into();
          trace!(
            "consumer cached mature recovery metadata: topic={}, partition={}, window_start={}, \
             segments={}",
            self.config.topic,
            partition_id,
            format_unix_timestamp_seconds(request.window.window_start_unix_seconds),
            cached_segments.len()
          );
          self
            .recovery_metadata_cache
            .insert(cache_key, Arc::clone(&cached_segments));
          self.metrics.record_recovery_metadata_cache_insert();
          cached_segments
        }
      } else {
        let mut segments = segments;
        segments.sort_by_key(|metadata| metadata.snowflake_id);
        segments.into()
      };
      cached_window_results.push((request_index, request, segments));
    }
    self.record_recovery_metadata_cache_state();
    cached_window_results.sort_by_key(|(request_index, ..)| *request_index);
    let window_results = cached_window_results
      .into_iter()
      .map(|(_, request, segments)| (request, segments))
      .collect::<Vec<_>>();

    let mut next_fast_frontiers = self.fast_frontiers.clone();
    let mut blocked_fast_sources = HashSet::new();
    // A deferred recovery window must hold back later windows for the same partition. Otherwise a
    // later sequence could advance the cursor and make the deferred batch permanently ineligible.
    let mut blocked_recovering_partitions = HashSet::new();
    let mut deferred_recovery_windows = HashMap::<VirtualPartitionId, i64>::new();
    let mut capacity_deferred_recovery_windows = HashMap::<VirtualPartitionId, i64>::new();
    let mut capacity_deferred_fresh_window_starts = HashSet::new();
    // Recovery can hand an active publication-horizon window to Fast. Fast retains the same
    // visibility safety check and replays its inclusive time floor once the row becomes eligible.
    let fast_horizon_windows = self
      .eligible_fast_scan_windows(now_unix_seconds, runtime_settings)?
      .into_iter()
      .map(|(window, _)| window.window_start_unix_seconds)
      .collect::<HashSet<_>>();
    let mut deferred_fresh_partitions = HashSet::new();
    let mut segment_read_plans = Vec::new();
    let mut capacity_exhausted = false;
    'windows: for (request_index, (request, segments)) in window_results.into_iter().enumerate() {
      let window = request.window;
      trace!(
        "consumer scanned window: topic={}, window_start={}, segments={}",
        window.topic,
        format_unix_timestamp_seconds(window.window_start_unix_seconds),
        segments.len()
      );

      for segment in segments.iter() {
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
              request
                .eligibility
                .recovering_partitions
                .contains(&partition_id)
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
            // The shared DynamoDB lower bound is the least restrictive partition bound. Reapply
            // each partition's inclusive frontier here so a sparse partition cannot be skipped
            // because another partition in the same query is further ahead.
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

          // Strong DynamoDB reads return committed metadata immediately. The publication horizon
          // still plans rescans for rows that have not been written yet, but returned rows need
          // no replica-visibility delay.
          if runtime_settings.metadata_read_consistency == MetadataReadConsistency::Eventual
            && segment.metadata_published_ts_ms > visibility_cutoff_ts_ms
          {
            self
              .metrics
              .metadata_segments_deferred_by_visibility_delay
              .inc();
            scan_state.metadata_segments_deferred_by_visibility = scan_state
              .metadata_segments_deferred_by_visibility
              .saturating_add(1);
            // `metadata_published_ts_ms + delay` is the exact eligibility time. The reader
            // accepts only whole-second `now` values, so ceiling division avoids a premature
            // retry when that exact time falls inside a later second.
            let visibility_eligible_unix_seconds = segment
              .metadata_published_ts_ms
              .saturating_add(visibility_delay_ms)
              .saturating_add(999)
              / 1_000;
            next_visibility_eligible_unix_seconds = Some(
              next_visibility_eligible_unix_seconds
                .map_or(visibility_eligible_unix_seconds, |current: i64| {
                  current.min(visibility_eligible_unix_seconds)
                }),
            );
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
              deferred_recovery_windows
                .entry(partition_id)
                .and_modify(|deferred_window| {
                  *deferred_window = (*deferred_window).min(window.window_start_unix_seconds);
                })
                .or_insert(window.window_start_unix_seconds);
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
            // Advance only after the segment passed visibility and processing checks. The
            // inclusive value replays the boundary row on the next scan, while max prevents an
            // unordered metadata result from regressing the observed frontier.
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
          segment_read_plans.push(SegmentReadPlan::new(
            segment.clone(),
            segment_read_candidates,
          )?);
        }
        if capacity_exhausted {
          // A capacity stop leaves the current request only partially observed and prevents every
          // later request from being processed. Keep all affected modes at their first incomplete
          // window so a later pass cannot skip unread batches by advancing to Fast.
          for deferred_request in scan_requests.iter().skip(request_index) {
            for &partition_id in &deferred_request.eligibility.recovering_partitions {
              capacity_deferred_recovery_windows
                .entry(partition_id)
                .and_modify(|deferred_window| {
                  *deferred_window =
                    (*deferred_window).min(deferred_request.window.window_start_unix_seconds);
                })
                .or_insert(deferred_request.window.window_start_unix_seconds);
            }
            if deferred_request.eligibility.fresh {
              capacity_deferred_fresh_window_starts
                .insert(deferred_request.window.window_start_unix_seconds);
            }
          }
          break 'windows;
        }
      }
    }

    // `buffered` preserves segment-plan order while allowing independent object-store requests
    // and decoding work to overlap. Cursor changes occur only after every planned read succeeds.
    let mut batch_read_results = stream::iter(
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
    batch_read_results.sort_by_key(|result| match result {
      BatchReadResult::Decoded { candidate, .. } | BatchReadResult::Missing { candidate, .. } => (
        candidate.virtual_partition_id,
        candidate.batch_metadata.seq_range.start,
      ),
    });

    for result in batch_read_results {
      let candidate = match &result {
        BatchReadResult::Decoded { candidate, .. } | BatchReadResult::Missing { candidate, .. } => {
          candidate
        },
      };
      let current_cursor = self
        .virtual_partition_states
        .get(&candidate.virtual_partition_id)
        .and_then(VirtualPartitionState::cursor);
      if current_cursor.is_some_and(|cursor| candidate.batch_metadata.seq_range.end <= cursor) {
        metadata_batches_skipped_by_cursor = metadata_batches_skipped_by_cursor.saturating_add(1);
        if let Some(scan_state) = scan_states.get_mut(&candidate.virtual_partition_id) {
          scan_state.metadata_batches_skipped_by_cursor = scan_state
            .metadata_batches_skipped_by_cursor
            .saturating_add(1);
        }
        continue;
      }

      match result {
        BatchReadResult::Decoded { candidate, batch } => {
          // Cursor always moves forward. max() keeps monotonicity if metadata ordering is odd.
          let next_cursor = batch.seq_range.end.max(current_cursor.unwrap_or(0));
          if let Some(state) = self
            .virtual_partition_states
            .get_mut(&candidate.virtual_partition_id)
          {
            state.advance_cursor(next_cursor);
          }
          trace!(
            "consumer accepted batch: topic={}, partition={}, seq_start={}, seq_end={}, \
             records={}, new_cursor={}",
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
        },
        BatchReadResult::Missing {
          candidate,
          blob_key,
        } => {
          let next_cursor = candidate
            .batch_metadata
            .seq_range
            .end
            .max(current_cursor.unwrap_or(0));
          if let Some(state) = self
            .virtual_partition_states
            .get_mut(&candidate.virtual_partition_id)
          {
            state.advance_cursor(next_cursor);
          }
          let lost_records = candidate.batch_metadata.seq_range.len();
          self.metrics.record_lost_records(lost_records);
          warn_every!(
            15.seconds(),
            "consumer skipped lost blob range: topic={}, blob_key={}, partition={}, seq_start={}, \
             seq_end={}, records={}, new_cursor={}",
            self.config.topic,
            blob_key.as_str(),
            candidate.virtual_partition_id,
            candidate.batch_metadata.seq_range.start,
            candidate.batch_metadata.seq_range.end,
            lost_records,
            next_cursor
          );
        },
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
          ..
        } if scanned_fresh_window_starts.contains(initial_window_start_unix_seconds)
          && !deferred_fresh_partitions.contains(partition_id)
          && !capacity_deferred_fresh_window_starts.contains(initial_window_start_unix_seconds) =>
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
          let Some(recovery_window_end) = recovery_window_ends.get(partition_id).copied() else {
            continue;
          };
          let recovery_window_end = deferred_recovery_windows.get(partition_id).map_or(
            recovery_window_end,
            |deferred_window| {
              recovery_window_end
                .min(deferred_window.saturating_sub(consumer_window_size_seconds(&self.config)))
            },
          );
          let recovery_window_end = capacity_deferred_recovery_windows.get(partition_id).map_or(
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

    self
      .recovery_metadata_cache
      .retain(|(partition_id, window_start, _), _| {
        matches!(
          self.virtual_partition_states.get(partition_id),
          Some(VirtualPartitionState::Recovering { recovery_state, .. })
            if recovery_state.next_window_start_unix_seconds <= *window_start
        )
      });
    self.record_recovery_metadata_cache_state();

    trace!(
      "consumer read_available complete: topic={}, output_batches={}, \
       metadata_batches_scanned={}, metadata_batches_skipped_by_cursor={}",
      self.config.topic,
      output.len(),
      metadata_batches_scanned,
      metadata_batches_skipped_by_cursor
    );

    self.fast_frontiers = next_fast_frontiers;
    self.prune_fast_frontiers(now_unix_seconds, runtime_settings)?;
    let completed_at_unix_seconds = system_now_unix_seconds();
    for ((partition_id, window_start_unix_seconds), snowflake_id) in &self.fast_frontiers {
      if let Some(scan_state) = scan_states.get_mut(partition_id) {
        scan_state
          .fast_frontiers
          .push(ConsumerReaderFastFrontierState {
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
         recovery_segments_blocked_by_visibility={}, recovery_metadata_cache_hits={}, \
         recovery_metadata_cache_misses={}, metadata_batches_deferred_by_capacity={}, \
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
        scan_state.recovery_metadata_cache_hits,
        scan_state.recovery_metadata_cache_misses,
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

    // A visibility deadline is useful only when it is the sole reason this successful pass has
    // no work. Ready output must keep the refill loop hot, and a capacity stop means unscanned
    // metadata could be ready now, so either case falls back to normal worker behavior.
    Ok(ConsumerReadOutcome {
      next_visibility_eligible_unix_seconds: output
        .is_empty()
        .then_some(next_visibility_eligible_unix_seconds)
        .flatten()
        .filter(|deadline| !capacity_exhausted && *deadline > now_unix_seconds),
      batches: output,
    })
  }
}
