use super::planning::FAST_GAP_PROBE_INTERVAL;
use super::{
  Arc,
  BatchReadCandidate,
  BatchReadResult,
  ConsumerBatch,
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
  ScanRequest,
  SegmentMetadata,
  SegmentReadPlan,
  SnowflakeId,
  StreamExt,
  TryStreamExt,
  VirtualPartitionId,
  VirtualPartitionState,
  debug,
  info,
  offset_datetime_from_unix_seconds,
  stream,
  system_now_unix_seconds,
  trace,
  try_join_all,
};
use crate::consumer::diagnostics::{ConsumerReaderMetadataSource, MAX_METADATA_SOURCE_DETAILS};
use crate::consumer::metadata_query::decode_metadata_response;
use crate::consumer::{
  BrokerBlobRangeQuery,
  BrokerBlobRangeRead,
  ConsumerReadOutcome,
  decode_blob_range_response_for_ranges,
};
use crate::diagnostics::reader_partition_scan_snapshot;
use bd_log_util::warn_every;
use blob_stream_blob_store::BlobKey;
use blob_stream_metadata_store::MetadataReadConsistency;
use blob_stream_proto::protos::blobstream::v1::broker::{
  BlobRangeRequest,
  FullRecoveryMetadataCoverage,
  MetadataPartitionBound,
  MetadataReadConsistency as BrokerReadConsistency,
  ReadBlobRangesRequest,
  ReadMetadataWindowRequest,
  TailMetadataCoverage,
  read_metadata_window_request,
};
use blob_stream_types::Window;
use time::ext::NumericalDuration;
use tokio::sync::Semaphore;

//
// PartitionScanFinalization
//

/// Scan-pass state that affects one partition's lifecycle transition after selected reads finish.
#[derive(Default)]
struct PartitionScanFinalization {
  recovery_window_end: Option<i64>,
  visibility_deferred_recovery_window: Option<i64>,
  capacity_deferred_recovery_window: Option<i64>,
  fresh_deferred_by_visibility: bool,
}

//
// ScanPassFinalization
//

/// State calculated during window selection and consumed after all selected reads complete.
struct ScanPassFinalization {
  scanned_fresh_window_starts: Vec<i64>,
  partitions: HashMap<VirtualPartitionId, PartitionScanFinalization>,
  capacity_deferred_fresh_window_starts: HashSet<i64>,
  next_fast_frontiers: HashMap<(VirtualPartitionId, i64), SnowflakeId>,
}

//
// BatchAcceptance
//

/// Batches admitted to delivery plus state that must remain incomplete for held Fast ranges.
struct BatchAcceptance {
  batches: Vec<ConsumerBatch>,
  next_metadata_eligible_at: Option<time::OffsetDateTime>,
  held_fast_sources: HashSet<(VirtualPartitionId, i64)>,
}

impl ConsumerReaderImpl {
  /// Load metadata for every planned window, preserving plan order across cache hits and queries.
  ///
  /// Only mature per-partition Recovery results and Fast rollover-tail entries are cached. They
  /// are immutable by the time they become eligible for this cache, while a visibility-deferred
  /// result must be queried again so a later pass can observe rows that were not yet safe to
  /// consume.
  async fn materialize_window_results(
    &mut self,
    scan_requests: &[ScanRequest],
    recovery_scan: bool,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
    visibility_cutoff: time::OffsetDateTime,
    scan_states: &mut HashMap<VirtualPartitionId, ConsumerReaderPartitionScanState>,
  ) -> Result<Vec<(ScanRequest, Arc<[SegmentMetadata]>, time::OffsetDateTime)>> {
    let mut cached_window_results = Vec::new();
    let mut scan_futures = Vec::new();
    for (request_index, request) in scan_requests.iter().cloned().enumerate() {
      let cache_keys = self.mature_metadata_cache_keys(&request, now, runtime_settings);
      // A merged Fast-tail query is stored as independently filtered entries because later scan
      // passes might need a subset of its partitions. Reuse is deliberately all-or-nothing:
      // returning only the entries that happen to be present would mark the request complete
      // while silently skipping uncached partitions. On a partial hit, query once and refresh
      // every participating entry together below.
      if !cache_keys.is_empty()
        && let Some(cached_segments) = cache_keys
          .iter()
          .map(|cache_key| self.recovery_metadata_cache.get(cache_key).cloned())
          .collect::<Option<Vec<_>>>()
      {
        // Cached entries no longer retain the other partitions from their original request.
        // Rebuild that response shape here so the normal filtering, frontier, and capacity code
        // cannot distinguish a cache hit from a direct metadata-store result.
        let mut merged_segments = cached_segments
          .into_iter()
          .flat_map(|segments| segments.iter().cloned().collect::<Vec<_>>())
          .collect::<Vec<_>>();
        merged_segments.sort_by_key(|metadata| metadata.snowflake_id);
        for cache_key in &cache_keys {
          self.metrics.record_mature_metadata_cache_reuse();
          if let Some(scan_state) = scan_states.get_mut(&cache_key.0) {
            scan_state.mature_metadata_cache_reuses =
              scan_state.mature_metadata_cache_reuses.saturating_add(1);
          }
        }
        trace!(
          "consumer reused cached mature metadata: topic={}, partitions={:?}, window_start={}, \
           segments={}",
          self.config.topic,
          cache_keys.iter().map(|key| key.0).collect::<Vec<_>>(),
          offset_datetime_from_unix_seconds(request.window.window_start_unix_seconds),
          merged_segments.len()
        );
        cached_window_results.push((
          request_index,
          request,
          merged_segments.into(),
          visibility_cutoff,
        ));
        continue;
      }
      let metadata_store = Arc::clone(&self.metadata_store);
      let broker_metadata_query = Arc::clone(&self.broker_metadata_query);
      let metrics = self.metrics.clone();
      let metadata_read_consistency = runtime_settings.metadata_read_consistency;
      let metadata_cache_max_age = self.metadata_cache_max_age;
      let time_provider = Arc::clone(&self.time_provider);
      let broker_request = broker_request_for_scan(&request, metadata_read_consistency);
      scan_futures.push(async move {
        if !request.recovery_scan && request.eligibility.fast && request.min_snowflake.is_none() {
          metrics.metadata_fast_scan_without_lower_bound.inc();
        }
        let scan_started_at = Instant::now();
        let broker_attempted = broker_request.is_some();
        let broker_result = if let Some(broker_request) = broker_request {
          metrics.record_broker_metadata_offload_request();
          match broker_metadata_query
            .read_metadata_window(broker_request.clone())
            .await
          {
            Ok(response) => {
              // The cache-age contract is measured at receipt, not at scan start. A broker RPC
              // can itself consume the allowed age, particularly when tests use a manual clock.
              let received_at = time_provider.now();
              match decode_metadata_response(
                &broker_request,
                response,
                received_at,
                metadata_cache_max_age,
              ) {
                Ok(result) => Some(result),
                Err(error) => {
                  trace!(
                    "consumer broker metadata response rejected; falling back direct: {error}"
                  );
                  None
                },
              }
            },
            Err(error) => {
              trace!("consumer broker metadata query failed; falling back direct: {error}");
              None
            },
          }
        } else {
          None
        };
        let (segments, result_visibility_cutoff) = if let Some(result) = broker_result {
          metrics.record_broker_metadata_offload_delivery();
          (
            result.segments,
            result
              .observed_at
              .saturating_sub(runtime_settings.metadata_visibility_delay),
          )
        } else {
          if broker_attempted {
            metrics.record_broker_metadata_offload_fallback();
          }
          (
            scan_direct_metadata(&metadata_store, &request, metadata_read_consistency).await?,
            visibility_cutoff,
          )
        };
        metrics.record_metadata_scan(scan_started_at, segments.len(), request.recovery_scan);
        Ok::<_, Error>((
          request_index,
          request,
          segments,
          cache_keys,
          result_visibility_cutoff,
        ))
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
    for (request_index, request, segments, cache_keys, result_visibility_cutoff) in
      queried_window_results
    {
      // Eventual-consistency results that contain rows newer than the cutoff are incomplete by
      // definition. Do not cache them: a later retry must query again to observe the rows that
      // were not yet safe to consume. Cached mature windows, in contrast, are immutable.
      let has_visibility_deferred_segment = runtime_settings.metadata_read_consistency
        == MetadataReadConsistency::Eventual
        && cache_keys.iter().any(|cache_key| {
          segments.iter().any(|segment| {
            segment.segment_index.contains_key(&cache_key.0)
              && segment.metadata_published_at > result_visibility_cutoff
          })
        });
      let mut segments = segments;
      segments.sort_by_key(|metadata| metadata.snowflake_id);
      if !cache_keys.is_empty() && !has_visibility_deferred_segment {
        // Preserve the full response for this pass, but cache a copy containing only one
        // partition's batches for each identity. The later cache-hit path merges those copies
        // back into a response in Snowflake order. Clearing the original index avoids retaining
        // unrelated partition batches in an entry that may be reused on its own.
        for cache_key in cache_keys {
          let partition_id = cache_key.0;
          let cached_segments = segments
            .iter()
            .cloned()
            .filter_map(|mut segment| {
              let partition_batches = segment.segment_index.remove(&partition_id)?;
              segment.segment_index.clear();
              segment
                .segment_index
                .insert(partition_id, partition_batches);
              Some(segment)
            })
            .collect::<Vec<_>>();
          let cached_segments: Arc<[SegmentMetadata]> = cached_segments.into();
          trace!(
            "consumer cached mature metadata: topic={}, partition={}, window_start={}, segments={}",
            self.config.topic,
            partition_id,
            offset_datetime_from_unix_seconds(request.window.window_start_unix_seconds),
            cached_segments.len()
          );
          self
            .recovery_metadata_cache
            .insert(cache_key, cached_segments);
        }
      }
      cached_window_results.push((
        request_index,
        request,
        segments.into(),
        result_visibility_cutoff,
      ));
    }
    self.record_recovery_metadata_cache_state();
    cached_window_results.sort_by_key(|(request_index, ..)| *request_index);
    Ok(
      cached_window_results
        .into_iter()
        .map(|(_, request, segments, visibility_cutoff)| (request, segments, visibility_cutoff))
        .collect(),
    )
  }

  /// Read selected segment ranges concurrently, then restore deterministic partition sequence
  /// order before cursor advancement.
  async fn execute_segment_reads(
    &self,
    segment_read_plans: Vec<SegmentReadPlan>,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<BatchReadResult>> {
    let mut batch_read_results = self
      .execute_broker_blob_range_reads(
        segment_read_plans,
        self.broker_blob_range_query.as_ref(),
        runtime_settings,
      )
      .await?;
    // Segment plans retain scan order, but concurrent metadata windows may not be ordered by a
    // partition's sequence range. Normalize before cursor advancement and delivery.
    batch_read_results.sort_by_key(|result| match result {
      BatchReadResult::Decoded { candidate, .. } | BatchReadResult::Missing { candidate, .. } => (
        candidate.virtual_partition_id,
        candidate.batch_metadata.seq_range.start,
      ),
    });
    Ok(batch_read_results)
  }

  /// Group immutable segment plans by blob key before attempting local broker cache delivery.
  async fn execute_broker_blob_range_reads(
    &self,
    segment_read_plans: Vec<SegmentReadPlan>,
    query: &dyn BrokerBlobRangeQuery,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<BatchReadResult>> {
    let mut plans_by_blob_key = HashMap::<BlobKey, Vec<SegmentReadPlan>>::new();
    for plan in segment_read_plans {
      plans_by_blob_key
        .entry(plan.metadata.blob_key.clone())
        .or_default()
        .push(plan);
    }
    let direct_read_concurrency = runtime_settings.max_in_flight_batch_reads;
    let direct_read_permits = Arc::new(Semaphore::new(direct_read_concurrency));

    stream::iter(plans_by_blob_key.into_values().map(|plans| {
      let direct_read_permits = Arc::clone(&direct_read_permits);
      async move {
        self
          .read_broker_blob_range_group(plans, query, direct_read_permits, direct_read_concurrency)
          .await
      }
    }))
    .buffered(runtime_settings.max_in_flight_batch_reads)
    .try_collect::<Vec<_>>()
    .await
    .map(|groups| groups.into_iter().flatten().collect())
  }

  /// Use broker bytes only after positional validation; retry every other group outcome directly.
  async fn read_broker_blob_range_group(
    &self,
    plans: Vec<SegmentReadPlan>,
    query: &dyn BrokerBlobRangeQuery,
    direct_read_permits: Arc<Semaphore>,
    direct_read_concurrency: usize,
  ) -> Result<Vec<BatchReadResult>> {
    let blob_key = plans
      .first()
      .map(|plan| plan.metadata.blob_key.as_str().to_string())
      .expect("blob range group is never empty");
    let request = ReadBlobRangesRequest {
      blob_key: blob_key.clone().into(),
      ranges: plans
        .iter()
        .map(|plan| BlobRangeRequest {
          start: plan.byte_range.start,
          end: plan.byte_range.end,
          ..Default::default()
        })
        .collect(),
      ..Default::default()
    };
    let started_at = Instant::now();
    self.metrics.record_broker_blob_range_request();
    let response = query.read_blob_ranges(request).await;
    match response.and_then(|response| {
      decode_blob_range_response_for_ranges(
        plans
          .iter()
          .map(|plan| (plan.byte_range.start, plan.byte_range.end)),
        response,
      )
    }) {
      Ok(BrokerBlobRangeRead::Success(payloads)) => {
        let delivered_bytes = payloads.iter().fold(0_u64, |total, payload| {
          total.saturating_add(u64::try_from(payload.len()).unwrap_or(u64::MAX))
        });
        let decoded = plans
          .iter()
          .zip(&payloads)
          .map(|(plan, payload)| {
            self.decode_segment_payload(&plan.metadata, &plan.candidates, &plan.byte_range, payload)
          })
          .collect::<Result<Vec<_>>>();
        match decoded {
          Ok(groups) => {
            let decoded = plans
              .into_iter()
              .zip(groups)
              .flat_map(|(plan, batches)| plan.candidates.into_iter().zip(batches))
              .map(|(candidate, batch)| {
                self.record_decoded_batch(&batch);
                BatchReadResult::Decoded { candidate, batch }
              })
              .collect();
            self
              .metrics
              .record_broker_blob_range_success(started_at, delivered_bytes);
            Ok(decoded)
          },
          Err(error) => {
            self.metrics.record_broker_blob_range_fallback();
            trace!(
              "consumer broker blob-range payload rejected; falling back direct: \
               blob_key={blob_key}, error={error}"
            );
            self
              .read_direct_blob_range_group(plans, direct_read_permits, direct_read_concurrency)
              .await
          },
        }
      },
      Ok(BrokerBlobRangeRead::NotFound) => {
        self.metrics.record_broker_blob_range_not_found();
        Ok(
          plans
            .into_iter()
            .flat_map(Self::missing_segment_plan)
            .collect(),
        )
      },
      Err(error) => {
        self.metrics.record_broker_blob_range_fallback();
        trace!(
          "consumer broker blob-range response rejected; falling back direct: \
           blob_key={blob_key}, error={error}"
        );
        self
          .read_direct_blob_range_group(plans, direct_read_permits, direct_read_concurrency)
          .await
      },
    }
  }

  /// Retry one failed broker group with the same global direct-read concurrency bound.
  async fn read_direct_blob_range_group(
    &self,
    plans: Vec<SegmentReadPlan>,
    direct_read_permits: Arc<Semaphore>,
    direct_read_concurrency: usize,
  ) -> Result<Vec<BatchReadResult>> {
    Ok(
      stream::iter(plans.into_iter().map(|plan| {
        let direct_read_permits = Arc::clone(&direct_read_permits);
        async move {
          let _permit = direct_read_permits
            .acquire_owned()
            .await
            .expect("direct read permits remain owned while a fallback is active");
          self.read_segment_plan(plan).await
        }
      }))
      .buffered(direct_read_concurrency.max(1))
      .try_collect::<Vec<_>>()
      .await?
      .into_iter()
      .flatten()
      .collect(),
    )
  }

  /// Advance partition cursors for completed reads and return only newly decoded batches.
  fn accept_batch_read_results(
    &mut self,
    batch_read_results: Vec<BatchReadResult>,
    scan_states: &mut HashMap<VirtualPartitionId, ConsumerReaderPartitionScanState>,
    metadata_batches_skipped_by_cursor: &mut usize,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> BatchAcceptance {
    // Results are normalized by partition sequence, but ordered responses are not a cross-window
    // snapshot. Never let a newer Fast result advance a cursor over a gap that an older, still
    // publication-open window could fill on its maturity retry.
    let mut output = Vec::new();
    let mut next_metadata_eligible_at = None::<time::OffsetDateTime>;
    let mut held_fast_sources = HashSet::new();
    for result in batch_read_results {
      let candidate = match &result {
        BatchReadResult::Decoded { candidate, .. } | BatchReadResult::Missing { candidate, .. } => {
          candidate
        },
      };
      if held_fast_sources
        .iter()
        .any(|(partition_id, _)| *partition_id == candidate.virtual_partition_id)
      {
        // Planning may already have advanced this later source's frontier. Retain it so the
        // retry restores every dropped window, not only the source that first exposed the gap.
        held_fast_sources.insert((
          candidate.virtual_partition_id,
          candidate.window_start_unix_seconds,
        ));
        continue;
      }
      let current_cursor = self
        .virtual_partition_states
        .get(&candidate.virtual_partition_id)
        .and_then(VirtualPartitionState::cursor);
      if current_cursor.is_some_and(|cursor| candidate.batch_metadata.seq_range.end <= cursor) {
        *metadata_batches_skipped_by_cursor = metadata_batches_skipped_by_cursor.saturating_add(1);
        if let Some(scan_state) = scan_states.get_mut(&candidate.virtual_partition_id) {
          scan_state.metadata_batches_skipped_by_cursor = scan_state
            .metadata_batches_skipped_by_cursor
            .saturating_add(1);
        }
        continue;
      }
      if let Some(maturity_at) = self.fast_gap_maturity_at(
        candidate.virtual_partition_id,
        candidate.window_start_unix_seconds,
        current_cursor,
        candidate.batch_metadata.seq_range.start,
        now,
        runtime_settings,
      ) {
        let retry_at = now.saturating_add(FAST_GAP_PROBE_INTERVAL).min(maturity_at);
        // Discard this decoded result intentionally: accepting it would make a late predecessor
        // permanently ineligible. The targeted Fast retry re-reads the retained tail at `retry_at`.
        if let Some(state) = self
          .virtual_partition_states
          .get_mut(&candidate.virtual_partition_id)
        {
          state.set_fast_gap_hold(
            retry_at,
            maturity_at,
            current_cursor
              .expect("Fast gap requires an existing cursor")
              .saturating_add(1),
          );
        }
        held_fast_sources.insert((
          candidate.virtual_partition_id,
          candidate.window_start_unix_seconds,
        ));
        next_metadata_eligible_at =
          Some(next_metadata_eligible_at.map_or(retry_at, |current| current.min(retry_at)));
        trace!(
          "consumer held Fast batch behind open prior window: topic={}, partition={}, \
           window_start={}, seq_start={}, seq_end={}, cursor={:?}, retry_at={}",
          self.config.topic,
          candidate.virtual_partition_id,
          offset_datetime_from_unix_seconds(candidate.window_start_unix_seconds),
          candidate.batch_metadata.seq_range.start,
          candidate.batch_metadata.seq_range.end,
          current_cursor,
          retry_at
        );
        continue;
      }

      match result {
        BatchReadResult::Decoded {
          candidate,
          mut batch,
        } => {
          // Cursor always moves forward. max() keeps monotonicity if metadata ordering is odd.
          let next_cursor = batch.seq_range.end.max(current_cursor.unwrap_or(0));
          if let Some(state) = self
            .virtual_partition_states
            .get_mut(&candidate.virtual_partition_id)
          {
            state.advance_cursor(next_cursor);
          }
          if let Some(cursor) = current_cursor {
            batch.discard_through(cursor);
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
    BatchAcceptance {
      batches: output,
      next_metadata_eligible_at,
      held_fast_sources,
    }
  }

  /// Complete lifecycle transitions and persist the diagnostics for a successful scan pass.
  fn finalize_scan_pass(
    &mut self,
    finalization: ScanPassFinalization,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
    scan_states: &mut HashMap<VirtualPartitionId, ConsumerReaderPartitionScanState>,
  ) -> Result<()> {
    let ScanPassFinalization {
      scanned_fresh_window_starts,
      partitions,
      capacity_deferred_fresh_window_starts,
      next_fast_frontiers,
    } = finalization;
    let mut initial_scans_completed = Vec::new();
    let mut recoveries_completed = Vec::new();
    // A Fresh scan covers its assigned current window. Once complete, Fast starts at this pass's
    // safety floor; any part of the preceding window that ages safe on the next pass is handled by
    // the planner's one-window Fast tail rather than by a transient Recovery transition.
    let fast_coverage_floor = self.fast_scan_safe_timestamp(now, runtime_settings);
    for (partition_id, state) in &mut self.virtual_partition_states {
      let mut next_state = None;
      match state {
        VirtualPartitionState::Fresh {
          cursor,
          initial_window_start_unix_seconds,
          ..
        } if scanned_fresh_window_starts.contains(initial_window_start_unix_seconds)
          && !partitions
            .get(partition_id)
            .is_some_and(|partition| partition.fresh_deferred_by_visibility)
          && !capacity_deferred_fresh_window_starts.contains(initial_window_start_unix_seconds) =>
        {
          initial_scans_completed.push((*partition_id, *initial_window_start_unix_seconds));
          next_state = Some(VirtualPartitionState::Fast {
            cursor: *cursor,
            coverage_floor: Some(fast_coverage_floor),
            gap: None,
            last_scan: None,
          });
        },
        VirtualPartitionState::Recovering {
          cursor,
          recovery_state,
          ..
        } => {
          let Some(partition_finalization) = partitions.get(partition_id) else {
            continue;
          };
          let Some(recovery_window_end) = partition_finalization.recovery_window_end else {
            continue;
          };
          let recovery_window_end = partition_finalization
            .visibility_deferred_recovery_window
            .map_or(recovery_window_end, |deferred_window| {
              // The deferred window is not complete. Resume at its predecessor so the next pass
              // advances into it normally instead of moving the recovery cursor past unread rows.
              recovery_window_end
                .min(deferred_window.saturating_sub(self.metadata_window_size.whole_seconds()))
            });
          let recovery_window_end = partition_finalization
            .capacity_deferred_recovery_window
            .map_or(recovery_window_end, |deferred_window| {
              // Capacity stops this window and every later planned request. The same predecessor
              // rule preserves chronological recovery and prevents a later cursor from skipping it.
              recovery_window_end
                .min(deferred_window.saturating_sub(self.metadata_window_size.whole_seconds()))
            });
          if recovery_state.next_window_start_unix_seconds > recovery_window_end {
            continue;
          }
          recovery_state.next_window_start_unix_seconds =
            recovery_window_end.saturating_add(self.metadata_window_size.whole_seconds());
          if recovery_state.next_window_start_unix_seconds
            > recovery_state.cutover_window_start_unix_seconds
          {
            let coverage_floor =
              offset_datetime_from_unix_seconds(recovery_state.cutover_window_start_unix_seconds);
            recoveries_completed.push((
              *partition_id,
              recovery_state.cutover_window_start_unix_seconds,
            ));
            next_state = Some(VirtualPartitionState::Fast {
              cursor: *cursor,
              coverage_floor: Some(coverage_floor),
              gap: None,
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
        offset_datetime_from_unix_seconds(initial_window_start_unix_seconds)
      );
    }
    for (partition_id, cutover_window_start_unix_seconds) in recoveries_completed {
      info!(
        "consumer partition recovery completed; fast path active: topic={}, partition={}, \
         cutover_window={}",
        self.config.topic,
        partition_id,
        offset_datetime_from_unix_seconds(cutover_window_start_unix_seconds)
      );
    }

    let retention_floor = self
      .retention_floor_window_start(Window::for_timestamp(now, self.metadata_window_size).start);
    self
      .recovery_metadata_cache
      .retain(|(partition_id, window_start, _), _| {
        matches!(
          self.virtual_partition_states.get(partition_id),
          Some(VirtualPartitionState::Recovering { recovery_state, .. })
            if recovery_state.next_window_start_unix_seconds <= *window_start
        ) || matches!(
          self.virtual_partition_states.get(partition_id),
          Some(VirtualPartitionState::Fast {
            coverage_floor: Some(coverage_floor),
            ..
          }) if Window::for_timestamp(
            (*coverage_floor).max(retention_floor),
            self.metadata_window_size,
          )
            .start
            .unix_timestamp()
            == *window_start
        )
      });
    self.record_recovery_metadata_cache_state();

    self.fast_frontiers = next_fast_frontiers;
    self.prune_fast_frontiers(now, runtime_settings)?;
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
    for (partition_id, scan_state) in scan_states.iter_mut() {
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
         metadata_sources_incomplete_by_capacity={}, \
         recovery_segments_handed_to_fast_by_visibility={}, \
         recovery_segments_blocked_by_visibility={}, mature_metadata_cache_reuses={}, \
         metadata_batches_deferred_by_capacity={}, batches_accepted={}, records_accepted={}, \
         fast_scan_bounds={:?}, fast_frontiers={:?}",
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
        scan_state.metadata_sources_incomplete_by_capacity,
        scan_state.recovery_segments_handed_to_fast_by_visibility,
        scan_state.recovery_segments_blocked_by_visibility,
        scan_state.mature_metadata_cache_reuses,
        scan_state.metadata_batches_deferred_by_capacity,
        scan_state.batches_accepted,
        scan_state.records_accepted,
        scan_state.fast_scan_bounds,
        scan_state.fast_frontiers
      );
    }
    for (partition_id, scan_state) in scan_states.drain() {
      if let Some(state) = self.virtual_partition_states.get_mut(&partition_id) {
        state.set_last_scan(scan_state);
      }
    }
    Ok(())
  }

  /// Execute one metadata scan pass, retaining no progress when a batch cannot reach the caller.
  pub(in crate::consumer) async fn read_available_impl(
    &mut self,
    now: time::OffsetDateTime,
    capacity: ReadCapacity,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<ConsumerReadOutcome> {
    // Cursor and frontier changes are only valid once every decoded batch is returned. A later
    // range-read failure must not make already decoded output disappear behind an advanced cursor.
    let virtual_partition_states = self.virtual_partition_states.clone();
    let fast_frontiers = self.fast_frontiers.clone();
    let result = self
      .read_available_impl_inner(now, capacity, runtime_settings)
      .await;
    if result.is_err() {
      self.virtual_partition_states = virtual_partition_states;
      self.fast_frontiers = fast_frontiers;
    }
    result
  }

  /// Execute one metadata scan pass, then advance scan modes only for windows fully observed.
  pub(in crate::consumer) async fn read_available_impl_inner(
    &mut self,
    now: time::OffsetDateTime,
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
    let mut metadata_batches_scanned = 0_usize;
    let mut metadata_batches_skipped_by_cursor = 0_usize;
    let now_unix_seconds = now.unix_timestamp();
    let initial_fast_partitions = assigned_partition_ids
      .iter()
      .copied()
      .filter(|partition_id| {
        matches!(
          self.virtual_partition_states.get(partition_id),
          Some(VirtualPartitionState::Fast { .. })
        )
      })
      .collect::<Vec<_>>();
    let fast_scan_start_floor = self.fast_scan_safe_timestamp(now, runtime_settings);
    for &partition_id in &initial_fast_partitions {
      if let Some(state) = self.virtual_partition_states.get_mut(&partition_id) {
        // Retain the pass-start floor until all Fast sources complete. If capacity interrupts this
        // pass, the next pass must still cover everything at or after this exact timestamp.
        state.seed_fast_coverage_floor(fast_scan_start_floor);
      }
    }
    let visibility_cutoff = now.saturating_sub(runtime_settings.metadata_visibility_delay);
    // A pass can defer several sources. The worker needs only the first safe retry, not a
    // per-source timer, because rescanning then will reconsider every deferred source.
    let mut next_metadata_eligible_at = None::<time::OffsetDateTime>;

    // Requests merge partitions sharing a metadata window. The DynamoDB query uses the lowest
    // needed Snowflake bound, then execution reapplies each partition's stricter Fast frontier.
    // Recovery catches debt that escaped the live horizon; Fast covers the normal live range and
    // its one-window rollover tail.
    let (scan_requests, recovery_scan) =
      self.scan_requests(now, &assigned_partition_ids, runtime_settings)?;
    for request in &scan_requests {
      trace!(
        "consumer metadata scan planned: topic={}, window_start={}, recovery={}, fast={}, \
         fresh={}, min_snowflake={:?}",
        request.window.topic,
        offset_datetime_from_unix_seconds(request.window.window_start_unix_seconds),
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
    self.record_fast_scan_bounds(&scan_requests, now, runtime_settings, &mut scan_states);
    let mut partition_finalizations =
      HashMap::<VirtualPartitionId, PartitionScanFinalization>::new();
    for request in &scan_requests {
      for &partition_id in &request.eligibility.recovering_partitions {
        let partition_finalization = partition_finalizations.entry(partition_id).or_default();
        partition_finalization.recovery_window_end = Some(
          partition_finalization
            .recovery_window_end
            .map_or(request.window.window_start_unix_seconds, |window_end| {
              window_end.max(request.window.window_start_unix_seconds)
            }),
        );
      }
    }
    let scanned_fresh_window_starts = scan_requests
      .iter()
      .filter(|request| request.eligibility.fresh)
      .map(|request| request.window.window_start_unix_seconds)
      .collect::<Vec<_>>();
    let window_results = self
      .materialize_window_results(
        &scan_requests,
        recovery_scan,
        now,
        runtime_settings,
        visibility_cutoff,
        &mut scan_states,
      )
      .await?;

    let mut next_fast_frontiers = self.fast_frontiers.clone();
    let mut blocked_fast_sources = HashSet::new();
    // A deferred recovery window must hold back later windows for the same partition. Otherwise a
    // later sequence could advance the cursor and make the deferred batch permanently ineligible.
    // TODO: Concurrent Recovery window queries can still miss an unobserved predecessor while a
    // later window sees its successor. Serialize publication-open Recovery windows or add a
    // Fast-style sequence barrier if this rare cross-window race needs to be eliminated.
    let mut blocked_recovering_partitions = HashSet::new();
    let mut capacity_deferred_fresh_window_starts = HashSet::new();
    // Recovery can hand an active publication-horizon window to Fast. A handoff is safe only when
    // Fast's own inclusive lower bound will still include the deferred segment on its next pass.
    let fast_horizon_windows = self
      .eligible_fast_scan_windows(now, runtime_settings)?
      .into_iter()
      .map(|window| window.window_start_unix_seconds)
      .collect::<HashSet<_>>();
    let fast_horizon_floor =
      Self::snowflake_floor(self.fast_scan_safe_timestamp(now, runtime_settings));
    let mut segment_read_plans = Vec::new();
    let mut capacity_exhausted = false;
    'windows: for (request_index, (request, segments, visibility_cutoff)) in
      window_results.into_iter().enumerate()
    {
      let window = request.window;
      trace!(
        "consumer scanned window: topic={}, window_start={}, segments={}",
        window.topic,
        offset_datetime_from_unix_seconds(window.window_start_unix_seconds),
        segments.len()
      );

      for segment in segments.iter() {
        // Restrict work to currently assigned virtual partitions only.
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
            Some(VirtualPartitionState::Fast { .. }) => {
              // Rollover-tail requests deliberately include only partitions with retained debt in
              // that old window. Ordinary Fast partitions must not turn this narrow repair into a
              // full reread of the previous DynamoDB partition.
              request.eligibility.fast && request.fast_partition_bounds.contains_key(&partition_id)
            },
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
              offset_datetime_from_unix_seconds(window.window_start_unix_seconds),
              segment.snowflake_id.as_u64()
            );
            scan_state.metadata_segments_without_partition_batches = scan_state
              .metadata_segments_without_partition_batches
              .saturating_add(1);
            continue;
          };
          let frontier_key = (partition_id, window.window_start_unix_seconds);
          let fast_partition = matches!(partition_state, Some(VirtualPartitionState::Fast { .. }));
          // Sources are Snowflake ordered, so keep the newest rows nearest accepted batches.
          if scan_state.metadata_sources.len() == MAX_METADATA_SOURCE_DETAILS {
            scan_state.metadata_sources.pop_front();
            scan_state.metadata_sources_truncated = true;
          }
          scan_state
            .metadata_sources
            .push_back(ConsumerReaderMetadataSource {
              window_start_unix_seconds: window.window_start_unix_seconds,
              snowflake_id: segment.snowflake_id.as_u64(),
              blob_key: segment.blob_key.as_str().to_string(),
              metadata_published_at: segment.metadata_published_at,
              batch_ranges: partition_batches
                .iter()
                .map(|batch| batch.seq_range.clone())
                .collect(),
            });
          if fast_partition {
            // The shared DynamoDB lower bound is the least restrictive partition bound. Reapply
            // each partition's inclusive bound here so a sparse partition cannot be skipped
            // because another partition in the same query requires an earlier floor.
            if blocked_fast_sources.contains(&frontier_key) {
              trace!(
                "consumer metadata segment blocked by earlier visibility delay: topic={}, \
                 partition={}, window_start={}, snowflake_id={}",
                self.config.topic,
                partition_id,
                offset_datetime_from_unix_seconds(window.window_start_unix_seconds),
                segment.snowflake_id.as_u64()
              );
              scan_state.metadata_segments_blocked_by_visibility = scan_state
                .metadata_segments_blocked_by_visibility
                .saturating_add(1);
              continue;
            }
            let partition_lower_bound = request
              .fast_partition_bounds
              .get(&partition_id)
              .expect("eligible Fast partition has a planned lower bound");
            if segment.snowflake_id < *partition_lower_bound {
              trace!(
                "consumer metadata segment skipped by fast lower bound: topic={}, partition={}, \
                 window_start={}, snowflake_id={}, lower_bound={}",
                self.config.topic,
                partition_id,
                offset_datetime_from_unix_seconds(window.window_start_unix_seconds),
                segment.snowflake_id.as_u64(),
                partition_lower_bound.as_u64()
              );
              scan_state.metadata_segments_skipped_by_frontier = scan_state
                .metadata_segments_skipped_by_frontier
                .saturating_add(1);
              continue;
            }
            if next_fast_frontiers
              .get(&frontier_key)
              .is_some_and(|frontier| segment.snowflake_id < *frontier)
            {
              trace!(
                "consumer metadata segment skipped by fast frontier: topic={}, partition={}, \
                 window_start={}, snowflake_id={}, frontier={}",
                self.config.topic,
                partition_id,
                offset_datetime_from_unix_seconds(window.window_start_unix_seconds),
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

          let published_at = segment.metadata_published_at;
          if runtime_settings.metadata_read_consistency == MetadataReadConsistency::Eventual
            && published_at > visibility_cutoff
          {
            // The cutoff is `now - visibility_delay`: newer metadata may not be present on every
            // eventual-consistency replica. Fast blocks its frontier; Fresh retries its sole
            // initial window; Recovery either hands the row to Fast or holds its cursor behind it.
            self
              .metrics
              .metadata_segments_deferred_by_visibility_delay
              .inc();
            scan_state.metadata_segments_deferred_by_visibility = scan_state
              .metadata_segments_deferred_by_visibility
              .saturating_add(1);
            let visibility_eligible_at =
              published_at.saturating_add(runtime_settings.metadata_visibility_delay);
            next_metadata_eligible_at = Some(
              next_metadata_eligible_at.map_or(visibility_eligible_at, |current| {
                current.min(visibility_eligible_at)
              }),
            );
            trace!(
              "consumer deferred metadata by visibility delay: topic={}, partition={}, \
               window_start={}, snowflake_id={}, published_at={}, visibility_cutoff={}",
              self.config.topic,
              partition_id,
              offset_datetime_from_unix_seconds(window.window_start_unix_seconds),
              segment.snowflake_id.as_u64(),
              published_at,
              visibility_cutoff
            );
            if fast_partition {
              blocked_fast_sources.insert(frontier_key);
            } else if matches!(partition_state, Some(VirtualPartitionState::Fresh { .. })) {
              partition_finalizations
                .entry(partition_id)
                .or_default()
                .fresh_deferred_by_visibility = true;
            } else {
              // Recovery may complete only when Fast can revisit this source. A retained Fast
              // frontier can be stricter than the time floor after a stalled Fast scan.
              let fast_handoff_floor = next_fast_frontiers
                .get(&frontier_key)
                .copied()
                .map_or(fast_horizon_floor, |frontier| {
                  frontier.max(fast_horizon_floor)
                });
              if fast_horizon_windows.contains(&window.window_start_unix_seconds)
                && segment.snowflake_id >= fast_handoff_floor
              {
                scan_state.recovery_segments_handed_to_fast_by_visibility = scan_state
                  .recovery_segments_handed_to_fast_by_visibility
                  .saturating_add(1);
              } else {
                scan_state.recovery_segments_blocked_by_visibility = scan_state
                  .recovery_segments_blocked_by_visibility
                  .saturating_add(1);
                let partition_finalization =
                  partition_finalizations.entry(partition_id).or_default();
                partition_finalization.visibility_deferred_recovery_window = Some(
                  partition_finalization
                    .visibility_deferred_recovery_window
                    .map_or(window.window_start_unix_seconds, |deferred_window| {
                      deferred_window.min(window.window_start_unix_seconds)
                    }),
                );
                blocked_recovering_partitions.insert(partition_id);
              }
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
              window_start_unix_seconds: window.window_start_unix_seconds,
            });
          }

          if capacity_exhausted {
            break;
          }

          if fast_partition {
            // Advance only after visibility and capacity checks. The value remains inclusive, so
            // the boundary row is replayed next pass; `max` prevents unordered results from
            // regressing the per-window frontier.
            next_fast_frontiers
              .entry(frontier_key)
              .and_modify(|frontier| *frontier = (*frontier).max(segment.snowflake_id))
              .or_insert(segment.snowflake_id);
          }
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
          for scan_state in scan_states.values_mut() {
            scan_state.metadata_sources_incomplete_by_capacity = true;
          }
          for deferred_request in scan_requests.iter().skip(request_index) {
            for &partition_id in &deferred_request.eligibility.recovering_partitions {
              let partition_finalization = partition_finalizations.entry(partition_id).or_default();
              partition_finalization.capacity_deferred_recovery_window = Some(
                partition_finalization
                  .capacity_deferred_recovery_window
                  .map_or(
                    deferred_request.window.window_start_unix_seconds,
                    |deferred_window| {
                      deferred_window.min(deferred_request.window.window_start_unix_seconds)
                    },
                  ),
              );
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

    let batch_read_results = self
      .execute_segment_reads(segment_read_plans, runtime_settings)
      .await?;

    let BatchAcceptance {
      batches: mut output,
      next_metadata_eligible_at: fast_gap_next_probe_at,
      held_fast_sources,
    } = self.accept_batch_read_results(
      batch_read_results,
      &mut scan_states,
      &mut metadata_batches_skipped_by_cursor,
      now,
      runtime_settings,
    );
    // Planning advances a per-window Fast frontier while it selects candidates. A held candidate
    // is deliberately not admitted, so its source is incomplete even when later segments in the
    // same window were selected. Restore the pre-pass frontier: otherwise the retry could begin
    // at a later segment and permanently exclude the first held sequence.
    for frontier_key in &held_fast_sources {
      if let Some(previous_frontier) = self.fast_frontiers.get(frontier_key) {
        next_fast_frontiers.insert(*frontier_key, *previous_frontier);
      } else {
        next_fast_frontiers.remove(frontier_key);
      }
    }
    if let Some(fast_gap_next_probe_at) = fast_gap_next_probe_at {
      next_metadata_eligible_at = Some(
        next_metadata_eligible_at.map_or(fast_gap_next_probe_at, |current| {
          current.min(fast_gap_next_probe_at)
        }),
      );
    }
    for state in self.virtual_partition_states.values_mut() {
      state.reschedule_fast_gap_probe_if_due(now, FAST_GAP_PROBE_INTERVAL);
    }
    // A prior pass can emit a contiguous prefix before it finds the held range. Preserve its
    // stored deadline on a following empty pass; a due probe that returns no rows reschedules
    // itself above until it observes the missing sequence or reaches the maturity boundary.
    if let Some(pending_fast_gap_next_probe_at) = self
      .virtual_partition_states
      .values()
      .filter_map(VirtualPartitionState::fast_gap_next_probe_at)
      .filter(|next_probe_at| *next_probe_at > now)
      .min()
    {
      next_metadata_eligible_at = Some(
        next_metadata_eligible_at.map_or(pending_fast_gap_next_probe_at, |current| {
          current.min(pending_fast_gap_next_probe_at)
        }),
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

    self.finalize_scan_pass(
      ScanPassFinalization {
        scanned_fresh_window_starts,
        partitions: partition_finalizations,
        capacity_deferred_fresh_window_starts,
        next_fast_frontiers,
      },
      now,
      runtime_settings,
      &mut scan_states,
    )?;
    if !capacity_exhausted {
      for partition_id in initial_fast_partitions {
        if !blocked_fast_sources
          .iter()
          .any(|(blocked_partition_id, _)| *blocked_partition_id == partition_id)
          // A held gap can suppress all requests until the next probe. Those empty passes must
          // keep the older coverage floor, or the later query would start after the sequence that
          // caused the hold.
          && !self
            .virtual_partition_states
            .get(&partition_id)
            .is_some_and(VirtualPartitionState::fast_gap_hold_is_pending)
          && let Some(state) = self.virtual_partition_states.get_mut(&partition_id)
        {
          state.set_fast_coverage_floor(fast_scan_start_floor);
        }
      }
    }
    self.attach_admission_scan_contexts(&mut output);

    self.metrics.record_read_available(
      read_started_at,
      output.len(),
      metadata_batches_scanned,
      metadata_batches_skipped_by_cursor,
      recovery_scan,
    );

    // A metadata maturity deadline is useful only when it is the sole reason this successful pass
    // has no work. Ready output must keep the refill loop hot. Capacity otherwise means unscanned
    // metadata could be ready now. A gap deadline remains useful only when every assigned
    // partition is already held; otherwise sleeping until that deadline delays another
    // partition's capacity-deferred work.
    let all_assigned_partitions_held = assigned_partition_ids.iter().all(|partition_id| {
      held_fast_sources
        .iter()
        .any(|(held_partition_id, _)| held_partition_id == partition_id)
    });
    Ok(ConsumerReadOutcome {
      next_metadata_eligible_at: output
        .is_empty()
        .then_some(next_metadata_eligible_at)
        .flatten()
        .filter(|deadline| {
          (!capacity_exhausted || all_assigned_partitions_held) && *deadline > now
        }),
      batches: output,
    })
  }

  fn attach_admission_scan_contexts(&self, batches: &mut [ConsumerBatch]) {
    let mut contexts = HashMap::new();
    for batch in batches {
      let context = contexts
        .entry(batch.virtual_partition_id)
        .or_insert_with(|| {
          self
            .virtual_partition_states
            .get(&batch.virtual_partition_id)
            .and_then(VirtualPartitionState::last_scan_handle)
            .map(|scan| Arc::new(reader_partition_scan_snapshot(&scan)))
        });
      batch.admission_scan.clone_from(context);
    }
  }
}

async fn scan_direct_metadata(
  metadata_store: &Arc<dyn blob_stream_metadata_store::MetadataStore>,
  request: &ScanRequest,
  consistency: MetadataReadConsistency,
) -> Result<Vec<SegmentMetadata>> {
  metadata_store
    .scan_window_from_snowflake(&request.window, request.min_snowflake, consistency)
    .await
    .with_context(|| {
      format!(
        "consumer metadata scan failed: topic={}, window_start={}, recovery={}, fast={}, \
         fresh={}, min_snowflake={:?}",
        request.window.topic,
        offset_datetime_from_unix_seconds(request.window.window_start_unix_seconds),
        !request.eligibility.recovering_partitions.is_empty(),
        request.eligibility.fast,
        request.eligibility.fresh,
        request.min_snowflake.map(SnowflakeId::as_u64)
      )
    })
}

fn broker_request_for_scan(
  request: &ScanRequest,
  consistency: MetadataReadConsistency,
) -> Option<ReadMetadataWindowRequest> {
  if request.fast_partition_bounds.is_empty() && request.recovery_partition_bounds.is_empty() {
    return None;
  }
  let consistency = match consistency {
    MetadataReadConsistency::Eventual => BrokerReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    MetadataReadConsistency::Strong => BrokerReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
  };
  let coverage = if request.recovery_partition_bounds.is_empty() {
    read_metadata_window_request::Coverage::Tail(TailMetadataCoverage {
      partition_bounds: request
        .fast_partition_bounds
        .iter()
        .map(
          |(&virtual_partition_id, &min_snowflake)| MetadataPartitionBound {
            virtual_partition_id,
            min_snowflake: min_snowflake.as_u64(),
            ..Default::default()
          },
        )
        .collect(),
      ..Default::default()
    })
  } else if request
    .recovery_partition_bounds
    .values()
    .all(Option::is_some)
  {
    let mut partition_bounds = request.fast_partition_bounds.clone();
    for (&partition_id, &min_snowflake) in &request.recovery_partition_bounds {
      let min_snowflake = min_snowflake.expect("bounded recovery partition has a lower bound");
      partition_bounds
        .entry(partition_id)
        .and_modify(|bound| *bound = (*bound).min(min_snowflake))
        .or_insert(min_snowflake);
    }
    read_metadata_window_request::Coverage::Tail(TailMetadataCoverage {
      partition_bounds: partition_bounds
        .into_iter()
        .map(
          |(virtual_partition_id, min_snowflake)| MetadataPartitionBound {
            virtual_partition_id,
            min_snowflake: min_snowflake.as_u64(),
            ..Default::default()
          },
        )
        .collect(),
      ..Default::default()
    })
  } else {
    let mut partition_ids = request
      .recovery_partition_bounds
      .keys()
      .copied()
      .collect::<Vec<_>>();
    partition_ids.extend(request.fast_partition_bounds.keys().copied());
    partition_ids.sort_unstable();
    partition_ids.dedup();
    read_metadata_window_request::Coverage::FullRecovery(FullRecoveryMetadataCoverage {
      virtual_partition_ids: partition_ids,
      ..Default::default()
    })
  };
  Some(ReadMetadataWindowRequest {
    topic: request.window.topic.clone().into(),
    window_start_unix_seconds: request.window.window_start_unix_seconds,
    consistency: consistency.into(),
    coverage: Some(coverage),
    max_response_bytes: 16 * 1024 * 1024,
    ..Default::default()
  })
}
