use super::{
  BTreeMap,
  ConsumerReadRuntimeSettings,
  ConsumerReaderFastScanBoundState,
  ConsumerReaderImpl,
  ConsumerReaderPartitionScanState,
  HashMap,
  HashSet,
  MAX_RECOVERY_WINDOWS_PER_SCAN,
  Result,
  ScanEligibility,
  ScanRequest,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  VirtualPartitionState,
  Window,
  info,
  offset_datetime_from_unix_seconds,
  trace,
};
use crate::config::consumer_candidate_window_count_with_availability_horizon;
use crate::consumer::reader::RecoveryMetadataCacheKey;
use crate::consumer::state::RecoveryState;

impl ConsumerReaderImpl {
  /// Return the cache identity for an immutable, single-partition metadata request.
  ///
  /// Mature Recovery windows and the single Fast rollover tail are immutable. The key
  /// intentionally excludes consistency: runtime changes do not invalidate a completed
  /// observation or replay it with stronger reads.
  pub(in crate::consumer) fn mature_recovery_metadata_cache_key(
    &self,
    request: &ScanRequest,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Option<RecoveryMetadataCacheKey> {
    let partition_id =
      if request.recovery_scan && !request.eligibility.fast && !request.eligibility.fresh {
        let &[partition_id] = request.eligibility.recovering_partitions.as_slice() else {
          return None;
        };
        partition_id
      } else if !request.recovery_scan && request.eligibility.fast && !request.eligibility.fresh {
        let (&partition_id, _) = request.fast_partition_bounds.first_key_value()?;
        if request.fast_partition_bounds.len() != 1 {
          return None;
        }
        partition_id
      } else {
        return None;
      };
    let window_end_unix_seconds = request
      .window
      .window_start_unix_seconds
      .saturating_add(self.metadata_window_size.whole_seconds());
    if window_end_unix_seconds > self.fast_scan_safe_timestamp_unix_seconds(now, runtime_settings) {
      return None;
    }
    // Recovery's first-window bound identifies its durable resume point. A Fast-tail frontier
    // only advances as batches are accepted, so its first mature response is a safe superset for
    // later capacity refills and must keep the same cache key as that frontier narrows.
    let min_snowflake = request
      .recovery_scan
      .then(|| request.min_snowflake.map(SnowflakeId::as_u64))
      .flatten();
    Some((
      partition_id,
      request.window.window_start_unix_seconds,
      min_snowflake,
    ))
  }

  /// Return the bounded current-window horizon, from oldest candidate to newest.
  pub(in crate::consumer) fn scan_windows(
    &self,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<TopicWindowKey>> {
    // Include the current window plus ceil(availability_horizon / window_size) preceding windows.
    // A row can be published or remain invisible anywhere in that interval, so omitting a partial
    // leading window would create a gap. Oldest -> newest ordering keeps traversal deterministic.
    let now_unix_seconds = now.unix_timestamp();
    let current_window = Window::for_timestamp(now, self.metadata_window_size)
      .start
      .unix_timestamp();

    let candidate_windows = consumer_candidate_window_count_with_availability_horizon(
      self.metadata_window_size,
      self.availability_horizon(runtime_settings).duration(),
    )?;
    let window_size_seconds = self.metadata_window_size.whole_seconds();
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
      offset_datetime_from_unix_seconds(now_unix_seconds)
    );

    Ok(windows)
  }

  /// Combine mode-specific demand for one window into one metadata-store query.
  pub(in crate::consumer) fn insert_scan_request(
    scan_requests: &mut BTreeMap<i64, ScanRequest>,
    topic: &str,
    window_start_unix_seconds: i64,
    min_snowflake: Option<SnowflakeId>,
    fast_partition_bounds: BTreeMap<VirtualPartitionId, SnowflakeId>,
    recovery_partition_bounds: BTreeMap<VirtualPartitionId, Option<SnowflakeId>>,
    recovery_scan: bool,
    eligibility: ScanEligibility,
  ) {
    scan_requests
      .entry(window_start_unix_seconds)
      .and_modify(|request| {
        // One DynamoDB query serves every mode and partition in this window. Its lower bound must
        // therefore be the least restrictive bound; per-partition bounds are reapplied later.
        request.min_snowflake = request.min_snowflake.min(min_snowflake);
        request
          .fast_partition_bounds
          .extend(fast_partition_bounds.clone());
        request
          .recovery_partition_bounds
          .extend(recovery_partition_bounds.clone());
        request.recovery_scan |= recovery_scan;
        for &partition_id in &eligibility.recovering_partitions {
          if !request
            .eligibility
            .recovering_partitions
            .contains(&partition_id)
          {
            request.eligibility.recovering_partitions.push(partition_id);
          }
        }
        request.eligibility.fast |= eligibility.fast;
        request.eligibility.fresh |= eligibility.fresh;
      })
      .or_insert_with(|| ScanRequest {
        window: TopicWindowKey {
          topic: topic.to_string(),
          window_start_unix_seconds,
        },
        min_snowflake,
        fast_partition_bounds,
        recovery_partition_bounds,
        recovery_scan,
        eligibility,
      });
  }

  /// Return each Fast partition's effective lower bound for one metadata window.
  pub(in crate::consumer) fn fast_scan_partition_bounds(
    &self,
    assigned_partition_ids: &[VirtualPartitionId],
    window_start_unix_seconds: i64,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> BTreeMap<VirtualPartitionId, SnowflakeId> {
    assigned_partition_ids
      .iter()
      .filter_map(|&partition_id| {
        let (_, time_floor) = self.fast_scan_partition_time_floor(
          partition_id,
          window_start_unix_seconds,
          now,
          runtime_settings,
        )?;
        self
          .fast_scan_partition_lower_bound(partition_id, window_start_unix_seconds, time_floor)
          .map(|(_, partition_lower_bound)| (partition_id, partition_lower_bound))
      })
      .collect()
  }

  /// Return the timestamp and Snowflake floor whose Fast metadata coverage is still required.
  fn fast_scan_partition_time_floor(
    &self,
    partition_id: VirtualPartitionId,
    window_start_unix_seconds: i64,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Option<(time::OffsetDateTime, SnowflakeId)> {
    let state = self.virtual_partition_states.get(&partition_id)?;
    if !matches!(state, VirtualPartitionState::Fast { .. }) {
      return None;
    }
    let safe_timestamp = self.fast_scan_safe_timestamp(now, runtime_settings);
    let coverage_floor = state.fast_coverage_floor().unwrap_or(safe_timestamp);
    let window_start = time::OffsetDateTime::from_unix_timestamp(window_start_unix_seconds)
      .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    // `coverage_floor` is retained when a prior Fast pass may have stopped early. `safe_timestamp`
    // is the newest point that must be revisited for publication/visibility safety. Use their
    // earlier value so an incomplete pass is never skipped, then clamp to this window's start: a
    // window-specific DynamoDB query cannot observe rows before its partition key.
    let floor_timestamp = window_start.max(safe_timestamp.min(coverage_floor));
    Some((floor_timestamp, Self::snowflake_floor(floor_timestamp)))
  }

  /// Return the observed and effective lower bounds for one Fast partition/window pair.
  pub(in crate::consumer) fn fast_scan_partition_lower_bound(
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
    // Frontiers are inclusive replay points. The higher lower bound preserves an already observed
    // per-window frontier while also discarding rows that are definitely outside the live horizon.
    // The shared query can be lower than this; execution reapplies this bound per partition.
    let partition_lower_bound =
      observed_frontier.map_or(safe_floor, |frontier| frontier.max(safe_floor));
    Some((observed_frontier, partition_lower_bound))
  }

  /// Record the time and frontier inputs that determined each Fast partition's shared query.
  pub(in crate::consumer) fn record_fast_scan_bounds(
    &self,
    scan_requests: &[ScanRequest],
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
    scan_states: &mut HashMap<VirtualPartitionId, ConsumerReaderPartitionScanState>,
  ) {
    for request in scan_requests {
      if !request.eligibility.fast {
        continue;
      }
      // A rollover tail can be targeted to a subset of Fast partitions. Record only the bounds
      // that actually contributed to this request so diagnostics match query execution.
      for &partition_id in request.fast_partition_bounds.keys() {
        let Some((floor_timestamp, time_floor)) = self.fast_scan_partition_time_floor(
          partition_id,
          request.window.window_start_unix_seconds,
          now,
          runtime_settings,
        ) else {
          continue;
        };
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
            .push(ConsumerReaderFastScanBoundState {
              window_start_unix_seconds: request.window.window_start_unix_seconds,
              floor_timestamp,
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
  pub(in crate::consumer) fn fast_scan_safe_timestamp_unix_seconds(
    &self,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> i64 {
    self
      .fast_scan_safe_timestamp(now, runtime_settings)
      .unix_timestamp()
  }

  /// Return the exact oldest instant whose metadata may still be unpublished or invisible.
  pub(in crate::consumer) fn fast_scan_safe_timestamp(
    &self,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> time::OffsetDateTime {
    now.saturating_sub(self.availability_horizon(runtime_settings).duration())
  }

  /// Return the lowest possible segment ID for a timestamp, preserving safety for invalid input.
  pub(in crate::consumer) fn snowflake_floor(timestamp: time::OffsetDateTime) -> SnowflakeId {
    SnowflakeId::minimum_for_timestamp(timestamp)
  }

  /// Return Fast windows that can still contain unpublished or invisible metadata.
  pub(in crate::consumer) fn eligible_fast_scan_windows(
    &self,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<TopicWindowKey>> {
    let safe_timestamp = self.fast_scan_safe_timestamp(now, runtime_settings);
    let window_size_seconds = self.metadata_window_size.whole_seconds();
    let windows = self
      .scan_windows(now, runtime_settings)?
      .into_iter()
      .filter(|window| {
        time::OffsetDateTime::from_unix_timestamp(
          window
            .window_start_unix_seconds
            .saturating_add(window_size_seconds),
        )
        .is_ok_and(|window_end| window_end > safe_timestamp)
      })
      .collect();
    Ok(windows)
  }

  /// Return the least lower bound that can serve every recovering partition in one window.
  pub(in crate::consumer) fn recovery_scan_min_snowflake(
    &self,
    partition_id: VirtualPartitionId,
    window_start_unix_seconds: i64,
  ) -> Option<SnowflakeId> {
    let VirtualPartitionState::Recovering { recovery_state, .. } =
      self.virtual_partition_states.get(&partition_id)?
    else {
      return None;
    };
    if recovery_state.next_window_start_unix_seconds > window_start_unix_seconds
      || window_start_unix_seconds > recovery_state.cutover_window_start_unix_seconds
    {
      return None;
    }
    recovery_state.first_window_min_snowflake.filter(|_| {
      recovery_state.first_window_start_unix_seconds == Some(window_start_unix_seconds)
        && recovery_state.next_window_start_unix_seconds == window_start_unix_seconds
    })
  }

  /// Convert Fast coverage debt that escaped the live horizon into bounded chronological recovery.
  fn start_fast_coverage_recoveries(
    &mut self,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<()> {
    let Some(first_fast_window) = self
      .eligible_fast_scan_windows(now, runtime_settings)?
      .into_iter()
      .next()
    else {
      return Ok(());
    };
    let cutover_window_start = Window::for_timestamp(now, self.metadata_window_size)
      .start
      .unix_timestamp();
    let retention_floor =
      self.retention_floor_window_start(offset_datetime_from_unix_seconds(cutover_window_start));
    let recoveries = self
      .virtual_partition_states
      .iter()
      .filter_map(|(partition_id, state)| {
        let coverage_floor = state.fast_coverage_floor()?;
        let recovery_floor = coverage_floor.max(retention_floor);
        let recovery_start_window =
          Window::for_timestamp(recovery_floor, self.metadata_window_size)
            .start
            .unix_timestamp();
        // The immediately preceding window is the ordinary rollover tail: its last unsafe rows
        // become eligible just after the safe timestamp enters the next window, and Fast scans it
        // below. Start bounded Recovery only when the debt is at least two windows behind the live
        // horizon. This avoids a Recovery round-trip for every partition at every window boundary.
        (recovery_start_window.saturating_add(self.metadata_window_size.whole_seconds())
          < first_fast_window.window_start_unix_seconds)
          .then_some((*partition_id, recovery_floor, recovery_start_window))
      })
      .collect::<Vec<_>>();
    for (partition_id, coverage_floor, recovery_start_window) in recoveries {
      let first_window_min_snowflake = Some(Self::snowflake_floor(coverage_floor));
      let Some(state) = self.virtual_partition_states.get_mut(&partition_id) else {
        continue;
      };
      state.start_recovery(RecoveryState {
        next_window_start_unix_seconds: recovery_start_window,
        cutover_window_start_unix_seconds: cutover_window_start,
        first_window_start_unix_seconds: Some(recovery_start_window),
        first_window_min_snowflake,
      });
      info!(
        "consumer fast coverage catch-up started: topic={}, partition={}, coverage_floor={}, \
         recovery_start_window={}, cutover_window={}",
        self.config.topic,
        partition_id,
        coverage_floor,
        offset_datetime_from_unix_seconds(recovery_start_window),
        offset_datetime_from_unix_seconds(cutover_window_start),
      );
    }
    Ok(())
  }

  /// Build a scan pass that prioritizes bounded recovery before using the fast path.
  pub(in crate::consumer) fn scan_requests(
    &mut self,
    now: time::OffsetDateTime,
    assigned_partition_ids: &[VirtualPartitionId],
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<(Vec<ScanRequest>, bool)> {
    let mut scan_requests = BTreeMap::new();
    let mut recovery_scan = false;

    self.start_fast_coverage_recoveries(now, runtime_settings)?;

    // Rotate recovery slices between partitions so a dense historical partition cannot consume
    // every prefetch cycle and starve a later partition's independent recovery.
    let mut recovering_partitions = self
      .virtual_partition_states
      .iter()
      .filter_map(|(partition_id, state)| match state {
        VirtualPartitionState::Recovering { recovery_state, .. } if state.is_assigned() => Some((
          *partition_id,
          recovery_state.next_window_start_unix_seconds,
          recovery_state.cutover_window_start_unix_seconds,
        )),
        VirtualPartitionState::Recovering { .. }
        | VirtualPartitionState::PendingCursor { .. }
        | VirtualPartitionState::PendingRecovering { .. }
        | VirtualPartitionState::PendingFast { .. }
        | VirtualPartitionState::Fresh { .. }
        | VirtualPartitionState::Fast { .. } => None,
      })
      .collect::<Vec<_>>();
    recovering_partitions.sort_by_key(|(partition_id, ..)| *partition_id);
    if recovering_partitions.is_empty() {
      self.recovery_scan_last_partition = None;
    } else if recovering_partitions.iter().all(
      |(_, recovery_start_window, recovery_cutover_window)| {
        recovery_start_window == recovery_cutover_window
      },
    ) {
      // All partitions need only their active cutover window. They can share one query because no
      // partition has earlier recovery work that could monopolize capacity; per-partition bounds
      // still ensure the shared DynamoDB lower bound is filtered correctly after the query.
      for (partition_id, recovery_start_window, _) in &recovering_partitions {
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          *recovery_start_window,
          self.recovery_scan_min_snowflake(*partition_id, *recovery_start_window),
          BTreeMap::new(),
          BTreeMap::from([(
            *partition_id,
            self.recovery_scan_min_snowflake(*partition_id, *recovery_start_window),
          )]),
          true,
          ScanEligibility {
            recovering_partitions: vec![*partition_id],
            fast: false,
            fresh: false,
          },
        );
      }
      self.recovery_scan_last_partition = None;
      recovery_scan = true;
    } else {
      let next_partition_index = self.recovery_scan_last_partition.map_or(0, |partition_id| {
        recovering_partitions
          .iter()
          .position(|(candidate, ..)| *candidate > partition_id)
          .unwrap_or(0)
      });
      let (partition_id, recovery_start_window, recovery_cutover_window) =
        recovering_partitions[next_partition_index];
      self.recovery_scan_last_partition = Some(partition_id);
      let window_size_seconds = self.metadata_window_size.whole_seconds();
      for offset in 0 .. MAX_RECOVERY_WINDOWS_PER_SCAN {
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        let window_start =
          recovery_start_window.saturating_add(offset.saturating_mul(window_size_seconds));
        if window_start > recovery_cutover_window {
          break;
        }
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          window_start,
          self.recovery_scan_min_snowflake(partition_id, window_start),
          BTreeMap::new(),
          BTreeMap::from([(
            partition_id,
            self.recovery_scan_min_snowflake(partition_id, window_start),
          )]),
          true,
          ScanEligibility {
            recovering_partitions: vec![partition_id],
            fast: false,
            fresh: false,
          },
        );
      }
      recovery_scan = true;
    }

    for (&partition_id, state) in self
      .virtual_partition_states
      .iter()
      .filter(|(_, state)| state.is_assigned())
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
          BTreeMap::new(),
          BTreeMap::from([(partition_id, None)]),
          true,
          ScanEligibility {
            recovering_partitions: Vec::new(),
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
      let fast_windows = self.eligible_fast_scan_windows(now, runtime_settings)?;
      if let Some(first_fast_window) = fast_windows.first() {
        // The first normal Fast window begins at or after `safe_timestamp`. Retain exactly the
        // preceding window as a Fast tail for partitions whose last complete coverage ended there.
        // It contains the small interval that became safe since the previous pass. Older debt is
        // handled above by Recovery; querying more than this one tail would widen every Fast pass.
        let coverage_tail_window_start = first_fast_window
          .window_start_unix_seconds
          .saturating_sub(self.metadata_window_size.whole_seconds());
        let retention_floor = self.retention_floor_window_start(offset_datetime_from_unix_seconds(
          Window::for_timestamp(now, self.metadata_window_size)
            .start
            .unix_timestamp(),
        ));
        let coverage_tail_partition_bounds: BTreeMap<VirtualPartitionId, SnowflakeId> = self
          .fast_scan_partition_bounds(
            assigned_partition_ids,
            coverage_tail_window_start,
            now,
            runtime_settings,
          )
          .into_iter()
          .filter(|(partition_id, _)| {
            // Tail membership is per partition. A Fast frontier in the current window says
            // nothing about the previous window, so only the retained coverage-floor window can
            // certify that this partition still needs the tail query.
            self
              .virtual_partition_states
              .get(partition_id)
              .and_then(VirtualPartitionState::fast_coverage_floor)
              .is_some_and(|coverage_floor| {
                // Use the same retention clamp as Recovery selection. A stale floor can still
                // require the retained tail even when its original window has expired.
                Window::for_timestamp(
                  coverage_floor.max(retention_floor),
                  self.metadata_window_size,
                )
                .start
                .unix_timestamp()
                  == coverage_tail_window_start
              })
          })
          .collect();
        if !coverage_tail_partition_bounds.is_empty() {
          Self::insert_scan_request(
            &mut scan_requests,
            self.config.topic.as_str(),
            coverage_tail_window_start,
            coverage_tail_partition_bounds.values().copied().min(),
            coverage_tail_partition_bounds,
            BTreeMap::new(),
            false,
            ScanEligibility {
              recovering_partitions: Vec::new(),
              fast: true,
              fresh: false,
            },
          );
        }
      }
      for window in fast_windows {
        let fast_partition_bounds = self.fast_scan_partition_bounds(
          assigned_partition_ids,
          window.window_start_unix_seconds,
          now,
          runtime_settings,
        );
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          window.window_start_unix_seconds,
          fast_partition_bounds.values().copied().min(),
          fast_partition_bounds,
          BTreeMap::new(),
          false,
          ScanEligibility {
            recovering_partitions: Vec::new(),
            fast: true,
            fresh: false,
          },
        );
      }
    }

    Ok((scan_requests.into_values().collect(), recovery_scan))
  }

  /// Discard frontiers for windows that Fast scans no longer query.
  pub(in crate::consumer) fn prune_fast_frontiers(
    &mut self,
    now: time::OffsetDateTime,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<()> {
    let eligible_window_starts = self
      .eligible_fast_scan_windows(now, runtime_settings)?
      .into_iter()
      .map(|window| window.window_start_unix_seconds)
      .collect::<HashSet<_>>();
    self
      .fast_frontiers
      .retain(|(_, window_start), _| eligible_window_starts.contains(window_start));
    Ok(())
  }
}
