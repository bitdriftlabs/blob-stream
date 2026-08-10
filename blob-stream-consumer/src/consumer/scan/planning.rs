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
  consumer_candidate_window_count_with_visibility_delay,
  consumer_window_size_seconds,
  format_unix_timestamp_seconds,
  metadata_availability_delay_seconds,
  trace,
};
use crate::consumer::reader::RecoveryMetadataCacheKey;

impl ConsumerReaderImpl {
  /// Return the cache identity for an immutable, recovery-only metadata request.
  ///
  /// The key intentionally excludes consistency: runtime changes do not invalidate a completed
  /// recovery observation or replay it with stronger reads.
  pub(in crate::consumer) fn mature_recovery_metadata_cache_key(
    &self,
    request: &ScanRequest,
    now_unix_seconds: i64,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Option<RecoveryMetadataCacheKey> {
    let &[partition_id] = request.eligibility.recovering_partitions.as_slice() else {
      return None;
    };
    let window_end_unix_seconds = request
      .window
      .window_start_unix_seconds
      .saturating_add(consumer_window_size_seconds(&self.config));
    if !request.recovery_scan
      || request.eligibility.fast
      || request.eligibility.fresh
      || window_end_unix_seconds
        > self.fast_scan_safe_timestamp_unix_seconds(now_unix_seconds, runtime_settings)
    {
      return None;
    }
    Some((
      partition_id,
      request.window.window_start_unix_seconds,
      request.min_snowflake.map(SnowflakeId::as_u64),
    ))
  }

  /// Return the bounded current-window horizon, from oldest candidate to newest.
  pub(in crate::consumer) fn scan_windows(
    &self,
    now_unix_seconds: i64,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<TopicWindowKey>> {
    // Anchor scans to the current window and cover the enforced publication deadline plus the
    // effective visibility delay. Oldest -> newest ordering keeps traversal deterministic.
    let current_window =
      Window::for_timestamp(now_unix_seconds, consumer_window_size_seconds(&self.config))
        .start_unix_seconds;

    let candidate_windows = consumer_candidate_window_count_with_visibility_delay(
      &self.config,
      self.maximum_metadata_publication_lag_ms,
      runtime_settings.metadata_visibility_delay_ms,
    )?;
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

  /// Combine mode-specific demand for one window into one metadata-store query.
  pub(in crate::consumer) fn insert_scan_request(
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
        recovery_scan,
        eligibility,
      });
  }

  /// Find the least inclusive lower bound required when one query serves several fast partitions.
  pub(in crate::consumer) fn fast_scan_min_snowflake(
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
    let partition_lower_bound =
      observed_frontier.map_or(safe_floor, |frontier| frontier.max(safe_floor));
    Some((observed_frontier, partition_lower_bound))
  }

  /// Record the time and frontier inputs that determined each Fast partition's shared query.
  pub(in crate::consumer) fn record_fast_scan_bounds(
    &self,
    scan_requests: &[ScanRequest],
    assigned_partition_ids: &[VirtualPartitionId],
    now_unix_seconds: i64,
    runtime_settings: ConsumerReadRuntimeSettings,
    scan_states: &mut HashMap<VirtualPartitionId, ConsumerReaderPartitionScanState>,
  ) {
    let safe_timestamp_unix_seconds =
      self.fast_scan_safe_timestamp_unix_seconds(now_unix_seconds, runtime_settings);
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
            .push(ConsumerReaderFastScanBoundState {
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
  pub(in crate::consumer) fn fast_scan_safe_timestamp_unix_seconds(
    &self,
    now_unix_seconds: i64,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> i64 {
    // This relies on the existing deployment assumption that broker and consumer clocks are
    // synchronized. Keep the two configured timing bounds together so a future skew margin has
    // one obvious place to join the safety calculation.
    now_unix_seconds.saturating_sub(metadata_availability_delay_seconds(
      runtime_settings.metadata_visibility_delay_ms,
      self.maximum_metadata_publication_lag_ms,
    ))
  }

  /// Return the lowest possible segment ID for a timestamp, preserving safety for invalid input.
  pub(in crate::consumer) fn snowflake_floor(timestamp_unix_seconds: i64) -> SnowflakeId {
    time::OffsetDateTime::from_unix_timestamp(timestamp_unix_seconds)
      .map_or(SnowflakeId(0), SnowflakeId::minimum_for_timestamp)
  }

  /// Return Fast windows that can still contain unpublished or invisible metadata and their floors.
  pub(in crate::consumer) fn eligible_fast_scan_windows(
    &self,
    now_unix_seconds: i64,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<(TopicWindowKey, SnowflakeId)>> {
    let safe_timestamp_unix_seconds =
      self.fast_scan_safe_timestamp_unix_seconds(now_unix_seconds, runtime_settings);
    let window_size_seconds = consumer_window_size_seconds(&self.config);
    let windows = self
      .scan_windows(now_unix_seconds, runtime_settings)?
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

  /// Build a scan pass that prioritizes bounded recovery before using the fast path.
  pub(in crate::consumer) fn scan_requests(
    &mut self,
    now_unix_seconds: i64,
    assigned_partition_ids: &[VirtualPartitionId],
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<(Vec<ScanRequest>, bool)> {
    let mut scan_requests = BTreeMap::new();
    let mut recovery_scan = false;

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
      // All partitions need only their active cutover window. They can share one query without
      // allowing a dense historical recovery to monopolize subsequent capacity-limited passes.
      for (partition_id, recovery_start_window, _) in &recovering_partitions {
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          *recovery_start_window,
          self.recovery_scan_min_snowflake(*partition_id, *recovery_start_window),
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
      let window_size_seconds = consumer_window_size_seconds(&self.config);
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
      for (window, safe_floor) in
        self.eligible_fast_scan_windows(now_unix_seconds, runtime_settings)?
      {
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
    now_unix_seconds: i64,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<()> {
    let eligible_window_starts = self
      .eligible_fast_scan_windows(now_unix_seconds, runtime_settings)?
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
}
