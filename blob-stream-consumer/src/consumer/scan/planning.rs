use super::{
  BTreeMap,
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
  consumer_candidate_window_count,
  consumer_window_size_seconds,
  format_unix_timestamp_seconds,
  metadata_availability_delay_seconds,
  trace,
};

impl ConsumerReaderImpl {
  /// Return the bounded current-window horizon, from oldest candidate to newest.
  pub(in crate::consumer) fn scan_windows(
    &self,
    now_unix_seconds: i64,
  ) -> Result<Vec<TopicWindowKey>> {
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
    scan_states: &mut HashMap<VirtualPartitionId, ConsumerReaderPartitionScanState>,
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
  ) -> i64 {
    // This relies on the existing deployment assumption that broker and consumer clocks are
    // synchronized. Keep the two configured timing bounds together so a future skew margin has
    // one obvious place to join the safety calculation.
    now_unix_seconds.saturating_sub(metadata_availability_delay_seconds(
      &self.config,
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
  pub(in crate::consumer) fn recovery_scan_min_snowflake(
    &self,
    window_start_unix_seconds: i64,
  ) -> Option<SnowflakeId> {
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
  pub(in crate::consumer) fn scan_requests(
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
  pub(in crate::consumer) fn prune_fast_frontiers(&mut self, now_unix_seconds: i64) -> Result<()> {
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
}
