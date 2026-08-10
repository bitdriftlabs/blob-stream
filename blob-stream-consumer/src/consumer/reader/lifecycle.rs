use super::{
  Arc,
  BlobStore,
  CommittedCursor,
  ConsumerReadConfig,
  ConsumerReaderImpl,
  ConsumerReaderMetrics,
  FeatureFlagsWatch,
  HISTORICAL_SEEK_RECOVERY_SECONDS,
  HashMap,
  HashSet,
  MetadataStore,
  RecoveryState,
  Result,
  Scope,
  SnowflakeId,
  VirtualPartitionId,
  VirtualPartitionState,
  Window,
  consumer_read_runtime_settings,
  consumer_window_size_seconds,
  ensure,
  format_unix_timestamp_seconds,
  info,
  metadata_availability_delay_seconds,
  validate_read_config,
};

impl ConsumerReaderImpl {
  /// Create a reader with finite retention recovery and an explicit metadata publication bound.
  pub fn new(
    config: ConsumerReadConfig,
    assigned_virtual_partitions: Vec<VirtualPartitionId>,
    initial_cursors: HashMap<VirtualPartitionId, u64>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    metrics_scope: &Scope,
    retention_days: u32,
    maximum_metadata_publication_lag_ms: u64,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Result<Self> {
    validate_read_config(&config)?;
    ensure!(
      retention_days > 0,
      "consumer retention recovery requires topic retention_days greater than zero"
    );
    info!(
      "consumer reader initialized: topic={}, retention_days={}, \
       maximum_metadata_publication_lag_ms={}, max_in_flight_batch_reads={}, \
       assigned_partitions={}, initial_cursors={}",
      config.topic,
      retention_days,
      maximum_metadata_publication_lag_ms,
      consumer_read_runtime_settings(&config, feature_flags.as_ref()).max_in_flight_batch_reads,
      assigned_virtual_partitions.len(),
      initial_cursors.len()
    );

    let mut virtual_partition_states = initial_cursors
      .into_iter()
      .map(|(partition_id, cursor)| {
        (
          partition_id,
          VirtualPartitionState::PendingCursor {
            cursor,
            last_scan: None,
          },
        )
      })
      .collect::<HashMap<_, _>>();
    for partition_id in assigned_virtual_partitions {
      let cursor = virtual_partition_states
        .remove(&partition_id)
        .and_then(|state| state.cursor());
      virtual_partition_states.insert(
        partition_id,
        VirtualPartitionState::Fast {
          cursor,
          last_scan: None,
        },
      );
    }

    Ok(Self {
      virtual_partition_states,
      retention_days,
      maximum_metadata_publication_lag_ms,
      fast_frontiers: HashMap::new(),
      recovery_scan_last_partition: None,
      recovery_metadata_cache: HashMap::new(),
      config,
      blob_store,
      metadata_store,
      feature_flags,
      metrics: ConsumerReaderMetrics::new(metrics_scope),
    })
  }

  fn window_start(&self, unix_seconds: i64) -> i64 {
    Window::for_timestamp(unix_seconds, consumer_window_size_seconds(&self.config))
      .start_unix_seconds
  }

  fn retention_floor_window_start(&self, cutover_window_start_unix_seconds: i64) -> i64 {
    let retention_seconds = i64::from(self.retention_days).saturating_mul(86_400);
    self.window_start(cutover_window_start_unix_seconds.saturating_sub(retention_seconds))
  }

  /// Replace the current assignment set.
  pub fn set_assigned_virtual_partitions(
    &mut self,
    assigned_virtual_partitions: &[VirtualPartitionId],
    now_unix_seconds: i64,
  ) -> Result<()> {
    let previous_assignment = self.assigned_virtual_partition_ids();
    let assigned = assigned_virtual_partitions
      .iter()
      .copied()
      .collect::<HashSet<_>>();
    if previous_assignment != assigned_virtual_partitions {
      self
        .fast_frontiers
        .retain(|(partition_id, _), _| assigned.contains(partition_id));
    }
    let cached_entry_count = self.recovery_metadata_cache.len();
    self
      .recovery_metadata_cache
      .retain(|(partition_id, ..), _| assigned.contains(partition_id));
    self.metrics.record_recovery_metadata_cache_invalidation(
      cached_entry_count.saturating_sub(self.recovery_metadata_cache.len()),
    );
    self.record_recovery_metadata_cache_state();
    // A revoked partition has no reader-local work left after the iterator drains it. Its durable
    // cursor belongs in the consumer-group lease and is hydrated again if this reader reacquires
    // the partition.
    self
      .virtual_partition_states
      .retain(|partition_id, _| assigned.contains(partition_id));
    let initial_window_start_unix_seconds = self.window_start(now_unix_seconds);
    for partition_id in assigned_virtual_partitions {
      let state = self.virtual_partition_states.remove(partition_id);
      let state = state.map_or_else(
        || VirtualPartitionState::fresh(initial_window_start_unix_seconds),
        |state| state.into_assigned(initial_window_start_unix_seconds),
      );
      self.virtual_partition_states.insert(*partition_id, state);
    }
    let current_assignment = self.assigned_virtual_partition_ids();
    if current_assignment != previous_assignment {
      let previous = previous_assignment.into_iter().collect::<HashSet<_>>();
      let current = current_assignment.iter().copied().collect::<HashSet<_>>();
      let mut added = current.difference(&previous).copied().collect::<Vec<_>>();
      let mut removed = previous.difference(&current).copied().collect::<Vec<_>>();
      added.sort_unstable();
      removed.sort_unstable();
      let (fresh, recovering, fast) = self.partition_read_mode_counts();
      info!(
        "consumer reader assignment updated: topic={}, assigned={current_assignment:?}, \
         added={added:?}, removed={removed:?}, fresh_partitions={fresh}, \
         recovering_partitions={recovering}, fast_partitions={fast}",
        self.config.topic
      );
    }
    Ok(())
  }

  /// Set the in-memory cursor for a virtual partition and reset its fast scan frontier.
  pub fn set_cursor(&mut self, virtual_partition_id: VirtualPartitionId, seq_end: u64) {
    self.clear_recovery_metadata_cache(virtual_partition_id);
    self
      .virtual_partition_states
      .entry(virtual_partition_id)
      .and_modify(|state| state.set_cursor(seq_end))
      .or_insert(VirtualPartitionState::PendingFast {
        cursor: seq_end,
        last_scan: None,
      });
    self
      .fast_frontiers
      .retain(|(partition_id, _), _| *partition_id != virtual_partition_id);
  }

  fn clear_recovery_metadata_cache(&mut self, virtual_partition_id: VirtualPartitionId) {
    let cached_entry_count = self.recovery_metadata_cache.len();
    self
      .recovery_metadata_cache
      .retain(|(partition_id, ..), _| *partition_id != virtual_partition_id);
    self.metrics.record_recovery_metadata_cache_invalidation(
      cached_entry_count.saturating_sub(self.recovery_metadata_cache.len()),
    );
    self.record_recovery_metadata_cache_state();
  }

  /// Reposition a partition cursor and recover recent windows before returning to the fast path.
  pub fn seek(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    seq_end: u64,
    now_unix_seconds: i64,
  ) {
    self.set_cursor(virtual_partition_id, seq_end);
    let cutover_window_start_unix_seconds = self.window_start(now_unix_seconds);
    let recovery_start_window_unix_seconds = self
      .window_start(now_unix_seconds.saturating_sub(HISTORICAL_SEEK_RECOVERY_SECONDS))
      .max(self.retention_floor_window_start(cutover_window_start_unix_seconds));
    let recovery_state = RecoveryState {
      next_window_start_unix_seconds: recovery_start_window_unix_seconds,
      cutover_window_start_unix_seconds,
      first_window_start_unix_seconds: None,
      first_window_min_snowflake: None,
    };
    if let Some(state) = self.virtual_partition_states.get_mut(&virtual_partition_id) {
      state.start_recovery(recovery_state);
    } else {
      self.virtual_partition_states.insert(
        virtual_partition_id,
        VirtualPartitionState::PendingRecovering {
          cursor: seq_end,
          recovery_state,
          last_scan: None,
        },
      );
    }
    info!(
      "consumer historical seek recovery started: topic={}, partition={}, offset={}, \
       recovery_start_window={}, cutover_window={}",
      self.config.topic,
      virtual_partition_id,
      seq_end,
      format_unix_timestamp_seconds(recovery_start_window_unix_seconds),
      format_unix_timestamp_seconds(cutover_window_start_unix_seconds)
    );
  }

  /// Update cursor and plan finite-retention recovery from externally committed state.
  pub fn hydrate_cursor_with_source(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    committed_cursor: &CommittedCursor,
    committed_ts_ms: Option<i64>,
    now_unix_seconds: i64,
  ) {
    // Hydration can arrive before assignment during a rebalance. The default Pending lifecycle
    // state retains the recovery plan without allowing the reader to scan this partition.
    // A later runtime consistency change affects new scan passes but deliberately does not rewrite
    // this hydrated source-window overlap or replay already planned recovery work.
    let cutover_window_start_unix_seconds = self.window_start(now_unix_seconds);
    let runtime_settings =
      consumer_read_runtime_settings(&self.config, self.feature_flags.as_ref());
    let retention_floor = self.retention_floor_window_start(cutover_window_start_unix_seconds);
    let source_checkpoint = committed_cursor.source_checkpoint.as_ref();
    let source_window_start_unix_seconds = committed_cursor
      .source_checkpoint
      .as_ref()
      .map(|checkpoint| checkpoint.window_start_unix_seconds)
      .or_else(|| committed_ts_ms.map(|timestamp_ms| self.window_start(timestamp_ms / 1_000)))
      .unwrap_or(retention_floor)
      .max(retention_floor);
    let first_window_min_snowflake = source_checkpoint
      .filter(|checkpoint| {
        checkpoint.window_start_unix_seconds == source_window_start_unix_seconds
          && source_window_start_unix_seconds <= cutover_window_start_unix_seconds
      })
      .and_then(|checkpoint| {
        let checkpoint_timestamp = SnowflakeId(checkpoint.snowflake_id).timestamp()?;
        let floor_timestamp_unix_seconds = checkpoint_timestamp
          .unix_timestamp()
          .saturating_sub(metadata_availability_delay_seconds(
            runtime_settings.metadata_visibility_delay_ms,
            self.maximum_metadata_publication_lag_ms,
          ))
          .max(source_window_start_unix_seconds);
        time::OffsetDateTime::from_unix_timestamp(floor_timestamp_unix_seconds)
          .ok()
          .map(SnowflakeId::minimum_for_timestamp)
      });
    let hydrated_cursor = self
      .virtual_partition_states
      .get(&virtual_partition_id)
      .and_then(VirtualPartitionState::cursor)
      .map_or(committed_cursor.seq_end, |cursor| {
        cursor.max(committed_cursor.seq_end)
      });
    let last_scan = self
      .virtual_partition_states
      .get(&virtual_partition_id)
      .and_then(VirtualPartitionState::last_scan_handle);
    if let Some(state) = self.virtual_partition_states.get_mut(&virtual_partition_id)
      && !matches!(state, VirtualPartitionState::PendingCursor { .. })
    {
      state.advance_cursor(committed_cursor.seq_end);
      return;
    }
    self.clear_recovery_metadata_cache(virtual_partition_id);
    if source_window_start_unix_seconds <= cutover_window_start_unix_seconds {
      self.virtual_partition_states.insert(
        virtual_partition_id,
        VirtualPartitionState::PendingRecovering {
          cursor: hydrated_cursor,
          recovery_state: RecoveryState {
            next_window_start_unix_seconds: source_window_start_unix_seconds,
            cutover_window_start_unix_seconds,
            first_window_start_unix_seconds: source_checkpoint
              .map(|checkpoint| checkpoint.window_start_unix_seconds),
            first_window_min_snowflake,
          },
          last_scan,
        },
      );
      info!(
        "consumer partition recovery started: topic={}, partition={}, committed_offset={}, \
         recovery_start_window={}, cutover_window={}",
        self.config.topic,
        virtual_partition_id,
        committed_cursor.seq_end,
        format_unix_timestamp_seconds(source_window_start_unix_seconds),
        format_unix_timestamp_seconds(cutover_window_start_unix_seconds)
      );
    } else {
      self.virtual_partition_states.insert(
        virtual_partition_id,
        VirtualPartitionState::PendingFast {
          cursor: hydrated_cursor,
          last_scan,
        },
      );
      info!(
        "consumer partition resumed on fast path: topic={}, partition={}, committed_offset={}, \
         source_window={}, cutover_window={}",
        self.config.topic,
        virtual_partition_id,
        committed_cursor.seq_end,
        format_unix_timestamp_seconds(source_window_start_unix_seconds),
        format_unix_timestamp_seconds(cutover_window_start_unix_seconds)
      );
    }
  }
}
