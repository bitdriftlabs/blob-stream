use super::{
  Arc,
  BlobStore,
  BrokerBlobRangeQuery,
  BrokerMetadataQuery,
  CommittedCursor,
  ConsumerReadConfig,
  ConsumerReaderImpl,
  ConsumerReaderMetrics,
  FeatureFlagsWatch,
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
  ensure,
  info,
  offset_datetime_from_unix_seconds,
  validate_read_config,
};
use crate::config::{ConsumerReadRuntimeSettings, consumer_max_clock_skew};
use crate::consumer::AvailabilityHorizon;
use crate::iterator::ConsumerSeekTarget;
use blob_stream_types::offset_datetime_from_unix_millis;
use time::{Duration, OffsetDateTime};

impl ConsumerReaderImpl {
  /// Create a reader with finite retention recovery and an explicit metadata publication bound.
  pub fn new(
    config: ConsumerReadConfig,
    assigned_virtual_partitions: Vec<VirtualPartitionId>,
    initial_cursors: HashMap<VirtualPartitionId, u64>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    broker_metadata_query: Arc<dyn BrokerMetadataQuery>,
    broker_blob_range_query: Arc<dyn BrokerBlobRangeQuery>,
    metrics_scope: &Scope,
    retention: Duration,
    maximum_metadata_publication_lag: Duration,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Result<Self> {
    validate_read_config(&config)?;
    ensure!(
      retention.is_positive(),
      "consumer retention recovery requires topic retention greater than zero"
    );
    ensure!(
      !maximum_metadata_publication_lag.is_negative(),
      "consumer maximum metadata publication lag must not be negative"
    );
    info!(
      "consumer reader initialized: topic={}, retention={}, maximum_metadata_publication_lag={}, \
       max_in_flight_batch_reads={}, assigned_partitions={}, initial_cursors={}",
      config.topic,
      retention,
      maximum_metadata_publication_lag,
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
      retention,
      maximum_metadata_publication_lag,
      metadata_window_size: blob_stream_types::DEFAULT_METADATA_WINDOW_SIZE,
      maximum_clock_skew: consumer_max_clock_skew(&config),
      metadata_cache_max_age: Duration::ZERO,
      fast_frontiers: HashMap::new(),
      recovery_scan_last_partition: None,
      recovery_metadata_cache: HashMap::new(),
      config,
      blob_store,
      metadata_store,
      broker_metadata_query,
      broker_blob_range_query,
      feature_flags,
      metrics: ConsumerReaderMetrics::new(metrics_scope),
    })
  }

  #[must_use]
  /// Override the typed clock-skew budget resolved from topic configuration.
  pub(crate) fn maximum_clock_skew(mut self, maximum_clock_skew: Duration) -> Self {
    self.maximum_clock_skew = maximum_clock_skew;
    self
  }

  #[must_use]
  /// Override the shared topic metadata-window contract for this reader.
  pub(crate) fn metadata_window_size(mut self, metadata_window_size: Duration) -> Self {
    self.metadata_window_size = metadata_window_size;
    self
  }

  #[must_use]
  /// Override the shared maximum age of retained eventual broker metadata.
  pub(crate) fn metadata_cache_max_age(mut self, metadata_cache_max_age: Duration) -> Self {
    self.metadata_cache_max_age = metadata_cache_max_age;
    self
  }

  #[must_use]
  /// Return the bounded availability horizon for one resolved read pass.
  pub(in crate::consumer) fn availability_horizon(
    &self,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> AvailabilityHorizon {
    AvailabilityHorizon::new(
      self.maximum_metadata_publication_lag,
      self.maximum_clock_skew,
      runtime_settings.metadata_visibility_delay,
      if runtime_settings.metadata_read_consistency
        == blob_stream_metadata_store::MetadataReadConsistency::Eventual
      {
        self.metadata_cache_max_age
      } else {
        Duration::ZERO
      },
    )
  }

  fn window_start(&self, timestamp: OffsetDateTime) -> OffsetDateTime {
    Window::for_timestamp(timestamp, self.metadata_window_size).start
  }

  fn retention_floor_window_start(&self, cutover_window_start: OffsetDateTime) -> OffsetDateTime {
    self.window_start(cutover_window_start.saturating_sub(self.retention))
  }

  /// Replace the current assignment set.
  pub fn set_assigned_virtual_partitions(
    &mut self,
    assigned_virtual_partitions: &[VirtualPartitionId],
    now: OffsetDateTime,
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
    let initial_window_start_unix_seconds = self.window_start(now).unix_timestamp();
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

  /// Discard a durable cursor and scan exactly the marker-selected metadata window.
  pub fn mark_fresh_at_window(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    target_window_start_unix_seconds: i64,
  ) {
    self.clear_recovery_metadata_cache(virtual_partition_id);
    self.virtual_partition_states.insert(
      virtual_partition_id,
      VirtualPartitionState::Fresh {
        cursor: None,
        initial_window_start_unix_seconds: target_window_start_unix_seconds,
        last_scan: None,
      },
    );
    self
      .fast_frontiers
      .retain(|(partition_id, _), _| *partition_id != virtual_partition_id);
    info!(
      "consumer partition marked fresh: topic={}, partition={}, target_window={}",
      self.config.topic, virtual_partition_id, target_window_start_unix_seconds
    );
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

  /// Reposition a partition cursor and recover from the caller-provided metadata source.
  pub fn seek(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    target: &ConsumerSeekTarget,
    now: OffsetDateTime,
  ) {
    let cutover_window_start = self.window_start(now);
    let target_window_start = self.window_start(offset_datetime_from_unix_seconds(
      target.window_start_unix_seconds,
    ));
    // Metadata keys exist only at aligned boundaries. A future target must not put Recovery past
    // this pass's cutover, so scan the current window and then transition to Fast instead.
    let recovery_start_window = target_window_start
      .min(cutover_window_start)
      .max(self.retention_floor_window_start(cutover_window_start));
    let starts_at_target_window = recovery_start_window == target_window_start;
    self.set_cursor(virtual_partition_id, target.offset);
    let runtime_settings =
      consumer_read_runtime_settings(&self.config, self.feature_flags.as_ref());
    let first_window_min_snowflake = starts_at_target_window
      .then(|| {
        target
          .snowflake_id
          .and_then(|snowflake_id| SnowflakeId(snowflake_id).timestamp())
          .map(|timestamp| {
            SnowflakeId::minimum_for_timestamp(
              timestamp
                .saturating_sub(self.availability_horizon(runtime_settings).duration())
                .max(recovery_start_window),
            )
          })
      })
      .flatten();
    let recovery_state = RecoveryState {
      next_window_start_unix_seconds: recovery_start_window.unix_timestamp(),
      cutover_window_start_unix_seconds: cutover_window_start.unix_timestamp(),
      first_window_start_unix_seconds: starts_at_target_window
        .then_some(recovery_start_window.unix_timestamp()),
      first_window_min_snowflake,
    };
    if let Some(state) = self.virtual_partition_states.get_mut(&virtual_partition_id) {
      state.start_recovery(recovery_state);
    } else {
      self.virtual_partition_states.insert(
        virtual_partition_id,
        VirtualPartitionState::PendingRecovering {
          cursor: target.offset,
          recovery_state,
          last_scan: None,
        },
      );
    }
    info!(
      "consumer seek recovery started: topic={}, partition={}, offset={}, requested_window={}, \
       recovery_start_window={}, cutover_window={}, has_snowflake_bound={}",
      self.config.topic,
      virtual_partition_id,
      target.offset,
      target.window_start_unix_seconds,
      recovery_start_window,
      cutover_window_start,
      first_window_min_snowflake.is_some(),
    );
  }

  /// Update cursor and plan finite-retention recovery from externally committed state.
  pub fn hydrate_cursor_with_source(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    committed_cursor: &CommittedCursor,
    committed_ts_ms: Option<i64>,
    now: OffsetDateTime,
  ) {
    // Hydration can arrive before assignment during a rebalance. The default Pending lifecycle
    // state retains the recovery plan without allowing the reader to scan this partition.
    // A later runtime consistency change affects new scan passes but deliberately does not rewrite
    // this hydrated source-window overlap or replay already planned recovery work.
    let cutover_window_start = self.window_start(now);
    let runtime_settings =
      consumer_read_runtime_settings(&self.config, self.feature_flags.as_ref());
    let retention_floor = self.retention_floor_window_start(cutover_window_start);
    let source_checkpoint = committed_cursor.source_checkpoint.as_ref();
    let source_window_start = committed_cursor
      .source_checkpoint
      .as_ref()
      .map(|checkpoint| offset_datetime_from_unix_seconds(checkpoint.window_start_unix_seconds))
      .or_else(|| {
        committed_ts_ms
          .map(offset_datetime_from_unix_millis)
          .map(|timestamp| self.window_start(timestamp))
      })
      .unwrap_or(retention_floor)
      .max(retention_floor);
    let first_window_min_snowflake = source_checkpoint
      .filter(|checkpoint| {
        offset_datetime_from_unix_seconds(checkpoint.window_start_unix_seconds)
          == source_window_start
          && source_window_start <= cutover_window_start
      })
      .and_then(|checkpoint| {
        let checkpoint_timestamp = SnowflakeId(checkpoint.snowflake_id).timestamp()?;
        Some(SnowflakeId::minimum_for_timestamp(
          checkpoint_timestamp
            .saturating_sub(self.availability_horizon(runtime_settings).duration())
            .max(source_window_start),
        ))
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
    if source_window_start <= cutover_window_start {
      self.virtual_partition_states.insert(
        virtual_partition_id,
        VirtualPartitionState::PendingRecovering {
          cursor: hydrated_cursor,
          recovery_state: RecoveryState {
            next_window_start_unix_seconds: source_window_start.unix_timestamp(),
            cutover_window_start_unix_seconds: cutover_window_start.unix_timestamp(),
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
        source_window_start,
        cutover_window_start
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
        source_window_start,
        cutover_window_start
      );
    }
  }
}
