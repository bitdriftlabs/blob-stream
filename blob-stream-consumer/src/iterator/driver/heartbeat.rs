use super::{
  CommittedCursor,
  ConsumerCommittedCursorSnapshot,
  ConsumerDriver,
  HashMap,
  HeartbeatReport,
  HeartbeatTrigger,
  Instant,
  PendingCommit,
  Result,
  VirtualPartitionId,
  consumer_heartbeat_interval_ms,
  consumer_lease_duration_ms,
  debug,
  format_unix_timestamp_ms,
  trace,
};

impl ConsumerDriver {
  pub(in crate::iterator) fn heartbeat_retry_deadline_ms(&self) -> i64 {
    self
      .membership_lease_expires_at_ms
      .min(self.active_partition_lease_expiration_deadline_ms)
  }

  pub(in crate::iterator) fn record_heartbeat_failure(&self, started_at: Instant) {
    self.metrics.heartbeat_failures.inc();
    self
      .metrics
      .heartbeat_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  pub(in crate::iterator) fn record_membership_heartbeat_failure(&self) {
    self.metrics.membership_heartbeat_failures.inc();
  }

  pub(in crate::iterator) fn record_lease_heartbeat_failure(&self) {
    self.metrics.lease_heartbeat_failures.inc();
  }

  pub(in crate::iterator) fn record_successful_heartbeat(
    &self,
    now_ts_ms: i64,
    trigger: HeartbeatTrigger,
    report: &HeartbeatReport,
    pending_commits: &HashMap<VirtualPartitionId, PendingCommit>,
    started_at: Instant,
  ) {
    self
      .metrics
      .heartbeat_renewed_partitions
      .inc_by(u64::try_from(report.renewed_partitions.len()).unwrap_or(u64::MAX));
    if matches!(trigger, HeartbeatTrigger::Scheduled) {
      self
        .metrics
        .lease_renewed_partitions
        .inc_by(u64::try_from(report.renewed_partitions.len()).unwrap_or(u64::MAX));
    }
    self
      .metrics
      .heartbeat_fenced_partitions
      .inc_by(u64::try_from(report.fenced_partitions.len()).unwrap_or(u64::MAX));

    let committed_cursors = report
      .renewed_partitions
      .iter()
      .filter_map(|partition_id| {
        pending_commits
          .get(partition_id)
          .map(|commit| (*partition_id, commit.clone()))
      })
      .collect::<Vec<_>>();
    self
      .metrics
      .heartbeat_committed_offsets
      .inc_by(u64::try_from(committed_cursors.len()).unwrap_or(u64::MAX));
    self
      .metrics
      .cursor_commit_partitions
      .inc_by(u64::try_from(committed_cursors.len()).unwrap_or(u64::MAX));
    self
      .metrics
      .heartbeat_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());

    let mut shared_state = self.shared_state.lock();
    for (partition_id, committed_cursor) in committed_cursors {
      shared_state.diagnostics.last_committed_cursors.insert(
        partition_id,
        ConsumerCommittedCursorSnapshot {
          offset: committed_cursor.offset,
          source_checkpoint: Some(committed_cursor.source_checkpoint),
          committed_at_ms: Some(now_ts_ms),
        },
      );
      if let Some(partition_state) = shared_state.active_partitions.get_mut(&partition_id) {
        partition_state
          .delivered_source_ranges
          .retain(|range| range.end_offset > committed_cursor.offset);
      }
    }
    shared_state.diagnostics.last_successful_heartbeat_at_ms = Some(now_ts_ms);
  }

  pub(in crate::iterator) async fn heartbeat(
    &mut self,
    now_ts_ms: i64,
    trigger: HeartbeatTrigger,
  ) -> Result<HeartbeatReport> {
    let started_at = Instant::now();
    let pending_commits = self
      .shared_state
      .lock()
      .active_partitions
      .iter()
      .filter_map(|(partition_id, state)| {
        state
          .pending_commit
          .as_ref()
          .map(|commit| (*partition_id, commit.clone()))
      })
      .collect::<HashMap<_, _>>();
    self.metrics.heartbeat_calls.inc();
    match trigger {
      HeartbeatTrigger::Scheduled => self.metrics.heartbeat_scheduled_calls.inc(),
      HeartbeatTrigger::Commit => self.metrics.heartbeat_commit_calls.inc(),
    }
    trace!(
      "consumer heartbeat started: topic={}, group_id={}, member_id={}, trigger={}, \
       generation={}, active_partitions={:?}, pending_commits={}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      trigger.as_str(),
      self.coordinator.generation(),
      self.active_assignment,
      pending_commits.len()
    );

    if matches!(trigger, HeartbeatTrigger::Scheduled)
      && let Err(error) = self
        .membership_store
        .heartbeat_member(
          &self.group_config.topic,
          &self.group_config.group_id,
          &self.group_config.member_id,
          now_ts_ms,
          consumer_lease_duration_ms(&self.group_config),
        )
        .await
    {
      debug!(
        "consumer membership heartbeat failed: topic={}, group_id={}, member_id={}, trigger={}, \
         generation={}, elapsed_ms={}, error={error:#}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        trigger.as_str(),
        self.coordinator.generation(),
        started_at.elapsed().as_millis()
      );
      self.record_membership_heartbeat_failure();
      self.record_heartbeat_failure(started_at);
      if now_ts_ms >= self.heartbeat_retry_deadline_ms() {
        self
          .fence_active_partitions_after_heartbeat_failure(now_ts_ms)
          .await?;
      } else {
        debug!(
          "consumer retaining active partitions until membership lease expires: topic={}, \
           group_id={}, member_id={}, generation={}, expires_at={}",
          self.group_config.topic,
          self.group_config.group_id,
          self.group_config.member_id,
          self.coordinator.generation(),
          format_unix_timestamp_ms(self.membership_lease_expires_at_ms),
        );
      }
      return Err(error);
    }
    if matches!(trigger, HeartbeatTrigger::Scheduled) {
      self.membership_lease_expires_at_ms =
        now_ts_ms.saturating_add(consumer_lease_duration_ms(&self.group_config));
    }

    let committed_cursors = pending_commits
      .iter()
      .map(|(partition_id, commit)| {
        (
          *partition_id,
          CommittedCursor {
            virtual_partition_id: *partition_id,
            seq_end: commit.offset,
            source_checkpoint: Some(commit.source_checkpoint.clone()),
          },
        )
      })
      .collect();
    let coordinator_result = match trigger {
      HeartbeatTrigger::Scheduled => {
        self
          .coordinator
          .heartbeat_and_commit(now_ts_ms, &committed_cursors)
          .await
      },
      HeartbeatTrigger::Commit => {
        self
          .coordinator
          .commit_cursors(now_ts_ms, &committed_cursors)
          .await
      },
    };
    let report = match coordinator_result {
      Ok(report) => report,
      Err(error) => {
        self.reconcile_fenced_partitions(now_ts_ms).await?;
        if matches!(trigger, HeartbeatTrigger::Scheduled)
          && now_ts_ms >= self.heartbeat_retry_deadline_ms()
        {
          self
            .fence_active_partitions_after_heartbeat_failure(now_ts_ms)
            .await?;
        }
        debug!(
          "consumer coordinator heartbeat failed: topic={}, group_id={}, member_id={}, \
           trigger={}, generation={}, elapsed_ms={}, error={error:#}",
          self.group_config.topic,
          self.group_config.group_id,
          self.group_config.member_id,
          trigger.as_str(),
          self.coordinator.generation(),
          started_at.elapsed().as_millis()
        );
        self.record_lease_heartbeat_failure();
        self.record_heartbeat_failure(started_at);
        return Err(error);
      },
    };

    if matches!(trigger, HeartbeatTrigger::Scheduled) {
      self.active_partition_lease_expiration_deadline_ms =
        now_ts_ms.saturating_add(consumer_lease_duration_ms(&self.group_config));
    }
    self.record_successful_heartbeat(now_ts_ms, trigger, &report, &pending_commits, started_at);

    self.remove_fenced_partitions(&report.fenced_partitions, now_ts_ms)?;

    if matches!(trigger, HeartbeatTrigger::Scheduled) {
      self.next_heartbeat_at_ms = now_ts_ms + consumer_heartbeat_interval_ms(&self.group_config);
    }
    self.refresh_diagnostics();
    let committed_offsets = report
      .renewed_partitions
      .iter()
      .filter(|partition_id| pending_commits.contains_key(partition_id))
      .count();
    trace!(
      "consumer heartbeat completed: topic={}, group_id={}, member_id={}, trigger={}, \
       generation={}, renewed={:?}, fenced={:?}, committed_offsets={}, elapsed_ms={}, \
       next_heartbeat_at={}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      trigger.as_str(),
      self.coordinator.generation(),
      report.renewed_partitions,
      report.fenced_partitions,
      committed_offsets,
      started_at.elapsed().as_millis(),
      format_unix_timestamp_ms(self.next_heartbeat_at_ms)
    );
    Ok(report)
  }
}
