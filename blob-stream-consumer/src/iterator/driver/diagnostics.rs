use super::{ConsumerDriver, ConsumerSharedState, CoordinationSnapshot, OffsetDateTimeExt, info};

impl ConsumerDriver {
  pub(in crate::iterator) fn now_unix_millis(&self) -> i64 {
    self.time_provider.now().unix_timestamp_ms()
  }

  pub(in crate::iterator) fn now_unix_seconds(&self) -> i64 {
    self.time_provider.now().unix_timestamp()
  }

  pub(in crate::iterator) fn record_coordination_snapshot(
    &mut self,
    snapshot: &CoordinationSnapshot,
  ) {
    let Some(previous) = self.last_coordination_snapshot.as_ref() else {
      info!(
        "consumer membership snapshot initialized: topic={}, group_id={}, member_id={}, \
         members={:?}, partitions={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        snapshot.members,
        snapshot.virtual_partitions
      );
      self.last_coordination_snapshot = Some(snapshot.clone());
      return;
    };

    if previous == snapshot {
      return;
    }

    if previous.members != snapshot.members {
      info!(
        "consumer membership changed: topic={}, group_id={}, member_id={}, previous_members={:?}, \
         members={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        previous.members,
        snapshot.members
      );
    }

    if previous.virtual_partitions != snapshot.virtual_partitions {
      info!(
        "consumer partition space changed: topic={}, group_id={}, member_id={}, \
         previous_partitions={:?}, partitions={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        previous.virtual_partitions,
        snapshot.virtual_partitions
      );
    }

    self.last_coordination_snapshot = Some(snapshot.clone());
  }

  pub(in crate::iterator) fn refresh_diagnostics(&self) {
    let mut shared_state = self.shared_state.lock();
    self.refresh_diagnostics_locked(&mut shared_state);
  }

  pub(in crate::iterator) fn refresh_diagnostics_locked(
    &self,
    shared_state: &mut ConsumerSharedState,
  ) {
    let diagnostics = &mut shared_state.diagnostics;
    diagnostics.accepted_assignment_plan_version = self.coordinator.generation();
    diagnostics.owned_partitions = self.coordinator.owned_partitions();
    diagnostics.active_assignment = self.active_assignment.iter().copied().collect();
    diagnostics
      .pending_assignment
      .clone_from(&self.pending_assignment);
    diagnostics.pending_revocation = self.pending_revocation_completion.is_some();
    diagnostics.next_heartbeat_at_ms = self.next_heartbeat_at_ms;
    diagnostics.next_rebalance_at_ms = self.next_rebalance_at_ms;
    diagnostics.started = self.started;
    diagnostics.prefetch_worker_running = self.prefetch_task.is_some();
  }

  pub(in crate::iterator) fn refresh_rebalance_diagnostics(&self) {
    let mut shared_state = self.shared_state.lock();
    let diagnostics = &mut shared_state.diagnostics;
    diagnostics.accepted_assignment_plan_version = self.coordinator.generation();
    diagnostics.next_rebalance_at_ms = self.next_rebalance_at_ms;
    diagnostics.started = self.started;
    diagnostics.prefetch_worker_running = self.prefetch_task.is_some();
  }
}
