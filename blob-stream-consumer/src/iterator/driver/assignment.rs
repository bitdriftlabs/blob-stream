use super::{
  Arc,
  ConsumerDriver,
  ConsumerGroupAssignmentPlan,
  HashSet,
  RebalanceReport,
  Result,
  RevokedPartitionsImpl,
  Span,
  VirtualPartitionId,
  assignment_plan_snapshot,
  consumer_rebalance_interval_ms,
  emit_partition_handoff_snapshots,
  field,
  info,
  oneshot,
  update_total_prefetch_bytes,
  update_worker_prefetch_metrics,
};
use crate::iterator::NextResult;

impl ConsumerDriver {
  pub(in crate::iterator) fn record_accepted_assignment_plan(
    &self,
    plan: Option<ConsumerGroupAssignmentPlan>,
  ) {
    {
      let mut shared_state = self.shared_state.lock();
      // Keep reporting the last structurally valid plan when this rebalance did not receive a
      // replacement. It remains the best available explanation of desired ownership.
      if let Some(plan) = plan {
        shared_state.diagnostics.assignment_plan = Some(assignment_plan_snapshot(plan));
      }
    }
  }

  pub(in crate::iterator) fn record_rebalance_metrics(&self, report: &RebalanceReport) {
    // The coordinator classifies each claim so operational metrics separate steady retention from
    // recoveries that moved a partition after graceful release or lease expiry.
    if report.assignment_plan_applied {
      self.metrics.assignment_plans_applied_total.inc();
    }
    if report.rejected_assignment_plan_version.is_some() {
      self.metrics.assignment_plan_rejections_total.inc();
    }
    self
      .metrics
      .desired_partitions
      .set(i64::try_from(report.desired_partitions).unwrap_or(i64::MAX));
    self
      .metrics
      .owned_partitions
      .set(i64::try_from(report.owned_partitions.len()).unwrap_or(i64::MAX));
    self
      .metrics
      .lease_claims_initial
      .inc_by(u64::try_from(report.lease_claim_counts.initial).unwrap_or(u64::MAX));
    self
      .metrics
      .lease_claims_retained
      .inc_by(u64::try_from(report.lease_claim_counts.retained).unwrap_or(u64::MAX));
    self
      .metrics
      .lease_claims_graceful_handoff
      .inc_by(u64::try_from(report.lease_claim_counts.graceful_handoffs).unwrap_or(u64::MAX));
    self
      .metrics
      .lease_claims_expiry_takeover
      .inc_by(u64::try_from(report.lease_claim_counts.expiry_takeovers).unwrap_or(u64::MAX));
  }

  pub(in crate::iterator) fn apply_rebalance_report(
    &mut self,
    report: RebalanceReport,
  ) -> Result<Vec<VirtualPartitionId>> {
    let RebalanceReport {
      owned_partitions,
      recovered_cursors,
      accepted_assignment_plan,
      ..
    } = report;
    self.record_accepted_assignment_plan(accepted_assignment_plan);
    // Hydrate before exposing the reader assignment so the first scan starts from the durable
    // cursor claimed with this generation.
    self.hydrate_cursors(recovered_cursors, self.now_unix_seconds())?;

    self.apply_assignment(&owned_partitions)?;
    Ok(owned_partitions)
  }

  pub(in crate::iterator) fn apply_assignment(
    &mut self,
    assignment: &[VirtualPartitionId],
  ) -> Result<()> {
    // A normal rebalance can make an assignment visible to delivery immediately.
    let active_assignment = assignment.iter().copied().collect();
    self.apply_assignment_with_active_set(assignment, active_assignment, false)
  }

  pub(in crate::iterator) fn apply_assignment_after_revocation(
    &mut self,
    assignment: &[VirtualPartitionId],
  ) -> Result<()> {
    // A replacement assignment follows an application callback, so release its delivery fence
    // only after the reader has accepted its new assignment.
    let active_assignment = assignment.iter().copied().collect();
    self.apply_assignment_with_active_set(assignment, active_assignment, true)
  }

  pub(in crate::iterator) fn apply_assignment_with_active_set(
    &mut self,
    assignment: &[VirtualPartitionId],
    active_assignment: HashSet<VirtualPartitionId>,
    release_delivery_fence: bool,
  ) -> Result<()> {
    let assignment_changed = self.active_assignment != active_assignment;
    let handoff_phase = assignment_changed.then_some(
      if self.active_assignment.is_empty() {
        "startup_assigned"
      } else {
        "rebalance_assigned"
      },
    );
    let reader_owned_by_driver = self.reader.is_some();
    let mut newly_assigned = active_assignment
      .difference(&self.active_assignment)
      .copied()
      .collect::<Vec<_>>();
    newly_assigned.sort_unstable();
    // The reader can fail to accept its command. Do that fallible work before publishing the new
    // assignment to delivery or diagnostics, keeping their view consistent with the reader.
    self.set_reader_assignment(
      assignment.to_owned(),
      self.now_unix_seconds(),
      if reader_owned_by_driver {
        None
      } else {
        handoff_phase
      },
      release_delivery_fence,
    )?;
    self.active_assignment = active_assignment;
    self
      .metrics
      .active_partitions
      .set(i64::try_from(self.active_assignment.len()).unwrap_or(i64::MAX));
    {
      let mut shared_state = self.shared_state.lock();
      shared_state.apply_active_assignment(&self.active_assignment);
      self.refresh_diagnostics_locked(&mut shared_state);
    }
    if reader_owned_by_driver && let Some(handoff_phase) = handoff_phase {
      let assignment_span = bd_log::otel_info_span!(
        "blob_stream.consumer.assignment",
        otel.kind = "internal",
        consumer.topic = %self.group_config.topic,
        consumer.group_id = %self.group_config.group_id,
        consumer.member_id = %self.group_config.member_id,
        consumer.generation = self.coordinator.generation(),
        assignment.phase = handoff_phase,
        assignment.partition_count = assignment.len(),
        otel.status_code = field::Empty,
      );
      emit_partition_handoff_snapshots(
        &self.diagnostics.state_snapshot(),
        assignment,
        handoff_phase,
        "assigned",
        "not_applicable",
        &assignment_span,
      );
      assignment_span.record("otel.status_code", "OK");
    }
    if assignment_changed {
      self.metrics.assignment_applications_total.inc();
      info!(
        "consumer assignment active: topic={}, group_id={}, member_id={}, partitions={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        assignment
      );
    }
    if !newly_assigned.is_empty()
      && let Some(callback) = self.assignment_callback.lock().clone()
    {
      callback(&newly_assigned);
    }
    Ok(())
  }

  pub(in crate::iterator) async fn fence_active_partitions_after_heartbeat_failure(
    &mut self,
    now_ts_ms: i64,
  ) -> Result<()> {
    // Once renewal reaches its deadline, none of the current leases are safe to keep delivering.
    let fenced = self.active_assignment.iter().copied().collect();
    self
      .begin_fenced_partition_revocation(fenced, now_ts_ms)
      .await
  }

  pub(in crate::iterator) async fn reconcile_fenced_partitions(
    &mut self,
    now_ts_ms: i64,
  ) -> Result<()> {
    // A failed heartbeat can still reveal lease loss through the coordinator's retained ownership
    // view before the broader heartbeat deadline requires fencing every active partition.
    let owned = self
      .coordinator
      .owned_partitions()
      .into_iter()
      .collect::<HashSet<_>>();
    let fenced = self.active_assignment.difference(&owned).copied().collect();
    self
      .begin_fenced_partition_revocation(fenced, now_ts_ms)
      .await
  }

  pub(in crate::iterator) fn remove_fenced_partitions(
    &mut self,
    fenced_partitions: &[VirtualPartitionId],
    now_ts_ms: i64,
  ) -> Result<()> {
    if fenced_partitions.is_empty() {
      return Ok(());
    }

    // A successful heartbeat can report individual leases fenced without requiring the
    // application-level revocation callback used by a cooperative rebalance.
    for partition_id in fenced_partitions {
      self.active_assignment.remove(partition_id);
    }
    {
      let mut shared_state = self.shared_state.lock();
      shared_state.remove_fenced_partitions(fenced_partitions);
      self.refresh_diagnostics_locked(&mut shared_state);
    }
    self.set_reader_assignment(
      self.active_assignment.iter().copied().collect(),
      now_ts_ms / 1_000,
      None,
      false,
    )?;
    info!(
      "consumer heartbeat fenced partitions: topic={}, group_id={}, member_id={}, \
       fenced={fenced_partitions:?}",
      self.group_config.topic, self.group_config.group_id, self.group_config.member_id,
    );
    Ok(())
  }

  async fn begin_fenced_partition_revocation(
    &mut self,
    mut fenced: Vec<VirtualPartitionId>,
    now_ts_ms: i64,
  ) -> Result<()> {
    if self.pending_revocation_completion.is_some() {
      return Ok(());
    }

    fenced.sort_unstable();
    if fenced.is_empty() {
      return Ok(());
    }

    let fenced_set = fenced.iter().copied().collect::<HashSet<_>>();
    self.metrics.revocations.inc();
    let (completion_tx, completion_rx) = oneshot::channel();
    {
      let mut shared_state = self.shared_state.lock();
      let pending_bytes = shared_state.diagnostics.prefetch_pending_bytes;
      // Fenced progress no longer describes a local owner. Preserve active state until the
      // callback completes, but do not publish its old durable cursor as current local state.
      shared_state.discard_committed_cursor_diagnostics(&fenced);
      shared_state.delivery_state.drop_partitions(&fenced_set);
      shared_state.delivery_state.pending_revocation =
        Some(NextResult::Revoked(Box::new(RevokedPartitionsImpl {
          revoked: fenced.clone(),
          completion_tx: Some(completion_tx),
          completion_notify: Arc::clone(&self.revocation_notify),
        })));
      shared_state.delivery_state.revocation_in_progress = true;
      update_worker_prefetch_metrics(&self.metrics, &shared_state.delivery_state);
      update_total_prefetch_bytes(&self.metrics, &shared_state.delivery_state, pending_bytes);
      self.refresh_diagnostics_locked(&mut shared_state);
    }
    let pending_assignment = self
      .active_assignment
      .difference(&fenced_set)
      .copied()
      .collect::<Vec<_>>();
    self.set_reader_assignment(pending_assignment.clone(), now_ts_ms / 1_000, None, false)?;
    self.prefetch_space_notify.notify_waiters();
    self.pending_assignment = Some(pending_assignment);
    self.pending_revocation_completion = Some(completion_rx);
    self.pending_revocation_partitions = Some(fenced.clone());
    self.refresh_diagnostics();
    self.delivery_notify.notify_waiters();
    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .revocation_emitted(
          &self.group_config.member_id,
          self.coordinator.generation(),
          &fenced,
        )
        .await;
    }
    info!(
      "consumer active partitions fenced after heartbeat renewal failure: topic={}, group_id={}, \
       member_id={}, generation={}, fenced={fenced:?}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      self.coordinator.generation(),
    );
    Ok(())
  }

  pub(in crate::iterator) async fn finish_pending_revocation_if_completed(
    &mut self,
  ) -> Result<bool> {
    let Some(recv) = self.pending_revocation_completion.as_mut() else {
      return Ok(true);
    };

    match recv.try_recv() {
      Ok(()) | Err(oneshot::error::TryRecvError::Closed) => {
        // The application has drained delivery. Release first so a replacement owner cannot
        // process alongside this iterator, then publish the pending assignment.
        let revoked = self
          .pending_revocation_partitions
          .take()
          .unwrap_or_default();
        let handoff_snapshot = self
          .pending_revocation_snapshot
          .take()
          .unwrap_or_else(|| self.diagnostics.state_snapshot());
        let handoff_span = self
          .pending_revocation_span
          .take()
          .unwrap_or_else(Span::none);
        let release_result = self
          .coordinator
          .release_partitions(&revoked, self.now_unix_millis())
          .await;
        match &release_result {
          Ok(released_partitions) => {
            let released_partitions = released_partitions.iter().copied().collect::<HashSet<_>>();
            let (released, not_released): (Vec<_>, Vec<_>) = revoked
              .iter()
              .copied()
              .partition(|partition_id| released_partitions.contains(partition_id));
            if !released.is_empty() {
              emit_partition_handoff_snapshots(
                &handoff_snapshot,
                &released,
                "revocation_release_result",
                "released",
                "not_attempted",
                &handoff_span,
              );
            }
            if !not_released.is_empty() {
              emit_partition_handoff_snapshots(
                &handoff_snapshot,
                &not_released,
                "revocation_release_result",
                "not_released",
                "not_attempted",
                &handoff_span,
              );
            }
          },
          Err(_) => emit_partition_handoff_snapshots(
            &handoff_snapshot,
            &revoked,
            "revocation_release_result",
            "failed",
            "not_attempted",
            &handoff_span,
          ),
        }
        handoff_span.record(
          "handoff.lease_release_outcome",
          if release_result.is_ok() {
            "succeeded"
          } else {
            "failed"
          },
        );
        if let Err(error) = release_result {
          // Do not activate the replacement assignment. The driver records this as a terminal
          // failure with delivery fenced because the release outcome is unknown.
          handoff_span.record("handoff.assignment_outcome", "not_attempted");
          handoff_span.record("error.message", format!("lease release: {error:#}"));
          handoff_span.record("otel.status_code", "ERROR");
          return Err(error);
        }
        let assignment = self.pending_assignment.take().unwrap_or_default();
        self.pending_revocation_completion = None;
        match self.apply_assignment_after_revocation(&assignment) {
          Ok(()) => {
            if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
              lifecycle_hooks
                .rebalance_applied(
                  &self.group_config.member_id,
                  self.coordinator.generation(),
                  &assignment,
                )
                .await;
            }
            handoff_span.record("handoff.assignment_outcome", "succeeded");
            handoff_span.record("otel.status_code", "OK");
            Ok(true)
          },
          Err(error) => {
            handoff_span.record("handoff.assignment_outcome", "failed");
            handoff_span.record("error.message", format!("assignment: {error:#}"));
            handoff_span.record("otel.status_code", "ERROR");
            Err(error)
          },
        }
      },
      Err(oneshot::error::TryRecvError::Empty) => Ok(false),
    }
  }

  pub(in crate::iterator) async fn maybe_rebalance(&mut self, now_ts_ms: i64) -> Result<()> {
    if now_ts_ms < self.next_rebalance_at_ms {
      return Ok(());
    }

    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .before_rebalance(&self.group_config.member_id, self.coordinator.generation())
        .await;
    }
    let snapshot = self.coordination_source.snapshot().await?;
    self.record_coordination_snapshot(&snapshot);
    self.metrics.rebalances_total.inc();
    // The coordinator validates the shared plan, claims the resulting leases, and returns both
    // the owned set and any durable cursors that must be recovered before delivery resumes.
    let report = match self
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now_ts_ms)
      .await
    {
      Ok(report) => report,
      Err(error) => {
        self.metrics.rebalance_failures_total.inc();
        return Err(error);
      },
    };
    self.record_rebalance_metrics(&report);
    let RebalanceReport {
      owned_partitions: next_assignment,
      active_partition_lease_expiration_deadline_ms,
      recovered_cursors,
      accepted_assignment_plan,
      retry_error,
      ..
    } = report;
    self.record_accepted_assignment_plan(accepted_assignment_plan);

    self.next_rebalance_at_ms = now_ts_ms + consumer_rebalance_interval_ms(&self.group_config);

    let next_assignment_set = next_assignment.iter().copied().collect::<HashSet<_>>();
    let assignment_changed = self.active_assignment != next_assignment_set;
    if let Some(active_partition_lease_expiration_deadline_ms) =
      active_partition_lease_expiration_deadline_ms
    {
      self.active_partition_lease_expiration_deadline_ms =
        active_partition_lease_expiration_deadline_ms;
    }
    self.hydrate_cursors(recovered_cursors, now_ts_ms / 1_000)?;

    if !assignment_changed {
      // Ownership is unchanged, but retain plan and retry diagnostics so an incomplete rebalance
      // remains observable without disrupting active delivery.
      self.refresh_rebalance_diagnostics();
      return retry_error.map_or_else(|| Ok(()), |error| Err(anyhow::Error::msg(error)));
    }

    let revoked = self
      .active_assignment
      .difference(&next_assignment_set)
      .copied()
      .collect::<Vec<_>>();

    if revoked.is_empty() {
      // Added partitions can be activated without an application drain because no delivered
      // records need to be handed off from this iterator.
      self.apply_assignment_with_active_set(&next_assignment, next_assignment_set, false)?;
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .rebalance_applied(
            &self.group_config.member_id,
            self.coordinator.generation(),
            &next_assignment,
          )
          .await;
      }
      return retry_error.map_or_else(|| Ok(()), |error| Err(anyhow::Error::msg(error)));
    }

    self.metrics.revocations.inc();
    let revoked_set = revoked.iter().copied().collect::<HashSet<_>>();
    let (completion_tx, completion_rx) = oneshot::channel();
    {
      let mut shared_state = self.shared_state.lock();
      let pending_bytes = shared_state.diagnostics.prefetch_pending_bytes;
      let delivery_state = &mut shared_state.delivery_state;
      // Cooperative revocation differs from lease fencing: retain active state and its cursor
      // until the application acknowledges the callback, allowing it to commit final work.
      delivery_state.drop_partitions(&revoked_set);
      delivery_state.pending_revocation =
        Some(NextResult::Revoked(Box::new(RevokedPartitionsImpl {
          revoked: revoked.clone(),
          completion_tx: Some(completion_tx),
          completion_notify: Arc::clone(&self.revocation_notify),
        })));
      delivery_state.revocation_in_progress = true;
      update_worker_prefetch_metrics(&self.metrics, delivery_state);
      update_total_prefetch_bytes(&self.metrics, delivery_state, pending_bytes);
    }
    self.prefetch_space_notify.notify_waiters();

    self.pending_assignment = Some(next_assignment);
    self.pending_revocation_completion = Some(completion_rx);
    self.pending_revocation_partitions = Some(revoked.clone());
    self.refresh_diagnostics();
    let handoff_snapshot = self.diagnostics.state_snapshot();
    let handoff_span = bd_log::otel_info_span!(
      "blob_stream.consumer.revocation_handoff",
      otel.kind = "internal",
      consumer.topic = %self.group_config.topic,
      consumer.group_id = %self.group_config.group_id,
      consumer.member_id = %self.group_config.member_id,
      consumer.generation = self.coordinator.generation(),
      handoff.revoked_partition_count = revoked.len(),
      handoff.lease_release_outcome = field::Empty,
      handoff.assignment_outcome = field::Empty,
      error.message = field::Empty,
      otel.status_code = field::Empty,
    );
    emit_partition_handoff_snapshots(
      &handoff_snapshot,
      &revoked,
      "revocation_pre_release",
      "awaiting_application_ack",
      "not_attempted",
      &handoff_span,
    );
    self.pending_revocation_snapshot = Some(handoff_snapshot);
    self.pending_revocation_span = Some(handoff_span);
    self.delivery_notify.notify_waiters();
    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .revocation_emitted(
          &self.group_config.member_id,
          self.coordinator.generation(),
          &revoked,
        )
        .await;
    }

    info!(
      "consumer revocation requested: topic={}, group_id={}, member_id={}, revoked={:?}",
      self.group_config.topic, self.group_config.group_id, self.group_config.member_id, revoked
    );

    retry_error.map_or_else(|| Ok(()), |error| Err(anyhow::Error::msg(error)))
  }
}
