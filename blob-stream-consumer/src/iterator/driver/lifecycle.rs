use super::{
  ConsumerDriver,
  HashSet,
  HeartbeatReport,
  HeartbeatTrigger,
  Result,
  VirtualPartitionId,
  anyhow,
  debug,
  emit_partition_handoff_snapshots,
  ensure,
  field,
  info,
  oneshot,
  trace,
  update_total_prefetch_bytes,
  update_worker_prefetch_metrics,
};

impl ConsumerDriver {
  pub(in crate::iterator) async fn commit(&mut self) -> Result<HeartbeatReport> {
    ensure!(
      self.started,
      "consumer iterator must be started before commit"
    );
    debug!(
      "consumer commit requested heartbeat: topic={}, group_id={}, member_id={}, generation={}, \
       active_partitions={:?}, pending_commit_partitions={}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      self.coordinator.generation(),
      self.active_assignment,
      self
        .shared_state
        .lock()
        .active_partitions
        .values()
        .filter(|state| state.pending_commit.is_some())
        .count()
    );
    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .before_commit(&self.group_config.member_id, self.coordinator.generation())
        .await;
    }
    self
      .heartbeat(self.now_unix_millis(), HeartbeatTrigger::Commit)
      .await
  }

  pub(in crate::iterator) async fn shutdown(&mut self) -> Result<()> {
    if self.started {
      // Attempt commit and explicit lease release before membership deregistration.
      // Releasing leases proactively shortens rebalance convergence on graceful shutdown.
      let shutdown_span = bd_log::otel_info_span!(
        "blob_stream.consumer.shutdown",
        otel.kind = "internal",
        consumer.topic = %self.group_config.topic,
        consumer.group_id = %self.group_config.group_id,
        consumer.member_id = %self.group_config.member_id,
        consumer.generation = self.coordinator.generation(),
        shutdown.commit_outcome = field::Empty,
        shutdown.lease_release_outcome = field::Empty,
        shutdown.deregistration_outcome = field::Empty,
        shutdown.planner_release_outcome = field::Empty,
        error.message = field::Empty,
        otel.status_code = field::Empty,
      );
      let commit_result = self.commit().await;
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .shutdown_commit_finished(&self.group_config.member_id, self.coordinator.generation())
          .await;
      }
      self.refresh_diagnostics();
      let mut handoff_partitions = self.coordinator.owned_partitions();
      handoff_partitions.extend(self.active_assignment.iter().copied());
      handoff_partitions.sort_unstable();
      handoff_partitions.dedup();
      let handoff_snapshot = self.diagnostics.state_snapshot();
      emit_partition_handoff_snapshots(
        &handoff_snapshot,
        &handoff_partitions,
        "shutdown_pre_release",
        "pending_release",
        if commit_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
        &shutdown_span,
      );
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .before_release_owned(&self.group_config.member_id, self.coordinator.generation())
          .await;
      }
      let release_result = self.coordinator.release_owned(self.now_unix_millis()).await;
      match &release_result {
        Ok(released_partitions) => {
          let released_partitions = released_partitions.iter().copied().collect::<HashSet<_>>();
          let (released, not_released): (Vec<_>, Vec<_>) = handoff_partitions
            .iter()
            .copied()
            .partition(|partition_id| released_partitions.contains(partition_id));
          if !released.is_empty() {
            emit_partition_handoff_snapshots(
              &handoff_snapshot,
              &released,
              "shutdown_release_result",
              "released",
              if commit_result.is_ok() {
                "succeeded"
              } else {
                "failed"
              },
              &shutdown_span,
            );
          }
          if !not_released.is_empty() {
            emit_partition_handoff_snapshots(
              &handoff_snapshot,
              &not_released,
              "shutdown_release_result",
              "not_released",
              if commit_result.is_ok() {
                "succeeded"
              } else {
                "failed"
              },
              &shutdown_span,
            );
          }
        },
        Err(_) => emit_partition_handoff_snapshots(
          &handoff_snapshot,
          &handoff_partitions,
          "shutdown_release_result",
          "failed",
          if commit_result.is_ok() {
            "succeeded"
          } else {
            "failed"
          },
          &shutdown_span,
        ),
      }
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .before_deregister_member(&self.group_config.member_id, self.coordinator.generation())
          .await;
      }
      let deregistration_result = self
        .membership_store
        .deregister_member(
          &self.group_config.topic,
          &self.group_config.group_id,
          &self.group_config.member_id,
        )
        .await;
      let planner_release_result = self
        .membership_store
        .release_planner(
          &self.group_config.topic,
          &self.group_config.group_id,
          &self.group_config.member_id,
          self.coordinator.planner_session_id(),
        )
        .await;
      if let Err(error) = &planner_release_result {
        debug!(
          "consumer planner release failed during shutdown: topic={}, group_id={}, member_id={}, \
           error={error:#}",
          self.group_config.topic, self.group_config.group_id, self.group_config.member_id
        );
      }
      if let Err(error) = &deregistration_result {
        debug!(
          "consumer membership deregistration failed during shutdown: topic={}, group_id={}, \
           member_id={}, error={error:#}",
          self.group_config.topic, self.group_config.group_id, self.group_config.member_id
        );
      }
      self.stop_prefetch_task().await;
      self.started = false;
      self.refresh_diagnostics();
      shutdown_span.record(
        "shutdown.commit_outcome",
        if commit_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
      );
      shutdown_span.record(
        "shutdown.lease_release_outcome",
        if release_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
      );
      shutdown_span.record(
        "shutdown.deregistration_outcome",
        if deregistration_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
      );
      shutdown_span.record(
        "shutdown.planner_release_outcome",
        if planner_release_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
      );
      let error_message = [
        ("commit", commit_result.as_ref().err()),
        ("lease release", release_result.as_ref().err()),
        (
          "membership deregistration",
          deregistration_result.as_ref().err(),
        ),
        ("planner release", planner_release_result.as_ref().err()),
      ]
      .into_iter()
      .filter_map(|(operation, error)| error.map(|error| format!("{operation}: {error:#}")))
      .collect::<Vec<_>>()
      .join("; ");
      if !error_message.is_empty() {
        shutdown_span.record("error.message", error_message);
      }
      shutdown_span.record(
        "otel.status_code",
        if commit_result.is_ok()
          && release_result.is_ok()
          && deregistration_result.is_ok()
          && planner_release_result.is_ok()
        {
          "OK"
        } else {
          "ERROR"
        },
      );
      info!(
        "consumer iterator shutdown: topic={}, group_id={}, member_id={}",
        self.group_config.topic, self.group_config.group_id, self.group_config.member_id
      );

      commit_result?;
      release_result?;
    }
    Ok(())
  }

  #[allow(clippy::needless_pass_by_ref_mut)]
  pub(in crate::iterator) fn seek(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
    response: oneshot::Sender<Result<()>>,
  ) {
    if !self.active_assignment.contains(&virtual_partition_id) {
      let _ = response.send(Err(anyhow!(
        "cannot seek unassigned virtual partition {virtual_partition_id}"
      )));
      return;
    }
    {
      let mut shared_state = self.shared_state.lock();
      shared_state
        .delivery_state
        .drop_partitions(&HashSet::from([virtual_partition_id]));
      if let Some(partition_state) = shared_state
        .active_partitions
        .get_mut(&virtual_partition_id)
      {
        partition_state.pending_commit = None;
        partition_state.delivered_source_ranges.clear();
      }
      update_worker_prefetch_metrics(&self.metrics, &shared_state.delivery_state);
      update_total_prefetch_bytes(
        &self.metrics,
        &shared_state.delivery_state,
        shared_state.diagnostics.prefetch_pending_bytes,
      );
    }
    self.prefetch_space_notify.notify_waiters();
    self.refresh_diagnostics();
    self.seek_reader(
      virtual_partition_id,
      offset,
      self.now_unix_seconds(),
      response,
    );
    trace!(
      "consumer seek: topic={}, partition={}, offset={}",
      self.group_config.topic, virtual_partition_id, offset
    );
  }
}
