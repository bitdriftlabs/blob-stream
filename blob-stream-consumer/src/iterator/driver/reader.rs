use super::{
  Arc,
  ConsumerCommittedCursorSnapshot,
  ConsumerDriver,
  ConsumerReaderCommand,
  HashMap,
  Ordering,
  PrefetchWorker,
  RecoveredCursor,
  Result,
  VirtualPartitionId,
  anyhow,
  mpsc,
  oneshot,
  record_reader_diagnostics,
};

impl ConsumerDriver {
  pub(in crate::iterator) fn hydrate_cursors(
    &mut self,
    recovered_cursors: HashMap<VirtualPartitionId, RecoveredCursor>,
    now_unix_seconds: i64,
  ) -> Result<()> {
    if recovered_cursors.is_empty() {
      return Ok(());
    }

    {
      let mut shared_state = self.shared_state.lock();
      for (partition_id, recovered_cursor) in &recovered_cursors {
        shared_state.diagnostics.last_committed_cursors.insert(
          *partition_id,
          ConsumerCommittedCursorSnapshot {
            offset: recovered_cursor.committed_cursor.seq_end,
            source_checkpoint: recovered_cursor.committed_cursor.source_checkpoint.clone(),
            committed_at_ms: recovered_cursor.committed_ts_ms,
          },
        );
      }
    }

    if let Some(reader) = &mut self.reader {
      for (partition_id, recovered_cursor) in recovered_cursors {
        reader.hydrate_cursor_with_source(
          partition_id,
          &recovered_cursor.committed_cursor,
          recovered_cursor.committed_ts_ms,
          now_unix_seconds,
        );
      }
      record_reader_diagnostics(reader, &self.shared_state);
      return Ok(());
    }

    self
      .reader_command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer reader is unavailable"))?
      .send(ConsumerReaderCommand::HydrateCursors {
        recovered_cursors,
        now_unix_seconds,
      })
      .map_err(|_| anyhow!("consumer reader worker stopped"))?;
    self.reader_command_notify.notify_one();
    Ok(())
  }

  pub(in crate::iterator) fn set_reader_assignment(
    &mut self,
    assignment: Vec<VirtualPartitionId>,
    now_unix_seconds: i64,
    handoff_phase: Option<&'static str>,
    release_delivery_fence: bool,
  ) -> Result<()> {
    if let Some(reader) = &mut self.reader {
      reader.set_assigned_virtual_partitions(&assignment, now_unix_seconds)?;
      record_reader_diagnostics(reader, &self.shared_state);
      if release_delivery_fence {
        self
          .shared_state
          .lock()
          .delivery_state
          .revocation_in_progress = false;
        self.delivery_notify.notify_waiters();
      }
      return Ok(());
    }

    self
      .reader_command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer reader is unavailable"))?
      .send(ConsumerReaderCommand::SetAssignment {
        assignment,
        now_unix_seconds,
        handoff_phase,
        release_delivery_fence,
      })
      .map_err(|_| anyhow!("consumer reader worker stopped"))?;
    self.reader_command_notify.notify_one();
    Ok(())
  }

  pub(in crate::iterator) fn seek_reader(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
    now_unix_seconds: i64,
    response: oneshot::Sender<Result<()>>,
  ) {
    if let Some(reader) = &mut self.reader {
      reader.seek(virtual_partition_id, offset, now_unix_seconds);
      record_reader_diagnostics(reader, &self.shared_state);
      let _ = response.send(Ok(()));
      return;
    }

    let Some(reader_command_tx) = self.reader_command_tx.as_ref() else {
      let _ = response.send(Err(anyhow!("consumer reader is unavailable")));
      return;
    };
    let command = ConsumerReaderCommand::Seek {
      virtual_partition_id,
      offset,
      now_unix_seconds,
      response,
    };
    if let Err(error) = reader_command_tx.send(command) {
      let ConsumerReaderCommand::Seek { response, .. } = error.0 else {
        unreachable!("only seek commands are sent through this path");
      };
      let _ = response.send(Err(anyhow!("consumer reader worker stopped")));
    } else {
      self.reader_command_notify.notify_one();
    }
  }

  pub(in crate::iterator) fn spawn_prefetch_task(&mut self) {
    if self.prefetch_task.is_some() {
      return;
    }

    self.prefetch_shutdown.store(false, Ordering::Release);

    let reader = self
      .reader
      .take()
      .expect("consumer reader must be available before prefetch starts");
    let (reader_command_tx, reader_command_rx) = mpsc::unbounded_channel();
    self.reader_command_tx = Some(reader_command_tx);
    let shared_state = Arc::clone(&self.shared_state);
    let delivery_notify = Arc::clone(&self.delivery_notify);
    let space_notify = Arc::clone(&self.prefetch_space_notify);
    let reader_command_notify = Arc::clone(&self.reader_command_notify);
    let shutdown = Arc::clone(&self.prefetch_shutdown);
    let metrics = self.metrics.clone();
    let diagnostics = self.diagnostics.clone();
    let base_idle_delay_ms = self.prefetch_idle_base_delay_ms;
    let max_idle_delay_ms = self.prefetch_idle_max_delay_ms;
    let time_provider = Arc::clone(&self.time_provider);
    let lifecycle_hooks = self.lifecycle_hooks.clone();
    let member_id = self.group_config.member_id.to_string();

    self.prefetch_task = Some(tokio::spawn(async move {
      PrefetchWorker::new(
        reader,
        reader_command_rx,
        shared_state,
        diagnostics,
        delivery_notify,
        space_notify,
        reader_command_notify,
        shutdown,
        metrics,
        base_idle_delay_ms,
        max_idle_delay_ms,
        time_provider,
        lifecycle_hooks,
        member_id,
      )
      .run()
      .await;
    }));
  }

  pub(in crate::iterator) async fn stop_prefetch_task(&mut self) {
    self.prefetch_shutdown.store(true, Ordering::Release);
    self.reader_command_tx = None;
    self.delivery_notify.notify_waiters();
    self.prefetch_space_notify.notify_waiters();

    if let Some(handle) = self.prefetch_task.take() {
      handle.abort();
      let _ = handle.await;
    }
  }
}
