use super::{
  ConsumerReaderImpl,
  ConsumerReaderPartitionScanState,
  ConsumerReaderPartitionState,
  VirtualPartitionId,
  VirtualPartitionState,
};

impl ConsumerReaderImpl {
  pub(crate) fn partition_read_states(&self) -> Vec<ConsumerReaderPartitionState> {
    let mut states = self
      .virtual_partition_states
      .iter()
      .filter_map(|(virtual_partition_id, state)| {
        state
          .reader_mode()
          .map(|mode| ConsumerReaderPartitionState {
            virtual_partition_id: *virtual_partition_id,
            mode,
          })
      })
      .collect::<Vec<_>>();
    states.sort_by_key(|state| state.virtual_partition_id);
    states
  }

  pub(crate) fn partition_scan_states(&self) -> Vec<&ConsumerReaderPartitionScanState> {
    let mut states = self
      .virtual_partition_states
      .values()
      .filter_map(VirtualPartitionState::last_scan)
      .collect::<Vec<_>>();
    states.sort_by_key(|state| state.virtual_partition_id);
    states
  }

  pub(in crate::consumer) fn partition_read_mode_counts(&self) -> (usize, usize, usize) {
    self.virtual_partition_states.values().fold(
      (0_usize, 0_usize, 0_usize),
      |(fresh, recovering, fast), state| match state {
        VirtualPartitionState::PendingCursor { .. } => (fresh, recovering, fast),
        VirtualPartitionState::Fresh { .. } => (fresh.saturating_add(1), recovering, fast),
        VirtualPartitionState::PendingRecovering { .. }
        | VirtualPartitionState::Recovering { .. } => (fresh, recovering.saturating_add(1), fast),
        VirtualPartitionState::PendingFast { .. } | VirtualPartitionState::Fast { .. } => {
          (fresh, recovering, fast.saturating_add(1))
        },
      },
    )
  }

  pub(in crate::consumer) fn assigned_virtual_partition_ids(&self) -> Vec<VirtualPartitionId> {
    // Keep the scan order deterministic while excluding recovered state that is still pending a
    // coordinator assignment.
    let mut partition_ids = self
      .virtual_partition_states
      .iter()
      .filter_map(|(partition_id, state)| state.is_assigned().then_some(*partition_id))
      .collect::<Vec<_>>();
    partition_ids.sort_unstable();
    partition_ids
  }
}
