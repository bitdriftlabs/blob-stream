use super::{
  ConsumerBatch,
  ConsumerReadRuntimeSettings,
  ConsumerReader,
  ConsumerReaderImpl,
  HashMap,
  ReadCapacity,
  Result,
  VirtualPartitionId,
  VirtualPartitionState,
  consumer_read_runtime_settings,
};
use crate::consumer::ConsumerReadOutcome;
use async_trait::async_trait;
use time::OffsetDateTime;

impl ConsumerReaderImpl {
  pub(crate) fn runtime_settings(&self) -> ConsumerReadRuntimeSettings {
    consumer_read_runtime_settings(&self.config, self.feature_flags.as_ref())
  }

  pub(crate) async fn read_available_with_capacity_and_settings(
    &mut self,
    now: OffsetDateTime,
    capacity: ReadCapacity,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<ConsumerReadOutcome> {
    // The background worker needs the empty-pass scheduling hint. Keep this internal entrypoint
    // separate so callers of the public batch-reading trait do not inherit a scheduling contract.
    self
      .read_available_impl(now, capacity, runtime_settings)
      .await
  }
}

#[async_trait]
impl ConsumerReader for ConsumerReaderImpl {
  async fn read_available(
    &mut self,
    now: OffsetDateTime,
    capacity: ReadCapacity,
  ) -> Result<Vec<ConsumerBatch>> {
    let runtime_settings = self.runtime_settings();
    // Synchronous callers receive only deliverable batches. The visibility deadline controls
    // polling, not the reader's public delivery semantics.
    Ok(
      self
        .read_available_impl(now, capacity, runtime_settings)
        .await?
        .batches,
    )
  }

  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64> {
    self
      .virtual_partition_states
      .get(&virtual_partition_id)
      .and_then(VirtualPartitionState::cursor)
  }

  fn cursors(&self) -> HashMap<VirtualPartitionId, u64> {
    self
      .virtual_partition_states
      .iter()
      .filter_map(|(partition_id, state)| state.cursor().map(|cursor| (*partition_id, cursor)))
      .collect()
  }
}
