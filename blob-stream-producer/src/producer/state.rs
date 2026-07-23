use super::{ProducerAck, ProducerError};
use blob_stream_proto::protos::blobstream::v1::broker::Record;
use blob_stream_types::VirtualPartitionId;
use protobuf::Chars;
use std::collections::BTreeMap;
use tokio::sync::oneshot;

//
// BufferedRecord
//

pub(super) struct BufferedRecord {
  pub(super) topic: Chars,
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) proto_record: Record,
  pub(super) waiter: oneshot::Sender<Result<ProducerAck, ProducerError>>,
}

//
// BufferedBatch
//

pub(super) struct BufferedBatch {
  pub(super) topic: Chars,
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) records: Vec<Record>,
  pub(super) waiters: Vec<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
}

//
// PartitionBuffer
//

#[derive(Default)]
pub(super) struct PartitionBuffer {
  pub(super) records: Vec<Record>,
  pub(super) waiters: Vec<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
  pub(super) buffered_bytes: usize,
}

impl PartitionBuffer {
  fn push(&mut self, record: Record, waiter: oneshot::Sender<Result<ProducerAck, ProducerError>>) {
    self.buffered_bytes = self.buffered_bytes.saturating_add(record.payload.len());
    self.records.push(record);
    self.waiters.push(waiter);
  }

  fn should_flush_by_size(&self, max_batch_records: usize, max_batch_bytes: usize) -> bool {
    self.records.len() >= max_batch_records || self.buffered_bytes >= max_batch_bytes
  }

  fn take_batch(
    &mut self,
    topic: Chars,
    virtual_partition_id: VirtualPartitionId,
  ) -> Option<BufferedBatch> {
    if self.records.is_empty() {
      return None;
    }

    let records = std::mem::take(&mut self.records);
    let waiters = std::mem::take(&mut self.waiters);
    self.buffered_bytes = 0;

    Some(BufferedBatch {
      topic,
      virtual_partition_id,
      records,
      waiters,
    })
  }
}

//
// ProducerState
//

#[derive(Default)]
pub(super) struct ProducerState {
  pub(super) buffers: BTreeMap<Chars, BTreeMap<VirtualPartitionId, PartitionBuffer>>,
}

impl ProducerState {
  pub(super) fn push_record(
    &mut self,
    record: BufferedRecord,
    max_batch_records: usize,
    max_batch_bytes: usize,
  ) -> bool {
    let BufferedRecord {
      topic,
      virtual_partition_id,
      proto_record,
      waiter,
    } = record;
    let buffer = self
      .buffers
      .entry(topic)
      .or_default()
      .entry(virtual_partition_id)
      .or_default();
    buffer.push(proto_record, waiter);
    buffer.should_flush_by_size(max_batch_records, max_batch_bytes)
  }

  pub(super) fn drain_all_batches(&mut self) -> Vec<BufferedBatch> {
    let mut batches = Vec::new();
    for (topic, partitions) in &mut self.buffers {
      for (virtual_partition_id, buffer) in partitions {
        if let Some(batch) = buffer.take_batch(topic.clone(), *virtual_partition_id) {
          batches.push(batch);
        }
      }
    }
    batches
  }
}
