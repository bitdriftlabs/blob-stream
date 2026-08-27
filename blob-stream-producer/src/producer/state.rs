use super::protocol::encoded_grouped_message_size;
use super::routing::BrokerBatchGroup;
use super::{ProducerAck, ProducerError};
use blob_stream_proto::protos::blobstream::v1::broker::Record;
use blob_stream_types::{MAX_PRODUCE_BATCHES_REQUEST_BYTES, VirtualPartitionId};
use protobuf::Chars;
use std::collections::{BTreeMap, VecDeque};
use tokio::sync::oneshot;

// ProducerState has two ownership phases. `buffers` accumulates records per partition until a
// scheduler trigger seals them. Sealed records move into broker-specific ready queues and remain
// there until a dispatch task has an available permit. All methods run while the producer state
// mutex is held and must not await.

//
// BufferedRecord
//

// The record data accepted by `produce`, including the waiter that must receive its terminal
// result and wire sizes calculated before acquiring the state mutex.
pub(super) struct BufferedRecord {
  pub(super) topic: Chars,
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) proto_record: Record,
  pub(super) waiter: oneshot::Sender<Result<ProducerAck, ProducerError>>,
  pub(super) encoded_record_size: usize,
  pub(super) request_base_size: usize,
}

//
// BufferedBatch
//

// One partition's records prepared for one broker RPC. Records and waiters remain aligned so a
// dispatch result can be reported to every caller after the RPC completes.
pub(super) struct BufferedBatch {
  pub(super) topic: Chars,
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) records: Vec<Record>,
  pub(super) waiters: Vec<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
}

// A sealed partition batch awaiting extraction into one or more bounded broker RPCs. A record
// cursor and waiter deque avoid shifting a potentially large remaining suffix after each group.
struct ReadyBatch {
  topic: Chars,
  virtual_partition_id: VirtualPartitionId,
  records: Vec<Record>,
  waiters: VecDeque<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
  encoded_record_sizes: Vec<usize>,
  request_base_size: usize,
  next_record_index: usize,
  buffered_bytes: usize,
}

impl ReadyBatch {
  fn take_prefix(&mut self, max_group_bytes: usize) -> Option<(BufferedBatch, usize)> {
    // Find the largest record prefix whose enclosing ProduceBatchRequest still fits in the
    // caller's grouped-request budget. The sizing vectors stay aligned with `records`.
    let mut request_size = self.request_base_size;
    let start_index = self.next_record_index;
    let mut end_index = start_index;
    for encoded_record_size in &self.encoded_record_sizes[start_index ..] {
      let next_request_size = request_size.saturating_add(*encoded_record_size);
      if encoded_grouped_message_size(next_request_size) > max_group_bytes {
        break;
      }
      request_size = next_request_size;
      end_index += 1;
    }
    if end_index == start_index {
      return None;
    }

    // Move selected records and their corresponding waiters out without shifting the unsent
    // suffix. `buffered_bytes` tracks payload bytes, unlike the wire-size budget above.
    let grouped_batch_size = encoded_grouped_message_size(request_size);
    let mut payload_bytes: usize = 0;
    let records = self.records[start_index .. end_index]
      .iter_mut()
      .map(|record| {
        let record = std::mem::take(record);
        payload_bytes = payload_bytes.saturating_add(record.payload.len());
        record
      })
      .collect();
    let waiters = (start_index .. end_index)
      .map(|_| {
        self
          .waiters
          .pop_front()
          .expect("ready batch has one waiter per record")
      })
      .collect();
    self.next_record_index = end_index;
    self.buffered_bytes = self.buffered_bytes.saturating_sub(payload_bytes);
    Some((
      BufferedBatch {
        topic: self.topic.clone(),
        virtual_partition_id: self.virtual_partition_id,
        records,
        waiters,
      },
      grouped_batch_size,
    ))
  }

  fn is_empty(&self) -> bool {
    // Extracted records are replaced with defaults, so the cursor is the authoritative progress
    // marker rather than checking the vector contents.
    self.next_record_index == self.records.len()
  }
}

// The result of sealing every currently accumulating partition. Unassigned batches are returned
// so the caller can notify their waiters immediately instead of retaining unroutable work.
pub(super) struct SealedBatches {
  pub(super) batch_count: usize,
  pub(super) unassigned: Vec<BufferedBatch>,
}

// Aggregated across accumulating and sealed-but-undispatched records for diagnostics.
pub(super) struct BufferedPartitionStats {
  pub(super) record_count: usize,
  pub(super) pending_ack_count: usize,
  pub(super) payload_bytes: usize,
}

//
// PartitionBuffer
//

// Records accepted for one topic/virtual-partition pair before a scheduler trigger seals them.
// The three vectors have one element per record at the same index. `request_base_size` is fixed
// for the partition while it is buffered, and `buffered_bytes` intentionally counts payload only.
// The compact size vector lets ready-group packing scan wire costs without loading record or waiter
// storage for every candidate.
#[derive(Default)]
pub(super) struct PartitionBuffer {
  pub(super) records: Vec<Record>,
  pub(super) waiters: Vec<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
  encoded_record_sizes: Vec<usize>,
  request_base_size: Option<usize>,
  pub(super) buffered_bytes: usize,
}

impl PartitionBuffer {
  fn push(
    &mut self,
    record: Record,
    waiter: oneshot::Sender<Result<ProducerAck, ProducerError>>,
    encoded_record_size: usize,
    request_base_size: usize,
  ) {
    // A ProduceBatchRequest's non-record fields must be identical for every record in this
    // partition buffer, so retain the first value and enforce the invariant for subsequent ones.
    assert!(
      self
        .request_base_size
        .is_none_or(|existing| existing == request_base_size),
      "all records in a partition buffer share the same request base size"
    );
    self.request_base_size = Some(request_base_size);
    self.buffered_bytes = self.buffered_bytes.saturating_add(record.payload.len());
    self.records.push(record);
    self.waiters.push(waiter);
    self.encoded_record_sizes.push(encoded_record_size);
  }

  fn should_flush_by_size(&self, max_batch_records: usize, max_batch_bytes: usize) -> bool {
    // Crossing either limit seals all buffered partitions so work can be coalesced by broker, not
    // only the partition that crossed the threshold.
    self.records.len() >= max_batch_records || self.buffered_bytes >= max_batch_bytes
  }

  fn take_batch(
    &mut self,
    topic: Chars,
    virtual_partition_id: VirtualPartitionId,
  ) -> Option<ReadyBatch> {
    if self.records.is_empty() {
      return None;
    }

    // Move the complete partition batch into the ready phase. Each collection is emptied together
    // so their index-based correspondence remains intact for extraction and waiter notification.
    let records = std::mem::take(&mut self.records);
    let waiters = std::mem::take(&mut self.waiters);
    let encoded_record_sizes = std::mem::take(&mut self.encoded_record_sizes);
    let request_base_size = self
      .request_base_size
      .take()
      .expect("nonempty partition buffer has a request base size");
    let buffered_bytes = std::mem::take(&mut self.buffered_bytes);

    Some(ReadyBatch {
      topic,
      virtual_partition_id,
      records,
      waiters: VecDeque::from(waiters),
      encoded_record_sizes,
      request_base_size,
      next_record_index: 0,
      buffered_bytes,
    })
  }
}

//
// ProducerState
//

// `ready_by_broker` holds sealed batches in FIFO order per broker. `ready_brokers` is a rotating
// index of brokers with nonempty queues; each extraction moves a still-ready broker to its tail.
#[derive(Default)]
pub(super) struct ProducerState {
  pub(super) buffers: BTreeMap<Chars, BTreeMap<VirtualPartitionId, PartitionBuffer>>,
  ready_by_broker: BTreeMap<Chars, VecDeque<ReadyBatch>>,
  ready_brokers: VecDeque<Chars>,
}

impl ProducerState {
  pub(super) fn push_record(
    &mut self,
    record: BufferedRecord,
    max_batch_records: usize,
    max_batch_bytes: usize,
  ) -> bool {
    // The nested maps isolate batching by topic and virtual partition before a later seal groups
    // the resulting batches by their selected broker.
    let BufferedRecord {
      topic,
      virtual_partition_id,
      proto_record,
      waiter,
      encoded_record_size,
      request_base_size,
    } = record;
    let buffer = self
      .buffers
      .entry(topic)
      .or_default()
      .entry(virtual_partition_id)
      .or_default();
    buffer.push(proto_record, waiter, encoded_record_size, request_base_size);
    buffer.should_flush_by_size(max_batch_records, max_batch_bytes)
  }

  pub(super) fn seal_ready_generation<F>(&mut self, broker_for: F) -> SealedBatches
  where
    F: Fn(&Chars, VirtualPartitionId) -> Option<Chars>,
  {
    // Take every accumulating buffer so the next producer write starts from empty maps rather than
    // retaining a growing index of already-sealed partitions. New records arrive in fresh buffers
    // while these batches wait for dispatch, so admission pressure never requires restoration.
    let mut batch_count = 0;
    let mut unassigned = Vec::new();
    let buffers = std::mem::take(&mut self.buffers);
    let (ready_by_broker, ready_brokers) = (&mut self.ready_by_broker, &mut self.ready_brokers);
    for (topic, partitions) in buffers {
      for (virtual_partition_id, mut buffer) in partitions {
        if let Some(batch) = buffer.take_batch(topic.clone(), virtual_partition_id) {
          batch_count += 1;
          let Some(broker_address) = broker_for(&topic, virtual_partition_id) else {
            // Preserve record/waiter alignment while returning the routing failure to the caller.
            unassigned.push(BufferedBatch {
              topic: batch.topic,
              virtual_partition_id: batch.virtual_partition_id,
              records: batch.records,
              waiters: batch.waiters.into_iter().collect(),
            });
            continue;
          };
          let broker_batches = ready_by_broker.entry(broker_address.clone()).or_default();
          if broker_batches.is_empty() {
            // Insert an address only when its queue transitions from empty to nonempty.
            ready_brokers.push_back(broker_address);
          }
          broker_batches.push_back(batch);
        }
      }
    }
    SealedBatches {
      batch_count,
      unassigned,
    }
  }

  pub(super) fn take_next_ready_group(&mut self) -> Option<BrokerBatchGroup> {
    // Rotate brokers so a large ready queue cannot monopolize dispatch permits. A group may
    // include several partition batches for one broker, bounded by the grouped RPC wire limit.
    let broker_address = self.ready_brokers.pop_front()?;
    let batches = self
      .ready_by_broker
      .get_mut(&broker_address)
      .expect("ready broker index references a batch queue");
    let mut group = Vec::new();
    let mut group_size = 0;
    while let Some(batch) = batches.front_mut() {
      // `take_prefix` leaves an oversized batch's suffix in place for a later group.
      let remaining_bytes = MAX_PRODUCE_BATCHES_REQUEST_BYTES.saturating_sub(group_size);
      let Some((next_batch, next_batch_size)) = batch.take_prefix(remaining_bytes) else {
        break;
      };
      group_size = group_size.saturating_add(next_batch_size);
      if batch.is_empty() {
        batches.pop_front();
      }
      group.push(next_batch);
    }
    assert!(
      !group.is_empty(),
      "every ready batch contains a record that fits a grouped request"
    );
    if batches.is_empty() {
      self.ready_by_broker.remove(&broker_address);
    } else {
      // This broker still has work, but let every other ready broker take a turn first.
      self.ready_brokers.push_back(broker_address.clone());
    }
    Some(BrokerBatchGroup {
      broker_address,
      batches: group,
    })
  }

  pub(super) fn buffered_partition_stats(
    &self,
  ) -> BTreeMap<(Chars, VirtualPartitionId), BufferedPartitionStats> {
    // Start with accumulating buffers, then add sealed queue state using stored counters rather
    // than walking payloads while diagnostics holds the producer state mutex.
    let mut stats = self
      .buffers
      .iter()
      .flat_map(|(topic, partitions)| {
        partitions
          .iter()
          .map(move |(virtual_partition_id, buffer)| {
            (
              (topic.clone(), *virtual_partition_id),
              BufferedPartitionStats {
                record_count: buffer.records.len(),
                pending_ack_count: buffer.waiters.len(),
                payload_bytes: buffer.buffered_bytes,
              },
            )
          })
      })
      .collect::<BTreeMap<_, _>>();
    for broker_batches in self.ready_by_broker.values() {
      for ready_batch in broker_batches {
        let entry = stats
          .entry((ready_batch.topic.clone(), ready_batch.virtual_partition_id))
          .or_insert(BufferedPartitionStats {
            record_count: 0,
            pending_ack_count: 0,
            payload_bytes: 0,
          });
        entry.record_count += ready_batch.records.len() - ready_batch.next_record_index;
        entry.pending_ack_count += ready_batch.waiters.len();
        entry.payload_bytes += ready_batch.buffered_bytes;
      }
    }
    stats
  }
}
