use super::state::BufferedBatch;
use crate::config::{ProducerConfig, ProducerTopicConfig, producer_writer_id};
use blob_stream_broker_discovery::{
  BrokerMembership,
  BrokerNode,
  BrokerPartition,
  balanced_assignment,
  writer_virtual_partitions,
};
use blob_stream_proto::protos::blobstream::v1::broker::{ProduceBatchRequest, Record};
use blob_stream_types::{MAX_PRODUCE_BATCHES_REQUEST_BYTES, VirtualPartitionId};
use parking_lot::RwLock;
use protobuf::{Chars, Message};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

//
// BrokerBatchGroup
//

pub(super) struct BrokerBatchGroup {
  pub(super) broker_address: Chars,
  pub(super) batches: Vec<BufferedBatch>,
}

//
// GroupedBatches
//

pub(super) struct GroupedBatches {
  pub(super) groups: Vec<BrokerBatchGroup>,
  pub(super) unassigned: Vec<BufferedBatch>,
}

type BrokerAssignment = BTreeMap<BrokerPartition, BrokerNode>;

//
// ProducerRoutes
//

#[derive(Clone)]
pub(super) struct ProducerRoutes {
  routes: Arc<RwLock<CachedRoutes>>,
}

struct CachedRoutes {
  membership: BrokerMembership,
  assignment: Arc<BrokerAssignment>,
}

impl ProducerRoutes {
  pub(super) fn new(
    config: &ProducerConfig,
    topics: &HashMap<Chars, ProducerTopicConfig>,
    membership: &BrokerMembership,
  ) -> Self {
    Self {
      routes: Arc::new(RwLock::new(CachedRoutes {
        membership: membership.clone(),
        assignment: Arc::new(broker_assignment(
          topics,
          producer_writer_id(config),
          membership,
        )),
      })),
    }
  }

  pub(super) fn refresh(
    &self,
    config: &ProducerConfig,
    topics: &HashMap<Chars, ProducerTopicConfig>,
    membership: &BrokerMembership,
  ) {
    let mut routes = self.routes.write();
    if routes.membership == *membership {
      return;
    }

    // Cache one deterministic assignment per membership snapshot. The same update can be seen by
    // the flush loop and an in-flight retry, so compare snapshots before rebuilding the map.
    routes.assignment = Arc::new(broker_assignment(
      topics,
      producer_writer_id(config),
      membership,
    ));
    routes.membership = membership.clone();
  }

  pub(super) fn owner(&self, partition: &BrokerPartition) -> Option<BrokerNode> {
    self.routes.read().assignment.get(partition).cloned()
  }
}

pub(super) fn broker_assignment(
  topics: &HashMap<Chars, ProducerTopicConfig>,
  writer_id: u32,
  membership: &BrokerMembership,
) -> BrokerAssignment {
  // Both producers and brokers use this deterministic assignment. Grouping must use the same
  // virtual-partition ownership calculation that the broker will validate on receipt.
  balanced_assignment(
    writer_virtual_partitions(
      topics
        .values()
        .map(|topic| (topic.name.clone(), topic.partition_count, topic.num_writers)),
      writer_id,
    ),
    membership,
  )
}

pub(super) fn group_batches_by_broker(
  routes: &ProducerRoutes,
  batches: Vec<BufferedBatch>,
) -> GroupedBatches {
  // Snapshot one assignment for the full drain. A batch without an owner is deliberately returned
  // rather than retained here; the caller notifies its waiters with NoBrokersAvailable.
  let assignment = Arc::clone(&routes.routes.read().assignment);
  let mut batches_by_broker = BTreeMap::<Chars, Vec<BufferedBatch>>::new();
  let mut unassigned = Vec::new();
  for batch in batches {
    let Some(broker) = assignment.get(&BrokerPartition {
      topic: batch.topic.clone(),
      virtual_partition_id: batch.virtual_partition_id,
    }) else {
      unassigned.push(batch);
      continue;
    };
    batches_by_broker
      .entry(broker.address.clone())
      .or_default()
      .push(batch);
  }

  // Then pack each broker's logical batches into wire-size-bounded ProduceBatches RPCs. The map
  // provides a stable broker order, while each broker retains the order of its drained batches.
  let mut groups = Vec::new();
  for (broker_address, batches) in batches_by_broker {
    let mut group = Vec::new();
    let mut group_size: usize = 0;

    // A producer batch can be larger than the grouped-RPC limit when its own configured limits
    // are larger. Split it only between records before deciding which broker request contains it.
    for batch in batches
      .into_iter()
      .flat_map(split_batch_for_grouped_request)
    {
      let batch_size = encoded_grouped_batch_size(&batch);

      // The current batch starts a new request when adding it would exceed the limit. A single
      // batch is known to fit because split_batch_for_grouped_request enforces the same bound.
      if !group.is_empty()
        && group_size.saturating_add(batch_size) > MAX_PRODUCE_BATCHES_REQUEST_BYTES
      {
        groups.push(BrokerBatchGroup {
          broker_address: broker_address.clone(),
          batches: group,
        });
        group = Vec::new();
        group_size = 0;
      }
      group_size = group_size.saturating_add(batch_size);
      group.push(batch);
    }
    if !group.is_empty() {
      groups.push(BrokerBatchGroup {
        broker_address,
        batches: group,
      });
    }
  }

  GroupedBatches { groups, unassigned }
}

pub(super) fn split_batch_for_grouped_request(batch: BufferedBatch) -> Vec<BufferedBatch> {
  assert_eq!(batch.records.len(), batch.waiters.len());

  // ProduceBatches embeds each ProduceBatchRequest as a length-delimited repeated field. Track
  // both the inner batch size and its outer field wrapper so every returned chunk fits the RPC.
  let base_size = encoded_produce_batch_size(&batch.topic, batch.virtual_partition_id, &[]);
  let mut batches = Vec::new();
  let mut records = Vec::new();
  let mut waiters = Vec::new();
  let mut request_size = base_size;
  for (record, waiter) in batch.records.into_iter().zip(batch.waiters) {
    let record_size = encoded_record_field_size(&record);
    let next_size = request_size.saturating_add(record_size);

    // Preserve record/waiter pairs when a full logical batch must span multiple broker RPCs.
    // Oversized individual records are rejected before buffering by record_fits_grouped_request.
    if !records.is_empty()
      && encoded_grouped_message_size(next_size) > MAX_PRODUCE_BATCHES_REQUEST_BYTES
    {
      batches.push(BufferedBatch {
        topic: batch.topic.clone(),
        virtual_partition_id: batch.virtual_partition_id,
        records: std::mem::take(&mut records),
        waiters: std::mem::take(&mut waiters),
      });
      request_size = base_size;
    }
    request_size = request_size.saturating_add(record_size);
    records.push(record);
    waiters.push(waiter);
  }
  if !records.is_empty() {
    batches.push(BufferedBatch {
      topic: batch.topic,
      virtual_partition_id: batch.virtual_partition_id,
      records,
      waiters,
    });
  }
  batches
}

pub(super) fn produce_batch_request(batch: &BufferedBatch) -> ProduceBatchRequest {
  // Dispatch keeps the BufferedBatch for waiter notification, so build an independent protobuf
  // request rather than consuming its records.
  ProduceBatchRequest {
    topic: batch.topic.clone(),
    virtual_partition_id: batch.virtual_partition_id,
    records: batch.records.clone(),
    ..Default::default()
  }
}

pub(super) fn record_fits_grouped_request(
  topic: &Chars,
  virtual_partition_id: VirtualPartitionId,
  record: &Record,
) -> bool {
  // Reject records that could never be sent, even alone. This lets splitting assume its first
  // record fits instead of creating an invalid one-record request.
  encoded_grouped_message_size(encoded_produce_batch_size(
    topic,
    virtual_partition_id,
    std::slice::from_ref(record),
  )) <= MAX_PRODUCE_BATCHES_REQUEST_BYTES
}

fn encoded_grouped_batch_size(batch: &BufferedBatch) -> usize {
  // A logical batch is a repeated message inside ProduceBatches, so include the outer tag and
  // length prefix rather than comparing only the nested ProduceBatchRequest size.
  let batch_size =
    encoded_produce_batch_size(&batch.topic, batch.virtual_partition_id, &batch.records);
  encoded_grouped_message_size(batch_size)
}

fn encoded_produce_batch_size(
  topic: &Chars,
  virtual_partition_id: VirtualPartitionId,
  records: &[Record],
) -> usize {
  // Compute the fixed ProduceBatchRequest fields once, then add the encoded repeated-record
  // fields. This avoids building a temporary protobuf request just to measure its serialized size.
  let request = ProduceBatchRequest {
    topic: topic.clone(),
    virtual_partition_id,
    ..Default::default()
  };
  let base_size =
    usize::try_from(request.compute_size()).expect("encoded protobuf batch size must fit in usize");
  records.iter().fold(base_size, |size, record| {
    size.saturating_add(encoded_record_field_size(record))
  })
}

fn encoded_record_field_size(record: &Record) -> usize {
  // Records are also length-delimited protobuf messages within ProduceBatchRequest.
  let record_size =
    usize::try_from(record.compute_size()).expect("encoded protobuf record size must fit in usize");
  encoded_grouped_message_size(record_size)
}

fn encoded_grouped_message_size(message_size: usize) -> usize {
  // Every nested protobuf message carries one field tag byte and a varint-encoded payload length.
  1usize
    .saturating_add(encoded_varint_size(message_size as u64))
    .saturating_add(message_size)
}

fn encoded_varint_size(mut value: u64) -> usize {
  // Protobuf encodes lengths in seven-bit groups, with the high bit marking continuation bytes.
  let mut size: usize = 1;
  while value >= 128 {
    size = size.saturating_add(1);
    value >>= 7;
  }
  size
}
