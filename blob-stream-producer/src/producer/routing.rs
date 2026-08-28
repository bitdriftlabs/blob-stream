use super::protocol::encoded_grouped_message_size;
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

type BrokerAssignment = BTreeMap<BrokerPartition, BrokerNode>;

pub(super) struct RecordWireSizes {
  pub(super) request_base_size: usize,
  pub(super) encoded_record_size: usize,
  grouped_batch_size: usize,
}

impl RecordWireSizes {
  pub(super) fn fits_grouped_request(&self) -> bool {
    self.grouped_batch_size <= MAX_PRODUCE_BATCHES_REQUEST_BYTES
  }

  #[cfg(test)]
  pub(super) fn grouped_batch_size(&self) -> usize {
    self.grouped_batch_size
  }
}

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

  pub(super) fn assignment_snapshot(&self) -> Arc<BrokerAssignment> {
    Arc::clone(&self.routes.read().assignment)
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

pub(super) fn record_wire_sizes(
  topic: &Chars,
  virtual_partition_id: VirtualPartitionId,
  record: &Record,
) -> RecordWireSizes {
  // Save the immutable wire-size components when a record is accepted. Ready-group extraction
  // then checks only stored lengths rather than traversing protobuf records on the coordinator.
  let request_base_size = encoded_produce_batch_size(topic, virtual_partition_id, &[]);
  let encoded_record_size = encoded_record_field_size(record);
  RecordWireSizes {
    request_base_size,
    encoded_record_size,
    grouped_batch_size: encoded_grouped_message_size(
      request_base_size.saturating_add(encoded_record_size),
    ),
  }
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
