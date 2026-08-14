use super::routing::ProducerRoutes;
use super::state::ProducerState;
use crate::config::{
  ProducerConfig,
  ProducerTopicConfig,
  producer_flush_max_delay_ms,
  producer_max_batch_bytes,
  producer_max_batch_records,
  producer_writer_id,
};
use blob_stream_broker_discovery::{BrokerMembership, writer_virtual_partitions};
use blob_stream_types::{VirtualPartitionId, serialize_as_string};
use parking_lot::Mutex;
use protobuf::Chars;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::watch;

//
// ProducerRetryReason
//

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProducerRetryReason {
  TransportError,
  NotLeaseHolder,
  Overloaded,
}

//
// ProducerRetrySample
//

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProducerRetrySample {
  pub reason: ProducerRetryReason,
  pub topic: Chars,
  pub virtual_partition_id: VirtualPartitionId,
  pub attempt: u32,
  pub detail: String,
}

//
// ProducerRetrySummary
//

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProducerRetrySummary {
  pub reason_counts: BTreeMap<ProducerRetryReason, u64>,
  pub samples: VecDeque<ProducerRetrySample>,
}

//
// ProducerRetryDiagnostics
//

#[derive(Clone, Default)]
pub(super) struct ProducerRetryDiagnostics {
  summary: Arc<Mutex<ProducerRetrySummary>>,
}

impl ProducerRetryDiagnostics {
  pub(super) fn record(
    &self,
    reason: ProducerRetryReason,
    topic: &Chars,
    virtual_partition_id: VirtualPartitionId,
    attempt: u32,
    detail: String,
  ) {
    const MAX_SAMPLES: usize = 20;

    let mut summary = self.summary.lock();
    *summary.reason_counts.entry(reason).or_default() += 1;
    if summary.samples.len() == MAX_SAMPLES {
      summary.samples.pop_front();
    }
    summary.samples.push_back(ProducerRetrySample {
      reason,
      topic: topic.clone(),
      virtual_partition_id,
      attempt,
      detail,
    });
  }

  pub(super) fn summary(&self) -> ProducerRetrySummary {
    self.summary.lock().clone()
  }
}

//
// ProducerStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct ProducerStateSnapshot {
  #[serde(with = "time::serde::rfc3339")]
  pub generated_at: time::OffsetDateTime,
  pub writer_id: u32,
  pub max_batch_records: u32,
  pub max_batch_bytes: u32,
  pub flush_max_delay_ms: u64,
  pub brokers: Vec<ProducerBrokerSnapshot>,
  pub topics: Vec<ProducerTopicSnapshot>,
  pub route_map: Vec<ProducerRouteSnapshot>,
  pub partition_buffers: Vec<ProducerPartitionBufferSnapshot>,
}

//
// ProducerBrokerSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProducerBrokerSnapshot {
  #[serde(serialize_with = "serialize_as_string")]
  pub node_id: Chars,
  #[serde(serialize_with = "serialize_as_string")]
  pub address: Chars,
}

//
// ProducerTopicSnapshot
//

#[derive(Debug, Serialize)]
pub struct ProducerTopicSnapshot {
  #[serde(serialize_with = "serialize_as_string")]
  pub name: Chars,
  pub partition_count: u32,
  pub num_writers: u32,
  pub retention_days: u32,
}

//
// ProducerRouteSnapshot
//

#[derive(Debug, Serialize)]
pub struct ProducerRouteSnapshot {
  #[serde(serialize_with = "serialize_as_string")]
  pub topic: Chars,
  pub virtual_partition_id: VirtualPartitionId,
  pub producer_writer_id: u32,
  pub logical_partition_id: u32,
  pub selected_broker: Option<ProducerBrokerSnapshot>,
}

//
// ProducerPartitionBufferSnapshot
//

#[derive(Debug, Serialize)]
pub struct ProducerPartitionBufferSnapshot {
  #[serde(serialize_with = "serialize_as_string")]
  pub topic: Chars,
  pub virtual_partition_id: VirtualPartitionId,
  pub buffered_record_count: usize,
  pub pending_ack_count: usize,
  pub buffered_bytes: usize,
}

//
// ProducerDiagnostics
//

#[derive(Clone)]
pub struct ProducerDiagnostics {
  config: ProducerConfig,
  topics: HashMap<Chars, ProducerTopicConfig>,
  membership_rx: watch::Receiver<BrokerMembership>,
  routes: ProducerRoutes,
  state: Arc<Mutex<ProducerState>>,
  retry_diagnostics: ProducerRetryDiagnostics,
}

impl ProducerDiagnostics {
  pub(super) fn new(
    config: ProducerConfig,
    topics: HashMap<Chars, ProducerTopicConfig>,
    membership_rx: watch::Receiver<BrokerMembership>,
    routes: ProducerRoutes,
    state: Arc<Mutex<ProducerState>>,
    retry_diagnostics: ProducerRetryDiagnostics,
  ) -> Self {
    Self {
      config,
      topics,
      membership_rx,
      routes,
      state,
      retry_diagnostics,
    }
  }

  #[must_use]
  pub fn retry_summary(&self) -> ProducerRetrySummary {
    self.retry_diagnostics.summary()
  }

  #[must_use]
  pub fn state_snapshot(&self) -> ProducerStateSnapshot {
    let generated_at = time::OffsetDateTime::now_utc();
    let writer_id = producer_writer_id(&self.config);
    let membership = self.membership_rx.borrow().clone();
    let mut brokers = membership
      .nodes()
      .unwrap_or_default()
      .iter()
      .map(|node| ProducerBrokerSnapshot {
        node_id: node.node_id.clone(),
        address: node.address.clone(),
      })
      .collect::<Vec<_>>();
    brokers.sort_by(|left, right| {
      left
        .node_id
        .cmp(&right.node_id)
        .then_with(|| left.address.cmp(&right.address))
    });

    let mut topics = self
      .topics
      .values()
      .map(|topic| ProducerTopicSnapshot {
        name: topic.name.clone(),
        partition_count: topic.partition_count,
        num_writers: topic.num_writers,
        retention_days: topic.retention_days,
      })
      .collect::<Vec<_>>();
    topics.sort_by(|left, right| left.name.cmp(&right.name));

    let mut route_map = writer_virtual_partitions(
      self
        .topics
        .values()
        .map(|topic| (topic.name.clone(), topic.partition_count, topic.num_writers)),
      writer_id,
    )
    .into_iter()
    .map(|partition| {
      let topic = self
        .topics
        .get(partition.topic.as_str())
        .expect("partition inventory must reference a configured topic");
      let selected_broker = self
        .routes
        .owner(&partition)
        .map(|broker| ProducerBrokerSnapshot {
          node_id: broker.node_id,
          address: broker.address,
        });
      ProducerRouteSnapshot {
        topic: partition.topic,
        virtual_partition_id: partition.virtual_partition_id,
        producer_writer_id: writer_id,
        logical_partition_id: partition.virtual_partition_id % topic.partition_count,
        selected_broker,
      }
    })
    .collect::<Vec<_>>();
    route_map.sort_by(|left, right| {
      (&left.topic, left.virtual_partition_id).cmp(&(&right.topic, right.virtual_partition_id))
    });

    let state = self.state.lock();
    let mut partition_buffers = state
      .buffers
      .iter()
      .flat_map(|(topic, partitions)| {
        partitions
          .iter()
          .map(
            move |(virtual_partition_id, buffer)| ProducerPartitionBufferSnapshot {
              topic: topic.clone(),
              virtual_partition_id: *virtual_partition_id,
              buffered_record_count: buffer.records.len(),
              pending_ack_count: buffer.waiters.len(),
              buffered_bytes: buffer.buffered_bytes,
            },
          )
      })
      .collect::<Vec<_>>();
    partition_buffers.sort_by(|left, right| {
      (&left.topic, left.virtual_partition_id).cmp(&(&right.topic, right.virtual_partition_id))
    });

    ProducerStateSnapshot {
      generated_at,
      writer_id,
      max_batch_records: producer_max_batch_records(&self.config),
      max_batch_bytes: producer_max_batch_bytes(&self.config),
      flush_max_delay_ms: producer_flush_max_delay_ms(&self.config),
      brokers,
      topics,
      route_map,
      partition_buffers,
    }
  }

  pub fn admin_router(self) -> axum::Router {
    crate::admin::router(self)
  }
}
