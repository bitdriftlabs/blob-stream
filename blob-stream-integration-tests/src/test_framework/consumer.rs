use super::{ClusterHarness, TOPIC};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_consumer::HeartbeatReport;
use blob_stream_consumer::iterator::{
  ConsumerIterator,
  ConsumerIteratorImpl,
  ConsumerSeekTarget,
  NextResult,
  RevokedPartitions,
};
use blob_stream_types::VirtualPartitionId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

//
// ConsumerTaskEvent
//

pub enum ConsumerTaskEvent {
  Batch {
    member_id: String,
    ids: Vec<String>,
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
  },
  Revoked {
    ack: oneshot::Sender<()>,
  },
  CommitSucceeded,
}

/// Runs a consumer until stopped, surfacing records, revocations, and completed commits to a test.
pub async fn run_consumer_task(
  mut consumer: Box<dyn ConsumerIterator>,
  mut stop_rx: watch::Receiver<bool>,
  event_tx: mpsc::UnboundedSender<ConsumerTaskEvent>,
) -> Result<()> {
  consumer.start()?;
  let member_id = consumer.diagnostics().map_or_else(
    || "test-consumer".to_string(),
    |diagnostics| diagnostics.state_snapshot().member_id,
  );

  loop {
    if *stop_rx.borrow() {
      break;
    }

    tokio::select! {
      changed = stop_rx.changed() => {
        if changed.is_ok() && *stop_rx.borrow() {
          break;
        }
      }
      next_result = consumer.next() => {
        let next_result = next_result
          .map_err(|error| anyhow!("consumer task {member_id} next failed: {error}"))?;

        match next_result {
          NextResult::Revoked(revoked) => {
            let (ack_tx, ack_rx) = oneshot::channel();
            if event_tx.send(ConsumerTaskEvent::Revoked { ack: ack_tx }).is_err() {
              revoked.complete().await;
              break;
            }

            let acknowledged = {
              tokio::pin!(ack_rx);
              loop {
                tokio::select! {
                  result = &mut ack_rx => break result.is_ok(),
                  changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                      break false;
                    }
                  }
                }
              }
            };
            revoked.complete().await;
            if !acknowledged {
              break;
            }
          },
          NextResult::Record(record) => {
            let id = String::from_utf8(record.record.payload.to_vec())
              .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;

            let _ = event_tx.send(ConsumerTaskEvent::Batch {
              member_id: member_id.clone(),
              ids: vec![id],
              virtual_partition_id: record.virtual_partition_id,
              offset: record.offset,
            });
            consumer.store_offset(record.virtual_partition_id, record.offset)?;
            let _ = consumer.commit().await?;
            let _ = event_tx.send(ConsumerTaskEvent::CommitSucceeded);
          },
        }
      }
    }
  }

  let _ = consumer.shutdown().await;
  Ok(())
}

//
// StopAwareRevocationTestConsumer
//

struct StopAwareRevocationTestConsumer {
  revocation_completed: Arc<AtomicBool>,
  sent_revocation: bool,
}

struct StopAwareRevokedPartitions {
  revocation_completed: Arc<AtomicBool>,
}

#[async_trait]
impl RevokedPartitions for StopAwareRevokedPartitions {
  fn partitions(&self) -> Vec<VirtualPartitionId> {
    vec![0]
  }

  async fn complete(self: Box<Self>) {
    self.revocation_completed.store(true, Ordering::Release);
  }
}

#[async_trait]
impl ConsumerIterator for StopAwareRevocationTestConsumer {
  fn start(&mut self) -> Result<()> {
    Ok(())
  }

  async fn next(&mut self) -> Result<NextResult> {
    if self.sent_revocation {
      std::future::pending().await
    } else {
      self.sent_revocation = true;
      Ok(NextResult::Revoked(Box::new(StopAwareRevokedPartitions {
        revocation_completed: Arc::clone(&self.revocation_completed),
      })))
    }
  }

  fn store_offset(
    &mut self,
    _virtual_partition_id: VirtualPartitionId,
    _offset: u64,
  ) -> Result<()> {
    Ok(())
  }

  async fn commit(&mut self) -> Result<HeartbeatReport> {
    unreachable!("revocation-only test consumer does not commit records")
  }

  async fn shutdown(self: Box<Self>) -> Result<()> {
    Ok(())
  }

  async fn seek(
    &mut self,
    _virtual_partition_id: VirtualPartitionId,
    _target: ConsumerSeekTarget,
  ) -> Result<()> {
    Ok(())
  }
}

pub fn stop_aware_revocation_consumer() -> (Box<dyn ConsumerIterator>, Arc<AtomicBool>) {
  let revocation_completed = Arc::new(AtomicBool::new(false));
  let consumer = Box::new(StopAwareRevocationTestConsumer {
    revocation_completed: Arc::clone(&revocation_completed),
    sent_revocation: false,
  });
  (consumer, revocation_completed)
}

//
// ConsumerDeliveryTrace
//

#[derive(Clone, Debug)]
pub struct ConsumerDeliveryTrace {
  pub member_id: String,
  pub virtual_partition_id: VirtualPartitionId,
  pub offset: u64,
}

pub type ConsumerDeliveryTraces = HashMap<String, Vec<ConsumerDeliveryTrace>>;

//
// ControlledConsumer
//

pub struct ControlledConsumer {
  member_id: String,
  iterator: ConsumerIteratorImpl,
  delivery_traces: ConsumerDeliveryTraces,
}

impl ControlledConsumer {
  #[must_use]
  pub fn new(member_id: impl Into<String>, iterator: ConsumerIteratorImpl) -> Self {
    Self {
      member_id: member_id.into(),
      iterator,
      delivery_traces: ConsumerDeliveryTraces::new(),
    }
  }

  pub fn start(&mut self) -> Result<()> {
    self.iterator.start()
  }

  pub async fn next(&mut self) -> Result<NextResult> {
    let next_result = self.iterator.next().await?;
    if let NextResult::Record(record) = &next_result {
      let id = String::from_utf8(record.record.payload.to_vec())
        .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
      self
        .delivery_traces
        .entry(id)
        .or_default()
        .push(ConsumerDeliveryTrace {
          member_id: self.member_id.clone(),
          virtual_partition_id: record.virtual_partition_id,
          offset: record.offset,
        });
    }
    Ok(next_result)
  }

  pub fn store_offset(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
  ) -> Result<()> {
    self.iterator.store_offset(virtual_partition_id, offset)
  }

  pub async fn commit(&mut self) -> Result<HeartbeatReport> {
    self.iterator.commit().await
  }

  pub async fn abort_for_test(&mut self) -> Result<()> {
    self.iterator.abort_for_test().await
  }

  pub async fn shutdown(self) -> Result<()> {
    Box::new(self.iterator).shutdown().await
  }

  #[must_use]
  pub fn iterator(&self) -> &ConsumerIteratorImpl {
    &self.iterator
  }

  #[must_use]
  pub fn delivery_traces(&self) -> &ConsumerDeliveryTraces {
    &self.delivery_traces
  }
}

pub fn handle_consumer_event_with_trace(
  event: ConsumerTaskEvent,
  delivery_traces: &mut ConsumerDeliveryTraces,
  revocation_count: &mut usize,
) {
  match event {
    ConsumerTaskEvent::Batch {
      member_id,
      ids,
      virtual_partition_id,
      offset,
    } => {
      for id in ids {
        delivery_traces
          .entry(id)
          .or_default()
          .push(ConsumerDeliveryTrace {
            member_id: member_id.clone(),
            virtual_partition_id,
            offset,
          });
      }
    },
    ConsumerTaskEvent::Revoked { ack } => {
      *revocation_count += 1;
      let _ = ack.send(());
    },
    ConsumerTaskEvent::CommitSucceeded => {},
  }
}

#[must_use]
pub fn delivery_counts(delivery_traces: &ConsumerDeliveryTraces) -> HashMap<String, usize> {
  delivery_traces
    .iter()
    .map(|(id, deliveries)| (id.clone(), deliveries.len()))
    .collect()
}

#[must_use]
pub fn maximum_delivery_offsets(
  delivery_traces: &ConsumerDeliveryTraces,
) -> HashMap<VirtualPartitionId, u64> {
  let mut maximum_offsets = HashMap::<VirtualPartitionId, u64>::new();
  for delivery in delivery_traces.values().flatten() {
    maximum_offsets
      .entry(delivery.virtual_partition_id)
      .and_modify(|offset| *offset = (*offset).max(delivery.offset))
      .or_insert(delivery.offset);
  }
  maximum_offsets
}

pub async fn wait_for_group_offsets_committed(
  cluster: &ClusterHarness,
  maximum_offsets: &HashMap<VirtualPartitionId, u64>,
  boundary: &str,
) -> Result<()> {
  timeout(Duration::from_secs(5), async {
    loop {
      let leases = cluster
        .consumer_lease_store()
        .list_group_leases(TOPIC, "integration-group")
        .await?;
      if maximum_offsets
        .iter()
        .all(|(partition_id, maximum_offset)| {
          leases
            .iter()
            .find(|lease| lease.key.virtual_partition_id == *partition_id)
            .is_some_and(|lease| {
              lease.committed_cursor.as_ref().is_some_and(|cursor| {
                cursor.seq_end >= *maximum_offset && cursor.source_checkpoint.is_some()
              })
            })
        })
      {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("{boundary}"))??;
  Ok(())
}

#[must_use]
pub fn delivery_members(delivery_traces: &ConsumerDeliveryTraces) -> HashSet<String> {
  delivery_traces
    .values()
    .flatten()
    .map(|delivery| delivery.member_id.clone())
    .collect()
}

pub fn handle_consumer_event_with_offsets(
  event: ConsumerTaskEvent,
  delivered_id_counts: &mut HashMap<String, usize>,
  last_offsets: &mut HashMap<VirtualPartitionId, u64>,
  revocation_count: &mut usize,
) {
  match event {
    ConsumerTaskEvent::Batch {
      ids,
      virtual_partition_id,
      offset,
      ..
    } => {
      if let Some(previous_offset) = last_offsets.insert(virtual_partition_id, offset) {
        assert!(
          offset >= previous_offset,
          "consumer cursor moved backwards for partition {virtual_partition_id}: \
           previous={previous_offset}, current={offset}"
        );
      }
      for id in ids {
        *delivered_id_counts.entry(id).or_insert(0) += 1;
      }
    },
    ConsumerTaskEvent::Revoked { ack } => {
      *revocation_count += 1;
      let _ = ack.send(());
    },
    ConsumerTaskEvent::CommitSucceeded => {},
  }
}

pub async fn poll_consumer_once(
  consumer: &mut ConsumerIteratorImpl,
  member_id: &str,
  delivery_traces: &mut ConsumerDeliveryTraces,
) -> Result<(bool, bool)> {
  let next_result = timeout(Duration::from_secs(2), consumer.next()).await;
  let next_result = match next_result {
    Err(_) => return Ok((false, false)),
    Ok(Err(error)) => return Err(anyhow!("consumer next failed: {error}")),
    Ok(Ok(next_result)) => next_result,
  };

  match next_result {
    NextResult::Revoked(revoked) => {
      revoked.complete().await;
      Ok((false, true))
    },
    NextResult::Record(record) => {
      let id = String::from_utf8(record.record.payload.to_vec())
        .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
      delivery_traces
        .entry(id)
        .or_default()
        .push(ConsumerDeliveryTrace {
          member_id: member_id.to_string(),
          virtual_partition_id: record.virtual_partition_id,
          offset: record.offset,
        });

      consumer.store_offset(record.virtual_partition_id, record.offset)?;
      let _ = consumer.commit().await?;
      Ok((true, false))
    },
  }
}
