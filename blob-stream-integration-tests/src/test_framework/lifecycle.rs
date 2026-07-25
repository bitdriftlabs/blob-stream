#[cfg(test)]
#[path = "./lifecycle_test.rs"]
mod tests;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_broker::write::BrokerLifecycleHooks;
use blob_stream_consumer::ConsumerLifecycleHooks;
use blob_stream_types::VirtualPartitionId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

//
// LifecycleEvent
//

/// Observable lifecycle boundaries that deterministic integration tests can gate.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LifecycleEvent {
  BrokerBeforeFlushPersist,
  BrokerBlobPersisted,
  BrokerMetadataPersisted,
  BrokerLeaseDrainStarted,
  BrokerPartitionDrained,
  BrokerBeforeLeaseRelease,
  BrokerLeaseReleased,
  ConsumerRevocationEmitted,
  ConsumerPrefetchBatchBuffered,
  ConsumerRecoveryFastPathActive,
  ConsumerBeforeRebalance,
  ConsumerRebalanceApplied,
  ConsumerBeforeCommit,
  ConsumerShutdownCommitFinished,
  ConsumerBeforeReleaseOwned,
  ConsumerBeforeDeregisterMember,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum LifecycleGateKey {
  Event(LifecycleEvent),
  Consumer {
    event: LifecycleEvent,
    member_id: String,
    virtual_partition_id: Option<VirtualPartitionId>,
    generation: Option<u64>,
  },
  BrokerPartition {
    event: LifecycleEvent,
    virtual_partition_id: VirtualPartitionId,
  },
  ConsumerPrefetchPartition(VirtualPartitionId),
}

//
// LifecycleGate
//

/// One-shot gate that a test releases after a selected lifecycle event is reached.
pub struct LifecycleGate {
  entered: Option<oneshot::Receiver<()>>,
  release: Option<oneshot::Sender<()>>,
}

impl LifecycleGate {
  /// Wait for the gated event to be reached.
  pub async fn wait_until_reached(&mut self) -> Result<()> {
    self
      .entered
      .take()
      .ok_or_else(|| anyhow!("lifecycle gate was already awaited"))?
      .await
      .map_err(|_| anyhow!("lifecycle hook dropped before reaching its armed event"))
  }

  /// Allow the production operation paused at the event to continue.
  pub fn release(mut self) -> Result<()> {
    self
      .release
      .take()
      .ok_or_else(|| anyhow!("lifecycle gate was already released"))?
      .send(())
      .map_err(|()| anyhow!("lifecycle operation stopped before gate release"))
  }
}

//
// TestLifecycleHooks
//

/// Test-only hook implementation that blocks only explicitly armed lifecycle events.
#[derive(Clone, Default)]
pub struct TestLifecycleHooks {
  gates: Arc<Mutex<HashMap<LifecycleGateKey, ArmedLifecycleGate>>>,
}

struct ArmedLifecycleGate {
  entered: oneshot::Sender<()>,
  release: oneshot::Receiver<()>,
}

impl TestLifecycleHooks {
  /// Arm a one-shot gate for one lifecycle event.
  pub async fn arm(&self, event: LifecycleEvent) -> Result<LifecycleGate> {
    self.arm_key(LifecycleGateKey::Event(event)).await
  }

  /// Arm a consumer lifecycle gate scoped to one member and optional partition and generation.
  pub async fn arm_consumer(
    &self,
    event: LifecycleEvent,
    member_id: &str,
    virtual_partition_id: Option<VirtualPartitionId>,
    generation: Option<u64>,
  ) -> Result<LifecycleGate> {
    self
      .arm_key(LifecycleGateKey::Consumer {
        event,
        member_id: member_id.to_string(),
        virtual_partition_id,
        generation,
      })
      .await
  }

  /// Arm a prefetch gate that only pauses the selected virtual partition.
  pub async fn arm_prefetch_for_partition(
    &self,
    virtual_partition_id: VirtualPartitionId,
  ) -> Result<LifecycleGate> {
    self
      .arm_key(LifecycleGateKey::ConsumerPrefetchPartition(
        virtual_partition_id,
      ))
      .await
  }

  /// Arm a broker lifecycle gate that only pauses the selected virtual partition.
  pub async fn arm_broker_for_partition(
    &self,
    event: LifecycleEvent,
    virtual_partition_id: VirtualPartitionId,
  ) -> Result<LifecycleGate> {
    self
      .arm_key(LifecycleGateKey::BrokerPartition {
        event,
        virtual_partition_id,
      })
      .await
  }

  async fn arm_key(&self, key: LifecycleGateKey) -> Result<LifecycleGate> {
    let mut gates = self.gates.lock().await;
    if gates.contains_key(&key) {
      return Err(anyhow!("lifecycle gate {key:?} already has an armed gate"));
    }
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    gates.insert(
      key.clone(),
      ArmedLifecycleGate {
        entered: entered_tx,
        release: release_rx,
      },
    );
    Ok(LifecycleGate {
      entered: Some(entered_rx),
      release: Some(release_tx),
    })
  }

  async fn reach_prefetch(&self, member_id: &str, virtual_partition_id: VirtualPartitionId) {
    let gate = {
      let mut gates = self.gates.lock().await;
      gates
        .remove(&LifecycleGateKey::ConsumerPrefetchPartition(
          virtual_partition_id,
        ))
        .or_else(|| {
          Self::take_consumer_gate(
            &mut gates,
            LifecycleEvent::ConsumerPrefetchBatchBuffered,
            member_id,
            None,
            &[virtual_partition_id],
          )
        })
        .or_else(|| {
          gates.remove(&LifecycleGateKey::Event(
            LifecycleEvent::ConsumerPrefetchBatchBuffered,
          ))
        })
    };
    Self::wait_for_gate(gate).await;
  }

  async fn reach_broker_partition(
    &self,
    event: LifecycleEvent,
    virtual_partition_id: VirtualPartitionId,
  ) {
    let gate = {
      let mut gates = self.gates.lock().await;
      gates
        .remove(&LifecycleGateKey::BrokerPartition {
          event,
          virtual_partition_id,
        })
        .or_else(|| gates.remove(&LifecycleGateKey::Event(event)))
    };
    Self::wait_for_gate(gate).await;
  }

  fn take_consumer_gate(
    gates: &mut HashMap<LifecycleGateKey, ArmedLifecycleGate>,
    event: LifecycleEvent,
    member_id: &str,
    generation: Option<u64>,
    partitions: &[VirtualPartitionId],
  ) -> Option<ArmedLifecycleGate> {
    let key = gates
      .keys()
      .filter_map(|key| {
        let LifecycleGateKey::Consumer {
          event: armed_event,
          member_id: armed_member_id,
          virtual_partition_id,
          generation: armed_generation,
        } = key
        else {
          return None;
        };
        (*armed_event == event
          && armed_member_id == member_id
          && armed_generation.is_none_or(|armed_generation| Some(armed_generation) == generation)
          && virtual_partition_id.is_none_or(|partition_id| partitions.contains(&partition_id)))
        .then_some((
          (
            armed_generation.is_some() && virtual_partition_id.is_some(),
            armed_generation.is_some(),
            virtual_partition_id.is_some(),
          ),
          key,
        ))
      })
      .max_by_key(|(specificity, _)| *specificity)
      .map(|(_, key)| key)
      .cloned()?;
    gates.remove(&key)
  }

  async fn reach_consumer(
    &self,
    event: LifecycleEvent,
    member_id: &str,
    generation: u64,
    partitions: &[VirtualPartitionId],
  ) {
    let gate = {
      let mut gates = self.gates.lock().await;
      Self::take_consumer_gate(&mut gates, event, member_id, Some(generation), partitions)
        .or_else(|| gates.remove(&LifecycleGateKey::Event(event)))
    };
    Self::wait_for_gate(gate).await;
  }

  async fn wait_for_gate(gate: Option<ArmedLifecycleGate>) {
    let Some(gate) = gate else {
      return;
    };
    let _ = gate.entered.send(());
    let _ = gate.release.await;
  }
}

#[async_trait]
impl BrokerLifecycleHooks for TestLifecycleHooks {
  async fn before_flush_persist(&self, _topic: &str, partitions: &[VirtualPartitionId]) {
    for virtual_partition_id in partitions {
      self
        .reach_broker_partition(
          LifecycleEvent::BrokerBeforeFlushPersist,
          *virtual_partition_id,
        )
        .await;
    }
  }

  async fn blob_persisted(&self, _topic: &str, partitions: &[VirtualPartitionId]) {
    for virtual_partition_id in partitions {
      self
        .reach_broker_partition(LifecycleEvent::BrokerBlobPersisted, *virtual_partition_id)
        .await;
    }
  }

  async fn metadata_persisted(&self, _topic: &str, partitions: &[VirtualPartitionId]) {
    for virtual_partition_id in partitions {
      self
        .reach_broker_partition(
          LifecycleEvent::BrokerMetadataPersisted,
          *virtual_partition_id,
        )
        .await;
    }
  }

  async fn lease_drain_started(&self, _topic: &str, virtual_partition_id: VirtualPartitionId) {
    self
      .reach_broker_partition(
        LifecycleEvent::BrokerLeaseDrainStarted,
        virtual_partition_id,
      )
      .await;
  }

  async fn partition_drained(&self, _topic: &str, virtual_partition_id: VirtualPartitionId) {
    self
      .reach_broker_partition(LifecycleEvent::BrokerPartitionDrained, virtual_partition_id)
      .await;
  }

  async fn before_lease_release(&self, _topic: &str, virtual_partition_id: VirtualPartitionId) {
    self
      .reach_broker_partition(
        LifecycleEvent::BrokerBeforeLeaseRelease,
        virtual_partition_id,
      )
      .await;
  }

  async fn lease_released(&self, _topic: &str, virtual_partition_id: VirtualPartitionId) {
    self
      .reach_broker_partition(LifecycleEvent::BrokerLeaseReleased, virtual_partition_id)
      .await;
  }
}

#[async_trait]
impl ConsumerLifecycleHooks for TestLifecycleHooks {
  async fn revocation_emitted(
    &self,
    member_id: &str,
    generation: u64,
    partitions: &[VirtualPartitionId],
  ) {
    self
      .reach_consumer(
        LifecycleEvent::ConsumerRevocationEmitted,
        member_id,
        generation,
        partitions,
      )
      .await;
  }

  async fn prefetch_batch_buffered(
    &self,
    member_id: &str,
    virtual_partition_id: VirtualPartitionId,
  ) {
    self.reach_prefetch(member_id, virtual_partition_id).await;
  }

  async fn recovery_fast_path_active(
    &self,
    member_id: &str,
    generation: u64,
    virtual_partition_id: VirtualPartitionId,
  ) {
    self
      .reach_consumer(
        LifecycleEvent::ConsumerRecoveryFastPathActive,
        member_id,
        generation,
        &[virtual_partition_id],
      )
      .await;
  }

  async fn before_rebalance(&self, member_id: &str, generation: u64) {
    self
      .reach_consumer(
        LifecycleEvent::ConsumerBeforeRebalance,
        member_id,
        generation,
        &[],
      )
      .await;
  }

  async fn rebalance_applied(
    &self,
    member_id: &str,
    generation: u64,
    partitions: &[VirtualPartitionId],
  ) {
    self
      .reach_consumer(
        LifecycleEvent::ConsumerRebalanceApplied,
        member_id,
        generation,
        partitions,
      )
      .await;
  }

  async fn before_commit(&self, member_id: &str, generation: u64) {
    self
      .reach_consumer(
        LifecycleEvent::ConsumerBeforeCommit,
        member_id,
        generation,
        &[],
      )
      .await;
  }

  async fn shutdown_commit_finished(&self, member_id: &str, generation: u64) {
    self
      .reach_consumer(
        LifecycleEvent::ConsumerShutdownCommitFinished,
        member_id,
        generation,
        &[],
      )
      .await;
  }

  async fn before_release_owned(&self, member_id: &str, generation: u64) {
    self
      .reach_consumer(
        LifecycleEvent::ConsumerBeforeReleaseOwned,
        member_id,
        generation,
        &[],
      )
      .await;
  }

  async fn before_deregister_member(&self, member_id: &str, generation: u64) {
    self
      .reach_consumer(
        LifecycleEvent::ConsumerBeforeDeregisterMember,
        member_id,
        generation,
        &[],
      )
      .await;
  }
}
