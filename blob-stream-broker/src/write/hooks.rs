use async_trait::async_trait;
use blob_stream_types::VirtualPartitionId;

//
// BrokerLifecycleHooks
//

/// Test-oriented lifecycle observation points for broker flush and lease transitions.
#[async_trait]
pub trait BrokerLifecycleHooks: Send + Sync {
  /// Runs immediately before a flush plan persists its segment blob.
  async fn before_flush_persist(&self, _topic: &str, _partitions: &[VirtualPartitionId]) {}

  /// Runs after a flush plan persists its segment blob.
  async fn blob_persisted(&self, _topic: &str, _partitions: &[VirtualPartitionId]) {}

  /// Runs after a flush plan persists its segment metadata.
  async fn metadata_persisted(&self, _topic: &str, _partitions: &[VirtualPartitionId]) {}

  /// Runs after a partition is marked draining and before its accepted work drains.
  async fn lease_drain_started(&self, _topic: &str, _virtual_partition_id: VirtualPartitionId) {}

  /// Runs after a membership snapshot publishes its local write assignment.
  async fn assignment_published(&self) {}

  /// Runs after a draining partition has no buffered or in-flight accepted work.
  async fn partition_drained(&self, _topic: &str, _virtual_partition_id: VirtualPartitionId) {}

  /// Runs immediately before a producer-partition lease release attempt.
  async fn before_lease_release(&self, _topic: &str, _virtual_partition_id: VirtualPartitionId) {}

  /// Runs after a producer-partition lease release attempt succeeds or is fenced.
  async fn lease_released(&self, _topic: &str, _virtual_partition_id: VirtualPartitionId) {}
}

//
// NoopBrokerLifecycleHooks
//

/// Production lifecycle hooks that preserve normal broker behavior.
#[derive(Default)]
pub struct NoopBrokerLifecycleHooks;

#[async_trait]
impl BrokerLifecycleHooks for NoopBrokerLifecycleHooks {}
