use super::{
  FaultInjectedConsumerGroupLeaseStore,
  FaultInjectedConsumerGroupMembershipStore,
  FaultInjectedProducerPartitionLeaseStore,
  StoreFaultAction,
  StoreFaultController,
  StoreFaultDomain,
  StoreFaultOperation,
  StoreFaultRule,
};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryProducerPartitionLeaseStore,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
};
use std::sync::Arc;

#[tokio::test]
async fn coalesced_reservation_honors_sequence_reservation_faults() {
  let controller = StoreFaultController::default();
  controller
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ProducerLease,
      operation: StoreFaultOperation::ProducerReserveSequences,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "reservation unavailable".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;
  let store = FaultInjectedProducerPartitionLeaseStore::new(
    Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    controller,
  );
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };

  let error = store
    .acquire_lease_and_reserve_sequences(key.clone(), "broker-a".to_string(), 1_000, 100, Some(1))
    .await
    .expect_err("coalesced reservation should honor the reservation fault");
  assert!(error.to_string().contains("reservation unavailable"));
  assert!(
    store
      .get_lease(&key)
      .await
      .expect("look up lease")
      .is_none()
  );
}

#[tokio::test]
async fn consumer_partition_release_honors_faults_without_releasing_ownership() {
  let controller = StoreFaultController::default();
  let inner: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let store = FaultInjectedConsumerGroupLeaseStore::new(Arc::clone(&inner), controller.clone());
  let key = ConsumerGroupLeaseKey {
    topic: "telemetry".into(),
    group_id: "group-a".into(),
    virtual_partition_id: 0,
  };
  inner
    .assign_partition(key.clone(), "consumer-a".to_string(), 1, 1_000, 100)
    .await
    .expect("assign partition");
  controller
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerReleasePartition,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "release unavailable".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;

  let error = store
    .release_partition(&key, "consumer-a", 1, 1_001)
    .await
    .expect_err("release should honor the injected fault");
  assert!(error.to_string().contains("release unavailable"));
  assert_eq!(
    inner
      .list_group_leases("telemetry", "group-a")
      .await
      .expect("list leases")
      .into_iter()
      .find(|lease| lease.key == key)
      .map(|lease| lease.owner_id),
    Some("consumer-a".to_string())
  );
}

#[tokio::test]
async fn consumer_member_deregistration_honors_faults_without_removing_membership() {
  let controller = StoreFaultController::default();
  let inner: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let store =
    FaultInjectedConsumerGroupMembershipStore::new(Arc::clone(&inner), controller.clone());
  inner
    .register_member("telemetry", "group-a", "consumer-a", 1_000, 100)
    .await
    .expect("register member");
  controller
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerDeregisterMember,
      key_pattern: Some("consumer-a".to_string()),
      action: StoreFaultAction::Fail {
        message: "deregistration unavailable".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;

  let error = store
    .deregister_member("telemetry", "group-a", "consumer-a")
    .await
    .expect_err("deregistration should honor the injected fault");
  assert!(error.to_string().contains("deregistration unavailable"));
  assert_eq!(
    inner
      .list_active_members("telemetry", "group-a", 1_001)
      .await
      .expect("list active members"),
    vec!["consumer-a".to_string()]
  );
}
