use super::{
  FaultInjectedBlobStore,
  FaultInjectedConsumerGroupLeaseStore,
  FaultInjectedConsumerGroupMembershipStore,
  FaultInjectedProducerPartitionLeaseStore,
  StoreFaultAction,
  StoreFaultController,
  StoreFaultDomain,
  StoreFaultOperation,
  StoreFaultRule,
};
use blob_stream_blob_store::{BlobKey, BlobStore, BlobStoreError, InMemoryBlobStore};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMember,
  ConsumerGroupMembershipStore,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryProducerPartitionLeaseStore,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  ProducerSequenceProgress,
};
use blob_stream_types::offset_datetime_from_unix_millis;
use std::sync::Arc;
use time::Duration;

#[tokio::test]
async fn blob_cache_admission_read_honors_not_found_faults() {
  let controller = StoreFaultController::default();
  let store = FaultInjectedBlobStore::new(Arc::new(InMemoryBlobStore::new()), controller.clone());
  let key = BlobKey::from("telemetry/blob");
  store
    .put(&key, bytes::Bytes::from_static(b"payload"))
    .await
    .expect("seed blob");
  controller
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Blob,
      operation: StoreFaultOperation::BlobGetWithCacheAdmission,
      key_pattern: Some(key.as_str().to_string()),
      action: StoreFaultAction::NotFound,
      remaining_hits: Some(1),
    })
    .await;

  let error = store
    .get_with_cache_admission(&key, &|_| true)
    .await
    .expect_err("cache admission read should honor the injected not-found fault");
  assert!(matches!(error, BlobStoreError::NotFound { .. }));
  assert!(controller.events().await.iter().any(|event| {
    event.operation == StoreFaultOperation::BlobGetWithCacheAdmission
      && event
        .action
        .as_ref()
        .is_some_and(|action| matches!(action, StoreFaultAction::NotFound))
  }));
}

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
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(1),
      ProducerSequenceProgress::default(),
    )
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
    .assign_partition(
      key.clone(),
      "consumer-a".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
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
    .release_partition(
      &key,
      "consumer-a",
      1,
      offset_datetime_from_unix_millis(1_001),
    )
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
    .register_member(
      "telemetry",
      "group-a",
      "consumer-a",
      None,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
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
      .list_active_members(
        "telemetry",
        "group-a",
        offset_datetime_from_unix_millis(1_001),
      )
      .await
      .expect("list active members"),
    vec![ConsumerGroupMember {
      member_id: "consumer-a".to_string(),
      pod_id: None,
    }]
  );
}
