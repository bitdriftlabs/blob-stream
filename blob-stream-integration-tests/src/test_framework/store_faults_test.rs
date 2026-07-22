use super::{
  FaultInjectedProducerPartitionLeaseStore,
  StoreFaultAction,
  StoreFaultController,
  StoreFaultDomain,
  StoreFaultOperation,
  StoreFaultRule,
};
use blob_stream_metadata_store::{
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
