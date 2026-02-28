// blob-stream - in-memory producer partition leases tests
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{
  InMemoryProducerPartitionLeaseStore,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservationOutcome,
};

fn lease_key() -> ProducerPartitionLeaseKey {
  ProducerPartitionLeaseKey {
    topic: "topic-a".to_string(),
    writer_id: 1,
    virtual_partition_id: 42,
  }
}

#[tokio::test]
async fn fences_lease_holders() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  let outcome = store
    .acquire_lease(key.clone(), "broker-a".to_string(), 1000, 100)
    .await
    .expect("acquire lease");

  assert!(matches!(outcome, LeaseAcquireOutcome::Acquired(_)));

  let outcome = store
    .acquire_lease(key.clone(), "broker-b".to_string(), 1000, 100)
    .await
    .expect("acquire lease");

  assert!(matches!(outcome, LeaseAcquireOutcome::HeldByOther(_)));

  let outcome = store
    .heartbeat_lease(&key, "broker-b", 1000, 100)
    .await
    .expect("heartbeat lease");

  assert!(matches!(outcome, LeaseHeartbeatOutcome::HeldByOther(_)));

  let outcome = store
    .acquire_lease(key.clone(), "broker-b".to_string(), 1100, 100)
    .await
    .expect("acquire lease after expiration");

  assert!(matches!(outcome, LeaseAcquireOutcome::Acquired(_)));
}

#[tokio::test]
async fn reserves_sequences_in_order() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  store
    .acquire_lease(key.clone(), "broker-a".to_string(), 1000, 100)
    .await
    .expect("acquire lease");

  let first = store
    .reserve_sequences(&key, "broker-a", 1000, 5)
    .await
    .expect("reserve seq");

  let SequenceReservationOutcome::Reserved(first) = first else {
    panic!("expected reservation");
  };

  assert_eq!(first.range.start, 0);
  assert_eq!(first.range.end, 4);

  let second = store
    .reserve_sequences(&key, "broker-a", 1000, 3)
    .await
    .expect("reserve seq");

  let SequenceReservationOutcome::Reserved(second) = second else {
    panic!("expected reservation");
  };

  assert_eq!(second.range.start, 5);
  assert_eq!(second.range.end, 7);
}

#[tokio::test]
async fn releases_lease_for_current_holder() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  store
    .acquire_lease(key.clone(), "broker-a".to_string(), 1000, 100)
    .await
    .expect("acquire lease");

  let outcome = store
    .release_lease(&key, "broker-a", 1000)
    .await
    .expect("release lease");
  assert!(matches!(outcome, LeaseReleaseOutcome::Released));

  let reacquired = store
    .acquire_lease(key, "broker-b".to_string(), 1000, 100)
    .await
    .expect("acquire after release");
  assert!(matches!(reacquired, LeaseAcquireOutcome::Acquired(_)));
}
