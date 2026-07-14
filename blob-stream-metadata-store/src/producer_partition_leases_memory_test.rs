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

  let reservation = store
    .reserve_sequences(&key, "broker-a", 1_000, 5)
    .await
    .expect("reserve sequences");
  assert!(matches!(
    reservation,
    SequenceReservationOutcome::Reserved(_)
  ));

  let outcome = store
    .release_lease(&key, "broker-a", 1000)
    .await
    .expect("release lease");
  assert!(matches!(outcome, LeaseReleaseOutcome::Released));

  let released_lease = store
    .get_lease(&key)
    .await
    .expect("look up released lease")
    .expect("released lease row remains available");
  assert_eq!(released_lease.lease_expiration_ts_ms, 1_000);
  assert_eq!(released_lease.max_allocated_seq, Some(4));

  let reacquired = store
    .acquire_lease(key.clone(), "broker-b".to_string(), 1_000, 100)
    .await
    .expect("acquire after release");
  assert!(matches!(reacquired, LeaseAcquireOutcome::Acquired(_)));

  let reservation = store
    .reserve_sequences(&key, "broker-b", 1_000, 2)
    .await
    .expect("reserve sequences after release");
  let SequenceReservationOutcome::Reserved(reservation) = reservation else {
    panic!("expected reservation");
  };
  assert_eq!(reservation.range.start, 5);
  assert_eq!(reservation.range.end, 6);
}

#[tokio::test]
async fn lookup_reports_absent_and_active_leases() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  assert!(
    store
      .get_lease(&key)
      .await
      .expect("lookup absent lease")
      .is_none()
  );

  store
    .acquire_lease(key.clone(), "broker-a".to_string(), 1_000, 100)
    .await
    .expect("acquire lease");

  let lease = store
    .get_lease(&key)
    .await
    .expect("lookup active lease")
    .expect("active lease exists");
  assert_eq!(lease.holder_id, "broker-a");
  assert_eq!(lease.lease_expiration_ts_ms, 1_100);
}
