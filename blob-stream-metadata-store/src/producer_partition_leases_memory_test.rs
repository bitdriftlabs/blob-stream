use crate::{
  InMemoryProducerPartitionLeaseStore,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  LeaseReleaseSequenceProgress,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservationOutcome,
};
use blob_stream_types::{SeqRange, offset_datetime_from_unix_millis};
use time::Duration;

fn lease_key() -> ProducerPartitionLeaseKey {
  ProducerPartitionLeaseKey {
    topic: "topic-a".into(),
    virtual_partition_id: 42,
  }
}

#[tokio::test]
async fn fences_lease_holders() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  let outcome = store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("acquire lease");

  assert!(matches!(outcome, LeaseAcquireOutcome::Acquired(_)));

  let outcome = store
    .acquire_lease(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("acquire lease");

  assert!(matches!(outcome, LeaseAcquireOutcome::HeldByOther(_)));

  let outcome = store
    .heartbeat_lease(
      &key,
      "broker-b",
      "session-b",
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("heartbeat lease");

  assert!(matches!(outcome, LeaseHeartbeatOutcome::HeldByOther(_)));

  let outcome = store
    .acquire_lease(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_100),
      Duration::milliseconds(100),
    )
    .await
    .expect("acquire lease after expiration");

  assert!(matches!(outcome, LeaseAcquireOutcome::Acquired(_)));
}

#[tokio::test]
async fn fences_stale_broker_sessions() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  let first = store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-1".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("initial acquisition");
  let LeaseAcquireOutcome::Acquired(first) = first else {
    panic!("expected initial acquisition");
  };
  assert_eq!(first.fence.lease_epoch, 1);

  let renewal = store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-1".to_string(),
      offset_datetime_from_unix_millis(1_050),
      Duration::milliseconds(100),
    )
    .await
    .expect("same session renewal");
  let LeaseAcquireOutcome::Acquired(renewal) = renewal else {
    panic!("expected same session renewal");
  };
  assert_eq!(renewal.fence.lease_epoch, 1);

  let live_takeover = store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-2".to_string(),
      offset_datetime_from_unix_millis(1_100),
      Duration::milliseconds(100),
    )
    .await
    .expect("live takeover attempt");
  assert!(matches!(live_takeover, LeaseAcquireOutcome::HeldByOther(_)));

  let takeover = store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-2".to_string(),
      offset_datetime_from_unix_millis(1_150),
      Duration::milliseconds(100),
    )
    .await
    .expect("expired takeover");
  let LeaseAcquireOutcome::Acquired(takeover) = takeover else {
    panic!("expected expired takeover");
  };
  assert_eq!(takeover.fence.lease_epoch, 2);

  let heartbeat = store
    .heartbeat_lease(
      &key,
      "broker-a",
      "session-1",
      offset_datetime_from_unix_millis(1_150),
      Duration::milliseconds(100),
    )
    .await
    .expect("stale heartbeat");
  assert!(matches!(heartbeat, LeaseHeartbeatOutcome::HeldByOther(_)));

  let reservation = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-1",
      offset_datetime_from_unix_millis(1_150),
      1,
    )
    .await
    .expect("stale reservation");
  assert!(matches!(
    reservation,
    SequenceReservationOutcome::HeldByOther(_)
  ));

  let release = store
    .release_lease(
      &key,
      &first.fence,
      offset_datetime_from_unix_millis(1_150),
      LeaseReleaseSequenceProgress::Preserve,
    )
    .await
    .expect("stale release");
  assert!(matches!(release, LeaseReleaseOutcome::HeldByOther(_)));
}

#[tokio::test]
async fn reserves_sequences_in_order() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("acquire lease");

  let first = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      5,
    )
    .await
    .expect("reserve seq");

  let SequenceReservationOutcome::Reserved(first) = first else {
    panic!("expected reservation");
  };

  assert_eq!(first.range.start, 0);
  assert_eq!(first.range.end, 4);

  let second = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      3,
    )
    .await
    .expect("reserve seq");

  let SequenceReservationOutcome::Reserved(second) = second else {
    panic!("expected reservation");
  };

  assert_eq!(second.range.start, 5);
  assert_eq!(second.range.end, 7);
}

#[tokio::test]
async fn acquires_and_reserves_sequences_in_one_operation() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  let first = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(5),
    )
    .await
    .expect("acquire and reserve");
  let LeaseAcquireAndReserveOutcome::Acquired { lease, reservation } = first else {
    panic!("expected acquired lease");
  };
  assert_eq!(
    lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_100)
  );
  assert_eq!(lease.max_allocated_seq, Some(4));
  assert_eq!(reservation, Some(SeqRange { start: 0, end: 4 }));

  let second = store
    .acquire_lease_and_reserve_sequences(
      key,
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_050),
      Duration::milliseconds(100),
      Some(3),
    )
    .await
    .expect("renew and reserve");
  let LeaseAcquireAndReserveOutcome::Acquired { lease, reservation } = second else {
    panic!("expected renewed lease");
  };
  assert_eq!(
    lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_150)
  );
  assert_eq!(lease.max_allocated_seq, Some(7));
  assert_eq!(reservation, Some(SeqRange { start: 5, end: 7 }));
}

#[tokio::test]
async fn preserves_previous_lease_when_takeover_reservation_overflows() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(u64::MAX),
    )
    .await
    .expect("initial acquisition and reservation");

  let error = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_100),
      Duration::milliseconds(100),
      Some(2),
    )
    .await
    .expect_err("overflowing takeover reservation must fail");
  assert!(error.to_string().contains("sequence range overflow"));

  let lease = store
    .get_lease(&key)
    .await
    .expect("read lease after failed takeover")
    .expect("previous lease remains present");
  assert_eq!(lease.fence.holder_id, "broker-a");
  assert_eq!(
    lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_100)
  );
  assert_eq!(lease.max_allocated_seq, Some(u64::MAX - 1));
  let fence = lease.fence;
  assert_eq!(fence.lease_epoch, 1);
  assert_eq!(fence.lease_session_id, "session-a");
}

#[tokio::test]
async fn releases_lease_for_current_holder() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  let lease = store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("acquire lease");
  let LeaseAcquireOutcome::Acquired(lease) = lease else {
    panic!("expected acquired lease");
  };

  let reservation = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      5,
    )
    .await
    .expect("reserve sequences");
  assert!(matches!(
    reservation,
    SequenceReservationOutcome::Reserved(_)
  ));

  let outcome = store
    .release_lease(
      &key,
      &lease.fence,
      offset_datetime_from_unix_millis(1_000),
      LeaseReleaseSequenceProgress::Preserve,
    )
    .await
    .expect("release lease");
  assert!(matches!(outcome, LeaseReleaseOutcome::Released));

  let released_lease = store
    .get_lease(&key)
    .await
    .expect("look up released lease")
    .expect("released lease row remains available");
  assert_eq!(
    released_lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_000)
  );
  assert_eq!(released_lease.max_allocated_seq, Some(4));

  let reacquired = store
    .acquire_lease(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("acquire after release");
  assert!(matches!(reacquired, LeaseAcquireOutcome::Acquired(_)));

  let reservation = store
    .reserve_sequences(
      &key,
      "broker-b",
      "session-b",
      offset_datetime_from_unix_millis(1_000),
      2,
    )
    .await
    .expect("reserve sequences after release");
  let SequenceReservationOutcome::Reserved(reservation) = reservation else {
    panic!("expected reservation");
  };
  assert_eq!(reservation.range.start, 5);
  assert_eq!(reservation.range.end, 6);
}

#[tokio::test]
async fn release_reclaims_unused_sequence_reservation_tail() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  let acquired = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(10),
    )
    .await
    .expect("acquire and reserve");
  let LeaseAcquireAndReserveOutcome::Acquired { lease, reservation } = acquired else {
    panic!("expected acquired lease");
  };
  assert_eq!(reservation, Some(SeqRange { start: 0, end: 9 }));

  let outcome = store
    .release_lease(
      &key,
      &lease.fence,
      offset_datetime_from_unix_millis(1_000),
      LeaseReleaseSequenceProgress::Set(Some(2)),
    )
    .await
    .expect("release lease with used progress");
  assert_eq!(outcome, LeaseReleaseOutcome::Released);

  let acquired = store
    .acquire_lease_and_reserve_sequences(
      key,
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(2),
    )
    .await
    .expect("successor acquire and reserve");
  let LeaseAcquireAndReserveOutcome::Acquired { reservation, .. } = acquired else {
    panic!("expected successor acquisition");
  };
  assert_eq!(reservation, Some(SeqRange { start: 3, end: 4 }));
}

#[tokio::test]
async fn release_reclaims_an_entirely_unused_initial_reservation() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  let acquired = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(10),
    )
    .await
    .expect("acquire and reserve");
  let LeaseAcquireAndReserveOutcome::Acquired { lease, reservation } = acquired else {
    panic!("expected acquired lease");
  };
  assert_eq!(reservation, Some(SeqRange { start: 0, end: 9 }));

  let outcome = store
    .release_lease(
      &key,
      &lease.fence,
      offset_datetime_from_unix_millis(1_000),
      LeaseReleaseSequenceProgress::Set(None),
    )
    .await
    .expect("release lease with no used sequences");
  assert_eq!(outcome, LeaseReleaseOutcome::Released);

  let acquired = store
    .acquire_lease_and_reserve_sequences(
      key,
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(2),
    )
    .await
    .expect("successor acquire and reserve");
  let LeaseAcquireAndReserveOutcome::Acquired { reservation, .. } = acquired else {
    panic!("expected successor acquisition");
  };
  assert_eq!(reservation, Some(SeqRange { start: 0, end: 1 }));
}

#[tokio::test]
async fn later_graceful_release_reclaims_its_unused_reservation_tail() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();

  let LeaseAcquireAndReserveOutcome::Acquired { lease, .. } = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(10),
    )
    .await
    .expect("acquire first lease")
  else {
    panic!("expected first lease acquisition");
  };
  store
    .release_lease(
      &key,
      &lease.fence,
      offset_datetime_from_unix_millis(1_000),
      LeaseReleaseSequenceProgress::Set(Some(2)),
    )
    .await
    .expect("release first lease");

  let LeaseAcquireAndReserveOutcome::Acquired {
    lease,
    reservation: Some(reservation),
  } = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(10),
    )
    .await
    .expect("acquire second lease")
  else {
    panic!("expected second lease acquisition and reservation");
  };
  assert_eq!(reservation, SeqRange { start: 3, end: 12 });
  store
    .release_lease(
      &key,
      &lease.fence,
      offset_datetime_from_unix_millis(1_000),
      LeaseReleaseSequenceProgress::Set(Some(4)),
    )
    .await
    .expect("release second lease");

  let LeaseAcquireAndReserveOutcome::Acquired {
    reservation: Some(reservation),
    ..
  } = store
    .acquire_lease_and_reserve_sequences(
      key,
      "broker-c".to_string(),
      "session-c".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(2),
    )
    .await
    .expect("acquire third lease")
  else {
    panic!("expected third lease acquisition and reservation");
  };
  assert_eq!(reservation, SeqRange { start: 5, end: 6 });
}

#[tokio::test]
async fn stale_release_cannot_lower_a_successor_reservation() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();
  let LeaseAcquireAndReserveOutcome::Acquired {
    lease: stale_lease, ..
  } = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      Some(10),
    )
    .await
    .expect("acquire first lease")
  else {
    panic!("expected first lease acquisition");
  };
  let LeaseAcquireAndReserveOutcome::Acquired {
    reservation: Some(reservation),
    ..
  } = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_100),
      Duration::milliseconds(100),
      Some(2),
    )
    .await
    .expect("acquire successor lease")
  else {
    panic!("expected successor lease acquisition and reservation");
  };
  assert_eq!(reservation, SeqRange { start: 10, end: 11 });

  let release = store
    .release_lease(
      &key,
      &stale_lease.fence,
      offset_datetime_from_unix_millis(1_100),
      LeaseReleaseSequenceProgress::Set(Some(2)),
    )
    .await
    .expect("stale release");
  assert!(matches!(release, LeaseReleaseOutcome::HeldByOther(_)));
  assert_eq!(
    store
      .get_lease(&key)
      .await
      .expect("lookup successor lease")
      .expect("successor lease remains")
      .max_allocated_seq,
    Some(11)
  );
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
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("acquire lease");

  let lease = store
    .get_lease(&key)
    .await
    .expect("lookup active lease")
    .expect("active lease exists");
  assert_eq!(lease.fence.holder_id, "broker-a");
  assert_eq!(
    lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_100)
  );
}
