use crate::{
  InMemoryProducerPartitionLeaseStore,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  ProducerSequenceProgress,
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      "broker-a",
      "session-1",
      offset_datetime_from_unix_millis(1_150),
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
async fn persists_sequence_progress_with_each_successful_mutation() {
  let store = InMemoryProducerPartitionLeaseStore::new();
  let key = lease_key();
  let now = offset_datetime_from_unix_millis(1_000);
  let initial_progress = ProducerSequenceProgress::default();

  store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      now,
      Duration::milliseconds(100),
      initial_progress,
    )
    .await
    .expect("acquire lease");
  let first = store
    .reserve_sequences(&key, "broker-a", "session-a", now, 5, initial_progress)
    .await
    .expect("reserve initial range");
  assert!(matches!(first, SequenceReservationOutcome::Reserved(_)));

  let progress = ProducerSequenceProgress {
    reservation_start: Some(0),
    last_handed_out_seq: Some(2),
  };
  store
    .heartbeat_lease(
      &key,
      "broker-a",
      "session-a",
      now,
      Duration::milliseconds(100),
      progress,
    )
    .await
    .expect("heartbeat lease");
  let extended = store
    .reserve_sequences(&key, "broker-a", "session-a", now, 3, progress)
    .await
    .expect("extend range");
  assert!(matches!(extended, SequenceReservationOutcome::Reserved(_)));

  let lease = store
    .get_lease(&key)
    .await
    .expect("read lease")
    .expect("lease exists");
  assert_eq!(lease.reservation_start, Some(0));
  assert_eq!(lease.max_allocated_seq, Some(7));
  assert_eq!(lease.last_handed_out_seq, Some(2));
  assert_eq!(lease.sequence_progress_updated_at, Some(now));

  store
    .release_lease(&key, "broker-a", "session-a", now, progress)
    .await
    .expect("release lease");
  let replacement = store
    .acquire_lease(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      now,
      Duration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await
    .expect("take over released lease");
  assert!(matches!(replacement, LeaseAcquireOutcome::Acquired(_)));
  let lease = store
    .get_lease(&key)
    .await
    .expect("read replacement lease")
    .expect("replacement lease exists");
  assert_eq!(lease.reservation_start, None);
  assert_eq!(lease.last_handed_out_seq, None);
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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

  store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await
    .expect("acquire lease");

  let reservation = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      5,
      ProducerSequenceProgress::default(),
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
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
      ProducerSequenceProgress::default(),
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
