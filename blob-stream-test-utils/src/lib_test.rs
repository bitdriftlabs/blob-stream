use super::ManualTimeProvider;
use bd_time::TimeProvider;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

#[tokio::test]
async fn sleep_waits_for_explicit_logical_time_advance() {
  let time_provider = Arc::new(ManualTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let sleeper = {
    let time_provider = Arc::clone(&time_provider);
    tokio::spawn(async move { time_provider.sleep(Duration::seconds(10)).await })
  };

  time_provider.wait_until_sleeping(1).await;
  assert!(!sleeper.is_finished());
  assert_eq!(time_provider.now(), OffsetDateTime::UNIX_EPOCH);

  time_provider.advance(Duration::seconds(9));
  tokio::task::yield_now().await;
  assert!(!sleeper.is_finished());

  time_provider.advance(Duration::seconds(1));
  sleeper.await.expect("manual clock sleeper should complete");
  assert_eq!(
    time_provider.now(),
    OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)
  );
}

#[tokio::test]
async fn sleepers_complete_at_their_individual_deadlines() {
  let time_provider = Arc::new(ManualTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let first = {
    let time_provider = Arc::clone(&time_provider);
    tokio::spawn(async move { time_provider.sleep(Duration::seconds(5)).await })
  };
  let second = {
    let time_provider = Arc::clone(&time_provider);
    tokio::spawn(async move { time_provider.sleep(Duration::seconds(10)).await })
  };

  time_provider.wait_until_sleeping(2).await;
  time_provider.advance(Duration::seconds(5));
  first
    .await
    .expect("first sleeper should complete at its deadline");
  assert!(!second.is_finished());

  time_provider.advance(Duration::seconds(5));
  second
    .await
    .expect("second sleeper should complete at its deadline");
}

#[tokio::test]
async fn set_time_wakes_registered_sleepers() {
  let time_provider = Arc::new(ManualTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let sleeper = {
    let time_provider = Arc::clone(&time_provider);
    tokio::spawn(async move { time_provider.sleep(Duration::seconds(10)).await })
  };

  time_provider.wait_until_sleeping(1).await;
  time_provider.set_time(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10));
  sleeper
    .await
    .expect("sleeper should complete after set_time reaches its deadline");
}

#[tokio::test]
async fn reports_each_new_sleep_registration() {
  let time_provider = Arc::new(ManualTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let initial_registration_count = time_provider.sleep_registration_count();
  let sleeper = {
    let time_provider = Arc::clone(&time_provider);
    tokio::spawn(async move {
      time_provider.sleep(Duration::seconds(1)).await;
      time_provider.sleep(Duration::seconds(1)).await;
    })
  };

  let first_registration_count = time_provider
    .wait_for_sleep_registration_after(initial_registration_count)
    .await;
  time_provider.advance(Duration::seconds(1));
  let second_registration_count = time_provider
    .wait_for_sleep_registration_after(first_registration_count)
    .await;
  assert_eq!(second_registration_count, first_registration_count + 1);

  time_provider.advance(Duration::seconds(1));
  sleeper
    .await
    .expect("manual clock sleeper should complete after the second advance");
}
