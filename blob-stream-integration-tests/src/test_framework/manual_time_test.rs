use super::ManualProducerRetryClock;
use blob_stream_producer::ProducerRetryClock;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test]
async fn advances_concurrent_sleeps_in_deadline_order() {
  let clock = ManualProducerRetryClock::new(Instant::now());
  let long_sleep = tokio::spawn({
    let clock = clock.clone();
    async move { clock.sleep(Duration::from_secs(10)).await }
  });
  let short_sleep = tokio::spawn({
    let clock = clock.clone();
    async move { clock.sleep(Duration::from_secs(5)).await }
  });

  clock.wait_until_sleepers(2).await;
  assert!(clock.advance_to_next_sleep());
  short_sleep.await.unwrap();
  assert!(!long_sleep.is_finished());

  assert!(clock.advance_to_next_sleep());
  long_sleep.await.unwrap();
  assert!(!clock.advance_to_next_sleep());
}
