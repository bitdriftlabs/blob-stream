use super::{TestEventLog, TestEventMatcher};
use anyhow::{Result, anyhow};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn wait_for_event_wakes_when_matching_event_is_recorded() -> Result<()> {
  let event_log = TestEventLog::default();
  let waiter_event_log = event_log.clone();
  let waiter = tokio::spawn(async move {
    waiter_event_log
      .wait_for_event(
        &TestEventMatcher {
          category: Some("broker".to_string()),
          operation: Some("flush".to_string()),
          key_contains: None,
          status: Some("completed".to_string()),
        },
        Duration::from_secs(1),
      )
      .await
  });

  tokio::task::yield_now().await;
  event_log
    .record("broker", "flush", None, "completed", None)
    .await;

  let event = timeout(Duration::from_secs(1), waiter)
    .await
    .map_err(|_| anyhow!("event-log waiter did not finish"))?
    .map_err(|error| anyhow!("event-log waiter panicked: {error}"))??;
  assert_eq!(event.sequence, 0);
  Ok(())
}

#[tokio::test]
async fn wait_for_event_after_returns_a_later_matching_event() -> Result<()> {
  let event_log = TestEventLog::default();
  event_log
    .record("broker", "flush", None, "completed", None)
    .await;
  event_log
    .record("broker", "flush", None, "completed", None)
    .await;

  let event = event_log
    .wait_for_event_after(
      &TestEventMatcher {
        category: Some("broker".to_string()),
        operation: Some("flush".to_string()),
        key_contains: None,
        status: Some("completed".to_string()),
      },
      Some(0),
      Duration::from_secs(1),
    )
    .await?;
  assert_eq!(event.sequence, 1);
  Ok(())
}
