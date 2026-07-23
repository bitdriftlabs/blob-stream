#[cfg(test)]
#[path = "./event_log_test.rs"]
mod tests;

use anyhow::{Result, anyhow};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

//
// TestEvent
//

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestEvent {
  pub sequence: u64,
  pub category: String,
  pub operation: String,
  pub key: Option<String>,
  pub status: String,
  pub detail: Option<String>,
}

//
// TestEventMatcher
//

#[derive(Clone, Debug, Default)]
pub struct TestEventMatcher {
  pub category: Option<String>,
  pub operation: Option<String>,
  pub key_contains: Option<String>,
  pub status: Option<String>,
}

impl TestEventMatcher {
  #[must_use]
  pub fn matches(&self, event: &TestEvent) -> bool {
    if let Some(category) = &self.category
      && &event.category != category
    {
      return false;
    }
    if let Some(operation) = &self.operation
      && &event.operation != operation
    {
      return false;
    }
    if let Some(key_contains) = &self.key_contains {
      let Some(key) = &event.key else {
        return false;
      };
      if !key.contains(key_contains) {
        return false;
      }
    }
    if let Some(status) = &self.status
      && &event.status != status
    {
      return false;
    }

    true
  }
}

//
// TestEventLog
//

#[derive(Clone, Default)]
pub struct TestEventLog {
  inner: Arc<Mutex<TestEventLogState>>,
  recorded: Arc<Notify>,
}

//
// TestEventLogState
//

#[derive(Default)]
struct TestEventLogState {
  next_sequence: u64,
  events: Vec<TestEvent>,
}

//
// TestEventLog
//

impl TestEventLog {
  pub async fn record(
    &self,
    category: impl Into<String>,
    operation: impl Into<String>,
    key: Option<String>,
    status: impl Into<String>,
    detail: Option<String>,
  ) {
    let mut guard = self.inner.lock().await;
    let sequence = guard.next_sequence;
    guard.next_sequence = guard.next_sequence.saturating_add(1);
    guard.events.push(TestEvent {
      sequence,
      category: category.into(),
      operation: operation.into(),
      key,
      status: status.into(),
      detail,
    });
    drop(guard);
    self.recorded.notify_waiters();
  }

  pub async fn snapshot(&self) -> Vec<TestEvent> {
    self.inner.lock().await.events.clone()
  }

  pub async fn wait_for_event(
    &self,
    matcher: &TestEventMatcher,
    timeout_duration: Duration,
  ) -> Result<TestEvent> {
    self
      .wait_for_event_after(matcher, None, timeout_duration)
      .await
  }

  pub async fn wait_for_event_after(
    &self,
    matcher: &TestEventMatcher,
    after_sequence: Option<u64>,
    timeout_duration: Duration,
  ) -> Result<TestEvent> {
    tokio::time::timeout(timeout_duration, async {
      loop {
        // Register the waiter before inspecting the event list so a record between the inspection
        // and await cannot be missed.
        let notified = self.recorded.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let maybe_event = {
          let guard = self.inner.lock().await;
          guard
            .events
            .iter()
            .find(|event| {
              after_sequence.is_none_or(|sequence| event.sequence > sequence)
                && matcher.matches(event)
            })
            .cloned()
        };

        if let Some(event) = maybe_event {
          return event;
        }

        notified.await;
      }
    })
    .await
    .map_err(|_| {
      anyhow!(
        "timed out waiting for event after sequence {after_sequence:?}: category={:?}, \
         operation={:?}, key_contains={:?}, status={:?}",
        matcher.category,
        matcher.operation,
        matcher.key_contains,
        matcher.status
      )
    })
  }
}
