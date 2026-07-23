//! Shared test support for Blob Stream crates.

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

use async_trait::async_trait;
use bd_time::TimeProvider;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use time::{Duration, OffsetDateTime};
use tokio::sync::{Notify, watch};

//
// ManualTimeProvider
//

/// A logical clock that changes only when the test explicitly advances it.
///
/// This differs from `bd_time::TestTimeProvider`, whose `sleep()` immediately advances its
/// logical time. That is useful for sequential foreground operations, but a recurring background
/// loop would advance time by repeatedly sleeping. Use this provider when a background component
/// owns a `TimeProvider::sleep()` future and the test must control when its logical clock moves.
#[derive(Clone)]
pub struct ManualTimeProvider {
  now: watch::Sender<OffsetDateTime>,
  active_sleeps: Arc<AtomicUsize>,
  sleep_registered: Arc<Notify>,
}

struct ManualTimeSleepRegistration {
  active_sleeps: Arc<AtomicUsize>,
}

impl ManualTimeSleepRegistration {
  fn register(active_sleeps: Arc<AtomicUsize>, sleep_registered: &Notify) -> Self {
    active_sleeps.fetch_add(1, Ordering::Release);
    sleep_registered.notify_waiters();
    Self { active_sleeps }
  }
}

impl Drop for ManualTimeSleepRegistration {
  fn drop(&mut self) {
    self.active_sleeps.fetch_sub(1, Ordering::Release);
  }
}

impl ManualTimeProvider {
  #[must_use]
  pub fn new(now: OffsetDateTime) -> Self {
    let (now, _receiver) = watch::channel(now);
    Self {
      now,
      active_sleeps: Arc::new(AtomicUsize::new(0)),
      sleep_registered: Arc::new(Notify::new()),
    }
  }

  /// Wait until the requested number of tasks have registered logical-clock sleeps.
  pub async fn wait_until_sleeping(&self, expected_sleepers: usize) {
    loop {
      let notified = self.sleep_registered.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if self.active_sleeps.load(Ordering::Acquire) >= expected_sleepers {
        return;
      }
      notified.await;
    }
  }

  pub fn advance(&self, duration: Duration) {
    self.now.send_modify(|now| *now += duration);
  }

  pub fn set_time(&self, now: OffsetDateTime) {
    self.now.send_replace(now);
  }
}

#[async_trait]
impl TimeProvider for ManualTimeProvider {
  fn now(&self) -> OffsetDateTime {
    *self.now.borrow()
  }

  async fn sleep(&self, duration: Duration) {
    let deadline = self.now() + duration;
    let _registration = ManualTimeSleepRegistration::register(
      Arc::clone(&self.active_sleeps),
      &self.sleep_registered,
    );
    let mut updates = self.now.subscribe();
    while *updates.borrow_and_update() < deadline {
      if updates.changed().await.is_err() {
        return;
      }
    }
  }
}
