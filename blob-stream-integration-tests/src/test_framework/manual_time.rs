#[cfg(test)]
#[path = "./manual_time_test.rs"]
mod tests;

use async_trait::async_trait;
use blob_stream_producer::ProducerRetryClock;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;

//
// ManualProducerRetryClock
//

/// A retry clock that advances only when the test advances its earliest registered sleep.
#[derive(Clone)]
pub struct ManualProducerRetryClock {
  now: watch::Sender<Instant>,
  sleep_deadlines: Arc<Mutex<BTreeMap<Instant, usize>>>,
  sleep_registered: Arc<Notify>,
}

struct SleepRegistration {
  sleep_deadlines: Arc<Mutex<BTreeMap<Instant, usize>>>,
  deadline: Instant,
}

impl Drop for SleepRegistration {
  fn drop(&mut self) {
    let mut sleep_deadlines = self.sleep_deadlines.lock();
    let Some(count) = sleep_deadlines.get_mut(&self.deadline) else {
      return;
    };
    if *count == 1 {
      sleep_deadlines.remove(&self.deadline);
    } else {
      *count -= 1;
    }
  }
}

impl ManualProducerRetryClock {
  #[must_use]
  pub fn new(now: Instant) -> Self {
    let (now, _receiver) = watch::channel(now);
    Self {
      now,
      sleep_deadlines: Arc::new(Mutex::new(BTreeMap::new())),
      sleep_registered: Arc::new(Notify::new()),
    }
  }

  pub async fn wait_until_sleeping(&self) {
    self.wait_until_sleepers(1).await;
  }

  pub async fn wait_until_sleepers(&self, expected_sleepers: usize) {
    loop {
      let notified = self.sleep_registered.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      let registered_sleepers = self.sleep_deadlines.lock().values().sum::<usize>();
      if registered_sleepers >= expected_sleepers {
        return;
      }
      notified.await;
    }
  }

  pub fn advance_to_next_sleep(&self) -> bool {
    let Some(deadline) = self.sleep_deadlines.lock().keys().next().copied() else {
      return false;
    };
    self.now.send_replace(deadline);
    true
  }

  pub fn advance(&self, duration: StdDuration) {
    self.now.send_modify(|now| *now += duration);
  }
}

#[async_trait]
impl ProducerRetryClock for ManualProducerRetryClock {
  fn now(&self) -> Instant {
    *self.now.borrow()
  }

  async fn sleep(&self, duration: StdDuration) {
    let deadline = self.now() + duration;
    {
      let mut sleep_deadlines = self.sleep_deadlines.lock();
      *sleep_deadlines.entry(deadline).or_default() += 1;
    }
    let _registration = SleepRegistration {
      sleep_deadlines: Arc::clone(&self.sleep_deadlines),
      deadline,
    };
    self.sleep_registered.notify_waiters();

    let mut updates = self.now.subscribe();
    while *updates.borrow_and_update() < deadline {
      if updates.changed().await.is_err() {
        return;
      }
    }
  }
}
