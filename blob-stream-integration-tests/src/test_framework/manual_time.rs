use async_trait::async_trait;
use blob_stream_producer::ProducerRetryClock;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::{Mutex, Notify, watch};
use tokio::time::Instant;

//
// ManualProducerRetryClock
//

/// A retry clock that advances only when the test releases its currently registered sleep.
#[derive(Clone)]
pub struct ManualProducerRetryClock {
  now: watch::Sender<Instant>,
  next_sleep_deadline: Arc<Mutex<Option<Instant>>>,
  sleep_registered: Arc<Notify>,
}

impl ManualProducerRetryClock {
  #[must_use]
  pub fn new(now: Instant) -> Self {
    let (now, _receiver) = watch::channel(now);
    Self {
      now,
      next_sleep_deadline: Arc::new(Mutex::new(None)),
      sleep_registered: Arc::new(Notify::new()),
    }
  }

  pub async fn wait_until_sleeping(&self) {
    loop {
      let notified = self.sleep_registered.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if self.next_sleep_deadline.lock().await.is_some() {
        return;
      }
      notified.await;
    }
  }

  pub async fn advance_to_next_sleep(&self) -> bool {
    let Some(deadline) = self.next_sleep_deadline.lock().await.take() else {
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
    *self.next_sleep_deadline.lock().await = Some(deadline);
    self.sleep_registered.notify_waiters();

    let mut updates = self.now.subscribe();
    while *updates.borrow_and_update() < deadline {
      if updates.changed().await.is_err() {
        return;
      }
    }
  }
}
