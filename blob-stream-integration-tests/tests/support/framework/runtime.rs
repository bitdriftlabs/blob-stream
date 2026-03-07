use async_trait::async_trait;
use std::time::Duration;
use tokio::time::sleep as tokio_sleep;

#[async_trait]
pub trait TestRuntime: Send + Sync {
  fn now_unix_millis(&self) -> i64;
  async fn sleep(&self, duration: Duration);
}

//
// TokioTestRuntime
//

#[derive(Default)]
pub struct TokioTestRuntime;

#[async_trait]
impl TestRuntime for TokioTestRuntime {
  fn now_unix_millis(&self) -> i64 {
    i64::try_from(
      std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is before unix epoch")
        .as_millis(),
    )
    .expect("unix millis exceeds i64")
  }

  async fn sleep(&self, duration: Duration) {
    tokio_sleep(duration).await;
  }
}

//
// DeterministicTestRuntime
//

#[allow(dead_code)]
pub struct DeterministicTestRuntime;

impl DeterministicTestRuntime {
  #[allow(dead_code)]
  pub fn new() -> Self {
    Self
  }
}

#[async_trait]
impl TestRuntime for DeterministicTestRuntime {
  fn now_unix_millis(&self) -> i64 {
    panic!("DeterministicTestRuntime::now_unix_millis is not wired in Phase A")
  }

  async fn sleep(&self, _duration: Duration) {
    panic!("DeterministicTestRuntime::sleep is not wired in Phase A")
  }
}

pub fn now_unix_millis() -> i64 {
  default_test_runtime().now_unix_millis()
}

pub fn now_unix_seconds() -> i64 {
  now_unix_millis() / 1_000
}

fn default_test_runtime() -> &'static TokioTestRuntime {
  static TOKIO_RUNTIME: TokioTestRuntime = TokioTestRuntime;
  &TOKIO_RUNTIME
}

pub async fn runtime_sleep(duration: Duration) {
  default_test_runtime().sleep(duration).await;
}
