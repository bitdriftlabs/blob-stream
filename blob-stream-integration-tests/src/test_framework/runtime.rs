use async_trait::async_trait;
use blob_stream_types::{
  now_unix_millis as shared_now_unix_millis,
  now_unix_seconds as shared_now_unix_seconds,
};
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
    shared_now_unix_millis()
  }

  async fn sleep(&self, duration: Duration) {
    tokio_sleep(duration).await;
  }
}

pub fn now_unix_millis() -> i64 {
  default_test_runtime().now_unix_millis()
}

pub fn now_unix_seconds() -> i64 {
  shared_now_unix_seconds()
}

fn default_test_runtime() -> &'static TokioTestRuntime {
  static TOKIO_RUNTIME: TokioTestRuntime = TokioTestRuntime;
  &TOKIO_RUNTIME
}

pub async fn runtime_sleep(duration: Duration) {
  default_test_runtime().sleep(duration).await;
}
