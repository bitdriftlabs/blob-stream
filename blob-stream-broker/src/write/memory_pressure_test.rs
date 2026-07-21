use super::{MemoryPressureController, MemoryPressureSource};
use anyhow::{Result, anyhow};
use bd_server_stats::stats::Collector;
use bd_shutdown::ComponentShutdownTrigger;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct TestMemoryPressureSource {
  samples: Mutex<VecDeque<Result<u32>>>,
  sample_count: AtomicUsize,
}

impl TestMemoryPressureSource {
  fn new(samples: impl IntoIterator<Item = Result<u32>>) -> Self {
    Self {
      samples: Mutex::new(samples.into_iter().collect()),
      sample_count: AtomicUsize::new(0),
    }
  }

  fn sample_count(&self) -> usize {
    self.sample_count.load(Ordering::Relaxed)
  }
}

impl MemoryPressureSource for TestMemoryPressureSource {
  fn utilization_permyriad(&self) -> Result<u32> {
    self.sample_count.fetch_add(1, Ordering::Relaxed);
    self
      .samples
      .lock()
      .pop_front()
      .unwrap_or_else(|| Err(anyhow!("test pressure samples exhausted")))
  }
}

#[test]
fn poll_updates_overload_state_and_metrics() -> Result<()> {
  let collector = Collector::default();
  let controller = MemoryPressureController::new_for_test(
    Arc::new(TestMemoryPressureSource::new([
      Ok(7_999),
      Ok(8_000),
      Ok(9_000),
      Ok(7_500),
    ])),
    &collector.scope("memory_pressure_test"),
  );

  controller.poll_once();
  assert!(!controller.is_overloaded());
  controller.poll_once();
  assert!(controller.is_overloaded());
  controller.poll_once();
  assert!(controller.is_overloaded());
  controller.poll_once();
  assert!(!controller.is_overloaded());

  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(metrics.contains("memory_pressure_test:memory_pressure:utilization_percent 75"));
  assert!(metrics.contains("memory_pressure_test:memory_pressure:overloaded 0"));
  assert!(metrics.contains("memory_pressure_test:memory_pressure:transitions_total 2"));
  Ok(())
}

#[test]
fn poll_records_sampling_failure_and_continues() -> Result<()> {
  let collector = Collector::default();
  let controller = MemoryPressureController::new_for_test(
    Arc::new(TestMemoryPressureSource::new([
      Err(anyhow!("synthetic sampler failure")),
      Ok(8_000),
    ])),
    &collector.scope("memory_pressure_test"),
  );

  controller.poll_once();
  assert!(!controller.is_overloaded());
  controller.poll_once();
  assert!(controller.is_overloaded());

  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(metrics.contains("memory_pressure_test:memory_pressure:sampling_failures_total 1"));
  assert!(metrics.contains("memory_pressure_test:memory_pressure:overloaded 1"));
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn poller_samples_source_and_stops_on_shutdown() {
  let collector = Collector::default();
  let source = Arc::new(TestMemoryPressureSource::new([Ok(8_000)]));
  let controller = MemoryPressureController::new_for_test(
    source.clone(),
    &collector.scope("memory_pressure_test"),
  );
  let shutdown_trigger = ComponentShutdownTrigger::default();
  controller.spawn_poller(&shutdown_trigger.make_handle());

  tokio::task::yield_now().await;
  tokio::time::advance(super::POLL_INTERVAL).await;
  tokio::task::yield_now().await;
  assert!(controller.is_overloaded());
  assert_eq!(source.sample_count(), 1);

  shutdown_trigger.shutdown().await;
  tokio::time::advance(super::POLL_INTERVAL * 2).await;
  tokio::task::yield_now().await;
  assert_eq!(source.sample_count(), 1);
}
