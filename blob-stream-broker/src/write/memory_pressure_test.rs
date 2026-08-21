#![allow(clippy::unwrap_used)]

use super::{MemoryPressureController, MemoryPressureSample, MemoryPressureSampler};
use anyhow::{Result, anyhow};
use bd_server_stats::stats::Collector;
use bd_server_stats::test::util::stats::Helper;
use bd_shutdown::ComponentShutdownTrigger;
use parking_lot::Mutex;
use prometheus::labels;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

struct TestMemoryPressureSampler {
  samples: Mutex<VecDeque<Result<MemoryPressureSample>>>,
  fallback: MemoryPressureSample,
  sample_count: AtomicUsize,
}

impl TestMemoryPressureSampler {
  fn new(
    fallback: MemoryPressureSample,
    samples: impl IntoIterator<Item = Result<MemoryPressureSample>>,
  ) -> Self {
    Self {
      samples: Mutex::new(samples.into_iter().collect()),
      fallback,
      sample_count: AtomicUsize::new(0),
    }
  }

  fn sample_count(&self) -> usize {
    self.sample_count.load(Ordering::Relaxed)
  }
}

impl MemoryPressureSampler for TestMemoryPressureSampler {
  fn sample(&self) -> Result<MemoryPressureSample> {
    self.sample_count.fetch_add(1, Ordering::Relaxed);
    self.samples.lock().pop_front().unwrap_or(Ok(self.fallback))
  }
}

fn sample(allocated_bytes: u64, limit_bytes: u64) -> MemoryPressureSample {
  MemoryPressureSample {
    allocated_bytes,
    limit_bytes,
  }
}

fn controller(
  sampler: Arc<dyn MemoryPressureSampler>,
  collector: &Collector,
) -> Arc<MemoryPressureController> {
  MemoryPressureController::new_for_test(sampler, &collector.scope("memory_pressure_test"))
}

#[test]
fn disabled_controller_rejects_cache_reservations() {
  let collector = Collector::default();
  let controller = MemoryPressureController::disabled(&collector.scope("memory_pressure_test"));

  assert!(!controller.is_overloaded());
  assert_eq!(controller.cache_headroom_bytes(), None);
  assert!(controller.try_reserve_cache_bytes(1).is_none());
  assert_eq!(controller.cache_reservations(), 0);
}

#[test]
fn polling_updates_overload_state_and_metrics() {
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let controller = controller(
    Arc::new(TestMemoryPressureSampler::new(
      sample(7_500, 10_000),
      [
        Ok(sample(7_999, 10_000)),
        Ok(sample(8_000, 10_000)),
        Ok(sample(9_000, 10_000)),
        Ok(sample(7_500, 10_000)),
      ],
    )),
    &collector,
  );

  controller.poll_once();
  assert!(!controller.is_overloaded());
  controller.poll_once();
  assert!(controller.is_overloaded());
  controller.poll_once();
  assert!(controller.is_overloaded());
  controller.poll_once();
  assert!(!controller.is_overloaded());

  metrics.assert_gauge_eq(
    75,
    "memory_pressure_test:memory_pressure:utilization_percent",
    &labels!(),
  );
  metrics.assert_gauge_eq(
    0,
    "memory_pressure_test:memory_pressure:overloaded",
    &labels!(),
  );
  metrics.assert_counter_eq(
    2,
    "memory_pressure_test:memory_pressure:transitions_total",
    &labels!(),
  );
}

#[test]
fn sampling_failure_retains_the_last_admission_state() {
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let controller = controller(
    Arc::new(TestMemoryPressureSampler::new(
      sample(7_000, 10_000),
      [
        Ok(sample(7_000, 10_000)),
        Err(anyhow!("synthetic sampler failure")),
      ],
    )),
    &collector,
  );

  controller.poll_once();
  assert_eq!(controller.cache_headroom_bytes(), Some(1_000));
  controller.poll_once();

  assert!(!controller.is_overloaded());
  assert_eq!(controller.cache_headroom_bytes(), Some(1_000));
  metrics.assert_counter_eq(
    1,
    "memory_pressure_test:memory_pressure:sampling_failures_total",
    &labels!(),
  );
}

#[test]
fn cache_reservations_consume_headroom_and_release_on_drop() {
  let collector = Collector::default();
  let controller = controller(
    Arc::new(TestMemoryPressureSampler::new(sample(7_000, 10_000), [])),
    &collector,
  );

  controller.poll_once();
  assert_eq!(controller.cache_headroom_bytes(), Some(1_000));
  let reservation = controller
    .try_reserve_cache_bytes(600)
    .expect("headroom admits reservation");
  assert_eq!(controller.cache_reservations(), 600);
  assert_eq!(controller.cache_headroom_bytes(), Some(400));
  assert!(controller.try_reserve_cache_bytes(401).is_none());

  drop(reservation);
  assert_eq!(controller.cache_reservations(), 0);
  assert_eq!(controller.cache_headroom_bytes(), Some(1_000));
}

#[test]
fn exact_threshold_reservation_is_admitted() {
  let collector = Collector::default();
  let controller = controller(
    Arc::new(TestMemoryPressureSampler::new(sample(7_000, 10_000), [])),
    &collector,
  );

  let reservation = controller
    .try_reserve_cache_bytes(1_000)
    .expect("reservation exactly at threshold is admitted");
  assert_eq!(controller.cache_headroom_bytes(), Some(0));
  assert!(controller.try_reserve_cache_bytes(1).is_none());
  drop(reservation);
}

#[test]
fn overload_rejects_reservations_until_a_recovery_sample() {
  let collector = Collector::default();
  let controller = controller(
    Arc::new(TestMemoryPressureSampler::new(
      sample(7_000, 10_000),
      [
        Ok(sample(8_000, 10_000)),
        Ok(sample(8_000, 10_000)),
        Ok(sample(7_000, 10_000)),
        Ok(sample(7_000, 10_000)),
      ],
    )),
    &collector,
  );

  controller.poll_once();
  assert!(controller.is_overloaded());
  assert!(controller.try_reserve_cache_bytes(1).is_none());

  controller.poll_once();
  assert!(!controller.is_overloaded());
  assert!(controller.try_reserve_cache_bytes(1_000).is_some());
}

#[test]
fn concurrent_reservations_never_exceed_headroom() {
  let collector = Collector::default();
  let controller = controller(
    Arc::new(TestMemoryPressureSampler::new(sample(0, 10_000), [])),
    &collector,
  );
  controller.poll_once();
  let reservations_ready = Arc::new(Barrier::new(17));
  let release_reservations = Arc::new(Barrier::new(17));
  let mut handles = Vec::with_capacity(16);
  for _ in 0 .. 16 {
    let controller = controller.clone();
    let reservations_ready = Arc::clone(&reservations_ready);
    let release_reservations = Arc::clone(&release_reservations);
    handles.push(std::thread::spawn(move || {
      let reservation = controller.try_reserve_cache_bytes(1_000);
      reservations_ready.wait();
      release_reservations.wait();
      reservation.is_some()
    }));
  }

  reservations_ready.wait();
  assert_eq!(controller.cache_reservations(), 8_000);
  assert!(controller.try_reserve_cache_bytes(1).is_none());
  release_reservations.wait();
  let admitted = handles
    .into_iter()
    .filter_map(|handle| handle.join().ok())
    .filter(|admitted| *admitted)
    .count();
  assert_eq!(admitted, 8);
  assert_eq!(controller.cache_reservations(), 0);
}

#[tokio::test(start_paused = true)]
async fn poller_samples_until_shutdown() {
  let collector = Collector::default();
  let sampler = Arc::new(TestMemoryPressureSampler::new(sample(8_000, 10_000), []));
  let controller = controller(sampler.clone(), &collector);
  let shutdown_trigger = ComponentShutdownTrigger::default();
  controller.spawn_poller(&shutdown_trigger.make_handle());

  tokio::task::yield_now().await;
  tokio::time::advance(super::POLL_INTERVAL).await;
  tokio::task::yield_now().await;
  assert!(controller.is_overloaded());
  assert_eq!(sampler.sample_count(), 1);

  shutdown_trigger.shutdown().await;
  tokio::time::advance(super::POLL_INTERVAL * 2).await;
  tokio::task::yield_now().await;
  assert_eq!(sampler.sample_count(), 1);
}
