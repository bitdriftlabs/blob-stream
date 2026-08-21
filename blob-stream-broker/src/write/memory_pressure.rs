//! Cgroup-aware broker memory admission control.
//!
//! The controller owns common overload and cache-reservation state. Platform-specific code only
//! supplies sampled jemalloc allocation and cgroup limits.

#[cfg(test)]
#[path = "./memory_pressure_test.rs"]
mod tests;

use anyhow::Result;
#[cfg(target_os = "linux")]
use anyhow::anyhow;
use bd_log_util::warn_every;
use bd_server_stats::stats::Scope;
use bd_shutdown::ComponentShutdownTriggerHandle;
#[cfg(target_os = "linux")]
use cgroups_rs::fs::cgroup::{Cgroup, existing_path, get_cgroups_relative_paths};
#[cfg(target_os = "linux")]
use cgroups_rs::fs::hierarchies;
#[cfg(target_os = "linux")]
use cgroups_rs::fs::memory::MemController;
use log::{debug, info};
use parking_lot::Mutex;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(any(target_os = "linux", test))]
use std::time::Duration;
use time::ext::NumericalDuration;

#[cfg(any(target_os = "linux", test))]
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const OVERLOADED_ON_PERMYRIAD: u32 = 8_000;
const PERMYRIAD: u32 = 10_000;
#[cfg(target_os = "linux")]
const MAX_REASONABLE_CGROUP_LIMIT_BYTES: u64 = 1_u64 << 60;

//
// MemoryPressureMetrics
//

#[derive(Clone)]
struct MemoryPressureMetrics {
  utilization_percent: prometheus::IntGauge,
  overloaded: prometheus::IntGauge,
  transitions_total: prometheus::IntCounter,
  sampling_failures_total: prometheus::IntCounter,
}

impl MemoryPressureMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("memory_pressure");
    Self {
      utilization_percent: scope.gauge("utilization_percent"),
      overloaded: scope.gauge("overloaded"),
      transitions_total: scope.counter("transitions_total"),
      sampling_failures_total: scope.counter("sampling_failures_total"),
    }
  }
}

//
// MemoryPressureSampler
//

/// Supplies a cgroup-normalized allocation measurement for common pressure admission logic.
pub trait MemoryPressureSampler: Send + Sync {
  fn sample(&self) -> Result<MemoryPressureSample>;
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryPressureSample {
  pub allocated_bytes: u64,
  pub limit_bytes: u64,
}

//
// LinuxMemoryPressureSampler
//

#[cfg(target_os = "linux")]
struct LinuxMemoryPressureSampler {
  epoch_mib: tikv_jemalloc_ctl::epoch_mib,
  allocated_mib: tikv_jemalloc_ctl::stats::allocated_mib,
  cgroup_memory_limit_bytes: u64,
}

#[cfg(target_os = "linux")]
impl LinuxMemoryPressureSampler {
  fn new() -> Result<Self> {
    let epoch_mib = tikv_jemalloc_ctl::epoch::mib()
      .map_err(|error| anyhow!("failed creating jemalloc epoch mib: {error}"))?;
    let allocated_mib = tikv_jemalloc_ctl::stats::allocated::mib()
      .map_err(|error| anyhow!("failed creating jemalloc allocated mib: {error}"))?;
    Ok(Self {
      epoch_mib,
      allocated_mib,
      cgroup_memory_limit_bytes: read_cgroup_memory_limit_bytes()?,
    })
  }
}

#[cfg(target_os = "linux")]
impl MemoryPressureSampler for LinuxMemoryPressureSampler {
  fn sample(&self) -> Result<MemoryPressureSample> {
    self
      .epoch_mib
      .advance()
      .map_err(|error| anyhow!("failed advancing jemalloc epoch: {error}"))?;
    let allocated_bytes = self
      .allocated_mib
      .read()
      .map_err(|error| anyhow!("failed reading jemalloc allocated bytes: {error}"))?;
    let allocated_bytes = u64::try_from(allocated_bytes)
      .map_err(|error| anyhow!("invalid jemalloc allocated value: {error}"))?;
    Ok(MemoryPressureSample {
      allocated_bytes,
      limit_bytes: self.cgroup_memory_limit_bytes,
    })
  }
}

//
// MemoryPressureController
//

pub struct MemoryPressureController {
  sampler: Option<Arc<dyn MemoryPressureSampler>>,
  overloaded: AtomicBool,
  allocated_bytes: AtomicU64,
  cgroup_limit_bytes: AtomicU64,
  cache_reservations: AtomicU64,
  overload_handlers: Mutex<Vec<Arc<dyn Fn() + Send + Sync>>>,
  metrics: MemoryPressureMetrics,
}

impl fmt::Debug for MemoryPressureController {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter
      .debug_struct("MemoryPressureController")
      .field("overloaded", &self.is_overloaded())
      .finish()
  }
}

impl MemoryPressureController {
  #[must_use]
  pub fn new(
    shutdown_trigger_handle: &ComponentShutdownTriggerHandle,
    metrics_scope: &Scope,
  ) -> Arc<Self> {
    #[cfg(target_os = "linux")]
    {
      let sampler = match LinuxMemoryPressureSampler::new() {
        Ok(sampler) => sampler,
        Err(error) => {
          log::warn!("broker memory-pressure admission disabled: {error}");
          return Self::disabled(metrics_scope);
        },
      };
      info!(
        "broker memory-pressure admission enabled: cgroup_memory_limit_bytes={}, \
         overloaded_on_permyriad={OVERLOADED_ON_PERMYRIAD}",
        sampler.cgroup_memory_limit_bytes,
      );
      let controller = Self::with_sampler(Arc::new(sampler), metrics_scope);
      controller.spawn_poller(shutdown_trigger_handle);
      controller
    }

    #[cfg(not(target_os = "linux"))]
    {
      debug!("broker memory-pressure admission disabled: cgroup limits require Linux");
      let _ = shutdown_trigger_handle;
      Self::disabled(metrics_scope)
    }
  }

  #[must_use]
  /// Build a controller from an explicit cgroup-normalized sampler.
  pub fn with_sampler(sampler: Arc<dyn MemoryPressureSampler>, metrics_scope: &Scope) -> Arc<Self> {
    Arc::new(Self {
      sampler: Some(sampler),
      overloaded: AtomicBool::new(false),
      allocated_bytes: AtomicU64::new(0),
      cgroup_limit_bytes: AtomicU64::new(0),
      cache_reservations: AtomicU64::new(0),
      overload_handlers: Mutex::new(Vec::new()),
      metrics: MemoryPressureMetrics::new(metrics_scope),
    })
  }

  fn disabled(metrics_scope: &Scope) -> Arc<Self> {
    Arc::new(Self {
      sampler: None,
      overloaded: AtomicBool::new(false),
      allocated_bytes: AtomicU64::new(0),
      cgroup_limit_bytes: AtomicU64::new(0),
      cache_reservations: AtomicU64::new(0),
      overload_handlers: Mutex::new(Vec::new()),
      metrics: MemoryPressureMetrics::new(metrics_scope),
    })
  }

  #[cfg(test)]
  pub(crate) fn new_for_test(
    sampler: Arc<dyn MemoryPressureSampler>,
    metrics_scope: &Scope,
  ) -> Arc<Self> {
    Self::with_sampler(sampler, metrics_scope)
  }

  #[cfg(test)]
  pub(crate) fn new_for_test_with_sample(
    sample: MemoryPressureSample,
    metrics_scope: &Scope,
  ) -> Arc<Self> {
    Self::with_sampler(Arc::new(StaticMemoryPressureSampler(sample)), metrics_scope)
  }

  #[must_use]
  pub fn is_overloaded(&self) -> bool {
    self.overloaded.load(Ordering::Relaxed)
  }

  /// Reserve cache bytes against the most recently sampled cgroup headroom until the guard drops.
  #[must_use]
  pub fn try_reserve_cache_bytes(
    self: &Arc<Self>,
    bytes: u64,
  ) -> Option<MemoryPressureReservation> {
    self.poll_once();
    if self.sampler.is_none() || self.is_overloaded() {
      return None;
    }

    let allocated = self.allocated_bytes.load(Ordering::Relaxed);
    let threshold = self.overload_threshold_bytes();
    loop {
      let reserved = self.cache_reservations.load(Ordering::Relaxed);
      if allocated.saturating_add(reserved).saturating_add(bytes) > threshold {
        return None;
      }
      if self
        .cache_reservations
        .compare_exchange_weak(
          reserved,
          reserved.saturating_add(bytes),
          Ordering::Relaxed,
          Ordering::Relaxed,
        )
        .is_ok()
      {
        return Some(MemoryPressureReservation {
          controller: Arc::clone(self),
          bytes,
        });
      }
    }
  }

  #[must_use]
  pub fn cache_reservations(&self) -> u64 {
    self.cache_reservations.load(Ordering::Relaxed)
  }

  #[must_use]
  pub fn cache_headroom_bytes(&self) -> Option<u64> {
    self.sampler.as_ref()?;
    let allocated = self.allocated_bytes.load(Ordering::Relaxed);
    Some(
      self
        .overload_threshold_bytes()
        .saturating_sub(allocated.saturating_add(self.cache_reservations())),
    )
  }

  /// Register maintenance to run each time memory pressure enters overload.
  pub fn register_overload_handler(&self, handler: &Arc<dyn Fn() + Send + Sync>) {
    let run_now = {
      let mut overload_handlers = self.overload_handlers.lock();
      let run_now = self.is_overloaded();
      overload_handlers.push(Arc::clone(handler));
      run_now
    };
    if run_now {
      handler();
    }
  }

  fn overload_threshold_bytes(&self) -> u64 {
    let limit = self.cgroup_limit_bytes.load(Ordering::Relaxed);
    u64::try_from(
      u128::from(limit).saturating_mul(u128::from(OVERLOADED_ON_PERMYRIAD)) / u128::from(PERMYRIAD),
    )
    .unwrap_or(u64::MAX)
  }

  #[cfg(any(target_os = "linux", test))]
  fn spawn_poller(self: &Arc<Self>, shutdown_trigger_handle: &ComponentShutdownTriggerHandle) {
    let controller = Arc::clone(self);
    let mut shutdown = shutdown_trigger_handle.clone().make_shutdown();
    tokio::spawn(async move {
      loop {
        tokio::select! {
          () = shutdown.cancelled() => {
            debug!("broker memory-pressure poller shutdown");
            return;
          },
          () = tokio::time::sleep(POLL_INTERVAL) => controller.poll_once(),
        }
      }
    });
  }

  fn poll_once(&self) {
    let Some(sampler) = self.sampler.as_ref() else {
      return;
    };
    let sample = match sampler.sample() {
      Ok(sample) => sample,
      Err(error) => {
        self.metrics.sampling_failures_total.inc();
        warn_every!(
          30.seconds(),
          "failed sampling broker memory pressure: {error}"
        );
        return;
      },
    };

    self
      .allocated_bytes
      .store(sample.allocated_bytes, Ordering::Relaxed);
    self
      .cgroup_limit_bytes
      .store(sample.limit_bytes, Ordering::Relaxed);
    let utilization_permyriad =
      utilization_to_permyriad(sample.allocated_bytes, sample.limit_bytes);
    self
      .metrics
      .utilization_percent
      .set(i64::from(utilization_permyriad / 100));

    let overloaded = utilization_permyriad >= OVERLOADED_ON_PERMYRIAD;
    self.metrics.overloaded.set(i64::from(overloaded));
    let previously_overloaded = self.overloaded.swap(overloaded, Ordering::Relaxed);
    if previously_overloaded != overloaded {
      self.metrics.transitions_total.inc();
      info!("broker memory-pressure overload transition {previously_overloaded} -> {overloaded}");
    }
    if overloaded && !previously_overloaded {
      let overload_handlers = self.overload_handlers.lock().clone();
      for handler in overload_handlers {
        handler();
      }
    }
  }
}

#[cfg(test)]
struct StaticMemoryPressureSampler(MemoryPressureSample);

#[cfg(test)]
impl MemoryPressureSampler for StaticMemoryPressureSampler {
  fn sample(&self) -> Result<MemoryPressureSample> {
    Ok(self.0)
  }
}

//
// MemoryPressureReservation
//

/// Active whole-object cache reservation released when the in-flight fetch completes.
pub struct MemoryPressureReservation {
  controller: Arc<MemoryPressureController>,
  bytes: u64,
}

impl Drop for MemoryPressureReservation {
  fn drop(&mut self) {
    self
      .controller
      .cache_reservations
      .fetch_sub(self.bytes, Ordering::Relaxed);
  }
}

fn utilization_to_permyriad(allocated_bytes: u64, limit_bytes: u64) -> u32 {
  let scaled = u128::from(allocated_bytes).saturating_mul(u128::from(PERMYRIAD))
    / u128::from(limit_bytes.max(1));
  u32::try_from(scaled.min(u128::from(u32::MAX))).unwrap_or(u32::MAX)
}

#[cfg(target_os = "linux")]
fn read_cgroup_memory_limit_bytes() -> anyhow::Result<u64> {
  let process_paths = get_cgroups_relative_paths()
    .map_err(|error| anyhow!("failed to discover cgroup relative paths: {error}"))?;
  let cgroup_path = if hierarchies::is_cgroup2_unified_mode() {
    process_paths
      .get("")
      .cloned()
      .ok_or_else(|| anyhow!("failed to resolve cgroup v2 path for current process"))?
  } else {
    existing_path(process_paths)
      .map_err(|error| anyhow!("failed to map cgroup v1 paths from mountinfo: {error}"))?
      .get("memory")
      .cloned()
      .ok_or_else(|| anyhow!("failed to resolve memory controller path for cgroup v1"))?
  };

  let normalized_path = cgroup_path.trim_start_matches('/');
  let cgroup = Cgroup::load(hierarchies::auto(), normalized_path);
  let memory_controller: &MemController = cgroup
    .controller_of()
    .ok_or_else(|| anyhow!("memory controller not available for cgroup path {normalized_path}"))?;
  validate_memory_limit_bytes(memory_controller.memory_stat().limit_in_bytes)
}

#[cfg(target_os = "linux")]
fn validate_memory_limit_bytes(raw_value: i64) -> anyhow::Result<u64> {
  let limit = u64::try_from(raw_value)
    .map_err(|_| anyhow!("cgroup memory limit is negative or invalid: {raw_value}"))?;
  if limit == 0 {
    return Err(anyhow!("cgroup memory limit cannot be zero"));
  }
  if limit >= MAX_REASONABLE_CGROUP_LIMIT_BYTES {
    return Err(anyhow!("cgroup memory limit appears unlimited: {limit}"));
  }
  Ok(limit)
}
