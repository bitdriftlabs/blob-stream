//! Cgroup-aware broker memory admission control.
//!
//! The controller samples jemalloc allocation relative to the cgroup memory limit and sheds new
//! writes when utilization exceeds the admission threshold.

#[cfg(test)]
#[path = "./memory_pressure_test.rs"]
mod tests;

#[cfg(any(target_os = "linux", test))]
use anyhow::Result;
#[cfg(target_os = "linux")]
use anyhow::anyhow;
#[cfg(any(target_os = "linux", test))]
use bd_log_util::warn_every;
use bd_server_stats::stats::Scope;
use bd_shutdown::ComponentShutdownTriggerHandle;
#[cfg(target_os = "linux")]
use cgroups_rs::fs::cgroup::{Cgroup, existing_path, get_cgroups_relative_paths};
#[cfg(target_os = "linux")]
use cgroups_rs::fs::hierarchies;
#[cfg(target_os = "linux")]
use cgroups_rs::fs::memory::MemController;
use log::debug;
#[cfg(any(target_os = "linux", test))]
use log::info;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(target_os = "linux", test))]
use std::time::Duration;
#[cfg(any(target_os = "linux", test))]
use time::ext::NumericalDuration;

#[cfg(any(target_os = "linux", test))]
const POLL_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(any(target_os = "linux", test))]
const OVERLOADED_ON_PERMYRIAD: u32 = 8_000;
#[cfg(target_os = "linux")]
const PERMYRIAD: u32 = 10_000;
#[cfg(target_os = "linux")]
const MAX_REASONABLE_CGROUP_LIMIT_BYTES: u64 = 1_u64 << 60;

//
// MemoryPressureMetrics
//

#[cfg(any(target_os = "linux", test))]
#[derive(Clone)]
struct MemoryPressureMetrics {
  utilization_percent: prometheus::IntGauge,
  overloaded: prometheus::IntGauge,
  transitions_total: prometheus::IntCounter,
  sampling_failures_total: prometheus::IntCounter,
}

#[cfg(any(target_os = "linux", test))]
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
// MemoryPressureSource
//

/// Supplies a cgroup-normalized allocation measurement for pressure admission.
#[cfg(any(target_os = "linux", test))]
trait MemoryPressureSource: Send + Sync {
  fn utilization_permyriad(&self) -> Result<u32>;
}

#[cfg(target_os = "linux")]
trait MemoryUsageSampler: Send + Sync {
  fn allocated_bytes(&self) -> anyhow::Result<u64>;
}

#[cfg(target_os = "linux")]
struct JemallocMemoryUsageSampler {
  epoch_mib: tikv_jemalloc_ctl::epoch_mib,
  allocated_mib: tikv_jemalloc_ctl::stats::allocated_mib,
}

#[cfg(target_os = "linux")]
impl JemallocMemoryUsageSampler {
  fn new() -> anyhow::Result<Self> {
    let epoch_mib = tikv_jemalloc_ctl::epoch::mib()
      .map_err(|error| anyhow!("failed creating jemalloc epoch mib: {error}"))?;
    let allocated_mib = tikv_jemalloc_ctl::stats::allocated::mib()
      .map_err(|error| anyhow!("failed creating jemalloc allocated mib: {error}"))?;
    Ok(Self {
      epoch_mib,
      allocated_mib,
    })
  }
}

#[cfg(target_os = "linux")]
impl MemoryUsageSampler for JemallocMemoryUsageSampler {
  fn allocated_bytes(&self) -> anyhow::Result<u64> {
    self
      .epoch_mib
      .advance()
      .map_err(|error| anyhow!("failed advancing jemalloc epoch: {error}"))?;
    let allocated = self
      .allocated_mib
      .read()
      .map_err(|error| anyhow!("failed reading jemalloc allocated bytes: {error}"))?;
    u64::try_from(allocated).map_err(|error| anyhow!("invalid jemalloc allocated value: {error}"))
  }
}

//
// LinuxMemoryPressureSource
//

#[cfg(target_os = "linux")]
struct LinuxMemoryPressureSource {
  sampler: Arc<dyn MemoryUsageSampler>,
  cgroup_memory_limit_bytes: u64,
}

#[cfg(target_os = "linux")]
impl LinuxMemoryPressureSource {
  fn new() -> Result<Self> {
    Ok(Self {
      sampler: Arc::new(JemallocMemoryUsageSampler::new()?),
      cgroup_memory_limit_bytes: read_cgroup_memory_limit_bytes()?,
    })
  }
}

#[cfg(target_os = "linux")]
impl MemoryPressureSource for LinuxMemoryPressureSource {
  fn utilization_permyriad(&self) -> Result<u32> {
    Ok(utilization_to_permyriad(
      self.sampler.allocated_bytes()?,
      self.cgroup_memory_limit_bytes,
    ))
  }
}

//
// MemoryPressureController
//

#[derive(Clone)]
pub(super) struct MemoryPressureController {
  inner: Arc<MemoryPressureInner>,
}

impl fmt::Debug for MemoryPressureController {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter
      .debug_struct("MemoryPressureController")
      .field("overloaded", &self.is_overloaded())
      .finish()
  }
}

struct MemoryPressureInner {
  overloaded: AtomicBool,
  #[cfg(any(target_os = "linux", test))]
  metrics: MemoryPressureMetrics,
  #[cfg(any(target_os = "linux", test))]
  source: Option<Arc<dyn MemoryPressureSource>>,
}

impl MemoryPressureController {
  pub(super) fn new(
    shutdown_trigger_handle: &ComponentShutdownTriggerHandle,
    metrics_scope: &Scope,
  ) -> Self {
    #[cfg(target_os = "linux")]
    {
      let source = match LinuxMemoryPressureSource::new() {
        Ok(source) => source,
        Err(error) => {
          log::warn!("broker memory-pressure admission disabled: {error}");
          return Self::disabled(metrics_scope);
        },
      };

      info!(
        "broker memory-pressure admission enabled: cgroup_memory_limit_bytes={}, \
         overloaded_on_permyriad={OVERLOADED_ON_PERMYRIAD}",
        source.cgroup_memory_limit_bytes,
      );
      let controller = Self::with_source(Arc::new(source), metrics_scope);
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

  #[cfg(any(target_os = "linux", test))]
  fn with_source(source: Arc<dyn MemoryPressureSource>, metrics_scope: &Scope) -> Self {
    Self {
      inner: Arc::new(MemoryPressureInner {
        overloaded: AtomicBool::new(false),
        metrics: MemoryPressureMetrics::new(metrics_scope),
        source: Some(source),
      }),
    }
  }

  fn disabled(metrics_scope: &Scope) -> Self {
    #[cfg(not(any(target_os = "linux", test)))]
    let _ = metrics_scope;
    Self {
      inner: Arc::new(MemoryPressureInner {
        overloaded: AtomicBool::new(false),
        #[cfg(any(target_os = "linux", test))]
        metrics: MemoryPressureMetrics::new(metrics_scope),
        #[cfg(any(target_os = "linux", test))]
        source: None,
      }),
    }
  }

  #[cfg(test)]
  fn new_for_test(source: Arc<dyn MemoryPressureSource>, metrics_scope: &Scope) -> Self {
    Self::with_source(source, metrics_scope)
  }

  pub(super) fn is_overloaded(&self) -> bool {
    self.inner.overloaded.load(Ordering::Relaxed)
  }

  #[cfg(any(target_os = "linux", test))]
  fn spawn_poller(&self, shutdown_trigger_handle: &ComponentShutdownTriggerHandle) {
    let controller = self.clone();
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

  #[cfg(any(target_os = "linux", test))]
  fn poll_once(&self) {
    let Some(source) = self.inner.source.as_ref() else {
      return;
    };
    let utilization_permyriad = match source.utilization_permyriad() {
      Ok(utilization_permyriad) => utilization_permyriad,
      Err(error) => {
        self.inner.metrics.sampling_failures_total.inc();
        warn_every!(
          30.seconds(),
          "failed sampling broker memory pressure: {error}"
        );
        return;
      },
    };
    self
      .inner
      .metrics
      .utilization_percent
      .set(i64::from(utilization_permyriad / 100));

    let overloaded = utilization_permyriad >= OVERLOADED_ON_PERMYRIAD;
    self.inner.metrics.overloaded.set(i64::from(overloaded));

    let previously_overloaded = self.inner.overloaded.swap(overloaded, Ordering::Relaxed);
    if previously_overloaded == overloaded {
      return;
    }

    self.inner.metrics.transitions_total.inc();
    info!("broker memory-pressure overload transition {previously_overloaded} -> {overloaded}");
  }
}

#[cfg(target_os = "linux")]
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
