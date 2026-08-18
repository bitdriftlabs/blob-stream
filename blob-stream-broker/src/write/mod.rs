#[cfg(test)]
#[path = "./write_test.rs"]
mod tests;

mod allocation;
mod api;
mod buffer;
mod config;
mod engine;
mod flush;
mod hooks;
mod lease;
mod memory_pressure;
mod metrics;
mod scheduler;
mod state;

pub use api::{
  BrokerLeaseSnapshot,
  BrokerLeaseStatus,
  BrokerNodeSnapshot,
  BrokerPartitionOwnershipSnapshot,
  BrokerPartitionStateSnapshot,
  BrokerStateSnapshot,
  BrokerTopicStateSnapshot,
  SequenceReservationSnapshot,
  WriteEngine,
  WriteError,
  WriteRequest,
  WriteResponse,
};
pub use config::{TopicInfo, WriteConfig, build_runtime_metadata_store, build_write_engine};
pub use engine::{
  AdmissionController,
  MemoryPressureAdmissionController,
  WriteEngineBuilder,
  WriteEngineImpl,
};
pub use hooks::{BrokerLifecycleHooks, NoopBrokerLifecycleHooks};
pub(crate) use metrics::ProduceOutcomeMetrics;
pub(super) const DEFAULT_ZSTD_LEVEL: i32 = 3;
pub(super) const MAX_IN_FLIGHT_FLUSH_PLANS: usize = 4;
