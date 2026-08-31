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
pub mod memory_pressure;
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
pub(crate) use config::{DEFAULT_MAX_SEGMENT_BYTES, effective_max_segment_bytes};
pub use config::{RuntimeWriteEngineBuilder, TopicInfo, WriteConfig, build_runtime_metadata_store};
pub use engine::{AdmissionController, WriteEngineBuilder, WriteEngineImpl};
pub use hooks::{BrokerLifecycleHooks, NoopBrokerLifecycleHooks};
pub(crate) use metrics::ProduceOutcomeMetrics;
pub(super) const DEFAULT_ZSTD_LEVEL: i32 = 3;
pub(super) const MAX_IN_FLIGHT_FLUSH_PLANS: usize = 4;
pub(super) const MAX_CONCURRENT_BLOB_UPLOADS: usize = 8;
