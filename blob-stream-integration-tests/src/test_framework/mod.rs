// Shared integration-test framework APIs are intentionally ergonomic; we don't require
// #[must_use] on every helper constructor/accessor or generic hasher plumbing in test-only APIs.
#![allow(clippy::must_use_candidate, clippy::implicit_hasher)]

mod cluster;
mod config;
mod discovery;
mod event_log;
mod helpers;
mod lifecycle;
mod manual_time;
mod resources;
mod runtime;
mod store_faults;
mod transport;

pub const TOPIC: &str = "telemetry";
pub const SECOND_TOPIC: &str = "telemetry-secondary";
pub const PARTITION_COUNT: u32 = 16;
pub const WINDOW_SIZE_SECONDS: i64 = 300;

pub use blob_stream_test_utils::ManualTimeProvider;
pub use cluster::{ClusterHarness, InMemoryClusterHarnessBuilder};
pub use config::{
  consumer_bootstrap_config,
  consumer_bootstrap_config_for,
  consumer_runtime_config,
  producer_config,
  producer_config_with_writer_id,
  producer_topic,
  producer_topic_named,
  producer_topic_named_with_partition_count,
  producer_topic_named_with_writers,
};
pub use discovery::DynamicBrokerDiscovery;
pub use event_log::{TestEvent, TestEventLog, TestEventMatcher};
pub use helpers::{
  ReaderDeliveryTrace,
  TestConsumerReader,
  append_reader_delivery_traces,
  drain_reader_until,
  drain_reader_until_with_trace,
  produce_message,
  produce_message_for_topic,
  reader_delivery_counts,
  rescan_reader_with_trace,
};
pub use lifecycle::{
  LifecycleEvent,
  LifecycleGate,
  TestLifecycleHooks,
  advance_manual_time_until_lifecycle_gate,
};
pub use manual_time::ManualProducerRetryClock;
pub use resources::IntegrationResources;
pub use runtime::now_unix_seconds;
pub use store_faults::{
  StoreFaultAction,
  StoreFaultController,
  StoreFaultDomain,
  StoreFaultEvent,
  StoreFaultOperation,
  StoreFaultRule,
  StoreFaultScriptAction,
  StoreFaultScriptStep,
};
pub use transport::{NetworkFault, NetworkFaultRule, NetworkOperation};
