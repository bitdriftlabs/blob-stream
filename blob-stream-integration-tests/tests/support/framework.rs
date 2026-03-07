#[path = "./framework/cluster.rs"]
mod cluster;
#[path = "./framework/config.rs"]
mod config;
#[path = "./framework/discovery.rs"]
mod discovery;
#[path = "./framework/event_log.rs"]
mod event_log;
#[path = "./framework/helpers.rs"]
mod helpers;
#[path = "./framework/resources.rs"]
mod resources;
#[path = "./framework/runtime.rs"]
mod runtime;
#[path = "./framework/store_faults.rs"]
mod store_faults;
#[path = "./framework/transport.rs"]
mod transport;

pub const TOPIC: &str = "telemetry";
pub const SECOND_TOPIC: &str = "telemetry-secondary";
pub const PARTITION_COUNT: u32 = 16;
pub const WINDOW_SIZE_SECONDS: i64 = 300;

pub use cluster::ClusterHarness;
pub use config::{
  consumer_runtime_config,
  producer_config,
  producer_config_with_writer_id,
  producer_topic,
  producer_topic_named,
  producer_topic_named_with_writers,
};
pub use discovery::{DynamicBrokerDiscovery, DynamicCoordinationSource};
#[allow(unused_imports)]
pub use event_log::{TestEvent, TestEventLog, TestEventMatcher};
pub use helpers::{drain_reader_until, produce_message, produce_message_for_topic};
pub use resources::IntegrationResources;
pub use runtime::now_unix_seconds;
#[allow(unused_imports)]
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
