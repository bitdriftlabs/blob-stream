#[cfg(test)]
#[path = "./consumer_test.rs"]
mod tests;

mod api;
mod diagnostics;
mod metadata_query;
mod metrics;
mod reader;
mod scan;
mod state;
mod time;

pub(crate) use api::ConsumerReadOutcome;
pub use api::{ConsumerBatch, ConsumerReader, ReadCapacity};
pub(crate) use diagnostics::{
  ConsumerReaderFastFrontierState,
  ConsumerReaderFastScanBoundState,
  ConsumerReaderPartitionScanState,
};
pub use metadata_query::{BrokerMetadataQuery, GrpcBrokerMetadataQuery};
use metrics::ConsumerReaderMetrics;
pub use reader::ConsumerReaderImpl;
pub(crate) use state::{
  ConsumerReaderPartitionMode,
  ConsumerReaderPartitionState,
  RecoveryState,
  VirtualPartitionState,
};
pub(crate) use time::metadata_visibility_delay;
use time::{AvailabilityHorizon, offset_datetime_from_unix_seconds};
