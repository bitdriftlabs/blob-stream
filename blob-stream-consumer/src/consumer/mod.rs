#[cfg(test)]
#[path = "./consumer_test.rs"]
mod tests;

mod api;
mod diagnostics;
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
use metrics::ConsumerReaderMetrics;
pub use reader::ConsumerReaderImpl;
pub(crate) use state::{
  ConsumerReaderPartitionMode,
  ConsumerReaderPartitionState,
  RecoveryState,
  VirtualPartitionState,
};
use time::{format_unix_timestamp_seconds, metadata_availability_delay_seconds};
