#[cfg(test)]
#[path = "./consumer_test.rs"]
mod tests;

mod api;
mod blob_query;
mod broker_client;
mod diagnostics;
mod metadata_query;
mod metrics;
mod reader;
mod scan;
mod state;
mod time;

pub(crate) use api::ConsumerReadOutcome;
pub use api::{ConsumerBatch, ConsumerReader, ReadCapacity};
pub(crate) use blob_query::decode_blob_range_response_for_ranges;
pub use blob_query::{
  BrokerBlobRangeQuery,
  BrokerBlobRangeRead,
  GrpcBrokerBlobRangeQuery,
  decode_blob_range_response,
};
pub(crate) use broker_client::BrokerClientPool;
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
