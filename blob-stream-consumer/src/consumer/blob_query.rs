#[cfg(test)]
#[path = "./blob_query_test.rs"]
mod tests;

use crate::consumer::broker_client::BrokerClientPool;
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use bd_grpc::compression::Compression;
use bd_grpc::service::ServiceMethod;
use blob_stream_broker_discovery::{BrokerDiscovery, blob_key_owner};
use blob_stream_proto::protos::blobstream::v1::broker::{
  BlobReadFailureStatus,
  ReadBlobRangesRequest,
  ReadBlobRangesResponse,
  read_blob_ranges_response,
};
use blob_stream_proto::protos::blobstream::v1::config::BrokerDiscoveryConfig;
use bytes::Bytes;
use std::sync::Arc;
use time::Duration as TimeDuration;

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

//
// BrokerBlobRangeRead
//

/// Validated response outcome for one broker blob-range request.
#[derive(Debug, Eq, PartialEq)]
pub enum BrokerBlobRangeRead {
  /// Raw compressed bytes in the exact request range order.
  Success(Vec<Bytes>),
  /// The immutable blob is authoritatively absent from storage.
  NotFound,
}

#[async_trait]
/// Injectable transport boundary for broker blob-range queries.
pub trait BrokerBlobRangeQuery: Send + Sync {
  /// Execute one consumer-planned broker blob-range request.
  async fn read_blob_ranges(
    &self,
    request: ReadBlobRangesRequest,
  ) -> Result<ReadBlobRangesResponse>;
}

//
// GrpcBrokerBlobRangeQuery
//

/// Production blob-range transport using the shared local broker client pool.
pub struct GrpcBrokerBlobRangeQuery {
  client_pool: Arc<BrokerClientPool>,
}

impl GrpcBrokerBlobRangeQuery {
  /// Build a query transport from the configured discovery source.
  pub async fn from_config(config: &BrokerDiscoveryConfig) -> Result<Self> {
    Ok(Self::from_client_pool(Arc::new(
      BrokerClientPool::from_config(config).await?,
    )))
  }

  /// Build a query transport from an explicit discovery source.
  pub async fn new(discovery: Arc<dyn BrokerDiscovery>) -> Result<Self> {
    Ok(Self::from_client_pool(Arc::new(
      BrokerClientPool::new(discovery).await?,
    )))
  }

  pub(crate) fn from_client_pool(client_pool: Arc<BrokerClientPool>) -> Self {
    Self { client_pool }
  }
}

#[async_trait]
impl BrokerBlobRangeQuery for GrpcBrokerBlobRangeQuery {
  async fn read_blob_ranges(
    &self,
    request: ReadBlobRangesRequest,
  ) -> Result<ReadBlobRangesResponse> {
    ensure!(
      !request.blob_key.is_empty(),
      "broker blob request has no blob key"
    );
    let owner = self.client_pool.owner_for("blob request", |membership| {
      blob_key_owner(request.blob_key.as_str(), membership)
    })?;
    let client = self
      .client_pool
      .client_for_address(owner.address.as_str())?;
    let method = ServiceMethod::<ReadBlobRangesRequest, ReadBlobRangesResponse>::new(
      "BrokerService",
      "ReadBlobRanges",
    );
    let timeout = TimeDuration::try_from(REQUEST_TIMEOUT)
      .map_err(|_| anyhow!("blob request timeout exceeds supported range"))?;
    client
      .unary(&method, None, request, timeout, Compression::None)
      .await
      .map_err(|error| anyhow!(error.to_string()))
  }
}

/// Validate a positional blob-range response before any bytes reach the consumer decode path.
pub fn decode_blob_range_response(
  request: &ReadBlobRangesRequest,
  response: ReadBlobRangesResponse,
) -> Result<BrokerBlobRangeRead> {
  decode_blob_range_response_for_ranges(
    request.ranges.iter().map(|range| (range.start, range.end)),
    response,
  )
}

pub fn decode_blob_range_response_for_ranges(
  expected_ranges: impl ExactSizeIterator<Item = (u64, u64)>,
  response: ReadBlobRangesResponse,
) -> Result<BrokerBlobRangeRead> {
  match response.result {
    Some(read_blob_ranges_response::Result::Success(success)) => {
      let expected_range_count = expected_ranges.len();
      ensure!(
        success.ranges.len() == expected_range_count,
        "broker blob response has {} ranges for {} requested ranges",
        success.ranges.len(),
        expected_range_count
      );
      let payloads = success
        .ranges
        .into_iter()
        .zip(expected_ranges)
        .map(|(result, (start, end))| {
          let expected_bytes = end
            .checked_sub(start)
            .ok_or_else(|| anyhow!("broker blob request has an invalid range"))?;
          let actual_bytes = u64::try_from(result.payload.len())
            .map_err(|_| anyhow!("broker blob response length does not fit in u64"))?;
          ensure!(
            actual_bytes == expected_bytes,
            "broker blob response range has {actual_bytes} bytes, expected {expected_bytes}"
          );
          Ok(result.payload)
        })
        .collect::<Result<Vec<_>>>()?;
      Ok(BrokerBlobRangeRead::Success(payloads))
    },
    Some(read_blob_ranges_response::Result::Failure(failure)) => {
      let status = failure
        .status
        .enum_value()
        .map_err(|_| anyhow!("broker blob response has an unsupported failure status"))?;
      if status == BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_NOT_FOUND {
        return Ok(BrokerBlobRangeRead::NotFound);
      }
      Err(anyhow!(
        "broker blob request failed: status={status:?}, error={}",
        failure.error_message
      ))
    },
    None => Err(anyhow!("broker blob response has no result")),
  }
}
