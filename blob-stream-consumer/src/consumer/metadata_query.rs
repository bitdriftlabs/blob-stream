#[cfg(test)]
#[path = "./metadata_query_test.rs"]
mod tests;

use crate::consumer::broker_client::BrokerClientPool;
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use bd_grpc::compression::Compression;
use bd_grpc::service::ServiceMethod;
use blob_stream_broker_discovery::{BrokerDiscovery, metadata_window_owner};
use blob_stream_metadata_store::{SegmentMetadata, decode_segment_metadata_v1};
use blob_stream_proto::protos::blobstream::v1::broker::{
  FullRecoveryMetadataCoverage,
  MetadataReadConsistency,
  ReadMetadataWindowRequest,
  ReadMetadataWindowResponse,
  read_metadata_window_request,
  read_metadata_window_response,
};
use blob_stream_proto::protos::blobstream::v1::config::BrokerDiscoveryConfig;
use blob_stream_types::{SnowflakeId, TopicWindowKey};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

//
// BrokerMetadataResult
//

/// Validated broker metadata and the observation that established its visibility cutoff.
#[derive(Debug)]
pub(super) struct BrokerMetadataResult {
  pub(super) segments: Vec<SegmentMetadata>,
  pub(super) observed_at: OffsetDateTime,
}

#[async_trait]
/// Injectable transport boundary for broker-collapsed metadata queries.
pub trait BrokerMetadataQuery: Send + Sync {
  /// Execute one consumer-planned broker metadata request.
  async fn read_metadata_window(
    &self,
    request: ReadMetadataWindowRequest,
  ) -> Result<ReadMetadataWindowResponse>;
}

/// Production transport using the same local discovery and address lifecycle as producers.
///
/// Construction waits for discovery's authoritative initial membership, so request handling never
/// has to treat `Pending` as either an empty membership or a retryable route.
pub struct GrpcBrokerMetadataQuery {
  client_pool: Arc<BrokerClientPool>,
}

impl GrpcBrokerMetadataQuery {
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
impl BrokerMetadataQuery for GrpcBrokerMetadataQuery {
  async fn read_metadata_window(
    &self,
    request: ReadMetadataWindowRequest,
  ) -> Result<ReadMetadataWindowResponse> {
    let owner = self
      .client_pool
      .owner_for("metadata request", |membership| {
        metadata_window_owner(
          request.topic.as_str(),
          request.window_start_unix_seconds,
          membership,
        )
      })?;
    let client = self
      .client_pool
      .client_for_address(owner.address.as_str())?;
    let method = ServiceMethod::<ReadMetadataWindowRequest, ReadMetadataWindowResponse>::new(
      "BrokerService",
      "ReadMetadataWindow",
    );
    let timeout = TimeDuration::try_from(REQUEST_TIMEOUT)
      .map_err(|_| anyhow!("metadata request timeout exceeds supported range"))?;
    client
      .unary(&method, None, request, timeout, Compression::None)
      .await
      .map_err(|error| anyhow!(error.to_string()))
  }
}

pub(super) fn decode_metadata_response(
  request: &ReadMetadataWindowRequest,
  response: ReadMetadataWindowResponse,
  received_at: OffsetDateTime,
  metadata_cache_max_age: TimeDuration,
) -> Result<BrokerMetadataResult> {
  let success = match response.result {
    Some(read_metadata_window_response::Result::Success(success)) => success,
    Some(read_metadata_window_response::Result::Failure(failure)) => {
      return Err(anyhow!(
        "broker metadata request failed: {}",
        failure.error_message
      ));
    },
    None => return Err(anyhow!("broker metadata response has no result")),
  };
  let observed_at_unix_ns = success
    .observed_at_unix_ms
    .checked_mul(1_000_000)
    .ok_or_else(|| anyhow!("broker observation timestamp overflows Unix nanoseconds"))?;
  let observed_at = OffsetDateTime::from_unix_timestamp_nanos(i128::from(observed_at_unix_ns))
    .map_err(|_| anyhow!("broker observation timestamp is out of range"))?;
  ensure!(
    success.generation > 0,
    "broker metadata response has an invalid cache generation"
  );
  let consistency = request
    .consistency
    .enum_value()
    .map_err(|_| anyhow!("broker metadata request has an unsupported consistency"))?;
  // A strongly consistent read must originate from storage, not a broker-retained observation.
  ensure!(
    consistency == MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL
      || !success.retained_coverage,
    "strong broker metadata response cannot use retained coverage"
  );
  if consistency == MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL {
    // The broker's observation time, rather than receipt time, defines the retained-data age.
    ensure!(
      observed_at >= received_at.saturating_sub(metadata_cache_max_age),
      "broker metadata response exceeds the configured cache age"
    );
  }

  let (requested_partitions, tail_bounds) = match request.coverage.as_ref() {
    Some(read_metadata_window_request::Coverage::Tail(coverage)) => {
      let bounds = coverage
        .partition_bounds
        .iter()
        .map(|bound| (bound.virtual_partition_id, SnowflakeId(bound.min_snowflake)))
        .collect::<HashMap<_, _>>();
      let request_floor = bounds
        .values()
        .copied()
        .min()
        .ok_or_else(|| anyhow!("broker metadata request has no tail partition bounds"))?;
      let refill_floor = success
        .refill_floor
        .map(SnowflakeId)
        .ok_or_else(|| anyhow!("broker tail response has no refill floor"))?;
      ensure!(
        refill_floor <= request_floor,
        "broker tail response refill floor does not cover the requested bound"
      );
      (bounds.keys().copied().collect(), Some(bounds))
    },
    Some(read_metadata_window_request::Coverage::FullRecovery(FullRecoveryMetadataCoverage {
      virtual_partition_ids,
      ..
    })) => {
      ensure!(
        success.refill_floor.is_none(),
        "broker full recovery response has a refill floor"
      );
      let partitions = virtual_partition_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
      ensure!(
        !partitions.is_empty() && partitions.len() == virtual_partition_ids.len(),
        "broker full recovery request has no partitions or duplicate partitions"
      );
      (partitions, None)
    },
    None => return Err(anyhow!("broker metadata request has no coverage")),
  };

  // The broker may scan and cache a superset, but its response must be the exact request-visible
  // projection: unique segments, requested partitions only, and no Tail partition below its bound.
  let mut snowflake_ids = BTreeSet::new();
  let segments = success
    .segments
    .into_iter()
    .map(|segment| {
      let snowflake_id = SnowflakeId(segment.snowflake_id);
      ensure!(
        snowflake_ids.insert(snowflake_id),
        "broker metadata response has duplicate snowflake {}",
        snowflake_id.as_u64()
      );
      let metadata = segment
        .metadata
        .into_option()
        .ok_or_else(|| anyhow!("broker metadata response has a segment without metadata"))?;
      ensure!(
        !metadata.partitions.is_empty(),
        "broker metadata response has a segment without requested partitions"
      );
      for partition in &metadata.partitions {
        ensure!(
          requested_partitions.contains(&partition.virtual_partition_id),
          "broker metadata response has an unrequested partition {}",
          partition.virtual_partition_id
        );
        if let Some(tail_bounds) = tail_bounds.as_ref() {
          let partition_floor = tail_bounds
            .get(&partition.virtual_partition_id)
            .ok_or_else(|| {
              anyhow!(
                "broker metadata response has an unrequested partition {}",
                partition.virtual_partition_id
              )
            })?;
          ensure!(
            snowflake_id >= *partition_floor,
            "broker metadata response has a segment below partition {} bound",
            partition.virtual_partition_id
          );
        }
      }
      decode_segment_metadata_v1(
        TopicWindowKey {
          topic: request.topic.to_string(),
          window_start_unix_seconds: request.window_start_unix_seconds,
        },
        snowflake_id,
        metadata,
      )
    })
    .collect::<Result<Vec<_>>>()?;
  Ok(BrokerMetadataResult {
    segments,
    observed_at,
  })
}
