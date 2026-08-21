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
use blob_stream_types::{BatchMetadata, SnowflakeId, TopicWindowKey};
use std::collections::{BTreeMap, BTreeSet, HashMap};
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

//
// CanonicalSegmentMetadata
//

/// Immutable metadata fields selected by one Tail request for shadow comparison.
#[derive(Debug, Eq, PartialEq)]
struct CanonicalSegmentMetadata {
  blob_key: String,
  compression: blob_stream_types::Compression,
  metadata_published_at: OffsetDateTime,
  partition_index: BTreeMap<u32, Vec<BatchMetadata>>,
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

/// Compare broker and direct metadata after applying the request's partition coverage.
///
/// A match requires the canonical projections to be comparable: every segment in the smaller
/// projection must appear with identical immutable metadata in the larger projection. This accepts
/// either source returning an overall superset when it observes a later eventual-consistency state,
/// but rejects gaps or conflicting metadata because neither side then covers the other.
pub(super) fn metadata_results_match(
  request: &ReadMetadataWindowRequest,
  broker_segments: &[SegmentMetadata],
  direct_segments: &[SegmentMetadata],
) -> Result<bool> {
  let broker = canonicalize_metadata(request, broker_segments)?;
  let direct = canonicalize_metadata(request, direct_segments)?;
  Ok(canonical_projection_covers(&broker, &direct) || canonical_projection_covers(&direct, &broker))
}

/// Produce the request-visible metadata used by the shadow comparison.
///
/// The projection discards unrequested partitions, Tail rows below a partition's lower bound, and
/// segments left empty by that filtering. It preserves the immutable fields needed to distinguish
/// identical visibility from conflicting metadata for the same snowflake.
fn canonicalize_metadata(
  request: &ReadMetadataWindowRequest,
  segments: &[SegmentMetadata],
) -> Result<BTreeMap<SnowflakeId, CanonicalSegmentMetadata>> {
  let (requested_partitions, partition_bounds) = match request.coverage.as_ref() {
    Some(read_metadata_window_request::Coverage::Tail(coverage)) => {
      let partition_bounds = coverage
        .partition_bounds
        .iter()
        .map(|bound| (bound.virtual_partition_id, SnowflakeId(bound.min_snowflake)))
        .collect::<HashMap<_, _>>();
      ensure!(
        !partition_bounds.is_empty(),
        "shadow comparison request has no Tail partition bounds"
      );
      (
        partition_bounds.keys().copied().collect(),
        Some(partition_bounds),
      )
    },
    Some(read_metadata_window_request::Coverage::FullRecovery(coverage)) => {
      let requested_partitions = coverage
        .virtual_partition_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
      ensure!(
        !requested_partitions.is_empty()
          && requested_partitions.len() == coverage.virtual_partition_ids.len(),
        "shadow comparison Full Recovery request has no partitions or duplicate partitions"
      );
      (requested_partitions, None)
    },
    None => return Err(anyhow!("shadow comparison request has no coverage")),
  };

  // Compare only the portion each request can observe; cache population may legitimately include
  // other partitions from the same segment.
  let mut canonical = BTreeMap::new();
  for segment in segments {
    let partition_index = segment
      .segment_index
      .iter()
      .filter_map(|(&partition_id, batches)| {
        requested_partitions
          .contains(&partition_id)
          .then_some(partition_id)
          .filter(|partition_id| {
            partition_bounds.as_ref().is_none_or(|bounds| {
              bounds
                .get(partition_id)
                .is_some_and(|bound| segment.snowflake_id >= *bound)
            })
          })
          .map(|partition_id| (partition_id, batches.clone()))
      })
      .collect::<BTreeMap<_, _>>();
    if partition_index.is_empty() {
      continue;
    }
    let candidate = CanonicalSegmentMetadata {
      blob_key: segment.blob_key.as_str().to_string(),
      compression: segment.compression.clone(),
      metadata_published_at: segment.metadata_published_at,
      partition_index,
    };
    if let Some(previous) = canonical.insert(segment.snowflake_id, candidate) {
      ensure!(
        canonical.get(&segment.snowflake_id) == Some(&previous),
        "shadow comparison metadata has duplicate snowflake {} with unequal metadata",
        segment.snowflake_id.as_u64()
      );
    }
  }
  Ok(canonical)
}

/// Return whether every segment in `required` occurs identically in `candidate`.
fn canonical_projection_covers(
  required: &BTreeMap<SnowflakeId, CanonicalSegmentMetadata>,
  candidate: &BTreeMap<SnowflakeId, CanonicalSegmentMetadata>,
) -> bool {
  required
    .iter()
    .all(|(snowflake_id, metadata)| candidate.get(snowflake_id) == Some(metadata))
}
