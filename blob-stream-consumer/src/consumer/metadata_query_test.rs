#![allow(clippy::unwrap_used)]

use super::*;
use blob_stream_blob_store::BlobKey;
use blob_stream_metadata_store::{SegmentMetadata, encode_segment_metadata_v1};
use blob_stream_proto::protos::blobstream::v1::broker::{
  BrokerSegmentMetadata,
  FullRecoveryMetadataCoverage,
  MetadataPartitionBound,
  MetadataReadConsistency,
  MetadataReadSuccess,
  ReadMetadataWindowRequest,
  ReadMetadataWindowResponse,
  TailMetadataCoverage,
  read_metadata_window_request,
  read_metadata_window_response,
};
use blob_stream_types::{BatchMetadata, ByteRange, Compression, SeqRange, TopicWindowKey};
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};

fn tail_request() -> ReadMetadataWindowRequest {
  ReadMetadataWindowRequest {
    topic: "topic".into(),
    window_start_unix_seconds: 0,
    consistency: MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL.into(),
    coverage: Some(read_metadata_window_request::Coverage::Tail(
      TailMetadataCoverage {
        partition_bounds: vec![MetadataPartitionBound {
          virtual_partition_id: 0,
          min_snowflake: 100,
          ..Default::default()
        }],
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

fn full_recovery_request() -> ReadMetadataWindowRequest {
  ReadMetadataWindowRequest {
    topic: "topic".into(),
    window_start_unix_seconds: 0,
    consistency: MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL.into(),
    coverage: Some(read_metadata_window_request::Coverage::FullRecovery(
      FullRecoveryMetadataCoverage {
        virtual_partition_ids: vec![0],
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

#[test]
fn rejects_eventual_response_older_than_configured_cache_age() {
  let response = ReadMetadataWindowResponse {
    result: Some(read_metadata_window_response::Result::Success(
      MetadataReadSuccess {
        observed_at_unix_ms: 0,
        refill_floor: Some(100),
        generation: 1,
        retained_coverage: true,
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let error = decode_metadata_response(
    &tail_request(),
    response,
    OffsetDateTime::UNIX_EPOCH.saturating_add(Duration::seconds(1)),
    Duration::milliseconds(250),
  )
  .unwrap_err();

  assert!(
    error
      .to_string()
      .contains("broker metadata response exceeds the configured cache age")
  );
}

fn segment() -> SegmentMetadata {
  SegmentMetadata::new(
    TopicWindowKey {
      topic: "topic".to_string(),
      window_start_unix_seconds: 0,
    },
    SnowflakeId(100),
    BlobKey::from("topic/segment"),
    Compression::none(),
    HashMap::from([(
      0,
      BatchMetadata {
        seq_range: SeqRange { start: 1, end: 2 },
        byte_range: ByteRange { start: 3, end: 4 },
        payload_bytes: 1,
      },
    )]),
    OffsetDateTime::UNIX_EPOCH,
    OffsetDateTime::UNIX_EPOCH,
  )
}

#[test]
fn full_recovery_response_rejects_partial_refill_floor() {
  let response = ReadMetadataWindowResponse {
    result: Some(read_metadata_window_response::Result::Success(
      MetadataReadSuccess {
        observed_at_unix_ms: 0,
        refill_floor: Some(100),
        generation: 1,
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let error = decode_metadata_response(
    &full_recovery_request(),
    response,
    OffsetDateTime::UNIX_EPOCH,
    Duration::seconds(1),
  )
  .unwrap_err();

  assert!(
    error
      .to_string()
      .contains("broker full recovery response has a refill floor")
  );
}

#[test]
fn full_recovery_response_rejects_unrequested_partitions() {
  let mut unexpected_partition = segment();
  let batches = unexpected_partition.segment_index.remove(&0).unwrap();
  unexpected_partition.segment_index.insert(1, batches);
  let response = ReadMetadataWindowResponse {
    result: Some(read_metadata_window_response::Result::Success(
      MetadataReadSuccess {
        observed_at_unix_ms: 0,
        generation: 1,
        segments: vec![BrokerSegmentMetadata {
          snowflake_id: unexpected_partition.snowflake_id.as_u64(),
          metadata: Some(encode_segment_metadata_v1(&unexpected_partition).unwrap()).into(),
          ..Default::default()
        }],
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let error = decode_metadata_response(
    &full_recovery_request(),
    response,
    OffsetDateTime::UNIX_EPOCH,
    Duration::seconds(1),
  )
  .unwrap_err();

  assert!(
    error
      .to_string()
      .contains("broker metadata response has an unrequested partition 1")
  );
}
