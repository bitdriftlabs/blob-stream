use crate::SegmentMetadata;
use anyhow::{Result, anyhow};
use blob_stream_blob_store::BlobKey;
use blob_stream_proto::protos::blobstream::v1::metadata::{
  SegmentBatchMetadata,
  SegmentMetadataV1,
  SegmentPartitionIndex,
  StoredSegmentCompression,
  StoredSegmentCompressionCodec,
};
use blob_stream_types::{
  BatchMetadata,
  ByteRange,
  Compression,
  CompressionCodec,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  offset_datetime_from_unix_millis_checked,
  unix_millis_from_offset_datetime,
};
use bytes::Bytes;
use protobuf::{EnumOrUnknown, Message};
use std::collections::HashMap;

#[cfg(test)]
#[path = "./codec_test.rs"]
mod tests;

//
// EncodedSegmentMetadata
//

#[derive(Clone, Debug)]
pub struct EncodedSegmentMetadata {
  pub partition_key: String,
  pub sort_key: String,
  pub payload: Bytes,
}

pub fn encode(metadata: SegmentMetadata) -> Result<EncodedSegmentMetadata> {
  let partition_key = metadata.partition_key();
  let sort_key = metadata.snowflake_key();
  let compression = encode_compression(&metadata.compression);
  let partitions = metadata
    .segment_index
    .into_iter()
    .map(|(virtual_partition_id, batches)| SegmentPartitionIndex {
      virtual_partition_id,
      batches: batches
        .into_iter()
        .map(|batch| SegmentBatchMetadata {
          seq_start: batch.seq_range.start,
          seq_end: batch.seq_range.end,
          byte_start: batch.byte_range.start,
          byte_end: batch.byte_range.end,
          payload_bytes: batch.payload_bytes,
          ..Default::default()
        })
        .collect(),
      ..Default::default()
    })
    .collect();
  let metadata = SegmentMetadataV1 {
    blob_key: metadata.blob_key.as_str().to_string().into(),
    created_ts_ms: unix_millis_from_offset_datetime(metadata.created_at)
      .map_err(|error| anyhow!("timestamp does not fit in Unix milliseconds: {error}"))?,
    metadata_published_ts_ms: unix_millis_from_offset_datetime(metadata.metadata_published_at)
      .map_err(|error| anyhow!("timestamp does not fit in Unix milliseconds: {error}"))?,
    compression: Some(compression).into(),
    partitions,
    ..Default::default()
  };

  Ok(EncodedSegmentMetadata {
    partition_key,
    sort_key,
    payload: metadata
      .write_to_bytes()
      .map(Bytes::from)
      .map_err(|error| anyhow!("encode segment metadata: {error}"))?,
  })
}

pub fn decode(partition_key: &str, sort_key: &str, payload: &Bytes) -> Result<SegmentMetadata> {
  let metadata = SegmentMetadataV1::parse_from_tokio_bytes(payload)
    .map_err(|error| anyhow!("decode segment metadata: {error}"))?;
  if metadata.blob_key.is_empty() {
    return Err(anyhow!("segment metadata is missing blob key"));
  }
  let compression = metadata
    .compression
    .as_ref()
    .ok_or_else(|| anyhow!("segment metadata is missing compression"))?;
  let compression = decode_compression(compression)?;
  if metadata.partitions.is_empty() {
    return Err(anyhow!("segment metadata has no partition indexes"));
  }
  let mut segment_index = HashMap::new();
  for partition in metadata.partitions {
    let virtual_partition_id: VirtualPartitionId = partition.virtual_partition_id;
    if partition.batches.is_empty() {
      return Err(anyhow!(
        "segment metadata has no batches for virtual partition {virtual_partition_id}"
      ));
    }
    let batches = partition
      .batches
      .into_iter()
      .map(|batch| {
        if batch.seq_start > batch.seq_end {
          return Err(anyhow!(
            "segment metadata has an invalid sequence range for virtual partition \
             {virtual_partition_id}"
          ));
        }
        if batch.byte_start >= batch.byte_end {
          return Err(anyhow!(
            "segment metadata has an empty byte range for virtual partition {virtual_partition_id}"
          ));
        }
        Ok(BatchMetadata {
          seq_range: SeqRange {
            start: batch.seq_start,
            end: batch.seq_end,
          },
          byte_range: ByteRange {
            start: batch.byte_start,
            end: batch.byte_end,
          },
          payload_bytes: batch.payload_bytes,
        })
      })
      .collect::<Result<Vec<_>>>()?;
    if segment_index
      .insert(virtual_partition_id, batches)
      .is_some()
    {
      return Err(anyhow!(
        "segment metadata contains duplicate virtual partition {virtual_partition_id}"
      ));
    }
  }

  let (topic, window_start) = partition_key
    .rsplit_once('#')
    .ok_or_else(|| anyhow!("invalid metadata partition key {partition_key}"))?;
  let window_start_unix_seconds = window_start
    .parse::<i64>()
    .map_err(|error| anyhow!("invalid metadata window start {window_start}: {error}"))?;
  let snowflake_id = sort_key
    .parse::<u64>()
    .map_err(|error| anyhow!("invalid metadata snowflake id {sort_key}: {error}"))?;

  Ok(SegmentMetadata {
    window: TopicWindowKey {
      topic: topic.to_string(),
      window_start_unix_seconds,
    },
    snowflake_id: SnowflakeId(snowflake_id),
    blob_key: BlobKey::from(metadata.blob_key.to_string()),
    compression,
    segment_index,
    created_at: offset_datetime_from_unix_millis_checked(metadata.created_ts_ms).map_err(
      |error| {
        anyhow!(
          "invalid Unix millisecond timestamp {}: {error}",
          metadata.created_ts_ms
        )
      },
    )?,
    metadata_published_at: offset_datetime_from_unix_millis_checked(
      metadata.metadata_published_ts_ms,
    )
    .map_err(|error| {
      anyhow!(
        "invalid Unix millisecond timestamp {}: {error}",
        metadata.metadata_published_ts_ms
      )
    })?,
  })
}

fn encode_compression(compression: &Compression) -> StoredSegmentCompression {
  let (codec, level) = match compression.codec {
    CompressionCodec::None => (
      StoredSegmentCompressionCodec::STORED_SEGMENT_COMPRESSION_CODEC_NONE,
      0,
    ),
    CompressionCodec::Zstd => (
      StoredSegmentCompressionCodec::STORED_SEGMENT_COMPRESSION_CODEC_ZSTD,
      compression.level.unwrap_or_default(),
    ),
  };
  StoredSegmentCompression {
    codec: EnumOrUnknown::new(codec),
    level,
    ..Default::default()
  }
}

fn decode_compression(compression: &StoredSegmentCompression) -> Result<Compression> {
  match compression.codec.value() {
    0 => Ok(Compression::none()),
    1 => Ok(Compression::zstd(compression.level)),
    value => Err(anyhow!("unsupported segment compression codec {value}")),
  }
}
