use super::{
  EncodedSegmentMetadata,
  decode,
  decode_segment_metadata_v1,
  encode,
  encode_segment_metadata_v1,
};
use crate::SegmentMetadata;
use blob_stream_blob_store::BlobKey;
use blob_stream_proto::protos::blobstream::v1::metadata::SegmentMetadataV1;
use blob_stream_types::{
  BatchMetadata,
  ByteRange,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  offset_datetime_from_unix_millis,
};
use bytes::Bytes;
use protobuf::Message;
use std::collections::HashMap;

const DYNAMO_ITEM_OVERHEAD_BYTES: usize = 100;
const DYNAMO_TTL_VALUE_BYTES: usize = 10;
const REPRESENTATIVE_DYNAMO_ITEM_BYTES: usize = 475;

fn build_segment(compression: Compression) -> SegmentMetadata {
  SegmentMetadata::new(
    TopicWindowKey {
      topic: "topic-a".to_string(),
      window_start_unix_seconds: 1_700_000_000,
    },
    SnowflakeId(1_234_567_890),
    BlobKey::from("topic-a/1700000000/1234567890.zst"),
    compression,
    HashMap::from([(
      7 as VirtualPartitionId,
      vec![BatchMetadata {
        seq_range: SeqRange {
          start: 100,
          end: 199,
        },
        byte_range: ByteRange {
          start: 1_024,
          end: 2_048,
        },
        payload_bytes: 1_024,
      }],
    )]),
    offset_datetime_from_unix_millis(1_700_000_000_000),
    offset_datetime_from_unix_millis(1_700_000_000_100),
  )
}

fn estimated_dynamo_item_bytes(encoded: &EncodedSegmentMetadata) -> usize {
  // DynamoDB adds 100 bytes of item overhead; the rest is the top-level attribute names and
  // values. The TTL uses the longest practical 10-digit Unix-second value.
  DYNAMO_ITEM_OVERHEAD_BYTES
    + "pk".len()
    + encoded.partition_key.len()
    + "sk".len()
    + encoded.sort_key.len()
    + "segment_metadata_v1".len()
    + encoded.payload.len()
    + "ttl_epoch_seconds".len()
    + DYNAMO_TTL_VALUE_BYTES
}

#[test]
fn round_trips_segment_metadata_with_each_compression_codec() {
  for compression in [Compression::none(), Compression::zstd(3)] {
    let metadata = build_segment(compression);
    let encoded = encode(&metadata).expect("encode metadata");

    assert_eq!(
      decode(&encoded.partition_key, &encoded.sort_key, &encoded.payload).expect("decode metadata"),
      metadata
    );
  }
}

#[test]
fn round_trips_segment_metadata_through_transport_proto() {
  let metadata = build_segment(Compression::zstd(3));
  let proto = encode_segment_metadata_v1(&metadata).expect("encode transport metadata");

  assert_eq!(
    decode_segment_metadata_v1(metadata.window.clone(), metadata.snowflake_id, proto)
      .expect("decode transport metadata"),
    metadata
  );
}

#[test]
fn rejects_malformed_payload() {
  let error = decode(
    "topic-a#1700000000",
    "00000000001234567890",
    &Bytes::from_static(&[0xff]),
  )
  .expect_err("malformed payload must fail");

  assert!(error.to_string().contains("decode segment metadata"));
}

#[test]
fn rejects_semantically_invalid_payloads() {
  let metadata = build_segment(Compression::none());
  let encoded = encode(&metadata).expect("encode metadata");
  for (description, mutate) in [
    (
      "missing blob key",
      Box::new(|metadata: &mut SegmentMetadataV1| metadata.blob_key.clear())
        as Box<dyn Fn(&mut SegmentMetadataV1)>,
    ),
    (
      "missing partition indexes",
      Box::new(|metadata: &mut SegmentMetadataV1| metadata.partitions.clear())
        as Box<dyn Fn(&mut SegmentMetadataV1)>,
    ),
    (
      "missing partition batches",
      Box::new(|metadata: &mut SegmentMetadataV1| metadata.partitions[0].batches.clear())
        as Box<dyn Fn(&mut SegmentMetadataV1)>,
    ),
    (
      "invalid sequence range",
      Box::new(|metadata: &mut SegmentMetadataV1| {
        metadata.partitions[0].batches[0].seq_start = 200;
      }) as Box<dyn Fn(&mut SegmentMetadataV1)>,
    ),
    (
      "empty byte range",
      Box::new(|metadata: &mut SegmentMetadataV1| {
        metadata.partitions[0].batches[0].byte_end = 1_024;
      }) as Box<dyn Fn(&mut SegmentMetadataV1)>,
    ),
  ] {
    let mut metadata =
      SegmentMetadataV1::parse_from_tokio_bytes(&encoded.payload).expect("parse encoded metadata");
    mutate(&mut metadata);
    let payload = Bytes::from(metadata.write_to_bytes().expect("encode invalid metadata"));

    assert!(
      decode(&encoded.partition_key, &encoded.sort_key, &payload).is_err(),
      "{description} must be rejected"
    );
  }
}

#[test]
fn representative_segment_metadata_fits_one_dynamodb_read_chunk() {
  let mut segment_index = HashMap::new();
  for partition_id in 0 .. 8_u64 {
    segment_index.insert(
      VirtualPartitionId::try_from(partition_id).expect("representative partition id fits u32"),
      vec![BatchMetadata {
        seq_range: SeqRange {
          start: partition_id * 10_000,
          end: (partition_id * 10_000) + 9_999,
        },
        byte_range: ByteRange {
          start: partition_id * 32 * 1_024 * 1_024,
          end: (partition_id + 1) * 32 * 1_024 * 1_024,
        },
        payload_bytes: 32 * 1_024 * 1_024,
      }],
    );
  }
  let metadata = SegmentMetadata::new(
    TopicWindowKey {
      topic: "example-topic".to_string(),
      window_start_unix_seconds: 1_700_000_000,
    },
    SnowflakeId(1_234_567_890),
    BlobKey::from("example-topic/1700000000/1234567890.zst"),
    Compression::zstd(3),
    segment_index,
    offset_datetime_from_unix_millis(1_700_000_000_000),
    offset_datetime_from_unix_millis(1_700_000_000_100),
  );
  let encoded = encode(&metadata).expect("encode representative metadata");
  let item_bytes = estimated_dynamo_item_bytes(&encoded);

  assert_eq!(item_bytes, REPRESENTATIVE_DYNAMO_ITEM_BYTES);

  assert!(
    item_bytes < 4_096,
    "representative DynamoDB metadata item is {item_bytes} bytes"
  );
}
