use crate::{InMemoryMetadataStore, MetadataStore, SegmentMetadata};
use blob_stream_blob_store::BlobKey;
use blob_stream_proto::protos::blobstream::v1::metadata::SegmentMetadataV1;
use blob_stream_types::{
  BatchMetadata,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use protobuf::Message;
use std::collections::HashMap;

fn build_segment(
  topic: &str,
  window_start_unix_seconds: i64,
  snowflake_id: u64,
) -> SegmentMetadata {
  let mut segment_index = HashMap::new();
  let batch = BatchMetadata {
    seq_range: SeqRange { start: 10, end: 19 },
    byte_range: blob_stream_types::ByteRange { start: 0, end: 512 },
    payload_bytes: 512,
  };
  segment_index.insert(0 as VirtualPartitionId, vec![batch]);

  SegmentMetadata::new(
    TopicWindowKey {
      topic: topic.to_string(),
      window_start_unix_seconds,
    },
    SnowflakeId(snowflake_id),
    BlobKey::from("topic/1/segment"),
    Compression::none(),
    segment_index,
    3000,
    3000,
  )
}

#[tokio::test]
async fn stores_and_scans_window() {
  let store = InMemoryMetadataStore::new();
  let first = build_segment("topic-a", 100, 1);
  let second = build_segment("topic-a", 100, 2);
  let other = build_segment("topic-b", 200, 3);

  store
    .write_segment(first.clone())
    .await
    .expect("write first");
  store
    .write_segment(second.clone())
    .await
    .expect("write second");
  store.write_segment(other).await.expect("write other");

  let window = TopicWindowKey {
    topic: "topic-a".to_string(),
    window_start_unix_seconds: 100,
  };
  let segments = store
    .scan_window_from_snowflake(&window, None)
    .await
    .expect("scan window");

  assert_eq!(segments.len(), 2);
  assert!(segments.contains(&first));
  assert!(segments.contains(&second));
}

#[tokio::test]
async fn scans_window_from_inclusive_snowflake() {
  let store = InMemoryMetadataStore::new();
  let first = build_segment("topic-a", 100, 1);
  let second = build_segment("topic-a", 100, 2);

  store.write_segment(first).await.expect("write first");
  store
    .write_segment(second.clone())
    .await
    .expect("write second");

  let window = TopicWindowKey {
    topic: "topic-a".to_string(),
    window_start_unix_seconds: 100,
  };
  let segments = store
    .scan_window_from_snowflake(&window, Some(SnowflakeId(2)))
    .await
    .expect("scan bounded window");

  assert_eq!(segments, vec![second]);
}

#[tokio::test]
async fn skips_noncompliant_segment_rows() {
  let store = InMemoryMetadataStore::new();
  let valid = build_segment("topic-a", 100, 2);
  store
    .write_segment(valid.clone())
    .await
    .expect("write valid segment");
  store
    .windows
    .write()
    .entry(valid.partition_key())
    .or_default()
    .push(super::EncodedSegmentMetadata {
      partition_key: valid.partition_key(),
      sort_key: SnowflakeId(1).format_lex(),
      payload: vec![0xff].into(),
    });
  let encoded = crate::codec::encode(valid.clone()).expect("encode valid metadata");
  let mut invalid_metadata =
    SegmentMetadataV1::parse_from_tokio_bytes(&encoded.payload).expect("parse valid metadata");
  invalid_metadata.partitions[0].batches[0].byte_end = 0;
  store
    .windows
    .write()
    .entry(valid.partition_key())
    .or_default()
    .push(super::EncodedSegmentMetadata {
      partition_key: valid.partition_key(),
      sort_key: SnowflakeId(3).format_lex(),
      payload: invalid_metadata
        .write_to_bytes()
        .expect("encode invalid metadata")
        .into(),
    });

  let window = TopicWindowKey {
    topic: "topic-a".to_string(),
    window_start_unix_seconds: 100,
  };
  assert_eq!(
    store
      .scan_window_from_snowflake(&window, None)
      .await
      .expect("scan window"),
    vec![valid]
  );
}
