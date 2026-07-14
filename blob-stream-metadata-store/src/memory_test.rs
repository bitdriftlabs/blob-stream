use crate::{InMemoryMetadataStore, MetadataStore, SegmentMetadata};
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{
  BatchMetadata,
  BatchSummary,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
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
    summary: BatchSummary {
      record_count: 10,
      payload_bytes: 512,
      min_event_ts_ms: 1000,
      max_event_ts_ms: 2000,
    },
    compression: Compression::none(),
  };
  segment_index.insert(0 as VirtualPartitionId, vec![batch]);

  SegmentMetadata::new(
    TopicWindowKey {
      topic: topic.to_string(),
      window_start_unix_seconds,
    },
    SnowflakeId(snowflake_id),
    BlobKey::from("topic/1/segment"),
    segment_index,
    Compression::none(),
    10,
    1000,
    2000,
    None,
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
