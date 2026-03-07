use crate::SegmentMetadata;
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{
  BatchMetadata,
  BatchSummary,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
};
use std::collections::HashMap;

#[test]
fn formats_partition_and_snowflake_keys() {
  let mut segment_index = HashMap::new();
  let batch = BatchMetadata {
    seq_range: SeqRange { start: 1, end: 2 },
    byte_range: blob_stream_types::ByteRange { start: 0, end: 10 },
    summary: BatchSummary {
      record_count: 2,
      payload_bytes: 10,
      min_event_ts_ms: 100,
      max_event_ts_ms: 200,
    },
    compression: Compression::none(),
  };
  segment_index.insert(0 as blob_stream_types::VirtualPartitionId, vec![batch]);

  let metadata = SegmentMetadata::new(
    TopicWindowKey {
      topic: "topic".to_string(),
      window_start_unix_seconds: 300,
    },
    SnowflakeId(42),
    BlobKey::from("topic/300/42"),
    segment_index,
    Compression::none(),
    2,
    100,
    200,
    None,
    400,
  );

  assert_eq!(metadata.partition_key(), "topic#300");
  assert_eq!(metadata.snowflake_key(), "00000000000000000042");
}
