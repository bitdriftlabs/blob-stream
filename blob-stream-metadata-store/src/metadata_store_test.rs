use crate::SegmentMetadata;
use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{BatchMetadata, Compression, SeqRange, SnowflakeId, TopicWindowKey};
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};

pub async fn dynamo_client() -> Result<Client> {
  unsafe {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
  }

  let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url(
      std::env::var("BD_ITEST_DYNAMODB_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:8000".to_string()),
    )
    .load()
    .await;
  Ok(Client::new(&config))
}

#[test]
fn formats_partition_and_snowflake_keys() {
  let mut segment_index = HashMap::new();
  let batch = BatchMetadata {
    seq_range: SeqRange { start: 1, end: 2 },
    byte_range: blob_stream_types::ByteRange { start: 0, end: 10 },
    payload_bytes: 10,
  };
  segment_index.insert(0 as blob_stream_types::VirtualPartitionId, batch);

  let metadata = SegmentMetadata::new(
    TopicWindowKey {
      topic: "topic".to_string(),
      window_start_unix_seconds: 300,
    },
    SnowflakeId(42),
    BlobKey::from("topic/300/42"),
    Compression::none(),
    segment_index,
    OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(400),
    OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(400),
  );

  assert_eq!(metadata.partition_key(), "topic#300");
  assert_eq!(metadata.snowflake_key(), "00000000000000000042");
}
