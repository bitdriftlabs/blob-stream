use super::*;
use blob_stream_blob_store::{BlobKey, ByteRange};
use blob_stream_proto::protos::blobstream::v1::config::{
  BlobStoreConfig,
  InMemoryBlobStoreConfig,
  S3BlobStoreConfig,
  blob_store_config,
};
use bytes::Bytes;

#[tokio::test]
async fn builds_an_in_memory_store_for_writer_reads() {
  let mut config = BlobStoreConfig::new();
  config.backend = Some(blob_store_config::Backend::InMemory(
    InMemoryBlobStoreConfig::new(),
  ));

  let store = build_blob_store(&config).await.unwrap();
  let key = BlobKey::from("topic/blob");
  store
    .blob_store
    .put(&key, Bytes::from_static(b"payload"))
    .await
    .unwrap();

  assert_eq!(
    store
      .blob_store
      .get_range(&key, ByteRange { start: 0, end: 7 })
      .await
      .unwrap(),
    Bytes::from_static(b"payload")
  );
  assert_eq!(store.prefix, None);
}

#[test]
fn builds_an_s3_store_with_the_configured_prefix() {
  let config = S3BlobStoreConfig {
    bucket: "blob-bucket".into(),
    prefix: "segments/".into(),
    region: "us-east-1".into(),
    ..Default::default()
  };
  let client = aws_sdk_s3::Client::from_conf(
    aws_sdk_s3::Config::builder()
      .region(Region::new("us-east-1"))
      .behavior_version(aws_config::BehaviorVersion::latest())
      .build(),
  );

  let store = broker_blob_store_from_s3_config(&config, client);

  assert_eq!(store.prefix.as_deref(), Some("segments/"));
}
