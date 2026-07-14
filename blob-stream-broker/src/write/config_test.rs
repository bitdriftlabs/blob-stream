use super::{DynamoTablePurpose, WriteConfig, dynamo_table_name};
use blob_stream_proto::protos::blobstream::v1::config::{
  BrokerConfig,
  DynamoMetadataStoreConfig,
  SegmentCompression,
};
use blob_stream_types::CompressionCodec;

fn dynamo_config() -> DynamoMetadataStoreConfig {
  let mut config = DynamoMetadataStoreConfig::new();
  config.segment_metadata_table_name = "blob-segments".into();
  config.producer_partition_lease_table_name = "producer-leases".into();
  config.consumer_group_lease_table_name = "consumer-leases".into();
  config
}

#[test]
fn prefers_explicit_segment_metadata_table_name() {
  let config = dynamo_config();
  let table_name = dynamo_table_name(&config, DynamoTablePurpose::SegmentMetadata);
  assert_eq!(table_name, "blob-segments");
}

#[test]
fn prefers_explicit_producer_lease_table_name() {
  let config = dynamo_config();
  let table_name = dynamo_table_name(&config, DynamoTablePurpose::ProducerPartitionLeases);
  assert_eq!(table_name, "producer-leases");
}

#[test]
fn returns_empty_when_explicit_fields_are_unset() {
  let config = DynamoMetadataStoreConfig::new();

  let metadata_table = dynamo_table_name(&config, DynamoTablePurpose::SegmentMetadata);
  let producer_lease_table =
    dynamo_table_name(&config, DynamoTablePurpose::ProducerPartitionLeases);

  assert_eq!(metadata_table, "");
  assert_eq!(producer_lease_table, "");
}

#[test]
fn defaults_segment_compression_to_zstd() {
  let config = WriteConfig::from_broker_config(&BrokerConfig::new()).unwrap();

  assert_eq!(config.compression.codec, CompressionCodec::Zstd);
  assert_eq!(config.compression.level, Some(3));
}

#[test]
fn respects_uncompressed_segment_configuration() {
  let mut broker_config = BrokerConfig::new();
  broker_config.segment_compression = Some(SegmentCompression::SEGMENT_COMPRESSION_NONE.into());

  let config = WriteConfig::from_broker_config(&broker_config).unwrap();

  assert_eq!(config.compression.codec, CompressionCodec::None);
  assert_eq!(config.compression.level, None);
}
