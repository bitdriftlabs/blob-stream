use super::{DynamoTablePurpose, dynamo_table_name};
use blob_stream_proto::protos::blobstream::v1::config::DynamoMetadataStoreConfig;

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
