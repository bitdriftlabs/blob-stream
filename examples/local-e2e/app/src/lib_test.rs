#![allow(clippy::unwrap_used)]

use super::{
  DEFAULT_GROUP_ID,
  TOPIC,
  consumer_bootstrap_config,
  discovery_config,
  producer_runtime_config,
  topic_config,
};
use bd_pgv::proto_validate;
use blob_stream_broker::config::{ConfigFormat, decode_runtime_config_str};

#[test]
fn shared_topic_config_is_valid() {
  let topic = topic_config();

  proto_validate::validate(&topic).unwrap();
  assert_eq!(topic.name, TOPIC.into());
  assert_eq!(topic.partition_count, 4);
  assert_eq!(topic.num_writers, 1);
}

#[test]
fn static_discovery_contains_both_local_brokers() {
  let discovery = discovery_config();

  proto_validate::validate(&discovery).unwrap();
  let nodes = &discovery.static_().nodes;
  assert_eq!(nodes.len(), 2);
  assert_eq!(nodes[0].node_id, "local-broker-1".into());
  assert_eq!(nodes[0].address, "127.0.0.1:8080".into());
  assert_eq!(nodes[1].node_id, "local-broker-2".into());
  assert_eq!(nodes[1].address, "127.0.0.1:8081".into());
}

#[test]
fn producer_and_consumer_share_valid_topic_and_discovery() {
  let producer = producer_runtime_config();
  let consumer = consumer_bootstrap_config(DEFAULT_GROUP_ID, "consumer-1");

  proto_validate::validate(&producer).unwrap();
  proto_validate::validate(&consumer).unwrap();
  assert_eq!(producer.topics, vec![topic_config()]);
  assert_eq!(consumer.topic.as_ref(), Some(&topic_config()));
  assert_eq!(producer.discovery, consumer.broker_discovery);
}

#[test]
fn checked_in_broker_configs_are_valid_yaml_runtime_configs() {
  for config in [
    include_str!("../../config/broker-1.yaml"),
    include_str!("../../config/broker-2.yaml"),
  ] {
    decode_runtime_config_str(config, ConfigFormat::Yaml).unwrap();
  }
}
