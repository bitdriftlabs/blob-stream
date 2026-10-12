use super::broker_log_config;
use blob_stream_proto::protos::blobstream::v1::config::BrokerConfig;
use std::time::Duration;

#[test]
fn broker_collector_is_optional_and_uses_standard_grpc_settings() {
  let mut config = BrokerConfig::default();
  assert!(broker_log_config(&config).unwrap().otel.is_none());
  config.otlp_collector_hostname = Some("collector.internal".into());
  let otel = broker_log_config(&config).unwrap().otel.unwrap();
  assert_eq!(otel.endpoint, "http://collector.internal:4317");
  assert_eq!(otel.service_name, "blob-stream-broker");
  assert_eq!(otel.tracer_name, "blob-stream-broker");
  assert_eq!(otel.timeout, Duration::from_secs(3));
  assert_eq!(otel.max_attributes_per_span, 16);
  config.otlp_collector_hostname = Some("".into());
  assert!(broker_log_config(&config).is_err());
}

#[test]
fn collector_hostname_round_trips_protobuf_json_and_yaml() {
  for input in [
    r#"{"otlpCollectorHostname":"collector.internal"}"#,
    "otlpCollectorHostname: collector.internal",
  ] {
    let value: serde_json::Value = serde_yaml::from_str(input).unwrap();
    let config = protobuf_json_mapping::parse_from_str::<BrokerConfig>(&value.to_string()).unwrap();
    let encoded = protobuf_json_mapping::print_to_string(&config).unwrap();
    let decoded = protobuf_json_mapping::parse_from_str::<BrokerConfig>(&encoded).unwrap();
    assert_eq!(
      decoded.otlp_collector_hostname.as_deref(),
      Some("collector.internal")
    );
  }
}
