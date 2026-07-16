use super::BrokerGrpcMetrics;
use bd_server_stats::stats::Collector;

#[test]
fn records_produce_request_timeouts() {
  let collector = Collector::default();
  let metrics = BrokerGrpcMetrics::new(&collector.scope("blob_stream_broker_test"));

  metrics.record_timeout();

  let output = String::from_utf8(collector.prometheus_output()).expect("metrics output is UTF-8");
  assert!(output.contains("blob_stream_broker_test:grpc:request_timeouts_total 1"));
}
