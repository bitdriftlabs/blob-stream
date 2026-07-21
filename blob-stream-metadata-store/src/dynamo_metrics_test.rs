use super::{DynamoCapacityMetrics, valid_request_units};
use aws_sdk_dynamodb::types::ConsumedCapacity;
use bd_server_stats::stats::Collector;

#[test]
fn records_fractional_request_units_directly() {
  let collector = Collector::default();
  let metrics = DynamoCapacityMetrics::new(&collector.scope("dynamo"));
  let capacity = ConsumedCapacity::builder().capacity_units(0.5).build();

  metrics.record_read(Some(&capacity));

  let output = String::from_utf8(collector.prometheus_output()).expect("metrics are UTF-8");
  assert!(output.contains("dynamo:read_request_units_total 0.5"));
}

#[test]
fn records_split_request_units_from_transactions() {
  let collector = Collector::default();
  let metrics = DynamoCapacityMetrics::new(&collector.scope("dynamo"));
  let capacity = ConsumedCapacity::builder()
    .capacity_units(3.0)
    .read_capacity_units(1.0)
    .write_capacity_units(2.0)
    .build();

  metrics.record_writes(&[capacity]);

  let output = String::from_utf8(collector.prometheus_output()).expect("metrics are UTF-8");
  assert!(output.contains("dynamo:read_request_units_total 1"));
  assert!(output.contains("dynamo:write_request_units_total 2"));
}

#[test]
fn ignores_invalid_request_unit_values() {
  for request_units in [Some(-1.0), Some(f64::NAN), None] {
    assert!(
      valid_request_units(request_units).abs() < f64::EPSILON,
      "invalid request units must not be recorded: {request_units:?}",
    );
  }
}
