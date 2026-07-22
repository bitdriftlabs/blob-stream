use aws_sdk_dynamodb::types::ConsumedCapacity;
use bd_server_stats::stats::Scope;

#[cfg(test)]
#[path = "./dynamo_metrics_test.rs"]
mod tests;

//
// DynamoCapacityMetrics
//

#[derive(Clone, Debug)]
pub struct DynamoCapacityMetrics {
  read_request_units_total: prometheus::Counter,
  write_request_units_total: prometheus::Counter,
}

impl DynamoCapacityMetrics {
  #[must_use]
  pub fn new(scope: &Scope) -> Self {
    Self {
      read_request_units_total: scope.float_counter("read_request_units_total"),
      write_request_units_total: scope.float_counter("write_request_units_total"),
    }
  }

  pub(crate) fn record_read(&self, consumed_capacity: Option<&ConsumedCapacity>) {
    if let Some(consumed_capacity) = consumed_capacity {
      self.record_capacity(consumed_capacity, CapacityFallback::Read);
    }
  }

  pub(crate) fn record_write(&self, consumed_capacity: Option<&ConsumedCapacity>) {
    if let Some(consumed_capacity) = consumed_capacity {
      self.record_capacity(consumed_capacity, CapacityFallback::Write);
    }
  }

  pub(crate) fn record_writes(&self, consumed_capacities: &[ConsumedCapacity]) {
    for consumed_capacity in consumed_capacities {
      self.record_capacity(consumed_capacity, CapacityFallback::Write);
    }
  }

  fn record_capacity(&self, consumed_capacity: &ConsumedCapacity, fallback: CapacityFallback) {
    let has_split_capacity = consumed_capacity.read_capacity_units().is_some()
      || consumed_capacity.write_capacity_units().is_some();
    if has_split_capacity {
      self
        .read_request_units_total
        .inc_by(valid_request_units(consumed_capacity.read_capacity_units()));
      self.write_request_units_total.inc_by(valid_request_units(
        consumed_capacity.write_capacity_units(),
      ));
      return;
    }

    let request_units = valid_request_units(consumed_capacity.capacity_units());
    match fallback {
      CapacityFallback::Read => self.read_request_units_total.inc_by(request_units),
      CapacityFallback::Write => self.write_request_units_total.inc_by(request_units),
    }
  }
}

#[derive(Clone, Copy)]
enum CapacityFallback {
  Read,
  Write,
}

fn valid_request_units(request_units: Option<f64>) -> f64 {
  request_units
    .filter(|value| value.is_finite() && *value > 0.0)
    .unwrap_or_default()
}
