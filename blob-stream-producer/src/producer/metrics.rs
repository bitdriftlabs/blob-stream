use bd_server_stats::stats::Scope;
use prometheus::{Histogram, IntCounter, IntGauge};

//
// ProducerMetrics
//

#[derive(Clone)]
pub(super) struct ProducerMetrics {
  pub(super) records_enqueued: IntCounter,
  pub(super) batches_sent: IntCounter,
  pub(super) records_sent: IntCounter,
  pub(super) flushes_max_size: IntCounter,
  pub(super) flushes_max_delay: IntCounter,
  pub(super) retries: IntCounter,
  pub(super) not_lease_holder_retry_timers: IntCounter,
  pub(super) not_lease_holder_retry_membership_updates: IntCounter,
  pub(super) not_lease_holder_retry_same_owner: IntCounter,
  pub(super) not_lease_holder_retry_changed_owner: IntCounter,
  pub(super) failures: IntCounter,
  pub(super) no_brokers: IntCounter,
  pub(super) active_requests: IntGauge,
  pub(super) send_latency_seconds: Histogram,
}

impl ProducerMetrics {
  pub(super) fn new(scope: &Scope) -> Self {
    let scope = scope.scope("producer");
    Self {
      records_enqueued: scope.counter("records_enqueued"),
      batches_sent: scope.counter("batches_sent"),
      records_sent: scope.counter("records_sent"),
      flushes_max_size: scope.counter("flushes_max_size"),
      flushes_max_delay: scope.counter("flushes_max_delay"),
      retries: scope.counter("retries"),
      not_lease_holder_retry_timers: scope.counter("not_lease_holder_retry_timers"),
      not_lease_holder_retry_membership_updates: scope
        .counter("not_lease_holder_retry_membership_updates"),
      not_lease_holder_retry_same_owner: scope.counter("not_lease_holder_retry_same_owner"),
      not_lease_holder_retry_changed_owner: scope.counter("not_lease_holder_retry_changed_owner"),
      failures: scope.counter("failures"),
      no_brokers: scope.counter("no_brokers"),
      active_requests: scope.gauge("active_requests"),
      send_latency_seconds: scope.histogram("send_latency_seconds"),
    }
  }
}
