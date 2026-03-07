use bd_server_stats::stats::{Collector, Scope};
use log::trace;

//
// BrokerMetrics
//

#[derive(Clone, Default)]
pub struct BrokerMetrics {
  collector: Collector,
}

impl BrokerMetrics {
  #[must_use]
  pub fn new() -> Self {
    trace!("initializing broker metrics collector");
    Self::default()
  }

  #[must_use]
  pub fn scope(&self) -> Scope {
    trace!("creating broker metrics scope");
    self.collector.scope("blob_stream_broker")
  }

  #[must_use]
  pub fn prometheus_output(&self) -> Vec<u8> {
    trace!("collecting broker prometheus metrics output");
    self.collector.prometheus_output()
  }
}
