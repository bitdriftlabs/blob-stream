use crate::config::{
  ProducerCompression,
  ProducerConfig,
  compression_as_grpc,
  producer_connect_timeout,
  producer_max_request_concurrency,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bd_grpc::client::Client as GrpcClient;
use bd_grpc::service::ServiceMethod;
use blob_stream_broker_discovery::BrokerMembership;
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchesRequest,
  ProduceBatchesResponse,
};
use hyper_util::client::legacy::connect::HttpConnector;
use log::debug;
use parking_lot::Mutex;
use protobuf::Chars;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use time::Duration as TimeDuration;

type HttpGrpcClient = GrpcClient<HttpConnector>;

//
// BrokerTransport
//

#[async_trait]
/// Transport abstraction used to send produce RPCs.
pub trait BrokerTransport: Send + Sync {
  /// Send pre-built produce batch requests to a specific broker address.
  async fn produce_batches(
    &self,
    broker_address: &Chars,
    request: ProduceBatchesRequest,
    request_timeout: Duration,
    compression: ProducerCompression,
  ) -> Result<ProduceBatchesResponse>;

  /// Retire any transport resources for brokers absent from the current discovery snapshot.
  fn reconcile_membership(&self, membership: &BrokerMembership) {
    let _ = membership;
  }
}

//
// GrpcBrokerTransport
//

/// Default gRPC transport implementation used by `ProducerClientImpl`.
pub struct GrpcBrokerTransport {
  config: ProducerConfig,
  clients: Mutex<HashMap<Chars, Arc<HttpGrpcClient>>>,
}

impl GrpcBrokerTransport {
  /// Create a transport from producer configuration.
  #[must_use]
  pub fn new(config: ProducerConfig) -> Self {
    Self {
      config,
      clients: Mutex::new(HashMap::new()),
    }
  }

  pub(super) fn client_for_address(&self, broker_address: &Chars) -> Result<Arc<HttpGrpcClient>> {
    let mut clients = self.clients.lock();
    if let Some(client) = clients.get(broker_address) {
      return Ok(Arc::clone(client));
    }

    let connect_timeout = producer_connect_timeout(&self.config);
    let client = Arc::new(GrpcClient::new_http(
      broker_address.as_str(),
      connect_timeout,
      producer_max_request_concurrency(&self.config),
    )?);
    clients.insert(broker_address.clone(), Arc::clone(&client));
    Ok(client)
  }

  fn reconcile_cached_clients(&self, membership: &BrokerMembership) {
    let addresses = membership
      .nodes()
      .unwrap_or_default()
      .iter()
      .map(|node| &node.address)
      .collect::<HashSet<_>>();
    let mut clients = self.clients.lock();
    let client_count = clients.len();
    clients.retain(|address, _| addresses.contains(address));
    let evicted = client_count.saturating_sub(clients.len());
    if evicted > 0 {
      debug!(
        "producer transport retired stale broker clients: evicted={evicted}, retained={}",
        clients.len()
      );
    }
  }
}

#[async_trait]
impl BrokerTransport for GrpcBrokerTransport {
  async fn produce_batches(
    &self,
    broker_address: &Chars,
    request: ProduceBatchesRequest,
    request_timeout: Duration,
    compression: ProducerCompression,
  ) -> Result<ProduceBatchesResponse> {
    let client = self.client_for_address(broker_address)?;
    let service_method = ServiceMethod::<ProduceBatchesRequest, ProduceBatchesResponse>::new(
      "BrokerService",
      "ProduceBatches",
    );
    let request_timeout = TimeDuration::try_from(request_timeout)
      .map_err(|_| anyhow!("producer retry request timeout exceeds supported range"))?;
    let response = client
      .unary(
        &service_method,
        None,
        request,
        request_timeout,
        compression_as_grpc(compression),
      )
      .await
      .map_err(|error| anyhow!(error.to_string()))?;
    Ok(response)
  }

  fn reconcile_membership(&self, membership: &BrokerMembership) {
    self.reconcile_cached_clients(membership);
  }
}
