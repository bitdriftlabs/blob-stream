use anyhow::{Result, anyhow};
use bd_grpc::client::Client as GrpcClient;
use blob_stream_broker_discovery::{
  BrokerDiscovery,
  BrokerMembership,
  BrokerNode,
  INITIAL_MEMBERSHIP_TIMEOUT,
  discovery_from_config,
  wait_for_initialized_membership,
};
use blob_stream_proto::protos::blobstream::v1::config::BrokerDiscoveryConfig;
use hyper_util::client::legacy::connect::HttpConnector;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use time::Duration as TimeDuration;
use tokio::sync::watch;

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const MAX_REQUEST_CONCURRENCY: u64 = 64;

type HttpGrpcClient = GrpcClient<HttpConnector>;

//
// BrokerClientPool
//

/// Shared local-membership watcher and address-keyed gRPC client pool for consumer broker reads.
pub struct BrokerClientPool {
  // The receiver remains live after startup; each request snapshots the newest membership.
  membership: Mutex<watch::Receiver<BrokerMembership>>,
  // Cached clients are retained only while their addresses appear in the current membership.
  clients: Mutex<HashMap<String, Arc<HttpGrpcClient>>>,
}

impl BrokerClientPool {
  /// Build a client pool from the configured discovery source.
  pub async fn from_config(config: &BrokerDiscoveryConfig) -> Result<Self> {
    Self::new(discovery_from_config(config)?).await
  }

  /// Wait for authoritative membership before allowing any broker request to be routed.
  pub async fn new(discovery: Arc<dyn BrokerDiscovery>) -> Result<Self> {
    let mut membership = discovery.watch_membership().await?;
    // An initialized empty membership is valid. Only `Pending` has no routing decision yet.
    let initial_membership = tokio::time::timeout(
      INITIAL_MEMBERSHIP_TIMEOUT,
      wait_for_initialized_membership(&mut membership),
    )
    .await
    .map_err(|_| anyhow!("consumer initial broker membership timed out after 10 seconds"))??;
    log::info!(
      "consumer broker client pool initialized: broker_count={}",
      initial_membership.nodes().map_or(0, <[_]>::len)
    );
    Ok(Self {
      membership: Mutex::new(membership),
      clients: Mutex::new(HashMap::new()),
    })
  }

  pub fn owner_for(
    &self,
    request_kind: &str,
    select_owner: impl FnOnce(&BrokerMembership) -> Option<BrokerNode>,
  ) -> Result<BrokerNode> {
    let mut membership = self.membership.lock();
    if membership.has_changed().unwrap_or(false) {
      // Existing requests retain their client Arc, while future requests cannot select removed
      // addresses after a discovery update.
      self.reconcile_clients(&membership.borrow_and_update());
    }
    select_owner(&membership.borrow())
      .ok_or_else(|| anyhow!("no initialized local broker route for {request_kind}"))
  }

  pub fn client_for_address(&self, address: &str) -> Result<Arc<HttpGrpcClient>> {
    // This lock only protects synchronous client-cache mutation; no network operation occurs while
    // it is held.
    let mut clients = self.clients.lock();
    if let Some(client) = clients.get(address) {
      return Ok(Arc::clone(client));
    }
    let connect_timeout = TimeDuration::try_from(CONNECT_TIMEOUT)
      .map_err(|_| anyhow!("broker connect timeout exceeds supported range"))?;
    let client = Arc::new(GrpcClient::new_http(
      address,
      connect_timeout,
      MAX_REQUEST_CONCURRENCY,
    )?);
    clients.insert(address.to_string(), Arc::clone(&client));
    Ok(client)
  }

  fn reconcile_clients(&self, membership: &BrokerMembership) {
    let active = membership
      .nodes()
      .unwrap_or_default()
      .iter()
      .map(|node| node.address.to_string())
      .collect::<HashSet<_>>();
    self
      .clients
      .lock()
      .retain(|address, _| active.contains(address));
  }
}
