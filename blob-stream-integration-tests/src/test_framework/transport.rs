#[cfg(test)]
#[path = "./transport_test.rs"]
mod tests;

use crate::test_framework::event_log::TestEventLog;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_broker::write::{WriteEngine, WriteError, WriteRequest};
use blob_stream_broker_discovery::BrokerNode;
use blob_stream_producer::{BrokerTransport as ProducerBrokerTransport, ProducerCompression};
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchResponse,
  ProduceBatchesRequest,
  ProduceBatchesResponse,
  ProduceStatus,
};
use protobuf::Chars;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, oneshot, watch};
use tokio::time::sleep as tokio_sleep;

//
// BrokerEndpointBinding
//

pub enum BrokerEndpointBinding {
  Tcp(tokio::net::TcpListener),
  InMemory,
}

//
// BrokerEndpoint
//

pub struct BrokerEndpoint {
  pub node: BrokerNode,
  pub binding: BrokerEndpointBinding,
}

#[async_trait]
pub trait BrokerTransport: Send + Sync {
  async fn bind_endpoint(&self, node_id: &str) -> Result<BrokerEndpoint>;

  async fn install_event_log(&self, _event_log: TestEventLog) -> Result<()> {
    Ok(())
  }

  fn register_write_engine(&self, _node_id: &str, _engine: Arc<dyn WriteEngine>) -> Result<()> {
    Ok(())
  }

  fn producer_transport(&self) -> Option<Arc<dyn ProducerBrokerTransport>> {
    None
  }

  fn fault_controller(&self) -> Option<NetworkFaultController> {
    None
  }
}

//
// GrpcTcpTransport
//

#[derive(Default)]
pub struct GrpcTcpTransport;

#[async_trait]
impl BrokerTransport for GrpcTcpTransport {
  async fn bind_endpoint(&self, node_id: &str) -> Result<BrokerEndpoint> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let node = BrokerNode {
      node_id: node_id.to_string().into(),
      address: listener.local_addr()?.to_string().into(),
    };

    Ok(BrokerEndpoint {
      node,
      binding: BrokerEndpointBinding::Tcp(listener),
    })
  }
}

//
// NetworkOperation
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkOperation {
  ProduceBatches,
}

//
// NetworkFault
//

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkFault {
  Drop,
  DropResponse,
  Delay(Duration),
  Duplicate { copies: u32 },
  Reorder { delay: Duration },
  Timeout(Duration),
  Partition,
}

//
// NetworkFaultRule
//

#[derive(Clone, Debug)]
pub struct NetworkFaultRule {
  pub target_node_id: Option<String>,
  pub operation: NetworkOperation,
  pub fault: NetworkFault,
  pub remaining_hits: Option<u64>,
}

//
// FaultScriptAction
//

#[derive(Clone, Debug)]
pub enum FaultScriptAction {
  Enable(NetworkFaultRule),
  Disable { fault_id: u64 },
  ClearAll,
}

//
// FaultScriptStep
//

#[derive(Clone, Debug)]
pub struct FaultScriptStep {
  pub on_call_count: u64,
  pub action: FaultScriptAction,
}

//
// NetworkFaultController
//

#[derive(Clone)]
pub struct NetworkFaultController {
  inner: Arc<Mutex<NetworkFaultControllerState>>,
}

//
// ManualNetworkScheduler
//

/// Releases transport delay and reorder waits only when a test explicitly advances it.
#[derive(Clone)]
pub struct ManualNetworkScheduler {
  generation: watch::Sender<u64>,
  active_sleeps: Arc<AtomicUsize>,
  sleep_registered: Arc<Notify>,
}

impl Default for ManualNetworkScheduler {
  fn default() -> Self {
    let (generation, _receiver) = watch::channel(0);
    Self {
      generation,
      active_sleeps: Arc::new(AtomicUsize::new(0)),
      sleep_registered: Arc::new(Notify::new()),
    }
  }
}

impl ManualNetworkScheduler {
  /// Wait until the expected number of transport waits have registered.
  pub async fn wait_until_sleeping(&self, expected_sleepers: usize) {
    loop {
      let notified = self.sleep_registered.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if self.active_sleeps.load(Ordering::Acquire) >= expected_sleepers {
        return;
      }
      notified.await;
    }
  }

  /// Release all waits that were registered before this call.
  pub fn advance(&self) {
    self
      .generation
      .send_modify(|generation| *generation = generation.saturating_add(1));
  }

  async fn sleep(&self, _duration: Duration) {
    let mut generations = self.generation.subscribe();
    let generation = *generations.borrow_and_update();
    self.active_sleeps.fetch_add(1, Ordering::Release);
    self.sleep_registered.notify_waiters();
    while *generations.borrow_and_update() <= generation {
      if generations.changed().await.is_err() {
        break;
      }
    }
    self.active_sleeps.fetch_sub(1, Ordering::Release);
  }
}

//
// NetworkFaultControllerState
//

#[derive(Default)]
struct NetworkFaultControllerState {
  next_fault_id: u64,
  total_calls: u64,
  rules: Vec<ActiveNetworkFaultRule>,
  script_steps: Vec<FaultScriptStep>,
  event_log: Option<TestEventLog>,
  manual_scheduler: Option<ManualNetworkScheduler>,
}

//
// ActiveNetworkFaultRule
//

struct ActiveNetworkFaultRule {
  id: u64,
  rule: NetworkFaultRule,
}

//
// NetworkFaultEffects
//

#[derive(Default)]
struct NetworkFaultEffects {
  drop_request: bool,
  drop_response: bool,
  partition: bool,
  delay: Option<Duration>,
  reorder_delay: Option<Duration>,
  timeout: Option<Duration>,
  duplicate_copies: u32,
}

//
// NetworkFaultController
//

impl Default for NetworkFaultController {
  fn default() -> Self {
    Self {
      inner: Arc::new(Mutex::new(NetworkFaultControllerState::default())),
    }
  }
}

impl NetworkFaultController {
  pub async fn attach_event_log(&self, event_log: TestEventLog) {
    self.inner.lock().await.event_log = Some(event_log);
  }

  /// Replace wall-clock transport sleeps with test-controlled manual scheduling.
  pub async fn enable_manual_scheduling(&self) -> ManualNetworkScheduler {
    let scheduler = ManualNetworkScheduler::default();
    self.inner.lock().await.manual_scheduler = Some(scheduler.clone());
    scheduler
  }

  pub async fn enable_fault(&self, rule: NetworkFaultRule) -> u64 {
    let mut guard = self.inner.lock().await;
    let id = guard.next_fault_id;
    guard.next_fault_id = guard.next_fault_id.saturating_add(1);
    guard.rules.push(ActiveNetworkFaultRule { id, rule });
    id
  }

  pub async fn disable_fault(&self, fault_id: u64) -> bool {
    let mut guard = self.inner.lock().await;
    let previous_len = guard.rules.len();
    guard.rules.retain(|entry| entry.id != fault_id);
    previous_len != guard.rules.len()
  }

  pub async fn clear_all_faults(&self) {
    let mut guard = self.inner.lock().await;
    guard.rules.clear();
    guard.script_steps.clear();
  }

  pub async fn load_script(&self, mut steps: Vec<FaultScriptStep>) {
    steps.sort_by_key(|step| step.on_call_count);
    let mut guard = self.inner.lock().await;
    guard.script_steps = steps;
  }

  async fn effects_for_call(
    &self,
    node_id: &str,
    operation: NetworkOperation,
  ) -> NetworkFaultEffects {
    let mut guard = self.inner.lock().await;
    guard.total_calls = guard.total_calls.saturating_add(1);
    apply_script_steps_locked(&mut guard);

    // Combine all matching rules into a single effect payload for this operation.
    let mut effects = NetworkFaultEffects::default();
    for entry in &mut guard.rules {
      if !rule_matches(&entry.rule, node_id, operation) {
        continue;
      }

      match &entry.rule.fault {
        NetworkFault::Drop => {
          effects.drop_request = true;
        },
        NetworkFault::DropResponse => {
          effects.drop_response = true;
        },
        NetworkFault::Delay(delay) => {
          effects.delay = Some(max_duration(effects.delay, *delay));
        },
        NetworkFault::Duplicate { copies } => {
          effects.duplicate_copies = effects.duplicate_copies.max(*copies);
        },
        NetworkFault::Reorder { delay } => {
          effects.reorder_delay = Some(max_duration(effects.reorder_delay, *delay));
        },
        NetworkFault::Timeout(duration) => {
          effects.timeout = Some(max_duration(effects.timeout, *duration));
        },
        NetworkFault::Partition => {
          effects.partition = true;
        },
      }

      if let Some(remaining) = entry.rule.remaining_hits.as_mut() {
        *remaining = remaining.saturating_sub(1);
      }
    }

    guard
      .rules
      .retain(|entry| entry.rule.remaining_hits != Some(0));
    let event_log = guard.event_log.clone();
    let status = if effects.drop_request
      || effects.drop_response
      || effects.partition
      || effects.delay.is_some()
      || effects.reorder_delay.is_some()
      || effects.timeout.is_some()
      || effects.duplicate_copies > 0
    {
      "fault_applied"
    } else {
      "pass"
    };
    drop(guard);

    if let Some(event_log) = event_log {
      event_log
        .record(
          "transport",
          describe_network_operation(operation),
          Some(node_id.to_string()),
          status,
          None,
        )
        .await;
    }

    effects
  }

  async fn record_produce_event(
    &self,
    node_id: &str,
    status: &'static str,
    detail: Option<String>,
  ) {
    let event_log = self.inner.lock().await.event_log.clone();
    if let Some(event_log) = event_log {
      event_log
        .record(
          "transport",
          describe_network_operation(NetworkOperation::ProduceBatches),
          Some(node_id.to_string()),
          status,
          detail,
        )
        .await;
    }
  }

  async fn sleep(&self, duration: Duration) {
    let scheduler = self.inner.lock().await.manual_scheduler.clone();
    if let Some(scheduler) = scheduler {
      scheduler.sleep(duration).await;
    } else {
      tokio_sleep(duration).await;
    }
  }

  async fn manual_scheduler(&self) -> Option<ManualNetworkScheduler> {
    self.inner.lock().await.manual_scheduler.clone()
  }
}

fn apply_script_steps_locked(state: &mut NetworkFaultControllerState) {
  // Apply all script steps that are due at the current call count.
  let total_calls = state.total_calls;
  while let Some(step) = state.script_steps.first().cloned() {
    if step.on_call_count > total_calls {
      break;
    }

    match step.action {
      FaultScriptAction::Enable(rule) => {
        let id = state.next_fault_id;
        state.next_fault_id = state.next_fault_id.saturating_add(1);
        state.rules.push(ActiveNetworkFaultRule { id, rule });
      },
      FaultScriptAction::Disable { fault_id } => {
        state.rules.retain(|entry| entry.id != fault_id);
      },
      FaultScriptAction::ClearAll => {
        state.rules.clear();
      },
    }

    state.script_steps.remove(0);
  }
}

fn rule_matches(rule: &NetworkFaultRule, node_id: &str, operation: NetworkOperation) -> bool {
  if rule.operation != operation {
    return false;
  }

  rule
    .target_node_id
    .as_ref()
    .is_none_or(|target_node_id| target_node_id == node_id)
}

fn max_duration(current: Option<Duration>, candidate: Duration) -> Duration {
  current.map_or(candidate, |duration| duration.max(candidate))
}

fn describe_network_operation(operation: NetworkOperation) -> &'static str {
  match operation {
    NetworkOperation::ProduceBatches => "produce_batches",
  }
}

//
// InMemoryTestTransport
//

pub struct InMemoryTestTransport {
  fault_controller: NetworkFaultController,
  producer_transport: Arc<InMemoryProducerTransport>,
}

//
// InMemoryTestTransport
//

impl InMemoryTestTransport {
  pub fn new() -> Self {
    let fault_controller = NetworkFaultController::default();
    let producer_transport = Arc::new(InMemoryProducerTransport::new(fault_controller.clone()));
    Self {
      fault_controller,
      producer_transport,
    }
  }
}

//
// InMemoryTestTransport
//

impl Default for InMemoryTestTransport {
  fn default() -> Self {
    Self::new()
  }
}

#[async_trait]
impl BrokerTransport for InMemoryTestTransport {
  async fn bind_endpoint(&self, node_id: &str) -> Result<BrokerEndpoint> {
    Ok(BrokerEndpoint {
      node: BrokerNode {
        node_id: node_id.to_string().into(),
        address: format!("inmemory://{node_id}").into(),
      },
      binding: BrokerEndpointBinding::InMemory,
    })
  }

  fn register_write_engine(&self, node_id: &str, engine: Arc<dyn WriteEngine>) -> Result<()> {
    self
      .producer_transport
      .register_write_engine(node_id, engine);
    Ok(())
  }

  fn producer_transport(&self) -> Option<Arc<dyn ProducerBrokerTransport>> {
    Some(self.producer_transport.clone())
  }

  fn fault_controller(&self) -> Option<NetworkFaultController> {
    Some(self.fault_controller.clone())
  }

  async fn install_event_log(&self, event_log: TestEventLog) -> Result<()> {
    self.fault_controller.attach_event_log(event_log).await;
    Ok(())
  }
}

//
// InMemoryProducerTransport
//

struct InMemoryProducerTransport {
  engines: StdMutex<HashMap<String, Arc<dyn WriteEngine>>>,
  fault_controller: NetworkFaultController,
  reorder_coordinator: ReorderCoordinator,
}

//
// ReorderCoordinator
//

/// Coordinates a pair of faulted requests so the second reaches the broker before the first.
struct ReorderCoordinator {
  state: Mutex<ReorderCoordinatorState>,
}

#[derive(Default)]
struct ReorderCoordinatorState {
  next_generation: u64,
  waiters_by_node: HashMap<String, ReorderWaiter>,
}

struct ReorderWaiter {
  generation: u64,
  release: oneshot::Sender<()>,
}

#[derive(Debug, Eq, PartialEq)]
enum ReorderOutcome {
  FirstReleased,
  FirstTimedOut,
  SecondReleased,
}

impl ReorderCoordinator {
  #[cfg(test)]
  async fn rendezvous(&self, node_id: &str, max_wait: Duration) -> ReorderOutcome {
    self
      .rendezvous_with_scheduler(node_id, max_wait, None)
      .await
  }

  async fn rendezvous_with_scheduler(
    &self,
    node_id: &str,
    max_wait: Duration,
    scheduler: Option<ManualNetworkScheduler>,
  ) -> ReorderOutcome {
    let (generation, waiting) = {
      let mut state = self.state.lock().await;
      if let Some(waiter) = state.waiters_by_node.remove(node_id) {
        let _ = waiter.release.send(());
        return ReorderOutcome::SecondReleased;
      }

      let generation = state.next_generation;
      state.next_generation = state.next_generation.saturating_add(1);
      let (release, waiting) = oneshot::channel();
      state.waiters_by_node.insert(
        node_id.to_string(),
        ReorderWaiter {
          generation,
          release,
        },
      );
      (generation, waiting)
    };

    let released = if scheduler.is_some() {
      waiting.await.is_ok()
    } else {
      tokio::time::timeout(max_wait, waiting).await.is_ok()
    };
    if released {
      return ReorderOutcome::FirstReleased;
    }

    let mut state = self.state.lock().await;
    if state
      .waiters_by_node
      .get(node_id)
      .is_some_and(|waiter| waiter.generation == generation)
    {
      state.waiters_by_node.remove(node_id);
    }
    ReorderOutcome::FirstTimedOut
  }
}

//
// InMemoryProducerTransport
//

impl InMemoryProducerTransport {
  fn new(fault_controller: NetworkFaultController) -> Self {
    Self {
      engines: StdMutex::new(HashMap::new()),
      fault_controller,
      reorder_coordinator: ReorderCoordinator {
        state: Mutex::new(ReorderCoordinatorState::default()),
      },
    }
  }

  fn register_write_engine(&self, node_id: &str, engine: Arc<dyn WriteEngine>) {
    let mut guard = self
      .engines
      .lock()
      .expect("in-memory transport engine lock poisoned");
    guard.insert(node_id.to_string(), engine);
  }

  fn write_engine_for(&self, broker_address: &str) -> Result<Arc<dyn WriteEngine>> {
    let node_id = broker_address
      .strip_prefix("inmemory://")
      .ok_or_else(|| anyhow!("in-memory transport requires inmemory:// address"))?;
    let guard = self
      .engines
      .lock()
      .expect("in-memory transport engine lock poisoned");
    guard
      .get(node_id)
      .cloned()
      .ok_or_else(|| anyhow!("in-memory broker not registered: {node_id}"))
  }
}

#[async_trait]
impl ProducerBrokerTransport for InMemoryProducerTransport {
  async fn produce_batches(
    &self,
    broker_address: &Chars,
    request: ProduceBatchesRequest,
    request_timeout: Duration,
    compression: ProducerCompression,
  ) -> Result<ProduceBatchesResponse> {
    // This transport directly forwards decoded protobuf requests to the in-memory write engine.
    // Compression only applies to the gRPC wire representation.
    let _ = compression;

    let node_id = broker_address
      .as_str()
      .strip_prefix("inmemory://")
      .ok_or_else(|| anyhow!("in-memory transport requires inmemory:// address"))?;

    let effects = self
      .fault_controller
      .effects_for_call(node_id, NetworkOperation::ProduceBatches)
      .await;

    if effects.partition {
      return Err(anyhow!(
        "in-memory transport partitioned for node {node_id}"
      ));
    }
    if effects.drop_request {
      return Err(anyhow!(
        "in-memory transport dropped request for node {node_id}"
      ));
    }
    if let Some(delay) = effects.delay {
      self.fault_controller.sleep(delay).await;
    }
    if let Some(delay) = effects.reorder_delay {
      let scheduler = self.fault_controller.manual_scheduler().await;
      let (status, detail) = match self
        .reorder_coordinator
        .rendezvous_with_scheduler(node_id, delay, scheduler)
        .await
      {
        ReorderOutcome::FirstReleased => ("reorder_released", Some("role=first".to_string())),
        ReorderOutcome::FirstTimedOut => ("reorder_expired", Some("role=first".to_string())),
        ReorderOutcome::SecondReleased => ("reorder_released", Some("role=second".to_string())),
      };
      self
        .fault_controller
        .record_produce_event(node_id, status, detail)
        .await;
    }
    if let Some(duration) = effects.timeout {
      self.fault_controller.sleep(duration).await;
      return Err(anyhow!("in-memory transport timed out for node {node_id}"));
    }

    let write_engine = self.write_engine_for(broker_address.as_str())?;
    let copies = effects.duplicate_copies.max(1);
    let mut first_response = None;
    for _ in 0 .. copies {
      // Duplicate faults replay the same request into the write engine and return the
      // first response to keep producer semantics stable.
      let mut results = Vec::with_capacity(request.batches.len());
      for batch in &request.batches {
        let write_request = WriteRequest {
          topic: batch.topic.clone(),
          virtual_partition_id: batch.virtual_partition_id,
          records: batch.records.clone(),
        };

        let response =
          match tokio::time::timeout(request_timeout, write_engine.produce_batch(write_request))
            .await
          {
            Ok(Ok(_write_response)) => ProduceBatchResponse {
              status: ProduceStatus::PRODUCE_STATUS_OK.into(),
              error_message: String::new().into(),
              ..Default::default()
            },
            Ok(Err(error)) => ProduceBatchResponse {
              status: error.status().into(),
              error_message: write_error_message(&error).into(),
              ..Default::default()
            },
            Err(_) => return Err(anyhow!("in-memory transport timed out for node {node_id}")),
          };
        results.push(response);
      }

      if first_response.is_none() {
        first_response = Some(ProduceBatchesResponse {
          results,
          ..Default::default()
        });
      }
    }

    let event_log = self.fault_controller.inner.lock().await.event_log.clone();
    if let Some(event_log) = event_log {
      event_log
        .record(
          "transport",
          "produce_batches",
          Some(node_id.to_string()),
          "ok",
          Some(format!("copies={copies}")),
        )
        .await;
    }

    if effects.drop_response {
      self
        .fault_controller
        .record_produce_event(
          node_id,
          "response_dropped",
          Some("broker_completed=true".to_string()),
        )
        .await;
      return Err(anyhow!(
        "in-memory transport dropped response after broker completed request for node {node_id}"
      ));
    }

    first_response.ok_or_else(|| anyhow!("in-memory transport did not produce a response"))
  }
}

fn write_error_message(error: &WriteError) -> String {
  match error {
    WriteError::Internal(inner) => inner.to_string(),
    other => other.to_string(),
  }
}
