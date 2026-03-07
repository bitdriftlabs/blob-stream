use crate::framework::event_log::TestEventLog;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream::write::{WriteEngine, WriteError, WriteRequest};
use blob_stream_broker_discovery::BrokerNode;
use blob_stream_producer::BrokerTransport as ProducerBrokerTransport;
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchResponse,
  ProduceStatus,
};
use blob_stream_types::Record;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep as tokio_sleep;

//
// BrokerEndpointBinding
//

#[allow(dead_code)]
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

  #[allow(dead_code)]
  fn register_write_engine(&self, _node_id: &str, _engine: Arc<dyn WriteEngine>) -> Result<()> {
    Ok(())
  }

  #[allow(dead_code)]
  fn producer_transport(&self) -> Option<Arc<dyn ProducerBrokerTransport>> {
    None
  }

  #[allow(dead_code)]
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
      node_id: node_id.to_string(),
      address: listener.local_addr()?.to_string(),
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

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkOperation {
  ProduceBatch,
}

//
// NetworkFault
//

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkFault {
  Drop,
  Delay(Duration),
  Duplicate { copies: u32 },
  Reorder { delay: Duration },
  Timeout(Duration),
  Partition,
}

//
// NetworkFaultRule
//

#[allow(dead_code)]
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

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum FaultScriptAction {
  Enable(NetworkFaultRule),
  Disable { fault_id: u64 },
  ClearAll,
}

//
// FaultScriptStep
//

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct FaultScriptStep {
  pub on_call_count: u64,
  pub action: FaultScriptAction,
}

//
// NetworkFaultController
//

#[allow(dead_code)]
#[derive(Clone)]
pub struct NetworkFaultController {
  inner: Arc<Mutex<NetworkFaultControllerState>>,
}

//
// NetworkFaultControllerState
//

#[allow(dead_code)]
#[derive(Default)]
struct NetworkFaultControllerState {
  next_fault_id: u64,
  total_calls: u64,
  rules: Vec<ActiveNetworkFaultRule>,
  script_steps: Vec<FaultScriptStep>,
  event_log: Option<TestEventLog>,
}

//
// ActiveNetworkFaultRule
//

#[allow(dead_code)]
struct ActiveNetworkFaultRule {
  id: u64,
  rule: NetworkFaultRule,
}

//
// NetworkFaultEffects
//

#[allow(dead_code)]
#[derive(Default)]
struct NetworkFaultEffects {
  drop_request: bool,
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

#[allow(dead_code)]
impl NetworkFaultController {
  pub async fn attach_event_log(&self, event_log: TestEventLog) {
    self.inner.lock().await.event_log = Some(event_log);
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
}

#[allow(dead_code)]
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

#[allow(dead_code)]
fn max_duration(current: Option<Duration>, candidate: Duration) -> Duration {
  current.map_or(candidate, |duration| duration.max(candidate))
}

fn describe_network_operation(operation: NetworkOperation) -> &'static str {
  match operation {
    NetworkOperation::ProduceBatch => "produce_batch",
  }
}

//
// InMemoryTestTransport
//

#[allow(dead_code)]
pub struct InMemoryTestTransport {
  fault_controller: NetworkFaultController,
  producer_transport: Arc<InMemoryProducerTransport>,
}

//
// InMemoryTestTransport
//

#[allow(dead_code)]
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

#[allow(dead_code)]
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
        node_id: node_id.to_string(),
        address: format!("inmemory://{node_id}"),
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

#[allow(dead_code)]
struct InMemoryProducerTransport {
  engines: StdMutex<HashMap<String, Arc<dyn WriteEngine>>>,
  fault_controller: NetworkFaultController,
}

//
// InMemoryProducerTransport
//

#[allow(dead_code)]
impl InMemoryProducerTransport {
  fn new(fault_controller: NetworkFaultController) -> Self {
    Self {
      engines: StdMutex::new(HashMap::new()),
      fault_controller,
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
  async fn produce_batch(
    &self,
    broker_address: &str,
    request: ProduceBatchRequest,
  ) -> Result<ProduceBatchResponse> {
    let node_id = broker_address
      .strip_prefix("inmemory://")
      .ok_or_else(|| anyhow!("in-memory transport requires inmemory:// address"))?
      .to_string();

    let effects = self
      .fault_controller
      .effects_for_call(&node_id, NetworkOperation::ProduceBatch)
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
      tokio_sleep(delay).await;
    }
    if let Some(delay) = effects.reorder_delay {
      tokio_sleep(delay).await;
    }
    if let Some(duration) = effects.timeout {
      tokio_sleep(duration).await;
      return Err(anyhow!("in-memory transport timed out for node {node_id}"));
    }

    let write_engine = self.write_engine_for(broker_address)?;
    let copies = effects.duplicate_copies.max(1);
    let mut first_response = None;
    for _ in 0 .. copies {
      // Duplicate faults replay the same request into the write engine and return the
      // first response to keep producer semantics stable.
      let write_request = WriteRequest {
        topic: request.topic.to_string(),
        virtual_partition_id: request.virtual_partition_id,
        records: request
          .records
          .iter()
          .map(|record| Record::new(record.payload.to_vec(), record.event_ts_ms))
          .collect(),
      };

      let response = match write_engine.produce_batch(write_request).await {
        Ok(_write_response) => ProduceBatchResponse {
          status: ProduceStatus::PRODUCE_STATUS_OK.into(),
          error_message: String::new().into(),
          ..Default::default()
        },
        Err(error) => ProduceBatchResponse {
          status: error.status().into(),
          error_message: write_error_message(&error).into(),
          ..Default::default()
        },
      };

      if first_response.is_none() {
        first_response = Some(response);
      }
    }

    let event_log = self.fault_controller.inner.lock().await.event_log.clone();
    if let Some(event_log) = event_log {
      event_log
        .record(
          "transport",
          "produce_batch",
          Some(node_id),
          "ok",
          Some(format!("copies={copies}")),
        )
        .await;
    }

    first_response.ok_or_else(|| anyhow!("in-memory transport did not produce a response"))
  }
}

#[allow(dead_code)]
fn write_error_message(error: &WriteError) -> String {
  match error {
    WriteError::Internal(inner) => inner.to_string(),
    other => other.to_string(),
  }
}
