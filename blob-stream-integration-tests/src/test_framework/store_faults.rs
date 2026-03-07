use crate::test_framework::event_log::TestEventLog;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_blob_store::{BlobKey, BlobStore, ByteRange};
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  ConsumerGroupReleaseOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  MetadataStore,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SegmentMetadata,
  SequenceReservationOutcome,
};
use blob_stream_types::{CommittedCursor, SnowflakeId, TopicWindowKey};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

//
// StoreFaultDomain
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreFaultDomain {
  Blob,
  Metadata,
  ProducerLease,
  ConsumerLease,
}

//
// StoreFaultOperation
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreFaultOperation {
  BlobPut,
  BlobGetRange,
  MetadataWriteSegment,
  MetadataScanWindow,
  ProducerAcquireLease,
  ProducerHeartbeatLease,
  ProducerReserveSequences,
  ProducerReleaseLease,
  ConsumerAssignPartition,
  ConsumerHeartbeatPartition,
  ConsumerCommitCursor,
  ConsumerReleasePartition,
}

//
// StoreFaultAction
//

#[derive(Clone, Debug)]
pub enum StoreFaultAction {
  Fail { message: String },
  Delay(Duration),
  Timeout(Duration),
  StaleRead,
  DelayedVisibility(Duration),
}

//
// StoreFaultRule
//

#[derive(Clone, Debug)]
pub struct StoreFaultRule {
  pub domain: StoreFaultDomain,
  pub operation: StoreFaultOperation,
  pub key_pattern: Option<String>,
  pub action: StoreFaultAction,
  pub remaining_hits: Option<u64>,
}

//
// StoreFaultScriptAction
//

#[derive(Clone, Debug)]
pub enum StoreFaultScriptAction {
  Enable(StoreFaultRule),
  Disable { fault_id: u64 },
  ClearAll,
}

//
// StoreFaultScriptStep
//

#[derive(Clone, Debug)]
pub struct StoreFaultScriptStep {
  pub on_call_count: u64,
  pub action: StoreFaultScriptAction,
}

//
// StoreFaultEvent
//

#[derive(Clone, Debug)]
pub struct StoreFaultEvent {
  pub domain: StoreFaultDomain,
  pub operation: StoreFaultOperation,
  pub key: String,
  pub call_count: u64,
  pub action: Option<StoreFaultAction>,
}

//
// StoreFaultController
//

#[derive(Clone)]
pub struct StoreFaultController {
  inner: Arc<Mutex<StoreFaultControllerState>>,
}

//
// StoreFaultControllerState
//

#[derive(Default)]
struct StoreFaultControllerState {
  next_fault_id: u64,
  total_calls: u64,
  rules: Vec<ActiveStoreFaultRule>,
  script_steps: Vec<StoreFaultScriptStep>,
  events: Vec<StoreFaultEvent>,
  delayed_metadata: Vec<DelayedMetadataEntry>,
  event_log: Option<TestEventLog>,
}

//
// ActiveStoreFaultRule
//

struct ActiveStoreFaultRule {
  id: u64,
  rule: StoreFaultRule,
}

//
// DelayedMetadataEntry
//

struct DelayedMetadataEntry {
  visible_at: Instant,
  metadata: SegmentMetadata,
}

//
// StoreFaultEffects
//

#[derive(Default)]
struct StoreFaultEffects {
  fail_message: Option<String>,
  delay: Option<Duration>,
  timeout: Option<Duration>,
  stale_read: bool,
  delayed_visibility: Option<Duration>,
  first_action: Option<StoreFaultAction>,
}

impl Default for StoreFaultController {
  fn default() -> Self {
    Self {
      inner: Arc::new(Mutex::new(StoreFaultControllerState::default())),
    }
  }
}

//
// StoreFaultController
//

impl StoreFaultController {
  pub async fn attach_event_log(&self, event_log: TestEventLog) {
    self.inner.lock().await.event_log = Some(event_log);
  }

  pub async fn enable_fault(&self, rule: StoreFaultRule) -> u64 {
    let mut guard = self.inner.lock().await;
    let id = guard.next_fault_id;
    guard.next_fault_id = guard.next_fault_id.saturating_add(1);
    guard.rules.push(ActiveStoreFaultRule { id, rule });
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
    guard.delayed_metadata.clear();
  }

  pub async fn load_script(&self, mut steps: Vec<StoreFaultScriptStep>) {
    steps.sort_by_key(|step| step.on_call_count);
    let mut guard = self.inner.lock().await;
    guard.script_steps = steps;
  }

  pub async fn events(&self) -> Vec<StoreFaultEvent> {
    self.inner.lock().await.events.clone()
  }

  pub async fn clear_events(&self) {
    self.inner.lock().await.events.clear();
  }

  async fn effects_for_call(
    &self,
    domain: StoreFaultDomain,
    operation: StoreFaultOperation,
    key: &str,
  ) -> StoreFaultEffects {
    let mut guard = self.inner.lock().await;
    guard.total_calls = guard.total_calls.saturating_add(1);
    apply_script_steps_locked(&mut guard);

    let mut effects = StoreFaultEffects::default();
    for entry in &mut guard.rules {
      if !store_rule_matches(&entry.rule, domain, operation, key) {
        continue;
      }

      let action = entry.rule.action.clone();
      if effects.first_action.is_none() {
        effects.first_action = Some(action.clone());
      }
      match action {
        StoreFaultAction::Fail { message } => {
          effects.fail_message = Some(message);
        },
        StoreFaultAction::Delay(duration) => {
          effects.delay = Some(max_duration(effects.delay, duration));
        },
        StoreFaultAction::Timeout(duration) => {
          effects.timeout = Some(max_duration(effects.timeout, duration));
        },
        StoreFaultAction::StaleRead => {
          effects.stale_read = true;
        },
        StoreFaultAction::DelayedVisibility(duration) => {
          effects.delayed_visibility = Some(max_duration(effects.delayed_visibility, duration));
        },
      }

      if let Some(remaining) = entry.rule.remaining_hits.as_mut() {
        *remaining = remaining.saturating_sub(1);
      }
    }

    guard
      .rules
      .retain(|entry| entry.rule.remaining_hits != Some(0));
    let call_count = guard.total_calls;
    guard.events.push(StoreFaultEvent {
      domain,
      operation,
      key: key.to_string(),
      call_count,
      action: effects.first_action.clone(),
    });
    let event_log = guard.event_log.clone();
    let status = effects
      .first_action
      .as_ref()
      .map_or_else(|| "pass".to_string(), |_| "fault_applied".to_string());
    let detail = effects
      .first_action
      .as_ref()
      .map(describe_store_fault_action);
    drop(guard);

    if let Some(event_log) = event_log {
      event_log
        .record(
          "store",
          describe_store_operation(operation),
          Some(key.to_string()),
          status,
          detail,
        )
        .await;
    }

    effects
  }

  async fn record_operation_outcome(
    &self,
    operation: StoreFaultOperation,
    key: String,
    status: &str,
    detail: Option<String>,
  ) {
    let event_log = self.inner.lock().await.event_log.clone();
    if let Some(event_log) = event_log {
      event_log
        .record(
          "store",
          describe_store_operation(operation),
          Some(key),
          status,
          detail,
        )
        .await;
    }
  }

  async fn delay_metadata_visibility(&self, metadata: SegmentMetadata, delay: Duration) {
    let visible_at = Instant::now() + delay;
    let mut guard = self.inner.lock().await;
    guard.delayed_metadata.push(DelayedMetadataEntry {
      visible_at,
      metadata,
    });
  }

  async fn visible_metadata_for_window(&self, window: &TopicWindowKey) -> Vec<SegmentMetadata> {
    let now = Instant::now();
    let mut guard = self.inner.lock().await;
    let mut visible = Vec::new();
    let mut future = Vec::new();

    for entry in guard.delayed_metadata.drain(..) {
      if entry.visible_at <= now && entry.metadata.window == *window {
        visible.push(entry.metadata);
      } else {
        future.push(entry);
      }
    }

    guard.delayed_metadata = future;
    visible
  }
}

fn apply_script_steps_locked(state: &mut StoreFaultControllerState) {
  let total_calls = state.total_calls;
  while let Some(step) = state.script_steps.first().cloned() {
    if step.on_call_count > total_calls {
      break;
    }

    match step.action {
      StoreFaultScriptAction::Enable(rule) => {
        let id = state.next_fault_id;
        state.next_fault_id = state.next_fault_id.saturating_add(1);
        state.rules.push(ActiveStoreFaultRule { id, rule });
      },
      StoreFaultScriptAction::Disable { fault_id } => {
        state.rules.retain(|entry| entry.id != fault_id);
      },
      StoreFaultScriptAction::ClearAll => {
        state.rules.clear();
        state.delayed_metadata.clear();
      },
    }

    state.script_steps.remove(0);
  }
}

fn store_rule_matches(
  rule: &StoreFaultRule,
  domain: StoreFaultDomain,
  operation: StoreFaultOperation,
  key: &str,
) -> bool {
  if rule.domain != domain || rule.operation != operation {
    return false;
  }

  rule
    .key_pattern
    .as_ref()
    .is_none_or(|pattern| key.contains(pattern))
}

fn max_duration(current: Option<Duration>, candidate: Duration) -> Duration {
  current.map_or(candidate, |duration| duration.max(candidate))
}

fn describe_store_operation(operation: StoreFaultOperation) -> &'static str {
  match operation {
    StoreFaultOperation::BlobPut => "blob_put",
    StoreFaultOperation::BlobGetRange => "blob_get_range",
    StoreFaultOperation::MetadataWriteSegment => "metadata_write_segment",
    StoreFaultOperation::MetadataScanWindow => "metadata_scan_window",
    StoreFaultOperation::ProducerAcquireLease => "producer_acquire_lease",
    StoreFaultOperation::ProducerHeartbeatLease => "producer_heartbeat_lease",
    StoreFaultOperation::ProducerReserveSequences => "producer_reserve_sequences",
    StoreFaultOperation::ProducerReleaseLease => "producer_release_lease",
    StoreFaultOperation::ConsumerAssignPartition => "consumer_assign_partition",
    StoreFaultOperation::ConsumerHeartbeatPartition => "consumer_heartbeat_partition",
    StoreFaultOperation::ConsumerCommitCursor => "consumer_commit_cursor",
    StoreFaultOperation::ConsumerReleasePartition => "consumer_release_partition",
  }
}

fn describe_store_fault_action(action: &StoreFaultAction) -> String {
  match action {
    StoreFaultAction::Fail { message } => format!("fail:{message}"),
    StoreFaultAction::Delay(duration) => format!("delay:{}ms", duration.as_millis()),
    StoreFaultAction::Timeout(duration) => format!("timeout:{}ms", duration.as_millis()),
    StoreFaultAction::StaleRead => "stale_read".to_string(),
    StoreFaultAction::DelayedVisibility(duration) => {
      format!("delayed_visibility:{}ms", duration.as_millis())
    },
  }
}

//
// FaultInjectedBlobStore
//

pub struct FaultInjectedBlobStore {
  inner: Arc<dyn BlobStore>,
  controller: StoreFaultController,
}

impl FaultInjectedBlobStore {
  pub fn new(inner: Arc<dyn BlobStore>, controller: StoreFaultController) -> Self {
    Self { inner, controller }
  }
}

#[async_trait]
impl BlobStore for FaultInjectedBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::Blob,
        StoreFaultOperation::BlobPut,
        key.as_str(),
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!("blob put timed out for key {}", key.as_str()));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "blob put fault for key {}: {}",
        key.as_str(),
        message
      ));
    }

    let result = self.inner.put(key, payload).await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::BlobPut,
        key.as_str().to_string(),
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().err().map(std::string::ToString::to_string),
      )
      .await;
    result
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> Result<Bytes> {
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::Blob,
        StoreFaultOperation::BlobGetRange,
        key.as_str(),
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!("blob get_range timed out for key {}", key.as_str()));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "blob get_range fault for key {} [{}-{}): {}",
        key.as_str(),
        range.start,
        range.end,
        message
      ));
    }

    let result = self.inner.get_range(key, range).await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::BlobGetRange,
        key.as_str().to_string(),
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().err().map(std::string::ToString::to_string),
      )
      .await;
    result
  }
}

//
// FaultInjectedMetadataStore
//

pub struct FaultInjectedMetadataStore {
  inner: Arc<dyn MetadataStore>,
  controller: StoreFaultController,
}

impl FaultInjectedMetadataStore {
  pub fn new(inner: Arc<dyn MetadataStore>, controller: StoreFaultController) -> Self {
    Self { inner, controller }
  }
}

#[async_trait]
impl MetadataStore for FaultInjectedMetadataStore {
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()> {
    let key = metadata.window.format();
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::Metadata,
        StoreFaultOperation::MetadataWriteSegment,
        &key,
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!("metadata write timed out for window {key}"));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!("metadata write fault for window {key}: {message}"));
    }

    if let Some(delay) = effects.delayed_visibility {
      self
        .controller
        .delay_metadata_visibility(metadata, delay)
        .await;
      return Ok(());
    }

    let result = self.inner.write_segment(metadata).await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::MetadataWriteSegment,
        key,
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().err().map(std::string::ToString::to_string),
      )
      .await;
    result
  }

  async fn scan_window(
    &self,
    window: &TopicWindowKey,
    min_snowflake_id: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>> {
    let key = window.format();
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::Metadata,
        StoreFaultOperation::MetadataScanWindow,
        &key,
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!("metadata scan timed out for window {key}"));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!("metadata scan fault for window {key}: {message}"));
    }

    let mut visible = self.controller.visible_metadata_for_window(window).await;
    if effects.stale_read {
      return Ok(visible);
    }

    let mut scanned = self.inner.scan_window(window, min_snowflake_id).await?;
    scanned.append(&mut visible);
    self
      .controller
      .record_operation_outcome(StoreFaultOperation::MetadataScanWindow, key, "ok", None)
      .await;
    Ok(scanned)
  }
}

//
// FaultInjectedProducerPartitionLeaseStore
//

pub struct FaultInjectedProducerPartitionLeaseStore {
  inner: Arc<dyn ProducerPartitionLeaseStore>,
  controller: StoreFaultController,
}

impl FaultInjectedProducerPartitionLeaseStore {
  pub fn new(
    inner: Arc<dyn ProducerPartitionLeaseStore>,
    controller: StoreFaultController,
  ) -> Self {
    Self { inner, controller }
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for FaultInjectedProducerPartitionLeaseStore {
  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseAcquireOutcome> {
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ProducerLease,
        StoreFaultOperation::ProducerAcquireLease,
        &key.format(),
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "producer acquire_lease timed out for key {}",
        key.format()
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "producer acquire_lease fault for key {}: {message}",
        key.format()
      ));
    }

    let result = self
      .inner
      .acquire_lease(key, holder_id, now_ts_ms, lease_duration_ms)
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ProducerAcquireLease,
        result.as_ref().map_or_else(
          |_| "unknown".to_string(),
          |outcome| match outcome {
            LeaseAcquireOutcome::Acquired(lease) | LeaseAcquireOutcome::HeldByOther(lease) => {
              lease.key.format()
            },
          },
        ),
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          LeaseAcquireOutcome::Acquired(_) => "acquired".to_string(),
          LeaseAcquireOutcome::HeldByOther(_) => "held_by_other".to_string(),
        }),
      )
      .await;
    result
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseHeartbeatOutcome> {
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ProducerLease,
        StoreFaultOperation::ProducerHeartbeatLease,
        &key.format(),
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "producer heartbeat_lease timed out for key {}",
        key.format()
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "producer heartbeat_lease fault for key {}: {message}",
        key.format()
      ));
    }

    let result = self
      .inner
      .heartbeat_lease(key, holder_id, now_ts_ms, lease_duration_ms)
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ProducerHeartbeatLease,
        key.format(),
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          LeaseHeartbeatOutcome::Renewed(_) => "renewed".to_string(),
          LeaseHeartbeatOutcome::HeldByOther(_) => "held_by_other".to_string(),
          LeaseHeartbeatOutcome::Expired => "expired".to_string(),
        }),
      )
      .await;
    result
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome> {
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ProducerLease,
        StoreFaultOperation::ProducerReserveSequences,
        &key.format(),
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "producer reserve_sequences timed out for key {}",
        key.format()
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "producer reserve_sequences fault for key {}: {message}",
        key.format()
      ));
    }

    let result = self
      .inner
      .reserve_sequences(key, holder_id, now_ts_ms, reservation_size)
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ProducerReserveSequences,
        key.format(),
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          SequenceReservationOutcome::Reserved(_) => "reserved".to_string(),
          SequenceReservationOutcome::HeldByOther(_) => "held_by_other".to_string(),
          SequenceReservationOutcome::Expired => "expired".to_string(),
        }),
      )
      .await;
    result
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
  ) -> Result<LeaseReleaseOutcome> {
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ProducerLease,
        StoreFaultOperation::ProducerReleaseLease,
        &key.format(),
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "producer release_lease timed out for key {}",
        key.format()
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "producer release_lease fault for key {}: {message}",
        key.format()
      ));
    }

    let result = self.inner.release_lease(key, holder_id, now_ts_ms).await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ProducerReleaseLease,
        key.format(),
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          LeaseReleaseOutcome::Released => "released".to_string(),
          LeaseReleaseOutcome::HeldByOther(_) => "held_by_other".to_string(),
          LeaseReleaseOutcome::Expired => "expired".to_string(),
        }),
      )
      .await;
    result
  }
}

//
// FaultInjectedConsumerGroupLeaseStore
//

pub struct FaultInjectedConsumerGroupLeaseStore {
  inner: Arc<dyn ConsumerGroupLeaseStore>,
  controller: StoreFaultController,
}

//
// FaultInjectedConsumerGroupMembershipStore
//

pub struct FaultInjectedConsumerGroupMembershipStore {
  inner: Arc<dyn ConsumerGroupMembershipStore>,
  controller: StoreFaultController,
}

impl FaultInjectedConsumerGroupMembershipStore {
  pub fn new(
    inner: Arc<dyn ConsumerGroupMembershipStore>,
    controller: StoreFaultController,
  ) -> Self {
    Self { inner, controller }
  }
}

#[async_trait]
impl ConsumerGroupMembershipStore for FaultInjectedConsumerGroupMembershipStore {
  async fn register_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> Result<()> {
    let _ = &self.controller;
    self
      .inner
      .register_member(topic, group_id, member_id, now_ts_ms, ttl_ms)
      .await
  }

  async fn heartbeat_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> Result<()> {
    let _ = &self.controller;
    self
      .inner
      .heartbeat_member(topic, group_id, member_id, now_ts_ms, ttl_ms)
      .await
  }

  async fn deregister_member(&self, topic: &str, group_id: &str, member_id: &str) -> Result<()> {
    let _ = &self.controller;
    self
      .inner
      .deregister_member(topic, group_id, member_id)
      .await
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now_ts_ms: i64,
  ) -> Result<Vec<String>> {
    let _ = &self.controller;
    self
      .inner
      .list_active_members(topic, group_id, now_ts_ms)
      .await
  }
}

impl FaultInjectedConsumerGroupLeaseStore {
  pub fn new(inner: Arc<dyn ConsumerGroupLeaseStore>, controller: StoreFaultController) -> Self {
    Self { inner, controller }
  }
}

#[async_trait]
impl ConsumerGroupLeaseStore for FaultInjectedConsumerGroupLeaseStore {
  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<ConsumerGroupAssignmentOutcome> {
    let key_str = format!("{}#{}", key.partition_key(), key.sort_key());
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ConsumerLease,
        StoreFaultOperation::ConsumerAssignPartition,
        &key_str,
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "consumer assign_partition timed out for key {key_str}"
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "consumer assign_partition fault for key {key_str}: {message}"
      ));
    }

    let result = self
      .inner
      .assign_partition(key, owner_id, generation, now_ts_ms, lease_duration_ms)
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ConsumerAssignPartition,
        key_str,
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          ConsumerGroupAssignmentOutcome::Assigned(_) => "assigned".to_string(),
          ConsumerGroupAssignmentOutcome::HeldByOther(_) => "held_by_other".to_string(),
        }),
      )
      .await;
    result
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
    committed_cursor: Option<CommittedCursor>,
  ) -> Result<ConsumerGroupHeartbeatOutcome> {
    let key_str = format!("{}#{}", key.partition_key(), key.sort_key());
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ConsumerLease,
        StoreFaultOperation::ConsumerHeartbeatPartition,
        &key_str,
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "consumer heartbeat_partition timed out for key {key_str}"
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "consumer heartbeat_partition fault for key {key_str}: {message}"
      ));
    }

    let result = self
      .inner
      .heartbeat_partition(
        key,
        owner_id,
        generation,
        now_ts_ms,
        lease_duration_ms,
        committed_cursor,
      )
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ConsumerHeartbeatPartition,
        key_str,
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          ConsumerGroupHeartbeatOutcome::Renewed(_) => "renewed".to_string(),
          ConsumerGroupHeartbeatOutcome::HeldByOther(_) => "held_by_other".to_string(),
          ConsumerGroupHeartbeatOutcome::Expired => "expired".to_string(),
        }),
      )
      .await;
    result
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    committed_cursor: CommittedCursor,
  ) -> Result<ConsumerGroupCommitOutcome> {
    let key_str = format!("{}#{}", key.partition_key(), key.sort_key());
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ConsumerLease,
        StoreFaultOperation::ConsumerCommitCursor,
        &key_str,
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "consumer commit_cursor timed out for key {key_str}"
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "consumer commit_cursor fault for key {key_str}: {message}"
      ));
    }

    let result = self
      .inner
      .commit_cursor(key, owner_id, generation, now_ts_ms, committed_cursor)
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ConsumerCommitCursor,
        key_str,
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          ConsumerGroupCommitOutcome::Committed(_) => "committed".to_string(),
          ConsumerGroupCommitOutcome::HeldByOther(_) => "held_by_other".to_string(),
          ConsumerGroupCommitOutcome::Expired => "expired".to_string(),
        }),
      )
      .await;
    result
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
  ) -> Result<ConsumerGroupReleaseOutcome> {
    let key_str = format!("{}#{}", key.partition_key(), key.sort_key());
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ConsumerReleasePartition,
        key_str.clone(),
        "attempt",
        None,
      )
      .await;

    let result = self
      .inner
      .release_partition(key, owner_id, generation, now_ts_ms)
      .await;

    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ConsumerReleasePartition,
        key_str,
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          ConsumerGroupReleaseOutcome::Released => "released".to_string(),
          ConsumerGroupReleaseOutcome::HeldByOther(_) => "held_by_other".to_string(),
          ConsumerGroupReleaseOutcome::Expired => "expired".to_string(),
        }),
      )
      .await;
    result
  }
}
