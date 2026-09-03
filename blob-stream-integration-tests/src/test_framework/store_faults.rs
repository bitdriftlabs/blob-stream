use crate::test_framework::event_log::TestEventLog;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_blob_store::{
  BlobCacheAdmission,
  BlobKey,
  BlobStore,
  BlobStoreError,
  BlobStoreResult,
  ByteRange,
};
use blob_stream_metadata_store::{
  ConsumerGroupArmFreshStartOutcome,
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMember,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLease,
  ConsumerGroupPlannerLeaseOutcome,
  ConsumerGroupReleaseOutcome,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  MetadataReadConsistency,
  MetadataStore,
  MetadataWriteResult,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SegmentMetadata,
  SequenceReservationOutcome,
};
use blob_stream_types::{CommittedCursor, SnowflakeId, TopicWindowKey};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

#[cfg(test)]
#[path = "./store_faults_test.rs"]
mod tests;

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
  BlobGetWithCacheAdmission,
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
  ConsumerListGroupLeases,
  ConsumerMembershipHeartbeat,
  ConsumerDeregisterMember,
  ConsumerGetAssignmentPlan,
  ConsumerGetPlannerLease,
  ConsumerAcquireOrRenewPlanner,
  ConsumerReleasePlanner,
  ConsumerPublishAssignmentPlan,
}

//
// StoreFaultAction
//

#[derive(Clone, Debug)]
pub enum StoreFaultAction {
  Fail { message: String },
  NotFound,
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
  not_found: bool,
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
        StoreFaultAction::NotFound => {
          effects.not_found = true;
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
    StoreFaultOperation::BlobGetWithCacheAdmission => "blob_get_with_cache_admission",
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
    StoreFaultOperation::ConsumerListGroupLeases => "consumer_list_group_leases",
    StoreFaultOperation::ConsumerMembershipHeartbeat => "consumer_membership_heartbeat",
    StoreFaultOperation::ConsumerDeregisterMember => "consumer_deregister_member",
    StoreFaultOperation::ConsumerGetAssignmentPlan => "consumer_get_assignment_plan",
    StoreFaultOperation::ConsumerGetPlannerLease => "consumer_get_planner_lease",
    StoreFaultOperation::ConsumerAcquireOrRenewPlanner => "consumer_acquire_or_renew_planner",
    StoreFaultOperation::ConsumerReleasePlanner => "consumer_release_planner",
    StoreFaultOperation::ConsumerPublishAssignmentPlan => "consumer_publish_assignment_plan",
  }
}

fn describe_store_fault_action(action: &StoreFaultAction) -> String {
  match action {
    StoreFaultAction::Fail { message } => format!("fail:{message}"),
    StoreFaultAction::NotFound => "not_found".to_string(),
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

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
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
    if effects.not_found {
      return Err(BlobStoreError::NotFound {
        key: key.as_str().to_string(),
      });
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(BlobStoreError::Read {
        key: key.as_str().to_string(),
        source: anyhow!("blob get_range timed out for key {}", key.as_str()),
      });
    }
    if let Some(message) = effects.fail_message {
      return Err(BlobStoreError::Read {
        key: key.as_str().to_string(),
        source: anyhow!(
          "blob get_range fault for key {} [{}-{}): {}",
          key.as_str(),
          range.start,
          range.end,
          message
        ),
      });
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

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::Blob,
        StoreFaultOperation::BlobGetWithCacheAdmission,
        key.as_str(),
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if effects.not_found {
      return Err(BlobStoreError::NotFound {
        key: key.as_str().to_string(),
      });
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(BlobStoreError::Read {
        key: key.as_str().to_string(),
        source: anyhow!(
          "blob cache admission read timed out for key {}",
          key.as_str()
        ),
      });
    }
    if let Some(message) = effects.fail_message {
      return Err(BlobStoreError::Read {
        key: key.as_str().to_string(),
        source: anyhow!(
          "blob cache admission read fault for key {}: {}",
          key.as_str(),
          message
        ),
      });
    }

    let result = self.inner.get_with_cache_admission(key, admission).await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::BlobGetWithCacheAdmission,
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

  async fn scan_window_with_bound(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
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
    visible.retain(|segment| {
      min_snowflake.is_none_or(|min_snowflake| segment.snowflake_id >= min_snowflake)
    });
    if effects.stale_read {
      self
        .controller
        .record_operation_outcome(StoreFaultOperation::MetadataScanWindow, key, "ok", None)
        .await;
      return Ok(visible);
    }

    let mut scanned = self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await?;
    scanned.append(&mut visible);
    self
      .controller
      .record_operation_outcome(StoreFaultOperation::MetadataScanWindow, key, "ok", None)
      .await;
    Ok(scanned)
  }
}

#[async_trait]
impl MetadataStore for FaultInjectedMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[blob_stream_metadata_store::ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult {
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
      return Err(anyhow!("metadata write timed out for window {key}").into());
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!("metadata write fault for window {key}: {message}").into());
    }

    if let Some(delay) = effects.delayed_visibility {
      self
        .controller
        .delay_metadata_visibility(metadata, delay)
        .await;
      return Ok(());
    }

    let result = self.inner.write_segment(metadata, fences, now_ts_ms).await;
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

  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self
      .scan_window_with_bound(window, min_snowflake, consistency)
      .await
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
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<blob_stream_metadata_store::ProducerPartitionLease>> {
    self.inner.get_lease(key).await
  }

  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
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
      .acquire_lease(key, holder_id, lease_session_id, now, lease_duration)
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

  async fn acquire_lease_and_reserve_sequences(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    reservation_size: Option<u64>,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    let key_format = key.format();
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ProducerLease,
        StoreFaultOperation::ProducerAcquireLease,
        &key_format,
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "producer acquire_lease_and_reserve_sequences timed out for key {key_format}"
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "producer acquire_lease_and_reserve_sequences fault for key {key_format}: {message}"
      ));
    }
    if reservation_size.is_some() {
      let effects = self
        .controller
        .effects_for_call(
          StoreFaultDomain::ProducerLease,
          StoreFaultOperation::ProducerReserveSequences,
          &key_format,
        )
        .await;
      if let Some(delay) = effects.delay {
        sleep(delay).await;
      }
      if let Some(timeout) = effects.timeout {
        sleep(timeout).await;
        return Err(anyhow!(
          "producer acquire_lease_and_reserve_sequences timed out reserving sequences for key \
           {key_format}"
        ));
      }
      if let Some(message) = effects.fail_message {
        return Err(anyhow!(
          "producer acquire_lease_and_reserve_sequences sequence reservation fault for key \
           {key_format}: {message}"
        ));
      }
    }

    let result = self
      .inner
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        reservation_size,
      )
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ProducerAcquireLease,
        key_format,
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          LeaseAcquireAndReserveOutcome::Acquired { .. } => "acquired".to_string(),
          LeaseAcquireAndReserveOutcome::HeldByOther(_) => "held_by_other".to_string(),
        }),
      )
      .await;
    result
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
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
      .heartbeat_lease(key, holder_id, lease_session_id, now, lease_duration)
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
    lease_session_id: &str,
    now: OffsetDateTime,
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
      .reserve_sequences(key, holder_id, lease_session_id, now, reservation_size)
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
    lease_session_id: &str,
    now: OffsetDateTime,
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

    let result = self
      .inner
      .release_lease(key, holder_id, lease_session_id, now)
      .await;
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

  async fn apply_faults(&self, operation: StoreFaultOperation, key: &str) -> Result<()> {
    let effects = self
      .controller
      .effects_for_call(StoreFaultDomain::ConsumerLease, operation, key)
      .await;
    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!("{operation:?} timed out for key {key}"));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!("{operation:?} fault for key {key}: {message}"));
    }
    Ok(())
  }

  async fn record_outcome<T>(
    &self,
    operation: StoreFaultOperation,
    key: String,
    result: Result<T>,
  ) -> Result<T> {
    self
      .controller
      .record_operation_outcome(
        operation,
        key,
        if result.is_ok() { "ok" } else { "error" },
        None,
      )
      .await;
    result
  }
}

#[async_trait]
impl ConsumerGroupMembershipStore for FaultInjectedConsumerGroupMembershipStore {
  async fn register_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    pod_id: Option<String>,
    now: OffsetDateTime,
    ttl: TimeDuration,
  ) -> Result<()> {
    let _ = &self.controller;
    self
      .inner
      .register_member(topic, group_id, member_id, pod_id, now, ttl)
      .await
  }

  async fn heartbeat_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    pod_id: Option<String>,
    now: OffsetDateTime,
    ttl: TimeDuration,
  ) -> Result<()> {
    let key = format!("{topic}#{group_id}#{member_id}");
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ConsumerLease,
        StoreFaultOperation::ConsumerMembershipHeartbeat,
        &key,
      )
      .await;

    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "consumer membership heartbeat timed out for key {key}"
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "consumer membership heartbeat fault for key {key}: {message}"
      ));
    }

    let result = self
      .inner
      .heartbeat_member(topic, group_id, member_id, pod_id, now, ttl)
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ConsumerMembershipHeartbeat,
        key,
        if result.is_ok() { "ok" } else { "error" },
        None,
      )
      .await;
    result
  }

  async fn deregister_member(&self, topic: &str, group_id: &str, member_id: &str) -> Result<()> {
    let key = format!("{topic}#{group_id}#{member_id}");
    self
      .apply_faults(StoreFaultOperation::ConsumerDeregisterMember, &key)
      .await?;
    self
      .record_outcome(
        StoreFaultOperation::ConsumerDeregisterMember,
        key,
        self
          .inner
          .deregister_member(topic, group_id, member_id)
          .await,
      )
      .await
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now: OffsetDateTime,
  ) -> Result<Vec<ConsumerGroupMember>> {
    let _ = &self.controller;
    self.inner.list_active_members(topic, group_id, now).await
  }

  async fn get_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Option<ConsumerGroupAssignmentPlan>> {
    let key = format!("{topic}#{group_id}");
    self
      .apply_faults(StoreFaultOperation::ConsumerGetAssignmentPlan, &key)
      .await?;
    self
      .record_outcome(
        StoreFaultOperation::ConsumerGetAssignmentPlan,
        key,
        self.inner.get_assignment_plan(topic, group_id).await,
      )
      .await
  }

  async fn get_planner_lease(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Option<ConsumerGroupPlannerLease>> {
    let key = format!("{topic}#{group_id}");
    self
      .apply_faults(StoreFaultOperation::ConsumerGetPlannerLease, &key)
      .await?;
    self
      .record_outcome(
        StoreFaultOperation::ConsumerGetPlannerLease,
        key,
        self.inner.get_planner_lease(topic, group_id).await,
      )
      .await
  }

  async fn acquire_or_renew_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
    now: OffsetDateTime,
    ttl: TimeDuration,
  ) -> Result<ConsumerGroupPlannerLeaseOutcome> {
    let key = format!("{topic}#{group_id}#{member_id}");
    self
      .apply_faults(StoreFaultOperation::ConsumerAcquireOrRenewPlanner, &key)
      .await?;
    self
      .record_outcome(
        StoreFaultOperation::ConsumerAcquireOrRenewPlanner,
        key,
        self
          .inner
          .acquire_or_renew_planner(topic, group_id, member_id, planner_session_id, now, ttl)
          .await,
      )
      .await
  }

  async fn release_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
  ) -> Result<bool> {
    let key = format!("{topic}#{group_id}#{member_id}");
    self
      .apply_faults(StoreFaultOperation::ConsumerReleasePlanner, &key)
      .await?;
    self
      .record_outcome(
        StoreFaultOperation::ConsumerReleasePlanner,
        key,
        self
          .inner
          .release_planner(topic, group_id, member_id, planner_session_id)
          .await,
      )
      .await
  }

  async fn publish_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
    now: OffsetDateTime,
    plan: ConsumerGroupAssignmentPlan,
  ) -> Result<bool> {
    let key = format!("{topic}#{group_id}#{member_id}");
    self
      .apply_faults(StoreFaultOperation::ConsumerPublishAssignmentPlan, &key)
      .await?;
    self
      .record_outcome(
        StoreFaultOperation::ConsumerPublishAssignmentPlan,
        key,
        self
          .inner
          .publish_assignment_plan(topic, group_id, member_id, planner_session_id, now, plan)
          .await,
      )
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
  async fn list_group_leases(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Vec<ConsumerGroupLease>> {
    let key = format!("{topic}#{group_id}");
    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ConsumerLease,
        StoreFaultOperation::ConsumerListGroupLeases,
        &key,
      )
      .await;
    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "consumer list_group_leases timed out for key {key}"
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "consumer list_group_leases fault for key {key}: {message}"
      ));
    }
    let result = self.inner.list_group_leases(topic, group_id).await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ConsumerListGroupLeases,
        key,
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|leases| leases.len().to_string()),
      )
      .await;
    result
  }

  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
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
      .assign_partition(key, owner_id, generation, now, lease_duration)
      .await;
    self
      .controller
      .record_operation_outcome(
        StoreFaultOperation::ConsumerAssignPartition,
        key_str,
        if result.is_ok() { "ok" } else { "error" },
        result.as_ref().ok().map(|outcome| match outcome {
          ConsumerGroupAssignmentOutcome::Assigned { .. } => "assigned".to_string(),
          ConsumerGroupAssignmentOutcome::HeldByOther(_) => "held_by_other".to_string(),
        }),
      )
      .await;
    result
  }

  async fn arm_next_window_fresh_start(
    &self,
    key: &ConsumerGroupLeaseKey,
    metadata_window_size: TimeDuration,
    marker_id: String,
    now: OffsetDateTime,
  ) -> Result<ConsumerGroupArmFreshStartOutcome> {
    self
      .inner
      .arm_next_window_fresh_start(key, metadata_window_size, marker_id, now)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    committed_cursor: Option<CommittedCursor>,
  ) -> Result<ConsumerGroupHeartbeatOutcome> {
    self
      .heartbeat_partition_consuming_fresh_start_marker(
        key,
        owner_id,
        generation,
        now,
        lease_duration,
        committed_cursor,
        None,
      )
      .await
  }

  async fn heartbeat_partition_consuming_fresh_start_marker(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    committed_cursor: Option<CommittedCursor>,
    consumed_fresh_start_marker_id: Option<String>,
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
      .heartbeat_partition_consuming_fresh_start_marker(
        key,
        owner_id,
        generation,
        now,
        lease_duration,
        committed_cursor,
        consumed_fresh_start_marker_id,
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
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
  ) -> Result<ConsumerGroupCommitOutcome> {
    self
      .commit_cursor_consuming_fresh_start_marker(
        key,
        owner_id,
        generation,
        now,
        committed_cursor,
        None,
      )
      .await
  }

  async fn commit_cursor_consuming_fresh_start_marker(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
    consumed_fresh_start_marker_id: Option<String>,
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
      .commit_cursor_consuming_fresh_start_marker(
        key,
        owner_id,
        generation,
        now,
        committed_cursor,
        consumed_fresh_start_marker_id,
      )
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
    now: OffsetDateTime,
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

    let effects = self
      .controller
      .effects_for_call(
        StoreFaultDomain::ConsumerLease,
        StoreFaultOperation::ConsumerReleasePartition,
        &key_str,
      )
      .await;
    if let Some(delay) = effects.delay {
      sleep(delay).await;
    }
    if let Some(timeout) = effects.timeout {
      sleep(timeout).await;
      return Err(anyhow!(
        "consumer release_partition timed out for key {key_str}"
      ));
    }
    if let Some(message) = effects.fail_message {
      return Err(anyhow!(
        "consumer release_partition fault for key {key_str}: {message}"
      ));
    }

    let result = self
      .inner
      .release_partition(key, owner_id, generation, now)
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
