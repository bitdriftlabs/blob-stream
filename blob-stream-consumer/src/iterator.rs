// blob-stream - consumer iterator API
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./iterator_test.rs"]
mod tests;

use crate::config::{
  ConsumerGroupConfig,
  ConsumerRuntimeConfig,
  consumer_heartbeat_interval_ms,
  consumer_rebalance_interval_ms,
  stats_scope,
  validate_runtime_config,
};
use crate::consumer::{ConsumerBatch, ConsumerReader, ConsumerReaderImpl};
use crate::coordination::{
  ConsumerGroupCoordinator,
  ConsumerGroupCoordinatorImpl,
  HeartbeatReport,
  RebalanceReport,
};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use bd_log::warn_every;
use bd_server_stats::stats::{Collector, Scope};
use blob_stream_blob_store::BlobStore;
use blob_stream_metadata_store::{ConsumerGroupLeaseStore, MetadataStore};
use blob_stream_types::VirtualPartitionId;
use log::{info, trace};
use prometheus::{Histogram, IntCounter};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use time::ext::NumericalDuration;
use tokio::sync::oneshot;

// TODO(mattklein123): Add prefetching.

// TODO(mattklein123): Add backoff to stop wasting resources in low throughput scenarios.
const IDLE_POLL_DELAY_MS: u64 = 50;

//
// ConsumerIteratorMetrics
//

#[derive(Clone)]
struct ConsumerIteratorMetrics {
  next_calls: IntCounter,
  batches_delivered: IntCounter,
  records_delivered: IntCounter,
  retries: IntCounter,
  failures: IntCounter,
  revocations: IntCounter,
  next_latency_seconds: Histogram,
  commit_latency_seconds: Histogram,
}

impl ConsumerIteratorMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("iterator");
    Self {
      next_calls: scope.counter("next_calls"),
      batches_delivered: scope.counter("batches_delivered"),
      records_delivered: scope.counter("records_delivered"),
      retries: scope.counter("retries"),
      failures: scope.counter("failures"),
      revocations: scope.counter("revocations"),
      next_latency_seconds: scope.histogram("next_latency_seconds"),
      commit_latency_seconds: scope.histogram("commit_latency_seconds"),
    }
  }
}

//
// CoordinationSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinationSnapshot {
  pub members: Vec<String>,
  pub virtual_partitions: Vec<VirtualPartitionId>,
}

//
// ConsumerCoordinationSource
//

#[async_trait]
pub trait ConsumerCoordinationSource: Send + Sync {
  async fn snapshot(&self) -> Result<CoordinationSnapshot>;
}

//
// StaticConsumerCoordinationSource
//

pub struct StaticConsumerCoordinationSource {
  members: Vec<String>,
  virtual_partitions: Vec<VirtualPartitionId>,
}

impl StaticConsumerCoordinationSource {
  #[must_use]
  pub fn new(members: Vec<String>, virtual_partitions: Vec<VirtualPartitionId>) -> Self {
    Self {
      members,
      virtual_partitions,
    }
  }
}

#[async_trait]
impl ConsumerCoordinationSource for StaticConsumerCoordinationSource {
  async fn snapshot(&self) -> Result<CoordinationSnapshot> {
    Ok(CoordinationSnapshot {
      members: self.members.clone(),
      virtual_partitions: self.virtual_partitions.clone(),
    })
  }
}

//
// RevokedPartitions
//

#[async_trait]
pub trait RevokedPartitions: Send {
  fn partitions(&self) -> Vec<VirtualPartitionId>;
  async fn complete(self: Box<Self>);
}

struct RevokedPartitionsImpl {
  revoked: Vec<VirtualPartitionId>,
  completion_tx: Option<oneshot::Sender<()>>,
}

#[async_trait]
impl RevokedPartitions for RevokedPartitionsImpl {
  fn partitions(&self) -> Vec<VirtualPartitionId> {
    self.revoked.clone()
  }

  async fn complete(mut self: Box<Self>) {
    if let Some(completion_tx) = self.completion_tx.take() {
      let _ = completion_tx.send(());
    }
  }
}

//
// NextResult
//

pub enum NextResult {
  Batch(ConsumerBatch),
  Revoked(Box<dyn RevokedPartitions>),
}

//
// ConsumerIterator
//

#[async_trait]
pub trait ConsumerIterator: Send {
  fn start(&mut self) -> Result<()>;
  async fn next(&mut self) -> Result<NextResult>;
  fn store_offset(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()>;
  async fn commit(&mut self) -> Result<HeartbeatReport>;
  async fn shutdown(self: Box<Self>) -> Result<()>;
  async fn seek(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()>;
}

//
// ConsumerIteratorImpl
//

pub struct ConsumerIteratorImpl {
  group_config: ConsumerGroupConfig,
  reader: ConsumerReaderImpl,
  coordinator: Box<dyn ConsumerGroupCoordinator>,
  coordination_source: Arc<dyn ConsumerCoordinationSource>,
  metrics: ConsumerIteratorMetrics,
  started: bool,
  pending_commits: HashMap<VirtualPartitionId, u64>,
  buffered_batches: VecDeque<ConsumerBatch>,
  active_assignment: HashSet<VirtualPartitionId>,
  pending_assignment: Option<Vec<VirtualPartitionId>>,
  pending_revocation_completion: Option<oneshot::Receiver<()>>,
  next_heartbeat_at_ms: i64,
  next_rebalance_at_ms: i64,
}

impl ConsumerIteratorImpl {
  pub async fn from_runtime_config(
    runtime: &ConsumerRuntimeConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    coordination_source: Arc<dyn ConsumerCoordinationSource>,
  ) -> Result<Self> {
    validate_runtime_config(runtime)?;
    let read_config = runtime
      .read
      .as_ref()
      .ok_or_else(|| anyhow!("consumer read config is required"))?
      .clone();
    let group_config = runtime
      .group
      .as_ref()
      .ok_or_else(|| anyhow!("consumer group config is required"))?
      .clone();

    let active_assignment = HashSet::new();
    let reader = ConsumerReaderImpl::new(
      read_config,
      Vec::new(),
      HashMap::new(),
      blob_store,
      metadata_store,
    )?;
    let coordinator = ConsumerGroupCoordinatorImpl::new(group_config.clone(), lease_store)?;
    let now_ts_ms = current_unix_millis();

    let metrics_scope = Collector::default().scope(stats_scope(runtime));
    let mut iterator = Self {
      group_config,
      reader,
      coordinator: Box::new(coordinator),
      coordination_source,
      metrics: ConsumerIteratorMetrics::new(&metrics_scope.scope("consumer")),
      started: false,
      pending_commits: HashMap::new(),
      buffered_batches: VecDeque::new(),
      active_assignment,
      pending_assignment: None,
      pending_revocation_completion: None,
      next_heartbeat_at_ms: now_ts_ms,
      next_rebalance_at_ms: now_ts_ms,
    };

    let snapshot = iterator.coordination_source.snapshot().await?;
    let report = iterator
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now_ts_ms)
      .await?;
    let owned = iterator.apply_rebalance_report(report)?;

    info!(
      "consumer iterator bootstrapped: topic={}, group_id={}, member_id={}, owned={}",
      iterator.group_config.topic,
      iterator.group_config.group_id,
      iterator.group_config.member_id,
      owned.len()
    );

    Ok(iterator)
  }

  fn apply_rebalance_report(&mut self, report: RebalanceReport) -> Result<Vec<VirtualPartitionId>> {
    for (partition_id, seq_end) in report.committed_cursors {
      self.reader.hydrate_cursor(partition_id, seq_end);
    }

    self.apply_assignment(&report.owned_partitions)?;
    Ok(report.owned_partitions)
  }

  fn apply_assignment(&mut self, assignment: &[VirtualPartitionId]) -> Result<()> {
    self
      .reader
      .set_assigned_virtual_partitions(assignment.to_owned())?;
    self.active_assignment = assignment.iter().copied().collect();
    self
      .pending_commits
      .retain(|partition_id, _| self.active_assignment.contains(partition_id));
    info!(
      "consumer assignment active: topic={}, group_id={}, member_id={}, partitions={:?}",
      self.group_config.topic, self.group_config.group_id, self.group_config.member_id, assignment
    );
    Ok(())
  }

  fn finish_pending_revocation_if_completed(&mut self) -> Result<bool> {
    let Some(recv) = self.pending_revocation_completion.as_mut() else {
      return Ok(true);
    };

    match recv.try_recv() {
      Ok(()) | Err(oneshot::error::TryRecvError::Closed) => {
        let assignment = self.pending_assignment.take().unwrap_or_default();
        self.pending_revocation_completion = None;
        self.apply_assignment(&assignment)?;
        Ok(true)
      },
      Err(oneshot::error::TryRecvError::Empty) => Ok(false),
    }
  }

  async fn maybe_rebalance(&mut self, now_ts_ms: i64) -> Result<Option<NextResult>> {
    if now_ts_ms < self.next_rebalance_at_ms {
      return Ok(None);
    }

    let snapshot = self.coordination_source.snapshot().await?;
    let previous_assignment = self.active_assignment.clone();
    let report = self
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now_ts_ms)
      .await?;

    let next_assignment = report.owned_partitions.clone();
    for (partition_id, seq_end) in report.committed_cursors {
      self.reader.hydrate_cursor(partition_id, seq_end);
    }

    self.next_rebalance_at_ms = now_ts_ms + consumer_rebalance_interval_ms(&self.group_config);

    let next_assignment_set = next_assignment.iter().copied().collect::<HashSet<_>>();
    let revoked = previous_assignment
      .difference(&next_assignment_set)
      .copied()
      .collect::<Vec<_>>();

    if revoked.is_empty() {
      self.apply_assignment(&next_assignment)?;
      return Ok(None);
    }

    self.metrics.revocations.inc();
    for partition_id in &revoked {
      self
        .buffered_batches
        .retain(|batch| batch.virtual_partition_id != *partition_id);
    }

    let (completion_tx, completion_rx) = oneshot::channel();
    self.pending_assignment = Some(next_assignment);
    self.pending_revocation_completion = Some(completion_rx);

    info!(
      "consumer revocation requested: topic={}, group_id={}, member_id={}, revoked={:?}",
      self.group_config.topic, self.group_config.group_id, self.group_config.member_id, revoked
    );

    Ok(Some(NextResult::Revoked(Box::new(RevokedPartitionsImpl {
      revoked,
      completion_tx: Some(completion_tx),
    }))))
  }

  async fn heartbeat(&mut self, now_ts_ms: i64) -> Result<HeartbeatReport> {
    let report = self
      .coordinator
      .heartbeat_and_commit(now_ts_ms, &self.pending_commits)
      .await?;

    if !report.fenced_partitions.is_empty() {
      for partition_id in &report.fenced_partitions {
        self.active_assignment.remove(partition_id);
        self.pending_commits.remove(partition_id);
      }
      self
        .reader
        .set_assigned_virtual_partitions(self.active_assignment.iter().copied().collect())?;
      info!(
        "consumer heartbeat fenced partitions: topic={}, group_id={}, member_id={}, fenced={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        report.fenced_partitions
      );
    }

    self.next_heartbeat_at_ms = now_ts_ms + consumer_heartbeat_interval_ms(&self.group_config);
    Ok(report)
  }
}

#[async_trait]
impl ConsumerIterator for ConsumerIteratorImpl {
  fn start(&mut self) -> Result<()> {
    ensure!(!self.started, "consumer iterator already started");
    self.started = true;
    info!(
      "consumer iterator started: topic={}, group_id={}, member_id={}",
      self.group_config.topic, self.group_config.group_id, self.group_config.member_id
    );
    Ok(())
  }

  async fn next(&mut self) -> Result<NextResult> {
    ensure!(
      self.started,
      "consumer iterator must be started before next"
    );
    self.metrics.next_calls.inc();

    loop {
      if !self.finish_pending_revocation_if_completed()? {
        return Err(anyhow!(
          "revocation callback must be completed before continuing iteration"
        ));
      }

      let now_ts_ms = current_unix_millis();
      if let Some(revoked) = self.maybe_rebalance(now_ts_ms).await? {
        return Ok(revoked);
      }

      if now_ts_ms >= self.next_heartbeat_at_ms {
        self.heartbeat(now_ts_ms).await?;
      }

      if let Some(batch) = self.buffered_batches.pop_front() {
        trace!(
          "consumer next delivering buffered batch: topic={}, partition={}, records={}",
          self.group_config.topic,
          batch.virtual_partition_id,
          batch.records.len()
        );
        self.metrics.batches_delivered.inc();
        self
          .metrics
          .records_delivered
          .inc_by(batch.records.len() as u64);
        return Ok(NextResult::Batch(batch));
      }

      let read_started_at = Instant::now();
      let mut read_attempt: u8 = 0;
      let batches = loop {
        let now_unix_seconds = current_unix_seconds();
        match self.reader.read_available(now_unix_seconds).await {
          Ok(batches) => break batches,
          Err(error) => {
            if read_attempt == 0 {
              read_attempt = 1;
              self.metrics.retries.inc();
              warn_every!(
                15.seconds(),
                "consumer read retrying after error: topic={}, group_id={}, member_id={}, \
                 error={error}",
                self.group_config.topic,
                self.group_config.group_id,
                self.group_config.member_id
              );
              continue;
            }
            self.metrics.failures.inc();
            return Err(error);
          },
        }
      };
      self
        .metrics
        .next_latency_seconds
        .observe(read_started_at.elapsed().as_secs_f64());

      if batches.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(IDLE_POLL_DELAY_MS)).await;
        continue;
      }

      self.buffered_batches.extend(batches);
    }
  }

  fn store_offset(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()> {
    ensure!(
      self.active_assignment.contains(&virtual_partition_id),
      "cannot store cursor for unassigned virtual partition {virtual_partition_id}"
    );
    self.pending_commits.insert(virtual_partition_id, offset);
    trace!(
      "consumer stored offset: topic={}, partition={}, offset={}",
      self.group_config.topic, virtual_partition_id, offset
    );
    Ok(())
  }

  async fn commit(&mut self) -> Result<HeartbeatReport> {
    ensure!(
      self.started,
      "consumer iterator must be started before commit"
    );
    let started_at = Instant::now();
    let report = self.heartbeat(current_unix_millis()).await?;
    self
      .metrics
      .commit_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
    Ok(report)
  }

  async fn shutdown(mut self: Box<Self>) -> Result<()> {
    if self.started {
      let _ = self.commit().await?;
      self.started = false;
      info!(
        "consumer iterator shutdown: topic={}, group_id={}, member_id={}",
        self.group_config.topic, self.group_config.group_id, self.group_config.member_id
      );
    }
    Ok(())
  }

  async fn seek(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()> {
    ensure!(
      self.active_assignment.contains(&virtual_partition_id),
      "cannot seek unassigned virtual partition {virtual_partition_id}"
    );
    self.reader.set_cursor(virtual_partition_id, offset);
    trace!(
      "consumer seek: topic={}, partition={}, offset={}",
      self.group_config.topic, virtual_partition_id, offset
    );
    Ok(())
  }
}

fn current_unix_seconds() -> i64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map_or(0, |duration| {
      i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
    })
}

fn current_unix_millis() -> i64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map_or(0, |duration| {
      i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
    })
}
