use super::{StressConfig, StressRecordIdentity, StressValidationSummary, StressValidator};
use crate::test_framework::{
  ClusterHarness,
  IntegrationResources,
  SECOND_TOPIC,
  TOPIC,
  consumer_bootstrap_config_for,
  producer_topic_named_with_partition_count,
};
use anyhow::{Result, anyhow};
use bd_server_stats::stats::Collector;
use blob_stream_broker::write::BrokerLeaseStatus;
use blob_stream_consumer::{
  ConsumerConfigFactory,
  ConsumerIterator,
  ConsumerReadConfig,
  ConsumerReader,
  ConsumerReaderImpl,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  NextResult,
  ReadCapacity,
};
use blob_stream_producer::{
  ProducerAck,
  ProducerClient,
  ProducerConfig,
  ProducerError,
  ProducerRecord,
  ProducerRetryReason,
  ProducerRetrySample,
  ProducerRetrySummary,
};
use blob_stream_types::{
  VirtualPartitionId,
  now_unix_millis,
  now_unix_seconds,
  virtual_partition_for_key,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, timeout};
use uuid::Uuid;

const CONSUMER_EVENT_BUFFER: usize = 4_096;
const CONSUMER_POLL_TIMEOUT: Duration = Duration::from_millis(250);
const BROKER_READY_POLL_INTERVAL: Duration = Duration::from_millis(10);

//
// StressStage
//

#[derive(Clone, Copy)]
#[repr(u8)]
enum StressStage {
  Initializing,
  BrokerStartup,
  WorkloadStartup,
  Producing,
  ConsumerDraining,
  ConsumerShutdown,
  Verifying,
}

impl StressStage {
  const fn as_str(self) -> &'static str {
    match self {
      Self::Initializing => "initializing",
      Self::BrokerStartup => "broker_startup",
      Self::WorkloadStartup => "workload_startup",
      Self::Producing => "producing",
      Self::ConsumerDraining => "consumer_draining",
      Self::ConsumerShutdown => "consumer_shutdown",
      Self::Verifying => "verifying",
    }
  }
}

//
// StressProgress
//

#[derive(Default)]
struct StressProgress {
  stage: AtomicU64,
  attempted_records: AtomicU64,
  acknowledged_records: AtomicU64,
  consumer_records: AtomicU64,
  verifier_records: AtomicU64,
}

impl StressProgress {
  fn enter_stage(&self, stage: StressStage, started: Instant) {
    self.stage.store(u64::from(stage as u8), Ordering::Relaxed);
    self.report(stage.as_str(), started, None);
  }

  fn current_stage(&self) -> StressStage {
    match self.stage.load(Ordering::Relaxed) {
      value if value == u64::from(StressStage::Initializing as u8) => StressStage::Initializing,
      value if value == u64::from(StressStage::BrokerStartup as u8) => StressStage::BrokerStartup,
      value if value == u64::from(StressStage::WorkloadStartup as u8) => {
        StressStage::WorkloadStartup
      },
      value if value == u64::from(StressStage::Producing as u8) => StressStage::Producing,
      value if value == u64::from(StressStage::ConsumerDraining as u8) => {
        StressStage::ConsumerDraining
      },
      value if value == u64::from(StressStage::ConsumerShutdown as u8) => {
        StressStage::ConsumerShutdown
      },
      value if value == u64::from(StressStage::Verifying as u8) => StressStage::Verifying,
      _ => StressStage::Initializing,
    }
  }

  fn counters(&self) -> String {
    format!(
      "attempted={} acknowledged={} consumer_records={} verifier_records={}",
      self.attempted_records.load(Ordering::Relaxed),
      self.acknowledged_records.load(Ordering::Relaxed),
      self.consumer_records.load(Ordering::Relaxed),
      self.verifier_records.load(Ordering::Relaxed),
    )
  }

  fn overall_timeout_error(&self, started: Instant) -> anyhow::Error {
    anyhow!(
      "overall stress timeout after {} ms while stage={}: {}",
      started.elapsed().as_millis(),
      self.current_stage().as_str(),
      self.counters()
    )
  }

  fn report(&self, stage: &str, started: Instant, unique_records: Option<u64>) {
    let unique_records =
      unique_records.map_or_else(|| "n/a".to_string(), |count| count.to_string());
    eprintln!(
      "stress progress stage={stage} elapsed_ms={} {} unique={unique_records}",
      started.elapsed().as_millis(),
      self.counters(),
    );
  }
}

//
// StressRunSummary
//

#[derive(Debug)]
pub struct StressRunSummary {
  pub run_id: Uuid,
  pub acknowledged_records: u64,
  pub retry_attempts: u64,
  pub retry_reasons: BTreeMap<ProducerRetryReason, u64>,
  pub retry_samples: Vec<ProducerRetrySample>,
  pub producer_errors: Vec<String>,
  pub consumer_errors: Vec<String>,
  pub inline_validation: InlineValidationSummary,
  pub validation: StressValidationSummary,
}

impl StressRunSummary {
  #[must_use]
  pub fn is_success(&self) -> bool {
    self.producer_errors.is_empty()
      && self.consumer_errors.is_empty()
      && self.inline_validation.is_complete()
      && self.validation.is_complete()
      && self.acknowledged_records == self.validation.expected_records
  }
}

//
// InlineValidationSummary
//

#[derive(Debug)]
pub struct InlineValidationSummary {
  pub validation: StressValidationSummary,
  pub owned_partitions: Vec<VirtualPartitionId>,
  pub expected_data_partitions: Vec<VirtualPartitionId>,
  pub observed_partition_records: BTreeMap<VirtualPartitionId, u64>,
  pub misrouted_records: u64,
  pub misrouted_samples: Vec<InlineMisroutedRecord>,
  expected_partition_count: usize,
}

impl InlineValidationSummary {
  #[must_use]
  pub fn is_complete(&self) -> bool {
    self.validation.is_complete()
      && self.owned_partitions.len() == self.expected_partition_count
      && self.misrouted_records == 0
  }
}

//
// InlineMisroutedRecord
//

#[derive(Debug)]
pub struct InlineMisroutedRecord {
  pub identity: StressRecordIdentity,
  pub expected_partition: VirtualPartitionId,
  pub observed_partition: VirtualPartitionId,
}

//
// InlineValidator
//

struct InlineValidator {
  validator: StressValidator,
  expected_records: u64,
  expected_data_partitions: BTreeSet<VirtualPartitionId>,
  observed_partition_records: BTreeMap<VirtualPartitionId, u64>,
  misrouted_records: u64,
  misrouted_samples: Vec<InlineMisroutedRecord>,
  max_samples: usize,
  expected_partition_count: usize,
}

impl InlineValidator {
  fn new(
    run_id: Uuid,
    expected_per_producer: Vec<u64>,
    max_samples: usize,
    partition_count: u32,
  ) -> Result<Self> {
    let expected_records = expected_per_producer
      .iter()
      .try_fold(0_u64, |total, count| {
        total
          .checked_add(*count)
          .ok_or_else(|| anyhow!("inline expected record count overflow"))
      })?;
    let expected_data_partitions = expected_per_producer
      .iter()
      .flat_map(|record_count| 0 .. *record_count)
      .map(|sequence| partition_for_sequence(sequence, partition_count))
      .collect();
    Ok(Self {
      validator: StressValidator::new(run_id, expected_per_producer, max_samples)?,
      expected_records,
      expected_data_partitions,
      observed_partition_records: BTreeMap::new(),
      misrouted_records: 0,
      misrouted_samples: Vec::with_capacity(max_samples),
      max_samples,
      expected_partition_count: usize::try_from(partition_count)
        .map_err(|_| anyhow!("partition count does not fit this platform"))?,
    })
  }

  fn observe(
    &mut self,
    identity: StressRecordIdentity,
    observed_partition: VirtualPartitionId,
    partition_count: u32,
  ) -> Result<()> {
    self.validator.observe(identity)?;
    *self
      .observed_partition_records
      .entry(observed_partition)
      .or_default() += 1;

    let expected_partition = partition_for_sequence(identity.sequence, partition_count);
    if observed_partition != expected_partition {
      self.misrouted_records += 1;
      if self.misrouted_samples.len() < self.max_samples {
        self.misrouted_samples.push(InlineMisroutedRecord {
          identity,
          expected_partition,
          observed_partition,
        });
      }
    }
    Ok(())
  }

  fn is_complete(&self, owned_partitions: &BTreeSet<VirtualPartitionId>) -> bool {
    self.validator.unique_records() == self.expected_records
      && self.validator.summary().duplicate_records == 0
      && owned_partitions.len() == self.expected_partition_count
      && self.misrouted_records == 0
  }

  fn summary(self, owned_partitions: BTreeSet<VirtualPartitionId>) -> InlineValidationSummary {
    InlineValidationSummary {
      validation: self.validator.summary(),
      owned_partitions: owned_partitions.into_iter().collect(),
      expected_data_partitions: self.expected_data_partitions.into_iter().collect(),
      observed_partition_records: self.observed_partition_records,
      misrouted_records: self.misrouted_records,
      misrouted_samples: self.misrouted_samples,
      expected_partition_count: self.expected_partition_count,
    }
  }
}

//
// ConsumerEvent
//

enum ConsumerEvent {
  Record {
    identity: StressRecordIdentity,
    partition: VirtualPartitionId,
  },
  Error(String),
}

//
// ProducerReport
//

struct ProducerReport {
  acknowledged_records: u64,
  retry_attempts: u64,
  retry_summary: ProducerRetrySummary,
  error: Option<String>,
}

pub async fn run(config: StressConfig) -> Result<StressRunSummary> {
  config.validate()?;
  eprintln!("stress configuration: {config:#?}");
  let started = Instant::now();
  let deadline = started + config.overall_timeout;
  let progress = Arc::new(StressProgress::default());
  progress.enter_stage(StressStage::Initializing, started);
  let run_id = Uuid::new_v4();
  let expected_per_producer = records_per_producer(config.total_records, config.producer_count)?;
  let mut validator = StressValidator::new(
    run_id,
    expected_per_producer.clone(),
    config.max_discrepancy_samples,
  )?;

  let resources = timeout(
    remaining_until(deadline, &progress, started)?,
    IntegrationResources::create(),
  )
  .await
  .map_err(|_| progress.overall_timeout_error(started))??;
  progress.enter_stage(StressStage::BrokerStartup, started);
  let cluster_result = timeout(
    remaining_until(deadline, &progress, started)?,
    ClusterHarness::builder(&resources, config.broker_count)
      .blob_store(resources.s3_blob_store())
      .partition_count(config.partition_count)
      .broker_flush_max_delay(config.broker_flush_max_delay)
      .start_with_all_nodes()
      .start(),
  )
  .await;
  let mut cluster = match cluster_result {
    Ok(Ok(cluster)) => cluster,
    Ok(Err(error)) => {
      resources.cleanup().await;
      return Err(error);
    },
    Err(_) => {
      resources.cleanup().await;
      return Err(progress.overall_timeout_error(started));
    },
  };

  let remaining = remaining_until(deadline, &progress, started)?;
  let result = timeout(remaining, async {
    wait_for_broker_readiness(
      &cluster,
      config.partition_count,
      config.startup_timeout,
      config.progress_interval,
      &progress,
    )
    .await?;

    run_with_resources(
      &config,
      run_id,
      expected_per_producer,
      &mut validator,
      &resources,
      &cluster,
      Arc::clone(&progress),
    )
    .await
  })
  .await;

  cluster.shutdown().await;
  resources.cleanup().await;
  result.unwrap_or_else(|_| Err(progress.overall_timeout_error(started)))
}

fn remaining_until(
  deadline: Instant,
  progress: &StressProgress,
  started: Instant,
) -> Result<Duration> {
  let remaining = deadline.saturating_duration_since(Instant::now());
  if remaining.is_zero() {
    return Err(progress.overall_timeout_error(started));
  }
  Ok(remaining)
}

async fn wait_for_broker_readiness(
  cluster: &ClusterHarness,
  partition_count: u32,
  startup_timeout: Duration,
  progress_interval: Duration,
  progress: &StressProgress,
) -> Result<()> {
  let started = Instant::now();
  let deadline = started + startup_timeout;
  let mut next_progress_at = started;
  let broker_count = cluster.live_nodes().len();
  let expected_partition_count = usize::try_from(partition_count)
    .map_err(|_| anyhow!("partition count does not fit this platform"))?;
  loop {
    let snapshots = cluster.broker_state_snapshots().await;
    let converged = snapshots.len() == broker_count
      && snapshots.iter().all(|snapshot| {
        let topic_ownership = snapshot
          .ownership
          .iter()
          .filter(|ownership| ownership.topic.as_str() == TOPIC)
          .collect::<Vec<_>>();
        snapshot.membership.len() == broker_count
          && topic_ownership.len() == expected_partition_count
          && topic_ownership.iter().all(|ownership| {
            let lease_is_active = if ownership.assignment_is_local {
              ownership.lease_status == BrokerLeaseStatus::LocalActive
            } else {
              ownership.lease_status == BrokerLeaseStatus::RemoteActive
            };
            lease_is_active
              && ownership
                .assigned_broker
                .as_ref()
                .zip(ownership.observed_lease.as_ref())
                .is_some_and(|(assigned, observed)| {
                  observed.is_active && observed.holder_id == assigned.node_id.as_str()
                })
          })
      });
    if converged {
      progress.report("broker_startup", started, None);
      return Ok(());
    }
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "broker startup stage timed out after {} ms: ownership did not converge for \
         {partition_count} partitions: {snapshots:#?}",
        startup_timeout.as_millis()
      ));
    }
    if Instant::now() >= next_progress_at {
      progress.report("broker_startup", started, None);
      next_progress_at += progress_interval;
    }
    tokio::time::sleep(BROKER_READY_POLL_INTERVAL).await;
  }
}

async fn run_with_resources(
  config: &StressConfig,
  run_id: Uuid,
  expected_per_producer: Vec<u64>,
  validator: &mut StressValidator,
  resources: &IntegrationResources,
  cluster: &ClusterHarness,
  progress: Arc<StressProgress>,
) -> Result<StressRunSummary> {
  progress.enter_stage(StressStage::WorkloadStartup, Instant::now());
  let group_id = format!("stress-{run_id}");
  let partition_keys = Arc::new(stress_keys_for_partitions(config.partition_count)?);
  let (event_tx, mut event_rx) = mpsc::channel(CONSUMER_EVENT_BUFFER);
  let (stop_tx, stop_rx) = watch::channel(false);
  let owned_partitions = Arc::new(Mutex::new(BTreeSet::new()));
  let mut inline_validator = InlineValidator::new(
    run_id,
    expected_per_producer.clone(),
    config.max_discrepancy_samples,
    config.partition_count,
  )?;
  let mut consumer_tasks = FuturesUnordered::new();
  let mut consumer_errors = Vec::new();

  for consumer_index in 0 .. config.consumer_count {
    let member_id = format!("stress-consumer-{consumer_index}");
    let mut consumer_config = consumer_bootstrap_config_for(
      TOPIC,
      config.partition_count,
      &group_id,
      &member_id,
      resources,
    );
    let consumer_group_config = consumer_config
      .runtime
      .as_mut()
      .and_then(|runtime| runtime.group.as_mut())
      .ok_or_else(|| anyhow!("stress consumer bootstrap config is missing group settings"))?;
    consumer_group_config.lease_duration_ms =
      Some(duration_millis(config.consumer_lease_duration)?);
    consumer_group_config.heartbeat_interval_ms =
      Some(duration_millis(config.consumer_heartbeat_interval)?);
    consumer_group_config.rebalance_interval_ms =
      Some(duration_millis(config.consumer_rebalance_interval)?);
    let mut consumer = ConsumerConfigFactory::build_iterator_from_proto_config(
      consumer_config,
      Collector::default().scope("blob_stream_stress_consumer"),
      None,
    )
    .await?;
    let owned_partitions = Arc::clone(&owned_partitions);
    consumer.set_assignment_callback(Arc::new(move |partitions| {
      owned_partitions.lock().extend(partitions.iter().copied());
    }));
    consumer_tasks.push(run_consumer(
      Box::new(consumer),
      stop_rx.clone(),
      event_tx.clone(),
      config.consumer_commit_interval_records,
      Arc::clone(&progress),
    ));
  }
  drop(event_tx);

  let mut producer_tasks = FuturesUnordered::new();
  for (producer_index, record_count) in expected_per_producer.iter().copied().enumerate() {
    let producer = cluster
      .create_producer(
        stress_producer_config(config)?,
        vec![
          producer_topic_named_with_partition_count(TOPIC, config.partition_count, 1),
          producer_topic_named_with_partition_count(SECOND_TOPIC, config.partition_count, 1),
        ],
      )
      .await?;
    producer_tasks.push(run_producer(
      producer,
      run_id,
      u32::try_from(producer_index)
        .map_err(|_| anyhow!("producer count exceeds payload identity capacity"))?,
      record_count,
      config.payload_size,
      Arc::clone(&partition_keys),
      config.partition_count,
      config.producer_submit_concurrency,
      Arc::clone(&progress),
    ));
  }

  let mut acknowledged_records = 0_u64;
  let mut retry_attempts = 0_u64;
  let mut retry_reasons = BTreeMap::new();
  let mut retry_samples = Vec::with_capacity(config.max_discrepancy_samples);
  let mut producer_errors = Vec::new();
  let mut completed_producers = 0_usize;
  let producer_started = Instant::now();
  progress.enter_stage(StressStage::Producing, producer_started);
  let producer_deadline = producer_started + config.producer_timeout;
  let mut progress_ticker = tokio::time::interval(config.progress_interval);

  while completed_producers < config.producer_count {
    tokio::select! {
      _ = progress_ticker.tick() => {
        progress.report("producing", producer_started, None);
      }
      () = tokio::time::sleep_until(producer_deadline) => {
        return Err(anyhow!(
          "producer stage timed out after {} ms: completed_producers={completed_producers}/{}",
          config.producer_timeout.as_millis(),
          config.producer_count
        ));
      }
      event = event_rx.recv() => {
        if let Some(event) = event {
          handle_consumer_event(
            event,
            &mut consumer_errors,
            &mut inline_validator,
            config.partition_count,
          )?;
        }
      }
      task = consumer_tasks.next(), if !consumer_tasks.is_empty() => {
        if let Some(Err(error)) = task {
          consumer_errors.push(error.to_string());
        }
      }
      task = producer_tasks.next() => {
        let report = task
          .ok_or_else(|| anyhow!("producer task set ended unexpectedly"))?;
        acknowledged_records += report.acknowledged_records;
        retry_attempts += report.retry_attempts;
        for (reason, count) in report.retry_summary.reason_counts {
          *retry_reasons.entry(reason).or_default() += count;
        }
        let remaining_sample_capacity = config
          .max_discrepancy_samples
          .saturating_sub(retry_samples.len());
        retry_samples.extend(
          report
            .retry_summary
            .samples
            .into_iter()
            .take(remaining_sample_capacity),
        );
        if let Some(error) = report.error {
          producer_errors.push(error);
        }
        completed_producers += 1;
      }
    }
  }

  if !producer_errors.is_empty() {
    return Err(anyhow!(
      "producer stage failed: acknowledged={acknowledged_records}/{}, \
       errors={producer_errors:#?}, retry_reasons={retry_reasons:#?}, \
       retry_samples={retry_samples:#?}",
      config.total_records,
    ));
  }

  progress.enter_stage(StressStage::ConsumerDraining, Instant::now());
  let inline_validation_error = wait_for_inline_validation(
    &mut event_rx,
    &mut consumer_tasks,
    &mut consumer_errors,
    &mut inline_validator,
    &owned_partitions,
    config.partition_count,
    config.drain_timeout,
    config.progress_interval,
  )
  .await
  .err();
  let broker_state_snapshots = if inline_validation_error.is_some() {
    Some(cluster.broker_state_snapshots().await)
  } else {
    None
  };

  progress.enter_stage(StressStage::ConsumerShutdown, Instant::now());
  let _ = stop_tx.send(true);
  let shutdown_started = Instant::now();
  timeout(config.consumer_shutdown_timeout, async {
    while let Some(task) = consumer_tasks.next().await {
      if let Err(error) = task {
        consumer_errors.push(error.to_string());
      }
    }
    Ok::<(), anyhow::Error>(())
  })
  .await
  .map_err(|_| {
    anyhow!(
      "consumer shutdown stage timed out after {} ms",
      config.consumer_shutdown_timeout.as_millis()
    )
  })??;
  progress.report("consumer_shutdown", shutdown_started, None);
  while let Ok(event) = event_rx.try_recv() {
    handle_consumer_event(
      event,
      &mut consumer_errors,
      &mut inline_validator,
      config.partition_count,
    )?;
  }

  let inline_validation = inline_validator.summary(owned_partitions.lock().clone());
  eprintln!("stress inline validation: {inline_validation:#?}");
  if let Some(error) = &inline_validation_error {
    eprintln!(
      "stress inline validation failed; continuing with direct verification: error={error}, \
       broker_states={broker_state_snapshots:#?}"
    );
  }
  if !retry_reasons.is_empty() {
    eprintln!(
      "stress producer retry diagnostics: reasons={retry_reasons:#?} samples={retry_samples:#?}"
    );
  }

  progress.enter_stage(StressStage::Verifying, Instant::now());
  let direct_verification_error = verify_all_records(
    validator,
    resources,
    config.partition_count,
    config.verification_timeout,
    config.progress_interval,
    &progress,
  )
  .await
  .err();
  let direct_validation = validator.summary();

  if let Some(error) = &direct_verification_error {
    eprintln!("stress direct verification failed: error={error}");
  }
  if inline_validation_error.is_some() || direct_verification_error.is_some() {
    eprintln!("stress direct validation after failure: {direct_validation:#?}");
  }
  if let Some(error) = inline_validation_error {
    return Err(error);
  }
  if let Some(error) = direct_verification_error {
    return Err(error);
  }

  Ok(StressRunSummary {
    run_id,
    acknowledged_records,
    retry_attempts,
    retry_reasons,
    retry_samples,
    producer_errors,
    consumer_errors,
    inline_validation,
    validation: direct_validation,
  })
}

fn duration_millis(duration: Duration) -> Result<i64> {
  i64::try_from(duration.as_millis())
    .map_err(|_| anyhow!("stress duration exceeds milliseconds as i64"))
}

async fn run_producer(
  producer: blob_stream_producer::ProducerClientImpl,
  run_id: Uuid,
  producer_index: u32,
  record_count: u64,
  payload_size: usize,
  partition_keys: Arc<Vec<Vec<u8>>>,
  partition_count: u32,
  submit_concurrency: usize,
  progress: Arc<StressProgress>,
) -> ProducerReport {
  let producer = Arc::new(producer);
  let mut acknowledged_records = 0_u64;
  let mut retry_attempts = 0_u64;
  let mut submissions = FuturesUnordered::new();

  for sequence in 0 .. record_count {
    progress.attempted_records.fetch_add(1, Ordering::Relaxed);
    let identity = StressRecordIdentity {
      run_id,
      producer_index,
      sequence,
    };
    let partition_index =
      usize::try_from(sequence % u64::from(partition_count)).map_or(0, |index| index);
    let key = partition_keys[partition_index].clone();
    let producer_client = Arc::clone(&producer);
    submissions.push(async move {
      producer_client
        .produce(ProducerRecord::new(
          TOPIC.into(),
          key,
          identity.encode(payload_size).into(),
          now_unix_millis(),
        ))
        .await
    });

    if submissions.len() >= submit_concurrency
      && let Err(error) = collect_producer_submission(
        &mut submissions,
        &mut acknowledged_records,
        &mut retry_attempts,
        &progress,
      )
      .await
    {
      return ProducerReport {
        acknowledged_records,
        retry_attempts,
        retry_summary: producer_retry_summary(&producer),
        error: Some(error.to_string()),
      };
    }
  }

  while !submissions.is_empty() {
    if let Err(error) = collect_producer_submission(
      &mut submissions,
      &mut acknowledged_records,
      &mut retry_attempts,
      &progress,
    )
    .await
    {
      return ProducerReport {
        acknowledged_records,
        retry_attempts,
        retry_summary: producer_retry_summary(&producer),
        error: Some(error.to_string()),
      };
    }
  }

  match producer.flush().await {
    Ok(()) => ProducerReport {
      acknowledged_records,
      retry_attempts,
      retry_summary: producer_retry_summary(&producer),
      error: None,
    },
    Err(error) => ProducerReport {
      acknowledged_records,
      retry_attempts,
      retry_summary: producer_retry_summary(&producer),
      error: Some(error.to_string()),
    },
  }
}

fn producer_retry_summary(
  producer: &blob_stream_producer::ProducerClientImpl,
) -> ProducerRetrySummary {
  producer
    .diagnostics()
    .map_or_else(ProducerRetrySummary::default, |diagnostics| {
      diagnostics.retry_summary()
    })
}

async fn collect_producer_submission(
  submissions: &mut FuturesUnordered<impl Future<Output = Result<ProducerAck, ProducerError>>>,
  acknowledged_records: &mut u64,
  retry_attempts: &mut u64,
  progress: &StressProgress,
) -> Result<()> {
  let result = submissions
    .next()
    .await
    .ok_or_else(|| anyhow!("producer submission set ended unexpectedly"))?;
  record_producer_ack(result, acknowledged_records, retry_attempts, progress)
}

fn stress_producer_config(config: &StressConfig) -> Result<ProducerConfig> {
  let mut producer_config = ProducerConfig::new();
  producer_config.max_batch_records = config.producer_max_batch_records;
  producer_config.max_batch_bytes = config.producer_max_batch_bytes;
  producer_config.flush_max_delay_ms = config
    .producer_flush_max_delay
    .map(|flush_max_delay| u64::try_from(flush_max_delay.as_millis()))
    .transpose()
    .map_err(|_| anyhow!("producer flush max delay exceeds milliseconds as u64"))?;
  producer_config.max_request_concurrency = config.producer_max_request_concurrency;
  Ok(producer_config)
}

fn record_producer_ack(
  result: Result<ProducerAck, ProducerError>,
  acknowledged_records: &mut u64,
  retry_attempts: &mut u64,
  progress: &StressProgress,
) -> Result<()> {
  let ack = result.map_err(|error| anyhow!(error.to_string()))?;
  *acknowledged_records += 1;
  progress
    .acknowledged_records
    .fetch_add(1, Ordering::Relaxed);
  *retry_attempts += u64::from(ack.attempts.saturating_sub(1));
  Ok(())
}

async fn run_consumer(
  mut consumer: Box<blob_stream_consumer::ConsumerIteratorImpl>,
  mut stop_rx: watch::Receiver<bool>,
  event_tx: mpsc::Sender<ConsumerEvent>,
  commit_interval_records: u64,
  progress: Arc<StressProgress>,
) -> Result<()> {
  consumer.start()?;
  let mut records_since_commit = 0_u64;

  loop {
    tokio::select! {
      changed = stop_rx.changed() => {
        if changed.is_ok() && *stop_rx.borrow() {
          break;
        }
      }
      result = timeout(CONSUMER_POLL_TIMEOUT, consumer.next()) => {
        let next_result = match result {
          Err(_) => continue,
          Ok(Err(error)) => {
            let _ = event_tx.send(ConsumerEvent::Error(error.to_string())).await;
            break;
          },
          Ok(Ok(next_result)) => next_result,
        };
        match next_result {
          NextResult::Revoked(revoked) => {
            consumer.commit().await?;
            records_since_commit = 0;
            revoked.complete().await;
          },
          NextResult::Record(record) => {
            let identity = match StressRecordIdentity::decode(&record.record.payload) {
              Ok(identity) => identity,
              Err(error) => {
                let _ = event_tx.send(ConsumerEvent::Error(error.to_string())).await;
                break;
              },
            };
            consumer.store_offset(record.virtual_partition_id, record.offset)?;
            records_since_commit += 1;
            if records_since_commit >= commit_interval_records {
              consumer.commit().await?;
              records_since_commit = 0;
            }
            progress.consumer_records.fetch_add(1, Ordering::Relaxed);
            if event_tx
              .send(ConsumerEvent::Record {
                identity,
                partition: record.virtual_partition_id,
              })
              .await
              .is_err()
            {
              break;
            }
          },
        }
      }
    }
  }

  consumer.shutdown().await
}

fn handle_consumer_event(
  event: ConsumerEvent,
  consumer_errors: &mut Vec<String>,
  inline_validator: &mut InlineValidator,
  partition_count: u32,
) -> Result<()> {
  match event {
    ConsumerEvent::Record {
      identity,
      partition,
    } => inline_validator.observe(identity, partition, partition_count)?,
    ConsumerEvent::Error(error) => consumer_errors.push(error),
  }
  Ok(())
}

async fn wait_for_inline_validation(
  event_rx: &mut mpsc::Receiver<ConsumerEvent>,
  consumer_tasks: &mut FuturesUnordered<impl Future<Output = Result<()>>>,
  consumer_errors: &mut Vec<String>,
  inline_validator: &mut InlineValidator,
  owned_partitions: &Arc<Mutex<BTreeSet<VirtualPartitionId>>>,
  partition_count: u32,
  drain_timeout: Duration,
  progress_interval: Duration,
) -> Result<()> {
  let started = Instant::now();
  let deadline = started + drain_timeout;
  let mut progress_ticker = tokio::time::interval(progress_interval);

  loop {
    let owned_partitions = owned_partitions.lock().clone();
    if inline_validator.is_complete(&owned_partitions) {
      return Ok(());
    }

    tokio::select! {
      _ = progress_ticker.tick() => {
        let summary = inline_validator.validator.summary();
        eprintln!(
          "stress inline progress elapsed_ms={} unique={} missing={} duplicates={} \
           misrouted={} owned_partitions={}/{} observed_partitions={}",
          started.elapsed().as_millis(),
          summary.unique_records,
          summary.missing_records,
          summary.duplicate_records,
          inline_validator.misrouted_records,
          owned_partitions.len(),
          partition_count,
          inline_validator.observed_partition_records.len(),
        );
      }
      () = tokio::time::sleep_until(deadline) => {
        let summary = inline_validator.validator.summary();
        return Err(anyhow!(
          "inline consumer validation timed out after {} ms: unique={} missing={} duplicates={} \
           misrouted={} owned_partitions={}/{} observed_partitions={}",
          drain_timeout.as_millis(),
          summary.unique_records,
          summary.missing_records,
          summary.duplicate_records,
          inline_validator.misrouted_records,
          owned_partitions.len(),
          partition_count,
          inline_validator.observed_partition_records.len(),
        ));
      }
      event = event_rx.recv() => {
        let event = event.ok_or_else(|| anyhow!("consumer event channel closed before validation completed"))?;
        handle_consumer_event(event, consumer_errors, inline_validator, partition_count)?;
      }
      task = consumer_tasks.next(), if !consumer_tasks.is_empty() => {
        if let Some(Err(error)) = task {
          consumer_errors.push(error.to_string());
        }
      }
    }
  }
}

fn partition_for_sequence(sequence: u64, partition_count: u32) -> VirtualPartitionId {
  u32::try_from(sequence % u64::from(partition_count)).unwrap_or(0)
}

pub fn stress_keys_for_partitions(partition_count: u32) -> Result<Vec<Vec<u8>>> {
  let partition_count = usize::try_from(partition_count)
    .map_err(|_| anyhow!("partition count does not fit this platform"))?;
  let partition_count_u32 =
    u32::try_from(partition_count).map_err(|_| anyhow!("partition count does not fit u32"))?;
  let mut partition_keys = vec![None; partition_count];
  let max_candidates = u64::try_from(partition_count)
    .unwrap_or(u64::MAX)
    .saturating_mul(1_024);

  for candidate in 0 .. max_candidates {
    let key = format!("stress-key-{candidate}").into_bytes();
    let partition = virtual_partition_for_key(&key, partition_count_u32, 0);
    let partition_index = usize::try_from(partition)
      .map_err(|_| anyhow!("virtual partition does not fit this platform"))?;
    partition_keys[partition_index].get_or_insert(key);
    if partition_keys.iter().all(Option::is_some) {
      return partition_keys
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| anyhow!("stress partition key generation was incomplete"));
    }
  }

  Err(anyhow!(
    "failed to generate a key for every virtual partition after {max_candidates} candidates"
  ))
}

async fn verify_all_records(
  validator: &mut StressValidator,
  resources: &IntegrationResources,
  partition_count: u32,
  verification_timeout: Duration,
  progress_interval: Duration,
  progress: &StressProgress,
) -> Result<()> {
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(crate::test_framework::WINDOW_SIZE_SECONDS),
      ..Default::default()
    },
    (0 .. partition_count).collect(),
    HashMap::new(),
    resources.s3_blob_store(),
    resources.metadata_store(),
    &Collector::default().scope("blob_stream_stress_verifier"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;
  let deadline = Instant::now() + verification_timeout;
  let started = Instant::now();
  let mut next_progress_at = started;

  while !validator.summary().is_complete() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "verification stage timed out after {} ms: unique={} expected={}",
        verification_timeout.as_millis(),
        validator.unique_records(),
        validator.summary().expected_records
      ));
    }

    if Instant::now() >= next_progress_at {
      progress.report("verifying", started, Some(validator.unique_records()));
      next_progress_at += progress_interval;
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    let batches = timeout(
      remaining,
      reader.read_available(now_unix_seconds(), ReadCapacity::new(64 * 1024 * 1024)),
    )
    .await
    .map_err(|_| anyhow!("verification stage timed out waiting for a metadata/blob read"))??;
    if batches.is_empty() {
      tokio::time::sleep(Duration::from_millis(50)).await;
      continue;
    }
    for batch in batches {
      for record in batch.records {
        validator.observe(StressRecordIdentity::decode(&record.payload)?)?;
        progress.verifier_records.fetch_add(1, Ordering::Relaxed);
      }
    }
  }

  progress.report("verifying", started, Some(validator.unique_records()));

  Ok(())
}

fn records_per_producer(total_records: u64, producer_count: usize) -> Result<Vec<u64>> {
  let producer_count = u64::try_from(producer_count)
    .map_err(|_| anyhow!("producer count does not fit the workload allocator"))?;
  let base = total_records / producer_count;
  let remainder = total_records % producer_count;

  Ok(
    (0 .. producer_count)
      .map(|producer_index| base + u64::from(producer_index < remainder))
      .collect::<Vec<_>>(),
  )
}
