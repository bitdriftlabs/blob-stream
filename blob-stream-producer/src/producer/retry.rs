use super::metrics::ProducerMetrics;
use super::routing::{ProducerRoutes, produce_batch_request};
use super::state::BufferedBatch;
use super::{
  BrokerTransport,
  ProducerAck,
  ProducerError,
  ProducerRetryDiagnostics,
  ProducerRetryReason,
};
use crate::config::{
  ProducerConfig,
  ProducerTopicConfig,
  producer_request_timeout_ms,
  producer_retry_base_delay_ms,
  producer_retry_deadline_ms,
  producer_retry_max_delay_ms,
};
use anyhow::anyhow;
use async_trait::async_trait;
use bd_backoff::{ExponentialBackoff, ExponentialBackoffBuilder, InfiniteBackoff};
use bd_log::warn_every;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode, BrokerPartition};
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchResponse,
  ProduceBatchesRequest,
  ProduceStatus,
};
use log::{debug, trace};
use protobuf::Chars;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use time::Duration as TimeDuration;
use time::ext::NumericalDuration;
use tokio::sync::{Semaphore, watch};
use tokio::time::Instant;

const NOT_LEASE_HOLDER_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(250);
const NOT_LEASE_HOLDER_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);
const NOT_LEASE_HOLDER_MEMBERSHIP_SETTLE_DELAY: Duration = Duration::from_millis(100);

pub(super) async fn send_batch_with_retry(
  config: &ProducerConfig,
  topics: &HashMap<Chars, ProducerTopicConfig>,
  routes: &ProducerRoutes,
  membership_rx: &watch::Receiver<BrokerMembership>,
  transport: &dyn BrokerTransport,
  batch: &BufferedBatch,
  dispatch_permits: &Arc<Semaphore>,
  metrics: &ProducerMetrics,
  retry_diagnostics: &ProducerRetryDiagnostics,
  initial_response: Option<ProduceBatchResponse>,
  completed_attempts: u32,
  retry_clock: &dyn ProducerRetryClock,
  retry_started_at: Instant,
) -> Result<ProducerAck, ProducerError> {
  let mut retry_backoff = producer_retry_backoff(config);
  let mut not_lease_holder_retry_backoff = producer_not_lease_holder_retry_backoff();
  let _topic = topics
    .get(&batch.topic)
    .ok_or_else(|| ProducerError::UnknownTopic(batch.topic.clone()))?;

  let mut completed_attempts = completed_attempts;
  let mut initial_response = initial_response;
  let mut previous_owner: Option<Chars> = None;
  let mut membership_updates = membership_rx.clone();
  let mut retried_after_not_lease_holder = None;
  let retry_deadline = retry_started_at + Duration::from_millis(producer_retry_deadline_ms(config));
  loop {
    let remaining = retry_deadline.saturating_duration_since(retry_clock.now());
    if remaining.is_zero() {
      metrics.failures.inc();
      metrics.send_latency_seconds.observe(
        retry_clock
          .now()
          .duration_since(retry_started_at)
          .as_secs_f64(),
      );
      return Err(ProducerError::RetriesExhausted(format!(
        "retry deadline of {} ms elapsed",
        producer_retry_deadline_ms(config)
      )));
    }

    let (broker_node_id, broker_address) = routes
      .owner(&BrokerPartition {
        topic: batch.topic.clone(),
        virtual_partition_id: batch.virtual_partition_id,
      })
      .map_or_else(
        || {
          let membership = membership_rx.borrow();
          metrics.no_brokers.inc();
          metrics.failures.inc();
          metrics.send_latency_seconds.observe(
            retry_clock
              .now()
              .duration_since(retry_started_at)
              .as_secs_f64(),
          );
          warn_every!(
            15.seconds(),
            "producer no broker owner: topic={}, virtual_partition_id={}, membership_nodes={}",
            batch.topic,
            batch.virtual_partition_id,
            membership.nodes().map_or(0, <[BrokerNode]>::len)
          );
          Err(ProducerError::NoBrokersAvailable)
        },
        |broker| Ok((broker.node_id, broker.address)),
      )?;

    let owner_changed = previous_owner
      .as_deref()
      .is_some_and(|old| old != broker_node_id.as_str());
    if owner_changed {
      debug!(
        "producer routing changed after retry: topic={}, virtual_partition_id={}, from={}, to={}",
        batch.topic,
        batch.virtual_partition_id,
        previous_owner.as_deref().unwrap_or_default(),
        broker_node_id
      );
    }
    if let Some(waited_for_membership_update) = retried_after_not_lease_holder.take() {
      if owner_changed {
        metrics.not_lease_holder_retry_changed_owner.inc();
      } else {
        metrics.not_lease_holder_retry_same_owner.inc();
      }
      debug!(
        "producer retrying after not lease holder: topic={}, virtual_partition_id={}, \
         membership_update={}, previous_broker={}, next_broker={}",
        batch.topic,
        batch.virtual_partition_id,
        waited_for_membership_update,
        previous_owner.as_deref().unwrap_or_default(),
        broker_node_id
      );
    }
    previous_owner = Some(broker_node_id);

    let response = if let Some(response) = initial_response.take() {
      Ok(response)
    } else {
      completed_attempts = completed_attempts.saturating_add(1);
      trace!(
        "send attempt: topic={}, virtual_partition_id={}, attempt={}, broker={}",
        batch.topic, batch.virtual_partition_id, completed_attempts, broker_address
      );
      send_single_batch_request(
        config,
        transport,
        &broker_address,
        batch,
        dispatch_permits,
        metrics,
        remaining,
      )
      .await
    };
    let (current_error, retry_reason) = match response {
      Ok(response) => {
        let status = response.status.enum_value_or_default();
        match status {
          ProduceStatus::PRODUCE_STATUS_OK => {
            return Ok(acknowledge_batch(
              batch,
              metrics,
              completed_attempts,
              retry_clock,
              retry_started_at,
            ));
          },
          ProduceStatus::PRODUCE_STATUS_UNKNOWN_TOPIC => {
            return Err(ProducerError::UnknownTopic(batch.topic.clone()));
          },
          ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER
          | ProduceStatus::PRODUCE_STATUS_OVERLOADED => {
            let error = if response.error_message.is_empty() {
              format!("broker status: {status:?}")
            } else {
              response.error_message.to_string()
            };
            let reason = match status {
              ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER => ProducerRetryReason::NotLeaseHolder,
              ProduceStatus::PRODUCE_STATUS_OVERLOADED => ProducerRetryReason::Overloaded,
              _ => unreachable!("only retryable statuses reach this branch"),
            };
            (error, reason)
          },
        }
      },
      Err(ProducerError::RetriesExhausted(error)) => (error, ProducerRetryReason::TransportError),
      Err(error) => return Err(error),
    };

    let is_not_lease_holder = retry_reason == ProducerRetryReason::NotLeaseHolder;
    let retry_delay = if is_not_lease_holder {
      next_retry_delay(
        &mut not_lease_holder_retry_backoff,
        NOT_LEASE_HOLDER_RETRY_MAX_DELAY,
      )
    } else {
      next_retry_delay(
        &mut retry_backoff,
        Duration::from_millis(producer_retry_max_delay_ms(config)),
      )
    };
    let delay = retry_delay.min(retry_deadline.saturating_duration_since(retry_clock.now()));
    metrics.retries.inc();
    retry_diagnostics.record(
      retry_reason,
      &batch.topic,
      batch.virtual_partition_id,
      completed_attempts,
      current_error.clone(),
    );
    warn_every!(
      15.seconds(),
      "producer retrying batch: topic={}, virtual_partition_id={}, attempt={}, delay_ms={}, \
       reason={retry_reason:?}, error={}",
      batch.topic,
      batch.virtual_partition_id,
      completed_attempts,
      delay.as_millis(),
      current_error
    );
    if is_not_lease_holder {
      let membership_updated = wait_for_not_lease_holder_retry(
        &mut membership_updates,
        retry_clock,
        delay,
        retry_deadline,
      )
      .await;
      if membership_updated {
        // The watch receiver may have advanced again while waiting for membership to settle.
        // Refresh from its current value so a retry cannot overwrite newer cached routes.
        routes.refresh(config, topics, &membership_rx.borrow());
        metrics.not_lease_holder_retry_membership_updates.inc();
      } else {
        metrics.not_lease_holder_retry_timers.inc();
      }
      retried_after_not_lease_holder = Some(membership_updated);
    } else {
      retry_clock.sleep(delay).await;
    }
  }
}

/// Account for a successful logical batch and build the acknowledgement for all of its records.
pub(super) fn acknowledge_batch(
  batch: &BufferedBatch,
  metrics: &ProducerMetrics,
  attempts: u32,
  retry_clock: &dyn ProducerRetryClock,
  retry_started_at: Instant,
) -> ProducerAck {
  metrics.batches_sent.inc();
  metrics.records_sent.inc_by(batch.records.len() as u64);
  metrics.send_latency_seconds.observe(
    retry_clock
      .now()
      .duration_since(retry_started_at)
      .as_secs_f64(),
  );
  ProducerAck {
    topic: batch.topic.clone(),
    virtual_partition_id: batch.virtual_partition_id,
    attempts,
  }
}

async fn send_single_batch_request(
  config: &ProducerConfig,
  transport: &dyn BrokerTransport,
  broker_address: &Chars,
  batch: &BufferedBatch,
  dispatch_permits: &Arc<Semaphore>,
  metrics: &ProducerMetrics,
  remaining: Duration,
) -> Result<ProduceBatchResponse, ProducerError> {
  let request_timeout = Duration::from_millis(
    u64::try_from(producer_request_timeout_ms(config))
      .expect("producer config validation requires a positive request timeout"),
  )
  .min(remaining);
  let request = ProduceBatchesRequest {
    batches: vec![produce_batch_request(batch)],
    ..Default::default()
  };
  let Ok(_permit) = dispatch_permits.clone().acquire_owned().await else {
    return Err(ProducerError::Shutdown);
  };
  let _active_request = bd_server_stats::stats::StackAutoGauge::new(&metrics.active_requests);
  let mut response = tokio::time::timeout(
    request_timeout,
    transport.produce_batches(broker_address, request, request_timeout),
  )
  .await
  .unwrap_or_else(|_| {
    Err(anyhow!(
      "producer request timed out after {} ms",
      request_timeout.as_millis()
    ))
  })
  .map_err(|error| ProducerError::RetriesExhausted(error.to_string()))?;
  if response.results.len() != 1 {
    return Err(ProducerError::Rejected(format!(
      "broker returned {} results for one produce batch",
      response.results.len()
    )));
  }
  Ok(
    response
      .results
      .pop()
      .expect("response result length was checked"),
  )
}

pub(super) fn producer_retry_backoff(config: &ProducerConfig) -> ExponentialBackoff {
  let base_delay = Duration::from_millis(producer_retry_base_delay_ms(config));
  let max_delay = Duration::from_millis(producer_retry_max_delay_ms(config));
  ExponentialBackoffBuilder::new_infinite()
    .with_initial_interval(TimeDuration::try_from(base_delay).unwrap_or(TimeDuration::MAX))
    .with_randomization_factor(0.5)
    .with_multiplier(2.0)
    .with_max_interval(TimeDuration::try_from(max_delay).unwrap_or(TimeDuration::MAX))
    .build()
}

/// Use a separate backoff because a lease rejection usually needs broker handoff to converge.
fn producer_not_lease_holder_retry_backoff() -> ExponentialBackoff {
  ExponentialBackoffBuilder::new_infinite()
    .with_initial_interval(
      TimeDuration::try_from(NOT_LEASE_HOLDER_RETRY_INITIAL_DELAY).unwrap_or(TimeDuration::MAX),
    )
    .with_randomization_factor(0.5)
    .with_multiplier(2.0)
    .with_max_interval(
      TimeDuration::try_from(NOT_LEASE_HOLDER_RETRY_MAX_DELAY).unwrap_or(TimeDuration::MAX),
    )
    .build()
}

pub(super) fn next_retry_delay(
  retry_backoff: &mut (dyn InfiniteBackoff + Send),
  max_delay: Duration,
) -> Duration {
  retry_backoff.next_backoff().unsigned_abs().min(max_delay)
}

/// Wait for broker routing to change, or for lease ownership to converge without a route update.
pub(super) async fn wait_for_not_lease_holder_retry(
  membership_rx: &mut watch::Receiver<BrokerMembership>,
  retry_clock: &dyn ProducerRetryClock,
  delay: Duration,
  retry_deadline: Instant,
) -> bool {
  let membership_updated = match membership_rx.has_changed() {
    Ok(true) => {
      membership_rx.borrow_and_update();
      true
    },
    Ok(false) => {
      tokio::select! {
        () = retry_clock.sleep(delay) => false,
        changed = membership_rx.changed() => {
          if changed.is_err() {
            retry_clock.sleep(delay).await;
            false
          } else {
            membership_rx.borrow_and_update();
            true
          }
        },
      }
    },
    Err(_) => {
      retry_clock.sleep(delay).await;
      false
    },
  };
  if membership_updated {
    retry_clock
      .sleep(
        delay
          .min(NOT_LEASE_HOLDER_MEMBERSHIP_SETTLE_DELAY)
          .min(retry_deadline.saturating_duration_since(retry_clock.now())),
      )
      .await;
  }
  membership_updated
}

//
// ProducerRetryClock
//

#[async_trait]
/// Clock used to evaluate producer retry deadlines and backoff delays.
pub trait ProducerRetryClock: Send + Sync {
  fn now(&self) -> Instant;
  async fn sleep(&self, duration: Duration);
}

pub(super) struct TokioProducerRetryClock;

#[async_trait]
impl ProducerRetryClock for TokioProducerRetryClock {
  fn now(&self) -> Instant {
    Instant::now()
  }

  async fn sleep(&self, duration: Duration) {
    tokio::time::sleep(duration).await;
  }
}
