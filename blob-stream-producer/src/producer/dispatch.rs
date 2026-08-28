use super::metrics::ProducerMetrics;
use super::protocol::broker_error_message;
use super::retry::{ProducerRetryClock, acknowledge_batch, send_batch_with_retry};
use super::routing::{BrokerBatchGroup, ProducerRoutes, produce_batch_request};
use super::state::BufferedBatch;
use super::{BrokerTransport, ProducerAck, ProducerError, ProducerRetryDiagnostics};
use crate::config::{
  ProducerConfig,
  ProducerTopicConfig,
  producer_request_timeout,
  producer_retry_deadline,
};
use anyhow::anyhow;
use bd_log_util::warn_every;
use blob_stream_broker_discovery::BrokerMembership;
use blob_stream_proto::protos::blobstream::v1::broker::{ProduceBatchesRequest, ProduceStatus};
use futures::stream::{FuturesUnordered, StreamExt};
use protobuf::Chars;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use time::ext::NumericalDuration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};

pub(super) fn notify_unassigned_batches(
  metrics: &ProducerMetrics,
  batches: Vec<BufferedBatch>,
) -> Result<(), ProducerError> {
  let has_unassigned_batches = !batches.is_empty();
  for batch in batches {
    metrics.no_brokers.inc();
    metrics.failures.inc();
    notify_waiters(batch.waiters, &Err(ProducerError::NoBrokersAvailable));
  }
  if has_unassigned_batches {
    return Err(ProducerError::NoBrokersAvailable);
  }
  Ok(())
}

fn notify_waiters(
  waiters: Vec<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
  result: &Result<ProducerAck, ProducerError>,
) {
  for waiter in waiters {
    let _ = waiter.send(result.clone());
  }
}

pub(super) async fn send_grouped_batches_and_notify(
  config: &ProducerConfig,
  topics: &HashMap<Chars, ProducerTopicConfig>,
  routes: &ProducerRoutes,
  membership_rx: &watch::Receiver<BrokerMembership>,
  transport: &dyn BrokerTransport,
  metrics: &ProducerMetrics,
  retry_diagnostics: &ProducerRetryDiagnostics,
  retry_clock: &dyn ProducerRetryClock,
  dispatch_task_permit: OwnedSemaphorePermit,
  initial_request_permit: OwnedSemaphorePermit,
  request_permits: Arc<Semaphore>,
  group: BrokerBatchGroup,
) -> Result<(), ProducerError> {
  // The task permit bounds retained group work while request permits bound active transport calls.
  let _dispatch_task_permit = dispatch_task_permit;
  let retry_started_at = retry_clock.now();

  // The grouped RPC amortizes transport overhead, but each response remains an independent
  // logical batch. A terminal response can therefore notify its record waiters immediately.
  let request = ProduceBatchesRequest {
    batches: group.batches.iter().map(produce_batch_request).collect(),
    ..Default::default()
  };
  let request_timeout = Duration::try_from(producer_request_timeout(config))
    .expect("producer config validation requires a positive request timeout")
    .min(
      Duration::try_from(producer_retry_deadline(config))
        .expect("producer config validation requires a positive retry deadline"),
    );
  let response = {
    let _initial_request_permit = initial_request_permit;
    let _active_request = bd_server_stats::stats::StackAutoGauge::new(&metrics.active_requests);
    tokio::time::timeout(
      request_timeout,
      transport.produce_batches(&group.broker_address, request, request_timeout),
    )
    .await
    .unwrap_or_else(|_| {
      Err(anyhow!(
        "producer request timed out after {} ms",
        request_timeout.as_millis()
      ))
    })
  };
  let result_count = group.batches.len();
  let results = match response {
    Ok(response) if response.results.len() == result_count => {
      response.results.into_iter().map(Some).collect::<Vec<_>>()
    },
    Ok(response) => {
      warn_every!(
        15.seconds(),
        "producer received {} results for {} grouped batches from broker {}",
        response.results.len(),
        group.batches.len(),
        group.broker_address,
      );
      std::iter::repeat_with(|| None).take(result_count).collect()
    },
    Err(error) => {
      warn_every!(
        15.seconds(),
        "producer grouped request failed for broker {}: {error}",
        group.broker_address,
      );
      std::iter::repeat_with(|| None).take(result_count).collect()
    },
  };

  let mut first_error = None;
  let mut retryable_batches = Vec::new();

  // Handle terminal statuses before beginning any retry backoff. A retryable batch must not delay
  // an acknowledgement or rejection already returned for a later batch in the same grouped RPC.
  for (batch, initial_response) in group.batches.into_iter().zip(results) {
    match initial_response
      .as_ref()
      .map(|response| response.status.enum_value_or_default())
    {
      Some(ProduceStatus::PRODUCE_STATUS_OK) => {
        let result = Ok(acknowledge_batch(
          &batch,
          metrics,
          1,
          retry_clock,
          retry_started_at,
        ));
        notify_waiters(batch.waiters, &result);
      },
      Some(ProduceStatus::PRODUCE_STATUS_UNKNOWN_TOPIC) => {
        let result = Err(ProducerError::UnknownTopic(batch.topic.clone()));
        notify_waiters(batch.waiters, &result);
        remember_first_error(&mut first_error, result);
      },
      Some(ProduceStatus::PRODUCE_STATUS_BAD_REQUEST) => {
        let response = initial_response.expect("bad request response must be present");
        let result = Err(ProducerError::Rejected(broker_error_message(&response)));
        notify_waiters(batch.waiters, &result);
        remember_first_error(&mut first_error, result);
      },
      _ => {
        retryable_batches.push((batch, initial_response));
      },
    }
  }

  // Retry futures remain owned by this bounded dispatch task. Each future acquires request
  // capacity independently, so one retryable batch cannot block sibling retry admission.
  let mut retries = FuturesUnordered::new();
  for (batch, initial_response) in retryable_batches {
    let request_permits = Arc::clone(&request_permits);
    retries.push(async move {
      let result = send_batch_with_retry(
        config,
        topics,
        routes,
        membership_rx,
        transport,
        &batch,
        metrics,
        retry_diagnostics,
        &request_permits,
        initial_response,
        1,
        retry_clock,
        retry_started_at,
      )
      .await;
      (batch, result)
    });
  }
  while let Some((batch, result)) = retries.next().await {
    notify_waiters(batch.waiters, &result);
    remember_first_error(&mut first_error, result);
  }
  first_error.map_or(Ok(()), Err)
}

pub(super) fn remember_first_error<T>(
  first_error: &mut Option<ProducerError>,
  result: Result<T, ProducerError>,
) {
  if let Err(error) = result
    && first_error.is_none()
  {
    *first_error = Some(error);
  }
}
