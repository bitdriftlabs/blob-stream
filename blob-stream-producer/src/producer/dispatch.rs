use super::metrics::ProducerMetrics;
use super::protocol::broker_error_message;
use super::retry::{
  acknowledge_batch,
  acquire_request_permit,
  retry_deadline_exhausted,
  send_batch_with_retry,
};
use super::routing::{BrokerBatchGroup, produce_batch_request};
use super::state::BufferedBatch;
use super::{ProducerAck, ProducerDispatchContext, ProducerError};
use crate::config::{producer_request_timeout, producer_retry_deadline};
use anyhow::anyhow;
use bd_log_util::warn_every;
use blob_stream_proto::protos::blobstream::v1::broker::{ProduceBatchesRequest, ProduceStatus};
use futures::stream::{FuturesUnordered, StreamExt};
use std::time::Duration;
use time::ext::NumericalDuration;
use tokio::sync::OwnedSemaphorePermit;

pub(super) fn notify_unassigned_batches(
  metrics: &ProducerMetrics,
  batches: Vec<BufferedBatch>,
) -> Result<(), ProducerError> {
  let has_unassigned_batches = !batches.is_empty();
  for batch in batches {
    metrics.no_brokers.inc();
    metrics.failures.inc();
    notify_completions(batch.completions, &Err(ProducerError::NoBrokersAvailable));
  }
  if has_unassigned_batches {
    return Err(ProducerError::NoBrokersAvailable);
  }
  Ok(())
}

fn notify_completions(
  completions: Vec<super::state::BulkCompletionSpan>,
  result: &Result<ProducerAck, ProducerError>,
) {
  for completion in completions {
    completion.complete(result);
  }
}

pub(super) async fn send_grouped_batches_and_notify(
  context: &ProducerDispatchContext,
  dispatch_task_permit: OwnedSemaphorePermit,
  group: BrokerBatchGroup,
) -> Result<(), ProducerError> {
  // The task permit bounds retained group work while request permits bound active transport calls.
  let _dispatch_task_permit = dispatch_task_permit;
  let retry_started_at = context.retry_clock.now();

  // The grouped RPC amortizes transport overhead, but each response remains an independent
  // logical batch. A terminal response can therefore complete its bulk spans immediately.
  let request = ProduceBatchesRequest {
    batches: group.batches.iter().map(produce_batch_request).collect(),
    ..Default::default()
  };
  let retry_deadline = retry_started_at
    + Duration::try_from(producer_retry_deadline(&context.config))
      .expect("producer config validation requires a positive retry deadline");
  let request_timeout = Duration::try_from(producer_request_timeout(&context.config))
    .expect("producer config validation requires a positive request timeout")
    .min(
      Duration::try_from(producer_retry_deadline(&context.config))
        .expect("producer config validation requires a positive retry deadline"),
    );
  let response = match acquire_request_permit(
    &context.request_permits,
    &context.config,
    &context.metrics,
    context.retry_clock.as_ref(),
    retry_deadline,
    retry_started_at,
  )
  .await
  {
    Ok(request_permit) => {
      let request_timeout =
        request_timeout.min(retry_deadline.saturating_duration_since(context.retry_clock.now()));
      if request_timeout.is_zero() {
        drop(request_permit);
        Err(anyhow!(retry_deadline_exhausted(
          &context.config,
          &context.metrics,
          context.retry_clock.as_ref(),
          retry_started_at,
        )))
      } else {
        let _active_request =
          bd_server_stats::stats::StackAutoGauge::new(&context.metrics.active_requests);
        let response = tokio::time::timeout(
          request_timeout,
          context
            .transport
            .produce_batches(&group.broker_address, request, request_timeout),
        )
        .await
        .unwrap_or_else(|_| {
          Err(anyhow!(
            "producer request timed out after {} ms",
            request_timeout.as_millis()
          ))
        });
        drop(request_permit);
        response
      }
    },
    Err(error) => Err(anyhow!(error)),
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
          &context.metrics,
          1,
          context.retry_clock.as_ref(),
          retry_started_at,
        ));
        notify_completions(batch.completions, &result);
      },
      Some(ProduceStatus::PRODUCE_STATUS_UNKNOWN_TOPIC) => {
        let result = Err(ProducerError::UnknownTopic(batch.topic.clone()));
        notify_completions(batch.completions, &result);
        remember_first_error(&mut first_error, result);
      },
      Some(ProduceStatus::PRODUCE_STATUS_BAD_REQUEST) => {
        let response = initial_response.expect("bad request response must be present");
        let result = Err(ProducerError::Rejected(broker_error_message(&response)));
        notify_completions(batch.completions, &result);
        remember_first_error(&mut first_error, result);
      },
      _ => {
        retryable_batches.push((batch, initial_response));
      },
    }
  }

  // Retry futures remain owned by this bounded dispatch task. Each future acquires request
  // capacity independently, so one retryable batch cannot block sibling retry admission. A very
  // large overloaded grouped request can still create many waiting futures; cap this fan-out if
  // retries become a material source of allocation or scheduler pressure.
  let mut retries = FuturesUnordered::new();
  for (batch, initial_response) in retryable_batches {
    retries.push(async move {
      let result = send_batch_with_retry(
        &context.config,
        &context.topics,
        &context.routes,
        &context.membership_rx,
        context.transport.as_ref(),
        &batch,
        &context.metrics,
        &context.retry_diagnostics,
        &context.request_permits,
        initial_response,
        1,
        context.retry_clock.as_ref(),
        retry_started_at,
      )
      .await;
      (batch, result)
    });
  }
  while let Some((batch, result)) = retries.next().await {
    notify_completions(batch.completions, &result);
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
