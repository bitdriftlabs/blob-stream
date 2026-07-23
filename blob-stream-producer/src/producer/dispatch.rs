use super::metrics::ProducerMetrics;
use super::retry::{ProducerRetryClock, acknowledge_batch, send_batch_with_retry};
use super::routing::{BrokerBatchGroup, ProducerRoutes, produce_batch_request};
use super::state::BufferedBatch;
use super::{BrokerTransport, ProducerAck, ProducerError, ProducerRetryDiagnostics};
use crate::config::{
  ProducerConfig,
  ProducerTopicConfig,
  producer_request_timeout_ms,
  producer_retry_deadline_ms,
};
use anyhow::anyhow;
use bd_log::warn_every;
use blob_stream_broker_discovery::BrokerMembership;
use blob_stream_proto::protos::blobstream::v1::broker::{ProduceBatchesRequest, ProduceStatus};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use protobuf::Chars;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use time::ext::NumericalDuration;
use tokio::sync::{Semaphore, oneshot, watch};

pub(super) async fn dispatch_group_and_notify(
  config: &ProducerConfig,
  topics: &HashMap<Chars, ProducerTopicConfig>,
  routes: &ProducerRoutes,
  membership_rx: &watch::Receiver<BrokerMembership>,
  transport: &Arc<dyn BrokerTransport>,
  metrics: &Arc<ProducerMetrics>,
  retry_diagnostics: &ProducerRetryDiagnostics,
  retry_clock: &Arc<dyn ProducerRetryClock>,
  dispatch_permits: &Arc<Semaphore>,
  group: BrokerBatchGroup,
) -> Result<(), ProducerError> {
  send_grouped_batches_and_notify(
    config,
    topics,
    routes,
    membership_rx,
    transport.as_ref(),
    metrics,
    retry_diagnostics,
    retry_clock.as_ref(),
    dispatch_permits,
    group,
  )
  .await
}

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

async fn send_grouped_batches_and_notify(
  config: &ProducerConfig,
  topics: &HashMap<Chars, ProducerTopicConfig>,
  routes: &ProducerRoutes,
  membership_rx: &watch::Receiver<BrokerMembership>,
  transport: &dyn BrokerTransport,
  metrics: &ProducerMetrics,
  retry_diagnostics: &ProducerRetryDiagnostics,
  retry_clock: &dyn ProducerRetryClock,
  dispatch_permits: &Arc<Semaphore>,
  group: BrokerBatchGroup,
) -> Result<(), ProducerError> {
  let retry_started_at = retry_clock.now();

  // The grouped RPC amortizes transport overhead, but each response remains an independent
  // logical batch. A terminal response can therefore notify its record waiters immediately.
  let request = ProduceBatchesRequest {
    batches: group.batches.iter().map(produce_batch_request).collect(),
    ..Default::default()
  };
  let request_timeout = Duration::from_millis(
    u64::try_from(producer_request_timeout_ms(config))
      .expect("producer config validation requires a positive request timeout"),
  )
  .min(Duration::from_millis(producer_retry_deadline_ms(config)));
  let response = {
    let Ok(_permit) = dispatch_permits.clone().acquire_owned().await else {
      let result = Err(ProducerError::Shutdown);
      for batch in group.batches {
        notify_waiters(batch.waiters, &result);
      }
      return Err(ProducerError::Shutdown);
    };
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
  let mut retries = FuturesUnordered::new();

  // Do not route successful responses through retry handling. The remaining futures are driven
  // together so one batch's backoff or broker handoff cannot delay another batch in this group.
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
      _ => {
        retries.push(async {
          let result = send_batch_with_retry(
            config,
            topics,
            routes,
            membership_rx,
            transport,
            &batch,
            dispatch_permits,
            metrics,
            retry_diagnostics,
            initial_response,
            1,
            retry_clock,
            retry_started_at,
          )
          .await;
          notify_waiters(batch.waiters, &result);
          result
        });
      },
    }
  }

  while let Some(result) = retries.next().await {
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
