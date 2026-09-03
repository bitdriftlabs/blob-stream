#[cfg(test)]
#[path = "./aws_test.rs"]
mod tests;

use crate::{ConsumerGroupLeaseStore, DynamoCapacityMetrics, DynamoConsumerGroupLeaseStore};
use aws_config::meta::region::RegionProviderChain;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use bd_backoff::{ExponentialBackoffBuilder, Finite, FiniteBackoff as _, SystemClock};
use log::trace;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use time::Duration as TimeDuration;
use tokio::time::sleep;

const MAX_ATTEMPTS: u32 = 4;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const OPERATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const TRANSACTION_CONFLICT_RETRY_BUDGET: Duration = Duration::from_millis(175);
const TRANSACTION_CONFLICT_INITIAL_DELAY: Duration = Duration::from_millis(25);
const TRANSACTION_CONFLICT_MAX_DELAY: Duration = Duration::from_millis(100);

#[must_use]
pub fn aws_retry_config() -> RetryConfig {
  RetryConfig::standard().with_max_attempts(MAX_ATTEMPTS)
}

#[must_use]
pub fn aws_timeout_config() -> TimeoutConfig {
  TimeoutConfig::builder()
    .operation_timeout(OPERATION_TIMEOUT)
    .operation_attempt_timeout(OPERATION_ATTEMPT_TIMEOUT)
    .build()
}

/// Build a `DynamoDB` client with Blob Stream's standard retry and timeout policy.
pub async fn build_dynamo_client(region: &str, endpoint: &str) -> Client {
  let region_provider = RegionProviderChain::first_try(Some(Region::new(region.to_string())));
  let mut loader = aws_config::defaults(BehaviorVersion::latest())
    .region(region_provider)
    .retry_config(aws_retry_config())
    .timeout_config(aws_timeout_config());
  if !endpoint.trim().is_empty() {
    loader = loader.endpoint_url(endpoint.to_string());
  }
  Client::new(&loader.load().await)
}

/// Build the consumer-group lease store from a client shared with the other Dynamo stores.
#[must_use]
pub fn build_dynamo_consumer_group_lease_store(
  client: Client,
  table_name: impl Into<String>,
  ttl_buffer: TimeDuration,
  capacity_metrics: Option<DynamoCapacityMetrics>,
) -> Arc<dyn ConsumerGroupLeaseStore> {
  Arc::new(DynamoConsumerGroupLeaseStore::new(
    client,
    table_name,
    ttl_buffer,
    capacity_metrics,
  ))
}

/// Returns whether a `DynamoDB` operation failed with a direct transaction conflict.
///
/// Single-item operations, such as `UpdateItem`, report this as
/// `TransactionConflictException`. Transactional writes report per-item cancellation reasons and
/// use [`transaction_cancellation_has_code`] instead.
pub fn is_dynamo_transaction_conflict<E, R>(error: &SdkError<E, R>) -> bool
where
  E: ProvideErrorMetadata,
{
  error
    .as_service_error()
    .is_some_and(|service_error| service_error.code() == Some("TransactionConflictException"))
}

/// Returns whether a transactional write was canceled with a specific per-item reason code.
pub fn transaction_cancellation_has_code(
  error: &TransactWriteItemsError,
  expected_code: &str,
) -> bool {
  matches!(
    error,
    TransactWriteItemsError::TransactionCanceledException(cancellation)
      if cancellation
        .cancellation_reasons()
        .iter()
        .any(|reason| reason.code() == Some(expected_code))
  )
}

/// Retry short-lived `DynamoDB` transaction conflicts outside the SDK retry classifier.
pub async fn retry_dynamo_transaction_conflicts<T, E, F, Fut, IsConflict>(
  operation_name: &str,
  mut operation: F,
  is_transaction_conflict: IsConflict,
) -> Result<T, E>
where
  F: FnMut() -> Fut,
  Fut: Future<Output = Result<T, E>>,
  IsConflict: Fn(&E) -> bool,
{
  let mut backoff = ExponentialBackoffBuilder::<SystemClock, Finite>::new()
    .with_max_elapsed_time(
      TimeDuration::try_from(TRANSACTION_CONFLICT_RETRY_BUDGET).unwrap_or(TimeDuration::MAX),
    )
    .with_initial_interval(
      TimeDuration::try_from(TRANSACTION_CONFLICT_INITIAL_DELAY).unwrap_or(TimeDuration::MAX),
    )
    .with_multiplier(2.0)
    .with_max_interval(
      TimeDuration::try_from(TRANSACTION_CONFLICT_MAX_DELAY).unwrap_or(TimeDuration::MAX),
    )
    .build();
  let mut retry = 0;

  loop {
    match operation().await {
      Ok(value) => return Ok(value),
      Err(error) if is_transaction_conflict(&error) => match backoff.next_backoff() {
        Some(delay) => {
          retry += 1;
          trace!(
            "DynamoDB transaction conflict retrying: operation={operation_name}, retry={retry}, \
             delay_ms={}",
            delay.whole_milliseconds()
          );
          sleep(delay.unsigned_abs()).await;
        },
        None => return Err(error),
      },
      Err(error) => return Err(error),
    }
  }
}
