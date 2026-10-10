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
use aws_sdk_dynamodb::operation::RequestId;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput;
use aws_sdk_dynamodb::operation::get_item::GetItemOutput;
use aws_sdk_dynamodb::operation::put_item::PutItemOutput;
use aws_sdk_dynamodb::operation::query::{QueryError, QueryOutput};
use aws_sdk_dynamodb::operation::scan::ScanOutput;
use aws_sdk_dynamodb::operation::transact_write_items::{
  TransactWriteItemsError,
  TransactWriteItemsOutput,
};
use aws_sdk_dynamodb::operation::update_item::UpdateItemOutput;
use aws_sdk_dynamodb::types::ConsumedCapacity;
use bd_backoff::{ExponentialBackoffBuilder, Finite, FiniteBackoff as _, SystemClock};
use blob_stream_runtime_config::aws_tracing::{bounded_diagnostic, next_request_index};
use log::trace;
use serde_json::{Value, json};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use time::Duration as TimeDuration;
use tokio::time::sleep;
use tracing::Instrument as _;

macro_rules! trace_dynamo_operation {
  ($store:expr, $operation:literal, $future:expr $(, $expected:expr)?) => {
    blob_stream_runtime_config::aws_tracing::instrument_operation(
      &$store.trace_sampler,
      blob_stream_runtime_config::AWS_DYNAMODB_TRACE_SAMPLE_RATE,
      "DynamoDB",
      $operation,
      $store.client.config().region().map_or("", |region| region.as_ref()),
      &$store.table_name,
      $future,
      trace_dynamo_operation!(@expected $($expected)?),
    ).await
  };
  (@expected $expected:expr) => { $expected };
  (@expected) => { |_| false };
}

pub(crate) use trace_dynamo_operation;

pub trait DynamoResponseDetails: RequestId {
  fn details(&self) -> Value;
}

fn capacity_details(capacity: Option<&ConsumedCapacity>) -> Value {
  json!({"capacity_units": capacity.and_then(ConsumedCapacity::capacity_units)})
}

macro_rules! response_capacity_details {
  ($($output:ty),+) => {
    $(impl DynamoResponseDetails for $output {
      fn details(&self) -> Value { capacity_details(self.consumed_capacity.as_ref()) }
    })+
  };
}

response_capacity_details!(
  GetItemOutput,
  PutItemOutput,
  UpdateItemOutput,
  DeleteItemOutput
);

impl DynamoResponseDetails for QueryOutput {
  fn details(&self) -> Value {
    json!({"count": self.count, "scanned_count": self.scanned_count, "capacity": capacity_details(self.consumed_capacity.as_ref())})
  }
}

impl DynamoResponseDetails for ScanOutput {
  fn details(&self) -> Value {
    json!({"count": self.count, "scanned_count": self.scanned_count, "capacity": capacity_details(self.consumed_capacity.as_ref())})
  }
}

impl DynamoResponseDetails for TransactWriteItemsOutput {
  fn details(&self) -> Value {
    json!({"capacities": self.consumed_capacity().iter().take(16).map(|capacity| json!({"table": capacity.table_name().map(bounded_diagnostic), "units": capacity.capacity_units()})).collect::<Vec<_>>(), "truncated": self.consumed_capacity().len() > 16})
  }
}

pub trait DynamoErrorDetails: ProvideErrorMetadata {
  fn details(&self) -> Value {
    Value::Null
  }
}

macro_rules! empty_error_details {
  ($($error:ty),+) => { $(impl DynamoErrorDetails for $error {})+ };
}

empty_error_details!(
  aws_sdk_dynamodb::operation::get_item::GetItemError,
  aws_sdk_dynamodb::operation::put_item::PutItemError,
  aws_sdk_dynamodb::operation::update_item::UpdateItemError,
  aws_sdk_dynamodb::operation::delete_item::DeleteItemError,
  aws_sdk_dynamodb::operation::scan::ScanError
);

impl DynamoErrorDetails for QueryError {
  fn details(&self) -> Value {
    let reasons = match self {
      Self::ThrottlingException(error) => error.throttling_reasons(),
      Self::ProvisionedThroughputExceededException(error) => error.throttling_reasons(),
      Self::RequestLimitExceeded(error) => error.throttling_reasons(),
      _ => &[],
    };
    json!({"throttling_reasons": reasons.iter().take(16).map(|reason| json!({"reason": reason.reason().map(bounded_diagnostic), "resource": reason.resource().map(bounded_diagnostic)})).collect::<Vec<_>>(), "truncated": reasons.len() > 16})
  }
}

impl DynamoErrorDetails for TransactWriteItemsError {
  fn details(&self) -> Value {
    match self {
      Self::TransactionCanceledException(error) => {
        json!({"cancellation_codes": error.cancellation_reasons().iter().take(100).map(|reason| reason.code().map(bounded_diagnostic)).collect::<Vec<_>>(), "truncated": error.cancellation_reasons().len() > 100})
      },
      _ => Value::Null,
    }
  }
}

pub trait DynamoRequestExt<T, E>: Future<Output = Result<T, SdkError<E>>> + Sized
where
  T: DynamoResponseDetails,
  E: DynamoErrorDetails,
{
  fn trace_request(self, method: &'static str) -> impl Future<Output = Result<T, SdkError<E>>> {
    async move {
      let span = bd_log::otel_info_span_if_parent!(
        "aws.dynamodb.request",
        rpc.method = method,
        aws.request_index = next_request_index(),
        aws.request_id = tracing::field::Empty,
        aws.error_category = tracing::field::Empty,
        aws.error_code = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        aws.details_json = tracing::field::Empty,
      );
      let completion = span.clone();
      let response = self.instrument(span).await;
      match &response {
        Ok(output) => {
          completion.record("aws.details_json", output.details().to_string());
          if let Some(id) = output.request_id() {
            completion.record("aws.request_id", bounded_diagnostic(id));
          }
        },
        Err(error) => {
          let category = match error {
            SdkError::ConstructionFailure(_) => "construction",
            SdkError::TimeoutError(_) => "timeout",
            SdkError::DispatchFailure(_) => "dispatch",
            SdkError::ResponseError(_) => "response_parse",
            SdkError::ServiceError(_) => "service",
            _ => "unknown",
          };
          completion.record("aws.error_category", category);
          if let Some(code) = error
            .as_service_error()
            .and_then(ProvideErrorMetadata::code)
          {
            completion.record("aws.error_code", bounded_diagnostic(code));
          }
          if let Some(service) = error.as_service_error() {
            completion.record("aws.details_json", service.details().to_string());
          }
          if let Some(raw) = error.raw_response() {
            if let Some(id) = raw.headers().get("x-amzn-requestid") {
              completion.record("aws.request_id", bounded_diagnostic(id));
            }
            completion.record("http.response.status_code", raw.status().as_u16());
          }
        },
      }
      response
    }
  }
}

impl<F, T, E> DynamoRequestExt<T, E> for F
where
  F: Future<Output = Result<T, SdkError<E>>>,
  T: DynamoResponseDetails,
  E: DynamoErrorDetails,
{
}

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
    None,
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
