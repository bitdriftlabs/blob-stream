use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use bd_backoff::{ExponentialBackoffBuilder, Finite, FiniteBackoff as _, SystemClock};
use log::trace;
use std::future::Future;
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
