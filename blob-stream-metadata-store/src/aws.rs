use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use std::time::Duration;

const MAX_ATTEMPTS: u32 = 4;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const OPERATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

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
