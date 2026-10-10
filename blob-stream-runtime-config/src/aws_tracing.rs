use crate::AwsTraceSampler;
use bd_log::otel::instrument_on_error_or_sample;
use std::cell::Cell;
use std::fmt::Display;
use std::future::Future;
use tokio::task_local;

task_local! {
  static AWS_OPERATION: OperationState;
}

struct OperationState {
  service: &'static str,
  expected: Cell<bool>,
  requests: Cell<u32>,
}

pub fn mark_expected_outcome() {
  let _ = AWS_OPERATION.try_with(|state| state.expected.set(true));
}

#[must_use]
pub fn next_request_index() -> u32 {
  AWS_OPERATION
    .try_with(|state| {
      let index = state.requests.get().saturating_add(1);
      state.requests.set(index);
      index
    })
    .unwrap_or(1)
}

/// Trace the final store result, including response decoding and application retries.
/// Nested helpers in the same service belong to the outer operation's retention decision.
pub async fn instrument_operation<F, T, E, Expected>(
  sampler: &AwsTraceSampler,
  sample_flag: &str,
  service: &'static str,
  operation: &'static str,
  region: &str,
  resource: &str,
  future: F,
  expected: Expected,
) -> Result<T, E>
where
  F: Future<Output = Result<T, E>>,
  E: Display,
  Expected: Fn(&E) -> bool,
{
  if AWS_OPERATION
    .try_with(|active| active.service == service)
    .unwrap_or(false)
  {
    return future.await;
  }
  let sampled = sampler.sample(sample_flag);
  let span = bd_log::otel_span_on_error!(
    "blob_stream.aws",
    rpc.system = "aws-api",
    rpc.service = service,
    blob_stream.operation = operation,
    aws.region = region,
    aws.resource = resource,
    aws.outcome = tracing::field::Empty,
    aws.retention_reason = tracing::field::Empty,
    aws.details_json = tracing::field::Empty,
  );
  let completion = span.clone();
  let future = async move {
    let result = future.await;
    let expected_error = result.as_ref().err().is_some_and(expected);
    let is_expected = expected_error
      || AWS_OPERATION
        .try_with(|state| state.expected.get())
        .unwrap_or(false);
    let failed = result.is_err() && !expected_error;
    completion.record(
      "aws.outcome",
      if failed {
        "failure"
      } else if is_expected {
        "expected"
      } else {
        "success"
      },
    );
    completion.record(
      "aws.retention_reason",
      if failed {
        "error"
      } else if sampled {
        "sample"
      } else {
        "none"
      },
    );
    match result {
      Err(error) if failed => Err(error),
      other => Ok(other),
    }
  };
  let state = OperationState {
    service,
    expected: Cell::new(false),
    requests: Cell::new(0),
  };
  instrument_on_error_or_sample(AWS_OPERATION.scope(state, future), span, sampled)
    .await
    .and_then(std::convert::identity)
}

/// Bound an individual diagnostic string without emitting a misleading partial support ID.
#[must_use]
pub fn bounded_diagnostic(value: &str) -> &str {
  if value.len() <= 1_024 {
    value
  } else {
    "[oversized diagnostic omitted]"
  }
}
