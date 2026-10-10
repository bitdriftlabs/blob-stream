use crate::aws::{
  DynamoRequestExt as _,
  retry_dynamo_transaction_conflicts,
  trace_dynamo_operation,
  transaction_cancellation_has_code,
};
use crate::dynamo_attributes::{
  ATTR_EPOCH,
  ATTR_EXPIRES,
  ATTR_HOLDER,
  ATTR_PK,
  ATTR_SESSION,
  ATTR_SK,
  ATTR_TTL,
};
use crate::{
  DynamoCapacityMetrics,
  MetadataReadConsistency,
  MetadataStore,
  MetadataWriteError,
  MetadataWriteResult,
  ProducerPartitionFence,
  SegmentMetadata,
  codec,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::RequestId;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::query::{QueryError, QueryOutput};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
  AttributeValue,
  ConditionCheck,
  Put,
  ReturnConsumedCapacity,
  TransactWriteItem,
};
use aws_sdk_dynamodb::{Client, Config};
use bd_backoff::{ExponentialBackoffBuilder, Finite, InfiniteBackoff as _, SystemClock};
use bd_log_util::warn_every;
use bd_runtime_config::feature_flags::{FeatureFlags, FeatureFlagsWatch, WatchedFeatureFlags};
use blob_stream_runtime_config::AwsTraceSampler;
use blob_stream_runtime_config::aws_tracing::bounded_diagnostic;
use blob_stream_types::{SnowflakeId, TopicWindowKey, offset_datetime_from_unix_seconds};
use bytes::Bytes;
use log::{debug, trace};
use parking_lot::Mutex;
use protobuf::Chars;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::Duration;
use time::ext::NumericalDuration;
use tokio::time::{Instant, sleep, timeout_at};
use tracing::{Instrument as _, Span};
use uuid::Uuid;

#[cfg(test)]
#[path = "./dynamo_test.rs"]
mod tests;

const ATTR_SEGMENT_METADATA_V1: &str = "segment_metadata_v1";
const QUERY_INITIAL_DELAY_FLAG: &str = "blob_stream_metadata_query_initial_delay_ms";
const QUERY_MAX_DELAY_FLAG: &str = "blob_stream_metadata_query_max_delay_ms";
const QUERY_PAGE_TIMEOUT_FLAG: &str = "blob_stream_metadata_query_page_timeout_ms";
pub const MAX_FENCED_METADATA_PARTITIONS: usize = 99;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QueryRetryPolicy {
  initial_delay: StdDuration,
  max_delay: StdDuration,
  page_timeout: StdDuration,
}

impl Default for QueryRetryPolicy {
  fn default() -> Self {
    Self {
      initial_delay: StdDuration::from_millis(50),
      max_delay: StdDuration::from_millis(200),
      page_timeout: StdDuration::from_secs(3),
    }
  }
}

impl QueryRetryPolicy {
  fn from_flags(flags: &dyn FeatureFlags) -> Option<Self> {
    let initial = flags.get_integer(QUERY_INITIAL_DELAY_FLAG, 50);
    let maximum = flags.get_integer(QUERY_MAX_DELAY_FLAG, 200);
    let timeout = flags.get_integer(QUERY_PAGE_TIMEOUT_FLAG, 3_000);
    if !(2 ..= 2_000).contains(&initial)
      || !(initial ..= 2_000).contains(&maximum)
      || !(maximum ..= 4_000).contains(&timeout)
    {
      return None;
    }
    Some(Self {
      initial_delay: StdDuration::from_millis(initial),
      max_delay: StdDuration::from_millis(maximum),
      page_timeout: StdDuration::from_millis(timeout),
    })
  }
}

//
// MetadataQueryThrottled
//

/// A metadata read whose `DynamoDB` throttling persisted beyond its page retry budget.
#[derive(Debug, thiserror::Error)]
#[error("metadata Query throttled after {attempts} attempts")]
pub struct MetadataQueryThrottled {
  attempts: u32,
  #[source]
  source: SdkError<QueryError>,
}

fn query_error_is_throttled(error: &SdkError<QueryError>) -> bool {
  error
    .as_service_error()
    .is_some_and(query_service_is_throttled)
}

fn query_service_is_throttled(service: &QueryError) -> bool {
  matches!(
    service,
    QueryError::ThrottlingException(_)
      | QueryError::ProvisionedThroughputExceededException(_)
      | QueryError::RequestLimitExceeded(_)
  ) || matches!(
    service.code(),
    Some("ThrottlingException" | "ProvisionedThroughputExceededException" | "RequestLimitExceeded")
  )
}

fn query_throttling_reasons(service: &QueryError) -> &[aws_sdk_dynamodb::types::ThrottlingReason] {
  match service {
    QueryError::ThrottlingException(error) => error.throttling_reasons(),
    QueryError::ProvisionedThroughputExceededException(error) => error.throttling_reasons(),
    QueryError::RequestLimitExceeded(error) => error.throttling_reasons(),
    _ => &[],
  }
}

fn exhausted_throttle(
  source: SdkError<QueryError>,
  attempts: u32,
  window: &TopicWindowKey,
) -> anyhow::Error {
  let code = source
    .as_service_error()
    .and_then(ProvideErrorMetadata::code);
  let reasons = source
    .as_service_error()
    .map(query_throttling_reasons)
    .unwrap_or_default();
  warn_every!(
    15.seconds(),
    "metadata(dynamo) Query throttle exhausted: topic={}, window_start={}, attempts={}, \
     code={code:?}, reasons={reasons:?}, error={source}",
    window.topic,
    window.window_start_unix_seconds,
    attempts
  );
  MetadataQueryThrottled { attempts, source }.into()
}

fn query_error_is_retryable(error: &SdkError<QueryError>) -> bool {
  query_error_is_throttled(error)
    || matches!(
      error,
      SdkError::TimeoutError(_) | SdkError::DispatchFailure(_)
    )
    || matches!(
      error.as_service_error(),
      Some(QueryError::InternalServerError(_))
    )
    || matches!(error, SdkError::ServiceError(service) if service.raw().status().as_u16() >= 500)
}

async fn query_page_with_retries(
  query: QueryFluentBuilder,
  window: &TopicWindowKey,
  policy: QueryRetryPolicy,
) -> Result<QueryOutput> {
  let page = bd_log::otel_info_span_if_parent!(
    "aws.dynamodb.query_page",
    aws.application_attempts = tracing::field::Empty,
    aws.last_response_request_id = tracing::field::Empty,
    aws.last_response_is_prior = tracing::field::Empty,
    aws.outcome = tracing::field::Empty,
  );
  let completion = page.clone();
  let mut attempt = 0;
  let mut last_request_id = None;
  let mut timed_out = false;
  let result = async {
    let deadline = Instant::now() + policy.page_timeout;
    // A 100% jitter factor produces delays in [0, 2 * interval].
    let mut backoff = ExponentialBackoffBuilder::<SystemClock, Finite>::new_infinite()
      .with_initial_interval(Duration::milliseconds(
        i64::try_from(policy.initial_delay.as_millis() / 2).unwrap_or(i64::MAX),
      ))
      .with_max_interval(Duration::milliseconds(
        i64::try_from(policy.max_delay.as_millis() / 2).unwrap_or(i64::MAX),
      ))
      .with_randomization_factor(1.0)
      .with_multiplier(2.0)
      .build();
    let mut last_throttle = None;
    loop {
      attempt += 1;
      let response = timeout_at(
        deadline,
        query
          .clone()
          .customize()
          .config_override(
            Config::builder()
              .retry_config(RetryConfig::disabled())
              .timeout_config(TimeoutConfig::disabled()),
          )
          .send()
          .trace_request("Query"),
      )
      .await;
      let request_id = match &response {
        Ok(Ok(output)) => output.request_id(),
        Ok(Err(error)) => error
          .raw_response()
          .and_then(|raw| raw.headers().get("x-amzn-requestid")),
        Err(_) => None,
      };
      if let Some(id) = request_id {
        last_request_id = Some(bounded_diagnostic(id).to_string());
      }
      let error = match response {
        Ok(Ok(output)) => return Ok(output),
        Ok(Err(error)) => error,
        Err(_) => {
          timed_out = true;
          return Err(last_throttle.map_or_else(
            || anyhow!("metadata Query page timed out"),
            |(attempts, source)| exhausted_throttle(source, attempts, window),
          ));
        },
      };
      let throttled = query_error_is_throttled(&error);
      if !query_error_is_retryable(&error) {
        return if throttled {
          Err(exhausted_throttle(error, attempt, window))
        } else {
          Err(error.into())
        };
      }
      debug!(
        "metadata(dynamo) Query retry: topic={}, window_start={}, attempt={}, \
         throttled={throttled}",
        window.topic, window.window_start_unix_seconds, attempt
      );
      last_throttle = throttled.then_some((attempt, error));
      let delay = backoff.next_backoff().unsigned_abs();
      if timeout_at(deadline, sleep(delay)).await.is_err() {
        timed_out = true;
        return Err(last_throttle.map_or_else(
          || anyhow!("metadata Query page timed out"),
          |(attempts, source)| exhausted_throttle(source, attempts, window),
        ));
      }
    }
  }
  .instrument(page)
  .await;
  completion.record("aws.application_attempts", attempt);
  completion.record("aws.last_response_is_prior", timed_out);
  completion.record(
    "aws.outcome",
    if timed_out {
      "deadline"
    } else if result.is_err() {
      "failure"
    } else {
      "success"
    },
  );
  if let Some(id) = last_request_id {
    completion.record("aws.last_response_request_id", id.as_str());
  }
  result
}

//
// DynamoMetadataStore
//

#[derive(Clone, Debug)]
pub struct DynamoMetadataStore {
  client: Client,
  table_name: String,
  producer_partition_lease_table_name: String,
  topic_retention: HashMap<Chars, Duration>,
  ttl_buffer: Duration,
  capacity_metrics: Option<DynamoCapacityMetrics>,
  watched_query_policy: Arc<Mutex<WatchedFeatureFlags<QueryRetryPolicy>>>,
  trace_sampler: AwsTraceSampler,
}

impl DynamoMetadataStore {
  /// Build a metadata store used only for read-only inspection.
  #[must_use]
  pub fn new_read_only(
    client: Client,
    table_name: impl Into<String>,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Self {
    Self::new(
      client,
      table_name,
      "",
      HashMap::new(),
      Duration::ZERO,
      None,
      feature_flags,
    )
  }

  #[must_use]
  pub fn new(
    client: Client,
    table_name: impl Into<String>,
    producer_partition_lease_table_name: impl Into<String>,
    topic_retention: HashMap<Chars, Duration>,
    ttl_buffer: Duration,
    capacity_metrics: Option<DynamoCapacityMetrics>,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Self {
    Self {
      client,
      table_name: table_name.into(),
      producer_partition_lease_table_name: producer_partition_lease_table_name.into(),
      topic_retention,
      ttl_buffer,
      capacity_metrics,
      trace_sampler: AwsTraceSampler::new(feature_flags.clone()),
      watched_query_policy: Arc::new(Mutex::new(WatchedFeatureFlags::new(
        feature_flags,
        Arc::new(QueryRetryPolicy::default()),
      ))),
    }
  }

  fn query_retry_policy(&self) -> QueryRetryPolicy {
    let mut watched = self.watched_query_policy.lock();
    let (policy, error) = watched.current(|flags, _| QueryRetryPolicy::from_flags(flags).ok_or(()));
    if error.is_some() {
      warn_every!(
        15.seconds(),
        "metadata(dynamo) ignoring invalid Query retry feature flags"
      );
    }
    *policy
  }

  fn record_read_capacity(
    &self,
    consumed_capacity: Option<&aws_sdk_dynamodb::types::ConsumedCapacity>,
  ) {
    if let Some(capacity_metrics) = &self.capacity_metrics {
      capacity_metrics.record_read(consumed_capacity);
    }
  }

  fn record_write_capacity(
    &self,
    consumed_capacity: Option<&aws_sdk_dynamodb::types::ConsumedCapacity>,
  ) {
    if let Some(capacity_metrics) = &self.capacity_metrics {
      capacity_metrics.record_write(consumed_capacity);
    }
  }

  fn metadata_ttl_epoch_seconds(&self, metadata: &SegmentMetadata) -> Option<i64> {
    let retention = self.topic_retention.get(metadata.window.topic.as_str())?;
    if !retention.is_positive() {
      return None;
    }

    let retention_seconds = duration_seconds_ceil(*retention)?;
    let ttl_buffer_seconds = duration_seconds_ceil(self.ttl_buffer)?;
    let created_seconds = metadata.created_at.unix_timestamp();

    created_seconds
      .checked_add(retention_seconds)?
      .checked_add(ttl_buffer_seconds)
  }
}

pub fn duration_seconds_ceil(duration: Duration) -> Option<i64> {
  duration
    .whole_seconds()
    .checked_add(i64::from(duration.subsec_nanoseconds() != 0))
}

#[async_trait]
impl MetadataStore for DynamoMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult {
    trace_dynamo_operation!(
      self,
      "write_segment",
      async {
        Span::current().record("aws.details_json", json!({"topic": bounded_diagnostic(metadata.window.topic.as_str()), "window_start": metadata.window.window_start_unix_seconds, "snowflake_id": metadata.snowflake_id.as_u64(), "fence_count": fences.map_or(0, <[ProducerPartitionFence]>::len), "producer_lease_table": bounded_diagnostic(&self.producer_partition_lease_table_name)}).to_string());
        trace!(
          "metadata(dynamo) write_segment start: table={}, topic={}, window_start={}, \
           snowflake_id={}",
          self.table_name,
          metadata.window.topic,
          offset_datetime_from_unix_seconds(metadata.window.window_start_unix_seconds),
          metadata.snowflake_id.as_u64()
        );
        if let Some(fences) = fences {
          if fences.is_empty() {
            return Err(
              anyhow!("fenced metadata publication requires at least one producer partition fence")
                .into(),
            );
          }
          if fences.len() > MAX_FENCED_METADATA_PARTITIONS {
            return Err(
              anyhow!(
                "fenced metadata publication supports at most {MAX_FENCED_METADATA_PARTITIONS} \
                 partitions"
              )
              .into(),
            );
          }
          let mut lease_keys = HashSet::with_capacity(fences.len());
          for fence in fences {
            if !lease_keys.insert(&fence.key) {
              return Err(
                anyhow!("fenced metadata publication has duplicate producer lease key").into(),
              );
            }
          }
          if fences.len() != metadata.segment_index.len()
            || !fences.iter().all(|fence| {
              fence.key.topic.as_str() == metadata.window.topic
                && metadata
                  .segment_index
                  .contains_key(&fence.key.virtual_partition_id)
            })
          {
            return Err(
              anyhow!(
                "fenced metadata publication requires producer lease fences matching segment \
                 partitions"
              )
              .into(),
            );
          }
        }

        let ttl_epoch_seconds = self.metadata_ttl_epoch_seconds(&metadata);
        let encoded = codec::encode(&metadata).map_err(MetadataWriteError::from)?;
        let mut item = HashMap::from([
          (
            ATTR_PK.to_string(),
            AttributeValue::S(encoded.partition_key),
          ),
          (ATTR_SK.to_string(), AttributeValue::S(encoded.sort_key)),
          (
            ATTR_SEGMENT_METADATA_V1.to_string(),
            AttributeValue::B(Blob::new(encoded.payload)),
          ),
        ]);
        if let Some(ttl_epoch_seconds) = ttl_epoch_seconds {
          item.insert(
            ATTR_TTL.to_string(),
            AttributeValue::N(ttl_epoch_seconds.to_string()),
          );
        }

        let Some(fences) = fences else {
          let response = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .send()
            .trace_request("PutItem")
            .await
            .map_err(|error| MetadataWriteError::Other(error.into()))?;
          self.record_write_capacity(response.consumed_capacity.as_ref());

          debug!(
            "metadata(dynamo) write_segment complete: table={}",
            self.table_name
          );
          return Ok(());
        };

        let metadata_put = Put::builder()
          .table_name(&self.table_name)
          .set_item(Some(item))
          .build()
          .map_err(|error| MetadataWriteError::Other(error.into()))?;
        let mut transaction_items = Vec::with_capacity(fences.len() + 1);
        transaction_items.push(TransactWriteItem::builder().put(metadata_put).build());
        for producer_fence in fences {
          let mut values = HashMap::new();
          values.insert(
            ":holder".to_string(),
            AttributeValue::S(producer_fence.fence.holder_id.clone()),
          );
          values.insert(
            ":epoch".to_string(),
            AttributeValue::N(producer_fence.fence.lease_epoch.to_string()),
          );
          values.insert(
            ":session".to_string(),
            AttributeValue::S(producer_fence.fence.lease_session_id.clone()),
          );
          values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
          let lease_check = ConditionCheck::builder()
            .table_name(&self.producer_partition_lease_table_name)
            .key(ATTR_PK, AttributeValue::S(producer_fence.key.format()))
            .condition_expression(format!(
              "{ATTR_HOLDER} = :holder AND {ATTR_EPOCH} = :epoch AND {ATTR_SESSION} = :session \
               AND {ATTR_EXPIRES} > :now"
            ))
            .set_expression_attribute_values(Some(values))
            .build()
            .map_err(|error| MetadataWriteError::Other(error.into()))?;
          transaction_items.push(
            TransactWriteItem::builder()
              .condition_check(lease_check)
              .build(),
          );
        }

        let client_request_token = Uuid::new_v4().to_string();
        let result = retry_dynamo_transaction_conflicts(
          "fenced_metadata_write",
          || {
            self
              .client
              .transact_write_items()
              .client_request_token(client_request_token.clone())
              .set_transact_items(Some(transaction_items.clone()))
              .return_consumed_capacity(ReturnConsumedCapacity::Total)
              .send()
              .trace_request("TransactWriteItems")
          },
          |error| {
            matches!(
              error,
              SdkError::ServiceError(service_error)
                if transaction_cancellation_has_code(service_error.err(), "TransactionConflict")
            )
          },
        )
        .await;
        match result {
          Ok(output) => {
            for capacity in output.consumed_capacity.as_deref().unwrap_or_default() {
              self.record_write_capacity(Some(capacity));
            }
            debug!(
              "metadata(dynamo) fenced write complete: table={}, partitions={}",
              self.table_name,
              fences.len()
            );
            Ok(())
          },
          Err(SdkError::ServiceError(service_error))
            if transaction_cancellation_has_code(service_error.err(), "ConditionalCheckFailed") =>
          {
            Err(MetadataWriteError::ProducerLeaseFenceLost)
          },
          Err(error) => Err(MetadataWriteError::Other(error.into())),
        }
      },
      |error| matches!(error, MetadataWriteError::ProducerLeaseFenceLost)
    )
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    trace_dynamo_operation!(self, "scan_window_from_snowflake", async {
      Span::current().record("aws.details_json", json!({"topic": bounded_diagnostic(&window.topic), "window_start": window.window_start_unix_seconds, "min_snowflake": min_snowflake.map(SnowflakeId::as_u64), "consistency": if matches!(consistency, MetadataReadConsistency::Strong) { "strong" } else { "eventual" }}).to_string());
      trace!(
        "metadata(dynamo) scan_window start: table={}, topic={}, window_start={}, \
         min_snowflake={:?}, consistency={consistency:?}",
        self.table_name,
        window.topic,
        offset_datetime_from_unix_seconds(window.window_start_unix_seconds),
        min_snowflake.map(SnowflakeId::as_u64)
      );

      let mut segments = Vec::new();
      let mut start_key = None;
      loop {
        let mut values = HashMap::new();
        values.insert(":pk".to_string(), AttributeValue::S(window.format()));
        if let Some(min_snowflake) = min_snowflake {
          values.insert(
            ":min_snowflake".to_string(),
            AttributeValue::S(min_snowflake.format_lex()),
          );
        }

        let key_condition = if min_snowflake.is_some() {
          format!("{ATTR_PK} = :pk AND sk >= :min_snowflake")
        } else {
          format!("{ATTR_PK} = :pk")
        };
        let mut query = self
          .client
          .query()
          .table_name(&self.table_name)
          .key_condition_expression(key_condition)
          .projection_expression(format!("{ATTR_PK}, {ATTR_SK}, {ATTR_SEGMENT_METADATA_V1}"))
          .set_expression_attribute_values(Some(values))
          .consistent_read(matches!(consistency, MetadataReadConsistency::Strong));
        if let Some(key) = start_key.clone() {
          query = query.set_exclusive_start_key(Some(key));
        }

        let response = query_page_with_retries(
          query.return_consumed_capacity(ReturnConsumedCapacity::Total),
          window,
          self.query_retry_policy(),
        )
        .await?;
        self.record_read_capacity(response.consumed_capacity.as_ref());
        for item in response.items.unwrap_or_default() {
          match Self::decode_item(item) {
            Ok(metadata) => segments.push(metadata),
            Err(error) => {
              warn_every!(
                15.seconds(),
                "metadata(dynamo) skipped noncompliant segment: {error}"
              );
            },
          }
        }

        let Some(key) = response.last_evaluated_key else {
          break;
        };
        start_key = Some(key);
      }

      debug!(
        "metadata(dynamo) scan_window complete: table={}, segments={}",
        self.table_name,
        segments.len()
      );
      Ok(segments)
    })
  }
}

impl DynamoMetadataStore {
  fn decode_item(mut item: HashMap<String, AttributeValue>) -> Result<SegmentMetadata> {
    let partition_key = Self::take_string_attribute(&mut item, ATTR_PK)?;
    let sort_key = Self::take_string_attribute(&mut item, ATTR_SK)?;
    let payload = item
      .remove(ATTR_SEGMENT_METADATA_V1)
      .ok_or_else(|| anyhow!("metadata row {partition_key}/{sort_key} is missing v1 payload"))?;
    let payload = match payload {
      AttributeValue::B(payload) => Bytes::from(payload.into_inner()),
      _ => {
        return Err(anyhow!(
          "metadata row {partition_key}/{sort_key} has a non-binary v1 payload"
        ));
      },
    };
    codec::decode(&partition_key, &sort_key, &payload)
  }

  fn take_string_attribute(
    item: &mut HashMap<String, AttributeValue>,
    attribute_name: &str,
  ) -> Result<String> {
    match item.remove(attribute_name) {
      Some(AttributeValue::S(value)) => Ok(value),
      Some(_) => Err(anyhow!("metadata row has a non-string {attribute_name}")),
      None => Err(anyhow!("metadata row is missing {attribute_name}")),
    }
  }
}
