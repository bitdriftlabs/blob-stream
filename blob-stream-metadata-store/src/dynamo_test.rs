use crate::aws::transaction_cancellation_has_code;
use crate::dynamo::{
  QUERY_PAGE_TIMEOUT_FLAG,
  QueryRetryPolicy,
  query_page_with_retries,
  query_service_is_throttled,
};
use crate::tests::dynamo_client;
use crate::{DynamoMetadataStore, MetadataReadConsistency, MetadataStore, SegmentMetadata};
use anyhow::{Context, Result, anyhow};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::error::{
  ProvisionedThroughputExceededException,
  RequestLimitExceeded,
  ThrottlingException,
  TransactionCanceledException,
};
use aws_sdk_dynamodb::types::{
  AttributeDefinition,
  AttributeValue,
  BillingMode,
  CancellationReason,
  KeySchemaElement,
  KeyType,
  ScalarAttributeType,
};
use bd_log::test::TestTraceContext;
use bd_runtime_config::loader::Loader;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_blob_store::BlobKey;
use blob_stream_proto::protos::blobstream::v1::metadata::SegmentMetadataV1;
use blob_stream_runtime_config::AWS_DYNAMODB_TRACE_SAMPLE_RATE;
use blob_stream_types::{
  BatchMetadata,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use protobuf::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::sleep;
use uuid::Uuid;

const TTL_ATTRIBUTE_NAME: &str = "ttl_epoch_seconds";

#[tokio::test]
async fn aws_tracing_dynamo_preserves_page_ids_and_final_results() {
  for sampled in [false, true] {
    for failed in [false, true] {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let address = listener.local_addr().unwrap();
      let server = tokio::spawn(async move {
        let responses = if failed {
          vec![(
            "400 Bad Request",
            r#"{"__type":"ResourceNotFoundException","message":"missing"}"#,
          )]
        } else {
          vec![
            (
              "500 Internal Server Error",
              r#"{"__type":"InternalServerError","message":"retry"}"#,
            ),
            (
              "200 OK",
              r#"{"Items":[],"Count":0,"ScannedCount":0,"LastEvaluatedKey":{"pk":{"S":"topic#0"},"sk":{"S":"1"}}}"#,
            ),
            ("200 OK", r#"{"Items":[],"Count":0,"ScannedCount":0}"#),
          ]
        };
        for (index, (status, body)) in responses.into_iter().enumerate() {
          let (mut socket, _) = listener.accept().await.unwrap();
          let mut request = Vec::new();
          while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut buffer = [0; 8192];
            let read = socket.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[.. read]);
          }
          let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/x-amz-json-1.0\r\nx-amzn-requestid: \
             request-{index}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
          );
          socket.write_all(response.as_bytes()).await.unwrap();
        }
      });
      let client = Client::from_conf(
        aws_sdk_dynamodb::Config::builder()
          .region(Region::new("us-east-1"))
          .credentials_provider(Credentials::new("test", "test", None, None, "test"))
          .endpoint_url(format!("http://{address}"))
          .behavior_version_latest()
          .build(),
      );
      let loader = FakeLoader::new(Arc::new(
        DefaultFeatureFlags::default().with_bool_flag(AWS_DYNAMODB_TRACE_SAMPLE_RATE, sampled),
      ));
      let store =
        DynamoMetadataStore::new_read_only(client, "metadata", Some(loader.snapshot_watch()));
      let context = TestTraceContext::new("dynamo-test");
      let dispatch = context.dispatch();
      let _guard = tracing::dispatcher::set_default(&dispatch);
      let result = store
        .scan_window_from_snowflake(
          &TopicWindowKey {
            topic: "topic".to_string(),
            window_start_unix_seconds: 0,
          },
          None,
          MetadataReadConsistency::Strong,
        )
        .await;
      server.await.unwrap();
      assert_eq!(result.is_err(), failed);
      let spans = context.exported_spans();
      let requests = spans
        .iter()
        .filter(|span| span.name == "aws.dynamodb.request")
        .collect::<Vec<_>>();
      assert_eq!(
        requests.len(),
        if failed {
          1
        } else if sampled {
          3
        } else {
          0
        }
      );
      for span in &spans {
        assert_eq!(span.dropped_attributes_count, 0);
      }
      for index in 0 .. requests.len() {
        assert!(requests.iter().any(|span| {
          span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == "aws.request_id"
              && attribute.value.as_str() == format!("request-{index}")
          })
        }));
      }
      if !failed && !sampled {
        assert_eq!(spans.len(), 0);
      }
    }
  }
}

#[tokio::test]
async fn aws_tracing_dynamo_sampling_changes_without_rebuilding_store() {
  let client = dynamo_client().await.unwrap();
  let table_name = format!("trace_{}", Uuid::new_v4().simple());
  create_segments_table(&client, &table_name).await.unwrap();
  let loader = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let store = DynamoMetadataStore::new_read_only(client, table_name, Some(loader.snapshot_watch()));
  let context = TestTraceContext::new("live-dynamo-test");
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  let window = TopicWindowKey {
    topic: "topic".to_string(),
    window_start_unix_seconds: 0,
  };
  for sampled in [false, true, false] {
    loader.update(Arc::new(
      DefaultFeatureFlags::default().with_bool_flag(AWS_DYNAMODB_TRACE_SAMPLE_RATE, sampled),
    ));
    let previous = context.exported_spans().len();
    store
      .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Strong)
      .await
      .unwrap();
    assert_eq!(context.exported_spans().len() > previous, sampled);
  }
}

#[tokio::test]
async fn metadata_query_disables_sdk_retries_per_application_attempt() -> Result<()> {
  let listener = TcpListener::bind("127.0.0.1:0").await?;
  let address = listener.local_addr()?;
  let requests = Arc::new(AtomicUsize::new(0));
  let counter = Arc::clone(&requests);
  let server = tokio::spawn(async move {
    loop {
      let (mut socket, _) = listener.accept().await.unwrap();
      let mut request = [0; 8192];
      assert!(socket.read(&mut request).await.unwrap() > 0);
      counter.fetch_add(1, Ordering::Relaxed);
      let body = r#"{"__type":"com.amazonaws.dynamodb.v20120810#ThrottlingException","message":"throttled","ThrottlingReasons":[{"reason":"TableReadKeyRangeThroughputExceeded","resource":"arn:aws:dynamodb:us-east-1:123456789012:table/metadata"}]}"#;
      let response = format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: \
         application/x-amz-json-1.0\r\nx-amzn-ErrorType: ThrottlingException\r\ncontent-length: \
         {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
      );
      socket.write_all(response.as_bytes()).await.unwrap();
    }
  });
  let config = aws_sdk_dynamodb::Config::builder()
    .region(Region::new("us-east-1"))
    .credentials_provider(Credentials::new("test", "test", None, None, "test"))
    .endpoint_url(format!("http://{address}"))
    .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(4))
    .behavior_version_latest()
    .build();
  let client = Client::from_conf(config);
  let window = TopicWindowKey {
    topic: "topic".to_string(),
    window_start_unix_seconds: 0,
  };
  let query = client
    .query()
    .table_name("metadata")
    .key_condition_expression("pk = :pk")
    .expression_attribute_values(":pk", AttributeValue::S(window.format()));
  let error = tokio::time::timeout(
    Duration::from_secs(5),
    query_page_with_retries(
      query,
      &window,
      QueryRetryPolicy {
        initial_delay: Duration::from_millis(2),
        max_delay: Duration::from_millis(2),
        page_timeout: Duration::from_millis(350),
      },
    ),
  )
  .await?
  .unwrap_err();
  server.abort();
  assert!(error.is::<crate::MetadataQueryThrottled>());
  assert!(requests.load(Ordering::Relaxed) > 2);
  Ok(())
}

#[tokio::test]
async fn metadata_query_retries_server_errors_but_not_nonretryable_errors() -> Result<()> {
  for (retryable, expected_requests) in [(true, 2), (false, 1)] {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests);
    let server = tokio::spawn(async move {
      for _ in 0 .. expected_requests {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 8192];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        let attempt = counter.fetch_add(1, Ordering::Relaxed);
        let (status, body) = if retryable && attempt > 0 {
          ("200 OK", r#"{"Items":[],"Count":0,"ScannedCount":0}"#)
        } else if retryable {
          (
            "500 Internal Server Error",
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"retry"}"#,
          )
        } else {
          (
            "400 Bad Request",
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"missing"}"#,
          )
        };
        let response = format!(
          "HTTP/1.1 {status}\r\ncontent-type: application/x-amz-json-1.0\r\ncontent-length: \
           {}\r\nconnection: close\r\n\r\n{body}",
          body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
      }
    });
    let config = aws_sdk_dynamodb::Config::builder()
      .region(Region::new("us-east-1"))
      .credentials_provider(Credentials::new("test", "test", None, None, "test"))
      .endpoint_url(format!("http://{address}"))
      .behavior_version_latest()
      .build();
    let client = Client::from_conf(config);
    let window = TopicWindowKey {
      topic: "topic".to_string(),
      window_start_unix_seconds: 0,
    };
    let query = client
      .query()
      .table_name("metadata")
      .key_condition_expression("pk = :pk")
      .expression_attribute_values(":pk", AttributeValue::S(window.format()));
    let result = tokio::time::timeout(
      Duration::from_secs(3),
      query_page_with_retries(query, &window, QueryRetryPolicy::default()),
    )
    .await?;
    assert_eq!(result.is_ok(), retryable);
    tokio::time::timeout(Duration::from_secs(1), server).await??;
    assert_eq!(requests.load(Ordering::Relaxed), expected_requests);
  }
  Ok(())
}

#[tokio::test]
async fn metadata_query_uses_page_deadline_instead_of_sdk_timeout() -> Result<()> {
  let listener = TcpListener::bind("127.0.0.1:0").await?;
  let address = listener.local_addr()?;
  let server = tokio::spawn(async move {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = [0; 8192];
    assert!(socket.read(&mut request).await.unwrap() > 0);
    sleep(Duration::from_millis(80)).await;
    let body = r#"{"Items":[],"Count":0,"ScannedCount":0}"#;
    let response = format!(
      "HTTP/1.1 200 OK\r\ncontent-type: application/x-amz-json-1.0\r\ncontent-length: \
       {}\r\nconnection: close\r\n\r\n{body}",
      body.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
  });
  let config = aws_sdk_dynamodb::Config::builder()
    .region(Region::new("us-east-1"))
    .credentials_provider(Credentials::new("test", "test", None, None, "test"))
    .endpoint_url(format!("http://{address}"))
    .timeout_config(
      aws_config::timeout::TimeoutConfig::builder()
        .operation_timeout(Duration::from_millis(20))
        .operation_attempt_timeout(Duration::from_millis(10))
        .build(),
    )
    .behavior_version_latest()
    .build();
  let window = TopicWindowKey {
    topic: "topic".to_string(),
    window_start_unix_seconds: 0,
  };
  let query = Client::from_conf(config)
    .query()
    .table_name("metadata")
    .key_condition_expression("pk = :pk")
    .expression_attribute_values(":pk", AttributeValue::S(window.format()));
  tokio::time::timeout(
    Duration::from_secs(3),
    query_page_with_retries(
      query,
      &window,
      QueryRetryPolicy {
        page_timeout: Duration::from_millis(300),
        ..Default::default()
      },
    ),
  )
  .await??;
  server.await?;
  Ok(())
}

#[tokio::test]
async fn metadata_query_page_deadline_cancels_inflight_sdk_attempt() -> Result<()> {
  let listener = TcpListener::bind("127.0.0.1:0").await?;
  let address = listener.local_addr()?;
  let server = tokio::spawn(async move {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = [0; 8192];
    assert!(socket.read(&mut request).await.unwrap() > 0);
    let mut drain = [0; 1];
    assert_eq!(socket.read(&mut drain).await.unwrap(), 0);
  });
  let config = aws_sdk_dynamodb::Config::builder()
    .region(Region::new("us-east-1"))
    .credentials_provider(Credentials::new("test", "test", None, None, "test"))
    .endpoint_url(format!("http://{address}"))
    .behavior_version_latest()
    .build();
  let client = Client::from_conf(config);
  let window = TopicWindowKey {
    topic: "topic".to_string(),
    window_start_unix_seconds: 0,
  };
  let query = client
    .query()
    .table_name("metadata")
    .key_condition_expression("pk = :pk")
    .expression_attribute_values(":pk", AttributeValue::S(window.format()));
  let result = tokio::time::timeout(
    Duration::from_secs(3),
    query_page_with_retries(
      query,
      &window,
      QueryRetryPolicy {
        page_timeout: Duration::from_millis(80),
        ..Default::default()
      },
    ),
  )
  .await?;
  assert!(result.unwrap_err().to_string().contains("timed out"));
  tokio::time::timeout(Duration::from_secs(1), server).await??;
  Ok(())
}

#[tokio::test]
async fn query_retry_flags_keep_last_valid_policy() -> Result<()> {
  let loader = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(QUERY_PAGE_TIMEOUT_FLAG, 1_000),
  ));
  let store = DynamoMetadataStore::new_read_only(
    dynamo_client().await?,
    "metadata",
    Some(loader.snapshot_watch()),
  );
  let first = store.query_retry_policy();
  assert_eq!(first.page_timeout, Duration::from_secs(1));

  loader.update(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(QUERY_PAGE_TIMEOUT_FLAG, 0),
  ));
  assert_eq!(store.query_retry_policy(), first);

  loader.update(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(QUERY_PAGE_TIMEOUT_FLAG, 2_000),
  ));
  assert_eq!(
    store.query_retry_policy().page_timeout,
    Duration::from_secs(2)
  );
  Ok(())
}

#[test]
fn classifies_query_throttle_variants_and_reasons() {
  let throttling = QueryError::ThrottlingException(
    ThrottlingException::builder()
      .throttling_reasons(
        aws_sdk_dynamodb::types::ThrottlingReason::builder()
          .reason("TableReadKeyRangeThroughputExceeded")
          .build(),
      )
      .build(),
  );
  assert!(query_service_is_throttled(&throttling));
  let QueryError::ThrottlingException(throttling) = throttling else {
    unreachable!();
  };
  assert_eq!(
    throttling.throttling_reasons()[0].reason(),
    Some("TableReadKeyRangeThroughputExceeded")
  );
  assert!(query_service_is_throttled(
    &QueryError::ProvisionedThroughputExceededException(
      ProvisionedThroughputExceededException::builder().build()
    )
  ));
  assert!(query_service_is_throttled(
    &QueryError::RequestLimitExceeded(RequestLimitExceeded::builder().build())
  ));
  assert!(!query_service_is_throttled(
    &QueryError::ResourceNotFoundException(
      aws_sdk_dynamodb::types::error::ResourceNotFoundException::builder().build()
    )
  ));
}

async fn create_segments_table(client: &Client, table_name: &str) -> Result<()> {
  client
    .create_table()
    .table_name(table_name)
    .attribute_definitions(
      AttributeDefinition::builder()
        .attribute_name("pk")
        .attribute_type(ScalarAttributeType::S)
        .build()?,
    )
    .attribute_definitions(
      AttributeDefinition::builder()
        .attribute_name("sk")
        .attribute_type(ScalarAttributeType::S)
        .build()?,
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("pk")
        .key_type(KeyType::Hash)
        .build()?,
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("sk")
        .key_type(KeyType::Range)
        .build()?,
    )
    .billing_mode(BillingMode::PayPerRequest)
    .send()
    .await
    .with_context(|| format!("create table {table_name}"))?;

  wait_for_table_active(client, table_name).await
}

async fn wait_for_table_active(client: &Client, table_name: &str) -> Result<()> {
  for _ in 0 .. 20 {
    let response = client.describe_table().table_name(table_name).send().await;
    if let Ok(response) = response
      && let Some(status) = response.table().and_then(|table| table.table_status())
      && status.as_str() == "ACTIVE"
    {
      return Ok(());
    }

    sleep(Duration::from_millis(100)).await;
  }

  Err(anyhow!("table {table_name} did not become active"))
}

fn build_segment(
  topic: &str,
  window_start_unix_seconds: i64,
  snowflake_id: u64,
) -> SegmentMetadata {
  let mut segment_index = HashMap::new();
  let batch = BatchMetadata {
    seq_range: SeqRange { start: 10, end: 19 },
    byte_range: blob_stream_types::ByteRange { start: 0, end: 512 },
    payload_bytes: 512,
  };
  segment_index.insert(0 as VirtualPartitionId, batch);

  SegmentMetadata::new(
    TopicWindowKey {
      topic: topic.to_string(),
      window_start_unix_seconds,
    },
    SnowflakeId(snowflake_id),
    BlobKey::from("topic/1/segment"),
    Compression::none(),
    segment_index,
    OffsetDateTime::UNIX_EPOCH + TimeDuration::seconds(3),
    OffsetDateTime::UNIX_EPOCH + TimeDuration::seconds(3),
  )
}

#[test]
fn classifies_transaction_cancellation_codes() {
  let transaction_conflict = TransactWriteItemsError::TransactionCanceledException(
    TransactionCanceledException::builder()
      .cancellation_reasons(
        CancellationReason::builder()
          .code("TransactionConflict")
          .build(),
      )
      .build(),
  );
  assert!(transaction_cancellation_has_code(
    &transaction_conflict,
    "TransactionConflict"
  ));
  assert!(!transaction_cancellation_has_code(
    &transaction_conflict,
    "ConditionalCheckFailed"
  ));

  let conditional_check_failure = TransactWriteItemsError::TransactionCanceledException(
    TransactionCanceledException::builder()
      .cancellation_reasons(
        CancellationReason::builder()
          .code("ConditionalCheckFailed")
          .build(),
      )
      .build(),
  );
  assert!(transaction_cancellation_has_code(
    &conditional_check_failure,
    "ConditionalCheckFailed"
  ));
  assert!(!transaction_cancellation_has_code(
    &conditional_check_failure,
    "TransactionConflict"
  ));
}

#[tokio::test]
async fn writes_and_scans_window() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("blob_segments_test_{}", Uuid::new_v4());
  create_segments_table(&client, &table_name).await?;

  let store = DynamoMetadataStore::new(
    client.clone(),
    table_name.clone(),
    "unused_producer_leases_table",
    HashMap::new(),
    TimeDuration::hours(1),
    None,
    None,
  );
  let first = build_segment("topic-a", 100, 1);
  let second = build_segment("topic-a", 100, 2);
  let other = build_segment("topic-b", 200, 3);

  store.write_segment(first.clone(), None, 0).await?;
  store.write_segment(second.clone(), None, 0).await?;
  store.write_segment(other, None, 0).await?;

  let window = TopicWindowKey {
    topic: "topic-a".to_string(),
    window_start_unix_seconds: 100,
  };
  let segments = store
    .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Eventual)
    .await?;

  assert_eq!(segments.len(), 2);
  assert!(segments.contains(&first));
  assert!(segments.contains(&second));

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn scans_window_from_inclusive_snowflake() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("blob_segments_test_{}", Uuid::new_v4());
  create_segments_table(&client, &table_name).await?;

  let store = DynamoMetadataStore::new(
    client.clone(),
    table_name.clone(),
    "unused_producer_leases_table",
    HashMap::new(),
    TimeDuration::hours(1),
    None,
    None,
  );
  let first = build_segment("topic-a", 100, 1);
  let second = build_segment("topic-a", 100, 2);
  store.write_segment(first, None, 0).await?;
  store.write_segment(second.clone(), None, 0).await?;

  let window = TopicWindowKey {
    topic: "topic-a".to_string(),
    window_start_unix_seconds: 100,
  };
  let segments = store
    .scan_window_from_snowflake(
      &window,
      Some(SnowflakeId(2)),
      MetadataReadConsistency::Eventual,
    )
    .await?;

  assert_eq!(segments, vec![second]);

  client.delete_table().table_name(&table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn writes_segment_ttl_attribute() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("blob_segments_test_{}", Uuid::new_v4());
  create_segments_table(&client, &table_name).await?;

  let mut topic_retention = HashMap::new();
  topic_retention.insert("topic-a".into(), TimeDuration::days(7));
  let store = DynamoMetadataStore::new(
    client.clone(),
    table_name.clone(),
    "unused_producer_leases_table",
    topic_retention,
    TimeDuration::hours(1),
    None,
    None,
  );
  let segment = build_segment("topic-a", 100, 1);

  store.write_segment(segment.clone(), None, 0).await?;

  let item = client
    .get_item()
    .table_name(&table_name)
    .key("pk", AttributeValue::S(segment.partition_key()))
    .key("sk", AttributeValue::S(segment.snowflake_key()))
    .send()
    .await?
    .item
    .ok_or_else(|| anyhow!("expected item"))?;

  let ttl = item
    .get(TTL_ATTRIBUTE_NAME)
    .and_then(|value| value.as_n().ok())
    .ok_or_else(|| anyhow!("missing ttl attribute"))?
    .parse::<i64>()?;
  let expected = segment.created_at.unix_timestamp() + (7 * 24 * 60 * 60) + 3_600;

  assert_eq!(ttl, expected);
  assert!(
    item
      .get(super::ATTR_SEGMENT_METADATA_V1)
      .is_some_and(|value| value.as_b().is_ok()),
    "metadata item must contain binary compact metadata"
  );
  for attribute in [
    "topic",
    "window_start_ts",
    "blob_key",
    "created_ts_ms",
    "metadata_published_ts_ms",
    "compression",
    "record_count",
    "min_event_ts_ms",
    "max_event_ts_ms",
    "checksum",
    "segment_index",
  ] {
    assert!(
      !item.contains_key(attribute),
      "metadata item unexpectedly contains {attribute}"
    );
  }

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn skips_noncompliant_segment_rows() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("blob_segments_test_{}", Uuid::new_v4());
  create_segments_table(&client, &table_name).await?;

  let store = DynamoMetadataStore::new(
    client.clone(),
    table_name.clone(),
    "unused_producer_leases_table",
    HashMap::new(),
    TimeDuration::hours(1),
    None,
    None,
  );
  let valid = build_segment("topic-a", 100, 2);
  store.write_segment(valid.clone(), None, 0).await?;
  let encoded = crate::codec::encode(&valid)?;
  let mut invalid_metadata = SegmentMetadataV1::parse_from_tokio_bytes(&encoded.payload)?;
  invalid_metadata.partitions[0]
    .batch
    .as_mut()
    .unwrap()
    .byte_end = 0;
  client
    .put_item()
    .table_name(&table_name)
    .item("pk", AttributeValue::S(valid.partition_key()))
    .item("sk", AttributeValue::S(SnowflakeId(1).format_lex()))
    .item(
      super::ATTR_SEGMENT_METADATA_V1,
      AttributeValue::S("not a binary payload".to_string()),
    )
    .send()
    .await?;
  client
    .put_item()
    .table_name(&table_name)
    .item("pk", AttributeValue::S(valid.partition_key()))
    .item("sk", AttributeValue::S(SnowflakeId(3).format_lex()))
    .item(
      super::ATTR_SEGMENT_METADATA_V1,
      AttributeValue::B(Blob::new(invalid_metadata.write_to_bytes()?)),
    )
    .send()
    .await?;

  let window = TopicWindowKey {
    topic: "topic-a".to_string(),
    window_start_unix_seconds: 100,
  };
  assert_eq!(
    store
      .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Eventual)
      .await?,
    vec![valid]
  );

  client.delete_table().table_name(&table_name).send().await?;
  Ok(())
}
