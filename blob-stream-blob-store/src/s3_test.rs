use super::{S3BlobStore, read_content_length_body};
use crate::{BlobKey, BlobStore, BlobStoreError, ByteRange};
use aws_sdk_s3::config::{Credentials, Region};
use bd_log::test::TestTraceContext;
use bd_runtime_config::loader::Loader;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_runtime_config::AWS_S3_TRACE_SAMPLE_RATE;
use bytes::Bytes;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::net::TcpListener;

#[tokio::test]
async fn aws_tracing_s3_preserves_support_ids_and_final_outcomes() {
  for sampled in [false, true] {
    for scenario in [
      "put",
      "range",
      "admission",
      "missing",
      "rejected",
      "failure",
      "short",
    ] {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let address = listener.local_addr().unwrap();
      let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
          let mut buffer = [0; 8192];
          let read = socket.read(&mut buffer).await.unwrap();
          assert_ne!(read, 0);
          request.extend_from_slice(&buffer[.. read]);
        }
        let (status, body) = match scenario {
          "missing" => (
            "404 Not Found",
            "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>",
          ),
          "failure" => (
            "403 Forbidden",
            "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>",
          ),
          "put" => ("200 OK", ""),
          _ => ("200 OK", "data"),
        };
        let length = if scenario == "short" { 10 } else { body.len() };
        let response = format!(
          "HTTP/1.1 {status}\r\nx-amz-request-id: request-123\r\nx-amz-id-2: \
           extended-456\r\ncontent-length: {length}\r\nconnection: close\r\n\r\n{body}"
        );
        socket.write_all(response.as_bytes()).await.unwrap();
      });
      let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
          .region(Region::new("us-east-1"))
          .credentials_provider(Credentials::new("test", "test", None, None, "test"))
          .endpoint_url(format!("http://{address}"))
          .force_path_style(true)
          .retry_config(aws_config::retry::RetryConfig::disabled())
          .behavior_version_latest()
          .build(),
      );
      let flags = FakeLoader::new(Arc::new(
        DefaultFeatureFlags::default().with_bool_flag(AWS_S3_TRACE_SAMPLE_RATE, sampled),
      ));
      let store = S3BlobStore::new(client, "bucket", Some(flags.snapshot_watch()));
      let context = TestTraceContext::new("s3-test");
      let dispatch = context.dispatch();
      let _guard = tracing::dispatcher::set_default(&dispatch);
      let key = BlobKey::from("key");
      let result = match scenario {
        "put" => store
          .put(&key, Bytes::from_static(b"data"))
          .await
          .map(|()| Bytes::new())
          .map_err(|source| BlobStoreError::Read {
            key: "key".to_string(),
            source,
          }),
        "admission" | "rejected" => {
          store
            .get_with_cache_admission(&key, &move |_| scenario != "rejected")
            .await
        },
        _ => store.get_range(&key, ByteRange { start: 0, end: 4 }).await,
      };
      server.await.unwrap();
      let unexpected = matches!(scenario, "failure" | "short");
      let expected = matches!(scenario, "missing" | "rejected");
      assert_eq!(result.is_err(), unexpected || expected, "{scenario}");
      let spans = context.exported_spans();
      assert_eq!(
        spans.len(),
        if sampled || unexpected { 2 } else { 0 },
        "{scenario}"
      );
      for span in &spans {
        assert_eq!(span.dropped_attributes_count, 0);
      }
      if let Some(request) = spans.iter().find(|span| span.name == "aws.s3.request") {
        for (name, value) in [
          ("aws.request_id", "request-123"),
          ("aws.s3.extended_request_id", "extended-456"),
        ] {
          assert!(
            request
              .attributes
              .iter()
              .any(|attribute| attribute.key.as_str() == name && attribute.value.as_str() == value),
            "{scenario}: {name}"
          );
        }
        let root = spans
          .iter()
          .find(|span| span.name == "blob_stream.aws")
          .unwrap();
        assert!(
          root
            .attributes
            .iter()
            .any(|attribute| attribute.key.as_str() == "aws.outcome"
              && attribute.value.as_str()
                == if unexpected {
                  "failure"
                } else if expected {
                  "expected"
                } else {
                  "success"
                })
        );
      }
    }
  }
}

#[tokio::test]
async fn content_length_read_accepts_the_advertised_length() {
  let (mut writer, reader) = duplex(16);
  writer.write_all(b"abcd").await.unwrap();
  drop(writer);

  let bytes = read_content_length_body(reader, 4).await.unwrap();

  assert_eq!(bytes, b"abcd");
}

#[tokio::test]
async fn content_length_read_rejects_a_short_body() {
  let (mut writer, reader) = duplex(16);
  writer.write_all(b"abc").await.unwrap();
  drop(writer);

  assert!(read_content_length_body(reader, 4).await.is_err());
}

#[tokio::test]
async fn content_length_read_reports_an_unallocatable_buffer() {
  let (_writer, reader) = duplex(16);

  assert!(read_content_length_body(reader, u64::MAX).await.is_err());
}
