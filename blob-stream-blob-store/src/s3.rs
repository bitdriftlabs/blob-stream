use crate::{BlobCacheAdmission, BlobKey, BlobStore, BlobStoreError, BlobStoreResult, ByteRange};
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::{RequestId, RequestIdExt};
use aws_sdk_s3::primitives::ByteStream;
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use blob_stream_runtime_config::aws_tracing::{bounded_diagnostic, instrument_operation};
use blob_stream_runtime_config::{AWS_S3_TRACE_SAMPLE_RATE, AwsTraceSampler};
use bytes::Bytes;
use log::{debug, trace};
use serde_json::json;
use std::fmt::Display;
use std::future::Future;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{Instrument as _, Span};

#[cfg(test)]
#[path = "./s3_test.rs"]
mod tests;

//
// S3BlobStore
//

#[derive(Debug, Clone)]
pub struct S3BlobStore {
  bucket: String,
  client: aws_sdk_s3::Client,
  trace_sampler: AwsTraceSampler,
}

impl S3BlobStore {
  #[must_use]
  pub fn new(
    client: aws_sdk_s3::Client,
    bucket: impl Into<String>,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Self {
    Self {
      bucket: bucket.into(),
      client,
      trace_sampler: AwsTraceSampler::new(feature_flags),
    }
  }

  async fn trace_operation<F, T, E, Expected>(
    &self,
    method: &'static str,
    key: &BlobKey,
    future: F,
    expected: Expected,
  ) -> std::result::Result<T, E>
  where
    F: Future<Output = std::result::Result<T, E>>,
    E: Display,
    Expected: Fn(&E) -> bool,
  {
    instrument_operation(
      &self.trace_sampler,
      AWS_S3_TRACE_SAMPLE_RATE,
      "S3",
      method,
      self
        .client
        .config()
        .region()
        .map_or("", |region| region.as_ref()),
      &self.bucket,
      async {
        let request = bd_log::otel_info_span_if_parent!(
          "aws.s3.request",
          rpc.method = method,
          aws.s3.bucket = self.bucket.as_str(),
          aws.s3.key = bounded_diagnostic(key.as_str()),
          aws.s3.range = tracing::field::Empty,
          aws.request_id = tracing::field::Empty,
          aws.s3.extended_request_id = tracing::field::Empty,
          aws.error_category = tracing::field::Empty,
          aws.error_code = tracing::field::Empty,
          http.response.status_code = tracing::field::Empty,
          aws.details_json = tracing::field::Empty,
        );
        future.instrument(request).await
      },
      expected,
    )
    .await
  }
}

fn record_s3_response(request_id: Option<&str>, extended_request_id: Option<&str>) {
  let span = Span::current();
  if let Some(id) = request_id {
    span.record("aws.request_id", bounded_diagnostic(id));
  }
  if let Some(id) = extended_request_id {
    span.record("aws.s3.extended_request_id", bounded_diagnostic(id));
  }
}

fn record_s3_error<E: ProvideErrorMetadata>(error: &SdkError<E>) {
  let span = Span::current();
  let category = match error {
    SdkError::ConstructionFailure(_) => "construction",
    SdkError::TimeoutError(_) => "timeout",
    SdkError::DispatchFailure(_) => "dispatch",
    SdkError::ResponseError(_) => "response_parse",
    SdkError::ServiceError(_) => "service",
    _ => "unknown",
  };
  span.record("aws.error_category", category);
  if let Some(code) = error
    .as_service_error()
    .and_then(ProvideErrorMetadata::code)
  {
    span.record("aws.error_code", bounded_diagnostic(code));
  }
  if let Some(response) = error.raw_response() {
    record_s3_response(
      response.headers().get("x-amz-request-id"),
      response.headers().get("x-amz-id-2"),
    );
    span.record("http.response.status_code", response.status().as_u16());
  }
}

async fn read_content_length_body(
  body: impl AsyncRead + Unpin,
  content_length: u64,
) -> std::io::Result<Vec<u8>> {
  let mut body = body.take(content_length);
  let capacity = usize::try_from(content_length).map_err(|_| {
    std::io::Error::other("S3 content length does not fit in this process's address space")
  })?;
  let mut bytes = Vec::new();
  bytes
    .try_reserve_exact(capacity)
    .map_err(|error| std::io::Error::other(format!("could not reserve S3 body buffer: {error}")))?;
  body.read_to_end(&mut bytes).await?;
  if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != content_length {
    return Err(std::io::Error::new(
      std::io::ErrorKind::UnexpectedEof,
      "S3 body did not match its advertised content length",
    ));
  }
  Ok(bytes)
}

impl S3BlobStore {
  async fn put_inner(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    Span::current().record(
      "aws.details_json",
      json!({"requested_bytes": payload.len()}).to_string(),
    );
    trace!(
      "s3 put start: bucket={}, key={}, bytes={}",
      self.bucket,
      key.as_str(),
      payload.len()
    );
    // TODO(multipart): Use S3 multipart uploads with parallel parts for large blobs.
    self
      .client
      .put_object()
      .bucket(&self.bucket)
      .key(key.as_str())
      .body(ByteStream::from(payload))
      .send()
      .await
      .inspect(|output| record_s3_response(output.request_id(), output.extended_request_id()))
      .inspect_err(record_s3_error)
      .with_context(|| format!("put S3 object {}", key.as_str()))?;

    debug!(
      "s3 put complete: bucket={}, key={}",
      self.bucket,
      key.as_str()
    );

    Ok(())
  }

  async fn get_range_inner(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    trace!(
      "s3 get_range start: bucket={}, key={}, start={}, end={}",
      self.bucket,
      key.as_str(),
      range.start,
      range.end
    );
    if range.is_empty() {
      return Ok(Bytes::new());
    }

    let end_inclusive = range.end - 1;
    let range_header = format!("bytes={}-{}", range.start, end_inclusive);
    Span::current().record("aws.s3.range", range_header.as_str());

    let response = match self
      .client
      .get_object()
      .bucket(&self.bucket)
      .key(key.as_str())
      .range(range_header)
      .send()
      .await
      .inspect_err(record_s3_error)
    {
      Ok(response) => response,
      Err(SdkError::ServiceError(error)) if matches!(error.err(), GetObjectError::NoSuchKey(_)) => {
        return Err(BlobStoreError::NotFound {
          key: key.as_str().to_string(),
        });
      },
      Err(source) => {
        return Err(BlobStoreError::Read {
          key: key.as_str().to_string(),
          source: source.into(),
        });
      },
    };

    record_s3_response(response.request_id(), response.extended_request_id());
    Span::current().record("aws.details_json", json!({"content_length": response.content_length, "etag": response.e_tag().map(bounded_diagnostic), "version_id": response.version_id().map(bounded_diagnostic)}).to_string());

    let body = response
      .body
      .collect()
      .await
      .map_err(|source| BlobStoreError::Read {
        key: key.as_str().to_string(),
        source: source.into(),
      })?;
    let bytes = body.into_bytes();
    debug!(
      "s3 get_range complete: bucket={}, key={}, bytes={}",
      self.bucket,
      key.as_str(),
      bytes.len()
    );

    Ok(bytes)
  }

  async fn get_with_cache_admission_inner(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let response = match self
      .client
      .get_object()
      .bucket(&self.bucket)
      .key(key.as_str())
      .send()
      .await
      .inspect_err(record_s3_error)
    {
      Ok(response) => response,
      Err(SdkError::ServiceError(error)) if matches!(error.err(), GetObjectError::NoSuchKey(_)) => {
        return Err(BlobStoreError::NotFound {
          key: key.as_str().to_string(),
        });
      },
      Err(source) => {
        return Err(BlobStoreError::Read {
          key: key.as_str().to_string(),
          source: source.into(),
        });
      },
    };
    record_s3_response(response.request_id(), response.extended_request_id());
    Span::current().record("aws.details_json", json!({"content_length": response.content_length, "etag": response.e_tag().map(bounded_diagnostic), "version_id": response.version_id().map(bounded_diagnostic)}).to_string());
    let content_length = response
      .content_length
      .ok_or_else(|| BlobStoreError::Read {
        key: key.as_str().to_string(),
        source: anyhow::anyhow!("S3 response has no content length"),
      })?;
    let content_length = u64::try_from(content_length).map_err(|_| BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("S3 response content length is negative"),
    })?;
    if !admission(content_length) {
      return Err(BlobStoreError::AdmissionRejected {
        key: key.as_str().to_string(),
      });
    }
    let bytes = read_content_length_body(response.body.into_async_read(), content_length)
      .await
      .map_err(|source| BlobStoreError::Read {
        key: key.as_str().to_string(),
        source: source.into(),
      })?;
    Ok(Bytes::from(bytes))
  }
}

fn expected_read_error(error: &BlobStoreError) -> bool {
  matches!(
    error,
    BlobStoreError::NotFound { .. } | BlobStoreError::AdmissionRejected { .. }
  )
}

#[async_trait]
impl BlobStore for S3BlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self
      .trace_operation(
        "PutObject",
        key,
        Box::pin(self.put_inner(key, payload)),
        |_| false,
      )
      .await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    if range.is_empty() {
      return Ok(Bytes::new());
    }
    self
      .trace_operation(
        "GetObject",
        key,
        Box::pin(self.get_range_inner(key, range)),
        expected_read_error,
      )
      .await
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    self
      .trace_operation(
        "GetObject",
        key,
        Box::pin(self.get_with_cache_admission_inner(key, admission)),
        expected_read_error,
      )
      .await
  }
}
