use crate::{BlobCacheAdmission, BlobKey, BlobStore, BlobStoreError, BlobStoreResult, ByteRange};
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use log::{debug, trace};
use tokio::io::{AsyncRead, AsyncReadExt};

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
}

impl S3BlobStore {
  #[must_use]
  pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> Self {
    Self {
      bucket: bucket.into(),
      client,
    }
  }
}

async fn read_content_length_body(
  body: impl AsyncRead + Unpin,
  content_length: u64,
) -> std::io::Result<Vec<u8>> {
  let mut body = body.take(content_length);
  let mut bytes = Vec::new();
  body.read_to_end(&mut bytes).await?;
  if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != content_length {
    return Err(std::io::Error::new(
      std::io::ErrorKind::UnexpectedEof,
      "S3 body did not match its advertised content length",
    ));
  }
  Ok(bytes)
}

#[async_trait]
impl BlobStore for S3BlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
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
      .with_context(|| format!("put S3 object {}", key.as_str()))?;

    debug!(
      "s3 put complete: bucket={}, key={}",
      self.bucket,
      key.as_str()
    );

    Ok(())
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
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

    let response = match self
      .client
      .get_object()
      .bucket(&self.bucket)
      .key(key.as_str())
      .range(range_header)
      .send()
      .await
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

  async fn get_with_cache_admission(
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
