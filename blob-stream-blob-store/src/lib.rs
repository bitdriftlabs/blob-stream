//! Blob storage abstraction and built-in backends.

#[cfg(test)]
#[path = "./blob_store_test.rs"]
mod tests;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod memory;
mod s3;

pub use memory::InMemoryBlobStore;
pub use s3::S3BlobStore;

//
// BlobKey
//

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Logical blob object key.
pub struct BlobKey(String);

impl BlobKey {
  /// Build a new blob key.
  #[must_use]
  pub fn new(key: impl Into<String>) -> Self {
    Self(key.into())
  }

  #[must_use]
  /// Borrow key as `&str`.
  pub fn as_str(&self) -> &str {
    &self.0
  }
}

impl From<String> for BlobKey {
  fn from(value: String) -> Self {
    Self(value)
  }
}

impl From<&str> for BlobKey {
  fn from(value: &str) -> Self {
    Self(value.to_string())
  }
}

//
// ByteRange
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Half-open byte range `[start, end)`.
pub struct ByteRange {
  /// Inclusive start offset.
  pub start: u64,
  /// Exclusive end offset.
  pub end: u64,
}

impl ByteRange {
  #[must_use]
  /// Byte length of the range.
  pub fn len(&self) -> u64 {
    self.end.saturating_sub(self.start)
  }

  #[must_use]
  /// Whether `start >= end`.
  pub fn is_empty(&self) -> bool {
    self.end <= self.start
  }
}

//
// BlobStoreError
//

#[derive(Debug, Error)]
/// Error returned when reading a blob range.
pub enum BlobStoreError {
  #[error("blob not found: {key}")]
  /// The requested blob key does not exist.
  NotFound { key: String },
  #[error("invalid byte range for blob {key}: {message}")]
  /// The requested byte range cannot be read from the blob.
  InvalidRange { key: String, message: String },
  #[error("blob cache admission rejected: {key}")]
  /// The caller rejected the content length before the complete object was read.
  AdmissionRejected { key: String },
  #[error("read blob {key}: {source}")]
  /// A storage or body-stream error not otherwise classified.
  Read {
    key: String,
    #[source]
    source: anyhow::Error,
  },
}

/// Result returned by blob range reads.
pub type BlobStoreResult<T> = std::result::Result<T, BlobStoreError>;

/// Decides whether a complete object of the reported byte length may be retained.
pub type BlobCacheAdmission = dyn Fn(u64) -> bool + Send + Sync;

//
// BlobStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
/// Blob store interface used by broker and consumer paths.
pub trait BlobStore: Send + Sync {
  /// Write the full blob payload for a key.
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()>;

  /// Read an exact byte range from a blob.
  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes>;

  /// Read a complete blob after admitting its content length for cache retention.
  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes>;
}
