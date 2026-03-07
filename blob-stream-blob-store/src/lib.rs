#[cfg(test)]
#[path = "./blob_store_test.rs"]
mod tests;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

mod memory;
mod s3;

pub use memory::InMemoryBlobStore;
pub use s3::S3BlobStore;

//
// BlobKey
//

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlobKey(String);

impl BlobKey {
  #[must_use]
  pub fn new(key: impl Into<String>) -> Self {
    Self(key.into())
  }

  #[must_use]
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
pub struct ByteRange {
  pub start: u64,
  pub end: u64,
}

impl ByteRange {
  #[must_use]
  pub fn len(&self) -> u64 {
    self.end.saturating_sub(self.start)
  }

  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.end <= self.start
  }
}

//
// BlobStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait BlobStore: Send + Sync {
  /// Write the full blob payload for a key.
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()>;

  /// Read an exact byte range from a blob.
  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> Result<Bytes>;
}
