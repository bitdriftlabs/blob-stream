#[cfg(test)]
#[path = "./memory_test.rs"]
mod tests;

use crate::{BlobKey, BlobStore, BlobStoreError, BlobStoreResult, ByteRange};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use log::trace;
use parking_lot::RwLock;
use std::collections::HashMap;

//
// InMemoryBlobStore
//

#[derive(Debug, Default)]
pub struct InMemoryBlobStore {
  blobs: RwLock<HashMap<BlobKey, Bytes>>,
}

impl InMemoryBlobStore {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    trace!(
      "in-memory blob put: key={}, bytes={}",
      key.as_str(),
      payload.len()
    );
    let mut guard = self.blobs.write();
    guard.insert(key.clone(), payload);
    Ok(())
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    trace!(
      "in-memory blob get_range: key={}, start={}, end={}",
      key.as_str(),
      range.start,
      range.end
    );
    if range.is_empty() {
      return Ok(Bytes::new());
    }

    let guard = self.blobs.read();
    let blob = guard.get(key).ok_or_else(|| BlobStoreError::NotFound {
      key: key.as_str().to_string(),
    })?;
    let len = blob.len() as u64;

    if range.end > len {
      return Err(BlobStoreError::InvalidRange {
        key: key.as_str().to_string(),
        message: format!("range end {} exceeds blob length {len}", range.end),
      });
    }

    let start = usize::try_from(range.start).map_err(|_| BlobStoreError::InvalidRange {
      key: key.as_str().to_string(),
      message: format!("range start {} does not fit in memory", range.start),
    })?;
    let end = usize::try_from(range.end).map_err(|_| BlobStoreError::InvalidRange {
      key: key.as_str().to_string(),
      message: format!("range end {} does not fit in memory", range.end),
    })?;
    Ok(blob.slice(start .. end))
  }
}
