// blob-stream - in-memory blob store
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./memory_test.rs"]
mod tests;

use crate::{BlobKey, BlobStore, ByteRange};
use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use log::trace;
use std::collections::HashMap;
use tokio::sync::RwLock;

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
    let mut guard = self.blobs.write().await;
    guard.insert(key.clone(), payload);
    Ok(())
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> Result<Bytes> {
    trace!(
      "in-memory blob get_range: key={}, start={}, end={}",
      key.as_str(),
      range.start,
      range.end
    );
    if range.is_empty() {
      return Ok(Bytes::new());
    }

    let guard = self.blobs.read().await;
    let blob = guard
      .get(key)
      .ok_or_else(|| anyhow::anyhow!("blob not found: {}", key.as_str()))?;
    let len = blob.len() as u64;

    if range.end > len {
      bail!(
        "byte range end {} exceeds blob length {} for key {}",
        range.end,
        len,
        key.as_str()
      );
    }

    let start = usize::try_from(range.start)
      .map_err(|_| anyhow!("byte range start {} exceeds usize", range.start))?;
    let end = usize::try_from(range.end)
      .map_err(|_| anyhow!("byte range end {} exceeds usize", range.end))?;
    Ok(blob.slice(start .. end))
  }
}
