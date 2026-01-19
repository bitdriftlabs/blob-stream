// blob-stream - in-memory blob store
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./memory_test.rs"]
mod tests;

use std::collections::HashMap;

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::{BlobKey, BlobStore, ByteRange};

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
    let mut guard = self.blobs.write().await;
    guard.insert(key.clone(), payload);
    Ok(())
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> Result<Bytes> {
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

    let start = range.start as usize;
    let end = range.end as usize;
    Ok(blob.slice(start..end))
  }
}
