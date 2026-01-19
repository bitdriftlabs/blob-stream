// blob-stream - in-memory blob store tests
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bytes::Bytes;

use crate::{BlobKey, BlobStore, ByteRange, InMemoryBlobStore};

#[tokio::test]
async fn stores_and_reads_ranges() {
  let store = InMemoryBlobStore::new();
  let key = BlobKey::from("topic/1/abc");
  let payload = Bytes::from_static(b"abcdef");

  store.put(&key, payload).await.expect("put blob");

  let range = ByteRange { start: 1, end: 4 };
  let slice = store
    .get_range(&key, range)
    .await
    .expect("read range");

  assert_eq!(slice, Bytes::from_static(b"bcd"));
}

#[tokio::test]
async fn empty_range_returns_empty_bytes() {
  let store = InMemoryBlobStore::new();
  let key = BlobKey::from("topic/1/empty");
  let payload = Bytes::from_static(b"data");

  store.put(&key, payload).await.expect("put blob");

  let range = ByteRange { start: 2, end: 2 };
  let slice = store
    .get_range(&key, range)
    .await
    .expect("read empty range");

  assert!(slice.is_empty());
}
