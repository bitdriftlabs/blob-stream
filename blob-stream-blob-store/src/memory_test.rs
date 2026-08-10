use crate::{BlobKey, BlobStore, BlobStoreError, ByteRange, InMemoryBlobStore};
use bytes::Bytes;

#[tokio::test]
async fn stores_and_reads_ranges() {
  let store = InMemoryBlobStore::new();
  let key = BlobKey::from("topic/1/abc");
  let payload = Bytes::from_static(b"abcdef");

  store.put(&key, payload).await.expect("put blob");

  let range = ByteRange { start: 1, end: 4 };
  let slice = store.get_range(&key, range).await.expect("read range");

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

#[tokio::test]
async fn missing_key_returns_not_found() {
  let store = InMemoryBlobStore::new();
  let key = BlobKey::from("topic/1/missing");

  let error = store
    .get_range(&key, ByteRange { start: 0, end: 1 })
    .await
    .expect_err("missing blob should return an error");

  assert!(matches!(
    error,
    BlobStoreError::NotFound { key } if key == "topic/1/missing"
  ));
}
