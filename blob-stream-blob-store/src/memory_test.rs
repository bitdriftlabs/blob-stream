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

#[tokio::test]
async fn bounded_full_read_returns_the_complete_blob() {
  let store = InMemoryBlobStore::new();
  let key = BlobKey::from("topic/1/full");
  let payload = Bytes::from_static(b"abcdef");
  store.put(&key, payload.clone()).await.expect("put blob");

  let fetched = store.get(&key, 6).await.expect("read full blob");

  assert_eq!(fetched, payload);
}

#[tokio::test]
async fn bounded_full_read_rejects_an_oversized_blob() {
  let store = InMemoryBlobStore::new();
  let key = BlobKey::from("topic/1/oversized");
  store
    .put(&key, Bytes::from_static(b"abcdef"))
    .await
    .expect("put blob");

  let error = store
    .get(&key, 5)
    .await
    .expect_err("oversized blob should be rejected");

  assert!(matches!(
    error,
    BlobStoreError::TooLarge {
      key,
      max_bytes: 5,
      actual_bytes: 6,
    } if key == "topic/1/oversized"
  ));
}
