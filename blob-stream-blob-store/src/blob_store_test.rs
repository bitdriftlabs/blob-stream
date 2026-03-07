use crate::{BlobKey, ByteRange};

#[test]
fn blob_key_conversions() {
  let key = BlobKey::new("topic/123");
  assert_eq!(key.as_str(), "topic/123");

  let from_str: BlobKey = "topic/456".into();
  assert_eq!(from_str.as_str(), "topic/456");
}

#[test]
fn byte_range_len() {
  let range = ByteRange { start: 10, end: 18 };

  assert_eq!(range.len(), 8);
  assert!(!range.is_empty());
}
