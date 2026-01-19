// blob-stream - blob store tests
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

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
