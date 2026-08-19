use super::{bounded_full_read_limit, read_bounded_body};
use tokio::io::{AsyncWriteExt, duplex};

#[test]
fn bounded_full_read_requests_one_extra_byte() {
  assert_eq!(bounded_full_read_limit(64), 65);
}

#[test]
fn bounded_full_read_limit_saturates_at_u64_maximum() {
  assert_eq!(bounded_full_read_limit(u64::MAX), u64::MAX);
}

#[tokio::test]
async fn bounded_full_read_stops_after_the_overflow_byte() {
  let (mut writer, reader) = duplex(16);
  let write = tokio::spawn(async move { writer.write_all(b"abcdef").await });

  let bytes = read_bounded_body(reader, 4).await.unwrap();

  write.await.unwrap().unwrap();
  assert_eq!(bytes, b"abcde");
}
