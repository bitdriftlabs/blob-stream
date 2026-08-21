use super::read_content_length_body;
use tokio::io::{AsyncWriteExt, duplex};

#[tokio::test]
async fn content_length_read_accepts_the_advertised_length() {
  let (mut writer, reader) = duplex(16);
  writer.write_all(b"abcd").await.unwrap();
  drop(writer);

  let bytes = read_content_length_body(reader, 4).await.unwrap();

  assert_eq!(bytes, b"abcd");
}

#[tokio::test]
async fn content_length_read_rejects_a_short_body() {
  let (mut writer, reader) = duplex(16);
  writer.write_all(b"abc").await.unwrap();
  drop(writer);

  assert!(read_content_length_body(reader, 4).await.is_err());
}

#[tokio::test]
async fn content_length_read_reports_an_unallocatable_buffer() {
  let (_writer, reader) = duplex(16);

  assert!(read_content_length_body(reader, u64::MAX).await.is_err());
}
