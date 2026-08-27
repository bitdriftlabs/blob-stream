use blob_stream_proto::protos::blobstream::v1::broker::ProduceBatchResponse;

pub(super) fn broker_error_message(response: &ProduceBatchResponse) -> String {
  if response.error_message.is_empty() {
    format!(
      "broker status: {:?}",
      response.status.enum_value_or_default()
    )
  } else {
    response.error_message.to_string()
  }
}

pub(super) fn encoded_grouped_message_size(message_size: usize) -> usize {
  // Nested protobuf messages carry one field tag byte and a varint-encoded payload length.
  1usize
    .saturating_add(encoded_varint_size(message_size as u64))
    .saturating_add(message_size)
}

fn encoded_varint_size(mut value: u64) -> usize {
  // Protobuf encodes lengths in seven-bit groups, with the high bit marking continuation bytes.
  let mut size: usize = 1;
  while value >= 128 {
    size = size.saturating_add(1);
    value >>= 7;
  }
  size
}
