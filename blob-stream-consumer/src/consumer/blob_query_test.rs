#![allow(clippy::unwrap_used)]

use super::*;
use blob_stream_proto::protos::blobstream::v1::broker::{
  BlobRangeRequest,
  BlobRangeResult,
  BlobReadFailure,
  BlobReadFailureStatus,
  BlobReadSuccess,
  ReadBlobRangesRequest,
  ReadBlobRangesResponse,
  read_blob_ranges_response,
};
use bytes::Bytes;

fn request() -> ReadBlobRangesRequest {
  ReadBlobRangesRequest {
    blob_key: "topic/blob".into(),
    ranges: vec![
      BlobRangeRequest {
        start: 2,
        end: 4,
        ..Default::default()
      },
      BlobRangeRequest {
        start: 7,
        end: 10,
        ..Default::default()
      },
    ],
    ..Default::default()
  }
}

#[test]
fn preserves_successful_response_order() {
  let response = ReadBlobRangesResponse {
    result: Some(read_blob_ranges_response::Result::Success(
      BlobReadSuccess {
        ranges: vec![
          BlobRangeResult {
            payload: Bytes::from_static(b"ab"),
            ..Default::default()
          },
          BlobRangeResult {
            payload: Bytes::from_static(b"cde"),
            ..Default::default()
          },
        ],
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  assert_eq!(
    decode_blob_range_response(&request(), response).unwrap(),
    BrokerBlobRangeRead::Success(vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cde")])
  );
}

#[test]
fn rejects_a_success_with_the_wrong_range_count() {
  let response = ReadBlobRangesResponse {
    result: Some(read_blob_ranges_response::Result::Success(
      BlobReadSuccess {
        ranges: vec![BlobRangeResult {
          payload: Bytes::from_static(b"ab"),
          ..Default::default()
        }],
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let error = decode_blob_range_response(&request(), response).unwrap_err();

  assert!(
    error
      .to_string()
      .contains("broker blob response has 1 ranges for 2 requested ranges")
  );
}

#[test]
fn rejects_a_success_with_the_wrong_payload_length() {
  let response = ReadBlobRangesResponse {
    result: Some(read_blob_ranges_response::Result::Success(
      BlobReadSuccess {
        ranges: vec![
          BlobRangeResult {
            payload: Bytes::from_static(b"ab"),
            ..Default::default()
          },
          BlobRangeResult {
            payload: Bytes::from_static(b"toolong"),
            ..Default::default()
          },
        ],
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let error = decode_blob_range_response(&request(), response).unwrap_err();

  assert!(
    error
      .to_string()
      .contains("broker blob response range has 7 bytes, expected 3")
  );
}

#[test]
fn rejects_a_response_without_an_outcome() {
  let error = decode_blob_range_response(&request(), ReadBlobRangesResponse::new()).unwrap_err();

  assert!(
    error
      .to_string()
      .contains("broker blob response has no result")
  );
}

#[test]
fn maps_not_found_failure_to_an_authoritative_result() {
  let response = ReadBlobRangesResponse {
    result: Some(read_blob_ranges_response::Result::Failure(
      BlobReadFailure {
        status: BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_NOT_FOUND.into(),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  assert_eq!(
    decode_blob_range_response(&request(), response).unwrap(),
    BrokerBlobRangeRead::NotFound
  );
}

#[test]
fn rejects_every_non_authoritative_failure_status() {
  for status in [
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_BAD_REQUEST,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_OVERLOADED,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_TOO_LARGE,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_STORAGE,
  ] {
    let response = ReadBlobRangesResponse {
      result: Some(read_blob_ranges_response::Result::Failure(
        BlobReadFailure {
          status: status.into(),
          ..Default::default()
        },
      )),
      ..Default::default()
    };

    let error = decode_blob_range_response(&request(), response).unwrap_err();
    assert!(
      error
        .to_string()
        .contains(&format!("broker blob request failed: status={status:?}"))
    );
  }
}
