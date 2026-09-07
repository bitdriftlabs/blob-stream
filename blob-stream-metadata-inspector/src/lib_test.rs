use super::{
  ContinuityIssue,
  CursorRelation,
  InspectorRequest,
  build_report,
  cursor_context_range,
  queried_window_starts,
};
use blob_stream_blob_store::BlobKey;
use blob_stream_metadata_store::SegmentMetadata;
use blob_stream_types::{
  BatchMetadata,
  ByteRange,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
};
use std::collections::HashMap;
use time::OffsetDateTime;
use time::macros::datetime;

fn request(suspect_cursor: u64) -> InspectorRequest {
  InspectorRequest {
    topic: "telemetry".to_string(),
    target_time: datetime!(2026-09-05 13:16:17 UTC),
    suspect_cursor,
    virtual_partition_id: 1,
    metadata_window_seconds: 300,
    window_radius: 1,
  }
}

fn segment(snowflake_id: u64, sequence_start: u64, sequence_end: u64) -> SegmentMetadata {
  SegmentMetadata::new(
    TopicWindowKey {
      topic: "telemetry".to_string(),
      window_start_unix_seconds: 1_788_612_500,
    },
    SnowflakeId(snowflake_id),
    BlobKey::new(format!("telemetry/{snowflake_id}.bin")),
    Compression::none(),
    HashMap::from([(
      1,
      vec![BatchMetadata {
        seq_range: SeqRange {
          start: sequence_start,
          end: sequence_end,
        },
        byte_range: ByteRange { start: 0, end: 10 },
        payload_bytes: 10,
      }],
    )]),
    OffsetDateTime::UNIX_EPOCH,
    OffsetDateTime::UNIX_EPOCH,
  )
}

#[test]
fn queried_windows_include_the_target_window_and_neighbors() {
  assert_eq!(
    queried_window_starts(&request(0)).unwrap(),
    vec![1_788_613_800, 1_788_614_100, 1_788_614_400]
  );
}

#[test]
fn queried_windows_reject_an_overflowing_window_offset() {
  let mut request = request(0);
  request.metadata_window_seconds = i64::MAX;
  request.window_radius = 1;

  let error = queried_window_starts(&request).expect_err("overflowing offset must be rejected");
  assert!(error.to_string().contains("overflow"));
}

#[test]
fn report_sorts_unordered_sources_and_finds_the_uncovered_range() {
  let request = request(11_480_895_592);
  let report = build_report(
    &request,
    vec![1_788_612_200, 1_788_612_500, 1_788_612_800],
    vec![
      segment(2, 11_480_907_916, 11_480_907_920),
      segment(1, 11_480_895_580, 11_480_895_592),
    ],
  );
  assert!(report.source_order_matches_sequence_order);
  assert_eq!(report.source_ordered_rows[0].snowflake_id, 1);
  assert_eq!(report.source_ordered_rows[1].snowflake_id, 2);
  assert_eq!(
    report.sequence_ordered_rows[1].cursor_relation,
    CursorRelation::AfterCursor
  );
  assert_eq!(
    report.continuity_issues,
    vec![ContinuityIssue::Gap {
      start: 11_480_895_593,
      end: 11_480_907_915,
    }]
  );
}

#[test]
fn report_flags_source_order_that_differs_from_sequence_order() {
  let report = build_report(
    &request(10),
    vec![1_788_612_500],
    vec![segment(1, 21, 25), segment(2, 11, 20)],
  );
  assert!(!report.source_order_matches_sequence_order);
  assert_eq!(
    report.source_order_continuity_issues,
    vec![
      ContinuityIssue::Gap { start: 11, end: 20 },
      ContinuityIssue::OutOfOrder {
        sequence_start: 11,
        sequence_end: 20,
        expected_at_least: 26,
      },
    ]
  );
  assert!(report.continuity_issues.is_empty());
}

#[test]
fn report_does_not_flag_the_consumed_prefix_of_a_cursor_crossing_batch() {
  let report = build_report(&request(10), vec![1_788_612_500], vec![segment(1, 1, 100)]);

  assert!(report.continuity_issues.is_empty());
}

#[test]
fn report_clamps_a_true_overlap_to_the_inspected_interval() {
  let report = build_report(
    &request(10),
    vec![1_788_612_500],
    vec![segment(1, 1, 20), segment(2, 5, 30)],
  );

  assert_eq!(
    report.continuity_issues,
    vec![ContinuityIssue::Overlap { start: 11, end: 20 }]
  );
}

#[test]
fn cursor_context_limits_the_rows_around_the_boundary() {
  let report = build_report(
    &request(100),
    vec![1_788_612_500],
    vec![
      segment(1, 80, 90),
      segment(2, 91, 100),
      segment(3, 101, 110),
      segment(4, 111, 120),
      segment(5, 121, 130),
    ],
  );
  assert_eq!(
    cursor_context_range(&report.sequence_ordered_rows, 100, 1),
    1 .. 4
  );
}

#[test]
fn cursor_context_finds_a_cursor_crossed_by_an_overlapping_range() {
  let report = build_report(
    &request(10),
    vec![1_788_612_500],
    vec![segment(1, 1, 100), segment(2, 2, 2), segment(3, 3, 3)],
  );

  assert_eq!(
    cursor_context_range(&report.sequence_ordered_rows, 10, 0),
    0 .. 1
  );
}
