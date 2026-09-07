//! Read-only analysis of Blob Stream metadata rows around a consumer cursor.

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

use anyhow::{Result, bail};
use blob_stream_metadata_store::{MetadataReadConsistency, MetadataStore, SegmentMetadata};
use blob_stream_types::{BatchMetadata, TopicWindowKey, VirtualPartitionId, Window};
use serde::Serialize;
use std::ops::Range;
use time::{Duration, OffsetDateTime};

pub const MAX_WINDOW_RADIUS: u32 = 10;

//
// InspectorRequest
//

/// Bounded metadata query and cursor analysis inputs.
#[derive(Clone, Debug)]
pub struct InspectorRequest {
  pub topic: String,
  pub target_time: OffsetDateTime,
  pub suspect_cursor: u64,
  pub virtual_partition_id: VirtualPartitionId,
  pub metadata_window_seconds: i64,
  pub window_radius: u32,
}

//
// CursorRelation
//

/// How a batch sequence range relates to the first expected offset after the cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorRelation {
  BeforeOrAtCursor,
  CrossesCursor,
  AfterCursor,
}

//
// MetadataBatchRow
//

/// One requested-partition batch in an inspected metadata segment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MetadataBatchRow {
  pub window_start_unix_seconds: i64,
  pub snowflake_id: u64,
  pub blob_key: String,
  #[serde(with = "time::serde::rfc3339")]
  pub created_at: OffsetDateTime,
  #[serde(with = "time::serde::rfc3339")]
  pub metadata_published_at: OffsetDateTime,
  pub sequence_start: u64,
  pub sequence_end: u64,
  pub byte_start: u64,
  pub byte_end: u64,
  pub payload_bytes: u64,
  pub cursor_relation: CursorRelation,
}

//
// ContinuityIssue
//

/// A range relation that prevents contiguous advancement from the suspect cursor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityIssue {
  Gap {
    start: u64,
    end: u64,
  },
  Overlap {
    start: u64,
    end: u64,
  },
  OutOfOrder {
    sequence_start: u64,
    sequence_end: u64,
    expected_at_least: u64,
  },
}

//
// InspectionReport
//

/// Deterministic evidence extracted from all queried windows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InspectionReport {
  pub topic: String,
  #[serde(with = "time::serde::rfc3339")]
  pub target_time: OffsetDateTime,
  pub suspect_cursor: u64,
  pub virtual_partition_id: VirtualPartitionId,
  pub queried_window_starts: Vec<i64>,
  pub source_order_matches_sequence_order: bool,
  pub source_ordered_rows: Vec<MetadataBatchRow>,
  pub sequence_ordered_rows: Vec<MetadataBatchRow>,
  pub source_order_continuity_issues: Vec<ContinuityIssue>,
  pub continuity_issues: Vec<ContinuityIssue>,
}

/// Query the requested windows with strong consistency and build a deterministic report.
pub async fn inspect_metadata(
  store: &dyn MetadataStore,
  request: &InspectorRequest,
) -> Result<InspectionReport> {
  let queried_window_starts = queried_window_starts(request)?;
  let mut segments = Vec::new();
  for window_start_unix_seconds in &queried_window_starts {
    let window = TopicWindowKey {
      topic: request.topic.clone(),
      window_start_unix_seconds: *window_start_unix_seconds,
    };
    segments.extend(
      store
        .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Strong)
        .await?,
    );
  }
  Ok(build_report(request, queried_window_starts, segments))
}

/// Return inclusive window starts centered on the target timestamp's aligned window.
pub fn queried_window_starts(request: &InspectorRequest) -> Result<Vec<i64>> {
  if request.metadata_window_seconds <= 0 {
    bail!("metadata window seconds must be positive");
  }
  if request.window_radius > MAX_WINDOW_RADIUS {
    bail!("metadata window radius must not exceed {MAX_WINDOW_RADIUS}");
  }
  let window_size = Duration::seconds(request.metadata_window_seconds);
  let focal_window_start = Window::for_timestamp(request.target_time, window_size)
    .start
    .unix_timestamp();
  let radius_seconds = request
    .metadata_window_seconds
    .checked_mul(i64::from(request.window_radius))
    .ok_or_else(|| anyhow::anyhow!("metadata window radius overflow"))?;
  let first_window_start = focal_window_start
    .checked_sub(radius_seconds)
    .ok_or_else(|| anyhow::anyhow!("metadata window start underflow"))?;
  let window_count = request
    .window_radius
    .checked_mul(2)
    .and_then(|radius| radius.checked_add(1))
    .ok_or_else(|| anyhow::anyhow!("metadata window count overflow"))?;
  (0 .. window_count)
    .map(|index| {
      let window_offset = request
        .metadata_window_seconds
        .checked_mul(i64::from(index))
        .ok_or_else(|| anyhow::anyhow!("metadata window offset overflow"))?;
      first_window_start
        .checked_add(window_offset)
        .ok_or_else(|| anyhow::anyhow!("metadata window end overflow"))
    })
    .collect()
}

/// Build a report from metadata returned by the selected `DynamoDB` windows.
#[must_use]
pub fn build_report(
  request: &InspectorRequest,
  queried_window_starts: Vec<i64>,
  segments: Vec<SegmentMetadata>,
) -> InspectionReport {
  let mut source_ordered_rows = segments
    .into_iter()
    .flat_map(|segment| {
      rows_for_segment(
        &segment,
        request.virtual_partition_id,
        request.suspect_cursor,
      )
    })
    .collect::<Vec<_>>();
  source_ordered_rows.sort_by_key(|row| {
    (
      row.window_start_unix_seconds,
      row.snowflake_id,
      row.sequence_start,
      row.sequence_end,
    )
  });
  let mut sequence_ordered_rows = source_ordered_rows.clone();
  sequence_ordered_rows.sort_by_key(|row| {
    (
      row.sequence_start,
      row.sequence_end,
      row.window_start_unix_seconds,
      row.snowflake_id,
    )
  });
  let source_order_matches_sequence_order = source_ordered_rows == sequence_ordered_rows;
  let source_order_continuity_issues =
    continuity_issues(request.suspect_cursor, &source_ordered_rows);
  let continuity_issues = continuity_issues(request.suspect_cursor, &sequence_ordered_rows);
  InspectionReport {
    topic: request.topic.clone(),
    target_time: request.target_time,
    suspect_cursor: request.suspect_cursor,
    virtual_partition_id: request.virtual_partition_id,
    queried_window_starts,
    source_order_matches_sequence_order,
    source_ordered_rows,
    sequence_ordered_rows,
    source_order_continuity_issues,
    continuity_issues,
  }
}

/// Select a bounded sequence-order context around the first row after the supplied cursor.
#[must_use]
pub fn cursor_context_range(
  rows: &[MetadataBatchRow],
  suspect_cursor: u64,
  context_rows: usize,
) -> Range<usize> {
  // Sequence starts are sorted, but overlapping batches need not have sorted ends.
  let first_after_cursor = rows
    .iter()
    .position(|row| row.sequence_end > suspect_cursor)
    .unwrap_or(rows.len());
  let start = first_after_cursor.saturating_sub(context_rows);
  let end = first_after_cursor
    .saturating_add(context_rows)
    .saturating_add(1)
    .min(rows.len());
  start .. end
}

fn rows_for_segment(
  segment: &SegmentMetadata,
  virtual_partition_id: VirtualPartitionId,
  suspect_cursor: u64,
) -> Vec<MetadataBatchRow> {
  segment
    .segment_index
    .get(&virtual_partition_id)
    .into_iter()
    .flatten()
    .map(|batch| row_for_batch(segment, batch, suspect_cursor))
    .collect()
}

fn row_for_batch(
  segment: &SegmentMetadata,
  batch: &BatchMetadata,
  suspect_cursor: u64,
) -> MetadataBatchRow {
  let next_expected_offset = suspect_cursor.saturating_add(1);
  let cursor_relation = if batch.seq_range.end <= suspect_cursor {
    CursorRelation::BeforeOrAtCursor
  } else if batch.seq_range.start <= next_expected_offset {
    CursorRelation::CrossesCursor
  } else {
    CursorRelation::AfterCursor
  };
  MetadataBatchRow {
    window_start_unix_seconds: segment.window.window_start_unix_seconds,
    snowflake_id: segment.snowflake_id.as_u64(),
    blob_key: segment.blob_key.as_str().to_string(),
    created_at: segment.created_at,
    metadata_published_at: segment.metadata_published_at,
    sequence_start: batch.seq_range.start,
    sequence_end: batch.seq_range.end,
    byte_start: batch.byte_range.start,
    byte_end: batch.byte_range.end,
    payload_bytes: batch.payload_bytes,
    cursor_relation,
  }
}

fn continuity_issues(suspect_cursor: u64, rows: &[MetadataBatchRow]) -> Vec<ContinuityIssue> {
  let mut next_expected = suspect_cursor.saturating_add(1);
  let mut highest_range_start = next_expected;
  let mut issues = Vec::new();
  for row in rows {
    if row.sequence_end < next_expected {
      if row.sequence_end > suspect_cursor {
        if row.sequence_start >= highest_range_start {
          issues.push(ContinuityIssue::Overlap {
            start: row.sequence_start,
            end: row.sequence_end,
          });
        } else {
          issues.push(ContinuityIssue::OutOfOrder {
            sequence_start: row.sequence_start,
            sequence_end: row.sequence_end,
            expected_at_least: next_expected,
          });
        }
      }
      continue;
    }
    if row.sequence_start > next_expected {
      issues.push(ContinuityIssue::Gap {
        start: next_expected,
        end: row.sequence_start - 1,
      });
    } else {
      let overlap_start = row.sequence_start.max(suspect_cursor.saturating_add(1));
      if overlap_start < next_expected {
        issues.push(ContinuityIssue::Overlap {
          start: overlap_start,
          end: next_expected - 1,
        });
      }
    }
    if row.sequence_end.saturating_add(1) > next_expected {
      next_expected = row.sequence_end.saturating_add(1);
      highest_range_start = row.sequence_start;
    }
  }
  issues
}
