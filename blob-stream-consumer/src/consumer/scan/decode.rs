use super::{
  BatchMetadata,
  BatchReadCandidate,
  BatchReadResult,
  CommittedSourceCheckpoint,
  CompressionCodec,
  ConsumerBatch,
  ConsumerReaderImpl,
  Result,
  SegmentMetadata,
  SegmentReadPlan,
  StoredRecordBatch,
  VirtualPartitionId,
  trace,
};
use anyhow::{anyhow, ensure};
use blob_stream_blob_store::{BlobStoreError, ByteRange};
use protobuf::Message;
use std::io::Cursor;
use std::time::Instant;

impl ConsumerReaderImpl {
  /// Fetch one segment range and decode all of its selected batches in planning order.
  pub(in crate::consumer) async fn read_segment_plan(
    &self,
    plan: SegmentReadPlan,
  ) -> Result<Vec<BatchReadResult>> {
    trace!(
      "consumer read segment range start: topic={}, blob_key={}, start={}, end={}, batches={}",
      self.config.topic,
      plan.metadata.blob_key.as_str(),
      plan.byte_range.start,
      plan.byte_range.end,
      plan.candidates.len()
    );

    let SegmentReadPlan {
      metadata,
      candidates,
      byte_range,
    } = plan;
    let blob_read_started_at = Instant::now();
    self.metrics.record_fallback_blob_range_request();
    let payload = match self
      .blob_store
      .get_range(&metadata.blob_key, byte_range.clone())
      .await
    {
      Ok(payload) => payload,
      Err(BlobStoreError::NotFound { .. }) => {
        return Ok(Self::missing_segment_plan(SegmentReadPlan {
          metadata,
          candidates,
          byte_range,
        }));
      },
      Err(error) => return Err(error.into()),
    };
    self
      .metrics
      .record_fallback_blob_range_success(blob_read_started_at, payload.len());

    self.decode_segment_plan_payload(
      SegmentReadPlan {
        metadata,
        candidates,
        byte_range,
      },
      &payload,
    )
  }

  /// Decode a complete raw payload supplied for one segment read plan.
  pub(in crate::consumer) fn decode_segment_plan_payload(
    &self,
    plan: SegmentReadPlan,
    payload: &bytes::Bytes,
  ) -> Result<Vec<BatchReadResult>> {
    let SegmentReadPlan {
      metadata,
      candidates,
      byte_range,
    } = plan;

    let decoded_batches =
      self.decode_segment_payload(&metadata, &candidates, &byte_range, payload)?;
    Ok(
      candidates
        .into_iter()
        .zip(decoded_batches)
        .map(|(candidate, batch)| {
          self.record_decoded_batch(&batch);
          BatchReadResult::Decoded { candidate, batch }
        })
        .collect(),
    )
  }

  /// Decode a payload for a plan without consuming it, so callers can retry the plan on failure.
  pub(in crate::consumer) fn decode_segment_payload(
    &self,
    metadata: &SegmentMetadata,
    candidates: &[BatchReadCandidate],
    byte_range: &ByteRange,
    payload: &bytes::Bytes,
  ) -> Result<Vec<ConsumerBatch>> {
    let mut decoded_batches = Vec::with_capacity(candidates.len());
    for candidate in candidates {
      let batch_range = &candidate.batch_metadata.byte_range;
      let start = batch_range
        .start
        .checked_sub(byte_range.start)
        .ok_or_else(|| anyhow!("batch range starts before its segment read range"))?;
      let end = batch_range
        .end
        .checked_sub(byte_range.start)
        .ok_or_else(|| anyhow!("batch range ends before its segment read range"))?;
      let start =
        usize::try_from(start).map_err(|_| anyhow!("batch range start does not fit in memory"))?;
      let end =
        usize::try_from(end).map_err(|_| anyhow!("batch range end does not fit in memory"))?;
      ensure!(
        start < end && end <= payload.len(),
        "batch range is outside fetched segment range: start={start}, end={end}, fetched_bytes={}",
        payload.len()
      );
      let batch_payload = payload.slice(start .. end);
      let batch = self.decode_batch(
        metadata,
        &candidate.batch_metadata,
        candidate.virtual_partition_id,
        batch_payload,
      )?;
      decoded_batches.push(batch);
    }

    Ok(decoded_batches)
  }

  /// Produce missing results for every batch selected from an authoritative absent blob.
  pub(in crate::consumer) fn missing_segment_plan(plan: SegmentReadPlan) -> Vec<BatchReadResult> {
    let SegmentReadPlan {
      metadata,
      candidates,
      ..
    } = plan;
    candidates
      .into_iter()
      .map(|candidate| BatchReadResult::Missing {
        candidate,
        blob_key: metadata.blob_key.clone(),
      })
      .collect()
  }

  /// Decompress, validate, and decode one batch payload supplied by a segment range read.
  pub(in crate::consumer) fn decode_batch(
    &self,
    metadata: &SegmentMetadata,
    batch_metadata: &BatchMetadata,
    virtual_partition_id: VirtualPartitionId,
    payload: bytes::Bytes,
  ) -> Result<ConsumerBatch> {
    trace!(
      "consumer decode batch: topic={}, partition={}, blob_key={}, seq_start={}, seq_end={}",
      self.config.topic,
      virtual_partition_id,
      metadata.blob_key.as_str(),
      batch_metadata.seq_range.start,
      batch_metadata.seq_range.end
    );

    // Decode in two stages: transport/storage compression first, then logical RecordBatch format.
    let decoded = match metadata.compression.codec {
      CompressionCodec::None => payload,
      CompressionCodec::Zstd => zstd::stream::decode_all(Cursor::new(payload))
        .map(bytes::Bytes::from)
        .map_err(|error| anyhow!("failed to decode zstd batch: {error}"))?,
    };

    let record_batch = StoredRecordBatch::parse_from_tokio_bytes(&decoded)
      .map_err(|error| anyhow!("failed to decode record batch protobuf: {error}"))?;

    // Defensive integrity check: segment index entry and decoded payload must agree on partition.
    ensure!(
      record_batch.virtual_partition_id == virtual_partition_id,
      "decoded batch partition {} does not match expected {}",
      record_batch.virtual_partition_id,
      virtual_partition_id
    );
    ensure!(
      u64::try_from(record_batch.records.len()).unwrap_or(u64::MAX)
        == batch_metadata.seq_range.len(),
      "decoded record count {} does not match sequence range {}..={}",
      record_batch.records.len(),
      batch_metadata.seq_range.start,
      batch_metadata.seq_range.end
    );

    Ok(ConsumerBatch {
      virtual_partition_id,
      seq_range: batch_metadata.seq_range.clone(),
      source_checkpoint: CommittedSourceCheckpoint {
        window_start_unix_seconds: metadata.window.window_start_unix_seconds,
        snowflake_id: metadata.snowflake_id.as_u64(),
      },
      records: record_batch.records,
    })
  }

  pub(in crate::consumer) fn record_decoded_batch(&self, batch: &ConsumerBatch) {
    let payload_bytes = batch.records.iter().fold(0_usize, |total, record| {
      total.saturating_add(record.payload.len())
    });
    self
      .metrics
      .record_batch(batch.records.len(), payload_bytes);
  }
}
