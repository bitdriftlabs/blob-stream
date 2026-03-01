// blob-stream - consumer read path
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./consumer_test.rs"]
mod tests;

use crate::config::ConsumerReadConfig;
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use blob_stream_blob_store::{BlobStore, ByteRange};
use blob_stream_metadata_store::MetadataStore;
use blob_stream_types::{
  BatchMetadata,
  CompressionCodec,
  Record,
  RecordBatch,
  SeqRange,
  TopicWindowKey,
  VirtualPartitionId,
  Window,
};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

//
// Scanning algorithm overview
//
// This reader implements the Milestone 9 consumer read path with a deliberately simple
// deterministic algorithm:
//
// 1) Compute a trailing window set
//    - For each read call, compute the current wall-clock window and include N lookback windows
//      ending at "now". This is the core protection against delayed metadata writes.
//
// 2) Scan metadata per window
//    - Query the metadata store for each window independently.
//    - Because metadata scans are intentionally unordered for cost, sort segments by snowflake id
//      to stabilize traversal order.
//
// 3) Filter to assigned virtual partitions
//    - For each segment, only inspect segment_index entries that belong to the partitions assigned
//      to this reader.
//
// 4) Sort and cursor-filter batches
//    - Sort per-partition batch metadata by seq_start to keep processing deterministic.
//    - Skip a batch when seq_end <= committed cursor, which dedupes both normal rescans and late
//      metadata arrivals.
//
// 5) Fetch blob byte range and decode
//    - Read exactly the byte range for the batch from blob storage.
//    - Decode compression (none/zstd), then decode RecordBatch JSON.
//    - Validate that decoded partition id matches metadata partition id.
//
// 6) Advance cursor monotonically
//    - After successful decode, advance cursor to max(existing_cursor, batch.seq_end) and emit
//      ConsumerBatch.
//
// Handling out-of-order and late metadata (why writes are not missed)
//
// The metadata store scan contract is unordered, and metadata writes can appear later than
// expected due to retries/throttling/failover. The code handles this with two mechanisms:
//
// - Time-based lookback replay: read_available() always scans a trailing window set
//   (scan_windows()), not only the newest window. This ensures previously scanned windows are
//   revisited.
//
// - Cursor-based idempotence: For each virtual partition, a batch is skipped only when seq_end <=
//   current_cursor. Therefore, a newly discovered late batch with seq_end > current_cursor is still
//   processed, even if its metadata appears in an older window that was already scanned.
//
// Worked example (matches the implementation in read_available())
//
// Assume window_size_seconds=300 and lookback_windows=3. For partition 11:
//
// - T1: Cursor is 2 after processing batch [1..2].
// - T2: A new batch [3..4] is produced, but its metadata row for an older window arrives late.
// - T3: read_available() runs again:
//   1. scan_windows() includes that older window because it is still inside the 3-window lookback.
//   2. scan_window() returns unordered segments; code sorts by snowflake_id.
//   3. For partition 11, code sorts batch metadata by seq_start, then checks cursor.
//   4. Since 4 > cursor(2), batch [3..4] is fetched/decoded and emitted.
//   5. Cursor advances to 4.
//
// Producer ordering/fencing assumptions (why cursor logic is safe)
//
// The consumer algorithm relies on producer+broker write-path invariants for each virtual
// partition:
//
// - Single active writer via lease fencing: Brokers must hold the producer-partition lease to
//   accept writes for a virtual partition. A broker without the lease rejects writes, preventing
//   concurrent seq assignment.
//
// - Monotonic sequence assignment: The sequence allocator (Hi-Lo reservation) hands out strictly
//   increasing seq values per virtual partition. Reservations may introduce gaps, but
//   overlapping/reused seq ranges are not allowed.
//
// - Retry semantics preserve monotonic progress: If routing is stale or lease ownership changes,
//   producers retry to the current lease holder. The accepted batch still receives seq ranges from
//   the active monotonic allocator.
//
// Given these invariants, cursor-based filtering by seq_end is correct: once cursor reaches X,
// any later valid batch for the same virtual partition must have seq_end > X (or be a duplicate
// replay of already processed data that safely satisfies seq_end <= X).
//
// Important bound:
// This guarantee is bounded by lookback_windows. If metadata arrives later than the configured
// lookback horizon, this reader will not rediscover it without a wider lookback or an explicit
// rewind/recovery scan policy.
//
// This design optimizes for correctness and operational clarity over aggressive optimization:
// no global ordering assumptions, resilient to delayed metadata writes, and deterministic
// replay behavior through monotonic cursors.

//
// ConsumerBatch
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerBatch {
  pub virtual_partition_id: VirtualPartitionId,
  pub seq_range: SeqRange,
  pub records: Vec<Record>,
}

//
// ConsumerReader
//

#[async_trait]
pub trait ConsumerReader: Send {
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<ConsumerBatch>>;
  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64>;
  fn cursors(&self) -> HashMap<VirtualPartitionId, u64>;
}

//
// ConsumerReaderImpl
//

pub struct ConsumerReaderImpl {
  config: ConsumerReadConfig,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  cursors: HashMap<VirtualPartitionId, u64>,
}

impl ConsumerReaderImpl {
  pub fn new(
    config: ConsumerReadConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
  ) -> Result<Self> {
    config.validate()?;

    // Seed runtime cursors from the caller-provided committed state.
    // Missing partitions default to cursor 0 during read processing.
    Ok(Self {
      cursors: config.initial_cursors.clone(),
      config,
      blob_store,
      metadata_store,
    })
  }

  fn scan_windows(&self, now_unix_seconds: i64) -> Vec<TopicWindowKey> {
    // Anchor scans to the current fixed-size time window and include a trailing lookback span.
    // Oldest -> newest ordering keeps processing chronology intuitive.
    let current_window =
      Window::for_timestamp(now_unix_seconds, self.config.window_size_seconds).start_unix_seconds;

    let mut windows = Vec::with_capacity(self.config.lookback_windows as usize);
    for offset in (0 .. self.config.lookback_windows).rev() {
      let window_start = current_window
        .saturating_sub(i64::from(offset).saturating_mul(self.config.window_size_seconds));
      windows.push(TopicWindowKey {
        topic: self.config.topic.clone(),
        window_start_unix_seconds: window_start,
      });
    }

    windows
  }

  async fn read_batch(
    &self,
    metadata: &blob_stream_metadata_store::SegmentMetadata,
    batch_metadata: &BatchMetadata,
    virtual_partition_id: VirtualPartitionId,
  ) -> Result<ConsumerBatch> {
    // Pull only the referenced byte range for this batch to avoid downloading full segments.
    let payload = self
      .blob_store
      .get_range(
        &metadata.blob_key,
        ByteRange {
          start: batch_metadata.byte_range.start,
          end: batch_metadata.byte_range.end,
        },
      )
      .await?;

    // Decode in two stages: transport/storage compression first, then logical RecordBatch format.
    let decoded = match batch_metadata.compression.codec {
      CompressionCodec::None => payload.to_vec(),
      CompressionCodec::Zstd => zstd::stream::decode_all(Cursor::new(payload.as_ref()))
        .map_err(|error| anyhow!("failed to decode zstd batch: {error}"))?,
    };

    // Record batches are serialized as JSON in the current write path implementation.
    let record_batch: RecordBatch = serde_json::from_slice(&decoded)
      .map_err(|error| anyhow!("failed to decode record batch JSON: {error}"))?;

    // Defensive integrity check: segment index entry and decoded payload must agree on partition.
    ensure!(
      record_batch.virtual_partition_id == virtual_partition_id,
      "decoded batch partition {} does not match expected {}",
      record_batch.virtual_partition_id,
      virtual_partition_id
    );

    Ok(ConsumerBatch {
      virtual_partition_id,
      seq_range: batch_metadata.seq_range.clone(),
      records: record_batch.records,
    })
  }
}

#[async_trait]
impl ConsumerReader for ConsumerReaderImpl {
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<ConsumerBatch>> {
    // Output contains only newly consumable batches according to per-partition cursor state.
    let mut output = Vec::new();

    // Iterate through the lookback scan region to catch both current and delayed metadata.
    for window in self.scan_windows(now_unix_seconds) {
      let mut segments = self.metadata_store.scan_window(&window, None).await?;

      // Metadata scans are unordered by contract; sorting provides deterministic processing.
      segments.sort_by_key(|metadata| metadata.snowflake_id);

      for segment in segments {
        // Restrict work to currently assigned virtual partitions only.
        for partition_id in &self.config.assigned_virtual_partitions {
          let Some(partition_batches) = segment.segment_index.get(partition_id) else {
            continue;
          };

          // Metadata scans are unordered and can arrive late. Sorting by seq_start keeps
          // processing deterministic while cursor checks prevent replay.
          let mut sorted_batches = partition_batches.clone();
          sorted_batches.sort_by_key(|batch| batch.seq_range.start);

          for batch_metadata in sorted_batches {
            // Cursor semantics: seq_end <= cursor was already consumed and can be skipped.
            let current_cursor = self.cursors.get(partition_id).copied().unwrap_or(0);
            if batch_metadata.seq_range.end <= current_cursor {
              continue;
            }

            // Decode batch payload only after passing cursor filter to avoid unnecessary I/O.
            let batch = self
              .read_batch(&segment, &batch_metadata, *partition_id)
              .await?;

            // Cursor always moves forward. max() keeps monotonicity if metadata ordering is odd.
            let next_cursor = batch.seq_range.end.max(current_cursor);
            self.cursors.insert(*partition_id, next_cursor);
            output.push(batch);
          }
        }
      }
    }

    Ok(output)
  }

  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64> {
    self.cursors.get(&virtual_partition_id).copied()
  }

  fn cursors(&self) -> HashMap<VirtualPartitionId, u64> {
    self.cursors.clone()
  }
}
