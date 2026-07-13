#[cfg(test)]
#[path = "./consumer_test.rs"]
mod tests;

use crate::config::{
  ConsumerReadConfig,
  consumer_lookback_windows,
  consumer_window_size_seconds,
  validate_read_config,
};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use bd_server_stats::stats::Scope;
use blob_stream_blob_store::{BlobStore, ByteRange};
use blob_stream_metadata_store::MetadataStore;
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  CompressionCodec,
  Record,
  SeqRange,
  TopicWindowKey,
  VirtualPartitionId,
  Window,
};
use futures::future::try_join_all;
use log::trace;
use prometheus::{Histogram, IntCounter};
use protobuf::Message;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

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

#[derive(Clone, Debug, PartialEq)]
/// A decoded batch returned by the consumer read path.
pub struct ConsumerBatch {
  /// Virtual partition that owns this batch.
  pub virtual_partition_id: VirtualPartitionId,
  /// Inclusive sequence range for this batch.
  pub seq_range: SeqRange,
  /// Decoded records for the batch.
  pub records: Vec<Record>,
}

//
// ConsumerReaderMetrics
//

#[derive(Clone)]
struct ConsumerReaderMetrics {
  read_available_calls: IntCounter,
  read_available_empty: IntCounter,
  read_available_latency_seconds: Histogram,
  metadata_scan_requests: IntCounter,
  metadata_scan_segments: IntCounter,
  metadata_scan_latency_seconds: Histogram,
  metadata_batches_scanned: IntCounter,
  metadata_batches_skipped_by_cursor: IntCounter,
  blob_range_requests: IntCounter,
  blob_range_bytes: IntCounter,
  blob_range_latency_seconds: Histogram,
  decompression_latency_seconds: Histogram,
  protobuf_decode_latency_seconds: Histogram,
  batches_read: IntCounter,
  records_read: IntCounter,
  record_payload_bytes: IntCounter,
}

impl ConsumerReaderMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("reader");
    Self {
      read_available_calls: scope.counter("read_available_calls"),
      read_available_empty: scope.counter("read_available_empty"),
      read_available_latency_seconds: scope.histogram("read_available_latency_seconds"),
      metadata_scan_requests: scope.counter("metadata_scan_requests"),
      metadata_scan_segments: scope.counter("metadata_scan_segments"),
      metadata_scan_latency_seconds: scope.histogram("metadata_scan_latency_seconds"),
      metadata_batches_scanned: scope.counter("metadata_batches_scanned"),
      metadata_batches_skipped_by_cursor: scope.counter("metadata_batches_skipped_by_cursor"),
      blob_range_requests: scope.counter("blob_range_requests"),
      blob_range_bytes: scope.counter("blob_range_bytes"),
      blob_range_latency_seconds: scope.histogram("blob_range_latency_seconds"),
      decompression_latency_seconds: scope.histogram("decompression_latency_seconds"),
      protobuf_decode_latency_seconds: scope.histogram("protobuf_decode_latency_seconds"),
      batches_read: scope.counter("batches_read"),
      records_read: scope.counter("records_read"),
      record_payload_bytes: scope.counter("record_payload_bytes"),
    }
  }

  fn record_metadata_scan(&self, started_at: Instant, segment_count: usize) {
    self.metadata_scan_requests.inc();
    self
      .metadata_scan_segments
      .inc_by(u64::try_from(segment_count).unwrap_or(u64::MAX));
    self
      .metadata_scan_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  fn record_blob_range(&self, started_at: Instant, bytes: usize) {
    self.blob_range_requests.inc();
    self
      .blob_range_bytes
      .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    self
      .blob_range_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  fn record_batch(&self, record_count: usize, payload_bytes: usize) {
    self.batches_read.inc();
    self
      .records_read
      .inc_by(u64::try_from(record_count).unwrap_or(u64::MAX));
    self
      .record_payload_bytes
      .inc_by(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
  }

  fn record_read_available(
    &self,
    started_at: Instant,
    batch_count: usize,
    metadata_batches_scanned: usize,
    metadata_batches_skipped_by_cursor: usize,
  ) {
    self.read_available_calls.inc();
    if batch_count == 0 {
      self.read_available_empty.inc();
    }
    self
      .metadata_batches_scanned
      .inc_by(u64::try_from(metadata_batches_scanned).unwrap_or(u64::MAX));
    self
      .metadata_batches_skipped_by_cursor
      .inc_by(u64::try_from(metadata_batches_skipped_by_cursor).unwrap_or(u64::MAX));
    self
      .read_available_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }
}

//
// ConsumerReader
//

#[async_trait]
/// Low-level batch reader over blob + metadata stores.
pub trait ConsumerReader: Send {
  /// Scan available windows and return newly available batches.
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<ConsumerBatch>>;
  /// Return committed cursor for a virtual partition, if known.
  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64>;
  /// Return all tracked cursors.
  fn cursors(&self) -> HashMap<VirtualPartitionId, u64>;
}

//
// ConsumerReaderImpl
//

/// Default `ConsumerReader` implementation used by `ConsumerIteratorImpl`.
pub struct ConsumerReaderImpl {
  config: ConsumerReadConfig,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  assigned_virtual_partitions: Vec<VirtualPartitionId>,
  cursors: HashMap<VirtualPartitionId, u64>,
  metrics: ConsumerReaderMetrics,
}

impl ConsumerReaderImpl {
  /// Create a reader with explicit assignment and initial cursor state.
  pub fn new(
    config: ConsumerReadConfig,
    assigned_virtual_partitions: Vec<VirtualPartitionId>,
    initial_cursors: HashMap<VirtualPartitionId, u64>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    metrics_scope: Scope,
  ) -> Result<Self> {
    validate_read_config(&config)?;
    trace!(
      "consumer reader init: topic={}, assigned_partitions={}, initial_cursors={}",
      config.topic,
      assigned_virtual_partitions.len(),
      initial_cursors.len()
    );

    // Seed runtime cursors from the caller-provided committed state.
    // Missing partitions default to cursor 0 during read processing.
    Ok(Self {
      assigned_virtual_partitions,
      cursors: initial_cursors,
      config,
      blob_store,
      metadata_store,
      metrics: ConsumerReaderMetrics::new(&metrics_scope),
    })
  }

  fn scan_windows(&self, now_unix_seconds: i64) -> Vec<TopicWindowKey> {
    // Anchor scans to the current fixed-size time window and include a trailing lookback span.
    // Oldest -> newest ordering keeps processing chronology intuitive.
    let current_window =
      Window::for_timestamp(now_unix_seconds, consumer_window_size_seconds(&self.config))
        .start_unix_seconds;

    let lookback_windows = consumer_lookback_windows(&self.config);
    let window_size_seconds = consumer_window_size_seconds(&self.config);
    let mut windows = Vec::with_capacity(lookback_windows as usize);
    for offset in (0 .. lookback_windows).rev() {
      let window_start =
        current_window.saturating_sub(i64::from(offset).saturating_mul(window_size_seconds));
      windows.push(TopicWindowKey {
        topic: self.config.topic.to_string(),
        window_start_unix_seconds: window_start,
      });
    }

    trace!(
      "consumer scan windows computed: topic={}, count={}, now_unix_seconds={}",
      self.config.topic,
      windows.len(),
      now_unix_seconds
    );

    windows
  }

  async fn read_batch(
    &self,
    metadata: &blob_stream_metadata_store::SegmentMetadata,
    batch_metadata: &BatchMetadata,
    virtual_partition_id: VirtualPartitionId,
  ) -> Result<ConsumerBatch> {
    trace!(
      "consumer read batch start: topic={}, partition={}, blob_key={}, seq_start={}, seq_end={}",
      self.config.topic,
      virtual_partition_id,
      metadata.blob_key.as_str(),
      batch_metadata.seq_range.start,
      batch_metadata.seq_range.end
    );
    // Pull only the referenced byte range for this batch to avoid downloading full segments.
    let blob_read_started_at = Instant::now();
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
    self
      .metrics
      .record_blob_range(blob_read_started_at, payload.len());

    // Decode in two stages: transport/storage compression first, then logical RecordBatch format.
    let decompression_started_at = Instant::now();
    let decoded: Cow<'_, [u8]> = match batch_metadata.compression.codec {
      CompressionCodec::None => Cow::Borrowed(payload.as_ref()),
      CompressionCodec::Zstd => zstd::stream::decode_all(Cursor::new(payload.as_ref()))
        .map(Cow::Owned)
        .map_err(|error| anyhow!("failed to decode zstd batch: {error}"))?,
    };
    if matches!(batch_metadata.compression.codec, CompressionCodec::Zstd) {
      self
        .metrics
        .decompression_latency_seconds
        .observe(decompression_started_at.elapsed().as_secs_f64());
    }

    let protobuf_decode_started_at = Instant::now();
    let record_batch = StoredRecordBatch::parse_from_bytes(decoded.as_ref())
      .map_err(|error| anyhow!("failed to decode record batch protobuf: {error}"))?;
    self
      .metrics
      .protobuf_decode_latency_seconds
      .observe(protobuf_decode_started_at.elapsed().as_secs_f64());

    // Defensive integrity check: segment index entry and decoded payload must agree on partition.
    ensure!(
      record_batch.virtual_partition_id == virtual_partition_id,
      "decoded batch partition {} does not match expected {}",
      record_batch.virtual_partition_id,
      virtual_partition_id
    );

    let payload_bytes = record_batch.records.iter().fold(0_usize, |total, record| {
      total.saturating_add(record.payload.len())
    });
    self
      .metrics
      .record_batch(record_batch.records.len(), payload_bytes);

    Ok(ConsumerBatch {
      virtual_partition_id,
      seq_range: batch_metadata.seq_range.clone(),
      records: record_batch.records,
    })
  }

  /// Replace the current assignment set.
  pub fn set_assigned_virtual_partitions(
    &mut self,
    assigned_virtual_partitions: Vec<VirtualPartitionId>,
  ) -> Result<()> {
    self.assigned_virtual_partitions = assigned_virtual_partitions;
    Ok(())
  }

  /// Set the in-memory cursor for a virtual partition.
  pub fn set_cursor(&mut self, virtual_partition_id: VirtualPartitionId, seq_end: u64) {
    self.cursors.insert(virtual_partition_id, seq_end);
  }

  /// Update cursor monotonically with externally committed offset state.
  pub fn hydrate_cursor(&mut self, virtual_partition_id: VirtualPartitionId, seq_end: u64) {
    let current = self
      .cursors
      .get(&virtual_partition_id)
      .copied()
      .unwrap_or(0);
    self
      .cursors
      .insert(virtual_partition_id, current.max(seq_end));
  }
}

#[async_trait]
impl ConsumerReader for ConsumerReaderImpl {
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<ConsumerBatch>> {
    let read_started_at = Instant::now();
    trace!(
      "consumer read_available start: topic={}, assigned_partitions={}",
      self.config.topic,
      self.assigned_virtual_partitions.len()
    );
    // Output contains only newly consumable batches according to per-partition cursor state.
    let mut output = Vec::new();
    let mut metadata_batches_scanned = 0_usize;
    let mut metadata_batches_skipped_by_cursor = 0_usize;

    // Iterate through the lookback scan region to catch both current and delayed metadata.
    let windows = self.scan_windows(now_unix_seconds);
    let scan_futures = windows.iter().map(|window| {
      let metadata_store = Arc::clone(&self.metadata_store);
      let metrics = self.metrics.clone();
      async move {
        let segments = metadata_store.scan_window(window).await?;
        metrics.record_metadata_scan(read_started_at, segments.len());
        Ok::<_, anyhow::Error>((window, segments))
      }
    });

    // Run independent per-window metadata queries concurrently while preserving input order.
    let window_results = try_join_all(scan_futures).await?;

    for (window, mut segments) in window_results {
      trace!(
        "consumer scanned window: topic={}, window_start={}, segments={}",
        window.topic,
        window.window_start_unix_seconds,
        segments.len()
      );

      // Metadata scans are unordered by contract; sorting provides deterministic processing.
      segments.sort_by_key(|metadata| metadata.snowflake_id);

      for segment in segments {
        // Restrict work to currently assigned virtual partitions only.
        for partition_id in &self.assigned_virtual_partitions {
          let Some(partition_batches) = segment.segment_index.get(partition_id) else {
            continue;
          };

          metadata_batches_scanned =
            metadata_batches_scanned.saturating_add(partition_batches.len());
          let current_cursor = self.cursors.get(partition_id).copied();

          // Avoid sorting historical batches when this partition has already consumed all of
          // them. Any late batch beyond the cursor still uses the normal sorted path below.
          if current_cursor.is_some_and(|cursor| {
            partition_batches
              .iter()
              .all(|batch| batch.seq_range.end <= cursor)
          }) {
            metadata_batches_skipped_by_cursor =
              metadata_batches_skipped_by_cursor.saturating_add(partition_batches.len());
            continue;
          }

          // Metadata scans are unordered and can arrive late. Sorting by seq_start keeps
          // processing deterministic while cursor checks prevent replay.
          let mut sorted_batches = partition_batches.iter().collect::<Vec<_>>();
          sorted_batches.sort_by_key(|batch| batch.seq_range.start);

          for batch_metadata in sorted_batches {
            // Cursor semantics: seq_end <= cursor was already consumed and can be skipped.
            let current_cursor = self.cursors.get(partition_id).copied();
            if current_cursor.is_some_and(|cursor| batch_metadata.seq_range.end <= cursor) {
              trace!(
                "consumer skipped batch by cursor: topic={}, partition={}, seq_end={}, cursor={}",
                self.config.topic,
                partition_id,
                batch_metadata.seq_range.end,
                current_cursor.unwrap_or(0)
              );
              metadata_batches_skipped_by_cursor =
                metadata_batches_skipped_by_cursor.saturating_add(1);
              continue;
            }

            // Decode batch payload only after passing cursor filter to avoid unnecessary I/O.
            let batch = self
              .read_batch(&segment, batch_metadata, *partition_id)
              .await?;

            // Cursor always moves forward. max() keeps monotonicity if metadata ordering is odd.
            let next_cursor = batch.seq_range.end.max(current_cursor.unwrap_or(0));
            self.cursors.insert(*partition_id, next_cursor);
            trace!(
              "consumer accepted batch: topic={}, partition={}, seq_start={}, seq_end={}, \
               records={}, new_cursor={}",
              self.config.topic,
              partition_id,
              batch.seq_range.start,
              batch.seq_range.end,
              batch.records.len(),
              next_cursor
            );
            output.push(batch);
          }
        }
      }
    }

    trace!(
      "consumer read_available complete: topic={}, output_batches={}, \
       metadata_batches_scanned={}, metadata_batches_skipped_by_cursor={}",
      self.config.topic,
      output.len(),
      metadata_batches_scanned,
      metadata_batches_skipped_by_cursor
    );

    self.metrics.record_read_available(
      read_started_at,
      output.len(),
      metadata_batches_scanned,
      metadata_batches_skipped_by_cursor,
    );

    Ok(output)
  }

  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64> {
    self.cursors.get(&virtual_partition_id).copied()
  }

  fn cursors(&self) -> HashMap<VirtualPartitionId, u64> {
    self.cursors.clone()
  }
}
