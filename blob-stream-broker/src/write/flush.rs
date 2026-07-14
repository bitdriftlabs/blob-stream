use super::{
  BufferedBatch,
  DEFAULT_ZSTD_LEVEL,
  FlushPartition,
  FlushPlan,
  SegmentEnvelope,
  WriteConfig,
  WriteError,
};
use anyhow::{Context, Result};
use bd_time::OffsetDateTimeExt;
use blob_stream_blob_store::{BlobKey, BlobStore};
use blob_stream_metadata_store::MetadataStore;
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  CompressionCodec,
  Record,
  SnowflakeId,
  VirtualPartitionId,
  Window,
};
use bytes::{Bytes, BytesMut};
use log::{debug, trace};
use protobuf::Message;
use sonyflake::Sonyflake;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use time::OffsetDateTime;

//
// SnowflakeGenerator
//

pub(super) struct SnowflakeGenerator {
  generator: Sonyflake,
}

impl SnowflakeGenerator {
  pub(super) fn new() -> Result<Self> {
    let generator = Sonyflake::new().context("initialize sonyflake generator")?;
    Ok(Self { generator })
  }

  fn next(&self, now: OffsetDateTime) -> Result<SnowflakeId> {
    let value = self
      .generator
      .next_id(now)
      .context("generate sonyflake id")?;
    Ok(SnowflakeId(value))
  }
}

//
// FlushContext
//

#[derive(Clone)]
pub struct FlushContext {
  config: WriteConfig,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  snowflake: Arc<SnowflakeGenerator>,
}

impl FlushContext {
  pub fn new(
    config: WriteConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    snowflake: SnowflakeGenerator,
  ) -> Self {
    Self {
      config,
      blob_store,
      metadata_store,
      snowflake: Arc::new(snowflake),
    }
  }

  fn make_blob_key(&self, topic: &str, window: &Window, snowflake_id: SnowflakeId) -> BlobKey {
    let extension = match self.config.compression.codec {
      CompressionCodec::None => "bin",
      CompressionCodec::Zstd => "zst",
    };

    let mut key = String::new();
    if let Some(prefix) = &self.config.blob_prefix {
      let prefix = prefix.trim();
      if !prefix.is_empty() {
        key.push_str(prefix.trim_end_matches('/'));
        key.push('/');
      }
    }

    key.push_str(topic);
    key.push('/');
    key.push_str(&window.start_unix_seconds.to_string());
    key.push('/');
    key.push_str(&snowflake_id.as_u64().to_string());
    key.push('.');
    key.push_str(extension);

    BlobKey::new(key)
  }

  fn encode_batch(
    virtual_partition_id: VirtualPartitionId,
    records: Vec<Record>,
  ) -> Result<Vec<u8>> {
    let proto_batch = StoredRecordBatch {
      virtual_partition_id,
      records,
      ..Default::default()
    };
    proto_batch.write_to_bytes().context("encode record batch")
  }

  fn compress_batch(&self, payload: &[u8]) -> Result<Bytes> {
    match self.config.compression.codec {
      CompressionCodec::None => Ok(Bytes::copy_from_slice(payload)),
      CompressionCodec::Zstd => {
        let level = self.config.compression.level.unwrap_or(DEFAULT_ZSTD_LEVEL);
        let compressed =
          zstd::stream::encode_all(Cursor::new(payload), level).context("compress batch")?;
        Ok(Bytes::from(compressed))
      },
    }
  }

  fn build_segment(
    &self,
    topic: &str,
    partitions: Vec<FlushPartition>,
    now: OffsetDateTime,
  ) -> Result<(Bytes, SegmentEnvelope)> {
    trace!("building flush segment: topic={topic}");
    let now_ts_ms = now.unix_timestamp_ms();
    let window = Window::for_timestamp(now.unix_timestamp(), self.config.window_size_seconds);
    let snowflake_id = self.snowflake.next(now)?;
    let blob_key = self.make_blob_key(topic, &window, snowflake_id);
    let compression = self.config.compression.clone();

    let mut payload = BytesMut::new();
    let mut segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>> = HashMap::new();
    let mut record_count = 0_u64;
    let mut min_event_ts_ms: Option<i64> = None;
    let mut max_event_ts_ms: Option<i64> = None;

    for partition in partitions {
      trace!(
        "encoding partition batches: topic={}, virtual_partition_id={}, batches={}",
        topic,
        partition.virtual_partition_id,
        partition.batches.len()
      );
      for batch in partition.batches {
        let BufferedBatch {
          records,
          summary,
          seq_range,
          ..
        } = batch;
        let encoded = Self::encode_batch(partition.virtual_partition_id, records)?;
        let compressed = self.compress_batch(&encoded)?;
        let start = payload.len() as u64;
        payload.extend_from_slice(&compressed);
        let end = payload.len() as u64;

        record_count = record_count.saturating_add(u64::from(summary.record_count));
        min_event_ts_ms = min_event_ts_ms.map_or(Some(summary.min_event_ts_ms), |current| {
          Some(current.min(summary.min_event_ts_ms))
        });
        max_event_ts_ms = max_event_ts_ms.map_or(Some(summary.max_event_ts_ms), |current| {
          Some(current.max(summary.max_event_ts_ms))
        });

        let metadata = BatchMetadata {
          seq_range,
          byte_range: blob_stream_types::ByteRange { start, end },
          summary,
          compression: compression.clone(),
        };

        segment_index
          .entry(partition.virtual_partition_id)
          .or_default()
          .push(metadata);
      }
    }

    let min_event_ts_ms = min_event_ts_ms.unwrap_or(now_ts_ms);
    let max_event_ts_ms = max_event_ts_ms.unwrap_or(now_ts_ms);

    let envelope = SegmentEnvelope {
      window: window.key(topic),
      snowflake_id,
      blob_key,
      segment_index,
      compression,
      record_count,
      min_event_ts_ms,
      max_event_ts_ms,
      created_ts_ms: now_ts_ms,
    };

    debug!(
      "built segment envelope: topic={}, partitions={}, records={}, bytes={}",
      topic,
      envelope.segment_index.len(),
      envelope.record_count,
      payload.len()
    );

    Ok((payload.freeze(), envelope))
  }

  pub(super) async fn flush_plan(
    &self,
    plan: &mut FlushPlan,
    now: OffsetDateTime,
  ) -> Result<(), WriteError> {
    let partitions = std::mem::take(&mut plan.partitions);
    let (payload, envelope) = self.build_segment(&plan.topic, partitions, now)?;
    let metadata = envelope.into_metadata();
    let payload_bytes = payload.len();
    let record_count = metadata.record_count;
    let partition_count = metadata.segment_index.len();

    self
      .blob_store
      .put(&metadata.blob_key, payload)
      .await
      .context("write segment blob")?;
    self
      .metadata_store
      .write_segment(metadata)
      .await
      .context("write segment metadata")?;

    debug!(
      "flush persisted segment: topic={}, partitions={partition_count}, records={record_count}, \
       bytes={payload_bytes}",
      plan.topic
    );
    Ok(())
  }

  #[must_use]
  pub(super) fn config(&self) -> &WriteConfig {
    &self.config
  }
}
