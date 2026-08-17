use super::buffer::{BufferedBatch, FlushPartition, FlushPlan};
use super::metrics::WriteMetrics;
use super::{BrokerLifecycleHooks, DEFAULT_ZSTD_LEVEL, WriteConfig, WriteError};
use anyhow::{Context, Result};
use bd_time::OffsetDateTimeExt;
use blob_stream_blob_store::{BlobKey, BlobStore};
use blob_stream_metadata_store::{
  MetadataStore,
  MetadataWriteError,
  ProducerPartitionFence,
  ProducerPartitionLeaseKey,
};
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  BatchSummary,
  CompressionCodec,
  Record,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
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
use std::time::Instant;
use time::OffsetDateTime;
use tokio::time::timeout;

#[cfg(test)]
#[path = "./flush_test.rs"]
mod tests;

//
// SnowflakeGenerator
//

pub(super) struct SnowflakeGenerator {
  generator: Sonyflake,
}

use bd_time::TimeProvider;
impl SnowflakeGenerator {
  pub(super) fn new() -> Result<Self> {
    let generator = Sonyflake::builder()
      .finalize()
      .context("initialize sonyflake generator")?;
    Ok(Self { generator })
  }

  pub(super) fn with_machine_id(machine_id: u16) -> Result<Self> {
    let generator = Sonyflake::builder()
      .machine_id(&|| Ok(machine_id))
      .finalize()
      .context("initialize sonyflake generator")?;
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
// SegmentEnvelope
//

#[derive(Debug)]
struct SegmentEnvelope {
  window: TopicWindowKey,
  snowflake_id: SnowflakeId,
  blob_key: BlobKey,
  compression: blob_stream_types::Compression,
  segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
  record_count: u64,
  created_at: OffsetDateTime,
}

impl SegmentEnvelope {
  fn into_metadata(
    self,
    metadata_published_at: OffsetDateTime,
  ) -> blob_stream_metadata_store::SegmentMetadata {
    blob_stream_metadata_store::SegmentMetadata::new(
      self.window,
      self.snowflake_id,
      self.blob_key,
      self.compression,
      self.segment_index,
      self.created_at,
      metadata_published_at,
    )
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
  time_provider: Arc<dyn TimeProvider>,
  lifecycle_hooks: Option<Arc<dyn BrokerLifecycleHooks>>,
}

impl FlushContext {
  pub fn new(
    config: WriteConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    snowflake: SnowflakeGenerator,
    time_provider: Arc<dyn TimeProvider>,
    lifecycle_hooks: Option<Arc<dyn BrokerLifecycleHooks>>,
  ) -> Self {
    Self {
      config,
      blob_store,
      metadata_store,
      snowflake: Arc::new(snowflake),
      time_provider,
      lifecycle_hooks,
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
    key.push_str(&window.start.unix_timestamp().to_string());
    key.push('/');
    key.push_str(&snowflake_id.as_u64().to_string());
    key.push('.');
    key.push_str(extension);

    BlobKey::new(key)
  }

  fn encode_batch(virtual_partition_id: VirtualPartitionId, records: Vec<Record>) -> Result<Bytes> {
    let proto_batch = StoredRecordBatch {
      virtual_partition_id,
      records,
      ..Default::default()
    };
    proto_batch
      .write_to_bytes()
      .map(Bytes::from)
      .context("encode record batch")
  }

  fn compress_batch(&self, payload: Bytes) -> Result<Bytes> {
    match self.config.compression.codec {
      CompressionCodec::None => Ok(payload),
      CompressionCodec::Zstd => {
        let level = self.config.compression.level.unwrap_or(DEFAULT_ZSTD_LEVEL);
        let compressed =
          zstd::stream::encode_all(Cursor::new(payload), level).context("compress batch")?;
        Ok(Bytes::from(compressed))
      },
    }
  }

  fn merge_partition_batches(
    partition: FlushPartition,
  ) -> Result<(VirtualPartitionId, Vec<Record>, BatchSummary, SeqRange)> {
    let virtual_partition_id = partition.virtual_partition_id;
    let record_capacity = partition
      .batches
      .iter()
      .map(|batch| batch.records.len())
      .sum::<usize>();
    let mut batches = partition.batches.into_iter();
    let Some(first_batch) = batches.next() else {
      return Err(anyhow::anyhow!(
        "flush partition {virtual_partition_id} has no buffered batches"
      ));
    };
    let BufferedBatch {
      mut records,
      mut summary,
      mut seq_range,
      ..
    } = first_batch;
    records.reserve(record_capacity.saturating_sub(records.len()));

    // Broker allocation is consecutive within a partition. Rejecting a gap prevents metadata from
    // advertising a sequence span that was never persisted.
    for batch in batches {
      let expected_start = seq_range
        .end
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("sequence range ends at u64::MAX"))?;
      anyhow::ensure!(
        batch.seq_range.start == expected_start,
        "flush partition {virtual_partition_id} has noncontiguous ranges: expected start \
         {expected_start}, found {}",
        batch.seq_range.start
      );
      seq_range.end = batch.seq_range.end;
      summary.record_count = summary
        .record_count
        .checked_add(batch.summary.record_count)
        .ok_or_else(|| anyhow::anyhow!("merged record count exceeds u32"))?;
      summary.payload_bytes = summary
        .payload_bytes
        .checked_add(batch.summary.payload_bytes)
        .ok_or_else(|| anyhow::anyhow!("merged payload bytes exceed u64"))?;
      records.extend(batch.records);
    }

    Ok((virtual_partition_id, records, summary, seq_range))
  }

  fn build_segment(
    &self,
    topic: &str,
    partitions: Vec<FlushPartition>,
    metadata_window_size: time::Duration,
    now: OffsetDateTime,
  ) -> Result<(Bytes, SegmentEnvelope)> {
    trace!("building flush segment: topic={topic}");
    let window = Window::for_timestamp(now, metadata_window_size);
    let snowflake_id = self.snowflake.next(now)?;
    let blob_key = self.make_blob_key(topic, &window, snowflake_id);
    let compression = self.config.compression.clone();

    let mut payload = BytesMut::new();
    let mut segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>> = HashMap::new();
    let mut record_count = 0_u64;
    for partition in partitions {
      trace!(
        "encoding partition batches: topic={}, virtual_partition_id={}, batches={}",
        topic,
        partition.virtual_partition_id,
        partition.batches.len()
      );
      let (virtual_partition_id, records, summary, seq_range) =
        Self::merge_partition_batches(partition)?;
      let encoded = Self::encode_batch(virtual_partition_id, records)?;
      let compressed = self.compress_batch(encoded)?;
      let start = payload.len() as u64;
      payload.extend_from_slice(&compressed);
      let end = payload.len() as u64;

      record_count = record_count.saturating_add(u64::from(summary.record_count));
      let metadata = BatchMetadata {
        seq_range,
        byte_range: blob_stream_types::ByteRange { start, end },
        payload_bytes: summary.payload_bytes,
      };

      segment_index.insert(virtual_partition_id, vec![metadata]);
    }

    let envelope = SegmentEnvelope {
      window: window.key(topic),
      snowflake_id,
      blob_key,
      compression,
      segment_index,
      record_count,
      created_at: now,
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
    metrics: &WriteMetrics,
  ) -> Result<(), WriteError> {
    let publication_started_at = Instant::now();
    let partitions = std::mem::take(&mut plan.partitions);
    let virtual_partition_ids = partitions
      .iter()
      .map(|partition| partition.virtual_partition_id)
      .collect::<Vec<_>>();
    let fences = if plan.fenced_metadata_writes {
      Some(
        partitions
          .iter()
          .map(|partition| {
            let fence = partition.lease_fence.as_deref().cloned().ok_or_else(|| {
              anyhow::anyhow!("fenced metadata publication requires a durable producer lease fence")
            })?;
            Ok(ProducerPartitionFence {
              key: ProducerPartitionLeaseKey {
                topic: plan.topic.clone(),
                virtual_partition_id: partition.virtual_partition_id,
              },
              fence,
            })
          })
          .collect::<Result<Vec<_>, anyhow::Error>>()?,
      )
    } else {
      None
    };
    let (payload, envelope) = self.build_segment(
      plan.topic.as_str(),
      partitions,
      plan.metadata_window_size,
      now,
    )?;
    let payload_bytes = payload.len();
    let record_count = envelope.record_count;
    let partition_count = envelope.segment_index.len();

    let publication_budget = std::time::Duration::try_from(plan.max_metadata_publication_lag)
      .map_err(|_| {
        WriteError::Internal(anyhow::anyhow!(
          "metadata publication deadline must not be negative"
        ))
      })?;
    let remaining_budget = publication_budget
      .checked_sub(publication_started_at.elapsed())
      .ok_or_else(|| {
        metrics.record_metadata_publication_latency(publication_started_at);
        metrics.record_metadata_publication_deadline_exhausted_before_persistence();
        anyhow::anyhow!(
          "metadata publication exceeded {} ms before segment persistence",
          plan.max_metadata_publication_lag.whole_milliseconds()
        )
      })?;
    let persistence_result = timeout(remaining_budget, async {
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .before_flush_persist(plan.topic.as_str(), &virtual_partition_ids)
          .await;
      }
      self
        .blob_store
        .put(&envelope.blob_key, payload)
        .await
        .map_err(|error| MetadataWriteError::Other(error.context("write segment blob")))?;
      metrics.record_uploaded_object(payload_bytes);
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .blob_persisted(plan.topic.as_str(), &virtual_partition_ids)
          .await;
      }
      let metadata_published_at = self.time_provider.now();
      let metadata = envelope.into_metadata(metadata_published_at);
      self
        .metadata_store
        .write_segment(
          metadata,
          fences.as_deref(),
          metadata_published_at.unix_timestamp_ms(),
        )
        .await?;
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .metadata_persisted(plan.topic.as_str(), &virtual_partition_ids)
          .await;
      }
      Ok::<(), MetadataWriteError>(())
    })
    .await;
    metrics.record_metadata_publication_latency(publication_started_at);
    match persistence_result {
      Ok(Ok(())) => {},
      Ok(Err(MetadataWriteError::ProducerLeaseFenceLost)) => {
        return Err(WriteError::LeaseFenceLost);
      },
      Ok(Err(error)) => {
        return Err(anyhow::Error::new(error).context("persist segment").into());
      },
      Err(_elapsed) => {
        metrics.record_metadata_publication_deadline_exhausted_while_persisting();
        return Err(
          anyhow::anyhow!(
            "metadata publication exceeded {} ms while persisting segment",
            plan.max_metadata_publication_lag.whole_milliseconds()
          )
          .into(),
        );
      },
    }

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
