use super::buffer::{
  BufferedBatch,
  FlushCompletionError,
  FlushPartition,
  FlushPartitionResult,
  FlushPlan,
  TopicFlushPlan,
};
use super::metrics::WriteMetrics;
use super::{BrokerLifecycleHooks, DEFAULT_ZSTD_LEVEL, WriteConfig, WriteError};
use anyhow::{Context, Result};
use bd_log_util::warn_every;
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
use futures::{StreamExt, TryStreamExt};
use log::{debug, trace};
use protobuf::Message;
use sonyflake::Sonyflake;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use tokio::time::timeout;

const MAX_CONCURRENT_METADATA_WRITES: usize = 8;

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
  created_at: OffsetDateTime,
}

struct PersistedTopic {
  envelope: SegmentEnvelope,
  fences: Option<Vec<ProducerPartitionFence>>,
  max_metadata_publication_lag: time::Duration,
}

struct PersistedObject {
  payload: Bytes,
  topics: Vec<PersistedTopic>,
  shared_blob: bool,
  oversized_singleton: bool,
}

impl PersistedObject {
  fn failure_results(
    topics: &[PersistedTopic],
    error: FlushCompletionError,
  ) -> Vec<FlushPartitionResult> {
    topics
      .iter()
      .flat_map(|topic| {
        topic
          .envelope
          .segment_index
          .keys()
          .map(|virtual_partition_id| FlushPartitionResult {
            topic: topic.envelope.window.topic.clone().into(),
            virtual_partition_id: *virtual_partition_id,
            error: Some(error),
          })
      })
      .collect()
  }
}

struct EncodedPartition {
  topic: protobuf::Chars,
  virtual_partition_id: VirtualPartitionId,
  payload: Bytes,
  metadata: BatchMetadata,
  fence: Option<ProducerPartitionFence>,
  max_metadata_publication_lag: time::Duration,
  metadata_window_size: time::Duration,
}

struct ObjectTopicBuilder {
  topic: protobuf::Chars,
  max_metadata_publication_lag: time::Duration,
  metadata_window_size: time::Duration,
  segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
  fences: Option<Vec<ProducerPartitionFence>>,
}

struct ObjectBuilder {
  payload: BytesMut,
  topics: Vec<ObjectTopicBuilder>,
}

impl ObjectBuilder {
  fn new() -> Self {
    Self {
      payload: BytesMut::new(),
      topics: Vec::new(),
    }
  }

  fn is_empty(&self) -> bool {
    self.payload.is_empty()
  }

  fn would_exceed(&self, encoded: &EncodedPartition, max_segment_bytes: u64) -> bool {
    u64::try_from(self.payload.len())
      .unwrap_or(u64::MAX)
      .saturating_add(u64::try_from(encoded.payload.len()).unwrap_or(u64::MAX))
      > max_segment_bytes
  }

  fn push(&mut self, encoded: EncodedPartition) {
    let start = u64::try_from(self.payload.len()).unwrap_or(u64::MAX);
    self.payload.extend_from_slice(&encoded.payload);
    let end = u64::try_from(self.payload.len()).unwrap_or(u64::MAX);
    let metadata = BatchMetadata {
      byte_range: blob_stream_types::ByteRange { start, end },
      ..encoded.metadata
    };
    let topic = if let Some(topic) = self
      .topics
      .iter_mut()
      .find(|topic| topic.topic == encoded.topic)
    {
      topic
    } else {
      self.topics.push(ObjectTopicBuilder {
        topic: encoded.topic.clone(),
        max_metadata_publication_lag: encoded.max_metadata_publication_lag,
        metadata_window_size: encoded.metadata_window_size,
        segment_index: HashMap::new(),
        fences: encoded.fence.as_ref().map(|_| Vec::new()),
      });
      self
        .topics
        .last_mut()
        .expect("new topic section is present")
    };
    topic
      .segment_index
      .insert(encoded.virtual_partition_id, vec![metadata]);
    if let Some(fence) = encoded.fence {
      topic
        .fences
        .as_mut()
        .expect("fenced partition has a fenced topic section")
        .push(fence);
    }
  }

  fn finish(
    self,
    context: &FlushContext,
    now: OffsetDateTime,
    shared_blob: bool,
    max_segment_bytes: u64,
  ) -> Result<PersistedObject> {
    let Self { payload, topics } = self;
    let snowflake_id = context.snowflake.next(now)?;
    let topic = topics.first().expect("nonempty object has a topic section");
    let window = Window::for_timestamp(now, topic.metadata_window_size);
    let blob_key = if shared_blob || topics.len() > 1 {
      context.make_shared_blob_key(&window, snowflake_id)
    } else {
      context.make_blob_key(topic.topic.as_str(), &window, snowflake_id)
    };
    let compression = context.config.compression.clone();
    let topics = topics
      .into_iter()
      .map(|topic| {
        let window = Window::for_timestamp(now, topic.metadata_window_size);
        PersistedTopic {
          envelope: SegmentEnvelope {
            window: window.key(topic.topic.as_str()),
            snowflake_id,
            blob_key: blob_key.clone(),
            compression: compression.clone(),
            segment_index: topic.segment_index,
            created_at: now,
          },
          fences: topic.fences,
          max_metadata_publication_lag: topic.max_metadata_publication_lag,
        }
      })
      .collect();
    let payload_len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    Ok(PersistedObject {
      payload: payload.freeze(),
      topics,
      shared_blob,
      oversized_singleton: payload_len > max_segment_bytes,
    })
  }
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

  fn make_blob_key(&self, namespace: &str, window: &Window, snowflake_id: SnowflakeId) -> BlobKey {
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

    key.push_str(namespace);
    key.push('/');
    key.push_str(&window.start.unix_timestamp().to_string());
    key.push('/');
    key.push_str(&snowflake_id.as_u64().to_string());
    key.push('.');
    key.push_str(extension);

    BlobKey::new(key)
  }

  fn make_shared_blob_key(&self, window: &Window, snowflake_id: SnowflakeId) -> BlobKey {
    self.make_blob_key("shared", window, snowflake_id)
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

  fn encode_partition(
    &self,
    topic_plan: &TopicFlushPlan,
    partition: FlushPartition,
  ) -> Result<EncodedPartition> {
    trace!(
      "encoding partition batches: topic={}, virtual_partition_id={}, batches={}",
      topic_plan.topic,
      partition.virtual_partition_id,
      partition.batches.len()
    );
    let virtual_partition_id = partition.virtual_partition_id;
    let fence = if topic_plan.fenced_metadata_writes {
      let fence = partition.lease_fence.as_deref().cloned().ok_or_else(|| {
        anyhow::anyhow!("fenced metadata publication requires a durable producer lease fence")
      })?;
      Some(ProducerPartitionFence {
        key: ProducerPartitionLeaseKey {
          topic: topic_plan.topic.clone(),
          virtual_partition_id,
        },
        fence,
      })
    } else {
      None
    };
    let (virtual_partition_id, records, summary, seq_range) =
      Self::merge_partition_batches(partition)?;
    let payload = self.compress_batch(Self::encode_batch(virtual_partition_id, records)?)?;
    Ok(EncodedPartition {
      topic: topic_plan.topic.clone(),
      virtual_partition_id,
      payload,
      metadata: BatchMetadata {
        seq_range,
        byte_range: blob_stream_types::ByteRange { start: 0, end: 0 },
        payload_bytes: summary.payload_bytes,
      },
      fence,
      max_metadata_publication_lag: topic_plan.max_metadata_publication_lag,
      metadata_window_size: topic_plan.metadata_window_size,
    })
  }

  fn build_objects(
    &self,
    plan: &mut FlushPlan,
    now: OffsetDateTime,
  ) -> Result<Vec<PersistedObject>> {
    let topic_plans = std::mem::take(&mut plan.topics);
    let mut current = ObjectBuilder::new();
    let mut objects = Vec::new();
    for mut topic_plan in topic_plans {
      for partition in std::mem::take(&mut topic_plan.partitions) {
        let encoded = self.encode_partition(&topic_plan, partition)?;
        if !current.is_empty() && current.would_exceed(&encoded, plan.max_segment_bytes) {
          objects.push(current.finish(self, now, plan.shared_blob, plan.max_segment_bytes)?);
          current = ObjectBuilder::new();
        }
        current.push(encoded);
      }
    }
    if !current.is_empty() {
      objects.push(current.finish(self, now, plan.shared_blob, plan.max_segment_bytes)?);
    }
    Ok(objects)
  }

  async fn persist_topic_metadata(
    &self,
    topic: PersistedTopic,
    publication_started_at: Instant,
    metrics: &WriteMetrics,
  ) -> Result<Vec<FlushPartitionResult>, WriteError> {
    let virtual_partition_ids = topic
      .envelope
      .segment_index
      .keys()
      .copied()
      .collect::<Vec<_>>();
    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .blob_persisted(topic.envelope.window.topic.as_str(), &virtual_partition_ids)
        .await;
    }
    let topic_name = topic.envelope.window.topic.clone();
    let metadata_publication_budget =
      std::time::Duration::try_from(topic.max_metadata_publication_lag).map_err(|_| {
        WriteError::Internal(anyhow::anyhow!(
          "metadata publication deadline must not be negative"
        ))
      })?;
    let metadata_remaining_budget =
      metadata_publication_budget.checked_sub(publication_started_at.elapsed());
    let metadata_published_at = self.time_provider.now();
    let metadata = topic.envelope.into_metadata(metadata_published_at);
    let error = match metadata_remaining_budget {
      None => {
        metrics.record_metadata_publication_deadline_exhausted_before_persistence();
        Some(FlushCompletionError::Internal)
      },
      Some(remaining_budget) => match timeout(
        remaining_budget,
        self.metadata_store.write_segment(
          metadata,
          topic.fences.as_deref(),
          metadata_published_at.unix_timestamp_ms(),
        ),
      )
      .await
      {
        Ok(Ok(())) => None,
        Ok(Err(MetadataWriteError::ProducerLeaseFenceLost)) => {
          Some(FlushCompletionError::LeaseFenceLost)
        },
        Ok(Err(_)) => Some(FlushCompletionError::Internal),
        Err(_) => {
          metrics.record_metadata_publication_deadline_exhausted_while_persisting();
          Some(FlushCompletionError::Internal)
        },
      },
    };
    if error.is_none()
      && let Some(lifecycle_hooks) = &self.lifecycle_hooks
    {
      lifecycle_hooks
        .metadata_persisted(topic_name.as_str(), &virtual_partition_ids)
        .await;
    }
    Ok(
      virtual_partition_ids
        .into_iter()
        .map(|virtual_partition_id| FlushPartitionResult {
          topic: topic_name.clone().into(),
          virtual_partition_id,
          error,
        })
        .collect(),
    )
  }

  pub(super) async fn flush_plan(
    &self,
    plan: &mut FlushPlan,
    now: OffsetDateTime,
    metrics: &WriteMetrics,
  ) -> Result<Vec<FlushPartitionResult>, WriteError> {
    let mut objects = self.build_objects(plan, now)?.into_iter();
    let publication_started_at = Instant::now();
    let mut results = Vec::new();
    while let Some(object) = objects.next() {
      let publication_budget = object
        .topics
        .iter()
        .map(|topic| topic.max_metadata_publication_lag)
        .min()
        .expect("persisted object has at least one topic");
      let publication_budget = std::time::Duration::try_from(publication_budget).map_err(|_| {
        WriteError::Internal(anyhow::anyhow!(
          "metadata publication deadline must not be negative"
        ))
      })?;
      let Some(remaining_budget) = publication_budget.checked_sub(publication_started_at.elapsed())
      else {
        metrics.record_metadata_publication_deadline_exhausted_before_persistence();
        results.extend(PersistedObject::failure_results(
          &object.topics,
          FlushCompletionError::Internal,
        ));
        for object in objects {
          results.extend(PersistedObject::failure_results(
            &object.topics,
            FlushCompletionError::Internal,
          ));
        }
        break;
      };
      for topic in &object.topics {
        if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
          lifecycle_hooks
            .before_flush_persist(
              topic.envelope.window.topic.as_str(),
              &topic
                .envelope
                .segment_index
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            )
            .await;
        }
      }
      let payload_bytes = object.payload.len();
      let blob_key = object
        .topics
        .first()
        .expect("persisted object has at least one topic")
        .envelope
        .blob_key
        .clone();
      let blob_result = timeout(
        remaining_budget,
        self.blob_store.put(&blob_key, object.payload),
      )
      .await;
      if blob_result.is_err() || blob_result.as_ref().is_ok_and(Result::is_err) {
        if blob_result.is_err() {
          metrics.record_metadata_publication_deadline_exhausted_while_persisting();
        }
        results.extend(PersistedObject::failure_results(
          &object.topics,
          FlushCompletionError::Internal,
        ));
        continue;
      }
      metrics.record_uploaded_object(payload_bytes, object.oversized_singleton);
      if object.oversized_singleton {
        let topic = object
          .topics
          .first()
          .expect("oversized object has one topic section");
        let virtual_partition_id = topic
          .envelope
          .segment_index
          .keys()
          .next()
          .expect("oversized object has one partition");
        warn_every!(
          15.seconds(),
          "broker persisted oversized single-partition object: topic={}, \
           virtual_partition_id={virtual_partition_id}, payload_bytes={payload_bytes}, \
           max_segment_bytes={}",
          topic.envelope.window.topic,
          plan.max_segment_bytes
        );
      }
      debug!(
        "persisted segment object: blob_key={}, payload_bytes={payload_bytes}, topics={}, \
         shared={}, oversized_singleton={}",
        blob_key.as_str(),
        object.topics.len(),
        object.shared_blob,
        object.oversized_singleton
      );
      // Metadata rows determine acknowledgement independently, even when their data shares a
      // single uploaded object. Bound the fan-out so a wide shared flush cannot overwhelm storage.
      let topic_results = futures::stream::iter(
        object
          .topics
          .into_iter()
          .map(|topic| self.persist_topic_metadata(topic, publication_started_at, metrics)),
      )
      .buffer_unordered(MAX_CONCURRENT_METADATA_WRITES)
      .try_collect::<Vec<_>>()
      .await?;
      results.extend(topic_results.into_iter().flatten());
    }
    results.sort_by(|left, right| {
      left
        .topic
        .as_str()
        .cmp(right.topic.as_str())
        .then_with(|| left.virtual_partition_id.cmp(&right.virtual_partition_id))
    });
    metrics.record_metadata_publication_latency(publication_started_at);
    debug!("flush persisted {} bounded segment objects", results.len());
    Ok(results)
  }

  #[must_use]
  pub(super) fn config(&self) -> &WriteConfig {
    &self.config
  }
}
