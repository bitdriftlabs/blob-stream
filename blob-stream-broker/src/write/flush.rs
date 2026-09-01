use super::buffer::{
  BufferedBatch,
  FlushCompletionError,
  FlushPartition,
  FlushPartitionResult,
  FlushPlan,
  FlushPublicationDependency,
  FlushPublicationResult,
  FlushPublicationState,
  TopicFlushPlan,
};
use super::metrics::WriteMetrics;
use super::{BrokerLifecycleHooks, DEFAULT_ZSTD_LEVEL, WriteConfig, WriteError};
use anyhow::{Context, Result};
use bd_log_util::warn_every;
use bd_time::{OffsetDateTimeExt, TimeProvider};
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
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{debug, trace};
use protobuf::Message;
use sonyflake::Sonyflake;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use time::ext::NumericalDuration;
use time::{Duration, OffsetDateTime};
use tokio::sync::Semaphore;
use tokio::time::{Instant, timeout, timeout_at};

pub(super) const MAX_CONCURRENT_METADATA_WRITES: usize = 8;

#[cfg(test)]
#[path = "./flush_test.rs"]
mod tests;

//
// SnowflakeGenerator
//

pub(super) struct SnowflakeGenerator {
  generator: Sonyflake,
}

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

  async fn next(&self, time_provider: &dyn TimeProvider) -> Result<(SnowflakeId, OffsetDateTime)> {
    loop {
      let now = time_provider.now();
      match self.generator.next_id(now) {
        Ok(value) => return Ok((SnowflakeId(value), now)),
        // A bounded flush can contain more than Sonyflake's 512 IDs per 10 ms bucket. Wait for
        // the next real bucket rather than synthesizing a future ID timestamp, which could move
        // consumer-visible ordering ahead of the clock.
        Err(sonyflake::Error::OverSequenceLimit) => {
          time_provider.sleep(Duration::milliseconds(10)).await;
        },
        Err(error) => return Err(error).context("generate sonyflake id"),
      }
    }
  }
}

//
// SegmentEnvelope
//

#[derive(Clone, Debug)]
struct SegmentEnvelope {
  window: TopicWindowKey,
  snowflake_id: SnowflakeId,
  blob_key: BlobKey,
  compression: blob_stream_types::Compression,
  created_at: OffsetDateTime,
}

struct PersistedTopic {
  envelope: SegmentEnvelope,
  partitions: HashMap<VirtualPartitionId, PersistedPartition>,
  max_metadata_publication_lag: time::Duration,
}

struct PersistedObject {
  blob_key: BlobKey,
  payload: Bytes,
  topics: Vec<PersistedTopic>,
  publication_budget: time::Duration,
  oversized_partition: Option<(String, VirtualPartitionId)>,
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
          .partitions
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
  publication_predecessor: Option<FlushPublicationDependency>,
  max_metadata_publication_lag: time::Duration,
  metadata_window_size: time::Duration,
}

struct PartitionPublicationState {
  fence: Option<ProducerPartitionFence>,
  publication_predecessor: Option<FlushPublicationDependency>,
}

struct PersistedPartition {
  metadata: Vec<BatchMetadata>,
  publication_state: PartitionPublicationState,
}

struct ObjectTopicBuilder {
  topic: protobuf::Chars,
  max_metadata_publication_lag: time::Duration,
  metadata_window_size: time::Duration,
  partitions: HashMap<VirtualPartitionId, PersistedPartition>,
}

struct ObjectBuilder {
  payload: BytesMut,
  topics: Vec<ObjectTopicBuilder>,
  blob_window_size: time::Duration,
  oversized_partition: Option<(String, VirtualPartitionId)>,
}

impl ObjectBuilder {
  fn new(encoded: EncodedPartition) -> Self {
    let mut payload = BytesMut::new();
    let (mut topic_builder, virtual_partition_id, partition) =
      Self::build_partition(&mut payload, encoded);
    topic_builder.insert_partition(virtual_partition_id, partition);
    Self {
      payload,
      blob_window_size: topic_builder.metadata_window_size,
      oversized_partition: Some((topic_builder.topic.to_string(), virtual_partition_id)),
      topics: vec![topic_builder],
    }
  }

  fn would_exceed(&self, encoded: &EncodedPartition, max_segment_bytes: u64) -> bool {
    u64::try_from(self.payload.len())
      .unwrap_or(u64::MAX)
      .saturating_add(u64::try_from(encoded.payload.len()).unwrap_or(u64::MAX))
      > max_segment_bytes
  }

  fn push(mut self, encoded: EncodedPartition) -> Self {
    let (topic_builder, virtual_partition_id, partition) =
      Self::build_partition(&mut self.payload, encoded);
    self.oversized_partition = None;
    if let Some(existing_topic) = self
      .topics
      .iter_mut()
      .find(|existing_topic| existing_topic.topic == topic_builder.topic)
    {
      existing_topic.insert_partition(virtual_partition_id, partition);
    } else {
      let mut topic_builder = topic_builder;
      topic_builder.insert_partition(virtual_partition_id, partition);
      self.topics.push(topic_builder);
    }
    self
  }

  fn build_partition(
    object_payload: &mut BytesMut,
    encoded: EncodedPartition,
  ) -> (ObjectTopicBuilder, VirtualPartitionId, PersistedPartition) {
    let EncodedPartition {
      topic,
      virtual_partition_id,
      payload: encoded_payload,
      metadata: encoded_metadata,
      fence,
      publication_predecessor,
      max_metadata_publication_lag,
      metadata_window_size,
    } = encoded;
    let start = u64::try_from(object_payload.len()).unwrap_or(u64::MAX);
    object_payload.extend_from_slice(&encoded_payload);
    let end = u64::try_from(object_payload.len()).unwrap_or(u64::MAX);
    let metadata = BatchMetadata {
      byte_range: blob_stream_types::ByteRange { start, end },
      ..encoded_metadata
    };
    let partition = PersistedPartition {
      metadata: vec![metadata],
      publication_state: PartitionPublicationState {
        fence,
        publication_predecessor,
      },
    };
    let topic_builder = ObjectTopicBuilder {
      topic,
      max_metadata_publication_lag,
      metadata_window_size,
      partitions: HashMap::new(),
    };
    (topic_builder, virtual_partition_id, partition)
  }

  async fn finish(self, context: &FlushContext, max_segment_bytes: u64) -> Result<PersistedObject> {
    let Self {
      payload,
      topics,
      blob_window_size,
      oversized_partition,
    } = self;
    let (snowflake_id, now) = context
      .snowflake
      .next(context.time_provider.as_ref())
      .await?;
    let publication_budget = topics
      .iter()
      .map(|topic| topic.max_metadata_publication_lag)
      .min()
      .unwrap_or_else(|| unreachable!("object builders always contain an initial partition"));
    let window = Window::for_timestamp(now, blob_window_size);
    let blob_key = context.make_blob_key(&window, snowflake_id);
    let compression = context.config.compression.clone();
    let oversized_partition =
      if u64::try_from(payload.len()).unwrap_or(u64::MAX) > max_segment_bytes {
        debug_assert!(
          oversized_partition.is_some(),
          "oversized segment object must contain one partition"
        );
        oversized_partition
      } else {
        None
      };
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
            created_at: now,
          },
          partitions: topic.partitions,
          max_metadata_publication_lag: topic.max_metadata_publication_lag,
        }
      })
      .collect::<Vec<_>>();
    Ok(PersistedObject {
      blob_key,
      payload: payload.freeze(),
      topics,
      publication_budget,
      oversized_partition,
    })
  }
}

impl SegmentEnvelope {
  fn into_metadata(
    self,
    segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
    metadata_published_at: OffsetDateTime,
  ) -> blob_stream_metadata_store::SegmentMetadata {
    blob_stream_metadata_store::SegmentMetadata::new(
      self.window,
      self.snowflake_id,
      self.blob_key,
      self.compression,
      segment_index,
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

  fn make_blob_key(&self, window: &Window, snowflake_id: SnowflakeId) -> BlobKey {
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

    key.push_str("shared");
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
    let publication_predecessor = partition.publication_predecessor.clone();
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
      publication_predecessor,
      max_metadata_publication_lag: topic_plan.max_metadata_publication_lag,
      metadata_window_size: topic_plan.metadata_window_size,
    })
  }

  async fn build_objects(
    &self,
    plan: &mut FlushPlan,
    metrics: &WriteMetrics,
  ) -> Result<(Vec<PersistedObject>, Vec<FlushPartitionResult>)> {
    let topic_plans = std::mem::take(&mut plan.topics);
    let mut current: Option<ObjectBuilder> = None;
    let mut objects = Vec::new();
    let mut failed_partitions = Vec::new();
    for mut topic_plan in topic_plans {
      for partition in std::mem::take(&mut topic_plan.partitions) {
        let virtual_partition_id = partition.virtual_partition_id;
        if let Some(error) =
          wait_for_segment_identity(partition.publication_predecessor.clone()).await
        {
          // The scheduler leaves pending predecessors buffered, so this immediate failure is
          // local to a predecessor that already reached a terminal failed state. Keep unrelated
          // partitions in this plan independent rather than abandoning their durable work.
          failed_partitions.push(FlushPartitionResult {
            topic: topic_plan.topic.clone(),
            virtual_partition_id,
            error: Some(error),
          });
          continue;
        }
        let encoded = self.encode_partition(&topic_plan, partition)?;
        current = Some(match current {
          Some(current) if current.would_exceed(&encoded, plan.max_segment_bytes) => {
            metrics.flush_max_segment_size_splits_total.inc();
            objects.push(current.finish(self, plan.max_segment_bytes).await?);
            ObjectBuilder::new(encoded)
          },
          Some(current) => current.push(encoded),
          None => ObjectBuilder::new(encoded),
        });
      }
    }
    if let Some(current) = current {
      objects.push(current.finish(self, plan.max_segment_bytes).await?);
    }
    Ok((objects, failed_partitions))
  }

  async fn persist_topic_metadata(
    &self,
    topic: PersistedTopic,
    publication_started_at: Instant,
    metrics: &WriteMetrics,
    metadata_write_permits: &Arc<Semaphore>,
  ) -> Result<Vec<FlushPartitionResult>, WriteError> {
    // A normal topic can become one metadata row immediately. A topic with ordered predecessor
    // dependencies is split below so each virtual partition can wait independently; successful
    // partitions are merged again before the store write because they share one snowflake key.
    if topic.partitions.values().all(|partition| {
      partition
        .publication_state
        .publication_predecessor
        .is_none()
    }) {
      return self
        .persist_ready_topic_metadata_with_permit(
          topic,
          publication_started_at,
          metrics,
          metadata_write_permits,
        )
        .await;
    }

    let mut results = Vec::new();
    let mut ready_topics = Vec::new();
    let mut pending_topics = Vec::new();
    for (topic, mut predecessor) in topic.into_partition_topics() {
      // A completed predecessor determines the outcome immediately. Pending predecessors stay
      // separate so an unrelated partition in the same object does not inherit their failure.
      match predecessor
        .as_mut()
        .map(|predecessor| *predecessor.state_rx.borrow_and_update())
      {
        None => ready_topics.push(topic),
        Some(FlushPublicationState::Completed(FlushPublicationResult::Succeeded)) => {
          ready_topics.push(topic);
        },
        Some(FlushPublicationState::Completed(FlushPublicationResult::Failed(error))) => {
          results.extend(PersistedObject::failure_results(
            std::slice::from_ref(&topic),
            error,
          ));
        },
        Some(FlushPublicationState::Pending | FlushPublicationState::SegmentIdentityAssigned) => {
          pending_topics.push((topic, predecessor));
        },
      }
    }
    // Wait for every blocked partition concurrently. These waits do not use metadata capacity:
    // the shared semaphore is acquired only immediately around the metadata-store write.
    let pending_results = futures::stream::iter(pending_topics)
      .map(|(topic, predecessor)| async move {
        if let Some(error) = wait_for_predecessor(predecessor).await {
          return Ok::<_, WriteError>((topic, Some(error)));
        }
        Ok::<_, WriteError>((topic, None))
      })
      .buffer_unordered(MAX_CONCURRENT_METADATA_WRITES)
      .try_collect::<Vec<_>>()
      .await?;
    for (topic, error) in pending_results {
      if let Some(error) = error {
        results.extend(PersistedObject::failure_results(
          std::slice::from_ref(&topic),
          error,
        ));
      } else {
        ready_topics.push(topic);
      }
    }
    // Splitting preserves dependency isolation, while merging preserves the storage invariant
    // that all partitions for this topic/object share its one snowflake metadata key.
    if let Some(topic) = PersistedTopic::merge_partition_topics(ready_topics) {
      results.extend(
        self
          .persist_ready_topic_metadata_with_permit(
            topic,
            publication_started_at,
            metrics,
            metadata_write_permits,
          )
          .await?,
      );
    }
    Ok(results)
  }

  async fn persist_ready_topic_metadata_with_permit(
    &self,
    topic: PersistedTopic,
    publication_started_at: Instant,
    metrics: &WriteMetrics,
    metadata_write_permits: &Arc<Semaphore>,
  ) -> Result<Vec<FlushPartitionResult>, WriteError> {
    // This semaphore belongs to the engine's flush loop, not this plan. Concurrent plans queue
    // here together, making the limit a broker-wide cap rather than a cap per plan or object.
    let _metadata_write_permit = metadata_write_permits.acquire().await.map_err(|error| {
      WriteError::Internal(anyhow::anyhow!("metadata write semaphore closed: {error}"))
    })?;
    let PersistedTopic {
      envelope,
      partitions,
      max_metadata_publication_lag,
    } = topic;
    let metadata_publication_budget = std::time::Duration::try_from(max_metadata_publication_lag)
      .map_err(|_| {
      WriteError::Internal(anyhow::anyhow!(
        "metadata publication deadline must not be negative"
      ))
    })?;
    let metadata_remaining_budget =
      metadata_publication_budget.checked_sub(publication_started_at.elapsed());
    let metadata_published_at = self.time_provider.now();
    let topic_name = envelope.window.topic.clone();
    let mut virtual_partition_ids = Vec::with_capacity(partitions.len());
    let mut segment_index = HashMap::with_capacity(partitions.len());
    let mut fences = Vec::with_capacity(partitions.len());
    for (virtual_partition_id, partition) in partitions {
      virtual_partition_ids.push(virtual_partition_id);
      segment_index.insert(virtual_partition_id, partition.metadata);
      fences.push(partition.publication_state.fence);
    }
    let metadata = envelope.into_metadata(segment_index, metadata_published_at);
    let fences = fences.into_iter().collect::<Option<Vec<_>>>();
    let error = match metadata_remaining_budget {
      None => {
        metrics.record_metadata_publication_deadline_exhausted_before_persistence();
        Some(FlushCompletionError::Internal)
      },
      Some(remaining_budget) => match timeout(
        remaining_budget,
        self.metadata_store.write_segment(
          metadata,
          fences.as_deref(),
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

  pub(super) async fn flush_plan_after(
    &self,
    plan: &mut FlushPlan,
    metrics: &WriteMetrics,
    blob_upload_permits: &Arc<Semaphore>,
    metadata_write_permits: &Arc<Semaphore>,
    flush_notifier: &Arc<tokio::sync::Notify>,
  ) -> Result<Vec<FlushPartitionResult>, WriteError> {
    let publication_started_at = Instant::now();
    let (objects, mut results) = match self.build_objects(plan, metrics).await {
      Ok(objects) => objects,
      Err(error) => {
        mark_publication_complete(
          &plan.publication_completions,
          FlushPublicationResult::Failed(FlushCompletionError::Internal),
        );
        return Err(error.into());
      },
    };
    mark_segment_identity_assigned(&plan.publication_completions, &results);
    // A successor may have remained buffered solely for this identity barrier. Wake the flush
    // loop now, rather than waiting for this plan's potentially slower upload/metadata terminal
    // state, so unrelated ready work can continue.
    flush_notifier.notify_waiters();
    let mut blob_uploads = FuturesUnordered::new();
    for object in objects {
      let publication_budget =
        std::time::Duration::try_from(object.publication_budget).map_err(|_| {
          WriteError::Internal(anyhow::anyhow!(
            "metadata publication deadline must not be negative"
          ))
        })?;
      let deadline = publication_started_at + publication_budget;
      if Instant::now() >= deadline {
        metrics.record_metadata_publication_deadline_exhausted_before_persistence();
        results.extend(PersistedObject::failure_results(
          &object.topics,
          FlushCompletionError::Internal,
        ));
        continue;
      }
      for topic in &object.topics {
        if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
          lifecycle_hooks
            .before_flush_persist(
              topic.envelope.window.topic.as_str(),
              &topic.partitions.keys().copied().collect::<Vec<_>>(),
            )
            .await;
        }
      }
      if Instant::now() >= deadline {
        metrics.record_metadata_publication_deadline_exhausted_before_persistence();
        results.extend(PersistedObject::failure_results(
          &object.topics,
          FlushCompletionError::Internal,
        ));
        continue;
      }
      let blob_key = object.blob_key.clone();
      let blob_store = Arc::clone(&self.blob_store);
      let payload = object.payload.clone();
      let blob_upload_permits = Arc::clone(blob_upload_permits);
      // Register every upload before polling for a result. Object identities and payloads are
      // immutable at this point, so separate objects have no blob-store dependency. Metadata is
      // intentionally deferred until this entire phase finishes. The broker-wide semaphore
      // bounds the storage burst, and the absolute deadline covers both queueing and I/O.
      blob_uploads.push(async move {
        let result = timeout_at(deadline, async {
          let _blob_upload_permit = blob_upload_permits
            .acquire()
            .await
            .map_err(|error| anyhow::anyhow!("blob upload semaphore closed: {error}"))?;
          blob_store.put(&blob_key, payload).await
        })
        .await;
        (object, result)
      });
    }

    let mut uploaded_objects = Vec::new();
    while let Some((object, blob_result)) = blob_uploads.next().await {
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
      let payload_bytes = object.payload.len();
      let blob_key = &object.blob_key;
      metrics.record_uploaded_object(payload_bytes, object.oversized_partition.is_some());
      if let Some((topic, virtual_partition_id)) = &object.oversized_partition {
        warn_every!(
          15.seconds(),
          "broker persisted oversized single-partition object: topic={}, \
           virtual_partition_id={virtual_partition_id}, payload_bytes={payload_bytes}, \
           max_segment_bytes={}",
          topic,
          plan.max_segment_bytes
        );
      }
      debug!(
        "persisted shared segment object: blob_key={}, payload_bytes={payload_bytes}, topics={}, \
         oversized_singleton={}",
        blob_key.as_str(),
        object.topics.len(),
        object.oversized_partition.is_some()
      );
      for topic in &object.topics {
        if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
          let virtual_partition_ids = topic.partitions.keys().copied().collect::<Vec<_>>();
          lifecycle_hooks
            .blob_persisted(topic.envelope.window.topic.as_str(), &virtual_partition_ids)
            .await;
        }
      }
      uploaded_objects.push(object);
    }

    // Metadata publication begins only after every blob upload has reached a terminal outcome.
    // Every plan shares the engine-owned semaphore, so these futures may be queued together but
    // at most MAX_CONCURRENT_METADATA_WRITES calls reach the metadata store across the broker.
    let mut metadata_publications = FuturesUnordered::new();
    for object in uploaded_objects {
      metadata_publications.push(self.persist_object_metadata(
        object.topics,
        publication_started_at,
        metrics,
        Arc::clone(metadata_write_permits),
      ));
    }

    while let Some(publication) = metadata_publications.next().await {
      results.extend(publication?);
    }
    results.sort_by(|left, right| {
      left
        .topic
        .as_str()
        .cmp(right.topic.as_str())
        .then_with(|| left.virtual_partition_id.cmp(&right.virtual_partition_id))
    });
    debug!("flush completed {} partition results", results.len());
    Ok(results)
  }

  #[must_use]
  pub(super) fn config(&self) -> &WriteConfig {
    &self.config
  }
}

impl PersistedTopic {
  fn merge_partition_topics(mut topics: Vec<Self>) -> Option<Self> {
    // Partition topics are split only while waiting for independent predecessors. Rejoin every
    // successful partition before persistence so the shared object has one metadata record and
    // therefore one unambiguous snowflake key for this topic.
    let mut merged = topics.pop()?;
    for topic in topics {
      debug_assert_eq!(merged.envelope.window, topic.envelope.window);
      debug_assert_eq!(merged.envelope.snowflake_id, topic.envelope.snowflake_id);
      debug_assert_eq!(merged.envelope.blob_key, topic.envelope.blob_key);
      debug_assert!(merged.partitions.values().all(|partition| {
        partition
          .publication_state
          .publication_predecessor
          .is_none()
      }));
      debug_assert!(topic.partitions.values().all(|partition| {
        partition
          .publication_state
          .publication_predecessor
          .is_none()
      }));
      merged.partitions.extend(topic.partitions);
    }
    Some(merged)
  }

  fn into_partition_topics(self) -> Vec<(Self, Option<FlushPublicationDependency>)> {
    let Self {
      envelope,
      partitions,
      max_metadata_publication_lag,
    } = self;
    partitions
      .into_iter()
      .map(|(virtual_partition_id, mut partition)| {
        // Keep the fence and predecessor together while this partition waits. The small one-key
        // topic remains a valid metadata row if its predecessor succeeds, then merge restores
        // all ready partitions before the row is written.
        let predecessor = partition.publication_state.publication_predecessor.take();
        (
          Self {
            envelope: envelope.clone(),
            partitions: HashMap::from([(virtual_partition_id, partition)]),
            max_metadata_publication_lag,
          },
          predecessor,
        )
      })
      .collect()
  }
}

impl ObjectTopicBuilder {
  fn insert_partition(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    partition: PersistedPartition,
  ) {
    let replaced = self.partitions.insert(virtual_partition_id, partition);
    debug_assert!(replaced.is_none());
  }
}

impl FlushContext {
  async fn persist_object_metadata(
    &self,
    topics: Vec<PersistedTopic>,
    publication_started_at: Instant,
    metrics: &WriteMetrics,
    metadata_write_permits: Arc<Semaphore>,
  ) -> Result<Vec<FlushPartitionResult>, WriteError> {
    // Topic futures may wait on different predecessor epochs concurrently. This per-object bound
    // limits queued dependency work, while actual metadata-store calls still pass through the
    // shared broker-wide semaphore in persist_ready_topic_metadata.
    let topic_results = futures::stream::iter(topics.into_iter().map(|topic| {
      self.persist_topic_metadata(
        topic,
        publication_started_at,
        metrics,
        &metadata_write_permits,
      )
    }))
    .buffer_unordered(MAX_CONCURRENT_METADATA_WRITES)
    .try_collect::<Vec<_>>()
    .await?;
    Ok(topic_results.into_iter().flatten().collect())
  }
}

/// Release successor identity assignment after every object in this epoch has a fixed identity.
/// Metadata publication is deliberately not complete yet, so successors still wait before they
/// make their own metadata visible.
fn mark_segment_identity_assigned(
  completions: &[super::buffer::FlushPublicationCompletion],
  failed_partitions: &[FlushPartitionResult],
) {
  for completion in completions {
    let state = failed_partitions
      .iter()
      .find(|result| {
        result.topic == completion.topic
          && result.virtual_partition_id == completion.virtual_partition_id
      })
      .and_then(|result| result.error)
      .map_or(FlushPublicationState::SegmentIdentityAssigned, |error| {
        FlushPublicationState::Completed(FlushPublicationResult::Failed(error))
      });
    completion.state_tx.send_replace(state);
  }
}

/// Wait only for the predecessor's identity barrier. A successful identity assignment permits
/// concurrent encoding and blob upload; a terminal failure rejects this successor before it can
/// create an unpublishable segment.
async fn wait_for_segment_identity(
  mut predecessor: Option<FlushPublicationDependency>,
) -> Option<FlushCompletionError> {
  let predecessor = predecessor.as_mut()?;
  loop {
    match *predecessor.state_rx.borrow_and_update() {
      FlushPublicationState::Pending => {},
      FlushPublicationState::SegmentIdentityAssigned
      | FlushPublicationState::Completed(FlushPublicationResult::Succeeded) => return None,
      FlushPublicationState::Completed(FlushPublicationResult::Failed(error)) => {
        return Some(error);
      },
    }
    if predecessor.state_rx.changed().await.is_err() {
      return Some(FlushCompletionError::Internal);
    }
  }
}

/// Signal terminal failure before identity assignment so every successor waiting on this epoch
/// receives the same outcome from the shared state machine.
fn mark_publication_complete(
  completions: &[super::buffer::FlushPublicationCompletion],
  result: FlushPublicationResult,
) {
  for completion in completions {
    completion
      .state_tx
      .send_replace(FlushPublicationState::Completed(result));
  }
}

/// Wait for terminal metadata publication. Seeing `SegmentIdentityAssigned` is not enough here:
/// a successor may have uploaded its blob, but must not publish a later sequence range first.
async fn wait_for_predecessor(
  mut predecessor: Option<FlushPublicationDependency>,
) -> Option<FlushCompletionError> {
  let predecessor = predecessor.as_mut()?;
  loop {
    match *predecessor.state_rx.borrow_and_update() {
      FlushPublicationState::Pending | FlushPublicationState::SegmentIdentityAssigned => {},
      FlushPublicationState::Completed(FlushPublicationResult::Succeeded) => return None,
      FlushPublicationState::Completed(FlushPublicationResult::Failed(error)) => {
        return Some(error);
      },
    }
    if predecessor.state_rx.changed().await.is_err() {
      return Some(FlushCompletionError::Internal);
    }
  }
}
