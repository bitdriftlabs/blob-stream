use super::{FlushContext, SnowflakeGenerator};
use crate::write::WriteConfig;
use crate::write::buffer::{BufferedBatch, FlushPartition, FlushTrigger};
use crate::write::metrics::WriteMetrics;
use anyhow::Result;
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  MetadataReadConsistency,
  MetadataStore,
  MetadataWriteError,
  MetadataWriteResult,
  ProducerLeaseFence,
  ProducerPartitionFence,
  ProducerPartitionLeaseKey,
  SegmentMetadata,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{BatchSummary, SeqRange, Window, new_record};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use time::OffsetDateTime;

struct LostFenceMetadataStore {
  calls: AtomicUsize,
}

#[async_trait]
impl MetadataStore for LostFenceMetadataStore {
  async fn write_segment(
    &self,
    _metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    _now_ts_ms: i64,
  ) -> MetadataWriteResult {
    assert_eq!(
      fences,
      Some(&[ProducerPartitionFence {
        key: ProducerPartitionLeaseKey {
          topic: "telemetry".into(),
          virtual_partition_id: 4,
        },
        fence: lease_fence(),
      }] as &[ProducerPartitionFence])
    );
    self.calls.fetch_add(1, Ordering::Relaxed);
    Err(MetadataWriteError::ProducerLeaseFenceLost)
  }

  async fn scan_window_from_snowflake(
    &self,
    _window: &blob_stream_types::TopicWindowKey,
    _min_snowflake: Option<blob_stream_types::SnowflakeId>,
    _consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    Ok(Vec::new())
  }
}

fn lease_fence() -> ProducerLeaseFence {
  ProducerLeaseFence {
    holder_id: "broker-a".to_string(),
    lease_epoch: 1,
    lease_session_id: "session-a".to_string(),
  }
}

#[test]
fn default_machine_id_generator_initializes() -> Result<()> {
  let generator = SnowflakeGenerator::new()?;

  assert!(generator.next(OffsetDateTime::now_utc())?.as_u64() > 0);
  Ok(())
}

#[test]
fn explicit_machine_ids_produce_distinct_ids() -> Result<()> {
  let first = SnowflakeGenerator::with_machine_id(1)?;
  let second = SnowflakeGenerator::with_machine_id(2)?;
  let now = OffsetDateTime::now_utc();

  assert_ne!(first.next(now)?.as_u64(), second.next(now)?.as_u64());
  Ok(())
}

#[test]
fn merge_partition_batches_preserves_order_and_combines_metadata() -> Result<()> {
  let (virtual_partition_id, records, summary, seq_range) =
    FlushContext::merge_partition_batches(FlushPartition {
      virtual_partition_id: 4,
      lease_fence: Some(Arc::new(lease_fence())),
      batches: vec![
        BufferedBatch {
          records: vec![new_record(b"first".to_vec(), 10)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 5,
          },
          seq_range: SeqRange { start: 8, end: 8 },
          acceptance_fence: Some(Arc::new(lease_fence())),
          completion: None,
        },
        BufferedBatch {
          records: vec![new_record(b"second".to_vec(), 20)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 6,
          },
          seq_range: SeqRange { start: 9, end: 9 },
          acceptance_fence: Some(Arc::new(lease_fence())),
          completion: None,
        },
      ],
      trigger: FlushTrigger::MaxBytes,
    })?;

  assert_eq!(virtual_partition_id, 4);
  assert_eq!(seq_range, SeqRange { start: 8, end: 9 });
  assert_eq!(
    summary,
    BatchSummary {
      record_count: 2,
      payload_bytes: 11,
    }
  );
  assert_eq!(records[0].payload.as_ref(), b"first");
  assert_eq!(records[1].payload.as_ref(), b"second");
  Ok(())
}

#[test]
fn merge_partition_batches_rejects_noncontiguous_ranges() {
  let result = FlushContext::merge_partition_batches(FlushPartition {
    virtual_partition_id: 4,
    lease_fence: Some(Arc::new(lease_fence())),
    batches: vec![
      BufferedBatch {
        records: vec![new_record(b"first".to_vec(), 10)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 5,
        },
        seq_range: SeqRange { start: 8, end: 8 },
        acceptance_fence: Some(Arc::new(lease_fence())),
        completion: None,
      },
      BufferedBatch {
        records: vec![new_record(b"third".to_vec(), 20)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 5,
        },
        seq_range: SeqRange { start: 10, end: 10 },
        acceptance_fence: Some(Arc::new(lease_fence())),
        completion: None,
      },
    ],
    trigger: FlushTrigger::MaxBytes,
  });

  let Err(error) = result else {
    panic!("noncontiguous batches must fail to merge");
  };
  assert!(error.to_string().contains("noncontiguous ranges"));
}

#[tokio::test]
async fn lost_fence_does_not_fall_back_to_ordinary_metadata_write() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let time_provider = Arc::new(ManualTimeProvider::new(now));
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let publisher = Arc::new(LostFenceMetadataStore {
    calls: AtomicUsize::new(0),
  });
  let collector = Collector::default();
  let scope = collector.scope("flush_test");
  let config = WriteConfig::with_defaults();
  let context = FlushContext::new(
    config.clone(),
    Arc::new(InMemoryBlobStore::new()),
    publisher.clone(),
    SnowflakeGenerator::with_machine_id(1)?,
    time_provider,
    None,
  );
  let mut plan = super::super::buffer::FlushPlan {
    topic: "telemetry".into(),
    partitions: vec![FlushPartition {
      virtual_partition_id: 4,
      lease_fence: Some(Arc::new(lease_fence())),
      batches: vec![BufferedBatch {
        records: vec![new_record(b"payload".to_vec(), 10)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 7,
        },
        seq_range: SeqRange { start: 0, end: 0 },
        acceptance_fence: Some(Arc::new(lease_fence())),
        completion: None,
      }],
      trigger: FlushTrigger::MaxBytes,
    }],
    max_metadata_publication_lag: time::Duration::seconds(1),
    metadata_window_size: time::Duration::minutes(5),
    fenced_metadata_writes: true,
  };

  let error = context
    .flush_plan(&mut plan, now, &WriteMetrics::new(&scope))
    .await
    .expect_err("lost fence must fail the flush");
  assert!(
    format!("{error:#}").contains("lease fence was lost"),
    "unexpected flush error: {error:#}"
  );
  assert_eq!(publisher.calls.load(Ordering::Relaxed), 1);

  let window = Window::for_timestamp(now, plan.metadata_window_size).key("telemetry");
  assert!(
    metadata_store
      .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Eventual)
      .await?
      .is_empty()
  );
  Ok(())
}
