// blob-stream - broker write path tests
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{TopicInfo, WriteConfig, WriteEngine, WriteEngineImpl, WriteRequest};
use anyhow::Result;
use bd_time::{OffsetDateTimeExt, TestTimeProvider, TimeProvider};
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  MetadataStore,
};
use blob_stream_types::{Compression, CompressionCodec, Record, SeqRange, Window};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::{Duration as TimeDuration, OffsetDateTime};

fn time_from_ms(ms: i64) -> OffsetDateTime {
  OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
    .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn make_engine(
  time_provider: Arc<TestTimeProvider>,
  config: WriteConfig,
) -> Result<(WriteEngineImpl, Arc<InMemoryMetadataStore>)> {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".to_string(),
    TopicInfo {
      name: "telemetry".to_string(),
      partition_count: 1,
      num_writers: 1,
    },
  );

  let blob_store = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());

  let engine = WriteEngineImpl::new_with_time_provider(
    config,
    topics,
    blob_store,
    metadata_store.clone(),
    lease_store,
    "test-node".to_string(),
    None,
    time_provider,
  )?;

  Ok((engine, metadata_store))
}

#[tokio::test]
async fn buffers_until_size_rollover() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 10;
  config.flush_max_delay_ms = 60_000;
  config.window_size_seconds = 60;

  let (engine, metadata_store) = make_engine(time_provider.clone(), config.clone())?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![1; 6], 10)],
  };

  engine.produce_batch(request).await?;

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    config.window_size_seconds,
  );
  let segments = metadata_store
    .scan_window(&window.key("telemetry"), None)
    .await?;
  assert!(segments.is_empty());

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![2; 6], 20)],
  };

  engine.produce_batch(request).await?;

  let segments = metadata_store
    .scan_window(&window.key("telemetry"), None)
    .await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].record_count, 2);
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn flushes_on_time_rollover() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1024;
  config.flush_max_delay_ms = 500;
  config.window_size_seconds = 60;

  let (engine, metadata_store) = make_engine(time_provider.clone(), config.clone())?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![3; 4], 30)],
  };

  engine.produce_batch(request).await?;

  let advance = TimeDuration::milliseconds(config.flush_max_delay_ms + 10);
  time_provider.advance(advance);
  tokio::time::advance(StdDuration::from_millis(
    (config.flush_max_delay_ms + 10).cast_unsigned(),
  ))
  .await;
  tokio::task::yield_now().await;

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    config.window_size_seconds,
  );
  let segments = metadata_store
    .scan_window(&window.key("telemetry"), None)
    .await?;
  assert_eq!(segments.len(), 1);
  Ok(())
}

#[tokio::test]
async fn assigns_monotonic_sequences() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1024;
  config.flush_max_delay_ms = 60_000;

  let (engine, _metadata_store) = make_engine(time_provider.clone(), config)?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![9; 2], 10), Record::new(vec![9; 2], 11)],
  };

  let response = engine.produce_batch(request).await?;
  assert_eq!(response.seq_range, SeqRange { start: 0, end: 1 });

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![10; 3], 12)],
  };

  let response = engine.produce_batch(request).await?;
  assert_eq!(response.seq_range, SeqRange { start: 2, end: 2 });
  Ok(())
}

#[tokio::test]
async fn writes_compressed_metadata() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 5;
  config.flush_max_delay_ms = 60_000;
  config.window_size_seconds = 60;
  config.compression = Compression::zstd(1);

  let (engine, metadata_store) = make_engine(time_provider.clone(), config.clone())?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![7; 6], 10)],
  };

  engine.produce_batch(request).await?;

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    config.window_size_seconds,
  );
  let segments = metadata_store
    .scan_window(&window.key("telemetry"), None)
    .await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].compression.codec, CompressionCodec::Zstd);

  let batch_metadata = segments[0].segment_index.get(&0).unwrap().first().unwrap();
  assert_eq!(batch_metadata.compression.codec, CompressionCodec::Zstd);
  Ok(())
}

#[test]
fn ownership_changes_with_membership() {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".to_string(),
    TopicInfo {
      name: "telemetry".to_string(),
      partition_count: 8,
      num_writers: 1,
    },
  );

  let solo_a = BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  }]);
  let owned_solo = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-a", &solo_a);
  assert_eq!(owned_solo.len(), 8);

  let split = BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".to_string(),
      address: "10.0.0.1:8080".to_string(),
    },
    BrokerNode {
      node_id: "node-b".to_string(),
      address: "10.0.0.2:8080".to_string(),
    },
  ]);

  let owned_a = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-a", &split)
    .into_iter()
    .collect::<HashSet<_>>();
  let owned_b = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-b", &split)
    .into_iter()
    .collect::<HashSet<_>>();

  assert!(!owned_a.is_empty());
  assert!(!owned_b.is_empty());
  assert!(owned_a.is_disjoint(&owned_b));
  assert_eq!(owned_a.union(&owned_b).count(), 8);

  let solo_b = BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".to_string(),
    address: "10.0.0.2:8080".to_string(),
  }]);
  let owned_after_move = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-a", &solo_b);
  assert!(owned_after_move.is_empty());
}
