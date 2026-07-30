use crate::test_framework::runtime::now_unix_millis;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_consumer::consumer::{
  ConsumerBatch,
  ConsumerReader,
  ConsumerReaderImpl,
  ReadCapacity,
};
use blob_stream_producer::{ProducerClient, ProducerClientImpl, ProducerRecord};
use blob_stream_types::VirtualPartitionId;
use std::collections::{HashMap, HashSet};
use tokio::time::Instant;

const TEST_READ_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

//
// ReaderDeliveryTrace
//

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReaderDeliveryTrace {
  pub id: String,
  pub virtual_partition_id: VirtualPartitionId,
  pub offset: u64,
}

#[async_trait]
pub trait TestConsumerReader {
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<ConsumerBatch>>;
  fn cursors(&self) -> HashMap<VirtualPartitionId, u64>;
}

#[async_trait]
impl TestConsumerReader for ConsumerReaderImpl {
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<ConsumerBatch>> {
    ConsumerReader::read_available(
      self,
      now_unix_seconds,
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
    )
    .await
  }

  fn cursors(&self) -> HashMap<VirtualPartitionId, u64> {
    ConsumerReader::cursors(self)
  }
}

pub async fn produce_message(
  producer: &ProducerClientImpl,
  key: Vec<u8>,
  id: &str,
) -> Result<blob_stream_producer::ProducerAck> {
  produce_message_for_topic(producer, crate::test_framework::TOPIC, key, id).await
}

pub async fn produce_message_for_topic(
  producer: &ProducerClientImpl,
  topic: &str,
  key: Vec<u8>,
  id: &str,
) -> Result<blob_stream_producer::ProducerAck> {
  let ack = producer
    .produce(ProducerRecord::new(
      topic.to_string().into(),
      key,
      id.as_bytes().to_vec().into(),
      now_unix_millis(),
    ))
    .await?;
  Ok(ack)
}

pub async fn drain_reader_until(
  reader: &mut ConsumerReaderImpl,
  consumed_ids: &mut HashSet<String>,
  expected: usize,
  reader_now_unix_seconds: i64,
  deadline: Instant,
) -> Result<()> {
  while consumed_ids.len() < expected {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded: expected={expected}, consumed={}",
        consumed_ids.len()
      ));
    }

    let batches = TestConsumerReader::read_available(reader, reader_now_unix_seconds).await?;
    for batch in batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload.to_vec())
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        consumed_ids.insert(id);
      }
    }
    tokio::task::yield_now().await;
  }

  Ok(())
}

pub async fn drain_reader_until_with_trace(
  reader: &mut ConsumerReaderImpl,
  expected_unique_ids: usize,
  reader_now_unix_seconds: i64,
  deadline: Instant,
) -> Result<Vec<ReaderDeliveryTrace>> {
  let mut deliveries = Vec::new();
  let mut delivered_ids = HashSet::new();

  while delivered_ids.len() < expected_unique_ids {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded: expected={expected_unique_ids}, consumed={}",
        delivered_ids.len()
      ));
    }

    let batches = TestConsumerReader::read_available(reader, reader_now_unix_seconds).await?;
    for batch in batches {
      let delivery_start = deliveries.len();
      append_reader_delivery_traces(vec![batch], &mut deliveries)?;
      delivered_ids.extend(
        deliveries[delivery_start ..]
          .iter()
          .map(|delivery| delivery.id.clone()),
      );
    }
    tokio::task::yield_now().await;
  }

  Ok(deliveries)
}

/// Perform one caller-timed reader rescan and append every observed delivery to its trace.
pub async fn rescan_reader_with_trace(
  reader: &mut ConsumerReaderImpl,
  now_unix_seconds: i64,
  deliveries: &mut Vec<ReaderDeliveryTrace>,
) -> Result<usize> {
  let batches = TestConsumerReader::read_available(reader, now_unix_seconds).await?;
  let batch_count = batches.len();
  append_reader_delivery_traces(batches, deliveries)?;
  Ok(batch_count)
}

pub fn append_reader_delivery_traces(
  batches: Vec<ConsumerBatch>,
  deliveries: &mut Vec<ReaderDeliveryTrace>,
) -> Result<()> {
  for batch in batches {
    for (record_index, record) in batch.records.into_iter().enumerate() {
      let offset = batch
        .seq_range
        .start
        .checked_add(u64::try_from(record_index)?)
        .ok_or_else(|| {
          anyhow!(
            "reader record offset overflow: partition={}, batch_start={}, \
             record_index={record_index}",
            batch.virtual_partition_id,
            batch.seq_range.start,
          )
        })?;
      let id = String::from_utf8(record.payload.to_vec())
        .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
      deliveries.push(ReaderDeliveryTrace {
        id,
        virtual_partition_id: batch.virtual_partition_id,
        offset,
      });
    }
  }

  Ok(())
}

pub fn reader_delivery_counts(deliveries: &[ReaderDeliveryTrace]) -> HashMap<String, usize> {
  let mut counts = HashMap::new();
  for delivery in deliveries {
    *counts.entry(delivery.id.clone()).or_insert(0) += 1;
  }
  counts
}
