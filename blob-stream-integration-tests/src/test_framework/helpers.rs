use crate::test_framework::runtime::{now_unix_millis, now_unix_seconds, runtime_sleep};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_consumer::{ConsumerBatch, ConsumerReader, ConsumerReaderImpl, ReadCapacity};
use blob_stream_producer::{ProducerClient, ProducerClientImpl, ProducerRecord};
use blob_stream_types::VirtualPartitionId;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::Instant;

const TEST_READ_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

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
      topic,
      key,
      id.as_bytes().to_vec(),
      now_unix_millis(),
    ))
    .await?;
  Ok(ack)
}

pub async fn drain_reader_until(
  reader: &mut ConsumerReaderImpl,
  consumed_ids: &mut HashSet<String>,
  expected: usize,
  deadline: Instant,
) -> Result<()> {
  while consumed_ids.len() < expected {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded: expected={expected}, consumed={}",
        consumed_ids.len()
      ));
    }

    // Keep polling until all expected IDs are observed or deadline is reached.
    let mut progressed = false;
    let batches = TestConsumerReader::read_available(reader, now_unix_seconds()).await?;
    for batch in batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload.to_vec())
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        consumed_ids.insert(id);
      }
      progressed = true;
    }

    if !progressed {
      runtime_sleep(Duration::from_millis(50)).await;
    }
  }

  Ok(())
}
