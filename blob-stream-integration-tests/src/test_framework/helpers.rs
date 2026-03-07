use crate::test_framework::runtime::{now_unix_millis, now_unix_seconds, runtime_sleep};
use anyhow::{Result, anyhow};
use blob_stream_consumer::{ConsumerReader, ConsumerReaderImpl};
use blob_stream_producer::{ProducerClient, ProducerClientImpl, ProducerRecord};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::Instant;

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
    let batches = reader.read_available(now_unix_seconds()).await?;
    for batch in batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload)
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
