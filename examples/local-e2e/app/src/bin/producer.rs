use anyhow::{Result, anyhow};
use bd_server_stats::stats::Collector;
use blob_stream_local_e2e::{TOPIC, producer_runtime_config};
use blob_stream_producer::{ProducerClient, ProducerClientImpl, ProducerRecord};
use blob_stream_types::now_unix_millis;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};

//
// Cli
//

#[derive(Debug, Parser)]
#[command(
  name = "blob-stream-local-producer",
  about = "Publish entered text lines to the local Blob Stream walkthrough"
)]
struct Cli {
  /// Use one fixed partitioning key instead of rotating demonstration keys.
  #[arg(long)]
  key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
  let cli = Cli::parse();
  let metrics_scope = Collector::default().scope("blob_stream_local_producer");
  let producer =
    ProducerClientImpl::from_runtime_config(producer_runtime_config(), metrics_scope).await?;

  println!("enter text lines to publish; press Ctrl-D to stop");
  let mut lines = BufReader::new(tokio::io::stdin()).lines();
  let mut message_number = 0_u64;

  while let Some(text) = lines.next_line().await? {
    if text.is_empty() {
      continue;
    }

    // Rotating keys exercises the configured partition space. A caller can provide --key to
    // intentionally route every message through the same partitioning key instead.
    let key = cli.key.as_deref().map_or_else(
      || format!("local-key-{}", message_number % 4),
      str::to_string,
    );
    message_number = message_number.saturating_add(1);
    let record = ProducerRecord::new(
      TOPIC.into(),
      key.into_bytes(),
      text.clone().into_bytes().into(),
      now_unix_millis(),
    );

    // A successful acknowledgement means the broker accepted this submission. Applications still
    // need idempotent consumers because retrying an ambiguous request can deliver duplicates.
    let acknowledgement = producer
      .produce(vec![record])
      .await
      .pop()
      .ok_or_else(|| anyhow!("producer returned no result for submitted record"))??;
    println!(
      "published text={text:?} partition={} attempts={}",
      acknowledgement.virtual_partition_id, acknowledgement.attempts
    );
  }

  Ok(())
}
