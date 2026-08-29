use anyhow::Result;
use bd_server_stats::stats::Collector;
use blob_stream_consumer::ConsumerConfigFactory;
use blob_stream_consumer::iterator::{ConsumerIterator, NextResult};
use blob_stream_local_e2e::{DEFAULT_GROUP_ID, consumer_bootstrap_config};
use clap::Parser;

//
// Cli
//

#[derive(Debug, Parser)]
#[command(
  name = "blob-stream-local-consumer",
  about = "Print messages from the local Blob Stream walkthrough"
)]
struct Cli {
  /// Stable identity for this member of its consumer group.
  #[arg(long)]
  member_id: String,

  /// Consumer group; members of the same group divide partitions between themselves.
  #[arg(long, default_value = DEFAULT_GROUP_ID)]
  group_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
  let cli = Cli::parse();
  let metrics_scope = Collector::default().scope("blob_stream_local_consumer");
  let mut iterator = ConsumerConfigFactory::build_iterator_from_proto_config(
    consumer_bootstrap_config(&cli.group_id, &cli.member_id),
    metrics_scope,
    None,
  )
  .await?;
  iterator.start()?;

  println!(
    "consumer member={} group={} started; press Ctrl-C to stop",
    cli.member_id, cli.group_id
  );

  loop {
    tokio::select! {
      result = iterator.next() => match result? {
        NextResult::Record(record) => {
          let text = String::from_utf8_lossy(&record.record.payload);
          println!(
            "member={} partition={} offset={} text={text:?}",
            cli.member_id, record.virtual_partition_id, record.offset
          );

          // Printing is this example's complete application processing. Record the offset only
          // afterward, then commit it so another group member can resume after this record.
          iterator.store_offset(record.virtual_partition_id, record.offset)?;
          iterator.commit().await?;
        },
        NextResult::Revoked(revoked) => {
          let partitions = revoked.partitions();
          println!("member={} released partitions={partitions:?}", cli.member_id);

          // This example has no background work. Real applications must drain in-flight work for
          // these partitions before calling complete, or a new member may replay that work.
          revoked.complete().await;
        },
      },
      signal = tokio::signal::ctrl_c() => {
        signal?;
        break;
      },
    }
  }

  // The iterator takes ownership for shutdown so it can make a final commit and relinquish its
  // group membership and partition leases before another consumer takes over.
  Box::new(iterator).shutdown().await
}
