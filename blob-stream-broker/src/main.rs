use anyhow::{Context, Result};
use blob_stream::config::load_runtime_config;
use blob_stream::grpc::make_broker_router;
use blob_stream::metrics::BrokerMetrics;
use blob_stream::write::build_write_engine;
use clap::Parser;
use log::info;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "blob-stream-broker", about = "Blob stream broker")]
struct Cli {
  #[arg(short, long, env = "BLOB_STREAM_CONFIG", value_name = "PATH")]
  config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
  let cli = Cli::parse();
  let config = load_runtime_config(&cli.config)?;
  let broker_config = config
    .broker
    .as_ref()
    .context("runtime config missing broker config")?;
  let bind_addr = broker_config.bind_addr.to_string();
  let bind_addr = bind_addr.trim().to_string();
  anyhow::ensure!(
    !bind_addr.is_empty(),
    "broker.bind_addr is required in runtime config"
  );
  let addr: SocketAddr = bind_addr
    .parse()
    .with_context(|| format!("invalid broker.bind_addr: {bind_addr}"))?;

  info!("broker starting: bind_addr={addr}");

  let metrics = BrokerMetrics::new();
  let write_engine = build_write_engine(&config, &metrics).await?;
  let listener = tokio::net::TcpListener::bind(addr).await?;
  info!("broker listening: bind_addr={addr}, metrics_path=/metrics");
  axum::serve(listener, make_broker_router(write_engine, &metrics)).await?;
  info!("broker shutdown complete");
  Ok(())
}
