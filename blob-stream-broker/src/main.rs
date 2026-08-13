use anyhow::{Context, Result};
use bd_panic::PanicType;
use bd_runtime_config::loader::Stats;
use bd_shutdown::{ComponentShutdownTrigger, real_graceful_shutdown};
use blob_stream_broker::config::load_runtime_config;
use blob_stream_broker::grpc::make_broker_router;
use blob_stream_broker::metrics::BrokerMetrics;
use blob_stream_broker::write::build_write_engine;
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

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
  bd_panic::default(PanicType::ForceAbort);
  bd_log::SwapLogger::initialize();

  let runtime = bd_rt::new_runtime()?;
  runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
  let cli = Cli::parse();
  let config = load_runtime_config(&cli.config)?;
  let broker_config = config
    .broker
    .as_ref()
    .context("runtime config missing broker config")?;
  let bind_addr = broker_config.bind_addr.to_string();
  let bind_addr = bind_addr.trim().to_string();
  let addr: SocketAddr = bind_addr
    .parse()
    .with_context(|| format!("invalid broker.bind_addr: {bind_addr}"))?;

  let config_text = protobuf::text_format::print_to_string_pretty(&config);
  info!("broker starting with config:\n{config_text}");

  let metrics = BrokerMetrics::new();
  let metrics_scope = metrics.scope();
  let feature_flags = broker_config
    .feature_flags
    .as_ref()
    .map(|feature_flags| {
      bd_runtime_config::feature_flags::new_memory_feature_flags_loader(
        &feature_flags.dir,
        &feature_flags.file,
        Stats::new(&metrics_scope.scope("feature_flags_loader")),
      )
    })
    .transpose()?;
  let feature_flags_watch = feature_flags
    .as_ref()
    .map(|feature_flags| feature_flags.snapshot_watch());
  let broker_shutdown_trigger = ComponentShutdownTrigger::default();
  let write_engine = build_write_engine(
    &config,
    broker_shutdown_trigger.make_handle(),
    &metrics_scope,
    feature_flags_watch,
  )
  .await?;
  let listener = tokio::net::TcpListener::bind(addr).await?;
  info!("broker listening: bind_addr={addr}, metrics_path=/metrics, log_path=/admin/log");

  let listener_shutdown_trigger = ComponentShutdownTrigger::default();
  let mut listener_shutdown = listener_shutdown_trigger.make_shutdown();
  let server = tokio::spawn(async move {
    axum::serve(listener, make_broker_router(write_engine, &metrics))
      .with_graceful_shutdown(async move { listener_shutdown.cancelled().await })
      .await
  });

  real_graceful_shutdown().await;
  info!("broker stopping listener and draining active requests");
  listener_shutdown_trigger.shutdown().await;
  server.await??;

  info!("broker draining writes and releasing producer leases");
  broker_shutdown_trigger.shutdown().await;
  if let Some(feature_flags) = feature_flags {
    feature_flags.shutdown().await;
  }
  info!("broker shutdown complete");
  Ok(())
}
