use anyhow::{Result, bail};
use blob_stream_metadata_inspector::{
  InspectionReport,
  InspectorRequest,
  MAX_WINDOW_RADIUS,
  MetadataBatchRow,
  cursor_context_range,
  inspect_metadata,
};
use blob_stream_metadata_store::{DynamoMetadataStore, build_dynamo_client};
use clap::{Parser, ValueEnum};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

//
// Cli
//

/// Inspect Blob Stream metadata rows around an application-visible consumer gap.
#[derive(Debug, Parser)]
#[command(name = "blob-stream-metadata-inspector")]
struct Cli {
  #[arg(long, env = "BLOB_STREAM_SEGMENT_DYNAMO_TABLE")]
  segment_dynamo_table: String,
  #[arg(long)]
  topic: String,
  #[arg(long, value_parser = parse_timestamp)]
  target_time: OffsetDateTime,
  #[arg(long)]
  suspect_cursor: u64,
  #[arg(long)]
  partition: u32,
  #[arg(long, default_value_t = 300)]
  metadata_window_seconds: i64,
  #[arg(long, default_value_t = 1)]
  window_radius: u32,
  #[arg(long, default_value_t = 3)]
  context_rows: usize,
  #[arg(long, env = "AWS_REGION")]
  aws_region: String,
  #[arg(long, env = "DYNAMODB_ENDPOINT", default_value = "")]
  dynamodb_endpoint: String,
  #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
  format: OutputFormat,
}

//
// OutputFormat
//

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
  Table,
  Json,
}

#[tokio::main]
async fn main() -> Result<()> {
  let cli = Cli::parse();
  if cli.metadata_window_seconds <= 0 {
    bail!("--metadata-window-seconds must be positive");
  }
  if cli.window_radius > MAX_WINDOW_RADIUS {
    bail!("--window-radius must not exceed {MAX_WINDOW_RADIUS}");
  }
  let request = InspectorRequest {
    topic: cli.topic,
    target_time: cli.target_time,
    suspect_cursor: cli.suspect_cursor,
    virtual_partition_id: cli.partition,
    metadata_window_seconds: cli.metadata_window_seconds,
    window_radius: cli.window_radius,
  };
  let client = build_dynamo_client(&cli.aws_region, &cli.dynamodb_endpoint).await;
  let store = DynamoMetadataStore::new_read_only(client, cli.segment_dynamo_table);
  let report = inspect_metadata(&store, &request).await?;
  match cli.format {
    OutputFormat::Table => print_table(&report, cli.context_rows),
    OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
  }
  Ok(())
}

fn parse_timestamp(value: &str) -> Result<OffsetDateTime, String> {
  OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())
}

fn print_table(report: &InspectionReport, context_rows: usize) {
  println!(
    "topic={} partition={} target_time={} suspect_cursor={}",
    report.topic, report.virtual_partition_id, report.target_time, report.suspect_cursor
  );
  println!("queried_window_starts={:?}", report.queried_window_starts);
  println!(
    "source_order_matches_sequence_order={}",
    report.source_order_matches_sequence_order
  );
  print_rows_around_cursor(
    "sequence-order cursor context",
    &report.sequence_ordered_rows,
    report.suspect_cursor,
    context_rows,
  );
  if !report.source_order_matches_sequence_order {
    print_rows_around_cursor(
      "source-order cursor context",
      &report.source_ordered_rows,
      report.suspect_cursor,
      context_rows,
    );
  }
  println!("source-order continuity issues:");
  for issue in &report.source_order_continuity_issues {
    println!("  {issue:?}");
  }
  println!("sequence-order continuity issues:");
  for issue in &report.continuity_issues {
    println!("  {issue:?}");
  }
}

fn print_rows_around_cursor(
  title: &str,
  rows: &[MetadataBatchRow],
  suspect_cursor: u64,
  context_rows: usize,
) {
  let context = cursor_context_range(rows, suspect_cursor, context_rows);
  println!("{title} ({} total rows):", rows.len());
  if context.start > 0 {
    println!("  ... {} earlier rows omitted", context.start);
  }
  for row in &rows[context.clone()] {
    println!(
      "  window={} snowflake={} range={}..={} bytes={}..{} payload_bytes={} created_at={} \
       published_at={} relation={:?} blob={}",
      row.window_start_unix_seconds,
      row.snowflake_id,
      row.sequence_start,
      row.sequence_end,
      row.byte_start,
      row.byte_end,
      row.payload_bytes,
      row.created_at,
      row.metadata_published_at,
      row.cursor_relation,
      row.blob_key
    );
  }
  if context.end < rows.len() {
    println!("  ... {} later rows omitted", rows.len() - context.end);
  }
}
