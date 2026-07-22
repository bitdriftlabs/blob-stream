use super::{FlushContext, SnowflakeGenerator};
use crate::write::buffer::{BufferedBatch, FlushPartition, FlushTrigger};
use anyhow::Result;
use blob_stream_types::{BatchSummary, SeqRange, new_record};
use time::OffsetDateTime;

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
      batches: vec![
        BufferedBatch {
          records: vec![new_record(b"first".to_vec(), 10)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 5,
          },
          seq_range: SeqRange { start: 8, end: 8 },
          completion: None,
        },
        BufferedBatch {
          records: vec![new_record(b"second".to_vec(), 20)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 6,
          },
          seq_range: SeqRange { start: 9, end: 9 },
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
    batches: vec![
      BufferedBatch {
        records: vec![new_record(b"first".to_vec(), 10)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 5,
        },
        seq_range: SeqRange { start: 8, end: 8 },
        completion: None,
      },
      BufferedBatch {
        records: vec![new_record(b"third".to_vec(), 20)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 5,
        },
        seq_range: SeqRange { start: 10, end: 10 },
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
