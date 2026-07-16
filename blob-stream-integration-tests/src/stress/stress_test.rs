#![allow(clippy::unwrap_used)]

use super::runner::stress_keys_for_partitions;
use super::{Observation, StressConfig, StressRecordIdentity, StressValidator};
use blob_stream_types::virtual_partition_for_key;
use uuid::Uuid;

#[test]
fn config_rejects_zero_record_workload() {
  let config = StressConfig {
    total_records: 0,
    ..Default::default()
  };
  assert!(config.validate().is_err());

  let config = StressConfig::default();
  assert!(config.validate().is_ok());
}

#[test]
fn config_rejects_zero_producer_submit_concurrency() {
  let config = StressConfig {
    producer_submit_concurrency: 0,
    ..Default::default()
  };

  assert!(config.validate().is_err());
}

#[test]
fn config_rejects_zero_broker_flush_delay() {
  let config = StressConfig {
    broker_flush_max_delay: std::time::Duration::ZERO,
    ..Default::default()
  };

  assert!(config.validate().is_err());
}

#[test]
fn config_rejects_zero_verification_timeout() {
  let config = StressConfig {
    verification_timeout: std::time::Duration::ZERO,
    ..Default::default()
  };

  assert!(config.validate().is_err());
}

#[test]
fn config_rejects_zero_consumer_commit_interval() {
  let config = StressConfig {
    consumer_commit_interval_records: 0,
    ..Default::default()
  };

  assert!(config.validate().is_err());
}

#[test]
fn config_rejects_zero_consumer_heartbeat_interval() {
  let config = StressConfig {
    consumer_heartbeat_interval: std::time::Duration::ZERO,
    ..Default::default()
  };

  assert!(config.validate().is_err());
}

#[test]
fn identity_round_trips_with_deterministic_padding() {
  let identity = StressRecordIdentity {
    run_id: Uuid::nil(),
    producer_index: 7,
    sequence: 42,
  };
  let payload = identity.encode(128);

  assert_eq!(payload.len(), 128);
  assert_eq!(StressRecordIdentity::decode(&payload).unwrap(), identity);
}

#[test]
fn validator_counts_duplicates_and_exact_missing_sequences() {
  let run_id = Uuid::nil();
  let mut validator = StressValidator::new(run_id, vec![3, 2], 2).unwrap();

  assert_eq!(
    validator
      .observe(StressRecordIdentity {
        run_id,
        producer_index: 0,
        sequence: 0,
      })
      .unwrap(),
    Observation::Unique
  );
  assert_eq!(
    validator
      .observe(StressRecordIdentity {
        run_id,
        producer_index: 0,
        sequence: 0,
      })
      .unwrap(),
    Observation::Duplicate
  );
  validator
    .observe(StressRecordIdentity {
      run_id,
      producer_index: 1,
      sequence: 1,
    })
    .unwrap();

  let summary = validator.summary();
  assert_eq!(summary.expected_records, 5);
  assert_eq!(summary.unique_records, 2);
  assert_eq!(summary.duplicate_records, 1);
  assert_eq!(summary.missing_records, 3);
  assert_eq!(summary.duplicate_samples.len(), 1);
  assert_eq!(summary.missing_samples.len(), 2);
  assert!(!summary.is_complete());
}

#[test]
fn validator_rejects_duplicate_only_completion() {
  let run_id = Uuid::nil();
  let mut validator = StressValidator::new(run_id, vec![1], 1).unwrap();
  let identity = StressRecordIdentity {
    run_id,
    producer_index: 0,
    sequence: 0,
  };

  validator.observe(identity).unwrap();
  validator.observe(identity).unwrap();

  assert!(!validator.summary().is_complete());
}

#[test]
fn generated_stress_keys_cover_every_partition() {
  let partition_count = 32;
  let partition_keys = stress_keys_for_partitions(partition_count).unwrap();

  for (partition, key) in partition_keys.iter().enumerate() {
    assert_eq!(
      virtual_partition_for_key(key, partition_count, 0),
      u32::try_from(partition).unwrap()
    );
  }
}
