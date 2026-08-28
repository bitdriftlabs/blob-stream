use super::collect_next_flush_plan;
use crate::write::buffer::{BufferedBatch, FlushCompletionError, FlushPlan, FlushTrigger};
use crate::write::config::{
  FLUSH_MAX_BYTES_FEATURE_FLAG,
  FLUSH_MAX_DELAY_FEATURE_FLAG,
  MAX_SEGMENT_BYTES_FEATURE_FLAG,
};
use crate::write::state::WriteState;
use crate::write::{TopicInfo, WriteConfig};
use bd_runtime_config::loader::Loader;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_metadata_store::ProducerLeaseFence;
use blob_stream_types::{BatchSummary, SeqRange, new_record, offset_datetime_from_unix_millis};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use time::Duration;

fn fence() -> ProducerLeaseFence {
  ProducerLeaseFence {
    holder_id: "broker-a".to_string(),
    lease_epoch: 1,
    lease_session_id: "session-a".to_string(),
  }
}

fn topics() -> HashMap<protobuf::Chars, TopicInfo> {
  HashMap::from([(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 100,
      num_writers: 1,
      retention: Duration::days(7),
      max_metadata_publication_lag: Duration::milliseconds(15_000),
      metadata_window_size: Duration::minutes(5),
    },
  )])
}

fn collect_all_flush_plans(
  state: &Arc<Mutex<WriteState>>,
  now: time::OffsetDateTime,
  config: &WriteConfig,
  feature_flags: Option<&bd_runtime_config::feature_flags::FeatureFlagsWatch>,
  topics: &HashMap<protobuf::Chars, TopicInfo>,
) -> Vec<FlushPlan> {
  let flush_config = config.effective_flush_config(feature_flags);
  let mut plans = Vec::new();
  while let Some(plan) =
    collect_next_flush_plan(state, now, config, &flush_config, feature_flags, topics)
  {
    plans.push(plan);
  }
  plans
}

#[test]
fn fenced_flushes_split_at_ninety_nine_flushable_partitions() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for virtual_partition_id in 0 .. 100 {
      let partition = state.partition_state_mut("telemetry", virtual_partition_id);
      partition.lease_fence = Some(Arc::new(fence()));
      partition.buffer.push(
        BufferedBatch {
          records: vec![new_record(vec![1], 0)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 1,
          },
          seq_range: SeqRange {
            start: virtual_partition_id.into(),
            end: virtual_partition_id.into(),
          },
          acceptance_fence: Some(Arc::new(fence())),
          completion: None,
        },
        offset_datetime_from_unix_millis(0),
      );
    }
  }

  let mut config = WriteConfig::with_defaults();
  config.fenced_metadata_writes = true;
  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &config,
    None,
    &topics(),
  );

  assert_eq!(plans.len(), 2);
  assert_eq!(plans[0].topics[0].partitions.len(), 99);
  assert_eq!(plans[1].topics[0].partitions.len(), 1);
  assert!(
    plans
      .iter()
      .all(|plan| plan.topics.iter().all(|topic| topic.fenced_metadata_writes))
  );
}

#[test]
fn shared_fenced_flushes_keep_topic_chunks_within_transaction_limit() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for topic in ["telemetry", "logs"] {
      for virtual_partition_id in 0 .. 100 {
        let partition = state.partition_state_mut(topic, virtual_partition_id);
        partition.lease_fence = Some(Arc::new(fence()));
        partition.buffer.push(
          BufferedBatch {
            records: vec![new_record(vec![1], 0)],
            summary: BatchSummary {
              record_count: 1,
              payload_bytes: 1,
            },
            seq_range: SeqRange {
              start: virtual_partition_id.into(),
              end: virtual_partition_id.into(),
            },
            acceptance_fence: Some(Arc::new(fence())),
            completion: None,
          },
          offset_datetime_from_unix_millis(0),
        );
      }
    }
  }

  let mut config = WriteConfig::with_defaults();
  config.fenced_metadata_writes = true;
  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &config,
    None,
    &HashMap::from([
      (
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 100,
          num_writers: 1,
          retention: Duration::days(7),
          max_metadata_publication_lag: Duration::milliseconds(15_000),
          metadata_window_size: Duration::minutes(5),
        },
      ),
      (
        "logs".into(),
        TopicInfo {
          name: "logs".into(),
          partition_count: 100,
          num_writers: 1,
          retention: Duration::days(7),
          max_metadata_publication_lag: Duration::milliseconds(15_000),
          metadata_window_size: Duration::minutes(5),
        },
      ),
    ]),
  );

  assert_eq!(plans.len(), 2);
  assert!(plans.iter().all(|plan| plan.shared_blob));
  assert!(plans.iter().all(|plan| plan.topics.len() == 2));
  let mut partition_counts = plans
    .iter()
    .map(|plan| {
      assert!(
        plan
          .topics
          .iter()
          .all(|topic| topic.partitions.len() == plan.topics[0].partitions.len())
      );
      plan.topics[0].partitions.len()
    })
    .collect::<Vec<_>>();
  partition_counts.sort_unstable();
  assert_eq!(partition_counts, [1, 99]);
}

#[test]
fn live_feature_flag_updates_apply_to_new_flush_plans() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for topic in ["first", "second"] {
      state.partition_state_mut(topic, 0).buffer.push(
        BufferedBatch {
          records: vec![new_record(vec![1], 0)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 1,
          },
          seq_range: SeqRange { start: 0, end: 0 },
          acceptance_fence: None,
          completion: None,
        },
        offset_datetime_from_unix_millis(0),
      );
    }
  }
  let topic_info = |name: &'static str| TopicInfo {
    name: name.into(),
    partition_count: 1,
    num_writers: 1,
    retention: Duration::days(7),
    max_metadata_publication_lag: Duration::seconds(15),
    metadata_window_size: Duration::minutes(5),
  };
  let topics = HashMap::from([
    ("first".into(), topic_info("first")),
    ("second".into(), topic_info("second")),
  ]);
  let flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let feature_flags = flags.snapshot_watch();
  flags.update(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(FLUSH_MAX_BYTES_FEATURE_FLAG, 2)
      .with_integer_flag(FLUSH_MAX_DELAY_FEATURE_FLAG, 500)
      .with_integer_flag(MAX_SEGMENT_BYTES_FEATURE_FLAG, 1),
  ));

  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(600),
    &WriteConfig::with_defaults(),
    Some(&feature_flags),
    &topics,
  );

  assert_eq!(plans.len(), 1);
  assert!(plans[0].shared_blob);
  assert_eq!(plans[0].max_segment_bytes, 1);
  assert_eq!(plans[0].topics.len(), 2);
}

#[test]
fn fenced_flush_uses_the_batches_acceptance_fence() {
  let acceptance_fence = fence();
  let current_fence = ProducerLeaseFence {
    lease_epoch: 2,
    ..acceptance_fence.clone()
  };
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    partition.lease_fence = Some(Arc::new(current_fence));
    partition.buffer.push(
      BufferedBatch {
        records: vec![new_record(vec![1], 0)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 1,
        },
        seq_range: SeqRange { start: 0, end: 0 },
        acceptance_fence: Some(Arc::new(acceptance_fence.clone())),
        completion: None,
      },
      offset_datetime_from_unix_millis(0),
    );
  }

  let mut config = WriteConfig::with_defaults();
  config.fenced_metadata_writes = true;
  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &config,
    None,
    &topics(),
  );

  assert_eq!(plans.len(), 1);
  assert_eq!(
    plans[0].topics[0].partitions[0].lease_fence.as_deref(),
    Some(&acceptance_fence)
  );
}

#[test]
fn fenced_flush_drops_batches_without_an_acceptance_fence() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  let (completion, mut completion_rx) = tokio::sync::oneshot::channel();
  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    partition.lease_fence = Some(Arc::new(fence()));
    partition.buffer.push(
      BufferedBatch {
        records: vec![new_record(vec![1], 0)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 1,
        },
        seq_range: SeqRange { start: 0, end: 0 },
        acceptance_fence: None,
        completion: Some(completion),
      },
      offset_datetime_from_unix_millis(0),
    );
  }

  let mut config = WriteConfig::with_defaults();
  config.fenced_metadata_writes = true;
  assert!(
    collect_all_flush_plans(
      &state,
      offset_datetime_from_unix_millis(1_000),
      &config,
      None,
      &topics(),
    )
    .is_empty()
  );
  assert_eq!(
    completion_rx
      .try_recv()
      .expect("discarded batch must complete"),
    Err(FlushCompletionError::LeaseFenceLost)
  );
  let state = state.lock();
  let partition = state
    .partition_state("telemetry", 0)
    .expect("partition exists");
  assert!(partition.buffer.batches.is_empty());
  assert!(!partition.flush_in_flight);
}

#[test]
fn shared_time_flush_pulls_forward_buffered_topics() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for (topic, virtual_partition_id, buffered_at) in [
      ("time-due", 0, 0),
      ("time-due", 1, 1_000),
      ("fresh", 0, 1_000),
      ("fresh", 1, 1_000),
    ] {
      state
        .partition_state_mut(topic, virtual_partition_id)
        .buffer
        .push(
          BufferedBatch {
            records: vec![new_record(vec![1], 0)],
            summary: BatchSummary {
              record_count: 1,
              payload_bytes: 1,
            },
            seq_range: SeqRange { start: 0, end: 0 },
            acceptance_fence: None,
            completion: None,
          },
          offset_datetime_from_unix_millis(buffered_at),
        );
    }
  }
  let topic_info = |name: &'static str| TopicInfo {
    name: name.into(),
    partition_count: 2,
    num_writers: 1,
    retention: Duration::days(7),
    max_metadata_publication_lag: Duration::seconds(15),
    metadata_window_size: Duration::minutes(5),
  };
  let topics = HashMap::from([
    ("time-due".into(), topic_info("time-due")),
    ("fresh".into(), topic_info("fresh")),
  ]);
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 2;

  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &config,
    None,
    &topics,
  );

  assert_eq!(plans.len(), 1);
  assert!(plans[0].shared_blob);
  assert_eq!(plans[0].topics.len(), 2);
  assert!(plans[0].topics.iter().all(|topic| {
    topic.partitions.len() == 2
      && topic
        .partitions
        .iter()
        .all(|partition| matches!(partition.trigger, FlushTrigger::MaxDelay))
  }));
}

#[test]
fn no_op_planning_keeps_multiple_topic_partitions_buffered() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for (topic, virtual_partition_id) in [("first", 0), ("first", 1), ("second", 0), ("second", 1)]
    {
      state
        .partition_state_mut(topic, virtual_partition_id)
        .buffer
        .push(
          BufferedBatch {
            records: vec![new_record(vec![1], 0)],
            summary: BatchSummary {
              record_count: 1,
              payload_bytes: 1,
            },
            seq_range: SeqRange { start: 0, end: 0 },
            acceptance_fence: None,
            completion: None,
          },
          offset_datetime_from_unix_millis(1_000),
        );
    }
  }
  let topic_info = |name: &'static str| TopicInfo {
    name: name.into(),
    partition_count: 2,
    num_writers: 1,
    retention: Duration::days(7),
    max_metadata_publication_lag: Duration::seconds(15),
    metadata_window_size: Duration::minutes(5),
  };
  let topics = HashMap::from([
    ("first".into(), topic_info("first")),
    ("second".into(), topic_info("second")),
  ]);
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 2;

  assert!(
    collect_all_flush_plans(
      &state,
      offset_datetime_from_unix_millis(1_000),
      &config,
      None,
      &topics,
    )
    .is_empty()
  );

  let state = state.lock();
  for (topic, virtual_partition_id) in [("first", 0), ("first", 1), ("second", 0), ("second", 1)] {
    let partition = state
      .partition_state(topic, virtual_partition_id)
      .expect("partition remains buffered");
    assert_eq!(partition.buffer.batches.len(), 1);
    assert!(!partition.flush_in_flight);
  }
}

#[test]
fn shared_time_flush_keeps_independently_byte_triggered_work_local() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for topic in ["first", "second"] {
      state.partition_state_mut(topic, 0).buffer.push(
        BufferedBatch {
          records: vec![new_record(vec![1], 0)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 1,
          },
          seq_range: SeqRange { start: 0, end: 0 },
          acceptance_fence: None,
          completion: None,
        },
        offset_datetime_from_unix_millis(1_000),
      );
    }
  }
  let topic_info = |name: &'static str| TopicInfo {
    name: name.into(),
    partition_count: 1,
    num_writers: 1,
    retention: Duration::days(7),
    max_metadata_publication_lag: Duration::seconds(15),
    metadata_window_size: Duration::minutes(5),
  };
  let topics = HashMap::from([
    ("first".into(), topic_info("first")),
    ("second".into(), topic_info("second")),
  ]);
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;

  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &config,
    None,
    &topics,
  );

  assert_eq!(plans.len(), 2);
  assert!(plans.iter().all(|plan| !plan.shared_blob));
  assert!(
    plans
      .iter()
      .all(|plan| { matches!(plan.topics[0].partitions[0].trigger, FlushTrigger::MaxBytes) })
  );
}

#[test]
fn shared_time_flushes_order_topics_by_name() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for topic in ["zebra", "alpha"] {
      state.partition_state_mut(topic, 0).buffer.push(
        BufferedBatch {
          records: vec![new_record(vec![1], 0)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 1,
          },
          seq_range: SeqRange { start: 0, end: 0 },
          acceptance_fence: None,
          completion: None,
        },
        offset_datetime_from_unix_millis(0),
      );
    }
  }
  let topic_info = |name: &'static str| TopicInfo {
    name: name.into(),
    partition_count: 1,
    num_writers: 1,
    retention: Duration::days(7),
    max_metadata_publication_lag: Duration::seconds(15),
    metadata_window_size: Duration::minutes(5),
  };
  let topics = HashMap::from([
    ("zebra".into(), topic_info("zebra")),
    ("alpha".into(), topic_info("alpha")),
  ]);
  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &WriteConfig::with_defaults(),
    None,
    &topics,
  );

  assert_eq!(plans.len(), 1);
  assert!(plans[0].shared_blob);
  assert_eq!(
    plans[0]
      .topics
      .iter()
      .map(|topic| topic.topic.as_str())
      .collect::<Vec<_>>(),
    ["alpha", "zebra"]
  );
}

#[test]
fn shared_time_flushes_group_all_topics_in_one_plan() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  let topic_info = |name: String| TopicInfo {
    name: name.into(),
    partition_count: 1,
    num_writers: 1,
    retention: Duration::days(7),
    max_metadata_publication_lag: Duration::seconds(15),
    metadata_window_size: Duration::minutes(5),
  };
  let topics = (0 .. 5)
    .map(|topic_index| {
      let topic = format!("topic-{topic_index}");
      {
        let mut state = state.lock();
        state.partition_state_mut(&topic, 0).buffer.push(
          BufferedBatch {
            records: vec![new_record(vec![1], 0)],
            summary: BatchSummary {
              record_count: 1,
              payload_bytes: 1,
            },
            seq_range: SeqRange { start: 0, end: 0 },
            acceptance_fence: None,
            completion: None,
          },
          offset_datetime_from_unix_millis(0),
        );
      }
      (topic.clone().into(), topic_info(topic))
    })
    .collect();
  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &WriteConfig::with_defaults(),
    None,
    &topics,
  );

  assert_eq!(plans.len(), 1);
  assert!(plans[0].shared_blob);
  assert_eq!(plans[0].topics.len(), 5);
}

#[test]
fn shared_time_flush_excludes_draining_peer_partitions() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for (topic, virtual_partition_id) in [("telemetry", 0), ("telemetry", 1), ("logs", 0)] {
      state
        .partition_state_mut(topic, virtual_partition_id)
        .buffer
        .push(
          BufferedBatch {
            records: vec![new_record(vec![1], 0)],
            summary: BatchSummary {
              record_count: 1,
              payload_bytes: 1,
            },
            seq_range: SeqRange { start: 0, end: 0 },
            acceptance_fence: None,
            completion: None,
          },
          offset_datetime_from_unix_millis(0),
        );
    }
    state.partition_state_mut("telemetry", 1).draining = true;
  }
  let topic_info = |name: &'static str| TopicInfo {
    name: name.into(),
    partition_count: 1,
    num_writers: 1,
    retention: Duration::days(7),
    max_metadata_publication_lag: Duration::seconds(15),
    metadata_window_size: Duration::minutes(5),
  };
  let topics = HashMap::from([
    ("telemetry".into(), topic_info("telemetry")),
    ("logs".into(), topic_info("logs")),
  ]);

  let plans = collect_all_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &WriteConfig::with_defaults(),
    None,
    &topics,
  );

  assert_eq!(plans.len(), 2);
  let shared_plan = plans.iter().find(|plan| plan.shared_blob).unwrap();
  assert_eq!(shared_plan.topics.len(), 2);
  assert_eq!(
    shared_plan
      .topics
      .iter()
      .find(|topic| topic.topic.as_str() == "telemetry")
      .unwrap()
      .partitions[0]
      .virtual_partition_id,
    0
  );
  let drain_plan = plans.iter().find(|plan| !plan.shared_blob).unwrap();
  assert_eq!(drain_plan.topics[0].partitions[0].virtual_partition_id, 1);
  assert!(matches!(
    drain_plan.topics[0].partitions[0].trigger,
    FlushTrigger::LeaseDrain
  ));
}
