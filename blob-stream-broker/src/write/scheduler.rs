use super::buffer::{FlushPartition, FlushPlan, FlushTrigger};
use super::flush::FlushContext;
use super::metrics::WriteMetrics;
use super::state::WriteState;
use super::{TopicInfo, WriteConfig};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use time::OffsetDateTime;

pub(super) async fn flush_plan_and_notify(
  flush_context: &FlushContext,
  mut plan: FlushPlan,
  now: OffsetDateTime,
  metrics: &WriteMetrics,
  state: &Arc<Mutex<WriteState>>,
) {
  let flushed_partitions: Vec<_> = plan
    .partitions
    .iter()
    .map(|partition| partition.virtual_partition_id)
    .collect();
  let mut completions = Vec::new();
  for partition in &mut plan.partitions {
    for batch in &mut partition.batches {
      if let Some(completion) = batch.completion.take() {
        completions.push(completion);
      }
    }
  }

  let flush_started = Instant::now();
  let result = flush_context.flush_plan(&mut plan, now, metrics).await;
  let topic = plan.topic;
  if result.is_err() {
    metrics.flush_failures_total.inc();
  }
  metrics
    .flush_latency_seconds
    .observe(flush_started.elapsed().as_secs_f64());
  mark_flush_complete(state, &topic, &flushed_partitions);
  let completion_result = result
    .as_ref()
    .map_or_else(|error| Err(error.to_string()), |_ok| Ok(()));
  for completion in completions {
    let _ignored = completion.send(completion_result.clone());
  }
}

fn mark_flush_complete(
  state: &Arc<Mutex<WriteState>>,
  topic: &str,
  virtual_partition_ids: &[blob_stream_types::VirtualPartitionId],
) {
  let mut drain_notifiers = Vec::new();
  {
    let mut state = state.lock();
    for virtual_partition_id in virtual_partition_ids {
      let Some(partition_state) =
        state.partition_state_mut_if_present(topic, *virtual_partition_id)
      else {
        continue;
      };
      partition_state.flush_in_flight = false;
      drain_notifiers.push(Arc::clone(&partition_state.drain_notify));
    }
  }
  for drain_notify in drain_notifiers {
    drain_notify.notify_waiters();
  }
}

pub(super) fn collect_flush_plans(
  state: &Arc<Mutex<WriteState>>,
  now_ts_ms: i64,
  config: &WriteConfig,
  topics: &HashMap<String, TopicInfo>,
  max_plans: usize,
) -> Vec<FlushPlan> {
  if max_plans == 0 {
    return Vec::new();
  }
  let mut state = state.lock();
  let mut partition_keys = state.partition_keys();
  partition_keys.sort_unstable();
  let last_flush_topic = state.last_flush_topic.clone();
  let start = last_flush_topic.as_ref().map_or(0, |last_topic| {
    partition_keys
      .iter()
      .position(|(topic, _)| topic > last_topic)
      .unwrap_or(0)
  });
  let partition_state_count = partition_keys.len();
  let mut plans_by_topic: HashMap<String, Vec<FlushPartition>> = HashMap::new();
  let mut last_planned_topic = None;

  for (topic, virtual_partition_id) in partition_keys
    .iter()
    .cycle()
    .skip(start)
    .take(partition_state_count)
  {
    topics
      .get(topic)
      .expect("write state is created only for configured topics");
    let is_new_topic = !plans_by_topic.contains_key(topic);
    if is_new_topic && plans_by_topic.len() == max_plans {
      continue;
    }
    let Some(partition_state) = state.partition_state_mut_if_present(topic, *virtual_partition_id)
    else {
      continue;
    };
    if partition_state.flush_in_flight {
      continue;
    }
    let flush_trigger = if partition_state.draining {
      Some(FlushTrigger::LeaseDrain)
    } else {
      partition_state.buffer.flush_trigger(now_ts_ms, config)
    };
    let Some(flush_trigger) = flush_trigger else {
      continue;
    };

    let batches = std::mem::take(&mut partition_state.buffer.batches);
    if batches.is_empty() {
      partition_state.buffer.reset();
      continue;
    }

    partition_state.buffer.reset();
    partition_state.flush_in_flight = true;
    plans_by_topic
      .entry(topic.clone())
      .or_default()
      .push(FlushPartition {
        virtual_partition_id: *virtual_partition_id,
        batches,
        trigger: flush_trigger,
      });
    if is_new_topic {
      last_planned_topic = Some(topic.clone());
    }
  }

  if let Some(last_planned_topic) = last_planned_topic {
    state.last_flush_topic = Some(last_planned_topic);
  }

  plans_by_topic
    .into_iter()
    .map(|(topic, partitions)| FlushPlan {
      max_metadata_publication_lag_ms: topics
        .get(&topic)
        .expect("flush plans are created only for configured topics")
        .max_metadata_publication_lag_ms,
      topic,
      partitions,
    })
    .collect()
}

pub(super) fn begin_shutdown_drain(state: &Arc<Mutex<WriteState>>) {
  let mut state = state.lock();
  for topic_state in state.topics.values_mut() {
    for partition_state in topic_state.partitions.values_mut() {
      partition_state.draining = true;
    }
  }
}
