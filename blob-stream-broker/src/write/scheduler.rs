use super::buffer::{FlushPartition, FlushPlan, FlushTrigger};
use super::flush::FlushContext;
use super::metrics::WriteMetrics;
use super::state::{PartitionState, WriteState};
use super::{TopicInfo, WriteConfig};
use log::trace;
use parking_lot::Mutex;
use protobuf::Chars;
use std::collections::HashMap;
use std::iter;
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
  topic: &Chars,
  virtual_partition_ids: &[blob_stream_types::VirtualPartitionId],
) {
  let mut drain_notifiers = Vec::new();
  {
    let mut state = state.lock();
    for virtual_partition_id in virtual_partition_ids {
      let Some(partition_state) =
        state.partition_state_mut_if_present(topic.as_str(), *virtual_partition_id)
      else {
        continue;
      };
      partition_state.flush_in_flight = false;
      trace!(
        "broker flush completion updated partition state: topic={topic}, \
         virtual_partition_id={virtual_partition_id}, allocation_in_flight={}, \
         buffered_batches={}, draining={}",
        partition_state.allocation_in_flight,
        partition_state.buffer.batches.len(),
        partition_state.draining,
      );
      drain_notifiers.push(Arc::clone(&partition_state.drain_notify));
    }
  }
  for drain_notify in drain_notifiers {
    trace!("broker flush completion notifying partition drain waiter: topic={topic}");
    drain_notify.notify_waiters();
  }
}

fn take_flush_partition(
  partition_state: &mut PartitionState,
  virtual_partition_id: blob_stream_types::VirtualPartitionId,
  trigger: FlushTrigger,
) -> Option<FlushPartition> {
  let batches = std::mem::take(&mut partition_state.buffer.batches);
  if batches.is_empty() {
    partition_state.buffer.reset();
    return None;
  }

  partition_state.buffer.reset();
  partition_state.flush_in_flight = true;
  Some(FlushPartition {
    virtual_partition_id,
    batches,
    trigger,
  })
}

fn flush_trigger(
  partition_state: &PartitionState,
  now_ts_ms: i64,
  config: &WriteConfig,
) -> Option<FlushTrigger> {
  if partition_state.flush_in_flight {
    return None;
  }
  if partition_state.draining {
    Some(FlushTrigger::LeaseDrain)
  } else {
    partition_state
      .buffer
      .is_time_due(now_ts_ms, config)
      .then_some(FlushTrigger::MaxDelay)
      .or_else(|| partition_state.buffer.flush_trigger(now_ts_ms, config))
  }
}

fn take_flush_partitions(
  state: &mut WriteState,
  topic: &Chars,
  virtual_partition_ids: impl IntoIterator<Item = blob_stream_types::VirtualPartitionId>,
  trigger: FlushTrigger,
) -> Vec<FlushPartition> {
  virtual_partition_ids
    .into_iter()
    .filter_map(|virtual_partition_id| {
      let partition_state =
        state.partition_state_mut_if_present(topic.as_str(), virtual_partition_id)?;
      if partition_state.flush_in_flight {
        None
      } else {
        take_flush_partition(partition_state, virtual_partition_id, trigger)
      }
    })
    .collect()
}

pub(super) fn collect_flush_plans(
  state: &Arc<Mutex<WriteState>>,
  now_ts_ms: i64,
  config: &WriteConfig,
  topics: &HashMap<Chars, TopicInfo>,
  max_plans: usize,
) -> Vec<FlushPlan> {
  if max_plans == 0 {
    return Vec::new();
  }
  let mut state = state.lock();
  let mut partition_keys = state.partition_keys();
  partition_keys.sort_unstable();
  let mut partition_ids_by_topic = HashMap::new();
  for (topic, virtual_partition_id) in &partition_keys {
    partition_ids_by_topic
      .entry(topic.clone())
      .or_insert_with(Vec::new)
      .push(*virtual_partition_id);
  }
  let last_flush_topic = state.last_flush_topic.clone();
  let start = last_flush_topic.as_ref().map_or(0, |last_topic| {
    partition_keys
      .iter()
      .position(|(topic, _)| topic > last_topic)
      .unwrap_or(0)
  });
  let partition_state_count = partition_keys.len();
  let mut plans_by_topic: HashMap<Chars, Vec<FlushPartition>> = HashMap::new();
  let mut last_planned_topic = None;

  for (topic, virtual_partition_id) in partition_keys
    .iter()
    .cycle()
    .skip(start)
    .take(partition_state_count)
  {
    topics
      .get(topic.as_str())
      .expect("write state is created only for configured topics");
    let is_new_topic = !plans_by_topic.contains_key(topic);
    if is_new_topic && plans_by_topic.len() == max_plans {
      continue;
    }
    let flush_trigger = state
      .partition_state(topic.as_str(), *virtual_partition_id)
      .and_then(|partition_state| flush_trigger(partition_state, now_ts_ms, config));
    let Some(flush_trigger) = flush_trigger else {
      continue;
    };

    let partitions = match flush_trigger {
      // A topic's first time-due partition establishes its flush cadence. Pulling its available
      // peers forward produces one larger segment rather than retaining their startup skew.
      FlushTrigger::MaxDelay => take_flush_partitions(
        &mut state,
        topic,
        partition_ids_by_topic
          .get(topic)
          .expect("partition keys are grouped by topic")
          .iter()
          .copied(),
        FlushTrigger::MaxDelay,
      ),
      FlushTrigger::MaxBytes | FlushTrigger::LeaseDrain => take_flush_partitions(
        &mut state,
        topic,
        iter::once(*virtual_partition_id),
        flush_trigger,
      ),
    };
    if partitions.is_empty() {
      continue;
    }
    plans_by_topic
      .entry(topic.clone())
      .or_default()
      .extend(partitions);
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
        .get(topic.as_str())
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
