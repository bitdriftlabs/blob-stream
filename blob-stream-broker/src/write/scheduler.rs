use super::buffer::{
  FlushCompletionError,
  FlushPartition,
  FlushPartitionResult,
  FlushPlan,
  FlushTrigger,
  TopicFlushPlan,
};
use super::flush::FlushContext;
use super::metrics::WriteMetrics;
use super::state::{PartitionState, WriteState};
use super::{TopicInfo, WriteConfig};
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use blob_stream_metadata_store::MAX_FENCED_METADATA_PARTITIONS;
use log::trace;
use parking_lot::Mutex;
use protobuf::Chars;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
#[path = "./scheduler_test.rs"]
mod tests;

pub(super) async fn flush_plan_and_notify(
  flush_context: &FlushContext,
  mut plan: FlushPlan,
  metrics: &WriteMetrics,
  state: &Arc<Mutex<WriteState>>,
) {
  let flushed_partitions: Vec<_> = plan
    .topics
    .iter()
    .map(|topic_plan| {
      (
        topic_plan.topic.clone(),
        topic_plan
          .partitions
          .iter()
          .map(|partition| partition.virtual_partition_id)
          .collect::<Vec<_>>(),
      )
    })
    .collect();
  let mut completions = Vec::new();
  for topic_plan in &mut plan.topics {
    for partition in &mut topic_plan.partitions {
      for batch in &mut partition.batches {
        if let Some(completion) = batch.completion.take() {
          completions.push((
            topic_plan.topic.clone(),
            partition.virtual_partition_id,
            completion,
          ));
        }
      }
    }
  }

  let flush_started = Instant::now();
  let result = flush_context.flush_plan(&mut plan, metrics).await;
  if result.as_ref().is_err()
    || result
      .as_ref()
      .is_ok_and(|results| results.iter().any(|result| result.error.is_some()))
  {
    metrics.flush_failures_total.inc();
  }
  metrics
    .flush_latency_seconds
    .observe(flush_started.elapsed().as_secs_f64());
  for (topic, virtual_partition_ids) in flushed_partitions {
    mark_flush_complete(state, &topic, &virtual_partition_ids);
  }
  let fallback_error = result
    .as_ref()
    .err()
    .map_or(FlushCompletionError::Internal, |error| match error {
      super::WriteError::LeaseFenceLost => FlushCompletionError::LeaseFenceLost,
      _ => FlushCompletionError::Internal,
    });
  let partition_results = result.unwrap_or_default();
  for (topic, virtual_partition_id, completion) in completions {
    let error = partition_results
      .iter()
      .find(|result: &&FlushPartitionResult| {
        result.topic == topic && result.virtual_partition_id == virtual_partition_id
      })
      .map_or(Some(fallback_error), |result| result.error);
    let completion_result = error.map_or(Ok(()), Err);
    let _ignored = completion.send(completion_result);
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
  require_fence: bool,
) -> Option<FlushPartition> {
  let mut batches = std::mem::take(&mut partition_state.buffer.batches);
  if batches.is_empty() {
    partition_state.buffer.reset();
    return None;
  }

  let lease_fence = batches
    .first()
    .and_then(|batch| batch.acceptance_fence.clone());
  let matching_fences = batches
    .iter()
    .all(|batch| batch.acceptance_fence == lease_fence);
  if require_fence && (lease_fence.is_none() || !matching_fences) {
    partition_state.buffer.reset();
    for batch in &mut batches {
      if let Some(completion) = batch.completion.take() {
        let _ignored = completion.send(Err(FlushCompletionError::LeaseFenceLost));
      }
    }
    return None;
  }

  partition_state.buffer.reset();
  partition_state.flush_in_flight = true;
  Some(FlushPartition {
    virtual_partition_id,
    lease_fence,
    batches,
    trigger,
  })
}

fn flush_trigger(
  partition_state: &PartitionState,
  now: time::OffsetDateTime,
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
      .is_time_due(now, config)
      .then_some(FlushTrigger::MaxDelay)
      .or_else(|| partition_state.buffer.flush_trigger(now, config))
  }
}

fn take_flush_partitions(
  state: &mut WriteState,
  topic: &Chars,
  virtual_partition_ids: impl IntoIterator<Item = blob_stream_types::VirtualPartitionId>,
  trigger: FlushTrigger,
  max_partitions: usize,
  require_fence: bool,
) -> Vec<FlushPartition> {
  virtual_partition_ids
    .into_iter()
    .filter_map(|virtual_partition_id| {
      let partition_state =
        state.partition_state_mut_if_present(topic.as_str(), virtual_partition_id)?;
      if partition_state.flush_in_flight
        || (matches!(trigger, FlushTrigger::MaxDelay) && partition_state.draining)
      {
        None
      } else {
        take_flush_partition(
          partition_state,
          virtual_partition_id,
          trigger,
          require_fence,
        )
      }
    })
    .take(max_partitions)
    .collect()
}

pub(super) fn collect_flush_plans(
  state: &Arc<Mutex<WriteState>>,
  now: time::OffsetDateTime,
  config: &WriteConfig,
  feature_flags: Option<&FeatureFlagsWatch>,
  topics: &HashMap<Chars, TopicInfo>,
  max_plans: usize,
) -> Vec<FlushPlan> {
  if max_plans == 0 {
    return Vec::new();
  }

  // Planning atomically removes batches from the buffers and marks them in flight. Hold the write
  // state lock for the entire selection so a batch belongs to exactly one plan, even if another
  // scheduler pass starts while this pass is constructing its output.
  let fenced_metadata_writes = config.fenced_metadata_writes(feature_flags);
  let shared_cross_topic_blobs = WriteConfig::shared_cross_topic_blobs(feature_flags);
  let mut state = state.lock();

  // Most scheduler passes are timer ticks or sub-threshold write notifications. Avoid cloning and
  // sorting all partition keys unless at least one partition has work to flush. The same scan finds
  // a time-due trigger for cross-topic promotion without allocating a temporary key collection.
  let mut has_flushable_partition = false;
  let mut shared_time_flush = false;
  'topics: for topic_state in state.topics.values() {
    for partition_state in topic_state.partitions.values() {
      let trigger = flush_trigger(partition_state, now, config);
      has_flushable_partition |= trigger.is_some();
      shared_time_flush |=
        shared_cross_topic_blobs && matches!(trigger, Some(FlushTrigger::MaxDelay));
      if has_flushable_partition && (!shared_cross_topic_blobs || shared_time_flush) {
        break 'topics;
      }
    }
  }
  if !has_flushable_partition {
    return Vec::new();
  }

  let mut partition_keys = state.partition_keys();
  partition_keys.sort_unstable();

  // Keep the global ordering stable for fair round-robin iteration below, but also index all of a
  // topic's partitions. A time-triggered flush takes the full topic group in one operation.
  let mut partition_ids_by_topic = HashMap::new();
  for (topic, virtual_partition_id) in &partition_keys {
    partition_ids_by_topic
      .entry(topic.clone())
      .or_insert_with(Vec::new)
      .push(*virtual_partition_id);
  }
  // Resume just after the topic that most recently consumed a durable plan. This rotates priority
  // between topics while still allowing every partition to be examined exactly once per pass.
  let start = state.last_flush_topic.as_ref().map_or(0, |last_topic| {
    partition_keys
      .iter()
      .position(|(topic, _)| topic > last_topic)
      .unwrap_or(0)
  });
  let partition_state_count = partition_keys.len();

  // These parallel maps describe logical topic plans before they are assembled into physical
  // objects. A topic can have multiple entries when fenced metadata writes force its partitions
  // into DynamoDB-transaction-sized chunks.
  let mut plans_by_topic: HashMap<Chars, Vec<Vec<FlushPartition>>> = HashMap::new();
  let mut plan_triggers_by_topic: HashMap<Chars, Vec<FlushTrigger>> = HashMap::new();

  // A shared object can contain the nth chunk from each topic. Track each topic's chunk count so
  // all first chunks share one plan, all second chunks share another, and so on.
  // `durable_plan_count` counts those physical shared objects rather than every logical per-topic
  // chunk.
  let mut shared_time_chunks_by_topic = HashMap::new();
  let mut durable_plan_count: usize = 0;
  let mut shared_time_plan_count: usize = 0;
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

    // Once any local partition has reached its latency limit, the feature flag permits buffered
    // peers to join that same shared flush. They receive a MaxDelay trigger deliberately: later
    // assembly groups only time-triggered topic plans into the shared object.
    let trigger = state
      .partition_state(topic.as_str(), *virtual_partition_id)
      .and_then(|partition_state| {
        if shared_time_flush
          && !partition_state.flush_in_flight
          && !partition_state.draining
          && !partition_state.buffer.batches.is_empty()
        {
          Some(FlushTrigger::MaxDelay)
        } else {
          flush_trigger(partition_state, now, config)
        }
      });
    let Some(trigger) = trigger else {
      continue;
    };
    let existing_partition_count = plans_by_topic
      .get(topic)
      .and_then(|plans| plans.last())
      .map_or(0, Vec::len);
    let existing_trigger = plan_triggers_by_topic
      .get(topic)
      .and_then(|triggers| triggers.last());

    // Different triggers cannot share a topic plan. Fenced metadata writes add a second boundary:
    // each durable metadata transaction can check at most MAX_FENCED_METADATA_PARTITIONS leases.
    let requires_new_plan = existing_trigger.is_none_or(|existing| *existing != trigger)
      || (fenced_metadata_writes && existing_partition_count == MAX_FENCED_METADATA_PARTITIONS);
    let shared_time_plan =
      shared_cross_topic_blobs && matches!(trigger, FlushTrigger::MaxDelay) && requires_new_plan;
    let additional_durable_plans = if shared_time_plan {
      let topic_chunks = shared_time_chunks_by_topic.get(topic).copied().unwrap_or(0);
      // Joining an existing nth shared chunk is free. Starting a new nth chunk requires another
      // physical object, so it consumes one of the scheduler's available durable-plan slots.
      usize::from(topic_chunks == shared_time_plan_count)
    } else {
      usize::from(requires_new_plan)
    };
    if durable_plan_count.saturating_add(additional_durable_plans) > max_plans {
      continue;
    }

    let max_partitions = if fenced_metadata_writes {
      if requires_new_plan {
        MAX_FENCED_METADATA_PARTITIONS
      } else {
        MAX_FENCED_METADATA_PARTITIONS.saturating_sub(existing_partition_count)
      }
    } else {
      usize::MAX
    };

    // Time-triggered work consumes every eligible partition for its topic so it establishes a
    // shared cadence. Byte and drain triggers intentionally take only their selected partition:
    // they must not pull unrelated fresh work forward.
    let partitions = match trigger {
      // A normal time-due flush establishes a cadence for its topic. When the shared pass promoted
      // peer topics above, each of their topic groups follows the same path to form one object.
      FlushTrigger::MaxDelay => take_flush_partitions(
        &mut state,
        topic,
        partition_ids_by_topic
          .get(topic)
          .expect("partition keys are grouped by topic")
          .iter()
          .copied(),
        FlushTrigger::MaxDelay,
        max_partitions,
        fenced_metadata_writes,
      ),
      FlushTrigger::MaxBytes | FlushTrigger::LeaseDrain => take_flush_partitions(
        &mut state,
        topic,
        iter::once(*virtual_partition_id),
        trigger,
        max_partitions,
        fenced_metadata_writes,
      ),
    };
    if partitions.is_empty() {
      continue;
    }
    let plans = plans_by_topic.entry(topic.clone()).or_default();
    if requires_new_plan {
      plans.push(Vec::new());
      plan_triggers_by_topic
        .entry(topic.clone())
        .or_default()
        .push(trigger);
      if shared_time_plan {
        *shared_time_chunks_by_topic
          .entry(topic.clone())
          .or_default() += 1;
        shared_time_plan_count = shared_time_plan_count.max(
          shared_time_chunks_by_topic
            .get(topic)
            .copied()
            .expect("shared time chunk count was inserted"),
        );
      }
      durable_plan_count = durable_plan_count.saturating_add(additional_durable_plans);

      // Only advance fairness after a plan actually claims capacity. Skipped partitions leave the
      // cursor unchanged, so exhaustion of max_plans does not make them look as though they ran.
      last_planned_topic = Some(topic.clone());
    }
    plans
      .last_mut()
      .expect("new or existing flush plan is available")
      .extend(partitions);
  }

  if let Some(last_planned_topic) = last_planned_topic {
    state.last_flush_topic = Some(last_planned_topic);
  }

  // Convert the mutable planning representation into immutable topic sections. Sort explicitly:
  // HashMap iteration is nondeterministic, whereas deterministic topic order stabilizes object
  // construction, tests, and the mapping from fenced chunks to shared objects.
  let mut topic_plans = plans_by_topic
    .into_iter()
    .flat_map(|(topic, plans)| {
      let topic_info = topics
        .get(topic.as_str())
        .expect("flush plans are created only for configured topics");
      let max_metadata_publication_lag = topic_info.max_metadata_publication_lag;
      let metadata_window_size = topic_info.metadata_window_size;
      plans.into_iter().map(move |partitions| TopicFlushPlan {
        max_metadata_publication_lag,
        metadata_window_size,
        topic: topic.clone(),
        partitions,
        fenced_metadata_writes,
      })
    })
    .collect::<Vec<_>>();
  topic_plans.sort_by(|left, right| left.topic.as_str().cmp(right.topic.as_str()));
  let max_segment_bytes = config.max_segment_bytes(feature_flags);
  if !shared_cross_topic_blobs {
    // The default mode preserves one storage object per topic section.
    return topic_plans
      .into_iter()
      .map(|topic| FlushPlan {
        topics: vec![topic],
        max_segment_bytes,
        shared_blob: false,
      })
      .collect();
  }

  // A time-due local partition has reached its latency bound, so its shared pass may amortize
  // object uploads across buffered peer topics. Independently size-triggered and drain work stays
  // local: pulling peer buffers forward would create smaller segments, couple their metadata
  // deadlines, and hold a flush slot while bounded objects persist sequentially.
  // TODO: Evaluate a bounded eligible-work policy that can share across trigger types. Benchmark
  // S3 PUTs, DynamoDB metadata writes, producer latency, and flush-slot occupancy, while retaining
  // per-topic metadata deadlines and the fenced-partition transaction limit.
  let (time_triggered, local) = topic_plans
    .into_iter()
    .partition::<Vec<_>, _>(TopicFlushPlan::is_time_triggered);
  let mut plans = local
    .into_iter()
    .map(|topic| FlushPlan {
      topics: vec![topic],
      max_segment_bytes,
      shared_blob: false,
    })
    .collect::<Vec<_>>();
  let mut shared_plans = Vec::new();
  for topic in time_triggered {
    // Fenced topic plans are split at DynamoDB's transaction limit. Keep those chunks in
    // distinct shared plans so object construction cannot merge their fence sets back together.
    // The first compatible shared plan is the same numbered chunk from previously sorted topics.
    if let Some(plan) = shared_plans.iter_mut().find(|plan: &&mut FlushPlan| {
      plan
        .topics
        .iter()
        .all(|existing| existing.topic != topic.topic)
    }) {
      plan.topics.push(topic);
    } else {
      shared_plans.push(FlushPlan {
        topics: vec![topic],
        max_segment_bytes,
        shared_blob: true,
      });
    }
  }
  plans.extend(shared_plans);
  plans
}

pub(super) fn begin_shutdown_drain(state: &Arc<Mutex<WriteState>>) {
  let mut state = state.lock();
  for topic_state in state.topics.values_mut() {
    for partition_state in topic_state.partitions.values_mut() {
      partition_state.draining = true;
    }
  }
}
