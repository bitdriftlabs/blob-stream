use super::buffer::{
  FlushCompletionError,
  FlushPartition,
  FlushPartitionResult,
  FlushPlan,
  FlushPublicationCompletion,
  FlushPublicationDependency,
  FlushPublicationResult,
  FlushPublicationState,
  FlushTrigger,
  TopicFlushPlan,
};
use super::config::EffectiveFlushConfig;
use super::flush::{FlushContext, FlushPlanCompletion};
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
use tokio::sync::OwnedSemaphorePermit;

#[cfg(test)]
#[path = "./scheduler_test.rs"]
mod tests;

//
// FlushPlanFeedback
//

pub(super) struct FlushPlanFeedback {
  pub(super) split_count: u64,
  pub(super) successful: bool,
}

pub(super) async fn flush_plan_and_notify(
  flush_context: &FlushContext,
  mut plan: FlushPlan,
  metrics: &WriteMetrics,
  state: &Arc<Mutex<WriteState>>,
  _plan_permit: OwnedSemaphorePermit,
  blob_upload_permits: &Arc<tokio::sync::Semaphore>,
  metadata_write_permits: &Arc<tokio::sync::Semaphore>,
  flush_notifier: &Arc<tokio::sync::Notify>,
) -> FlushPlanFeedback {
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
  let result = flush_context
    .flush_plan_after(
      &mut plan,
      metrics,
      blob_upload_permits,
      metadata_write_permits,
      flush_notifier,
    )
    .await;
  let successful = result.as_ref().is_ok_and(|completion| {
    completion
      .partition_results
      .iter()
      .all(|result| result.error.is_none())
  });
  if !successful {
    metrics.flush_failures_total.inc();
  }
  metrics
    .flush_latency_seconds
    .observe(flush_started.elapsed().as_secs_f64());
  let fallback_error = result
    .as_ref()
    .err()
    .map_or(FlushCompletionError::Internal, |error| match error {
      super::WriteError::LeaseFenceLost => FlushCompletionError::LeaseFenceLost,
      _ => FlushCompletionError::Internal,
    });
  let FlushPlanCompletion {
    partition_results,
    split_count,
  } = result.unwrap_or(FlushPlanCompletion {
    partition_results: Vec::new(),
    split_count: 0,
  });
  mark_flush_complete(
    state,
    plan.publication_completions,
    &partition_results,
    fallback_error,
  );
  for (topic, virtual_partition_id, completion) in completions {
    let error = partition_result_error(
      &partition_results,
      &topic,
      virtual_partition_id,
      fallback_error,
    );
    let completion_result = error.map_or(Ok(()), Err);
    let _ignored = completion.send(completion_result);
  }

  FlushPlanFeedback {
    split_count,
    successful,
  }
}

fn mark_flush_complete(
  state: &Arc<Mutex<WriteState>>,
  publication_completions: Vec<FlushPublicationCompletion>,
  partition_results: &[FlushPartitionResult],
  fallback_error: FlushCompletionError,
) {
  let mut drain_notifiers = Vec::new();
  {
    let mut state = state.lock();
    for completion in publication_completions {
      let error = partition_result_error(
        partition_results,
        &completion.topic,
        completion.virtual_partition_id,
        fallback_error,
      );
      completion
        .state_tx
        .send_replace(FlushPublicationState::Completed(error.map_or(
          FlushPublicationResult::Succeeded,
          FlushPublicationResult::Failed,
        )));
      let Some(partition_state) = state
        .partition_state_mut_if_present(completion.topic.as_str(), completion.virtual_partition_id)
      else {
        continue;
      };
      debug_assert!(partition_state.outstanding_flushes > 0);
      partition_state.outstanding_flushes = partition_state.outstanding_flushes.saturating_sub(1);
      if partition_state
        .publication_tail
        .as_ref()
        .is_some_and(|tail| {
          matches!(
            *tail.state_rx.borrow(),
            FlushPublicationState::Completed(FlushPublicationResult::Failed(_))
          )
        })
      {
        // A retry must begin a new publication chain after the failed chain has unwound. A
        // successor replaces the tail before it can observe its predecessor's failure, so this
        // only clears the tail after the final dependent plan reaches its terminal result.
        partition_state.publication_tail = None;
      }
      trace!(
        "broker flush completion updated partition state: topic={topic}, \
         virtual_partition_id={virtual_partition_id}, \
         allocation_in_flight={allocation_in_flight}, buffered_batches={buffered_batches}, \
         outstanding_flushes={outstanding_flushes}, draining={draining}",
        topic = completion.topic,
        virtual_partition_id = completion.virtual_partition_id,
        allocation_in_flight = partition_state.allocation_in_flight,
        buffered_batches = partition_state.buffer.batches.len(),
        outstanding_flushes = partition_state.outstanding_flushes,
        draining = partition_state.draining,
      );
      drain_notifiers.push(Arc::clone(&partition_state.drain_notify));
    }
  }
  for drain_notify in drain_notifiers {
    trace!("broker flush completion notifying partition drain waiter");
    drain_notify.notify_waiters();
  }
}

fn partition_result_error(
  partition_results: &[FlushPartitionResult],
  topic: &Chars,
  virtual_partition_id: blob_stream_types::VirtualPartitionId,
  fallback_error: FlushCompletionError,
) -> Option<FlushCompletionError> {
  partition_results
    .iter()
    .find(|result| result.topic == *topic && result.virtual_partition_id == virtual_partition_id)
    .map_or(Some(fallback_error), |result| result.error)
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
  // Each epoch advances through one state machine. Its successor may begin assigning identities
  // at `SegmentIdentityAssigned`, but it cannot publish metadata until `Completed`.
  let (publication_state_tx, publication_state_rx) =
    tokio::sync::watch::channel(FlushPublicationState::Pending);
  let publication_predecessor = partition_state.publication_tail.clone();
  partition_state.publication_tail = Some(FlushPublicationDependency {
    state_rx: publication_state_rx,
  });
  partition_state.outstanding_flushes = partition_state.outstanding_flushes.saturating_add(1);
  Some(FlushPartition {
    virtual_partition_id,
    lease_fence,
    batches,
    trigger,
    publication_predecessor,
    publication_state_tx: Some(publication_state_tx),
  })
}

fn flush_trigger(
  partition_state: &PartitionState,
  now: time::OffsetDateTime,
  flush_config: &EffectiveFlushConfig,
) -> Option<FlushTrigger> {
  if partition_state.draining {
    Some(FlushTrigger::LeaseDrain)
  } else {
    partition_state
      .buffer
      .is_time_due(now, flush_config)
      .then_some(FlushTrigger::MaxDelay)
      .or_else(|| partition_state.buffer.flush_trigger(now, flush_config))
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
      if (matches!(trigger, FlushTrigger::MaxDelay) && partition_state.draining)
        || partition_state
          .publication_tail
          .as_ref()
          .is_some_and(|tail| matches!(*tail.state_rx.borrow(), FlushPublicationState::Pending))
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

pub(super) fn collect_next_flush_plan(
  state: &Arc<Mutex<WriteState>>,
  now: time::OffsetDateTime,
  config: &WriteConfig,
  flush_config: &EffectiveFlushConfig,
  feature_flags: Option<&FeatureFlagsWatch>,
  topics: &HashMap<Chars, TopicInfo>,
) -> Option<FlushPlan> {
  // Planning atomically removes batches from the buffers and marks them in flight. Hold the write
  // state lock for the entire selection so a batch belongs to exactly one plan, even if another
  // scheduler pass starts while this pass is constructing its output.
  let fenced_metadata_writes = config.fenced_metadata_writes(feature_flags);
  let mut state = state.lock();

  // Most scheduler passes are timer ticks or sub-threshold write notifications. Avoid cloning and
  // sorting all partition keys unless at least one partition has work to flush. The same scan finds
  // a time-due trigger for cross-topic promotion without allocating a temporary key collection.
  let mut has_flushable_partition = false;
  let mut shared_time_flush = false;
  'topics: for topic_state in state.topics.values() {
    for partition_state in topic_state.partitions.values() {
      let trigger = flush_trigger(partition_state, now, flush_config);
      has_flushable_partition |= trigger.is_some();
      shared_time_flush |= matches!(trigger, Some(FlushTrigger::MaxDelay));
      if has_flushable_partition && shared_time_flush {
        break 'topics;
      }
    }
  }
  if !has_flushable_partition {
    return None;
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

  // The scheduler acquires one permit before calling this function, so this pass builds exactly
  // one durable plan. A shared time plan may include a single compatible section per topic.
  let mut topic_plans = HashMap::<Chars, TopicFlushPlan>::new();
  // This controls only which topic sections may join the same scheduler pass. Flush assembly
  // stores every resulting object under the shared blob namespace.
  let mut selected_shared_time_flush = None;
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

    // Once any local partition has reached its latency limit, buffered peers join that same shared
    // flush. They receive a MaxDelay trigger deliberately: later assembly groups only
    // time-triggered topic plans into the shared object.
    let trigger = state
      .partition_state(topic.as_str(), *virtual_partition_id)
      .and_then(|partition_state| {
        if shared_time_flush
          && !partition_state.draining
          && !partition_state.buffer.batches.is_empty()
        {
          Some(FlushTrigger::MaxDelay)
        } else {
          flush_trigger(partition_state, now, flush_config)
        }
      });
    let Some(trigger) = trigger else {
      continue;
    };
    let existing_topic_plan = topic_plans.get(topic);
    let existing_partition_count = existing_topic_plan.map_or(0, |plan| plan.partitions.len());
    let existing_trigger = existing_topic_plan
      .and_then(|plan| plan.partitions.first())
      .map(|partition| &partition.trigger);

    // Different triggers cannot share a topic section. Fenced metadata writes add a second
    // boundary: each durable metadata transaction can check at most 99 leases.
    let requires_new_topic_plan = existing_trigger.is_none_or(|existing_trigger| {
      *existing_trigger != trigger
        || (fenced_metadata_writes && existing_partition_count == MAX_FENCED_METADATA_PARTITIONS)
    });
    let shared_time_plan = matches!(trigger, FlushTrigger::MaxDelay) && requires_new_topic_plan;
    let joins_selected_plan = selected_shared_time_flush.is_none_or(|shared_time_flush| {
      if shared_time_flush {
        // A shared plan may add another topic, or more partitions for a topic already in its
        // current chunk. A second chunk for that topic requires a later scheduler pass.
        shared_time_plan && (!requires_new_topic_plan || existing_topic_plan.is_none())
      } else {
        !requires_new_topic_plan
      }
    });
    if !joins_selected_plan {
      continue;
    }

    let max_partitions = if fenced_metadata_writes {
      if requires_new_topic_plan {
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
    if requires_new_topic_plan {
      let topic_info = topics
        .get(topic.as_str())
        .expect("flush plans are created only for configured topics");
      topic_plans.insert(
        topic.clone(),
        TopicFlushPlan {
          max_metadata_publication_lag: topic_info.max_metadata_publication_lag,
          metadata_window_size: topic_info.metadata_window_size,
          topic: topic.clone(),
          partitions,
          fenced_metadata_writes,
        },
      );
      selected_shared_time_flush.get_or_insert(shared_time_plan);

      // Only advance fairness after this pass has claimed work. Skipped partitions leave the
      // cursor unchanged, so a later selection can resume from the same eligible work.
      last_planned_topic = Some(topic.clone());
    } else {
      topic_plans
        .get_mut(topic)
        .expect("existing topic section remains available")
        .partitions
        .extend(partitions);
    }
  }

  if let Some(last_planned_topic) = last_planned_topic {
    state.last_flush_topic = Some(last_planned_topic);
  }

  selected_shared_time_flush?;
  let mut topics = topic_plans.into_values().collect::<Vec<_>>();
  topics.sort_by(|left, right| left.topic.as_str().cmp(right.topic.as_str()));
  let mut publication_completions = Vec::new();
  for topic_plan in &mut topics {
    for partition in &mut topic_plan.partitions {
      let state_tx = partition
        .publication_state_tx
        .take()
        .expect("selected flush partition has a publication state sender");
      publication_completions.push(FlushPublicationCompletion {
        topic: topic_plan.topic.clone(),
        virtual_partition_id: partition.virtual_partition_id,
        state_tx,
      });
    }
  }
  Some(FlushPlan {
    topics,
    max_segment_bytes: config.max_segment_bytes(feature_flags),
    publication_completions,
  })
}

pub(super) fn begin_shutdown_drain(state: &Arc<Mutex<WriteState>>) {
  let mut state = state.lock();
  for topic_state in state.topics.values_mut() {
    for partition_state in topic_state.partitions.values_mut() {
      partition_state.draining = true;
    }
  }
}
