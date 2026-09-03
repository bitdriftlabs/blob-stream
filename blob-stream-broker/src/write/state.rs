use super::buffer::{BufferState, FlushPublicationDependency};
use blob_stream_broker_discovery::BrokerMembership;
use blob_stream_metadata_store::{LeaseReleaseSequenceProgress, ProducerLeaseFence};
use blob_stream_types::{SeqRange, VirtualPartitionId};
use log::debug;
use protobuf::Chars;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::Notify;

//
// WriteState
//

#[derive(Debug, Default)]
pub(super) struct WriteState {
  pub(super) membership: BrokerMembership,
  // Membership updates and allocation transitions share this mutex. Keeping authorization here
  // makes the decision to admit a request atomic with creating or mutating its partition state.
  assignment: LocalAssignment,
  pub(super) topics: HashMap<Chars, TopicState>,
  pub(super) last_flush_topic: Option<Chars>,
}

impl WriteState {
  pub(super) fn with_membership(membership: BrokerMembership) -> Self {
    Self {
      membership,
      ..Self::default()
    }
  }

  pub(super) fn publish_assignment(&mut self, partitions: &[(Chars, VirtualPartitionId)]) -> u64 {
    let partitions =
      partitions
        .iter()
        .fold(HashMap::new(), |mut partitions, (topic, partition)| {
          partitions
            .entry(topic.clone())
            .or_insert_with(HashSet::new)
            .insert(*partition);
          partitions
        });
    if self.assignment.partitions != partitions {
      self.assignment.generation = self.assignment.generation.saturating_add(1);
      self.assignment.partitions = partitions;
    }
    self.assignment.active = true;
    self.assignment.generation
  }

  pub(super) fn assignment_generation_is_current(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
    assignment_generation: u64,
  ) -> bool {
    self.assignment_generation_for_partition(topic, virtual_partition_id)
      == Some(assignment_generation)
  }

  pub(super) fn assignment_generation_for_partition(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> Option<u64> {
    // A pending discovery snapshot is not an authorization to write. Once activated, an empty
    // partition set is authoritative and therefore rejects every foreground allocation.
    (self.assignment.active
      && self
        .assignment
        .partitions
        .get(topic)
        .is_some_and(|partitions| partitions.contains(&virtual_partition_id)))
    .then_some(self.assignment.generation)
  }

  pub(super) fn partition_state_mut(
    &mut self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> &mut PartitionState {
    if self.topics.contains_key(topic) {
      return self
        .topics
        .get_mut(topic)
        .expect("topic was present immediately before mutable lookup")
        .partitions
        .entry(virtual_partition_id)
        .or_default();
    }

    self
      .topics
      .entry(topic.to_string().into())
      .or_default()
      .partitions
      .entry(virtual_partition_id)
      .or_default()
  }

  pub(super) fn partition_state(
    &self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> Option<&PartitionState> {
    self
      .topics
      .get(topic)
      .and_then(|topic_state| topic_state.partitions.get(&virtual_partition_id))
  }

  pub(super) fn partition_state_mut_if_present(
    &mut self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> Option<&mut PartitionState> {
    self
      .topics
      .get_mut(topic)
      .and_then(|topic_state| topic_state.partitions.get_mut(&virtual_partition_id))
  }

  pub(super) fn clear_terminally_released_partition(
    &mut self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) {
    let assignment_is_current = self
      .assignment_generation_for_partition(topic, virtual_partition_id)
      .is_some();
    let should_remove = self
      .partition_state_mut_if_present(topic, virtual_partition_id)
      .is_some_and(|partition_state| {
        // A terminal release fences the old lease even if a later membership update has already
        // restored the partition. Keep that state for maintenance to reacquire, but retire an
        // unassigned drained partition so reconciliation does not release it every heartbeat.
        partition_state.lease_expiration_at = None;
        partition_state.lease_fence = None;
        partition_state.reset_sequence_allocation();
        !assignment_is_current && partition_state.draining && partition_state.is_drained()
      });
    if !should_remove {
      return;
    }

    let remove_topic = {
      let topic_state = self
        .topics
        .get_mut(topic)
        .expect("terminal release state was present immediately before removal");
      topic_state.partitions.remove(&virtual_partition_id);
      topic_state.partitions.is_empty()
    };
    if remove_topic {
      self.topics.remove(topic);
      if self
        .last_flush_topic
        .as_ref()
        .is_some_and(|last_flush_topic| last_flush_topic.as_str() == topic)
      {
        self.last_flush_topic = None;
      }
    }
  }

  pub(super) fn partition_keys(&self) -> Vec<(Chars, VirtualPartitionId)> {
    self
      .topics
      .iter()
      .flat_map(|(topic, topic_state)| {
        topic_state
          .partitions
          .keys()
          .map(|virtual_partition_id| (topic.clone(), *virtual_partition_id))
      })
      .collect()
  }
}

//
// LocalAssignment
//

#[derive(Debug, Default)]
struct LocalAssignment {
  // `active` distinguishes discovery pending from an initialized snapshot that simply assigns no
  // local partitions. Both cases reject writes, but the distinction is useful to lifecycle code.
  active: bool,
  generation: u64,
  // This layout retains owned topic names but lets the foreground `&str` lookup borrow directly
  // from the topic map, avoiding an allocation for every admitted producer request.
  partitions: HashMap<Chars, HashSet<VirtualPartitionId>>,
}

//
// TopicState
//

#[derive(Debug, Default)]
pub(super) struct TopicState {
  pub(super) partitions: HashMap<VirtualPartitionId, PartitionState>,
}

//
// PartitionState
//

#[derive(Debug, Default)]
pub(super) struct PartitionState {
  pub(super) buffer: BufferState,
  pub(super) seq_allocator: SeqAllocator,
  pub(super) adaptive_reservation_size: Option<u64>,
  pub(super) records_allocated_since_lease_maintenance: u64,
  pub(super) lease_expiration_at: Option<OffsetDateTime>,
  pub(super) lease_fence: Option<Arc<ProducerLeaseFence>>,
  pub(super) uninstalled_lease_acquisition: Option<UninstalledLeaseAcquisition>,
  pub(super) publication_tail: Option<FlushPublicationDependency>,
  pub(super) outstanding_flushes: usize,
  pub(super) allocation_in_flight: bool,
  pub(super) allocation_started_at: Option<OffsetDateTime>,
  pub(super) allocation_notify: Arc<Notify>,
  pub(super) draining: bool,
  pub(super) drain_notify: Arc<Notify>,
}

//
// UninstalledLeaseAcquisition
//

#[derive(Debug)]
pub(super) struct UninstalledLeaseAcquisition {
  pub(super) fence: ProducerLeaseFence,
  pub(super) sequence_progress: LeaseReleaseSequenceProgress,
}

impl PartitionState {
  pub(super) fn needs_lease(&self, now: OffsetDateTime) -> bool {
    self
      .lease_expiration_at
      .is_none_or(|expires_at| now >= expires_at)
  }

  pub(super) fn is_drained(&self) -> bool {
    self.outstanding_flushes == 0 && !self.allocation_in_flight && self.buffer.batches.is_empty()
  }

  pub(super) fn reservation_target(&mut self, base_reservation_size: u64) -> u64 {
    *self
      .adaptive_reservation_size
      .get_or_insert(base_reservation_size)
  }

  pub(super) fn double_reservation_target(&mut self, base_reservation_size: u64) -> u64 {
    let previous = self.reservation_target(base_reservation_size);
    let target = previous.saturating_mul(2).min(u64::from(u32::MAX));
    self.adaptive_reservation_size = Some(target);
    debug!(
      "broker sequence reservation target increased: previous_size={previous}, \
       target_size={target}"
    );
    target
  }

  pub(super) fn reset_sequence_allocation(&mut self) {
    self.seq_allocator = SeqAllocator::default();
    self.uninstalled_lease_acquisition = None;
    self.adaptive_reservation_size = None;
    self.records_allocated_since_lease_maintenance = 0;
  }
}

//
// SeqAllocator
//

#[derive(Debug, Default)]
pub(super) struct SeqAllocator {
  pub(super) reservation: Option<SeqRange>,
  pub(super) next_seq: u64,
  pub(super) used_high_watermark: Option<u64>,
}

impl SeqAllocator {
  pub(super) fn has_reservation(&self) -> bool {
    self.reservation.is_some()
  }

  pub(super) fn remaining_capacity(&self) -> u64 {
    self
      .reservation
      .as_ref()
      .and_then(|reservation| reservation.end.checked_sub(self.next_seq))
      .map_or(0, |remaining| remaining.saturating_add(1))
  }

  pub(super) fn can_allocate(&self, count: u64) -> bool {
    let Some(reservation) = self.reservation.as_ref() else {
      return false;
    };
    if count == 0 {
      return false;
    }

    let end = self.next_seq.saturating_add(count.saturating_sub(1));
    end <= reservation.end
  }

  pub(super) fn allocate(&mut self, count: u64) -> Option<SeqRange> {
    if !self.can_allocate(count) {
      return None;
    }

    let start = self.next_seq;
    let end = start.checked_add(count.saturating_sub(1))?;
    self.next_seq = end.saturating_add(1);
    self.used_high_watermark = Some(end);
    Some(SeqRange { start, end })
  }

  pub(super) fn used_high_watermark(&self) -> Option<u64> {
    self.reservation.as_ref().and(self.used_high_watermark)
  }

  pub(super) fn install_or_extend_reservation(&mut self, range: SeqRange) {
    if self.reservation.is_none() {
      self.next_seq = range.start;
      self.used_high_watermark = range.start.checked_sub(1);
      self.reservation = Some(range);
      return;
    }

    if self.remaining_capacity() > 0 {
      let reservation = self
        .reservation
        .as_mut()
        .expect("remaining capacity requires a reservation");
      if reservation
        .end
        .checked_add(1)
        .is_some_and(|next_start| range.start == next_start)
      {
        reservation.end = range.end;
        return;
      }

      debug!(
        "broker sequence reservation replaced nonadjacent local range: previous_start={}, \
         previous_end={}, replacement_start={}, replacement_end={}",
        reservation.start, reservation.end, range.start, range.end
      );
      self.used_high_watermark = match (self.used_high_watermark, range.start.checked_sub(1)) {
        (Some(previous), Some(new_minimum)) => Some(previous.max(new_minimum)),
        (previous, new_minimum) => previous.or(new_minimum),
      };
    }

    self.next_seq = range.start;
    self.reservation = Some(range);
  }
}
