#[cfg(test)]
#[path = "./metadata_cache_test.rs"]
mod tests;

use blob_stream_metadata_store::{MetadataReadConsistency, SegmentMetadata};
use blob_stream_types::{BatchMetadata, SnowflakeId, VirtualPartitionId};
use log::debug;
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::{Arc, Weak};
use time::{Duration, OffsetDateTime};

pub(in crate::consumer) const MAX_RETAINED_METADATA_BYTES: u64 = 16 * 1024 * 1024;
pub(in crate::consumer) const MAX_RETAINED_METADATA_ENTRIES: usize = 512;

pub(in crate::consumer) type RecoveryMetadataCacheKey = (VirtualPartitionId, i64, Option<u64>);

//
// SealedMetadataPrefix
//

pub(in crate::consumer) struct SealedMetadataPrefix {
  pub(in crate::consumer) lower_bound: SnowflakeId,
  pub(in crate::consumer) sealed_before: SnowflakeId,
  pub(in crate::consumer) observed_at: OffsetDateTime,
  pub(in crate::consumer) segments: Arc<[SegmentMetadata]>,
}

//
// ReaderMetadataCache
//

#[derive(Default)]
/// Reader-local immutable observations and separately checkpointed traversal positions.
pub(in crate::consumer) struct ReaderMetadataCache {
  mature_entries: HashMap<RecoveryMetadataCacheKey, Arc<[SegmentMetadata]>>,
  sealed_prefixes: HashMap<(VirtualPartitionId, i64), SealedMetadataPrefix>,
  progress_entries: HashMap<(VirtualPartitionId, i64), RecoveryMetadataProgress>,
  backing_bytes: HashMap<usize, (Weak<[SegmentMetadata]>, u64)>,
  policy: Option<(MetadataReadConsistency, Duration)>,
}

//
// RecoveryMetadataProgress
//

#[derive(Clone)]
struct RecoveryMetadataProgress {
  segments: Arc<[SegmentMetadata]>,
  next_segment: usize,
  cursor: Option<u64>,
}

//
// RecoveryMetadataCheckpoint
//

pub(in crate::consumer) struct RecoveryMetadataCheckpoint {
  progress: HashMap<(VirtualPartitionId, i64), RecoveryMetadataProgress>,
}

impl ReaderMetadataCache {
  pub(in crate::consumer) fn mature(
    &self,
    key: &RecoveryMetadataCacheKey,
  ) -> Option<&Arc<[SegmentMetadata]>> {
    self.mature_entries.get(key)
  }

  pub(in crate::consumer) fn has_mature(&self, key: &RecoveryMetadataCacheKey) -> bool {
    self.mature_entries.contains_key(key)
  }

  pub(in crate::consumer) fn prefix(
    &self,
    key: &(VirtualPartitionId, i64),
  ) -> Option<&SealedMetadataPrefix> {
    self.sealed_prefixes.get(key)
  }

  pub(in crate::consumer) fn install_prefix(
    &mut self,
    key: (VirtualPartitionId, i64),
    lower_bound: SnowflakeId,
    sealed_before: SnowflakeId,
    observed_at: OffsetDateTime,
    segments: &[SegmentMetadata],
  ) -> bool {
    if lower_bound >= sealed_before {
      return false;
    }
    let previous = self
      .prefix(&key)
      .filter(|cached| cached.lower_bound <= lower_bound && cached.sealed_before >= lower_bound);
    if previous.is_some_and(|cached| cached.sealed_before >= sealed_before) {
      return false;
    }
    let append_from = previous.map_or(lower_bound, |cached| cached.sealed_before);
    let retained_lower_bound = previous.map_or(lower_bound, |cached| cached.lower_bound);
    let previous_segments = previous.map(|cached| Arc::clone(&cached.segments));
    let suffix = Self::project_segments(
      key.0,
      segments.iter().filter(|segment| {
        segment.snowflake_id >= append_from && segment.snowflake_id < sealed_before
      }),
    );
    // Only a contiguous observation can inherit the old rows and their traversal position.
    let extended: Arc<[SegmentMetadata]> = if let Some(previous) = previous_segments.as_ref() {
      previous
        .iter()
        .cloned()
        .chain(suffix)
        .collect::<Vec<_>>()
        .into()
    } else {
      suffix.into()
    };
    if let Some(previous) = previous_segments.as_ref() {
      self.extend_recovery_metadata_progress(key, previous, &extended);
    }
    self.sealed_prefixes.insert(
      key,
      SealedMetadataPrefix {
        lower_bound: retained_lower_bound,
        sealed_before,
        observed_at,
        segments: extended,
      },
    );
    self.enforce_budget(MAX_RETAINED_METADATA_BYTES, MAX_RETAINED_METADATA_ENTRIES);
    true
  }

  pub(in crate::consumer) fn install_mature(
    &mut self,
    key: RecoveryMetadataCacheKey,
    segments: &[SegmentMetadata],
  ) {
    let complete_prefix = self
      .prefix(&(key.0, key.1))
      .filter(|prefix| {
        segments
          .iter()
          .filter(|segment| segment.segment_index.contains_key(&key.0))
          .all(|segment| {
            segment.snowflake_id >= prefix.lower_bound
              && segment.snowflake_id < prefix.sealed_before
          })
      })
      .map(|prefix| Arc::clone(&prefix.segments));
    let projected =
      complete_prefix.unwrap_or_else(|| Self::project_segments(key.0, segments.iter()).into());
    self.mature_entries.insert(key, projected);
    self.enforce_budget(MAX_RETAINED_METADATA_BYTES, MAX_RETAINED_METADATA_ENTRIES);
  }

  fn project_segments<'a>(
    partition_id: VirtualPartitionId,
    segments: impl Iterator<Item = &'a SegmentMetadata>,
  ) -> Vec<SegmentMetadata> {
    segments
      .filter(|segment| segment.segment_index.contains_key(&partition_id))
      .cloned()
      .filter_map(|mut segment| {
        let batch = segment.segment_index.remove(&partition_id)?;
        segment.segment_index = HashMap::from([(partition_id, batch)]);
        Some(segment)
      })
      .collect()
  }

  pub(in crate::consumer) fn retain_partitions(&mut self, assigned: &HashSet<VirtualPartitionId>) {
    self
      .mature_entries
      .retain(|(partition, ..), _| assigned.contains(partition));
    self
      .sealed_prefixes
      .retain(|(partition, _), _| assigned.contains(partition));
    self
      .progress_entries
      .retain(|(partition, _), _| assigned.contains(partition));
    self.refresh_metadata_backing_bytes();
  }

  pub(in crate::consumer) fn invalidate_partition(&mut self, partition_id: VirtualPartitionId) {
    self
      .mature_entries
      .retain(|(partition, ..), _| *partition != partition_id);
    self
      .sealed_prefixes
      .retain(|(partition, _), _| *partition != partition_id);
    self
      .progress_entries
      .retain(|(partition, _), _| *partition != partition_id);
    self.refresh_metadata_backing_bytes();
  }

  pub(in crate::consumer) fn retain_windows(
    &mut self,
    mut mature: impl FnMut(&RecoveryMetadataCacheKey) -> bool,
    mut prefix: impl FnMut(&(VirtualPartitionId, i64)) -> bool,
    mut progress: impl FnMut(&(VirtualPartitionId, i64)) -> bool,
  ) {
    self.mature_entries.retain(|key, _| mature(key));
    self.sealed_prefixes.retain(|key, _| prefix(key));
    self.progress_entries.retain(|key, _| progress(key));
    self.refresh_metadata_backing_bytes();
  }

  #[cfg(test)]
  pub(in crate::consumer) fn insert_mature_for_test(
    &mut self,
    key: RecoveryMetadataCacheKey,
    segments: Arc<[SegmentMetadata]>,
  ) {
    self.mature_entries.insert(key, segments);
  }

  #[cfg(test)]
  pub(in crate::consumer) fn insert_prefix_for_test(
    &mut self,
    key: (VirtualPartitionId, i64),
    prefix: SealedMetadataPrefix,
  ) {
    self.sealed_prefixes.insert(key, prefix);
  }

  #[cfg(test)]
  pub(in crate::consumer) fn prefix_mut_for_test(
    &mut self,
    key: &(VirtualPartitionId, i64),
  ) -> Option<&mut SealedMetadataPrefix> {
    self.sealed_prefixes.get_mut(key)
  }

  #[cfg(test)]
  pub(in crate::consumer) fn remove_prefix_for_test(&mut self, key: &(VirtualPartitionId, i64)) {
    self.sealed_prefixes.remove(key);
  }

  #[cfg(test)]
  pub(in crate::consumer) fn remove_mature_for_test(
    &mut self,
    key: &RecoveryMetadataCacheKey,
  ) -> Option<Arc<[SegmentMetadata]>> {
    self.mature_entries.remove(key)
  }

  #[cfg(test)]
  pub(in crate::consumer) fn mature_keys(&self) -> impl Iterator<Item = &RecoveryMetadataCacheKey> {
    self.mature_entries.keys()
  }

  #[cfg(test)]
  pub(in crate::consumer) fn mature_count(&self) -> usize {
    self.mature_entries.len()
  }

  #[cfg(test)]
  pub(in crate::consumer) fn prefix_count(&self) -> usize {
    self.sealed_prefixes.len()
  }

  #[cfg(test)]
  pub(in crate::consumer) fn progress_count(&self) -> usize {
    self.progress_entries.len()
  }

  pub(in crate::consumer) fn validate_policy(
    &mut self,
    consistency: MetadataReadConsistency,
    horizon: Duration,
  ) -> bool {
    let policy = (consistency, horizon);
    let changed = self.policy.is_some_and(|previous| previous != policy);
    if changed {
      self.mature_entries.clear();
      self.sealed_prefixes.clear();
      self.progress_entries.clear();
      self.backing_bytes.clear();
    }
    self.policy = Some(policy);
    changed
  }

  pub(in crate::consumer) fn recovery_start(
    &self,
    partition_id: VirtualPartitionId,
    window_start: i64,
    segments: &Arc<[SegmentMetadata]>,
    floor: Option<SnowflakeId>,
    cursor: Option<u64>,
  ) -> usize {
    let floor = floor.unwrap_or(SnowflakeId(0));
    let start = segments.partition_point(|segment| segment.snowflake_id < floor);
    self
      .progress_entries
      .get(&(partition_id, window_start))
      .filter(|progress| Arc::ptr_eq(&progress.segments, segments) && cursor >= progress.cursor)
      .map_or(start, |progress| progress.next_segment.max(start))
  }

  fn immutable_recovery_segments(
    &self,
    partition_id: VirtualPartitionId,
    window_start: i64,
    floor: Option<SnowflakeId>,
  ) -> Option<Arc<[SegmentMetadata]>> {
    let bound = floor.map(SnowflakeId::as_u64);
    self
      .mature_entries
      .get(&(partition_id, window_start, bound))
      .cloned()
      .or_else(|| {
        self
          .sealed_prefixes
          .get(&(partition_id, window_start))
          .map(|prefix| Arc::clone(&prefix.segments))
      })
  }

  fn progress(
    &mut self,
    partition_id: VirtualPartitionId,
    window_start: i64,
    floor: Option<SnowflakeId>,
    cursor: Option<u64>,
  ) -> Option<&mut RecoveryMetadataProgress> {
    let segments = self.immutable_recovery_segments(partition_id, window_start, floor)?;
    let start = self.recovery_start(partition_id, window_start, &segments, floor, cursor);
    let progress = self
      .progress_entries
      .entry((partition_id, window_start))
      .or_insert_with(|| RecoveryMetadataProgress {
        segments: Arc::clone(&segments),
        next_segment: start,
        cursor,
      });
    if !Arc::ptr_eq(&progress.segments, &segments) || cursor < progress.cursor {
      *progress = RecoveryMetadataProgress {
        segments,
        next_segment: start,
        cursor,
      };
    }
    Some(progress)
  }

  /// Stage only the next row proved consumed by the reader's pre-pass cursor.
  pub(in crate::consumer) fn stage_skip(
    &mut self,
    partition_id: VirtualPartitionId,
    window_start: i64,
    floor: Option<SnowflakeId>,
    cursor: Option<u64>,
    snowflake_id: SnowflakeId,
  ) {
    let Some(progress) = self.progress(partition_id, window_start, floor, cursor) else {
      return;
    };
    if progress
      .segments
      .get(progress.next_segment)
      .is_some_and(|segment| segment.snowflake_id == snowflake_id)
    {
      progress.next_segment += 1;
    }
  }

  pub(in crate::consumer) fn recovery_keys(&self) -> HashSet<(VirtualPartitionId, i64)> {
    self
      .sealed_prefixes
      .keys()
      .copied()
      .chain(
        self
          .mature_entries
          .keys()
          .map(|&(partition, window, _)| (partition, window)),
      )
      .collect()
  }

  /// Commit positions only after the reader's ordering and missing-blob policy succeeds.
  pub(in crate::consumer) fn commit_progress(
    &mut self,
    partition_id: VirtualPartitionId,
    window_start: i64,
    floor: Option<SnowflakeId>,
    cursor: Option<u64>,
  ) {
    let Some(progress) = self.progress(partition_id, window_start, floor, cursor) else {
      return;
    };
    while progress
      .segments
      .get(progress.next_segment)
      .is_some_and(|segment| {
        segment
          .segment_index
          .get(&partition_id)
          .is_some_and(|batch| cursor.is_some_and(|cursor| batch.seq_range.end <= cursor))
      })
    {
      progress.next_segment += 1;
    }
    progress.cursor = cursor;
  }

  pub(in crate::consumer) fn checkpoint_progress(&self) -> RecoveryMetadataCheckpoint {
    RecoveryMetadataCheckpoint {
      progress: self.progress_entries.clone(),
    }
  }

  pub(in crate::consumer) fn restore_progress(&mut self, checkpoint: RecoveryMetadataCheckpoint) {
    self.progress_entries = checkpoint.progress;
    self.enforce_budget(MAX_RETAINED_METADATA_BYTES, MAX_RETAINED_METADATA_ENTRIES);
  }

  pub(in crate::consumer) fn invalidate_progress_from(
    &mut self,
    partition_id: VirtualPartitionId,
    window_start: i64,
  ) {
    self
      .progress_entries
      .retain(|&(partition, window), _| partition != partition_id || window < window_start);
  }

  pub(in crate::consumer) fn entry_count(&self) -> usize {
    self
      .mature_entries
      .len()
      .saturating_add(self.sealed_prefixes.len())
  }

  pub(in crate::consumer) fn footprint(&mut self) -> (usize, u64) {
    self.refresh_metadata_backing_bytes();
    (self.entry_count(), self.retained_bytes())
  }

  fn mature_metadata_cache_segment_bytes(segment: &SegmentMetadata) -> u64 {
    let mut bytes = u64::try_from(size_of::<SegmentMetadata>()).unwrap_or(u64::MAX);
    bytes =
      bytes.saturating_add(u64::try_from(segment.window.topic.capacity()).unwrap_or(u64::MAX));
    bytes =
      bytes.saturating_add(u64::try_from(segment.blob_key.as_str().len()).unwrap_or(u64::MAX));
    bytes = bytes.saturating_add(
      u64::try_from(
        (size_of::<BatchMetadata>() + size_of::<VirtualPartitionId>() + 8)
          .saturating_mul(segment.segment_index.capacity().saturating_mul(2)),
      )
      .unwrap_or(u64::MAX),
    );
    bytes
  }

  pub(in crate::consumer) fn retained_bytes(&self) -> u64 {
    let entry_bytes = u64::try_from(
      (size_of::<SealedMetadataPrefix>() + size_of::<RecoveryMetadataProgress>() + 128)
        .saturating_mul(2),
    )
    .unwrap_or(u64::MAX);
    let mut backing = HashSet::new();
    self
      .mature_entries
      .values()
      .chain(self.sealed_prefixes.values().map(|prefix| &prefix.segments))
      .map(|segments| {
        let address = Arc::as_ptr(segments).cast::<SegmentMetadata>().addr();
        if backing.insert(address) {
          let bytes = self.backing_bytes.get(&address).map_or_else(
            || {
              segments
                .iter()
                .map(Self::mature_metadata_cache_segment_bytes)
                .fold(16, u64::saturating_add)
            },
            |(_, bytes)| *bytes,
          );
          entry_bytes.saturating_add(bytes)
        } else {
          entry_bytes
        }
      })
      .fold(0, u64::saturating_add)
  }

  fn refresh_metadata_backing_bytes(&mut self) {
    let mut retained = HashSet::new();
    for segments in self
      .mature_entries
      .values()
      .chain(self.sealed_prefixes.values().map(|prefix| &prefix.segments))
    {
      let address = Arc::as_ptr(segments).cast::<SegmentMetadata>().addr();
      retained.insert(address);
      self.backing_bytes.entry(address).or_insert_with(|| {
        // The weak identity prevents allocator address reuse until this entry is discarded.
        let bytes = segments
          .iter()
          .map(Self::mature_metadata_cache_segment_bytes)
          .fold(16, u64::saturating_add);
        (Arc::downgrade(segments), bytes)
      });
    }
    self
      .backing_bytes
      .retain(|address, _| retained.contains(address));
  }

  fn extend_recovery_metadata_progress(
    &mut self,
    key: (VirtualPartitionId, i64),
    previous: &Arc<[SegmentMetadata]>,
    extended: &Arc<[SegmentMetadata]>,
  ) {
    if let Some(progress) = self.progress_entries.get_mut(&key)
      && Arc::ptr_eq(&progress.segments, previous)
    {
      progress.segments = Arc::clone(extended);
    }
  }

  /// Eviction loses only an optimization; the checkpoint floor remains the safe query boundary.
  pub(in crate::consumer) fn enforce_budget(&mut self, max_bytes: u64, max_entries: usize) {
    self.refresh_metadata_backing_bytes();
    self
      .progress_entries
      .retain(|&(partition, window), progress| {
        self
          .mature_entries
          .iter()
          .any(|(&(cached_partition, cached_window, _), segments)| {
            cached_partition == partition
              && cached_window == window
              && Arc::ptr_eq(segments, &progress.segments)
          })
          || self
            .sealed_prefixes
            .get(&(partition, window))
            .is_some_and(|prefix| Arc::ptr_eq(&prefix.segments, &progress.segments))
      });
    let mut keys = self
      .mature_entries
      .keys()
      .map(|&(partition, window, _)| (window, partition))
      .chain(
        self
          .sealed_prefixes
          .keys()
          .map(|&(partition, window)| (window, partition)),
      )
      .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    let mut bytes = self.retained_bytes();
    let mut entries = self.mature_entries.len() + self.sealed_prefixes.len();
    for (window, partition) in keys {
      if bytes <= max_bytes && entries <= max_entries {
        break;
      }
      self
        .mature_entries
        .retain(|&(cached_partition, cached_window, _), _| {
          cached_partition != partition || cached_window != window
        });
      self.sealed_prefixes.remove(&(partition, window));
      self.progress_entries.remove(&(partition, window));
      bytes = self.retained_bytes();
      entries = self.mature_entries.len() + self.sealed_prefixes.len();
      debug!(
        "consumer evicted retained metadata: partition={partition}, window_start={window}, \
         retained_bytes={bytes}, entries={entries}"
      );
    }
    self.refresh_metadata_backing_bytes();
  }
}
