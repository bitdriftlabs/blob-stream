//! Reader-local immutable metadata, coverage proofs, and rollbackable Recovery traversal.
//!
//! Metadata queries can return thousands of rows while delivery admits only a few batches per
//! pass. Retaining the immutable rows avoids repeated queries; retaining a next-unexamined position
//! avoids repeatedly walking the same consumed rows. These are separate optimizations with separate
//! rollback rules: a failed blob read invalidates delivery progress, not a successful metadata
//! scan.
//!
//! Each partition/window owns exactly one canonical, Snowflake-ordered segment snapshot. Coverage
//! is described separately: a mature lower bound proves completeness through the window's end,
//! while a strong seal proves only a bounded interval under its original observation time. Either
//! or both qualifications can describe the same snapshot. Returned rows alone do not prove that
//! absent rows cannot arrive, and two disjoint covered intervals do not prove the gap between them.
//!
//! The reader decides when a window is mature, validates strong observation proofs against its
//! availability horizon, and supplies lifecycle retention decisions. This object stores those
//! decisions, merges immutable rows, preserves only index-safe traversal, and enforces both
//! budgets. Losing an entry is always a cache miss: the durable checkpoint remains the safe query
//! boundary.

#[cfg(test)]
#[path = "./metadata_cache_test.rs"]
mod tests;

use blob_stream_metadata_store::{MetadataReadConsistency, SegmentMetadata};
use blob_stream_types::{BatchMetadata, SnowflakeId, VirtualPartitionId};
use im::Vector;
use log::debug;
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

pub(in crate::consumer) const MAX_RETAINED_METADATA_BYTES: u64 = 16 * 1024 * 1024;
pub(in crate::consumer) const MAX_RETAINED_METADATA_ENTRIES: usize = 512;

//
// MetadataSnapshot
//

#[derive(Clone, Default)]
/// An immutable ordered view whose persistent nodes and row objects are shared across extensions.
/// Copy-on-write touches only the insertion path, not the retained prefix or rollback snapshots.
pub(in crate::consumer) struct MetadataSnapshot {
  rows: Vector<Arc<SegmentMetadata>>,
}

impl MetadataSnapshot {
  pub(in crate::consumer) fn len(&self) -> usize {
    self.rows.len()
  }

  pub(in crate::consumer) fn get(&self, index: usize) -> Option<&SegmentMetadata> {
    self.rows.get(index).map(Arc::as_ref)
  }

  pub(in crate::consumer) fn iter(&self) -> impl Iterator<Item = &SegmentMetadata> {
    self.rows.iter().map(Arc::as_ref)
  }

  pub(in crate::consumer) fn partition_point(
    &self,
    mut predicate: impl FnMut(&SegmentMetadata) -> bool,
  ) -> usize {
    let mut left = 0;
    let mut right = self.len();
    while left < right {
      let middle = left + (right - left) / 2;
      if predicate(&self.rows[middle]) {
        left = middle + 1;
      } else {
        right = middle;
      }
    }
    left
  }
}

//
// MetadataWindowKey
//

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
/// Identity of retained observations, independent of read mode or a particular query's lower bound.
pub(in crate::consumer) struct MetadataWindowKey {
  pub(in crate::consumer) virtual_partition_id: VirtualPartitionId,
  pub(in crate::consumer) window_start_unix_seconds: i64,
}

impl MetadataWindowKey {
  pub(in crate::consumer) fn with_min_snowflake(
    self,
    min_snowflake: Option<SnowflakeId>,
  ) -> MatureMetadataCacheKey {
    MatureMetadataCacheKey {
      window: self,
      min_snowflake,
    }
  }
}

//
// MatureMetadataCacheKey
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// A coverage lookup, not another storage key. One snapshot serves every floor it actually covers.
/// `None` requests completeness from Snowflake zero; a bounded Fast request must supply its floor.
pub(in crate::consumer) struct MatureMetadataCacheKey {
  pub(in crate::consumer) window: MetadataWindowKey,
  pub(in crate::consumer) min_snowflake: Option<SnowflakeId>,
}

//
// SealedMetadataPrefix
//

/// A view of the canonical snapshot plus its strong interval proof. Cloning the Arc does not copy
/// rows. The snapshot may also contain mature rows outside this interval; callers must apply both
/// the requested lower bound and `sealed_before`, rather than treating its length as coverage.
pub(in crate::consumer) struct SealedMetadataPrefix {
  pub(in crate::consumer) lower_bound: SnowflakeId,
  pub(in crate::consumer) sealed_before: SnowflakeId,
  pub(in crate::consumer) observed_at: OffsetDateTime,
  pub(in crate::consumer) segments: Arc<MetadataSnapshot>,
}

//
// SealedMetadataCoverage
//

#[derive(Clone, Copy)]
/// Original strong-read proof, without its own segment backing. Time passing cannot extend it.
struct SealedMetadataCoverage {
  lower_bound: SnowflakeId,
  sealed_before: SnowflakeId,
  observed_at: OffsetDateTime,
}

//
// WindowMetadataCacheEntry
//

/// One physical snapshot, with independently retained completeness and strong-seal qualifications.
struct WindowMetadataCacheEntry {
  /// All retained immutable rows for this partition/window, sorted and unique by Snowflake ID.
  segments: Arc<MetadataSnapshot>,
  /// `Some(floor)` proves the complete range from floor through the window's end. `Some(0)` is
  /// unbounded completeness; `None` means that no complete mature range has been observed.
  mature_lower_bound: Option<SnowflakeId>,
  /// Strong coverage is separate from maturity: an open prefix is incomplete, and an eventual
  /// mature response has no strong seal. Neither qualification implies the other.
  sealed_prefix: Option<SealedMetadataCoverage>,
  /// Decoded allocation estimate, incremented only for newly retained rows. Footprint checks never
  /// walk the rows, and no allocation-identity map is needed because each entry owns one backing.
  backing_bytes: u64,
}

impl WindowMetadataCacheEntry {
  /// Preserve the logical-entry budget: complete coverage and a strong prefix each count once,
  /// even when both reference one physical snapshot. Narrower lookup floors add no entries.
  fn entry_count(&self) -> usize {
    usize::from(self.mature_lower_bound.is_some()) + usize::from(self.sealed_prefix.is_some())
  }

  fn is_empty(&self) -> bool {
    self.mature_lower_bound.is_none() && self.sealed_prefix.is_none()
  }
}

//
// ReaderMetadataCache
//

#[derive(Default)]
/// Reader-local immutable observations and separately checkpointed traversal positions.
pub(in crate::consumer) struct ReaderMetadataCache {
  /// Observations survive failed delivery passes; ownership, policy, and retention bound their
  /// life.
  entries: HashMap<MetadataWindowKey, WindowMetadataCacheEntry>,
  /// Mutable delivery-side bookmarks. Kept separate so rollback clones only small positions and
  /// Arc references, not observations or segment rows. Fast never uses these Recovery positions.
  progress_entries: HashMap<MetadataWindowKey, RecoveryMetadataProgress>,
  /// Coverage is meaningful only under the consistency/horizon used to establish it. The first
  /// policy establishes that contract; subsequent changes invalidate observations and positions.
  policy: Option<(MetadataReadConsistency, Duration)>,
}

//
// RecoveryMetadataProgress
//

#[derive(Clone)]
/// A bookmark into one exact snapshot, justified by its initial floor and accepted sequence cursor.
struct RecoveryMetadataProgress {
  /// A shared reference, not another metadata copy. Pointer identity binds the numeric index to
  /// the snapshot it indexes; replacement or a rewound cursor otherwise requires recomputing it.
  segments: Arc<MetadataSnapshot>,
  /// First row not yet proved consumed. Capacity, visibility, and ordering barriers cannot skip
  /// it.
  next_segment: usize,
  /// Rows before this inclusive floor were excluded, not proved consumed. Relaxing the floor must
  /// reconsider them even when a coverage upgrade leaves the snapshot and accepted cursor
  /// unchanged.
  floor: SnowflakeId,
  /// Cursor against which the saved position was justified, not a Snowflake query lower bound.
  cursor: Option<u64>,
}

//
// RecoveryMetadataCheckpoint
//

/// Transactional traversal state only. Successful metadata observations are deliberately excluded.
pub(in crate::consumer) struct RecoveryMetadataCheckpoint {
  progress: HashMap<MetadataWindowKey, RecoveryMetadataProgress>,
}

impl ReaderMetadataCache {
  /// A wider complete observation serves a narrower request, including after a mode change.
  /// Completeness is checked against the proven floor, never inferred from the first returned row.
  pub(in crate::consumer) fn mature(
    &self,
    key: &MatureMetadataCacheKey,
  ) -> Option<&Arc<MetadataSnapshot>> {
    let entry = self.entries.get(&key.window)?;
    let requested_floor = key.min_snowflake.unwrap_or(SnowflakeId(0));
    (entry.mature_lower_bound? <= requested_floor).then_some(&entry.segments)
  }

  pub(in crate::consumer) fn has_mature(&self, key: &MatureMetadataCacheKey) -> bool {
    self.mature(key).is_some()
  }

  pub(in crate::consumer) fn prefix(
    &self,
    key: &MetadataWindowKey,
  ) -> Option<SealedMetadataPrefix> {
    let entry = self.entries.get(key)?;
    let prefix = entry.sealed_prefix?;
    Some(SealedMetadataPrefix {
      lower_bound: prefix.lower_bound,
      sealed_before: prefix.sealed_before,
      observed_at: prefix.observed_at,
      segments: Arc::clone(&entry.segments),
    })
  }

  /// Install an already validated strong observation of `[lower_bound, sealed_before)`. Empty row
  /// sets still prove absence inside the interval; an empty or reversed interval proves nothing.
  pub(in crate::consumer) fn install_prefix(
    &mut self,
    key: MetadataWindowKey,
    lower_bound: SnowflakeId,
    sealed_before: SnowflakeId,
    observed_at: OffsetDateTime,
    segments: &[SegmentMetadata],
  ) -> bool {
    if lower_bound >= sealed_before {
      return false;
    }
    let previous_entry = self.entries.get(&key);
    let previous = previous_entry
      .and_then(|entry| entry.sealed_prefix)
      .filter(|cached| cached.lower_bound <= lower_bound && cached.sealed_before >= lower_bound);
    // Extend only an overlapping or adjacent proof that already covers the new request's floor.
    // A disjoint prefix replaces prefix-only coverage rather than inventing coverage across a gap.
    if previous.is_some_and(|cached| cached.sealed_before >= sealed_before) {
      return false;
    }
    let append_from = previous.map_or(lower_bound, |cached| cached.sealed_before);
    let retained_lower_bound = previous.map_or(lower_bound, |cached| cached.lower_bound);
    let suffix = Self::project_segments(
      key.virtual_partition_id,
      segments.iter().filter(|segment| {
        segment.snowflake_id >= append_from && segment.snowflake_id < sealed_before
      }),
    );
    // Strong coverage can widen a complete mature range only if the two intervals touch. Rows
    // retained from disjoint intervals still share one snapshot, but keep distinct coverage bounds.
    let mature_lower_bound = previous_entry
      .and_then(|entry| entry.mature_lower_bound)
      .map(|floor| {
        if sealed_before >= floor {
          floor.min(retained_lower_bound)
        } else {
          floor
        }
      });
    self.install_observation(
      key,
      suffix,
      previous.is_some() || mature_lower_bound.is_some(),
      mature_lower_bound,
      Some(SealedMetadataCoverage {
        lower_bound: retained_lower_bound,
        sealed_before,
        observed_at,
      }),
    );
    true
  }

  /// Install a complete response from its actual query floor. The caller establishes maturity and
  /// rejects visibility-incomplete responses. Repeated or narrower requests enrich the same
  /// snapshot rather than allocating response variants; a touching strong prefix can lower the
  /// complete floor.
  pub(in crate::consumer) fn install_mature(
    &mut self,
    key: MatureMetadataCacheKey,
    segments: &[SegmentMetadata],
  ) {
    let previous = self.entries.get(&key.window);
    let prefix = previous.and_then(|entry| entry.sealed_prefix);
    let floor = key.min_snowflake.unwrap_or(SnowflakeId(0));
    let mut mature_lower_bound = previous
      .and_then(|entry| entry.mature_lower_bound)
      .map_or(floor, |previous_floor| previous_floor.min(floor));
    if let Some(prefix) = prefix
      && prefix.sealed_before >= mature_lower_bound
    {
      mature_lower_bound = mature_lower_bound.min(prefix.lower_bound);
    }
    self.install_observation(
      key.window,
      Self::project_segments(
        key.window.virtual_partition_id,
        segments.iter().filter(|segment| {
          previous.is_none_or(|entry| {
            let index = entry
              .segments
              .partition_point(|cached| cached.snowflake_id < segment.snowflake_id);
            entry
              .segments
              .get(index)
              .is_none_or(|cached| cached.snowflake_id != segment.snowflake_id)
          })
        }),
      ),
      previous.is_some(),
      Some(mature_lower_bound),
      prefix,
    );
  }

  /// Merge proven immutable rows into the sole backing and reconcile traversal with its new layout.
  /// Coverage bounds describe absence guarantees independently of which rows happen to be present.
  fn install_observation(
    &mut self,
    key: MetadataWindowKey,
    mut new_segments: Vec<SegmentMetadata>,
    retain_previous: bool,
    mature_lower_bound: Option<SnowflakeId>,
    sealed_prefix: Option<SealedMetadataCoverage>,
  ) {
    let previous = self.entries.get(&key);
    let retained = previous.filter(|_| retain_previous);
    let mut merged = retained.map_or_else(MetadataSnapshot::default, |entry| {
      entry.segments.as_ref().clone()
    });
    let previous_len = merged.len();
    let mut backing_bytes = retained.map_or_else(
      || u64::try_from(size_of::<MetadataSnapshot>() + 1024).unwrap_or(u64::MAX),
      |entry| entry.backing_bytes,
    );
    new_segments.sort_by_key(|segment| segment.snowflake_id);
    new_segments.dedup_by_key(|segment| segment.snowflake_id);
    let mut preserves_positions = retained.is_some();
    for segment in new_segments {
      let index = if merged
        .rows
        .back()
        .is_none_or(|last| last.snowflake_id < segment.snowflake_id)
      {
        merged.len()
      } else {
        merged.partition_point(|cached| cached.snowflake_id < segment.snowflake_id)
      };
      if merged
        .get(index)
        .is_some_and(|cached| cached.snowflake_id == segment.snowflake_id)
      {
        continue;
      }
      preserves_positions &= index == merged.len();
      // Include per-row Arc allocation and conservative persistent-node slack. Cost only newly
      // retained rows; old nodes and rows remain shared with the previous immutable view.
      backing_bytes =
        backing_bytes.saturating_add(Self::segment_retained_bytes(&segment).saturating_add(256));
      if index == merged.len() {
        merged.rows.push_back(Arc::new(segment));
      } else {
        merged.rows.insert(index, Arc::new(segment));
      }
    }
    let segments = if let Some(entry) = retained
      && merged.len() == previous_len
    {
      Arc::clone(&entry.segments)
    } else {
      Arc::new(merged)
    };
    let previous_segments = previous.map(|entry| Arc::clone(&entry.segments));
    if preserves_positions && let Some(previous) = previous_segments.as_ref() {
      self.extend_recovery_metadata_progress(key, previous, &segments);
    }
    debug!(
      "consumer retained metadata observation: partition={}, window_start={}, rows={}, \
       mature_floor={:?}, sealed_before={:?}, preserves_positions={preserves_positions}",
      key.virtual_partition_id,
      key.window_start_unix_seconds,
      segments.len(),
      mature_lower_bound.map(SnowflakeId::as_u64),
      sealed_prefix.map(|prefix| prefix.sealed_before.as_u64()),
    );
    self.entries.insert(
      key,
      WindowMetadataCacheEntry {
        segments,
        mature_lower_bound,
        sealed_prefix,
        backing_bytes,
      },
    );
    self.enforce_budget(MAX_RETAINED_METADATA_BYTES, MAX_RETAINED_METADATA_ENTRIES);
  }

  /// Store only the requested partition's index. Shared broker responses otherwise multiply the
  /// unrelated partition data retained and visited by every reader-local window entry.
  fn project_segments<'a>(
    partition_id: VirtualPartitionId,
    segments: impl Iterator<Item = &'a SegmentMetadata>,
  ) -> Vec<SegmentMetadata> {
    segments
      .filter_map(|segment| {
        let batch = segment.segment_index.get(&partition_id)?;
        Some(SegmentMetadata {
          window: segment.window.clone(),
          snowflake_id: segment.snowflake_id,
          blob_key: segment.blob_key.clone(),
          compression: segment.compression.clone(),
          segment_index: HashMap::from([(partition_id, batch.clone())]),
          created_at: segment.created_at,
          metadata_published_at: segment.metadata_published_at,
        })
      })
      .collect()
  }

  /// Assignment loss invalidates both observations and progress for partitions no longer owned.
  pub(in crate::consumer) fn retain_partitions(&mut self, assigned: &HashSet<VirtualPartitionId>) {
    self
      .entries
      .retain(|key, _| assigned.contains(&key.virtual_partition_id));
    self
      .progress_entries
      .retain(|key, _| assigned.contains(&key.virtual_partition_id));
  }

  /// Seek or hydration changes the delivery boundary, so neither old coverage nor a bookmark may
  /// bypass the reader's newly chosen checkpoint overlap.
  pub(in crate::consumer) fn invalidate_partition(&mut self, partition_id: VirtualPartitionId) {
    self
      .entries
      .retain(|key, _| key.virtual_partition_id != partition_id);
    self
      .progress_entries
      .retain(|key, _| key.virtual_partition_id != partition_id);
  }

  /// Retain each qualification under its own lifecycle rules. Recovery can need a prefix beyond
  /// the live Fast horizon; maturity can outlive a strong seal or exist without one. Clearing one
  /// qualification keeps the same immutable backing and positions for the other. Extra known rows
  /// may remain in that snapshot, but cannot widen its remaining proof and remain charged to the
  /// budget.
  pub(in crate::consumer) fn retain_windows(
    &mut self,
    mut mature: impl FnMut(&MatureMetadataCacheKey) -> bool,
    mut prefix: impl FnMut(&MetadataWindowKey) -> bool,
    mut progress: impl FnMut(&MetadataWindowKey) -> bool,
  ) {
    self.entries.retain(|key, entry| {
      if let Some(floor) = entry.mature_lower_bound
        && !mature(&key.with_min_snowflake((floor != SnowflakeId(0)).then_some(floor)))
      {
        entry.mature_lower_bound = None;
      }
      if entry.sealed_prefix.is_some() && !prefix(key) {
        entry.sealed_prefix = None;
      }
      !entry.is_empty()
    });
    self.progress_entries.retain(|key, _| progress(key));
  }

  /// A new consistency or availability horizon changes which absences were safe to cache. Clear
  /// both kinds of coverage rather than reinterpreting old observations under the new contract.
  pub(in crate::consumer) fn validate_policy(
    &mut self,
    consistency: MetadataReadConsistency,
    horizon: Duration,
  ) -> bool {
    let policy = (consistency, horizon);
    let changed = self.policy.is_some_and(|previous| previous != policy);
    if changed {
      self.entries.clear();
      self.progress_entries.clear();
    }
    self.policy = Some(policy);
    changed
  }

  /// Start at the requested Snowflake floor, then reuse only a snapshot-matching, nonrewound
  /// position whose initial floor is no later than this request's. The unbounded floor is zero,
  /// not a timestamp-derived floor for the window.
  pub(in crate::consumer) fn recovery_start(
    &self,
    partition_id: VirtualPartitionId,
    window_start: i64,
    segments: &Arc<MetadataSnapshot>,
    floor: Option<SnowflakeId>,
    cursor: Option<u64>,
  ) -> usize {
    let floor = floor.unwrap_or(SnowflakeId(0));
    let start = segments.partition_point(|segment| segment.snowflake_id < floor);
    self
      .progress_entries
      .get(&MetadataWindowKey {
        virtual_partition_id: partition_id,
        window_start_unix_seconds: window_start,
      })
      .filter(|progress| {
        Arc::ptr_eq(&progress.segments, segments)
          && cursor >= progress.cursor
          && floor >= progress.floor
      })
      .map_or(start, |progress| progress.next_segment.max(start))
  }

  /// Traversal uses the same canonical snapshot regardless of the active coverage qualification.
  /// The reader checks interval coverage before scanning; this lookup alone is not a completeness
  /// test, and must not be used to suppress a required query for an uncovered interval.
  fn immutable_recovery_segments(
    &self,
    partition_id: VirtualPartitionId,
    window_start: i64,
  ) -> Option<Arc<MetadataSnapshot>> {
    let window = MetadataWindowKey {
      virtual_partition_id: partition_id,
      window_start_unix_seconds: window_start,
    };
    self
      .entries
      .get(&window)
      .map(|entry| Arc::clone(&entry.segments))
  }

  /// Lazily establish or reset a bookmark from the caller's current floor and accepted cursor.
  fn progress(
    &mut self,
    partition_id: VirtualPartitionId,
    window_start: i64,
    floor: Option<SnowflakeId>,
    cursor: Option<u64>,
  ) -> Option<&mut RecoveryMetadataProgress> {
    let segments = self.immutable_recovery_segments(partition_id, window_start)?;
    let start = self.recovery_start(partition_id, window_start, &segments, floor, cursor);
    let requested_floor = floor.unwrap_or(SnowflakeId(0));
    let progress = self
      .progress_entries
      .entry(MetadataWindowKey {
        virtual_partition_id: partition_id,
        window_start_unix_seconds: window_start,
      })
      .or_insert_with(|| RecoveryMetadataProgress {
        segments: Arc::clone(&segments),
        next_segment: start,
        floor: requested_floor,
        cursor,
      });
    if !Arc::ptr_eq(&progress.segments, &segments)
      || cursor < progress.cursor
      || requested_floor < progress.floor
    {
      *progress = RecoveryMetadataProgress {
        segments,
        next_segment: start,
        floor: requested_floor,
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

  pub(in crate::consumer) fn retained_window_keys(&self) -> HashSet<MetadataWindowKey> {
    self.entries.keys().copied().collect()
  }

  /// Commit positions only after the reader's ordering and missing-blob policy succeeds.
  /// Advance a contiguous consumed prefix only: accepting a later sequence must not jump across
  /// an earlier partial, oversized, deferred, or held batch whose row remains unexamined.
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

  /// Clone small bookmarks and shared Arc references before a delivery pass mutates traversal.
  pub(in crate::consumer) fn checkpoint_progress(&self) -> RecoveryMetadataCheckpoint {
    RecoveryMetadataCheckpoint {
      progress: self.progress_entries.clone(),
    }
  }

  /// Undo traversal, not successful observations. If the pass replaced, extended, or evicted a
  /// backing, its old checkpoint position no longer indexes the retained snapshot and is discarded.
  /// Recomputing from the restored reader cursor may revisit rows, but cannot lose undelivered
  /// data.
  pub(in crate::consumer) fn restore_progress(&mut self, checkpoint: RecoveryMetadataCheckpoint) {
    self.progress_entries = checkpoint.progress;
    self.enforce_budget(MAX_RETAINED_METADATA_BYTES, MAX_RETAINED_METADATA_ENTRIES);
  }

  /// A held sequence gap rewinds Recovery to its oldest possible predecessor window. Drop later
  /// bookmarks while retaining immutable observations, so the validating retry cannot skip the gap.
  pub(in crate::consumer) fn invalidate_progress_from(
    &mut self,
    partition_id: VirtualPartitionId,
    window_start: i64,
  ) {
    self.progress_entries.retain(|key, _| {
      key.virtual_partition_id != partition_id || key.window_start_unix_seconds < window_start
    });
  }

  pub(in crate::consumer) fn entry_count(&self) -> usize {
    self
      .entries
      .values()
      .map(WindowMetadataCacheEntry::entry_count)
      .fold(0, usize::saturating_add)
  }

  pub(in crate::consumer) fn footprint(&self) -> (usize, u64) {
    (self.entry_count(), self.retained_bytes())
  }

  /// Conservative decoded allocation cost, including owned strings and partition-index capacity.
  /// This intentionally overestimates hash buckets rather than measuring compressed wire bytes.
  fn segment_retained_bytes(segment: &SegmentMetadata) -> u64 {
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

  /// Charge qualification and traversal overhead conservatively, and each entry's snapshot once.
  /// Prefix views and progress/checkpoint Arcs share that backing and do not add copies of its
  /// rows.
  pub(in crate::consumer) fn retained_bytes(&self) -> u64 {
    let entry_bytes = u64::try_from(
      (size_of::<SealedMetadataPrefix>() + size_of::<RecoveryMetadataProgress>() + 128)
        .saturating_mul(2),
    )
    .unwrap_or(u64::MAX);
    self
      .entries
      .values()
      .map(|entry| {
        entry_bytes
          .saturating_mul(u64::try_from(entry.entry_count()).unwrap_or(u64::MAX))
          .saturating_add(entry.backing_bytes)
      })
      .fold(0, u64::saturating_add)
  }

  /// Translate only a bookmark into the backing whose ordered rows were preserved as a prefix.
  /// Checkpoint snapshots retain their old identity; rollback reconciles them separately.
  fn extend_recovery_metadata_progress(
    &mut self,
    key: MetadataWindowKey,
    previous: &Arc<MetadataSnapshot>,
    extended: &Arc<MetadataSnapshot>,
  ) {
    if let Some(progress) = self.progress_entries.get_mut(&key)
      && Arc::ptr_eq(&progress.segments, previous)
    {
      progress.segments = Arc::clone(extended);
    }
  }

  /// Eviction loses only an optimization; the checkpoint floor remains the safe query boundary.
  /// Enforce byte and logical-entry limits independently. Remove whole partition/windows oldest
  /// first, with a deterministic partition tie-breaker, so no bookmark survives without its
  /// backing.
  pub(in crate::consumer) fn enforce_budget(&mut self, max_bytes: u64, max_entries: usize) {
    // Replacement and rollback can leave a position tied to an obsolete allocation. Reject it
    // before considering the budgets, even when memory pressure does not require eviction.
    self.progress_entries.retain(|key, progress| {
      self
        .entries
        .get(key)
        .is_some_and(|entry| Arc::ptr_eq(&entry.segments, &progress.segments))
    });
    let mut keys = self.entries.keys().copied().collect::<Vec<_>>();
    keys.sort_unstable_by_key(|key| (key.window_start_unix_seconds, key.virtual_partition_id));
    let mut bytes = self.retained_bytes();
    let mut entries = self.entry_count();
    for key in keys {
      if bytes <= max_bytes && entries <= max_entries {
        break;
      }
      self.entries.remove(&key);
      self.progress_entries.remove(&key);
      bytes = self.retained_bytes();
      entries = self.entry_count();
      let partition = key.virtual_partition_id;
      let window = key.window_start_unix_seconds;
      debug!(
        "consumer evicted retained metadata: partition={partition}, window_start={window}, \
         retained_bytes={bytes}, entries={entries}"
      );
    }
  }
}
