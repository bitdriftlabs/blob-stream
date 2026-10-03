#![allow(clippy::unwrap_used)]

use super::{
  MatureMetadataCacheKey,
  MetadataWindowKey,
  ReaderMetadataCache,
  RecoveryMetadataProgress,
  SealedMetadataPrefix,
};
use blob_stream_blob_store::{BlobKey, ByteRange};
use blob_stream_metadata_store::{MetadataReadConsistency, SegmentMetadata};
use blob_stream_types::{
  BatchMetadata,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::ptr;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

fn window_key(
  virtual_partition_id: VirtualPartitionId,
  window_start_unix_seconds: i64,
) -> MetadataWindowKey {
  MetadataWindowKey {
    virtual_partition_id,
    window_start_unix_seconds,
  }
}

fn segments() -> Vec<SegmentMetadata> {
  (1 ..= 8)
    .map(|sequence| SegmentMetadata {
      window: TopicWindowKey {
        topic: "telemetry".to_owned(),
        window_start_unix_seconds: 900,
      },
      snowflake_id: SnowflakeId(sequence),
      blob_key: BlobKey::new(format!("segment-{sequence}")),
      compression: Compression::none(),
      segment_index: HashMap::from([7, 8].map(|partition| {
        (
          partition,
          BatchMetadata {
            seq_range: SeqRange {
              start: sequence,
              end: sequence,
            },
            byte_range: ByteRange { start: 0, end: 1 },
            payload_bytes: 1,
          },
        )
      })),
      created_at: OffsetDateTime::UNIX_EPOCH,
      metadata_published_at: OffsetDateTime::UNIX_EPOCH,
    })
    .collect()
}

//
// ReaderMetadataCache
//

impl ReaderMetadataCache {
  pub(in crate::consumer) fn mature_keys(
    &self,
  ) -> impl Iterator<Item = MatureMetadataCacheKey> + '_ {
    self.entries.iter().filter_map(|(&window, entry)| {
      entry
        .mature_lower_bound
        .map(move |floor| window.with_min_snowflake((floor != SnowflakeId(0)).then_some(floor)))
    })
  }

  pub(in crate::consumer) fn progress_count(&self) -> usize {
    self.progress_entries.len()
  }
}

#[test]
fn recovery_positions_require_matching_snapshot_and_nonrewound_cursor() {
  let mut cache = ReaderMetadataCache::default();
  let key = window_key(7, 900).with_min_snowflake(Some(SnowflakeId(2)));
  cache.install_mature(key, &segments());
  cache.commit_progress(7, 900, Some(SnowflakeId(2)), Some(3));
  let snapshot = cache.mature(&key).unwrap();
  assert_eq!(
    cache.recovery_start(7, 900, snapshot, Some(SnowflakeId(2)), Some(3)),
    3
  );
  assert_eq!(
    cache.recovery_start(7, 900, snapshot, Some(SnowflakeId(2)), Some(1)),
    1
  );
  assert_eq!(
    cache.recovery_start(7, 900, snapshot, Some(SnowflakeId(5)), Some(3)),
    4
  );
  assert_eq!(cache.recovery_start(7, 900, snapshot, None, None), 0);
  let replacement = Arc::new(snapshot.as_ref().clone());
  assert_eq!(
    cache.recovery_start(7, 900, &replacement, Some(SnowflakeId(2)), Some(3)),
    1
  );
  assert!(
    snapshot
      .iter()
      .all(|segment| segment.segment_index.len() == 1)
  );
}

#[test]
fn staging_skips_cannot_jump_over_the_next_unexamined_row() {
  let mut cache = ReaderMetadataCache::default();
  let key = window_key(7, 900).with_min_snowflake(None);
  cache.install_mature(key, &segments());
  cache.stage_skip(7, 900, None, Some(3), SnowflakeId(2));
  assert_eq!(
    cache.recovery_start(7, 900, cache.mature(&key).unwrap(), None, Some(3)),
    0
  );
  cache.stage_skip(7, 900, None, Some(3), SnowflakeId(1));
  cache.stage_skip(7, 900, None, Some(3), SnowflakeId(3));
  assert_eq!(
    cache.recovery_start(7, 900, cache.mature(&key).unwrap(), None, Some(3)),
    1
  );
  cache.commit_progress(7, 900, None, Some(3));
  assert_eq!(
    cache.recovery_start(7, 900, cache.mature(&key).unwrap(), None, Some(3)),
    3
  );
}

#[test]
fn contiguous_extension_preserves_positions_but_disjoint_coverage_does_not() {
  let mut cache = ReaderMetadataCache::default();
  let key = window_key(7, 900);
  let rows = segments();
  let observed_at = OffsetDateTime::UNIX_EPOCH;
  assert!(cache.install_prefix(key, SnowflakeId(1), SnowflakeId(3), observed_at, &rows));
  cache.commit_progress(7, 900, None, Some(1));
  let original = Arc::clone(&cache.prefix(&key).unwrap().segments);
  assert!(cache.install_prefix(key, SnowflakeId(3), SnowflakeId(5), observed_at, &rows));
  let extended = Arc::clone(&cache.prefix(&key).unwrap().segments);
  assert!(!Arc::ptr_eq(&original, &extended));
  assert_eq!(original.len(), 2);
  assert!(ptr::eq(original.get(0).unwrap(), extended.get(0).unwrap()));
  assert_eq!(cache.prefix(&key).unwrap().lower_bound, SnowflakeId(1));
  assert_eq!(extended.len(), 4);
  assert_eq!(cache.recovery_start(7, 900, &extended, None, Some(1)), 1);
  assert!(!cache.install_prefix(key, SnowflakeId(2), SnowflakeId(4), observed_at, &rows));
  assert!(Arc::ptr_eq(
    &extended,
    &cache.prefix(&key).unwrap().segments
  ));
  assert!(cache.install_prefix(key, SnowflakeId(6), SnowflakeId(8), observed_at, &rows));
  let disjoint = &cache.prefix(&key).unwrap().segments;
  assert_eq!(disjoint.len(), 2);
  assert_eq!(cache.prefix(&key).unwrap().lower_bound, SnowflakeId(6));
  assert_eq!(cache.recovery_start(7, 900, disjoint, None, Some(1)), 0);
  assert_eq!(cache.progress_count(), 0);
}

#[test]
fn rollback_restores_positions_without_discarding_retained_observations() {
  let mut cache = ReaderMetadataCache::default();
  let rows = segments();
  let key = window_key(7, 900).with_min_snowflake(None);
  cache.install_mature(key, &rows);
  cache.commit_progress(7, 900, None, Some(1));
  let snapshot = Arc::clone(cache.mature(&key).unwrap());
  let checkpoint = cache.checkpoint_progress();
  cache.commit_progress(7, 900, None, Some(3));
  cache.install_mature(window_key(8, 1200).with_min_snowflake(None), &rows);
  cache.restore_progress(checkpoint);
  assert!(cache.has_mature(&window_key(8, 1200).with_min_snowflake(None)));
  assert!(Arc::ptr_eq(&snapshot, cache.mature(&key).unwrap()));
  assert_eq!(cache.recovery_start(7, 900, &snapshot, None, Some(1)), 1);
}

#[test]
fn rollback_drops_positions_whose_backing_was_replaced_or_evicted() {
  for evict in [false, true] {
    let mut cache = ReaderMetadataCache::default();
    let rows = segments();
    let key = window_key(7, 900);
    cache.install_prefix(
      key,
      SnowflakeId(1),
      SnowflakeId(3),
      OffsetDateTime::UNIX_EPOCH,
      &rows,
    );
    cache.commit_progress(7, 900, None, Some(1));
    let checkpoint = cache.checkpoint_progress();
    if evict {
      cache.enforce_budget(0, usize::MAX);
    } else {
      cache.install_prefix(
        key,
        SnowflakeId(3),
        SnowflakeId(5),
        OffsetDateTime::UNIX_EPOCH,
        &rows,
      );
    }
    cache.restore_progress(checkpoint);
    assert_eq!(cache.progress_count(), 0);
    assert_eq!(cache.prefix(&key).is_some(), !evict);
  }
}

#[test]
fn mature_and_prefix_entries_share_backing_and_charge_it_once() {
  let mut cache = ReaderMetadataCache::default();
  let rows = segments();
  let key = window_key(7, 900);
  cache.install_prefix(
    key,
    SnowflakeId(1),
    SnowflakeId(9),
    OffsetDateTime::UNIX_EPOCH,
    &rows,
  );
  let (_, prefix_bytes) = cache.footprint();
  cache.install_mature(key.with_min_snowflake(None), &rows);
  assert!(Arc::ptr_eq(
    &cache.prefix(&key).unwrap().segments,
    cache.mature(&key.with_min_snowflake(None)).unwrap()
  ));
  let entry_bytes = u64::try_from(
    (size_of::<SealedMetadataPrefix>() + size_of::<RecoveryMetadataProgress>() + 128) * 2,
  )
  .unwrap();
  assert_eq!(cache.footprint(), (2, prefix_bytes + entry_bytes));
  assert_eq!(cache.entries.len(), 1);
}

#[test]
fn byte_and_entry_limits_evict_coverage_and_progress_together() {
  for (max_bytes, max_entries) in [(u64::MAX, 2), (0, usize::MAX)] {
    let mut cache = ReaderMetadataCache::default();
    let rows = segments();
    cache.install_prefix(
      window_key(8, 900),
      SnowflakeId(1),
      SnowflakeId(9),
      OffsetDateTime::UNIX_EPOCH,
      &rows,
    );
    cache.install_mature(window_key(8, 900).with_min_snowflake(None), &rows);
    cache.install_mature(window_key(7, 1200).with_min_snowflake(None), &rows);
    cache.commit_progress(8, 900, None, Some(1));
    cache.commit_progress(7, 1200, None, Some(1));
    cache.enforce_budget(max_bytes, max_entries);
    assert!(cache.prefix(&window_key(8, 900)).is_none());
    assert!(!cache.has_mature(&window_key(8, 900).with_min_snowflake(None)));
    assert!(!cache.progress_entries.contains_key(&window_key(8, 900)));
    assert!(cache.retained_bytes() <= max_bytes);
    assert!(cache.entry_count() <= max_entries);
    assert_eq!(
      cache.has_mature(&window_key(7, 1200).with_min_snowflake(None)),
      max_bytes != 0
    );
    assert_eq!(cache.progress_count(), usize::from(max_bytes != 0));
  }
}

#[test]
fn window_coverage_has_independent_retention_and_one_mature_snapshot() {
  for retain_prefix in [false, true] {
    let mut cache = ReaderMetadataCache::default();
    let rows = segments();
    let key = window_key(7, 900);
    cache.install_prefix(
      key,
      SnowflakeId(1),
      SnowflakeId(9),
      OffsetDateTime::UNIX_EPOCH,
      &rows,
    );
    cache.install_mature(key.with_min_snowflake(None), &rows);
    cache.install_mature(key.with_min_snowflake(Some(SnowflakeId(3))), &rows);
    cache.install_mature(window_key(8, 1200).with_min_snowflake(None), &rows);
    assert_eq!(cache.entry_count(), 3);
    assert_eq!(
      cache.retained_window_keys(),
      HashSet::from([key, window_key(8, 1200)])
    );
    cache.commit_progress(7, 900, Some(SnowflakeId(3)), Some(4));
    let snapshot = Arc::clone(
      cache
        .mature(&key.with_min_snowflake(Some(SnowflakeId(3))))
        .unwrap(),
    );
    cache.retain_windows(
      |mature_key| !retain_prefix && mature_key.window == key,
      |window_key| retain_prefix && *window_key == key,
      |_| true,
    );
    assert_eq!(cache.entry_count(), 1);
    assert_eq!(cache.retained_window_keys(), HashSet::from([key]));
    assert_eq!(
      cache.has_mature(&key.with_min_snowflake(None)),
      !retain_prefix
    );
    assert_eq!(
      cache.has_mature(&key.with_min_snowflake(Some(SnowflakeId(3)))),
      !retain_prefix
    );
    assert_eq!(cache.prefix(&key).is_some(), retain_prefix);
    assert_eq!(
      cache.recovery_start(7, 900, &snapshot, Some(SnowflakeId(3)), Some(4)),
      4
    );
    cache.retain_windows(|_| false, |_| false, |_| false);
    assert!(cache.retained_window_keys().is_empty());
    assert_eq!(cache.footprint(), (0, 0));
  }
}

#[test]
fn mature_coverage_serves_narrower_floors_without_retaining_variants() {
  let mut cache = ReaderMetadataCache::default();
  let rows = segments();
  let window = window_key(7, 900);
  let bounded = window.with_min_snowflake(Some(SnowflakeId(3)));
  cache.install_mature(bounded, &rows[2 ..]);
  cache.commit_progress(7, 900, Some(SnowflakeId(3)), Some(4));
  let snapshot = Arc::clone(cache.mature(&bounded).unwrap());
  assert!(cache.has_mature(&window.with_min_snowflake(Some(SnowflakeId(5)))));
  assert!(!cache.has_mature(&window.with_min_snowflake(Some(SnowflakeId(2)))));
  assert!(!cache.has_mature(&window.with_min_snowflake(None)));

  cache.install_mature(window.with_min_snowflake(Some(SnowflakeId(5))), &rows[4 ..]);
  assert_eq!(cache.entry_count(), 1);
  assert!(Arc::ptr_eq(&snapshot, cache.mature(&bounded).unwrap()));
  assert_eq!(
    cache.recovery_start(7, 900, &snapshot, Some(SnowflakeId(3)), Some(4)),
    2
  );

  cache.install_mature(window.with_min_snowflake(None), &rows);
  let widened = cache.mature(&window.with_min_snowflake(None)).unwrap();
  assert_eq!(widened.len(), 8);
  assert_eq!(cache.entry_count(), 1);
  assert!(!Arc::ptr_eq(&snapshot, widened));
  assert_eq!(cache.progress_count(), 0);
}

#[test]
fn widening_coverage_without_new_rows_reconsiders_the_earlier_floor() {
  let mut cache = ReaderMetadataCache::default();
  let window = window_key(7, 900);
  let bounded = window.with_min_snowflake(Some(SnowflakeId(6)));
  let rows = segments();
  cache.install_mature(bounded, &rows);
  cache.commit_progress(7, 900, Some(SnowflakeId(6)), Some(1));
  let snapshot = Arc::clone(cache.mature(&bounded).unwrap());
  assert_eq!(
    cache.recovery_start(7, 900, &snapshot, Some(SnowflakeId(6)), Some(1)),
    5
  );

  cache.install_mature(window.with_min_snowflake(None), &rows);
  assert!(Arc::ptr_eq(
    &snapshot,
    cache.mature(&window.with_min_snowflake(None)).unwrap()
  ));
  assert_eq!(cache.recovery_start(7, 900, &snapshot, None, Some(1)), 0);
  cache.commit_progress(7, 900, None, Some(1));
  assert_eq!(cache.recovery_start(7, 900, &snapshot, None, Some(1)), 1);
}

#[test]
fn maturing_a_prefix_appends_the_tail_without_losing_progress() {
  let mut cache = ReaderMetadataCache::default();
  let rows = segments();
  let window = window_key(7, 900);
  cache.install_prefix(
    window,
    SnowflakeId(1),
    SnowflakeId(5),
    OffsetDateTime::UNIX_EPOCH,
    &rows,
  );
  cache.commit_progress(7, 900, Some(SnowflakeId(1)), Some(2));
  cache.install_mature(window.with_min_snowflake(Some(SnowflakeId(5))), &rows[4 ..]);
  let mature = cache
    .mature(&window.with_min_snowflake(Some(SnowflakeId(1))))
    .unwrap();
  assert_eq!(mature.len(), 8);
  assert!(Arc::ptr_eq(
    &cache.prefix(&window).unwrap().segments,
    mature
  ));
  assert_eq!(
    cache.recovery_start(7, 900, mature, Some(SnowflakeId(1)), Some(2)),
    2
  );
  assert!(!cache.has_mature(&window.with_min_snowflake(None)));
  assert_eq!(cache.entry_count(), 2);
}

#[test]
fn disjoint_prefix_and_mature_coverage_do_not_prove_the_gap() {
  let mut cache = ReaderMetadataCache::default();
  let rows = segments();
  let window = window_key(7, 900);
  cache.install_prefix(
    window,
    SnowflakeId(1),
    SnowflakeId(3),
    OffsetDateTime::UNIX_EPOCH,
    &rows,
  );
  cache.install_mature(window.with_min_snowflake(Some(SnowflakeId(6))), &rows[5 ..]);
  assert!(!cache.has_mature(&window.with_min_snowflake(Some(SnowflakeId(3)))));
  assert!(!cache.has_mature(&window.with_min_snowflake(Some(SnowflakeId(1)))));
  assert_eq!(cache.prefix(&window).unwrap().sealed_before, SnowflakeId(3));
  assert_eq!(
    cache
      .mature(&window.with_min_snowflake(Some(SnowflakeId(6))))
      .unwrap()
      .len(),
    5
  );
  cache.commit_progress(7, 900, Some(SnowflakeId(6)), Some(6));

  cache.install_prefix(
    window,
    SnowflakeId(3),
    SnowflakeId(6),
    OffsetDateTime::UNIX_EPOCH,
    &rows,
  );
  let mature = cache
    .mature(&window.with_min_snowflake(Some(SnowflakeId(1))))
    .unwrap();
  assert_eq!(mature.len(), 8);
  assert_eq!(cache.progress_count(), 0);
  assert!(!cache.has_mature(&window.with_min_snowflake(None)));
}

#[test]
fn strong_seal_extension_reuses_the_mature_snapshot() {
  let mut cache = ReaderMetadataCache::default();
  let rows = segments();
  let window = window_key(7, 900);
  let request = window.with_min_snowflake(Some(SnowflakeId(1)));
  cache.install_mature(request, &rows);
  cache.commit_progress(7, 900, Some(SnowflakeId(1)), Some(2));
  let snapshot = Arc::clone(cache.mature(&request).unwrap());
  for (lower_bound, sealed_before) in [(1, 3), (3, 5)] {
    assert!(cache.install_prefix(
      window,
      SnowflakeId(lower_bound),
      SnowflakeId(sealed_before),
      OffsetDateTime::UNIX_EPOCH,
      &rows,
    ));
    let prefix = cache.prefix(&window).unwrap();
    assert_eq!(prefix.lower_bound, SnowflakeId(1));
    assert_eq!(prefix.sealed_before, SnowflakeId(sealed_before));
    assert!(Arc::ptr_eq(&prefix.segments, &snapshot));
    assert_eq!(
      cache.recovery_start(7, 900, &snapshot, Some(SnowflakeId(1)), Some(2)),
      2
    );
  }
}

#[test]
fn partition_and_policy_invalidation_discard_all_associated_state() {
  for change_consistency in [false, true] {
    let mut cache = ReaderMetadataCache::default();
    let rows = segments();
    let strong = MetadataReadConsistency::Strong;
    assert!(!cache.validate_policy(strong, Duration::seconds(15)));
    cache.install_mature(window_key(7, 900).with_min_snowflake(None), &rows);
    cache.install_mature(window_key(8, 900).with_min_snowflake(None), &rows);
    cache.commit_progress(7, 900, None, Some(1));
    cache.commit_progress(8, 900, None, Some(1));
    cache.retain_partitions(&HashSet::from([7]));
    assert!(!cache.has_mature(&window_key(8, 900).with_min_snowflake(None)));
    assert_eq!(cache.progress_count(), 1);
    cache.invalidate_partition(7);
    assert_eq!(cache.footprint(), (0, 0));
    assert_eq!(cache.progress_count(), 0);
    cache.install_prefix(
      window_key(7, 900),
      SnowflakeId(1),
      SnowflakeId(9),
      OffsetDateTime::UNIX_EPOCH,
      &rows,
    );
    cache.commit_progress(7, 900, None, Some(1));
    assert!(!cache.validate_policy(strong, Duration::seconds(15)));
    let consistency = if change_consistency {
      MetadataReadConsistency::Eventual
    } else {
      strong
    };
    let horizon = if change_consistency {
      Duration::seconds(15)
    } else {
      Duration::seconds(16)
    };
    assert!(cache.validate_policy(consistency, horizon));
    assert_eq!(cache.footprint(), (0, 0));
    assert_eq!(cache.progress_count(), 0);
    assert!(cache.entries.is_empty());
  }
}
