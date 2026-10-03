#![allow(clippy::unwrap_used)]

use super::{ReaderMetadataCache, RecoveryMetadataProgress, SealedMetadataPrefix};
use blob_stream_blob_store::{BlobKey, ByteRange};
use blob_stream_metadata_store::{MetadataReadConsistency, SegmentMetadata};
use blob_stream_types::{BatchMetadata, Compression, SeqRange, SnowflakeId, TopicWindowKey};
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

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

#[test]
fn recovery_positions_require_matching_snapshot_and_nonrewound_cursor() {
  let mut cache = ReaderMetadataCache::default();
  let key = (7, 900, Some(2));
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
  let replacement: Arc<[SegmentMetadata]> = snapshot.to_vec().into();
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
  let key = (7, 900, None);
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
  let key = (7, 900);
  let rows = segments();
  let observed_at = OffsetDateTime::UNIX_EPOCH;
  assert!(cache.install_prefix(key, SnowflakeId(1), SnowflakeId(3), observed_at, &rows));
  cache.commit_progress(7, 900, None, Some(1));
  let original = Arc::clone(&cache.prefix(&key).unwrap().segments);
  assert!(cache.install_prefix(key, SnowflakeId(3), SnowflakeId(5), observed_at, &rows));
  let extended = Arc::clone(&cache.prefix(&key).unwrap().segments);
  assert!(!Arc::ptr_eq(&original, &extended));
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
  let key = (7, 900, None);
  cache.install_mature(key, &rows);
  cache.commit_progress(7, 900, None, Some(1));
  let snapshot = Arc::clone(cache.mature(&key).unwrap());
  let checkpoint = cache.checkpoint_progress();
  cache.commit_progress(7, 900, None, Some(3));
  cache.install_mature((8, 1200, None), &rows);
  cache.restore_progress(checkpoint);
  assert!(cache.has_mature(&(8, 1200, None)));
  assert!(Arc::ptr_eq(&snapshot, cache.mature(&key).unwrap()));
  assert_eq!(cache.recovery_start(7, 900, &snapshot, None, Some(1)), 1);
}

#[test]
fn rollback_drops_positions_whose_backing_was_replaced_or_evicted() {
  for evict in [false, true] {
    let mut cache = ReaderMetadataCache::default();
    let rows = segments();
    let key = (7, 900);
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
    assert_eq!(cache.prefix_count(), usize::from(!evict));
  }
}

#[test]
fn mature_and_prefix_entries_share_backing_and_charge_it_once() {
  let mut cache = ReaderMetadataCache::default();
  let rows = segments();
  let key = (7, 900);
  cache.install_prefix(
    key,
    SnowflakeId(1),
    SnowflakeId(9),
    OffsetDateTime::UNIX_EPOCH,
    &rows,
  );
  let (_, prefix_bytes) = cache.footprint();
  cache.install_mature((7, 900, None), &rows);
  assert!(Arc::ptr_eq(
    &cache.prefix(&key).unwrap().segments,
    cache.mature(&(7, 900, None)).unwrap()
  ));
  let entry_bytes = u64::try_from(
    (size_of::<SealedMetadataPrefix>() + size_of::<RecoveryMetadataProgress>() + 128) * 2,
  )
  .unwrap();
  assert_eq!(cache.footprint(), (2, prefix_bytes + entry_bytes));
  assert_eq!(cache.backing_bytes.len(), 1);
}

#[test]
fn byte_and_entry_limits_evict_coverage_and_progress_together() {
  for (max_bytes, max_entries) in [(u64::MAX, 2), (0, usize::MAX)] {
    let mut cache = ReaderMetadataCache::default();
    let rows = segments();
    cache.install_prefix(
      (7, 900),
      SnowflakeId(1),
      SnowflakeId(9),
      OffsetDateTime::UNIX_EPOCH,
      &rows,
    );
    cache.install_mature((7, 900, None), &rows);
    cache.install_mature((8, 1200, None), &rows);
    cache.commit_progress(7, 900, None, Some(1));
    cache.commit_progress(8, 1200, None, Some(1));
    cache.enforce_budget(max_bytes, max_entries);
    assert!(cache.prefix(&(7, 900)).is_none());
    assert!(!cache.has_mature(&(7, 900, None)));
    assert!(!cache.progress_entries.contains_key(&(7, 900)));
    assert!(cache.retained_bytes() <= max_bytes);
    assert!(cache.entry_count() <= max_entries);
    assert_eq!(cache.has_mature(&(8, 1200, None)), max_bytes != 0);
    assert_eq!(cache.progress_count(), usize::from(max_bytes != 0));
  }
}

#[test]
fn partition_and_policy_invalidation_discard_all_associated_state() {
  for change_consistency in [false, true] {
    let mut cache = ReaderMetadataCache::default();
    let rows = segments();
    let strong = MetadataReadConsistency::Strong;
    assert!(!cache.validate_policy(strong, Duration::seconds(15)));
    cache.install_mature((7, 900, None), &rows);
    cache.install_mature((8, 900, None), &rows);
    cache.commit_progress(7, 900, None, Some(1));
    cache.commit_progress(8, 900, None, Some(1));
    cache.retain_partitions(&HashSet::from([7]));
    assert!(!cache.has_mature(&(8, 900, None)));
    assert_eq!(cache.progress_count(), 1);
    cache.invalidate_partition(7);
    assert_eq!(cache.footprint(), (0, 0));
    assert_eq!(cache.progress_count(), 0);
    cache.install_prefix(
      (7, 900),
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
    assert!(cache.backing_bytes.is_empty());
  }
}
