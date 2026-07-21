//! Per-virtual-partition lifecycle state for `ConsumerReaderImpl`.
//!
//! Cursor hydration can arrive before a coordinator assignment. Pending variants retain that
//! durable recovery intent but are never scanned. Assignment activates the corresponding variant,
//! and revocation removes the state from the reader entirely because the consumer-group lease is
//! the durable source of truth for a later owner.
//!
//! Cursor replacement is reserved for explicit seeks. Normal reads and hydrated commits advance
//! monotonically, which makes unordered metadata scans and boundary-row replays safe.

use super::ConsumerReaderPartitionScanState;
use blob_stream_types::{SnowflakeId, VirtualPartitionId};
use std::sync::Arc;

//
// RecoveryState
//

/// Inclusive recovery range from the next unread window through the captured live cutover.
#[derive(Clone, Debug)]
pub struct RecoveryState {
  pub next_window_start_unix_seconds: i64,
  pub cutover_window_start_unix_seconds: i64,
  pub first_window_start_unix_seconds: Option<i64>,
  pub first_window_min_snowflake: Option<SnowflakeId>,
}

//
// VirtualPartitionState
//

/// Lifecycle, read mode, cursor, and latest scan diagnostic for one virtual partition.
#[derive(Clone, Debug)]
pub enum VirtualPartitionState {
  /// A hydrated cursor that must not scan until the coordinator assigns this partition.
  PendingCursor {
    cursor: u64,
    last_scan: Option<Arc<ConsumerReaderPartitionScanState>>,
  },
  /// Recovery intent captured before assignment; it becomes `Recovering` when assigned.
  PendingRecovering {
    cursor: u64,
    recovery_state: RecoveryState,
    last_scan: Option<Arc<ConsumerReaderPartitionScanState>>,
  },
  /// A cursor ready for current-window scanning once assignment activates it.
  PendingFast {
    cursor: u64,
    last_scan: Option<Arc<ConsumerReaderPartitionScanState>>,
  },
  /// A newly assigned partition scanning its current aligned window exactly once.
  Fresh {
    cursor: Option<u64>,
    initial_window_start_unix_seconds: i64,
    last_scan: Option<Arc<ConsumerReaderPartitionScanState>>,
  },
  /// A resumed or explicitly sought partition scanning bounded retained history.
  Recovering {
    cursor: Option<u64>,
    recovery_state: RecoveryState,
    last_scan: Option<Arc<ConsumerReaderPartitionScanState>>,
  },
  /// A partition scanning the bounded live publication horizon.
  Fast {
    cursor: Option<u64>,
    last_scan: Option<Arc<ConsumerReaderPartitionScanState>>,
  },
}

impl VirtualPartitionState {
  /// Pending variants deliberately exclude a partition from reader scans.
  pub(super) fn is_assigned(&self) -> bool {
    !matches!(
      self,
      Self::PendingCursor { .. } | Self::PendingRecovering { .. } | Self::PendingFast { .. }
    )
  }

  /// Return the last consumed sequence offset, if the partition has consumed any data.
  pub(super) fn cursor(&self) -> Option<u64> {
    match self {
      Self::PendingCursor { cursor, .. }
      | Self::PendingRecovering { cursor, .. }
      | Self::PendingFast { cursor, .. } => Some(*cursor),
      Self::Fresh { cursor, .. } | Self::Recovering { cursor, .. } | Self::Fast { cursor, .. } => {
        *cursor
      },
    }
  }

  /// Replace the cursor exactly for an explicit caller-directed seek.
  pub(super) fn set_cursor(&mut self, cursor: u64) {
    match self {
      Self::PendingCursor {
        cursor: current_cursor,
        ..
      }
      | Self::PendingRecovering {
        cursor: current_cursor,
        ..
      }
      | Self::PendingFast {
        cursor: current_cursor,
        ..
      } => *current_cursor = cursor,
      Self::Fresh {
        cursor: current_cursor,
        ..
      }
      | Self::Recovering {
        cursor: current_cursor,
        ..
      }
      | Self::Fast {
        cursor: current_cursor,
        ..
      } => *current_cursor = Some(cursor),
    }
  }

  /// Advance monotonically after reads or durable cursor hydration; never rewind implicitly.
  pub(super) fn advance_cursor(&mut self, cursor: u64) {
    self.set_cursor(self.cursor().map_or(cursor, |current| current.max(cursor)));
  }

  /// Activate pending state without altering the already chosen recovery or fast-path mode.
  pub(super) fn into_assigned(self, initial_window_start_unix_seconds: i64) -> Self {
    match self {
      Self::PendingCursor { cursor, last_scan } => Self::Fresh {
        cursor: Some(cursor),
        initial_window_start_unix_seconds,
        last_scan,
      },
      Self::PendingRecovering {
        cursor,
        recovery_state,
        last_scan,
      } => Self::Recovering {
        cursor: Some(cursor),
        recovery_state,
        last_scan,
      },
      Self::PendingFast { cursor, last_scan } => Self::Fast {
        cursor: Some(cursor),
        last_scan,
      },
      state @ (Self::Fresh { .. } | Self::Recovering { .. } | Self::Fast { .. }) => state,
    }
  }

  /// Create the initial current-window-only state for a partition without a durable cursor.
  pub(super) fn fresh(initial_window_start_unix_seconds: i64) -> Self {
    Self::Fresh {
      cursor: None,
      initial_window_start_unix_seconds,
      last_scan: None,
    }
  }

  /// Start bounded recovery while preserving whether the partition is currently assigned.
  pub(super) fn start_recovery(&mut self, recovery_state: RecoveryState) {
    let cursor = self.cursor();
    let last_scan = self.last_scan_handle();
    *self = if self.is_assigned() {
      Self::Recovering {
        cursor,
        recovery_state,
        last_scan,
      }
    } else {
      Self::PendingRecovering {
        cursor: cursor.unwrap_or(0),
        recovery_state,
        last_scan,
      }
    };
  }

  /// Return the most recent completed scan diagnostic, if this partition has scanned.
  pub(super) fn last_scan(&self) -> Option<&ConsumerReaderPartitionScanState> {
    match self {
      Self::PendingCursor { last_scan, .. }
      | Self::PendingRecovering { last_scan, .. }
      | Self::PendingFast { last_scan, .. }
      | Self::Fresh { last_scan, .. }
      | Self::Recovering { last_scan, .. }
      | Self::Fast { last_scan, .. } => last_scan.as_deref(),
    }
  }

  /// Clone the retained diagnostic handle without copying the diagnostic contents.
  pub(super) fn last_scan_handle(&self) -> Option<Arc<ConsumerReaderPartitionScanState>> {
    match self {
      Self::PendingCursor { last_scan, .. }
      | Self::PendingRecovering { last_scan, .. }
      | Self::PendingFast { last_scan, .. }
      | Self::Fresh { last_scan, .. }
      | Self::Recovering { last_scan, .. }
      | Self::Fast { last_scan, .. } => Some(Arc::clone(last_scan.as_ref()?)),
    }
  }

  /// Replace the last successful scan diagnostic for this partition.
  pub(super) fn set_last_scan(&mut self, scan: ConsumerReaderPartitionScanState) {
    match self {
      Self::PendingCursor { last_scan, .. }
      | Self::PendingRecovering { last_scan, .. }
      | Self::PendingFast { last_scan, .. }
      | Self::Fresh { last_scan, .. }
      | Self::Recovering { last_scan, .. }
      | Self::Fast { last_scan, .. } => *last_scan = Some(Arc::new(scan)),
    }
  }

  /// Return the diagnostic read mode; a pending cursor has no active reader mode yet.
  pub(super) fn reader_mode(&self) -> Option<ConsumerReaderPartitionMode> {
    match self {
      Self::PendingRecovering { recovery_state, .. } | Self::Recovering { recovery_state, .. } => {
        Some(ConsumerReaderPartitionMode::Recovering {
          next_window_start_unix_seconds: recovery_state.next_window_start_unix_seconds,
          cutover_window_start_unix_seconds: recovery_state.cutover_window_start_unix_seconds,
        })
      },
      Self::PendingFast { .. } | Self::Fast { .. } => Some(ConsumerReaderPartitionMode::Fast),
      Self::Fresh {
        initial_window_start_unix_seconds,
        ..
      } => Some(ConsumerReaderPartitionMode::Fresh {
        initial_window_start_unix_seconds: *initial_window_start_unix_seconds,
      }),
      Self::PendingCursor { .. } => None,
    }
  }
}

//
// ConsumerReaderPartitionMode
//

/// Active scan mode exposed through reader diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerReaderPartitionMode {
  Fresh {
    initial_window_start_unix_seconds: i64,
  },
  Recovering {
    next_window_start_unix_seconds: i64,
    cutover_window_start_unix_seconds: i64,
  },
  Fast,
}

//
// ConsumerReaderPartitionState
//

/// Read mode for one partition, ordered by id when published for diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerReaderPartitionState {
  pub virtual_partition_id: VirtualPartitionId,
  pub mode: ConsumerReaderPartitionMode,
}
