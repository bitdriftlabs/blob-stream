// blob-stream - consumer configuration
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::{Result, ensure};
use blob_stream_types::VirtualPartitionId;
use std::collections::{HashMap, HashSet};

//
// ConsumerReadConfig
//

#[derive(Clone, Debug)]
pub struct ConsumerReadConfig {
  pub topic: String,
  pub window_size_seconds: i64,
  pub lookback_windows: u32,
  pub assigned_virtual_partitions: Vec<VirtualPartitionId>,
  pub initial_cursors: HashMap<VirtualPartitionId, u64>,
}

impl ConsumerReadConfig {
  pub fn validate(&self) -> Result<()> {
    // Validate fundamental runtime parameters first so callers fail fast on invalid wiring.
    ensure!(!self.topic.trim().is_empty(), "consumer topic is required");
    ensure!(
      self.window_size_seconds > 0,
      "window_size_seconds must be greater than zero"
    );
    ensure!(
      self.lookback_windows > 0,
      "lookback_windows must be greater than zero"
    );
    ensure!(
      !self.assigned_virtual_partitions.is_empty(),
      "at least one virtual partition must be assigned"
    );

    // Duplicate partition assignment would cause duplicate reads and ambiguous cursor ownership.
    let mut seen = HashSet::new();
    for partition_id in &self.assigned_virtual_partitions {
      ensure!(
        seen.insert(*partition_id),
        "duplicate virtual partition assignment: {partition_id}"
      );
    }

    Ok(())
  }
}

//
// ConsumerGroupConfig
//

#[derive(Clone, Debug)]
pub struct ConsumerGroupConfig {
  pub topic: String,
  pub group_id: String,
  pub member_id: String,
  pub lease_duration_ms: i64,
}

impl ConsumerGroupConfig {
  pub fn validate(&self) -> Result<()> {
    ensure!(
      !self.topic.trim().is_empty(),
      "consumer group topic is required"
    );
    ensure!(
      !self.group_id.trim().is_empty(),
      "consumer group id is required"
    );
    ensure!(
      !self.member_id.trim().is_empty(),
      "consumer member id is required"
    );
    ensure!(
      self.lease_duration_ms > 0,
      "lease_duration_ms must be greater than zero"
    );
    Ok(())
  }
}
