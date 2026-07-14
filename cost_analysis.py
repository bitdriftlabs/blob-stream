#!/usr/bin/env python3
"""Hourly request-cost model for blob-stream (DynamoDB + S3).

This script converts the COST_ANALYSIS.md model into executable math with:
- all assumptions documented as comments near the formulas
- default values for every model variable
- a clear printed breakdown for sanity-checking

Update the defaults in `DEFAULTS` with your real workload and regional pricing.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil


@dataclass(frozen=True)
class Inputs:
  # ---------- Pricing inputs ----------
  # DynamoDB on-demand price per 1,000,000 write/read request units.
  p_ddb_w_million: float
  p_ddb_r_million: float

  # S3 request prices per 1,000 requests.
  p_s3_put_1k: float
  p_s3_get_1k: float

  # ---------- Throughput / topology ----------
  # Ingest volume and average payload size before compression.
  r_ingest_per_hour: float
  b_record_bytes: float

  # Number of consumer groups reading the same topic.
  g_consumer_groups: float

  # Producer-side ownership scope (this broker instance).
  p_owned_prod: float

  # Consumer-side owned partitions across all instances in a group.
  p_owned_cons_total: float

  # Number of consumer instances in one consumer group.
  n_cons: float

  # ---------- Broker/consumer behavior ----------
  flush_max_bytes: float
  flush_max_delay_s: float
  lease_duration_prod_s: float
  reservation_size: float

  window_size_s: float
  lookback_windows: float
  metadata_recovery_scan_interval_s: float
  metadata_fast_scan_enabled: bool
  heartbeat_cons_s: float
  rebalance_interval_s: float

  # Average calls to read_available() per consumer instance per second.
  polls_per_s: float

  # ---------- Query pagination/cardinality ----------
  # Average number of pages returned by each query type.
  pages_per_scan_window_query: float
  pages_per_membership_query: float

  # ---------- DynamoDB RU size approximations ----------
  # Average item size in KB for each table operation type.
  kb_seg_item: float
  kb_lease_prod: float
  kb_lease_cons: float
  kb_membership: float

  # Average KB scanned/read per query page.
  kb_scanned_per_window_query: float
  kb_membership_query_page: float

  # Read consistency factor:
  # - eventually consistent reads: 0.5
  # - strongly consistent reads:   1.0
  c_read: float

  # ---------- Operational overhead factors ----------
  # Optional multipliers for retries/contention overhead.
  f_write_conflict_overhead: float
  f_read_conflict_overhead: float

  # ---------- Derived-rate fallback knobs ----------
  # If `s_seg_per_hour` is <= 0, the script estimates it from size/time triggers.
  effective_segment_payload_bytes: float
  active_partition_buffers: float

  # If `s_batch_read_per_hour` is <= 0, the script estimates from ingest + avg records per batch.
  avg_records_per_batch: float

  # Optional direct overrides. Set >0 to bypass fallback estimators.
  s_seg_per_hour: float
  s_batch_read_per_hour: float


def estimate_segments_per_hour(i: Inputs) -> float:
  """Estimate flushed segments/hour if not directly provided.

  S_size_trigger ~= ingest bytes/hour / effective segment payload bytes
  S_time_trigger ~= active buffers * (3600 / flush_max_delay_s)
  S_seg ~= max(S_size_trigger, S_time_trigger)

  This is intentionally rough because real flush behavior depends on partition activity skew.
  """
  s_size_trigger = (i.r_ingest_per_hour * i.b_record_bytes) / max(
    i.effective_segment_payload_bytes,
    1.0,
  )
  s_time_trigger = i.active_partition_buffers * (3600.0 / max(i.flush_max_delay_s, 1e-9))
  return max(s_size_trigger, s_time_trigger)


def estimate_batch_reads_per_hour(i: Inputs) -> float:
  """Estimate S3 range GET count if not directly provided.

  Current consumer code path performs one range GET per accepted batch.
  Approximation:
    new_batches_per_hour ~= R_ingest / avg_records_per_batch
    S_batch_read ~= G * new_batches_per_hour
  """
  new_batches_per_hour = i.r_ingest_per_hour / max(i.avg_records_per_batch, 1.0)
  return i.g_consumer_groups * new_batches_per_hour


def compute(i: Inputs) -> dict[str, float]:
  # Resolve primary S3 request drivers (use overrides if provided).
  s_seg = i.s_seg_per_hour if i.s_seg_per_hour > 0 else estimate_segments_per_hour(i)
  s_batch_read = (
    i.s_batch_read_per_hour
    if i.s_batch_read_per_hour > 0
    else estimate_batch_reads_per_hour(i)
  )

  # ---------- S3 request counts ----------
  # Broker: one PutObject per flushed segment.
  req_s3_put = s_seg

  # Consumer: one ranged GetObject per accepted batch.
  req_s3_get = s_batch_read

  # ---------- DynamoDB write request counts ----------
  # Segment metadata write per flushed segment.
  req_ddb_w_segment = s_seg

  # Producer lease maintenance loop runs roughly every lease_duration / 3 seconds.
  req_ddb_w_prod_lease = i.p_owned_prod * (
    3600.0 / max(i.lease_duration_prod_s / 3.0, 1.0)
  )

  # Sequence reservations are amortized by reservation_size.
  req_ddb_w_prod_reserve = i.r_ingest_per_hour / max(i.reservation_size, 1.0)

  # Consumer lease heartbeats and reassignments.
  req_ddb_w_cons_lease_hb = i.p_owned_cons_total * (
    3600.0 / max(i.heartbeat_cons_s, 1e-9)
  )
  req_ddb_w_cons_assign = i.p_owned_cons_total * (
    3600.0 / max(i.rebalance_interval_s, 1e-9)
  )

  # Membership heartbeat writes (register/heartbeat path modeled at heartbeat frequency).
  req_ddb_w_membership_hb = i.n_cons * (3600.0 / max(i.heartbeat_cons_s, 1e-9))

  req_ddb_w_total = (
    req_ddb_w_segment
    + req_ddb_w_prod_lease
    + req_ddb_w_prod_reserve
    + req_ddb_w_cons_lease_hb
    + req_ddb_w_cons_assign
    + req_ddb_w_membership_hb
  )
  req_ddb_w_total *= 1.0 + max(i.f_write_conflict_overhead, 0.0)

  # ---------- DynamoDB read request counts ----------
  # Metadata fast path: one current-window query per poll, times pagination. Recovery scans
  # revisit every lookback window at the configured cadence to find lower-snowflake stragglers.
  if i.metadata_fast_scan_enabled:
    req_ddb_r_scan_fast = (
      i.n_cons * (i.polls_per_s * 3600.0) * i.pages_per_scan_window_query
    )
    req_ddb_r_scan_recovery = (
      i.n_cons
      * (3600.0 / max(i.metadata_recovery_scan_interval_s, 1e-9))
      * i.lookback_windows
      * i.pages_per_scan_window_query
    )
  else:
    # Rollback mode retains the legacy unbounded lookback scan for every poll.
    req_ddb_r_scan_fast = 0.0
    req_ddb_r_scan_recovery = (
      i.n_cons
      * (i.polls_per_s * 3600.0)
      * i.lookback_windows
      * i.pages_per_scan_window_query
    )
  req_ddb_r_scan = req_ddb_r_scan_fast + req_ddb_r_scan_recovery

  # Membership snapshot query at rebalance cadence, times pagination.
  req_ddb_r_membership = (
    i.n_cons
    * (3600.0 / max(i.rebalance_interval_s, 1e-9))
    * i.pages_per_membership_query
  )

  req_ddb_r_total = req_ddb_r_scan + req_ddb_r_membership
  req_ddb_r_total *= 1.0 + max(i.f_read_conflict_overhead, 0.0)

  # ---------- Convert requests to DynamoDB RU ----------
  # Writes: 1 WRU per 1 KB chunk (rounded up).
  wru_hour = (
    req_ddb_w_segment * ceil(i.kb_seg_item / 1.0)
    + req_ddb_w_prod_lease * ceil(i.kb_lease_prod / 1.0)
    + req_ddb_w_prod_reserve * ceil(i.kb_lease_prod / 1.0)
    + req_ddb_w_cons_lease_hb * ceil(i.kb_lease_cons / 1.0)
    + req_ddb_w_cons_assign * ceil(i.kb_lease_cons / 1.0)
    + req_ddb_w_membership_hb * ceil(i.kb_membership / 1.0)
  )

  # Reads: 1 RRU per 4 KB strongly-consistent read, 0.5 for eventual consistency.
  rru_hour = (
    req_ddb_r_scan * ceil(i.kb_scanned_per_window_query / 4.0) * i.c_read
    + req_ddb_r_membership * ceil(i.kb_membership_query_page / 4.0) * i.c_read
  )

  # ---------- Hourly cost ----------
  cost_ddb_hour = (wru_hour / 1_000_000.0) * i.p_ddb_w_million + (
    rru_hour / 1_000_000.0
  ) * i.p_ddb_r_million

  cost_s3_hour = (req_s3_put / 1000.0) * i.p_s3_put_1k + (req_s3_get / 1000.0) * i.p_s3_get_1k

  cost_total_hour = cost_ddb_hour + cost_s3_hour

  return {
    "s_seg_per_hour": s_seg,
    "s_batch_read_per_hour": s_batch_read,
    "req_s3_put": req_s3_put,
    "req_s3_get": req_s3_get,
    "req_ddb_w_total": req_ddb_w_total,
    "req_ddb_r_scan_fast": req_ddb_r_scan_fast,
    "req_ddb_r_scan_recovery": req_ddb_r_scan_recovery,
    "req_ddb_r_total": req_ddb_r_total,
    "wru_hour": wru_hour,
    "rru_hour": rru_hour,
    "cost_ddb_hour": cost_ddb_hour,
    "cost_s3_hour": cost_s3_hour,
    "cost_total_hour": cost_total_hour,
  }


def print_report(i: Inputs, result: dict[str, float]) -> None:
  """Pretty-print an hourly cost breakdown."""
  print("blob-stream hourly request-cost estimate (DynamoDB + S3)")
  print("=" * 62)
  print(f"Segments/hour (S3 PUT driver):       {result['s_seg_per_hour']:,.2f}")
  print(f"Batch reads/hour (S3 GET driver):    {result['s_batch_read_per_hour']:,.2f}")
  print(f"S3 PUT requests/hour:                {result['req_s3_put']:,.2f}")
  print(f"S3 GET requests/hour:                {result['req_s3_get']:,.2f}")
  print(f"Dynamo write requests/hour:          {result['req_ddb_w_total']:,.2f}")
  print(f"Dynamo fast scan requests/hour:      {result['req_ddb_r_scan_fast']:,.2f}")
  print(f"Dynamo recovery scan requests/hour:  {result['req_ddb_r_scan_recovery']:,.2f}")
  print(f"Dynamo read requests/hour:           {result['req_ddb_r_total']:,.2f}")
  print(f"Dynamo WRU/hour:                     {result['wru_hour']:,.2f}")
  print(f"Dynamo RRU/hour:                     {result['rru_hour']:,.2f}")
  print("-" * 62)
  print(f"DynamoDB cost/hour:                  ${result['cost_ddb_hour']:,.4f}")
  print(f"S3 request cost/hour:                ${result['cost_s3_hour']:,.4f}")
  print(f"TOTAL cost/hour:                     ${result['cost_total_hour']:,.4f}")


# Default values are intentionally explicit so you can adjust a single block and re-run.
# Prices below are placeholders/examples; replace with your region's current rates.
DEFAULTS = Inputs(
  # Pricing inputs (example values; update these first).
  p_ddb_w_million=1.25,
  p_ddb_r_million=0.25,
  p_s3_put_1k=0.005,
  p_s3_get_1k=0.0004,

  # Throughput/topology.
  r_ingest_per_hour=10_000_000,
  b_record_bytes=350.0,
  g_consumer_groups=1.0,
  p_owned_prod=128.0,
  p_owned_cons_total=128.0,
  n_cons=8.0,

  # Runtime behavior/config.
  flush_max_bytes=64.0 * 1024.0 * 1024.0,
  flush_max_delay_s=1.0,
  lease_duration_prod_s=30.0,
  reservation_size=1000.0,
  window_size_s=300.0,
  lookback_windows=2.0,
  metadata_recovery_scan_interval_s=60.0,
  metadata_fast_scan_enabled=True,
  heartbeat_cons_s=10.0,
  rebalance_interval_s=10.0,
  polls_per_s=2.0,

  # Pagination/cardinality assumptions.
  pages_per_scan_window_query=1.2,
  pages_per_membership_query=1.0,

  # Item/query sizes (KB).
  kb_seg_item=3.0,
  kb_lease_prod=1.0,
  kb_lease_cons=1.0,
  kb_membership=1.0,
  kb_scanned_per_window_query=8.0,
  kb_membership_query_page=4.0,

  # Eventual consistency by default in current code paths.
  c_read=0.5,

  # No extra contention overhead by default.
  f_write_conflict_overhead=0.0,
  f_read_conflict_overhead=0.0,

  # Fallback estimation knobs.
  effective_segment_payload_bytes=32.0 * 1024.0 * 1024.0,
  active_partition_buffers=16.0,
  avg_records_per_batch=1000.0,

  # Set <= 0 to auto-estimate; set > 0 to override directly.
  s_seg_per_hour=0.0,
  s_batch_read_per_hour=0.0,
)


def main() -> None:
  result = compute(DEFAULTS)
  print_report(DEFAULTS, result)


if __name__ == "__main__":
  main()
