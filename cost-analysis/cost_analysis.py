#!/usr/bin/env python3
"""Hourly request-cost model for blob-stream (DynamoDB + S3).

The model follows current behavior: bounded Fast scans, assignment-driven recovery,
topic-coalesced time flushes, and one S3 range read per selected segment. Prefer direct
observed request-rate overrides over the fallback estimators.
"""

from __future__ import annotations

from argparse import ArgumentParser
from dataclasses import dataclass, fields, replace
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
  flush_max_delay_s: float
  lease_duration_prod_s: float
  reservation_size: float
  heartbeat_cons_s: float
  rebalance_interval_s: float
  polls_per_s: float

  # Fast scans query this many physical metadata windows per read pass on average. The default is
  # one, with a short second-window overlap at 300-second window rollover.
  fast_window_queries_per_poll: float
  # Fresh/Recovering scans are assignment-driven, not steady Fast work. This is the observed
  # aggregate window-query rate across all readers, before pagination.
  recovery_window_queries_per_hour: float
  metadata_eventual_reads: bool
  transactional_metadata_writes: bool
  avg_partitions_per_segment: float

  # ---------- Query pagination/cardinality ----------
  pages_per_metadata_query: float
  # Current consumer-group coordination reads are strongly consistent. This includes Gets and
  # membership queries per consumer per rebalance.
  coordination_strong_reads_per_rebalance: float

  # ---------- DynamoDB RU size approximations ----------
  # Average item size in KB for each table operation type.
  kb_seg_item: float
  kb_lease_prod: float
  kb_lease_cons: float
  kb_membership: float
  kb_assignment_plan: float
  kb_scanned_per_metadata_query_page: float
  kb_coordination_read: float

  # ---------- Operational overhead factors ----------
  # Optional multipliers for retries/contention overhead.
  f_write_conflict_overhead: float
  f_read_conflict_overhead: float

  # ---------- Derived-rate fallback knobs ----------
  effective_segment_payload_bytes: float
  active_topics_with_buffered_data: float
  range_reads_per_segment_per_consumer_group: float
  # Estimate reservations from ingest when zero. Maintenance/refill can coalesce into one update.
  producer_foreground_reservation_writes_per_hour: float

  # ---------- Direct observed-rate overrides ----------
  s_seg_per_hour: float
  s_range_read_per_hour: float
  consumer_partition_claims_per_hour: float
  consumer_cursor_commits_per_hour: float
  assignment_plan_publications_per_hour: float


def estimate_segments_per_hour(i: Inputs) -> float:
  """Estimate flushed segments/hour if not directly provided.

  S_size_trigger ~= ingest bytes/hour / effective segment payload bytes
  S_time_trigger ~= active buffered topics * (3600 / flush_max_delay_s)
  S_seg ~= max(S_size_trigger, S_time_trigger)

  Time-due broker flushes coalesce partitions for a topic. Byte threshold and lease-drain work can
  still be partition-local, so use `s_seg_per_hour` from metrics for decisions.
  """
  s_size_trigger = (i.r_ingest_per_hour * i.b_record_bytes) / max(
    i.effective_segment_payload_bytes,
    1.0,
  )
  s_time_trigger = i.active_topics_with_buffered_data * (
    3600.0 / max(i.flush_max_delay_s, 1e-9)
  )
  return max(s_size_trigger, s_time_trigger)


def strong_read_units(kb: float) -> float:
  """DynamoDB strong-read units for a read or evaluated query page."""
  return float(ceil(max(kb, 0.0) / 4.0))


def write_units(kb: float) -> float:
  """DynamoDB write units for an item."""
  return float(ceil(max(kb, 0.0)))


def estimate_range_reads_per_hour(i: Inputs, s_seg_per_hour: float) -> float:
  """Fallback S3 range GET estimate; selected ranges are consolidated per segment."""
  return (
    s_seg_per_hour
    * i.g_consumer_groups
    * max(i.range_reads_per_segment_per_consumer_group, 0.0)
  )


def compute(i: Inputs) -> dict[str, float]:
  """Compute hourly request units and request charges."""
  s_seg = i.s_seg_per_hour if i.s_seg_per_hour > 0 else estimate_segments_per_hour(i)
  s_range_read = (
    i.s_range_read_per_hour
    if i.s_range_read_per_hour > 0
    else estimate_range_reads_per_hour(i, s_seg)
  )

  # ---------- S3 request counts ----------
  # Broker: one PutObject per flushed segment.
  req_s3_put = s_seg

  # Consumer: one range GetObject per selected segment.
  req_s3_get = s_range_read

  # ---------- DynamoDB write request counts ----------
  # Producer maintenance runs every lease duration / 3 seconds.
  req_ddb_w_prod_lease = i.p_owned_prod * (
    3600.0 / max(i.lease_duration_prod_s / 3.0, 1.0)
  )
  estimated_reservations = i.r_ingest_per_hour / max(i.reservation_size, 1.0)
  req_ddb_w_prod_reserve = (
    i.producer_foreground_reservation_writes_per_hour
    if i.producer_foreground_reservation_writes_per_hour > 0
    else max(estimated_reservations - req_ddb_w_prod_lease, 0.0)
  )

  # Stable rebalances retain leases; claims and cursor-only commits are observed rates.
  req_ddb_w_cons_lease_hb = i.g_consumer_groups * i.p_owned_cons_total * (
    3600.0 / max(i.heartbeat_cons_s, 1e-9)
  )
  req_ddb_w_cons_claim = i.consumer_partition_claims_per_hour
  req_ddb_w_cons_commit = i.consumer_cursor_commits_per_hour
  req_ddb_w_membership_hb = i.g_consumer_groups * i.n_cons * (
    3600.0 / max(i.heartbeat_cons_s, 1e-9)
  )
  req_ddb_w_planner = i.g_consumer_groups * (
    3600.0 / max(i.rebalance_interval_s, 1e-9)
  )

  # ---------- DynamoDB read request counts ----------
  # Fast scans are bounded recent-window work. Fresh/Recovering scans only occur after assignment
  # changes or restart, and are supplied separately rather than charged universally.
  req_ddb_r_scan_fast = (
    i.g_consumer_groups * i.n_cons * i.polls_per_s * 3600.0
    * i.fast_window_queries_per_poll * i.pages_per_metadata_query
  )
  req_ddb_r_scan_recovery = i.recovery_window_queries_per_hour * i.pages_per_metadata_query
  req_ddb_r_scan = req_ddb_r_scan_fast + req_ddb_r_scan_recovery
  req_ddb_r_coordination = (
    i.g_consumer_groups * i.n_cons
    * (3600.0 / max(i.rebalance_interval_s, 1e-9))
    * i.coordination_strong_reads_per_rebalance
  )

  # ---------- Convert requests to DynamoDB RU ----------
  segment_wru = s_seg * write_units(i.kb_seg_item)
  transactional_fence_rru = 0.0
  if i.transactional_metadata_writes:
    # Transactional Put and each transactional ConditionCheck charge prepare and commit.
    segment_wru *= 2.0
    transactional_fence_rru = (
      s_seg * max(i.avg_partitions_per_segment, 0.0) * 2.0 * strong_read_units(i.kb_lease_prod)
    )
  assignment_plan_wru = (
    i.assignment_plan_publications_per_hour * 2.0 * write_units(i.kb_assignment_plan)
  )
  assignment_plan_rru = (
    i.assignment_plan_publications_per_hour * 2.0 * strong_read_units(i.kb_membership)
  )
  wru_hour = (
    segment_wru
    + req_ddb_w_prod_lease * write_units(i.kb_lease_prod)
    + req_ddb_w_prod_reserve * write_units(i.kb_lease_prod)
    + req_ddb_w_cons_lease_hb * write_units(i.kb_lease_cons)
    + req_ddb_w_cons_claim * write_units(i.kb_lease_cons)
    + req_ddb_w_cons_commit * write_units(i.kb_lease_cons)
    + req_ddb_w_membership_hb * write_units(i.kb_membership)
    + req_ddb_w_planner * write_units(i.kb_membership)
    + assignment_plan_wru
  )
  rru_hour = (
    req_ddb_r_scan * strong_read_units(i.kb_scanned_per_metadata_query_page)
    * (0.5 if i.metadata_eventual_reads else 1.0)
    + req_ddb_r_coordination * strong_read_units(i.kb_coordination_read)
    + transactional_fence_rru
    + assignment_plan_rru
  )
  write_overhead = 1.0 + max(i.f_write_conflict_overhead, 0.0)
  read_overhead = 1.0 + max(i.f_read_conflict_overhead, 0.0)
  wru_hour *= write_overhead
  rru_hour *= read_overhead

  # ---------- Hourly cost ----------
  cost_ddb_hour = (wru_hour / 1_000_000.0) * i.p_ddb_w_million + (
    rru_hour / 1_000_000.0
  ) * i.p_ddb_r_million

  cost_s3_hour = (req_s3_put / 1000.0) * i.p_s3_put_1k + (req_s3_get / 1000.0) * i.p_s3_get_1k

  cost_total_hour = cost_ddb_hour + cost_s3_hour

  return {
    "s_seg_per_hour": s_seg,
    "s_range_read_per_hour": s_range_read,
    "req_s3_put": req_s3_put,
    "req_s3_get": req_s3_get,
    "req_ddb_w_prod_lease": req_ddb_w_prod_lease,
    "req_ddb_w_prod_reserve": req_ddb_w_prod_reserve,
    "req_ddb_w_cons_lease_hb": req_ddb_w_cons_lease_hb,
    "req_ddb_w_membership_hb": req_ddb_w_membership_hb,
    "req_ddb_r_scan_fast": req_ddb_r_scan_fast,
    "req_ddb_r_scan_recovery": req_ddb_r_scan_recovery,
    "req_ddb_r_coordination": req_ddb_r_coordination,
    "segment_wru_hour": segment_wru * write_overhead,
    "transactional_fence_rru_hour": transactional_fence_rru * read_overhead,
    "wru_hour": wru_hour,
    "rru_hour": rru_hour,
    "cost_ddb_hour": cost_ddb_hour,
    "cost_s3_hour": cost_s3_hour,
    "cost_total_hour": cost_total_hour,
  }


def print_report(i: Inputs, result: dict[str, float]) -> None:
  """Pretty-print an hourly cost breakdown."""
  print("blob-stream hourly request-cost estimate (DynamoDB + S3)")
  print("=" * 68)
  print(f"Metadata read consistency:              {'eventual' if i.metadata_eventual_reads else 'strong'}")
  print(f"Metadata write mode:                    {'transactional fence' if i.transactional_metadata_writes else 'plain PutItem'}")
  print(f"Segments/hour (publication rate):       {result['s_seg_per_hour']:,.2f}")
  print(f"S3 range GETs/hour:                     {result['s_range_read_per_hour']:,.2f}")
  print(f"Producer lease writes/hour:              {result['req_ddb_w_prod_lease']:,.2f}")
  print(f"Foreground reservation writes/hour:      {result['req_ddb_w_prod_reserve']:,.2f}")
  print(f"Consumer lease heartbeat writes/hour:    {result['req_ddb_w_cons_lease_hb']:,.2f}")
  print(f"Membership heartbeat writes/hour:        {result['req_ddb_w_membership_hb']:,.2f}")
  print(f"Dynamo Fast metadata pages/hour:         {result['req_ddb_r_scan_fast']:,.2f}")
  print(f"Dynamo recovery metadata pages/hour:     {result['req_ddb_r_scan_recovery']:,.2f}")
  print(f"Dynamo coordination reads/hour:          {result['req_ddb_r_coordination']:,.2f}")
  print(f"Metadata publication WRU/hour:           {result['segment_wru_hour']:,.2f}")
  print(f"Transactional fence RRU/hour:            {result['transactional_fence_rru_hour']:,.2f}")
  print(f"Dynamo WRU/hour:                         {result['wru_hour']:,.2f}")
  print(f"Dynamo RRU/hour:                         {result['rru_hour']:,.2f}")
  print("-" * 68)
  print(f"DynamoDB cost/hour:                      ${result['cost_ddb_hour']:,.4f}")
  print(f"S3 request cost/hour:                    ${result['cost_s3_hour']:,.4f}")
  print(f"TOTAL cost/hour:                         ${result['cost_total_hour']:,.4f}")


DEFAULTS = Inputs(
  p_ddb_w_million=1.25,
  p_ddb_r_million=0.25,
  p_s3_put_1k=0.005,
  p_s3_get_1k=0.0004,

  r_ingest_per_hour=10_000_000,
  b_record_bytes=350.0,
  g_consumer_groups=1.0,
  p_owned_prod=128.0,
  p_owned_cons_total=128.0,
  n_cons=8.0,

  flush_max_delay_s=1.0,
  lease_duration_prod_s=30.0,
  reservation_size=10_000.0,
  heartbeat_cons_s=10.0,
  rebalance_interval_s=10.0,
  polls_per_s=2.0,
  fast_window_queries_per_poll=1.0,
  recovery_window_queries_per_hour=0.0,
  metadata_eventual_reads=False,
  transactional_metadata_writes=False,
  avg_partitions_per_segment=8.0,
  pages_per_metadata_query=1.2,
  coordination_strong_reads_per_rebalance=2.0,
  kb_seg_item=0.475,
  kb_lease_prod=1.0,
  kb_lease_cons=1.0,
  kb_membership=1.0,
  kb_assignment_plan=8.0,
  kb_scanned_per_metadata_query_page=8.0,
  kb_coordination_read=4.0,
  f_write_conflict_overhead=0.0,
  f_read_conflict_overhead=0.0,

  effective_segment_payload_bytes=32.0 * 1024.0 * 1024.0,
  active_topics_with_buffered_data=16.0,
  range_reads_per_segment_per_consumer_group=1.0,
  producer_foreground_reservation_writes_per_hour=0.0,
  s_seg_per_hour=0.0,
  s_range_read_per_hour=0.0,
  consumer_partition_claims_per_hour=0.0,
  consumer_cursor_commits_per_hour=0.0,
  assignment_plan_publications_per_hour=0.0,
)


def parse_override(specification: str) -> tuple[str, float | bool]:
  """Parse a NAME=VALUE override using the matching default's primitive type."""
  name, separator, raw_value = specification.partition("=")
  if not separator or not name or not raw_value:
    raise ValueError(f"invalid --set value {specification!r}; use NAME=VALUE")
  if name not in {field.name for field in fields(Inputs)}:
    raise ValueError(f"unknown input {name!r}")
  default_value = getattr(DEFAULTS, name)
  if isinstance(default_value, bool):
    if raw_value.lower() not in {"true", "false"}:
      raise ValueError(f"{name} must be true or false")
    return name, raw_value.lower() == "true"
  return name, float(raw_value)


def parse_inputs() -> Inputs:
  """Apply CLI consistency switches and explicit overrides to DEFAULTS."""
  parser = ArgumentParser(description=__doc__)
  parser.add_argument("--eventual-metadata-reads", action="store_true")
  parser.add_argument("--transactional-metadata-writes", action="store_true")
  parser.add_argument("--set", action="append", default=[], metavar="NAME=VALUE")
  arguments = parser.parse_args()
  overrides = dict(parse_override(specification) for specification in arguments.set)
  if arguments.eventual_metadata_reads:
    overrides["metadata_eventual_reads"] = True
  if arguments.transactional_metadata_writes:
    overrides["transactional_metadata_writes"] = True
  return replace(DEFAULTS, **overrides)


def main() -> None:
  inputs = parse_inputs()
  print_report(inputs, compute(inputs))


if __name__ == "__main__":
  main()
