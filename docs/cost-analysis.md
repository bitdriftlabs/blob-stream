# Cost Analysis

`cost_analysis.py` estimates hourly DynamoDB request-unit charges and S3 request charges for one
`blob-stream` topic. It models the current implementation: topic-coalesced time flushes, Fast's
bounded metadata horizon, assignment-driven Fresh/Recovering scans, and one S3 range request per
selected segment.

It does not estimate DynamoDB or S3 storage, S3 data transfer, broker/consumer compute, or network
egress. Use provider billing data for those costs.

## Quick Start

From this `docs` directory, run the baseline model:

```bash
python3 ../cost-analysis/cost_analysis.py
```

The defaults are illustrative only. They contain placeholder regional prices and fallback estimates,
not a production workload. Override individual values without editing the script:

```bash
python3 ../cost-analysis/cost_analysis.py \
	--set r_ingest_per_hour=50000000 \
	--set s_seg_per_hour=24000 \
	--set s_range_read_per_hour=36000 \
	--set p_owned_prod=96 \
	--set p_owned_cons_total=96 \
	--set n_cons=12
```

The current data path uses eventual segment-metadata queries and plain `PutItem` publication. Price
the two proposed stronger modes separately:

```bash
# Strongly consistent segment-metadata queries. Consumer coordination reads are already strong.
python3 ../cost-analysis/cost_analysis.py --strong-metadata-reads

# Transactionally fence metadata publication against the producing lease rows.
python3 ../cost-analysis/cost_analysis.py \
	--transactional-metadata-writes \
	--set avg_partitions_per_segment=6

# Price both stronger modes for the same workload.
python3 ../cost-analysis/cost_analysis.py \
	--strong-metadata-reads \
	--transactional-metadata-writes \
	--set avg_partitions_per_segment=6
```

Every `Inputs` field can also be set with `--set NAME=VALUE`. Boolean values are `true` or `false`.

```bash
python3 ../cost-analysis/cost_analysis.py \
	--set metadata_strong_reads=true \
	--set transactional_metadata_writes=true \
	--set pages_per_metadata_query=1.4
```

`--strong-metadata-reads` and `metadata_strong_reads` model the resolved
`ConsumerReadConfig.strongly_consistent_metadata_reads` setting, including its
`blob_stream_consumer_strong_metadata_reads` runtime override. `--transactional-metadata-writes`
and `transactional_metadata_writes` model broker `fenced_metadata_writes`, including its
`blob_stream_broker_fenced_metadata_writes` runtime override. The fence mode is optional, but its
holder ID, lease epoch, and session ID remain required in every producer lease row.

The model's metadata-read cost is unaffected by the fact that strong reads have no visibility delay
or by the configured consumer clock-skew horizon. The clock-skew bound can increase the scanned time
range near a window boundary; size the model with observed page sizes and poll rates for deployments
where that additional overlap is material.

## Calibration

Use direct observed rates whenever available. The fallback segment and range-read estimates are for
early sizing only: time-due broker flushes coalesce partitions by topic, and, when cross-topic blobs
are enabled, by locally buffered topic; several consumer instances can select ranges from the same
segment.

| Input | Meaning | Preferred source |
| --- | --- | --- |
| `s_seg_per_hour` | Segment metadata publications and S3 object writes per hour | `write:flush_uploaded_objects_total` delta |
| `s_range_read_per_hour` | S3 range requests per hour | `reader:blob_range_requests` delta |
| `recovery_window_queries_per_hour` | Fresh/Recovering metadata window queries before pagination | `reader:metadata_recovery_scan_requests` delta |
| `consumer_partition_claims_per_hour` | Lease claims caused by actual assignment changes | `consumer:lease_claims_*` deltas |
| `consumer_cursor_commits_per_hour` | Explicit cursor-only commits outside scheduled heartbeats | `consumer:cursor_commit_partitions` delta |
| `assignment_plan_publications_per_hour` | Assignment-plan publication rate | DynamoDB write capacity or a planner-side publication counter |

In steady state, `recovery_window_queries_per_hour=0` is normal. Recovery is not a periodic Fast
rescan; it begins with assignment/restart work and ends once the partition catches up. Fast scans
only query their bounded recent windows on each read pass.

Collect a steady interval and a peak/recovery interval using the same time range for request counts,
DynamoDB capacity, and regional prices. Compare the model with:

- `reader:metadata_fast_scan_requests` and `reader:metadata_recovery_scan_requests`
- `reader:blob_range_requests` and `reader:blob_range_bytes`
- `write:flush_uploaded_objects_total` and `write:flush_uploaded_object_bytes_total`
- `dynamo:read_request_units_total` and `dynamo:write_request_units_total`
- CloudWatch/billing data for S3 request counts, storage, and transfer

Set `pages_per_metadata_query` and `kb_scanned_per_metadata_query_page` from table-capacity data.
Update the `p_*` price inputs for the deployed AWS region and DynamoDB mode before using currency
values for a decision.

## Consistency Costs

With eventual metadata reads, each 4 KiB metadata-query page costs $0.5$ RRU. The
`--strong-metadata-reads` switch changes only those metadata-query pages to 1 RRU. Consumer-group
coordination is already strongly consistent, so it is unaffected by the switch.

`--transactional-metadata-writes` models enabled broker fenced metadata publication: a transactional
metadata `Put` plus one transactional producer-lease `ConditionCheck` per contributing virtual
partition. For metadata size $M$ KiB and $P$ average partitions per segment, capacity per published
segment is:

$$
2\left\lceil\frac{M}{1\ \mathrm{KiB}}\right\rceil\ \mathrm{WRU}
+
2P\left\lceil\frac{\text{producer lease size}}{4\ \mathrm{KiB}}\right\rceil\ \mathrm{RRU}
$$

DynamoDB charges prepare and commit capacity for every transaction item, including a transaction
canceled by a failed condition. The model reports these checks as `Transactional fence RRU/hour`.
