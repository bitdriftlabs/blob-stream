# Integration Test Backlog

This file tracks end-to-end integration test work items so we can add and validate tests one at a time.

## Status Legend
- `todo` — scoped but not started
- `in_progress` — actively being implemented
- `done` — merged and passing with command output captured
- `blocked` — cannot proceed until dependency/decision is resolved

## Workflow (one test at a time)
1. Move exactly one item to `in_progress`.
2. Implement only that test (and minimal supporting code).
2. First run the test with extra logging to aid in debugging failures with
   `RUST_LOG=blob_stream=trace,bd=trace`. Do not let tests hang indefinitely so cap the timeout
   failure to 60s and then look at logs to see if it's hung. Iterate.
3. Finally run with `RUST_LOG=off`.
4. On success, mark `done` and fill in `Evidence` (date + command + result).
5. Start next item.

## Backlog

| ID | Status | Test Name | Goal | Acceptance Criteria | Command | Evidence |
|---|---|---|---|---|---|---|
| IT-000 | done | single_broker_single_record_end_to_end | Validate basic produce/read path | 1 record produced and consumed; duplicate scan empty | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test single_broker_single_record_end_to_end` | 2026-03-01: pass |
| IT-001 | done | single_broker_cursor_monotonicity_and_dedup | Validate cursor progression + dedupe | Record consumed once; repeated scans empty; cursor map stable | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test single_broker_cursor_monotonicity_and_dedup` | 2026-03-01: pass |
| IT-002 | done | autoscaling_rebalance_and_failover_preserves_progress | Validate rebalance + broker failover progress | Revocation observed; phase1+phase2 IDs fully consumed | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test autoscaling_rebalance_and_failover_preserves_progress` | 2026-03-01: pass |
| IT-003 | done | consumer_restart_resume_from_committed_offsets | Ensure resumed consumer starts from committed cursor | Commit offsets, recreate consumer, no gaps/duplicates beyond at-least-once expectation | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test consumer_restart_resume_from_committed_offsets` | 2026-03-01: pass |
| IT-004 | done | group_rebalance_continuous_traffic_no_loss | Verify 2→3→1 consumer membership changes under ongoing produce | All produced IDs eventually consumed; progress continues through each rebalance | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test group_rebalance_continuous_traffic_no_loss` | 2026-03-07: pass (31.63s) |
| IT-005 | done | active_broker_restart_continuity | Validate system behavior when active broker restarts | Producing and consuming continue; no deadlock; final consumed set matches expected | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test active_broker_restart_continuity` | 2026-03-07: pass (2.13s) |
| IT-006 | done | per_partition_sequence_monotonicity | Verify monotonic seq ranges per virtual partition | For each partition, observed seq_end is strictly increasing over produced batches | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test per_partition_sequence_monotonicity` | 2026-03-07: pass (2.24s) |
| IT-007 | done | multi_topic_isolation | Validate topic isolation in produce/read and leases | No cross-topic reads; each topic consumes exactly its own produced IDs | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test multi_topic_isolation` | 2026-03-07: pass (1.63s) |
| IT-008 | done | payload_boundary_and_batching_behavior | Cover payload size boundaries and batch flush behavior | Empty/small/near-limit payloads accepted; consumption matches production exactly | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test payload_boundary_and_batching_behavior` | 2026-03-07: pass (0.50s) |
| IT-009 | done | delayed_metadata_cross_window_no_loss | Validate look-back catches metadata that arrives after initial window scan | Records produced before delay are eventually consumed exactly once when metadata lands in a later window | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test delayed_metadata_cross_window_no_loss` | `RUST_LOG=blob_stream=trace,bd=trace cargo test -p blob-stream-integration-tests --test end_to_end_test delayed_metadata_cross_window_no_loss`; `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test delayed_metadata_cross_window_no_loss`; `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test` |
| IT-010 | done | consumer_generation_fencing_rejects_stale_commit | Verify stale consumer owner cannot advance cursor after rebalance generation change | Stale owner commit/heartbeat is rejected; new owner cursor remains authoritative and monotonic | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test consumer_generation_fencing_rejects_stale_commit` | `RUST_LOG=blob_stream=trace,bd=trace cargo test -p blob-stream-integration-tests --test end_to_end_test consumer_generation_fencing_rejects_stale_commit`; `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test consumer_generation_fencing_rejects_stale_commit`; `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test` |
| IT-011 | done | multi_writer_virtual_partition_merge_correctness | Validate multi-writer topic read behavior for logical partition fan-in | Data from multiple writer_id virtual partitions is consumed without loss and cursor progression remains monotonic | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test multi_writer_virtual_partition_merge_correctness` | `RUST_LOG=blob_stream=trace,bd=trace cargo test -p blob-stream-integration-tests --test end_to_end_test multi_writer_virtual_partition_merge_correctness`; `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test multi_writer_virtual_partition_merge_correctness`; `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test` |
| IT-012 | done | lease_expiry_takeover_preserves_progress | Validate lease-expiry-based ownership transfer without explicit membership update | When owner heartbeat stops and lease expires, takeover proceeds and final consumed set matches produced set | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test lease_expiry_takeover_preserves_progress` | `RUST_LOG=blob_stream=trace,bd=trace cargo test -p blob-stream-integration-tests --test end_to_end_test lease_expiry_takeover_preserves_progress`; `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test lease_expiry_takeover_preserves_progress`; `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test` |

## Notes
- Keep scope minimal per item; if extra work appears, add a new row instead of expanding current acceptance criteria.
- If a test reveals a product bug, link fix PR/commit in `Evidence`.
