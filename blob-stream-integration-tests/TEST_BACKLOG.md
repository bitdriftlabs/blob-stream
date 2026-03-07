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
| IT-013 | todo | bootstrap_dynamic_membership_scale_out_rebalances | Validate production bootstrap discovers new consumers from `consumer_group_membership` and rebalances | Start member A with `ConsumerConfigFactory::build_iterator_from_proto_config`; start member B with same group; at least one revocation observed and both members make forward progress while consumed set matches produced set | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test bootstrap_dynamic_membership_scale_out_rebalances` |  |
| IT-014 | todo | bootstrap_dynamic_membership_scale_in_after_expiry | Validate membership-store-driven scale-in when a member stops heartbeating | Start members A+B via production bootstrap; stop B without graceful shutdown; after membership TTL expiry A regains partitions and drains remaining records with no loss | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test end_to_end_test bootstrap_dynamic_membership_scale_in_after_expiry` |  |
| IT-015 | todo | bootstrap_rebalance_with_membership_and_lease_faults | Validate rebalance convergence when membership and lease operations intermittently fail under fault injection | Use in-memory transport + store fault controller to inject membership heartbeat failures and consumer lease heartbeat/assign failures; membership/rebalance converges and final consumed set equals produced set; event log shows fault application and eventual successful ownership transitions | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test bootstrap_rebalance_with_membership_and_lease_faults` |  |

## Notes
- Keep scope minimal per item; if extra work appears, add a new row instead of expanding current acceptance criteria.
- If a test reveals a product bug, link fix PR/commit in `Evidence`.

## Milestone 13 Item 4: Fault Injection Framework Plan

### Framework Design Changes
1. Add a deterministic integration runtime wrapper in test harness:
   - `DeterministicTestRuntime` owns virtual clock, seeded RNG, and step-driven task progression.
   - Replace direct `Instant::now()/sleep` usage in test harness loops with runtime clock APIs.
   - Provide `advance(ms)` and `run_until(predicate, timeout_ms)` helpers so tests never rely on wall-clock races.
2. Replace real socket binding (`127.0.0.1:0`) with injectable test transport:
   - Introduce `BrokerTransport` trait with implementations:
     - `GrpcTcpTransport` (existing behavior for compatibility).
     - `InMemoryTestTransport` (deterministic request/response channels).
   - Add transport hooks in `ClusterHarnessBuilder` so each broker/client path uses the selected transport.
   - Add per-link fault controls for drop, delay, duplication, reordering, partition, and recovery.
3. Add deterministic fault wrappers for persistence paths:
   - Wrap `BlobStore`, `MetadataStore`, `ProducerPartitionLeaseStore`, and `ConsumerGroupLeaseStore` in fault-aware decorators.
   - Support operation-level faults by method name and key pattern:
     - fail once / fail N times / fail until cleared
     - deterministic latency injection
     - timeout simulation
     - stale read / delayed visibility (for scans)
   - Ensure wrappers emit structured events for assertions.
4. Add scenario scripting primitives to the framework:
   - `FaultScript` with ordered phases and trigger conditions (`on_call_count`, `on_partition`, `on_time`).
   - `FaultController` API exposed to tests:
     - `enable_fault(...)`, `disable_fault(...)`, `clear_all_faults()`
     - `wait_for_event(...)` with deterministic timeout.
5. Add deterministic observability for assertions:
   - Central `TestEventLog` capturing transport operations, store operations, retries, lease ownership changes, and cursor commits.
   - Assert against event sequences rather than timing-sensitive side effects.
6. Keep current non-fault integration mode available:
   - Default builder still supports current behavior for smoke/regression tests.
   - Fault mode is opt-in through `ClusterHarness::builder(...).fault_injection(...)`.

### Implementation Phases
1. Phase A: Runtime + transport abstraction skeleton, no faults enabled yet. ✅ done (2026-03-07)
   - Added `TestRuntime` abstraction (`TokioTestRuntime`, `DeterministicTestRuntime` skeleton).
   - Added `BrokerTransport` abstraction (`GrpcTcpTransport` default) and wired `ClusterHarness` startup/restart through transport endpoint binding.
   - Routed harness timing helpers through runtime APIs without changing default behavior.
2. Phase B: In-memory transport path and scriptable network faults. ✅ done (2026-03-07)
    - Added `InMemoryTestTransport` with `inmemory://` endpoint binding and direct write-engine routing.
    - Added scriptable network fault primitives (`NetworkFaultController`, `NetworkFaultRule`, `FaultScriptStep`) covering drop/delay/duplicate/reorder/timeout/partition.
    - Wired in-memory producer path via `ClusterHarness::create_producer(...)` and transport-provided producer transport injection.
    - Gate evidence:
       - `cargo +nightly fmt` -> pass
       - `cargo clippy --workspace --bins --examples --tests -- -D warnings --no-deps` -> pass
       - `RUST_LOG=off cargo nextest run -p blob-stream-integration-tests` -> pass (13/13)
3. Phase C: Store wrappers and scriptable S3/Dynamo fault injection. ✅ done (2026-03-07)
    - Added `store_faults.rs` fault-aware wrappers for `BlobStore`, `MetadataStore`,
       `ProducerPartitionLeaseStore`, and `ConsumerGroupLeaseStore`.
    - Added `StoreFaultController` + rules/scripts supporting fail, delay, timeout, stale reads,
       delayed visibility, and operation/key-pattern scoping.
    - Wired `IntegrationResources` to return fault-wrapped store implementations while preserving
       default no-fault behavior when no rules are configured.
    - Added structured `StoreFaultEvent` capture APIs on the controller for later assertion usage.
    - Gate evidence:
       - `cargo +nightly fmt` -> pass
       - `cargo clippy -p blob-stream-integration-tests --tests -- -D warnings --no-deps` -> pass
       - `RUST_LOG=off cargo nextest run -p blob-stream-integration-tests` -> pass (13/13)
4. Phase D: Deterministic event log + helper assertions. ✅ done (2026-03-07)
      - Added centralized `event_log.rs` with `TestEventLog`, `TestEventMatcher`,
         `wait_for_event(...)`, and `assert_event_sequence_contains(...)` helpers.
      - Wired shared event log into transport and store fault controllers via harness startup.
      - Added transport/store event emission for operation calls and outcomes, including lease
         ownership-related outcomes and cursor commit outcomes for deterministic assertions.
      - Exposed harness-level helpers (`ClusterHarness::wait_for_event`,
         `ClusterHarness::assert_event_sequence_contains`) for fault tests.
      - Gate evidence:
          - `cargo +nightly fmt` -> pass
          - `cargo clippy -p blob-stream-integration-tests --tests -- -D warnings --no-deps` -> pass
          - `RUST_LOG=off cargo nextest run -p blob-stream-integration-tests` -> pass (13/13, 2 leaky)
5. Phase E: Port and stabilize all fault-injection tests below. ✅ done (2026-03-07)
   - Implemented and validated FIT-001 through FIT-012 in `fault_injection_test.rs`.
   - Each FIT test was run in trace mode first and then with `RUST_LOG=off`.
   - See per-test `Evidence` in the FIT table below for command-level results.
      - Gate evidence:
          - `cargo +nightly fmt` -> pass
          - `cargo clippy -p blob-stream-integration-tests --tests -- -D warnings --no-deps` -> pass
          - `RUST_LOG=off cargo nextest run -p blob-stream-integration-tests` -> pass (25/25)
6. Phase F: Cleanup any allow dead code annotations in the framework. ✅ done (2026-03-07)
      - Removed legacy item-level `#[allow(dead_code)]` annotations across framework support modules.
      - Consolidated dead-code suppression to a single documented module-level allowance in
         `tests/support/framework.rs` to handle per-binary API subsets.
      - Gate evidence:
          - `cargo +nightly fmt` -> pass
          - `cargo clippy -p blob-stream-integration-tests --tests -- -D warnings --no-deps` -> pass
          - `RUST_LOG=off cargo nextest run -p blob-stream-integration-tests` -> pass (25/25)

### Per-Phase Exit Gate (Required)
After every phase, run all of the following before marking the phase complete:
1. Format per rust instructions
2. Clippy per rust instructions
3. Existing integration regression suite:
   - `RUST_LOG=off cargo nextest run -p blob-stream-integration-tests`
4. Record pass/fail evidence in this file alongside the completed phase.

## Fault Injection Test Backlog (Deterministic)

These tests should run under the deterministic runtime and test transport only.

| ID | Status | Test Name | Fault Domain | Goal | Acceptance Criteria | Command | Evidence |
|---|---|---|---|---|---|---|---|
| FIT-001 | done | network_drop_produce_retry_no_loss | Transport | Drop first producer->broker request per partition and verify retry path | All produced IDs are eventually consumed; no missing IDs; retries observed in event log | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test network_drop_produce_retry_no_loss` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test network_drop_produce_retry_no_loss`, 0.87s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test network_drop_produce_retry_no_loss`, 0.81s) |
| FIT-002 | done | network_delay_and_reorder_preserves_cursor_monotonicity | Transport | Inject deterministic delay + reorder of produce RPCs | Consumption completes without loss; per-partition cursor monotonicity preserved | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test network_delay_and_reorder_preserves_cursor_monotonicity` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test network_delay_and_reorder_preserves_cursor_monotonicity`, 4.10s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test network_delay_and_reorder_preserves_cursor_monotonicity`, 4.12s) |
| FIT-003 | done | network_partition_active_broker_takeover | Transport | Partition producer from active broker while standby remains reachable | Produce/consume progress resumes after deterministic reroute; final set matches expected | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test network_partition_active_broker_takeover` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test network_partition_active_broker_takeover`, 1.09s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test network_partition_active_broker_takeover`, 1.14s) |
| FIT-004 | done | broker_response_timeout_retry_budget_respected | Transport | Force timeout responses for bounded attempts | Producer retries up to configured budget; success/failure behavior is deterministic and asserted | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test broker_response_timeout_retry_budget_respected` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test broker_response_timeout_retry_budget_respected`, 1.01s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test broker_response_timeout_retry_budget_respected`, 0.97s) |
| FIT-005 | done | s3_put_transient_failures_recover_without_loss | BlobStore (S3 write path) | Fail blob `put` N times before success | Segment metadata eventually committed and all records consumed exactly once from reader perspective | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test s3_put_transient_failures_recover_without_loss` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test s3_put_transient_failures_recover_without_loss`, 0.87s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test s3_put_transient_failures_recover_without_loss`, 0.89s) |
| FIT-006 | done | s3_get_failures_consumer_rescan_recovers | BlobStore (S3 read path) | Inject transient blob `get` failures during consume | Reader eventually catches up via retry/re-scan; no missing IDs | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test s3_get_failures_consumer_rescan_recovers` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test s3_get_failures_consumer_rescan_recovers`, 1.24s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test s3_get_failures_consumer_rescan_recovers`, 1.35s) |
| FIT-007 | done | metadata_write_fail_then_retry_ack_semantics | MetadataStore (Dynamo write path) | Fail metadata `write_segment` before succeeding | Producer ack must only occur after successful metadata write; no phantom acks | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test metadata_write_fail_then_retry_ack_semantics` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test metadata_write_fail_then_retry_ack_semantics`, 0.40s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test metadata_write_fail_then_retry_ack_semantics`, 0.36s) |
| FIT-008 | done | metadata_scan_stale_visibility_no_duplicate_progress | MetadataStore (Dynamo scan path) | Return stale/partial scan windows for deterministic interval | Consumer eventually sees all data; duplicate scans do not regress cursor | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test metadata_scan_stale_visibility_no_duplicate_progress` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test metadata_scan_stale_visibility_no_duplicate_progress`, 1.21s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test metadata_scan_stale_visibility_no_duplicate_progress`, 1.28s) |
| FIT-009 | done | producer_lease_store_conflict_then_expiry_takeover | Lease store (producer) | Simulate lease conflicts and expiry-based takeover | Old holder fenced; new holder progresses; no split-write accepted | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test producer_lease_store_conflict_then_expiry_takeover` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test producer_lease_store_conflict_then_expiry_takeover`, 1.14s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test producer_lease_store_conflict_then_expiry_takeover`, 1.15s) |
| FIT-010 | done | consumer_lease_store_heartbeat_failover | Lease store (consumer) | Drop heartbeat commits for active owner until lease expires | Another member takes ownership and full progress is preserved | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test consumer_lease_store_heartbeat_failover` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test consumer_lease_store_heartbeat_failover`, 0.34s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test consumer_lease_store_heartbeat_failover`, 0.28s) |
| FIT-011 | done | combined_network_and_metadata_faults_end_to_end | Cross-domain | Combine transport drops with metadata write delays | System converges deterministically; final consumed set equals produced set | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test combined_network_and_metadata_faults_end_to_end` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test combined_network_and_metadata_faults_end_to_end`, 2.21s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test combined_network_and_metadata_faults_end_to_end`, 2.20s) |
| FIT-012 | done | deterministic_replay_same_seed_same_event_trace | Determinism guarantee | Re-run same scripted scenario with same seed | Event trace and terminal assertions are bit-for-bit equivalent across runs | `RUST_LOG=off cargo test -p blob-stream-integration-tests --test fault_injection_test deterministic_replay_same_seed_same_event_trace` | 2026-03-07: pass (`RUST_LOG=blob_stream=trace,bd=trace cargo nextest run -p blob-stream-integration-tests --test fault_injection_test deterministic_replay_same_seed_same_event_trace`, 1.64s); pass (`RUST_LOG=off cargo nextest run -p blob-stream-integration-tests --test fault_injection_test deterministic_replay_same_seed_same_event_trace`, 1.60s) |

## Fault Injection Notes
- All FIT-* tests must use in-memory test transport and fault-wrapped stores; no ephemeral port binding.
- All FIT-* tests must use deterministic runtime controls (virtual time + scripted triggers).
- Use `RUST_LOG=blob_stream=trace,bd=trace` first during development, then finalize each with `RUST_LOG=off` evidence.
