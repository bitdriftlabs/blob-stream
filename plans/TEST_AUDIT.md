# Integration Test Audit

This is the working checklist for hardening the broker and consumer lifecycle tests. It covers
the integration scenarios in `blob-stream-integration-tests/tests/end_to_end_test.rs` and
`blob-stream-integration-tests/tests/fault_injection_test.rs`.

## Rules

- Every test must check a named causal boundary, its required durable state, and its final outcome.
- Never use a wall-clock delay as a success mechanism. Use manual time, a lifecycle gate, or an
  observable test-framework event. New lifecycle gates may be added as needed in aid of
  deterministic tests.
- A consumer-group, membership, or rebalance test must establish its final outcome through live
  group consumers, never a standalone `ConsumerReaderImpl`.
- Never use a `HashSet` alone to establish duplicate policy. Record per-ID counts whenever a
  scenario has a duplicate or redelivery contract.
- A timeout may diagnose a missing named event or progress boundary; it must not be the reason a
  test succeeds.
- Do not paper over dubious product behavior. When a deterministic test exposes a likely product
  problem, stop that test effort, report the evidence and the expected contract, and request
  guidance on whether the product behavior should change.

## Deflaking Help

Use `scripts/deflake.sh` to reproduce an intermittently failing nextest filter under CPU-capped
parallel load. It runs each requested iteration in a separate `.tmp/deflake-*/run-####/` directory
and reports every failed run directory, which contains `test.log` and `exit-status`.

```sh
scripts/deflake.sh 100 -- -p blob-stream-integration-tests -E 'test(the_flaky_test)'
```

The script accepts the arguments after `--` as `cargo nextest run` arguments. Use the failed run's
`test.log` as evidence for the missing causal boundary; do not treat a higher iteration count as a
substitute for deterministic synchronization.

## Already Hardened

- [x] Production broker-metadata routing, collapse, fallback, and shadow boundaries use real TCP
  discovery and lifecycle/store gates.
  - Covered: independent consumer groups collapse compatible eventual and strong Fast Tail reads
    into one broker metadata scan; an unavailable metadata owner takes the direct-read path; and
    shadow mode issues broker reads while retaining direct metadata as the delivery authority for
    every initial-recovery window. A broker Tail request that exceeds its configured deadline also
    takes the original direct-read path while the held broker scan remains in flight. A replacement
    member recovers through the broker transport after a graceful restart and only delivers work
    published after its predecessor's committed source checkpoint; retained recovery across two
    historical windows likewise queries through the broker cache and skips that checkpoint.
    A consumer whose injected metadata membership loses its initially healthy owner also falls
    back directly for the next Tail query without disrupting the producer's live route.
    A one-byte Tail-cache budget forces an over-budget generation to evict, and the next Tail
    request reloads through the broker cache before delivering the later record.
    Real TCP reader transport verifies equal Fast frontiers retain their exact shared Tail bound,
    while mixed frontiers use the lowest bound. A zero-age eventual cache refills after an empty
    observation, defers late metadata before its publication time, and delivers it at the
    observation-aware visibility boundary.
- [x] `consumer_restart_resume_from_committed_offsets`: gates graceful shutdown so the test proves
  commit, lease release, and member deregistration ordering.
- [x] `autoscaling_rebalance_and_failover_preserves_progress`: drains its terminal result through
  live group consumers across scale-out and broker failover.
- [x] `prefetch_rebalance_delayed_metadata_no_loss`: requires an actual prefetch-buffer event and
  drains through the group after rebalance.
  - Covered: the phase-one prefetch gate is scoped to `prefetch-consumer-0`, and the next group
    rebalance is held at its lifecycle boundary before phase-two publication.
- [x] `dynamic_membership_scale_out_rebalances`: uses live group consumers over shared memory
  stores for its final no-loss result, concurrently polls both members, and records its
  at-least-once delivery trace and durable cursors.
- [x] `metadata_write_fail_then_retry_ack_semantics` and
  `combined_network_and_metadata_faults_preserve_producer_publication`: drive producer retries with a manual retry
  clock and assert the scripted fault ordering.
- [x] `consumer_partition_release_honors_faults_without_releasing_ownership` and
  `consumer_member_deregistration_honors_faults_without_removing_membership`: verify that the
  store wrappers apply release and deregistration faults rather than bypassing them.
- [x] `consumer_crash_recovery_redelivers_only_uncommitted_record`: A commits record 0, crashes
  after delivery of record 1 without a commit, and B takes over after logical expiry. The test
  proves record 0 is skipped, record 1 is redelivered once, B advances generation and commits a
  source checkpoint, and A's stale heartbeat is fenced.

## Existing Tests To Harden

### P0: Deterministic Fault Timing And Direct-Reader Oracles

- [x] Replace wall-clock fault scheduling with explicit control in the remaining retry and reorder
  tests.
  - Verified coverage: `network_drop_produce_retry_no_loss`,
    `network_partition_active_broker_takeover`, and
    `s3_put_transient_failures_recover_without_loss` wait for each scripted fault and advance a
    manual retry clock. `delayed_metadata_cross_window_no_loss` and
    `prefetch_rebalance_delayed_metadata_no_loss` use explicit metadata visibility holds.
    `broker_coalesces_same_partition_requests_into_one_consumer_batch` advances a manual broker
    flush clock after both requests enter the broker buffer.
    `network_delay_and_reorder_preserves_cursor_monotonicity` waits for both manually scheduled
    transport delays before advancing them into the explicit paired reorder rendezvous.
  - Completed: `network_delay_and_reorder_preserves_cursor_monotonicity` uses a fixed reader
    horizon and cooperative yielding.
  - Covered: `producer_lease_store_conflicts_then_broker_reroute_preserves_progress` waits for
    both producer-route and standby broker-lease convergence after discovery changes, then drives
    every pre- and post-reroute producer retry with a manual retry clock.
  - Acceptance: wait for the named fault, buffered-work, or visibility boundary before advancing
    a manual retry/scheduler clock or releasing a lifecycle gate. No fixed delay may create the
    success condition.

- [x] Replace set-only direct-reader terminal assertions with exact delivery traces where the
  test asserts no duplicate progress.
  - Covered: `drain_reader_until_with_trace` and `append_reader_delivery_traces` retain every
    direct-reader ID, partition, and offset. The affected pre-persistence retry, rescan,
    stale-metadata, reroute, combined-fault, sequence-monotonicity, and delayed-metadata tests
    require exact-once delivery. The response-loss scenario retains its explicit exactly-twice
    group-delivery policy.

### P0: Consumer Ownership And Recovery

- [x] Rebuild `lease_expiry_takeover_preserves_progress` as a real member-loss test.
  - Covered: A normally owns and commits a multi-partition phase, then crashes without graceful
    cleanup. Shared manual time expires A's membership and leases; B takes over through normal
    coordination, skips every committed ID, drains the post-crash phase once per ID, obtains newer
    generations, and fences A's stale heartbeat.
  - The dedicated crash-handoff test remains the precise delivered-but-uncommitted single-record
    oracle; this test supplies distinct multi-partition planner convergence coverage.

- [x] `lease_expiry_takeover_preserves_progress` asserts every recovered partition's durable
  cursor and source checkpoint.
  - Covered: B's observed recovery offsets are retained per partition, and every touched lease has
    B ownership, a newer generation, a cursor at or beyond the observed offset, and a source
    checkpoint. A stale heartbeat is separately fenced on one explicitly selected touched lease.

- [x] `prefetch_rebalance_revocation_fences_buffered_record` is a deterministic fencing oracle.
  - Covered: the revocation gate is scoped to A and the target partition. A completes the callback
    before the prefetch worker is released; B acquires a newer lease, and A is held before a later
    rebalance while B prefetches. An A record then fails immediately, while B's replacement
    delivery and durable source checkpoint provide the positive completion boundary.
  - Completed: A reaches a member-scoped rebalance boundary before publication, then proves
    target-partition ownership through its durable lease. The replacement-delivery loop yields
    cooperatively rather than sleeping.

- [x] Strengthen `bootstrap_dynamic_membership_scale_in_after_expiry` with a phase-scoped oracle.
  - Covered: bootstrap B first acquires concrete partitions, its per-partition post-ownership phase
    is published without being polled, B aborts without graceful cleanup, and shared manual time
    drives membership/lease expiry. A alone drains each post-crash ID once, B is absent from active
    membership and final leases, and recovered partitions retain durable source checkpoints.
  - Completed: member-scoped rebalance and assignment gates identify both the scale-out and
    post-expiry phases before the test releases dependent work.

- [x] Harden `consumer_restart_resume_from_committed_offsets` and
  `iterator_recovers_persisted_checkpoint_across_multiple_recovery_slices` with complete
  delivery and durable-cursor evidence.
  - Covered: both restart phases retain per-ID counts and per-partition maximum offsets; each
    phase is exactly once, phase one cannot replay after restart, and every touched partition has
    a cursor at or beyond its observed offset with a source checkpoint. Multi-slice recovery also
    retains exact per-ID counts and proves the recovery member durably commits every touched
    partition with a source checkpoint.

- [x] Keep `consumer_generation_fencing_rejects_stale_commit` as a store-level guard and add the
  required live-consumer commit-race oracle.
  - Covered by `live_consumer_commit_race_is_fenced_and_redelivered`: real iterator A stages a
    record, `ConsumerBeforeCommit` holds its commit, manual time expires A, and B acquires a new
    generation. A reports the target partition fenced with no renewed partitions and no durable
    cursor; B redelivers the record and commits its source checkpoint.

### P0: Broker Restart While Consumers Are Live

- [x] `active_broker_restart_continuity` uses two live group consumers throughout the restart.
  - Covered: traffic is produced before and after restart, the active broker's drain start is
    gated, accepted in-flight metadata persists while the drain remains held, every ID is
    delivered exactly once by the group with per-partition monotonic offsets, final ownership
    converges, and durable group cursor offsets advance through every produced partition.

- [x] `active_broker_restart_continuity` requires a source checkpoint for every asserted cursor.
  - Covered: every partition with produced traffic has a cursor at or beyond its observed delivery
    offset and retains a source checkpoint.

- [x] `graceful_broker_restart_waits_for_partition_drain_before_lease_release` holds accepted
  work at `BrokerBeforeFlushPersist` while the broker drains.
  - Covered: the test observes target-partition drain start, rejects later same-partition work
    from the draining owner, releases blob/metadata persistence and drain/lease-release gates in
    causal order, then proves the accepted record reaches a live group consumer and its durable
    cursor after restart.

### P1: Faulted Group Convergence

- [x] `bootstrap_rebalance_with_membership_and_lease_faults` proves deterministic convergence
  beyond eventual set equality.
  - Verified coverage: the test waits for the planner fault before enabling the remaining exact fault script,
    checks the planner, assignment, partition-heartbeat, and membership-heartbeat budgets in
    causal order, propagates commit errors, and records per-ID at-least-once delivery with each
    partition's maximum observed offset retained for the durable cursor oracle.
  - Completed: final lease owners are queried against `list_active_members` at final logical time;
    every owner is active while durable cursors and revocation evidence remain required.
  - Completed: each logical-time advance follows an observed scripted store fault or a concrete
    consumer delivery/revocation, and the post-fault rebalance is held at a lifecycle boundary.

- [x] `consumer_lease_store_heartbeat_failover` uses a live iterator handoff.
  - Covered: A commits a pre-failure record, its partition heartbeat fault makes the commit fail,
    and persistent membership-heartbeat failure crosses the membership lease horizon. B then takes
    every lease through normal coordination at a newer generation.
  - Final proof: A cannot deliver the post-fencing record, B alone receives and commits it on the
    same partition, and B's final lease retains the durable source checkpoint.

- [x] Harden producer-ack fault tests with complete group-delivery and durable-checkpoint proof.
  - Covered: `metadata_write_fail_then_retry_ack_semantics` drains through a same-partition marker
    and requires only the successful record and marker. Response loss requires exactly two
    deliveries and the second cursor's source checkpoint. The scripted transport trace requires
    exact-once group delivery and durable source checkpoints for every touched partition.

### P1: Existing Group-Test Oracles

- [x] Declare duplicate policy and retain per-ID delivery counts in the older live-group tests.
  - Verified coverage: `autoscaling_rebalance_and_failover_preserves_progress`,
    `group_rebalance_continuous_traffic_no_loss`,
    `prefetch_rebalance_delayed_metadata_no_loss`, and
    `dynamic_membership_scale_out_rebalances` all use at-least-once delivery.
  - Completed: `run_consumer_task` emits `Batch` before `store_offset` and commit, emits a
    separate commit-success event, and each task-driven scenario independently waits for durable
    source checkpoints.
  - Completed: the existing group scenarios retain their per-ID and durable-cursor assertions,
    while lifecycle gates now control their membership transition boundaries.

- [x] `dynamic_membership_scale_out_rebalances` observes convergence concurrently.
  - Covered: both bootstrap member polls are started together during convergence, so an idle
    member's poll timeout cannot delay a concrete event from its peer.
  - Completed: its commit cadence uses ownership and completed-poll boundaries.
  - Completed: the related prefetch revocation test confirms target-partition ownership through a
    durable lease and yields cooperatively while scoped lifecycle gates control the handoff.

### P2: Scope Existing Fault Tests Precisely

- [x] Rename `combined_network_and_metadata_faults_preserve_producer_publication` so its
  direct-reader final oracle explicitly covers producer retry and durable visibility, not
  consumer-group recovery.
  - Covered: the test drives its scripted transport and metadata faults with a manual retry clock,
    then uses `ConsumerReaderImpl` only to verify that the successfully acknowledged records are
    durably readable without loss.

- [x] Make the permitted duplicate multiplicity explicit in
  `network_response_loss_after_persistence_retries_with_duplicate_batch`.
  - Covered: the ambiguous-ack scenario requires exactly two deliveries of the one application
    record, consecutive offsets `0` and `1` on the acknowledged partition, and a durable group
    cursor at offset `1`.

## New Coverage To Add

### P0: Shared Test Framework Support

- [x] Add member- and generation-scoped consumer lifecycle gates.
  - Covered: `TestLifecycleHooks::arm_consumer` matches a consumer event by member ID, optional
    partition, and optional generation. The prefetch revocation fencing test verifies that the
    exact A handoff reaches its gate.

- [x] Add a local test helper for explicit consumer delivery control.
  - Why: `run_consumer_task` and `poll_consumer_once` store and commit every record, hiding the
    crash, delayed-commit, and revocation boundaries the lifecycle tests need. The task helper
    also emits its delivery trace only after commit, so it cannot represent failed-commit
    redelivery multiplicity.
  - Acceptance: tests can receive one record or revocation, choose whether to `store_offset`,
    `commit`, complete revocation, graceful-shutdown, or use the explicit crash primitive, while
    recording member/partition/offset/ID delivery traces.
  - Covered locally: `ControlledConsumer` supports those operations and traces in
    `end_to_end_test.rs`. Promote it to shared test-framework support only if a second test module
    needs the same control surface.

- [x] Add a direct-reader delivery-trace and controlled-rescan helper.
  - Why: `drain_reader_until` ends on unique-ID cardinality and sleeps on wall time, so direct
    reader tests cannot distinguish duplicates from successful no-loss delivery or coordinate a
    re-scan against a named store/fault event.
  - Covered: `rescan_reader_with_trace` performs one caller-timed scan and retains every delivery.
    The S3 blob-get and stale-metadata fault tests use it with zero visibility and explicit
    scripted-fault boundaries rather than retry sleeps. The generic drain helpers now require a
    caller-supplied visibility horizon and yield cooperatively rather than polling with
    `runtime_sleep`.
  - Acceptance: callers receive every ID plus its partition and sequence/offset observation,
    declare exact-once or at-least-once multiplicity, and drive each retry/rescan from an explicit
    event or supplied test clock rather than `runtime_sleep`.

- [x] Complete explicit direct-reader fault-script boundaries.
  - Covered: `metadata_scan_stale_visibility_no_duplicate_progress` exhausts and asserts all
    eight scripted `metadata_scan_window` stale-read faults before its clean no-duplicate rescans.

- [x] Add manually scheduled transport delay/reorder support.
  - Covered: `ManualNetworkScheduler` replaces elapsed delay and reorder fallback waits when
    enabled. The ordering test waits for both delayed requests, advances them explicitly, and
    verifies the paired reorder trace.

### P0: Consumer Lifecycle Scenarios

- [x] Prove graceful shutdown's final checkpoint commits staged work before lease release.
  - Gap: `consumer_restart_resume_from_committed_offsets` commits phase-one work before
    shutdown, so its lifecycle gates prove ordering but not the final-commit data path.
  - Acceptance: a live group consumer calls `store_offset` without `commit`; a shutdown commit
    gate proves the durable cursor and source checkpoint reach that exact offset before the
    release gate opens; a restarted consumer does not replay the record.
  - Covered by `graceful_shutdown_final_checkpoint_commits_staged_record_before_release`: the
    test observes the unstored cursor before shutdown, holds final commit completion and lease
    release independently, then proves a restarted member only receives a post-restart marker.

- [x] Prove a final-checkpoint failure retains at-least-once recovery semantics.
  - Gap: shutdown release and deregistration faults are covered, but no test injects a
    `ConsumerHeartbeatPartition` failure into shutdown's final commit. The implementation
    releases leases and deregisters membership before returning the commit error.
  - Acceptance: stage a record, fault the shutdown commit, and require shutdown to report the
    error while its membership and leases are released. A replacement live consumer must then
    redeliver the record exactly once and durably commit its source checkpoint.
  - Covered by `graceful_shutdown_final_commit_failure_redelivers_staged_record`: a one-shot
    final heartbeat failure leaves the cursor unchanged, while shutdown still releases and
    deregisters. The replacement redelivers the exact staged offset once and durably commits it.
  - Current assessment: the observed behavior matches the intended at-least-once contract; no
    product defect was found.

- [x] Crash-like handoff of an uncommitted record.
  - Covered by `consumer_crash_recovery_redelivers_only_uncommitted_record`.

- [x] Commit race at ownership loss.
  - Covered by `live_consumer_commit_race_is_fenced_and_redelivered`: A stages a record before a
    gated commit, B takes ownership after manual expiry, the stale commit is fenced, and B
    redelivers and durably commits the record from the prior cursor.

- [x] Shutdown fault matrix for release and deregistration.
  - Covered: one-partition graceful shutdown faults make the residual state deterministic.
    Release failure is returned from shutdown, retains the unexpired lease while membership is
    removed, and fences replacement ownership until logical expiry. Deregistration failure is
    reported to telemetry but does not fail shutdown; released leases remain available while the
    residual member prevents complete replacement ownership until logical expiry.

### P1: Consumer Restart, Recovery, And Prefetch

- [x] `live_group_restart_recovers_retained_history_before_fast_path` reaches Fast through normal
  group coordination after retained recovery.
  - Covered: A commits a live source checkpoint and gracefully leaves the group. A real producer
    then publishes one ID in each of three manually advanced windows; B skips the checkpoint,
    reaches the gated `ConsumerRecoveryFastPathActive` boundary, exactly-once delivers every
    downtime ID, and durably checkpoints the recovered partition.

- [x] `live_group_recovery_waits_for_deferred_historical_visibility` blocks later recovery at a
  deferred historical window.
  - Covered: a live B restart sees a named middle-window visibility hold while a later window is
    visible. Its diagnostics remain `Recovering`, its durable cursor stays at A's checkpoint, and
    any deferred/later application delivery fails immediately. Releasing the named boundary
    delivers the historical IDs in order and durably checkpoints the partition.

- [x] `graceful_shutdown_recovers_prefetched_undelivered_record` preserves application delivery
  semantics for buffered work.
  - Covered: A reaches `ConsumerPrefetchBatchBuffered` with no caller `next()` and no durable
    cursor, then gracefully releases its lease. B alone application-delivers the buffered ID once
    and durably commits its source checkpoint.

### P1: Broker And Consumer Interaction

- [x] Broker restart combined with consumer membership movement.
  - Covered: `active_broker_restart_continuity` starts a third live group consumer after the
    active writer reaches its partition-scoped drain gate, holds the joining member at its
    rebalance boundary, then releases it while accepted in-flight and continuous producer traffic
    cross the restart. The live group proves exact-once delivery, membership-driven revocation,
    final ownership, and durable cursor/source-checkpoint coverage without a reader fallback.

- [x] Ambiguous producer response during broker membership movement.
  - Covered: `response_loss_retry_during_broker_handoff_preserves_group_delivery_contract`
    persists on A, drops A's response, routes the already-retrying producer to B while A's lease
    release is held, then releases A and waits for B ownership. Producer acknowledgement has two
    attempts; the live group observes the application ID exactly twice on one partition with
    increasing offsets, and its durable cursor/source checkpoint covers the maximum observed
    offset. A broker handoff may reserve a new sequence range, so offset contiguity is not part of
    the duplicate contract.

- [x] S3/Dynamo graceful-restart lifecycle smoke path.
  - Covered: `bootstrap_graceful_restart_resumes_durable_s3_dynamo_progress` builds both
    iterators from the production bootstrap configuration. Each iterator first reaches its
    member-scoped initial rebalance boundary. The first iterator exactly-once commits phase 1,
    reaches gated shutdown commit/release/deregistration boundaries, and removes its membership.
    The rebuilt iterator does not replay phase 1, exactly-once commits phase 2, and leaves durable
    cursor/source-checkpoint coverage in Dynamo for every observed partition.

### P2: Direct-Reader Oracle Cleanup

- [x] Replace remaining set-only initial-delivery checks with exact traces.
  - Covered: `single_broker_single_record_end_to_end` and
    `single_broker_cursor_monotonicity_and_dedup` now use `drain_reader_until_with_trace` and
    require an exact one-delivery-per-ID map before retaining their existing re-scan/cursor
    assertions.

## Suggested Implementation Order

- [x] Deterministic crash-like consumer handoff.
- [x] Partition-scoped prefetch lifecycle gate.
- [x] Member/generation-scoped consumer gates.
- [x] Explicit-delivery helper.
- [x] Complete direct-reader controlled-rescan helper.
- [x] Manual transport delay/reorder scheduler.
- [x] Harden deterministic fault timing, including the producer lease-conflict/reroute transition.
- [x] Complete deterministic prefetch revocation fencing.
- [x] Complete bootstrap scale-in with named phase boundaries.
- [x] Complete scoped phase boundaries for existing group tests.
- [x] Explicit duplicate policy for existing group-test terminal assertions.
- [x] Harden restart/recovery durable cursor oracles.
- [x] Commit-race test.
- [x] In-flight broker drain with live group consumers.
- [x] Broker restart plus member movement and ambiguous-response movement.
- [x] S3/Dynamo graceful-restart lifecycle smoke coverage.

## Verification Standard For Each Checked Item

- The focused `cargo nextest run -p blob-stream-integration-tests <test-name>` passes.
- `cargo clippy -p blob-stream-integration-tests --bins --examples --tests -- --no-deps`
  passes for test-only changes; include touched runtime crates when framework/runtime code changes.
- `cargo +nightly fmt -- --check` passes.
- The full `cargo nextest run -p blob-stream-integration-tests` suite passes before marking
  the audit item complete.
