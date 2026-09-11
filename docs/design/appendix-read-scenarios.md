# Appendix: Read-Path Scenarios

These examples explain the [consumption protocol](consumption.md). They are illustrative, not an
additional behavioral contract.

**Fresh assignment.** Assume topic `telemetry` uses 300-second windows and `now = 950`, so the
current window is `[900, 1200)`. Partition 7 has no committed cursor and is therefore Fresh. It
queries `pk = telemetry#900` with no Sonyflake lower bound. If the query returns an already
visibility-eligible segment containing sequence range `[1, 2]`, the reader delivers that range,
advances its in-memory cursor to 2, and enters Fast after it completes this one window.

**Same-window checkpoint recovery.** Assume topic `telemetry` uses 300-second windows, the broker
publication deadline is 15 seconds, and the consumer visibility delay is explicitly zero. At
`now = 1,050`, the current and cutover window is `[900, 1200)`. A replacement owner restores
partition 7 with cursor 10 and a usable source checkpoint at timestamp 1,020 in that same window.
Here $D = 15.010s$, so its one recovery query is `pk = telemetry#900` with the Sonyflake minimum
for timestamp $1,020 - 15.010 = 1,004.990$. The checkpoint row is replayed and skipped because
its sequence end is 10; a later row ending at 11 is delivered. Because the source and cutover are
the same window, recovery completes in one scan and the partition enters Fast. This is covered by
`recovery_scans_single_checkpoint_window_with_overlap_bound` and is the expected shape during a
consumer recovery or reassignment that overlaps a broker rolling restart.

**Later recovery window.** Assume topic `telemetry` uses 300-second windows, the default 15-second
publication deadline, default `max_clock_skew = 10ms`, and explicit eventual reads with the default
2-second visibility delay, making $D = 17.010s$. At `now = 1,350`, the cutover window is `[1,200,
1,500)`. Partition 7 restores a usable source checkpoint from timestamp 1,020 in the earlier `[900,
1,200)` window. Its source-window query is `pk = telemetry#900` with a Sonyflake lower bound for
$1,020 - 17.010 = 1,002.990$. Its later cutover-window query is `pk = telemetry#1200` with no
Sonyflake lower bound. Only the source window uses the checkpoint overlap; every later recovery
window remains a full-window query. Partition 7 enters Fast only after it has fully scanned the
`[1,200, 1,500)` cutover window in a recovery pass. If a visibility deferral or prefetch-capacity
limit blocks that window, it remains Recovering and retries that window on a later pass.

**Sparse partition after downtime.** Assume topic `telemetry` has seven-day retention and
300-second windows. Partition 7 last delivered sequence 10 from window `W0`, then remains idle
while the consumer group is down for six days. Its replacement owner restores cursor 10 and the
source checkpoint in `W0`, then scans every retained window from `W0` through its captured current
cutover in chronological slices of at most 32 windows. This can require many scan passes, but it
discovers records written to partition 7 while the group was down. In contrast, a partition that
has never delivered a record has no durable cursor or source checkpoint. On reassignment it is
Fresh and scans only the new current window, just as a newly created group does; it has no durable
origin from which to recover earlier retained windows.

**Recovery waits for a visibility gap.** Assume topic `telemetry` uses 300-second windows and
explicit eventual reads with a 2-second visibility delay. At `now = 1,230`, a partition with cursor
1 performs legacy recovery from window `[900, 1,200)` through its cutover window `[1,200, 1,500)`;
legacy recovery has no checkpoint lower bound. The `[2, 2]` row in the first window has
`metadata_published_ts_ms = 1,229,000`, so it is deferred because it is newer than `1,230,000 -
2,000`. The `[3, 3]` row in the cutover window has `metadata_published_ts_ms = 1,200,000` and is
already visible. The pass delivers neither range and retains cursor 1, because the deferred earlier
window blocks recovery progress. At `now = 1,232`, the `[2, 2]` row becomes eligible; the next pass
delivers `[2, 2]` followed by `[3, 3]`. This prevents cursor 3 from hiding sequence 2 and is covered
by `recovery_does_not_advance_cursor_past_visibility_deferred_window`.

**Active-window visibility handoff.** Assume a graceful replacement starts in the same 300-second
window as its committed source checkpoint. A newer row in that window is still inside the reader's
visibility delay. Recovery leaves the durable cursor at the checkpoint and enters Fast because the
deferred window is in Fast's bounded availability horizon. After the row becomes visible, Fast
rescans it from its inclusive lower bound and delivers it. This avoids waiting for the active window
to close while preserving the normal visibility and cursor protections; it is covered by
`recovery_hands_active_window_visibility_deferral_to_fast`.

**Constantly producing Fast partition.** Assume one Fast partition in topic `telemetry` produces
segments continuously in the `[900, 1,200)` window. With the default 15-second publication deadline,
default `max_clock_skew = 10ms`, and explicit eventual reads with a 2-second visibility delay, at
`now = 1,020` $D = 17.010s$ and $T_{safe} = 1,002.990$. Suppose an earlier scan already observed a
visibility-eligible segment at Sonyflake timestamp 1,015, so its inclusive frontier is 1,015. The
next query uses $max(1,002.990, 1,015) = 1,015$: it replays the boundary row and does not rescan the
interval from 1,002.990 through 1,014. If that query returns a later segment whose metadata was
published at 1,019, Fast defers that segment at `now = 1,020` because it is newer than the 2-second
visibility cutoff of 1,018. Its frontier remains 1,015 and the segment becomes eligible at `now =
1,021`. The 17.010-second floor still matters before a higher frontier exists: a segment with
Sonyflake timestamp 1,004 can be selected by that floor but, if its metadata was published at 1,019,
is separately deferred until 1,021. Thus 17.010 seconds controls how far back Fast queries; 2
seconds controls whether a returned metadata row is accepted on that pass.

**Fast time floor and frontiers.** Assume topic `telemetry` uses 300-second windows, the default
15-second publication deadline, default `max_clock_skew = 10ms`, and explicit eventual reads with a
2-second visibility delay. At `now = 1,020`, the current window is `[900, 1,200)` and $D = 17.010s$,
so $T_{safe} = 1,002.990$. The preceding window `[600, 900)` ended before $T_{safe}$ and is omitted.
In the current window, partition 7 has an inclusive frontier at Sonyflake timestamp 1,005, while
partition 8 has an inclusive frontier at timestamp 1,001. Their shared query uses the lower
effective bound, timestamp 1,001; partition 7 filters rows below its own 1,005 frontier locally,
while partition 8 can still receive rows at or after 1,001.

**One range read for several selected batches.** Assume a selected segment from topic `telemetry`
contains three independently compressed batches: batches for owned partitions 7 and 9 occupy
`[0, 100)` and `[150, 240)`, while an unowned partition 8 batch occupies `[100, 150)`. Window
size, publication deadline, and visibility delay no longer affect this stage: metadata selection
has already chosen the two owned batches. The reader issues one blob range request for `[0, 240)`,
then decodes only `[0, 100)` and `[150, 240)`. The middle 50 bytes are intentional overfetch that
trades data transfer for one fewer object-store request.

[Design overview](README.md) | [Consumption](consumption.md)
