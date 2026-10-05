# Appendix: Read-Path Scenarios

These examples explain the [consumption protocol](consumption.md). They are illustrative, not an
additional behavioral contract.

**Fresh assignment.** Assume topic `telemetry` uses 300-second windows and `now = 950`, so the
current window is `[900, 1200)`. Partition 7 has no committed cursor and is therefore Fresh. It
queries `pk = telemetry#900` with no Sonyflake lower bound. If the query returns an already
visibility-eligible segment containing sequence range `[1, 2]`, the reader delivers that range,
advances its in-memory cursor to 2, and enters Fast after it completes this one window.
With broker metadata reads enabled, this unbounded Fresh query requests `FullRecovery` coverage
for partition 7. An eligible eventual broker entry can answer it; a strong read can reuse only a
proven sealed prefix and must query the open suffix. The reader does not retain this open Fresh
window as mature recovery metadata.

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
legacy recovery has no checkpoint lower bound. To stress ordering, assume a `[2, 2]` row in the
first window was published at `metadata_published_ts_ms = 1,229,000`, beyond the normal 15-second
publication deadline for that window. It is deferred because it is newer than `1,230,000 - 2,000`.
The `[3, 3]` row in the cutover window has `metadata_published_ts_ms = 1,200,000` and is already
visible. The pass delivers neither range and retains cursor 1, because the deferred earlier window
blocks recovery progress. At `now = 1,232`, the `[2, 2]` row becomes eligible; the next pass
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
In the current window, partition 7 has an inclusive frontier at Sonyflake timestamp 1,005.
Partition 8 has unfinished coverage with a retained coverage floor and inclusive frontier at
timestamp 1,001; without that older coverage floor, its bound would be $T_{safe}$. Their shared
query uses the lower effective bound, timestamp 1,001; partition 7 filters rows below its own
1,005 frontier locally, while partition 8 can still receive rows at or after 1,001.

**Broker and direct reads for several selected batches.** Assume a selected segment from topic
`telemetry` contains three independently compressed batches: batches for owned partitions 7 and 9
occupy `[0, 100)` and `[150, 240)`, while an unowned partition 8 batch occupies `[100, 150)`. Window
size, publication deadline, and visibility delay no longer affect this stage: metadata selection has
already chosen the two owned batches. The reader requests `[0, 100)` and `[150, 240)` in one broker
RPC; the broker fetches or reuses the full blob and returns only those two slices. If the broker
cannot deliver them, the direct fallback reads `[0, 240)` in one object-store request and decodes
only the owned batches. That fallback intentionally overfetches the middle 50 bytes to avoid a
second object-store request.

## Cache Use Across Reads

The reader has two metadata reuse paths: complete mature windows and proven sealed Fast prefixes.
The broker separately caches eventual `Tail` and `FullRecovery` metadata, strong sealed prefixes,
and whole blob objects. These layers are independent: a reader hit avoids the broker RPC entirely;
a broker hit avoids a storage read. The examples below assume broker reads are available and the
requested data fits cache limits; a miss or eviction still follows the normal read path.

**Two eventual readers in the same window.** At `now = 1,050`, Fresh partition 7 requests
`FullRecovery` for `[900, 1,200)` with no lower bound. The broker fills its eventual recovery
entry. A second Fresh reader requesting the same window and a partition covered by that entry can
reuse the observation if it is still within the topic's metadata cache max age (250 ms by default).
The broker scanned the entire window, so a different partition can reuse the entry: it filters
the retained response for each caller. If the observation is too old, the broker refills from
storage. A bounded checkpoint recovery or Fast request instead asks for `Tail`: it cannot use the
`FullRecovery` entry because the coverage modes have separate caches. A `Tail` entry can serve a
later request only if its original refill floor includes that request; a lower floor triggers a
refill. Strong requests cannot reuse either eventual entry.

**Strong Fast scan with an open suffix.** Suppose partition 7 has a valid Fast lower bound at
timestamp 920 in `[900, 1,200)`. At `now = 1,000`, with the default 15-second publication deadline
and 10 ms clock-skew bound, a complete strong scan can prove a seal only up to timestamp 984.990.
Rows at timestamps 930 and 940 form a reusable prefix; a row at 990 remains in the open suffix.
If that first pass admits only the row at 930, the next pass processes the reader's retained
per-partition prefix before querying the inclusive suffix from the seal boundary. With capacity
for just one more batch, the cached row at 940 fills capacity and the reader defers the suffix query
entirely. If the reader has no usable prefix, the broker may still reuse its own sealed prefix, but
it reads the open suffix on each strong request; simultaneous suffix requests can share an in-flight
query. A seal is based on the original pre-scan strong observation, never an eventual result or
elapsed time alone.

**A Fast reader falls behind within one window.** Assume strong reads, 300-second windows, the
default 15-second publication deadline, and 10 ms clock-skew bound. Partition 7 has unfinished
Fast coverage starting at timestamp 920 in `[900, 1,200)`, with sequence 1 at 930, sequence 2 at
940, and sequence 3 at 990. At `now = 1,000`, the reader has room for one batch and queries from
920. That complete scan proves
the prefix `[920, 984.990)` sealed, including the rows at 930 and 940, but delivers only the row at
930. On its next capacity-one pass, the reader replays that *same* prefix: cursor filtering skips
930, and 940 fills capacity. It does not query the open suffix. On a third pass, both cached rows
are behind the cursor, so capacity remains; the reader queries only the inclusive suffix from
984.990 and can deliver 990. The first two passes use one metadata query in total; the third needs a
second query. The cached prefix remains `[920, 984.990)`, not a stack of overlapping snapshots with
new seals after every pass.

**Strong read of a fully sealed window.** At `now = 1,500`, the entire `[900, 1,200)` window is
older than the strong publication and clock-skew horizon. A complete strong `FullRecovery` scan can
seal through the end of that window. A second strong request with the same coverage can reuse the
broker's retained sealed window without another metadata-store query, even if it selects a
different partition. This differs from the open-window case, where only the prefix can be reused
and the suffix is queried again. The reader may also reuse its own per-partition copy of a mature
Recovery window, avoiding even the broker request on later capacity-limited passes.

**Capacity-limited mature Recovery.** At `now = 1,650`, partition 7 recovers through the completed
window `[900, 1,200)`. Its first pass reads rows `[2, 2]` and `[3, 3]` but has capacity to admit
only `[2, 2]`. Because the window ended before the Fast safety floor and no row was deferred by
visibility, the reader retains its per-partition metadata (including the unread row). The next
pass can deliver `[3, 3]` without a broker RPC or another metadata-store query. If the first pass
had seen a visibility-deferred row, it would not retain that result: the retry must query again.
These retained entries are cleared on assignment or seek resets.

**Fast rollover tail shared by partitions.** Shortly after a window rolls, a Fast partition with
unfinished coverage of the prior window may need its last tail even when the normal Fast safety
floor omits that window. Once the window is mature, the reader can retain that tail's metadata per
partition across capacity-limited passes. If partitions 7 and 8 share the next query, both entries
must be present for a reader cache hit; a partial hit makes one query and refreshes both entries.
The reader keeps only the immediately preceding tail needed for this handoff, not an unbounded
history of old Fast windows.

**Falling behind across windows.** A sealed Fast prefix is keyed by partition and window; prefixes
for partitions 7 and 8 in the same window are merged in Sonyflake order, using the earliest seal
as their shared suffix boundary. If any participating partition lacks a usable prefix, the merged
request cannot skip its missing rows. Once a window leaves Fast's availability horizon, its prefix
is discarded. An immediately preceding window with unfinished coverage can use the mature Fast
rollover-tail cache described above. If partition 7's coverage falls at least two windows behind
the live Fast horizon, the reader starts bounded Recovery from that older floor instead of piling
up Fast prefixes. Complete mature Recovery windows can then be replayed from the reader's cache
across capacity-limited passes; incomplete metadata scans or visibility-deferred windows still
require queries.

**Several range reads of one blob.** After the selected `[0, 240)` span above is delivered, a
later reader can request a different range of the same immutable blob key. The broker caches the
whole object on the first miss and serves both range requests as slices of that object, without
another object-store fetch. Concurrent misses for the same key share an in-flight fetch. Retention
is conditional on whole-object cache admission, memory headroom, and idle expiry (10 seconds by
default); pressure can clear the cache. The broker reports `NOT_FOUND` authoritatively, while an
overload, invalid response, or other broker blob failure makes the consumer retry that blob-key
group directly from object storage. Metadata-cache hits do not imply blob-cache hits.

[Design overview](README.md) | [Consumption](consumption.md)
