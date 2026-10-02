import argparse
import collections
import csv
import datetime
import hashlib
import json
import pathlib
import shutil
import statistics


ROOT = pathlib.Path(__file__).resolve().parent
SOURCE = "69e134032e628e9e2c5f1bd5"
START = "2026-10-02T21:40:54.000Z"
END = "2026-10-02T21:44:54.000Z"
WHERE = (
    "ResourceAttributes['k8s.pod.name'] LIKE '%merge-worker%' "
    "AND ServiceName = 'loop-api' AND SpanName IN "
    "('blob_stream.consumer.assignment', 'blob_stream.consumer.partition_handoff', "
    "'blob_stream.consumer.partition_recovery', 'blob_stream.consumer.revocation_handoff', "
    "'blob_stream.consumer.shutdown')"
)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def plan_batches():
    hashes = json.loads((ROOT / "hashes.json").read_text())
    if len(hashes) != 412:
        raise ValueError(f"Expected 412 hash entries, got {len(hashes)}")
    counts = collections.Counter(hashes)
    batches = []
    for bucket, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        if count > 10:
            raise ValueError(f"Bucket {bucket} exceeds the safe response size")
        target = next((batch for batch in batches if batch["expected"] + count <= 10), None)
        if target is None:
            target = {"buckets": [], "expected": 0}
            batches.append(target)
        target["buckets"].append(bucket)
        target["expected"] += count
    for index, batch in enumerate(batches):
        batch["index"] = index
        batch["where"] = WHERE + " AND cityHash64(SpanId) % 1024 IN (" + ",".join(
            str(bucket) for bucket in batch["buckets"]
        ) + ")"
    write_json(ROOT / "manifest.json", {
        "sourceId": SOURCE, "startTime": START, "endTime": END,
        "whereLanguage": "sql", "expected": len(hashes), "batches": batches,
    })
    for batch in batches:
        print(f"{batch['index']:02}: {batch['expected']:2} rows; buckets {','.join(map(str, batch['buckets']))}")
    print(f"Total: {sum(batch['expected'] for batch in batches)} rows in {len(batches)} batches")


def reject_trimmed(value):
    if isinstance(value, dict):
        if value.get("__hdx_trimmed"):
            raise ValueError("MCP response contains trimmed data")
        for child in value.values():
            reject_trimmed(child)
    elif isinstance(value, list):
        for child in value:
            reject_trimmed(child)


def timestamp_ns(value):
    whole, fraction = value.split(".")
    seconds = int(datetime.datetime.strptime(whole, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=datetime.timezone.utc
    ).timestamp())
    return seconds * 1_000_000_000 + int(fraction.ljust(9, "0"))


def analyze(inputs, expected):
    spans = []
    provenance = []
    existing_provenance = {
        record["file"]: record for record in json.loads((ROOT / "provenance.json").read_text())
    } if (ROOT / "provenance.json").exists() else {}
    raw_directory = ROOT / "raw"
    raw_directory.mkdir(exist_ok=True)
    for index, path in enumerate(inputs):
        document = json.loads(path.read_text())
        reject_trimmed(document)
        result = document["result"]
        if document.get("note") or result["rows"] != len(result["data"]):
            raise ValueError(f"Incomplete MCP response: {path}")
        spans.extend(result["data"])
        destination = raw_directory / f"batch-{index:02}.json"
        if path.resolve() != destination.resolve():
            shutil.copyfile(path, destination)
        source_provenance = {
            "file": str(destination.relative_to(ROOT)), "original": str(path),
            "rows": len(result["data"]), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        if path.resolve() == destination.resolve():
            source_provenance = existing_provenance.get(source_provenance["file"], source_provenance)
        provenance.append(source_provenance)
    keys = [(span["TraceId"], span["SpanId"]) for span in spans]
    if len(keys) != expected or len(set(keys)) != expected:
        raise ValueError(f"Expected {expected} distinct spans; got {len(keys)} rows, {len(set(keys))} unique")
    spans.sort(key=lambda span: (timestamp_ns(span["timestamp_ns"]), span["SpanId"]))
    (ROOT / "spans.jsonl").write_text("".join(json.dumps(span) + "\n" for span in spans))
    records = []
    for span in spans:
        attributes = span["SpanAttributes"]
        record = {
            "timestamp": span["timestamp_ns"], "duration_seconds": int(span["duration_ns"]) / 1e9,
            "span": span["SpanName"].removeprefix("blob_stream.consumer."),
            "pod": span["ResourceAttributes"].get("k8s.pod.name"),
            "cluster": span["ResourceAttributes"].get("k8s.cluster.name"),
            "trace_id": span["TraceId"], "span_id": span["SpanId"],
        }
        record.update(attributes)
        for attribute, prefix in [("recovery.summary_json", "recovery."), ("handoff.snapshot_json", "snapshot.")]:
            if attribute in attributes:
                decoded = json.loads(attributes[attribute])
                for name, value in decoded.items():
                    record[prefix + name] = value
        records.append(record)
    columns = sorted({column for record in records for column in record})
    with (ROOT / "spans.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)
    counts = collections.Counter(record["span"] for record in records)
    write_json(ROOT / "records.json", records)
    write_json(ROOT / "provenance.json", provenance)
    write_json(ROOT / "verification.json", {
        "rows": len(spans), "unique_trace_span_pairs": len(set(keys)),
        "raw_responses": len(inputs), "span_types": counts,
        "all_responses_complete": True, "no_trimming_markers": True,
        "first_span_start": spans[0]["timestamp_ns"], "last_span_start": spans[-1]["timestamp_ns"],
    })
    summarize(records)
    print(json.dumps({"rows": len(spans), "counts": counts, "pods": len({record["pod"] for record in records})}, indent=2))


def summarize(records):
    recoveries = [record for record in records if record["span"] == "partition_recovery"]
    grouped = collections.defaultdict(list)
    for record in recoveries:
        grouped[(record["pod"], record.get("consumer.generation"))].append(record)
    summary = []
    for (pod, generation), group in grouped.items():
        metric_names = [
            "metadata_batches_seen", "batches_skipped_by_cursor", "batches_accepted",
            "records_accepted", "batches_deferred_by_capacity", "segments_deferred_by_visibility",
            "segments_skipped_by_frontier", "recovery_segments_blocked_by_visibility",
            "recovery_segments_handed_to_fast_by_visibility",
        ]
        totals = {name: sum(record.get("recovery." + name, 0) for record in group) for name in metric_names}
        summary.append({
            "pod": pod, "generation": generation, "partitions": len(group),
            "start": min(record["timestamp"] for record in group),
            "max_seconds": max(record["duration_seconds"] for record in group),
            "median_seconds": statistics.median(record["duration_seconds"] for record in group),
            "scan_passes": sorted({int(record["recovery.scan_passes"]) for record in group}),
            "outcomes": dict(collections.Counter(record.get("recovery.outcome") for record in group)),
            **totals,
            "cursor_skip_fraction": totals["batches_skipped_by_cursor"] / totals["metadata_batches_seen"] if totals["metadata_batches_seen"] else 0,
        })
    phase_counts = collections.Counter(record.get("handoff.phase") for record in records if record["span"] == "partition_handoff")
    assignments = [{name: value for name, value in record.items() if name.startswith("assignment.") or name in (
        "timestamp", "pod", "cluster", "consumer.generation", "trace_id", "duration_seconds"
    )} for record in records if record["span"] == "assignment"]
    revocations = [record for record in records if record["span"] in ("revocation_handoff", "shutdown")]
    totals = {name: sum(record.get("recovery." + name, 0) for record in recoveries) for name in metric_names}
    durations = sorted(record["duration_seconds"] for record in recoveries)
    quantiles = statistics.quantiles(durations, n=100, method="inclusive")
    totals.update({
        "partitions": len(recoveries), "cursor_skip_fraction": totals["batches_skipped_by_cursor"] / totals["metadata_batches_seen"],
        "metadata_observations_per_accepted_batch": totals["metadata_batches_seen"] / totals["batches_accepted"],
        "recovery_min_seconds": min(durations), "recovery_median_seconds": statistics.median(durations),
        "recovery_p95_seconds": quantiles[94], "recovery_max_seconds": max(durations),
        "revocation_min_seconds": min(record["duration_seconds"] for record in revocations if record["span"] == "revocation_handoff"),
        "revocation_max_seconds": max(record["duration_seconds"] for record in revocations if record["span"] == "revocation_handoff"),
    })
    write_json(ROOT / "summary.json", {
        "totals": totals,
        "recoveries_by_pod": summary, "handoff_phases": phase_counts,
        "assignments": assignments, "revocations": revocations,
    })
    movement_summary(records, recoveries)


def movement_summary(records, recoveries):
    releases = {}
    final_assignment = {}
    assigned_records = []
    release_results = {}
    for record in records:
        partition = record.get("messaging.partition")
        phase = record.get("handoff.phase")
        if phase in ("shutdown_pre_release", "revocation_pre_release"):
            if partition in releases:
                raise ValueError(f"Repeated release for partition {partition}")
            releases[partition] = record
        if phase in ("shutdown_release_result", "revocation_release_result"):
            release_results[partition] = record
        if phase in ("startup_assigned", "rebalance_assigned"):
            assigned_records.append(record)
            final_assignment[partition] = record
    previous_assignment = {
        partition: releases.get(partition, record)["pod"]
        for partition, record in final_assignment.items()
    }
    next_assignment = {partition: record["pod"] for partition, record in final_assignment.items()}
    recovery_by_partition = {record["messaging.partition"]: record for record in recoveries}
    transitions = []
    for partition, old in releases.items():
        recovery = recovery_by_partition[partition]
        new = next(record for record in assigned_records if record["messaging.partition"] == partition
                   and record["pod"] == recovery["pod"] and record["timestamp"] >= old["timestamp"])
        release = release_results[partition]
        transitions.append({
            "partition": int(partition), "logical_partition": old["snapshot.logical_partition_id"],
            "old_pod": old["pod"], "new_pod": recovery["pod"],
            "reason": old["handoff.phase"], "revocation_start": old["timestamp"],
            "release": release["timestamp"], "recovery_start": recovery["timestamp"],
            "assignment_active": new["timestamp"],
            "pending_assignment_seconds": max(0, (timestamp_ns(new["timestamp"]) - timestamp_ns(recovery["timestamp"])) / 1e9),
            "activation_to_fast_seconds": (timestamp_ns(recovery["timestamp"]) - timestamp_ns(new["timestamp"])) / 1e9 + recovery["duration_seconds"],
            "release_to_recovery_seconds": (timestamp_ns(recovery["timestamp"]) - timestamp_ns(release["timestamp"])) / 1e9,
            "revocation_to_recovery_seconds": (timestamp_ns(recovery["timestamp"]) - timestamp_ns(old["timestamp"])) / 1e9,
            "recovery_seconds": recovery["duration_seconds"],
            "revocation_to_fast_seconds": (timestamp_ns(recovery["timestamp"]) - timestamp_ns(old["timestamp"])) / 1e9 + recovery["duration_seconds"],
            "pre_ack_committed_cursor": old["snapshot.last_committed_offset"],
            "pre_ack_read_cursor": old["snapshot.cursor"],
            "claimed_committed_cursor": new["snapshot.last_committed_offset"],
            "cursor_key_matches_pre_ack": recovery["handoff.cursor_key"] == old["handoff.cursor_key"],
            "cursor_key_matches_assignment": recovery["handoff.cursor_key"] == new["handoff.cursor_key"],
            "pre_ack_buffered_batches": old["snapshot.prefetch_buffered_batch_count"],
            "pre_ack_buffered_records": old["snapshot.prefetch_buffered_record_count"],
        })
    def colocation(assignment):
        owners = collections.defaultdict(set)
        for partition, pod in assignment.items():
            logical = final_assignment[partition]["snapshot.logical_partition_id"]
            owners[logical].add(pod)
        return {"observed_logical_groups": len(owners), "observed_single_owner_groups": sum(len(group) == 1 for group in owners.values()),
            "observed_split_groups": sum(len(group) > 1 for group in owners.values())}
    movements = sum(previous_assignment[partition] != next_assignment[partition] for partition in next_assignment)
    if movements != len(transitions) or len(recovery_by_partition) != len(recoveries):
        raise ValueError("Release and recovery counts do not match unique ownership transitions")
    old_load = collections.Counter(previous_assignment.values())
    new_load = collections.Counter(next_assignment.values())
    result = {
        "observed_partitions": len(next_assignment), "moves": movements,
        "observed_old_owners": len(old_load), "observed_new_owners": len(new_load),
        "old_load": old_load, "new_load": new_load,
        "old_colocation": colocation(previous_assignment), "new_colocation": colocation(next_assignment),
        "old_assignment": previous_assignment, "new_assignment": next_assignment,
        "transitions": sorted(transitions, key=lambda transition: transition["partition"]),
        "release_outcomes": dict(collections.Counter(record.get("handoff.outcome") for record in release_results.values())),
        "pre_ack_buffers": {
            "batches": sum(transition["pre_ack_buffered_batches"] for transition in transitions),
            "records": sum(transition["pre_ack_buffered_records"] for transition in transitions),
        },
        "by_reason": {},
        "max_release_to_recovery_seconds": max(transition["release_to_recovery_seconds"] for transition in transitions),
        "max_revocation_to_fast_seconds": max(transition["revocation_to_fast_seconds"] for transition in transitions),
        "max_pending_assignment_seconds": max(transition["pending_assignment_seconds"] for transition in transitions),
        "max_activation_to_fast_seconds": max(transition["activation_to_fast_seconds"] for transition in transitions),
    }
    for reason in ("shutdown_pre_release", "revocation_pre_release"):
        matching = [transition for transition in transitions if transition["reason"] == reason]
        partition_ids = {str(transition["partition"]) for transition in matching}
        matching_recoveries = [record for record in recoveries if record["messaging.partition"] in partition_ids]
        result["by_reason"][reason] = {
            "moves": len(matching),
            **{name: sum(record["recovery." + name] for record in matching_recoveries) for name in (
                "metadata_batches_seen", "batches_skipped_by_cursor", "batches_accepted", "records_accepted", "batches_deferred_by_capacity",
            )},
        }
    write_json(ROOT / "movement.json", result)
    with (ROOT / "transitions.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(transitions[0]))
        writer.writeheader()
        writer.writerows(sorted(transitions, key=lambda transition: transition["partition"]))


def colocated_assignment(owners, partitions, previous, logical_count):
    groups = collections.defaultdict(list)
    for partition in partitions:
        groups[partition % logical_count].append(partition)
    groups = sorted(groups.items(), key=lambda item: (-len(item[1]), item[0]))
    capacity = {owner: len(partitions) // len(owners) for owner in owners}
    previous_load = collections.Counter(previous.values())
    residual_owners = sorted(owners, key=lambda owner: (-previous_load[owner], owner))
    for owner in residual_owners[:len(partitions) % len(owners)]:
        capacity[owner] += 1
    assigned = {}
    for _, group in groups:
        whole_owners = [owner for owner in owners if capacity[owner] >= len(group)]
        if whole_owners:
            owner = min(whole_owners, key=lambda owner: (
                capacity[owner] - len(group), -sum(previous.get(partition) == owner for partition in group), owner,
            ))
            for partition in group:
                assigned[partition] = owner
                capacity[owner] -= 1
        else:
            for partition in group:
                owner = min((owner for owner in owners if capacity[owner]), key=lambda owner: (
                    -(previous.get(partition) == owner), -capacity[owner], owner,
                ))
                assigned[partition] = owner
                capacity[owner] -= 1
    if any(capacity.values()) or len(assigned) != len(partitions):
        raise ValueError("Policy reproduction did not fill the balanced capacities")
    return assigned


def policy_probe():
    def whole_groups(assigned, logical_count):
        return sum(len({assigned[partition] for partition in assigned if partition % logical_count == logical}) == 1
                   for logical in range(logical_count))
    for logical_count in range(4, 21):
        partitions = list(range(logical_count * 2))
        for owner_count in range(3, min(10, len(partitions))):
            owners = [f"pod-{index:02}" for index in range(owner_count)]
            old = colocated_assignment(owners, partitions, {}, logical_count)
            for departed in owners:
                survivors = [owner for owner in owners if owner != departed]
                retained = {partition: owner for partition, owner in old.items() if owner != departed}
                greedy = colocated_assignment(survivors, partitions, retained, logical_count)
                sticky = dict(retained)
                load = collections.Counter(retained.values())
                for partition in partitions:
                    if partition not in sticky:
                        owner = min(survivors, key=lambda owner: (load[owner], owner))
                        sticky[partition] = owner
                        load[owner] += 1
                if max(load.values()) - min(load.values()) > 1:
                    continue
                greedy_moves = sum(greedy[partition] != old[partition] for partition in partitions)
                sticky_moves = sum(sticky[partition] != old[partition] for partition in partitions)
                if greedy_moves > sticky_moves and whole_groups(sticky, logical_count) >= whole_groups(greedy, logical_count):
                    result = {
                        "kind": "synthetic_uniform_cluster_counterexample_not_event_replay",
                        "logical_groups": logical_count, "partitions": len(partitions),
                        "departed": departed, "greedy_moves": greedy_moves, "sticky_moves": sticky_moves,
                        "greedy_whole_groups": whole_groups(greedy, logical_count),
                        "sticky_whole_groups": whole_groups(sticky, logical_count),
                        "greedy_load": collections.Counter(greedy.values()), "sticky_load": load,
                        "old": old, "greedy": greedy, "sticky": sticky,
                    }
                    write_json(ROOT / "policy-probe.json", result)
                    print(json.dumps(result, indent=2))
                    return
    raise ValueError("No counterexample found in the tested fixtures")


def event_shaped_probe():
    logical_count = 64
    owners = [f"pod-{index:02}" for index in range(21)]
    previous = {
        logical_id + logical_count * partition_copy: owners[logical_id // 2]
        for logical_id in range(42)
        for partition_copy in range(3)
    }
    previous.update({
        logical_id + logical_count * partition_copy: owners[(logical_id - 42) * 3 + partition_copy]
        for logical_id in range(42, 45)
        for partition_copy in range(3)
    })
    previous[63] = owners[9]
    departed = owners[-1]
    survivors = owners[:-1]
    feasible = {partition: owner for partition, owner in previous.items() if owner != departed}
    orphans = sorted(set(previous) - set(feasible))
    feasible.update(zip(orphans, owners[10:16]))
    greedy = colocated_assignment(survivors, sorted(previous), previous, logical_count)

    def split_groups(assignment):
        groups = collections.defaultdict(set)
        for partition, owner in assignment.items():
            groups[partition % logical_count].add(owner)
        return sum(len(group) > 1 for group in groups.values())

    def moves(assignment, survivors_only=False):
        return sum(assignment[partition] != owner for partition, owner in previous.items()
                   if not survivors_only or owner != departed)

    old_load = collections.Counter(previous.values())
    feasible_load = collections.Counter(feasible.values())
    if (len(previous) != 136 or collections.Counter(old_load.values()) != {6: 11, 7: 10}
            or collections.Counter(feasible_load.values()) != {6: 4, 7: 16}
            or split_groups(previous) != 3 or split_groups(feasible) != 5
            or moves(feasible) != 6 or moves(feasible, survivors_only=True) != 0):
        raise ValueError("Event-shaped fixture violates its balance, co-location, or movement contract")
    result = {
        "kind": "synthetic_event_shaped_counterexample_not_production_replay",
        "logical_partition_count": logical_count, "partitions": len(previous), "departed": departed,
        "old_split_groups": split_groups(previous), "greedy_split_groups": split_groups(greedy),
        "feasible_split_groups": split_groups(feasible), "greedy_moves": moves(greedy),
        "feasible_moves": moves(feasible), "greedy_survivor_moves": moves(greedy, survivors_only=True),
        "feasible_survivor_moves": moves(feasible, survivors_only=True), "old_load": old_load,
        "greedy_load": collections.Counter(greedy.values()), "feasible_load": feasible_load,
        "old_assignment": previous, "greedy_assignment": greedy, "feasible_assignment": feasible,
    }
    write_json(ROOT / "event-shaped-probe.json", result)
    print(json.dumps({name: value for name, value in result.items()
                      if not name.endswith("_assignment") and not name.endswith("_load")}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--policy-probe", action="store_true")
    parser.add_argument("--event-shaped-probe", action="store_true")
    parser.add_argument("--expected", type=int, default=412)
    parser.add_argument("--resource-dir", type=pathlib.Path)
    parser.add_argument("--min-call", type=int, default=1790903619611)
    parser.add_argument("--max-call", type=int, default=1790903619653)
    parser.add_argument("inputs", nargs="*", type=pathlib.Path)
    arguments = parser.parse_args()
    if arguments.plan:
        plan_batches()
    elif arguments.policy_probe:
        policy_probe()
    elif arguments.event_shaped_probe:
        event_shaped_probe()
    else:
        inputs = arguments.inputs
        if arguments.resource_dir:
            inputs = sorted(
                (path for path in arguments.resource_dir.glob("call_*/content.json")
                 if arguments.min_call <= int(path.parent.name.rsplit("-", 1)[1]) <= arguments.max_call),
                key=lambda path: int(path.parent.name.rsplit("-", 1)[1]),
            )
            inputs = [path for path in inputs if "result" in json.loads(path.read_text())]
        analyze(inputs, arguments.expected)
