# Rebalance Investigation: 2026-10-02

Preserved evidence for the production merge-worker rebalance at
`2026-10-02T21:40:54Z` through `2026-10-02T21:44:54Z`. Start with the
[report](report.md) and [implementation plan](../plan.md).

## Contents

- [analyze.py](analyze.py): standard-library Python analysis, integrity checks, and placement probes.
- [manifest.json](manifest.json) and [hashes.json](hashes.json): original query and safe MCP batching.
- `raw/`: 42 complete MCP response envelopes, totaling 412 unique spans.
- [provenance.json](provenance.json) and [verification.json](verification.json): original capture
  provenance, response SHA-256 hashes, uniqueness, and completeness checks.
- [spans.jsonl](spans.jsonl): full spans with original attributes and nanosecond strings.
- [spans.csv](spans.csv), [records.json](records.json), [summary.json](summary.json),
  [movement.json](movement.json), and [transitions.csv](transitions.csv): generated analysis.
- [policy-probe.json](policy-probe.json): paired-group eight-partition counterexample.
- [event-shaped-probe.json](event-shaped-probe.json): complete synthetic 21-to-20-pod case;
  64 greedy moves versus six feasible moves at equal balance/co-location. Not a production replay.

Original absolute paths in provenance record where the capture came from. They are historical
metadata, not runtime dependencies. Reruns use the adjacent raw files and retain existing provenance.

## Reproduce

Run from the Blob Stream checkout root with Python 3. No services or third-party packages are needed:

```sh
python3 -B plans/rebalance-2026-10-02/analyze.py plans/rebalance-2026-10-02/raw/batch-*.json
python3 -B plans/rebalance-2026-10-02/analyze.py --policy-probe
python3 -B plans/rebalance-2026-10-02/analyze.py --event-shaped-probe
python3 -B plans/rebalance-2026-10-02/analyze.py --plan
```

These commands regenerate derived data and probes beside the script, not the report. The analysis
rejects trimming markers, response notes, row-count mismatches, duplicate spans, and a wrong total.
It does not establish that a synthetic fixture is the complete production assignment inventory.

For another event, create a separate dated investigation directory and preserve its original capture
before adapting the script. Source, time range, hash inventory, expected count, and movement/timing
assumptions here are incident-specific. `--resource-dir`, `--min-call`, and `--max-call` can import
compatible VS Code MCP response files; `--expected` only changes the completeness check, not the
query or event assumptions. The current importer expects MCP result envelopes, not arbitrary UI CSV
or direct ClickHouse JSONEachRow. Keep that format distinction explicit when adding another importer.

## Span Export Options

Upstream checked on 2026-10-02 at `hyperdxio/hyperdx` revision
`ab7032436144bc52b5add25e4c7be637ba5f7496`. These findings describe upstream code, not a verified
deployment upgrade or a newly exercised production export.

### MCP Limits

[Search](https://github.com/hyperdxio/hyperdx/blob/ab7032436144bc52b5add25e4c7be637ba5f7496/packages/api/src/mcp/tools/query/search.ts)
allows at most 200 rows and has no offset/cursor parameter. The
[shared formatter](https://github.com/hyperdxio/hyperdx/blob/ab7032436144bc52b5add25e4c7be637ba5f7496/packages/api/src/mcp/tools/query/helpers.ts)
applies [response trimming](https://github.com/hyperdxio/hyperdx/blob/ab7032436144bc52b5add25e4c7be637ba5f7496/packages/api/src/utils/trimToolResponse.ts)
above 50,000 serialized characters. It can shrink arrays toward ten rows and replace large nested
values with `__hdx_trimmed`. This explains why increasing `maxResults` or saving the tool response
to a file did not give an intact bulk export. No bulk file-export tool was found in the checked
upstream query-tool registry. This archive's verified hash batches remain a workaround when MCP
is the only available access path; smaller column projections can reduce calls for lighter analysis.

### UI Export

The upstream [row-selection menu](https://github.com/hyperdxio/hyperdx/blob/ab7032436144bc52b5add25e4c7be637ba5f7496/packages/app/src/components/DBTable/RowSelectionMenu.tsx)
has **Download CSV**, **Copy as CSV**, and **Copy as JSON**. This is simpler for a modest selection
when the deployed UI supports it. It exports selected rows subject to an export cap, not the entire
matching search automatically. Its [row projection](https://github.com/hyperdxio/hyperdx/blob/ab7032436144bc52b5add25e4c7be637ba5f7496/packages/app/src/components/DBTable/rowExport.ts)
limits output to displayed columns. Load/select every required row and include all required raw
fields; verify count, unique IDs, attributes, events/links, and timestamp precision after export.

### Direct ClickHouse Export

For repeatable full-span bulk capture, prefer an authorized direct ClickHouse connection when
available. The [HTTP interface documentation](https://clickhouse.com/docs/interfaces/http) describes
streaming a filtered query in `JSONEachRow` format to a file. The ClickHouse `/play` UI also has a
full-result download in JSONLines and other formats, even when only one page is displayed.
Neither path uses MCP's response trimming. Availability depends on deployment version, endpoint
access, and authorized ClickHouse credentials; MCP access alone does not supply those credentials.

Use the exact source table, filter, and UTC window from this capture; preserve raw attributes,
events/links, and nanosecond timestamps rather than only diagnostic summaries. Save a new capture
inside the workspace, never over this verified archive or in OS temporary directories. Parse the
complete result and verify expected/unique counts: an HTTP 200 can still contain a late streaming
query error. Do not treat successful transport or a UI page count as proof of a complete export.

Recommendation: use the UI for small selected-row investigations, direct ClickHouse JSONEachRow
for lossless bulk capture, and MCP for discovery/aggregation or verified bounded retrieval. If
MCP-only bulk capture must become routine, an upstream export-to-file/resource capability would
avoid putting full payloads in context; no such change or upstream issue was created here.
