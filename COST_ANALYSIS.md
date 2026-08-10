# Cost Analysis

The executable model lives in `cost_analysis.py`.

Run it from repo root:

```bash
/opt/homebrew/bin/python3 cost_analysis.py
```

All formula explanations and assumptions are documented as comments directly in the script.
Edit the `DEFAULTS` block in `cost_analysis.py` with your workload and regional pricing.

`strongly_consistent_metadata_reads` maps to the consumer configuration of the same name (or its
runtime override, `blob_stream_consumer_strong_metadata_reads`). Leave it false for the default
eventual-read mode; set it true to model the next-pass strong mode. The model charges eventual
metadata query pages at 0.5 RRU and strong pages at 1 RRU per 4 KiB. Consumer-group coordination
queries are already modeled separately as strongly consistent and do not change with this setting.
