# Blob Stream Dashboard References

These CUE files are reference definitions for observing Blob Stream. They are copied from the
bitdrift's internal dashboard configuration repository and are not standalone dashboard packages.

- `blob-stream.cue` defines the broker dashboard, including gRPC, write-path, storage, cache,
  DynamoDB, Kubernetes, and estimated-cost panels.
- `lib/blob-stream.cue` provides reusable `#producer_row`, `#consumer_row`, and
  `#consumer_reader_row` templates for application dashboards.

Use the rows, panels, and queries in these dashboards as inspiration for building your own!

See the [metrics reference](../../docs/metrics.md) for the metric inventory.
