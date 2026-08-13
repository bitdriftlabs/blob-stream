<img width="2926" height="1606" alt="blob-stream" src="https://github.com/user-attachments/assets/18979bb4-3c66-4222-a359-9d98f741a021" />

# blob-stream

`blob-stream` is a Kafka-like streaming system for high-throughput telemetry workloads that
prioritizes storage cost over ultra-low latency. Brokers accept producer batches, persist compressed
payload segments in blob storage, and publish metadata indexes for pull-based consumers.

Delivery is at least once. Producers can retry ambiguous requests and consumers can replay records
after interrupted commits or rebalances, so applications must tolerate duplicates.

## System At A Glance

- Producers hash record keys into logical partitions and map them to virtual partitions by
  `writer_id`.
- Producers send `ProduceBatches` requests to the locally discovered broker selected by rendezvous
  hashing.
- Brokers hold producer-partition leases, reserve monotonic sequence ranges, and flush compressed
  segment blobs with metadata indexes.
- Consumers scan metadata windows, range-read selected blob batches, and commit progress per
  virtual partition.

## Project Layout

- `blob-stream-broker/`: broker binary and write path
- `blob-stream-producer/`: producer client library
- `blob-stream-consumer/`: consumer iterator and coordinator library
- `blob-stream-broker-discovery/`: static and Kubernetes service discovery
- `blob-stream-blob-store/`: blob store trait with in-memory and S3 backends
- `blob-stream-metadata-store/`: metadata and lease stores with in-memory and DynamoDB backends
- `blob-stream-proto/`: protobuf API and configuration schemas
- `blob-stream-types/`: shared wire and storage types
- `blob-stream-integration-tests/`: end-to-end and deterministic fault-injection tests

## Documentation

### Deploy And Operate

- [System design and correctness contract](docs/design.md)
- [Infrastructure setup](docs/infrastructure.md): configuration, DynamoDB, S3, IAM, and Kubernetes
  discovery RBAC
- [Operations guide](docs/operations.md): runtime controls, metrics, diagnostic endpoints, and
  troubleshooting
- [FAQ](docs/faq.md)
- [Cost analysis](docs/cost-analysis.md)

### Contribute And Verify

- [Development guide](DEVELOPMENT.md): standalone Cargo and monorepo Bazel workflows, local
  dependencies, stress runner, and Rust API documentation
- [Integration-test audit](plans/TEST_AUDIT.md): deterministic test requirements and hardening work
- [TLA+ model](tla/README.md): finite model of lease, sequence, publication, and cursor safety

The protobuf configuration schema in
[blobstream/v1/config.proto](blob-stream-proto/proto/blobstream/v1/config.proto) is the canonical
field-level reference for broker, producer, and consumer configuration.
