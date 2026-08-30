# Local Blob Stream Walkthrough

Run two brokers, two consumers, and an interactive text producer on one machine. The brokers use
static discovery; Docker Compose supplies only the S3-compatible and DynamoDB-compatible backing
services. The producer and consumers are normal host processes, so their logs and configuration
remain visible while you experiment.

## Prerequisites

- Docker Compose
- Rust with the Blob Stream workspace dependencies available
- A local clock that is reasonably synchronized; Blob Stream consumers rely on bounded
  broker-to-consumer clock skew

The local AWS emulators accept placeholder credentials. Export them once in every terminal that
runs a broker or consumer:

```bash
export AWS_ACCESS_KEY_ID=local
export AWS_SECRET_ACCESS_KEY=local
export AWS_REGION=us-east-1
```

## Start The Local Services

Run these commands from this directory. The bootstrap container waits for both services, then
creates the local S3 bucket and all four DynamoDB tables that Blob Stream needs. It is idempotent,
so rerunning it is safe while the Compose project is running.

```bash
docker compose up -d dynamodb localstack
docker compose run --rm bootstrap
```

## Start Two Brokers

From the `blob-stream` repository root, start each broker in a separate terminal. The two YAML
files have the same topic definition and static node list. They differ only in their stable node
identity and local port.

```bash
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
cargo run -p blob-stream-broker -- --config examples/local-e2e/config/broker-1.yaml
```

```bash
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
cargo run -p blob-stream-broker -- --config examples/local-e2e/config/broker-2.yaml
```

Both logs should report that the broker is listening on `127.0.0.1:8080` or
`127.0.0.1:8081`. Static discovery is intentionally explicit here: every participating process
has the same two addresses, and no Kubernetes or cluster manager is involved.

## Start A Consumer Group

Open two more terminals at the `blob-stream` repository root. These consumers share the default
`local-demo` group and have distinct stable member IDs. Blob Stream assigns its four virtual
partitions across members of the same group, so a given message appears once in one consumer's
output under steady state.

```bash
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
cargo run -p blob-stream-local-e2e --bin consumer -- --member-id consumer-1
```

```bash
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
cargo run -p blob-stream-local-e2e --bin consumer -- --member-id consumer-2
```

Each process prints its member ID, partition, and offset. The consumer commits the offset only
after printing the message. Press Ctrl-C in one consumer terminal, then send more messages: after
the short rebalance interval, the remaining member takes over its partitions.

## Publish Text Messages

In a fifth terminal at the `blob-stream` repository root, start the producer:

```bash
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
cargo run -p blob-stream-local-e2e --bin producer
```

Enter a few lines:

```text
first local message
the stream is alive
one more for a different partition
```

The producer prints the selected virtual partition and acknowledgement attempt count. Within a few
seconds, one consumer prints each message with the committed offset. The producer rotates demo
keys to exercise more than one partition. Use `--key example-key` to intentionally publish every
line with one fixed key.

## Observe Fan-Out

A separate group receives its own copy of the topic rather than sharing work with `local-demo`.
Start a consumer with a different group ID, then publish more text:

```bash
cargo run -p blob-stream-local-e2e --bin consumer -- \
  --member-id audit-1 --group-id local-audit
```

`audit-1` receives every retained message assigned to its group, independently of the default
group's committed offsets. Consumer delivery is at least once: real applications must make their
processing idempotent even though this small demo normally prints each message once.

## Stop And Clean Up

Stop the producer with Ctrl-D and consumers/brokers with Ctrl-C. From this directory, remove the
ephemeral containers and local emulated state:

```bash
docker compose down -v
```

## Troubleshooting

- `address already in use`: stop another local Blob Stream run or change the conflicting broker,
  DynamoDB, or LocalStack port consistently in every configuration.
- Bootstrap repeats `waiting for DynamoDB Local and LocalStack`: inspect `docker compose logs
  dynamodb localstack`; the emulators must be reachable before tables and buckets are created.
- A consumer has no output: confirm both broker logs show that they are listening, then wait for
  the broker flush and consumer metadata-visibility delay after publishing a line.
- A restarted consumer prints an earlier message: this is permitted at-least-once delivery when a
  committed offset was not durable before the process stopped.

The `config/` files show the complete static topology. The `app/` sources are intentionally
commented to connect producer acknowledgements, consumer commits, and revocations to the behavior
visible in the terminals.
