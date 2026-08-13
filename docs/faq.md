# This thing uses eventual consistency, isn't it broken?

deadline.
By default, consumer metadata reads are eventually consistent. See [Design](design.md) and
[Operations](operations.md) for details. This is an intentional cost/consistency tradeoff: the
default two-second visibility delay is a best-effort margin for ordinary replica lag, not a DynamoDB
correctness guarantee. It is part of a broader bounded availability horizon that also includes the
broker's metadata-publication deadline.

Set `strongly_consistent_metadata_reads` or the runtime flag
`blob_stream_consumer_strong_metadata_reads` to use strongly consistent metadata queries. This
removes read-replica staleness, ignores the configured visibility delay, and approximately doubles
metadata-query RRUs. It does not make paginated scans atomic. Enable broker
`fenced_metadata_writes` as well when stale producer publication must be rejected: it conditions
metadata publication on the active lease session and epoch, requires transactional DynamoDB IAM
permissions, and defaults off. The durable holder ID, lease epoch, and session ID are required in
every producer lease row regardless of this setting. Fenced publication has a significant write-cost
impact: each metadata publication becomes a DynamoDB transaction containing the segment metadata
write plus a lease condition check for every partition in the flush, and DynamoDB charges
transactional writes and reads at twice the normal capacity-unit rate.

# Why haven't you implemented compaction?

The intended use case is single consumer groups that generally read with low latency. We do not
expect recovery to be common. Compaction would add ongoing write, storage, and operational cost to
reduce the cost of replay and delayed-consumer recovery. We have not found that tradeoff worthwhile
for the primary workload, but may reconsider it if long-retention recovery becomes a material cost.

# Will you add libraries in X language?

It should be relatively easy to add a C interop shim to the existing libraries which would allow
wrapping in almost any language. We have no plans on doing this work but reach out if you are
interested in helping with this.

# Will you improve the system to make it more performant and cost effective for multiple concurrent consumer groups?

We have no plans at the current time. For compaction specifically, see the question above. For
general improvements such as consuming through a broker API, attempting to consolidate S3 reads
at the broker level, etc. this is technically possible but adds complexity. We may consider this
in the future depending on need and interest.

# Are you going to implement the Kafka API?

No.

# Are you going to add other Kafka like features such as transactions?

There are no plans at the current time but there is nothing in the current system that would
outright prevent this if the need arises.

# Are you going to support other metadata and blob stores?

We have no plans currently but it should be relatively easy to do this if there is interest. The
main requirement is that both the metadata and blob store must support out of band TTL for records
and blobs. Doing internal cleanup adds a lot of complexity (and cost) and we would prefer to avoid
that.

# Will configuration become centrally managed?

This is not currently planned but it is something we would like to do in the future. Brokers,
producers, and consumers still receive their configuration independently, and the shared topic shape
remains a deployment contract. Broker and consumer runtime feature flags can override selected
operational settings, but they are not a configuration-distribution API. See [Infrastructure
setup](infrastructure.md) for the configuration contract.

# Will you provide broker docker images?

There are no plans currently. The broker is a single binary so it should be easy to compile it and
pack it in your own image.
