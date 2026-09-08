# What data loss conditions are tolerated?

By default, consumer metadata reads are strongly consistent. See [Design](design.md) and
[Operations](operations.md) for details. To opt into eventually consistent reads in order to reduce
cost, set `eventual_metadata_reads`; its optional `visibility_delay` defaults to two seconds. This
delay is a best-effort margin for ordinary replica lag, not a DynamoDB correctness guarantee, and is
part of a broader bounded availability horizon that also includes the broker's metadata-publication
deadline. As of this writing, metadata reads are coalesced via the brokers and in practice the
marginal cost increase of strongly consistent reads is low. We do not recommend changing this
setting and might even remove the eventually consistent option in the future as it makes the code
substantially more complicated in various places.

Strong reads remove read-replica staleness and approximately double metadata-query RRUs. They do not
make paginated scans atomic. Enable broker `fenced_metadata_writes` as well when stale producer
publication must be rejected: it conditions metadata publication on the active lease session and
epoch, requires transactional DynamoDB IAM permissions, and defaults off. Fenced publication has a
significant write-cost impact: each metadata publication becomes a DynamoDB transaction containing
the segment metadata write plus a lease condition check for every partition in the flush, and
DynamoDB charges transactional writes and reads at twice the normal capacity-unit rate. The default
configuration accepts the stalled broker write loss condition because the mitigation cost is very
high compared to the potential data loss it prevents in real world usage.

# What time source is required to operate blob-stream correctly?

All brokers and consumers for a topic need a shared, continuously monitored time service with a
bounded pairwise clock offset. Configure each consumer's `max_clock_skew` to that bound; it
defaults to 10 ms only when unset. It is safe to retain the default only when the deployment can
demonstrate and alert on a broker-to-consumer offset no greater than 10 ms; ordinary best-effort NTP
synchronization is not itself an adequate guarantee.

Use an infrastructure time source with a documented uncertainty or measure the actual offset and
include its measurement, propagation, and alerting allowance in `max_clock_skew`. Keep brokers
and consumers in the same bounded-time domain. When the measured envelope exceeds the configured
value, raise the consumer setting before operating the deployment. A consumer clock that leads a
broker beyond this bound can omit a segment from the Fast or checkpoint-recovery floor; a leading
broker is conservative but increases availability latency.

# How do partitions relate to virtual partitions?

Blob-stream was designed for zero cross-AZ traffic. Every AZ is assigned a "writer domain." Within
that domain every topic has N partitions. In aggregate across all AZs, if there are N writer domains
and M topics, there are N * M virtual partitions in total.

From the consumer perspective, *all* virtual partitions are balanced across all AZs. This is subtly
different from Kafka. In Kafka, when using a producer controlled partition hash, the same hash is
going to wind up in the same global partition regardless of which AZ it is written from. With
blob-stream, the same hash across 3 AZs is going to wind up in 3 different virtual partitions which
may or may not get assigned to the same consumer.

Use cases that require strict ordering across AZs for a single partition hash cannot currently be
satisfied by blob-stream. In the future we may consider two possible improvements to satisfy this:

1. Add a consumer assignment mode which will force all virtual partitions for a given partition to
   be assigned to the same consumer. This would send all same-hashed data to the same consumer, but
   there would not be strict ordering guarantees. This is still likely good enough for many
   workflows.
2. It is technically possible to create a meta-iterator that merges all virtual partitions for a
   partition and defines a strict global order based on (snowflake ID, [per partition sequence
   range]). This is more complicated but we can consider this in the future if there is demand.

# Why haven't you implemented compaction?

The intended use case is single consumer groups that generally read with low latency. We do not
expect recovery to be common. Compaction would add ongoing write, storage, and operational cost to
reduce the cost of replay and delayed-consumer recovery. We have not found that tradeoff worthwhile
for the primary workload, but may reconsider it if long-retention recovery becomes a material cost.

# Will you add libraries in X language?

It should be relatively easy to add a C interop shim to the existing libraries which would allow
wrapping in almost any language. We have no plans on doing this work but reach out if you are
interested in helping with this.

# Will you improve the system to make it more performant and cost effective for multiple concurrent consumer groups or very large groups?

For compaction specifically, see the question above. Various efficiency improvements have already
been implemented including reading metadata and blobs via broker caches as well as writing multiple
topics into a single blob. We are always looking for ways to improve the system and welcome
contributions.

# Are you going to implement the Kafka API?

No.

# Are you going to add other Kafka like features such as transactions, authn/authz, and whatever else?

There are no plans at the current time but there is nothing in the current system that would
outright prevent this if the need arises.

# Are you going to support other metadata and blob stores?

We have no plans currently but it should be relatively easy to do this if there is interest. The
main requirement is that both the metadata and blob store must support out of band TTL for records
and blobs. Doing internal cleanup adds a lot of complexity (and cost) and we would prefer to avoid
that. The metadata store must also support efficient range key scans.

# Will configuration become centrally managed?

This is not currently planned but it is something we would like to do in the future. Brokers,
producers, and consumers still receive their configuration independently, and the shared topic shape
remains a deployment contract. Broker, producer, and consumer runtime feature flags override
selected local operational settings. See [Infrastructure setup](infrastructure.md) for the
configuration contract.

# Will you provide broker docker images?

There are no plans currently. The broker is a single binary so it should be easy to compile it and
pack it in your own image.
