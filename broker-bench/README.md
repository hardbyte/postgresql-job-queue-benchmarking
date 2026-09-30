# Kafka-compatible broker benchmark

This is a separate comparison from the job-queue and CDC rankings. It runs
one Kafka protocol workload against Apache Kafka 4.3.1 and kafgres 0.2.0
at source commit `00168534b8899300896b5ca1582a6ecca3de81d1`, with kafgres's
segment and table storage engines as distinct arms. Kafgres 0.2.0 builds
only against PostgreSQL 16 and earlier: it uses `ReorderBufferTupleBuf`,
`TupleDescData.attrs` and the pre-17 `CreateWaitEventSet` signature, so it
cannot share the job-queue suite's PostgreSQL 18 server. Results from the two
suites must not be merged into one ranking.

## Run a smoke test

```sh
git submodule update --init --recursive
docker compose -f broker-bench/compose.yml build kafgres-segment
docker compose -f broker-bench/compose.yml up -d --wait
uv run broker-bench --messages 100 --payload-bytes 256 --partitions 3 --timeout-s 90
docker compose -f broker-bench/compose.yml down -v
```

The runner creates a unique topic and consumer group for each arm. It uses
`confluent-kafka` (librdkafka) 2.15.1 for all three, with `acks=all`, an
idempotent producer, 5 ms linger, three partitions, and synchronous consumer
offset commits every 100 messages. It validates each sequence number and
reports acknowledged, consumed, duplicate, invalid and missing counts.
Output goes to `results/custom-broker-*/summary.json`. Any invalid arm exits
nonzero. For a small offered-load check, pass `--rate 200 --messages 1000`.
For a brief consumer-work check, add `--work-ms 1`.

The Compose setup caps each server at four CPUs and 8 GiB. Each kafgres arm
gets a fresh PostgreSQL data directory. Segment mode uses upstream's
`fsync_before_ack=off` default; its `acks=all` response therefore does not
promise power-loss durability. The table engine commits through PostgreSQL
WAL. Kafka's single-replica `acks=all` also does not imply an fsync before
acknowledgment. The runner records the live kafgres settings and PostgreSQL
version with each result.

## Design for publication runs

The finite run above is a correctness and throughput smoke test. A full
comparison should retain its common client and protocol contract, and add
these controlled phases:

1. Warm up each fresh broker, then run several fixed offered rates below and
   above saturation. Repeat each cell on fresh storage and randomize arm
   order. Record offered, acknowledged and consumed rates per time window,
   along with pickup and ack p50/p95/p99, backlog and errors.
2. Run payload size, partition count, producer count and consumer-group count
   as separate axes. Use unique topics, identical retention and cleanup
   settings, and report client CPU saturation before attributing a ceiling
   to a broker.
3. Measure co-resident PostgreSQL work with `pgbench` before, during and
   after broker load. Charge Kafka its broker CPU/RSS/disk and kafgres its
   PostgreSQL CPU/RSS/WAL/segment disk. Report WAL growth, relation/segment
   size, checkpoints and fsyncs alongside throughput.
4. Run acknowledged-message crash and restart checks, including a power-loss
   durability profile with `kafgres.fsync_before_ack=on`. Keep that profile
   separate from the default throughput profile. Test failover and segment
   archive/restore separately; upstream notes that a base backup alone is
   insufficient for the segment log.

The common Kafka API can measure broker transport. Kafgres's atomic SQL
`kafgres_produce()` and CDC mapping are product-specific capabilities and
need their own correctness workloads, not a Kafka throughput row. The table
and segment engines also have different transaction semantics; report them
as separate implementations.

## Versions and provenance

`broker-bench/vendor/kafgres` is a submodule pinned to the source commit
above. Kafka uses `apache/kafka:4.3.1`, the latest stable Kafka release at
the time of this update. The kafgres image is built from the pinned source
inside `postgres:16.15-bookworm`, so the extension links against the exact
server headers it runs with; override `PG_IMAGE` to test another server;
the runner queries its extension and PostgreSQL versions at runtime. Save the
Compose image digests, host CPU model, Docker version and kernel with any
published run.
