# Event streams and queues

These destinations receive every change as an event: Kafka, the cloud queues and streams, and
HTTP. All of them run on Debezium; Kafka also runs on Flink CDC. See
[Destinations](destinations.md) for what every destination shares.

Each section below has the same rows, so you can compare destinations. "Reading it back" means
the pipeline's **Events** tab and the [delivery canary](operate.md#delivery-canary). "Drop
destinations" is the choice made when you [delete a pipeline](operate.md#delete-a-pipeline).
Wherever a row says "formats as for Kafka", the choices are the ones listed under Kafka.

## Kafka

One topic per table, keyed by the row key so each row keeps its order.

| | Kafka |
|---|---|
| Engine | Debezium (default) or Flink CDC (see [Kafka on Flink CDC](#kafka-on-flink-cdc)) |
| What you get | One topic per table, named `cdc.{schema}.{table}` by default, keyed by the row key |
| Connection | Presets: **Self-hosted Kafka**, **Confluent Cloud**, **Amazon MSK** (SASL/SCRAM) and **Redpanda**. Bootstrap servers; security protocol (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`); SASL mechanism, user and password; a private CA in Expert mode |
| Pipeline settings and formats | Destination step: JSON (default), Avro or Protobuf with a schema registry URL (Apicurio or Confluent), or CloudEvents. The Transform step chooses the event shape |
| Preflight checks | Reads the broker list, reaches every broker, fetches metadata and signs in. Checks that the cluster has as many brokers as the replication factor, and that the user may create topics (the fix gives the `kafka-acls` command) |
| Reading it back | The Events tab and the delivery canary read the newest records of each topic |
| Deleting a pipeline | With "drop destinations", deletes its topics |

Topics are created at deploy with the connection's partitions and replication factor (1 and 1
unless set in Expert mode). When *Create topics automatically* is off, the topics must already
exist.

## Kafka on Flink CDC

On Flink CDC, records keep a Debezium-style change envelope, but in a shorter form than
Debezium's own.

| | Kafka on Flink CDC |
|---|---|
| Engine | Flink CDC. Debezium writes Kafka by default (see [Kafka](#kafka)) |
| What you get | Each table goes to the topic its naming pattern gives. Records are keyed by the primary key (`{"TableId": …, "id": …}`) and spread over partitions by it |
| Connection | The same Kafka connection; topics are created at deploy with the connection's partitions and replication factor |
| Pipeline settings and formats | Only JSON without the schema. Choosing Flink CDC sets it; saving another format is refused |
| Transform step | *Keep events*, *Fields*, the *Message key* (the key columns the record key carries, which also spread events over partitions), and an **Events** panel with the topic, the key and the value |

The record value is Flink CDC's Debezium-style JSON: `before`, `after` and `op`, with a short
`source` and no schema or `ts_ms`.

- Consumers written for Debezium's records need to accept that shape. Switching a pipeline's
  engine changes it.
- The Transform step's event shape (Row after change, delete handling, added fields) doesn't apply
  here: it is hidden on Flink CDC, and records are always the change envelope above.
- *Keep deleted rows* sends a delete as an insert event whose `after` carries `__op = -D`, with no
  delete event.

## Google Pub/Sub

One topic per table in the connection's Google Cloud project.

| | Google Pub/Sub |
|---|---|
| Engine | Debezium |
| What you get | One topic per table, created at deploy unless turned off |
| Connection | A service-account key, or leave it empty to use the runtime's own identity (Workload Identity on GKE). The account needs `roles/pubsub.editor`. An emulator host (Expert) is for tests |
| Pipeline settings and formats | *Keep per-row order* (default): each message carries the row key as its ordering key. Formats as for Kafka |
| Preflight checks | Signs in and lists the project's topics; gives the `gcloud` command when it can't |
| Reading it back | Nothing: the Events tab has nothing to show and there is no delivery canary |
| Deleting a pipeline | "Drop destinations" deletes the topics |

- To use the ordering key, turn on message ordering on your subscriptions.
- BeziSync can't read Pub/Sub without taking messages from your subscriptions, which is why it
  reads nothing back.

## AWS Kinesis

One Kinesis data stream per table in the connection's region.

| | AWS Kinesis |
|---|---|
| Engine | Debezium |
| What you get | One stream per table, created at deploy (one shard, more in Expert mode) unless turned off. Records are keyed by the row key |
| Connection | An access key or the runtime's IAM role. A custom endpoint (Expert) points at a VPC endpoint or an emulator |
| Pipeline settings and formats | Formats as for Kafka |
| Preflight checks | Reaches the endpoint, signs in and says what is missing |
| Reading it back | The Events tab and the delivery canary read the newest records |
| Deleting a pipeline | "Drop destinations" deletes the streams |

The role needs `kinesis:ListStreams`, `kinesis:DescribeStreamSummary`, `kinesis:CreateStream`,
`kinesis:PutRecord` and `kinesis:PutRecords`.

## Amazon SQS and Amazon SNS

SQS sends every table to one queue; SNS gives each table its own topic.

| | Amazon SQS | Amazon SNS |
|---|---|---|
| Engine | Debezium | Debezium |
| What you get | Every table of the pipeline goes to the connection's queue, one message per change, so it suits low-volume tables. Consumers tell tables apart by `source.table` (or `__table` with the "row after change" shape) | One topic per table (`cdc_{schema}_{table}`). Names ending in `.fifo` make FIFO topics, created with content-based deduplication and ordered per row key. Topics are created on deploy unless turned off |
| Connection | Keys or the runtime's IAM role | Keys or the runtime's IAM role |
| Pipeline settings and formats | JSON or CloudEvents (SQS takes text). Deletes as `__deleted` rows, or dropped | JSON or CloudEvents (SNS takes text). Deletes as `__deleted` rows, or dropped |
| Reading it back | The Events tab peeks at messages still waiting. No delivery canary, as consumers may take it first | Through a small queue of BeziSync's own subscribed to each topic (`bz-preview-<topic>`, 15-minute retention), for the Events tab and the delivery canary |
| Deleting a pipeline | — | Removes the preview queues, and the topics with "drop destinations" |
| Limits | A FIFO queue needs content-based deduplication (Debezium sends no deduplication ID). Messages over 256 KiB (by default) stop the pipeline | Messages over 256 KiB stop the pipeline |

- SQS and SNS refuse empty "tombstone" messages, so BeziSync turns those off.
- SQS peeking leaves messages available to your consumers, but each look counts as a receive. It
  is off when the queue's dead-letter redrive allows fewer than 10 receives.
- SNS keeps nothing to read back, which is why BeziSync adds its own preview queues.
- BeziSync keeps the SQS connector from logging message bodies.

## Azure Event Hubs

Every table of the pipeline goes to the connection's event hub.

| | Azure Event Hubs |
|---|---|
| Engine | Debezium |
| What you get | Events in the connection's event hub (it must exist), partitioned by the row key so each row keeps its order. Consumers tell tables apart by `source.table` (or `__table`) |
| Connection | A namespace connection string (shared access policy with **Send**), without `EntityPath`. Optionally a second connection string for reading |
| Preflight checks | Warns when the connection string cannot listen |
| Reading it back | The Events tab and the delivery canary read the last events of each partition, which needs **Listen**: add a second connection string for reading when the first cannot listen |
| Limits | Events over the tier's limit (1 MB on Standard) stop the pipeline. Entra ID sign-in is not available: Debezium's sink supports connection strings only |

## Apache Pulsar

One persistent topic per table in the connection's tenant and namespace, keyed by the row key.

| | Apache Pulsar |
|---|---|
| Engine | Debezium |
| What you get | One persistent topic per table, keyed by the row key; missing topics are created on deploy |
| Connection | The **service URL** the connector produces to (`pulsar://…:6650`, or `pulsar+ssl://…:6651` with an optional private CA), the **admin URL** (`http(s)://…:8080`), and a token to sign in (JWT, or a StreamNative API key) |
| Admin URL | BeziSync uses it to check the namespace, create missing topics on deploy, and read the newest messages |
| Reading it back | The Events tab and the delivery canary read the newest messages through the admin URL (`examinemessage`: no subscription, your consumers are untouched) |
| Limits | A send that does not complete within 30 seconds fails the batch, which Debezium retries, rather than being lost |

## Redis Streams

Every change is appended (`XADD`) to one stream per table.

| | Redis Streams |
|---|---|
| Engine | Debezium |
| What you get | One stream per table; each entry has the row key as its first field and the event as its value |
| Connection | Presets: **Redis** and **AWS ElastiCache**. Host, port, an ACL user (Redis 6+), password and TLS |
| Pipeline settings and formats | *Trim streams to N entries* (Expert, an approximate `MAXLEN`) caps the streams |
| Preflight checks | Reaches Redis, signs in, checks that the user may write streams, and warns when Redis uses more than 80% of `maxmemory` |
| Reading it back | The Events tab and the delivery canary read the newest entries |
| Deleting a pipeline | "Drop destinations" deletes the streams |
| Limits | Streams grow with every change unless you trim them |

With TLS, preflight only signs in once Redis shows a certificate for the host name you entered,
signed by a trusted authority. Connect by a name the certificate lists, not by IP address.
Debezium Server itself checks the chain but not the name.

## NATS JetStream

One subject per table in a persistent JetStream stream.

| | NATS JetStream |
|---|---|
| Engine | Debezium |
| What you get | One subject per table in a stream (`CDC` by default). BeziSync creates the stream at deploy with the pipeline's subjects, or adds missing subjects to a stream that exists |
| Connection | The server URL, and an optional user and password |
| Preflight checks | Reaches the server and checks that JetStream is enabled |
| Reading it back | The Events tab and the delivery canary read the newest messages |
| Deleting a pipeline | Leaves the stream in place |
| Limits | BeziSync never changes the stream's limits or retention, so set those on the server |

## HTTP

Every change event is sent as a `POST` to the connection's URL, one request per event.

| | HTTP |
|---|---|
| Engine | Debezium |
| What you get | One `POST` per change event |
| Connection | The URL |
| Pipeline settings and formats | JSON by default; formats as for Kafka. Timeout of 10 seconds; 5 retries 1 second apart. Expert mode changes all three |
| Preflight checks | Reaches the endpoint and checks that it answers HTTP. Any status other than 2xx is a warning, as deliveries may fail |
| Reading it back | Nothing: BeziSync keeps nothing to read back, so the Events tab is empty and there is no delivery canary |

A request that fails or takes longer than the timeout is retried.

---

[All documentation](../README.md)
