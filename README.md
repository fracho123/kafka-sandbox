# kafka-sandbox
Sandbox for experimenting with and testing librdkafka

## Prerequisites
* Docker Desktop or Colima
* Python 3
* `pip install confluent-kafka`

## Quickstart (Single or Multi Broker)
This repo supports a single broker (via `docker run`) or a 3-broker KRaft cluster (via `docker compose`).

1. Start Kafka:
Single broker:
```bash
docker run -d --name broker -p 9092:9092 apache/kafka:latest
```
Multi broker (compose):
```bash
docker rm -f broker
docker compose up -d
```

2. Create a test topic:
Single broker (replication must be `1`):
```bash
python3 python/kafka_util.py create-topic --topic test-topic --partitions 6 --replication-factor 1
```
Multi broker (replication `3`):
```bash
python3 python/kafka_util.py create-topic --topic test-topic-6-3 --partitions 6 --replication-factor 3
```

3. Produce messages:
Single broker:
```bash
python3 python/kafka_util.py produce --topic test-topic --message "single message"
```
Multi broker (bootstrap from any broker):
```bash
python3 python/kafka_util.py produce --topic test-topic-6-3 --message "hello" \
  --bootstrap-servers 127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096
```

4. Consume messages (a `-1` timeout means infinite consumption):
Single broker:
```bash
python3 python/kafka_util.py consume --topic test-topic --timeout -1
```
Multi broker:
```bash
python3 python/kafka_util.py consume --topic test-topic-6-3 --group-id demo-group \
  --bootstrap-servers 127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096
```

## `python/kafka_util.py` Reference
This utility wraps common admin/produce/consume workflows for local testing.

### Global flags
* `--bootstrap-servers` (default: `127.0.0.1:9092`)

### `create-topic`
* `--topic` (default: `test-topic`)
* `--partitions` (default: `1`)
* `--replication-factor` (default: `1`)

Example:
```bash
python3 python/kafka_util.py create-topic --topic test-topic --partitions 6 --replication-factor 1
```

### `produce`
* `--topic` (default: `test-topic`)
* `--message` (optional single message; if omitted, generated messages are used)
* `--count` (default: `1`, number of messages to send)
* `--key` (optional message key)
* `--partitioner` (librdkafka partitioner override)
  * `consistent`
  * `consistent_random`
  * `murmur2`
  * `murmur2_random`
  * `fnv1a`
  * `fnv1a_random`
  * `random`
* `--sticky-linger-ms` (sticky partitioner linger in ms for keyless messages)

Examples:
```bash
python3 python/kafka_util.py produce --topic test-topic --message "hello"
```

```bash
python3 python/kafka_util.py produce --topic test-topic --count 100 --partitioner murmur2
```

### `consume`
* `--topic` (default: `test-topic`)
* `--group-id` (default: `demo-group`)
* `--timeout` (default: `30.0`; `-1` means no timeout)
* `--max-messages` (default: `0`, unlimited)
* `--from-beginning` (read from earliest offset; default is latest)

Examples:
```bash
python3 python/kafka_util.py consume --topic test-topic --group-id demo-group --from-beginning
```

```bash
python3 python/kafka_util.py consume --topic test-topic --timeout -1
```
