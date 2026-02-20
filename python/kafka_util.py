#!/usr/bin/env python3
"""Small utility for local Kafka topic/admin/produce/consume workflows.

Examples:
  python3 examples/python/kafka_util.py create-topic --topic test-topic
  python3 examples/python/kafka_util.py produce --topic test-topic --message "hello"
  python3 examples/python/kafka_util.py consume --topic test-topic --group-id demo --from-beginning
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Optional

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kafka utility using confluent-kafka-python")
    parser.add_argument(
        "--bootstrap-servers",
        default="127.0.0.1:9092",
        help="Kafka bootstrap servers (default: 127.0.0.1:9092)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create-topic", help="Create a topic")
    create.add_argument("--topic", default="test-topic", help="Topic name")
    create.add_argument("--partitions", type=int, default=1, help="Partition count")
    create.add_argument("--replication-factor", type=int, default=1, help="Replication factor")

    produce = sub.add_parser("produce", help="Produce one or more messages")
    produce.add_argument("--topic", default="test-topic", help="Topic name")
    produce.add_argument("--message", help="Single message to send")
    produce.add_argument("--count", type=int, default=1, help="Number of generated messages")
    produce.add_argument("--key", help="Optional message key")
    produce.add_argument(
        "--partitioner",
        choices=[
            "consistent", # for keys and sending messages to the same partitions based on CRC32 hash
            "consistent_random", # consistent hybrid (uses random for null keys)
            "murmur2", # uses murmur2 hash. higher performance, superior speed and lower collision rates
            "murmur2_random", # default for java
            "fnv1a", # default for go sarama
            "fnv1a_random",
            "random", # allocates randomly regardless of key. however still effected by sticker partitioner linger
        ],
        help="librdkafka partitioner (overrides default)",
    )
    produce.add_argument(
        "--sticky-linger-ms",
        type=int,
        help="Sticky partitioner linger in ms (keyless messages)",
    )

    consume = sub.add_parser("consume", help="Consume messages")
    consume.add_argument("--topic", default="test-topic", help="Topic name")
    consume.add_argument("--group-id", default="demo-group", help="Consumer group id")
    consume.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Total seconds to wait (-1 = no timeout)",
    )
    consume.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 = unlimited)")
    consume.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from earliest offset (default reads latest)",
    )

    return parser


def create_topic(bootstrap_servers: str, topic: str, partitions: int, replication_factor: int) -> int:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)])

    fut = futures[topic]
    try:
        fut.result(timeout=15)
        print(f"Created topic '{topic}'")
        return 0
    except Exception as exc:  # pragma: no cover - depends on broker state
        text = str(exc)
        if "TOPIC_ALREADY_EXISTS" in text:
            print(f"Topic '{topic}' already exists")
            return 0
        print(f"Failed to create topic '{topic}': {exc}", file=sys.stderr)
        return 1


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)
    else:
        print(
            f"Delivered to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def produce_messages(
    bootstrap_servers: str,
    topic: str,
    message: Optional[str],
    count: int,
    key: Optional[str],
    partitioner: Optional[str],
    sticky_linger_ms: Optional[int],
) -> int:
    conf = {"bootstrap.servers": bootstrap_servers}
    if partitioner:
        conf["partitioner"] = partitioner
    if sticky_linger_ms is not None:
        conf["sticky.partitioning.linger.ms"] = sticky_linger_ms
    producer = Producer(conf)

    if message is not None:
        payloads = [message] * max(count, 1)
    else:
        ts = int(time.time())
        payloads = [f"message-{i}-ts-{ts}" for i in range(count)]

    for payload in payloads:
        producer.produce(topic=topic, value=payload.encode("utf-8"), key=key, callback=delivery_report)

    producer.flush(10)
    return 0


def consume_messages(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    timeout_s: float,
    max_messages: int,
    from_beginning: bool,
) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )

    seen = 0
    deadline = None if timeout_s < 0 else time.time() + timeout_s

    try:
        consumer.subscribe([topic])
        while deadline is None or time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue

            key = msg.key().decode("utf-8") if msg.key() else ""
            value = msg.value().decode("utf-8") if msg.value() else ""
            print(
                f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key} value={value}"
            )
            seen += 1
            if max_messages > 0 and seen >= max_messages:
                break
    finally:
        consumer.close()

    if seen == 0 and deadline is not None:
        print("No messages consumed before timeout", file=sys.stderr)
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "create-topic":
        return create_topic(args.bootstrap_servers, args.topic, args.partitions, args.replication_factor)

    if args.command == "produce":
        return produce_messages(
            args.bootstrap_servers,
            args.topic,
            args.message,
            args.count,
            args.key,
            args.partitioner,
            args.sticky_linger_ms,
        )

    if args.command == "consume":
        return consume_messages(
            args.bootstrap_servers,
            args.topic,
            args.group_id,
            args.timeout,
            args.max_messages,
            args.from_beginning,
        )

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
