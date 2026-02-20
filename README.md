# kafka-sandbox
Sandbox for experimenting with and testing librdkafka

## Prerequisites
* Docker Desktop or Colima
* Python

## Python Usage Guide
1. Create a docker container (make sure you have Colima or Docker Desktop running!):

```docker run -d --name broker -p 9092:9092 apache/kafka:latest```

2. Create a test topic using this command:

```python3 python/kafka_util.py create-topic --topic test-topic```

3. Produce messages with this command:

```python3 python/kafka_util.py produce --topic test-topic --message "single message"```

4. Consume messages with this command (a -1 timeout means infinite consumption):

```python3 python/kafka_util.py consume --topic test-topic --timeout -1```