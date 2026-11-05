"""Small shared helpers; configuration is immutable within a run."""
import hashlib
import json
import os
from pathlib import Path

BOOTSTRAP = os.getenv("S12_KAFKA", "10.7.11.22:9092,10.7.11.23:9092,10.7.11.24:9092")
HDFS = os.getenv("S12_HDFS", "hdfs://10.7.11.21:9000")
ROOT = "/rbd/s1s2"


def config():
    return json.loads(Path(os.getenv("S12_CONFIG", "/app/S1_S2/config.json")).read_text())


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def topic(run, suffix):
    import re
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,48}", run):
        raise ValueError("run_id: 1–48 letters, digits, _ or -")
    return f"s12.{run}.{suffix}"


def producer():
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})


def publish(p, target, key, value):
    errors = []
    p.produce(target, key=str(key), value=json.dumps(value, allow_nan=False),
              on_delivery=lambda err, msg: errors.append(str(err)) if err else None)
    if p.flush(120) or errors:
        raise RuntimeError(f"Kafka delivery failed: {errors}")


def consumer(run, suffix, group):
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"s12.{run}.{group}",
                  "enable.auto.commit": False, "enable.auto.offset.store": False,
                  "auto.offset.reset": "earliest", "max.poll.interval.ms": 600000})
    c.subscribe([topic(run, suffix)])
    return c
