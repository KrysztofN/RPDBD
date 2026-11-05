#!/usr/bin/env python3
"""Publish a single historical S2 incident to a dedicated Kafka test topic.

Run from worker 04 in the existing S1_S2 init container.
No Telegram credentials required; never sends directly to Telegram.
"""
from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config import KAFKA
from stream_common import ch_client
from telegram_incidents import read_incidents

TOPIC = "s1s2.incidents.test"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--publish", action="store_true")
    p.add_argument("--allow-historical", action="store_true")
    args = p.parse_args()
    ch = ch_client()
    try:
        rows = read_incidents(ch,
            datetime(2018,4,1,tzinfo=timezone.utc),
            datetime(2018,4,8,tzinfo=timezone.utc),["0206"],2)
        if not rows:
            raise SystemExit("No incidents for county 0206 in 1–7 April 2018")
        i = rows[0]
        event = dict(source="S2", historical=True, event_id=i.event_id,
                     region_id=i.region, crop_group=i.crop,
                     start=i.start.strftime("%Y-%m-%d %H:%M UTC"),
                     end=(i.end + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M UTC"),
                     hours=i.hours, minimum=i.minimum, threshold=i.threshold)
        print(json.dumps(event, ensure_ascii=False, indent=2), flush=True)
        if not (args.publish and args.allow_historical):
            print("DRY RUN — to publish add --publish --allow-historical", flush=True)
            return
        admin = KafkaAdminClient(bootstrap_servers=KAFKA.split(','),client_id="s2-test-admin")
        try:
            if TOPIC not in admin.list_topics():
                try:
                    admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=3)])
                except TopicAlreadyExistsError:
                    pass
        finally:
            admin.close()
        producer = KafkaProducer(bootstrap_servers=KAFKA.split(','),
                                 value_serializer=lambda obj: json.dumps(obj, ensure_ascii=False).encode("utf-8"))
        try:
            record = producer.send(TOPIC, key=i.region.encode(), value=event).get(timeout=30)
            print(f"PUBLISHED {TOPIC} partition={record.partition} offset={record.offset}", flush=True)
        finally:
            producer.flush(30)
            producer.close()
    finally:
        ch.close()

if __name__ == "__main__":
    main()
