#!/usr/bin/env python3
"""Bounded S2 incident watcher: ClickHouse hourly view -> SQLite lifecycle -> Kafka test.

Dry-run by default. One test county, one publication ever per SQLite database by default.
Only closed historical incidents are published. Does NOT call Telegram API.
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telegram_incidents import read_incidents, Incident

TOPIC = 's1s2.incidents.test'


def utc(s: str) -> datetime:
    d = datetime.fromisoformat(s.replace('Z', '+00:00'))
    if d.tzinfo is None:
        raise ValueError('UTC offset required in ISO timestamps')
    return d.astimezone(timezone.utc)


def connect_state(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('''CREATE TABLE IF NOT EXISTS incident_state (
      event_id TEXT PRIMARY KEY, region_id TEXT NOT NULL, crop_group TEXT NOT NULL,
      started TEXT NOT NULL, last_hour TEXT NOT NULL, hours INTEGER NOT NULL,
      minimum REAL NOT NULL, phase TEXT NOT NULL, published INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS audit (
      id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,
      phase TEXT NOT NULL, recorded_at TEXT NOT NULL, details TEXT NOT NULL)''')
    db.commit()
    return db


def current_watermark(ch) -> datetime | None:
    # Max completed hour across counties. A lag avoids premature closure as data trickles in.
    rows = ch.query('SELECT max(event_hour) FROM rbd.s2_hourly').result_rows
    if not rows or rows[0][0] is None:
        return None
    d = rows[0][0]
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)


def event_payload(i: Incident):
    return {
        'source': 'S2', 'historical': True, 'event_id': i.event_id,
        'region_id': i.region, 'crop_group': i.crop,
        'start': i.start.strftime('%Y-%m-%d %H:%M UTC'),
        'end': (i.end + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M UTC'),
        'hours': i.hours, 'minimum': i.minimum, 'threshold': i.threshold,
    }


def phase_of(i: Incident, max_hour: datetime | None, grace_hours: int) -> str:
    # Last dangerous hour [end, end+1h); close when >=grace quiet subsequent hours seen.
    if max_hour is not None and max_hour >= i.end + timedelta(hours=1+grace_hours):
        return 'closed'
    return 'open'


def record(db, i: Incident, phase: str):
    now = datetime.now(timezone.utc).isoformat()
    prev = db.execute('SELECT hours,minimum,phase,published FROM incident_state WHERE event_id=?',
                      (i.event_id,)).fetchone()
    new_phase = phase if phase == 'closed' else (
        'updated' if prev and (i.hours != prev[0] or i.minimum != prev[1] or prev[2] == 'updated')
        else 'open')
    changed = prev is None or (i.hours, i.minimum, new_phase) != (prev[0], prev[1], prev[2])
    if changed:
        db.execute('''INSERT INTO incident_state(event_id,region_id,crop_group,started,last_hour,hours,minimum,phase,published,updated_at)
            VALUES(?,?,?,?,?,?,?,?,0,?)
            ON CONFLICT(event_id) DO UPDATE SET last_hour=excluded.last_hour,hours=excluded.hours,
              minimum=excluded.minimum,phase=excluded.phase,updated_at=excluded.updated_at''',
            (i.event_id, i.region, i.crop, i.start.isoformat(), i.end.isoformat(),
             i.hours, i.minimum, new_phase, now))
        db.execute('INSERT INTO audit(event_id,phase,recorded_at,details) VALUES(?,?,?,?)',
                   (i.event_id,new_phase,now,f'hours={i.hours};minimum={i.minimum:.2f}'))
        db.commit()
    return new_phase, bool(prev and prev[3]), changed


def publish_one(event, bootstrap: str):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=bootstrap.split(','), acks='all', retries=5,
        key_serializer=lambda s: s.encode('utf-8'),
        value_serializer=lambda p: json.dumps(p,ensure_ascii=False).encode('utf-8'))
    try:
        md = producer.send(TOPIC, key=event['region_id'], value=event).get(timeout=30)
        return md.partition, md.offset
    finally:
        producer.flush(15)
        producer.close()


def poll_once(ch, db, start, end, regions, min_hours, grace_hours, max_publish, enabled, bootstrap):
    max_hour = current_watermark(ch)
    if max_hour is None:
        print('S2 hourly empty; no action', flush=True)
        return
    # Do not let an end beyond available data make incidents appear final.
    scan_end = min(end, max_hour + timedelta(hours=1))
    if scan_end <= start:
        print('S2 has not reached configured start; no action', flush=True)
        return
    candidates = read_incidents(ch, start, scan_end, regions, min_hours)
    sent_total = db.execute('SELECT count(*) FROM incident_state WHERE published=1').fetchone()[0]
    phases = {'open': 0, 'updated': 0, 'closed': 0}
    for i in candidates:
        phase, was_published, changed = record(db, i, phase_of(i,max_hour,grace_hours))
        phases[phase] += 1
        if phase != 'closed' or was_published or not changed and not enabled:
            continue
        if sent_total >= max_publish:
            continue
        event = event_payload(i)
        if not enabled:
            print('DRY RUN closed incident (not published): '+json.dumps(event,ensure_ascii=False),flush=True)
            continue
        part, offset = publish_one(event, bootstrap)
        db.execute('UPDATE incident_state SET published=1 WHERE event_id=?',(i.event_id,))
        db.execute('INSERT INTO audit(event_id,phase,recorded_at,details) VALUES(?,?,?,?)',
                   (i.event_id,'published',datetime.now(timezone.utc).isoformat(),f'{TOPIC}:{part}:{offset}'))
        db.commit()
        sent_total += 1
        print(f'PUBLISHED event={i.event_id} partition={part} offset={offset} total={sent_total}/{max_publish}',flush=True)
    print(f'S2 last_hour={max_hour.isoformat()} incidents={len(candidates)} states={phases} '
          f'published_total={sent_total}/{max_publish} enabled={enabled}',flush=True)


def main():
    p = argparse.ArgumentParser(description='S2 incident watcher (dry run by default)')
    p.add_argument('--start',default='2018-04-01T00:00:00Z')
    p.add_argument('--end',default='2018-04-08T00:00:00Z')
    p.add_argument('--regions',default='0206',help='comma-separated TERYT codes; default only 0206')
    p.add_argument('--min-hours',type=int,default=2)
    p.add_argument('--grace-hours',type=int,default=3)
    p.add_argument('--max-publish',type=int,default=1,help='LIFETIME cap per state database')
    p.add_argument('--state',default='/state/s2_watcher.sqlite3')
    p.add_argument('--interval',type=int,default=60)
    p.add_argument('--once',action='store_true')
    args=p.parse_args()
    start,end=utc(args.start),utc(args.end)
    regions=[x.strip() for x in args.regions.split(',') if x.strip()]
    if not (0 < (end-start).total_seconds() <= 7*86400): p.error('test window must be <=7 days')
    if not regions or len(regions)>3 or not all(re.fullmatch(r'\d{4}',x) for x in regions):
        p.error('1..3 four-digit TERYT region codes required')
    if args.min_hours<2 or args.grace_hours<1 or args.max_publish<1 or args.max_publish>3 or args.interval<10:
        p.error('min-hours >=2, grace >=1, max-publish 1..3, interval >=10')
    enabled = os.getenv('S2_AUTOPUBLISH','0') == '1'
    print(f'Watcher start: regions={regions} range=[{start},{end}) publish={enabled} '
          f'max={args.max_publish}, state={args.state}',flush=True)
    from stream_common import ch_client
    from config import KAFKA
    db=connect_state(args.state)
    try:
        while True:
            try:
                ch=ch_client()
                try:
                    poll_once(ch,db,start,end,regions,args.min_hours,args.grace_hours,
                              args.max_publish,enabled,KAFKA)
                finally:
                    ch.close()
            except Exception as exc:
                print(f'ERROR: {type(exc).__name__}: {exc}',flush=True)
                if args.once:
                    raise
            if args.once:
                break
            time.sleep(args.interval)
    finally:
        db.close()

if __name__=='__main__':
    main()
