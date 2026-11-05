#!/usr/bin/env python3
"""Safe historical S2 frost incident preview / optional test-chat Telegram sender.

Run INSIDE existing S1_S2 `init` container (read-only ClickHouse + Telegram Bot API).
No consumer-group changes, no changes to running Spark, no Telegram getUpdates.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


@dataclass(frozen=True)
class Hour:
    region: str
    crop: str
    when: datetime
    minimum: float
    threshold: float


@dataclass(frozen=True)
class Incident:
    region: str
    crop: str
    start: datetime
    end: datetime  # inclusive hour start
    hours: int
    minimum: float
    threshold: float
    event_id: str


def as_utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def make_incident(group: list[Hour]) -> Incident:
    fst, lst = group[0], group[-1]
    key = f'S2|{fst.region}|{fst.crop}|{fst.when.isoformat()}'
    return Incident(fst.region, fst.crop, fst.when, lst.when, len(group),
                    min(x.minimum for x in group), fst.threshold,
                    hashlib.sha256(key.encode()).hexdigest()[:24])


def combine_hours(hours: Iterable[Hour], min_hours=2) -> list[Incident]:
    """Coalesce consecutive 1h frost hours per (county,crop). De-duplicate input hours."""
    unique = {}
    for h in hours:
        key = (h.region, h.crop, as_utc(h.when))
        normalized = Hour(h.region, h.crop, key[2], h.minimum, h.threshold)
        old = unique.get(key)
        if old is None or normalized.minimum < old.minimum:
            unique[key] = normalized
    ordered = sorted(unique.values(), key=lambda h: (h.region, h.crop, h.when))
    result, run = [], []
    for h in ordered:
        if run and (h.region != run[-1].region or h.crop != run[-1].crop
                    or h.when != run[-1].when + timedelta(hours=1)):
            if len(run) >= min_hours:
                result.append(make_incident(run))
            run = []
        run.append(h)
    if run and len(run) >= min_hours:
        result.append(make_incident(run))
    return sorted(result, key=lambda x: (x.start, x.region, x.crop))


def read_incidents(client, start: datetime, end: datetime, regions: list[str], min_hours: int):
    """Query only the bounded historic interval, no global scan / live stream."""
    filters = ['event_hour >= {start:DateTime}', 'event_hour < {end:DateTime}']
    params = {'start': start.strftime('%Y-%m-%d %H:%M:%S'),
              'end': end.strftime('%Y-%m-%d %H:%M:%S')}
    if regions:
        filters.append('region_id IN {regions:Array(String)}')
        params['regions'] = regions
    query = f'''SELECT region_id, crop_group, event_hour,
                 min(min_ground_est_c) AS minimum, any(threshold_c) AS threshold
             FROM rbd.s2_frost_alerts
             WHERE {' AND '.join(filters)}
             GROUP BY region_id, crop_group, event_hour
             ORDER BY region_id, crop_group, event_hour'''
    rows = client.query(query, parameters=params).result_rows
    return combine_hours([Hour(str(reg), str(crop), as_utc(when), float(temp), float(threshold))
                          for reg, crop, when, temp, threshold in rows], min_hours)


def describe(i: Incident, llm_comment=''):
    crop_name = {
        'kukurydza_ziemniaki': 'kukurydza i ziemniaki',
        'rzepak_zboza_jare': 'rzepak i zboża jare',
    }.get(i.crop, i.crop.replace('_', ' '))
    # exclusive end is a more natural boundary for human-readable incident duration
    stop = i.end + timedelta(hours=1)
    body = (f'<b>SYMULACJA 2018 — ryzyko przymrozku (S2)</b>\n'
            f'Powiat TERYT: <code>{html.escape(i.region)}</code>\n'
            f'Uprawy: {html.escape(crop_name)}\n'
            f'Od: {i.start:%Y-%m-%d %H:%M} UTC\n'
            f'Do: {stop:%Y-%m-%d %H:%M} UTC\n'
            f'Czas zagrożenia: {i.hours} h\n'
            f'Minimum szacowane przy gruncie: {i.minimum:.1f} °C\n'
            f'Próg umowny: {i.threshold:.1f} °C\n'
            '<i>Historyczne dane ERA5-Land, modelowy wskaźnik ryzyka, '
            'nie prognoza ani potwierdzenie szkód.</i>')
    if llm_comment:
        body += '\n\n<b>Komentarz Bielika:</b> ' + html.escape(llm_comment[:600])
    return body


def bielik_comment(i: Incident):
    """Optional local Ollama-compatible API. Fully disabled without explicit endpoint+model."""
    endpoint = os.getenv('BIELIK_OLLAMA_URL', '').strip().rstrip('/')
    model = os.getenv('BIELIK_MODEL', '').strip()
    if not endpoint or not model:
        return ''
    import requests
    prompt = (f'Napisz jedno krótkie zdanie po polsku o historycznym ryzyku przymrozku. '
              f'Bez prognoz, bez dodatkowych faktów, bez wymyślania szkód. '
              f'Powiat TERYT {i.region}; grupa {i.crop}; '
              f'{i.start.isoformat()} do {i.end.isoformat()} UTC; '
              f'{i.hours} godzin, minimum {i.minimum:.1f} C, próg {i.threshold:.1f} C. '
              f'Zaznacz, że to symulacja danych z 2018.')
    try:
        response = requests.post(endpoint + '/api/generate', json={
            'model': model, 'prompt': prompt, 'stream': False,
            'options': {'temperature': 0.1, 'num_predict': 90}}, timeout=25)
        response.raise_for_status()
        return str(response.json().get('response', '')).strip()[:600]
    except Exception as exc:
        print(f'Bielik unavailable ({type(exc).__name__}); using deterministic message', file=sys.stderr)
        return ''


def ensure_log(client):
    client.command('''CREATE TABLE IF NOT EXISTS rbd.s1s2_incident_test_log (
        event_id String, chat_id Int64, region_id String, status LowCardinality(String),
        error String, sent_at DateTime64(3, 'UTC') DEFAULT now64(3)
    ) ENGINE MergeTree ORDER BY (event_id, chat_id, sent_at)''')


def already_sent(client, event_id, chat_id):
    rows = client.query('''SELECT count() FROM rbd.s1s2_incident_test_log
        WHERE event_id = {event_id:String} AND chat_id = {chat_id:Int64} AND status = 'ok' ''',
        parameters={'event_id': event_id, 'chat_id': chat_id}).result_rows
    return int(rows[0][0]) > 0


def send_telegram(token, chat_id, message):
    import requests
    proxy = os.getenv('TELEGRAM_PROXY', '').strip()
    proxies = {'https': proxy} if proxy else None
    try:
        r = requests.post(f'https://api.telegram.org/bot{token}/sendMessage', data={
            'chat_id': chat_id, 'text': message, 'parse_mode': 'HTML',
            'disable_web_page_preview': 'true'}, proxies=proxies, timeout=35)
        r.raise_for_status()
        result = r.json()
        if not result.get('ok'):
            raise RuntimeError(f'Telegram response: {result.get("description", "not ok")}')
    except Exception as err:
        # requests errors may contain the URL with token!
        raise RuntimeError(str(err).replace(token, '<BOT_TOKEN>')) from None


def parse_date(s):
    return as_utc(s)


def main(argv=None):
    p = argparse.ArgumentParser(description='Safe S2 historical incident Telegram test')
    p.add_argument('--start', default='2018-04-01T00:00:00Z')
    p.add_argument('--end', default='2018-04-08T00:00:00Z',
                   help='exclusive UTC; bounded range to avoid mass alerts')
    p.add_argument('--region', action='append', default=[], help='optional 4-digit TERYT; repeatable')
    p.add_argument('--min-hours', type=int, default=2)
    p.add_argument('--limit', type=int, default=3)
    p.add_argument('--send', action='store_true', help='opt in to send to ONE test chat only')
    p.add_argument('--allow-historical', action='store_true', help='second explicit opt in')
    p.add_argument('--use-bielik', action='store_true', help='optional local Ollama model')
    args = p.parse_args(argv)
    if args.min_hours < 1 or args.limit < 1 or args.limit > 5:
        p.error('min-hours must be >=1 and limit must be 1..5')
    if args.region and not all(re.fullmatch(r'\d{4}', r) for r in args.region):
        p.error('region must be a 4-digit TERYT')
    start, end = parse_date(args.start), parse_date(args.end)
    if not start < end or end - start > timedelta(days=7):
        p.error('test window must be positive and <=7 days')
    if args.send and not args.allow_historical:
        p.error('--send requires --allow-historical')
    if args.send:
        token = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
        chat_value = os.getenv('TELEGRAM_TEST_CHAT_ID', '').strip()
        if not token or not re.fullmatch(r'-?\d+', chat_value):
            p.error('configure TELEGRAM_BOT_TOKEN and TELEGRAM_TEST_CHAT_ID in container environment')
        chat_id = int(chat_value)
    else:
        token = None
        chat_id = None
    from stream_common import ch_client
    ch = ch_client()
    try:
        incidents = read_incidents(ch, start, end, args.region, args.min_hours)
        print(f'Window: {start.isoformat()} — {end.isoformat()} (end exclusive)')
        print(f'Incidents >= {args.min_hours} consecutive hours: {len(incidents)}')
        chosen = incidents[:args.limit]
        if not chosen:
            print('No incidents in this window. Try a different 7-day period or --min-hours 1.')
            return 0
        if args.send:
            ensure_log(ch)
        for i in chosen:
            comment = bielik_comment(i) if args.use_bielik else ''
            message = describe(i, comment)
            print(f'\nEvent {i.event_id} [{i.region}] {i.hours}h:')
            print(re.sub(r'<[^>]+>', '', message))
            if args.send:
                if already_sent(ch, i.event_id, chat_id):
                    print('SKIP: already sent to test chat')
                    continue
                send_telegram(token, chat_id, message)
                ch.insert('s1s2_incident_test_log',
                          [(i.event_id, chat_id, i.region, 'ok', '')],
                          column_names=['event_id', 'chat_id', 'region_id', 'status', 'error'])
                print('SENT to configured test chat')
            else:
                print('DRY RUN: not sent')
        return 0
    finally:
        ch.close()


if __name__ == '__main__':
    raise SystemExit(main())
