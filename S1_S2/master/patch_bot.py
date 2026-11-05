"""Patch an existing B1_B2/reporter/telegram_bot.py, not a standalone bot.
Reads one Kafka topic inside the same existing single getUpdates service.
"""
from pathlib import Path
import sys

path = Path(sys.argv[1])
s = path.read_text()
marker = 'S2_TEST_TOPIC = "s1s2.incidents.test"'
assert marker in s, 'Expected the previously deployed S2 test bot (rbd-reporter:0.2)'
assert 'INCIDENTS_TOPIC = ' not in s, 'Already patched'
s=s.replace(marker, marker+'\nINCIDENTS_TOPIC = os.environ.get("S1S2_INCIDENTS_TOPIC", "s1s2.incidents.v1")')
anchor = '\ndef run_service(bot: Bot) -> None:\n'
assert anchor in s
handler = r"""
def send_county_incident(bot: Bot, event: dict) -> None:
    # Only county subscriptions; best-effort durable idempotency.
    if os.environ.get('S1S2_INCIDENTS_ENABLED', '0') != '1':
        log.info('Incident ignored (disabled)')
        return
    if event.get('state') != 'closed' or event.get('source') not in ('S1','S2'):
        raise ValueError('Unexpected incident type/state')
    eid = str(event.get('event_id', ''))
    rid = str(event.get('region_id', ''))
    if not re.fullmatch(r'[a-f0-9]{64}', eid) or not re.fullmatch(r'[0-9]{4}', rid):
        raise ValueError('Invalid incident ID/region')
    source = event['source']
    duration = int(event['duration'])
    minimum = float(event['minimum'])
    if duration < 1 or duration > 366 or abs(minimum) > 10000:
        raise ValueError('Invalid duration/minimum')
    start, end = str(event.get('start', '')), str(event.get('end', ''))
    if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ', start):
        raise ValueError('Invalid incident start')
    if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ', end):
        raise ValueError('Invalid incident end')
    # Safety: explicit test mode restricts recipients to the one chat.
    test_only = os.environ.get('S1S2_INCIDENTS_TEST_ONLY', '1') == '1'
    test_chat = os.environ.get('S1S2_TEST_CHAT_ID', '')
    if test_only and not re.fullmatch(r'-?\d+', test_chat):
        raise ValueError('Missing S1S2_TEST_CHAT_ID; refusing send')
    if bool(event.get('historical')) and os.environ.get('S1S2_ALLOW_HISTORICAL', '0') != '1':
        log.info('Historical incident %s skipped by policy', eid)
        return

    # This query checks the actual /powiat subscription. No matching voiv/all scope.
    subscribers = bot.ch.query_df(
        f"SELECT DISTINCT chat_id FROM rbd.tg_subscribers FINAL "
        f"WHERE active=1 AND scope={sql_str('county:' + rid)}")
    chats = [int(row.chat_id) for row in subscribers.itertuples()]
    if test_only:
        chats = [cid for cid in chats if cid == int(test_chat)]
    if not chats:
        log.info('No /powiat subscribers for incident %s %s', eid, rid)
        return
    bot.ch.command('''CREATE TABLE IF NOT EXISTS rbd.tg_incident_delivery (
        event_id String, chat_id Int64, region_id String, source String,
        status LowCardinality(String), error String, sent DateTime('UTC')
    ) ENGINE MergeTree ORDER BY (event_id,chat_id,sent)''')
    crops = event.get('crops') or []
    if source == 'S2':
        labels = {'kukurydza_ziemniaki':'kukurydza i ziemniaki',
                  'rzepak_zboza_jare':'rzepak i zboża jare'}
        crops_text = ', '.join(labels.get(str(x), str(x)) for x in crops) or 'brak określonej grupy'
        headline = 'Incydent przymrozkowy S2'
        metric = f'Minimum szacowane przy gruncie: {minimum:.1f} °C'
        detail = f'Zagrożone grupy: {escape(crops_text)}\nCzas: {duration} h'
    else:
        headline = 'Incydent deficytu wodnego S1'
        metric = f'Najniższy KBW 60 dni: {minimum:.1f} mm'
        detail = f'Czas: {duration} dni (pełne okna KBW 60 dni)'
    prefix = 'SYMULACJA HISTORYCZNA 2018 — ' if bool(event.get('historical')) else ''
    body = (f'<b>{escape(prefix + headline)}</b>\n'
            f'Powiat TERYT: <code>{rid}</code>\n'
            f'{detail}\nOd: {escape(start)}\nDo: {escape(end)}\n'
            f'{metric}\n'
            '<i>Wskaźnik modelowy; nie potwierdza szkód i nie stanowi prognozy.</i>')
    import pandas as pd
    for chat_id in chats:
        already = bot.ch.query_df(
            f"SELECT count() AS n FROM rbd.tg_incident_delivery "
            f"WHERE event_id={sql_str(eid)} AND chat_id={chat_id} AND status='ok'")
        if int(already.iloc[0]['n']) > 0:
            log.info('Duplicate incident %s chat=%s skipped', eid, chat_id)
            continue
        # If Telegram fails, leave Kafka offset uncommitted; retry after recovery.
        bot.tg.send_text(chat_id, body)
        bot.ch.insert_df('tg_incident_delivery', pd.DataFrame([{
            'event_id': eid, 'chat_id': chat_id, 'region_id':rid, 'source':source,
            'status':'ok', 'error':'', 'sent':datetime.utcnow().replace(microsecond=0)}]))
        log.info('Incident delivered event=%s source=%s county=%s chat=%s', eid, source, rid, chat_id)
        time.sleep(PAUSE_S)

"""
s=s.replace(anchor, '\n'+handler+anchor)
s=s.replace('consumer.subscribe(topics)', '''if os.environ.get('S1S2_INCIDENTS_ENABLED', '0') == '1':
        topics.append(INCIDENTS_TOPIC)
    consumer.subscribe(topics)''')
s=s.replace('            if msg.topic() == S2_TEST_TOPIC:', '''            if msg.topic() == INCIDENTS_TOPIC:
                try:
                    send_county_incident(bot, json.loads(msg.value()))
                except (RuntimeError, requests.RequestException, OSError) as e:
                    log.error('Incident delivery failed: %s', type(e).__name__)
                    from confluent_kafka import TopicPartition
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    time.sleep(10)
                    continue
                except (ValueError, KeyError, TypeError) as e:
                    log.error('Invalid incident ignored: %s', type(e).__name__)
                consumer.commit(msg, asynchronous=False)
                continue
            if msg.topic() == S2_TEST_TOPIC:''')
path.write_text(s)
print('Patched',path)
