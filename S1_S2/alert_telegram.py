"""Separate consumer of S1/S2 events. Reuses existing bot subscriptions + Telegram API.
Default is dry-run; actual Telegram delivery requires --send --allow-historical.
Never calls Telegram getUpdates, so it does not compete with the team's running bot.
"""
import argparse
import json
import os
from datetime import datetime, timezone
from html import escape
from config import KAFKA
from stream_common import ch_client


def format_message(event):
    source = event['source']
    region = escape(str(event['region_id']))
    when = escape(str(event['event_time']))
    value = float(event['value'])
    if source == 'S1' and event.get('alert_type') == 'soil_moisture_low':
        detail = f'Niska wilgotność gleby 0–7 cm: {value:.3f} m³/m³ (próg demonstracyjny: 0,150).'
    elif source == 'S1':
        detail = f'Bilans klimatyczny z 60 dni: {value:.1f} mm (próg: -150 mm).'
    else:
        crop = escape(str(event.get('crop_group','')))
        detail = (f'Szacowana temperatura przy gruncie: {value:.1f} °C; '
                  f'próg: {float(event["threshold"]):.1f} °C. Grupa: {crop}.')
    return (f'<b>SYMULACJA 2018 — alert {escape(source)}</b>\n'
            f'Powiat TERYT: <code>{region}</code>\n'
            f'Zdarzenie: {when}\n{detail}\n'
            '<i>Dane archiwalne ERA5-Land. Wskaźnik ryzyka, nie stwierdzenie szkody.</i>')


def main():
    a = argparse.ArgumentParser()
    a.add_argument('--send',action='store_true',help='send messages; otherwise print only')
    a.add_argument('--allow-historical',action='store_true',help='required to send simulated archival alerts')
    args = a.parse_args()
    if args.send and not args.allow_historical:
        raise SystemExit('History replay: --send requires --allow-historical')
    # Team bot module needed only when actual delivery is enabled.
    if args.send:
        from B1_B2.reporter.telegram_bot import Telegram, scope_matches
    from confluent_kafka import Consumer
    token = os.getenv('TELEGRAM_BOT_TOKEN','')
    if args.send and not token:
        raise SystemExit('Missing TELEGRAM_BOT_TOKEN')
    ch = ch_client()
    telegram = Telegram(token, os.getenv('TELEGRAM_PROXY','')) if args.send else None
    consumer = Consumer({'bootstrap.servers':KAFKA,'group.id':'s1s2-telegram-v1',
       'auto.offset.reset':'earliest','enable.auto.commit':False})
    consumer.subscribe(['alerts.detected'])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): raise RuntimeError(msg.error())
            event = json.loads(msg.value())
            eid = str(event['event_id'])
            region = str(event['region_id'])
            if not args.send:
                print(eid,format_message(event),flush=True)
                consumer.commit(msg,asynchronous=False)
                continue
            subscriptions = ch.query('''SELECT chat_id,scope FROM tg_subscribers FINAL
                WHERE active=1''').result_rows
            recipients = sorted({int(chat) for chat,scope in subscriptions if scope_matches(scope,region)})
            # The log is idempotency control at the application level; delivery is at-least-once.
            done = {int(r[0]) for r in ch.query('''SELECT DISTINCT chat_id FROM s1s2_alert_log
                WHERE event_id={id:String} AND status='ok' ''',parameters={'id':eid}).result_rows}
            for chat in recipients:
                if chat in done: continue
                try:
                    telegram.send_text(chat,format_message(event))
                    status,error = 'ok',''
                except Exception as ex:
                    status,error = 'error',str(ex)[:200].replace(token,'<token>')
                ch.insert('s1s2_alert_log',[(eid,chat,region,status,error)],
                    column_names=['event_id','chat_id','region_id','status','error'])
                if status != 'ok':
                    raise RuntimeError(f'Failed delivery for event {eid}, chat {chat}: {error}')
            consumer.commit(msg,asynchronous=False)
    finally:
        consumer.close()
        ch.close()

if __name__=='__main__':
    main()
