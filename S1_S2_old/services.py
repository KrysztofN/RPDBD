"""Infrastructure initialization, result sink, API and notification workers."""
import argparse
import json
import os
import time
from pathlib import Path
from datetime import datetime, timezone
from S1_S2.common import BOOTSTRAP, consumer, digest, producer, publish, topic
from S1_S2.storage import DDL, Store, connect


def initialize(run):
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaException, KafkaError
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    futures = admin.create_topics([NewTopic(topic(run, s), num_partitions=12, replication_factor=3,
                         config={'min.insync.replicas':'2','retention.ms':'1209600000'})
                                  for s in ('weather','results','dlq','telegram')])
    for name, future in futures.items():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        print(name)
    cluster, session = connect()
    try:
        for ddl in DDL:
            session.execute(ddl)
    finally:
        cluster.shutdown()


def subscribers():
    path = Path(os.getenv('S12_SUBSCRIPTIONS', '/app/S1_S2/local/subscriptions.json'))
    if not path.exists():
        return []
    return json.loads(path.read_text())


def sink(run):
    from prometheus_client import Counter, start_http_server
    start_http_server(9212)
    saved = Counter('s12_results_saved_total', 'Processed result records including retries')
    cluster, session = connect()
    store, prod = Store(session), producer()
    c = consumer(run, 'results', 'cassandra')
    try:
        while True:
            msg = c.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            doc = json.loads(msg.value())
            store.save(doc)
            if doc['notify']:
                for sub in subscribers():
                    if doc['region_id'] not in sub['regions'] and '*' not in sub['regions']:
                        continue
                    if doc['process'] not in sub.get('processes', ['S1','S2']):
                        continue
                    delivery = dict(chat_id=str(sub['chat_id']), event=doc,
                                    delivery_id=digest([doc['event_id'], str(sub['chat_id'])]))
                    # Key by chat serializes sends to one recipient across workers.
                    publish(prod, topic(run,'telegram'), sub['chat_id'], delivery)
            c.commit(msg, asynchronous=False)
            saved.inc()
    finally:
        c.close()
        cluster.shutdown()


def notification_text(doc):
    stamp = datetime.fromtimestamp(doc['ts'], timezone.utc).isoformat()
    levels = {'ok':'OK', 'warning':'OSTRZEŻENIE', 'alarm':'ALARM', 'unknown':'BRAK PEŁNYCH DANYCH'}
    return (f"[REPLAY {doc['run_id']}] {doc['process']} — {levels[doc['severity']]}\n"
            f"Powiat {doc['region_id']}, wariant: {doc['crop']}\nCzas danych: {stamp}\n"
            f"Punkty alarmowe: {doc['alarm_points']}/{doc['expected_points']}\n"
            f"Dane: {json.dumps(doc['metrics'], ensure_ascii=False)}\n"
            'Ocena modelowa regionu; nie jest prognozą ani oceną szkody na konkretnym polu.')


def telegram(run):
    import requests
    token_path = os.getenv('S12_TELEGRAM_TOKEN_FILE', '/run/secrets/s12_telegram_token')
    token = Path(token_path).read_text().strip()
    cluster, session = connect()
    store, c = Store(session), consumer(run, 'telegram', 'senders')
    http = requests.Session()
    proxy = os.getenv('S12_TELEGRAM_PROXY')
    if proxy:
        http.proxies.update({'https':proxy, 'http':proxy})
    try:
        while True:
            msg = c.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            request = json.loads(msg.value())
            did = request['delivery_id']
            if session.execute(store.sent, (did,)).one():
                c.commit(msg, asynchronous=False)
                continue
            try:
                response = http.post(f'https://api.telegram.org/bot{token}/sendMessage',
                                     json={'chat_id':request['chat_id'], 'text':notification_text(request['event'])}, timeout=30)
                data = response.json()
                if not response.ok or not data.get('ok'):
                    # Keep the offset uncommitted: no silent loss of failed recipients.
                    wait = min(int(data.get('parameters', {}).get('retry_after', 30)), 120)
                    print(f'Telegram HTTP {response.status_code}; retry in {wait}s', flush=True)
                    c.seek(msg)
                    time.sleep(wait)
                    continue
                session.execute(store.mark, (did, datetime.now(timezone.utc)))
                c.commit(msg, asynchronous=False)
                time.sleep(1.1)  # conservative per-recipient pacing
            except (requests.RequestException, ValueError):
                # Never log the request URL (it contains the token).
                c.seek(msg)
                time.sleep(30)
    finally:
        c.close()
        cluster.shutdown()


def api():
    from contextlib import asynccontextmanager
    from fastapi import FastAPI, HTTPException, Query
    import uvicorn
    @asynccontextmanager
    async def lifespan(app):
        app.state.cluster, app.state.db = connect()
        yield
        app.state.cluster.shutdown()
    app = FastAPI(title='S1/S2 results', lifespan=lifespan)

    @app.get('/health')
    def health():
        app.state.db.execute('SELECT now() FROM system.local')
        return {'status':'ok'}

    @app.get('/runs/{run}/counties/{region}')
    def current(run: str, region: str):
        rows = app.state.db.execute('SELECT payload FROM s12.latest WHERE run_id=%s AND region_id=%s', (run,region))
        return [json.loads(r.payload) for r in rows]

    @app.get('/runs/{run}/counties/{region}/history')
    def history(run: str, region: str, process: str = 'S1', month: str = '2018-04', limit: int = Query(100, ge=1, le=2000)):
        rows = app.state.db.execute('SELECT payload FROM s12.results_by_month WHERE run_id=%s AND region_id=%s AND process=%s AND month=%s LIMIT %s', (run,region,process,month,limit))
        return [json.loads(r.payload) for r in rows]

    uvicorn.run(app, host='0.0.0.0', port=8212)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('command', choices=['init','sink','telegram','api'])
    p.add_argument('--run', default=os.getenv('S12_RUN', 'demo2018'))
    args = p.parse_args()
    {'init':lambda: initialize(args.run), 'sink':lambda: sink(args.run),
     'telegram':lambda: telegram(args.run), 'api':api}[args.command]()


if __name__ == '__main__':
    main()
