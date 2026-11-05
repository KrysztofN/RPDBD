"""One-time patch of the deployed S1 script. Creates a backup; refuses unknown code."""
from pathlib import Path
import sys
p=Path(sys.argv[1] if len(sys.argv)>1 else 's1_stream.py')
s=p.read_text()
if 'publish_daily_metrics(' in s: raise SystemExit('Already patched')
old='''    scores = rolling_results(batch.sparkSession, daily)
    insert_rows('s1_balance', ['region_id','day','kbw_7d_mm','kbw_30d_mm',
         'kbw_60d_mm','days_7d','days_30d','days_60d','swvl1','alert','version'],
         [(*r,version) for r in scores])
    alerts = [dict(event_id=f'S1:{r[0]}:{r[1].isoformat()}:deficit',source='S1',
        alert_type='water_deficit',region_id=r[0],
        event_time=r[1].isoformat()+'T23:00:00Z',value=round(r[4],2),
        threshold=-150.0,simulation=True) for r in scores if r[9]]
    publish_alerts(alerts)
    print(f'S1 daily batch={batch_id} daily={len(daily)} balance={len(scores)}',flush=True)'''
new='''    # KBW 7/30/60 is now computed by stateful Spark s1_kbw_state.py, not ClickHouse.
    publish_daily_metrics(daily)
    print(f'S1 daily batch={batch_id} daily={len(daily)} metrics_sent={len(daily)}',flush=True)'''
if old not in s: raise SystemExit('Original S1 daily block differs; no file changed. Review manually.')
fn='''\n\ndef publish_daily_metrics(daily):
    """Send complete daily county metrics to Kafka (idempotent downstream by region/day)."""
    import os, json
    from kafka import KafkaProducer
    from config import KAFKA
    from kafka_setup import ensure_topics
    topic=os.getenv('S1_DAILY_TOPIC','s1.daily.metrics.v1')
    ensure_topics([topic])
    p=KafkaProducer(bootstrap_servers=KAFKA.split(','),acks='all',retries=5)
    try:
        for rid, day, precip, et0, swvl1, valid in daily:
            obj=dict(region_id=rid,day=day.isoformat(),precip_mm=precip,et0_mm=et0,swvl1=swvl1)
            p.send(topic,key=rid.encode(),value=json.dumps(obj).encode())
        p.flush()
    finally:
        p.close()
'''
s=s.replace(old,new)
needle='\ndef save_daily(batch, batch_id):'
if needle not in s: raise SystemExit('Cannot find save_daily')
s=s.replace(needle,fn+needle)
backup=p.with_suffix('.py.pre_incidents_backup')
if backup.exists(): raise SystemExit(f'Backup {backup} already exists; refusing overwrite')
backup.write_text(p.read_text());p.write_text(s)
print('Patched',p,'backup',backup)
