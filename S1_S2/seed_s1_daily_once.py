"""ONE-TIME migration/backfill: CH historical s1_daily -> Kafka daily metrics.
NOT part of the production streaming data path. Does not modify ClickHouse.
"""
import json
import os
from stream_common import ch_client
from config import KAFKA
from kafka import KafkaProducer
from kafka_setup import ensure_topics

def main():
    if os.getenv('CONFIRM_S1_BACKFILL')!='YES':
        raise SystemExit('Set CONFIRM_S1_BACKFILL=YES explicitly for one-time backfill')
    topic=os.getenv('S1_DAILY_TOPIC','s1.daily.metrics.v1')
    ensure_topics([topic])
    c=ch_client()
    p=KafkaProducer(bootstrap_servers=KAFKA.split(','),acks='all',retries=5)
    count=0
    try:
        # Per-county ordering matters for stateful KBW; run once on empty topic/checkpoint.
        regions=[r[0] for r in c.query('SELECT DISTINCT region_id FROM rbd.s1_daily FINAL ORDER BY region_id').result_rows]
        for rid in regions:
            rows=c.query('''SELECT day,precip_mm,et0_mm,swvl1 FROM rbd.s1_daily FINAL
                            WHERE region_id={rid:String} AND valid_hours=24 ORDER BY day''',
                         parameters={'rid':rid}).result_rows
            for day,ppt,et0,moist in rows:
                val=dict(region_id=rid, day=day.isoformat(), precip_mm=float(ppt),
                         et0_mm=float(et0), swvl1=float(moist))
                p.send(topic,key=rid.encode(),value=json.dumps(val).encode())
                count+=1
            p.flush()  # preserve order per county before proceeding
        print('Seeded complete S1 daily metrics:',count)
    finally:
        p.close();c.close()
if __name__=='__main__': main()
