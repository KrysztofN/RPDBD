"""Spark 4.0.4 stateful 60-day KBW from Kafka daily metrics, not ClickHouse.

PySpark applyInPandasWithState keeps 60 days per county in HDFS Spark checkpoint.
Assumptions: daily metrics are immutable once emitted, in event-date order per county;
late/corrected historical days are not supported in this version.
"""
from __future__ import annotations
import json
import math
import os
from datetime import date, datetime, timedelta
import pandas as pd
from pyspark.sql import functions as F, types as T
from pyspark.sql.streaming.state import GroupStateTimeout
from config import KAFKA, HDFS, CHECKPOINT_ROOT
from stream_common import make_spark, ch_client

IN = os.getenv('S1_DAILY_TOPIC', 's1.daily.metrics.v1')
ALERTS = os.getenv('INCIDENTS_INPUT_TOPIC','alerts.detected')
SCHEMA = T.StructType([
    T.StructField('region_id', T.StringType()), T.StructField('day', T.StringType()),
    T.StructField('precip_mm', T.DoubleType()), T.StructField('et0_mm', T.DoubleType()),
    T.StructField('swvl1', T.DoubleType())])
OUT = T.StructType([
    T.StructField('region_id', T.StringType()), T.StructField('day', T.StringType()),
    T.StructField('kbw_7d_mm', T.DoubleType()), T.StructField('kbw_30d_mm', T.DoubleType()),
    T.StructField('kbw_60d_mm', T.DoubleType()),
    T.StructField('days_7d', T.IntegerType()), T.StructField('days_30d', T.IntegerType()),
    T.StructField('days_60d', T.IntegerType()), T.StructField('swvl1', T.DoubleType()),
    T.StructField('alert', T.IntegerType())])
STATE = T.StructType([T.StructField('days_json', T.StringType()), T.StructField('last_day', T.StringType())])


def reducer(key, iterator, state):
    rid = str(key[0])
    if state.exists:
        days_json, last_day = state.get
        days = json.loads(days_json)
    else:
        days, last_day = {}, ''
    incoming = []
    for pdf in iterator:
        incoming.extend(pdf.to_dict('records'))
    result=[]
    for r in sorted(incoming, key=lambda x: str(x['day'])):
        day = str(r['day'])[:10]
        if day <= last_day:  # immutable daily events; no historical revisions
            continue
        precip, et0, moist = float(r['precip_mm']), float(r['et0_mm']), float(r['swvl1'])
        if not all(math.isfinite(x) for x in (precip, et0, moist)):
            continue
        days[day] = [precip-et0, moist]
        d = date.fromisoformat(day)
        cutoff = (d-timedelta(days=59)).isoformat()
        days = {k:v for k,v in days.items() if k>=cutoff}
        fields=[]
        for span in (7,30,60):
            dates = [(d-timedelta(days=n)).isoformat() for n in range(span)]
            available = [k for k in dates if k in days]
            fields += [float(sum(days[k][0] for k in available)), len(available)]
        k7,n7,k30,n30,k60,n60 = fields
        result.append(dict(region_id=rid,day=day,kbw_7d_mm=k7,kbw_30d_mm=k30,kbw_60d_mm=k60,
                           days_7d=n7,days_30d=n30,days_60d=n60,swvl1=moist,
                           alert=int(n7==10 and k7 < -50.0)))
        last_day=day
    if result:
        state.update((json.dumps(days,sort_keys=True),last_day))
        yield pd.DataFrame(result)


def sink(batch, batch_id):
    if batch.isEmpty(): return
    import time
    rows=[(r.region_id, date.fromisoformat(r.day),r.kbw_7d_mm,r.kbw_30d_mm,r.kbw_60d_mm,
           r.days_7d,r.days_30d,r.days_60d,r.swvl1,r.alert,time.time_ns())
          for r in batch.toLocalIterator()]
    c=ch_client()
    try:
        c.insert('s1_balance',rows,column_names=['region_id','day','kbw_7d_mm','kbw_30d_mm',
           'kbw_60d_mm','days_7d','days_30d','days_60d','swvl1','alert','version'])
    finally: c.close()
    # Send only valid 60-day drought signals to Kafka, including consecutive days.
    alerts=(batch.filter(F.col('alert')==1)
       .select(F.col('region_id').alias('key'),F.to_json(F.struct(
          F.concat(F.lit('S1:'),F.col('region_id'),F.lit(':'),F.col('day'),F.lit(':deficit')).alias('event_id'),
          F.lit('S1').alias('source'), F.lit('water_deficit').alias('alert_type'),
          'region_id', F.concat(F.col('day'),F.lit('T23:00:00Z')).alias('event_time'),
          F.col('kbw_7d_mm').alias('value'),F.lit(-50.0).alias('threshold'),
          F.lit(os.getenv('INCIDENTS_SIMULATION','1') == '1').alias('simulation'))).alias('value')))
    (alerts.write.format('kafka').option('kafka.bootstrap.servers',KAFKA)
      .option('topic',ALERTS).save())
    print(f'S1 KBW batch={batch_id} days={len(rows)}',flush=True)


def main():
    spark=make_spark('rbd-s1-kbw-state')
    source=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',KAFKA)
       .option('subscribe',IN).option('startingOffsets','earliest').load())
    df=(source.select(F.from_json(F.col('value').cast('string'),SCHEMA).alias('v'))
        .select('v.*').filter(F.col('region_id').rlike('^[0-9]{4}$') & F.col('day').isNotNull()))
    stateful=df.groupBy('region_id').applyInPandasWithState(reducer,OUT,STATE,
             outputMode='Update', timeoutConf=GroupStateTimeout.NoTimeout)
    q=(stateful.writeStream.outputMode('update').foreachBatch(sink)
       .option('checkpointLocation',HDFS+CHECKPOINT_ROOT+'/s1_kbw_state_v1')
       .queryName('rbd-s1-kbw-state').trigger(processingTime='30 seconds').start())
    q.awaitTermination()

if __name__=='__main__': main()
