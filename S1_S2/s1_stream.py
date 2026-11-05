"""S1: hourly soil-moisture monitoring and daily FAO-56 ET0 with 7/30/60d KBW.
Two streaming queries share the same Kafka input but have independent checkpoints.
The daily query writes complete event-time windows after watermark closure.
"""
import time
from datetime import timedelta
import numpy as np
from pyspark.sql import functions as F, Window
from pyspark.sql import types as T
from config import HDFS, CHECKPOINT_ROOT
from stream_common import make_spark, source, insert_rows, ch_client, publish_alerts
from create_tables import main as ensure_tables


def save_hourly(batch, batch_id):
    rows = [(r.region_id, r.hour, float(r.swvl1), float(r.t2m_c),
             float(r.precip_mm), int(r.observations), int(batch_id))
            for r in batch.toLocalIterator()]
    insert_rows('s1_hourly', ['region_id','event_hour','swvl1','t2m_c',
                             'precip_mm_mean_cell','observations','batch_id'], rows)
    # Illustrative screening threshold, NOT a crop-specific drought diagnosis.
    alerts = [dict(event_id=f'S1:{r[0]}:{r[1].isoformat()}:soil_low',
        source='S1',alert_type='soil_moisture_low',region_id=r[0],
        event_time=r[1].isoformat()+'Z',value=round(r[2],4),
        threshold=0.15,simulation=True) for r in rows if r[2] < 0.15]
    publish_alerts(alerts)
    print(f'S1 hourly batch={batch_id} rows={len(rows)}', flush=True)


def calc_et0(day_rows):
    # Original B1/B2 implementation -- same numerical assumptions as team.
    from lib.era5_counties_spark import et0_fao56
    output = []
    for r in day_rows:
        doy = r.day.timetuple().tm_yday
        et = et0_fao56(np.array([r.t_min]), np.array([r.t_max]),
                 np.array([r.td_mean]), np.array([r.radiation_j_m2/1e6]),
                 np.array([r.wind10_ms*0.748]), np.array([r.latitude]),
                 np.array([doy]))
        output.append((r.region_id,r.day,float(r.precip_mm),float(et[0]),
                       float(r.swvl1),int(r.valid_hours)))
    return output


def rolling_results(spark, day_rows):
    if not day_rows:
        return []
    regions = sorted({r[0] for r in day_rows})
    low = min(r[1] for r in day_rows)-timedelta(days=59)
    high = max(r[1] for r in day_rows)
    ch = ch_client()
    try:
        values = ch.query('''SELECT region_id,day,precip_mm,et0_mm,swvl1
           FROM s1_daily FINAL WHERE day BETWEEN {low:Date} AND {high:Date}
           AND region_id IN {regions:Array(String)}''',
           parameters={'low':low,'high':high,'regions':regions}).result_rows
    finally:
        ch.close()
    if not values:
        return []
    schema = T.StructType([T.StructField('region_id',T.StringType()),
        T.StructField('day',T.DateType()),
        T.StructField('precip_mm',T.DoubleType()),
        T.StructField('et0_mm',T.DoubleType()),
        T.StructField('swvl1',T.DoubleType())])
    d = spark.createDataFrame(values, schema=schema)
    d = d.withColumn('day_num', F.datediff('day',F.lit('1970-01-01')))
    d = d.withColumn('kbw_day', F.col('precip_mm')-F.col('et0_mm'))
    for days in (7,30,60):
        w = Window.partitionBy('region_id').orderBy('day_num').rangeBetween(1-days,0)
        d = (d.withColumn(f'kbw_{days}d_mm', F.sum('kbw_day').over(w))
              .withColumn(f'valid_{days}d', F.count('kbw_day').over(w)))
    first = min(r[1] for r in day_rows)
    d = d.filter((F.col('day')>=F.lit(str(first))) & (F.col('day')<=F.lit(str(high))))
    result = []
    for r in d.toLocalIterator():
        # Strictly require each day in the rolling interval, not just 60 scattered samples.
        alert = int(r.valid_60d == 60 and r.kbw_60d_mm < -150)
        result.append((r.region_id,r.day,float(r.kbw_7d_mm),float(r.kbw_30d_mm),
                       float(r.kbw_60d_mm),int(r.valid_7d),int(r.valid_30d),
                       int(r.valid_60d),float(r.swvl1),alert))
    return result


def save_daily(batch, batch_id):
    rows = list(batch.toLocalIterator())
    # Reject incomplete day windows, including the first UTC date of a truncated replay.
    complete = [r for r in rows if r.valid_hours == 24 and r.observations >= 24]
    if not complete:
        print(f'S1 daily batch={batch_id} no complete days', flush=True)
        return
    daily = calc_et0(complete)
    version = time.time_ns()
    insert_rows('s1_daily', ['region_id','day','precip_mm','et0_mm','swvl1',
                            'valid_hours','version'], [(*r,version) for r in daily])
    scores = rolling_results(batch.sparkSession, daily)
    insert_rows('s1_balance', ['region_id','day','kbw_7d_mm','kbw_30d_mm',
         'kbw_60d_mm','days_7d','days_30d','days_60d','swvl1','alert','version'],
         [(*r,version) for r in scores])
    alerts = [dict(event_id=f'S1:{r[0]}:{r[1].isoformat()}:deficit',source='S1',
        alert_type='water_deficit',region_id=r[0],
        event_time=r[1].isoformat()+'T23:00:00Z',value=round(r[4],2),
        threshold=-150.0,simulation=True) for r in scores if r[9]]
    publish_alerts(alerts)
    print(f'S1 daily batch={batch_id} daily={len(daily)} balance={len(scores)}',flush=True)


def main():
    ensure_tables()
    spark = make_spark('rbd-s1-water')
    df = source(spark)
    # First aggregation over individual hours per county, then over complete UTC days.
    hourly = (df.withWatermark('event_time','3 hours')
        .groupBy(F.window('event_time','1 hour'),'region_id')
        .agg(F.avg('swvl1').alias('swvl1'),F.avg('t2m_c').alias('t2m_c'),
             F.avg('tp_mm').alias('precip_mm'), F.avg('ssrd_j_m2').alias('ssrd_j_m2'),
             F.min('t2m_c').alias('t_min'),F.max('t2m_c').alias('t_max'),
             F.avg('d2m_c').alias('td_mean'),
             F.avg(F.sqrt(F.pow('u10',2)+F.pow('v10',2))).alias('wind10_ms'),
             F.avg('latitude').alias('latitude'),F.count('*').alias('observations'))
        .select('region_id',F.col('window.start').alias('hour'),
           'swvl1','t2m_c','precip_mm','ssrd_j_m2','t_min','t_max',
           'td_mean','wind10_ms','latitude','observations'))
    # Separate hourly view (independent query and checkpoint).
    qh = (hourly.select('region_id','hour','swvl1','t2m_c','precip_mm','observations')
          .writeStream.outputMode('append').foreachBatch(save_hourly)
          .option('checkpointLocation',HDFS+CHECKPOINT_ROOT+'/s1_hourly')
          .queryName('rbd-s1-hourly').trigger(processingTime='15 seconds').start())
    # A separate query ensures daily windows close independently of the hourly sink.
    # Aggregate the original stream to days (not an unsupported streaming reaggregation
    # of a previously watermarked event-time streaming aggregate).
    daily = (df.withWatermark('event_time','3 hours')
      .groupBy(F.window('event_time','1 day'),'region_id')
      .agg(F.min('t2m_c').alias('t_min'),F.max('t2m_c').alias('t_max'),
           F.avg('d2m_c').alias('td_mean'),
           F.avg(F.sqrt(F.pow('u10',2)+F.pow('v10',2))).alias('wind10_ms'),
           F.avg('latitude').alias('latitude'),F.avg('swvl1').alias('swvl1'),
           # Valid with a constant cell set (true for our ERA5 grid; check separately).
           (F.avg('tp_mm')*24).alias('precip_mm'),
           (F.avg('ssrd_j_m2')*24).alias('radiation_j_m2'),
           F.size(F.collect_set(F.hour('event_time'))).alias('valid_hours'),
           F.count('*').alias('observations'))
      .select('region_id',F.to_date(F.col('window.start')).alias('day'),
              't_min','t_max','td_mean','wind10_ms','latitude','swvl1',
              'precip_mm','radiation_j_m2','valid_hours','observations'))
    qd = (daily.writeStream.outputMode('append').foreachBatch(save_daily)
          .option('checkpointLocation',HDFS+CHECKPOINT_ROOT+'/s1_daily')
          .queryName('rbd-s1-daily').trigger(processingTime='30 seconds').start())
    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    main()
