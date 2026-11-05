"""S2: hourly frost detection and an independent rolling 24h summary.
One row is emitted once the event-time watermark has closed an entire hour.
"""
import time
from pyspark.sql import functions as F
from config import HDFS, CHECKPOINT_ROOT
from stream_common import make_spark, source, insert_rows, publish_alerts
from create_tables import main as ensure_tables


def save_hour(batch, batch_id):
    rows = []
    alerts = []
    for r in batch.toLocalIterator():
        h = r.event_hour
        rows.append((r.region_id, h, float(r.min_air_c), float(r.min_ground_est_c),
                     float(r.min_surface_c), float(r.min_soil_c), int(r.observations), int(batch_id)))
        # These are *potential* exposure thresholds, not crop-damage predictions.
        for crop, threshold in (("rzepak_zboza_jare", -4.0), ("kukurydza_ziemniaki", 0.0)):
            if r.min_ground_est_c < threshold:
                alerts.append(dict(event_id=f"S2:{r.region_id}:{h.isoformat()}:{crop}",
                    source="S2", alert_type="frost", region_id=r.region_id,
                    event_time=h.isoformat()+"Z", crop_group=crop,
                    value=round(float(r.min_ground_est_c), 2), threshold=threshold,
                    simulation=True))
    insert_rows('s2_hourly', ['region_id','event_hour','min_air_c','min_ground_est_c',
                   'min_surface_c','min_soil_c','observations','batch_id'], rows)
    # Send after persisting. A retry can resend; alert delivery must deduplicate event_id.
    publish_alerts(alerts)
    print(f'S2 batch={batch_id} hour_rows={len(rows)} alerts={len(alerts)}', flush=True)


def main():
    ensure_tables()
    spark = make_spark('rbd-s2-frost')
    df = source(spark)
    hourly = (df.withWatermark('event_time', '3 hours')
      .groupBy(F.window('event_time','1 hour'), 'region_id')
      .agg(F.min('t2m_c').alias('min_air_c'),
           (F.min('t2m_c')-F.lit(1.6)).alias('min_ground_est_c'),
           F.min('skt_c').alias('min_surface_c'),
           F.min('stl1_c').alias('min_soil_c'), F.count('*').alias('observations'))
      .select('region_id', F.col('window.start').alias('event_hour'),
              'min_air_c','min_ground_est_c','min_surface_c','min_soil_c','observations'))
    q = (hourly.writeStream.outputMode('append').foreachBatch(save_hour)
         .option('checkpointLocation', HDFS+CHECKPOINT_ROOT+'/s2_hourly')
         .queryName('rbd-s2-hourly').trigger(processingTime='15 seconds').start())
    q.awaitTermination()

if __name__ == '__main__':
    main()
