"""Spark 4.0.4 Structured Streaming: finalized county incidents from Kafka alerts.detected.

Separate stateful session queries for S1 and S2; Spark HDFS checkpoints own
session state. This program never reads ClickHouse to decide if an incident exists.
S1 input must be water_deficit signals (not soil_moisture_low); S2 input frost.
"""
from __future__ import annotations

import os
from pyspark.sql import functions as F, types as T
from config import KAFKA, HDFS, CHECKPOINT_ROOT
from stream_common import make_spark, ch_client

IN_TOPIC = os.getenv('INCIDENTS_INPUT_TOPIC', 'alerts.detected')
OUT_TOPIC = os.getenv('INCIDENTS_OUTPUT_TOPIC', 's1s2.incidents.v1')
CH_HISTORY = os.getenv('INCIDENTS_HISTORY_ENABLED', '1') == '1'

SCHEMA = T.StructType([
    T.StructField('event_id', T.StringType()),
    T.StructField('source', T.StringType()),
    T.StructField('alert_type', T.StringType()),
    T.StructField('region_id', T.StringType()),
    T.StructField('event_time', T.StringType()),
    T.StructField('crop_group', T.StringType()),
    T.StructField('value', T.DoubleType()),
    T.StructField('threshold', T.DoubleType()),
    T.StructField('simulation', T.BooleanType()),
])


def ensure_history():
    if not CH_HISTORY:
        return
    c = ch_client()
    try:
        c.command('''CREATE TABLE IF NOT EXISTS rbd.s1s2_incident_history (
            event_id String, source LowCardinality(String), region_id String,
            start_at DateTime('UTC'), end_at DateTime('UTC'),
            duration_units UInt32, min_value Float64,
            crop_groups Array(String), simulation UInt8,
            version UInt64
        ) ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMM(start_at) ORDER BY (source,region_id,event_id)''')
    finally:
        c.close()


def signals(spark):
    raw = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', KAFKA)
           .option('subscribe', IN_TOPIC)
           .option('startingOffsets', os.getenv('INCIDENTS_START_OFFSETS', 'latest'))
           .option('maxOffsetsPerTrigger', int(os.getenv('INCIDENTS_MAX_OFFSETS', '100000')))
           .load())
    data = raw.select(F.from_json(F.col('value').cast('string'), SCHEMA).alias('v')).select('v.*')
    return (data.withColumn('event_ts', F.to_timestamp('event_time', "yyyy-MM-dd'T'HH:mm:ssX"))
            .filter(F.col('event_ts').isNotNull() & F.col('region_id').rlike('^[0-9]{4}$')))


def aggregate(df, source):
    if source == 'S2':
        sub = df.filter((F.col('source') == 'S2') & (F.col('alert_type') == 'frost'))
        gap, wm, unit, label = '90 minutes', '3 hours', 3600, 'hours'
    else:
        sub = df.filter((F.col('source') == 'S1') & (F.col('alert_type') == 'water_deficit'))
        gap, wm, unit, label = '36 hours', '2 days', 86400, 'days'
    # One state key per county across crop groups; overlapping crop alerts are merged.
    # Watermark closes the session. Output is FINAL/closed incidents only.
    sessions = (sub.withWatermark('event_ts', wm)
        .groupBy('region_id', F.session_window('event_ts', gap).alias('session'))
        .agg(F.min('value').alias('minimum'),
             F.collect_set('crop_group').alias('crops_raw'),
             F.max(F.col('simulation').cast('int')).alias('simulation')))
    if source == 'S2':
        end_expr = F.expr('session.end - INTERVAL 90 MINUTES + INTERVAL 1 HOUR')
    else:
        end_expr = F.expr('session.end - INTERVAL 36 HOURS + INTERVAL 1 DAY')
    incidents = (sessions
        .select('region_id', F.col('session.start').alias('start_at'),
                end_expr.alias('end_at'), 'minimum',
                F.array_sort(F.expr('filter(crops_raw, x -> x is not null)')).alias('crops'),
                'simulation')
        .withColumn('duration', ((F.unix_timestamp('end_at') - F.unix_timestamp('start_at')) / F.lit(unit)).cast('int'))
        .filter(F.col('duration') >= F.lit(2 if source == 'S2' else 1))
        .withColumn('source', F.lit(source))
        .withColumn('state', F.lit('closed'))
        .withColumn('event_id', F.sha2(F.concat_ws('|', 'source', 'region_id',
                   F.date_format('start_at', "yyyy-MM-dd'T'HH:mm:ss")), 256))
        .withColumn('start', F.date_format('start_at', "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn('end', F.date_format('end_at', "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn('historical', F.col('simulation').cast('boolean')))
    return incidents


def sink(batch, batch_id, source):
    if batch.isEmpty():
        return
    # Persist one completed micro-batch in Kafka. Retry may duplicate; event_id is stable
    # and relay uses durable delivery deduplication per recipient.
    outgoing = batch.select(
        F.col('region_id').cast('string').alias('key'),
        F.to_json(F.struct('event_id', 'source', 'region_id', 'state',
                           'start', 'end', 'duration', 'minimum', 'crops', 'historical')).alias('value'))
    (outgoing.write.format('kafka')
       .option('kafka.bootstrap.servers', KAFKA)
       .option('topic', OUT_TOPIC).save())
    # History is a secondary sink. Kafka is the delivery system.
    if CH_HISTORY:
        import time
        ver = time.time_ns()
        rows = [(r.event_id, r.source, r.region_id, r.start_at, r.end_at,
                 int(r.duration), float(r.minimum), list(r.crops or []),
                 int(bool(r.historical)), ver) for r in batch.toLocalIterator()]
        c = ch_client()
        try:
            c.insert('s1s2_incident_history', rows,
                column_names=['event_id','source','region_id','start_at','end_at',
                  'duration_units','min_value','crop_groups','simulation','version'])
        finally:
            c.close()
    print(f'INCIDENTS {source} batch={batch_id} published to {OUT_TOPIC}', flush=True)


def main():
    ensure_history()
    spark = make_spark('rbd-s1s2-incidents')
    base = signals(spark)
    for source in ('S1', 'S2'):
        q = (aggregate(base, source).writeStream.outputMode('append')
             .foreachBatch(lambda batch, batch_id, source=source: sink(batch, batch_id, source))
             .option('checkpointLocation', HDFS+CHECKPOINT_ROOT+'/incident_sessions_v1/'+source.lower())
             .queryName('rbd-incidents-'+source.lower())
             .trigger(processingTime='20 seconds').start())
        print('Started:', q.name, flush=True)
    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    main()
