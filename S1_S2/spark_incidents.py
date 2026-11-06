"""Stateful S1/S2 county incidents, Spark 4.0.4.

Kafka alerts.detected -> s1s2.incidents.v1 (OPEN, UPDATE, CLOSED).
One state per (source, county), stored in Spark/HDFS checkpoint.
ClickHouse is only a secondary sink for finalized (CLOSED) incidents.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

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

OUTPUT = T.StructType([
    T.StructField('event_id', T.StringType(), False),
    T.StructField('source', T.StringType(), False),
    T.StructField('region_id', T.StringType(), False),
    T.StructField('state', T.StringType(), False),
    T.StructField('start', T.StringType(), False),
    T.StructField('end', T.StringType(), False),
    T.StructField('duration', T.IntegerType(), False),
    T.StructField('minimum', T.DoubleType(), False),
    T.StructField('crops', T.ArrayType(T.StringType()), False),
    T.StructField('historical', T.BooleanType(), False),
])
STATE = T.StructType([
    T.StructField('start_ms', T.LongType(), False),
    T.StructField('last_ms', T.LongType(), False),
    T.StructField('minimum', T.DoubleType(), False),
    T.StructField('crops_json', T.StringType(), False),
    T.StructField('historical', T.BooleanType(), False),
])
OUTPUT_COLUMNS = [f.name for f in OUTPUT.fields]


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
           .option('startingOffsets', os.getenv('INCIDENTS_START_OFFSETS', 'earliest'))
           .option('maxOffsetsPerTrigger', int(os.getenv('INCIDENTS_MAX_OFFSETS', '100000')))
           .load())
    data = raw.select(F.from_json(F.col('value').cast('string'), SCHEMA).alias('v')).select('v.*')
    return (data.withColumn('event_ts', F.to_timestamp('event_time', "yyyy-MM-dd'T'HH:mm:ssX"))
            .filter(F.col('event_ts').isNotNull()
                    & F.col('region_id').rlike('^[0-9]{4}$')
                    & F.col('value').isNotNull()))


def utc_ms(value):
    """Spark TIMESTAMP -> pandas Timestamp or Python datetime -> UTC epoch ms."""
    if hasattr(value, 'to_pydatetime'):
        value = value.to_pydatetime()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def dt_ms(value):
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def iso(value):
    return dt_ms(value).strftime('%Y-%m-%dT%H:%M:%SZ')


def stateful_incidents(df, source):
    """Emit OPEN with first alert, UPDATE on subsequent time steps, CLOSED on event-time gap.

    A missing normal/non-alert signal cannot close the state immediately; an event-time
    watermark from further alerts eventually expires the state.
    """
    if source == 'S1':
        sub = df.filter((F.col('source') == 'S1') & (F.col('alert_type') == 'water_deficit'))
        gap_ms, unit_ms, watermark = 36 * 3600000, 86400000, '2 days'
    else:
        sub = df.filter((F.col('source') == 'S2') & (F.col('alert_type') == 'frost'))
        gap_ms, unit_ms, watermark = 90 * 60000, 3600000, '3 hours'

    def processor(key, batches, state):
        import pandas as pd

        region = str(key[0])

        def event_id(start_ms):
            first = dt_ms(start_ms).strftime('%Y-%m-%dT%H:%M:%S')
            return hashlib.sha256(f'{source}|{region}|{first}'.encode()).hexdigest()

        def make_event(status, record):
            start_ms, last_ms, minimum, crops_json, historical = record
            # Last observation describes one full daily or hourly window.
            end_ms = last_ms + unit_ms
            duration = max(1, int(round((end_ms - start_ms) / unit_ms)))
            return (event_id(start_ms), source, region, status,
                    iso(start_ms), iso(end_ms), duration, float(minimum),
                    sorted(json.loads(crops_json)), bool(historical))

        emitted = []
        if state.hasTimedOut:
            if state.exists:
                emitted.append(make_event('closed', state.get))
                state.remove()
            if emitted:
                yield pd.DataFrame(emitted, columns=OUTPUT_COLUMNS)
            return

        record = tuple(state.get) if state.exists else None
        # Group equal timestamps before triggering OPEN so S2 initial event includes
        # all known crop groups for the same county/time.
        frames = [p for p in batches if not p.empty]
        if not frames:
            return
        all_rows = pd.concat(frames, ignore_index=True)
        all_rows['ts_ms'] = all_rows['event_ts'].map(utc_ms)
        all_rows.sort_values('ts_ms', kind='stable', inplace=True)
        for stamp, at_time in all_rows.groupby('ts_ms', sort=True):
            stamp = int(stamp)
            values = [float(v) for v in at_time['value'] if pd.notna(v)]
            if not values:
                continue
            minimum = min(values)
            crops = sorted({str(x) for x in at_time['crop_group'] if pd.notna(x) and str(x)})
            historical = bool(at_time['simulation'].fillna(False).any())

            if record is not None and stamp < record[1]:
                # Old/out-of-order signal: never reopen or corrupt a newer state.
                continue
            if record is not None and stamp - record[1] > gap_ms:
                emitted.append(make_event('closed', record))
                record = None
            if record is None:
                record = (stamp, stamp, minimum, json.dumps(crops), historical)
                emitted.append(make_event('open', record))
            else:
                old_crops = set(json.loads(record[3]))
                updated = (record[0], max(record[1], stamp), min(record[2], minimum),
                           json.dumps(sorted(old_crops.union(crops))), record[4] or historical)
                if updated != record:
                    record = updated
                    emitted.append(make_event('update', record))

        if record is not None:
            state.update(record)
            state.setTimeoutTimestamp(int(record[1] + gap_ms))
        if emitted:
            yield pd.DataFrame(emitted, columns=OUTPUT_COLUMNS)

    return (sub.withWatermark('event_ts', watermark)
            .groupBy('region_id')
            .applyInPandasWithState(
                processor, outputStructType=OUTPUT, stateStructType=STATE,
                outputMode='Append', timeoutConf='EventTimeTimeout'))


def sink(batch, batch_id, source):
    if batch.isEmpty():
        return
    # Kafka is primary; retries can duplicate messages, bot deduplicates OPEN by event_id.
    outgoing = batch.select(
        F.col('region_id').cast('string').alias('key'),
        F.to_json(F.struct(*OUTPUT_COLUMNS)).alias('value'))
    (outgoing.write.format('kafka')
     .option('kafka.bootstrap.servers', KAFKA)
     .option('topic', OUT_TOPIC).save())

    # Existing CH table represents completed incidents only; no changes to Grafana schema.
    if CH_HISTORY:
        import time
        finalized = batch.filter(F.col('state') == 'closed')
        if not finalized.isEmpty():
            ver = time.time_ns()
            rows = []
            for r in finalized.toLocalIterator():
                start = datetime.strptime(r.start, '%Y-%m-%dT%H:%M:%SZ')
                end = datetime.strptime(r.end, '%Y-%m-%dT%H:%M:%SZ')
                rows.append((r.event_id, r.source, r.region_id, start, end,
                             int(r.duration), float(r.minimum), list(r.crops),
                             int(bool(r.historical)), ver))
            c = ch_client()
            try:
                c.insert('s1s2_incident_history', rows,
                         column_names=['event_id', 'source', 'region_id', 'start_at',
                                       'end_at', 'duration_units', 'min_value',
                                       'crop_groups', 'simulation', 'version'])
            finally:
                c.close()
    print(f'INCIDENTS {source} batch={batch_id} published to {OUT_TOPIC}', flush=True)


def main():
    ensure_history()
    spark = make_spark('rbd-s1s2-incidents')
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    base = signals(spark)
    for source in ('S1', 'S2'):
        q = (stateful_incidents(base, source).writeStream.outputMode('append')
             .foreachBatch(lambda batch, batch_id, source=source: sink(batch, batch_id, source))
             .option('checkpointLocation', HDFS + CHECKPOINT_ROOT + '/incident_states_v2/' + source.lower())
             .queryName('rbd-incidents-' + source.lower())
             .trigger(processingTime='20 seconds').start())
        print('Started:', q.name, flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
