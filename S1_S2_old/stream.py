"""Spark Structured Streaming: event-time state by county, checkpointed on HDFS."""
import argparse
import json
from S1_S2.common import BOOTSTRAP, HDFS, ROOT, config, digest, topic
from S1_S2.domain import advance, initial_state, validate
from S1_S2.replay import epoch


def state_function(refs, norms, cfg, run, start, end):
    def process(key, frames, state):
        import pandas as pd
        region = key[0]
        county_refs = {k: v for k, v in refs.items() if v['region_id'] == region}
        current = json.loads(state.get[0]) if state.exists else initial_state()
        if current['last_hour'] is None:
            current['last_hour'] = start - 3600
        incoming = []
        for frame in frames:
            for payload in frame['payload']:
                e = json.loads(payload)
                if e['kind'] == 'weather':
                    incoming.append(e)
        watermark = state.getCurrentWatermarkMs() // 1000
        docs = advance(current, incoming, watermark, county_refs, norms, cfg, run, region, end)
        state.update((json.dumps(current, allow_nan=False),))
        if current['last_hour'] + 3600 < end:
            state.setTimeoutTimestamp(max((current['last_hour'] + 3601)*1000,
                                          state.getCurrentWatermarkMs() + 1))
        rows = [(region, json.dumps(doc, allow_nan=False)) for doc in docs]
        yield pd.DataFrame(rows, columns=['key', 'value'])
    return process


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run', required=True)
    p.add_argument('--dataset', required=True)
    p.add_argument('--start', required=True)
    p.add_argument('--end', required=True)
    args = p.parse_args()
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout
    spark = SparkSession.builder.appName('S12-stream').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    spark.conf.set('spark.sql.shuffle.partitions', '12')
    cfg = config()
    start, end = epoch(args.start), epoch(args.end)
    if start >= end or int(args.start[:4]) <= cfg['baseline_end']:
        raise ValueError('Run must be after baseline; start < end')
    base = HDFS + ROOT + '/datasets/' + args.dataset
    refs = {r.point_id: r.asDict() for r in spark.read.parquet(base + '/points').collect()}
    norms = {f'{r.point_id}:{r.dekad}': {'p10':r.p10,'p20':r.p20} for r in spark.read.parquet(base + '/norms').collect()}
    baseline = json.loads(spark.read.json(base + '/baseline_config').first().config)
    for name in ('baseline_start', 'baseline_end', 'min_baseline_days'):
        if baseline[name] != cfg[name]:
            raise ValueError(f'Norms mismatch: {name}')
    regions = {v['region_id'] for v in refs.values()}
    checkpoint = HDFS + ROOT + '/runs/' + args.run
    manifest = digest(dict(config=cfg, refs=refs, norms=norms, start=start, end=end, dataset=args.dataset))
    # Fail on reusing checkpoint with different rules, references, or dates.
    from lib.webhdfs import WebHDFS
    h = WebHDFS()
    manifest_path = ROOT + '/runs/' + args.run + '/manifest'
    if h.exists(manifest_path):
        old = spark.read.text(checkpoint + '/manifest').first().value
        if old != manifest:
            raise ValueError('Run configuration changed: choose a NEW run_id and NEW topics/checkpoint')
    else:
        spark.createDataFrame([(manifest,)], ['value']).coalesce(1).write.mode('errorifexists').text(checkpoint + '/manifest')

    @F.udf('region_id string, ts long, payload string, error string')
    def parse(raw):
        try:
            e = json.loads(raw)
            if isinstance(e, dict) and e.get('kind') == 'clock':
                good = (e.get('run_id') == args.run and e.get('schema_version') == 1 and
                        e.get('region_id') in regions and type(e.get('ts')) is int and e['ts'] % 3600 == 0 and
                        start <= e['ts'] < end + (cfg['watermark_hours'] + 3)*3600)
                error = None if good else 'invalid_clock'
            else:
                error = validate(e, refs, start, end, args.run)
                if not error and e.get('kind') != 'weather':
                    error = 'invalid_kind'
            if error:
                return ('invalid', None, raw, error)
            return (e['region_id'], e['ts'], json.dumps(e, sort_keys=True), None)
        except (ValueError, TypeError, KeyError):
            return ('invalid', None, raw, 'invalid_json')

    source = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', BOOTSTRAP)
              .option('subscribe', topic(args.run, 'weather')).option('startingOffsets', 'earliest')
              .option('failOnDataLoss', 'true').option('maxOffsetsPerTrigger', 100000).load())
    parsed = source.select(parse(F.col('value').cast('string')).alias('e'), 'partition', 'offset').select('e.*','partition','offset')
    bad = parsed.where('error IS NOT NULL').select(F.lit('invalid').alias('key'),
             F.to_json(F.struct('error','payload','partition','offset')).alias('value'))
    dlq = (bad.writeStream.format('kafka').option('kafka.bootstrap.servers', BOOTSTRAP)
           .option('topic', topic(args.run, 'dlq')).option('checkpointLocation', checkpoint + '/dlq')
           .queryName('s12-quality').start())
    good = (parsed.where('error IS NULL').withColumn('event_time', F.timestamp_seconds('ts'))
            .withWatermark('event_time', f"{cfg['watermark_hours']} hours"))
    output = good.groupBy('region_id').applyInPandasWithState(
        state_function(refs, norms, cfg, args.run, start, end),
        outputStructType='key string, value string', stateStructType='json string',
        outputMode='Append', timeoutConf=GroupStateTimeout.EventTimeTimeout)
    query = (output.writeStream.format('kafka').option('kafka.bootstrap.servers', BOOTSTRAP)
             .option('topic', topic(args.run, 'results')).option('checkpointLocation', checkpoint + '/state')
             .queryName('s12-state').trigger(processingTime='5 seconds').start())
    # Both queries must stay alive. awaitAnyTermination propagates failures.
    spark.streams.awaitAnyTermination()
    dlq.stop()
    query.stop()


if __name__ == '__main__':
    main()
