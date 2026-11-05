"""Three barrier tasks = three synchronized producers on three one-core workers."""
import argparse
import itertools
import json
import time
from datetime import datetime, timezone
from S1_S2.common import HDFS, ROOT, config, producer, topic


def epoch(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp())


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run', required=True)
    p.add_argument('--dataset', required=True)
    p.add_argument('--start', required=True, help='UTC inclusive YYYY-MM-DD')
    p.add_argument('--end', required=True, help='UTC exclusive YYYY-MM-DD')
    p.add_argument('--seconds-per-hour', type=float, default=1)
    p.add_argument('--inject-errors', action='store_true')
    args = p.parse_args()
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.appName('S12-replay').getOrCreate()
    start, end = epoch(args.start), epoch(args.end)
    if start >= end or utc_year(start) <= config()['baseline_end']:
        raise ValueError('Replay must be after baseline and start < end')
    base = HDFS + ROOT + '/datasets/' + args.dataset
    refs = {r.point_id: r.asDict() for r in spark.read.parquet(base + '/points').collect()}
    counties = sorted({r['region_id'] for r in refs.values()})
    frame = spark.read.parquet(base + '/hourly').where((F.col('ts') >= start) & (F.col('ts') < end))
    cfg = config()
    # Sorting is distributed; never collect historical weather on the driver.
    ordered = (frame.rdd.map(lambda r: ((r.shard, r.ts, r.point_id), r.asDict()))
               .repartitionAndSortWithinPartitions(3, partitionFunc=lambda k: k[0]))
    def send(iterator):
        from pyspark import BarrierTaskContext
        ctx = BarrierTaskContext.get()
        shard = ctx.partitionId()
        owned = [c for c in counties if int(c) % 3 == shard]
        prod = producer()
        failures = []
        def delivery(err, msg):
            if err:
                failures.append(str(err))
        def push(doc):
            while True:
                try:
                    prod.produce(topic(args.run, 'weather'), key=doc['region_id'], value=json.dumps(doc, allow_nan=False), on_delivery=delivery)
                    break
                except BufferError:
                    prod.poll(.1)
            prod.poll(0)
        iterator = iter(iterator)
        item = next(iterator, None)
        count = 0
        # Tail clocks advance the watermark, including when no more weather arrives.
        for ts in range(start, end + (cfg['watermark_hours'] + 3)*3600, 3600):
            tick_start = time.monotonic()
            while item is not None and item[0][1] == ts:
                row = item[1]
                for name in ('year','month','shard'):
                    row.pop(name)
                row.update(run_id=args.run, schema_version=1, kind='weather')
                push(row)
                if args.inject_errors and count % 1000 == 0:
                    push(dict(row))  # duplicate, should not change output
                    push(dict(row, swvl1=9))  # invalid, should reach DLQ
                count += 1
                item = next(iterator, None)
            for county in owned:
                push(dict(run_id=args.run, schema_version=1, kind='clock', region_id=county, ts=ts))
            if prod.flush(120) or failures:
                raise RuntimeError(f'Kafka delivery failed: {failures}')
            ctx.barrier()  # a fast producer cannot run days ahead of the others
            time.sleep(max(0, args.seconds_per_hour - (time.monotonic() - tick_start)))
        yield (shard, count)
    print(ordered.barrier().mapPartitions(send).collect())
    spark.stop()


def utc_year(ts):
    return datetime.fromtimestamp(ts, timezone.utc).year


if __name__ == '__main__':
    main()
