"""Distributed NetCDF -> interval-aligned hourly Parquet, and soil baselines.

Run through S1_S2/infra/submit.sh. Raw B1/B2 files are read-only.
"""
import argparse
import calendar
import io
import json
from datetime import datetime, timezone

from S1_S2.common import HDFS, ROOT, config


def points_command(args):
    # Reuse the source reader, without calling its main() or overwriting its CSV.
    from S1.data.points import make_grid, assign_regions, cropland_share, elevation
    from pathlib import Path
    points = assign_regions(make_grid())
    share = cropland_share()
    points['cropland_share'] = share[points['lat_idx'], points['lon_idx']]
    points = points[(points['cropland_share'] >= .2) &
                    (points['county_code'].str[-2:].astype(int) < 61)].copy()
    points['elevation_m'] = elevation(points)
    points['point_id'] = (points['lat'].mul(10).round().astype(int).astype(str) + '_' +
                          points['lon'].mul(10).round().astype(int).astype(str))
    points = points.rename(columns={'county_code': 'region_id'})
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    points[['point_id', 'region_id', 'county_name', 'lat', 'lon', 'cropland_share', 'elevation_m']].to_csv(args.out, index=False)
    print(f'{len(points)} agricultural grid points -> {args.out}')


def month_rows(job, refs):
    import numpy as np
    import pandas as pd
    import xarray as xr
    import tempfile
    from pathlib import Path
    from lib.webhdfs import WebHDFS

    year, month = job
    next_y, next_m = (year + 1, 1) if month == 12 else (year, month + 1)
    h = WebHDFS()
    def path(y, m, kind):
        return f'/rbd/raw/era5_land/{y}/era5_land_{y}-{m:02}_{kind}.nc'
    with tempfile.TemporaryDirectory() as tmp:
        def load(y, m, kind, required=True):
            source = path(y, m, kind)
            if not h.exists(source):
                if required:
                    raise FileNotFoundError(source)
                return None
            local = Path(tmp) / f'{y}-{m}-{kind}.nc'
            local.write_bytes(h.read_bytes(source))
            with xr.open_dataset(local) as ds:
                # Exact coordinates, no accidental nearest-grid substitution.
                da = ds.sel(latitude=xr.DataArray([r['lat'] for r in refs], dims='point'),
                            longitude=xr.DataArray([r['lon'] for r in refs], dims='point'), method='nearest', tolerance=1e-5)
                return da.load()
        ins, acc = load(year, month, 'instant'), load(year, month, 'accum')
        following = load(next_y, next_m, 'accum', required=False)
        expected = pd.date_range(f'{year}-{month:02}-01', periods=calendar.monthrange(year, month)[1]*24, freq='h')
        if not np.array_equal(ins.valid_time.values, expected.values) or not np.array_equal(acc.valid_time.values, expected.values):
            raise ValueError(f'Incomplete/misaligned month {year}-{month}')
        missing = {'t2m','d2m','skt','stl1','swvl1','u10','v10'} - set(ins.data_vars)
        if missing or not {'tp','ssrd'} <= set(acc.data_vars):
            raise ValueError(f'Missing variables: {missing}')
        vals = {}
        for name in ('tp', 'ssrd'):
            raw = acc[name].transpose('valid_time', 'point').values
            # Start t: interval [t,t+1h), endpoint is stored at t+1h in ERA5-Land.
            endpoint = np.concatenate([raw[1:], following[name].isel(valid_time=0).values[None, :]
                                       if following is not None else np.full((1, len(refs)), np.nan)])
            hourly = endpoint - raw
            hourly[expected.hour == 0] = endpoint[expected.hour == 0]
            vals[name] = hourly * (1000 if name == 'tp' else 1)
        for name in ('t2m','d2m','skt','stl1','swvl1','u10','v10'):
            vals[name] = ins[name].transpose('valid_time', 'point').values - (273.15 if name in ('t2m','d2m','skt','stl1') else 0)
        for ti, stamp in enumerate(expected):
            ts = int(stamp.tz_localize('UTC').timestamp())
            for pi, ref in enumerate(refs):
                values = [float(vals[name][ti, pi]) for name in FIELDS]
                # Preserve missing boundary hours as NULL for the quality pipeline.
                values = [v if np.isfinite(v) else None for v in values]
                yield (ref['point_id'], ref['region_id'], ts, year, month, int(ref['region_id']) % 3, *values)


FIELDS = ['t2m', 'd2m', 'skt', 'stl1', 'swvl1', 'u10', 'v10', 'tp', 'ssrd']


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='command', required=True)
    pts = sub.add_parser('points')
    pts.add_argument('--out', default='/app/S1_S2/local/points.csv')
    hourly = sub.add_parser('hourly')
    hourly.add_argument('--points', required=True, help='local CSV on submit host')
    hourly.add_argument('--from-month', required=True)
    hourly.add_argument('--to-month', required=True)
    hourly.add_argument('--dataset', required=True, help='new immutable dataset name, e.g. weather-v1')
    norms = sub.add_parser('norms')
    norms.add_argument('--dataset', required=True)
    args = p.parse_args()
    if args.command == 'points':
        return points_command(args)
    import pandas as pd
    from pyspark.sql import SparkSession, functions as F
    from S1_S2.common import topic
    topic(args.dataset, 'check')  # safe path component
    spark = SparkSession.builder.appName('S12-prepare').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    base = HDFS + ROOT + '/datasets/' + args.dataset
    if args.command == 'hourly':
        df = pd.read_csv(args.points, dtype={'point_id': str, 'region_id': str})
        required = {'point_id','region_id','lat','lon','elevation_m','cropland_share'}
        if not required <= set(df) or df.point_id.duplicated().any() or df[list(required)].isna().any().any():
            raise ValueError('Invalid point reference CSV')
        df['region_id'] = df.region_id.str.zfill(4)
        if not df.region_id.str.fullmatch(r'\d{4}').all():
            raise ValueError('Invalid TERYT')
        df = df[(df.cropland_share >= .2) & (df.region_id.str[-2:].astype(int) < 61)]
        refs = df.to_dict('records')
        if not refs:
            raise ValueError('No agricultural points')
        bc = spark.sparkContext.broadcast(refs)
        months = pd.period_range(args.from_month, args.to_month, freq='M')
        jobs = [(m.year, m.month) for m in months]
        schema = 'point_id string, region_id string, ts long, year int, month int, shard int, ' + ', '.join(f'{f} double' for f in FIELDS)
        rows = spark.sparkContext.parallelize(jobs, max(3, len(jobs))).flatMap(lambda job: month_rows(job, bc.value))
        spark.createDataFrame(rows, schema).write.mode('errorifexists').partitionBy('year','month','shard').parquet(base + '/hourly')
        spark.createDataFrame(df).write.mode('errorifexists').parquet(base + '/points')
    else:
        cfg = config()
        data = spark.read.parquet(base + '/hourly').where(F.col('year').between(cfg['baseline_start'], cfg['baseline_end']))
        years = {r.year for r in data.select('year').distinct().collect()}
        if years != set(range(cfg['baseline_start'], cfg['baseline_end'] + 1)):
            raise ValueError(f'Baseline years incomplete: {years}')
        daily = (data.withColumn('day', F.to_date(F.timestamp_seconds('ts')))
                 .groupBy('point_id','day').agg(F.avg('swvl1').alias('soil'), F.count('swvl1').alias('n'))
                 .where('n = 24').withColumn('dekad', (F.month('day')-1)*3 + F.least(F.floor((F.dayofmonth('day')-1)/10), F.lit(2))+1))
        out = daily.groupBy('point_id','dekad').agg(F.percentile_approx('soil', [.1,.2], 10000).alias('q'), F.count('*').alias('days'))
        out = out.where(F.col('days') >= cfg['min_baseline_days']).select('point_id', F.col('dekad').cast('int'), F.col('q')[0].alias('p10'), F.col('q')[1].alias('p20'), 'days')
        out.write.mode('errorifexists').parquet(base + '/norms')
        spark.createDataFrame([(json.dumps(cfg, sort_keys=True),)], ['config']).write.mode('errorifexists').json(base + '/baseline_config')
    spark.stop()


if __name__ == '__main__':
    main()
