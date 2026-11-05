"""ERA5-Land hourly replay: NetCDF in HDFS -> Kafka; event_time is UTC hour start.
Accumulation for hour h uses valid_time h+1 (end of accumulation interval).
For h=00 the 01 value is the first hour; for other hours subtract value at h.
The last hour of a month requires 00 of the following month; fail if absent.
"""
import argparse
import json
import time
from datetime import datetime, timedelta, timezone
import numpy as np
from kafka import KafkaProducer
from config import KAFKA, TOPIC
from hdfs_io import read_nc
from kafka_setup import ensure_topic

RAW = '/rbd/raw/era5_land'
FIELDS = ('t2m', 'd2m', 'skt', 'stl1', 'u10', 'v10', 'swvl1')

def month_path(t, kind):
    return f'{RAW}/{t.year}/era5_land_{t.year}-{t.month:02d}_{kind}.nc'

def parse_utc(value):
    value = value.replace('Z', '+00:00')
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    if dt.minute or dt.second or dt.microsecond:
        raise ValueError('Time must be aligned to whole UTC hours')
    return dt

def hourly_increment(cur, nxt, hour, *, tiny_tolerance=1e-3):
    """Return accumulated field increment. At 00 UTC next 01 starts new cycle.
    tiny_tolerance is in ORIGINAL units, so use more precise thresholds at call site.
    """
    out = nxt if hour == 0 else nxt - cur
    if np.nanmin(out) < -tiny_tolerance:
        raise ValueError(f'Accumulation decreased by {float(np.nanmin(out))}; check hour alignment/data')
    return np.maximum(out, 0)

class MonthCache:
    def __init__(self):
        self.month = None
        self.ins = self.acc = self.next_acc = None
        self.clock = self.acc_clock = None
        self.next_clock = None
    def load(self, when):
        key = (when.year, when.month)
        if key == self.month:
            return
        self.close()
        self.ins = read_nc(month_path(when, 'instant'))
        self.acc = read_nc(month_path(when, 'accum'))
        self.clock = {str(v.astype('datetime64[h]')): i for i, v in enumerate(self.ins.valid_time.values)}
        self.acc_clock = {str(v.astype('datetime64[h]')): i for i, v in enumerate(self.acc.valid_time.values)}
        self.month = key
        self.next_acc = self.next_clock = None
    def get_acc(self, when):
        key = str(np.datetime64(when.replace(tzinfo=None), 'h'))
        if (when.year, when.month) == self.month:
            return self.acc, self.acc_clock[key]
        if self.next_acc is None:
            self.next_acc = read_nc(month_path(when, 'accum'))
            self.next_clock = {str(v.astype('datetime64[h]')): i for i, v in enumerate(self.next_acc.valid_time.values)}
        return self.next_acc, self.next_clock[key]
    def close(self):
        for obj in (self.ins, self.acc, self.next_acc):
            if obj is not None:
                obj.close()
        self.month = None

def replay(start, end, speed=1.0, max_points=None):
    """Inclusive UTC start/end; repeated runs append duplicates (consumer deduplicates)."""
    if end < start:
        raise ValueError('end < start')
    ensure_topic()
    p = KafkaProducer(bootstrap_servers=KAFKA.split(','),
                      key_serializer=lambda s: s.encode(),
                      value_serializer=lambda v: json.dumps(v, allow_nan=False, separators=(',', ':')).encode(),
                      compression_type='gzip', acks='all', retries=5, linger_ms=30,
                      max_in_flight_requests_per_connection=1)
    cache = MonthCache()
    t = start
    try:
        while t <= end:
            begun = time.monotonic()
            cache.load(t)
            tkey = str(np.datetime64(t.replace(tzinfo=None), 'h'))
            idx = cache.clock[tkey]
            next_time = t + timedelta(hours=1)
            acc_next, ni = cache.get_acc(next_time)
            hour = t.hour
            current = None if hour == 0 else cache.acc
            ci = None if hour == 0 else cache.acc_clock[tkey]
            def inc(var, tolerance):
                next_arr = acc_next[var].isel(valid_time=ni).values.astype(np.float64)
                prev_arr = np.zeros_like(next_arr) if hour == 0 else current[var].isel(valid_time=ci).values.astype(np.float64)
                return hourly_increment(prev_arr, next_arr, hour, tiny_tolerance=tolerance)
            weather = {x: cache.ins[x].isel(valid_time=idx).values for x in FIELDS}
            weather['tp_mm'] = inc('tp', 1e-5) * 1000
            weather['ssrd_j_m2'] = inc('ssrd', 50.0)
            lat = cache.ins.latitude.values
            lon = cache.ins.longitude.values
            ny, nx = len(lat), len(lon)
            n = 0
            for y in range(ny):
                for x in range(nx):
                    if max_points is not None and n >= max_points:
                        break
                    values = {k: float(v[y, x]) for k, v in weather.items()}
                    if not all(np.isfinite(v) for v in values.values()):
                        continue
                    record = dict(event_time=t.strftime('%Y-%m-%dT%H:%M:%SZ'), grid_id=y * nx + x,
                                  latitude=round(float(lat[y]), 1), longitude=round(float(lon[x]), 1),
                                  t2m_c=values['t2m']-273.15, d2m_c=values['d2m']-273.15,
                                  skt_c=values['skt']-273.15, stl1_c=values['stl1']-273.15,
                                  u10=values['u10'], v10=values['v10'], swvl1=values['swvl1'],
                                  tp_mm=values['tp_mm'], ssrd_j_m2=values['ssrd_j_m2'])
                    p.send(TOPIC, key=str(record['grid_id']), value=record)
                    n += 1
                if max_points is not None and n >= max_points:
                    break
            p.flush()  # delivery errors raise, do not falsely claim success
            print(t.isoformat(), n, 'records', flush=True)
            t += timedelta(hours=1)
            if speed > 0:
                time.sleep(max(0, 1.0/speed - (time.monotonic()-begun)))
    finally:
        cache.close()
        p.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', required=True, help='2018-04-01T00:00')
    parser.add_argument('--end', required=True, help='2018-04-03T23:00 (inclusive)')
    parser.add_argument('--speed', type=float, default=1.0, help='simulated hours per wall-clock second; 0=no pacing')
    parser.add_argument('--max-points', type=int, default=None, help='small smoke test only')
    a = parser.parse_args()
    replay(parse_utc(a.start), parse_utc(a.end), a.speed, a.max_points)

if __name__ == '__main__':
    main()
