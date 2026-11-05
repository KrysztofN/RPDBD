"""Deterministic domain logic, independent of Spark/Kafka/Cassandra.

UTC days; hourly precipitation/radiation belong to the interval START, not its end.
Each county is one state key, its points are processed together. No future-day ET0.
"""
import math
from datetime import datetime, timedelta, timezone
from S1_S2.common import digest


def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc)


def dekad(date):
    return (date.month - 1) * 3 + min((date.day - 1) // 10, 2) + 1


def et0(rows, lat, elevation):
    """Daily FAO-56 Penman–Monteith, mm/day; 24 hourly interval-aligned rows."""
    def es(t):
        return .6108 * math.exp(17.27 * t / (t + 237.3))
    tmin, tmax = min(r['t2m'] for r in rows), max(r['t2m'] for r in rows)
    tm = (tmin + tmax) / 2
    ea = sum(es(r['d2m']) for r in rows) / len(rows)
    wind = sum(math.hypot(r['u10'], r['v10']) for r in rows) / len(rows) * .748
    rs = sum(r['ssrd'] for r in rows) / 1e6
    doy = utc(rows[0]['ts']).timetuple().tm_yday
    phi = math.radians(lat)
    dec = .409 * math.sin(2 * math.pi / 365 * doy - 1.39)
    ws = math.acos(max(-1, min(1, -math.tan(phi) * math.tan(dec))))
    ra = 24 * 60 / math.pi * .0820 * (1 + .033 * math.cos(2 * math.pi / 365 * doy)) * (
        ws * math.sin(phi) * math.sin(dec) + math.cos(phi) * math.cos(dec) * math.sin(ws))
    rso = (.75 + 2e-5 * elevation) * ra
    rnl = 4.903e-9 * ((tmax + 273.16)**4 + (tmin + 273.16)**4) / 2 * (
        .34 - .14 * math.sqrt(max(ea, 0))) * (1.35 * min(1, max(0, rs / max(rso, 1e-6))) - .35)
    delta = 4098 * es(tm) / (tm + 237.3)**2
    gamma = .000665 * 101.3 * ((293 - .0065 * elevation) / 293)**5.26
    return max(0, (.408 * delta * (.77 * rs - rnl) + gamma * 900 / (tm + 273) * wind * (
        (es(tmin) + es(tmax)) / 2 - ea)) / (delta + gamma * (1 + .34 * wind)))


def validate(e, points, start, end, run):
    if not isinstance(e, dict):
        return 'not_object'
    if e.get('run_id') != run or e.get('schema_version') != 1:
        return 'run_or_schema'
    ts = e.get('ts')
    if type(ts) is not int or ts % 3600 or not start <= ts < end:
        return 'event_time'
    pid = str(e.get('point_id'))
    if pid not in points or e.get('region_id') != points[pid]['region_id']:
        return 'unknown_point_or_region'
    bounds = {'t2m': (-90, 65), 'd2m': (-100, 65), 'skt': (-100, 90), 'stl1': (-80, 65),
              'swvl1': (0, 1), 'tp': (-1e-5, 250), 'ssrd': (-1, 6e6), 'u10': (-100, 100), 'v10': (-100, 100)}
    for field, (lo, hi) in bounds.items():
        x = e.get(field)
        if isinstance(x, bool) or not isinstance(x, (float, int)) or not math.isfinite(x) or not lo <= x <= hi:
            return f'invalid_{field}'
    if e['d2m'] > e['t2m'] + 2:
        return 'dewpoint_above_temperature'
    return None


def county_level(statuses, expected, cfg):
    valid = [s for s in statuses if s != 'unknown']
    if expected < cfg['min_points'] or len(valid) / max(expected, 1) < cfg['min_coverage']:
        return 'unknown'
    # Missing points never shrink the denominator and inflate the alarm share.
    if statuses.count('alarm') / expected >= cfg['alarm_share']:
        return 'alarm'
    if sum(s in ('warning', 'alarm') for s in statuses) / expected >= cfg['warning_share']:
        return 'warning'
    return 'ok'


def initial_state():
    return {'pending': {}, 'last_hour': None, 'days': {}, 'history': {}, 'frost': {}, 'levels': {}}


def result(state, run, region, process, crop, ts, statuses, expected, cfg, metrics):
    level = county_level(statuses, expected, cfg)
    key = process + ':' + crop
    previous = state['levels'].get(key, 'unknown')
    state['levels'][key] = level
    doc = dict(run_id=run, region_id=region, process=process, crop=crop, ts=ts,
               severity=level, previous_severity=previous, rule_version=cfg['rule_version'],
               expected_points=expected, valid_points=sum(s != 'unknown' for s in statuses),
               alarm_points=statuses.count('alarm'), warning_points=statuses.count('warning'), metrics=metrics,
               notify=level != previous and (level in ('warning', 'alarm') or previous in ('warning', 'alarm')))
    doc['event_id'] = digest([run, region, process, crop, ts, cfg['rule_version']])
    return doc


def finish_day(state, day, refs, norms, cfg, run, region):
    statuses, balances = [], []
    n = cfg['s1']['window_days']
    for pid, ref in refs.items():
        rows = state['days'].get(pid, [])
        valid = len(rows) == 24 and len({r['ts'] for r in rows}) == 24
        hist = state['history'].setdefault(pid, [])
        if valid:
            balance = sum(max(r['tp'], 0) for r in rows) - et0(rows, ref['lat'], ref['elevation_m'])
            soil = sum(r['swvl1'] for r in rows) / 24
            hist.append([day, balance])
        else:
            soil = None
            hist.append([day, None])
        hist[:] = [r for r in hist if r[0] >= day - (n - 1) * 86400]
        norm = norms.get(f'{pid}:{dekad(utc(day))}')
        full = len(hist) == n and all(r[1] is not None for r in hist) and hist[0][0] == day - (n - 1) * 86400
        if not full or not norm or soil is None:
            statuses.append('unknown')
            continue
        kbw = sum(r[1] for r in hist)
        balances.append(kbw)
        if kbw < cfg['s1']['alarm_kbw_mm'] and soil < norm['p10']:
            statuses.append('alarm')
        elif kbw < cfg['s1']['warning_kbw_mm'] or soil < norm['p20']:
            statuses.append('warning')
        else:
            statuses.append('ok')
    state['days'] = {}
    if cfg['s1']['season_start'] <= utc(day).strftime('%m-%d') <= cfg['s1']['season_end']:
        return result(state, run, region, 'S1', 'all', day, statuses, len(refs), cfg,
                      {'kbw60_mean_mm': sum(balances) / len(balances) if balances else None,
                       'soil_layer': '0-7cm', 'window_days': n})
    return None


def advance(state, records, watermark, refs, norms, cfg, run, region, end):
    """Buffer out-of-order hours; finalize strictly before the event-time watermark.

    Called even with no rows at event-time timeout. Returns deterministic results.
    Identical duplicates are ignored; conflicting duplicates fail instead of silently choosing.
    """
    for e in records:
        ts = e['ts']
        if ts <= (state['last_hour'] if state['last_hour'] is not None else -1):
            continue
        hour = state['pending'].setdefault(str(ts), {})
        old = hour.get(str(e['point_id']))
        if old is not None and old != e:
            raise ValueError(f'Conflicting duplicate {e["point_id"]}@{ts}')
        hour[str(e['point_id'])] = e
    if state['last_hour'] is None:
        if not state['pending']:
            return []
        state['last_hour'] = min(map(int, state['pending'])) - 3600
    outputs = []
    # Bounded by the immutable run end; heartbeat never fabricates observations beyond end.
    while state['last_hour'] + 3600 < min(watermark, end):
        ts = state['last_hour'] + 3600
        events = state['pending'].pop(str(ts), {})
        for pid, e in events.items():
            state['days'].setdefault(pid, []).append(e)
        if utc(ts).month in cfg['s2']['months']:
            for crop, threshold in cfg['s2']['thresholds'].items():
                statuses, temperatures, soil_temperatures = [], [], []
                for pid in refs:
                    key = f'{pid}:{crop}'
                    e = events.get(pid)
                    # Continuity is across midnight; missing hours reset the run.
                    temp = e['t2m'] + cfg['s2']['temperature_correction'] if e else None
                    count = state['frost'].get(key, 0) + 1 if temp is not None and temp < threshold else 0
                    state['frost'][key] = count
                    if temp is None:
                        statuses.append('unknown')
                    else:
                        temperatures.append(temp)
                        soil_temperatures.append(e['stl1'])
                        statuses.append('alarm' if count >= cfg['s2']['alarm_hours'] else
                                        'warning' if count >= cfg['s2']['warning_hours'] else 'ok')
                outputs.append(result(state, run, region, 'S2', crop, ts, statuses, len(refs), cfg,
                                      {'estimated_ground_min_c': min(temperatures) if temperatures else None,
                                       'soil_min_c': min(soil_temperatures) if soil_temperatures else None,
                                       'threshold_c': threshold, 'alarm_consecutive_hours': cfg['s2']['alarm_hours'],
                                       'crop_presence_verified': False}))
        else:
            state['frost'] = {}
        if ts % 86400 == 23 * 3600:
            output = finish_day(state, ts - 23 * 3600, refs, norms, cfg, run, region)
            if output:
                outputs.append(output)
        state['last_hour'] = ts
    return outputs
