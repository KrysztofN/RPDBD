"""
Spark: bilans wody (opad − ET₀) na siatce ERA5-Land dla Polski — do map jak w IMGW (bot: /kbw).
Krok wspólny zespołu (S1, H, B1/B2) — stąd lib/.

Co robi:
  1. driver: wybiera punkty siatki ERA5-Land (0,1°, ok. 9 km) leżące w Polsce (z buforem 0,1°, żeby mapa nie
     urywała się na granicy) i rozsyła ich listę do workerów;
  2. workery: każdy miesiąc (para plików instant + accum z HDFS) = jedno zadanie -> dzienny opad i ET₀ (FAO-56)
     w tych punktach — te same funkcje co pogoda per powiat (lib/era5_counties_spark.py);
  3. driver: skleja dni w jeden kalendarz i dla każdego punktu i 16-dniowego terminu NDVI liczy: opad, ET₀,
     bilans terminu oraz bilans z 60 dni na koniec terminu (kbw_60d — susza: < −150 mm);
  4. zapis: HDFS /rbd/clean/weather_grid_periods (Parquet, podział na lata) i ClickHouse rbd.weather_grid_periods.

Trochę teorii:
  Klimatyczny bilans wodny (KBW) = opad − parowanie. IMGW publikuje go jako mapę Polski za miesiąc; my liczymy go
  na siatce ERA5-Land za 60 dni do końca terminu — ten sam wskaźnik, którego używa biuletyn (próg suszy −150 mm,
  uproszczenie IUNG), więc mapa i biuletyn mówią to samo. Brakujący miesiąc (np. jeszcze niepobrany) = dni bez
  danych: kbw_60d liczony z dostępnych dni, a kbw_60d_days mówi, z ilu (pełny = 60).

Uruchomienie (na masterze, z katalogu z kodem; hasło ClickHouse w zmiennej):
  infra/spark/submit.sh lib/era5_kbw_grid_spark.py
  infra/spark/submit.sh lib/era5_kbw_grid_spark.py --years 2018 2018
"""
from __future__ import annotations

import argparse
import io
import tempfile
from pathlib import Path

import numpy as np

from lib.era5_counties_spark import COUNTIES, HDFS, IN_DIR, _FILE_RE, daily_grid
from lib.log import Timer, get_logger, setup_logging

log = get_logger(Path(__file__).stem)

OUT = "/rbd/clean/weather_grid_periods"
TABLE = "weather_grid_periods"
BUFFER_DEG = 0.1                     # punkty do ~1 oczka siatki za granicą — mapa bez „schodków” na brzegu
WINDOW_DAYS = 60


def poland_cells(sc, hdfs: str, sample_instant: bytes) -> dict:
    """Punkty lądowe siatki w Polsce (z buforem): indeksy w spłaszczonej siatce + ich współrzędne."""
    import geopandas as gpd
    import xarray as xr

    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "s.nc"
        f.write_bytes(sample_instant)
        ds = xr.open_dataset(f)
        lat, lon = ds["latitude"].values, ds["longitude"].values
        land = np.isfinite(ds["t2m"].isel({("valid_time" if "valid_time" in ds.dims else "time"): 0}).values)
    raw = gpd.read_file(io.BytesIO(sc.binaryFiles(f"{hdfs}{COUNTIES}").first()[1]))
    poland = raw.to_crs("EPSG:4326").union_all().buffer(BUFFER_DEG)
    lon2, lat2 = np.meshgrid(lon, lat)
    inside = gpd.points_from_xy(lon2.ravel(), lat2.ravel()).within(poland) & land.ravel()
    cells = np.flatnonzero(inside)
    log.info("siatka ERA5-Land %dx%d, punktów w Polsce (z buforem %.1f°): %d", len(lat), len(lon), BUFFER_DEG,
             len(cells))
    return dict(shape=(len(lat), len(lon)), cells=cells, lat=lat2.ravel()[cells], lon=lon2.ravel()[cells])


def month_grid(month_key: str, files, cells_bc):
    """Worker: (rok-miesiąc, [(rodzaj, bajty)]) -> [(rok-miesiąc, dni, opad [dni × punkty], ET₀ [dni × punkty])]."""
    parts = dict(files)
    if "instant" not in parts or "accum" not in parts:
        return []                                         # miesiąc niekompletny — pominięty (dojdzie po pobraniu)
    c = cells_bc.value
    days, lat, lon, res = daily_grid(bytes(parts["instant"]), bytes(parts["accum"]))
    if (len(lat), len(lon)) != c["shape"]:
        raise ValueError(f"{month_key}: inna siatka ({len(lat)}x{len(lon)} vs {c['shape']})")
    n = len(days)
    return [(month_key, days.astype(str).tolist(),
             res["precip_mm"].reshape(n, -1)[:, c["cells"]].astype(np.float32),
             res["et0_mm"].reshape(n, -1)[:, c["cells"]].astype(np.float32))]


def period_table(months: list, cells: dict):
    """Dni z wszystkich miesięcy -> DataFrame: punkt × termin (opad, ET₀, bilans terminu, bilans 60 dni)."""
    import pandas as pd

    months = sorted(months, key=lambda m: m[0])
    days = np.concatenate([np.array(m[1], dtype="datetime64[D]") for m in months])
    cal = np.arange(days.min(), days.max() + 1, dtype="datetime64[D]")     # pełny kalendarz: luki = NaN
    pos = (days - cal[0]).astype(int)
    n = len(cells["cells"])
    precip = np.full((len(cal), n), np.nan, dtype=np.float32)
    et0 = np.full((len(cal), n), np.nan, dtype=np.float32)
    precip[pos] = np.concatenate([m[2] for m in months])
    et0[pos] = np.concatenate([m[3] for m in months])
    kbw = precip - et0

    ok = np.isfinite(kbw)
    cs = np.vstack([np.zeros((1, n)), np.cumsum(np.where(ok, kbw, 0), axis=0, dtype=np.float64)])
    cc = np.vstack([np.zeros((1, n)), np.cumsum(ok, axis=0)])
    start = np.maximum(np.arange(len(cal)) - (WINDOW_DAYS - 1), 0)
    kbw60 = cs[1:] - cs[start]                                            # suma z 60 dni (z dostępnych)
    days60 = cc[1:] - cc[start]

    years = cal.astype("datetime64[Y]").astype(int) + 1970
    doy = (cal - cal.astype("datetime64[Y]")).astype(int) + 1
    period = (doy - 1) // 16
    frames = []
    for (y, p) in sorted(set(zip(years.tolist(), period.tolist()))):
        idx = np.flatnonzero((years == y) & (period == p))
        have = ok[idx].any(axis=1)
        if not have.any():
            continue
        last = idx[have][-1]                                              # ostatni dzień terminu z danymi
        with np.errstate(all="ignore"):
            frames.append(pd.DataFrame(dict(
                year=np.uint16(y), period=np.uint8(p), date=cal[idx[0]],
                lat=cells["lat"].astype(np.float32), lon=cells["lon"].astype(np.float32),
                days=np.uint8(have.sum()),
                precip_mm=np.round(np.nansum(precip[idx], axis=0), 1).astype(np.float32),
                et0_mm=np.round(np.nansum(et0[idx], axis=0), 1).astype(np.float32),
                kbw_mm=np.round(np.nansum(kbw[idx], axis=0), 1).astype(np.float32),
                kbw_60d_mm=np.round(kbw60[last], 1).astype(np.float32),
                kbw_60d_days=days60[last].astype(np.uint8))))
    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Spark: bilans wody na siatce ERA5-Land (terminy 16-dniowe)")
    p.add_argument("--years", type=int, nargs=2, metavar=("OD", "DO"), default=(2010, 2024))
    p.add_argument("--hdfs", default=HDFS)
    p.add_argument("--no-clickhouse", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName("rbd-era5-kbw-grid")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.driver.maxResultSize", "2g").getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    y0, y1 = args.years
    fs_path = spark._jvm.org.apache.hadoop.fs.Path
    conf = sc._jsc.hadoopConfiguration()
    paths = ",".join(f"{args.hdfs}{IN_DIR}/{y}/*.nc" for y in range(y0, y1 + 1)
                     if fs_path(f"{args.hdfs}{IN_DIR}/{y}").getFileSystem(conf).exists(fs_path(f"{args.hdfs}{IN_DIR}/{y}")))
    if not paths:
        raise SystemExit(f"brak danych ERA5-Land na HDFS dla lat {y0}–{y1}")

    files = sc.binaryFiles(paths, minPartitions=48)
    with Timer(log, "punkty siatki w Polsce (driver)"):
        sample = files.filter(lambda kv: kv[0].endswith("_instant.nc")).first()[1]
        cells = poland_cells(sc, args.hdfs, bytes(sample))
        cells_bc = sc.broadcast(cells)

    def by_month(kv):
        m = _FILE_RE.search(kv[0])
        return [] if not m else [(f"{m.group(1)}-{m.group(2)}", (m.group(3), kv[1]))]

    with Timer(log, f"opad i ET₀ dzienne w punktach siatki {y0}–{y1} (workery)"):
        months = files.flatMap(by_month).groupByKey().flatMap(lambda kv: month_grid(kv[0], list(kv[1]), cells_bc)) \
            .collect()
    log.info("miesięcy kompletnych: %d (%s … %s)", len(months), min(m[0] for m in months), max(m[0] for m in months))

    with Timer(log, "terminy 16-dniowe + bilans 60 dni (driver)"):
        df = period_table(months, cells)
    full = df[df["kbw_60d_days"] == WINDOW_DAYS]
    log.info("punkty × terminy: %d (pełne 60 dni: %d); lata: %s", len(df), len(full), sorted(df["year"].unique().tolist()))
    summer = full[full["period"].between(10, 12)].groupby("year")["kbw_60d_mm"]
    log.info("VI–VII (terminy 10–12), Polska: średni bilans 60 dni / %% punktów z suszą (< −150 mm): %s",
             ", ".join(f"{y}: {s.mean():.0f} mm/{100 * (s < -150).mean():.0f}%" for y, s in summer))

    with Timer(log, f"zapis HDFS {OUT}"):
        ints = {c: "int32" for c in ("year", "period", "days", "kbw_60d_days")}   # Arrow w Sparku: bez uint
        spark.createDataFrame(df.astype(ints)).write.mode("overwrite").partitionBy("year").parquet(f"{args.hdfs}{OUT}")

    if not args.no_clickhouse:
        with Timer(log, f"zapis do ClickHouse rbd.{TABLE}"):
            from lib.clickhouse import ClickHouse

            ch = ClickHouse()
            ch.command(f"DROP TABLE IF EXISTS {TABLE}")
            ch.command(f"""
                CREATE TABLE {TABLE} (
                    year UInt16, period UInt8, date Date, lat Float32, lon Float32, days UInt8,
                    precip_mm Float32, et0_mm Float32, kbw_mm Float32, kbw_60d_mm Float32, kbw_60d_days UInt8
                ) ENGINE = MergeTree ORDER BY (year, period, lat, lon)""")
            ch.insert_df(TABLE, df)
    spark.stop()


if __name__ == "__main__":
    main()
