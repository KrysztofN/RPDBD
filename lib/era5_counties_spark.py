"""
Spark: pogoda ERA5-Land per powiat — dzień po dniu i w 16-dniowych terminach NDVI (HDFS -> Parquet + ClickHouse).
Krok wspólny zespołu (S1, H, B1/B2) — stąd lib/.

Co robi:
  1. driver: mapuje punkty siatki ERA5-Land (0,1°, ok. 9 km) na powiaty (punkt w granicach; powiat bez punktu
     — zwykle mały powiat miejski — dostaje najbliższy punkt) i rozsyła to do workerów;
  2. workery: każdy miesiąc (para plików instant + accum z HDFS) = jedno zadanie -> wartości dzienne per powiat:
       precip_mm, ssrd_mj (promieniowanie), t_min/t_max/t_mean (°C), td_mean (punkt rosy), wind2_ms (wiatr na 2 m),
       swvl1 (wilgotność gleby 0–7 cm), snow_m, et0_mm (parowanie wzorcowe FAO-56);
  3. Spark SQL: terminy 16-dniowe zgodne z NDVI (period = (dzień roku - 1) // 16): suma opadu i ET₀, bilans wodny
     (kbw = opad - ET₀), bilans 60-dniowy na koniec terminu (kbw_60d — wskaźnik suszy z README: < -150 mm),
     dni upalne (t_max >= 30 °C), dni z przymrozkiem (t_min < 0 °C), średnie temperatury i wilgotność gleby;
  4. zapis: HDFS /rbd/clean/weather_counties_daily i /rbd/clean/weather_counties_periods (Parquet, podział na lata)
     oraz ClickHouse rbd.weather_periods (terminy; dane dzienne tylko na HDFS — dla S1/H, gdy będą potrzebne).

Trochę teorii:
  ERA5-Land: zmienne „narastające” (opad tp, promieniowanie ssrd) sumują się od 00 UTC; wartość o 00:00 to suma
  całego poprzedniego dnia. Dzień = doba UTC (w Polsce przesunięcie 1–2 h — bez znaczenia dla sum dobowych).
  ET₀ (FAO-56, Penman–Monteith): ile wody odparowałoby z wzorcowej trawy przy danej pogodzie — z temperatury,
  wilgotności (punkt rosy), promieniowania i wiatru. Bilans wodny opad − ET₀ < 0 = rośliny tracą więcej wody,
  niż dostają. Wysokość terenu przyjęta stała 150 m (ciśnienie w ET₀) — Polska jest głównie nizinna, błąd < 2%.

Uruchomienie (na masterze, z katalogu z kodem; hasło ClickHouse w zmiennej):
  infra/spark/submit.sh lib/era5_counties_spark.py                 # wszystkie miesiące, które są na HDFS
  infra/spark/submit.sh lib/era5_counties_spark.py --years 2018 2019
"""
from __future__ import annotations

import argparse
import io
import re
import tempfile
from pathlib import Path

import numpy as np

from lib.log import Timer, get_logger, setup_logging

log = get_logger(Path(__file__).stem)

HDFS = "hdfs://10.7.11.21:9000"
IN_DIR = "/rbd/raw/era5_land"
COUNTIES = "/rbd/ref/boundaries/counties_raw.gpkg"
OUT_DAILY = "/rbd/clean/weather_counties_daily"
OUT_PERIODS = "/rbd/clean/weather_counties_periods"
ELEVATION_M = 150.0
HOT_DAY_C, FROST_C = 30.0, 0.0
_FILE_RE = re.compile(r"era5_land_(\d{4})-(\d{2})_(instant|accum)\.nc$")

DAILY_COLS = ["precip_mm", "ssrd_mj", "t_min", "t_max", "t_mean", "td_mean", "wind2_ms", "swvl1", "snow_m", "et0_mm"]


# --------------------------------------------------------------------------- fizyka (FAO-56)
def sat_vapour_pressure(t_c):
    """Ciśnienie pary nasyconej [kPa] przy temperaturze t_c [°C] (FAO-56, wzór 11)."""
    return 0.6108 * np.exp(17.27 * t_c / (t_c + 237.3))


def et0_fao56(t_min, t_max, td_mean, rs_mj, u2, lat_deg, doy, z=ELEVATION_M):
    """
    Dzienne ET₀ [mm] (FAO-56, Penman–Monteith; wzory 6–40). Tablice o tych samych wymiarach (lat_deg i doy rozgłaszane).
    t_min/t_max/td_mean [°C], rs_mj — promieniowanie krótkofalowe docierające [MJ/m²/dzień], u2 — wiatr na 2 m [m/s].
    """
    t_mean = (t_max + t_min) / 2
    delta = 4098 * sat_vapour_pressure(t_mean) / (t_mean + 237.3) ** 2
    p = 101.3 * ((293 - 0.0065 * z) / 293) ** 5.26
    gamma = 0.000665 * p
    es = (sat_vapour_pressure(t_max) + sat_vapour_pressure(t_min)) / 2
    ea = sat_vapour_pressure(td_mean)
    # promieniowanie pozaziemskie Ra (wzory 21–25)
    phi = np.radians(lat_deg)
    dr = 1 + 0.033 * np.cos(2 * np.pi / 365 * doy)
    dec = 0.409 * np.sin(2 * np.pi / 365 * doy - 1.39)
    ws = np.arccos(np.clip(-np.tan(phi) * np.tan(dec), -1, 1))
    ra = 24 * 60 / np.pi * 0.0820 * dr * (ws * np.sin(phi) * np.sin(dec) + np.cos(phi) * np.cos(dec) * np.sin(ws))
    rso = (0.75 + 2e-5 * z) * ra
    rns = 0.77 * rs_mj
    sigma = 4.903e-9
    rnl = sigma * ((t_max + 273.16) ** 4 + (t_min + 273.16) ** 4) / 2 * (0.34 - 0.14 * np.sqrt(np.maximum(ea, 0))) \
        * (1.35 * np.clip(rs_mj / np.where(rso > 0, rso, np.nan), 0, 1) - 0.35)
    rn = rns - rnl
    et0 = (0.408 * delta * rn + gamma * 900 / (t_mean + 273) * u2 * (es - ea)) / (delta + gamma * (1 + 0.34 * u2))
    return np.maximum(et0, 0)


# --------------------------------------------------------------------------- jeden miesiąc
def daily_grid(instant_bytes: bytes, accum_bytes: bytes):
    """
    Para plików miesiąca -> (dni [datetime64[D]], lat [ny], lon [nx], dict zmienna -> tablica [dni, ny, nx]).
    """
    import xarray as xr

    with tempfile.TemporaryDirectory() as tmp:
        fi, fa = Path(tmp) / "i.nc", Path(tmp) / "a.nc"
        fi.write_bytes(instant_bytes)
        fa.write_bytes(accum_bytes)
        ins = xr.open_dataset(fi).load()
        acc = xr.open_dataset(fa).load()
    tdim = "valid_time" if "valid_time" in ins.dims else "time"
    times = ins[tdim].values.astype("datetime64[h]")
    days = np.unique(times.astype("datetime64[D]"))
    lat, lon = ins["latitude"].values, ins["longitude"].values

    def per_day(da, how):
        arr = da.values
        out = np.full((len(days),) + arr.shape[1:], np.nan, dtype=np.float32)
        for i, d in enumerate(days):
            sel = times.astype("datetime64[D]") == d
            with np.errstate(all="ignore"):
                out[i] = getattr(np, how)(arr[sel], axis=0)
        return out

    t2m = ins["t2m"] - 273.15
    res = {
        "t_min": per_day(t2m, "nanmin"), "t_max": per_day(t2m, "nanmax"), "t_mean": per_day(t2m, "nanmean"),
        "td_mean": per_day(ins["d2m"] - 273.15, "nanmean"),
        "wind2_ms": per_day(np.sqrt(ins["u10"] ** 2 + ins["v10"] ** 2) * 0.748, "nanmean"),   # 10 m -> 2 m (FAO-56)
        "swvl1": per_day(ins["swvl1"], "nanmean"),
        "snow_m": per_day(ins["sde"], "nanmean"),
    }
    # narastające: suma dnia D = wartość o 00:00 dnia D+1; ostatni dzień miesiąca: 23:00 × 24/23
    atimes = acc[tdim].values.astype("datetime64[h]")
    for var, name, scale in (("tp", "precip_mm", 1000.0), ("ssrd", "ssrd_mj", 1e-6)):
        arr = acc[var].values
        out = np.full((len(days),) + arr.shape[1:], np.nan, dtype=np.float32)
        for i, d in enumerate(days):
            nxt = np.flatnonzero(atimes == (d + 1).astype("datetime64[h]"))
            if nxt.size:
                out[i] = arr[nxt[0]] * scale
            else:
                last = np.flatnonzero(atimes == d.astype("datetime64[h]") + np.timedelta64(23, "h"))
                if last.size:
                    out[i] = arr[last[0]] * scale * 24 / 23
        res[name] = np.maximum(out, 0)
    doy = (days - days.astype("datetime64[Y]")).astype(int) + 1
    res["et0_mm"] = et0_fao56(res["t_min"], res["t_max"], res["td_mean"], res["ssrd_mj"], res["wind2_ms"],
                              lat[None, :, None], doy[:, None, None]).astype(np.float32)
    return days, lat, lon, res


def month_rows(month_key, files, zones_bc):
    """Worker: (rok-miesiąc, [(rodzaj, bajty)]) -> wiersze (region_id, date, *DAILY_COLS)."""
    parts = dict(files)
    if "instant" not in parts or "accum" not in parts:
        return []                                         # miesiąc niekompletny — pominięty (dojdzie po pobraniu)
    z = zones_bc.value
    days, lat, lon, res = daily_grid(bytes(parts["instant"]), bytes(parts["accum"]))
    if (len(lat), len(lon)) != z["shape"]:
        raise ValueError(f"{month_key}: inna siatka niż w mapie powiatów ({len(lat)}x{len(lon)} vs {z['shape']})")
    idx = z["cell_region"].ravel()                        # 0 = poza powiatami, i+1 = powiat
    n = len(z["region_ids"]) + 1
    rows = []
    for di, d in enumerate(days):
        sums = {}
        for col in DAILY_COLS:
            v = res[col][di].ravel()
            ok = np.isfinite(v) & (idx > 0)
            s = np.bincount(idx[ok], weights=v[ok], minlength=n)[1:]
            c = np.bincount(idx[ok], minlength=n)[1:]
            sums[col] = np.where(c > 0, s / np.maximum(c, 1), np.nan)
            for ci, k in z["extra"].items():              # powiat bez punktu siatki: wartość najbliższego punktu
                sums[col][ci] = v[k]
        dstr = str(d)
        for i, rid in enumerate(z["region_ids"]):
            if np.isfinite(sums["t_mean"][i]):
                rows.append((rid, dstr) + tuple(float(sums[c][i]) for c in DAILY_COLS))
    return rows


# --------------------------------------------------------------------------- driver
def build_zones(sc, hdfs: str, sample_instant: bytes) -> dict:
    """Punkty siatki ERA5-Land -> powiaty (punkt w granicach; powiat bez punktu -> najbliższy punkt)."""
    import geopandas as gpd
    import xarray as xr
    from shapely.geometry import Point

    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "s.nc"
        f.write_bytes(sample_instant)
        ds = xr.open_dataset(f)
        lat, lon = ds["latitude"].values, ds["longitude"].values
        land = np.isfinite(ds["t2m"].isel({("valid_time" if "valid_time" in ds.dims else "time"): 0}).values)
    raw = gpd.read_file(io.BytesIO(sc.binaryFiles(f"{hdfs}{COUNTIES}").first()[1]))
    counties = gpd.GeoDataFrame({"region_id": raw["JPT_KOD_JE"].astype(str)}, geometry=raw.geometry, crs=raw.crs) \
        .sort_values("region_id").reset_index(drop=True).to_crs("EPSG:4326")
    lon2, lat2 = np.meshgrid(lon, lat)
    pts = gpd.GeoDataFrame(geometry=gpd.points_from_xy(lon2.ravel(), lat2.ravel()), crs="EPSG:4326")
    pts = pts[land.ravel()]
    joined = gpd.sjoin(pts, counties.reset_index().rename(columns={"index": "ci"}), predicate="within", how="inner")
    cell_region = np.zeros(lat2.size, dtype=np.int32)
    cell_region[joined.index.to_numpy()] = joined["ci"].to_numpy() + 1
    # powiaty bez żadnego punktu: najbliższy punkt lądowy do środka powiatu (dzielony z sąsiadem — tylko do odczytu)
    covered = set(joined["ci"])
    extra = {}
    land_idx = np.flatnonzero(land.ravel())
    for ci, geom in enumerate(counties.geometry):
        if ci not in covered:
            c = geom.representative_point()
            k = land_idx[np.argmin((lon2.ravel()[land_idx] - c.x) ** 2 + (lat2.ravel()[land_idx] - c.y) ** 2)]
            extra[ci] = int(k)
    log.info("siatka ERA5-Land %dx%d, punktów lądowych %d; powiaty z punktami: %d, z najbliższym punktem: %d",
             len(lat), len(lon), int(land.sum()), len(covered), len(extra))
    return dict(shape=(len(lat), len(lon)), cell_region=cell_region.reshape(lat2.shape),
                region_ids=counties["region_id"].tolist(), extra=extra)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Spark: pogoda ERA5-Land per powiat (dzień, termin 16-dniowy)")
    p.add_argument("--years", type=int, nargs=2, metavar=("OD", "DO"), default=(2010, 2024))
    p.add_argument("--hdfs", default=HDFS)
    p.add_argument("--no-clickhouse", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    spark = (SparkSession.builder.appName("rbd-era5-counties")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    y0, y1 = args.years
    paths = ",".join(f"{args.hdfs}{IN_DIR}/{y}/*.nc" for y in range(y0, y1 + 1)
                     if spark._jvm.org.apache.hadoop.fs.Path(f"{args.hdfs}{IN_DIR}/{y}")
                     .getFileSystem(sc._jsc.hadoopConfiguration())
                     .exists(spark._jvm.org.apache.hadoop.fs.Path(f"{args.hdfs}{IN_DIR}/{y}")))
    if not paths:
        raise SystemExit(f"brak danych ERA5-Land na HDFS dla lat {y0}–{y1}")

    files = sc.binaryFiles(paths, minPartitions=48)
    with Timer(log, "mapa punktów siatki -> powiaty (driver)"):
        sample = files.filter(lambda kv: kv[0].endswith("_instant.nc")).first()[1]
        zones = build_zones(sc, args.hdfs, bytes(sample))
        if zones["extra"]:
            log.debug("powiaty z najbliższym punktem: %s", [zones["region_ids"][c] for c in zones["extra"]])
        zones_bc = sc.broadcast(zones)

    schema = StructType([StructField("region_id", StringType()), StructField("date", StringType())]
                        + [StructField(c, DoubleType()) for c in DAILY_COLS])

    def by_month(kv):
        m = _FILE_RE.search(kv[0])
        return [] if not m else [(f"{m.group(1)}-{m.group(2)}", (m.group(3), kv[1]))]

    with Timer(log, f"pogoda dzienna per powiat {y0}–{y1} (workery)"):
        rdd = files.flatMap(by_month).groupByKey().flatMap(lambda kv: month_rows(kv[0], list(kv[1]), zones_bc))
        daily = (spark.createDataFrame(rdd, schema).withColumn("date", F.to_date("date"))
                 .withColumn("year", F.year("date")).cache())
        n_daily = daily.count()
        daily.write.mode("overwrite").partitionBy("year").parquet(f"{args.hdfs}{OUT_DAILY}")
    log.info("dni × powiaty: %d, miesięcy: %d", n_daily, daily.select(F.date_format("date", "yyyy-MM")).distinct().count())

    with Timer(log, "terminy 16-dniowe + bilans wodny 60 dni (Spark SQL)"):
        d = (daily.withColumn("doy", F.dayofyear("date")).withColumn("period", ((F.col("doy") - 1) / 16).cast("int"))
             .withColumn("kbw_day", F.col("precip_mm") - F.col("et0_mm"))
             .withColumn("day_num", F.datediff("date", F.lit("2000-01-01"))))
        w60 = Window.partitionBy("region_id").orderBy("day_num").rangeBetween(-59, 0)
        d = d.withColumn("kbw_60d", F.sum("kbw_day").over(w60)) \
             .withColumn("days_60d", F.count("kbw_day").over(w60))
        last = Window.partitionBy("region_id", "year", "period").orderBy(F.col("date").desc())
        periods = (d.withColumn("rn", F.row_number().over(last))
                   .groupBy("region_id", "year", "period")
                   .agg(F.min("date").alias("date"), F.count("*").alias("days"),
                        F.round(F.sum("precip_mm"), 2).alias("precip_mm"), F.round(F.sum("et0_mm"), 2).alias("et0_mm"),
                        F.round(F.sum("kbw_day"), 2).alias("kbw_mm"),
                        F.round(F.max(F.when(F.col("rn") == 1, F.col("kbw_60d"))), 2).alias("kbw_60d_mm"),
                        F.max(F.when(F.col("rn") == 1, F.col("days_60d"))).alias("kbw_60d_days"),
                        F.round(F.avg("t_mean"), 2).alias("t_mean"), F.round(F.max("t_max"), 2).alias("t_max"),
                        F.round(F.min("t_min"), 2).alias("t_min"),
                        F.sum((F.col("t_max") >= HOT_DAY_C).cast("int")).alias("hot_days"),
                        F.sum((F.col("t_min") < FROST_C).cast("int")).alias("frost_days"),
                        F.round(F.avg("swvl1"), 4).alias("swvl1"), F.round(F.avg("snow_m"), 4).alias("snow_m"))
                   .cache())
        periods.write.mode("overwrite").partitionBy("year").parquet(f"{args.hdfs}{OUT_PERIODS}")
        p = periods.toPandas()
    log.info("terminy × powiaty: %d; lata: %s", len(p), sorted(p["year"].unique().tolist()))

    # kontrola sensowności: czerwiec–lipiec 2018 (susza) vs średnia pozostałych lat, cała Polska
    summer = p[p["period"].between(10, 12)]
    if not summer.empty:
        by_year = summer.groupby("year").agg(opad=("precip_mm", "mean"), et0=("et0_mm", "mean"),
                                             kbw60=("kbw_60d_mm", "mean"), upal=("hot_days", "mean"))
        log.info("VI–VII (terminy 10–12), średnio na powiat — opad / ET₀ / bilans 60 dni / dni upalne: %s",
                 ", ".join(f"{y}: {r.opad:.0f}/{r.et0:.0f}/{r.kbw60:.0f} mm/{r.upal:.1f}" for y, r in by_year.iterrows()))

    if not args.no_clickhouse:
        with Timer(log, "zapis do ClickHouse"):
            from lib.clickhouse import ClickHouse

            ch = ClickHouse()
            ch.command("DROP TABLE IF EXISTS weather_periods")
            ch.command("""
                CREATE TABLE weather_periods (
                    region_id LowCardinality(String), year UInt16, period UInt8, date Date, days UInt8,
                    precip_mm Nullable(Float64), et0_mm Nullable(Float64), kbw_mm Nullable(Float64),
                    kbw_60d_mm Nullable(Float64), kbw_60d_days UInt8, t_mean Nullable(Float64),
                    t_max Nullable(Float64), t_min Nullable(Float64), hot_days UInt8, frost_days UInt8,
                    swvl1 Nullable(Float64), snow_m Nullable(Float64)
                ) ENGINE = MergeTree ORDER BY (region_id, year, period)""")
            p["date"] = p["date"].astype("datetime64[ns]").dt.date
            ch.insert_df("weather_periods", p)
    spark.stop()


if __name__ == "__main__":
    main()
