"""
B1 — stan upraw per powiat: NDVI pól ornych na tle normy 2001–2017 (Spark) -> HDFS + ClickHouse.

Co robi:
  1. czyta z HDFS tabelę NDVI per powiat (lib/ndvi_zonal_spark.py), klasa „arable”, tylko powiaty ziemskie;
  2. dla każdego powiatu osobno (Spark rozdziela powiaty między workery, applyInPandas):
       pełna siatka 23 terminów/rok -> uzupełnienie luk (do 3 terminów) -> wygładzenie Savitzky–Golay,
       norma per termin (średnia, odchylenie, kwartyle) bez lat skrajnych (dalszych niż k odchyleń),
       diff, % normy, z = diff / odchylenie, status, „od ilu terminów gorzej niż zwykle” (below_streak);
  3. sezon IV–IX: średni NDVI sezonu vs norma sezonowa (też per powiat, ta sama norma);
  Norma (--norm-mode):
       fixed    stała: lata 2001–2017 dla każdego roku
       rolling  krocząca: dla roku Y lata Y-15 … Y-1 (--rolling-years); gdy historii < --min-norm-years,
                norma stała. Kroczącą porównujemy rok z niedawną przeszłością — pola z czasem „zielenieją”
                (nawożenie, odmiany, ew. dryf czujnika), więc stara norma zaniża dziś susze.
  4. zapis: HDFS /rbd/clean/b1_periods, /rbd/clean/b1_season (Parquet) i ClickHouse rbd.b1_periods, rbd.b1_season,
     rbd.counties (powiaty z nazwami i punktem na mapę — dla Grafany).

Trochę teorii:
  Norma tylko z lat 2001–2017 — lata 2018–2019 to testowane susze; gdyby weszły do normy, zaniżyłyby ją.
  z = ile „typowych odchyleń” od normy; |z| >= 1 -> powyżej / poniżej normy (ok. 1/6 lat normy wypada poniżej
  z samej natury rozrzutu, susze powinny dawać wyraźnie więcej). Odchylenie ma dolny próg (0.02), żeby
  w zimie przy prawie stałym NDVI drobne wahania nie dawały ogromnych z.
  Do zrobienia po B2: uwzględnienie przesunięcia sezonu (spóźniony start != słabe uprawy).

Uruchomienie (na masterze, z katalogu z kodem; hasło ClickHouse w zmiennej środowiskowej):
  set -a; . ~/rbd-build/clickhouse/.env; set +a
  infra/spark/submit.sh B1_B2/b1_crop_condition.py
"""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import pandas as pd

from B1_B2.norms import NORM_FIRST, NORM_LAST, norms_by_year
from lib.log import Timer, get_logger, setup_logging

log = get_logger(Path(__file__).stem)

HDFS = "hdfs://10.7.11.21:9000"
IN_DIR = "/rbd/clean/ndvi_counties"
OUT_PERIODS = "/rbd/clean/b1_periods"
OUT_SEASON = "/rbd/clean/b1_season"
REF_DIR = "/rbd/ref/boundaries"
SEASON_MONTHS = (4, 9)
LAST_RURAL_CODE = 60
# statusy po polsku (czyta je człowiek: Grafana, biuletyn); „worse/better” = rośliny mniej/bardziej zielone niż zwykle
STATUS_BELOW, STATUS_NORMAL, STATUS_ABOVE, STATUS_NO_DATA = "gorzej niż zwykle", "typowo", "lepiej niż zwykle", "brak danych"

PERIOD_COLS = ["region_id", "year", "period", "date", "ndvi_mean", "coverage", "ndvi_smooth",
               "norm_mean", "norm_std", "norm_p25", "norm_p75", "norm_n", "norm_from", "norm_to",
               "diff", "pct_of_norm", "z", "status", "below_streak"]
PERIOD_SCHEMA = ("region_id string, year int, period int, date date, ndvi_mean double, coverage double, "
                 "ndvi_smooth double, norm_mean double, norm_std double, norm_p25 double, norm_p75 double, "
                 "norm_n int, norm_from int, norm_to int, diff double, pct_of_norm double, z double, "
                 "status string, below_streak int")
SEASON_COLS = ["region_id", "year", "season_mean", "season_n", "norm_mean", "norm_std", "norm_n",
               "norm_from", "norm_to", "diff", "pct_of_norm", "z", "status"]
SEASON_SCHEMA = ("region_id string, year int, season_mean double, season_n int, norm_mean double, "
                 "norm_std double, norm_n int, norm_from int, norm_to int, diff double, pct_of_norm double, "
                 "z double, status string")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="B1: stan upraw per powiat vs norma 2001–2017")
    p.add_argument("--hdfs", default=HDFS)
    p.add_argument("--z-threshold", type=float, default=1.0)
    p.add_argument("--min-std", type=float, default=0.02, help="dolny próg odchylenia normy")
    p.add_argument("--clip-k", type=float, default=1.5,
                   help="lata dalsze niż k odchyleń od średniej nie wchodzą do normy (0 = bez odcinania)")
    p.add_argument("--norm-mode", choices=("fixed", "rolling"), default="rolling",
                   help="fixed = norma 2001–2017; rolling = poprzednie --rolling-years lat")
    p.add_argument("--rolling-years", type=int, default=15)
    p.add_argument("--min-norm-years", type=int, default=10, help="mniej lat historii -> norma stała")
    p.add_argument("--tag", default="", help="dopisek do katalogów HDFS (np. _rolling) — do porównań")
    p.add_argument("--no-clickhouse", action="store_true", help="tylko HDFS (bez zapisu do ClickHouse)")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args()


def compare(df: pd.DataFrame, value_col: str, z_threshold: float, min_std: float) -> pd.DataFrame:
    """diff, % normy, z, status — wspólne dla terminów i sezonu."""
    import numpy as np

    df["diff"] = df[value_col] - df["norm_mean"]
    df["pct_of_norm"] = 100 * df[value_col] / df["norm_mean"]
    df["z"] = df["diff"] / df["norm_std"].clip(lower=min_std)
    df["status"] = np.select([df["z"].isna(), df["z"] <= -z_threshold, df["z"] >= z_threshold],
                             [STATUS_NO_DATA, STATUS_BELOW, STATUS_ABOVE], STATUS_NORMAL)
    return df


def b1_for_county(pdf: pd.DataFrame, z_threshold: float, min_std: float, cfg: dict) -> pd.DataFrame:
    """Jeden powiat (wszystkie lata): wygładzenie, norma per termin, odchylenie, status. Działa na workerze."""
    from lib import ndvi_tools as nt

    rid = pdf["region_id"].iloc[0]
    years = range(int(pdf["year"].min()), int(pdf["year"].max()) + 1)
    grid = nt.complete_grid(pdf[["region_id", "year", "period", "ndvi_mean", "coverage"]], [rid], years)
    s = nt.smooth_regional_series(grid, "ndvi_mean")

    norm = norms_by_year(s[["year", "period", "ndvi_smooth"]], ["period"], "ndvi_smooth", cfg)
    out = compare(s.merge(norm, on=["year", "period"], how="left"), "ndvi_smooth", z_threshold, min_std)

    # ile kolejnych terminów (w danym roku, do tego terminu włącznie) powiat jest gorzej niż zwykle
    out = out.sort_values(["year", "period"])
    below = (out["status"] == STATUS_BELOW).astype(int)
    run_id = (below != below.groupby(out["year"]).shift(fill_value=0)).groupby(out["year"]).cumsum()
    out["below_streak"] = below.groupby([out["year"], run_id]).cumsum() * below

    out["date"] = out["date"].dt.date
    for c in ("norm_n", "norm_from", "norm_to"):
        out[c] = out[c].fillna(0).astype(int)
    return out[PERIOD_COLS]


def season_for_county(pdf: pd.DataFrame, z_threshold: float, min_std: float, cfg: dict) -> pd.DataFrame:
    """Jeden powiat: średni NDVI sezonu IV–IX w każdym roku vs norma sezonowa (ta sama zasada normy)."""
    m = pd.to_datetime(pdf["date"]).dt.month
    sel = pdf[(m >= SEASON_MONTHS[0]) & (m <= SEASON_MONTHS[1])]
    season = (sel.groupby("year")["ndvi_smooth"].agg(season_mean="mean", season_n="count").reset_index())
    norm = norms_by_year(season[["year", "season_mean"]], [], "season_mean", cfg)
    out = compare(season.merge(norm, on="year", how="left"), "season_mean", z_threshold, min_std)
    out["region_id"] = pdf["region_id"].iloc[0]
    for c in ("season_n", "norm_n", "norm_from", "norm_to"):
        out[c] = out[c].fillna(0).astype(int)
    return out[SEASON_COLS]


def county_table(sc, hdfs: str) -> pd.DataFrame:
    """
    Powiaty z granic na HDFS: region_id, county_name, voiv_id, voiv_name, is_city, lat, lon.
    lat/lon = punkt wewnątrz powiatu (representative_point, zawsze leży w granicach) — do mapy w Grafanie.
    """
    import geopandas as gpd

    files = dict(sc.binaryFiles(f"{hdfs}{REF_DIR}/counties_raw.gpkg,{hdfs}{REF_DIR}/voivodeships_raw.gpkg").collect())
    with tempfile.TemporaryDirectory() as tmp:
        local = {}
        for k, v in files.items():
            local[Path(k).name] = Path(tmp) / Path(k).name
            local[Path(k).name].write_bytes(bytes(v))
        c = gpd.read_file(local["counties_raw.gpkg"])
        w = gpd.read_file(local["voivodeships_raw.gpkg"], ignore_geometry=True)
    voiv = dict(zip(w["JPT_KOD_JE"].astype(str), w["JPT_NAZWA_"].str.strip()))
    pts = c.geometry.representative_point().to_crs("EPSG:4326")
    code = c["JPT_KOD_JE"].astype(str)
    return pd.DataFrame({"region_id": code,
                         "county_name": c["JPT_NAZWA_"].str.strip(),
                         "voiv_id": code.str[:2],
                         "voiv_name": code.str[:2].map(voiv),
                         "is_city": (code.str[2:4].astype(int) > LAST_RURAL_CODE).astype("uint8"),
                         "lat": pts.y.round(5), "lon": pts.x.round(5)})


def write_clickhouse(periods: pd.DataFrame, season: pd.DataFrame, counties: pd.DataFrame) -> None:
    from lib.clickhouse import ClickHouse

    ch = ClickHouse()
    # pełne przeliczenie: tabele tworzymy od nowa (także gdy zmienił się zestaw kolumn)
    for table in ("b1_periods", "b1_season", "counties"):
        ch.command(f"DROP TABLE IF EXISTS {table}")
    ch.command("""
        CREATE TABLE counties (
            region_id String, county_name String, voiv_id LowCardinality(String),
            voiv_name LowCardinality(String), is_city UInt8, lat Float64, lon Float64
        ) ENGINE = MergeTree ORDER BY region_id""")
    ch.insert_df("counties", counties)
    ch.command("""
        CREATE TABLE b1_periods (
            region_id LowCardinality(String), county_name String, voiv_id LowCardinality(String),
            voiv_name LowCardinality(String), year UInt16, period UInt8, date Date,
            ndvi_mean Nullable(Float64), coverage Nullable(Float64), ndvi_smooth Nullable(Float64),
            norm_mean Nullable(Float64), norm_std Nullable(Float64), norm_p25 Nullable(Float64),
            norm_p75 Nullable(Float64), norm_n UInt16, norm_from UInt16, norm_to UInt16,
            diff Nullable(Float64), pct_of_norm Nullable(Float64),
            z Nullable(Float64), status LowCardinality(String), below_streak UInt8
        ) ENGINE = MergeTree ORDER BY (region_id, year, period)""")
    ch.command("""
        CREATE TABLE b1_season (
            region_id LowCardinality(String), county_name String, voiv_id LowCardinality(String),
            voiv_name LowCardinality(String), year UInt16, season_mean Nullable(Float64), season_n UInt8,
            norm_mean Nullable(Float64), norm_std Nullable(Float64), norm_n UInt8,
            norm_from UInt16, norm_to UInt16, diff Nullable(Float64),
            pct_of_norm Nullable(Float64), z Nullable(Float64), status LowCardinality(String)
        ) ENGINE = MergeTree ORDER BY (region_id, year)""")
    for table, df in (("b1_periods", periods), ("b1_season", season)):
        ch.insert_df(table, df)


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("rbd-b1-crop-condition").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    zt, ms = args.z_threshold, args.min_std
    cfg = dict(mode=args.norm_mode, rolling=args.rolling_years, min_years=args.min_norm_years,
               clip_k=args.clip_k, first_year=NORM_FIRST)
    if args.norm_mode == "rolling":
        log.info("norma krocząca: %d poprzednich lat (min. %d, inaczej %d–%d), lata dalsze niż %.1f odch. odrzucane, "
                 "próg |z| >= %.1f", args.rolling_years, args.min_norm_years, NORM_FIRST, NORM_LAST, args.clip_k, zt)
    else:
        log.info("norma stała %d–%d, lata dalsze niż %.1f odch. odrzucane, próg |z| >= %.1f",
                 NORM_FIRST, NORM_LAST, args.clip_k, zt)
    out_periods, out_season = f"{OUT_PERIODS}{args.tag}", f"{OUT_SEASON}{args.tag}"

    with Timer(log, "B1 per termin (workery, applyInPandas)"):
        ndvi = (spark.read.parquet(f"{args.hdfs}{IN_DIR}")
                .where((F.col("crop_class") == "arable")
                       & (F.substring("region_id", 3, 2).cast("int") <= LAST_RURAL_CODE))
                .select("region_id", "year", "period", "ndvi_mean", "coverage"))
        log.info("wejście: %d wierszy, %d powiatów", ndvi.count(), ndvi.select("region_id").distinct().count())
        periods = (ndvi.repartition("region_id")
                   .groupBy("region_id")
                   .applyInPandas(lambda pdf: b1_for_county(pdf, zt, ms, cfg), schema=PERIOD_SCHEMA)
                   .cache())
        periods.write.mode("overwrite").partitionBy("year").parquet(f"{args.hdfs}{out_periods}")

    with Timer(log, "B1 sezon IV–IX (workery, applyInPandas)"):
        season = (periods.select("region_id", "year", "date", "ndvi_smooth")
                  .groupBy("region_id")
                  .applyInPandas(lambda pdf: season_for_county(pdf, zt, ms, cfg), schema=SEASON_SCHEMA))
        season.write.mode("overwrite").parquet(f"{args.hdfs}{out_season}")

    counties = county_table(spark.sparkContext, args.hdfs)
    names = counties[["region_id", "county_name", "voiv_id", "voiv_name"]]
    p = periods.toPandas().merge(names, on="region_id", how="left")
    s = season.toPandas().merge(names, on="region_id", how="left")
    log.info("zapisano HDFS: %s (%d wierszy), %s (%d wierszy)", out_periods, len(p), out_season, len(s))

    # kontrola: ile powiatów gorzej / lepiej niż zwykle w sezonie IV–IX, rok po roku (jedna linia)
    per_year = s.groupby("year")["status"].agg(worse=lambda x: (x == STATUS_BELOW).sum(),
                                               better=lambda x: (x == STATUS_ABOVE).sum())
    log.info("sezon IV–IX, powiaty gorzej/lepiej niż zwykle (z %d): %s", s["region_id"].nunique(),
             ", ".join(f"{y}: -{r.worse}/+{r.better}" for y, r in per_year.iterrows()))

    if not args.no_clickhouse:
        with Timer(log, "zapis do ClickHouse"):
            write_clickhouse(p, s, counties)
    spark.stop()


if __name__ == "__main__":
    main()
