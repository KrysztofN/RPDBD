"""
B2 — przebieg sezonu (fenologia) per powiat: start, szczyt i koniec sezonu z krzywej NDVI (Spark) -> HDFS + ClickHouse.

Co robi:
  1. czyta z HDFS tabelę NDVI per powiat (lib/ndvi_zonal_spark.py), tylko powiaty ziemskie:
       pola orne (arable) — wszystkie lata; grupy upraw (np. maize) — lata map 2018 i 2022, gdy >= 50 czystych pikseli;
  2. dla każdej krzywej (powiat × klasa) osobno, na workerach (applyInPandas):
       pełna siatka terminów -> uzupełnienie luk -> wygładzenie (te same funkcje co B1),
       w każdym roku: szczyt = maksimum w III–IX, start/koniec = przecięcie 50% wysokości „górki”
       (od minimum przed szczytem / do minimum po szczycie), z dokładnością do dnia (interpolacja między terminami);
  3. dla pól ornych: typowe daty z normy kroczącej (B1_B2/norms.py — ta sama zasada co w B1) i przesunięcie w dniach,
     status: „wcześniej niż zwykle” / „typowo” / „później niż zwykle”;
  4. zapis: HDFS /rbd/clean/b2_phenology (Parquet) i ClickHouse rbd.b2_phenology.

Trochę teorii:
  NDVI pól w roku ma kształt „górki”: rośnie od startu wegetacji, ma szczyt (pełny wzrost), spada przy dojrzewaniu
  i zbiorze. Metoda „50% amplitudy” jest odporna na różny poziom zimowego NDVI (oziminy są zielone już zimą):
  liczy się połowa drogi między minimum a szczytem, a nie stały próg. Krzywa pól ornych to mieszanka upraw
  (ozime mają szczyt w maju–czerwcu, kukurydza w sierpniu) — dlatego w latach map liczymy też grupy upraw osobno.
  Zdjęcia są co 16 dni, więc dokładność dat to ok. ±8 dni; próg statusu: przesunięcie >= max(8 dni, 1 odchylenie).
  Dla bieżącego roku w trakcie sezonu koniec może jeszcze nie nastąpić — wtedy end_* jest puste.

Uruchomienie (na masterze, z katalogu z kodem):
  set -a; . ~/rbd-build/clickhouse/.env; set +a
  infra/spark/submit.sh B1_B2/b2_phenology.py
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from B1_B2.norms import DEFAULT_CFG, norms_by_year
from lib.log import Timer, get_logger, setup_logging

log = get_logger(Path(__file__).stem)

HDFS = "hdfs://10.7.11.21:9000"
IN_DIR = "/rbd/clean/ndvi_counties"
OUT_DIR = "/rbd/clean/b2_phenology"
LAST_RURAL_CODE = 60
PEAK_DOY = (81, 273)           # szczyt szukamy od ok. 22 marca do 30 września
STATUS_EARLY, STATUS_NORMAL, STATUS_LATE, STATUS_NO_DATA = ("wcześniej niż zwykle", "typowo",
                                                            "później niż zwykle", "brak danych")
METRICS = ("start", "peak", "end")

OUT_COLS = (["region_id", "crop_class", "year", "start_doy", "peak_doy", "end_doy", "length_days",
             "peak_ndvi", "base_ndvi", "amplitude"]
            + [f"{m}_{c}" for m in METRICS for c in ("norm", "norm_std", "shift", "status")]
            + ["norm_from", "norm_to"])
OUT_SCHEMA = ("region_id string, crop_class string, year int, start_doy double, peak_doy double, end_doy double, "
              "length_days double, peak_ndvi double, base_ndvi double, amplitude double, "
              + ", ".join(f"{m}_norm double, {m}_norm_std double, {m}_shift double, {m}_status string" for m in METRICS)
              + ", norm_from int, norm_to int")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="B2: start / szczyt / koniec sezonu per powiat")
    p.add_argument("--hdfs", default=HDFS)
    p.add_argument("--min-amplitude", type=float, default=0.1, help="mniejsza „górka” = brak wyraźnego sezonu")
    p.add_argument("--min-shift-days", type=float, default=8.0, help="minimalne przesunięcie, by nie było „typowo”")
    p.add_argument("--min-group-pixels", type=int, default=50, help="min. czystych pikseli dla krzywej grupy upraw")
    p.add_argument("--no-clickhouse", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args()


def _crossing(doy: np.ndarray, y: np.ndarray, i: int, thr: float) -> float:
    """Dzień, w którym odcinek (i, i+1) przecina próg thr (interpolacja liniowa)."""
    y0, y1 = y[i], y[i + 1]
    if y1 == y0:
        return float(doy[i])
    return float(doy[i] + (thr - y0) / (y1 - y0) * (doy[i + 1] - doy[i]))


def season_dates(doy: np.ndarray, y: np.ndarray, min_amplitude: float = 0.1) -> dict:
    """
    Jeden rok, jedna krzywa (23 terminy, wygładzone NDVI; NaN = brak).
    Szczyt = maksimum w PEAK_DOY; start = ostatnie przejście w górę przez 50% amplitudy przed szczytem;
    koniec = pierwsze przejście w dół przez 50% amplitudy po szczycie.
    """
    out = dict(start_doy=np.nan, peak_doy=np.nan, end_doy=np.nan, peak_ndvi=np.nan, base_ndvi=np.nan, amplitude=np.nan)
    if np.isfinite(y).sum() < 12:
        return out
    win = (doy >= PEAK_DOY[0]) & (doy <= PEAK_DOY[1]) & np.isfinite(y)
    if not win.any():
        return out
    p = int(np.flatnonzero(win)[np.nanargmax(y[win])])
    peak = y[p]
    out.update(peak_doy=float(doy[p]), peak_ndvi=float(peak))

    left = y[: p + 1]
    if np.isfinite(left).any():
        base = np.nanmin(left)
        amp = peak - base
        out.update(base_ndvi=float(base), amplitude=float(amp))
        if amp >= min_amplitude:
            thr = base + 0.5 * amp
            for i in range(p - 1, -1, -1):                    # od szczytu wstecz: pierwszy termin poniżej progu
                if np.isfinite(y[i]) and np.isfinite(y[i + 1]) and y[i] < thr <= y[i + 1]:
                    out["start_doy"] = _crossing(doy, y, i, thr)
                    break
    right = y[p:]
    if np.isfinite(right).any():
        base_r = np.nanmin(right)
        amp_r = peak - base_r
        if amp_r >= min_amplitude:
            thr = base_r + 0.5 * amp_r
            for i in range(p, len(y) - 1):                     # od szczytu w przód: pierwsze zejście poniżej progu
                if np.isfinite(y[i]) and np.isfinite(y[i + 1]) and y[i] >= thr > y[i + 1]:
                    out["end_doy"] = _crossing(doy, y, i, thr)
                    break
    return out


def b2_for_curve(pdf: pd.DataFrame, cfg: dict, min_amplitude: float, min_shift: float) -> pd.DataFrame:
    """Jedna krzywa (powiat × klasa, wszystkie lata): daty sezonu w każdym roku + (pola orne) norma i przesunięcie."""
    from lib import ndvi_tools as nt

    rid, cls = pdf["region_id"].iloc[0], pdf["crop_class"].iloc[0]
    years = sorted(pdf["year"].unique())
    grid = nt.complete_grid(pdf[["region_id", "year", "period", "ndvi_mean"]], [rid], years)
    s = nt.smooth_regional_series(grid, "ndvi_mean")

    rows = []
    for year, g in s.groupby("year"):
        g = g.sort_values("period")
        d = season_dates(g["doy"].to_numpy(float), g["ndvi_smooth"].to_numpy(float), min_amplitude)
        rows.append(dict(year=int(year), **d))
    out = pd.DataFrame(rows)
    out["length_days"] = out["end_doy"] - out["start_doy"]
    out["region_id"], out["crop_class"] = rid, cls

    for m in METRICS:                                      # domyślnie puste (grupy upraw: tylko 2 lata, bez normy)
        out[f"{m}_norm"] = out[f"{m}_norm_std"] = out[f"{m}_shift"] = np.nan
        out[f"{m}_status"] = STATUS_NO_DATA
    out["norm_from"] = out["norm_to"] = 0

    if cls == "arable":
        for m in METRICS:
            norm = norms_by_year(out[["year", f"{m}_doy"]], [], f"{m}_doy", cfg)
            norm = norm[["year", "norm_mean", "norm_std", "norm_from", "norm_to"]]
            merged = out[["year", f"{m}_doy"]].merge(norm, on="year", how="left")
            shift = merged[f"{m}_doy"] - merged["norm_mean"]
            limit = np.maximum(min_shift, merged["norm_std"].fillna(0))
            out[f"{m}_norm"], out[f"{m}_norm_std"], out[f"{m}_shift"] = merged["norm_mean"], merged["norm_std"], shift
            out[f"{m}_status"] = np.select([shift.isna(), shift <= -limit, shift >= limit],
                                           [STATUS_NO_DATA, STATUS_EARLY, STATUS_LATE], STATUS_NORMAL)
            out["norm_from"], out["norm_to"] = merged["norm_from"].fillna(0), merged["norm_to"].fillna(0)
    out["norm_from"] = out["norm_from"].astype(int)
    out["norm_to"] = out["norm_to"].astype(int)
    return out[OUT_COLS]


def write_clickhouse(df: pd.DataFrame) -> None:
    from lib.clickhouse import ClickHouse

    ch = ClickHouse()
    ch.command("DROP TABLE IF EXISTS b2_phenology")
    metric_cols = ", ".join(f"{m}_norm Nullable(Float64), {m}_norm_std Nullable(Float64), "
                            f"{m}_shift Nullable(Float64), {m}_status LowCardinality(String), {m}_date Nullable(Date)"
                            for m in METRICS)
    ch.command(f"""
        CREATE TABLE b2_phenology (
            region_id LowCardinality(String), county_name String, voiv_name LowCardinality(String),
            crop_class LowCardinality(String), year UInt16,
            start_doy Nullable(Float64), peak_doy Nullable(Float64), end_doy Nullable(Float64),
            length_days Nullable(Float64), peak_ndvi Nullable(Float64), base_ndvi Nullable(Float64),
            amplitude Nullable(Float64), {metric_cols}, norm_from UInt16, norm_to UInt16
        ) ENGINE = MergeTree ORDER BY (region_id, crop_class, year)""")
    ch.insert_df("b2_phenology", df)


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("rbd-b2-phenology").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    cfg, ma, ms = dict(DEFAULT_CFG), args.min_amplitude, args.min_shift_days
    log.info("start/koniec = 50%% amplitudy, szczyt w dniach %d–%d, min. amplituda %.2f, próg statusu >= max(%.0f dni, 1 odch.), "
             "norma krocząca %d lat", PEAK_DOY[0], PEAK_DOY[1], ma, ms, cfg["rolling"])

    with Timer(log, "B2: daty sezonu (workery, applyInPandas)"):
        ndvi = (spark.read.parquet(f"{args.hdfs}{IN_DIR}")
                .where(F.substring("region_id", 3, 2).cast("int") <= LAST_RURAL_CODE)
                .where((F.col("crop_class") == "arable") | (F.col("n_total") >= args.min_group_pixels))
                .select("region_id", "crop_class", "year", "period", "ndvi_mean"))
        b2 = (ndvi.repartition("region_id")
              .groupBy("region_id", "crop_class")
              .applyInPandas(lambda pdf: b2_for_curve(pdf, cfg, ma, ms), schema=OUT_SCHEMA)
              .cache())
        b2.write.mode("overwrite").partitionBy("crop_class").parquet(f"{args.hdfs}{OUT_DIR}")
        df = b2.toPandas()

    arable = df[df["crop_class"] == "arable"]
    log.info("zapisano HDFS %s: %d wierszy (pola orne: %d powiatów × %d lat; grupy upraw: %d krzywych)", OUT_DIR,
             len(df), arable["region_id"].nunique(), arable["year"].nunique(),
             len(df[df["crop_class"] != "arable"]))
    med = arable.groupby("year")[["start_shift", "peak_shift", "end_shift"]].median().round(0)
    log.info("pola orne, mediana przesunięcia po powiatach (dni: start/szczyt/koniec, + = później): %s",
             ", ".join(f"{y}: {r.start_shift:+.0f}/{r.peak_shift:+.0f}/{r.end_shift:+.0f}" for y, r in med.iterrows()))
    typical = arable[arable["year"].between(2011, 2024)][["start_doy", "peak_doy", "end_doy"]].median()
    log.info("typowe daty pól ornych (mediana 2011–2024, dzień roku): start %.0f, szczyt %.0f, koniec %.0f",
             typical["start_doy"], typical["peak_doy"], typical["end_doy"])

    if not args.no_clickhouse:
        with Timer(log, "zapis do ClickHouse"):
            from lib.clickhouse import ClickHouse

            names = ClickHouse().query_df("SELECT region_id, county_name, voiv_name FROM counties")
            out = df.merge(names, on="region_id", how="left")
            for m in METRICS:
                out[f"{m}_date"] = pd.to_datetime(out["year"].astype(str) + "-01-01") \
                    + pd.to_timedelta(out[f"{m}_doy"].round() - 1, unit="D")
                out[f"{m}_date"] = out[f"{m}_date"].dt.date
            write_clickhouse(out)
    spark.stop()


if __name__ == "__main__":
    main()
