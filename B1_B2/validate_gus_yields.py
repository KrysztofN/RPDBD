"""
Sprawdzenie B1 niezależnym źródłem: czy „gorzej niż zwykle” z satelity idzie w parze z niższymi plonami GUS.

Co robi:
  1. pobiera plony GUS (BDL, dt/ha) dla województw: pszenica, jęczmień, rzepak, buraki cukrowe (lib/gus_bdl.py);
  2. liczy odchylenie plonu od normy (ta sama zasada co w B1: średnia z 15 poprzednich lat bez lat skrajnych,
     min. 5 lat) -> yield_anom_pct = plon / norma − 1 [%];
  3. z wyników B1 (ClickHouse) liczy dla województwa i roku odchylenie NDVI od normy, średnio ważone powierzchnią
     pól ornych powiatów (EUCROPMAP 2018): cały sezon IV–IX oraz okno V–VI (kłoszenie i nalewanie ziarna);
     do tego udział powiatów „gorzej niż zwykle” w sezonie;
  4. zapisuje do ClickHouse: rbd.validation_gus (województwo × rok × uprawa) i rbd.validation_summary
     (korelacje i trafność per uprawa) — do dashboardu Grafany „Walidacja B1”.

Trochę teorii:
  Plon zależy od stanu roślin w kluczowych fazach (zboża: maj–czerwiec), a NDVI mierzy zieloną biomasę — gdy satelita
  widzi słabsze rośliny, plon zwykle jest niższy. Miara zgodności: korelacja Pearsona r (−1…1; > 0 = te same kierunki)
  z wartością p (< 0,05 = mało prawdopodobne, że przypadek) oraz trafność: w ilu przypadkach, gdy ≥ 50% powiatów
  województwa było „gorzej niż zwykle”, plon GUS był poniżej normy. GUS podaje plony tylko dla województw, więc
  porównanie jest na tym poziomie (B1 liczymy dla powiatów — tu uśrednione).

Użycie (kontener rbd-python na masterze):
  python3 -m B1_B2.validate_gus_yields
"""
from __future__ import annotations

import argparse
import io
from pathlib import Path

import numpy as np
import pandas as pd

from lib.log import get_logger, setup_logging

log = get_logger(Path(__file__).stem)

STRUCTURE = "/rbd/ref/crops/crop_structure_counties_2018.parquet"
WORSE = "gorzej niż zwykle"
NORM_YEARS, NORM_MIN, CLIP_K = 15, 5, 1.5
FIRST_GUS_YEAR = 1999                          # lata wcześniejsze tylko do normy plonu


def yield_anomalies(yields: pd.DataFrame) -> pd.DataFrame:
    """Odchylenie plonu od normy z poprzednich lat (jak B1: krocząca, bez lat skrajnych)."""
    from B1_B2.norms import clipped_stats

    out = []
    for (voiv, crop), g in yields.groupby(["voiv_id", "crop"]):
        s = g.set_index("year")["yield_dt_ha"].sort_index()
        for year, val in s.items():
            ref = s[(s.index < year) & (s.index >= year - NORM_YEARS)].dropna()
            if pd.isna(val) or len(ref) < NORM_MIN:
                continue
            norm = clipped_stats(ref, CLIP_K)["norm_mean"]
            out.append(dict(voiv_id=voiv, crop=crop, year=int(year), yield_dt_ha=float(val), yield_norm=float(norm),
                            yield_anom_pct=100 * (val / norm - 1)))
    return pd.DataFrame(out)


def ndvi_by_voiv(ch) -> pd.DataFrame:
    """Odchylenie NDVI od normy (%) dla województw: ważone powierzchnią pól ornych powiatów."""
    from lib.webhdfs import WebHDFS

    counties = ch.query_df("SELECT region_id, voiv_id, voiv_name FROM rbd.counties WHERE is_city = 0")
    weights = pd.read_parquet(io.BytesIO(WebHDFS().read_bytes(STRUCTURE)))[["region_id", "arable_km2"]]
    season = ch.query_df("SELECT region_id, year, pct_of_norm - 100 AS season_anom, status FROM rbd.b1_season")
    window = ch.query_df("SELECT region_id, year, avg(pct_of_norm) - 100 AS v_vi_anom FROM rbd.b1_periods "
                         "WHERE toMonth(date) IN (5, 6) GROUP BY region_id, year")
    df = (season.merge(window, on=["region_id", "year"], how="left").merge(counties, on="region_id")
          .merge(weights, on="region_id"))
    df["worse"] = (df["status"] == WORSE).astype(float)

    def wavg(g: pd.DataFrame, col: str) -> float:
        ok = g[col].notna() & (g["arable_km2"] > 0)
        return float(np.average(g.loc[ok, col], weights=g.loc[ok, "arable_km2"])) if ok.any() else np.nan

    rows = [dict(voiv_id=v, voiv_name=name, year=int(y), ndvi_season_anom_pct=wavg(g, "season_anom"),
                 ndvi_v_vi_anom_pct=wavg(g, "v_vi_anom"), worse_share_pct=100 * wavg(g, "worse"),
                 n_counties=len(g))
            for (v, name, y), g in df.groupby(["voiv_id", "voiv_name", "year"])]
    return pd.DataFrame(rows)


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """Korelacje i trafność per uprawa (+ wszystkie razem)."""
    from scipy.stats import pearsonr

    rows = []
    for crop, g in list(df.groupby("crop")) + [("wszystkie", df)]:
        for col, label in (("ndvi_v_vi_anom_pct", "NDVI V–VI"), ("ndvi_season_anom_pct", "NDVI sezon IV–IX")):
            ok = g[[col, "yield_anom_pct"]].dropna()
            if len(ok) >= 10:
                r, p = pearsonr(ok[col], ok["yield_anom_pct"])
                rows.append(dict(crop=crop, metric=f"korelacja r: {label} vs plon", value=round(float(r), 3),
                                 p_value=float(p), n=len(ok)))
        bad = g[g["worse_share_pct"] >= 50]
        if len(bad):
            rows.append(dict(crop=crop, metric="trafność: ≥50% powiatów gorzej → plon poniżej normy [%]",
                             value=round(100 * float((bad["yield_anom_pct"] < 0).mean()), 1), p_value=np.nan,
                             n=len(bad)))
        good = g[g["worse_share_pct"] < 10]
        if len(good):
            rows.append(dict(crop=crop, metric="dla porównania: <10% powiatów gorzej → plon poniżej normy [%]",
                             value=round(100 * float((good["yield_anom_pct"] < 0).mean()), 1), p_value=np.nan,
                             n=len(good)))
    return pd.DataFrame(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Walidacja B1: NDVI vs plony GUS (województwa)")
    p.add_argument("--to", dest="last_year", type=int, default=2024)
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    a = p.parse_args()
    setup_logging(a.verbose, a.quiet)
    from lib.clickhouse import ClickHouse
    from lib.gus_bdl import YIELD_VARIABLES, variable

    ch = ClickHouse()
    frames = []
    for var_id, crop in YIELD_VARIABLES.items():
        v = variable(var_id, range(FIRST_GUS_YEAR, a.last_year + 1))
        frames.append(v.rename(columns={"unit_id": "voiv_id", "value": "yield_dt_ha"}).assign(crop=crop))
    yields = pd.concat(frames)[["voiv_id", "crop", "year", "yield_dt_ha"]]
    anom = yield_anomalies(yields)
    ndvi = ndvi_by_voiv(ch)
    df = anom.merge(ndvi, on=["voiv_id", "year"], how="inner")
    log.info("porównanie: %d par (województwo × rok × uprawa), lata %d–%d", len(df), df["year"].min(), df["year"].max())

    summary = summarize(df)
    for r in summary.itertuples():
        log.info("%-15s %-62s %7.3f  (n=%d%s)", r.crop, r.metric, r.value, r.n,
                 f", p={r.p_value:.1g}" if pd.notna(r.p_value) else "")

    ch.command("DROP TABLE IF EXISTS validation_gus")
    ch.command("""CREATE TABLE validation_gus (
        voiv_id LowCardinality(String), voiv_name String, year UInt16, crop LowCardinality(String),
        yield_dt_ha Float64, yield_norm Float64, yield_anom_pct Float64, ndvi_season_anom_pct Float64,
        ndvi_v_vi_anom_pct Float64, worse_share_pct Float64, n_counties UInt16
    ) ENGINE = MergeTree ORDER BY (crop, voiv_id, year)""")
    ch.insert_df("validation_gus", df[["voiv_id", "voiv_name", "year", "crop", "yield_dt_ha", "yield_norm",
                                       "yield_anom_pct", "ndvi_season_anom_pct", "ndvi_v_vi_anom_pct",
                                       "worse_share_pct", "n_counties"]])
    ch.command("DROP TABLE IF EXISTS validation_summary")
    ch.command("""CREATE TABLE validation_summary (
        source LowCardinality(String), crop LowCardinality(String), metric String, value Float64,
        p_value Nullable(Float64), n UInt32
    ) ENGINE = MergeTree ORDER BY (source, crop, metric)""")
    ch.insert_df("validation_summary", summary.assign(source="GUS plony")[["source", "crop", "metric", "value",
                                                                          "p_value", "n"]])
    log.info("zapisano: rbd.validation_gus (%d), rbd.validation_summary (%d)", len(df), len(summary))


if __name__ == "__main__":
    main()
