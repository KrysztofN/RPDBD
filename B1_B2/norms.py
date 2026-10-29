"""
Norma (typowy poziom) wspólna dla B1 i B2 — żeby oba procesy porównywały z tymi samymi latami, tą samą metodą.

Co robi:
  clipped_stats(wartości, k)       średnia, odchylenie, kwartyle bez lat skrajnych
  norm_window(rok, cfg)            z jakich lat liczyć normę dla danego roku
  norms_by_year(df, klucz, kol)    norma dla każdego roku osobno (lata z tym samym oknem liczone raz)

Trochę teorii:
  Norma = typowa wartość w danym powiecie (np. NDVI w danym terminie albo data startu sezonu).
  Odcinanie lat skrajnych: najpierw średnia i odchylenie, potem odrzucenie lat dalszych niż k odchyleń
  (np. susze 2003, 2015), na koniec ponowne liczenie z pozostałych — susza nie zaniża „typowego” poziomu.
  Norma krocząca (rolling): dla roku Y lata Y-15 … Y-1 — porównanie z niedawną przeszłością, bo pola z czasem
  zielenieją (nawożenie, odmiany, ew. dryf czujnika). Gdy historii jest mniej niż min_years, norma stała 2001–2017.

cfg = dict(mode="rolling"|"fixed", rolling=15, min_years=10, clip_k=1.5, first_year=2001)
"""
from __future__ import annotations

import pandas as pd

NORM_FIRST, NORM_LAST = 2001, 2017
DEFAULT_CFG = dict(mode="rolling", rolling=15, min_years=10, clip_k=1.5, first_year=NORM_FIRST)


def clipped_stats(values: pd.Series, k: float) -> pd.Series:
    """Norma bez lat skrajnych (dalszych niż k odchyleń od średniej). k <= 0 -> bez odcinania."""
    x = values.dropna()
    if k > 0 and len(x) >= 3:
        m, sd = x.mean(), x.std()
        if sd > 0:
            x = x[(x - m).abs() <= k * sd]
    return pd.Series({"norm_mean": x.mean(), "norm_std": x.std(), "norm_p25": x.quantile(0.25),
                      "norm_p75": x.quantile(0.75), "norm_n": len(x)})


def norm_window(year: int, cfg: dict) -> tuple[int, int]:
    """Lata normy dla danego roku: krocząca (Y-n … Y-1, gdy dość historii) albo stała 2001–2017."""
    if cfg["mode"] == "rolling":
        first, last = max(cfg["first_year"], year - cfg["rolling"]), year - 1
        if last - first + 1 >= cfg["min_years"]:
            return first, last
    return NORM_FIRST, NORM_LAST


def norms_by_year(values: pd.DataFrame, key: list[str], value_col: str, cfg: dict) -> pd.DataFrame:
    """
    Norma dla każdego roku osobno (przy normie kroczącej każdy rok ma inne lata odniesienia).
    values: kolumny year, *key, value_col. Wynik: year, *key, norm_mean/std/p25/p75/n, norm_from, norm_to.
    """
    out, windows = [], {}
    for y in sorted(values["year"].unique()):
        windows.setdefault(norm_window(int(y), cfg), []).append(int(y))
    for (first, last), ys in windows.items():
        ref = values[values["year"].between(first, last)]
        if key:
            st = ref.groupby(key)[value_col].apply(lambda v: clipped_stats(v, cfg["clip_k"])).unstack().reset_index()
        else:
            st = clipped_stats(ref[value_col], cfg["clip_k"]).to_frame().T
        st["norm_from"], st["norm_to"] = first, last
        out.append(st.merge(pd.DataFrame({"year": ys}), how="cross"))
    return pd.concat(out, ignore_index=True)
