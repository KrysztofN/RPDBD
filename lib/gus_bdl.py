"""
Pobieranie danych z Banku Danych Lokalnych GUS (BDL API, bez konta).

Co robi: dla zmiennej BDL (np. 4332 = plon pszenicy w dt/ha) zwraca tabelę dla województw: kod TERYT (2 cyfry),
nazwa, rok, wartość.

Trochę teorii:
  BDL publikuje statystyki z rozbiciem terytorialnym. Plony (dt/ha = 100 kg/ha) GUS podaje dla województw
  (szacunki rzeczoznawców + badania reprezentacyjne) — to niezależne od satelity źródło stanu upraw.
  Brak uprawy GUS zapisuje jako 0 — tu zamieniane na brak wartości. Bez klucza API limit to kilka zapytań/s.

Użycie:
  python -m lib.gus_bdl 4332 --from 2001 --to 2024        # podgląd zmiennej (województwa)
"""
from __future__ import annotations

import argparse
import time

import pandas as pd
import requests

from lib.log import get_logger, setup_logging

log = get_logger(__name__.split(".")[-1])

BDL = "https://bdl.stat.gov.pl/api/v1"
# plony GUS (dt/ha), województwa
YIELD_VARIABLES = {4332: "pszenica", 4344: "jęczmień", 4380: "rzepak", 4374: "buraki cukrowe"}


def variable(var_id: int, years: range, retries: int = 4) -> pd.DataFrame:
    """Zmienna BDL dla województw -> DataFrame(unit_id, unit_name, year, value); unit_id = kod TERYT (2 cyfry)."""
    rows, url = [], f"{BDL}/data/by-variable/{var_id}"
    params = {"unit-level": 2, "year": list(years), "format": "json", "page-size": 100}
    while url:
        for attempt in range(retries):
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 429:                      # limit zapytań: chwila przerwy
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            break
        body = r.json()
        for unit in body["results"]:
            # id BDL: 12 znaków, np. 011200000000 -> TERYT województwa „12” na pozycjach 3–4
            for v in unit["values"]:
                rows.append(dict(unit_id=unit["id"][2:4], unit_name=unit["name"].lower(), year=int(v["year"]),
                                 value=v["val"]))
        url, params = body.get("links", {}).get("next"), None
    df = pd.DataFrame(rows)
    if not df.empty:
        df.loc[df["value"] == 0, "value"] = None
    log.info("BDL %s: %d wartości (%d jednostek, lata %s–%s)", var_id, df["value"].notna().sum() if len(df) else 0,
             df["unit_id"].nunique() if len(df) else 0, years.start, years.stop - 1)
    return df


def main() -> None:
    p = argparse.ArgumentParser(description="Podgląd zmiennej GUS BDL")
    p.add_argument("var_id", type=int)
    p.add_argument("--from", dest="y0", type=int, default=2001)
    p.add_argument("--to", dest="y1", type=int, default=2024)
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()
    setup_logging(a.verbose)
    df = variable(a.var_id, range(a.y0, a.y1 + 1))
    print(df.pivot(index="unit_name", columns="year", values="value").round(1).to_string())


if __name__ == "__main__":
    main()
