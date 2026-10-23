"""Sprawdzenie pobranych plików ERA5-Land — czy pobieranie się udało.

Co robi:
    Dla każdego oczekiwanego miesiąca (config.py) sprawdza plik w S1/data/raw/:
    czy istnieje, czy się otwiera, czy ma wszystkie 10 zmiennych,
    czy ma tyle godzin, ile trzeba (dni × 24), bez przerw, i czy obszar się zgadza.
    Wypisuje listę problemów; kod wyjścia 1, jeśli jakieś są.

Uruchomienie: python S1/data/check.py [folder_z_plikami]

Teoria:
    To kontrola POBRANIA (kompletność archiwum), a nie jakości danych.
    Jakość pojedynczych rekordów sprawdza dopiero strumień S1 — tak jak w prawdziwym
    systemie, gdzie dane przychodzą z czujników na bieżąco.
"""

import calendar
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from config import AREA, MONTHS, RAW_DIR, VARIABLES, YEARS

EXPECTED_LAT = 61
EXPECTED_LON = 103


def check_file(path, year, month):
    problems = []
    try:
        ds = xr.open_dataset(path)
    except Exception as error:
        return [f"nie otwiera się ({error})"]

    missing = [name for name in VARIABLES.values() if name not in ds.data_vars]
    if missing:
        problems.append(f"brak zmiennych: {', '.join(missing)}")

    times = pd.to_datetime(ds["valid_time"].values)
    expected_hours = calendar.monthrange(year, month)[1] * 24
    if len(times) != expected_hours:
        problems.append(f"godzin {len(times)}, powinno być {expected_hours}")
    if len(times) > 1 and not (np.diff(times) == np.timedelta64(1, "h")).all():
        problems.append("przerwy w godzinach")
    if times[0] != pd.Timestamp(year, month, 1):
        problems.append(f"zaczyna się od {times[0]}, a nie od {year}-{month:02d}-01 00:00")

    north, west, south, east = AREA
    if ds.sizes["latitude"] != EXPECTED_LAT or ds.sizes["longitude"] != EXPECTED_LON:
        problems.append(f"siatka {ds.sizes['latitude']} × {ds.sizes['longitude']}, powinno być {EXPECTED_LAT} × {EXPECTED_LON}")
    elif not np.isclose([ds.latitude.max(), ds.longitude.min(), ds.latitude.min(), ds.longitude.max()], [north, west, south, east]).all():
        problems.append("inny obszar niż w config.py")

    ds.close()
    return problems


def main():
    raw_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else RAW_DIR
    expected = {f"era5land_{y}_{m:02d}.nc": (y, m) for y in YEARS for m in MONTHS}
    found = {path.name for path in raw_dir.glob("*.nc")}

    missing_files = sorted(set(expected) - found)
    extra_files = sorted(found - set(expected))
    bad = {}
    for name in sorted(found & set(expected)):
        problems = check_file(raw_dir / name, *expected[name])
        if problems:
            bad[name] = problems

    print(f"folder: {raw_dir}")
    print(f"oczekiwanych plików: {len(expected)}, jest: {len(found & set(expected))}, poprawnych: {len(found & set(expected)) - len(bad)}")
    if missing_files:
        print(f"brakuje {len(missing_files)} plików, np.: {', '.join(missing_files[:5])}")
    if extra_files:
        print(f"pliki spoza planu (pomijam): {', '.join(extra_files)}")
    for name, problems in bad.items():
        print(f"{name}: {'; '.join(problems)}")

    sys.exit(1 if missing_files or bad else 0)


if __name__ == "__main__":
    main()
