"""Zamiana plików ERA5-Land na tabelę „punkt × godzina” (Parquet).

Co robi:
    Czyta miesięczne pliki NetCDF z S1/data/raw/ i dla punktów z polami (points.csv,
    udział pól ≥ MIN_CROPLAND_SHARE) zapisuje tabelę, w której jeden wiersz to jeden punkt
    w jednej godzinie: point_id, ts_utc, tp, pev, t2m, d2m, u10, v10, ssrd, swvl1, swvl2, swvl3.
    Wynik: S1/data/table/era5land_RRRR_MM.parquet. Gotowe miesiące pomija.

Uruchomienie: python S1/data/to_table.py [folder_z_plikami]

Teoria:
    Plik NetCDF to „kostka”: dla każdej godziny cała siatka liczb. Do strumienia i obliczeń
    potrzebujemy wierszy — jak pomiary z 2 805 stacji co godzinę.
    Opad (tp), parowanie (pev) i promieniowanie (ssrd) ERA5-Land zapisuje NARASTAJĄCO:
    o 01:00 jest suma z pierwszej godziny doby, potem coraz większe sumy, o 00:00 suma całej
    poprzedniej doby. Wartość z jednej godziny = obecna − poprzednia, a o 01:00 bierzemy wprost.
    O 00:00 pierwszego dnia miesiąca „poprzednia” godzina jest w pliku poprzedniego miesiąca —
    jeśli go nie ma, ta jedna godzina zostaje pusta.
    Jednostek nie zmieniamy (kelwiny, metry, J/m²) i niczego nie czyścimy —
    walidacja i przeliczenia dzieją się w strumieniu S1.
"""

import sys
from pathlib import Path

import pandas as pd
import xarray as xr

from config import ACCUMULATED, MIN_CROPLAND_SHARE, POINTS_FILE, RAW_DIR, VARIABLES


def field_points():
    points = pd.read_csv(POINTS_FILE)
    return points[points["cropland_share"] >= MIN_CROPLAND_SHARE][["point_id", "lat", "lon"]]


def previous_file(path):
    year, month = map(int, path.stem.split("_")[1:])
    year, month = (year - 1, 12) if month == 1 else (year, month - 1)
    return path.with_name(f"era5land_{year}_{month:02d}.nc")


def at_points(ds, points):
    lat = xr.DataArray(points["lat"].values, dims="point")
    lon = xr.DataArray(points["lon"].values, dims="point")
    return ds.sel(latitude=lat, longitude=lon, method="nearest")


def deaccumulate(values, previous_last, hours):
    hourly = values.diff("valid_time")
    first = values.isel(valid_time=0) - previous_last if previous_last is not None else values.isel(valid_time=0) * float("nan")
    hourly = xr.concat([first.expand_dims(valid_time=values.valid_time[:1]), hourly], dim="valid_time")
    return hourly.where(hours != 1, values)


def convert(path, points):
    ds = at_points(xr.open_dataset(path), points)
    names = [name for name in VARIABLES.values() if name in ds.data_vars]
    missing = sorted(set(VARIABLES.values()) - set(names))
    if missing:
        print(f"  uwaga: brak zmiennych {', '.join(missing)}")

    previous = previous_file(path)
    previous_ds = at_points(xr.open_dataset(previous), points).isel(valid_time=-1) if previous.exists() else None
    hours = ds["valid_time"].dt.hour

    for name in ACCUMULATED:
        if name in names:
            last = previous_ds[name] if previous_ds is not None and name in previous_ds else None
            ds[name] = deaccumulate(ds[name], last, hours).astype("float32")

    table = ds[names].to_dataframe().reset_index()
    table["point_id"] = points["point_id"].values[table["point"].values]
    table = table.rename(columns={"valid_time": "ts_utc"})
    return table[["point_id", "ts_utc", *names]]


def main():
    raw_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else RAW_DIR
    table_dir = raw_dir.parent / "table"
    table_dir.mkdir(exist_ok=True)
    points = field_points().reset_index(drop=True)
    print(f"punkty z polami: {len(points)}")

    for path in sorted(raw_dir.glob("era5land_*.nc")):
        out = table_dir / f"{path.stem}.parquet"
        if out.exists():
            continue
        # kostka NetCDF -> wiersze punkt x godzina, dla jednego miesiaca
        print(f"przerabiam {path.name}")
        table = convert(path, points)
        table.to_parquet(out, index=False)
        print(f"  {len(table):,} wierszy -> {out.name}")


if __name__ == "__main__":
    main()
