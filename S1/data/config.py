"""Wspólne ustawienia danych S1 — jedno miejsce dla wszystkich skryptów.

Co tu jest:
    lata i miesiące do pobrania, obszar, lista zmiennych ERA5-Land, foldery, próg pól
    i ścieżki w HDFS.

Teoria:
    S1 działa w sezonie 21.03–30.09, a bilans wody liczymy z 60 dni wstecz,
    więc dane są potrzebne od ok. 20 stycznia — stąd miesiące I–IX.
    Zmienne „narastające” (opad, parowanie, promieniowanie) ERA5-Land zapisuje jako sumę
    od początku doby (od 01:00 UTC; o 00:00 suma całej poprzedniej doby) — trzeba je odwrócić.
"""

from pathlib import Path

DATA_DIR = Path(__file__).parent
RAW_DIR = DATA_DIR / "raw"
TABLE_DIR = DATA_DIR / "table"
POINTS_FILE = DATA_DIR / "points.csv"

YEARS = range(2010, 2025)
MONTHS = range(1, 10)
AREA = [55.0, 14.0, 49.0, 24.2]

# nazwa w API CDS -> nazwa w pliku NetCDF
VARIABLES = {
    "total_precipitation": "tp",
    "potential_evaporation": "pev",
    "2m_temperature": "t2m",
    "2m_dewpoint_temperature": "d2m",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
    "surface_solar_radiation_downwards": "ssrd",
    "volumetric_soil_water_layer_1": "swvl1",
    "volumetric_soil_water_layer_2": "swvl2",
    "volumetric_soil_water_layer_3": "swvl3",
}
ACCUMULATED = ["tp", "pev", "ssrd"]

MIN_CROPLAND_SHARE = 0.2

# HDFS: kontener, przez który wrzucamy pliki (na masterze "namenode", na workerze "datanode")
HDFS_CONTAINER = "namenode"
HDFS_RAW_DIR = "/era5land/raw"


def raw_file(year, month):
    return RAW_DIR / f"era5land_{year}_{month:02d}.nc"


def hdfs_file(year, month):
    return f"{HDFS_RAW_DIR}/era5land_{year}_{month:02d}.nc"