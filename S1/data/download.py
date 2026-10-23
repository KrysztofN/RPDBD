"""Pobieranie pogody ERA5-Land dla Polski — miesiąc po miesiącu, prosto do HDFS.

Co robi:
    Pobiera z Copernicus Climate Data Store 10 zmiennych godzinowych potrzebnych do S1
    (opad, parowanie, temperatura, punkt rosy, wiatr, promieniowanie, wilgotność gleby)
    dla lat i miesięcy z config.py (2010–2024, I–IX).
    Każdy miesiąc: pobranie na dysk lokalny -> wrzucenie do HDFS -> usunięcie lokalnej kopii.
    Wynik w HDFS: /era5land/raw/era5land_RRRR_MM.nc.
    Miesiące, które są już w HDFS, pomija, więc można przerwać i uruchomić ponownie.

Wymaga: konta CDS, pliku ~/.cdsapirc z kluczem, akceptacji licencji ERA5-Land,
    pip install "cdsapi>=0.7.7", działającego kontenera HDFS na tej maszynie.

Teoria:
    ERA5-Land to reanaliza — odtworzenie pogody z przeszłości (model pogodowy poprawiany
    pomiarami), na siatce co 0,1° (~11 × 7 km), dla każdej godziny.
    CDS ma limit 12 000 „pól” na zapytanie (zmienna × dzień × godzina):
    10 zmiennych × 31 dni × 24 h = 7 440, więc jedno zapytanie = jeden miesiąc.
"""

import calendar
import subprocess

import cdsapi

from config import (AREA, HDFS_CONTAINER, HDFS_RAW_DIR, MONTHS, RAW_DIR,
                    VARIABLES, YEARS, hdfs_file, raw_file)

HDFS = ["docker", "exec", "-i", HDFS_CONTAINER, "hdfs", "dfs"]


def request_for(year, month):
    days = calendar.monthrange(year, month)[1]
    return {
        "variable": list(VARIABLES),
        "year": str(year),
        "month": f"{month:02d}",
        "day": [f"{d:02d}" for d in range(1, days + 1)],
        "time": [f"{h:02d}:00" for h in range(24)],
        "area": AREA,
        "data_format": "netcdf",
        "download_format": "unarchived",
    }


def hdfs_existing():
    """Zbiór ścieżek plików, które już są w HDFS."""
    subprocess.run(HDFS + ["-mkdir", "-p", HDFS_RAW_DIR], check=True)
    out = subprocess.run(HDFS + ["-ls", "-C", HDFS_RAW_DIR],
                         capture_output=True, text=True, check=True).stdout
    return set(out.split())


def upload(local_path, hdfs_path):
    """Wysyła plik do HDFS przez stdin kontenera (bez kopiowania do /opt/hdfs/upload)."""
    with open(local_path, "rb") as f:
        subprocess.run(HDFS + ["-put", "-f", "-", hdfs_path], stdin=f, check=True)


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    client = cdsapi.Client()
    existing = hdfs_existing()

    for year in YEARS:
        for month in MONTHS:
            dest = hdfs_file(year, month)
            if dest in existing:
                continue

            path = raw_file(year, month)
            if not path.exists():
                # ERA5-Land: jeden miesiac calej Polski, 10 zmiennych (~7 440 pol, limit 12 000)
                print(f"pobieram {year}-{month:02d}", flush=True)
                tmp_path = path.with_suffix(".part")
                client.retrieve("reanalysis-era5-land", request_for(year, month), str(tmp_path))
                tmp_path.rename(path)

            print(f"wrzucam do HDFS {dest}", flush=True)
            upload(path, dest)
            path.unlink()


if __name__ == "__main__":
    main()