"""Pobieranie pogody ERA5-Land dla Polski — miesiąc po miesiącu.

Co robi:
    Pobiera z Copernicus Climate Data Store 10 zmiennych godzinowych potrzebnych do S1
    (opad, parowanie, temperatura, punkt rosy, wiatr, promieniowanie, wilgotność gleby)
    dla lat i miesięcy z config.py (2010–2024, I–IX).
    Jeden plik = jeden miesiąc: S1/data/raw/era5land_RRRR_MM.nc.
    Pobrane miesiące pomija, więc można przerwać i uruchomić ponownie.

Wymaga: konta CDS, pliku ~/.cdsapirc z kluczem, akceptacji licencji ERA5-Land,
    pip install "cdsapi>=0.7.7".

Teoria:
    ERA5-Land to reanaliza — odtworzenie pogody z przeszłości (model pogodowy poprawiany
    pomiarami), na siatce co 0,1° (~11 × 7 km), dla każdej godziny.
    CDS ma limit 12 000 „pól” na zapytanie (zmienna × dzień × godzina):
    10 zmiennych × 31 dni × 24 h = 7 440, więc jedno zapytanie = jeden miesiąc.
"""

import calendar

import cdsapi

from config import AREA, MONTHS, RAW_DIR, VARIABLES, YEARS, raw_file


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


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    client = cdsapi.Client()

    for year in YEARS:
        for month in MONTHS:
            path = raw_file(year, month)
            if path.exists():
                continue
            # ERA5-Land: jeden miesiac calej Polski, 10 zmiennych (~7 440 pol, limit 12 000)
            print(f"pobieram {year}-{month:02d}")
            tmp_path = path.with_suffix(".part")
            client.retrieve("reanalysis-era5-land", request_for(year, month), str(tmp_path))
            tmp_path.rename(path)


if __name__ == "__main__":
    main()
