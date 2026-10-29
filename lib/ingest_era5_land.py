"""
Pobranie pogody ERA5-Land (Copernicus CDS) dla Polski na HDFS — krok wspólny zespołu (S1, S2, D, B1/B2, H).

Co robi:
  dla każdego miesiąca z zakresu:
    1. zamawia w Copernicus CDS dane godzinowe ERA5-Land dla prostokąta Polski (10 zmiennych, niżej),
    2. czeka w kolejce CDS i pobiera plik (NetCDF; czasem ZIP z dwoma plikami NetCDF — patrz niżej),
    3. wysyła go na HDFS: <hdfs-dir>/<rok>/era5_land_<RRRR-MM>_<instant|accum>.nc (replikacja 1), usuwa kopię lokalną.
  Na miesiąc 2 zamówienia: zmienne chwilowe (instant) i narastające (accum) — limit wielkości zamówienia w CDS.
  Miesiące, które już są na HDFS, pomija — można przerwać i uruchomić ponownie.

Zmienne (siatka 0,1°, ok. 9 km, co godzinę):
  2m_temperature, skin_temperature, soil_temperature_level_1  temperatura powietrza, powierzchni, gleby (S2, D, B1/B2)
  2m_dewpoint_temperature, 10m_u/v_component_of_wind,
  surface_solar_radiation_downwards                           do parowania ET₀ (FAO-56) — bilans wodny (S1, H, B1/B2)
  total_precipitation                                         opad (narasta od 00 UTC — trzeba odejmować kolejne godziny)
  volumetric_soil_water_layer_1                               wilgotność gleby 0–7 cm (S1)
  snow_depth                                                  śnieg (D: wymarzanie ozimin)

Trochę teorii:
  ERA5-Land to reanaliza ECMWF: model pogody „dopasowany” do obserwacji, bez dziur w czasie i przestrzeni.
  CDS przyjmuje zamówienie, kolejkuje je (od minut do godzin) i dopiero potem daje plik — dlatego zamawiamy
  całe miesiące i pobieramy raz. Zmienne „chwilowe” (temperatura) i „narastające” (opad, promieniowanie) CDS
  może oddać jako dwa pliki NetCDF w jednym ZIP-ie — zapisujemy wtedy ZIP bez zmian, rozpakowuje Spark.

Klucz: plik ~/.cdsapirc (url + key) — tylko na klastrze, prawa 600, do kontenera podpinany tylko do odczytu.
Uruchomienie (na masterze, w kontenerze rbd-python):
  docker run --rm --network host -v ~/rbd-build/app:/app -v ~/.cdsapirc:/tmp/.cdsapirc:ro -e HOME=/tmp \
    rbd-python:0.1 python3 -m lib.ingest_era5_land --months 2018-06 2018-06 --webhdfs-url http://localhost:9870
"""
from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from lib.log import Progress, Timer, get_logger, setup_logging
from lib.webhdfs import DEFAULT_URL, DEFAULT_USER, WebHDFS

log = get_logger(Path(__file__).stem)

DATASET = "reanalysis-era5-land"
HDFS_DIR = "/rbd/raw/era5_land"
POLAND_AREA = [55.0, 14.0, 49.0, 24.2]          # północ, zachód, południe, wschód (jak w notebooks/01_pogoda)
# dwa zamówienia na miesiąc: limit CDS to 12 000 „pól” (zmienne × dni × godziny × 2), a cały miesiąc z 10 zmiennymi
# to ok. 14 400. Podział wg rodzaju zmiennej daje też po jednym czystym pliku NetCDF (bez ZIP-a).
VARIABLE_GROUPS = {
    "instant": ["2m_temperature", "2m_dewpoint_temperature", "skin_temperature", "soil_temperature_level_1",
                "10m_u_component_of_wind", "10m_v_component_of_wind", "volumetric_soil_water_layer_1", "snow_depth"],
    "accum": ["total_precipitation", "surface_solar_radiation_downwards"],
}
VARIABLES = [v for vs in VARIABLE_GROUPS.values() for v in vs]
MIN_COMPLETE_BYTES = 1_000_000
QUEUE_WAIT_S, QUEUE_RETRIES = 120, 60   # pełna kolejka CDS: czekamy 2 min, maks. ok. 2 h na zamówienie


def file_kind(path: Path) -> str:
    """Rozszerzenie wg zawartości: ZIP ('PK') albo NetCDF (HDF5 / klasyczny CDF)."""
    head = path.read_bytes()[:8]
    if head.startswith(b"PK"):
        return "zip"
    if head.startswith(b"\x89HDF") or head.startswith(b"CDF"):
        return "nc"
    raise ValueError(f"nieznany format pliku z CDS (pierwsze bajty: {head!r})")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ERA5-Land (CDS) dla Polski -> HDFS, miesiąc po miesiącu")
    p.add_argument("--months", nargs=2, metavar=("OD", "DO"), required=True, help="np. 2001-01 2024-12")
    p.add_argument("--hdfs-dir", default=HDFS_DIR)
    p.add_argument("--webhdfs-url", default=DEFAULT_URL)
    p.add_argument("--hdfs-user", default=DEFAULT_USER)
    p.add_argument("--replication", type=int, default=1)
    p.add_argument("--workers", type=int, default=2,
                   help="ile zamówień naraz w kolejce CDS (limit na użytkownika; klucz jest wspólny dla zespołu)")
    p.add_argument("--cache-dir", default="data/era5_cache",
                   help="pobrane pliki czekają tu do udanej wysyłki na HDFS (ponowne uruchomienie ich nie zamawia)")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)
    import cdsapi

    hdfs = WebHDFS(args.webhdfs_url, args.hdfs_user)
    months = pd.period_range(args.months[0], args.months[1], freq="M")
    done = {f"{args.hdfs_dir}/{y}/{f['pathSuffix'].rsplit('.', 1)[0]}"
            for y in sorted({m.year for m in months}) for f in hdfs.list(f"{args.hdfs_dir}/{y}")
            if f["length"] >= MIN_COMPLETE_BYTES}
    todo = [(m, g) for m in months for g in VARIABLE_GROUPS
            if f"{args.hdfs_dir}/{m.year}/era5_land_{m}_{g}" not in done]
    log.info("miesiące %s–%s: %d (= %d zamówień), już na HDFS %d, do pobrania %d (%d zmiennych, co godzinę, obszar %s)",
             months[0], months[-1], len(months), 2 * len(months), 2 * len(months) - len(todo), len(todo),
             len(VARIABLES), POLAND_AREA)
    if not todo:
        return

    client = cdsapi.Client(quiet=True, progress=False)
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    def one(job) -> int:
        """Jedno zamówienie: (kopia z dysku albo CDS) -> HDFS -> usunięcie kopii. Zwraca rozmiar w bajtach."""
        m, group = job
        cached = cache_dir / f"era5_land_{m}_{group}.download"
        t0 = time.time()
        if cached.exists():
            log.info("%s %s: plik już na dysku (%s) — bez ponownego zamawiania w CDS", m, group, cached.name)
        else:
            request = {
                "variable": VARIABLE_GROUPS[group],
                "year": f"{m.year}", "month": f"{m.month:02d}",
                "day": [f"{d:02d}" for d in range(1, m.days_in_month + 1)],
                "time": [f"{h:02d}:00" for h in range(24)],
                "area": POLAND_AREA,
                "data_format": "netcdf",
                "download_format": "unarchived",
            }
            part = cached.with_suffix(".part")
            with Timer(log, f"CDS {m} {group} (kolejka + pobranie)"):
                for attempt in range(QUEUE_RETRIES):
                    try:
                        client.retrieve(DATASET, request, str(part))
                        break
                    except Exception as e:           # CDS odrzuca zamówienie, gdy kolejka użytkownika jest pełna
                        text = str(e)
                        if attempt + 1 < QUEUE_RETRIES and ("Maximum number queued requests" in text
                                                             or "has been rejected" in text):
                            log.info("%s %s: kolejka CDS pełna — czekam %d s (próba %d)", m, group, QUEUE_WAIT_S,
                                     attempt + 1)
                            time.sleep(QUEUE_WAIT_S)
                            continue
                        raise
            part.rename(cached)                      # kompletny plik dostaje właściwą nazwę dopiero po pobraniu
        kind = file_kind(cached)
        size = cached.stat().st_size
        target = f"{args.hdfs_dir}/{m.year}/era5_land_{m}_{group}.{kind}"
        hdfs.mkdirs(f"{args.hdfs_dir}/{m.year}")
        hdfs.put(cached, target, replication=args.replication, overwrite=True)
        cached.unlink()                              # kopię usuwamy dopiero po udanej wysyłce
        log.info("%s %s: %.1f MB (%s) w %.0f s -> %s", m, group, size / 1e6, kind.upper(), time.time() - t0, target)
        return size

    progress = Progress(log, len(todo), "zamówienia ERA5-Land -> HDFS", every=1 / max(1, len(todo)))
    total, failed = 0, []
    with ThreadPoolExecutor(args.workers) as ex:
        futures = {ex.submit(one, job): job for job in todo}
        for fut in as_completed(futures):
            m, group = futures[fut]
            try:
                total += fut.result()
            except Exception as e:                   # jedno nieudane zamówienie nie zatrzymuje reszty
                failed.append(f"{m} {group}")
                log.warning("%s %s: nieudane (%s: %s) — uruchom ponownie, by dokończyć", m, group,
                            type(e).__name__, str(e)[:200])
            progress.update(f"{m} {group}")
    log.info("zapisano %d plików, %.2f GB, replikacja %d -> %s; nieudane: %d %s", len(todo) - len(failed),
             total / 1e9, args.replication, args.hdfs_dir, len(failed), failed[:10])
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
