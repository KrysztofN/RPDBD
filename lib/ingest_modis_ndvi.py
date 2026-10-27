"""
Pobranie NDVI MODIS dla Polski na HDFS (krok wspólny zespołu: korzystają B1, B2, G, H).

Co robi:
  dla każdego terminu MOD13Q1 z podanego zakresu lat:
    1. skleja 4 kafelki i wycina prostokąt Polski (lib/modis_ingest.py) do pliku tymczasowego,
    2. wysyła go na HDFS: <hdfs-dir>/<rok>/modis_ndvi_poland_<RRRR-MM-DD>.tif (replikacja 1),
    3. usuwa plik tymczasowy.
  Terminy, które już są na HDFS, pomija — skrypt można przerwać i uruchomić ponownie.

Trochę teorii:
  Dane pobieramy raz (z Microsoft Planetary Computer) i dalej liczymy już tylko z HDFS — demo nie zależy od
  zewnętrznego serwera. Replikacja 1, bo każdy plik można odtworzyć tym samym skryptem; jeden termin to
  ok. 18 MB, całe 2001–2024 (552 terminy) ok. 10 GB.

Uruchomienie (z katalogu głównego repo):
  python -m lib.ingest_modis_ndvi --years 2018 --limit 1      # test: jeden termin
  python -m lib.ingest_modis_ndvi --years 2001 2024           # całość (zakres lat od-do)

Gdzie uruchamiać: sieć klastra blokuje magazyn plików Microsoftu (*.blob.core.windows.net), więc pobieramy
z laptopa (przez VPN) i wysyłamy prosto na HDFS. Wtedy trzeba podać adres NameNode i tłumaczenie nazw DataNode'ów:
  RBD_WEBHDFS_URL=http://10.7.11.21:9870
  RBD_HDFS_HOST_MAP="Bigdata-wn212-02=10.7.11.22,Bigdata-wn212-03=10.7.11.23,Bigdata-wn212-04=10.7.11.24,Bigdata-wn212-05=10.7.11.25"
"""
from __future__ import annotations

import argparse
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from lib import boundaries
from lib import modis_ingest as mi
from lib import ndvi_tools as nt
from lib.log import Progress, Timer, get_logger, setup_logging
from lib.webhdfs import DEFAULT_URL, DEFAULT_USER, WebHDFS

ROOT = Path(__file__).resolve().parents[1]
log = get_logger(Path(__file__).stem)

HDFS_DIR = "/rbd/raw/modis_ndvi_poland"
MIN_COMPLETE_BYTES = 1_000_000   # prawdziwy termin ma ok. 17 MB; mniejszy plik = przerwana wysyłka
RETRIES = 5                      # próby na termin przy błędach sieci (30 s, 60 s, 90 s, ...)


def hdfs_path(hdfs_dir: str, date) -> str:
    return f"{hdfs_dir}/{date.year}/modis_ndvi_poland_{date.date()}.tif"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NDVI MODIS (wycinek Polski) -> HDFS")
    p.add_argument("--years", type=int, nargs="+", required=True, help="rok albo zakres: od do (np. 2001 2024)")
    p.add_argument("--limit", type=int, default=None, help="tylko N pierwszych terminów (test)")
    p.add_argument("--workers", type=int, default=4, help="ile terminów naraz")
    p.add_argument("--hdfs-dir", default=HDFS_DIR)
    p.add_argument("--webhdfs-url", default=DEFAULT_URL)
    p.add_argument("--hdfs-user", default=DEFAULT_USER)
    p.add_argument("--replication", type=int, default=1)
    p.add_argument("--data-dir", type=Path, default=ROOT / "data", help="lokalny cache granic powiatów")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)
    start, end = args.years[0], args.years[-1]
    hdfs = WebHDFS(args.webhdfs_url, args.hdfs_user)

    with Timer(log, "katalog terminów MODIS"):
        items = nt.list_modis_items(start, end)
        dates = sorted(pd.to_datetime(items["date"].unique()))
        # pliki mniejsze niż MIN_COMPLETE_BYTES to niedokończona wysyłka (np. zerwany VPN) -> wysyłamy ponownie
        on_hdfs = {f"{args.hdfs_dir}/{y}/{f['pathSuffix']}"
                   for y in range(start, end + 1) for f in hdfs.list(f"{args.hdfs_dir}/{y}")
                   if f["length"] >= MIN_COMPLETE_BYTES}
        todo = [d for d in dates if hdfs_path(args.hdfs_dir, d) not in on_hdfs]
        log.info("lata %d–%d: %d terminów, już na HDFS %d, do pobrania %d", start, end, len(dates),
                 len(dates) - len(todo), len(todo))
        if args.limit:
            todo = todo[: args.limit]
            log.info("--limit %d: pobieram tylko %d", args.limit, len(todo))

    if not todo:
        log.info("nic do zrobienia")
        return
    area = boundaries.load_counties(args.data_dir)
    for year in sorted({d.year for d in todo}):
        hdfs.mkdirs(f"{args.hdfs_dir}/{year}")

    def one(date) -> int:
        day = items[items["date"] == date].to_dict("records")
        if len(day) != 4:
            log.warning("%s: %d kafelków zamiast 4", date.date(), len(day))
        for attempt in range(1, RETRIES + 1):
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    local = mi.clip_date_to_poland(day, area, Path(tmp) / "ndvi.tif")
                    size = local.stat().st_size
                    # overwrite=True: nadpisuje ewentualny niedokończony plik z przerwanej wysyłki
                    hdfs.put(local, hdfs_path(args.hdfs_dir, date), replication=args.replication, overwrite=True)
                return size
            except Exception as e:          # sieć (VPN), Planetary Computer, HDFS — ponawiamy
                if attempt == RETRIES:
                    raise
                wait = 30 * attempt
                log.warning("%s: próba %d/%d nieudana (%s) — ponawiam za %d s",
                            date.date(), attempt, RETRIES, type(e).__name__, wait)
                time.sleep(wait)

    total = 0
    progress = Progress(log, len(todo), "terminy -> HDFS")
    with Timer(log, "pobieranie na HDFS"), ThreadPoolExecutor(args.workers) as ex:
        for date, size in zip(todo, ex.map(one, todo)):
            total += size
            progress.update(f"{date.date()} ({size / 1e6:.1f} MB)")
    log.info("zapisano %d plików, %.2f GB, replikacja %d -> %s", len(todo), total / 1e9, args.replication, args.hdfs_dir)


if __name__ == "__main__":
    main()
