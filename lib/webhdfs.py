"""
Prosty klient HDFS przez WebHDFS (HTTP) — bez instalowania klienta Hadoop w kontenerze.

Co robi:
  WebHDFS(url, user).put(lokalny_plik, ścieżka_hdfs, replication=1)  — zapis pliku z podaną replikacją
  .exists(), .list(), .mkdirs(), .status()                            — odczyt informacji o plikach
  Z linii poleceń: python -m lib.webhdfs put <plik|katalog> <katalog_hdfs> | ls <ścieżka>

Trochę teorii:
  NameNode wystawia REST API na porcie 9870. Zapis jest dwuetapowy: NameNode odpowiada przekierowaniem (307)
  na konkretny DataNode (port 9864), a dane wysyłamy już bezpośrednio do niego. Replikację podajemy przy
  zapisie (parametr `replication`), więc nasze pliki mogą mieć 1 kopię, a domyślna replikacja klastra (3)
  zostaje bez zmian dla innych. Uwierzytelnianie „simple”: nazwa użytkownika HDFS w parametrze `user.name`.
"""
from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests

from .log import get_logger

log = get_logger(__name__)

DEFAULT_URL = os.environ.get("RBD_WEBHDFS_URL", "http://localhost:9870")
DEFAULT_USER = os.environ.get("RBD_HDFS_USER", "hadoop")


def _parse_host_map(text: str) -> dict[str, str]:
    """'host1=10.0.0.1,host2=10.0.0.2' -> {'host1': '10.0.0.1', ...}"""
    pairs = (item.split("=", 1) for item in text.split(",") if "=" in item)
    return {k.strip().lower(): v.strip() for k, v in pairs}   # nazwy hostów bez rozróżniania wielkości liter


class WebHDFS:
    """
    host_map: tłumaczenie nazw DataNode'ów na adresy IP — potrzebne poza klastrem (np. z laptopa przez VPN),
    bo NameNode przekierowuje zapis na nazwę maszyny (np. Bigdata-wn212-03), której laptop nie zna.
    Domyślnie ze zmiennej RBD_HDFS_HOST_MAP="Bigdata-wn212-02=10.7.11.22,...".
    """

    def __init__(self, url: str = DEFAULT_URL, user: str = DEFAULT_USER, timeout: int = 120,
                 host_map: dict[str, str] | None = None):
        self.base = url.rstrip("/") + "/webhdfs/v1"
        self.user = user
        self.timeout = timeout
        self.host_map = ({k.lower(): v for k, v in host_map.items()} if host_map is not None
                         else _parse_host_map(os.environ.get("RBD_HDFS_HOST_MAP", "")))
        self.session = requests.Session()

    def _resolve(self, location: str) -> str:
        """Podmienia nazwę DataNode'a w adresie przekierowania na IP (jeśli jest w host_map)."""
        parts = urlsplit(location)
        ip = self.host_map.get((parts.hostname or "").lower())
        if not ip:
            return location
        netloc = ip + (f":{parts.port}" if parts.port else "")
        return urlunsplit(parts._replace(netloc=netloc))

    def _url(self, path: str) -> str:
        return self.base + ("/" + path.lstrip("/"))

    def _params(self, op: str, **extra) -> dict:
        return {"op": op, "user.name": self.user, **extra}

    def status(self, path: str) -> dict | None:
        r = self.session.get(self._url(path), params=self._params("GETFILESTATUS"), timeout=self.timeout)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()["FileStatus"]

    def exists(self, path: str) -> bool:
        return self.status(path) is not None

    def list(self, path: str) -> list[dict]:
        r = self.session.get(self._url(path), params=self._params("LISTSTATUS"), timeout=self.timeout)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json()["FileStatuses"]["FileStatus"]

    def mkdirs(self, path: str) -> None:
        r = self.session.put(self._url(path), params=self._params("MKDIRS"), timeout=self.timeout)
        r.raise_for_status()

    def put(self, local_path: str | Path, hdfs_path: str, replication: int = 1, overwrite: bool = False) -> None:
        """Zapis pliku: NameNode -> przekierowanie -> DataNode. Replikacja ustawiana dla tego pliku."""
        params = self._params("CREATE", replication=replication, overwrite=str(overwrite).lower())
        r = self.session.put(self._url(hdfs_path), params=params, allow_redirects=False, timeout=self.timeout)
        if r.status_code != 307:
            r.raise_for_status()
            raise RuntimeError(f"WebHDFS CREATE {hdfs_path}: oczekiwano przekierowania 307, jest {r.status_code}")
        with open(local_path, "rb") as f:
            r2 = self.session.put(self._resolve(r.headers["Location"]), data=f, timeout=self.timeout,
                                  headers={"Content-Type": "application/octet-stream"})
        r2.raise_for_status()
        log.debug("HDFS <- %s (%s, replikacja %d)", hdfs_path, Path(local_path).name, replication)


# --------------------------------------------------------------------------- linia poleceń
def _cli() -> None:
    """
    python -m lib.webhdfs put <plik|katalog> <katalog_hdfs> [--replication 1] [--overwrite]
    python -m lib.webhdfs ls <ścieżka_hdfs>
    Katalog wysyłany jest rekurencyjnie (z zachowaniem podkatalogów). Adres i mapa hostów: zmienne
    RBD_WEBHDFS_URL, RBD_HDFS_HOST_MAP (patrz WebHDFS).
    W Git Bash na Windows ustaw MSYS_NO_PATHCONV=1 — inaczej „/rbd/...” zamieni się na „C:/Program Files/Git/rbd/...”.
    """
    global log
    log = get_logger("webhdfs")
    import argparse

    from .log import Progress, setup_logging

    p = argparse.ArgumentParser(description="HDFS przez WebHDFS: put / ls")
    sub = p.add_subparsers(dest="cmd", required=True)
    put = sub.add_parser("put", help="wyślij plik albo katalog")
    put.add_argument("local", type=Path)
    put.add_argument("hdfs_dir")
    put.add_argument("--replication", type=int, default=1)
    put.add_argument("--overwrite", action="store_true")
    ls = sub.add_parser("ls", help="lista plików")
    ls.add_argument("hdfs_path")
    for s in (put, ls):
        s.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    setup_logging(args.verbose)
    hdfs = WebHDFS()

    if args.cmd == "ls":
        for f in hdfs.list(args.hdfs_path):
            kind = "katalog" if f["type"] == "DIRECTORY" else f"{f['length'] / 1e6:8.1f} MB, replikacja {f['replication']}"
            print(f"{f['pathSuffix']:<45} {kind}")
        return

    files = [args.local] if args.local.is_file() else sorted(f for f in args.local.rglob("*") if f.is_file())
    base = args.local.parent if args.local.is_file() else args.local
    progress = Progress(log, len(files), f"wysyłka {args.local.name}", every=1 / max(1, len(files)))
    for f in files:
        target = f"{args.hdfs_dir.rstrip('/')}/{f.relative_to(base).as_posix()}"
        hdfs.mkdirs(target.rsplit("/", 1)[0])
        hdfs.put(f, target, replication=args.replication, overwrite=args.overwrite)
        progress.update(target)
        log.info("%s -> %s (%.1f MB)", f.name, target, f.stat().st_size / 1e6)


if __name__ == "__main__":
    _cli()
