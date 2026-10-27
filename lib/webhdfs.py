"""
Prosty klient HDFS przez WebHDFS (HTTP) — bez instalowania klienta Hadoop w kontenerze.

Co robi:
  WebHDFS(url, user).put(lokalny_plik, ścieżka_hdfs, replication=1)  — zapis pliku z podaną replikacją
  .exists(), .list(), .mkdirs(), .status()                            — odczyt informacji o plikach

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
