"""
Prosty klient ClickHouse przez HTTP (port 8123) — zapytania i wstawianie tabel pandas.

Co robi:
  ClickHouse().command(sql)            — polecenie (CREATE, DELETE, ...)
  ClickHouse().query_df(sql)           — wynik zapytania jako pandas.DataFrame
  ClickHouse().insert_df(tabela, df)   — wstawia DataFrame (wysyłany jako Parquet — szybko, z typami)

Trochę teorii:
  ClickHouse przyjmuje dane przez HTTP w wielu formatach; Parquet zachowuje typy kolumn (liczby, daty, NULL)
  i jest zwarty, więc nie potrzebujemy osobnego sterownika (JDBC) ani dodatkowych bibliotek.
  Hasło bierzemy ze zmiennej CLICKHOUSE_PASSWORD (na klastrze: ~/rbd-build/clickhouse/.env) — nigdy z kodu.
"""
from __future__ import annotations

import io
import os

import pandas as pd
import requests

from .log import get_logger

log = get_logger(__name__)

DEFAULT_URL = os.environ.get("RBD_CLICKHOUSE_URL", "http://10.7.11.21:8123")


class ClickHouse:
    def __init__(self, url: str = DEFAULT_URL, user: str = "rbd", password: str | None = None,
                 database: str = "rbd", timeout: int = 300):
        password = password if password is not None else os.environ.get("CLICKHOUSE_PASSWORD", "")
        if not password:
            raise RuntimeError("brak hasła: ustaw CLICKHOUSE_PASSWORD (na klastrze w ~/rbd-build/clickhouse/.env)")
        self.url, self.database, self.timeout = url.rstrip("/"), database, timeout
        self.session = requests.Session()
        self.session.auth = (user, password)

    def _post(self, sql: str, data: bytes | None = None, **params) -> requests.Response:
        params = {"database": self.database, **params}
        if data is None:
            r = self.session.post(self.url, params=params, data=sql.encode(), timeout=self.timeout)
        else:
            r = self.session.post(self.url, params={**params, "query": sql}, data=data, timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(f"ClickHouse {r.status_code}: {r.text[:500]}")
        return r

    def command(self, sql: str) -> str:
        return self._post(sql).text.strip()

    def query_df(self, sql: str) -> pd.DataFrame:
        r = self._post(f"{sql.rstrip().rstrip(';')} FORMAT Parquet")
        return pd.read_parquet(io.BytesIO(r.content))

    def insert_df(self, table: str, df: pd.DataFrame) -> int:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        self._post(f"INSERT INTO {table} FORMAT Parquet", data=buf.getvalue())
        log.info("ClickHouse <- %s: %d wierszy", table, len(df))
        return len(df)
