"""
Wspólne ustawienie logów dla skryptów projektu.

Poziomy (jedna zasada dla wszystkich skryptów):
  INFO    (domyślnie) start i koniec etapu, ile elementów, czas, gdzie zapisano wynik, postęp co ok. 25%
  DEBUG   (-v)        szczegóły: pojedyncze kawałki danych, parametry, pominięte elementy
  WARNING             coś pominięto albo trzeba było ponowić; ERROR — etap przerwany
Komunikaty bibliotek zewnętrznych (rasterio, urllib3, ...) pokazujemy dopiero od WARNING.

Użycie w skrypcie:
    from lib.log import setup_logging, get_logger, Timer
    setup_logging(verbose=args.verbose)
    log = get_logger(__name__)
W module biblioteki wystarczy: log = get_logger(__name__)  (setup robi skrypt).
"""
from __future__ import annotations

import logging
import os
import sys
import time

FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
DATEFMT = "%H:%M:%S"
QUIET_LIBS = ("rasterio", "urllib3", "requests", "fiona", "pyogrio", "matplotlib", "PIL", "asyncio")


def setup_logging(verbose: bool = False, quiet: bool = False) -> None:
    """verbose -> DEBUG, quiet -> WARNING, inaczej INFO. Zmienna RBD_LOG (np. DEBUG) ma pierwszeństwo."""
    level = os.environ.get("RBD_LOG") or ("DEBUG" if verbose else "WARNING" if quiet else "INFO")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(FORMAT, DATEFMT))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    for name in QUIET_LIBS:
        logging.getLogger(name).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Krótka nazwa w logach: 'lib.eucropmap' -> 'eucropmap', '__main__' zostaje."""
    return logging.getLogger(name.rsplit(".", 1)[-1])


class Progress:
    """Postęp pętli: loguje INFO co `every` (domyślnie 25%) i zawsze na końcu; każdy krok tylko na DEBUG."""

    def __init__(self, log: logging.Logger, total: int, what: str, every: float = 0.25):
        self.log, self.total, self.what = log, total, what
        self.step = max(1, int(total * every))
        self.done, self.t0 = 0, time.time()

    def update(self, detail: str = "") -> None:
        self.done += 1
        if detail:
            self.log.debug("%s %d/%d: %s", self.what, self.done, self.total, detail)
        if self.done % self.step == 0 or self.done == self.total:
            self.log.info("%s: %d/%d (%.0f s)", self.what, self.done, self.total, time.time() - self.t0)


class Timer:
    """with Timer(log, "etap"): ...  -> INFO 'etap: start' i 'etap: gotowe w X s'."""

    def __init__(self, log: logging.Logger, name: str):
        self.log, self.name = log, name

    def __enter__(self):
        self.t0 = time.time()
        self.log.info("%s: start", self.name)
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.log.info("%s: gotowe w %.0f s", self.name, time.time() - self.t0)
        else:
            self.log.error("%s: przerwane po %.0f s", self.name, time.time() - self.t0)
        return False
