"""
B1 + B2 razem: widoki w ClickHouse, które do stanu upraw (B1) dopisują kontekst przebiegu sezonu (B2).

Co robi (tworzy / odtwarza widoki, nic nie przelicza):
  rbd.b1b2_periods   każdy termin B1 + faza sezonu (przed sezonem / wzrost / dojrzewanie / po sezonie)
                     + wyjaśnienie, gdy odchylenie wynika z przesunięcia sezonu, np.
                     „gorzej niż zwykle” wiosną przy późnym starcie = rośliny spóźnione, niekoniecznie słabsze
  rbd.b1b2_season    sezon IV–IX z B1 + daty i statusy B2 tego samego powiatu i roku (do biuletynu)

Trochę teorii:
  B1 porównuje zieloność z typowym poziomem w tym samym terminie. Gdy sezon jest przesunięty, porównanie
  „termin do terminu” myli: przy późnym starcie wiosną NDVI jest niskie, choć rośliny są zdrowe — dogonią normę.
  B2 mówi, czy sezon jest przesunięty, więc razem dają poprawną interpretację. Widok (VIEW) liczy to w chwili
  zapytania, więc B1 i B2 mogą się przeliczać niezależnie (osobne zadania w Airflow).

Uruchomienie (na masterze, w kontenerze rbd-python, z hasłem ClickHouse w zmiennej):
  python3 -m B1_B2.b1b2_context
"""
from __future__ import annotations

from pathlib import Path

from lib.log import get_logger, setup_logging

log = get_logger(Path(__file__).stem)

PERIODS_VIEW = """
CREATE OR REPLACE VIEW b1b2_periods AS
SELECT
    p.*,
    b.start_date, b.peak_date, b.end_date, b.start_status, b.end_status,
    multiIf(b.start_date IS NULL OR b.end_date IS NULL, 'brak dat sezonu',
            p.date < b.start_date, 'przed sezonem',
            p.date < b.peak_date, 'wzrost',
            p.date <= b.end_date, 'dojrzewanie',
            'po sezonie') AS season_phase,
    multiIf(p.status = 'gorzej niż zwykle' AND p.date <= b.peak_date AND b.start_status = 'później niż zwykle',
                'późniejszy start sezonu — rośliny spóźnione, niekoniecznie słabsze',
            p.status = 'gorzej niż zwykle' AND p.date >= b.peak_date AND b.end_status = 'wcześniej niż zwykle',
                'wcześniejszy koniec sezonu — przyspieszone dojrzewanie (np. susza, upał)',
            p.status = 'lepiej niż zwykle' AND p.date <= b.peak_date AND b.start_status = 'wcześniej niż zwykle',
                'wcześniejszy start sezonu (np. ciepła wiosna)',
            p.status = 'lepiej niż zwykle' AND p.date >= b.peak_date AND b.end_status = 'później niż zwykle',
                'dłuższy sezon niż zwykle',
            '') AS season_context
FROM b1_periods AS p
LEFT JOIN (SELECT * FROM b2_phenology WHERE crop_class = 'arable') AS b
    ON b.region_id = p.region_id AND b.year = p.year
"""

SEASON_VIEW = """
CREATE OR REPLACE VIEW b1b2_season AS
SELECT
    s.*,
    b.start_date, b.peak_date, b.end_date, b.length_days,
    b.start_shift, b.peak_shift, b.end_shift, b.start_status, b.peak_status, b.end_status
FROM b1_season AS s
LEFT JOIN (SELECT * FROM b2_phenology WHERE crop_class = 'arable') AS b
    ON b.region_id = s.region_id AND b.year = s.year
"""

CHECK = """
SELECT year, countIf(status = 'gorzej niż zwykle') AS gorzej_terminy,
       countIf(season_context LIKE 'późniejszy start%') AS z_tego_pozny_start,
       countIf(season_context LIKE 'wcześniejszy koniec%') AS z_tego_wczesny_koniec
FROM b1b2_periods WHERE year IN (2003, 2018, 2019, 2020) AND toMonth(date) BETWEEN 3 AND 9
GROUP BY year ORDER BY year
"""


def main() -> None:
    setup_logging()
    from lib.clickhouse import ClickHouse

    ch = ClickHouse()
    for name, sql in (("b1b2_periods", PERIODS_VIEW), ("b1b2_season", SEASON_VIEW)):
        ch.command(sql)
        log.info("widok %s: gotowy (%s wierszy)", name, ch.command(f"SELECT count() FROM {name}"))
    df = ch.query_df(CHECK)
    for _, r in df.iterrows():
        log.info("%d (III–IX): terminy „gorzej niż zwykle” %d, z tego późny start %d, wczesny koniec %d",
                 r["year"], r["gorzej_terminy"], r["z_tego_pozny_start"], r["z_tego_wczesny_koniec"])


if __name__ == "__main__":
    main()
