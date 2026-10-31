"""
Zlecenia biuletynów B1/B2 do Kafki (temat bulletin.requests) — wysyła je Airflow po przeliczeniu B1 i B2, albo ręcznie.

Co robi: dla podanego roku i terminu wysyła po jednym zleceniu na powiat: wybrane powiaty (--counties) albo
wszystkie powiaty ziemskie województwa (--voiv) albo cała Polska (--all). Kluczem wiadomości jest kod powiatu,
więc zlecenia rozkładają się po partycjach tematu, a raporterzy w grupie dzielą się nimi.

Uruchomienie:
  python3 -m B1_B2.reporter.request_bulletins --year 2018 --period 10 --counties 3011 1206
  python3 -m B1_B2.reporter.request_bulletins --year 2018 --period 10 --voiv 30
"""
from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

from lib.log import get_logger, setup_logging

log = get_logger(Path(__file__).stem)


def main() -> None:
    p = argparse.ArgumentParser(description="Zlecenia biuletynów B1/B2 -> Kafka")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--period", type=int, required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--counties", nargs="+", help="kody TERYT powiatów")
    g.add_argument("--voiv", help="kod województwa (2 cyfry) — wszystkie powiaty ziemskie")
    g.add_argument("--all", action="store_true", help="wszystkie powiaty ziemskie")
    args = p.parse_args()
    setup_logging()
    from confluent_kafka import Producer

    from B1_B2.reporter.reporter import BOOTSTRAP, TOPIC_REQUESTS
    from lib.clickhouse import ClickHouse

    if args.counties:
        counties = args.counties
    else:
        where = "is_city = 0" + (f" AND voiv_id = '{args.voiv}'" if args.voiv else "")
        counties = ClickHouse().query_df(f"SELECT region_id FROM counties WHERE {where} ORDER BY region_id")["region_id"].tolist()
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    batch = uuid.uuid4().hex[:8]
    for c in counties:
        req = {"request_id": f"{batch}-{c}-{args.year}-{args.period:02d}", "county": c, "year": args.year,
               "period": args.period}
        producer.produce(TOPIC_REQUESTS, key=c, value=json.dumps(req).encode())
    producer.flush(30)
    log.info("wysłano %d zleceń (partia %s, rok %d, termin %d) -> %s", len(counties), batch, args.year, args.period,
             TOPIC_REQUESTS)


if __name__ == "__main__":
    main()
