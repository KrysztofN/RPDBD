"""
Raporter biuletynów B1/B2: bierze zlecenia z Kafki, robi PDF z komentarzem Bielika, odkłada wynik.

Co robi (pętla, działa jako usługa Swarma na maszynach z Bielikiem):
  1. czyta zlecenie z tematu Kafki `bulletin.requests`: {"request_id", "county", "year", "period"};
  2. dane + wykresy + teksty z reguł (B1_B2/bulletin/build_bulletin.py);
  3. Bielik pisze „Streszczenie” i „Ocenę” (B1_B2/reporter/bielik.py, odpowiedź sprawdzana; gdy nie przejdzie
     sprawdzenia — zostają teksty z reguł, biuletyn i tak powstaje);
  4. LaTeX -> PDF, zapis na HDFS: /rbd/bulletins/<rok>/<powiat>_<rok>_p<termin>.pdf;
  5. wpis w ClickHouse `rbd.bulletins` (status, czy użyto AI, czas, ścieżka) i wiadomość w `bulletin.ready`;
  6. dopiero potem potwierdza zlecenie w Kafce (commit) — awaria w trakcie = zlecenie zostanie zrobione ponownie.

Trochę teorii:
  Asynchronicznie: obliczenia (Spark, Airflow) tylko wrzucają zlecenia i nie czekają na model (ok. 1–3 min na
  komentarz na CPU). Kilka raporterów w jednej grupie konsumentów Kafki dzieli się zleceniami automatycznie
  (każda partycja tematu trafia do jednego raportera) — 2 maszyny z Bielikiem = 2 biuletyny naraz.

Uruchomienie: usługa infra/reporter/stack.yml (obraz rbd-reporter). Ręcznie (test):
  python3 -m B1_B2.reporter.reporter --once
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import tempfile
import time
from datetime import datetime
from pathlib import Path

from lib.log import get_logger, setup_logging

log = get_logger(Path(__file__).stem)

BOOTSTRAP = os.environ.get("RBD_KAFKA_BOOTSTRAP", "10.7.11.22:9092,10.7.11.23:9092,10.7.11.24:9092")
TOPIC_REQUESTS, TOPIC_READY = "bulletin.requests", "bulletin.ready"
GROUP = "bulletin-reporters"
HDFS_DIR = "/rbd/bulletins"

DDL = """
CREATE TABLE IF NOT EXISTS bulletins (
    request_id String, region_id LowCardinality(String), county_name String, year UInt16, period UInt8,
    period_label String, status LowCardinality(String), ai_used UInt8, ai_result String, cause LowCardinality(String),
    pdf_path String, reporter_host LowCardinality(String), duration_s Float32, created DateTime
) ENGINE = MergeTree ORDER BY (year, period, region_id, created)"""


def make_one(req: dict) -> dict:
    """Jedno zlecenie -> PDF na HDFS; zwraca rekord wyniku (do ClickHouse i tematu bulletin.ready)."""
    from B1_B2.bulletin import build_bulletin as bb
    from B1_B2.reporter.bielik import ask_bielik, facts_from_context
    from lib.webhdfs import WebHDFS

    t0 = time.time()
    county, year, period = str(req["county"]), int(req["year"]), int(req["period"])
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        ctx = bb.prepare_bulletin(county, year, period, out)
        answer, ai_result = ask_bielik(facts_from_context(ctx))
        ctx.pop("_weather_summary")
        if answer:
            ctx["summary"] = answer["streszczenie"].strip()
            ctx["assessment_text"] = answer["ocena"].strip()
            ctx["summary_note"] = "Streszczenie napisał model językowy Bielik 11B na podstawie liczb z biuletynu."
            ctx["ai_note"] = ("Streszczenie i ocenę przyczyn napisał model Bielik 11B (SpeakLeash) wyłącznie z liczb "
                              "z tego biuletynu; odpowiedź sprawdzono automatycznie (format, liczby, lista przyczyn).")
        bb.render(ctx, out / "bulletin.tex")
        pdf = bb.compile_pdf(out)
        target = f"{HDFS_DIR}/{year}/{county}_{year}_p{period:02d}.pdf"
        hdfs = WebHDFS()
        hdfs.mkdirs(f"{HDFS_DIR}/{year}")
        hdfs.put(pdf, target, replication=1, overwrite=True)
    return dict(request_id=req.get("request_id", ""), region_id=county, county_name=ctx["county_name"], year=year,
                period=period, period_label=ctx["period_label"], status="ok", ai_used=int(answer is not None),
                ai_result=ai_result, cause=(answer or {}).get("przyczyna", "reguły"), pdf_path=target,
                reporter_host=socket.gethostname(), duration_s=round(time.time() - t0, 1),
                created=datetime.now().replace(microsecond=0))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Raporter biuletynów B1/B2 (Kafka -> Bielik -> PDF)")
    p.add_argument("--once", action="store_true", help="obsłuż jedno zlecenie i zakończ (test)")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)
    import pandas as pd
    from confluent_kafka import Consumer, Producer

    from lib.clickhouse import ClickHouse

    ch = ClickHouse()
    ch.command(DDL)
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest", "max.poll.interval.ms": 1_800_000})
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC_REQUESTS])
    log.info("raporter %s: czekam na zlecenia w %s (grupa %s)", socket.gethostname(), TOPIC_REQUESTS, GROUP)
    try:
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("Kafka: %s", msg.error())
                continue
            req = json.loads(msg.value())
            log.info("zlecenie %s: powiat %s, rok %s, termin %s", req.get("request_id"), req.get("county"),
                     req.get("year"), req.get("period"))
            try:
                result = make_one(req)
            except Exception as e:                       # zły powiat/brak danych: zapisujemy błąd, nie blokujemy kolejki
                log.error("zlecenie %s nieudane: %s: %s", req.get("request_id"), type(e).__name__, str(e)[:300])
                result = dict(request_id=req.get("request_id", ""), region_id=str(req.get("county")), county_name="",
                              year=int(req.get("year", 0)), period=int(req.get("period", 0)), period_label="",
                              status="błąd", ai_used=0, ai_result=f"{type(e).__name__}: {str(e)[:200]}", cause="",
                              pdf_path="", reporter_host=socket.gethostname(), duration_s=0.0,
                              created=datetime.now().replace(microsecond=0))
            ch.insert_df("bulletins", pd.DataFrame([result]))
            producer.produce(TOPIC_READY, key=result["region_id"],
                             value=json.dumps(result, default=str, ensure_ascii=False).encode())
            producer.flush(10)
            consumer.commit(msg, asynchronous=False)    # potwierdzenie dopiero po zapisaniu wyniku
            log.info("zlecenie %s: %s, AI: %s, %.0f s -> %s", result["request_id"], result["status"],
                     result["ai_result"], result["duration_s"], result["pdf_path"])
            if args.once:
                break
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
