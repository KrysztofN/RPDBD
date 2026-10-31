"""
Bot Telegram biuletynów B1/B2: zapisy na biuletyny i wysyłka PDF do subskrybentów.

Co robi (pętla, usługa Swarma na masterze):
  1. odbiera polecenia od ludzi (Telegram getUpdates):
       /start            — opis i lista poleceń
       /powiat 3011      — subskrypcja powiatu (kod TERYT albo nazwa, np. /powiat poznański)
       /woj 30           — subskrypcja całego województwa (kod albo nazwa, np. /woj wielkopolskie)
       /wszystko         — wszystkie powiaty
       /moje             — moje subskrypcje
       /ostatni          — przyślij ostatni gotowy biuletyn z moich subskrypcji (test)
       /stop             — wypisz mnie ze wszystkiego
     subskrypcje zapisuje w ClickHouse `rbd.tg_subscribers` (zakres: all | voiv:XX | county:XXXX);
  2. czyta temat Kafki `bulletin.ready` (gotowy PDF od raportera), wybiera subskrybentów, których zakres obejmuje
     powiat, pobiera PDF z HDFS i wysyła go każdemu (sendDocument) z krótkim opisem;
  3. każdą wysyłkę zapisuje w `rbd.tg_log`; ten sam biuletyn nie idzie drugi raz do tej samej osoby;
  4. dopiero potem potwierdza wiadomość w Kafce (commit).

Trochę teorii:
  Bot może pisać tylko do osób, które same go uruchomiły (/start) — zapis do bota = zgoda na wiadomości, nie ma
  filtrów spamu jak w poczcie. Bot pyta Telegram o nowe polecenia („long polling”, getUpdates) — nie potrzebuje
  publicznego adresu ani otwartego portu na klastrze. Osobna grupa konsumentów Kafki (`bulletin-telegram`) czyta
  gotowe biuletyny niezależnie od innych odbiorców; awaria Telegrama nie gubi biuletynów (zostają w Kafce).

Konfiguracja: TELEGRAM_BOT_TOKEN w ~/rbd-build/telegram/.env na masterze (NIE w repo, NIE w logach).
  TELEGRAM_PROXY (opcjonalnie), np. socks5h://127.0.0.1:1080 — sieć klastra blokuje api.telegram.org, więc ruch
  bota do Telegrama idzie tunelem SSH przez laptop: na laptopie `ssh -N -R 127.0.0.1:1080 rbd`.

Użycie:
  python3 -m B1_B2.reporter.telegram_bot            # usługa
  python3 -m B1_B2.reporter.telegram_bot check      # sprawdź token (nazwa bota) i liczbę subskrybentów
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import date, datetime
from pathlib import Path

import requests

from lib.log import get_logger, setup_logging

log = get_logger(Path(__file__).stem)

BOOTSTRAP = os.environ.get("RBD_KAFKA_BOOTSTRAP", "10.7.11.22:9092,10.7.11.23:9092,10.7.11.24:9092")
TOPIC_READY, GROUP = "bulletin.ready", "bulletin-telegram"
API = "https://api.telegram.org/bot{token}/{method}"
PAUSE_S = 0.2                                  # Telegram: do ~30 wiadomości/s, my wysyłamy dużo wolniej

DDL = [
    """CREATE TABLE IF NOT EXISTS tg_subscribers (
        chat_id Int64, name String, scope String, active UInt8, updated DateTime
    ) ENGINE = ReplacingMergeTree(updated) ORDER BY (chat_id, scope)""",
    """CREATE TABLE IF NOT EXISTS tg_log (
        request_id String, region_id LowCardinality(String), chat_id Int64, status LowCardinality(String),
        error String, sent DateTime
    ) ENGINE = MergeTree ORDER BY (sent, request_id)""",
]

HELP = (
    "Biuletyn stanu upraw RBD — co 16 dni (IV–IX) PDF dla powiatu: stan upraw względem normy z 15 lat "
    "(NDVI, MODIS), przebieg sezonu, pogoda (ERA5-Land) i komentarz modelu Bielik.\n\n"
    "Kody: /kody — województwa, /kody 30 albo /kody wielkopolskie — powiaty województwa.\n"
    "Powiat podajesz kodem (3011) albo nazwą (poznański).\n\n"
    "Wykresy (rok opcjonalny, domyślnie ostatni z danymi):\n"
    "/mapa 2018 15.07 — mapa Polski: stan upraw w powiatach w danym dniu\n"
    "/wykres 3011 2018 — krzywa NDVI powiatu na tle normy\n"
    "/woda 3011 2018 — opad, parowanie i bilans wody (susza)\n"
    "/porownaj 3011 1206 2018 — dwa powiaty: NDVI i bilans wody\n\n"
    "Biuletyny PDF:\n"
    "/powiat 3011 — subskrypcja powiatu, /woj 30 — całego województwa, /wszystko — całej Polski\n"
    "/moje — moje subskrypcje, /ostatni — przyślij ostatni gotowy biuletyn, /stop — wypisz mnie")

COMMANDS = [("kody", "lista kodów województw i powiatów"), ("mapa", "mapa Polski: stan upraw, np. /mapa 2018 15.07"),
            ("wykres", "krzywa NDVI powiatu, np. /wykres 3011 2018"), ("woda", "bilans wody powiatu, np. /woda 3011 2018"),
            ("porownaj", "dwa powiaty, np. /porownaj 3011 1206 2018"), ("powiat", "subskrypcja biuletynów powiatu"),
            ("woj", "subskrypcja biuletynów województwa"), ("wszystko", "subskrypcja całej Polski"),
            ("moje", "moje subskrypcje"), ("ostatni", "ostatni gotowy biuletyn"), ("stop", "wypisz mnie"),
            ("start", "opis i pomoc")]
MAP_DEFAULT_PERIOD = 11                       # ok. 25 VI — środek sezonu, gdy nie podano daty
REF_BOUNDARIES = "/rbd/ref/boundaries/counties_raw.gpkg"


def scope_matches(scope: str, region_id: str) -> bool:
    """Czy zakres subskrypcji obejmuje powiat (kod TERYT, 4 cyfry)."""
    if scope == "all":
        return True
    kind, _, code = scope.partition(":")
    return (kind == "voiv" and region_id[:2] == code) or (kind == "county" and region_id == code)


def sql_str(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


class Telegram:
    """Minimalny klient Bot API (requests). Token nigdy nie trafia do logów."""

    def __init__(self, token: str, proxy: str = ""):
        self.token = token
        self.session = requests.Session()
        if proxy:                                   # np. socks5h://127.0.0.1:1080 (tunel SSH przez laptop)
            self.session.proxies = {"https": proxy}
            log.info("Telegram przez proxy %s", proxy)

    def call(self, method: str, http_timeout: int = 30, files=None, **params) -> dict | list:
        try:
            r = self.session.post(API.format(token=self.token, method=method), data=params, files=files,
                                  timeout=http_timeout)
            body = r.json()
        except (requests.RequestException, ValueError) as e:     # komunikat zawiera adres z tokenem — wycinamy
            raise RuntimeError(f"Telegram {method}: {type(e).__name__}: "
                               f"{str(e).replace(self.token, '<token>')[:200]}") from None
        if not body.get("ok"):
            raise RuntimeError(f"Telegram {method}: {body.get('error_code')} {body.get('description')}")
        return body["result"]

    def send_text(self, chat_id: int, text: str) -> None:
        self.call("sendMessage", chat_id=chat_id, text=text)

    def send_photo(self, chat_id: int, png: bytes, caption: str) -> None:
        self.call("sendPhoto", http_timeout=60, chat_id=chat_id, caption=caption[:1024],
                  files={"photo": ("wykres.png", png, "image/png")})

    def send_pdf(self, chat_id: int, pdf: bytes, filename: str, caption: str) -> None:
        self.call("sendDocument", http_timeout=120, chat_id=chat_id, caption=caption[:1024],
                  files={"document": (filename, pdf, "application/pdf")})


class Bot:
    def __init__(self, ch, tg: Telegram):
        self.ch, self.tg = ch, tg
        self.counties = ch.query_df("SELECT region_id, county_name, voiv_id, voiv_name FROM rbd.counties "
                                    "WHERE is_city = 0 ORDER BY region_id")
        self.years = [int(y) for y in ch.query_df("SELECT DISTINCT year FROM rbd.b1_periods ORDER BY year")["year"]]
        self._geo = None                                         # granice powiatów do mapy (wczytane przy 1. /mapa)

    # ---------- wykresy ----------

    def geo(self):
        if self._geo is None:
            import io

            import geopandas as gpd

            from lib.webhdfs import WebHDFS
            import warnings
            with warnings.catch_warnings():                      # pyogrio: plik z pamięci bez rozszerzenia .gpkg
                warnings.simplefilter("ignore", RuntimeWarning)
                g = gpd.read_file(io.BytesIO(WebHDFS().read_bytes(REF_BOUNDARIES)))
            g = g.assign(region_id=g["JPT_KOD_JE"].astype(str))[["region_id", "geometry"]]
            self._geo = g.set_geometry(g.geometry.simplify(800))  # 800 m: mapa całej Polski, szybkie rysowanie
        return self._geo

    def parse_year(self, tokens: list[str], keep: int, default: int | None = None) -> tuple[int | None, str]:
        """Ostatni token = rok, jeśli to 4 cyfry i zostaje `keep` innych tokenów (kod powiatu też ma 4 cyfry).
        Usuwa rok z `tokens`; zwraca (rok, błąd)."""
        year = default if default is not None else self.years[-1]
        if len(tokens) > keep and re.fullmatch(r"\d{4}", tokens[-1]):
            year = int(tokens.pop())
        if year not in self.years:
            return None, f"Brak danych dla roku {year}. Dostępne: {self.years[0]}–{self.years[-1]}."
        return year, ""

    def county_args(self, arg: str, n: int, default_year: int | None = None):
        """'3011 1206 2018' -> ([(kod, nazwa), ...], rok, błąd); najpierw n powiatów, rok na końcu."""
        tokens = arg.split()
        if len(tokens) < n:
            return [], None, "Podaj " + ("powiat" if n == 1 else f"{n} powiaty") + " (kod albo nazwę). Kody: /kody"
        year, err = self.parse_year(tokens, n, default_year)
        if err:
            return [], None, err
        names = [" ".join(tokens)] if n == 1 else tokens[:n]          # 1 powiat: nazwa może mieć spację
        places = []
        for t in names:
            scope, err = self.find_scope("county", t)
            if scope is None:
                return [], None, f"„{t}”: {err}"
            code = scope.split(":")[1]
            places.append((code, self.counties.loc[self.counties["region_id"] == code, "county_name"].iloc[0]))
        return places, year, ""

    def cmd_map(self, chat_id: int, arg: str) -> None:
        from B1_B2.reporter import bot_charts as bc
        tokens, period = arg.split(), None
        for t in list(tokens):                                   # data: 15.07 albo 15.07.2018
            m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?", t)
            if m:
                tokens.remove(t)
                if m.group(3):
                    tokens.append(m.group(3))
                day, month = int(m.group(1)), int(m.group(2))
                period = (day, month)
        year, err = self.parse_year(tokens, 0)
        if err:
            self.tg.send_text(chat_id, err)
            return
        try:
            p = bc.period_of(date(year, period[1], period[0])) if period else MAP_DEFAULT_PERIOD
        except ValueError:
            self.tg.send_text(chat_id, "Zła data. Przykład: /mapa 2018 15.07")
            return
        png, text = bc.map_png(self.ch, self.geo(), year, p)
        self.tg.send_photo(chat_id, png, text + "\nInny dzień: /mapa 2018 15.07")

    def cmd_chart(self, chat_id: int, kind: str, arg: str, n: int) -> None:
        from B1_B2.reporter import bot_charts as bc
        wy = bc.weather_years(self.ch)
        default = wy[-1] if kind == "water" and wy else None   # /woda bez roku: ostatni rok z pogodą
        places, year, err = self.county_args(arg, n, default)
        if err:
            self.tg.send_text(chat_id, err)
            return
        charts = []
        if kind in ("ndvi", "both"):
            charts.append(bc.curve_png(self.ch, places, year))
        if kind in ("water", "both"):
            res = bc.water_png(self.ch, places, year) if year in wy else None
            if res is None:
                years = ", ".join(map(str, wy)) or "brak"
                charts.append((None, f"Bilans wody: brak danych pogodowych dla {year} (dane pogodowe dla lat: {years})."))
            else:
                charts.append(res)
        for res in charts:
            if res is None:
                self.tg.send_text(chat_id, f"Brak danych NDVI dla {year}.")
            elif res[0] is None:
                self.tg.send_text(chat_id, res[1])
            else:
                self.tg.send_photo(chat_id, *res)

    def cmd_codes(self, chat_id: int, arg: str) -> None:
        c = self.counties
        if not arg.strip():
            v = c.drop_duplicates("voiv_id")
            text = "Województwa (kod — nazwa):\n" + "\n".join(f"{i} — {n}" for i, n in zip(v["voiv_id"], v["voiv_name"]))
            self.tg.send_text(chat_id, text + "\n\nPowiaty województwa: /kody 30 albo /kody wielkopolskie")
            return
        scope, err = self.find_scope("voiv", arg)
        if scope is None:
            self.tg.send_text(chat_id, err + " Lista: /kody")
            return
        code = scope.split(":")[1]
        rows = c[c["voiv_id"] == code]
        text = (f"Powiaty — {rows['voiv_name'].iloc[0]} (kod — nazwa):\n"
                + "\n".join(f"{i} — {n}" for i, n in zip(rows["region_id"], rows["county_name"]))
                + "\n\nMiasta na prawach powiatu pomijamy (prawie bez upraw).")
        self.tg.send_text(chat_id, text)

    # ---------- subskrypcje ----------

    def subscriptions(self, chat_id: int | None = None):
        where = f"AND chat_id = {int(chat_id)}" if chat_id is not None else ""
        return self.ch.query_df(f"SELECT chat_id, name, scope FROM rbd.tg_subscribers FINAL WHERE active = 1 {where}")

    def set_scope(self, chat_id: int, name: str, scope: str, active: int) -> None:
        import pandas as pd
        self.ch.insert_df("tg_subscribers", pd.DataFrame([dict(chat_id=chat_id, name=name, scope=scope, active=active,
                                                               updated=datetime.now().replace(microsecond=0))]))

    def scope_label(self, scope: str) -> str:
        if scope == "all":
            return "wszystkie powiaty"
        kind, _, code = scope.partition(":")
        c = self.counties
        if kind == "voiv":
            names = c.loc[c["voiv_id"] == code, "voiv_name"]
            return f"województwo {names.iloc[0]} ({code})" if len(names) else scope
        names = c.loc[c["region_id"] == code, "county_name"]
        return f"powiat {names.iloc[0]} ({code})" if len(names) else scope

    def find_scope(self, kind: str, arg: str) -> tuple[str | None, str]:
        """Kod albo nazwa -> zakres; przy kilku pasujących nazwach zwraca listę do wyboru."""
        arg = arg.strip().lower()
        c = self.counties
        if kind == "county":
            id_col, name_col = "region_id", "county_name"
        else:
            id_col, name_col = "voiv_id", "voiv_name"
            c = c.drop_duplicates("voiv_id")
        hit = c[c[id_col] == arg]
        if not len(hit):
            hit = c[c[name_col].str.lower() == arg]
        if not len(hit) and arg:
            hit = c[c[name_col].str.lower().str.contains(arg, regex=False)]
        if len(hit) == 1:
            return f"{kind if kind == 'county' else 'voiv'}:{hit[id_col].iloc[0]}", ""
        if len(hit) > 1:
            opts = ", ".join(f"{n} ({i})" for i, n in zip(hit[id_col].head(10), hit[name_col].head(10)))
            return None, f"Pasuje kilka: {opts}. Podaj kod."
        return None, "Nie znam takiego. Podaj kod TERYT (np. 3011) albo nazwę."

    # ---------- polecenia ----------

    def handle(self, msg: dict) -> None:
        chat_id = msg["chat"]["id"]
        name = " ".join(filter(None, [msg["chat"].get("first_name"), msg["chat"].get("last_name")])) \
            or msg["chat"].get("title", "")
        cmd, _, arg = (msg.get("text") or "").strip().partition(" ")
        cmd = cmd.split("@")[0].lower()
        log.info("polecenie %s od czatu %s", cmd, chat_id)
        if cmd in ("/start", "/pomoc", "/help"):
            self.tg.send_text(chat_id, HELP)
        elif cmd == "/kody":
            self.cmd_codes(chat_id, arg)
        elif cmd == "/mapa":
            self.cmd_map(chat_id, arg)
        elif cmd == "/wykres":
            self.cmd_chart(chat_id, "ndvi", arg, 1)
        elif cmd == "/woda":
            self.cmd_chart(chat_id, "water", arg, 1)
        elif cmd == "/porownaj":
            self.cmd_chart(chat_id, "both", arg, 2)
        elif cmd in ("/powiat", "/woj", "/wszystko"):
            if cmd == "/wszystko":
                scope, err = "all", ""
            else:
                scope, err = self.find_scope("county" if cmd == "/powiat" else "voiv", arg)
            if scope is None:
                self.tg.send_text(chat_id, err)
                return
            self.set_scope(chat_id, name, scope, 1)
            self.tg.send_text(chat_id, f"Zapisano: {self.scope_label(scope)}. Nowe biuletyny przyjdą tutaj. "
                                       "/ostatni — przyślij ostatni gotowy.")
        elif cmd == "/moje":
            subs = self.subscriptions(chat_id)
            text = "\n".join(self.scope_label(s) for s in subs["scope"]) if len(subs) else "Brak subskrypcji."
            self.tg.send_text(chat_id, text)
        elif cmd == "/stop":
            subs = self.subscriptions(chat_id)
            for s in subs["scope"]:
                self.set_scope(chat_id, name, s, 0)
            self.tg.send_text(chat_id, "Wypisano. Nie dostaniesz więcej biuletynów. /start — zapis ponownie.")
        elif cmd == "/ostatni":
            self.send_latest(chat_id)
        else:
            self.tg.send_text(chat_id, "Nie znam tego polecenia.\n\n" + HELP)

    def send_latest(self, chat_id: int) -> None:
        scopes = list(self.subscriptions(chat_id)["scope"])
        if not scopes:
            self.tg.send_text(chat_id, "Najpierw zapisz się: /powiat, /woj albo /wszystko.")
            return
        df = self.ch.query_df("SELECT * FROM rbd.bulletins WHERE status = 'ok' ORDER BY created DESC LIMIT 500")
        for row in df.to_dict("records"):
            if any(scope_matches(s, row["region_id"]) for s in scopes):
                self.send_bulletin(row, only_chat=chat_id)
                return
        self.tg.send_text(chat_id, "Nie ma jeszcze gotowego biuletynu dla Twoich subskrypcji.")

    # ---------- biuletyny ----------

    def send_bulletin(self, b: dict, only_chat: int | None = None) -> int:
        """Wysyła biuletyn pasującym subskrybentom (albo jednemu czatowi); zwraca liczbę wysłanych."""
        import pandas as pd

        from lib.webhdfs import WebHDFS

        rid, req = str(b["region_id"]), str(b["request_id"])
        if only_chat is not None:
            chats = [only_chat]
        else:
            subs = self.subscriptions()
            chats = sorted({int(r.chat_id) for r in subs.itertuples() if scope_matches(r.scope, rid)})
            done = set(self.ch.query_df(f"SELECT DISTINCT chat_id FROM rbd.tg_log WHERE request_id = {sql_str(req)} "
                                        "AND status = 'ok'")["chat_id"].astype(int))
            chats = [c for c in chats if c not in done]
        if not chats:
            log.info("biuletyn %s (%s): brak nowych odbiorców", req, b["county_name"])
            return 0
        pdf = WebHDFS().read_bytes(b["pdf_path"])
        cause = b.get("cause") or ""
        caption = (f"Biuletyn stanu upraw: powiat {b['county_name']}, {b['period_label']} {b['year']}."
                   + (f"\nNajbardziej prawdopodobna przyczyna odchyleń (Bielik): {cause}." if cause not in ("", "reguły") else ""))
        fname = f"biuletyn_{rid}_{b['year']}_p{int(b['period']):02d}.pdf"
        rows, sent = [], 0
        for chat in chats:
            try:
                self.tg.send_pdf(chat, pdf, fname, caption)
                status, err = "ok", ""
                sent += 1
            except (RuntimeError, requests.RequestException) as e:
                status, err = "błąd", str(e)[:200]
                log.warning("czat %s: wysyłka nieudana: %s", chat, err)
            rows.append(dict(request_id=req, region_id=rid, chat_id=chat, status=status, error=err,
                             sent=datetime.now().replace(microsecond=0)))
            time.sleep(PAUSE_S)
        self.ch.insert_df("tg_log", pd.DataFrame(rows))
        log.info("biuletyn %s (%s, %s): wysłano %d/%d", req, b["county_name"], b["period_label"], sent, len(chats))
        return sent


def run_service(bot: Bot) -> None:
    from confluent_kafka import Consumer

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP, "enable.auto.commit": False,
                         "auto.offset.reset": "latest", "max.poll.interval.ms": 1_800_000})
    consumer.subscribe([TOPIC_READY])
    offset = None
    me = bot.tg.call("getMe")
    bot.tg.call("setMyCommands", commands=json.dumps([{"command": c, "description": d} for c, d in COMMANDS],
                                                     ensure_ascii=False))
    log.info("bot @%s: czekam na polecenia i na gotowe biuletyny (%s, grupa %s)", me["username"], TOPIC_READY, GROUP)
    try:
        while True:
            # 1) polecenia od ludzi (long polling do 5 s)
            try:
                params = {"timeout": 5, "allowed_updates": json.dumps(["message"])}
                if offset is not None:
                    params["offset"] = offset
                for upd in bot.tg.call("getUpdates", http_timeout=15, **params):
                    offset = upd["update_id"] + 1
                    if "message" in upd:
                        try:
                            bot.handle(upd["message"])
                        except Exception as e:           # zła wiadomość nie może zatrzymać bota
                            log.warning("polecenie nieobsłużone: %s: %s", type(e).__name__, str(e)[:200])
            except (RuntimeError, requests.RequestException) as e:
                log.warning("Telegram niedostępny: %s", str(e)[:200])
                time.sleep(10)
            # 2) gotowe biuletyny z Kafki
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("Kafka: %s", msg.error())
                continue
            b = json.loads(msg.value())
            if b.get("status") == "ok":
                try:
                    bot.send_bulletin(b)
                except (OSError, requests.RequestException) as e:   # HDFS/Telegram niedostępne: ponów później
                    log.error("wysyłka nieudana (%s), ponowię: %s", type(e).__name__, str(e)[:200])
                    consumer.seek(msg)
                    time.sleep(60)
                    continue
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bot Telegram biuletynów B1/B2")
    p.add_argument("cmd", nargs="?", choices=["run", "check"], default="run")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)
    from lib.clickhouse import ClickHouse

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise SystemExit("brak TELEGRAM_BOT_TOKEN (plik ~/rbd-build/telegram/.env)")
    ch = ClickHouse()
    for ddl in DDL:
        ch.command(ddl)
    bot = Bot(ch, Telegram(token, os.environ.get("TELEGRAM_PROXY", "")))
    if args.cmd == "check":
        me = bot.tg.call("getMe")
        subs = bot.subscriptions()
        log.info("token OK: bot @%s (%s); subskrypcji: %d, osób: %d", me["username"], me["first_name"], len(subs),
                 subs["chat_id"].nunique() if len(subs) else 0)
    else:
        run_service(bot)


if __name__ == "__main__":
    main()
