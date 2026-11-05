"""
Bot Telegram biuletynów B1/B2: zapisy na biuletyny i wysyłka PDF do subskrybentów.

Co robi (pętla, usługa Swarma na masterze):
  1. odbiera polecenia od ludzi (Telegram getUpdates):
       /start            — opis i lista poleceń
       /mapa, /ndvi, /kbw, /uprawy, /wykres, /woda, /porownaj, /kody — mapy i wykresy na żądanie (bot_charts.py)
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
from html import escape
from pathlib import Path

import requests

from lib.log import get_logger, setup_logging

log = get_logger(Path(__file__).stem)

BOOTSTRAP = os.environ.get("RBD_KAFKA_BOOTSTRAP", "10.7.11.22:9092,10.7.11.23:9092,10.7.11.24:9092")
TOPIC_READY, GROUP = "bulletin.ready", "bulletin-telegram"
S2_TEST_TOPIC = "s1s2.incidents.test"
INCIDENTS_TOPIC = os.environ.get("S1S2_INCIDENTS_TOPIC", "s1s2.incidents.v1")
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

# Format poleceń: kursywa = co wpisać, [nawias] = można pominąć (nawiasów się nie wpisuje)
USAGE = {
    "/mapa": ("/mapa <i>[rok] [dzień]</i>", "/mapa 2018 15.07"),
    "/ndvi": ("/ndvi <i>[powiat] [rok] [dzień]</i>", "/ndvi 3011 2018 15.07"),
    "/kbw": ("/kbw <i>[powiat] [rok] [dzień]</i>", "/kbw 3011 2018 15.07"),
    "/uprawy": ("/uprawy <i>[powiat] [rok]</i>", "/uprawy 3011 2018"),
    "/wykres": ("/wykres <i>powiat [rok]</i>", "/wykres 3011 2018"),
    "/woda": ("/woda <i>powiat [rok]</i>", "/woda 3011 2018"),
    "/porownaj": ("/porownaj <i>powiat powiat [rok]</i>", "/porownaj 3011 1206 2018"),
    "/kody": ("/kody <i>[województwo]</i>", "/kody 30"),
    "/powiat": ("/powiat <i>powiat</i>", "/powiat 3011"),
    "/woj": ("/woj <i>województwo</i>", "/woj 30"),
}


def usage(cmd: str) -> str:
    form, example = USAGE[cmd]
    return f"{form}\n    np. <code>{example}</code>"


HELP = (                                       # HTML Telegrama: <b> pogrubienie, <i> kursywa, <code> do skopiowania
    "<b>Biuletyn stanu upraw RBD</b>\n"
    "Biuletyny PDF o stanie upraw w powiatach oraz mapy i wykresy na żądanie.\n\n"
    "<b>Jak wpisywać polecenia</b>\n"
    "Najpierw polecenie, potem spacja i dane, zawsze w tej kolejności:\n"
    "<code>/polecenie powiat rok dzień</code>\n\n"
    "• <b>powiat</b> — kod TERYT (4 cyfry), np. <code>3011</code>, albo nazwa, np. <code>kościański</code>\n"
    "• <b>województwo</b> — kod (2 cyfry), np. <code>30</code>, albo nazwa, np. <code>wielkopolskie</code>\n"
    "• <b>rok</b> — 4 cyfry, np. <code>2018</code>\n"
    "• <b>dzień</b> — dzień.miesiąc, np. <code>15.07</code>\n"
    "• dane w <i>[nawiasie]</i> można pominąć (nawiasów nie wpisujemy); bez powiatu = cała Polska, "
    "bez roku = ostatni rok, bez dnia = koniec czerwca\n"
    "• kody powiatów i województw: /kody\n\n"
    "<b>Mapy</b> — cała Polska albo jeden powiat\n"
    + "\n".join(f"• {usage(c)}\n    {d}" for c, d in (
        ("/mapa", "ocena stanu upraw w każdym powiecie"),
        ("/ndvi", "zieloność roślin z satelity"),
        ("/kbw", "bilans wody z 60 dni (susza)"),
        ("/uprawy", "jakie uprawy gdzie rosną"))) + "\n\n"
    "<b>Wykresy powiatu</b>\n"
    + "\n".join(f"• {usage(c)}\n    {d}" for c, d in (
        ("/wykres", "zieloność pól przez cały rok na tle typowych lat"),
        ("/woda", "opad, parowanie i bilans wody przez cały rok"),
        ("/porownaj", "dwa powiaty na jednym wykresie"))) + "\n\n"
    "<b>Kody</b>\n"
    "• /kody — kody województw\n"
    f"• {usage('/kody')}\n    kody powiatów województwa\n\n"
    "<b>Biuletyny PDF</b> — przychodzą same co 16 dni (IV–IX)\n"
    f"• {usage('/powiat')}\n    zapis na biuletyny powiatu\n"
    f"• {usage('/woj')}\n    zapis na biuletyny wszystkich powiatów województwa\n"
    "• /wszystko — zapis na biuletyny całej Polski\n"
    "• /moje — moje zapisy\n"
    "• /ostatni — przyślij ostatni gotowy biuletyn\n"
    "• /stop — wypisz mnie ze wszystkiego")

COMMANDS = [("kody", "lista kodów województw i powiatów"), ("mapa", "mapa Polski: stan upraw, np. /mapa 2018 15.07"),
            ("ndvi", "mapa NDVI 250 m: Polska lub powiat, np. /ndvi 3011 2018 15.07"),
            ("kbw", "mapa bilansu wody (susza): Polska lub powiat, np. /kbw 2018 15.07"),
            ("uprawy", "mapa upraw (2018, 2022): Polska lub powiat, np. /uprawy 3011 2018"),
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


def years_text(years: list[int]) -> str:
    """[2010, 2011, 2012] -> '2010–2012'; z dziurami -> lista po przecinku."""
    return f"{years[0]}–{years[-1]}" if years[-1] - years[0] + 1 == len(years) else ", ".join(map(str, years))


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
        self.call("sendMessage", chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)

    def send_photo(self, chat_id: int, png: bytes, caption: str) -> None:
        self.call("sendPhoto", http_timeout=60, chat_id=chat_id, caption=caption[:1024], parse_mode="HTML",
                  files={"photo": ("wykres.png", png, "image/png")})

    def send_pdf(self, chat_id: int, pdf: bytes, filename: str, caption: str) -> None:
        self.call("sendDocument", http_timeout=120, chat_id=chat_id, caption=caption[:1024], parse_mode="HTML",
                  files={"document": (filename, pdf, "application/pdf")})


class Bot:
    def __init__(self, ch, tg: Telegram):
        self.ch, self.tg = ch, tg
        self.counties = ch.query_df("SELECT region_id, county_name, voiv_id, voiv_name FROM rbd.counties "
                                    "WHERE is_city = 0 ORDER BY region_id")
        self.years = [int(y) for y in ch.query_df("SELECT DISTINCT year FROM rbd.b1_periods ORDER BY year")["year"]]
        self._geo = None                                         # granice powiatów do mapy (wczytane przy 1. /mapa)
        self._geo_cache = {}                                     # granice województw (map Polski)
        self.cmd = ""                                            # bieżące polecenie (podpowiedź formatu)

    def warn(self, chat_id: int, text: str) -> None:
        """Komunikat o błędzie / braku danych (escapowany — może zawierać tekst od użytkownika) + format polecenia."""
        hint = f"\n\n<b>Format:</b>\n{usage(self.cmd)}\n\nKody powiatów: /kody · pomoc: /start" \
            if self.cmd in USAGE else ""
        self.tg.send_text(chat_id, f"<b>Uwaga:</b> {escape(text)}{hint}")

    # ---------- wykresy ----------

    def geo(self, full: bool = False):
        """Granice powiatów (EPSG:2180): uproszczone do 800 m (cała Polska, szybko) albo pełne (zbliżenie na powiat)."""
        if self._geo is None:
            import io

            import geopandas as gpd

            from lib.webhdfs import WebHDFS
            import warnings
            with warnings.catch_warnings():                      # pyogrio: plik z pamięci bez rozszerzenia .gpkg
                warnings.simplefilter("ignore", RuntimeWarning)
                g = gpd.read_file(io.BytesIO(WebHDFS().read_bytes(REF_BOUNDARIES)))
            g = g.assign(region_id=g["JPT_KOD_JE"].astype(str))[["region_id", "geometry"]]
            self._geo = {True: g, False: g.set_geometry(g.geometry.simplify(800))}
        return self._geo[full]

    def voiv(self):
        """Granice województw do map Polski (liczone raz, z pełnych granic)."""
        from B1_B2.reporter import bot_charts as bc
        if "voiv" not in self._geo_cache:
            self._geo_cache["voiv"] = bc.voivodeships(self.geo(full=True))
        return self._geo_cache["voiv"]

    def map_args(self, arg: str, years: list[int]):
        """'[powiat] [rok] [dd.mm]' -> (powiat (kod, nazwa) albo None, rok, termin, błąd).
        Sam 4-cyfrowy numer to rok (np. /ndvi 2018); powiat z kodem 20xx podaj razem z rokiem: /ndvi 2007 2018."""
        from B1_B2.reporter import bot_charts as bc
        if not years:
            return None, None, None, "Brak danych do tej mapy."
        tokens, day_month = arg.split(), None
        for t in list(tokens):                                   # data: 15.07 albo 15.07.2018
            m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?", t)
            if m:
                tokens.remove(t)
                if m.group(3):
                    tokens.append(m.group(3))
                day_month = (int(m.group(1)), int(m.group(2)))
        year = years[-1]
        if tokens and re.fullmatch(r"\d{4}", tokens[-1]) and (len(tokens) > 1 or 1990 <= int(tokens[-1]) <= 2100):
            year = int(tokens.pop())
        if year not in years:
            return None, None, None, f"Brak danych dla roku {year}. Dostępne: {years_text(years)}."
        place = None
        if tokens:
            scope, err = self.find_scope("county", " ".join(tokens))
            if scope is None:
                return None, None, None, f"„{' '.join(tokens)}” — {err[0].lower()}{err[1:]}"
            code = scope.split(":")[1]
            place = (code, self.counties.loc[self.counties["region_id"] == code, "county_name"].iloc[0])
        try:
            period = bc.period_of(date(year, day_month[1], day_month[0])) if day_month else MAP_DEFAULT_PERIOD
        except ValueError:
            return None, None, None, "Zła data — wpisz dzień.miesiąc, np. 15.07."
        return place, year, period, ""

    def cmd_raster_map(self, chat_id: int, kind: str, arg: str) -> None:
        """/ndvi, /kbw, /uprawy (kind = nazwa polecenia): mapa Polski albo powiatu dla roku (i terminu)."""
        from B1_B2.reporter import bot_charts as bc
        years = bc.kbw_years(self.ch) if kind == "kbw" else self.years
        place, year, period, err = self.map_args(arg, years)
        if err:
            self.warn(chat_id, err)
            return
        draw = {"ndvi": bc.ndvi_map_png, "kbw": bc.kbw_map_png, "uprawy": bc.crop_map_png}[kind]
        png, text = draw(self.ch, self.geo(full=place is not None), year, period, place,
                         voiv=self.voiv() if place is None else None)
        if png is None:
            self.warn(chat_id, text)
        else:
            other = "Inny powiat, rok:" if kind == "uprawy" else "Inny powiat, rok, dzień:"
            self.tg.send_photo(chat_id, png, text + f"\n\n<b>{other}</b>\n{usage('/' + kind)}")

    def parse_year(self, tokens: list[str], keep: int, default: int | None = None) -> tuple[int | None, str]:
        """Ostatni token = rok, jeśli to 4 cyfry i zostaje `keep` innych tokenów (kod powiatu też ma 4 cyfry).
        Usuwa rok z `tokens`; zwraca (rok, błąd)."""
        year = default if default is not None else self.years[-1]
        if len(tokens) > keep and re.fullmatch(r"\d{4}", tokens[-1]):
            year = int(tokens.pop())
        if year not in self.years:
            return None, f"Brak danych dla roku {year}. Dostępne: {years_text(self.years)}."
        return year, ""

    def county_args(self, arg: str, n: int, default_year: int | None = None):
        """'3011 1206 2018' -> ([(kod, nazwa), ...], rok, błąd); najpierw n powiatów, rok na końcu."""
        tokens = arg.split()
        if len(tokens) < n:
            return [], None, "Brakuje powiatu." if n == 1 else f"Potrzebne są {n} powiaty."
        year, err = self.parse_year(tokens, n, default_year)
        if err:
            return [], None, err
        names = [" ".join(tokens)] if n == 1 else tokens[:n]          # 1 powiat: nazwa może mieć spację
        places = []
        for t in names:
            scope, err = self.find_scope("county", t)
            if scope is None:
                return [], None, f"„{t}” — {err[0].lower()}{err[1:]}"
            code = scope.split(":")[1]
            places.append((code, self.counties.loc[self.counties["region_id"] == code, "county_name"].iloc[0]))
        return places, year, ""

    def cmd_map(self, chat_id: int, arg: str) -> None:
        from B1_B2.reporter import bot_charts as bc
        _, year, p, err = self.map_args(arg, self.years)
        if err:
            self.warn(chat_id, err)
            return
        png, text = bc.map_png(self.ch, self.geo(), year, p)
        self.tg.send_photo(chat_id, png, text + f"\n\n<b>Inny rok, dzień:</b>\n{usage('/mapa')}")

    def cmd_chart(self, chat_id: int, kind: str, arg: str, n: int) -> None:
        from B1_B2.reporter import bot_charts as bc
        wy = bc.weather_years(self.ch)
        default = wy[-1] if kind == "water" and wy else None   # /woda bez roku: ostatni rok z pogodą
        places, year, err = self.county_args(arg, n, default)
        if err:
            self.warn(chat_id, err)
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
                self.warn(chat_id, f"Brak danych NDVI dla {year}.")
            elif res[0] is None:
                self.warn(chat_id, res[1])
            else:
                self.tg.send_photo(chat_id, *res)

    def cmd_codes(self, chat_id: int, arg: str) -> None:
        c = self.counties
        if not arg.strip():
            v = c.drop_duplicates("voiv_id")
            text = ("<b>Województwa</b> (kod — nazwa)\n\n"
                    + "\n".join(f"• <code>{i}</code> — {escape(n)}" for i, n in zip(v["voiv_id"], v["voiv_name"]))
                    + "\n\n<i>Powiaty województwa: /kody 30 albo /kody wielkopolskie</i>")
            self.tg.send_text(chat_id, text)
            return
        scope, err = self.find_scope("voiv", arg)
        if scope is None:
            self.warn(chat_id, err + " Lista województw: /kody")
            return
        code = scope.split(":")[1]
        rows = c[c["voiv_id"] == code]
        text = (f"<b>Powiaty — {escape(rows['voiv_name'].iloc[0])}</b> (kod — nazwa)\n\n"
                + "\n".join(f"• <code>{i}</code> — {escape(n.removeprefix('powiat '))}"
                            for i, n in zip(rows["region_id"], rows["county_name"]))
                + "\n\n<i>Miasta na prawach powiatu pomijamy (prawie bez upraw). "
                  "Użycie kodu, np. /wykres 3011 albo /ndvi 3011 2018 15.07</i>")
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
        return f"{names.iloc[0]} ({code})" if len(names) else scope

    def find_scope(self, kind: str, arg: str) -> tuple[str | None, str]:
        """Kod albo nazwa -> zakres; przy kilku pasujących nazwach zwraca listę do wyboru."""
        arg = arg.strip().lower()
        if not arg:
            return None, "Brakuje " + ("powiatu." if kind == "county" else "województwa.")
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
        return None, "Nie znam takiego miejsca — wpisz kod albo nazwę."

    # ---------- polecenia ----------

    def handle(self, msg: dict) -> None:
        chat_id = msg["chat"]["id"]
        name = " ".join(filter(None, [msg["chat"].get("first_name"), msg["chat"].get("last_name")])) \
            or msg["chat"].get("title", "")
        cmd, _, arg = (msg.get("text") or "").strip().partition(" ")
        cmd = cmd.split("@")[0].lower()
        self.cmd = cmd                                           # do podpowiedzi formatu w komunikatach błędów
        log.info("polecenie %s od czatu %s", cmd, chat_id)
        if cmd in ("/start", "/pomoc", "/help"):
            self.tg.send_text(chat_id, HELP)
        elif cmd == "/kody":
            self.cmd_codes(chat_id, arg)
        elif cmd == "/mapa":
            self.cmd_map(chat_id, arg)
        elif cmd == "/ndvi":
            self.cmd_raster_map(chat_id, "ndvi", arg)
        elif cmd == "/kbw":
            self.cmd_raster_map(chat_id, "kbw", arg)
        elif cmd == "/uprawy":
            self.cmd_raster_map(chat_id, "uprawy", arg)
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
                self.warn(chat_id, err)
                return
            self.set_scope(chat_id, name, scope, 1)
            self.tg.send_text(chat_id, "<b>Zapisano subskrypcję</b>\n\n"
                                       f"• Zakres: {escape(self.scope_label(scope))}\n"
                                       "• Nowe biuletyny PDF przyjdą tutaj automatycznie, co 16 dni (IV–IX)\n\n"
                                       "<i>Ostatni gotowy biuletyn: /ostatni · wszystkie subskrypcje: /moje</i>")
        elif cmd == "/moje":
            subs = self.subscriptions(chat_id)
            if len(subs):
                text = ("<b>Twoje subskrypcje</b>\n\n"
                        + "\n".join(f"• {escape(self.scope_label(s))}" for s in subs["scope"])
                        + "\n\n<i>Wypisanie ze wszystkich: /stop</i>")
            else:
                text = ("<b>Nie masz subskrypcji</b>\n\n• /powiat 3011 — powiat\n• /woj 30 — województwo\n"
                        "• /wszystko — cała Polska")
            self.tg.send_text(chat_id, text)
        elif cmd == "/stop":
            subs = self.subscriptions(chat_id)
            for s in subs["scope"]:
                self.set_scope(chat_id, name, s, 0)
            self.tg.send_text(chat_id, "<b>Wypisano</b>\n\n• Nie dostaniesz więcej biuletynów\n\n"
                                       "<i>Zapis ponownie: /powiat, /woj albo /wszystko</i>")
        elif cmd == "/ostatni":
            self.send_latest(chat_id)
        else:
            self.tg.send_text(chat_id, "Nie znam tego polecenia. Lista poleceń:\n\n" + HELP)

    def send_latest(self, chat_id: int) -> None:
        scopes = list(self.subscriptions(chat_id)["scope"])
        if not scopes:
            self.warn(chat_id, "Najpierw zapisz się: /powiat 3011, /woj 30 albo /wszystko.")
            return
        df = self.ch.query_df("SELECT * FROM rbd.bulletins WHERE status = 'ok' ORDER BY created DESC LIMIT 500")
        for row in df.to_dict("records"):
            if any(scope_matches(s, row["region_id"]) for s in scopes):
                self.send_bulletin(row, only_chat=chat_id)
                return
        self.warn(chat_id, "Nie ma jeszcze gotowego biuletynu dla Twoich subskrypcji.")

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
        caption = ("<b>Biuletyn stanu upraw</b>\n\n"
                   f"• Powiat: {escape(str(b['county_name']))}\n"
                   f"• Termin: {escape(str(b['period_label']))} {b['year']}"
                   + (f"\n• Najbardziej prawdopodobna przyczyna odchyleń (Bielik): {escape(cause)}"
                      if cause not in ("", "reguły") else "")
                   + "\n\n<i>W PDF: stan upraw względem normy, przebieg sezonu, pogoda i komentarz.</i>")
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



def send_s2_test_incident(bot: Bot, event: dict) -> None:
    """Historical S2 test event to one explicitly allowed chat only.

    Uses the existing bot token/proxy and never calls getUpdates.
    A success log suppresses duplicate Kafka redelivery; like other Bot API
    sends this is at-least-once if the process crashes between send and log.
    """
    import hashlib

    if os.environ.get("S1S2_TEST_ALERTS_ENABLED", "0") != "1":
        log.info("S2 test event ignored: test delivery disabled")
        return
    raw_chat = os.environ.get("S1S2_TEST_CHAT_ID", "").strip()
    if not re.fullmatch(r"-?\d+", raw_chat):
        raise ValueError("Set S1S2_TEST_CHAT_ID in master telegram/.env")
    chat_id = int(raw_chat)
    if event.get("source") != "S2" or event.get("historical") is not True:
        raise ValueError("Only explicit historical S2 events are accepted")
    rid = str(event.get("region_id", ""))
    if not re.fullmatch(r"\d{4}", rid):
        raise ValueError("Invalid region_id")
    eid = str(event.get("event_id", ""))
    if not re.fullmatch(r"[a-f0-9]{24}", eid):
        raise ValueError("Invalid event_id")
    if not str(event.get("start", "")).startswith("2018-"):
        raise ValueError("Refusing non-2018 test event")
    if int(event.get("hours", 0)) < 2:
        raise ValueError("Only incidents >= 2h accepted")

    ddl = """CREATE TABLE IF NOT EXISTS rbd.tg_s1s2_test_log (
        event_id String, chat_id Int64, region_id String,
        status LowCardinality(String), sent DateTime
    ) ENGINE = MergeTree ORDER BY (event_id, chat_id, sent)"""
    bot.ch.command(ddl)
    safe_eid = sql_str(eid)
    seen = bot.ch.query_df(
        f"SELECT count() AS n FROM rbd.tg_s1s2_test_log "
        f"WHERE event_id={safe_eid} AND chat_id={chat_id} AND status='ok'")
    if int(seen.iloc[0]["n"]) > 0:
        log.info("S2 historical test event %s already delivered to allowed chat", eid)
        return
    crop = {"kukurydza_ziemniaki":"kukurydza i ziemniaki",
            "rzepak_zboza_jare":"rzepak i zboża jare"}.get(
                str(event.get("crop_group", "")), str(event.get("crop_group", "")))
    msg = ("<b>SYMULACJA 2018 — incydent przymrozkowy S2</b>\n"
           f"Powiat TERYT: <code>{escape(rid)}</code>\n"
           f"Uprawy: {escape(crop)}\n"
           f"Od: {escape(str(event['start']))}\n"
           f"Do: {escape(str(event['end']))}\n"
           f"Czas zagrożenia: {int(event['hours'])} h\n"
           f"Minimum szacowane przy gruncie: {float(event['minimum']):.1f} °C\n"
           f"Próg umowny: {float(event['threshold']):.1f} °C\n"
           "<i>Dane historyczne ERA5-Land; wskaźnik modelowy, nie prognoza szkód.</i>")
    bot.tg.send_text(chat_id, msg)
    import pandas as pd
    bot.ch.insert_df("tg_s1s2_test_log", pd.DataFrame([{
        "event_id":eid,"chat_id":chat_id,"region_id":rid,"status":"ok",
        "sent":datetime.now().replace(microsecond=0)
    }]))
    log.info("S2 test incident %s delivered to configured test chat", eid)



def send_county_incident(bot: Bot, event: dict) -> None:
    # Only county subscriptions; best-effort durable idempotency.
    if os.environ.get('S1S2_INCIDENTS_ENABLED', '0') != '1':
        log.info('Incident ignored (disabled)')
        return
    if event.get('state') != 'closed' or event.get('source') not in ('S1','S2'):
        raise ValueError('Unexpected incident type/state')
    eid = str(event.get('event_id', ''))
    rid = str(event.get('region_id', ''))
    if not re.fullmatch(r'[a-f0-9]{64}', eid) or not re.fullmatch(r'[0-9]{4}', rid):
        raise ValueError('Invalid incident ID/region')
    source = event['source']
    duration = int(event['duration'])
    minimum = float(event['minimum'])
    if duration < 1 or duration > 366 or abs(minimum) > 10000:
        raise ValueError('Invalid duration/minimum')
    start, end = str(event.get('start', '')), str(event.get('end', ''))
    if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ', start):
        raise ValueError('Invalid incident start')
    if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ', end):
        raise ValueError('Invalid incident end')
    # Safety: explicit test mode restricts recipients to the one chat.
    test_only = os.environ.get('S1S2_INCIDENTS_TEST_ONLY', '1') == '1'
    test_chat = os.environ.get('S1S2_TEST_CHAT_ID', '')
    if test_only and not re.fullmatch(r'-?\d+', test_chat):
        raise ValueError('Missing S1S2_TEST_CHAT_ID; refusing send')
    if bool(event.get('historical')) and os.environ.get('S1S2_ALLOW_HISTORICAL', '0') != '1':
        log.info('Historical incident %s skipped by policy', eid)
        return

    # This query checks the actual /powiat subscription. No matching voiv/all scope.
    subscribers = bot.ch.query_df(
        f"SELECT DISTINCT chat_id FROM rbd.tg_subscribers FINAL "
        f"WHERE active=1 AND scope={sql_str('county:' + rid)}")
    chats = [int(row.chat_id) for row in subscribers.itertuples()]
    if test_only:
        chats = [cid for cid in chats if cid == int(test_chat)]
    if not chats:
        log.info('No /powiat subscribers for incident %s %s', eid, rid)
        return
    bot.ch.command('''CREATE TABLE IF NOT EXISTS rbd.tg_incident_delivery (
        event_id String, chat_id Int64, region_id String, source String,
        status LowCardinality(String), error String, sent DateTime('UTC')
    ) ENGINE MergeTree ORDER BY (event_id,chat_id,sent)''')
    crops = event.get('crops') or []
    if source == 'S2':
        labels = {'kukurydza_ziemniaki':'kukurydza i ziemniaki',
                  'rzepak_zboza_jare':'rzepak i zboża jare'}
        crops_text = ', '.join(labels.get(str(x), str(x)) for x in crops) or 'brak określonej grupy'
        headline = 'Incydent przymrozkowy S2'
        metric = f'Minimum szacowane przy gruncie: {minimum:.1f} °C'
        detail = f'Zagrożone grupy: {escape(crops_text)}\nCzas: {duration} h'
    else:
        headline = 'Incydent deficytu wodnego S1'
        metric = f'Najniższy KBW 60 dni: {minimum:.1f} mm'
        detail = f'Czas: {duration} dni (pełne okna KBW 60 dni)'
    prefix = 'SYMULACJA HISTORYCZNA 2018 — ' if bool(event.get('historical')) else ''
    body = (f'<b>{escape(prefix + headline)}</b>\n'
            f'Powiat TERYT: <code>{rid}</code>\n'
            f'{detail}\nOd: {escape(start)}\nDo: {escape(end)}\n'
            f'{metric}\n'
            '<i>Wskaźnik modelowy; nie potwierdza szkód i nie stanowi prognozy.</i>')
    import pandas as pd
    for chat_id in chats:
        already = bot.ch.query_df(
            f"SELECT count() AS n FROM rbd.tg_incident_delivery "
            f"WHERE event_id={sql_str(eid)} AND chat_id={chat_id} AND status='ok'")
        if int(already.iloc[0]['n']) > 0:
            log.info('Duplicate incident %s chat=%s skipped', eid, chat_id)
            continue
        # If Telegram fails, leave Kafka offset uncommitted; retry after recovery.
        bot.tg.send_text(chat_id, body)
        bot.ch.insert_df('tg_incident_delivery', pd.DataFrame([{
            'event_id': eid, 'chat_id': chat_id, 'region_id':rid, 'source':source,
            'status':'ok', 'error':'', 'sent':datetime.utcnow().replace(microsecond=0)}]))
        log.info('Incident delivered event=%s source=%s county=%s chat=%s', eid, source, rid, chat_id)
        time.sleep(PAUSE_S)


def run_service(bot: Bot) -> None:
    from confluent_kafka import Consumer

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP, "enable.auto.commit": False,
                         "auto.offset.reset": "latest", "max.poll.interval.ms": 1_800_000})
    topics = [TOPIC_READY]
    if os.environ.get("S1S2_TEST_ALERTS_ENABLED", "0") == "1":
        topics.append(S2_TEST_TOPIC)
    if os.environ.get('S1S2_INCIDENTS_ENABLED', '0') == '1':
        topics.append(INCIDENTS_TOPIC)
    consumer.subscribe(topics)
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
            if msg.topic() == INCIDENTS_TOPIC:
                try:
                    send_county_incident(bot, json.loads(msg.value()))
                except (RuntimeError, requests.RequestException, OSError) as e:
                    log.error('Incident delivery failed: %s', type(e).__name__)
                    from confluent_kafka import TopicPartition
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    time.sleep(10)
                    continue
                except (ValueError, KeyError, TypeError) as e:
                    log.error('Invalid incident ignored: %s', type(e).__name__)
                consumer.commit(msg, asynchronous=False)
                continue
            if msg.topic() == S2_TEST_TOPIC:
                try:
                    send_s2_test_incident(bot, json.loads(msg.value()))
                except (RuntimeError, requests.RequestException, OSError) as e:
                    log.error("S2 test delivery failed: %s", type(e).__name__)
                    time.sleep(10)
                    continue  # no offset commit, retry after recovery
                except (ValueError, KeyError, TypeError) as e:
                    log.error("Invalid S2 test event skipped: %s", type(e).__name__)
                consumer.commit(msg, asynchronous=False)
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
