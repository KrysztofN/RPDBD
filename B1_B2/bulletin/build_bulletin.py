"""
Biuletyn stanu upraw (B1 + B2) dla jednego powiatu i jednego terminu: dane -> wykresy -> plik LaTeX.
PDF składa osobny obraz rbd-latex (infra/rbd-latex/Dockerfile) — polecenie na dole tego opisu.

Co robi:
  1. pobiera z ClickHouse: stan upraw (widok b1b2_periods — B1 z kontekstem sezonu z B2), daty sezonu (b2_phenology),
     a z HDFS: granice powiatów (mapka województwa) i strukturę upraw powiatu (EUCROPMAP);
  2. rysuje 3 wykresy (matplotlib, PDF wektorowy): krzywa NDVI na tle normy z datami sezonu, mapka województwa,
     struktura upraw;
  3. układa teksty z danych (reguły — bez AI) i wypełnia szablon template.tex (Jinja2) -> bulletin.tex.

Trochę teorii:
  Biuletyn ma odpowiedzieć analitykowi na jedno pytanie: „jak w tym terminie przebiega sezon w powiecie i czy jest powód
  do niepokoju?”. B1 mówi „jak jest” (zieloność vs norma), B2 „czy sezon jest przesunięty”, razem odróżniają słabe uprawy
  od spóźnionych. Wydanie = jeden 16-dniowy termin zdjęć MODIS (period 0..22, początek = 1 + 16·period dzień roku).
  Teksty są składane z liczb regułami, więc biuletyn powstaje także bez modelu AI; komentarz Bielika dojdzie później.

Uruchomienie (na masterze, w kontenerze rbd-python; hasło ClickHouse w zmiennej CLICKHOUSE_PASSWORD):
  python3 -m B1_B2.bulletin.build_bulletin --county 3011 --year 2018 --period 10
  -> data/bulletins/3011_2018_p10/bulletin.tex (+ fig_*.pdf); kompilacja: latexmk -pdf bulletin.tex (obraz rbd-latex)
"""
from __future__ import annotations

import argparse
import io
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from lib.log import Timer, get_logger, setup_logging

log = get_logger(Path(__file__).stem)

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = Path(__file__).with_name("template.tex")
REF_BOUNDARIES = "/rbd/ref/boundaries/counties_raw.gpkg"
REF_STRUCTURE = "/rbd/ref/crops/crop_structure_counties_{map_year}.parquet"
MONTHS_GEN = ["stycznia", "lutego", "marca", "kwietnia", "maja", "czerwca", "lipca", "sierpnia", "września",
              "października", "listopada", "grudnia"]
ROMAN = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"]
B1_COLOR = {"gorzej niż zwykle": "worse", "typowo": "normal", "lepiej niż zwykle": "better", "brak danych": "muted"}
B2_COLOR = {"wcześniej niż zwykle": "better", "typowo": "normal", "później niż zwykle": "late", "brak danych": "muted"}
HEX = {"worse": "#D7301F", "normal": "#2E8B57", "better": "#2C6FBB", "late": "#E6862E", "muted": "#B0B0B0"}
GROUP_LABELS = {"winter_cereals_rapeseed": "zboża ozime i rzepak", "spring_cereals": "zboża jare", "maize": "kukurydza",
                "root_crops": "okopowe", "grassland": "łąki i pastwiska", "other_crops": "inne uprawy"}


# --------------------------------------------------------------------------- pomocnicze: tekst
def tex(value) -> str:
    """Oczyszczenie tekstu dla LaTeX (znaki specjalne); liczby i None też obsługuje."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "—"
    s = str(value)
    repl = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#", "_": r"\_",
            "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}",
            # znaki spoza kodowania LaTeX (pdflatex) — zamieniane na polecenia
            "≥": r"$\ge$", "≤": r"$\le$", "−": r"$-$", "₀": r"\textsubscript{0}", "±": r"$\pm$"}
    return "".join(repl.get(ch, ch) for ch in s)


def num(x, digits: int = 0) -> str:
    """Liczba po polsku (przecinek dziesiętny)."""
    if x is None or pd.isna(x):
        return "—"
    return f"{x:.{digits}f}".replace(".", ",")


def day_month(d) -> str:
    return "—" if d is None or pd.isna(d) else f"{pd.Timestamp(d).day} {MONTHS_GEN[pd.Timestamp(d).month - 1]}"


def shift_text(days) -> str:
    if days is None or pd.isna(days):
        return "—"
    d = int(round(days))
    return "w terminie" if d == 0 else (f"{abs(d)} dni później" if d > 0 else f"{abs(d)} dni wcześniej")


def period_dates(year: int, period: int) -> tuple[date, date]:
    start = date(year, 1, 1) + timedelta(days=16 * period)
    return start, start + timedelta(days=15)


def period_label(year: int, period: int) -> str:
    s, e = period_dates(year, period)
    return f"{s.day}–{e.day} {ROMAN[e.month - 1]} {year}" if s.month == e.month else \
        f"{s.day} {ROMAN[s.month - 1]} – {e.day} {ROMAN[e.month - 1]} {year}"


# --------------------------------------------------------------------------- dane
def load_data(county: str, year: int, period: int) -> dict:
    from lib.clickhouse import ClickHouse
    from lib.webhdfs import WebHDFS

    ch = ClickHouse()
    periods = ch.query_df(f"SELECT * FROM b1b2_periods WHERE region_id = '{county}' AND year = {year} ORDER BY period")
    if periods.empty or period not in set(periods["period"]):
        raise SystemExit(f"brak danych B1 dla powiatu {county}, rok {year}, termin {period}")
    b2 = ch.query_df(f"SELECT * FROM b2_phenology WHERE region_id = '{county}' AND year = {year}")
    voiv_id = county[:2]
    voiv_now = ch.query_df(f"SELECT region_id, status, z FROM b1_periods WHERE voiv_id = '{voiv_id}' "
                           f"AND year = {year} AND period = {period}")
    hdfs = WebHDFS()
    import geopandas as gpd
    counties = gpd.read_file(io.BytesIO(hdfs.read_bytes(REF_BOUNDARIES)))
    map_year = 2018 if year <= 2020 else 2022
    structure = pd.read_parquet(io.BytesIO(hdfs.read_bytes(REF_STRUCTURE.format(map_year=map_year))))
    try:                                   # pogoda: wszystkie lata powiatu (bieżący + lata do normy)
        weather = ch.query_df(f"SELECT * FROM weather_periods WHERE region_id = '{county}' ORDER BY year, period")
    except RuntimeError:
        weather = pd.DataFrame()
    return dict(periods=periods, b2=b2, voiv_now=voiv_now, counties=counties, map_year=map_year,
                structure=structure[structure["region_id"] == county], weather=weather)


# --------------------------------------------------------------------------- pogoda
SEASON_FIRST_PERIOD = 6                    # termin od ok. 7 kwietnia
DROUGHT_KBW_60D = -150.0                   # próg suszy z README (uproszczenie IUNG): bilans wodny 60 dni < -150 mm


def weather_summary(weather: pd.DataFrame, year: int, period: int, clip_k: float = 1.5) -> dict | None:
    """
    Pogoda powiatu w terminie wydania i od początku sezonu na tle normy z lat poprzednich (do 15 lat, bez lat
    skrajnych — ta sama zasada co w B1). Zwraca liczby do tekstu, wykresu i reguł przyczyn; None = brak danych.
    """
    from B1_B2.norms import clipped_stats

    if weather.empty or not ((weather["year"] == year) & (weather["period"] == period)).any():
        return None
    w = weather.set_index(["year", "period"]).sort_index()
    ref = weather[(weather["year"] < year) & (weather["year"] >= year - 15)]
    norm = (ref.groupby("period")[["precip_mm", "et0_mm", "kbw_mm", "kbw_60d_mm", "t_mean"]]
            .agg(lambda v: clipped_stats(v, clip_k)["norm_mean"]))
    cur = w.loc[(year, period)]
    season = weather[(weather["year"] == year) & weather["period"].between(SEASON_FIRST_PERIOD, period)]
    recent = weather[(weather["year"] == year) & weather["period"].between(period - 1, period)]
    spring = weather[(weather["year"] == year) & weather["period"].between(SEASON_FIRST_PERIOD, min(period, 9))]
    n_ref = ref["year"].nunique()
    ref_season = norm.loc[norm.index.intersection(range(SEASON_FIRST_PERIOD, period + 1))]
    return dict(
        cur=cur, norm=norm, n_ref=n_ref, ref_years=(int(ref["year"].min()), int(ref["year"].max())) if n_ref else None,
        year_rows=weather[weather["year"] == year].set_index("period"),
        kbw_60d=float(cur["kbw_60d_mm"]),
        kbw_60d_norm=float(norm.loc[period, "kbw_60d_mm"]) if period in norm.index else np.nan,
        season_precip=float(season["precip_mm"].sum()), season_et0=float(season["et0_mm"].sum()),
        season_precip_norm=float(ref_season["precip_mm"].sum()) if n_ref else np.nan,
        hot_recent=int(recent["hot_days"].sum()), frost_spring=int(spring["frost_days"].sum()),
        recent_precip=float(recent["precip_mm"].sum()),
        recent_precip_norm=float(norm.loc[norm.index.intersection([period - 1, period]), "precip_mm"].sum()) if n_ref else np.nan,
        spring_t=float(spring["t_mean"].mean()) if not spring.empty else np.nan,
        spring_t_norm=float(norm.loc[norm.index.intersection(range(SEASON_FIRST_PERIOD, min(period, 9) + 1)), "t_mean"].mean())
        if n_ref else np.nan,
    )


def weather_causes(ws: dict | None, b1_status: str, start_status: str | None, end_status: str | None,
                   period: int) -> list[str]:
    """Możliwe przyczyny z liczb pogodowych (reguły; lista uporządkowana od najsilniejszej)."""
    if ws is None:
        return []
    causes = []
    k, kn = ws["kbw_60d"], ws["kbw_60d_norm"]
    if k <= DROUGHT_KBW_60D:
        causes.append(f"susza: bilans wodny z 60 dni {num(k)} mm (próg suszy {num(DROUGHT_KBW_60D)} mm)")
    elif k <= -100 or (np.isfinite(kn) and k <= kn - 50):
        causes.append(f"niedobór wody: bilans wodny z 60 dni {num(k)} mm (typowo {num(kn)} mm)")
    if ws["hot_recent"] >= 3:
        causes.append(f"upał: {ws['hot_recent']} dni z temperaturą ≥ 30 °C w ostatnich ok. 32 dniach")
    if ws["frost_spring"] >= 1 and period <= 12:
        causes.append(f"przymrozki wiosną: {ws['frost_spring']} dni z temperaturą < 0 °C od początku kwietnia")
    if np.isfinite(ws["recent_precip_norm"]) and ws["recent_precip"] >= 1.8 * ws["recent_precip_norm"] \
            and ws["recent_precip"] >= 60:
        causes.append(f"nadmiar wody: opad {num(ws['recent_precip'])} mm w ok. 32 dni (typowo {num(ws['recent_precip_norm'])} mm)")
    if start_status == "później niż zwykle" and np.isfinite(ws["spring_t_norm"]) \
            and ws["spring_t"] <= ws["spring_t_norm"] - 1:
        causes.append(f"chłodna wiosna: średnio {num(ws['spring_t'], 1)} °C (typowo {num(ws['spring_t_norm'], 1)} °C)")
    return causes


def plot_weather(ws: dict, year: int, edition_start: date, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    yr = ws["year_rows"]
    x = yr.index.to_numpy().astype(int) * 16 + 1          # numer terminu z ClickHouse to UInt8 — bez astype przepełnienie
    fig, ax = plt.subplots(figsize=(3.6, 2.5))
    ax.bar(x - 3, yr["precip_mm"], width=6, color="#2C6FBB", label="opad")
    ax.bar(x + 3, yr["et0_mm"], width=6, color="#E6862E", label="parowanie ET₀")
    ax.set_ylabel("mm / 16 dni", fontsize=6)
    ax2 = ax.twinx()
    ax2.plot(x, yr["kbw_60d_mm"], color="#8B0000", lw=1.6, label="bilans wodny 60 dni")
    if ws["n_ref"]:
        nx = ws["norm"].index.to_numpy().astype(int) * 16 + 1
        ax2.plot(nx, ws["norm"]["kbw_60d_mm"], color="#8B0000", lw=0.8, ls="--", alpha=0.6, label="bilans — typowo")
    ax2.axhline(DROUGHT_KBW_60D, color="black", lw=0.6, ls=":")
    ax2.text(5, DROUGHT_KBW_60D, " próg suszy", fontsize=5, va="bottom")
    ax2.set_ylabel("bilans 60 dni [mm]", fontsize=6)
    ax.axvline((edition_start - date(year, 1, 1)).days + 1, color="black", lw=0.8, ls=":")
    ax.set_xticks([1, 60, 121, 182, 244, 305])
    ax.set_xticklabels(["I", "III", "V", "VII", "IX", "XI"])
    ax.set_xlim(-8, 372)
    for a in (ax, ax2):
        a.tick_params(labelsize=6)
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, fontsize=5, loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=4, frameon=False)
    fig.tight_layout(pad=0.3)
    fig.savefig(out)
    plt.close(fig)


# --------------------------------------------------------------------------- wykresy
def plot_curve(periods: pd.DataFrame, b2: pd.Series | None, edition_start: date, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    d = pd.to_datetime(periods["date"])
    fig, ax = plt.subplots(figsize=(3.6, 2.25))
    ax.fill_between(d, periods["norm_p25"], periods["norm_p75"], color="#d9d9d9", lw=0, label="norma 25–75%")
    ax.plot(d, periods["norm_mean"], color="#777777", lw=1, ls="--", label="norma (średnia)")
    ax.plot(d, periods["ndvi_smooth"], color="#2E8B57", lw=2, label=f"{d.dt.year.iloc[0]} (wygładzone)")
    ax.scatter(d, periods["ndvi_mean"], s=7, color="#E6862E", zorder=3, label="pomiar")
    if b2 is not None:
        for col, lab in (("start_date", "start"), ("peak_date", "szczyt"), ("end_date", "koniec")):
            if pd.notna(b2.get(col)):
                ax.axvline(pd.Timestamp(b2[col]), color="#2C6FBB", lw=0.8, alpha=0.8)
                ax.text(pd.Timestamp(b2[col]), 0.98, lab, rotation=90, va="top", ha="right", fontsize=5.5,
                        color="#2C6FBB", transform=ax.get_xaxis_transform())
    ax.axvline(pd.Timestamp(edition_start), color="black", lw=0.8, ls=":")
    ax.set_ylim(0, 1)
    ax.set_ylabel("NDVI", fontsize=7)
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=range(1, 13, 2)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m.%y"))
    ax.tick_params(labelsize=6)
    ax.legend(fontsize=5.5, loc="lower center", ncol=2, frameon=False)
    ax.grid(alpha=0.25, lw=0.5)
    fig.tight_layout(pad=0.3)
    fig.savefig(out)
    plt.close(fig)


def plot_map(counties, voiv_now: pd.DataFrame, county: str, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    c = counties.assign(region_id=counties["JPT_KOD_JE"].astype(str))
    c = c[c["region_id"].str[:2] == county[:2]].merge(voiv_now, on="region_id", how="left")
    c = c.set_geometry(c.geometry.simplify(200))           # ok. 200 m: na mapce wielkości kolumny bez różnicy, mniejszy PDF
    c["color"] = c["status"].map(lambda s: HEX[B1_COLOR.get(s, "muted")] if isinstance(s, str) else HEX["muted"])
    fig, ax = plt.subplots(figsize=(3.6, 2.6))
    c.plot(ax=ax, color=c["color"], edgecolor="white", linewidth=0.4)
    c[c["region_id"] == county].boundary.plot(ax=ax, color="black", linewidth=1.6)
    ax.set_axis_off()
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=HEX["worse"], label="gorzej niż zwykle"), Patch(color=HEX["normal"], label="typowo"),
                       Patch(color=HEX["better"], label="lepiej niż zwykle"), Patch(color=HEX["muted"], label="brak danych")],
              fontsize=5.5, loc="lower left", frameon=False)
    fig.tight_layout(pad=0.1)
    fig.savefig(out)
    plt.close(fig)


def plot_structure(structure: pd.DataFrame, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    row = structure.iloc[0]
    keys = [k for k in GROUP_LABELS if k != "grassland" and f"share_{k}_pct" in row]
    vals = [row[f"share_{k}_pct"] for k in keys]
    order = np.argsort(vals)
    fig, ax = plt.subplots(figsize=(3.4, 1.5))
    ax.barh([GROUP_LABELS[keys[i]] for i in order], [vals[i] for i in order], color="#2E8B57")
    for y, i in enumerate(order):
        ax.text(vals[i] + 1, y, f"{vals[i]:.0f}%".replace(".", ","), va="center", fontsize=6)
    ax.set_xlim(0, max(vals) * 1.2 + 1)
    ax.tick_params(labelsize=6)
    ax.set_xlabel("% pól ornych powiatu", fontsize=6)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.tight_layout(pad=0.3)
    fig.savefig(out)
    plt.close(fig)


# --------------------------------------------------------------------------- teksty (reguły)
def build_context(data: dict, county: str, year: int, period: int) -> dict:
    p = data["periods"]
    cur = p[p["period"] == period].iloc[0]
    ed_start, ed_end = period_dates(year, period)
    b2a = data["b2"][data["b2"]["crop_class"] == "arable"]
    b2 = b2a.iloc[0] if not b2a.empty else None
    status = cur["status"]
    name = cur["county_name"]

    # B1: ostatnie terminy od kwietnia do wydania
    recent = p[(p["period"] <= period) & (p["period"] >= max(5, period - 5))]
    b1_rows = [dict(label=period_label(year, int(r.period)), pct=f"{num(r.pct_of_norm)}%", z=num(r.z, 1),
                    status=r.status, color=B1_COLOR.get(r.status, "muted")) for r in recent.itertuples()]
    season_part = p[(p["period"] >= 6) & (p["period"] <= period)]
    season_pct = season_part["pct_of_norm"].mean() if not season_part.empty else np.nan
    streak = int(cur["below_streak"])

    # B2: etapy, które już nastąpiły do końca terminu wydania
    def stage(col, label, norm_col, shift_col, status_col):
        if b2 is None or pd.isna(b2.get(col)):
            return dict(stage=label, date="—", typical="—", shift="—", color="muted", happened=False)
        happened = pd.Timestamp(b2[col]).date() <= ed_end
        typical = date(year, 1, 1) + timedelta(days=int(round(b2[norm_col])) - 1) if pd.notna(b2[norm_col]) else None
        return dict(stage=label, date=day_month(b2[col]) if happened else "jeszcze nie",
                    typical=day_month(typical) if typical else "—",
                    shift=shift_text(b2[shift_col]) if happened else "—",
                    color=B2_COLOR.get(b2[status_col], "muted") if happened else "muted", happened=happened,
                    status=b2[status_col])
    stages = [stage("start_date", "start", "start_norm", "start_shift", "start_status"),
              stage("peak_date", "szczyt", "peak_norm", "peak_shift", "peak_status"),
              stage("end_date", "koniec", "end_norm", "end_shift", "end_status")]

    groups = data["b2"][data["b2"]["crop_class"] != "arable"]
    group_rows = [dict(group=GROUP_LABELS.get(r.crop_class, r.crop_class), start=day_month(r.start_date),
                       peak=day_month(r.peak_date), end=day_month(r.end_date)) for r in groups.itertuples()] \
        if year in (2018, 2022) else []

    # sąsiedzi w województwie
    vn = data["voiv_now"]
    n_worse = int((vn["status"] == "gorzej niż zwykle").sum())
    neighbours_text = f"Gorzej niż zwykle: {n_worse} z {len(vn)} powiatów województwa; {name} obwiedziony."

    # fazy
    phase = cur.get("season_phase") or "—"
    context = cur.get("season_context") or ""

    # teksty
    s = [f"W terminie {period_label(year, period)} zieloność pól ornych w powiecie wynosiła {num(cur['pct_of_norm'])}% "
         f"typowego poziomu dla tej pory roku — {status}."]
    if streak >= 2:
        s.append(f"Taki stan utrzymuje się od {streak} kolejnych terminów (ok. {16 * streak} dni).")
    if context:
        s.append(f"Kontekst sezonu: {context}.")
    st = stages[0]
    if st["happened"]:
        s.append(f"Sezon wegetacyjny ruszył {st['date']} ({st['shift']} niż zwykle)."
                 if st["shift"] not in ("—", "w terminie") else f"Sezon wegetacyjny ruszył {st['date']}, w typowym terminie.")
    s.append(f"W całym województwie ({cur['voiv_name']}) gorzej niż zwykle jest w {n_worse} z {len(vn)} powiatów.")

    b1_text = (f"Średnia zieloność pól od początku kwietnia do tego terminu: {num(season_pct)}% normy. "
               f"Tabela pokazuje ostatnie terminy; status opiera się na odchyleniu od normy dla danego terminu.")
    done = [x for x in stages if x["happened"]]
    b2_text = ("Etapy sezonu, które już nastąpiły: " + ", ".join(f"{x['stage']} {x['date']} ({x['shift']})" for x in done) + "."
               if done else "Sezon wegetacyjny jeszcze się nie rozpoczął (krzywa NDVI poniżej połowy rocznej „górki”).")
    srow = data["structure"].iloc[0] if not data["structure"].empty else None
    if srow is not None:
        top = sorted(((srow[f"share_{k}_pct"], GROUP_LABELS[k]) for k in GROUP_LABELS
                      if k != "grassland" and f"share_{k}_pct" in srow), reverse=True)[:2]
        structure_text = (f"Pola orne: ok. {num(srow['arable_km2'])} km² (mapa upraw {data['map_year']}). "
                          f"Przeważają: {top[0][1]} ({num(top[0][0])}%) i {top[1][1]} ({num(top[1][0])}%). "
                          f"Łąki i pastwiska: ok. {num(srow['grassland_km2'])} km².")
    else:
        structure_text = "Brak danych o strukturze upraw dla tego powiatu."
    ws = weather_summary(data.get("weather", pd.DataFrame()), year, period)
    causes = weather_causes(ws, status, stages[0].get("status"), stages[2].get("status") if stages[2]["happened"] else None,
                            period)
    if ws is None:
        weather_text = ("Brak danych pogodowych ERA5-Land dla tego roku — są jeszcze pobierane; "
                        "sekcja pojawi się po ich przeliczeniu.")
    else:
        c = ws["cur"]
        ref = (f"lata {ws['ref_years'][0]}–{ws['ref_years'][1]} ({ws['n_ref']} lat)" if ws["n_ref"]
               else "brak lat do porównania")
        weather_text = (
            f"W tym terminie spadło {num(c['precip_mm'])} mm deszczu przy parowaniu {num(c['et0_mm'])} mm "
            f"(bilans {num(c['kbw_mm'])} mm). Bilans wodny z ostatnich 60 dni: {num(ws['kbw_60d'])} mm"
            + (f" (typowo {num(ws['kbw_60d_norm'])} mm)" if np.isfinite(ws["kbw_60d_norm"]) else "")
            + f"; próg suszy to {num(DROUGHT_KBW_60D)} mm. Od początku kwietnia: opad {num(ws['season_precip'])} mm"
            + (f" (typowo {num(ws['season_precip_norm'])} mm)" if np.isfinite(ws["season_precip_norm"]) else "")
            + f", parowanie {num(ws['season_et0'])} mm. Dni upalne (≥ 30 °C) w ostatnich ok. 32 dniach: {ws['hot_recent']}; "
            f"dni z przymrozkiem od kwietnia: {ws['frost_spring']}. Norma pogodowa: {ref}.")
    lead = (f"Uprawy {status} w tym terminie" + (f"; {context}" if context else "") + ".")
    if causes:
        assessment_text = lead + " Możliwe przyczyny wg danych pogodowych: " + "; ".join(causes) + "."
    elif status == "gorzej niż zwykle":
        assessment_text = lead + (" Dane pogodowe nie wskazują wyraźnej przyczyny (susza, upał, przymrozek, nadmiar wody) — "
                                  "możliwe przyczyny lokalne (np. grad, szkodniki, choroby); warto sprawdzić w terenie."
                                  if ws is not None else " Ocena przyczyn pogodowych pojawi się po przeliczeniu danych ERA5-Land.")
    else:
        assessment_text = lead + " Brak sygnałów niepokojących."

    norm_from, norm_to = int(cur["norm_from"]), int(cur["norm_to"])
    short = {"gorzej niż zwykle": "gorzej", "typowo": "typowo", "lepiej niż zwykle": "lepiej", "brak danych": "brak"}
    return dict(
        county_name=name, voiv_name=cur["voiv_name"], region_id=county, year=year,
        edition=f"{period + 1}/{year}", period_label=period_label(year, period),
        pct_of_norm=f"{num(cur['pct_of_norm'])}%", pct_caption="typowego poziomu",
        status_short=short.get(status, status), status_color=B1_COLOR.get(status, "muted"),
        status_caption=(f"od {streak} terminów" if streak >= 2 else "niż zwykle w tym terminie"),
        start_kpi=(stages[0]["date"] if stages[0]["happened"] else "—"),
        start_caption=(stages[0]["shift"] if stages[0]["happened"] else "jeszcze nie"),
        phase_short=phase, phase_caption="wg krzywej NDVI (B2)",
        summary=" ".join(s),
        summary_note="Streszczenie złożone automatycznie z danych (reguły).",
        b1_text=b1_text, b1_rows=b1_rows, norm_years=f"{norm_from}–{norm_to}",
        b2_text=b2_text, b2_rows=stages, group_rows=group_rows, neighbours_text=neighbours_text,
        structure_text=structure_text, weather_text=weather_text, assessment_text=assessment_text,
        map_year=data["map_year"],
        ai_note="Komentarz analityka AI (model Bielik) zostanie dodany w kolejnej wersji biuletynu.",
        generated=datetime.now().strftime("%Y-%m-%d %H:%M"),
        has_weather=ws is not None, causes=causes,
        _b2=b2, _edition_start=ed_start, _weather=ws,
    )


def render(context: dict, out: Path) -> None:
    import jinja2

    env = jinja2.Environment(
        block_start_string=r"\BLOCK{", block_end_string="}",
        variable_start_string=r"\VAR{", variable_end_string="}",
        comment_start_string=r"\#{", comment_end_string="}",
        line_statement_prefix=None, trim_blocks=True, lstrip_blocks=True, autoescape=False,
        loader=jinja2.FileSystemLoader(str(TEMPLATE.parent)), undefined=jinja2.StrictUndefined)
    env.filters["tex"] = tex
    out.write_text(env.get_template(TEMPLATE.name).render(**context), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Biuletyn stanu upraw (B1 + B2) dla powiatu i terminu -> LaTeX")
    p.add_argument("--county", required=True, help="kod TERYT powiatu, np. 3011")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--period", type=int, required=True, help="termin 16-dniowy 0..22 (np. 10 = 10–25 czerwca)")
    p.add_argument("--out-dir", type=Path, default=ROOT / "data" / "bulletins")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def prepare_bulletin(county: str, year: int, period: int, out: Path) -> dict:
    """Dane + wykresy + teksty z reguł (bez renderowania). Zwraca kontekst szablonu — raporter może podmienić teksty."""
    out.mkdir(parents=True, exist_ok=True)
    data = load_data(county, year, period)
    ctx = build_context(data, county, year, period)
    ws, ed_start = ctx.pop("_weather"), ctx.pop("_edition_start")
    if ws is not None:
        plot_weather(ws, year, ed_start, out / "fig_weather.pdf")
    plot_curve(data["periods"], ctx.pop("_b2"), ed_start, out / "fig_curve.pdf")
    plot_map(data["counties"], data["voiv_now"], county, out / "fig_map.pdf")
    if not data["structure"].empty:
        plot_structure(data["structure"], out / "fig_structure.pdf")
    ctx["_weather_summary"] = ws
    return ctx


def compile_pdf(out: Path) -> Path:
    """latexmk -> bulletin.pdf (w obrazie z TeX Live: rbd-latex albo rbd-reporter)."""
    import subprocess

    r = subprocess.run(["latexmk", "-pdf", "-interaction=nonstopmode", "-halt-on-error", "bulletin.tex"],
                       cwd=out, capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        errors = [line for line in r.stdout.splitlines() if line.startswith("!")][:5]
        raise RuntimeError(f"LaTeX: {errors or r.stdout[-500:]}")
    return out / "bulletin.pdf"


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)
    out = args.out_dir / f"{args.county}_{args.year}_p{args.period:02d}"
    with Timer(log, f"biuletyn {args.county} {args.year} termin {args.period}"):
        ctx = prepare_bulletin(args.county, args.year, args.period, out)
        ctx.pop("_weather_summary")
        render(ctx, out / "bulletin.tex")
    log.info("%s: %s | %s | %s -> %s", ctx["county_name"], ctx["period_label"], ctx["pct_of_norm"],
             ctx["status_short"], out / "bulletin.tex")


if __name__ == "__main__":
    main()
