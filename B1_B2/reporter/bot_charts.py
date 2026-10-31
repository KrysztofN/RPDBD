"""
Wykresy i krótkie opisy dla bota Telegram (PNG w pamięci): mapa Polski, krzywe NDVI, bilans wody.

Co robi:
  map_png      — mapa powiatów Polski w kolorach oceny B1 (gorzej / typowo / lepiej niż zwykle) w danym terminie;
  curve_png    — krzywa NDVI powiatu w roku na tle normy (pasmo 25–75%) + daty sezonu B2; dla 2 powiatów obie
                 krzywe na jednym wykresie (porównanie);
  water_png    — opad, parowanie (ET₀) i bilans wody z 60 dni na tle typowego i progu suszy; dla 2 powiatów
                 porównanie bilansów;
  *_text       — 2–4 zdania z tymi samymi liczbami (podpis pod obrazkiem).
Dane z ClickHouse (b1b2_periods, b1_periods, b2_phenology, weather_periods), kolory i progi jak w biuletynie PDF.

Trochę teorii:
  NDVI (0–1) mierzy „zieloność” roślin z satelity; norma = średnia z 15 poprzednich lat bez lat skrajnych.
  Bilans wody = opad − parowanie (ET₀, FAO-56) sumowany z 60 dni; poniżej −150 mm uznajemy suszę rolniczą
  (uproszczenie progu IUNG).
"""
from __future__ import annotations

import io
from datetime import date, timedelta

import numpy as np
import pandas as pd

from B1_B2.bulletin.build_bulletin import (B1_COLOR, DROUGHT_KBW_60D, HEX, day_month, num, period_label,
                                           shift_text, weather_summary)

COLORS_2 = ("#2E8B57", "#7B3294")             # dwa powiaty w porównaniu: zielony i fiolet


def _plt():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


def _png(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    _plt().close(fig)
    return buf.getvalue()


def period_of(d: date) -> int:
    """Numer 16-dniowego terminu MODIS (0 = 1–16 I)."""
    return (d - date(d.year, 1, 1)).days // 16


# ---------------------------------------------------------------- mapa
def map_png(ch, counties, year: int, period: int) -> tuple[bytes, str]:
    """counties: GeoDataFrame granic (region_id, geometry), już uproszczony."""
    plt = _plt()
    from matplotlib.patches import Patch

    st = ch.query_df(f"SELECT region_id, status FROM rbd.b1_periods WHERE year = {year} AND period = {period}")
    c = counties.merge(st, on="region_id", how="left")
    c["color"] = c["status"].map(lambda s: HEX[B1_COLOR.get(s, "muted")] if isinstance(s, str) else HEX["muted"])
    fig, ax = plt.subplots(figsize=(7, 6.6))
    c.plot(ax=ax, color=c["color"], edgecolor="white", linewidth=0.3)
    ax.set_axis_off()
    ax.set_title(f"Stan upraw względem normy, {period_label(year, period)}", fontsize=11)
    counts = st["status"].value_counts()
    ax.legend(handles=[Patch(color=HEX[B1_COLOR[s]], label=f"{s} ({counts.get(s, 0)})")
                       for s in ("gorzej niż zwykle", "typowo", "lepiej niż zwykle")]
              + [Patch(color=HEX["muted"], label="brak danych / miasto")],
              fontsize=8, loc="lower left", frameon=False)
    fig.tight_layout(pad=0.2)
    n = len(st)
    worse = int(counts.get("gorzej niż zwykle", 0))
    text = (f"Mapa: {period_label(year, period)}. Powiaty z danymi: {n}; gorzej niż zwykle: {worse} "
            f"({num(100 * worse / n if n else 0)}%), lepiej niż zwykle: {int(counts.get('lepiej niż zwykle', 0))}.")
    return _png(fig), text


# ---------------------------------------------------------------- krzywe NDVI
def _curve_data(ch, county: str, year: int):
    p = ch.query_df(f"SELECT * FROM rbd.b1b2_periods WHERE region_id = '{county}' AND year = {year} ORDER BY period")
    b2 = ch.query_df(f"SELECT * FROM rbd.b2_phenology WHERE region_id = '{county}' AND year = {year} AND crop_class = 'arable'")
    return p, (b2.iloc[0] if len(b2) else None)


def curve_png(ch, counties: list[tuple[str, str]], year: int) -> tuple[bytes, str] | None:
    """counties: [(kod, nazwa)] — 1 albo 2 powiaty. None = brak danych."""
    plt = _plt()
    import matplotlib.dates as mdates

    fig, ax = plt.subplots(figsize=(7, 4))
    lines = []
    for i, (code, name) in enumerate(counties):
        p, b2 = _curve_data(ch, code, year)
        if p.empty:
            plt.close(fig)
            return None
        d = pd.to_datetime(p["date"])
        color = COLORS_2[i]
        if len(counties) == 1:
            ax.fill_between(d, p["norm_p25"], p["norm_p75"], color="#d9d9d9", lw=0, label="norma 25–75%")
            ax.plot(d, p["norm_mean"], color="#777777", lw=1, ls="--", label="norma (średnia)")
            ax.scatter(d, p["ndvi_mean"], s=10, color="#E6862E", zorder=3, label="pomiar")
        else:
            ax.plot(d, p["norm_mean"], color=color, lw=0.9, ls="--", alpha=0.7, label=f"{name}: norma")
        ax.plot(d, p["ndvi_smooth"], color=color, lw=2.2, label=f"{name} {year}")
        if b2 is not None and len(counties) == 1:
            for col, lab in (("start_date", "start"), ("peak_date", "szczyt"), ("end_date", "koniec")):
                if pd.notna(b2.get(col)):
                    ax.axvline(pd.Timestamp(b2[col]), color="#2C6FBB", lw=0.8, alpha=0.8)
                    ax.text(pd.Timestamp(b2[col]), 0.98, lab, rotation=90, va="top", ha="right", fontsize=8,
                            color="#2C6FBB", transform=ax.get_xaxis_transform())
        lines.append(_curve_line(name, p, b2))
    ax.set_ylim(0, 1)
    ax.set_ylabel("NDVI")
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m"))
    ax.set_xlabel("miesiąc")
    ax.grid(alpha=0.25, lw=0.5)
    ax.legend(fontsize=8, loc="lower center", ncol=2, frameon=False)
    ax.set_title(("Krzywa NDVI: " if len(counties) == 1 else "Porównanie NDVI: ")
                 + " vs ".join(n for _, n in counties) + f", {year}", fontsize=11)
    fig.tight_layout(pad=0.3)
    return _png(fig), "\n".join(lines)


def _curve_line(name: str, p: pd.DataFrame, b2) -> str:
    season = p[p["period"].between(6, 17)]
    worse = int((season["status"] == "gorzej niż zwykle").sum())
    text = f"{name}: w sezonie (IV–IX) {worse} z {len(season)} terminów gorzej niż zwykle"
    if len(season):
        last = season.iloc[-1]                     # ostatni termin sezonu z danymi (najpóźniej IX)
        text += f"; ostatni termin ({period_label(int(last['year']), int(last['period']))}): {last['status']}"
    text += "."
    if b2 is not None:
        text += (f" Sezon: start {day_month(b2.get('start_date'))} ({shift_text(b2.get('start_shift'))}), "
                 f"koniec {day_month(b2.get('end_date'))} ({shift_text(b2.get('end_shift'))}).")
    return text


# ---------------------------------------------------------------- bilans wody
def _weather(ch, county: str) -> pd.DataFrame:
    return ch.query_df(f"SELECT * FROM rbd.weather_periods WHERE region_id = '{county}' ORDER BY year, period")


def weather_years(ch) -> list[int]:
    return [int(y) for y in ch.query_df("SELECT DISTINCT year FROM rbd.weather_periods ORDER BY year")["year"]]


def water_png(ch, counties: list[tuple[str, str]], year: int) -> tuple[bytes, str] | None:
    plt = _plt()
    fig, ax = plt.subplots(figsize=(7, 4))
    lines, ax2 = [], None
    for i, (code, name) in enumerate(counties):
        w = _weather(ch, code)
        yr = w[w["year"] == year].set_index("period")
        if yr.empty:
            plt.close(fig)
            return None
        last = int(yr.index.max())
        ws = weather_summary(w, year, last)
        x = yr.index.to_numpy().astype(int) * 16 + 1
        color = COLORS_2[i] if len(counties) > 1 else "#8B0000"
        if len(counties) == 1:
            ax.bar(x - 3, yr["precip_mm"], width=6, color="#2C6FBB", label="opad / 16 dni")
            ax.bar(x + 3, yr["et0_mm"], width=6, color="#E6862E", label="parowanie ET₀ / 16 dni")
            ax.set_ylabel("mm / 16 dni")
            ax2 = ax.twinx()
            target = ax2
        else:
            target = ax
        target.plot(x, yr["kbw_60d_mm"], color=color, lw=2, label=f"{name}: bilans 60 dni")
        if ws and ws["n_ref"]:
            nx = ws["norm"].index.to_numpy().astype(int) * 16 + 1
            target.plot(nx, ws["norm"]["kbw_60d_mm"], color=color, lw=0.9, ls="--", alpha=0.7,
                        label=f"{name}: bilans typowo")
        lines.append(_water_line(name, yr, ws))
    target.axhline(DROUGHT_KBW_60D, color="black", lw=0.8, ls=":")
    target.text(5, DROUGHT_KBW_60D, " próg suszy (−150 mm)", fontsize=8, va="bottom")
    target.set_ylabel("bilans wody z 60 dni [mm]")
    ax.set_xticks([1, 60, 121, 182, 244, 305])
    ax.set_xticklabels(["I", "III", "V", "VII", "IX", "XI"])
    ax.set_xlim(-8, 372)
    ax.grid(alpha=0.25, lw=0.5)
    h, l = ax.get_legend_handles_labels()
    if ax2 is not None:
        h2, l2 = ax2.get_legend_handles_labels()
        h, l = h + h2, l + l2
    ax.legend(h, l, fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=2, frameon=False)
    ax.set_title(("Bilans wody: " if len(counties) == 1 else "Porównanie bilansu wody: ")
                 + " vs ".join(n for _, n in counties) + f", {year}", fontsize=11)
    fig.tight_layout(pad=0.3)
    return _png(fig), "\n".join(lines)


def _water_line(name: str, yr: pd.DataFrame, ws: dict | None) -> str:
    season = yr.loc[yr.index.intersection(range(6, 18))]
    if season.empty:
        return f"{name}: brak danych pogodowych w sezonie."
    worst_p = int(season["kbw_60d_mm"].idxmin())
    worst = float(season["kbw_60d_mm"].min())
    drought = int((season["kbw_60d_mm"] <= DROUGHT_KBW_60D).sum())
    text = (f"{name}: najniższy bilans wody z 60 dni {num(worst)} mm ({period_label(int(season['year'].iloc[0]), worst_p)}); "
            f"terminów z suszą (≤ −150 mm): {drought}; opad IV–IX {num(season['precip_mm'].sum())} mm, "
            f"dni upału (≥ 30 °C): {int(season['hot_days'].sum())}.")
    if ws and ws["n_ref"] and worst_p in ws["norm"].index:
        text += f" Typowo w tym terminie: {num(ws['norm'].loc[worst_p, 'kbw_60d_mm'])} mm."
    return text
