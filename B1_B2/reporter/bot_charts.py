"""
Wykresy i krótkie opisy dla bota Telegram (PNG w pamięci): mapa Polski, krzywe NDVI, bilans wody.

Co robi:
  map_png      — mapa powiatów Polski w kolorach oceny B1 (gorzej / typowo / lepiej niż zwykle) w danym terminie;
  curve_png    — krzywa NDVI powiatu w roku na tle normy (pasmo 25–75%) + daty sezonu B2; dla 2 powiatów obie
                 krzywe na jednym wykresie (porównanie);
  water_png    — opad, parowanie (ET₀) i bilans wody z 60 dni na tle typowego i progu suszy; dla 2 powiatów
                 porównanie bilansów;
  ndvi_map_png — NDVI pikseli 250 m z pliku MODIS terminu (HDFS): cała Polska albo zbliżenie na powiat;
  kbw_map_png  — bilans wody z 60 dni na siatce ERA5-Land (rbd.weather_grid_periods) w klasach jak mapy IMGW:
                 cała Polska albo zbliżenie na powiat;
  *_text       — 2–4 zdania z tymi samymi liczbami (podpis pod obrazkiem).
Dane z ClickHouse (b1b2_periods, b1_periods, b2_phenology, weather_periods, weather_grid_periods) i HDFS (NDVI),
kolory i progi jak w biuletynie PDF.

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


# ---------------------------------------------------------------- mapy rastrowe: NDVI pikseli i bilans wody na siatce
MAP_CRS = "EPSG:2180"                         # układ granic PRG (metry) — mapy rysujemy w nim
NDVI_DIR = "/rbd/raw/modis_ndvi_poland"
NDVI_COLORS = ["#8C510A", "#D8B365", "#F6E8C3", "#C7E9C0", "#74C476", "#238B45", "#00441B"]   # 0 → 0,9
NO_DATA_GREY = "#9E9E9E"
KBW_BOUNDS = [-200, -150, -100, -50, 0, 50, 100]                                  # klasy mapy bilansu [mm]
KBW_COLORS = ["#7F2704", "#CC4C02", "#F16913", "#FDB863", "#FFF5C0", "#C6E2F0", "#6BAED6", "#2166AC"]


def _target_grid(area, pad: float, res: float):
    """Siatka mapy (EPSG:2180) obejmująca obszar z marginesem -> (transform, (wiersze, kolumny), extent)."""
    from rasterio.transform import from_origin
    minx, miny, maxx, maxy = area.total_bounds
    minx, miny, maxx, maxy = minx - pad, miny - pad, maxx + pad, maxy + pad
    shape = (int(np.ceil((maxy - miny) / res)), int(np.ceil((maxx - minx) / res)))
    return from_origin(minx, maxy, res, res), shape, (minx, minx + shape[1] * res, maxy - shape[0] * res, maxy)


def _inside(area, transform, shape) -> np.ndarray:
    from rasterio.features import rasterize
    return rasterize(((g, 1) for g in area.geometry), out_shape=shape, transform=transform, fill=0,
                     dtype="uint8").astype(bool)


def _finish_rgba(rgba: np.ndarray, inside: np.ndarray, county: bool) -> np.ndarray:
    """Poza obszarem: biało (Polska) albo przygaszone tło sąsiadów (powiat)."""
    if county:
        rgba[~inside, :3] = rgba[~inside, :3] * 0.35 + 0.65
    else:
        rgba[~inside] = (1, 1, 1, 1)
    return rgba


def voivodeships(geo_full):
    """Granice województw z pełnych granic powiatów (sklejone przed uproszczeniem — bez szczelin), do map Polski."""
    v = geo_full.dissolve(by=geo_full["region_id"].str[:2])
    return v.set_geometry(v.geometry.simplify(500))


def _outlines(ax, geo, place, extent, voiv=None):
    """Granice: województwa (Polska) albo sąsiednie powiaty + wybrany powiat (zbliżenie)."""
    if place is None:
        (voiv if voiv is not None else geo).boundary.plot(ax=ax, color="black", lw=0.4)
    else:
        geo.boundary.plot(ax=ax, color="#555555", lw=0.4)
        geo[geo["region_id"] == place[0]].boundary.plot(ax=ax, color="black", lw=1.6)
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    ax.set_axis_off()


def ndvi_map_png(ch, geo, year: int, period: int, place: tuple[str, str] | None = None,
                 voiv=None) -> tuple[bytes | None, str]:
    """NDVI pikseli 250 m z pliku MODIS terminu: cała Polska albo zbliżenie na powiat (geo: granice EPSG:2180)."""
    import rasterio
    from matplotlib.cm import ScalarMappable
    from matplotlib.colors import LinearSegmentedColormap, Normalize, to_rgba
    from matplotlib.patches import Patch
    from rasterio.io import MemoryFile
    from rasterio.warp import Resampling, reproject

    from lib.webhdfs import WebHDFS

    start = date(year, 1, 1) + timedelta(days=16 * period)
    try:
        data = WebHDFS().read_bytes(f"{NDVI_DIR}/{year}/modis_ndvi_poland_{start}.tif")
    except Exception:                                            # brak pliku terminu (np. koniec roku 2024)
        return None, f"Brak zdjęcia NDVI dla terminu {period_label(year, period)}."
    area = (voiv if voiv is not None else geo) if place is None else geo[geo["region_id"] == place[0]]   # maska bez szczelin
    transform, shape, extent = _target_grid(area, 5000 if place else 2000, 250)
    ndvi = np.full(shape, -3000, dtype=np.int16)
    rel = np.full(shape, 255, dtype=np.int16)
    with MemoryFile(data) as mf, mf.open() as src:
        for band, dst, nodata in ((1, ndvi, -3000), (2, rel, 255)):
            reproject(rasterio.band(src, band), dst, dst_transform=transform, dst_crs=MAP_CRS,
                      resampling=Resampling.nearest, src_nodata=nodata, dst_nodata=nodata)
    inside = _inside(area, transform, shape)
    valid = (ndvi >= -2000) & (ndvi <= 10000) & np.isin(rel, (0, 1))   # te same zasady co w obliczeniach B1/B2
    val = ndvi / 10000.0

    cmap, norm = LinearSegmentedColormap.from_list("ndvi", NDVI_COLORS), Normalize(0, 0.9)
    rgba = cmap(norm(np.clip(val, 0, 0.9)))
    rgba[~valid] = to_rgba(NO_DATA_GREY)
    rgba[~valid & (rel == 255)] = (1, 1, 1, 1)                  # poza zdjęciem
    rgba = _finish_rgba(rgba, inside, place is not None)

    plt = _plt()
    fig, ax = plt.subplots(figsize=(7, 6.6) if place is None else (6.4, 6))
    ax.imshow(rgba, extent=extent, interpolation="nearest")
    _outlines(ax, geo, place, extent, voiv)
    fig.colorbar(ScalarMappable(norm=norm, cmap=cmap), ax=ax, shrink=0.55, pad=0.01, label="NDVI")
    ax.legend(handles=[Patch(color=NO_DATA_GREY, label="brak danych (chmura, śnieg)")], fontsize=8,
              loc="lower left", frameon=False)
    where = "Polska" if place is None else place[1]
    ax.set_title(f"NDVI z satelity (MODIS, 250 m): {where}, {period_label(year, period)}", fontsize=10)
    fig.tight_layout(pad=0.2)

    n = int(inside.sum())
    gone = 100 * int((inside & ~valid).sum()) / n if n else 0
    mean = float(val[inside & valid].mean()) if (inside & valid).any() else float("nan")
    text = (f"NDVI {where}, {period_label(year, period)}: średnio {num(mean, 2)} (wszystkie piksele: pola, łąki, "
            f"lasy); bez danych (chmura, śnieg): {num(gone)}%.")
    if place is None:
        st = ch.query_df(f"SELECT status FROM rbd.b1_periods WHERE year = {year} AND period = {period}")["status"]
        if len(st):
            text += (f" Pola orne: gorzej niż zwykle w {int((st == 'gorzej niż zwykle').sum())} z {len(st)} powiatów"
                     " (/mapa).")
    else:
        b1 = ch.query_df(f"SELECT pct_of_norm, status FROM rbd.b1_periods WHERE region_id = '{place[0]}' "
                         f"AND year = {year} AND period = {period}")
        if len(b1) and pd.notna(b1["pct_of_norm"].iloc[0]):
            text += f" Pola orne powiatu: {num(b1['pct_of_norm'].iloc[0])}% normy — {b1['status'].iloc[0]}."
    return _png(fig), text


def kbw_years(ch) -> list[int]:
    try:
        return [int(y) for y in ch.query_df("SELECT DISTINCT year FROM rbd.weather_grid_periods ORDER BY year")["year"]]
    except Exception:                                            # tabela jeszcze nie policzona
        return []


def kbw_map_png(ch, geo, year: int, period: int, place: tuple[str, str] | None = None,
                voiv=None) -> tuple[bytes | None, str]:
    """Bilans wody z 60 dni (opad − ET₀) na siatce ERA5-Land, w klasach jak mapy IMGW: Polska albo powiat."""
    from matplotlib.cm import ScalarMappable
    from matplotlib.colors import BoundaryNorm, ListedColormap
    from rasterio.transform import from_origin
    from rasterio.warp import Resampling, reproject

    df = ch.query_df(f"SELECT lat, lon, kbw_60d_mm, kbw_60d_days FROM rbd.weather_grid_periods "
                     f"WHERE year = {year} AND period = {period}")
    if df.empty:
        return None, f"Brak bilansu wody dla terminu {period_label(year, period)}."
    lat_max, lon_min = float(df["lat"].max()), float(df["lon"].min())
    rows = np.round((lat_max - df["lat"].to_numpy()) / 0.1).astype(int)
    cols = np.round((df["lon"].to_numpy() - lon_min) / 0.1).astype(int)
    src = np.full((rows.max() + 1, cols.max() + 1), np.nan, dtype=np.float32)
    src[rows, cols] = df["kbw_60d_mm"].to_numpy()

    area = (voiv if voiv is not None else geo) if place is None else geo[geo["region_id"] == place[0]]   # maska bez szczelin
    transform, shape, extent = _target_grid(area, 15000 if place else 2000, 1000 if place is None else 250)
    arr = np.full(shape, np.nan, dtype=np.float32)
    reproject(src, arr, src_transform=from_origin(lon_min - 0.05, lat_max + 0.05, 0.1, 0.1), src_crs="EPSG:4326",
              dst_transform=transform, dst_crs=MAP_CRS, resampling=Resampling.bilinear,   # płynnie, jak mapy IMGW
              src_nodata=np.nan, dst_nodata=np.nan)
    inside = _inside(area, transform, shape)

    cmap = ListedColormap(KBW_COLORS)
    norm = BoundaryNorm(KBW_BOUNDS, cmap.N, extend="both")
    rgba = cmap(norm(np.nan_to_num(arr, nan=0)))
    rgba[~np.isfinite(arr)] = (1, 1, 1, 1)
    rgba = _finish_rgba(rgba, inside, place is not None)

    plt = _plt()
    fig, ax = plt.subplots(figsize=(7, 6.6) if place is None else (6.4, 6))
    ax.imshow(rgba, extent=extent, interpolation="bilinear")
    _outlines(ax, geo, place, extent, voiv)
    cb = fig.colorbar(ScalarMappable(norm=norm, cmap=cmap), ax=ax, shrink=0.55, pad=0.01, extend="both",
                      ticks=KBW_BOUNDS, label="bilans wody z 60 dni [mm]")
    cb.ax.axhline(DROUGHT_KBW_60D, color="black", lw=1.2)
    where = "Polska" if place is None else place[1]
    ax.set_title(f"Bilans wody (opad − parowanie) z 60 dni: {where}\nkoniec okresu: {period_label(year, period)}",
                 fontsize=10)
    fig.tight_layout(pad=0.2)

    v = arr[inside & np.isfinite(arr)]
    text = (f"Bilans wody z 60 dni (opad − parowanie ET₀, ERA5-Land ~9 km), {where}, do końca terminu "
            f"{period_label(year, period)}: średnio {num(v.mean())} mm, od {num(v.min())} do {num(v.max())} mm; "
            f"susza (< −150 mm) na {num(100 * (v < DROUGHT_KBW_60D).mean())}% powierzchni.")
    if place is not None:
        w = ch.query_df(f"SELECT kbw_60d_mm FROM rbd.weather_periods WHERE region_id = '{place[0]}' "
                        f"AND year = {year} AND period = {period}")
        if len(w) and pd.notna(w["kbw_60d_mm"].iloc[0]):
            text += f" Wartość powiatu w biuletynie: {num(w['kbw_60d_mm'].iloc[0])} mm."
    days = int(df["kbw_60d_days"].min())
    if days < 60:
        text += f" Uwaga: brak części danych pogodowych — bilans liczony z {days} dni."
    return _png(fig), text
