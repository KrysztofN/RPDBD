"""
Wspólne funkcje NDVI dla procesów B1, B2, G, H (umieścić jako lib/ndvi_tools.py).

Warstwy:
  1. Dostęp do Planetary Computer: stac_search, sign_href, list_modis_items
  2. Raster -> region: read_modis_ndvi, rasterize_regions, cropland_fraction_on_grid,
     build_tile_grid, process_item, aggregate_tiles, build_regional_ndvi
  3. Szeregi czasowe (B1/B2/G/H): add_time_keys, complete_grid, interpolate_gaps,
     savgol_smooth, smooth_regional_series, prepare_series
  4. Norma i anomalie (B1/H): compute_norm, compare_to_norm, season_aggregate,
     residual_vs_smooth
  5. Cechy dla B2: wide_period_features
  6. Gotowy proces B1: run_b1

Konwencja kolumn (long format): region_id, date, year, doy, period (0..22), ndvi_mean, coverage, ...

Przykład:
    regions = gpd.read_file("wojewodztwa.gpkg")[["region_id", "geometry"]]   # dowolny poziom: woj./powiat
    regional = build_regional_ndvi(regions, 2001, 2024, out_dir="data/ndvi_regional",
                                   cache_dir="data/grids", decimation=1)
    periods, season = run_b1(regional)
"""
from __future__ import annotations

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import parse_qs, urlsplit

import numpy as np
import pandas as pd
import rasterio
import requests
from rasterio import features
from rasterio.errors import WindowError
from rasterio.transform import Affine, array_bounds
from rasterio.warp import Resampling, reproject, transform_bounds
from rasterio.windows import Window, from_bounds
from requests.adapters import HTTPAdapter
from scipy.signal import savgol_filter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------- stałe
STAC = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
SIGN = "https://planetarycomputer.microsoft.com/api/sas/v1/sign"
POLAND = [14.0, 49.0, 24.2, 55.0]

MODIS_COLLECTION = "modis-13Q1-061"
WORLDCOVER_COLLECTION = "esa-worldcover"
NDVI_ASSET = "250m_16_days_NDVI"
RELIABILITY_ASSET = "250m_16_days_pixel_reliability"

NDVI_NODATA = -3000
NDVI_SCALE = 0.0001
RELIABILITY_OK = (0, 1)          # 0 dobra, 1 średnia; 2 śnieg, 3 chmura, 255 brak
CROPLAND_CLASS = 40              # ESA WorldCover: pola uprawne
PERIODS_PER_YEAR = 23            # kompozyty 16-dniowe: DOY 1, 17, ..., 353
NORM_YEARS = range(2001, 2018)   # norma tylko z lat sprzed testowanych susz
LAST_NORM_YEAR = 2017

_GDAL_OPTS = dict(GDAL_HTTP_MAX_RETRY=5, GDAL_HTTP_RETRY_DELAY=2,
                  GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR")
_TILE_RE = re.compile(r"\.(h\d{2}v\d{2})\.")


# --------------------------------------------------------------------------- 1. dostęp do PC
def _make_session() -> requests.Session:
    # 429 obsługuje _request (z Retry-After), więc nie ma go na liście adaptera
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=(500, 502, 503, 504),
                  allowed_methods=None)
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=16))
    return s


_SESSION = _make_session()
_SIGN_CACHE: dict[str, tuple[str, float]] = {}   # kontener -> (query tokenu SAS, wygaśnięcie)
_SIGN_LOCK = threading.Lock()


def _request(method: str, url: str, max_tries: int = 8, **kw) -> requests.Response:
    """Żądanie HTTP z ponawianiem na 429: czeka Retry-After (jeśli jest), inaczej 2, 4, 8... do 60 s."""
    kw.setdefault("timeout", 60)
    for i in range(max_tries):
        r = _SESSION.request(method, url, **kw)
        if r.status_code != 429:
            r.raise_for_status()
            return r
        try:
            wait = float(r.headers.get("Retry-After", ""))
        except ValueError:
            wait = min(60.0, 2.0 ** (i + 1))
        time.sleep(min(wait, 120.0))
    raise RuntimeError(f"HTTP 429 po {max_tries} próbach: {url}")


def _container_key(href: str) -> str:
    u = urlsplit(href)
    return f"{u.scheme}://{u.netloc}/{u.path.lstrip('/').split('/', 1)[0]}"


def sign_href(href: str, safety_s: int = 300) -> str:
    """
    Anonimowo podpisuje link (SAS). Token Planetary Computer jest ważny dla całego kontenera
    (sr=c), więc pobieramy go raz na kontener i doklejamy do kolejnych plików - zamiast
    jednego żądania /sign na plik (to powodowało 429). Token nie-kontenerowy nie jest współdzielony.
    """
    key = _container_key(href)
    with _SIGN_LOCK:     # jedno żądanie naraz: brak lawiny z wielu wątków przy starcie/wygaśnięciu
        hit = _SIGN_CACHE.get(key)
        if hit and hit[1] - time.time() > safety_s:
            return f"{href.split('?')[0]}?{hit[0]}"
        js = _request("GET", SIGN, params={"href": href.split("?")[0]}, timeout=30).json()
        signed = js["href"]
        query = urlsplit(signed).query
        try:
            expiry = pd.Timestamp(js["msft:expiry"]).timestamp()
        except Exception:
            expiry = time.time() + 1800
        if parse_qs(query).get("sr") == ["c"]:
            _SIGN_CACHE[key] = (query, expiry)
        return signed


def stac_search(collection: str, datetime: str, bbox: Sequence[float] = POLAND, limit: int = 100) -> list[dict]:
    """Wyszukiwanie STAC z paginacją (notebook 02 nie paginuje - limit 50 ucina wyniki)."""
    body = {"collections": [collection], "bbox": list(bbox), "datetime": datetime, "limit": limit}
    url, out = STAC, []
    while url:
        js = _request("POST", url, json=body).json()
        out.extend(js["features"])
        nxt = next((l for l in js.get("links", []) if l.get("rel") == "next"), None)
        if nxt and str(nxt.get("method", "GET")).upper() == "POST":
            url, body = nxt["href"], {**body, **nxt.get("body", {})}
        else:
            url = None
    return out


def add_time_keys(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Dodaje year, doy, period (indeks kompozytu 16-dniowego 0..22)."""
    out = df.copy()
    d = pd.to_datetime(out[date_col])
    out["year"] = d.dt.year
    out["doy"] = d.dt.dayofyear
    out["period"] = (out["doy"] - 1) // 16
    return out


def list_modis_items(start_year: int, end_year: int, bbox: Sequence[float] = POLAND) -> pd.DataFrame:
    """Katalog kafelków MOD13Q1 (tylko Terra - jeden produkt na całe 2001-2024)."""
    rows = []
    for year in range(start_year, end_year + 1):
        for it in stac_search(MODIS_COLLECTION, f"{year}-01-01/{year}-12-31", bbox):
            if not it["id"].startswith("MOD13Q1"):
                continue
            tile = _TILE_RE.search(it["id"]).group(1)
            date = pd.to_datetime(it["properties"]["start_datetime"], utc=True).tz_localize(None).normalize()
            rows.append(dict(id=it["id"], tile=tile, date=date,
                             ndvi_href=it["assets"][NDVI_ASSET]["href"],
                             reliability_href=it["assets"][RELIABILITY_ASSET]["href"]))
    df = pd.DataFrame(rows).drop_duplicates(["tile", "date"])
    df = df[(df["date"].dt.year >= start_year) & (df["date"].dt.year <= end_year)]
    return add_time_keys(df).sort_values(["date", "tile"]).reset_index(drop=True)


# --------------------------------------------------------------------------- 2. raster -> region
def read_modis_ndvi(ndvi_href: str, reliability_href: str, decimation: int = 1,
                    ok_reliability: Iterable[int] = RELIABILITY_OK):
    """
    Czyta kafelek: zwraca (ndvi float32 z NaN, transform, crs).
    NaN: brak danych, poza zakresem [-0.2, 1], jakość piksela spoza ok_reliability.
    NDVI i reliability czytane tym samym out_shape + nearest, więc siatki się pokrywają.
    decimation>1 = szybki podgląd/testy (n-ty piksel), produkcyjnie 1.
    """
    with rasterio.Env(**_GDAL_OPTS):
        with rasterio.open(sign_href(ndvi_href)) as src:
            out_shape = (src.height // decimation, src.width // decimation)
            raw = src.read(1, out_shape=out_shape)
            transform = src.transform * Affine.scale(src.width / out_shape[1], src.height / out_shape[0])
            crs = src.crs
        with rasterio.open(sign_href(reliability_href)) as src:
            rel = src.read(1, out_shape=out_shape)
    valid = (raw != NDVI_NODATA) & (raw >= -2000) & (raw <= 10000) & np.isin(rel, list(ok_reliability))
    ndvi = np.where(valid, raw.astype(np.float32) * np.float32(NDVI_SCALE), np.nan).astype(np.float32)
    return ndvi, transform, crs


def rasterize_regions(regions, crs, transform: Affine, shape: tuple[int, int]) -> np.ndarray:
    """GeoDataFrame -> raster int16 (0 = poza regionami, i+1 = wiersz i w `regions`)."""
    g = regions.reset_index(drop=True).to_crs(crs.to_wkt() if hasattr(crs, "to_wkt") else crs)
    shapes = ((geom, i + 1) for i, geom in enumerate(g.geometry) if geom is not None and not geom.is_empty)
    return features.rasterize(shapes, out_shape=shape, transform=transform, fill=0, dtype="int16")


def cropland_fraction_on_grid(transform: Affine, crs, shape: tuple[int, int], year: int = 2021,
                              limit_bbox: Sequence[float] | None = None) -> np.ndarray:
    """
    Ułamek pól uprawnych (WorldCover klasa 40) w pikselu siatki MODIS (float32 0..1).
    WorldCover czytany z dezagregacją 'mode' do rozdzielczości docelowej, potem reproject(average).
    limit_bbox (lon/lat) ogranicza odczyt do obszaru regionów.
    """
    west, south, east, north = array_bounds(shape[0], shape[1], transform)
    bbox = list(transform_bounds(crs, "EPSG:4326", west, south, east, north, densify_pts=21))
    if limit_bbox is not None:
        bbox = [max(bbox[0], limit_bbox[0]), max(bbox[1], limit_bbox[1]),
                min(bbox[2], limit_bbox[2]), min(bbox[3], limit_bbox[3])]
        if bbox[0] >= bbox[2] or bbox[1] >= bbox[3]:
            return np.zeros(shape, np.float32)
    factor = max(1, int(round(abs(transform.a) / 10)))   # 10 m -> rozdzielczość MODIS
    frac = np.zeros(shape, np.float32)
    for it in stac_search(WORLDCOVER_COLLECTION, f"{year}-01-01/{year}-12-31", bbox):
        with rasterio.Env(**_GDAL_OPTS), rasterio.open(sign_href(it["assets"]["map"]["href"])) as src:
            win = from_bounds(*bbox, transform=src.transform)
            try:
                win = win.intersection(Window(0, 0, src.width, src.height))
            except WindowError:
                continue
            win = Window(int(np.floor(win.col_off)), int(np.floor(win.row_off)),
                         int(np.ceil(win.width)), int(np.ceil(win.height)))
            out_h, out_w = max(1, win.height // factor), max(1, win.width // factor)
            arr = src.read(1, window=win, out_shape=(out_h, out_w), resampling=Resampling.mode)
            t = src.window_transform(win) * Affine.scale(win.width / out_w, win.height / out_h)
            tmp = np.zeros(shape, np.float32)
            reproject((arr == CROPLAND_CLASS).astype(np.float32), tmp, src_transform=t, src_crs=src.crs,
                      dst_transform=transform, dst_crs=crs, resampling=Resampling.average)
            frac = np.maximum(frac, tmp)
    return frac


@dataclass
class TileGrid:
    """Statyczne dane kafelka (ta sama siatka dla wszystkich dat) - liczone raz."""
    tile: str
    crs: object
    transform: Affine
    shape: tuple[int, int]
    region_idx: np.ndarray    # int16, 0 = poza regionami
    crop_mask: np.ndarray     # bool
    region_ids: np.ndarray    # region_id w kolejności wierszy `regions`
    n_total: np.ndarray       # liczba pikseli maski w regionie (ten kafelek), wyrównane z region_ids


def build_tile_grid(item: dict, regions, decimation: int = 1, use_cropland: bool = True,
                    crop_threshold: float = 0.5, worldcover_year: int = 2021,
                    cache_dir: str | Path | None = None, cache_tag: str = "regions") -> TileGrid:
    """
    item: wiersz z list_modis_items (dict). regions: GeoDataFrame z kolumną region_id.
    cache_tag: zmienić przy zmianie regionów (np. 'woj', 'powiaty') - inaczej cache będzie nieaktualny.
    """
    regions = regions.reset_index(drop=True)
    with rasterio.Env(**_GDAL_OPTS), rasterio.open(sign_href(item["ndvi_href"])) as src:
        shape = (src.height // decimation, src.width // decimation)
        transform = src.transform * Affine.scale(src.width / shape[1], src.height / shape[0])
        crs = src.crs

    cache = None
    if cache_dir:
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        cache = Path(cache_dir) / f"grid_{item['tile']}_{cache_tag}_d{decimation}_c{int(use_cropland)}.npz"
    if cache and cache.exists():
        z = np.load(cache)
        region_idx, crop_mask = z["region_idx"], z["crop_mask"]
    else:
        region_idx = rasterize_regions(regions, crs, transform, shape)
        if use_cropland:
            lim = regions.to_crs("EPSG:4326").total_bounds
            frac = cropland_fraction_on_grid(transform, crs, shape, worldcover_year, limit_bbox=lim)
            crop_mask = frac >= crop_threshold
        else:
            crop_mask = np.ones(shape, bool)
        if cache:
            np.savez_compressed(cache, region_idx=region_idx, crop_mask=crop_mask)

    sel = crop_mask & (region_idx > 0)
    n_total = np.bincount(region_idx[sel], minlength=len(regions) + 1)[1:]
    return TileGrid(item["tile"], crs, transform, shape, region_idx, crop_mask,
                    regions["region_id"].to_numpy(), n_total)


def zonal_sum_count(ndvi: np.ndarray, grid: TileGrid) -> tuple[np.ndarray, np.ndarray]:
    """Suma i liczba ważnych pikseli NDVI per region (indeks 0..n_regions-1)."""
    valid = np.isfinite(ndvi) & grid.crop_mask & (grid.region_idx > 0)
    ids = grid.region_idx[valid]
    n = len(grid.region_ids) + 1
    s = np.bincount(ids, weights=ndvi[valid].astype(np.float64), minlength=n)[1:]
    c = np.bincount(ids, minlength=n)[1:]
    return s, c


def process_item(item: dict, grid: TileGrid, decimation: int = 1) -> pd.DataFrame:
    """Jeden kafelek x jeden termin -> sumy cząstkowe (region ma zwykle kilka kafelków: sumujemy, nie uśredniamy średnich)."""
    ndvi, _, _ = read_modis_ndvi(item["ndvi_href"], item["reliability_href"], decimation)
    s, c = zonal_sum_count(ndvi, grid)
    keep = c > 0
    return pd.DataFrame({"region_id": grid.region_ids[keep], "tile": item["tile"], "date": item["date"],
                         "ndvi_sum": s[keep], "n_valid": c[keep]})


def aggregate_tiles(parts: pd.DataFrame, n_total: pd.Series, min_coverage: float = 0.1) -> pd.DataFrame:
    """
    Łączy kafelki: ndvi_mean = suma/liczba ważnych pikseli; coverage = n_valid / n_total (cały region).
    ndvi_mean = NaN gdy coverage < min_coverage (chmury, brakujący kafelek) - luki uzupełni interpolate_gaps.
    n_total: Series (index=region_id) - suma n_total po wszystkich kafelkach.
    """
    g = parts.groupby(["region_id", "date"], as_index=False)[["ndvi_sum", "n_valid"]].sum()
    g["n_total"] = g["region_id"].map(n_total)
    g["coverage"] = g["n_valid"] / g["n_total"]
    g["ndvi_mean"] = np.where(g["coverage"] >= min_coverage, g["ndvi_sum"] / g["n_valid"], np.nan)
    return add_time_keys(g)


def build_regional_ndvi(regions, start_year: int, end_year: int, id_col: str = "region_id",
                        decimation: int = 1, use_cropland: bool = True, out_dir: str | Path | None = None,
                        cache_dir: str | Path | None = None, max_workers: int = 4,
                        min_coverage: float = 0.1, **grid_kw) -> pd.DataFrame:
    """
    Pełny pipeline: MODIS -> średni NDVI pól per region i termin.
    out_dir: zapis parquet per rok (wznawianie - istniejące lata pomijane; ~100 GB danych, więc przydatne).
    Wynik: region_id, date, ndvi_sum, n_valid, n_total, coverage, ndvi_mean, year, doy, period.
    """
    geom = regions.geometry.name
    regions = regions.rename(columns={id_col: "region_id"}).reset_index(drop=True)[["region_id", geom]]
    items = list_modis_items(start_year, end_year)

    grids = {tile: build_tile_grid(g.iloc[0].to_dict(), regions, decimation, use_cropland, cache_dir=cache_dir, **grid_kw)
             for tile, g in items.groupby("tile")}
    n_total = (pd.concat([pd.Series(g.n_total, index=g.region_ids) for g in grids.values()])
               .groupby(level=0).sum())

    out_dir = Path(out_dir) if out_dir else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    for year, yitems in items.groupby("year"):
        path = out_dir / f"ndvi_regional_{year}.parquet" if out_dir else None
        if path and path.exists():
            frames.append(pd.read_parquet(path))
            continue
        rows = yitems.to_dict("records")
        with ThreadPoolExecutor(max_workers) as ex:
            parts = list(ex.map(lambda r: process_item(r, grids[r["tile"]], decimation), rows))
        year_df = aggregate_tiles(pd.concat(parts, ignore_index=True), n_total, min_coverage)
        if path:
            year_df.to_parquet(path, index=False)
        frames.append(year_df)
    return pd.concat(frames, ignore_index=True).sort_values(["region_id", "date"]).reset_index(drop=True)


# --------------------------------------------------------------------------- 3. szeregi czasowe
def complete_grid(df: pd.DataFrame, region_ids: Iterable, years: Iterable[int]) -> pd.DataFrame:
    """Pełna siatka region x rok x period (brakujące terminy -> NaN) + kolumna date."""
    keys = ["region_id", "year", "period"]
    idx = pd.MultiIndex.from_product([list(region_ids), list(years), range(PERIODS_PER_YEAR)], names=keys)
    out = df.drop_duplicates(keys).set_index(keys).reindex(idx).reset_index()
    out["date"] = (pd.to_datetime(out["year"].astype(str) + "-01-01")
                   + pd.to_timedelta(out["period"] * 16, unit="D"))
    out["doy"] = out["period"] * 16 + 1
    return out


def _runs(mask: np.ndarray):
    """Zwraca (start, end) kolejnych przedziałów True (end wyłącznie)."""
    m = np.concatenate([[False], mask, [False]])
    d = np.diff(m.astype(np.int8))
    return list(zip(np.flatnonzero(d == 1), np.flatnonzero(d == -1)))


def interpolate_gaps(y: np.ndarray, max_gap: int = 3) -> np.ndarray:
    """Liniowe uzupełnianie luk do `max_gap` kolejnych terminów (3 = 48 dni); dłuższe zostają NaN.
    Luki na brzegach: wartość skrajna (extrapolacja stała), też tylko do max_gap."""
    y = np.asarray(y, dtype=float)
    out = y.copy()
    nan = np.isnan(y)
    if nan.all() or not nan.any():
        return out
    idx = np.arange(len(y))
    interp = np.interp(idx, idx[~nan], y[~nan])
    for s, e in _runs(nan):
        if e - s <= max_gap:
            out[s:e] = interp[s:e]
    return out


def savgol_smooth(y: np.ndarray, window: int = 5, polyorder: int = 2) -> np.ndarray:
    """Savitzky-Golay na ciągłych odcinkach bez NaN; odcinki krótsze niż okno zostają bez zmian."""
    y = np.asarray(y, dtype=float)
    out = y.copy()
    for s, e in _runs(~np.isnan(y)):
        if e - s >= window:
            out[s:e] = savgol_filter(y[s:e], window, polyorder)
    return out


def smooth_regional_series(df: pd.DataFrame, value_col: str = "ndvi_mean", max_gap: int = 3,
                           window: int = 5, polyorder: int = 2,
                           group_cols: Sequence[str] = ("region_id", "year")) -> pd.DataFrame:
    """Dodaje ndvi_filled (po uzupełnieniu luk) i ndvi_smooth (po SG), osobno dla każdej grupy.
    Dane muszą mieć pełną siatkę terminów (complete_grid) i być posortowane po (region_id, year, period)."""
    df = df.sort_values(["region_id", "year", "period"]).copy()
    gcols = list(group_cols)
    df["ndvi_filled"] = df.groupby(gcols)[value_col].transform(
        lambda s: pd.Series(interpolate_gaps(s.to_numpy(), max_gap), index=s.index))
    df["ndvi_smooth"] = df.groupby(gcols)["ndvi_filled"].transform(
        lambda s: pd.Series(savgol_smooth(s.to_numpy(), window, polyorder), index=s.index))
    return df


def prepare_series(regional: pd.DataFrame, years: Iterable[int] | None = None, value_col: str = "ndvi_mean",
                   **smooth_kw) -> pd.DataFrame:
    """regional (z build_regional_ndvi / parquet) -> pełna siatka + luki uzupełnione + wygładzenie.
    Wspólny punkt wejścia dla B1, B2, G i H."""
    regional = add_time_keys(regional) if "period" not in regional else regional
    years = sorted(regional["year"].unique()) if years is None else list(years)
    grid = complete_grid(regional[["region_id", "year", "period", value_col]
                                  + [c for c in ("coverage",) if c in regional]],
                         regional["region_id"].unique(), years)
    return smooth_regional_series(grid, value_col, **smooth_kw)


# --------------------------------------------------------------------------- 4. norma i anomalie
def compute_norm(df: pd.DataFrame, value_col: str = "ndvi_smooth", years: Iterable[int] = NORM_YEARS,
                 by: Sequence[str] = ("region_id", "period"), allow_test_years: bool = False) -> pd.DataFrame:
    """Statystyki normy (mean/median/std/p25/p75/n) w grupach `by`.
    Domyślnie 2001-2017; lata >2017 zablokowane (testowane susze 2018-2019), chyba że allow_test_years=True.
    by=('region_id','period') -> norma per termin (B1, H1); by=('region_id',) -> norma sezonowa (H2)."""
    years = list(years)
    if not allow_test_years and max(years) > LAST_NORM_YEAR:
        raise ValueError(f"Norma nie może zawierać lat > {LAST_NORM_YEAR} (allow_test_years=True, by pominąć).")
    sub = df[df["year"].isin(years)]
    if sub.empty:
        raise ValueError("Brak danych w latach normy.")
    return (sub.groupby(list(by))[value_col]
            .agg(norm_mean="mean", norm_median="median", norm_std="std",
                 norm_p25=lambda s: s.quantile(0.25), norm_p75=lambda s: s.quantile(0.75),
                 norm_n="count")
            .reset_index())


def compare_to_norm(df: pd.DataFrame, norm: pd.DataFrame, value_col: str = "ndvi_smooth",
                    by: Sequence[str] = ("region_id", "period"), z_threshold: float = 1.0,
                    min_std: float = 0.02) -> pd.DataFrame:
    """Dodaje: diff, pct_of_norm, z (diff/std, std >= min_std), status
    ('powyżej normy' | 'w normie' | 'poniżej normy' | 'brak danych')."""
    out = df.merge(norm, on=list(by), how="left")
    out["diff"] = out[value_col] - out["norm_mean"]
    out["pct_of_norm"] = 100 * out[value_col] / out["norm_mean"]
    out["z"] = out["diff"] / out["norm_std"].clip(lower=min_std)
    out["status"] = np.select([out["z"].isna(), out["z"] >= z_threshold, out["z"] <= -z_threshold],
                              ["brak danych", "powyżej normy", "poniżej normy"], "w normie")
    return out


def season_aggregate(df: pd.DataFrame, value_col: str = "ndvi_smooth", months: tuple[int, int] = (4, 9),
                     by: Sequence[str] = ("region_id", "year")) -> pd.DataFrame:
    """Agregat sezonowy (domyślnie IV-IX): season_mean, season_max, season_n.
    Wejście do B1 (ocena sezonu), H2 (sezon vs mediana 2001-2017) i B2 (cechy)."""
    m = df["date"].dt.month
    sel = df[(m >= months[0]) & (m <= months[1])]
    return (sel.groupby(list(by))[value_col]
            .agg(season_mean="mean", season_max="max", season_n="count").reset_index())


def residual_vs_smooth(df: pd.DataFrame, raw_col: str = "ndvi_mean", smooth_col: str = "ndvi_smooth",
                       drop_threshold: float = 0.1) -> pd.DataFrame:
    """H1: reszta = surowy NDVI - krzywa wygładzona (G); sudden_drop gdy reszta <= -drop_threshold."""
    out = df.copy()
    out["ndvi_resid"] = out[raw_col] - out[smooth_col]
    out["sudden_drop"] = out["ndvi_resid"] <= -drop_threshold
    return out


# --------------------------------------------------------------------------- 5. cechy dla B2
def wide_period_features(df: pd.DataFrame, value_col: str = "ndvi_smooth",
                         periods: Iterable[int] = range(5, 16)) -> pd.DataFrame:
    """NDVI w terminach 5..15 (ok. 25.III-6.IX) jako kolumny ndvi_p05... -> (region_id, year) do łączenia z GUS."""
    w = (df[df["period"].isin(list(periods))]
         .pivot_table(index=["region_id", "year"], columns="period", values=value_col))
    w.columns = [f"ndvi_p{p:02d}" for p in w.columns]
    return w.reset_index()


# --------------------------------------------------------------------------- 6. proces B1
def run_b1(regional: pd.DataFrame, years: Iterable[int] | None = None, norm_years: Iterable[int] = NORM_YEARS,
           season_months: tuple[int, int] = (4, 9), z_threshold: float = 1.0,
           **smooth_kw) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    B1: stan upraw względem normy 2001-2017.
    Zwraca (periods, season):
      periods - region x rok x termin: ndvi_smooth, norm_*, diff, pct_of_norm, z, status
      season  - region x rok: season_mean/max, norm sezonowa, diff, pct_of_norm, z, status
    """
    series = prepare_series(regional, years, **smooth_kw)
    norm_p = compute_norm(series, "ndvi_smooth", norm_years, by=("region_id", "period"))
    periods = compare_to_norm(series, norm_p, "ndvi_smooth", ("region_id", "period"), z_threshold)

    season = season_aggregate(series, "ndvi_smooth", season_months)
    norm_s = compute_norm(season, "season_mean", norm_years, by=("region_id",))
    season = compare_to_norm(season, norm_s, "season_mean", ("region_id",), z_threshold)
    return periods, season
