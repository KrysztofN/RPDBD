"""
Mapa typów upraw EUCROPMAP (JRC, Komisja Europejska) -> udział grup upraw w kwadratach 250 m.

Co robi:
  build_group_fractions(year)  -> czyta mapę 10 m dla Polski prosto z serwera JRC (bez konta, licencja CC BY 4.0),
                                  liczy udział każdej grupy upraw w kwadracie 250 x 250 m i zapisuje mały GeoTIFF
                                  data/crops/eucropmap_<rok>_groups_250m.tif (pełnej mapy 10 m nie zapisujemy).
  crop_structure(...)          -> struktura upraw powiatu (jaki % pól zajmuje każda grupa).
  pure_pixel_counts(...)       -> ile pikseli MODIS w powiecie to „czysta” jedna grupa upraw (do krzywych per uprawa).

Trochę teorii:
  EUCROPMAP powstał z radarowych zdjęć Sentinel-1 (2018) i Sentinel-1 + Sentinel-2 (2022), uczonych na terenowych
  obserwacjach LUCAS. Rozróżnia 19 upraw, ale NIE rozróżnia ozimych i jarych (jest „pszenica”, „jęczmień”).
  Grupy budujemy więc wg tego, jak zwykle sieje się w Polsce: pszenica, żyto i pszenżyto to głównie oziminy,
  jęczmień i owies głównie jare. To przybliżenie — piszemy o tym w biuletynie.
  Piksel NDVI MODIS ma 250 m (ok. 6 ha), a wiele polskich pól jest mniejszych. Dlatego dla każdego piksela MODIS
  liczymy udział grup z mapy 10 m (625 małych pikseli na jeden duży); „czysty” piksel to taki, gdzie jedna grupa
  ma np. >= 75%. Rolnicy co roku zmieniają uprawy (płodozmian), więc przypisanie piksela do uprawy jest ważne
  tylko w roku mapy (2018, 2022); struktura upraw całego powiatu zmienia się wolno.
  Plik źródłowy nie ma wersji pomniejszonych, więc raz trzeba przeczytać całe 10 m (ok. 5,8 mld pikseli na rok);
  czytamy go kawałkami, równolegle, tylko kawałki nachodzące na Polskę.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio import features
from rasterio.transform import Affine
from rasterio.warp import Resampling, reproject
from rasterio.windows import Window, bounds as window_bounds, from_bounds
from shapely.geometry import box
from shapely.prepared import prep

from .log import Progress, get_logger

log = get_logger(__name__)

BASE_URL = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/EUCROPMAP"
FILES = {2018: "2018/EUCROPMAP_2018.tif",
         2022: "2022/EU_CropMap_22_v1_stratum_EU27-HR.tif"}
MAP_CRS = "EPSG:3035"
FACTOR = 25                     # 25 x 10 m = 250 m
_GDAL = dict(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", GDAL_HTTP_MAX_RETRY=5, GDAL_HTTP_RETRY_DELAY=2,
             CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif")

# kod grupy -> (klucz, nazwa po polsku do wykresów i biuletynu, kody klas EUCROPMAP)
GROUPS = {
    1: ("winter_cereals_rapeseed", "zboża ozime i rzepak", (211, 212, 214, 218, 232)),  # pszenica, durum, żyto, pszenżyto, rzepak
    2: ("spring_cereals", "zboża jare", (213, 215, 219)),                        # jęczmień, owies, inne zboża
    3: ("maize", "kukurydza", (216,)),
    4: ("root_crops", "okopowe", (221, 222, 223)),                              # ziemniaki, buraki, inne okopowe
    5: ("grassland", "łąki i pastwiska", (500,)),
    6: ("other_crops", "inne uprawy", (217, 230, 231, 233, 240, 250, 290)),  # strączkowe, pastewne, słonecznik, ugór...
}
NON_CROP = 7                    # zabudowa, lasy, woda, mokradła, goła ziemia (100, 300, 600, 700, 800)
BAND_KEYS = [GROUPS[g][0] for g in sorted(GROUPS)] + ["non_crop"]
ARABLE_GROUPS = (1, 2, 3, 4, 6)  # grupy „pól ornych” (bez łąk) — do struktury upraw


def _lut() -> np.ndarray:
    lut = np.zeros(1024, np.uint8)          # 0 = brak danych
    for g, (_, _, codes) in GROUPS.items():
        lut[list(codes)] = g
    lut[[100, 300, 600, 700, 800]] = NON_CROP
    return lut


LUT = _lut()


def output_path(year: int, data_dir: str | Path = "data") -> Path:
    return Path(data_dir) / "crops" / f"eucropmap_{year}_groups_250m.tif"


def _poland_window(src, area_3035) -> Window:
    """Okno obejmujące obszar, wyrównane do siatki 250 m (wielokrotność 25 pikseli od początku pliku)."""
    w = from_bounds(*area_3035.bounds, transform=src.transform)
    c0 = int(np.floor(w.col_off / FACTOR)) * FACTOR
    r0 = int(np.floor(w.row_off / FACTOR)) * FACTOR
    c1 = int(np.ceil((w.col_off + w.width) / FACTOR)) * FACTOR
    r1 = int(np.ceil((w.row_off + w.height) / FACTOR)) * FACTOR
    return Window(c0, r0, c1 - c0, r1 - r0)


def _reduce_chunk(url: str, win: Window) -> np.ndarray:
    """Kawałek mapy 10 m -> udziały 7 pasm (6 grup + poza uprawami) w kwadratach 250 m."""
    with rasterio.Env(**_GDAL), rasterio.open(url) as src:
        raw = src.read(1, window=win)
    codes = LUT[np.clip(raw, 0, 1023)]
    h, w = codes.shape[0] // FACTOR, codes.shape[1] // FACTOR
    blocks = codes.reshape(h, FACTOR, w, FACTOR)
    return np.stack([(blocks == k).sum(axis=(1, 3)) for k in range(1, NON_CROP + 1)]).astype(np.float32) / FACTOR ** 2


def build_group_fractions(year: int, area: gpd.GeoDataFrame, data_dir: str | Path = "data",
                          chunk: int = 2500, workers: int = 8, overwrite: bool = False) -> Path:
    """
    area: granice obszaru (np. wszystkie powiaty) — czytamy tylko kawałki mapy, które na niego nachodzą.
    Wynik: GeoTIFF EPSG:3035, 250 m, 7 pasm float32 (udział 0..1 w kwadracie), opisy pasm = BAND_KEYS.
    """
    out = output_path(year, data_dir)
    if out.exists() and not overwrite:
        log.info("EUCROPMAP %d: jest już %s — pomijam (od nowa: overwrite=True / --overwrite)", year, out)
        return out
    assert chunk % FACTOR == 0
    url = f"/vsicurl/{BASE_URL}/{FILES[year]}"
    area_3035 = area.to_crs(MAP_CRS).union_all().buffer(1000)
    area_prep = prep(area_3035)

    with rasterio.Env(**_GDAL), rasterio.open(url) as src:
        full = _poland_window(src, area_3035)
        transform = src.window_transform(full) * Affine.scale(FACTOR)
        jobs = []
        for r in range(0, full.height, chunk):
            for c in range(0, full.width, chunk):
                win = Window(full.col_off + c, full.row_off + r,
                             min(chunk, full.width - c), min(chunk, full.height - r))
                if area_prep.intersects(box(*window_bounds(win, src.transform))):
                    jobs.append((r // FACTOR, c // FACTOR, win))

    result = np.zeros((NON_CROP, full.height // FACTOR, full.width // FACTOR), np.float32)
    log.info("EUCROPMAP %d: czytam %d kawałków po %dx%d px (10 m), %d wątków, źródło %s",
             year, len(jobs), chunk, chunk, workers, FILES[year])
    progress = Progress(log, len(jobs), f"EUCROPMAP {year}")
    with ThreadPoolExecutor(workers) as ex:
        futures = {ex.submit(_reduce_chunk, url, win): (r, c, win) for r, c, win in jobs}
        for fut, (r, c, win) in futures.items():
            part = fut.result()
            result[:, r:r + part.shape[1], c:c + part.shape[2]] = part
            progress.update(f"kawałek od kolumny {win.col_off}, wiersza {win.row_off}")

    out.parent.mkdir(parents=True, exist_ok=True)
    profile = dict(driver="GTiff", width=result.shape[2], height=result.shape[1], count=NON_CROP,
                   dtype="float32", crs=MAP_CRS, transform=transform, compress="deflate",
                   predictor=3, tiled=True, blockxsize=256, blockysize=256)
    with rasterio.open(out, "w", **profile) as dst:
        dst.write(result)
        for i, key in enumerate(BAND_KEYS, 1):
            dst.set_band_description(i, key)
        dst.update_tags(source=f"{BASE_URL}/{FILES[year]}", license="CC BY 4.0, (c) European Union", year=year)
    log.info("EUCROPMAP %d: zapisano %s (%dx%d px po 250 m, %.0f MB)", year, out, result.shape[2],
             result.shape[1], out.stat().st_size / 1e6)
    return out


def crop_structure(path: str | Path, regions: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Struktura upraw regionu: <grupa>_km2, arable_km2 (grupy orne, bez łąk) i share_<grupa>_pct (udział w polach ornych).
    Liczone na siatce 250 m mapy (każdy kwadrat = 0,0625 km2).
    """
    with rasterio.open(path) as src:
        frac = src.read()
        idx = features.rasterize(((g, i + 1) for i, g in enumerate(regions.to_crs(src.crs).geometry)),
                                 out_shape=frac.shape[1:], transform=src.transform, fill=0, dtype="int32")
        px_km2 = abs(src.transform.a * src.transform.e) / 1e6
    n = len(regions) + 1
    area = {BAND_KEYS[b]: np.bincount(idx.ravel(), weights=frac[b].ravel(), minlength=n)[1:] * px_km2
            for b in range(NON_CROP)}
    df = pd.DataFrame(area)
    df.insert(0, "region_id", regions["region_id"].to_numpy())
    df["arable_km2"] = df[[GROUPS[g][0] for g in ARABLE_GROUPS]].sum(axis=1)
    for g in ARABLE_GROUPS:
        key = GROUPS[g][0]
        df[f"share_{key}_pct"] = 100 * df[key] / df["arable_km2"]
    return df.rename(columns={GROUPS[g][0]: f"{GROUPS[g][0]}_km2" for g in GROUPS}).drop(columns="non_crop")


def fractions_on_grid(path: str | Path, dst_transform: Affine, dst_crs, dst_shape: tuple[int, int]) -> np.ndarray:
    """Udziały grup (7 pasm) przeliczone na dowolną siatkę, np. kafelek MODIS (średnia ważona powierzchnią)."""
    out = np.zeros((NON_CROP, *dst_shape), np.float32)
    with rasterio.open(path) as src:
        for b in range(NON_CROP):
            reproject(src.read(b + 1), out[b], src_transform=src.transform, src_crs=src.crs,
                      dst_transform=dst_transform, dst_crs=dst_crs, resampling=Resampling.average)
    return out


def pure_pixel_counts(path: str | Path, regions: gpd.GeoDataFrame, modis_grids: list[tuple],
                      thresholds: tuple[float, ...] = (0.5, 0.75)) -> pd.DataFrame:
    """
    Ile pikseli MODIS (250 m) w regionie jest „czystych” dla grupy (udział grupy >= próg).
    modis_grids: lista ([kafelek,] transform, crs, shape) kafelków MODIS, np. z ndvi_tools.modis_tile_grids().
    Wynik (long): region_id, group, threshold, n_pixels.
    """
    n = len(regions) + 1
    counts = {(g, t): np.zeros(n - 1, np.int64) for g in GROUPS for t in thresholds}
    progress = Progress(log, len(modis_grids), "czyste piksele (kafelki MODIS)", every=1.0)
    for *tile, transform, crs, shape in modis_grids:   # działa też z wynikiem nt.modis_tile_grids
        frac = fractions_on_grid(path, transform, crs, shape)
        idx = features.rasterize(((geom, i + 1) for i, geom in enumerate(regions.to_crs(crs).geometry)),
                                 out_shape=shape, transform=transform, fill=0, dtype="int32")
        for g in GROUPS:
            for t in thresholds:
                sel = (frac[g - 1] >= t) & (idx > 0)
                counts[(g, t)] += np.bincount(idx[sel], minlength=n)[1:]
        progress.update(f"kafelek {tile[0] if tile else '?'}")
    rows = [{"region_id": rid, "group": GROUPS[g][0], "threshold": t, "n_pixels": int(c[i])}
            for (g, t), c in counts.items() for i, rid in enumerate(regions["region_id"])]
    return pd.DataFrame(rows)
