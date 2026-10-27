"""
Pobieranie NDVI MODIS dla Polski: jeden termin (4 kafelki) -> jeden mały plik GeoTIFF z wycinkiem Polski.

Co robi:
  poland_bounds(...)      -> prostokąt Polski (z buforem) w układzie zdjęć MODIS
  clip_date_to_poland()   -> skleja 4 kafelki jednego terminu, wycina Polskę, zapisuje 2 warstwy:
                             1 = NDVI (int16, skala 0.0001, brak = -3000), 2 = jakość piksela (0 dobra, 1 średnia,
                             2 śnieg, 3 chmura, 255 brak) — bez przeliczania, dokładnie jak w produkcie NASA.
  Plik: modis_ndvi_poland_<RRRR-MM-DD>.tif (kompresja deflate). Z tych plików liczą NDVI per region B1, B2, G, H.

Trochę teorii:
  MOD13Q1 to 16-dniowy kompozyt NDVI z satelity Terra (250 m), podzielony na kafelki 10° x 10° w układzie
  sinusoidalnym. Polska leży na 4 kafelkach (h18v03, h18v04, h19v03, h19v04), ale zajmuje tylko część ich
  powierzchni — trzymamy więc tylko prostokąt Polski. Wszystkie kafelki mają tę samą siatkę pikseli, więc
  sklejenie nie zmienia wartości (bez przepróbkowania). Surowe wartości zostawiamy, żeby norma i lata bieżące
  były liczone identycznie w każdym procesie.
"""
from __future__ import annotations

from pathlib import Path
from typing import Sequence

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.warp import transform_bounds

from .log import get_logger
from .ndvi_tools import NDVI_ASSET, NDVI_NODATA, RELIABILITY_ASSET, _GDAL_OPTS, sign_href

log = get_logger(__name__)

RELIABILITY_NODATA = 255


def poland_bounds(area: gpd.GeoDataFrame, crs, buffer_m: float = 2000) -> tuple[float, float, float, float]:
    """Prostokąt obszaru (np. wszystkich powiatów) w układzie `crs`, powiększony o bufor."""
    west, south, east, north = area.to_crs(crs).total_bounds
    return west - buffer_m, south - buffer_m, east + buffer_m, north + buffer_m


def _merge_asset(hrefs: Sequence[str], bounds, nodata) -> tuple[np.ndarray, object, object]:
    with rasterio.Env(**_GDAL_OPTS):
        sources = [rasterio.open(sign_href(h)) for h in hrefs]
        try:
            crs = sources[0].crs
            arr, transform = merge(sources, bounds=bounds, nodata=nodata)
        finally:
            for s in sources:
                s.close()
    return arr[0], transform, crs


def clip_date_to_poland(items: list[dict], area: gpd.GeoDataFrame, out_path: str | Path) -> Path:
    """
    items: wiersze z ndvi_tools.list_modis_items dla JEDNEGO terminu (zwykle 4 kafelki).
    area: obszar do wycięcia (np. powiaty). Zwraca ścieżkę zapisanego pliku.
    """
    out_path = Path(out_path)
    with rasterio.Env(**_GDAL_OPTS), rasterio.open(sign_href(items[0]["ndvi_href"])) as src:
        crs = src.crs
    bounds = poland_bounds(area, crs)
    ndvi, transform, crs = _merge_asset([it["ndvi_href"] for it in items], bounds, NDVI_NODATA)
    rel, _, _ = _merge_asset([it["reliability_href"] for it in items], bounds, RELIABILITY_NODATA)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    profile = dict(driver="GTiff", width=ndvi.shape[1], height=ndvi.shape[0], count=2, dtype="int16",
                   crs=crs, transform=transform, nodata=NDVI_NODATA, compress="deflate", predictor=2,
                   tiled=True, blockxsize=512, blockysize=512)
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(ndvi.astype(np.int16), 1)
        dst.write(rel.astype(np.int16), 2)
        dst.set_band_description(1, NDVI_ASSET)
        dst.set_band_description(2, RELIABILITY_ASSET)
        dst.update_tags(date=str(items[0]["date"].date()), tiles=",".join(sorted(it["tile"] for it in items)))
    log.debug("%s: %d kafelki -> %s (%dx%d px, %.1f MB)", items[0]["date"].date(), len(items), out_path.name,
             ndvi.shape[1], ndvi.shape[0], out_path.stat().st_size / 1e6)
    return out_path
