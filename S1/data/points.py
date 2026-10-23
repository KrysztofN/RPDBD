"""Tabela punktów siatki ERA5-Land — gdzie leży każdy punkt i czy są wokół niego pola.

Co robi:
    Dla każdego punktu siatki ERA5-Land (co 0,1°) zapisuje: powiat, województwo,
    udział pól uprawnych w jego kratce i wysokość nad poziomem morza.
    Wynik: S1/data/points.csv (~4 100 wierszy — tylko punkty w Polsce).

Skąd dane (wszystko bez kont):
    - granice powiatów i województw: GUGiK, usługa WFS (Państwowy Rejestr Granic),
    - pola uprawne: mapa ESA WorldCover 2021 (10 m) — lista kafelków z katalogu STAC
      Microsoft Planetary Computer, same rastry z publicznej kopii ESA na AWS S3
      (blob storage Microsoftu jest niedostępny z klastra),
    - wysokość: Open-Meteo Elevation API.

Teoria:
    ERA5-Land podaje pogodę na siatce co 0,1° — punkt reprezentuje kratkę ok. 11 × 7 km
    (0,1° szerokości ≈ 11 km, 0,1° długości w Polsce ≈ 7 km), a wartość to średnia z kratki.
    Proces S1 liczy wynik dla powiatów, więc każdy punkt musi wiedzieć, do którego należy.
    Punkty, gdzie w kratce prawie nie ma pól (las, miasto, jezioro), S1 pomija —
    dlatego liczymy udział pól: część pikseli WorldCover z klasą 40 („pola uprawne”).
    Wysokość jest potrzebna do wzoru FAO-56 na parowanie (ciśnienie powietrza zależy od wysokości).
"""

import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
from rasterio.enums import Resampling

OUT_FILE = Path(__file__).parent / "points.csv"

LAT_MIN, LAT_MAX = 49.0, 55.0
LON_MIN, LON_MAX = 14.0, 24.2
STEP = 0.1
N_LAT = round((LAT_MAX - LAT_MIN) / STEP) + 1
N_LON = round((LON_MAX - LON_MIN) / STEP) + 1

WFS = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/PRG/WFS/AdministrativeBoundaries"
STAC = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
WORLDCOVER_S3 = "https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map"
ELEVATION = "https://api.open-meteo.com/v1/elevation"

CROPLAND_CLASS = 40
WORLDCOVER_REDUCTION = 10

# ustawienia GDAL do czytania COG-ów po HTTP: ponawianie i mniej zbędnych zapytań
GDAL_OPTS = dict(
    GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif",
    GDAL_HTTP_MAX_RETRY=5,
    GDAL_HTTP_RETRY_DELAY=2,
    GDAL_HTTP_MULTIRANGE="YES",
    GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
)


def make_grid():
    lat_idx, lon_idx = np.meshgrid(np.arange(N_LAT), np.arange(N_LON), indexing="ij")
    grid = pd.DataFrame({"lat_idx": lat_idx.ravel(), "lon_idx": lon_idx.ravel()})
    grid["lat"] = (LAT_MIN + grid["lat_idx"] * STEP).round(1)
    grid["lon"] = (LON_MIN + grid["lon_idx"] * STEP).round(1)
    return grid


def read_wfs_layer(layer):
    url = f"{WFS}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={layer}"
    return gpd.read_file(url).to_crs(4326)


def assign_regions(grid):
    # granice: wojewodztwa i powiaty z GUGiK
    voivodeships = read_wfs_layer("ms:A01_Granice_wojewodztw")
    counties = read_wfs_layer("ms:A02_Granice_powiatow")
    voiv_names = dict(zip(voivodeships["JPT_KOD_JE"], voivodeships["JPT_NAZWA_"]))

    points = gpd.GeoDataFrame(grid, geometry=gpd.points_from_xy(grid["lon"], grid["lat"]), crs=4326)
    counties = counties[["JPT_KOD_JE", "JPT_NAZWA_", "geometry"]].rename(
        columns={"JPT_KOD_JE": "county_code", "JPT_NAZWA_": "county_name"}
    )
    joined = gpd.sjoin(points, counties, predicate="within").drop(columns=["index_right", "geometry"])
    joined["voiv_code"] = joined["county_code"].str[:2]
    joined["voiv_name"] = joined["voiv_code"].map(voiv_names)
    return pd.DataFrame(joined)


def open_with_retry(url, tries=5):
    for attempt in range(tries):
        try:
            return rasterio.open(url)
        except rasterio.errors.RasterioIOError as error:
            print(f"  próba {attempt + 1}/{tries} nieudana: {error}")
            if attempt == tries - 1:
                raise
            time.sleep(2 ** attempt)


def cropland_share():
    total = np.zeros(N_LAT * N_LON)
    cropland = np.zeros(N_LAT * N_LON)

    # lista kafelkow z katalogu STAC (samo API Planetary Computer jest osiagalne)
    body = {
        "collections": ["esa-worldcover"],
        "bbox": [LON_MIN, LAT_MIN, LON_MAX, LAT_MAX],
        "datetime": "2021-01-01/2021-12-31",
        "limit": 100,
    }
    response = requests.post(STAC, json=body, timeout=60)
    response.raise_for_status()
    items = response.json()["features"]

    with rasterio.Env(**GDAL_OPTS):
        for item in items:
            # mapa pokrycia terenu: jeden kafelek 3x3 stopnie, czytany w 100 m zamiast 10 m
            print("WorldCover:", item["id"])
            url = f"{WORLDCOVER_S3}/{item['id']}_Map.tif"
            with open_with_retry(url) as src:
                height, width = src.height // WORLDCOVER_REDUCTION, src.width // WORLDCOVER_REDUCTION
                landcover = src.read(1, out_shape=(height, width), resampling=Resampling.nearest)
                transform = src.transform * src.transform.scale(src.width / width, src.height / height)

            pixel_lat = transform.f + (np.arange(height) + 0.5) * transform.e
            pixel_lon = transform.c + (np.arange(width) + 0.5) * transform.a
            lat_idx = np.round((pixel_lat - LAT_MIN) / STEP).astype(int)
            lon_idx = np.round((pixel_lon - LON_MIN) / STEP).astype(int)

            inside = (
                (lat_idx[:, None] >= 0) & (lat_idx[:, None] < N_LAT)
                & (lon_idx[None, :] >= 0) & (lon_idx[None, :] < N_LON)
            )
            valid = inside & (landcover != 0)
            cell = (lat_idx[:, None] * N_LON + lon_idx[None, :])[valid]
            total += np.bincount(cell, minlength=N_LAT * N_LON)
            cropland += np.bincount(cell, weights=(landcover[valid] == CROPLAND_CLASS), minlength=N_LAT * N_LON)

    with np.errstate(invalid="ignore", divide="ignore"):
        return (cropland / total).reshape(N_LAT, N_LON)


def elevation(points):
    heights = []
    for start in range(0, len(points), 100):
        batch = points.iloc[start:start + 100]
        # wysokosc: 100 punktow w jednym zapytaniu, limit 600 punktow na minute
        params = {"latitude": ",".join(map(str, batch["lat"])), "longitude": ",".join(map(str, batch["lon"]))}
        while True:
            response = requests.get(ELEVATION, params=params, timeout=60)
            if response.status_code != 429:
                break
            print("limit Open-Meteo, czekam 60 s")
            time.sleep(60)
        response.raise_for_status()
        heights += response.json()["elevation"]
    return heights


def main():
    points = assign_regions(make_grid())
    print("punktów w Polsce:", len(points))

    share = cropland_share()
    points["cropland_share"] = share[points["lat_idx"], points["lon_idx"]].round(3)
    points["elevation_m"] = elevation(points)

    points = points.sort_values(["lat_idx", "lon_idx"]).reset_index(drop=True)
    points.insert(0, "point_id", points.index + 1)
    columns = ["point_id", "lat", "lon", "voiv_code", "voiv_name", "county_code", "county_name", "cropland_share", "elevation_m"]
    points[columns].to_csv(OUT_FILE, index=False)
    print("zapisano:", OUT_FILE)


if __name__ == "__main__":
    main()