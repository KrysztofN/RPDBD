"""
Granice powiatów i województw z GUGiK (usługa WFS „PRG — granice administracyjne”), bez konta.

Co robi:
  load_counties()       -> 380 powiatów: region_id (TERYT, 4 cyfry), nazwa, województwo, czy miasto, geometria
  load_voivodeships()   -> 16 województw (do map w biuletynie)
  Wynik zapisujemy lokalnie (data/boundaries/*.gpkg), żeby nie pobierać ~60 MB przy każdym uruchomieniu.

Trochę teorii:
  TERYT to urzędowy rejestr jednostek terytorialnych (GUS). Kod powiatu ma 4 cyfry:
  2 cyfry województwa + 2 cyfry powiatu, np. 3021 = powiat poznański (30 = wielkopolskie).
  Numery powiatu 01–60 to powiaty ziemskie, 61 i wyżej to miasta na prawach powiatu (np. 3064 = Poznań).
  W miastach prawie nie ma pól, więc biuletyn robimy tylko dla powiatów ziemskich (is_city == False).
  PRG (Państwowy Rejestr Granic) jest w układzie EPSG:2180 (metry); zapisujemy też w nim — przeliczenie
  na układ zdjęć satelitarnych robią funkcje, które z granic korzystają.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from .log import get_logger

log = get_logger(__name__)

WFS = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/PRG/WFS/AdministrativeBoundaries"
LAYER_COUNTIES = "ms:A02_Granice_powiatow"
LAYER_VOIVODESHIPS = "ms:A01_Granice_wojewodztw"
LAST_RURAL_CODE = 60          # numery powiatu > 60 = miasta na prawach powiatu


def _wfs_url(layer: str) -> str:
    return f"{WFS}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={layer}"


def _download(layer: str, cache: Path) -> gpd.GeoDataFrame:
    if cache.exists():
        log.debug("%s: wczytuję z %s", layer, cache)
        return gpd.read_file(cache)
    cache.parent.mkdir(parents=True, exist_ok=True)
    log.info("%s: pobieram z GUGiK WFS (jednorazowo, ok. 1 min)", layer)
    gdf = gpd.read_file(_wfs_url(layer))        # jedno zapytanie zwraca całą warstwę
    gdf.to_file(cache, driver="GPKG")
    log.info("%s: %d obiektów -> %s", layer, len(gdf), cache)
    return gdf


def load_voivodeships(data_dir: str | Path = "data") -> gpd.GeoDataFrame:
    """16 województw: region_id (2 cyfry), name, geometry (EPSG:2180)."""
    raw = _download(LAYER_VOIVODESHIPS, Path(data_dir) / "boundaries" / "voivodeships_raw.gpkg")
    out = gpd.GeoDataFrame({"region_id": raw["JPT_KOD_JE"].astype(str),
                            "name": raw["JPT_NAZWA_"].str.strip()},
                           geometry=raw.geometry, crs=raw.crs)
    return out.sort_values("region_id").reset_index(drop=True)


def load_counties(data_dir: str | Path = "data", only_rural: bool = False) -> gpd.GeoDataFrame:
    """
    380 powiatów: region_id (TERYT 4 cyfry), name, voiv_id, voiv_name, is_city, area_km2, geometry (EPSG:2180).
    only_rural=True -> tylko 314 powiatów ziemskich.
    """
    raw = _download(LAYER_COUNTIES, Path(data_dir) / "boundaries" / "counties_raw.gpkg")
    code = raw["JPT_KOD_JE"].astype(str)
    out = gpd.GeoDataFrame({"region_id": code,
                            "name": raw["JPT_NAZWA_"].str.strip(),
                            "voiv_id": code.str[:2],
                            "is_city": code.str[2:4].astype(int) > LAST_RURAL_CODE},
                           geometry=raw.geometry, crs=raw.crs)
    out["area_km2"] = out.geometry.area / 1e6
    voiv = load_voivodeships(data_dir).set_index("region_id")["name"]
    out["voiv_name"] = out["voiv_id"].map(voiv)
    if only_rural:
        out = out[~out["is_city"]]
    return out.sort_values("region_id").reset_index(drop=True)


def export_counties_geojson(out_path: str | Path, data_dir: str | Path = "data", tolerance_m: float = 250) -> Path:
    """
    Granice powiatów jako GeoJSON dla map w przeglądarce (np. Grafana, warstwa Dynamic GeoJSON):
    WGS84 (EPSG:4326), uproszczone (tolerance_m, w metrach — mniejszy plik, szybsza mapa),
    `id` każdego obiektu = kod TERYT (Grafana dopasowuje po nim dane z zapytania), właściwości: nazwa, województwo.
    Uproszczenie z zachowaniem topologii (sąsiednie powiaty bez dziur i zakładek).
    """
    import json

    counties = load_counties(data_dir)
    simplified = counties.geometry.simplify_coverage(tolerance_m) if hasattr(counties.geometry, "simplify_coverage") \
        else counties.geometry.simplify(tolerance_m, preserve_topology=True)
    gdf = counties.set_geometry(simplified).to_crs("EPSG:4326")
    features = []
    for row, geom in zip(gdf.itertuples(), gdf.geometry):
        features.append({"type": "Feature", "id": row.region_id,
                         "properties": {"region_id": row.region_id, "name": row.name, "voiv_name": row.voiv_name},
                         "geometry": json.loads(gpd.GeoSeries([geom]).to_json())["features"][0]["geometry"]})
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"type": "FeatureCollection", "features": features},
                                   ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log.info("GeoJSON powiatów: %d obiektów -> %s (%.1f MB, uproszczenie %.0f m)",
             len(features), out_path, out_path.stat().st_size / 1e6, tolerance_m)
    return out_path


if __name__ == "__main__":
    # python -m lib.boundaries geojson <plik.geojson> [tolerancja_m]
    import sys

    from .log import setup_logging

    setup_logging()
    if len(sys.argv) >= 3 and sys.argv[1] == "geojson":
        export_counties_geojson(sys.argv[2], tolerance_m=float(sys.argv[3]) if len(sys.argv) > 3 else 250)
    else:
        print("użycie: python -m lib.boundaries geojson <plik.geojson> [tolerancja_m]")
