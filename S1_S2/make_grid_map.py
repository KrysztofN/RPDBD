"""One-time reference CSV: original ERA5 grid id -> county region_id.
Reads same boundaries as B1/B2; only rural counties (county local TERYT code <=60).
Nearest valid grid-cell fallback for a county with no centre point inside it.
"""
import io
import tempfile
from pathlib import Path
import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from hdfs_io import read_bytes, read_nc, write_bytes
from config import MAP_HDFS

BOUNDARIES = '/rbd/ref/boundaries/counties_raw.gpkg'
SAMPLE = '/rbd/raw/era5_land/2018/era5_land_2018-04_instant.nc'

def main():
    with tempfile.TemporaryDirectory() as tmp:
        file = Path(tmp) / 'counties.gpkg'
        file.write_bytes(read_bytes(BOUNDARIES))
        raw = gpd.read_file(file)
    if 'JPT_KOD_JE' not in raw.columns:
        raise ValueError('Expected JPT_KOD_JE in county boundaries')
    raw['region_id'] = raw['JPT_KOD_JE'].astype(str).str.zfill(4)
    raw = raw[(raw.region_id.str.len() == 4) & (raw.region_id.str[-2:].astype(int) <= 60)].copy()
    counties = raw[['region_id', 'geometry']].to_crs('EPSG:4326')
    ds = read_nc(SAMPLE)
    lat, lon = ds.latitude.values, ds.longitude.values
    ny, nx = len(lat), len(lon)
    lons, lats = np.meshgrid(lon, lat)
    land = np.isfinite(ds.t2m.isel(valid_time=0).values).ravel()
    ids = np.flatnonzero(land)
    points = gpd.GeoDataFrame({'grid_id': ids.astype(int)},
        geometry=gpd.points_from_xy(lons.ravel()[ids], lats.ravel()[ids]), crs='EPSG:4326')
    ds.close()
    found = gpd.sjoin(points, counties, predicate='within', how='inner')
    mapping = found[['grid_id','region_id']].copy()
    # Rural counties without grid centres get nearest land point, as in B1/B2.
    covered = set(mapping.region_id)
    extras = []
    for r in counties.itertuples():
        if r.region_id not in covered:
            c = r.geometry.representative_point()
            near = np.argmin((lons.ravel()[ids]-c.x)**2 + (lats.ravel()[ids]-c.y)**2)
            extras.append((int(ids[near]), r.region_id))
    mapping = pd.concat([mapping, pd.DataFrame(extras, columns=['grid_id','region_id'])], ignore_index=True)
    mapping = mapping.drop_duplicates(['grid_id','region_id']).sort_values(['region_id','grid_id'])
    # Parent directory must exist (created once by existing HDFS admin or provisioned below).
    write_bytes(MAP_HDFS, mapping.to_csv(index=False).encode())
    print('Map saved:', MAP_HDFS, 'pairs=',len(mapping), 'counties=',mapping.region_id.nunique(), 'nearest=',len(extras))

if __name__ == '__main__':
    main()
