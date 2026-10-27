"""
Test klastra Spark: czy praca rozkłada się na workery i czy Spark czyta pliki NDVI z HDFS.

1. 48 drobnych zadań zwraca nazwę maszyny -> ile zadań policzyła która maszyna.
2. Pliki GeoTIFF NDVI z HDFS (binaryFiles) otwierane w rasterio na workerach -> średni NDVI każdego pliku.

Uruchomienie (na masterze, z katalogu z kodem projektu):
  infra/spark/submit.sh infra/spark/smoke_test.py
Wynik 2026-10-27: 48 zadań po równo na 4 workery (12/12/12/12), 23 pliki NDVI z 2018 przeczytane z HDFS.
"""
import socket
from collections import Counter

from pyspark.sql import SparkSession

HDFS_NDVI = "hdfs://10.7.11.21:9000/rbd/raw/modis_ndvi_poland/2018"


def ndvi_mean(path_and_bytes):
    import numpy as np
    from rasterio.io import MemoryFile

    path, data = path_and_bytes
    with MemoryFile(bytes(data)) as mem, mem.open() as src:
        ndvi, rel = src.read(1), src.read(2)
    ok = (ndvi != -3000) & np.isin(rel, (0, 1))
    return path.rsplit("/", 1)[-1], socket.gethostname(), round(float(ndvi[ok].mean()) * 1e-4, 3), int(ok.sum())


spark = SparkSession.builder.appName("rbd-smoke-test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

hosts = sc.parallelize(range(48), 48).map(lambda _: socket.gethostname()).collect()
print("== zadania per maszyna:", dict(sorted(Counter(hosts).items())))

files = sc.binaryFiles(HDFS_NDVI)
rows = files.map(ndvi_mean).collect()
print(f"== pliki NDVI z HDFS ({HDFS_NDVI}): {len(rows)}")
for name, host, mean, n in sorted(rows)[:5]:
    print(f"   {name}: średni NDVI {mean} z {n} pikseli (liczył {host})")
spark.stop()
