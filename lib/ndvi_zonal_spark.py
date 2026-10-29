"""
Spark: średni NDVI pól per powiat i termin, z plików NDVI na HDFS -> jedna tabela Parquet na HDFS.
Krok wspólny zespołu (B1, B2, G, H) — stąd lib/.

Co robi:
  1. driver czyta z HDFS granice powiatów i mapy upraw EUCROPMAP (250 m) i nakłada je na siatkę zdjęć NDVI
     (wszystkie zdjęcia mają identyczną siatkę — wycinek Polski z lib/modis_ingest.py), tworząc „mapę
     przynależności”: powiat piksela, czy piksel jest polem ornym, czy jest „czystą” grupą upraw;
  2. rozsyła ją do workerów (broadcast) — liczona raz, nie dla każdego zdjęcia;
  3. każde zdjęcie = osobne zadanie: odrzuca chmury/śnieg (jakość piksela 0 i 1 zostaje), sumuje NDVI per powiat;
  4. zapisuje Parquet (podział na lata): <hdfs>/rbd/clean/ndvi_counties/year=<rok>/
       region_id, date, crop_class, ndvi_sum, n_valid, n_total, coverage, ndvi_mean, doy, period

Klasy pikseli (crop_class):
  arable                       pole orne: udział grup ornych (bez łąk) w pikselu >= --arable-min (0.5);
                               maska z EUCROPMAP 2018 dla lat <= 2020, z 2022 dla lat >= 2021
  <grupa> (np. maize)          piksel z udziałem jednej grupy >= --pure (0.75) — tylko w latach map (2018, 2022),
                               bo uprawy co roku się zmieniają (płodozmian)

Trochę teorii:
  Średnia regionu = suma NDVI / liczba ważnych pikseli (nie średnia ze średnich), więc chmury w części powiatu
  nie zniekształcają wyniku. coverage = ważne piksele / wszystkie piksele klasy w powiecie; przy coverage
  < --min-coverage ndvi_mean = NaN (luka uzupełniana później przy wygładzaniu krzywej).
  Ciężka część (549 zdjęć, ok. 5 mld pikseli) jest równoległa: jedno zdjęcie = jedno zadanie na workerze.

Uruchomienie (na masterze, z katalogu z kodem; submit.sh dołącza lib/ dla workerów):
  infra/spark/submit.sh lib/ndvi_zonal_spark.py --years 2018 2018      # test: jeden rok
  infra/spark/submit.sh lib/ndvi_zonal_spark.py --years 2001 2024      # całość
"""
from __future__ import annotations

import argparse
import re
import tempfile
from pathlib import Path

import numpy as np

from lib.log import Timer, get_logger, setup_logging

log = get_logger(Path(__file__).stem)

HDFS = "hdfs://10.7.11.21:9000"
NDVI_DIR = "/rbd/raw/modis_ndvi_poland"
REF_DIR = "/rbd/ref"
OUT_DIR = "/rbd/clean/ndvi_counties"
MAP_YEARS = (2018, 2022)
NDVI_NODATA = -3000
RELIABILITY_OK = (0, 1)
_DATE_RE = re.compile(r"modis_ndvi_poland_(\d{4}-\d{2}-\d{2})\.tif$")


def map_year_for(year: int) -> int:
    """Która mapa upraw dla danego roku: 2018 dla lat <= 2020, 2022 dla późniejszych."""
    return 2018 if year <= 2020 else 2022


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Spark: NDVI pól per powiat i termin (HDFS -> Parquet)")
    p.add_argument("--years", type=int, nargs=2, metavar=("OD", "DO"), required=True)
    p.add_argument("--hdfs", default=HDFS)
    p.add_argument("--ndvi-dir", default=NDVI_DIR)
    p.add_argument("--ref-dir", default=REF_DIR)
    p.add_argument("--out", default=OUT_DIR)
    p.add_argument("--arable-min", type=float, default=0.5, help="min. udział pól ornych w pikselu")
    p.add_argument("--pure", type=float, default=0.75, help="min. udział grupy, by piksel był „czysty”")
    p.add_argument("--min-coverage", type=float, default=0.1, help="poniżej: ndvi_mean = NaN")
    p.add_argument("--partitions", type=int, default=96, help="na ile zadań podzielić zdjęcia")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args()


def _bytes_to_file(data: bytes, folder: str, name: str) -> Path:
    path = Path(folder) / name
    path.write_bytes(bytes(data))
    return path


def zones_from_files(counties_gpkg: str | Path, crop_tifs: dict[int, str | Path], grid_tif: str | Path,
                     arable_min: float = 0.5, pure: float = 0.75) -> dict:
    """
    Mapa przynależności na siatce NDVI, z plików lokalnych (bez Sparka — da się testować na laptopie):
      region_idx  int16  0 = poza powiatami, i+1 = powiat region_ids[i]
      arable[my]  bool   pole orne wg mapy roku my
      group[my]   uint8  0 = brak czystej grupy, g = kod grupy z eucropmap.GROUPS
      n_total     {(my, klasa): liczba pikseli klasy per powiat} — do coverage
    crop_tifs: {rok_mapy: eucropmap_<rok>_groups_250m.tif}, grid_tif: dowolne zdjęcie NDVI (siatka).
    """
    import geopandas as gpd
    import rasterio

    from lib import eucropmap as ec
    from lib import ndvi_tools as nt

    with rasterio.open(grid_tif) as src:
        transform, crs, shape = src.transform, src.crs, (src.height, src.width)
    log.info("siatka NDVI: %dx%d px", shape[1], shape[0])

    # wszystkie 380 powiatów (dane wspólne zespołu); miasta na prawach powiatu odfiltrowuje odbiorca
    raw = gpd.read_file(counties_gpkg)
    counties = gpd.GeoDataFrame({"region_id": raw["JPT_KOD_JE"].astype(str)}, geometry=raw.geometry, crs=raw.crs)
    counties = counties.sort_values("region_id").reset_index(drop=True)
    region_idx = nt.rasterize_regions(counties, crs, transform, shape)
    log.info("powiaty: %d, piksele w powiatach: %d", len(counties), int((region_idx > 0).sum()))

    arable, group = {}, {}
    for my, tif in crop_tifs.items():
        frac = ec.fractions_on_grid(tif, transform, crs, shape)
        arable[my] = frac[[g - 1 for g in ec.ARABLE_GROUPS]].sum(axis=0) >= arable_min
        best = frac[: len(ec.GROUPS)].argmax(axis=0)
        group[my] = np.where(frac[: len(ec.GROUPS)].max(axis=0) >= pure, best + 1, 0).astype(np.uint8)
        log.info("mapa %d: pól ornych %d px, czystych pikseli grup %d px", my,
                 int((arable[my] & (region_idx > 0)).sum()), int(((group[my] > 0) & (region_idx > 0)).sum()))

    n = len(counties) + 1
    n_total = {}
    for my in crop_tifs:
        n_total[(my, "arable")] = np.bincount(region_idx[arable[my]], minlength=n)[1:]
        for g, (key, _, _) in ec.GROUPS.items():
            n_total[(my, key)] = np.bincount(region_idx[group[my] == g], minlength=n)[1:]
    return dict(region_idx=region_idx, arable=arable, group=group, n_total=n_total,
                region_ids=counties["region_id"].tolist(),
                group_keys={g: key for g, (key, _, _) in ec.GROUPS.items()})


def build_zones(sc, args) -> dict:
    """Pobiera z HDFS (przez Sparka) granice, mapy upraw i jedno zdjęcie NDVI, potem zones_from_files."""
    base = args.hdfs
    with tempfile.TemporaryDirectory() as tmp:
        refs = dict(sc.binaryFiles(
            f"{base}{args.ref_dir}/boundaries/counties_raw.gpkg,"
            + ",".join(f"{base}{args.ref_dir}/crops/eucropmap_{y}_groups_250m.tif" for y in MAP_YEARS)).collect())
        local = {Path(k).name: _bytes_to_file(v, tmp, Path(k).name) for k, v in refs.items()}
        first = sc.binaryFiles(f"{base}{args.ndvi_dir}/{args.years[0]}/*.tif").first()
        grid = _bytes_to_file(first[1], tmp, "grid.tif")
        return zones_from_files(local["counties_raw.gpkg"],
                                {y: local[f"eucropmap_{y}_groups_250m.tif"] for y in MAP_YEARS},
                                grid, args.arable_min, args.pure)


def zonal_rows(path_and_bytes, zones_bc):
    """Jedno zdjęcie -> wiersze (region_id, date, crop_class, ndvi_sum, n_valid). Działa na workerze."""
    from rasterio.io import MemoryFile

    path, data = path_and_bytes
    m = _DATE_RE.search(path)
    if not m:
        return []
    date = m.group(1)
    year = int(date[:4])
    z = zones_bc.value
    my = map_year_for(year)

    with MemoryFile(bytes(data)) as mem, mem.open() as src:
        raw, rel = src.read(1), src.read(2)
    valid = (raw != NDVI_NODATA) & (raw >= -2000) & (raw <= 10000) & np.isin(rel, RELIABILITY_OK)
    ndvi = raw.astype(np.float64) * 1e-4
    idx = z["region_idx"]
    n = len(z["region_ids"]) + 1

    masks = {"arable": z["arable"][my]}
    if year in MAP_YEARS:                      # grupy upraw tylko w roku mapy
        for g, key in z["group_keys"].items():
            masks[key] = z["group"][my] == g

    rows = []
    for cls, mask in masks.items():
        sel = valid & mask & (idx > 0)
        s = np.bincount(idx[sel], weights=ndvi[sel], minlength=n)[1:]
        c = np.bincount(idx[sel], minlength=n)[1:]
        for i in np.nonzero(c)[0]:
            rows.append((z["region_ids"][i], date, cls, float(s[i]), int(c[i])))
    return rows


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    spark = (SparkSession.builder.appName("rbd-ndvi-zonal")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    start, end = args.years

    with Timer(log, "mapa przynależności (driver)"):
        zones = build_zones(sc, args)
        zones_bc = sc.broadcast({k: zones[k] for k in ("region_idx", "arable", "group", "region_ids", "group_keys")})

    totals = [(my, cls, rid, int(cnt)) for (my, cls), arr in zones["n_total"].items()
              for rid, cnt in zip(zones["region_ids"], arr) if cnt > 0]
    totals_df = spark.createDataFrame(totals, ["map_year", "crop_class", "region_id", "n_total"])

    paths = ",".join(f"{args.hdfs}{args.ndvi_dir}/{y}/*.tif" for y in range(start, end + 1))
    schema = StructType([StructField("region_id", StringType()), StructField("date", StringType()),
                         StructField("crop_class", StringType()), StructField("ndvi_sum", DoubleType()),
                         StructField("n_valid", LongType())])

    with Timer(log, f"NDVI per powiat {start}–{end} (workery)"):
        rdd = (sc.binaryFiles(paths, minPartitions=args.partitions)
                 .flatMap(lambda pb: zonal_rows(pb, zones_bc)))
        df = (spark.createDataFrame(rdd, schema)
              .withColumn("date", F.to_date("date"))
              .withColumn("year", F.year("date"))
              .withColumn("map_year", F.when(F.col("year") <= 2020, 2018).otherwise(2022))
              .join(totals_df, ["map_year", "crop_class", "region_id"], "left")
              .withColumn("coverage", F.col("n_valid") / F.col("n_total"))
              .withColumn("ndvi_mean", F.when(F.col("coverage") >= args.min_coverage,
                                              F.col("ndvi_sum") / F.col("n_valid")))
              .withColumn("doy", F.dayofyear("date"))
              .withColumn("period", ((F.col("doy") - 1) / 16).cast("int"))
              .drop("map_year"))
        out = f"{args.hdfs}{args.out}"
        df.write.mode("overwrite").partitionBy("year").parquet(out)

    summary = (spark.read.parquet(out).where(F.col("year").between(start, end))
               .groupBy("crop_class").agg(F.countDistinct("date").alias("terminy"),
                                          F.countDistinct("region_id").alias("powiaty"),
                                          F.count("*").alias("wiersze"),
                                          F.round(F.avg("coverage"), 2).alias("sr_pokrycie"))
               .orderBy("crop_class").collect())
    log.info("zapisano -> %s (lata %d–%d)", out, start, end)
    for r in summary:
        log.info("  %-24s terminy %3d | powiaty %3d | wiersze %7d | średnie pokrycie %.2f",
                 r["crop_class"], r["terminy"], r["powiaty"], r["wiersze"], r["sr_pokrycie"])
    spark.stop()


if __name__ == "__main__":
    main()
