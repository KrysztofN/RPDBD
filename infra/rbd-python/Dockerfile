# Obraz rbd-python: Spark 4.0 + Java 17 + Python z bibliotekami do danych satelitarnych.
#
# Po co: maszyny klastra mają goły Python (bez numpy, rasterio). Jeden obraz działa tak samo na wszystkich
# 5 maszynach — do pobierania NDVI na HDFS (lib/modis_ingest.py) i jako baza Sparka (driver i executory).
#
# Budowa (na maszynie z Dockerem, z katalogu głównego repo):
#   docker build -t rbd-python:0.1 infra/rbd-python
# Test:
#   docker run --rm rbd-python:0.1 python3 -c "import rasterio, geopandas, pyarrow, pyspark; print('ok')"
FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu

USER root

# wersje przypięte, żeby obraz zawsze budował się tak samo (koła binarne — bez kompilacji GDAL)
RUN python3 -m pip install --no-cache-dir \
        numpy==2.2.6 \
        pandas==2.2.3 \
        pyarrow==19.0.1 \
        scipy==1.15.3 \
        shapely==2.0.7 \
        pyogrio==0.10.0 \
        geopandas==1.0.1 \
        affine==2.4.0 \
        rasterio==1.4.3 \
        requests==2.32.3

# kod projektu montujemy przy uruchomieniu (-v ~/RPDBD:/app), dzięki temu `python -m lib...` działa bez przebudowy
WORKDIR /app
# pyspark nie jest instalowany przez pip — leży w obrazie Sparka (/opt/spark/python), więc dodajemy go do ścieżki
ENV PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYTHONUNBUFFERED=1

USER spark
