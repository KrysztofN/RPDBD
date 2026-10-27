"""
DAG testowy infrastruktury: czy Airflow umie uruchomić kontener rbd-python i czy Spark/HDFS są osiągalne.

Zadania:
  1. python_env  — kontener rbd-python: wersje bibliotek (rasterio, pyspark) i nazwa maszyny
  2. spark_hdfs  — kontener rbd-python: ile plików NDVI jest na HDFS (przez WebHDFS) i czy master Sparka żyje

Uruchamiany ręcznie (bez harmonogramu): w UI przycisk „Trigger” albo `airflow dags trigger infra_smoke`.
"""
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

COMMON = dict(
    image="rbd-python:0.1",
    network_mode="host",
    docker_url="unix://var/run/docker.sock",
    auto_remove="success",
    mount_tmp_dir=False,
)

with DAG(
    dag_id="infra_smoke",
    description="Test: Airflow -> kontener rbd-python -> Spark i HDFS",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["infra", "test"],
) as dag:
    python_env = DockerOperator(
        task_id="python_env",
        command=["python3", "-c",
                 "import socket, rasterio, pyspark, geopandas; "
                 "print('maszyna:', socket.gethostname(), '| rasterio', rasterio.__version__, "
                 "'| pyspark', pyspark.__version__, '| geopandas', geopandas.__version__)"],
        **COMMON,
    )

    spark_hdfs = DockerOperator(
        task_id="spark_hdfs",
        command=["python3", "-c",
                 "import requests; "
                 "s = requests.get('http://10.7.11.21:8080/json/', timeout=10).json(); "
                 "print('Spark:', s['status'], '| workery:', s['aliveworkers'], '| rdzenie:', s['cores']); "
                 "r = requests.get('http://10.7.11.21:9870/webhdfs/v1/rbd/raw/modis_ndvi_poland', "
                 "params={'op': 'GETCONTENTSUMMARY', 'user.name': 'hadoop'}, timeout=10).json()['ContentSummary']; "
                 "print('HDFS NDVI: plików', r['fileCount'], '| GB', round(r['length'] / 1e9, 2))"],
        **COMMON,
    )

    python_env >> spark_hdfs
