"""
Airflow: przepływ B1 + B2 + biuletyn — osobne DAG-i spięte zbiorami danych (Asset), bez wzajemnego wywoływania.

DAG-i:
  rbd_ndvi_counties      (ręcznie, parametr: rok)  Spark: NDVI pól per powiat z HDFS          -> Asset ndvi_counties
  rbd_weather_counties   (ręcznie)                 Spark: pogoda ERA5-Land per powiat          -> Asset weather_counties
  rbd_b1                 (gdy nowe ndvi_counties)  Spark: stan upraw vs norma                  -> Asset b1
  rbd_b2                 (gdy nowe ndvi_counties)  Spark: fenologia + widoki B1+B2 w ClickHouse -> Asset b2
  rbd_bulletin_requests  (gdy nowe b1 I b2)        zlecenia biuletynów do Kafki (powiaty/rok/termin ze zmiennych)
  rbd_demo_season        (ręcznie, demo)           zlecenia dla wielu terminów sezonu (mapowanie zadań)

Trochę teorii:
  Asset = „ten zbiór danych właśnie się zmienił”. DAG z `schedule=[asset]` startuje sam, gdy inny DAG go zaktualizuje,
  więc B1 i B2 ruszają równolegle po NDVI, a zlecenia biuletynów dopiero, gdy gotowe są oba (warunek „i”).
  Każde zadanie to kontener rbd-python / rbd-reporter (DockerOperator) z tymi samymi ustawieniami co
  infra/spark/submit.sh. Biuletyny robią asynchronicznie raportery (Kafka -> Bielik -> PDF) — Airflow na nie nie czeka.

Zmienne Airflow (airflow variables set ...): clickhouse_password (hasło; ustawiane skryptem z .env, nie w repo),
  bulletin_year, bulletin_period, bulletin_counties (np. "3011 1206" albo "voiv:30").
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset, Param, task
from docker.types import Mount

HDFS = "hdfs://10.7.11.21:9000"
CODE_DIR = "/home/student/rbd-build/app"          # kod projektu na masterze (montowany do kontenerów jako /app)
MASTER_IP = "10.7.11.21"
HOST_MAP = ("Bigdata-wn212-02=10.7.11.22,Bigdata-wn212-03=10.7.11.23,"
            "Bigdata-wn212-04=10.7.11.24,Bigdata-wn212-05=10.7.11.25")

NDVI_COUNTIES = Asset(f"{HDFS}/rbd/clean/ndvi_counties")
WEATHER = Asset(f"{HDFS}/rbd/clean/weather_counties_periods")
B1 = Asset(f"{HDFS}/rbd/clean/b1_periods")
B2 = Asset(f"{HDFS}/rbd/clean/b2_phenology")

COMMON = dict(network_mode="host", docker_url="unix://var/run/docker.sock", auto_remove="success",
              mount_tmp_dir=False, mounts=[Mount(source=CODE_DIR, target="/app", type="bind")])


# hasło ClickHouse ze zmiennej Airflow (szyfrowanej kluczem Fernet); czytane w chwili uruchomienia (szablon),
# a Airflow maskuje w logach i UI wartości zmiennych, których nazwa zawiera „password”
SECRET_ENV = {"CLICKHOUSE_PASSWORD": "{{ var.value.clickhouse_password }}"}
PY_ENV = {"RBD_WEBHDFS_URL": "http://localhost:9870", "RBD_HDFS_HOST_MAP": HOST_MAP, "HOME": "/tmp", **SECRET_ENV}


def spark_task(task_id: str, script: str, args: str = "", **kw) -> DockerOperator:
    """spark-submit w kontenerze rbd-python — te same ustawienia co infra/spark/submit.sh."""
    cmd = (f"cd /app && python3 -m zipfile -c /tmp/lib.zip lib B1_B2 >/dev/null && "
           f"exec /opt/spark/bin/spark-submit --master spark://{MASTER_IP}:7077 "
           f"--conf spark.driver.host={MASTER_IP} --conf spark.executorEnv.HADOOP_USER_NAME=hadoop "
           f"--conf spark.hadoop.dfs.replication=1 --conf spark.executor.memory=8g --conf spark.driver.memory=4g "
           f"--conf spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=/app/infra/spark/log4j2.properties "
           f"--py-files /tmp/lib.zip {script} {args}")
    return DockerOperator(task_id=task_id, image="rbd-python:0.1", entrypoint=["bash", "-c"], command=[cmd],
                          environment={"HADOOP_USER_NAME": "hadoop", **SECRET_ENV}, **COMMON, **kw)


def python_task(task_id: str, module_args: str, image: str = "rbd-python:0.1", **kw) -> DockerOperator:
    return DockerOperator(task_id=task_id, image=image, entrypoint=["bash", "-c"],
                          command=[f"python3 -m {module_args}"], environment=PY_ENV, **COMMON, **kw)


DEFAULTS = dict(start_date=datetime(2026, 10, 1), catchup=False, tags=["rbd", "b1b2"])

with DAG("rbd_ndvi_counties", schedule=None, description="NDVI pól per powiat (Spark) z plików na HDFS",
         params={"year_from": Param(2001, type="integer"), "year_to": Param(2024, type="integer")}, **DEFAULTS):
    spark_task("ndvi_counties", "lib/ndvi_zonal_spark.py",
               "--years {{ params.year_from }} {{ params.year_to }}", outlets=[NDVI_COUNTIES])

with DAG("rbd_weather_counties", schedule=None, description="Pogoda ERA5-Land per powiat (Spark)", **DEFAULTS):
    spark_task("weather_counties", "lib/era5_counties_spark.py", outlets=[WEATHER])

with DAG("rbd_b1", schedule=[NDVI_COUNTIES], description="B1: stan upraw vs norma krocząca", **DEFAULTS):
    spark_task("b1_crop_condition", "B1_B2/b1_crop_condition.py", outlets=[B1])

with DAG("rbd_b2", schedule=[NDVI_COUNTIES], description="B2: fenologia + widoki B1+B2", **DEFAULTS):
    (spark_task("b2_phenology", "B1_B2/b2_phenology.py")
     >> python_task("b1b2_views", "B1_B2.b1b2_context", outlets=[B2]))

with DAG("rbd_bulletin_requests", schedule=(B1 & B2), description="Zlecenia biuletynów -> Kafka (gdy B1 i B2 gotowe)",
         **DEFAULTS):
    python_task("request_bulletins",
                "B1_B2.reporter.request_bulletins --year {{ var.value.get('bulletin_year', '2018') }} "
                "--period {{ var.value.get('bulletin_period', '10') }} "
                "{% set c = var.value.get('bulletin_counties', '3011') %}"
                "{% if c.startswith('voiv:') %}--voiv {{ c[5:] }}{% else %}--counties {{ c }}{% endif %}",
                image="rbd-reporter:0.1")

with DAG("rbd_demo_season", schedule=None, description="Demo: zlecenia biuletynów dla kolejnych terminów sezonu",
         params={"year": Param(2018, type="integer"), "periods": Param([7, 8, 9, 10, 11, 12], type="array"),
                 "counties": Param("3011 1206 0605", type="string")}, **DEFAULTS):

    @task
    def commands(params=None) -> list[list[str]]:
        """Po jednym poleceniu na termin — z tego Airflow robi osobne zadania (mapowanie)."""
        return [[f"python3 -m B1_B2.reporter.request_bulletins --year {params['year']} --period {p} "
                 f"--counties {params['counties']}"] for p in params["periods"]]

    DockerOperator.partial(task_id="request_period", image="rbd-reporter:0.1", entrypoint=["bash", "-c"],
                           environment=PY_ENV, **COMMON).expand(command=commands())
