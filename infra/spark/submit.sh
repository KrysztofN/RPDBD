#!/usr/bin/env bash
# Uruchomienie zadania Spark na klastrze (na masterze, z katalogu z kodem projektu).
#
# Po co: jedno miejsce z ustawieniami, o których łatwo zapomnieć:
#   --network host                 kontener-driver widzi Sparka i HDFS pod adresami maszyn
#   spark.driver.host=<IP>         executory na workerach łączą się z driverem po IP (workery nie znają
#                                  nazwy Bigdata-wn212-01 — bez tego executory padają w kółko)
#   HADOOP_USER_NAME=hadoop        katalogi /rbd na HDFS należą do użytkownika hadoop (proste uwierzytelnianie)
#   spark.hadoop.dfs.replication=1 pliki zapisywane przez Sparka mają 1 kopię (decyzja zespołu, dane odtwarzalne)
#   spark.executor.memory=8g       domyślne 1 GB to za mało dla zdjęć NDVI (ok. 20 MB każde + maski)
#   --py-files lib.zip             kod z lib/ i B1_B2/ dostępny także na workerach
#   log4j2.properties              logi samego Sparka tylko od WARN (nasze INFO z lib/log.py zostają)
#   -e CLICKHOUSE_PASSWORD         przekazuje hasło ClickHouse z otoczenia (jeśli ustawione), nie z kodu
#   -v <kod>:/app                  kod projektu bez przebudowy obrazu
#
# Użycie:  infra/spark/submit.sh <skrypt.py> [argumenty]
#   np.    infra/spark/submit.sh infra/spark/smoke_test.py
# Zmienne: RBD_CODE_DIR (domyślnie bieżący katalog), RBD_SPARK_MASTER_IP (domyślnie 10.7.11.21),
#          RBD_EXECUTOR_MEMORY (domyślnie 8g)
set -euo pipefail

MASTER_IP="${RBD_SPARK_MASTER_IP:-10.7.11.21}"
CODE_DIR="${RBD_CODE_DIR:-$PWD}"
EXEC_MEM="${RBD_EXECUTOR_MEMORY:-8g}"

exec docker run --rm --network host -v "$CODE_DIR":/app -e HADOOP_USER_NAME=hadoop -e CLICKHOUSE_PASSWORD \
  rbd-python:0.1 bash -c '
  set -e
  cd /app && python3 -m zipfile -c /tmp/lib.zip $(ls -d lib B1_B2 2>/dev/null) >/dev/null
  exec /opt/spark/bin/spark-submit \
    --master "spark://'"$MASTER_IP"':7077" \
    --conf "spark.driver.host='"$MASTER_IP"'" \
    --conf spark.executorEnv.HADOOP_USER_NAME=hadoop \
    --conf spark.hadoop.dfs.replication=1 \
    --conf "spark.executor.memory='"$EXEC_MEM"'" \
    --conf spark.driver.memory=4g \
    --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=/app/infra/spark/log4j2.properties" \
    --py-files /tmp/lib.zip \
    "$@"
' submit "$@"
