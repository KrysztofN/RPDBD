#!/usr/bin/env bash
# Uruchomienie zadania Spark na klastrze (na masterze, z katalogu z kodem projektu).
#
# Po co: jedno miejsce z ustawieniami, o których łatwo zapomnieć:
#   --network host               kontener-driver widzi Sparka i HDFS pod adresami maszyn
#   spark.driver.host=<IP>       executory na workerach łączą się z driverem po IP (workery nie znają
#                                nazwy Bigdata-wn212-01 — bez tego executory padają w kółko)
#   -v <kod>:/app                kod projektu bez przebudowy obrazu
#
# Użycie:  infra/spark/submit.sh <skrypt.py> [argumenty]
#   np.    infra/spark/submit.sh infra/spark/smoke_test.py
# Zmienne: RBD_CODE_DIR (domyślnie bieżący katalog), RBD_SPARK_MASTER_IP (domyślnie 10.7.11.21)
set -euo pipefail

MASTER_IP="${RBD_SPARK_MASTER_IP:-10.7.11.21}"
CODE_DIR="${RBD_CODE_DIR:-$PWD}"

exec docker run --rm --network host -v "$CODE_DIR":/app rbd-python:0.1 \
  /opt/spark/bin/spark-submit \
  --master "spark://$MASTER_IP:7077" \
  --conf "spark.driver.host=$MASTER_IP" \
  "$@"
