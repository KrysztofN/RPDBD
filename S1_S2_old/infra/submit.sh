#!/usr/bin/env bash
set -euo pipefail
# Run on a cluster host with the repository; args: module.py [options].
# DRIVER_IP must be reachable from all workers. Never use 127.0.0.1.
: "${S12_DRIVER_IP:?Set S12_DRIVER_IP to this host IP (e.g. 10.7.11.21)}"
CODE_DIR="${S12_CODE_DIR:-$PWD}"
exec docker run --rm --init --network host \
  -v "$CODE_DIR:/app:ro" \
  -e HADOOP_USER_NAME=hadoop -e S12_KAFKA -e S12_CONFIG -e S12_HDFS \
  -e RBD_WEBHDFS_URL=http://10.7.11.21:9870 \
  -e RBD_HDFS_HOST_MAP=Bigdata-wn212-02=10.7.11.22,Bigdata-wn212-03=10.7.11.23,Bigdata-wn212-04=10.7.11.24,Bigdata-wn212-05=10.7.11.25 \
  -e S12_DRIVER_IP \
  s12-python:0.1 bash -c '
    set -euo pipefail
    cd /app
    python3 -m zipfile -c /tmp/s12.zip S1_S2 lib S1/data
    exec /opt/spark/bin/spark-submit \
      --master spark://10.7.11.23:7177,10.7.11.24:7177,10.7.11.25:7177 \
      --conf "spark.driver.host=$S12_DRIVER_IP" \
      --conf spark.executorEnv.HADOOP_USER_NAME=hadoop \
      --conf spark.hadoop.dfs.replication=3 \
      --conf spark.cores.max=3 --conf spark.executor.cores=1 \
      --conf spark.executor.memory=1536m --conf spark.driver.memory=2g \
      --conf spark.sql.shuffle.partitions=12 \
      --conf spark.sql.adaptive.enabled=false \
      --py-files /tmp/s12.zip "$@"
  ' submit "$@"
