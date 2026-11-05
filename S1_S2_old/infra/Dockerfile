FROM rbd-python:0.1
USER root
RUN python3 -m pip install --no-cache-dir \
    confluent-kafka==2.6.1 cassandra-driver==3.29.2 \
    fastapi==0.115.6 uvicorn==0.34.0 prometheus-client==0.21.1 \
    'requests[socks]==2.32.3' pytest==8.3.4
# A separate image/tag: do not rebuild/replace rbd-python or rbd-reporter.
# Bundle Spark Kafka jars at build time; runtime cluster needs no Maven internet access.
RUN mkdir -p /tmp/ivy && \
    /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4 \
      --conf spark.jars.ivy=/tmp/ivy /opt/spark/examples/src/main/python/pi.py 1 && \
    cp /tmp/ivy/jars/*.jar /opt/spark/jars/ && rm -rf /tmp/ivy
COPY S1_S2 /app/S1_S2
COPY lib /app/lib
COPY S1/data /app/S1/data
RUN mkdir -p /app/S1_S2/local && chmod 777 /app/S1_S2/local
ENV PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
USER spark
