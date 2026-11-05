"""Shared Spark schemas, config and idempotent ClickHouse driver inserts."""
import time
from config import *


def ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER,
                                          password=CH_PASSWORD, database=CH_DB)


def source(spark):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    schema = T.StructType([
       T.StructField('event_time', T.StringType()),
       T.StructField('grid_id', T.IntegerType()),
       *[T.StructField(k, T.DoubleType()) for k in (
          'latitude','longitude','t2m_c','d2m_c','skt_c','stl1_c','u10','v10','swvl1','tp_mm','ssrd_j_m2')]
    ])
    stream = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', KAFKA)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'earliest')
              .option('maxOffsetsPerTrigger', 120000)
              .load())
    raw = stream.select(F.from_json(F.col('value').cast('string'), schema).alias('p')).select('p.*')
    raw = raw.withColumn('event_time', F.to_timestamp('event_time', "yyyy-MM-dd'T'HH:mm:ssX"))
    raw = raw.filter(F.col('event_time').isNotNull() & F.col('grid_id').isNotNull())
    # Kafka replay is at-least-once. Avoid multiplying hourly rainfall on replay.
    # raw = raw.withWatermark('event_time', '3 hours').dropDuplicates(['grid_id', 'event_time'])
    raw = raw
    grid = (spark.read.option('header','true').csv(HDFS + MAP_HDFS)
            .select(F.col('grid_id').cast('int').alias('join_id'), F.col('region_id')))
    return raw.join(F.broadcast(grid), raw.grid_id == grid.join_id).drop('join_id')


def insert_rows(table, columns, rows):
    if not rows:
        return
    client = ch_client()
    try:
        client.insert(table, rows, column_names=columns)
    finally:
        client.close()


def make_spark(app):
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName(app)
            .config('spark.sql.session.timeZone','UTC')
            .config('spark.sql.shuffle.partitions','24')
            .getOrCreate())


def publish_alerts(alerts):
    """Separate Kafka topic; duplicates are possible on microbatch retry.
    The downstream Telegram relay uses event_id to suppress re-delivery.
    """
    if not alerts:
        return
    import json
    from kafka import KafkaProducer
    from kafka_setup import ensure_alert_topic
    ensure_alert_topic()
    producer = KafkaProducer(bootstrap_servers=KAFKA.split(','),
        key_serializer=lambda value: value.encode(),
        value_serializer=lambda value: json.dumps(value,ensure_ascii=False).encode(),
        acks='all',retries=5)
    try:
        for item in alerts:
            producer.send('alerts.detected',key=item['event_id'],value=item)
        producer.flush()
    finally:
        producer.close()
