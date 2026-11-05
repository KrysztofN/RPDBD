import os
KAFKA = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '10.7.11.22:9092,10.7.11.23:9092,10.7.11.24:9092')
TOPIC = os.getenv('WEATHER_TOPIC', 'weather.hourly.v2')
WEBHDFS = os.getenv('WEBHDFS_URL', 'http://10.7.11.21:9870')
HDFS = os.getenv('HDFS_URL', 'hdfs://10.7.11.21:9000')
CH_HOST = os.getenv('CLICKHOUSE_HOST', '10.7.11.21')
CH_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CH_DB = os.getenv('CLICKHOUSE_DATABASE', 'rbd')
MAP_HDFS = '/rbd/ref/weather_grid_counties/map.csv'
CHECKPOINT_ROOT = os.getenv('CHECKPOINT_ROOT', '/rbd/checkpoints/s1s2_v2')
