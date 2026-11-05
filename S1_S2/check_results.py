"""Lightweight checks from inside the project's container."""
from kafka import KafkaConsumer
from config import TOPIC, KAFKA
from stream_common import ch_client


def main():
    c = KafkaConsumer(bootstrap_servers=KAFKA.split(','))
    try:
        print('Kafka topics:', sorted(t for t in c.topics() if t.startswith('weather.')))
        print('Partitions:', c.partitions_for_topic(TOPIC))
    finally:
        c.close()
    ch = ch_client()
    try:
        for table in ('s1_hourly','s1_daily','s1_balance','s2_hourly'):
            count = ch.query(f'SELECT count() FROM {table}').first_row[0]
            print(f'{table}: {count} rows')
        for r in ch.query('''SELECT region_id,event_hour,crop_group,min_ground_est_c,threshold_c
                              FROM s2_frost_alerts ORDER BY event_hour DESC LIMIT 5''').result_rows:
            print('S2 alert:', r)
    finally:
        ch.close()

if __name__ == '__main__':
    main()
