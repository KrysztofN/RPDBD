from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config import KAFKA, TOPIC

def ensure_topics(names):
    admin = KafkaAdminClient(bootstrap_servers=KAFKA.split(','), client_id='rbd-s1s2-admin')
    try:
        existing = set(admin.list_topics())
        for name in names:
            if name not in existing:
                try:
                    admin.create_topics([NewTopic(name, num_partitions=6, replication_factor=3,
                       topic_configs={'min.insync.replicas':'2','retention.ms':'1209600000'})])
                    print('Kafka topic created:',name,flush=True)
                except TopicAlreadyExistsError:
                    pass
    finally:
        admin.close()

def ensure_topic():
    ensure_topics([TOPIC])

def ensure_alert_topic():
    ensure_topics(['alerts.detected'])

if __name__ == '__main__':
    ensure_topics([TOPIC,'alerts.detected'])
