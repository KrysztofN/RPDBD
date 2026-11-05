from kafka_setup import ensure_topics
ensure_topics(['alerts.detected', 's1.daily.metrics.v1', 's1s2.incidents.v1'])
