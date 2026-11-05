"""Cassandra queries, bounded partitions and deterministic event-time versions."""
import json
import os
from datetime import datetime, timezone

DDL = [
    "CREATE KEYSPACE IF NOT EXISTS s12 WITH replication = {'class':'NetworkTopologyStrategy','dc1':3}",
    """CREATE TABLE IF NOT EXISTS s12.results_by_month (
      run_id text, region_id text, process text, month text, ts timestamp, crop text,
      event_id text, payload text, PRIMARY KEY ((run_id, region_id, process, month), ts, crop))
      WITH CLUSTERING ORDER BY (ts DESC, crop ASC)""",
    """CREATE TABLE IF NOT EXISTS s12.latest (
      run_id text, region_id text, process text, crop text, ts timestamp, payload text,
      PRIMARY KEY ((run_id, region_id), process, crop))""",
    """CREATE TABLE IF NOT EXISTS s12.deliveries (
      delivery_id text PRIMARY KEY, sent_at timestamp)"""
]


def connect():
    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import DCAwareRoundRobinPolicy
    profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                               load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'), request_timeout=30)
    cluster = Cluster(os.getenv('S12_CASSANDRA', '10.7.11.23,10.7.11.24,10.7.11.25').split(','),
                      execution_profiles={EXEC_PROFILE_DEFAULT:profile})
    return cluster, cluster.connect()


class Store:
    def __init__(self, session):
        self.session = session
        self.history = session.prepare('INSERT INTO s12.results_by_month (run_id,region_id,process,month,ts,crop,event_id,payload) VALUES (?,?,?,?,?,?,?,?)')
        self.latest = session.prepare('INSERT INTO s12.latest (run_id,region_id,process,crop,ts,payload) VALUES (?,?,?,?,?,?) USING TIMESTAMP ?')
        self.sent = session.prepare('SELECT sent_at FROM s12.deliveries WHERE delivery_id=?')
        self.mark = session.prepare('INSERT INTO s12.deliveries (delivery_id,sent_at) VALUES (?,?)')

    def save(self, doc):
        stamp = datetime.fromtimestamp(doc['ts'], timezone.utc)
        payload = json.dumps(doc, allow_nan=False)
        self.session.execute(self.history, (doc['run_id'],doc['region_id'],doc['process'],stamp.strftime('%Y-%m'),stamp,doc['crop'],doc['event_id'],payload))
        # Old replayed result cannot overwrite a newer current state.
        self.session.execute(self.latest, (doc['run_id'],doc['region_id'],doc['process'],doc['crop'],stamp,payload,doc['ts']*1000000))
