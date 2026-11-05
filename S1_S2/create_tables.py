"""Non-destructive S1/S2 schema in the existing rbd database."""
from stream_common import ch_client

def main():
    c = ch_client()
    ddl = [
    '''CREATE TABLE IF NOT EXISTS s1_hourly (
        region_id String, event_hour DateTime('UTC'), swvl1 Float64,
        t2m_c Float64, precip_mm_mean_cell Float64, observations UInt32,
        batch_id UInt64
      ) ENGINE ReplacingMergeTree(batch_id)
      PARTITION BY toYYYYMM(event_hour) ORDER BY (region_id,event_hour)''',
    '''CREATE TABLE IF NOT EXISTS s1_daily (
        region_id String, day Date, precip_mm Float64, et0_mm Float64,
        swvl1 Float64, valid_hours UInt8, version UInt64
      ) ENGINE ReplacingMergeTree(version)
      PARTITION BY toYYYYMM(day) ORDER BY (region_id,day)''',
    '''CREATE TABLE IF NOT EXISTS s1_balance (
        region_id String, day Date, kbw_7d_mm Float64, kbw_30d_mm Float64,
        kbw_60d_mm Float64, days_7d UInt8, days_30d UInt8, days_60d UInt8,
        swvl1 Float64, alert UInt8, version UInt64
      ) ENGINE ReplacingMergeTree(version)
      PARTITION BY toYYYYMM(day) ORDER BY (region_id,day)''',
    '''CREATE TABLE IF NOT EXISTS s2_hourly (
        region_id String, event_hour DateTime('UTC'), min_air_c Float64,
        min_ground_est_c Float64, min_surface_c Float64, min_soil_c Float64,
        observations UInt32, batch_id UInt64
      ) ENGINE ReplacingMergeTree(batch_id)
      PARTITION BY toYYYYMM(event_hour) ORDER BY (region_id,event_hour)''',
    '''CREATE VIEW IF NOT EXISTS s2_frost_alerts AS
       SELECT region_id,event_hour,min_air_c,min_ground_est_c,min_surface_c,
          min_soil_c, 'kukurydza_ziemniaki' AS crop_group, 0.0 AS threshold_c
       FROM s2_hourly FINAL WHERE min_ground_est_c < 0
       UNION ALL
       SELECT region_id,event_hour,min_air_c,min_ground_est_c,min_surface_c,
          min_soil_c,'rzepak_zboza_jare' AS crop_group,-4.0 AS threshold_c
       FROM s2_hourly FINAL WHERE min_ground_est_c < -4''',
    '''CREATE VIEW IF NOT EXISTS s2_rolling_24h AS
       SELECT region_id,event_hour,
          min(min_ground_est_c) OVER (
            PARTITION BY region_id ORDER BY toUnixTimestamp(event_hour)
            RANGE BETWEEN 82800 PRECEDING AND CURRENT ROW
          ) AS min_ground_24h_c,
          countIf(min_ground_est_c < 0) OVER (
            PARTITION BY region_id ORDER BY toUnixTimestamp(event_hour)
            RANGE BETWEEN 82800 PRECEDING AND CURRENT ROW
          ) AS frost_hours_24h
       FROM s2_hourly FINAL''',
    '''CREATE TABLE IF NOT EXISTS s1s2_alert_log (
        event_id String, chat_id Int64, region_id String,
        status LowCardinality(String), error String,
        sent_at DateTime64(3,'UTC') DEFAULT now64(3)
      ) ENGINE MergeTree ORDER BY (event_id,chat_id,sent_at)'''
    ]
    try:
        for stmt in ddl:
            c.command(stmt)
        print('Created S1/S2 tables without modifying B1/B2')
    finally:
        c.close()

if __name__ == '__main__':
    main()
