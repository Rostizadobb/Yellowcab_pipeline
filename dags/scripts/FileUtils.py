import os

#File path
source_path = os.environ('SOURCE_FILE_PATH')

#SQL string connection
sql_conn = os.environ('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')

#SQL statements
sql_query = """
WITH last_date AS (
    SELECT MAX(tpep_pickup_datetime) AS last_start_time
    FROM yellowcab_data
)
SELECT 
    AVG(duration) AS average_duration,
    MIN(tpep_pickup_datetime) AS min_pickup_datetime,
    MAX(tpep_pickup_datetime) AS max_pickup_datetime
FROM (
    SELECT 
        (tpep_dropoff_datetime - tpep_pickup_datetime) AS duration,
        tpep_pickup_datetime
    FROM yellowcab_data 
    CROSS JOIN last_date 
    WHERE tpep_pickup_datetime >= last_start_time - INTERVAL '45 days'
) AS trip_durations;
"""

