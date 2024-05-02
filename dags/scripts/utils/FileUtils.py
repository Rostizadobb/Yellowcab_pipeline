#File path
source_path = '/opt/airflow/dags/files'

#Azure conection string
azure_conn = 'DefaultEndpointsProtocol=https;AccountName=azureyellowcab;AccountKey=r/A3SvJIIzCnK+7RXvM+Pa8hrwE8MFAt6jwAwibQlap03V5oRbyD52911/AdxQN91srwByCo/LnC+ASt03RZLg==;EndpointSuffix=core.windows.net'

#SQL string connection
sql_conn = 'postgresql+psycopg2://airflow:airflow@postgres/postgres'


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

