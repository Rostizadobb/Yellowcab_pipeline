import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from scripts.scripts import file_download, load_files, calculation_file

with DAG("dag_yellow_cab",
    start_date=airflow.utils.dates.days_ago(1), 
    schedule_interval='@daily', 
    catchup=False) as dag:

    request_file = PythonOperator(
        task_id="request_file",
        python_callable=file_download,
    )    

    load_data = PythonOperator(
        task_id= 'load_data',
        python_callable=load_files,
    )

    calculate_mean_time = PythonOperator(
        task_id= 'calculate_mean_time',
        python_callable=calculation_file,
    )

request_file >> load_data >> calculate_mean_time
