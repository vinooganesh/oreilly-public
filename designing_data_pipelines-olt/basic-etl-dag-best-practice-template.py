from airflow import DAG

# Sensors
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor

# Operators
from airflow.operators.bash  import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
import pandas as pd
from datetime import datetime, date

default_args = {
    'mysql_conn_id': 'demo_local_mysql'
}

with DAG('basic_etl_dag',
         schedule_interval=None,
         default_args=default_args,
         start_date=datetime(2022, 1, 1),
         catchup=False) as dag:

    extract_task_http_sensor = None # TODO In Exercise

    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://datahub.io/core/top-level-domain-names/r/top-level-domain-names.csv.csv -O /root/airflow-extract-data.csv',
    )

    transform_task_filesystem_sensor = None # TODO In Exercise

    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv("/root/airflow-extract-data.csv")
        generic_type_df = df[df["Type"] == "generic"]
        generic_type_df["Date"] = today.strftime("%Y-%m-%d")
        generic_type_df.to_csv("/root/airflow-transformed-data.csv", index=False)
        print(f'Number of rows: {len(generic_type_df)}')

    transform_task = PythonOperator(
         task_id='transform_task',
         python_callable=transform_data,
         dag=dag)

    load_task_filesystem_sensor = None # TODO In Exercise

    load_task = MySqlOperator(
        task_id='load_task', sql=r"""
        USE airflow_load_database;
        LOAD DATA LOCAL INFILE '/root/airflow-transformed-data.csv' INTO TABLE top_level_domains FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
        """, dag=dag
    )

    extract_task >> transform_task >> load_task

    # Final DAG:
    # extract_task_http_sensor >> extract_task >> transform_task_filesystem_sensor >> transform_task >> load_task_filesystem_sensor >> load_task
