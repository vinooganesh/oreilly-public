from datetime import datetime

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator

with DAG('load_dag',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={'mysql_conn_id': 'demo_local_mysql'},
    catchup=False) as dag:

    load_task = MySqlOperator(
        task_id='load_task', sql=r"""
        USE airflow_load_database;
        LOAD DATA LOCAL INFILE '/root/airflow-transformed-data.csv' INTO TABLE top_level_domains FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
        """, dag=dag
    )

    load_task
