from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, date

with DAG(
    dag_id='transform_dag',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

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

  transform_task
