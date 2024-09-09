import sys
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


sys.path.append('/home/dipatel/PySparkPractise/scripts')
from extract_data import data_extract
from transform_data import data_transform
from load_data import data_load


def Extract():
    extract_df= data_extract()
    return extract_df


def Transformations():
    transform_data_df=data_transform()
    return transform_data_df


def Load():
    transform_data_df=data_load()
    return transform_data_df

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'Fast_food_sales',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

task1 = PythonOperator(
    task_id='Extract_data',
    python_callable=Extract,
    dag=dag,
)

task2 = PythonOperator(
    task_id='Transform_data',
    python_callable=Transformations,
    dag=dag,
)


task3 = PythonOperator(
    task_id='Load_data',
    python_callable=Load,
    dag=dag,
)


# Set task dependencies
task1 >> task2 >> task3
