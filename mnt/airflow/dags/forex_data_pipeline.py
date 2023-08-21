import airflow
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from datetime import datetime, timedelta

import csv
import requests
import json

default_args ={
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retires": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("forex_data_pipeline", start_date=datetime(2021,1,1), 
         schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    is_forex_rates_available = HttpSensor(
        task_id = "is_forex_rates_available",
        http_conn_id = "forex_api",
        endpoint = "marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check = lambda response: "rates" in response.text,
        poke_interval = 5,
        timeout = 20
    )

    is_forex_currencies_file_available = FileSensor(
        task_id = "is_forex_currencies_file_available",
        fs_conn_id = "forex_path",
        filepath = "forex_currencies.csv",
        poke_interval = 5,
        timeout = 20
    )

    downloading_rates = PythonOperator(
            task_id="downloading_rates",
            python_callable=download_rates
    )

    saving_rates = BashOperator(
        task_id="saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
            """
    )