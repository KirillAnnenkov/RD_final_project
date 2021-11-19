# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.exceptions import AirflowFailException
from hdfs import InsecureClient
from pyspark.sql import SparkSession

from datetime import datetime, timedelta
import json
import os
import requests
import logging
import sys


sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

# Получить текущую директорию
current_dir = os.path.dirname(os.path.abspath(__file__))

import get_config
import libs

# # Получить список таблиц для выгрузки из конфига
tables = get_config.config(current_dir+'/config_fp.json',"db_tables")

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
}

#  Создать DAG: FinalProject
with DAG(
    'FinalProject',
    description='FinalProject',
    start_date=datetime(2021, 11, 18, 1, 0),
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    # schedule_interval="@daily",
    # dagrun_timeout=timedelta(minutes=1)
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    finish = DummyOperator(
        task_id='finish',
    )

    # Получить список таблиц для выгрузки из конфига
    tables = get_config.config(current_dir+'/config_fp.json',"db_tables")

    # for table_name in tables:
    #     bronze = PythonOperator(
    #         task_id=f'data_to_bronze_{table_name}',
    #         dag=dag,
    #         python_callable=libs.db_export_bronze,
    #         op_kwargs={
    #             "table_name": table_name,
    #         }
    #     )

    #     silver = PythonOperator(
    #         task_id=f'data_to_silver_{table_name}',
    #         dag=dag,
    #         python_callable=libs.db_to_silver,
    #         op_kwargs={
    #             "table_name": table_name,
    #         }
    #     )

    #     start >> bronze >> silver >> finish
        
    load_clients = PythonOperator(
        task_id='load_clients_to_dwh',
        dag=dag,
        python_callable=libs.load_clients_to_dwh
    )
    
    finish >> load_clients