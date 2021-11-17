# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.exceptions import AirflowFailException
from hdfs import InsecureClient
# from pyspark.sql import SparkSession

from datetime import datetime, timedelta
import json
import os
import requests
import logging

import function.get_config as get_config

import function.libs as libs

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
}


# Создать DAG: FinalProject выгрузка из api
with DAG(
    'FinalProject',
    start_date=datetime(2021, 11, 10),
    # schedule_interval='@daily'
    schedule_interval=None
) as fp_dag:

    start = DummyOperator(
        task_id='start',
    )

    test = PythonOperator(        
        task_id='test',
            dag=fp_dag,
            python_callable=libs.db_export_bronze,
        )

    start >> test