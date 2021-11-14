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

import lib_fp

