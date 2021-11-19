# -*- coding: utf-8 -*-

# %%
# from airflow.hooks.postgres_hook import PostgresHook
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from airflow.hooks.postgres_hook import PostgresHook
from pyspark.sql import functions as F
from airflow.hooks.base_hook import BaseHook

import logging
from datetime import datetime, timedelta
import os

import get_config

def db_export_bronze(**kwargs):
    '''Принимает название таблицы из БД, выгружает таблицу в bronze'''

    # Получить текущую директорию
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Получить список таблиц для выгрузки из конфига
    table_name = kwargs['table_name']
    logging.info(f'Начало экспорта таблицы в bronze: {table_name}')

    # Создаем соеденение с hdfs
    url_hdfs = get_config.config(current_dir +'/config_fp.json',"url_hdfs")
    client = InsecureClient(f'{url_hdfs}', user='user')

    # Создать папку (дата) для выгрузки данных
    path_bronze = get_config.config(current_dir+'/config_fp.json',"path_bronze")
    path_hdfs = os.path.join(path_bronze, str(datetime.today().strftime('%Y-%m-%d')))

    # Создать папку, если ее нет
    client.makedirs(path_hdfs)

    # Подключится к базе дaнных
    conn = PostgresHook(postgres_conn_id="postgres_default").get_conn()
    cur = conn.cursor()

    with client.write(os.path.join(path_hdfs, f'{kwargs["table_name"]}.csv' ), overwrite=True) as csv_file:
        cur.copy_expert('COPY (SELECT * FROM {0}) TO STDOUT WITH HEADER CSV'.format(kwargs['table_name']), csv_file)

    logging.info(f'Успешный экспорт таблицы в bronze: {table_name}')

def db_to_silver(**kwargs):
    '''Принимает название таблицы из БД, выгружает таблицу в silver'''

    # Получить текущую директорию
    current_dir = os.path.dirname(os.path.abspath(__file__))

    table_name = kwargs['table_name']
    logging.info(f'Начало экспорта таблицы в silver: {table_name}')

    # Создаем соеденение с hdfs
    url_hdfs = get_config.config(current_dir+'/config_fp.json',"url_hdfs")
    client = InsecureClient(f'{url_hdfs}', user='user')

   # Получить путь к bronze
    path_bronze = get_config.config(current_dir+'/config_fp.json',"path_bronze")
    path_bronze_date = os.path.join(path_bronze, str(datetime.today().strftime('%Y-%m-%d')))

    # Создать папку (дата) для выгрузки данных в silver
    path_silver = get_config.config(current_dir+'/config_fp.json',"path_silver")
    path_silver_date = os.path.join(path_silver, str(datetime.today().strftime('%Y-%m-%d')))

    client.makedirs(path_silver_date)

    spark = SparkSession \
        .builder.master('local') \
        .appName('FP') \
        .getOrCreate()

    df = spark.read.load(os.path.join(path_bronze_date, table_name + '.csv')
                         , header="true"
                         , inferSchema="true"
                         , format="csv")

    df = df.drop_duplicates()

    df.write.parquet(os.path.join(path_silver_date, table_name), mode='overwrite')

    logging.info(f'Успешный экспорт таблицы в silver: {table_name}')

def load_clients_to_dwh():
    '''Загрузка таблицы clients в DWH (greenplum)'''

    # Получить текущую директорию
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Получить путь к данным в silver
    path_silver = get_config.config(current_dir+'/config_fp.json',"path_silver")
    path_silver_date = os.path.join(path_silver, str(datetime.today().strftime('%Y-%m-%d')))

    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('FP')\
        .getOrCreate()

    df_clients_silver = spark.read.parquet(path_silver_date+'/clients')
    df_dim_clients = df_clients_silver.select('client_id', F.col('fullname').alias('client_name'))

    gp_conn = BaseHook.get_connection('dwh_greenplum')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user":gp_conn.login, "password":gp_conn.password}

    df_dim_clients.write.jdbc(gp_url, table = 'dim_clients', properties = gp_creds, mode='overwrite')

    logging.info(f'Успешная загрузка clients в DWH')

def load_products_to_dwh():
    '''Загрузка таблицы products в DWH (greenplum)'''

    # Получить текущую директорию
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Получить путь к данным в silver
    path_silver = get_config.config(current_dir+'/config_fp.json',"path_silver")
    path_silver_date = os.path.join(path_silver, str(datetime.today().strftime('%Y-%m-%d')))

    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('FP')\
        .getOrCreate()

    df_products_silver = spark.read.parquet(path_silver_date+'/products')
    df_aisles_silver = spark.read.parquet(path_silver_date+'/aisles')
    df_departments_silver = spark.read.parquet(path_silver_date+'/departments')

    df_dim_products = df_products_silver\
        .join(df_aisles_silver, 'aisle_id', 'left')\
        .join(df_departments_silver, 'department_id', 'left')\
        .select('product_id', 'product_name', 'aisle', 'department')

    gp_conn = BaseHook.get_connection('dwh_greenplum')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user":gp_conn.login, "password":gp_conn.password}

    df_dim_products.write.jdbc(gp_url, table = 'dim_products', properties = gp_creds, mode='overwrite')

    logging.info(f'Успешная загрузка products в DWH')    

def load_aisles_to_dwh():
    pass

def load_dapartmens_to_dwh():
    pass

def load_dates_to_dwh():
    pass

def load_orders_fact_to_dwh():
    pass


if __name__ == "__main__":
    # Получить список таблиц для выгрузки из конфига
    tables = get_config.config('../config/config_fp.json',"db_tables")

    for table in tables:
        db_export_bronze(table_name=table)
        db_to_silver(table_name=table)

# %%
