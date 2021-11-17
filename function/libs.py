# -*- coding: utf-8 -*-

# from airflow.hooks.postgres_hook import PostgresHook
from hdfs import InsecureClient

import logging
from datetime import datetime, timedelta
import os

import psycopg2

# import function.get_config as get_config
import get_config

def db_export_bronze(**kwargs):
    '''Принимает название таблицы из БД, выгружает таблицу в bronze'''

# Настрой доступа к postgress (no airflow)
    pg_creds = {
        'host': '192.168.0.142'
        , 'port': '5432'
        , 'database': 'dshop'
        , 'user': 'pguser'
        , 'password': 'secret'
    }

    # Получить список таблиц для выгрузки из конфига
    table_name = kwargs['table_name']
    logging.info(f'Начало экспорта таблицы в bronze: {table_name}')

    # Создаем соеденение с hdfs
    url_hdfs = get_config.config('./config/config_fp.json',"url_hdfs")
    client = InsecureClient(f'{url_hdfs}', user='user')

    # Создать папку (дата) для выгрузки данных
    path_bronze = get_config.config('./config/config_fp.json',"path_bronze")
    path_hdfs = os.path.join(path_bronze, str(datetime.today().strftime('%Y-%m-%d')))

    # Создать папку, если ее нет
    client.makedirs(path_hdfs)

    # # Подключится к базе дaнных, выгрузить в bronze таблицу
    # with psycopg2.connect(**pg_creds) as pg_connection:
    #     cursor = pg_connection.cursor()
    #     with client.write(os.path.join(path_hdfs, f'{kwargs["table_name"]}.csv' )) as csv_file:
    #         cursor.copy_expert('COPY (SELECT * FROM {0}) TO STDOUT WITH HEADER CSV'.format(kwargs['table_name']), csv_file)


    # Создать папку (дата) для выгрузки данных
    path_hdfs = os.path.join(
        '/', 'hm', 'homework7', 'bronze', 'db', str(datetime.today().strftime('%Y-%m-%d')))

    client.makedirs(path_hdfs)


    # with psycopg2.connect(**pg_creds) as pg_connection:
    #     cursor = pg_connection.cursor()        
    #     with open(file=table_name+'.csv', mode='w') as csv_file:            
    #         cursor.copy_expert('COPY (SELECT * FROM {0}) TO STDOUT WITH HEADER CSV'.format(kwargs['table_name']), csv_file)


    import pandas as pd
    # df = pd.read_csv("aisles.csv")

    # with client.write(os.path.join(path_hdfs, f'{kwargs["table_name"]}.csv'), encoding = 'utf-8') as writer:
    #     df.to_csv(writer)

    # Creating a simple Pandas DataFrame
    liste_hello = ['hello1','hello2']
    liste_world = ['world1','world2']
    df = pd.DataFrame(data = {'hello' : liste_hello, 'world': liste_world})
    
    # # Writing Dataframe to hdfs
    # with client.write('/user/hdfs/wiki/helloworld.csv', encoding = 'utf-8') as writer:
    #     pass
    #     # df.to_csv(writer)


    # # # # Подключится к базе дaнных
    # # # conn = PostgresHook(postgres_conn_id="postgres_default").get_conn()
    # # # cur = conn.cursor()

    # logging.info(f'Успешный экспорт таблицы в bronze: {table_name}')


db_export_bronze(table_name='aisles')