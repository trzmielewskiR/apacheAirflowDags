import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

default_args = {
    'owner': 'radtrz',
    'start_date': dt.datetime(2023,10,1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}


def queryPostgresql():
    conn_string = "dbname='dataengineering' host='localhost' user='postgres' password='postgres'"
    conn = db.connect(conn_string)
    df = pd.read_sql("Select name, city from users", conn)
    df.to_csv('postgresqldata.csv')
    print('---------DataSaved--------')


def insertElasticsearch():
    es = Elasticsearch(hosts=[{"host": "127.0.0.1", "port": 9200, "scheme": "http"}])
    df = pd.read_csv('postgresqldata.csv')
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index='frompostgresql', document=doc)
        print(res)


with DAG('MYDBDAG',
         default_args=default_args,
         schedule=timedelta(minutes=5),
         # '0 * * * *',
         ) as dag:
    
    getData = PythonOperator(task_id='QueryPostGreSQL',
                             python_callable=queryPostgresql)
    
    insertData = PythonOperator(task_id='InsertDataElasticSearch',
                                python_callable=insertElasticsearch)
    
    getData >> insertData