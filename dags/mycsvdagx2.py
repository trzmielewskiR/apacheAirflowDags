import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd

def csvToJson():
    df = pd.read_csv('/home/radtrz/Programming/DataEngineering/data.csv')
    for i, r in df.iterrows():
        print(r['name'])
    df.to_json('fromAirflow.json', orient='records')


default_args = {
    'owner': 'radtrz',
    'start_date': dt.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}


with DAG('MYFIRSTDAG', 
         default_args=default_args,
         schedule=timedelta(minutes=1),
         # '0 * * * *',
         ) as dag:
    print_starting = BashOperator(task_id = 'starting',
                                  bash_command='echo "I am reading the CSV now ..."')
    
    CSVJson = PythonOperator(task_id = 'convertCSVToJson',
                             python_callable=csvToJson)
    
    print_starting .set_downstream(CSVJson)
    print_starting >> CSVJson