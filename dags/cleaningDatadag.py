import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

defaults_args = {
    'owner': 'radtrz',
    'start_date': dt.datetime(2023, 10, 9),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}


def cleanScooter():
    df = pd.read_csv('/home/radtrz/Programming/DataEngineering/DataExploration/scooter.csv')
    df.drop(columns=['region_id'], inplace=True)
    df.columns=[x.lower() for x in df.columns]
    df['started_at'] = pd.to_datetime(df['started_at'], format='%m/%d/%Y %H:%M')
    df.to_csv('/home/radtrz/Programming/DataEngineering/DataExploration/cleanscooter.csv')


def filterData():
    df=pd.read_csv('/home/radtrz/Programming/DataEngineering/DataExploration/cleanscooter.csv')
    fromd = '2019-05-23'
    tod = '2019-06-03'
    tofrom = df[(df['started_at']>fromd) & (df['started_at']<tod)]
    tofrom.to_csv('/home/radtrz/Programming/DataEngineering/DataExploration/may23-june3.csv')


with DAG('CleanData', 
         default_args=defaults_args,
         schedule=timedelta(minutes=5),
         # '0 * * * *',
        ) as dag:
    
    cleanData = PythonOperator(task_id='clean',
                               python_callable=cleanScooter)
    
    selectData = PythonOperator(task_id='filter',
                                python_callable=filterData)
    
    copyFile = BashOperator(task_id='copy',
                            bash_command='cp /home/radtrz/Programming/DataEngineering/DataExploration/may23-june3.csv /home/radtrz/Pulpit')
    
    cleanData >> selectData >> copyFile