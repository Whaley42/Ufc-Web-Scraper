from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from datetime import timedelta
from postscrape.spiders.Main import run
from HelperClasses.Transform import transform_data
from HelperClasses.GoogleConnector import connect
import datetime
import pandas as pd




def update():
    """
    Update the user refresh token and send them a monthly an email update of their most
    listened to songs, artists, and genres. Also create a new Spotify playlist based
    on their recent listens.
    """

   


with DAG(
    dag_id="Airflow-Ufc",
    schedule_interval="0 10 * * 0",
    catchup=False,
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime.datetime(2023, 1, 25)
    }) as dag:
        t1 = PythonOperator(
        task_id="ScrapeUfc",
        # "scripts" folder is under "/usr/local/airflow/dags"
        python_callable =run,
        dag=dag
        )
        t2 = PythonOperator(
            task_id="TransformData",
            python_callable=transform_data,
            dag=dag
        )
        t3 = PythonOperator(
            task_id="GoogleConnector",
            python_callable=connect,
            dag=dag
        )
        t1 >> t2 >> t3

        




