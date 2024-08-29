import json
import os
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

import logging
import requests
from hdfs import InsecureClient

logger = logging.getLogger(__name__)


@dag(
    dag_id='pipeline_enedis_dl_import',
    start_date=None,  # pendulum.datetime(2024, 6, 29),
    max_active_runs=3,
    schedule=None,  # "@daily",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        'start_date': days_ago(1),
        "retries": 1,
    },
    catchup=False,
    tags=['enedis_rawdata','enedis']
)
def pipeline_enedis_dl_import():
    start = EmptyOperator(
        task_id='start'
    )
    end = EmptyOperator(
        task_id='end'
    )
    __date_conso = ['2023', '2022', '2021']

    @task_group()
    def pull_and_push():
        @task(task_id=f'fetch_from_api')
        def fetch_from_api():
            res = requests.get(
                f'https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-residentielle-par-adresse/exports/csv')
            res = res.json()
            return res

        @task(task_id=f'store__data_in_hdfs')
        def store_data_in_hdfs():

            client = InsecureClient('http://namenode:9870')

            tmp_file_path = f'/opt/airflow/dags/data/in/enedis_rawdata.csv' #for test remove at the end
            # with open(tmp_file_path, "w+") as file: #commented for test remove at the end
            #     json.dump(data, file)

            hdfs_file_path = f'/hadoop/dfs/data/enedis/raw_data/enedis_rawdata.csv'
            # Remove tmp file
            os.remove
            # Upload the file to HDFS
            client.upload(hdfs_file_path, tmp_file_path, overwrite=True)

        # file_data = fetch_from_api()
        store_data_in_hdfs()

    start >> pull_and_push() >> end


pipeline_enedis_dl_import()
