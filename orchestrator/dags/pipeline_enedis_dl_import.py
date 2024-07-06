import json
import os
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
import logging
import requests
from hdfs import InsecureClient
from airflow.operators.python import get_current_context
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
        "retries": 1,
    },
    catchup=False,
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
        for date in __date_conso:
            @task(task_id=f'fetch_from_api_{date}')
            def fetch_from_api(date=date):
                res = requests.get(
                    f'https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-residentielle-par-adresse/records?limit=20&refine=annee%3A%22{date}%22')
                res = res.json()
                res = res['results']
                logger.info(f'date export conso : {date}:::::{res}')
                return res

            @task(task_id=f'store__{date}_data_in_hdfs')
            def store_data_in_hdfs(data, date=date):

                client = InsecureClient('http://namenode:50070')

                tmp_file_path = f'/opt/airflow/dags/data/in/consommation-annuelle-residentielle_{date}.json'
                with open(tmp_file_path, "w+") as file:
                    json.dump(data, file)

                hdfs_file_path = f'/hadoop/dfs/data/consommation-annuelle-residentielle_{date}.json'
                # Remove tmp file
                os.remove
                # Upload the file to HDFS
                client.upload(hdfs_file_path, tmp_file_path, overwrite=True)

            file_data = fetch_from_api()
            store_data_in_hdfs(file_data)

    start >> pull_and_push() >> end


pipeline_enedis_dl_import()
