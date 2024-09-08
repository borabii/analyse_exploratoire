import json
import os
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
import gzip
import shutil
import logging
import requests
from hdfs import InsecureClient

logger = logging.getLogger(__name__)


@dag(
    dag_id='pipeline_ban_dl_import',
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
    tags=['ban_rawdata','ban']
)
def pipeline_ban_dl_import():
    __departement_codes = [
    '01', '02', '03', '04', '05', '06', '07', '08', '09',
    '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
    '2A', '2B', 
    '21', '22', '23', '24', '25', '26', '27', '28', '29',
    '30', '31', '32', '33', '34', '35', '36', '37', '38', '39',
    '40', '41', '42', '43', '44', '45', '46', '47', '48', '49',
    '50', '51', '52', '53', '54', '55', '56', '57', '58', '59',
    '60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
    '70', '71', '72', '73', '74', '75', '76', '77', '78', '79',
    '80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
    '90', '91', '92', '93', '94', '95',
    '90','91', '92'
    ]
    start = EmptyOperator(
        task_id='start'
    )
    end = EmptyOperator(
        task_id='end'
    )
    
    @task_group()
    def pull_and_push():
        for dep in __departement_codes:
            @task(task_id=f'fetch_from_api_{dep}')
            def fetch_from_api(dep=dep):
                res = requests.get(
                    f'https://adresse.data.gouv.fr/data/ban/adresses/weekly/csv/adresses-{dep}.csv.gz')
                logger.info(f'departement {dep} export: :::::')
                if res.status_code == 200:
                    # Paths for the gz file and the output csv file
                    gz_file_path = f'/opt/airflow/dags/data/in/adresses-{dep}.csv.gz'
                    # csv_file_path = f'/opt/airflow/dags/data/in/adresses-{dep}.csv'
                    
                    # Save the gzipped file
                    with open(gz_file_path, 'wb') as gz_file:
                        gz_file.write(res.content)
                    
                    # # Extract the gz file to get the CSV file
                    # with gzip.open(gz_file_path, 'rb') as f_in:
                    #     with open(csv_file_path, 'wb') as f_out:
                    #         shutil.copyfileobj(f_in, f_out)
                    
                    # # Remove the gz file after extraction (optional)
                    # os.remove(gz_file_path)
                    
                    logger.info(f'Departement {dep} export: CSV saved to {gz_file_path}')
                
                    return gz_file_path
                else:
                    logger.error(f'Failed to fetch the file for departement {dep}: {res.status_code}')
                return None

            @task(task_id=f'store__{dep}_data_in_hdfs')
            def store_data_in_hdfs(dep=dep):

                client = InsecureClient('http://namenode:9870')

                tmp_file_path = f'/opt/airflow/dags/data/in/adresses-{dep}.csv.gz'
                
                hdfs_file_path = f'/hadoop/dfs/data/base-adresse-national/'
                # Remove tmp file
                os.remove
                # Upload the file to HDFS
                client.upload(hdfs_file_path, tmp_file_path, overwrite=True)

            fetch_from_api()
            store_data_in_hdfs()

    start >> pull_and_push() >> end

pipeline_ban_dl_import()