import json
import os
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

import logging
import requests
from hdfs import InsecureClient

logger = logging.getLogger(__name__)


@dag(
    dag_id='pipeline_dpe_logements_existants_dl_import',
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
    catchup=False
)
def pipeline_dpe_logements_existants_dl_import():
    start = EmptyOperator(
        task_id='start'
    )
    end = EmptyOperator(
        task_id='end'
    )

    @task_group()
    def pull_and_push():
    
        @task(task_id=f'fetch_from_api_logements_neufs')
        def fetch_from_api():
            url = 'https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines'
            
            all_results = []

            # Loop to handle pagination
            while url:
                # Fetch the JSON data from the URL
                response = requests.get(url)
                
                # Check if the request was successful
                if response.status_code == 200:
                    try:
                        # Parse the JSON data
                        data = response.json()
                        
                        # Append the results to the all_results list
                        all_results.extend(data['results'])
                        
                        # Update the URL to the next page
                        url = data.get('next')
                        logger.info(f'This is the next URL: {url}')
                        logger.info(f'This is the dataL: {data}')
                        if len(all_results) > 1000:
                            logger.info(f"Collected more than 1000 results, stopping fetch.")
                            break
                    except ValueError as e:
                        logger.error(f"Error parsing JSON: {e}")
                        break
                else:
                    logger.error(f"Failed to fetch data. Status code: {response.status_code}")
                    logger.error(f"Response content: {response.text}")
                    break
            
            return all_results

        @task(task_id=f'store___data_in_hdfs')
        def store_data_in_hdfs(data):
            logger.info(f'data from hdf fn {data}')
            client = InsecureClient('http://namenode:50070')
    
            tmp_file_path = f'/opt/airflow/dags/data/in/dpe_logements_existants.json'
            with open(tmp_file_path, "w+") as file:
                json.dump(data, file)

            hdfs_file_path = f'/hadoop/dfs/data/DPE/dpe_logements_existants.json'
            
            # Upload the file to HDFS
            client.upload(hdfs_file_path, tmp_file_path, overwrite=True)

            # Remove tmp file
            os.remove(tmp_file_path)

        file_data = fetch_from_api()
        store_data_in_hdfs(file_data)

    start >> pull_and_push() >> end


pipeline_dpe_logements_existants_dl_import()
