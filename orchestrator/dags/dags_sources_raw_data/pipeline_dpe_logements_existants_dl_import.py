
from datetime import timedelta
import json
import os
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator
import logging

import pendulum
import requests


logger = logging.getLogger(__name__)


def fetch_from_api():
        url_file_path = '/opt/airflow/dags/data/out/next_url_existants.txt'

        # Lire l'URL précédente depuis le fichier, si elle existe
        if os.path.exists(url_file_path):
            with open(url_file_path, 'r') as file:
                url = file.read().strip()
        else:
            url = 'https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?page=1&size=10000'
        
        all_results = []
        max_calls = 10 # Number of API calls to make
        call_count = 0  # Counter for the number of API calls
        # Loop to handle pagination
        while url and call_count < max_calls:
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
                    call_count += 1
                    print(f"Call {call_count} completed. Next URL: {url}")
                    
                    # Check if we have made 10 API calls
                    if call_count >= max_calls:
                        print("Reached maximum number of API calls. Stopping fetch.")
                        with open(url_file_path, 'w') as file:
                            file.write(url)
                        break

                except ValueError as e:
                    logger.error(f"Error parsing JSON: {e}")
                    break
            else:
                logger.error(f"Failed to fetch data. Status code: {response.status_code}")
                logger.error(f"Response content: {response.text}")
                break
        # Crée un fichier temporaire contenant les résultats
        result_file = '/opt/airflow/dags/data/in/dpe_existants_data.json'
        with open(result_file, 'w') as f:
            json.dump(all_results, f)
        return all_results

def delete_tmp_file():
    file = '/opt/airflow/dags/data/in/dpe_existants_data.json'
    os.remove(file)

dag = DAG(
    dag_id='pipeline_dpe_logements_existants_dl_import',
    start_date=pendulum.datetime(2024, 9, 17),
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
    tags=['dpe_existants_rawdata','dpe']
)
def pipeline_dpe_logements_existants_dl_import():
    
    start = PythonOperator(
        task_id="start",
        python_callable = lambda: print("Jobs started"),
        dag=dag
    )

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable = fetch_from_api,
        dag=dag,
        execution_timeout=timedelta(minutes=30)
    )

    python_job = SparkSubmitOperator(
        task_id="python_job",
        conn_id="spark_connection",
        application="/opt/airflow/spark_jobs/rawdata_hadoop_jobs/dpe_existants_rawdata_dl_job.py",
        conf={
            'spark.yarn.submit.waitAppCompletion': 'false',
            'spark.master': 'spark://spark-master:7077',
            'spark.jars': '/opt/airflow/jars/postgresql-42.7.3.jar'
        },
        env_vars={
        'HADOOP_CONF_DIR': '/opt/hadoop/conf',
        'YARN_CONF_DIR': '/opt/hadoop/conf',
        },
        dag=dag
    )

    end = PythonOperator(
        task_id="end",
        python_callable = delete_tmp_file,
        dag=dag
    )

    start >> fetch_data >> python_job >> end


pipeline_dpe_logements_existants_dl_import()
