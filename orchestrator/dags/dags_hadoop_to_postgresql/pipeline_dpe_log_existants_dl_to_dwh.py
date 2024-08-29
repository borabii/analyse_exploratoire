import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)


dag = DAG(
    dag_id='pipeline_dpe_log_existants_dl_to_dwh',
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
    catchup=False
)
def pipeline_dpe_log_existants_dl_to_dwh():
    
    start = PythonOperator(
        task_id="start",
        python_callable = lambda: print("Jobs started"),
        dag=dag
    )

    python_job = SparkSubmitOperator(
        task_id="python_job",
        conn_id="spark_connection",
        application="/opt/airflow/dags/spark_jobs/hadoop_to_posgresql/dpe_existants_rawdata_dl_to_dwh_job.py",
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
        python_callable = lambda: print("Jobs completed successfully"),
        dag=dag
    )

    start >> python_job >> end


pipeline_dpe_log_existants_dl_to_dwh()
