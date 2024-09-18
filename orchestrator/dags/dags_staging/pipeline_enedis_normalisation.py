import logging
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow import DAG
logger = logging.getLogger(__name__)


dag = DAG(
    dag_id='pipeline_enedis_normalisation',
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
    tags=['enedis_normalisation','enedis']
)
def pipeline_enedis_normalisation():
    
    start = EmptyOperator(
        task_id='start'
    )

    python_job = SparkSubmitOperator(
        task_id="python_job",
        conn_id="spark_connection",
        application="/opt/airflow/spark_jobs/rawdata_hadoop_jobs/enedis_rawdata_normalisation_job.py",
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

    end = EmptyOperator(
        task_id='end'
    )
    start >> python_job >> end


pipeline_enedis_normalisation()
