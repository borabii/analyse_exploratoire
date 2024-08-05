from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    
   
}

# Define the DAG
dag = DAG(
    'test_spark_connection',
    default_args=default_args,
    description='A simple DAG to test Spark connection',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the SparkSubmitOperator
test_spark_connection = SparkSubmitOperator(
    task_id='test_spark_connection',
    application='/opt/airflow/dags/spark_jobs/test_spark_job.py',  
    conn_id='spark_connection',
    verbose=True,
    dag=dag,
)

# Set the task in the DAG
test_spark_connection
