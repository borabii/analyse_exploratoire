import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType, BooleanType)

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = SparkSession.builder \
.appName('bi_job_enedis_data') \
.master('spark://spark-master:7077') \
.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
.config('spark.ui.port', '4041') \
.getOrCreate()

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://postgres:5432/postgres"  

# Write DataFrame to PostgreSQL table
table_name = "bi_report_enedis_source"

def read_table_dwh(table_path,table):
    """
    Reads data from postgres and returns a Spark DataFrame.

    :param table_path: The path to the table in the dwh.
    :return: Spark DataFrame containing the data.
    """
    jdbc_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
    # create an Empty DataFrame object
    df = spark.read \
    .jdbc(url=table_path, table=table, properties=jdbc_properties)
    logger.info(f'DataFrame loaded with schema:{df.printSchema()}')
    return df

def trasforme(df):
    """
    transforms data from postgres and returns a Spark DataFrame.

    :param df: data
    :return: Spark DataFrame containing the data.
    """
    logger.info(f'DataFrame loaded with schema:{df.printSchema()}')
    return df

def store_processed_in_postgres(df: DataFrame, postgres_url: str, table_name: str):
    """
    Stores a Spark DataFrame into a PostgreSQL table with incremental logic (insert and update).

    :param df: The new Spark DataFrame to be stored.
    :param postgres_url: JDBC URL for the PostgreSQL database.
    :param table_name: The name of the table in PostgreSQL.
    """
    jdbc_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
    df.write \
        .mode('overwrite') \
        .jdbc(url=postgres_url, table=table_name, properties=jdbc_properties)
    logger.info("Data written to postgre")

bi_enedis_rawdata = read_table_dwh(postgres_url,table_name)
store_processed_in_postgres(bi_enedis_rawdata, postgres_url, table_name)

# Stop the SparkSession
spark.stop()