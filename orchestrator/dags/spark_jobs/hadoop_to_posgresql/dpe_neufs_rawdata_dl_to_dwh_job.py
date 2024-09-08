import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType, BooleanType)

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = SparkSession.builder \
.appName('dpe_neufs_rawdata_dl_to_dwh_job') \
.master('spark://spark-master:7077') \
.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
.config('spark.ui.port', '4041') \
.getOrCreate()

hdfs_json_path = f"hdfs://namenode:9000/hadoop/dfs/data/"

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://postgres:5432/postgres"  

# Write DataFrame to PostgreSQL table
table_name = "dpe_logement_neufs"

def read_data_from_hdfs(hdfs_path):
    """
    Reads data from HDFS and returns a Spark DataFrame.

    :param hdfs_path: The path to the JSON file in HDFS.
    :return: Spark DataFrame containing the data.
    """
    df = None
    for lot in ['lot','lot_1','lot_2']:
        df1 = spark.read.parquet(f'{hdfs_path}/DPE/raw_data/dpe_logements_neufs/{lot}')
        if df is None:
            df = df1  # Initialize the df for the first iteration
        else:
            df = df.union(df1)  # Union for subsequent DataFrames
        logger.info(f'DataFrame loaded with schema:{df.printSchema()} ')
    
    return df

def store_data_in_postgres(df: DataFrame, postgres_url: str, table_name: str):
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

dpe = read_data_from_hdfs(hdfs_json_path)
store_data_in_postgres(dpe, postgres_url, table_name)

# Stop the SparkSession
spark.stop()