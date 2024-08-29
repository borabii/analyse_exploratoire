import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = SparkSession.builder \
.appName('enedis_rawdata_job') \
.master('spark://spark-master:7077') \
.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
.config('spark.ui.port', '4041') \
.getOrCreate()

hdfs_json_path = f"hdfs://namenode:9000/hadoop/dfs/data/"

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://postgres:5432/postgres"  

__annee_conso = ['2021','2022','2023']

# Write DataFrame to PostgreSQL table
table_name = "rawdata_enedis"

# Define the schema based on the provided structure
schema = StructType([
    StructField("adresse", StringType(), True),
    StructField("annee", StringType(), True),
    StructField("code_commune", StringType(), True),
    StructField("code_departement", StringType(), True),
    StructField("code_epci", StringType(), True),
    StructField("code_iris", StringType(), True),
    StructField("code_region", StringType(), True),
    StructField("consommation_annuelle_moyenne_de_la_commune_mwh", DoubleType(), True),
    StructField("consommation_annuelle_moyenne_par_site_de_l_adresse_mwh", DoubleType(), True),
    StructField("consommation_annuelle_totale_de_l_adresse_mwh", DoubleType(), True),
    StructField("indice_de_repetition", StringType(), True),
    StructField("libelle_de_voie", StringType(), True),
    StructField("nom_commune", StringType(), True),
    StructField("nom_iris", StringType(), True),
    StructField("nombre_de_logements", LongType(), True),
    StructField("numero_de_voie", LongType(), True),
    StructField("segment_de_client", StringType(), True),
    StructField("tri_des_adresses", LongType(), True),
    StructField("type_de_voie", StringType(), True)
])

def read_data_from_hdfs(hdfs_path):
    """
    Reads data from HDFS and returns a Spark DataFrame.

    :param hdfs_path: The path to the JSON file in HDFS.
    :return: Spark DataFrame containing the data.
    """
    # create an Empty DataFrame object
    df = spark.createDataFrame([], schema=schema)
    for annee in __annee_conso:
        df1 = spark.read.json(f'{hdfs_path}enedis/consommation-annuelle-residentielle_{annee}.json')
        df = df.union(df1)

    logger.info(f'DataFrame loaded with schema:{df.schema.simpleString()} ')
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

enedis_rawdata = read_data_from_hdfs(hdfs_json_path)
store_data_in_postgres(enedis_rawdata, postgres_url, table_name)

# Stop the SparkSession
spark.stop()