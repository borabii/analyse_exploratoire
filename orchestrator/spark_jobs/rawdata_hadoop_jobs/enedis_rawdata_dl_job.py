import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = SparkSession.builder \
.appName('enedis_rawdata_dl_job') \
.master('spark://spark-master:7077') \
.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
.config('spark.ui.port', '4041') \
.getOrCreate()

hdfs_json_path = f"hdfs://namenode:9000/hadoop/dfs/data/"


def read_data_from_hdfs(hdfs_path):
    """
    Reads data from HDFS and returns a Spark DataFrame.

    :param hdfs_path: The path to the JSON file in HDFS.
    :return: Spark DataFrame containing the data.
    """
    df_rawdata = spark.read.csv(path=f"{hdfs_path}/enedis/raw_data/enedis_rawdata.csv", sep=';', header=True, inferSchema=True)

    logger.info(f'DataFrame loaded with schema:{df_rawdata.schema.simpleString()} ')
    return df_rawdata

def transform_and_push_to_hdfs(df: DataFrame,hdfs_path):
    """
    transform_and_push_to_hdfs

    :param df: The new Spark DataFrame to be stored.
    
    """
    for date in ['2021','2022','2023']:
        
        filtred_df = df.filter(col("annee") == date)
        output = f"{hdfs_path}/enedis/raw_data/consommation-annuelle-residentielle_{date}.csv"
        
        # Write the filtered DataFrame to a CSV file in HDFS
        filtred_df.write.csv(
            path=output, 
            sep=';', 
            header=True, 
            mode='overwrite'
        )
        
        logger.info(f"Data procecced with total count: {filtred_df.count()}")

    return df

enedis_rawdata = read_data_from_hdfs(hdfs_json_path)
proceced_data = transform_and_push_to_hdfs(enedis_rawdata,hdfs_json_path)

# Stop the SparkSession
spark.stop()