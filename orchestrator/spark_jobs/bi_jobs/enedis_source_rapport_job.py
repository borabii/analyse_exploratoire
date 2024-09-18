from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
.appName('enedis_rawdata_job') \
.master('spark://spark-master:7077') \
.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
.config('spark.ui.port', '4041') \
.getOrCreate()

hdfs_json_path = "hdfs://namenode:9000/hadoop/dfs/data/enedis/consommation-annuelle-residentielle_2023.json"

# Read the JSON file into a DataFrame
df = spark.read.json(hdfs_json_path)   


df.head()



# Stop the SparkSession
spark.stop()