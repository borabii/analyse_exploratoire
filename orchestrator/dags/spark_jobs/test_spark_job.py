from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Standalone Spark Connection Test') \
    .master('spark://<master-ip>:7077') \
    .getOrCreate()

df = spark.range(1, 10)
df.show()

spark.stop()
