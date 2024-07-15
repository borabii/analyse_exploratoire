from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("TestSparkConnection").getOrCreate()
    df = spark.createDataFrame([("Hello, Spark!",)], ["message"])
    df.show()
    spark.stop()

if __name__ == "__main__":
    main()
