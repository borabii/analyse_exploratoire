from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lower, regexp_replace,concat

spark = SparkSession.builder \
.appName('dpe_existants_rawdata_dl_job') \
.master('spark://spark-master:7077') \
.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
.config('spark.ui.port', '4041') \
.getOrCreate()

ban = spark.read.csv('hdfs:///hadoop/dfs/data/base-adresse-national/',sep=';',header=True)
ban_normaliser = ban.withColumn("adresse_normalisee_insee", concat_ws(" ",
    col("numero"),
    col("nom_voie"),
    col("code_insee"),
    col("nom_commune")
))
ban_normaliser = ban_normaliser.withColumn("adresse_normalisee_insee", lower(regexp_replace(col("adresse_normalisee_insee"), r"\s+", " ")))

for date in ['2021','2022','2023']:
    enedis = spark.read.csv(f'hdfs:///hadoop/dfs/data/enedis/raw_data/consommation-annuelle-residentielle_{date}.csv',sep=';',header=True)
    enedis_normaliser = enedis.withColumn("adresse_brute", concat_ws(" ",
        col("adresse"),
        col("code_commune"),
        col("nom_commune")))
    #formater les adresses
    enedis_normaliser = enedis_normaliser.withColumn("adresse_brute", lower(regexp_replace(col("adresse_brute"), r"\s+", " ")))
    # jointure
    enedis_join_ban = enedis_normaliser.join(ban_normaliser.select("adresse_normalisee_insee", "id"), enedis_normaliser.adresse_brute == ban_normaliser.adresse_normalisee_insee, how="inner")

    # Écrire le DataFrame en un seul fichier CSV 
    enedis_join_ban.write.mode('overwrite').format('csv') \
        .option("path", f"hdfs:///hadoop/dfs/data/enedis/staging/consommation-annuelle-residentielle_{date}.csv") \
        .option("sep", ";")  \
        .option("header", "true").save()

spark.stop()