import json
import logging
import os
# Imports pour Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Import pour les requêtes HTTP
import requests

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

spark = SparkSession.builder \
.appName('dpe_existants_rawdata_dl_job') \
.master('spark://spark-master:7077') \
.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
.config('spark.ui.port', '4041') \
.getOrCreate()



schema = StructType([
    StructField("Conso_chauffage_dépensier_é_finale", StringType(), True),
    StructField("Emission_GES_ECS", StringType(), True),
    StructField("Type_énergie_n°1", StringType(), True),
    StructField("Type_énergie_n°2", StringType(), True),
    StructField("Nom__commune_(BAN)", StringType(), True),
    StructField("Coût_ECS_énergie_n°2", StringType(), True),
    StructField("Emission_GES_chauffage", StringType(), True),
    StructField("Date_réception_DPE", StringType(), True),
    StructField("Coût_ECS_énergie_n°1", StringType(), True),
    StructField("Coût_total_5_usages", StringType(), True),
    StructField("Conso_ECS_é_finale", StringType(), True),
    StructField("Emission_GES_5_usages", StringType(), True),
    StructField("Code_postal_(BAN)", StringType(), True),
    StructField("Conso_éclairage_é_finale", StringType(), True),
    StructField("Coût_refroidissement_dépensier", StringType(), True),
    StructField("Coordonnée_cartographique_X_(BAN)", StringType(), True),
    StructField("Date_fin_validité_DPE", StringType(), True),
    StructField("Nombre_niveau_logement", StringType(), True),
    StructField("Emission_GES_refroidissement_dépensier", StringType(), True),
    StructField("Type_bâtiment", StringType(), True),
    StructField("Conso_5_usages_par_m²_é_primaire", StringType(), True),
    StructField("Coût_refroidissement", StringType(), True),
    StructField("Ubat_W/m²_K", StringType(), True),
    StructField("Coût_ECS_dépensier", StringType(), True),
    StructField("Coût_chauffage", StringType(), True),
    StructField("Emission_GES_auxiliaires", StringType(), True),
    StructField("Emission_GES_5_usages_par_m²", StringType(), True),
    StructField("Emission_GES_éclairage", StringType(), True),
    StructField("_geopoint", StringType(), True),
    StructField("Conso_ECS_dépensier_é_primaire", StringType(), True),
    StructField("Conso_refroidissement_dépensier_é_finale", StringType(), True),
    StructField("Conso_ECS_dépensier_é_finale", StringType(), True),
    StructField("Adresse_(BAN)", StringType(), True),
    StructField("Version_DPE", StringType(), True),
    StructField("Date_visite_diagnostiqueur", StringType(), True),
    StructField("Coût_ECS", StringType(), True),
    StructField("Nombre_niveau_immeuble", StringType(), True),
    StructField("Surface_habitable_immeuble", StringType(), True),
    StructField("Coût_éclairage", StringType(), True),
    StructField("Date_établissement_DPE", StringType(), True),
    StructField("Qualité_isolation_enveloppe", StringType(), True),
    StructField("N°_voie_(BAN)", StringType(), True),
    StructField("Emission_GES_chauffage_dépensier", StringType(), True),
    StructField("N°DPE", StringType(), True),
    StructField("Conso_refroidissement_é_finale", StringType(), True),
    StructField("Conso_chauffage_é_primaire", StringType(), True),
    StructField("Appartement_non_visité_(0/1)", StringType(), True),
    StructField("Adresse_brute", StringType(), True),
    StructField("Conso_éclairage_é_primaire", StringType(), True),
    StructField("Qualité_isolation_menuiseries", StringType(), True),
    StructField("Qualité_isolation_murs", StringType(), True),
    StructField("Conso_ECS_é_primaire", StringType(), True),
    StructField("Emission_GES_5_usages_énergie_n°1", StringType(), True),
    StructField("Emission_GES_5_usages_énergie_n°2", StringType(), True),
    StructField("Etiquette_GES", StringType(), True),
    StructField("Conso_5_usages_é_finale_énergie_n°1", StringType(), True),
    StructField("Statut_géocodage", StringType(), True),
    StructField("Conso_auxiliaires_é_primaire", StringType(), True),
    StructField("Nombre_appartement", StringType(), True),
    StructField("Conso_auxiliaires_é_finale", StringType(), True),
    StructField("Conso_chauffage_é_finale", StringType(), True),
    StructField("Coût_chauffage_dépensier", StringType(), True),
    StructField("Modèle_DPE", StringType(), True),
    StructField("Etiquette_DPE", StringType(), True),
    StructField("Conso_refroidissement_dépensier_é_primaire", StringType(), True),
    StructField("Nom__commune_(Brut)", StringType(), True),
    StructField("Conso_5_usages_é_finale", StringType(), True),
    StructField("N°_département_(BAN)", StringType(), True),
    StructField("Conso_refroidissement_é_primaire", StringType(), True),
    StructField("Coût_chauffage_énergie_n°1", StringType(), True),
    StructField("_i", IntegerType(), True),
    StructField("Méthode_application_DPE", StringType(), True),
    StructField("N°_région_(BAN)", StringType(), True),
    StructField("Coût_chauffage_énergie_n°2", StringType(), True),
    StructField("Qualité_isolation_plancher_bas", StringType(), True),
    StructField("Conso_5_usages/m²_é_finale", StringType(), True),
    StructField("Hauteur_sous-plafond", StringType(), True),
    StructField("Identifiant__BAN", StringType(), True),
    StructField("Surface_habitable_logement", StringType(), True),
    StructField("Code_postal_(brut)", StringType(), True),
    StructField("Coût_auxiliaires", StringType(), True),
    StructField("Coordonnée_cartographique_Y_(BAN)", StringType(), True),
    StructField("_rand", StringType(), True),
    StructField("Nom__rue_(BAN)", StringType(), True),
    StructField("Conso_chauffage_dépensier_é_primaire", StringType(), True),
    StructField("Emission_GES_ECS_dépensier", StringType(), True),
    StructField("Code_INSEE_(BAN)", StringType(), True),
    StructField("Score_BAN", StringType(), True),
    StructField("Emission_GES_refroidissement", StringType(), True),
    StructField("Conso_5_usages_é_primaire", StringType(), True),
    StructField("_score", StringType(), True),
    StructField("_id", StringType(), True)
])

result_file = '/opt/airflow/dags/data/in/dpe_existants_data.json'
data = []

if not os.path.exists(result_file):
    logger.error(f"Le fichier {result_file} n'existe pas.")
else:
    logger.info(f"Lecture du fichier JSON à partir de {result_file}")
    
    # Lecture du fichier JSON
    with open(result_file, 'r') as file:
        try:
            data = json.load(file)
            logger.info(f"Le fichier JSON a été lu avec succès. Nombre d'entrées: {len(data)}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Erreur lors de la lecture du fichier JSON: {e}")

df = spark.createDataFrame(data, schema) 

df = df.withColumn('Année', F.year(F.col('Date_établissement_DPE')))
df_filtered = df.filter(F.col('Année').isin(2021, 2022, 2023))

df_filtered = df_filtered.repartition('Année')

df_filtered.write.mode('append').partitionBy('Année').format('parquet') \
    .option("path", "hdfs:///hadoop/dfs/data/DPE/raw_data/dpe_logements_existants") \
    .saveAsTable("dpe_logements_existants")

# Stop the SparkSession
spark.stop()