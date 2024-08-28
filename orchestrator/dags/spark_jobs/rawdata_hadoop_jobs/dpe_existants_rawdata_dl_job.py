import logging
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

def fetch_from_api():
        url = 'https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?page=1&size=10'
        
        all_results = []
        max_calls = 1  # Number of API calls to make
        call_count = 0  # Counter for the number of API calls
        # Loop to handle pagination
        while url and call_count < max_calls:
        # Fetch the JSON data from the URL
            response = requests.get(url)
            
            # Check if the request was successful
            if response.status_code == 200:
                try:
                    # Parse the JSON data
                    data = response.json()
                    
                    # Append the results to the all_results list
                    all_results.extend(data['results'])
                    
                    # Update the URL to the next page
                    url = data.get('next')
                    call_count += 1
                    print(f"Call {call_count} completed. Next URL: {url}")
                    
                    # Check if we have made 10 API calls
                    if call_count >= max_calls:
                        print("Reached maximum number of API calls. Stopping fetch.")
                        break

                except ValueError as e:
                    logger.error(f"Error parsing JSON: {e}")
                    break
            else:
                logger.error(f"Failed to fetch data. Status code: {response.status_code}")
                logger.error(f"Response content: {response.text}")
                break
        
        return all_results

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

data = fetch_from_api()

df = spark.createDataFrame(data, schema) 

df = df.withColumn('Année', F.year(F.col('Date_établissement_DPE')))
df_filtered = df.filter(F.col('Année').isin(2021, 2022, 2023))

df_filtered = df_filtered.repartition('Année')

df_filtered.write.mode('overwrite').partitionBy('Année').format('parquet') \
    .option("path", "hdfs:///hadoop/dfs/data/DPE/raw_data/dpe_logements_existants") \
    .saveAsTable("dpe_logements_existants")

# Stop the SparkSession
spark.stop()