import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType, BooleanType)

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = SparkSession.builder \
.appName('dpe_existants_rawdata_dl_to_dwh_job') \
.master('spark://spark-master:7077') \
.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
.config('spark.ui.port', '4041') \
.getOrCreate()

hdfs_json_path = f"hdfs://namenode:9000/hadoop/dfs/data/"

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://postgres:5432/postgres"  

# Write DataFrame to PostgreSQL table
table_name = "rawdata_dpe_logement_existants"

# Define the schema based on the provided structure
schema = StructType([
    StructField("Adresse_(BAN)", StringType(), True),
    StructField("Adresse_brute", StringType(), True),
    StructField("Appartement_non_visité_(0/1)", BooleanType(), True),
    StructField("Besoin_ECS", StringType(), True),
    StructField("Besoin_chauffage", LongType(), True),
    StructField("Besoin_refroidissement", StringType(), True),
    StructField("Cage_d'escalier", StringType(), True),
    StructField("Catégorie_ENR", StringType(), True),
    StructField("Classe_altitude", StringType(), True),
    StructField("Clé_répartition_chauffage", DoubleType(), True),
    StructField("Code_INSEE_(BAN)", StringType(), True),
    StructField("Code_postal_(BAN)", StringType(), True),
    StructField("Code_postal_(brut)", StringType(), True),
    StructField("Complément_d'adresse_bâtiment", StringType(), True),
    StructField("Complément_d'adresse_logement", StringType(), True),
    StructField("Conso_5_usages/m²_é_finale", DoubleType(), True),
    StructField("Conso_5_usages_par_m²_é_primaire", DoubleType(), True),
    StructField("Conso_5_usages_é_finale", DoubleType(), True),
    StructField("Conso_5_usages_é_finale_énergie_n°1", DoubleType(), True),
    StructField("Conso_5_usages_é_primaire", DoubleType(), True),
    StructField("Conso_ECS_dépensier_é_finale", DoubleType(), True),
    StructField("Conso_ECS_dépensier_é_primaire", DoubleType(), True),
    StructField("Conso_ECS_é_finale", DoubleType(), True),
    StructField("Conso_ECS_é_primaire", DoubleType(), True),
    StructField("Conso_auxiliaires_é_finale", DoubleType(), True),
    StructField("Conso_auxiliaires_é_primaire", DoubleType(), True),
    StructField("Conso_chauffage_dépensier_é_finale", DoubleType(), True),
    StructField("Conso_chauffage_dépensier_é_primaire", DoubleType(), True),
    StructField("Conso_chauffage_é_finale", DoubleType(), True),
    StructField("Conso_chauffage_é_primaire", DoubleType(), True),
    StructField("Conso_refroidissement_dépensier_é_finale", DoubleType(), True),
    StructField("Conso_refroidissement_dépensier_é_primaire", DoubleType(), True),
    StructField("Conso_refroidissement_é_finale", DoubleType(), True),
    StructField("Conso_refroidissement_é_primaire", DoubleType(), True),
    StructField("Conso_éclairage_é_finale", DoubleType(), True),
    StructField("Conso_éclairage_é_primaire", DoubleType(), True),
    StructField("Coordonnée_cartographique_X_(BAN)", DoubleType(), True),
    StructField("Coordonnée_cartographique_Y_(BAN)", DoubleType(), True),
    StructField("Coût_ECS", DoubleType(), True),
    StructField("Coût_ECS_dépensier", DoubleType(), True),
    StructField("Coût_ECS_énergie_n°1", DoubleType(), True),
    StructField("Coût_ECS_énergie_n°2", DoubleType(), True),
    StructField("Coût_auxiliaires", DoubleType(), True),
    StructField("Coût_chauffage", DoubleType(), True),
    StructField("Coût_chauffage_dépensier", DoubleType(), True),
    StructField("Coût_chauffage_énergie_n°1", DoubleType(), True),
    StructField("Coût_chauffage_énergie_n°2", DoubleType(), True),
    StructField("Coût_refroidissement", DoubleType(), True),
    StructField("Coût_refroidissement_dépensier", DoubleType(), True),
    StructField("Coût_total_5_usages", DoubleType(), True),
    StructField("Coût_éclairage", DoubleType(), True),
    StructField("Date_fin_validité_DPE", StringType(), True),
    StructField("Date_réception_DPE", StringType(), True),
    StructField("Date_visite_diagnostiqueur", StringType(), True),
    StructField("Date_établissement_DPE", StringType(), True),
    StructField("Deperditions_baies_vitrées", DoubleType(), True),
    StructField("Deperditions_enveloppe", DoubleType(), True),
    StructField("Deperditions_planchers_bas", DoubleType(), True),
    StructField("Deperditions_planchers_hauts", DoubleType(), True),
    StructField("Déperditions_murs", DoubleType(), True),
    StructField("Déperditions_ponts_thermiques", DoubleType(), True),
    StructField("Déperditions_portes", DoubleType(), True),
    StructField("Déperditions_renouvellement_air", DoubleType(), True),
    StructField("Déperditions_totales_bâtiment", StringType(), True),
    StructField("Déperditions_totales_logement", StringType(), True),
    StructField("Emission_GES_5_usages", DoubleType(), True),
    StructField("Emission_GES_5_usages_par_m²", DoubleType(), True),
    StructField("Emission_GES_5_usages_énergie_n°1", DoubleType(), True),
    StructField("Emission_GES_5_usages_énergie_n°2", DoubleType(), True),
    StructField("Emission_GES_ECS", DoubleType(), True),
    StructField("Emission_GES_ECS_dépensier", DoubleType(), True),
    StructField("Emission_GES_auxiliaires", DoubleType(), True),
    StructField("Emission_GES_chauffage", DoubleType(), True),
    StructField("Emission_GES_chauffage_dépensier", DoubleType(), True),
    StructField("Emission_GES_refroidissement", DoubleType(), True),
    StructField("Emission_GES_refroidissement_dépensier", DoubleType(), True),
    StructField("Emission_GES_éclairage", DoubleType(), True),
    StructField("Etiquette_DPE", StringType(), True),
    StructField("Etiquette_GES", StringType(), True),
    StructField("Hauteur_sous-plafond", DoubleType(), True),
    StructField("Identifiant__BAN", StringType(), True),
    StructField("Indicateur_confort_été", StringType(), True),
    StructField("Inertie_lourde_(0/1)", BooleanType(), True),
    StructField("Invariant_fiscal_logement", StringType(), True),
    StructField("Isolation_toiture_(0/1)", BooleanType(), True),
    StructField("Logement_traversant_(0/1)", BooleanType(), True),
    StructField("Modèle_DPE", StringType(), True),
    StructField("Méthode_application_DPE", StringType(), True),
    StructField("Nom__commune_(BAN)", StringType(), True),
    StructField("Nom__commune_(Brut)", StringType(), True),
    StructField("Nom__rue_(BAN)", StringType(), True),
    StructField("Nom_résidence", StringType(), True),
    StructField("Nombre_appartement", LongType(), True),
    StructField("Nombre_niveau_immeuble", LongType(), True),
    StructField("Nombre_niveau_logement", LongType(), True),
    StructField("N°DPE", StringType(), True),
    StructField("N°_DPE_immeuble_associé", StringType(), True),
    StructField("N°_DPE_remplacé", StringType(), True),
    StructField("N°_département_(BAN)", StringType(), True),
    StructField("N°_région_(BAN)", StringType(), True),
    StructField("N°_voie_(BAN)", StringType(), True),
    StructField("N°_étage_appartement", LongType(), True),
    StructField("Position_logement_dans_immeuble", StringType(), True),
    StructField("Protection_solaire_exterieure_(0/1)", BooleanType(), True),
    StructField("Présence_brasseur_air_(0/1)", BooleanType(), True),
    StructField("Qualité_isolation_enveloppe", StringType(), True),
    StructField("Qualité_isolation_menuiseries", StringType(), True),
    StructField("Qualité_isolation_murs", StringType(), True),
    StructField("Qualité_isolation_plancher_bas", StringType(), True),
    StructField("Qualité_isolation_plancher_haut_comble_aménagé", StringType(), True),
    StructField("Score_BAN", DoubleType(), True),
    StructField("Statut_géocodage", StringType(), True),
    StructField("Surface_habitable_immeuble", DoubleType(), True),
    StructField("Surface_habitable_logement", DoubleType(), True),
    StructField("Type_bâtiment", StringType(), True),
    StructField("Type_installation_ECS_(général)", StringType(), True),
    StructField("Type_installation_chauffage", StringType(), True),
    StructField("Type_énergie_n°1", StringType(), True),
    StructField("Type_énergie_n°2", StringType(), True),
    StructField("Type_énergie_principale_chauffage", StringType(), True),
    StructField("Typologie_logement", StringType(), True),
    StructField("Ubat_W/m²_K", DoubleType(), True),
    StructField("Version_DPE", DoubleType(), True),
    StructField("Zone_climatique_", StringType(), True),
    StructField("_geopoint", StringType(), True),
    StructField("_i", LongType(), True),
    StructField("_id", StringType(), True),
    StructField("_rand", LongType(), True),
    StructField("_score", StringType(), True)
])

def read_data_from_hdfs(hdfs_path):
    """
    Reads data from HDFS and returns a Spark DataFrame.

    :param hdfs_path: The path to the JSON file in HDFS.
    :return: Spark DataFrame containing the data.
    """
    # create an Empty DataFrame object
    df = spark.createDataFrame([], schema=schema)
    df = spark.read.json(f'{hdfs_path}DPE/dpe_logements_existants.json')

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