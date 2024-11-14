# -*- coding: utf-8 -*-
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configurer l'encodage pour UTF-8
reload(sys)
sys.setdefaultencoding('utf-8')


def initialize_spark_session():
    """
    Initialise une session Spark avec support Hive.
    """
    print("Initialisation de la session Spark avec support Hive...")
    spark = SparkSession.builder \
        .appName("Extraction Précise de Marques, Modèles et Puissance Uniformisée dans CO2") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")  # Réduire le niveau de journalisation
    print("Session Spark initialisée avec succès.\n")
    return spark


def clean_encoding(df):
    """
    Nettoie les erreurs d'encodage (comme Ã© au lieu de é) et les guillemets présents dans la colonne 'modele'.
    Remplace les valeurs nulles par "Inconnu".
    """
    return df.withColumn("modele", F.regexp_replace(F.col("modele"), "Ã©", "é")) \
        .withColumn("modele", F.regexp_replace(F.col("modele"), r'^"|"$', '')) \
        .withColumn("modele", F.when(F.col("modele").isNull(), "Inconnu").otherwise(F.col("modele")))

def clean_data(df):
    """
    Nettoie les colonnes 'bonus_malus' et 'cout_energie' pour supprimer les symboles monétaires et convertir en float.
    Remplace les valeurs '0' dans 'rejets_co2' par NaN pour signaler des données manquantes.
    """
    df = df.withColumn("bonus_malus", F.regexp_replace("bonus_malus", r'€.*', ''))
    df = df.withColumn("bonus_malus", F.regexp_replace("bonus_malus", r'[^\d-]', '').cast("float"))
    df = df.withColumn("cout_energie", F.regexp_replace("cout_energie", r'€.*', ''))
    df = df.withColumn("cout_energie", F.regexp_replace("cout_energie", r'[^\d-]', '').cast("float"))
    df = df.withColumn("rejets_co2", F.when(F.col("rejets_co2") == 0, None).otherwise(F.col("rejets_co2")))
    return df



# Fonction pour extraire dynamiquement la marque sans inclure le modèle
def add_first_words_columns(df):
    """
    Ajoute les colonnes 'first_word' et 'first_two_words' pour l'extraction des marques.
    """
    df = df.withColumn("first_word", F.split(F.col("modele"), " ")[0])
    df = df.withColumn("first_two_words", F.concat_ws(" ", F.split(F.col("modele"), " ").getItem(0),
                                                      F.split(F.col("modele"), " ").getItem(1)))
    return df


def get_top_brands(df, threshold=2):
    """
    Retourne une liste des marques les plus fréquentes en fonction du threshold.
    """
    brand_counts = df.groupBy("first_word").count().orderBy(F.desc("count"))
    return [row['first_word'] for row in brand_counts.collect() if row['count'] >= threshold]


def extract_brand_case_expr(df, top_brands):
    """
    Construit l'expression CASE pour extraire dynamiquement la marque.
    """
    case_expr = "CASE "
    for brand in top_brands:
        case_expr += "WHEN first_word = '{}' THEN '{}' ".format(brand, brand)  #
        case_expr += "WHEN first_two_words = '{}' THEN '{}' ".format(brand, brand.split()[0])
    case_expr += "ELSE NULL END"
    return case_expr


def dynamic_extract_brand(df, threshold=2):
    """
    Extrait dynamiquement la marque en utilisant les premières colonnes 'first_word' et 'first_two_words'.
    """
    df = add_first_words_columns(df)  # Ajouter les colonnes 'first_word' et 'first_two_words'
    top_brands = get_top_brands(df, threshold)  # Obtenir les marques les plus fréquentes
    case_expr = extract_brand_case_expr(df, top_brands)  # Construire l'expression CASE
    return df.withColumn("marque", F.expr(case_expr)).drop("first_word",
                                                           "first_two_words")  # Appliquer l'expression CASE


def extract_model(df):
    """
    Remplit les colonnes 'modele' et 'modele_detail'.
    """
    df = df.withColumn("modele", F.regexp_extract(F.col("modele_old"), r'^\S+\s+(\S+)', 1))
    df = df.withColumn("modele_detail",
                       F.when(F.col("modele_old").contains("("),
                              F.regexp_extract(F.col("modele_old"), r'^\S+\s+\S+\s+(.*?)\s*\(', 1))
                       .otherwise(F.regexp_extract(F.col("modele_old"), r'^\S+\s+\S+\s+(\S+)', 1)))
    return df


def fill_model_and_detail(df):
    """
    Extrait la puissance, le modèle et les informations de puissance unifiée.
    """
    df = extract_model(df)
    df = df.withColumn("horse_power",
                       F.regexp_extract(F.col("modele_old"), r'(\d{1,3})\s*(?i)(?=ch|kw|kwh)', 1).cast("float"))
    df = df.withColumn("unit",
                       F.when(F.col("modele_old").rlike(r'(?i)\d{1,3}\s*ch'), "ch")
                       .when(F.col("modele_old").rlike(r'(?i)\d{1,3}\s*kw|kwh'), "kW")
                       .otherwise("Inconnu"))
    df = df.withColumn("unified_horse_power",
                       F.when(F.col("unit") == "kW", F.round(F.col("horse_power") * 1.36))
                       .otherwise(F.col("horse_power")))
    return df

# Initialisation de la session Spark
print("Étape 1 : Initialisation de la session Spark...")
spark = initialize_spark_session()
print("Session Spark initialisée avec succès.\n")

# Charger les données de la table CO2 depuis Hive
print("Étape 2 : Chargement des données depuis Hive...")
co2_df = spark.sql("SELECT marque, modele, bonus_malus, rejets_co2, cout_energie FROM concessionnaire.co2_data") \
    .filter("modele != 'Marque / Modele'")
print("Données chargées depuis Hive avec succès.\n")

# Nettoyer les erreurs d'encodage et les guillemets, puis gérer les valeurs NULL
print("Étape 3 : Nettoyage de l'encodage et des données...")
co2_df = clean_encoding(co2_df)
co2_df = clean_data(co2_df)
print("Nettoyage de l'encodage et des données terminé.\n")

# Extraire dynamiquement les marques avec une séparation précise de marque et modèle
print("Étape 4 : Extraction dynamique des marques...")
co2_df = dynamic_extract_brand(co2_df, threshold=3)
print("Extraction des marques terminée.\n")

# Renommer la colonne "modele" en "modele_old" et créer les nouvelles colonnes
print("Étape 5 : Création des colonnes 'modele', 'modele_detail', 'horse_power', 'unit', et 'unified_horse_power'...")
co2_df = co2_df.withColumnRenamed("modele", "modele_old")
co2_df = fill_model_and_detail(co2_df)
print("Colonnes créées avec succès.\n")

# Suppression des duplications sur toutes les colonnes
print("Étape 6 : Suppression des duplications...")
co2_df = co2_df.dropDuplicates()
print("Duplications supprimées.\n")

# Réorganisation des colonnes sans 'modele_old' pour correspondre à la table Hive
print("Étape 7 : Réorganisation des colonnes...")
columns_order = ["marque", "modele", "modele_detail", "horse_power", "unit", "unified_horse_power", "bonus_malus",
                 "cout_energie", "rejets_co2"]
co2_df = co2_df.select(columns_order)
print("Colonnes réorganisées.\n")

# Enregistrer les données dans la table Hive externe
print("Étape 8 : Enregistrement des données dans la table Hive...")
co2_df.write.mode("overwrite").insertInto("concessionnaire.co2_data_processed")
print("Données enregistrées dans la table Hive avec succès.\n")

# Afficher les données de la table Hive "concessionnaire.co2_data_processed"
print("Affichage des données de la table concessionnaire.co2_data_processed...")
co2_data_df = spark.sql("SELECT * FROM concessionnaire.co2_data_processed")
co2_data_df.show(truncate=False)  # Utilisez truncate=False pour afficher toutes les colonnes sans troncature




###############################################################################################################
#                                              Affichage des données                                          #
###############################################################################################################

# Afficher TOUT
# co2_df.show(1000, truncate=False)
# co2_df.printSchema()

#  Affichage des colonnes dans l'ordre défini

# columns_order = ["marque", "modele", "modele_detail", "unit", "bonus_malus", "unified_horse_power", "categorie", "horse_power"]
# co2_df.select([col for col in columns_order if col in co2_df.columns]).show(1000, truncate=False)
