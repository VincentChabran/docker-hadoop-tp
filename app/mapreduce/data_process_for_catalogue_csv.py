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
        .appName("Data Processing dans catalogue csv") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")  # Réduire le niveau de journalisation
    print("Session Spark initialisée avec succès.\n")
    return spark


def rename_and_create_columns(df):
    """
    Renomme et crée les colonnes nécessaires pour correspondre aux champs de CO2.
    """
    # Renommer les colonnes existantes
    df = df.withColumnRenamed("nom", "modele_old") \
        .withColumnRenamed("puissance", "unified_horse_power") \
        .withColumnRenamed("nbPlaces", "nbplaces") \
        .withColumnRenamed("nbPortes", "nbportes")

    # Créer les colonnes manquantes avec des valeurs par défaut
    df = df.withColumn("modele", F.lit(None)) \
        .withColumn("modele_detail", F.lit(None))

    return df


def clean_encoding(df):
    """
    Nettoie les erreurs d'encodage dans les colonnes 'marque' et 'modele_old'.
    """
    df = df.withColumn("marque", F.regexp_replace(F.col("marque"), "Ã©",
                                                  "é"))  # Remplacer les caractères mal encodés dans 'marque'
    df = df.withColumn("modele_old", F.regexp_replace(F.col("modele_old"), "Ã©",
                                                      "é"))  # Remplacer les caractères mal encodés dans 'modele_old'
    return df


def split_modele_columns(df):
    """
    Sépare les mots de 'modele_old' pour les distribuer dans 'modele' et 'modele_detail'.
    Le premier mot reste toujours dans 'modele'. Tout ce qui suit va dans 'modele_detail'.
    """
    # Étape 1 : Extraire le premier mot de 'modele_old' pour 'modele'
    df = df.withColumn(
        "modele",
        F.when(
            F.col("modele_old").rlike(r"^\S+\s+\d"),  # Si le deuxième mot commence par un chiffre
            F.regexp_extract(F.col("modele_old"), r"^(\S+)", 1)  # Extraire uniquement le premier mot
        ).otherwise(
            F.regexp_extract(F.col("modele_old"), r"^(\S+\s\S+)", 1)
            # Extraire les deux premiers mots s'ils ne commencent pas par un chiffre
        )
    )

    # Étape 2 : Capturer tout ce qui suit pour 'modele_detail'
    df = df.withColumn(
        "modele_detail",
        F.when(
            F.col("modele_old").rlike(r"^\S+\s+\d"),  # Si le deuxième mot commence par un chiffre
            F.regexp_extract(F.col("modele_old"), r"^\S+\s+(.*)", 1)  # Extraire le reste après le premier mot
        ).otherwise(
            F.regexp_extract(F.col("modele_old"), r"^\S+\s+\S+\s+(.*)", 1)
            # Extraire tout ce qui suit les deux premiers mots
        )
    )

    # Étape 3 : Corriger les cas où 'modele' est vide
    df = df.withColumn(
        "modele",
        F.when(F.col("modele") == "", F.col("modele_old")).otherwise(F.col("modele"))
    )

    return df


def reorder_and_save(df):
    """
    Réorganise les colonnes, supprime 'modele_old' et enregistre le DataFrame dans Hive.
    """
    # Réordonner les colonnes
    columns_order = ["marque", "modele", "modele_detail", "unified_horse_power", "longueur", "nbplaces",
                     "nbportes", "couleur", "occasion", "prix"]
    df = df.select(columns_order)

    # Enregistrer le DataFrame dans Hive
    df.write.mode("overwrite").saveAsTable("concessionnaire.catalogue_table_processed")
    print("Enregistrement dans Hive effectué avec succès.")
    df.show(500)


# Initialiser la session Spark et charger les données de la table Hive
spark = initialize_spark_session()
catalogue_df = spark.sql("""
    SELECT marque, nom, puissance, longueur, nbPlaces, nbPortes, couleur, occasion, prix
    FROM concessionnaire.catalogue_table
""")

# Renommer la colonne 'nom' en 'modele_old' et créer les colonnes manquantes
catalogue_df = rename_and_create_columns(catalogue_df)

# Nettoyer l'encodage dans les colonnes 'marque' et 'modele_old'
catalogue_df = clean_encoding(catalogue_df)

# Séparer les colonnes 'modele' et 'modele_detail' à partir de 'modele_old'
catalogue_df = split_modele_columns(catalogue_df)

# Réordonner les colonnes et enregistrer le résultat dans Hive
reorder_and_save(catalogue_df)

# Arrêter la session Spark
spark.stop()
print("Traitement terminé.")
