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
    df = df.withColumn("modele", F.regexp_replace(F.col("modele"), "Ã©", "é"))  # Remplacer les caractères mal encodés
    df = df.withColumn("modele", F.regexp_replace(F.col("modele"), r'^"|"$', ''))  # Supprimer les guillemets
    df = df.withColumn("modele", F.when(F.col("modele").isNull(), "Inconnu").otherwise(
        F.col("modele")))  # Remplacer les valeurs NULL
    return df


def clean_data(df):
    """
    Nettoie les colonnes 'bonus_malus' et 'cout_energie' pour supprimer les symboles monétaires et convertir en float.
    Remplace les valeurs '0' dans 'rejets_co2' par NaN pour signaler des données manquantes.
    """
    # Nettoyage des colonnes 'bonus_malus' et 'cout_energie' pour retirer les symboles monétaires
    df = df.withColumn("bonus_malus", F.regexp_replace("bonus_malus", r'€.*', ''))  # Remplacer les symboles monétaires
    df = df.withColumn("bonus_malus", F.regexp_replace("bonus_malus", r'[^\d-]', '').cast("float"))

    df = df.withColumn("cout_energie",
                       F.regexp_replace("cout_energie", r'€.*', ''))  # Remplacer les symboles monétaires
    df = df.withColumn("cout_energie",
                       F.regexp_replace("cout_energie", r'[^\d-]', '').cast("float"))  # Convertir en float

    # Remplacer les valeurs 0 dans 'rejets_co2' par NaN (None) pour signaler une donnée manquante
    df = df.withColumn("rejets_co2", F.when(F.col("rejets_co2") == 0, None).otherwise(F.col("rejets_co2")))

    return df


# Fonction pour extraire dynamiquement la marque sans inclure le modèle
def dynamic_extract_brand(df, threshold=2):
    """
    Extrait dynamiquement la marque d'un modèle en utilisant le premier ou les deux premiers mots du modèle.
    Les marques sont déterminées en fonction de la fréquence d'apparition des premiers mots dans les modèles ici le reglages est sur 2.
    """
    df = df.withColumn("first_word", F.split(F.col("modele"), " ")[0])  # Extraire le premier mot du modèle
    df = df.withColumn("first_two_words", F.concat_ws(" ", F.split(F.col("modele"), " ").getItem(0),
                                                      F.split(F.col("modele"), " ").getItem(
                                                          1)))  # Extraire les deux premiers mots du modèle

    # Compter les occurrences de first_word pour identifier les candidats probables
    brand_counts = df.groupBy("first_word").count().orderBy(F.desc("count"))  # Compter les occurrences de first_word
    top_brands = [row['first_word'] for row in brand_counts.collect() if
                  row['count'] >= threshold]  # Sélectionner les marques les plus fréquentes

    # Ajouter les first_two_words uniques comme candidats de marque si nécessaires
    unique_two_words = df.select("first_two_words").distinct().rdd.flatMap(
        lambda x: x).collect()  # Extraire les first_two_words uniques
    top_brands.extend(unique_two_words)  # Ajouter les first_two_words uniques

    # Construire l'expression CASE pour extraire seulement la marque sans inclure le modèle
    case_expr = "CASE "  # Initialiser l'expression CASE
    for brand in top_brands:
        case_expr += "WHEN first_word = '{}' THEN '{}' ".format(brand,
                                                                brand)  # Ajouter une condition pour chaque marque
        case_expr += "WHEN first_two_words = '{}' THEN '{}' ".format(brand, brand.split()[
            0])  # Ajouter une condition pour chaque first_two_words
    case_expr += "ELSE NULL END"  # Ajouter une condition par défaut

    return df.withColumn("marque", F.expr(case_expr)).drop("first_word",
                                                           "first_two_words")  # Appliquer l'expression CASE et supprimer les colonnes temporaires


def fill_model_and_detail(df):
    """
    Remplir les colonnes 'modele', 'modele_detail' avec des informations extraites du modèle.
    Extrait également la puissance et l'unité (ch, kW) et calcule la puissance unifiée.
    """
    # Extraction du mot suivant la marque pour 'modele'
    df = df.withColumn("modele",
                       F.regexp_extract(F.col("modele_old"), r'^\S+\s+(\S+)', 1))  # Extraire le mot suivant la marque

    # Extraction conditionnelle pour 'modele_detail'
    # Si parenthèse, tout jusqu'à avant la parenthèse, sinon un mot après 'modele'
    df = df.withColumn(
        "modele_detail",
        F.when(
            F.col("modele_old").contains("("),
            F.regexp_extract(F.col("modele_old"), r'^\S+\s+\S+\s+(.*?)\s*\(', 1)
        ).otherwise(
            F.regexp_extract(F.col("modele_old"), r'^\S+\s+\S+\s+(\S+)', 1)
        )
    )

    # Remplir modele_detail avec 'STANDARD' si vide
    df = df.withColumn("modele_detail",
                       F.when(F.col("modele_detail") == "", "STANDARD").otherwise(
                           F.col("modele_detail")))  # Remplacer les valeurs vides

    # Extraction de la puissance et de l'unité (1 à 3 chiffres suivis éventuellement d'un espace puis de "ch", "kW", "KW", etc.)
    df = df.withColumn("horse_power",
                       F.regexp_extract(F.col("modele_old"), r'(\d{1,3})\s*(?i)(?=ch|kw|kwh)', 1).cast("float"))
    df = df.withColumn("unit",
                       F.when(F.col("modele_old").rlike(r'(?i)\d{1,3}\s*ch'), "ch")
                       .when(F.col("modele_old").rlike(r'(?i)\d{1,3}\s*kw|kwh'), "kW")
                       .otherwise("Inconnu"))

    # Conversion de kW en ch si nécessaire (1 kW ≈ 1.36 ch) et arrondir le résultat
    df = df.withColumn("unified_horse_power",
                       F.when(F.col("unit") == "kW", F.round(F.col("horse_power") * 1.36))
                       .otherwise(F.col("horse_power")))  # Convertir kW en ch si nécessaire

    # Gestion des valeurs manquantes
    df = df.withColumn("unified_horse_power",
                       F.when(F.col("unified_horse_power").isNull(), "Inconnu").otherwise(
                           F.col("unified_horse_power")))  # Remplacer les valeurs NULL

    return df


# Initialisation de la session Spark
spark = initialize_spark_session()

# Charger les données de la table CO2 depuis Hive
co2_df = spark.sql("SELECT marque, modele, bonus_malus, rejets_co2, cout_energie FROM concessionnaire.co2_data").filter(
    "modele != 'Marque / Modele'")  # Exclure les lignes où la colonne modele contient la valeur 'Marque / Modele' entete pris en compte

# Nettoyer les erreurs d'encodage et les guillemets, puis gérer les valeurs NULL
co2_df = clean_encoding(co2_df)
co2_df = clean_data(co2_df)

# Extraire dynamiquement les marques avec une séparation précise de marque et modèle
co2_df = dynamic_extract_brand(co2_df, threshold=3)

# Renommer la colonne "modele" en "modele_old" et créer les nouvelles colonnes "modele",
# "modele_detail", "horse_power", "unit", et "unified_horse_power"
co2_df = co2_df.withColumnRenamed("modele", "modele_old")
co2_df = fill_model_and_detail(co2_df)

# Suppression des duplications sur toutes les colonnes
co2_df = co2_df.dropDuplicates()

# Supprimer la colonne 'modele_old' avant d'insérer dans Hive
co2_df = co2_df.drop("modele_old")

# Réordonner les colonnes pour s'assurer qu'elles correspondent à la table Hive
columns_order = ["marque", "modele", "modele_detail", "horse_power", "unit", "unified_horse_power", "bonus_malus",
                 "cout_energie", "rejets_co2"]
co2_df = co2_df.select(columns_order)

# Enregistrer les données dans la table Hive externe
co2_df.write \
    .mode("overwrite") \
    .insertInto("concessionnaire.co2_data_processed")

###############################################################################################################
#                                              Affichage des données                                          #
###############################################################################################################

# Afficher TOUT
# co2_df.show(1000, truncate=False)
# co2_df.printSchema()

#  Affichage des colonnes dans l'ordre défini

# columns_order = ["marque", "modele", "modele_detail", "unit", "bonus_malus", "unified_horse_power", "categorie", "horse_power"]
# co2_df.select([col for col in columns_order if col in co2_df.columns]).show(1000, truncate=False)
