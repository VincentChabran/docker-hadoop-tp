# -*- coding: utf-8 -*-  # Spécifie que le fichier utilise l'encodage UTF-8 pour gérer les caractères spéciaux.
import sys  # Importe le module système pour configurer l'encodage.

from pyspark.sql import SparkSession  # Importation de SparkSession pour créer une session Spark.
from pyspark.sql import functions as F  # Raccourci pour utiliser les fonctions Spark SQL.
from pyspark.sql.types import FloatType  # Importation du type de données FloatType pour les colonnes.

# Configurer l'encodage pour UTF-8
reload(sys)  # Recharge le module sys pour permettre la modification de l'encodage.
sys.setdefaultencoding('utf-8')  # Définit l'encodage par défaut sur UTF-8.


# Initialiser la session Spark
def initialize_spark_session():
    # Crée et configure une session Spark avec le support de Hive.
    spark = SparkSession.builder \
        .appName("Fusion des Données Catalogue et CO2") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()  # Crée ou récupère la session Spark existante.
    spark.sql("USE concessionnaire")

    spark.sparkContext.setLogLevel("ERROR")  # Réduit les logs pour afficher uniquement les erreurs.
    return spark  # Retourne la session Spark.


# Charger les tables depuis Hive
def load_dataframes(spark):
    # Charge la table catalogue depuis la base Hive.
    catalogue_df = spark.sql("SELECT * FROM concessionnaire.catalogue_table_processed")
    # Charge la table CO2 depuis la base Hive.
    co2_df = spark.sql("SELECT * FROM concessionnaire.co2_data_processed")
    return catalogue_df, co2_df  # Retourne les deux DataFrames.


# Nettoyage des colonnes numériques sans UDF
def clean_numeric_columns(df):
    # Nettoyer et convertir la colonne 'bonus_malus'
    df = df.withColumn("bonus_malus", F.regexp_replace(F.col("bonus_malus"), r"[^\d\-,\.]", "").cast(FloatType()))
    # Nettoyer et convertir la colonne 'rejets_co2'
    df = df.withColumn("rejets_co2", F.regexp_replace(F.col("rejets_co2"), r"[^\d\-,\.]", "").cast(FloatType()))
    # Nettoyer et convertir la colonne 'cout_energie'
    df = df.withColumn("cout_energie", F.regexp_replace(F.col("cout_energie"), r"[^\d\-,\.]", "").cast(FloatType()))
    return df  # Retourne le DataFrame nettoyé.


# Calcul des moyennes pour les données manquantes
def calculate_averages(co2_df):
    # Normalise la colonne 'marque' en minuscule et sans espaces.
    co2_df = co2_df.withColumn("marque_norm", F.lower(F.trim(F.col("marque"))))
    # Calcule les moyennes par marque.
    averages_by_marque = co2_df.groupBy("marque_norm").agg(
        F.round(F.avg("bonus_malus"), 1).alias("avg_bonus_malus"),
        F.round(F.avg("rejets_co2"), 1).alias("avg_rejets_co2"),
        F.round(F.avg("cout_energie"), 1).alias("avg_cout_energie")
    )
    # Calcule les moyennes globales.
    overall_averages = co2_df.agg(
        F.round(F.avg("bonus_malus"), 1).alias("avg_bonus_malus"),
        F.round(F.avg("rejets_co2"), 1).alias("avg_rejets_co2"),
        F.round(F.avg("cout_energie"), 1).alias("avg_cout_energie")
    ).collect()[0]  # Collecte les résultats dans un dictionnaire.

    # Affiche les moyennes par marque.
    print("Moyennes par marque :")
    averages_by_marque.show(truncate=False)
    # Affiche les moyennes globales.
    print("\nMoyennes globales :")
    print("Bonus/Malus : {}, Rejets CO2 : {}, Coût Énergie : {}".format(
        overall_averages["avg_bonus_malus"],
        overall_averages["avg_rejets_co2"],
        overall_averages["avg_cout_energie"]
    ))
    return averages_by_marque, overall_averages  # Retourne les moyennes.


# Fusionner les données manquantes dans Catalogue
def integrate_missing_data(catalogue_df, co2_df, averages_by_marque, overall_averages):
    # Normalise les colonnes 'marque' et 'modele' pour le catalogue et CO2.
    catalogue_df = catalogue_df.withColumn("marque_norm", F.lower(F.trim(F.col("marque")))) \
        .withColumn("modele_norm", F.lower(F.trim(F.col("modele"))))
    co2_df = co2_df.withColumn("marque_norm", F.lower(F.trim(F.col("marque")))) \
        .withColumn("modele_norm", F.lower(F.trim(F.col("modele"))))

    # Jointure entre le catalogue et les données CO2 sur les colonnes normalisées.
    catalogue_with_co2 = catalogue_df.join(
        co2_df.select("marque_norm", "modele_norm", "bonus_malus", "rejets_co2", "cout_energie"),
        on=["marque_norm", "modele_norm"],
        how="left"
    )
    # Jointure avec les moyennes par marque.
    catalogue_with_co2 = catalogue_with_co2.join(
        averages_by_marque,
        on="marque_norm",
        how="left"
    )

    # Remplit les valeurs manquantes avec les moyennes.
    catalogue_with_co2 = catalogue_with_co2.withColumn(
        "bonus_malus_final",
        F.coalesce(
            F.col("bonus_malus"),
            F.col("avg_bonus_malus"),
            F.lit(overall_averages["avg_bonus_malus"])
        )
    ).withColumn(
        "rejets_co2_final",
        F.coalesce(
            F.col("rejets_co2"),
            F.col("avg_rejets_co2"),
            F.lit(overall_averages["avg_rejets_co2"])
        )
    ).withColumn(
        "cout_energie_final",
        F.coalesce(
            F.col("cout_energie"),
            F.col("avg_cout_energie"),
            F.lit(overall_averages["avg_cout_energie"])
        )
    )

    # Sélectionne les colonnes finales à conserver.
    final_columns = [col for col in catalogue_df.columns if col not in ["marque_norm", "modele_norm"]] + \
                    ["bonus_malus_final", "rejets_co2_final", "cout_energie_final"]
    catalogue_with_co2 = catalogue_with_co2.select(*final_columns)

    # Renomme les colonnes finales pour enlever le suffixe '_final'.
    catalogue_with_co2 = catalogue_with_co2.withColumnRenamed("bonus_malus_final", "bonus_malus") \
        .withColumnRenamed("rejets_co2_final", "rejets_co2") \
        .withColumnRenamed("cout_energie_final", "cout_energie")
    return catalogue_with_co2  # Retourne le DataFrame final.


# Main
spark = initialize_spark_session()  # Initialise la session Spark.

# Étape 1 : Charger les données
catalogue_df, co2_df = load_dataframes(spark)  # Charge les DataFrames depuis Hive.

# Étape 2 : Nettoyer les données numériques
co2_df = clean_numeric_columns(co2_df)  # Nettoie les colonnes numériques.

# Vérifier les données nettoyées
print("Données CO2 après nettoyage :")
co2_df.select("marque", "modele", "bonus_malus", "rejets_co2", "cout_energie").show(10)

# Étape 3 : Calculer les moyennes pour les données manquantes
averages_by_marque, overall_averages = calculate_averages(co2_df)

# Étape 4 : Fusionner les données
updated_catalogue_df = integrate_missing_data(catalogue_df, co2_df, averages_by_marque, overall_averages)

# Afficher le résultat final
updated_catalogue_df.show(100)

# Enregistrer le DataFrame dans Hive
updated_catalogue_df.write.mode("overwrite").saveAsTable("catalogue_co2_merge_processed")
print("Enregistrement dans Hive effectué avec succès.")
updated_catalogue_df.show(500)




# Arrêter la session Spark
spark.stop()  # Arrête la session Spark.
