# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

# Initialisation de la session Spark avec Hive et encodage UTF-8
spark = SparkSession.builder \
    .appName("TP Analyse Clientèle") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
    .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
    .enableHiveSupport() \
    .getOrCreate()

print("Chargement des données de la table CO2 depuis Hive...")

try:
    # Charger les données de la table CO2
    co2_df = spark.sql("SELECT * FROM co2")
    print("Données de la table CO2 récupérées avec succès.")
    
    # Affiche les 5 premières lignes pour vérification
    co2_df.show(5)
except Exception as e:
    print("Erreur lors de la récupération des données de la table CO2 :", str(e))

# Arrêt de la session Spark
spark.stop()
