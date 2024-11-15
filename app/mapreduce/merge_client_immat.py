# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import regexp_extract, when, trim, col


# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("DataMergingWithHive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Utilisation de la base de données Hive
spark.sql("USE concessionnaire")

# Charger les données des tables traitées
clients_df = spark.sql("SELECT * FROM client_processed")
immatriculations_df = spark.sql("SELECT * FROM immatriculations_processed")

# Fusionner les tables sur la colonne 'Immatriculation'
merged_df = clients_df.join(immatriculations_df, "Immatriculation", "inner")

# Enregistrer le DataFrame fusionné sous forme de table Hive
merged_df.createOrReplaceTempView("merged_view")
spark.sql("DROP TABLE IF EXISTS client_immatriculation_merged")
spark.sql("CREATE TABLE client_immatriculation_merged AS SELECT * FROM merged_view")
