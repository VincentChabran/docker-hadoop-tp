# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("MongoDBToHive") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/concessionnaire.marketing") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .enableHiveSupport() \
    .getOrCreate()

# Lecture des données de MongoDB
marketing_df = spark.read.format("mongo").load()
# Renomme la colonne "2eme voiture" en "deuxieme_voiture"
marketing_df = marketing_df.withColumnRenamed("2eme voiture", "deuxieme_voiture")

# Vérifie le schéma et affiche les 10 premières lignes
marketing_df.printSchema()
marketing_df.show(10)

hive_table_path = "hdfs://namenode:9000/user/hive/warehouse/concessionnaire.db/marketing_data"

# Écriture des données de Mongodb dans Hive
marketing_df.write.format("parquet").mode("append").option("path", hive_table_path).saveAsTable("marketing_data")

spark.stop()
