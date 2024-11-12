# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession

# Initialisation de la session Spark avec support Hive
spark = SparkSession.builder \
    .appName("Lister les bases de données Hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Lister les bases de données
databases_df = spark.sql("SHOW DATABASES;")
databases_df.show()

# Arrêt de la session Spark
spark.stop()