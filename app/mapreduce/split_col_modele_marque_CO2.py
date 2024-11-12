# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession

# Initialisation de la session Spark avec support Hive
spark = SparkSession.builder \
    .appName("Lister les bases de données Hive") \
    .enableHiveSupport() \
    .getOrCreate()

print("Récupération des bases de données dans Hive...")

try:
    # Lister les bases de données
    databases_df = spark.sql("SHOW DATABASES;")
    print("Bases de données trouvées dans Hive :")
    databases_df.show()
except Exception as e:
    print("Erreur lors de la récupération des bases de données :", str(e))

# Arrêt de la session Spark
spark.stop()
