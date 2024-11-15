# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import count
import matplotlib.pyplot as plt


spark = SparkSession.builder \
    .appName("DataCleaningWithHive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()


spark.sql("USE concessionnaire")
immat_df = spark.sql("SELECT * FROM immatriculations_data")

# Afficher un échantillon des données
immat_df.show(5)

# Afficher le schéma des données
immat_df.printSchema()

# Résumer les statistiques descriptives
immat_df.describe().show()


prix_moyen_par_marque = immat_df.groupBy("Marque").agg(avg("Prix").alias("PrixMoyen"))
prix_moyen_par_marque.show()


modeles_populaires = immat_df.groupBy("Nom").agg(count("*").alias("NombreImmatriculations"))
modeles_populaires.orderBy("NombreImmatriculations", ascending=False).show()


pandas_df = prix_moyen_par_marque.toPandas()

pandas_df.plot(kind="bar", x="Marque", y="PrixMoyen", legend=False)
plt.title("Prix moyen par marque")
plt.xlabel("Marque")
plt.ylabel("Prix moyen (€)")
plt.show()
