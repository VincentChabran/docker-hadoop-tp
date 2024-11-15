# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, when



spark = SparkSession.builder \
    .appName("DataCleaningWithHive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE concessionnaire")
df_client = spark.sql("SELECT * FROM client_data")  



# Utiliser zipWithIndex pour ajouter un index à chaque ligne, puis filtrer la première ligne
df_client = df_client.rdd.zipWithIndex().filter(lambda row_index: row_index[1] > 0).map(lambda row_index: row_index[0]).toDF(df_client.schema)



# Remplacer les valeurs nulles par la moyenne pour chaque colonne spécifiée
for col_name in ["age", "taux", "nbenfantacharge"]:
    mean_value = df_client.agg({col_name: "mean"}).first()[0] 
    df_client = df_client.na.fill({col_name: mean_value})



# Remplacer les caractères spéciaux par 'é' dans la colonne 'situationfamilliale'
df_client = df_client.withColumn("situationfamilliale", regexp_replace("situationfamilliale", "�", "é"))
df_client = df_client.withColumn("situationfamilliale", regexp_replace("situationfamilliale", "^(?i)seul(e)?$", "Célibataire"))
df_client = df_client.withColumn(
    "situationfamilliale",
    when((col("situationfamilliale").isin("N/D", "", " ","?")), "Inconnu")
    .otherwise(col("situationfamilliale")) 
)



# Normaliser les valeurs de la colonne 'sexe'
df_client = df_client.withColumn(
    "sexe",
    when(col("sexe").isin("M", "Masculin", "Homme"), "Homme")  
    .when(col("sexe").isin("F", "Femme", "Féminin", "F�minin"), "Femme")  
    .otherwise("Inconnu")  
)



# Normaliser les valeurs de la colonne 'deuxiemevoiture'
df_client = df_client.withColumn(
    "deuxiemevoiture",
    when((col("deuxiemevoiture") == "true"), "true") 
    .otherwise("false")
)



# Filtrer les lignes avec des valeurs manquantes dans la colonne 'immatriculation'
df_client = df_client.na.drop(subset=["immatriculation"])


# Afficher les valeurs distinctes de la colonne 'deuxiemevoiture'
df_client.select("deuxiemevoiture").distinct().show()

df_client.show()



df_client.write.mode("overwrite").saveAsTable("client_processed")



