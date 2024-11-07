from pyspark.sql import SparkSession
import shutil
import os

# Création de la session Spark avec les configurations nécessaires
spark = SparkSession.builder \
    .appName("Cassandra to Hive") \
    .config("spark.sql.warehouse.dir", "/spark-warehouse") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/spark-warehouse")\
    .config("spark.cassandra.connection.port", "9042") \
    .enableHiveSupport() \
    .getOrCreate()

# Créer la base de données Hive si elle n'existe pas déjà
spark.sql("CREATE DATABASE IF NOT EXISTS my_database")

# Sélectionner la base de données créée
spark.catalog.setCurrentDatabase("my_database")

# Supprimer la table Hive si elle existe déjà dans la base de données
spark.sql("DROP TABLE IF EXISTS cassandra_catalogue")

# Supprimer le répertoire de données associé s'il existe
warehouse_location = "/spark-warehouse"
table_location = os.path.join(warehouse_location, "my_database.db", "cassandra_catalogue")
if os.path.exists(table_location):
    shutil.rmtree(table_location)
    print(f"Le répertoire {table_location} a été supprimé.")

# Lire les données depuis Cassandra
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="catalogue", keyspace="db_keyspace") \
    .load()

# Vérifier le nombre de lignes lues depuis Cassandra
nb_lignes_cassandra = df.count()
print(f"Nombre de lignes lues depuis Cassandra : {nb_lignes_cassandra}")

# Afficher les données pour vérification
df.show()

# Écrire les données dans la table Hive au sein de la base de données spécifiée
df.write.mode('overwrite').saveAsTable("cassandra_catalogue")

# Lire la table depuis Hive pour vérification
df_hive = spark.table("cassandra_catalogue")
nb_lignes_hive = df_hive.count()
print(f"Nombre de lignes écrites dans Hive : {nb_lignes_hive}")
df_hive.show()

# Arrêter la session Spark
spark.stop()
