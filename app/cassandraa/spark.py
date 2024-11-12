# -*- coding: utf-8 -*-
import sys
from imp import reload

reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CassandraToHive") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars", "/opt/spark/jars/spark-cassandra-connector-assembly_2.12-3.1.0.jar") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    # Chargement des données de Cassandra
    catalogue_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="catalogue", keyspace="db_keyspace") \
        .load()

    # Affichage des informations de schéma et quelques lignes
    catalogue_df.printSchema()
    catalogue_df.show(10)

    # Écriture des données dans la table Hive sans spécifier explicitement le chemin
    catalogue_df.write.mode("overwrite").saveAsTable("concessionnaire.catalogue_table")

    print("Les données ont été écrites dans Hive avec succès.".encode('utf-8'))

except Exception as e:
    print("Erreur lors du transfert des données de Cassandra à Hive : {}".format(e).encode('ascii', 'ignore'))

spark.stop()
