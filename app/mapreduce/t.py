from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean
from pyspark.sql.types import IntegerType, StringType

# Charger le fichier CSV
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()
df_clients = spark.read.csv("/path/to/Clients_0.csv", header=True, inferSchema=True)

# Remplacer les valeurs manquantes pour les colonnes numériques par la moyenne
numeric_cols = ["Age", "Taux", "NbEnfantsAcharge"]
for col_name in numeric_cols:
    mean_value = df_clients.agg({col_name: "mean"}).first()[0]
    df_clients = df_clients.fillna({col_name: mean_value})

# Remplacer les valeurs manquantes pour les colonnes catégorielles par "Inconnu"
categorical_cols = ["Sexe", "SituationFamiliale", "2eme voiture"]
df_clients = df_clients.fillna({col_name: "Inconnu" for col_name in categorical_cols})

# Correction des formats (par exemple, uniformiser les majuscules)
df_clients = df_clients.withColumn("Sexe", when(col("Sexe") == "m", "M").otherwise(col("Sexe"))) \
                       .withColumn("Sexe", when(col("Sexe") == "f", "F").otherwise(col("Sexe")))

# Filtrage des valeurs aberrantes pour l'age
df_clients = df_clients.filter((col("Age") >= 18) & (col("Age") <= 84))

# Afficher les données nettoyées
df_clients.show()
