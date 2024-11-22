# Étape 1 : Préparation des données - Cellule 1
# La première étape consiste à charger et prétraiter vos données, en vous assurant qu'elles sont propres et prêtes pour le clustering.

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import col, avg
from pyspark.sql.types import DoubleType

# Convertir la colonne 'longueur' en type numérique
immatriculation_df = immatriculation_df.withColumn("longueur", col("longueur").cast(DoubleType()))
immatriculation_df = immatriculation_df.withColumn("puissance", col("puissance").cast(DoubleType()))
immatriculation_df = immatriculation_df.withColumn("nbPlaces", col("nbPlaces").cast(DoubleType()))
immatriculation_df = immatriculation_df.withColumn("nbPortes", col("nbPortes").cast(DoubleType()))
immatriculation_df = immatriculation_df.withColumn("prix", col("prix").cast(DoubleType()))

# Remplacer les valeurs nulles par une valeur par défaut
immatriculation_df = immatriculation_df.fillna({
    "longueur": 0.0,
    "puissance": 0.0,
    "nbPlaces": 0.0,
    "nbPortes": 0.0,
    "prix": 0.0
})

# Sélectionner les caractéristiques pertinentes pour le clustering
features_df = immatriculation_df.select("immatriculation", "marque", "modele", "puissance", "longueur", "nbPlaces",
                                        "nbPortes", "couleur", "occasion", "prix")

# Étape 1 : Préparation des données - Cellule 2
# Assembler les caractéristiques dans un vecteur de caractéristiques
assembler = VectorAssembler(inputCols=["puissance", "longueur", "nbPlaces", "nbPortes", "prix"], outputCol="features",
                            handleInvalid="skip")
assembled_df = assembler.transform(features_df)

# Étape 1 : Préparation des données - Cellule 3
# Standardiser les caractéristiques pour les ramener à la même échelle
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)
scaler_model = scaler.fit(assembled_df)
scaled_df = scaler_model.transform(assembled_df)

# Supprimer la colonne 'scaled_features' car elle n'est plus nécessaire
drop_columns = ['scaled_features']
scaled_df = scaled_df.drop(*drop_columns)

# Étape 2 : Appliquer le clustering K-Means - Cellule 4
# Maintenant que les données sont prêtes, nous pouvons appliquer K-means pour regrouper les véhicules.

# Définir le nombre de clusters (catégories) - cela peut être ajusté en fonction de votre analyse
k = 10  # Vous avez mentionné environ 10 catégories
kmeans = KMeans(featuresCol="scaled_features", predictionCol="category", k=k)
model = kmeans.fit(scaled_df)

# Étape 2 : Appliquer le clustering K-Means - Cellule 5
# Faire des prédictions
clustered_df = model.transform(scaled_df)

# Étape 3 : Visualiser les résultats - Cellule 6
# Afficher un échantillon des résultats pour vérifier les catégories assignées
result_df = clustered_df.select("immatriculation", "marque", "modele", "puissance", "longueur", "nbPlaces", "nbPortes",
                                "couleur", "occasion", "prix", "category")
result_df.show(10)

# Étape 3 : Sauvegarder les résultats - Cellule 7
# Vous pouvez enregistrer les résultats dans Hive ou tout autre stockage pour une analyse ultérieure.
result_df.write.mode("overwrite").saveAsTable("immatriculations_with_categories")

# Étape 4 : Analyser les clusters - Cellule 8
# Facultativement, vous pouvez visualiser la distribution des catégories pour comprendre le clustering.

# Collecter les données pour la visualisation
category_counts = result_df.groupBy("category").count().orderBy("category").collect()
categories = [row["category"] for row in category_counts]
counts = [row["count"] for row in category_counts]

# Étape 4 : Analyser les clusters - Cellule 9
# Tracer la distribution des catégories
plt.figure(figsize=(10, 6))
sns.barplot(x=categories, y=counts)
plt.xlabel("Catégorie")
plt.ylabel("Nombre de véhicules")
plt.title("Distribution des véhicules par catégorie")
plt.show()

# Étape 5 : Calculer les moyennes des caractéristiques par catégorie - Cellule 10
# Analyser les caractéristiques moyennes des clusters
cluster_summary = result_df.groupBy("category").agg(
    avg("puissance").alias("moy_puissance"),
    avg("longueur").alias("moy_longueur"),
    avg("nbPlaces").alias("moy_nbPlaces"),
    avg("nbPortes").alias("moy_nbPortes"),
    avg("prix").alias("moy_prix")
)
cluster_summary.show()

# Étape 6 : Appliquer les labels aux nouvelles données de 'client_immatriculation_merged' - Cellule 11
# Charger la table 'client_immatriculation_merged'
client_immatriculation_df = spark.sql("SELECT * FROM client_immatriculation_merged")

# Convertir les colonnes nécessaires en type numérique
client_immatriculation_df = client_immatriculation_df.withColumn("puissance", col("puissance").cast(DoubleType()))
client_immatriculation_df = client_immatriculation_df.withColumn("longueur", col("longueur").cast(DoubleType()))
client_immatriculation_df = client_immatriculation_df.withColumn("nbplaces", col("nbplaces").cast(DoubleType()))
client_immatriculation_df = client_immatriculation_df.withColumn("nbportes", col("nbportes").cast(DoubleType()))
client_immatriculation_df = client_immatriculation_df.withColumn("prix", col("prix").cast(DoubleType()))

# Remplacer les valeurs nulles par une valeur par défaut
client_immatriculation_df = client_immatriculation_df.fillna({
    "puissance": 0.0,
    "longueur": 0.0,
    "nbplaces": 0.0,
    "nbportes": 0.0,
    "prix": 0.0
})

# Assembler les caractéristiques dans un vecteur de caractéristiques
client_assembler = VectorAssembler(inputCols=["puissance", "longueur", "nbplaces", "nbportes", "prix"],
                                   outputCol="features", handleInvalid="skip")
client_assembled_df = client_assembler.transform(client_immatriculation_df)

# Standardiser les caractéristiques
client_scaled_df = scaler_model.transform(client_assembled_df)

# Supprimer la colonne 'scaled_features' car elle n'est plus nécessaire
drop_columns = ['scaled_features']
client_scaled_df = client_scaled_df.drop(*drop_columns)

# Faire des prédictions pour les nouvelles données
client_clustered_df = model.transform(client_scaled_df)

# Ajouter la catégorie numérique aux données des clients
client_labeled_df = client_clustered_df

# Étape supplémentaire : Visualiser la distribution des catégories dans 'client_labeled_df'
client_category_counts = client_labeled_df.groupBy("category").count().orderBy("category").collect()
client_categories = [row["category"] for row in client_category_counts]
client_counts = [row["count"] for row in client_category_counts]

# Tracer la distribution des catégories dans 'client_labeled_df'
plt.figure(figsize=(10, 6))
sns.barplot(x=client_categories, y=client_counts)
plt.xlabel("Catégorie")
plt.ylabel("Nombre de clients")
plt.title("Distribution des clients par catégorie")
plt.show()
client_labeled_df.select("marque", "modele", "puissance", "category").show(10)

# Sauvegarder les résultats avec les catégories
client_labeled_df.write.mode("overwrite").saveAsTable("client_immatriculation_with_labels")
