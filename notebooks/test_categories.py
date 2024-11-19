# Importer les bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("Analyse Immatriculations") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Utilisation de la base de données Hive
spark.sql("USE concessionnaire")

# Charger les données des immatriculations traitées
immatriculation_df = spark.sql("SELECT * FROM immatriculations_processed")

# Vérification des données avant le traitement
# Créer un sous-échantillon des données, par exemple 10% des lignes
immatriculation_sample_df = immatriculation_df.sample(fraction=0.1, seed=42)

# Convertir le sous-échantillon en Pandas
immatriculations_df_pandas = immatriculation_sample_df.toPandas()

# Vérifier si le DataFrame est vide
if immatriculations_df_pandas.empty:
    print("Le DataFrame est vide après la conversion en Pandas. Veuillez vérifier les données initiales.")
else:
    # Vérifier les valeurs uniques de la colonne "longueur"
    print("Valeurs uniques dans la colonne 'longueur' avant traitement :")
    print(immatriculations_df_pandas['longueur'].unique())

    # Supprimer les lignes avec des valeurs manquantes
    immatriculations_df_pandas.dropna(subset=['puissance', 'prix', 'longueur', 'nbplaces'], inplace=True)

    # Vérifier à nouveau si le DataFrame est vide après suppression des valeurs manquantes
    if immatriculations_df_pandas.empty:
        print("Le DataFrame est vide après suppression des valeurs manquantes.")
    else:
        # Sélectionner les caractéristiques pertinentes
        features = ['puissance', 'prix', 'longueur', 'nbplaces']
        immatriculations_df_clustering = immatriculations_df_pandas[features].copy()

        # Encoder les valeurs catégorielles de "longueur" en utilisant .loc[]
        immatriculations_df_clustering.loc[:, 'longueur'] = immatriculations_df_clustering['longueur'].map({
            'courte': 0,
            'moyenne': 1,
            'longue': 2,
            'très longue': 3
        })

        # Vérifier s'il reste des valeurs non mappées dans la colonne "longueur"
        if immatriculations_df_clustering['longueur'].isnull().any():
            print("Attention : Certaines valeurs de 'longueur' n'ont pas été correctement mappées.")
            print(immatriculations_df_clustering['longueur'].unique())
            # Supprimer les lignes avec des valeurs non mappées
            immatriculations_df_clustering.dropna(subset=['longueur'], inplace=True)

        # Vérifier si le DataFrame est vide après le mappage
        if immatriculations_df_clustering.empty:
            print("Le DataFrame est vide après le mappage des valeurs de 'longueur'.")
        else:
            # Normaliser les données
            scaler = StandardScaler()
            features_normalized = scaler.fit_transform(immatriculations_df_clustering)

            # Étape 2 : Appliquer K-Means

            # Créer le modèle K-Means avec 10 clusters, en spécifiant `n_init`
            kmeans = KMeans(n_clusters=10, random_state=42, n_init='auto')
            immatriculations_df_pandas['categorie_kmeans'] = kmeans.fit_predict(features_normalized)

            # Afficher un échantillon des catégories créées
            print(immatriculations_df_pandas[['modele', 'puissance', 'prix', 'longueur', 'nbplaces', 'categorie_kmeans']].head())

            # Étape 3 : Analyser les Catégories Créées

            # Calculer des statistiques descriptives par catégorie
            categories_stats = immatriculations_df_pandas.groupby('categorie_kmeans').agg({
                'puissance': ['mean', 'min', 'max'],
                'prix': ['mean', 'min', 'max'],
                'nbplaces': ['mean'],
                'longueur': ['mean']
            })
            print(categories_stats)

            # Visualisation des catégories avec un sous-échantillon

            # Créer un sous-échantillon de 1% des données pour la visualisation
            immatriculations_sample_df = immatriculations_df_pandas.sample(frac=0.01, random_state=42)

            # Limiter le nombre de catégories affichées aux 3 plus fréquentes
            top_3_categories = immatriculations_df_pandas['categorie_kmeans'].value_counts().nlargest(3).index
            immatriculations_sample_df = immatriculations_sample_df[immatriculations_sample_df['categorie_kmeans'].isin(top_3_categories)]

            # Utiliser un barplot pour afficher la moyenne du prix par catégorie
            prix_moyen_par_categorie = immatriculations_sample_df.groupby('categorie_kmeans')['prix'].mean().reset_index()

            plt.figure(figsize=(14, 8))
            sns.barplot(x='categorie_kmeans', y='prix', data=prix_moyen_par_categorie, palette='viridis')
            plt.xlabel('Catégorie de Véhicule (K-Means)')
            plt.ylabel('Prix Moyen des Véhicules')
            plt.title('Prix Moyen par Catégorie de Véhicule (Top 3 des Catégories)')
            plt.tight_layout()

            # Enregistrer le graphique au lieu de l'afficher
            plt.savefig('prix_moyen_par_categorie.png')
            plt.close()

            # Étape 4 : Interprétation et Attribution des Noms de Catégories

            # Analyser chaque cluster pour déterminer des noms appropriés
            for cluster in range(10):
                cluster_data = immatriculations_df_pandas[immatriculations_df_pandas['categorie_kmeans'] == cluster]
                print(f"Cluster {cluster}:")
                print(cluster_data.describe())
                print("\n")

            # Étape 5 : Validation des Catégories
            # Discuter avec des experts ou analyser les graphiques pour vérifier la pertinence des catégories
