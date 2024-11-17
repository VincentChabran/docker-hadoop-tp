from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
import seaborn as sns

spark = SparkSession.builder \
    .appName("Analyse Immatriculations") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE concessionnaire")
immatriculation_df = spark.sql("SELECT * FROM immatriculations_data")

immatriculation_df = immatriculation_df.filter(immatriculation_df.immatriculation != "immatriculation")
immatriculation_df.show(20)


immatriculation_df.describe().show()

null_counts = {column: immatriculation_df.filter(F.col(column).isNull()).count() for column in immatriculation_df.columns}
print(null_counts)

immatriculation_df.select("longueur").distinct().show()

immatriculation_df = immatriculation_df.withColumn(
    "longueur",
    F.regexp_replace("longueur", "�", "è")
)
immatriculation_df.select("longueur").distinct().show()


nombre_de_modeles_et_marques = immatriculation_df.agg(
    F.countDistinct("nom").alias("nombre_de_modeles"),
    F.countDistinct("marque").alias("nombre_de_marques")
)
nombre_de_modeles_et_marques.show()

nombre_modeles_par_marque = immatriculation_df.groupBy("marque").agg(
    F.countDistinct("nom").alias("nombre_de_modeles")
)
nombre_modeles_par_marque.orderBy(F.desc("nombre_de_modeles")).show()

#Graph 
nombre_modeles_par_marque_pandas = nombre_modeles_par_marque.toPandas().sort_values(by="nombre_de_modeles", ascending=False)
plt.figure(figsize=(10, 6))
plt.bar(nombre_modeles_par_marque_pandas['marque'], nombre_modeles_par_marque_pandas['nombre_de_modeles'])
plt.xlabel('Marques')
plt.ylabel('Nb de modèles')
plt.title('Modèles par marque')
plt.xticks(rotation=90)
plt.show()


nombre_immatriculations_par_modele = immatriculation_df.groupBy("marque").agg(
    F.count("*").alias("nombre_d_immatriculations")
)
nombre_immatriculations_par_modele.orderBy(F.desc("nombre_d_immatriculations")).show(100)


nombre_immatriculations_par_modele_pandas = nombre_immatriculations_par_modele.orderBy(F.desc("nombre_d_immatriculations")).toPandas()

plt.figure(figsize=(12, 6))
plt.bar(nombre_immatriculations_par_modele_pandas['marque'], nombre_immatriculations_par_modele_pandas['nombre_d_immatriculations'])
plt.xlabel('Modèle')
plt.ylabel('Nombre d\'immatriculations')
plt.title('Marques immatriculés sur l \'année')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# Calculer la puissance moyenne par marque
puissance_moyenne_par_marque = immatriculation_df.groupBy("marque").agg(
    F.avg("puissance").alias("puissance_moyenne")
)

# Trier par ordre décroissant de la puissance moyenne
puissance_moyenne_par_marque = puissance_moyenne_par_marque.orderBy(F.desc("puissance_moyenne"))

# Convertir en DataFrame Pandas
puissance_moyenne_par_marque_pandas = puissance_moyenne_par_marque.toPandas()

# Limiter aux 10 marques avec la plus grande puissance moyenne
top_10_puissance = puissance_moyenne_par_marque_pandas.head(10)

# Créer un graphique en barres
plt.figure(figsize=(12, 6))
sns.barplot(data=top_10_puissance, x='marque', y='puissance_moyenne')

plt.xlabel('Marque')
plt.ylabel('Puissance Moyenne (CV)')
plt.title('Puissance moyenne des véhicules par marque')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


nombre_immatriculations_par_modele = immatriculation_df.groupBy("nom").agg(
    F.count("*").alias("nombre_d_immatriculations")
)

nombre_immatriculations_par_modele.orderBy(F.desc("nombre_d_immatriculations")).show(100)


immatriculations_par_longueur = immatriculation_df.groupBy("longueur").agg(
    F.count("*").alias("nombre_d_immatriculations")
)

immatriculations_par_longueur = immatriculations_par_longueur.orderBy(F.desc("nombre_d_immatriculations"))
immatriculations_par_longueur.show()

immatriculations_par_longueur_pandas = immatriculations_par_longueur.toPandas()

plt.figure(figsize=(8, 8))
plt.pie(
    immatriculations_par_longueur_pandas['nombre_d_immatriculations'], 
    labels=immatriculations_par_longueur_pandas['longueur'], 
    autopct='%1.1f%%',
    startangle=140,
    colors=sns.color_palette('muted'),
)
plt.title('Immatriculations par catégorie de longueur')
plt.tight_layout()
plt.show()


immatriculations_par_longueur = immatriculation_df.groupBy("occasion").agg(
    F.count("*").alias("nombre_d_immatriculations")
)

immatriculations_par_longueur = immatriculations_par_longueur.orderBy(F.desc("nombre_d_immatriculations"))
immatriculations_par_longueur.show()

#Graph
immatriculations_par_longueur_pandas = immatriculations_par_longueur.toPandas()
immatriculations_par_longueur_pandas['label'] = immatriculations_par_longueur_pandas['occasion'].apply(
    lambda x: "Occasion" if x else "Neuf"
)

plt.figure(figsize=(8, 8))
plt.pie(
    immatriculations_par_longueur_pandas['nombre_d_immatriculations'], 
    labels=immatriculations_par_longueur_pandas['label'], 
    autopct='%1.1f%%',
    startangle=140,
    colors=sns.color_palette('muted'),
)
plt.title('Immatriculations par catégorie de longueur')
plt.tight_layout()
plt.show()

