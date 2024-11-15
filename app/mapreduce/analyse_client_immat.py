import pandas as pd

clients_df = pd.read_csv('data/Clients_0.csv', encoding='ISO-8859-1')
immatriculations_df = pd.read_csv('data/Immatriculations.csv', encoding='ISO-8859-1')

fusion_df = pd.merge(clients_df, immatriculations_df, on='immatriculation', how='inner')
print(fusion_df.head())

print(fusion_df.describe())

fusion_df['age'] = pd.to_numeric(fusion_df['age'], errors='coerce')
# Gérer les valeurs manquantes
fusion_df['age'].fillna(fusion_df['age'].mean(), inplace=True)

# Compter le nombre de clients par sexe
print(fusion_df['sexe'].value_counts())

# # Compter le nombre de véhicules par couleur
# print(fusion_df['couleur'].value_counts())

# # Moyenne, médiane, et écart-type de l'âge des clients
# print("Moyenne de l'âge :", fusion_df['age'].mean())
# print("Médiane de l'âge :", fusion_df['age'].median())
# print("Ecart-Type de l'âge :", fusion_df['age'].std())

# # Moyenne, médiane, et écart-type de la puissance des voitures
# print("Moyenne de la puissance :", fusion_df['puissance'].mean())
# print("Médiane de la puissance :", fusion_df['puissance'].median())
# print("Ecart-Type de la puissance :", fusion_df['puissance'].std())

# # Moyenne, médiane, et écart-type du prix des voitures
# print("Moyenne du prix :", fusion_df['prix'].mean())
# print("Médiane du prix :", fusion_df['prix'].median())
# print("Ecart-Type du prix :", fusion_df['prix'].std())



clients_unique = fusion_df['immatriculation'].nunique()
print(f"Nombre de clients uniques (basé sur immatriculation) : {clients_unique}")

