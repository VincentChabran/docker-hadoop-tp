import pandas as pd
import matplotlib.pyplot as plt

clients_df = pd.read_csv('data/Clients_0.csv', encoding='ISO-8859-1')
immatriculations_df = pd.read_csv('data/Immatriculations.csv', encoding='ISO-8859-1')

fusion_df = pd.merge(clients_df, immatriculations_df, on='immatriculation', how='inner')

fusion_df['age'] = pd.to_numeric(fusion_df['age'], errors='coerce')
# Gérer les valeurs manquantes
# fusion_df['age'].fillna(fusion_df['age'].mean(), inplace=True)

# # Tracer l'histogramme
# plt.figure(figsize=(10, 5))
# fusion_df['age'].plot(kind='hist', bins=20, title='Distribution de l\'âge des clients')
# plt.xlabel('Âge')
# plt.ylabel('Fréquence')
# plt.savefig('app/graphiques/distribution_age_clients.png')
# plt.close()

# # Tracer un diagramme en barres
# plt.figure(figsize=(10, 5))
# fusion_df['couleur'].value_counts().plot(kind='bar', title='Distribution des couleurs des voitures')
# plt.xlabel('Couleur')
# plt.ylabel('Nombre de véhicules')
# plt.savefig('app/graphiques/distribution_couleur_voitures.png')
# plt.close()

# # BoxPlot Vente par marques prix
# plt.figure(figsize=(15, 8))
# fusion_df.boxplot(column='prix', by='marque', rot=90)
# plt.title('Distribution des prix par marque')
# plt.suptitle('')
# plt.xlabel('Marque')
# plt.ylabel('Prix')
# plt.tight_layout()
# plt.savefig('app/graphiques/boxplot_prix_par_marque.png')
# plt.close()

# # ScatterPlot
# plt.figure(figsize=(10, 6))
# plt.scatter(fusion_df['puissance'], fusion_df['prix'])
# plt.title('Relation entre la puissance et le prix des voitures')
# plt.xlabel('Puissance (CV)')
# plt.ylabel('Prix (EUR)')
# plt.savefig('app/graphiques/puissance_vs_prix.png')
# plt.close()

# # Histogramme
# plt.figure(figsize=(10, 5))
# fusion_df['puissance'].plot(kind='hist', bins=20, title='Distribution de la puissance des voitures')
# plt.xlabel('Puissance (CV)')
# plt.ylabel('Fréquence')
# plt.savefig('app/graphiques/distribution_puissance_voitures.png')
# plt.close()

plt.figure(figsize=(10, 5))
moyenne_prix = fusion_df.groupby('situationFamiliale')['prix'].mean()
moyenne_prix.plot(kind='bar', title='Prix moyen des voitures par situation familiale')
plt.xlabel('Situation Familiale')
plt.ylabel('Prix Moyen (EUR)')
plt.savefig('prix_moyen_par_situation_familiale.png')
plt.close()



