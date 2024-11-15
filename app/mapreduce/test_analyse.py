import pandas as pd
import matplotlib.pyplot as plt

# Charger les données CSV
client_data = pd.read_csv('data/Clients_0.csv', encoding='ISO-8859-1')
immatriculations_data = pd.read_csv('data/Immatriculations.csv', encoding='ISO-8859-1')

# Étape 1: Identifier les immatriculations en doublon
immatriculations_doublons = immatriculations_data['immatriculation'].value_counts()
immatriculations_doublons = immatriculations_doublons[immatriculations_doublons > 1].index

# Étape 2: Trouver les clients ayant des immatriculations en doublon
clients_doublons = client_data[client_data['immatriculation'].isin(immatriculations_doublons)]
nombre_clients_doublons = clients_doublons['immatriculation'].nunique()

# Étape 3: Calculer le nombre total de clients
nombre_total_clients = client_data['immatriculation'].nunique()

# Calculer le taux de clients avec une immatriculation en doublon
taux_clients_doublons = (nombre_clients_doublons / nombre_total_clients) * 100

# Afficher le taux
print(f"Taux de clients avec une immatriculation en doublon : {taux_clients_doublons:.2f}%")

# Étape 4: Visualiser le résultat sous forme de graphique
labels = ['Clients avec Doublons', 'Clients sans Doublons']
values = [taux_clients_doublons, 100 - taux_clients_doublons]

plt.figure(figsize=(10, 6))
plt.bar(labels, values, color=['red', 'green'])
plt.ylabel('Pourcentage (%)')
plt.title('Taux de Clients avec une Immatriculation en Doublon')
plt.ylim(0, 100)

# Ajouter les valeurs sur chaque barre
for i, v in enumerate(values):
    plt.text(i, v + 1, f"{v:.2f}%", ha='center', fontsize=12)

# Afficher le graphique
plt.savefig('app/graphiques/doublons.png')

