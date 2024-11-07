#!/bin/bash      

# Accéder au conteneur namenode et exécuter les commandes HDFS
docker exec -it namenode /bin/bash -c "
    echo 'Création du dossier cible dans HDFS, s'il n'existe pas...'
    hdfs dfs -mkdir -p /data

    echo 'Importation du fichier CSV dans HDFS...'
    hdfs dfs -put -f /data/Immatriculations.csv /data/Immatriculations.csv
    hdfs dfs -put -f /data/CO2.csv /data/CO2.csv
    hdfs dfs -put -f /data/Clients_0.csv /data/Clients_0.csv

    echo 'Vérification de l'importation :'
    hdfs dfs -ls /data
"

echo "Importation Hdfs terminée."
