#!/bin/bash

CSV_FILE="Immatriculations.csv"          
LOCAL_PATH="/data/$CSV_FILE"       
HDFS_PATH="/data/$CSV_FILE"         

# Accéder au conteneur namenode et exécuter les commandes HDFS
docker exec -it namenode /bin/bash -c "
    echo 'Création du dossier cible dans HDFS, s'il n'existe pas...'
    hdfs dfs -mkdir -p /data

    echo 'Importation du fichier CSV dans HDFS...'
    hdfs dfs -put -f $LOCAL_PATH $HDFS_PATH

    echo 'Vérification de l'importation :'
    hdfs dfs -ls /data
"

echo "Importation terminée."
