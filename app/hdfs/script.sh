#!/bin/bash

docker exec -it namenode /bin/bash -c "
    echo 'Création des dossiers cibles dans HDFS, s'ils n'existent pas'
    hdfs dfs -mkdir -p /data/immatriculations
    hdfs dfs -mkdir -p /data/co2
    hdfs dfs -mkdir -p /data/clients

    echo 'Importation du fichier Immatriculations.csv dans HDFS'
    hdfs dfs -put -f /data/Immatriculations.csv /data/immatriculations/Immatriculations.csv

    echo 'Importation du fichier CO2.csv dans HDFS'
    hdfs dfs -put -f /data/CO2.csv /data/co2/CO2.csv

    echo 'Importation du fichier Clients_0.csv dans HDFS'
    hdfs dfs -put -f /data/Clients_0.csv /data/clients/Clients_0.csv

    echo 'Vérification de l\'importation :'
    hdfs dfs -ls /data
"

echo "Importation terminée."
