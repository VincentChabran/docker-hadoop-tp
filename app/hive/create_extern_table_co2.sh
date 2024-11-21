#!/bin/bash

CSV_DIRECTORY="/data/co2"  # Chemin du fichier CSV sur HDFS
CO2_TABLE_NAME="co2_data"  # Nom de la table Hive
DB_NAME="concessionnaire"  # Nom de la base de données Hive

# Définition des colonnes de la table Hive
CO2_COLUMNS="
  ligne INT,
  marque_modele STRING,
  bonus_malus STRING,
  rejets_co2 STRING,
  cout_energie STRING
"

DELIMITER=","  # Délimiteur utilisé dans le fichier CSV

# Création de la table Hive
docker exec -it hive-server bash -c "
  hive -e '
    CREATE DATABASE IF NOT EXISTS ${DB_NAME};
    USE ${DB_NAME};
    DROP TABLE IF EXISTS ${CO2_TABLE_NAME};
    CREATE EXTERNAL TABLE ${CO2_TABLE_NAME} (${CO2_COLUMNS})
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY \"${DELIMITER}\"
    STORED AS TEXTFILE
    LOCATION \"hdfs://namenode:9000${CSV_DIRECTORY}\"
    TBLPROPERTIES (\"skip.header.line.count\"=\"1\");
  '
"

echo 'Table externe Co2 créée dans Hive.'
