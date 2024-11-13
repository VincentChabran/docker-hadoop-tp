#!/bin/bash

# Répertoire HDFS où les fichiers traités Parquet ou CSV sont stockés
CSV_DIRECTORY="/user/hive/warehouse/concessionnaire.db/co2_data_processed"

# Nom de la table Hive à créer
CO2_TABLE_NAME="co2_data_processed"

# Nom de la base de données Hive
DB_NAME="concessionnaire"

CO2_COLUMNS="marque STRING, modele STRING, modele_detail STRING, horse_power STRING, unit STRING, unified_horse_power STRING, bonus_malus STRING, cout_energie STRING, rejets_co2 STRING"

DELIMITER=","

# Exécuter la commande Hive à l'intérieur du conteneur Docker
docker exec -it hive-server bash -c "
  hive -e '
    -- Créer la base de données si elle n'existe pas
    CREATE DATABASE IF NOT EXISTS ${DB_NAME};
    USE ${DB_NAME};

    -- Supprimer la table existante si elle existe
    DROP EXTERNAL TABLE IF EXISTS ${CO2_TABLE_NAME};

    -- Créer la table externe Hive
    CREATE EXTERNAL TABLE ${CO2_TABLE_NAME} (${CO2_COLUMNS})
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY \"${DELIMITER}\"
    STORED AS PARQUET
    LOCATION \"hdfs://namenode:9000${CSV_DIRECTORY}\"
    TBLPROPERTIES (\"skip.header.line.count\"=\"1\");
  '
"

# Message de confirmation
echo 'Table externe CO2 créée dans Hive avec succès.'
