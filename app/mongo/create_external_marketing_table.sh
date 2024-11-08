#!/bin/bash

# Nom du conteneur où Hive est installé
CONTAINER_NAME="hive-server" # Remplacez par le nom réel de votre conteneur

# Script SQL à exécuter dans Hive
SQL_SCRIPT="
CREATE DATABASE IF NOT EXISTS concessionnaire;
USE concessionnaire;
DROP TABLE IF EXISTS marketing_table;

CREATE EXTERNAL TABLE IF NOT EXISTS marketing_table (
    deuxieme_voiture string,
    age INT,
    nbEnfantsAcharge INT,
    sexe STRING,
    situationFamiliale STRING,
    taux INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/concessionnaire.db/marketing_table';
"

# Exécution de la commande SQL dans le conteneur Docker
docker exec -i $CONTAINER_NAME /bin/bash -c "hive -e \"$SQL_SCRIPT\""
