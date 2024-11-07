#!/bin/bash

CSV_DIRECTORY="/data"
IMMAT_TABLE_NAME="immatriculations_data"
DB_NAME="client"

IMMAT_COLUMNS="immatriculation STRING, marque STRING, nom STRING, puissance INT, longueur STRING, nbplaces INT, nbportes INT, couleur STRING, occasion BOOLEAN, prix INT"
DELIMITER=","

docker exec -it hive-server bash -c "
  hive -e '
    CREATE DATABASE IF NOT EXISTS ${DB_NAME};
    USE ${DB_NAME};
    DROP TABLE IF EXISTS ${IMMAT_TABLE_NAME};
    CREATE EXTERNAL TABLE ${IMMAT_TABLE_NAME} (${IMMAT_COLUMNS})
    ROW FORMAT DELIMITED FIELDS TERMINATED BY \"${DELIMITER}\"
    STORED AS TEXTFILE LOCATION \"hdfs://namenode:9000${CSV_DIRECTORY}\";
  '
"
echo 'Table externe Immatriculations créée dans Hive.'
