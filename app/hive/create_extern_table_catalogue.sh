#!/bin/bash

CSV_DIRECTORY="/data/catalogue"
CATALOGUE_TABLE_NAME="catalogue_data"
DB_NAME="client"

CATALOGUE_COLUMNS="marque STRING, nom STRING, puissance INT, longueur STRING, nbplaces INT, nbportes INT, couleur STRING, occasion BOOLEAN, prix FLOAT"
DELIMITER=","

docker exec -it hive-server bash -c "
  hive -e '
    CREATE DATABASE IF NOT EXISTS ${DB_NAME};
    USE ${DB_NAME};
    DROP TABLE IF EXISTS ${CATALOGUE_TABLE_NAME};
    CREATE EXTERNAL TABLE ${CATALOGUE_TABLE_NAME} (${CATALOGUE_COLUMNS})
    ROW FORMAT DELIMITED FIELDS TERMINATED BY \"${DELIMITER}\"
    STORED AS TEXTFILE LOCATION \"hdfs://namenode:9000${CSV_DIRECTORY}\"
    TBLPROPERTIES (\"skip.header.line.count\"=\"1\");
  '
"
echo 'Table externe Catalogue créée dans Hive.'
