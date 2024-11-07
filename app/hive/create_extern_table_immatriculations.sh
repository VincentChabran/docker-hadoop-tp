#!/bin/bash

IMMAT_CSV_PATH="/data/Immatriculations.csv"
IMMAT_TABLE_NAME="immatriculations_data"
DB_NAME="client"

IMMAT_COLUMNS="immatriculation STRING, marque STRING, nom STRING, puissance INT, longueur STRING, nbplaces INT, nbportes INT, couleur STRING, occasion BOOLEAN, prix INT"

DELIMITER=","

docker exec -it hive-server bash -c "
  echo 'CREATE DATABASE IF NOT EXISTS ${DB_NAME};' > /tmp/create_immat_table.sql
  echo 'USE ${DB_NAME};' >> /tmp/create_immat_table.sql
  echo 'DROP TABLE IF EXISTS ${IMMAT_TABLE_NAME};' >> /tmp/create_immat_table.sql
  echo 'CREATE EXTERNAL TABLE ${IMMAT_TABLE_NAME} (${IMMAT_COLUMNS})' >> /tmp/create_immat_table.sql
  echo 'ROW FORMAT DELIMITED FIELDS TERMINATED BY \"${DELIMITER}\"' >> /tmp/create_immat_table.sql
  echo 'STORED AS TEXTFILE LOCATION \"${IMMAT_CSV_PATH}\";' >> /tmp/create_immat_table.sql
  /opt/hive/bin/hive -f /tmp/create_immat_table.sql
"

echo 'Table externe Immatriculations créée avec succès dans Hive.'
