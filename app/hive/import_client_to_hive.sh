#!/bin/bash

CSV_PATH="/data/Clients_0.csv"
TABLE_NAME="client_data"
DB_NAME="client"

COLUMN_NAMES="age INT, sexe STRING, taux FLOAT, situationfamilliale STRING, nbenfantacharge FLOAT, 2emevoiture BOOLEAN, immatriculation STRING"
DELIMITER=","

docker exec -it hive-server bash -c "
  echo 'CREATE DATABASE IF NOT EXISTS ${DB_NAME};' > /tmp/create_table.sql  # Créer la base si elle n'existe pas
  echo 'USE ${DB_NAME};' >> /tmp/create_table.sql  # Utiliser la base de données
  echo 'DROP TABLE IF EXISTS ${TABLE_NAME};' >> /tmp/create_table.sql  # Supprimer la table si elle existe
  echo 'CREATE TABLE ${TABLE_NAME} (${COLUMN_NAMES})' >> /tmp/create_table.sql  # Créer la table
  echo 'ROW FORMAT DELIMITED FIELDS TERMINATED BY \"${DELIMITER}\" STORED AS TEXTFILE;' >> /tmp/create_table.sql  # Définir le format
  echo 'LOAD DATA INPATH \"${CSV_PATH}\" INTO TABLE ${TABLE_NAME};' >> /tmp/create_table.sql  # Charger les données dans la table
  /opt/hive/bin/hive -f /tmp/create_table.sql  # Exécuter les commandes Hive
"

echo 'Importation de Client dans Hive terminée.'
