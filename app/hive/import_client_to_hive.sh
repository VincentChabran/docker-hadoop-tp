#!/bin/bash

CSV_PATH="/data/clients"
TABLE_NAME="client_data"
DB_NAME="concessionnaire"

COLUMN_NAMES="age STRING, sexe STRING, taux STRING, situationfamilliale STRING, nbenfantacharge STRING, deuxiemeVoiture STRING, immatriculation STRING"
DELIMITER=","

docker exec -it hive-server bash -c "
  echo 'CREATE DATABASE IF NOT EXISTS ${DB_NAME};' > /tmp/create_table.sql
  echo 'USE ${DB_NAME};' >> /tmp/create_table.sql
  echo 'DROP TABLE IF EXISTS ${TABLE_NAME};' >> /tmp/create_table.sql
  echo 'CREATE TABLE ${TABLE_NAME} (${COLUMN_NAMES})' >> /tmp/create_table.sql
  echo 'ROW FORMAT DELIMITED FIELDS TERMINATED BY \"${DELIMITER}\" STORED AS TEXTFILE' >> /tmp/create_table.sql
  echo 'TBLPROPERTIES (\"skip.header.line.count\"=\"1\");' >> /tmp/create_table.sql
  echo 'LOAD DATA INPATH \"${CSV_PATH}\" INTO TABLE ${TABLE_NAME};' >> /tmp/create_table.sql
  /opt/hive/bin/hive -f /tmp/create_table.sql
"

echo 'Importation de Client dans Hive terminée.'
