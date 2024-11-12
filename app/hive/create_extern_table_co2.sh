#!/bin/bash

CSV_DIRECTORY="/data/co2"
CO2_TABLE_NAME="co2_data"
DB_NAME="client"

CO2_COLUMNS="marque STRING, modele STRING, bonus_malus STRING, rejets_co2 STRING, cout_energie STRING"
DELIMITER=","

docker exec -it hive-server bash -c "
  hive -e '
    CREATE DATABASE IF NOT EXISTS ${DB_NAME};
    USE ${DB_NAME};
    DROP TABLE IF EXISTS ${CO2_TABLE_NAME};
    CREATE EXTERNAL TABLE ${CO2_TABLE_NAME} (${CO2_COLUMNS})
    ROW FORMAT DELIMITED FIELDS TERMINATED BY \"${DELIMITER}\"
    STORED AS TEXTFILE LOCATION \"hdfs://namenode:9000${CSV_DIRECTORY}\"
    TBLPROPERTIES (\"skip.header.line.count\"=\"1\");
  '
"
echo 'Table externe Co2 créée dans Hive.'
