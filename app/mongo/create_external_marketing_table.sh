#!/bin/bash

CONTAINER_NAME="hive-server" 

SQL_SCRIPT="
CREATE DATABASE IF NOT EXISTS concessionnaire;
USE concessionnaire;
DROP TABLE IF EXISTS marketing_table;

CREATE EXTERNAL TABLE IF NOT EXISTS marketing_table (
    deuxieme_voiture BOOLEAN,
    age INT,
    nbEnfantsAcharge INT,
    sexe STRING,
    situationFamiliale STRING,
    taux INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/concessionnaire.db/marketing_table';
"

docker exec -i $CONTAINER_NAME /bin/bash -c "hive -e \"$SQL_SCRIPT\""
