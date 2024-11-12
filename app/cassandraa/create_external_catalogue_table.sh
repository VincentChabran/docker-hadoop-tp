#!/bin/bash

CONTAINER_NAME="hive-server"

SQL_SCRIPT="
CREATE DATABASE IF NOT EXISTS concessionnaire;
USE concessionnaire;
DROP TABLE IF EXISTS catalogue_table;

CREATE EXTERNAL TABLE IF NOT EXISTS catalogue_table (
    id STRING,
    marque STRING,
    nom STRING,
    puissance INT,
    longueur STRING,
    nbPlaces INT,
    nbPortes INT,
    couleur STRING,
    occasion BOOLEAN,
    prix DOUBLE
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/concessionnaire.db/catalogue_table';
"