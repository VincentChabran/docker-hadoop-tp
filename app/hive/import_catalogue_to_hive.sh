#!/bin/bash

# Téléchargez le connecteur JDBC pour Cassandra
echo "Téléchargement du connecteur JDBC pour Cassandra..."
wget -O /hive/lib/cassandra-jdbc-driver.jar https://github.com/healthonnet/hon-cassandra-jdbc/releases/download/v1.2.5/hive-cassandra.jar

# Vérifiez si le fichier est bien téléchargé
if [ -f /hive/lib/cassandra-jdbc-driver.jar ]; then
    echo "Connecteur JDBC pour Cassandra téléchargé avec succès."
else
    echo "Erreur lors du téléchargement du connecteur JDBC pour Cassandra."
    exit 1
fi

# Accéder au shell Hive pour exécuter les commandes HiveQL
echo "Accès à Hive pour la création de la base de données et de la table externe..."
hive -e "
    -- Créer la base de données s'il n'existe pas déjà
    CREATE DATABASE IF NOT EXISTS db_keyspace;

    -- Utiliser la base de données db_keyspace
    USE db_keyspace;

    -- Créer la table externe connectée à Cassandra
    CREATE EXTERNAL TABLE IF NOT EXISTS catalogue (
        id STRING,
        marque STRING,
        nom STRING,
        puissance INT,
        longueur STRING,
        nbPlaces INT,
        nbPortes INT,
        couleur STRING,
        occasion BOOLEAN,
        prix FLOAT
    )
    STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
    TBLPROPERTIES (
        'cassandra.host'='cassandraa',
        'cassandra.port'='9042',
        'cassandra.ks.name'='db_keyspace',
        'cassandra.cf.name'='catalogue',
        'cassandra.cql.version'='3.0'
    );
"

echo "Création de la base de données et de la table externe dans Hive terminée."
