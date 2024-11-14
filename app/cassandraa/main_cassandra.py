import subprocess

from app.cassandraa.cassandra_client import execute_cassandra_operations

def delete_hive_table_and_hdfs():
    """
    Supprime la table Hive existante et les fichiers HDFS associés.
    """
    print("Suppression de la table Hive existante et des fichiers HDFS associés")
    try:
        # Supprimer la table existante pour éviter les doublons
        drop_table_cmd = (
            "docker exec -it hive-server /bin/bash -c "
            "\"hive -e 'USE concessionnaire; DROP TABLE IF EXISTS catalogue_table;'\""
        )
        result_drop = subprocess.run(drop_table_cmd, capture_output=True, text=True, check=True)
        print("Table 'catalogue_table' supprimée si elle existait auparavant.")
        print("Résultat de la suppression de la table :")
        print(result_drop.stdout)

        # Supprimer les fichiers HDFS associés (car la table est externe)
        delete_hdfs_cmd = (
            "docker exec -it namenode /bin/bash -c "
            "\"hadoop fs -rm -r -f /user/hive/warehouse/concessionnaire.db/catalogue_table\""
        )
        result_delete_hdfs = subprocess.run(delete_hdfs_cmd, capture_output=True, text=True, check=True)
        print("Fichiers HDFS associés supprimés.")
        print("Résultat de la suppression des fichiers HDFS :")
        print(result_delete_hdfs.stdout)

    except subprocess.CalledProcessError as e:
        print("Erreur lors de la suppression de la table Hive ou des fichiers HDFS :")
        print(e.stderr)

def create_external_catalogue_table():
    """
    Création de la table externe catalogue dans Hive.
    """
    print("Création de la table externe catalogue dans Hive")
    try:
        result = subprocess.run(
            ["docker", "exec", "-it", "hive-server", "/bin/bash", "-c", "hive -e \"CREATE DATABASE IF NOT EXISTS concessionnaire; USE concessionnaire; DROP TABLE IF EXISTS catalogue_table; CREATE EXTERNAL TABLE IF NOT EXISTS catalogue_table (id STRING, marque STRING, nom STRING, puissance INT, longueur STRING, nbPlaces INT, nbPortes INT, couleur STRING, occasion BOOLEAN, prix FLOAT) STORED AS PARQUET LOCATION '/user/hive/warehouse/concessionnaire.db/catalogue_table';\""],
            capture_output=True, text=True, check=True)
        print("Résultat de la création de la table externe Hive :")
        print(result)
    except subprocess.CalledProcessError as e:
        print("Erreur lors de la création de la table Hive :")
        print(e.stderr)

def check_and_delete_hive_table():
    """
    Vérifie si la table catalogue_table existe, puis la supprime si nécessaire
    """
    print("Vérification de l'existence de la table Hive et suppression si nécessaire")
    try:
        check_table_cmd = (
            "docker exec -it hive-server /bin/bash -c "
            "\"hive -e 'USE concessionnaire; SHOW TABLES LIKE \\\"catalogue_table\\\";'\""
        )
        result_check = subprocess.run(check_table_cmd, capture_output=True, text=True, check=True)
        if "catalogue_table" in result_check.stdout:
            delete_hive_table_and_hdfs()
        else:
            print("La table 'catalogue_table' n'existe pas, aucune suppression nécessaire.")
    except subprocess.CalledProcessError as e:
        print("Erreur lors de la vérification ou de la suppression de la table Hive :")
        print(e.stderr)

def check_and_delete_hdfs_directory():
    """
    Vérifie et supprime le répertoire HDFS associé à la table si nécessaire.
    """
    print("Vérification de l'existence du répertoire HDFS et suppression si nécessaire")
    try:
        check_hdfs_cmd = (
            "docker exec -it namenode /bin/bash -c "
            "\"hadoop fs -test -d /user/hive/warehouse/concessionnaire.db/catalogue_table\""
        )
        result_check_hdfs = subprocess.run(check_hdfs_cmd, capture_output=True, text=True)
        if result_check_hdfs.returncode == 0:
            # Le répertoire existe, on le supprime
            delete_hdfs_cmd = (
                "docker exec -it namenode /bin/bash -c "
                "\"hadoop fs -rm -r -f /user/hive/warehouse/concessionnaire.db/catalogue_table\""
            )
            result_delete_hdfs = subprocess.run(delete_hdfs_cmd, capture_output=True, text=True, check=True)
            print("Fichiers HDFS associés supprimés.")
        else:
            print("Le répertoire HDFS n'existe pas, aucune suppression nécessaire.")
    except subprocess.CalledProcessError as e:
        print("Erreur lors de la vérification ou de la suppression du répertoire HDFS :")
        print(e.stderr)

def run_spark_for_cassandra_to_hive():
    print("Lancement du script Spark pour transférer les données de Cassandra vers Hive")
    try:
        exec_bash_cmd = ["docker", "exec", "-it", "spark-master", "/bin/bash", "-c"]

        spark_submit_cmd = (
            "/spark/bin/spark-submit "
            "--jars /opt/spark/jars/spark-cassandra-connector-assembly_2.12-3.1.0.jar "
            "/spark_cassandra.py"
        )

        result = subprocess.run(exec_bash_cmd + [spark_submit_cmd], check=True, text=True, capture_output=True)

        print("Résultat de spark-submit :")
        print(result.stdout)

    except subprocess.CalledProcessError as e:
        print("Erreur lors de l'exécution de la commande :", e)
        print("Sortie d'erreur :", e.stderr)

    except Exception as e:
        print("Erreur inattendue :", e)

def orchestration_cassandra_task():
    """
    Orchestration des tâches Cassandra.
    """
    print("**************************************************************")
    print("Début de l'orchestration des tâches Cassandra")
    print("**************************************************************")

    # Étape 1 : Charger les données dans Cassandra depuis le fichier CSV
    execute_cassandra_operations("data/Catalogue.csv")

    # # Étape 2 : Vérifier et supprimer la table existante si nécessaire
    # check_and_delete_hive_table()

    # # Étape 3 : Vérifier et supprimer le répertoire HDFS si nécessaire
    # check_and_delete_hdfs_directory()

    # Étape 4 : Créer la table externe dans Hive
    create_external_catalogue_table()

    # Étape 5 : Transférer les données de Cassandra à Hive avec Spark
    run_spark_for_cassandra_to_hive()

    print("**************************************************************")
    print("Fin de l'orchestration des tâches Cassandra")
    print("**************************************************************")
