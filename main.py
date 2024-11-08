# main.py
import os
import subprocess
from app.mongo.script import load_data_to_mongo
from app.cassandraa.cassandra_client import create_keyspace, create_table, push_data_to_cassandra, \
    get_cassandra_session, \
    close_connection


def load_to_mongo():
    print("Chargement des données dans MongoDB...")
    load_data_to_mongo("data/Marketing.csv", "concessionnaire", "marketing")


def import_csv_to_hdfs():
    try:
        result = subprocess.run(["bash", "app/hdfs/script.sh"], capture_output=True, text=True, check=True)

        print("Résultat de l'importation dans HDFS :")
        print(result.stdout)

    except subprocess.CalledProcessError as e:
        print("Erreur lors de l'importation dans HDFS :")
        print(e.stderr)


def import_csv_to_hive():
    try:
        result = subprocess.run(["bash", "app/hive/import_client_to_hive.sh"], capture_output=True, text=True,
                                check=True)
        print("Résultat de l'importation dans Hive :")
        print(result.stdout)

    except subprocess.CalledProcessError as e:
        print("Erreur lors de l'importation dans Hive :")
        print(e.stderr)


def create_external_table_immat():
    try:
        result = subprocess.run(["bash", "app/hive/create_extern_table_immatriculations.sh"], capture_output=True,
                                text=True, check=True)
        print("Table externe Immatriculations créée dans Hive avec succès :")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Erreur lors de la création de la table Immatriculations dans Hive :")
        print(e.stderr)


def run_hive_import_script():
    try:
        # Définir le conteneur Hive qui va exécuter le script
        container_name = "hive-server"

        # Chemin du script à l'intérieur du conteneur Docker
        script_path_in_container = "/hive_scripts/import_client_to_hive.sh"

        # Commande pour exécuter le script dans le conteneur Hive
        command = [
            "docker", "exec", "-it", container_name, "/bin/bash", "-c", f"bash {script_path_in_container}"
        ]

        # Exécution de la commande Docker
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print("Script Hive exécuté avec succès :", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Erreur lors de l'exécution du script Hive :", e.stderr)


def run_spark_job():
    try:
        # Utiliser le chemin complet vers spark-submit qui fonctionne
        command = [
            "docker", "exec", "spark", "/usr/bin/spark-submit",
            "--jars",
            "/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.1.0.jar,/opt/bitnami/spark/jars/mongo-spark-connector_2.12-3.0.1.jar",
            "/spark_scripts/cassandra_to_hive.py"
        ]

        # Remplacer capture_output par stdout et stderr explicites
        result = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        print("Spark job finished successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("An error occurred while running the Spark job.")
        print(e.stderr)


if __name__ == "__main__":
    print("Début de l'orchestration des tâches")
    # Crée une session Cassandra
    session = get_cassandra_session()
    print("Session Cassandra créée.")

    create_keyspace(session)
    print("Keyspace créé.")

    create_table(session)
    print("Table créée.")

    # run_hive_import_script();


    csv_file_path = "data/Catalogue.csv"
    # Pousse les données du CSV vers Cassandra
    push_data_to_cassandra(csv_file_path, session)

    # Ferme la connexion à Cassandra
    close_connection(session)  # Assurez-vous que le chemin soit correct

    run_spark_job();
    print("Script Hive pour cassandra exécuté avec succès.")

    load_to_mongo()
    import_csv_to_hdfs()
    import_csv_to_hive()
    create_external_table_immat()

    print("Orchestration terminée.")