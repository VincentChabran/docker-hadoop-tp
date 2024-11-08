# main.py
import subprocess
from app.mongo.script import load_data_to_mongo
from app.cassandraa.cassandra_client import create_keyspace, create_table, push_data_to_cassandra, \
    get_cassandra_session, \
    close_connection, drop_all_keyspaces


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
        result = subprocess.run(["bash", "app/hive/import_client_to_hive.sh"], capture_output=True, text=True, check=True)
        print("Résultat de l'importation dans Hive :")
        print(result.stdout)
        
    except subprocess.CalledProcessError as e:
        print("Erreur lors de l'importation dans Hive :")
        print(e.stderr)

def create_external_table_immat():
    try:
        result = subprocess.run(["bash", "app/hive/create_extern_table_immatriculations.sh"], capture_output=True, text=True, check=True)
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
        # Définir la commande spark-submit avec les arguments appropriés
        command = [
            "/opt/spark/bin/spark-submit",  # Chemin vers l'exécutable spark-submit
            "--jars",
            "/opt/spark/jars/spark-cassandra-connector_2.12-3.1.0.jar,/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar",
            "/spark_scripts/cassandra_to_hive.py"  # Chemin vers votre script Spark
        ]

        # Utiliser subprocess pour exécuter la commande
        result = subprocess.run(command, check=True, text=True, capture_output=True)

        # Afficher la sortie du job Spark
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

   run_hive_import_script();

   print("Script Hive pour cassandra exécuté avec succès.")

   csv_file_path = "data/Catalogue.csv"
   # Pousse les données du CSV vers Cassandra
   push_data_to_cassandra(csv_file_path, session)

   # Ferme la connexion à Cassandra
   close_connection(session) # Assurez-vous que le chemin soit correct

   # run_spark_job();


   load_to_mongo()
   import_csv_to_hdfs()
   import_csv_to_hive()
   create_external_table_immat()

   print("Orchestration terminée.")
