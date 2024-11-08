
# main.py
import os
import subprocess
from app.mongo.script import load_data_to_mongo
from app.cassandraa.cassandra_client import create_keyspace, create_table, push_data_to_cassandra, \
    get_cassandra_session, \
    close_connection, drop_all_keyspaces, execute_cassandra_operations
from app.hdfs.main import run_all_setup_from_hdfs
from app.hive.main import run_all_setup_from_hive



def load_to_mongo():
   print("Chargement des données dans MongoDB...")
   load_data_to_mongo("data/Marketing.csv", "concessionnaire", "marketing")
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

   load_to_mongo()
   run_all_setup_from_hdfs()
   run_all_setup_from_hive()

   csv_file_path = "data/Catalogue.csv"
   execute_cassandra_operations(csv_file_path)



   run_spark_job();

   print("Orchestration du main terminée.")
