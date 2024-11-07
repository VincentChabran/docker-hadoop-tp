
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


if __name__ == "__main__":
   print("Début de l'orchestration des tâches")
   # Crée une session Cassandra
   session = get_cassandra_session()
   print("Session Cassandra créée.")

   create_keyspace(session)
   print("Keyspace créé.")

   create_table(session)
   print("Table créée.")
   csv_file_path = "data/Catalogue.csv"
   # Pousse les données du CSV vers Cassandra
   push_data_to_cassandra(csv_file_path, session)

   # Ferme la connexion à Cassandra
   close_connection(session) # Assurez-vous que le chemin soit correct



   load_to_mongo()
   import_csv_to_hdfs()
   import_csv_to_hive()
#    create_external_table_immat()

   print("Orchestration terminée.")
