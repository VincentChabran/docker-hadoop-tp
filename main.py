# main.py
import subprocess
import sys
from app.mongo.script import load_data_to_mongo

def load_to_mongo():
   print("Chargement des données dans MongoDB...")
   load_data_to_mongo("data/Marketing.csv", "concessionnaire", "marketing")


def import_csv_to_hdfs():
    try:
        # Exécute le script shell pour importer le fichier CSV dans HDFS
        result = subprocess.run(["bash", "app/hdfs/script.sh"], capture_output=True, text=True, check=True)
        
        # Affiche la sortie du script shell
        print("Résultat de l'importation dans HDFS :")
        print(result.stdout)
        
    except subprocess.CalledProcessError as e:
        # Affiche l'erreur en cas d'échec
        print("Erreur lors de l'importation dans HDFS :")
        print(e.stderr)


if __name__ == "__main__":
   print("Début de l'orchestration des tâches")

   load_to_mongo()
   import_csv_to_hdfs()

   print("Orchestration terminée.")
