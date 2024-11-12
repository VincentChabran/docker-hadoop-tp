import subprocess
from app.mongo.load_data_to_mongo import load_csv_to_mongo


def create_external_marketing_table():
   print("Création de la table externe marketing dans hive")
   try:
      result = subprocess.run(["bash", "app/mongo/create_external_marketing_table.sh"], capture_output=True, text=True, check=True)
      
      print("Résultat de la create_external_marketing_table.sh :")
      print(result.stdout)
      print("Table externe marketing créée dans Hive avec succès")
      
   except subprocess.CalledProcessError as e:
      print("Erreur lors de l'importation dans HDFS :")
      print(e.stderr)


def run_spark_for_monogodb_to_hive():
   print("Lancement du script Spark pour transférer les données de MongoDB vers Hive")
   try:
      exec_bash_cmd = ["docker", "exec", "-i", "spark-master", "/bin/bash", "-c"]
      
      spark_submit_cmd = (
         "/spark/bin/spark-submit "
         "--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 "
         "/spark_mongo.py"
      )

      result = subprocess.run(exec_bash_cmd + [spark_submit_cmd], check=True, text=True, capture_output=True)
      
      print("Résultat de spark-submit :")
      print(result.stdout)

   except subprocess.CalledProcessError as e:
      print("Erreur lors de l'exécution de la commande :", e)
      print("Sortie d'erreur :", e.stderr)


def orchestration_mongo_task():
   print("**************************************************************")
   print("Début de l'orchestration des tâches Mongo DB")

   load_csv_to_mongo("data/Marketing.csv", "concessionnaire", "marketing")

   create_external_marketing_table()

   run_spark_for_monogodb_to_hive()

   print("Fin de l'orchestration des tâches Mongo DB")
   print("**************************************************************")

