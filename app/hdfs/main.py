import subprocess

def import_csvs_to_hdfs():
    try:
        result = subprocess.run(["bash", "app/hdfs/import_csvs_to_hdfs.sh"], capture_output=True, text=True, check=True)
        print("Résultat de l'importation dans HDFS :")
        print(result.stdout)
        
    except subprocess.CalledProcessError as e:
        print("Erreur lors de l'importation dans HDFS :")
        print(e.stderr)

def run_all_setup_from_hdfs():

   import_csvs_to_hdfs()

   print("Orchestration dans HDFS terminée.")