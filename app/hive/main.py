import subprocess

def import_client_to_hive():
    try:
        result = subprocess.run(["bash", "app/hive/import_client_to_hive.sh"], capture_output=True, text=True, check=True)
        print("Résultat de l'importation dans Hive :")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Erreur lors de l'importation dans Hive :")
        print(e.stderr)

def create_and_populate_external_table_immat ():
    try:
        result = subprocess.run(["bash", "app/hive/create_extern_table_immatriculations.sh"], capture_output=True, text=True, check=True)
        print("Table externe Immatriculations créée dans Hive avec succès :")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Erreur lors de la création de la table Immatriculations dans Hive :")
        print(e.stderr)

def create_and_populate_external_table_co2():
    try:
        result = subprocess.run(["bash", "app/hive/create_extern_table_co2.sh"], capture_output=True, text=True, check=True)
        print("Table externe Co2 créée dans Hive avec succès :")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Erreur lors de la création de la table Co2 dans Hive :")
        print(e.stderr)


def create_and_populate_external_table_catalogue():
    try:
        result = subprocess.run(["bash", "app/hive/create_extern_table_catalogue.sh"], capture_output=True, text=True, check=True)
        
        print("Table externe Catalogue créée dans Hive avec succès :")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Erreur lors de la création de la table Catalogue dans Hive :")
        print(e.stderr)

def run_all_setup_from_hive():
   print("Orchestration dans Hive en cours...")
   
   import_client_to_hive()
   create_and_populate_external_table_immat()
   create_and_populate_external_table_co2()
   create_and_populate_external_table_catalogue()

   print("Orchestration dans Hive terminée.")