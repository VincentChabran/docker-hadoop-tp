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


def create_external_co2_data_processed():
    """
    Création de la table externe co2_data_processed dans Hive.
    """
    print("Création de la table externe co2_data_processed  dans Hive")
    try:

        # result = subprocess.run(["bash", "app/hive/create_extern_table_co2_data_processed.sh"], capture_output=True, text=True,
        #                         check=True)
        result = subprocess.run(
            ["docker", "exec", "-i", "hive-server", "/bin/bash", "-c",
             "hive -e \"CREATE DATABASE IF NOT EXISTS concessionnaire; "
             "USE concessionnaire; "
             "DROP TABLE IF EXISTS co2_data_processed; "
             "CREATE EXTERNAL TABLE IF NOT EXISTS co2_data_processed ("
             "marque STRING, modele STRING, modele_detail STRING, horse_power STRING, "
             "unit STRING, unified_horse_power STRING, bonus_malus STRING, cout_energie STRING, "
             "rejets_co2 STRING) "
             "STORED AS PARQUET "
             "LOCATION '/user/hive/warehouse/concessionnaire.db/co2_data_processed';\""
             ],
            capture_output=True, text=True, check=True)

        print("Résultat de la création de la table externe Hive :")
        print(result.stdout)  # Affiche la sortie standard
    except subprocess.CalledProcessError as e:
        print("Erreur lors de la création de la table Hive :")
        print(e.stderr)  # Affiche l'erreur si la commande échoue


def run_all_setup_from_hive():
    print("Orchestration dans Hive en cours...")

    import_client_to_hive()
    create_and_populate_external_table_immat()
    create_and_populate_external_table_co2()
    create_external_co2_data_processed()

    print("Orchestration dans Hive terminée.")
    
if __name__ == "__main__":
    run_all_setup_from_hive()

    print("Orchestration du main hive terminé")

