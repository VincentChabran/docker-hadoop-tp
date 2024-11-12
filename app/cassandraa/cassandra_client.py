from cassandra.cluster import Cluster
import pandas as pd
import uuid

def get_cassandra_session():
    """
    Crée une session Cassandra.
    """
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session, cluster
    except Exception as e:
        print(f"Erreur lors de la connexion à Cassandra : {e}")
        return None, None

def create_keyspace(session):
    """
    Crée un keyspace pour stocker les données si elle n'existe pas.
    """
    try:
        create_keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS db_keyspace
        WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
        """
        session.execute(create_keyspace_query)
        print("Keyspace 'db_keyspace' créé avec succès.")
    except Exception as e:
        print(f"Erreur lors de la création du keyspace : {e}")

def create_table(session):
    """
    Crée une table pour stocker les données si elle n'existe pas.
    """
    try:
        # Assurez-vous d'utiliser le keyspace
        session.set_keyspace('db_keyspace')

        create_table_query = """
        CREATE TABLE IF NOT EXISTS catalogue (
            id UUID PRIMARY KEY,
            marque TEXT,
            nom TEXT,
            puissance INT,
            longueur TEXT,
            nbPlaces INT,
            nbPortes INT,
            couleur TEXT,
            occasion BOOLEAN,
            prix FLOAT
        )
        """
        session.execute(create_table_query)
        print("Table 'catalogue' créée avec succès.")
    except Exception as e:
        print(f"Erreur lors de la création de la table : {e}")

def push_data_to_cassandra(csv_file_path, session):
    """
    Charge les données du fichier CSV et les insère dans la table Cassandra.
    """
    try:
        df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')
        print(f"{len(df)} lignes trouvées dans le fichier CSV.")

        for index, row in df.iterrows():
            # Générer un UUID pour chaque ligne
            row_id = uuid.uuid4()
            try:
                session.execute(
                    """
                    INSERT INTO catalogue (id, marque, nom, puissance, longueur, nbPlaces, nbPortes, couleur, occasion, prix)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (row_id, row['marque'], row['nom'], row['puissance'], row['longueur'],
                     row['nbPlaces'], row['nbPortes'], row['couleur'], row['occasion'], row['prix'])
                )
            except Exception as e:
                print(f"Erreur lors de l'insertion de la ligne {index} : {e}")

        print("Insertion des données terminée.")
    except FileNotFoundError:
        print(f"Fichier CSV non trouvé : {csv_file_path}")
    except pd.errors.ParserError as e:
        print(f"Erreur lors du parsing du fichier CSV : {e}")

def close_connection(cluster):
    """
    Ferme la connexion à Cassandra.
    """
    if cluster:
        cluster.shutdown()
        print("Connexion à Cassandra fermée.")

def execute_cassandra_operations(csv_file_path):
    """
    Exécute toutes les opérations nécessaires sur Cassandra : création de keyspace,
    création de table, et insertion des données depuis un fichier CSV.

    Args:
        csv_file_path (str): Le chemin du fichier CSV à charger dans Cassandra.
    """
    session, cluster = get_cassandra_session()
    if not session or not cluster:
        print("Impossible d'établir une connexion à Cassandra.")
        return

    try:
        create_keyspace(session)
        create_table(session)
        push_data_to_cassandra(csv_file_path, session)
    finally:
        close_connection(cluster)

