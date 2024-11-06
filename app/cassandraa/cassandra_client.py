from cassandra.cluster import Cluster
import pandas as pd
import uuid

def get_cassandra_session():
    """
    Crée une session Cassandra.
    """
    cluster = Cluster(['localhost'])
    session = cluster.connect()
    return session

def create_keyspace(session):
    """
    Crée un keyspace pour stocker les données si elle n'existe pas.
    """
    create_keyspace_query = """
    CREATE KEYSPACE IF NOT EXISTS db_keyspace
    WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
    """
    session.execute(create_keyspace_query)

def create_table(session):
    """
    Crée une table pour stocker les données si elle n'existe pas.
    """
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

def drop_all_keyspaces(session):
    """
    Supprime tous les keyspaces.
    """
    keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    for row in keyspaces:
        keyspace_name = row.keyspace_name
        if keyspace_name not in ["system_auth", "system_schema", "system", "system_traces"]:
            print(f"Suppression du keyspace: {keyspace_name}")
            session.execute(f"DROP KEYSPACE {keyspace_name}")

def push_data_to_cassandra(csv_file_path, session):
    df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')

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
            print(f"Error inserting row {index}: {e}")

def close_connection(cluster):
    """
    Ferme la connexion à Cassandra.
    """
    cluster.shutdown()


