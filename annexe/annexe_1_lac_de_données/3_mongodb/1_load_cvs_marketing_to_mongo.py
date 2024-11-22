import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

def load_csv_to_mongo(csv_path, db_name, collection_name):
    try:
        print(f"Chargement des données CSV dans MongoDB - Base: {db_name}, Collection: {collection_name}")
        
        # Connexion à MongoDB
        try:
            client = MongoClient("mongodb://localhost:27016/")
            client.admin.command('ping')  
            db = client[db_name]
            collection = db[collection_name]
        except ConnectionFailure as e:
            print("Erreur de connexion à MongoDB:", e)
            return
        except OperationFailure as e:
            print("Erreur lors de la vérification de la connexion MongoDB:", e)
            return

        # Chargement des données CSV
        try:
            data = pd.read_csv(csv_path, encoding="ISO-8859-1")
        except FileNotFoundError:
            print(f"Fichier non trouvé : {csv_path}")
            return
        except pd.errors.ParserError as e:
            print("Erreur de parsing du fichier CSV :", e)
            return

        # Conversion en dictionnaire et insertion dans MongoDB
        data_dict = data.to_dict(orient="records")
        
        try:
            collection.insert_many(data_dict)
            print(f"Données insérées dans MongoDB - Base: {db_name}, Collection: {collection_name}")
        except OperationFailure as e:
            print("Erreur lors de l'insertion des données dans MongoDB :", e)

    except Exception as e:
        print("Une erreur inattendue est survenue :", e)


if __name__ == "__main__":
   load_csv_to_mongo("data/Marketing.csv", "concessionnaire", "marketing")
