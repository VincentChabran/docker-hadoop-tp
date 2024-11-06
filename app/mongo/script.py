import pandas as pd
from pymongo import MongoClient

def load_data_to_mongo(csv_path, db_name, collection_name):
   print(f"Chargement des données CSV dans MongoDB - Base: {db_name}, Collection: {collection_name}")

   client = MongoClient("mongodb://localhost:27016/")
   db = client[db_name]
   collection = db[collection_name]

   data = pd.read_csv(csv_path, encoding="ISO-8859-1")
   data_dict = data.to_dict(orient="records")

   collection.insert_many(data_dict)
   
   print(f"Données insérées dans MongoDB - Base: {db_name}, Collection: {collection_name}")

if __name__ == "__main__":
   load_data_to_mongo("data/Marketing.csv", "concessionnaire", "marketing")
