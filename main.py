
from app.mongo.script import load_data_to_mongo
from app.cassandraa.cassandra_client import create_keyspace, create_table, push_data_to_cassandra, \
    get_cassandra_session, \
    close_connection, drop_all_keyspaces
from app.hdfs.main import run_all_setup_from_hdfs
from app.hive.main import run_all_setup_from_hive



def load_to_mongo():
   print("Chargement des données dans MongoDB...")
   load_data_to_mongo("data/Marketing.csv", "concessionnaire", "marketing")


if __name__ == "__main__":

   load_to_mongo()
   run_all_setup_from_hdfs()
   run_all_setup_from_hive()

   print("Orchestration du main terminée.")
