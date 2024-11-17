# from app.cassandraa.main_cassandra import orchestration_cassandra_task
from app.hdfs.main import run_all_setup_from_hdfs
from app.hive.main import run_all_setup_from_hive
from app.mongo.main_mongo import orchestration_mongo_task


if __name__ == "__main__":

   run_all_setup_from_hdfs()
   run_all_setup_from_hive()
   orchestration_mongo_task()
   # orchestration_cassandra_task()

   print("Orchestration du main terminée.")
