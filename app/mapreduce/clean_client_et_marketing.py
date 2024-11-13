from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataCleaningWithHive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE concessionnaire")

df_hive = spark.sql("SELECT * FROM client_data;")  
df_hive.show()


