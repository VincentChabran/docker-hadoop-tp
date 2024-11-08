# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("MongoDBToHive") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/concessionnaire.marketing") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .enableHiveSupport() \
    .getOrCreate()
    # .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/concessionnaire.marketing?retryWrites=false") \

marketing_df = spark.read.format("mongo").load()
marketing_df = marketing_df.withColumnRenamed("2eme voiture", "deuxieme_voiture")

marketing_df.show()

# marketing_df = marketing_df.withColumn("deuxieme_voiture", col("deuxieme_voiture").cast("int"))



hive_table_path = "hdfs://namenode:9000/user/hive/warehouse/concessionnaire.db/marketing_table"


marketing_df.write.format("parquet").mode("overwrite").option("path", hive_table_path).saveAsTable("marketing_table")


spark.stop()
