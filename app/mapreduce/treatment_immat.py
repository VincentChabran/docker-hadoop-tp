# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession

reload(sys)
sys.setdefaultencoding('utf-8')

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Traitement données Immatriculations") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setLogLevel("OFF")
    return spark

def load_data(spark, query):
    return spark.sql(query)

def check_nulls(df):
    null_rows_df = df.filter(
        " OR ".join(["{} IS NULL".format(col) for col in df.columns])
    )
    if null_rows_df.count() > 0:
        print("Lignes avec des champs NULL détectées :")
        null_rows_df.show()
    else:
        print("Aucune ligne contenant des champs NULL n'a été trouvée.")

def show_distinct_values(df, columns):
    for column in columns:
        print("Valeurs distinctes de {}:".format(column))
        distinct_values = df.select(column).distinct().collect()
        for value in distinct_values:
            print(value[column])
        print("\n" + "="*50 + "\n")

def describe_columns(df, columns):
    df.describe(columns).show()

def main():
    spark = create_spark_session()

    spark.sql("USE {}".format("concessionnaire"))

    immatriculations_df = load_data(spark, "SELECT * FROM immatriculations_data WHERE CAST(immatriculation AS STRING) RLIKE '^[0-9]'")

    check_nulls(immatriculations_df)

    columns_to_check = ["prix", "marque", "nom", "puissance", "longueur", "nbplaces", "nbportes", "couleur", "occasion"]
    show_distinct_values(immatriculations_df, columns_to_check)

    describe_columns(immatriculations_df, ["puissance", "prix", "longueur", "nbPlaces"])

    spark.stop()

if __name__ == "__main__":
    main()
