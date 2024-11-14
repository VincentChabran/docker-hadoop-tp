# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions as F

reload(sys)
sys.setdefaultencoding('utf-8')

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Traitement données Immatriculations") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def load_data(spark, query):
    return spark.sql(query)

def get_clear_data(spark):
    immatriculations_df = load_data(spark, "SELECT * FROM immatriculations_data WHERE CAST(immatriculation AS STRING) RLIKE '^[0-9]'")

    immatriculations_df = immatriculations_df.withColumn(
        "longueur",
        regexp_replace("longueur", "tr�s", "très")
    )
    immatriculations_df = immatriculations_df.withColumnRenamed("nom", "modele")

    return immatriculations_df

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

def split_modele_column(df):
    # Extraire le premier mot du modèle, s'il commence par une lettre
    modele_extract = regexp_extract("modele", r"^(\w+)", 1)

    # Extraire le détail du modèle s'il y en a (partie après le premier mot)
    detail_extract = regexp_extract("modele", r"^\w+\s+(.*)", 1)

    # Créer une nouvelle colonne "modele" qui contient seulement le premier mot
    df = df.withColumn("modele_cleaned", when(col("modele").rlike("^[0-9]"), col("modele")).otherwise(modele_extract))

    # Créer une nouvelle colonne "detail" qui contient les détails après le premier mot
    df = df.withColumn("detail", when(col("modele").rlike("^[0-9]"), "").otherwise(detail_extract))

    # Enlever les espaces blancs au début et à la fin des colonnes
    df = df.withColumn("modele_cleaned", trim(col("modele_cleaned")))
    df = df.withColumn("detail", trim(col("detail")))

    return df

def main():
    spark = create_spark_session()

    spark.sql("USE {}".format("concessionnaire"))

    immatriculations_df = get_clear_data(spark)

    check_nulls(immatriculations_df)
    immatriculations_df.limit(50).show()

    columns_to_check = ["prix", "marque", "modele", "puissance", "longueur", "nbplaces", "nbportes", "couleur", "occasion"]
    show_distinct_values(immatriculations_df, columns_to_check)

    describe_columns(immatriculations_df, ["puissance", "prix", "longueur", "nbPlaces"])
    spark.stop()

if __name__ == "__main__":
    main()
