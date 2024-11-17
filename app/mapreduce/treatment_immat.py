# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, trim, col

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

# Récupère les data de la table immatriculations_data et les mets propre dans le dataframe
# (nom => modele, supprime accent, vire la premoière ligne)
def get_clear_data(spark):
    immatriculations_df = spark.sql("SELECT * FROM immatriculations_data WHERE CAST(immatriculation AS STRING) RLIKE '^[0-9]'")

    immatriculations_df = immatriculations_df.withColumn( "longueur", regexp_replace("longueur", "�", "è"))
    immatriculations_df = immatriculations_df.withColumnRenamed("nom", "modele")

    return immatriculations_df

# Vérifie si il y a des champs nul 
def check_nulls(df):
    null_rows_df = df.filter(
        " OR ".join(["{} IS NULL".format(col) for col in df.columns])
    )
    if null_rows_df.count() > 0:
        print("Lignes avec des champs NULL détectées :")
        null_rows_df.show()
    else:
        print("Aucune ligne contenant des champs NULL n'a été trouvée.")

def extract_model(df):
    modele_name_extract = regexp_extract(col("modele"), r'^([A-Za-z0-9\.-]+)', 1)

    df = df.withColumn("modele_simple", trim(modele_name_extract))
    df = df.drop("modele")

    return df

def drop_doublon(df):
    nb_total_lignes = df.count()
    doublons_immatriculation = df.groupBy("immatriculation").count().filter(col("count") > 1)

    nb_doublons = doublons_immatriculation.count()
    print(nb_doublons)

    pourcentage_doublons = (nb_doublons / nb_total_lignes) * 100
    print(pourcentage_doublons)

    df = df.dropDuplicates(["immatriculation"])
    return df


def main():
    spark = create_spark_session()
    spark.sql("USE concessionnaire")
    
    immatriculations_df = get_clear_data(spark)

    immatriculations_df = extract_model(immatriculations_df)    
    immatriculations_df = drop_doublon(immatriculations_df)    
    immatriculations_df.show(20, truncate=False)
    immatriculations_df.write.mode("overwrite").saveAsTable("immatriculations_processed")
    
    spark.stop()

if __name__ == "__main__":
    main()
