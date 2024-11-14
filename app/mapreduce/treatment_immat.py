# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import regexp_extract, when, trim, col

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

# Récupère les data de la table immatriculations_data et les mets propre dans le dataframe
# (nom => modele, supprime accent, vire la premoière ligne)
def get_clear_data(spark):
    immatriculations_df = load_data(spark, "SELECT * FROM immatriculations_data WHERE CAST(immatriculation AS STRING) RLIKE '^[0-9]'")

    immatriculations_df = immatriculations_df.withColumn(
        "longueur",
        regexp_replace("longueur", "�", "è")
    )
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

# Affiche les valeurs distinctes pour chaque colonne spécifiée dans le DataFrame, une par une.
def show_distinct_values(df, columns):
    for column in columns:
        print("Valeurs distinctes de {}:".format(column))
        distinct_values = df.select(column).distinct().collect()
        for value in distinct_values:
            print(value[column])
        print("\n" + "="*50 + "\n")

def describe_columns(df, columns):
    df.describe(columns).show()

def extract_model(df):
    modele_name_extract = regexp_extract(col("modele"), r'^([A-Za-z0-9\.-]+)', 1)

    df = df.withColumn("modele_temp", trim(modele_name_extract))
    
    df = df.drop("modele")
    
    cols = df.columns
    marque_index = cols.index("marque")    
    reordered_cols = cols[:marque_index + 1] + ["modele_temp"] + cols[marque_index + 1:]

    return df


def main():
    spark = create_spark_session()
    spark.sql("USE concessionnaire")
    
    immatriculations_df = get_clear_data(spark)

    # check_nulls(immatriculations_df)
    
    immatriculations_df = extract_model(immatriculations_df)

    # columns_to_check = ["prix", "marque", "modele", "puissance", "longueur", "nbplaces", "nbportes", "couleur", "occasion"]
    # show_distinct_values(immatriculations_df, columns_to_check)
    # describe_columns(immatriculations_df, ["puissance", "prix", "longueur", "nbPlaces"])
    
    immatriculations_df.show(100, truncate=False)
    
    immatriculations_df.write.mode("overwrite").saveAsTable("immatriculations_processed")
    
    spark.stop()

if __name__ == "__main__":
    main()
