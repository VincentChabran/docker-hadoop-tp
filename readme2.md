# Accéder docker HDFS

```bash
docker exec -it namenode bash

hdfs dfs -ls /data
hdfs dfs -ls /user/hive/warehouse/concessionnaire.db/

```

<!--  -->
<!--  -->
<!--  -->
<!--  -->

# Hive

```bash

docker exec -it hive-server bash

hive

```

```sql

show databases;

show tables;

use concessionnaire;

select * from client_data limit 10;
select * from immatriculations_data limit 10;
select * from marketing_data limit 10;
select count(*) from marketing_data;
select count(*) from client_data;
select count(*) from client_processed;
select count(*) from marketing_processed;
select count(*) from client_immatriculation_merged;

```

<!--  -->
<!--  -->
<!--  -->
<!--  -->

# Saprk

```bash

/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /spark_mongo.py

/spark/bin/spark-submit /mapreduce/split_col_modele_marque_CO2.py
/spark/bin/spark-submit /mapreduce/show_db.py

docker exec -it spark-master /bin/bash

/spark/bin/spark-submit /mapreduce/clean_client_et_marketing.py
/spark/bin/spark-submit /mapreduce/treatment_immat.py
/spark/bin/spark-submit /mapreduce/merge_client_immat.py

/spark/bin/spark-submit --packages org.apache.spark:spark-hive_2.12:3.0.1 /mapreduce/show_db.py

 /spark/bin/spark-submit /mapreduce/data_process_for_CO2.py
 /spark/bin/spark-submit /mapreduce/data_process_for_catalogue_csv.py

```

<!--  -->
<!--  -->
<!--  -->
<!--  -->

# Pyhton env

```bash

python3.11 -m venv env # Créee l'env

source env/bin/activate

```

# Accéder a mongo express

USERNAME = admin
PASSWORD = pass
