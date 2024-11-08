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

show tables;

use client;

select * from client_data limit 10;

```

<!--  -->
<!--  -->
<!--  -->
<!--  -->

# Saprk

```bash

docker cp app/mongo/spark.py spark-master:/spark.py

docker exec -it spark-master /bin/bash

/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /spark_mongo.py


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
