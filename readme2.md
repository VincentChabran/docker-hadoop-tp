# Accéder docker HDFS

```bash
docker exec -it namenode bash

hdfs dfs -ls /data

```

# Hive

```bash

docker exec -it hive-server bash

hive

```

```sql

use client;

select * from client_data limit 10;

```

# Saprk

```bash

docker exec -it  bash


```

# Pyhton env

```bash

python3.11 -m venv env # Créee l'env

source env/bin/activate

```

# Accéder a mongo express

USERNAME = admin
PASSWORD = pass
