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

# Accéder a mongo express

USERNAME = admin
PASSWORD = pass
