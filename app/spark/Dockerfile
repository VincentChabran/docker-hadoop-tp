FROM bde2020/spark-master:3.1.1-hadoop3.2

# Utiliser l'utilisateur root pour installer les dépendances
USER root

# Désinstaller PySpark s'il est déjà installé
RUN pip3 uninstall -y pyspark

# Installer les dépendances nécessaires
RUN apk update && apk add --no-cache \
    wget \
    openjdk8 \
    python3 \
    py3-pip \
    bash \
    coreutils

# Définir les variables d'environnement nécessaires
ENV SPARK_HOME=/spark
ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# Installer PySpark version 3.1.1 via pip
RUN pip3 install pyspark==3.1.1

# Vérifier la version de PySpark installée
RUN pip3 show pyspark

# Supprimer les anciens jars si présents
RUN rm -f $SPARK_HOME/jars/spark-cassandra-connector_2.12-3.1.0.jar \
          $SPARK_HOME/jars/cassandra-driver-core-3.10.2.jar

# Télécharger le jar d'assemblage du Spark Cassandra Connector
RUN wget -O $SPARK_HOME/jars/spark-cassandra-connector-assembly_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.1.0/spark-cassandra-connector-assembly_2.12-3.1.0.jar

# S'assurer que /spark/logs existe et est accessible
RUN mkdir -p /spark/logs && chmod -R 777 /spark/logs

# Copier hive-site.xml dans le répertoire de configuration de Spark
COPY hive-site.xml $SPARK_HOME/conf/

# Rester en tant qu'utilisateur root
USER root
