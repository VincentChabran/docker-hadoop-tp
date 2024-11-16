#!/bin/bash

# Nom du conteneur Docker où Spark est exécuté
CONTAINER_NAME="spark-master"

# Liste des scripts Spark à exécuter
SPARK_SCRIPTS=(
    "/mapreduce/clean_client_et_marketing.py"
    "/mapreduce/treatment_immat.py"
    "/mapreduce/merge_client_immat.py"
)

# Exécuter chaque script Spark dans l'ordre
for script in "${SPARK_SCRIPTS[@]}"; do
    echo "Lancement du script Spark : $script"
    
    docker exec -i $CONTAINER_NAME /bin/bash -c "/spark/bin/spark-submit $script"
    
    # Vérification du résultat de chaque exécution
    if [ $? -eq 0 ]; then
        echo "Le script $script a été exécuté avec succès."
    else
        echo "Erreur lors de l'exécution du script $script."
        exit 1
    fi
done

echo "Tous les scripts Spark ont été exécutés avec succès."
