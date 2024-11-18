#!/bin/bash

# Nom du conteneur Docker
CONTAINER_NAME="spark-notebook"

# Répertoire contenant les notebooks dans le conteneur
NOTEBOOKS_DIR="/home/jovyan/work"
OUTPUT_DIR="/home/jovyan/work/executed"

# Installer pyspark et autres dépendances si nécessaire
echo "Ensuring pyspark and papermill are installed in the container $CONTAINER_NAME..."
docker exec $CONTAINER_NAME pip install pyspark papermill matplotlib seaborn --quiet

# Créer un répertoire de sortie pour les notebooks exécutés
docker exec $CONTAINER_NAME mkdir -p $OUTPUT_DIR

# Exécuter chaque notebook avec papermill
echo "Starting execution of notebooks in $NOTEBOOKS_DIR from container $CONTAINER_NAME..."
docker exec $CONTAINER_NAME bash -c "
    for notebook in $NOTEBOOKS_DIR/*.ipynb; do
        echo 'Running notebook: ' \$notebook
        papermill \$notebook $OUTPUT_DIR/\$(basename \$notebook)
    done
"

echo "Execution of notebooks completed. Results saved in $OUTPUT_DIR inside the container."
