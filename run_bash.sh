#!/bin/bash
# run_project.sh

echo "=== Démarrage du projet d'analyse de prêts ==="
echo "Date: $(date)"

# 1. Configuration
echo "1. Configuration de l'environnement..."
#python -m venv venv 2>/dev/null || echo "Virtualenv existe déjà"
#source venv/bin/activate
#pip install -r requirements.txt --quiet

# 2. Vérification des données
echo "2. Vérification des données..."
if [ ! -f "data/raw/accepted_2007_to_2018Q4.csv" ]; then
    echo "⚠️  Données non trouvées. Veuillez télécharger manuellement:"
    echo "https://www.kaggle.com/datasets/wordsforthewise/lending-club"
    echo "Et placez le fichier dans data/raw/"
    exit 1
fi

#read -e "pause"



# 3. Pipeline ETL
echo "3. Exécution du pipeline ETL..."
python etl/pipeline.py --sample-size 500000

# 4. Requêtes SQL
echo "4. Exécution des requêtes SQL..."
python sql/run_queries.py

																																																																																																																																																																																																																																																																																																																																																																																																																												

# 5. Export BI
echo "5. Export pour le dashboard..."
python scripts/export_for_bi.py

# 6. Rapport
echo "6. Génération du rapport..."
python generate_docs.py

echo "=== Projet terminé avec succès! ==="
echo "📊 Dashboard: Importer les fichiers CSV de data/outputs/ dans Looker Studio"
echo "📁 Fichiers générés dans data/outputs/:"
ls -la data/outputs/
