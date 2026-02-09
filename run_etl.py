#!/usr/bin/env python3
"""
Script d'exécution sécurisé du pipeline ETL
Évite l'erreur "too many SQL variables"
"""

import logging
import sys
import os
from etl.pipeline import LoanETLPipeline

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_safe_etl():
    """Exécute le pipeline ETL avec des paramètres sécurisés"""
    
    print("""
    ========================================
    EXÉCUTION SÉCURISÉE DU PIPELINE ETL
    ========================================
    
    Ce script utilise des paramètres optimisés pour éviter
    l'erreur "too many SQL variables" de SQLite.
    """)
    
    # Demander à l'utilisateur
    use_sample = input("Utiliser un échantillon? (o/n) [o]: ").lower() != 'n'
    
    if use_sample:
        sample_size = input("Taille de l'échantillon (ex: 10000) [10000]: ")
        sample_size = int(sample_size) if sample_size else 10000
    else:
        sample_size = None
    
    # Configuration sécurisée
    config = {
        'raw_data_path': 'data/raw',
        'processed_data_path': 'data/processed',
        'database_path': 'data/loans_safe.db',
        'sample_size': sample_size,
        'chunk_size': 10000,
        'batch_size': 300,  # Réduit pour SQLite
        'outputs_path': 'data/outputs',
        'logs_path': 'logs',
        'reports_path': 'reports',
        'etl_settings': {
            'handle_missing': True,
            'convert_dates': True,
            'create_features': True,
            'remove_outliers': False,
            'outlier_threshold': 3.0,
            'max_columns': 25  # Limite le nombre de colonnes
        }
    }
    
    print(f"\nConfiguration:")
    print(f"- Sample size: {sample_size if sample_size else 'Toutes les données'}")
    print(f"- Batch size: {config['batch_size']}")
    print(f"- Max columns: {config['etl_settings']['max_columns']}")
    print(f"- Database: {config['database_path']}")
    
    confirm = input("\nConfirmer l'exécution? (o/n) [o]: ").lower()
    if confirm == 'n':
        print("Annulation.")
        return
    
    # Exécuter le pipeline
    try:
        # Créer une instance avec configuration personnalisée
        class SafePipeline(LoanETLPipeline):
            def _get_relevant_columns(self, df):
                """Version réduite pour éviter l'erreur SQLite"""
                essential_columns = [
                    'id', 'loan_amnt', 'int_rate', 'term', 'grade',
                    'sub_grade', 'issue_d', 'loan_status', 'annual_inc',
                    'dti', 'home_ownership', 'emp_length', 'verification_status',
                    'purpose', 'addr_state', 'delinq_2yrs', 'earliest_cr_line',
                    'inq_last_6mths', 'open_acc', 'revol_bal', 'revol_util',
                    'total_acc', 'total_pymnt', 'last_pymnt_d', 'last_pymnt_amnt'
                ]
                
                # Colonnes calculées
                calculated_columns = [
                    'is_default', 'is_fully_paid', 'income_category',
                    'risk_category'
                ]
                
                # Prendre seulement les colonnes qui existent
                existing_essential = [col for col in essential_columns if col in df.columns]
                existing_calculated = [col for col in calculated_columns if col in df.columns]
                
                # Limiter à 25 colonnes
                all_columns = existing_essential + existing_calculated
                if len(all_columns) > 25:
                    logger.warning(f"Limitation à 25 colonnes sur {len(all_columns)}")
                    all_columns = all_columns[:25]
                
                return all_columns
        
        pipeline = SafePipeline()
        
        # Modifier la configuration
        for key, value in config.items():
            if key in pipeline.config:
                if isinstance(value, dict) and isinstance(pipeline.config[key], dict):
                    pipeline.config[key].update(value)
                else:
                    pipeline.config[key] = value
        
        # Exécuter
        result = pipeline.run()
        
        if result['status'] == 'success':
            print("\n" + "="*60)
            print("✅ PIPELINE EXÉCUTÉ AVEC SUCCÈS!")
            print("="*60)
            print(f"Lignes traitées: {result.get('total_rows_processed', 0):,}")
            print(f"Base de données: {result.get('database_path', 'N/A')}")
            print(f"Durée: {result.get('duration', 'N/A')}")
            print("="*60)
            
            # Conseils pour la suite
            print("\n📊 Pour créer le dashboard:")
            print("1. python scripts/export_for_bi.py")
            print("2. Importer les fichiers CSV de data/outputs/ dans Looker Studio")
            
        else:
            print(f"\n❌ Échec du pipeline: {result.get('error', 'Erreur inconnue')}")
            
    except Exception as e:
        print(f"\n❌ Erreur critique: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_safe_etl()
