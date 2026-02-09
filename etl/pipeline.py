"""
Pipeline ETL corrigé - Version avec paramètres corrects
"""

import logging
from datetime import datetime
import os
import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imports des modules ETL
from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load_sqlite import SQLiteDataLoader

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class LoanETLPipeline:
    """
    Classe principale pour le pipeline ETL
    """
    
    def __init__(self, config=None):
        """
        Initialise le pipeline avec configuration
        """
        # Configuration par défaut
        default_config = {
            'raw_data_path': 'data/raw',
            'processed_data_path': 'data/processed',
            'database_path': 'data/loans.db',
            'sample_size': 10000,      # Échantillon pour les tests
            'chunksize': 10000,        # Taille des chunks pour la lecture
            'batch_size': 500,         # Taille des lots pour SQLite
            'max_columns': 25          # Limite du nombre de colonnes
        }
        
        # Fusionner avec la configuration fournie
        if config:
            default_config.update(config)
        
        self.config = default_config
        
        # Convertir les chemins en Path
        self.raw_data_path = Path(self.config['raw_data_path'])
        self.processed_data_path = Path(self.config['processed_data_path'])
        self.database_path = Path(self.config['database_path'])
        
        # Créer les répertoires
        self._create_directories()
        
        # Initialiser les composants
        self.extractor = DataExtractor(str(self.raw_data_path))
        self.transformer = DataTransformer()
        self.loader = SQLiteDataLoader(str(self.database_path))
        
        logger.info("Pipeline ETL initialisé")
        logger.info(f"Configuration: sample_size={self.config['sample_size']}, chunksize={self.config['chunksize']}")
    
    def _create_directories(self):
        """Crée tous les répertoires nécessaires"""
        directories = [
            self.raw_data_path,
            self.processed_data_path,
            self.database_path.parent,
            Path('data/outputs'),
            Path('logs'),
            Path('reports')
        ]
        
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Répertoire créé/vérifié: {directory}")
            except Exception as e:
                logger.error(f"Erreur création répertoire {directory}: {e}")
    
    def extract(self):
        """
        Étape 1: Extraction des données
        """
        logger.info("=" * 60)
        logger.info("ÉTAPE 1: EXTRACTION")
        logger.info("=" * 60)
        
        try:
            # Vérifier si le dossier raw existe
            if not self.raw_data_path.exists():
                logger.error(f"Le dossier {self.raw_data_path} n'existe pas!")
                self._create_directories()
            
            # Chercher des fichiers CSV
            csv_files = list(self.raw_data_path.glob('*.csv'))
            
            if not csv_files:
                logger.error(f"Aucun fichier CSV trouvé dans {self.raw_data_path}")
                logger.info("\nOptions:")
                logger.info("1. Placez un fichier CSV dans data/raw/")
                logger.info("2. Exécutez: python scripts/download_sample.py")
                logger.info("3. Utilisez --test-mode pour des données synthétiques")
                
                # Demander à l'utilisateur
                if self.config.get('test_mode', False):
                    logger.info("Mode test activé - Création de données synthétiques")
                    return self._create_test_data()
                else:
                    raise FileNotFoundError(f"Aucun fichier CSV dans {self.raw_data_path}")
            
            # Utiliser le premier fichier trouvé
            data_file = csv_files[0]
            logger.info(f"Utilisation du fichier: {data_file.name}")
            
            # Charger les données avec les paramètres configurés
            df_raw = self.extractor.load_raw_data(
                file_path=str(data_file),
                sample_size=self.config['sample_size'],
                chunksize=self.config['chunksize']
            )
            
            # Valider les données
            validation = self.extractor.validate_raw_data(df_raw)
            
            if validation['status'] == 'FAIL':
                logger.error("Données invalides, impossible de continuer")
                raise ValueError("Validation des données échouée")
            
            logger.info(f"✓ Extraction réussie: {len(df_raw)} lignes")
            return df_raw
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction: {e}")
            raise
    
    def _create_test_data(self):
        """Crée des données de test synthétiques"""
        logger.info("Création de données de test...")
        
        import pandas as pd
        import numpy as np
        
        np.random.seed(42)
        n_samples = self.config.get('sample_size', 10000)
        
        # Données réalistes
        data = {
            'id': range(1, n_samples + 1),
            'loan_amnt': np.random.randint(1000, 40000, n_samples),
            'int_rate': np.round(np.random.uniform(5, 30, n_samples), 2),
            'term': np.random.choice(['36 months', '60 months'], n_samples, p=[0.7, 0.3]),
            'grade': np.random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G'], n_samples, p=[0.1, 0.2, 0.25, 0.2, 0.15, 0.05, 0.05]),
            'issue_d': pd.date_range('2015-01-01', periods=n_samples, freq='D').strftime('%b-%Y'),
            'loan_status': np.random.choice(['Fully Paid', 'Current', 'Charged Off', 'Late'], n_samples, p=[0.6, 0.3, 0.08, 0.02]),
            'annual_inc': np.random.randint(20000, 150000, n_samples),
            'dti': np.round(np.random.uniform(0, 40, n_samples), 2),
            'home_ownership': np.random.choice(['RENT', 'MORTGAGE', 'OWN', 'OTHER'], n_samples),
            'emp_length': np.random.choice(['< 1 year', '1 year', '2-4 years', '5-9 years', '10+ years'], n_samples),
            'purpose': np.random.choice(['debt_consolidation', 'credit_card', 'home_improvement', 'car', 'medical'], n_samples),
            'addr_state': np.random.choice(['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH'], n_samples),
            'delinq_2yrs': np.random.randint(0, 5, n_samples),
            'revol_util': np.round(np.random.uniform(0, 100, n_samples), 2),
            'total_pymnt': np.round(np.random.uniform(0, 50000, n_samples), 2),
            'is_default': np.random.choice([0, 1], n_samples, p=[0.85, 0.15]),
            'is_fully_paid': np.random.choice([0, 1], n_samples, p=[0.3, 0.7])
        }
        
        df = pd.DataFrame(data)
        logger.info(f"✓ Données de test créées: {len(df)} lignes")
        
        return df
    
    def transform(self, df_raw):
        """
        Étape 2: Transformation des données
        """
        logger.info("=" * 60)
        logger.info("ÉTAPE 2: TRANSFORMATION")
        logger.info("=" * 60)
        
        try:
            # Nettoyer et transformer
            df_clean = self.transformer.clean_loan_data(df_raw)
            
            # Sélectionner les colonnes pertinentes
            df_final = self._select_columns(df_clean)
            
            # Sauvegarder les données nettoyées
            output_file = self.processed_data_path / 'cleaned_data.csv'
            df_final.to_csv(output_file, index=False)
            logger.info(f"✓ Données sauvegardées: {output_file}")
            
            logger.info(f"✓ Transformation réussie: {len(df_final)} lignes, {len(df_final.columns)} colonnes")
            return df_final
            
        except Exception as e:
            logger.error(f"Erreur lors de la transformation: {e}")
            raise
    
    def _select_columns(self, df):
        """Sélectionne les colonnes pertinentes"""
        # Colonnes essentielles
        essential_columns = [
            'loan_amnt', 'int_rate', 'term', 'grade', 'issue_d',
            'loan_status', 'annual_inc', 'dti', 'home_ownership',
            'emp_length', 'purpose', 'addr_state', 'delinq_2yrs',
            'revol_util', 'total_pymnt', 'is_default', 'is_fully_paid'
        ]
        
        # Colonnes additionnelles si disponibles
        additional_columns = [
            'funded_amnt', 'installment', 'sub_grade', 'verification_status',
            'earliest_cr_line', 'inq_last_6mths', 'open_acc', 'pub_rec',
            'revol_bal', 'total_acc', 'total_rec_prncp', 'total_rec_int',
            'last_pymnt_d', 'last_pymnt_amnt'
        ]
        
        # Trouver les colonnes disponibles
        available_essential = [col for col in essential_columns if col in df.columns]
        available_additional = [col for col in additional_columns if col in df.columns]
        
        # Limiter le nombre total de colonnes
        max_cols = self.config.get('max_columns', 25)
        selected_cols = available_essential
        
        # Ajouter des colonnes additionnelles jusqu'à la limite
        for col in available_additional:
            if len(selected_cols) < max_cols:
                selected_cols.append(col)
            else:
                break
        
        logger.info(f"Sélection de {len(selected_cols)} colonnes (limite: {max_cols})")
        return df[selected_cols].copy()

    # Dans etl/pipeline.py, modifier la partie load :

    def load(self, df_transformed):
        """
        Étape 3: Chargement des données
        """
        logger.info("=" * 60)
        logger.info("ÉTAPE 3: CHARGEMENT")
        logger.info("=" * 60)
    
        try:
           # Utiliser le chargeur ultra simple
           from etl.load_ultra_simple import run_simple_load
        
           logger.info(f"Chargement dans {self.config['database_path']}...")
        
           success = run_simple_load(df_transformed, self.config['database_path'])
        
           if not success:
              raise Exception("Échec du chargement dans la base de données")
        
           logger.info(f"✓ Chargement terminé: {self.config['database_path']}")
        
           # Vérifier que la base a été créée
           import sqlite3
           conn = sqlite3.connect(self.config['database_path'])
           cursor = conn.cursor()
           cursor.execute("SELECT COUNT(*) FROM loans")
           count = cursor.fetchone()[0]
           conn.close()
        
           logger.info(f"✓ Lignes chargées: {count:,}")
        
           return True
        
        except Exception as e:
           logger.error(f"Erreur lors du chargement: {e}")
           raise
 
    
    def run(self):
        """
        Exécute le pipeline complet
        """
        logger.info("=" * 60)
        logger.info("DÉMARRAGE DU PIPELINE ETL")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # 1. Extraction
            df_raw = self.extract()
            
            # 2. Transformation
            df_transformed = self.transform(df_raw)
            
            # 3. Chargement
            self.load(df_transformed)
            
            # Calcul du temps
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=" * 60)
            logger.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
            logger.info("=" * 60)
            logger.info(f"Durée totale: {duration}")
            logger.info(f"Lignes traitées: {len(df_transformed):,}")
            logger.info(f"Base de données: {self.database_path}")
            logger.info("=" * 60)
            
            return {
                'status': 'success',
                'duration': str(duration),
                'rows_processed': len(df_transformed),
                'database_path': str(self.database_path)
            }
            
        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.error("=" * 60)
            logger.error("❌ ÉCHEC DU PIPELINE")
            logger.error("=" * 60)
            logger.error(f"Erreur: {e}")
            logger.error(f"Durée: {duration}")
            logger.error("=" * 60)
            
            return {
                'status': 'failed',
                'error': str(e),
                'duration': str(duration)
            }


def run_pipeline(config=None):
    """
    Fonction principale pour exécuter le pipeline
    """
    try:
        pipeline = LoanETLPipeline(config)
        result = pipeline.run()
        return result
    except Exception as e:
        logging.error(f"Erreur lors de l'exécution: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Exécute le pipeline ETL')
    parser.add_argument('--sample-size', type=int, default=10000, help='Taille de l\'échantillon')
    parser.add_argument('--chunksize', type=int, default=10000, help='Taille des chunks pour la lecture')
    parser.add_argument('--batch-size', type=int, default=500, help='Taille des lots pour SQLite')
    parser.add_argument('--db-path', type=str, default='data/loans.db', help='Chemin de la base de données')
    parser.add_argument('--test-mode', action='store_true', help='Utiliser des données de test')
    
    args = parser.parse_args()
    
    # Configuration
    config = {
        'sample_size': args.sample_size,
        'chunksize': args.chunksize,
        'batch_size': args.batch_size,
        'database_path': args.db_path,
        'test_mode': args.test_mode
    }
    
    print(f"\nConfiguration:")
    print(f"- Sample size: {config['sample_size']}")
    print(f"- Chunksize: {config['chunksize']}")
    print(f"- Batch size: {config['batch_size']}")
    print(f"- Database: {config['database_path']}")
    print(f"- Test mode: {config['test_mode']}")
    print()
    
    result = run_pipeline(config)
    
    if result['status'] == 'success':
        print(f"\n✅ Succès! Base créée: {result['database_path']}")
    else:
        print(f"\n❌ Échec: {result.get('error', 'Erreur inconnue')}")
