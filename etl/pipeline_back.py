# etl/pipeline.py
"""
Pipeline ETL pour l'analyse de portefeuille de prêts
Orchestre l'extraction, transformation et chargement des données
"""

import logging
from datetime import datetime
import os
import sys
import yaml

# Ajouter le répertoire parent au path pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imports des modules ETL
from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DataLoader

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
    
    def __init__(self, config_path=None):
        """
        Initialise le pipeline avec configuration
        
        Args:
            config_path: Chemin vers le fichier de configuration YAML
        """
        # Charger la configuration
        self.config = self._load_config(config_path)
        
        # Initialiser les composants
        self.extractor = DataExtractor(self.config['raw_data_path'])
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.config['database_path'])
        
        # Création des répertoires nécessaires
        self._create_directories()
        
        logger.info("Pipeline ETL initialisé")
    
    def _load_config(self, config_path):
        """Charge la configuration depuis un fichier YAML"""
        default_config = {
            'raw_data_path': 'data/raw',
            'processed_data_path': 'data/processed',
            'database_path': 'data/loans.db',
            'sample_size': None,  # None pour toutes les données
            'chunk_size': 100000,
            'outputs_path': 'data/outputs',
            'logs_path': 'logs',
            'reports_path': 'reports',
            'etl_settings': {
                'handle_missing': True,
                'convert_dates': True,
                'create_features': True,
                'remove_outliers': False,
                'outlier_threshold': 3.0
            }
        }
        
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    user_config = yaml.safe_load(f)
                
                # Fusionner avec la configuration par défaut
                for key, value in user_config.items():
                    if isinstance(value, dict) and key in default_config:
                        default_config[key].update(value)
                    else:
                        default_config[key] = value
                
                logger.info(f"Configuration chargée depuis {config_path}")
                
            except Exception as e:
                logger.warning(f"Erreur de chargement de la configuration: {e}. Utilisation des valeurs par défaut.")
        
        return default_config
    
    def _create_directories(self):
        """Crée les répertoires nécessaires"""
        directories = [
            self.config['raw_data_path'],
            self.config['processed_data_path'],
            self.config['outputs_path'],
            self.config['logs_path'],
            self.config['reports_path']
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.debug(f"Répertoire créé/vérifié: {directory}")
    
    def extract(self):
        """
        Étape 1: Extraction des données
        
        Returns:
            DataFrame avec les données brutes
        """
        logger.info("=" * 60)
        logger.info("ÉTAPE 1: EXTRACTION")
        logger.info("=" * 60)
        
        try:
            # Option 1: Téléchargement automatique (nécessite token Kaggle)
            # files = self.extractor.download_from_kaggle()
            
            # Option 2: Chargement depuis fichiers existants
            raw_files = list(self.extractor.raw_data_dir.glob('*.csv'))
            
            input(raw_files)
            if not raw_files:
                error_msg = f"Aucun fichier CSV trouvé dans {self.config['raw_data_path']}"
                logger.error(error_msg)
                logger.info("""
                Instructions:
                1. Téléchargez manuellement le dataset Lending Club depuis:
                   https://www.kaggle.com/datasets/wordsforthewise/lending-club
                2. Placez le fichier CSV dans le dossier data/raw/
                3. Relancez le pipeline
                """)
                raise FileNotFoundError(error_msg)
            
            logger.info(f"Fichiers bruts détectés: {[f.name for f in raw_files]}")
            
            # Charger les données
            df_raw = self.extractor.load_raw_data(
                sample_size=self.config['sample_size'],
                chunk_size=self.config['chunk_size']
            )
            
            # Valider les données brutes
            validation = self.extractor.validate_raw_data(df_raw)
            
            if validation['status'] == 'FAIL':
                logger.warning("Problèmes détectés dans les données brutes:")
                for issue in validation['issues']:
                    logger.warning(f"  - {issue}")
            
            # Sauvegarde intermédiaire
            raw_sample_path = os.path.join(
                self.config['processed_data_path'], 
                'raw_data_sample.csv'
            )
            df_raw.head(1000).to_csv(raw_sample_path, index=False)
            logger.info(f"Échantillon brut sauvegardé: {raw_sample_path}")
            
            self.extract_stats = {
                'rows': len(df_raw),
                'columns': len(df_raw.columns),
                'file_count': len(raw_files),
                'validation_status': validation['status']
            }
            
            logger.info(f"Extraction terminée: {len(df_raw)} lignes, {len(df_raw.columns)} colonnes")
            
            return df_raw
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction: {e}")
            raise
    
    def transform(self, df_raw):
        """
        Étape 2: Transformation des données
        
        Args:
            df_raw: DataFrame avec les données brutes
            
        Returns:
            DataFrame transformé
        """
        logger.info("=" * 60)
        logger.info("ÉTAPE 2: TRANSFORMATION")
        logger.info("=" * 60)
        
        try:
            # Nettoyer et transformer les données
            df_clean = self.transformer.clean_loan_data(
                df_raw, 
                config=self.config['etl_settings']
            )
            
            # Valider les données transformées
            validation = self.transformer._validate_cleaned_data(df_clean)
            
            # Générer le rapport de qualité
            self._generate_quality_report(validation)
            
            # Sélectionner les colonnes pertinentes
            relevant_columns = self._get_relevant_columns(df_clean)
            df_transformed = df_clean[relevant_columns]
            
            # Sauvegarde intermédiaire
            processed_path = os.path.join(
                self.config['processed_data_path'], 
                'cleaned_data.csv'
            )
            df_transformed.to_csv(processed_path, index=False)
            logger.info(f"Données nettoyées sauvegardées: {processed_path}")
            
            self.transform_stats = {
                'rows': len(df_transformed),
                'columns': len(df_transformed.columns),
                'missing_values': validation['stats']['missing_values'],
                'duplicate_rows': validation['stats']['duplicate_rows'],
                'validation_status': validation['status']
            }
            
            logger.info(f"Transformation terminée: {len(df_transformed)} lignes, {len(df_transformed.columns)} colonnes")
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"Erreur lors de la transformation: {e}")
            raise
    
    def load(self, df_transformed):
        """
        Étape 3: Chargement des données
        
        Args:
            df_transformed: DataFrame transformé
            
        Returns:
            Tuple (engine, stats)
        """
        logger.info("=" * 60)
        logger.info("ÉTAPE 3: CHARGEMENT")
        logger.info("=" * 60)
        
        try:
            # Charger dans la base de données
            success = self.loader.load_to_sqlite(
                df_transformed, 
                table_name='loans'
            )
            
            if not success:
                raise Exception("Échec du chargement dans la base de données")
            
            # Créer les vues analytiques
            self.loader.create_analytical_views()
            
            # Exporter des tables pour le dashboard
            self._export_for_dashboard()
            
            # Générer des statistiques
            stats = self.loader.get_database_stats()
            
            self.load_stats = {
                'database_path': self.config['database_path'],
                'table_count': stats.get('table_count', 0),
                'view_count': stats.get('view_count', 0),
                'database_size_mb': stats.get('database_size_mb', 0),
                'loans_row_count': stats.get('loans_row_count', 0)
            }
            
            logger.info(f"Chargement terminé. Base de données: {self.config['database_path']}")
            
            return self.loader.engine, stats
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement: {e}")
            raise
    
    def run(self):
        """
        Exécute le pipeline ETL complet
        
        Returns:
            Dict avec les résultats de l'exécution
        """
        logger.info("=" * 60)
        logger.info("DÉMARRAGE DU PIPELINE ETL COMPLET")
        logger.info(f"Date et heure: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # Étape 1: Extraction
            df_raw = self.extract()
            
            # Étape 2: Transformation
            df_transformed = self.transform(df_raw)
            
            # Étape 3: Chargement
            engine, db_stats = self.load(df_transformed)
            
            # Calcul du temps d'exécution
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Générer le rapport final
            self._generate_final_report(start_time, end_time, db_stats)
            
            result = {
                'status': 'success',
                'duration': str(duration),
                'total_rows_processed': len(df_transformed),
                'database_path': self.config['database_path'],
                'extract_stats': self.extract_stats,
                'transform_stats': self.transform_stats,
                'load_stats': self.load_stats,
                'timestamp': end_time.isoformat()
            }
            
            logger.info("=" * 60)
            logger.info("PIPELINE ETL TERMINÉ AVEC SUCCÈS")
            logger.info(f"Durée totale: {duration}")
            logger.info(f"Lignes traitées: {len(df_transformed)}")
            logger.info(f"Base de données: {self.config['database_path']}")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.error("=" * 60)
            logger.error("ÉCHEC DU PIPELINE ETL")
            logger.error(f"Erreur: {e}")
            logger.error(f"Durée avant échec: {duration}")
            logger.error("=" * 60)
            
            return {
                'status': 'failed',
                'error': str(e),
                'duration': str(duration),
                'timestamp': end_time.isoformat()
            }
    
    def _get_relevant_columns(self, df):
        """
        Définit les colonnes pertinentes pour l'analyse
        """
        # Colonnes de base requises
        base_columns = [
            'id', 'loan_amnt', 'funded_amnt', 'term', 'int_rate',
            'installment', 'grade', 'sub_grade', 'emp_title', 'emp_length',
            'home_ownership', 'annual_inc', 'verification_status',
            'issue_d', 'loan_status', 'purpose', 'title', 'addr_state',
            'dti', 'delinq_2yrs', 'earliest_cr_line', 'inq_last_6mths',
            'open_acc', 'pub_rec', 'revol_bal', 'revol_util', 'total_acc',
            'initial_list_status', 'out_prncp', 'out_prncp_inv',
            'total_pymnt', 'total_pymnt_inv', 'total_rec_prncp',
            'total_rec_int', 'last_pymnt_d', 'last_pymnt_amnt',
            'next_pymnt_d', 'last_credit_pull_d'
        ]
        
        # Colonnes calculées
        calculated_columns = [
            'is_default', 'is_fully_paid', 'income_category',
            'loan_to_income_ratio', 'credit_age_years', 'credit_age_category',
            'risk_category', 'issue_year', 'issue_month', 'issue_quarter',
            'issue_season', 'int_rate_category'
        ]
        
        # Filtrer pour garder seulement les colonnes qui existent
        existing_base = [col for col in base_columns if col in df.columns]
        existing_calculated = [col for col in calculated_columns if col in df.columns]
        
        return existing_base + existing_calculated
    
    def _generate_quality_report(self, validation_report):
        """Génère un rapport de qualité des données"""
        report_path = os.path.join(self.config['reports_path'], 'data_quality_report.txt')
        
        with open(report_path, 'w') as f:
            f.write("RAPPORT DE QUALITÉ DES DONNÉES\n")
            f.write("=" * 50 + "\n")
            f.write(f"Date de génération: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Pipeline: {self.__class__.__name__}\n\n")
            
            f.write("STATISTIQUES DE BASE:\n")
            f.write("-" * 30 + "\n")
            for key, value in validation_report['stats'].items():
                f.write(f"{key.replace('_', ' ').title()}: {value}\n")
            
            f.write(f"\nSTATUT DE VALIDATION: {validation_report['status']}\n")
            
            if validation_report['issues']:
                f.write("\nPROBLÈMES IDENTIFIÉS:\n")
                f.write("-" * 30 + "\n")
                for issue in validation_report['issues']:
                    f.write(f"• {issue}\n")
            
            f.write("\nRECOMMANDATIONS:\n")
            f.write("-" * 30 + "\n")
            if validation_report['status'] == 'FAIL':
                f.write("1. Vérifier la source des données\n")
                f.write("2. Corriger les problèmes listés ci-dessus\n")
                f.write("3. Relancer le pipeline après corrections\n")
            else:
                f.write("✓ Les données sont de bonne qualité\n")
                f.write("✓ Le pipeline peut continuer normalement\n")
        
        logger.info(f"Rapport de qualité généré: {report_path}")
    
    def _export_for_dashboard(self):
        """Exporte les données pour le dashboard"""
        try:
            outputs_dir = Path(self.config['outputs_path'])
            outputs_dir.mkdir(exist_ok=True)
            
            # Tables à exporter pour le dashboard
            tables_to_export = [
                'dashboard_kpis',
                'loan_default_analysis',
                'monthly_performance',
                'borrower_segmentation',
                'loan_recovery_analysis'
            ]
            
            for table in tables_to_export:
                output_path = outputs_dir / f"{table}.csv"
                self.loader.export_table_to_csv(table, str(output_path))
            
            logger.info(f"Données exportées pour le dashboard dans {outputs_dir}")
            
        except Exception as e:
            logger.warning(f"Impossible d'exporter pour le dashboard: {e}")
    
    def _generate_final_report(self, start_time, end_time, db_stats):
        """Génère un rapport final d'exécution"""
        report_path = os.path.join(self.config['reports_path'], 'etl_execution_report.txt')
        
        with open(report_path, 'w') as f:
            f.write("RAPPORT D'EXÉCUTION ETL\n")
            f.write("=" * 60 + "\n\n")
            
            f.write("INFORMATIONS GÉNÉRALES:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Date de début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Date de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Durée totale: {end_time - start_time}\n")
            f.write(f"Statut: SUCCÈS\n\n")
            
            f.write("STATISTIQUES D'EXTRACTION:\n")
            f.write("-" * 30 + "\n")
            if hasattr(self, 'extract_stats'):
                for key, value in self.extract_stats.items():
                    f.write(f"{key.replace('_', ' ').title()}: {value}\n")
            
            f.write("\nSTATISTIQUES DE TRANSFORMATION:\n")
            f.write("-" * 30 + "\n")
            if hasattr(self, 'transform_stats'):
                for key, value in self.transform_stats.items():
                    f.write(f"{key.replace('_', ' ').title()}: {value}\n")
            
            f.write("\nSTATISTIQUES DE CHARGEMENT:\n")
            f.write("-" * 30 + "\n")
            if hasattr(self, 'load_stats'):
                for key, value in self.load_stats.items():
                    f.write(f"{key.replace('_', ' ').title()}: {value}\n")
            
            f.write("\nCONFIGURATION UTILISÉE:\n")
            f.write("-" * 30 + "\n")
            for key, value in self.config.items():
                if key != 'etl_settings':
                    f.write(f"{key}: {value}\n")
            
            f.write("\nPARAMÈTRES ETL:\n")
            f.write("-" * 30 + "\n")
            for key, value in self.config['etl_settings'].items():
                f.write(f"{key}: {value}\n")
            
            f.write("\nRECOMMANDATIONS POUR LA PROCHAINE EXÉCUTION:\n")
            f.write("-" * 30 + "\n")
            f.write("1. Vérifier la fraîcheur des données sources\n")
            f.write("2. Mettre à jour les paramètres si nécessaire\n")
            f.write("3. Planifier une exécution régulière\n")
            f.write("4. Surveiller les logs pour détecter les anomalies\n")
        
        logger.info(f"Rapport d'exécution généré: {report_path}")


def run_pipeline(config_path=None):
    """
    Fonction principale pour exécuter le pipeline
    
    Args:
        config_path: Chemin vers le fichier de configuration
        
    Returns:
        Résultats de l'exécution
    """
    try:
        # Création et exécution du pipeline
        pipeline = LoanETLPipeline(config_path)
        result = pipeline.run()
        
        return result
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline: {e}")
        return {
            'status': 'failed',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


if __name__ == "__main__":
    # Point d'entrée principal
    import argparse
    
    parser = argparse.ArgumentParser(description='Exécute le pipeline ETL pour les données de prêt')
    parser.add_argument('--config', type=str, help='Chemin vers le fichier de configuration YAML')
    parser.add_argument('--sample-size', type=int, help='Taille de l\'échantillon à charger')
    parser.add_argument('--db-path', type=str, help='Chemin de la base de données de sortie')
    
    args = parser.parse_args()
    
    # Configuration personnalisée
    custom_config = {}
    
    if args.config:
        custom_config['config_path'] = args.config
    
    if args.sample_size:
        if 'config' not in custom_config:
            custom_config['config'] = {}
        custom_config['config']['sample_size'] = args.sample_size
    
    if args.db_path:
        if 'config' not in custom_config:
            custom_config['config'] = {}
        custom_config['config']['database_path'] = args.db_path
    
    # Exécuter le pipeline
    result = run_pipeline(
        config_path=custom_config.get('config_path') if custom_config else None
    )
    
    # Afficher le résultat
    if result['status'] == 'success':
        print("\n" + "="*60)
        print("✅ PIPELINE EXÉCUTÉ AVEC SUCCÈS!")
        print("="*60)
        print(f"📊 Durée: {result['duration']}")
        print(f"📈 Lignes traitées: {result['total_rows_processed']:,}")
        print(f"💾 Base de données: {result['database_path']}")
        print(f"🕒 Timestamp: {result['timestamp']}")
        print("="*60 + "\n")
    else:
        print(f"\n❌ Échec du pipeline: {result.get('error', 'Erreur inconnue')}\n")
