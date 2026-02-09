"""
Module d'extraction des données - Version corrigée
"""

import pandas as pd
import numpy as np
import os
import glob
import logging
from pathlib import Path
from typing import Optional
from tqdm import tqdm

# Configuration du logging
logger = logging.getLogger(__name__)

class DataExtractor:
    """Classe pour extraire les données depuis différentes sources"""
    
    def __init__(self, raw_data_dir='data/raw'):
        self.raw_data_dir = Path(raw_data_dir)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
    
    def load_raw_data(self, file_path: Optional[str] = None, 
                      sample_size: Optional[int] = None, 
                      chunksize: Optional[int] = None) -> pd.DataFrame:
        """
        Charge les données brutes depuis un fichier CSV
        
        Args:
            file_path: Chemin du fichier CSV (optionnel)
            sample_size: Nombre de lignes à échantillonner (optionnel)
            chunksize: Taille des chunks pour lecture par morceaux (optionnel)
            
        Returns:
            DataFrame pandas avec les données brutes
        """
        try:
            # Si aucun fichier spécifié, trouver le premier CSV
            if file_path is None:
                csv_files = list(self.raw_data_dir.glob('*.csv'))
                if not csv_files:
                    raise FileNotFoundError(f"Aucun fichier CSV trouvé dans {self.raw_data_dir}")
                file_path = str(csv_files[0])
            
            file_path = Path(file_path)
            logger.info(f"Chargement du fichier: {file_path}")
            
            # Vérifier si le fichier existe
            if not file_path.exists():
                raise FileNotFoundError(f"Le fichier {file_path} n'existe pas")
            
            # Obtenir la taille du fichier
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"Taille du fichier: {file_size_mb:.2f} MB")
            
            # Décider de la méthode de lecture
            if file_size_mb > 500 or chunksize:
                # Lecture par chunks pour les gros fichiers
                if chunksize is None:
                    chunksize = 100000  # Valeur par défaut
                
                logger.info(f"Lecture par chunks de {chunksize} lignes...")
                chunks = []
                
                # Utiliser tqdm pour la barre de progression
                chunk_iterator = pd.read_csv(
                    file_path, 
                    chunksize=chunksize, 
                    low_memory=False,
                    encoding_errors='ignore'
                )
                
                total_chunks = 0
                for chunk in tqdm(chunk_iterator, desc="Chargement des données"):
                    chunks.append(chunk)
                    total_chunks += 1
                    
                    # Arrêter si on a atteint sample_size
                    if sample_size:
                        total_rows = sum(len(c) for c in chunks)
                        if total_rows >= sample_size:
                            logger.info(f"Arrêt après {total_rows} lignes (sample_size atteint)")
                            break
                
                # Concaténer tous les chunks
                df = pd.concat(chunks, ignore_index=True)
                
            else:
                # Lecture normale pour les petits fichiers
                logger.info("Lecture directe du fichier...")
                df = pd.read_csv(file_path, low_memory=False, encoding_errors='ignore')
            
            # Échantillonnage si demandé (après lecture complète)
            if sample_size and len(df) > sample_size:
                logger.info(f"Échantillonnage à {sample_size} lignes")
                df = df.sample(n=sample_size, random_state=42).reset_index(drop=True)
            
            logger.info(f"Données chargées: {len(df)} lignes, {len(df.columns)} colonnes")
            
            # Informations de base
            self._log_data_info(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {e}")
            raise
    
    def _log_data_info(self, df: pd.DataFrame):
        """Affiche des informations sur les données chargées"""
        logger.info("\n" + "="*50)
        logger.info("INFORMATIONS SUR LES DONNÉES")
        logger.info("="*50)
        logger.info(f"Shape: {df.shape[0]} lignes x {df.shape[1]} colonnes")
        logger.info(f"\nTypes de données:")
        for dtype, count in df.dtypes.value_counts().items():
            logger.info(f"  {dtype}: {count}")
        
        # Aperçu des colonnes
        logger.info(f"\nPremières colonnes:")
        for i, col in enumerate(df.columns[:20], 1):
            logger.info(f"  {i:2}. {col}")
        if len(df.columns) > 20:
            logger.info(f"  ... et {len(df.columns) - 20} colonnes supplémentaires")
        
        # Valeurs manquantes
        missing = df.isnull().sum()
        missing_total = missing.sum()
        missing_pct = (missing_total / (len(df) * len(df.columns))) * 100
        
        logger.info(f"\nValeurs manquantes: {missing_total:,} ({missing_pct:.2f}%)")
        
        if missing_total > 0:
            logger.info("Colonnes avec plus de 50% de valeurs manquantes:")
            for col in df.columns:
                pct_missing = df[col].isnull().mean() * 100
                if pct_missing > 50:
                    logger.info(f"  {col}: {pct_missing:.1f}%")
    
    def validate_raw_data(self, df: pd.DataFrame) -> dict:
        """
        Valide les données brutes pour s'assurer qu'elles sont exploitables
        
        Args:
            df: DataFrame à valider
            
        Returns:
            Dict avec les résultats de validation
        """
        validation_results = {
            'status': 'PASS',
            'issues': [],
            'stats': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'missing_values': df.isnull().sum().sum()
            }
        }
        
        # Vérifications de base
        if df.empty:
            validation_results['status'] = 'FAIL'
            validation_results['issues'].append('DataFrame vide')
        
        # Colonnes minimales requises
        required_columns = ['loan_amnt', 'int_rate', 'issue_d', 'loan_status']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            validation_results['status'] = 'WARNING'
            validation_results['issues'].append(f'Colonnes manquantes: {missing_columns}')
        
        # Vérifier les types de données
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        validation_results['stats']['numeric_columns'] = len(numeric_columns)
        
        # Vérifier les valeurs extrêmes
        if 'loan_amnt' in df.columns:
            negative_loans = (df['loan_amnt'] <= 0).sum()
            if negative_loans > 0:
                validation_results['issues'].append(f'{negative_loans} prêts avec montant négatif ou nul')
        
        logger.info(f"Validation des données brutes: {validation_results['status']}")
        if validation_results['issues']:
            for issue in validation_results['issues']:
                logger.warning(f"  - {issue}")
        
        return validation_results


# Fonctions utilitaires
def load_raw_data(file_path=None, sample_size=None, chunksize=None):
    """
    Fonction simplifiée pour charger les données
    
    Args:
        file_path: Chemin du fichier CSV
        sample_size: Taille de l'échantillon
        chunksize: Taille des chunks pour la lecture
        
    Returns:
        DataFrame pandas
    """
    extractor = DataExtractor()
    return extractor.load_raw_data(file_path, sample_size, chunksize)


if __name__ == "__main__":
    # Test du module
    logging.basicConfig(level=logging.INFO)
    
    extractor = DataExtractor()
    
    # Vérifier si des fichiers existent
    csv_files = list(extractor.raw_data_dir.glob('*.csv'))
    
    if csv_files:
        print(f"Fichiers trouvés: {[f.name for f in csv_files]}")
        
        # Tester avec un petit échantillon
        try:
            df_sample = extractor.load_raw_data(
                file_path=csv_files[0],
                sample_size=1000,
                chunksize=10000
            )
            print(f"\nTest réussi!")
            print(f"Shape: {df_sample.shape}")
            print(f"Colonnes: {list(df_sample.columns)[:10]}...")
        except Exception as e:
            print(f"Erreur lors du test: {e}")
    else:
        print(f"Aucun fichier CSV trouvé dans {extractor.raw_data_dir}")
        print("Placez un fichier CSV dans ce dossier ou utilisez download_sample.py")
