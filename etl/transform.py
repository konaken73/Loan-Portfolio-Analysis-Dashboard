"""
Module de transformation des données
Nettoie, transforme et prépare les données pour l'analyse
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import logging
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class DataTransformer:
    """Classe pour transformer et nettoyer les données de prêt"""
    
    def __init__(self):
        self.date_columns = []
        self.categorical_columns = []
        self.numeric_columns = []
    
    def clean_loan_data(self, df: pd.DataFrame, config: Optional[Dict] = None) -> pd.DataFrame:
        """
        Nettoie et transforme les données de prêt
        
        Args:
            df: DataFrame brut
            config: Configuration de nettoyage
            
        Returns:
            DataFrame nettoyé
        """
        logger.info("Début du nettoyage des données")
        
        # Configuration par défaut
        if config is None:
            config = {
                'handle_missing': True,
                'convert_dates': True,
                'create_features': True,
                'remove_outliers': False,
                'outlier_threshold': 3.0  # Écart-type pour outliers
            }
        
        # Créer une copie pour éviter les modifications en place
        df_clean = df.copy()
        
        # 1. Standardiser les noms de colonnes
        df_clean = self._standardize_column_names(df_clean)
        
        # 2. Gestion des valeurs manquantes
        if config['handle_missing']:
            df_clean = self._handle_missing_values(df_clean)
        
        # 3. Conversion des types de données
        df_clean = self._convert_data_types(df_clean)
        
        # 4. Conversion des dates
        if config['convert_dates']:
            df_clean = self._convert_date_columns(df_clean)
        
        # 5. Nettoyage des valeurs textuelles
        df_clean = self._clean_text_columns(df_clean)
        
        # 6. Création de variables dérivées
        if config['create_features']:
            df_clean = self._create_derived_features(df_clean)
        
        # 7. Gestion des outliers (optionnel)
        if config['remove_outliers']:
            df_clean = self._handle_outliers(df_clean, config['outlier_threshold'])
        
        # 8. Validation finale
        validation_report = self._validate_cleaned_data(df_clean)
        
        logger.info(f"Nettoyage terminé. Shape final: {df_clean.shape}")
        logger.info(f"Rapport de validation: {validation_report['status']}")
        
        return df_clean
    
    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardise les noms de colonnes"""
        logger.info("Standardisation des noms de colonnes")
        
        # Créer un mapping pour renommer les colonnes
        column_mapping = {}
        
        for col in df.columns:
            # Convertir en minuscules, remplacer espaces et caractères spéciaux
            new_name = str(col).strip().lower()
            new_name = re.sub(r'[^\w\s]', '_', new_name)  # Remplace caractères spéciaux
            new_name = re.sub(r'\s+', '_', new_name)  # Remplace espaces
            
            if new_name != col:
                column_mapping[col] = new_name
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
            logger.info(f"Colonnes renommées: {len(column_mapping)}")
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gère les valeurs manquantes de manière stratégique"""
        logger.info("Gestion des valeurs manquantes")
        
        # Identifier les colonnes avec valeurs manquantes
        missing_stats = df.isnull().sum()
        total_missing = missing_stats.sum()
        logger.info(f"Valeurs manquantes totales avant traitement: {total_missing}")
        
        # Stratégie par type de colonne
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            
            if missing_count == 0:
                continue
            
            missing_pct = (missing_count / len(df)) * 100
            
            # Colonnes critiques - on ne peut pas avoir de valeurs manquantes
            critical_columns = ['loan_amnt', 'issue_d', 'loan_status']
            if col in critical_columns and missing_pct > 0:
                logger.warning(f"Colonne critique avec valeurs manquantes: {col} ({missing_pct:.1f}%)")
            
            # Stratégie basée sur le type de données
            if df[col].dtype == 'object':
                # Colonnes catégorielles
                if missing_pct > 30:
                    # Trop de valeurs manquantes - créer une catégorie spéciale
                    df[col] = df[col].fillna('UNKNOWN')
                else:
                    # Remplacer par le mode (valeur la plus fréquente)
                    mode_val = df[col].mode()[0] if not df[col].mode().empty else 'UNKNOWN'
                    df[col] = df[col].fillna(mode_val)
            
            elif pd.api.types.is_numeric_dtype(df[col]):
                # Colonnes numériques
                if missing_pct > 30:
                    # Remplacer par 0 ou median selon le contexte
                    if 'rate' in col.lower() or 'pct' in col.lower():
                        df[col] = df[col].fillna(0)
                    else:
                        df[col] = df[col].fillna(df[col].median())
                else:
                    # Remplacer par la médiane
                    df[col] = df[col].fillna(df[col].median())
            
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                # Colonnes date - remplacer par une date par défaut ou supprimer
                df[col] = df[col].fillna(pd.NaT)
        
        # Supprimer les lignes où les colonnes critiques sont toujours manquantes
        rows_before = len(df)
        df = df.dropna(subset=critical_columns)
        rows_after = len(df)
        
        if rows_before != rows_after:
            logger.info(f"Lignes supprimées: {rows_before - rows_after}")
        
        missing_after = df.isnull().sum().sum()
        logger.info(f"Valeurs manquantes après traitement: {missing_after}")
        
        return df
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convertit les types de données"""
        logger.info("Conversion des types de données")
        
        # Identifier les colonnes par type
        self.numeric_columns = []
        self.categorical_columns = []
        self.date_columns = []
        
        for col in df.columns:
            # Vérifier si c'est une colonne de date par son nom
            if any(date_term in col.lower() for date_term in ['date', 'd_', '_d', 'issue', 'last_', 'next_', 'earliest']):
                self.date_columns.append(col)
            
            # Vérifier les colonnes numériques
            elif pd.api.types.is_numeric_dtype(df[col]):
                self.numeric_columns.append(col)
            
            # Colonnes catégorielles (texte avec peu de valeurs uniques)
            elif df[col].dtype == 'object':
                unique_count = df[col].nunique()
                if unique_count < 50 and unique_count < len(df) * 0.1:
                    self.categorical_columns.append(col)
        
        # Conversion spécifique pour les colonnes connues
        if 'int_rate' in df.columns:
            # Convertir les taux d'intérêt (format "10.5%" -> 10.5)
            df['int_rate'] = pd.to_numeric(
                df['int_rate'].astype(str).str.replace('%', ''), 
                errors='coerce'
            )
        
        if 'revol_util' in df.columns:
            # Convertir l'utilisation de la revolving (format "50.5%" -> 50.5)
            df['revol_util'] = pd.to_numeric(
                df['revol_util'].astype(str).str.replace('%', ''), 
                errors='coerce'
            )
        
        # Convertir les colonnes catégorielles
        for col in self.categorical_columns:
            df[col] = df[col].astype('category')
        
        logger.info(f"Colonnes numériques: {len(self.numeric_columns)}")
        logger.info(f"Colonnes catégorielles: {len(self.categorical_columns)}")
        logger.info(f"Colonnes date: {len(self.date_columns)}")
        
        return df
    
    def _convert_date_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convertit les colonnes de date"""
        logger.info("Conversion des colonnes de date")
        
        date_patterns = [
            '%b-%Y',  # Dec-2018
            '%Y-%m-%d',  # 2018-12-01
            '%m/%d/%Y',  # 12/01/2018
            '%d-%m-%Y',  # 01-12-2018
        ]
        
        for col in self.date_columns:
            if col not in df.columns:
                continue
            
            # Essayer différents formats de date
            for pattern in date_patterns:
                try:
                    df[col] = pd.to_datetime(df[col], format=pattern, errors='coerce')
                    # Vérifier si la conversion a réussi pour au moins certaines valeurs
                    if df[col].notna().any():
                        logger.info(f"Colonne {col} convertie avec format {pattern}")
                        break
                except Exception:
                    continue
            
            # Si aucune conversion n'a fonctionné, essayer la conversion automatique
            if df[col].dtype != 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    
    def _clean_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie les colonnes textuelles"""
        logger.info("Nettoyage des colonnes textuelles")
        
        text_columns = [col for col in df.columns if df[col].dtype == 'object']
        
        for col in text_columns:
            # Convertir en string, supprimer les espaces
            df[col] = df[col].astype(str).str.strip()
            
            # Standardiser les valeurs NULL/NaN
            null_values = ['nan', 'none', 'null', 'na', '']
            df[col] = df[col].replace(null_values, np.nan)
            
            # Standardiser la casse pour certaines colonnes
            if col in ['loan_status', 'grade', 'sub_grade', 'emp_title']:
                df[col] = df[col].str.upper()
        
        return df
    
    def _create_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crée des variables dérivées pour l'analyse"""
        logger.info("Création de variables dérivées")
        
        # 1. Variable cible: Défaut
        if 'loan_status' in df.columns:
            # Définir les statuts considérés comme défaut
            default_statuses = [
                'CHARGED OFF', 'DEFAULT', 'LATE (31-120 DAYS)', 
                'LATE (16-30 DAYS)', 'IN GRACE PERIOD'
            ]
            df['is_default'] = df['loan_status'].isin(default_statuses).astype(int)
            df['is_fully_paid'] = (df['loan_status'] == 'FULLY PAID').astype(int)
            
            logger.info(f"Distribution des défauts: {df['is_default'].mean():.2%}")
        
        # 2. Catégories de revenu
        if 'annual_inc' in df.columns:
            df['income_category'] = pd.cut(
                df['annual_inc'],
                bins=[0, 30000, 60000, 100000, 200000, float('inf')],
                labels=['Très faible', 'Faible', 'Moyen', 'Élevé', 'Très élevé'],
                include_lowest=True
            )
        
        # 3. Ratio prêt/revenu
        if all(col in df.columns for col in ['loan_amnt', 'annual_inc']):
            df['loan_to_income_ratio'] = df['loan_amnt'] / df['annual_inc'].replace(0, np.nan)
        
        # 4. Catégories d'âge du crédit
        if 'earliest_cr_line' in df.columns and 'issue_d' in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df['earliest_cr_line']) and \
               pd.api.types.is_datetime64_any_dtype(df['issue_d']):
                df['credit_age_years'] = (df['issue_d'] - df['earliest_cr_line']).dt.days / 365.25
                df['credit_age_category'] = pd.cut(
                    df['credit_age_years'],
                    bins=[0, 2, 5, 10, 20, float('inf')],
                    labels=['0-2 ans', '2-5 ans', '5-10 ans', '10-20 ans', '20+ ans']
                )
        
        # 5. Risque par grade
        if 'grade' in df.columns:
            grade_risk_map = {
                'A': 'Faible risque',
                'B': 'Risque modéré',
                'C': 'Risque moyen',
                'D': 'Risque élevé',
                'E': 'Risque très élevé',
                'F': 'Risque extrême',
                'G': 'Risque extrême'
            }
            df['risk_category'] = df['grade'].map(grade_risk_map)
        
        # 6. Saisonnalité
        if 'issue_d' in df.columns and pd.api.types.is_datetime64_any_dtype(df['issue_d']):
            df['issue_year'] = df['issue_d'].dt.year
            df['issue_month'] = df['issue_d'].dt.month
            df['issue_quarter'] = df['issue_d'].dt.quarter
            df['issue_season'] = df['issue_month'].apply(self._get_season)
        
        # 7. Taux d'intérêt normalisé
        if 'int_rate' in df.columns:
            df['int_rate_category'] = pd.cut(
                df['int_rate'],
                bins=[0, 5, 10, 15, 20, 30, float('inf')],
                labels=['0-5%', '5-10%', '10-15%', '15-20%', '20-30%', '30%+']
            )
        
        logger.info(f"Variables dérivées créées: {[col for col in df.columns if col.startswith('is_') or col.endswith('_category')]}")
        
        return df
    
    def _get_season(self, month: int) -> str:
        """Convertit un mois en saison"""
        if month in [12, 1, 2]:
            return 'Hiver'
        elif month in [3, 4, 5]:
            return 'Printemps'
        elif month in [6, 7, 8]:
            return 'Été'
        else:
            return 'Automne'
    
    def _handle_outliers(self, df: pd.DataFrame, threshold: float = 3.0) -> pd.DataFrame:
        """Gère les valeurs aberrantes"""
        logger.info("Gestion des outliers")
        
        for col in self.numeric_columns:
            if col in ['is_default', 'is_fully_paid']:
                continue
            
            # Calculer les bornes avec l'écart-type
            mean = df[col].mean()
            std = df[col].std()
            
            lower_bound = mean - threshold * std
            upper_bound = mean + threshold * std
            
            # Identifier les outliers
            outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_count = outliers.sum()
            
            if outlier_count > 0:
                logger.info(f"Colonne {col}: {outlier_count} outliers détectés")
                
                # Option 1: Winsorizing (remplacer par les bornes)
                df[col] = np.where(df[col] < lower_bound, lower_bound, df[col])
                df[col] = np.where(df[col] > upper_bound, upper_bound, df[col])
        
        return df
    
    def _validate_cleaned_data(self, df: pd.DataFrame) -> Dict:
        """Valide les données nettoyées"""
        logger.info("Validation des données nettoyées")
        
        validation = {
            'status': 'PASS',
            'issues': [],
            'stats': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'missing_values': df.isnull().sum().sum(),
                'duplicate_rows': df.duplicated().sum()
            }
        }
        
        # Vérifications
        if df.empty:
            validation['status'] = 'FAIL'
            validation['issues'].append('DataFrame vide')
        
        if validation['stats']['duplicate_rows'] > 0:
            validation['issues'].append(f"{validation['stats']['duplicate_rows']} lignes dupliquées")
        
        if validation['stats']['missing_values'] > len(df) * len(df.columns) * 0.05:
            validation['issues'].append(f"Trop de valeurs manquantes: {validation['stats']['missing_values']}")
        
        # Vérifier les colonnes requises
        required_columns = ['loan_amnt', 'int_rate', 'issue_d', 'loan_status', 'is_default']
        missing_required = [col for col in required_columns if col not in df.columns]
        
        if missing_required:
            validation['status'] = 'FAIL'
            validation['issues'].append(f"Colonnes requises manquantes: {missing_required}")
        
        # Vérifier les valeurs aberrantes
        if 'annual_inc' in df.columns:
            negative_income = (df['annual_inc'] <= 0).sum()
            if negative_income > 0:
                validation['issues'].append(f"{negative_income} revenus annuels négatifs ou nuls")
        
        if validation['issues']:
            logger.warning(f"Issues de validation: {validation['issues']}")
        
        return validation


# Fonctions utilitaires
def clean_loan_data(df, config=None):
    """Fonction simplifiée pour nettoyer les données"""
    transformer = DataTransformer()
    return transformer.clean_loan_data(df, config)

def validate_data_quality(df):
    """Fonction simplifiée pour valider la qualité des données"""
    transformer = DataTransformer()
    return transformer._validate_cleaned_data(df)


if __name__ == "__main__":
    # Exemple d'utilisation
    logging.basicConfig(level=logging.INFO)
    
    # Créer des données de test
    test_data = pd.DataFrame({
        'loan_amnt': [10000, 20000, 15000, None, 25000],
        'int_rate': ['10.5%', '15.2%', None, '12.0%', '8.5%'],
        'issue_d': ['Dec-2018', 'Jan-2019', None, 'Feb-2019', 'Mar-2019'],
        'loan_status': ['Fully Paid', 'Charged Off', 'Current', 'Default', 'Fully Paid'],
        'annual_inc': [50000, 60000, 30000, 45000, 80000],
        'emp_title': ['Engineer', 'Teacher', None, 'Manager', 'Doctor']
    })
    
    transformer = DataTransformer()
    cleaned_data = transformer.clean_loan_data(test_data)
    
    print("Données nettoyées:")
    print(cleaned_data)
    print(f"\nShape: {cleaned_data.shape}")
