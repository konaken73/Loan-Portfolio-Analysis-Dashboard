"""
Module de chargement des données
Charge les données transformées dans la base de données et crée des vues analytiques
"""

import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class DataLoader:
    """Classe pour charger les données dans la base de données"""
    
    def __init__(self, db_path: str = 'data/loans.db'):
        """
        Initialise le chargeur de données
        
        Args:
            db_path: Chemin vers la base de données SQLite
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.engine = None
        
    def create_database_connection(self):
        """Crée une connexion à la base de données"""
        try:
            # Créer l'engine SQLAlchemy
            self.engine = create_engine(f'sqlite:///{self.db_path}')
            
            # Tester la connexion
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Connexion à la base de données établie: {self.db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur de connexion à la base de données: {e}")
            return False

    
    def load_to_sqlite(self, df: pd.DataFrame, table_name: str = 'loans', 
                       if_exists: str = 'replace', chunksize: int = 10000) -> bool:
        """
        Charge un DataFrame dans SQLite
        
        Args:
            df: DataFrame à charger
            table_name: Nom de la table
            if_exists: Comportement si table existe ('replace', 'append', 'fail')
            chunksize: Taille des chunks pour le chargement
            
        Returns:
            True si succès, False sinon
        """
        try:
            logger.info(f"Chargement des données dans la table '{table_name}'...")
            
            if self.engine is None:
                self.create_database_connection()
            
            # Adapter les types de données pour SQLite
            df = self._adapt_dataframe_for_sqlite(df)
            
            # Calculer le nombre de chunks
            total_rows = len(df)
            num_chunks = (total_rows // chunksize) + 1
            
            logger.info(f"Chargement de {total_rows} lignes en {num_chunks} chunks...")
            
            # Charger par chunks avec progression
            for i in range(0, total_rows, chunksize):
                chunk = df.iloc[i:i + chunksize]
                chunk_num = (i // chunksize) + 1
                
                # Charger le chunk
                chunk.to_sql(
                    table_name,
                    self.engine,
                    if_exists='append' if i > 0 else if_exists,
                    index=False,
                    method='multi',
                    chunksize=chunksize
                )
                
                logger.info(f"Chunk {chunk_num}/{num_chunks} chargé ({len(chunk)} lignes)")
            
            logger.info(f"Chargement terminé: {total_rows} lignes dans la table '{table_name}'")
            
            # Créer les index pour optimiser les requêtes
            self._create_indexes(table_name)
            
            # Valider le chargement
            validation = self._validate_load(table_name, expected_rows=total_rows)
            
            if validation['success']:
                logger.info("Validation du chargement réussie")
            else:
                logger.warning(f"Problèmes de validation: {validation['issues']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement: {e}")
            return False
    
    def _adapt_dataframe_for_sqlite(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adapte un DataFrame pour le chargement dans SQLite
        
        Args:
            df: DataFrame à adapter
            
        Returns:
            DataFrame adapté
        """
        df_adapted = df.copy()
        
        # Gérer les dates
        date_columns = df_adapted.select_dtypes(include=['datetime64[ns]']).columns
        
        for col in date_columns:
            # Convertir en string ISO format pour SQLite
            df_adapted[col] = df_adapted[col].dt.strftime('%Y-%m-%d')
        
        # Gérer les catégories
        category_columns = df_adapted.select_dtypes(include=['category']).columns
        
        for col in category_columns:
            df_adapted[col] = df_adapted[col].astype(str)
        
        # Remplacer les NaN par None pour SQLite
        df_adapted = df_adapted.where(pd.notnull(df_adapted), None)
        
        # Limiter la longueur des strings pour les colonnes textuelles
        text_columns = df_adapted.select_dtypes(include=['object']).columns
        
        for col in text_columns:
            # Tronquer les strings trop longs
            max_length = 255  # Longueur maximale raisonnable
            df_adapted[col] = df_adapted[col].astype(str).str[:max_length]
        
        return df_adapted
    
    def _create_indexes(self, table_name: str):
        """Crée des index pour optimiser les requêtes"""
        try:
            with self.engine.connect() as conn:
                # Index pour les requêtes fréquentes
                indexes = [
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_grade ON {table_name}(grade)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_loan_status ON {table_name}(loan_status)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_issue_date ON {table_name}(issue_d)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_is_default ON {table_name}(is_default)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_term ON {table_name}(term)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_int_rate ON {table_name}(int_rate)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_emp_title ON {table_name}(emp_title)",
                ]
                
                for index_sql in indexes:
                    conn.execute(text(index_sql))
                
                conn.commit()
            
            logger.info(f"Index créés pour la table '{table_name}'")
            
        except Exception as e:
            logger.error(f"Erreur lors de la création des index: {e}")
    
    def _validate_load(self, table_name: str, expected_rows: int) -> Dict:
        """
        Valide le chargement des données
        
        Args:
            table_name: Nom de la table
            expected_rows: Nombre de lignes attendu
            
        Returns:
            Dictionnaire avec résultats de validation
        """
        validation = {
            'success': True,
            'issues': [],
            'actual_rows': 0,
            'expected_rows': expected_rows
        }
        
        try:
            with self.engine.connect() as conn:
                # Compter les lignes dans la table
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                actual_rows = result.fetchone()[0]
                validation['actual_rows'] = actual_rows
                
                # Vérifier si le compte correspond
                if actual_rows != expected_rows:
                    validation['success'] = False
                    validation['issues'].append(
                        f"Nombre de lignes incorrect: {actual_rows} au lieu de {expected_rows}"
                    )
                
                # Vérifier les valeurs NULL dans les colonnes critiques
                critical_columns = ['loan_amnt', 'int_rate', 'issue_d', 'loan_status']
                
                for col in critical_columns:
                    result = conn.execute(
                        text(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")
                    )
                    null_count = result.fetchone()[0]
                    
                    if null_count > 0:
                        validation['issues'].append(
                            f"Colonne '{col}': {null_count} valeurs NULL"
                        )
                
                # Vérifier les valeurs aberrantes
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name} WHERE loan_amnt <= 0")
                )
                invalid_amounts = result.fetchone()[0]
                
                if invalid_amounts > 0:
                    validation['issues'].append(
                        f"Montants de prêt invalides: {invalid_amounts}"
                    )
            
        except Exception as e:
            validation['success'] = False
            validation['issues'].append(f"Erreur de validation: {e}")
        
        return validation
    
    def create_analytical_views(self):
        """Crée des vues analytiques pour faciliter l'analyse"""
        try:
            logger.info("Création des vues analytiques...")
            
            with self.engine.connect() as conn:
                # 1. Vue d'analyse des défauts
                conn.execute(text("""
                CREATE VIEW IF NOT EXISTS loan_default_analysis AS
                SELECT 
                    grade,
                    term,
                    home_ownership,
                    purpose,
                    income_category,
                    risk_category,
                    COUNT(*) as total_loans,
                    SUM(loan_amnt) as total_amount,
                    AVG(int_rate) as avg_interest_rate,
                    SUM(CASE WHEN is_default = 1 THEN loan_amnt ELSE 0 END) as default_amount,
                    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as default_count,
                    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate_percentage,
                    ROUND(AVG(annual_inc), 0) as avg_annual_income,
                    ROUND(AVG(dti), 2) as avg_dti
                FROM loans
                GROUP BY grade, term, home_ownership, purpose, income_category, risk_category
                ORDER BY default_rate_percentage DESC
                """))
                
                # 2. Vue de performance mensuelle
                conn.execute(text("""
                CREATE VIEW IF NOT EXISTS monthly_performance AS
                SELECT 
                    issue_year,
                    issue_month,
                    issue_quarter,
                    issue_season,
                    COUNT(*) as loans_issued,
                    SUM(loan_amnt) as amount_issued,
                    AVG(int_rate) as avg_interest_rate,
                    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defaults_count,
                    SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) as paid_count,
                    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate,
                    ROUND(SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as paid_rate
                FROM loans
                WHERE issue_year IS NOT NULL
                GROUP BY issue_year, issue_month, issue_quarter, issue_season
                ORDER BY issue_year, issue_month
                """))
                
                # 3. Vue de segmentation des emprunteurs
                conn.execute(text("""
                CREATE VIEW IF NOT EXISTS borrower_segmentation AS
                SELECT 
                    emp_title,
                    home_ownership,
                    verification_status,
                    income_category,
                    credit_age_category,
                    COUNT(*) as borrower_count,
                    SUM(loan_amnt) as total_borrowed,
                    AVG(int_rate) as avg_interest_rate,
                    AVG(annual_inc) as avg_annual_income,
                    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as default_count,
                    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate,
                    ROUND(AVG(loan_amnt), 0) as avg_loan_amount
                FROM loans
                WHERE emp_title IS NOT NULL AND emp_title != 'UNKNOWN'
                GROUP BY emp_title, home_ownership, verification_status, income_category, credit_age_category
                HAVING COUNT(*) >= 5
                ORDER BY borrower_count DESC
                """))
                
                # 4. Vue de récupération des prêts
                conn.execute(text("""
                CREATE VIEW IF NOT EXISTS loan_recovery_analysis AS
                SELECT 
                    loan_status,
                    grade,
                    COUNT(*) as loan_count,
                    SUM(loan_amnt) as total_issued,
                    AVG(total_pymnt) as avg_amount_paid,
                    SUM(total_pymnt) as total_received,
                    ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2) as recovery_rate_percentage,
                    ROUND(AVG(int_rate), 2) as avg_interest_rate
                FROM loans
                WHERE loan_status IN ('CHARGED OFF', 'DEFAULT', 'FULLY PAID', 'CURRENT')
                GROUP BY loan_status, grade
                ORDER BY loan_status, recovery_rate_percentage DESC
                """))
                
                # 5. Vue pour le dashboard (KPIs agrégés)
                conn.execute(text("""
                CREATE VIEW IF NOT EXISTS dashboard_kpis AS
                SELECT 
                    'Portfolio Total' as metric,
                    COUNT(*) as value,
                    'Nombre de prêts' as description
                FROM loans
                UNION ALL
                SELECT 
                    'Montant Total',
                    SUM(loan_amnt),
                    'Montant total du portefeuille ($)'
                FROM loans
                UNION ALL
                SELECT 
                    'Taux de Défaut',
                    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
                    'Pourcentage de prêts en défaut (%)'
                FROM loans
                UNION ALL
                SELECT 
                    'Taux Intérêt Moyen',
                    ROUND(AVG(int_rate), 2),
                    'Taux d\'intérêt moyen (%)'
                FROM loans
                UNION ALL
                SELECT 
                    'Revenu Moyen',
                    ROUND(AVG(annual_inc), 0),
                    'Revenu annuel moyen des emprunteurs ($)'
                FROM loans
                UNION ALL
                SELECT 
                    'Durée Moyenne',
                    ROUND(AVG(CASE 
                        WHEN term LIKE '%36%' THEN 36
                        WHEN term LIKE '%60%' THEN 60
                        ELSE 36 
                    END), 0),
                    'Durée moyenne des prêts (mois)'
                FROM loans
                """))
                
                conn.commit()
            
            logger.info("Vues analytiques créées avec succès")
            
            # Lister les vues créées
            self._list_database_objects()
            
        except Exception as e:
            logger.error(f"Erreur lors de la création des vues: {e}")
    
    def _list_database_objects(self):
        """Liste les tables et vues de la base de données"""
        try:
            inspector = inspect(self.engine)
            
            tables = inspector.get_table_names()
            logger.info(f"Tables dans la base: {tables}")
            
            views = inspector.get_view_names()
            logger.info(f"Vues dans la base: {views}")
            
        except Exception as e:
            logger.error(f"Erreur lors du listing des objets: {e}")
    
    def export_table_to_csv(self, table_name: str, output_path: str):
        """
        Exporte une table vers CSV
        
        Args:
            table_name: Nom de la table ou vue
            output_path: Chemin de sortie
        """
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, self.engine)
            
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(output_path, index=False)
            logger.info(f"Table '{table_name}' exportée vers {output_path} ({len(df)} lignes)")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'export: {e}")
    
    def get_database_stats(self) -> Dict:
        """Retourne des statistiques sur la base de données"""
        stats = {}
        
        try:
            with self.engine.connect() as conn:
                # Taille de la base
                result = conn.execute(
                    text("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
                )
                db_size = result.fetchone()[0]
                stats['database_size_mb'] = db_size / (1024 * 1024)
                
                # Nombre de tables
                result = conn.execute(
                    text("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                )
                stats['table_count'] = result.fetchone()[0]
                
                # Nombre de vues
                result = conn.execute(
                    text("SELECT COUNT(*) FROM sqlite_master WHERE type='view'")
                )
                stats['view_count'] = result.fetchone()[0]
                
                # Statistiques sur la table loans
                if 'loans' in self.get_table_names():
                    result = conn.execute(text("SELECT COUNT(*) FROM loans"))
                    stats['loans_row_count'] = result.fetchone()[0]
                    
                    result = conn.execute(
                        text("SELECT MIN(issue_d), MAX(issue_d) FROM loans WHERE issue_d IS NOT NULL")
                    )
                    min_date, max_date = result.fetchone()
                    stats['date_range'] = f"{min_date} to {max_date}"
                
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des stats: {e}")
        
        return stats
    
    def get_table_names(self) -> List[str]:
        """Retourne la liste des tables"""
        try:
            inspector = inspect(self.engine)
            return inspector.get_table_names()
        except:
            return []
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Exécute une requête SQL et retourne un DataFrame"""
        try:
            return pd.read_sql_query(query, self.engine)
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            raise


# Fonctions utilitaires
def load_to_sqlite(df, db_path='data/loans.db', table_name='loans'):
    """Fonction simplifiée pour charger des données"""
    loader = DataLoader(db_path)
    return loader.load_to_sqlite(df, table_name)

def create_analytical_views(db_path='data/loans.db'):
    """Fonction simplifiée pour créer les vues"""
    loader = DataLoader(db_path)
    loader.create_database_connection()
    loader.create_analytical_views()


if __name__ == "__main__":
    # Exemple d'utilisation
    logging.basicConfig(level=logging.INFO)
    
    # Créer des données de test
    test_data = pd.DataFrame({
        'loan_amnt': [10000, 20000, 15000, 25000, 30000],
        'int_rate': [10.5, 15.2, 12.0, 8.5, 9.0],
        'issue_d': ['2018-12-01', '2019-01-15', '2019-02-20', '2019-03-10', '2019-04-05'],
        'loan_status': ['FULLY PAID', 'CHARGED OFF', 'CURRENT', 'DEFAULT', 'FULLY PAID'],
        'grade': ['A', 'B', 'C', 'D', 'A'],
        'term': ['36 months', '60 months', '36 months', '60 months', '36 months'],
        'is_default': [0, 1, 0, 1, 0]
    })
    
    # Initialiser le loader
    loader = DataLoader('data/test_loans.db')
    
    # Charger les données
    success = loader.load_to_sqlite(test_data, 'test_loans')
    
    if success:
        # Créer des vues analytiques
        loader.create_analytical_views()
        
        # Exporter en CSV
        loader.export_table_to_csv('test_loans', 'data/outputs/test_loans.csv')
        
        # Afficher les stats
        stats = loader.get_database_stats()
        print(f"Statistiques de la base: {stats}")
