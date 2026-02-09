"""
Module de chargement des données avec SQLite pur - Version corrigée
Gère correctement les types de données et évite les erreurs de signature
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import numpy as np
from typing import Dict, List, Optional
import json

logger = logging.getLogger(__name__)

class SQLiteDataLoader:
    """Chargeur de données utilisant uniquement SQLite"""
    
    def __init__(self, db_path: str = 'data/loans.db'):
        """
        Initialise le chargeur de données
        
        Args:
            db_path: Chemin vers la base de données SQLite
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
    
    def create_connection(self):
        """Crée une connexion à la base de données"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            # Optimiser SQLite
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA synchronous = NORMAL")
            self.conn.execute("PRAGMA cache_size = -64000")
            self.conn.execute("PRAGMA foreign_keys = OFF")
            
            logger.info(f"Connexion SQLite établie: {self.db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur de connexion SQLite: {e}")
            return False
    
    def load_to_sqlite(self, df: pd.DataFrame, table_name: str = 'loans', 
                       batch_size: int = 500) -> bool:
        """
        Charge un DataFrame dans SQLite en utilisant SQLite directement
        
        Args:
            df: DataFrame à charger
            table_name: Nom de la table
            batch_size: Taille des lots pour l'insertion
            
        Returns:
            True si succès, False sinon
        """
        try:
            logger.info(f"Chargement des données dans '{table_name}'...")
            
            if self.conn is None:
                self.create_connection()
            
            # Préparer les données - GESTION SIMPLIFIÉE DES TYPES
            df_prepared = self._prepare_dataframe_simple(df)
            
            total_rows = len(df_prepared)
            logger.info(f"Chargement de {total_rows} lignes en lots de {batch_size}...")
            
            # Créer la table
            self._create_table_simple(df_prepared, table_name)
            
            # Insérer les données par lots
            success = self._insert_data_simple(df_prepared, table_name, batch_size)
            
            if success:
                # Créer les index
                self._create_indexes(table_name)
                
                # Valider le chargement
                validation = self._validate_load(table_name, expected_rows=total_rows)
                
                if validation['success']:
                    logger.info("✓ Validation du chargement réussie")
                else:
                    logger.warning(f"Problèmes de validation: {validation['issues']}")
                
                logger.info(f"✓ Chargement terminé: {total_rows} lignes")
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _prepare_dataframe_simple(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prépare le DataFrame pour SQLite - Version SIMPLIFIÉE
        
        Args:
            df: DataFrame à préparer
            
        Returns:
            DataFrame préparé
        """
        # Sélectionner les colonnes essentielles (limitées)
        essential_columns = [
            'loan_amnt', 'int_rate', 'term', 'grade', 'issue_d',
            'loan_status', 'annual_inc', 'dti', 'home_ownership',
            'emp_length', 'purpose', 'addr_state', 'delinq_2yrs',
            'revol_util', 'total_pymnt', 'is_default', 'is_fully_paid'
        ]
        
        # Garder seulement les colonnes qui existent
        cols_to_keep = []
        for col in essential_columns:
            if col in df.columns:
                cols_to_keep.append(col)
        
        # Si trop peu de colonnes, prendre les premières colonnes
        if len(cols_to_keep) < 10:
            logger.warning("Peu de colonnes essentielles trouvées, prise des premières colonnes")
            cols_to_keep = list(df.columns)[:20]  # Prendre les 20 premières
        
        logger.info(f"Colonnes à charger: {len(cols_to_keep)}")
        logger.info(f"Colonnes: {cols_to_keep}")
        
        df_prepared = df[cols_to_keep].copy()
        
        # CONVERSION SIMPLIFIÉE DES TYPES
        for col in df_prepared.columns:
            # Convertir les dates
            if 'date' in col.lower() or col in ['issue_d', 'earliest_cr_line', 'last_pymnt_d']:
                try:
                    df_prepared[col] = pd.to_datetime(df_prepared[col], errors='coerce')
                    df_prepared[col] = df_prepared[col].dt.strftime('%Y-%m-%d')
                except:
                    # Si conversion échoue, garder comme string
                    df_prepared[col] = df_prepared[col].astype(str)
            
            # Convertir les booléens
            elif col in ['is_default', 'is_fully_paid']:
                df_prepared[col] = df_prepared[col].astype(int)
            
            # Convertir les nombres
            elif pd.api.types.is_numeric_dtype(df_prepared[col]):
                df_prepared[col] = pd.to_numeric(df_prepared[col], errors='coerce')
                # Remplacer NaN par None
                df_prepared[col] = df_prepared[col].where(pd.notnull(df_prepared[col]), None)
            
            # Pour les autres, convertir en string
            else:
                df_prepared[col] = df_prepared[col].astype(str)
                # Remplacer 'nan' string par None
                df_prepared[col] = df_prepared[col].replace(['nan', 'NaN', 'NaT', 'None', 'null'], None)
        
        # Remplacer tous les NaN restants par None
        df_prepared = df_prepared.where(pd.notnull(df_prepared), None)
        
        logger.info(f"DataFrame préparé: {len(df_prepared)} lignes, {len(df_prepared.columns)} colonnes")
        return df_prepared
    
    def _create_table_simple(self, df: pd.DataFrame, table_name: str):
        """
        Crée une table dans SQLite - Version SIMPLIFIÉE
        
        Args:
            df: DataFrame avec les données
            table_name: Nom de la table
        """
        try:
            cursor = self.conn.cursor()
            
            # Supprimer la table si elle existe
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Détecter les types SQL simples
            columns_def = []
            for col in df.columns:
                # Échantillonner la première valeur non-nulle pour déterminer le type
                sample = df[col].dropna()
                if len(sample) > 0:
                    sample_value = sample.iloc[0]
                    
                    # Déterminer le type SQL
                    if isinstance(sample_value, (int, np.integer)):
                        sql_type = "INTEGER"
                    elif isinstance(sample_value, (float, np.floating)):
                        sql_type = "REAL"
                    elif isinstance(sample_value, str) and len(sample_value) == 10 and sample_value[4] == '-':
                        # Date au format YYYY-MM-DD
                        sql_type = "DATE"
                    else:
                        sql_type = "TEXT"
                else:
                    # Par défaut TEXT si pas de données
                    sql_type = "TEXT"
                
                columns_def.append(f'"{col}" {sql_type}')
            
            # Créer la table avec types simples
            create_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns_def)}
            )
            """
            
            cursor.execute(create_sql)
            self.conn.commit()
            
            logger.info(f"✓ Table '{table_name}' créée avec {len(df.columns)} colonnes")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création de la table: {e}")
            raise
    
    def _insert_data_simple(self, df: pd.DataFrame, table_name: str, 
                            batch_size: int = 500) -> bool:
        """
        Insère les données - Version SIMPLIFIÉE
        
        Args:
            df: DataFrame avec les données
            table_name: Nom de la table
            batch_size: Taille des lots
            
        Returns:
            True si succès
        """
        try:
            cursor = self.conn.cursor()
            
            # Préparer la requête SQL
            columns = list(df.columns)
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"""
            INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in columns])})
            VALUES ({placeholders})
            """
            
            # Convertir les données en liste de tuples avec conversion de type SIMPLE
            data = []
            for _, row in df.iterrows():
                row_data = []
                for col in columns:
                    val = row[col]
                    
                    # CONVERSION SIMPLE DES TYPES POUR SQLite
                    if val is None or pd.isna(val):
                        row_data.append(None)
                    elif isinstance(val, (int, np.integer)):
                        row_data.append(int(val))
                    elif isinstance(val, (float, np.floating)):
                        row_data.append(float(val))
                    elif isinstance(val, str):
                        # Nettoyer les strings
                        clean_val = str(val).strip()
                        if clean_val in ['', 'nan', 'NaN', 'NaT', 'None', 'null']:
                            row_data.append(None)
                        else:
                            row_data.append(clean_val)
                    else:
                        # Convertir en string pour tout le reste
                        row_data.append(str(val))
                
                data.append(tuple(row_data))
            
            # Insérer par lots
            total_batches = (len(data) // batch_size) + 1
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                try:
                    cursor.executemany(insert_sql, batch)
                    self.conn.commit()
                    
                    batch_num = (i // batch_size) + 1
                    if batch_num % 10 == 0 or batch_num == total_batches:
                        logger.info(f"Lot {batch_num}/{total_batches} inséré ({len(batch)} lignes)")
                
                except sqlite3.Error as e:
                    logger.error(f"❌ Erreur SQLite sur le lot {batch_num}: {e}")
                    
                    # Log détaillé pour debug
                    if batch and len(batch) > 0:
                        problematic_row = batch[0]
                        logger.error(f"Première ligne du lot problématique: {problematic_row}")
                        logger.error(f"Types: {[type(v) for v in problematic_row]}")
                    
                    self.conn.rollback()
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'insertion des données: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def _create_indexes(self, table_name: str):
        """Crée des index pour optimiser les requêtes"""
        try:
            cursor = self.conn.cursor()
            
            # Index essentiels seulement
            indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_grade ON {table_name}(grade)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_loan_status ON {table_name}(loan_status)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_issue_d ON {table_name}(issue_d)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_is_default ON {table_name}(is_default)",
            ]
            
            for index_sql in indexes:
                try:
                    cursor.execute(index_sql)
                except Exception as e:
                    logger.warning(f"⚠️ Erreur création index {index_sql}: {e}")
            
            self.conn.commit()
            
            logger.info(f"✓ Index créés pour la table '{table_name}'")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création des index: {e}")
    
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
            cursor = self.conn.cursor()
            
            # Compter les lignes dans la table
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            actual_rows = cursor.fetchone()[0]
            validation['actual_rows'] = actual_rows
            
            # Vérifier si le compte correspond (tolérance de 1%)
            if abs(actual_rows - expected_rows) > expected_rows * 0.01:
                validation['success'] = False
                validation['issues'].append(
                    f"Nombre de lignes incorrect: {actual_rows} au lieu de {expected_rows}"
                )
            
            # Vérifier les valeurs NULL dans les colonnes critiques
            critical_columns = ['loan_amnt', 'int_rate', 'loan_status']
            
            for col in critical_columns:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")
                    null_count = cursor.fetchone()[0]
                    
                    if null_count > 0:
                        validation['issues'].append(
                            f"Colonne '{col}': {null_count} valeurs NULL"
                        )
                except:
                    # La colonne n'existe peut-être pas
                    pass
            
        except Exception as e:
            validation['success'] = False
            validation['issues'].append(f"Erreur de validation: {e}")
        
        return validation
    
    def create_views(self):
        """Crée des vues analytiques SIMPLIFIÉES"""
        try:
            cursor = self.conn.cursor()
            
            # Vérifier si la table existe
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='loans'")
            if not cursor.fetchone():
                logger.error("❌ La table 'loans' n'existe pas. Impossible de créer les vues.")
                return
            
            # 1. Vue simplifiée pour le dashboard
            cursor.execute("""
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
                'Montant total ($)'
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
                'Taux d''intérêt moyen (%)'
            FROM loans
            UNION ALL
            SELECT 
                'Revenu Moyen',
                ROUND(AVG(annual_inc), 0),
                'Revenu annuel moyen ($)'
            FROM loans
            """)
            
            # 2. Vue par grade
            cursor.execute("""
            CREATE VIEW IF NOT EXISTS grade_analysis AS
            SELECT 
                grade,
                COUNT(*) as loan_count,
                SUM(loan_amnt) as total_amount,
                ROUND(AVG(int_rate), 2) as avg_interest_rate,
                SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as default_count,
                ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate
            FROM loans
            WHERE grade IS NOT NULL
            GROUP BY grade
            ORDER BY grade
            """)
            
            # 3. Vue temporelle simple
            cursor.execute("""
            CREATE VIEW IF NOT EXISTS time_analysis AS
            SELECT 
                SUBSTR(issue_d, 1, 4) as year,
                SUBSTR(issue_d, 6, 2) as month,
                COUNT(*) as loans_issued,
                SUM(loan_amnt) as amount_issued,
                SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defaults
            FROM loans
            WHERE issue_d IS NOT NULL AND LENGTH(issue_d) >= 10
            GROUP BY year, month
            ORDER BY year, month
            """)
            
            self.conn.commit()
            
            logger.info("✓ Vues analytiques créées avec succès")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création des vues: {e}")
            self.conn.rollback()
    
    def close(self):
        """Ferme la connexion à la base de données"""
        if self.conn:
            self.conn.close()
            logger.info("Connexion SQLite fermée")
    
    def get_table_info(self):
        """Retourne des informations sur les tables"""
        try:
            cursor = self.conn.cursor()
            
            # Liste des tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Liste des vues
            cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
            views = [row[0] for row in cursor.fetchall()]
            
            # Statistiques sur la table loans
            stats = {}
            if 'loans' in tables:
                cursor.execute("SELECT COUNT(*) FROM loans")
                stats['loans_row_count'] = cursor.fetchone()[0]
                
                try:
                    cursor.execute("SELECT MIN(issue_d), MAX(issue_d) FROM loans WHERE issue_d IS NOT NULL")
                    min_date, max_date = cursor.fetchone()
                    stats['date_range'] = f"{min_date} to {max_date}"
                except:
                    stats['date_range'] = "Non disponible"
            
            return {
                'tables': tables,
                'views': views,
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des infos: {e}")
            return {}


# Version ULTRA SIMPLIFIÉE pour les tests
def load_to_sqlite_simple(df, db_path='data/loans.db', table_name='loans'):
    """
    Fonction ultra simplifiée pour charger des données
    """
    try:
        logger.info(f"Chargement ultra simplifié dans {db_path}...")
        
        # Préparer le DataFrame
        df_simple = df.copy()
        
        # Limiter aux colonnes numériques et string simples
        numeric_cols = df_simple.select_dtypes(include=[np.number]).columns.tolist()
        string_cols = ['grade', 'term', 'loan_status', 'home_ownership', 'purpose', 'addr_state']
        date_cols = ['issue_d']
        
        # Garder seulement les colonnes qui existent
        cols_to_keep = []
        for col in numeric_cols + string_cols + date_cols:
            if col in df_simple.columns:
                cols_to_keep.append(col)
        
        # Limiter à 15 colonnes max
        cols_to_keep = cols_to_keep[:15]
        df_simple = df_simple[cols_to_keep].copy()
        
        # Convertir les dates
        if 'issue_d' in df_simple.columns:
            df_simple['issue_d'] = pd.to_datetime(df_simple['issue_d'], errors='coerce')
            df_simple['issue_d'] = df_simple['issue_d'].dt.strftime('%Y-%m-%d')
        
        # Connexion SQLite
        conn = sqlite3.connect(db_path)
        
        # Charger avec pandas (qui gère bien les types)
        df_simple.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # Créer un index simple
        cursor = conn.cursor()
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_grade ON {table_name}(grade)")
        conn.commit()
        conn.close()
        
        logger.info(f"✓ Chargement simplifié réussi: {len(df_simple)} lignes")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur chargement simplifié: {e}")
        return False


# Fonctions utilitaires
def load_to_sqlite(df, db_path='data/loans.db', table_name='loans'):
    """Fonction simplifiée pour charger des données"""
    loader = SQLiteDataLoader(db_path)
    success = loader.load_to_sqlite(df, table_name)
    loader.close()
    return success

def create_views(db_path='data/loans.db'):
    """Fonction simplifiée pour créer les vues"""
    loader = SQLiteDataLoader(db_path)
    loader.create_connection()
    loader.create_views()
    loader.close()


if __name__ == "__main__":
    # Test du module
    logging.basicConfig(level=logging.INFO)
    
    # Créer des données de test SIMPLES
    test_data = pd.DataFrame({
        'loan_amnt': [10000, 20000, 15000, 25000, 30000],
        'int_rate': [10.5, 15.2, 12.0, 8.5, 9.0],
        'issue_d': ['2018-12-01', '2019-01-15', '2019-02-20', '2019-03-10', '2019-04-05'],
        'loan_status': ['FULLY PAID', 'CHARGED OFF', 'CURRENT', 'DEFAULT', 'FULLY PAID'],
        'grade': ['A', 'B', 'C', 'D', 'A'],
        'term': ['36 months', '60 months', '36 months', '60 months', '36 months'],
        'annual_inc': [50000, 60000, 30000, 45000, 80000],
        'dti': [15.5, 20.2, 25.0, 18.5, 12.0],
        'home_ownership': ['RENT', 'MORTGAGE', 'RENT', 'OWN', 'MORTGAGE'],
        'purpose': ['debt_consolidation', 'credit_card', 'home_improvement', 'car', 'medical'],
        'is_default': [0, 1, 0, 1, 0],
        'is_fully_paid': [1, 0, 0, 0, 1]
    })
    
    print("Test du chargeur SQLite...")
    
    # Test 1: Version simplifiée
    print("\n1. Test version simplifiée...")
    success = load_to_sqlite_simple(test_data, 'data/test_simple.db', 'test_loans')
    
    if success:
        # Vérifier
        conn = sqlite3.connect('data/test_simple.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_loans")
        count = cursor.fetchone()[0]
        print(f"✓ Chargement réussi: {count} lignes")
        conn.close()
    else:
        print("❌ Échec du chargement simplifié")
    
    # Test 2: Version complète
    print("\n2. Test version complète...")
    loader = SQLiteDataLoader('data/test_full.db')
    
    # Charger les données
    success = loader.load_to_sqlite(test_data, 'test_loans', batch_size=2)
    
    if success:
        # Créer des vues
        loader.create_views()
        
        # Afficher les informations
        info = loader.get_table_info()
        print(f"✓ Tables: {info.get('tables', [])}")
        print(f"✓ Vues: {info.get('views', [])}")
        print(f"✓ Stats: {info.get('stats', {})}")
        
        # Tester une requête
        cursor = loader.conn.cursor()
        cursor.execute("SELECT * FROM dashboard_kpis")
        results = cursor.fetchall()
        print("\nKPIs du dashboard:")
        for row in results:
            print(f"  {row[0]}: {row[1]}")
    
    loader.close()
    print("\nTest terminé!")
