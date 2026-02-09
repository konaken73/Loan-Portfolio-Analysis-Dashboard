"""
etl/load_ultra_simple.py
Version ULTRA SIMPLE du chargeur SQLite
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path
import numpy as np

logger = logging.getLogger(__name__)

def load_to_sqlite_ultra_simple(df, db_path='data/loans.db', table_name='loans'):
    """
    Chargeur ULTRA SIMPLE - Utilise pandas.to_sql qui gère bien les types
    """
    try:
        logger.info(f"Chargement ultra simple dans {db_path}...")
        
        # Créer le dossier si besoin
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Sélectionner seulement quelques colonnes pour éviter les problèmes
        if len(df.columns) > 20:
            logger.info(f"Trop de colonnes ({len(df.columns)}), limitation à 20")
            # Garder les colonnes les plus importantes
            priority_cols = [
                'loan_amnt', 'int_rate', 'term', 'grade', 'issue_d',
                'loan_status', 'annual_inc', 'dti', 'home_ownership',
                'emp_length', 'purpose', 'addr_state', 'delinq_2yrs',
                'revol_util', 'total_pymnt', 'is_default', 'is_fully_paid'
            ]
            
            # Garder les colonnes prioritaires qui existent
            cols_to_keep = []
            for col in priority_cols:
                if col in df.columns:
                    cols_to_keep.append(col)
            
            # Ajouter d'autres colonnes si nécessaire
            if len(cols_to_keep) < 15:
                other_cols = [c for c in df.columns if c not in cols_to_keep]
                cols_to_keep.extend(other_cols[:20 - len(cols_to_keep)])
            
            df = df[cols_to_keep].copy()
        
        # Connexion SQLite
        conn = sqlite3.connect(str(db_path))
        
        # Charger avec pandas (qui convertit automatiquement les types)
        # Utiliser if_exists='replace' pour écraser la table existante
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists='replace',
            index=False,
            chunksize=1000  # Chunks pour éviter les problèmes de mémoire
        )
        
        # Créer quelques index utiles
        cursor = conn.cursor()
        
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_grade ON {table_name}(grade)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_loan_status ON {table_name}(loan_status)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_is_default ON {table_name}(is_default)",
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except:
                pass  # Ignorer les erreurs d'index
        
        conn.commit()
        conn.close()
        
        logger.info(f"✓ Chargement ultra simple réussi: {len(df)} lignes dans {db_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur chargement ultra simple: {e}")
        return False

def create_simple_views(db_path='data/loans.db'):
    """Crée des vues simples"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Vue dashboard simple
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS dashboard_kpis AS
        SELECT 
            'Total Loans' as metric,
            COUNT(*) as value
        FROM loans
        UNION ALL
        SELECT 
            'Total Amount',
            SUM(loan_amnt)
        FROM loans
        UNION ALL
        SELECT 
            'Default Rate',
            ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM loans
        UNION ALL
        SELECT 
            'Avg Interest Rate',
            ROUND(AVG(int_rate), 2)
        FROM loans
        """)
        
        # Vue par grade
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS grade_summary AS
        SELECT 
            grade,
            COUNT(*) as count,
            ROUND(AVG(int_rate), 2) as avg_rate,
            ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate
        FROM loans
        WHERE grade IS NOT NULL
        GROUP BY grade
        ORDER BY grade
        """)
        
        conn.commit()
        conn.close()
        
        logger.info("✓ Vues simples créées")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur création vues: {e}")
        return False

# Fonction principale pour le pipeline
def run_simple_load(df, db_path='data/loans.db'):
    """Fonction principale pour le pipeline"""
    success = load_to_sqlite_ultra_simple(df, db_path)
    if success:
        create_simple_views(db_path)
    return success
