#!/usr/bin/env python3
"""
Script d'initialisation de la base de données
Crée la structure de la base de données et les tables nécessaires
"""

import sqlite3
import os
import sys
from pathlib import Path
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseSetup:
    """Classe pour configurer la base de données"""
    
    def __init__(self, db_path='data/loans.db'):
        """
        Initialise le setup de la base de données
        
        Args:
            db_path: Chemin vers la base de données
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
        
    def create_connection(self):
        """Crée une connexion à la base de données"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Connexion à la base de données établie: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Erreur de connexion: {e}")
            return False
    
    def execute_sql_file(self, sql_file_path):
        """
        Exécute un fichier SQL
        
        Args:
            sql_file_path: Chemin vers le fichier SQL
        """
        try:
            sql_file = Path(sql_file_path)
            if not sql_file.exists():
                logger.error(f"Fichier SQL non trouvé: {sql_file_path}")
                return False
            
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # Diviser le script en commandes individuelles
            commands = sql_script.split(';')
            
            cursor = self.conn.cursor()
            for command in commands:
                command = command.strip()
                if command:
                    try:
                        cursor.execute(command)
                    except Exception as e:
                        logger.warning(f"Erreur lors de l'exécution d'une commande: {e}")
                        logger.debug(f"Commande problématique: {command[:100]}...")
            
            self.conn.commit()
            logger.info(f"Script SQL exécuté: {sql_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution du fichier SQL: {e}")
            return False
    
    def create_tables_from_schema(self, schema_file='sql/schema.sql'):
        """
        Crée les tables à partir du schéma
        
        Args:
            schema_file: Chemin vers le fichier de schéma
        """
        logger.info("Création des tables à partir du schéma...")
        return self.execute_sql_file(schema_file)
    
    def create_sample_data(self, sample_size=1000):
        """
        Crée des données d'exemple pour les tests
        
        Args:
            sample_size: Nombre d'enregistrements à créer
        """
        try:
            logger.info(f"Création de {sample_size} enregistrements d'exemple...")
            
            cursor = self.conn.cursor()
            
            # Créer des données d'exemple pour la table loans
            import random
            from datetime import datetime, timedelta
            
            grades = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
            loan_statuses = ['FULLY PAID', 'CURRENT', 'CHARGED OFF', 'DEFAULT']
            purposes = ['debt_consolidation', 'credit_card', 'home_improvement', 'car', 'medical']
            states = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
            
            for i in range(sample_size):
                issue_date = datetime.now() - timedelta(days=random.randint(1, 365*3))
                grade = random.choice(grades)
                int_rate = random.uniform(5.0, 25.0)
                
                cursor.execute("""
                INSERT INTO loans (
                    loan_amnt, funded_amnt, term, int_rate, grade, sub_grade,
                    emp_title, emp_length, home_ownership, annual_inc,
                    verification_status, issue_d, loan_status, purpose,
                    addr_state, dti, delinq_2yrs, earliest_cr_line,
                    inq_last_6mths, open_acc, pub_rec, revol_bal, revol_util,
                    total_acc, initial_list_status, is_default, is_fully_paid,
                    income_category, issue_year, issue_month, issue_quarter,
                    issue_season, int_rate_category
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    random.randint(1000, 35000),  # loan_amnt
                    random.randint(1000, 35000),  # funded_amnt
                    random.choice(['36 months', '60 months']),  # term
                    int_rate,  # int_rate
                    grade,  # grade
                    f"{grade}{random.randint(1,5)}",  # sub_grade
                    random.choice(['Software Engineer', 'Teacher', 'Nurse', 'Manager', 'Sales Representative']),
                    random.choice(['< 1 year', '1 year', '2 years', '3 years', '4 years', '5 years', '6 years', '7 years', '8 years', '9 years', '10+ years']),
                    random.choice(['RENT', 'MORTGAGE', 'OWN', 'OTHER']),
                    random.randint(30000, 150000),  # annual_inc
                    random.choice(['Verified', 'Source Verified', 'Not Verified']),
                    issue_date.strftime('%Y-%m-%d'),  # issue_d
                    random.choice(loan_statuses),  # loan_status
                    random.choice(purposes),  # purpose
                    random.choice(states),  # addr_state
                    random.uniform(0, 40),  # dti
                    random.randint(0, 3),  # delinq_2yrs
                    (issue_date - timedelta(days=random.randint(365, 365*20))).strftime('%Y-%m-%d'),  # earliest_cr_line
                    random.randint(0, 5),  # inq_last_6mths
                    random.randint(1, 20),  # open_acc
                    random.randint(0, 2),  # pub_rec
                    random.randint(0, 50000),  # revol_bal
                    random.uniform(0, 100),  # revol_util
                    random.randint(1, 30),  # total_acc
                    random.choice(['f', 'w']),  # initial_list_status
                    random.choice([0, 1]),  # is_default
                    random.choice([0, 1]),  # is_fully_paid
                    random.choice(['Très faible', 'Faible', 'Moyen', 'Élevé', 'Très élevé']),  # income_category
                    issue_date.year,  # issue_year
                    issue_date.month,  # issue_month
                    (issue_date.month - 1) // 3 + 1,  # issue_quarter
                    random.choice(['Hiver', 'Printemps', 'Été', 'Automne']),  # issue_season
                    random.choice(['0-5%', '5-10%', '10-15%', '15-20%', '20-30%', '30%+'])  # int_rate_category
                ))
            
            self.conn.commit()
            logger.info(f"{sample_size} enregistrements d'exemple créés")
            
            # Créer des données pour les autres tables
            self._create_sample_kpi_data()
            self._create_sample_alert_data()
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la création des données d'exemple: {e}")
            return False
    
    def _create_sample_kpi_data(self):
        """Crée des données d'exemple pour les KPIs historiques"""
        try:
            cursor = self.conn.cursor()
            
            # Créer des KPIs historiques pour les 30 derniers jours
            from datetime import datetime, timedelta
            
            kpi_names = ['Portfolio Total', 'Montant Total', 'Taux Défaut', 
                        'Taux Intérêt Moyen', 'Revenu Moyen', 'Taux Remboursement']
            
            for i in range(30):
                calculation_date = datetime.now() - timedelta(days=i)
                
                for kpi_name in kpi_names:
                    # Valeurs aléatoires réalistes
                    if kpi_name == 'Portfolio Total':
                        value = random.randint(1000, 5000)
                    elif kpi_name == 'Montant Total':
                        value = random.randint(10000000, 50000000)
                    elif kpi_name == 'Taux Défaut':
                        value = random.uniform(1.0, 10.0)
                    elif kpi_name == 'Taux Intérêt Moyen':
                        value = random.uniform(8.0, 15.0)
                    elif kpi_name == 'Revenu Moyen':
                        value = random.randint(50000, 80000)
                    elif kpi_name == 'Taux Remboursement':
                        value = random.uniform(70.0, 95.0)
                    
                    cursor.execute("""
                    INSERT OR IGNORE INTO historical_kpis 
                    (calculation_date, kpi_name, kpi_value, kpi_description, period)
                    VALUES (?, ?, ?, ?, ?)
                    """, (
                        calculation_date.strftime('%Y-%m-%d'),
                        kpi_name,
                        value,
                        f"Valeur du {kpi_name}",
                        'daily'
                    ))
            
            self.conn.commit()
            logger.info("Données KPI historiques créées")
            
        except Exception as e:
            logger.warning(f"Impossible de créer les données KPI: {e}")
    
    def _create_sample_alert_data(self):
        """Crée des données d'exemple pour les alertes"""
        try:
            cursor = self.conn.cursor()
            
            alert_types = ['RISK', 'PERFORMANCE', 'DATA_QUALITY', 'COMPLIANCE']
            alert_levels = ['info', 'warning', 'critical']
            
            for i in range(10):
                alert_date = datetime.now() - timedelta(days=random.randint(0, 7))
                
                cursor.execute("""
                INSERT INTO alerts 
                (alert_type, alert_level, alert_message, related_table, related_id, triggered_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    random.choice(alert_types),
                    random.choice(alert_levels),
                    f"Message d'alerte exemple #{i+1}",
                    'loans',
                    random.randint(1, 1000),
                    alert_date.strftime('%Y-%m-%d %H:%M:%S')
                ))
            
            self.conn.commit()
            logger.info("Données d'alerte créées")
            
        except Exception as e:
            logger.warning(f"Impossible de créer les données d'alerte: {e}")
    
    def verify_database_structure(self):
        """Vérifie la structure de la base de données"""
        try:
            cursor = self.conn.cursor()
            
            # Récupérer la liste des tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row['name'] for row in cursor.fetchall()]
            
            logger.info(f"Tables dans la base de données: {tables}")
            
            # Vérifier chaque table
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                
                logger.info(f"\nTable: {table}")
                logger.info(f"Nombre de colonnes: {len(columns)}")
                for col in columns:
                    logger.info(f"  - {col['name']} ({col['type']})")
            
            # Compter les enregistrements
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()['count']
                logger.info(f"\n{table}: {count} enregistrements")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la vérification: {e}")
            return False
    
    def run_sql_queries(self, queries_dir='sql/queries'):
        """
        Exécute toutes les requêtes SQL de test
        
        Args:
            queries_dir: Répertoire contenant les fichiers de requêtes
        """
        queries_path = Path(queries_dir)
        if not queries_path.exists():
            logger.warning(f"Répertoire des requêtes non trouvé: {queries_dir}")
            return
        
        # Lister tous les fichiers SQL
        sql_files = list(queries_path.glob('*.sql'))
        
        for sql_file in sql_files:
            logger.info(f"\nExécution de la requête: {sql_file.name}")
            try:
                self.execute_sql_file(sql_file)
                
                # Pour les requêtes SELECT, afficher un aperçu
                if 'SELECT' in open(sql_file).read().upper():
                    cursor = self.conn.cursor()
                    
                    # Lire la première requête SELECT
                    with open(sql_file, 'r') as f:
                        sql_content = f.read()
                    
                    # Trouver la première requête SELECT
                    lines = sql_content.split('\n')
                    select_query = ''
                    in_select = False
                    
                    for line in lines:
                        if 'SELECT' in line.upper() and not in_select:
                            in_select = True
                        if in_select:
                            select_query += line + ' '
                            if ';' in line:
                                break
                    
                    if select_query:
                        # Exécuter et afficher les premières lignes
                        cursor.execute(select_query.replace(';', ''))
                        rows = cursor.fetchmany(5)
                        
                        if rows:
                            logger.info(f"Aperçu des résultats ({len(rows)} lignes):")
                            for row in rows:
                                logger.info(f"  {row}")
            
            except Exception as e:
                logger.warning(f"Erreur avec {sql_file.name}: {e}")
    
    def backup_database(self, backup_dir='backups'):
        """
        Crée une sauvegarde de la base de données
        
        Args:
            backup_dir: Répertoire de sauvegarde
        """
        try:
            backup_path = Path(backup_dir)
            backup_path.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = backup_path / f"loans_backup_{timestamp}.db"
            
            # Fermer la connexion actuelle pour permettre la copie
            if self.conn:
                self.conn.close()
            
            # Copier le fichier de base de données
            import shutil
            shutil.copy2(self.db_path, backup_file)
            
            logger.info(f"Sauvegarde créée: {backup_file}")
            
            # Rétablir la connexion
            self.create_connection()
            
            return backup_file
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde: {e}")
            return None
    
    def run_complete_setup(self, create_sample_data=False, sample_size=1000):
        """
        Exécute la configuration complète de la base de données
        
        Args:
            create_sample_data: Créer des données d'exemple
            sample_size: Taille de l'échantillon si création de données
        """
        logger.info("=" * 60)
        logger.info("DÉMARRAGE DE LA CONFIGURATION DE LA BASE DE DONNÉES")
        logger.info("=" * 60)
        
        # 1. Créer la connexion
        if not self.create_connection():
            return False
        
        # 2. Sauvegarde si la base existe déjà
        if self.db_path.exists():
            logger.info("Base de données existante détectée, création d'une sauvegarde...")
            self.backup_database()
        
        # 3. Créer les tables
        logger.info("\nÉtape 1: Création des tables...")
        if not self.create_tables_from_schema():
            logger.error("Échec de la création des tables")
            return False
        
        # 4. Créer des données d'exemple si demandé
        if create_sample_data:
            logger.info("\nÉtape 2: Création de données d'exemple...")
            if not self.create_sample_data(sample_size):
                logger.warning("Échec de la création des données d'exemple")
        
        # 5. Vérifier la structure
        logger.info("\nÉtape 3: Vérification de la structure...")
        self.verify_database_structure()
        
        # 6. Exécuter les requêtes de test
        logger.info("\nÉtape 4: Exécution des requêtes de test...")
        self.run_sql_queries()
        
        logger.info("\n" + "=" * 60)
        logger.info("CONFIGURATION TERMINÉE AVEC SUCCÈS")
        logger.info("=" * 60)
        
        return True


def main():
    """Fonction principale"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Configure la base de données pour l\'analyse de prêts')
    parser.add_argument('--db-path', type=str, default='data/loans.db', 
                       help='Chemin de la base de données')
    parser.add_argument('--create-sample', action='store_true',
                       help='Crée des données d\'exemple')
    parser.add_argument('--sample-size', type=int, default=1000,
                       help='Taille de l\'échantillon de données d\'exemple')
    parser.add_argument('--backup-first', action='store_true',
                       help='Crée une sauvegarde avant toute modification')
    parser.add_argument('--verify-only', action='store_true',
                       help='Vérifie seulement la structure sans modifier')
    
    args = parser.parse_args()
    
    # Initialiser le setup
    setup = DatabaseSetup(args.db_path)
    setup.create_connection()
    
    if args.verify_only:
        # Mode vérification seulement
        setup.verify_database_structure()
    else:
        # Mode configuration complète
        if args.backup_first:
            setup.backup_database()
        
        setup.run_complete_setup(
            create_sample_data=args.create_sample,
            sample_size=args.sample_size
        )
    
    # Fermer la connexion
    if setup.conn:
        setup.conn.close()
        logger.info("Connexion à la base de données fermée")


if __name__ == "__main__":
    # Importer random pour les données d'exemple
    import random
    main()
