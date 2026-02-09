#!/usr/bin/env python3
"""
Script d'export des données pour les outils BI (Business Intelligence)
Exporte les données dans des formats compatibles avec Tableau, Power BI, Looker Studio, etc.
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json
from datetime import datetime
import csv
import sys

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BIExporter:
    """Classe pour exporter les données vers des formats BI"""
    
    def __init__(self, db_path='data/loans.db', output_dir='data/exports'):
        """
        Initialise l'exportateur
        
        Args:
            db_path: Chemin vers la base de données SQLite
            output_dir: Répertoire de sortie pour les exports
        """
        self.db_path = Path(db_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.conn = None
        
    def create_connection(self):
        """Crée une connexion à la base de données"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logger.info(f"Connexion à la base de données établie: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Erreur de connexion: {e}")
            return False
    
    def export_table_to_csv(self, table_name, output_file=None):
        """
        Exporte une table complète vers CSV
        
        Args:
            table_name: Nom de la table à exporter
            output_file: Fichier de sortie (optionnel)
        """
        try:
            if output_file is None:
                output_file = self.output_dir / f"{table_name}.csv"
            
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, self.conn)
            
            output_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.info(f"Table '{table_name}' exportée: {len(df)} lignes -> {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Erreur lors de l'export de {table_name}: {e}")
            return None
    
    def export_view_to_csv(self, view_name, output_file=None):
        """
        Exporte une vue vers CSV
        
        Args:
            view_name: Nom de la vue à exporter
            output_file: Fichier de sortie (optionnel)
        """
        return self.export_table_to_csv(view_name, output_file)
    
    def export_custom_query(self, query, output_file, description=""):
        """
        Exporte le résultat d'une requête personnalisée
        
        Args:
            query: Requête SQL à exécuter
            output_file: Fichier de sortie
            description: Description de l'export
        """
        try:
            df = pd.read_sql_query(query, self.conn)
            
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False, encoding='utf-8')
            
            logger.info(f"Requête exportée: {len(df)} lignes -> {output_file}")
            if description:
                logger.info(f"Description: {description}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Erreur lors de l'export de la requête: {e}")
            return None
    
    def export_for_looker_studio(self):
        """
        Exporte les données spécifiquement formatées pour Google Looker Studio
        """
        logger.info("Export des données pour Google Looker Studio...")
        
        exports = []
        
        # 1. KPIs principaux pour le dashboard
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                metric as KPI,
                value as Valeur,
                description as Description,
                CASE 
                    WHEN value_type = 'percentage' THEN '%'
                    WHEN value_type = 'currency' THEN 'USD'
                    WHEN value_type = 'count' THEN 'unités'
                    ELSE ''
                END as Unité
            FROM dashboard_kpis
            """,
            output_file=self.output_dir / "looker_studio" / "dashboard_kpis.csv",
            description="KPIs principaux pour le tableau de bord"
        ))
        
        # 2. Performance par grade
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                grade as Grade,
                COUNT(*) as Nombre_Prêts,
                ROUND(SUM(loan_amnt), 0) as Montant_Total,
                ROUND(AVG(int_rate), 2) as Taux_Intérêt_Moyen,
                SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as Défauts,
                ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Taux_Défaut,
                ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2) as Taux_Récupération
            FROM loans
            WHERE grade IS NOT NULL
            GROUP BY grade
            ORDER BY grade
            """,
            output_file=self.output_dir / "looker_studio" / "performance_par_grade.csv",
            description="Performance par grade de crédit"
        ))
        
        # 3. Tendances temporelles
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                issue_year as Année,
                issue_month as Mois,
                issue_year || '-' || PRINTF('%02d', issue_month) as Période,
                COUNT(*) as Prêts_Émis,
                ROUND(SUM(loan_amnt), 0) as Montant_Total,
                ROUND(AVG(int_rate), 2) as Taux_Intérêt_Moyen,
                SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as Défauts,
                ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Taux_Défaut
            FROM loans
            WHERE issue_year IS NOT NULL
            GROUP BY issue_year, issue_month
            ORDER BY issue_year, issue_month
            """,
            output_file=self.output_dir / "looker_studio" / "tendances_temporelles.csv",
            description="Tendances mensuelles des émissions et défauts"
        ))
        
        # 4. Segmentation géographique
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                addr_state as État,
                COUNT(*) as Nombre_Prêts,
                ROUND(SUM(loan_amnt), 0) as Montant_Total,
                ROUND(AVG(int_rate), 2) as Taux_Intérêt_Moyen,
                SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as Défauts,
                ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Taux_Défaut,
                ROUND(AVG(annual_inc), 0) as Revenu_Moyen
            FROM loans
            WHERE addr_state IS NOT NULL AND addr_state != ''
            GROUP BY addr_state
            HAVING COUNT(*) >= 10
            ORDER BY Montant_Total DESC
            """,
            output_file=self.output_dir / "looker_studio" / "segmentation_géographique.csv",
            description="Performance par état"
        ))
        
        # 5. Analyse des risques
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                risk_category as Catégorie_Risque,
                COUNT(*) as Nombre_Prêts,
                ROUND(SUM(loan_amnt), 0) as Exposition_Totale,
                ROUND(AVG(int_rate), 2) as Taux_Intérêt_Moyen,
                SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as Défauts_Réels,
                ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Taux_Défaut_Réel
            FROM loans
            WHERE risk_category IS NOT NULL
            GROUP BY risk_category
            ORDER BY 
                CASE risk_category
                    WHEN 'Faible risque' THEN 1
                    WHEN 'Risque modéré' THEN 2
                    WHEN 'Risque moyen' THEN 3
                    WHEN 'Risque élevé' THEN 4
                    WHEN 'Risque très élevé' THEN 5
                    WHEN 'Risque extrême' THEN 6
                    ELSE 7
                END
            """,
            output_file=self.output_dir / "looker_studio" / "analyse_risques.csv",
            description="Analyse des risques par catégorie"
        ))
        
        # 6. Métadonnées pour Looker Studio
        self._create_looker_metadata()
        
        logger.info(f"Export Looker Studio terminé: {len([e for e in exports if e])} fichiers créés")
        return exports
    
    def export_for_power_bi(self):
        """
        Exporte les données spécifiquement formatées pour Microsoft Power BI
        """
        logger.info("Export des données pour Microsoft Power BI...")
        
        exports = []
        
        # Power BI préfère souvent un modèle étoile avec une table de faits et des dimensions
        # 1. Table de faits (transactions de prêt)
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                id as LoanID,
                loan_amnt as LoanAmount,
                funded_amnt as FundedAmount,
                int_rate as InterestRate,
                installment as MonthlyPayment,
                grade as Grade,
                sub_grade as SubGrade,
                annual_inc as AnnualIncome,
                dti as DebtToIncome,
                issue_d as IssueDate,
                loan_status as LoanStatus,
                purpose as Purpose,
                addr_state as State,
                is_default as IsDefault,
                is_fully_paid as IsFullyPaid,
                total_pymnt as TotalPayment,
                total_rec_int as TotalInterest,
                out_prncp as OutstandingPrincipal
            FROM loans
            WHERE loan_amnt IS NOT NULL
            """,
            output_file=self.output_dir / "power_bi" / "fact_loans.csv",
            description="Table de faits des prêts"
        ))
        
        # 2. Dimension Temps
        exports.append(self.export_custom_query(
            query="""
            SELECT DISTINCT
                issue_d as Date,
                issue_year as Year,
                issue_month as Month,
                issue_quarter as Quarter,
                issue_season as Season
            FROM loans
            WHERE issue_d IS NOT NULL
            ORDER BY Date
            """,
            output_file=self.output_dir / "power_bi" / "dim_time.csv",
            description="Dimension Temps"
        ))
        
        # 3. Dimension Géographie
        exports.append(self.export_custom_query(
            query="""
            SELECT DISTINCT
                addr_state as StateCode,
                addr_state as StateName,
                COUNT(*) as TotalLoans,
                ROUND(AVG(annual_inc), 0) as AvgIncome
            FROM loans
            WHERE addr_state IS NOT NULL AND addr_state != ''
            GROUP BY addr_state
            """,
            output_file=self.output_dir / "power_bi" / "dim_geography.csv",
            description="Dimension Géographie"
        ))
        
        # 4. Dimension Produit (Grade/Purpose)
        exports.append(self.export_custom_query(
            query="""
            SELECT DISTINCT
                grade as Grade,
                sub_grade as SubGrade,
                purpose as Purpose,
                term as Term,
                COUNT(*) as LoanCount
            FROM loans
            WHERE grade IS NOT NULL
            GROUP BY grade, sub_grade, purpose, term
            """,
            output_file=self.output_dir / "power_bi" / "dim_product.csv",
            description="Dimension Produit"
        ))
        
        # 5. Dimension Client
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                ROW_NUMBER() OVER (ORDER BY emp_title, home_ownership) as CustomerID,
                emp_title as Employment,
                home_ownership as HomeOwnership,
                income_category as IncomeCategory,
                verification_status as VerificationStatus,
                COUNT(*) as CustomerCount
            FROM loans
            WHERE emp_title IS NOT NULL
            GROUP BY emp_title, home_ownership, income_category, verification_status
            """,
            output_file=self.output_dir / "power_bi" / "dim_customer.csv",
            description="Dimension Client"
        ))
        
        # 6. Relations pour Power BI (format JSON)
        self._create_power_bi_relationships()
        
        logger.info(f"Export Power BI terminé: {len([e for e in exports if e])} fichiers créés")
        return exports
    
    def export_for_tableau(self):
        """
        Exporte les données spécifiquement formatées pour Tableau
        """
        logger.info("Export des données pour Tableau...")
        
        exports = []
        
        # Tableau fonctionne bien avec des données dénormalisées
        # 1. Vue détaillée des prêts
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                l.*,
                t.issue_year,
                t.issue_month,
                t.issue_quarter,
                t.issue_season,
                g.state_name,
                g.avg_income as state_avg_income,
                p.loan_count as product_count
            FROM loans l
            LEFT JOIN (
                SELECT DISTINCT
                    issue_d,
                    issue_year,
                    issue_month,
                    issue_quarter,
                    issue_season
                FROM loans
            ) t ON l.issue_d = t.issue_d
            LEFT JOIN (
                SELECT 
                    addr_state,
                    addr_state as state_name,
                    ROUND(AVG(annual_inc), 0) as avg_income
                FROM loans
                GROUP BY addr_state
            ) g ON l.addr_state = g.addr_state
            LEFT JOIN (
                SELECT 
                    grade,
                    sub_grade,
                    purpose,
                    COUNT(*) as loan_count
                FROM loans
                GROUP BY grade, sub_grade, purpose
            ) p ON l.grade = p.grade AND l.sub_grade = p.sub_grade AND l.purpose = p.purpose
            """,
            output_file=self.output_dir / "tableau" / "loans_denormalized.csv",
            description="Vue détaillée dénormalisée pour Tableau"
        ))
        
        # 2. Vue agrégée pour les dashboards
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                grade,
                sub_grade,
                purpose,
                addr_state,
                issue_year,
                issue_quarter,
                income_category,
                COUNT(*) as loan_count,
                ROUND(SUM(loan_amnt), 0) as total_amount,
                ROUND(AVG(int_rate), 2) as avg_interest_rate,
                SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as default_count,
                ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate,
                ROUND(SUM(total_pymnt), 0) as total_payments,
                ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2) as recovery_rate
            FROM loans
            GROUP BY grade, sub_grade, purpose, addr_state, issue_year, issue_quarter, income_category
            HAVING COUNT(*) >= 1
            """,
            output_file=self.output_dir / "tableau" / "loans_aggregated.csv",
            description="Vue agrégée pour Tableau"
        ))
        
        # 3. Données pour les cartes (géographiques)
        exports.append(self.export_custom_query(
            query="""
            SELECT 
                addr_state as State,
                COUNT(*) as LoanCount,
                ROUND(SUM(loan_amnt), 0) as TotalAmount,
                ROUND(AVG(int_rate), 2) as AvgInterestRate,
                SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as DefaultCount,
                ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as DefaultRate
            FROM loans
            WHERE addr_state IS NOT NULL AND addr_state != ''
            GROUP BY addr_state
            ORDER BY State
            """,
            output_file=self.output_dir / "tableau" / "geographic_data.csv",
            description="Données géographiques pour Tableau"
        ))
        
        # 4. Fichier TDS (Tableau Data Source) - metadata
        self._create_tableau_tds_file()
        
        logger.info(f"Export Tableau terminé: {len([e for e in exports if e])} fichiers créés")
        return exports
    
    def export_for_metabase(self):
        """
        Exporte les données spécifiquement formatées pour Metabase
        """
        logger.info("Export des données pour Metabase...")
        
        exports = []
        
        # Metabase peut se connecter directement à SQLite, mais on exporte quand même des vues utiles
        
        # 1. Questions fréquentes prédéfinies
        exports.append(self.export_custom_query(
            query="SELECT * FROM dashboard_kpis",
            output_file=self.output_dir / "metabase" / "dashboard_kpis.csv",
            description="KPIs pour le dashboard Metabase"
        ))
        
        exports.append(self.export_custom_query(
            query="SELECT * FROM loan_default_analysis",
            output_file=self.output_dir / "metabase" / "default_analysis.csv",
            description="Analyse des défauts"
        ))
        
        exports.append(self.export_custom_query(
            query="SELECT * FROM monthly_performance",
            output_file=self.output_dir / "metabase" / "monthly_performance.csv",
            description="Performance mensuelle"
        ))
        
        exports.append(self.export_custom_query(
            query="SELECT * FROM borrower_segmentation",
            output_file=self.output_dir / "metabase" / "borrower_segmentation.csv",
            description="Segmentation des emprunteurs"
        ))
        
        # 2. Créer un fichier de configuration Metabase
        self._create_metabase_config()
        
        logger.info(f"Export Metabase terminé: {len([e for e in exports if e])} fichiers créés")
        return exports
    
    def export_all_formats(self):
        """Exporte dans tous les formats"""
        logger.info("Export dans tous les formats...")
        
        all_exports = []
        
        # Créer les répertoires
        (self.output_dir / "looker_studio").mkdir(exist_ok=True)
        (self.output_dir / "power_bi").mkdir(exist_ok=True)
        (self.output_dir / "tableau").mkdir(exist_ok=True)
        (self.output_dir / "metabase").mkdir(exist_ok=True)
        (self.output_dir / "excel").mkdir(exist_ok=True)
        (self.output_dir / "json").mkdir(exist_ok=True)
        
        # Exporter dans chaque format
        all_exports.extend(self.export_for_looker_studio())
        all_exports.extend(self.export_for_power_bi())
        all_exports.extend(self.export_for_tableau())
        all_exports.extend(self.export_for_metabase())
        
        # Export Excel
        all_exports.extend(self._export_to_excel())
        
        # Export JSON
        all_exports.extend(self._export_to_json())
        
        # Créer un rapport d'export
        self._create_export_report(all_exports)
        
        logger.info(f"Export complet terminé: {len([e for e in all_exports if e])} fichiers créés")
        return all_exports
    
    def _export_to_excel(self):
        """Exporte vers Excel"""
        try:
            # Créer un classeur Excel avec plusieurs onglets
            output_file = self.output_dir / "excel" / "loan_portfolio_analysis.xlsx"
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Onglet 1: Résumé
                df_summary = pd.read_sql_query("SELECT * FROM dashboard_kpis", self.conn)
                df_summary.to_excel(writer, sheet_name='Résumé', index=False)
                
                # Onglet 2: Détails des prêts (échantillon)
                df_loans = pd.read_sql_query("SELECT * FROM loans LIMIT 10000", self.conn)
                df_loans.to_excel(writer, sheet_name='Prêts', index=False)
                
                # Onglet 3: Analyse des risques
                df_risk = pd.read_sql_query("SELECT * FROM loan_default_analysis", self.conn)
                df_risk.to_excel(writer, sheet_name='Risques', index=False)
                
                # Onglet 4: Performance
                df_perf = pd.read_sql_query("SELECT * FROM monthly_performance", self.conn)
                df_perf.to_excel(writer, sheet_name='Performance', index=False)
                
                # Onglet 5: Métadonnées
                metadata = {
                    'Date_Export': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                    'Source_DB': [str(self.db_path)],
                    'Total_Prêts': [len(df_loans)],
                    'Fichier_Excel': [str(output_file)]
                }
                df_meta = pd.DataFrame(metadata)
                df_meta.to_excel(writer, sheet_name='Métadonnées', index=False)
            
            logger.info(f"Fichier Excel créé: {output_file}")
            return [output_file]
            
        except Exception as e:
            logger.error(f"Erreur lors de l'export Excel: {e}")
            return []
    
    def _export_to_json(self):
        """Exporte vers JSON"""
        try:
            # 1. Export des KPIs en JSON
            df_kpis = pd.read_sql_query("SELECT * FROM dashboard_kpis", self.conn)
            json_file = self.output_dir / "json" / "kpis.json"
            df_kpis.to_json(json_file, orient='records', indent=2)
            
            # 2. Export des métadonnées
            metadata = {
                "export_date": datetime.now().isoformat(),
                "database": str(self.db_path),
                "tables": self._get_table_list(),
                "record_counts": self._get_record_counts()
            }
            
            meta_file = self.output_dir / "json" / "metadata.json"
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Fichiers JSON créés dans {self.output_dir / 'json'}")
            return [json_file, meta_file]
            
        except Exception as e:
            logger.error(f"Erreur lors de l'export JSON: {e}")
            return []
    
    def _get_table_list(self):
        """Retourne la liste des tables"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cursor.fetchall()]
        except:
            return []
    
    def _get_record_counts(self):
        """Retourne le nombre d'enregistrements par table"""
        counts = {}
        try:
            cursor = self.conn.cursor()
            tables = self._get_table_list()
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                counts[table] = cursor.fetchone()[0]
            
            return counts
        except:
            return {}
    
    def _create_looker_metadata(self):
        """Crée des métadonnées pour Looker Studio"""
        try:
            metadata = {
                "data_sources": [
                    {
                        "name": "Loan Portfolio Analysis",
                        "description": "Analyse du portefeuille de prêts Lending Club",
                        "tables": [
                            {
                                "name": "dashboard_kpis",
                                "description": "Indicateurs clés de performance",
                                "fields": self._get_table_schema("dashboard_kpis")
                            },
                            {
                                "name": "performance_par_grade",
                                "description": "Performance par grade de crédit",
                                "fields": self._get_table_schema_by_query("""
                                    SELECT grade as Grade, COUNT(*) as Nombre_Prêts 
                                    FROM loans GROUP BY grade
                                """)
                            }
                        ]
                    }
                ],
                "export_date": datetime.now().isoformat()
            }
            
            meta_file = self.output_dir / "looker_studio" / "metadata.json"
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Métadonnées Looker Studio créées: {meta_file}")
            
        except Exception as e:
            logger.warning(f"Impossible de créer les métadonnées Looker Studio: {e}")
    
    def _create_power_bi_relationships(self):
        """Crée les relations pour Power BI"""
        try:
            relationships = {
                "relationships": [
                    {
                        "fromTable": "fact_loans",
                        "fromColumn": "IssueDate",
                        "toTable": "dim_time",
                        "toColumn": "Date"
                    },
                    {
                        "fromTable": "fact_loans",
                        "fromColumn": "State",
                        "toTable": "dim_geography",
                        "toColumn": "StateCode"
                    },
                    {
                        "fromTable": "fact_loans",
                        "fromColumn": "Grade",
                        "toTable": "dim_product",
                        "toColumn": "Grade"
                    }
                ]
            }
            
            rel_file = self.output_dir / "power_bi" / "relationships.json"
            with open(rel_file, 'w', encoding='utf-8') as f:
                json.dump(relationships, f, indent=2)
            
            logger.info(f"Relations Power BI créées: {rel_file}")
            
        except Exception as e:
            logger.warning(f"Impossible de créer les relations Power BI: {e}")
    
    def _create_tableau_tds_file(self):
        """Crée un fichier TDS (Tableau Data Source)"""
        try:
            tds_content = f"""<?xml version='1.0' encoding='utf-8' ?>
<datasource formatted-name='Loan Portfolio Analysis' inline='true' version='18.1'>
  <connection class='sqlite'>
    <named-connections>
      <named-connection name='LoanDB' caption='Loan Database'>
        <connection>
          <dbname>{self.db_path}</dbname>
        </connection>
      </named-connection>
    </named-connections>
  </connection>
  <metadata-records>
    <metadata-record class='column'>
      <remote-name>loan_amnt</remote-name>
      <remote-type>real</remote-type>
      <local-name>[Loan Amount]</local-name>
      <parent-name>[loans]</parent-name>
    </metadata-record>
    <!-- Additional metadata records would go here -->
  </metadata-records>
</datasource>"""
            
            tds_file = self.output_dir / "tableau" / "loan_analysis.tds"
            with open(tds_file, 'w', encoding='utf-8') as f:
                f.write(tds_content)
            
            logger.info(f"Fichier TDS Tableau créé: {tds_file}")
            
        except Exception as e:
            logger.warning(f"Impossible de créer le fichier TDS Tableau: {e}")
    
    def _create_metabase_config(self):
        """Crée une configuration pour Metabase"""
        try:
            config = {
                "name": "Loan Portfolio Dashboard",
                "description": "Dashboard d'analyse du portefeuille de prêts",
                "database": str(self.db_path),
                "questions": [
                    {
                        "name": "Portfolio Overview",
                        "query": "SELECT * FROM dashboard_kpis"
                    },
                    {
                        "name": "Default Analysis",
                        "query": "SELECT * FROM loan_default_analysis"
                    }
                ]
            }
            
            config_file = self.output_dir / "metabase" / "config.json"
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Configuration Metabase créée: {config_file}")
            
        except Exception as e:
            logger.warning(f"Impossible de créer la configuration Metabase: {e}")
    
    def _get_table_schema(self, table_name):
        """Retourne le schéma d'une table"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            return [
                {
                    "name": col[1],
                    "type": col[2],
                    "nullable": col[3] == 0
                }
                for col in columns
            ]
        except:
            return []
    
    def _get_table_schema_by_query(self, query):
        """Retourne le schéma basé sur une requête"""
        try:
            # Exécuter la requête pour obtenir un échantillon
            df = pd.read_sql_query(query + " LIMIT 1", self.conn)
            
            return [
                {
                    "name": col,
                    "type": str(df[col].dtype)
                }
                for col in df.columns
            ]
        except:
            return []
    
    def _create_export_report(self, all_exports):
        """Crée un rapport d'export"""
        try:
            report = {
                "export_date": datetime.now().isoformat(),
                "database": str(self.db_path),
                "total_exports": len([e for e in all_exports if e]),
                "exports_by_format": {},
                "files": []
            }
            
            # Organiser par format
            for export_file in all_exports:
                if export_file:
                    export_path = Path(export_file)
                    format_name = export_path.parent.name
                    
                    if format_name not in report["exports_by_format"]:
                        report["exports_by_format"][format_name] = 0
                    report["exports_by_format"][format_name] += 1
                    
                    report["files"].append({
                        "path": str(export_path),
                        "format": format_name,
                        "size": export_path.stat().st_size if export_path.exists() else 0
                    })
            
            # Écrire le rapport
            report_file = self.output_dir / "export_report.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            # Écrire également en CSV pour plus de lisibilité
            report_csv = self.output_dir / "export_report.csv"
            with open(report_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Format', 'File Count', 'Example Files'])
                
                for format_name, count in report["exports_by_format"].items():
                    example_files = [
                        f["path"] for f in report["files"] 
                        if f["format"] == format_name
                    ][:3]  # Juste 3 exemples
                    writer.writerow([format_name, count, "; ".join(example_files)])
            
            logger.info(f"Rapport d'export créé: {report_file}")
            logger.info(f"Rapport CSV créé: {report_csv}")
            
        except Exception as e:
            logger.warning(f"Impossible de créer le rapport d'export: {e}")


def main():
    """Fonction principale"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Exporte les données pour les outils BI')
    parser.add_argument('--db-path', type=str, default='data/loans.db', 
                       help='Chemin de la base de données')
    parser.add_argument('--output-dir', type=str, default='data/exports',
                       help='Répertoire de sortie')
    parser.add_argument('--format', type=str, choices=['all', 'looker', 'powerbi', 'tableau', 'metabase', 'excel', 'json'],
                       default='all', help='Format d\'export')
    parser.add_argument('--tables', type=str, nargs='+',
                       help='Tables spécifiques à exporter')
    
    args = parser.parse_args()
    
    # Initialiser l'exportateur
    exporter = BIExporter(args.db_path, args.output_dir)
    
    if not exporter.create_connection():
        logger.error("Impossible de se connecter à la base de données")
        sys.exit(1)
    
    # Exécuter l'export
    try:
        if args.tables:
            # Exporter des tables spécifiques
            for table in args.tables:
                exporter.export_table_to_csv(table)
        elif args.format == 'all':
            exporter.export_all_formats()
        elif args.format == 'looker':
            exporter.export_for_looker_studio()
        elif args.format == 'powerbi':
            exporter.export_for_power_bi()
        elif args.format == 'tableau':
            exporter.export_for_tableau()
        elif args.format == 'metabase':
            exporter.export_for_metabase()
        elif args.format == 'excel':
            exporter._export_to_excel()
        elif args.format == 'json':
            exporter._export_to_json()
        
        logger.info("Export terminé avec succès!")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'export: {e}")
        sys.exit(1)
    
    finally:
        if exporter.conn:
            exporter.conn.close()
            logger.info("Connexion à la base de données fermée")


if __name__ == "__main__":
    main()
