import sqlite3
import pandas as pd
import glob
import os
import sys
from datetime import datetime

def setup_directories():
    """Crée les répertoires nécessaires s'ils n'existent pas"""
    directories = ['data/outputs', 'sql/queries', 'logs']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def log_error(error_msg, sql_file, query=""):
    """Enregistre les erreurs dans un fichier log"""
    log_file = f"logs/execution_errors_{datetime.now().strftime('%Y%m%d')}.log"
    
    log_entry = f"""
{'='*80}
Date: {datetime.now()}
Fichier: {sql_file}
Erreur: {error_msg}
Requête: {query[:200]}... (tronquée)
{'='*80}
"""
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(log_entry)
    
    return log_file

def get_query_type(query):
    """Détermine le type de requête SQL"""
    query_upper = query.strip().upper()
    if query_upper.startswith('SELECT'):
        return 'SELECT'
    elif query_upper.startswith(('INSERT', 'UPDATE', 'DELETE')):
        return 'DML'
    elif query_upper.startswith(('CREATE', 'DROP', 'ALTER')):
        return 'DDL'
    else:
        return 'OTHER'

def execute_single_query(conn, query, sql_file):
    """Exécute une seule requête SQL"""
    cursor = conn.cursor()
    query_type = get_query_type(query)
    
    try:
        if query_type == 'SELECT':
            # Pour les SELECT, utiliser pandas pour récupérer les résultats
            df = pd.read_sql_query(query, conn)
            return df, None
        else:
            # Pour les autres types de requêtes
            cursor.execute(query)
            conn.commit()
            
            if query_type == 'DML':
                affected_rows = cursor.rowcount
                return pd.DataFrame({'affected_rows': [affected_rows]}), None
            else:
                return pd.DataFrame({'status': [f'{query_type} executed successfully']}), None
                
    except sqlite3.Error as e:
        error_msg = f"Erreur SQL: {e}"
        log_file = log_error(error_msg, sql_file, query)
        return None, f"{error_msg}\nVoir le log: {log_file}"
    except Exception as e:
        error_msg = f"Erreur inattendue: {e}"
        log_file = log_error(error_msg, sql_file, query)
        return None, f"{error_msg}\nVoir le log: {log_file}"

def run_all_queries(db_path='data/loans.db'):
    """Exécute toutes les requêtes SQL dans le dossier sql/queries/"""
    setup_directories()
    
    # Connexion à la base de données
    try:
        conn = sqlite3.connect(db_path)
        print(f"✅ Connecté à la base de données: {db_path}")
    except sqlite3.Error as e:
        print(f"❌ Erreur de connexion à la base de données: {e}")
        sys.exit(1)
    
    # Lister tous les fichiers SQL
    sql_files = glob.glob('sql/queries/*.sql')
    
    if not sql_files:
        print("⚠️  Aucun fichier SQL trouvé dans le dossier sql/queries/")
        print("📁 Créez des fichiers .sql dans le dossier sql/queries/")
        conn.close()
        return
    
    print(f"📊 {len(sql_files)} fichier(s) SQL trouvé(s)")
    
    for sql_file in sorted(sql_files):
        print(f"\n{'='*60}")
        print(f"📋 Exécution: {os.path.basename(sql_file)}")
        print(f"📁 Chemin: {sql_file}")
        print('='*60)
        
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Vérifier si le fichier est vide
            if not content.strip():
                print("⚠️  Fichier vide, ignoré")
                continue
            
            # Séparer les requêtes multiples (séparées par des points-virgules)
            queries = [q.strip() for q in content.split(';') if q.strip()]
            
            print(f"📝 {len(queries)} requête(s) trouvée(s) dans le fichier")
            
            for i, query in enumerate(queries, 1):
                print(f"\n  └─ Requête {i}/{len(queries)}:")
                print(f"    Type: {get_query_type(query)}")
                
                df, error = execute_single_query(conn, query, sql_file)
                
                if error:
                    print(f"    ❌ {error}")
                elif df is not None:
                    print(f"    ✅ Succès: {len(df)} ligne(s), {len(df.columns)} colonne(s)")
                    
                    # Sauvegarder les résultats pour les SELECT
                    if get_query_type(query) == 'SELECT' and not df.empty:
                        output_file = f"data/outputs/{os.path.basename(sql_file).replace('.sql', '')}_query{i}.csv"
                        df.to_csv(output_file, index=False, encoding='utf-8')
                        print(f"    💾 Exporté vers: {output_file}")
                        
                        # Afficher un aperçu
                        if len(df) > 0:
                            print(f"    📊 Aperçu (3 premières lignes):")
                            print(df.head(3).to_string())
                    elif not df.empty:
                        print(f"    📝 Résultat: {df.iloc[0,0]}")
        
        except FileNotFoundError:
            print(f"❌ Fichier non trouvé: {sql_file}")
        except UnicodeDecodeError:
            print(f"❌ Erreur d'encodage du fichier: {sql_file}")
        except Exception as e:
            print(f"❌ Erreur lors de la lecture du fichier: {e}")
    
    # Fermer la connexion
    conn.close()
    print(f"\n{'='*60}")
    print("✅ Exécution terminée!")
    print("📁 Résultats dans: data/outputs/")
    print("📁 Logs dans: logs/")
    print('='*60)

def create_sample_schema():
    """Crée un schéma exemple si la base de données n'existe pas"""
    schema_sql = """
    -- Création des tables exemple
    CREATE TABLE IF NOT EXISTS clients (
        client_id INTEGER PRIMARY KEY,
        nom TEXT,
        prenom TEXT,
        age INTEGER,
        revenu_mensuel REAL,
        credit_score INTEGER
    );
    
    CREATE TABLE IF NOT EXISTS prets (
        pret_id INTEGER PRIMARY KEY,
        client_id INTEGER,
        montant REAL,
        duree_mois INTEGER,
        taux_interet REAL,
        date_debut DATE,
        statut TEXT,
        FOREIGN KEY (client_id) REFERENCES clients(client_id)
    );
    
    CREATE TABLE IF NOT EXISTS paiements (
        paiement_id INTEGER PRIMARY KEY,
        pret_id INTEGER,
        date_paiement DATE,
        montant REAL,
        FOREIGN KEY (pret_id) REFERENCES prets(pret_id)
    );
    
    -- Insertion de données exemple
    INSERT INTO clients (nom, prenom, age, revenu_mensuel, credit_score) VALUES
    ('Dupont', 'Jean', 35, 3500.00, 750),
    ('Martin', 'Marie', 28, 2800.00, 680),
    ('Durand', 'Pierre', 45, 4200.00, 820);
    
    INSERT INTO prets (client_id, montant, duree_mois, taux_interet, date_debut, statut) VALUES
    (1, 15000.00, 36, 3.5, '2023-01-15', 'ACTIF'),
    (2, 8000.00, 24, 4.2, '2023-02-20', 'ACTIF'),
    (3, 25000.00, 48, 2.9, '2023-03-10', 'CLOTURE');
    
    INSERT INTO paiements (pret_id, date_paiement, montant) VALUES
    (1, '2023-02-15', 450.00),
    (1, '2023-03-15', 450.00),
    (2, '2023-03-20', 350.00);
    """
    
    conn = sqlite3.connect('data/loans.db')
    cursor = conn.cursor()
    
    try:
        cursor.executescript(schema_sql)
        conn.commit()
        print("✅ Schéma exemple créé avec succès!")
    except sqlite3.Error as e:
        print(f"❌ Erreur lors de la création du schéma: {e}")
    finally:
        conn.close()

def create_sample_query_files():
    """Crée des fichiers d'exemple de requêtes SQL"""
    sample_queries = {
        '01_total_pret_par_client.sql': """
            -- KPI 1: Total des prêts par client
            SELECT 
                c.client_id,
                c.nom || ' ' || c.prenom AS client_nom,
                COUNT(p.pret_id) AS nombre_prets,
                SUM(p.montant) AS total_montant_pret,
                AVG(p.taux_interet) AS taux_interet_moyen
            FROM clients c
            LEFT JOIN prets p ON c.client_id = p.client_id
            GROUP BY c.client_id
            ORDER BY total_montant_pret DESC;
        """,
        
        '02_portefeuille_pret.sql': """
            -- KPI 2: Vue d'ensemble du portefeuille de prêts
            SELECT 
                statut,
                COUNT(*) AS nombre_prets,
                SUM(montant) AS montant_total,
                AVG(taux_interet) AS taux_moyen,
                AVG(duree_mois) AS duree_moyenne
            FROM prets
            GROUP BY statut
            ORDER BY montant_total DESC;
        """,
        
        '03_analyse_risque.sql': """
            -- KPI 3: Analyse de risque par client
            SELECT 
                c.client_id,
                c.nom || ' ' || c.prenom AS client_nom,
                c.credit_score,
                c.revenu_mensuel,
                SUM(p.montant) AS total_pret,
                SUM(p.montant) / (c.revenu_mensuel * 12) AS ratio_dette_revenu,
                CASE 
                    WHEN c.credit_score > 750 THEN 'FAIBLE'
                    WHEN c.credit_score BETWEEN 650 AND 750 THEN 'MOYEN'
                    ELSE 'ÉLEVÉ'
                END AS niveau_risque
            FROM clients c
            LEFT JOIN prets p ON c.client_id = p.client_id
            WHERE p.statut = 'ACTIF'
            GROUP BY c.client_id
            ORDER BY niveau_risque, credit_score;
        """
    }
    
    for filename, query in sample_queries.items():
        filepath = f"sql/queries/{filename}"
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(query)
        print(f"✅ Fichier créé: {filepath}")

if __name__ == "__main__":
    print("🔧 Initialisation du système d'exécution SQL")
    print('='*60)
    
    # Vérifier si la base de données existe
    if not os.path.exists('data/loans.db'):
        print("⚠️  Base de données non trouvée")
        response = input("Voulez-vous créer une base de données exemple? (o/n): ")
        if response.lower() == 'o':
            create_sample_schema()
            create_sample_query_files()
        else:
            print("❌ Veuillez créer la base de données manuellement")
            sys.exit(1)
    
    # Vérifier s'il y a des fichiers SQL
    if not glob.glob('sql/queries/*.sql'):
        print("⚠️  Aucune requête SQL trouvée")
        response = input("Voulez-vous créer des exemples de requêtes? (o/n): ")
        if response.lower() == 'o':
            create_sample_query_files()
    
    # Exécuter toutes les requêtes
    run_all_queries()
