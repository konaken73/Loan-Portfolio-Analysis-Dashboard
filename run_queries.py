# 1. Créer un script pour exécuter toutes les requêtes SQL

import sqlite3
import pandas as pd
import glob
import os

def run_all_queries(db_path='data/loans.db'):
    conn = sqlite3.connect(db_path)
    
    # Lister tous les fichiers SQL
    sql_files = glob.glob('sql/queries/*.sql')
    
    for sql_file in sql_files:
        print(f"\n{'='*60}")
        print(f"Exécution: {os.path.basename(sql_file)}")
        print('='*60)
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            query = f.read()
        
        try:
            df = pd.read_sql_query(query, conn)
            print(f"Résultat: {len(df)} lignes, {len(df.columns)} colonnes")
            
            # Sauvegarder les résultats
            output_file = f"data/outputs/{os.path.basename(sql_file).replace('.sql', '.csv')}"
            df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"Exporté vers: {output_file}")
            
            # Afficher un aperçu
            if not df.empty:
                print("\nAperçu des données:")
                print(df.head())
                
        except Exception as e:
            print(f"Erreur: {e}")
    
    conn.close()
    print("\n✅ Toutes les requêtes ont été exécutées!")

if __name__ == "__main__":
    conn = sqlite3.connect('data/loans.db')
    
    
        
    with open('sql/schema.sql', 'r', encoding='utf-8') as f:
        query = f.read()
        
    try:
            df = pd.read_sql_query(query, conn)
            print(f"df")
            print(f"Résultat: {len(df)} lignes, {len(df.columns)} colonnes")

    except Exception as e:
            print(f"Erreur: {e}")
    
    conn.close()
    print("\n✅ Toutes les requêtes ont été exécutées!")


    #run_all_queries()

