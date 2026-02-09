-- =============================================================================
-- REQUÊTES SQL POUR LES KPIs DE VUE D'ENSEMBLE
-- =============================================================================

-- 1. METRIQUES CLÉS DU PORTEFEUILLE
SELECT 
    'Portefeuille Total' as indicateur,
    COUNT(*) as valeur,
    'Nombre de prêts' as description
FROM loans
UNION ALL
SELECT 
    'Montant Total',
    ROUND(SUM(loan_amnt) / 1000000, 2),
    'Montant total en millions USD'
FROM loans
UNION ALL
SELECT 
    'Montant Moyen',
    ROUND(AVG(loan_amnt), 0),
    'Montant moyen par prêt (USD)'
FROM loans
UNION ALL
SELECT 
    'Taux Défaut',
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
    'Pourcentage de prêts en défaut'
FROM loans
UNION ALL
SELECT 
    'Taux Remboursement',
    ROUND(SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
    'Pourcentage de prêts entièrement remboursés'
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
    'Revenu annuel moyen (USD)'
FROM loans
UNION ALL
SELECT 
    'Ratio D/R Moyen',
    ROUND(AVG(dti), 2),
    'Ratio dette/revenu moyen'
FROM loans
ORDER BY 
    CASE indicateur 
        WHEN 'Portefeuille Total' THEN 1
        WHEN 'Montant Total' THEN 2
        WHEN 'Taux Défaut' THEN 3
        WHEN 'Taux Intérêt Moyen' THEN 4
        ELSE 5 
    END;

-- 2. DISTRIBUTION DES PRÊTS PAR GRADE DE CRÉDIT
SELECT 
    grade,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as montant_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(SUM(loan_amnt), 0) as montant_total
FROM loans
WHERE grade IS NOT NULL
GROUP BY grade
ORDER BY grade;

-- 3. DISTRIBUTION PAR DURÉE DE PRÊT (TERM)
SELECT 
    term,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as montant_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(SUM(loan_amnt), 0) as montant_total
FROM loans
WHERE term IS NOT NULL
GROUP BY term
ORDER BY 
    CASE 
        WHEN term LIKE '%36%' THEN 1
        WHEN term LIKE '%60%' THEN 2
        ELSE 3 
    END;

-- 4. DISTRIBUTION DES MONTANTS DE PRÊT
SELECT 
    CASE 
        WHEN loan_amnt < 5000 THEN '0-5k'
        WHEN loan_amnt < 10000 THEN '5k-10k'
        WHEN loan_amnt < 15000 THEN '10k-15k'
        WHEN loan_amnt < 20000 THEN '15k-20k'
        WHEN loan_amnt < 25000 THEN '20k-25k'
        WHEN loan_amnt < 30000 THEN '25k-30k'
        WHEN loan_amnt < 35000 THEN '30k-35k'
        ELSE '35k+' 
    END as tranche_montant,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    MIN(loan_amnt) as montant_min,
    ROUND(AVG(loan_amnt), 0) as montant_moyen,
    MAX(loan_amnt) as montant_max,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut
FROM loans
GROUP BY tranche_montant
ORDER BY 
    CASE 
        WHEN tranche_montant = '0-5k' THEN 1
        WHEN tranche_montant = '5k-10k' THEN 2
        WHEN tranche_montant = '10k-15k' THEN 3
        WHEN tranche_montant = '15k-20k' THEN 4
        WHEN tranche_montant = '20k-25k' THEN 5
        WHEN tranche_montant = '25k-30k' THEN 6
        WHEN tranche_montant = '30k-35k' THEN 7
        ELSE 8 
    END;

-- 5. DISTRIBUTION DES TAUX D'INTÉRÊT
SELECT 
    CASE 
        WHEN int_rate < 5 THEN '0-5%'
        WHEN int_rate < 10 THEN '5-10%'
        WHEN int_rate < 15 THEN '10-15%'
        WHEN int_rate < 20 THEN '15-20%'
        WHEN int_rate < 25 THEN '20-25%'
        ELSE '25%+' 
    END as tranche_taux,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(int_rate), 2) as taux_moyen,
    MIN(int_rate) as taux_min,
    MAX(int_rate) as taux_max,
    ROUND(AVG(loan_amnt), 0) as montant_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut
FROM loans
WHERE int_rate IS NOT NULL
GROUP BY tranche_taux
ORDER BY 
    CASE 
        WHEN tranche_taux = '0-5%' THEN 1
        WHEN tranche_taux = '5-10%' THEN 2
        WHEN tranche_taux = '10-15%' THEN 3
        WHEN tranche_taux = '15-20%' THEN 4
        WHEN tranche_taux = '20-25%' THEN 5
        ELSE 6 
    END;

-- 6. ÉVOLUTION TEMPORELLE DES DÉCAISSEMENTS
SELECT 
    issue_year as annee,
    issue_month as mois,
    issue_year || '-' || PRINTF('%02d', issue_month) as periode,
    COUNT(*) as prets_emis,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut_mensuel
FROM loans
WHERE issue_year IS NOT NULL AND issue_month IS NOT NULL
GROUP BY issue_year, issue_month
ORDER BY issue_year, issue_month;

-- 7. PERFORMANCE PAR ANNÉE
SELECT 
    issue_year as annee,
    COUNT(*) as total_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as total_defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) as total_rembourses,
    ROUND(SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_remboursement
FROM loans
WHERE issue_year IS NOT NULL
GROUP BY issue_year
ORDER BY issue_year;

-- 8. STATUT ACTUEL DES PRÊTS
SELECT 
    loan_status as statut,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as montant_moyen,
    CASE 
        WHEN loan_status IN ('FULLY PAID', 'CURRENT', 'IN GRACE PERIOD') THEN 'Performing'
        WHEN loan_status IN ('CHARGED OFF', 'DEFAULT') THEN 'Non-Performing'
        WHEN loan_status LIKE 'LATE%' THEN 'Late'
        ELSE 'Other'
    END as categorie_performance
FROM loans
WHERE loan_status IS NOT NULL
GROUP BY loan_status
ORDER BY 
    CASE 
        WHEN loan_status = 'FULLY PAID' THEN 1
        WHEN loan_status = 'CURRENT' THEN 2
        WHEN loan_status = 'IN GRACE PERIOD' THEN 3
        WHEN loan_status LIKE 'LATE%' THEN 4
        WHEN loan_status = 'CHARGED OFF' THEN 5
        WHEN loan_status = 'DEFAULT' THEN 6
        ELSE 7 
    END;

-- 9. RÉCAPITULATIF PAR SAISON
SELECT 
    issue_season as saison,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(loan_amnt), 0) as montant_moyen
FROM loans
WHERE issue_season IS NOT NULL
GROUP BY issue_season
ORDER BY 
    CASE issue_season
        WHEN 'Hiver' THEN 1
        WHEN 'Printemps' THEN 2
        WHEN 'Été' THEN 3
        WHEN 'Automne' THEN 4
        ELSE 5 
    END;

-- 10. TOP 10 DES ÉTATS PAR VOLUME DE PRÊT
SELECT 
    addr_state as etat,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(dti), 2) as dti_moyen
FROM loans
WHERE addr_state IS NOT NULL AND addr_state != ''
GROUP BY addr_state
ORDER BY montant_total DESC
LIMIT 10;
