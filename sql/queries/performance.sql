-- =============================================================================
-- ANALYSE DE PERFORMANCE ET RECOUVREMENT
-- =============================================================================

-- 1. TAUX DE RECOUVREMENT PAR STATUT DE PRÊT
SELECT 
    loan_status as statut_pret,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_emis,
    ROUND(SUM(total_pymnt), 0) as montant_recupere,
    ROUND(SUM(total_rec_prncp), 0) as capital_recupere,
    ROUND(SUM(total_rec_int), 0) as interets_recuperes,
    ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2) as taux_recouvrement,
    ROUND(SUM(total_rec_int) * 100.0 / SUM(loan_amnt), 2) as rendement_interets,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen
FROM loans
WHERE loan_status IN ('FULLY PAID', 'CHARGED OFF', 'DEFAULT')
GROUP BY loan_status
ORDER BY taux_recouvrement DESC;

-- 2. PERFORMANCE PAR GRADE - ANALYSE DE RENTABILITÉ
SELECT 
    grade,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(SUM(total_pymnt), 0) as paiements_recus,
    ROUND(SUM(total_rec_int), 0) as interets_recus,
    ROUND(SUM(total_pymnt) - SUM(loan_amnt), 0) as profit_brut,
    ROUND((SUM(total_pymnt) - SUM(loan_amnt)) * 100.0 / SUM(loan_amnt), 2) as marge_brute_pourcentage,
    ROUND(SUM(total_rec_int) * 100.0 / SUM(loan_amnt), 2) as rendement_interets,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut
FROM loans
WHERE grade IS NOT NULL
GROUP BY grade
ORDER BY grade;

-- 3. ÉVOLUTION TEMPORELLE DES PERFORMANCES
SELECT 
    issue_year as annee,
    issue_quarter as trimestre,
    issue_year || 'T' || issue_quarter as periode,
    COUNT(*) as prets_emis,
    ROUND(SUM(loan_amnt), 0) as montant_emis,
    ROUND(SUM(total_pymnt), 0) as paiements_recus,
    ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2) as taux_recouvrement_global,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) as rembourses,
    ROUND(SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_remboursement
FROM loans
WHERE issue_year IS NOT NULL AND issue_quarter IS NOT NULL
GROUP BY issue_year, issue_quarter
ORDER BY issue_year, issue_quarter;

-- 4. ANALYSE DES PAIEMENTS EN RETARD
SELECT 
    CASE 
        WHEN loan_status LIKE 'LATE%' THEN 'En retard'
        WHEN loan_status = 'IN GRACE PERIOD' THEN 'Période de grâce'
        ELSE 'À jour'
    END as statut_paiement,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(SUM(out_prncp), 0) as capital_restant,
    ROUND(SUM(last_pymnt_amnt), 0) as dernier_paiement_total,
    ROUND(AVG(last_pymnt_amnt), 0) as dernier_paiement_moyen,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    MIN(last_pymnt_d) as plus_ancien_paiement,
    MAX(last_pymnt_d) as plus_recent_paiement
FROM loans
WHERE loan_status IS NOT NULL
GROUP BY statut_paiement
ORDER BY 
    CASE statut_paiement
        WHEN 'En retard' THEN 1
        WHEN 'Période de grâce' THEN 2
        ELSE 3
    END;

-- 5. PERFORMANCE PAR DURÉE DE PRÊT
SELECT 
    term as duree,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_emis,
    ROUND(SUM(total_pymnt), 0) as paiements_recus,
    ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2) as taux_recouvrement,
    ROUND(SUM(total_rec_int), 0) as interets_recus,
    ROUND(SUM(total_rec_int) * 100.0 / SUM(loan_amnt), 2) as rendement_interets,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(CASE 
        WHEN last_pymnt_d IS NOT NULL AND issue_d IS NOT NULL 
        THEN (julianday(last_pymnt_d) - julianday(issue_d)) 
        ELSE NULL 
    END), 0) as duree_moyenne_jours
FROM loans
WHERE term IS NOT NULL
GROUP BY term
ORDER BY term;

-- 6. RECOUVREMENT DES PRÊTS EN DÉFAUT
SELECT 
    grade,
    COUNT(*) as nombre_defauts,
    ROUND(SUM(loan_amnt), 0) as montant_defaut,
    ROUND(SUM(recoveries), 0) as montant_recupere,
    ROUND(SUM(collection_recovery_fee), 0) as frais_recouvrement,
    ROUND(SUM(recoveries) * 100.0 / SUM(loan_amnt), 2) as taux_recouvrement_defaut,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as montant_moyen_defaut
FROM loans
WHERE is_default = 1 AND recoveries IS NOT NULL
GROUP BY grade
ORDER BY grade;

-- 7. FLUX DE TRÉSORERIE MENSUEL
WITH paiements_mensuels AS (
    SELECT 
        STRFTIME('%Y-%m', last_pymnt_d) as mois_paiement,
        COUNT(*) as nombre_paiements,
        ROUND(SUM(last_pymnt_amnt), 0) as total_paiements,
        ROUND(AVG(last_pymnt_amnt), 0) as paiement_moyen
    FROM loans
    WHERE last_pymnt_d IS NOT NULL
    GROUP BY STRFTIME('%Y-%m', last_pymnt_d)
),
decaissements_mensuels AS (
    SELECT 
        STRFTIME('%Y-%m', issue_d) as mois_decaissement,
        COUNT(*) as nombre_prets,
        ROUND(SUM(loan_amnt), 0) as total_decaissements
    FROM loans
    WHERE issue_d IS NOT NULL
    GROUP BY STRFTIME('%Y-%m', issue_d)
)
SELECT 
    COALESCE(p.mois_paiement, d.mois_decaissement) as mois,
    COALESCE(d.nombre_prets, 0) as prets_emis,
    COALESCE(d.total_decaissements, 0) as decaissements,
    COALESCE(p.nombre_paiements, 0) as paiements_recus,
    COALESCE(p.total_paiements, 0) as montant_paiements,
    COALESCE(p.total_paiements, 0) - COALESCE(d.total_decaissements, 0) as flux_net
FROM paiements_mensuels p
FULL OUTER JOIN decaissements_mensuels d ON p.mois_paiement = d.mois_decaissement
WHERE COALESCE(p.mois_paiement, d.mois_decaissement) IS NOT NULL
ORDER BY mois;

-- 8. ANALYSE DE LA DURÉE EFFECTIVE DES PRÊTS
SELECT 
    CASE 
        WHEN term LIKE '%36%' THEN '36 mois'
        WHEN term LIKE '%60%' THEN '60 mois'
        ELSE 'Autre'
    END as duree_contractuelle,
    COUNT(*) as nombre_prets,
    ROUND(AVG(CASE 
        WHEN last_pymnt_d IS NOT NULL AND issue_d IS NOT NULL 
        THEN (julianday(last_pymnt_d) - julianday(issue_d)) / 30.44
        ELSE NULL 
    END), 1) as duree_effective_mois,
    ROUND(AVG(CASE 
        WHEN last_pymnt_d IS NOT NULL AND issue_d IS NOT NULL 
        THEN (julianday(last_pymnt_d) - julianday(issue_d)) / 30.44
        ELSE NULL 
    END) * 100.0 / 
    CASE 
        WHEN term LIKE '%36%' THEN 36
        WHEN term LIKE '%60%' THEN 60
        ELSE 36 
    END, 1) as pourcentage_duree_atteinte,
    SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) as prets_termines,
    ROUND(SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as taux_terminaison
FROM loans
WHERE term IS NOT NULL
GROUP BY duree_contractuelle
ORDER BY duree_contractuelle;

-- 9. PERFORMANCE PAR CATÉGORIE DE REVENU
SELECT 
    income_category as categorie_revenu,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_emis,
    ROUND(SUM(total_pymnt), 0) as paiements_recus,
    ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2) as taux_recouvrement,
    ROUND(SUM(total_rec_int), 0) as interets_recus,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(annual_inc), 0) as revenu_moyen
FROM loans
WHERE income_category IS NOT NULL
GROUP BY income_category
ORDER BY 
    CASE categorie_revenu
        WHEN 'Très faible' THEN 1
        WHEN 'Faible' THEN 2
        WHEN 'Moyen' THEN 3
        WHEN 'Élevé' THEN 4
        WHEN 'Très élevé' THEN 5
        ELSE 6
    END;

-- 10. RENDEMENT PAR TAUX D'INTÉRÊT
SELECT 
    int_rate_category as tranche_taux,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_emis,
    ROUND(SUM(total_pymnt), 0) as paiements_recus,
    ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2) as taux_recouvrement,
    ROUND(SUM(total_rec_int), 0) as interets_recus,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(SUM(total_rec_int) * 100.0 / SUM(loan_amnt), 2) as rendement_interets
FROM loans
WHERE int_rate_category IS NOT NULL
GROUP BY int_rate_category
ORDER BY 
    CASE tranche_taux
        WHEN '0-5%' THEN 1
        WHEN '5-10%' THEN 2
        WHEN '10-15%' THEN 3
        WHEN '15-20%' THEN 4
        WHEN '20-30%' THEN 5
        WHEN '30%+' THEN 6
        ELSE 7
    END;

-- 11. ANALYSE DES PRÊTS REMBOURSÉS ANTICIPÉMENT
SELECT 
    CASE 
        WHEN last_pymnt_d IS NOT NULL AND issue_d IS NOT NULL 
        THEN 
            CASE 
                WHEN term LIKE '%36%' AND (julianday(last_pymnt_d) - julianday(issue_d)) < 365 THEN 1
                WHEN term LIKE '%60%' AND (julianday(last_pymnt_d) - julianday(issue_d)) < 730 THEN 1
                ELSE 0
            END
        ELSE 0
    END as remboursement_anticipé,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(CASE 
        WHEN last_pymnt_d IS NOT NULL AND issue_d IS NOT NULL 
        THEN (julianday(last_pymnt_d) - julianday(issue_d)) / 30.44
        ELSE NULL 
    END), 1) as duree_moyenne_mois,
    ROUND(SUM(total_rec_int), 0) as interets_recus,
    ROUND(SUM(total_rec_int) * 100.0 / SUM(loan_amnt), 2) as rendement_interets
FROM loans
WHERE is_fully_paid = 1
GROUP BY remboursement_anticipé;

-- 12. TABLEAU DE BORD PERFORMANCE (VUE SYNTHÈSE)
SELECT 
    'Rentabilité' as categorie,
    'Marge brute' as indicateur,
    ROUND((SUM(total_pymnt) - SUM(loan_amnt)) * 100.0 / SUM(loan_amnt), 2) as valeur,
    '%' as unite
FROM loans
WHERE loan_amnt > 0
UNION ALL
SELECT 
    'Rentabilité',
    'Rendement intérêts',
    ROUND(SUM(total_rec_int) * 100.0 / SUM(loan_amnt), 2),
    '%'
FROM loans
WHERE loan_amnt > 0
UNION ALL
SELECT 
    'Recouvrement',
    'Taux global',
    ROUND(SUM(total_pymnt) * 100.0 / SUM(loan_amnt), 2),
    '%'
FROM loans
WHERE loan_amnt > 0
UNION ALL
SELECT 
    'Recouvrement',
    'Prêts entièrement remboursés',
    ROUND(SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
    '%'
FROM loans
UNION ALL
SELECT 
    'Performance',
    'Durée moyenne effective',
    ROUND(AVG(CASE 
        WHEN last_pymnt_d IS NOT NULL AND issue_d IS NOT NULL 
        THEN (julianday(last_pymnt_d) - julianday(issue_d)) / 30.44
        ELSE NULL 
    END), 1),
    'mois'
FROM loans
UNION ALL
SELECT 
    'Performance',
    'Paiement moyen',
    ROUND(AVG(last_pymnt_amnt), 0),
    'USD'
FROM loans
WHERE last_pymnt_amnt > 0
ORDER BY categorie, indicateur;
