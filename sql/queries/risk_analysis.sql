-- =============================================================================
-- ANALYSE DES RISQUES DU PORTEFEUILLE
-- =============================================================================

-- 1. TAUX DE DÉFAUT PAR GRADE ET SOUS-GRADE
SELECT 
    grade,
    
    COUNT(*) as nombre_prets,
    SUM(loan_amnt) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as nombre_defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(SUM(CASE WHEN is_default = 1 THEN loan_amnt ELSE 0 END), 0) as montant_en_defaut,
    ROUND(SUM(CASE WHEN is_default = 1 THEN loan_amnt ELSE 0 END) * 100.0 / SUM(loan_amnt), 2) as taux_defaut_montant
FROM loans
WHERE grade IS NOT NULL
GROUP BY grade
ORDER BY grade;

-- 2. TAUX DE DÉFAUT PAR DURÉE DE PRÊT
SELECT 
    term,
    COUNT(*) as nombre_prets,
    SUM(loan_amnt) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(CASE WHEN is_default = 1 THEN loan_amnt ELSE NULL END), 0) as montant_moyen_defaut,
    ROUND(AVG(CASE WHEN is_default = 0 THEN loan_amnt ELSE NULL END), 0) as montant_moyen_non_defaut
FROM loans
WHERE term IS NOT NULL
GROUP BY term
ORDER BY term;

-- 3. ANALYSE DE LA CORRÉLATION MONTANT/TAUX DE DÉFAUT
WITH montant_tranches AS (
    SELECT 
        loan_amnt,
        CASE 
            WHEN loan_amnt <= 5000 THEN '0-5k'
            WHEN loan_amnt <= 10000 THEN '5k-10k'
            WHEN loan_amnt <= 15000 THEN '10k-15k'
            WHEN loan_amnt <= 20000 THEN '15k-20k'
            WHEN loan_amnt <= 25000 THEN '20k-25k'
            WHEN loan_amnt <= 30000 THEN '25k-30k'
            WHEN loan_amnt <= 35000 THEN '30k-35k'
            ELSE '35k+' 
        END as tranche_montant
    FROM loans
)
SELECT 
    tranche_montant,
    COUNT(*) as nombre_prets,
    ROUND(AVG(l.loan_amnt), 0) as montant_moyen,
    ROUND(AVG(l.int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN l.is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN l.is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(l.annual_inc), 0) as revenu_moyen,
    ROUND(AVG(l.dti), 2) as dti_moyen
FROM montant_tranches mt
JOIN loans l ON mt.loan_amnt = l.loan_amnt
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

-- 4. DÉFAUT PAR TYPE DE PROPRIÉTÉ (HOME OWNERSHIP)
SELECT 
    home_ownership as type_propriete,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(dti), 2) as dti_moyen
FROM loans
WHERE home_ownership IS NOT NULL AND home_ownership != ''
GROUP BY home_ownership
ORDER BY taux_defaut DESC;

-- 5. DÉFAUT PAR STATUT DE VÉRIFICATION DE REVENU
SELECT 
    verification_status as statut_verification,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(dti), 2) as dti_moyen
FROM loans
WHERE verification_status IS NOT NULL
GROUP BY verification_status
ORDER BY taux_defaut DESC;

-- 6. ANALYSE DE RISQUE PAR ÂGE DU CRÉDIT
SELECT 
    credit_age_category as categorie_age_credit,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(credit_age_years), 1) as age_credit_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(delinq_2yrs), 2) as delinquence_moyenne,
    ROUND(AVG(inq_last_6mths), 2) as requetes_recentes_moyennes
FROM loans
WHERE credit_age_category IS NOT NULL
GROUP BY credit_age_category
ORDER BY 
    CASE categorie_age_credit
        WHEN '0-2 ans' THEN 1
        WHEN '2-5 ans' THEN 2
        WHEN '5-10 ans' THEN 3
        WHEN '10-20 ans' THEN 4
        WHEN '20+ ans' THEN 5
        ELSE 6
    END;

-- 7. PRÊTS À HAUT RISQUE (MULTIPLE CRITÈRES)
SELECT 
    id,
    loan_amnt,
    int_rate,
    grade,
    
    term,
    home_ownership,
    annual_inc,
    dti,
    loan_status,
    credit_age_years,
    delinq_2yrs,
    inq_last_6mths,
    revol_util,
    risk_category,
    CASE 
        WHEN int_rate > 20 THEN 1 ELSE 0 
    END + 
    CASE 
        WHEN grade IN ('F', 'G') THEN 1 ELSE 0 
    END + 
    CASE 
        WHEN dti > 30 THEN 1 ELSE 0 
    END + 
    CASE 
        WHEN revol_util > 80 THEN 1 ELSE 0 
    END + 
    CASE 
        WHEN delinq_2yrs > 0 THEN 1 ELSE 0 
    END as score_risque
FROM loans
WHERE is_default = 0 AND loan_status IN ('CURRENT', 'IN GRACE PERIOD')
HAVING score_risque >= 3
ORDER BY score_risque DESC, int_rate DESC
LIMIT 50;

-- 8. IMPACT DU REVENU SUR LES DÉFAUTS
SELECT 
    income_category as categorie_revenu,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(dti), 2) as dti_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(loan_amnt), 0) as montant_pret_moyen
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

-- 9. DÉFAUTS PAR NOMBRE DE DÉLINQUANCES ANTÉRIEURES
SELECT 
    delinq_2yrs as delinquances_2_ans,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as montant_moyen,
    ROUND(AVG(annual_inc), 0) as revenu_moyen
FROM loans
WHERE delinq_2yrs IS NOT NULL
GROUP BY delinq_2yrs
ORDER BY delinq_2yrs;

-- 10. RISQUE PAR UTILISATION DU CRÉDIT REVOLVING
SELECT 
    CASE 
        WHEN revol_util IS NULL THEN 'Non renseigné'
        WHEN revol_util <= 20 THEN '0-20%'
        WHEN revol_util <= 40 THEN '20-40%'
        WHEN revol_util <= 60 THEN '40-60%'
        WHEN revol_util <= 80 THEN '60-80%'
        WHEN revol_util <= 100 THEN '80-100%'
        ELSE '100%+' 
    END as utilisation_revolving,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(revol_util), 2) as utilisation_moyenne,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(revol_bal), 0) as solde_revolving_moyen
FROM loans
GROUP BY utilisation_revolving
ORDER BY 
    CASE utilisation_revolving
        WHEN 'Non renseigné' THEN 1
        WHEN '0-20%' THEN 2
        WHEN '20-40%' THEN 3
        WHEN '40-60%' THEN 4
        WHEN '60-80%' THEN 5
        WHEN '80-100%' THEN 6
        ELSE 7
    END;

-- 11. CONCENTRATION DES RISQUES PAR EMPRUNTEUR
SELECT 
    addr_state as etat,
    COUNT(*) as nombre_prets,
    ROUND(SUM(loan_amnt), 0) as montant_total,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(SUM(CASE WHEN is_default = 1 THEN loan_amnt ELSE 0 END), 0) as montant_en_defaut,
    ROUND(SUM(CASE WHEN is_default = 1 THEN loan_amnt ELSE 0 END) * 100.0 / SUM(loan_amnt), 2) as concentration_risque
FROM loans
WHERE addr_state IS NOT NULL AND addr_state != ''
GROUP BY addr_state
HAVING COUNT(*) > 100
ORDER BY concentration_risque DESC
LIMIT 15;

-- 12. PRÉDICTION DE DÉFAUT BASÉE SUR LE SCORE
WITH risque_scores AS (
    SELECT 
        grade,
        ROUND(AVG(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100, 2) as taux_defaut_historique,
        CASE grade
            WHEN 'A' THEN 'Faible'
            WHEN 'B' THEN 'Modéré'
            WHEN 'C' THEN 'Moyen'
            WHEN 'D' THEN 'Élevé'
            WHEN 'E' THEN 'Très élevé'
            WHEN 'F' THEN 'Extrême'
            WHEN 'G' THEN 'Extrême'
            ELSE 'Inconnu'
        END as niveau_risque,
        CASE grade
            WHEN 'A' THEN 1
            WHEN 'B' THEN 2
            WHEN 'C' THEN 3
            WHEN 'D' THEN 4
            WHEN 'E' THEN 5
            WHEN 'F' THEN 6
            WHEN 'G' THEN 7
            ELSE 8
        END as score_risque
    FROM loans
    WHERE grade IS NOT NULL
    GROUP BY grade
)
SELECT 
    niveau_risque,
    grade,
    score_risque,
    taux_defaut_historique,
    CASE 
        WHEN taux_defaut_historique < 5 THEN 'Acceptable'
        WHEN taux_defaut_historique < 10 THEN 'À surveiller'
        WHEN taux_defaut_historique < 20 THEN 'Élevé'
        ELSE 'Critique'
    END as evaluation_risque,
    CASE 
        WHEN taux_defaut_historique < 5 THEN '🟢'
        WHEN taux_defaut_historique < 10 THEN '🟡'
        WHEN taux_defaut_historique < 20 THEN '🟠'
        ELSE '🔴'
    END as indicateur
FROM risque_scores
ORDER BY score_risque;
