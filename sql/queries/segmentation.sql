-- =============================================================================
-- SEGMENTATION DES EMPRUNTEURS
-- =============================================================================

-- 1. SEGMENTATION PAR PROFESSION
SELECT 
    CASE 
        WHEN emp_title IS NULL OR emp_title = '' OR emp_title = 'UNKNOWN' THEN 'Non renseigné'
        WHEN UPPER(emp_title) LIKE '%TEACH%' THEN 'Enseignement'
        WHEN UPPER(emp_title) LIKE '%NURS%' OR UPPER(emp_title) LIKE '%DOCTOR%' OR UPPER(emp_title) LIKE '%MEDICAL%' THEN 'Santé'
        WHEN UPPER(emp_title) LIKE '%ENGINEER%' THEN 'Ingénierie'
        WHEN UPPER(emp_title) LIKE '%MANAGER%' OR UPPER(emp_title) LIKE '%DIRECTOR%' THEN 'Management'
        WHEN UPPER(emp_title) LIKE '%SALES%' THEN 'Ventes'
        WHEN UPPER(emp_title) LIKE '%DRIVER%' OR UPPER(emp_title) LIKE '%TRUCK%' THEN 'Transport'
        WHEN UPPER(emp_title) LIKE '%STUDENT%' THEN 'Étudiant'
        WHEN UPPER(emp_title) LIKE '%RETIRED%' THEN 'Retraité'
        WHEN UPPER(emp_title) LIKE '%OWNER%' OR UPPER(emp_title) LIKE '%SELF%' THEN 'Propriétaire/Indépendant'
        ELSE 'Autre'
    END as categorie_profession,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(dti), 2) as dti_moyen
FROM loans
GROUP BY categorie_profession
ORDER BY nombre_emprunteurs DESC;

-- 2. SEGMENTATION PAR ANCIENNETÉ D'EMPLOI
SELECT 
    CASE 
        WHEN emp_length IS NULL OR emp_length = '' THEN 'Non renseigné'
        WHEN emp_length LIKE '%10+%' THEN '10+ ans'
        WHEN emp_length LIKE '%< 1%' THEN 'Moins de 1 an'
        ELSE emp_length
    END as anciennete_emploi,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(dti), 2) as dti_moyen
FROM loans
GROUP BY anciennete_emploi
ORDER BY 
    CASE anciennete_emploi
        WHEN 'Non renseigné' THEN 0
        WHEN 'Moins de 1 an' THEN 1
        WHEN '1 an' THEN 2
        WHEN '2 ans' THEN 3
        WHEN '3 ans' THEN 4
        WHEN '4 ans' THEN 5
        WHEN '5 ans' THEN 6
        WHEN '6 ans' THEN 7
        WHEN '7 ans' THEN 8
        WHEN '8 ans' THEN 9
        WHEN '9 ans' THEN 10
        WHEN '10+ ans' THEN 11
        ELSE 12
    END;

-- 3. PROFIL D'ENDRETTEMENT (DTI)
SELECT 
    CASE 
        WHEN dti IS NULL THEN 'Non renseigné'
        WHEN dti <= 10 THEN '0-10%'
        WHEN dti <= 20 THEN '10-20%'
        WHEN dti <= 30 THEN '20-30%'
        WHEN dti <= 40 THEN '30-40%'
        WHEN dti <= 50 THEN '40-50%'
        ELSE '50%+' 
    END as tranche_dti,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(dti), 2) as dti_moyen,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(revol_util), 2) as utilisation_revolving_moyenne
FROM loans
GROUP BY tranche_dti
ORDER BY 
    CASE tranche_dti
        WHEN 'Non renseigné' THEN 0
        WHEN '0-10%' THEN 1
        WHEN '10-20%' THEN 2
        WHEN '20-30%' THEN 3
        WHEN '30-40%' THEN 4
        WHEN '40-50%' THEN 5
        WHEN '50%+' THEN 6
        ELSE 7
    END;

-- 4. SEGMENTATION PAR OBJECTIF DE PRÊT
SELECT 
    purpose as objectif_pret,
    COUNT(*) as nombre_prets,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    ROUND(MIN(loan_amnt), 0) as pret_min,
    ROUND(MAX(loan_amnt), 0) as pret_max,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(dti), 2) as dti_moyen
FROM loans
WHERE purpose IS NOT NULL AND purpose != ''
GROUP BY purpose
ORDER BY nombre_prets DESC;

-- 5. PROFIL GÉOGRAPHIQUE DÉTAILLÉ
SELECT 
    addr_state as etat,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(dti), 2) as dti_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(home_ownership = 'OWN' OR home_ownership = 'MORTGAGE') * 100, 1) as pourcentage_proprietaires
FROM loans
WHERE addr_state IS NOT NULL AND addr_state != ''
GROUP BY addr_state
HAVING COUNT(*) > 50
ORDER BY nombre_emprunteurs DESC;

-- 6. SEGMENTATION PAR ÂGE DU CRÉDIT
SELECT 
    credit_age_category as categorie_age_credit,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(credit_age_years), 1) as age_credit_moyen,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(total_acc), 1) as nombre_comptes_moyen
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

-- 7. PROFIL DE CRÉDIT (HISTORIQUE)
SELECT 
    CASE 
        WHEN delinq_2yrs = 0 AND pub_rec = 0 THEN 'Historique propre'
        WHEN delinq_2yrs > 0 AND pub_rec = 0 THEN 'Délinquances mineures'
        WHEN pub_rec > 0 THEN 'Problèmes publics'
        ELSE 'Autre'
    END as profil_credit,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(delinq_2yrs), 2) as delinquence_moyenne,
    ROUND(AVG(pub_rec), 2) as problemes_publics_moyens,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut
FROM loans
GROUP BY profil_credit
ORDER BY nombre_emprunteurs DESC;

-- 8. SEGMENTATION PAR TYPE DE PROPRIÉTÉ
SELECT 
    home_ownership as type_propriete,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(annual_inc), 0) as revenu_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(dti), 2) as dti_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(credit_age_years), 1) as age_credit_moyen
FROM loans
WHERE home_ownership IS NOT NULL AND home_ownership != ''
GROUP BY home_ownership
ORDER BY nombre_emprunteurs DESC;

-- 9. COMPORTEMENT DE RECHERCHE DE CRÉDIT
SELECT 
    CASE 
        WHEN inq_last_6mths = 0 THEN '0 requête'
        WHEN inq_last_6mths = 1 THEN '1 requête'
        WHEN inq_last_6mths = 2 THEN '2 requêtes'
        WHEN inq_last_6mths = 3 THEN '3 requêtes'
        WHEN inq_last_6mths <= 5 THEN '4-5 requêtes'
        ELSE '6+ requêtes'
    END as requetes_recentes,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(inq_last_6mths), 2) as requetes_moyennes,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(annual_inc), 0) as revenu_moyen
FROM loans
WHERE inq_last_6mths IS NOT NULL
GROUP BY requetes_recentes
ORDER BY 
    CASE requetes_recentes
        WHEN '0 requête' THEN 1
        WHEN '1 requête' THEN 2
        WHEN '2 requêtes' THEN 3
        WHEN '3 requêtes' THEN 4
        WHEN '4-5 requêtes' THEN 5
        WHEN '6+ requêtes' THEN 6
        ELSE 7
    END;

-- 10. SEGMENTATION PAR NOMBRE DE COMPTES OUVERTS
SELECT 
    CASE 
        WHEN open_acc <= 5 THEN '0-5 comptes'
        WHEN open_acc <= 10 THEN '6-10 comptes'
        WHEN open_acc <= 15 THEN '11-15 comptes'
        WHEN open_acc <= 20 THEN '16-20 comptes'
        ELSE '21+ comptes'
    END as nombre_comptes_ouverts,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(open_acc), 1) as comptes_moyens,
    ROUND(AVG(total_acc), 1) as total_comptes_moyen,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(revol_util), 2) as utilisation_revolving_moyenne
FROM loans
WHERE open_acc IS NOT NULL
GROUP BY nombre_comptes_ouverts
ORDER BY 
    CASE nombre_comptes_ouverts
        WHEN '0-5 comptes' THEN 1
        WHEN '6-10 comptes' THEN 2
        WHEN '11-15 comptes' THEN 3
        WHEN '16-20 comptes' THEN 4
        WHEN '21+ comptes' THEN 5
        ELSE 6
    END;

-- 11. PROFILS COMPLEXES (CROISEMENT MULTI-CRITÈRES)
WITH profils AS (
    SELECT 
        id,
        CASE 
            WHEN annual_inc >= 100000 AND dti <= 20 AND grade IN ('A', 'B') THEN 'Premium'
            WHEN annual_inc >= 50000 AND dti <= 30 AND grade IN ('A', 'B', 'C') THEN 'Standard Plus'
            WHEN annual_inc >= 30000 AND dti <= 40 AND grade IN ('A', 'B', 'C', 'D') THEN 'Standard'
            WHEN annual_inc < 30000 OR dti > 40 OR grade IN ('E', 'F', 'G') THEN 'Risqué'
            ELSE 'Autre'
        END as profil_emprunteur
    FROM loans
)
SELECT 
    p.profil_emprunteur,
    COUNT(*) as nombre_emprunteurs,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_total,
    ROUND(AVG(l.annual_inc), 0) as revenu_moyen,
    ROUND(AVG(l.dti), 2) as dti_moyen,
    ROUND(AVG(l.int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(l.loan_amnt), 0) as pret_moyen,
    SUM(CASE WHEN l.is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN l.is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut,
    ROUND(AVG(l.credit_age_years), 1) as age_credit_moyen
FROM profils p
JOIN loans l ON p.id = l.id
GROUP BY p.profil_emprunteur
ORDER BY 
    CASE p.profil_emprunteur
        WHEN 'Premium' THEN 1
        WHEN 'Standard Plus' THEN 2
        WHEN 'Standard' THEN 3
        WHEN 'Risqué' THEN 4
        ELSE 5
    END;

-- 12. MATRICE DE SEGMENTATION FINALE POUR MARKETING
SELECT 
    income_category as segment_revenu,
    risk_category as segment_risque,
    COUNT(*) as nombre_clients,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM loans), 2) as pourcentage_portefeuille,
    ROUND(AVG(annual_inc), 0) as revenu_moyen_segment,
    ROUND(AVG(int_rate), 2) as taux_interet_moyen,
    ROUND(AVG(loan_amnt), 0) as pret_moyen_demande,
    SUM(loan_amnt) as montant_total_pret,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as defauts,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_defaut_segment,
    ROUND(AVG(total_pymnt) * 100.0 / AVG(loan_amnt), 2) as taux_recouvrement_segment
FROM loans
WHERE income_category IS NOT NULL AND risk_category IS NOT NULL
GROUP BY income_category, risk_category
ORDER BY 
    CASE income_category
        WHEN 'Très élevé' THEN 1
        WHEN 'Élevé' THEN 2
        WHEN 'Moyen' THEN 3
        WHEN 'Faible' THEN 4
        WHEN 'Très faible' THEN 5
        ELSE 6
    END,
    CASE risk_category
        WHEN 'Faible risque' THEN 1
        WHEN 'Risque modéré' THEN 2
        WHEN 'Risque moyen' THEN 3
        WHEN 'Risque élevé' THEN 4
        WHEN 'Risque très élevé' THEN 5
        WHEN 'Risque extrême' THEN 6
        ELSE 7
    END;
