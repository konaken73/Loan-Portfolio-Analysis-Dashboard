-- Schéma de la base de données pour l'analyse de portefeuille de prêts

-- Table principale des prêts
CREATE TABLE IF NOT EXISTS loans (
    -- Identifiants
    id INTEGER PRIMARY KEY,
    
    -- Informations de base du prêt
    loan_amnt REAL NOT NULL,
    funded_amnt REAL,
    funded_amnt_inv REAL,
    term TEXT,
    int_rate REAL,
    installment REAL,
    grade TEXT,
    sub_grade TEXT,
    
    -- Informations de l'emprunteur
    emp_title TEXT,
    emp_length TEXT,
    home_ownership TEXT,
    annual_inc REAL,
    verification_status TEXT,
    issue_d DATE,
    loan_status TEXT,
    
    -- Détails du prêt
    purpose TEXT,
    title TEXT,
    zip_code TEXT,
    addr_state TEXT,
    dti REAL,
    delinq_2yrs INTEGER,
    earliest_cr_line DATE,
    inq_last_6mths INTEGER,
    mths_since_last_delinq INTEGER,
    mths_since_last_record INTEGER,
    open_acc INTEGER,
    pub_rec INTEGER,
    revol_bal REAL,
    revol_util REAL,
    total_acc INTEGER,
    initial_list_status TEXT,
    
    -- Informations de paiement
    out_prncp REAL,
    out_prncp_inv REAL,
    total_pymnt REAL,
    total_pymnt_inv REAL,
    total_rec_prncp REAL,
    total_rec_int REAL,
    total_rec_late_fee REAL,
    recoveries REAL,
    collection_recovery_fee REAL,
    last_pymnt_d DATE,
    last_pymnt_amnt REAL,
    next_pymnt_d DATE,
    last_credit_pull_d DATE,
    
    -- Variables calculées
    is_default INTEGER,
    is_fully_paid INTEGER,
    income_category TEXT,
    loan_to_income_ratio REAL,
    credit_age_years REAL,
    credit_age_category TEXT,
    risk_category TEXT,
    issue_year INTEGER,
    issue_month INTEGER,
    issue_quarter INTEGER,
    issue_season TEXT,
    int_rate_category TEXT,
    
    -- Métadonnées
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table pour les métadonnées des exécutions ETL
CREATE TABLE IF NOT EXISTS etl_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT UNIQUE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    rows_processed INTEGER,
    status TEXT,
    error_message TEXT,
    config_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table pour les KPIs historiques
CREATE TABLE IF NOT EXISTS historical_kpis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    calculation_date DATE,
    kpi_name TEXT,
    kpi_value REAL,
    kpi_description TEXT,
    period TEXT, -- 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(calculation_date, kpi_name, period)
);

-- Table pour les alertes
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type TEXT,
    alert_level TEXT, -- 'info', 'warning', 'critical'
    alert_message TEXT,
    related_table TEXT,
    related_id INTEGER,
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    resolved_by TEXT,
    notes TEXT
);

-- Table pour la configuration du dashboard
CREATE TABLE IF NOT EXISTS dashboard_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    widget_name TEXT UNIQUE,
    widget_type TEXT,
    data_source TEXT,
    refresh_interval INTEGER, -- en secondes
    is_active INTEGER DEFAULT 1,
    config_json TEXT,
    last_refresh TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour optimiser les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_loans_grade ON loans(grade);
CREATE INDEX IF NOT EXISTS idx_loans_loan_status ON loans(loan_status);
CREATE INDEX IF NOT EXISTS idx_loans_issue_date ON loans(issue_d);
CREATE INDEX IF NOT EXISTS idx_loans_is_default ON loans(is_default);
CREATE INDEX IF NOT EXISTS idx_loans_int_rate ON loans(int_rate);
CREATE INDEX IF NOT EXISTS idx_loans_term ON loans(term);
CREATE INDEX IF NOT EXISTS idx_loans_home_ownership ON loans(home_ownership);
CREATE INDEX IF NOT EXISTS idx_loans_purpose ON loans(purpose);
CREATE INDEX IF NOT EXISTS idx_loans_income_category ON loans(income_category);
CREATE INDEX IF NOT EXISTS idx_loans_risk_category ON loans(risk_category);
CREATE INDEX IF NOT EXISTS idx_loans_issue_year_month ON loans(issue_year, issue_month);

-- Index pour les tables de support
CREATE INDEX IF NOT EXISTS idx_historical_kpis_date ON historical_kpis(calculation_date);
CREATE INDEX IF NOT EXISTS idx_historical_kpis_name ON historical_kpis(kpi_name);
CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON alerts(triggered_at);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON alerts(resolved_at);
CREATE INDEX IF NOT EXISTS idx_alerts_level ON alerts(alert_level);

-- Déclencheur pour mettre à jour updated_at
CREATE TRIGGER IF NOT EXISTS update_loans_timestamp 
AFTER UPDATE ON loans
BEGIN
    UPDATE loans SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Vue pour les KPIs principaux (utilisée par le dashboard)
CREATE VIEW IF NOT EXISTS dashboard_kpis AS
SELECT 
    'Portfolio Total' as metric,
    COUNT(*) as value,
    'Nombre total de prêts dans le portefeuille' as description,
    'count' as value_type
FROM loans
UNION ALL
SELECT 
    'Montant Total',
    ROUND(SUM(loan_amnt), 0),
    'Montant total du portefeuille (USD)',
    'currency'
FROM loans
UNION ALL
SELECT 
    'Taux de Défaut',
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
    'Pourcentage de prêts en défaut',
    'percentage'
FROM loans
UNION ALL
SELECT 
    'Taux Intérêt Moyen',
    ROUND(AVG(int_rate), 2),
    'Taux d''intérêt moyen sur tous les prêts',
    'percentage'
FROM loans
UNION ALL
SELECT 
    'Revenu Moyen',
    ROUND(AVG(annual_inc), 0),
    'Revenu annuel moyen des emprunteurs (USD)',
    'currency'
FROM loans
UNION ALL
SELECT 
    'Taux de Remboursement',
    ROUND(SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
    'Pourcentage de prêts entièrement remboursés',
    'percentage'
FROM loans
UNION ALL
SELECT 
    'Ratio Dette/Revenu Moyen',
    ROUND(AVG(dti), 2),
    'Ratio dette/revenu moyen des emprunteurs',
    'ratio'
FROM loans
UNION ALL
SELECT 
    'Durée Moyenne',
    ROUND(AVG(CASE 
        WHEN term LIKE '%36%' THEN 36
        WHEN term LIKE '%60%' THEN 60
        ELSE 36 
    END), 0),
    'Durée moyenne des prêts (mois)',
    'months'
FROM loans;

-- Vue pour l'analyse temporelle
CREATE VIEW IF NOT EXISTS time_series_analysis AS
SELECT 
    issue_year,
    issue_quarter,
    issue_month,
    COUNT(*) as loans_issued,
    SUM(loan_amnt) as total_amount_issued,
    AVG(int_rate) as avg_interest_rate,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as default_count,
    SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) as paid_count,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate,
    ROUND(SUM(CASE WHEN is_fully_paid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as paid_rate
FROM loans
WHERE issue_year IS NOT NULL
GROUP BY issue_year, issue_quarter, issue_month
ORDER BY issue_year DESC, issue_quarter DESC, issue_month DESC;

-- Vue pour l'analyse géographique
CREATE VIEW IF NOT EXISTS geographic_analysis AS
SELECT 
    addr_state as state,
    COUNT(*) as loan_count,
    SUM(loan_amnt) as total_amount,
    AVG(int_rate) as avg_interest_rate,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as default_count,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate,
    ROUND(AVG(annual_inc), 0) as avg_annual_income,
    AVG(dti) as avg_dti
FROM loans
WHERE addr_state IS NOT NULL AND addr_state != ''
GROUP BY addr_state
ORDER BY total_amount DESC;

-- Vue pour l'analyse par objectif de prêt
CREATE VIEW IF NOT EXISTS purpose_analysis AS
SELECT 
    purpose,
    COUNT(*) as loan_count,
    SUM(loan_amnt) as total_amount,
    AVG(int_rate) as avg_interest_rate,
    SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) as default_count,
    ROUND(SUM(CASE WHEN is_default = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as default_rate,
    ROUND(AVG(loan_amnt), 0) as avg_loan_amount
FROM loans
WHERE purpose IS NOT NULL AND purpose != ''
GROUP BY purpose
ORDER BY total_amount DESC;

-- Insertion de données de configuration par défaut
INSERT OR IGNORE INTO dashboard_config (widget_name, widget_type, data_source, refresh_interval, config_json) VALUES
('portfolio_overview', 'scorecard', 'SELECT * FROM dashboard_kpis', 3600, '{"title": "Vue d''ensemble", "columns": 3}'),
('default_by_grade', 'bar_chart', 'SELECT grade, default_rate_percentage FROM loan_default_analysis ORDER BY grade', 1800, '{"title": "Taux de défaut par grade", "x_axis": "Grade", "y_axis": "Taux de défaut (%)"}'),
('monthly_trends', 'line_chart', 'SELECT issue_year || ''-'' || issue_month as period, loans_issued, default_rate FROM time_series_analysis ORDER BY issue_year, issue_month', 1800, '{"title": "Tendances mensuelles", "x_axis": "Période", "y_axis": "Nombre de prêts"}'),
('geographic_distribution', 'map', 'SELECT state, total_amount, default_rate FROM geographic_analysis', 3600, '{"title": "Distribution géographique", "region": "US"}'),
('purpose_analysis', 'pie_chart', 'SELECT purpose, total_amount FROM purpose_analysis LIMIT 10', 1800, '{"title": "Répartition par objectif", "limit": 10}');

-- Insertion des KPIs initiaux
INSERT OR IGNORE INTO historical_kpis (calculation_date, kpi_name, kpi_value, kpi_description, period) 
SELECT 
    DATE('now') as calculation_date,
    metric as kpi_name,
    value as kpi_value,
    description as kpi_description,
    'daily' as period
FROM dashboard_kpis;

-- Message de confirmation
SELECT 'Schéma de base de données créé avec succès!' as status;
