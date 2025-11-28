-- Initialize database schema
CREATE TABLE IF NOT EXISTS domain_stats (
    id SERIAL PRIMARY KEY,
    account_email TEXT NOT NULL,
    domain_name TEXT NOT NULL,
    status TEXT,
    verified BOOLEAN,
    added_date TEXT,
    timestamp TEXT NOT NULL,
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    delivered_count INTEGER,
    delivered_percentage TEXT,
    complaint_rate REAL,
    complaint_percentage TEXT,
    complaint_trend TEXT,
    time_range TEXT,
    insights_data TEXT,
    full_data TEXT,
    screenshot_path TEXT,
    has_data BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_email, domain_name, date)
);

CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    api_key TEXT UNIQUE NOT NULL,
    name TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scraping_sessions (
    id SERIAL PRIMARY KEY,
    account_email TEXT NOT NULL,
    session_start TEXT NOT NULL,
    session_end TEXT,
    domains_processed INTEGER DEFAULT 0,
    total_domains INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scraping_accounts (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT,
    last_used TIMESTAMP,
    total_sessions INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample API key
INSERT INTO api_keys (api_key, name, is_active) 
VALUES ('test-api-key-12345', 'Default API Key', TRUE)
ON CONFLICT (api_key) DO NOTHING;