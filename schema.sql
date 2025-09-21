CREATE TABLE campaign_clicks (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    campaign_id INTEGER NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    fp_feed_id VARCHAR(50) NOT NULL,
    traffic_source_id INTEGER NOT NULL,
    clicks INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, campaign_id, fp_feed_id)
);

-- Raw feed provider data (imported from CSV)
CREATE TABLE feed_provider_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    fp_feed_id VARCHAR(50) NOT NULL,
    total_searches INTEGER NOT NULL,
    monetized_searches INTEGER NOT NULL,
    paid_clicks INTEGER NOT NULL,
    feed_revenue DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, fp_feed_id)
);

-- Calculated distributed statistics (output of processing)
CREATE TABLE distributed_stats (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    campaign_id INTEGER NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    fp_feed_id VARCHAR(50) NOT NULL,
    traffic_source_id INTEGER NOT NULL,
    total_searches INTEGER NOT NULL,
    monetized_searches INTEGER NOT NULL,
    paid_clicks INTEGER NOT NULL,
    feed_revenue DECIMAL(10,2) NOT NULL,
    pub_revenue DECIMAL(10,2) NOT NULL, -- 75% of feed_revenue
    is_feed_data BOOLEAN DEFAULT true,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, campaign_id, fp_feed_id)
);

-- API keys for authentication
CREATE TABLE api_keys (
    key_id VARCHAR(255) PRIMARY KEY,
    traffic_source_id INTEGER NOT NULL,
    description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test API keys
INSERT INTO api_keys (key_id, traffic_source_id, description) 
VALUES 
    ('test_key_66', 66, 'Test key for traffic source 66'),
    ('test_key_67', 67, 'Test key for traffic source 67');