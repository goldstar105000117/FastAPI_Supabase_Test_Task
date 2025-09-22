-- Drop existing tables if they exist
DROP TABLE IF EXISTS distributed_stats CASCADE;
DROP TABLE IF EXISTS campaign_clicks CASCADE;
DROP TABLE IF EXISTS feed_provider_data CASCADE;
DROP TABLE IF EXISTS api_keys CASCADE;
DROP TABLE IF EXISTS processing_logs CASCADE;

-- Raw campaign clicks data (imported from CSV)
CREATE TABLE campaign_clicks (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    campaign_id INTEGER NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    fp_feed_id VARCHAR(50) NOT NULL,
    traffic_source_id INTEGER NOT NULL,
    clicks INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    batch_id VARCHAR(50),
    UNIQUE(date, campaign_id, fp_feed_id)
);

-- API keys for authentication (encrypted storage)
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    key_hash VARCHAR(255) UNIQUE NOT NULL,
    traffic_source_id INTEGER NOT NULL,
    description VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP,
    rate_limit_per_hour INTEGER DEFAULT 1000
);

-- Processing logs for monitoring and debugging
CREATE TABLE processing_logs (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    operation VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL, -- success, error, warning
    message TEXT,
    records_processed INTEGER DEFAULT 0,
    execution_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create comprehensive indexes for performance
CREATE INDEX idx_campaign_clicks_feed_date ON campaign_clicks(fp_feed_id, date);
CREATE INDEX idx_campaign_clicks_traffic_source ON campaign_clicks(traffic_source_id, date);
CREATE INDEX idx_campaign_clicks_campaign ON campaign_clicks(campaign_id, date);

CREATE INDEX idx_feed_provider_data_feed_date ON feed_provider_data(fp_feed_id, date);
CREATE INDEX idx_feed_provider_data_date ON feed_provider_data(date);

CREATE INDEX idx_distributed_stats_traffic_date ON distributed_stats(traffic_source_id, date DESC);
CREATE INDEX idx_distributed_stats_revenue ON distributed_stats(pub_revenue) WHERE pub_revenue > 0;
CREATE INDEX idx_distributed_stats_campaign ON distributed_stats(campaign_id, date DESC);
CREATE INDEX idx_distributed_stats_feed ON distributed_stats(fp_feed_id, date DESC);
CREATE INDEX idx_distributed_stats_batch ON distributed_stats(batch_id);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX idx_api_keys_traffic_source ON api_keys(traffic_source_id) WHERE is_active = true;

CREATE INDEX idx_processing_logs_batch ON processing_logs(batch_id, created_at DESC);
CREATE INDEX idx_processing_logs_status ON processing_logs(status, created_at DESC);

-- Function to hash API keys (use in application)
CREATE OR REPLACE FUNCTION hash_api_key(key_text TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN encode(sha256(key_text::bytea), 'hex');
END;
$$ LANGUAGE plpgsql;

-- Insert hashed test API keys (replace with proper key management in production)
INSERT INTO api_keys (key_hash, traffic_source_id, description) 
VALUES 
    (hash_api_key('test_key_66'), 66, 'Test key for traffic source 66'),
    (hash_api_key('test_key_67'), 67, 'Test key for traffic source 67');

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_campaign_clicks_updated_at BEFORE UPDATE ON campaign_clicks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_feed_provider_data_updated_at BEFORE UPDATE ON feed_provider_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Views for monitoring
CREATE VIEW processing_summary AS
SELECT 
    batch_id,
    MIN(created_at) as started_at,
    MAX(created_at) as completed_at,
    SUM(records_processed) as total_records,
    SUM(execution_time_ms) as total_time_ms,
    COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
    COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count
FROM processing_logs 
GROUP BY batch_id
ORDER BY started_at DESC;

CREATE VIEW api_usage_stats AS
SELECT 
    traffic_source_id,
    COUNT(*) as total_requests,
    MAX(last_used_at) as last_request,
    description
FROM api_keys 
WHERE is_active = true
GROUP BY traffic_source_id, description
ORDER BY total_requests DESC;