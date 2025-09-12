-- Database schema for token information

-- Table for basic token information
CREATE TABLE IF NOT EXISTS tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id TEXT NOT NULL,
    token_address TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    token_name TEXT NOT NULL,
    decimals INTEGER NOT NULL,
    icon_url TEXT,
    daily_volume_usd DECIMAL(30,18) DEFAULT 0,
    volume_updated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_token_chain_address ON tokens(chain_id, token_address);
CREATE INDEX IF NOT EXISTS idx_token_symbol ON tokens(token_symbol);
CREATE UNIQUE INDEX IF NOT EXISTS idx_token_unique ON tokens(chain_id, token_address);