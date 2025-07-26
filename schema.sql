-- Database schema for crypto pump pool information
CREATE TABLE IF NOT EXISTS pool_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id TEXT NOT NULL,
    protocol TEXT NOT NULL,
    pool_address TEXT NOT NULL UNIQUE,
    pool_name TEXT NOT NULL,
    cost_token_address TEXT NOT NULL,
    cost_token_symbol TEXT NOT NULL,
    cost_token_decimals INTEGER NOT NULL,
    get_token_address TEXT NOT NULL,
    get_token_symbol TEXT NOT NULL,
    get_token_decimals INTEGER NOT NULL
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pool_chain_id ON pool_info(chain_id);
CREATE INDEX IF NOT EXISTS idx_pool_protocol ON pool_info(protocol);
CREATE INDEX IF NOT EXISTS idx_pool_cost_token_symbol ON pool_info(cost_token_symbol);
CREATE INDEX IF NOT EXISTS idx_pool_get_token_symbol ON pool_info(get_token_symbol);
CREATE INDEX IF NOT EXISTS idx_pool_created_at ON pool_info(created_at);

-- Create unique constraint for chain_id + pool_address combination
CREATE UNIQUE INDEX IF NOT EXISTS idx_pool_unique ON pool_info(chain_id, pool_address);
