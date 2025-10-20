-- Database schema for token information

-- Table for users (authentication)
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE,
    telegram_id TEXT UNIQUE,
    telegram_username TEXT,
    password_hash TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);

-- Table for email verification codes
CREATE TABLE IF NOT EXISTS verification_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    code TEXT NOT NULL,
    purpose TEXT NOT NULL, -- 'register' or 'login'
    expires_at TIMESTAMP NOT NULL,
    used BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for verification codes
CREATE INDEX IF NOT EXISTS idx_verification_email ON verification_codes(email);
CREATE INDEX IF NOT EXISTS idx_verification_code ON verification_codes(code);
CREATE INDEX IF NOT EXISTS idx_verification_expires ON verification_codes(expires_at);

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

-- Table for user watched tokens (watchlist)
CREATE TABLE IF NOT EXISTS user_watched_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    token_id INTEGER NOT NULL,
    notes TEXT, -- Optional notes the user can add about this token
    interval_1m DECIMAL(10,2), -- Price change threshold for 1 minute interval (percentage)
    interval_5m DECIMAL(10,2), -- Price change threshold for 5 minute interval (percentage)
    interval_15m DECIMAL(10,2), -- Price change threshold for 15 minute interval (percentage)
    interval_1h DECIMAL(10,2), -- Price change threshold for 1 hour interval (percentage)
    alert_active BOOLEAN DEFAULT TRUE, -- Whether alerts are enabled for this watched token
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (token_id) REFERENCES tokens(id) ON DELETE CASCADE
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_watched_user_id ON user_watched_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_watched_token_id ON user_watched_tokens(token_id);
CREATE INDEX IF NOT EXISTS idx_watched_created_at ON user_watched_tokens(created_at);
CREATE INDEX IF NOT EXISTS idx_watched_alert_active ON user_watched_tokens(alert_active);

-- Ensure a user can't watch the same token multiple times
CREATE UNIQUE INDEX IF NOT EXISTS idx_watched_unique ON user_watched_tokens(user_id, token_id);


-- Database schema for crypto pump pool information
CREATE TABLE IF NOT EXISTS pool_info (
	 id INTEGER PRIMARY KEY AUTOINCREMENT,
	 chain_id TEXT NOT NULL,
	 protocol TEXT NOT NULL,
	 pool_address TEXT NOT NULL UNIQUE,
	 pool_name TEXT NOT NULL,
	 token_0_address TEXT NOT NULL,
	 token_0_symbol TEXT NOT NULL,
	 token_0_decimals INTEGER NOT NULL,
	 token_1_address TEXT NOT NULL,
	 token_1_symbol TEXT NOT NULL,
	 token_1_decimals INTEGER NOT NULL,
	 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pool_chain_id ON pool_info(chain_id);
CREATE INDEX IF NOT EXISTS idx_pool_protocol ON pool_info(protocol);
CREATE INDEX IF NOT EXISTS idx_pool_token_0_symbol ON pool_info(token_0_symbol);
CREATE INDEX IF NOT EXISTS idx_pool_token_1_symbol ON pool_info(token_1_symbol);
CREATE INDEX IF NOT EXISTS idx_pool_created_at ON pool_info(created_at);

-- Create unique constraint for chain_id + pool_address combination
CREATE UNIQUE INDEX IF NOT EXISTS idx_pool_unique ON pool_info(chain_id, pool_address);
