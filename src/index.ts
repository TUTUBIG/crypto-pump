import { DurableObject } from 'cloudflare:workers';
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { jwtVerify, SignJWT } from 'jose';

/**
 * WebSocket Gateway using Cloudflare Durable Objects + Pool Information Management
 *
 * This worker acts as a real-time gateway where:
 * - Clients can connect via WebSocket at /ws
 * - External services (Go service) can push data via /publish
 * - All connected clients receive broadcasted trade data
 * - Store and retrieve pool information using D1 database
 */

interface DBQuery {
	sql_template: string;
	sql_arguments: [unknown];
}

interface PoolInfo {
	chain_id: string;
	protocol: string;
	pool_address: string;
	pool_name: string;
	token_0_address: string;
	token_0_symbol: string;
	token_0_decimals: number;
	token_1_address: string;
	token_1_symbol: string;
	token_1_decimals: number;
}

interface PoolInfoWithId extends PoolInfo {
	id: number;
}

interface TokenInfo {
    chain_id: string;
    token_address: string;
    token_symbol: string;
    token_name: string;
    decimals: number;
    icon_url?: string;
    daily_volume_usd?: number;
}

interface WebSocketConnection {
	id: string;
	createdAt: number;
	lastHeartbeat: number;
	tokenId: string;
}

interface User {
	id: number;
	email?: string;
	telegram_id?: string;
	telegram_username?: string;
	google_id?: string;
	apple_id?: string;
	x_id?: string;
	password_hash?: string;
	full_name?: string;
	avatar_url?: string;
	bot_started?: boolean;
	bot_started_at?: string;
	created_at: string;
	updated_at: string;
	last_login_at?: string;
}

interface CustomJWTPayload {
	userId: number;
	email?: string;
	telegram_id?: string;
	iat: number;
	exp: number;
}

interface VerificationCode {
	id: number;
	email: string;
	code: string;
	purpose: 'register' | 'login';
	expires_at: string;
	used: boolean;
	created_at: string;
}

interface RefreshTokenData {
	token: string;
	userId: number;
	email?: string;
	telegram_id?: string;
	expiresAt: number;
	createdAt: number;
}

interface WatchedToken {
	id: number;
	user_id: number;
	token_id: number;
	notes?: string;
	interval_1m?: number;
	interval_5m?: number;
	interval_15m?: number;
	interval_1h?: number;
	alert_active: boolean;
	created_at: string;
}

interface WatchedTokenWithDetails extends WatchedToken {
	chain_id: string;
	token_address: string;
	token_symbol: string;
	token_name: string;
	decimals: number;
	icon_url?: string;
}

// Database helper functions
async function addPool(db: D1Database, poolData: PoolInfo): Promise<{ success: boolean; id: number }> {
	const result = await db.prepare(`
		INSERT INTO pool_info (chain_id, protocol, pool_address, pool_name,token_0_address, token_0_symbol, token_0_decimals,token_1_address, token_1_symbol, token_1_decimals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`).bind(
		poolData.chain_id,
		poolData.protocol,
		poolData.pool_address,
		poolData.pool_name,
		poolData.token_0_address,
		poolData.token_0_symbol,
		poolData.token_0_decimals,
		poolData.token_1_address,
		poolData.token_1_symbol,
		poolData.token_1_decimals,
	).run();

	if (!result.success) {
		throw new Error('Failed to insert pool data');
	}

	return {
		success: true,
		id: Number(result.meta.last_row_id) || 0
	};
}

async function getPool(
	db: D1Database,
	chainId: string,
	protocol: string,
	poolAddress: string
): Promise<PoolInfoWithId | null> {
	const query = `
		SELECT
			id, chain_id, protocol, pool_address, pool_name,
			token_0_address, token_0_symbol, token_0_decimals,
			token_1_address, token_1_symbol, token_1_decimals
		FROM pool_info
		WHERE chain_id = ? AND protocol = ? AND pool_address = ?
		LIMIT 1
	`;
	const result = await db.prepare(query)
		.bind(chainId, protocol, poolAddress)
		.first();

	if (!result) {
		return null;
	}

	return {
		id: Number(result.id),
		chain_id: String(result.chain_id),
		protocol: String(result.protocol),
		pool_address: String(result.pool_address),
		pool_name: String(result.pool_name),
		token_0_address: String(result.token_0_address),
		token_0_symbol: String(result.token_0_symbol),
		token_0_decimals: Number(result.token_0_decimals),
		token_1_address: String(result.token_1_address),
		token_1_symbol: String(result.token_1_symbol),
		token_1_decimals: Number(result.token_1_decimals),
	}
}

async function listPools(
	db: D1Database,
	page: number = 1,
	pageSize: number = 20,
	chainId?: string | null,
	protocol?: string | null,
	poolAddress?: string | null,
): Promise<PoolInfoWithId[]> {
	const offset = (page - 1) * pageSize;

	// Build WHERE clause dynamically
	let whereClause = '';
	const bindings: any[] = [];

	if (chainId) {
		whereClause += ' WHERE chain_id = ?';
		bindings.push(chainId);
	}

	if (protocol) {
		whereClause += chainId ? ' AND protocol = ?' : ' WHERE protocol = ?';
		bindings.push(protocol);
	}

	if (poolAddress) {
		whereClause += (chainId || protocol) ? ' AND pool_address = ?' : ' WHERE pool_address = ?';
		bindings.push(poolAddress);
	}

	// Get total count
	const countQuery = `SELECT COUNT(*) as count FROM pool_info ${whereClause}`;
	const countResult = await db.prepare(countQuery).bind(...bindings).first();
	const total = Number(countResult?.count) || 0;

	// Get paginated results
	const dataQuery = `
		SELECT
			id, chain_id, protocol, pool_address, pool_name,
			token_0_address, token_0_symbol, token_0_decimals,
			token_1_address, token_1_symbol, token_1_decimals
		FROM pool_info ${whereClause}
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`;

	const dataResult = await db.prepare(dataQuery)
		.bind(...bindings, pageSize, offset)
		.all();

	const pools: PoolInfoWithId[] = dataResult.results.map((row: any) => ({
		id: row.id,
		chain_id: row.chain_id,
		protocol: row.protocol,
		pool_address: row.pool_address,
		pool_name: row.pool_name,
		token_0_address: row.token_0_address,
		token_0_symbol: row.token_0_symbol,
		token_0_decimals: row.token_0_decimals,
		token_1_address: row.token_1_address,
		token_1_symbol: row.token_1_symbol,
		token_1_decimals: row.token_1_decimals
	}));

	const totalPages = Math.ceil(total / pageSize);

	return pools;
}

async function addToken(db: D1Database, tokenData: TokenInfo): Promise<{ success: boolean; id: number }> {
    const result = await db.prepare(`
        INSERT INTO tokens (chain_id, token_address, token_symbol, token_name, decimals, icon_url, daily_volume_usd) VALUES (?, ?, ?, ?, ?, ?, ?)
    `).bind(
        tokenData.chain_id,
        tokenData.token_address,
        tokenData.token_symbol,
        tokenData.token_name,
        tokenData.decimals,
        tokenData.icon_url || '',
        tokenData.daily_volume_usd || 0,
    ).run();

    if (!result.success) {
        throw new Error('Failed to insert token data');
    }

    return {
        success: true,
        id: Number(result.meta.last_row_id) || 0
    };
}

async function getToken(
    db: D1Database,
    chainId: string,
    tokenAddress: string
): Promise<TokenInfo> {
    const query = `
        SELECT id, chain_id, token_address, token_symbol, token_name, decimals,
               icon_url, daily_volume_usd, volume_updated_at, created_at, updated_at
        FROM tokens
        WHERE chain_id = ? AND token_address = ?
        LIMIT 1
    `;
    const result: any = await db.prepare(query)
        .bind(chainId, tokenAddress)
        .first();

    if (!result) {
        throw new Error('Token not found');
    }

    return {
		chain_id: result.chain_id,
		daily_volume_usd: result.daily_volume_usd,
		decimals: result.decimals,
		icon_url: result.icon_url,
		token_address: result.token_address,
		token_name: result.token_name,
		token_symbol: result.token_symbol,
	};
}

async function listTokens(
    db: D1Database,
    page: number = 1,
    pageSize: number = 20,
    chainId?: string,
    search?: string
): Promise<TokenInfo[]> {
    // Build query parts
    let whereClause = '';
    const bindings: any[] = [];

    if (chainId) {
        whereClause = 'WHERE chain_id = ?';
        bindings.push(chainId);
    }

    if (search) {
        // Check if search param is a token address format (starts with 0x)
        const isTokenAddress = search.toLowerCase().startsWith('0x');

        if (isTokenAddress) {
            // Precise search on token_address
            whereClause += chainId ? ' AND LOWER(token_address) = ?' : 'WHERE LOWER(token_address) = ?';
            bindings.push(search.toLowerCase());
        } else {
            // Fuzzy search on token_name
            whereClause += chainId ? ' AND LOWER(token_name) LIKE ?' : 'WHERE LOWER(token_name) LIKE ?';
            bindings.push(`%${search.toLowerCase()}%`);
        }
    }

    // Get total count
    const countResult = await db.prepare(
        `SELECT COUNT(*) as count FROM tokens ${whereClause}`
    ).bind(...bindings).first();

    const total = Number(countResult?.count) || 0;

    // Get paginated results
    const offset = (page - 1) * pageSize;
    bindings.push(pageSize, offset);

    const result = await db.prepare(`
        SELECT id, chain_id, token_address, token_symbol, token_name, decimals,
               icon_url, daily_volume_usd, volume_updated_at, created_at, updated_at
        FROM tokens
        ${whereClause}
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    `).bind(...bindings).all();

	return result.results.map((row: any) => ({
			id: row.id,
			chain_id: row.chain_id,
			token_address: row.token_address,
			token_symbol: row.token_symbol,
			token_name: row.token_name,
			decimals: row.decimals,
			icon_url: row.icon_url,
			daily_volume_usd: row.daily_volume_usd,
			volume_updated_at: row.volume_updated_at,
			created_at: row.created_at,
			updated_at: row.updated_at
		}));
}

async function deleteToken(
    db: D1Database,
    chainId: string,
    tokenAddress: string
): Promise<boolean> {
    const result = await db.prepare(
        'DELETE FROM tokens WHERE chain_id = ? AND token_address = ?'
    ).bind(chainId, tokenAddress).run();
    return result.success;
}

/**
 * Add a tag to a token
 * @param db - Database instance
 * @param tokenId - Token ID
 * @param tag - Tag name
 */
async function addTokenTag(db: D1Database, tokenId: number, tag: string): Promise<{ success: boolean; id?: number }> {
    try {
        const result = await db.prepare(`
            INSERT INTO token_tags (token_id, tag) VALUES (?, ?)
        `).bind(tokenId, tag).run();

        return {
            success: result.success,
            id: result.success ? Number(result.meta.last_row_id) : undefined
        };
    } catch (error: any) {
        // Handle unique constraint violation (tag already exists for this token)
        if (error.message?.includes('UNIQUE constraint failed')) {
            return { success: true }; // Already has this tag, treat as success
        }
        throw error;
    }
}

/**
 * Remove a tag from a token
 * @param db - Database instance
 * @param tokenId - Token ID
 * @param tag - Tag name
 */
async function removeTokenTag(db: D1Database, tokenId: number, tag: string): Promise<boolean> {
    const result = await db.prepare(
        'DELETE FROM token_tags WHERE token_id = ? AND tag = ?'
    ).bind(tokenId, tag).run();
    return result.success;
}

/**
 * Remove all tags from a token
 * @param db - Database instance
 * @param tokenId - Token ID
 */
async function removeAllTokenTags(db: D1Database, tokenId: number): Promise<boolean> {
    const result = await db.prepare(
        'DELETE FROM token_tags WHERE token_id = ?'
    ).bind(tokenId).run();
    return result.success;
}

/**
 * Get all tags for a token
 * @param db - Database instance
 * @param tokenId - Token ID
 */
async function getTokenTags(db: D1Database, tokenId: number): Promise<string[]> {
    const result = await db.prepare(
        'SELECT tag FROM token_tags WHERE token_id = ? ORDER BY created_at DESC'
    ).bind(tokenId).all();

    return result.results.map((row: any) => String(row.tag));
}

/**
 * Get tokens by tag
 * @param db - Database instance
 * @param tag - Tag name
 * @param limit - Number of tokens to return
 * @param chainId - Optional chain ID filter
 */
async function getTokensByTag(
    db: D1Database,
    tag: string,
    limit: number = 20,
    chainId?: string
): Promise<any[]> {
    let whereClause = '';
    const bindings: any[] = [tag];

    if (chainId) {
        whereClause = 'AND t.chain_id = ?';
        bindings.push(chainId);
    }

    bindings.push(limit);

    const query = `
        SELECT
            t.id,
            t.chain_id,
            t.token_address,
            t.token_symbol,
            t.token_name,
            t.decimals,
            t.icon_url,
            t.daily_volume_usd,
            t.volume_updated_at,
            t.created_at,
            t.updated_at,
            tt.created_at as tagged_at
        FROM token_tags tt
        INNER JOIN tokens t ON tt.token_id = t.id
        WHERE tt.tag = ?
        ${whereClause}
        ORDER BY tt.created_at DESC
        LIMIT ?
    `;

    const result = await db.prepare(query).bind(...bindings).all();

    return result.results.map((row: any) => ({
        id: Number(row.id),
        chain_id: String(row.chain_id),
        token_address: String(row.token_address),
        token_symbol: String(row.token_symbol),
        token_name: String(row.token_name),
        decimals: Number(row.decimals),
        icon_url: row.icon_url ? String(row.icon_url) : undefined,
        daily_volume_usd: row.daily_volume_usd ? Number(row.daily_volume_usd) : 0,
        volume_updated_at: row.volume_updated_at ? String(row.volume_updated_at) : undefined,
        created_at: String(row.created_at),
        updated_at: String(row.updated_at),
        tagged_at: String(row.tagged_at)
    }));
}

/**
 * Get token ID by chain_id and token_address
 * @param db - Database instance
 * @param chainId - Chain ID
 * @param tokenAddress - Token address
 */
async function getTokenId(db: D1Database, chainId: string, tokenAddress: string): Promise<number | null> {
    const result: any = await db.prepare(
        'SELECT id FROM tokens WHERE chain_id = ? AND token_address = ? LIMIT 1'
    ).bind(chainId, tokenAddress).first();

    return result ? Number(result.id) : null;
}

async function updateTokenVolume(
    db: D1Database,
    chainId: string,
    tokenAddress: string,
    volumeUsd: string
): Promise<boolean> {
    const result = await db.prepare(`
        UPDATE tokens
        SET daily_volume_usd = ?,
            volume_updated_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE chain_id = ? AND token_address = ?
    `).bind(volumeUsd, chainId, tokenAddress).run();
    return result.success;
}

// Watched Token Validation
function validateIntervalThresholds(
    interval_1m?: number | null,
    interval_5m?: number | null,
    interval_15m?: number | null,
    interval_1h?: number | null
): { valid: boolean; error?: string } {
    // Collect intervals with their names
    const intervals: Array<{ name: string; value: number; absValue: number }> = [];

    if (interval_1m !== undefined && interval_1m !== null) {
        intervals.push({ name: '1m', value: interval_1m, absValue: Math.abs(interval_1m) });
    }
    if (interval_5m !== undefined && interval_5m !== null) {
        intervals.push({ name: '5m', value: interval_5m, absValue: Math.abs(interval_5m) });
    }
    if (interval_15m !== undefined && interval_15m !== null) {
        intervals.push({ name: '15m', value: interval_15m, absValue: Math.abs(interval_15m) });
    }
    if (interval_1h !== undefined && interval_1h !== null) {
        intervals.push({ name: '1h', value: interval_1h, absValue: Math.abs(interval_1h) });
    }

    // Validate that abs(larger interval) >= abs(smaller interval)
    // Order: 1m < 5m < 15m < 1h
    const timeOrder = ['1m', '5m', '15m', '1h'];
    const intervalMap = new Map(intervals.map(i => [i.name, i]));

    for (let i = 0; i < timeOrder.length - 1; i++) {
        const smallerInterval = intervalMap.get(timeOrder[i]);

        // Find the next larger interval that exists
        for (let j = i + 1; j < timeOrder.length; j++) {
            const largerInterval = intervalMap.get(timeOrder[j]);

            if (smallerInterval && largerInterval) {
                if (largerInterval.absValue < smallerInterval.absValue) {
                    return {
                        valid: false,
                        error: `Absolute value of interval_${largerInterval.name} (${Math.abs(largerInterval.value)}) must be >= interval_${smallerInterval.name} (${Math.abs(smallerInterval.value)})`
                    };
                }
                break; // Only check the next existing larger interval
            }
        }
    }

    return { valid: true };
}

// Watched Token Database Functions
async function addWatchedToken(
    db: D1Database,
    userId: number,
    tokenId: number,
    notes?: string,
    interval1m?: number,
    interval5m?: number,
    interval15m?: number,
    interval1h?: number,
    alertActive: boolean = true
): Promise<{ success: boolean; id: number }> {
    const result = await db.prepare(`
        INSERT INTO user_watched_tokens
        (user_id, token_id, notes, interval_1m, interval_5m, interval_15m, interval_1h, alert_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
        userId,
        tokenId,
        notes || null,
        interval1m || null,
        interval5m || null,
        interval15m || null,
        interval1h || null,
        alertActive ? 1 : 0
    ).run();

    if (!result.success) {
        throw new Error('Failed to add watched token');
    }

    return {
        success: true,
        id: Number(result.meta.last_row_id) || 0
    };
}

async function getWatchedTokensByUserId(
    db: D1Database,
    userId: number
): Promise<WatchedTokenWithDetails[]> {
    const query = `
        SELECT
            wt.id,
            wt.user_id,
            wt.token_id,
            wt.notes,
            wt.interval_1m,
            wt.interval_5m,
            wt.interval_15m,
            wt.interval_1h,
            wt.alert_active,
            wt.created_at,
            t.chain_id,
            t.token_address,
            t.token_symbol,
            t.token_name,
            t.decimals,
            t.icon_url
        FROM user_watched_tokens wt
        INNER JOIN tokens t ON wt.token_id = t.id
        WHERE wt.user_id = ?
        ORDER BY wt.created_at DESC
    `;

    const result = await db.prepare(query).bind(userId).all();

    return result.results.map((row: any) => ({
        id: Number(row.id),
        user_id: Number(row.user_id),
        token_id: Number(row.token_id),
        notes: row.notes,
        interval_1m: row.interval_1m ? Number(row.interval_1m) : undefined,
        interval_5m: row.interval_5m ? Number(row.interval_5m) : undefined,
        interval_15m: row.interval_15m ? Number(row.interval_15m) : undefined,
        interval_1h: row.interval_1h ? Number(row.interval_1h) : undefined,
        alert_active: Boolean(row.alert_active),
        created_at: String(row.created_at),
        chain_id: String(row.chain_id),
        token_address: String(row.token_address),
        token_symbol: String(row.token_symbol),
        token_name: String(row.token_name),
        decimals: Number(row.decimals),
        icon_url: row.icon_url ? String(row.icon_url) : undefined
    }));
}

async function getWatchedTokenById(
    db: D1Database,
    id: number,
    userId: number
): Promise<WatchedTokenWithDetails | null> {
    const query = `
        SELECT
            wt.id,
            wt.user_id,
            wt.token_id,
            wt.notes,
            wt.interval_1m,
            wt.interval_5m,
            wt.interval_15m,
            wt.interval_1h,
            wt.alert_active,
            wt.created_at,
            t.chain_id,
            t.token_address,
            t.token_symbol,
            t.token_name,
            t.decimals,
            t.icon_url
        FROM user_watched_tokens wt
        INNER JOIN tokens t ON wt.token_id = t.id
        WHERE wt.id = ? AND wt.user_id = ?
        LIMIT 1
    `;

    const result = await db.prepare(query).bind(id, userId).first();

    if (!result) {
        return null;
    }

    return {
        id: Number(result.id),
        user_id: Number(result.user_id),
        token_id: Number(result.token_id),
        notes: result.notes as string | undefined,
        interval_1m: result.interval_1m ? Number(result.interval_1m) : undefined,
        interval_5m: result.interval_5m ? Number(result.interval_5m) : undefined,
        interval_15m: result.interval_15m ? Number(result.interval_15m) : undefined,
        interval_1h: result.interval_1h ? Number(result.interval_1h) : undefined,
        alert_active: Boolean(result.alert_active),
        created_at: String(result.created_at),
        chain_id: String(result.chain_id),
        token_address: String(result.token_address),
        token_symbol: String(result.token_symbol),
        token_name: String(result.token_name),
        decimals: Number(result.decimals),
        icon_url: result.icon_url ? String(result.icon_url) : undefined
    };
}

async function updateWatchedToken(
    db: D1Database,
    id: number,
    userId: number,
    updates: {
        notes?: string;
        interval_1m?: number | null;
        interval_5m?: number | null;
        interval_15m?: number | null;
        interval_1h?: number | null;
        alert_active?: boolean;
    }
): Promise<boolean> {
    const setParts: string[] = [];
    const values: any[] = [];

    if (updates.notes !== undefined) {
        setParts.push('notes = ?');
        values.push(updates.notes || null);
    }
    if (updates.interval_1m !== undefined) {
        setParts.push('interval_1m = ?');
        values.push(updates.interval_1m);
    }
    if (updates.interval_5m !== undefined) {
        setParts.push('interval_5m = ?');
        values.push(updates.interval_5m);
    }
    if (updates.interval_15m !== undefined) {
        setParts.push('interval_15m = ?');
        values.push(updates.interval_15m);
    }
    if (updates.interval_1h !== undefined) {
        setParts.push('interval_1h = ?');
        values.push(updates.interval_1h);
    }
    if (updates.alert_active !== undefined) {
        setParts.push('alert_active = ?');
        values.push(updates.alert_active ? 1 : 0);
    }

    if (setParts.length === 0) {
        return false; // No updates provided
    }

    values.push(id, userId);

    const query = `
        UPDATE user_watched_tokens
        SET ${setParts.join(', ')}
        WHERE id = ? AND user_id = ?
    `;

    const result = await db.prepare(query).bind(...values).run();
    return result.success;
}

async function deleteWatchedToken(
    db: D1Database,
    id: number,
    userId: number
): Promise<boolean> {
    const result = await db.prepare(
        'DELETE FROM user_watched_tokens WHERE id = ? AND user_id = ?'
    ).bind(id, userId).run();
    return result.success;
}

async function getActiveWatchedTokens(
    db: D1Database
): Promise<WatchedTokenWithDetails[]> {
    const query = `
        SELECT
            wt.id,
            wt.user_id,
            wt.token_id,
            wt.notes,
            wt.interval_1m,
            wt.interval_5m,
            wt.interval_15m,
            wt.interval_1h,
            wt.alert_active,
            wt.created_at,
            t.chain_id,
            t.token_address,
            t.token_symbol,
            t.token_name,
            t.decimals,
            t.icon_url
        FROM user_watched_tokens wt
        INNER JOIN tokens t ON wt.token_id = t.id
        WHERE wt.alert_active = 1
        ORDER BY wt.created_at DESC
    `;

    const result = await db.prepare(query).all();

    return result.results.map((row: any) => ({
        id: Number(row.id),
        user_id: Number(row.user_id),
        token_id: Number(row.token_id),
        notes: row.notes,
        interval_1m: row.interval_1m ? Number(row.interval_1m) : undefined,
        interval_5m: row.interval_5m ? Number(row.interval_5m) : undefined,
        interval_15m: row.interval_15m ? Number(row.interval_15m) : undefined,
        interval_1h: row.interval_1h ? Number(row.interval_1h) : undefined,
        alert_active: Boolean(row.alert_active),
        created_at: String(row.created_at),
        chain_id: String(row.chain_id),
        token_address: String(row.token_address),
        token_symbol: String(row.token_symbol),
        token_name: String(row.token_name),
        decimals: Number(row.decimals),
        icon_url: row.icon_url ? String(row.icon_url) : undefined
    }));
}

// Telegram Authentication Utilities
async function verifyTelegramAuth(authData: any, botToken: string): Promise<{ valid: boolean; error?: string }> {
	try {
		// 1. Validate required fields
		if (!authData.hash) {
			console.error('No hash provided in Telegram auth data');
			return { valid: false, error: 'Missing hash' };
		}

		if (!authData.auth_date) {
			console.error('No auth_date provided in Telegram auth data');
			return { valid: false, error: 'Missing auth_date' };
		}

		// 2. Check if auth_date is out of date
		const authDate = parseInt(authData.auth_date);
		const now = Math.floor(Date.now() / 1000);
		const maxAge = 60; // 1 minutes in seconds

		if (isNaN(authDate)) {
			console.error('Invalid auth_date format');
			return { valid: false, error: 'Invalid auth_date' };
		}

		if (now - authDate > maxAge) {
			console.error('Auth data expired:', { authDate, now, diff: now - authDate });
			return { valid: false, error: 'Authentication data expired (older than 1 minutes)' };
		}

		// 3. Prepare data check string
		// Extract hash and create data_check_string from remaining fields
		const { hash, ...dataToCheck } = authData;

		// Data-check-string: concatenation of all fields sorted alphabetically
		// Format: key=<value>\nkey=<value> (line feed 0x0A as separator)
		const dataCheckString = Object.keys(dataToCheck)
			.sort()
			.map(key => `${key}=${dataToCheck[key]}`)
			.join('\n');

		console.log('Data check string:', dataCheckString);

		// 4. Calculate hash and verify
		// Step 4.1: Compute secret_key = SHA256(bot_token)
		const encoder = new TextEncoder();
		const botTokenData = encoder.encode(botToken);
		const secretKeyBuffer = await crypto.subtle.digest('SHA-256', botTokenData);

		// Step 4.2: Compute HMAC-SHA256(data_check_string, secret_key)
		const secretKey = await crypto.subtle.importKey(
			'raw',
			secretKeyBuffer,
			{ name: 'HMAC', hash: 'SHA-256' },
			false,
			['sign']
		);

		const dataCheckStringData = encoder.encode(dataCheckString);
		const signatureBuffer = await crypto.subtle.sign(
			'HMAC',
			secretKey,
			dataCheckStringData
		);

		// Step 4.3: Convert to hex string
		const signatureArray = Array.from(new Uint8Array(signatureBuffer));
		const computedHash = signatureArray.map(b => b.toString(16).padStart(2, '0')).join('');

		console.log('Computed hash:', computedHash);
		console.log('Provided hash:', hash);

		// Step 4.4: Compare computed hash with provided hash
		if (computedHash !== hash) {
			console.error('Hash mismatch - authentication failed');
			return { valid: false, error: 'Invalid hash signature' };
		}

		return { valid: true };
	} catch (error) {
		console.error('Error verifying Telegram auth:', error);
		return { valid: false, error: 'Verification failed' };
	}
}

// JWT Utilities
async function hashPassword(password: string): Promise<string> {
	const encoder = new TextEncoder();
	const data = encoder.encode(password);
	const hashBuffer = await crypto.subtle.digest('SHA-256', data);
	const hashArray = Array.from(new Uint8Array(hashBuffer));
	return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

async function verifyPassword(password: string, hash: string): Promise<boolean> {
	const passwordHash = await hashPassword(password);
	return passwordHash === hash;
}

async function generateJWT(payload: { userId: number; email?: string; telegram_id?: string }, secret: string): Promise<string> {
	const encoder = new TextEncoder();
	const secretKey = encoder.encode(secret);

	const jwt = await new SignJWT({ ...payload })
		.setProtectedHeader({ alg: 'HS256' })
		.setIssuedAt()
		.setExpirationTime('30m') // Token expires in 30 minutes
		.sign(secretKey);

	return jwt;
}

function generateRefreshToken(): string {
	// Generate a secure random refresh token
	const array = new Uint8Array(32);
	crypto.getRandomValues(array);
	return Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('');
}

async function verifyJWT(token: string, secret: string): Promise<CustomJWTPayload | null> {
	try {
		const encoder = new TextEncoder();
		const secretKey = encoder.encode(secret);

		const { payload } = await jwtVerify(token, secretKey);
		return payload as unknown as CustomJWTPayload;
	} catch (error) {
		console.error('JWT verification failed:', error);
		return null;
	}
}

// Authentication middleware
async function authenticateUser(c: any): Promise<User | null> {
	const authHeader = c.req.header('Authorization');
	if (!authHeader || !authHeader.startsWith('Bearer ')) {
		return null;
	}

	const token = authHeader.substring(7);
	const jwtSecret = c.env.JWT_SECRET || 'default-secret-change-in-production';
	const payload = await verifyJWT(token, jwtSecret);

	if (!payload) {
		return null;
	}

	// Fetch user from database
	const result = await c.env.DB.prepare(
		'SELECT * FROM users WHERE id = ?'
	).bind(payload.userId).first();

	if (!result) {
		return null;
	}

	return result as User;
}

// User database functions
async function createUserWithEmail(
	db: D1Database,
	email: string,
	password: string
): Promise<{ success: boolean; userId?: number; error?: string }> {
	try {
		const passwordHash = await hashPassword(password);
		const result = await db.prepare(`
			INSERT INTO users (email, password_hash, last_login_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)
		`).bind(email, passwordHash).run();

		if (!result.success) {
			return { success: false, error: 'Failed to create user' };
		}

		return {
			success: true,
			userId: Number(result.meta.last_row_id)
		};
	} catch (error) {
		if (error instanceof Error && error.message.includes('UNIQUE constraint failed')) {
			return { success: false, error: 'Email already exists' };
		}
		return { success: false, error: 'Database error' };
	}
}

async function getUserByEmail(db: D1Database, email: string): Promise<User | null> {
	const result = await db.prepare(
		'SELECT * FROM users WHERE email = ?'
	).bind(email).first();

	return result as User | null;
}

async function updateUserPassword(
	db: D1Database,
	email: string,
	newPassword: string
): Promise<{ success: boolean; error?: string }> {
	try {
		const passwordHash = await hashPassword(newPassword);
		const result = await db.prepare(`
			UPDATE users
			SET password_hash = ?, updated_at = CURRENT_TIMESTAMP
			WHERE email = ?
		`).bind(passwordHash, email).run();

		if (!result.success) {
			return { success: false, error: 'Failed to update password' };
		}

		if (result.meta.changes === 0) {
			return { success: false, error: 'User not found' };
		}

		return { success: true };
	} catch (error) {
		console.error('Error updating password:', error);
		return { success: false, error: 'Database error' };
	}
}

async function createUserWithTelegram(
	db: D1Database,
	telegramId: string,
	telegramUsername?: string
): Promise<{ success: boolean; userId?: number; error?: string }> {
	try {
		const result = await db.prepare(`
			INSERT INTO users (telegram_id, telegram_username, last_login_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)
		`).bind(telegramId, telegramUsername || null).run();

		if (!result.success) {
			return { success: false, error: 'Failed to create user' };
		}

		return {
			success: true,
			userId: Number(result.meta.last_row_id)
		};
	} catch (error) {
		if (error instanceof Error && error.message.includes('UNIQUE constraint failed')) {
			return { success: false, error: 'Telegram account already linked' };
		}
		return { success: false, error: 'Database error' };
	}
}

async function getUserByTelegramId(db: D1Database, telegramId: string): Promise<User | null> {
	const result = await db.prepare(
		'SELECT * FROM users WHERE telegram_id = ?'
	).bind(telegramId).first();

	return result as User | null;
}

async function updateUserLastLogin(db: D1Database, userId: number): Promise<boolean> {
	const result = await db.prepare(`
		UPDATE users
		SET last_login_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`).bind(userId).run();

	return result.success;
}

async function bindTelegramToUser(
	db: D1Database,
	userId: number,
	telegramId: string,
	telegramUsername?: string
): Promise<{ success: boolean; error?: string }> {
	try {
		// Check if Telegram ID is already linked to another account
		const existingUser = await getUserByTelegramId(db, telegramId);
		if (existingUser && existingUser.id !== userId) {
			return {
				success: false,
				error: 'This Telegram account is already linked to another user'
			};
		}

		// Update user with Telegram info
		const result = await db.prepare(`
			UPDATE users
			SET telegram_id = ?,
			    telegram_username = ?,
			    updated_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`).bind(telegramId, telegramUsername || null, userId).run();

		if (!result.success) {
			return { success: false, error: 'Failed to bind Telegram account' };
		}

		return { success: true };
	} catch (error) {
		console.error('Error binding Telegram to user:', error);
		return { success: false, error: 'Database error' };
	}
}

// Verification Code Functions using Durable Object
async function createVerificationCodeDurable(
	env: any,
	email: string,
	purpose: 'register' | 'login' | 'reset-password'
): Promise<{ success: boolean; code?: string; error?: string }> {
	try {
		const codeStore = env.VERIFICATION_STORE.get(env.VERIFICATION_STORE.idFromName("codes"));
		const response = await codeStore.fetch(new Request('http://localhost/create', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ email, purpose })
		}));

		const result = await response.json() as { success: boolean; code?: string; error?: string };
		return result;
	} catch (error) {
		console.error('Error creating verification code:', error);
		return { success: false, error: 'Failed to create verification code' };
	}
}

async function verifyCodeDurable(
	env: any,
	email: string,
	code: string,
	purpose: 'register' | 'login' | 'reset-password'
): Promise<{ valid: boolean; error?: string }> {
	try {
		const codeStore = env.VERIFICATION_STORE.get(env.VERIFICATION_STORE.idFromName("codes"));
		const response = await codeStore.fetch(new Request('http://localhost/verify', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ email, code, purpose })
		}));

		const result = await response.json() as { valid: boolean; error?: string };
		return result;
	} catch (error) {
		console.error('Error verifying code:', error);
		return { valid: false, error: 'Verification failed' };
	}
}

// Refresh Token Functions using Durable Object
async function createRefreshTokenDurable(
	env: any,
	userId: number,
	email?: string,
	telegram_id?: string
): Promise<{ success: boolean; token?: string; error?: string }> {
	try {
		const tokenStore = env.REFRESH_TOKEN_STORE.get(env.REFRESH_TOKEN_STORE.idFromName("tokens"));
		const response = await tokenStore.fetch(new Request('http://localhost/create', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ userId, email, telegram_id })
		}));

		return await response.json() as { success: boolean; token?: string; error?: string };
	} catch (error) {
		console.error('Error creating refresh token:', error);
		return { success: false, error: 'Failed to create refresh token' };
	}
}

async function verifyRefreshTokenDurable(
	env: any,
	token: string
): Promise<{ valid: boolean; userId?: number; email?: string; telegram_id?: string; error?: string }> {
	try {
		const tokenStore = env.REFRESH_TOKEN_STORE.get(env.REFRESH_TOKEN_STORE.idFromName("tokens"));
		const response = await tokenStore.fetch(new Request('http://localhost/verify', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ token })
		}));

		const result = await response.json() as { valid: boolean; userId?: number; email?: string; telegram_id?: string; error?: string };
		return result;
	} catch (error) {
		console.error('Error verifying refresh token:', error);
		return { valid: false, error: 'Verification failed' };
	}
}

async function revokeRefreshTokenDurable(
	env: any,
	token: string
): Promise<{ success: boolean; error?: string }> {
	try {
		const tokenStore = env.REFRESH_TOKEN_STORE.get(env.REFRESH_TOKEN_STORE.idFromName("tokens"));
		const response = await tokenStore.fetch(new Request('http://localhost/revoke', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ token })
		}));

		const result = await response.json() as { success: boolean; error?: string };
		return result;
	} catch (error) {
		console.error('Error revoking refresh token:', error);
		return { success: false, error: 'Revocation failed' };
	}
}

async function sendVerificationEmail(
	email: string,
	code: string,
	purpose: string,
	emailApiKey?: string,
	senderEmail?: string,
	senderName?: string
): Promise<{ success: boolean; error?: string }> {
	// If no email API key is configured, just log the code (for development)
	if (!emailApiKey) {
		console.log(`[DEV] Verification code for ${email}: ${code}`);
		return { success: true };
	}

	try {
		// Using Brevo (formerly Sendinblue) API
		const subject = purpose === 'register'
			? 'Verify Your Email'
			: purpose === 'reset-password'
				? 'Reset Your Password'
				: 'Your Login Code';
		const htmlContent = `
			<!DOCTYPE html>
			<html>
			<head>
				<meta charset="UTF-8">
				<meta name="viewport" content="width=device-width, initial-scale=1.0">
				<style>
					body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
					.container { max-width: 600px; margin: 0 auto; padding: 20px; }
					.header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }
					.content { background: #f9f9f9; padding: 30px; border-radius: 0 0 10px 10px; }
					.code-box { background: white; border: 2px dashed #667eea; border-radius: 8px; padding: 20px; text-align: center; margin: 20px 0; }
					.code { font-size: 32px; letter-spacing: 8px; font-weight: bold; color: #667eea; font-family: 'Courier New', monospace; }
					.footer { text-align: center; margin-top: 20px; color: #666; font-size: 12px; }
				</style>
			</head>
			<body>
				<div class="container">
					<div class="header">
						<h1>${subject}</h1>
					</div>
					<div class="content">
						<p>Hello,</p>
						<p>Your verification code is:</p>
						<div class="code-box">
							<div class="code">${code}</div>
						</div>
						<p><strong>This code will expire in 10 minutes.</strong></p>
						<p>If you didn't request this code, please ignore this email.</p>
						<div class="footer">
							<p>This is an automated message, please do not reply.</p>
						</div>
					</div>
				</div>
			</body>
			</html>
		`;

		console.log(emailApiKey);

		const response = await fetch('https://api.brevo.com/v3/smtp/email', {
			method: 'POST',
			headers: {
				'api-key': emailApiKey,
				'Content-Type': 'application/json',
				'accept': 'application/json'
			},
			body: JSON.stringify({
				sender: {
					name: senderName!,
					email: senderEmail!
				},
				to: [
					{
						email: email,
						name: email.split('@')[0]
					}
				],
				subject: subject,
				htmlContent: htmlContent
			})
		});

		if (!response.ok) {
			const error = await response.text();
			console.error('Email sending failed:', error);
			return { success: false, error: 'Failed to send email' };
		}

		return { success: true };
	} catch (error) {
		console.error('Error sending email:', error);
		return { success: false, error: 'Email service error' };
	}
}

// Durable Object for storing verification codes temporarily
export class VerificationCodeStore extends DurableObject<Env> {
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;

		try {
			switch (path) {
				case '/create':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}
					return await this.createCode(request);

				case '/verify':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}
					return await this.verifyCode(request);

				case '/cleanup':
					return await this.cleanupExpiredCodes();

				default:
					return new Response('Not Found', { status: 404 });
			}
		} catch (error) {
			console.error('Error in VerificationCodeStore:', error);
			return new Response('Internal Server Error', { status: 500 });
		}
	}

	private async createCode(request: Request): Promise<Response> {
		try {
			const body = await request.json() as { email: string; purpose: string };
			const { email, purpose } = body;
			const code = Math.floor(100000 + Math.random() * 900000).toString();
			const expiresAt = Date.now() + 10 * 60 * 1000; // 10 minutes

			// Store code with email-purpose as key
			const key = `${email}:${purpose}`;
			await this.ctx.storage.put(key, {
				code,
				email,
				purpose,
				expiresAt,
				used: false,
				createdAt: Date.now()
			});

			return new Response(JSON.stringify({ success: true, code }), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error) {
			console.error('Error creating code:', error);
			return new Response(JSON.stringify({ success: false, error: 'Failed to create code' }), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	}

	private async verifyCode(request: Request): Promise<Response> {
		try {
			const body = await request.json() as { email: string; code: string; purpose: string };
			const { email, code, purpose } = body;
			const key = `${email}:${purpose}`;

			const stored = await this.ctx.storage.get(key) as any;

			if (!stored) {
				return new Response(JSON.stringify({
					valid: false,
					error: 'Invalid verification code'
				}), {
					status: 400,
					headers: { 'Content-Type': 'application/json' }
				});
			}

			// Check if expired
			if (stored.expiresAt < Date.now()) {
				await this.ctx.storage.delete(key);
				return new Response(JSON.stringify({
					valid: false,
					error: 'Verification code has expired'
				}), {
					status: 400,
					headers: { 'Content-Type': 'application/json' }
				});
			}

			// Check if already used
			if (stored.used) {
				return new Response(JSON.stringify({
					valid: false,
					error: 'Verification code already used'
				}), {
					status: 400,
					headers: { 'Content-Type': 'application/json' }
				});
			}

			// Check if code matches
			if (stored.code !== code) {
				return new Response(JSON.stringify({
					valid: false,
					error: 'Invalid verification code'
				}), {
					status: 400,
					headers: { 'Content-Type': 'application/json' }
				});
			}

			// Mark as used
			stored.used = true;
			await this.ctx.storage.put(key, stored);

			return new Response(JSON.stringify({ valid: true }), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error) {
			console.error('Error verifying code:', error);
			return new Response(JSON.stringify({
				valid: false,
				error: 'Verification failed'
			}), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	}

	private async cleanupExpiredCodes(): Promise<Response> {
		try {
			const allKeys = await this.ctx.storage.list();
			const now = Date.now();
			let cleaned = 0;

			for (const [key, value] of allKeys) {
				const stored = value as any;
				if (stored.expiresAt < now) {
					await this.ctx.storage.delete(key);
					cleaned++;
				}
			}

			return new Response(JSON.stringify({
				success: true,
				cleaned
			}), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error) {
			console.error('Error cleaning up codes:', error);
			return new Response(JSON.stringify({
				success: false,
				error: 'Cleanup failed'
			}), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	}
}

// Durable Object for storing refresh tokens
export class RefreshTokenStore extends DurableObject<Env> {
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;

		try {
			switch (path) {
				case '/create':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}
					return await this.createRefreshToken(request);

				case '/verify':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}
					return await this.verifyRefreshToken(request);

				case '/revoke':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}
					return await this.revokeRefreshToken(request);

				case '/cleanup':
					return await this.cleanupExpiredTokens();

				default:
					return new Response('Not Found', { status: 404 });
			}
		} catch (error) {
			console.error('Error in RefreshTokenStore:', error);
			return new Response('Internal Server Error', { status: 500 });
		}
	}

	private async createRefreshToken(request: Request): Promise<Response> {
		try {
			const body = await request.json() as { userId: number; email?: string; telegram_id?: string };
			const { userId, email, telegram_id } = body;

			// Generate refresh token
			const array = new Uint8Array(32);
			crypto.getRandomValues(array);
			const token = Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('');

			const expiresAt = Date.now() + 7 * 24 * 60 * 60 * 1000; // 7 days

			// Store with token as key
			await this.ctx.storage.put(token, {
				token,
				userId,
				email,
				telegram_id,
				expiresAt,
				createdAt: Date.now()
			});

			// Also store user's token list for management
			const userKey = `user:${userId}`;
			const userTokens = (await this.ctx.storage.get(userKey) as string[]) || [];
			userTokens.push(token);
			await this.ctx.storage.put(userKey, userTokens);

			return new Response(JSON.stringify({ success: true, token }), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error) {
			console.error('Error creating refresh token:', error);
			return new Response(JSON.stringify({ success: false, error: 'Failed to create token' }), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	}

	private async verifyRefreshToken(request: Request): Promise<Response> {
		try {
			const body = await request.json() as { token: string };
			const { token } = body;

			const stored = await this.ctx.storage.get(token) as RefreshTokenData | undefined;

			if (!stored) {
				return new Response(JSON.stringify({
					valid: false,
					error: 'Invalid refresh token'
				}), {
					status: 401,
					headers: { 'Content-Type': 'application/json' }
				});
			}

			// Check if expired
			if (stored.expiresAt < Date.now()) {
				await this.ctx.storage.delete(token);
				return new Response(JSON.stringify({
					valid: false,
					error: 'Refresh token has expired'
				}), {
					status: 401,
					headers: { 'Content-Type': 'application/json' }
				});
			}

			return new Response(JSON.stringify({
				valid: true,
				userId: stored.userId,
				email: stored.email,
				telegram_id: stored.telegram_id
			}), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error) {
			console.error('Error verifying refresh token:', error);
			return new Response(JSON.stringify({
				valid: false,
				error: 'Verification failed'
			}), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	}

	private async revokeRefreshToken(request: Request): Promise<Response> {
		try {
			const body = await request.json() as { token: string };
			const { token } = body;

			const stored = await this.ctx.storage.get(token) as RefreshTokenData | undefined;

			if (stored) {
				// Remove from storage
				await this.ctx.storage.delete(token);

				// Remove from user's token list
				const userKey = `user:${stored.userId}`;
				const userTokens = (await this.ctx.storage.get(userKey) as string[]) || [];
				const updatedTokens = userTokens.filter(t => t !== token);
				await this.ctx.storage.put(userKey, updatedTokens);
			}

			return new Response(JSON.stringify({ success: true }), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error) {
			console.error('Error revoking refresh token:', error);
			return new Response(JSON.stringify({
				success: false,
				error: 'Revocation failed'
			}), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	}

	private async cleanupExpiredTokens(): Promise<Response> {
		try {
			const allKeys = await this.ctx.storage.list();
			const now = Date.now();
			let cleaned = 0;

			for (const [key, value] of allKeys) {
				if (key.toString().startsWith('user:')) continue;

				const stored = value as RefreshTokenData;
				if (stored.expiresAt < now) {
					await this.ctx.storage.delete(key);
					cleaned++;
				}
			}

			return new Response(JSON.stringify({
				success: true,
				cleaned
			}), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error) {
			console.error('Error cleaning up tokens:', error);
			return new Response(JSON.stringify({
				success: false,
				error: 'Cleanup failed'
			}), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	}
}

export class WebSocketGateway extends DurableObject<Env> {
	private connections: Map<WebSocket, WebSocketConnection> = new Map();

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.connections = new Map();
		this.ctx.getWebSockets().forEach((ws) => {
			let meta = ws.deserializeAttachment();
			this.connections.set(ws, meta);
		})
	}

	/**
	 * Handle WebSocket connection using native Cloudflare Workers API
	 */
	async handleWebSocketConnection(request: Request): Promise<Response> {

		if (request.headers.get("Upgrade") != "websocket") {
			return new Response("expected websocket", {status: 400});
		}

		const connectionId = crypto.randomUUID();
		console.log(`🔗 NEW WEBSOCKET CONNECTION: ${connectionId}`);

		// Create WebSocket pair
		const pair= new WebSocketPair();

		// Store connection
		const now = Date.now();
		const connection = {
			id: connectionId,
			createdAt: now,
			lastHeartbeat: now,
			tokenId: ""
		}
		this.connections.set(pair[1], connection);

		pair[1].serializeAttachment(connection)

		// Use native Cloudflare Workers Durable Object API to accept WebSocket
		this.ctx.acceptWebSocket(pair[1], [connectionId]);

		// Send welcome message with reconnection info
		const welcomeMessage = JSON.stringify({
			type: 'connected',
			connectionId,
			message: 'Connected to WebSocket trade data stream',
			timestamp: now,
			reconnected: false
		});
		pair[1].send(welcomeMessage);

		console.log(`✅ WebSocket connection established: ${connectionId} (total: ${this.connections.size})`);

		return new Response(null, {
			status: 101,
			webSocket: pair[0]
		});
	}

	/**
	 * Native Cloudflare Workers WebSocket message handler
	 */
	async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
		// Try to find connection by WebSocket instance first (most reliable)
		const connection = this.connections.get(ws);

		if (connection) {
			console.log(`✅ Found connection via WebSocket instance: ${connection.id}`);
			await this.handleMessage(ws, connection, message);
			return;
		}

		console.log('❌ Received message from unknown WebSocket connection');
		console.log('🔍 WebSocket instance:', ws);
		console.log('🔍 Available connections:', Array.from(this.connections.keys()));

		// Send error message to client about connection recovery failure
		try {
			const errorMessage = JSON.stringify({
				type: 'connection_error',
				error: 'Connection state lost. Please reconnect.',
				timestamp: Date.now()
			});
			ws.send(errorMessage);
		} catch (sendError) {
			console.log('❌ Could not send error message');
		}
	}

	/**
	 * Handle WebSocket message processing
	 */
	private async handleMessage(ws: WebSocket, connection: WebSocketConnection, message: string | ArrayBuffer): Promise<void> {
		try {
			if (typeof message === 'string') {
				const data = JSON.parse(message);
				console.log(`📨 Message from ${connection.id}:`, data);

				// Handle different message types
				if (data.action === 'ping') {
					const pongMessage = JSON.stringify({
						action: 'pong',
						timestamp: Date.now()
					});
					ws.send(pongMessage);
				} else if (data.action === 'subscribe') {
					// Handle token subscription
					if (data.token_id) {
						connection.tokenId = data.token_id;
						ws.serializeAttachment(connection)

						console.log(`📋 ${connection.id} subscribed to token: ${data.token_id}`);

						const response = JSON.stringify({
							action: 'subscribed',
							timestamp: Date.now()
						});
						ws.send(response);
					}
				} else if (data.action === 'unsubscribe') {
					// Handle token unsubscription
					if (data.token_id) {
						connection.tokenId = ""
						ws.serializeAttachment(connection)
						console.log(`📋 ${connection.id} unsubscribed from pool: ${data.token_id}`);

						const response = JSON.stringify({
							action: 'unsubscribed',
							status: 'success',
							timestamp: Date.now()
						});
						ws.send(response);
					}
				}
			} else {
				console.log(`📨 Binary message from ${connection.id}, size: ${message.byteLength}`);
			}
		} catch (error) {
			console.log(`❌ Error parsing message from ${connection.id}:`, error);
		}
	}

	/**
	 * Native Cloudflare Workers WebSocket close handler
	 */
	async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): Promise<void> {
		// Try to find connection by WebSocket instance first
		const connection = this.connections.get(ws);

		if (connection) {
			console.log(`🔌 WEBSOCKET CLOSED: ${connection.id} (code: ${code}, reason: ${reason}, wasClean: ${wasClean})`);
			this.removeConnection(ws, 'client_disconnect');
			return;
		}

		console.log('❌ Received close from unknown WebSocket connection');
	}

	/**
	 * Native Cloudflare Workers WebSocket error handler
	 */
	async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
		// Try to find connection by WebSocket instance first
		const connection = this.connections.get(ws);

		if (connection) {
			console.log(`🔌 WebSocket error for ${connection.id}:`, error);
			this.removeConnection(ws, 'websocket_error');
			return;
		}

		console.log('❌ Received error from unknown WebSocket connection');
	}

	/**
	 * Remove a connection and clean up resources
	 */
	private removeConnection(ws: WebSocket, reason: string = 'unknown') {
		const connection = this.connections.get(ws);
		if (connection) {
			// Close the WebSocket
			try {
				ws.close(1000, 'Connection removed');
			} catch (error) {
				// WebSocket might already be closed
			}

			// Remove from connections map
			this.connections.delete(ws);

			console.log(`❌ REMOVED WEBSOCKET: ${connection.id} (reason: ${reason})`);
			console.log(`📊 Remaining connections: ${this.connections.size}`);
		}
	}

	/**
	 * Send data to a single WebSocket connection
	 */
	private async sendToConnection(ws: WebSocket, connection: WebSocketConnection, data: string | ArrayBuffer): Promise<boolean> {
		return new Promise((resolve) => {
			// 100ms timeout for dead connection detection
			const timeout = setTimeout(() => {
				console.log(`⏰ Timeout sending to WebSocket ${connection.id}`);
				resolve(false);
			}, 100);

			try {
				ws.send(data);
				clearTimeout(timeout);
				resolve(true);
			} catch (error) {
				clearTimeout(timeout);
				console.log(`❌ WebSocket send error for ${connection.id}:`, error);
				this.removeConnection(ws, 'send_error');
				resolve(false);
			}
		});
	}

	/**
	 * Get connection statistics
	 */
	getStats(): { connectionCount: number; connections: any[] } {
		const now = Date.now();
		return {
			connectionCount: this.connections.size,
			connections: Array.from(this.connections.values()).map(conn => ({
				id: conn.id,
				createdAt: new Date(conn.createdAt).toISOString(),
				lastHeartbeat: new Date(conn.lastHeartbeat).toISOString(),
				ageSeconds: Math.floor((now - conn.createdAt) / 1000),
			}))
		};
	}

	/**
	 * Broadcast binary data to all WebSocket connections
	 */
	async publishBinaryData(binaryData: ArrayBuffer | Uint8Array, targetTokenId: string): Promise<{ success: boolean; connectionsNotified: number }> {
		console.log(`Publishing binary data to WebSocket connections${targetTokenId ? ` for token: ${targetTokenId}` : ''}, size:`, binaryData.byteLength);
		try {
			if (this.connections.size === 0) {
				console.log('No WebSocket connections to broadcast to');
				return { success: true, connectionsNotified: 0 };
			}

		// Convert to ArrayBuffer if it's Uint8Array
		const arrayBuffer: ArrayBuffer = binaryData instanceof Uint8Array ? binaryData.buffer as ArrayBuffer : binaryData;

			// Iterate through connections and filter in the loop
			const broadcastTasks: Promise<boolean>[] = [];
			let targetCount = 0;

			for (const [ws, connection] of this.connections) {
				// Filter logic in the loop
				if (connection.tokenId != targetTokenId) {
					continue; // Skip this connection
				}

				// Add to broadcast tasks
				broadcastTasks.push(this.sendToConnection(ws, connection, arrayBuffer));
				targetCount++;
			}

			if (targetCount === 0) {
				console.log('No subscribed connections to broadcast to');
				return { success: true, connectionsNotified: 0 };
			}

			console.log(`📋 Broadcasting binary data to ${targetCount} connections${targetTokenId ? ` subscribed to token: ${targetTokenId}` : ''}`);

			// Wait for all broadcasts to complete
			const results = await Promise.allSettled(broadcastTasks);

			// Count successful sends
			const successCount = results.filter(r => r.status === 'fulfilled' && r.value).length;
			const failedCount = results.length - successCount;

			if (failedCount > 0) {
				console.log(`🧹 Cleaned up ${failedCount} failed WebSocket connections`);
			}

			console.log(`Binary broadcast completed: ${successCount}/${targetCount} connections notified`);

			return {
				success: true,
				connectionsNotified: successCount
			};
		} catch (error) {
			console.error('Error during binary broadcast:', error);
			return {
				success: false,
				connectionsNotified: 0
			};
		}
	}

	/**
	 * Handle internal requests from the main worker (RPC)
	 */
	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;

		try {
			switch (path) {
				case '/ws':
					return await this.handleWebSocketConnection(request);

				case '/publish-binary':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}
					const binaryData = await request.arrayBuffer();
					const binaryTargetTokenId = request.headers.get('Customized-Token-ID')!
					const result = await this.publishBinaryData(binaryData, binaryTargetTokenId);
					return new Response(JSON.stringify(result), {
						headers: { 'Content-Type': 'application/json' }
					});

				case '/stats':
					const stats = this.getStats();
					return new Response(JSON.stringify(stats), {
						headers: { 'Content-Type': 'application/json' }
					});

				default:
					return new Response('Not Found', { status: 404 });
			}
		} catch (error) {
			console.error('Error in Durable Object fetch:', error);
			return new Response('Internal Server Error', { status: 500 });
		}
	}
}

// Create Hono app with type-safe env
const app = new Hono<{ Bindings: any }>();

// Add CORS middleware
app.use('*', cors({
	origin: '*',
	allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
	allowHeaders: ['Content-Type', 'Authorization', 'Customized-Token-Address', 'Customized-Token-ID'],
	maxAge: 86400,
}));

// WebSocket route
app.get('/ws', async (c) => {
	console.log(`[FETCH] Handling WebSocket upgrade request`);
	const gateway = c.env.TRADE_GATEWAY.get(c.env.TRADE_GATEWAY.idFromName("main-gateway"));
	console.log(`[FETCH] Calling gateway.fetch for WebSocket`);
	return await gateway.fetch(c.req.raw);
});

// Publish route
app.post('/publish', async (c) => {
	const gateway = c.env.TRADE_GATEWAY.get(c.env.TRADE_GATEWAY.idFromName("main-gateway"));
	const contentType = c.req.header('content-type') || '';

	if (contentType.includes('application/json')) {
		// Handle JSON data
		const jsonData = await c.req.json();
		const targetTokenAddress = c.req.header('Customized-Token-Address');

		// Create request to Durable Object for JSON publishing
		const publishHeaders: Record<string, string> = { 'Content-Type': 'application/json' };
		if (targetTokenAddress) {
			publishHeaders['Customized-Token-Address'] = targetTokenAddress;
		}

		const publishRequest = new Request('http://localhost/publish-json', {
			method: 'POST',
			headers: publishHeaders,
			body: JSON.stringify(jsonData)
		});

		// Wait for broadcast to complete
		const publishResponse = await gateway.fetch(publishRequest);
		const broadcastResult = await publishResponse.json() as { success: boolean; connectionsNotified: number };

		// Respond with broadcast results
		return c.json({
			success: broadcastResult.success,
			connectionsNotified: broadcastResult.connectionsNotified,
			message: `Data broadcast to ${broadcastResult.connectionsNotified} WebSocket connections${targetTokenAddress ? ` for token: ${targetTokenAddress}` : ''}`,
			tokenAddress: targetTokenAddress,
			timestamp: Date.now()
		});
	} else if (contentType.includes('application/octet-stream') || contentType.includes('application/binary')) {
		// Handle binary data
		const binaryData = await c.req.arrayBuffer();
		const binaryTargetTokenId = c.req.header('Customized-Token-ID')!;

		// Create request to Durable Object for binary publishing
		const publishHeaders: Record<string, string> = { 'Content-Type': 'application/octet-stream' };
		publishHeaders['Customized-Token-ID'] = binaryTargetTokenId;

		const publishRequest = new Request('http://localhost/publish-binary', {
			method: 'POST',
			headers: publishHeaders,
			body: binaryData
		});

		// Wait for broadcast to complete
		const publishResponse = await gateway.fetch(publishRequest);
		const broadcastResult = await publishResponse.json() as { success: boolean; connectionsNotified: number };

		// Respond with broadcast results
		return c.json({
			success: broadcastResult.success,
			connectionsNotified: broadcastResult.connectionsNotified,
			message: `Binary data broadcast to ${broadcastResult.connectionsNotified} WebSocket connections${binaryTargetTokenId ? ` for token: ${binaryTargetTokenId}` : ''}`,
			poolId: binaryTargetTokenId,
			dataSize: binaryData.byteLength,
			timestamp: Date.now()
		});
	} else {
		return c.text('Unsupported content type. Use application/json for JSON data or application/octet-stream for binary data', 415);
	}
});

// Stats route
app.get('/stats', async (c) => {
	const gateway = c.env.TRADE_GATEWAY.get(c.env.TRADE_GATEWAY.idFromName("main-gateway"));
	const statsRequest = new Request('http://localhost/stats', {
		method: 'GET'
	});
	const statsResponse = await gateway.fetch(statsRequest);
	const stats = await statsResponse.json();
	return c.json(stats);
});

// Get single pool
app.get('/pool', async (c) => {
	try {
		const chainId = c.req.query('chain_id');
		const protocolName = c.req.query('protocol_name');
		const poolAddress = c.req.query('pool_address');

		if (!chainId || !poolAddress || !protocolName) {
			return c.text("Empty params", 400);
		}

		const result = await getPool(c.env.DB, chainId, protocolName, poolAddress);
		return c.json(result);
	} catch (error) {
		console.error('Error getting pool:', error);
		return c.json({ error: 'Failed to get pool' }, 500);
	}
});

// List pools
app.get('/pools', async (c) => {
	try {
		const page = parseInt(c.req.query('page') || '1');
		const pageSize = Math.min(parseInt(c.req.query('pageSize') || '20'), 300);

		const result = await listPools(c.env.DB, page, pageSize);
		return c.json(result);
	} catch (error) {
		console.error('Error listing pools:', error);
		return c.json({ error: 'Failed to list pools' }, 500);
	}
});

// Search pools
app.get('/pools/search', async (c) => {
	try {
		const query = c.req.query('q') || '';

		if (!query) {
			return c.json({ error: 'Missing search query parameter "q"' }, 400);
		}

		// Fuzzy search by pool_name (case-insensitive, partial match), top 10 results
		const db = c.env.DB;

		const dataResult = await db.prepare(
			`SELECT
				id, chain_id, protocol, pool_address, pool_name,
				token_0_address, token_0_symbol, token_0_decimals,
				token_1_address, token_1_symbol, token_1_decimals
			FROM pool_info
			WHERE LOWER(pool_name) LIKE ?
			ORDER BY created_at DESC
			LIMIT 10`
		).bind(`%${query.toLowerCase()}%`).all();

		const pools: PoolInfoWithId[] = dataResult.results.map((row: any) => ({
			id: row.id,
			chain_id: row.chain_id,
			protocol: row.protocol,
			pool_address: row.pool_address,
			pool_name: row.pool_name,
			token_0_address: row.token_0_address,
			token_0_symbol: row.token_0_symbol,
			token_0_decimals: row.token_0_decimals,
			token_1_address: row.token_1_address,
			token_1_symbol: row.token_1_symbol,
			token_1_decimals: row.token_1_decimals
		}));

		return c.json({ pools });
	} catch (error) {
		console.error('Error searching pools:', error);
		return c.json({ error: 'Failed to search pools' }, 500);
	}
});

// Add pool
app.post('/pools/add', async (c) => {
	try {
		const poolData: PoolInfo = await c.req.json();

		// Validate required fields
		if (!poolData.chain_id || !poolData.pool_address || !poolData.protocol) {
			return c.json({
				error: 'Missing required fields: chain_id, pool_address, protocol'
			}, 400);
		}

		const result = await addPool(c.env.DB, poolData);
		return c.json(result, 201);
	} catch (error) {
		console.error('Error adding pool:', error);

		// Handle duplicate pool address error
		if (error instanceof Error && error.message.includes('UNIQUE constraint failed')) {
			return c.json({
				error: 'Pool already exists with this address and chain ID'
			}, 201);
		}

		return c.json({ error: 'Failed to add pool' }, 500);
	}
});

// Tokens routes
app.get('/tokens', async (c) => {
	try {
		const page = parseInt(c.req.query('page') || '1');
		const pageSize = Math.min(parseInt(c.req.query('pageSize') || '20'), 100);
		const chainId = c.req.query('chainId') || undefined;
		const searchKey = c.req.query('search') || undefined;

		const result = await listTokens(c.env.DB, page, pageSize, chainId, searchKey);
		return c.json(result);
	} catch (error) {
		console.error('Error listing tokens:', error);
		return c.json({ error: 'Failed to list tokens' }, 500);
	}
});

// Get tokens by tag (e.g., trending)
app.get('/tokens/tag/:tag', async (c) => {
	try {
		const tag = c.req.param('tag');
		const limit = Math.min(parseInt(c.req.query('limit') || '20'), 100);
		const chainId = c.req.query('chainId') || undefined;

		if (!tag) {
			return c.json({ error: 'Tag parameter is required' }, 400);
		}

		const tokens = await getTokensByTag(c.env.DB, tag, limit, chainId);

		return c.json({
			success: true,
			data: tokens,
			count: tokens.length,
			tag,
			chainId: chainId || 'all'
		});
	} catch (error) {
		console.error('Error getting tokens by tag:', error);
		return c.json({ error: 'Failed to get tokens by tag' }, 500);
	}
});

// Add tag to token
app.post('/tokens/:chainId/:tokenAddress/tags', async (c) => {
	try {
		const chainId = c.req.param('chainId');
		const tokenAddress = c.req.param('tokenAddress');
		const { tag } = await c.req.json();

		if (!tag) {
			return c.json({ error: 'Tag is required' }, 400);
		}

		// Get token ID
		const tokenId = await getTokenId(c.env.DB, chainId, tokenAddress);
		if (!tokenId) {
			return c.json({ error: 'Token not found' }, 404);
		}

		// Add the tag
		const result = await addTokenTag(c.env.DB, tokenId, tag);

		if (!result.success) {
			return c.json({ error: 'Failed to add tag' }, 500);
		}

		// Get all tags for this token
		const tags = await getTokenTags(c.env.DB, tokenId);

		return c.json({
			success: true,
			message: `Tag '${tag}' added to token`,
			tags
		});
	} catch (error) {
		console.error('Error adding token tag:', error);
		return c.json({ error: 'Failed to add token tag' }, 500);
	}
});

// Remove tag from token
app.delete('/tokens/:chainId/:tokenAddress/tags/:tag', async (c) => {
	try {
		const chainId = c.req.param('chainId');
		const tokenAddress = c.req.param('tokenAddress');
		const tag = c.req.param('tag');

		// Get token ID
		const tokenId = await getTokenId(c.env.DB, chainId, tokenAddress);
		if (!tokenId) {
			return c.json({ error: 'Token not found' }, 404);
		}

		// Remove the tag
		const result = await removeTokenTag(c.env.DB, tokenId, tag);

		if (!result) {
			return c.json({ error: 'Failed to remove tag' }, 500);
		}

		// Get remaining tags
		const tags = await getTokenTags(c.env.DB, tokenId);

		return c.json({
			success: true,
			message: `Tag '${tag}' removed from token`,
			tags
		});
	} catch (error) {
		console.error('Error removing token tag:', error);
		return c.json({ error: 'Failed to remove token tag' }, 500);
	}
});

// Get all tags for a token
app.get('/tokens/:chainId/:tokenAddress/tags', async (c) => {
	try {
		const chainId = c.req.param('chainId');
		const tokenAddress = c.req.param('tokenAddress');

		// Get token ID
		const tokenId = await getTokenId(c.env.DB, chainId, tokenAddress);
		if (!tokenId) {
			return c.json({ error: 'Token not found' }, 404);
		}

		// Get tags
		const tags = await getTokenTags(c.env.DB, tokenId);

		return c.json({
			success: true,
			data: tags,
			count: tags.length
		});
	} catch (error) {
		console.error('Error getting token tags:', error);
		return c.json({ error: 'Failed to get token tags' }, 500);
	}
});

app.post('/tokens', async (c) => {
	try {
		const tokenData: TokenInfo = await c.req.json();

		// Validate required fields
		if (!tokenData.chain_id || !tokenData.token_address || !tokenData.token_symbol || !tokenData.token_name || tokenData.decimals === undefined) {
			return c.json({
				error: 'Missing required fields: chain_id, token_address, token_symbol, token_name, decimals'
			}, 400);
		}

		const result = await addToken(c.env.DB, tokenData);
		return c.json(result, 201);
	} catch (error) {
		console.error('Error adding token:', error);

		if (error instanceof Error && error.message.includes('UNIQUE constraint failed')) {
			return c.json({
				error: 'Token already exists with this address and chain ID'
			}, 409);
		}

		return c.json({ error: 'Failed to add token' }, 500);
	}
});

// Candle chart routes
app.get('/candle-chart', async (c) => {
	console.log(`[FETCH] Handling /candle-chart request`);
	return await handleCandleChart(c.req.raw, c.env);
});

app.get('/single-candle', async (c) => {
	console.log(`[FETCH] Handling /single-candle request`);
	return await handleSingleCandle(c.req.raw, c.env);
});

// Database query route
app.get('/db', async (c) => {
	try {
		const query: DBQuery = await c.req.json();

		if (!query || query.sql_template == "") {
			return c.json({
				error: 'Missing required param: query sql'
			}, 400);
		}

		const result = await c.env.DB.prepare(query.sql_template).bind(...query.sql_arguments).all();
		return c.json(result);
	} catch (error) {
		console.error('Error querying db:', error);
		return c.text('Internal Server Error', 500);
	}
});

// ============== Authentication Routes ==============

// Send Verification Code
app.post('/auth/send-code', async (c) => {
	try {
		const { email, purpose } = await c.req.json();

		// Validate input
		if (!email || !purpose) {
			return c.json({ error: 'Email and purpose are required' }, 400);
		}

		// Validate email format
		const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
		if (!emailRegex.test(email)) {
			return c.json({ error: 'Invalid email format' }, 400);
		}

		// Validate purpose
		if (purpose !== 'register' && purpose !== 'login' && purpose !== 'reset-password') {
			return c.json({ error: 'Purpose must be "register", "login", or "reset-password"' }, 400);
		}

		// For registration, check if email already exists
		if (purpose === 'register') {
			const existingUser = await getUserByEmail(c.env.DB, email);
			if (existingUser) {
				return c.json({ error: 'Email already registered' }, 400);
			}
		}

		// For login and reset-password, check if email exists
		if (purpose === 'login' || purpose === 'reset-password') {
			const existingUser = await getUserByEmail(c.env.DB, email);
			if (!existingUser) {
				return c.json({ error: 'Email not registered' }, 400);
			}
		}

		// Create verification code using Durable Object
		const result = await createVerificationCodeDurable(c.env, email, purpose);

		if (!result.success || !result.code) {
			return c.json({ error: result.error || 'Failed to create verification code' }, 500);
		}

		// Send email (if EMAIL_API_KEY is configured)
		const emailResult = await sendVerificationEmail(
			email,
			result.code,
			purpose,
			c.env.EMAIL_API_KEY,
			c.env.SENDER_EMAIL,
			c.env.SENDER_NAME
		);

		if (!emailResult.success) {
			console.error('Failed to send email:', emailResult.error);
			// Don't fail the request if email sending fails (code is still valid)
		}

		return c.json({
			success: true,
			message: 'Verification code sent to email'
		});
	} catch (error) {
		console.error('Error sending verification code:', error);
		return c.json({ error: 'Failed to send verification code' }, 500);
	}
});

// Email Register (with verification code)
app.post('/auth/register', async (c) => {
	try {
		const { email, password, verification_code } = await c.req.json();

		// Validate input
		if (!email || !password || !verification_code) {
			return c.json({ error: 'Email, password, and verification code are required' }, 400);
		}

		// Validate email format
		const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
		if (!emailRegex.test(email)) {
			return c.json({ error: 'Invalid email format' }, 400);
		}

		// Validate password length
		if (password.length < 6) {
			return c.json({ error: 'Password must be at least 6 characters' }, 400);
		}

		// Verify the verification code using Durable Object
		const verificationResult = await verifyCodeDurable(c.env, email, verification_code, 'register');

		if (!verificationResult.valid) {
			return c.json({ error: verificationResult.error || 'Invalid verification code' }, 400);
		}

		// Create user
		const result = await createUserWithEmail(c.env.DB, email, password);

		if (!result.success) {
			return c.json({ error: result.error }, 400);
		}

		// Generate JWT access token
		const jwtSecret = c.env.JWT_SECRET || 'default-secret-change-in-production';
		const accessToken = await generateJWT(
			{ userId: result.userId!, email },
			jwtSecret
		);

		// Generate refresh token
		const refreshResult = await createRefreshTokenDurable(c.env, result.userId!, email);

		if (!refreshResult.success || !refreshResult.token) {
			// If refresh token creation fails, still return access token but log error
			console.error('Failed to create refresh token:', refreshResult.error);
		}

		// Fetch the newly created user to get all fields
		const newUser = await c.env.DB.prepare(
			'SELECT * FROM users WHERE id = ?'
		).bind(result.userId).first() as User;

		return c.json({
			success: true,
			access_token: accessToken,
			refresh_token: refreshResult.token,
			token_type: 'Bearer',
			expires_in: 1800, // 30 minutes in seconds
			user: {
				id: newUser.id,
				email: newUser.email,
				telegram_id: newUser.telegram_id,
				telegram_username: newUser.telegram_username,
				full_name: newUser.full_name,
				avatar_url: newUser.avatar_url,
				bot_started: newUser.bot_started,
				bot_started_at: newUser.bot_started_at,
				created_at: newUser.created_at,
				last_login_at: newUser.last_login_at
			}
		}, 201);
	} catch (error) {
		console.error('Error registering user:', error);
		return c.json({ error: 'Failed to register user' }, 500);
	}
});

// Email Login (supports both password and verification code)
app.post('/auth/login', async (c) => {
	try {
		const { email, password, verification_code } = await c.req.json();

		// Validate input - must have email and either password or verification_code
		if (!email) {
			return c.json({ error: 'Email is required' }, 400);
		}

		if (!password && !verification_code) {
			return c.json({ error: 'Either password or verification_code is required' }, 400);
		}

		// Get user by email
		const user = await getUserByEmail(c.env.DB, email);

		if (!user) {
			return c.json({ error: 'Invalid email or credentials' }, 401);
		}

		// Authenticate with verification code using Durable Object
		if (verification_code) {
			const verificationResult = await verifyCodeDurable(c.env, email, verification_code, 'login');

			if (!verificationResult.valid) {
				return c.json({ error: verificationResult.error || 'Invalid verification code' }, 401);
			}
		}
		// Authenticate with password
		else if (password) {
			if (!user.password_hash) {
				return c.json({ error: 'Password login not available for this account' }, 401);
			}

			const isValidPassword = await verifyPassword(password, user.password_hash);

			if (!isValidPassword) {
				return c.json({ error: 'Invalid email or password' }, 401);
			}
		}

		// Update last login
		await updateUserLastLogin(c.env.DB, user.id);

		// Generate JWT access token
		const jwtSecret = c.env.JWT_SECRET || 'default-secret-change-in-production';
		const accessToken = await generateJWT(
			{ userId: user.id, email: user.email },
			jwtSecret
		);

		// Generate refresh token
		const refreshResult = await createRefreshTokenDurable(c.env, user.id, user.email);

		if (!refreshResult.success || !refreshResult.token) {
			console.error('Failed to create refresh token:', refreshResult.error);
		}

		return c.json({
			success: true,
			access_token: accessToken,
			refresh_token: refreshResult.token,
			token_type: 'Bearer',
			expires_in: 1800, // 30 minutes in seconds
			user: {
				id: user.id,
				email: user.email,
				telegram_id: user.telegram_id,
				telegram_username: user.telegram_username,
				full_name: user.full_name,
				avatar_url: user.avatar_url,
				bot_started: user.bot_started,
				bot_started_at: user.bot_started_at,
				created_at: user.created_at,
				last_login_at: user.last_login_at
			}
		});
	} catch (error) {
		console.error('Error logging in user:', error);
		return c.json({ error: 'Failed to login' }, 500);
	}
});

// Telegram Login/Bind
// If called with valid JWT token: binds Telegram to existing account
// If called without JWT token: login/register with Telegram
app.post('/auth/telegram', async (c) => {
	try {
		const authData = await c.req.json();

		// Validate required fields
		if (!authData.id) {
			return c.json({ error: 'Telegram ID is required' }, 400);
		}

		// Get bot token from environment
		const botToken = c.env.TELEGRAM_BOT_TOKEN;
		if (!botToken) {
			console.error('TELEGRAM_BOT_TOKEN not configured');
			return c.json({ error: 'Telegram authentication not configured' }, 500);
		}

		// Verify Telegram authentication (includes all 3 checks)
		// 1. Data-check-string format: key=<value>\nkey=<value> (sorted alphabetically)
		// 2. Check auth_date is not out of date
		// 3. Calculate hash and verify it matches
		const verification = await verifyTelegramAuth(authData, botToken);
		if (!verification.valid) {
			console.error('Telegram auth verification failed:', verification.error);
			return c.json({ error: verification.error || 'Invalid Telegram authentication' }, 401);
		}

		const telegram_id = authData.id.toString();
		const telegram_username = authData.username || authData.first_name || null;

		// Check if user is already authenticated (has valid JWT token)
		const authenticatedUser = await authenticateUser(c);

		// CASE 1: User is authenticated - bind Telegram to their account
		if (authenticatedUser) {
			console.log('Binding Telegram to authenticated user:', authenticatedUser.id);

			// Check if user already has Telegram linked
			if (authenticatedUser.telegram_id) {
				if (authenticatedUser.telegram_id === telegram_id) {
					return c.json({
						success: true,
						message: 'Telegram account already linked to this account',
						user: {
							id: authenticatedUser.id,
							email: authenticatedUser.email,
							telegram_id: authenticatedUser.telegram_id,
							telegram_username: authenticatedUser.telegram_username,
						}
					});
				} else {
					return c.json({
						error: 'This account is already linked to a different Telegram account. Please unlink first.'
					}, 400);
				}
			}

			// Bind Telegram to current user
			const bindResult = await bindTelegramToUser(
				c.env.DB,
				authenticatedUser.id,
				telegram_id,
				telegram_username
			);

			if (!bindResult.success) {
				return c.json({ error: bindResult.error }, 400);
			}

			// Fetch updated user
			const updatedUser = await c.env.DB.prepare(
				'SELECT * FROM users WHERE id = ?'
			).bind(authenticatedUser.id).first() as User;

			return c.json({
				success: true,
				message: 'Telegram account successfully linked',
				user: {
					id: updatedUser.id,
					email: updatedUser.email,
					telegram_id: updatedUser.telegram_id,
					telegram_username: updatedUser.telegram_username,
					created_at: updatedUser.created_at,
					updated_at: updatedUser.updated_at
				}
			});
		}

		// CASE 2: User is not authenticated - login/register with Telegram
		console.log('Telegram login/register for:', telegram_id);

		// Check if user exists with this Telegram ID
		let user = await getUserByTelegramId(c.env.DB, telegram_id);

		if (!user) {
			// Create new user
			const result = await createUserWithTelegram(
				c.env.DB,
				telegram_id,
				telegram_username
			);

			if (!result.success) {
				return c.json({ error: result.error }, 400);
			}

			// Fetch the newly created user
			user = await getUserByTelegramId(c.env.DB, telegram_id);

			if (!user) {
				return c.json({ error: 'Failed to create user' }, 500);
			}
		} else {
			// Update last login for existing user
			await updateUserLastLogin(c.env.DB, user.id);
		}

		// Generate JWT access token
		const jwtSecret = c.env.JWT_SECRET || 'default-secret-change-in-production';
		const accessToken = await generateJWT(
			{ userId: user.id, telegram_id: user.telegram_id },
			jwtSecret
		);

		// Generate refresh token
		const refreshResult = await createRefreshTokenDurable(c.env, user.id, undefined, user.telegram_id);

		if (!refreshResult.success || !refreshResult.token) {
			console.error('Failed to create refresh token:', refreshResult.error);
		}

		return c.json({
			success: true,
			access_token: accessToken,
			refresh_token: refreshResult.token,
			token_type: 'Bearer',
			expires_in: 1800, // 30 minutes in seconds
			user: {
				id: user.id,
				email: user.email,
				telegram_id: user.telegram_id,
				telegram_username: user.telegram_username,
				full_name: user.full_name,
				avatar_url: user.avatar_url,
				bot_started: user.bot_started,
				bot_started_at: user.bot_started_at,
				created_at: user.created_at,
				last_login_at: user.last_login_at
			}
		});
	} catch (error) {
		console.error('Error with Telegram login:', error);
		return c.json({ error: 'Failed to login with Telegram' }, 500);
	}
});

// Unbind Telegram from account (requires authentication)
app.delete('/auth/telegram', async (c) => {
	try {
		const user = await authenticateUser(c);
		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		// Check if user has Telegram linked
		if (!user.telegram_id) {
			return c.json({ error: 'No Telegram account linked' }, 400);
		}

		// Remove Telegram info from user
		const result = await c.env.DB.prepare(`
			UPDATE users
			SET telegram_id = NULL,
			    telegram_username = NULL,
			    updated_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`).bind(user.id).run();

		if (!result.success) {
			return c.json({ error: 'Failed to unbind Telegram account' }, 500);
		}

		return c.json({
			success: true,
			message: 'Telegram account successfully unlinked'
		});
	} catch (error) {
		console.error('Error unbinding Telegram:', error);
		return c.json({ error: 'Failed to unbind Telegram' }, 500);
	}
});

// Resend Verification Code (for expired codes)
app.post('/auth/resend-code', async (c) => {
	try {
		const { email, purpose } = await c.req.json();

		// Validate input
		if (!email || !purpose) {
			return c.json({ error: 'Email and purpose are required' }, 400);
		}

		// Validate purpose
		if (purpose !== 'register' && purpose !== 'login' && purpose !== 'reset-password') {
			return c.json({ error: 'Purpose must be "register", "login", or "reset-password"' }, 400);
		}

		// Create new verification code using Durable Object
		const result = await createVerificationCodeDurable(c.env, email, purpose);

		if (!result.success || !result.code) {
			return c.json({ error: result.error || 'Failed to create verification code' }, 500);
		}

		// Send email
		const emailResult = await sendVerificationEmail(
			email,
			result.code,
			purpose,
			c.env.EMAIL_API_KEY,
			c.env.SENDER_EMAIL,
			c.env.SENDER_NAME
		);

		if (!emailResult.success) {
			console.error('Failed to send email:', emailResult.error);
		}

		return c.json({
			success: true,
			message: 'New verification code sent to email',
			...(c.env.ENVIRONMENT === 'development' ? { code: result.code } : {})
		});
	} catch (error) {
		console.error('Error resending verification code:', error);
		return c.json({ error: 'Failed to resend verification code' }, 500);
	}
});

// Reset Password with Verification Code
app.post('/auth/reset-password', async (c) => {
	try {
		const { email, verification_code, new_password } = await c.req.json();

		// Validate input
		if (!email || !verification_code || !new_password) {
			return c.json({ error: 'Email, verification code, and new password are required' }, 400);
		}

		// Validate email format
		const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
		if (!emailRegex.test(email)) {
			return c.json({ error: 'Invalid email format' }, 400);
		}

		// Validate password length
		if (new_password.length < 6) {
			return c.json({ error: 'Password must be at least 6 characters' }, 400);
		}

		// Verify the verification code using Durable Object
		const verificationResult = await verifyCodeDurable(c.env, email, verification_code, 'reset-password');

		if (!verificationResult.valid) {
			return c.json({ error: verificationResult.error || 'Invalid verification code' }, 400);
		}

		// Check if user exists
		const user = await getUserByEmail(c.env.DB, email);
		if (!user) {
			return c.json({ error: 'User not found' }, 404);
		}

		// Update user password
		const updateResult = await updateUserPassword(c.env.DB, email, new_password);

		if (!updateResult.success) {
			return c.json({ error: updateResult.error || 'Failed to reset password' }, 500);
		}

		return c.json({
			success: true,
			message: 'Password reset successfully'
		});
	} catch (error) {
		console.error('Error resetting password:', error);
		return c.json({ error: 'Failed to reset password' }, 500);
	}
});

// OAuth Initiation Endpoints
app.get('/auth/oauth/:provider', async (c) => {
	try {
		const provider = c.req.param('provider');

		// Validate provider
		if (!['google', 'apple', 'x', 'telegram'].includes(provider)) {
			return c.json({ error: 'Invalid OAuth provider' }, 400);
		}

		// Generate state parameter for CSRF protection
		const state = crypto.randomUUID();

		let authUrl: string;

		switch (provider) {
			case 'google':
				authUrl = `https://accounts.google.com/o/oauth2/v2/auth?` +
					`client_id=${c.env.GOOGLE_CLIENT_ID}` +
					`&redirect_uri=${encodeURIComponent(`${c.env.WORKER_URL || 'https://crypto-pump.bigtutu.workers.dev'}/auth/oauth/callback/google`)}` +
					`&response_type=code` +
					`&scope=${encodeURIComponent('openid profile email')}` +
					`&state=${state}`;
				break;

			case 'apple':
				authUrl = `https://appleid.apple.com/auth/authorize?` +
					`client_id=${c.env.APPLE_CLIENT_ID}` +
					`&redirect_uri=${encodeURIComponent(`${c.env.WORKER_URL || 'https://crypto-pump.bigtutu.workers.dev'}/auth/oauth/callback/apple`)}` +
					`&response_type=code` +
					`&scope=${encodeURIComponent('name email')}` +
					`&response_mode=form_post` +
					`&state=${state}`;
				break;

			case 'x':
				// X (Twitter) OAuth 2.0
				authUrl = `https://twitter.com/i/oauth2/authorize?` +
					`client_id=${c.env.X_CLIENT_ID}` +
					`&redirect_uri=${encodeURIComponent(`${c.env.WORKER_URL || 'https://crypto-pump.bigtutu.workers.dev'}/auth/oauth/callback/x`)}` +
					`&response_type=code` +
					`&scope=${encodeURIComponent('tweet.read users.read offline.access')}` +
					`&state=${state}` +
					`&code_challenge=challenge` +
					`&code_challenge_method=plain`;
				break;

			case 'telegram':
				// Telegram OAuth (using Telegram Login Widget redirect)
				authUrl = `https://oauth.telegram.org/auth?` +
					`bot_id=${c.env.TELEGRAM_BOT_ID}` +
					`&origin=${encodeURIComponent(c.env.FRONTEND_URL || 'http://localhost:4200')}` +
					`&request_access=write` +
					`&return_to=${encodeURIComponent(`${c.env.WORKER_URL || 'https://crypto-pump.bigtutu.workers.dev'}/auth/oauth/callback/telegram`)}`;
				break;

			default:
				return c.json({ error: 'Unsupported provider' }, 400);
		}

		return c.json({ auth_url: authUrl, state });
	} catch (error) {
		console.error('Error initiating OAuth:', error);
		return c.json({ error: 'Failed to initiate OAuth' }, 500);
	}
});

// OAuth Callback (handled by backend, redirects to frontend with tokens)
app.get('/auth/oauth/callback/:provider', async (c) => {
	try {
		const provider = c.req.param('provider');
		const code = c.req.query('code');

		if (!code) {
			const frontendUrl = c.env.FRONTEND_URL || 'http://localhost:4200';
			return c.redirect(`${frontendUrl}/login?error=no_code`);
		}

		// For now, return a simple message indicating OAuth is configured but needs credentials
		// In production, you would exchange the code for tokens here
		const frontendUrl = c.env.FRONTEND_URL || 'http://localhost:4200';
		return c.redirect(`${frontendUrl}/login?message=oauth_${provider}_pending`);
	} catch (error) {
		console.error('Error in OAuth callback:', error);
		const frontendUrl = c.env.FRONTEND_URL || 'http://localhost:4200';
		return c.redirect(`${frontendUrl}/login?error=oauth_failed`);
	}
});

// Get current user profile (protected route example)
app.get('/auth/me', async (c) => {
	try {
		const user = await authenticateUser(c);

		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		// Return user info without sensitive data
		return c.json({
			success: true,
			user: {
				id: user.id,
				email: user.email,
				telegram_id: user.telegram_id,
				telegram_username: user.telegram_username,
				google_id: user.google_id,
				apple_id: user.apple_id,
				x_id: user.x_id,
				full_name: user.full_name,
				avatar_url: user.avatar_url,
				bot_started: user.bot_started,
				bot_started_at: user.bot_started_at,
				created_at: user.created_at,
				last_login_at: user.last_login_at
			}
		});
	} catch (error) {
		console.error('Error getting user profile:', error);
		return c.json({ error: 'Failed to get user profile' }, 500);
	}
});

// Get user notification preferences
app.get('/user/notification-preferences', async (c) => {
	try {
		const user = await authenticateUser(c);
		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		// Fetch notification preferences from database
		const preferences = await c.env.DB.prepare('SELECT email_enabled, telegram_enabled FROM notification_preferences WHERE user_id = ?').bind(user.id).first<{ email_enabled: number; telegram_enabled: number }>();

		// If no preferences found, return defaults
		if (!preferences) {
			return c.json({
				email_enabled: true,
				telegram_enabled: false
			});
		}

		// D1 returns booleans as integers (0 or 1), convert to boolean
		return c.json({
			email_enabled: preferences.email_enabled === 1,
			telegram_enabled: preferences.telegram_enabled === 1
		});
	} catch (error) {
		console.error('Error getting notification preferences:', error);
		return c.json({ error: 'Failed to get notification preferences' }, 500);
	}
});

// Update user notification preferences
app.put('/user/notification-preferences', async (c) => {
	try {
		const user = await authenticateUser(c);
		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const body = await c.req.json<{ email_enabled?: boolean; telegram_enabled?: boolean }>();

		// Validate request body
		if (typeof body.email_enabled !== 'boolean' && typeof body.telegram_enabled !== 'boolean') {
			return c.json({ error: 'At least one preference must be provided' }, 400);
		}

		// Get current preferences or defaults
		const current = await c.env.DB.prepare('SELECT email_enabled, telegram_enabled FROM notification_preferences WHERE user_id = ?').bind(user.id).first<{ email_enabled: number; telegram_enabled: number }>();

		const emailEnabled = typeof body.email_enabled === 'boolean'
			? body.email_enabled
			: (current?.email_enabled === 1 ? true : current?.email_enabled === 0 ? false : true);
		const telegramEnabled = typeof body.telegram_enabled === 'boolean'
			? body.telegram_enabled
			: (current?.telegram_enabled === 1 ? true : current?.telegram_enabled === 0 ? false : false);

		// Upsert notification preferences
		await c.env.DB.prepare(`
			INSERT INTO notification_preferences (user_id, email_enabled, telegram_enabled, created_at, updated_at)
			VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(user_id) DO UPDATE SET
				email_enabled = excluded.email_enabled,
				telegram_enabled = excluded.telegram_enabled,
				updated_at = CURRENT_TIMESTAMP
		`).bind(user.id, emailEnabled ? 1 : 0, telegramEnabled ? 1 : 0).run();

		return c.json({
			email_enabled: emailEnabled,
			telegram_enabled: telegramEnabled
		});
	} catch (error) {
		console.error('Error updating notification preferences:', error);
		return c.json({ error: 'Failed to update notification preferences' }, 500);
	}
});

// Verify token endpoint
app.post('/auth/verify', async (c) => {
	try {
		const { token } = await c.req.json();

		if (!token) {
			return c.json({ error: 'Token is required' }, 400);
		}

		const jwtSecret = c.env.JWT_SECRET || 'default-secret-change-in-production';
		const payload = await verifyJWT(token, jwtSecret);

		if (!payload) {
			return c.json({ error: 'Invalid or expired token', valid: false }, 401);
		}

		return c.json({
			success: true,
			valid: true,
			payload: {
				userId: payload.userId,
				email: payload.email,
				telegram_id: payload.telegram_id,
				exp: payload.exp
			}
		});
	} catch (error) {
		console.error('Error verifying token:', error);
		return c.json({ error: 'Failed to verify token' }, 500);
	}
});

// Refresh token endpoint
app.post('/auth/refresh', async (c) => {
	try {
		const { refresh_token } = await c.req.json();

		if (!refresh_token) {
			return c.json({ error: 'Refresh token is required' }, 400);
		}

		// Verify refresh token
		const verifyResult = await verifyRefreshTokenDurable(c.env, refresh_token);

		if (!verifyResult.valid) {
			return c.json({ error: verifyResult.error || 'Invalid refresh token' }, 401);
		}

		// Generate new access token
		const jwtSecret = c.env.JWT_SECRET || 'default-secret-change-in-production';
		const accessToken = await generateJWT(
			{
				userId: verifyResult.userId!,
				email: verifyResult.email,
				telegram_id: verifyResult.telegram_id
			},
			jwtSecret
		);

		return c.json({
			success: true,
			access_token: accessToken,
			token_type: 'Bearer',
			expires_in: 1800 // 30 minutes in seconds
		});
	} catch (error) {
		console.error('Error refreshing token:', error);
		return c.json({ error: 'Failed to refresh token' }, 500);
	}
});

// Logout endpoint (revokes refresh token)
app.post('/auth/logout', async (c) => {
	try {
		const { refresh_token } = await c.req.json();

		if (!refresh_token) {
			return c.json({ error: 'Refresh token is required' }, 400);
		}

		// Revoke refresh token
		const revokeResult = await revokeRefreshTokenDurable(c.env, refresh_token);

		if (!revokeResult.success) {
			console.error('Failed to revoke refresh token:', revokeResult.error);
			// Don't fail logout if revoke fails
		}

		return c.json({
			success: true,
			message: 'Logged out successfully'
		});
	} catch (error) {
		console.error('Error logging out:', error);
		return c.json({ error: 'Failed to logout' }, 500);
	}
});

// ============== Watched Token Routes (Protected) ==============

// Add token to watchlist
app.post('/watched-tokens', async (c) => {
	try {
		const user = await authenticateUser(c);
		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const body = await c.req.json();
		const { token_id, notes, interval_1m, interval_5m, interval_15m, interval_1h, alert_active } = body;

		// Validate required fields
		if (!token_id) {
			return c.json({ error: 'token_id is required' }, 400);
		}

		// Validate interval thresholds
		const validation = validateIntervalThresholds(interval_1m, interval_5m, interval_15m, interval_1h);
		if (!validation.valid) {
			return c.json({ error: validation.error }, 400);
		}

		// Check if token exists
		const tokenCheck = await c.env.DB.prepare(
			'SELECT id FROM tokens WHERE id = ?'
		).bind(token_id).first();

		if (!tokenCheck) {
			return c.json({ error: 'Token not found' }, 404);
		}

		// Check if already watching this token
		const existingWatch = await c.env.DB.prepare(
			'SELECT id FROM user_watched_tokens WHERE user_id = ? AND token_id = ?'
		).bind(user.id, token_id).first();

		if (existingWatch) {
			return c.json({ error: 'Token already in watchlist' }, 409);
		}

		const result = await addWatchedToken(
			c.env.DB,
			user.id,
			token_id,
			notes,
			interval_1m,
			interval_5m,
			interval_15m,
			interval_1h,
			alert_active !== undefined ? alert_active : true
		);

		// Get the full watched token details
		const watchedToken = await getWatchedTokenById(c.env.DB, result.id, user.id);

		return c.json({
			success: true,
			data: watchedToken
		}, 201);
	} catch (error) {
		console.error('Error adding watched token:', error);
		return c.json({ error: 'Failed to add watched token' }, 500);
	}
});

// Get all watched tokens for current user
app.get('/watched-tokens', async (c) => {
	try {
		const user = await authenticateUser(c);
		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const watchedTokens = await getWatchedTokensByUserId(c.env.DB, user.id);

		return c.json({
			success: true,
			data: watchedTokens,
			count: watchedTokens.length
		});
	} catch (error) {
		console.error('Error getting watched tokens:', error);
		return c.json({ error: 'Failed to get watched tokens' }, 500);
	}
});

// Get specific watched token
app.get('/watched-tokens/:id', async (c) => {
	try {
		const user = await authenticateUser(c);
		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const id = parseInt(c.req.param('id'));
		if (isNaN(id)) {
			return c.json({ error: 'Invalid ID' }, 400);
		}

		const watchedToken = await getWatchedTokenById(c.env.DB, id, user.id);

		if (!watchedToken) {
			return c.json({ error: 'Watched token not found' }, 404);
		}

		return c.json({
			success: true,
			data: watchedToken
		});
	} catch (error) {
		console.error('Error getting watched token:', error);
		return c.json({ error: 'Failed to get watched token' }, 500);
	}
});

// Update watched token
app.put('/watched-tokens/:id', async (c) => {
	try {
		const user = await authenticateUser(c);
		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const id = parseInt(c.req.param('id'));
		if (isNaN(id)) {
			return c.json({ error: 'Invalid ID' }, 400);
		}

		const body = await c.req.json();
		const updates: any = {};

		// Only include fields that are present in the request
		if (body.notes !== undefined) updates.notes = body.notes;
		if (body.interval_1m !== undefined) updates.interval_1m = body.interval_1m;
		if (body.interval_5m !== undefined) updates.interval_5m = body.interval_5m;
		if (body.interval_15m !== undefined) updates.interval_15m = body.interval_15m;
		if (body.interval_1h !== undefined) updates.interval_1h = body.interval_1h;
		if (body.alert_active !== undefined) updates.alert_active = body.alert_active;

		if (Object.keys(updates).length === 0) {
			return c.json({ error: 'No update fields provided' }, 400);
		}

		// If any interval is being updated, validate with existing values
		const hasIntervalUpdate = body.interval_1m !== undefined ||
		                         body.interval_5m !== undefined ||
		                         body.interval_15m !== undefined ||
		                         body.interval_1h !== undefined;

		if (hasIntervalUpdate) {
			// Fetch existing watched token to merge with updates
			const existing = await getWatchedTokenById(c.env.DB, id, user.id);

			if (!existing) {
				return c.json({ error: 'Watched token not found' }, 404);
			}

			// Merge existing values with updates
			const merged_1m = body.interval_1m !== undefined ? body.interval_1m : existing.interval_1m;
			const merged_5m = body.interval_5m !== undefined ? body.interval_5m : existing.interval_5m;
			const merged_15m = body.interval_15m !== undefined ? body.interval_15m : existing.interval_15m;
			const merged_1h = body.interval_1h !== undefined ? body.interval_1h : existing.interval_1h;

			// Validate the merged intervals
			const validation = validateIntervalThresholds(merged_1m, merged_5m, merged_15m, merged_1h);
			if (!validation.valid) {
				return c.json({ error: validation.error }, 400);
			}
		}

		const success = await updateWatchedToken(c.env.DB, id, user.id, updates);

		if (!success) {
			return c.json({ error: 'Watched token not found or update failed' }, 404);
		}

		// Get the updated watched token
		const watchedToken = await getWatchedTokenById(c.env.DB, id, user.id);

		return c.json({
			success: true,
			data: watchedToken
		});
	} catch (error) {
		console.error('Error updating watched token:', error);
		return c.json({ error: 'Failed to update watched token' }, 500);
	}
});

// Delete watched token
app.delete('/watched-tokens/:id', async (c) => {
	try {
		const user = await authenticateUser(c);
		if (!user) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const id = parseInt(c.req.param('id'));
		if (isNaN(id)) {
			return c.json({ error: 'Invalid ID' }, 400);
		}

		const success = await deleteWatchedToken(c.env.DB, id, user.id);

		if (!success) {
			return c.json({ error: 'Watched token not found' }, 404);
		}

		return c.json({
			success: true,
			message: 'Watched token removed from watchlist'
		});
	} catch (error) {
		console.error('Error deleting watched token:', error);
		return c.json({ error: 'Failed to delete watched token' }, 500);
	}
});

// Get all active watched tokens (for alert system)
app.get('/watched-tokens/active/all', async (c) => {
	try {
		const activeWatchedTokens = await getActiveWatchedTokens(c.env.DB);

		return c.json({
			success: true,
			data: activeWatchedTokens,
			count: activeWatchedTokens.length
		});
	} catch (error) {
		console.error('Error getting active watched tokens:', error);
		return c.json({ error: 'Failed to get active watched tokens' }, 500);
	}
});

// ============== Webhook Routes ==============

// Webhook: Mark bot as started (called by Telegram bot service)
// Telegram message format: { message: { from: { id: telegram_id }, text: "/start user_id" } }
// Extract telegram_id from message.from.id and user_id from message.text
app.post('/webhook/bot', async (c) => {
	try {
		const requestBody = await c.req.json();

		// Log all headers for debugging
		const allHeaders: Record<string, string> = {};
		c.req.raw.headers.forEach((value, key) => {
			// Mask sensitive headers
			if (key.toLowerCase() === 'x-webhook-secret') {
				allHeaders[key] = value ? '***MASKED***' : 'missing';
			} else {
				allHeaders[key] = value;
			}
		});

		console.log('[WEBHOOK /bot] Request received:', {
			timestamp: new Date().toISOString(),
			body: requestBody,
			headers: allHeaders
		});

		// Verify webhook secret
		const webhookSecret = c.req.header('x-telegram-bot-api-secret-token');
		const expectedSecret = c.env.API_KEY || 'change-this-in-production';

		if (webhookSecret !== expectedSecret) {
			console.error('[WEBHOOK /bot] Invalid webhook secret');
			return c.json({ error: 'Unauthorized' }, 401);
		}

		// Parse request body - support both direct format and Telegram webhook format
		let user_id: number | undefined;
		let telegram_id: string | undefined;
		let telegram_username: string | undefined;
		let telegram_first_name: string | undefined;

		// Check if it's a Telegram webhook format: { message: { from: { id }, text: "/start user_id" } }
		if (requestBody.message) {
			const message = requestBody.message;

			// Extract telegram data from message.from
			if (message.from && message.from.id) {
				telegram_id = message.from.id.toString();
				telegram_username = message.from.username;
				telegram_first_name = message.from.first_name;
				console.log('[WEBHOOK /bot] Extracted from message.from:', {
					telegram_id,
					telegram_username,
					telegram_first_name
				});
			}

			// Extract user_id from message.text (e.g., "/start 3")
			if (message.text) {
				const textParts = message.text.split(' ');
				if (textParts.length >= 2 && textParts[0] === '/start') {
					const parsedUserId = parseInt(textParts[1]);
					if (!isNaN(parsedUserId)) {
						user_id = parsedUserId;
						console.log('[WEBHOOK /bot] Extracted user_id from message.text:', user_id);
					}
				}
			}
		} else {
			// Direct format: { user_id: 123, telegram_id: "456789" }
			user_id = requestBody.user_id;
			telegram_id = requestBody.telegram_id;
		}

		if (!user_id && !telegram_id) {
			console.error('[WEBHOOK /bot] Missing required fields. Expected either direct format {user_id, telegram_id} or Telegram format {message: {from: {id}, text: "/start user_id"}}');
			return c.json({ error: 'Either user_id or telegram_id is required' }, 400);
		}

		console.log('[WEBHOOK /bot] Parsed data:', { user_id, telegram_id, telegram_username });

		// Update bot_started status and link Telegram account
		let result;
		if (user_id && telegram_id) {
			// Link Telegram account and set bot_started (when user clicks deep link with user_id)
			result = await c.env.DB.prepare(`
				UPDATE users
				SET bot_started = TRUE,
					bot_started_at = CURRENT_TIMESTAMP,
					telegram_id = ?,
					telegram_username = ?,
					updated_at = CURRENT_TIMESTAMP
				WHERE id = ?
			`).bind(telegram_id, telegram_username, user_id).run();
			console.log('[WEBHOOK /bot] Linked Telegram account and set bot_started for user_id:', user_id);
		} else if (user_id) {
			// Just set bot_started for user_id (no telegram linking)
			result = await c.env.DB.prepare(`
				UPDATE users
				SET bot_started = TRUE,
					bot_started_at = CURRENT_TIMESTAMP,
					updated_at = CURRENT_TIMESTAMP
				WHERE id = ?
			`).bind(user_id).run();
		} else {
			// Set bot_started for telegram_id (telegram account already linked)
			result = await c.env.DB.prepare(`
				UPDATE users
				SET bot_started = TRUE,
					bot_started_at = CURRENT_TIMESTAMP,
					updated_at = CURRENT_TIMESTAMP
				WHERE telegram_id = ?
			`).bind(telegram_id).run();
		}

		if (!result.success || result.meta.changes === 0) {
			console.error('[WEBHOOK /bot] User not found or update failed:', { user_id, telegram_id });
			return c.json({ error: 'User not found or update failed' }, 404);
		}

		console.log('[WEBHOOK /bot] Bot started status updated successfully:', {
			user_id: user_id || 'N/A',
			telegram_id: telegram_id || 'N/A',
			changes: result.meta.changes
		});

		// Publish bot_started event to WebSocket clients
		try {
			const gateway = c.env.TRADE_GATEWAY.get(c.env.TRADE_GATEWAY.idFromName("main-gateway"));
			const wsEvent = {
				type: 'bot_started',
				data: {
					user_id: user_id,
					telegram_id: telegram_id,
					telegram_username: telegram_username,
					bot_started_at: new Date().toISOString()
				},
				timestamp: new Date().toISOString()
			};

			const publishRequest = new Request('http://localhost/publish-json', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(wsEvent)
			});

			const publishResponse = await gateway.fetch(publishRequest);
			const publishResult = await publishResponse.json() as { success: boolean; connectionsNotified: number };

			console.log('[WEBHOOK /bot] Published bot_started event to WebSocket:', {
				success: publishResult.success,
				connectionsNotified: publishResult.connectionsNotified
			});
		} catch (wsError) {
			console.error('[WEBHOOK /bot] Failed to publish WebSocket event:', wsError);
			// Don't fail the webhook if WebSocket publish fails
		}

		return c.json({
			success: true,
			message: 'Bot started status updated'
		});
	} catch (error) {
		console.error('[WEBHOOK /bot] Error:', error);
		return c.json({ error: 'Failed to update bot started status' }, 500);
	}
});

// ============== Database API Routes (for internal ORM) ==============

// Helper function to authenticate internal API requests
async function authenticateInternalAPI(c: any): Promise<boolean> {
	const apiKey = c.req.header('X-API-Key');
	const expectedKey = c.env.API_KEY || 'change-this-in-production';
	return apiKey === expectedKey;
}

// POST /db/query - Execute SQL query (for ORM)
interface SQLQueryRequest {
	sql: string;
	params?: any[];
}

app.post('/db/query', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const body: SQLQueryRequest = await c.req.json();

		if (!body.sql || typeof body.sql !== 'string') {
			return c.json({ error: 'SQL query is required' }, 400);
		}

		// Validate SQL - only allow SELECT, INSERT, UPDATE, DELETE
		const sqlUpper = body.sql.trim().toUpperCase();
		const allowedOperations = ['SELECT', 'INSERT', 'UPDATE', 'DELETE'];
		const isAllowed = allowedOperations.some(op => sqlUpper.startsWith(op));

		if (!isAllowed) {
			return c.json({ error: 'Only SELECT, INSERT, UPDATE, DELETE operations are allowed' }, 400);
		}

		// Execute query
		const params = body.params || [];
		let result;

		if (sqlUpper.startsWith('SELECT')) {
			// SELECT queries return results
			const queryResult = await c.env.DB.prepare(body.sql).bind(...params).all();
			result = {
				success: true,
				data: queryResult.results,
				meta: queryResult.meta
			};
		} else {
			// INSERT, UPDATE, DELETE queries return success status
			const queryResult = await c.env.DB.prepare(body.sql).bind(...params).run();
			result = {
				success: queryResult.success,
				meta: queryResult.meta
			};
		}

		return c.json(result);
	} catch (error: any) {
		console.error('Error executing SQL query:', error);
		return c.json({ 
			error: 'Failed to execute SQL query',
			message: error.message 
		}, 500);
	}
});

// POST /db/query/batch - Execute multiple SQL queries in a transaction
interface SQLBatchRequest {
	queries: Array<{
		sql: string;
		params?: any[];
	}>;
}

app.post('/db/query/batch', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const body: SQLBatchRequest = await c.req.json();

		if (!body.queries || !Array.isArray(body.queries) || body.queries.length === 0) {
			return c.json({ error: 'Queries array is required' }, 400);
		}

		// Validate all queries
		for (const query of body.queries) {
			if (!query.sql || typeof query.sql !== 'string') {
				return c.json({ error: 'All queries must have SQL string' }, 400);
			}

			const sqlUpper = query.sql.trim().toUpperCase();
			const allowedOperations = ['SELECT', 'INSERT', 'UPDATE', 'DELETE'];
			const isAllowed = allowedOperations.some(op => sqlUpper.startsWith(op));

			if (!isAllowed) {
				return c.json({ error: `Query contains disallowed operation: ${query.sql.substring(0, 50)}` }, 400);
			}
		}

		// Execute queries in a transaction-like manner (D1 doesn't support transactions in workers)
		// We'll execute them sequentially and return results
		const results = [];
		for (const query of body.queries) {
			const params = query.params || [];
			const sqlUpper = query.sql.trim().toUpperCase();

			try {
				if (sqlUpper.startsWith('SELECT')) {
					const queryResult = await c.env.DB.prepare(query.sql).bind(...params).all();
					results.push({
						success: true,
						data: queryResult.results,
						meta: queryResult.meta
					});
				} else {
					const queryResult = await c.env.DB.prepare(query.sql).bind(...params).run();
					results.push({
						success: queryResult.success,
						meta: queryResult.meta
					});
				}
			} catch (error: any) {
				results.push({
					success: false,
					error: error.message
				});
			}
		}

		return c.json({
			success: true,
			results: results
		});
	} catch (error: any) {
		console.error('Error executing batch SQL queries:', error);
		return c.json({ 
			error: 'Failed to execute batch SQL queries',
			message: error.message 
		}, 500);
	}
});

// GET /db/watched-tokens/active - Get all active watched tokens (for alert system)
app.get('/db/watched-tokens/active', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const activeWatchedTokens = await getActiveWatchedTokens(c.env.DB);

		return c.json({
			success: true,
			data: activeWatchedTokens,
			count: activeWatchedTokens.length
		});
	} catch (error) {
		console.error('Error getting active watched tokens:', error);
		return c.json({ error: 'Failed to get active watched tokens' }, 500);
	}
});

// GET /db/watched-tokens/:tokenId - Get watched tokens by token ID
app.get('/db/watched-tokens/token/:tokenId', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const tokenId = parseInt(c.req.param('tokenId'));
		if (isNaN(tokenId)) {
			return c.json({ error: 'Invalid token ID' }, 400);
		}

		const query = `
			SELECT
				wt.id,
				wt.user_id,
				wt.token_id,
				wt.notes,
				wt.interval_1m,
				wt.interval_5m,
				wt.interval_15m,
				wt.interval_1h,
				wt.alert_active,
				wt.created_at,
				t.chain_id,
				t.token_address,
				t.token_symbol,
				t.token_name,
				t.decimals,
				t.icon_url
			FROM user_watched_tokens wt
			INNER JOIN tokens t ON wt.token_id = t.id
			WHERE wt.token_id = ? AND wt.alert_active = 1
			ORDER BY wt.created_at DESC
		`;

		const result = await c.env.DB.prepare(query).bind(tokenId).all();

		const watchedTokens = result.results.map((row: any) => ({
			id: Number(row.id),
			user_id: Number(row.user_id),
			token_id: Number(row.token_id),
			notes: row.notes ? String(row.notes) : undefined,
			interval_1m: row.interval_1m ? Number(row.interval_1m) : undefined,
			interval_5m: row.interval_5m ? Number(row.interval_5m) : undefined,
			interval_15m: row.interval_15m ? Number(row.interval_15m) : undefined,
			interval_1h: row.interval_1h ? Number(row.interval_1h) : undefined,
			alert_active: Boolean(row.alert_active),
			created_at: String(row.created_at),
			chain_id: String(row.chain_id),
			token_address: String(row.token_address),
			token_symbol: String(row.token_symbol),
			token_name: String(row.token_name),
			decimals: Number(row.decimals),
			icon_url: row.icon_url ? String(row.icon_url) : undefined
		}));

		return c.json({
			success: true,
			data: watchedTokens,
			count: watchedTokens.length
		});
	} catch (error) {
		console.error('Error getting watched tokens by token ID:', error);
		return c.json({ error: 'Failed to get watched tokens' }, 500);
	}
});

// GET /db/users/:userId - Get user by ID
app.get('/db/users/:userId', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const userId = parseInt(c.req.param('userId'));
		if (isNaN(userId)) {
			return c.json({ error: 'Invalid user ID' }, 400);
		}

		const result = await c.env.DB.prepare(
			'SELECT * FROM users WHERE id = ?'
		).bind(userId).first();

		if (!result) {
			return c.json({ error: 'User not found' }, 404);
		}

		return c.json({
			success: true,
			data: result
		});
	} catch (error) {
		console.error('Error getting user:', error);
		return c.json({ error: 'Failed to get user' }, 500);
	}
});

// GET /db/users/telegram/:telegramId - Get user by Telegram ID
app.get('/db/users/telegram/:telegramId', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const telegramId = c.req.param('telegramId');

		const result = await c.env.DB.prepare(
			'SELECT * FROM users WHERE telegram_id = ?'
		).bind(telegramId).first();

		if (!result) {
			return c.json({ error: 'User not found' }, 404);
		}

		return c.json({
			success: true,
			data: result
		});
	} catch (error) {
		console.error('Error getting user by Telegram ID:', error);
		return c.json({ error: 'Failed to get user' }, 500);
	}
});

// GET /db/users/:userId/notification-preferences - Get notification preferences for user
app.get('/db/users/:userId/notification-preferences', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const userId = parseInt(c.req.param('userId'));
		if (isNaN(userId)) {
			return c.json({ error: 'Invalid user ID' }, 400);
		}

		const result = await c.env.DB.prepare(
			'SELECT * FROM notification_preferences WHERE user_id = ?'
		).bind(userId).first();

		if (!result) {
			// Return defaults if not found
			return c.json({
				success: true,
				data: {
					user_id: userId,
					email_enabled: true,
					telegram_enabled: false
				}
			});
		}

		return c.json({
			success: true,
			data: result
		});
	} catch (error) {
		console.error('Error getting notification preferences:', error);
		return c.json({ error: 'Failed to get notification preferences' }, 500);
	}
});

// GET /db/tokens/:chainId/:tokenAddress - Get token by chain ID and address
app.get('/db/tokens/:chainId/:tokenAddress', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const chainId = c.req.param('chainId');
		const tokenAddress = c.req.param('tokenAddress');

		const result = await c.env.DB.prepare(
			'SELECT * FROM tokens WHERE chain_id = ? AND token_address = ?'
		).bind(chainId, tokenAddress).first();

		if (!result) {
			return c.json({ error: 'Token not found' }, 404);
		}

		return c.json({
			success: true,
			data: result
		});
	} catch (error) {
		console.error('Error getting token:', error);
		return c.json({ error: 'Failed to get token' }, 500);
	}
});

// GET /db/tokens/id/:tokenId - Get token by ID
app.get('/db/tokens/id/:tokenId', async (c) => {
	try {
		if (!(await authenticateInternalAPI(c))) {
			return c.json({ error: 'Unauthorized' }, 401);
		}

		const tokenId = parseInt(c.req.param('tokenId'));
		if (isNaN(tokenId)) {
			return c.json({ error: 'Invalid token ID' }, 400);
		}

		const result = await c.env.DB.prepare(
			'SELECT * FROM tokens WHERE id = ?'
		).bind(tokenId).first();

		if (!result) {
			return c.json({ error: 'Token not found' }, 404);
		}

		return c.json({
			success: true,
			data: result
		});
	} catch (error) {
		console.error('Error getting token by ID:', error);
		return c.json({ error: 'Failed to get token' }, 500);
	}
});

// Root/health check route
app.get('/', async (c) => {
	console.log(`[FETCH] Handling root / request`);
	return c.json({
		service: 'WebSocket Trade Data Gateway with Authentication',
		endpoints: {
			// Authentication
			'/auth/send-code': 'POST - Send verification code to email',
			'/auth/resend-code': 'POST - Resend verification code',
			'/auth/register': 'POST - Register new user with email/password/verification_code',
			'/auth/login': 'POST - Login with email and password OR verification_code',
			'/auth/reset-password': 'POST - Reset password with email/verification_code/new_password',
			'/auth/telegram': 'POST - Login/Register with Telegram',
			'/auth/oauth/:provider': 'GET - Initiate OAuth login (google, apple, x, telegram)',
			'/auth/oauth/callback/:provider': 'GET - OAuth callback endpoint',
			'/auth/refresh': 'POST - Refresh access token using refresh token',
			'/auth/logout': 'POST - Logout and revoke refresh token',
			'/auth/verify': 'POST - Verify JWT token',
			'/auth/me': 'GET - Get current user profile (requires Authorization header)',

			// WebSocket & Trading
			'/ws': 'GET - Connect to WebSocket for real-time trade data',
			'/publish': 'POST - Publish data to all connected WebSocket clients (JSON: application/json, Binary: application/octet-stream)',
			'/stats': 'GET - Get WebSocket connection statistics',

			// Pools
			'/pools': 'GET - List pools with pagination (?page=1&pageSize=20&chainId=1&protocol=uniswap)',
			'/pools/add': 'POST - Add new pool information',
			'/pools/search': 'GET - Search pools by name (?q=query)',

		// Tokens
		'/tokens': 'GET - List tokens (?page=1&pageSize=20&chainId=1&search=name_or_address) | POST - Add new token',
		'/tokens/tag/:tag': 'GET - Get tokens by tag (?limit=20&chainId=1) - e.g., /tokens/tag/trending',
		'/tokens/:chainId/:tokenAddress/tags': 'GET - Get all tags for a token | POST - Add tag to token (body: {tag: "trending"})',
		'/tokens/:chainId/:tokenAddress/tags/:tag': 'DELETE - Remove tag from token',

		// Watched Tokens (Protected)
			'/watched-tokens': 'GET - Get user watchlist (requires auth) | POST - Add token to watchlist (requires auth)',
			'/watched-tokens/:id': 'GET - Get specific watched token (requires auth) | PUT - Update watched token (requires auth) | DELETE - Remove from watchlist (requires auth)',
			'/watched-tokens/active/all': 'GET - Get all active watched tokens (for alert system)',

			// Webhooks (requires x-telegram-bot-api-secret-token header)
			'/webhook/bot': 'POST - Link Telegram account and mark bot as started (Telegram message format or {user_id, telegram_id})',

			// Candles
			'/candle-chart': 'GET - Retrieve candle chart data from KV',
			'/single-candle': 'GET - Retrieve single candle data from KV',

			'/': 'GET - This info page'
		},
		features: [
			'JWT-based authentication',
			'Email verification codes (6-digit, 10-minute expiry)',
			'Email/password registration and login',
			'Passwordless login with verification code',
			'Telegram authentication support',
			'Protected routes with Bearer token',
			'User watchlist management with custom alert thresholds',
			'Price change alerts for multiple time intervals (1m, 5m, 15m, 1h)',
			'WebSocket-based real-time communication',
			'Support for both JSON and binary data',
			'Immediate connection state detection',
			'Bidirectional messaging support',
			'Automatic dead connection cleanup',
			'KV storage for candle chart data',
			'D1 database for user management'
		],
		authentication: {
			type: 'Bearer Token (JWT)',
			example: 'Authorization: Bearer <your_access_token>',
			accessTokenExpiration: '30 minutes',
			refreshTokenExpiration: '7 days',
			note: 'Use refresh token to get new access token when expired'
		},
		timestamp: new Date().toISOString()
	});
});

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		console.log(`[FETCH] Incoming request: ${request.method} ${request.url}`);
		try {
			return await app.fetch(request, env, ctx);
		} catch (error) {
			console.error('Error handling request:', error);
			return new Response('Internal Server Error', { status: 500 });
		}
	},
} satisfies ExportedHandler<Env>;

/**
 * Handle candle chart data using KV storage
 */
async function handleCandleChart(request: Request, env: Env): Promise<Response> {
	try {
		const url = new URL(request.url);
		const tokenId = url.searchParams.get('token_id') || '';
		const timeframe = url.searchParams.get('time_frame') || '60';
		const page_index = url.searchParams.get('page') || '1';

		if (tokenId.toString() == '') {
			return new Response('Empty tokenId', { status: 400 });
		}

		const page = parseInt(page_index, 10) || 1;
		const maxDaysToSearch = 30; // Search up to 30 days back

		// Try to find data starting from the requested page (days back)
		let candleData: string | null = null;
		let foundDate: string | null = null;

		for (let i = page - 1; i < maxDaysToSearch; i++) {
			const now = new Date();
			const targetDate = new Date(now);
			targetDate.setUTCDate(now.getUTCDate() - i);

			// Format date as YYYY-MM-DD
			const yyyy = targetDate.getUTCFullYear();
			const mm = String(targetDate.getUTCMonth() + 1).padStart(2, '0');
			const dd = String(targetDate.getUTCDate()).padStart(2, '0');
			const dateStr = `${yyyy}-${mm}-${dd}`;

			// Compose the key for this day's candle data
			const key = `${tokenId}-${timeframe}-${dateStr}`;

			// Try to retrieve candle data for this day from KV
			candleData = await env.KV.get(key, 'text');

			if (candleData) {
				foundDate = dateStr;
				break;
			}
		}

		if (!candleData) {
			return new Response(JSON.stringify({
				success: false,
				error: 'No candle data found for the requested period'
			}), {
				status: 404,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		return new Response(candleData, {
			headers: {
				'Content-Type': 'application/base64',
			}
		});
	} catch (error) {
		return new Response(JSON.stringify({
			success: false,
			error: error instanceof Error ? error.message : 'Unknown error'
		}), {
			status: 500,
			headers: { 'Content-Type': 'application/json' }
		});
	}
}

/**
 * Handle single candle data retrieval from KV
 */
async function handleSingleCandle(request: Request, env: Env): Promise<Response> {
	try {
		const url = new URL(request.url);
		const tokenId = url.searchParams.get('token_id') || '';
		const timeframe = url.searchParams.get('time_frame') || '60';

		if (tokenId.toString() == '') {
			return new Response('Empty token_id', { status: 400 });
		}

		// Compose the key for this day's candle data
		const key = `${tokenId}-${timeframe}-current`;

		// Retrieve candle data for this day from KV
		const candleData = await env.KV.get(key, 'text');

		if (!candleData) {
			return new Response(JSON.stringify({
				success: false,
				error: 'data not found'
			}))
		}


		return new Response(candleData, {
			headers: {
				'Content-Type': 'application/base64',
				'Access-Control-Allow-Origin': '*',
				'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
				'Access-Control-Allow-Headers': 'Content-Type',
				'Access-Control-Max-Age': '86400'
			}
		});
	} catch (error) {
		return new Response(JSON.stringify({
			success: false,
			error: error instanceof Error ? error.message : 'Unknown error'
		}), {
			status: 500,
			headers: { 'Content-Type': 'application/json' }
		});
	}
}
