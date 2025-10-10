import { DurableObject } from 'cloudflare:workers';

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
	subscribedPools: Set<string>; // Track which token this connection is subscribed to
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
    chainId?: string
): Promise<TokenInfo[]> {
    // Build query parts
    let whereClause = '';
    const bindings: any[] = [];

    if (chainId) {
        whereClause = 'WHERE chain_id = ?';
        bindings.push(chainId);
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
			subscribedPools: new Set<string>()
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
						connection.subscribedPools.add(data.token_id);
						ws.serializeAttachment(connection)

						console.log(`📋 ${connection.id} subscribed to token: ${data.token_id}`);

						const response = JSON.stringify({
							action: 'subscribed',
							subscribe_id: data.token_id,
							subscribedPools: Array.from(connection.subscribedPools),
							timestamp: Date.now()
						});
						ws.send(response);
					}
				} else if (data.action === 'unsubscribe') {
					// Handle token unsubscription
					if (data.token_id) {
						connection.subscribedPools.delete(data.token_id);
						ws.serializeAttachment(connection)
						console.log(`📋 ${connection.id} unsubscribed from pool: ${data.token_id}`);

						const response = JSON.stringify({
							action: 'unsubscribed',
							status: 'success',
							subscribedPools: Array.from(connection.subscribedPools),
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
				subscribedPools: Array.from(conn.subscribedPools)
			}))
		};
	}

	/**
	 * Broadcast data to all WebSocket connections
	 */
	async publishData(data: any, targetPoolId?: string): Promise<{ success: boolean; connectionsNotified: number }> {
		console.log(`Publishing data to WebSocket connections${targetPoolId ? ` for pool: ${targetPoolId}` : ''}`);

		try {
			if (this.connections.size === 0) {
				console.log('No WebSocket connections to broadcast to');
				return { success: true, connectionsNotified: 0 };
			}

							// Format data as JSON message (without custom type to avoid browser warnings)
		const message = JSON.stringify({
			data: data,
			poolId: targetPoolId,
			timestamp: Date.now()
		});

		// Iterate through connections and filter in the loop
		const broadcastTasks: Promise<boolean>[] = [];
		let targetCount = 0;

		for (const [ws, connection] of this.connections) {
			// Filter logic in the loop
			if (targetPoolId && !connection.subscribedPools.has(targetPoolId)) {
				continue; // Skip this connection
			}

			// Add to broadcast tasks
			broadcastTasks.push(this.sendToConnection(ws, connection, message));
			targetCount++;
		}

		if (targetCount === 0) {
			console.log('No subscribed connections to broadcast to');
			return { success: true, connectionsNotified: 0 };
		}

		console.log(`📋 Broadcasting to ${targetCount} connections${targetPoolId ? ` subscribed to token: ${targetPoolId}` : ''}`);

					// Wait for all broadcasts to complete
		const results = await Promise.allSettled(broadcastTasks);

		// Count successful sends
		const successCount = results.filter(r => r.status === 'fulfilled' && r.value).length;
		const failedCount = results.length - successCount;

		if (failedCount > 0) {
			console.log(`🧹 Cleaned up ${failedCount} failed WebSocket connections`);
		}

		console.log(`WebSocket broadcast completed: ${successCount}/${targetCount} connections notified`);

		return {
			success: true,
			connectionsNotified: successCount
		};
		} catch (error) {
			console.error('Error during WebSocket broadcast:', error);
			return {
				success: false,
				connectionsNotified: 0
			};
		}
	}

	/**
	 * Broadcast binary data to all WebSocket connections
	 */
	async publishBinaryData(binaryData: ArrayBuffer | Uint8Array, targetTokenId?: string): Promise<{ success: boolean; connectionsNotified: number }> {
		console.log(`Publishing binary data to WebSocket connections${targetTokenId ? ` for token: ${targetTokenId}` : ''}, size:`, binaryData.byteLength);
		try {
			if (this.connections.size === 0) {
				console.log('No WebSocket connections to broadcast to');
				return { success: true, connectionsNotified: 0 };
			}

			// Convert to ArrayBuffer if it's Uint8Array
			const arrayBuffer = binaryData instanceof Uint8Array ? binaryData.buffer : binaryData;

			// Iterate through connections and filter in the loop
			const broadcastTasks: Promise<boolean>[] = [];
			let targetCount = 0;

			for (const [ws, connection] of this.connections) {
				// Filter logic in the loop
				if (targetTokenId && !connection.subscribedPools.has(targetTokenId)) {
					console.log(targetTokenId,connection.subscribedPools)
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
					const binaryResult = await this.publishBinaryData(binaryData, binaryTargetTokenId);
					return new Response(JSON.stringify(binaryResult), {
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

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;

		console.log(`[FETCH] Incoming request: ${request.method} ${path}`);

		// Create Durable Object instance
		const id: DurableObjectId = env.TRADE_GATEWAY.idFromName("main-gateway");
		const gateway: DurableObjectStub<undefined> = env.TRADE_GATEWAY.get(id);

		console.log(`[FETCH] GET Durable Object instance with ID: ${id}`);

		try {
			// Handle CORS preflight requests
			if (request.method === 'OPTIONS') {
				return new Response(null, {
					status: 200,
					headers: {
						'Access-Control-Allow-Origin': '*',
						'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
						'Access-Control-Allow-Headers': 'Content-Type',
						'Access-Control-Max-Age': '86400'
					}
				});
			}

			switch (path) {
				case '/ws':
					console.log(`[FETCH] Handling WebSocket upgrade request`);
					// Handle WebSocket connections
					if (request.method !== 'GET') {
						return new Response('Method not allowed', { status: 405 });
					}
					console.log(`[FETCH] Calling gateway.fetch for WebSocket`);
					return await gateway.fetch(request);

				case '/publish':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}

					const contentType = request.headers.get('content-type') || '';

					if (contentType.includes('application/json')) {
						// Handle JSON data
						const jsonData = await request.json();
						const targetTokenAddress = request.headers.get('Customized-Token-Address');

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
						return new Response(JSON.stringify({
							success: broadcastResult.success,
							connectionsNotified: broadcastResult.connectionsNotified,
							message: `Data broadcast to ${broadcastResult.connectionsNotified} WebSocket connections${targetTokenAddress ? ` for token: ${targetTokenAddress}` : ''}`,
							tokenAddress: targetTokenAddress,
							timestamp: Date.now()
						}), {
							headers: { 'Content-Type': 'application/json' }
						});
					} else if (contentType.includes('application/octet-stream') || contentType.includes('application/binary')) {
						// Handle binary data
						const binaryData = await request.arrayBuffer();
						const binaryTargetTokenId = request.headers.get('Customized-Token-ID')!;

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
						return new Response(JSON.stringify({
							success: broadcastResult.success,
							connectionsNotified: broadcastResult.connectionsNotified,
							message: `Binary data broadcast to ${broadcastResult.connectionsNotified} WebSocket connections${binaryTargetTokenId ? ` for token: ${binaryTargetTokenId}` : ''}`,
							poolId: binaryTargetTokenId,
							dataSize: binaryData.byteLength,
							timestamp: Date.now()
						}), {
							headers: { 'Content-Type': 'application/json' }
						});
					} else {
						return new Response('Unsupported content type. Use application/json for JSON data or application/octet-stream for binary data', { status: 415 });
					}

				case '/stats':
					// Get gateway statistics
					const statsRequest = new Request('http://localhost/stats', {
						method: 'GET'
					});
					const statsResponse = await gateway.fetch(statsRequest);
					const stats = await statsResponse.json();
					return new Response(JSON.stringify(stats), {
						headers: { 'Content-Type': 'application/json' }
					});

				case '/pool':
					// List pools with pagination
					if (request.method !== 'GET') {
						return new Response('Method not allowed', { status: 405 });
					}

					try {
						const searchParams = url.searchParams;
						const chainId = searchParams.get('chain_id');
						const protocolName = searchParams.get('protocol_name');
						const poolAddress = searchParams.get('pool_address');

						if (!chainId || !poolAddress || !protocolName) {
							return new Response("Empty params",{
								status: 400,
							})
						}

						const result = await getPool(env.DB, chainId, protocolName, poolAddress);
						return new Response(JSON.stringify(result), {
							headers: { 'Content-Type': 'application/json' }
						});
					} catch (error) {
						console.error('Error listing pools:', error);
						return new Response(JSON.stringify({ error: 'Failed to list pools' }), {
							status: 500,
							headers: { 'Content-Type': 'application/json' }
						});
					}

				case '/pools':
					// List pools with pagination
					if (request.method !== 'GET') {
						return new Response('Method not allowed', { status: 405 });
					}

					try {
						const searchParams = url.searchParams;
						const page = parseInt(searchParams.get('page') || '1');
						const pageSize = Math.min(parseInt(searchParams.get('pageSize') || '20'), 300); // Max 100 per page

						const result = await listPools(env.DB, page, pageSize);
						return new Response(JSON.stringify(result), {
							headers: { 'Content-Type': 'application/json' }
						});
					} catch (error) {
						console.error('Error listing pools:', error);
						return new Response(JSON.stringify({ error: 'Failed to list pools' }), {
							status: 500,
							headers: { 'Content-Type': 'application/json' }
						});
					}

				case '/pools/search':
					// Search pools with pagination
					if (request.method !== 'GET') {
						return new Response('Method not allowed', { status: 405 });
					}

					try {
						const searchParams = url.searchParams;
						const query = searchParams.get('q') || '';

						if (!query) {
							return new Response(JSON.stringify({
								error: 'Missing search query parameter "q"'
							}), {
								status: 400,
								headers: { 'Content-Type': 'application/json' }
							});
						}

						// Fuzzy search by pool_name (case-insensitive, partial match), top 10 results
						const db = env.DB;

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

						return new Response(JSON.stringify({ pools }), {
							headers: {
								'Content-Type': 'application/binary',
								'Access-Control-Allow-Origin': '*',
								'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
								'Access-Control-Allow-Headers': 'Content-Type',
								'Access-Control-Max-Age': '86400'
							}
						});
					} catch (error) {
						console.error('Error searching pools:', error);
						return new Response(JSON.stringify({ error: 'Failed to search pools' }), {
							status: 500,
							headers: { 'Content-Type': 'application/json' }
						});
					}

				case '/pools/add':
					// Add new pool
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}

					try {
						const poolData: PoolInfo = await request.json();

						// Validate required fields
						if (!poolData.chain_id || !poolData.pool_address || !poolData.protocol) {
							return new Response(JSON.stringify({
								error: 'Missing required fields: chain_id, pool_address, protocol'
							}), {
								status: 400,
								headers: { 'Content-Type': 'application/json' }
							});
						}

						const result = await addPool(env.DB, poolData);
						return new Response(JSON.stringify(result), {
							status: 201,
							headers: { 'Content-Type': 'application/json' }
						});
					} catch (error) {
						console.error('Error adding pool:', error);

						// Handle duplicate pool address error
						if (error instanceof Error && error.message.includes('UNIQUE constraint failed')) {
							return new Response(JSON.stringify({
								error: 'Pool already exists with this address and chain ID'
							}), {
								status: 201,
								headers: { 'Content-Type': 'application/json' }
							});
						}

						return new Response(JSON.stringify({ error: 'Failed to add pool' }), {
							status: 500,
							headers: { 'Content-Type': 'application/json' }
						});
					}

				case '/tokens':
						if (request.method === 'GET') {
								try {
										const searchParams = url.searchParams;
										const page = parseInt(searchParams.get('page') || '1');
										const pageSize = Math.min(parseInt(searchParams.get('pageSize') || '20'), 100);
										const chainId = searchParams.get('chainId') || undefined;

										const result = await listTokens(env.DB, page, pageSize, chainId);
										return new Response(JSON.stringify(result), {
												headers: {
													'Content-Type': 'application/json',
													'Access-Control-Allow-Origin': '*',
													'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
													'Access-Control-Allow-Headers': 'Content-Type',
													'Access-Control-Max-Age': '86400'
												}

										});
								} catch (error) {
										console.error('Error listing tokens:', error);
										return new Response(JSON.stringify({ error: 'Failed to list tokens' }), {
												status: 500,
												headers: { 'Content-Type': 'application/json' }
										});
								}
						} else if (request.method === 'POST') {
								try {
										const tokenData: TokenInfo = await request.json();

										// Validate required fields
										if (!tokenData.chain_id || !tokenData.token_address || !tokenData.token_symbol || !tokenData.token_name || tokenData.decimals === undefined) {
												return new Response(JSON.stringify({
														error: 'Missing required fields: chain_id, token_address, token_symbol, token_name, decimals'
												}), {
														status: 400,
														headers: { 'Content-Type': 'application/json' }
												});
										}

										const result = await addToken(env.DB, tokenData);
										return new Response(JSON.stringify(result), {
												status: 201,
												headers: { 'Content-Type': 'application/json' }
										});
								} catch (error) {
										console.error('Error adding token:', error);

										if (error instanceof Error && error.message.includes('UNIQUE constraint failed')) {
												return new Response(JSON.stringify({
														error: 'Token already exists with this address and chain ID'
												}), {
														status: 409,
														headers: { 'Content-Type': 'application/json' }
												});
										}

										return new Response(JSON.stringify({ error: 'Failed to add token' }), {
												status: 500,
												headers: { 'Content-Type': 'application/json' }
										});
								}
						} else {
								return new Response('Method not allowed', { status: 405 });
						}

				case '/candle-chart':
					console.log(`[FETCH] Handling /candle-chart request`);
					return await handleCandleChart(request, env);

				case '/single-candle':
					console.log(`[FETCH] Handling /single-candle request`);
					return await handleSingleCandle(request, env);

				case '/':
					console.log(`[FETCH] Handling root / request`);
					// Health check / info endpoint
					return new Response(JSON.stringify({
						service: 'WebSocket Trade Data Gateway',
						endpoints: {
							'/ws': 'GET - Connect to WebSocket for real-time trade data',
							'/publish': 'POST - Publish data to all connected WebSocket clients (JSON: application/json, Binary: application/octet-stream)',
							'/stats': 'GET - Get WebSocket connection statistics',
							'/pools': 'GET - List pools with pagination (?page=1&pageSize=20&chainId=1&protocol=uniswap)',
							'/pools/add': 'POST - Add new pool information',
							'/candle-chart': 'POST - Store candle chart data in KV',
							'/single-candle': 'GET - Retrieve candle data from KV',
							'/': 'GET - This info page'
						},
						features: [
							'WebSocket-based real-time communication',
							'Support for both JSON and binary data',
							'Immediate connection state detection',
							'Bidirectional messaging support',
							'Automatic dead connection cleanup',
							'KV storage for candle chart data'
						],
						timestamp: new Date().toISOString()
					}), {
						headers: { 'Content-Type': 'application/json' }
					});

				case '/db':
					if (request.method !== 'GET') {
						return new Response('Method not allowed', { status: 405 });
					}

					try {
						const query: DBQuery = await request.json();

						if (!query || query.sql_template == "") {
							return new Response(JSON.stringify({
								error: 'Missing required param: query sql'
							}), {
								status: 400,
								headers: { 'Content-Type': 'application/json' }
							});
						}

						const result = await env.DB.prepare(query.sql_template).bind(...query.sql_arguments).all()


					} catch (error) {
						console.error('Error listing db:', error);
						return new Response('Internal Server Error',{ status: 500 });
					}

				default:
					return new Response('Not Found', { status: 404 });
			}
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

		// Calculate the date for the requested page
		const page = parseInt(page_index, 10) || 1;
		const now = new Date();
		// Clone the date to avoid mutating 'now'
		const targetDate = new Date(now);
		targetDate.setUTCDate(now.getUTCDate() - (page - 1));

		// Format date as YYYY-MM-DD
		const yyyy = targetDate.getUTCFullYear();
		const mm = String(targetDate.getUTCMonth() + 1).padStart(2, '0');
		const dd = String(targetDate.getUTCDate()).padStart(2, '0');
		// todo different time frame data
		const dateStr = `${yyyy}-${mm}-${dd}`;

		// Compose the key for this day's candle data
		const key = `${tokenId}-${timeframe}-${dateStr}`;

		// Retrieve candle data for this day from KV
		const candleData = await env.KV.get(key, 'text');

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

/**
 * Handle single candle data retrieval from KV
 */
async function handleSingleCandle(request: Request, env: Env): Promise<Response> {
	try {
		const url = new URL(request.url);
		const tokenId = url.searchParams.get('token_id') || '';
		const timeframe = url.searchParams.get('timeframe') || '60';

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
