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

interface PoolInfo {
	chain_id: string;
	protocol: string;
	pool_address: string;
	pool_name: string;
	cost_token_address: string;
	cost_token_symbol: string;
	cost_token_decimals: number;
	get_token_address: string;
	get_token_symbol: string;
	get_token_decimals: number;
}

interface PoolInfoWithId extends PoolInfo {
	id: number;
}

interface ListPoolsResponse {
	pools: PoolInfoWithId[];
	pagination: {
		page: number;
		pageSize: number;
		total: number;
		totalPages: number;
		hasNext: boolean;
		hasPrev: boolean;
	};
}

interface WebSocketConnection {
	id: string;
	webSocket: WebSocket;
	createdAt: number;
	lastHeartbeat: number;
}

// Database helper functions
async function addPool(db: D1Database, poolData: PoolInfo): Promise<{ success: boolean; id: number }> {
	const result = await db.prepare(`
		INSERT INTO pool_info (
			chain_id, protocol, pool_address, pool_name,
			cost_token_address, cost_token_symbol, cost_token_decimals,
			get_token_address, get_token_symbol, get_token_decimals
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`).bind(
		poolData.chain_id,
		poolData.protocol,
		poolData.pool_address,
		poolData.pool_name,
		poolData.cost_token_address,
		poolData.cost_token_symbol,
		poolData.cost_token_decimals,
		poolData.get_token_address,
		poolData.get_token_symbol,
		poolData.get_token_decimals
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
			cost_token_address, cost_token_symbol, cost_token_decimals,
			get_token_address, get_token_symbol, get_token_decimals
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
		cost_token_address: String(result.cost_token_address),
		cost_token_symbol: String(result.cost_token_symbol),
		cost_token_decimals: Number(result.cost_token_decimals),
		get_token_address: String(result.get_token_address),
		get_token_symbol: String(result.get_token_symbol),
		get_token_decimals: Number(result.get_token_decimals),
	}
}

async function listPools(
	db: D1Database,
	page: number = 1,
	pageSize: number = 20,
	chainId?: string | null,
	protocol?: string | null,
	poolAddress?: string | null,
): Promise<ListPoolsResponse> {
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
			cost_token_address, cost_token_symbol, cost_token_decimals,
			get_token_address, get_token_symbol, get_token_decimals
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
		cost_token_address: row.cost_token_address,
		cost_token_symbol: row.cost_token_symbol,
		cost_token_decimals: row.cost_token_decimals,
		get_token_address: row.get_token_address,
		get_token_symbol: row.get_token_symbol,
		get_token_decimals: row.get_token_decimals
	}));

	const totalPages = Math.ceil(total / pageSize);

	return {
		pools,
		pagination: {
			page,
			pageSize,
			total,
			totalPages,
			hasNext: page < totalPages,
			hasPrev: page > 1
		}
	};
}

export class WebSocketGateway extends DurableObject<Env> {
	private connections: Map<string, WebSocketConnection> = new Map();

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);

		// Enable WebSocket hibernation for cost savings
		this.setupHibernation();
	}

	/**
	 * Set up WebSocket hibernation to reduce costs
	 */
	private setupHibernation() {
		// Note: WebSocket hibernation is automatically enabled
		// Cloudflare Workers will hibernate WebSockets when idle
		console.log('💤 WebSocket hibernation enabled for cost savings');
	}



	/**
	 * Handle WebSocket connection
	 */
	async handleWebSocketConnection(request: Request): Promise<Response> {
		const connectionId = crypto.randomUUID();
		console.log(`🔗 NEW WEBSOCKET CONNECTION: ${connectionId}`);

		// Create WebSocket pair
		const [client, server] = Object.values(new WebSocketPair());

		// Store connection
		const now = Date.now();
		this.connections.set(connectionId, {
			id: connectionId,
			webSocket: server,
			createdAt: now,
			lastHeartbeat: now
		});

		// Set up WebSocket event handlers
		server.accept();

		// Send welcome message (optional - can be removed if causing browser warnings)
		// const welcomeMessage = JSON.stringify({
		// 	type: 'connected',
		// 	connectionId,
		// 	message: 'Connected to WebSocket trade data stream',
		// 	timestamp: now
		// });
		// server.send(welcomeMessage);

		// Handle WebSocket events
		server.addEventListener('message', (event) => {
			try {
				const data = JSON.parse(event.data as string);
				console.log(`📨 Message from ${connectionId}:`, data);

				// Handle different message types
				if (data.type === 'ping') {
					const pongMessage = JSON.stringify({
						response: 'pong',
						timestamp: Date.now()
					});
					server.send(pongMessage);
				}
			} catch (error) {
				console.log(`❌ Error parsing message from ${connectionId}:`, error);
			}
		});

		server.addEventListener('close', (event) => {
			console.log(`🔌 WEBSOCKET CLOSED: ${connectionId} (code: ${event.code}, reason: ${event.reason})`);
			this.removeConnection(connectionId, 'client_disconnect');
		});

		server.addEventListener('error', (event) => {
			console.log(`🔌 WebSocket error event for ${connectionId}:`, event);
			this.removeConnection(connectionId, 'websocket_error');
		});

		console.log(`✅ WebSocket connection established: ${connectionId} (total: ${this.connections.size})`);

		return new Response(null, {
			status: 101,
			webSocket: client
		});
	}

	/**
	 * Remove a connection and clean up resources
	 */
	private removeConnection(connectionId: string, reason: string = 'unknown') {
		const connection = this.connections.get(connectionId);
		if (connection) {
			// Close the WebSocket
			try {
				connection.webSocket.close(1000, 'Connection removed');
			} catch (error) {
				// WebSocket might already be closed
			}

			// Remove from connections map
			this.connections.delete(connectionId);

			console.log(`❌ REMOVED WEBSOCKET: ${connectionId} (reason: ${reason})`);
			console.log(`📊 Remaining connections: ${this.connections.size}`);
		}
	}

	/**
	 * Send data to a single WebSocket connection
	 */
	private async sendToConnection(connectionId: string, connection: WebSocketConnection, data: string | ArrayBuffer): Promise<boolean> {
		return new Promise((resolve) => {
			// 100ms timeout for dead connection detection
			const timeout = setTimeout(() => {
				console.log(`⏰ Timeout sending to WebSocket ${connectionId}`);
				resolve(false);
			}, 100);

			try {
				connection.webSocket.send(data);
				clearTimeout(timeout);
				resolve(true);
			} catch (error) {
				clearTimeout(timeout);
				console.log(`❌ WebSocket send error for ${connectionId}:`, error);
				this.removeConnection(connectionId, 'send_error');
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
				readyState: conn.webSocket.readyState
			}))
		};
	}

	/**
	 * Broadcast data to all WebSocket connections
	 */
	async publishData(data: any): Promise<{ success: boolean; connectionsNotified: number }> {
		console.log("Publishing data to WebSocket connections");

		try {
			if (this.connections.size === 0) {
				console.log('No WebSocket connections to broadcast to');
				return { success: true, connectionsNotified: 0 };
			}

			console.log(`Broadcasting to ${this.connections.size} WebSocket connections`);

			// Format data as JSON message (without custom type to avoid browser warnings)
			const message = JSON.stringify({
				data: data,
				timestamp: Date.now()
			});

			// Send to all connections in parallel
			const broadcastTasks = Array.from(this.connections.entries()).map(([connectionId, connection]) => {
				return this.sendToConnection(connectionId, connection, message);
			});

			// Wait for all broadcasts to complete
			const results = await Promise.allSettled(broadcastTasks);

			// Count successful sends
			const successCount = results.filter(r => r.status === 'fulfilled' && r.value).length;
			const failedCount = results.length - successCount;

			if (failedCount > 0) {
				console.log(`🧹 Cleaned up ${failedCount} failed WebSocket connections`);
			}

			console.log(`WebSocket broadcast completed: ${successCount}/${this.connections.size} connections notified`);

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
	async publishBinaryData(binaryData: ArrayBuffer | Uint8Array): Promise<{ success: boolean; connectionsNotified: number }> {
		console.log("Publishing binary data to WebSocket connections, size:", binaryData.byteLength);

		try {
			if (this.connections.size === 0) {
				console.log('No WebSocket connections to broadcast to');
				return { success: true, connectionsNotified: 0 };
			}

			console.log(`Broadcasting binary data to ${this.connections.size} WebSocket connections`);

			// Convert to ArrayBuffer if it's Uint8Array
			const arrayBuffer = binaryData instanceof Uint8Array ? binaryData.buffer : binaryData;

			// Send to all connections in parallel
			const broadcastTasks = Array.from(this.connections.entries()).map(([connectionId, connection]) => {
				return this.sendToConnection(connectionId, connection, arrayBuffer);
			});

			// Wait for all broadcasts to complete
			const results = await Promise.allSettled(broadcastTasks);

			// Count successful sends
			const successCount = results.filter(r => r.status === 'fulfilled' && r.value).length;
			const failedCount = results.length - successCount;

			if (failedCount > 0) {
				console.log(`🧹 Cleaned up ${failedCount} failed WebSocket connections`);
			}

			console.log(`Binary broadcast completed: ${successCount}/${this.connections.size} connections notified`);

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
	 * RPC method to handle WebSocket connections
	 */
	async handleWebSocketRequest(request: Request): Promise<Response> {
		try {
			return await this.handleWebSocketConnection(request);
		} catch (error) {
			return new Response('Internal Server Error', { status: 500 });
		}
	}

	/**
	 * RPC method to get gateway statistics
	 */
	async getGatewayStats(): Promise<{ connectionCount: number; connections: any[] }> {
		return this.getStats();
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

				case '/publish-json':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}
					const jsonData = await request.json();
					const jsonResult = await this.publishData(jsonData);
					return new Response(JSON.stringify(jsonResult), {
						headers: { 'Content-Type': 'application/json' }
					});

				case '/publish-binary':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}
					const binaryData = await request.arrayBuffer();
					const binaryResult = await this.publishBinaryData(binaryData);
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

		console.log(`[FETCH] Created Durable Object instance with ID: ${id}`);

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

						// Create request to Durable Object for JSON publishing
						const publishRequest = new Request('http://localhost/publish-json', {
							method: 'POST',
							headers: { 'Content-Type': 'application/json' },
							body: JSON.stringify(jsonData)
						});

						// Wait for broadcast to complete
						const publishResponse = await gateway.fetch(publishRequest);
						const broadcastResult = await publishResponse.json() as { success: boolean; connectionsNotified: number };

						// Respond with broadcast results
						return new Response(JSON.stringify({
							success: broadcastResult.success,
							connectionsNotified: broadcastResult.connectionsNotified,
							message: `Data broadcast to ${broadcastResult.connectionsNotified} WebSocket connections`,
							timestamp: Date.now()
						}), {
							headers: { 'Content-Type': 'application/json' }
						});
					} else if (contentType.includes('application/octet-stream') || contentType.includes('application/binary')) {
						// Handle binary data
						const binaryData = await request.arrayBuffer();

						// Create request to Durable Object for binary publishing
						const publishRequest = new Request('http://localhost/publish-binary', {
							method: 'POST',
							headers: { 'Content-Type': 'application/octet-stream' },
							body: binaryData
						});

						// Wait for broadcast to complete
						const publishResponse = await gateway.fetch(publishRequest);
						const broadcastResult = await publishResponse.json() as { success: boolean; connectionsNotified: number };

						// Respond with broadcast results
						return new Response(JSON.stringify({
							success: broadcastResult.success,
							connectionsNotified: broadcastResult.connectionsNotified,
							message: `Binary data broadcast to ${broadcastResult.connectionsNotified} WebSocket connections`,
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
						return new Response(JSON.stringify(result.pools), {
							headers: { 'Content-Type': 'application/json' }
						});
					} catch (error) {
						console.error('Error listing pools:', error);
						return new Response(JSON.stringify({ error: 'Failed to list pools' }), {
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
		const tradePairId = url.searchParams.get('trade_pair_id') || '';
		const timeframe = url.searchParams.get('timeframe') || '60';
		const page_index = url.searchParams.get('page') || '1';

		if (tradePairId.toString() == '') {
			return new Response('Empty trade_pair_id', { status: 400 });
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
		const dateStr = `${yyyy}-${mm}-${dd}`;

		// Compose the key for this day's candle data
		const key = `${tradePairId}-${timeframe}-${dateStr}`;

		// Retrieve candle data for this day from KV
		const candleData = await env.KV.get(key, 'arrayBuffer');

		return new Response(candleData, {
			headers: { 'Content-Type': 'application/binary' }
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
		const tradePairId = url.searchParams.get('trade_pair_id') || '';
		const timeframe = url.searchParams.get('timeframe') || '60';

		if (tradePairId.toString() == '') {
			return new Response('Empty trade_pair_id', { status: 400 });
		}

		// Compose the key for this day's candle data
		const key = `${tradePairId}-${timeframe}-current`;

		// Retrieve candle data for this day from KV
		const candleData = await env.KV.get(key, 'arrayBuffer');

		return new Response(candleData, {
			headers: { 'Content-Type': 'application/binary' }
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
