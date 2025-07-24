import { DurableObject } from 'cloudflare:workers';

/**
 * SSE Gateway using Cloudflare Durable Objects + Pool Information Management
 *
 * This worker acts as a real-time gateway where:
 * - Clients can subscribe to trade data via SSE at /subscribe
 * - External services (Go service) can push data via /publish
 * - All connected clients receive broadcasted trade data
 * - Store and retrieve pool information using D1 database
 */

interface PoolInfo {
	ChainId: string;
	Protocol: string;
	PoolAddress: string;
	PoolName: string;
	CostTokenAddress: string;
	CostTokenSymbol: string;
	CostTokenDecimals: number;
	GetTokenAddress: string;
	GetTokenSymbol: string;
	GetTokenDecimals: number;
}

interface PoolInfoWithId extends PoolInfo {
	id: number;
	created_at: string;
	updated_at: string;
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

interface SSEConnection {
	id: string;
	controller: ReadableStreamDefaultController;
	abortController: AbortController;
	writer: WritableStreamDefaultWriter; // For proper cleanup
	consecutiveTimeouts: number; // Count consecutive 100ms timeouts
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
		poolData.ChainId,
		poolData.Protocol,
		poolData.PoolAddress,
		poolData.PoolName,
		poolData.CostTokenAddress,
		poolData.CostTokenSymbol,
		poolData.CostTokenDecimals,
		poolData.GetTokenAddress,
		poolData.GetTokenSymbol,
		poolData.GetTokenDecimals
	).run();

	if (!result.success) {
		throw new Error('Failed to insert pool data');
	}

	return {
		success: true,
		id: Number(result.meta.last_row_id) || 0
	};
}

async function listPools(
	db: D1Database,
	page: number = 1,
	pageSize: number = 20,
	chainId?: string | null,
	protocol?: string | null
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

	// Get total count
	const countQuery = `SELECT COUNT(*) as count FROM pool_info${whereClause}`;
	const countResult = await db.prepare(countQuery).bind(...bindings).first();
	const total = Number(countResult?.count) || 0;

	// Get paginated results
	const dataQuery = `
		SELECT
			id, chain_id, protocol, pool_address, pool_name,
			cost_token_address, cost_token_symbol, cost_token_decimals,
			get_token_address, get_token_symbol, get_token_decimals,
			created_at, updated_at
		FROM pool_info${whereClause}
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`;

	const dataResult = await db.prepare(dataQuery)
		.bind(...bindings, pageSize, offset)
		.all();

	const pools: PoolInfoWithId[] = dataResult.results.map((row: any) => ({
		id: row.id,
		ChainId: row.chain_id,
		Protocol: row.protocol,
		PoolAddress: row.pool_address,
		PoolName: row.pool_name,
		CostTokenAddress: row.cost_token_address,
		CostTokenSymbol: row.cost_token_symbol,
		CostTokenDecimals: row.cost_token_decimals,
		GetTokenAddress: row.get_token_address,
		GetTokenSymbol: row.get_token_symbol,
		GetTokenDecimals: row.get_token_decimals,
		created_at: row.created_at,
		updated_at: row.updated_at
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

export class TradeDataGateway extends DurableObject<Env> {
	private connections: Map<string, SSEConnection> = new Map();

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);

		// Add global error handler to catch "Network connection lost" errors
		this.setupGlobalErrorHandler();
	}

	/**
	 * Setup global error handler to catch unhandled network errors
	 */
	private setupGlobalErrorHandler() {
		// Listen for unhandled promise rejections
		self.addEventListener('unhandledrejection', (event) => {
			console.log(`🚨 GLOBAL ERROR CAUGHT:`, event.reason);

			// Check if this is a network connection error
			if (event.reason && typeof event.reason === 'object') {
				const error = event.reason as Error;
				if (error.message.includes('Network connection lost') ||
					error.message.includes('NetworkError') ||
					error.message.includes('connection')) {

					console.log(`🚨 NETWORK CONNECTION ERROR DETECTED GLOBALLY`);

					// Mark all connections as potentially unhealthy
					for (const [connectionId, connection] of this.connections) {
						console.log(`💀 Marking connection ${connectionId} as potentially unhealthy due to global network error`);
						connection.consecutiveTimeouts = 5;
					}
				}
			}

			// Prevent the error from being unhandled
			event.preventDefault();
		});
	}

	/**
	 * Add a new SSE connection
	 */
	async addConnection(request: Request): Promise<Response> {
		const connectionId = crypto.randomUUID();
		console.log(`🔗 NEW CONNECTION: ${connectionId}`);

		// Create readable stream for SSE
		const { readable, writable } = new TransformStream();
		const writer = writable.getWriter();

		// Create abort controller for cleanup
		const abortController = new AbortController();

		// Controller that captures "Network connection lost" errors
		const customController = {
			enqueue: (chunk: Uint8Array) => {
				try {
					writer.write(chunk).catch((error) => {
						console.log(`🚨 NETWORK CONNECTION LOST for ${connectionId}:`, error);
						// Immediately mark connection as unhealthy
						const connection = this.connections.get(connectionId);
						if (connection) {
							connection.consecutiveTimeouts = 5; // Force unhealthy
							console.log(`💀 Connection ${connectionId} marked UNHEALTHY due to network error`);
						}
					});
				} catch (error) {
					console.log(`🚨 SYNC NETWORK ERROR for ${connectionId}:`, error);
					// Immediately mark connection as unhealthy
					const connection = this.connections.get(connectionId);
					if (connection) {
						connection.consecutiveTimeouts = 5; // Force unhealthy
						console.log(`💀 Connection ${connectionId} marked UNHEALTHY due to sync network error`);
					}
				}
			}
		};

		// Store connection with writer for cleanup
		this.connections.set(connectionId, {
			id: connectionId,
			controller: customController as any,
			abortController,
			writer,
			consecutiveTimeouts: 0
		});

		// Handle client disconnect
		request.signal?.addEventListener('abort', () => {
			console.log(`🔌 CLIENT DISCONNECTED: Connection ${connectionId} closed by client`);
			console.log(`📊 Connection details: TCP connection terminated (FIN/RST packet received)`);
			console.log(`📈 Active connections before cleanup: ${this.connections.size}`);
			writer.close().catch(() => {}); // Close the writer
			this.removeConnection(connectionId, 'client_disconnect');
		});


		// Additional cleanup for server-side abort
		abortController.signal.addEventListener('abort', () => {
			console.log(`Connection aborted by server - Connection ID: ${connectionId}`);
			writer.close().catch(() => {}); // Close the writer
		});


		// Send padding to force immediate stream start (prevents browser buffering)
		const padding = ': SSE connection established\n\n';
		customController.enqueue(new TextEncoder().encode(padding));

		// Send welcome message
		const welcomeMessage = `data: ${JSON.stringify({ connectionId, message: 'Connected to trade data stream from Wallet Kit' })}\n\n`;
		customController.enqueue(new TextEncoder().encode(welcomeMessage));

		// Send another small chunk to ensure stream is fully active
		const activationChunk = ': Stream active\n\n';
		customController.enqueue(new TextEncoder().encode(activationChunk));

		console.log(`✅ SSE connection established: ${connectionId} (total: ${this.connections.size})`);


		// Return SSE response with proper headers for immediate streaming
		return new Response(readable, {
			headers: {
				'Content-Type': 'text/event-stream; charset=utf-8',
				'Cache-Control': 'no-cache, no-store, must-revalidate',
				'Connection': 'keep-alive',
				'Access-Control-Allow-Origin': '*',
				'Access-Control-Allow-Headers': 'Cache-Control',
				'X-Accel-Buffering': 'no', // Disable nginx buffering
				'Transfer-Encoding': 'chunked', // Force chunked encoding
				'Content-Encoding': 'identity', // Prevent compression buffering
			},
		});
	}

	/**
	 * Remove a connection and clean up resources
	 */
	private removeConnection(connectionId: string, reason: string = 'unknown') {
		const connection = this.connections.get(connectionId);
		if (connection) {
			// Abort the connection
			connection.abortController.abort();

			// Close the writer to free up resources
			try {
				connection.writer.close().catch(() => {
					// Ignore close errors - writer might already be closed
				});
			} catch (error) {
				// Ignore sync close errors
			}

			// Remove from connections map
			this.connections.delete(connectionId);

			console.log(`❌ REMOVED CONNECTION: ${connectionId} (reason: ${reason})`);
		}
	}

		/**
	 * Send data with aggressive dead connection detection
	 */
	private async sendToConnection(connectionId: string, connection: SSEConnection, data: Uint8Array): Promise<boolean> {
		return new Promise((resolve) => {
			// Check if connection was already marked unhealthy
			if (connection.consecutiveTimeouts >= 5) {
				console.log(`💀 Connection ${connectionId} already marked UNHEALTHY - removing immediately`);
				this.removeConnection(connectionId, 'network_error_detected');
				resolve(false);
				return;
			}

			// Very short timeout to catch dead connections quickly
			const timeout = setTimeout(() => {
				connection.consecutiveTimeouts++;
				console.log(`⏰ Timeout #${connection.consecutiveTimeouts} for connection ${connectionId}`);

				if (connection.consecutiveTimeouts >= 5) {
					console.log(`💀 Connection ${connectionId} marked UNHEALTHY after 5 consecutive timeouts`);
					this.removeConnection(connectionId, 'timeout_unhealthy');
				}
				resolve(false);
			}, 50); // Reduced to 50ms for faster detection

			try {
				// Use enqueue instead of direct writer operations
				connection.controller.enqueue(data);

				// Small delay to allow the writer to complete
				setTimeout(() => {
					clearTimeout(timeout);
					connection.consecutiveTimeouts = 0; // Reset on success
					resolve(true);
				}, 10);
			} catch (error) {
				clearTimeout(timeout);
				console.log(`❌ Sync write error for connection ${connectionId}:`, error);
				connection.consecutiveTimeouts = 5; // Mark as unhealthy
				this.removeConnection(connectionId, 'write_error');
				resolve(false);
			}
		});
	}

	/**
	 * Get connection statistics - simplified
	 */
	getStats(): { connectionCount: number; connections: any[] } {
		return {
			connectionCount: this.connections.size,
			connections: Array.from(this.connections.values()).map(conn => ({
				id: conn.id,
				consecutiveTimeouts: conn.consecutiveTimeouts,
				isHealthy: conn.consecutiveTimeouts < 5,
				serverAborted: conn.abortController.signal.aborted
			}))
		};
	}

		/**
	 * RPC method to handle data (runs asynchronously)
	 */
	async publishBinaryData(base64Data: string): Promise<{ success: boolean; connectionsNotified: number }> {
		try {
			if (this.connections.size === 0) {
				console.log('No connections to broadcast binary data to');
				return { success: true, connectionsNotified: 0 };
			}

			console.log(`[ASYNC BROADCAST] Starting broadcast to ${this.connections.size} connections`);

			// Format data as simple SSE message (no event field needed)
			const sseEvent = `data: ${base64Data}\n\n`;
			const sseData = new TextEncoder().encode(sseEvent);

			console.log(`[ASYNC BROADCAST] Sending SSE data: ${sseEvent.substring(0, 100)}...`);

			// Create parallel tasks for each connection
			const broadcastTasks = Array.from(this.connections.entries()).map(([connectionId, connection]) => {
				return this.sendToConnection(connectionId, connection, sseData);
			});

			// Execute all broadcasts in parallel with timeout
			const results = await Promise.allSettled(broadcastTasks);

			// Process results and clean up failed connections
			const deadConnections: string[] = [];
			results.forEach((result, index) => {
				const connectionId = Array.from(this.connections.keys())[index];
				if (result.status === 'rejected') {
					deadConnections.push(connectionId);
					console.log(`[ASYNC BROADCAST] Connection ${connectionId} failed:`, result.reason);
				} else if (result.status === 'fulfilled' && !result.value) {
					deadConnections.push(connectionId);
					console.log(`[ASYNC BROADCAST] Connection ${connectionId} failed to send data`);
				}
			});

			// Remove dead connections
			deadConnections.forEach(id => this.removeConnection(id, 'broadcast_failed'));

			const successCount = results.filter(r => r.status === 'fulfilled' && r.value).length;
			console.log(`[ASYNC BROADCAST] Completed: ${successCount}/${this.connections.size} connections notified`);

			return {
				success: true,
				connectionsNotified: successCount
			};
		} catch (error) {
			console.error('[ASYNC BROADCAST] Error during broadcast:', error);
			return {
				success: false,
				connectionsNotified: 0
			};
		}
	}

	/**
	 * RPC method to handle SSE connections
	 */
	async handleSSEConnection(request: Request): Promise<Response> {
		try {
			return await this.addConnection(request);
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
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;

		console.log(`[FETCH] Incoming request: ${request.method} ${path}`);

		// Create Durable Object instance
		const id: DurableObjectId = env.TRADE_GATEWAY.idFromName("main-gateway");
		const gateway = env.TRADE_GATEWAY.get(id);

		console.log(`[FETCH] Created Durable Object instance with ID: ${id}`);

		// For global distribution, you could use:
		// const region = getRegionFromRequest(request); // 'us', 'eu', 'asia'
		// const id: DurableObjectId = env.TRADE_GATEWAY.idFromName(`gateway-${region}`);
		// This creates separate instances per region for lower latency

		try {
			switch (path) {
				case '/subscribe':
					console.log(`[FETCH] Handling /subscribe request`);
					// Handle SSE client connections
					if (request.method !== 'GET') {
						return new Response('Method not allowed', { status: 405 });
					}
					return await gateway.handleSSEConnection(request);

				case '/publish':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}

					const contentType = request.headers.get('content-type') || '';

					if (contentType.includes('application/base64')) {
						// Handle raw binary data
						const binaryData = await request.arrayBuffer();
						if (binaryData.byteLength == 0) {
							return new Response('Empty data', { status: 400 });
						}
						const base64Data = btoa(String.fromCharCode(...new Uint8Array(binaryData)));

						// Start async broadcast - don't await it
						const broadcastPromise = gateway.publishBinaryData(base64Data);

						// Use waitUntil to ensure broadcast completes even after response is sent
						ctx.waitUntil(broadcastPromise);

						// Respond immediately without waiting for broadcast to complete
						return new Response(JSON.stringify({
							success: true,
							message: 'Data received and broadcasting asynchronously',
							timestamp: Date.now()
						}), {
							headers: { 'Content-Type': 'application/json' }
						});
					} else {
						return new Response('Unsupported content type. Use application/base64', { status: 415 });
					}

				case '/stats':
					// Get gateway statistics
					const stats = await gateway.getGatewayStats();
					return new Response(JSON.stringify(stats), {
						headers: { 'Content-Type': 'application/json' }
					});

				case '/pools':
					// List pools with pagination
					if (request.method !== 'GET') {
						return new Response('Method not allowed', { status: 405 });
					}

					try {
						const searchParams = url.searchParams;
						const page = parseInt(searchParams.get('page') || '1');
						const pageSize = Math.min(parseInt(searchParams.get('pageSize') || '20'), 100); // Max 100 per page
						const chainId = searchParams.get('chainId');
						const protocol = searchParams.get('protocol');

						const result = await listPools(env.DB, page, pageSize, chainId, protocol);
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

				case '/pools/add':
					// Add new pool
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}

					try {
						const poolData: PoolInfo = await request.json();

						// Validate required fields
						if (!poolData.ChainId || !poolData.PoolAddress || !poolData.Protocol) {
							return new Response(JSON.stringify({
								error: 'Missing required fields: ChainId, PoolAddress, Protocol'
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

				case '/':
					console.log(`[FETCH] Handling root / request`);
					// Health check / info endpoint
					return new Response(JSON.stringify({
						service: 'Trade Data SSE Gateway',
						endpoints: {
							'/subscribe': 'GET - Subscribe to trade data via SSE',
							'/publish': 'POST - Publish base64 data (responds immediately, broadcasts async)',
							'/stats': 'GET - Get connection statistics',
							'/pools': 'GET - List pools with pagination (?page=1&pageSize=20&chainId=1&protocol=uniswap)',
							'/pools/add': 'POST - Add new pool information',
							'/': 'GET - This info page'
						},
						features: [
							'100ms timeout + immediate network error capture',
							'Unhealthy after 5 consecutive timeouts OR any network error',
							'Immediate removal of unhealthy connections',
							'No background heartbeat overhead'
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
