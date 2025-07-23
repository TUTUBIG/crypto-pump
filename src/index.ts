import { DurableObject } from 'cloudflare:workers';

/**
 * SSE Gateway using Cloudflare Durable Objects
 *
 * This worker acts as a real-time gateway where:
 * - Clients can subscribe to trade data via SSE at /subscribe
 * - External services (Go service) can push data via /publish
 * - All connected clients receive broadcasted trade data
 */

interface SSEConnection {
	id: string;
	controller: ReadableStreamDefaultController;
	abortController: AbortController;
}

export class TradeDataGateway extends DurableObject<Env> {
	private connections: Map<string, SSEConnection> = new Map();
	private heartbeatInterval: number | null = null;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}

		/**
	 * Add a new SSE connection
	 */
	async addConnection(request: Request): Promise<Response> {
		const connectionId = crypto.randomUUID();

		// Create readable stream for SSE
		const { readable, writable } = new TransformStream();
		const writer = writable.getWriter();

		// Create abort controller for cleanup
		const abortController = new AbortController();

		// Create custom controller interface
		const customController = {
			enqueue: (chunk: Uint8Array) => {
				try {
					// Use the promise-based approach without await in enqueue
					writer.write(chunk).catch(error => {
						console.log(`Failed to write to connection ${connectionId}:`, error);
						this.removeConnection(connectionId);
					});
				} catch (error) {
					console.log(`Failed to write to connection ${connectionId}:`, error);
					this.removeConnection(connectionId);
				}
			}
		};

		// Store connection
		this.connections.set(connectionId, {
			id: connectionId,
			controller: customController as any,
			abortController
		});

		// Handle client disconnect
		request.signal?.addEventListener('abort', () => {
			console.log(`[NETWORK-LEVEL] TCP connection closed for ${connectionId}`);
			console.log(`[TECHNICAL] This is NOT sent over SSE - it's TCP FIN/RST detection`);
			writer.close().catch(() => {}); // Close the writer
			this.removeConnection(connectionId);
		});

		// Additional cleanup for server-side abort
		abortController.signal.addEventListener('abort', () => {
			console.log(`Connection aborted by server - Connection ID: ${connectionId}`);
			writer.close().catch(() => {}); // Close the writer
		});

		// Send initial data to force stream to start immediately

		// Send a small amount of padding to force immediate stream start
		const padding = ': SSE connection established\n\n';
		customController.enqueue(new TextEncoder().encode(padding));

		// Send welcome message
		const welcomeMessage = `event: connected\ndata: ${JSON.stringify({ connectionId, message: 'Connected to trade data stream from Wallet Kit' })}\n\n`;

		customController.enqueue(new TextEncoder().encode(welcomeMessage));


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
			},
		});
	}

	/**
	 * Remove a connection
	 */
	private removeConnection(connectionId: string) {
		const connection = this.connections.get(connectionId);
		if (connection) {
			connection.abortController.abort();
			this.connections.delete(connectionId);
			console.log(`Removed connection: ${connectionId}. Remaining connections: ${this.connections.size}`);
		}
	}

	/**
	 * Send data to a single connection with timeout and error handling
	 */
	private async sendToConnection(connectionId: string, connection: SSEConnection, data: Uint8Array): Promise<void> {
		return new Promise((resolve, reject) => {
			// Set timeout for slow connections
			const timeout = setTimeout(() => {
				reject(new Error(`Timeout sending to connection ${connectionId}`));
			}, 1000); // 1 second timeout

			try {
				// Don't await here to avoid blocking
				connection.controller.enqueue(data);
				clearTimeout(timeout);
				resolve();
			} catch (error) {
				clearTimeout(timeout);
				reject(error);
			}
		});
	}

	/**
	 * Get connection statistics
	 */
	getStats(): { connectionCount: number; connections: string[] } {
		return {
			connectionCount: this.connections.size,
			connections: Array.from(this.connections.keys())
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

			// Convert base64 string to Uint8Array before broadcasting
			const binaryData = Uint8Array.from(atob(base64Data), c => c.charCodeAt(0));

			// Create parallel tasks for each connection
			const broadcastTasks = Array.from(this.connections.entries()).map(([connectionId, connection]) => {
				return this.sendToConnection(connectionId, connection, binaryData);
			});

			// Execute all broadcasts in parallel with timeout
			const results = await Promise.allSettled(broadcastTasks);

			// Process results and clean up failed connections
			const deadConnections: string[] = [];
			results.forEach((result, index) => {
				if (result.status === 'rejected') {
					const connectionId = Array.from(this.connections.keys())[index];
					deadConnections.push(connectionId);
					console.log(`[ASYNC BROADCAST] Connection ${connectionId} failed:`, result.reason);
				}
			});

			// Remove dead connections
			deadConnections.forEach(id => this.removeConnection(id));

			const successCount = results.filter(r => r.status === 'fulfilled').length;
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
	async getGatewayStats(): Promise<{ connectionCount: number; connections: string[] }> {
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
					console.log(`[FETCH] Calling gateway.handleSSEConnection`);
					return await gateway.handleSSEConnection(request);

				case '/publish':
					if (request.method !== 'POST') {
						return new Response('Method not allowed', { status: 405 });
					}

					const contentType = request.headers.get('content-type') || '';

					if (contentType.includes('application/base64')) {
						// Handle raw binary data
						const binaryData = await request.arrayBuffer();
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

				case '/':
					console.log(`[FETCH] Handling root / request`);
					// Health check / info endpoint
					return new Response(JSON.stringify({
						service: 'Trade Data SSE Gateway',
						endpoints: {
							'/subscribe': 'GET - Subscribe to trade data via SSE',
							'/publish': 'POST - Publish base64 data (responds immediately, broadcasts async)',
							'/stats': 'GET - Get connection statistics',
							'/': 'GET - This info page'
						},
						features: [
							'Asynchronous broadcasting (immediate response)',
							'Parallel connection handling',
							'Automatic dead connection cleanup',
							'Base64 binary data support'
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
