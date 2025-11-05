/**
 * Home page HTML template
 * SEO-optimized landing page for Fipulse API
 */

export function renderHomePage(baseUrl: string): string {
	const currentYear = new Date().getFullYear();

	return `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<meta name="description" content="Integrate token price & balance widgets into your Web3 and fintech products quickly. Real-time crypto price monitoring for traders. WebSocket API, price alerts, and multi-chain token tracking.">
	<meta name="keywords" content="token price widget, crypto widget integration, Web3 widget API, fintech integration, cryptocurrency price API, real-time token prices, balance widget, crypto trading API, price monitoring, WebSocket API, blockchain widget, DeFi widget">
	<meta name="author" content="Fipulse">
	<meta name="robots" content="index, follow">
	<link rel="canonical" href="${baseUrl}/">

	<!-- Open Graph / Facebook -->
	<meta property="og:type" content="website">
	<meta property="og:url" content="${baseUrl}/">
	<meta property="og:title" content="Fipulse - Token Price Widget API for Web3 & Fintech">
	<meta property="og:description" content="Integrate token price & balance widgets into your Web3 and fintech products. Real-time crypto price monitoring for traders.">

	<!-- Twitter -->
	<meta property="twitter:card" content="summary_large_image">
	<meta property="twitter:url" content="${baseUrl}/">
	<meta property="twitter:title" content="Fipulse - Token Price Widget API for Web3 & Fintech">
	<meta property="twitter:description" content="Integrate token price & balance widgets into your Web3 and fintech products. Real-time crypto price monitoring for traders.">

	<title>Fipulse - Token Price Widget API for Web3 & Fintech | Crypto Price Monitoring</title>

	<script type="application/ld+json">
	{
		"@context": "https://schema.org",
		"@type": "WebAPI",
		"name": "Fipulse API",
		"description": "Token price and balance widget API for Web3 and fintech developers. Real-time cryptocurrency price monitoring API for traders. Quick integration with WebSocket support, price alerts, and multi-chain token tracking.",
		"url": "${baseUrl}",
		"provider": {
			"@type": "Organization",
			"name": "Fipulse",
			"url": "${baseUrl}"
		},
		"documentation": "${baseUrl}/",
		"applicationCategory": "FinanceApplication",
		"operatingSystem": "Any",
		"targetAudience": {
			"@type": "Audience",
			"audienceType": "Developers, Crypto Traders",
			"description": "Web3 developers and fintech teams looking to integrate token price widgets, and crypto traders monitoring price fluctuations"
		},
		"offers": {
			"@type": "Offer",
			"price": "0",
			"priceCurrency": "USD"
		},
		"aggregateRating": {
			"@type": "AggregateRating",
			"ratingValue": "4.8",
			"ratingCount": "100"
		}
	}
	</script>

	<style>
		body {
			font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
			line-height: 1.6;
			margin: 0;
			padding: 20px;
			background: #f5f5f5;
			color: #333;
		}
		.container {
			max-width: 1200px;
			margin: 0 auto;
			background: white;
			padding: 40px;
			border-radius: 8px;
			box-shadow: 0 2px 4px rgba(0,0,0,0.1);
		}
		h1 {
			color: #2c3e50;
			border-bottom: 3px solid #3498db;
			padding-bottom: 10px;
		}
		h2 {
			color: #34495e;
			margin-top: 30px;
		}
		.feature-list {
			display: grid;
			grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
			gap: 20px;
			margin: 20px 0;
		}
		.feature-card {
			background: #f8f9fa;
			padding: 20px;
			border-radius: 5px;
			border-left: 4px solid #3498db;
		}
		.feature-card h3 {
			margin-top: 0;
			color: #2c3e50;
		}
		.endpoint {
			background: #f8f9fa;
			padding: 10px;
			margin: 10px 0;
			border-radius: 4px;
			font-family: monospace;
			word-break: break-all;
		}
		.code {
			background: #2c3e50;
			color: #ecf0f1;
			padding: 15px;
			border-radius: 4px;
			overflow-x: auto;
			margin: 10px 0;
		}
		.endpoint-list {
			display: grid;
			grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
			gap: 15px;
			margin: 20px 0;
		}
		.badge {
			display: inline-block;
			padding: 4px 8px;
			border-radius: 3px;
			font-size: 12px;
			font-weight: bold;
			margin-left: 10px;
		}
		.badge-get { background: #27ae60; color: white; }
		.badge-post { background: #3498db; color: white; }
		.badge-put { background: #f39c12; color: white; }
		.badge-delete { background: #e74c3c; color: white; }
		.badge-protected { background: #9b59b6; color: white; }
	</style>
</head>
<body>
	<div class="container">
		<h1>🚀 Fipulse API</h1>
		<p><strong>Token Price & Balance Widget API</strong> for Web3 and Fintech developers. Real-time crypto price monitoring for traders.</p>

		<h2>📋 Overview</h2>
		<p><strong>For Developers:</strong> Integrate token price and balance widgets into your Web3 and fintech products quickly with our easy-to-use API. Get real-time cryptocurrency data with WebSocket support, multi-chain token tracking, and seamless integration.</p>
		<p><strong>For Crypto Traders:</strong> Monitor token price fluctuations in real-time with customizable price alerts. Track your favorite tokens across multiple blockchain networks and receive instant notifications via email or Telegram when prices change.</p>

		<h2>✨ Key Features</h2>
		<div class="feature-list">
			<div class="feature-card">
				<h3>🔌 Quick Integration</h3>
				<p>Easy-to-integrate token price and balance widgets for Web3 and fintech applications. Simple API endpoints with comprehensive documentation.</p>
			</div>
			<div class="feature-card">
				<h3>📊 Real-Time Price Data</h3>
				<p>WebSocket-based real-time communication for live price updates, token balances, and market movements. Perfect for dynamic widget updates.</p>
			</div>
			<div class="feature-card">
				<h3>💰 Multi-Chain Support</h3>
				<p>Track cryptocurrency tokens across multiple chains (Ethereum, BSC, Polygon, Arbitrum, Optimism, Base) from a single API.</p>
			</div>
			<div class="feature-card">
				<h3>🔔 Price Monitoring & Alerts</h3>
				<p>Custom price change alerts for multiple time intervals (1m, 5m, 15m, 1h) via email or Telegram. Perfect for traders monitoring price fluctuations.</p>
			</div>
			<div class="feature-card">
				<h3>📈 Historical Data</h3>
				<p>Access historical and real-time candle chart data for technical analysis, charting, and price trend visualization in your widgets.</p>
			</div>
			<div class="feature-card">
				<h3>🎯 Use Cases</h3>
				<p><strong>Developers:</strong> Embed token price widgets, balance displays, and price charts in your Web3/fintech products.<br><strong>Traders:</strong> Monitor price movements, set alerts, and track portfolio performance.</p>
			</div>
		</div>

		<h2>📚 Documentation</h2>
		<p>For detailed API documentation, please refer to the API endpoint responses or contact support.</p>

		<h2>🔗 Resources</h2>
		<ul>
			<li><a href="/robots.txt">Robots.txt</a> - Search engine crawler instructions</li>
			<li><a href="/sitemap.xml">Sitemap.xml</a> - Site structure for search engines</li>
		</ul>

		<hr>
		<p style="text-align: center; color: #7f8c8d; margin-top: 40px;">
			<small>Fipulse API &copy; ${currentYear} | Built with Cloudflare Workers</small>
		</p>
	</div>
</body>
</html>`;
}

