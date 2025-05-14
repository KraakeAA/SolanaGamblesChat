// lib/solana-connection.js - Enhanced for Multi-RPC, Retries, and Rate Limiting
// --- VERSION: 2.3.1-super-constructor-fix ---

import { Connection } from '@solana/web3.js';
import PQueue from 'p-queue';

// --- Define your specific endpoint details with hardcoded API keys ---
const HELIUS_HTTPS_URL_1 = 'https://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';
const HELIUS_WSS_URL_1 = 'wss://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';

const HELIUS_HTTPS_URL_2 = 'https://mainnet.helius-rpc.com/?api-key=d399a59e-2d9c-43c3-9775-e21d3b3ea00f';
const HELIUS_WSS_URL_2 = 'wss://mainnet.helius-rpc.com/?api-key=d399a59e-2d9c-43c3-9775-e21d3b3ea00f';

const QUICKNODE_HTTPS_URL = 'https://multi-young-tab.solana-mainnet.quiknode.pro/56662595a48eb3798b005654091f77aa5673e15e/';
const QUICKNODE_WSS_URL = 'wss://multi-young-tab.solana-mainnet.quiknode.pro/56662595a48eb3798b005654091f77aa5673e15e/';

const HARCODED_RPC_ENDPOINTS = [
    { http: HELIUS_HTTPS_URL_1, ws: HELIUS_WSS_URL_1, provider: 'helius1' },
    { http: HELIUS_HTTPS_URL_2, ws: HELIUS_WSS_URL_2, provider: 'helius2' },
    { http: QUICKNODE_HTTPS_URL, ws: QUICKNODE_WSS_URL, provider: 'quicknode' },
    // Example public fallback
    // { 
    //     http: 'https://api.mainnet-beta.solana.com', 
    //     ws: 'wss://api.mainnet-beta.solana.com', 
    //     provider: 'solana-public' 
    // }
];

if (HARCODED_RPC_ENDPOINTS.length === 0) {
    console.warn("[RateLimitedConnection] WARNING: The HARCODED_RPC_ENDPOINTS array is empty. RPC functionality will rely solely on URLs from RPC_URLS_ENV or fail.");
}
// --- End endpoint definitions ---


function isInternalRetryableSolanaError(error) {
    if (!error) return false;
    const message = String(error.message || '').toLowerCase();
    const retryableMessages = [
        'timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error',
        'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch',
        'getaddrinfo enotfound', 'connection refused', 'connection reset by peer', 'etimedout',
        'transaction simulation failed', 'failed to simulate transaction', 'blockhash not found',
        'slot leader does not match', 'node is behind', 'transaction was not confirmed',
        'block not available', 'block cleanout', 'sending transaction', 'connection closed',
        'load balancer error', 'backend unhealthy', 'overloaded', 'proxy internal error',
        'too many requests', 'rate limit exceeded', 'unknown block', 'leader not ready',
        'heavily throttled', 'failed to query long-term storage', 'rpc node error',
        'temporarily unavailable', 'service unavailable'
    ];
    if (retryableMessages.some(m => message.includes(m))) return true;
    
    const status = error?.response?.status || error?.statusCode || error?.status || error?.code;
    if (status) {
        if ([408, 429, 500, 502, 503, 504].includes(Number(status))) return true;
        if (typeof status === 'string' && ['ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED', 'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT'].includes(status.toUpperCase())) return true;
    }
    
    const rpcErrorCode = error?.code;
    if (rpcErrorCode && Number.isInteger(rpcErrorCode) && rpcErrorCode <= -32000 && rpcErrorCode >= -32099) {
        if ([-32000, -32001, -32002, -32003, -32004, -32005, -32007, -32008, -32009, -32010, -32011, -32014, -32015, -32016].includes(Number(rpcErrorCode))) return true;
    }

    if (error?.data?.message?.toLowerCase().includes('rate limit exceeded') ||
        error?.data?.message?.toLowerCase().includes('too many requests')) {
        return true;
    }
    
    return false;
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

class RateLimitedConnection extends Connection {
    constructor(endpointsFromEnv = [], options = {}) { 
        let effectiveRpcEndpoints = [];
        const envUrls = Array.isArray(endpointsFromEnv) ? endpointsFromEnv.filter(u => u) : (endpointsFromEnv ? [endpointsFromEnv] : []);

        if (envUrls.length > 0) {
            envUrls.forEach(url => {
                if (url.startsWith('http')) { 
                    const matchingHardcoded = HARCODED_RPC_ENDPOINTS.find(hardcoded => 
                        url.startsWith(hardcoded.http.split('?')[0]) 
                    );
                    if (matchingHardcoded) { 
                        if (!effectiveRpcEndpoints.find(ep => ep.http === matchingHardcoded.http)) {
                             effectiveRpcEndpoints.push(matchingHardcoded);
                        }
                    } else { 
                         if (!effectiveRpcEndpoints.find(ep => ep.http === url)) {
                            effectiveRpcEndpoints.push({
                                http: url,
                                ws: url.replace(/^http/, 'ws'), 
                                provider: new URL(url).hostname 
                            });
                        }
                    }
                }
            });
        }

        if (effectiveRpcEndpoints.length === 0) {
            if (envUrls.length > 0) {
                 console.warn(`[RateLimitedConnection] Provided environment URLs did not match any hardcoded configurations. Defaulting to all available hardcoded RPCs.`);
            } else {
                console.log(`[RateLimitedConnection] No specific RPCs provided via environment. Defaulting to all available hardcoded RPCs.`);
            }
            effectiveRpcEndpoints = [...HARCODED_RPC_ENDPOINTS];
        }
        
        if (effectiveRpcEndpoints.length === 0) {
            console.error("[RateLimitedConnection] CRITICAL: No RPC endpoints available after all checks. RPC calls will fail.");
            throw new Error("RateLimitedConnection: No effective RPC endpoints available. Bot cannot operate.");
        }

        const primaryHttpEndpoint = effectiveRpcEndpoints[0].http;
        // Determine primaryWsEndpoint based on options first, then from the selected primary HTTP endpoint.
        // The this.initialWsEndpointUserProvided flag will be set *after* super()
        let determinedPrimaryWsEndpoint = (options && options.wsEndpoint) ? options.wsEndpoint : effectiveRpcEndpoints[0].ws;


        const defaultBotVersion = typeof process !== 'undefined' && process.env && process.env.BOT_VERSION ? process.env.BOT_VERSION : 'unknown_version';
        const defaultNodeEnv = typeof process !== 'undefined' && process.env && process.env.NODE_ENV ? process.env.NODE_ENV : 'unknown_env';
        const defaultClientId = `SolanaCasinoBot/${defaultBotVersion} (env: ${defaultNodeEnv})`;
        
        const safeOptions = options || {}; // Ensure options is an object
        const clientId = safeOptions.clientId || defaultClientId;

        const connectionConfig = {
            commitment: safeOptions.commitment || 'confirmed',
            wsEndpoint: determinedPrimaryWsEndpoint, // Use the determined WSS endpoint
            httpHeaders: {
                'Content-Type': 'application/json',
                'User-Agent': clientId, 
                'X-Client-Version': defaultBotVersion,
                ...(safeOptions.httpHeaders || {})
            },
            fetch: typeof fetch !== 'undefined' ? fetch : undefined,
            disableRetryOnRateLimit: true, 
        };

        super(primaryHttpEndpoint, connectionConfig); // Call super() constructor first

        // Now it's safe to use 'this'
        this.initialWsEndpointUserProvided = !!(options && options.wsEndpoint); // Set the flag correctly

        this.allEffectiveEndpoints = effectiveRpcEndpoints; 
        this.currentEndpointInternalIndex = 0; 
        this.clientIdentity = clientId;

        this.maxRetries = safeOptions.maxRetries && Number.isInteger(safeOptions.maxRetries) ? safeOptions.maxRetries : 3;
        this.retryBaseDelay = safeOptions.retryBaseDelay && Number.isInteger(safeOptions.retryBaseDelay) ? safeOptions.retryBaseDelay : 750;
        this.retryMaxDelay = safeOptions.retryMaxDelay && Number.isInteger(safeOptions.retryMaxDelay) ? safeOptions.retryMaxDelay : 25000;
        this.retryJitter = safeOptions.retryJitter && !isNaN(safeOptions.retryJitter) ? safeOptions.retryJitter : 0.3;
        this.rateLimitCooloff = safeOptions.rateLimitCooloff && Number.isInteger(safeOptions.rateLimitCooloff) ? safeOptions.rateLimitCooloff : 3000;

        this.requestQueue = new PQueue({ concurrency: safeOptions.maxConcurrent || 10 });

        console.log(`[RateLimitedConnection] Initialized. Primary RPC: ${this.rpcEndpoint} (Provider: ${this.getCurrentEndpointDetail().provider}, WSS: ${this._wsEndpoint || 'Not set'}). Effective Pool Size: ${this.allEffectiveEndpoints.length}. User-Agent: "${clientId}"`);
    }

    switchToNextEndpoint() {
        if (this.allEffectiveEndpoints.length <= 1) {
            console.warn(`[RateLimitedConnection] No alternative RPCs to switch from ${this.rpcEndpoint}.`);
            return false;
        }
        const oldProvider = this.allEffectiveEndpoints[this.currentEndpointInternalIndex]?.provider;
        this.currentEndpointInternalIndex = (this.currentEndpointInternalIndex + 1) % this.allEffectiveEndpoints.length;
        const newEndpointDetail = this.allEffectiveEndpoints[this.currentEndpointInternalIndex];

        this._rpcEndpoint = newEndpointDetail.http;
        // Only update WSS if it wasn't explicitly set by the user during initial construction
        if (newEndpointDetail.ws && !this.initialWsEndpointUserProvided) {
            this._wsEndpoint = newEndpointDetail.ws;
        }

        console.warn(`[RateLimitedConnection] Switched RPC endpoint from ${oldProvider || 'unknown'} to: ${this.rpcEndpoint} (Provider: ${newEndpointDetail.provider}, WSS: ${this._wsEndpoint || 'N/A'})`);
        return true;
    }

    async _rpcRequest(method, args) {
        return this.requestQueue.add(async () => {
            let attempts = 0;
            let currentDelay = this.retryBaseDelay;
            let cycleDetectionIndex = this.currentEndpointInternalIndex;

            while (true) {
                attempts++;
                const requestId = `rpc_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                const currentEndpointDetail = this.allEffectiveEndpoints[this.currentEndpointInternalIndex];
                const currentProvider = currentEndpointDetail?.provider || 'unknown';
                
                try {
                    const result = await super._rpcRequest(method, args);
                    return result;
                } catch (error) {
                    console.warn(`[RateLimitedConnection] RPC Error (ID: ${requestId}, Try ${attempts}, Prov: ${currentProvider}, Endpoint: ${this.rpcEndpoint}). Method: ${method}. Err: ${error.message.substring(0,100)}...`);
                    
                    const isRetryable = isInternalRetryableSolanaError(error);
                    const httpStatus = error?.response?.status || error?.statusCode || error?.status;

                    if (attempts > this.maxRetries || !isRetryable) {
                        console.error(`[RateLimitedConnection] RPC (ID: ${requestId}) - Unretryable or max retries (${this.maxRetries}) for ${method} on ${currentProvider}. Final Error: ${error.message}`);
                        throw error;
                    }

                    let switchedEndpointThisTry = false;
                    if (httpStatus === 429 || String(error.message).toLowerCase().includes("rate limit") || String(error.message).toLowerCase().includes("too many requests")) {
                        console.warn(`[RateLimitedConnection] RPC (ID: ${requestId}) - Rate limit (Status: ${httpStatus || 'N/A'}) from ${currentProvider}. Cooling ${this.rateLimitCooloff}ms.`);
                        await sleep(this.rateLimitCooloff);
                        if (this.allEffectiveEndpoints.length > 1) {
                            switchedEndpointThisTry = this.switchToNextEndpoint();
                        }
                    } else if (isRetryable && this.allEffectiveEndpoints.length > 1) {
                        switchedEndpointThisTry = this.switchToNextEndpoint();
                    }
                    
                    if (switchedEndpointThisTry) {
                        currentDelay = this.retryBaseDelay; 
                        if (this.currentEndpointInternalIndex === cycleDetectionIndex && attempts > 1) { 
                            console.warn(`[RateLimitedConnection] RPC (ID: ${requestId}) - Cycled all ${this.allEffectiveEndpoints.length} RPCs for ${method}. Increasing base delay for next cycle.`);
                            currentDelay = Math.min(this.retryMaxDelay, this.retryBaseDelay * (Math.floor(attempts / this.allEffectiveEndpoints.length) + 1) ); 
                        }
                        cycleDetectionIndex = this.currentEndpointInternalIndex;
                    }

                    const jitterValue = currentDelay * this.retryJitter * (Math.random() * 2 - 1); 
                    const delayWithJitter = Math.max(100, Math.min(this.retryMaxDelay, Math.round(currentDelay + jitterValue)));
                    
                    console.warn(`[RateLimitedConnection] RPC (ID: ${requestId}) - Retryable error on ${method}. Retrying in ${delayWithJitter}ms. (Next RPC: ${this.rpcEndpoint} - ${this.allEffectiveEndpoints[this.currentEndpointInternalIndex]?.provider}).`);
                    await sleep(delayWithJitter);
                    
                    if (!switchedEndpointThisTry) { 
                        currentDelay = Math.min(this.retryMaxDelay, currentDelay * (1.5 + Math.random())); 
                    }
                }
            }
        });
    }

    getCurrentEndpointDetail() {
        return this.allEffectiveEndpoints[this.currentEndpointInternalIndex] || {http: this.rpcEndpoint, ws: this._wsEndpoint, provider: 'unknown (current)'};
    }
}

export default RateLimitedConnection;
