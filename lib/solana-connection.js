// lib/solana-connection.js - Enhanced for Multi-RPC (Hardcoded with Allow-list), Retries, and Rate Limiting
// --- VERSION: 2.1.0-hardcoded-rpc ---

import { Connection } from '@solana/web3.js';
import PQueue from 'p-queue';

// --- Define your specific endpoint details ---
const HELIUS_HTTPS_URL_1 = 'https://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';
const HELIUS_WSS_URL_1 = 'wss://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';

const HELIUS_HTTPS_URL_2 = 'https://mainnet.helius-rpc.com/?api-key=d399a59e-2d9c-43c3-9775-e21d3b3ea00f';
const HELIUS_WSS_URL_2 = 'wss://mainnet.helius-rpc.com/?api-key=d399a59e-2d9c-43c3-9775-e21d3b3ea00f';

const QUICKNODE_HTTPS_URL = 'https://multi-young-tab.solana-mainnet.quiknode.pro/56662595a48eb3798b005654091f77aa5673e15e/';
const QUICKNODE_WSS_URL = QUICKNODE_HTTPS_URL.replace(/^https:/, 'wss:');

const HARCODED_RPC_ENDPOINTS = [
    { http: HELIUS_HTTPS_URL_1, ws: HELIUS_WSS_URL_1, provider: 'helius1' },
    { http: HELIUS_HTTPS_URL_2, ws: HELIUS_WSS_URL_2, provider: 'helius2' },
    { http: QUICKNODE_HTTPS_URL, ws: QUICKNODE_WSS_URL, provider: 'quicknode' },
    // Add more pairs if needed
];
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
    if (status && [408, 429, 500, 502, 503, 504].includes(Number(status))) return true;
    if (typeof status === 'string' && ['ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED', 'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT'].includes(status.toUpperCase())) return true;
    const rpcErrorCode = error?.code;
    if (rpcErrorCode && [-32000, -32001, -32002, -32003, -32004, -32005, -32007, -32008, -32009, -32010, -32011, -32014].includes(Number(rpcErrorCode))) return true;
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
        const envUrls = Array.isArray(endpointsFromEnv) ? endpointsFromEnv : (endpointsFromEnv ? [endpointsFromEnv] : []);

        if (envUrls.length > 0) {
            // Filter hardcoded list based on environment variable allow-list
            effectiveRpcEndpoints = HARCODED_RPC_ENDPOINTS.filter(hardcoded => envUrls.includes(hardcoded.http));
        }

        // If no matches from env var or env var was empty, use all hardcoded HTTPS endpoints
        if (effectiveRpcEndpoints.length === 0) {
            console.warn(`[RateLimitedConnection] No matching RPCs from environment variable allow-list, or allow-list empty. Defaulting to all hardcoded HTTPS endpoints.`);
            effectiveRpcEndpoints = HARCODED_RPC_ENDPOINTS.map(ep => ep); // Create a new array copy
        }

        if (effectiveRpcEndpoints.length === 0) {
            // This case should ideally not be reached if HARCODED_RPC_ENDPOINTS is not empty.
            throw new Error("RateLimitedConnection: No effective RPC endpoints available after filtering and defaulting. Check hardcoded list and environment variables.");
        }

        const primaryHttpEndpoint = effectiveRpcEndpoints[0].http;
        // Attempt to find a matching WSS endpoint for the primary HTTP endpoint.
        // If options.wsEndpoint is explicitly provided, it will override this.
        let primaryWsEndpoint = effectiveRpcEndpoints[0].ws || options.wsEndpoint;


        const defaultClientId = `SolanaCasinoBot/${process.env.BOT_VERSION || 'unknown'} (env: ${process.env.NODE_ENV || 'development'})`;
        const clientId = options.clientId || defaultClientId;

        const connectionConfig = {
            commitment: options.commitment || 'confirmed',
            wsEndpoint: primaryWsEndpoint, // Use determined or provided WSS endpoint
            httpHeaders: {
                'Content-Type': 'application/json',
                'solana-client': clientId,
                ...(options.httpHeaders || {})
            },
            fetch: typeof fetch !== 'undefined' ? fetch : undefined,
            disableRetryOnRateLimit: true,
        };

        super(primaryHttpEndpoint, connectionConfig);

        this.allEffectiveEndpoints = effectiveRpcEndpoints; // Array of {http, ws, provider} objects
        this.currentEndpointInternalIndex = 0; // Index for this.allEffectiveEndpoints
        this.clientIdentity = clientId;

        this.maxRetries = options.maxRetries || 3;
        this.retryBaseDelay = options.retryBaseDelay || 500;
        this.retryMaxDelay = options.retryMaxDelay || 15000;
        this.retryJitter = options.retryJitter || 0.2;
        this.rateLimitCooloff = options.rateLimitCooloff || 1500;

        this.requestQueue = new PQueue({ concurrency: options.maxConcurrent || 5 });

        console.log(`[RateLimitedConnection] Initialized. Primary RPC: ${this.rpcEndpoint} (WSS: ${this._wsEndpoint || 'Not set'}). Effective Pool: [${this.allEffectiveEndpoints.map(e=>e.http).join(', ')}]. Client ID: "${clientId}"`);
    }

    switchToNextEndpoint() {
        if (this.allEffectiveEndpoints.length <= 1) {
            console.warn(`[RateLimitedConnection] No alternative RPC endpoints available in the effective pool to switch from ${this.rpcEndpoint}.`);
            return false;
        }
        this.currentEndpointInternalIndex = (this.currentEndpointInternalIndex + 1) % this.allEffectiveEndpoints.length;
        const newEndpointDetail = this.allEffectiveEndpoints[this.currentEndpointInternalIndex];

        this._rpcEndpoint = newEndpointDetail.http;
        // Also update the WSS endpoint if possible and not explicitly overridden by user in initial options
        if (this.allEffectiveEndpoints[this.currentEndpointInternalIndex].ws && (!this. ursprünglichWsEndpointConfigured)) { // `ursprünglichWsEndpointConfigured` would be a flag set in constructor if options.wsEndpoint was given
             this._wsEndpoint = newEndpointDetail.ws;
        }


        console.warn(`[RateLimitedConnection] Switched RPC endpoint to: ${this.rpcEndpoint} (Provider: ${newEndpointDetail.provider}, WSS: ${this._wsEndpoint || 'N/A'})`);
        return true;
    }

    async _rpcRequest(method, args) {
        return this.requestQueue.add(async () => {
            let attempts = 0;
            let currentDelay = this.retryBaseDelay;

            while (true) {
                attempts++;
                const requestId = `req_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                const currentProvider = this.allEffectiveEndpoints[this.currentEndpointInternalIndex]?.provider || 'unknown';
                console.log(`[RateLimitedConnection] RPC Outgoing (ID: ${requestId}, Attempt: ${attempts}/${this.maxRetries + 1}) -> Method: ${method}, Endpoint: ${this.rpcEndpoint} (Provider: ${currentProvider})`);

                try {
                    const result = await super._rpcRequest(method, args);
                    console.log(`[RateLimitedConnection] RPC Incoming (ID: ${requestId}) <- Method: ${method}, Provider: ${currentProvider}, Status: Success`);
                    return result;
                } catch (error) {
                    console.warn(`[RateLimitedConnection] RPC Error (ID: ${requestId}, Attempt ${attempts}, Provider: ${currentProvider}). Method: ${method}, Endpoint: ${this.rpcEndpoint}. Message: ${error.message}`);

                    const isRetryable = isInternalRetryableSolanaError(error);
                    const status = error?.response?.status || error?.statusCode || error?.status;

                    if (attempts > this.maxRetries || !isRetryable) {
                        console.error(`[RateLimitedConnection] RPC Error (ID: ${requestId}) - Unretryable or max retries reached for ${method} on ${currentProvider}. Final Error: ${error.message}`);
                        throw error;
                    }

                    if (status === 429) {
                        console.warn(`[RateLimitedConnection] RPC Info (ID: ${requestId}) - Rate limit (429) from ${this.rpcEndpoint} (${currentProvider}). Cooling off for ${this.rateLimitCooloff}ms.`);
                        await sleep(this.rateLimitCooloff);
                        if (this.allEffectiveEndpoints.length > 1) {
                           this.switchToNextEndpoint();
                        }
                    } else if (isRetryable) {
                        const switched = (this.allEffectiveEndpoints.length > 1) ? this.switchToNextEndpoint() : false;
                        if (!switched && attempts >= this.maxRetries && this.allEffectiveEndpoints.length > 1) { // Cycled through all and still failing
                             console.error(`[RateLimitedConnection] RPC Error (ID: ${requestId}) - All endpoints in pool failed after retries for ${method}.`);
                             throw error;
                        } else if (!switched && this.allEffectiveEndpoints.length <= 1) {
                            // Single RPC, just retry with delay
                        }


                        const jitter = currentDelay * this.retryJitter * (Math.random() - 0.5);
                        const delayWithJitter = Math.min(this.retryMaxDelay, Math.round(currentDelay + jitter));
                        console.warn(`[RateLimitedConnection] RPC Info (ID: ${requestId}) - Retryable error on ${method}. Retrying in ${delayWithJitter}ms (Next RPC: ${this.rpcEndpoint} - ${this.allEffectiveEndpoints[this.currentEndpointInternalIndex]?.provider})...`);
                        await sleep(delayWithJitter);
                        currentDelay = Math.min(this.retryMaxDelay, currentDelay * 2);
                    } else {
                        throw error;
                    }
                }
            }
        });
    }

    getCurrentEndpointDetail() {
        return this.allEffectiveEndpoints[this.currentEndpointInternalIndex];
    }
}

export default RateLimitedConnection;
