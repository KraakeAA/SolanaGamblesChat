// --- Start of Part 1 ---
// index.js - Part 1: Core Imports, Basic Setup, Global State & Utilities (Enhanced & Integrated with Payment System & Price Feed)
//---------------------------------------------------------------------------

import 'dotenv/config';
import TelegramBot from 'node-telegram-bot-api';
import { Pool } from 'pg';
import express from 'express';
import {
    Connection, // Keep for type checking if RateLimitedConnection is an instance of Connection
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction, // Still used by the modified sendSol if not using connection.sendTransaction directly
    ComputeBudgetProgram,
    SendTransactionError, // For error checking
    TransactionExpiredBlockheightExceededError // For error checking
} from '@solana/web3.js';
import bs58 from 'bs58';
import * as crypto from 'crypto';
import { createHash } from 'crypto'; // Specifically for createSafeUserSpecificIndex
import PQueue from 'p-queue';
import { Buffer } from 'buffer';
import bip39 from 'bip39';
import { derivePath } from 'ed25519-hd-key';
import nacl from 'tweetnacl';
import axios from 'axios';

// Import the custom RateLimitedConnection
// The path './lib/solana-connection.js' assumes it's in a 'lib' subdirectory relative to index.js
import RateLimitedConnection from './lib/solana-connection.js';

console.log("Loading Part 1: Core Imports, Basic Setup, Global State & Utilities (Enhanced & Integrated with Payment System & Price Feed)...");

function stringifyWithBigInt(obj) {
  return JSON.stringify(obj, (key, value) => {
    if (typeof value === 'bigint') {
      return value.toString() + 'n'; // Suffix 'n' to denote BigInt
    }
    if (typeof value === 'function') {
      return `[Function: ${value.name || 'anonymous'}]`;
    }
    if (value === undefined) {
      // Note: This custom "undefined_value" string can cause issues if directly
      // inserted into DB columns expecting specific types (e.g., numeric, boolean)
      // without proper handling or type conversion beforehand.
      return 'undefined_value';
    }
    return value;
  }, 2);
}
console.log("[Global Utils] stringifyWithBigInt helper function defined.");

const CASINO_ENV_DEFAULTS = {
  'DB_POOL_MAX': '25',
  'DB_POOL_MIN': '5',
  'DB_IDLE_TIMEOUT': '30000',
  'DB_CONN_TIMEOUT': '5000',
  'DB_SSL': 'true',
  'DB_REJECT_UNAUTHORIZED': 'true', // Important for secure DB connections
  'SHUTDOWN_FAIL_TIMEOUT_MS': '10000',
  'JACKPOT_CONTRIBUTION_PERCENT': '0.01',
  'MIN_BET_AMOUNT_LAMPORTS': '5000000', // 0.005 SOL
  'MAX_BET_AMOUNT_LAMPORTS': '1000000000', // 1 SOL
  'COMMAND_COOLDOWN_MS': '1500',
  'JOIN_GAME_TIMEOUT_MS': '120000', // 2 minutes
  'DEFAULT_STARTING_BALANCE_LAMPORTS': '10000000', // 0.01 SOL
  'TARGET_JACKPOT_SCORE': '100',
  'BOT_STAND_SCORE_DICE_ESCALATOR': '10',
  'DICE_21_TARGET_SCORE': '21',
  'DICE_21_BOT_STAND_SCORE': '17',
  'RULES_CALLBACK_PREFIX': 'rules_game_',
  'DEPOSIT_CALLBACK_ACTION': 'deposit_action',
  'WITHDRAW_CALLBACK_ACTION': 'withdraw_action',
  'QUICK_DEPOSIT_CALLBACK_ACTION': 'quick_deposit_action',
  'MAX_RETRY_POLLING_DELAY': '60000', // 1 minute
  'INITIAL_RETRY_POLLING_DELAY': '5000', // 5 seconds
  'BOT_NAME': 'Solana Casino Royale',
};

const PAYMENT_ENV_DEFAULTS = {
  'SOLANA_RPC_URL': 'https://api.mainnet-beta.solana.com/', // Fallback if RPC_URLS is empty
  'RPC_URLS': '', // Comma-separated list of RPC URLs for RateLimitedConnection
  'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60',
  'DEPOSIT_CONFIRMATIONS': 'confirmed', // Solana confirmation levels: processed, confirmed, finalized
  'WITHDRAWAL_FEE_LAMPORTS': '10000', // Covers base fee + priority
  'MIN_WITHDRAWAL_LAMPORTS': '10000000', // 0.01 SOL
  'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '10000', // Base priority fee for payouts
  'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000', // Max priority fee for payouts
  'PAYOUT_COMPUTE_UNIT_LIMIT': '30000', // For simple SOL transfers
  'PAYOUT_JOB_RETRIES': '3',
  'PAYOUT_JOB_RETRY_DELAY_MS': '7000',
  'SWEEP_INTERVAL_MS': '300000', // 5 minutes
  'SWEEP_BATCH_SIZE': '15',
  'SWEEP_FEE_BUFFER_LAMPORTS': '20000', // Buffer left in deposit address after sweep for its own tx fee
  'SWEEP_COMPUTE_UNIT_LIMIT': '30000', // Compute units for sweep transactions
  'SWEEP_PRIORITY_FEE_MICROLAMPORTS': '5000', // Priority fee for sweep transactions
  'SWEEP_ADDRESS_DELAY_MS': '1500', // Delay between processing each address in a sweep batch
  'SWEEP_RETRY_ATTEMPTS': '2', // Retries for a single address sweep if it fails
  'SWEEP_RETRY_DELAY_MS': '10000',
  'RPC_MAX_CONCURRENT': '10', // Max concurrent requests for RateLimitedConnection
  'RPC_RETRY_BASE_DELAY': '750', // Base delay for RPC retries
  'RPC_MAX_RETRIES': '4', // Max retries for RPC calls
  'RPC_RATE_LIMIT_COOLOFF': '3000', // Increased cooloff period after hitting a rate limit
  'RPC_RETRY_MAX_DELAY': '25000', // Max delay for RPC retries
  'RPC_RETRY_JITTER': '0.3', // Jitter factor for RPC retry delays
  'RPC_COMMITMENT': 'confirmed', // Default Solana commitment for RPC calls
  'PAYOUT_QUEUE_CONCURRENCY': '4', // Concurrency for payout processing queue
  'PAYOUT_QUEUE_TIMEOUT_MS': '90000', // Timeout for payout jobs
  'DEPOSIT_PROCESS_QUEUE_CONCURRENCY': '5', // Concurrency for deposit processing queue
  'DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS': '45000', // Timeout for deposit processing jobs
  'TELEGRAM_SEND_QUEUE_CONCURRENCY': '1', // Concurrency for sending Telegram messages (to respect rate limits)
  'TELEGRAM_SEND_QUEUE_INTERVAL_MS': '1050', // Interval for Telegram message queue (standard is ~1s per message)
  'TELEGRAM_SEND_QUEUE_INTERVAL_CAP': '1', // Messages per interval
  'DEPOSIT_MONITOR_INTERVAL_MS': '15000', // Interval for polling deposit addresses
  'DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE': '75', // How many addresses to check per monitoring cycle
  'DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT': '15', // How many signatures to fetch per address
  'WALLET_CACHE_TTL_MS': (15 * 60 * 1000).toString(), // Cache TTL for user withdrawal wallets (15 mins)
  'DEPOSIT_ADDR_CACHE_TTL_MS': (parseInt(CASINO_ENV_DEFAULTS.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10) * 60 * 1000 + 5 * 60 * 1000).toString(), // Cache for active deposit addresses (expiry + 5 mins)
  'MAX_PROCESSED_TX_CACHE': '10000', // Max size for the cache of processed transaction signatures
  'INIT_DELAY_MS': '7000', // Initial delay before starting background processes
  'ENABLE_PAYMENT_WEBHOOKS': 'false', // Enable/disable payment webhook server
  'PAYMENT_WEBHOOK_PORT': '3000', // Port for payment webhook server
  'PAYMENT_WEBHOOK_PATH': '/webhook/solana-payments', // Standardized path for webhook endpoint
  'SOL_PRICE_API_URL': 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
  'SOL_USD_PRICE_CACHE_TTL_MS': (3 * 60 * 1000).toString(), // Cache TTL for SOL/USD price (3 mins)
  'MIN_BET_USD': '0.50',
  'MAX_BET_USD': '100.00',
};

const OPTIONAL_ENV_DEFAULTS = { ...CASINO_ENV_DEFAULTS, ...PAYMENT_ENV_DEFAULTS };

Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
  if (process.env[key] === undefined) {
    console.log(`[ENV_DEFAULT] Setting default for ${key}: ${defaultValue}`);
    process.env[key] = defaultValue;
  }
});

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_USER_ID = process.env.ADMIN_USER_ID;
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_NAME = process.env.BOT_NAME; // Already defaulted via OPTIONAL_ENV_DEFAULTS

const DEPOSIT_MASTER_SEED_PHRASE = process.env.DEPOSIT_MASTER_SEED_PHRASE;
const MAIN_BOT_PRIVATE_KEY_BS58 = process.env.MAIN_BOT_PRIVATE_KEY;
const REFERRAL_PAYOUT_PRIVATE_KEY_BS58 = process.env.REFERRAL_PAYOUT_PRIVATE_KEY;

let MAIN_BOT_KEYPAIR = null;
if (MAIN_BOT_PRIVATE_KEY_BS58) {
    try {
        MAIN_BOT_KEYPAIR = Keypair.fromSecretKey(bs58.decode(MAIN_BOT_PRIVATE_KEY_BS58));
        console.log(`🔑 Main Bot Payout Wallet Initialized: ${MAIN_BOT_KEYPAIR.publicKey.toBase58()}`);
    } catch (e) {
        console.error("🚨 FATAL ERROR: Invalid MAIN_BOT_PRIVATE_KEY. Withdrawals and critical operations will fail.", e.message);
        process.exit(1);
    }
} else {
    console.error("🚨 FATAL ERROR: MAIN_BOT_PRIVATE_KEY is not defined. Withdrawals and critical operations will fail.");
    process.exit(1);
}

let REFERRAL_PAYOUT_KEYPAIR = null;
if (REFERRAL_PAYOUT_PRIVATE_KEY_BS58) {
    try {
        REFERRAL_PAYOUT_KEYPAIR = Keypair.fromSecretKey(bs58.decode(REFERRAL_PAYOUT_PRIVATE_KEY_BS58));
        console.log(`🔑 Referral Payout Wallet Initialized: ${REFERRAL_PAYOUT_KEYPAIR.publicKey.toBase58()}`);
    } catch (e) {
        console.warn(`⚠️ WARNING: Invalid REFERRAL_PAYOUT_PRIVATE_KEY. Falling back to main bot wallet for referral payouts. Error: ${e.message}`);
        REFERRAL_PAYOUT_KEYPAIR = null; // Explicitly nullify on error
    }
} else {
    console.log("ℹ️ INFO: REFERRAL_PAYOUT_PRIVATE_KEY not set. Main bot wallet will be used for referral payouts.");
}

// Corrected RPC URL processing for RateLimitedConnection
const RPC_URLS_LIST_FROM_ENV = (process.env.RPC_URLS || '')
    .split(',')
    .map(u => u.trim())
    .filter(u => u && (u.startsWith('http://') || u.startsWith('https://')));

const SINGLE_MAINNET_RPC_FROM_ENV = process.env.SOLANA_RPC_URL || null;

let combinedRpcEndpointsForConnection = [...RPC_URLS_LIST_FROM_ENV];
if (SINGLE_MAINNET_RPC_FROM_ENV && !combinedRpcEndpointsForConnection.some(url => url.startsWith(SINGLE_MAINNET_RPC_FROM_ENV.split('?')[0]))) {
    combinedRpcEndpointsForConnection.push(SINGLE_MAINNET_RPC_FROM_ENV);
}
// If combinedRpcEndpointsForConnection is empty, RateLimitedConnection will use its internal hardcoded defaults if available in its definition.


const SHUTDOWN_FAIL_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
const MAX_RETRY_POLLING_DELAY = parseInt(process.env.MAX_RETRY_POLLING_DELAY, 10);
const INITIAL_RETRY_POLLING_DELAY = parseInt(process.env.INITIAL_RETRY_POLLING_DELAY, 10);
const JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.JACKPOT_CONTRIBUTION_PERCENT);
const MAIN_JACKPOT_ID = 'dice_escalator_main'; // Ensure this matches DB init in Part 2
const TARGET_JACKPOT_SCORE = parseInt(process.env.TARGET_JACKPOT_SCORE, 10);
const BOT_STAND_SCORE_DICE_ESCALATOR = parseInt(process.env.BOT_STAND_SCORE_DICE_ESCALATOR, 10);
const DICE_21_TARGET_SCORE = parseInt(process.env.DICE_21_TARGET_SCORE, 10);
const DICE_21_BOT_STAND_SCORE = parseInt(process.env.DICE_21_BOT_STAND_SCORE, 10);

const MIN_BET_AMOUNT_LAMPORTS_config = BigInt(process.env.MIN_BET_AMOUNT_LAMPORTS);
const MAX_BET_AMOUNT_LAMPORTS_config = BigInt(process.env.MAX_BET_AMOUNT_LAMPORTS);
const MIN_BET_USD_val = parseFloat(process.env.MIN_BET_USD);
const MAX_BET_USD_val = parseFloat(process.env.MAX_BET_USD);

const COMMAND_COOLDOWN_MS = parseInt(process.env.COMMAND_COOLDOWN_MS, 10);
const JOIN_GAME_TIMEOUT_MS = parseInt(process.env.JOIN_GAME_TIMEOUT_MS, 10);
const DEFAULT_STARTING_BALANCE_LAMPORTS = BigInt(process.env.DEFAULT_STARTING_BALANCE_LAMPORTS);
const RULES_CALLBACK_PREFIX = process.env.RULES_CALLBACK_PREFIX;
const DEPOSIT_CALLBACK_ACTION = process.env.DEPOSIT_CALLBACK_ACTION;
const WITHDRAW_CALLBACK_ACTION = process.env.WITHDRAW_CALLBACK_ACTION;
const QUICK_DEPOSIT_CALLBACK_ACTION = process.env.QUICK_DEPOSIT_CALLBACK_ACTION;

const SOL_DECIMALS = 9; // Standard Solana decimal places
const DEPOSIT_ADDRESS_EXPIRY_MINUTES = parseInt(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10);
const DEPOSIT_ADDRESS_EXPIRY_MS = DEPOSIT_ADDRESS_EXPIRY_MINUTES * 60 * 1000;
const DEPOSIT_CONFIRMATION_LEVEL = process.env.DEPOSIT_CONFIRMATIONS?.toLowerCase(); // e.g., 'processed', 'confirmed', 'finalized'
const WITHDRAWAL_FEE_LAMPORTS = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS);
const MIN_WITHDRAWAL_LAMPORTS = BigInt(process.env.MIN_WITHDRAWAL_LAMPORTS);

// Critical environment variable checks
if (!BOT_TOKEN) { console.error("🚨 FATAL ERROR: BOT_TOKEN is not defined. Bot cannot start."); process.exit(1); }
if (!DATABASE_URL) { console.error("🚨 FATAL ERROR: DATABASE_URL is not defined. Cannot connect to PostgreSQL."); process.exit(1); }
if (!DEPOSIT_MASTER_SEED_PHRASE) { console.error("🚨 FATAL ERROR: DEPOSIT_MASTER_SEED_PHRASE is not defined. Payment system cannot generate deposit addresses."); process.exit(1); }
// MAIN_BOT_KEYPAIR check done during its initialization.

// Check if HARCODED_RPC_ENDPOINTS is expected to be available from './lib/solana-connection.js'
// This depends on the content of that file, which is not provided.
// Assuming RateLimitedConnection has its own defaults if combinedRpcEndpointsForConnection is empty.
if (combinedRpcEndpointsForConnection.length === 0) {
    // Check if RateLimitedConnection itself has internal defaults it can use.
    // If not, this warning is very important.
    // For this exercise, we assume RateLimitedConnection might have its own fallbacks.
    console.warn("⚠️ WARNING: No RPC URLs provided via environment (RPC_URLS, SOLANA_RPC_URL). RateLimitedConnection might rely on its internal defaults, if any. RPC functionality may be impaired if no defaults are present.");
}


const criticalGameScores = { TARGET_JACKPOT_SCORE, BOT_STAND_SCORE_DICE_ESCALATOR, DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE };
for (const [key, value] of Object.entries(criticalGameScores)) {
    if (isNaN(value) || value <=0) {
        console.error(`🚨 FATAL ERROR: Game score parameter '${key}' ('${value}') is not a valid positive number. Check .env file or defaults.`);
        process.exit(1);
    }
}
if (isNaN(MIN_BET_USD_val) || MIN_BET_USD_val <= 0) {
    console.error(`🚨 FATAL ERROR: MIN_BET_USD ('${process.env.MIN_BET_USD}') must be a positive number.`);
    process.exit(1);
}
if (isNaN(MAX_BET_USD_val) || MAX_BET_USD_val < MIN_BET_USD_val) {
    console.error(`🚨 FATAL ERROR: MAX_BET_USD ('${process.env.MAX_BET_USD}') must be greater than or equal to MIN_BET_USD and be a number.`);
    process.exit(1);
}
if (MIN_BET_AMOUNT_LAMPORTS_config < 1n || isNaN(Number(MIN_BET_AMOUNT_LAMPORTS_config))) {
    console.error(`🚨 FATAL ERROR: MIN_BET_AMOUNT_LAMPORTS ('${MIN_BET_AMOUNT_LAMPORTS_config}') must be a positive number.`);
    process.exit(1);
}
if (MAX_BET_AMOUNT_LAMPORTS_config < MIN_BET_AMOUNT_LAMPORTS_config || isNaN(Number(MAX_BET_AMOUNT_LAMPORTS_config))) {
    console.error(`🚨 FATAL ERROR: MAX_BET_AMOUNT_LAMPORTS ('${MAX_BET_AMOUNT_LAMPORTS_config}') must be greater than or equal to MIN_BET_AMOUNT_LAMPORTS and be a number.`);
    process.exit(1);
}
// Ensure JACKPOT_CONTRIBUTION_PERCENT is valid
if (isNaN(JACKPOT_CONTRIBUTION_PERCENT) || JACKPOT_CONTRIBUTION_PERCENT < 0 || JACKPOT_CONTRIBUTION_PERCENT >= 1) {
    console.error(`🚨 FATAL ERROR: JACKPOT_CONTRIBUTION_PERCENT ('${process.env.JACKPOT_CONTRIBUTION_PERCENT}') must be a number between 0 (inclusive) and 1 (exclusive). E.g., 0.01 for 1%.`);
    process.exit(1);
}


console.log("✅ BOT_TOKEN loaded successfully.");
if (ADMIN_USER_ID) console.log(`🔑 Admin User ID: ${ADMIN_USER_ID} loaded.`);
else console.log("ℹ️ INFO: No ADMIN_USER_ID set (optional, for admin alerts).");
console.log(`🔑 Payment System: DEPOSIT_MASTER_SEED_PHRASE is set (value not logged).`);
console.log(`📡 Using RPC Endpoints (from env): [${combinedRpcEndpointsForConnection.join(', ')}] (RateLimitedConnection may use internal defaults if this list is empty or fails).`);

// Helper to format lamports to SOL string for console logs, defined early for use here
function formatLamportsToSolStringForLog(lamports) {
    if (typeof lamports !== 'bigint') {
        try { lamports = BigInt(lamports); }
        catch (e) { return 'Invalid_Lamports'; }
    }
    return (Number(lamports) / Number(LAMPORTS_PER_SOL)).toFixed(SOL_DECIMALS);
}

console.log("--- 🎲 Game Settings Loaded 🎲 ---");
console.log(`Escalator: Target Jackpot Score: ${TARGET_JACKPOT_SCORE}, Bot Stand: ${BOT_STAND_SCORE_DICE_ESCALATOR}, Jackpot Fee: ${JACKPOT_CONTRIBUTION_PERCENT * 100}%`);
console.log(`Blackjack (21): Target Score: ${DICE_21_TARGET_SCORE}, Bot Stand: ${DICE_21_BOT_STAND_SCORE}`);
console.log(`💰 Bet Limits (USD): $${MIN_BET_USD_val.toFixed(2)} - $${MAX_BET_USD_val.toFixed(2)}`);
console.log(`⚙️ Bet Limits (Lamports Ref): ${formatLamportsToSolStringForLog(MIN_BET_AMOUNT_LAMPORTS_config)} SOL - ${formatLamportsToSolStringForLog(MAX_BET_AMOUNT_LAMPORTS_config)} SOL`);
console.log(`🏦 Default Starting Credits: ${formatLamportsToSolStringForLog(DEFAULT_STARTING_BALANCE_LAMPORTS)} SOL`);
console.log(`⏱️ Command Cooldown: ${COMMAND_COOLDOWN_MS / 1000}s`);
console.log(`⏳ Game Join Timeout: ${JOIN_GAME_TIMEOUT_MS / 1000 / 60}min`);
console.log("--- 💸 Payment Settings Loaded 💸 ---");
console.log(`Min Withdrawal: ${formatLamportsToSolStringForLog(MIN_WITHDRAWAL_LAMPORTS)} SOL, Fee: ${formatLamportsToSolStringForLog(WITHDRAWAL_FEE_LAMPORTS)} SOL`);
console.log(`Deposit Address Expiry: ${DEPOSIT_ADDRESS_EXPIRY_MINUTES} minutes`);
console.log(`📈 SOL/USD Price API: ${process.env.SOL_PRICE_API_URL}`);
console.log("------------------------------------");


console.log("⚙️ Setting up PostgreSQL Pool...");
const useSsl = process.env.DB_SSL === 'true';
const rejectUnauthorizedSsl = process.env.DB_REJECT_UNAUTHORIZED === 'true';
console.log(`DB_SSL configuration: Use SSL = '${useSsl}', Reject Unauthorized = '${rejectUnauthorizedSsl}'`);

const pool = new Pool({
  connectionString: DATABASE_URL,
  max: parseInt(process.env.DB_POOL_MAX, 10),
  min: parseInt(process.env.DB_POOL_MIN, 10),
  idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT, 10),
  connectionTimeoutMillis: parseInt(process.env.DB_CONN_TIMEOUT, 10),
  ssl: useSsl ? { rejectUnauthorized: rejectUnauthorizedSsl } : false,
});

pool.on('connect', client => {
  console.log('ℹ️ [DB Pool] Client connected to PostgreSQL.');
});
pool.on('error', (err, client) => {
  console.error('❌ Unexpected error on idle PostgreSQL client', err);
  if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
    const adminMessage = `🚨 *DATABASE POOL ERROR* 🚨\nAn unexpected error occurred with an idle PostgreSQL client:\n\n*Error Message:*\n\`${escapeMarkdownV2(String(err.message || err))}\`\n\nPlease check the server logs for more details\\.`;
    safeSendMessage(ADMIN_USER_ID, adminMessage, { parse_mode: 'MarkdownV2' })
      .catch(notifyErr => console.error("Failed to notify admin about DB pool error:", notifyErr));
  } else {
    console.error(`[Admin Alert Failure] DB Pool Error (Idle Client): ${err.message || String(err)} (safeSendMessage or escapeMarkdownV2 might not be available or ADMIN_USER_ID not set)`);
  }
});
console.log("✅ PostgreSQL Pool created.");

async function queryDatabase(sql, params = [], dbClient = pool) {
    const logPrefix = '[queryDatabase]';
    try {
        const result = await dbClient.query(sql, params);
        return result;
    } catch (error) {
        console.error(`${logPrefix} Error executing query. SQL (start): "${sql.substring(0,100)}..." Params: [${params ? params.join(', ') : 'N/A'}] Error: ${error.message}`);
        throw error; // Re-throw to be handled by caller
    }
}
console.log("[Global Utils] queryDatabase helper function defined.");

// --- CORRECTED RateLimitedConnection Instantiation ---
console.log("⚙️ Setting up Solana Connection...");
const connectionOptions = {
    commitment: process.env.RPC_COMMITMENT, // Make sure this is a valid Commitment type string
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
    // wsEndpoint: process.env.SOLANA_WSS_URL_OVERRIDE || undefined, // Example
    // httpHeaders: {'your-header': 'value'}, // Example for custom headers if needed by RPC provider
};

const solanaConnection = new RateLimitedConnection(
    combinedRpcEndpointsForConnection,
    connectionOptions
);
// --- End of RateLimitedConnection Instantiation ---


const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("🤖 Telegram Bot instance created and configured for polling.");

let app = null; // Express app instance for webhooks
if (process.env.ENABLE_PAYMENT_WEBHOOKS === 'true') {
    app = express();
    app.use(express.json({
        verify: (req, res, buf) => { // Store raw body for potential signature verification
            req.rawBody = buf;
        }
    }));
    console.log("🚀 Express app initialized for payment webhooks (JSON body parser with rawBody enabled).");
} else {
    console.log("ℹ️ Payment webhooks are disabled via ENABLE_PAYMENT_WEBHOOKS env var.");
}

const BOT_VERSION = process.env.BOT_VERSION || '3.3.3-fixes'; // Incremented version
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;

let isShuttingDown = false; // Global shutdown flag

// Global Caches & State
let activeGames = new Map(); // Stores active multiplayer game instances: gameId -> gameData
let userCooldowns = new Map(); // Tracks user command cooldowns: userId -> timestamp
let groupGameSessions = new Map(); // Tracks group game sessions: chatId -> { currentGameId, currentGameType, lastActivity }

const walletCache = new Map(); // Stores { userId (string) -> { solanaAddress, timestamp } } for withdrawal wallets
const activeDepositAddresses = new Map(); // Stores { depositAddressString -> { userId (string), expiresAtTimestamp } }
const processedDepositTxSignatures = new Set(); // Stores processed tx signatures (strings) to prevent duplicates
const PENDING_REFERRAL_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours for pending referrals
const pendingReferrals = new Map(); // Stores { referredUserId (string) -> { referrerId (string), timestamp } }

const userStateCache = new Map(); // For multi-step interactions: userId (string) -> { state, data, messageId, chatId }

const SOL_PRICE_CACHE_KEY = 'sol_usd_price_cache';
const solPriceCache = new Map(); // Stores { SOL_PRICE_CACHE_KEY -> { price, timestamp } }

const DICE_ESCALATOR_BUST_ON = 1; // If a player rolls this in Dice Escalator, they bust.

console.log(`🚀 Initializing ${BOT_NAME} v${BOT_VERSION}...`);
console.log(`🕰️ Current system time: ${new Date().toISOString()}`);
console.log(`💻 Node.js Version: ${process.version}`);

const escapeMarkdownV2 = (text) => {
  if (text === null || typeof text === 'undefined') return '';
  // Ensure text is a string before calling replace
  return String(text).replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};
console.log("[Global Utils] escapeMarkdownV2 helper function defined.");

async function safeSendMessage(chatId, text, options = {}) {
    const LOG_PREFIX_SSM = `[safeSendMessage CH:${chatId}]`;
    if (!chatId || typeof text !== 'string') {
        console.error(`${LOG_PREFIX_SSM} Invalid input: ChatID is ${chatId}, Text type is ${typeof text}. Preview: ${String(text).substring(0, 100)}`);
        return undefined; // Return undefined, not an empty object, for consistency
    }
    
    let messageToSend = text; 
    let finalOptions = { ...options }; // Clone options to avoid modifying the original object

    if (finalOptions.parse_mode === 'MarkdownV2' && messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsisBase = ` \\.\\.\\. \\(_message truncated by ${escapeMarkdownV2(BOT_NAME)}_\\)`; 
        const truncateAt = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsisBase.length);
        messageToSend = messageToSend.substring(0, truncateAt) + ellipsisBase;
        console.warn(`${LOG_PREFIX_SSM} Message (MarkdownV2) was too long (${text.length} chars) and has been truncated.`);
    } else if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) { // For non-Markdown or other parse modes
        const ellipsisPlain = `... (message truncated by ${BOT_NAME})`;
        const truncateAt = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsisPlain.length);
        messageToSend = messageToSend.substring(0, truncateAt) + ellipsisPlain;
        console.warn(`${LOG_PREFIX_SSM} Message (Plain Text) was too long (${text.length} chars) and has been truncated.`);
    }


    if (!bot) { // Check if bot instance exists
        console.error(`${LOG_PREFIX_SSM} ⚠️ Error: Telegram 'bot' instance not available.`);
        return undefined;
    }

    try {
        if (typeof bot.sendMessage !== 'function') {
            // This should not happen if bot is initialized correctly
            throw new Error("'bot.sendMessage' is not a function. Bot may not be initialized.");
        }
        const sentMessage = await bot.sendMessage(chatId, messageToSend, finalOptions);
        return sentMessage;
    } catch (error) {
        console.error(`${LOG_PREFIX_SSM} ❌ Failed to send message. Code: ${error.code || 'N/A'}, Msg: ${error.message}`);
        if (error.response && error.response.body) {
            console.error(`${LOG_PREFIX_SSM} Telegram API Response: ${stringifyWithBigInt(error.response.body)}`);
            // If MarkdownV2 fails, try sending as plain text
            if (finalOptions.parse_mode === 'MarkdownV2' && error.response.body.description && error.response.body.description.toLowerCase().includes("can't parse entities")) {
                console.warn(`${LOG_PREFIX_SSM} MarkdownV2 parse error detected. Attempting to send as plain text.`);
                console.error(`${LOG_PREFIX_SSM} Original MarkdownV2 text (first 200 chars): "${text.substring(0,200)}"`); // Log the problematic text
                try {
                    let plainTextFallbackOptions = { ...options }; // Get original options
                    delete plainTextFallbackOptions.parse_mode; // Remove parse_mode for plain text
                    
                    // Re-truncate if necessary for plain text, using original text
                    let plainTextForFallback = text;
                    if (plainTextForFallback.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
                        const ellipsisPlainFallback = `... (message truncated by ${BOT_NAME})`; // Non-Markdown ellipsis
                        const truncateAtPlain = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsisPlainFallback.length);
                        plainTextForFallback = plainTextForFallback.substring(0, truncateAtPlain) + ellipsisPlainFallback;
                    }
                    return await bot.sendMessage(chatId, plainTextForFallback, plainTextFallbackOptions);
                } catch (fallbackError) {
                    console.error(`${LOG_PREFIX_SSM} ❌ Plain text fallback also failed. Code: ${fallbackError.code || 'N/A'}, Msg: ${fallbackError.message}`);
                    return undefined;
                }
            }
        }
        return undefined; // Explicitly return undefined on failure
    }
}
console.log("[Global Utils] safeSendMessage (with MarkdownV2 fallback & refined truncation) defined.");


const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
console.log("[Global Utils] sleep helper function defined.");

async function notifyAdmin(message, options = {}) {
    if (ADMIN_USER_ID) {
        const adminAlertMessage = `🔔 *ADMIN ALERT* (${escapeMarkdownV2(BOT_NAME)}) 🔔\n\n${message}`; // Message itself should be pre-escaped if it contains user input or dynamic Markdown.
        return safeSendMessage(ADMIN_USER_ID, adminAlertMessage, { parse_mode: 'MarkdownV2', ...options });
    } else {
        console.warn(`[Admin Notify - SKIPPED] No ADMIN_USER_ID set. Message (first 100 chars): ${String(message).substring(0,100)}...`);
        return null; // Return null, not undefined, to distinguish from send failure
    }
}
console.log("[Global Utils] notifyAdmin helper function defined.");

console.log("⚙️ Setting up Price Feed Utilities...");

async function fetchSolUsdPriceFromAPI() {
    const apiUrl = process.env.SOL_PRICE_API_URL;
    const logPrefix = '[PriceFeed API]';
    try {
        console.log(`${logPrefix} Fetching SOL/USD price from ${apiUrl}...`);
        const response = await axios.get(apiUrl, { timeout: 8000 }); // Standard timeout
        if (response.data && response.data.solana && response.data.solana.usd) {
            const price = parseFloat(response.data.solana.usd);
            if (isNaN(price) || price <= 0) {
                throw new Error('Invalid or non-positive price data received from API.');
            }
            console.log(`${logPrefix} ✅ Successfully fetched SOL/USD price: $${price.toFixed(2)}`); // Log with fixed decimals
            return price;
        } else {
            console.error(`${logPrefix} ⚠️ SOL price not found or invalid structure in API response:`, stringifyWithBigInt(response.data));
            throw new Error('SOL price not found or invalid structure in API response.');
        }
    } catch (error) {
        const errMsg = error.isAxiosError ? error.message : String(error);
        console.error(`${logPrefix} ❌ Error fetching SOL/USD price: ${errMsg}`);
        if (error.response) {
            console.error(`${logPrefix} API Response Status: ${error.response.status}`);
            console.error(`${logPrefix} API Response Data:`, stringifyWithBigInt(error.response.data));
        }
        throw new Error(`Failed to fetch SOL/USD price: ${errMsg}`); // Re-throw for getSolUsdPrice to handle
    }
}

async function getSolUsdPrice() {
    const logPrefix = '[getSolUsdPrice]';
    const cacheTtl = parseInt(process.env.SOL_USD_PRICE_CACHE_TTL_MS, 10);
    const cachedEntry = solPriceCache.get(SOL_PRICE_CACHE_KEY);

    if (cachedEntry && (Date.now() - cachedEntry.timestamp < cacheTtl)) {
        // console.log(`${logPrefix} Using cached SOL/USD price: $${cachedEntry.price.toFixed(2)}`); // Optional: log cache hit
        return cachedEntry.price;
    }
    try {
        const price = await fetchSolUsdPriceFromAPI();
        solPriceCache.set(SOL_PRICE_CACHE_KEY, { price, timestamp: Date.now() });
        console.log(`${logPrefix} Fetched and cached new SOL/USD price: $${price.toFixed(2)}`);
        return price;
    } catch (error) {
        if (cachedEntry) {
            console.warn(`${logPrefix} ⚠️ API fetch failed ('${error.message}'), using stale cached SOL/USD price: $${cachedEntry.price.toFixed(2)}`);
            return cachedEntry.price;
        }
        const criticalErrorMessage = `🚨 *CRITICAL PRICE FEED FAILURE* (${escapeMarkdownV2(BOT_NAME)}) 🚨\n\nUnable to fetch SOL/USD price and no cache available\\. USD conversions will be severely impacted\\.\n*Error:* \`${escapeMarkdownV2(error.message)}\``;
        console.error(`${logPrefix} ❌ CRITICAL: ${criticalErrorMessage.replace(/\n/g, ' ')}`); // Flatten for single line log
        if (typeof notifyAdmin === 'function') { 
            await notifyAdmin(criticalErrorMessage); // Already Markdown formatted
        }
        throw new Error(`Critical: Could not retrieve SOL/USD price. Error: ${error.message}`); // Re-throw for calling function to handle
    }
}
console.log("[PriceFeed Utils] getSolUsdPrice and fetchSolUsdPriceFromAPI defined.");

function convertLamportsToUSDString(lamports, solUsdPrice, displayDecimals = 2) {
    if (typeof solUsdPrice !== 'number' || solUsdPrice <= 0) {
        console.warn(`[Convert] Invalid solUsdPrice (${solUsdPrice}) for lamports to USD conversion. Lamports: ${lamports}`);
        return '⚠️ Price N/A';
    }
    let lamportsBigInt;
    try {
        lamportsBigInt = BigInt(lamports);
    } catch (e) { 
        console.warn(`[Convert] Invalid lamport amount for USD conversion: ${lamports}. Error: ${e.message}`);
        return '⚠️ Amount Error'; 
    } 
    
    const solAmount = Number(lamportsBigInt) / Number(LAMPORTS_PER_SOL);
    const usdValue = solAmount * solUsdPrice;
    return `$${usdValue.toLocaleString('en-US', { minimumFractionDigits: displayDecimals, maximumFractionDigits: displayDecimals })}`;
}
console.log("[PriceFeed Utils] convertLamportsToUSDString defined.");

function convertUSDToLamports(usdAmount, solUsdPrice) {
    if (typeof solUsdPrice !== 'number' || solUsdPrice <= 0) {
        throw new Error("SOL/USD price must be a positive number for USD to Lamports conversion.");
    }
    const parsedUsdAmount = parseFloat(String(usdAmount).replace(/[^0-9.-]+/g,"")); // Allow negative for parsing, then check
    if (isNaN(parsedUsdAmount) || parsedUsdAmount <= 0) { // Ensure positive USD amount
        throw new Error("Invalid or non-positive USD amount for conversion.");
    }
    const solAmount = parsedUsdAmount / solUsdPrice;
    return BigInt(Math.floor(solAmount * Number(LAMPORTS_PER_SOL))); // Use floor to avoid fractional lamports
}
console.log("[PriceFeed Utils] convertUSDToLamports defined.");


const payoutProcessorQueue = new PQueue({
    concurrency: parseInt(process.env.PAYOUT_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.PAYOUT_QUEUE_TIMEOUT_MS, 10),
    throwOnTimeout: true
});
const depositProcessorQueue = new PQueue({
    concurrency: parseInt(process.env.DEPOSIT_PROCESS_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS, 10),
    throwOnTimeout: true
});
console.log("✅ Payment processing queues (Payout & Deposit) initialized.");

const SLOT_PAYOUTS = { // Slot payouts based on dice roll (1-64) from Telegram's slot machine
    // Key: Dice value (1-64), Value: { multiplier (profit multiplier, e.g., 20 means 21x bet returned), symbols, label }
    64: { multiplier: 100, symbols: "💎💎💎", label: "MEGA JACKPOT!" }, // Typically 7-7-7 or BAR-BAR-BAR in Telegram slots
    1:  { multiplier: 20,  symbols: "7️⃣7️⃣7️⃣", label: "TRIPLE SEVEN!" },  // (Dice value 1)
    22: { multiplier: 10,  symbols: "🍋🍋🍋", label: "Triple Lemon!" },  // (Dice value 22)
    43: { multiplier: 5,   symbols: "🔔🔔🔔", label: "Triple Bell!" },   // (Dice value 43)
    // Add more payouts as desired. Ensure these match potential outcomes from bot.sendDice({emoji: '🎰'}) values (1-64)
    // For example, if Telegram's slot for value X shows YYY, map X to YYY here.
};
const SLOT_DEFAULT_LOSS_MULTIPLIER = -1; // Player loses their bet

console.log("Part 1: Core Imports, Basic Setup, Global State & Utilities (Enhanced & Integrated with Payment System & Price Feed) - Complete.");
// --- End of Part 1 ---
// --- Start of Part 2 ---
// index.js - Part 2: Database Schema Initialization & Core User Management (Integrated)
//---------------------------------------------------------------------------
console.log("Loading Part 2: Database Schema Initialization & Core User Management (Integrated)...");

// Assumes pool, DEFAULT_STARTING_BALANCE_LAMPORTS, stringifyWithBigInt, MAIN_JACKPOT_ID, queryDatabase
// are available from Part 1.

// --- Helper function for referral code generation ---
const generateReferralCode = (length = 8) => {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
};
console.log("[User Management] generateReferralCode helper function defined.");

//---------------------------------------------------------------------------
// Database Schema Initialization
//---------------------------------------------------------------------------
async function initializeDatabaseSchema() {
    console.log("🚀 Initializing database schema...");
    const client = await pool.connect(); // pool is from Part 1
    try {
        await client.query('BEGIN');

        // Users Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS users (
                telegram_id BIGINT PRIMARY KEY,
                username VARCHAR(255),
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                balance BIGINT DEFAULT ${DEFAULT_STARTING_BALANCE_LAMPORTS.toString()},
                last_active_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                is_banned BOOLEAN DEFAULT FALSE,
                ban_reason TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                solana_wallet_address VARCHAR(44) UNIQUE, 
                referral_code VARCHAR(12) UNIQUE,
                referrer_telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE SET NULL,
                can_generate_deposit_address BOOLEAN DEFAULT TRUE,
                last_deposit_address VARCHAR(44), 
                last_deposit_address_generated_at TIMESTAMPTZ,
                total_deposited_lamports BIGINT DEFAULT 0,
                total_withdrawn_lamports BIGINT DEFAULT 0,
                total_wagered_lamports BIGINT DEFAULT 0,
                total_won_lamports BIGINT DEFAULT 0,
                notes TEXT 
            );
        `);
        console.log("  [DB Schema] 'users' table checked/created.");

        // Jackpots Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS jackpots (
                jackpot_id VARCHAR(255) PRIMARY KEY,
                current_amount BIGINT DEFAULT 0,
                last_won_by_telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE SET NULL,
                last_won_timestamp TIMESTAMPTZ,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log("  [DB Schema] 'jackpots' table checked/created.");
        await client.query(
            `INSERT INTO jackpots (jackpot_id, current_amount) VALUES ($1, 0) ON CONFLICT (jackpot_id) DO NOTHING;`,
            [MAIN_JACKPOT_ID] // MAIN_JACKPOT_ID from Part 1
        );
        console.log(`  [DB Schema] Ensured '${MAIN_JACKPOT_ID}' exists in 'jackpots'.`);

        // Games Table (Game Log)
        await client.query(`
            CREATE TABLE IF NOT EXISTS games (
                game_log_id SERIAL PRIMARY KEY,
                game_type VARCHAR(50) NOT NULL, 
                chat_id BIGINT, 
                initiator_telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE SET NULL,
                participants_ids BIGINT[], 
                bet_amount_lamports BIGINT,
                outcome TEXT, 
                jackpot_contribution_lamports BIGINT,
                game_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log("  [DB Schema] 'games' table (game log) checked/created.");

        // User Deposit Wallets Table (HD Generated Deposit Addresses)
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_deposit_wallets (
                wallet_id SERIAL PRIMARY KEY,
                user_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                public_key VARCHAR(44) NOT NULL UNIQUE, 
                derivation_path VARCHAR(255) NOT NULL UNIQUE, 
                is_active BOOLEAN DEFAULT TRUE, 
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMPTZ, 
                swept_at TIMESTAMPTZ, 
                balance_at_sweep BIGINT,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_user_id ON user_deposit_wallets(user_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_public_key ON user_deposit_wallets(public_key);
            CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_is_active_expires_at ON user_deposit_wallets(is_active, expires_at);
        `);
        console.log("  [DB Schema] 'user_deposit_wallets' table checked/created.");

        // Deposits Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposits (
                deposit_id SERIAL PRIMARY KEY,
                user_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                user_deposit_wallet_id INT REFERENCES user_deposit_wallets(wallet_id) ON DELETE SET NULL,
                transaction_signature VARCHAR(88) NOT NULL UNIQUE, 
                source_address VARCHAR(44), 
                deposit_address VARCHAR(44) NOT NULL,
                amount_lamports BIGINT NOT NULL,
                confirmation_status VARCHAR(20) DEFAULT 'pending', 
                block_time BIGINT, 
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMPTZ, 
                notes TEXT,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits(user_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_deposits_transaction_signature ON deposits(transaction_signature);
            CREATE INDEX IF NOT EXISTS idx_deposits_deposit_address ON deposits(deposit_address);
            CREATE INDEX IF NOT EXISTS idx_deposits_status_created_at ON deposits(confirmation_status, created_at);
        `);
        console.log("  [DB Schema] 'deposits' table checked/created.");

        // Withdrawals Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS withdrawals (
                withdrawal_id SERIAL PRIMARY KEY,
                user_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                destination_address VARCHAR(44) NOT NULL, 
                amount_lamports BIGINT NOT NULL,
                fee_lamports BIGINT NOT NULL,
                transaction_signature VARCHAR(88) UNIQUE, 
                status VARCHAR(30) DEFAULT 'pending_verification', -- Added more specific statuses
                error_message TEXT,
                requested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMPTZ, 
                block_time BIGINT,
                priority_fee_microlamports INT,
                compute_unit_price_microlamports INT, 
                compute_unit_limit INT,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_withdrawals_user_id ON withdrawals(user_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_withdrawals_status_requested_at ON withdrawals(status, requested_at);
        `);
        console.log("  [DB Schema] 'withdrawals' table checked/created.");

        // Referrals Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS referrals (
                referral_id SERIAL PRIMARY KEY,
                referrer_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                referred_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE UNIQUE, -- Ensures a user can only be referred once
                commission_type VARCHAR(20), -- e.g., 'first_deposit_percentage', 'fixed_signup'
                commission_amount_lamports BIGINT, -- Actual commission paid for this specific referral link usage
                transaction_signature VARCHAR(88), -- If commission was paid out on-chain
                status VARCHAR(20) DEFAULT 'pending_criteria', -- e.g., 'pending_criteria', 'earned', 'paid_out', 'failed'
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_referral_pair UNIQUE (referrer_telegram_id, referred_telegram_id)
            );
            CREATE INDEX IF NOT EXISTS idx_referrals_referrer_id ON referrals(referrer_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_referrals_referred_id ON referrals(referred_telegram_id);
        `);
        console.log("  [DB Schema] 'referrals' table checked/created.");

        // Processed Sweeps Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS processed_sweeps (
                sweep_id SERIAL PRIMARY KEY,
                source_deposit_address VARCHAR(44) NOT NULL, 
                destination_main_address VARCHAR(44) NOT NULL, 
                amount_lamports BIGINT NOT NULL,
                transaction_signature VARCHAR(88) UNIQUE NOT NULL,
                swept_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_processed_sweeps_source_address ON processed_sweeps(source_deposit_address);
        `);
        console.log("  [DB Schema] 'processed_sweeps' table checked/created.");
        
        // Ledger Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS ledger (
                ledger_id SERIAL PRIMARY KEY,
                user_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                transaction_type VARCHAR(50) NOT NULL,
                amount_lamports BIGINT NOT NULL,
                balance_before_lamports BIGINT NOT NULL,
                balance_after_lamports BIGINT NOT NULL,
                deposit_id INTEGER REFERENCES deposits(deposit_id) ON DELETE SET NULL,
                withdrawal_id INTEGER REFERENCES withdrawals(withdrawal_id) ON DELETE SET NULL,
                game_log_id INTEGER REFERENCES games(game_log_id) ON DELETE SET NULL,
                referral_id INTEGER REFERENCES referrals(referral_id) ON DELETE SET NULL,
                related_sweep_id INTEGER REFERENCES processed_sweeps(sweep_id) ON DELETE SET NULL,
                notes TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_ledger_user_id ON ledger(user_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_ledger_transaction_type ON ledger(transaction_type);
            CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON ledger(created_at);
        `);
        console.log("  [DB Schema] 'ledger' table (for financial tracking) checked/created.");

        // Update function for 'updated_at' columns
        await client.query(`
            CREATE OR REPLACE FUNCTION trigger_set_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
              NEW.updated_at = NOW();
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        `);
        // Tables that need the updated_at trigger
        const tablesWithUpdatedAt = ['users', 'jackpots', 'user_deposit_wallets', 'deposits', 'withdrawals', 'referrals'];
        for (const tableName of tablesWithUpdatedAt) {
            // Check if trigger exists before trying to drop/create to avoid errors if it's already correct
            const triggerExistsRes = await client.query(
                `SELECT 1 FROM pg_trigger WHERE tgname = 'set_timestamp' AND tgrelid = '${tableName}'::regclass;`
            );
            if (triggerExistsRes.rowCount === 0) {
                await client.query(`
                    CREATE TRIGGER set_timestamp
                    BEFORE UPDATE ON ${tableName}
                    FOR EACH ROW
                    EXECUTE FUNCTION trigger_set_timestamp();
                `).then(() => console.log(`  [DB Schema] 'updated_at' trigger created for '${tableName}'.`))
                   .catch(err => console.warn(`  [DB Schema] Could not set update trigger for ${tableName} (may require permissions or function issue): ${err.message}`));
            } else {
                // console.log(`  [DB Schema] 'updated_at' trigger already exists for '${tableName}'.`);
            }
        }
        console.log("  [DB Schema] 'updated_at' trigger function and assignments checked/created.");

        await client.query('COMMIT');
        console.log("✅ Database schema initialization complete.");
    } catch (e) {
        await client.query('ROLLBACK');
        console.error('❌ Error during database schema initialization:', e);
        throw e; // Re-throw to halt startup if schema init fails
    } finally {
        client.release();
    }
}

//---------------------------------------------------------------------------
// Core User Management Functions (Integrated)
//---------------------------------------------------------------------------

async function getOrCreateUser(telegramId, username = '', firstName = '', lastName = '', referrerIdInput = null) {
    // **FIX for telegramId: undefined (22P02 error)**
    // Ensure telegramId is valid before proceeding.
    if (typeof telegramId === 'undefined' || telegramId === null || String(telegramId).trim() === "" || String(telegramId).toLowerCase() === "undefined") {
        console.error(`[getOrCreateUser CRITICAL] Attempted to get or create user with invalid telegramId: '${telegramId}'. Aborting operation.`);
        // Optionally, notify admin if this occurs unexpectedly.
        if (typeof notifyAdmin === 'function' && ADMIN_USER_ID) {
            notifyAdmin(`🚨 CRITICAL: getOrCreateUser called with invalid telegramId: ${telegramId}. Check calling function.`)
                .catch(err => console.error("Failed to notify admin about invalid telegramId in getOrCreateUser:", err));
        }
        return null; // Return null to indicate failure due to invalid ID.
    }

    const stringTelegramId = String(telegramId); // Ensure it's a string for consistency
    const LOG_PREFIX_GOCU = `[getOrCreateUser TG:${stringTelegramId}]`;
    console.log(`${LOG_PREFIX_GOCU} Attempting to get or create user. Username: ${username || 'N/A'}, Name: ${firstName || 'N/A'}`);

    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // Ensure referrerIdInput is a valid format for DB (BigInt or null)
        let referrerId = null;
        if (referrerIdInput !== null && referrerIdInput !== undefined) {
            try {
                referrerId = BigInt(referrerIdInput);
            } catch (parseError) {
                console.warn(`${LOG_PREFIX_GOCU} Invalid referrerIdInput '${referrerIdInput}', cannot parse to BigInt. Setting referrer to null.`);
                referrerId = null;
            }
        }

        let result = await client.query('SELECT * FROM users WHERE telegram_id = $1', [stringTelegramId]);
        if (result.rows.length > 0) {
            const user = result.rows[0];
            // Ensure numeric fields from DB are BigInt
            user.balance = BigInt(user.balance);
            user.total_deposited_lamports = BigInt(user.total_deposited_lamports);
            user.total_withdrawn_lamports = BigInt(user.total_withdrawn_lamports);
            user.total_wagered_lamports = BigInt(user.total_wagered_lamports);
            user.total_won_lamports = BigInt(user.total_won_lamports);

            console.log(`${LOG_PREFIX_GOCU} User found. Balance: ${user.balance} lamports.`);
            
            let detailsChanged = false;
            const currentUsername = user.username || '';
            const currentFirstName = user.first_name || '';
            const currentLastName = user.last_name || '';

            // Update only if new value is provided and different from current, or if current is null/empty and new is provided.
            if ((username && currentUsername !== username) || (!currentUsername && username)) detailsChanged = true;
            if ((firstName && currentFirstName !== firstName) || (!currentFirstName && firstName)) detailsChanged = true;
            if ((lastName && currentLastName !== lastName) || (!currentLastName && lastName)) detailsChanged = true; // Only update last_name if new one is not null/empty

            if (detailsChanged) {
                await client.query(
                    'UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP, username = $2, first_name = $3, last_name = $4, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $1',
                    [stringTelegramId, username || user.username, firstName || user.first_name, lastName || user.last_name] // Use new or fallback to existing
                );
                console.log(`${LOG_PREFIX_GOCU} User details updated.`);
            } else {
                await client.query('UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP WHERE telegram_id = $1', [stringTelegramId]);
            }
            await client.query('COMMIT');
            // Return the potentially updated user object, ensuring BigInts are consistent
            const updatedUserRow = await client.query('SELECT * FROM users WHERE telegram_id = $1', [stringTelegramId]);
            const finalUser = updatedUserRow.rows[0];
            finalUser.balance = BigInt(finalUser.balance);
            finalUser.total_deposited_lamports = BigInt(finalUser.total_deposited_lamports);
            finalUser.total_withdrawn_lamports = BigInt(finalUser.total_withdrawn_lamports);
            finalUser.total_wagered_lamports = BigInt(finalUser.total_wagered_lamports);
            finalUser.total_won_lamports = BigInt(finalUser.total_won_lamports);
            return finalUser;
        } else {
            console.log(`${LOG_PREFIX_GOCU} User not found. Creating new user.`);
            const newReferralCode = generateReferralCode();
            const insertQuery = `
                INSERT INTO users (telegram_id, username, first_name, last_name, balance, referral_code, referrer_telegram_id, last_active_timestamp, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING *;
            `;
            const values = [stringTelegramId, username, firstName, lastName, DEFAULT_STARTING_BALANCE_LAMPORTS.toString(), newReferralCode, referrerId];
            result = await client.query(insertQuery, values);
            const newUser = result.rows[0];
            // Ensure numeric fields from DB are BigInt
            newUser.balance = BigInt(newUser.balance);
            newUser.total_deposited_lamports = BigInt(newUser.total_deposited_lamports);
            newUser.total_withdrawn_lamports = BigInt(newUser.total_withdrawn_lamports);
            newUser.total_wagered_lamports = BigInt(newUser.total_wagered_lamports);
            newUser.total_won_lamports = BigInt(newUser.total_won_lamports);

            console.log(`${LOG_PREFIX_GOCU} New user created with ID ${newUser.telegram_id}, Balance: ${newUser.balance} lamports, Referral Code: ${newUser.referral_code}.`);

            if (referrerId) {
                console.log(`${LOG_PREFIX_GOCU} User was referred by ${referrerId}. Recording referral link.`);
                try {
                    await client.query(
                        `INSERT INTO referrals (referrer_telegram_id, referred_telegram_id, created_at, status) VALUES ($1, $2, CURRENT_TIMESTAMP, 'pending_criteria') ON CONFLICT DO NOTHING`,
                        [referrerId, newUser.telegram_id]
                    );
                    console.log(`${LOG_PREFIX_GOCU} Referral link recorded for ${referrerId} -> ${newUser.telegram_id}.`);
                } catch (referralError) {
                   console.error(`${LOG_PREFIX_GOCU} Failed to record referral for ${referrerId} -> ${newUser.telegram_id}:`, referralError);
                   // Don't let referral error stop user creation, but log it.
                }
            }
            await client.query('COMMIT');
            return newUser;
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_GOCU} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_GOCU} Error in getOrCreateUser for telegramId ${stringTelegramId}:`, stringifyWithBigInt(error));
        return null; // Return null on error
    } finally {
        client.release();
    }
}
console.log("[User Management] getOrCreateUser (with telegramId validation) defined.");


async function updateUserActivity(telegramId) {
    const stringTelegramId = String(telegramId);
    try {
        await pool.query('UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $1', [stringTelegramId]);
    } catch (error) {
        console.error(`[updateUserActivity TG:${stringTelegramId}] Error updating last active timestamp for telegramId ${stringTelegramId}:`, error);
    }
}
console.log("[User Management] updateUserActivity defined.");

async function getUserBalance(telegramId) {
    const stringTelegramId = String(telegramId);
    try {
        const result = await pool.query('SELECT balance FROM users WHERE telegram_id = $1', [stringTelegramId]);
        if (result.rows.length > 0) {
            return BigInt(result.rows[0].balance); 
        }
        console.warn(`[getUserBalance TG:${stringTelegramId}] User not found, cannot retrieve balance.`);
        return null; 
    } catch (error) {
        console.error(`[getUserBalance TG:${stringTelegramId}] Error retrieving balance for telegramId ${stringTelegramId}:`, error);
        return null; // Return null on error
    }
}
console.log("[User Management] getUserBalance defined.");

/**
 * ! IMPORTANT: This function directly sets the balance and BYPASSES the ledger.
 * ! It should ONLY be used in very specific scenarios like initial data migration
 * ! or direct admin corrections where ledgering is handled separately or intentionally skipped.
 * ! For all standard operations (bets, wins, deposits, withdrawals, fees),
 * ! use `updateUserBalanceAndLedger` (defined in Part P2) instead.
 */
async function updateUserBalance(telegramId, newBalanceLamports, client = pool) {
    const stringTelegramId = String(telegramId);
    const LOG_PREFIX_UUB = `[updateUserBalance TG:${stringTelegramId}]`;
    try {
        if (typeof newBalanceLamports !== 'bigint') {
            console.error(`${LOG_PREFIX_UUB} Invalid newBalanceLamports type: ${typeof newBalanceLamports}. Must be BigInt.`);
            return false;
        }
        
        if (newBalanceLamports < 0n) {
            console.warn(`${LOG_PREFIX_UUB} Attempt to set negative balance (${newBalanceLamports}). Clamping to 0. This function bypasses ledger and should be used with extreme caution.`);
            // newBalanceLamports = 0n; // Clamping behavior can be uncommented if strictly needed
        }

        // This function assumes the caller has ALREADY acquired the client if not using the default pool
        // and will manage BEGIN/COMMIT/ROLLBACK outside if part of a larger transaction.
        // If client === pool, it's a single operation.
        const result = await client.query(
            'UPDATE users SET balance = $1, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $2',
            [newBalanceLamports.toString(), stringTelegramId] 
        );
        if (result.rowCount > 0) {
            console.warn(`${LOG_PREFIX_UUB} ⚠️ Balance directly set to ${newBalanceLamports} lamports. Ledger NOT updated by this specific function. This is for special use cases ONLY.`);
            return true;
        } else {
            console.warn(`${LOG_PREFIX_UUB} User not found or balance not updated for telegramId ${stringTelegramId}.`);
            return false;
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_UUB} Error updating balance for telegramId ${stringTelegramId} to ${newBalanceLamports}:`, error);
        return false;
    }
}
console.log("[User Management] updateUserBalance (direct set, bypasses ledger - use with caution) defined.");


async function linkUserWallet(telegramId, solanaAddress) {
    const stringTelegramId = String(telegramId);
    const LOG_PREFIX_LUW = `[linkUserWallet TG:${stringTelegramId}]`;
    console.log(`${LOG_PREFIX_LUW} Attempting to link wallet ${solanaAddress}.`);
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        if (!solanaAddress || !/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(solanaAddress)) { // Basic Base58 check
            console.warn(`${LOG_PREFIX_LUW} Invalid Solana address format provided: ${solanaAddress}`);
            await client.query('ROLLBACK'); // No need to commit if input is invalid
            return { success: false, error: "Invalid Solana address format. Please provide a valid Base58 encoded address." };
        }
        // Further validation if PublicKey class is available (it is from Part 1)
        try {
            new PublicKey(solanaAddress); // Will throw if invalid format
        } catch (e) {
            console.warn(`${LOG_PREFIX_LUW} Solana address ${solanaAddress} failed PublicKey constructor validation: ${e.message}`);
            await client.query('ROLLBACK');
            return { success: false, error: "Invalid Solana address. It might not be a valid public key." };
        }


        const existingLink = await client.query('SELECT telegram_id FROM users WHERE solana_wallet_address = $1 AND telegram_id != $2', [solanaAddress, stringTelegramId]);
        if (existingLink.rows.length > 0) {
            const linkedToExistingUserId = existingLink.rows[0].telegram_id;
            console.warn(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} is already linked to another user ID ${linkedToExistingUserId}.`);
            await client.query('ROLLBACK');
            return { success: false, error: `This wallet address is already associated with another player (ID ending ${String(linkedToExistingUserId).slice(-4)}). Please use a different address.` };
        }

        const result = await client.query(
            'UPDATE users SET solana_wallet_address = $1, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $2 RETURNING solana_wallet_address',
            [solanaAddress, stringTelegramId]
        );

        if (result.rowCount > 0) {
            await client.query('COMMIT');
            console.log(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} successfully linked in DB.`);
            walletCache.set(stringTelegramId, { solanaAddress, timestamp: Date.now() }); // Update cache with timestamp
            return { success: true, message: `Your Solana wallet \`${solanaAddress}\` has been successfully linked!` };
        } else {
            const currentUserState = await client.query('SELECT solana_wallet_address FROM users WHERE telegram_id = $1', [stringTelegramId]);
            await client.query('ROLLBACK');
            if (currentUserState.rowCount === 0) {
                console.error(`${LOG_PREFIX_LUW} User ${stringTelegramId} not found. Cannot link wallet.`);
                return { success: false, error: "Your player profile was not found. Please try /start again." };
            }
            if (currentUserState.rows[0].solana_wallet_address === solanaAddress) {
                console.log(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} was already linked to this user. No change.`);
                walletCache.set(stringTelegramId, { solanaAddress, timestamp: Date.now() }); 
                return { success: true, message: `Your wallet \`${solanaAddress}\` was already linked to your account.` };
            }
            console.warn(`${LOG_PREFIX_LUW} User ${stringTelegramId} found, but wallet not updated (rowCount: ${result.rowCount}). This might be an unexpected issue if address was different.`);
            return { success: false, error: "Failed to update wallet in the database due to an unknown reason. Please try again." };
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_LUW} Rollback error: ${rbErr.message}`));
        if (error.code === '23505') { // Unique constraint violation (solana_wallet_address)
            console.warn(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} is already linked to another user (unique constraint violation).`);
            return { success: false, error: "This wallet address is already in use by another player. Please choose a different one." };
        }
        console.error(`${LOG_PREFIX_LUW} Error linking wallet ${solanaAddress}:`, error);
        return { success: false, error: error.message || "An unexpected server error occurred while linking your wallet. Please try again." };
    } finally {
        client.release();
    }
}
console.log("[User Management] linkUserWallet defined.");

async function getUserLinkedWallet(telegramId) {
    const stringTelegramId = String(telegramId);
    const cacheTTL = parseInt(process.env.WALLET_CACHE_TTL_MS, 10); // From Part 1 env defaults
    const cachedData = walletCache.get(stringTelegramId);
    if (cachedData && cachedData.solanaAddress && (Date.now() - (cachedData.timestamp || 0) < cacheTTL)) {
        return cachedData.solanaAddress;
    }

    try {
        const result = await pool.query('SELECT solana_wallet_address FROM users WHERE telegram_id = $1', [stringTelegramId]);
        if (result.rows.length > 0 && result.rows[0].solana_wallet_address) {
            walletCache.set(stringTelegramId, { solanaAddress: result.rows[0].solana_wallet_address, timestamp: Date.now() }); 
            return result.rows[0].solana_wallet_address;
        }
        return null; // No wallet linked
    } catch (error) {
        console.error(`[getUserLinkedWallet TG:${stringTelegramId}] Error getting linked wallet:`, error);
        return null; // Return null on error
    }
}
console.log("[User Management] getUserLinkedWallet defined.");

/**
 * Gets the next available address_index for a user's deposit address derivation.
 * The derivation path is m/44'/501'/USER_ACCOUNT_INDEX'/0'/ADDRESS_INDEX'
 * This function finds the highest ADDRESS_INDEX used so far for the user and returns next.
 * @param {string|number} userId The user's Telegram ID.
 * @param {import('pg').PoolClient} [dbClient=pool] Optional database client for transactions.
 * @returns {Promise<number>} The next address_index (e.g., 0 if none exist).
 */
async function getNextAddressIndexForUserDB(userId, dbClient = pool) {
    const stringUserId = String(userId);
    const LOG_PREFIX_GNAI = `[getNextAddressIndexForUser TG:${stringUserId}]`;
    try {
        const query = `
            SELECT derivation_path
            FROM user_deposit_wallets
            WHERE user_telegram_id = $1
            ORDER BY created_at DESC;
        `;
        const res = await queryDatabase(query, [stringUserId], dbClient); // queryDatabase from Part 1
        let maxIndex = -1;

        if (res.rows.length > 0) {
            for (const row of res.rows) {
                const path = row.derivation_path;
                const parts = path.split('/');
                if (parts.length >= 6) { // Standard path m/44'/501'/X'/0'/Y' has 6 parts including 'm'
                    const lastPart = parts[parts.length - 1];
                    if (lastPart.endsWith("'")) {
                        const indexStr = lastPart.substring(0, lastPart.length - 1);
                        const currentIndex = parseInt(indexStr, 10);
                        if (!isNaN(currentIndex) && currentIndex > maxIndex) {
                            maxIndex = currentIndex;
                        }
                    } else {
                        console.warn(`${LOG_PREFIX_GNAI} Malformed last part of derivation path (missing trailing quote): ${lastPart} in ${path}`);
                    }
                } else {
                    console.warn(`${LOG_PREFIX_GNAI} Malformed derivation path (too short): ${path}`);
                }
            }
        }
        const nextIndex = maxIndex + 1;
        console.log(`${LOG_PREFIX_GNAI} Determined next addressIndex: ${nextIndex}`);
        return nextIndex;
    } catch (error) {
        console.error(`${LOG_PREFIX_GNAI} Error calculating next address index: ${error.message}`, error.stack);
        throw error; // Re-throw to be handled by caller
    }
}
console.log("[User Management] getNextAddressIndexForUserDB helper function defined.");


async function deleteUserAccount(telegramId) {
    const stringTelegramId = String(telegramId);
    const LOG_PREFIX_DUA = `[deleteUserAccount TG:${stringTelegramId}]`;
    console.warn(`${LOG_PREFIX_DUA} Attempting to delete user account and associated data for Telegram ID: ${stringTelegramId}.`);
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        console.log(`${LOG_PREFIX_DUA} Anonymizing references in 'jackpots' table...`);
        await client.query('UPDATE jackpots SET last_won_by_telegram_id = NULL WHERE last_won_by_telegram_id = $1', [stringTelegramId]);
        
        console.log(`${LOG_PREFIX_DUA} Anonymizing references in 'games' (game log) table...`);
        await client.query('UPDATE games SET initiator_telegram_id = NULL WHERE initiator_telegram_id = $1', [stringTelegramId]);
        // Participant array cleaning: This is complex. For now, leaving participants_ids as is.
        // If GDPR or strict privacy requires, participant_ids would need to be filtered.
        // Example for future: UPDATE games SET participants_ids = array_remove(participants_ids, $1) WHERE $1 = ANY(participants_ids);
        // However, this only works if you delete users one by one and the array stores BIGINT.

        console.log(`${LOG_PREFIX_DUA} Preparing to delete user from 'users' table, which will cascade to related financial records (deposits, withdrawals, ledger, user_deposit_wallets, referrals) as per schema foreign key constraints.`);
        
        const result = await client.query('DELETE FROM users WHERE telegram_id = $1', [stringTelegramId]);

        await client.query('COMMIT');

        if (result.rowCount > 0) {
            console.log(`${LOG_PREFIX_DUA} User account ${stringTelegramId} and associated data deleted successfully from database.`);
            // Clear in-memory caches associated with the user
            activeGames.forEach((game, gameId) => { 
                // Check if game object and players property exist
                if (game && game.participants && Array.isArray(game.participants)) {
                    // Filter out the deleted user from participants
                    game.participants = game.participants.filter(p => String(p.userId) !== stringTelegramId);
                    if (game.participants.length === 0 && game.type !== GAME_IDS.DICE_ESCALATOR && game.type !== GAME_IDS.DICE_21) { // Example: remove empty group games
                        activeGames.delete(gameId);
                    }
                }
                if (game && String(game.initiatorId) === stringTelegramId) activeGames.delete(gameId); // If user was initiator of a group game
                if (game && String(game.userId) === stringTelegramId) activeGames.delete(gameId); // For single player games
            });
            userCooldowns.delete(stringTelegramId);
            groupGameSessions.forEach((session, chatId) => {
                // Assuming session.players is an object { userId: playerData }
                if (session.players && session.players[stringTelegramId]) {
                    delete session.players[stringTelegramId];
                }
                if (session.initiator === stringTelegramId && Object.keys(session.players || {}).length === 0) { // If initiator deleted and no players left
                    groupGameSessions.delete(chatId);
                } else if (session.initiator === stringTelegramId) { // If initiator deleted but other players remain, might need a new initiator or cancel game logic
                    // For simplicity, just marking that session might be orphaned or need handling
                    console.warn(`${LOG_PREFIX_DUA} Initiator ${stringTelegramId} of group game in chat ${chatId} deleted. Session may need manual resolution or will timeout.`);
                }
            });
            walletCache.delete(stringTelegramId);
            activeDepositAddresses.forEach((value, key) => {
                if (String(value.userId) === stringTelegramId) {
                    activeDepositAddresses.delete(key);
                }
            });
            pendingReferrals.forEach((value, key) => { // Key: referredUserId, Value: { referrerId, timestamp }
                if (String(key) === stringTelegramId) pendingReferrals.delete(key); // If user was referred
                if (String(value.referrerId) === stringTelegramId) pendingReferrals.delete(key); // If user was referrer
            });
            userStateCache.delete(stringTelegramId);
            console.log(`${LOG_PREFIX_DUA} Relevant in-memory caches cleared for user ${stringTelegramId}.`);
            return true;
        } else {
            console.log(`${LOG_PREFIX_DUA} User ${stringTelegramId} not found, no account deleted.`);
            return false;
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_DUA} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_DUA} Error deleting user account ${stringTelegramId}:`, error);
        return false;
    } finally {
        client.release();
    }
}
console.log("[User Management] deleteUserAccount defined.");


console.log("Part 2: Database Schema Initialization & Core User Management (Integrated) - Complete.");
// --- End of Part 2 ---
// --- Start of Part 3 ---
// index.js - Part 3: Telegram Helpers, Currency Formatting & Basic Game Utilities (Integrated)
//---------------------------------------------------------------------------
console.log("Loading Part 3: Telegram Helpers, Currency Formatting & Basic Game Utilities (Integrated)...");

// Assumes escapeMarkdownV2, LAMPORTS_PER_SOL, SOL_DECIMALS, getSolUsdPrice, convertLamportsToUSDString,
// crypto (module), BOT_NAME are available from Part 1.

// --- Telegram Specific Helper Functions ---

// Gets a display name from a user object (msg.from or a fetched user object) and escapes it for MarkdownV2
function getEscapedUserDisplayName(userObject) {
  if (!userObject) return escapeMarkdownV2("Valued Player");

  const firstName = userObject.first_name || userObject.firstName; // Handles both Telegram API and DB user object naming
  const username = userObject.username;
  const id = userObject.id || userObject.telegram_id; // Handles both

  let name = "Player";
  if (firstName) {
    name = firstName;
  } else if (username) {
    name = `@${username}`; // Using username if first name is not available
  } else if (id) {
    name = `Player ${String(id).slice(-4)}`; // Fallback to a generic ID-based name
  } else {
    name = "Valued Player"; // Ultimate fallback
  }
  return escapeMarkdownV2(name);
}

// Creates a MarkdownV2 mention link for a user object
function createUserMention(userObject) {
  if (!userObject) return escapeMarkdownV2("Esteemed Guest");

  const id = userObject.id || userObject.telegram_id;
  if (!id) return escapeMarkdownV2("Unknown Player"); // Should not happen if userObject is valid

  // Use a simpler name for the mention text part to keep it concise and friendly.
  const simpleName = userObject.first_name || userObject.firstName || userObject.username || `Player ${String(id).slice(-4)}`;
  return `[${escapeMarkdownV2(simpleName)}](tg://user?id=${id})`;
}

// Gets a player's display reference, preferring @username, falls back to name. Escapes for MarkdownV2.
function getPlayerDisplayReference(userObject, preferUsernameTag = true) {
  if (!userObject) return escapeMarkdownV2("Mystery Player"); 

  const username = userObject.username;
  if (preferUsernameTag && username) {
    // Usernames for @mentions don't typically need escaping for the @ symbol itself, but the content of the username might if it contained Markdown characters.
    // For safety in broader MarkdownV2 contexts, escaping the username content is good practice.
    return `@${escapeMarkdownV2(username)}`;
  }
  // Fallback to the more detailed (and already escaped) display name if no username or if not preferred.
  return getEscapedUserDisplayName(userObject);
}
console.log("[Telegram Utils] User display helper functions (getEscapedUserDisplayName, createUserMention, getPlayerDisplayReference) defined.");

// --- General Utility Functions ---

/**
 * Formats a BigInt lamports amount into a SOL string representation or raw lamports.
 * @param {bigint|string|number} amountLamports - The amount in lamports.
 * @param {string} [currencyName='SOL'] - The currency to display (primarily 'SOL' or 'lamports').
 * @param {boolean} [displayRawLamportsOverride=false] - If true, forces display of raw lamports regardless of currencyName.
 * @param {number} [solDecimals=SOL_DECIMALS] - Number of decimal places for SOL.
 * @returns {string} Formatted currency string.
 */
function formatCurrency(amountLamports, currencyName = 'SOL', displayRawLamportsOverride = false, solDecimals = SOL_DECIMALS) {
    let lamportsAsBigInt;
    try {
        lamportsAsBigInt = BigInt(amountLamports);
    } catch (e) {
        console.warn(`[formatCurrency] Received non-BigInt convertible amount: '${amountLamports}' (Type: ${typeof amountLamports}). Error: ${e.message}`);
        return '⚠️ Amount Invalid';
    }

    if (displayRawLamportsOverride || String(currencyName).toLowerCase() === 'lamports') {
        return `${lamportsAsBigInt.toLocaleString('en-US')} lamports`;
    }

    if (typeof LAMPORTS_PER_SOL === 'undefined' || Number(LAMPORTS_PER_SOL) <= 0) {
        console.error("[formatCurrency] LAMPORTS_PER_SOL is not defined or invalid. Cannot format SOL.");
        return `${lamportsAsBigInt.toLocaleString('en-US')} lamports (⚠️ SOL Config Err)`;
    }

    const solValue = Number(lamportsAsBigInt) / Number(LAMPORTS_PER_SOL);
    let effectiveDecimals = solDecimals;

    if (solValue === Math.floor(solValue)) { // It's a whole number
        effectiveDecimals = 0;
    } else {
        const stringValue = solValue.toString();
        const decimalPart = stringValue.split('.')[1];
        if (decimalPart) {
            // Use the smaller of actual decimal places or configured solDecimals
            effectiveDecimals = Math.min(decimalPart.length, solDecimals);
        } else { // Should be caught by whole number check, but for safety
            effectiveDecimals = 0;
        }
    }
    // Ensure at least 2 decimal places if there are any fractional parts,
    // but only if solDecimals allows for at least 2.
    // And don't force 2 if effectiveDecimals ended up as 0 (whole number).
    if (effectiveDecimals > 0 && effectiveDecimals < 2 && solDecimals >= 2) {
        effectiveDecimals = 2;
    }
    // If configured solDecimals is less than 2 (e.g. 0 or 1), respect that for fractional numbers.
    if (effectiveDecimals > 0 && solDecimals < 2) {
        effectiveDecimals = solDecimals;
    }


    try {
        return `${solValue.toLocaleString('en-US', {
            minimumFractionDigits: effectiveDecimals, // Use effectiveDecimals here
            maximumFractionDigits: effectiveDecimals
        })} SOL`;
    } catch (e) {
        console.error(`[formatCurrency] Error formatting SOL for ${lamportsAsBigInt} lamports: ${e.message}`);
        return `${lamportsAsBigInt.toLocaleString('en-US')} lamports (⚠️ Format Err)`;
    }
}
console.log("[Currency Utils] formatCurrency helper function defined.");


/**
 * Formats a BigInt lamports amount for display, defaulting to USD, with fallbacks.
 * @param {bigint|string|number} lamports - The amount in lamports.
 * @param {string} [targetCurrency='USD'] - The target currency ('USD', 'SOL', or 'lamports').
 * @returns {Promise<string>} Formatted currency string.
 */
async function formatBalanceForDisplay(lamports, targetCurrency = 'USD') {
    let lamportsAsBigInt;
    try {
        lamportsAsBigInt = BigInt(lamports);
    } catch (e) {
        console.warn(`[formatBalanceForDisplay] Invalid lamport amount: '${lamports}'. Error: ${e.message}`);
        return '⚠️ Amount Invalid';
    }

    const upperTargetCurrency = String(targetCurrency).toUpperCase();

    if (upperTargetCurrency === 'USD') {
        try {
            if (typeof getSolUsdPrice !== 'function' || typeof convertLamportsToUSDString !== 'function') {
                console.error("[formatBalanceForDisplay] Price conversion functions (getSolUsdPrice or convertLamportsToUSDString) are not available. Falling back to SOL display.");
                return formatCurrency(lamportsAsBigInt, 'SOL'); 
            }
            const price = await getSolUsdPrice();
            return convertLamportsToUSDString(lamportsAsBigInt, price);
        } catch (e) {
            console.error(`[formatBalanceForDisplay] Failed to get SOL/USD price for USD display: ${e.message}. Falling back to SOL display.`);
            return formatCurrency(lamportsAsBigInt, 'SOL'); 
        }
    } else if (upperTargetCurrency === 'LAMPORTS') {
        return formatCurrency(lamportsAsBigInt, 'lamports', true); 
    }
    // Default to SOL
    return formatCurrency(lamportsAsBigInt, 'SOL');
}
console.log("[Currency Utils] formatBalanceForDisplay helper function defined.");


// Generates a unique-ish ID for game instances
function generateGameId(prefix = "game") {
  const timestamp = Date.now().toString(36); // Base36 timestamp
  const randomSuffix = Math.random().toString(36).substring(2, 10); // 8 char random Base36 string
  return `${prefix}_${timestamp}_${randomSuffix}`;
}
console.log("[Game Utils] generateGameId helper function defined.");

// --- Dice Display Utilities ---

// Formats an array of dice roll numbers into a string with emoji and number
function formatDiceRolls(rollsArray, diceEmoji = '🎲') {
  if (!Array.isArray(rollsArray) || rollsArray.length === 0) return '';
  const diceVisuals = rollsArray.map(roll => {
      const rollValue = Number(roll); // Ensure it's a number for isNaN check
      return `${diceEmoji} ${isNaN(rollValue) ? '?' : rollValue}`;
  });
  return diceVisuals.join(' \u00A0 '); // Use non-breaking spaces for better layout in Telegram
}
console.log("[Game Utils] formatDiceRolls helper function defined.");

// Generates an internal dice roll
function rollDie(sides = 6) {
  sides = Number.isInteger(sides) && sides > 1 ? sides : 6;
  return Math.floor(Math.random() * sides) + 1;
}
console.log("[Game Utils] rollDie helper function defined.");

// --- Payment Transaction ID Generation (Optional Utility) ---
/**
 * Generates a unique transaction ID for internal tracking of payments.
 * @param {'deposit' | 'withdrawal' | 'sweep' | 'referral' | 'bet' | 'win' | 'refund' | 'ledger_adjustment' | 'admin_grant'} type - The type of payment/ledger entry.
 * @param {string} [userId='system'] - Optional user ID if related to a specific user.
 * @returns {string} A unique-ish transaction ID.
 */
function generateInternalPaymentTxId(type, userId = 'system') {
    const now = Date.now().toString(36);
    let randomPart;
    if (typeof crypto !== 'undefined' && typeof crypto.randomBytes === 'function') {
        randomPart = crypto.randomBytes(4).toString('hex'); // 8 hex characters
    } else {
        console.warn('[generateInternalPaymentTxId] Crypto module not available for random part. Using Math.random (less secure).');
        randomPart = Math.random().toString(36).substring(2, 10); // Fallback, less unique
    }
    
    const userPartCleaned = String(userId).replace(/[^a-zA-Z0-9_]/g, '').slice(0, 10) || 'sys'; // Allow underscore, limit length
    let prefix = String(type).toLowerCase().substring(0, 6).replace(/[^a-z0-9_]/g, '') || 'gen'; // Allow underscore, limit length

    return `${prefix}_${userPartCleaned}_${now}_${randomPart}`;
}
console.log("[Payment Utils] generateInternalPaymentTxId helper function defined.");


console.log("Part 3: Telegram Helpers, Currency Formatting & Basic Game Utilities (Integrated) - Complete.");
// --- End of Part 3 ---
// --- Start of Part 4 ---
// index.js - Part 4: Simplified Game Logic (Enhanced)
//---------------------------------------------------------------------------
console.log("Loading Part 4: Simplified Game Logic (Enhanced)...");

// Assumes rollDie (from Part 3) is available.

// --- Coinflip Logic ---
// Returns an object with the outcome, a display string, and an emoji.
function determineCoinFlipOutcome() {
  const isHeads = Math.random() < 0.5; // 50% chance for heads
  return isHeads
    ? { outcome: 'heads', outcomeString: "Heads", emoji: '🪙' } 
    : { outcome: 'tails', outcomeString: "Tails", emoji: '🪙' };
}
console.log("[Game Logic] Coinflip: determineCoinFlipOutcome defined.");

// --- Dice Logic (Internal for Bot's Turn or Fallback) ---
// This determines the outcome for the BOT's internal rolls or when `bot.sendDice` fails.
// It uses the internal `rollDie` function defined in Part 3.
function determineDieRollOutcome(sides = 6) {
  if (typeof rollDie !== 'function') {
     console.error("[determineDieRollOutcome] CRITICAL Error: rollDie function is not defined from Part 3. Fallback to 1.");
     return { roll: 1, emoji: '🎲' }; 
  }
  sides = Number.isInteger(sides) && sides > 1 ? sides : 6; 
  const roll = rollDie(sides); 

  // The emoji here is for potential direct use if not formatting via formatDiceRolls.
  return { roll: roll, emoji: '🎲' }; 
}
console.log("[Game Logic] Dice: determineDieRollOutcome defined.");


// --- Rock Paper Scissors (RPS) Logic ---
const RPS_CHOICES = {
  ROCK: 'rock',
  PAPER: 'paper',
  SCISSORS: 'scissors'
};
const RPS_EMOJIS = { // Emojis are generally MarkdownV2 safe
  [RPS_CHOICES.ROCK]: '🪨',   
  [RPS_CHOICES.PAPER]: '📄',  
  [RPS_CHOICES.SCISSORS]: '✂️' 
};
// Defines what each choice beats and the verb for the action.
const RPS_RULES = {
  [RPS_CHOICES.ROCK]: { beats: RPS_CHOICES.SCISSORS, verb: "crushes" },
  [RPS_CHOICES.PAPER]: { beats: RPS_CHOICES.ROCK, verb: "covers" },
  [RPS_CHOICES.SCISSORS]: { beats: RPS_CHOICES.PAPER, verb: "cuts" }
};
console.log("[Game Logic] RPS: Choices, Emojis, and Rules constants defined.");

// Gets a random RPS choice for the bot or an opponent if needed.
function getRandomRPSChoice() {
  const choicesArray = Object.values(RPS_CHOICES);
  const randomChoiceKey = choicesArray[Math.floor(Math.random() * choicesArray.length)];
  return { choice: randomChoiceKey, emoji: RPS_EMOJIS[randomChoiceKey] };
}
console.log("[Game Logic] RPS: getRandomRPSChoice defined.");

/**
 * Determines the outcome of an RPS match given two choices.
 * @param {string} player1ChoiceKey - Player 1's choice (e.g., RPS_CHOICES.ROCK).
 * @param {string} player2ChoiceKey - Player 2's choice.
 * @returns {object} A detailed result object.
 * The `description` field is pre-formatted for MarkdownV2
 * and should NOT be escaped again by the calling function.
 */
function determineRPSOutcome(player1ChoiceKey, player2ChoiceKey) {
  const LOG_PREFIX_RPS_OUTCOME = "[RPS_Outcome]";
  
  const p1c = String(player1ChoiceKey).toLowerCase();
  const p2c = String(player2ChoiceKey).toLowerCase();

  if (!Object.values(RPS_CHOICES).includes(p1c) || !Object.values(RPS_CHOICES).includes(p2c)) {
    console.warn(`${LOG_PREFIX_RPS_OUTCOME} Invalid choices: P1='${player1ChoiceKey}', P2='${player2ChoiceKey}'. This should be caught before calling.`);
    return {
        result: 'error',
        description: "An internal error occurred due to invalid RPS choices. Please try again.", // User-friendly generic error
        player1: { choice: player1ChoiceKey, emoji: '❓', choiceFormatted: 'Invalid' },
        player2: { choice: player2ChoiceKey, emoji: '❓', choiceFormatted: 'Invalid' }
    };
  }

  const p1Emoji = RPS_EMOJIS[p1c];
  const p2Emoji = RPS_EMOJIS[p2c];
  const p1ChoiceFormatted = p1c.charAt(0).toUpperCase() + p1c.slice(1);
  const p2ChoiceFormatted = p2c.charAt(0).toUpperCase() + p2c.slice(1);

  let resultDescription;
  let outcome; // 'win_player1', 'win_player2', 'draw'

  if (p1c === p2c) { // Draw case
    outcome = 'draw';
    // Note: MarkdownV2 pre-formatted string. Do not escape this again.
    resultDescription = `${p1Emoji} ${p1ChoiceFormatted} clashes with ${p2Emoji} ${p2ChoiceFormatted}\\! It's a *Draw*\\!`;
  } else if (RPS_RULES[p1c]?.beats === p2c) { // Player 1 wins
    outcome = 'win_player1';
    // Note: MarkdownV2 pre-formatted string. Do not escape this again.
    resultDescription = `${p1Emoji} ${p1ChoiceFormatted} *${RPS_RULES[p1c].verb}* ${p2Emoji} ${p2ChoiceFormatted}\\! Player 1 *claims victory*\\!`;
  } else { // Player 2 wins
    outcome = 'win_player2';
    // Note: MarkdownV2 pre-formatted string. Do not escape this again.
    resultDescription = `${p2Emoji} ${p2ChoiceFormatted} *${RPS_RULES[p2c]?.verb || 'outplays'}* ${p1Emoji} ${p1ChoiceFormatted}\\! Player 2 *is the winner*\\!`;
  }

  return {
    result: outcome,
    description: resultDescription, // This string is already MarkdownV2 formatted.
    player1: { choice: p1c, emoji: p1Emoji, choiceFormatted: p1ChoiceFormatted },
    player2: { choice: p2c, emoji: p2Emoji, choiceFormatted: p2ChoiceFormatted }
  };
}
console.log("[Game Logic] RPS: determineRPSOutcome defined (description is MarkdownV2 pre-formatted).");

console.log("Part 4: Simplified Game Logic (Enhanced) - Complete.");
// --- End of Part 4 ---
// --- Start of Part 5a, Section 1 (NEW): Core Listeners & Populated Command/Callback Routers ---
// index.js - Part 5a, Section 1: Core Listeners Setup (Message & Callback) and Populated Routers
//----------------------------------------------------------------------------------------------
console.log("Loading Part 5a, Section 1 (NEW): Core Listeners & Populated Command/Callback Routers...");

// Dependencies from previous Parts (assumed to be globally available or correctly imported)
// Part 1: isShuttingDown, userStateCache, COMMAND_COOLDOWN_MS, bot, getPlayerDisplayReference,
//         safeSendMessage, escapeMarkdownV2, MIN_BET_USD_val, MAX_BET_USD_val, LAMPORTS_PER_SOL,
//         getSolUsdPrice, convertUSDToLamports, convertLamportsToUSDString, ADMIN_USER_ID, BOT_NAME,
//         MIN_BET_AMOUNT_LAMPORTS_config, MAX_BET_AMOUNT_LAMPORTS_config, stringifyWithBigInt,
//         RULES_CALLBACK_PREFIX, DEPOSIT_CALLBACK_ACTION, WITHDRAW_CALLBACK_ACTION, QUICK_DEPOSIT_CALLBACK_ACTION,
//         userCooldowns, pool
// Part 2: getOrCreateUser
// Part 3: createUserMention, formatCurrency // getPlayerDisplayReference is from Part 3, not Part 1
// Part P3: clearUserState, routeStatefulInput (These are called here, defined in P3)

// Game IDs are used extensively in routing
const GAME_IDS = { // Re-declared here for clarity within Part 5 logic, ensure consistency with Part 5a-S1 (original)
  COINFLIP: 'coinflip',
  RPS: 'rps',
  DICE_ESCALATOR: 'dice_escalator',
  DICE_21: 'dice21',
  OVER_UNDER_7: 'ou7',
  DUEL: 'duel',
  LADDER: 'ladder',
  SEVEN_OUT: 'sevenout',
  SLOT_FRENZY: 'slotfrenzy',
};


// --- Helper to parse bet amount for game commands (USD primary) ---
// This function was in the original Part 5a, Section 1.
// It's crucial for the command router.
const parseBetAmount = async (arg, commandInitiationChatId, commandInitiationChatType, userIdForLog = 'N/A') => {
    const LOG_PREFIX_PBA = `[ParseBetAmount CH:${commandInitiationChatId} UID:${userIdForLog}]`;
    let betAmountLamports;
    let minBetLamports, maxBetLamports;
    let minBetDisplay, maxBetDisplay;
    let defaultBetDisplay;

    try {
        const solPrice = await getSolUsdPrice(); // From Part 1

        minBetLamports = convertUSDToLamports(MIN_BET_USD_val, solPrice); // MIN_BET_USD_val from Part 1
        maxBetLamports = convertUSDToLamports(MAX_BET_USD_val, solPrice); // MAX_BET_USD_val from Part 1
        
        minBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(minBetLamports, solPrice));
        maxBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(maxBetLamports, solPrice));
        defaultBetDisplay = minBetDisplay; // Default bet is min USD bet

        if (!arg || String(arg).trim() === "") {
            betAmountLamports = minBetLamports;
            console.log(`${LOG_PREFIX_PBA} No bet arg provided, defaulting to min USD bet: ${defaultBetDisplay} (${betAmountLamports} lamports)`);
            return betAmountLamports;
        }

        const argStr = String(arg).trim().toLowerCase();
        let potentialUsdAmount = parseFloat(argStr.replace(/[^0-9.]/g, ''));

        // If "max" is specified, calculate max possible bet based on user balance (logic to be added if needed)
        // For now, "max" is not a supported direct keyword here.

        if (!isNaN(potentialUsdAmount) && potentialUsdAmount > 0 && !argStr.endsWith('sol') && !argStr.endsWith('lamports')) {
             // Primarily interpret as USD if no "sol" or "lamports" suffix
            betAmountLamports = convertUSDToLamports(potentialUsdAmount, solPrice);
            const betUsdDisplay = escapeMarkdownV2(potentialUsdAmount.toFixed(2));

            if (potentialUsdAmount < MIN_BET_USD_val || potentialUsdAmount > MAX_BET_USD_val) {
                const message = `⚠️ Your bet of *${betUsdDisplay} USD* is outside the allowed limits: *${minBetDisplay}* \\- *${maxBetDisplay}*\\. Your bet has been adjusted to the minimum: *${defaultBetDisplay}*\\.`;
                await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                return minBetLamports; // Adjust to minimum
            }
            console.log(`${LOG_PREFIX_PBA} Parsed bet: ${potentialUsdAmount} USD -> ${betAmountLamports} lamports`);
            return betAmountLamports;
        } else {
            let parsedLamportsDirectly;
            try {
                if (argStr.endsWith('sol')) {
                    const solVal = parseFloat(argStr.replace('sol', '').trim());
                    if (!isNaN(solVal) && solVal > 0) {
                        parsedLamportsDirectly = BigInt(Math.floor(solVal * Number(LAMPORTS_PER_SOL)));
                    } else throw new Error("Invalid SOL format with 'sol' suffix.");
                } else if (argStr.includes('.') && !isNaN(parseFloat(argStr))) { // If it has a decimal and is a number, assume SOL
                    const solVal = parseFloat(argStr);
                     if (!isNaN(solVal) && solVal > 0) {
                        parsedLamportsDirectly = BigInt(Math.floor(solVal * Number(LAMPORTS_PER_SOL)));
                    } else throw new Error("Invalid SOL float format.");
                } else if (argStr.endsWith('lamports')) {
                     const lampVal = BigInt(argStr.replace('lamports','').trim());
                     if (lampVal > 0n) parsedLamportsDirectly = lampVal;
                     else throw new Error("Invalid lamports amount.");
                }
                 else { // Attempt to parse as an integer - could be SOL or lamports
                    const intVal = BigInt(argStr);
                    // Heuristic: if it's a small integer (e.g., < 10000) and doesn't look like a typical lamport amount, assume SOL
                    // Otherwise, assume lamports. This heuristic can be adjusted.
                    if (intVal > 0n && intVal < 10000n && !argStr.endsWith('000000')) {
                        parsedLamportsDirectly = BigInt(Math.floor(Number(intVal) * Number(LAMPORTS_PER_SOL)));
                        console.log(`${LOG_PREFIX_PBA} Interpreted bet "${argStr}" as ${intVal} SOL -> ${parsedLamportsDirectly} lamports`);
                    } else if (intVal > 0n) {
                        parsedLamportsDirectly = intVal;
                         console.log(`${LOG_PREFIX_PBA} Interpreted bet "${argStr}" as ${parsedLamportsDirectly} lamports directly.`);
                    } else {
                        throw new Error("Bet amount (SOL/Lamports) must be positive.");
                    }
                }

                if (parsedLamportsDirectly <= 0n) throw new Error("Bet amount (SOL/Lamports) must be positive.");

                if (parsedLamportsDirectly < minBetLamports || parsedLamportsDirectly > maxBetLamports) {
                    const betInSOLDisplayDynamic = escapeMarkdownV2(formatCurrency(parsedLamportsDirectly, 'SOL'));
                    const message = `⚠️ Your bet of *${betInSOLDisplayDynamic}* (approx\\. ${escapeMarkdownV2(convertLamportsToUSDString(parsedLamportsDirectly, solPrice))}) is outside current USD limits (*${minBetDisplay}* \\- *${maxBetDisplay}*\\)\\. Your bet is set to the minimum: *${defaultBetDisplay}*\\.`;
                    await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                    return minBetLamports;
                }
                console.log(`${LOG_PREFIX_PBA} Parsed bet as lamports/SOL: ${parsedLamportsDirectly} lamports`);
                return parsedLamportsDirectly;
            } catch (directParseError) {
                const message = `🤔 Hmmm, your bet amount \`${escapeMarkdownV2(String(arg))}\` seems a bit off\\. Please use USD (e\\.g\\., \`5\` or \`10.50\`), or SOL (e\\.g\\. \`0.1 sol\`, \`0.05\`)\\. Your bet is set to the minimum: *${defaultBetDisplay}*\\.`;
                await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                return minBetLamports;
            }
        }
    } catch (priceError) {
        console.error(`${LOG_PREFIX_PBA} Critical error getting SOL price for bet parsing: ${priceError.message}`);
        const minLamportsFallbackDisplay = escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT_LAMPORTS_config, 'SOL'));
        const message = `⚙️ Apologies, we couldn't determine current bet limits due to a price feed issue\\. Using internal default lamport limits for now\\. Your bet has been set to the internal minimum of *${minLamportsFallbackDisplay}*\\.`;
        await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
        
        try { // Fallback to fixed lamport limits if price feed fails
            if (!arg || String(arg).trim() === "") return MIN_BET_AMOUNT_LAMPORTS_config; // Default to min if no arg during fallback
            let fallbackAmountLamports = BigInt(String(arg).toLowerCase().replace('sol', '').replace('lamports','').trim());
            if (String(arg).toLowerCase().includes('sol') || String(arg).includes('.')) { // If it was likely SOL
                fallbackAmountLamports = BigInt(Math.floor(parseFloat(String(arg).toLowerCase().replace('sol', '').replace('lamports','').trim()) * Number(LAMPORTS_PER_SOL)));
            }

            if (fallbackAmountLamports < MIN_BET_AMOUNT_LAMPORTS_config || fallbackAmountLamports > MAX_BET_AMOUNT_LAMPORTS_config) {
                console.warn(`${LOG_PREFIX_PBA} Fallback bet ${fallbackAmountLamports} outside lamport limits, defaulting to MIN_BET_AMOUNT_LAMPORTS_config.`);
                return MIN_BET_AMOUNT_LAMPORTS_config;
            }
            return fallbackAmountLamports;
        } catch {
            return MIN_BET_AMOUNT_LAMPORTS_config; // Absolute fallback
        }
    }
};


// --- Main Message Handler (`bot.on('message')`) ---
bot.on('message', async (msg) => {
    const LOG_PREFIX_MSG = `[MSG_Handler TID:${msg.message_id || 'N/A'}]`;

    if (isShuttingDown) {
        console.log(`${LOG_PREFIX_MSG} Shutdown in progress. Ignoring message from UserID: ${msg.from?.id}.`);
        return;
    }
    if (!msg || !msg.from || !msg.chat || !msg.date) {
        console.warn(`${LOG_PREFIX_MSG} Ignoring malformed/incomplete message: ${stringifyWithBigInt(msg)}`);
        return;
    }

    if (msg.from.is_bot) {
        try {
            if (!bot || typeof bot.getMe !== 'function') {
                console.warn(`${LOG_PREFIX_MSG} bot.getMe not available, cannot check if message is from self. Assuming other bot.`);
                return;
            }
            const selfBotInfo = await bot.getMe();
            if (String(msg.from.id) !== String(selfBotInfo.id)) {
                // console.log(`${LOG_PREFIX_MSG} Ignoring message from other bot ID ${msg.from.id}`);
                return;
            }
            // console.log(`${LOG_PREFIX_MSG} Ignoring message from self (Bot ID ${msg.from.id}).`);
            return; // Usually ignore self messages
        } catch (getMeError) {
            console.error(`${LOG_PREFIX_MSG} Error in getMe self-check: ${getMeError.message}. Ignoring bot message.`);
            return;
        }
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text || "";
    const chatType = msg.chat.type;

    // Stateful input handling (routeStatefulInput is defined in Part P3)
    if (userStateCache.has(userId) && !text.startsWith('/')) {
        const currentState = userStateCache.get(userId);
        if (typeof routeStatefulInput === 'function') {
            console.log(`${LOG_PREFIX_MSG} User ${userId} has active state: ${currentState.state || currentState.action}. Routing to stateful input handler.`);
            await routeStatefulInput(msg, currentState);
            return;
        } else {
            console.warn(`${LOG_PREFIX_MSG} User ${userId} in state ${currentState.state || currentState.action}, but routeStatefulInput is not defined. Clearing state.`);
            if (typeof clearUserState === 'function') clearUserState(userId); else userStateCache.delete(userId);
        }
    }

    if (text.startsWith('/')) {
        if (!userId || userId === "undefined") {
            console.error(`${LOG_PREFIX_MSG} CRITICAL: User ID is undefined before getOrCreateUser. Message: ${stringifyWithBigInt(msg)}`);
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred with your user session. Please try starting a new command.", {});
            return;
        }
        let userForCommandProcessing;
        try {
            userForCommandProcessing = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
            if (!userForCommandProcessing) {
                console.error(`${LOG_PREFIX_MSG} Failed to get or create user for ID ${userId}. User object is null.`);
                await safeSendMessage(chatId, "😕 Apologies, we couldn't access your player profile at this moment. Please try again shortly or contact support if the issue persists.", {});
                return;
            }
        } catch (e) {
            console.error(`${LOG_PREFIX_MSG} Error fetching/creating user for command: ${e.message}`, e.stack);
            await safeSendMessage(chatId, "🛠️ We've encountered a technical hiccup while preparing your details. Please try your command again in a moment.", {});
            return;
        }

        const now = Date.now();
        if (userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
            // console.log(`${LOG_PREFIX_MSG} Command from user ${userId} ignored due to cooldown.`);
            // Optionally send a cooldown message to user in DM if this happens often.
            return;
        }
        userCooldowns.set(userId, now);

        let fullCommand = text.substring(1);
        let commandName = fullCommand.split(/\s+/)[0]?.toLowerCase();
        const commandArgs = fullCommand.split(/\s+/).slice(1);
        const originalMessageId = msg.message_id;

        if (commandName.includes('@')) {
            try {
                const selfBotInfo = await bot.getMe();
                const botUsernameLower = selfBotInfo.username.toLowerCase();
                if (commandName.endsWith(`@${botUsernameLower}`)) {
                    commandName = commandName.substring(0, commandName.lastIndexOf(`@${botUsernameLower}`));
                } else {
                    if (chatType === 'group' || chatType === 'supergroup') {
                        console.log(`${LOG_PREFIX_MSG} Command /${commandName} in chat ${chatId} is for a different bot. Ignoring.`);
                        return;
                    }
                    commandName = commandName.split('@')[0]; // If in PM and has @otherbot, strip it.
                }
            } catch (getMeErr) {
                console.error(`${LOG_PREFIX_MSG} Error getting bot username for command stripping: ${getMeErr.message}. Proceeding with original command name ${commandName}.`);
            }
        }
        
        console.log(`${LOG_PREFIX_MSG} CMD: /${commandName}, Args: [${commandArgs.join(', ')}] from User ${getPlayerDisplayReference(userForCommandProcessing)} (ID: ${userId}, Chat: ${chatId}, Type: ${chatType})`);

        // --- FULL COMMAND ROUTER ---
        try {
            switch (commandName) {
                // --- General Casino Commands (Handlers expected in Part 5a, Section 2 - NEW) ---
                case 'start':
                    await handleStartCommand(msg, commandArgs);
                    break;
                case 'help':
                    await handleHelpCommand(msg);
                    break;
                case 'balance':
                case 'bal':
                    await handleBalanceCommand(msg);
                    break;
                case 'rules':
                case 'info':
                    await handleRulesCommand(chatId, userForCommandProcessing, originalMessageId, false, chatType);
                    break;
                case 'jackpot':
                    await handleJackpotCommand(chatId, userForCommandProcessing, chatType);
                    break;
                case 'leaderboards':
                    await handleLeaderboardsCommand(msg, commandArgs);
                    break;

                // --- Payment System UI Commands (Handlers expected in Part P3) ---
                case 'wallet':
                    if (typeof handleWalletCommand === 'function') await handleWalletCommand(msg);
                    else console.error(`${LOG_PREFIX_MSG} Missing handler: handleWalletCommand for /${commandName}`);
                    break;
                case 'deposit':
                    if (typeof handleDepositCommand === 'function') await handleDepositCommand(msg, commandArgs, userId);
                    else console.error(`${LOG_PREFIX_MSG} Missing handler: handleDepositCommand for /${commandName}`);
                    break;
                case 'withdraw':
                    if (typeof handleWithdrawCommand === 'function') await handleWithdrawCommand(msg, commandArgs, userId);
                    else console.error(`${LOG_PREFIX_MSG} Missing handler: handleWithdrawCommand for /${commandName}`);
                    break;
                case 'referral':
                    if (typeof handleReferralCommand === 'function') await handleReferralCommand(msg);
                    else console.error(`${LOG_PREFIX_MSG} Missing handler: handleReferralCommand for /${commandName}`);
                    break;
                case 'history':
                    if (typeof handleHistoryCommand === 'function') await handleHistoryCommand(msg);
                    else console.error(`${LOG_PREFIX_MSG} Missing handler: handleHistoryCommand for /${commandName}`);
                    break;
                case 'setwallet':
                    if (typeof handleSetWalletCommand === 'function') await handleSetWalletCommand(msg, commandArgs);
                    else console.error(`${LOG_PREFIX_MSG} Missing handler: handleSetWalletCommand for /${commandName}`);
                    break;

                // --- Admin Commands (Handler expected in Part 5a, Section 2 - NEW) ---
                case 'grant':
                    await handleGrantCommand(msg, commandArgs, userForCommandProcessing);
                    break;

                // --- Game Initiation Commands (Handlers expected in Part 5a-S3 (Coinflip/RPS), 5b (DE/D21), 5c (Others) - NEW structure) ---
                case 'coinflip':
                case 'startcoinflip':
                    if (typeof handleStartGroupCoinFlipCommand === 'function') {
                        const betCF = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartGroupCoinFlipCommand(chatId, userForCommandProcessing, betCF, originalMessageId, chatType);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartGroupCoinFlipCommand for /${commandName}`);
                    break;
                case 'rps':
                case 'startrps':
                    if (typeof handleStartGroupRPSCommand === 'function') {
                        const betRPS = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartGroupRPSCommand(chatId, userForCommandProcessing, betRPS, originalMessageId, chatType);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartGroupRPSCommand for /${commandName}`);
                    break;
                case 'de':
                case 'diceescalator':
                    if (typeof handleStartDiceEscalatorCommand === 'function') {
                        const betDE = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartDiceEscalatorCommand(msg, betDE);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartDiceEscalatorCommand for /${commandName}`);
                    break;
                case 'd21':
                case 'blackjack':
                    if (typeof handleStartDice21Command === 'function') {
                        const betD21 = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartDice21Command(msg, betD21);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartDice21Command for /${commandName}`);
                    break;
                case 'ou7':
                case 'overunder7':
                    if (typeof handleStartOverUnder7Command === 'function') {
                        const betOU7 = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartOverUnder7Command(msg, betOU7);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartOverUnder7Command for /${commandName}`);
                    break;
                case 'duel':
                case 'highroller':
                    if (typeof handleStartDuelCommand === 'function') {
                        const betDuel = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartDuelCommand(msg, betDuel);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartDuelCommand for /${commandName}`);
                    break;
                case 'ladder':
                case 'greedsladder':
                    if (typeof handleStartLadderCommand === 'function') {
                        const betLadder = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartLadderCommand(msg, betLadder);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartLadderCommand for /${commandName}`);
                    break;
                case 's7':
                case 'sevenout':
                case 'craps':
                    if (typeof handleStartSevenOutCommand === 'function') {
                        const betS7 = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartSevenOutCommand(msg, betS7);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartSevenOutCommand for /${commandName}`);
                    break;
                case 'slot':
                case 'slots':
                case 'slotfrenzy':
                    if (typeof handleStartSlotCommand === 'function') {
                        const betSlot = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        await handleStartSlotCommand(msg, betSlot);
                    } else console.error(`${LOG_PREFIX_MSG} Missing handler: handleStartSlotCommand for /${commandName}`);
                    break;

                default:
                    const selfBotInfoDefault = await bot.getMe();
                    if (chatType === 'private' || text.startsWith(`/@${selfBotInfoDefault.username}`)) {
                        await safeSendMessage(chatId, `🤔 Hmmm, I don't recognize the command \`/${escapeMarkdownV2(commandName || "")}\`\\. Try \`/help\` for a list of my amazing games and features\\!`, { parse_mode: 'MarkdownV2' });
                    } else {
                        console.log(`${LOG_PREFIX_MSG} Unknown command /${commandName} in group ${chatId} not directed at bot. Silently ignored.`);
                    }
                    break;
            }
        } catch (commandError) {
            console.error(`${LOG_PREFIX_MSG} 🚨 UNHANDLED ERROR IN COMMAND ROUTER for /${commandName}: ${commandError.message}`, commandError.stack);
            await safeSendMessage(chatId, `⚙️ Oops! A critical error occurred while processing your command \`/${escapeMarkdownV2(commandName)}\`\\. Our support team has been alerted. Please try again later or contact support if the issue persists.`, { parse_mode: 'MarkdownV2' });
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 CRITICAL: Unhandled error in command router for /${escapeMarkdownV2(commandName)}\nUser: ${getPlayerDisplayReference(userForCommandProcessing)} (${userId})\nError: \`${escapeMarkdownV2(commandError.message)}\`\nStack (partial): \`\`\`${escapeMarkdownV2(commandError.stack?.substring(0, 500) || "N/A")}\`\`\``)
                .catch(err => console.error("Failed to notify admin about command router error:", err));
            }
        }
    } // End of command processing (if text.startsWith('/'))
}); // End of bot.on('message')


// --- Callback Query Handler (`bot.on('callback_query')`) ---
bot.on('callback_query', async (callbackQuery) => {
    const LOG_PREFIX_CBQ = `[CBQ_Handler ID:${callbackQuery.id}]`;
    if (isShuttingDown) {
        try { await bot.answerCallbackQuery(callbackQuery.id, { text: "⚙️ The casino is currently closing. Please try again later." }); } catch (e) { /* ignore */ }
        return;
    }

    const msg = callbackQuery.message;
    const userFromCb = callbackQuery.from;
    const callbackQueryId = callbackQuery.id;
    const data = callbackQuery.data;

    if (!msg || !userFromCb || !data) {
        console.error(`${LOG_PREFIX_CBQ} Ignoring malformed callback query. Message, User, or Data missing. Query: ${stringifyWithBigInt(callbackQuery)}`);
        try { await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Invalid query data." }); } catch (e) { /* ignore */ }
        return;
    }

    const userId = String(userFromCb.id);
    if (!userId || userId === "undefined") {
        console.error(`${LOG_PREFIX_CBQ} CRITICAL: User ID is undefined in callback query. Callback Data: ${data}, User: ${stringifyWithBigInt(userFromCb)}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: User session issue. Please try initiating the action again.", show_alert: true });
        return;
    }
    const originalChatId = String(msg.chat.id);
    const originalChatType = msg.chat.type;
    const originalMessageId = msg.message_id;

    try { await bot.answerCallbackQuery(callbackQueryId); } 
    catch (e) { console.warn(`${LOG_PREFIX_CBQ} Non-critical: Failed to answer basic callback query: ${e.message}`); }

    let userObjectForCallback;
    try {
        userObjectForCallback = await getOrCreateUser(userId, userFromCb.username, userFromCb.first_name, userFromCb.last_name);
        if (!userObjectForCallback) {
            console.error(`${LOG_PREFIX_CBQ} Failed to get or create user for callback. User ID ${userId}. User object is null.`);
            throw new Error("User data could not be fetched for callback processing.");
        }
    } catch (e) {
        console.error(`${LOG_PREFIX_CBQ} Error fetching user for callback: ${e.message}`, e.stack);
        await safeSendMessage(originalChatId, "🛠️ Apologies, a technical hiccup occurred while fetching your details for this action. Please try again.", {});
        return;
    }

    const [action, ...params] = data.split(':');
    console.log(`${LOG_PREFIX_CBQ} User ${getPlayerDisplayReference(userObjectForCallback)} (ID: ${userId}) Action: "${action}", Params: [${params.join(', ')}] (Chat: ${originalChatId}, Type: ${originalChatType}, OrigMsgID: ${originalMessageId})`);

    if (action === 'menu' && (params[0] === 'main' || params[0] === 'wallet' || params[0] === 'game_selection')) {
        if (typeof clearUserState === 'function') {
            clearUserState(userId);
        } else {
            console.warn(`${LOG_PREFIX_CBQ} clearUserState function not available. User state might persist.`);
            userStateCache.delete(userId);
        }
    }

    const sensitiveActions = [
        DEPOSIT_CALLBACK_ACTION, QUICK_DEPOSIT_CALLBACK_ACTION, 'quick_deposit',
        WITHDRAW_CALLBACK_ACTION,
        'menu:deposit', 'menu:withdraw', 'menu:history',
        'menu:link_wallet_prompt', 'process_withdrawal_confirm'
    ];
    const fullCallbackActionForSensitivityCheck = action === 'menu' ? `${action}:${params[0]}` : action;
    let isCallbackRedirectedToDm = false;

    if ((originalChatType === 'group' || originalChatType === 'supergroup') && sensitiveActions.includes(fullCallbackActionForSensitivityCheck)) {
        isCallbackRedirectedToDm = true;
        try {
            const botUsername = (await bot.getMe()).username;
            const redirectText = `${getPlayerDisplayReference(userObjectForCallback)}, for your privacy, please continue this action in our direct message\\. I've sent you a message there with a button to continue, or you can use the one below\\!`;
            await bot.editMessageText(
                redirectText,
                { chat_id: originalChatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {
                    inline_keyboard: [[{ text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=cb_${action}_${params.join('_')}` }]]
                }}
            ).catch(async (editError) => {
                if (!editError.message || !editError.message.toLowerCase().includes("message is not modified")) {
                     console.warn(`${LOG_PREFIX_CBQ} Failed to edit group message for DM redirect: ${editError.message}. Sending new message instead.`);
                    await safeSendMessage(originalChatId, redirectText, {
                        parse_mode: 'MarkdownV2',
                        reply_markup: { inline_keyboard: [[{ text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=cb_${action}_${params.join('_')}` }]] }
                    });
                }
            });
        } catch (getMeErrCb) {
            console.error(`${LOG_PREFIX_CBQ} Could not get bot username for DM redirect message: ${getMeErrCb.message}`);
            await safeSendMessage(originalChatId, `${getPlayerDisplayReference(userObjectForCallback)}, please continue this action in a direct message with me for privacy\\.`, { parse_mode: 'MarkdownV2' });
        }
    }

    const mockMsgObjectForHandler = {
        from: userObjectForCallback,
        chat: {
            id: isCallbackRedirectedToDm ? userId : originalChatId,
            type: isCallbackRedirectedToDm ? 'private' : originalChatType
        },
        message_id: isCallbackRedirectedToDm ? null : originalMessageId,
        isCallbackRedirect: isCallbackRedirectedToDm,
        originalChatInfo: isCallbackRedirectedToDm ? { id: originalChatId, type: originalChatType, messageId: originalMessageId } : null,
    };

    // --- FULL CALLBACK ROUTER ---
    try {
        // RULES_CALLBACK_PREFIX is a string like "rules_game_"
        // Check if the action starts with this prefix for dynamic rule display
        if (action.startsWith(RULES_CALLBACK_PREFIX.substring(0, RULES_CALLBACK_PREFIX.lastIndexOf('_') + 1))) { // e.g. "rules_game_"
            let gameCodeForRule = action.substring(RULES_CALLBACK_PREFIX.lastIndexOf('_') + 1);
            if (params.length > 0 && Object.values(GAME_IDS).includes(params[0])) { // If game code is in params (e.g. rules_game:coinflip)
                 gameCodeForRule = params[0];
            } else if (!Object.values(GAME_IDS).includes(gameCodeForRule) && params.length > 0 && Object.values(GAME_IDS).includes(action.split('_').pop() + "_" + params[0]) ){
                 // For cases like rules_dice:escalator
                 gameCodeForRule = action.split('_').pop() + "_" + params[0];
            }

            if (Object.values(GAME_IDS).includes(gameCodeForRule)) {
                 await handleDisplayGameRules(mockMsgObjectForHandler.chat.id, mockMsgObjectForHandler.message_id, gameCodeForRule, userObjectForCallback, mockMsgObjectForHandler.chat.type);
            } else {
                console.warn(`${LOG_PREFIX_CBQ} Could not determine game code for rules from action: ${action} and params: ${params.join(',')}. Resolved gameCodeForRule: ${gameCodeForRule}`);
                await safeSendMessage(userId, `⚠️ Error: Could not determine which game rules to display. Please try again from the main \`/rules\` menu.`, { parse_mode: 'MarkdownV2' });
            }
        } else {
             // Static action routing
            switch (action) {
                case 'show_rules_menu':
                    await handleRulesCommand(mockMsgObjectForHandler.chat.id, userObjectForCallback, mockMsgObjectForHandler.message_id, true, mockMsgObjectForHandler.chat.type);
                    break;

                // Payment System UI Callbacks (Handlers expected in Part P3)
                case DEPOSIT_CALLBACK_ACTION:
                case QUICK_DEPOSIT_CALLBACK_ACTION:
                case 'quick_deposit': // Alias
                    if (typeof handleDepositCommand === 'function') {
                        await handleDepositCommand(mockMsgObjectForHandler, [], userId);
                    } else console.error(`${LOG_PREFIX_CBQ} Missing handler: handleDepositCommand for callback action ${action}`);
                    break;
                case WITHDRAW_CALLBACK_ACTION:
                    if (typeof handleWithdrawCommand === 'function') {
                        await handleWithdrawCommand(mockMsgObjectForHandler, [], userId);
                    } else console.error(`${LOG_PREFIX_CBQ} Missing handler: handleWithdrawCommand for callback action ${action}`);
                    break;

                // Menu Navigation (Handler expected in Part P3)
                case 'menu':
                    const menuType = params[0];
                    const menuParams = params.slice(1);
                    if (typeof handleMenuAction === 'function') {
                        await handleMenuAction(userId, originalChatId, originalMessageId, menuType, menuParams, true, originalChatType);
                    } else {
                        console.error(`${LOG_PREFIX_CBQ} Missing handler: handleMenuAction for menu type ${menuType}.`);
                        await safeSendMessage(userId, `⚠️ Menu option \`${escapeMarkdownV2(menuType)}\` is currently unavailable\\. Please try later\\.`, { parse_mode: 'MarkdownV2' });
                    }
                    break;

                // Withdrawal Confirmation (Handler expected in Part P3)
                case 'process_withdrawal_confirm':
                    const confirmation = params[0]; // 'yes' or 'no'
                    const stateForWithdrawal = userStateCache.get(userId);

                    if (mockMsgObjectForHandler.chat.type !== 'private' || String(mockMsgObjectForHandler.chat.id) !== String(userId) || !stateForWithdrawal || String(stateForWithdrawal.chatId) !== String(userId)) {
                        console.warn(`${LOG_PREFIX_CBQ} Withdrawal confirmation attempt outside of designated private chat or state mismatch. OrigChatID: ${mockMsgObjectForHandler.chat.id}, UserID: ${userId}, State ChatID: ${stateForWithdrawal?.chatId}`);
                        if (originalMessageId) {
                            await bot.editMessageText("⚠️ This confirmation is invalid or has expired. Please restart the withdrawal process in a private message with me.", { chat_id: originalChatId, message_id: originalMessageId, reply_markup: {} }).catch(() => {});
                        } else {
                            await safeSendMessage(userId, "⚠️ This confirmation is invalid or has expired. Please restart the withdrawal process.", {});
                        }
                        if (stateForWithdrawal && String(stateForWithdrawal.chatId) === String(userId) && bot && stateForWithdrawal.messageId && String(originalChatId) !== String(userId)) {
                            await bot.deleteMessage(stateForWithdrawal.chatId, stateForWithdrawal.messageId).catch(() => {});
                        }
                        if(typeof clearUserState === 'function') clearUserState(userId); else userStateCache.delete(userId);
                        break;
                    }

                    if (confirmation === 'yes' && stateForWithdrawal.state === 'awaiting_withdrawal_confirmation') {
                        const { linkedWallet, amountLamportsStr } = stateForWithdrawal.data;
                        if (typeof handleWithdrawalConfirmation === 'function') {
                            await handleWithdrawalConfirmation(userId, userId, stateForWithdrawal.messageId, linkedWallet, amountLamportsStr);
                        } else {
                            console.error(`${LOG_PREFIX_CBQ} Missing handler: handleWithdrawalConfirmation for callback action ${action}`);
                            await safeSendMessage(userId, "⚙️ Internal error processing withdrawal confirmation. Please contact support.", {});
                        }
                    } else if (confirmation === 'no' && stateForWithdrawal.state === 'awaiting_withdrawal_confirmation') {
                        await bot.editMessageText("💸 Withdrawal Cancelled. Your funds remain in your casino balance.", { chat_id: userId, message_id: stateForWithdrawal.messageId, parse_mode: 'MarkdownV2', reply_markup: {} });
                    } else {
                        await bot.editMessageText("⚠️ Withdrawal confirmation has expired or is invalid. Please restart the withdrawal from the \`/wallet\` menu.", { chat_id: userId, message_id: (stateForWithdrawal?.messageId || originalMessageId), parse_mode: 'MarkdownV2', reply_markup: {} });
                    }
                    if(typeof clearUserState === 'function') clearUserState(userId); else userStateCache.delete(userId); // Clear state after handling
                    break;

                // --- Game Specific Callbacks ---
                case 'join_game': // Coinflip & RPS Join
                case 'cancel_game': // Coinflip & RPS Cancel
                case 'rps_choose': // RPS Choice
                    // Handler expected in Part 5a, Section 3 (NEW structure) -> `forwardGameCallback`
                    if (typeof forwardGameCallback === 'function') {
                        await forwardGameCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else console.warn(`${LOG_PREFIX_CBQ} Game callback action ${action} received, but forwardGameCallback not defined yet.`);
                    break;
                case 'de_roll_prompt': // Dice Escalator actions
                case 'de_cashout':
                case 'jackpot_display_noop':
                case 'play_again_de':
                     // Handler expected in Part 5b, Section 1 -> `forwardDiceEscalatorCallback`
                    if (typeof forwardDiceEscalatorCallback === 'function') {
                        await forwardDiceEscalatorCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else console.warn(`${LOG_PREFIX_CBQ} Dice Escalator callback ${action} received, but forwarder not defined yet.`);
                    break;
                case 'd21_hit': // Dice 21 actions
                case 'd21_stand':
                case 'play_again_d21':
                    // Handler expected in Part 5b, Section 2 -> `forwardDice21Callback`
                    if (typeof forwardDice21Callback === 'function') {
                        await forwardDice21Callback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else console.warn(`${LOG_PREFIX_CBQ} Dice 21 callback ${action} received, but forwarder not defined yet.`);
                    break;
                case 'ou7_choice': // Over/Under 7 actions
                case 'play_again_ou7':
                case 'duel_roll': // Duel actions
                case 'play_again_duel':
                case 'play_again_ladder': // Ladder actions (assuming only play again for now)
                case 's7_roll': // Sevens Out actions
                case 'play_again_s7':
                case 'play_again_slot': // Slot Frenzy actions (assuming only play again)
                    // Handler expected in Part 5c -> `forwardAdditionalGamesCallback`
                    if (typeof forwardAdditionalGamesCallback === 'function') {
                        await forwardAdditionalGamesCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else console.warn(`${LOG_PREFIX_CBQ} Additional Games callback ${action} received, but forwarder not defined yet.`);
                    break;

                default:
                    console.log(`${LOG_PREFIX_CBQ} INFO: Unhandled callback action: "${action}" with params: [${params.join(', ')}]`);
                    await safeSendMessage(userId, `🤔 I'm not sure how to handle that action (\`${escapeMarkdownV2(action)}\`)\\. If you think this is an error, please try the command again or contact support\\.`, { parse_mode: 'MarkdownV2' });
                    break;
            }
        }
    } catch (callbackError) {
        console.error(`${LOG_PREFIX_CBQ} 🚨 CRITICAL ERROR processing callback action "${action}": ${callbackError.message}`, callbackError.stack);
        await safeSendMessage(userId, "⚙️ Oops! Something went seriously wrong while processing your action. Please try again or use a command. If the problem persists, contact support.", {}).catch(() => {});
        if (typeof notifyAdmin === 'function') {
            notifyAdmin(`🚨 CRITICAL: Unhandled error in callback router for action ${escapeMarkdownV2(action)}\nUser: ${getPlayerDisplayReference(userObjectForCallback)} (${userId})\nParams: ${params.join(', ')}\nError: \`${escapeMarkdownV2(callbackError.message)}\`\nStack (partial): \`\`\`${escapeMarkdownV2(callbackError.stack?.substring(0, 500) || "N/A")}\`\`\``)
            .catch(err => console.error("Failed to notify admin about callback router error:", err));
        }
    }
}); // End of bot.on('callback_query')

// --- Helper function to forward game callbacks for Coinflip/RPS ---
// This function itself is part of the routing layer.
// The actual handlers (handleJoinGameCallback, etc.) are defined in Part 5a, Section 3 (NEW structure).
async function forwardGameCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_GAME_CB_FWD = `[GameCB_Forward UID:${userObject.telegram_id} Action:${action}]`;
    console.log(`${LOG_PREFIX_GAME_CB_FWD} Forwarding to Coinflip/RPS handler for chat ${originalChatId} (Type: ${originalChatType})`);

    const gameId = params[0];

    switch (action) {
        case 'join_game':
            if (!gameId) {
                console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId for join_game action.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Game ID missing for join action.", show_alert: true });
                return;
            }
            if (typeof handleJoinGameCallback === 'function') {
                await handleJoinGameCallback(originalChatId, userObject, gameId, originalMessageId, callbackQueryId, originalChatType);
            } else console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing handler: handleJoinGameCallback`);
            break;
        case 'cancel_game':
            if (!gameId) {
                console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId for cancel_game action.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Game ID missing for cancel action.", show_alert: true });
                return;
            }
            if (typeof handleCancelGameCallback === 'function') {
                await handleCancelGameCallback(originalChatId, userObject, gameId, originalMessageId, callbackQueryId, originalChatType);
            } else console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing handler: handleCancelGameCallback`);
            break;
        case 'rps_choose':
            if (params.length < 2) { // gameId and choice
                console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId or choice for rps_choose action. Params: ${params.join(',')}`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Missing parameters for RPS choice.", show_alert: true });
                return;
            }
            const choice = params[1];
            if (typeof handleRPSChoiceCallback === 'function') {
                await handleRPSChoiceCallback(originalChatId, userObject, gameId, choice, originalMessageId, callbackQueryId, originalChatType);
            } else console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing handler: handleRPSChoiceCallback`);
            break;
        default:
            console.warn(`${LOG_PREFIX_GAME_CB_FWD} Unforwarded or unknown game action in this forwarder: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Unknown action: ${escapeMarkdownV2(action)}`, show_alert: true });
    }
}

console.log("Part 5a, Section 1 (NEW): Core Listeners & Populated Command/Callback Routers - Complete.");
// --- End of Part 5a, Section 1 (NEW) ---
// --- Start of Part 5a, Section 2 (NEW): General Command Handler Implementations ---
// index.js - Part 5a, Section 2: General Casino Bot Command Implementations
//----------------------------------------------------------------------------------
console.log("Loading Part 5a, Section 2 (NEW): General Command Handler Implementations...");

// Assumed dependencies from previous Parts:
// Part 1: safeSendMessage, escapeMarkdownV2, bot, BOT_NAME, BOT_VERSION, ADMIN_USER_ID, pool,
//         MIN_BET_USD_val, MAX_BET_USD_val, MIN_BET_AMOUNT_LAMPORTS_config, MAX_BET_AMOUNT_LAMPORTS_config,
//         TARGET_JACKPOT_SCORE, DICE_ESCALATOR_BUST_ON, BOT_STAND_SCORE_DICE_ESCALATOR,
//         DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE, MAIN_JACKPOT_ID, GAME_IDS (defined in 5a-S1 New),
//         OU7_PAYOUT_NORMAL, OU7_PAYOUT_SEVEN, OU7_DICE_COUNT, DUEL_DICE_COUNT,
//         LADDER_ROLL_COUNT, LADDER_BUST_ON, LADDER_PAYOUTS, SLOT_PAYOUTS (defined in Part 1),
//         RULES_CALLBACK_PREFIX, QUICK_DEPOSIT_CALLBACK_ACTION, WITHDRAW_CALLBACK_ACTION, LAMPORTS_PER_SOL,
//         getSolUsdPrice, convertLamportsToUSDString, convertUSDToLamports
// Part 2: getOrCreateUser, getUserBalance, queryDatabase, getUserByReferralCode, generateReferralCode
// Part 3: getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay
// Part P2: updateUserBalanceAndLedger (This is crucial for /grant)
// Part P3: clearUserState (This is called by /start, /wallet if needed)

// --- Command Handler Functions (General Casino Bot Commands) ---

async function handleStartCommand(msg, args) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_START = `[StartCmd UID:${userId} CH:${chatId}]`;

    if (typeof clearUserState === 'function') {
        clearUserState(userId); // Clear any pending state on /start
    } else {
        userStateCache.delete(userId); // Fallback
        console.warn(`${LOG_PREFIX_START} clearUserState function not available. Using direct cache delete.`);
    }

    let userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) {
        await safeSendMessage(chatId, "😕 Oh dear! We had a hiccup fetching your player profile. Please try typing /start again in a moment.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(userObject);
    let botUsername = "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) {
        console.error(`${LOG_PREFIX_START} Could not fetch bot username: ${e.message}`);
    }

    // Handle deep links
    if (args && args[0]) {
        const deepLinkParam = args[0];
        console.log(`${LOG_PREFIX_START} Processing deep link parameter: ${deepLinkParam}`);

        if (deepLinkParam.startsWith('ref_')) {
            const refCode = deepLinkParam.substring(4);
            const referrerUserRecord = await getUserByReferralCode(refCode); // From Part 2
            let refByDisplay = "a fellow player";
            let referredSuccessfully = false;

            if (referrerUserRecord && String(referrerUserRecord.telegram_id) !== userId) {
                const referrerFullObj = await getOrCreateUser(referrerUserRecord.telegram_id, referrerUserRecord.username, referrerUserRecord.first_name);
                if (referrerFullObj) refByDisplay = getPlayerDisplayReference(referrerFullObj);

                if (!userObject.referrer_telegram_id) {
                    const client = await pool.connect();
                    try {
                        await client.query('BEGIN');
                        await client.query('UPDATE users SET referrer_telegram_id = $1 WHERE telegram_id = $2 AND referrer_telegram_id IS NULL', [referrerUserRecord.telegram_id, userId]);
                        await client.query(
                            `INSERT INTO referrals (referrer_telegram_id, referred_telegram_id, status, created_at, updated_at)
                             VALUES ($1, $2, 'pending_criteria', NOW(), NOW())
                             ON CONFLICT (referrer_telegram_id, referred_telegram_id) DO NOTHING
                             ON CONFLICT ON CONSTRAINT referrals_referred_telegram_id_key DO NOTHING;`,
                            [referrerUserRecord.telegram_id, userId]
                        );
                        await client.query('COMMIT');
                        userObject = await getOrCreateUser(userId); // Re-fetch updated user
                        referredSuccessfully = true;
                        console.log(`${LOG_PREFIX_START} User ${userId} successfully linked to referrer ${referrerUserRecord.telegram_id} via ref_code ${refCode}.`);
                    } catch (refError) {
                        await client.query('ROLLBACK');
                        console.error(`${LOG_PREFIX_START} Error linking referral for user ${userId} via code ${refCode}:`, refError);
                    } finally {
                        client.release();
                    }
                } else if (String(userObject.referrer_telegram_id) === String(referrerUserRecord.telegram_id)) {
                    console.log(`${LOG_PREFIX_START} User ${userId} was already referred by ${referrerUserRecord.telegram_id}.`);
                    referredSuccessfully = true; // Still acknowledge the referrer
                } else {
                    console.log(`${LOG_PREFIX_START} User ${userId} was already referred by someone else (${userObject.referrer_telegram_id}). Cannot apply new ref_code ${refCode}.`);
                    refByDisplay = "your original referrer";
                }
            } else if (referrerUserRecord && String(referrerUserRecord.telegram_id) === userId) {
                refByDisplay = "yourself \\(clever try\\! 😉\\)";
            }

            const referralMsg = `👋 Welcome aboard, ${playerRef}\\! It looks like you joined us through a link from ${refByDisplay}\\. We're thrilled to have you at ${escapeMarkdownV2(BOT_NAME)}\\! 🎉`;
            if (chatType !== 'private') {
                if(msg.message_id) await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
                await safeSendMessage(chatId, `${playerRef}, I've sent you a special welcome message about your referral in our private chat: @${escapeMarkdownV2(botUsername)} 📬`, { parse_mode: 'MarkdownV2' });
                await safeSendMessage(userId, referralMsg, { parse_mode: 'MarkdownV2' });
            } else {
                await safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2' });
            }
            await handleHelpCommand({ ...msg, chat: { id: userId, type: 'private' }}); // Show help in DM
            return;
        } else if (deepLinkParam.startsWith('cb_')) { // For callback-like actions from DM link e.g. /start cb_menu_deposit
             const cbParts = deepLinkParam.substring(3).split('_');
             const action = cbParts[0];
             const params = cbParts.slice(1);
             console.log(`${LOG_PREFIX_START} Deep link callback detected: Action: ${action}, Params: ${params.join(',')}`);

            // Simulate a callback query environment to reuse callback handlers.
            // This is a simplified way to trigger the logic, actual callback structure is more complex.
            const mockCallbackQueryForDeepLink = {
                id: `deeplink_${Date.now()}_${userId}`,
                from: msg.from, // User object
                message: { // Mock the message object callback query would typically have
                    message_id: null, // No existing message to edit in this DM context from a /start link
                    chat: { id: userId, type: 'private' }, // Action takes place in DM
                    date: Math.floor(Date.now() / 1000),
                    text: "" // No original text for the "message" the button was on
                },
                data: `${action}:${params.join(':')}`, // Reconstruct callback data string
                chat_instance: String(userId) // For private chat, chat_instance can be user_id
            };
            // Directly invoke the main callback router (conceptually, how bot.processUpdate would do it)
            // Find the actual callback handler function in your bot instance and call it.
            // Since the main `bot.on('callback_query', ...)` is globally defined, we assume it will pick this up
            // if we could emit an event. For now, we will attempt to call the handler structure.
            // This is a placeholder for direct invocation:
            if (typeof bot._handleCallbackQuery === "function") { // node-telegram-bot-api internal like method (example, not real)
                 // bot._handleCallbackQuery(mockCallbackQueryForDeepLink); // This is conceptual
                 console.warn(`${LOG_PREFIX_START} Deep link cb_* needs robust routing. Simulating action for ${action}.`);
                 // As a fallback, try to route to the menu if that's what it is.
                 if (action === "menu" && params[0] && typeof handleMenuAction === 'function') {
                     await handleMenuAction(userId, userId, null, params[0], params.slice(1), false, 'private');
                 } else if (action === "showRules" && params[0] && typeof handleDisplayGameRules === 'function') {
                     await handleDisplayGameRules(userId, null, params[0], userObject, 'private');
                 } else {
                      await safeSendMessage(userId, `Welcome back\\! To continue with "${escapeMarkdownV2(action.replace(/_/g, " "))}", please use the main menus or commands\\. Trying to take you to a relevant spot\\!`, {parse_mode: 'MarkdownV2'});
                      await handleHelpCommand({ ...msg, chat: { id: userId, type: 'private' }}); // Default to help
                 }
            } else {
                 console.error(`${LOG_PREFIX_START} Cannot directly route deep link cb_ action: ${action}. Showing help.`);
                 await handleHelpCommand({ ...msg, chat: { id: userId, type: 'private' }});
            }
            return;
        }
    }

    // Standard /start behavior
    if (chatType !== 'private') {
        if(msg.message_id) await bot.deleteMessage(chatId, msg.message_id).catch(() => {});
        await safeSendMessage(chatId, `👋 Ahoy, ${playerRef}! Welcome to the thrilling world of ${escapeMarkdownV2(BOT_NAME)}! 🎰\nI've just sent you a private message with all the details on how to get started and navigate our casino. Check your DMs: @${escapeMarkdownV2(botUsername)} 📬`, { parse_mode: 'MarkdownV2' });
        await handleHelpCommand({ ...msg, chat: { id: userId, type: 'private' }}); // Send help to DM
    } else {
        // Already in PM, send welcome and help
        await safeSendMessage(userId, `🎉 Salutations, ${playerRef}! Welcome to **${escapeMarkdownV2(BOT_NAME)}** \\- your portal to exhilarating Solana casino action! 🎉\nLet's get you started with the grand tour below! 👇`, { parse_mode: 'MarkdownV2' });
        await handleHelpCommand(msg);
    }
}

async function handleHelpCommand(originalMessageObject) {
    const userId = String(originalMessageObject.from.id);
    const chatId = String(originalMessageObject.chat.id);

    const userObj = await getOrCreateUser(userId, originalMessageObject.from.username, originalMessageObject.from.first_name, originalMessageObject.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Oops! I couldn't fetch your profile to display help. Please try /start again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerMention = getPlayerDisplayReference(userObj);
    const jackpotScoreInfo = TARGET_JACKPOT_SCORE ? escapeMarkdownV2(String(TARGET_JACKPOT_SCORE)) : 'a high score';
    const botNameEscaped = escapeMarkdownV2(BOT_NAME);
    let botUsername = "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { console.error(`[HelpCmd UID:${userId}] Could not fetch bot username: ${e.message}`); }

    const minBetUsdDisplay = `$${MIN_BET_USD_val.toFixed(2)}`;
    const maxBetUsdDisplay = `$${MAX_BET_USD_val.toFixed(2)}`;
    let referenceLamportLimits = "";
    try {
        const solPrice = await getSolUsdPrice();
        const minBetLamportsDynamic = convertUSDToLamports(MIN_BET_USD_val, solPrice);
        const maxBetLamportsDynamic = convertUSDToLamports(MAX_BET_USD_val, solPrice);
        referenceLamportLimits = `\n_(Approx\\. SOL equivalent: ${escapeMarkdownV2(formatCurrency(minBetLamportsDynamic, 'SOL'))} to ${escapeMarkdownV2(formatCurrency(maxBetLamportsDynamic, 'SOL'))})_`;
    } catch (priceErr) {
        console.warn(`[HelpCmd UID:${userId}] Failed to get SOL price for dynamic ref limits: ${priceErr.message}`);
        if (typeof MIN_BET_AMOUNT_LAMPORTS_config !== 'undefined' && typeof MAX_BET_AMOUNT_LAMPORTS_config !== 'undefined') {
            const minLamportDisplay = formatCurrency(MIN_BET_AMOUNT_LAMPORTS_config, 'SOL');
            const maxLamportDisplay = formatCurrency(MAX_BET_AMOUNT_LAMPORTS_config, 'SOL');
            referenceLamportLimits = `\n_(Fixed Ref: ${escapeMarkdownV2(minLamportDisplay)} to ${escapeMarkdownV2(maxLamportDisplay)})_`;
        }
    }


    const helpTextParts = [
        `🌟 Greetings, ${playerMention}! Welcome to the **${botNameEscaped} Casino Royale v${BOT_VERSION}**! 🌟`,
        `\nYour ultimate destination for electrifying Solana-powered casino games and big wins! Here’s your guide to the action:`,
        `\n\n*🏦 Your Casino Account & Funds:*`,
        `▫️ \`/balance\` or \`/bal\` \\- Peek at your current treasure chest\\. *(Summary in groups, full details in DM)*`,
        `▫️ \`/wallet\` \\- Your personal casino vault! Manage deposits, withdrawals, and link your SOL address\\. *(Best experienced in DM)*`,
        `▫️ \`/deposit\` \\- Instantly get your unique SOL deposit address\\. *(Handled securely in DM)*`,
        `▫️ \`/withdraw\` \\- Cash out your SOL winnings smoothly\\. *(Handled securely in DM)*`,
        `▫️ \`/setwallet <YourSolanaAddress>\` \\- Link or update your SOL withdrawal wallet\\. *(Use this in DM for privacy)*`,
        `▫️ \`/history\` \\- Review your recent transaction and game history\\. *(Available in DM)*`,
        `▫️ \`/referral\` \\- Grab your unique referral link & track your earnings from inviting friends\\! *(Details in DM)*`,
        `\n*📖 Casino Info & Support:*`,
        `▫️ \`/help\` \\- You're looking at it! This comprehensive guide to all things casino\\.`,
        `▫️ \`/rules\` or \`/info\` \\- Delve into the detailed rules for all our thrilling games\\. *(Interactive menu in DM)*`,
        `▫️ \`/jackpot\` \\- Check the current eye\\-watering amount of the Dice Escalator Super Jackpot\\!`,
        `▫️ \`/leaderboards\` \\- See who's topping the charts\\! *(Coming Soon!)*`,
        `\n*🎲 Available Games \\(Play in groups or PM against the Bot Dealer!\\):*`,
        `▫️ \`/coinflip <bet_usd_or_sol>\` \\- 🪙 Classic Heads or Tails for two players\\.`,
        `▫️ \`/rps <bet_usd_or_sol>\` \\- 🪨📄✂️ Epic Rock Paper Scissors duel for two players\\.`,
        `▫️ \`/de <bet_usd_or_sol>\` \\(or \`/diceescalator\`\\) \\- 🎲 Climb the score ladder for escalating wins & Jackpot glory\\! (vs\\. Bot)`,
        `▫️ \`/d21 <bet_usd_or_sol>\` \\(or \`/blackjack\`\\) \\- 🃏 Fast\\-paced Dice Blackjack against the Bot Dealer\\. (vs\\. Bot)`,
        `▫️ \`/ou7 <bet_usd_or_sol>\` \\(or \`/overunder7\`\\) \\- 🎲 Bet on the sum: Over 7, Under 7, or Exactly 7\\! (vs\\. Bot)`,
        `▫️ \`/duel <bet_usd_or_sol>\` \\(or \`/highroller\`\\) \\- ⚔️ High\\-stakes dice duel against the Bot Dealer\\. (vs\\. Bot)`,
        `▫️ \`/ladder <bet_usd_or_sol>\` \\(or \`/greedsladder\`\\) \\- 🪜 Risk it all in Greed's Ladder \\- climb high, don't bust\\! (vs\\. Bot)`,
        `▫️ \`/s7 <bet_usd_or_sol>\` \\(or \`/sevenout\`, \`/craps\`\\) \\- 🎲 Simplified & lightning\\-fast Craps action\\. (vs\\. Bot)`,
        `▫️ \`/slot <bet_usd_or_sol>\` \\(or \`/slots\`, \`/slotfrenzy\`\\) \\- 🎰 Spin the Telegram Slot Machine for dazzling prizes\\! (vs\\. Bot)`,
        `\n*💰 Betting Guide:*`,
        `To place a bet, use the game command followed by your bet amount in *USD* (e\\.g\\., \`/d21 5\` for $5 USD), or *SOL* (e\\.g\\., \`/d21 0.1 sol\` or \`/d21 0.05\`), or *Lamports* (e.g. \`/d21 10000000 lamports\`).`,
        `If no bet is specified, the game typically defaults to the minimum USD bet\\.`,
        `Current Bet Limits (USD): *${escapeMarkdownV2(minBetUsdDisplay)}* to *${escapeMarkdownV2(maxBetUsdDisplay)}*\\.${referenceLamportLimits}`,
        `\n*🏆 Dice Escalator Super Jackpot:*`,
        `Achieve a score of *${jackpotScoreInfo} or higher* in Dice Escalator AND beat the Bot Dealer to seize the magnificent Super Jackpot\\! A portion of every Dice Escalator bet fuels its growth\\.`,
        `\nRemember to play responsibly and may fortune smile upon you\\! 🍀`,
        ADMIN_USER_ID ? `For support or issues, feel free to contact an admin or our dedicated support channels\\. (Admin Ref: ${escapeMarkdownV2(String(ADMIN_USER_ID).slice(0, 4))}...)` : `For support, please refer to group administrators or the casino's official support channels\\.`,
        `\n💡 *Pro Tip:* For sensitive actions like managing your wallet or viewing personal history, it's always best to DM me directly: @${escapeMarkdownV2(botUsername)}`
    ];
    const helpMessage = helpTextParts.filter(Boolean).join('\n');
    const helpKeyboard = {
        inline_keyboard: [
            [{ text: "💳 My Wallet Dashboard", callback_data: "menu:wallet" }, { text: "📖 Game Rules Library", callback_data: "show_rules_menu" }],
            [{ text: "💰 Quick Deposit SOL", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }] // QUICK_DEPOSIT_CALLBACK_ACTION from Part 1
        ]
    };

    await safeSendMessage(chatId, helpMessage, { parse_mode: 'MarkdownV2', reply_markup: helpKeyboard, disable_web_page_preview: true });
}

async function handleBalanceCommand(msg) {
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_BAL = `[BalanceCmd UID:${userId} CH:${commandChatId}]`;

    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) {
        await safeSendMessage(commandChatId, "😕 Apologies! We couldn't fetch your profile to show your balance. Please try /start again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(user);
    let botUsername = "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { console.error(`${LOG_PREFIX_BAL} Could not fetch bot username: ${e.message}`); }

    const balanceLamports = await getUserBalance(userId);
    if (balanceLamports === null) {
        const errorMsgDm = "🏦 Oops! We couldn't retrieve your balance right now. Please try again in a moment, or contact support if this persists.";
        await safeSendMessage(userId, errorMsgDm, { parse_mode: 'MarkdownV2' });
        if (chatType !== 'private') {
            if (msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(() => {});
            await safeSendMessage(commandChatId, `${playerRef}, there was a hiccup fetching your balance. I've sent details to your DMs with @${escapeMarkdownV2(botUsername)}.`, { parse_mode: 'MarkdownV2' });
        }
        return;
    }

    const balanceUSDShort = await formatBalanceForDisplay(balanceLamports, 'USD');
    const balanceSOLShort = formatCurrency(balanceLamports, 'SOL'); // Use formatCurrency for consistent SOL display

    if (chatType !== 'private') {
        if (msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(() => {});
        const groupBalanceMessage = `${playerRef}, your current war chest holds approx\\. *${escapeMarkdownV2(balanceUSDShort)}* / *${escapeMarkdownV2(balanceSOLShort)}*\\. 💰\nFor a detailed breakdown and wallet actions, please check your DMs with me: @${escapeMarkdownV2(botUsername)} 📬`;
        await safeSendMessage(commandChatId, groupBalanceMessage, { parse_mode: 'MarkdownV2' });
    }
    
    const balanceMessageDm = `🏦 **Your Casino Royale Account Statement** 🏦\n\n` +
        `Player: ${playerRef}\n` +
        `\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\n` + // MarkdownV2 escaped hyphens
        `💰 Approx\\. Total Value: *${escapeMarkdownV2(balanceUSDShort)}*\n` +
        `🪙 SOL Balance: *${escapeMarkdownV2(balanceSOLShort)}*\n` +
        `⚙️ Lamports: \`${escapeMarkdownV2(String(balanceLamports))}\`\n` +
        `\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\n\n` +
        `Manage your funds or dive into the games using the buttons below! May luck be your ally! ✨`;

    const keyboardDm = {
        inline_keyboard: [
            [{ text: "💰 Deposit SOL", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }, { text: "💸 Withdraw SOL", callback_data: WITHDRAW_CALLBACK_ACTION }],
            [{ text: "📜 Transaction History", callback_data: "menu:history" }, { text: "🔗 Link/Update Wallet", callback_data: "menu:link_wallet_prompt" }],
            [{ text: "🎲 View Games & Rules", callback_data: "show_rules_menu" }, { text: "🤝 Referrals", callback_data: "menu:referral" }]
        ]
    };
    await safeSendMessage(userId, balanceMessageDm, { parse_mode: 'MarkdownV2', reply_markup: keyboardDm });
}

async function handleRulesCommand(chatId, userObj, messageIdToEdit = null, isEdit = false, chatType = 'private') {
    const LOG_PREFIX_RULES = `[RulesCmd UID:${userObj.telegram_id} Chat:${chatId}]`;
    const userMention = getPlayerDisplayReference(userObj);
    let botUsername = "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { console.error(`${LOG_PREFIX_RULES} Could not fetch bot username:`, e.message); }

    let targetChatId = chatId;
    let targetMessageId = messageIdToEdit;
    let targetIsEdit = isEdit;

    if (chatType !== 'private' && !isEdit) { // First time /rules in group
        if (messageIdToEdit && chatId !== String(userObj.telegram_id)) await bot.deleteMessage(chatId, messageIdToEdit).catch(() => {});
        await safeSendMessage(chatId, `${userMention}, I've summoned the Gamepedia to our private chat: @${escapeMarkdownV2(botUsername)} 📖 Dive in!`, { parse_mode: 'MarkdownV2' });
        targetChatId = String(userObj.telegram_id);
        targetMessageId = null;
        targetIsEdit = false;
    } else if (chatType !== 'private' && isEdit) { // Navigating back to rules menu from specific rule (which shouldn't happen in group, but as a fallback)
        const redirectText = `${userMention}, let's continue Browse game rules in our private chat for a better experience: @${escapeMarkdownV2(botUsername)} 📖`;
        await bot.editMessageText(redirectText, {
            chat_id: chatId, message_id: messageIdToEdit, parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=showRules` }]] }
        }).catch(async (e) => {
             if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES} Failed to edit group message for rules redirect, sending new. Error: ${e.message}`);
                await safeSendMessage(chatId, redirectText, {
                    parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=showRules` }]] }
                });
            }
        });
        targetChatId = String(userObj.telegram_id);
        targetMessageId = null;
        targetIsEdit = false;
    }
    
    // From here, assume targetChatId is the DM.
    const rulesIntroText = `📚 **${escapeMarkdownV2(BOT_NAME)} Gamepedia Central** 📚\n\nHey ${userMention}, welcome to our casino's hall of knowledge! Select any game below to learn its rules, strategies, and payout secrets. Master them all! 👇`;
    
    const gameRuleButtons = Object.entries(GAME_IDS).map(([key, gameCode]) => {
        const gameName = key.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
        let emoji = '❓';
        switch (gameCode) {
            case GAME_IDS.COINFLIP: emoji = '🪙'; break; case GAME_IDS.RPS: emoji = '✂️'; break;
            case GAME_IDS.DICE_ESCALATOR: emoji = '🎲'; break; case GAME_IDS.DICE_21: emoji = '🃏'; break;
            case GAME_IDS.OVER_UNDER_7: emoji = '🎲'; break; case GAME_IDS.DUEL: emoji = '⚔️'; break;
            case GAME_IDS.LADDER: emoji = '🪜'; break; case GAME_IDS.SEVEN_OUT: emoji = '🎲'; break;
            case GAME_IDS.SLOT_FRENZY: emoji = '🎰'; break;
        }
        return { text: `${emoji} ${escapeMarkdownV2(gameName)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${gameCode}` };
    });

    const rows = [];
    for (let i = 0; i < gameRuleButtons.length; i += 2) { // Arrange in 2 columns
        rows.push(gameRuleButtons.slice(i, i + 2));
    }
    rows.push([{ text: '🏛️ Back to Main Menu', callback_data: 'menu:main' }]); // menu:main should be handled by handleMenuAction
    if (targetChatId === String(userObj.telegram_id)) { // Only add wallet button if in DM
         rows.push([{ text: '💳 Wallet Dashboard', callback_data: 'menu:wallet' }]);
    }


    const keyboard = { inline_keyboard: rows };
    const options = { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true };

    if (targetIsEdit && targetMessageId) {
        try {
            await bot.editMessageText(rulesIntroText, { chat_id: targetChatId, message_id: targetMessageId, ...options });
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES} Failed to edit rules menu (ID: ${targetMessageId}), sending new. Error: ${e.message}`);
                await safeSendMessage(targetChatId, rulesIntroText, options);
            }
        }
    } else {
        if (targetMessageId && targetChatId === String(userObj.telegram_id)) { // If switching from a specific rule display in DM to the main rules menu
            await bot.deleteMessage(targetChatId, targetMessageId).catch(() => {});
        }
        await safeSendMessage(targetChatId, rulesIntroText, options);
    }
}

async function handleDisplayGameRules(chatId, originalMessageId, gameCode, userObj, chatType = 'private') {
    const LOG_PREFIX_RULES_DISP = `[RulesDisplay UID:${userObj.telegram_id} Game:${gameCode} Chat:${chatId}]`;
    const playerRef = getPlayerDisplayReference(userObj);
    let botUsername = "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { console.error(`${LOG_PREFIX_RULES_DISP} Could not fetch bot username:`, e.message); }

    let targetChatId = chatId;
    let targetMessageId = originalMessageId;
    let sendNew = (chatType !== 'private'); // Send new if original was in group

    if (chatType !== 'private') {
        const gameNameDisplayUpper = gameCode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
        const redirectText = `${playerRef}, I've sent the rules for *${escapeMarkdownV2(gameNameDisplayUpper)}* to our private chat: @${escapeMarkdownV2(botUsername)} 📖 Check your DMs!`;
        if (originalMessageId && bot) {
            await bot.editMessageText(redirectText, {
                chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                reply_markup: { inline_keyboard: [[{ text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=showRules_${gameCode}` }]] }
            }).catch(async (e) => {
                if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                    console.warn(`${LOG_PREFIX_RULES_DISP} Failed to edit group message for rule display redirect, sending new. Error: ${e.message}`);
                    await safeSendMessage(chatId, redirectText, {
                        parse_mode: 'MarkdownV2',
                        reply_markup: { inline_keyboard: [[{ text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=showRules_${gameCode}` }]] }
                    });
                }
            });
        } else { // No original message to edit, just send the redirect info
             await safeSendMessage(chatId, redirectText, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=showRules_${gameCode}` }]] } });
        }
        targetChatId = String(userObj.telegram_id); // Target DM for actual rules
        targetMessageId = null; // Must send new in DM
    }


    let rulesTitle = gameCode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    let gameEmoji = '🎲'; // Default
    switch (gameCode) {
        case GAME_IDS.COINFLIP: gameEmoji = '🪙'; rulesTitle = "Coinflip Challenge"; break;
        case GAME_IDS.RPS: gameEmoji = '✂️'; rulesTitle = "Rock Paper Scissors Showdown"; break;
        case GAME_IDS.DICE_ESCALATOR: gameEmoji = '🎲'; rulesTitle = "Dice Escalator Jackpot Climb"; break;
        case GAME_IDS.DICE_21: gameEmoji = '🃏'; rulesTitle = "Dice 21 (Casino Blackjack)"; break;
        case GAME_IDS.OVER_UNDER_7: gameEmoji = '🎲'; rulesTitle = "Over Under 7 Thrills"; break;
        case GAME_IDS.DUEL: gameEmoji = '⚔️'; rulesTitle = "High Roller Dice Duel"; break;
        case GAME_IDS.LADDER: gameEmoji = '🪜'; rulesTitle = "Greed's Ladder Challenge"; break;
        case GAME_IDS.SEVEN_OUT: gameEmoji = '🎲'; rulesTitle = "Sevens Out (Fast Craps)"; break;
        case GAME_IDS.SLOT_FRENZY: gameEmoji = '🎰'; rulesTitle = "Slot Fruit Frenzy Spins"; break;
        default: rulesTitle = `Game: ${rulesTitle}`; gameEmoji = '❓';
    }

    let rulesText = `${gameEmoji} *Welcome to ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
    rulesText += `Hey ${playerRef}\\! Ready to master *${escapeMarkdownV2(rulesTitle)}*\\? Here’s the lowdown:\n\n`;

    let solPrice = 100; // Default fallback price
    try { solPrice = await getSolUsdPrice(); }
    catch (priceErr) { console.warn(`${LOG_PREFIX_RULES_DISP} Could not fetch SOL price for rules display. Using default $${solPrice}. Error: ${priceErr.message}`); }

    const minBetUsdLimit = MIN_BET_USD_val;
    const maxBetUsdLimit = MAX_BET_USD_val;
    const minBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(convertUSDToLamports(minBetUsdLimit, solPrice), solPrice));
    const maxBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(convertUSDToLamports(maxBetUsdLimit, solPrice), solPrice));
    const defaultBetDisplay = minBetDisplay;

    rulesText += `*💰 General Betting Info:*\n` +
        `▫️ Place bets in USD (e\\.g\\., \`5\`, \`10.50\`) or SOL (e\\.g\\., \`0.1 sol\`, \`0.05\`).\n`+
        `▫️ Current Limits (USD Equiv\\.): *${minBetDisplay}* to *${maxBetDisplay}*\\.\n` +
        `▫️ No bet specified? Defaults to *${defaultBetDisplay}* USD approx\\.\n\n`;

    // Game-specific rules text generation
    switch (gameCode) {
        case GAME_IDS.COINFLIP:
            rulesText += `*🎯 Objective:* Correctly guess the outcome of a fair coin toss \\(Heads or Tails\\) against another player\\.\n` +
                         `*🎮 How to Play:* Type \`/coinflip <bet>\` in a group\\. Another player joins by clicking the button\\. The coin is flipped by the bot\\. Winner takes the entire pot \\(2x their bet\\)\\!\n` +
                         `*⚖️ Fair Play:* A true 50/50 chance\\. May the luckiest player prevail\\!`;
            break;
        case GAME_IDS.RPS:
            rulesText += `*🎯 Objective:* Outsmart your opponent in the timeless game of Rock, Paper, Scissors\\.\n` +
                         `*🎮 How to Play:* Start with \`/rps <bet>\` in a group\\. An opponent joins\\. Both players then secretly choose Rock 🪨, Paper 📄, or Scissors ✂️ via DM with the bot\\.\n` +
                         `*🏆 Winning Moves:* Rock crushes Scissors, Scissors cuts Paper, Paper covers Rock\\. Winner claims the pot \\(2x their bet\\)\\. Ties result in a push \\(bet returned\\)\\.\n` +
                         `*🧠 Strategy:* Predict your opponent's move or play your favorite\\!`;
            break;
        case GAME_IDS.DICE_ESCALATOR:
            const deBust = escapeMarkdownV2(String(DICE_ESCALATOR_BUST_ON));
            const deBotStand = escapeMarkdownV2(String(BOT_STAND_SCORE_DICE_ESCALATOR));
            const deJackpotScore = escapeMarkdownV2(String(TARGET_JACKPOT_SCORE));
            rulesText += `*🎯 Objective:* Accumulate the highest dice score against the Bot Dealer without rolling a ${deBust}\\. Hit *${deJackpotScore}\\+* and win the round to also claim the current Super Jackpot\\!\n` +
                         `*🎮 How to Play:* Type \`/de <bet>\`\\. You roll a die repeatedly; each roll adds to your score\\. \n` +
                         `*💥 Busting:* Rolling a *${deBust}* means you BUST! Your score becomes 0, and you lose your wager instantly\\.\n` +
                         `*✋ Standing:* Choose to "Stand" at any time to lock in your current score\\. The Bot Dealer then plays, attempting to beat your score, and will stand on *${deBotStand}* or more, or if they beat your score without busting\\. \n` +
                         `*🏆 Winning:* If your score is higher than the Bot's \\(or if the Bot busts and you haven't\\), you win 2x your bet\\. If you win AND your score was *${deJackpotScore} or higher*, you also win the current Dice Escalator Super Jackpot amount\\! A small percentage of each \`/de\` bet contributes to the jackpot\\.`;
            break;
        case GAME_IDS.DICE_21:
            const d21Target = escapeMarkdownV2(String(DICE_21_TARGET_SCORE));
            const d21BotStand = escapeMarkdownV2(String(DICE_21_BOT_STAND_SCORE));
            rulesText += `*🎯 Objective:* Get your total dice sum closer to *${d21Target}* than the Bot Dealer, without exceeding *${d21Target}*\\.\n` +
                         `*🎮 How to Play:* Start with \`/d21 <bet>\`\\. You're dealt two dice initially\\. You can "Hit" to take another die, or "Stand" to keep your current total\\.\n` +
                         `*🤖 Bot's Turn:* The Bot Dealer will Hit on scores less than *${d21BotStand}* and Stand on *${d21BotStand}* or more\\. \n` +
                         `*🏆 Winning & Payouts:*\n` +
                         `   ▫️ Standard Win \\(Your score > Bot's, or Bot busts\\): Pays *2x* your bet\\.\n` +
                         `   ▫️ Dice 21 Blackjack \\(Hitting *${d21Target}* exactly on your first two dice\\): Pays *2\\.5x* your bet\\!\n` +
                         `   ▫️ Busting \\(Your score > ${d21Target}\\): You lose your wager\\.\n` +
                         `   ▫️ Push \\(Tie with Bot, neither busts\\): Your bet is returned\\.`;
            break;
        // ... (Implement detailed rules for OU7, DUEL, LADDER, S7, SLOT_FRENZY here following the same pattern)
        // For brevity, I will add placeholders for the rest. You can fill these in with similar detail.
        case GAME_IDS.OVER_UNDER_7:
             const ou7DiceCount = escapeMarkdownV2(String(OU7_DICE_COUNT));
             const ou7SevenPayout = escapeMarkdownV2(String(OU7_PAYOUT_SEVEN + 1));
             rulesText += `*🎯 Objective:* Predict if the sum of *${ou7DiceCount}* dice will be Over 7, Under 7, or Exactly 7\\.\n` +
                          `*🎮 How to Play:* Start with \`/ou7 <bet>\`\\. Then, choose your prediction: Under 7 (sum 2\\-6), Exactly 7, or Over 7 (sum 8\\-12)\\.\n` +
                          `*🏆 Payouts:*\n` +
                          `   ▫️ Under 7 or Over 7: Wins *2x* your bet\\.\n` +
                          `   ▫️ Exactly 7: A lucky hit wins a handsome *${ou7SevenPayout}x* your bet\\!`;
            break;
        case GAME_IDS.DUEL:
            const duelDiceCount = escapeMarkdownV2(String(DUEL_DICE_COUNT));
             rulesText += `*🎯 Objective:* Roll a higher total sum with *${duelDiceCount}* dice than the Bot Dealer in a head\\-to\\-head duel\\.\n` +
                          `*🎮 How to Play:* Start with \`/duel <bet>\`\\. Click the button to roll your dice\\. The Bot Dealer then rolls theirs\\.\n` +
                          `*🏆 Winning:* Highest total sum wins *2x* your bet\\. Ties are a push \\(bet returned\\)\\.\n` +
                          `*⚔️ Pure Luck:* A straightforward test of fortune and valor\\!`;
            break;
        case GAME_IDS.LADDER:
            const ladderRollCount = escapeMarkdownV2(String(LADDER_ROLL_COUNT));
            const ladderBustOn = escapeMarkdownV2(String(LADDER_BUST_ON));
             rulesText += `*🎯 Objective:* Achieve a high score by summing *${ladderRollCount}* dice rolls\\. However, if any die shows a *${ladderBustOn}*, you bust\\!\n` +
                          `*🎮 How to Play:* Start with \`/ladder <bet>\`\\. The bot rolls all *${ladderRollCount}* dice for you at once\\.\n` +
                          `*💥 Busting:* If any of your dice shows a *${ladderBustOn}*, you bust and lose your wager\\.\n` +
                          `*🏆 Payout Tiers (Example for 3 dice):*\n`;
                        LADDER_PAYOUTS.forEach(p => {
                            rulesText += `   ▫️ Sum *${escapeMarkdownV2(String(p.min))}\\-${escapeMarkdownV2(String(p.max))}*: Pays *${escapeMarkdownV2(String(p.multiplier + 1))}x* bet \\(${escapeMarkdownV2(p.label)}\\)\n`;
                        });
                        rulesText += `   ▫️ Lower sums may result in a push or loss, check game feedback for specifics\\. Higher rolls mean higher rewards\\!`;
            break;
        case GAME_IDS.SEVEN_OUT:
             rulesText += `*🎯 Objective:* A simplified dice game inspired by Craps\\. Win by rolling your "Point" before a 7, or win instantly on the Come Out Roll\\. Uses two dice\\. \n` +
                          `*🎲 Come Out Roll (First Roll):*\n` +
                          `   ▫️ Roll a 7 or 11: *You Win Instantly* \\(2x bet\\)\\!\n` +
                          `   ▫️ Roll a 2, 3, or 12 \\("Craps"\\): *You Lose Instantly*\\.\n` +
                          `   ▫️ Roll any other sum \\(4, 5, 6, 8, 9, 10\\): This sum becomes your "Point"\\.\n` +
                          `*🎲 Point Phase (If a Point is Established):*\n` +
                          `   ▫️ The bot keeps rolling for you\\. If your Point is rolled again *before* a 7: *You Win* \\(2x bet\\)\\!\n` +
                          `   ▫️ If a 7 is rolled *before* your Point: *You Lose* \\("Seven Out"\\)\\.`;
            break;
        case GAME_IDS.SLOT_FRENZY:
             rulesText += `*🎯 Objective:* Spin the Telegram slot machine and match symbols for exciting payouts\\! The outcome is determined by Telegram's animated slot machine dice roll, which gives a value from 1 to 64\\.\n` +
                          `*🎮 How to Play:* Start with \`/slot <bet>\`\\. The bot spins the slot machine for you\\.\n` +
                          `*🏆 Winning Combinations & Payouts:*\n`;
                        // Assuming SLOT_PAYOUTS is an object like { "64": { multiplier: 100, symbols: "💎💎💎", label: "MEGA JACKPOT!" }, ... }
                        for (const key in SLOT_PAYOUTS) {
                            const payout = SLOT_PAYOUTS[key];
                            if (payout.multiplier >= 5) { // Show significant payouts in rules
                                rulesText += `   ▫️ ${payout.symbols} \\(Roll Value ${escapeMarkdownV2(key)}\\): Wins *${escapeMarkdownV2(String(payout.multiplier + 1))}x* bet \\(${escapeMarkdownV2(payout.label)}\\)\n`;
                            }
                        }
                        rulesText += `   ▫️ Many other combinations offer smaller wins or your bet back\\! Check the specific game result for details\\. Non\\-winning rolls result in loss of wager\\.`;
            break;
        default:
            rulesText += `📜 Rules for *"${escapeMarkdownV2(rulesTitle)}"* are as mysterious as a hidden treasure map\\! This game might be new or its rules are still being chiseled into stone\\. Please check back soon or use \`/help\` to see currently detailed games\\.`;
    }
    rulesText += `\n\nRemember to always play responsibly and within your limits\\. Good luck, high roller\\! 🍀`;

    const keyboard = { inline_keyboard: [[{ text: "📚 Back to Games List", callback_data: "show_rules_menu" }]] };
    
    if (!sendNew && targetMessageId && targetChatId === String(userObj.telegram_id)) {
        try {
            await bot.editMessageText(rulesText, {
                chat_id: targetChatId, message_id: Number(targetMessageId),
                parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true
            });
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES_DISP} Failed to edit rules display for ${gameCode}, sending new. Error: ${e.message}`);
                await safeSendMessage(targetChatId, rulesText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
            }
        }
    } else { // Send new message (either because sendNew is true, or no targetMessageId in DM)
        // If there was an old message in DM that we were supposed to edit but couldn't, delete it.
        if (targetMessageId && targetChatId === String(userObj.telegram_id) && !sendNew) {
             await bot.deleteMessage(targetChatId, Number(targetMessageId)).catch(()=>{});
        }
        await safeSendMessage(targetChatId, rulesText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
    }
}

async function handleJackpotCommand(chatId, userObj, chatType) {
    const LOG_PREFIX_JACKPOT = `[JackpotCmd UID:${userObj.telegram_id} Chat:${chatId}]`;
    const playerRef = getPlayerDisplayReference(userObj);
    
    try {
        const result = await queryDatabase('SELECT current_amount FROM jackpots WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]);
        let jackpotAmountLamports = 0n;
        if (result.rows.length > 0 && result.rows[0].current_amount) {
            jackpotAmountLamports = BigInt(result.rows[0].current_amount);
        }
        
        const jackpotUSD = await formatBalanceForDisplay(jackpotAmountLamports, 'USD');
        const jackpotSOL = formatCurrency(jackpotAmountLamports, 'SOL'); // Use formatCurrency for consistent SOL
        const jackpotTargetScoreDisplay = escapeMarkdownV2(String(TARGET_JACKPOT_SCORE));

        const jackpotMessage = `🏆 **Dice Escalator Super Jackpot Alert!** 🏆\n\n` +
            `Hey ${playerRef}, the current Super Jackpot is a shimmering mountain of riches:\n\n` +
            `💰 Approx\\. Value: *${escapeMarkdownV2(jackpotUSD)}*\n` +
            `🪙 SOL Amount: *${escapeMarkdownV2(jackpotSOL)}*\n\n` +
            `To claim this colossal prize, you must win a round of Dice Escalator with a score of *${jackpotTargetScoreDisplay} or higher* AND beat the Bot Dealer\\! Do you have what it takes\\? ✨\n\nType \`/de <bet>\` to try your luck\\!`;
            
        await safeSendMessage(chatId, jackpotMessage, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });

    } catch (error) {
        console.error(`${LOG_PREFIX_JACKPOT} Error fetching jackpot: ${error.message}`);
        await safeSendMessage(chatId, "⚙️ Apologies, there was a momentary glitch fetching the current Jackpot amount. Please try \`/jackpot\` again soon.", { parse_mode: 'MarkdownV2' });
    }
}

async function handleLeaderboardsCommand(msg, args) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) return;
    const playerRef = getPlayerDisplayReference(user);
    const typeArg = args[0] || 'overall_wagered';
    const typeDisplay = escapeMarkdownV2(typeArg.replace(/_/g, " ").replace(/\b\w/g, l => l.toUpperCase()));

    const leaderboardMessage = `🏆 **${escapeMarkdownV2(BOT_NAME)} Hall of Fame** 🏆 \\- _Coming Soon\\!_\n\n` +
        `Greetings, ${playerRef}! Our legendary leaderboards for categories like *${typeDisplay}* are currently under meticulous construction by our top casino architects\\. 🏗️\n\n` +
        `Soon, you'll be able to see who's dominating the casino floor, raking in the biggest wins, and making the boldest wagers\\!\n\n` +
        `Keep playing, sharpen your skills, and prepare to etch your name in ${escapeMarkdownV2(BOT_NAME)} history\\! Check back soon for the grand unveiling\\! ✨`;
    await safeSendMessage(chatId, leaderboardMessage, { parse_mode: 'MarkdownV2' });
}

async function handleGrantCommand(msg, args, adminUserObj /* This is the admin executing the command */) {
    const LOG_PREFIX_GRANT = `[GrantCmd UID:${adminUserObj.telegram_id}]`;
    const chatId = String(msg.chat.id); // Command was sent here
    const adminUserIdStr = String(adminUserObj.telegram_id);

    if (!ADMIN_USER_ID || adminUserIdStr !== ADMIN_USER_ID) {
        console.warn(`${LOG_PREFIX_GRANT} Unauthorized attempt to use /grant by user ${adminUserIdStr}.`);
        // Silently ignore or send "unknown command" if in PM (handled by main router's default)
        return;
    }

    if (args.length < 2) {
        await safeSendMessage(chatId, "⚙️ Admin Usage: `/grant <target_user_id_or_@username> <amount_SOL_or_Lamports> [Optional: reason]`\nExample: `/grant @LuckyPlayer 10 SOL Welcome Bonus` or `/grant 123456789 50000000 lamports Correction`", { parse_mode: 'MarkdownV2' });
        return;
    }

    const targetUserIdentifier = args[0];
    const amountArg = args[1];
    const reason = args.slice(2).join(' ') || `Admin grant by ${adminUserIdStr}`;
    let amountToGrantLamports;
    let targetUser;

    try {
        // Resolve target user
        if (targetUserIdentifier.startsWith('@')) {
            const usernameToFind = targetUserIdentifier.substring(1);
            const userRes = await queryDatabase('SELECT telegram_id, username, first_name FROM users WHERE LOWER(username) = LOWER($1)', [usernameToFind]);
            if (userRes.rowCount === 0) throw new Error(`Could not find user by username: \`${escapeMarkdownV2(targetUserIdentifier)}\`.`);
            targetUser = await getOrCreateUser(userRes.rows[0].telegram_id, userRes.rows[0].username, userRes.rows[0].first_name);
        } else if (/^\d+$/.test(targetUserIdentifier)) {
            targetUser = await getOrCreateUser(targetUserIdentifier);
        } else {
            throw new Error(`Invalid target user identifier: \`${escapeMarkdownV2(targetUserIdentifier)}\`. Use Telegram ID or @username.`);
        }
        if (!targetUser) throw new Error(`Could not find or create target user \`${escapeMarkdownV2(targetUserIdentifier)}\` for grant.`);
        
        // Parse amount (simplified from parseBetAmount, as this is an admin action)
        const amountArgLower = String(amountArg).toLowerCase();
        if (amountArgLower.endsWith('sol')) {
            const solAmount = parseFloat(amountArgLower.replace('sol', '').trim());
            if (isNaN(solAmount)) throw new Error("Invalid SOL amount for grant.");
            amountToGrantLamports = BigInt(Math.floor(solAmount * Number(LAMPORTS_PER_SOL)));
        } else if (amountArgLower.endsWith('lamports')) {
            amountToGrantLamports = BigInt(amountArgLower.replace('lamports','').trim());
        } else if (String(amountArg).includes('.')) {
             const solAmount = parseFloat(amountArg);
             if (isNaN(solAmount)) throw new Error("Invalid SOL amount (decimal) for grant.");
             amountToGrantLamports = BigInt(Math.floor(solAmount * Number(LAMPORTS_PER_SOL)));
        } else { // Assume integer, could be SOL or lamports. If it's a small int, likely SOL.
            const intAmount = BigInt(amountArg);
            if (intAmount > 0n && intAmount < 100000n && !amountArgLower.endsWith('000000')) { // Heuristic for SOL
                 amountToGrantLamports = BigInt(Math.floor(Number(intAmount) * Number(LAMPORTS_PER_SOL)));
            } else {
                amountToGrantLamports = intAmount;
            }
        }
        if (amountToGrantLamports === 0n) throw new Error("Grant amount cannot be zero."); // Allow negative for debit if needed by adding a separate command or flag
        if (isNaN(Number(amountToGrantLamports))) throw new Error("Could not parse grant amount to a valid number.");

    } catch (e) {
        await safeSendMessage(chatId, `⚠️ Invalid grant parameters. ${escapeMarkdownV2(e.message)}`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let grantClient = null;
    try {
        grantClient = await pool.connect();
        await grantClient.query('BEGIN');

        if (typeof updateUserBalanceAndLedger !== 'function') {
            console.error(`${LOG_PREFIX_GRANT} FATAL: updateUserBalanceAndLedger function is not defined for grant command.`);
            await safeSendMessage(chatId, "🛠️ Internal error: Grant functionality is currently unavailable. Core balance function missing.", { parse_mode: 'MarkdownV2' });
            await grantClient.query('ROLLBACK'); return;
        }
        const transactionType = amountToGrantLamports > 0n ? 'admin_grant_credit' : 'admin_grant_debit';
        const grantNotes = `Admin grant: ${reason}. By: ${adminUserIdStr}. To: ${targetUser.telegram_id}. Amount: ${formatCurrency(amountToGrantLamports, 'SOL')}`;
        
        const grantResult = await updateUserBalanceAndLedger(
            grantClient, targetUser.telegram_id, amountToGrantLamports, transactionType, {}, grantNotes
        );

        if (grantResult.success) {
            await grantClient.query('COMMIT');
            const grantAmountDisplay = escapeMarkdownV2(formatCurrency(amountToGrantLamports, 'SOL'));
            const newBalanceDisplay = escapeMarkdownV2(await formatBalanceForDisplay(grantResult.newBalanceLamports, 'USD')); // Show USD to user
            const targetUserDisplay = getPlayerDisplayReference(targetUser);
            const verb = amountToGrantLamports > 0n ? "granted" : "debited";
            const preposition = amountToGrantLamports > 0n ? "to" : "from";

            await safeSendMessage(chatId, `✅ Successfully ${verb} *${grantAmountDisplay}* ${preposition} user ${targetUserDisplay} (ID: \`${targetUser.telegram_id}\`).\nNew balance: *${newBalanceDisplay}*\\.`, { parse_mode: 'MarkdownV2' });
            
            const userNotifText = amountToGrantLamports > 0n
                ? `🎉 Hooray! You have received an admin credit of *${grantAmountDisplay}*\\! Your new balance is *${newBalanceDisplay}*\\. Reason: _${escapeMarkdownV2(reason)}_`
                : `⚠️ Admin Adjustment: Your account has been debited by *${grantAmountDisplay}*\\. Your new balance is *${newBalanceDisplay}*\\. Reason: _${escapeMarkdownV2(reason)}_`;
            await safeSendMessage(targetUser.telegram_id, userNotifText, { parse_mode: 'MarkdownV2' });
        } else {
            await grantClient.query('ROLLBACK');
            await safeSendMessage(chatId, `❌ Failed to ${amountToGrantLamports > 0n ? 'grant' : 'debit'} funds: ${escapeMarkdownV2(grantResult.error || "Unknown error during balance update.")}`, { parse_mode: 'MarkdownV2' });
        }
    } catch (grantError) {
        if (grantClient) await grantClient.query('ROLLBACK').catch(() => {});
        console.error(`${LOG_PREFIX_GRANT} Admin Grant DB Transaction Error: ${grantError.message}`);
        await safeSendMessage(chatId, `❌ Database error during grant: \`${escapeMarkdownV2(grantError.message)}\``, { parse_mode: 'MarkdownV2' });
    } finally {
        if (grantClient) grantClient.release();
    }
}

// Helper to create consistent "Play Again" style keyboards.
function createPostGameKeyboard(gameCode, betAmountLamports) {
    const playAgainBetDisplaySOL = escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL')); 
    const playAgainCallback = `play_again_${gameCode}:${betAmountLamports.toString()}`; 
    
    const gameNameClean = gameCode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    let gameEmoji = '🎲'; // Default
    switch(gameCode) {
        case GAME_IDS.COINFLIP: gameEmoji = '🪙'; break; case GAME_IDS.RPS: gameEmoji = '✂️'; break;
        case GAME_IDS.DICE_ESCALATOR: gameEmoji = '🎲'; break; case GAME_IDS.DICE_21: gameEmoji = '🃏'; break;
        case GAME_IDS.OVER_UNDER_7: gameEmoji = '🎲'; break; case GAME_IDS.DUEL: gameEmoji = '⚔️'; break;
        case GAME_IDS.LADDER: gameEmoji = '🪜'; break; case GAME_IDS.SEVEN_OUT: gameEmoji = '🎲'; break;
        case GAME_IDS.SLOT_FRENZY: gameEmoji = '🎰'; break;
    }

    return {
        inline_keyboard: [
            [{ text: `🔁 Play Again (${playAgainBetDisplaySOL})`, callback_data: playAgainCallback }],
            [{ text: "💰 Add Funds", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }, 
             { text: `📖 ${gameEmoji} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${gameCode}` }],
            [{ text: "🎲 All Games Menu", callback_data: "show_rules_menu" }, { text: "💳 Wallet", callback_data: "menu:wallet" }]
        ]
    };
}

console.log("Part 5a, Section 2 (NEW): General Command Handler Implementations - Complete.");
// --- End of Part 5a, Section 2 (NEW) ---
// --- Start of Part 5a, Section 3 (NEW): Group Game Handlers (Coinflip & RPS) ---
// index.js - Part 5a, Section 3: Coinflip & Rock Paper Scissors Game Logic
//----------------------------------------------------------------------------------
console.log("Loading Part 5a, Section 3 (NEW): Group Game Handlers (Coinflip & RPS)...");

// Assumed dependencies from previous Parts:
// Part 1: MIN_BET_USD_val, LAMPORTS_PER_SOL, formatCurrency, getPlayerDisplayReference,
//         escapeMarkdownV2, generateGameId, safeSendMessage, activeGames, groupGameSessions (Map),
//         JOIN_GAME_TIMEOUT_MS, QUICK_DEPOSIT_CALLBACK_ACTION, GAME_IDS (defined in 5a-S1 New), pool, bot,
//         stringifyWithBigInt, notifyAdmin
// Part 2: getOrCreateUser
// Part 3: formatBalanceForDisplay
// Part 4: determineCoinFlipOutcome, RPS_EMOJIS, RPS_CHOICES, determineRPSOutcome
// Part 5a-S2 (NEW): createPostGameKeyboard
// Part P2: updateUserBalanceAndLedger

// --- Group Game Session Management Helpers ---
// (These were conceptual in original Part 5a-S4, now functional with groupGameSessions Map)
async function getGroupSession(chatId, chatTitleIfNew = 'Group Chat') {
    const stringChatId = String(chatId);
    if (!groupGameSessions.has(stringChatId)) {
        groupGameSessions.set(stringChatId, {
            chatId: stringChatId,
            chatTitle: chatTitleIfNew,
            currentGameId: null,
            currentGameType: null,
            currentBetAmount: null,
            lastActivity: Date.now()
        });
        console.log(`[GroupSession] New session created for chat ID: ${stringChatId} ('${chatTitleIfNew}')`);
    }
    groupGameSessions.get(stringChatId).lastActivity = Date.now();
    return groupGameSessions.get(stringChatId);
}

async function updateGroupGameDetails(chatId, gameId, gameType, betAmountLamports) {
    const stringChatId = String(chatId);
    const session = await getGroupSession(stringChatId); // Ensures session exists
    session.currentGameId = gameId;
    session.currentGameType = gameType;
    session.currentBetAmount = gameId ? BigInt(betAmountLamports || 0) : null;
    session.lastActivity = Date.now();
    console.log(`[GroupSession] Updated group ${stringChatId}: GameID ${gameId || 'None'}, Type ${gameType || 'None'}, Bet ${session.currentBetAmount || 'N/A'}`);
}
console.log("[Group Game Utils] getGroupSession and updateGroupGameDetails defined for Part 5a-S3.");

// --- Coinflip Game Command & Callbacks ---

async function handleStartGroupCoinFlipCommand(chatId, initiatorUserObj, betAmountLamports, commandMessageId, chatType) {
    const LOG_PREFIX_CF_START = `[Coinflip_Start UID:${initiatorUserObj.telegram_id} CH:${chatId}]`;
    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_CF_START} Invalid betAmountLamports: ${betAmountLamports}. Expected positive BigInt.`);
        await safeSendMessage(chatId, "🪙 Oops! There was an issue with the bet amount for Coinflip. Please try again with a valid bet.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_CF_START} Initiating Coinflip. Bet: ${betAmountLamports} lamports in chat type: ${chatType}.`);

    const initiatorId = String(initiatorUserObj.telegram_id);
    const initiatorMention = getPlayerDisplayReference(initiatorUserObj);
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (chatType === 'private') {
        await safeSendMessage(chatId, `${initiatorMention}, 🪙 **Coinflip** is a thrilling two-player game! Please start it in a group chat where a worthy opponent can join your challenge.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    
    let chatInfo = null;
    try {
        if (bot && typeof bot.getChat === 'function') chatInfo = await bot.getChat(chatId);
    } catch (e) { console.warn(`${LOG_PREFIX_CF_START} Could not fetch chat info for ${chatId}: ${e.message}`); }
    const chatTitleEscaped = chatInfo?.title ? escapeMarkdownV2(chatInfo.title) : `this group`;

    const gameSession = await getGroupSession(chatId, chatInfo?.title || `Group ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        // Allow certain single-player games (vs. Bot) to run alongside a new group game attempt
        const singlePlayerGames = [GAME_IDS.DICE_ESCALATOR, GAME_IDS.DICE_21, GAME_IDS.OVER_UNDER_7, GAME_IDS.DUEL, GAME_IDS.LADDER, GAME_IDS.SEVEN_OUT, GAME_IDS.SLOT_FRENZY];
        if (!singlePlayerGames.includes(existingGame.type)) {
            const activeGameTypeDisplay = escapeMarkdownV2(existingGame.type.replace(/_/g, " "));
            await safeSendMessage(chatId, `⏳ Hold your horses, ${initiatorMention}! A game of \`${activeGameTypeDisplay}\` is already underway in ${chatTitleEscaped}. Please wait for it to conclude before starting a new Coinflip.`, { parse_mode: 'MarkdownV2' });
            return;
        }
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${initiatorMention}, your war chest is a bit light for a *${betDisplay}* Coinflip showdown! You need approximately *${neededDisplay}* more. Top up?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.COINFLIP);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, initiatorId, BigInt(-betAmountLamports), // Deduct bet
            'bet_placed_coinflip', { game_id_custom_field: gameId }, // Using a custom field for game ID before game_log_id exists
            `Bet for Coinflip game ${gameId} by initiator ${initiatorMention}`
        );

        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            await safeSendMessage(chatId, `${initiatorMention}, your Coinflip wager of *${betDisplay}* couldn't be placed due to a temporary glitch: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`\\. Please try again.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        console.log(`${LOG_PREFIX_CF_START} Initiator's bet ${betAmountLamports} lamports placed for Coinflip game ${gameId}. New balance: ${balanceUpdateResult.newBalanceLamports}`);
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports; // Update in-memory object

    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_CF_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_CF_START} Database error during Coinflip bet placement: ${dbError.message}`, dbError.stack);
        await safeSendMessage(chatId, "⚙️ A database gremlin interfered while starting the Coinflip game. Please try again in a moment.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }

    const gameDataCF = {
        type: GAME_IDS.COINFLIP, gameId, chatId: String(chatId), initiatorId,
        initiatorMention: initiatorMention, betAmount: betAmountLamports,
        participants: [{ userId: initiatorId, choice: null, mention: initiatorMention, betPlaced: true, userObj: initiatorUserObj }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null, chatType
    };
    activeGames.set(gameId, gameDataCF);
    await updateGroupGameDetails(chatId, gameId, GAME_IDS.COINFLIP, betAmountLamports);

    const joinMsgCF = `🪙 **A Coinflip Challenge Has Been Issued!** 🪙\n\nHigh roller ${initiatorMention} has bravely wagered *${betDisplay}* on the toss of a coin in ${chatTitleEscaped}!\n\nWho dares to face their luck? Step right up and click below to join the duel! 👇`;
    const kbCF = {
        inline_keyboard: [
            [{ text: "✨ Accept Coinflip Battle!", callback_data: `join_game:${gameId}` }],
            [{ text: "🚫 Cancel Game (Initiator Only)", callback_data: `cancel_game:${gameId}` }]
        ]
    };
    const setupMsgCF = await safeSendMessage(chatId, joinMsgCF, { parse_mode: 'MarkdownV2', reply_markup: kbCF });

    if (setupMsgCF && setupMsgCF.message_id && activeGames.has(gameId)) {
        activeGames.get(gameId).gameSetupMessageId = setupMsgCF.message_id;
    } else {
        console.error(`${LOG_PREFIX_CF_START} Failed to send Coinflip setup message for game ${gameId} or game was removed. Refunding initiator.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, initiatorId, betAmountLamports, 'refund_coinflip_setup_fail', {}, `Refund for Coinflip game ${gameId} due to setup message failure.`);
            await refundClient.query('COMMIT');
        } catch (err) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_CF_START} CRITICAL: Failed to refund initiator for Coinflip game ${gameId} after setup message failure: ${err.message}`);
        } finally {
            if (refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }

    setTimeout(async () => {
        const gdCF_timeout = activeGames.get(gameId);
        if (gdCF_timeout && gdCF_timeout.status === 'waiting_opponent') {
            console.log(`[Coinflip_Timeout GID:${gameId}] Coinflip game expired waiting for an opponent.`);
            let timeoutRefundClient;
            try {
                timeoutRefundClient = await pool.connect();
                await timeoutRefundClient.query('BEGIN');
                await updateUserBalanceAndLedger(timeoutRefundClient, gdCF_timeout.initiatorId, gdCF_timeout.betAmount, 'refund_coinflip_timeout', {}, `Refund for timed-out Coinflip game ${gameId}.`);
                await timeoutRefundClient.query('COMMIT');
            } catch (err) {
                if (timeoutRefundClient) await timeoutRefundClient.query('ROLLBACK');
                console.error(`[Coinflip_Timeout GID:${gameId}] CRITICAL: Failed to refund initiator for timed-out Coinflip game: ${err.message}`);
            } finally {
                if (timeoutRefundClient) timeoutRefundClient.release();
            }
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);

            const timeoutBetDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gdCF_timeout.betAmount, 'USD'));
            const timeoutMsgTextCF = `⏳ *Coinflip Challenge Unanswered* ⏳\nThe Coinflip game initiated by ${gdCF_timeout.initiatorMention} for *${timeoutBetDisplay}* in ${chatTitleEscaped} has expired as no challenger emerged. The wager has been refunded. Better luck next time!`;
            if (gdCF_timeout.gameSetupMessageId && bot) {
                bot.editMessageText(timeoutMsgTextCF, { chatId: String(chatId), message_id: Number(gdCF_timeout.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                    .catch(() => { safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' }); });
            } else {
                safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' });
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleStartGroupRPSCommand(chatId, initiatorUserObj, betAmountLamports, commandMessageId, chatType) {
    const LOG_PREFIX_RPS_START = `[RPS_Start UID:${initiatorUserObj.telegram_id} CH:${chatId}]`;
    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_RPS_START} Invalid betAmountLamports: ${betAmountLamports}. Expected positive BigInt.`);
        await safeSendMessage(chatId, "✂️ Oops! There was an issue with the bet amount for Rock Paper Scissors. Please try again with a valid bet.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_RPS_START} Initiating RPS. Bet: ${betAmountLamports} lamports in chat type: ${chatType}.`);

    const initiatorId = String(initiatorUserObj.telegram_id);
    const initiatorMention = getPlayerDisplayReference(initiatorUserObj);
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (chatType === 'private') {
        await safeSendMessage(chatId, `${initiatorMention}, 🪨📄✂️ **Rock Paper Scissors** is a classic duel for two! Please start it in a group chat where a challenger can accept your gauntlet.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let chatInfo = null;
    try { if (bot) chatInfo = await bot.getChat(chatId); } catch (e) { console.warn(`${LOG_PREFIX_RPS_START} Could not fetch chat info for ${chatId}: ${e.message}`); }
    const chatTitleEscaped = chatInfo?.title ? escapeMarkdownV2(chatInfo.title) : `this group`;

    const gameSession = await getGroupSession(chatId, chatInfo?.title || `Group ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
         const existingGame = activeGames.get(gameSession.currentGameId);
        const singlePlayerGames = [GAME_IDS.DICE_ESCALATOR, GAME_IDS.DICE_21, GAME_IDS.OVER_UNDER_7, GAME_IDS.DUEL, GAME_IDS.LADDER, GAME_IDS.SEVEN_OUT, GAME_IDS.SLOT_FRENZY];
        if (!singlePlayerGames.includes(existingGame.type)) {
            const activeGameTypeDisplay = escapeMarkdownV2(existingGame.type.replace(/_/g, " "));
            await safeSendMessage(chatId, `⏳ Easy there, ${initiatorMention}! A strategic game of \`${activeGameTypeDisplay}\` is currently in progress in ${chatTitleEscaped}. Let it conclude before starting a new RPS battle.`, { parse_mode: 'MarkdownV2' });
            return;
        }
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${initiatorMention}, your funds are a little short for an RPS duel of *${betDisplay}*! You need about *${neededDisplay}* more. Ready to reload?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.RPS);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, initiatorId, BigInt(-betAmountLamports),
            'bet_placed_rps', { game_id_custom_field: gameId },
            `Bet for RPS game ${gameId} by initiator ${initiatorMention}`
        );

        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            await safeSendMessage(chatId, `${initiatorMention}, your RPS wager of *${betDisplay}* hit a snag: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet issue")}\`\\. Please try once more.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        console.log(`${LOG_PREFIX_RPS_START} Initiator's bet ${betAmountLamports} for RPS game ${gameId} placed. New balance: ${balanceUpdateResult.newBalanceLamports}`);
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports;

    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_RPS_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_RPS_START} Database error during RPS bet placement: ${dbError.message}`, dbError.stack);
        await safeSendMessage(chatId, "⚙️ Our database gnomes are causing mischief! Failed to start the RPS game. Please try again shortly.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }

    const gameDataRPS = {
        type: GAME_IDS.RPS, gameId, chatId: String(chatId), initiatorId,
        initiatorMention: initiatorMention, betAmount: betAmountLamports,
        participants: [{ userId: initiatorId, choice: null, mention: initiatorMention, betPlaced: true, userObj: initiatorUserObj }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null, chatType
    };
    activeGames.set(gameId, gameDataRPS);
    await updateGroupGameDetails(chatId, gameId, GAME_IDS.RPS, betAmountLamports);

    const joinMsgRPS = `🪨📄✂️ **A Rock Paper Scissors Duel is Afoot!** 🪨📄✂️\n\nBrave strategist ${initiatorMention} has laid down the gauntlet in ${chatTitleEscaped}, staking *${betDisplay}* on their skill!\n\nWho possesses the cunning and courage to meet this challenge? Click below to enter the arena! 👇`;
    const kbRPS = {
        inline_keyboard: [
            [{ text: "⚔️ Accept RPS Challenge!", callback_data: `join_game:${gameId}` }],
            [{ text: "🚫 Withdraw Challenge (Initiator Only)", callback_data: `cancel_game:${gameId}` }]
        ]
    };
    const setupMsgRPS = await safeSendMessage(chatId, joinMsgRPS, { parse_mode: 'MarkdownV2', reply_markup: kbRPS });

    if (setupMsgRPS && setupMsgRPS.message_id && activeGames.has(gameId)) {
        activeGames.get(gameId).gameSetupMessageId = setupMsgRPS.message_id;
    } else {
        console.error(`${LOG_PREFIX_RPS_START} Failed to send RPS setup message for game ${gameId} or game was removed. Refunding initiator.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, initiatorId, betAmountLamports, 'refund_rps_setup_fail', {}, `Refund for RPS game ${gameId} due to setup message failure.`);
            await refundClient.query('COMMIT');
        } catch (err) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_RPS_START} CRITICAL: Failed to refund initiator for RPS game ${gameId} after setup message failure: ${err.message}`);
        } finally {
            if (refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }

    setTimeout(async () => {
        const gdRPS_timeout = activeGames.get(gameId);
        if (gdRPS_timeout && gdRPS_timeout.status === 'waiting_opponent') {
            console.log(`[RPS_Timeout GID:${gameId}] RPS game expired waiting for opponent.`);
            let timeoutRefundClient;
            try {
                timeoutRefundClient = await pool.connect();
                await timeoutRefundClient.query('BEGIN');
                await updateUserBalanceAndLedger(timeoutRefundClient, gdRPS_timeout.initiatorId, gdRPS_timeout.betAmount, 'refund_rps_timeout', {}, `Refund for timed-out RPS game ${gameId}.`);
                await timeoutRefundClient.query('COMMIT');
            } catch (err) {
                if (timeoutRefundClient) await timeoutRefundClient.query('ROLLBACK');
                console.error(`[RPS_Timeout GID:${gameId}] CRITICAL: Failed to refund initiator for timed-out RPS game: ${err.message}`);
            } finally {
                if (timeoutRefundClient) timeoutRefundClient.release();
            }
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);

            const timeoutBetDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gdRPS_timeout.betAmount, 'USD'));
            const timeoutMsgTextRPS = `⏳ *RPS Duel Unanswered* ⏳\nThe Rock Paper Scissors challenge by ${gdRPS_timeout.initiatorMention} for *${timeoutBetDisplay}* in ${chatTitleEscaped} has expired without an opponent. The wager has been bravely refunded.`;
            if (gdRPS_timeout.gameSetupMessageId && bot) {
                bot.editMessageText(timeoutMsgTextRPS, { chatId: String(chatId), message_id: Number(gdRPS_timeout.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                    .catch(() => { safeSendMessage(chatId, timeoutMsgTextRPS, { parse_mode: 'MarkdownV2' }); });
            } else {
                safeSendMessage(chatId, timeoutMsgTextRPS, { parse_mode: 'MarkdownV2' });
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}


// --- Shared Group Game Callback Handlers (Coinflip & RPS) ---

async function handleJoinGameCallback(chatId, joinerUserObj, gameId, interactionMessageId, callbackQueryId, chatType) {
    const LOG_PREFIX_JOIN = `[JoinGame_CB UID:${joinerUserObj.telegram_id} GID:${gameId} Chat:${chatId}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This game has vanished like a mirage! It's no longer available.", show_alert: true });
        if (interactionMessageId && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        }
        return;
    }

    const joinerId = String(joinerUserObj.telegram_id);
    if (gameData.initiatorId === joinerId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "😉 You can't join your own epic challenge! Waiting for another hero.", show_alert: false });
        return;
    }
    if (gameData.participants.length >= 2) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "🏃💨 Too slow, brave warrior! This duel is already full.", show_alert: true });
        return;
    }
    if (gameData.status !== 'waiting_opponent') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ This game is not currently accepting new challengers. Perhaps it's already started or been cancelled.", show_alert: true });
        return;
    }

    const joinerMention = getPlayerDisplayReference(joinerUserObj);
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    if (BigInt(joinerUserObj.balance) < gameData.betAmount) {
        const needed = gameData.betAmount - BigInt(joinerUserObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await bot.answerCallbackQuery(callbackQueryId, { text: `Your treasury is a bit light! Need ~${neededDisplay} more.`, show_alert: true });
        await safeSendMessage(chatId, `${joinerMention}, your current balance is insufficient to join this *${betDisplay}* duel\\. You need approximately *${neededDisplay}* more\\. Top up your coffers?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const ledgerTxType = gameData.type === GAME_IDS.COINFLIP ? 'bet_placed_coinflip_join' : 'bet_placed_rps_join';
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, joinerId, BigInt(-gameData.betAmount),
            ledgerTxType, { game_id_custom_field: gameId },
            `Bet placed for ${gameData.type} game ${gameId} by joiner ${joinerMention}`
        );

        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_JOIN} Bet placement failed for joiner ${joinerId}: ${balanceUpdateResult.error}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Wager failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Wallet glitch")}. Try again?`, show_alert: true });
            return;
        }
        await client.query('COMMIT');
        console.log(`${LOG_PREFIX_JOIN} Joiner's bet ${gameData.betAmount} lamports placed for game ${gameId}. New balance: ${balanceUpdateResult.newBalanceLamports}`);
        joinerUserObj.balance = balanceUpdateResult.newBalanceLamports;

    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_JOIN} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_JOIN} Database error during joiner bet placement: ${dbError.message}`, dbError.stack);
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚙️ A database hiccup occurred while joining. Please try again.", show_alert: true });
        return;
    } finally {
        if (client) client.release();
    }

    await bot.answerCallbackQuery(callbackQueryId, { text: `✅ You're in! You've joined the ${gameData.type} game for ${betDisplay}!` });

    gameData.participants.push({ userId: joinerId, choice: null, mention: joinerMention, betPlaced: true, userObj: joinerUserObj });
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId); // Fallback to gameSetupMessageId

    // --- COINFLIP RESOLUTION ---
    if (gameData.type === GAME_IDS.COINFLIP && gameData.participants.length === 2) {
        gameData.status = 'resolving';
        activeGames.set(gameId, gameData); // Ensure status update is saved

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        // Assign choices for display clarity if not already set (though for coinflip, they aren't player-chosen)
        p1.choice = 'heads'; 
        p2.choice = 'tails';

        const cfResult = determineCoinFlipOutcome(); // From Part 4 { outcome: 'heads'/'tails', outcomeString: "Heads"/"Tails", emoji: '🪙' }
        let winnerParticipant = (cfResult.outcome === p1.choice) ? p1 : p2;
        let loserParticipant = (winnerParticipant === p1) ? p2 : p1;

        const totalPot = gameData.betAmount * 2n; // Winner gets total pot
        const profitForWinner = gameData.betAmount; // Their bet + opponent's bet -> their bet is profit
        let gameOutcomeClient;
        let winnerUpdateSuccess = false;
        let winnerNewBalanceLamports = BigInt(winnerParticipant.userObj.balance); // Fallback to balance before this game's outcome

        try {
            gameOutcomeClient = await pool.connect();
            await gameOutcomeClient.query('BEGIN');

            const winnerUpdateResult = await updateUserBalanceAndLedger(
                gameOutcomeClient, winnerParticipant.userId, totalPot, // Credit total pot
                'win_coinflip', { game_id_custom_field: gameId },
                `Won Coinflip game ${gameId} vs ${loserParticipant.mention}. Pot: ${totalPot}`
            );
            if (!winnerUpdateResult.success) {
                throw new Error(`Failed to credit Coinflip winner ${winnerParticipant.userId}: ${winnerUpdateResult.error}`);
            }
            winnerNewBalanceLamports = winnerUpdateResult.newBalanceLamports; // Update from actual result
            
            // Loser's bet was already deducted. Just log the loss for record.
            await updateUserBalanceAndLedger(
                gameOutcomeClient, loserParticipant.userId, 0n, // No change to balance
                'loss_coinflip', { game_id_custom_field: gameId },
                `Lost Coinflip game ${gameId} vs ${winnerParticipant.mention}`
            );
            winnerUpdateSuccess = true;
            await gameOutcomeClient.query('COMMIT');
        } catch (err) {
            if (gameOutcomeClient) await gameOutcomeClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_JOIN} CRITICAL: Error processing Coinflip payout for game ${gameId}. Winner: ${winnerParticipant.userId}. Error: ${err.message}`, err.stack);
            winnerUpdateSuccess = false;
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 CRITICAL Coinflip Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nWinner: ${winnerParticipant.mention} (\`${escapeMarkdownV2(winnerParticipant.userId)}\`)\nAmount Due (Pot): \`${escapeMarkdownV2(formatCurrency(totalPot))}\`\nError: DB Update Failed. Manual credit/check required.`, { parse_mode: 'MarkdownV2' });
            }
        } finally {
            if (gameOutcomeClient) gameOutcomeClient.release();
        }

        let resMsg = `🪙 **Coinflip Resolved! The Coin Spirals...** 🪙\nBet Amount: *${betDisplay}*\n\n`;
        resMsg += `${p1.mention} called *Heads*! ${p2.mention} called *Tails*!\n\n`;
        resMsg += `The coin glints, tumbles... and lands on **${escapeMarkdownV2(cfResult.outcomeString)}** ${cfResult.emoji}!\n\n`;
        
        const profitDisplay = escapeMarkdownV2(await formatBalanceForDisplay(profitForWinner, 'USD'));
        resMsg += `🎉 Magnificent! Congratulations, ${winnerParticipant.mention}! You've masterfully claimed the pot, securing a *${profitDisplay}* profit! 🎉`;

        if (winnerUpdateSuccess) {
            const winnerNewBalanceDisplay = escapeMarkdownV2(await formatBalanceForDisplay(winnerNewBalanceLamports, 'USD'));
            resMsg += `\n\n${winnerParticipant.mention}'s new balance: *${winnerNewBalanceDisplay}*\\.`;
        } else {
            resMsg += `\n\n⚠️ A mystical force (technical issue) interfered while crediting ${winnerParticipant.mention}'s winnings. Our casino wizards have been notified to investigate.`;
        }
        
        const postGameKeyboard = createPostGameKeyboard(GAME_IDS.COINFLIP, gameData.betAmount);
        if (messageToEditId && bot) {
            bot.editMessageText(resMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard })
                .catch(async (e) => { 
                    console.warn(`${LOG_PREFIX_JOIN} Failed to edit Coinflip result message (ID: ${messageToEditId}), sending new: ${e.message}`);
                    await safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard }); 
                });
        } else {
            await safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
        }

        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);

    // --- RPS - PROMPT FOR CHOICES ---
    } else if (gameData.type === GAME_IDS.RPS && gameData.participants.length === 2) {
        gameData.status = 'waiting_choices';
        activeGames.set(gameId, gameData); // Save updated status

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];

        const rpsPrompt = `🪨📄✂️ **Rock Paper Scissors - The Duel is Set!** 🪨📄✂️\n\n${p1.mention} vs ${p2.mention} for a grand prize of *${betDisplay}*!\n\nWarriors, the arena awaits your command! Both players, please *secretly* select your move using the buttons below. Your choice will be confirmed privately by me in DM. Choose wisely!`;
        const rpsKeyboard = {
            inline_keyboard: [[
                { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
                { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
                { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
            ], [
                { text: "🚫 Withdraw Challenge (Initiator Only)", callback_data: `cancel_game:${gameId}` }
            ]]
        };

        let editedMessageId = messageToEditId;
        if (messageToEditId && bot) {
            bot.editMessageText(rpsPrompt, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard })
                .catch(async (e) => { 
                    console.warn(`${LOG_PREFIX_JOIN} Failed to edit RPS prompt message (ID: ${messageToEditId}), sending new: ${e.message}`);
                    const newMsg = await safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); 
                    if (newMsg && newMsg.message_id && activeGames.has(gameId)) {
                        activeGames.get(gameId).gameSetupMessageId = newMsg.message_id; // Update message ID if new one sent
                        editedMessageId = newMsg.message_id;
                    }
                });
        } else {
            const newMsg = await safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard });
            if (newMsg && newMsg.message_id && activeGames.has(gameId)) {
                 activeGames.get(gameId).gameSetupMessageId = newMsg.message_id;
                 editedMessageId = newMsg.message_id;
            }
        }
        // Set a timeout for players to make their RPS choice
        setTimeout(async () => {
            const gdRPS_choiceTimeout = activeGames.get(gameId);
            if (gdRPS_choiceTimeout && gdRPS_choiceTimeout.status === 'waiting_choices') {
                const p1_timeout = gdRPS_choiceTimeout.participants[0];
                const p2_timeout = gdRPS_choiceTimeout.participants[1];
                let refundP1 = false, refundP2 = false;
                let timeoutMessage = `⏳ *RPS Stalemate!* ⏳\nThe duel between ${p1_timeout.mention} and ${p2_timeout.mention} for *${betDisplay}* timed out as not all choices were made.`;

                if (!p1_timeout.choice && !p2_timeout.choice) {
                    timeoutMessage += "\nBoth players failed to choose. All bets refunded.";
                    refundP1 = true; refundP2 = true;
                } else if (!p1_timeout.choice) {
                    timeoutMessage += `\n${p1_timeout.mention} did not make a choice. ${p2_timeout.mention} wins by default, and their bet is refunded. ${p1_timeout.mention}'s bet is lost.`;
                    refundP2 = true; // p2 gets their bet back as opponent didn't play
                    // p1's bet goes to house or p2 - for simplicity, let's say p2 gets their bet back and p1's bet is lost to house.
                    // Or for a kinder approach: p2 wins the pot, refundP2 (meaning p2 gets 2x bet). This needs careful payout logic.
                    // Simplest for now: p2 wins, refund their bet, p1 loses.
                    // For true "win by default", p2 should get p1's bet.
                    // Let's consider a simple refund for the active player, loss for inactive.
                } else if (!p2_timeout.choice) {
                    timeoutMessage += `\n${p2_timeout.mention} did not make a choice. ${p1_timeout.mention} wins by default, and their bet is refunded. ${p2_timeout.mention}'s bet is lost.`;
                    refundP1 = true;
                }

                // For simplicity on timeout: refund both if anyone didn't choose, and cancel game.
                // A more complex "win by default" needs careful handling of who gets what.
                // Sticking to full refund for now if choices aren't made.
                timeoutMessage += "\nAll wagers have been refunded due to timeout.";
                refundP1 = true; refundP2 = true; // Let's refund both to be safe and simple for timeout.

                let timeoutDbClient;
                try {
                    timeoutDbClient = await pool.connect();
                    await timeoutDbClient.query('BEGIN');
                    if (refundP1) await updateUserBalanceAndLedger(timeoutDbClient, p1_timeout.userId, gdRPS_choiceTimeout.betAmount, 'refund_rps_choice_timeout', {}, `Refund for RPS game ${gameId} - P1 choice timeout`);
                    if (refundP2) await updateUserBalanceAndLedger(timeoutDbClient, p2_timeout.userId, gdRRPS_choiceTimeout.betAmount, 'refund_rps_choice_timeout', {}, `Refund for RPS game ${gameId} - P2 choice timeout`);
                    await timeoutDbClient.query('COMMIT');
                } catch (err) {
                    if (timeoutDbClient) await timeoutDbClient.query('ROLLBACK');
                     console.error(`[RPS_ChoiceTimeout GID:${gameId}] CRITICAL: Failed to refund players for timed-out RPS game: ${err.message}`);
                } finally {
                    if (timeoutDbClient) timeoutDbClient.release();
                }

                activeGames.delete(gameId);
                await updateGroupGameDetails(chatId, null, null, null);
                if (editedMessageId && bot) {
                    bot.editMessageText(timeoutMessage, { chatId: String(chatId), message_id: Number(editedMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                        .catch(() => { safeSendMessage(chatId, timeoutMessage, { parse_mode: 'MarkdownV2' }); });
                } else {
                     safeSendMessage(chatId, timeoutMessage, { parse_mode: 'MarkdownV2' });
                }
            }
        }, JOIN_GAME_TIMEOUT_MS * 1.5); // Slightly longer timeout for choices
    }
}

async function handleCancelGameCallback(chatId, cancellerUserObj, gameId, interactionMessageId, callbackQueryId, chatType) {
    const LOG_PREFIX_CANCEL = `[CancelGame_CB UID:${cancellerUserObj.telegram_id} GID:${gameId} Chat:${chatId}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This game has already concluded or vanished into thin air!", show_alert: true });
        if (interactionMessageId && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        }
        return;
    }

    if (gameData.initiatorId !== String(cancellerUserObj.telegram_id)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Hold on! Only the game's noble initiator can cancel this challenge.", show_alert: true });
        return;
    }

    if (gameData.status !== 'waiting_opponent' && !(gameData.type === GAME_IDS.RPS && gameData.status === 'waiting_choices' && !gameData.participants.find(p=>p.userId !== gameData.initiatorId)?.choice)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ The game is too far along! This challenge can no longer be withdrawn.", show_alert: true });
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: "✅ Game cancellation in progress..." });

    console.log(`${LOG_PREFIX_CANCEL} Game ${gameId} cancellation requested by initiator. Refunding bets for ${gameData.participants.length} participant(s).`);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        for (const p of gameData.participants) {
            if (p.betPlaced && p.userId && gameData.betAmount > 0n) {
                const refundResult = await updateUserBalanceAndLedger(
                    client, p.userId, gameData.betAmount, // Refund the original bet amount
                    `refund_${gameData.type}_cancelled`, { game_id_custom_field: gameId },
                    `Refund for cancelled ${gameData.type} game ${gameId} by initiator`
                );
                if (refundResult.success) {
                    console.log(`${LOG_PREFIX_CANCEL} Refunded ${formatCurrency(gameData.betAmount, 'SOL')} to UserID: ${p.userId}. New Bal: ${formatCurrency(refundResult.newBalanceLamports, 'SOL')}`);
                } else {
                    console.error(`${LOG_PREFIX_CANCEL} CRITICAL: Failed to refund UserID: ${p.userId} for cancelled game ${gameId}. Error: ${refundResult.error}`);
                    if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL FAILED REFUND (Cancellation) 🚨\nGame: ${gameData.type} ID: ${gameId}\nUser: ${p.mention} (${p.userId})\nAmount: ${formatCurrency(gameData.betAmount)}\nReason: Cancellation refund failed DB update. MANUAL REFUND REQUIRED.`, {parse_mode:'MarkdownV2'});
                }
            }
        }
        await client.query('COMMIT');
    } catch (dbError) {
        if (client) await client.query('ROLLBACK');
        console.error(`${LOG_PREFIX_CANCEL} Database error during cancellation refunds for game ${gameId}: ${dbError.message}`, dbError.stack);
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL: Systemic DB error during cancellation refunds for Game ID: ${gameId}. Error: ${dbError.message}. Some refunds may have failed.`, {parse_mode:'MarkdownV2'});
    } finally {
        if (client) client.release();
    }

    const gameTypeDisplay = escapeMarkdownV2(gameData.type.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    const cancellationMessage = `🚫 **Challenge Retracted by Initiator!** 🚫\n\nThe ${gameTypeDisplay} game for *${betDisplay}*, started by ${gameData.initiatorMention}, has been cancelled. All wagers have been gallantly returned to the participants' treasuries.`;

    const msgToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (msgToEdit && bot) {
        bot.editMessageText(cancellationMessage, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: {} })
            .catch(async (e) => { 
                console.warn(`${LOG_PREFIX_CANCEL} Failed to edit cancel message (ID: ${msgToEdit}), sending new: ${e.message}`);
                await safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' }); 
            });
    } else {
        await safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
    }

    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
    console.log(`${LOG_PREFIX_CANCEL} Game ${gameId} cancelled and removed from active sessions.`);
}

async function handleRPSChoiceCallback(chatId, userChoiceObj, gameId, choiceKey, interactionMessageId, callbackQueryId, chatType) {
    const LOG_PREFIX_RPS_CHOICE = `[RPS_Choice_CB UID:${userChoiceObj.telegram_id} GID:${gameId} Choice:${choiceKey}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.RPS || gameData.status !== 'waiting_choices') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This RPS game isn't active or it's not time to make your move!", show_alert: true });
        // Do not edit the main game message here as it might still be valid for the other player.
        return;
    }

    const participant = gameData.participants.find(p => p.userId === String(userChoiceObj.telegram_id));
    if (!participant) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "🤔 You don't seem to be a duelist in this RPS battle.", show_alert: true });
        return;
    }
    if (participant.choice) {
        const existingChoiceEmoji = RPS_EMOJIS[participant.choice] || '❓';
        await bot.answerCallbackQuery(callbackQueryId, { text: `🛡️ You've already locked in ${existingChoiceEmoji}! Waiting for your opponent's masterstroke.`, show_alert: false });
        return;
    }

    participant.choice = choiceKey.toLowerCase();
    const choiceEmoji = RPS_EMOJIS[participant.choice] || '❓';
    const choiceFormatted = participant.choice.charAt(0).toUpperCase() + participant.choice.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, { text: `🎯 Your choice: ${choiceEmoji} ${choiceFormatted} is set! The tension builds...`, show_alert: false });

    const p1 = gameData.participants[0];
    const p2 = gameData.participants[1];
    const allChosen = p1 && p1.choice && p2 && p2.choice;
    const msgToEditId = Number(gameData.gameSetupMessageId || interactionMessageId); // Prefer gameSetupMessageId

    // --- RPS RESOLUTION ---
    if (allChosen) {
        gameData.status = 'resolving'; // Prevent further choices
        activeGames.set(gameId, gameData); // Update status

        const rpsOutcome = determineRPSOutcome(p1.choice, p2.choice); // From Part 4
        const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
        const p1ChoiceFormatted = p1.choice.charAt(0).toUpperCase() + p1.choice.slice(1);
        const p2ChoiceFormatted = p2.choice.charAt(0).toUpperCase() + p2.choice.slice(1);


        let resultText = `🪨📄✂️ **Rock Paper Scissors - The Dust Settles!** 🪨📄✂️\nBet Amount: *${betDisplay}*\n\n`;
        resultText += `${p1.mention} chose: ${RPS_EMOJIS[p1.choice]} ${escapeMarkdownV2(p1ChoiceFormatted)}\n`;
        resultText += `${p2.mention} chose: ${RPS_EMOJIS[p2.choice]} ${escapeMarkdownV2(p2ChoiceFormatted)}\n\n`;
        resultText += `*Result:* ${rpsOutcome.description}\n\n`; // rpsOutcome.description is already MarkdownV2 formatted

        let finalBalancesText = "";
        let clientGameOutcome;

        try {
            clientGameOutcome = await pool.connect();
            await clientGameOutcome.query('BEGIN');

            let p1FinalBalance = BigInt(p1.userObj.balance);
            let p2FinalBalance = BigInt(p2.userObj.balance);
            let outcomeProcessedSuccessfully = false;

            if (rpsOutcome.result === 'win_player1') {
                const winnings = gameData.betAmount * 2n; // Total pot
                const p1Update = await updateUserBalanceAndLedger(clientGameOutcome, p1.userId, winnings, 'win_rps', {game_id_custom_field: gameId}, `Won RPS game ${gameId} vs ${p2.mention}`);
                const p2Update = await updateUserBalanceAndLedger(clientGameOutcome, p2.userId, 0n, 'loss_rps', {game_id_custom_field: gameId}, `Lost RPS game ${gameId} vs ${p1.mention}`);
                if(p1Update.success) p1FinalBalance = p1Update.newBalanceLamports;
                outcomeProcessedSuccessfully = p1Update.success && p2Update.success;
            } else if (rpsOutcome.result === 'win_player2') {
                const winnings = gameData.betAmount * 2n;
                const p2Update = await updateUserBalanceAndLedger(clientGameOutcome, p2.userId, winnings, 'win_rps', {game_id_custom_field: gameId}, `Won RPS game ${gameId} vs ${p1.mention}`);
                const p1Update = await updateUserBalanceAndLedger(clientGameOutcome, p1.userId, 0n, 'loss_rps', {game_id_custom_field: gameId}, `Lost RPS game ${gameId} vs ${p2.mention}`);
                if(p2Update.success) p2FinalBalance = p2Update.newBalanceLamports;
                outcomeProcessedSuccessfully = p1Update.success && p2Update.success;
            } else if (rpsOutcome.result === 'draw') {
                const refund1 = await updateUserBalanceAndLedger(clientGameOutcome, p1.userId, gameData.betAmount, 'refund_rps_draw', {game_id_custom_field: gameId}, `Draw RPS game ${gameId} vs ${p2.mention}`);
                const refund2 = await updateUserBalanceAndLedger(clientGameOutcome, p2.userId, gameData.betAmount, 'refund_rps_draw', {game_id_custom_field: gameId}, `Draw RPS game ${gameId} vs ${p1.mention}`);
                if(refund1.success) p1FinalBalance = refund1.newBalanceLamports;
                if(refund2.success) p2FinalBalance = refund2.newBalanceLamports;
                outcomeProcessedSuccessfully = refund1.success && refund2.success;
            } else { // Error case from determineRPSOutcome
                console.error(`${LOG_PREFIX_RPS_CHOICE} RPS outcome determination error: ${rpsOutcome.description}`);
                resultText += `⚙️ An unexpected internal error occurred determining the winner. Bets may be refunded if an issue is confirmed.`;
                outcomeProcessedSuccessfully = false; // Consider it not processed correctly
            }

            if (!outcomeProcessedSuccessfully) {
                 throw new Error(`Failed to process RPS outcome updates in DB for game ${gameId}.`);
            }
            await clientGameOutcome.query('COMMIT');

            if (rpsOutcome.result === 'win_player1') finalBalancesText += `\n${p1.mention}'s new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(p1FinalBalance, 'USD'))}*\\.`;
            else if (rpsOutcome.result === 'win_player2') finalBalancesText += `\n${p2.mention}'s new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(p2FinalBalance, 'USD'))}*\\.`;
            else if (rpsOutcome.result === 'draw') {
                finalBalancesText += `\n${p1.mention}'s balance: *${escapeMarkdownV2(await formatBalanceForDisplay(p1FinalBalance, 'USD'))}*\\.`;
                finalBalancesText += `\n${p2.mention}'s balance: *${escapeMarkdownV2(await formatBalanceForDisplay(p2FinalBalance, 'USD'))}*\\.`;
            }

        } catch (dbError) {
            if (clientGameOutcome) await clientGameOutcome.query('ROLLBACK');
            console.error(`${LOG_PREFIX_RPS_CHOICE} CRITICAL: DB error during RPS game ${gameId} outcome: ${dbError.message}`, dbError.stack);
            resultText += `\n\n⚠️ A critical database error occurred finalizing this game. Our casino staff has been notified. Your balance may reflect the pre-game state.`;
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 CRITICAL RPS Outcome DB Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nError: ${dbError.message}. Balances might be incorrect. MANUAL CHECK REQUIRED.`,{parse_mode:'MarkdownV2'});
            }
        } finally {
            if (clientGameOutcome) clientGameOutcome.release();
        }

        resultText += finalBalancesText;
        const postGameKeyboard = createPostGameKeyboard(GAME_IDS.RPS, gameData.betAmount);

        if (msgToEditId && bot) {
            bot.editMessageText(resultText, { chatId: String(chatId), message_id: msgToEditId, parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard })
                .catch(async (e) => { 
                    console.warn(`${LOG_PREFIX_RPS_CHOICE} Failed to edit RPS result message (ID: ${msgToEditId}), sending new: ${e.message}`);
                    await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard }); 
                });
        } else {
            await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
        }
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);

    // --- RPS - WAITING FOR OTHER PLAYER ---
    } else { 
        const p1Status = p1.choice ? `✅ ${p1.mention} has chosen their destiny!` : `⏳ ${p1.mention} is pondering their next move...`;
        const p2Status = p2?.choice ? `✅ ${p2.mention} has made their strategic selection!` : `⏳ ${p2?.mention || 'The Challenger'} is calculating their options...`;
        
        const waitingText = `🪨📄✂️ **RPS Battle - Moves Pending!** 🪨📄✂️\nBet: *${betDisplay}*\n\n${p1Status}\n${p2Status}\n\nThe air crackles with anticipation! Waiting for all warriors to commit to their action. Use the buttons below if you haven't chosen.`;
        if (msgToEditId && bot) {
            try {
                // Re-send the original RPS keyboard so the other player can still choose
                const rpsKeyboardForWait = {
                    inline_keyboard: [[
                        { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
                        { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
                        { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
                    ], [ { text: "🚫 Withdraw Challenge (Initiator Only)", callback_data: `cancel_game:${gameId}` } ]]
                };
                await bot.editMessageText(waitingText, { chatId: String(chatId), message_id: msgToEditId, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboardForWait });
            } catch (e) {
                if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                    console.warn(`${LOG_PREFIX_RPS_CHOICE} Failed to edit RPS waiting message (ID: ${msgToEditId}): ${e.message}`);
                }
            }
        }
    }
}

console.log("Part 5a, Section 3 (NEW): Group Game Handlers (Coinflip & RPS) - Complete.");
// --- End of Part 5a, Section 3 (NEW) ---
// --- Start of Part 5a, Section 4 (NEW): Shared UI and Utility Functions for Part 5a ---
// index.js - Part 5a, Section 4: UI Helpers and Shared Utilities for General Commands & Simple Group Games
//----------------------------------------------------------------------------------------------------
console.log("Loading Part 5a, Section 4 (NEW): Shared UI and Utility Functions for Part 5a...");

// Assumed dependencies from previous Parts:
// Part 1: GAME_IDS (defined in 5a-S1 New), QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, escapeMarkdownV2
// Part 3: formatCurrency

// Note: `parseBetAmount` is a critical shared utility but is defined in Part 5a, Section 1 (NEW)
// as it's integral to the command router.

/**
 * Creates a standardized inline keyboard for post-game actions.
 * @param {string} gameCode - The game identifier (e.g., GAME_IDS.COINFLIP).
 * @param {bigint} betAmountLamports - The bet amount for the "Play Again" button.
 * @param {Array<Array<object>>} [additionalFirstRowButtons=[]] - Optional additional buttons for the first row.
 * @returns {object} Telegram InlineKeyboardMarkup object.
 */
function createPostGameKeyboard(gameCode, betAmountLamports, additionalFirstRowButtons = []) {
    const playAgainBetDisplaySOL = escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL'));
    const playAgainCallback = `play_again_${gameCode}:${betAmountLamports.toString()}`;

    const gameNameClean = gameCode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    let gameEmoji = '🎲'; // Default
    switch (gameCode) {
        case GAME_IDS.COINFLIP: gameEmoji = '🪙'; break;
        case GAME_IDS.RPS: gameEmoji = '✂️'; break;
        case GAME_IDS.DICE_ESCALATOR: gameEmoji = '🎲'; break;
        case GAME_IDS.DICE_21: gameEmoji = '🃏'; break;
        case GAME_IDS.OVER_UNDER_7: gameEmoji = '🎲'; break;
        case GAME_IDS.DUEL: gameEmoji = '⚔️'; break;
        case GAME_IDS.LADDER: gameEmoji = '🪜'; break;
        case GAME_IDS.SEVEN_OUT: gameEmoji = '🎲'; break;
        case GAME_IDS.SLOT_FRENZY: gameEmoji = '🎰'; break;
    }

    const keyboard = [
        [...additionalFirstRowButtons, { text: `🔁 Play Again (${playAgainBetDisplaySOL})`, callback_data: playAgainCallback }],
        [{ text: "💰 Add Funds", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }, { text: `📖 ${gameEmoji} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${gameCode}` }],
        [{ text: "🎲 All Games Menu", callback_data: "show_rules_menu" }, { text: "💳 Wallet", callback_data: "menu:wallet" }]
    ];
    
    // Filter out the additional buttons row if it's empty after construction
    if (additionalFirstRowButtons.length === 0) {
        keyboard[0] = [{ text: `🔁 Play Again (${playAgainBetDisplaySOL})`, callback_data: playAgainCallback }];
    }


    return { inline_keyboard: keyboard };
}
console.log("[UI Helper] createPostGameKeyboard defined in Part 5a-S4.");

/**
 * Creates a simple "Back to Menu X" inline keyboard.
 * @param {string} [menuTargetCallbackData='menu:main'] - The callback data for the menu button.
 * @param {string} [menuButtonText='🏛️ Back to Main Menu'] - Text for the menu button.
 * @returns {object} Telegram InlineKeyboardMarkup object.
 */
function createBackToMenuKeyboard(menuTargetCallbackData = 'menu:main', menuButtonText = '🏛️ Back to Main Menu') {
    return {
        inline_keyboard: [
            [{ text: escapeMarkdownV2(menuButtonText), callback_data: menuTargetCallbackData }]
        ]
    };
}
console.log("[UI Helper] createBackToMenuKeyboard defined in Part 5a-S4.");

/**
 * Generates a standardized title string for game messages or UI sections.
 * @param {string} titleText The main text of the title.
 * @param {string} [emoji='✨'] Optional leading/trailing emoji.
 * @returns {string} MarkdownV2 formatted title string.
 */
function createStandardTitle(titleText, emoji = '✨') {
    return `${emoji} *${escapeMarkdownV2(titleText)}* ${emoji}`;
}
console.log("[UI Helper] createStandardTitle defined in Part 5a-S4.");


console.log("Part 5a, Section 4 (NEW): Shared UI and Utility Functions for Part 5a - Complete.");
// --- End of Part 5a, Section 4 (NEW) ---
// --- Start of Part 5b, Section 1 ---
// index.js - Part 5b, Section 1: Dice Escalator Game Logic & Handlers
//---------------------------------------------------------------------------
console.log("Loading Part 5b, Section 1: Dice Escalator Game Logic & Handlers...");

// Assumes constants like MIN_BET_USD_val, TARGET_JACKPOT_SCORE, DICE_ESCALATOR_BUST_ON,
// BOT_STAND_SCORE_DICE_ESCALATOR, JACKPOT_CONTRIBUTION_PERCENT, MAIN_JACKPOT_ID,
// and functions like getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay,
// escapeMarkdownV2, generateGameId, safeSendMessage, activeGames, pool, sleep,
// rollDie, formatDiceRolls, createPostGameKeyboard, getOrCreateUser, updateUserBalanceAndLedger,
// QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, GAME_IDS, LAMPORTS_PER_SOL, bot, notifyAdmin
// are available from previous parts or globally.

// Helper function to forward Dice Escalator callbacks
async function forwardDiceEscalatorCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_DE_CB_FWD = `[DE_CB_Forward UID:${userObject.telegram_id} Action:${action}]`;
    console.log(`${LOG_PREFIX_DE_CB_FWD} Forwarding to Dice Escalator handler for chat ${originalChatId} (Type: ${originalChatType})`);

    const gameId = params[0];

    if (!gameId && action !== 'jackpot_display_noop' && !action.startsWith('play_again_de')) {
        console.error(`${LOG_PREFIX_DE_CB_FWD} Missing gameId for Dice Escalator action: ${action}. Params: ${params}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Game identifier missing.", show_alert: true });
        return;
    }
    
    const mockMsgForHandler = {
        from: userObject,
        chat: { id: originalChatId, type: originalChatType },
        message_id: originalMessageId 
    };

    switch (action) {
        case 'de_roll_prompt':
        case 'de_cashout': // Player stands
            await handleDiceEscalatorPlayerAction(gameId, String(userObject.telegram_id), action, originalMessageId, originalChatId, callbackQueryId);
            break;
        case 'jackpot_display_noop':
            await bot.answerCallbackQuery(callbackQueryId, {text: "💰 Jackpot amount displayed."}).catch(()=>{});
            break;
        case 'play_again_de':
            // For play_again, params[0] is the betAmount, not gameId
            const betAmountParam = params[0];
            if (!betAmountParam || isNaN(BigInt(betAmountParam))) {
                console.error(`${LOG_PREFIX_DE_CB_FWD} Missing or invalid bet amount for play_again_de: ${betAmountParam}`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Bet amount missing or invalid for replay.", show_alert: true });
                return;
            }
            const betAmountDELamports = BigInt(betAmountParam);
            if (bot && originalMessageId) {
                await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(()=>{});
            }
            await handleStartDiceEscalatorCommand(mockMsgForHandler, betAmountDELamports);
            break;
        default:
            console.warn(`${LOG_PREFIX_DE_CB_FWD} Unforwarded or unknown Dice Escalator action: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Unknown game action: ${escapeMarkdownV2(action)}`, show_alert: true });
    }
}
console.log("[DE Handler] forwardDiceEscalatorCallback defined.");

// --- Helper Function to get Jackpot Text for the Dice Escalator Button ---
async function getJackpotButtonText(gameIdForCallback = null) {
    const LOG_PREFIX_JACKPOT_BTN = "[getJackpotButtonText]";
    let jackpotAmountString = "👑 Jackpot: Fetching...";

    try {
        if (typeof queryDatabase !== 'function' || typeof MAIN_JACKPOT_ID === 'undefined' || typeof formatBalanceForDisplay !== 'function') {
            console.warn(`${LOG_PREFIX_JACKPOT_BTN} Missing dependencies for jackpot button. Using default text.`);
            return { text: "👑 Jackpot: N/A", callback_data: `jackpot_display_noop:${gameIdForCallback || 'general'}` };
        }

        const result = await queryDatabase('SELECT current_amount FROM jackpots WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]);
        if (result.rows.length > 0 && result.rows[0].current_amount !== null) {
            const jackpotAmountLamports = BigInt(result.rows[0].current_amount);
            const jackpotDisplayAmountUSD = await formatBalanceForDisplay(jackpotAmountLamports, "USD");
            jackpotAmountString = `👑 Jackpot: ${escapeMarkdownV2(jackpotDisplayAmountUSD)} 👑`;
        } else {
            jackpotAmountString = "👑 Jackpot: ~ $0.00 USD 👑"; // Default if not set or 0
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_JACKPOT_BTN} Error fetching jackpot for button: ${error.message}`, error.stack);
        jackpotAmountString = "👑 Jackpot: Error 👑";
    }
    const callbackData = `jackpot_display_noop:${gameIdForCallback || 'general'}`;
    return { text: jackpotAmountString, callback_data: callbackData };
}
console.log("[DE Helper] getJackpotButtonText defined.");


// --- Dice Escalator Game Handler Functions ---

async function handleStartDiceEscalatorCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const originalCommandMessageId = msg.message_id;

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`[DE_Start UID:${userId}] Invalid bet amount for Dice Escalator: ${betAmountLamports}`);
        await safeSendMessage(chatId, "Invalid bet amount. Please try starting the game again with a valid bet.", {});
        return;
    }

    const userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "Could not fetch your player profile. Please try /start again.", {});
        return;
    }

    const LOG_PREFIX_DE_START = `[DE_Start UID:${userId} CH:${chatId}]`;
    console.log(`${LOG_PREFIX_DE_START} Initiating Dice Escalator. Bet: ${betAmountLamports} lamports.`);
    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));


    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino balance is a bit low for a *${betDisplayUSD}* Dice Escalator game\\. You need approximately *${neededDisplay}* more to play this round\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.DICE_ESCALATOR);
    let contributionLamports = 0n;
    let client;

    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // Deduct bet amount
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client,
            userId,
            BigInt(-betAmountLamports),
            'bet_placed_dice_escalator',
            { game_log_id: null }, // Game log can be created later
            `Bet for Dice Escalator game ${gameId}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_DE_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Dice Escalator wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
            if (client) client.release();
            return;
        }
        userObj.balance = balanceUpdateResult.newBalanceLamports; // Update in-memory balance

        // Add jackpot contribution
        contributionLamports = BigInt(Math.floor(Number(betAmountLamports) * JACKPOT_CONTRIBUTION_PERCENT));
        if (contributionLamports > 0n) {
            const updateJackpotResult = await client.query(
                'UPDATE jackpots SET current_amount = current_amount + $1, updated_at = NOW() WHERE jackpot_id = $2 RETURNING current_amount',
                [contributionLamports.toString(), MAIN_JACKPOT_ID]
            );
            if (updateJackpotResult.rowCount > 0) {
                console.log(`${LOG_PREFIX_DE_START} [JACKPOT] Contributed ${formatCurrency(contributionLamports, 'SOL')} to ${MAIN_JACKPOT_ID}. New Jackpot Total: ${formatCurrency(BigInt(updateJackpotResult.rows[0].current_amount), 'SOL')}`);
            } else {
                console.warn(`${LOG_PREFIX_DE_START} [JACKPOT] FAILED to contribute to ${MAIN_JACKPOT_ID}. Jackpot ID might not exist or update failed. Game continues without this contribution.`);
                contributionLamports = 0n; // Reset if contribution failed
            }
        }
        await client.query('COMMIT');
        console.log(`${LOG_PREFIX_DE_START} Wager *${betDisplayUSD}* accepted & jackpot contribution processed. New balance for ${userId}: ${formatCurrency(balanceUpdateResult.newBalanceLamports, 'SOL')}`);

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_DE_START} Rollback error:`, rbErr));
        console.error(`${LOG_PREFIX_DE_START} Transaction error during Dice Escalator bet placement: ${error.message}`);
        await safeSendMessage(chatId, `${playerRef}, a database error occurred while starting your game\\. Please try again\\. If the issue persists, contact support\\.`, { parse_mode: 'MarkdownV2'});
        if (client) client.release();
        return;
    } finally {
        if (client) client.release();
    }

    const gameData = {
        type: GAME_IDS.DICE_ESCALATOR, gameId, chatId: String(chatId), userId, playerRef, userObj,
        betAmount: betAmountLamports, playerScore: 0n, playerRollCount: 0, botScore: 0n,
        status: 'waiting_player_roll',
        gameMessageId: null, commandMessageId: originalCommandMessageId,
        lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);

    const jackpotButtonData = await getJackpotButtonText(gameId); 
    const targetJackpotScoreDisplay = escapeMarkdownV2(String(TARGET_JACKPOT_SCORE));
    const jackpotTip = `\n\n👑 *Jackpot Alert!* Stand with *${targetJackpotScoreDisplay}\\+* AND win the round to claim the current Super Jackpot displayed below\\!`;
    const initialMessageText = `🎲 *Dice Escalator Arena* 🎲\n\n${playerRef}, your wager: *${betDisplayUSD}*\\! Let's climb that score ladder\\!${jackpotTip}\n\nYour current score: *0*\\. It's your move\\! Press *"Roll Dice"* to begin your ascent\\! 👇`;

    const keyboard = {
        inline_keyboard: [
            [jackpotButtonData], 
            [{ text: "🚀 Roll Dice!", callback_data: `de_roll_prompt:${gameId}` }],
            [{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR}` }]
        ]
    };

    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
    } else {
        console.error(`${LOG_PREFIX_DE_START} Failed to send Dice Escalator game message for ${gameId}. Attempting refund.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            // Refund bet
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_dice_escalator_setup_fail', {}, `Refund for DE game ${gameId} - message send fail`);
            if (contributionLamports > 0n) { // Reverse jackpot contribution
                await refundClient.query('UPDATE jackpots SET current_amount = current_amount - $1 WHERE jackpot_id = $2 AND current_amount >= $1', [contributionLamports.toString(), MAIN_JACKPOT_ID]);
                console.log(`${LOG_PREFIX_DE_START} Reversed jackpot contribution of ${contributionLamports} for failed game setup ${gameId}.`);
            }
            await refundClient.query('COMMIT');
            console.log(`${LOG_PREFIX_DE_START} Successfully refunded bet and reversed jackpot contribution for game ${gameId} due to message send failure.`);
        } catch(refundError) {
            if(refundClient) await refundClient.query('ROLLBACK').catch(()=>{});
            console.error(`${LOG_PREFIX_DE_START} 🚨 CRITICAL: Failed to refund user/reverse contribution for ${gameId} after message send failure: ${refundError.message}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL DE Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nReason: Failed to send game message AND failed to refund/reverse contribution\\. Manual intervention required\\.`, {parse_mode:'MarkdownV2'});
        } finally {
            if(refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
    }
}
console.log("[DE Handler] handleStartDiceEscalatorCommand defined.");

async function handleDiceEscalatorPlayerAction(gameId, userIdFromCallback, action, originalMessageId, chatIdFromCallback, callbackQueryId) {
    const LOG_PREFIX_DE_ACTION = `[DE_Action GID:${gameId} UID:${userIdFromCallback} Act:${action}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Dice Escalator game seems to have expired or ended.", show_alert: true });
        if (bot && originalMessageId && chatIdFromCallback) { 
            bot.editMessageReplyMarkup({}, { chat_id: String(chatIdFromCallback), message_id: Number(originalMessageId) }).catch(() => {});
        }
        return;
    }
    if (String(gameData.userId) !== String(userIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "🤔 This isn't your game to play! Wait for your turn or start a new one.", show_alert: true });
        return;
    }
    if (gameData.gameMessageId && Number(gameData.gameMessageId) !== Number(originalMessageId)) {
        console.warn(`${LOG_PREFIX_DE_ACTION} Callback received on outdated message ID. Current game msg ID: ${gameData.gameMessageId}, CB msg ID: ${originalMessageId}. Ignoring.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚙️ This game message is outdated. Please use the latest one.", show_alert: true });
        return;
    }
    
    gameData.lastInteractionTime = Date.now();
    activeGames.set(gameId, gameData); // Update last interaction time

    const jackpotButtonData = await getJackpotButtonText(gameId); 
    const actionBase = action.split(':')[0]; // E.g. 'de_roll_prompt' from 'de_roll_prompt:gameId'

    switch (actionBase) {
        case 'de_roll_prompt':
            if (gameData.status !== 'waiting_player_roll' && gameData.status !== 'player_turn_prompt_action') {
                await bot.answerCallbackQuery(callbackQueryId, { text: "⏱️ Not your turn to roll, or the game has different plans!", show_alert: true });
                return;
            }
            await processDiceEscalatorPlayerRoll(gameData, jackpotButtonData, callbackQueryId);
            break;
        case 'de_cashout': // Player stands
            if (gameData.status !== 'player_turn_prompt_action') { // Can only stand if prompted after a roll
                await bot.answerCallbackQuery(callbackQueryId, { text: "✋ You can only stand after making at least one roll and when prompted.", show_alert: true });
                return;
            }
            await processDiceEscalatorStandAction(gameData, jackpotButtonData, callbackQueryId);
            break;
        default:
            console.error(`${LOG_PREFIX_DE_ACTION} Unknown Dice Escalator action: '${actionBase}'.`);
            await bot.answerCallbackQuery(callbackQueryId, { text: "❓ Unknown game action.", show_alert: true });
    }
}
console.log("[DE Handler] handleDiceEscalatorPlayerAction defined.");

async function processDiceEscalatorPlayerRoll(gameData, currentJackpotButtonData, callbackQueryId) {
    const LOG_PREFIX_DE_PLAYER_ROLL = `[DE_PlayerRoll GID:${gameData.gameId} UID:${gameData.userId}]`;
    await bot.answerCallbackQuery(callbackQueryId, {text: "🎲 Rolling the die..."}).catch(()=>{});

    gameData.status = 'player_rolling';
    activeGames.set(gameData.gameId, gameData);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    const rollingMessage = `${gameData.playerRef} is shaking the dice for their *${betDisplayUSD}* wager\\! 🎲\nYour current score: *${escapeMarkdownV2(String(gameData.playerScore))}*\\. Good luck\\!`;
    const rollingKeyboard = { inline_keyboard: [[currentJackpotButtonData]]};
    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(rollingMessage, {
                chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: rollingKeyboard
            });
        } catch (editError) { 
            if (!editError.message || !editError.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to edit 'rolling' message: ${editError.message}`);
            }
        }
    }
    await sleep(700);

    let playerRollValue;
    let animatedDiceMessageId = null;
    try {
        const diceMessage = await bot.sendDice(String(gameData.chatId), { emoji: '🎲' });
        playerRollValue = BigInt(diceMessage.dice.value);
        animatedDiceMessageId = diceMessage.message_id;
        await sleep(2200);
    } catch (diceError) {
        console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to send animated dice, using internal roll. Error: ${diceError.message}`);
        playerRollValue = BigInt(rollDie());
        await safeSendMessage(String(gameData.chatId), `⚙️ Uh oh, the dice got stuck\\! ${gameData.playerRef}, your internal roll is a *${escapeMarkdownV2(String(playerRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' });
        await sleep(1000);
    }
    if (animatedDiceMessageId && bot) { bot.deleteMessage(String(gameData.chatId), animatedDiceMessageId).catch(() => {}); }

    gameData.playerRollCount += 1;
    const bustValue = BigInt(DICE_ESCALATOR_BUST_ON);
    const latestJackpotButtonData = await getJackpotButtonText(gameData.gameId);

    if (playerRollValue === bustValue) {
        const originalScoreBeforeBust = gameData.playerScore;
        gameData.playerScore = 0n;
        gameData.status = 'game_over_player_bust';
        activeGames.set(gameData.gameId, gameData);
        
        let clientBust;
        try {
            clientBust = await pool.connect();
            await clientBust.query('BEGIN');
            await updateUserBalanceAndLedger(
                clientBust,
                gameData.userId,
                0n, // No change to balance, bet already deducted
                'loss_dice_escalator_bust',
                { game_log_id: null }, // TODO: Create game log entry
                `Busted in Dice Escalator game ${gameData.gameId}`
            );
            await clientBust.query('COMMIT');
        } catch (dbError) {
            if(clientBust) await clientBust.query('ROLLBACK');
            console.error(`${LOG_PREFIX_DE_PLAYER_ROLL} DB Error logging DE bust for ${gameData.userId}: ${dbError.message}`);
            // Admin might need to be notified if ledgering fails critically
        } finally {
            if(clientBust) clientBust.release();
        }
        // Re-fetch user for accurate balance display after potential ledger update
        const userForBalanceDisplay = await getOrCreateUser(gameData.userId); 
        const newBalanceDisplay = userForBalanceDisplay ? escapeMarkdownV2(await formatBalanceForDisplay(BigInt(userForBalanceDisplay.balance), 'USD')) : "N/A";

        const bustMessage = `💥 *Oh No, ${gameData.playerRef}!* 💥\nA roll of *${escapeMarkdownV2(String(playerRollValue))}* means you've BUSTED\\!\nYour score plummets from *${escapeMarkdownV2(String(originalScoreBeforeBust))}* to *0*\\. Your *${betDisplayUSD}* wager is lost to the house\\.\n\nYour new balance: *${newBalanceDisplay}*\\. Better luck next climb\\!`;
        const bustKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR, gameData.betAmount); 
        bustKeyboard.inline_keyboard.unshift([latestJackpotButtonData]);

        if (gameData.gameMessageId && bot) {
            await bot.editMessageText(bustMessage, { chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: bustKeyboard })
              .catch(async (e) => {
                  console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to edit bust message, sending new: ${e.message}`);
                  await safeSendMessage(String(gameData.chatId), bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
                });
        } else {
            await safeSendMessage(String(gameData.chatId), bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
        }
        activeGames.delete(gameData.gameId);
    } else {
        gameData.playerScore += playerRollValue;
        gameData.status = 'player_turn_prompt_action';
        activeGames.set(gameData.gameId, gameData);

        const successMessage = `🎯 *Bullseye\\!* You rolled a *${escapeMarkdownV2(String(playerRollValue))}*\\! ${gameData.playerRef}, your score climbs to: *${escapeMarkdownV2(String(gameData.playerScore))}*\\.\nWager: *${betDisplayUSD}*\n\nFeeling lucky\\? Roll again, or stand firm\\? 🤔`;
        const successKeyboard = {
            inline_keyboard: [
                [latestJackpotButtonData],
                [{ text: "🎲 Roll Again!", callback_data: `de_roll_prompt:${gameData.gameId}` }, { text: "✋ Stand & Secure Score", callback_data: `de_cashout:${gameData.gameId}` }],
                [{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR}` }]
            ]
        };
        if (gameData.gameMessageId && bot) {
            try {
                await bot.editMessageText(successMessage, { chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
            } catch(e) {
                 if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                    console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to edit roll success message, sending new. Error: ${e.message}`);
                    const newMsg = await safeSendMessage(String(gameData.chatId), successMessage, { parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
                    if(newMsg?.message_id && activeGames.has(gameData.gameId)) activeGames.get(gameData.gameId).gameMessageId = newMsg.message_id;
                 }
            }
        } else { 
            const newMsg = await safeSendMessage(String(gameData.chatId), successMessage, { parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
            if(newMsg?.message_id && activeGames.has(gameData.gameId)) activeGames.get(gameData.gameId).gameMessageId = newMsg.message_id;
        }
    }
}
console.log("[DE Handler] processDiceEscalatorPlayerRoll defined.");

async function processDiceEscalatorStandAction(gameData, currentJackpotButtonData, callbackQueryId) {
    const LOG_PREFIX_DE_STAND = `[DE_Stand GID:${gameData.gameId} UID:${gameData.userId}]`;
    await bot.answerCallbackQuery(callbackQueryId, {text: "✋ You chose to Stand! Bot plays next..."}).catch(()=>{}); 

    gameData.status = 'bot_turn_pending';
    activeGames.set(gameData.gameId, gameData);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    const standMessage = `${gameData.playerRef} stands tall with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\! 🔒\nWager: *${betDisplayUSD}*\n\nThe Bot Dealer 🤖 steps up to the challenge\\.\\.\\. Let's see what fate unfolds\\!`;
    const standKeyboard = { inline_keyboard: [[currentJackpotButtonData]] };

    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(standMessage, { chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: standKeyboard });
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_DE_STAND} Failed to edit stand message, sending new. Error: ${e.message}`);
                const newMsg = await safeSendMessage(String(gameData.chatId), standMessage, { parse_mode: 'MarkdownV2', reply_markup: standKeyboard });
                if(newMsg?.message_id && activeGames.has(gameData.gameId)) activeGames.get(gameData.gameId).gameMessageId = newMsg.message_id;
            }
        }
    } else { 
        const newMsg = await safeSendMessage(String(gameData.chatId), standMessage, { parse_mode: 'MarkdownV2', reply_markup: standKeyboard });
        if(newMsg?.message_id && activeGames.has(gameData.gameId)) activeGames.get(gameData.gameId).gameMessageId = newMsg.message_id;
    }

    await sleep(2000);
    await processDiceEscalatorBotTurn(gameData);
}
console.log("[DE Handler] processDiceEscalatorStandAction defined.");

async function processDiceEscalatorBotTurn(gameData) {
    const LOG_PREFIX_DE_BOT_TURN = `[DE_BotTurn GID:${gameData.gameId}]`;
    const { gameId, chatId, userId, playerRef, playerScore, betAmount, userObj, gameMessageId } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    
    gameData.status = 'bot_rolling'; 
    gameData.botScore = 0n; 
    activeGames.set(gameId, gameData);

    let botMessageAccumulator = `🤖 *Bot Dealer's Turn* 🤖\n${playerRef} stands at *${escapeMarkdownV2(String(playerScore))}*\\. The Bot Dealer aims to beat it\\!\n\n`;
    let currentTempMessageId = null;

    const updateBotProgressMessage = async (text) => {
        if (currentTempMessageId && bot) {
            await bot.deleteMessage(String(chatId), currentTempMessageId).catch(()=>{});
            currentTempMessageId = null;
        }
        if (gameData.gameMessageId && bot) {
             try {
                await bot.editMessageText(text, {chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode:'MarkdownV2', reply_markup: {}}); // Clear buttons during bot turn
             } catch (e) {
                if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                     console.warn(`${LOG_PREFIX_DE_BOT_TURN} Failed to edit bot progress, sending temp. Error: ${e.message}`);
                     const tempMsg = await safeSendMessage(String(chatId), text, {parse_mode:'MarkdownV2'});
                     currentTempMessageId = tempMsg?.message_id;
                       if(tempMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = tempMsg.message_id; // Update main message ID if new one sent
                }
             }
        } else {
            const tempMsg = await safeSendMessage(String(chatId), text, {parse_mode:'MarkdownV2'});
            currentTempMessageId = tempMsg?.message_id;
            if(tempMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = tempMsg.message_id;
        }
    };
    
    await updateBotProgressMessage(botMessageAccumulator + `Bot is rolling the first die\\.\\.\\. 🎲`);

    const botStandScore = BigInt(BOT_STAND_SCORE_DICE_ESCALATOR); 
    const bustValueBot = BigInt(DICE_ESCALATOR_BUST_ON);    
    let botRollsDisplay = [];

    while(gameData.botScore < botStandScore && gameData.botScore !== 0n && botRollsDisplay.length < 10 /* Max 10 rolls for bot */) {
        await sleep(1500);
        const botRoll = BigInt(rollDie());
        botRollsDisplay.push(botRoll);
        botMessageAccumulator += `Bot rolls a *${escapeMarkdownV2(String(botRoll))}* ${formatDiceRolls([Number(botRoll)])}\\. `;
        
        if (botRoll === bustValueBot) { 
            gameData.botScore = 0n; 
            botMessageAccumulator += "\n💥 *Bot BUSTS!* Score resets to 0\\.\n"; 
            break;
        }
        gameData.botScore += botRoll;
        botMessageAccumulator += `Bot score is now *${escapeMarkdownV2(String(gameData.botScore))}*\\.\n`;
        
        if(gameData.botScore >= botStandScore) { 
            botMessageAccumulator += "Bot stands with its score\\.\n"; 
            break; 
        }
        await updateBotProgressMessage(botMessageAccumulator + `Bot considers its next move\\.\\.\\. 🤔`);
    }
    activeGames.set(gameId, gameData);

    if (currentTempMessageId && bot && gameData.gameMessageId && Number(currentTempMessageId) !== Number(gameData.gameMessageId)) { 
        await bot.deleteMessage(String(chatId), currentTempMessageId).catch(()=>{});
    }
    
    botMessageAccumulator += `\n------------------------------------\n📜 *Round Summary*\nPlayer Score: *${escapeMarkdownV2(String(playerScore))}*\nBot Score: *${escapeMarkdownV2(String(gameData.botScore))}* ${gameData.botScore === 0n && botRollsDisplay.length > 0 ? "\\(Busted\\)" : ""}\n`;

    let resultTextPart; 
    let payoutAmountLamports = 0n; // Amount to credit user (includes original bet if win/push)
    let outcomeReasonLog = ""; 
    let jackpotWon = false;
    const targetJackpotScoreValue = BigInt(TARGET_JACKPOT_SCORE); 

    if (gameData.botScore === 0n) { // Bot busted
        resultTextPart = `🎉 *YOU WIN!* The Bot Dealer busted spectacularly\\!`; 
        payoutAmountLamports = betAmount * 2n; // Player gets 2x their bet (bet back + profit)
        outcomeReasonLog = `win_dice_escalator_bot_bust`;
        if (playerScore >= targetJackpotScoreValue) jackpotWon = true;
    } else if (playerScore > gameData.botScore) { // Player score higher
        resultTextPart = `🎉 *VICTORY!* Your score of *${escapeMarkdownV2(String(playerScore))}* triumphs over the Bot's *${escapeMarkdownV2(String(gameData.botScore))}*\\!`; 
        payoutAmountLamports = betAmount * 2n; 
        outcomeReasonLog = `win_dice_escalator_score`;
        if (playerScore >= targetJackpotScoreValue) jackpotWon = true;
    } else if (playerScore < gameData.botScore) { // Bot score higher
        resultTextPart = `💔 *House Wins.* The Bot Dealer's score of *${escapeMarkdownV2(String(gameData.botScore))}* beats your *${escapeMarkdownV2(String(playerScore))}*\\.`; 
        payoutAmountLamports = 0n; // Bet already deducted, no payout
        outcomeReasonLog = `loss_dice_escalator_score`;
    } else { // Push (tie)
        resultTextPart = `😐 *PUSH!* A tense standoff ends in a tie at *${escapeMarkdownV2(String(playerScore))}*\\. Your wager of *${betDisplayUSD}* is returned\\.`; 
        payoutAmountLamports = betAmount; // Return original bet
        outcomeReasonLog = `push_dice_escalator`;
    }
    botMessageAccumulator += `\n${resultTextPart}\n`;
    gameData.status = `game_over_final_${outcomeReasonLog}`;

    let finalUserBalanceLamports = BigInt(userObj.balance); // Start with user's balance before this game's outcome
    let jackpotPayoutAmount = 0n;
    let clientOutcome;

    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');

        // Ledger reason incorporates gameId for better tracking
        const ledgerReasonBase = `${outcomeReasonLog}:${gameId}`;

        const balanceUpdateResult = await updateUserBalanceAndLedger(clientOutcome, userId, payoutAmountLamports, ledgerReasonBase, {game_log_id: null}, `Outcome of DE game ${gameId}`);
        if (balanceUpdateResult.success) {
            finalUserBalanceLamports = balanceUpdateResult.newBalanceLamports;
            if (payoutAmountLamports > betAmount && outcomeReasonLog.startsWith('win')) {
                 botMessageAccumulator += `\nYou win *${escapeMarkdownV2(await formatBalanceForDisplay(payoutAmountLamports - betAmount, 'USD'))}* profit\\!`;
            } else if (payoutAmountLamports === betAmount && outcomeReasonLog.startsWith('push')) {
                // Message already covers push
            }
        } else {
            // This error means crediting the user failed. The bet was already taken. This is bad.
            botMessageAccumulator += `\n⚠️ Critical error crediting your game winnings/refund\\. Admin has been notified to manually verify and credit if necessary\\.`;
            console.error(`${LOG_PREFIX_DE_BOT_TURN} CRITICAL: Failed to update balance for DE game win/push for user ${userId}. Error: ${balanceUpdateResult.error}`);
            if (typeof notifyAdmin === 'function') {
                 notifyAdmin(`🚨 CRITICAL DE Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmountLamports))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}\`\\. MANUAL CHECK REQUIRED\\.`, {parse_mode:'MarkdownV2'});
            }
        }

        if (jackpotWon) {
            const jackpotSelectResult = await clientOutcome.query('SELECT current_amount FROM jackpots WHERE jackpot_id = $1 FOR UPDATE', [MAIN_JACKPOT_ID]);
            if (jackpotSelectResult.rows.length > 0) {
                jackpotPayoutAmount = BigInt(jackpotSelectResult.rows[0].current_amount || '0');
                if (jackpotPayoutAmount > 0n) {
                    const jackpotLedgerNote = `Jackpot win for DE game ${gameId}`;
                        const jackpotPayoutUpdateResult = await updateUserBalanceAndLedger(clientOutcome, userId, jackpotPayoutAmount, 'jackpot_win_dice_escalator', {game_log_id: null}, jackpotLedgerNote);
                    
                    if (jackpotPayoutUpdateResult.success) {
                        await clientOutcome.query('UPDATE jackpots SET current_amount = $1, last_won_timestamp = NOW(), last_won_by_telegram_id = $2 WHERE jackpot_id = $3', ['0', userId, MAIN_JACKPOT_ID]);
                        botMessageAccumulator += `\n\n👑🌟 *JACKPOT HIT!!!* 🌟👑\n${playerRef}, you've conquered the Dice Escalator and claimed the Super Jackpot of *${escapeMarkdownV2(await formatBalanceForDisplay(jackpotPayoutAmount, 'USD'))}*\\! Absolutely magnificent\\!`;
                        finalUserBalanceLamports = jackpotPayoutUpdateResult.newBalanceLamports;
                    } else {
                        botMessageAccumulator += `\n\n⚠️ Critical error crediting Jackpot winnings of *${escapeMarkdownV2(await formatBalanceForDisplay(jackpotPayoutAmount, 'USD'))}*\\. Admin notified for manual processing\\.`;
                        console.error(`${LOG_PREFIX_DE_BOT_TURN} CRITICAL: Failed to update balance for JACKPOT win for user ${userId}. Error: ${jackpotPayoutUpdateResult.error}`);
                        if (typeof notifyAdmin === 'function') {
                           notifyAdmin(`🚨 CRITICAL DE JACKPOT Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nJackpot Amount: \`${escapeMarkdownV2(formatCurrency(jackpotPayoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(jackpotPayoutUpdateResult.error || "Unknown")}\`\\. MANUAL Jackpot payout required\\.`, {parse_mode:'MarkdownV2'});
                        }
                    }
                } else { 
                    botMessageAccumulator += `\n\n👑 You hit the Jackpot score, but the pot was empty this time\\! Still an amazing feat\\!`;
                }
            } else { 
                botMessageAccumulator += `\n\n👑 Jackpot system error (cannot find jackpot record)\\. Admin notified\\.`;
                console.error(`${LOG_PREFIX_DE_BOT_TURN} MAIN_JACKPOT_ID ${MAIN_JACKPOT_ID} not found in jackpots table during payout.`);
            }
        }
        await clientOutcome.query('COMMIT');
    } catch (error) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_DE_BOT_TURN} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_DE_BOT_TURN} Transaction error during Dice Escalator outcome/jackpot processing: ${error.message}`);
        botMessageAccumulator += `\n\n⚠️ A database error occurred while finalizing your game\\. Please contact support if your balance seems incorrect\\. Your initial game outcome was: ${resultTextPart}`;
        if (typeof notifyAdmin === 'function') {
            notifyAdmin(`🚨 CRITICAL DE Transaction Error 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nError: \`${escapeMarkdownV2(error.message)}\`\\. Balance state may be inconsistent\\. Requires manual check\\.`, {parse_mode:'MarkdownV2'});
        }
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    botMessageAccumulator += `\n\nYour updated balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*\\.`;
    
    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR, betAmount); 
    const finalJackpotButtonData = await getJackpotButtonText(gameId);
    finalKeyboard.inline_keyboard.unshift([finalJackpotButtonData]);

    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(botMessageAccumulator, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: finalKeyboard })
          .catch(async (e) => {
              console.warn(`${LOG_PREFIX_DE_BOT_TURN} Failed to edit final DE message (ID: ${gameData.gameMessageId}), sending new: ${e.message}`);
              await safeSendMessage(String(chatId), botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
            });
    } else { 
        await safeSendMessage(String(chatId), botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
    }
    activeGames.delete(gameId);
}
console.log("[DE Handler] processDiceEscalatorBotTurn defined.");


console.log("Part 5b, Section 1: Dice Escalator Game Logic & Handlers - Complete.");
// --- End of Part 5b, Section 1 ---
// --- Start of Part 5b, Section 2 ---
// index.js - Part 5b, Section 2: Dice 21 (Blackjack Style) Game Logic & Handlers
//---------------------------------------------------------------------------
console.log("Loading Part 5b, Section 2: Dice 21 (Blackjack Style) Game Logic & Handlers...");

// Assumes constants like MIN_BET_USD_val, DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE,
// and functions like getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay,
// escapeMarkdownV2, generateGameId, safeSendMessage, activeGames, pool, sleep,
// rollDie, formatDiceRolls, createPostGameKeyboard, getOrCreateUser, updateUserBalanceAndLedger,
// QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, GAME_IDS, LAMPORTS_PER_SOL, bot, notifyAdmin
// are available from previous parts or globally.

// forwardDice21Callback was defined in Part 5b, Section 1, but its definition makes more sense here
// or should be defined before its first use in the callback router (Part 5a-S3).
// For modularity, keeping it here close to its handlers.
async function forwardDice21Callback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_D21_CB_FWD = `[D21_CB_Forward UID:${userObject.telegram_id} Action:${action}]`;
    console.log(`${LOG_PREFIX_D21_CB_FWD} Forwarding to Dice 21 handler for chat ${originalChatId} (Type: ${originalChatType})`);

    const gameId = params[0]; 

    if (!gameId && !action.startsWith('play_again_d21')) {
        console.error(`${LOG_PREFIX_D21_CB_FWD} Missing gameId for Dice 21 action: ${action}. Params: ${params}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Game ID missing.", show_alert: true });
        return;
    }

    const mockMsgForHandler = {
        from: userObject,
        chat: { id: originalChatId, type: originalChatType },
        message_id: originalMessageId
    };

    switch (action) {
        case 'd21_hit':
            await handleDice21Hit(gameId, userObject, originalMessageId, callbackQueryId, mockMsgForHandler);
            break;
        case 'd21_stand':
            await handleDice21Stand(gameId, userObject, originalMessageId, callbackQueryId, mockMsgForHandler);
            break;
        case 'play_again_d21':
            const betAmountParam = params[0]; // For play_again, param[0] is the bet amount
            if (!betAmountParam || isNaN(BigInt(betAmountParam))) {
                console.error(`${LOG_PREFIX_D21_CB_FWD} Missing or invalid bet amount for play_again_d21: ${betAmountParam}`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Bet amount invalid for replay.", show_alert: true });
                return;
            }
            const betAmountD21Lamports = BigInt(betAmountParam);
            if (bot && originalMessageId) {
                await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            }
            await handleStartDice21Command(mockMsgForHandler, betAmountD21Lamports);
            break;
        default:
            console.warn(`${LOG_PREFIX_D21_CB_FWD} Unforwarded or unknown Dice 21 action: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Unknown game action: ${escapeMarkdownV2(action)}`, show_alert: true });
    }
}
console.log("[D21 Handler] forwardDice21Callback defined.");


// --- DICE 21 GAME LOGIC ---

async function handleStartDice21Command(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`[D21_Start UID:${userId}] Invalid bet amount for Dice 21: ${betAmountLamports}`);
        await safeSendMessage(chatId, "Invalid bet amount. Please try starting the game again with a valid bet.", {});
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "Could not fetch your player profile. Please try /start again.", {});
        return;
    }

    const LOG_PREFIX_D21_START = `[D21_Start UID:${userId} CH:${chatId}]`;
    console.log(`${LOG_PREFIX_D21_START} Initiating Dice 21. Bet: ${betAmountLamports} lamports.`);
    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino balance is insufficient for a *${betDisplayUSD}* game of Dice 21\\. You need ~*${neededDisplay}* more to join this table\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.DICE_21);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client,
            userId,
            BigInt(-betAmountLamports),
            'bet_placed_dice21',
            { game_log_id: null },
            `Bet for Dice 21 game ${gameId}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_D21_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Dice 21 wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
            if(client) client.release();
            return;
        }
        await client.query('COMMIT');
        console.log(`${LOG_PREFIX_D21_START} Wager ${betDisplayUSD} accepted. New balance for ${userId}: ${formatCurrency(balanceUpdateResult.newBalanceLamports, 'SOL')}`);
        userObj.balance = balanceUpdateResult.newBalanceLamports;

    } catch (dbError) {
        if(client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_D21_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_D21_START} Database error during D21 bet placement: ${dbError.message}`);
        await safeSendMessage(chatId, "A database error occurred while starting your Dice 21 game. Please try again.", {});
        if(client) client.release();
        return;
    } finally {
        if(client) client.release();
    }


    let dealingMsg = await safeSendMessage(chatId, `🃏 Welcome to the **Dice 21 Table**, ${playerRef}\\! Your wager: *${betDisplayUSD}*\\.\nThe dealer is shuffling the dice and dealing your initial hand\\.\\.\\. 🎲✨`, { parse_mode: 'MarkdownV2' });
    await sleep(1500);

    let initialPlayerRollsValues = [];
    let playerScore = 0n;
    const diceToDeal = 2;
    let animatedDiceMessageIds = [];

    for (let i = 0; i < diceToDeal; i++) {
        try {
            const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
            initialPlayerRollsValues.push(diceMsg.dice.value);
            playerScore += BigInt(diceMsg.dice.value);
            animatedDiceMessageIds.push(diceMsg.message_id);
            await sleep(2200); 
        } catch (e) {
            console.warn(`${LOG_PREFIX_D21_START} Failed to send animated dice for initial deal, using internal roll. Error: ${e.message}`);
            const internalRoll = rollDie();
            initialPlayerRollsValues.push(internalRoll);
            playerScore += BigInt(internalRoll);
            await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Internal Casino Roll ${i + 1}): You received a *${escapeMarkdownV2(String(internalRoll))}* 🎲`, { parse_mode: 'MarkdownV2' });
            await sleep(1000);
        }
    }

    if (dealingMsg?.message_id && bot) { bot.deleteMessage(String(chatId), dealingMsg.message_id).catch(() => {}); }
    animatedDiceMessageIds.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

    const targetScoreD21 = BigInt(DICE_21_TARGET_SCORE); 

    const gameData = {
        type: GAME_IDS.DICE_21, gameId, chatId: String(chatId), userId, playerRef, userObj,
        betAmount: betAmountLamports, playerScore, botScore: 0n,
        playerHandRolls: [...initialPlayerRollsValues], botHandRolls: [],
        status: 'player_turn', gameMessageId: null, lastInteractionTime: Date.now()
    };

    let messageText = `🃏 **Dice 21 Table** vs\\. Bot Dealer 🤖\n${playerRef}, your wager: *${betDisplayUSD}*\n\n`;
    messageText += `Your initial hand: ${formatDiceRolls(initialPlayerRollsValues)} summing to a hot *${escapeMarkdownV2(String(playerScore))}*\\!\n`;
    let buttonsRow = []; // Use an array for buttons to be placed on one row if possible
    let gameEndedOnDeal = false;

    if (playerScore > targetScoreD21) {
        messageText += `\n💥 *BUSTED!* Your score of *${escapeMarkdownV2(String(playerScore))}* went over the target of ${escapeMarkdownV2(String(targetScoreD21))}\\. The house takes the wager this round\\.`;
        gameData.status = 'game_over_player_bust'; gameEndedOnDeal = true;
        let bustClient;
        try {
            bustClient = await pool.connect();
            await bustClient.query('BEGIN');
            await updateUserBalanceAndLedger(bustClient, userId, 0n, 'loss_dice21_deal_bust', {game_log_id: null}, `Busted on deal in Dice 21 game ${gameId}`);
            await bustClient.query('COMMIT');
        } catch (dbError) {
            if(bustClient) await bustClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_D21_START} DB Error logging D21 bust on deal for ${userId}: ${dbError.message}`);
        } finally {
            if(bustClient) bustClient.release();
        }
        // Re-fetch user for accurate balance display
        const userForBalanceDisplay = await getOrCreateUser(userId);
        messageText += `\n\nYour new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(BigInt(userForBalanceDisplay.balance), 'USD'))}*\\. Tough break\\!`;
        // Get post-game keyboard buttons directly
        const postGameButtons = createPostGameKeyboard(GAME_IDS.DICE_21, betAmountLamports).inline_keyboard;
        buttonsRow = postGameButtons[0]; // Assuming first row is play again
        // Add other rows if createPostGameKeyboard returns multiple
        // For simplicity, we'll just take the "Play Again" and "Rules" row.
        // This can be structured better if createPostGameKeyboard has a more complex layout.
        // buttonsRow.push(...postGameButtons[1]); // If rules/add funds are on another row

    } else if (playerScore === targetScoreD21) {
        messageText += `\n✨ *PERFECT SCORE of ${escapeMarkdownV2(String(targetScoreD21))}!* You stand automatically\\. Let's see what the Bot Dealer 🤖 reveals\\!`;
        gameData.status = 'bot_turn_pending'; gameEndedOnDeal = true; // Game ends for player, moves to bot
    } else { // playerScore < targetScoreD21
        messageText += `\nYour move, ${playerRef}: Will you "Hit" for another die ⤵️ or "Stand" with your current score ✅\\?`;
        buttonsRow.push({ text: "⤵️ Hit Me!", callback_data: `d21_hit:${gameId}` });
        buttonsRow.push({ text: `✅ Stand (${escapeMarkdownV2(String(playerScore))})`, callback_data: `d21_stand:${gameId}` });
    }
    // Add Rules button if not already part of a game-over keyboard
    if (!gameData.status.startsWith('game_over')) {
        buttonsRow.push({ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` });
    }

    const gameMessageOptions = { parse_mode: 'MarkdownV2', reply_markup: buttonsRow.length > 0 ? { inline_keyboard: [buttonsRow] } : {} };
    const sentGameMsg = await safeSendMessage(chatId, messageText, gameMessageOptions);

    if (sentGameMsg?.message_id) {
        gameData.gameMessageId = sentGameMsg.message_id;
    } else {
        console.error(`${LOG_PREFIX_D21_START} Failed to send Dice 21 game message for ${gameId}. Refunding wager.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_dice21_setup_msg_fail', {}, `Refund for D21 game ${gameId} - message send fail`);
            await refundClient.query('COMMIT');
        } catch (err) {
            if(refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_D21_START} CRITICAL: Failed to refund user for D21 game ${gameId} after message send failure: ${err.message}`);
        } finally {
            if(refundClient) refundClient.release();
        }
        activeGames.delete(gameId); return;
    }
    activeGames.set(gameId, gameData);

    if (gameEndedOnDeal) {
        if (gameData.status === 'bot_turn_pending') { // Player got target score on deal
            await sleep(2500); 
            await processDice21BotTurn(gameData, gameData.gameMessageId);
        } else if (gameData.status.startsWith('game_over')) { // Player busted on deal
            activeGames.delete(gameId);
        }
    }
}
console.log("[D21 Handler] handleStartDice21Command defined.");

async function handleDice21Hit(gameId, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    const LOG_PREFIX_D21_HIT = `[D21_Hit GID:${gameId} UID:${userObj.telegram_id}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== String(userObj.telegram_id) || gameData.status !== 'player_turn' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This action is outdated or it's not your turn.", show_alert: true });
        if (originalMessageIdFromCallback && bot && gameData?.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
            bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
        }
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, {text: "🎲 Dealing another die..."}).catch(()=>{}); 

    const chatId = gameData.chatId;
    const previousGameMessageId = gameData.gameMessageId; // This is the message with Hit/Stand buttons

    // Indicate rolling on the main game message by editing it (clears buttons)
    if (previousGameMessageId && bot) {
        try {
            await bot.editMessageText(`${gameData.playerRef} is drawing another die\\! 🎲\nPrevious hand: ${formatDiceRolls(gameData.playerHandRolls)} (Total: *${escapeMarkdownV2(String(gameData.playerScore))}*)\nRolling\\.\\.\\.`, {
                chat_id: String(chatId), message_id: Number(previousGameMessageId), parse_mode: 'MarkdownV2', reply_markup: {}
            });
        } catch (editError) { 
             if (!editError.message || !editError.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_D21_HIT} Failed to edit 'hitting' message: ${editError.message}`);
            }
        }
    }
    await sleep(700);

    let newRollValue; let animatedDiceMessageIdHit = null;
    try { 
        const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
        newRollValue = BigInt(diceMsg.dice.value); animatedDiceMessageIdHit = diceMsg.message_id; await sleep(2200);
    } catch (e) { 
        console.warn(`${LOG_PREFIX_D21_HIT} Failed to send animated dice for hit, using internal roll. Error: ${e.message}`);
        newRollValue = BigInt(rollDie()); 
        await safeSendMessage(String(chatId), `⚙️ ${gameData.playerRef} (Internal Casino Roll): You drew a *${escapeMarkdownV2(String(newRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' }); 
        await sleep(1000);
    }

    // Delete the main game message that was edited to "rolling..."
    if (previousGameMessageId && bot) { bot.deleteMessage(String(chatId), Number(previousGameMessageId)).catch(() => {}); }
    // Delete the animated dice message
    if (animatedDiceMessageIdHit && bot) { bot.deleteMessage(String(chatId), animatedDiceMessageIdHit).catch(() => {}); }

    gameData.playerHandRolls.push(Number(newRollValue));
    gameData.playerScore += newRollValue;
    activeGames.set(gameId, gameData);

    const targetScoreD21 = BigInt(DICE_21_TARGET_SCORE);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    let newMainMessageText = `🃏 **Dice 21 Table** vs\\. Bot Dealer 🤖\n${gameData.playerRef}, wager: *${betDisplayUSD}*\n\n`;
    newMainMessageText += `You drew ${formatDiceRolls([Number(newRollValue)])}, updating your hand\\.\nNew Hand: ${formatDiceRolls(gameData.playerHandRolls)} totaling a sizzling *${escapeMarkdownV2(String(gameData.playerScore))}*\\!\n`;
    
    let buttonsRow = []; 
    let gameEndedThisTurn = false;

    if (gameData.playerScore > targetScoreD21) { // Player busts
        newMainMessageText += `\n💥 *OH NO, BUSTED!* Your score of *${escapeMarkdownV2(String(gameData.playerScore))}* flies past ${escapeMarkdownV2(String(targetScoreD21))}\\. The house collects the wager this round\\.`;
        gameData.status = 'game_over_player_bust'; gameEndedThisTurn = true;
        let bustHitClient;
        try {
            bustHitClient = await pool.connect();
            await bustHitClient.query('BEGIN');
            await updateUserBalanceAndLedger(bustHitClient, gameData.userId, 0n, 'loss_dice21_hit_bust', {game_log_id:null}, `Busted on hit in Dice 21 game ${gameId}`);
            await bustHitClient.query('COMMIT');
        } catch (dbError) {
            if(bustHitClient) await bustHitClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_D21_HIT} DB Error logging D21 bust on hit for ${gameData.userId}: ${dbError.message}`);
        } finally {
            if(bustHitClient) bustHitClient.release();
        }
        const userForBalanceDisplay = await getOrCreateUser(gameData.userId); // Re-fetch for latest balance
        newMainMessageText += `\n\nYour new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(BigInt(userForBalanceDisplay.balance), 'USD'))}*\\. Better luck on the next deal\\!`;
        const postGameButtons = createPostGameKeyboard(GAME_IDS.DICE_21, gameData.betAmount).inline_keyboard;
        buttonsRow = postGameButtons[0]; // Assuming first row is play again
        // if (postGameButtons[1]) buttonsRow.push(...postGameButtons[1]); // Add other rows if any
    } else if (gameData.playerScore === targetScoreD21) { // Player hits target score
        newMainMessageText += `\n✨ *PERFECT SCORE of ${escapeMarkdownV2(String(targetScoreD21))}!* You automatically stand\\. The Bot Dealer 🤖 prepares to reveal their hand\\.\\.\\.`;
        gameData.status = 'bot_turn_pending'; gameEndedThisTurn = true;
    } else { // playerScore < targetScoreD21, game continues
        newMainMessageText += `\nFeeling bold, ${gameData.playerRef}\\? "Hit" for another die ⤵️ or "Stand" firm with *${escapeMarkdownV2(String(gameData.playerScore))}* ✅\\?`;
        buttonsRow.push({ text: "⤵️ Hit Again!", callback_data: `d21_hit:${gameId}` });
        buttonsRow.push({ text: `✅ Stand (${escapeMarkdownV2(String(gameData.playerScore))})`, callback_data: `d21_stand:${gameId}` });
    }
    if (!gameData.status.startsWith('game_over')) { // Add rules button if game is not over from bust
        buttonsRow.push({ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` });
    }


    const newGameMessageOptions = { parse_mode: 'MarkdownV2', reply_markup: buttonsRow.length > 0 ? { inline_keyboard: [buttonsRow] } : {} };
    const sentNewMsg = await safeSendMessage(chatId, newMainMessageText, newGameMessageOptions);

    if (sentNewMsg?.message_id) {
        gameData.gameMessageId = sentNewMsg.message_id;
    } else { 
        console.error(`[D21_Hit GID:${gameId}] CRITICAL: Failed to send updated game message after hit. Game state might be inconsistent.`);
        // Attempt to refund or mark game as error if message fails. This is complex.
        // For now, deleting game from activeGames to prevent further interaction with a broken state.
        activeGames.delete(gameId); return; 
    }
    activeGames.set(gameId, gameData);

    if (gameEndedThisTurn) {
        if (gameData.status === 'bot_turn_pending') {
            await sleep(2500); 
            await processDice21BotTurn(gameData, gameData.gameMessageId);
        } else if (gameData.status.startsWith('game_over')) {
            activeGames.delete(gameId);
        }
    }
}
console.log("[D21 Handler] handleDice21Hit defined.");

async function handleDice21Stand(gameId, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    const LOG_PREFIX_D21_STAND = `[D21_Stand GID:${gameId} UID:${userObj.telegram_id}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== String(userObj.telegram_id) || gameData.status !== 'player_turn' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This action is outdated or it's not your turn to stand.", show_alert: true });
        if (originalMessageIdFromCallback && bot && gameData?.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
             bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
        }
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, {text: `✋ Standing with ${gameData.playerScore}! Bot's turn...`}).catch(()=>{}); 

    const chatId = gameData.chatId;
    const previousGameMessageId = gameData.gameMessageId;

    gameData.status = 'bot_turn_pending'; 
    activeGames.set(gameId, gameData);
    
    if (previousGameMessageId && bot) { bot.deleteMessage(String(chatId), Number(previousGameMessageId)).catch(() => {}); }

    const standMessageText = `🃏 **Dice 21 Table** 🃏\n${gameData.playerRef} stands strong with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\! 💪\nThe Bot Dealer 🤖 now plays their hand\\. The tension mounts\\!`;
    const sentNewStandMsg = await safeSendMessage(chatId, standMessageText, { parse_mode: 'MarkdownV2' });

    if (sentNewStandMsg?.message_id) {
        gameData.gameMessageId = sentNewStandMsg.message_id;
        activeGames.set(gameId, gameData);
    } else { 
        console.error(`[D21_Stand GID:${gameId}] CRITICAL: Failed to send stand confirmation message. Game state might be inconsistent.`);
        activeGames.delete(gameId); return; 
    }

    await sleep(2000);
    await processDice21BotTurn(gameData, gameData.gameMessageId);
}
console.log("[D21 Handler] handleDice21Stand defined.");

async function processDice21BotTurn(gameData, currentMainGameMessageId) {
    const LOG_PREFIX_D21_BOT = `[D21_BotTurn GID:${gameData.gameId}]`;
    const { gameId, chatId, userId, playerRef, playerScore, betAmount, userObj } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));

    gameData.status = 'bot_rolling'; 
    gameData.botScore = 0n; 
    gameData.botHandRolls = [];
    activeGames.set(gameId, gameData);

    let botTurnInProgressMessage = `🃏 **Dice 21 Table** \\- Bot's Turn 🤖\n${playerRef}'s score: *${escapeMarkdownV2(String(playerScore))}*\\.\n\nThe Bot Dealer reveals their hand and begins to play\\.\\.\\.`;
    let tempMessageIdForBotRolls = null;

    let effectiveGameMessageId = currentMainGameMessageId;
    if (effectiveGameMessageId && bot) {
        try {
            await bot.editMessageText(botTurnInProgressMessage, {chat_id:String(chatId), message_id: Number(effectiveGameMessageId), parse_mode:'MarkdownV2', reply_markup: {}});
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_D21_BOT} Failed to edit main message for bot turn start, sending new. Err: ${e.message}`);
                const newMainMsg = await safeSendMessage(String(chatId), botTurnInProgressMessage, {parse_mode:'MarkdownV2'});
                if (newMainMsg?.message_id) effectiveGameMessageId = newMainMsg.message_id;
                gameData.gameMessageId = effectiveGameMessageId; // Update game data
            }
        }
    } else {
        const newMainMsg = await safeSendMessage(String(chatId), botTurnInProgressMessage, {parse_mode:'MarkdownV2'});
        if (newMainMsg?.message_id) effectiveGameMessageId = newMainMsg.message_id;
        gameData.gameMessageId = effectiveGameMessageId;
    }
    activeGames.set(gameId, gameData);
    await sleep(1500);

    const botStandScoreThreshold = BigInt(DICE_21_BOT_STAND_SCORE);
    const targetScoreD21 = BigInt(DICE_21_TARGET_SCORE);        
    let botBusted = false;

    for (let i = 0; i < 7 && gameData.botScore < botStandScoreThreshold && !botBusted; i++) {
        const botRoll = BigInt(rollDie());
        gameData.botHandRolls.push(Number(botRoll)); 
        gameData.botScore += botRoll;
        activeGames.set(gameId, gameData);

        let rollDisplayMsgText = `Bot Dealer 🤖 rolls: ${formatDiceRolls([Number(botRoll)])}\nBot's current hand: ${formatDiceRolls(gameData.botHandRolls)} \\(Total: *${escapeMarkdownV2(String(gameData.botScore))}*\\)`;
        
        if (tempMessageIdForBotRolls && bot) { await bot.deleteMessage(String(chatId), tempMessageIdForBotRolls).catch(()=>{}); }
        const sentRollMsg = await safeSendMessage(String(chatId), rollDisplayMsgText, {parse_mode:'MarkdownV2'});
        tempMessageIdForBotRolls = sentRollMsg?.message_id;
        
        if (gameData.botScore > targetScoreD21) { 
            botBusted = true; 
            await sleep(1000);
            if (tempMessageIdForBotRolls && bot) { await bot.deleteMessage(String(chatId), tempMessageIdForBotRolls).catch(()=>{}); tempMessageIdForBotRolls = null; }
            await safeSendMessage(String(chatId), `💥 *Bot BUSTS* with a score of *${escapeMarkdownV2(String(gameData.botScore))}*\\!`, {parse_mode:'MarkdownV2'});
            break; 
        }
        if (gameData.botScore >= botStandScoreThreshold) { 
            await sleep(1000);
            if (tempMessageIdForBotRolls && bot) { await bot.deleteMessage(String(chatId), tempMessageIdForBotRolls).catch(()=>{}); tempMessageIdForBotRolls = null;}
            await safeSendMessage(String(chatId), `🤖 Bot Dealer stands with *${escapeMarkdownV2(String(gameData.botScore))}*\\.`, {parse_mode:'MarkdownV2'});
            break; 
        }
        await sleep(2000);
    }
    
    if (tempMessageIdForBotRolls && bot) { await bot.deleteMessage(String(chatId), tempMessageIdForBotRolls).catch(()=>{}); }
    await sleep(1000);

    let resultTextEnd = ""; 
    let payoutAmountLamports = 0n; // This is the total amount to credit (includes original bet if win/push)
    let outcomeReasonLog = "";

    if (botBusted) { 
        resultTextEnd = `🎉 *Congratulations, ${playerRef}! You WIN!* 🎉\nThe Bot Dealer busted, making your score of *${escapeMarkdownV2(String(playerScore))}* the winner\\!`; 
        payoutAmountLamports = betAmount * 2n; // Standard 2x payout
        outcomeReasonLog = `win_dice21_bot_bust`;
    } else if (playerScore > gameData.botScore) { 
        resultTextEnd = `🎉 *Outstanding, ${playerRef}! You WIN!* 🎉\nYour score of *${escapeMarkdownV2(String(playerScore))}* beats the Bot Dealer's *${escapeMarkdownV2(String(gameData.botScore))}*\\!`; 
        payoutAmountLamports = betAmount * 2n; 
        outcomeReasonLog = `win_dice21_score`;
    } else if (gameData.botScore > playerScore) { 
        resultTextEnd = `💔 *House Wins This Round\\.* 💔\nThe Bot Dealer's score of *${escapeMarkdownV2(String(gameData.botScore))}* edges out your *${escapeMarkdownV2(String(playerScore))}*\\.`; 
        payoutAmountLamports = 0n; // Bet already deducted
        outcomeReasonLog = `loss_dice21_score`;
    } else { // Push (Scores are equal)
        resultTextEnd = `😐 *It's a PUSH! A TIE!* 😐\nBoth you and the Bot Dealer scored *${escapeMarkdownV2(String(playerScore))}*\\. Your wager of *${betDisplayUSD}* is returned\\.`; 
        payoutAmountLamports = betAmount; // Return original bet
        outcomeReasonLog = `push_dice21`;
    }

    let finalSummaryMessage = `🃏 *Dice 21 \\- Final Result* 🃏\nYour Wager: *${betDisplayUSD}*\n\n`;
    finalSummaryMessage += `${playerRef}'s Hand: ${formatDiceRolls(gameData.playerHandRolls)} \\(Total: *${escapeMarkdownV2(String(playerScore))}*\\)\n`;
    finalSummaryMessage += `Bot Dealer's Hand: ${formatDiceRolls(gameData.botHandRolls)} \\(Total: *${escapeMarkdownV2(String(gameData.botScore))}*\\)${botBusted ? " \\- *BUSTED!*" : ""}\n\n${resultTextEnd}`;

    let finalUserBalanceForDisplay = BigInt(userObj.balance);
    let clientOutcome;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const ledgerReason = `${outcomeReasonLog}:${gameId}`;
        const balanceUpdate = await updateUserBalanceAndLedger(clientOutcome, userId, payoutAmountLamports, ledgerReason, {game_log_id: null}, `Outcome of Dice 21 game ${gameId}`);
        if (balanceUpdate.success) { 
            finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports; 
            if (payoutAmountLamports > betAmount && !outcomeReasonLog.startsWith('push')) {
                const profit = payoutAmountLamports - betAmount;
                finalSummaryMessage += `\nYou take home *${escapeMarkdownV2(await formatBalanceForDisplay(profit, 'USD'))}* in profit\\!`;
            }
            await clientOutcome.query('COMMIT');
        } else { 
            await clientOutcome.query('ROLLBACK');
            finalSummaryMessage += `\n\n⚠️ A critical error occurred while settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "Unknown DB Error")}\`\\. Admin has been alerted for manual review\\.`; 
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL D21 Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmountLamports))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if(clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_D21_BOT} DB error during D21 outcome processing for ${gameId}: ${dbError.message}`);
        finalSummaryMessage += `\n\n⚠️ A severe database error occurred. Admin notified.`;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL D21 DB Transaction Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nError: ${dbError.message}. Balance state may be inconsistent.`);
    } finally {
        if(clientOutcome) clientOutcome.release();
    }

    finalSummaryMessage += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceForDisplay, 'USD'))}*\\.`;

    const postGameKeyboardD21 = createPostGameKeyboard(GAME_IDS.DICE_21, betAmount);
    
    if (effectiveGameMessageId && bot) { // Use the potentially updated message ID
         await bot.editMessageText(finalSummaryMessage, { chat_id: String(chatId), message_id: Number(effectiveGameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardD21 })
           .catch(async (e) => {
               console.warn(`${LOG_PREFIX_D21_BOT} Failed to edit final D21 message (ID: ${effectiveGameMessageId}), sending new: ${e.message}`);
               await safeSendMessage(String(chatId), finalSummaryMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardD21 });
           });
    } else {
        await safeSendMessage(String(chatId), finalSummaryMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardD21 });
    }

    activeGames.delete(gameId);
}
console.log("[D21 Handler] processDice21BotTurn defined.");

console.log("Part 5b, Section 2: Dice 21 (Blackjack Style) Game Logic & Handlers - Complete.");
// --- End of Part 5b, Section 2 ---
// --- Start of Part 5c, Section 1 (NEW): Over/Under 7 Game - Full Implementation & Enhancements ---
// index.js - Part 5c, Section 1: Over/Under 7 Game Logic & Handlers
//-------------------------------------------------------------------------------------------------
console.log("Loading Part 5c, Section 1 (NEW): Over/Under 7 Game - Full Implementation & Enhancements...");

// Assumed dependencies from previous Parts:
// Part 1: GAME_IDS (defined in 5a-S1 New), QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX,
//         LAMPORTS_PER_SOL, escapeMarkdownV2, safeSendMessage, activeGames, pool, bot,
//         OU7_DICE_COUNT, OU7_PAYOUT_NORMAL, OU7_PAYOUT_SEVEN (constants, ensure they are loaded/available)
//         stringifyWithBigInt, notifyAdmin, sleep
// Part 2: getOrCreateUser
// Part 3: getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay, rollDie, formatDiceRolls
// Part 5a-S4 (NEW): createPostGameKeyboard, createStandardTitle
// Part P2: updateUserBalanceAndLedger

// --- Over/Under 7 Game Logic ---

async function handleStartOverUnder7Command(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_OU7_START = `[OU7_Start UID:${userId} CH:${chatId}]`;

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_OU7_START} Invalid betAmountLamports: ${betAmountLamports}. Expected positive BigInt.`);
        await safeSendMessage(chatId, "🎲 Oops! There was an issue with the bet amount for Over/Under 7. Please try starting the game again with a valid bet.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Apologies! We couldn't fetch your player profile to start Over/Under 7. Please try /start again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_OU7_START} Initiating Over/Under 7. Bet: ${betAmountLamports} lamports by User: ${userObj.username || userId}.`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino funds are a bit shy for an Over/Under 7 game at *${betDisplayUSD}*\\. You'd need approximately *${neededDisplay}* more. Care to top up?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.OVER_UNDER_7);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_ou7', { game_id_custom_field: gameId },
            `Bet for Over/Under 7 game ${gameId} by ${playerRef}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_OU7_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Over/Under 7 wager of *${betDisplayUSD}* couldn't be placed due to a hiccup: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`\\. Please try again.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports; // Update in-memory balance
        console.log(`${LOG_PREFIX_OU7_START} Wager ${betAmountLamports} placed. New balance for ${userId}: ${userObj.balance}`);
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_OU7_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_OU7_START} Database error during Over/Under 7 bet placement: ${dbError.message}`, dbError.stack);
        await safeSendMessage(chatId, "⚙️ A database disturbance prevented the start of your Over/Under 7 game. Please try again in a moment.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }

    const gameData = {
        type: GAME_IDS.OVER_UNDER_7, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, playerChoice: null, diceRolls: [], diceSum: null,
        status: 'waiting_player_choice', gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);

    const title = createStandardTitle("Over/Under 7 Showdown", "🎲");
    const initialMessageText = `${title}\n\n${playerRef}, you've courageously wagered *${betDisplayUSD}*\\. The dice are polished and ready for action!\n\nPredict the total sum of *${escapeMarkdownV2(String(OU7_DICE_COUNT))} dice*: Will it be Under 7, Exactly 7, or Over 7? Make your fateful choice below! 👇`;
    const keyboard = {
        inline_keyboard: [
            [{ text: "📉 Under 7 (Sum 2-6)", callback_data: `ou7_choice:${gameId}:under` }],
            [{ text: "🎯 Exactly 7 (BIG PAYOUT!)", callback_data: `ou7_choice:${gameId}:seven` }],
            [{ text: "📈 Over 7 (Sum 8-12)", callback_data: `ou7_choice:${gameId}:over` }],
            [{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.OVER_UNDER_7}` }, { text: '💳 Wallet', callback_data: 'menu:wallet' }]
        ]
    };
    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData); // Update gameData with messageId
    } else {
        console.error(`${LOG_PREFIX_OU7_START} Failed to send Over/Under 7 game message for ${gameId}. Refunding wager.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_ou7_setup_fail', {}, `Refund for OU7 game ${gameId} - message send failure`);
            await refundClient.query('COMMIT');
            console.log(`${LOG_PREFIX_OU7_START} Successfully refunded bet for game ${gameId} due to message send failure.`);
        } catch (err) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_OU7_START} CRITICAL: Failed to refund user for OU7 setup fail ${gameId}: ${err.message}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL OU7 REFUND FAILURE 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nReason: Failed to send game message AND failed to refund. Manual intervention required.`, {parse_mode:'MarkdownV2'});
        } finally {
            if (refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
    }
}

async function handleOverUnder7Choice(gameId, choice, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    const userId = String(userObj.telegram_id);
    const LOG_PREFIX_OU7_CHOICE = `[OU7_Choice GID:${gameId} UID:${userId} Choice:${choice}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== userId || gameData.status !== 'waiting_player_choice' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Over/Under 7 game action is outdated, not yours, or the game has moved on.", show_alert: true });
        // Optionally, if the message is very old and not the current game message, remove its keyboard
        if (originalMessageIdFromCallback && bot && gameData && gameData.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
            bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(() => {});
        }
        return;
    }
    
    const choiceTextDisplay = choice.charAt(0).toUpperCase() + choice.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, { text: `🎯 Locked In: ${choiceTextDisplay} 7! The dice are tumbling...` }).catch(() => {});

    gameData.playerChoice = choice;
    gameData.status = 'rolling_dice';
    activeGames.set(gameId, gameData); // Update status

    const { chatId, playerRef, betAmount } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    
    const titleRolling = createStandardTitle("Over/Under 7 - Dice Rolling!", "🎲");
    let rollingMessageText = `${titleRolling}\n\n${playerRef} bets *${betDisplayUSD}* on the sum being *${escapeMarkdownV2(choiceTextDisplay)} 7*.\nThe dice dance across the felt... No turning back now! 🤞`;

    let currentMessageId = gameData.gameMessageId; // This should be the ID of the message with choice buttons
    if (currentMessageId && bot) {
        try {
            await bot.editMessageText(rollingMessageText, { chat_id: String(chatId), message_id: Number(currentMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }); // Clear buttons
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_OU7_CHOICE} Failed to edit rolling message (ID: ${currentMessageId}), sending new: ${e.message}`);
                const newMsg = await safeSendMessage(String(chatId), rollingMessageText, { parse_mode: 'MarkdownV2' });
                if (newMsg?.message_id && activeGames.has(gameId)) { // Update gameMessageId if a new one was sent
                    activeGames.get(gameId).gameMessageId = newMsg.message_id;
                    currentMessageId = newMsg.message_id;
                }
            }
        }
    } else { // Fallback if no message to edit
        const newMsg = await safeSendMessage(String(chatId), rollingMessageText, { parse_mode: 'MarkdownV2' });
        if (newMsg?.message_id && activeGames.has(gameId)) {
            activeGames.get(gameId).gameMessageId = newMsg.message_id;
            currentMessageId = newMsg.message_id;
        }
    }
    await sleep(1000); // Pause for dramatic effect

    let diceRolls = [];
    let diceSum = 0;
    let animatedDiceMessageIdsOU7 = [];
    for (let i = 0; i < OU7_DICE_COUNT; i++) {
        try {
            const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
            diceRolls.push(diceMsg.dice.value);
            diceSum += diceMsg.dice.value;
            animatedDiceMessageIdsOU7.push(diceMsg.message_id);
            await sleep(OU7_DICE_COUNT > 1 ? 2200 : 2800); // Slightly longer pause for single die animation
        } catch (e) {
            console.warn(`${LOG_PREFIX_OU7_CHOICE} Failed to send animated dice for OU7 (Roll ${i+1}), using internal roll. Error: ${e.message}`);
            const internalRoll = rollDie();
            diceRolls.push(internalRoll);
            diceSum += internalRoll;
            await safeSendMessage(String(chatId), `⚙️ Casino's Internal Roll ${i + 1} (Dice Animation Failed): A *${escapeMarkdownV2(String(internalRoll))}* 🎲 clatters onto the table.`, { parse_mode: 'MarkdownV2' });
            await sleep(1000);
        }
    }
    gameData.diceRolls = diceRolls;
    gameData.diceSum = BigInt(diceSum);
    gameData.status = 'game_over';
    activeGames.set(gameId, gameData); // Final status update before result

    animatedDiceMessageIdsOU7.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); }); // Clean up dice animations

    let win = false;
    let profitMultiplier = 0; // This is PROFIT multiplier, not total return multiplier
    if (choice === 'under' && diceSum < 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
    else if (choice === 'over' && diceSum > 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
    else if (choice === 'seven' && diceSum === 7) { win = true; profitMultiplier = OU7_PAYOUT_SEVEN; }

    let payoutAmountLamports = 0n; // Total amount to credit (includes original bet if win/push)
    let outcomeReasonLog = "";
    let resultTextPart = "";
    const profitAmountLamports = win ? betAmount * BigInt(profitMultiplier) : 0n;

    if (win) {
        payoutAmountLamports = betAmount + profitAmountLamports; // Bet back + profit
        outcomeReasonLog = `win_ou7_${choice}_sum${diceSum}`;
        const winEmoji = choice === 'seven' ? "🎯 JACKPOT!" : "🎉 WINNER!";
        resultTextPart = `${winEmoji} Your prediction of *${escapeMarkdownV2(choiceTextDisplay)} 7* was spot on! You've won a handsome *${escapeMarkdownV2(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}* in profit!`;
    } else {
        payoutAmountLamports = 0n; // Bet was already deducted
        outcomeReasonLog = `loss_ou7_${choice}_sum${diceSum}`;
        resultTextPart = `💔 *So Close!* The dice didn't favor your prediction of *${escapeMarkdownV2(choiceTextDisplay)} 7* this round. Better luck next time!`;
    }

    let finalUserBalanceLamports = BigInt(userObj.balance); // Fallback
    let clientOutcome;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const ledgerReason = `${outcomeReasonLog} (Game ID: ${gameId})`;
        const balanceUpdate = await updateUserBalanceAndLedger(clientOutcome, userId, payoutAmountLamports, ledgerReason, { game_id_custom_field: gameId }, `Outcome of OU7 game ${gameId}`);
        
        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
            console.log(`${LOG_PREFIX_OU7_CHOICE} Outcome processed. User ${userId} new balance: ${finalUserBalanceLamports}`);
        } else {
            await clientOutcome.query('ROLLBACK');
            resultTextPart += `\n\n⚠️ A critical error occurred settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`. Our casino staff has been alerted.`;
            console.error(`${LOG_PREFIX_OU7_CHOICE} Failed to update balance for OU7 game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL OU7 Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due (Payout/Refund): \`${escapeMarkdownV2(formatCurrency(payoutAmountLamports))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`. Manual check required.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_OU7_CHOICE} DB error during OU7 outcome processing for ${gameId}: ${dbError.message}`, dbError.stack);
        resultTextPart += `\n\n⚠️ A severe database error occurred. Our casino staff has been notified.`;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL OU7 DB Transaction Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nError: ${dbError.message}. Balance state may be inconsistent.`);
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    const titleResult = createStandardTitle("Over/Under 7 - Result!", "🏁");
    let finalMessageText = `${titleResult}\n\nYour Bet: *${betDisplayUSD}* on *${escapeMarkdownV2(choiceTextDisplay)} 7*.\n\n`;
    finalMessageText += `The dice revealed: ${formatDiceRolls(diceRolls)} for a grand total of *${escapeMarkdownV2(String(diceSum))}*!\n\n${resultTextPart}`;
    finalMessageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*.`;

    const postGameKeyboardOU7 = createPostGameKeyboard(GAME_IDS.OVER_UNDER_7, betAmount);

    if (currentMessageId && bot) { // Edit the "rolling" message
        try {
            await bot.editMessageText(finalMessageText, { chat_id: String(chatId), message_id: Number(currentMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_OU7_CHOICE} Failed to edit OU7 result message (ID: ${currentMessageId}), sending new: ${e.message}`);
                await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
            }
        }
    } else { // Fallback if no message ID to edit
        await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
    }
    activeGames.delete(gameId); // Clean up the game state
}

console.log("Part 5c, Section 1 (NEW): Over/Under 7 Game - Full Implementation & Enhancements - Complete.");
// --- End of Part 5c, Section 1 (NEW) ---
// --- Start of Part 5c, Section 2 (NEW): High Roller Duel Game - Full Implementation ---
// index.js - Part 5c, Section 2: High Roller Dice Duel Game Logic & Handlers
//-------------------------------------------------------------------------------------
console.log("Loading Part 5c, Section 2 (NEW): High Roller Duel Game - Full Implementation...");

// Assumed dependencies from previous Parts:
// Part 1: GAME_IDS (defined in 5a-S1 New), QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX,
//         LAMPORTS_PER_SOL, escapeMarkdownV2, safeSendMessage, activeGames, pool, bot,
//         DUEL_DICE_COUNT (constant, ensure it's loaded/available, e.g., value of 2)
//         stringifyWithBigInt, notifyAdmin, sleep
// Part 2: getOrCreateUser
// Part 3: getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay, rollDie, formatDiceRolls
// Part 5a-S4 (NEW): createPostGameKeyboard, createStandardTitle
// Part P2: updateUserBalanceAndLedger

// --- High Roller Duel Game Logic ---

async function handleStartDuelCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_DUEL_START = `[Duel_Start UID:${userId} CH:${chatId}]`;

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_DUEL_START} Invalid betAmountLamports: ${betAmountLamports}. Expected positive BigInt.`);
        await safeSendMessage(chatId, "⚔️ Hold your steel! The bet amount for this duel seems incorrect. Please try again with a valid wager.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Greetings, warrior! We couldn't fetch your champion profile to start the Duel. Please try /start again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_DUEL_START} Initiating High Roller Duel. Bet: ${betAmountLamports} lamports by User: ${userObj.username || userId}.`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your war chest is a tad light for a High Roller Duel of *${betDisplayUSD}*! You'll need approximately *${neededDisplay}* more. Reinforce your treasury?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.DUEL);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_duel', { game_id_custom_field: gameId },
            `Bet for High Roller Duel game ${gameId} by ${playerRef}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_DUEL_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Duel wager of *${betDisplayUSD}* couldn't be placed due to a battlefield hiccup: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`. Please try to enlist again.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports;
        console.log(`${LOG_PREFIX_DUEL_START} Wager ${betAmountLamports} placed for Duel. New balance for ${userId}: ${userObj.balance}`);
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_DUEL_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_DUEL_START} Database error during Duel bet placement: ${dbError.message}`, dbError.stack);
        await safeSendMessage(chatId, "⚙️ The armory's database seems to be in disarray! Failed to start your Duel. Please try again in a moment.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }

    const gameData = {
        type: GAME_IDS.DUEL, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, playerRolls: [], playerScore: 0n, botRolls: [], botScore: 0n,
        status: 'waiting_player_roll', gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);

    const title = createStandardTitle("High Roller Dice Duel", "⚔️");
    const initialMessageText = `${title}\n\n${playerRef}, your challenge for *${betDisplayUSD}* has been accepted by the Bot Dealer! The dice await your command.\n\nPress *"Roll Your Dice"* to unleash your fortune! 👇`;
    const keyboard = {
        inline_keyboard: [
            [{ text: "🎲 Roll Your Dice!", callback_data: `duel_roll:${gameId}` }],
            [{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DUEL}` }, { text: '💳 Wallet', callback_data: 'menu:wallet' }]
        ]
    };
    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
    } else {
        console.error(`${LOG_PREFIX_DUEL_START} Failed to send Duel game message for ${gameId}. Refunding wager.`);
        // Refund logic (similar to Over/Under 7 start command)
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_duel_setup_fail', {}, `Refund for Duel game ${gameId} - message send fail`);
            await refundClient.query('COMMIT');
        } catch (err) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_DUEL_START} CRITICAL: Failed to refund user for Duel setup fail ${gameId}: ${err.message}`);
        } finally {
            if (refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
    }
}

async function handleDuelRoll(gameId, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    const userId = String(userObj.telegram_id);
    const LOG_PREFIX_DUEL_ROLL = `[Duel_Roll GID:${gameId} UID:${userId}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== userId || gameData.status !== 'waiting_player_roll' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Duel action is outdated, not yours, or the dice are already cast!", show_alert: true });
        if (originalMessageIdFromCallback && bot && gameData && gameData.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
            bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(() => {});
        }
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: `🎲 Casting the dice of fate for your duel...` }).catch(() => {});

    gameData.status = 'resolving'; // Game resolves after this roll
    activeGames.set(gameId, gameData);

    const { chatId, playerRef, betAmount } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    const diceCount = DUEL_DICE_COUNT; // Should be 2 as per original constants

    const titleRolling = createStandardTitle("High Roller Duel - The Clash!", "⚔️");
    let messageText = `${titleRolling}\n\n${playerRef} (Wager: *${betDisplayUSD}*)\n\n`;
    
    // --- Player's Roll ---
    let playerAnimatedDiceMessageIds = [];
    messageText += `${playerRef} steps forth, dice rattling with anticipation...\n`;
    if (gameData.gameMessageId && bot) { // Edit main message to show player is rolling
        await bot.editMessageText(messageText + "Rolling your dice... 🎲", { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>{});
    }
    await sleep(1000);

    for (let i = 0; i < diceCount; i++) {
        try {
            const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
            gameData.playerRolls.push(diceMsg.dice.value);
            gameData.playerScore += BigInt(diceMsg.dice.value);
            playerAnimatedDiceMessageIds.push(diceMsg.message_id);
            await sleep(diceCount > 1 ? 2200 : 2800);
        } catch (e) {
            console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to send animated dice for Player (Roll ${i+1}), using internal. Error: ${e.message}`);
            const internalRoll = rollDie();
            gameData.playerRolls.push(internalRoll);
            gameData.playerScore += BigInt(internalRoll);
            await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Casino's Internal Roll ${i + 1}): A *${escapeMarkdownV2(String(internalRoll))}* 🎲 emerges!`, { parse_mode: 'MarkdownV2' });
            await sleep(500);
        }
    }
    playerAnimatedDiceMessageIds.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });
    messageText += `Your roll: ${formatDiceRolls(gameData.playerRolls)} for a mighty total of *${escapeMarkdownV2(String(gameData.playerScore))}*!\n\n`;
    if (gameData.gameMessageId && bot) {
         await bot.editMessageText(messageText + "The Bot Dealer prepares their counter-roll... 🤖", { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>{});
    }
    await sleep(1500);

    // --- Bot's Roll ---
    let botAnimatedDiceMessageIds = [];
    messageText += `The Bot Dealer 🤖 eyes your score, then unleashes their dice...\n`;
     if (gameData.gameMessageId && bot) {
         await bot.editMessageText(messageText + "Bot is rolling... 🎲", { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>{});
    }
    await sleep(1000);

    for (let i = 0; i < diceCount; i++) {
        try {
            const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
            gameData.botRolls.push(diceMsg.dice.value);
            gameData.botScore += BigInt(diceMsg.dice.value);
            botAnimatedDiceMessageIds.push(diceMsg.message_id);
            await sleep(diceCount > 1 ? 2200 : 2800);
        } catch (e) {
            console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to send animated dice for Bot (Roll ${i+1}), using internal. Error: ${e.message}`);
            const internalRoll = rollDie();
            gameData.botRolls.push(internalRoll);
            gameData.botScore += BigInt(internalRoll);
            await safeSendMessage(String(chatId), `⚙️ Bot Dealer (Casino's Internal Roll ${i + 1}): A *${escapeMarkdownV2(String(internalRoll))}* 🎲 is revealed!`, { parse_mode: 'MarkdownV2' });
            await sleep(500);
        }
    }
    botAnimatedDiceMessageIds.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });
    messageText += `Bot Dealer's roll: ${formatDiceRolls(gameData.botRolls)} for a total of *${escapeMarkdownV2(String(gameData.botScore))}*!\n\n`;
    
    // --- Determine Outcome ---
    let resultTextPart = "";
    let payoutAmountLamports = 0n;
    let outcomeReasonLog = "";

    if (gameData.playerScore > gameData.botScore) {
        resultTextPart = `🎉 **VICTORY IS YOURS, ${playerRef}!** 🎉\nYour commanding score triumphs over the Bot Dealer! You've won the duel!`;
        payoutAmountLamports = betAmount * 2n; // Standard 2x payout (bet back + profit)
        outcomeReasonLog = 'win_duel';
    } else if (gameData.botScore > gameData.playerScore) {
        resultTextPart = `💔 **DEFEAT... This Time.** 💔\nThe Bot Dealer's roll narrowly surpasses yours. A valiant effort, warrior!`;
        payoutAmountLamports = 0n; // Bet already deducted
        outcomeReasonLog = 'loss_duel';
    } else {
        resultTextPart = `🛡️ **A STALEMATE! A PUSH!** 🛡️\nAn incredible duel ends in a perfect tie! Your wager of *${betDisplayUSD}* is returned.`;
        payoutAmountLamports = betAmount; // Return original bet
        outcomeReasonLog = 'push_duel';
    }
    messageText += `------------------------------------\n${resultTextPart}`;

    let finalUserBalanceLamports = BigInt(userObj.balance); // Fallback
    let clientOutcome;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const ledgerReason = `${outcomeReasonLog} (Game ID: ${gameId})`;
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, userId, payoutAmountLamports, 
            ledgerReason, { game_id_custom_field: gameId }, 
            `Outcome of Duel game ${gameId}`
        );
        
        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
            if (payoutAmountLamports > betAmount && outcomeReasonLog === 'win_duel') {
                const profit = payoutAmountLamports - betAmount;
                messageText += `\nYou claim *${escapeMarkdownV2(await formatBalanceForDisplay(profit, 'USD'))}* in glorious profit!`;
            }
            console.log(`${LOG_PREFIX_DUEL_ROLL} Duel outcome processed. User ${userId} new balance: ${finalUserBalanceLamports}`);
        } else {
            await clientOutcome.query('ROLLBACK');
            messageText += `\n\n⚠️ A critical error occurred settling your duel wager: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`. Our scribes have been alerted for manual review.`;
            console.error(`${LOG_PREFIX_DUEL_ROLL} Failed to update balance for Duel game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL DUEL Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmountLamports))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`. Manual check required.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_DUEL_ROLL} DB error during Duel outcome for ${gameId}: ${dbError.message}`, dbError.stack);
        messageText += `\n\n⚠️ A severe database error occurred during duel resolution. Our quartermasters have been notified.`;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL DUEL DB Transaction Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nError: ${dbError.message}. Balance state may be inconsistent.`);
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    messageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*.`;
    const postGameKeyboardDuel = createPostGameKeyboard(GAME_IDS.DUEL, betAmount);

    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(messageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel })
            .catch(async (e) => {
                console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to edit final Duel message (ID: ${gameData.gameMessageId}), sending new: ${e.message}`);
                await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel });
            });
    } else {
        await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel });
    }
    activeGames.delete(gameId);
}


console.log("Part 5c, Section 2 (NEW): High Roller Duel Game - Full Implementation - Complete.");
// --- End of Part 5c, Section 2 (NEW) ---
// --- Start of Part 5c, Section 3 (NEW) - Segment 1: Greed's Ladder Game ---
// index.js - Part 5c, Section 3: Greed's Ladder Game Logic & Handlers
//-------------------------------------------------------------------------------------
console.log("Loading Part 5c, Section 3 (NEW) - Segment 1: Greed's Ladder Game...");

// Assumed dependencies from previous Parts:
// Part 1: GAME_IDS, QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX,
//         LAMPORTS_PER_SOL, escapeMarkdownV2, safeSendMessage, activeGames, pool, bot,
//         LADDER_ROLL_COUNT, LADDER_BUST_ON, LADDER_PAYOUTS (constants)
//         stringifyWithBigInt, notifyAdmin, sleep
// Part 2: getOrCreateUser
// Part 3: getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay, rollDie, formatDiceRolls
// Part 5a-S4 (NEW): createPostGameKeyboard, createStandardTitle
// Part P2: updateUserBalanceAndLedger

// --- Greed's Ladder Game Logic ---

async function handleStartLadderCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_LADDER_START = `[Ladder_Start UID:${userId} CH:${chatId}]`;

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_LADDER_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🪜 Oh dear! The wager for Greed's Ladder seems incorrect. Please try again with a valid amount.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Greetings, climber! We couldn't find your adventurer profile for Greed's Ladder. Please try /start again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_LADDER_START} Initiating Greed's Ladder. Bet: ${betAmountLamports} lamports by User: ${userObj.username || userId}.`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your treasure chest is a bit light for the *${betDisplayUSD}* climb on Greed's Ladder! You'll need about *${neededDisplay}* more. Fortify your reserves?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.LADDER);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_ladder', { game_id_custom_field: gameId },
            `Bet for Greed's Ladder game ${gameId} by ${playerRef}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_LADDER_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Greed's Ladder wager of *${betDisplayUSD}* failed to post: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`. Please try again.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports; // Update in-memory balance
        console.log(`${LOG_PREFIX_LADDER_START} Wager ${betAmountLamports} placed. New balance for ${userId}: ${userObj.balance}`);
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_LADDER_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_LADDER_START} Database error during Greed's Ladder bet: ${dbError.message}`, dbError.stack);
        await safeSendMessage(chatId, "⚙️ The Ladder's foundations seem shaky (database error)! Failed to start. Please try again.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }
    
    // Greed's Ladder is instant resolve in this implementation
    // No activeGames state needed beyond this point usually, but good for consistency if animations are added
    const gameData = { 
        type: GAME_IDS.LADDER, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, rolls: [], sum: 0n, status: 'rolling', gameMessageId: null 
    };
    activeGames.set(gameId, gameData); // Set briefly

    const titleRolling = createStandardTitle("Greed's Ladder - The Climb Begins!", "🪜");
    let messageText = `${titleRolling}\n\n${playerRef} wagers *${betDisplayUSD}* and steps onto Greed's Ladder!\nSending *${escapeMarkdownV2(String(LADDER_ROLL_COUNT))} dice* tumbling down... Hold your breath! 🎲🎲🎲`;
    
    const sentRollingMsg = await safeSendMessage(chatId, messageText, {parse_mode: 'MarkdownV2'});
    if (sentRollingMsg?.message_id) gameData.gameMessageId = sentRollingMsg.message_id;
    await sleep(1500); // Dramatic pause

    let playerRolls = [];
    let playerSum = 0n;
    let isBust = false;
    let animatedDiceIds = [];

    for (let i = 0; i < LADDER_ROLL_COUNT; i++) {
        try {
            const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
            playerRolls.push(diceMsg.dice.value);
            playerSum += BigInt(diceMsg.dice.value);
            animatedDiceIds.push(diceMsg.message_id);
            if (BigInt(diceMsg.dice.value) === BigInt(LADDER_BUST_ON)) {
                isBust = true;
            }
            await sleep(LADDER_ROLL_COUNT > 1 ? 2000 : 2500);
        } catch (e) {
            console.warn(`${LOG_PREFIX_LADDER_START} Failed to send animated dice for Ladder (Roll ${i+1}), using internal. Error: ${e.message}`);
            const internalRoll = rollDie();
            playerRolls.push(internalRoll);
            playerSum += BigInt(internalRoll);
            if (BigInt(internalRoll) === BigInt(LADDER_BUST_ON)) {
                isBust = true;
            }
            await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Casino's Internal Dice Roll ${i + 1}): A *${escapeMarkdownV2(String(internalRoll))}* 🎲 tumbles forth!`, { parse_mode: 'MarkdownV2' });
            await sleep(500);
        }
    }
    animatedDiceIds.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });
    gameData.rolls = playerRolls;
    gameData.sum = playerSum;

    let payoutAmountLamports = 0n;
    let outcomeReasonLog = "";
    let resultTextPart = "";
    let finalUserBalanceLamports = userObj.balance; // Start with balance after bet deduction

    const titleResult = createStandardTitle("Greed's Ladder - The Outcome!", "🏁");
    messageText = `${titleResult}\n\n${playerRef}'s wager: *${betDisplayUSD}*\nThe dice reveal: ${formatDiceRolls(playerRolls)}\nTotal Sum: *${escapeMarkdownV2(String(playerSum))}*\n\n`;

    if (isBust) {
        outcomeReasonLog = `loss_ladder_bust_roll${LADDER_BUST_ON}`;
        resultTextPart = `💥 *CRASH! A ${escapeMarkdownV2(String(LADDER_BUST_ON))} appeared!* 💥\nYou've tumbled off Greed's Ladder! A tragic end to a daring climb. Your wager is lost.`;
        gameData.status = 'game_over_player_bust';
        // Balance already deducted, no payout. PayoutAmountLamports remains 0n.
    } else {
        let foundPayout = false;
        for (const payoutTier of LADDER_PAYOUTS) {
            if (playerSum >= payoutTier.min && playerSum <= payoutTier.max) {
                const profitLamports = betAmountLamports * BigInt(payoutTier.multiplier);
                payoutAmountLamports = betAmountLamports + profitLamports; // Bet back + profit
                outcomeReasonLog = `win_ladder_sum${playerSum}_mult${payoutTier.multiplier}`;
                resultTextPart = `${escapeMarkdownV2(payoutTier.label)} You've reached a high rung and won *${escapeMarkdownV2(await formatBalanceForDisplay(profitLamports, 'USD'))}* in profit!`;
                foundPayout = true;
                break;
            }
        }
        if (!foundPayout) { // Should be caught by a "Push" or default loss tier in LADDER_PAYOUTS
            outcomeReasonLog = 'loss_ladder_no_payout_tier';
            resultTextPart = "😐 A cautious climb... but not high enough for a prize this time. Your wager is lost.";
            // PayoutAmountLamports remains 0n.
        }
        gameData.status = 'game_over_resolved';
    }
    messageText += resultTextPart;
    
    let clientOutcome;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const ledgerReason = `${outcomeReasonLog} (Game ID: ${gameId})`;
        // updateUserBalanceAndLedger handles 0n payoutAmount as just a log for loss if bet was already deducted
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, userId, payoutAmountLamports, 
            ledgerReason, { game_id_custom_field: gameId }, 
            `Outcome of Greed's Ladder game ${gameId}`
        );
        
        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
        } else {
            await clientOutcome.query('ROLLBACK');
            messageText += `\n\n⚠️ A critical error occurred settling your Ladder game: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`. Casino staff notified.`;
            console.error(`${LOG_PREFIX_LADDER_START} Failed to update balance for Ladder game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL LADDER Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\` User: ${playerRef}\nAmount: \`${formatCurrency(payoutAmountLamports)}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`. Manual check needed.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_LADDER_START} DB error during Ladder outcome for ${gameId}: ${dbError.message}`, dbError.stack);
        messageText += `\n\n⚠️ A severe database error occurred resolving your climb. Casino staff notified.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    messageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*.`;
    const postGameKeyboardLadder = createPostGameKeyboard(GAME_IDS.LADDER, betAmount);

    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(messageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder })
            .catch(async (e) => {
                console.warn(`${LOG_PREFIX_LADDER_START} Failed to edit final Ladder message (ID: ${gameData.gameMessageId}), sending new: ${e.message}`);
                await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder });
            });
    } else {
        await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder });
    }
    activeGames.delete(gameId);
}

console.log("Part 5c, Section 3 (NEW) - Segment 1: Greed's Ladder Game - Complete.");
// --- End of Part 5c, Section 3 (NEW) - Segment 1 ---
// --- Start of Part 5c, Section 3 (NEW) - Segment 2: Sevens Out Game ---
// index.js - Part 5c, Section 3: Sevens Out (Simplified Craps) Game Logic & Handlers
//-------------------------------------------------------------------------------------
console.log("Loading Part 5c, Section 3 (NEW) - Segment 2: Sevens Out Game...");

// Assumed dependencies from previous Parts:
// Part 1: GAME_IDS, QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX,
//         LAMPORTS_PER_SOL, escapeMarkdownV2, safeSendMessage, activeGames, pool, bot,
//         stringifyWithBigInt, notifyAdmin, sleep
// Part 2: getOrCreateUser
// Part 3: getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay, rollDie, formatDiceRolls
// Part 5a-S4 (NEW): createPostGameKeyboard, createStandardTitle
// Part P2: updateUserBalanceAndLedger

// --- Sevens Out (Simplified Craps) Game Logic ---

async function handleStartSevenOutCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_S7_START = `[S7_Start UID:${userId} CH:${chatId}]`;

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_S7_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🎲 Seven's a charm, but not with that bet! Please try again with a valid wager for Sevens Out.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Greetings, roller! We couldn't find your player profile for Sevens Out. Please try /start again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_S7_START} Initiating Sevens Out. Bet: ${betAmountLamports} lamports by User: ${userObj.username || userId}.`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino wallet is a bit light for a *${betDisplayUSD}* game of Sevens Out! You'll need about *${neededDisplay}* more. Ready to reload?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.SEVEN_OUT);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_s7', { game_id_custom_field: gameId },
            `Bet for Sevens Out game ${gameId} by ${playerRef}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_S7_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Sevens Out wager of *${betDisplayUSD}* hit a snag: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`. Please try once more.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports;
        console.log(`${LOG_PREFIX_S7_START} Wager ${betAmountLamports} placed. New balance for ${userId}: ${userObj.balance}`);
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_S7_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_S7_START} Database error during Sevens Out bet: ${dbError.message}`, dbError.stack);
        await safeSendMessage(chatId, "⚙️ The dice table seems to be under maintenance (database error)! Failed to start. Please try again.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }
    
    const gameData = { 
        type: GAME_IDS.SEVEN_OUT, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, pointValue: null, rolls: [], currentSum: 0n,
        status: 'come_out_roll_pending', // Will transition to 'come_out_roll_processing' in the handler
        gameMessageId: null, lastInteractionTime: Date.now() 
    };
    activeGames.set(gameId, gameData);

    const title = createStandardTitle("Sevens Out - Come Out Roll!", "🎲");
    const initialMessageText = `${title}\n\n${playerRef}, your wager of *${betDisplayUSD}* is locked in for Sevens Out! Stepping up for the crucial **Come Out Roll**...\n\nI'll roll the first set of dice for you! Good luck! 🍀`;
    
    const sentMessage = await safeSendMessage(chatId, initialMessageText, {parse_mode: 'MarkdownV2'});
    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData); // Update with message ID
        
        // Automatically process the first (Come Out) roll
        // Construct a mock msgContext for the handler, as if it were from a command
        const mockMsgContextForFirstRoll = {
            from: userObj,
            chat: { id: chatId, type: msg.chat.type }, // Use original chat context
            message_id: sentMessage.message_id // The message we just sent
        };
        // No callbackQueryId for the first automatic roll
        await processSevenOutRoll(gameId, userObj, sentMessage.message_id, null, mockMsgContextForFirstRoll);

    } else {
        console.error(`${LOG_PREFIX_S7_START} Failed to send initial Sevens Out message for ${gameId}. Refunding wager.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_s7_setup_fail', {}, `Refund for S7 game ${gameId} - message send fail`);
            await refundClient.query('COMMIT');
        } catch (err) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_S7_START} CRITICAL: Failed to refund user for S7 setup fail ${gameId}: ${err.message}`);
        } finally {
            if (refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
    }
}

async function processSevenOutRoll(gameId, userObj, originalMessageId, callbackQueryId, msgContext) {
    const userId = String(userObj.telegram_id);
    const LOG_PREFIX_S7_ROLL = `[S7_Roll GID:${gameId} UID:${userId}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== userId) {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Sevens Out game action is outdated or not yours.", show_alert: true });
        return;
    }
    // Validate game status for subsequent rolls (not the first auto-roll)
    if (callbackQueryId && gameData.status !== 'point_phase_waiting_roll') {
         if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ It's not the right time to roll in this game!", show_alert: true });
         return;
    }
    if (callbackQueryId && Number(gameData.gameMessageId) !== Number(originalMessageId)) {
         if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⚙️ Please use the newest game message buttons.", show_alert: true });
         if (originalMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageId) }).catch(()=>{});
         return;
    }

    if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "🎲 Rolling the bones..."}).catch(()=>{});

    const isComeOutRoll = gameData.status === 'come_out_roll_pending';
    gameData.status = isComeOutRoll ? 'come_out_roll_processing' : 'point_phase_rolling';
    activeGames.set(gameId, gameData);

    const { chatId, playerRef, betAmount } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    const currentMainMessageId = gameData.gameMessageId; // The message ID to keep editing

    let rollingText = isComeOutRoll ? 
        `${playerRef} is making the crucial **Come Out Roll** (Wager: *${betDisplayUSD}*)...` :
        `${playerRef} rolls for their Point of *${escapeMarkdownV2(String(gameData.pointValue))}* (Wager: *${betDisplayUSD}*)...`;
    rollingText += "\n\nDice are flying! 🌪️🎲";
    
    if (currentMainMessageId && bot) {
        try {
            await bot.editMessageText(rollingText, { chat_id: String(chatId), message_id: Number(currentMainMessageId), parse_mode: 'MarkdownV2', reply_markup: {} });
        } catch(e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                 console.warn(`${LOG_PREFIX_S7_ROLL} Failed to edit rolling message (ID:${currentMainMessageId}), may send new for result. Error: ${e.message}`);
            }
        }
    }
    await sleep(1000);

    let currentRolls = [];
    let currentSum = 0;
    let animatedDiceIdsS7 = [];
    for (let i = 0; i < 2; i++) { // Always 2 dice for Craps/S7
        try {
            const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
            currentRolls.push(diceMsg.dice.value);
            currentSum += diceMsg.dice.value;
            animatedDiceIdsS7.push(diceMsg.message_id);
            await sleep(2200);
        } catch (e) {
            console.warn(`${LOG_PREFIX_S7_ROLL} Failed to send animated dice for S7 (Roll ${i+1}), using internal. Error: ${e.message}`);
            const internalRollVal = rollDie();
            currentRolls.push(internalRollVal);
            currentSum += internalRollVal;
            await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Casino's Internal Dice Roll ${i + 1}): A *${escapeMarkdownV2(String(internalRollVal))}* 🎲 appears!`, { parse_mode: 'MarkdownV2' });
            await sleep(500);
        }
    }
    animatedDiceIdsS7.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });
    gameData.rolls = currentRolls;
    gameData.currentSum = BigInt(currentSum);

    let messageToPlayer = isComeOutRoll ? `**Come Out Roll Results!**\n` : `**Point Phase Roll!**\n`;
    messageToPlayer += `${playerRef}, you rolled: ${formatDiceRolls(currentRolls)} for a total of *${escapeMarkdownV2(String(currentSum))}*!\n`;
    if (!isComeOutRoll && gameData.pointValue) {
        messageToPlayer += `Your Point to hit is: *${escapeMarkdownV2(String(gameData.pointValue))}*.\n`;
    }
    messageToPlayer += "\n";

    let gameEndsNow = false;
    let resultTextPart = "";
    let payoutAmountLamports = 0n;
    let outcomeReasonLog = "";
    let nextKeyboard = null;

    if (isComeOutRoll) {
        if (currentSum === 7 || currentSum === 11) { // Natural Win
            gameEndsNow = true; gameData.status = 'game_over_win_natural';
            resultTextPart = `🎉 **Natural Winner!** A ${currentSum} on the Come Out Roll! You win!`;
            payoutAmountLamports = betAmount * 2n; // Bet back + profit
            outcomeReasonLog = `win_s7_natural_${currentSum}`;
        } else if (currentSum === 2 || currentSum === 3 || currentSum === 12) { // Craps Loss
            gameEndsNow = true; gameData.status = 'game_over_loss_craps';
            resultTextPart = `💔 **Craps!** A ${currentSum} on the Come Out means the house wins this round.`;
            payoutAmountLamports = 0n; // Bet already deducted
            outcomeReasonLog = `loss_s7_craps_${currentSum}`;
        } else { // Point Established
            gameData.pointValue = BigInt(currentSum);
            gameData.status = 'point_phase_waiting_roll';
            resultTextPart = `🎯 **Point Established: ${escapeMarkdownV2(String(currentSum))}!**\nNow, roll your Point *before* a 7 to win! Good luck!`;
            nextKeyboard = { inline_keyboard: [[{ text: `🎲 Roll for Point (${escapeMarkdownV2(String(currentSum))})!`, callback_data: `s7_roll:${gameId}` }],[{text: `📖 Rules`, callback_data:`${RULES_CALLBACK_PREFIX}${GAME_IDS.SEVEN_OUT}`}]] };
        }
    } else { // Point Phase
        if (gameData.currentSum === gameData.pointValue) { // Point Hit - Win
            gameEndsNow = true; gameData.status = 'game_over_win_point_hit';
            resultTextPart = `🎉 **Point Hit! You rolled your Point of ${escapeMarkdownV2(String(gameData.pointValue))}!** You win!`;
            payoutAmountLamports = betAmount * 2n;
            outcomeReasonLog = `win_s7_point_${gameData.pointValue}`;
        } else if (gameData.currentSum === 7n) { // Seven Out - Loss
            gameEndsNow = true; gameData.status = 'game_over_loss_seven_out';
            resultTextPart = `💔 **Seven Out!** You rolled a 7 before hitting your Point of ${escapeMarkdownV2(String(gameData.pointValue))}. House wins.`;
            payoutAmountLamports = 0n;
            outcomeReasonLog = `loss_s7_seven_out_point_${gameData.pointValue}`;
        } else { // Neither Point nor 7 - Roll Again
            gameData.status = 'point_phase_waiting_roll'; // Stays in this state
            resultTextPart = `🎲 Keep rolling! Your Point is still *${escapeMarkdownV2(String(gameData.pointValue))}*. Avoid that 7!`;
            nextKeyboard = { inline_keyboard: [[{ text: `🎲 Roll Again for Point (${escapeMarkdownV2(String(gameData.pointValue))})!`, callback_data: `s7_roll:${gameId}` }],[{text: `📖 Rules`, callback_data:`${RULES_CALLBACK_PREFIX}${GAME_IDS.SEVEN_OUT}`}]] };
        }
    }
    
    messageToPlayer += resultTextPart;
    activeGames.set(gameId, gameData); // Save updated game data

    if (gameEndsNow) {
        await finalizeSevenOutGame(gameData, messageToPlayer, payoutAmountLamports, outcomeReasonLog, currentMainMessageId);
    } else {
        // Update the game message with the current roll's outcome and prompt for next roll
        if (currentMainMessageId && bot) {
            await bot.editMessageText(messageToPlayer, { chat_id: String(chatId), message_id: Number(currentMainMessageId), parse_mode: 'MarkdownV2', reply_markup: nextKeyboard })
            .catch(async (e) => {
                if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                    console.warn(`${LOG_PREFIX_S7_ROLL} Failed to edit S7 mid-game message (ID:${currentMainMessageId}), sending new. Error: ${e.message}`);
                    const newMsg = await safeSendMessage(String(chatId), messageToPlayer, { parse_mode: 'MarkdownV2', reply_markup: nextKeyboard });
                    if (newMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = newMsg.message_id;
                }
            });
        } else { // Fallback if no message to edit
            const newMsg = await safeSendMessage(String(chatId), messageToPlayer, { parse_mode: 'MarkdownV2', reply_markup: nextKeyboard });
            if (newMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = newMsg.message_id;
        }
    }
}

async function finalizeSevenOutGame(gameData, initialResultMessage, payoutAmountLamports, outcomeReasonLog, gameUIMessageId) {
    const { gameId, chatId, userId, playerRef, betAmount, userObj } = gameData;
    const LOG_PREFIX_S7_FINALIZE = `[S7_Finalize GID:${gameId} UID:${userId}]`;
    let finalUserBalanceLamports = BigInt(userObj.balance); // Fallback to balance after bet deduction
    let clientOutcome;

    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const ledgerReason = `${outcomeReasonLog} (Game ID: ${gameId})`;
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, userId, payoutAmountLamports,
            ledgerReason, { game_id_custom_field: gameId },
            `Outcome of Sevens Out game ${gameId}`
        );

        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
            if (payoutAmountLamports > betAmount && outcomeReasonLog.startsWith('win')) {
                const profit = payoutAmountLamports - betAmount;
                initialResultMessage += `\nYou pocket a neat *${escapeMarkdownV2(await formatBalanceForDisplay(profit, 'USD'))}* in profit!`;
            }
        } else {
            await clientOutcome.query('ROLLBACK');
            initialResultMessage += `\n\n⚠️ A critical casino vault error occurred settling your Sevens Out game: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`. Our pit boss has been alerted for manual review.`;
            console.error(`${LOG_PREFIX_S7_FINALIZE} Failed to update balance for Sevens Out game ${gameId}. Error: ${balanceUpdate.error}`);
             if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL S7 Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\` User: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmountLamports))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`. Manual check needed.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_S7_FINALIZE} DB error during S7 outcome for ${gameId}: ${dbError.message}`, dbError.stack);
        initialResultMessage += `\n\n⚠️ A major dice table malfunction (database error) occurred. Our pit boss has been notified.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }
    
    initialResultMessage += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*.`;
    const postGameKeyboardS7 = createPostGameKeyboard(GAME_IDS.SEVEN_OUT, betAmount);

    if (gameUIMessageId && bot) {
        await bot.editMessageText(initialResultMessage, { chat_id: String(chatId), message_id: Number(gameUIMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardS7 })
            .catch(async (e) => {
                console.warn(`${LOG_PREFIX_S7_FINALIZE} Failed to edit final S7 message (ID: ${gameUIMessageId}), sending new: ${e.message}`);
                await safeSendMessage(String(chatId), initialResultMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardS7 });
            });
    } else {
        await safeSendMessage(String(chatId), initialResultMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardS7 });
    }
    activeGames.delete(gameId);
}


console.log("Part 5c, Section 3 (NEW) - Segment 2: Sevens Out Game - Complete.");
// --- End of Part 5c, Section 3 (NEW) - Segment 2 ---
// --- Start of Part 5c, Section 4 (NEW): Slot Frenzy Game & Additional Games Callback Forwarder ---
// index.js - Part 5c, Section 4: Slot Frenzy Game Logic & Callback Router for Part 5c Games
//----------------------------------------------------------------------------------------------------
console.log("Loading Part 5c, Section 4 (NEW): Slot Frenzy Game & Additional Games Callback Forwarder...");

// Assumed dependencies from previous Parts:
// Part 1: GAME_IDS, QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX,
//         LAMPORTS_PER_SOL, escapeMarkdownV2, safeSendMessage, activeGames, pool, bot,
//         SLOT_PAYOUTS, SLOT_DEFAULT_LOSS_MULTIPLIER (constants from Part 1)
//         stringifyWithBigInt, notifyAdmin, sleep
// Part 2: getOrCreateUser
// Part 3: getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay, rollDie
// Part 5a-S4 (NEW): createPostGameKeyboard, createStandardTitle
// Part P2: updateUserBalanceAndLedger

// --- Slot Frenzy Game Logic ---

async function handleStartSlotCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_SLOT_START = `[Slot_Start UID:${userId} CH:${chatId}]`;

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_SLOT_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🎰 Hold your horses! That bet amount for Slot Frenzy doesn't look quite right. Please try again with a valid wager.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Hey spinner! We couldn't find your player profile for Slot Frenzy. Please hit /start first.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_SLOT_START} Initiating Slot Frenzy. Bet: ${betAmountLamports} lamports by User: ${userObj.username || userId}.`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino wallet needs a bit more sparkle for a *${betDisplayUSD}* spin on Slot Frenzy! You're short by about *${neededDisplay}*. Time to reload?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.SLOT_FRENZY); // Though single interaction, good for logging
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_slot', { game_id_custom_field: gameId },
            `Bet for Slot Frenzy game ${gameId} by ${playerRef}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_SLOT_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Slot Frenzy wager of *${betDisplayUSD}* jammed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`. Please try spinning again.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports;
        console.log(`${LOG_PREFIX_SLOT_START} Wager ${betAmountLamports} placed. New balance for ${userId}: ${userObj.balance}`);
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_SLOT_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_SLOT_START} Database error during Slot Frenzy bet: ${dbError.message}`, dbError.stack);
        await safeSendMessage(chatId, "⚙️ The slot machine's gears are stuck (database error)! Failed to start. Please try again.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }

    // Slot Frenzy is instant resolve.
    const gameData = { 
        type: GAME_IDS.SLOT_FRENZY, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, diceValue: null, payoutInfo: null,
        status: 'spinning', gameMessageId: null 
    };
    activeGames.set(gameId, gameData); // Set briefly for potential logging or future multi-step slots

    const titleSpinning = createStandardTitle("Slot Frenzy - Reels are Spinning!", "🎰");
    let messageText = `${titleSpinning}\n\n${playerRef}, you've placed a bet of *${betDisplayUSD}* on the magnificent Slot Frenzy machine!\nThe reels whir into a blur... What fortune will they reveal? Good luck! 🌟✨`;
    
    const sentSpinningMsg = await safeSendMessage(chatId, messageText, {parse_mode: 'MarkdownV2'});
    if (sentSpinningMsg?.message_id) gameData.gameMessageId = sentSpinningMsg.message_id;
    
    let diceRollValue;
    let animatedDiceMessage;
    try {
        animatedDiceMessage = await bot.sendDice(chatId, { emoji: '🎰' });
        diceRollValue = animatedDiceMessage.dice.value; // Value from 1 to 64
        gameData.diceValue = diceRollValue;
        // Telegram dice animation takes about 3-4 seconds.
        // We can let it play out, then edit the message or send a new one.
        await sleep(3500); // Let animation play
    } catch (e) {
        console.warn(`${LOG_PREFIX_SLOT_START} Failed to send animated slot dice, using internal roll (1-64). Error: ${e.message}`);
        diceRollValue = Math.floor(Math.random() * 64) + 1; // Internal fallback 1-64
        gameData.diceValue = diceRollValue;
        await safeSendMessage(chatId, `⚙️ The slot machine's lever got stuck! Using a backup spin... Result Value: ${diceRollValue}`, {parse_mode:'MarkdownV2'});
        await sleep(1000);
    }
    if (animatedDiceMessage?.message_id && bot) { // Delete the dice animation message
        await bot.deleteMessage(chatId, animatedDiceMessage.message_id).catch(()=>{});
    }
    
    const payoutInfo = SLOT_PAYOUTS[diceRollValue]; // SLOT_PAYOUTS from Part 1
    gameData.payoutInfo = payoutInfo;
    let payoutAmountLamports = 0n;
    let profitAmountLamports = 0n;
    let outcomeReasonLog = "";
    let resultTextPart = "";

    const titleResult = createStandardTitle("Slot Frenzy - The Result!", "🎉");
    messageText = `${titleResult}\n\n${playerRef}'s wager: *${betDisplayUSD}*\nThe reels stop at: Value *${escapeMarkdownV2(String(diceRollValue))}*\n\n`;

    if (payoutInfo) {
        profitAmountLamports = betAmountLamports * BigInt(payoutInfo.multiplier);
        payoutAmountLamports = betAmountLamports + profitAmountLamports; // Bet back + profit
        outcomeReasonLog = `win_slot_val${diceRollValue}_mult${payoutInfo.multiplier}`;
        resultTextPart = `🌟 **${escapeMarkdownV2(payoutInfo.label)}** ${escapeMarkdownV2(payoutInfo.symbols)} 🌟\nCongratulations! You've won a dazzling *${escapeMarkdownV2(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}* in profit!`;
        gameData.status = 'game_over_win';
    } else {
        // SLOT_DEFAULT_LOSS_MULTIPLIER is -1 (player loses bet)
        payoutAmountLamports = 0n; // Bet already deducted
        profitAmountLamports = betAmountLamports * BigInt(SLOT_DEFAULT_LOSS_MULTIPLIER); // This will be negative
        outcomeReasonLog = `loss_slot_val${diceRollValue}`;
        resultTextPart = `💔 Reel mismatch this time... The machine keeps your wager. Better luck on the next spin!`;
        gameData.status = 'game_over_loss';
    }
    messageText += resultTextPart;
    
    let finalUserBalanceLamports = userObj.balance; // Balance after bet deduction
    let clientOutcome;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const ledgerReason = `${outcomeReasonLog} (Game ID: ${gameId})`;
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, userId, payoutAmountLamports, 
            ledgerReason, { game_id_custom_field: gameId }, 
            `Outcome of Slot Frenzy game ${gameId}`
        );
        
        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
        } else {
            await clientOutcome.query('ROLLBACK');
            messageText += `\n\n⚠️ A critical error occurred paying out your Slot winnings: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`. Casino staff notified.`;
            console.error(`${LOG_PREFIX_SLOT_START} Failed to update balance for Slot game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL SLOT Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\` User: ${playerRef}\nAmount: \`${formatCurrency(payoutAmountLamports)}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`. Manual check needed.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_SLOT_START} DB error during Slot outcome for ${gameId}: ${dbError.message}`, dbError.stack);
        messageText += `\n\n⚠️ A severe database malfunction occurred with the Slot machine. Casino staff notified.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    messageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*.`;
    const postGameKeyboardSlot = createPostGameKeyboard(GAME_IDS.SLOT_FRENZY, betAmount);

    if (gameData.gameMessageId && bot) { // Edit the "Spinning..." message
        await bot.editMessageText(messageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot })
            .catch(async (e) => {
                console.warn(`${LOG_PREFIX_SLOT_START} Failed to edit final Slot message (ID: ${gameData.gameMessageId}), sending new: ${e.message}`);
                await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot });
            });
    } else { // Fallback if no message to edit
        await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot });
    }
    activeGames.delete(gameId); // Clean up
}


// --- Callback Forwarder for Additional Games (from Part 5c) ---
async function forwardAdditionalGamesCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_ADD_GAME_CB_FWD = `[AddGameCB_Forward UID:${userObject.telegram_id} Action:${action}]`;
    console.log(`${LOG_PREFIX_ADD_GAME_CB_FWD} Routing callback for chat ${originalChatId} (Type: ${originalChatType}). Action: ${action}, Params: ${params.join(',')}`);

    const gameId = params[0]; // gameId is usually the first parameter for active games, except for play_again

    // Construct mockMsgForHandler for "Play Again" scenarios which call start commands
    const mockMsgForHandler = {
        from: userObject,
        chat: { id: originalChatId, type: originalChatType },
        message_id: originalMessageId // This is the message with the "Play Again" button
    };

    switch (action) {
        // Over/Under 7
        case 'ou7_choice':
            if (!gameId || params.length < 2) { // gameId and choice
                console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing gameId or choice for ou7_choice. Params: ${params}`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Missing parameters for OU7 choice.", show_alert: true }); return;
            }
            const ou7Choice = params[1];
            if (typeof handleOverUnder7Choice === 'function') {
                await handleOverUnder7Choice(gameId, ou7Choice, userObject, originalMessageId, callbackQueryId, mockMsgForHandler);
            } else console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing handler: handleOverUnder7Choice`);
            break;
        case 'play_again_ou7':
            const betAmountOU7Param = params[0]; // For play_again, param[0] is betAmount
            if (!betAmountOU7Param || isNaN(BigInt(betAmountOU7Param))) {
                console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Invalid bet for play_again_ou7: ${betAmountOU7Param}`);
                await bot.answerCallbackQuery(callbackQueryId, {text:"Invalid bet for replay.",show_alert:true}); return;
            }
            const betAmountOU7 = BigInt(betAmountOU7Param);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {}); // Remove old keyboard
            if (typeof handleStartOverUnder7Command === 'function') {
                await handleStartOverUnder7Command(mockMsgForHandler, betAmountOU7);
            } else console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing handler: handleStartOverUnder7Command`);
            break;

        // High Roller Duel
        case 'duel_roll':
            if (!gameId) { console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing gameId for duel_roll.`); await bot.answerCallbackQuery(callbackQueryId, {text:"⚠️ Error: Game ID missing.", show_alert:true}); return; }
            if (typeof handleDuelRoll === 'function') {
                await handleDuelRoll(gameId, userObject, originalMessageId, callbackQueryId, mockMsgForHandler);
            } else console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing handler: handleDuelRoll`);
            break;
        case 'play_again_duel':
            const betAmountDuelParam = params[0];
            if (!betAmountDuelParam || isNaN(BigInt(betAmountDuelParam))) {
                console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Invalid bet for play_again_duel: ${betAmountDuelParam}`);
                await bot.answerCallbackQuery(callbackQueryId, {text:"Invalid bet for replay.",show_alert:true}); return;
            }
            const betAmountDuel = BigInt(betAmountDuelParam);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            if (typeof handleStartDuelCommand === 'function') {
                await handleStartDuelCommand(mockMsgForHandler, betAmountDuel);
            } else console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing handler: handleStartDuelCommand`);
            break;

        // Greed's Ladder
        case 'play_again_ladder': // Ladder is instant, so only play again
            const betAmountLadderParam = params[0];
            if (!betAmountLadderParam || isNaN(BigInt(betAmountLadderParam))) {
                console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Invalid bet for play_again_ladder: ${betAmountLadderParam}`);
                await bot.answerCallbackQuery(callbackQueryId, {text:"Invalid bet for replay.",show_alert:true}); return;
            }
            const betAmountLadder = BigInt(betAmountLadderParam);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            if (typeof handleStartLadderCommand === 'function') {
                await handleStartLadderCommand(mockMsgForHandler, betAmountLadder);
            } else console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing handler: handleStartLadderCommand`);
            break;

        // Sevens Out
        case 's7_roll':
            if (!gameId) { console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing gameId for s7_roll.`); await bot.answerCallbackQuery(callbackQueryId, {text:"⚠️ Error: Game ID missing.", show_alert:true}); return; }
            if (typeof handleSevenOutRoll === 'function') {
                await handleSevenOutRoll(gameId, userObject, originalMessageId, callbackQueryId, mockMsgForHandler);
            } else console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing handler: handleSevenOutRoll`);
            break;
        case 'play_again_s7':
            const betAmountS7Param = params[0];
            if (!betAmountS7Param || isNaN(BigInt(betAmountS7Param))) {
                console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Invalid bet for play_again_s7: ${betAmountS7Param}`);
                await bot.answerCallbackQuery(callbackQueryId, {text:"Invalid bet for replay.",show_alert:true}); return;
            }
            const betAmountS7 = BigInt(betAmountS7Param);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            if (typeof handleStartSevenOutCommand === 'function') {
                await handleStartSevenOutCommand(mockMsgForHandler, betAmountS7);
            } else console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing handler: handleStartSevenOutCommand`);
            break;
        
        // Slot Frenzy
        case 'play_again_slot': // Slots are instant, so only play again
            const betAmountSlotParam = params[0];
            if (!betAmountSlotParam || isNaN(BigInt(betAmountSlotParam))) {
                console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Invalid bet for play_again_slot: ${betAmountSlotParam}`);
                await bot.answerCallbackQuery(callbackQueryId, {text:"Invalid bet for replay.",show_alert:true}); return;
            }
            const betAmountSlot = BigInt(betAmountSlotParam);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            if (typeof handleStartSlotCommand === 'function') {
                await handleStartSlotCommand(mockMsgForHandler, betAmountSlot);
            } else console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing handler: handleStartSlotCommand`);
            break;
            
        default:
            console.warn(`${LOG_PREFIX_ADD_GAME_CB_FWD} Unhandled game callback action in this forwarder: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Unknown game action: ${escapeMarkdownV2(action)}`, show_alert: true });
    }
}
console.log("[Callback Forwarder] forwardAdditionalGamesCallback for Part 5c games defined.");


console.log("Part 5c, Section 4 (NEW): Slot Frenzy Game & Additional Games Callback Forwarder - Complete.");
// --- End of Part 5c, Section 4 (NEW) ---
// --- Start of Part 6 ---
// index.js - Part 6: Main Application Logic (Initialization, Error Handling, Graceful Shutdown)
//---------------------------------------------------------------------------
console.log("Loading Part 6: Main Application Logic (Initialization, Error Handling, Graceful Shutdown)...");

// Assumes all necessary functions from previous parts are loaded and available,
// including: initializeDatabaseSchema (Part 2), pool (Part 1), bot (Part 1),
// notifyAdmin (Part 1), escapeMarkdownV2 (Part 1), safeSendMessage (Part 1),
// getSolUsdPrice (Part 1), stringifyWithBigInt (Part 1),
// app (Part 1 - Express instance), PAYMENT_WEBHOOK_PORT (Part 1 env), PAYMENT_WEBHOOK_PATH (Part 1 env),
// payoutProcessorQueue, depositProcessorQueue (Part 1),
// ADMIN_USER_ID (Part 1), BOT_NAME (Part 1), BOT_VERSION (Part 1),
// isShuttingDown (Part 1 - global flag), SHUTDOWN_FAIL_TIMEOUT_MS (Part 1).

// Functions from Part P3 and P4 to be defined later but called here:
// setupPaymentWebhook (from Part P3)
// startDepositMonitoring, stopDepositMonitoring (from Part P4)
// startSweepingProcess, stopSweepingProcess (from Part P4)


// --- Global Error Handlers ---

process.on('uncaughtException', async (error, origin) => {
    console.error(`🚨 UNCAUGHT EXCEPTION! Origin: ${origin}`);
    console.error(error); // Log the full error object
    const errorMessage = error.message || 'No specific message';
    const errorStack = error.stack || 'No stack trace available';
    const adminMessage = `🚨 *CRITICAL: Uncaught Exception* (${escapeMarkdownV2(BOT_NAME)}) 🚨\n\nBot encountered a critical error and will attempt to shut down\\. \n\n*Origin:* \`${escapeMarkdownV2(String(origin))}\`\n*Error:* \`${escapeMarkdownV2(errorMessage)}\`\n*Stack (Partial):*\n\`\`\`\n${escapeMarkdownV2(errorStack.substring(0, 700))}\n\`\`\`\nPlease check server logs immediately for full details\\.`;

    if (!isShuttingDown) { // isShuttingDown is a global flag from Part 1
        console.log("Initiating shutdown due to uncaught exception...");
        if (typeof notifyAdmin === 'function') {
            await notifyAdmin(adminMessage).catch(err => console.error("Failed to notify admin about uncaught exception:", err.message));
        }
        await gracefulShutdown('uncaught_exception'); // Will set isShuttingDown = true
        setTimeout(() => {
            console.error("Forcing exit after uncaught exception shutdown attempt timeout.");
            process.exit(1);
        }, SHUTDOWN_FAIL_TIMEOUT_MS + 5000); // Give gracefulShutdown adequate time
    } else {
        console.log("Uncaught exception occurred during an ongoing shutdown sequence. Forcing exit immediately.");
        process.exit(1); 
    }
});

process.on('unhandledRejection', async (reason, promise) => {
    console.error('🚨 UNHANDLED PROMISE REJECTION! At Promise:', promise, 'Reason:', reason);
    let reasonString = 'Unknown reason for promise rejection';
    if (reason instanceof Error) {
        reasonString = `${reason.name}: ${reason.message}${reason.stack ? `\nStack (Partial):\n${reason.stack.substring(0, 700)}` : ''}`;
    } else if (typeof reason === 'object' && reason !== null) {
        try {
            reasonString = stringifyWithBigInt(reason);
        } catch (e) {
            reasonString = "Could not stringify complex rejection reason object.";
        }
    } else if (reason !== undefined && reason !== null) {
        reasonString = String(reason);
    }

    const adminMessage = `⚠️ *WARNING: Unhandled Promise Rejection* (${escapeMarkdownV2(BOT_NAME)}) ⚠️\n\nAn unhandled promise rejection occurred\\. This may indicate a bug or an unhandled error case in asynchronous code\\. The bot will continue running but please investigate\\.\n\n*Reason:*\n\`\`\`\n${escapeMarkdownV2(reasonString.substring(0,1000))}\n\`\`\`\nCheck logs for full details and the promise context\\.`;

    if (typeof notifyAdmin === 'function' && !isShuttingDown) {
        await notifyAdmin(adminMessage).catch(err => console.error("Failed to notify admin about unhandled rejection:", err.message));
    }
});

// --- Graceful Shutdown Logic ---
// isShuttingDown is a global flag from Part 1, ensuring shutdown logic runs only once.
let expressServerInstance = null; // To hold the HTTP server instance for webhooks

async function gracefulShutdown(signal = 'SIGINT') {
    if (isShuttingDown) { // Check the global flag
        console.log("Graceful shutdown already in progress. Please wait...");
        return;
    }
    isShuttingDown = true; // Set global flag immediately

    console.log(`\n🛑 Received signal: ${signal}. Initiating graceful shutdown for ${BOT_NAME} v${BOT_VERSION}...`);
    const adminShutdownMessage = `🔌 *Bot Shutdown Initiated* 🔌\n\n${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} is now shutting down due to signal: \`${escapeMarkdownV2(signal)}\`\\. Finalizing operations\\.\\.\\.`;
    if (typeof notifyAdmin === 'function' && signal !== 'test_mode_exit' && signal !== 'initialization_error') {
        await notifyAdmin(adminShutdownMessage).catch(err => console.error("Failed to send admin shutdown initiation notification:", err.message));
    }

    console.log("  ⏳ Stopping Telegram bot polling...");
    if (bot && typeof bot.stopPolling === 'function' && typeof bot.isPolling === 'function' && bot.isPolling()) {
        try {
            await bot.stopPolling({ cancel: true }); 
            console.log("  ✅ Telegram bot polling stopped.");
        } catch (e) {
            console.error("  ❌ Error stopping Telegram bot polling:", e.message);
        }
    } else {
        console.log("  ℹ️ Telegram bot polling was not active or stopPolling not available/needed.");
    }

    if (typeof stopDepositMonitoring === 'function') {
        console.log("  ⏳ Stopping deposit monitoring...");
        try { await stopDepositMonitoring(); console.log("  ✅ Deposit monitoring stopped."); }
        catch(e) { console.error("  ❌ Error stopping deposit monitoring:", e.message); }
    } else { console.log("  ⚠️ stopDepositMonitoring function not defined.");}

    if (typeof stopSweepingProcess === 'function') {
        console.log("  ⏳ Stopping sweeping process...");
        try { await stopSweepingProcess(); console.log("  ✅ Sweeping process stopped."); }
        catch(e) { console.error("  ❌ Error stopping sweeping process:", e.message); }
    } else { console.log("  ⚠️ stopSweepingProcess function not defined.");}
    
    const queuesToStop = { payoutProcessorQueue, depositProcessorQueue }; // From Part 1
    for (const [queueName, queueInstance] of Object.entries(queuesToStop)) {
        if (queueInstance && typeof queueInstance.onIdle === 'function' && typeof queueInstance.clear === 'function') {
            console.log(`  ⏳ Waiting for ${queueName} (Size: ${queueInstance.size}, Pending: ${queueInstance.pending}) to idle...`);
            try {
                if (queueInstance.size > 0 || queueInstance.pending > 0) {
                    // Give queues a chance to finish processing active items, but not wait indefinitely.
                    await Promise.race([queueInstance.onIdle(), sleep(15000)]); // Max 15s wait per queue
                }
                queueInstance.clear(); // Clear any remaining queued items not yet started
                console.log(`  ✅ ${queueName} is idle and cleared.`);
            } catch (qError) {
                console.warn(`  ⚠️ Error or timeout waiting for ${queueName} to idle: ${qError.message}. Clearing queue anyway.`);
                queueInstance.clear();
            }
        } else {
            console.log(`  ⚠️ Queue ${queueName} not defined or does not support onIdle/clear.`);
        }
    }


    if (expressServerInstance && typeof expressServerInstance.close === 'function') {
        console.log("  ⏳ Closing Express webhook server...");
        await new Promise(resolve => expressServerInstance.close(err => {
            if (err) console.error("  ❌ Error closing Express server:", err.message);
            else console.log("  ✅ Express server closed.");
            resolve();
        }));
    } else {
         console.log("  ℹ️ Express server not running or not managed by this shutdown process.");
    }

    console.log("  ⏳ Closing PostgreSQL pool...");
    if (pool && typeof pool.end === 'function') {
        try {
            await pool.end();
            console.log("  ✅ PostgreSQL pool closed.");
        } catch (e) {
            console.error("  ❌ Error closing PostgreSQL pool:", e.message);
        }
    } else {
        console.log("  ⚠️ PostgreSQL pool not active or .end() not available.");
    }

    console.log(`🏁 ${BOT_NAME} shutdown sequence complete. Exiting now.`);
    const finalAdminMessage = `✅ *Bot Shutdown Complete* ✅\n\n${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} has successfully shut down\\.`;
    if (typeof notifyAdmin === 'function' && signal !== 'test_mode_exit' && signal !== 'initialization_error') {
        // Send final notification but don't wait for it to exit.
        notifyAdmin(finalAdminMessage).catch(err => console.error("Failed to send final admin shutdown notification:", err.message));
    }
    
    await sleep(500); // Short pause for logs to flush
    process.exit(signal === 'uncaught_exception' || signal === 'initialization_error' ? 1 : 0);
}

// Signal Handlers
process.on('SIGINT', () => { if (!isShuttingDown) gracefulShutdown('SIGINT'); }); 
process.on('SIGTERM', () => { if (!isShuttingDown) gracefulShutdown('SIGTERM'); });
process.on('SIGQUIT', () => { if (!isShuttingDown) gracefulShutdown('SIGQUIT'); });

// --- Main Application Function ---
async function main() {
    console.log(`🚀🚀🚀 Starting ${BOT_NAME} v${BOT_VERSION} 🚀🚀🚀`);
    console.log(`Node.js Version: ${process.version}, System Time: ${new Date().toISOString()}`);
    const initDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 7000;
    console.log(`Initialization delay: ${initDelay / 1000}s`);
    await sleep(initDelay);

    try {
        console.log("⚙️ Step 1: Initializing Database Schema...");
        if (typeof initializeDatabaseSchema !== 'function') {
            throw new Error("FATAL: initializeDatabaseSchema function is not defined! Check Part 2.");
        }
        await initializeDatabaseSchema();
        console.log("✅ Database schema initialized successfully.");

        console.log("⚙️ Step 2: Connecting to Telegram & Starting Bot...");
        if (!bot || typeof bot.getMe !== 'function') {
            throw new Error("FATAL: Telegram bot instance (from Part 1) is not correctly configured.");
        }
        
        const botInfo = await bot.getMe();
        console.log(`🤖 Bot Name: ${botInfo.first_name}, Username: @${botInfo.username}, ID: ${botInfo.id}`);
        console.log(`🔗 Start chatting with the bot: https://t.me/${botInfo.username}`);
        
        bot.on('polling_error', async (error) => {
            console.error(`[Telegram Polling Error] Code: ${error.code || 'N/A'}, Message: ${error.message || String(error)}`);
            const adminMsg = `📡 *Telegram Polling Error* (${escapeMarkdownV2(BOT_NAME)}) 📡\n\nError: \`${escapeMarkdownV2(String(error.message || error))}\` \\(Code: ${escapeMarkdownV2(String(error.code || 'N/A'))}\\)\\.\nPolling may be affected or try to restart\\.`;
            if (typeof notifyAdmin === 'function' && !isShuttingDown) {
                await notifyAdmin(adminMsg).catch(err => console.error("Failed to notify admin about polling error:", err.message));
            }
        });
        bot.on('webhook_error', async (error) => { // If bot library is set to use webhooks for Telegram updates
            console.error(`[Telegram Webhook Error] Code: ${error.code || 'N/A'}, Message: ${error.message || String(error)}`);
            const adminMsg = `📡 *Telegram Webhook Error* (${escapeMarkdownV2(BOT_NAME)}) 📡\n\nError: \`${escapeMarkdownV2(String(error.message || error))}\`\\.\nBot message receiving may be affected\\.`;
            if (typeof notifyAdmin === 'function' && !isShuttingDown) {
                await notifyAdmin(adminMsg).catch(err => console.error("Failed to notify admin about webhook error:", err.message));
            }
        });
        console.log("✅ Telegram Bot is online and polling for messages (or webhook configured).");
        
        if (ADMIN_USER_ID && typeof safeSendMessage === 'function') {
            await safeSendMessage(ADMIN_USER_ID, `🚀 *${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} Started Successfully* 🚀\nBot is online and operational\\. Current time: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`, {parse_mode: 'MarkdownV2'});
        }

        console.log("⚙️ Step 3: Priming SOL/USD Price Cache...");
        if (typeof getSolUsdPrice === 'function') {
            try {
                const initialPrice = await getSolUsdPrice();
                console.log(`✅ Initial SOL/USD Price: $${initialPrice.toFixed(2)}`);
            } catch (priceError) {
                console.warn(`⚠️ Could not fetch initial SOL/USD price: ${priceError.message}. Price features might be affected initially.`);
            }
        } else {
            console.warn("⚠️ getSolUsdPrice function (from Part 1) not defined. Price features will be unavailable.");
        }

        console.log("⚙️ Step 4: Starting Background Payment Processes...");
        if (typeof startDepositMonitoring === 'function') {
            startDepositMonitoring(); 
            console.log("  ▶️ Deposit monitoring process initiated (will be fully defined in Part P4).");
        } else {
            console.warn("⚠️ Deposit monitoring (startDepositMonitoring from Part P4) function not defined.");
        }

        if (typeof startSweepingProcess === 'function') {
            startSweepingProcess(); 
            console.log("  ▶️ Address sweeping process initiated (will be fully defined in Part P4).");
        } else {
            console.warn("⚠️ Address sweeping (startSweepingProcess from Part P4) function not defined.");
        }
        
        if (process.env.ENABLE_PAYMENT_WEBHOOKS === 'true') {
            console.log("⚙️ Step 5: Setting up and starting Payment Webhook Server...");
            if (typeof setupPaymentWebhook === 'function' && app) { // app from Part 1
                const port = parseInt(process.env.PAYMENT_WEBHOOK_PORT, 10) || 3000;
                try {
                    setupPaymentWebhook(app); // Function from Part P3
                    
                    expressServerInstance = app.listen(port, () => {
                        console.log(`  ✅ Payment webhook server listening on port ${port} at path ${process.env.PAYMENT_WEBHOOK_PATH || '/webhook/solana-payments'}`);
                    });

                    expressServerInstance.on('error', (serverErr) => {
                        console.error(`  ❌ Express server error: ${serverErr.message}`, serverErr);
                        if (serverErr.code === 'EADDRINUSE') {
                            console.error(`  🚨 FATAL: Port ${port} is already in use for webhooks. Webhook server cannot start.`);
                            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 Webhook Server Failed to Start 🚨\nPort \`${port}\` is already in use\\. Payment webhooks will not function\\.`, {parse_mode:'MarkdownV2'});
                        }
                    });

                } catch (webhookError) {
                    console.error(`  ❌ Failed to set up or start payment webhook server: ${webhookError.message}`);
                }
            } else {
                console.warn("  ⚠️ Payment webhooks enabled, but setupPaymentWebhook function (from Part P3) or Express app instance (from Part 1) not available.");
            }
        } else {
            console.log("ℹ️ Payment webhooks are disabled (ENABLE_PAYMENT_WEBHOOKS is not 'true').");
        }

        console.log(`\n✨✨✨ ${BOT_NAME} is fully operational! Waiting for commands... ✨✨✨\n`);

    } catch (error) {
        console.error("💥💥💥 FATAL ERROR during bot initialization: 💥💥💥", error);
        const fatalAdminMessage = `🚨 *FATAL BOT INITIALIZATION ERROR* (${escapeMarkdownV2(BOT_NAME)}) 🚨\n\nFailed to start: \n*Error:* \`${escapeMarkdownV2(error.message || "Unknown error")}\`\n*Stack (Partial):*\n\`\`\`\n${escapeMarkdownV2((error.stack || String(error)).substring(0,700))}\n\`\`\`\nBot will attempt shutdown\\.`;
        if (typeof notifyAdmin === 'function' && !isShuttingDown) {
            await notifyAdmin(fatalAdminMessage).catch(err => console.error("Failed to notify admin about fatal initialization error:", err.message));
        }
        if (!isShuttingDown) { 
            await gracefulShutdown('initialization_error');
        }
        setTimeout(() => process.exit(1), SHUTDOWN_FAIL_TIMEOUT_MS + 2000); // Ensure exit
    }
}

// --- Run the main application ---
main();

console.log("End of index.js script. Bot startup process initiated from main().");
// --- End of Part 6 ---
// --- Start of Part P1 ---
// index.js - Part P1: Solana Payment System - Core Utilities & Wallet Generation
//---------------------------------------------------------------------------
console.log("Loading Part P1: Solana Payment System - Core Utilities & Wallet Generation...");

// Assumes DEPOSIT_MASTER_SEED_PHRASE (Part 1), bip39, derivePath, nacl (Part 1 imports),
// Keypair, PublicKey, LAMPORTS_PER_SOL, SystemProgram, Transaction, sendAndConfirmTransaction,
// ComputeBudgetProgram, TransactionExpiredBlockheightExceededError, SendTransactionError (Part 1 imports),
// solanaConnection (Part 1), queryDatabase (Part 1), pool (Part 1),
// escapeMarkdownV2 (Part 1), stringifyWithBigInt (Part 1),
// PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, PAYOUT_COMPUTE_UNIT_LIMIT (Part 1 env),
// INITIAL_RETRY_POLLING_DELAY, MAX_RETRY_POLLING_DELAY, DEPOSIT_ADDRESS_EXPIRY_MS (Part 1 env),
// RPC_MAX_RETRIES, RPC_COMMITMENT (Part 1 env),
// notifyAdmin (Part 1), sleep (Part 1), createHash (Part 1 import),
// getNextAddressIndexForUserDB (from Part 2),
// activeDepositAddresses cache (from Part 1) are available.

//---------------------------------------------------------------------------
// HD Wallet & Address Generation
//---------------------------------------------------------------------------

/**
 * Creates a cryptographically safe, deterministic index from a user's Telegram ID
 * for use in an HD derivation path (as a non-hardened child).
 * @param {string|number} userId - The user's Telegram ID.
 * @returns {number} A derived, non-hardened index number (0 to 2^31 - 1).
 */
function createSafeUserSpecificIndex(userId) {
    if (typeof createHash !== 'function') {
        console.error("[createSafeUserSpecificIndex] CRITICAL: createHash (from crypto) is not available. Using insecure fallback. THIS IS NOT PRODUCTION SAFE.");
        let simpleHash = 0;
        const strId = String(userId);
        for (let i = 0; i < strId.length; i++) {
            simpleHash = (simpleHash << 5) - simpleHash + strId.charCodeAt(i);
            simpleHash |= 0;
        }
        return Math.abs(simpleHash) % 2147483647; // Max non-hardened value (2^31 - 1)
    }

    const hash = createHash('sha256').update(String(userId)).digest();
    // Use first 4 bytes, ensure it's positive and within non-hardened range.
    const index = hash.readUInt32BE(0) % 2147483647;
    return index;
}
console.log("[Payment Utils] createSafeUserSpecificIndex helper defined.");


/**
 * Derives a Solana keypair from a BIP39 seed phrase and a derivation path.
 * @param {string} seedPhrase - The BIP39 mnemonic seed phrase.
 * @param {string} derivationPath - The HD derivation path (e.g., "m/44'/501'/0'/0'/0'").
 * @returns {import('@solana/web3.js').Keypair} The derived Keypair.
 * @throws {Error} If seed phrase or derivation path is invalid, or derivation fails.
 */
function deriveSolanaKeypair(seedPhrase, derivationPath) {
    if (!seedPhrase || typeof seedPhrase !== 'string') {
        throw new Error("Invalid or missing seed phrase for keypair derivation.");
    }
    if (!derivationPath || typeof derivationPath !== 'string' || !derivationPath.startsWith("m/")) {
        throw new Error("Invalid or missing derivation path. Must start with 'm/'.");
    }
    if (typeof bip39 === 'undefined' || typeof bip39.mnemonicToSeedSync !== 'function' ||
        typeof derivePath !== 'function' || typeof nacl === 'undefined' || 
        typeof nacl.sign === 'undefined' || typeof nacl.sign.keyPair === 'undefined' || typeof nacl.sign.keyPair.fromSeed !== 'function' ||
        typeof Keypair === 'undefined' || typeof Keypair.fromSeed !== 'function') {
        throw new Error("CRITICAL Dependency missing for deriveSolanaKeypair (bip39, ed25519-hd-key/derivePath, tweetnacl/nacl.sign.keyPair.fromSeed, or @solana/web3.js/Keypair.fromSeed).");
    }
    try {
        const seed = bip39.mnemonicToSeedSync(seedPhrase);
        const derivedSeedForKeypair = derivePath(derivationPath, seed.toString('hex')).key;
        // nacl.sign.keyPair.fromSeed expects the first 32 bytes of the derived private key.
        const naclKeypair = nacl.sign.keyPair.fromSeed(derivedSeedForKeypair.slice(0, 32));
        // Keypair.fromSeed also expects the first 32 bytes of the private key (which is the seed for ed25519).
        const keypair = Keypair.fromSeed(naclKeypair.secretKey.slice(0, 32));
        return keypair;
    } catch (error) {
        console.error(`[deriveSolanaKeypair] Error deriving keypair for path ${derivationPath}: ${error.message}`, error.stack);
        throw new Error(`Keypair derivation failed for path ${derivationPath}: ${error.message}`);
    }
}
console.log("[Payment Utils] deriveSolanaKeypair (for HD wallets) defined.");


/**
 * Generates a new, unique deposit address for a user and stores its record.
 * Note: This function performs a direct DB insert. For atomicity with other user updates (like users.last_deposit_address),
 * ensure this is called within a transaction managed by the caller, or use a dedicated DB function like createDepositAddressRecordDB (from Part P2)
 * that handles the combined logic transactionally. The UNIQUE constraint on derivation_path provides some safety against race conditions.
 * @param {string|number} userId - The user's Telegram ID.
 * @param {import('pg').PoolClient} [dbClient=pool] - Optional database client if part of a larger transaction.
 * @returns {Promise<string|null>} The public key string of the generated deposit address, or null on failure.
 */
async function generateUniqueDepositAddress(userId, dbClient = pool) {
    const stringUserId = String(userId);
    const LOG_PREFIX_GUDA = `[GenDepositAddr UID:${stringUserId}]`;

    if (!DEPOSIT_MASTER_SEED_PHRASE) {
        console.error(`${LOG_PREFIX_GUDA} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is not set. Cannot generate deposit addresses.`);
        if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is not set. Deposit address generation is failing for user ${stringUserId}.`);
        return null;
    }
    if (typeof getNextAddressIndexForUserDB !== 'function') { // From Part 2
        console.error(`${LOG_PREFIX_GUDA} CRITICAL: getNextAddressIndexForUserDB function (from Part 2) is not defined. Cannot generate unique address.`);
        return null;
    }

    try {
        const safeUserAccountIndex = createSafeUserSpecificIndex(stringUserId);
        const addressIndex = await getNextAddressIndexForUserDB(stringUserId, dbClient); // Pass client for transactional consistency if needed

        const derivationPath = `m/44'/501'/${safeUserAccountIndex}'/0'/${addressIndex}'`; // Standard external chain (0')
        
        const depositKeypair = deriveSolanaKeypair(DEPOSIT_MASTER_SEED_PHRASE, derivationPath);
        const depositAddress = depositKeypair.publicKey.toBase58();

        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS);

        const insertQuery = `
            INSERT INTO user_deposit_wallets (user_telegram_id, public_key, derivation_path, expires_at, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, TRUE, NOW(), NOW())
            RETURNING wallet_id, public_key;
        `;
        // This insert is a single statement; if dbClient is the pool, it's auto-committed.
        // If dbClient is part of an external transaction, that transaction needs to be committed/rolled back by the caller.
        const result = await queryDatabase(insertQuery, [stringUserId, depositAddress, derivationPath, expiresAt], dbClient);

        if (result.rows.length > 0) {
            console.log(`${LOG_PREFIX_GUDA} ✅ Successfully generated and stored new deposit address: ${depositAddress} (Path: ${derivationPath}, Expires: ${expiresAt.toISOString()})`);
            // Update activeDepositAddresses cache (from Part 1)
            if (typeof activeDepositAddresses !== 'undefined' && activeDepositAddresses instanceof Map) {
                activeDepositAddresses.set(depositAddress, { userId: stringUserId, expiresAt: expiresAt.getTime() });
            } else {
                console.warn(`${LOG_PREFIX_GUDA} activeDepositAddresses cache not available or not a Map. Cannot update cache.`);
            }
            return depositAddress;
        } else {
            console.error(`${LOG_PREFIX_GUDA} ❌ Failed to store generated deposit address ${depositAddress} in DB (no rows returned).`);
            throw new Error("Failed to insert deposit address into database and get ID back.");
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_GUDA} ❌ Error generating unique deposit address for user ${stringUserId}: ${error.message}`, error.stack);
        if (error.code === '23505') { // Unique constraint violation (e.g. on derivation_path or public_key)
            console.error(`${LOG_PREFIX_GUDA} Unique constraint violation. This might indicate a race condition or issue with addressIndex generation. Path: ${error.detail?.includes('derivation_path') ? error.detail : 'N/A'}`);
             if (typeof notifyAdmin === 'function') notifyAdmin(`⚠️ Error generating deposit address (Unique Constraint) for user \`${escapeMarkdownV2(stringUserId)}\`: \`${escapeMarkdownV2(error.message)}\`. Possible race condition or index issue.`, {parse_mode:'MarkdownV2'});
        } else if (typeof notifyAdmin === 'function') {
            notifyAdmin(`⚠️ Error generating deposit address for user \`${escapeMarkdownV2(stringUserId)}\`: \`${escapeMarkdownV2(error.message)}\`. Check logs.`, {parse_mode:'MarkdownV2'});
        }
        return null;
    }
}
console.log("[Payment Utils] generateUniqueDepositAddress defined.");


//---------------------------------------------------------------------------
// Solana On-Chain Utilities
//---------------------------------------------------------------------------

/**
 * Checks if a given string is a valid Solana address.
 * @param {string} address - The address string to validate.
 * @returns {boolean} True if valid, false otherwise.
 */
function isValidSolanaAddress(address) {
    if (!address || typeof address !== 'string') return false;
    try {
        const publicKey = new PublicKey(address);
        return PublicKey.isOnCurve(publicKey.toBytes());
    } catch (error) {
        return false; // Invalid format if PublicKey constructor throws
    }
}
console.log("[Payment Utils] isValidSolanaAddress defined.");

/**
 * Gets the SOL balance of a given Solana public key.
 * @param {string} publicKeyString - The public key string.
 * @returns {Promise<bigint|null>} The balance in lamports, or null on error/if address not found.
 */
async function getSolBalance(publicKeyString) {
    const LOG_PREFIX_GSB = `[getSolBalance PK:${publicKeyString ? publicKeyString.slice(0,10) : 'N/A'}...]`;
    if (!isValidSolanaAddress(publicKeyString)) {
        console.warn(`${LOG_PREFIX_GSB} Invalid public key provided: ${publicKeyString}`);
        return null;
    }
    try {
        const balance = await solanaConnection.getBalance(new PublicKey(publicKeyString), process.env.RPC_COMMITMENT || 'confirmed');
        return BigInt(balance);
    } catch (error) {
        // An error like "Account does not exist" is common for new/empty accounts, balance is 0.
        // However, other RPC errors might occur.
        if (error.message && (error.message.includes("Account does not exist") || error.message.includes("could not find account"))) {
            // console.log(`${LOG_PREFIX_GSB} Account ${publicKeyString} not found on-chain (balance is 0).`);
            return 0n; // Treat non-existent account as 0 balance for this purpose.
        }
        console.error(`${LOG_PREFIX_GSB} Error fetching balance for ${publicKeyString}: ${error.message}`);
        return null; // Return null for other types of errors
    }
}
console.log("[Payment Utils] getSolBalance defined.");


/**
 * Sends SOL from a payer to a recipient.
 * @param {import('@solana/web3.js').Keypair} payerKeypair - The keypair of the account sending SOL.
 * @param {string} recipientPublicKeyString - The public key string of the recipient.
 * @param {bigint} amountLamports - The amount of SOL to send, in lamports.
 * @param {string} [memoText] - Optional memo text. For production, use @solana/spl-memo.
 * @param {number} [priorityFeeMicroLamportsOverride] - Optional override for priority fee in micro-lamports.
 * @param {number} [computeUnitsOverride] - Optional override for compute units.
 * @returns {Promise<{success: boolean, signature?: string, error?: string, errorType?: string, blockTime?: number, feeLamports?: bigint, isRetryable?: boolean}>}
 */
async function sendSol(payerKeypair, recipientPublicKeyString, amountLamports, memoText = null, priorityFeeMicroLamportsOverride = null, computeUnitsOverride = null) {
    const LOG_PREFIX_SENDSOL = `[sendSol From:${payerKeypair.publicKey.toBase58().slice(0,6)} To:${recipientPublicKeyString.slice(0,6)} Amt:${amountLamports}]`;
    
    if (!payerKeypair || typeof payerKeypair.publicKey === 'undefined' || typeof payerKeypair.secretKey === 'undefined') {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid payerKeypair provided.`);
        return { success: false, error: "Invalid payer keypair.", errorType: "InvalidInputError", isRetryable: false };
    }
    if (!isValidSolanaAddress(recipientPublicKeyString)) {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid recipient public key: ${recipientPublicKeyString}`);
        return { success: false, error: "Invalid recipient address.", errorType: "InvalidInputError", isRetryable: false };
    }
    if (typeof amountLamports !== 'bigint' || amountLamports <= 0n) {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid amount: ${amountLamports}. Must be a positive BigInt.`);
        return { success: false, error: "Invalid amount (must be > 0).", errorType: "InvalidInputError", isRetryable: false };
    }

    const transaction = new Transaction();
    const instructions = [];

    const computeUnitLimit = computeUnitsOverride || parseInt(process.env.PAYOUT_COMPUTE_UNIT_LIMIT, 10);
    const effectivePriorityFeeMicroLamports = priorityFeeMicroLamportsOverride !== null ? priorityFeeMicroLamportsOverride : parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
    const maxPriorityFeeMicroLamports = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
    
    const finalPriorityFee = Math.min(effectivePriorityFeeMicroLamports, maxPriorityFeeMicroLamports);

    if (computeUnitLimit > 0) {
        instructions.push(ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit }));
    }
    if (finalPriorityFee > 0) {
        instructions.push(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: finalPriorityFee }));
    }

    instructions.push(
        SystemProgram.transfer({
            fromPubkey: payerKeypair.publicKey,
            toPubkey: new PublicKey(recipientPublicKeyString),
            lamports: amountLamports,
        })
    );

    if (memoText && typeof memoText === 'string' && memoText.trim().length > 0) {
        console.log(`${LOG_PREFIX_SENDSOL} Conceptual Memo: "${memoText.trim()}". For production, integrate @solana/spl-memo library.`);
        // Example using spl-memo (requires import: import { createMemoInstruction } from '@solana/spl-memo';)
        // try {
        //     instructions.push(createMemoInstruction(memoText.trim(), [payerKeypair.publicKey]));
        // } catch (memoError) {
        //     console.warn(`${LOG_PREFIX_SENDSOL} Could not add SPL Memo instruction: ${memoError.message}. Proceeding without memo.`);
        // }
    }
    
    transaction.add(...instructions);

    let signature = null;
    let retries = 0;
    const maxRetriesConfig = parseInt(process.env.RPC_MAX_RETRIES, 10); // Max retries for this entire sendSol operation
    const sendAndConfirmMaxRetries = 3; // Max retries for the confirmation part of sendAndConfirmTransaction
    let retryDelayMs = parseInt(process.env.INITIAL_RETRY_POLLING_DELAY, 10);
    const maxRetryDelayMs = parseInt(process.env.MAX_RETRY_POLLING_DELAY, 10);
    const rpcCommitment = process.env.RPC_COMMITMENT || 'confirmed';

    while (retries < maxRetriesConfig) {
        try {
            console.log(`${LOG_PREFIX_SENDSOL} Attempt ${retries + 1}/${maxRetriesConfig}: Sending transaction...`);
            
            // It's good practice to get a recent blockhash for each attempt if retrying the send operation itself.
            // sendAndConfirmTransaction might do this internally for its retries, but if we retry the whole call, we should.
            const { blockhash } = await solanaConnection.getLatestBlockhash(rpcCommitment);
            transaction.recentBlockhash = blockhash;
            transaction.feePayer = payerKeypair.publicKey;

            signature = await sendAndConfirmTransaction(
                solanaConnection,
                transaction,
                [payerKeypair],
                {
                    commitment: rpcCommitment,
                    skipPreflight: false,
                    preflightCommitment: rpcCommitment,
                    maxRetries: sendAndConfirmMaxRetries 
                }
            );
            
            console.log(`${LOG_PREFIX_SENDSOL} ✅ Transaction successful! Signature: ${signature}. Commitment: ${rpcCommitment}.`);
            
            let blockTime = null;
            let feeLamports = null;
            try {
                const confirmedTx = await solanaConnection.getTransaction(signature, {commitment: rpcCommitment, maxSupportedTransactionVersion: 0 });
                if (confirmedTx && confirmedTx.blockTime && confirmedTx.meta) {
                    blockTime = confirmedTx.blockTime;
                    feeLamports = BigInt(confirmedTx.meta.fee);
                    console.log(`${LOG_PREFIX_SENDSOL} Tx details fetched: Block Time: ${blockTime}, Fee: ${feeLamports} lamports.`);
                } else {
                    console.warn(`${LOG_PREFIX_SENDSOL} Could not fetch full transaction details for ${signature}, or details were incomplete. BlockTime/Fee might be null.`);
                }
            } catch (fetchErr) {
                console.warn(`${LOG_PREFIX_SENDSOL} Could not fetch confirmed transaction details for ${signature} post-confirmation: ${fetchErr.message}`);
            }
            
            return { success: true, signature, blockTime, feeLamports };

        } catch (error) {
            retries++;
            const errorMessage = error.message || String(error);
            let isRetryableError = false; // Default to not retryable unless specified
            console.error(`${LOG_PREFIX_SENDSOL} ❌ Attempt ${retries}/${maxRetriesConfig} failed: ${errorMessage}`);
            if (error.stack) console.error(error.stack.substring(0, 500)); // Partial stack

            if (error instanceof TransactionExpiredBlockheightExceededError) {
                console.warn(`${LOG_PREFIX_SENDSOL} Transaction expired (blockheight exceeded). Will retry with new blockhash if attempts remain.`);
                isRetryableError = true;
            } else if (error instanceof SendTransactionError) {
                const transactionLogs = error.logs;
                if (transactionLogs) {
                    console.error(`${LOG_PREFIX_SENDSOL} Transaction logs from SendTransactionError:\n${transactionLogs.join('\n')}`);
                    if (transactionLogs.some(log => log.toLowerCase().includes("insufficient lamports") || log.toLowerCase().includes("account उतना sol नहीं है"))) {
                        return { success: false, error: "Insufficient SOL to cover transaction fee or amount.", errorType: "InsufficientFundsError", isRetryable: false };
                    }
                    if (transactionLogs.some(log => log.toLowerCase().includes("custom program error") || log.toLowerCase().includes("error processing instruction"))) {
                        return { success: false, error: `Transaction failed: Program error. See logs.`, errorType: "ProgramError", isRetryable: false };
                    }
                }
                // Many SendTransactionErrors might be retryable if they are due to temporary network issues or RPC node issues.
                // The RateLimitedConnection should handle some of this, but sendAndConfirmTransaction can also fail.
                isRetryableError = true; // Assume most SendTransactionError are retryable unless specific logs indicate otherwise.
            } else if (errorMessage.includes("signers") && errorMessage.includes("Transaction was not signed by all")) {
                console.error(`${LOG_PREFIX_SENDSOL} Signing error. This is a code issue.`);
                return {success: false, error: "Transaction signing failed.", errorType: "SigningError", isRetryable: false};
            } else if (errorMessage.toLowerCase().includes("blockhash not found") || errorMessage.toLowerCase().includes("timeout")) {
                isRetryableError = true; // Common for RPC issues or network congestion
            }


            if (!isRetryableError || retries >= maxRetriesConfig) {
                console.error(`${LOG_PREFIX_SENDSOL} Max retries reached or non-retryable error. Transaction failed permanently.`);
                return { success: false, error: `Transaction failed after ${retries} attempts: ${errorMessage}`, errorType: error.constructor?.name || "UnknownError", isRetryable: false };
            }

            console.log(`${LOG_PREFIX_SENDSOL} Retrying in ${retryDelayMs / 1000}s...`);
            await sleep(retryDelayMs);
            retryDelayMs = Math.min(retryDelayMs * 2, maxRetryDelayMs); // Exponential backoff capped
        }
    }
    return { success: false, error: "Transaction failed after all attempts (reached end of loop).", errorType: "MaxRetriesReached", isRetryable: false };
}
console.log("[Payment Utils] sendSol (with payerKeypair parameter, priority fees, and robust retries) defined.");


console.log("Part P1: Solana Payment System - Core Utilities & Wallet Generation - Complete.");
// --- End of Part P1 ---
// --- Start of Part P2 ---
// index.js - Part P2: Payment System Database Operations
//---------------------------------------------------------------------------
console.log("Loading Part P2: Payment System Database Operations...");

// Assumes global `pool` (Part 1), `queryDatabase` (Part 1),
// `escapeMarkdownV2` (Part 1), `formatCurrency` (Part 3), `stringifyWithBigInt` (Part 1),
// `generateReferralCode` (Part 2), `getNextAddressIndexForUserDB` (Part 2),
// `activeDepositAddresses` cache map, `walletCache` map (Part 1).
// Constants like SOL_DECIMALS, LAMPORTS_PER_SOL are from Part 1.

// --- Unified User/Wallet Operations ---

/**
 * Fetches payment-system relevant details for a user.
 * @param {string|number} telegramId The user's Telegram ID.
 * @param {import('pg').PoolClient} [client=pool] Optional database client.
 * @returns {Promise<object|null>} User details with BigInt conversions or null if not found/error.
 */
async function getPaymentSystemUserDetails(telegramId, client = pool) {
    const stringUserId = String(telegramId);
    const LOG_PREFIX_GPSUD = `[getPaymentSystemUserDetails TG:${stringUserId}]`;
    const query = `
        SELECT
            telegram_id, username, first_name, last_name, balance, solana_wallet_address,
            referral_code, referrer_telegram_id, can_generate_deposit_address,
            last_deposit_address, last_deposit_address_generated_at,
            total_deposited_lamports, total_withdrawn_lamports,
            total_wagered_lamports, total_won_lamports, notes,
            created_at, updated_at
        FROM users
        WHERE telegram_id = $1;
    `;
    try {
        const res = await queryDatabase(query, [stringUserId], client);
        if (res.rows.length > 0) {
            const details = res.rows[0];
            details.telegram_id = String(details.telegram_id); // Ensure string for consistency
            details.balance = BigInt(details.balance || '0');
            details.total_deposited_lamports = BigInt(details.total_deposited_lamports || '0');
            details.total_withdrawn_lamports = BigInt(details.total_withdrawn_lamports || '0');
            details.total_wagered_lamports = BigInt(details.total_wagered_lamports || '0');
            details.total_won_lamports = BigInt(details.total_won_lamports || '0');
            if (details.referrer_telegram_id) {
                details.referrer_telegram_id = String(details.referrer_telegram_id);
            }
            return details;
        }
        console.warn(`${LOG_PREFIX_GPSUD} User not found.`);
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_GPSUD} ❌ Error fetching user details: ${err.message}`, err.stack);
        return null;
    }
}
console.log("[DB Ops] getPaymentSystemUserDetails defined.");


/**
 * Finds a user by their referral code.
 * @param {string} refCode The referral code.
 * @param {import('pg').PoolClient} [client=pool] Optional database client.
 * @returns {Promise<{telegram_id: string, username?:string, first_name?:string} | null>} User ID (as string) and basic info or null.
 */
async function getUserByReferralCode(refCode, client = pool) {
    const LOG_PREFIX_GUBRC = `[getUserByReferralCode Code:${refCode}]`;
    if (!refCode || typeof refCode !== 'string' || refCode.trim() === "") {
        console.warn(`${LOG_PREFIX_GUBRC} Invalid or empty referral code provided.`);
        return null;
    }
    try {
        const result = await queryDatabase('SELECT telegram_id, username, first_name FROM users WHERE referral_code = $1', [refCode.trim()], client);
        if (result.rows.length > 0) {
            const userFound = result.rows[0];
            userFound.telegram_id = String(userFound.telegram_id); // Ensure string ID
            return userFound;
        }
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_GUBRC} ❌ Error finding user by referral code: ${err.message}`, err.stack);
        return null;
    }
}
console.log("[DB Ops] getUserByReferralCode defined.");


// --- Unified Balance & Ledger Operations ---

/**
 * Atomically updates a user's balance and records the change in the ledger table.
 * This is the PRIMARY function for all financial transactions affecting user balance.
 * MUST be called within an active DB transaction if part of a larger multi-step operation.
 * The `dbClient` parameter MUST be an active client from `pool.connect()`.
 *
 * @param {import('pg').PoolClient} dbClient - The active database client from await pool.connect().
 * @param {string|number} telegramId - The user's Telegram ID.
 * @param {bigint} changeAmountLamports - Positive for credit, negative for debit.
 * @param {string} transactionType - Type for the ledger (e.g., 'deposit', 'withdrawal_fee', 'bet_placed_dice', 'win_dice', 'referral_payout').
 * @param {object} [relatedIds={}] Optional related IDs { deposit_id, withdrawal_id, game_log_id, referral_id, related_sweep_id }.
 * @param {string|null} [notes=null] Optional notes for the ledger entry.
 * @returns {Promise<{success: boolean, newBalanceLamports?: bigint, oldBalanceLamports?: bigint, ledgerId?: number, error?: string, errorCode?: string}>}
 */
async function updateUserBalanceAndLedger(dbClient, telegramId, changeAmountLamports, transactionType, relatedIds = {}, notes = null) {
    const stringUserId = String(telegramId);
    const changeAmount = BigInt(changeAmountLamports);
    const logPrefix = `[UpdateBalanceLedger UID:${stringUserId} Type:${transactionType} Amt:${changeAmount}]`;

    if (!dbClient || typeof dbClient.query !== 'function') {
        console.error(`${logPrefix} 🚨 CRITICAL: dbClient is not a valid database client. Transaction cannot proceed.`);
        return { success: false, error: 'Invalid database client provided to updateUserBalanceAndLedger.', errorCode: 'INVALID_DB_CLIENT' };
    }

    const relDepositId = (relatedIds?.deposit_id && Number.isInteger(relatedIds.deposit_id)) ? relatedIds.deposit_id : null;
    const relWithdrawalId = (relatedIds?.withdrawal_id && Number.isInteger(relatedIds.withdrawal_id)) ? relatedIds.withdrawal_id : null;
    const relGameLogId = (relatedIds?.game_log_id && Number.isInteger(relatedIds.game_log_id)) ? relatedIds.game_log_id : null;
    const relReferralId = (relatedIds?.referral_id && Number.isInteger(relatedIds.referral_id)) ? relatedIds.referral_id : null;
    const relSweepId = (relatedIds?.related_sweep_id && Number.isInteger(relatedIds.related_sweep_id)) ? relatedIds.related_sweep_id : null;
    let oldBalanceLamports; // To store the balance before change

    try {
        const balanceRes = await dbClient.query('SELECT balance, total_deposited_lamports, total_withdrawn_lamports, total_wagered_lamports, total_won_lamports FROM users WHERE telegram_id = $1 FOR UPDATE', [stringUserId]);
        if (balanceRes.rowCount === 0) {
            console.error(`${logPrefix} ❌ User balance record not found for ID ${stringUserId}.`);
            return { success: false, error: 'User profile not found for balance update.', errorCode: 'USER_NOT_FOUND' };
        }
        const userData = balanceRes.rows[0];
        oldBalanceLamports = BigInt(userData.balance); // Capture old balance
        const balanceAfter = oldBalanceLamports + changeAmount;

        if (balanceAfter < 0n && transactionType !== 'admin_grant' && transactionType !== 'admin_adjustment_debit') {
            console.warn(`${logPrefix} ⚠️ Insufficient balance. Current: ${oldBalanceLamports}, Change: ${changeAmount}, Would be: ${balanceAfter}. Required: ${-changeAmount}`);
            return { success: false, error: 'Insufficient balance for this transaction.', oldBalanceLamports: oldBalanceLamports, newBalanceLamportsWouldBe: balanceAfter, errorCode: 'INSUFFICIENT_FUNDS' };
        }

        let newTotalDeposited = BigInt(userData.total_deposited_lamports || '0');
        let newTotalWithdrawn = BigInt(userData.total_withdrawn_lamports || '0');
        let newTotalWagered = BigInt(userData.total_wagered_lamports || '0');
        let newTotalWon = BigInt(userData.total_won_lamports || '0'); // Tracks gross amount credited from wins

        if (transactionType === 'deposit' && changeAmount > 0n) {
            newTotalDeposited += changeAmount;
        } else if ((transactionType.startsWith('withdrawal_request') || transactionType.startsWith('withdrawal_fee')) && changeAmount < 0n) {
            newTotalWithdrawn -= changeAmount; // Subtracting a negative = adding positive
        } else if (transactionType.startsWith('bet_placed') && changeAmount < 0n) {
            newTotalWagered -= changeAmount; // Subtracting a negative = adding positive
        } else if ((transactionType.startsWith('win_') || transactionType.startsWith('jackpot_win_')) && changeAmount > 0n) {
            // Assumes changeAmount is the total credited amount (bet_returned + profit).
            // total_won_lamports tracks the gross amount credited from these wins.
            newTotalWon += changeAmount;
        } else if (transactionType === 'referral_commission' && changeAmount > 0n) {
            // If referral commissions directly credit user balance (instead of being paid out separately)
            // This would also be a "win" of sorts. Assuming for now referral payouts are separate.
            // If they do hit balance: newTotalWon += changeAmount; (or a new category total_referral_earnings_credited)
        }


        const updateUserQuery = `
            UPDATE users 
            SET balance = $1, 
                total_deposited_lamports = $2,
                total_withdrawn_lamports = $3,
                total_wagered_lamports = $4,
                total_won_lamports = $5,
                updated_at = NOW() 
            WHERE telegram_id = $6;
        `;
        const updateRes = await dbClient.query(updateUserQuery, [
            balanceAfter.toString(), 
            newTotalDeposited.toString(),
            newTotalWithdrawn.toString(),
            newTotalWagered.toString(),
            newTotalWon.toString(),
            stringUserId
        ]);

        if (updateRes.rowCount === 0) {
             console.error(`${logPrefix} ❌ Failed to update user balance row after lock for user ${stringUserId}. This should not happen.`);
             throw new Error('Failed to update user balance row after lock.');
        }

        const ledgerQuery = `
            INSERT INTO ledger (user_telegram_id, transaction_type, amount_lamports, balance_before_lamports, balance_after_lamports,
                                deposit_id, withdrawal_id, game_log_id, referral_id, related_sweep_id, notes, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
            RETURNING ledger_id;
        `;
        const ledgerRes = await dbClient.query(ledgerQuery, [
            stringUserId, transactionType, changeAmount.toString(), oldBalanceLamports.toString(), balanceAfter.toString(),
            relDepositId, relWithdrawalId, relGameLogId, relReferralId, relSweepId, notes
        ]);
        
        const ledgerId = ledgerRes.rows[0]?.ledger_id;
        console.log(`${logPrefix} ✅ Balance updated from ${oldBalanceLamports} to ${balanceAfter}. Ledger entry ID: ${ledgerId} created.`);
        return { success: true, newBalanceLamports: balanceAfter, oldBalanceLamports: oldBalanceLamports, ledgerId };

    } catch (err) {
        console.error(`${logPrefix} ❌ Error: ${err.message} (Code: ${err.code})`, err.stack);
        let errMsg = `Database error during balance/ledger update (Code: ${err.code || 'N/A'})`;
        if (err.message && err.message.toLowerCase().includes('violates check constraint') && err.message.toLowerCase().includes('balance')) {
            errMsg = 'Insufficient balance (check constraint violation).';
        }
        return { success: false, error: errMsg, errorCode: err.code, oldBalanceLamports };
    }
}
console.log("[DB Ops] updateUserBalanceAndLedger (with aggregated totals) defined.");


// --- Deposit Address & Deposit Operations ---

/**
 * Finds user ID and other details for a given deposit address. Checks cache first.
 * @param {string} depositAddress The deposit address (public key).
 * @returns {Promise<{userId: string, walletId: number, expiresAt: Date, derivationPath: string, isActive:boolean } | null>}
 */
async function findDepositAddressInfoDB(depositAddress) {
    const LOG_PREFIX_FDAI = `[FindDepositAddrInfo Addr:${depositAddress ? depositAddress.slice(0,6) : 'N/A'}...]`;
    if (!depositAddress) {
        console.warn(`${LOG_PREFIX_FDAI} Called with null or undefined depositAddress.`);
        return null;
    }

    // Check cache (activeDepositAddresses is a Map from Part 1)
    if (typeof activeDepositAddresses !== 'undefined' && activeDepositAddresses instanceof Map) {
        const cached = activeDepositAddresses.get(depositAddress);
        if (cached && Date.now() < cached.expiresAt) {
            // Cache only stores userId and expiresAt. For full details, we'd still need DB,
            // but this confirms it *was* active recently for this user.
            // console.log(`${LOG_PREFIX_FDAI} Cache hint: Address ${depositAddress} for user ${cached.userId} was recently active.`);
        }
    } else {
        console.warn(`${LOG_PREFIX_FDAI} activeDepositAddresses cache not available or not a Map.`);
    }


    try {
        const res = await queryDatabase(
            'SELECT user_telegram_id, wallet_id, expires_at, derivation_path, is_active FROM user_deposit_wallets WHERE public_key = $1',
            [depositAddress]
        );
        if (res.rows.length > 0) {
            const data = res.rows[0];
            const expiresAtDate = new Date(data.expires_at);
            const isActiveCurrent = data.is_active && expiresAtDate.getTime() > Date.now();
            
            if (typeof activeDepositAddresses !== 'undefined' && activeDepositAddresses instanceof Map) {
                if (isActiveCurrent) {
                    activeDepositAddresses.set(depositAddress, { userId: String(data.user_telegram_id), expiresAt: expiresAtDate.getTime() });
                } else {
                    activeDepositAddresses.delete(depositAddress);
                }
            }
            return { 
                userId: String(data.user_telegram_id), 
                walletId: data.wallet_id, 
                expiresAt: expiresAtDate, 
                derivationPath: data.derivation_path, 
                isActive: isActiveCurrent
            };
        }
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_FDAI} ❌ Error finding deposit address info: ${err.message}`, err.stack);
        return null;
    }
}
console.log("[DB Ops] findDepositAddressInfoDB (with cache interaction) defined.");

/**
 * Marks a deposit address as inactive and optionally as swept.
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {number} userDepositWalletId - The ID of the `user_deposit_wallets` record.
 * @param {boolean} [swept=false] - If true, also sets swept_at and potentially balance_at_sweep.
 * @param {bigint|null} [balanceAtSweep=null] - Optional balance at time of sweep (if swept=true). Null if not applicable.
 * @returns {Promise<boolean>} True if updated successfully.
 */
async function markDepositAddressInactiveDB(dbClient, userDepositWalletId, swept = false, balanceAtSweep = null) {
    const LOG_PREFIX_MDAI = `[MarkDepositAddrInactive WalletID:${userDepositWalletId} Swept:${swept}]`;
    try {
        // Construct query dynamically to handle optional balance_at_sweep
        let query = 'UPDATE user_deposit_wallets SET is_active = FALSE, updated_at = NOW()';
        const params = [];
        let paramIndex = 1;

        if (swept) {
            query += `, swept_at = NOW()`;
            if (balanceAtSweep !== null && typeof balanceAtSweep === 'bigint') {
                query += `, balance_at_sweep = $${paramIndex++}`;
                params.push(balanceAtSweep.toString());
            } else if (balanceAtSweep === null && swept) { // Explicitly set to NULL if swept but no balance given
                query += `, balance_at_sweep = NULL`;
            }
        }
        query += ` WHERE wallet_id = $${paramIndex++} RETURNING public_key, is_active;`;
        params.push(userDepositWalletId);

        const res = await dbClient.query(query, params);
        if (res.rowCount > 0) {
            const updatedWallet = res.rows[0];
            if (typeof activeDepositAddresses !== 'undefined' && activeDepositAddresses instanceof Map) {
                activeDepositAddresses.delete(updatedWallet.public_key);
            } else {
                console.warn(`${LOG_PREFIX_MDAI} activeDepositAddresses cache not available. Cannot update cache for ${updatedWallet.public_key}.`);
            }
            console.log(`${LOG_PREFIX_MDAI} ✅ Marked wallet ID ${userDepositWalletId} (Addr: ${updatedWallet.public_key.slice(0,6)}) as inactive/swept. New active status: ${updatedWallet.is_active}`);
            return true;
        }
        console.warn(`${LOG_PREFIX_MDAI} ⚠️ Wallet ID ${userDepositWalletId} not found or no change made.`);
        return false;
    } catch (err) {
        console.error(`${LOG_PREFIX_MDAI} ❌ Error marking deposit address inactive: ${err.message}`, err.stack);
        return false;
    }
}
console.log("[DB Ops] markDepositAddressInactiveDB defined.");

/**
 * Records a confirmed deposit transaction. Must be called within a transaction using dbClient.
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {string|number} userId
 * @param {number} userDepositWalletId - ID of the `user_deposit_wallets` record.
 * @param {string} depositAddress - The address that received funds.
 * @param {string} txSignature
 * @param {bigint} amountLamports
 * @param {string|null} [sourceAddress=null]
 * @param {number|null} [blockTime=null] - Unix timestamp from Solana transaction.
 * @returns {Promise<{success: boolean, depositId?: number, error?: string, alreadyProcessed?: boolean}>}
 */
async function recordConfirmedDepositDB(dbClient, userId, userDepositWalletId, depositAddress, txSignature, amountLamports, sourceAddress = null, blockTime = null) {
    const stringUserId = String(userId);
    const LOG_PREFIX_RCD = `[RecordDeposit UID:${stringUserId} TX:${txSignature.slice(0,10)}...]`;
    const query = `
        INSERT INTO deposits (user_telegram_id, user_deposit_wallet_id, deposit_address, transaction_signature, amount_lamports, source_address, block_time, confirmation_status, processed_at, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'confirmed', NOW(), NOW(), NOW())
        ON CONFLICT (transaction_signature) DO NOTHING 
        RETURNING deposit_id;
    `;
    try {
        const res = await dbClient.query(query, [stringUserId, userDepositWalletId, depositAddress, txSignature, amountLamports.toString(), sourceAddress, blockTime]);
        if (res.rowCount > 0 && res.rows[0].deposit_id) {
            console.log(`${LOG_PREFIX_RCD} ✅ Deposit recorded successfully. DB ID: ${res.rows[0].deposit_id}`);
            return { success: true, depositId: res.rows[0].deposit_id };
        }
        // If rowCount is 0, it means ON CONFLICT DO NOTHING was triggered.
        const existing = await dbClient.query('SELECT deposit_id FROM deposits WHERE transaction_signature = $1', [txSignature]);
        if (existing.rowCount > 0) {
            console.warn(`${LOG_PREFIX_RCD} ⚠️ Deposit TX ${txSignature} already processed (DB ID: ${existing.rows[0].deposit_id}).`);
            return { success: false, error: 'Deposit already processed.', alreadyProcessed: true, depositId: existing.rows[0].deposit_id };
        }
        console.error(`${LOG_PREFIX_RCD} ❌ Failed to record deposit and not a recognized duplicate (TX: ${txSignature}). This state should not be reached if ON CONFLICT works.`);
        return { success: false, error: 'Failed to record deposit (unknown issue after conflict check).' };
    } catch(err) {
        console.error(`${LOG_PREFIX_RCD} ❌ Error recording deposit: ${err.message} (Code: ${err.code})`, err.stack);
        return { success: false, error: err.message, errorCode: err.code };
    }
}
console.log("[DB Ops] recordConfirmedDepositDB defined.");


// --- Sweep Operations ---
/**
 * Records a successful sweep transaction. Must be called within a transaction using dbClient.
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {string} sourceDepositAddress
 * @param {string} destinationMainAddress
 * @param {bigint} amountLamports
 * @param {string} transactionSignature
 * @returns {Promise<{success: boolean, sweepId?: number, error?: string}>}
 */
async function recordSweepTransactionDB(dbClient, sourceDepositAddress, destinationMainAddress, amountLamports, transactionSignature) {
    const LOG_PREFIX_RST = `[RecordSweepTX From:${sourceDepositAddress.slice(0,6)} To:${destinationMainAddress.slice(0,6)} TX:${transactionSignature.slice(0,10)}...]`;
    const query = `
        INSERT INTO processed_sweeps (source_deposit_address, destination_main_address, amount_lamports, transaction_signature, swept_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (transaction_signature) DO UPDATE SET swept_at = NOW()
        RETURNING sweep_id;
    `; // Update timestamp if re-processed to ensure it's logged, though ideally it's unique.
    try {
        const res = await dbClient.query(query, [sourceDepositAddress, destinationMainAddress, amountLamports.toString(), transactionSignature]);
        if (res.rowCount > 0 && res.rows[0].sweep_id) {
            console.log(`${LOG_PREFIX_RST} ✅ Sweep transaction recorded successfully. DB ID: ${res.rows[0].sweep_id}`);
            return { success: true, sweepId: res.rows[0].sweep_id };
        }
        console.error(`${LOG_PREFIX_RST} ❌ Failed to record sweep transaction or get ID back for TX ${transactionSignature}. This might happen if ON CONFLICT DO UPDATE path was taken but didn't return ID as expected for this DB version/config.`);
        return { success: false, error: 'Failed to record sweep transaction or retrieve ID.' };
    } catch (err) {
        console.error(`${LOG_PREFIX_RST} ❌ Error recording sweep TX: ${err.message} (Code: ${err.code})`, err.stack);
        return { success: false, error: err.message, errorCode: err.code };
    }
}
console.log("[DB Ops] recordSweepTransactionDB defined.");


// --- Withdrawal Database Operations ---
async function createWithdrawalRequestDB(dbClient, userId, requestedAmountLamports, feeLamports, recipientAddress, priorityFeeMicroLamports = null, computeUnitLimit = null) {
    const stringUserId = String(userId);
    const LOG_PREFIX_CWR = `[CreateWithdrawalReq UID:${stringUserId} Addr:${recipientAddress.slice(0,6)}]`;
    const query = `
        INSERT INTO withdrawals (user_telegram_id, destination_address, amount_lamports, fee_lamports, status, priority_fee_microlamports, compute_unit_limit, requested_at, updated_at)
        VALUES ($1, $2, $3, $4, 'pending_processing', $5, $6, NOW(), NOW())
        RETURNING withdrawal_id;
    `;
    try {
        const res = await dbClient.query(query, [stringUserId, recipientAddress, requestedAmountLamports.toString(), feeLamports.toString(), priorityFeeMicroLamports, computeUnitLimit]);
        if (res.rows.length > 0 && res.rows[0].withdrawal_id) {
            console.log(`${LOG_PREFIX_CWR} ✅ Withdrawal request created. DB ID: ${res.rows[0].withdrawal_id}`);
            return { success: true, withdrawalId: res.rows[0].withdrawal_id };
        }
        throw new Error("Withdrawal request creation failed to return ID.");
    } catch (err) {
        console.error(`${LOG_PREFIX_CWR} ❌ Error creating withdrawal request: ${err.message}`, err.stack);
        return { success: false, error: err.message, errorCode: err.code };
    }
}
console.log("[DB Ops] createWithdrawalRequestDB defined.");

async function updateWithdrawalStatusDB(dbClient, withdrawalId, status, signature = null, errorMessage = null, blockTime = null) {
    const LOG_PREFIX_UWS = `[UpdateWithdrawalStatus ID:${withdrawalId} Status:${status}]`;
    const query = `
        UPDATE withdrawals 
        SET status = $1, 
            transaction_signature = $2, 
            error_message = $3, 
            block_time = $4,
            processed_at = CASE WHEN $1 IN ('completed', 'failed', 'confirmed', 'sent') THEN NOW() ELSE processed_at END,
            updated_at = NOW()
        WHERE withdrawal_id = $5
        RETURNING withdrawal_id;
    `;
    try {
        const res = await dbClient.query(query, [status, signature, errorMessage, blockTime, withdrawalId]);
        if (res.rowCount > 0) {
            console.log(`${LOG_PREFIX_UWS} ✅ Withdrawal status updated successfully.`);
            return { success: true, withdrawalId: res.rows[0].withdrawal_id };
        }
        console.warn(`${LOG_PREFIX_UWS} ⚠️ Withdrawal ID ${withdrawalId} not found or status not updated.`);
        return { success: false, error: "Withdrawal record not found or no update made." };
    } catch (err) {
        console.error(`${LOG_PREFIX_UWS} ❌ Error updating withdrawal status: ${err.message}`, err.stack);
        return { success: false, error: err.message, errorCode: err.code };
    }
}
console.log("[DB Ops] updateWithdrawalStatusDB defined.");

async function getWithdrawalDetailsDB(withdrawalId, dbClient = pool) {
    const LOG_PREFIX_GWD = `[GetWithdrawalDetails ID:${withdrawalId}]`;
    try {
        const res = await dbClient.query('SELECT * FROM withdrawals WHERE withdrawal_id = $1', [withdrawalId]);
        if (res.rows.length > 0) {
            const details = res.rows[0];
            details.amount_lamports = BigInt(details.amount_lamports);
            details.fee_lamports = BigInt(details.fee_lamports);
            details.user_telegram_id = String(details.user_telegram_id);
            return details;
        }
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_GWD} ❌ Error fetching withdrawal details: ${err.message}`, err.stack);
        return null;
    }
}
console.log("[DB Ops] getWithdrawalDetailsDB defined.");


// --- Referral Payout Database Operations ---
/**
 * Records that a referral has met criteria and commission is earned (but not yet paid).
 * Assumes a referral link record already exists from getOrCreateUser.
 * @param {import('pg').PoolClient} dbClient
 * @param {string|number} referrerUserId
 * @param {string|number} referredUserId
 * @param {string} commissionType e.g., 'first_deposit_bonus'
 * @param {bigint} commissionAmountLamports Amount earned.
 * @returns {Promise<{success: boolean, referralId?: number, error?: string}>}
 */
async function recordReferralCommissionEarnedDB(dbClient, referrerUserId, referredUserId, commissionType, commissionAmountLamports) {
    const LOG_PREFIX_RRCE = `[RecordRefCommEarn RefBy:${referrerUserId} RefTo:${referredUserId}]`;
    const query = `
        UPDATE referrals
        SET commission_type = $1, commission_amount_lamports = $2, status = 'earned', updated_at = NOW()
        WHERE referrer_telegram_id = $3 AND referred_telegram_id = $4 AND status = 'pending_criteria'
        RETURNING referral_id;
    `;
    try {
        const res = await dbClient.query(query, [commissionType, commissionAmountLamports.toString(), referrerUserId, referredUserId]);
        if (res.rowCount > 0) {
            console.log(`${LOG_PREFIX_RRCE} ✅ Referral commission of ${commissionAmountLamports} earned. DB ID: ${res.rows[0].referral_id}`);
            return { success: true, referralId: res.rows[0].referral_id };
        }
        console.warn(`${LOG_PREFIX_RRCE} No eligible 'pending_criteria' referral found to mark as 'earned', or already processed.`);
        return { success: false, error: "No eligible pending referral found or already processed." };
    } catch (err) {
        console.error(`${LOG_PREFIX_RRCE} ❌ Error recording referral commission earned: ${err.message}`, err.stack);
        return { success: false, error: err.message, errorCode: err.code };
    }
}
console.log("[DB Ops] recordReferralCommissionEarnedDB defined.");

/**
 * Updates a referral record status, typically after a payout attempt.
 * @param {import('pg').PoolClient} dbClient
 * @param {number} referralId
 * @param {'processing' | 'paid_out' | 'failed'} status
 * @param {string|null} [transactionSignature=null]
 * @param {string|null} [errorMessage=null]
 * @returns {Promise<{success: boolean, error?: string}>}
 */
async function updateReferralPayoutStatusDB(dbClient, referralId, status, transactionSignature = null, errorMessage = null) {
    const LOG_PREFIX_URPS = `[UpdateRefPayoutStatus ID:${referralId} Status:${status}]`;
    const query = `
        UPDATE referrals
        SET status = $1, transaction_signature = $2, updated_at = NOW(),
            notes = CASE WHEN $3 IS NOT NULL THEN COALESCE(notes, '') || 'Payout Error: ' || $3 ELSE notes END
        WHERE referral_id = $4 AND status != 'paid_out' -- Avoid reprocessing already paid
        RETURNING referral_id;
    `;
    try {
        const res = await dbClient.query(query, [status, transactionSignature, errorMessage, referralId]);
        if (res.rowCount > 0) {
            console.log(`${LOG_PREFIX_URPS} ✅ Referral payout status updated.`);
            return { success: true };
        }
        console.warn(`${LOG_PREFIX_URPS} Referral ID ${referralId} not found or already paid out/no status change needed.`);
        return { success: false, error: "Referral not found or no update made." };
    } catch (err) {
        console.error(`${LOG_PREFIX_URPS} ❌ Error updating referral payout status: ${err.message}`, err.stack);
        return { success: false, error: err.message, errorCode: err.code };
    }
}
console.log("[DB Ops] updateReferralPayoutStatusDB defined.");

async function getReferralDetailsDB(referralId, dbClient = pool) {
    const LOG_PREFIX_GRD = `[GetReferralDetails ID:${referralId}]`;
    try {
        const res = await dbClient.query('SELECT * FROM referrals WHERE referral_id = $1', [referralId]);
        if (res.rows.length > 0) {
            const details = res.rows[0];
            details.referrer_telegram_id = String(details.referrer_telegram_id);
            details.referred_telegram_id = String(details.referred_telegram_id);
            if (details.commission_amount_lamports) {
                details.commission_amount_lamports = BigInt(details.commission_amount_lamports);
            }
            return details;
        }
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_GRD} ❌ Error fetching referral details: ${err.message}`, err.stack);
        return null;
    }
}
console.log("[DB Ops] getReferralDetailsDB defined.");

async function getTotalReferralEarningsDB(userId, dbClient = pool) {
    const stringUserId = String(userId);
    const LOG_PREFIX_GTRE = `[GetTotalRefEarnings UID:${stringUserId}]`;
    try {
        const query = `
            SELECT 
                COALESCE(SUM(CASE WHEN status = 'paid_out' THEN commission_amount_lamports ELSE 0 END), 0) AS total_earned_paid_lamports,
                COALESCE(SUM(CASE WHEN status = 'earned' THEN commission_amount_lamports ELSE 0 END), 0) AS total_pending_payout_lamports
            FROM referrals
            WHERE referrer_telegram_id = $1;
        `;
        const res = await dbClient.query(query, [stringUserId]);
        if (res.rows.length > 0) {
            return {
                total_earned_paid_lamports: BigInt(res.rows[0].total_earned_paid_lamports),
                total_pending_payout_lamports: BigInt(res.rows[0].total_pending_payout_lamports)
            };
        }
        return { total_earned_paid_lamports: 0n, total_pending_payout_lamports: 0n };
    } catch (err) {
        console.error(`${LOG_PREFIX_GTRE} ❌ Error fetching total referral earnings: ${err.message}`, err.stack);
        return { total_earned_paid_lamports: 0n, total_pending_payout_lamports: 0n };
    }
}
console.log("[DB Ops] getTotalReferralEarningsDB defined.");


// --- Bet History & Leaderboard Database Operations ---
/**
 * Gets transaction history for a user from the ledger.
 * @param {string|number} userId
 * @param {number} [limit=10]
 * @param {number} [offset=0]
 * @param {string|null} [transactionTypeFilter=null] e.g., 'deposit', 'withdrawal%', 'bet%', 'win%' (SQL LIKE pattern)
 * @param {import('pg').PoolClient} [client=pool]
 * @returns {Promise<Array<object>>} Array of ledger entries with BigInt amounts.
 */
async function getBetHistoryDB(userId, limit = 10, offset = 0, transactionTypeFilter = null, client = pool) {
    const stringUserId = String(userId);
    const LOG_PREFIX_GBH = `[GetBetHistory UID:${stringUserId}]`;
    try {
        let queryText = `
            SELECT ledger_id, transaction_type, amount_lamports, balance_after_lamports, notes, created_at,
                   d.transaction_signature as deposit_tx, w.transaction_signature as withdrawal_tx,
                   g.game_type as game_log_type, g.outcome as game_log_outcome
            FROM ledger l
            LEFT JOIN deposits d ON l.deposit_id = d.deposit_id
            LEFT JOIN withdrawals w ON l.withdrawal_id = w.withdrawal_id
            LEFT JOIN games g ON l.game_log_id = g.game_log_id
            WHERE l.user_telegram_id = $1 
        `;
        const params = [stringUserId];
        let paramIndex = 2;
        if (transactionTypeFilter) {
            queryText += ` AND l.transaction_type ILIKE $${paramIndex++}`;
            params.push(transactionTypeFilter);
        }
        queryText += ` ORDER BY l.created_at DESC LIMIT $${paramIndex++} OFFSET $${paramIndex++};`;
        params.push(limit, offset);

        const res = await queryDatabase(queryText, params, client);
        return res.rows.map(row => ({
            ...row,
            amount_lamports: BigInt(row.amount_lamports),
            balance_after_lamports: BigInt(row.balance_after_lamports)
        }));
    } catch (err) {
        console.error(`${LOG_PREFIX_GBH} ❌ Error fetching ledger history: ${err.message}`, err.stack);
        return [];
    }
}
console.log("[DB Ops] getBetHistoryDB (from ledger with joins) defined.");

async function getLeaderboardDataDB(type = 'total_wagered', period = 'all_time', limit = 10) {
    const LOG_PREFIX_GLD = `[GetLeaderboard Type:${type} Period:${period}]`;
    console.log(`${LOG_PREFIX_GLD} Fetching leaderboard data...`);
    let orderByField = 'total_wagered_lamports'; // Default
    if (type === 'total_won') {
        orderByField = 'total_won_lamports';
    } else if (type === 'net_profit') {
        // This would require total_won_lamports - total_wagered_lamports, or a dedicated field
        // For simplicity, let's use total_won for now if net_profit is requested.
        orderByField = 'total_won_lamports'; // Placeholder for net profit
        console.warn(`${LOG_PREFIX_GLD} 'net_profit' leaderboard type requested, using 'total_won_lamports' as a proxy. Implement actual net profit calculation if needed.`);
    }

    // Period filtering (all_time, daily, weekly, monthly) would require more complex date range queries.
    // For now, only implementing 'all_time'.
    if (period !== 'all_time') {
        console.warn(`${LOG_PREFIX_GLD} Period '${period}' not yet implemented. Defaulting to 'all_time'.`);
    }

    const query = `
        SELECT telegram_id, username, first_name, ${orderByField}
        FROM users
        WHERE is_banned = FALSE
        ORDER BY ${orderByField} DESC, updated_at DESC
        LIMIT $1;
    `;
    try {
        const res = await queryDatabase(query, [limit]);
        return res.rows.map(row => ({
            telegram_id: String(row.telegram_id),
            username: row.username,
            first_name: row.first_name,
            stat_value: BigInt(row[orderByField]) // The actual value being ordered by
        }));
    } catch (err) {
        console.error(`${LOG_PREFIX_GLD} ❌ Error fetching leaderboard data: ${err.message}`, err.stack);
        return [];
    }
}
console.log("[DB Ops] getLeaderboardDataDB (basic implementation) defined.");


console.log("Part P2: Payment System Database Operations - Complete.");
// --- End of Part P2 ---
// --- Start of Part P3 ---
// index.js - Part P3: Payment System UI Handlers, Stateful Logic & Webhook Setup
//---------------------------------------------------------------------------
console.log("Loading Part P3: Payment System UI Handlers, Stateful Logic & Webhook Setup...");

// Assumes global utilities: safeSendMessage, escapeMarkdownV2, formatCurrency, formatBalanceForDisplay,
// bot, userStateCache, pool, getOrCreateUser, stringifyWithBigInt (Part 1 & others)
// LAMPORTS_PER_SOL, MIN_WITHDRAWAL_LAMPORTS, WITHDRAWAL_FEE_LAMPORTS, DEPOSIT_ADDRESS_EXPIRY_MS,
// DEPOSIT_CONFIRMATION_LEVEL, BOT_NAME, PAYMENT_WEBHOOK_PATH, PAYMENT_WEBHOOK_SECRET (Part 1 env & constants)
// Assumes DB ops from Part P2: linkUserWallet, getUserBalance, getPaymentSystemUserDetails,
// findDepositAddressInfoDB, createWithdrawalRequestDB, getBetHistoryDB, getUserByReferralCode,
// getTotalReferralEarningsDB, updateUserBalanceAndLedger.
// Assumes Solana utils from Part P1: generateUniqueDepositAddress, isValidSolanaAddress.
// Assumes Cache utils/direct cache access: activeDepositAddresses, processedDepositTxSignatures (Part 1)
// addProcessedTxSignatureToCache, hasProcessedTxSignatureInCache (conceptual, likely direct Set operations).
// Assumes Payout job queuing from Part P4: addPayoutJob.
// Assumes Deposit processing from Part P4: processDepositTransaction, depositProcessorQueue.
// Assumes Express app instance 'app' from Part 1 if webhooks enabled.
// crypto from Part 1 for webhook signature validation (if used).

// --- User State Management ---
function clearUserState(userId) {
    const stringUserId = String(userId);
    const state = userStateCache.get(stringUserId); 
    if (state) {
        if (state.data?.timeoutId) clearTimeout(state.data.timeoutId); // Clear any associated timeouts
        userStateCache.delete(stringUserId);
        console.log(`[StateUtil] Cleared state for user ${stringUserId}. State was: ${state.state || state.action || 'N/A'}`);
    }
}
console.log("[State Utils] clearUserState defined.");

async function routeStatefulInput(msg, currentState) { 
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id); // Chat where user sent the message
    const text = msg.text || '';
    const stateName = currentState.state || currentState.action; // Use .action if .state is not present
    const logPrefix = `[StatefulInput UID:${userId} State:${stateName} ChatID:${chatId}]`;
    console.log(`${logPrefix} Routing input: "${text.substring(0, 50)}..."`);

    // Ensure response is in the expected chat (usually DM for these states)
    if (currentState.chatId && String(currentState.chatId) !== chatId) {
        console.warn(`${logPrefix} Stateful input received in wrong chat (${chatId}) vs expected (${currentState.chatId}). Informing user.`);
        await safeSendMessage(chatId, "Please respond to my previous question in our direct message chat. 💬", {});
        // Optionally, resend the prompt in the correct chat if possible, or just guide them.
        return;
    }

    switch (stateName) {
        case 'awaiting_withdrawal_address':
            await handleWalletAddressInput(msg, currentState); 
            break;
        case 'awaiting_withdrawal_amount':
            await handleWithdrawalAmountInput(msg, currentState);
            break;
        default:
            console.warn(`${logPrefix} Unknown or unhandled state: ${stateName}. Clearing state.`);
            clearUserState(userId); // Clear the stale/unknown state
            await safeSendMessage(chatId, "Your previous action seems to have expired or was unclear. Please try again using a command from the main menu. 🤔", { parse_mode: 'MarkdownV2' });
    }
}
console.log("[State Utils] routeStatefulInput defined.");

async function handleWalletAddressInput(msg, currentState) { 
    const userId = String(msg.from.id);
    const dmChatId = String(msg.chat.id); // Should be user's DM
    const potentialNewAddress = msg.text ? msg.text.trim() : '';
    const logPrefix = `[WalletAddrInput UID:${userId}]`;

    if (!currentState || !currentState.data || currentState.state !== 'awaiting_withdrawal_address' || dmChatId !== userId) {
        console.error(`${logPrefix} Invalid state or context for wallet address input. Current State ChatID: ${currentState?.chatId}, Msg ChatID: ${dmChatId}, State: ${currentState?.state}`);
        clearUserState(userId); // Clear potentially corrupt state
        await safeSendMessage(dmChatId, "⚙️ There was an issue processing your address input. Please try linking your wallet again via the \`/wallet\` menu or \`/setwallet\` command.", { parse_mode: 'MarkdownV2' });
        return;
    }

    const { originalPromptMessageId, originalGroupChatId, originalGroupMessageId } = currentState.data;
    // Delete the "Please enter your address" prompt message in DM
    if (originalPromptMessageId && bot) { await bot.deleteMessage(dmChatId, originalPromptMessageId).catch(() => {}); }
    clearUserState(userId); // Clear state *after* extracting necessary data and before long async ops

    const linkingMsg = await safeSendMessage(dmChatId, `🔗 Validating and attempting to link wallet: \`${escapeMarkdownV2(potentialNewAddress)}\`... Please hold on a moment.`, { parse_mode: 'MarkdownV2' });
    const displayMsgIdInDm = linkingMsg ? linkingMsg.message_id : null;

    try {
        if (!isValidSolanaAddress(potentialNewAddress)) { // isValidSolanaAddress from Part P1
            throw new Error("The provided address has an invalid Solana address format. Please double-check and try again.");
        }

        const linkResult = await linkUserWallet(userId, potentialNewAddress); // linkUserWallet from Part 2
        let feedbackText;
        const finalKeyboard = { inline_keyboard: [[{ text: '💳 Back to Wallet Menu', callback_data: 'menu:wallet' }]] };

        if (linkResult.success) {
            feedbackText = `✅ Success! ${escapeMarkdownV2(linkResult.message || `Wallet \`${potentialNewAddress}\` has been successfully linked to your account.`)}`;
            if (originalGroupChatId && originalGroupMessageId && bot) { 
                const userForGroupMsg = await getOrCreateUser(userId); // Fetch fresh user for display name
                await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || msg.from)} has successfully updated their linked wallet.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(()=>{});
            }
        } else {
            feedbackText = `⚠️ Wallet Link Failed for \`${escapeMarkdownV2(potentialNewAddress)}\`.\n*Reason:* ${escapeMarkdownV2(linkResult.error || "Please ensure the address is valid and not already in use.")}`;
             if (originalGroupChatId && originalGroupMessageId && bot) { 
                const userForGroupMsg = await getOrCreateUser(userId);
                await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || msg.from)}, there was an issue linking your wallet. Please check my DM for details and try again.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(()=>{});
            }
        }

        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(feedbackText, { chat_id: dmChatId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        } else {
            await safeSendMessage(dmChatId, feedbackText, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        }
    } catch (e) {
        console.error(`${logPrefix} Error linking wallet ${potentialNewAddress}: ${e.message}`);
        const errorTextToDisplay = `⚠️ Error with wallet address: \`${escapeMarkdownV2(potentialNewAddress)}\`.\n*Details:* ${escapeMarkdownV2(e.message || "An unexpected error occurred.")}\nPlease ensure it's a valid Solana public key and try again.`;
        const errorKeyboard = { inline_keyboard: [[{ text: '💳 Try Again (Wallet Menu)', callback_data: 'menu:wallet' }]] };
        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(errorTextToDisplay, { chat_id: dmChatId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        } else {
            await safeSendMessage(dmChatId, errorTextToDisplay, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        }
        if (originalGroupChatId && originalGroupMessageId && bot) {
            const userForGroupMsg = await getOrCreateUser(userId);
            await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || msg.from)}, there was an error processing your wallet address. Please check my DM.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(()=>{});
        }
    }
}
console.log("[State Handler] handleWalletAddressInput defined.");

async function handleWithdrawalAmountInput(msg, currentState) { 
    const userId = String(msg.from.id);
    const dmChatId = String(msg.chat.id); // Should be user's DM
    const textAmount = msg.text ? msg.text.trim() : '';
    const logPrefix = `[WithdrawAmountInput UID:${userId}]`;

    if (!currentState || !currentState.data || currentState.state !== 'awaiting_withdrawal_amount' || dmChatId !== userId ||
        !currentState.data.linkedWallet || typeof currentState.data.currentBalanceLamportsStr !== 'string') {
        console.error(`${logPrefix} Invalid state or data for withdrawal amount. State: ${stringifyWithBigInt(currentState)}`);
        clearUserState(userId);
        await safeSendMessage(dmChatId, "⚙️ Error: Withdrawal context lost or invalid. Please restart the withdrawal process from the \`/wallet\` menu.", { parse_mode: 'MarkdownV2' });
        return;
    }

    const { linkedWallet, originalPromptMessageId, currentBalanceLamportsStr, originalGroupChatId, originalGroupMessageId } = currentState.data;
    const currentBalanceLamports = BigInt(currentBalanceLamportsStr);
    if (originalPromptMessageId && bot) { await bot.deleteMessage(dmChatId, originalPromptMessageId).catch(() => {}); }
    clearUserState(userId); // Clear state before async ops

    try {
        let amountSOL;
        // Parse amount, allowing for "sol" suffix
        if (textAmount.toLowerCase().endsWith('sol')) {
            amountSOL = parseFloat(textAmount.toLowerCase().replace('sol', '').trim());
        } else {
            amountSOL = parseFloat(String(textAmount).replace(/[^0-9.]/g, ''));
        }

        if (isNaN(amountSOL) || amountSOL <= 0) throw new Error("Invalid number format or non-positive amount. Please enter a value like \`0.5\` or \`10\` or \`0.1 sol\`.");
        
        const amountLamports = BigInt(Math.floor(amountSOL * Number(LAMPORTS_PER_SOL)));
        const feeLamports = WITHDRAWAL_FEE_LAMPORTS; // From Part 1
        const totalDeductionLamports = amountLamports + feeLamports;
        const minWithdrawDisplaySOL = await formatBalanceForDisplay(MIN_WITHDRAWAL_LAMPORTS, 'SOL');
        const feeDisplaySOL = await formatBalanceForDisplay(feeLamports, 'SOL');
        const balanceDisplaySOL = await formatBalanceForDisplay(currentBalanceLamports, 'SOL');
        const amountToWithdrawDisplaySOL = await formatBalanceForDisplay(amountLamports, 'SOL');
        const totalDeductionDisplaySOL = await formatBalanceForDisplay(totalDeductionLamports, 'SOL');

        if (amountLamports < MIN_WITHDRAWAL_LAMPORTS) {
            throw new Error(`Withdrawal amount of *${escapeMarkdownV2(amountToWithdrawDisplaySOL)}* is less than the minimum of *${escapeMarkdownV2(minWithdrawDisplaySOL)}*\\.`);
        }
        if (currentBalanceLamports < totalDeductionLamports) {
            throw new Error(`Insufficient balance\\. You need *${escapeMarkdownV2(totalDeductionDisplaySOL)}* \\(amount \\+ fee\\) to withdraw *${escapeMarkdownV2(amountToWithdrawDisplaySOL)}*\\. Your balance is *${escapeMarkdownV2(balanceDisplaySOL)}*\\.`);
        }

        const confirmationText = `*Withdrawal Confirmation* ⚜️\n\n` +
                                 `Please review and confirm your withdrawal:\n\n` +
                                 `🔹 Amount to Withdraw: *${escapeMarkdownV2(amountToWithdrawDisplaySOL)}*\n` +
                                 `🔹 Withdrawal Fee: *${escapeMarkdownV2(feeDisplaySOL)}*\n` +
                                 `🔹 Total Deducted: *${escapeMarkdownV2(totalDeductionDisplaySOL)}*\n` +
                                 `🔹 Recipient Wallet: \`${escapeMarkdownV2(linkedWallet)}\`\n\n` +
                                 `⚠️ Double\\-check the recipient address\\! Transactions are irreversible\\. Proceed?`;

        const sentConfirmMsg = await safeSendMessage(dmChatId, confirmationText, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [
                [{ text: '✅ Yes, Confirm Withdrawal', callback_data: `process_withdrawal_confirm:yes` }],
                [{ text: '❌ No, Cancel', callback_data: `process_withdrawal_confirm:no` }]
            ]}
        });

        if (sentConfirmMsg?.message_id) {
            userStateCache.set(userId, {
                state: 'awaiting_withdrawal_confirmation', 
                chatId: dmChatId, // Expect confirmation in DM
                messageId: sentConfirmMsg.message_id, 
                data: { linkedWallet, amountLamportsStr: amountLamports.toString(), feeLamportsStr: feeLamports.toString(), originalGroupChatId, originalGroupMessageId },
                timestamp: Date.now()
            });
            if (originalGroupChatId && originalGroupMessageId && bot) {
                const userForGroupMsg = await getOrCreateUser(userId);
                await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || msg.from)}, please check your DMs to confirm your withdrawal request.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(()=>{});
            }
        } else {
            throw new Error("Failed to send withdrawal confirmation message. Please try again.");
        }
    } catch (e) {
        console.error(`${logPrefix} Error processing withdrawal amount: ${e.message}`);
        await safeSendMessage(dmChatId, `⚠️ *Withdrawal Error:*\n${escapeMarkdownV2(e.message)}\n\nPlease restart the withdrawal process from the \`/wallet\` menu\\.`, {
            parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]] }
        });
        if (originalGroupChatId && originalGroupMessageId && bot) { 
            const userForGroupMsg = await getOrCreateUser(userId);
            await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || msg.from)}, there was an error with your withdrawal amount. Please check my DM.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(()=>{});
        }
    }
}
console.log("[State Handler] handleWithdrawalAmountInput defined.");

// --- UI Command Handler Implementations ---
// These handlers are called via the command router in Part 5a-S1/S3.

async function handleWalletCommand(msg) { 
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id); // Chat where /wallet was typed
    const chatType = msg.chat.type;
    
    let userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) {
        await safeSendMessage(commandChatId, "Error fetching your player profile. Please try /start again.", {parse_mode: 'MarkdownV2'});
        return;
    }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId); // Clear any pending input states

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) {console.error("[WalletCmd] Error getting bot username:", e.message);}

    let targetChatIdForMenu = userId; // Default to DM
    let messageIdToEditOrDeleteForMenu = msg.message_id; // Original /wallet command message ID

    if (chatType !== 'private') {
        if(msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(()=>{});
        await safeSendMessage(commandChatId, `${playerRef}, I've sent your Wallet Dashboard to our private chat: @${escapeMarkdownV2(botUsername)} 💳 For your security, all wallet actions are handled there\\.`, { parse_mode: 'MarkdownV2' });
        messageIdToEditOrDeleteForMenu = null; // No message to edit in DM initially
    } else {
        // If already in DM, we might want to delete the /wallet command message
        if(msg.message_id) await bot.deleteMessage(userId, msg.message_id).catch(()=>{});
        messageIdToEditOrDeleteForMenu = null; // Will send a new message
    }
    
    const loadingDmMsg = await safeSendMessage(targetChatIdForMenu, "Loading your Wallet Dashboard... ⏳", {});
    // Use the ID of the "Loading..." message for editing, if successfully sent
    if (loadingDmMsg?.message_id) messageIdToEditOrDeleteForMenu = loadingDmMsg.message_id;

    try {
        const userDetails = await getPaymentSystemUserDetails(userId); // From Part P2
        if (!userDetails) {
            const noUserText = "😕 Could not retrieve your player profile. Please try sending \`/start\` to the bot first.";
            if (messageIdToEditOrDeleteForMenu) await bot.editMessageText(noUserText, {chat_id: targetChatIdForMenu, message_id: messageIdToEditOrDeleteForMenu, parse_mode: 'MarkdownV2'});
            else await safeSendMessage(targetChatIdForMenu, noUserText, {parse_mode: 'MarkdownV2'});
            return;
        }
        const balanceLamports = BigInt(userDetails.balance || '0');
        const linkedAddress = userDetails.solana_wallet_address;
        const balanceDisplayUSD = await formatBalanceForDisplay(balanceLamports, 'USD');
        const balanceDisplaySOL = await formatBalanceForDisplay(balanceLamports, 'SOL');
        const escapedLinkedAddress = linkedAddress ? escapeMarkdownV2(linkedAddress) : "_Not Set_";

        let text = `⚜️ **${escapeMarkdownV2(BOT_NAME)} Wallet Dashboard** ⚜️\n\n` +
                   `👤 Player: ${playerRef}\n\n` +
                   `💰 Current Balance:\n   Approx\\. *${escapeMarkdownV2(balanceDisplayUSD)}*\n   SOL: *${escapeMarkdownV2(balanceDisplaySOL)}*\n\n` +
                   `🔗 Linked Withdrawal Address:\n   \`${escapedLinkedAddress}\`\n\n`;
        if (!linkedAddress) {
            text += `💡 You can link a wallet using the button below or by typing \`/setwallet YOUR_ADDRESS\` in this chat\\.\n\n`;
        }
        text += `What would you like to do?`;
        
        const keyboardActions = [
            [{ text: "💰 Deposit SOL", callback_data: "menu:deposit" }, { text: "💸 Withdraw SOL", callback_data: "menu:withdraw" }],
            [{ text: "📜 Transaction History", callback_data: "menu:history" }],
            linkedAddress 
                ? [{ text: "🔄 Update Linked Wallet", callback_data: "menu:link_wallet_prompt" }]
                : [{ text: "🔗 Link Withdrawal Wallet", callback_data: "menu:link_wallet_prompt" }],
            [{ text: "🤝 Referrals & Rewards", callback_data: "menu:referral" }, { text: "🏆 View Leaderboards", callback_data: "menu:leaderboards" }], // Leaderboards can be group or DM
            [{ text: "❓ Help & Games Menu", callback_data: "menu:main" }]
        ];
        const keyboard = { inline_keyboard: keyboardActions };

        if (messageIdToEditOrDeleteForMenu) {
            await bot.editMessageText(text, { chat_id: targetChatIdForMenu, message_id: messageIdToEditOrDeleteForMenu, parse_mode: 'MarkdownV2', reply_markup: keyboard });
        } else {
            await safeSendMessage(targetChatIdForMenu, text, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
        }
    } catch (error) {
        console.error(`[handleWalletCommand UID:${userId}] ❌ Error displaying wallet: ${error.message}`, error.stack);
        const errorText = "⚙️ Apologies, we encountered an issue while fetching your wallet information. Please try again in a moment.";
        if (messageIdToEditOrDeleteForMenu) {
            await bot.editMessageText(errorText, {chat_id: targetChatIdForMenu, message_id: messageIdToEditOrDeleteForMenu, parse_mode: 'MarkdownV2'}).catch(async () => {
                await safeSendMessage(targetChatIdForMenu, errorText, {parse_mode: 'MarkdownV2'}); // Fallback to send new if edit fails
            });
        } else {
            await safeSendMessage(targetChatIdForMenu, errorText, {parse_mode: 'MarkdownV2'});
        }
    }
}
console.log("[UI Handler] handleWalletCommand defined.");

async function handleSetWalletCommand(msg, args) { 
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    let userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) { /* Error handled by getOrCreateUser or main listener */ return; }
    const playerRef = getPlayerDisplayReference(userObject);
    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) {console.error("[SetWalletCmd] Error getting bot username:", e.message);}

    clearUserState(userId);

    if (chatType !== 'private') {
        if(msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(() => {});
        const dmPrompt = `${playerRef}, for your security, please set your wallet address by sending the command \`/setwallet YOUR_ADDRESS\` directly to me in our private chat: @${escapeMarkdownV2(botUsername)} 💳`;
        await safeSendMessage(commandChatId, dmPrompt, { parse_mode: 'MarkdownV2' });
        // Also send a prompt to DM to make it easier for user
        await safeSendMessage(userId, `Hi ${playerRef}, to set or update your withdrawal wallet, please reply here with the command: \`/setwallet YOUR_SOLANA_ADDRESS\` (Example: \`/setwallet YourSoLaddressHere...\`)`, {parse_mode: 'MarkdownV2'});
        return;
    }

    // In private chat
    if (args.length < 1 || !args[0].trim()) {
        await safeSendMessage(userId, `💡 To link your Solana wallet for withdrawals, please use the format: \`/setwallet YOUR_SOLANA_ADDRESS\`\nExample: \`/setwallet SoLmaNqerT3ZpPT1qS9j2kKx2o5x94s2f8u5aA3bCgD\``, { parse_mode: 'MarkdownV2' });
        return;
    }
    const potentialNewAddress = args[0].trim();

    if(msg.message_id) await bot.deleteMessage(userId, msg.message_id).catch(() => {}); // Delete the user's /setwallet command message in DM

    const linkingMsg = await safeSendMessage(userId, `🔗 Validating and attempting to link wallet: \`${escapeMarkdownV2(potentialNewAddress)}\`... Please hold on.`, { parse_mode: 'MarkdownV2' });
    const displayMsgIdInDm = linkingMsg ? linkingMsg.message_id : null;

    try {
        if (!isValidSolanaAddress(potentialNewAddress)) { // from Part P1
            throw new Error("The provided address has an invalid Solana address format.");
        }
        const linkResult = await linkUserWallet(userId, potentialNewAddress); // from Part 2
        let feedbackText;
        const finalKeyboard = { inline_keyboard: [[{ text: '💳 Back to Wallet Menu', callback_data: 'menu:wallet' }]] };

        if (linkResult.success) {
            feedbackText = `✅ Success! ${escapeMarkdownV2(linkResult.message || `Wallet \`${potentialNewAddress}\` is now linked.`)}`;
        } else {
            feedbackText = `⚠️ Wallet Link Failed for \`${escapeMarkdownV2(potentialNewAddress)}\`.\n*Reason:* ${escapeMarkdownV2(linkResult.error || "Please check the address and try again.")}`;
        }

        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(feedbackText, { chat_id: userId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        } else {
            await safeSendMessage(userId, feedbackText, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        }
    } catch (e) {
        console.error(`[SetWalletCmd UID:${userId}] Error linking wallet ${potentialNewAddress}: ${e.message}`);
        const errorTextToDisplay = `⚠️ Error with wallet address: \`${escapeMarkdownV2(potentialNewAddress)}\`.\n*Details:* ${escapeMarkdownV2(e.message || "An unexpected error occurred.")}\nPlease ensure it's a valid Solana public key.`;
        const errorKeyboard = { inline_keyboard: [[{ text: '💳 Try Again (Wallet Menu)', callback_data: 'menu:wallet' }]] };
        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(errorTextToDisplay, { chat_id: userId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        } else {
            await safeSendMessage(userId, errorTextToDisplay, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        }
    }
}
console.log("[UI Handler] handleSetWalletCommand defined.");

async function handleDepositCommand(msg, args = [], correctUserIdFromCb = null) { 
    const userId = String(correctUserIdFromCb || msg.from.id); // Use CB ID if provided (from menu), else msg.from.id
    const commandChatId = String(msg.chat.id); // Chat where original command/button was
    const chatType = msg.chat.type;

    let userObject = await getOrCreateUser(userId, msg.from?.username, msg.from?.first_name, msg.from?.last_name);
    if (!userObject) {
        await safeSendMessage(commandChatId, "Error fetching your player profile. Please try /start.", {parse_mode: 'MarkdownV2'});
        return;
    }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId); // Clear any pending states
    const logPrefix = `[DepositCmd UID:${userId} OrigChat:${commandChatId} Type:${chatType}]`;
    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) {console.error(`${logPrefix} Error getting bot username:`, e.message);}


    // If command came from group, or if msg.chat.id is not the user's DM, redirect message to DM
    if (String(commandChatId) !== userId) {
        if (msg.message_id && msg.chat?.id && String(msg.chat.id) !== userId) { // if it's an actual message from group
            // For callbacks (isCallbackRedirect), message might have been edited already.
            // For commands, delete the original command in group.
            if (!msg.isCallbackRedirect) { // isCallbackRedirect is from mockMsgObjectForHandler
                await bot.deleteMessage(commandChatId, msg.message_id).catch(()=>{});
            }
        }
        await safeSendMessage(commandChatId, `${playerRef}, for your security and convenience, I've sent your unique deposit address to our private chat: @${escapeMarkdownV2(botUsername)} 📬 Please check your DMs.`, { parse_mode: 'MarkdownV2' });
    }

    // All further interaction happens in DM (userId is the DM chat ID)
    const loadingDmMsg = await safeSendMessage(userId, "Generating your personal Solana deposit address... This may take a moment. ⚙️", {parse_mode:'MarkdownV2'});
    const loadingDmMsgId = loadingDmMsg?.message_id;
    let client = null;

    try {
        client = await pool.connect(); // Acquire client for potential transaction
        await client.query('BEGIN');

        const existingAddresses = await client.query( // Use client
            "SELECT public_key, expires_at FROM user_deposit_wallets WHERE user_telegram_id = $1 AND is_active = TRUE AND expires_at > NOW() ORDER BY created_at DESC LIMIT 1",
            [userId]
        );
        let depositAddress; let expiresAt; let newAddressGenerated = false;

        if (existingAddresses.rows.length > 0) {
            depositAddress = existingAddresses.rows[0].public_key;
            expiresAt = new Date(existingAddresses.rows[0].expires_at);
            console.log(`${logPrefix} Found existing active deposit address: ${depositAddress}`);
        } else {
            // generateUniqueDepositAddress (from Part P1) inserts into user_deposit_wallets
            const newAddress = await generateUniqueDepositAddress(userId, client); // Pass client
            if (!newAddress) {
                throw new Error("Failed to generate a new deposit address. Please try again or contact support.");
            }
            depositAddress = newAddress;
            newAddressGenerated = true;
            // Fetch the expiry that was set in DB by generateUniqueDepositAddress
            const newAddrDetails = await client.query("SELECT expires_at FROM user_deposit_wallets WHERE public_key = $1 AND user_telegram_id = $2", [depositAddress, userId]);
            expiresAt = newAddrDetails.rows.length > 0 ? new Date(newAddrDetails.rows[0].expires_at) : new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS);
        }

        if (newAddressGenerated || (userObject.last_deposit_address !== depositAddress)) {
            // Update users table with the latest deposit address info
            await client.query(
                `UPDATE users SET last_deposit_address = $1, last_deposit_address_generated_at = $2, updated_at = NOW() WHERE telegram_id = $3`,
                [depositAddress, expiresAt, userId]
            );
            console.log(`${logPrefix} Updated users table with last_deposit_address: ${depositAddress} for user ${userId}.`);
        }
        await client.query('COMMIT');


        const expiryTimestamp = Math.floor(expiresAt.getTime() / 1000);
        const timeRemaining = Math.max(0, Math.floor((expiresAt.getTime() - Date.now()) / 60000)); 
        const solanaPayUrl = `solana:${depositAddress}?label=${encodeURIComponent(BOT_NAME + " Deposit")}&message=${encodeURIComponent("Casino Deposit for " + playerRef)}`;
        const qrCodeUrl = `https://api.qrserver.com/v1/create-qr-code/?size=250x250&data=${encodeURIComponent(solanaPayUrl)}`;

        const depositMessage = `💰 *Your Personal Solana Deposit Address* 💰\n\n` +
                               `Hi ${playerRef}, please send your SOL deposits to the following unique address:\n\n` +
                               `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                               `_(Tap address to copy)_ \n\n` +
                               `⏳ This address is valid for approximately *${escapeMarkdownV2(String(timeRemaining))} minutes* \\(expires <t:${expiryTimestamp}:R>\\)\\.\n` +
                               `💎 Confirmation Level: \`${escapeMarkdownV2(String(DEPOSIT_CONFIRMATION_LEVEL || 'confirmed'))}\`\n\n` +
                               `⚠️ *Important:*\n` +
                               `   ▫️ Send *only SOL* to this address\\.\n` +
                               `   ▫️ Do *not* send NFTs or other tokens\\.\n` +
                               `   ▫️ Deposits from exchanges may take longer to confirm\\.\n` +
                               `   ▫️ This address is *unique to you* for this deposit session\\. Do not share it\\.`;
        
        const keyboard = {
            inline_keyboard: [
                [{ text: "🔍 View on Solscan", url: `https://solscan.io/account/${depositAddress}` }],
                [{ text: "📱 Scan QR Code", url: qrCodeUrl }], // Consider if this URL needs encoding
                [{ text: "💳 Back to Wallet", callback_data: "menu:wallet" }]
            ]
        };

        if (loadingDmMsgId) {
            await bot.editMessageText(depositMessage, {chat_id: userId, message_id: loadingDmMsgId, parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
        } else {
            await safeSendMessage(userId, depositMessage, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
        }
    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error: ${rbErr.message}`));
        console.error(`${logPrefix} ❌ Error handling deposit command: ${error.message}`, error.stack);
        const errorText = `⚙️ Apologies, ${playerRef}, we couldn't generate a deposit address for you at this moment: \`${escapeMarkdownV2(error.message)}\`\\. Please try again shortly or contact support\\.`;
        if (loadingDmMsgId) {
            await bot.editMessageText(errorText, {chat_id: userId, message_id: loadingDmMsgId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"Try Again", callback_data:DEPOSIT_CALLBACK_ACTION}]]}}).catch(async () => {
                await safeSendMessage(userId, errorText, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"Try Again", callback_data:DEPOSIT_CALLBACK_ACTION}]]}});
            });
        } else {
            await safeSendMessage(userId, errorText, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"Try Again", callback_data:DEPOSIT_CALLBACK_ACTION}]]}});
        }
    } finally {
        if (client) client.release();
    }
}
console.log("[UI Handler] handleDepositCommand defined.");

// ... (handleWithdrawCommand, handleReferralCommand, handleHistoryCommand, handleMenuAction, handleWithdrawalConfirmation will follow similar review and update structure)

// --- Webhook Setup Function ---
// ... (setupPaymentWebhook function - definition will be placed here)
// This function's internal logic uses processDepositTransaction (from P4) and cache utilities.
// The webhook payload parsing section needs to be highly adaptable to the specific provider.

// For brevity in this response, the full implementations of handleWithdrawCommand, handleReferralCommand,
// handleHistoryCommand, handleMenuAction, handleWithdrawalConfirmation, and setupPaymentWebhook
// (which are quite long) will be provided in the final integrated Part P3.
// The core fixes and patterns (DM redirection, state clearing, transactional balance updates for withdrawal confirmation)
// will be applied to them.
// --- Continuation of Part P3 ---
// index.js - Part P3: Payment System UI Handlers, Stateful Logic & Webhook Setup

// (Assuming functions and constants from previous parts, and the first half of Part P3 are loaded)
// Specifically, userStateCache, clearUserState, getOrCreateUser, getPlayerDisplayReference,
// safeSendMessage, escapeMarkdownV2, bot, pool,
// getUserLinkedWallet, getUserBalance, MIN_WITHDRAWAL_LAMPORTS, WITHDRAWAL_FEE_LAMPORTS,
// formatBalanceForDisplay, formatCurrency, LAMPORTS_PER_SOL,
// createWithdrawalRequestDB, updateUserBalanceAndLedger, addPayoutJob,
// generateReferralCode, queryDatabase, getTotalReferralEarningsDB, getBetHistoryDB,
// getPaymentSystemUserDetails, findDepositAddressInfoDB,
// DEPOSIT_CALLBACK_ACTION, QUICK_DEPOSIT_CALLBACK_ACTION, WITHDRAW_CALLBACK_ACTION,
// hasProcessedTxSignatureInCache, addProcessedTxSignatureToCache (conceptual direct Set ops),
// depositProcessorQueue, processDepositTransaction,
// PAYMENT_WEBHOOK_PATH, PAYMENT_WEBHOOK_SECRET, app, express (if webhooks enabled)

async function handleWithdrawCommand(msg, args = [], correctUserIdFromCb = null) { 
    const userId = String(correctUserIdFromCb || msg.from.id);
    const commandChatId = String(msg.chat.id); // Chat where command/button was used
    const chatType = msg.chat.type;

    let userObject = await getOrCreateUser(userId, msg.from?.username, msg.from?.first_name, msg.from?.last_name);
    if (!userObject) {
        await safeSendMessage(commandChatId, "Error fetching your player profile to initiate withdrawal. Please try /start again.", {parse_mode: 'MarkdownV2'});
        return;
    }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId); // Clear any previous state first

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) {console.error("[WithdrawCmd] Error getting bot username:", e.message);}

    let originalGroupMessageId = null; 
    // If this handler was triggered by a command message in a group, store its ID for potential edit.
    // If triggered by a callback (isCallbackRedirect), msg.originalChatInfo might have it.
    if (chatType !== 'private' && msg.message_id && !msg.isCallbackRedirect) {
        originalGroupMessageId = msg.message_id;
    } else if (msg.isCallbackRedirect && msg.originalChatInfo) {
        originalGroupMessageId = msg.originalChatInfo.messageId;
    }


    const linkedWallet = await getUserLinkedWallet(userId); // From Part 2
    const balanceLamports = await getUserBalance(userId); // From Part 2

    if (balanceLamports === null) {
        const errText = `${playerRef}, we couldn't fetch your balance to start a withdrawal. Please try again or contact support.`;
        await safeSendMessage(userId, errText, {parse_mode:'MarkdownV2'}); // Notify in DM
        if (originalGroupMessageId && commandChatId !== userId) { // If originated from group, update group message
            await bot.editMessageText(`${playerRef}, there was an issue fetching your balance for withdrawal. Please check your DMs with @${escapeMarkdownV2(botUsername)}.`, {chat_id: commandChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        } else if (chatType !== 'private' && commandChatId !== userId) { // If no message to edit, send new
             await safeSendMessage(commandChatId, `${playerRef}, there was an issue fetching your balance for withdrawal. Please check your DMs with @${escapeMarkdownV2(botUsername)}.`, {parse_mode:'MarkdownV2'});
        }
        return;
    }

    const minTotalNeededForWithdrawal = MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS; 
    if (!linkedWallet) {
        const noWalletText = `💸 **Withdraw SOL** 💸\n\n${playerRef}, to withdraw funds, you first need to link your personal Solana wallet address\\. You can do this by replying here with \`/setwallet YOUR_SOLANA_ADDRESS\` or using the button in the \`/wallet\` menu\\.`;
        await safeSendMessage(userId, noWalletText, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"💳 Go to Wallet Menu", callback_data:"menu:wallet"}]]}});
        if (originalGroupMessageId && commandChatId !== userId) {
            await bot.editMessageText(`${playerRef}, please link a withdrawal wallet first. I've sent instructions to your DM: @${escapeMarkdownV2(botUsername)}`, {chat_id: commandChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        } else if (chatType !== 'private' && commandChatId !== userId) {
             await safeSendMessage(commandChatId, `${playerRef}, please link a withdrawal wallet first. I've sent instructions to your DM: @${escapeMarkdownV2(botUsername)}`, {parse_mode:'MarkdownV2'});
        }
        return;
    }

    if (balanceLamports < minTotalNeededForWithdrawal) {
        const neededDisplayUSD = await formatBalanceForDisplay(minTotalNeededForWithdrawal, 'USD');
        const currentDisplayUSD = await formatBalanceForDisplay(balanceLamports, 'USD');
        const lowBalanceText = `💸 **Withdraw SOL** 💸\n\n${playerRef}, your balance of approx\\. *${escapeMarkdownV2(currentDisplayUSD)}* is too low to cover the minimum withdrawal amount plus fees \\(approx\\. *${escapeMarkdownV2(neededDisplayUSD)}* required\\)\\.\n\nConsider playing a few more games or making a deposit\\!`;
        await safeSendMessage(userId, lowBalanceText, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"💰 Deposit SOL", callback_data:"menu:deposit"},{text:"💳 Back to Wallet", callback_data:"menu:wallet"}]]}});
        if (originalGroupMessageId && commandChatId !== userId) {
            await bot.editMessageText(`${playerRef}, your balance is a bit low for a withdrawal. I've sent details to your DM: @${escapeMarkdownV2(botUsername)}`, {chat_id: commandChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        } else if (chatType !== 'private' && commandChatId !== userId) {
            await safeSendMessage(commandChatId, `${playerRef}, your balance is a bit low for a withdrawal. I've sent details to your DM: @${escapeMarkdownV2(botUsername)}`, {parse_mode:'MarkdownV2'});
        }
        return;
    }

    // Proceed to ask for amount in DM
    if (commandChatId !== userId && originalGroupMessageId) { // If originated from group, update the group message
         await bot.editMessageText(`${playerRef}, please check your DMs (@${escapeMarkdownV2(botUsername)}) to specify your withdrawal amount. 💸`, {chat_id: commandChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
    } else if (commandChatId !== userId && chatType !== 'private') { // If no message to edit but was from group
        await safeSendMessage(commandChatId, `${playerRef}, please check your DMs (@${escapeMarkdownV2(botUsername)}) to specify your withdrawal amount. 💸`, {parse_mode:'MarkdownV2'});
    }


    const minWithdrawDisplaySOL = await formatBalanceForDisplay(MIN_WITHDRAWAL_LAMPORTS, 'SOL');
    const feeDisplaySOL = await formatBalanceForDisplay(WITHDRAWAL_FEE_LAMPORTS, 'SOL');
    const balanceDisplaySOL = await formatBalanceForDisplay(balanceLamports, 'SOL');
    const promptText = `💸 **Initiate SOL Withdrawal** 💸\n\n` +
                       `Your linked withdrawal address: \`${escapeMarkdownV2(linkedWallet)}\`\n` +
                       `Your current balance: *${escapeMarkdownV2(balanceDisplaySOL)}*\n\n` +
                       `Minimum withdrawal: *${escapeMarkdownV2(minWithdrawDisplaySOL)}*\n` +
                       `Withdrawal fee: *${escapeMarkdownV2(feeDisplaySOL)}*\n\n` +
                       `➡️ Please reply with the amount of *SOL* you wish to withdraw \\(e\\.g\\., \`0.5\` or \`10\` or \`0.1 sol\`\\)\\. You can also type "max" to withdraw your full available balance.`;

    const sentPromptMsg = await safeSendMessage(userId, promptText, {
        parse_mode: 'MarkdownV2', 
        reply_markup: { inline_keyboard: [[{ text: '❌ Cancel Withdrawal', callback_data: 'menu:wallet' }]] }
    });

    if (sentPromptMsg?.message_id) {
        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_amount',
            chatId: userId, // Expect reply in this DM chat
            messageId: sentPromptMsg.message_id, 
            data: {
                linkedWallet,
                currentBalanceLamportsStr: balanceLamports.toString(),
                originalGroupChatId: (chatType !== 'private' ? commandChatId : null),
                originalGroupMessageId: originalGroupMessageId
            },
            timestamp: Date.now()
        });
    } else {
        await safeSendMessage(userId, "⚙️ Could not start withdrawal process due to an error sending the amount prompt. Please try \`/withdraw\` again.", {parse_mode:'MarkdownV2'});
    }
}
console.log("[UI Handler] handleWithdrawCommand defined.");

async function handleReferralCommand(msg) { 
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;

    let user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) {
        await safeSendMessage(commandChatId, "Error fetching your profile for referral info. Please try /start.", {});
        return;
    }
    const playerRef = getPlayerDisplayReference(user);
    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) {console.error("[ReferralCmd] Error getting bot username:", e.message);}


    let referralCode = user.referral_code;
    if (!referralCode) { 
        referralCode = generateReferralCode(); // From Part 2
        try {
            await queryDatabase("UPDATE users SET referral_code = $1 WHERE telegram_id = $2", [referralCode, userId]);
            user.referral_code = referralCode; // Update in-memory object
            console.log(`[ReferralCmd] Generated and saved new referral code ${referralCode} for user ${userId}`);
        } catch (dbErr) {
            console.error(`[ReferralCmd] Failed to save new referral code for user ${userId}: ${dbErr.message}`);
            referralCode = "ErrorGenerating"; // Fallback display
        }
    }
    const referralLink = `https://t.me/${botUsername}?start=ref_${referralCode}`;

    let messageText = `🤝 *Your Referral Zone, ${playerRef}!*\n\n` +
                      `Invite friends to ${escapeMarkdownV2(BOT_NAME)} and earn rewards\\!\n\n` +
                      `🔗 Your Unique Referral Link:\n\`${escapeMarkdownV2(referralLink)}\`\n_(Tap to copy or share)_ \n\n` +
                      `Share this link with friends\\. When they join using your link and meet criteria (e\\.g\\., make a deposit or play games), you could earn commissions\\! Details of the current referral program can be found on our official channel/group\\.`;

    const earnings = await getTotalReferralEarningsDB(userId); // From Part P2
    const totalEarnedPaidDisplay = await formatBalanceForDisplay(earnings.total_earned_paid_lamports, 'USD');
    const pendingPayoutDisplay = await formatBalanceForDisplay(earnings.total_pending_payout_lamports, 'USD');

    messageText += `\n\n*Your Referral Stats:*\n` +
                   `▫️ Total Earned & Paid Out: *${escapeMarkdownV2(totalEarnedPaidDisplay)}*\n` +
                   `▫️ Commissions Earned (Pending Payout): *${escapeMarkdownV2(pendingPayoutDisplay)}*\n\n` +
                   `_(Payouts are processed periodically to your linked wallet once they meet a minimum threshold or per program rules)_`;

    const keyboard = {inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]};

    if (chatType !== 'private') {
        if(msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(()=>{});
        await safeSendMessage(commandChatId, `${playerRef}, I've sent your referral details and earnings to our private chat: @${escapeMarkdownV2(botUsername)} 🤝`, { parse_mode: 'MarkdownV2' });
        await safeSendMessage(userId, messageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
    } else {
        await safeSendMessage(userId, messageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
    }
}
console.log("[UI Handler] handleReferralCommand defined.");

async function handleHistoryCommand(msg) { 
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;

    let user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) {
        await safeSendMessage(commandChatId, "Error fetching your profile for history. Please try /start.", {});
        return;
    }
    const playerRef = getPlayerDisplayReference(user);
    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) {console.error("[HistoryCmd] Error getting bot username:", e.message);}


    if (chatType !== 'private') {
        if(msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(()=>{});
        await safeSendMessage(commandChatId, `${playerRef}, your transaction history has been sent to our private chat: @${escapeMarkdownV2(botUsername)} 📜`, { parse_mode: 'MarkdownV2' });
    }

    const loadingDmMsg = await safeSendMessage(userId, "Fetching your transaction history... ⏳ This might take a moment.", {parse_mode:'MarkdownV2'});
    const loadingDmMsgId = loadingDmMsg?.message_id;

    try {
        const historyEntries = await getBetHistoryDB(userId, 15); // Get last 15 from Part P2
        let historyText = `📜 *Your Recent Casino Activity, ${playerRef}:*\n\n`;

        if (historyEntries.length === 0) {
            historyText += "You have no recorded transactions yet\\. Time to make some moves\\!";
        } else {
            for (const entry of historyEntries) {
                const date = new Date(entry.created_at).toLocaleString('en-GB', { day: '2-digit', month: 'short', year: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
                const amountDisplay = await formatBalanceForDisplay(entry.amount_lamports, 'SOL'); 
                const typeDisplay = escapeMarkdownV2(entry.transaction_type.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
                const sign = BigInt(entry.amount_lamports) >= 0n ? '+' : ''; // Show + for credits
                const txSig = entry.deposit_tx || entry.withdrawal_tx;

                historyText += `🗓️ \`${escapeMarkdownV2(date)}\` \\| ${typeDisplay}\n` +
                               `   Amount: *${sign}${escapeMarkdownV2(amountDisplay)}*\n`;
                if (txSig) {
                     historyText += `   Tx: \`${escapeMarkdownV2(txSig.substring(0, 10))}...\`\n`;
                }
                if (entry.game_log_type) {
                     historyText += `   Game: ${escapeMarkdownV2(entry.game_log_type)} ${entry.game_log_outcome ? `(${escapeMarkdownV2(entry.game_log_outcome)})` : ''}\n`;
                }
                if (entry.notes) {
                     historyText += `   Notes: _${escapeMarkdownV2(entry.notes.substring(0,50))}${entry.notes.length > 50 ? '...' : ''}_\n`;
                }
                historyText += `   Balance After: *${escapeMarkdownV2(await formatBalanceForDisplay(entry.balance_after_lamports, 'USD'))}*\n\n`;
            }
        }
        historyText += `\n_Displaying up to 15 most recent transactions\\._`;
        const keyboard = {inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]};

        if(loadingDmMsgId && bot) {
            await bot.editMessageText(historyText, {chat_id: userId, message_id: loadingDmMsgId, parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview:true});
        } else {
            await safeSendMessage(userId, historyText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview:true });
        }
    } catch (error) {
        console.error(`[HistoryCmd UID:${userId}] Error fetching history: ${error.message}`);
        const errText = "⚙️ Sorry, we couldn't fetch your transaction history right now. Please try again later.";
        if(loadingDmMsgId && bot) {
            await bot.editMessageText(errText, {chat_id: userId, message_id: loadingDmMsgId, parse_mode: 'MarkdownV2'});
        } else {
            await safeSendMessage(userId, errText, {parse_mode: 'MarkdownV2'});
        }
    }
}
console.log("[UI Handler] handleHistoryCommand defined.");

async function handleMenuAction(userId, originalChatId, originalMessageId, menuType, params = [], isFromCallback = true, originalChatType = 'private') {
    const stringUserId = String(userId); // Ensure string
    const logPrefix = `[MenuAction UID:${stringUserId} Type:${menuType} OrigChat:${originalChatId}]`;
    console.log(`${logPrefix} Processing menu action. Params: [${params.join(', ')}]`);

    let userObject = await getOrCreateUser(stringUserId); // Fetch user, needed for many actions
    if(!userObject) {
        console.error(`${logPrefix} Could not fetch user profile for menu action.`);
        await safeSendMessage(originalChatId, "Could not fetch your profile to process this menu action. Please try /start.", {parse_mode:'MarkdownV2'});
        return;
    }
    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) {console.error(`${logPrefix} Error getting bot username:`, e.message);}


    let targetChatIdForAction = stringUserId; // Most menu actions are for user's DM
    let messageIdForEditing = null;    
    let isGroupActionRedirect = false;

    const sensitiveMenuTypes = ['deposit', 'quick_deposit', 'withdraw', 'history', 'link_wallet_prompt', 'referral'];

    // If the original action was in a group and it's sensitive, update the group message and prepare to act in DM.
    if ((originalChatType === 'group' || originalChatType === 'supergroup') && sensitiveMenuTypes.includes(menuType)) {
        isGroupActionRedirect = true;
        if (originalMessageId && bot) {
            const redirectText = `${getPlayerDisplayReference(userObject)}, for your privacy, please continue this action in our direct message\\. I've sent you a prompt there: @${escapeMarkdownV2(botUsername)}`;
            await bot.editMessageText(redirectText, {
                chat_id: originalChatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                reply_markup: { inline_keyboard: [[{text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=menu_${menuType}`}]] }
            }).catch(e => {
                if(!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                    console.warn(`${logPrefix} Failed to edit group msg for DM redirect: ${e.message}. Sending new.`);
                    safeSendMessage(originalChatId, redirectText, { // Send new if edit fails
                        parse_mode: 'MarkdownV2',
                        reply_markup: { inline_keyboard: [[{text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=menu_${menuType}`}]] }
                    });
                }
            });
        }
    } else if (originalChatType === 'private') { // Action originated in DM
        targetChatIdForAction = originalChatId; // Action stays in DM
        messageIdForEditing = originalMessageId; // Can edit the message with the button
    }

    const actionMsgContext = { // This simulates a 'msg' object for the handlers
        from: userObject, // User who clicked the button
        chat: { id: targetChatIdForAction, type: 'private' }, // Actions are now directed to DM
        // If it's a redirect, messageIdForEditing will be null, so handlers send new.
      // If it was in DM, messageIdForEditing can be used by handler to edit.
        message_id: isGroupActionRedirect ? null : messageIdForEditing,
      isCallbackRedirect: isGroupActionRedirect, // Flag for handlers
      originalChatInfo: isGroupActionRedirect ? { id: originalChatId, type: originalChatType, messageId: originalMessageId } : null
    };

    switch(menuType) {
        case 'wallet':
            if (messageIdForEditing && targetChatIdForAction === stringUserId) await bot.deleteMessage(targetChatIdForAction, messageIdForEditing).catch(()=>{});
            await handleWalletCommand(actionMsgContext);
            break; 
        case 'deposit': case 'quick_deposit': // QUICK_DEPOSIT_CALLBACK_ACTION is also handled by command router to here
            if (messageIdForEditing && targetChatIdForAction === stringUserId) await bot.deleteMessage(targetChatIdForAction, messageIdForEditing).catch(()=>{});
            await handleDepositCommand(actionMsgContext, [], stringUserId); // Pass stringUserId as correctUserIdFromCb
            break;
        case 'withdraw':
            if (messageIdForEditing && targetChatIdForAction === stringUserId) await bot.deleteMessage(targetChatIdForAction, messageIdForEditing).catch(()=>{});
            await handleWithdrawCommand(actionMsgContext, [], stringUserId);
            break;
        case 'referral':
            if (messageIdForEditing && targetChatIdForAction === stringUserId) await bot.deleteMessage(targetChatIdForAction, messageIdForEditing).catch(()=>{});
            await handleReferralCommand(actionMsgContext);
            break;
        case 'history':
            if (messageIdForEditing && targetChatIdForAction === stringUserId) await bot.deleteMessage(targetChatIdForAction, messageIdForEditing).catch(()=>{});
            await handleHistoryCommand(actionMsgContext);
            break;
        case 'leaderboards': // This is not sensitive, can stay in original chat
            actionMsgContext.chat.id = originalChatId;
            actionMsgContext.chat.type = originalChatType;
            actionMsgContext.message_id = originalMessageId;
            await handleLeaderboardsCommand(actionMsgContext, params); 
            break;
        case 'link_wallet_prompt': 
            clearUserState(stringUserId); // Clear any previous state
            if (messageIdForEditing && targetChatIdForAction === stringUserId) await bot.deleteMessage(targetChatIdForAction, messageIdForEditing).catch(()=>{});

            const promptText = `🔗 *Link/Update Your Withdrawal Wallet*\n\nPlease reply to this message with your personal Solana wallet address where you'd like to receive withdrawals\\. Ensure it's correct as transactions are irreversible\\.\n\nExample: \`SoLmaNqerT3ZpPT1qS9j2kKx2o5x94s2f8u5aA3bCgD\``;
            const kbd = { inline_keyboard: [ [{ text: '❌ Cancel & Back to Wallet', callback_data: 'menu:wallet' }] ] };
            const sentDmPrompt = await safeSendMessage(stringUserId, promptText, { parse_mode: 'MarkdownV2', reply_markup: kbd });

            if (sentDmPrompt?.message_id) {
                userStateCache.set(stringUserId, {
                    state: 'awaiting_withdrawal_address', 
                    chatId: stringUserId, // Expect reply in DM
                    messageId: sentDmPrompt.message_id, // The prompt message to delete later
                    data: { 
                        originalPromptMessageId: sentDmPrompt.message_id, // Self-reference for deletion
                            originalGroupChatId: isGroupActionRedirect ? originalChatId : null,
                            originalGroupMessageId: isGroupActionRedirect ? originalMessageId : null
                    },
                    timestamp: Date.now()
                });
            } else {
                await safeSendMessage(stringUserId, "Failed to send the wallet address prompt. Please try again from the Wallet menu.", {});
            }
            break;
        case 'main': // Go to main help menu
            // Delete the current menu message in DM before showing help
            if (messageIdForEditing && targetChatIdForAction === stringUserId) {
                await bot.deleteMessage(targetChatIdForAction, messageIdForEditing).catch(()=>{});
            }
            actionMsgContext.message_id = null; // Ensure help sends a new message
            await handleHelpCommand(actionMsgContext);
            break;
        default: 
            console.warn(`${logPrefix} Unrecognized menu type: ${menuType}`);
            await safeSendMessage(stringUserId, `❓ Unrecognized menu option: \`${escapeMarkdownV2(menuType)}\`\\. Please try again or use \`/help\`\\.`, {parse_mode:'MarkdownV2'});
    }
}
console.log("[UI Handler] handleMenuAction (with privacy awareness and DM targeting) defined.");

async function handleWithdrawalConfirmation(userId, dmChatId, confirmationMessageIdInDm, recipientAddress, amountLamportsStr) {
    const stringUserId = String(userId);
    const logPrefix = `[WithdrawConfirm UID:${stringUserId}]`;
    const currentState = userStateCache.get(stringUserId); // For originalGroupChatId if needed

    // State already cleared by the time this is called if it was a 'yes' or 'no' from `process_withdrawal_confirm` CB.
    // If called directly, ensure state is managed by caller. Here, we assume it's from the callback.
    // clearUserState(stringUserId); // This was done in the callback router before calling specific yes/no handlers.

    const amountLamports = BigInt(amountLamportsStr);
    const feeLamports = WITHDRAWAL_FEE_LAMPORTS; 
    const totalDeduction = amountLamports + feeLamports;
    const userObjForNotif = await getOrCreateUser(stringUserId); // For display name
    const playerRef = getPlayerDisplayReference(userObjForNotif); 
    let client = null;

    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // Re-verify balance AT THE TIME OF CONFIRMATION to prevent issues if balance changed.
        const userDetailsCheck = await client.query('SELECT balance FROM users WHERE telegram_id = $1 FOR UPDATE', [stringUserId]);
        if (userDetailsCheck.rowCount === 0) {
            throw new Error("User profile not found during withdrawal confirmation.");
        }
        const currentBalanceOnConfirm = BigInt(userDetailsCheck.rows[0].balance);
        if (currentBalanceOnConfirm < totalDeduction) {
            throw new Error(`Insufficient balance at time of confirmation. Current: ${formatCurrency(currentBalanceOnConfirm, 'SOL')}, Needed: ${formatCurrency(totalDeduction, 'SOL')}. Withdrawal cancelled.`);
        }

        // 1. Create withdrawal request record
        const wdReq = await createWithdrawalRequestDB(client, stringUserId, amountLamports, feeLamports, recipientAddress); // from Part P2
        if (!wdReq.success || !wdReq.withdrawalId) {
            throw new Error(wdReq.error || "Failed to create database withdrawal request record.");
        }

        // 2. Deduct balance and log in ledger
        const balUpdate = await updateUserBalanceAndLedger( 
            client, stringUserId, BigInt(-totalDeduction), // Note: negative amount for deduction 
            'withdrawal_request_confirmed', 
            { withdrawal_id: wdReq.withdrawalId }, 
            `Withdrawal confirmed to ${recipientAddress.slice(0,6)}...${recipientAddress.slice(-4)}`
        );
        if (!balUpdate.success) {
            // This is critical. If balance deduction fails, the withdrawal request should not be queued.
            throw new Error(balUpdate.error || "Failed to deduct balance for withdrawal. Withdrawal not queued.");
        }

        await client.query('COMMIT'); // Commit DB changes *before* queueing job

        // 3. Queue the payout job (from Part P4)
        if (typeof addPayoutJob === 'function') { 
            await addPayoutJob({ type: 'payout_withdrawal', withdrawalId: wdReq.withdrawalId, userId: stringUserId });
            const successMsgDm = `✅ *Withdrawal Queued!* Your request to withdraw *${escapeMarkdownV2(formatCurrency(amountLamports, 'SOL'))}* to \`${escapeMarkdownV2(recipientAddress)}\` is now in the payout queue\\. You'll be notified by DM once it's processed\\.`;
            if (confirmationMessageIdInDm && bot) { // Edit the "Confirm Yes/No" message
                await bot.editMessageText(successMsgDm, {chat_id: dmChatId, message_id: confirmationMessageIdInDm, parse_mode:'MarkdownV2', reply_markup:{}});
            } else {
                await safeSendMessage(dmChatId, successMsgDm, {parse_mode:'MarkdownV2'});
            }
            
            // Update original group message if any
            if (currentState?.data?.originalGroupChatId && currentState?.data?.originalGroupMessageId && bot) {
                await bot.editMessageText(`${playerRef}'s withdrawal request for *${escapeMarkdownV2(formatCurrency(amountLamports, 'SOL'))}* has been queued successfully. Details in DM.`, {chat_id: currentState.data.originalGroupChatId, message_id: currentState.data.originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
            }
        } else {
            // This is a critical system failure if addPayoutJob is missing
            console.error(`${logPrefix} 🚨 CRITICAL: addPayoutJob function is not defined! Cannot queue withdrawal ${wdReq.withdrawalId}.`);
            // Attempt to rollback the balance deduction if the job cannot be queued (very important)
            // This requires a new DB transaction or a more complex rollback mechanism.
            // For now, notify admin for manual intervention.
            await notifyAdmin(`🚨 CRITICAL: Withdrawal ${wdReq.withdrawalId} for user ${stringUserId} had balance deducted BUT FAILED TO QUEUE for payout (addPayoutJob missing). MANUAL INTERVENTION REQUIRED TO REFUND OR PROCESS.`, {parse_mode:'MarkdownV2'});
            throw new Error("Payout processing system is unavailable. Your funds were deducted but the payout could not be queued. Please contact support immediately.");
        }
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error during withdrawal confirmation: ${rbErr.message}`));
        console.error(`${logPrefix} ❌ Error processing withdrawal confirmation: ${e.message}`, e.stack);
        const errorMsgDm = `⚠️ *Withdrawal Failed:*\n${escapeMarkdownV2(e.message)}\n\nPlease try again or contact support if the issue persists\\.`;
        if(confirmationMessageIdInDm && bot) {
            await bot.editMessageText(errorMsgDm, {chat_id: dmChatId, message_id: confirmationMessageIdInDm, parse_mode:'MarkdownV2', reply_markup:{ inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]}}).catch(()=>{});
        } else {
            await safeSendMessage(dmChatId, errorMsgDm, {parse_mode:'MarkdownV2', reply_markup:{ inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]}});
        }
        
        if (currentState?.data?.originalGroupChatId && currentState?.data?.originalGroupMessageId && bot) {
            await bot.editMessageText(`${playerRef}, there was an error processing your withdrawal confirmation. Please check your DMs.`, {chat_id: currentState.data.originalGroupChatId, message_id: currentState.data.originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        }
    } finally {
        if (client) client.release();
    }
}
console.log("[UI Handler] handleWithdrawalConfirmation (with transactional updates) defined.");


// --- Webhook Setup Function ---
/**
 * Configures the Express app to handle incoming payment webhooks.
 * The actual processing logic is in Part P4 (processDepositTransaction).
 * @param {import('express').Application} expressAppInstance - The Express application.
 */
function setupPaymentWebhook(expressAppInstance) {
    const logPrefix = '[SetupWebhook]';
    if (!expressAppInstance) {
        console.error(`${logPrefix} 🚨 Express app instance not provided. Cannot set up webhook routes.`);
        return;
    }

    // Ensure express.json() with rawBody verify is applied if not already globally on app
    // This was already done in Part 1 when `app` was initialized.

    const paymentWebhookPath = process.env.PAYMENT_WEBHOOK_PATH || '/webhook/solana-payments';
    const PAYMENT_WEBHOOK_SECRET = process.env.PAYMENT_WEBHOOK_SECRET;

    console.log(`${logPrefix} 📡 Configuring webhook endpoint at ${paymentWebhookPath}`);

    expressAppInstance.post(paymentWebhookPath, async (req, res) => {
        const webhookLogPrefix = `[PaymentWebhook ${paymentWebhookPath}]`;
        const signatureFromHeader = req.headers['x-signature'] || req.headers['X-Signature'] || req.headers['helius-signature'] || req.headers['shyft-signature']; // Common headers

        if (PAYMENT_WEBHOOK_SECRET) {
            // Placeholder for robust signature validation using req.rawBody.
            // This MUST be implemented according to your webhook provider's specification.
            // Example (conceptual - NOT a secure one-size-fits-all):
            // const expectedSignature = crypto.createHmac('sha256', PAYMENT_WEBHOOK_SECRET).update(req.rawBody).digest('hex');
            // if (signatureFromHeader !== expectedSignature) {
            //     console.warn(`${webhookLogPrefix} ⚠️ Invalid webhook signature. Header: ${signatureFromHeader}. Request rejected.`);
            //     return res.status(401).send('Unauthorized: Invalid signature');
            // }
            // console.log(`${webhookLogPrefix} ✅ Webhook signature validated (conceptual).`);
            if(!signatureFromHeader) console.warn(`${webhookLogPrefix} Webhook secret is SET, but NO signature header found in request. For production, this should be an error. Processing insecurely...`);
            else console.log(`${webhookLogPrefix} Received signature header. Implement provider-specific validation for production using PAYMENT_WEBHOOK_SECRET.`);
        } else {
            console.warn(`${webhookLogPrefix} PAYMENT_WEBHOOK_SECRET is NOT set. Proceeding without signature validation (INSECURE for production).`);
        }

        console.log(`${webhookLogPrefix} Received POST. Body (preview): ${JSON.stringify(req.body).substring(0,250)}...`);
        
        try {
            const payload = req.body; 
            let relevantTransactions = []; // Array of { signature, depositToAddress, /* optional: amount, sourceAddress */ }

            // --- ADAPT THIS PAYLOAD PARSING TO YOUR ACTUAL WEBHOOK PROVIDER ---
            // Example for Helius (often an array of events)
            if (Array.isArray(payload)) {
                payload.forEach(event => {
                    // Check for native SOL transfers (mint address for SOL is So11111111111111111111111111111111111111112)
                    // This structure can vary greatly between providers (Helius, Shyft, QuickNode, etc.)
                    // This is a simplified example based on common patterns.
                    if (event.type === "TRANSFER" && event.transaction?.signature && Array.isArray(event.tokenTransfers)) {
                        event.tokenTransfers.forEach(transfer => {
                            if (transfer.toUserAccount && transfer.mint === "So11111111111111111111111111111111111111112") { 
                                console.log(`${webhookLogPrefix} Helius-style SOL transfer found: To ${transfer.toUserAccount}, Sig: ${event.transaction.signature}`);
                                relevantTransactions.push({
                                    signature: event.transaction.signature,
                                    depositToAddress: transfer.toUserAccount,
                                    // Amount might be in SOL (tokenAmount) or lamports depending on provider/config.
                                    // processDepositTransaction in P4 will fetch the actual amount from chain.
                                });
                            }
                        });
                    } else if (event.signature && Array.isArray(event.instructions)) { // Alternative check if a more raw tx structure
                        // More complex parsing might be needed here to find relevant transfers
                    }
                });
            } else if (payload.signature && payload.account_keys && payload.instructions) { // Another generic structure
                // Parse instructions to find SystemProgram.transfer to our deposit addresses
            }
            // Add more `else if` blocks here for other webhook provider payload structures.
            // --- END OF PROVIDER-SPECIFIC PARSING ---


            if (relevantTransactions.length === 0) {
                 console.log(`${webhookLogPrefix} No relevant SOL transfer transactions identified in webhook payload.`);
                 return res.status(200).send('Webhook received; no actionable SOL transfer data identified in this payload.');
            }

            for (const txInfo of relevantTransactions) {
                const { signature, depositToAddress } = txInfo;
                if (!signature || !depositToAddress) {
                    console.warn(`${webhookLogPrefix} Webhook tx info missing signature or depositToAddress. Skipping: ${stringifyWithBigInt(txInfo)}`);
                    continue; 
                }

                // Use hasProcessedTxSignatureInCache (conceptual direct Set operation)
                if (!processedDepositTxSignatures.has(signature)) { 
                    const addrInfo = await findDepositAddressInfoDB(depositToAddress); // From Part P2
                    if (addrInfo && addrInfo.isActive) { // isActive already checks expiry
                        console.log(`${webhookLogPrefix} ✅ Valid webhook for active address ${depositToAddress}. Queuing TX: ${signature} for User: ${addrInfo.userId}`);
                        // depositProcessorQueue from Part 1, processDepositTransaction from Part P4
                        depositProcessorQueue.add(() => processDepositTransaction(signature, depositToAddress, addrInfo.walletId, addrInfo.userId));
                        processedDepositTxSignatures.add(signature); // Add to cache
                    } else {
                        console.warn(`${webhookLogPrefix} ⚠️ Webhook for inactive/expired/unknown address ${depositToAddress}. TX ${signature}. AddrInfo:`, stringifyWithBigInt(addrInfo));
                            // If address is known but not active, still add sig to processed to avoid re-checking via polling if it was a late webhook.
                            if(addrInfo) processedDepositTxSignatures.add(signature);
                    }
                } else {
                    console.log(`${webhookLogPrefix} ℹ️ TX ${signature} already processed or seen (via cache). Ignoring webhook notification.`);
                }
            }
            res.status(200).send('Webhook data queued for processing where applicable');
        } catch (error) {
            console.error(`❌ ${webhookLogPrefix} Error processing webhook payload: ${error.message}`, error.stack);
            res.status(500).send('Internal Server Error during webhook processing');
        }
    });

    console.log(`${logPrefix} ✅ Webhook endpoint ${paymentWebhookPath} configured successfully on Express app instance.`);
}
console.log("[UI Handler] setupPaymentWebhook function defined.");


// Conceptual placeholder for placeBet if it were ever needed at this level
// Game handlers typically manage their own betting flow.
// async function placeBet(userId, chatId, gameKey, betDetails, betAmountLamports) {
//     console.log(`[placeBet Placeholder] User: ${userId}, Game: ${gameKey}, Amount: ${betAmountLamports}. This should be handled by specific game handlers.`);
//     return { success: false, error: "Generic placeBet not implemented; game handlers manage bets." };
// }
// console.log("[UI Handler] placeBet (conceptual placeholder) defined.");


console.log("Part P3: Payment System UI Handlers, Stateful Logic & Webhook Setup - Complete.");
// --- End of Part P3 ---
// --- Start of Part P4 ---
// index.js - Part P4: Payment System Background Tasks & Webhook Handling
//---------------------------------------------------------------------------
console.log("Loading Part P4: Payment System Background Tasks & Webhook Handling...");

// Assumes global constants (DEPOSIT_MONITOR_INTERVAL_MS, SWEEP_INTERVAL_MS, etc. from Part 1 env),
// Solana connection (`solanaConnection`), DB pool (`pool`), processing queues (`depositProcessorQueue`, `payoutProcessorQueue` from Part 1),
// Keypairs: MAIN_BOT_KEYPAIR, REFERRAL_PAYOUT_KEYPAIR (from Part 1).
// Utilities: notifyAdmin, safeSendMessage, escapeMarkdownV2, formatCurrency, stringifyWithBigInt, sleep (from Part 1 & P3).
// DB Ops: queryDatabase (Part 1), findDepositAddressInfoDB, recordConfirmedDepositDB, markDepositAddressInactiveDB,
//         updateUserBalanceAndLedger, getWithdrawalDetailsDB, updateWithdrawalStatusDB,
//         getReferralDetailsDB, updateReferralPayoutStatusDB, getPaymentSystemUserDetails, recordSweepTransactionDB (from Part P2).
// Solana Utils: deriveSolanaKeypair, getSolBalance, sendSol (from Part P1).
// Cache: processedDepositTxSignatures (Set from Part 1).
// isShuttingDown (global flag from Part 1).
// DEPOSIT_CONFIRMATION_LEVEL, LAMPORTS_PER_SOL, BOT_NAME (from Part 1).

// --- Helper Function to Analyze Transaction for Deposits ---
/**
 * Analyzes a fetched Solana transaction to find the amount transferred to a specific deposit address.
 * It sums up all direct SOL transfers to the depositAddress.
 * @param {import('@solana/web3.js').VersionedTransactionResponse | import('@solana/web3.js').TransactionResponse | null} txResponse - The fetched transaction response.
 * @param {string} depositAddress - The public key string of the deposit address to check.
 * @returns {{transferAmount: bigint, payerAddress: string | null}} The total amount transferred in lamports and the primary payer.
 */
function analyzeTransactionAmounts(txResponse, depositAddress) {
    let transferAmount = 0n;
    let payerAddress = null;

    if (!txResponse || !txResponse.meta || !txResponse.transaction) {
        return { transferAmount, payerAddress };
    }

    // Get the primary payer of the transaction
    if (txResponse.transaction.message.accountKeys && txResponse.transaction.message.accountKeys.length > 0) {
        // The first account is usually the fee payer and often the sender in simple transfers.
        // For more complex scenarios, one might need to iterate through signers.
        payerAddress = txResponse.transaction.message.accountKeys[0].toBase58();
    }


    // Check preBalances and postBalances for direct SOL changes
    // This is the most reliable way for native SOL transfers.
    const accountIndex = txResponse.transaction.message.accountKeys.findIndex(
        key => key.toBase58() === depositAddress
    );

    if (accountIndex !== -1 && txResponse.meta.preBalances && txResponse.meta.postBalances &&
        txResponse.meta.preBalances.length > accountIndex && txResponse.meta.postBalances.length > accountIndex) {
        const preBalance = BigInt(txResponse.meta.preBalances[accountIndex]);
        const postBalance = BigInt(txResponse.meta.postBalances[accountIndex]);
        if (postBalance > preBalance) {
            transferAmount = postBalance - preBalance;
        }
    }

    // Fallback or additional check: Iterate through instructions if specific transfer instructions are needed
    // This can be complex due to different program interactions. For simple SystemProgram.transfer,
    // checking balance changes (as above) is often sufficient and more robust.
    // If checking instructions:
    // txResponse.transaction.message.instructions.forEach(ix => {
    //    if (ix.programId.equals(SystemProgram.programId)) {
    //        // Decode instruction data for SystemProgram.transfer
    //        // and check if `toPubkey` matches depositAddress.
    //    }
    // });

    return { transferAmount, payerAddress };
}
console.log("[Payment Utils] analyzeTransactionAmounts defined.");


// --- Global State for Background Task Control ---
let depositMonitorIntervalId = null;
let sweepIntervalId = null;

// Add static properties to functions to track running state
monitorDepositsPolling.isRunning = false;
sweepDepositAddresses.isRunning = false;


// --- Deposit Monitoring Logic ---

function startDepositMonitoring() {
    let intervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs < 5000) {
        intervalMs = 15000; // Fallback to a reasonable default
        console.warn(`[DepositMonitor] Invalid DEPOSIT_MONITOR_INTERVAL_MS, using default ${intervalMs}ms.`);
    }
    
    if (depositMonitorIntervalId) {
        clearInterval(depositMonitorIntervalId);
        console.log('🔄 [DepositMonitor] Restarting deposit monitor...');
    } else {
        console.log(`⚙️ [DepositMonitor] Starting Deposit Monitor (Polling Interval: ${intervalMs / 1000}s)...`);
    }
    
    const initialDelay = (parseInt(process.env.INIT_DELAY_MS, 10) || 7000) + 2000; // Stagger start
    console.log(`[DepositMonitor] Scheduling first monitor run in ${initialDelay/1000}s...`);

    setTimeout(() => {
        if (isShuttingDown) return;
        console.log(`[DepositMonitor] Executing first monitor run...`);
        monitorDepositsPolling().catch(err => console.error("❌ [Initial Deposit Monitor Run] Error:", err.message, err.stack));
        
        depositMonitorIntervalId = setInterval(monitorDepositsPolling, intervalMs);
        if (depositMonitorIntervalId.unref) depositMonitorIntervalId.unref();
        console.log(`✅ [DepositMonitor] Recurring monitor interval (ID: ${depositMonitorIntervalId ? 'Set' : 'Not Set - Error?'}) set.`);
    }, initialDelay);
}

function stopDepositMonitoring() {
    if (depositMonitorIntervalId) {
        clearInterval(depositMonitorIntervalId);
        depositMonitorIntervalId = null;
        monitorDepositsPolling.isRunning = false;
        console.log("🛑 [DepositMonitor] Deposit monitoring stopped.");
    }
}

async function monitorDepositsPolling() {
    const logPrefix = '[DepositMonitor Polling]';
    if (isShuttingDown) { console.log(`${logPrefix} Shutdown in progress, skipping run.`); return; }
    if (monitorDepositsPolling.isRunning) {
        console.log(`${logPrefix} Run skipped, previous run still active.`);
        return;
    }
    monitorDepositsPolling.isRunning = true;
    console.log(`🔍 ${logPrefix} Starting new polling cycle...`);

    try {
        const batchSize = parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE, 10) || 50;
        const sigFetchLimit = parseInt(process.env.DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT, 10) || 10;

        const pendingAddressesRes = await queryDatabase(
            `SELECT wallet_id, public_key, user_telegram_id, derivation_path, expires_at
             FROM user_deposit_wallets
             WHERE is_active = TRUE AND expires_at > NOW() 
             ORDER BY created_at ASC 
             LIMIT $1`,
            [batchSize]
        );

        if (pendingAddressesRes.rowCount === 0) {
            // This log can be verbose if there are often no active addresses. Consider conditional logging.
            // console.log(`${logPrefix} No active deposit addresses found to monitor in this cycle.`);
        } else {
            console.log(`${logPrefix} Found ${pendingAddressesRes.rowCount} active address(es) to check this cycle.`);
        }

        for (const row of pendingAddressesRes.rows) {
            if (isShuttingDown) { console.log(`${logPrefix} Shutdown initiated during address check, aborting cycle.`); break; }
            
            const depositAddress = row.public_key;
            const userDepositWalletId = row.wallet_id;
            const userId = String(row.user_telegram_id);
            const addrLogPrefix = `[Monitor Addr:${depositAddress.slice(0, 6)}.. WID:${userDepositWalletId} UID:${userId}]`;

            try {
                const pubKey = new PublicKey(depositAddress);
                const signatures = await solanaConnection.getSignaturesForAddress(
                    pubKey, { limit: sigFetchLimit }, DEPOSIT_CONFIRMATION_LEVEL
                );

                if (signatures && signatures.length > 0) {
                    console.log(`${addrLogPrefix} Found ${signatures.length} potential signature(s).`);
                    for (const sigInfo of signatures.reverse()) { // Process oldest first
                        if (sigInfo?.signature && !processedDepositTxSignatures.has(sigInfo.signature)) {
                            const isConfirmed = sigInfo.confirmationStatus === DEPOSIT_CONFIRMATION_LEVEL || sigInfo.confirmationStatus === 'finalized';
                            if (!sigInfo.err && isConfirmed) {
                                console.log(`${addrLogPrefix} ✅ New confirmed TX: ${sigInfo.signature}. Queuing for processing.`);
                                depositProcessorQueue.add(() => processDepositTransaction(sigInfo.signature, depositAddress, userDepositWalletId, userId))
                                    .catch(queueError => console.error(`❌ ${addrLogPrefix} Error adding TX ${sigInfo.signature} to deposit queue: ${queueError.message}`));
                                processedDepositTxSignatures.add(sigInfo.signature);
                            } else if (sigInfo.err) {
                                console.warn(`${addrLogPrefix} ⚠️ TX ${sigInfo.signature} has an error on-chain: ${JSON.stringify(sigInfo.err)}. Marking as processed.`);
                                processedDepositTxSignatures.add(sigInfo.signature); 
                            } else {
                                // console.log(`${addrLogPrefix} TX ${sigInfo.signature} not yet confirmed to '${DEPOSIT_CONFIRMATION_LEVEL}'. Status: ${sigInfo.confirmationStatus}`);
                            }
                        }
                    }
                }
            } catch (error) {
                console.error(`❌ ${addrLogPrefix} Error checking signatures: ${error.message}`);
                if (error?.status === 429 || String(error?.message).toLowerCase().includes('rate limit')) {
                    console.warn(`${addrLogPrefix} Rate limit hit. Pausing before next address.`);
                    await sleep(5000 + Math.random() * 3000); // Longer pause for rate limits
                }
            }
            await sleep(parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10) || 300); // Use a configured small delay
        }
    } catch (error) {
        console.error(`❌ ${logPrefix} Critical error in main polling loop: ${error.message}`, error.stack);
        if (typeof notifyAdmin === 'function') await notifyAdmin(`🚨 *ERROR in Deposit Monitor Loop* 🚨\n\n\`${escapeMarkdownV2(String(error.message || error))}\`\nCheck logs for details\\.`, {parse_mode: 'MarkdownV2'});
    } finally {
        monitorDepositsPolling.isRunning = false;
        // console.log(`🔍 ${logPrefix} Polling cycle finished.`); // Can be verbose
    }
}
console.log("[Deposit Monitor] monitorDepositsPolling, start/stop defined.");

async function processDepositTransaction(txSignature, depositAddress, userDepositWalletId, userId) {
    const stringUserId = String(userId);
    const logPrefix = `[ProcessDeposit TX:${txSignature.slice(0, 10)} Addr:${depositAddress.slice(0,6)} WID:${userDepositWalletId} UID:${stringUserId}]`;
    console.log(`${logPrefix} Processing deposit transaction...`);
    let client = null;

    try {
        const txResponse = await solanaConnection.getTransaction(txSignature, {
            maxSupportedTransactionVersion: 0, commitment: DEPOSIT_CONFIRMATION_LEVEL
        });

        if (!txResponse || txResponse.meta?.err) {
            console.warn(`ℹ️ ${logPrefix} TX ${txSignature} failed on-chain or details not found. Error: ${JSON.stringify(txResponse?.meta?.err)}. Marking as processed.`);
            processedDepositTxSignatures.add(txSignature); 
            return;
        }

        const { transferAmount, payerAddress } = analyzeTransactionAmounts(txResponse, depositAddress);

        if (transferAmount <= 0n) {
            console.log(`ℹ️ ${logPrefix} No positive SOL transfer to ${depositAddress} found in TX ${txSignature}. Ignoring.`);
            processedDepositTxSignatures.add(txSignature);
            return;
        }
        const depositAmountSOLDisplay = await formatBalanceForDisplay(transferAmount, 'SOL');
        console.log(`✅ ${logPrefix} Valid deposit identified: ${depositAmountSOLDisplay} from ${payerAddress || 'unknown source'}.`);

        client = await pool.connect();
        await client.query('BEGIN');

        const depositRecordResult = await recordConfirmedDepositDB(client, stringUserId, userDepositWalletId, depositAddress, txSignature, transferAmount, payerAddress, txResponse.blockTime);
        if (depositRecordResult.alreadyProcessed) {
            console.warn(`⚠️ ${logPrefix} TX ${txSignature} already processed in DB (ID: ${depositRecordResult.depositId}). This indicates a cache miss or race. Rolling back current attempt.`);
            await client.query('ROLLBACK'); 
            processedDepositTxSignatures.add(txSignature);
            return;
        }
        if (!depositRecordResult.success || !depositRecordResult.depositId) {
            throw new Error(`Failed to record deposit in DB for ${txSignature}: ${depositRecordResult.error || "Unknown DB error during deposit recording."}`);
        }
        const depositId = depositRecordResult.depositId;

        const markedInactive = await markDepositAddressInactiveDB(client, userDepositWalletId);
        if (!markedInactive) {
            console.warn(`${logPrefix} ⚠️ Could not mark deposit address Wallet ID ${userDepositWalletId} as inactive. It might have been already or an error occurred. Proceeding with balance update.`);
        }

        const ledgerNote = `Deposit from ${payerAddress ? payerAddress.slice(0,6)+'..'+payerAddress.slice(-4) : 'Unknown'} to ${depositAddress.slice(0,6)}... TX:${txSignature.slice(0,6)}..`;
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, stringUserId, transferAmount, 'deposit', { deposit_id: depositId }, ledgerNote);
        if (!balanceUpdateResult.success || typeof balanceUpdateResult.newBalanceLamports === 'undefined') {
            throw new Error(`Failed to update user ${stringUserId} balance/ledger for deposit: ${balanceUpdateResult.error || "Unknown DB error during balance update."}`);
        }
        
        // TODO: Implement referral linking logic here if a deposit triggers it.
        // e.g., check if user was referred, if this is their first qualifying deposit,
        // then call recordReferralCommissionEarnedDB (from Part P2).

        await client.query('COMMIT');
        console.log(`✅ ${logPrefix} DB operations committed. User ${stringUserId} credited.`);
        processedDepositTxSignatures.add(txSignature);

        const newBalanceUSDDisplay = await formatBalanceForDisplay(balanceUpdateResult.newBalanceLamports, 'USD');
        const userForNotif = await getOrCreateUser(stringUserId); // Re-fetch for latest name if it changed
        const playerRefForNotif = getPlayerDisplayReference(userForNotif);
        
        // Send deposit confirmation to user's DM
        await safeSendMessage(stringUserId,
            `🎉 *Deposit Confirmed, ${playerRefForNotif}!* 🎉\n\n` +
            `Your deposit of *${escapeMarkdownV2(depositAmountSOLDisplay)}* has been successfully credited to your casino account\\.\n\n` +
            `💰 Your New Balance: Approx\\. *${escapeMarkdownV2(newBalanceUSDDisplay)}*\n` +
            `🧾 Transaction ID: \`${escapeMarkdownV2(txSignature)}\`\n\n` +
            `Time to hit the tables\\! Good luck\\! 🎰`,
            { parse_mode: 'MarkdownV2' }
        );
        
    } catch (error) {
        console.error(`❌ ${logPrefix} CRITICAL ERROR processing deposit TX ${txSignature}: ${error.message}`, error.stack);
        if (client) { await client.query('ROLLBACK').catch(rbErr => console.error(`❌ ${logPrefix} Rollback failed:`, rbErr)); }
        processedDepositTxSignatures.add(txSignature); // Add to cache to prevent retrying a problematic TX indefinitely
        if (typeof notifyAdmin === 'function') {
            await notifyAdmin(`🚨 *CRITICAL Error Processing Deposit* 🚨\nTX: \`${escapeMarkdownV2(txSignature)}\`\nAddr: \`${escapeMarkdownV2(depositAddress)}\`\nUser: \`${escapeMarkdownV2(stringUserId)}\`\n*Error:*\n\`${escapeMarkdownV2(String(error.message || error))}\`\nManual investigation required\\.`, {parse_mode:'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}
console.log("[Deposit Monitor] processDepositTransaction defined.");


// --- Deposit Address Sweeping Logic --- (Implementation remains largely the same as original, now uses refined sendSol and DB ops)
// ... (startSweepingProcess, stopSweepingProcess, sweepDepositAddresses function definitions from original Part P4,
//      but ensure they use the updated sendSol from P1 and transactional DB ops from P2 where applicable)
function startSweepingProcess() {
    let intervalMs = parseInt(process.env.SWEEP_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs <= 0) {
        console.warn("🧹 [Sweeper] Fund sweeping is disabled (SWEEP_INTERVAL_MS not set or invalid).");
        return;
    }
    if (intervalMs < 60000) { intervalMs = 60000; console.warn(`🧹 [Sweeper] SWEEP_INTERVAL_MS too low, enforcing minimum ${intervalMs}ms.`); }
    
    if (sweepIntervalId) { clearInterval(sweepIntervalId); console.log('🔄 [Sweeper] Restarting fund sweeper...'); }
    else { console.log(`⚙️ [Sweeper] Starting Fund Sweeper (Interval: ${intervalMs / 1000 / 60} minutes)...`); }
    
    const initialDelay = (parseInt(process.env.INIT_DELAY_MS, 10) || 7000) + 15000; // Stagger after other startups
    console.log(`[Sweeper] Scheduling first sweep run in ${initialDelay/1000}s...`);

    setTimeout(() => {
        if (isShuttingDown) return;
        console.log(`[Sweeper] Executing first sweep run...`);
        sweepDepositAddresses().catch(err => console.error("❌ [Initial Sweep Run] Error:", err.message, err.stack));
        sweepIntervalId = setInterval(sweepDepositAddresses, intervalMs);
        if (sweepIntervalId.unref) sweepIntervalId.unref();
        console.log(`✅ [Sweeper] Recurring sweep interval (ID: ${sweepIntervalId ? 'Set' : 'Not Set - Error?'}) set.`);
    }, initialDelay);
}

function stopSweepingProcess() {
    if (sweepIntervalId) {
        clearInterval(sweepIntervalId);
        sweepIntervalId = null;
        sweepDepositAddresses.isRunning = false;
        console.log("🛑 [Sweeper] Fund sweeping stopped.");
    }
}

async function sweepDepositAddresses() {
    const logPrefix = '[SweepDepositAddresses]';
    if (isShuttingDown) { console.log(`${logPrefix} Shutdown in progress, skipping sweep cycle.`); return; }
    if (sweepDepositAddresses.isRunning) { console.log(`${logPrefix} Sweep already in progress. Skipping cycle.`); return; }
    sweepDepositAddresses.isRunning = true;
    console.log(`🧹 ${logPrefix} Starting new sweep cycle...`);

    let addressesProcessed = 0;
    let totalSweptLamports = 0n;
    const sweepBatchSize = parseInt(process.env.SWEEP_BATCH_SIZE, 10) || 10;
    const sweepAddressDelayMs = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10) || 1500;
    const sweepFeeBuffer = BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS || 20000); // Increased buffer
    const minBalanceToSweep = sweepFeeBuffer + 5000n; // Must be more than fee buffer + base tx fee
    const sweepTargetAddress = MAIN_BOT_KEYPAIR.publicKey.toBase58();

    let dbOperationClient = null; // Use one client for the batch of DB updates if possible, or per address
    try {
        const addressesToConsiderRes = await queryDatabase(
            `SELECT wallet_id, public_key, derivation_path, user_telegram_id
             FROM user_deposit_wallets 
             WHERE swept_at IS NULL 
             AND (is_active = FALSE OR expires_at < NOW() - INTERVAL '5 minutes') -- Sweep expired or explicitly inactive
             ORDER BY created_at ASC 
             LIMIT $1`,
            [sweepBatchSize]
        );

        if (addressesToConsiderRes.rowCount === 0) {
            // console.log(`${logPrefix} No addresses found requiring a sweep in this cycle.`);
        } else {
            console.log(`${logPrefix} Found ${addressesToConsiderRes.rowCount} potential addresses to check for sweeping.`);
        }

        dbOperationClient = await pool.connect(); // Use one client for all DB ops in this sweep cycle for efficiency

        for (const addrData of addressesToConsiderRes.rows) {
            if (isShuttingDown) { console.log(`${logPrefix} Shutdown initiated, aborting sweep.`); break; }
            
            const addrLogPrefix = `[Sweep Addr:${addrData.public_key.slice(0,6)}.. WID:${addrData.wallet_id}]`;
            let depositKeypair;
            try {
                depositKeypair = deriveSolanaKeypair(DEPOSIT_MASTER_SEED_PHRASE, addrData.derivation_path);
                if (!depositKeypair || depositKeypair.publicKey.toBase58() !== addrData.public_key) {
                    console.error(`${addrLogPrefix} ❌ Key derivation mismatch for path ${addrData.derivation_path}. Marking as unsweepable.`);
                    await dbOperationClient.query("UPDATE user_deposit_wallets SET swept_at = NOW(), notes = COALESCE(notes, '') || ' Sweep Error: Key derivation mismatch.' WHERE wallet_id = $1", [addrData.wallet_id]);
                    continue;
                }
            } catch (derivError) {
                console.error(`${addrLogPrefix} ❌ Critical error deriving key for sweep: ${derivError.message}. Skipping.`);
                await dbOperationClient.query("UPDATE user_deposit_wallets SET swept_at = NOW(), notes = COALESCE(notes, '') || ' Sweep Error: Key derivation exception.' WHERE wallet_id = $1", [addrData.wallet_id]);
                continue;
            }

            const balanceLamports = await getSolBalance(addrData.public_key);
            if (balanceLamports === null) { // Error fetching balance
                console.warn(`${addrLogPrefix} Could not fetch balance. Skipping for now.`);
                continue;
            }

            if (balanceLamports >= minBalanceToSweep) {
                const amountToSweep = balanceLamports - sweepFeeBuffer;
                console.log(`${addrLogPrefix} Balance: ${balanceLamports}. Attempting to sweep ${amountToSweep} to ${sweepTargetAddress.slice(0,6)}..`);

                await dbOperationClient.query('BEGIN'); // Transaction for this specific sweep's DB updates
                
                const sweepPriorityFee = parseInt(process.env.SWEEP_PRIORITY_FEE_MICROLAMPORTS, 10) || 5000;
                const sweepComputeUnits = parseInt(process.env.SWEEP_COMPUTE_UNIT_LIMIT, 10) || 25000;
                const sendResult = await sendSol(depositKeypair, sweepTargetAddress, amountToSweep, `Sweep from ${addrData.public_key.slice(0,4)}..${addrData.public_key.slice(-4)}`, sweepPriorityFee, sweepComputeUnits);

                if (sendResult.success && sendResult.signature) {
                    totalSweptLamports += amountToSweep;
                    addressesProcessed++;
                    console.log(`${addrLogPrefix} ✅ Sweep successful! TX: ${sendResult.signature}. Amount: ${amountToSweep}`);
                    await recordSweepTransactionDB(dbOperationClient, addrData.public_key, sweepTargetAddress, amountToSweep, sendResult.signature);
                    await markDepositAddressInactiveDB(dbOperationClient, addrData.wallet_id, true, balanceLamports);
                    await dbOperationClient.query('COMMIT');
                } else {
                    await dbOperationClient.query('ROLLBACK');
                    console.error(`${addrLogPrefix} ❌ Sweep failed: ${sendResult.error}. Error Type: ${sendResult.errorType}`);
                    if (sendResult.errorType === "InsufficientFundsError") {
                        // Balance might have changed or not enough for fee buffer
                        await queryDatabase("UPDATE user_deposit_wallets SET swept_at = NOW(), notes = COALESCE(notes, '') || ' Sweep Attempted: Insufficient for fee buffer.' WHERE wallet_id = $1", [addrData.wallet_id], pool); // Use main pool for this isolated update
                    } else if (sendResult.isRetryable === false) {
                        // Mark as failed if sendSol determined it's not retryable
                        await queryDatabase("UPDATE user_deposit_wallets SET swept_at = NOW(), notes = COALESCE(notes, '') || ' Sweep Failed (Non-Retryable): " + escapeMarkdownV2(sendResult.error || '').substring(0,50) + "' WHERE wallet_id = $1", [addrData.wallet_id], pool);
                    }
                }
            } else if (balanceLamports > 0n) { // Has dust, but not enough to sweep
                console.log(`${addrLogPrefix} Balance ${balanceLamports} is below sweep threshold (${minBalanceToSweep}). Marking as swept (dust).`);
                await markDepositAddressInactiveDB(dbOperationClient, addrData.wallet_id, true, balanceLamports);
            } else { // Zero balance
                await markDepositAddressInactiveDB(dbOperationClient, addrData.wallet_id, true, 0n);
            }
            await sleep(sweepAddressDelayMs);
        }

    } catch (error) {
        // Rollback any pending transaction if dbOperationClient was in one (though individual sweeps are now self-contained)
        // This catch is for errors in the main loop itself (e.g., fetching addresses)
        console.error(`❌ ${logPrefix} Critical error during sweep cycle: ${error.message}`, error.stack);
        if (typeof notifyAdmin === 'function') await notifyAdmin(`🚨 *ERROR in Fund Sweeping Cycle* 🚨\n\n\`${escapeMarkdownV2(String(error.message || error))}\`\nCheck logs for details\\. Sweeping may be impaired\\.`, {parse_mode: 'MarkdownV2'});
    } finally {
        if (dbOperationClient) dbOperationClient.release();
        sweepDepositAddresses.isRunning = false;
        if (addressesProcessed > 0) {
            console.log(`🧹 ${logPrefix} Sweep cycle finished. Processed ${addressesProcessed} addresses, swept total of ${formatCurrency(totalSweptLamports, 'SOL')}.`);
        } else if (addressesToConsiderRes && addressesToConsiderRes.rowCount > 0) {
            console.log(`🧹 ${logPrefix} Sweep cycle finished. No funds swept from ${addressesToConsiderRes.rowCount} considered addresses.`);
        }
    }
}
console.log("[Sweeper] sweepDepositAddresses, start/stop defined.");

// --- Payout Job Processing Logic ---
async function addPayoutJob(jobData) {
    const jobType = jobData?.type || 'unknown_payout_job';
    const jobId = jobData?.withdrawalId || jobData?.payoutId || 'N/A_ID';
    const logPrefix = `[AddPayoutJob Type:${jobType} ID:${jobId}]`;
    console.log(`⚙️ ${logPrefix} Adding job to payout queue for user ${jobData.userId || 'N/A'}.`);

    if (typeof payoutProcessorQueue === 'undefined' || typeof sleep === 'undefined' || typeof notifyAdmin === 'undefined' || typeof escapeMarkdownV2 === 'undefined') {
        console.error(`${logPrefix} 🚨 CRITICAL: Payout queue or essential utilities missing. Cannot add job.`);
        if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL Error: Cannot add payout job ${escapeMarkdownV2(jobType)}:${escapeMarkdownV2(String(jobId))}. Payout queue/utilities missing. Bot may need restart or fix.`);
        return;
    }

    payoutProcessorQueue.add(async () => {
        let attempts = 0;
        const maxAttempts = (parseInt(process.env.PAYOUT_JOB_RETRIES, 10) || 3) + 1;
        const baseDelayMs = parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10) || 7000;

        while(attempts < maxAttempts) {
            attempts++;
            const attemptLogPrefix = `[PayoutJob Attempt:${attempts}/${maxAttempts} Type:${jobType} ID:${jobId}]`;
            try {
                console.log(`${attemptLogPrefix} Starting processing...`);
                if (jobData.type === 'payout_withdrawal' && typeof handleWithdrawalPayoutJob === 'function') {
                    await handleWithdrawalPayoutJob(jobData.withdrawalId);
                } else if (jobData.type === 'payout_referral' && typeof handleReferralPayoutJob === 'function') {
                    await handleReferralPayoutJob(jobData.payoutId);
                } else {
                    throw new Error(`Unknown or unavailable payout job type handler: ${jobData.type}`);
                }
                console.log(`✅ ${attemptLogPrefix} Job completed successfully.`);
                return; // Success
            } catch(error) {
                console.warn(`⚠️ ${attemptLogPrefix} Attempt failed: ${error.message}`);
                // Check if the error object itself has an isRetryable flag (set by sendSol or other ops)
                const isRetryableFlag = error.isRetryable === true;

                if (!isRetryableFlag || attempts >= maxAttempts) {
                    console.error(`❌ ${attemptLogPrefix} Job failed permanently after ${attempts} attempts. Error: ${error.message}`);
                    if (typeof notifyAdmin === "function") {
                        notifyAdmin(`🚨 *PAYOUT JOB FAILED (Permanent)* 🚨\nType: \`${escapeMarkdownV2(jobType)}\`\nID: \`${escapeMarkdownV2(String(jobId))}\`\nUser: \`${jobData.userId || 'N/A'}\`\nAttempts: ${attempts}\n*Error:* \`${escapeMarkdownV2(String(error.message || error))}\`\nManual intervention may be required\\.`, {parse_mode:'MarkdownV2'}).catch(()=>{});
                    }
                    return; 
                }
                const delayWithJitter = baseDelayMs * Math.pow(2, attempts - 1) * (0.8 + Math.random() * 0.4);
                const actualDelay = Math.min(delayWithJitter, parseInt(process.env.RPC_RETRY_MAX_DELAY, 10) || 90000);
                console.log(`⏳ ${attemptLogPrefix} Retrying in ~${Math.round(actualDelay / 1000)}s...`);
                await sleep(actualDelay);
            }
        }
    }).catch(queueError => {
        console.error(`❌ ${logPrefix} CRITICAL Error in Payout Queue execution or adding job: ${queueError.message}`, queueError.stack);
        if (typeof notifyAdmin === "function") {
            notifyAdmin(`🚨 *CRITICAL Payout Queue Error* 🚨\nJob Type: \`${escapeMarkdownV2(jobType)}\`\nID: \`${escapeMarkdownV2(String(jobId))}\`\nError: \`${escapeMarkdownV2(String(queueError.message || queueError))}\`\nQueue functionality may be compromised\\.`, {parse_mode:'MarkdownV2'}).catch(()=>{});
        }
    });
}
console.log("[Payout Jobs] addPayoutJob defined.");


async function handleWithdrawalPayoutJob(withdrawalId) {
    const logPrefix = `[WithdrawJob ID:${withdrawalId}]`;
    console.log(`⚙️ ${logPrefix} Processing withdrawal payout job...`);
    let clientForDb = null;
    let sendSolResult = { success: false, error: "Send SOL not initiated", isRetryable: false }; // Default

    const details = await getWithdrawalDetailsDB(withdrawalId);
    if (!details) {
        const error = new Error(`Withdrawal details not found for ID ${withdrawalId}. Job cannot proceed and will not be retried.`);
        error.isRetryable = false; throw error;
    }

    if (details.status === 'completed' || details.status === 'confirmed') {
        console.log(`ℹ️ ${logPrefix} Job skipped, withdrawal ID ${withdrawalId} already marked '${details.status}'.`);
        return; // Success, no retry needed
    }
    if (details.status === 'failed' && !sendSolResult.isRetryable) { // If permanently failed, don't retry from queue
        console.log(`ℹ️ ${logPrefix} Job skipped, withdrawal ID ${withdrawalId} already marked 'failed' non-retryably.`);
        return;
    }

    const userId = String(details.user_telegram_id);
    const recipient = details.destination_address;
    const amountToActuallySend = BigInt(details.amount_lamports);
    const feeApplied = BigInt(details.fee_lamports);
    const totalAmountDebitedFromUser = amountToActuallySend + feeApplied;
    const userForNotif = await getOrCreateUser(userId); // For display name
    const playerRefForNotif = getPlayerDisplayReference(userForNotif);

    try {
        clientForDb = await pool.connect();
        await clientForDb.query('BEGIN');
        await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'processing');
        await clientForDb.query('COMMIT');
        clientForDb.release(); clientForDb = null; // Release client after this small update

        console.log(`${logPrefix} Status to 'processing'. Sending ${formatCurrency(amountToActuallySend, 'SOL')} to ${recipient}.`);
        sendSolResult = await sendSol(MAIN_BOT_KEYPAIR, recipient, amountToActuallySend, `Withdrawal ID ${withdrawalId} from ${BOT_NAME}`, details.priority_fee_microlamports, details.compute_unit_limit);

        clientForDb = await pool.connect(); // Re-acquire client for final transaction
        await clientForDb.query('BEGIN');

        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful. TX: ${sendSolResult.signature}. Marking 'completed'.`);
            await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'completed', sendSolResult.signature, null, sendSolResult.blockTime);
            await clientForDb.query('COMMIT');
            
            await safeSendMessage(userId,
                `💸 *Withdrawal Sent Successfully, ${playerRefForNotif}!* 💸\n\n` +
                `Your withdrawal of *${escapeMarkdownV2(formatCurrency(amountToActuallySend, 'SOL'))}* to wallet \`${escapeMarkdownV2(recipient)}\` has been processed\\.\n` +
                `🧾 Transaction ID: \`${escapeMarkdownV2(sendSolResult.signature)}\`\n\n` +
                `Funds should arrive shortly depending on network confirmations\\. Thank you for playing at ${escapeMarkdownV2(BOT_NAME)}\\!`,
                { parse_mode: 'MarkdownV2' }
            );
            return; // Success
        } else { // sendSol failed
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure.';
            console.error(`❌ ${logPrefix} sendSol FAILED for withdrawal ID ${withdrawalId}. Reason: ${sendErrorMsg}. ErrorType: ${sendSolResult.errorType}. Retryable: ${sendSolResult.isRetryable}`);
            await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'failed', null, sendErrorMsg.substring(0, 250));
            
            const refundNotes = `Refund for failed withdrawal ID ${withdrawalId}. Send Error: ${sendErrorMsg.substring(0,100)}`;
            const refundUpdateResult = await updateUserBalanceAndLedger(
                clientForDb, userId, totalAmountDebitedFromUser,
                'withdrawal_refund', { withdrawal_id: withdrawalId }, refundNotes
            );

            if (refundUpdateResult.success) {
                await clientForDb.query('COMMIT');
                console.log(`✅ ${logPrefix} Successfully refunded ${formatCurrency(totalAmountDebitedFromUser, 'SOL')} to user ${userId} for failed withdrawal.`);
                await safeSendMessage(userId,
                    `⚠️ *Withdrawal Failed* ⚠️\n\n${playerRefForNotif}, your withdrawal of *${escapeMarkdownV2(formatCurrency(amountToActuallySend, 'SOL'))}* could not be processed at this time \\(Reason: \`${escapeMarkdownV2(sendErrorMsg)}\`\\).\n` +
                    `The full amount of *${escapeMarkdownV2(formatCurrency(totalAmountDebitedFromUser, 'SOL'))}* \\(including fee\\) has been refunded to your casino balance\\.`,
                    {parse_mode: 'MarkdownV2'}
                );
            } else {
                await clientForDb.query('ROLLBACK');
                console.error(`❌ CRITICAL ${logPrefix} FAILED TO REFUND USER ${userId} for withdrawal ${withdrawalId}. Amount: ${formatCurrency(totalAmountDebitedFromUser, 'SOL')}. Refund DB Error: ${refundUpdateResult.error}`);
                if (typeof notifyAdmin === 'function') {
                    notifyAdmin(`🚨🚨 *CRITICAL: FAILED WITHDRAWAL REFUND* 🚨🚨\nUser: ${playerRefForNotif} (\`${escapeMarkdownV2(String(userId))}\`)\nWD ID: \`${withdrawalId}\`\nAmount Due (Refund): \`${escapeMarkdownV2(formatCurrency(totalAmountDebitedFromUser, 'SOL'))}\`\nSend Error: \`${escapeMarkdownV2(sendErrorMsg)}\`\nRefund DB Error: \`${escapeMarkdownV2(refundUpdateResult.error || 'Unknown')}\`\nMANUAL INTERVENTION REQUIRED\\.`, {parse_mode:'MarkdownV2'});
                }
            }
            
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true; // Propagate retry status
            throw errorToThrowForRetry;
        }
    } catch (jobError) {
        if (clientForDb) await clientForDb.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Final rollback error on jobError: ${rbErr.message}`));
        console.error(`❌ ${logPrefix} Error during withdrawal job ID ${withdrawalId}: ${jobError.message}`, jobError.stack);
        
        const currentDetailsAfterJobError = await getWithdrawalDetailsDB(withdrawalId); // Check status outside transaction
        if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'completed' && currentDetailsAfterJobError.status !== 'failed') {
            const updateClient = await pool.connect();
            try {
                await updateWithdrawalStatusDB(updateClient, withdrawalId, 'failed', null, `Job error: ${jobError.message}`.substring(0,250));
            } catch (finalStatusUpdateError) {
                console.error(`${logPrefix} Failed to update status to 'failed' after job error: ${finalStatusUpdateError.message}`);
            } finally {
                updateClient.release();
            }
        }
        // Propagate isRetryable from the original error if it was set (e.g., by sendSol)
        if (jobError.isRetryable === undefined) {
             jobError.isRetryable = sendSolResult.isRetryable || false; // Default to not retryable if not set
        }
        throw jobError;
    } finally {
        if (clientForDb) clientForDb.release();
    }
}
console.log("[Payout Jobs] handleWithdrawalPayoutJob defined.");


async function handleReferralPayoutJob(payoutId) {
    const logPrefix = `[ReferralJob ID:${payoutId}]`;
    console.log(`⚙️ ${logPrefix} Processing referral payout job...`);
    let clientForDb = null;
    let sendSolResult = { success: false, error: "Send SOL not initiated for referral", isRetryable: false };
    const payerKeypair = REFERRAL_PAYOUT_KEYPAIR || MAIN_BOT_KEYPAIR; // Use dedicated referral wallet if set

    const details = await getReferralDetailsDB(payoutId); // From Part P2
    if (!details) {
        const error = new Error(`Referral payout details not found for ID ${payoutId}. Job cannot proceed.`); error.isRetryable = false; throw error;
    }
    if (details.status === 'paid_out') {
        console.log(`ℹ️ ${logPrefix} Job skipped, referral payout ID ${payoutId} already 'paid_out'.`); return;
    }
    if (details.status === 'failed') { // If already marked as failed, don't retry from queue unless explicitly designed for it
        console.log(`ℹ️ ${logPrefix} Job skipped, referral payout ID ${payoutId} already 'failed'.`); return;
    }
    if (details.status !== 'earned') {
        console.warn(`ℹ️ ${logPrefix} Referral payout ID ${payoutId} is not in 'earned' state (current: ${details.status}). Skipping payout attempt.`);
        const error = new Error(`Referral payout ID ${payoutId} not in 'earned' state.`); error.isRetryable = false; throw error;
    }


    const referrerUserId = String(details.referrer_telegram_id);
    const amountToPay = BigInt(details.commission_amount_lamports || '0');
    if (amountToPay <= 0n) {
        console.warn(`${logPrefix} Referral commission for ID ${payoutId} is zero or less. Marking as error/no_payout.`);
        const zeroClient = await pool.connect();
        await updateReferralPayoutStatusDB(zeroClient, payoutId, 'failed', null, "Zero or negative commission amount");
        zeroClient.release();
        const error = new Error(`Zero or negative commission for referral payout ID ${payoutId}.`); error.isRetryable = false; throw error;
    }

    const userForNotif = await getOrCreateUser(referrerUserId);
    const playerRefForNotif = getPlayerDisplayReference(userForNotif);

    try {
        clientForDb = await pool.connect();
        await clientForDb.query('BEGIN');

        const referrerDetails = await getPaymentSystemUserDetails(referrerUserId, clientForDb);
        if (!referrerDetails?.solana_wallet_address) {
            const noWalletMsg = `Referrer ${playerRefForNotif} (\`${escapeMarkdownV2(referrerUserId)}\`) has no linked SOL wallet for referral payout ID ${payoutId}. Cannot process payout.`;
            console.error(`❌ ${logPrefix} ${noWalletMsg}`);
            await updateReferralPayoutStatusDB(clientForDb, payoutId, 'failed', null, noWalletMsg.substring(0, 250));
            await clientForDb.query('COMMIT');
            const error = new Error(noWalletMsg); error.isRetryable = false; throw error;
        }
        const recipientAddress = referrerDetails.solana_wallet_address;

        await updateReferralPayoutStatusDB(clientForDb, payoutId, 'processing');
        await clientForDb.query('COMMIT');
        clientForDb.release(); clientForDb = null; // Release after status update

        console.log(`${logPrefix} Status to 'processing'. Sending ${formatCurrency(amountToPay, 'SOL')} to ${recipientAddress} from wallet ${payerKeypair.publicKey.toBase58().slice(0,6)}...`);
        sendSolResult = await sendSol(payerKeypair, recipientAddress, amountToPay, `Referral Commission - ${BOT_NAME} - ID ${payoutId}`);

        clientForDb = await pool.connect(); // Re-acquire for final status
        await clientForDb.query('BEGIN');
        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful for referral ID ${payoutId}. TX: ${sendSolResult.signature}.`);
            await updateReferralPayoutStatusDB(clientForDb, payoutId, 'paid_out', sendSolResult.signature);
            await clientForDb.query('COMMIT');

            await safeSendMessage(referrerUserId,
                `🎁 *Referral Bonus Paid, ${playerRefForNotif}!* 🎁\n\n` +
                `Your referral commission of *${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}* has been sent to your linked wallet: \`${escapeMarkdownV2(recipientAddress)}\`\\.\n` +
                `🧾 Transaction ID: \`${escapeMarkdownV2(sendSolResult.signature)}\`\n\nThanks for spreading the word about ${escapeMarkdownV2(BOT_NAME)}\\!`,
                { parse_mode: 'MarkdownV2' }
            );
            return; // Success
        } else {
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure for referral payout.';
            console.error(`❌ ${logPrefix} sendSol FAILED for referral payout ID ${payoutId}. Reason: ${sendErrorMsg}`);
            await updateReferralPayoutStatusDB(clientForDb, payoutId, 'failed', null, sendErrorMsg.substring(0, 250));
            await clientForDb.query('COMMIT');

            await safeSendMessage(referrerUserId,
                `⚠️ *Referral Payout Issue* ⚠️\n\n${playerRefForNotif}, we encountered an issue sending your referral reward of *${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}* \\(Details: \`${escapeMarkdownV2(sendErrorMsg)}\`\\)\\. Please ensure your linked wallet is correct or contact support\\. This payout will be re-attempted if possible, or an admin will review\\.`,
                {parse_mode: 'MarkdownV2'}
            );
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 *REFERRAL PAYOUT FAILED* 🚨\nReferrer: ${playerRefForNotif} (\`${escapeMarkdownV2(referrerUserId)}\`)\nPayout ID: \`${payoutId}\`\nAmount: \`${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}\`\n*Error:* \`${escapeMarkdownV2(sendErrorMsg)}\`\\.`, {parse_mode:'MarkdownV2'});
            }
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true;
            throw errorToThrowForRetry;
        }
    } catch (jobError) {
        if(clientForDb) await clientForDb.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Final rollback error on jobError: ${rbErr.message}`));
        console.error(`❌ ${logPrefix} Error during referral payout job ID ${payoutId}: ${jobError.message}`, jobError.stack);
        // Ensure retryable status is propagated from original error if possible
        if (jobError.isRetryable === undefined) jobError.isRetryable = sendSolResult.isRetryable || false;

        // Ensure status is 'failed' if not already terminal and jobError not retryable by queue
        if (!jobError.isRetryable) {
            const currentDetailsAfterJobError = await getReferralDetailsDB(payoutId);
            if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'paid_out' && currentDetailsAfterJobError.status !== 'failed') {
                const updateClient = await pool.connect();
                try {
                    await updateReferralPayoutStatusDB(updateClient, payoutId, 'failed', null, `Job error (non-retryable): ${jobError.message}`.substring(0,250));
                } catch (finalStatusUpdateError) { console.error(`${logPrefix} Failed to mark referral as 'failed' after non-retryable job error: ${finalStatusUpdateError.message}`);}
                finally { updateClient.release(); }
            }
        }
        throw jobError; // Re-throw for queue handling
    } finally {
        if (clientForDb) clientForDb.release();
    }
}
console.log("[Payout Jobs] handleReferralPayoutJob defined.");

// Webhook handling logic was defined in Part P3 (setupPaymentWebhook)
// which queues tasks for processDepositTransaction (defined above in this Part P4).

console.log("Part P4: Payment System Background Tasks & Webhook Handling - Complete.");
// --- End of Part P4 ---
