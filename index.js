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
      return value.toString() + 'n';
    }
    if (typeof value === 'function') {
      return `[Function: ${value.name || 'anonymous'}]`;
    }
    if (value === undefined) {
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
  'DEPOSIT_CONFIRMATIONS': 'confirmed',
  'WITHDRAWAL_FEE_LAMPORTS': '10000', // Covers base fee + priority
  'MIN_WITHDRAWAL_LAMPORTS': '10000000', // 0.01 SOL
  'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '10000',
  'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000',
  'PAYOUT_COMPUTE_UNIT_LIMIT': '30000', // For simple SOL transfers
  'PAYOUT_JOB_RETRIES': '3',
  'PAYOUT_JOB_RETRY_DELAY_MS': '7000',
  'SWEEP_INTERVAL_MS': '300000', // 5 minutes
  'SWEEP_BATCH_SIZE': '15',
  'SWEEP_FEE_BUFFER_LAMPORTS': '20000',
  'SWEEP_COMPUTE_UNIT_LIMIT': '30000',
  'SWEEP_PRIORITY_FEE_MICROLAMPORTS': '5000',
  'SWEEP_ADDRESS_DELAY_MS': '1500',
  'SWEEP_RETRY_ATTEMPTS': '2',
  'SWEEP_RETRY_DELAY_MS': '10000',
  'RPC_MAX_CONCURRENT': '10',
  'RPC_RETRY_BASE_DELAY': '750',
  'RPC_MAX_RETRIES': '4',
  'RPC_RATE_LIMIT_COOLOFF': '3000', // Increased cooloff
  'RPC_RETRY_MAX_DELAY': '25000',
  'RPC_RETRY_JITTER': '0.3',
  'RPC_COMMITMENT': 'confirmed',
  'PAYOUT_QUEUE_CONCURRENCY': '4',
  'PAYOUT_QUEUE_TIMEOUT_MS': '90000',
  'DEPOSIT_PROCESS_QUEUE_CONCURRENCY': '5',
  'DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS': '45000',
  'TELEGRAM_SEND_QUEUE_CONCURRENCY': '1',
  'TELEGRAM_SEND_QUEUE_INTERVAL_MS': '1050', // Standard Telegram rate limit
  'TELEGRAM_SEND_QUEUE_INTERVAL_CAP': '1',
  'DEPOSIT_MONITOR_INTERVAL_MS': '15000',
  'DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE': '75',
  'DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT': '15',
  'WALLET_CACHE_TTL_MS': (15 * 60 * 1000).toString(),
  'DEPOSIT_ADDR_CACHE_TTL_MS': (parseInt(CASINO_ENV_DEFAULTS.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10) + 5 * 60 * 1000).toString(), // 5 mins longer than address expiry
  'MAX_PROCESSED_TX_CACHE': '10000',
  'INIT_DELAY_MS': '7000',
  'ENABLE_PAYMENT_WEBHOOKS': 'false',
  'PAYMENT_WEBHOOK_PORT': '3000',
  'PAYMENT_WEBHOOK_PATH': '/webhook/solana-payments', // Standardized path
  'SOL_PRICE_API_URL': 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
  'SOL_USD_PRICE_CACHE_TTL_MS': (3 * 60 * 1000).toString(),
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
    // Add if not already present (checking base URL part if keys are involved)
    combinedRpcEndpointsForConnection.push(SINGLE_MAINNET_RPC_FROM_ENV);
}
// If combinedRpcEndpointsForConnection is empty, RateLimitedConnection will use its internal hardcoded defaults.


const SHUTDOWN_FAIL_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
const MAX_RETRY_POLLING_DELAY = parseInt(process.env.MAX_RETRY_POLLING_DELAY, 10);
const INITIAL_RETRY_POLLING_DELAY = parseInt(process.env.INITIAL_RETRY_POLLING_DELAY, 10);
const JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.JACKPOT_CONTRIBUTION_PERCENT);
const MAIN_JACKPOT_ID = 'dice_escalator_main';
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
const DEPOSIT_CONFIRMATION_LEVEL = process.env.DEPOSIT_CONFIRMATIONS?.toLowerCase();
const WITHDRAWAL_FEE_LAMPORTS = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS);
const MIN_WITHDRAWAL_LAMPORTS = BigInt(process.env.MIN_WITHDRAWAL_LAMPORTS);

// Critical environment variable checks
if (!BOT_TOKEN) { console.error("🚨 FATAL ERROR: BOT_TOKEN is not defined. Bot cannot start."); process.exit(1); }
if (!DATABASE_URL) { console.error("🚨 FATAL ERROR: DATABASE_URL is not defined. Cannot connect to PostgreSQL."); process.exit(1); }
if (!DEPOSIT_MASTER_SEED_PHRASE) { console.error("🚨 FATAL ERROR: DEPOSIT_MASTER_SEED_PHRASE is not defined. Payment system cannot generate deposit addresses."); process.exit(1); }
// MAIN_BOT_KEYPAIR check done during its initialization.

if (combinedRpcEndpointsForConnection.length === 0 && HARCODED_RPC_ENDPOINTS.length === 0) { // HARCODED_RPC_ENDPOINTS from lib/solana-connection.js
    console.warn("⚠️ WARNING: No RPC URLs provided via environment (RPC_URLS, SOLANA_RPC_URL) AND no hardcoded RPCs configured or enabled in lib/solana-connection.js. RPC functionality will likely fail.");
}

const criticalGameScores = { TARGET_JACKPOT_SCORE, BOT_STAND_SCORE_DICE_ESCALATOR, DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE };
for (const [key, value] of Object.entries(criticalGameScores)) {
    if (isNaN(value) || value <=0) { // Also check for non-positive scores where applicable
        console.error(`🚨 FATAL ERROR: Game score parameter '${key}' ('${value}') is not a valid positive number. Check .env file or defaults.`);
        process.exit(1);
    }
}
// ... (other critical numeric config checks from previous version remain) ...
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


console.log("✅ BOT_TOKEN loaded successfully.");
if (ADMIN_USER_ID) console.log(`🔑 Admin User ID: ${ADMIN_USER_ID} loaded.`);
else console.log("ℹ️ INFO: No ADMIN_USER_ID set (optional, for admin alerts).");
console.log(`🔑 Payment System: DEPOSIT_MASTER_SEED_PHRASE is set (value not logged).`);
console.log(`📡 Using RPC Endpoints (from env): [${combinedRpcEndpointsForConnection.join(', ')}] (RateLimitedConnection may use internal defaults if this list is empty or fails).`);

// Helper to format lamports to SOL string for console logs, defined early for use here
function formatLamportsToSolStringForLog(lamports) {
    if (typeof lamports !== 'bigint') lamports = BigInt(lamports);
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
const rejectUnauthorizedSsl = process.env.DB_REJECT_UNAUTHORIZED === 'true'; // Defaulting to true is safer
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
    console.error(`[Admin Alert Failure] DB Pool Error (Idle Client): ${err.message || String(err)}`);
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
        throw error;
    }
}
console.log("[Global Utils] queryDatabase helper function defined.");

// --- CORRECTED RateLimitedConnection Instantiation ---
console.log("⚙️ Setting up Solana Connection...");
const connectionOptions = {
    commitment: process.env.RPC_COMMITMENT,
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
    // wsEndpoint: process.env.SOLANA_WSS_URL_OVERRIDE || undefined, // Example: if you need to override WSS
    // clientId: `${BOT_NAME}/${BOT_VERSION}` // Example: if you want a very specific client ID format
};

const solanaConnection = new RateLimitedConnection(
    combinedRpcEndpointsForConnection, // This array is prepared earlier
    connectionOptions
);
// --- End of RateLimitedConnection Instantiation ---


const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("🤖 Telegram Bot instance created and configured for polling.");

let app = null; // Express app instance for webhooks
if (process.env.ENABLE_PAYMENT_WEBHOOKS === 'true') {
    app = express();
    app.use(express.json({ // Middleware to parse JSON bodies, needed for webhooks
        verify: (req, res, buf) => { // Store raw body for potential signature verification
            req.rawBody = buf;
        }
    }));
    console.log("🚀 Express app initialized for payment webhooks (JSON body parser enabled).");
} else {
    console.log("ℹ️ Payment webhooks are disabled via ENABLE_PAYMENT_WEBHOOKS env var.");
}

const BOT_VERSION = process.env.BOT_VERSION || '3.3.2-errFixes'; // Use env var or default
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;

let isShuttingDown = false; // Global shutdown flag

// Global Caches & State
let activeGames = new Map(); // Stores active multiplayer game instances
let userCooldowns = new Map(); // Tracks user command cooldowns: userId -> timestamp
let groupGameSessions = new Map(); // Tracks group game sessions: chatId -> { currentGameId, currentGameType, lastActivity }

const walletCache = new Map(); // Stores { userId -> { solanaAddress, timestamp } } for withdrawal wallets
const activeDepositAddresses = new Map(); // Stores { depositAddressString -> { userId, expiresAtTimestamp } }
const processedDepositTxSignatures = new Set(); // Stores processed tx signatures to prevent duplicates
const PENDING_REFERRAL_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours for pending referrals
const pendingReferrals = new Map(); // Stores { referredUserId -> { referrerId, timestamp } }

const userStateCache = new Map(); // For multi-step interactions: userId -> { state, data, messageId, chatId }

const SOL_PRICE_CACHE_KEY = 'sol_usd_price_cache'; // More descriptive key
const solPriceCache = new Map(); // Stores { SOL_PRICE_CACHE_KEY -> { price, timestamp } }

const DICE_ESCALATOR_BUST_ON = 1;

console.log(`🚀 Initializing ${BOT_NAME} v${BOT_VERSION}...`);
console.log(`🕰️ Current system time: ${new Date().toISOString()}`);
console.log(`💻 Node.js Version: ${process.version}`);

const escapeMarkdownV2 = (text) => {
  if (text === null || typeof text === 'undefined') return '';
  return String(text).replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};
console.log("[Global Utils] escapeMarkdownV2 helper function defined.");

async function safeSendMessage(chatId, text, options = {}) {
    // ... (implementation from previous Part 1, remains unchanged but uses updated BOT_NAME, BOT_VERSION if they are used in any error/truncation messages)
    const LOG_PREFIX_SSM = `[safeSendMessage CH:${chatId}]`;
    if (!chatId || typeof text !== 'string') {
        console.error(`${LOG_PREFIX_SSM} Invalid input: ChatID is ${chatId}, Text type is ${typeof text}. Preview: ${String(text).substring(0, 100)}`);
        return undefined;
    }
    
    let messageToSend = text; 
    let finalOptions = { ...options };

    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsisBase = ` \\.\\.\\. \\(_message truncated by ${escapeMarkdownV2(BOT_NAME)}_\\)`; 
        const ellipsisPlain = `... (message truncated by ${BOT_NAME})`;
        const ellipsis = (finalOptions.parse_mode === 'MarkdownV2') ? ellipsisBase : ellipsisPlain;
        const truncateAt = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsis.length);
        messageToSend = messageToSend.substring(0, truncateAt) + ellipsis;
        console.warn(`${LOG_PREFIX_SSM} Message content was too long (${text.length} chars) and has been truncated.`);
    }

    if (!bot) {
        console.error(`${LOG_PREFIX_SSM} ⚠️ Error: Telegram 'bot' instance not available.`);
        return undefined;
    }

    try {
        if (typeof bot.sendMessage !== 'function') {
            throw new Error("'bot.sendMessage' is not a function.");
        }
        const sentMessage = await bot.sendMessage(chatId, messageToSend, finalOptions);
        return sentMessage;
    } catch (error) {
        console.error(`${LOG_PREFIX_SSM} ❌ Failed to send message. Code: ${error.code || 'N/A'}, Msg: ${error.message}`);
        if (error.response && error.response.body) {
            console.error(`${LOG_PREFIX_SSM} Telegram API Response: ${stringifyWithBigInt(error.response.body)}`);
            if (finalOptions.parse_mode === 'MarkdownV2' && error.response.body.description && error.response.body.description.includes("can't parse entities")) {
                console.warn(`${LOG_PREFIX_SSM} MarkdownV2 parse error detected. Attempting to send as plain text.`);
                console.error(`${LOG_PREFIX_SSM} Original MarkdownV2 text (first 200 chars): "${text.substring(0,200)}"`);
                try {
                    delete finalOptions.parse_mode; 
                    let plainTextForFallback = text; 
                    if (plainTextForFallback.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
                        const ellipsisPlainFallback = `... (message truncated by ${BOT_NAME})`; 
                        const truncateAtPlain = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsisPlainFallback.length);
                        plainTextForFallback = plainTextForFallback.substring(0, truncateAtPlain) + ellipsisPlainFallback;
                    }
                    return await bot.sendMessage(chatId, plainTextForFallback, finalOptions);
                } catch (fallbackError) {
                    console.error(`${LOG_PREFIX_SSM} ❌ Plain text fallback also failed. Code: ${fallbackError.code || 'N/A'}, Msg: ${fallbackError.message}`);
                    return undefined;
                }
            }
        }
        return undefined;
    }
}
console.log("[Global Utils] safeSendMessage (with MarkdownV2 fallback & refined truncation) defined.");


const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
console.log("[Global Utils] sleep helper function defined.");

async function notifyAdmin(message, options = {}) {
    // ... (implementation from previous Part 1, remains unchanged but uses updated BOT_NAME)
     if (ADMIN_USER_ID) {
        const adminAlertMessage = `🔔 *ADMIN ALERT* (${escapeMarkdownV2(BOT_NAME)}) 🔔\n\n${message}`;
        return safeSendMessage(ADMIN_USER_ID, adminAlertMessage, { parse_mode: 'MarkdownV2', ...options });
    } else {
        console.warn(`[Admin Notify - SKIPPED] No ADMIN_USER_ID set. Message (first 100 chars): ${String(message).substring(0,100)}...`);
        return null;
    }
}
console.log("[Global Utils] notifyAdmin helper function defined.");

console.log("⚙️ Setting up Price Feed Utilities...");
// fetchSolUsdPriceFromAPI, getSolUsdPrice, convertLamportsToUSDString, convertUSDToLamports
// ... (implementations from previous Part 1, remain unchanged but use updated BOT_NAME in logs/errors if applicable)
async function fetchSolUsdPriceFromAPI() {
    const apiUrl = process.env.SOL_PRICE_API_URL;
    const logPrefix = '[PriceFeed API]';
    try {
        console.log(`${logPrefix} Fetching SOL/USD price from ${apiUrl}...`);
        const response = await axios.get(apiUrl, { timeout: 8000 });
        if (response.data && response.data.solana && response.data.solana.usd) {
            const price = parseFloat(response.data.solana.usd);
            if (isNaN(price) || price <= 0) {
                throw new Error('Invalid or non-positive price data received from API.');
            }
            console.log(`${logPrefix} ✅ Successfully fetched SOL/USD price: $${price}`);
            return price;
        } else {
            console.error(`${logPrefix} ⚠️ SOL price not found or invalid structure in API response:`, stringifyWithBigInt(response.data)); // Log full data on structure error
            throw new Error('SOL price not found or invalid structure in API response.');
        }
    } catch (error) {
        const errMsg = error.isAxiosError ? error.message : String(error);
        console.error(`${logPrefix} ❌ Error fetching SOL/USD price: ${errMsg}`);
        if (error.response) {
            console.error(`${logPrefix} API Response Status: ${error.response.status}`);
            console.error(`${logPrefix} API Response Data:`, stringifyWithBigInt(error.response.data));
        }
        throw new Error(`Failed to fetch SOL/USD price: ${errMsg}`);
    }
}

async function getSolUsdPrice() {
    const logPrefix = '[getSolUsdPrice]';
    const cacheTtl = parseInt(process.env.SOL_USD_PRICE_CACHE_TTL_MS, 10);
    const cachedEntry = solPriceCache.get(SOL_PRICE_CACHE_KEY);

    if (cachedEntry && (Date.now() - cachedEntry.timestamp < cacheTtl)) {
        return cachedEntry.price;
    }
    try {
        const price = await fetchSolUsdPriceFromAPI();
        solPriceCache.set(SOL_PRICE_CACHE_KEY, { price, timestamp: Date.now() });
        console.log(`${logPrefix} Fetched and cached new SOL/USD price: $${price}`);
        return price;
    } catch (error) {
        if (cachedEntry) {
            console.warn(`${logPrefix} ⚠️ API fetch failed ('${error.message}'), using stale cached SOL/USD price: $${cachedEntry.price}`);
            return cachedEntry.price;
        }
        const criticalErrorMessage = `🚨 *CRITICAL PRICE FEED FAILURE* (${escapeMarkdownV2(BOT_NAME)}) 🚨\n\nUnable to fetch SOL/USD price and no cache available\\. USD conversions will be severely impacted\\.\n*Error:* \`${escapeMarkdownV2(error.message)}\``;
        console.error(`${logPrefix} ❌ CRITICAL: ${criticalErrorMessage.replace(/\n/g, ' ')}`);
        if (typeof notifyAdmin === 'function') { 
            await notifyAdmin(criticalErrorMessage);
        }
        throw new Error(`Critical: Could not retrieve SOL/USD price. Error: ${error.message}`);
    }
}
console.log("[PriceFeed Utils] getSolUsdPrice and fetchSolUsdPriceFromAPI defined.");

function convertLamportsToUSDString(lamports, solUsdPrice, displayDecimals = 2) {
    if (typeof solUsdPrice !== 'number' || solUsdPrice <= 0) {
        console.warn(`[Convert] Invalid solUsdPrice (${solUsdPrice}) for lamports to USD conversion. Lamports: ${lamports}`);
        return '⚠️ Price N/A';
    }
    if (typeof lamports !== 'bigint') {
        try { lamports = BigInt(lamports); }
        catch (e) { 
            console.warn(`[Convert] Invalid lamport amount for USD conversion: ${lamports}`);
            return '⚠️ Amount Error'; 
        } 
    }
    const solAmount = Number(lamports) / Number(LAMPORTS_PER_SOL);
    const usdValue = solAmount * solUsdPrice;
    return `$${usdValue.toLocaleString('en-US', { minimumFractionDigits: displayDecimals, maximumFractionDigits: displayDecimals })}`;
}
console.log("[PriceFeed Utils] convertLamportsToUSDString defined.");

function convertUSDToLamports(usdAmount, solUsdPrice) {
    if (typeof solUsdPrice !== 'number' || solUsdPrice <= 0) {
        throw new Error("SOL/USD price must be a positive number for USD to Lamports conversion.");
    }
    const parsedUsdAmount = parseFloat(String(usdAmount).replace(/[^0-9.-]+/g,""));
    if (isNaN(parsedUsdAmount) || parsedUsdAmount <= 0) {
        throw new Error("Invalid or non-positive USD amount for conversion.");
    }
    const solAmount = parsedUsdAmount / solUsdPrice;
    return BigInt(Math.floor(solAmount * Number(LAMPORTS_PER_SOL)));
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

const SLOT_PAYOUTS = {
    64: { multiplier: 100, symbols: "💎💎💎", label: "MEGA JACKPOT!" },
    1:  { multiplier: 20,  symbols: "7️⃣7️⃣7️⃣", label: "TRIPLE SEVEN!" },
    22: { multiplier: 10,  symbols: "🍋🍋🍋", label: "Triple Lemon!" },
    43: { multiplier: 5,   symbols: "🔔🔔🔔", label: "Triple Bell!" },
};
const SLOT_DEFAULT_LOSS_MULTIPLIER = -1; 

console.log("Part 1: Core Imports, Basic Setup, Global State & Utilities (Enhanced & Integrated with Payment System & Price Feed) - Complete.");
// --- End of Part 1 ---
// --- Start of Part 2 ---
// index.js - Part 2: Database Schema Initialization & Core User Management (Integrated)
//---------------------------------------------------------------------------
console.log("Loading Part 2: Database Schema Initialization & Core User Management (Integrated)...");

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
        console.log("  [DB Schema] 'users' table checked/created.");

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
        console.log("  [DB Schema] 'jackpots' table checked/created.");
        await client.query(
            `INSERT INTO jackpots (jackpot_id, current_amount) VALUES ($1, 0) ON CONFLICT (jackpot_id) DO NOTHING;`,
            [MAIN_JACKPOT_ID] // MAIN_JACKPOT_ID from Part 1
        );

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
        console.log("  [DB Schema] 'games' table (game log) checked/created.");

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
                balance_at_sweep BIGINT 
            );
            CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_user_id ON user_deposit_wallets(user_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_public_key ON user_deposit_wallets(public_key);
            CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_is_active_expires_at ON user_deposit_wallets(is_active, expires_at);
        `);
        console.log("  [DB Schema] 'user_deposit_wallets' table checked/created.");

        // Deposits Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposits (
                deposit_id SERIAL PRIMARY KEY,
                user_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                user_deposit_wallet_id INT REFERENCES user_deposit_wallets(wallet_id) ON DELETE SET NULL, -- Link to the specific user_deposit_wallets entry
                transaction_signature VARCHAR(88) NOT NULL UNIQUE, 
                source_address VARCHAR(44), 
                deposit_address VARCHAR(44) NOT NULL, -- The bot's address that received funds (public_key from user_deposit_wallets)
                amount_lamports BIGINT NOT NULL,
                confirmation_status VARCHAR(20) DEFAULT 'pending', 
                block_time BIGINT, 
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMPTZ, 
                notes TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits(user_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_deposits_transaction_signature ON deposits(transaction_signature);
            CREATE INDEX IF NOT EXISTS idx_deposits_deposit_address ON deposits(deposit_address);
            CREATE INDEX IF NOT EXISTS idx_deposits_status_created_at ON deposits(confirmation_status, created_at);
        `);
        console.log("  [DB Schema] 'deposits' table checked/created.");

        // Withdrawals Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS withdrawals (
                withdrawal_id SERIAL PRIMARY KEY,
                user_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                destination_address VARCHAR(44) NOT NULL, 
                amount_lamports BIGINT NOT NULL,
                fee_lamports BIGINT NOT NULL,
                transaction_signature VARCHAR(88) UNIQUE, 
                status VARCHAR(20) DEFAULT 'pending', 
                error_message TEXT,
                requested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMPTZ, 
                block_time BIGINT,
                priority_fee_microlamports INT,
                compute_unit_price_microlamports INT, 
                compute_unit_limit INT 
            );
            CREATE INDEX IF NOT EXISTS idx_withdrawals_user_id ON withdrawals(user_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_withdrawals_status_requested_at ON withdrawals(status, requested_at);
        `);
        console.log("  [DB Schema] 'withdrawals' table checked/created.");

        // Referrals Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS referrals (
                referral_id SERIAL PRIMARY KEY,
                referrer_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                referred_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE UNIQUE,
                commission_type VARCHAR(20), 
                commission_amount_lamports BIGINT, 
                transaction_signature VARCHAR(88), 
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_referral_pair UNIQUE (referrer_telegram_id, referred_telegram_id)
            );
            CREATE INDEX IF NOT EXISTS idx_referrals_referrer_id ON referrals(referrer_telegram_id);
        `);
        console.log("  [DB Schema] 'referrals' table checked/created.");

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
        console.log("  [DB Schema] 'processed_sweeps' table checked/created.");
        
        // Ledger Table (NEW - for comprehensive transaction tracking)
        await client.query(`
            CREATE TABLE IF NOT EXISTS ledger (
                ledger_id SERIAL PRIMARY KEY,
                user_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                transaction_type VARCHAR(50) NOT NULL, -- e.g., 'deposit', 'withdrawal', 'bet_win', 'bet_loss', 'referral_bonus', 'jackpot_win', 'admin_grant'
                amount_lamports BIGINT NOT NULL, -- Positive for credit, negative for debit to user balance
                balance_before_lamports BIGINT NOT NULL,
                balance_after_lamports BIGINT NOT NULL,
                deposit_id INTEGER REFERENCES deposits(deposit_id) ON DELETE SET NULL,
                withdrawal_id INTEGER REFERENCES withdrawals(withdrawal_id) ON DELETE SET NULL,
                game_log_id INTEGER REFERENCES games(game_log_id) ON DELETE SET NULL,
                referral_id INTEGER REFERENCES referrals(referral_id) ON DELETE SET NULL, -- If directly related to a referral record
                related_sweep_id INTEGER REFERENCES processed_sweeps(sweep_id) ON DELETE SET NULL,
                notes TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_ledger_user_id ON ledger(user_telegram_id);
            CREATE INDEX IF NOT EXISTS idx_ledger_transaction_type ON ledger(transaction_type);
            CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON ledger(created_at);
        `);
        console.log("  [DB Schema] 'ledger' table (for financial tracking) checked/created.");


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
        const tablesWithUpdatedAt = ['users', 'jackpots'];
        for (const tableName of tablesWithUpdatedAt) {
            await client.query(`DROP TRIGGER IF EXISTS set_timestamp ON ${tableName};`).catch(err => console.warn(`  [DB Schema] Failed to drop existing trigger for ${tableName}: ${err.message}. Proceeding.`));
            await client.query(`
                CREATE TRIGGER set_timestamp
                BEFORE UPDATE ON ${tableName}
                FOR EACH ROW
                EXECUTE FUNCTION trigger_set_timestamp();
            `).catch(err => console.warn(`  [DB Schema] Could not set update trigger for ${tableName}: ${err.message}. It might already exist correctly.`));
        }
        console.log("  [DB Schema] 'updated_at' trigger function and assignments checked/created.");

        await client.query('COMMIT');
        console.log("✅ Database schema initialization complete.");
    } catch (e) {
        await client.query('ROLLBACK');
        console.error('❌ Error during database schema initialization:', e);
        throw e; 
    } finally {
        client.release();
    }
}

//---------------------------------------------------------------------------
// Core User Management Functions (Integrated)
//---------------------------------------------------------------------------

async function getOrCreateUser(telegramId, username = '', firstName = '', lastName = '', referrerId = null) {
    const LOG_PREFIX_GOCU = `[getOrCreateUser TG:${telegramId}]`;
    console.log(`${LOG_PREFIX_GOCU} Attempting to get or create user. Username: ${username || 'N/A'}, Name: ${firstName || 'N/A'}`);
    const client = await pool.connect();
    try {
        await client.query('BEGIN'); // Start transaction

        let result = await client.query('SELECT * FROM users WHERE telegram_id = $1', [telegramId]);
        if (result.rows.length > 0) {
            const user = result.rows[0];
            console.log(`${LOG_PREFIX_GOCU} User found. Balance: ${user.balance} lamports.`);
            
            // Check if user details need updating
            let detailsChanged = false;
            if ((username && user.username !== username) || 
                (firstName && user.first_name !== firstName) ||
                (lastName && user.last_name !== lastName && lastName !== null) ) { // only update if new last_name is not null
                detailsChanged = true;
            }

            if (detailsChanged) {
                 await client.query(
                    'UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP, username = $2, first_name = $3, last_name = $4, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $1',
                    [telegramId, username || user.username, firstName || user.first_name, lastName !== null ? (lastName || user.last_name) : user.last_name]
                 );
                 console.log(`${LOG_PREFIX_GOCU} User details updated.`);
            } else {
                // Only update last_active_timestamp if other details haven't changed
                await client.query('UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP WHERE telegram_id = $1', [telegramId]);
            }
            await client.query('COMMIT');
            // Return the potentially updated user object
            return { 
                ...user, 
                username: username || user.username, 
                first_name: firstName || user.first_name, 
                last_name: lastName !== null ? (lastName || user.last_name) : user.last_name 
            };
        } else {
            console.log(`${LOG_PREFIX_GOCU} User not found. Creating new user.`);
            const newReferralCode = generateReferralCode();
            const insertQuery = `
                INSERT INTO users (telegram_id, username, first_name, last_name, balance, referral_code, referrer_telegram_id, last_active_timestamp, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING *;
            `;
            const values = [telegramId, username, firstName, lastName, DEFAULT_STARTING_BALANCE_LAMPORTS.toString(), newReferralCode, referrerId];
            result = await client.query(insertQuery, values);
            const newUser = result.rows[0];
            console.log(`${LOG_PREFIX_GOCU} New user created with ID ${newUser.telegram_id}, Balance: ${newUser.balance} lamports, Referral Code: ${newUser.referral_code}.`);

            if (referrerId) {
                console.log(`${LOG_PREFIX_GOCU} User was referred by ${referrerId}. Recording referral link.`);
                try {
                    await client.query(
                        `INSERT INTO referrals (referrer_telegram_id, referred_telegram_id, created_at) VALUES ($1, $2, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING`,
                        [referrerId, newUser.telegram_id]
                    );
                    console.log(`${LOG_PREFIX_GOCU} Referral link recorded for ${referrerId} -> ${newUser.telegram_id}.`);
                } catch (referralError) {
                   console.error(`${LOG_PREFIX_GOCU} Failed to record referral for ${referrerId} -> ${newUser.telegram_id}:`, referralError);
                }
            }
            await client.query('COMMIT');
            return newUser;
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_GOCU} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_GOCU} Error in getOrCreateUser for telegramId ${telegramId}:`, stringifyWithBigInt(error));
        return null;
    } finally {
        client.release();
    }
}

async function updateUserActivity(telegramId) {
    try {
        await pool.query('UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $1', [telegramId]);
    } catch (error) {
        console.error(`[updateUserActivity TG:${telegramId}] Error updating last active timestamp for telegramId ${telegramId}:`, error);
    }
}

async function getUserBalance(telegramId) {
    try {
        const result = await pool.query('SELECT balance FROM users WHERE telegram_id = $1', [telegramId]);
        if (result.rows.length > 0) {
            return BigInt(result.rows[0].balance); 
        }
        console.warn(`[getUserBalance TG:${telegramId}] User not found, cannot retrieve balance.`);
        return null; 
    } catch (error) {
        console.error(`[getUserBalance TG:${telegramId}] Error retrieving balance for telegramId ${telegramId}:`, error);
        return null;
    }
}

async function updateUserBalance(telegramId, newBalanceLamports, client = pool) {
    const LOG_PREFIX_UUB = `[updateUserBalance TG:${telegramId}]`;
    try {
        if (typeof newBalanceLamports !== 'bigint') {
            console.error(`${LOG_PREFIX_UUB} Invalid newBalanceLamports type: ${typeof newBalanceLamports}. Must be BigInt.`);
            return false;
        }
        // This function is a simple balance setter now. 
        // updateUserBalanceAndLedger should be used for all actual debits/credits.
        // We might still allow direct setting if necessary, but it bypasses ledger.
        // For safety, let's ensure balance does not go negative if this is used directly.
        if (newBalanceLamports < 0n) {
            console.warn(`${LOG_PREFIX_UUB} Attempt to set negative balance (${newBalanceLamports}). Clamping to 0. This should ideally be handled by updateUserBalanceAndLedger.`);
            // newBalanceLamports = 0n; // Or reject depending on rules. Forcing this will be logged.
        }

        const result = await client.query(
            'UPDATE users SET balance = $1, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $2',
            [newBalanceLamports.toString(), telegramId] 
        );
        if (result.rowCount > 0) {
            console.log(`${LOG_PREFIX_UUB} Balance directly set to ${newBalanceLamports} lamports. Note: Ledger not updated by this specific function.`);
            return true;
        } else {
            console.warn(`${LOG_PREFIX_UUB} User not found or balance not updated for telegramId ${telegramId}.`);
            return false;
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_UUB} Error updating balance for telegramId ${telegramId} to ${newBalanceLamports}:`, error);
        return false;
    }
}


async function linkUserWallet(telegramId, solanaAddress) {
    const LOG_PREFIX_LUW = `[linkUserWallet TG:${telegramId}]`;
    console.log(`${LOG_PREFIX_LUW} Attempting to link wallet ${solanaAddress}.`);
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        if (!solanaAddress || !/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(solanaAddress)) {
            console.warn(`${LOG_PREFIX_LUW} Invalid Solana address format provided: ${solanaAddress}`);
            await client.query('ROLLBACK');
            return { success: false, error: "Invalid Solana address format. Please provide a valid Base58 encoded address." };
        }

        const existingLink = await client.query('SELECT telegram_id FROM users WHERE solana_wallet_address = $1 AND telegram_id != $2', [solanaAddress, telegramId]);
        if (existingLink.rows.length > 0) {
            const linkedToExistingUserId = existingLink.rows[0].telegram_id;
            console.warn(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} is already linked to another user ID ${linkedToExistingUserId}.`);
            await client.query('ROLLBACK');
            return { success: false, error: `This wallet address is already associated with another player (ID ending ${String(linkedToExistingUserId).slice(-4)}). Please use a different address.` };
        }

        const result = await client.query(
            'UPDATE users SET solana_wallet_address = $1, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $2 RETURNING solana_wallet_address',
            [solanaAddress, telegramId]
        );

        if (result.rowCount > 0) {
            await client.query('COMMIT');
            console.log(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} successfully linked in DB.`);
            walletCache.set(String(telegramId), { solanaAddress }); // Update cache from Part 1
            return { success: true, message: `Your Solana wallet \`${solanaAddress}\` has been successfully linked!` };
        } else {
            const currentUserState = await client.query('SELECT solana_wallet_address FROM users WHERE telegram_id = $1', [telegramId]);
             await client.query('ROLLBACK'); // Rollback as no update happened or user not found
            if (currentUserState.rowCount === 0) {
                console.error(`${LOG_PREFIX_LUW} User ${telegramId} not found. Cannot link wallet.`);
                return { success: false, error: "Your player profile was not found. Please try /start again." };
            }
            if (currentUserState.rows[0].solana_wallet_address === solanaAddress) {
                console.log(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} was already linked to this user. No change.`);
                walletCache.set(String(telegramId), { solanaAddress }); 
                return { success: true, message: `Your wallet \`${solanaAddress}\` was already linked to your account.` };
            }
            console.warn(`${LOG_PREFIX_LUW} User ${telegramId} found, but wallet not updated (rowCount: ${result.rowCount}). This might be an unexpected issue.`);
            return { success: false, error: "Failed to update wallet in the database due to an unknown reason. Please try again." };
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_LUW} Rollback error: ${rbErr.message}`));
        if (error.code === '23505') { // Unique constraint violation (solana_wallet_address)
            console.warn(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} is already linked (unique constraint). Verifying owner.`);
            // This state implies the earlier check failed or there was a race condition, though less likely with FOR UPDATE if that was used.
            // The earlier check for `telegram_id != $2` should catch this for *other* users.
            // If it's for the *same* user, the update logic should handle it.
            return { success: false, error: "This wallet address is already in use. Please choose a different one." };
        }
        console.error(`${LOG_PREFIX_LUW} Error linking wallet ${solanaAddress}:`, error);
        return { success: false, error: error.message || "An unexpected server error occurred while linking your wallet. Please try again." };
    } finally {
        client.release();
    }
}

async function getUserLinkedWallet(telegramId) {
    const cachedData = walletCache.get(String(telegramId));
    if (cachedData && cachedData.solanaAddress) {
        return cachedData.solanaAddress;
    }

    try {
        const result = await pool.query('SELECT solana_wallet_address FROM users WHERE telegram_id = $1', [telegramId]);
        if (result.rows.length > 0 && result.rows[0].solana_wallet_address) {
            walletCache.set(String(telegramId), { solanaAddress: result.rows[0].solana_wallet_address }); 
            return result.rows[0].solana_wallet_address;
        }
        return null;
    } catch (error) {
        console.error(`[getUserLinkedWallet TG:${telegramId}] Error getting linked wallet:`, error);
        return null;
    }
}

/**
 * Gets the next available address_index for a user's deposit address derivation.
 * The derivation path is m/44'/501'/USER_ACCOUNT'/0'/ADDRESS_INDEX'
 * This function finds the highest ADDRESS_INDEX used so far for the user and returns next.
 * @param {string|number} userId The user's Telegram ID.
 * @param {import('pg').PoolClient} [client=pool] Optional database client for transactions.
 * @returns {Promise<number>} The next address_index (e.g., 0 if none exist).
 */
async function getNextAddressIndexForUserDB(userId, client = pool) {
    const LOG_PREFIX_GNAI = `[getNextAddressIndexForUser TG:${userId}]`;
    try {
        const query = `
            SELECT derivation_path
            FROM user_deposit_wallets
            WHERE user_telegram_id = $1
            ORDER BY created_at DESC; 
        `;
        const res = await queryDatabase(query, [userId], client); // queryDatabase from Part 1
        let maxIndex = -1;

        if (res.rows.length > 0) {
            for (const row of res.rows) {
                const path = row.derivation_path;
                // Example path: m/44'/501'/12345'/0'/0'
                const parts = path.split('/');
                if (parts.length >= 6) { // Ensure path is long enough
                    const lastPart = parts[parts.length - 1]; // Should be "addressIndex'"
                    if (lastPart.endsWith("'")) {
                        const indexStr = lastPart.substring(0, lastPart.length - 1);
                        const currentIndex = parseInt(indexStr, 10);
                        if (!isNaN(currentIndex) && currentIndex > maxIndex) {
                            maxIndex = currentIndex;
                        }
                    } else {
                        console.warn(`${LOG_PREFIX_GNAI} Malformed last part of derivation path for user ${userId}: ${lastPart} in ${path}`);
                    }
                } else {
                     console.warn(`${LOG_PREFIX_GNAI} Malformed derivation path for user ${userId}: ${path}`);
                }
            }
        }
        const nextIndex = maxIndex + 1;
        console.log(`${LOG_PREFIX_GNAI} Determined next addressIndex for user ${userId}: ${nextIndex}`);
        return nextIndex;
    } catch (error) {
        console.error(`${LOG_PREFIX_GNAI} Error calculating next address index for user ${userId}: ${error.message}`, error.stack);
        throw error; 
    }
}
console.log("[User Management] getNextAddressIndexForUserDB helper function defined.");


async function deleteUserAccount(telegramId) {
    const LOG_PREFIX_DUA = `[deleteUserAccount TG:${telegramId}]`;
    console.warn(`${LOG_PREFIX_DUA} Attempting to delete user account and associated data for Telegram ID: ${telegramId}.`);
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        console.log(`${LOG_PREFIX_DUA} Anonymizing references in 'jackpots' table...`);
        await client.query('UPDATE jackpots SET last_won_by_telegram_id = NULL WHERE last_won_by_telegram_id = $1', [telegramId]);
        
        console.log(`${LOG_PREFIX_DUA} Anonymizing references in 'games' (game log) table...`);
        await client.query('UPDATE games SET initiator_telegram_id = NULL WHERE initiator_telegram_id = $1', [telegramId]);
        // Note: participants_ids array cleaning is complex and might be skipped for simplicity or handled differently based on GDPR.
        // For now, only initiator_telegram_id is nullified.

        // Financial records in 'deposits', 'withdrawals', 'ledger' are kept for audit but user_telegram_id will be affected by ON DELETE CASCADE/SET NULL.
        // 'user_deposit_wallets' will be CASCADE deleted.
        // 'referrals' where this user is referrer or referred will be CASCADE deleted.
        // This is a strong deletion policy. Ensure this aligns with legal/operational requirements.
        console.log(`${LOG_PREFIX_DUA} Preparing to delete user from 'users' table, which will cascade to related financial records as per schema foreign key constraints.`);
        
        const result = await client.query('DELETE FROM users WHERE telegram_id = $1', [telegramId]);

        await client.query('COMMIT');

        if (result.rowCount > 0) {
            console.log(`${LOG_PREFIX_DUA} User account ${telegramId} and associated data deleted successfully from database.`);
            // Clear in-memory caches associated with the user
            activeGames.forEach((game, gameId) => { 
                if (game.players && game.players.has(telegramId)) game.players.delete(telegramId);
                if (game.creatorId === telegramId) activeGames.delete(gameId);
                if (game.userId === telegramId) activeGames.delete(gameId); // For single player games
            });
            userCooldowns.delete(telegramId);
            groupGameSessions.forEach((session, chatId) => {
                if (session.players && session.players[telegramId]) delete session.players[telegramId];
                if (session.initiator === telegramId) groupGameSessions.delete(chatId);
            });
            walletCache.delete(String(telegramId));
            activeDepositAddresses.forEach((value, key) => { // activeDepositAddresses maps depositAddr -> {userId, expiresAt}
                if (value.userId === String(telegramId)) {
                    activeDepositAddresses.delete(key);
                }
            });
            userStateCache.delete(String(telegramId));
            console.log(`${LOG_PREFIX_DUA} Relevant in-memory caches cleared for user ${telegramId}.`);
            return true;
        } else {
            console.log(`${LOG_PREFIX_DUA} User ${telegramId} not found, no account deleted.`);
            return false;
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_DUA} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_DUA} Error deleting user account ${telegramId}:`, error);
        return false;
    } finally {
        client.release();
    }
}


console.log("Part 2: Database Schema Initialization & Core User Management (Integrated) - Complete.");
// --- End of Part 2 ---
// --- Start of Part 3 ---
// index.js - Part 3: Telegram Helpers, Currency Formatting & Basic Game Utilities (Integrated)
//---------------------------------------------------------------------------
console.log("Loading Part 3: Telegram Helpers, Currency Formatting & Basic Game Utilities (Integrated)...");

// --- Telegram Specific Helper Functions ---

// Gets a display name from a user object (msg.from or a fetched user object) and escapes it for MarkdownV2
function getEscapedUserDisplayName(userObject) {
  if (!userObject) return escapeMarkdownV2("Valued Player"); // escapeMarkdownV2 from Part 1

  const firstName = userObject.first_name || userObject.firstName;
  const username = userObject.username;
  // Use telegram_id from our DB user object (from getOrCreateUser) or id from Telegram's user object
  const id = userObject.id || userObject.telegram_id; 

  // Prioritize first_name, then username, then a generic player ID.
  let name = "Player"; // Default
  if (firstName) {
    name = firstName;
  } else if (username) {
    name = `@${username}`; // Include @ for username if no first name
  } else if (id) {
    name = `Player ${String(id).slice(-4)}`;
  } else {
    name = "Valued Player"; // Fallback if no identifiable info
  }
  return escapeMarkdownV2(name);
}

// Creates a MarkdownV2 mention link for a user object
function createUserMention(userObject) {
  if (!userObject) return escapeMarkdownV2("Esteemed Guest"); // escapeMarkdownV2 from Part 1

  const id = userObject.id || userObject.telegram_id; 
  if (!id) return escapeMarkdownV2("Unknown Player");

  // Use a simpler name for the mention text part to keep it concise.
  const simpleName = userObject.first_name || userObject.firstName || userObject.username || `Player ${String(id).slice(-4)}`;
  return `[${escapeMarkdownV2(simpleName)}](tg://user?id=${id})`;
}

// Gets a player's display reference, preferring @username, falls back to name. Escapes for MarkdownV2.
function getPlayerDisplayReference(userObject, preferUsernameTag = true) { // Changed default to prefer username tag
  if (!userObject) return escapeMarkdownV2("Mystery Player"); 

  const username = userObject.username;
  if (preferUsernameTag && username) {
    return `@${escapeMarkdownV2(username)}`; // Usernames don't need the same level of complex escaping for the @ part, but content after @ does.
                                         // However, if it's part of a larger MarkdownV2 string, the @ itself might need care.
                                         // For simplicity, escape the username part.
  }
  // Fallback to the more detailed display name if no username or if not preferred.
  return getEscapedUserDisplayName(userObject);
}
console.log("[Telegram Utils] User display helper functions defined.");

// --- General Utility Functions ---

/**
 * Formats a BigInt lamports amount into a SOL string representation or raw lamports.
 * @param {bigint|string|number} amountLamports - The amount in lamports.
 * @param {string} [currencyName='SOL'] - The currency to display (primarily 'SOL' or 'lamports').
 * @param {boolean} [displayRawLamportsOverride=false] - If true, forces display of raw lamports regardless of currencyName.
 * @param {number} [solDecimals=SOL_DECIMALS] - Number of decimal places for SOL (SOL_DECIMALS from Part 1).
 * @returns {string} Formatted currency string.
 */
function formatCurrency(amountLamports, currencyName = 'SOL', displayRawLamportsOverride = false, solDecimals = SOL_DECIMALS) { // SOL_DECIMALS from Part 1
    if (typeof amountLamports !== 'bigint') {
        try {
            amountLamports = BigInt(amountLamports);
        } catch (e) {
            console.warn(`[formatCurrency] Received non-BigInt convertible amount: ${amountLamports}.`);
            return '⚠️ Amount N/A'; // More user-friendly error
        }
    }

    if (displayRawLamportsOverride || String(currencyName).toLowerCase() === 'lamports') {
        return `${amountLamports.toLocaleString('en-US')} lamports`; // Use en-US for consistent lamport formatting
    }

    // Default to SOL formatting
    if (typeof LAMPORTS_PER_SOL === 'undefined') { // LAMPORTS_PER_SOL from Part 1
        console.error("[formatCurrency] LAMPORTS_PER_SOL is not defined. Cannot format SOL.");
        return `${amountLamports.toLocaleString('en-US')} lamports (⚠️ SOL Config Error)`; // More user-friendly
    }

    const solValue = Number(amountLamports) / Number(LAMPORTS_PER_SOL);

    let effectiveDecimals = solDecimals;
    // Show fewer decimals if it's a whole number or has fewer significant decimal places
    if (solValue === Math.floor(solValue)) { 
        effectiveDecimals = 0;
    } else {
        // Attempt to show natural decimal places up to solDecimals
        const stringValue = solValue.toString();
        const decimalPart = stringValue.split('.')[1];
        if (decimalPart) {
            effectiveDecimals = Math.min(decimalPart.length, solDecimals);
        } else {
            effectiveDecimals = 0; // Should be caught by solValue === Math.floor(solValue)
        }
    }
    // Ensure at least 2 decimal places if there are any, unless it's a whole number and solDecimals is high.
    if (effectiveDecimals > 0 && effectiveDecimals < 2 && solDecimals >=2) effectiveDecimals = 2;


    try {
        return `${solValue.toLocaleString('en-US', { // Use en-US for consistent SOL formatting
            minimumFractionDigits: 0, // Show 0 for whole numbers if effectiveDecimals is 0
            maximumFractionDigits: effectiveDecimals
        })} SOL`;
    } catch (e) {
        console.error(`[formatCurrency] Error formatting SOL for ${amountLamports} lamports: ${e.message}`);
        return `${amountLamports.toLocaleString('en-US')} lamports (⚠️ Format Error)`; // More user-friendly
    }
}
console.log("[Currency Utils] formatCurrency helper function defined.");


/**
 * Formats a BigInt lamports amount for display, defaulting to USD, with fallbacks.
 * Assumes getSolUsdPrice() and convertLamportsToUSDString() are globally available (defined in Part 1).
 * @param {bigint|string|number} lamports - The amount in lamports.
 * @param {string} [targetCurrency='USD'] - The target currency ('USD', 'SOL', or 'lamports').
 * @returns {Promise<string>} Formatted currency string.
 */
async function formatBalanceForDisplay(lamports, targetCurrency = 'USD') {
    if (typeof lamports !== 'bigint') {
        try {
            lamports = BigInt(lamports);
        } catch (e) {
            console.warn(`[formatBalanceForDisplay] Invalid lamport amount: ${lamports}.`);
            return '⚠️ Amount N/A'; // User-friendly error
        }
    }

    const upperTargetCurrency = String(targetCurrency).toUpperCase();

    if (upperTargetCurrency === 'USD') {
        try {
            if (typeof getSolUsdPrice !== 'function' || typeof convertLamportsToUSDString !== 'function') {
                console.error("[formatBalanceForDisplay] Price conversion functions (getSolUsdPrice or convertLamportsToUSDString) are not available. Falling back to SOL display.");
                return formatCurrency(lamports, 'SOL'); 
            }
            const price = await getSolUsdPrice(); // getSolUsdPrice from Part 1
            return convertLamportsToUSDString(lamports, price); // convertLamportsToUSDString from Part 1
        } catch (e) {
            console.error(`[formatBalanceForDisplay] Failed to get SOL/USD price for USD display: ${e.message}. Falling back to SOL display.`);
            return formatCurrency(lamports, 'SOL'); 
        }
    } else if (upperTargetCurrency === 'LAMPORTS') {
        return formatCurrency(lamports, 'lamports', true); 
    }
    // Default to SOL using the original function
    return formatCurrency(lamports, 'SOL');
}
console.log("[Currency Utils] formatBalanceForDisplay helper function defined.");


// Generates a unique-ish ID for game instances
function generateGameId(prefix = "game") {
  const timestamp = Date.now().toString(36);
  const randomSuffix = Math.random().toString(36).substring(2, 10); // Longer suffix for better uniqueness
  return `${prefix}_${timestamp}_${randomSuffix}`;
}
console.log("[Game Utils] generateGameId helper function defined.");

// --- Dice Display Utilities ---

// Formats an array of dice roll numbers into a string with emoji and number
function formatDiceRolls(rollsArray, diceEmoji = '🎲') {
  if (!Array.isArray(rollsArray) || rollsArray.length === 0) return '';
  // Ensure all rolls are numbers, default to '?' if not
  const diceVisuals = rollsArray.map(roll => {
      const rollValue = Number(roll);
      return `${diceEmoji} ${isNaN(rollValue) ? '?' : rollValue}`;
  });
  return diceVisuals.join('   '); // Use three spaces for clearer visual separation
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
 * @param {'deposit' | 'withdrawal' | 'sweep' | 'referral' | 'bet' | 'win' | 'refund' | 'ledger_adjustment'} type - The type of payment/ledger entry.
 * @param {string} [userId='system'] - Optional user ID if related to a specific user.
 * @returns {string} A unique-ish transaction ID.
 */
function generateInternalPaymentTxId(type, userId = 'system') {
    const now = Date.now().toString(36); // Timestamp part
    // crypto should be imported in Part 1 (import * as crypto from 'crypto';)
    let randomPart;
    if (typeof crypto !== 'undefined' && typeof crypto.randomBytes === 'function') {
        randomPart = crypto.randomBytes(4).toString('hex'); // Increased randomness
    } else {
        console.warn('[generateInternalPaymentTxId] Crypto module not available for random part. Using Math.random, which is less secure for critical IDs.');
        randomPart = Math.random().toString(36).substring(2, 10); // Fallback
    }
    
    const userPartCleaned = String(userId).replace(/[^a-zA-Z0-9]/g, '').slice(-6) || 'sys'; // Sanitize and shorten user ID part
    let prefix = String(type).toLowerCase().substring(0, 4).replace(/[^a-z]/g, '') || 'gen'; // Sanitize prefix

    return `${prefix}_${userPartCleaned}_${now}_${randomPart}`;
}
console.log("[Payment Utils] generateInternalPaymentTxId helper function defined.");


console.log("Part 3: Telegram Helpers, Currency Formatting & Basic Game Utilities (Integrated) - Complete.");
// --- End of Part 3 ---
// --- Start of Part 4 ---
// index.js - Part 4: Simplified Game Logic (Enhanced)
//---------------------------------------------------------------------------
console.log("Loading Part 4: Simplified Game Logic (Enhanced)...");

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
  if (typeof rollDie !== 'function') { // rollDie from Part 3
     console.error("[determineDieRollOutcome] CRITICAL Error: rollDie function is not defined from Part 3.");
     // Fallback to a predictable, safe roll if rollDie is missing.
     return { roll: 1, emoji: '🎲' }; 
  }
  sides = Number.isInteger(sides) && sides > 1 ? sides : 6; 
  const roll = rollDie(sides); 

  // The emoji here is for potential direct use if not formatting via formatDiceRolls.
  // Display formatting (e.g., "🎲 5") is best handled by formatDiceRolls (Part 3).
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

// Determines the outcome of an RPS match given two choices (e.g., RPS_CHOICES.ROCK).
// Returns a detailed result object.
function determineRPSOutcome(player1ChoiceKey, player2ChoiceKey) {
  const LOG_PREFIX_RPS_OUTCOME = "[RPS_Outcome]";
  
  const p1c = String(player1ChoiceKey).toLowerCase();
  const p2c = String(player2ChoiceKey).toLowerCase();

  if (!Object.values(RPS_CHOICES).includes(p1c) || !Object.values(RPS_CHOICES).includes(p2c)) {
    console.warn(`${LOG_PREFIX_RPS_OUTCOME} Invalid choices: P1='${player1ChoiceKey}', P2='${player2ChoiceKey}'. This should be caught before calling.`);
    return { // This error message is for internal use or debugging, not typically shown directly to user.
        result: 'error',
        description: "An internal error occurred due to invalid RPS choices.", // Generic error
        player1: { choice: player1ChoiceKey, emoji: '❓' },
        player2: { choice: player2ChoiceKey, emoji: '❓' }
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
    resultDescription = `${p1Emoji} ${p1ChoiceFormatted} clashes with ${p2Emoji} ${p2ChoiceFormatted}\\! It's a *Draw*\\!`;
  } else if (RPS_RULES[p1c]?.beats === p2c) { // Player 1 wins
    outcome = 'win_player1';
    resultDescription = `${p1Emoji} ${p1ChoiceFormatted} *${RPS_RULES[p1c].verb}* ${p2Emoji} ${p2ChoiceFormatted}\\! Player 1 *claims victory*\\!`;
  } else { // Player 2 wins
    outcome = 'win_player2';
    resultDescription = `${p2Emoji} ${p2ChoiceFormatted} *${RPS_RULES[p2c]?.verb || 'outplays'}* ${p1Emoji} ${p1ChoiceFormatted}\\! Player 2 *is the winner*\\!`;
  }
  // The descriptions are constructed from safe components and literals that don't conflict with MarkdownV2.
  // Bolding and italics are added for emphasis here. The calling function will then use escapeMarkdownV2 on this entire string if needed,
  // but since we control all components, it's already safe for MarkdownV2.

  return {
    result: outcome,
    description: resultDescription, // A full sentence describing the outcome, styled for casino feel
    player1: { choice: p1c, emoji: p1Emoji, choiceFormatted: p1ChoiceFormatted },
    player2: { choice: p2c, emoji: p2Emoji, choiceFormatted: p2ChoiceFormatted }
  };
}
console.log("[Game Logic] RPS: determineRPSOutcome defined.");

console.log("Part 4: Simplified Game Logic (Enhanced) - Complete.");

// --- End of Part 4 ---
// --- Start of Part 5a, Section 1 ---
// index.js - Part 5a, Section 1: Core Listeners, General Command Handlers & Payment UI Integration
//---------------------------------------------------------------------------
console.log("Loading Part 5a, Section 1: Core Listeners, General Command Handlers & Payment UI Integration...");

// Game constants & configurations are from Part 1
// (MIN_BET_USD_val, MAX_BET_USD_val, COMMAND_COOLDOWN_MS, JOIN_GAME_TIMEOUT_MS, etc.)
// LAMPORTS_PER_SOL, formatCurrency, escapeMarkdownV2, getPlayerDisplayReference, etc. are available.

// --- Game Identifiers (used for callback data, rules, game logic routing) ---
const GAME_IDS = {
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

// --- CONSTANTS FOR SPECIFIC GAMES (Required by Rules Display and Game Handlers) ---
// Over/Under 7
const OU7_PAYOUT_NORMAL = 1; // 1:1 profit (total 2x bet returned)
const OU7_PAYOUT_SEVEN = 4;  // 4:1 profit (total 5x bet returned)
const OU7_DICE_COUNT = 2;

// High Roller Duel
const DUEL_DICE_COUNT = 2;

// Greed's Ladder
const LADDER_ROLL_COUNT = 3;
const LADDER_BUST_ON = 1; // Rolling a 1 busts
const LADDER_PAYOUTS = [ 
  { min: (LADDER_ROLL_COUNT * 5 + 1), max: (LADDER_ROLL_COUNT * 6), multiplier: 5, label: "🌟 Excellent Climb!" },
  { min: (LADDER_ROLL_COUNT * 4 + 1), max: (LADDER_ROLL_COUNT * 5), multiplier: 3, label: "🎉 Great Ascent!" },
  { min: (LADDER_ROLL_COUNT * 3 + 1), max: (LADDER_ROLL_COUNT * 4), multiplier: 1, label: "👍 Good Progress!" },
  // Score of (LADDER_ROLL_COUNT * 1) to (LADDER_ROLL_COUNT*1 + (LADDER_ROLL_COUNT-1))
  { min: (LADDER_ROLL_COUNT * 1), max: (LADDER_ROLL_COUNT * 3), multiplier: 0, label: "😐 Steady Steps (Push)" }, // Example for push on lower scores
];

// Slot Fruit Frenzy (SLOT_PAYOUTS and SLOT_DEFAULT_LOSS_MULTIPLIER are defined in Part 1)

// --- Main Message Handler (`bot.on('message')`) ---
bot.on('message', async (msg) => {
  const LOG_PREFIX_MSG = `[MSG_Handler TID:${msg.message_id}]`;

  if (isShuttingDown) { 
    console.log(`${LOG_PREFIX_MSG} Shutdown in progress. Ignoring message.`);
    return;
  }
  if (!msg || !msg.from || !msg.chat || !msg.date) {
    console.log(`${LOG_PREFIX_MSG} Ignoring malformed/incomplete message: ${stringifyWithBigInt(msg)}`);
    return;
  }

  // Ignore messages from other bots, but allow self-interaction if needed for some flows (currently not).
  if (msg.from.is_bot) {
    try {
      if (!bot || typeof bot.getMe !== 'function') return; // Bot not ready
      const selfBotInfo = await bot.getMe();
      if (String(msg.from.id) !== String(selfBotInfo.id)) {
        // console.log(`${LOG_PREFIX_MSG} Ignoring message from other bot ID ${msg.from.id}`);
        return; 
      }
      // If it's from self, potentially allow if it's part of a specific flow. For now, generally ignore.
      // console.log(`${LOG_PREFIX_MSG} Ignoring message from self (Bot ID ${msg.from.id}) unless specific flow handles it.`);
      // return; 
    } catch (getMeError) {
      console.error(`${LOG_PREFIX_MSG} Error in getMe self-check: ${getMeError.message}. Ignoring bot message.`);
      return;
    }
  }

  const userId = String(msg.from.id);
  const chatId = String(msg.chat.id); // This is the chat where the message was sent
  const text = msg.text || "";
  const chatType = msg.chat.type; // 'private', 'group', or 'supergroup'

  // Stateful input handling (from Part P3)
  if (userStateCache.has(userId) && !text.startsWith('/')) {
    const currentState = userStateCache.get(userId);
    if (typeof routeStatefulInput === 'function') { // routeStatefulInput from Part P3
      console.log(`${LOG_PREFIX_MSG} User ${userId} has active state: ${currentState.state || currentState.action}. Routing to stateful input handler.`);
      await routeStatefulInput(msg, currentState); // Pass the full msg object
      return;
    } else {
      console.warn(`${LOG_PREFIX_MSG} User ${userId} in state ${currentState.state || currentState.action}, but routeStatefulInput is not defined. Clearing state.`);
      if(typeof clearUserState === 'function') clearUserState(userId); else userStateCache.delete(userId);
    }
  }

  if (text.startsWith('/')) {
    let userForCommandProcessing;
    try {
      userForCommandProcessing = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name); // getOrCreateUser from Part 2
      if (!userForCommandProcessing) {
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
      // Optionally send a subtle hint or just ignore. For now, ignoring.
      return;
    }
    userCooldowns.set(userId, now);

    let fullCommand = text.substring(1);
    let commandName = fullCommand.split(/\s+/)[0]?.toLowerCase();
    const commandArgs = fullCommand.split(/\s+/).slice(1);
    const originalMessageId = msg.message_id; // For potential deletion or reply

    // Handle commands with @botusername
    if (commandName.includes('@')) {
        const selfBotInfo = await bot.getMe();
        const botUsernameLower = selfBotInfo.username.toLowerCase();
        if (commandName.endsWith(`@${botUsernameLower}`)) {
            commandName = commandName.substring(0, commandName.lastIndexOf(`@${botUsernameLower}`));
        } else {
            // Command is for a different bot in a group chat
            if (chatType === 'group' || chatType === 'supergroup') {
                console.log(`${LOG_PREFIX_MSG} Command /${commandName} in chat ${chatId} is for a different bot. Ignoring.`);
                return; 
            }
             // If in PM, or if bot should respond to any @mention, strip the @part.
            commandName = commandName.split('@')[0];
        }
    }
    
    console.log(`${LOG_PREFIX_MSG} CMD: /${commandName}, Args: [${commandArgs.join(', ')}] from User ${getPlayerDisplayReference(userForCommandProcessing)} (Chat: ${chatId}, Type: ${chatType})`);

    // Helper to parse bet amount for game commands (USD primary)
    const parseBetAmount = async (arg, commandInitiationChatId, commandInitiationChatType) => {
        let betAmountLamports;
        let minBetLamports, maxBetLamports;
        let minBetDisplay, maxBetDisplay;
        let defaultBetDisplay; // Will be min bet

        try {
            const solPrice = await getSolUsdPrice(); // From Part 1

            // Convert USD limits to lamports using current price
            minBetLamports = convertUSDToLamports(MIN_BET_USD_val, solPrice); // MIN_BET_USD_val from Part 1
            maxBetLamports = convertUSDToLamports(MAX_BET_USD_val, solPrice); // MAX_BET_USD_val from Part 1
            
            minBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(minBetLamports, solPrice));
            maxBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(maxBetLamports, solPrice));
            defaultBetDisplay = minBetDisplay;


            if (!arg) { // No bet amount provided, use minimum
                betAmountLamports = minBetLamports;
                // No message needed if defaulting, or a very subtle one if desired.
                // console.log(`${LOG_PREFIX_MSG} No bet arg provided, defaulting to min USD bet: ${defaultBetDisplay} (${betAmountLamports} lamports)`);
                return betAmountLamports;
            }

            // Try parsing as USD first
            const potentialUsdAmount = parseFloat(String(arg).replace(/[^0-9.]/g, ''));
            if (!isNaN(potentialUsdAmount) && potentialUsdAmount > 0) {
                 betAmountLamports = convertUSDToLamports(potentialUsdAmount, solPrice);
                 if (potentialUsdAmount < MIN_BET_USD_val || potentialUsdAmount > MAX_BET_USD_val) {
                    const message = `⚠️ Your bet of *${escapeMarkdownV2(potentialUsdAmount.toFixed(2))} USD* is outside the allowed limits: *${minBetDisplay}* \\- *${maxBetDisplay}*\\. Your bet has been adjusted to the minimum: *${defaultBetDisplay}*\\.`;
                    await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                    return minBetLamports;
                 }
                 console.log(`${LOG_PREFIX_MSG} Parsed bet: ${potentialUsdAmount} USD -> ${betAmountLamports} lamports`);
                 return betAmountLamports;
            } else {
                 // If USD parsing fails or is invalid, try to interpret as SOL or lamports as a fallback (less common for users)
                 // This part can be made more restrictive if only USD input is desired.
                 let parsedLamportsDirectly;
                 try {
                    if (String(arg).includes('.')) { // Potentially SOL with decimal
                        const solVal = parseFloat(arg);
                        if (!isNaN(solVal) && solVal > 0) {
                            parsedLamportsDirectly = BigInt(Math.floor(solVal * Number(LAMPORTS_PER_SOL)));
                        } else throw new Error("Invalid SOL float format.");
                    } else { // Lamports or SOL as integer
                        const intVal = BigInt(arg);
                         // Heuristic: if small number not ending in many zeros, might have been SOL intended as integer
                        if (intVal > 0 && intVal < 10000 && !String(arg).endsWith('00000')) {
                             parsedLamportsDirectly = BigInt(Math.floor(Number(intVal) * Number(LAMPORTS_PER_SOL)));
                             console.log(`${LOG_PREFIX_MSG} Interpreted bet "${arg}" as ${intVal} SOL -> ${parsedLamportsDirectly} lamports`);
                        } else {
                            parsedLamportsDirectly = intVal;
                        }
                    }

                    if (parsedLamportsDirectly <= 0n) throw new Error("Bet amount must be positive.");

                    // Check against the dynamic lamport limits derived from USD limits
                    if (parsedLamportsDirectly < minBetLamports || parsedLamportsDirectly > maxBetLamports) {
                        const betInLamportsDisplay = escapeMarkdownV2(formatCurrency(parsedLamportsDirectly, 'SOL')); // Show their attempt in SOL
                        const message = `⚠️ Your bet of *${betInLamportsDisplay}* is outside current USD limits (*${minBetDisplay}* \\- *${maxBetDisplay}*\\)\\. Your bet is set to the minimum: *${defaultBetDisplay}*\\.`;
                        await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                        return minBetLamports;
                    }
                    console.log(`${LOG_PREFIX_MSG} Parsed bet as lamports/SOL: ${parsedLamportsDirectly} lamports`);
                    return parsedLamportsDirectly;

                 } catch (directParseError) {
                    const message = `🤔 Hmmm, your bet amount \`${escapeMarkdownV2(String(arg))}\` seems a bit off\\. Please use USD (e\\.g\\., \`5\` or \`10.50\`)\\. Your bet is set to the minimum: *${defaultBetDisplay}*\\.`;
                    await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                    return minBetLamports;
                 }
            }
        } catch (priceError) {
            console.error(`${LOG_PREFIX_MSG} Critical error getting SOL price for bet parsing: ${priceError.message}`);
            const message = `⚙️ Apologies, we couldn't determine current bet limits due to a price feed issue\\. Using internal default lamport limits for now\\. Your bet has been set to the internal minimum of *${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT_LAMPORTS_config, 'SOL'))}*\\.`;
            await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
            // Fallback to fixed lamport limits if price feed fails
            // Ensure arg (if any) is within MIN_BET_AMOUNT_LAMPORTS_config and MAX_BET_AMOUNT_LAMPORTS_config
            try {
                betAmountLamports = BigInt(arg);
                if (betAmountLamports < MIN_BET_AMOUNT_LAMPORTS_config || betAmountLamports > MAX_BET_AMOUNT_LAMPORTS_config) {
                    return MIN_BET_AMOUNT_LAMPORTS_config;
                }
                return betAmountLamports;
            } catch {
                return MIN_BET_AMOUNT_LAMPORTS_config;
            }
        }
    };


    // Command routing
    switch (commandName) {
      // --- General Casino Commands ---
      case 'start': // Also often triggered by deep links
        // For /start in group, give generic welcome and DM link.
        // For /start in PM, show full help.
        // If args exist (e.g. from deep link like /start ref_XYZ or /start deposit), handle them.
        await handleStartCommand(msg, commandArgs); // Pass full msg and args
        break;
      case 'help':
        await handleHelpCommand(msg); // Pass full msg
        break;
      case 'balance':
      case 'bal':
        await handleBalanceCommand(msg); // Pass full msg
        break;
      case 'rules':
      case 'info':
        // Pass originalMessageId for potential edit in callback, or deletion if new message sent.
        await handleRulesCommand(msg.chat.id, userForCommandProcessing, originalMessageId, false, msg.chat.type);
        break;
      case 'jackpot':
        await handleJackpotCommand(msg.chat.id, userForCommandProcessing, msg.chat.type);
        break;

      // --- Payment System UI Commands (Most should redirect to PM if in group) ---
      case 'wallet': // Manages linked address, shows balance, deposit/withdraw options
        await handleWalletCommand(msg); // Pass full msg
        break;
      case 'deposit':
        await handleDepositCommand(msg); // Pass full msg
        break;
      case 'withdraw':
        await handleWithdrawCommand(msg); // Pass full msg
        break;
      case 'referral':
        await handleReferralCommand(msg); // Pass full msg
        break;
      case 'history':
        await handleHistoryCommand(msg); // Pass full msg
        break;
      case 'leaderboards': // Generally fine for groups
        await handleLeaderboardsCommand(msg, commandArgs); // Pass full msg and args
        break;
      case 'setwallet':
        // handleSetWalletCommand will manage group/privacy itself.
        await handleSetWalletCommand(msg, commandArgs); // Pass full msg and args
        break;

      // --- Admin Commands ---
      case 'grant':
        if (ADMIN_USER_ID && userId === ADMIN_USER_ID) { // ADMIN_USER_ID from Part 1
          if (commandArgs.length < 2) { // Expect /grant <target_user_id> <amount_lamports_or_SOL>
            await safeSendMessage(chatId, "⚙️ Admin Usage: `/grant <target_user_id> <amount_SOL_or_Lamports>`", { parse_mode: 'MarkdownV2' });
            break;
          }
          const targetUserIdForGrant = commandArgs[0];
          const amountArgForGrant = commandArgs[1];
          let amountToGrantLamports;

          try {
            // Try parsing as SOL first if it contains a decimal or is a small number
            if (amountArgForGrant.includes('.') || (BigInt(amountArgForGrant) > 0 && BigInt(amountArgForGrant) < 10000 && !amountArgForGrant.endsWith('00000'))) {
                const solAmount = parseFloat(amountArgForGrant);
                if (isNaN(solAmount) || solAmount <=0) throw new Error("Invalid SOL amount for grant.");
                amountToGrantLamports = BigInt(Math.floor(solAmount * Number(LAMPORTS_PER_SOL)));
            } else {
                amountToGrantLamports = BigInt(amountArgForGrant);
            }
            if (amountToGrantLamports <= 0n) throw new Error("Grant amount must be positive.");
          } catch (e) {
            await safeSendMessage(chatId, `⚠️ Invalid grant amount: \`${escapeMarkdownV2(amountArgForGrant)}\`. Please use SOL (e.g., \`0.5\`) or lamports.`, { parse_mode: 'MarkdownV2' });
            break;
          }
          
          let targetUser;
          try {
            targetUser = await getOrCreateUser(targetUserIdForGrant); // Ensures target user exists
            if (!targetUser) throw new Error(`Target user ID ${targetUserIdForGrant} could not be fetched or created.`);
          } catch (grantGetUserError) {
            console.error(`${LOG_PREFIX_MSG} Admin Grant: Error fetching target user ${targetUserIdForGrant}: ${grantGetUserError.message}`);
            await safeSendMessage(chatId, `⚠️ Could not find or create target user \`${escapeMarkdownV2(targetUserIdForGrant)}\` for grant.`, { parse_mode: 'MarkdownV2' });
            break;
          }
          
          let grantClient = null;
          try {
            grantClient = await pool.connect(); // pool from Part 1
            await grantClient.query('BEGIN');
            
            if (typeof updateUserBalanceAndLedger !== 'function') { // From Part P2
              console.error("FATAL: updateUserBalanceAndLedger function is not defined for grant command.");
              await safeSendMessage(chatId, "🛠️ Internal error: Grant functionality is currently unavailable.", { parse_mode: 'MarkdownV2' });
              await grantClient.query('ROLLBACK');
              break;
            }
            const grantNotes = `Admin grant by ${userId} to ${targetUser.telegram_id}. Amount: ${formatCurrency(amountToGrantLamports, 'SOL')}`;
            const grantResult = await updateUserBalanceAndLedger(
              grantClient, targetUser.telegram_id, amountToGrantLamports, 'admin_grant', {}, grantNotes
            );
            
            if (grantResult.success) {
              await grantClient.query('COMMIT');
              await safeSendMessage(chatId, `✅ Successfully granted *${escapeMarkdownV2(formatCurrency(amountToGrantLamports, 'SOL'))}* to user ${getPlayerDisplayReference(targetUser)} (ID: \`${targetUser.telegram_id}\`).\nNew balance: *${escapeMarkdownV2(formatCurrency(grantResult.newBalanceLamports, 'SOL'))}*\\.`, { parse_mode: 'MarkdownV2' });
              // Notify the user who received the grant
              await safeSendMessage(targetUser.telegram_id, `🎉 You have received an admin grant of *${escapeMarkdownV2(formatCurrency(amountToGrantLamports, 'SOL'))}*\\! Your new balance is *${escapeMarkdownV2(formatCurrency(grantResult.newBalanceLamports, 'SOL'))}*\\.`, { parse_mode: 'MarkdownV2' });
            } else {
              await grantClient.query('ROLLBACK');
              await safeSendMessage(chatId, `❌ Failed to grant SOL: ${escapeMarkdownV2(grantResult.error || "Unknown error during balance update.")}`, { parse_mode: 'MarkdownV2'});
            }
          } catch (grantError) {
            if (grantClient) await grantClient.query('ROLLBACK').catch(()=>{});
            console.error(`${LOG_PREFIX_MSG} Admin Grant DB Transaction Error: ${grantError.message}`);
            await safeSendMessage(chatId, `❌ Database error during grant: \`${escapeMarkdownV2(grantError.message)}\``, { parse_mode: 'MarkdownV2'});
          } finally {
            if (grantClient) grantClient.release();
          }
        } else {
          // Non-admin trying to use /grant
          // await safeSendMessage(chatId, "🤔 This command is reserved for casino administrators.", {}); // Can be silent
        }
        break;

      // --- Game Initiation Commands (Placeholders - full handlers in Part 5a S2, 5b, 5c) ---
      case 'coinflip':
      case 'startcoinflip':
        if (typeof handleStartGroupCoinFlipCommand === 'function') { // From Part 5a S2
          const betCF = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartGroupCoinFlipCommand(chatId, userForCommandProcessing, betCF, originalMessageId, chatType);
        } else console.error("handleStartGroupCoinFlipCommand not defined yet.");
        break;
      case 'rps':
      case 'startrps':
        if (typeof handleStartGroupRPSCommand === 'function') { // From Part 5a S2
          const betRPS = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartGroupRPSCommand(chatId, userForCommandProcessing, betRPS, originalMessageId, chatType);
        } else console.error("handleStartGroupRPSCommand not defined yet.");
        break;
      case 'diceescalator':
      case 'de': 
        if (typeof handleStartDiceEscalatorCommand === 'function') { // From Part 5b S1
          const betDE = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartDiceEscalatorCommand(msg, betDE); // Pass full msg object
        } else console.error("handleStartDiceEscalatorCommand not defined yet.");
        break;
      case 'dice21':
      case 'd21':
      case 'blackjack':
        if (typeof handleStartDice21Command === 'function') { // From Part 5b S2
          const betD21 = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartDice21Command(msg, betD21); // Pass full msg object
        } else console.error("handleStartDice21Command not defined yet.");
        break;
      case 'ou7':
      case 'overunder7':
        if (typeof handleStartOverUnder7Command === 'function') { // From Part 5c
          const betOU7 = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartOverUnder7Command(msg, betOU7); // Pass full msg object
        } else console.error("handleStartOverUnder7Command not defined yet.");
        break;
      case 'duel':
      case 'highroller':
        if (typeof handleStartDuelCommand === 'function') { // From Part 5c
          const betDuel = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartDuelCommand(msg, betDuel); // Pass full msg object
        } else console.error("handleStartDuelCommand not defined yet.");
        break;
      case 'ladder':
      case 'greedsladder':
        if (typeof handleStartLadderCommand === 'function') { // From Part 5c
          const betLadder = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartLadderCommand(msg, betLadder); // Pass full msg object
        } else console.error("handleStartLadderCommand not defined yet.");
        break;
      case 'sevenout':
      case 's7':
      case 'craps':
        if (typeof handleStartSevenOutCommand === 'function') { // From Part 5c
          const betS7 = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartSevenOutCommand(msg, betS7); // Pass full msg object
        } else console.error("handleStartSevenOutCommand not defined yet.");
        break;
      case 'slot':
      case 'slots':
      case 'slotfrenzy':
        if (typeof handleStartSlotCommand === 'function') { // From Part 5c
          const betSlot = await parseBetAmount(commandArgs[0], chatId, chatType);
          await handleStartSlotCommand(msg, betSlot); // Pass full msg object
        } else console.error("handleStartSlotCommand not defined yet.");
        break;

      default:
        // Only respond to unknown commands if in PM or if bot is explicitly mentioned in a group.
        const selfBotInfo = await bot.getMe();
        if (chatType === 'private' || text.startsWith(`/@${selfBotInfo.username}`)) { 
          await safeSendMessage(chatId, `❓ Hmmm, I don't recognize the command \`/${escapeMarkdownV2(commandName || "")}\`\\. Try \`/help\` for a list of available commands\\.`, { parse_mode: 'MarkdownV2' });
        }
    }
  } // End of command processing (if text.startsWith('/'))
}); // End of bot.on('message')

// --- Callback Query Handler (`bot.on('callback_query')`) ---
bot.on('callback_query', async (callbackQuery) => {
  const LOG_PREFIX_CBQ = `[CBQ_Handler ID:${callbackQuery.id}]`;
  if (isShuttingDown) {
    try { await bot.answerCallbackQuery(callbackQuery.id, { text: "⚙️ The casino is currently closing. Please try again later."}); } catch(e) {/* ignore */}
    return;
  }

  const msg = callbackQuery.message; // Message the button was attached to
  const userFromCb = callbackQuery.from; // User who clicked the button
  const callbackQueryId = callbackQuery.id;
  const data = callbackQuery.data; // Data from the button: "action:param1:param2"

  if (!msg || !userFromCb || !data) {
    console.error(`${LOG_PREFIX_CBQ} Ignoring malformed callback query. Message or User or Data missing.`);
    try { await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Invalid query data." }); } catch(e) {/* ignore */}
    return;
  }

  const userId = String(userFromCb.id);
  const originalChatId = String(msg.chat.id); // Chat where the button was clicked
  const originalChatType = msg.chat.type;
  const originalMessageId = msg.message_id; // ID of the message with the button

  // Answer the callback query quickly to remove the "loading" state on the button.
  // Specific handlers might answer again with an alert if needed.
  try { await bot.answerCallbackQuery(callbackQueryId); } 
  catch(e) { console.warn(`${LOG_PREFIX_CBQ} Non-critical: Failed to answer basic callback query: ${e.message}`); }

  let userObjectForCallback;
  try {
    userObjectForCallback = await getOrCreateUser(userId, userFromCb.username, userFromCb.first_name, userFromCb.last_name);
    if (!userObjectForCallback) {
      throw new Error("User data could not be fetched for callback processing.");
    }
  } catch(e) {
    console.error(`${LOG_PREFIX_CBQ} Error fetching user for callback: ${e.message}`, e.stack);
    // Send error to the chat where the button was (might be group or PM)
    await safeSendMessage(originalChatId, "🛠️ Apologies, a technical hiccup occurred while fetching your details for this action. Please try again.", {});
    return;
  }

  const [action, ...params] = data.split(':');
  console.log(`${LOG_PREFIX_CBQ} User ${getPlayerDisplayReference(userObjectForCallback)} Action: "${action}", Params: [${params.join(', ')}] (Chat: ${originalChatId}, Type: ${originalChatType}, OrigMsgID: ${originalMessageId})`);

  // If navigating to a main menu, clear any pending input state
  if (action === 'menu' && (params[0] === 'main' || params[0] === 'wallet' || params[0] === 'game_selection')) {
    if (typeof clearUserState === 'function') { // clearUserState from Part P3
      clearUserState(userId);
    } else {
      console.warn(`${LOG_PREFIX_CBQ} clearUserState function not available. User state might persist.`);
      userStateCache.delete(userId); // Fallback
    }
  }

  try {
    // RULES_CALLBACK_PREFIX from Part 1
    if (action.startsWith(RULES_CALLBACK_PREFIX)) { 
      let gameCodeForRule = action.substring(RULES_CALLBACK_PREFIX.length); 
      if (!gameCodeForRule && params.length > 0) { gameCodeForRule = params[0];} // Fallback if prefix was used with colon
      if (!gameCodeForRule) throw new Error("Missing game_code for rules display.");
      // handleDisplayGameRules will edit the message originalMessageId in originalChatId
      await handleDisplayGameRules(originalChatId, originalMessageId, gameCodeForRule, userObjectForCallback, originalChatType);
      return;
    }
    if (action === 'show_rules_menu') {
      // handleRulesCommand will edit originalMessageId or send new if edit fails
      await handleRulesCommand(originalChatId, userObjectForCallback, originalMessageId, true, originalChatType); // true for isEdit
      return;
    }
    // Privacy-sensitive actions: if in group, redirect to PM.
    const sensitiveActions = [
        DEPOSIT_CALLBACK_ACTION, 'quick_deposit', 
        WITHDRAW_CALLBACK_ACTION, 
        'menu:withdraw', 'menu:deposit', 'menu:history', 
        'menu:link_wallet_prompt', 'process_withdrawal_confirm'
    ];

    if ((originalChatType === 'group' || originalChatType === 'supergroup') && sensitiveActions.includes(action) || (action ==='menu' && sensitiveActions.includes(`${action}:${params[0]}`))) {
        const botUsername = (await bot.getMe()).username;
        // Edit the group message to indicate action is being handled in DM
        try {
            await bot.editMessageText(
                `For your privacy, ${getPlayerDisplayReference(userObjectForCallback)}, please continue this action in our direct message. I've sent you a message there!`,
                { chat_id: originalChatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {
                    inline_keyboard: [[{text: `📬 Open DM with @${botUsername}`, url: `https://t.me/${botUsername}?start=continueAction`}]]
                }}
            );
        } catch (editError) {
            if (!editError.message.includes("message is not modified")) {
                 console.warn(`${LOG_PREFIX_CBQ} Failed to edit group message for DM redirect: ${editError.message}`);
            }
        }
        // Now, call the actual handler but with the target chatId as the user's ID (for DM)
        // The handler needs to be aware it's sending a new message to DM, not editing.
        // We pass 'userId' as the targetChatId for the action.
        // And a special flag or modified originalMessageId (e.g., null) to indicate it should send new.
        // For simplicity, some handlers might need to be called with (userId, null, ...)
    }
    // Construct a mock msgOrCbMsg for handlers that expect it
    const mockMsgObjectForHandler = {
        from: userFromCb, // User who clicked
        chat: { id: (originalChatType === 'private' ? originalChatId : userId), type: (originalChatType === 'private' ? 'private' : 'private') }, // Target PM chat
        message_id: (originalChatType === 'private' ? originalMessageId : null), // Null if should send new in DM
        originalChatIdIfGroup: (originalChatType !== 'private' ? originalChatId : null), // Store original group ID if needed
        originalMessageIdIfGroup: (originalChatType !== 'private' ? originalMessageId : null)
    };


    switch (action) {
      // DEPOSIT_CALLBACK_ACTION and QUICK_DEPOSIT_CALLBACK_ACTION from Part 1
      case DEPOSIT_CALLBACK_ACTION: // Usually from /wallet menu
      case 'quick_deposit': // Usually from post-game or balance low messages
        if (typeof handleDepositCommand === 'function') { // Defined in Part P3
            await handleDepositCommand(mockMsgObjectForHandler, [], userId); // Pass mocked msg, no args, ensure userId
        } else console.error("handleDepositCommand not defined for callback.");
        break;
      case WITHDRAW_CALLBACK_ACTION: // Usually from /wallet menu
        if (typeof handleWithdrawCommand === 'function') { // Defined in Part P3
            await handleWithdrawCommand(mockMsgObjectForHandler, [], userId); // Pass mocked msg
        } else console.error("handleWithdrawCommand not defined for callback.");
        break;
      case 'menu': // Generic menu navigation
        const menuType = params[0];
        const menuParams = params.slice(1);
        if (typeof handleMenuAction === 'function') { // Defined in Part P3
            // handleMenuAction needs to be smart about originalChatId vs userId for sending response
            await handleMenuAction(userId, originalChatId, originalMessageId, menuType, menuParams, true, originalChatType);
        } else {
          console.error(`${LOG_PREFIX_CBQ} handleMenuAction not defined for menu type ${menuType}.`);
          await safeSendMessage(userId, `⚠️ Menu option \`${escapeMarkdownV2(menuType)}\` is currently unavailable\\. Please try later\\.`, { parse_mode: 'MarkdownV2'});
        }
        break;
      case 'process_withdrawal_confirm': // This action MUST be in PM
        const confirmation = params[0];
        const stateForWithdrawal = userStateCache.get(userId);
        if (originalChatType !== 'private' || !stateForWithdrawal || stateForWithdrawal.chatId !== userId) {
             console.warn(`${LOG_PREFIX_CBQ} Withdrawal confirmation attempt outside of designated private chat or state mismatch.`);
             await bot.editMessageText("⚠️ This confirmation is invalid or has expired. Please restart the withdrawal process in a private message with me.", { chat_id: originalChatId, message_id: originalMessageId, reply_markup: {}});
             if (stateForWithdrawal && stateForWithdrawal.chatId !== userId && bot) { // if state was for a DM but button clicked in group
                await bot.deleteMessage(stateForWithdrawal.chatId, stateForWithdrawal.messageId).catch(()=>{}); // Clean up DM message
             }
             clearUserState(userId);
             break;
        }

        if (confirmation === 'yes' && stateForWithdrawal.state === 'awaiting_withdrawal_confirmation') {
          const { linkedWallet, amountLamportsStr } = stateForWithdrawal.data;
          if (typeof handleWithdrawalConfirmation === 'function') { // Defined in Part P3
            await handleWithdrawalConfirmation(userId, userId, stateForWithdrawal.messageId, linkedWallet, amountLamportsStr); // Target PM
          } else {
            console.error(`${LOG_PREFIX_CBQ} handleWithdrawalConfirmation is not defined!`);
            await safeSendMessage(userId, "⚙️ Internal error processing withdrawal confirmation. Please contact support.", {});
          }
          clearUserState(userId);
        } else if (confirmation === 'no') {
          await bot.editMessageText("💸 Withdrawal Cancelled. Your funds remain in your casino balance.", { chat_id: userId, message_id: stateForWithdrawal.messageId, parse_mode:'MarkdownV2', reply_markup: {} });
          clearUserState(userId);
        } else if (!stateForWithdrawal || stateForWithdrawal.state !== 'awaiting_withdrawal_confirmation') {
          await bot.editMessageText("⚠️ Withdrawal confirmation has expired or is invalid. Please restart the withdrawal from the `/wallet` menu.", { chat_id: userId, message_id: (stateForWithdrawal?.messageId || originalMessageId), parse_mode:'MarkdownV2', reply_markup: {} });
          clearUserState(userId);
        }
        break;

      // --- Game Specific Callbacks (Forward to dedicated handlers) ---
      // These handlers need to be aware of originalChatId and originalChatType for responding.
      case 'join_game':
      case 'cancel_game':
      case 'rps_choose':
        if (typeof forwardGameCallback === 'function') { // Defined in Part 5a, Section 2
            await forwardGameCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
        } else console.warn(`${LOG_PREFIX_CBQ} Game callback action ${action} received, but forwardGameCallback not defined yet.`);
        break;
      case 'de_roll_prompt':
      case 'de_cashout':
      case 'jackpot_display_noop': 
      case 'play_again_de':
        if (typeof forwardDiceEscalatorCallback === 'function') { // Defined in Part 5b, Section 1
            await forwardDiceEscalatorCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
        } else console.warn(`${LOG_PREFIX_CBQ} Dice Escalator callback ${action} received, but forwarder not defined yet.`);
        break;
      case 'd21_hit': 
      case 'd21_stand':
      case 'play_again_d21':
        if (typeof forwardDice21Callback === 'function') { // Defined in Part 5b, Section 2
            await forwardDice21Callback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
        } else console.warn(`${LOG_PREFIX_CBQ} Dice 21 callback ${action} received, but forwarder not defined yet.`);
        break;
      case 'ou7_choice':
      case 'play_again_ou7':
      case 'duel_roll':
      case 'play_again_duel':
      case 'play_again_ladder': 
      case 's7_roll':
      case 'play_again_s7':
      case 'play_again_slot': 
        if (typeof forwardAdditionalGamesCallback === 'function') { // Defined in Part 5c
            await forwardAdditionalGamesCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
        } else console.warn(`${LOG_PREFIX_CBQ} Additional Games callback ${action} received, but forwarder not defined yet.`);
        break;

      default:
        console.log(`${LOG_PREFIX_CBQ} INFO: Unhandled callback action: "${action}" with params: [${params.join(', ')}]`);
        // Send to user's PM if it's an unknown action that might have been sensitive.
        await safeSendMessage(userId, `🤔 I'm not sure how to handle that action (\`${escapeMarkdownV2(action)}\`)\\. If you think this is an error, please try the command again or contact support\\.`, { parse_mode: 'MarkdownV2'});
    }
  } catch (error) {
    console.error(`${LOG_PREFIX_CBQ} 🚨 CRITICAL ERROR processing callback action "${action}": ${error.message}`, error.stack);
    // Send error to user's PM.
    await safeSendMessage(userId, "⚙️ Oops! Something went wrong while processing your action. Please try again or use a command. If the problem persists, contact support.", {}).catch(() => {});
  }
}); // End of bot.on('callback_query')


// --- Command Handler Functions (General Casino Bot Commands) ---
// These handlers now accept the full `msg` object to handle chatType for privacy.

async function handleStartCommand(msg, args) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) { /* Error handled by getOrCreateUser or main handler */ return; }

    const playerRef = getPlayerDisplayReference(userObject);
    const botUsername = (await bot.getMe()).username;

    // Handle deep links like /start ref_CODE or /start deposit_ACTION etc.
    if (args && args[0]) {
        const deepLinkParam = args[0];
        if (deepLinkParam.startsWith('ref_')) {
            const refCode = deepLinkParam.substring(4);
            // Logic to handle referral code, e.g., store it to apply on first deposit or if user is new.
            // This is complex and belongs in a dedicated referral processing logic, possibly called from getOrCreateUser if new.
            // For now, just acknowledge.
            const referrerUser = await getUserByReferralCode(refCode); // From Part P2
            const refBy = referrerUser ? getPlayerDisplayReference(await getOrCreateUser(referrerUser.telegram_id)) : "a fellow player";

            const referralMsg = `👋 Welcome, ${playerRef}\\! It looks like you joined via a referral link from ${refBy}\\. Enjoy the casino\\! 🎉`;
            if (chatType !== 'private') {
                await safeSendMessage(chatId, `${playerRef}, I've sent you a welcome message in our private chat regarding your referral\\.`, { parse_mode: 'MarkdownV2' });
                await safeSendMessage(userId, referralMsg, { parse_mode: 'MarkdownV2' });
            } else {
                await safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2' });
            }
            // Then proceed to show help or main menu
            await handleHelpCommand(msg); // Show help in the current context (group or PM)
            return;
        }
        // Add other deep link actions like /start deposit etc.
    }

    // Standard /start behavior
    if (chatType !== 'private') {
        await bot.deleteMessage(chatId, msg.message_id).catch(() => {}); // Delete /start command in group
        await safeSendMessage(chatId, `👋 Welcome to ${escapeMarkdownV2(BOT_NAME)}, ${playerRef}\\! I've sent you a private message with more details and how to get started\\. You can also DM me directly: @${escapeMarkdownV2(botUsername)}`, { parse_mode: 'MarkdownV2' });
        // Send help to DM
        const dmMsg = { ...msg, chat: { id: userId, type: 'private' }}; // Simulate a DM context for help
        await handleHelpCommand(dmMsg);
    } else {
        await handleHelpCommand(msg); // Show help directly in PM
    }
}


async function handleHelpCommand(originalMessageObject) {
    const userId = String(originalMessageObject.from.id);
    const chatId = String(originalMessageObject.chat.id); // Where to send the help message
    const chatType = originalMessageObject.chat.type;

    const userObj = await getOrCreateUser(userId, originalMessageObject.from.username, originalMessageObject.from.first_name, originalMessageObject.from.last_name);
    if(!userObj) return;

    const userMention = getPlayerDisplayReference(userObj);
    const jackpotScoreInfo = TARGET_JACKPOT_SCORE ? escapeMarkdownV2(String(TARGET_JACKPOT_SCORE)) : 'a high score';
    const botNameEscaped = escapeMarkdownV2(BOT_NAME);
    const botUsername = (await bot.getMe()).username;

    const minBetUsdDisplay = `$${MIN_BET_USD_val.toFixed(2)}`;
    const maxBetUsdDisplay = `$${MAX_BET_USD_val.toFixed(2)}`;
    
    let referenceLamportLimits = "";
    if (typeof MIN_BET_AMOUNT_LAMPORTS_config !== 'undefined' && typeof MAX_BET_AMOUNT_LAMPORTS_config !== 'undefined') {
        const minLamportDisplay = formatCurrency(MIN_BET_AMOUNT_LAMPORTS_config, 'SOL'); // Show as SOL for reference
        const maxLamportDisplay = formatCurrency(MAX_BET_AMOUNT_LAMPORTS_config, 'SOL');
        referenceLamportLimits = `\n_(Internal Ref: ${escapeMarkdownV2(minLamportDisplay)} to ${escapeMarkdownV2(maxLamportDisplay)})_`;
    }

    const helpTextParts = [
        `🎉 Welcome ${userMention} to **${botNameEscaped}** v${BOT_VERSION}\\! 🎉`,
        `\nYour ultimate destination for thrilling Solana casino games\\! Here's how to navigate your adventure:`,
        `\n\n*🏦 Your Casino Account & Funds:*`,
        `▫️ \`/balance\` or \`/bal\` \\- Check your current balance\\. *(Details sent via DM if in group)*`,
        `▫️ \`/deposit\` \\- Get your personal SOL deposit address\\. *(Handled in DM)*`,
        `▫️ \`/withdraw\` \\- Withdraw your SOL winnings\\. *(Handled in DM)*`,
        `▫️ \`/setwallet <YourSolanaAddress>\` \\- Link or update your withdrawal wallet\\. *(Best used in DM)*`,
        `▫️ \`/history\` \\- View your transaction history\\. *(Handled in DM)*`,
        `▫️ \`/referral\` \\- Get your referral link & check earnings\\. *(Earnings in DM)*`,
        `\n* casino_information Information & Support:*`,
        `▫️ \`/help\` \\- Displays this comprehensive guide\\.`,
        `▫️ \`/rules\` or \`/info\` \\- Get detailed rules for all our exciting games\\.`,
        `▫️ \`/jackpot\` \\- View the current Dice Escalator Super Jackpot amount\\.`,
        `\n*🎲 Available Games \\(Best in Groups!\\):*`,
        `▫️ \`/coinflip <bet_usd>\` \\- Classic 🪙 Heads or Tails\\.`,
        `▫️ \`/rps <bet_usd>\` \\- 🪨📄✂️ Rock Paper Scissors showdown\\.`,
        `▫️ \`/de <bet_usd>\` \\(or \`/diceescalator\`\\) \\- 🎲 Climb the score ladder for big wins & Jackpot glory\\!`,
        `▫️ \`/d21 <bet_usd>\` \\(or \`/blackjack\`\\) \\- 🃏 Dice Blackjack against the dealer\\.`,
        `▫️ \`/ou7 <bet_usd>\` \\(or \`/overunder7\`\\) \\- 🎲 Bet on the sum of two dice: Over, Under, or Exactly 7\\.`,
        `▫️ \`/duel <bet_usd>\` \\(or \`/highroller\`\\) \\- ⚔️ High\\-stakes dice duel against the Bot Dealer\\.`,
        `▫️ \`/ladder <bet_usd>\` \\(or \`/greedsladder\`\\) \\- 🪜 Risk it all in Greed's Ladder with 3 dice rolls\\.`,
        `▫️ \`/s7 <bet_usd>\` \\(or \`/sevenout\`, \`/craps\`\\) \\- 🎲 Simplified & fast\\-paced Craps\\.`,
        `▫️ \`/slot <bet_usd>\` \\(or \`/slots\`, \`/slotfrenzy\`\\) \\- 🎰 Spin the Slot Machine for instant prizes\\!`,
        `\n*💰 Betting Guide:*`,
        `To place a bet, simply type the game command followed by your bet amount in *USD*\\. Example: \`/d21 5\` for a $5 USD bet\\.`,
        `If no bet is specified, the game will use the minimum default bet\\.`,
        `Current Bet Limits: *${escapeMarkdownV2(minBetUsdDisplay)}* to *${escapeMarkdownV2(maxBetUsdDisplay)}* USD\\.${referenceLamportLimits}`,
        `\n*🏆 Dice Escalator Jackpot:*`,
        `Win a round of Dice Escalator by standing with a score of *${jackpotScoreInfo}\\+* AND beating the Bot Dealer to claim the dazzling Super Jackpot\\!`,
        `\nRemember to play responsibly and may fortune favor you\\! 🍀`,
        ADMIN_USER_ID ? `For support or issues, you can contact an admin or reach out to our support channel\\. (Admin Ref ID: ${escapeMarkdownV2(String(ADMIN_USER_ID).slice(0,4))}... )` : `For support, please refer to group administrators or the casino's official support channels\\.`,
        `\n Pssst\\! For private actions like checking balance or deposits, it's best to DM me: @${escapeMarkdownV2(botUsername)}`
    ];

    await safeSendMessage(chatId, helpTextParts.filter(Boolean).join('\n'), { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


async function handleBalanceCommand(msg) {
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;

    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) {
        await safeSendMessage(commandChatId, "Error fetching your profile. Please try /start.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(user);

    if (chatType !== 'private') {
        await bot.deleteMessage(commandChatId, msg.message_id).catch(()=>{});
        const botUsername = (await bot.getMe()).username;
        await safeSendMessage(commandChatId, `${playerRef}, for your privacy, I've sent your balance details to our private chat: @${escapeMarkdownV2(botUsername)} 📬`, { parse_mode: 'MarkdownV2' });
    }
    
    // Always send detailed balance to DM (userId)
    const balanceLamports = await getUserBalance(userId); // from Part 2
    if (balanceLamports === null) {
        await safeSendMessage(userId, "🏦 Oops! We couldn't retrieve your balance right now. Please try again in a moment.", { parse_mode: 'MarkdownV2' });
        return;
    }

    const balanceUSD = await formatBalanceForDisplay(balanceLamports, 'USD'); // from Part 3
    const balanceSOL = await formatBalanceForDisplay(balanceLamports, 'SOL');

    const balanceMessage = `🏦 *Your Casino Account Balance*\n\n` +
                           `Player: ${playerRef}\n` +
                           `------------------------------------\n` +
                           `💰 Approx\\. Value: *${escapeMarkdownV2(balanceUSD)}*\n` +
                           `🪙 SOL Balance: *${escapeMarkdownV2(balanceSOL)}*\n` +
                           `------------------------------------\n\n` +
                           `Use the buttons below or type commands to manage your funds or play games\\!`;
                           
    const keyboard = {
        inline_keyboard: [
            [{ text: "💰 Deposit SOL", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }], // QUICK_DEPOSIT_CALLBACK_ACTION from Part 1
            [{ text: "💸 Withdraw SOL", callback_data: WITHDRAW_CALLBACK_ACTION }],   // WITHDRAW_CALLBACK_ACTION from Part 1
            [{ text: "📜 Transaction History", callback_data: "menu:history" }],
            [{ text: "🎲 View Games & Rules", callback_data: "show_rules_menu" }]
        ]
    };
    await safeSendMessage(userId, balanceMessage, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
}


async function handleRulesCommand(chatId, userObj, messageIdToEdit = null, isEdit = false, chatType = 'private') {
    const LOG_PREFIX_RULES = `[RulesCmd UID:${userObj.telegram_id} Chat:${chatId}]`;
    const userMention = getPlayerDisplayReference(userObj);
    const botUsername = (await bot.getMe()).username;

    if (chatType !== 'private' && !isEdit) { // If /rules is used in group first time
        await bot.deleteMessage(chatId, messageIdToEdit).catch(()=>{}); // Delete user's /rules command
        await safeSendMessage(chatId, `${userMention}, I've sent the game rules menu to our private chat: @${escapeMarkdownV2(botUsername)} 룰렛`, {parse_mode: 'MarkdownV2'});
        // Update chatId to be the user's DM for sending the actual rules menu
        chatId = String(userObj.telegram_id);
        messageIdToEdit = null; // Cannot edit a message in a different chat, so send new
        isEdit = false;
    } else if (chatType !== 'private' && isEdit) {
        // If trying to edit a rules message in a group (e.g. back from specific rules),
        // it's better to just send a new message in DM, or simplify the group message.
        // For simplicity, we'll assume callbacks for rules navigation are handled primarily in PM.
        // If this call comes from a group context to re-display main rules, redirect to DM.
        await bot.editMessageText( `${userMention}, please continue Browse rules in our private chat: @${escapeMarkdownV2(botUsername)} 룰렛`, {chat_id: chatId, message_id: messageIdToEdit, parse_mode: 'MarkdownV2', reply_markup: {
            inline_keyboard: [[{text: `📬 Open DM with @${botUsername}`, url: `https://t.me/${botUsername}?start=showRules`}]]
        }}).catch(()=>{});
        chatId = String(userObj.telegram_id);
        messageIdToEdit = null; 
        isEdit = false; // Force send new in DM
    }


    const rulesIntroText = `📚 **${escapeMarkdownV2(BOT_NAME)} Gamepedia** 📚\n\n${userMention}, welcome to our library of games\\! Select any game below to learn its rules, payouts, and how to master it:`;
    
    const gameRuleButtons = Object.entries(GAME_IDS).map(([key, gameCode]) => {
        const gameName = key.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
        let emoji = '❓'; // Default emoji
        if (gameCode === GAME_IDS.COINFLIP) emoji = '🪙'; 
        else if (gameCode === GAME_IDS.RPS) emoji = '✂️'; // Rock, Paper, Scissors
        else if (gameCode === GAME_IDS.DICE_ESCALATOR) emoji = '🎲';
        else if (gameCode === GAME_IDS.DICE_21) emoji = '🃏'; // Blackjack often associated with cards
        else if (gameCode === GAME_IDS.OVER_UNDER_7) emoji = '🎲';
        else if (gameCode === GAME_IDS.DUEL) emoji = '⚔️';
        else if (gameCode === GAME_IDS.LADDER) emoji = '🪜';
        else if (gameCode === GAME_IDS.SEVEN_OUT) emoji = '🎲'; // Craps like game
        else if (gameCode === GAME_IDS.SLOT_FRENZY) emoji = '🎰';
        return { text: `${emoji} ${escapeMarkdownV2(gameName)}`, callback_data: `${RULES_CALLBACK_PREFIX}${gameCode}` }; 
    });

    const rows = [];
    for (let i = 0; i < gameRuleButtons.length; i += 2) { // Max 2 buttons per row
        rows.push(gameRuleButtons.slice(i, i + 2));
    }
    rows.push([{ text: '🏛️ Back to Main Menu', callback_data: 'menu:main' }]); // menu:main should take to main help/options in PM

    const keyboard = { inline_keyboard: rows };
    const options = { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true };

    if (isEdit && messageIdToEdit) {
        try {
            await bot.editMessageText(rulesIntroText, { chat_id: chatId, message_id: messageIdToEdit, ...options });
        } catch (e) {
            if (!e.message || !e.message.includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES} Failed to edit rules menu (ID: ${messageIdToEdit}), sending new. Error: ${e.message}`);
                await safeSendMessage(chatId, rulesIntroText, options);
            } else {
                 // console.log(`${LOG_PREFIX_RULES} Rules menu message not modified, no edit needed.`);
            }
        }
    } else {
        // If not an edit, and original messageId was from a command (so it exists), delete it first.
        if (!isEdit && messageIdToEdit && String(chatId) === String(userObj.telegram_id)) { // Only delete if we are in the same (PM) chat
             await bot.deleteMessage(chatId, messageIdToEdit).catch(()=>{});
        }
        await safeSendMessage(chatId, rulesIntroText, options);
    }
}

async function handleDisplayGameRules(chatId, originalMessageId, gameCode, userObj, chatType = 'private') {
    const LOG_PREFIX_RULES_DISP = `[RulesDisplay UID:${userObj.telegram_id} Game:${gameCode} Chat:${chatId}]`;
    const playerRef = getPlayerDisplayReference(userObj);
    const botUsername = (await bot.getMe()).username;

    if (chatType !== 'private') {
        await bot.editMessageText(`${playerRef}, I've sent the rules for *${escapeMarkdownV2(gameCode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()))}* to our private chat: @${escapeMarkdownV2(botUsername)} 📖`, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {
             inline_keyboard: [[{text: `📬 Open DM with @${botUsername}`, url: `https://t.me/${botUsername}?start=showRules_${gameCode}`}]]
        }}).catch(()=>{});
        // Send the actual rules to the user's DM
        chatId = String(userObj.telegram_id);
        originalMessageId = null; // Cannot edit, must send new in DM
    }

    let rulesTitle = gameCode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    let gameEmoji = '🎲'; // Default
    switch(gameCode) {
        case GAME_IDS.COINFLIP: gameEmoji = '🪙'; rulesTitle = "Coinflip Challenge"; break;
        case GAME_IDS.RPS: gameEmoji = '✂️'; rulesTitle = "Rock Paper Scissors Battle"; break;
        case GAME_IDS.DICE_ESCALATOR: gameEmoji = '🎲'; rulesTitle = "Dice Escalator Jackpot"; break;
        case GAME_IDS.DICE_21: gameEmoji = '🃏'; rulesTitle = "Dice 21 (Blackjack)"; break;
        case GAME_IDS.OVER_UNDER_7: gameEmoji = '🎲'; rulesTitle = "Over Under 7"; break;
        case GAME_IDS.DUEL: gameEmoji = '⚔️'; rulesTitle = "High Roller Duel"; break;
        case GAME_IDS.LADDER: gameEmoji = '🪜'; rulesTitle = "Greed's Ladder"; break;
        case GAME_IDS.SEVEN_OUT: gameEmoji = '🎲'; rulesTitle = "Sevens Out (Simplified Craps)"; break;
        case GAME_IDS.SLOT_FRENZY: gameEmoji = '🎰'; rulesTitle = "Slot Fruit Frenzy"; break;
        default: rulesTitle = `Game: ${rulesTitle}`;
    }

    let rulesText = `${gameEmoji} *Welcome to ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
    rulesText += `Hey ${playerRef}\\! Ready to learn the ropes for *${escapeMarkdownV2(rulesTitle)}*\\?\n\n`;

    // Use USD bet limits for display
    const solPrice = await getSolUsdPrice();
    const minBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(convertUSDToLamports(MIN_BET_USD_val, solPrice), solPrice));
    const maxBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(convertUSDToLamports(MAX_BET_USD_val, solPrice), solPrice));
    const defaultBetDisplay = minBetDisplay; // Default bet is min USD bet

    rulesText += `*General Betting:*\n` +
                 ` Bets are placed in USD\\. Min: *${minBetDisplay}*, Max: *${maxBetDisplay}*\\. \n` +
                 ` If no bet amount is specified, the game defaults to *${defaultBetDisplay}* USD\\.\n\n`;


    switch (gameCode) {
        case GAME_IDS.COINFLIP:
            rulesText += `*Objective:* Correctly guess the outcome of a coin toss \\(Heads or Tails\\)\\.\n` +
                         `*How to Play:* Type \`/coinflip <bet_usd>\` \\(e\\.g\\., \`/coinflip 1.50\` for $1\\.50 USD\\)\\. Another player joins, the coin is flipped\\. Winner takes the pot \\(2x their bet\\)\\!\n` +
                         `*Fair Play:* Truly 50/50 chance\\. May the luckiest player win\\!`;
            break;
        case GAME_IDS.RPS:
            rulesText += `*Objective:* Outsmart your opponent in the classic game of Rock, Paper, Scissors\\.\n` +
                         `*How to Play:* Start with \`/rps <bet_usd>\`\\. Another player joins\\. Both secretly choose Rock 🪨, Paper 📄, or Scissors ✂️\\.\n` +
                         `*Winning:* Rock crushes Scissors, Scissors cuts Paper, Paper covers Rock\\. Winner takes the pot \\(2x their bet\\)\\. Ties are a push \\(bet returned\\)\\.\n` +
                         `*Strategy:* Read your opponent's mind\\!`;
            break;
        case GAME_IDS.DICE_ESCALATOR:
            rulesText += `*Objective:* Accumulate the highest dice score without busting, then beat the Bot Dealer\\. Hit *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}\\+* and win the round to claim the Super Jackpot\\!\n` +
                         `*How to Play:* Type \`/de <bet_usd>\`\\. You roll a die repeatedly\\. Each roll adds to your score\\. \n` +
                         `*Busting:* Rolling a *${escapeMarkdownV2(String(DICE_ESCALATOR_BUST_ON))}* busts your score to 0, and you lose your wager instantly\\.\n` +
                         `*Standing:* Choose to "Stand" at any time to lock in your score\\. The Bot Dealer then plays, standing on *${escapeMarkdownV2(String(BOT_STAND_SCORE_DICE_ESCALATOR))}* or more\\. \n` +
                         `*Winning:* If your score is higher than the Bot's \\(or if the Bot busts\\), you win 2x your bet\\. If you win AND your score was *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}\\+*, you also win the current Jackpot amount\\!`;
            break;
        case GAME_IDS.DICE_21:
            rulesText += `*Objective:* Get your dice total closer to *${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}* than the Bot Dealer, without going over\\.\n` +
                         `*How to Play:* Start with \`/d21 <bet_usd>\`\\. You're dealt two dice\\. "Hit" to take another die, or "Stand" to keep your current total\\.\n` +
                         `*Bot's Turn:* The Bot Dealer stands on *${escapeMarkdownV2(String(DICE_21_BOT_STAND_SCORE))}* or more\\. \n` +
                         `*Winning:* Standard win pays 2x your bet\\. Getting *${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}* on your first two dice \\("Blackjack"\\) pays 2\\.5x your bet\\! Busting \\(score > ${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}\\) means you lose\\.`;
            break;
        case GAME_IDS.OVER_UNDER_7:
             rulesText += `*Objective:* Predict if the sum of *${escapeMarkdownV2(String(OU7_DICE_COUNT))}* dice will be Over 7, Under 7, or Exactly 7\\.\n` +
                          `*How to Play:* Start with \`/ou7 <bet_usd>\`\\. Then, choose your prediction from the buttons provided\\.\n` +
                          `*Payouts:*\n` +
                          `  ▫️ Under 7 \\(sum 2\\-6\\): Wins *2x* your bet\\.\n` +
                          `  ▫️ Over 7 \\(sum 8\\-12\\): Wins *2x* your bet\\.\n` +
                          `  ▫️ Exactly 7: Wins a handsome *${escapeMarkdownV2(String(OU7_PAYOUT_SEVEN + 1))}x* your bet\\! \\(${escapeMarkdownV2(String(OU7_PAYOUT_SEVEN))}:1 profit\\)\\.`;
            break;
        case GAME_IDS.DUEL:
            rulesText += `*Objective:* Roll a higher total sum with *${escapeMarkdownV2(String(DUEL_DICE_COUNT))}* dice than the Bot Dealer\\.\n` +
                         `*How to Play:* Start with \`/duel <bet_usd>\`\\. Click the button to roll your dice\\. The Bot Dealer then rolls theirs\\.\n` +
                         `*Winning:* Highest total sum wins 2x your bet\\. Ties are a push \\(bet returned\\)\\.\n` +
                         `*Pure Luck:* A straightforward test of fortune\\!`;
            break;
        case GAME_IDS.LADDER:
            rulesText += `*Objective:* Achieve a high score by summing *${escapeMarkdownV2(String(LADDER_ROLL_COUNT))}* dice rolls, without any die showing a *${escapeMarkdownV2(String(LADDER_BUST_ON))}*\\.\n` +
                         `*How to Play:* Start with \`/ladder <bet_usd>\`\\. The bot rolls all *${escapeMarkdownV2(String(LADDER_ROLL_COUNT))}* dice for you at once\\.\n` +
                         `*Busting:* If any die shows a *${escapeMarkdownV2(String(LADDER_BUST_ON))}*, you bust and lose your wager\\.\n` +
                         `*Payout Tiers:* If you don't bust, your payout is based on the total sum of your dice:\n`;
            LADDER_PAYOUTS.forEach(p => {
                 rulesText += `  ▫️ Sum *${escapeMarkdownV2(String(p.min))}\\-${escapeMarkdownV2(String(p.max))}*: Pays *${escapeMarkdownV2(String(p.multiplier + 1))}x* bet \\(${escapeMarkdownV2(p.label)}\\)\n`;
            });
            rulesText += `  ▫️ Lower sums may result in a push or loss\\.`;
            break;
        case GAME_IDS.SEVEN_OUT:
            rulesText += `*Objective:* A simplified dice game inspired by Craps\\. Win by rolling your "Point" before a 7, or win instantly on the Come Out Roll\\.\n` +
                         `*Come Out Roll (First Roll):*\n` +
                         `  ▫️ Roll a 7 or 11: *You Win Instantly* \\(2x bet\\)\\!\n` +
                         `  ▫️ Roll a 2, 3, or 12 \\("Craps"\\): *You Lose*\\.\n` +
                         `  ▫️ Roll any other sum \\(4, 5, 6, 8, 9, 10\\): This sum becomes your "Point"\\.\n` +
                         `*Point Phase (If a Point is Established):*\n` +
                         `  ▫️ The bot keeps rolling\\. If your Point is rolled again *before* a 7 is rolled: *You Win* \\(2x bet\\)\\!\n` +
                         `  ▫️ If a 7 is rolled *before* your Point: *You Lose* \\("Seven Out"\\)\\.`;
            break;
        case GAME_IDS.SLOT_FRENZY:
            rulesText += `*Objective:* Spin the reels and match symbols for exciting payouts based on a simulated dice roll \\(1\\-64\\)\\!\n` +
                         `*How to Play:* Start with \`/slot <bet_usd>\`\\. The bot spins the slot machine for you\\.\n` +
                         `*Winning:* Payouts depend on the combination rolled\\. Here are some top prizes \\(full paytable available via game announcements or help\\):\n`;
            for(const key in SLOT_PAYOUTS){ // SLOT_PAYOUTS from Part 1
                if(SLOT_PAYOUTS[key].multiplier >= 10){ 
                     rulesText += `  ▫️ ${SLOT_PAYOUTS[key].symbols} : Wins *${escapeMarkdownV2(String(SLOT_PAYOUTS[key].multiplier + 1))}x* bet \\(${escapeMarkdownV2(SLOT_PAYOUTS[key].label)}\\)\n`;
                }
            }
            rulesText += `  ▫️ Many other combinations offer smaller wins or your bet back\\!`;
            break;
        default:
            rulesText += `📜 Rules for *"${escapeMarkdownV2(rulesTitle)}"* are currently under development or this is an invalid game code\\. Please check back soon\\!`;
    }
    rulesText += `\n\nRemember to always play responsibly and within your limits\\. Good luck\\! 🍀`;

    const keyboard = { inline_keyboard: [[{ text: "📚 Back to Games List", callback_data: "show_rules_menu" }]] };
    
    if (originalMessageId && String(chatId) === String(userObj.telegram_id)) { // Can only edit if in the same PM chat
        try {
            await bot.editMessageText(rulesText, {
                chat_id: chatId, message_id: Number(originalMessageId),
                parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true
            });
        } catch (e) {
            if (!e.message || !e.message.includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES_DISP} Failed to edit rules display for ${gameCode}, sending new. Error: ${e.message}`);
                await safeSendMessage(chatId, rulesText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
            }
        }
    } else { // Send new message (either originalMessageId was null, or we are in a different chat like DM)
         await safeSendMessage(chatId, rulesText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
    }
}


async function handleJackpotCommand(chatId, userObj, chatType) {
    const LOG_PREFIX_JACKPOT = `[JackpotCmd UID:${userObj.telegram_id} Chat:${chatId}]`;
    const playerRef = getPlayerDisplayReference(userObj);
    let messageToSendIn = chatId; // Default to original chat

    if (chatType !== 'private') {
        // If in group, tell them info is sent to DM, but jackpot amount is not super sensitive.
        // We can show it in group, but also good to encourage DM for consistency with other financial info.
        // Let's show it in group for jackpot.
        // await bot.deleteMessage(chatId, msg.message_id).catch(()=>{}); // Optional: delete user's command
        // messageToSendIn = String(userObj.telegram_id); // Send to DM
        // await safeSendMessage(chatId, `${playerRef}, I've sent the current Jackpot details to your DMs! 📬`, {parse_mode: 'MarkdownV2'});
    }
    
    try {
        const result = await queryDatabase('SELECT current_amount FROM jackpots WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]); // queryDatabase from Part 1, MAIN_JACKPOT_ID from Part 1
        let jackpotAmountLamports = 0n;
        if (result.rows.length > 0) {
            jackpotAmountLamports = BigInt(result.rows[0].current_amount || '0');
        }
        
        const jackpotUSD = await formatBalanceForDisplay(jackpotAmountLamports, 'USD'); // from Part 3
        const jackpotSOL = await formatBalanceForDisplay(jackpotAmountLamports, 'SOL');

        const jackpotMessage = `🏆 *Dice Escalator Super Jackpot Status* 🏆\n\n` +
                               `The current Jackpot is a whopping:\n` +
                               `💰 Approx\\. Value: *${escapeMarkdownV2(jackpotUSD)}*\n` +
                               `🪙 SOL Amount: *${escapeMarkdownV2(jackpotSOL)}*\n\n` +
                               `Win a round of Dice Escalator with a score of *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}\\+* AND beat the Bot Dealer to take it all home\\! Good luck, high roller\\! ✨`;
        
        await safeSendMessage(messageToSendIn, jackpotMessage, { parse_mode: 'MarkdownV2' });

    } catch (error) {
        console.error(`${LOG_PREFIX_JACKPOT} Error fetching jackpot: ${error.message}`);
        await safeSendMessage(messageToSendIn, "⚙️ Apologies, there was an issue fetching the current Jackpot amount. Please try again soon.", {parse_mode: 'MarkdownV2'});
    }
}


// Helper to create consistent "Play Again" style keyboards. Game handlers will add game-specific buttons.
function createPostGameKeyboard(gameCode, betAmountLamports) { // betAmountLamports is BigInt
    // formatCurrency from Part 3. Bet amount is shown in SOL for "Play Again".
    const playAgainBetDisplay = escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL')); 
    let playAgainCallback = `play_again_${gameCode}:${betAmountLamports.toString()}`; 
    
    return {
        inline_keyboard: [
            [{ text: `🔁 Play Again (${playAgainBetDisplay})`, callback_data: playAgainCallback }],
            // QUICK_DEPOSIT_CALLBACK_ACTION from Part 1, RULES_CALLBACK_PREFIX from Part 1
            [{ text: "💰 Add Funds", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }, 
             { text: `📖 Rules (${gameCode.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())})`, callback_data: `${RULES_CALLBACK_PREFIX}${gameCode}` }]
        ]
    };
}

console.log("Part 5a, Section 1: Core Listeners, General Command Handlers & Payment UI Integration - Complete.");
// --- End of Part 5a, Section 1 ---
// --- Start of Part 5a, Section 2 ---
// index.js - Part 5a, Section 2: Simpler Group Game Handlers (Coinflip & RPS)
//---------------------------------------------------------------------------
console.log("Loading Part 5a, Section 2: Simpler Group Game Handlers (Coinflip & RPS)...");

// Assumes MIN_BET_USD_val, MAX_BET_USD_val, LAMPORTS_PER_SOL, formatCurrency, getPlayerDisplayReference,
// escapeMarkdownV2, generateGameId, updateUserBalance, getGroupSession, updateGroupGameDetails,
// safeSendMessage, activeGames, JOIN_GAME_TIMEOUT_MS, QUICK_DEPOSIT_CALLBACK_ACTION, GAME_IDS,
// determineCoinFlipOutcome, RPS_EMOJIS, RPS_CHOICES, determineRPSOutcome, createPostGameKeyboard
// are available from previous parts or globally.

// Helper function to forward game callbacks to this section's handlers
async function forwardGameCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_GAME_CB_FWD = `[GameCB_Forward UID:${userObject.telegram_id} Action:${action}]`;
    console.log(`${LOG_PREFIX_GAME_CB_FWD} Forwarding to Coinflip/RPS handler for chat ${originalChatId} (Type: ${originalChatType})`);

    const gameId = params[0]; // gameId is typically the first parameter

    switch (action) {
        case 'join_game':
            if (!gameId) {
                console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId for join_game action.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Game ID missing for join action.", show_alert: true });
                return;
            }
            await handleJoinGameCallback(originalChatId, userObject, gameId, originalMessageId, callbackQueryId, originalChatType);
            break;
        case 'cancel_game':
            if (!gameId) {
                console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId for cancel_game action.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Game ID missing for cancel action.", show_alert: true });
                return;
            }
            await handleCancelGameCallback(originalChatId, userObject, gameId, originalMessageId, callbackQueryId, originalChatType);
            break;
        case 'rps_choose':
            if (params.length < 2) { // gameId and choice
                console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId or choice for rps_choose action.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Missing parameters for RPS choice.", show_alert: true });
                return;
            }
            const choice = params[1];
            await handleRPSChoiceCallback(originalChatId, userObject, gameId, choice, originalMessageId, callbackQueryId, originalChatType);
            break;
        default:
            console.warn(`${LOG_PREFIX_GAME_CB_FWD} Unforwarded or unknown game action in this section: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Unknown action: ${action}`, show_alert: true });
    }
}


// --- Coinflip Game Command & Callbacks ---

async function handleStartGroupCoinFlipCommand(chatId, initiatorUserObj, betAmountLamports, commandMessageId, chatType) {
    const LOG_PREFIX_CF_START = `[Coinflip_Start UID:${initiatorUserObj.telegram_id} CH:${chatId}]`;
    // betAmountLamports is BigInt from parseBetAmount
    console.log(`${LOG_PREFIX_CF_START} Initiating Coinflip. Bet: ${betAmountLamports} lamports in chat type: ${chatType}.`);

    const initiatorId = String(initiatorUserObj.telegram_id);
    const initiatorMention = getPlayerDisplayReference(initiatorUserObj); // From Part 3
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD')); // For messages

    // Ensure game is started in a group chat
    if (chatType === 'private') {
        await safeSendMessage(chatId, `${initiatorMention}, Coinflip 🪙 is a group game\\! Please start it in a group chat where others can join\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    
    let chatInfo = null;
    try {
        if (bot && typeof bot.getChat === 'function') chatInfo = await bot.getChat(chatId);
    } catch (e) { console.warn(`${LOG_PREFIX_CF_START} Could not fetch chat info for ${chatId}: ${e.message}`); }
    const chatTitleEscaped = chatInfo?.title ? escapeMarkdownV2(chatInfo.title) : `Group Chat ${escapeMarkdownV2(chatId)}`;

    const gameSession = await getGroupSession(chatId, chatTitleEscaped); // From casino bot original Part 2 utils
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId) && activeGames.get(gameSession.currentGameId).type !== GAME_IDS.DICE_ESCALATOR && activeGames.get(gameSession.currentGameId).type !== GAME_IDS.DICE_21 ) { // Allow DE/D21 to run alongside simple group games
        const activeGameType = activeGames.get(gameSession.currentGameId)?.type || 'Another Game';
        await safeSendMessage(chatId, `⏳ A game of \`${escapeMarkdownV2(activeGameType)}\` is already active in this chat, ${initiatorMention}\\. Please wait for it to conclude before starting a new Coinflip\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${initiatorMention}, your balance is a bit low for a *${betDisplay}* Coinflip bet\\. You need approximately *${neededDisplay}* more\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]} // QUICK_DEPOSIT_CALLBACK_ACTION from Part 1
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.COINFLIP); // From Part 3
    // updateUserBalance from Part 2 (casino bot original, ideally uses ledger)
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmountLamports, `bet_placed_group_coinflip_init:${gameId}`, null, gameId, String(chatId));

    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${initiatorMention}, your Coinflip wager of *${betDisplay}* could not be placed due to an issue: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_CF_START} Initiator's bet of ${betAmountLamports} lamports placed for Coinflip game ${gameId}.`);

    const gameDataCF = {
        type: GAME_IDS.COINFLIP, gameId, chatId: String(chatId), initiatorId,
        initiatorMention: initiatorMention, // Already escaped by getPlayerDisplayReference
        betAmount: betAmountLamports, 
        participants: [{ userId: initiatorId, choice: null, mention: initiatorMention, betPlaced: true, userObj: initiatorUserObj }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null, chatType
    };
    activeGames.set(gameId, gameDataCF); 
    await updateGroupGameDetails(chatId, gameId, GAME_IDS.COINFLIP, Number(betAmountLamports)); 

    const joinMsgCF = `🪙 *Coinflip Challenge Accepted!* 🪙\n\nHigh roller ${initiatorMention} has started a Coinflip game for a thrilling *${betDisplay}*\\!\n\nWho dares to challenge their luck\\? Step right up\\!`;
    const kbCF = {
        inline_keyboard: [
            [{ text: "✨ Join Coinflip Battle!", callback_data: `join_game:${gameId}` }],
            [{ text: "🚫 Cancel Game (Initiator Only)", callback_data: `cancel_game:${gameId}` }]
        ]
    };
    const setupMsgCF = await safeSendMessage(chatId, joinMsgCF, { parse_mode: 'MarkdownV2', reply_markup: kbCF });

    if (setupMsgCF && activeGames.has(gameId)) {
        activeGames.get(gameId).gameSetupMessageId = setupMsgCF.message_id;
    } else {
        console.error(`${LOG_PREFIX_CF_START} Failed to send Coinflip setup message for game ${gameId} or game was removed from activeGames.`);
        await updateUserBalance(initiatorId, betAmountLamports, `refund_coinflip_setup_fail:${gameId}`, null, gameId, String(chatId)); // Refund
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }

    setTimeout(async () => {
        const gdCF_timeout = activeGames.get(gameId);
        if (gdCF_timeout && gdCF_timeout.status === 'waiting_opponent') {
            console.log(`[Coinflip_Timeout GID:${gameId}] Coinflip game expired waiting for an opponent.`);
            await updateUserBalance(gdCF_timeout.initiatorId, gdCF_timeout.betAmount, `refund_coinflip_timeout:${gameId}`, null, gameId, String(chatId));
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);

            const timeoutBetDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gdCF_timeout.betAmount, 'USD'));
            const timeoutMsgTextCF = `⏳ *Coinflip Expired* ⏳\nThe Coinflip game by ${gdCF_timeout.initiatorMention} for *${timeoutBetDisplay}* has expired as no challenger emerged\\. The wager has been refunded\\.`;
            if (gdCF_timeout.gameSetupMessageId && bot) {
                bot.editMessageText(timeoutMsgTextCF, { chatId: String(chatId), message_id: Number(gdCF_timeout.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                  .catch(() => { safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' }); });
            } else {
                safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' });
            }
        }
    }, JOIN_GAME_TIMEOUT_MS); // JOIN_GAME_TIMEOUT_MS from Part 1
}


// --- Rock Paper Scissors (RPS) Game Command & Callbacks ---

async function handleStartGroupRPSCommand(chatId, initiatorUserObj, betAmountLamports, commandMessageId, chatType) {
    const LOG_PREFIX_RPS_START = `[RPS_Start UID:${initiatorUserObj.telegram_id} CH:${chatId}]`;
    console.log(`${LOG_PREFIX_RPS_START} Initiating RPS. Bet: ${betAmountLamports} lamports in chat type: ${chatType}.`);

    const initiatorId = String(initiatorUserObj.telegram_id);
    const initiatorMention = getPlayerDisplayReference(initiatorUserObj);
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (chatType === 'private') {
        await safeSendMessage(chatId, `${initiatorMention}, Rock Paper Scissors ✂️ is a group game\\! Please start it in a group chat where an opponent can join the battle\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let chatInfo = null;
    try { if (bot) chatInfo = await bot.getChat(chatId); } catch (e) { /* ignore */ }
    const chatTitleEscaped = chatInfo?.title ? escapeMarkdownV2(chatInfo.title) : `Group Chat ${escapeMarkdownV2(chatId)}`;

    const gameSession = await getGroupSession(chatId, chatTitleEscaped);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId) && activeGames.get(gameSession.currentGameId).type !== GAME_IDS.DICE_ESCALATOR && activeGames.get(gameSession.currentGameId).type !== GAME_IDS.DICE_21 ) {
        const activeGameType = activeGames.get(gameSession.currentGameId)?.type || 'Another Game';
        await safeSendMessage(chatId, `⏳ A game of \`${escapeMarkdownV2(activeGameType)}\` is already active, ${initiatorMention}\\. Please wait for it to conclude before starting an RPS battle\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${initiatorMention}, your current balance is insufficient for an RPS battle of *${betDisplay}*\\. You need approximately *${neededDisplay}* more\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.RPS);
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmountLamports, `bet_placed_group_rps_init:${gameId}`, null, gameId, String(chatId));

    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${initiatorMention}, your RPS wager of *${betDisplay}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_RPS_START} Initiator's bet of ${betAmountLamports} lamports placed for RPS game ${gameId}.`);

    const gameDataRPS = {
        type: GAME_IDS.RPS, gameId, chatId: String(chatId), initiatorId,
        initiatorMention: initiatorMention,
        betAmount: betAmountLamports, 
        participants: [{ userId: initiatorId, choice: null, mention: initiatorMention, betPlaced: true, userObj: initiatorUserObj }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null, chatType
    };
    activeGames.set(gameId, gameDataRPS);
    await updateGroupGameDetails(chatId, gameId, GAME_IDS.RPS, Number(betAmountLamports));

    const joinMsgRPS = `🪨📄✂️ *An RPS Battle Cry Echoes!* 🪨📄✂️\n\nBrave warrior ${initiatorMention} has thrown down the gauntlet for an epic duel of Rock Paper Scissors, with *${betDisplay}* on the line\\!\n\nWho has the wits and courage to face them\\?`;
    const kbRPS = {
        inline_keyboard: [
            [{ text: "⚔️ Join RPS Duel!", callback_data: `join_game:${gameId}` }],
            [{ text: "🚫 Withdraw Challenge (Initiator Only)", callback_data: `cancel_game:${gameId}` }]
        ]
    };
    const setupMsgRPS = await safeSendMessage(chatId, joinMsgRPS, { parse_mode: 'MarkdownV2', reply_markup: kbRPS });

    if (setupMsgRPS && activeGames.has(gameId)) {
        activeGames.get(gameId).gameSetupMessageId = setupMsgRPS.message_id;
    } else {
        console.error(`${LOG_PREFIX_RPS_START} Failed to send RPS setup message for game ${gameId} or game was removed.`);
        await updateUserBalance(initiatorId, betAmountLamports, `refund_rps_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }

    setTimeout(async () => {
        const gdRPS_timeout = activeGames.get(gameId);
        if (gdRPS_timeout && gdRPS_timeout.status === 'waiting_opponent') {
            console.log(`[RPS_Timeout GID:${gameId}] RPS game expired waiting for opponent.`);
            await updateUserBalance(gdRPS_timeout.initiatorId, gdRPS_timeout.betAmount, `refund_rps_timeout:${gameId}`, null, gameId, String(chatId));
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);

            const timeoutBetDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gdRPS_timeout.betAmount, 'USD'));
            const timeoutMsgTextRPS = `⏳ *RPS Challenge Unanswered* ⏳\nThe Rock Paper Scissors battle initiated by ${gdRPS_timeout.initiatorMention} for *${timeoutBetDisplay}* has expired without a challenger\\. The wager has been refunded\\.`;
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
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This game is no longer available or has expired.", show_alert: true });
        if (interactionMessageId && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        }
        return;
    }

    const joinerId = String(joinerUserObj.telegram_id);
    if (gameData.initiatorId === joinerId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "😉 You can't join your own game! Waiting for another brave soul.", show_alert: true });
        return;
    }
    if (gameData.participants.length >= 2) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "🏃💨 Too slow! This game is already full.", show_alert: true });
        return;
    }
     if (gameData.status !== 'waiting_opponent') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ This game is not currently accepting new players.", show_alert: true });
        return;
    }


    const joinerMention = getPlayerDisplayReference(joinerUserObj);
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    if (BigInt(joinerUserObj.balance) < gameData.betAmount) {
        const needed = gameData.betAmount - BigInt(joinerUserObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await bot.answerCallbackQuery(callbackQueryId, { text: `Balance too low! Need ~${neededDisplay} more.`, show_alert: true });
        // Send a message in chat as well, as answerCallbackQuery is ephemeral
        await safeSendMessage(chatId, `${joinerMention}, your current balance is insufficient to join this *${betDisplay}* game\\. You need approximately *${neededDisplay}* more\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
        });
        return;
    }

    const balanceUpdateResult = await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_group_${gameData.type}_join:${gameId}`, null, gameId, String(chatId));

    if (!balanceUpdateResult.success) {
        console.error(`${LOG_PREFIX_JOIN} Bet placement failed for joiner ${joinerId}: ${balanceUpdateResult.error}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Wager failed: ${balanceUpdateResult.error || "Unknown issue"}.`, show_alert: true });
        return;
    }
    console.log(`${LOG_PREFIX_JOIN} Joiner's bet of ${gameData.betAmount} lamports placed for game ${gameId}.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: `✅ You've joined the ${gameData.type} game for ${betDisplay}!` });


    gameData.participants.push({ userId: joinerId, choice: null, mention: joinerMention, betPlaced: true, userObj: joinerUserObj });
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);

    if (gameData.type === GAME_IDS.COINFLIP && gameData.participants.length === 2) {
        gameData.status = 'resolving';
        activeGames.set(gameId, gameData); // Update game state

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        // Assign choices deterministically for fairness display if needed, though outcome is random
        p1.choice = 'heads'; 
        p2.choice = 'tails'; 

        const cfResult = determineCoinFlipOutcome(); // From Part 4
        let winnerParticipant = (cfResult.outcome === p1.choice) ? p1 : p2;
        let loserParticipant = (winnerParticipant === p1) ? p2 : p1;

        const winningsToCredit = gameData.betAmount + gameData.betAmount; // Total pot in lamports (winner gets both bets)

        const winnerUpdateResult = await updateUserBalance(winnerParticipant.userId, winningsToCredit, `won_group_coinflip:${gameId}`, null, gameId, String(chatId));
        await updateUserBalance(loserParticipant.userId, 0n, `lost_group_coinflip:${gameId}`, null, gameId, String(chatId)); // Log loss (0 net change as bet already deducted)

        let resMsg = `🪙 *Coinflip Resolved! The Coin is Tossed...* 🪙\nBet Amount: *${betDisplay}*\n\n`;
        resMsg += `${p1.mention} chose *Heads* ⬆️\n`;
        resMsg += `${p2.mention} chose *Tails* ⬇️\n\n`;
        resMsg += `The coin spins through the air and lands on\\.\\.\\. **${escapeMarkdownV2(cfResult.outcomeString)}** ${cfResult.emoji}\\!\n\n`;
        
        // Profit is one betAmount (winner gets their bet back + opponent's bet)
        resMsg += `🎉 Congratulations, ${winnerParticipant.mention}\\! You've won the pot and gained *${betDisplay}* profit\\! 🎉`;

        if (winnerUpdateResult.success) {
            const winnerNewBalanceDisplay = escapeMarkdownV2(await formatBalanceForDisplay(winnerUpdateResult.newBalanceLamports, 'USD'));
            resMsg += `\n\n${winnerParticipant.mention}'s new balance: *${winnerNewBalanceDisplay}*\\.`;
        } else {
            resMsg += `\n\n⚠️ A technical issue occurred while crediting ${winnerParticipant.mention}'s winnings\\. Our team has been notified\\.`;
            console.error(`${LOG_PREFIX_JOIN} CRITICAL: Failed to credit Coinflip winner ${winnerParticipant.userId} for game ${gameId}. Winnings: ${winningsToCredit}`);
            // Notify admin if credit failed
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 CRITICAL Coinflip Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nWinner: ${winnerParticipant.mention} (\`${escapeMarkdownV2(winnerParticipant.userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(winningsToCredit))}\`\nError: Failed to update balance in DB\\. Please investigate and credit manually if necessary\\.`, {parse_mode: 'MarkdownV2'});
            }
        }
        
        const postGameKeyboard = createPostGameKeyboard(GAME_IDS.COINFLIP, gameData.betAmount); // createPostGameKeyboard from Part 5a S1
        if (messageToEditId && bot) {
            bot.editMessageText(resMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard })
              .catch(async (e) => { 
                  console.warn(`${LOG_PREFIX_JOIN} Failed to edit Coinflip result message, sending new: ${e.message}`);
                  await safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard }); 
                });
        } else {
            await safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
        }

        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);

    } else if (gameData.type === GAME_IDS.RPS && gameData.participants.length === 2) {
        gameData.status = 'waiting_choices';
        activeGames.set(gameId, gameData);

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];

        const rpsPrompt = `🪨📄✂️ *Rock Paper Scissors \\- Battle Joined!* 🪨📄✂️\n\n${p1.mention} vs ${p2.mention} for *${betDisplay}*\\!\n\nBoth players, please *secretly* select your move using the buttons below\\. Your choice will be confirmed privately\\.`;
        const rpsKeyboard = {
            inline_keyboard: [[
                { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
                { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
                { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
            ],[
                { text: "🚫 Cancel Duel (Initiator Only)", callback_data: `cancel_game:${gameId}` }
            ]]
        };

        if (messageToEditId && bot) {
            bot.editMessageText(rpsPrompt, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard })
              .catch(async (e) => { 
                  console.warn(`${LOG_PREFIX_JOIN} Failed to edit RPS prompt message, sending new: ${e.message}`);
                  const newMsg = await safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); 
                  if(newMsg && activeGames.has(gameId)) activeGames.get(gameId).gameSetupMessageId = newMsg.message_id;
                });
        } else {
            const newMsg = await safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard });
            if (newMsg && activeGames.has(gameId)) activeGames.get(gameId).gameSetupMessageId = newMsg.message_id;
        }
        // No timeout for choices for now, relies on players acting. Could add one.
    }
}

async function handleCancelGameCallback(chatId, cancellerUserObj, gameId, interactionMessageId, callbackQueryId, chatType) {
    const LOG_PREFIX_CANCEL = `[CancelGame_CB UID:${cancellerUserObj.telegram_id} GID:${gameId} Chat:${chatId}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This game is no longer active or has already concluded.", show_alert: true });
        if (interactionMessageId && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        }
        return;
    }

    if (gameData.initiatorId !== String(cancellerUserObj.telegram_id)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Only the game initiator can cancel this game.", show_alert: true });
        return;
    }

    if (gameData.status !== 'waiting_opponent' && !(gameData.type === GAME_IDS.RPS && gameData.status === 'waiting_choices' && gameData.participants.length < 2) ) { // RPS can be cancelled if opponent hasn't fully joined or made choice
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ This game has already started or has an opponent. It cannot be cancelled now.", show_alert: true });
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: "✅ Game cancellation initiated." });

    console.log(`${LOG_PREFIX_CANCEL} Game ${gameId} cancellation requested by initiator. Refunding bets.`);
    for (const p of gameData.participants) {
        if (p.betPlaced && p.userId && gameData.betAmount > 0n) {
            await updateUserBalance(p.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, null, gameId, String(chatId));
            console.log(`${LOG_PREFIX_CANCEL} Refunded ${formatCurrency(gameData.betAmount, 'SOL')} to UserID: ${p.userId}`);
        }
    }

    const gameTypeDisplay = escapeMarkdownV2(gameData.type.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    const cancellationMessage = `🚫 *Game Cancelled by Initiator* 🚫\n\nThe ${gameTypeDisplay} game for *${betDisplay}*, started by ${gameData.initiatorMention}, has been cancelled\\. All wagers have been refunded\\.`;

    const msgToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (msgToEdit && bot) {
        bot.editMessageText(cancellationMessage, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: {} })
          .catch(async (e) => { 
              console.warn(`${LOG_PREFIX_CANCEL} Failed to edit cancel message, sending new: ${e.message}`);
              await safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' }); 
            });
    } else {
        await safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
    }

    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
    console.log(`${LOG_PREFIX_CANCEL} Game ${gameId} cancelled and removed.`);
}

async function handleRPSChoiceCallback(chatId, userChoiceObj, gameId, choiceKey, interactionMessageId, callbackQueryId, chatType) {
    const LOG_PREFIX_RPS_CHOICE = `[RPS_Choice_CB UID:${userChoiceObj.telegram_id} GID:${gameId} Choice:${choiceKey}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.RPS || gameData.status !== 'waiting_choices') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This RPS game isn't active or it's not time to choose.", show_alert: true });
        if (interactionMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        return;
    }

    const participant = gameData.participants.find(p => p.userId === String(userChoiceObj.telegram_id));
    if (!participant) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "🤔 You don't seem to be a player in this RPS game.", show_alert: true });
        return;
    }
    if (participant.choice) {
        await bot.answerCallbackQuery(callbackQueryId, { text: `🛡️ You've already chosen ${RPS_EMOJIS[participant.choice]}! Waiting for your opponent.`, show_alert: false });
        return;
    }

    participant.choice = choiceKey.toLowerCase(); // RPS_CHOICES are lowercase
    const choiceEmoji = RPS_EMOJIS[participant.choice] || '❓';
    const choiceFormatted = participant.choice.charAt(0).toUpperCase() + participant.choice.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, { text: `🎯 You chose ${choiceEmoji} ${choiceFormatted}! Waiting for opponent...`, show_alert: false });

    const p1 = gameData.participants[0];
    const p2 = gameData.participants[1];
    const allChosen = p1 && p1.choice && p2 && p2.choice;
    const msgToEdit = Number(gameData.gameSetupMessageId || interactionMessageId); // Should be the message with the buttons

    if (allChosen) {
        gameData.status = 'game_over'; // Prevent further choices
        activeGames.set(gameId, gameData); // Update status

        const rpsOutcome = determineRPSOutcome(p1.choice, p2.choice); // From Part 4 (returns styled description)
        const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

        let resultText = `🪨📄✂️ *RPS Battle Concluded!* 🪨📄✂️\nBet Amount: *${betDisplay}*\n\n`;
        resultText += `${p1.mention} chose: ${RPS_EMOJIS[p1.choice]} ${escapeMarkdownV2(p1.choiceFormatted || p1.choice)}\n`;
        resultText += `${p2.mention} chose: ${RPS_EMOJIS[p2.choice]} ${escapeMarkdownV2(p2.choiceFormatted || p2.choice)}\n\n`;
        resultText += `*Result:* ${rpsOutcome.description}\n\n`; // Description is already styled and MarkdownV2 safe from Part 4

        let finalBalancesText = "";

        if (rpsOutcome.result === 'win_player1') {
            const winnings = gameData.betAmount + gameData.betAmount; // Total pot
            const winUpdate = await updateUserBalance(p1.userId, winnings, `won_group_rps:${gameId}`, null, gameId, String(chatId));
            await updateUserBalance(p2.userId, 0n, `lost_group_rps:${gameId}`, null, gameId, String(chatId)); // Log loss
            resultText += `🎉 ${p1.mention} is the undisputed RPS Champion!`;
            if (winUpdate.success) finalBalancesText += `\n${p1.mention}'s new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(winUpdate.newBalanceLamports, 'USD'))}*\\.`;
        } else if (rpsOutcome.result === 'win_player2') {
            const winnings = gameData.betAmount + gameData.betAmount;
            const winUpdate = await updateUserBalance(p2.userId, winnings, `won_group_rps:${gameId}`, null, gameId, String(chatId));
            await updateUserBalance(p1.userId, 0n, `lost_group_rps:${gameId}`, null, gameId, String(chatId));
            resultText += `🎉 ${p2.mention} emerges victorious in the RPS arena!`;
            if (winUpdate.success) finalBalancesText += `\n${p2.mention}'s new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(winUpdate.newBalanceLamports, 'USD'))}*\\.`;
        } else if (rpsOutcome.result === 'draw') {
            resultText += `🤝 A hard-fought battle ends in a *Draw*! All wagers are returned.`;
            const refund1 = await updateUserBalance(p1.userId, gameData.betAmount, `refund_group_rps_draw:${gameId}`, null, gameId, String(chatId));
            const refund2 = await updateUserBalance(p2.userId, gameData.betAmount, `refund_group_rps_draw:${gameId}`, null, gameId, String(chatId));
            if (refund1.success) finalBalancesText += `\n${p1.mention}'s balance: *${escapeMarkdownV2(await formatBalanceForDisplay(refund1.newBalanceLamports, 'USD'))}*\\.`;
            if (refund2.success) finalBalancesText += `\n${p2.mention}'s balance: *${escapeMarkdownV2(await formatBalanceForDisplay(refund2.newBalanceLamports, 'USD'))}*\\.`;
        } else { 
            resultText += `⚙️ An unexpected error occurred during outcome calculation. Bets may be refunded if an issue is confirmed.`;
            console.error(`${LOG_PREFIX_RPS_CHOICE} RPS outcome error: ${rpsOutcome.description}`);
        }

        resultText += finalBalancesText;
        const postGameKeyboard = createPostGameKeyboard(GAME_IDS.RPS, gameData.betAmount);

        if (msgToEdit && bot) {
            bot.editMessageText(resultText, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard })
              .catch(async (e) => { 
                  console.warn(`${LOG_PREFIX_RPS_CHOICE} Failed to edit RPS result message, sending new: ${e.message}`);
                  await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard }); 
                });
        } else {
            await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
        }
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    } else {
        // Only one player has chosen, update the group message subtly.
        const p1Status = p1.choice ? `✅ ${p1.mention} has chosen.` : `⏳ ${p1.mention} is thinking...`;
        const p2Status = p2 && p2.choice ? `✅ ${p2.mention} has chosen.` : `⏳ ${p2.mention || 'Opponent'} is thinking...`; // p2 might not exist if bug
        
        const waitingText = `🪨📄✂️ *RPS Battle - Choices Pending* 🪨📄✂️\nBet: *${betDisplay}*\n\n${p1Status}\n${p2Status}\n\nWaiting for all players to make their move! Click your choice below.`;
         if (msgToEdit && bot) {
            try {
                // Keep the original keyboard available for the other player
                const rpsKeyboardForWait = {
                     inline_keyboard: [[
                        { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
                        { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
                        { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
                    ],[
                        { text: "🚫 Cancel Duel (Initiator Only)", callback_data: `cancel_game:${gameId}` }
                    ]]
                };
                await bot.editMessageText(waitingText, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboardForWait });
            } catch(e) {
                if (!e.message || !e.message.includes("message is not modified")) {
                    console.warn(`${LOG_PREFIX_RPS_CHOICE} Failed to edit RPS waiting message: ${e.message}`);
                }
            }
        }
    }
}

console.log("Part 5a, Section 2: Simpler Group Game Handlers (Coinflip & RPS) - Complete.");
// --- End of Part 5a, Section 2 ---
// --- Start of Part 5b, Section 1 ---
// index.js - Part 5b, Section 1: Dice Escalator Game Logic & Handlers
//---------------------------------------------------------------------------
console.log("Loading Part 5b, Section 1: Dice Escalator Game Logic & Handlers...");

// Assumes constants like MIN_BET_USD_val, TARGET_JACKPOT_SCORE, DICE_ESCALATOR_BUST_ON,
// BOT_STAND_SCORE_DICE_ESCALATOR, JACKPOT_CONTRIBUTION_PERCENT, MAIN_JACKPOT_ID,
// and functions like getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay,
// escapeMarkdownV2, generateGameId, updateUserBalance (ideally ledgered), getOrCreateUser,
// safeSendMessage, activeGames, pool, sleep, rollDie, formatDiceRolls, createPostGameKeyboard,
// QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, GAME_IDS are available.

// Helper function to forward Dice Escalator callbacks
async function forwardDiceEscalatorCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_DE_CB_FWD = `[DE_CB_Forward UID:${userObject.telegram_id} Action:${action}]`;
    console.log(`${LOG_PREFIX_DE_CB_FWD} Forwarding to Dice Escalator handler for chat ${originalChatId} (Type: ${originalChatType})`);

    const gameId = params[0]; // gameId is usually the first parameter

    if (!gameId && action !== 'jackpot_display_noop') { // jackpot_display_noop might not have a specific gameId if general
        console.error(`${LOG_PREFIX_DE_CB_FWD} Missing gameId for Dice Escalator action: ${action}.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Game identifier missing.", show_alert: true });
        return;
    }
    
    // Construct a mock msg object for handlers if they expect it for context (like chatType, originalMessageId)
    // For Dice Escalator, it's player vs bot, so chatType context is less critical for privacy than for financial commands.
    const mockMsgForHandler = {
        from: userObject, // User who clicked
        chat: { id: originalChatId, type: originalChatType },
        message_id: originalMessageId 
    };

    switch (action) {
        case 'de_roll_prompt': // Player chooses to roll
        case 'de_cashout':   // Player chooses to stand/cashout
            await handleDiceEscalatorPlayerAction(gameId, userObject.telegram_id, action, originalMessageId, originalChatId, callbackQueryId);
            break;
        case 'jackpot_display_noop': // User clicked the jackpot display button
            await bot.answerCallbackQuery(callbackQueryId, {text: "💰 Jackpot amount displayed."}).catch(()=>{});
            break;
        case 'play_again_de':
            if (!params[0] || isNaN(BigInt(params[0]))) {
                 console.error(`${LOG_PREFIX_DE_CB_FWD} Missing or invalid bet amount for play_again_de.`);
                 await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Bet amount missing for replay.", show_alert: true });
                 return;
            }
            const betAmountDELamports = BigInt(params[0]); // This param is actually the bet amount for "play again"
            if (bot && originalMessageId) { // Remove buttons from previous game message
                await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(()=>{});
            }
            // Call start command with a full msg-like object
            await handleStartDiceEscalatorCommand(mockMsgForHandler, betAmountDELamports);
            break;
        default:
            console.warn(`${LOG_PREFIX_DE_CB_FWD} Unforwarded or unknown Dice Escalator action: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Unknown game action: ${escapeMarkdownV2(action)}`, show_alert: true });
    }
}

// --- Helper Function to get Jackpot Text for the Dice Escalator Button ---
async function getJackpotButtonText(gameIdForCallback = null) {
    const LOG_PREFIX_JACKPOT_BTN = "[getJackpotButtonText]";
    let jackpotAmountString = "🎰 Jackpot: Fetching..."; // Default text

    try {
        if (typeof queryDatabase !== 'function' || typeof MAIN_JACKPOT_ID === 'undefined' || typeof formatBalanceForDisplay !== 'function') {
            console.warn(`${LOG_PREFIX_JACKPOT_BTN} Missing dependencies for jackpot button. Using default text.`);
            return { text: "💎 Jackpot: N/A", callback_data: `jackpot_display_noop:${gameIdForCallback || 'general'}` };
        }

        const result = await queryDatabase('SELECT current_amount FROM jackpots WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]);
        if (result.rows.length > 0) {
            const jackpotAmountLamports = BigInt(result.rows[0].current_amount || '0');
            // Display jackpot in USD for more impact on the button
            const jackpotDisplayAmountUSD = await formatBalanceForDisplay(jackpotAmountLamports, "USD");
            jackpotAmountString = `👑 Jackpot: ${escapeMarkdownV2(jackpotDisplayAmountUSD)} 👑`;
        } else {
            jackpotAmountString = "👑 Jackpot: Not Set 👑"; // More casino-like
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_JACKPOT_BTN} Error fetching jackpot for button: ${error.message}`, error.stack);
        jackpotAmountString = "👑 Jackpot: Error 👑";
    }
    const callbackData = `jackpot_display_noop:${gameIdForCallback || 'general'}`;
    return { text: jackpotAmountString, callback_data: callbackData };
}


// --- Dice Escalator Game Handler Functions ---

async function handleStartDiceEscalatorCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id); // Game plays out in this chat
    // const chatType = msg.chat.type; // Not strictly needed for DE as it's player vs bot
    const originalCommandMessageId = msg.message_id; // ID of the /de command message or previous game message

    const userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) { /* Error handled by getOrCreateUser or main message handler */ return; }

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

    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        const betReason = `bet_placed_dice_escalator:${gameId}`;
        const balanceUpdateResult = await updateUserBalance(userId, -betAmountLamports, betReason, client, gameId, String(chatId)); // Pass client for transaction

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_DE_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Dice Escalator wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }

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
                contributionLamports = 0n; // Reset if contribution failed, so refund logic is correct
            }
        }
        await client.query('COMMIT');
        console.log(`${LOG_PREFIX_DE_START} Wager *${betDisplayUSD}* accepted & jackpot contribution processed. New balance for ${userId}: ${formatCurrency(balanceUpdateResult.newBalanceLamports, 'SOL')}`);

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_DE_START} Rollback error:`, rbErr));
        console.error(`${LOG_PREFIX_DE_START} Transaction error during Dice Escalator bet placement: ${error.message}`);
        await safeSendMessage(chatId, `${playerRef}, a database error occurred while starting your game\\. Please try again\\. If the issue persists, contact support\\.`, { parse_mode: 'MarkdownV2'});
        return;
    } finally {
        if (client) client.release();
    }

    const gameData = {
        type: GAME_IDS.DICE_ESCALATOR, gameId, chatId: String(chatId), userId, playerRef, userObj,
        betAmount: betAmountLamports, playerScore: 0n, playerRollCount: 0, botScore: 0n,
        status: 'waiting_player_roll',
        gameMessageId: null, commandMessageId: originalCommandMessageId, // originalCommandMessageId might be null if from play_again
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
        activeGames.set(gameId, gameData); // Update gameData with message ID
    } else {
        console.error(`${LOG_PREFIX_DE_START} Failed to send Dice Escalator game message for ${gameId}. Attempting refund.`);
        const refundClient = await pool.connect();
        try {
            await refundClient.query('BEGIN');
            await updateUserBalance(userId, betAmountLamports, `refund_dice_escalator_setup_fail:${gameId}`, refundClient, gameId, String(chatId));
            if (contributionLamports > 0n) { // Reverse jackpot contribution if it happened
                await refundClient.query('UPDATE jackpots SET current_amount = current_amount - $1 WHERE jackpot_id = $2 AND current_amount >= $1', [contributionLamports.toString(), MAIN_JACKPOT_ID]);
            }
            await refundClient.query('COMMIT');
            console.log(`${LOG_PREFIX_DE_START} Successfully refunded bet and reversed jackpot contribution for game ${gameId} due to message send failure.`);
        } catch(refundError) {
            if(refundClient) await refundClient.query('ROLLBACK').catch(()=>{});
            console.error(`${LOG_PREFIX_DE_START} 🚨 CRITICAL: Failed to refund user/reverse contribution for ${gameId} after message send failure: ${refundError.message}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL DE Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount: \`${escapeMarkdownV2(formatCurrency(betAmountLamports))}\`\nReason: Failed to send game message AND failed to refund\\. Manual intervention required\\.`, {parse_mode:'MarkdownV2'});
        } finally {
            if(refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
    }
}

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
         await bot.answerCallbackQuery(callbackQueryId, { text: "⚙️ This game message is outdated. Please use the latest one.", show_alert: true });
         // Don't clear buttons here, user might find the correct message.
        return;
    }
    
    gameData.lastInteractionTime = Date.now(); // Update activity timer for the game
    activeGames.set(gameId, gameData);

    const jackpotButtonData = await getJackpotButtonText(gameId); 
    const actionBase = action.split(':')[0]; 

    switch (actionBase) {
        case 'de_roll_prompt':
            if (gameData.status !== 'waiting_player_roll' && gameData.status !== 'player_turn_prompt_action') {
                await bot.answerCallbackQuery(callbackQueryId, { text: "⏱️ Not your turn to roll, or the game has different plans!", show_alert: true });
                return;
            }
            await processDiceEscalatorPlayerRoll(gameData, jackpotButtonData, callbackQueryId);
            break;
        case 'de_cashout': // Player stands
            if (gameData.status !== 'player_turn_prompt_action') {
                await bot.answerCallbackQuery(callbackQueryId, { text: "✋ You can only stand after making at least one roll.", show_alert: true });
                return;
            }
            if (gameData.playerRollCount === 0) { // Should be caught by status check, but double-check
                 await bot.answerCallbackQuery(callbackQueryId, { text: "🎲 You need to roll at least once before standing!", show_alert: true });
                return;
            }
            await processDiceEscalatorStandAction(gameData, jackpotButtonData, callbackQueryId);
            break;
        default:
            console.error(`${LOG_PREFIX_DE_ACTION} Unknown Dice Escalator action: '${action}'.`);
            await bot.answerCallbackQuery(callbackQueryId, { text: "❓ Unknown game action.", show_alert: true });
    }
}

async function processDiceEscalatorPlayerRoll(gameData, currentJackpotButtonData, callbackQueryId) {
    const LOG_PREFIX_DE_PLAYER_ROLL = `[DE_PlayerRoll GID:${gameData.gameId} UID:${gameData.userId}]`;
    await bot.answerCallbackQuery(callbackQueryId, {text: "🎲 Rolling the die..."}).catch(()=>{});

    gameData.status = 'player_rolling'; // Intermediate status
    activeGames.set(gameData.gameId, gameData);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    const rollingMessage = `${gameData.playerRef} is shaking the dice for their *${betDisplayUSD}* wager\\! 🎲\nYour current score: *${escapeMarkdownV2(String(gameData.playerScore))}*\\. Good luck\\!`;
    const rollingKeyboard = { inline_keyboard: [[currentJackpotButtonData]]}; // Show jackpot while rolling
    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(rollingMessage, {
                chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: rollingKeyboard
            });
        } catch (editError) { 
            if (!editError.message.includes("message is not modified")) console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to edit 'rolling' message: ${editError.message}`);
        }
    }
    await sleep(700); // Pause for effect

    let playerRollValue;
    let animatedDiceMessageId = null;
    try {
        const diceMessage = await bot.sendDice(String(gameData.chatId), { emoji: '🎲' });
        playerRollValue = BigInt(diceMessage.dice.value);
        animatedDiceMessageId = diceMessage.message_id;
        await sleep(2200); // Let dice animation play
    } catch (diceError) {
        console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to send animated dice, using internal roll. Error: ${diceError.message}`);
        playerRollValue = BigInt(rollDie()); // rollDie from Part 3
        await safeSendMessage(String(gameData.chatId), `⚙️ Uh oh, the dice got stuck\\! ${gameData.playerRef}, your internal roll is a *${escapeMarkdownV2(String(playerRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' });
        await sleep(1000);
    }
    if (animatedDiceMessageId && bot) { bot.deleteMessage(String(gameData.chatId), animatedDiceMessageId).catch(() => {}); }

    gameData.playerRollCount += 1;
    const bustValue = BigInt(DICE_ESCALATOR_BUST_ON); // From Part 1
    const latestJackpotButtonData = await getJackpotButtonText(gameData.gameId); // Get fresh jackpot value

    if (playerRollValue === bustValue) {
        const originalScoreBeforeBust = gameData.playerScore;
        gameData.playerScore = 0n; // Busted score
        gameData.status = 'game_over_player_bust';
        activeGames.set(gameData.gameId, gameData);
        
        // Log loss - bet already deducted, so 0n change effectively. Ledger will record 'bet_loss_dice_escalator_bust'
        await updateUserBalance(gameData.userId, 0n, `lost_dice_escalator_bust:${gameData.gameId}`, null, gameData.gameId, String(gameData.chatId));

        const userForBalanceDisplay = await getOrCreateUser(gameData.userId); 
        const newBalanceDisplay = userForBalanceDisplay ? escapeMarkdownV2(await formatBalanceForDisplay(BigInt(userForBalanceDisplay.balance), 'USD')) : "N/A";

        const bustMessage = `💥 *Oh No, ${gameData.playerRef}!* 💥\nA roll of *${escapeMarkdownV2(String(playerRollValue))}* means you've BUSTED\\!\nYour score plummets from *${escapeMarkdownV2(String(originalScoreBeforeBust))}* to *0*\\. Your *${betDisplayUSD}* wager is lost to the house\\.\n\nYour new balance: *${newBalanceDisplay}*\\. Better luck next climb\\!`;
        const bustKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR, gameData.betAmount); 
        bustKeyboard.inline_keyboard.unshift([latestJackpotButtonData]); // Add jackpot button to top

        if (gameData.gameMessageId && bot) {
            await bot.editMessageText(bustMessage, { chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: bustKeyboard })
              .catch(async (e) => {
                  console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to edit bust message, sending new: ${e.message}`);
                  await safeSendMessage(String(gameData.chatId), bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
                });
        } else {
            await safeSendMessage(String(gameData.chatId), bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
        }
        activeGames.delete(gameData.gameId); // Game over
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
                 if (!e.message.includes("message is not modified")) {
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

async function processDiceEscalatorStandAction(gameData, currentJackpotButtonData, callbackQueryId) {
    const LOG_PREFIX_DE_STAND = `[DE_Stand GID:${gameData.gameId} UID:${gameData.userId}]`;
    await bot.answerCallbackQuery(callbackQueryId, {text: "✋ You chose to Stand! Bot plays next..."}).catch(()=>{}); 

    gameData.status = 'bot_turn_pending'; // Bot is about to play
    activeGames.set(gameData.gameId, gameData);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    const standMessage = `${gameData.playerRef} stands tall with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\! 🔒\nWager: *${betDisplayUSD}*\n\nThe Bot Dealer 🤖 steps up to the challenge\\.\\.\\. Let's see what fate unfolds\\!`;
    const standKeyboard = { inline_keyboard: [[currentJackpotButtonData]] }; // Just show jackpot

    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(standMessage, { chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: standKeyboard });
        } catch (e) {
            if (!e.message.includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_DE_STAND} Failed to edit stand message, sending new. Error: ${e.message}`);
                const newMsg = await safeSendMessage(String(gameData.chatId), standMessage, { parse_mode: 'MarkdownV2', reply_markup: standKeyboard });
                if(newMsg?.message_id && activeGames.has(gameData.gameId)) activeGames.get(gameData.gameId).gameMessageId = newMsg.message_id;
            }
        }
    } else { 
        const newMsg = await safeSendMessage(String(gameData.chatId), standMessage, { parse_mode: 'MarkdownV2', reply_markup: standKeyboard });
        if(newMsg?.message_id && activeGames.has(gameData.gameId)) activeGames.get(gameData.gameId).gameMessageId = newMsg.message_id;
    }

    await sleep(2000); // Dramatic pause before bot's turn
    await processDiceEscalatorBotTurn(gameData); // Process the bot's turn
}

async function processDiceEscalatorBotTurn(gameData) {
    const LOG_PREFIX_DE_BOT_TURN = `[DE_BotTurn GID:${gameData.gameId}]`;
    const { gameId, chatId, userId, playerRef, playerScore, betAmount, userObj, gameMessageId } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    
    gameData.status = 'bot_rolling'; 
    gameData.botScore = 0n; 
    activeGames.set(gameId, gameData);

    let botMessageAccumulator = `🤖 *Bot Dealer's Turn* 🤖\n${playerRef} stands at *${escapeMarkdownV2(String(playerScore))}*\\. The Bot Dealer aims to beat it\\!\n\n`;
    let currentTempMessageId = null;

    // Function to update bot's progress message
    const updateBotProgressMessage = async (text) => {
        if (currentTempMessageId && bot) { // Delete previous temporary message
            await bot.deleteMessage(String(chatId), currentTempMessageId).catch(()=>{});
            currentTempMessageId = null;
        }
        if (gameMessageId && bot) { // Edit the main game message
             try {
                await bot.editMessageText(text, {chat_id: String(chatId), message_id: Number(gameMessageId), parse_mode:'MarkdownV2'});
             } catch (e) {
                if (!e.message.includes("message is not modified")) {
                     console.warn(`${LOG_PREFIX_DE_BOT_TURN} Failed to edit bot progress, sending temp. Error: ${e.message}`);
                     const tempMsg = await safeSendMessage(String(chatId), text, {parse_mode:'MarkdownV2'});
                     currentTempMessageId = tempMsg?.message_id;
                }
             }
        } else { // Fallback to sending new temp messages
            const tempMsg = await safeSendMessage(String(chatId), text, {parse_mode:'MarkdownV2'});
            currentTempMessageId = tempMsg?.message_id;
        }
    };
    
    await updateBotProgressMessage(botMessageAccumulator + `Bot is rolling the first die\\.\\.\\. 🎲`);

    const botStandScore = BigInt(BOT_STAND_SCORE_DICE_ESCALATOR); 
    const bustValueBot = BigInt(DICE_ESCALATOR_BUST_ON);    
    let botRollsDisplay = [];

    while(gameData.botScore < botStandScore && gameData.botScore !== 0n /* not busted */) {
        await sleep(1500); // Pause between bot rolls
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
    activeGames.set(gameId, gameData); // Save final bot score

    // Clean up any last temporary message
    if (currentTempMessageId && bot && gameMessageId) { 
        await bot.deleteMessage(String(chatId), currentTempMessageId).catch(()=>{});
    }
    
    botMessageAccumulator += `\n------------------------------------\n📜 *Round Summary*\nPlayer Score: *${escapeMarkdownV2(String(playerScore))}*\nBot Score: *${escapeMarkdownV2(String(gameData.botScore))}* ${gameData.botScore === 0n && botRollsDisplay.length > 0 ? "\\(Busted\\)" : ""}\n`;

    let resultTextPart; 
    let payoutAmount = 0n; 
    let outcomeReasonLog = ""; 
    let jackpotWon = false;
    const targetJackpotScoreValue = BigInt(TARGET_JACKPOT_SCORE); 

    if (gameData.botScore === 0n) { 
        resultTextPart = `🎉 *YOU WIN!* The Bot Dealer busted spectacularly\\!`; 
        payoutAmount = betAmount + betAmount; // Player gets their bet back + opponent's (which is their bet)
        outcomeReasonLog = `won_dice_escalator_bot_bust:${gameId}`;
        if (playerScore >= targetJackpotScoreValue) jackpotWon = true;
    } else if (playerScore > gameData.botScore) { 
        resultTextPart = `🎉 *VICTORY!* Your score of *${escapeMarkdownV2(String(playerScore))}* triumphs over the Bot's *${escapeMarkdownV2(String(gameData.botScore))}*\\!`; 
        payoutAmount = betAmount + betAmount; 
        outcomeReasonLog = `won_dice_escalator_score:${gameId}`;
        if (playerScore >= targetJackpotScoreValue) jackpotWon = true;
    } else if (playerScore < gameData.botScore) { 
        resultTextPart = `💔 *House Wins.* The Bot Dealer's score of *${escapeMarkdownV2(String(gameData.botScore))}* narrowly beats your *${escapeMarkdownV2(String(playerScore))}*\\.`; 
        payoutAmount = 0n; // Bet already deducted
        outcomeReasonLog = `lost_dice_escalator_score:${gameId}`;
    } else { 
        resultTextPart = `😐 *PUSH!* A tense standoff ends in a tie at *${escapeMarkdownV2(String(playerScore))}*\\. Your wager of *${betDisplayUSD}* is returned\\.`; 
        payoutAmount = betAmount; // Return original bet
        outcomeReasonLog = `push_dice_escalator:${gameId}`;
    }
    botMessageAccumulator += `\n${resultTextPart}\n`;
    gameData.status = `game_over_final_${outcomeReasonLog.split(':')[0]}`;

    let finalUserBalanceForDisplay = BigInt(userObj.balance); 
    let jackpotPayoutAmount = 0n;

    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, client, gameId, String(chatId));
        if (balanceUpdateResult.success) {
            finalUserBalanceForDisplay = balanceUpdateResult.newBalanceLamports;
            if (payoutAmount > betAmount) { // Actual win, not just push
                 botMessageAccumulator += `\nYou win *${escapeMarkdownV2(await formatBalanceForDisplay(payoutAmount - betAmount, 'USD'))}* profit\\!`;
            } else if (payoutAmount === betAmount && outcomeReasonLog.startsWith('push')) {
                // Already covered by PUSH message
            } else if (payoutAmount === 0n) {
                // Loss message already covered
            }
        } else {
            botMessageAccumulator += `\n⚠️ Critical error crediting your game winnings\\. Admin has been notified\\.`;
            console.error(`${LOG_PREFIX_DE_BOT_TURN} Failed to update balance for game win/push. Error: ${balanceUpdateResult.error}`);
            // Notify admin - this is important as money movement failed
            if (typeof notifyAdmin === 'function') {
                 notifyAdmin(`🚨 CRITICAL DE Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
            }
        }

        if (jackpotWon) {
            const jackpotSelectResult = await client.query('SELECT current_amount FROM jackpots WHERE jackpot_id = $1 FOR UPDATE', [MAIN_JACKPOT_ID]);
            if (jackpotSelectResult.rows.length > 0) {
                jackpotPayoutAmount = BigInt(jackpotSelectResult.rows[0].current_amount || '0');
                if (jackpotPayoutAmount > 0n) {
                    const jackpotPayoutUpdate = await updateUserBalance(userId, jackpotPayoutAmount, `jackpot_win_dice_escalator:${gameId}`, client, gameId, String(chatId));
                    if (jackpotPayoutUpdate.success) {
                        await client.query('UPDATE jackpots SET current_amount = $1, last_won_timestamp = NOW(), last_won_by_telegram_id = $2 WHERE jackpot_id = $3', ['0', userId, MAIN_JACKPOT_ID]);
                        botMessageAccumulator += `\n\n👑🌟 *JACKPOT HIT!!!* 🌟👑\n${playerRef}, you've conquered the Dice Escalator and claimed the Super Jackpot of *${escapeMarkdownV2(await formatBalanceForDisplay(jackpotPayoutAmount, 'USD'))}*\\! Absolutely magnificent\\!`;
                        finalUserBalanceForDisplay = jackpotPayoutUpdate.newBalanceLamports; // Update with jackpot
                    } else {
                        botMessageAccumulator += `\n\n⚠️ Critical error crediting Jackpot winnings\\. Admin notified for manual processing\\.`;
                        console.error(`${LOG_PREFIX_DE_BOT_TURN} Failed to update balance for JACKPOT win. Error: ${jackpotPayoutUpdate.error}`);
                        if (typeof notifyAdmin === 'function') {
                           notifyAdmin(`🚨 CRITICAL DE JACKPOT Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nJackpot Amount: \`${escapeMarkdownV2(formatCurrency(jackpotPayoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(jackpotPayoutUpdate.error || "Unknown")}\`\\. Manual Jackpot payout required\\.`, {parse_mode:'MarkdownV2'});
                        }
                    }
                } else { 
                    botMessageAccumulator += `\n\n👑 You hit the Jackpot score, but the pot was empty this time\\! Still an amazing feat\\!`;
                }
            } else { 
                botMessageAccumulator += `\n\n👑 Jackpot system error\\. Admin notified\\.`;
                console.error(`${LOG_PREFIX_DE_BOT_TURN} MAIN_JACKPOT_ID not found in jackpots table.`);
            }
        }
        await client.query('COMMIT');
    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_DE_BOT_TURN} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_DE_BOT_TURN} Transaction error during Dice Escalator outcome/jackpot processing: ${error.message}`);
        botMessageAccumulator += `\n\n⚠️ A database error occurred while finalizing your game\\. Please contact support if your balance seems incorrect\\.`;
        if (typeof notifyAdmin === 'function') {
            notifyAdmin(`🚨 CRITICAL DE Transaction Error 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nError: \`${escapeMarkdownV2(error.message)}\`\\. Balance state may be inconsistent\\. Requires manual check\\.`, {parse_mode:'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }

    botMessageAccumulator += `\n\nYour updated balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceForDisplay, 'USD'))}*\\.`;
    
    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR, betAmount); 
    const finalJackpotButtonData = await getJackpotButtonText(gameId); // Get fresh jackpot for new game
    finalKeyboard.inline_keyboard.unshift([finalJackpotButtonData]);

    if (gameMessageId && bot) {
        await bot.editMessageText(botMessageAccumulator, { chat_id: String(chatId), message_id: Number(gameMessageId), parse_mode: 'MarkdownV2', reply_markup: finalKeyboard })
          .catch(async (e) => {
              console.warn(`${LOG_PREFIX_DE_BOT_TURN} Failed to edit final DE message, sending new: ${e.message}`);
              await safeSendMessage(String(chatId), botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
            });
    } else { 
        await safeSendMessage(String(chatId), botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
    }
    activeGames.delete(gameId);
}


console.log("Part 5b, Section 1: Dice Escalator Game Logic & Handlers - Complete.");
// --- End of Part 5b, Section 1 ---
// --- Start of Part 5b, Section 2 ---
// index.js - Part 5b, Section 2: Dice 21 (Blackjack Style) Game Logic & Handlers
//---------------------------------------------------------------------------
console.log("Loading Part 5b, Section 2: Dice 21 (Blackjack Style) Game Logic & Handlers...");

// Assumes constants like MIN_BET_USD_val, DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE,
// and functions like getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay,
// escapeMarkdownV2, generateGameId, updateUserBalance, getOrCreateUser,
// safeSendMessage, activeGames, pool, sleep, rollDie, formatDiceRolls, createPostGameKeyboard,
// QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, GAME_IDS are available.

// Helper function to forward Dice 21 callbacks
async function forwardDice21Callback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_D21_CB_FWD = `[D21_CB_Forward UID:${userObject.telegram_id} Action:${action}]`;
    console.log(`${LOG_PREFIX_D21_CB_FWD} Forwarding to Dice 21 handler for chat ${originalChatId} (Type: ${originalChatType})`);

    const gameId = params[0]; 

    if (!gameId) {
        console.error(`${LOG_PREFIX_D21_CB_FWD} Missing gameId for Dice 21 action: ${action}.`);
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
            if (!params[0] || isNaN(BigInt(params[0]))) {
                console.error(`${LOG_PREFIX_D21_CB_FWD} Missing or invalid bet amount for play_again_d21.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Bet amount invalid for replay.", show_alert: true });
                return;
            }
            const betAmountD21Lamports = BigInt(params[0]);
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


// --- DICE 21 GAME LOGIC ---

async function handleStartDice21Command(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    // const originalCommandMessageId = msg.message_id; // ID of /d21 command or previous game message (if play again)

    const userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) return;

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
    const balanceUpdateResult = await updateUserBalance(userId, -betAmountLamports, `bet_placed_dice21:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        console.error(`${LOG_PREFIX_D21_START} Wager placement failed: ${balanceUpdateResult.error}`);
        await safeSendMessage(chatId, `${playerRef}, your Dice 21 wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_D21_START} Wager ${betDisplayUSD} accepted. New balance for ${userId}: ${formatCurrency(balanceUpdateResult.newBalanceLamports, 'SOL')}`);

    let dealingMsg = await safeSendMessage(chatId, `🃏 Welcome to the **Dice 21 Blackjack Table**, ${playerRef}\\! Your wager: *${betDisplayUSD}*\\.\nThe dealer is shuffling the dice and dealing your initial hand\\.\\.\\. 🎲✨`, { parse_mode: 'MarkdownV2' });
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
        // playerHasNaturalBlackjack: false, // This logic is now removed
        status: 'player_turn', gameMessageId: null, lastInteractionTime: Date.now()
    };

    let messageText = `🃏 **Dice 21 Table** vs\\. Bot Dealer 🤖\n${playerRef}, your wager: *${betDisplayUSD}*\n\n`;
    messageText += `Your initial hand: ${formatDiceRolls(initialPlayerRollsValues)} summing to a hot *${escapeMarkdownV2(String(playerScore))}*\\!\n`; // formatDiceRolls from Part 3
    let buttons = [];

    if (playerScore > targetScoreD21) {
        messageText += `\n💥 *BUSTED!* Your score of *${escapeMarkdownV2(String(playerScore))}* went over the target of ${escapeMarkdownV2(String(targetScoreD21))}\\. The house takes the wager this round\\.`;
        gameData.status = 'game_over_player_bust';
        await updateUserBalance(userId, 0n, `lost_dice21_deal_bust:${gameId}`, null, gameId, String(chatId));
        const userForBalanceDisplay = await getOrCreateUser(userId);
        messageText += `\n\nYour new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(BigInt(userForBalanceDisplay.balance), 'USD'))}*\\. Tough break\\!`;
        buttons = createPostGameKeyboard(GAME_IDS.DICE_21, betAmountLamports).inline_keyboard[0]; 
    } else if (playerScore === targetScoreD21) {
        messageText += `\n✨ *PERFECT SCORE of ${escapeMarkdownV2(String(targetScoreD21))}!* You stand automatically\\. Let's see what the Bot Dealer 🤖 reveals\\!`;
        gameData.status = 'bot_turn_pending'; // Player stands automatically on target score
    } else { // playerScore < targetScoreD21
        messageText += `\nYour move, ${playerRef}: Will you "Hit" for another die ⤵️ or "Stand" with your current score ✅\\?`;
        buttons.push({ text: "⤵️ Hit Me!", callback_data: `d21_hit:${gameId}` });
        buttons.push({ text: `✅ Stand (${escapeMarkdownV2(String(playerScore))})`, callback_data: `d21_stand:${gameId}` });
    }
    buttons.push({ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` });

    const gameMessageOptions = { parse_mode: 'MarkdownV2', reply_markup: buttons.length > 0 ? { inline_keyboard: [buttons] } : {} };
    const sentGameMsg = await safeSendMessage(chatId, messageText, gameMessageOptions);

    if (sentGameMsg?.message_id) {
        gameData.gameMessageId = sentGameMsg.message_id;
    } else {
        console.error(`${LOG_PREFIX_D21_START} Failed to send Dice 21 game message for ${gameId}. Refunding wager.`);
        await updateUserBalance(userId, betAmountLamports, `refund_dice21_setup_msg_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId); return;
    }
    activeGames.set(gameId, gameData);

    if (gameData.status === 'bot_turn_pending') { // Player got target score on deal
        await sleep(2500); 
        await processDice21BotTurn(gameData, gameData.gameMessageId);
    } else if (gameData.status.startsWith('game_over')) { // Player busted on deal
        activeGames.delete(gameId);
    }
}

async function handleDice21Hit(gameId, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    const LOG_PREFIX_D21_HIT = `[D21_Hit GID:${gameId} UID:${userObj.telegram_id}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== String(userObj.telegram_id) || gameData.status !== 'player_turn' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This action is outdated or it's not your turn.", show_alert: true });
        if (originalMessageIdFromCallback && bot && gameData?.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) { // If message ID mismatch, clear buttons on old one
            bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
        }
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, {text: "🎲 Dealing another die..."}).catch(()=>{}); 

    const chatId = gameData.chatId;
    const previousGameMessageId = gameData.gameMessageId;

    if (previousGameMessageId && bot) { // Indicate rolling on the main game message
        try {
            await bot.editMessageText(`${gameData.playerRef} is drawing another die\\! 🎲\nPrevious hand: ${formatDiceRolls(gameData.playerHandRolls)} (Total: *${escapeMarkdownV2(String(gameData.playerScore))}*)\nRolling\\.\\.\\.`, {
                chat_id: String(chatId), message_id: Number(previousGameMessageId), parse_mode: 'MarkdownV2', reply_markup: {} // Clear buttons during roll
            });
        } catch (editError) { 
             if (!editError.message.includes("message is not modified")) console.warn(`${LOG_PREFIX_D21_HIT} Failed to edit 'hitting' message: ${editError.message}`);
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

    // Delete the main game message that was edited to "rolling..." and the animated dice
    if (previousGameMessageId && bot) { bot.deleteMessage(String(chatId), Number(previousGameMessageId)).catch(() => {}); }
    if (animatedDiceMessageIdHit && bot) { bot.deleteMessage(String(chatId), animatedDiceMessageIdHit).catch(() => {}); }

    gameData.playerHandRolls.push(Number(newRollValue)); // Store as number for formatDiceRolls
    gameData.playerScore += newRollValue;
    activeGames.set(gameId, gameData); // Update score before sending message

    const targetScoreD21 = BigInt(DICE_21_TARGET_SCORE);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    let newMainMessageText = `🃏 **Dice 21 Table** vs\\. Bot Dealer 🤖\n${gameData.playerRef}, wager: *${betDisplayUSD}*\n\n`;
    newMainMessageText += `You drew ${formatDiceRolls([Number(newRollValue)])}, updating your hand\\.\nNew Hand: ${formatDiceRolls(gameData.playerHandRolls)} totaling a sizzling *${escapeMarkdownV2(String(gameData.playerScore))}*\\!\n`;
    
    let buttons = []; 
    let gameEndedThisTurn = false;

    if (gameData.playerScore > targetScoreD21) {
        newMainMessageText += `\n💥 *OH NO, BUSTED!* Your score of *${escapeMarkdownV2(String(gameData.playerScore))}* flies past ${escapeMarkdownV2(String(targetScoreD21))}\\. The house collects the wager this round\\.`;
        gameData.status = 'game_over_player_bust'; gameEndedThisTurn = true;
        await updateUserBalance(gameData.userId, 0n, `lost_dice21_hit_bust:${gameId}`, null, gameId, String(chatId));
        const userForBalanceDisplay = await getOrCreateUser(gameData.userId);
        newMainMessageText += `\n\nYour new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(BigInt(userForBalanceDisplay.balance), 'USD'))}*\\. Better luck on the next deal\\!`;
        buttons = createPostGameKeyboard(GAME_IDS.DICE_21, gameData.betAmount).inline_keyboard[0];
    } else if (gameData.playerScore === targetScoreD21) {
        newMainMessageText += `\n✨ *PERFECT SCORE of ${escapeMarkdownV2(String(targetScoreD21))}!* You automatically stand\\. The Bot Dealer 🤖 prepares to reveal their hand\\.\\.\\.`;
        gameData.status = 'bot_turn_pending'; gameEndedThisTurn = true;
    } else { // playerScore < targetScoreD21
        newMainMessageText += `\nFeeling bold, ${gameData.playerRef}\\? "Hit" for another die ⤵️ or "Stand" firm with *${escapeMarkdownV2(String(gameData.playerScore))}* ✅\\?`;
        buttons.push({ text: "⤵️ Hit Again!", callback_data: `d21_hit:${gameId}` });
        buttons.push({ text: `✅ Stand (${escapeMarkdownV2(String(gameData.playerScore))})`, callback_data: `d21_stand:${gameId}` });
    }
    buttons.push({ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` });

    const newGameMessageOptions = { parse_mode: 'MarkdownV2', reply_markup: buttons.length > 0 ? { inline_keyboard: [buttons] } : {} };
    const sentNewMsg = await safeSendMessage(chatId, newMainMessageText, newGameMessageOptions);

    if (sentNewMsg?.message_id) {
        gameData.gameMessageId = sentNewMsg.message_id; // Update with new message ID
    } else { 
        console.error(`[D21_Hit GID:${gameId}] Failed to send updated game message after hit. Game might be stuck.`);
        activeGames.delete(gameId); return; 
    }
    activeGames.set(gameId, gameData); // Save updated game state

    if (gameEndedThisTurn) {
        if (gameData.status === 'bot_turn_pending') {
            await sleep(2500); 
            await processDice21BotTurn(gameData, gameData.gameMessageId);
        } else if (gameData.status.startsWith('game_over')) {
            activeGames.delete(gameId);
        }
    }
}

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
    
    // Delete the previous game message with Hit/Stand buttons
    if (previousGameMessageId && bot) { bot.deleteMessage(String(chatId), Number(previousGameMessageId)).catch(() => {}); }

    const standMessageText = `🃏 **Dice 21 Table** 🃏\n${gameData.playerRef} stands strong with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\! 💪\nThe Bot Dealer 🤖 now plays their hand\\. The tension mounts\\!`;
    const sentNewStandMsg = await safeSendMessage(chatId, standMessageText, { parse_mode: 'MarkdownV2' });

    if (sentNewStandMsg?.message_id) {
        gameData.gameMessageId = sentNewStandMsg.message_id; // Update to the new message ID
        activeGames.set(gameId, gameData);
    } else { 
        console.error(`[D21_Stand GID:${gameId}] Failed to send stand confirmation message. Game might be stuck.`);
        activeGames.delete(gameId); return; 
    }

    await sleep(2000); // Dramatic pause
    await processDice21BotTurn(gameData, gameData.gameMessageId);
}

async function processDice21BotTurn(gameData, currentMainGameMessageId) {
    const LOG_PREFIX_D21_BOT = `[D21_BotTurn GID:${gameData.gameId}]`;
    const { gameId, chatId, userId, playerRef, playerScore, betAmount, userObj, playerHasNaturalBlackjack /*This is now removed*/ } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));

    gameData.status = 'bot_rolling'; 
    gameData.botScore = 0n; 
    gameData.botHandRolls = [];
    activeGames.set(gameId, gameData); // Save initial bot state

    let botTurnInProgressMessage = `🃏 **Dice 21 Table** \\- Bot's Turn 🤖\n${playerRef}'s score: *${escapeMarkdownV2(String(playerScore))}*\\.\n\nThe Bot Dealer reveals their hand and begins to play\\.\\.\\.`;
    let tempMessageIdForBotRolls = null; // To show dice rolls separately

    // Edit main message or send initial bot turn status
    if (currentMainGameMessageId && bot) {
        try {
            await bot.editMessageText(botTurnInProgressMessage, {chat_id:String(chatId), message_id: Number(currentMainGameMessageId), parse_mode:'MarkdownV2', reply_markup: {}});
        } catch (e) {
            if (!e.message.includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_D21_BOT} Failed to edit main message for bot turn start, sending new. Err: ${e.message}`);
                const newMainMsg = await safeSendMessage(String(chatId), botTurnInProgressMessage, {parse_mode:'MarkdownV2'});
                if (newMainMsg?.message_id) currentMainGameMessageId = newMainMsg.message_id; // Update if new one sent
                gameData.gameMessageId = currentMainGameMessageId;
            }
        }
    } else {
        const newMainMsg = await safeSendMessage(String(chatId), botTurnInProgressMessage, {parse_mode:'MarkdownV2'});
        if (newMainMsg?.message_id) currentMainGameMessageId = newMainMsg.message_id;
        gameData.gameMessageId = currentMainGameMessageId;
    }
    activeGames.set(gameId, gameData); // Update game message ID if it changed
    await sleep(1500);

    const botStandScoreThreshold = BigInt(DICE_21_BOT_STAND_SCORE); // From Part 1
    const targetScoreD21 = BigInt(DICE_21_TARGET_SCORE);         // From Part 1
    let botBusted = false;

    for (let i = 0; i < 7 && gameData.botScore < botStandScoreThreshold && !botBusted; i++) { // Bot hits up to 7 times or until stand/bust
        const botRoll = BigInt(rollDie());
        gameData.botHandRolls.push(Number(botRoll)); 
        gameData.botScore += botRoll;
        activeGames.set(gameId, gameData); // Save each roll

        let rollDisplayMsgText = `Bot Dealer 🤖 rolls: ${formatDiceRolls([Number(botRoll)])}\nBot's current hand: ${formatDiceRolls(gameData.botHandRolls)} \\(Total: *${escapeMarkdownV2(String(gameData.botScore))}*\\)`;
        
        // Delete previous roll message and send new one
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
        await sleep(2000); // Pause between bot hits
    }
    
    // Clean up last temporary dice roll message if any
    if (tempMessageIdForBotRolls && bot) { await bot.deleteMessage(String(chatId), tempMessageIdForBotRolls).catch(()=>{}); }
    await sleep(1000); // Pause before final result

    // Final Result Calculation
    let resultTextEnd = ""; 
    let payoutAmount = 0n; 
    let outcomeReasonLog = "";
    // Removed playerHasNaturalBlackjack logic based on user feedback

    if (botBusted) { 
        resultTextEnd = `🎉 *Congratulations, ${playerRef}! You WIN!* 🎉\nThe Bot Dealer busted, making your score of *${escapeMarkdownV2(String(playerScore))}* the winner\\!`; 
        payoutAmount = betAmount + betAmount; // Standard 2x payout (bet back + 1x profit)
        outcomeReasonLog = `won_dice21_bot_bust:${gameId}`;
    } else if (playerScore > gameData.botScore) { 
        resultTextEnd = `🎉 *Outstanding, ${playerRef}! You WIN!* 🎉\nYour score of *${escapeMarkdownV2(String(playerScore))}* beats the Bot Dealer's *${escapeMarkdownV2(String(gameData.botScore))}*\\!`; 
        payoutAmount = betAmount + betAmount; 
        outcomeReasonLog = `won_dice21_score:${gameId}`;
    } else if (gameData.botScore > playerScore) { 
        resultTextEnd = `💔 *House Wins This Round\\.* 💔\nThe Bot Dealer's score of *${escapeMarkdownV2(String(gameData.botScore))}* edges out your *${escapeMarkdownV2(String(playerScore))}*\\.`; 
        payoutAmount = 0n; // Bet already deducted
        outcomeReasonLog = `lost_dice21_score:${gameId}`;
    } else { // Push (Scores are equal)
        resultTextEnd = `😐 *It's a PUSH! A TIE!* 😐\nBoth you and the Bot Dealer scored *${escapeMarkdownV2(String(playerScore))}*\\. Your wager of *${betDisplayUSD}* is returned\\.`; 
        payoutAmount = betAmount; // Return original bet
        outcomeReasonLog = `push_dice21:${gameId}`;
    }

    let finalSummaryMessage = `🃏 *Dice 21 \\- Final Result* 🃏\nYour Wager: *${betDisplayUSD}*\n\n`;
    finalSummaryMessage += `${playerRef}'s Hand: ${formatDiceRolls(gameData.playerHandRolls)} \\(Total: *${escapeMarkdownV2(String(playerScore))}*\\)\n`;
    finalSummaryMessage += `Bot Dealer's Hand: ${formatDiceRolls(gameData.botHandRolls)} \\(Total: *${escapeMarkdownV2(String(gameData.botScore))}*\\)${botBusted ? " \\- *BUSTED!*" : ""}\n\n${resultTextEnd}`;

    let finalUserBalanceForDisplay = BigInt(userObj.balance); // Start with balance before this game's outcome
    const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    if (balanceUpdate.success) { 
        finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports; 
        if (payoutAmount > betAmount && !outcomeReasonLog.startsWith('push')) { // Actual win
             finalSummaryMessage += `\nYou take home *${escapeMarkdownV2(await formatBalanceForDisplay(payoutAmount - betAmount, 'USD'))}* in profit\\!`;
        }
    } else { 
        finalSummaryMessage += `\n\n⚠️ A critical error occurred while settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "Unknown DB Error")}\`\\. Admin has been alerted\\.`; 
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL D21 Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
    }
    finalSummaryMessage += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceForDisplay, 'USD'))}*\\.`;

    const postGameKeyboardD21 = createPostGameKeyboard(GAME_IDS.DICE_21, betAmount);
    // Add rules button directly if not already there or if it's preferred
    // postGameKeyboardD21.inline_keyboard.push([{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` }]);
    
    // Edit the main game status message (which showed bot's turn starting) with the final result
    if (currentMainGameMessageId && bot) {
         await bot.editMessageText(finalSummaryMessage, { chat_id: String(chatId), message_id: Number(currentMainGameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardD21 })
           .catch(async (e) => {
               console.warn(`${LOG_PREFIX_D21_BOT} Failed to edit final D21 message, sending new: ${e.message}`);
               await safeSendMessage(String(chatId), finalSummaryMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardD21 });
           });
    } else {
        await safeSendMessage(String(chatId), finalSummaryMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardD21 });
    }

    activeGames.delete(gameId);
}

console.log("Part 5b, Section 2: Dice 21 (Blackjack Style) Game Logic & Handlers - Complete.");
// --- End of Part 5b, Section 2 ---
// --- Start of Part 5c ---
// index.js - Part 5c: Additional Game Logic & Handlers
// (Over/Under 7, Duel, Ladder, Sevens Out, Slot Frenzy)
//---------------------------------------------------------------------------
console.log("Loading Part 5c: Additional Game Logic & Handlers...");

// Assumes all necessary constants (GAME_IDS, OU7_*, DUEL_*, LADDER_*, SLOT_PAYOUTS, etc.)
// and functions (getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay, escapeMarkdownV2,
// generateGameId, updateUserBalance, getOrCreateUser, safeSendMessage, activeGames, pool, sleep,
// rollDie, formatDiceRolls, createPostGameKeyboard, QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX)
// are available from previous parts.

// --- Callback Forwarder for Additional Games ---
async function forwardAdditionalGamesCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_ADD_GAME_CB_FWD = `[AddGameCB_Forward UID:${userObject.telegram_id} Action:${action}]`;
    console.log(`${LOG_PREFIX_ADD_GAME_CB_FWD} Forwarding to Additional Games handler for chat ${originalChatId} (Type: ${originalChatType})`);

    const gameId = params[0]; // Often the first param, but for play_again it's the bet amount.
    const mockMsgForHandler = { // Construct mock msg for handlers
        from: userObject,
        chat: { id: originalChatId, type: originalChatType },
        message_id: originalMessageId
    };

    switch (action) {
        case 'ou7_choice':
            if (params.length < 2) { // gameId and choice
                console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing gameId or choice for ou7_choice.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Error: Missing parameters for OU7 choice.", show_alert: true });
                return;
            }
            const ou7Choice = params[1];
            await handleOverUnder7Choice(gameId, ou7Choice, userObject, originalMessageId, callbackQueryId, mockMsgForHandler);
            break;
        case 'play_again_ou7':
            const betAmountOU7 = BigInt(params[0]); // Param[0] is bet for play_again
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            await handleStartOverUnder7Command(mockMsgForHandler, betAmountOU7);
            break;
        case 'duel_roll':
            if (!gameId) { console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing gameId for duel_roll.`); await bot.answerCallbackQuery(callbackQueryId, {text:"⚠️ Error: Game ID missing.", show_alert:true}); return; }
            await handleDuelRoll(gameId, userObject, originalMessageId, callbackQueryId, mockMsgForHandler);
            break;
        case 'play_again_duel':
            const betAmountDuel = BigInt(params[0]);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            await handleStartDuelCommand(mockMsgForHandler, betAmountDuel);
            break;
        case 'play_again_ladder':
            const betAmountLadder = BigInt(params[0]);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            await handleStartLadderCommand(mockMsgForHandler, betAmountLadder);
            break;
        case 's7_roll': // For "Roll for Point" button in Sevens Out
             if (!gameId) { console.error(`${LOG_PREFIX_ADD_GAME_CB_FWD} Missing gameId for s7_roll.`); await bot.answerCallbackQuery(callbackQueryId, {text:"⚠️ Error: Game ID missing.", show_alert:true}); return; }
            await handleSevenOutRoll(gameId, userObject, originalMessageId, callbackQueryId, mockMsgForHandler);
            break;
        case 'play_again_s7':
            const betAmountS7 = BigInt(params[0]);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            await handleStartSevenOutCommand(mockMsgForHandler, betAmountS7);
            break;
        case 'play_again_slot':
            const betAmountSlot = BigInt(params[0]);
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
            await handleStartSlotCommand(mockMsgForHandler, betAmountSlot);
            break;
        default:
            console.warn(`${LOG_PREFIX_ADD_GAME_CB_FWD} Unhandled game callback prefix for action: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: `⚠️ Unknown action: ${escapeMarkdownV2(action)}`, show_alert: true });
    }
}


// --- Over/Under 7 Game Logic ---

async function handleStartOverUnder7Command(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) return;

    const LOG_PREFIX_OU7_START = `[OU7_Start UID:${userId} CH:${chatId}]`;
    console.log(`${LOG_PREFIX_OU7_START} Initiating Over/Under 7. Bet: ${betAmountLamports} lamports.`);
    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino balance is too low for an Over/Under 7 game at *${betDisplayUSD}*\\. You need ~*${neededDisplay}* more\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.OVER_UNDER_7);
    const balanceUpdateResult = await updateUserBalance(userId, -betAmountLamports, `bet_placed_ou7_init:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        console.error(`${LOG_PREFIX_OU7_START} Wager placement failed: ${balanceUpdateResult.error}`);
        await safeSendMessage(chatId, `${playerRef}, your Over/Under 7 wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const gameData = {
        type: GAME_IDS.OVER_UNDER_7, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, playerChoice: null, diceRolls: [], diceSum: null,
        status: 'waiting_player_choice', gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);

    const initialMessageText = `🎲 **Over/Under 7 Table** 🎲\n\n${playerRef}, you've wagered *${betDisplayUSD}*\\. The dice are ready\\!\nPredict the sum of *${escapeMarkdownV2(String(OU7_DICE_COUNT))}* dice: Will it be Under 7, Exactly 7, or Over 7\\? Make your choice below\\!`;
    const keyboard = {
        inline_keyboard: [
            [{ text: "📉 Under 7 (2-6)", callback_data: `ou7_choice:${gameId}:under` }],
            [{ text: "🎯 Exactly 7 (BIG PAYOUT!)", callback_data: `ou7_choice:${gameId}:seven` }],
            [{ text: "📈 Over 7 (8-12)", callback_data: `ou7_choice:${gameId}:over` }],
            [{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.OVER_UNDER_7}` }]
        ]
    };
    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
    } else {
        console.error(`${LOG_PREFIX_OU7_START} Failed to send OU7 game message for ${gameId}. Refunding wager.`);
        await updateUserBalance(userId, betAmountLamports, `refund_ou7_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
    }
}

async function handleOverUnder7Choice(gameId, choice, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    const LOG_PREFIX_OU7_CHOICE = `[OU7_Choice GID:${gameId} UID:${userObj.telegram_id} Choice:${choice}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== String(userObj.telegram_id) || gameData.status !== 'waiting_player_choice' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Over/Under 7 game action is outdated or not for you.", show_alert: true });
        if (originalMessageIdFromCallback && bot && gameData?.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
            bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(() => {});
        }
        return;
    }
    
    const choiceTextDisplay = choice.charAt(0).toUpperCase() + choice.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, {text: `🎯 You chose ${choiceTextDisplay} 7! Rolling...`}).catch(() => {});

    gameData.playerChoice = choice;
    gameData.status = 'rolling_dice';
    activeGames.set(gameId, gameData);

    const { chatId, playerRef, betAmount } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    
    let rollingMessageText = `🎲 **Over/Under 7 - Rolling!** 🎲\n${playerRef} bets *${betDisplayUSD}* on *${escapeMarkdownV2(choiceTextDisplay)} 7*\\.\nThe dice are tumbling\\! No going back now\\!`;

    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(rollingMessageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: {} });
        } catch (e) {
            if (!e.message.includes("message is not modified")) console.warn(`${LOG_PREFIX_OU7_CHOICE} Failed to edit rolling message: ${e.message}`);
        }
    } else {
        const newMsg = await safeSendMessage(String(chatId), rollingMessageText, { parse_mode: 'MarkdownV2' });
        if (newMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = newMsg.message_id;
    }
    await sleep(1000);

    let diceRolls = [];
    let diceSum = 0;
    let animatedDiceMessageIdsOU7 = [];
    for (let i = 0; i < OU7_DICE_COUNT; i++) {
        try {
            const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
            diceRolls.push(diceMsg.dice.value);
            diceSum += diceMsg.dice.value;
            animatedDiceMessageIdsOU7.push(diceMsg.message_id);
            await sleep(2000); // Let dice animation play
        } catch (e) {
            console.warn(`${LOG_PREFIX_OU7_CHOICE} Failed to send animated dice for OU7, using internal roll. Error: ${e.message}`);
            const internalRoll = rollDie();
            diceRolls.push(internalRoll);
            diceSum += internalRoll;
            await safeSendMessage(String(chatId), `⚙️ (Internal Casino Roll ${i + 1}): A *${escapeMarkdownV2(String(internalRoll))}* 🎲 tumbles out\\.`, { parse_mode: 'MarkdownV2' });
            await sleep(1000);
        }
    }
    gameData.diceRolls = diceRolls;
    gameData.diceSum = BigInt(diceSum);
    gameData.status = 'game_over';
    activeGames.set(gameId, gameData);
    animatedDiceMessageIdsOU7.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

    let win = false;
    let profitMultiplier = 0; // This is the profit multiplier (e.g., 1 for 1:1 profit, 4 for 4:1 profit)
    if (choice === 'under' && diceSum < 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
    else if (choice === 'over' && diceSum > 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
    else if (choice === 'seven' && diceSum === 7) { win = true; profitMultiplier = OU7_PAYOUT_SEVEN; }

    let payoutAmount = 0n; // Total amount to credit (bet + profit)
    let outcomeReasonLog = "";
    let resultTextPart = "";
    const profitAmountLamports = win ? betAmount * BigInt(profitMultiplier) : 0n;

    if (win) {
        payoutAmount = betAmount + profitAmountLamports; // Bet back + profit
        outcomeReasonLog = `won_ou7_${choice}_sum${diceSum}:${gameId}`;
        resultTextPart = `🎉 *WINNER!* Your prediction of *${escapeMarkdownV2(choiceTextDisplay)} 7* was spot on\\! You've won *${escapeMarkdownV2(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}* profit\\!`;
    } else {
        payoutAmount = 0n; // Bet already deducted
        outcomeReasonLog = `lost_ou7_${choice}_sum${diceSum}:${gameId}`;
        resultTextPart = `💔 *So Close!* Unfortunately, your prediction of *${escapeMarkdownV2(choiceTextDisplay)} 7* didn't hit this time\\.`;
    }

    let finalUserBalanceForDisplay = BigInt(userObj.balance); // Start with original balance for calculation
    const balanceUpdate = await updateUserBalance(userObj.telegram_id, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    if (balanceUpdate.success) {
        finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports;
    } else {
        resultTextPart += `\n\n⚠️ A critical error occurred settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Admin has been alerted\\.`;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL OU7 Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(String(userObj.telegram_id))}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
    }

    let finalMessageText = `🎲 **Over/Under 7 - Result** 🎲\nYour Bet: *${betDisplayUSD}* on *${escapeMarkdownV2(choiceTextDisplay)} 7*\\.\n\n`;
    finalMessageText += `The dice reveal: ${formatDiceRolls(diceRolls)} for a grand total of *${escapeMarkdownV2(String(diceSum))}*\\!\n\n${resultTextPart}`;
    finalMessageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceForDisplay, 'USD'))}*\\.`;

    const postGameKeyboardOU7 = createPostGameKeyboard(GAME_IDS.OVER_UNDER_7, betAmount);
    // Rules button is already part of createPostGameKeyboard

    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(finalMessageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
        } catch (e) {
             if (!e.message.includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_OU7_CHOICE} Failed to edit OU7 result message, sending new: ${e.message}`);
                await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
             }
        }
    } else {
        await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
    }
    activeGames.delete(gameId);
}


// --- High Roller Duel Game Logic ---

async function handleStartDuelCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) return;
    
    const LOG_PREFIX_DUEL_START = `[Duel_Start UID:${userId} CH:${chatId}]`;
    console.log(`${LOG_PREFIX_DUEL_START} Initiating High Roller Duel. Bet: ${betAmountLamports} lamports.`);
    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your funds are a little short for a *${betDisplayUSD}* High Roller Duel\\. You need about *${neededDisplay}* more\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.DUEL);
    const balanceUpdateResult = await updateUserBalance(userId, -betAmountLamports, `bet_placed_duel:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        console.error(`${LOG_PREFIX_DUEL_START} Wager placement failed: ${balanceUpdateResult.error}`);
        await safeSendMessage(chatId, `${playerRef}, your High Roller Duel wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const gameData = {
        type: GAME_IDS.DUEL, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, playerScore: 0n, playerRolls: [], botScore: 0n, botRolls: [],
        status: 'player_turn_to_roll', gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);

    const initialMessageText = `⚔️ **High Roller Duel Arena!** ⚔️\n\n${playerRef}, you've bravely wagered *${betDisplayUSD}* on this duel of dice\\!\nIt's your honor to roll first\\. Click to unleash your *${escapeMarkdownV2(String(DUEL_DICE_COUNT))}* dice\\! May fortune be your ally\\!`;
    const keyboard = { inline_keyboard: [
        [{ text: `🎲 Roll Your ${escapeMarkdownV2(String(DUEL_DICE_COUNT))} Dice!`, callback_data: `duel_roll:${gameId}` }],
        [{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DUEL}` }]
    ]};
    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
    } else {
        console.error(`${LOG_PREFIX_DUEL_START} Failed to send Duel game message for ${gameId}. Refunding.`);
        await updateUserBalance(userId, betAmountLamports, `refund_duel_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
    }
}

async function handleDuelRoll(gameId, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    const LOG_PREFIX_DUEL_ROLL = `[Duel_Roll GID:${gameId} UID:${userObj.telegram_id}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== String(userObj.telegram_id) || gameData.status !== 'player_turn_to_roll' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Duel action is outdated or not your turn.", show_alert: true });
        if (originalMessageIdFromCallback && bot && gameData?.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
            bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
        }
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: "🎲 Rolling your dice..." }).catch(()=>{});

    gameData.status = 'player_rolling'; 
    activeGames.set(gameId, gameData);
    const { chatId, playerRef, betAmount } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    
    let currentMessageText = `⚔️ **High Roller Duel!** ⚔️\n${playerRef} (Bet: *${betDisplayUSD}*) is rolling *${escapeMarkdownV2(String(DUEL_DICE_COUNT))}* dice for glory\\!`;
    if (gameData.gameMessageId && bot) {
        try { await bot.editMessageText(currentMessageText, {chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: {}}); }
        catch(e) { if (!e.message.includes("message is not modified")) console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to edit 'player rolling' message: ${e.message}`);}
    }
    await sleep(1000);

    let playerRolls = []; let playerScore = 0n; let animatedDicePlayer = [];
    for (let i = 0; i < DUEL_DICE_COUNT; i++) { 
        try { 
            const d = await bot.sendDice(String(chatId),{emoji:'🎲'}); playerRolls.push(d.dice.value); playerScore+=BigInt(d.dice.value); animatedDicePlayer.push(d.message_id); await sleep(2000); 
        } catch(e){ 
            console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to send animated dice for player, using internal. Err: ${e.message}`);
            const ir=rollDie(); playerRolls.push(ir); playerScore+=BigInt(ir); 
            await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Internal Casino Roll ${i+1}): *${escapeMarkdownV2(String(ir))}* 🎲`, {parse_mode:'MarkdownV2'}); await sleep(1000);
        }
    }
    gameData.playerRolls = playerRolls; gameData.playerScore = playerScore;
    activeGames.set(gameId, gameData);
    animatedDicePlayer.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

    currentMessageText += `\n\nYour roll: ${formatDiceRolls(playerRolls)} for a total of *${escapeMarkdownV2(String(playerScore))}*\\!`;
    currentMessageText += `\nNow, the Bot Dealer 🤖 steps up to roll their dice\\.\\.\\.`;
    if (gameData.gameMessageId && bot) { 
         try { await bot.editMessageText(currentMessageText, {chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2'}); }
         catch(e) { if (!e.message.includes("message is not modified")) console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to edit after player roll: ${e.message}`);}
    }
    await sleep(1500);

    gameData.status = 'bot_rolling'; activeGames.set(gameId, gameData);
    let botRolls = []; let botScore = 0n; let animatedDiceBot = [];
    for (let i = 0; i < DUEL_DICE_COUNT; i++) { 
        try { 
            const d = await bot.sendDice(String(chatId),{emoji:'🎲'}); botRolls.push(d.dice.value); botScore+=BigInt(d.dice.value); animatedDiceBot.push(d.message_id); await sleep(2000); 
        } catch(e){ 
            console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to send animated dice for bot, using internal. Err: ${e.message}`);
            const ir=rollDie(); botRolls.push(ir); botScore+=BigInt(ir); 
            await safeSendMessage(String(chatId), `⚙️ Bot Dealer (Internal Casino Roll ${i+1}): *${escapeMarkdownV2(String(ir))}* 🎲`, {parse_mode:'MarkdownV2'}); await sleep(1000);
        }
    }
    gameData.botRolls = botRolls; gameData.botScore = botScore; 
    gameData.status = 'game_over'; 
    activeGames.set(gameId, gameData);
    animatedDiceBot.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

    // Clear the "Bot is rolling..." text by sending the final result message
    // This is done by editing gameData.gameMessageId if possible, or sending new.

    let resultTextPart; let payoutAmount = 0n; let outcomeReasonLog = "";
    if (playerScore > botScore) { 
        resultTextPart = `🎉 *Victory is Yours, ${playerRef}!* Your superior roll of *${escapeMarkdownV2(String(playerScore))}* crushes the Bot's *${escapeMarkdownV2(String(botScore))}*\\!`; 
        payoutAmount = betAmount + betAmount; // 2x bet
        outcomeReasonLog = `won_duel:${gameId}`; 
    } else if (botScore > playerScore) { 
        resultTextPart = `💔 *Defeated in the Duel!* The Bot Dealer's roll of *${escapeMarkdownV2(String(botScore))}* outmatches your *${escapeMarkdownV2(String(playerScore))}*\\.`; 
        payoutAmount = 0n; 
        outcomeReasonLog = `lost_duel:${gameId}`; 
    } else { 
        resultTextPart = `😐 *A Stalemate! The Duel is a PUSH!* Both combatants rolled *${escapeMarkdownV2(String(playerScore))}*\\. Your wager is returned\\.`; 
        payoutAmount = betAmount; // Bet returned
        outcomeReasonLog = `push_duel:${gameId}`; 
    }
    
    let finalSummaryMessage = `⚔️ **High Roller Duel - Final Result** ⚔️\nYour Wager: *${betDisplayUSD}*\n\n` +
                              `${playerRef}'s Roll: ${formatDiceRolls(playerRolls)} \\(Total: *${escapeMarkdownV2(String(playerScore))}*\\)\n` +
                              `Bot Dealer's Roll: ${formatDiceRolls(botRolls)} \\(Total: *${escapeMarkdownV2(String(botScore))}*\\)\n\n` +
                              `${resultTextPart}`;

    let finalUserBalanceForDisplay = BigInt(userObj.balance);
    const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    if (balanceUpdate.success) { 
        finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports; 
        if (payoutAmount > betAmount && !outcomeReasonLog.startsWith('push')) { // Actual win
             finalSummaryMessage += `\nYou claim *${escapeMarkdownV2(await formatBalanceForDisplay(payoutAmount - betAmount, 'USD'))}* in profit\\!`;
        }
    } else { 
        finalSummaryMessage += `\n\n⚠️ Critical error settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Admin has been alerted\\.`; 
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Duel Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
    }
    finalSummaryMessage += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceForDisplay, 'USD'))}*\\.`;

    const postGameKeyboardDuel = createPostGameKeyboard(GAME_IDS.DUEL, betAmount);
    if (gameData.gameMessageId && bot) {
         await bot.editMessageText(finalSummaryMessage, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel })
           .catch(async (e) => {
               console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to edit final Duel message, sending new: ${e.message}`);
               await safeSendMessage(String(chatId), finalSummaryMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel });
           });
    } else {
        await safeSendMessage(String(chatId), finalSummaryMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel });
    }
    activeGames.delete(gameId);
}


// --- Greed's Ladder Game Logic ---
async function handleStartLadderCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) return;

    const LOG_PREFIX_LADDER_START = `[Ladder_Start UID:${userId} CH:${chatId}]`;
    console.log(`${LOG_PREFIX_LADDER_START} Initiating Greed's Ladder. Bet: ${betAmountLamports} lamports.`);
    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your balance is too low for a *${betDisplayUSD}* attempt at Greed's Ladder\\. You need ~*${neededDisplay}* more\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.LADDER);
    const balanceUpdateResult = await updateUserBalance(userId, -betAmountLamports, `bet_placed_ladder:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        console.error(`${LOG_PREFIX_LADDER_START} Wager placement failed: ${balanceUpdateResult.error}`);
        await safeSendMessage(chatId, `${playerRef}, your Greed's Ladder wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const gameData = { // Not stored in activeGames for long as it resolves quickly
        type: GAME_IDS.LADDER, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, rolls: [], currentSum: 0n, busted: false,
        status: 'rolling', gameMessageId: null, lastInteractionTime: Date.now()
    };
    // activeGames.set(gameId, gameData); // Optional for quick, non-interactive games

    let mainMessageText = `🪜 **Welcome to Greed's Ladder, ${playerRef}!** 🪜\nYour wager: *${betDisplayUSD}*\\.\nThe Bot Dealer will now roll *${escapeMarkdownV2(String(LADDER_ROLL_COUNT))}* dice for you\\. Watch out for the dreaded *${escapeMarkdownV2(String(LADDER_BUST_ON))}* \\- it's a BUST\\! Rolling\\.\\.\\.`;
    const sentMessage = await safeSendMessage(chatId, mainMessageText, { parse_mode: 'MarkdownV2' });
    if (sentMessage?.message_id) { gameData.gameMessageId = sentMessage.message_id; }
    
    await sleep(1500);

    let animatedDiceLadder = [];
    let rollSequenceText = "";
    for (let i = 0; i < LADDER_ROLL_COUNT; i++) {
        let rollValue;
        try { 
            const d = await bot.sendDice(String(chatId),{emoji:'🎲'}); rollValue=d.dice.value; animatedDiceLadder.push(d.message_id); await sleep(2000); 
        } catch(e){ 
            console.warn(`${LOG_PREFIX_LADDER_START} Failed to send animated dice for Ladder, using internal. Err: ${e.message}`);
            rollValue=rollDie(); 
            await safeSendMessage(String(chatId), `⚙️ Bot Dealer (Internal Casino Roll ${i+1}): *${escapeMarkdownV2(String(rollValue))}* 🎲`, {parse_mode:'MarkdownV2'}); await sleep(1000);
        }
        gameData.rolls.push(rollValue);
        rollSequenceText += `${formatDiceRolls([rollValue])} `;
        if (rollValue === LADDER_BUST_ON) {
            gameData.busted = true;
            rollSequenceText += "💥 *BUST!*";
            break; 
        }
        gameData.currentSum += BigInt(rollValue);
    }
    animatedDiceLadder.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

    let resultTextPart = ""; 
    let payoutAmount = 0n; 
    let outcomeReasonLog = "";
    let profitAmountLamports = 0n;

    if (gameData.busted) {
        resultTextPart = `💔 *Oh Dear! A ${escapeMarkdownV2(String(LADDER_BUST_ON))} Appeared!* That's a bust on Greed's Ladder\\. Your wager is lost\\.`;
        payoutAmount = 0n; // Bet already deducted
        outcomeReasonLog = `lost_ladder_bust:${gameId}`;
    } else {
        let foundPayout = false;
        for (const tier of LADDER_PAYOUTS) { // LADDER_PAYOUTS from Part 5a S1
            if (gameData.currentSum >= tier.min && gameData.currentSum <= tier.max) {
                profitAmountLamports = betAmountLamports * BigInt(tier.multiplier);
                payoutAmount = betAmountLamports + profitAmountLamports; // Bet back + profit
                resultTextPart = `✨ *${escapeMarkdownV2(tier.label)}* ✨\nYour total score of *${escapeMarkdownV2(String(gameData.currentSum))}* nets you a fantastic payout\\!`;
                if (tier.multiplier === 0) { // Push condition
                     resultTextPart = `😐 *Phew, Safe!* Your score of *${escapeMarkdownV2(String(gameData.currentSum))}* means your wager is returned\\. ${escapeMarkdownV2(tier.label)}`;
                }
                outcomeReasonLog = `won_ladder_score${gameData.currentSum}_mult${tier.multiplier}:${gameId}`;
                foundPayout = true;
                break;
            }
        }
        if (!foundPayout) { // Score below lowest payout tier that isn't a loss
            resultTextPart = `📉 *Not Quite There\\.* Your score of *${escapeMarkdownV2(String(gameData.currentSum))}* didn't reach a winning rung on the ladder this time\\. Wager lost\\.`;
            payoutAmount = 0n;
            outcomeReasonLog = `lost_ladder_lowscore${gameData.currentSum}:${gameId}`;
        }
    }
    
    mainMessageText = `🪜 **Greed's Ladder - Result** 🪜\nYour Wager: *${betDisplayUSD}*\n\nDice Rolled: ${rollSequenceText}\nTotal Score: *${escapeMarkdownV2(String(gameData.currentSum))}* ${gameData.busted ? "\\(Busted\\)" : ""}\n\n${resultTextPart}`;

    let finalUserBalanceForDisplay = BigInt(userObj.balance);
    const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    if (balanceUpdate.success) { 
        finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports; 
        if (profitAmountLamports > 0n && !gameData.busted) {
             mainMessageText += `\nYou've won *${escapeMarkdownV2(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}* in profit\\!`;
        }
    } else { 
        mainMessageText += `\n\n⚠️ Critical error settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Admin has been alerted\\.`; 
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Ladder Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
    }
    mainMessageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceForDisplay, 'USD'))}*\\.`;

    const postGameKeyboardLadder = createPostGameKeyboard(GAME_IDS.LADDER, betAmountLamports);
    if (gameData.gameMessageId && bot) {
         await bot.editMessageText(mainMessageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder })
           .catch(async (e) => {
               console.warn(`${LOG_PREFIX_LADDER_START} Failed to edit final Ladder message, sending new: ${e.message}`);
               await safeSendMessage(String(chatId), mainMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder });
           });
    } else {
        await safeSendMessage(String(chatId), mainMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder });
    }
    // activeGames.delete(gameId); // Not strictly needed if not stored for long
}


// --- Sevens Out Game Logic (Simplified Craps) ---
async function handleStartSevenOutCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) return;

    const LOG_PREFIX_S7_START = `[S7_Start UID:${userId} CH:${chatId}]`;
    console.log(`${LOG_PREFIX_S7_START} Initiating Sevens Out. Bet: ${betAmountLamports} lamports.`);
    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your balance is a bit shy for a *${betDisplayUSD}* game of Sevens Out\\. You need about *${neededDisplay}* more\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }
    
    const gameId = generateGameId(GAME_IDS.SEVEN_OUT);
    const balanceUpdateResult = await updateUserBalance(userId, -betAmountLamports, `bet_placed_s7_init:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        console.error(`${LOG_PREFIX_S7_START} Wager placement failed: ${balanceUpdateResult.error}`);
        await safeSendMessage(chatId, `${playerRef}, your Sevens Out wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const gameData = {
        type: GAME_IDS.SEVEN_OUT, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, point: null, status: 'come_out_roll', // Initial status
        gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);

    const initialMessageText = `🎲 **Sevens Out Arena!** 🎲\n\n${playerRef}, your wager: *${betDisplayUSD}*\\.\nThis is the crucial **Come Out Roll**\\! The dice are flying\\!`;
    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2' });
    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData); // Update with message ID
        await sleep(1500);
        await processSevenOutRoll(gameData); // Process the first (Come Out) roll
    } else {
        console.error(`${LOG_PREFIX_S7_START} Failed to send S7 game message for ${gameId}. Refunding.`);
        await updateUserBalance(userId, betAmountLamports, `refund_s7_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
    }
}

async function handleSevenOutRoll(gameId, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    // This handler is for when the player clicks a "Roll for Point" button
    const LOG_PREFIX_S7_PLAYER_ROLL = `[S7_PlayerRoll GID:${gameId} UID:${userObj.telegram_id}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.userId !== String(userObj.telegram_id) || gameData.status !== 'point_phase' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Sevens Out action is outdated or not your turn.", show_alert: true });
        if (originalMessageIdFromCallback && bot && gameData?.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
             bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
        }
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, {text: "🎲 Rolling for your Point..."}).catch(() => {});
    
    // Update message to show rolling for point
    const pointDisplay = escapeMarkdownV2(String(gameData.point));
    const rollingForPointMsg = `🎲 **Sevens Out - Point Phase** 🎲\n${gameData.playerRef}, your Point is *${pointDisplay}*\\. Rolling again to hit your Point before a 7 appears\\!`;
    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(rollingForPointMsg, {chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: {}});
        } catch(e) {if (!e.message.includes("message is not modified")) console.warn(`${LOG_PREFIX_S7_PLAYER_ROLL} Error editing S7 message for point roll: ${e.message}`);}
    }
    
    await sleep(1500);
    await processSevenOutRoll(gameData); // Process the roll
}

async function processSevenOutRoll(gameData) {
    const LOG_PREFIX_S7_PROCESS = `[S7_ProcessRoll GID:${gameData.gameId}]`;
    const { gameId, chatId, userId, playerRef, betAmount, userObj, gameMessageId, status } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));

    let animatedDiceS7 = [];
    let rolls = [rollDie(), rollDie()];
    const totalSum = rolls[0] + rolls[1];
    
    // Send animated dice
    for (let rollVal of rolls) {
        try {
            const d = await bot.sendDice(String(chatId), {emoji:'🎲', value:rollVal}); // Send pre-determined value
            animatedDiceS7.push(d.message_id); 
            await sleep(2000); // Let animation play
        } catch (e) {
            console.warn(`${LOG_PREFIX_S7_PROCESS} Failed to send specific animated dice, sending random. Err: ${e.message}`);
            try { // Fallback to random animated dice
                 const dRand = await bot.sendDice(String(chatId), {emoji:'🎲'}); animatedDiceS7.push(dRand.message_id); await sleep(2000);
            } catch (e2) { // Fallback to text
                 await safeSendMessage(String(chatId), `⚙️ Bot Dealer (Internal Casino Roll): *${escapeMarkdownV2(String(rollVal))}* 🎲`, {parse_mode:'MarkdownV2'}); await sleep(500);
            }
        }
    }
    animatedDiceS7.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

    let resultTextPart = ""; 
    let payoutAmount = 0n; 
    let outcomeReasonLog = "";
    let nextStatus = gameData.status; // Persist status unless changed
    let gameOver = false;
    let keyboard = null;

    if (status === 'come_out_roll') {
        resultTextPart = `The Come Out Roll is: ${formatDiceRolls(rolls)} for a total of *${escapeMarkdownV2(String(totalSum))}*\\!\n`;
        if (totalSum === 7 || totalSum === 11) {
            resultTextPart += `🎉 *NATURAL WINNER!* A ${escapeMarkdownV2(String(totalSum))} on the Come Out Roll means you win instantly\\!`;
            payoutAmount = betAmount + betAmount; // 2x bet
            outcomeReasonLog = `won_s7_natural${totalSum}:${gameId}`;
            gameOver = true;
        } else if (totalSum === 2 || totalSum === 3 || totalSum === 12) {
            resultTextPart += `💔 *CRAPS!* A ${escapeMarkdownV2(String(totalSum))} on the Come Out Roll means the house wins this round\\.`;
            payoutAmount = 0n;
            outcomeReasonLog = `lost_s7_craps${totalSum}:${gameId}`;
            gameOver = true;
        } else { // Point is established
            gameData.point = totalSum;
            nextStatus = 'point_phase';
            resultTextPart += `✨ Your *POINT* is now *${escapeMarkdownV2(String(totalSum))}*\\! You need to roll this Point again before a 7 shows up\\.`;
            keyboard = { inline_keyboard: [[{ text: `🎲 Roll for Point (${escapeMarkdownV2(String(totalSum))})!`, callback_data: `s7_roll:${gameId}` }]]};
        }
    } else if (status === 'point_phase') {
        resultTextPart = `Rolling for Point *${escapeMarkdownV2(String(gameData.point))}*\\.\\.\\. Dice show: ${formatDiceRolls(rolls)} for a total of *${escapeMarkdownV2(String(totalSum))}*\\!\n`;
        if (totalSum === gameData.point) {
            resultTextPart += `🎉 *POINT HIT!* You rolled your Point *${escapeMarkdownV2(String(gameData.point))}* again\\! You WIN\\!`;
            payoutAmount = betAmount + betAmount;
            outcomeReasonLog = `won_s7_point${gameData.point}:${gameId}`;
            gameOver = true;
        } else if (totalSum === 7) {
            resultTextPart += `💔 *SEVEN OUT!* A 7 appeared before your Point *${escapeMarkdownV2(String(gameData.point))}*\\. The house wins\\.`;
            payoutAmount = 0n;
            outcomeReasonLog = `lost_s7_seven_out_point${gameData.point}:${gameId}`;
            gameOver = true;
        } else { // Neither Point nor 7, roll again for point
            resultTextPart += `Neither your Point nor a 7\\. Roll again for *${escapeMarkdownV2(String(gameData.point))}*\\!`;
            keyboard = { inline_keyboard: [[{ text: `🎲 Roll for Point (${escapeMarkdownV2(String(gameData.point))})!`, callback_data: `s7_roll:${gameId}` }]]};
        }
    }
    gameData.status = nextStatus;
    activeGames.set(gameId, gameData); // Update status and point

    let finalMessageText = `🎲 **Sevens Out - ${status === 'come_out_roll' ? "Come Out" : "Point"} Phase** 🎲\nYour Wager: *${betDisplayUSD}*\n\n${resultTextPart}`;
    
    if (gameOver) {
        let finalUserBalanceForDisplay = BigInt(userObj.balance);
        const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
        if (balanceUpdate.success) { 
            finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports; 
            if (payoutAmount > betAmount && !outcomeReasonLog.startsWith('push')) {
                 finalMessageText += `\nYou collect *${escapeMarkdownV2(await formatBalanceForDisplay(payoutAmount - betAmount, 'USD'))}* in profit\\!`;
            }
        } else { 
            finalMessageText += `\n\n⚠️ Critical error settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Admin alerted\\.`; 
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL S7 Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
        }
        finalMessageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceForDisplay, 'USD'))}*\\.`;
        keyboard = createPostGameKeyboard(GAME_IDS.SEVEN_OUT, betAmount);
        activeGames.delete(gameId);
    }
    if(keyboard && keyboard.inline_keyboard) { // Add rules button if other buttons exist
         keyboard.inline_keyboard.push([{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.SEVEN_OUT}` }]);
    } else if (gameOver && keyboard && keyboard.inline_keyboard) {
        // createPostGameKeyboard already includes rules
    } else { // If no other buttons, just add rules (e.g. after a win/loss with no next roll)
        keyboard = {inline_keyboard: [[{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.SEVEN_OUT}` }]]};
    }


    if (gameData.gameMessageId && bot) {
         await bot.editMessageText(finalMessageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: keyboard || {} })
           .catch(async (e) => {
               if (!e.message.includes("message is not modified")) {
                    console.warn(`${LOG_PREFIX_S7_PROCESS} Failed to edit S7 message, sending new: ${e.message}`);
                    const newMsg = await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard || {} });
                    if(newMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = newMsg.message_id;
               }
           });
    } else {
        const newMsg = await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard || {} });
        if(newMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = newMsg.message_id;
    }
}


// --- Slot Fruit Frenzy Game Logic ---
async function handleStartSlotCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) return;
    
    const LOG_PREFIX_SLOT_START = `[Slot_Start UID:${userId} CH:${chatId}]`;
    console.log(`${LOG_PREFIX_SLOT_START} Initiating Slot Fruit Frenzy. Bet: ${betAmountLamports} lamports.`);
    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your balance is a bit low for a *${betDisplayUSD}* spin on Slot Frenzy\\. You need ~*${neededDisplay}* more\\.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.SLOT_FRENZY);
    const balanceUpdateResult = await updateUserBalance(userId, -betAmountLamports, `bet_placed_slot:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        console.error(`${LOG_PREFIX_SLOT_START} Wager placement failed: ${balanceUpdateResult.error}`);
        await safeSendMessage(chatId, `${playerRef}, your Slot Frenzy wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    
    // For slots, game is quick, no need to store in activeGames long term usually.
    // const gameData = { type: GAME_IDS.SLOT_FRENZY, gameId, chatId, userId, playerRef, userObj, betAmount: betAmountLamports, status: 'spinning' };

    let initialMessageText = `🎰 **Welcome to Slot Fruit Frenzy, ${playerRef}!** 🎰\nYou've placed a bet of *${betDisplayUSD}*\\. The reels are a blur of color\\! Let's see what fortune spins up for you\\!`;
    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2' });
    let gameMessageId = sentMessage?.message_id;
    
    await sleep(1000); // Pause before sending dice

    let slotResultValue; 
    let animatedSlotMessageId = null;
    try { 
        const d = await bot.sendDice(String(chatId), {emoji:'🎰'}); // Sends a dice with slot machine emoji
        slotResultValue = d.dice.value; // Value from 1 to 64
        animatedSlotMessageId = d.message_id; 
        await sleep(3000); // Let slot animation play
    } catch (e) { 
        console.warn(`${LOG_PREFIX_SLOT_START} Failed to send animated slot dice, using internal roll. Err: ${e.message}`);
        slotResultValue = Math.floor(Math.random() * 64) + 1; // Random value 1-64
        // To ensure a loss for fallback if SLOT_PAYOUTS is sparse:
        while(SLOT_PAYOUTS[slotResultValue]) { slotResultValue = Math.floor(Math.random() * 64) + 1; }
        await safeSendMessage(String(chatId), `⚙️ The casino spirits are spinning internally\\! Result determined\\.\\.\\.`, {parse_mode:'MarkdownV2'}); await sleep(1000);
    }
    if (animatedSlotMessageId && bot) { bot.deleteMessage(String(chatId), animatedSlotMessageId).catch(() => {}); }

    const outcomeDetails = SLOT_PAYOUTS[slotResultValue]; // SLOT_PAYOUTS from Part 1
    let resultTextPart = "";
    let payoutAmount = 0n;
    let outcomeReasonLog = "";
    let profitAmountLamports = 0n;

    if (outcomeDetails) { // WIN!
        profitAmountLamports = betAmountLamports * BigInt(outcomeDetails.multiplier);
        payoutAmount = betAmountLamports + profitAmountLamports; // Bet back + profit
        resultTextPart = `🎉 *${escapeMarkdownV2(outcomeDetails.label)}* 🎉\nThe reels stop at: ${escapeMarkdownV2(outcomeDetails.symbols)}\nYou've won *${escapeMarkdownV2(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}* profit\\!`;
        outcomeReasonLog = `won_slot_val${slotResultValue}_${outcomeDetails.symbols.replace(/[^A-Za-z0-9]/g, '')}:${gameId}`;
    } else { // LOSS
        payoutAmount = 0n; // Bet already deducted
        const lossSymbols = ["🍒➖BAR➖🔔", "🍋➖❌➖💎", "🔔➖BAR➖7️⃣"]; // Example losing lines
        const randomLossSymbol = lossSymbols[Math.floor(Math.random() * lossSymbols.length)];
        resultTextPart = `💔 *So Close!* The reels show: ${escapeMarkdownV2(randomLossSymbol)}\nNo win this time\\. Better luck on your next spin\\!`;
        outcomeReasonLog = `lost_slot_val${slotResultValue}:${gameId}`;
    }
    
    let finalMessageText = `🎰 **Slot Fruit Frenzy - Spin Result!** 🎰\nYour Wager: *${betDisplayUSD}*\n\n${resultTextPart}`;

    let finalUserBalanceForDisplay = BigInt(userObj.balance);
    const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    if (balanceUpdate.success) { 
        finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports; 
    } else { 
        finalMessageText += `\n\n⚠️ Critical error settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Admin alerted\\.`; 
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Slot Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmount))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
    }
    finalMessageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceForDisplay, 'USD'))}*\\.`;

    const postGameKeyboardSlot = createPostGameKeyboard(GAME_IDS.SLOT_FRENZY, betAmountLamports);
    
    if (gameMessageId && bot) { // Edit the "Reels are spinning" message
         await bot.editMessageText(finalMessageText, { chat_id: String(chatId), message_id: Number(gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot })
           .catch(async (e) => {
               console.warn(`${LOG_PREFIX_SLOT_START} Failed to edit final Slot message, sending new: ${e.message}`);
               await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot });
           });
    } else { // Fallback if initial message ID wasn't stored or failed to send
        await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot });
    }
}


console.log("Part 5c: Additional Game Logic & Handlers - Complete.");
// --- End of Part 5c ---
// --- Start of Part 6 ---
// index.js - Part 6: Main Application Logic (Initialization, Error Handling, Graceful Shutdown)
//---------------------------------------------------------------------------
console.log("Loading Part 6: Main Application Logic (Initialization, Error Handling, Graceful Shutdown)...");

// Assumes all necessary functions from previous parts are loaded and available,
// including: initializeDatabaseSchema (Part 2), pool (Part 1), bot (Part 1),
// notifyAdmin (Part 1), escapeMarkdownV2 (Part 1), safeSendMessage (Part 1),
// getSolUsdPrice (Part 1), 
// startDepositMonitoring, stopDepositMonitoring (Part P4), 
// startSweepingProcess, stopSweepingProcess (Part P4),
// setupPaymentWebhook (Part P3), // <-- Ensure this is accessible
// app (Part 1 - Express instance), PAYMENT_WEBHOOK_PORT (Part 1),
// ADMIN_USER_ID (Part 1), BOT_NAME (Part 1), BOT_VERSION (Part 1), isShuttingDown (Part 1),
// SHUTDOWN_FAIL_TIMEOUT_MS (Part 1), MAX_RETRY_POLLING_DELAY, INITIAL_RETRY_POLLING_DELAY (Part 1),
// stringifyWithBigInt (Part 1).

// --- Global Error Handlers ---

process.on('uncaughtException', async (error, origin) => { // Added origin
    console.error('🚨 UNCAUGHT EXCEPTION! Origin:', origin, 'Error:', error);
    const errorMessage = error.message || 'No message provided';
    const errorStack = error.stack || 'No stack trace available';
    // Escape dynamic content for Markdown
    const adminMessage = `🚨 *CRITICAL: Uncaught Exception* (${escapeMarkdownV2(BOT_NAME)}) 🚨\n\nBot encountered a critical error and will attempt to shut down\\. \n\n*Origin:* \`${escapeMarkdownV2(String(origin))}\`\n*Error:* \`${escapeMarkdownV2(errorMessage)}\`\n*Stack (Partial):*\n\`\`\`\n${escapeMarkdownV2(errorStack.substring(0, 500))}\n\`\`\`\nPlease check server logs immediately for full details\\.`;

    if (!isShuttingDown) {
        isShuttingDown = true; 
        console.log("Initiating shutdown due to uncaught exception...");
        if (typeof notifyAdmin === 'function') {
            await notifyAdmin(adminMessage).catch(err => console.error("Failed to notify admin about uncaught exception:", err));
        }
        await gracefulShutdown('uncaught_exception');
        // Ensure process exits after shutdown attempt, even if gracefulShutdown hangs slightly
        setTimeout(() => {
            console.error("Forcing exit after uncaught exception shutdown attempt.");
            process.exit(1);
        }, SHUTDOWN_FAIL_TIMEOUT_MS + 3000); // Give shutdown a bit more time
    } else {
        console.log("Uncaught exception occurred during an ongoing shutdown sequence. Forcing exit immediately.");
        process.exit(1); 
    }
});

process.on('unhandledRejection', async (reason, promise) => {
    console.error('🚨 UNHANDLED PROMISE REJECTION! Reason:', reason, 'At Promise:', promise);
    let reasonString = 'Unknown reason for promise rejection';
    if (reason instanceof Error) {
        reasonString = `${reason.message}${reason.stack ? `\nStack (Partial):\n${reason.stack.substring(0, 500)}` : ''}`;
    } else if (typeof reason === 'object' && reason !== null) {
        try {
            reasonString = stringifyWithBigInt(reason); // stringifyWithBigInt from Part 1
        } catch (e) {
            reasonString = "Could not stringify complex rejection reason object.";
        }
    } else if (reason) {
        reasonString = String(reason);
    }

    const adminMessage = `⚠️ *WARNING: Unhandled Promise Rejection* (${escapeMarkdownV2(BOT_NAME)}) ⚠️\n\nAn unhandled promise rejection occurred\\. This may indicate a bug or an unhandled error case in asynchronous code\\. The bot will continue running but please investigate\\.\n\n*Reason:*\n\`\`\`\n${escapeMarkdownV2(reasonString.substring(0,1000))}\n\`\`\`\nCheck logs for full details and the promise context\\.`;

    if (typeof notifyAdmin === 'function' && !isShuttingDown) { // Don't spam admin during shutdown
        await notifyAdmin(adminMessage).catch(err => console.error("Failed to notify admin about unhandled rejection:", err));
    }
});

// --- Graceful Shutdown Logic ---
let shutdownInProgressFlag = false; 
let expressServerInstance = null; // To hold the HTTP server instance for webhooks

async function gracefulShutdown(signal = 'SIGINT') {
    if (shutdownInProgressFlag) {
        console.log("Graceful shutdown already in progress. Please wait...");
        return;
    }
    shutdownInProgressFlag = true;
    isShuttingDown = true; 

    console.log(`\n🛑 Received signal: ${signal}. Initiating graceful shutdown for ${BOT_NAME}...`);
    const adminShutdownMessage = `🔌 *Bot Shutdown Initiated* 🔌\n\n${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} is now shutting down due to signal: \`${escapeMarkdownV2(signal)}\`\\. Finalizing operations\\.\\.\\.`;
    if (typeof notifyAdmin === 'function' && signal !== 'test_mode_exit' && signal !== 'initialization_error') {
        await notifyAdmin(adminShutdownMessage).catch(err => console.error("Failed to send admin shutdown initiation notification:", err));
    }

    console.log("  ⏳ Stopping Telegram bot polling...");
    if (bot && typeof bot.stopPolling === 'function' && bot.isPolling && typeof bot.isPolling === 'function' && await bot.isPolling()) {
        try {
            await bot.stopPolling({ cancel: true }); 
            console.log("  ✅ Telegram bot polling stopped.");
        } catch (e) {
            console.error("  ❌ Error stopping Telegram bot polling:", e.message);
        }
    } else {
        console.log("  ℹ️ Telegram bot polling was not active or stopPolling not available.");
    }

    if (typeof stopDepositMonitoring === 'function') { // From Part P4
        console.log("  ⏳ Stopping deposit monitoring...");
        try { await stopDepositMonitoring(); console.log("  ✅ Deposit monitoring stopped."); }
        catch(e) { console.error("  ❌ Error stopping deposit monitoring:", e.message); }
    }
    if (typeof stopSweepingProcess === 'function') { // From Part P4
        console.log("  ⏳ Stopping sweeping process...");
        try { await stopSweepingProcess(); console.log("  ✅ Sweeping process stopped."); }
        catch(e) { console.error("  ❌ Error stopping sweeping process:", e.message); }
    }
    
    // Stop PQueue instances
    const queuesToStop = { payoutProcessorQueue, depositProcessorQueue }; // From Part 1
    for (const [queueName, queueInstance] of Object.entries(queuesToStop)) {
        if (queueInstance && typeof queueInstance.onIdle === 'function' && typeof queueInstance.clear === 'function') {
            console.log(`  ⏳ Waiting for ${queueName} (Size: ${queueInstance.size}, Pending: ${queueInstance.pending}) to idle...`);
            try {
                if (queueInstance.size > 0 || queueInstance.pending > 0) {
                    await Promise.race([queueInstance.onIdle(), sleep(10000)]); // Max 10s wait for queue
                }
                queueInstance.clear(); // Clear any remaining queued items not yet started
                console.log(`  ✅ ${queueName} is idle and cleared.`);
            } catch (qError) {
                console.warn(`  ⚠️ Error or timeout waiting for ${queueName} to idle: ${qError.message}. Clearing queue.`);
                queueInstance.clear();
            }
        }
    }


    if (expressServerInstance && typeof expressServerInstance.close === 'function') {
        console.log("  ⏳ Closing Express webhook server...");
        await new Promise(resolve => expressServerInstance.close(err => {
            if (err) console.error("  ❌ Error closing Express server:", err.message);
            else console.log("  ✅ Express server closed.");
            resolve();
        }));
    } else {
         console.log("  ℹ️ Express server not running or not managed by this instance.");
    }

    console.log("  ⏳ Closing PostgreSQL pool...");
    if (pool && typeof pool.end === 'function') { // pool from Part 1
        try {
            await pool.end();
            console.log("  ✅ PostgreSQL pool closed.");
        } catch (e) {
            console.error("  ❌ Error closing PostgreSQL pool:", e.message);
        }
    } else {
        console.log("  ⚠️ PostgreSQL pool not active or .end() not available.");
    }

    console.log(`🏁 ${BOT_NAME} shutdown sequence complete. Exiting now.`);
    const finalAdminMessage = `✅ *Bot Shutdown Complete* ✅\n\n${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} has successfully shut down\\.`;
    if (typeof notifyAdmin === 'function' && signal !== 'test_mode_exit' && signal !== 'initialization_error') {
        notifyAdmin(finalAdminMessage).catch(err => console.error("Failed to send final admin shutdown notification:", err));
    }
    
    await sleep(500); // Short pause for final async ops
    process.exit(signal === 'uncaught_exception' || signal === 'initialization_error' ? 1 : 0);
}

// Signal Handlers
process.on('SIGINT', () => { if (!shutdownInProgressFlag) gracefulShutdown('SIGINT'); }); 
process.on('SIGTERM', () => { if (!shutdownInProgressFlag) gracefulShutdown('SIGTERM'); });
process.on('SIGQUIT', () => { if (!shutdownInProgressFlag) gracefulShutdown('SIGQUIT'); });

// --- Main Application Function ---
async function main() {
    console.log(`🚀🚀🚀 Starting ${BOT_NAME} v${BOT_VERSION} 🚀🚀🚀`);
    console.log(`Node.js Version: ${process.version}, System Time: ${new Date().toISOString()}`);
    const initDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 7000; // From Part 1 env defaults
    console.log(`Initialization delay: ${initDelay / 1000}s`);
    await sleep(initDelay);

    try {
        // 1. Initialize Database Schema
        console.log("⚙️ Step 1: Initializing Database Schema...");
        if (typeof initializeDatabaseSchema !== 'function') { // From Part 2
            throw new Error("FATAL: initializeDatabaseSchema function is not defined! Cannot proceed.");
        }
        await initializeDatabaseSchema();
        console.log("✅ Database schema initialized successfully.");

        // 2. Start Telegram Bot Polling & Welcome Message
        console.log("⚙️ Step 2: Connecting to Telegram & Starting Bot...");
        if (!bot || typeof bot.getMe !== 'function') { // bot from Part 1
            throw new Error("FATAL: Telegram bot instance is not correctly configured.");
        }
        
        const botInfo = await bot.getMe();
        console.log(`🤖 Bot Name: ${botInfo.first_name}, Username: @${botInfo.username}, ID: ${botInfo.id}`);
        console.log(`🔗 Start chatting with the bot: https://t.me/${botInfo.username}`);
        // Polling is started by `new TelegramBot(TOKEN, { polling: true });` in Part 1.
        // We can add listeners for polling errors here.
        bot.on('polling_error', async (error) => {
            console.error(`[Telegram Polling Error] Code: ${error.code || 'N/A'}, Message: ${error.message || String(error)}`);
            const adminMessage = `📡 *Telegram Polling Error* (${escapeMarkdownV2(BOT_NAME)}) 📡\n\nError: \`${escapeMarkdownV2(error.message || String(error))}\` \\(Code: ${escapeMarkdownV2(String(error.code || 'N/A'))}\\)\\.\nPolling may be affected or try to restart\\.`;
            if (typeof notifyAdmin === 'function' && !isShuttingDown) {
                await notifyAdmin(adminMessage).catch(err => console.error("Failed to notify admin about polling error:", err));
            }
        });
        bot.on('webhook_error', async (error) => { /* ... similar handling ... */ }); // If using webhooks for Telegram itself
        console.log("✅ Telegram Bot is online and polling for messages.");
        
        if (typeof ADMIN_USER_ID !== 'undefined' && ADMIN_USER_ID && typeof safeSendMessage === 'function') {
            await safeSendMessage(ADMIN_USER_ID, `🚀 *${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} Started Successfully* 🚀\nBot is online and operational\\. Polling for messages\\.`, {parse_mode: 'MarkdownV2'});
        }

        // 3. Initial SOL/USD Price Fetch
        console.log("⚙️ Step 3: Priming SOL/USD Price Cache...");
        if (typeof getSolUsdPrice === 'function') { // From Part 1
            try {
                const initialPrice = await getSolUsdPrice();
                console.log(`✅ Initial SOL/USD Price: $${initialPrice.toFixed(2)}`);
            } catch (priceError) {
                console.warn(`⚠️ Could not fetch initial SOL/USD price: ${priceError.message}. Price features might be affected.`);
            }
        } else {
            console.warn("⚠️ getSolUsdPrice function not defined. Price features will be unavailable.");
        }

        // 4. Start Background Payment Processes
        console.log("⚙️ Step 4: Starting Background Payment Processes...");
        if (typeof startDepositMonitoring === 'function') { // From Part P4
            startDepositMonitoring(); 
            console.log("  ▶️ Deposit monitoring process initiated.");
        } else {
            console.warn("⚠️ Deposit monitoring (startDepositMonitoring) function not defined.");
        }

        if (typeof startSweepingProcess === 'function') { // From Part P4
            startSweepingProcess(); 
            console.log("  ▶️ Address sweeping process initiated.");
        } else {
            console.warn("⚠️ Address sweeping (startSweepingProcess) function not defined.");
        }
        
        // 5. Setup and Start Payment Webhook Server (if enabled)
        if (process.env.ENABLE_PAYMENT_WEBHOOKS === 'true') {
            console.log("⚙️ Step 5: Setting up and starting Payment Webhook Server...");
            if (typeof setupPaymentWebhook === 'function' && typeof app !== 'undefined' && app !== null) { // setupPaymentWebhook from P3, app from Part 1
                const port = parseInt(process.env.PAYMENT_WEBHOOK_PORT, 10) || 3000; // PAYMENT_WEBHOOK_PORT from Part 1 env
                try {
                    setupPaymentWebhook(app); // Configure routes on the Express app
                    
                    expressServerInstance = app.listen(port, () => { // Start listening and store the server instance
                        console.log(`  ✅ Payment webhook server listening on port ${port} at path ${process.env.PAYMENT_WEBHOOK_PATH || '/webhook/solana-payments'}`);
                    });

                    expressServerInstance.on('error', (serverErr) => {
                        console.error(`  ❌ Express server error: ${serverErr.message}`, serverErr);
                        if (serverErr.code === 'EADDRINUSE') {
                            console.error(`  🚨 FATAL: Port ${port} is already in use for webhooks. Webhook server cannot start.`);
                            // Optionally notify admin and attempt graceful shutdown of other parts
                            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 Webhook Server Failed to Start 🚨\nPort \`${port}\` is already in use\\. Payment webhooks will not function\\.`, {parse_mode:'MarkdownV2'});
                            // Don't necessarily exit the whole bot if webhooks are auxiliary, but log it as critical.
                        }
                    });

                } catch (webhookError) {
                    console.error(`  ❌ Failed to set up or start payment webhook server: ${webhookError.message}`);
                }
            } else {
                console.warn("  ⚠️ Payment webhooks enabled, but setupPaymentWebhook function or Express app instance not available.");
            }
        } else {
            console.log("ℹ️ Payment webhooks are disabled (ENABLE_PAYMENT_WEBHOOKS is not 'true').");
        }

        console.log(`\n✨✨✨ ${BOT_NAME} is fully operational! Waiting for commands... ✨✨✨\n`);

    } catch (error) {
        console.error("💥💥💥 FATAL ERROR during bot initialization: 💥💥💥", error);
        const fatalAdminMessage = `🚨 *FATAL BOT INITIALIZATION ERROR* (${escapeMarkdownV2(BOT_NAME)}) 🚨\n\nFailed to start: \n*Error:* \`${escapeMarkdownV2(error.message || "Unknown error")}\`\n*Stack (Partial):*\n\`\`\`\n${escapeMarkdownV2((error.stack || String(error)).substring(0,500))}\n\`\`\`\nBot will attempt shutdown\\.`;
        if (typeof notifyAdmin === 'function' && !isShuttingDown) {
            await notifyAdmin(fatalAdminMessage).catch(err => console.error("Failed to notify admin about fatal initialization error:", err));
        }
        if (!isShuttingDown) { 
            await gracefulShutdown('initialization_error');
        }
        // Ensure process exits if gracefulShutdown doesn't (it should)
        setTimeout(() => process.exit(1), SHUTDOWN_FAIL_TIMEOUT_MS + 1000);
    }
}

// --- Run the main application ---
main();

// Note: The final "Part 6 Complete" log might print before main() fully resolves if main becomes very async early on.
// The "fully operational" log inside main() is a better indicator.
console.log("End of index.js script. Bot startup process initiated.");
// --- End of Part 6 ---
// --- Start of Part P1 ---
// index.js - Part P1: Solana Payment System - Core Utilities & Wallet Generation
//---------------------------------------------------------------------------
console.log("Loading Part P1: Solana Payment System - Core Utilities & Wallet Generation...");

// Assumes DEPOSIT_MASTER_SEED_PHRASE, bip39, derivePath, nacl, Keypair, LAMPORTS_PER_SOL,
// solanaConnection, queryDatabase, pool, MAIN_BOT_KEYPAIR, escapeMarkdownV2, stringifyWithBigInt,
// PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, PAYOUT_COMPUTE_UNIT_LIMIT,
// INITIAL_RETRY_POLLING_DELAY, MAX_RETRY_POLLING_DELAY, DEPOSIT_ADDRESS_EXPIRY_MS,
// SOL_DECIMALS, notifyAdmin, sleep, formatCurrency, getNextAddressIndexForUserDB (Part 2)
// are available.

//---------------------------------------------------------------------------
// HD Wallet & Address Generation
//---------------------------------------------------------------------------

/**
 * Creates a cryptographically safe, deterministic index from a user's Telegram ID.
 * Uses SHA256 to hash the ID, then takes a portion for the index.
 * This ensures the user's actual ID isn't directly in the derivation path visibly,
 * and provides a consistent integer for path derivation.
 * @param {string|number} userId - The user's Telegram ID.
 * @returns {number} A derived, non-hardened index number for the HD path.
 */
function createSafeUserSpecificIndex(userId) {
    // crypto should be from Part 1 (import { createHash } from 'crypto';)
    if (typeof createHash !== 'function') {
        console.error("[createSafeUserSpecificIndex] createHash (from crypto) is not available. Cannot generate safe user index.");
        // Fallback to a less ideal method if crypto is missing, though this shouldn't happen.
        // This fallback is NOT cryptographically strong for this purpose.
        let simpleHash = 0;
        const strId = String(userId);
        for (let i = 0; i < strId.length; i++) {
            simpleHash = (simpleHash << 5) - simpleHash + strId.charCodeAt(i);
            simpleHash |= 0; // Convert to 32bit integer
        }
        return Math.abs(simpleHash) % 2147483647; // Keep it within non-hardened range
    }

    const hash = createHash('sha256').update(String(userId)).digest();
    // Take the first 4 bytes of the hash and interpret as an unsigned integer.
    // Ensure it's kept positive and within the non-hardened range (0 to 2^31 - 1).
    const index = hash.readUInt32BE(0) % 2147483647; // 2^31 - 1
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
        typeof derivePath !== 'function' || typeof nacl === 'undefined' || typeof nacl.sign.keyPair.fromSeed !== 'function' ||
        typeof Keypair === 'undefined' || typeof Keypair.fromSeed !== 'function') {
        throw new Error("Dependency missing for deriveSolanaKeypair (bip39, derivePath, nacl, or Keypair).");
    }
    try {
        const seed = bip39.mnemonicToSeedSync(seedPhrase);
        const derivedSeed = derivePath(derivationPath, seed.toString('hex')).key; // Using ed25519-hd-key's derivePath
        const keypair = Keypair.fromSeed(nacl.sign.keyPair.fromSeed(derivedSeed).secretKey.slice(0, 32)); // Use nacl to get the secret key part compatible with Keypair.fromSeed
        return keypair;
    } catch (error) {
        console.error(`[deriveSolanaKeypair] Error deriving keypair for path ${derivationPath}: ${error.message}`);
        throw new Error(`Keypair derivation failed: ${error.message}`);
    }
}
console.log("[Payment Utils] deriveSolanaKeypair (for HD wallets) defined.");


/**
 * Generates a new, unique deposit address for a user and stores it in the database.
 * @param {string|number} userId - The user's Telegram ID.
 * @param {import('pg').PoolClient} [dbClient] - Optional database client if part of a transaction.
 * @returns {Promise<string|null>} The public key of the generated deposit address, or null on failure.
 */
async function generateUniqueDepositAddress(userId, dbClient = pool) {
    const LOG_PREFIX_GUDA = `[GenDepositAddr UID:${userId}]`;
    if (!DEPOSIT_MASTER_SEED_PHRASE) { // From Part 1
        console.error(`${LOG_PREFIX_GUDA} DEPOSIT_MASTER_SEED_PHRASE is not set. Cannot generate deposit addresses.`);
        if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is not set. Deposit address generation is failing.`);
        return null;
    }
    if (typeof getNextAddressIndexForUserDB !== 'function') { // From Part 2
        console.error(`${LOG_PREFIX_GUDA} getNextAddressIndexForUserDB function is not defined. Cannot generate unique address.`);
        return null;
    }

    try {
        const safeUserAccountIndex = createSafeUserSpecificIndex(userId); // User's unique "account" for HD path
        const addressIndex = await getNextAddressIndexForUserDB(userId, dbClient); // Next available index for this user

        // Standard Solana derivation path structure: m/purpose'/coin_type'/account'/change'/address_index'
        // For Solana (SLIP-0044): purpose=44', coin_type=501'
        // We'll use account' for safeUserAccountIndex, change'=0' (external chain), address_index' for sequential addresses.
        const derivationPath = `m/44'/501'/${safeUserAccountIndex}'/0'/${addressIndex}'`;
        
        const depositKeypair = deriveSolanaKeypair(DEPOSIT_MASTER_SEED_PHRASE, derivationPath);
        const depositAddress = depositKeypair.publicKey.toBase58();

        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS); // DEPOSIT_ADDRESS_EXPIRY_MS from Part 1

        const insertQuery = `
            INSERT INTO user_deposit_wallets (user_telegram_id, public_key, derivation_path, expires_at, is_active)
            VALUES ($1, $2, $3, $4, TRUE)
            RETURNING wallet_id, public_key;
        `;
        const result = await queryDatabase(insertQuery, [userId, depositAddress, derivationPath, expiresAt], dbClient);

        if (result.rows.length > 0) {
            console.log(`${LOG_PREFIX_GUDA} ✅ Successfully generated and stored new deposit address: ${depositAddress} (Path: ${derivationPath}, Expires: ${expiresAt.toISOString()})`);
            activeDepositAddresses.set(depositAddress, { userId: String(userId), expiresAt: expiresAt.getTime() }); // Update cache from Part 1
            return depositAddress;
        } else {
            console.error(`${LOG_PREFIX_GUDA} ❌ Failed to store generated deposit address ${depositAddress} in DB.`);
            throw new Error("Failed to insert deposit address into database.");
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_GUDA} ❌ Error generating unique deposit address for user ${userId}: ${error.message}`, error.stack);
        if (typeof notifyAdmin === 'function') notifyAdmin(`⚠️ Error generating deposit address for user \`${escapeMarkdownV2(String(userId))}\`: \`${escapeMarkdownV2(error.message)}\`. Check logs.`);
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
        const publicKey = new PublicKey(address); // PublicKey from @solana/web3.js
        return PublicKey.isOnCurve(publicKey.toBytes()); // More robust check
    } catch (error) {
        return false;
    }
}
console.log("[Payment Utils] isValidSolanaAddress defined.");

/**
 * Gets the SOL balance of a given Solana public key.
 * @param {string} publicKeyString - The public key string.
 * @returns {Promise<bigint|null>} The balance in lamports, or null on error.
 */
async function getSolBalance(publicKeyString) {
    const LOG_PREFIX_GSB = `[getSolBalance PK:${publicKeyString.slice(0,10)}...]`;
    if (!isValidSolanaAddress(publicKeyString)) {
        console.warn(`${LOG_PREFIX_GSB} Invalid public key provided: ${publicKeyString}`);
        return null;
    }
    try {
        const balance = await solanaConnection.getBalance(new PublicKey(publicKeyString)); // solanaConnection from Part 1
        return BigInt(balance);
    } catch (error) {
        console.error(`${LOG_PREFIX_GSB} Error fetching balance for ${publicKeyString}: ${error.message}`);
        return null;
    }
}
console.log("[Payment Utils] getSolBalance defined.");


/**
 * Sends SOL from a payer to a recipient.
 * THIS IS A CRITICAL FUNCTION - Handles actual SOL transfers.
 *
 * @param {import('@solana/web3.js').Keypair} payerKeypair - The keypair of the account sending SOL.
 * @param {string} recipientPublicKeyString - The public key string of the recipient.
 * @param {bigint} amountLamports - The amount of SOL to send, in lamports.
 * @param {string} [memoText] - Optional memo text to include in the transaction (max ~50-70 chars recommended).
 * @param {number} [priorityFeeMicroLamportsOverride] - Optional override for priority fee in micro-lamports.
 * @param {number} [computeUnitsOverride] - Optional override for compute units.
 * @returns {Promise<{success: boolean, signature?: string, error?: string, errorType?: string, blockTime?: number, feeLamports?: number}>}
 * Object indicating success, transaction signature, and optional error message/type.
 */
async function sendSol(payerKeypair, recipientPublicKeyString, amountLamports, memoText = null, priorityFeeMicroLamportsOverride = null, computeUnitsOverride = null) {
    const LOG_PREFIX_SENDSOL = `[sendSol From:${payerKeypair.publicKey.toBase58().slice(0,6)} To:${recipientPublicKeyString.slice(0,6)} Amt:${amountLamports}]`;
    
    if (!payerKeypair || typeof payerKeypair.publicKey === 'undefined' || typeof payerKeypair.secretKey === 'undefined') {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid payerKeypair provided.`);
        return { success: false, error: "Invalid payer keypair." };
    }
    if (!isValidSolanaAddress(recipientPublicKeyString)) {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid recipient public key: ${recipientPublicKeyString}`);
        return { success: false, error: "Invalid recipient address." };
    }
    if (typeof amountLamports !== 'bigint' || amountLamports <= 0n) {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid amount: ${amountLamports}. Must be a positive BigInt.`);
        return { success: false, error: "Invalid amount (must be > 0)." };
    }

    const transaction = new Transaction(); // Transaction from @solana/web3.js
    const instructions = [];

    // Compute Budget Instructions (for priority fee and compute units)
    const computeUnitLimit = computeUnitsOverride || parseInt(process.env.PAYOUT_COMPUTE_UNIT_LIMIT, 10); // PAYOUT_COMPUTE_UNIT_LIMIT from Part 1
    const effectivePriorityFeeMicroLamports = priorityFeeMicroLamportsOverride || parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10); // PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS from Part 1
    const maxPriorityFeeMicroLamports = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10); // PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS from Part 1
    
    const finalPriorityFee = Math.min(effectivePriorityFeeMicroLamports, maxPriorityFeeMicroLamports);

    if (computeUnitLimit > 0) {
        instructions.push(ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit }));
    }
    if (finalPriorityFee > 0) {
        instructions.push(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: finalPriorityFee }));
    }

    // SOL Transfer Instruction
    instructions.push(
        SystemProgram.transfer({
            fromPubkey: payerKeypair.publicKey,
            toPubkey: new PublicKey(recipientPublicKeyString),
            lamports: amountLamports,
        })
    );

    // Memo Instruction (if memoText is provided)
    if (memoText && typeof memoText === 'string' && memoText.trim().length > 0) {
        try {
            // SimpleProgram from @solana/web3.js could be an option if it's still widely supported,
            // or use the more common spl-memo program or a custom on-chain memo.
            // For simplicity here, we'll use the basic memo program structure.
            // Note: This uses a deprecated way to add memo. For production, use @solana/spl-memo.
            // However, to avoid adding new dependencies in this step, I will use the concept.
            // The actual spl-memo program ID is 'Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo'.
            // This is just a conceptual placeholder.
            // For a real implementation, you'd use the spl-memo program.
            // For this example, we'll simulate it by adding a log. For production, this needs changing.
            console.log(`${LOG_PREFIX_SENDSOL} Conceptual Memo: "${memoText.trim()}"`);
            // If using spl-memo:
            // instructions.push(
            //    createMemoInstruction(memoText.trim(), [payerKeypair.publicKey]) // from @solana/spl-memo
            // );
        } catch (memoError) {
            console.warn(`${LOG_PREFIX_SENDSOL} Could not add memo instruction: ${memoError.message}. Proceeding without memo.`);
        }
    }
    
    transaction.add(...instructions);

    let signature = null;
    let retries = 0;
    const maxRetries = parseInt(process.env.RPC_MAX_RETRIES, 10) || 3; // RPC_MAX_RETRIES from Part 1
    let retryDelayMs = parseInt(process.env.INITIAL_RETRY_POLLING_DELAY, 10) || 5000; // INITIAL_RETRY_POLLING_DELAY from Part 1
    const maxRetryDelayMs = parseInt(process.env.MAX_RETRY_POLLING_DELAY, 10) || 60000; // MAX_RETRY_POLLING_DELAY from Part 1
    const rpcCommitment = process.env.RPC_COMMITMENT || 'confirmed'; // RPC_COMMITMENT from Part 1

    while (retries < maxRetries) {
        try {
            console.log(`${LOG_PREFIX_SENDSOL} Attempt ${retries + 1}/${maxRetries}: Sending transaction...`);
            // Fetch recent blockhash for each attempt as it can expire
            const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(rpcCommitment);
            transaction.recentBlockhash = blockhash;
            transaction.feePayer = payerKeypair.publicKey; // Ensure fee payer is set

            // Sign transaction
            // transaction.sign(payerKeypair); // This signs all instructions if payerKeypair is the only signer.
                                          // If other signers are needed for specific instructions, they sign first.
                                          // For simple SOL transfer from one wallet, this is fine.

            // Send and confirm
            // Using sendAndConfirmTransaction which handles signing internally if you pass signers.
            signature = await sendAndConfirmTransaction(
                solanaConnection,
                transaction,
                [payerKeypair], // Array of signers
                {
                    commitment: rpcCommitment,
                    skipPreflight: false, // Set to true only if you are very sure about the transaction
                    preflightCommitment: rpcCommitment,
                    maxRetries: 3, // Internal retries for sendAndConfirmTransaction's confirmation part
                }
            );
            
            console.log(`${LOG_PREFIX_SENDSOL} ✅ Transaction successful! Signature: ${signature}. Commitment: ${rpcCommitment}.`);
            
            // Optional: Fetch transaction details to get fee and blockTime
            let blockTime = null;
            let feeLamports = null;
            try {
                const confirmedTx = await solanaConnection.getConfirmedTransaction(signature, rpcCommitment);
                if (confirmedTx && confirmedTx.blockTime && confirmedTx.meta) {
                    blockTime = confirmedTx.blockTime;
                    feeLamports = BigInt(confirmedTx.meta.fee);
                    console.log(`${LOG_PREFIX_SENDSOL} Tx confirmed at Block Time: ${blockTime}, Fee: ${feeLamports} lamports.`);
                }
            } catch (fetchErr) {
                console.warn(`${LOG_PREFIX_SENDSOL} Could not fetch confirmed transaction details for ${signature}: ${fetchErr.message}`);
            }
            
            return { success: true, signature, blockTime, feeLamports };

        } catch (error) {
            retries++;
            console.error(`${LOG_PREFIX_SENDSOL} ❌ Attempt ${retries}/${maxRetries} failed: ${error.message}`);
            // console.error(error.stack); // Full stack trace if needed for debugging

            if (error instanceof TransactionExpiredBlockheightExceededError) {
                console.warn(`${LOG_PREFIX_SENDSOL} Transaction expired (blockheight exceeded). Will retry with new blockhash if attempts remain.`);
            } else if (error instanceof SendTransactionError) {
                // Detailed logs for SendTransactionError
                const transactionLogs = error.logs;
                if (transactionLogs) {
                    console.error(`${LOG_PREFIX_SENDSOL} Transaction logs:\n${transactionLogs.join('\n')}`);
                    if (transactionLogs.some(log => log.includes("insufficient lamports") || log.includes("Attempt to debit an account but found no record of a prior credit."))) {
                        return { success: false, error: "Insufficient SOL to cover transaction fee or amount.", errorType: "InsufficientFundsError" };
                    }
                    if (transactionLogs.some(log => log.includes("custom program error") || log.includes("Error processing Instruction"))) {
                         return { success: false, error: `Transaction failed: Program error. Logs: ${transactionLogs.join('; ')}`, errorType: "ProgramError" };
                    }
                }
            } else if (error.message && error.message.includes("signers") && error.message.includes("Transaction was not signed by all")) {
                console.error(`${LOG_PREFIX_SENDSOL} Signing error. Ensure payerKeypair is correct and transaction structure is valid.`);
                return {success: false, error: "Transaction signing failed.", errorType: "SigningError"};
            }


            if (retries >= maxRetries) {
                console.error(`${LOG_PREFIX_SENDSOL} Max retries reached. Transaction failed permanently.`);
                return { success: false, error: `Transaction failed after ${maxRetries} retries: ${error.message}`, errorType: error.constructor.name };
            }

            console.log(`${LOG_PREFIX_SENDSOL} Retrying in ${retryDelayMs / 1000}s...`);
            await sleep(retryDelayMs); // sleep from Part 1
            retryDelayMs = Math.min(retryDelayMs * 2, maxRetryDelayMs); // Exponential backoff
        }
    }
    // Should not be reached if loop logic is correct, but as a fallback:
    return { success: false, error: "Transaction failed after all attempts." };
}
console.log("[Payment Utils] sendSol (with payerKeypair parameter, priority fees, and robust retries) defined.");


console.log("Part P1: Solana Payment System - Core Utilities & Wallet Generation - Complete.");
// --- End of Part P1 ---
// --- Start of Part P2 ---
// index.js - Part P2: Payment System Database Operations
//---------------------------------------------------------------------------
console.log("Loading Part P2: Payment System Database Operations...");

// Assumes global `pool` (Part 1), `queryDatabase` (Part 1), cache utilities (Part P1),
// `escapeMarkdownV2` (Part 1), `formatCurrency` (Part 3),
// `generateReferralCode` (Part 2), `getNextAddressIndexForUserDB` (Part 2).
// Constants like SOL_DECIMALS, LAMPORTS_PER_SOL are from Part 1.

// --- Unified User/Wallet Operations ---

/**
 * Fetches payment-system relevant details for a user.
 * @param {string|number} telegramId The user's Telegram ID.
 * @param {import('pg').PoolClient} [client=pool] Optional database client.
 * @returns {Promise<object|null>} User details or null if not found/error.
 */
async function getPaymentSystemUserDetails(telegramId, client = pool) {
    const LOG_PREFIX_GPSUD = `[getPaymentSystemUserDetails TG:${telegramId}]`;
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
        const res = await queryDatabase(query, [telegramId], client);
        if (res.rows.length > 0) {
            const details = res.rows[0];
            // Ensure numeric fields that should be BigInt are converted
            details.balance = BigInt(details.balance || '0');
            details.total_deposited_lamports = BigInt(details.total_deposited_lamports || '0');
            details.total_withdrawn_lamports = BigInt(details.total_withdrawn_lamports || '0');
            details.total_wagered_lamports = BigInt(details.total_wagered_lamports || '0');
            details.total_won_lamports = BigInt(details.total_won_lamports || '0');
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
 * @returns {Promise<{telegram_id: string, username?:string, first_name?:string} | null>} User ID and basic info or null.
 */
async function getUserByReferralCode(refCode, client = pool) {
    const LOG_PREFIX_GUBRC = `[getUserByReferralCode Code:${refCode}]`;
    if (!refCode || typeof refCode !== 'string') {
        console.warn(`${LOG_PREFIX_GUBRC} Invalid referral code provided.`);
        return null;
    }
    try {
        const result = await queryDatabase('SELECT telegram_id, username, first_name FROM users WHERE referral_code = $1', [refCode], client);
        if (result.rows.length > 0) {
            return result.rows[0];
        }
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_GUBRC} ❌ Error finding user: ${err.message}`, err.stack);
        return null;
    }
}
console.log("[DB Ops] getUserByReferralCode defined.");


// --- Unified Balance & Ledger Operations ---

/**
 * Atomically updates a user's balance and records the change in the ledger table.
 * This is the PRIMARY function for all financial transactions affecting user balance.
 * MUST be called within an active DB transaction if part of a larger multi-step operation,
 * or it will create its own transaction if `client` is the main pool.
 *
 * @param {import('pg').PoolClient} dbClient - The active database client (e.g., from await pool.connect() or passed in).
 * @param {string|number} telegramId - The user's Telegram ID.
 * @param {bigint} changeAmountLamports - Positive for credit, negative for debit.
 * @param {string} transactionType - Type for the ledger (e.g., 'deposit', 'withdrawal_fee', 'bet_placed_dice', 'win_dice', 'referral_payout').
 * @param {object} [relatedIds={}] Optional related IDs { deposit_id, withdrawal_id, game_log_id, referral_id, related_sweep_id }.
 * @param {string|null} [notes=null] Optional notes for the ledger entry.
 * @returns {Promise<{success: boolean, newBalanceLamports?: bigint, oldBalanceLamports?: bigint, ledgerId?: number, error?: string, errorCode?: string}>}
 */
async function updateUserBalanceAndLedger(dbClient, telegramId, changeAmountLamports, transactionType, relatedIds = {}, notes = null) {
    const stringUserId = String(telegramId);
    const changeAmount = BigInt(changeAmountLamports); // Ensure it's a BigInt
    const logPrefix = `[UpdateBalanceLedger UID:${stringUserId} Type:${transactionType} Amt:${changeAmount}]`;

    const relDepositId = (relatedIds?.deposit_id && Number.isInteger(relatedIds.deposit_id)) ? relatedIds.deposit_id : null;
    const relWithdrawalId = (relatedIds?.withdrawal_id && Number.isInteger(relatedIds.withdrawal_id)) ? relatedIds.withdrawal_id : null;
    const relGameLogId = (relatedIds?.game_log_id && Number.isInteger(relatedIds.game_log_id)) ? relatedIds.game_log_id : null;
    const relReferralId = (relatedIds?.referral_id && Number.isInteger(relatedIds.referral_id)) ? relatedIds.referral_id : null;
    const relSweepId = (relatedIds?.related_sweep_id && Number.isInteger(relatedIds.related_sweep_id)) ? relatedIds.related_sweep_id : null;

    try {
        // Lock the user's row for update to prevent race conditions on balance.
        const balanceRes = await dbClient.query('SELECT balance, total_deposited_lamports, total_withdrawn_lamports, total_wagered_lamports, total_won_lamports FROM users WHERE telegram_id = $1 FOR UPDATE', [stringUserId]);
        if (balanceRes.rowCount === 0) {
            console.error(`${logPrefix} ❌ User balance record not found. User must exist before balance can be updated.`);
            return { success: false, error: 'User profile not found for balance update.', errorCode: 'USER_NOT_FOUND' };
        }
        const userData = balanceRes.rows[0];
        const balanceBefore = BigInt(userData.balance);
        const balanceAfter = balanceBefore + changeAmount;

        if (balanceAfter < 0n && transactionType !== 'admin_adjustment') { // Allow admin to force negative for correction if needed
            console.warn(`${logPrefix} ⚠️ Insufficient balance. Current: ${balanceBefore}, Change: ${changeAmount}, Would be: ${balanceAfter}`);
            return { success: false, error: 'Insufficient balance for this transaction.', oldBalanceLamports: balanceBefore, errorCode: 'INSUFFICIENT_FUNDS' };
        }

        // Update aggregated totals based on transaction type
        let newTotalDeposited = BigInt(userData.total_deposited_lamports);
        let newTotalWithdrawn = BigInt(userData.total_withdrawn_lamports);
        let newTotalWagered = BigInt(userData.total_wagered_lamports);
        let newTotalWon = BigInt(userData.total_won_lamports);

        if (transactionType === 'deposit') {
            newTotalDeposited += changeAmount;
        } else if (transactionType.startsWith('withdrawal_request') || transactionType.startsWith('withdrawal_fee')) {
             // For withdrawal_request, changeAmount is negative. For withdrawal_fee, changeAmount is negative.
            newTotalWithdrawn += (-changeAmount); // Add the positive value of the withdrawal/fee
        } else if (transactionType.startsWith('bet_placed')) {
            newTotalWagered += (-changeAmount); // changeAmount is negative for bet placed
        } else if (transactionType.startsWith('win_') || transactionType.startsWith('jackpot_win')) {
            // changeAmount is positive. If it's a win, it includes original bet back + profit.
            // total_won_lamports should reflect net winnings (profit).
            // This needs careful thought: if changeAmount is (bet + profit), then profit is (changeAmount - originalBet).
            // For simplicity here, let's assume relatedIds.original_bet_amount_for_win might be passed if complex.
            // Or, record the gross amount won. If changeAmount is purely profit, then just add it.
            // Let's assume changeAmount for 'win_' types represents (bet_returned + profit).
            // If relatedIds.betAmountForWin is provided, profit = changeAmount - betAmountForWin.
            // For now, simpler: if it's a 'win' type, we assume `changeAmount` is the total credited.
            // The actual profit calculation might need the original bet amount.
            // Let's assume total_won_lamports tracks the gross amount credited from wins.
             newTotalWon += changeAmount;
        }
        // Add more conditions for other transaction types like referral commissions, etc.


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
             console.error(`${LOG_PREFIX_UUBAL} ❌ Failed to update user balance row after lock for user ${stringUserId}. This should not happen.`);
             throw new Error('Failed to update user balance row after lock.'); // This will trigger rollback
        }

        const ledgerQuery = `
            INSERT INTO ledger (user_telegram_id, transaction_type, amount_lamports, balance_before_lamports, balance_after_lamports,
                                deposit_id, withdrawal_id, game_log_id, referral_id, related_sweep_id, notes, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
            RETURNING ledger_id;
        `;
        const ledgerRes = await dbClient.query(ledgerQuery, [
            stringUserId, transactionType, changeAmount.toString(), balanceBefore.toString(), balanceAfter.toString(),
            relDepositId, relWithdrawalId, relGameLogId, relReferralId, relSweepId, notes
        ]);
        
        const ledgerId = ledgerRes.rows[0]?.ledger_id;
        console.log(`${LOG_PREFIX_UUBAL} ✅ Balance updated from ${balanceBefore} to ${balanceAfter}. Ledger entry ID: ${ledgerId} created.`);
        return { success: true, newBalanceLamports: balanceAfter, oldBalanceLamports: balanceBefore, ledgerId };

    } catch (err) {
        console.error(`${LOG_PREFIX_UUBAL} ❌ Error: ${err.message} (Code: ${err.code})`, err.stack);
        let errMsg = `Database error during balance/ledger update (Code: ${err.code || 'N/A'})`;
        if (err.message && err.message.toLowerCase().includes('violates check constraint') && err.message.toLowerCase().includes('balance')) {
            errMsg = 'Insufficient balance (check constraint violation).';
        }
        return { success: false, error: errMsg, errorCode: err.code, oldBalanceLamports: (typeof balanceBefore !== 'undefined' ? balanceBefore : undefined) };
    }
}
console.log("[DB Ops] updateUserBalanceAndLedger (with aggregated totals) defined.");


// --- Deposit Address & Deposit Operations ---

/**
 * Creates a new unique deposit address record and updates user's last deposit info.
 * This should ideally be called within a transaction if other related updates are needed.
 * @param {string|number} userId
 * @param {string} depositAddress The generated unique Solana address.
 * @param {string} derivationPath The HD derivation path used.
 * @param {Date} expiresAt Expiry timestamp for the address.
 * @param {import('pg').PoolClient} [dbClient=pool] Optional DB client.
 * @returns {Promise<{success: boolean, walletId?: number, error?: string}>}
 */
async function createDepositAddressRecordDB(userId, depositAddress, derivationPath, expiresAt, dbClient = pool) {
    const LOG_PREFIX_CDAR = `[CreateDepositAddrRec UID:${userId} Addr:${depositAddress.slice(0,6)}]`;
    let internalClient = false;
    let clientToUse = dbClient;

    if (dbClient === pool) { // If no client passed, manage transaction internally
        clientToUse = await pool.connect();
        internalClient = true;
        await clientToUse.query('BEGIN');
    }

    try {
        const insertWalletQuery = `
            INSERT INTO user_deposit_wallets (user_telegram_id, public_key, derivation_path, expires_at, is_active, created_at)
            VALUES ($1, $2, $3, $4, TRUE, NOW()) 
            RETURNING wallet_id;
        `;
        const walletRes = await clientToUse.query(insertWalletQuery, [userId, depositAddress, derivationPath, expiresAt]);

        if (!walletRes.rows[0]?.wallet_id) {
            throw new Error('Failed to insert new deposit wallet and get ID.');
        }
        const walletId = walletRes.rows[0].wallet_id;

        // Update users table with last deposit address info
        const updateUserQuery = `
            UPDATE users 
            SET last_deposit_address = $1, 
                last_deposit_address_generated_at = $2,
                updated_at = NOW()
            WHERE telegram_id = $3;
        `;
        await clientToUse.query(updateUserQuery, [depositAddress, expiresAt, userId]); // Using expiresAt as generation time for this purpose

        if (internalClient) await clientToUse.query('COMMIT');
        
        addActiveDepositAddressCache(depositAddress, String(userId), expiresAt.getTime()); // From Part P1
        console.log(`${LOG_PREFIX_CDAR} ✅ New deposit address record created (ID: ${walletId}), user table updated.`);
        return { success: true, walletId: walletId };

    } catch (err) {
        if (internalClient) await clientToUse.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_CDAR} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_CDAR} ❌ Error: ${err.message} (Code: ${err.code})`, err.stack);
        if (err.code === '23505') { // Unique constraint violation
             return { success: false, error: "This deposit address or derivation path already exists.", errorCode: err.code };
        }
        return { success: false, error: err.message, errorCode: err.code };
    } finally {
        if (internalClient && clientToUse) clientToUse.release();
    }
}
console.log("[DB Ops] createDepositAddressRecordDB (updates user's last address) defined.");

/**
 * Finds user ID and other details for a given deposit address. Checks cache first.
 * @param {string} depositAddress The deposit address (public key).
 * @returns {Promise<{userId: string, walletId: number, expiresAt: Date, derivationPath: string, isActive:boolean } | null>}
 */
async function findDepositAddressInfoDB(depositAddress) {
    const LOG_PREFIX_FDAI = `[FindDepositAddrInfo Addr:${depositAddress.slice(0,6)}]`;
    const cached = getActiveDepositAddressCache(depositAddress); // From Part P1
    if (cached && Date.now() < cached.expiresAt) {
        // To get full info like walletId, derivationPath, we still need DB query,
        // but we know it's likely active and for this userId.
        // console.log(`${LOG_PREFIX_FDAI} Cache hit for user ${cached.userId}. Verifying from DB for full details.`);
    }

    try {
        const res = await queryDatabase(
            'SELECT user_telegram_id, wallet_id, expires_at, derivation_path, is_active FROM user_deposit_wallets WHERE public_key = $1',
            [depositAddress]
        );
        if (res.rows.length > 0) {
            const data = res.rows[0];
            const isActive = data.is_active && new Date(data.expires_at).getTime() > Date.now();
            if (isActive) { // Refresh cache if active
                addActiveDepositAddressCache(depositAddress, String(data.user_telegram_id), new Date(data.expires_at).getTime());
            } else { // Remove from cache if inactive or expired
                removeActiveDepositAddressCache(depositAddress); // From Part P1
            }
            return { 
                userId: String(data.user_telegram_id), 
                walletId: data.wallet_id, 
                expiresAt: new Date(data.expires_at), 
                derivationPath: data.derivation_path, 
                isActive: isActive // Reflects current real status
            };
        }
        // console.log(`${LOG_PREFIX_FDAI} Address not found in database.`);
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_FDAI} ❌ Error finding deposit address info: ${err.message}`, err.stack);
        return null;
    }
}
console.log("[DB Ops] findDepositAddressInfoDB defined.");

/**
 * Marks a deposit address as inactive and optionally as swept.
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {number} userDepositWalletId - The ID of the `user_deposit_wallets` record.
 * @param {boolean} [swept=false] - If true, also sets swept_at and potentially balance_at_sweep.
 * @param {bigint} [balanceAtSweep=null] - Optional balance at time of sweep (if swept=true).
 * @returns {Promise<boolean>} True if updated successfully.
 */
async function markDepositAddressInactiveDB(dbClient, userDepositWalletId, swept = false, balanceAtSweep = null) {
    const LOG_PREFIX_MDAI = `[MarkDepositAddrInactive WalletID:${userDepositWalletId} Swept:${swept}]`;
    try {
        let query = 'UPDATE user_deposit_wallets SET is_active = FALSE, updated_at = NOW()';
        const params = [];
        let paramIndex = 1;

        if (swept) {
            query += `, swept_at = NOW()`;
            if (balanceAtSweep !== null && typeof balanceAtSweep === 'bigint') {
                query += `, balance_at_sweep = $${paramIndex++}`;
                params.push(balanceAtSweep.toString());
            }
        }
        query += ` WHERE wallet_id = $${paramIndex++} RETURNING public_key, is_active;`; // Ensure we only update if it was active or needs update
        params.push(userDepositWalletId);

        const res = await dbClient.query(query, params);
        if (res.rowCount > 0) {
            const updatedWallet = res.rows[0];
            removeActiveDepositAddressCache(updatedWallet.public_key); // From Part P1
            console.log(`${LOG_PREFIX_MDAI} ✅ Marked wallet ID ${userDepositWalletId} (Addr: ${updatedWallet.public_key.slice(0,6)}) as inactive/swept. New active status: ${updatedWallet.is_active}`);
            return true;
        }
        console.warn(`${LOG_PREFIX_MDAI} ⚠️ Wallet ID ${userDepositWalletId} not found or no change made (already inactive/swept?).`);
        return false;
    } catch (err) {
        console.error(`${LOG_PREFIX_MDAI} ❌ Error: ${err.message}`, err.stack);
        return false;
    }
}
console.log("[DB Ops] markDepositAddressInactiveDB defined.");

/**
 * Records a confirmed deposit transaction. Must be called within a transaction.
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
    const LOG_PREFIX_RCD = `[RecordDeposit UID:${userId} TX:${txSignature.slice(0,6)}]`;
    const query = `
        INSERT INTO deposits (user_telegram_id, user_deposit_wallet_id, deposit_address, transaction_signature, amount_lamports, source_address, block_time, confirmation_status, processed_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'confirmed', NOW(), NOW())
        ON CONFLICT (transaction_signature) DO NOTHING 
        RETURNING deposit_id;
    `;
    try {
        const res = await dbClient.query(query, [userId, userDepositWalletId, depositAddress, txSignature, amountLamports.toString(), sourceAddress, blockTime]);
        if (res.rowCount > 0 && res.rows[0].deposit_id) {
            console.log(`${LOG_PREFIX_RCD} ✅ Deposit recorded successfully. DB ID: ${res.rows[0].deposit_id}`);
            return { success: true, depositId: res.rows[0].deposit_id };
        }
        // If rowCount is 0, it means ON CONFLICT DO NOTHING was triggered. Check if it exists.
        const existing = await dbClient.query('SELECT deposit_id FROM deposits WHERE transaction_signature = $1', [txSignature]);
        if (existing.rowCount > 0) {
            console.warn(`${LOG_PREFIX_RCD} ⚠️ Deposit TX ${txSignature} already processed (DB ID: ${existing.rows[0].deposit_id}).`);
            return { success: false, error: 'Deposit already processed.', alreadyProcessed: true, depositId: existing.rows[0].deposit_id };
        }
        // This case should ideally not be reached if ON CONFLICT works as expected.
        console.error(`${LOG_PREFIX_RCD} ❌ Failed to record deposit and not a recognized duplicate. TX: ${txSignature}`);
        return { success: false, error: 'Failed to record deposit (unknown issue after conflict check).' };
    } catch(err) {
        console.error(`${LOG_PREFIX_RCD} ❌ Error recording deposit: ${err.message} (Code: ${err.code})`, err.stack);
        return { success: false, error: err.message, errorCode: err.code };
    }
}
console.log("[DB Ops] recordConfirmedDepositDB defined.");


// --- Sweep Operations ---
/**
 * Records a successful sweep transaction. Must be called within a transaction.
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {string} sourceDepositAddress - The deposit address that was swept.
 * @param {string} destinationMainAddress - The main bot wallet it was swept to.
 * @param {bigint} amountLamports - Amount swept.
 * @param {string} transactionSignature - Solana transaction signature of the sweep.
 * @returns {Promise<{success: boolean, sweepId?: number, error?: string}>}
 */
async function recordSweepTransactionDB(dbClient, sourceDepositAddress, destinationMainAddress, amountLamports, transactionSignature) {
    const LOG_PREFIX_RST = `[RecordSweepTX From:${sourceDepositAddress.slice(0,6)} To:${destinationMainAddress.slice(0,6)} TX:${transactionSignature.slice(0,6)}]`;
    const query = `
        INSERT INTO processed_sweeps (source_deposit_address, destination_main_address, amount_lamports, transaction_signature, swept_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (transaction_signature) DO UPDATE SET swept_at = NOW() -- Update timestamp if somehow re-processed
        RETURNING sweep_id;
    `;
    try {
        const res = await dbClient.query(query, [sourceDepositAddress, destinationMainAddress, amountLamports.toString(), transactionSignature]);
        if (res.rowCount > 0 && res.rows[0].sweep_id) {
            console.log(`${LOG_PREFIX_RST} ✅ Sweep transaction recorded successfully. DB ID: ${res.rows[0].sweep_id}`);
            return { success: true, sweepId: res.rows[0].sweep_id };
        }
        // This indicates an issue, possibly the ON CONFLICT DO UPDATE didn't return ID as expected, or insert failed silently before conflict.
        console.error(`${LOG_PREFIX_RST} ❌ Failed to record sweep transaction or get ID back for TX ${transactionSignature}.`);
        return { success: false, error: 'Failed to record sweep transaction or retrieve ID.' };
    } catch (err) {
        console.error(`${LOG_PREFIX_RST} ❌ Error recording sweep TX: ${err.message} (Code: ${err.code})`, err.stack);
        return { success: false, error: err.message, errorCode: err.code };
    }
}
console.log("[DB Ops] recordSweepTransactionDB defined.");


// --- Withdrawal Database Operations ---
// (Implementations will be similar in structure to deposit operations)
async function createWithdrawalRequestDB(dbClient, userId, requestedAmountLamports, feeLamports, recipientAddress, priorityFee = null, computeLimit = null) {
    const LOG_PREFIX_CWR = `[CreateWithdrawalReq UID:${userId} Addr:${recipientAddress.slice(0,6)}]`;
    const query = `
        INSERT INTO withdrawals (user_telegram_id, destination_address, amount_lamports, fee_lamports, status, priority_fee_microlamports, compute_unit_limit, requested_at)
        VALUES ($1, $2, $3, $4, 'pending_processing', $5, $6, NOW())
        RETURNING withdrawal_id;
    `;
    try {
        const res = await dbClient.query(query, [userId, recipientAddress, requestedAmountLamports.toString(), feeLamports.toString(), priorityFee, computeLimit]);
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
    `; // Only update processed_at if moving to a terminal or sent state
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
            // Convert amounts to BigInt
            details.amount_lamports = BigInt(details.amount_lamports);
            details.fee_lamports = BigInt(details.fee_lamports);
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
// (Stubs - Full implementation would mirror withdrawal operations for 'referral_payouts' table)
async function recordPendingReferralPayoutDB(dbClient, referrerUserId, refereeUserId, payoutType, payoutAmountLamports, triggeringGameLogId = null, milestoneReachedLamports = null) { 
    console.log(`[DB Ops Stub] recordPendingReferralPayoutDB called for referrer ${referrerUserId}.`);
    // INSERT into referrals (or a dedicated referral_payouts table if commission is paid separately)
    return { success: true, payoutId: Date.now() }; // Placeholder
}
async function updateReferralPayoutStatusDB(dbClient, payoutId, status, signature = null, errorMessage = null) { 
    console.log(`[DB Ops Stub] updateReferralPayoutStatusDB called for payout ${payoutId} to status ${status}.`);
    return { success: true }; // Placeholder
}
async function getReferralPayoutDetailsDB(payoutId, dbClient = pool) { 
    console.log(`[DB Ops Stub] getReferralPayoutDetailsDB called for payout ${payoutId}.`);
    return { /* mock details */ payout_id: payoutId, referrer_telegram_id: 'mockReferrer', commission_amount_lamports: '100000', status: 'pending' }; // Placeholder
}
async function getTotalReferralEarningsDB(userId, dbClient = pool) { 
    console.log(`[DB Ops Stub] getTotalReferralEarningsDB called for user ${userId}.`);
    return { total_earned_lamports: BigInt(0), total_pending_lamports: BigInt(0) }; // Placeholder
}
console.log("[DB Ops Placeholder] Referral Payout DB functions defined (as stubs).");


// --- Bet History & Leaderboard Database Operations ---
/**
 * Gets transaction history for a user from the ledger.
 * @param {string|number} userId
 * @param {number} [limit=10]
 * @param {number} [offset=0]
 * @param {string|null} [transactionTypeFilter=null] e.g., 'deposit', 'withdrawal', 'bet%', 'win%'
 * @param {import('pg').PoolClient} [client=pool]
 * @returns {Promise<Array<object>>} Array of ledger entries.
 */
async function getBetHistoryDB(userId, limit = 10, offset = 0, transactionTypeFilter = null, client = pool) {
    const LOG_PREFIX_GBH = `[GetBetHistory UID:${userId}]`;
    try {
        let query = `
            SELECT ledger_id, transaction_type, amount_lamports, balance_after_lamports, notes, created_at 
            FROM ledger 
            WHERE user_telegram_id = $1 
        `;
        const params = [userId];
        let paramIndex = 2;
        if (transactionTypeFilter) {
            query += ` AND transaction_type ILIKE $${paramIndex++}`; // ILIKE for case-insensitive, use LIKE for case-sensitive
            params.push(transactionTypeFilter); // e.g., 'bet_placed_%' or 'win_%'
        }
        query += ` ORDER BY created_at DESC LIMIT $${paramIndex++} OFFSET $${paramIndex++};`;
        params.push(limit, offset);

        const res = await queryDatabase(query, params, client);
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
console.log("[DB Ops] getBetHistoryDB (from ledger) defined.");

async function getLeaderboardDataDB(type, periodType, periodIdentifier, limit = 10, offset = 0) { 
    console.log(`[DB Ops Stub] getLeaderboardDataDB called for type ${type}.`);
    // Query aggregated data, potentially from `users` (total_wagered, total_won) or a dedicated leaderboard table.
    return []; // Placeholder
}
console.log("[DB Ops Placeholder] getLeaderboardDataDB defined (as stub).");


console.log("Part P2: Payment System Database Operations - Complete.");
// --- End of Part P2 ---
// --- Start of Part P3 ---
// index.js - Part P3: Payment System UI Handlers, Stateful Logic & Webhook Setup
//---------------------------------------------------------------------------
console.log("Loading Part P3: Payment System UI Handlers, Stateful Logic & Webhook Setup...");

// Assumes global utilities: safeSendMessage, escapeMarkdownV2, formatCurrency, formatBalanceForDisplay,
// clearUserState (defined below), bot, userStateCache, pool, getOrCreateUser,
// LAMPORTS_PER_SOL, MIN_WITHDRAWAL_LAMPORTS, WITHDRAWAL_FEE_LAMPORTS, DEPOSIT_ADDRESS_EXPIRY_MS,
// DEPOSIT_CONFIRMATION_LEVEL, BOT_NAME, stringifyWithBigInt.
// Assumes DB ops from Part P2: linkUserWallet, getUserBalance, getPaymentSystemUserDetails,
// createDepositAddressRecordDB, findDepositAddressInfoDB, getNextAddressIndexForUserDB,
// createWithdrawalRequestDB, getBetHistoryDB, getUserByReferralCode, getTotalReferralEarningsDB,
// updateUserBalanceAndLedger.
// Assumes Solana utils from Part P1: generateUniqueDepositAddress, isValidSolanaAddress.
// Assumes Cache utils from Part P1 (or direct cache access): addProcessedTxSignatureToCache, hasProcessedTxSignatureInCache.
// Assumes Payout job queuing from Part P4: addPayoutJob.
// Assumes Deposit processing from Part P4: processDepositTransaction, depositProcessorQueue.
// Assumes crypto for webhook signature validation (if used) is available (imported in Part 1).


// --- User State Management ---
// clearUserState, routeStatefulInput, handleWalletAddressInput, handleWithdrawalAmountInput
// ... (These functions remain as previously defined in Part P3) ...
function clearUserState(userId) {
    const stringUserId = String(userId);
    const state = userStateCache.get(stringUserId); 
    if (state) {
        if (state.data?.timeoutId) clearTimeout(state.data.timeoutId);
        userStateCache.delete(stringUserId);
        console.log(`[StateUtil] Cleared state for user ${stringUserId}. State was: ${state.state || state.action || 'N/A'}`);
    }
}
console.log("[State Utils] clearUserState defined.");

async function routeStatefulInput(msg, currentState) { 
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id); 
    const text = msg.text || '';
    const stateName = currentState.state || currentState.action;
    const logPrefix = `[StatefulInput UID:${userId} State:${stateName} ChatID:${chatId}]`;
    console.log(`${logPrefix} Routing input: "${text.substring(0, 30)}..."`);

    if (currentState.chatId && String(currentState.chatId) !== chatId) {
        console.warn(`${logPrefix} Stateful input received in wrong chat (${chatId}) vs expected (${currentState.chatId}). Ignoring.`);
        await safeSendMessage(chatId, "Please respond to my previous question in our direct message chat. 💬", {});
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
            clearUserState(userId);
            await safeSendMessage(chatId, "Your previous action seems to have expired or was unclear. Please try again using a command from the main menu. 🤔", { parse_mode: 'MarkdownV2' });
    }
}
console.log("[State Utils] routeStatefulInput defined.");

async function handleWalletAddressInput(msg, currentState) { 
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(msg.from.id);
    const dmChatId = String(msg.chat.id); 
    const potentialNewAddress = msg.text ? msg.text.trim() : '';
    const logPrefix = `[WalletAddrInput UID:${userId}]`;

    if (!currentState || !currentState.data || currentState.state !== 'awaiting_withdrawal_address' || dmChatId !== userId) {
        console.error(`${logPrefix} Invalid state or context for wallet address input. State:`, currentState);
        clearUserState(userId);
        await safeSendMessage(dmChatId, "⚙️ There was an issue processing your address input. Please try linking your wallet again via the `/wallet` menu.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const { originalPromptMessageId, originalGroupChatId, originalGroupMessageId } = currentState.data;
    if (originalPromptMessageId && bot) { await bot.deleteMessage(dmChatId, originalPromptMessageId).catch(() => {}); }
    clearUserState(userId); 
    const linkingMsg = await safeSendMessage(dmChatId, `🔗 Validating and attempting to link wallet: \`${escapeMarkdownV2(potentialNewAddress)}\`... Please hold on a moment.`, { parse_mode: 'MarkdownV2' });
    const displayMsgIdInDm = linkingMsg ? linkingMsg.message_id : null;
    try {
        if (!isValidSolanaAddress(potentialNewAddress)) { 
            throw new Error("The provided address has an invalid Solana address format.");
        }
        const linkResult = await linkUserWallet(userId, potentialNewAddress); 
        let feedbackText;
        const finalKeyboard = { inline_keyboard: [[{ text: '💳 Back to Wallet Menu', callback_data: 'menu:wallet' }]] };
        if (linkResult.success) {
            feedbackText = `✅ Success! ${escapeMarkdownV2(linkResult.message || `Wallet \`${potentialNewAddress}\` has been successfully linked to your account.`)}`;
            if (originalGroupChatId && originalGroupMessageId && bot) { 
                await bot.editMessageText(`${getPlayerDisplayReference(msg.from)} has successfully updated their linked wallet.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, reply_markup: {}}).catch(()=>{});
            }
        } else {
            feedbackText = `⚠️ Wallet Link Failed: \`${escapeMarkdownV2(potentialNewAddress)}\`.\n*Reason:* ${escapeMarkdownV2(linkResult.error || "Please ensure the address is valid and not already in use.")}`;
             if (originalGroupChatId && originalGroupMessageId && bot) { 
                await bot.editMessageText(`${getPlayerDisplayReference(msg.from)}, there was an issue linking your wallet. Please check my DM for details and try again.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, reply_markup: {}}).catch(()=>{});
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
             await bot.editMessageText(`${getPlayerDisplayReference(msg.from)}, there was an error processing your wallet address. Please check my DM.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, reply_markup: {}}).catch(()=>{});
        }
    }
}
console.log("[State Handler] handleWalletAddressInput defined.");

async function handleWithdrawalAmountInput(msg, currentState) { 
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(msg.from.id);
    const dmChatId = String(msg.chat.id);
    const textAmount = msg.text ? msg.text.trim() : '';
    const logPrefix = `[WithdrawAmountInput UID:${userId}]`;

    if (!currentState || !currentState.data || currentState.state !== 'awaiting_withdrawal_amount' || dmChatId !== userId ||
        !currentState.data.linkedWallet || typeof currentState.data.currentBalanceLamportsStr !== 'string') {
        console.error(`${logPrefix} Invalid state or data for withdrawal amount. State:`, currentState);
        clearUserState(userId);
        await safeSendMessage(dmChatId, "⚙️ Error: Withdrawal context lost. Please restart the withdrawal process from the `/wallet` menu.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const { linkedWallet, originalPromptMessageId, currentBalanceLamportsStr, originalGroupChatId, originalGroupMessageId } = currentState.data;
    const currentBalanceLamports = BigInt(currentBalanceLamportsStr);
    if (originalPromptMessageId && bot) { await bot.deleteMessage(dmChatId, originalPromptMessageId).catch(() => {}); }
    clearUserState(userId);
    try {
        const amountSOL = parseFloat(String(textAmount).replace(/[^0-9.]/g, ''));
        if (isNaN(amountSOL) || amountSOL <= 0) throw new Error("Invalid number format or non-positive amount. Please enter a value like `0.5` or `10`.");
        const amountLamports = BigInt(Math.floor(amountSOL * Number(LAMPORTS_PER_SOL)));
        const feeLamports = WITHDRAWAL_FEE_LAMPORTS; 
        const totalDeductionLamports = amountLamports + feeLamports;
        const minWithdrawDisplay = await formatBalanceForDisplay(MIN_WITHDRAWAL_LAMPORTS, 'SOL');
        const feeDisplay = await formatBalanceForDisplay(feeLamports, 'SOL');
        const balanceDisplay = await formatBalanceForDisplay(currentBalanceLamports, 'SOL');

        if (amountLamports < MIN_WITHDRAWAL_LAMPORTS) {
            throw new Error(`Withdrawal amount of *${escapeMarkdownV2(formatCurrency(amountLamports, 'SOL'))}* is less than the minimum of *${escapeMarkdownV2(minWithdrawDisplay)}*\\.`);
        }
        if (currentBalanceLamports < totalDeductionLamports) {
            throw new Error(`Insufficient balance\\. You need *${escapeMarkdownV2(formatCurrency(totalDeductionLamports, 'SOL'))}* \\(amount \\+ fee\\) to withdraw *${escapeMarkdownV2(formatCurrency(amountLamports, 'SOL'))}*\\. Your balance is *${escapeMarkdownV2(balanceDisplay)}*\\.`);
        }
        const confirmationText = `*Withdrawal Confirmation* ⚜️\n\n` +
                                 `Please review and confirm your withdrawal:\n\n` +
                                 `🔹 Amount to Withdraw: *${escapeMarkdownV2(formatCurrency(amountLamports, 'SOL'))}*\n` +
                                 `🔹 Withdrawal Fee: *${escapeMarkdownV2(feeDisplay)}*\n` +
                                 `🔹 Total Deducted: *${escapeMarkdownV2(formatCurrency(totalDeductionLamports, 'SOL'))}*\n` +
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
                chatId: dmChatId, 
                messageId: sentConfirmMsg.message_id, 
                data: { linkedWallet, amountLamportsStr: amountLamports.toString(), feeLamportsStr: feeLamports.toString(), originalGroupChatId, originalGroupMessageId },
                timestamp: Date.now()
            });
            if (originalGroupChatId && originalGroupMessageId && bot) {
                 await bot.editMessageText(`${getPlayerDisplayReference(msg.from)}, please check your DMs to confirm your withdrawal request.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, reply_markup: {}}).catch(()=>{});
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
            await bot.editMessageText(`${getPlayerDisplayReference(msg.from)}, there was an error with your withdrawal amount. Please check my DM.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, reply_markup: {}}).catch(()=>{});
        }
    }
}
console.log("[State Handler] handleWithdrawalAmountInput defined.");


// --- UI Command Handler Implementations ---
// handleWalletCommand, handleSetWalletCommand, handleDepositCommand, handleWithdrawCommand,
// handleReferralCommand, handleHistoryCommand, handleLeaderboardsCommand, handleMenuAction,
// handleWithdrawalConfirmation, placeBet
// ... (These functions remain as previously defined in Part P3, with privacy logic) ...
async function handleWalletCommand(msg) { 
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) { await safeSendMessage(commandChatId, "Error fetching your profile. Please try /start.", {parse_mode: 'MarkdownV2'}); return; }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId); 
    const botUsername = (await bot.getMe()).username;
    let targetChatIdForMenu = userId; 
    let messageIdToEditOrDeleteForMenu = null; 
    if (chatType !== 'private') {
        if(msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(()=>{});
        await safeSendMessage(commandChatId, `${playerRef}, I've sent your Wallet Overview to our private chat: @${escapeMarkdownV2(botUsername)} 💳 For your security, all wallet actions are handled there\\.`, { parse_mode: 'MarkdownV2' });
    }
    const loadingDmMsg = await safeSendMessage(targetChatIdForMenu, "Loading your Wallet Dashboard... ⏳", {});
    messageIdToEditOrDeleteForMenu = loadingDmMsg?.message_id;
    try {
        const userDetails = await getPaymentSystemUserDetails(userId); 
        if (!userDetails) {
            const noUserText = "😕 Could not retrieve your player profile. Please try sending `/start` to the bot first.";
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
                   `💰 Current Balance:\n   Approx\\. *${escapeMarkdownV2(balanceDisplayUSD)}*\n   SOL: *${escapeMarkdownV2(balanceDisplaySOL)}*\n\n` +
                   `🔗 Linked Withdrawal Address:\n   \`${escapedLinkedAddress}\`\n\n`;
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
            [{ text: "🤝 Referrals & Rewards", callback_data: "menu:referral" }, { text: "🏆 View Leaderboards", callback_data: "menu:leaderboards" }],
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
            await bot.editMessageText(errorText, {chat_id: targetChatIdForMenu, message_id: messageIdToEditOrDeleteForMenu, parse_mode: 'MarkdownV2'}).catch(() => {
                safeSendMessage(targetChatIdForMenu, errorText, {parse_mode: 'MarkdownV2'});
            });
        } else {
            await safeSendMessage(targetChatIdForMenu, errorText, {parse_mode: 'MarkdownV2'});
        }
    }
}
console.log("[UI Handler] handleWalletCommand defined.");

async function handleSetWalletCommand(msg, args) { 
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) return;
    const playerRef = getPlayerDisplayReference(userObject);
    const botUsername = (await bot.getMe()).username;
    clearUserState(userId);
    if (chatType !== 'private') {
        if(msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(() => {});
        const dmPrompt = `${playerRef}, for your security, please set your wallet address by sending the command \`/setwallet YOUR_ADDRESS\` directly to me in our private chat: @${escapeMarkdownV2(botUsername)} 💳`;
        await safeSendMessage(commandChatId, dmPrompt, { parse_mode: 'MarkdownV2' });
        await safeSendMessage(userId, `Hi ${playerRef}, to set or update your withdrawal wallet, please reply here with the command: \`/setwallet YOUR_SOLANA_ADDRESS\``, {parse_mode: 'MarkdownV2'});
        return;
    }
    if (args.length < 1 || !args[0].trim()) {
        await safeSendMessage(userId, `💡 To link your Solana wallet for withdrawals, please use the format: \`/setwallet YOUR_SOLANA_ADDRESS\`\nExample: \`/setwallet SoLmaNqerT3ZpPT1qS9j2kKx2o5x94s2f8u5aA3bCgD\``, { parse_mode: 'MarkdownV2' });
        return;
    }
    const potentialNewAddress = args[0].trim();
    if(msg.message_id) await bot.deleteMessage(userId, msg.message_id).catch(() => {});
    const linkingMsg = await safeSendMessage(userId, `🔗 Validating and attempting to link wallet: \`${escapeMarkdownV2(potentialNewAddress)}\`... Please hold on.`, { parse_mode: 'MarkdownV2' });
    const displayMsgIdInDm = linkingMsg ? linkingMsg.message_id : null;
    try {
        if (!isValidSolanaAddress(potentialNewAddress)) {
            throw new Error("The provided address has an invalid Solana address format.");
        }
        const linkResult = await linkUserWallet(userId, potentialNewAddress); 
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
        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(errorTextToDisplay, { chat_id: userId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '💳 Try Again (Wallet Menu)', callback_data: 'menu:wallet' }]] }});
        } else {
            await safeSendMessage(userId, errorTextToDisplay, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '💳 Try Again (Wallet Menu)', callback_data: 'menu:wallet' }]] }});
        }
    }
}
console.log("[UI Handler] handleSetWalletCommand defined.");

async function handleDepositCommand(msg, args = [], correctUserIdFromCb = null) {
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(correctUserIdFromCb || msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) { await safeSendMessage(commandChatId, "Error fetching your profile.", {parse_mode: 'MarkdownV2'}); return; }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId);
    const logPrefix = `[DepositCmd UID:${userId} Chat:${commandChatId} Type:${chatType}]`;
    const botUsername = (await bot.getMe()).username;
    if (chatType !== 'private') {
        if (msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(()=>{});
        await safeSendMessage(commandChatId, `${playerRef}, for your security and convenience, I've sent your unique deposit address to our private chat: @${escapeMarkdownV2(botUsername)} 📬 Please check your DMs.`, { parse_mode: 'MarkdownV2' });
    }
    const loadingDmMsg = await safeSendMessage(userId, "Generating your personal Solana deposit address... This may take a moment. ⚙️", {parse_mode:'MarkdownV2'});
    const loadingDmMsgId = loadingDmMsg?.message_id;
    try {
        const existingAddresses = await queryDatabase(
            "SELECT public_key, expires_at FROM user_deposit_wallets WHERE user_telegram_id = $1 AND is_active = TRUE AND expires_at > NOW() ORDER BY created_at DESC LIMIT 1",
            [userId]
        );
        let depositAddress; let expiresAt;
        if (existingAddresses.rows.length > 0) {
            depositAddress = existingAddresses.rows[0].public_key;
            expiresAt = new Date(existingAddresses.rows[0].expires_at);
        } else {
            const newAddress = await generateUniqueDepositAddress(userId); 
            if (!newAddress) {
                throw new Error("Failed to generate a new deposit address. Please try again or contact support.");
            }
            depositAddress = newAddress;
            // Fetch the expiry that was set in DB by generateUniqueDepositAddress (via createDepositAddressRecordDB)
            const newAddrDetails = await queryDatabase("SELECT expires_at FROM user_deposit_wallets WHERE public_key = $1", [depositAddress]);
            expiresAt = newAddrDetails.rows.length > 0 ? new Date(newAddrDetails.rows[0].expires_at) : new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS);
        }
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
                               `   ▫️ Send *only SOL* to this address\\.\n` +
                               `   ▫️ Do *not* send NFTs or other tokens\\.\n` +
                               `   ▫️ Deposits from exchanges may take longer to confirm\\.\n` +
                               `   ▫️ This address is *unique to you* for this deposit session\\. Do not share it\\.`;
        const keyboard = {
            inline_keyboard: [
                [{ text: "🔍 View on Solscan", url: `https://solscan.io/account/${depositAddress}` }],
                [{ text: "📱 Scan QR Code", url: qrCodeUrl }],
                [{ text: "💳 Back to Wallet", callback_data: "menu:wallet" }]
            ]
        };
        if (loadingDmMsgId) {
            await bot.editMessageText(depositMessage, {chat_id: userId, message_id: loadingDmMsgId, parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
        } else {
            await safeSendMessage(userId, depositMessage, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
        }
    } catch (error) {
        console.error(`${logPrefix} ❌ Error handling deposit command: ${error.message}`, error.stack);
        const errorText = `⚙️ Apologies, ${playerRef}, we couldn't generate a deposit address for you at this moment: \`${escapeMarkdownV2(error.message)}\`\\. Please try again shortly or contact support\\.`;
        if (loadingDmMsgId) {
            await bot.editMessageText(errorText, {chat_id: userId, message_id: loadingDmMsgId, parse_mode: 'MarkdownV2'}).catch(() => {
                safeSendMessage(userId, errorText, {parse_mode: 'MarkdownV2'});
            });
        } else {
            safeSendMessage(userId, errorText, {parse_mode: 'MarkdownV2'});
        }
    }
}
console.log("[UI Handler] handleDepositCommand defined.");

async function handleWithdrawCommand(msg, args = [], correctUserIdFromCb = null) { 
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(correctUserIdFromCb || msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) { await safeSendMessage(commandChatId, "Error fetching your profile.", {parse_mode: 'MarkdownV2'}); return; }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId);
    const botUsername = (await bot.getMe()).username;
    let originalGroupMessageId = null; 
    if (chatType !== 'private') {
        if (msg.message_id && commandChatId !== userId) {
            originalGroupMessageId = msg.message_id; 
        }
    }
    const linkedWallet = await getUserLinkedWallet(userId); 
    const balanceLamports = await getUserBalance(userId);
    if (balanceLamports === null) {
        const errText = `${playerRef}, we couldn't fetch your balance to start a withdrawal. Please try again or contact support.`;
        await safeSendMessage(userId, errText, {parse_mode:'MarkdownV2'});
        if (originalGroupMessageId) await bot.editMessageText(`${playerRef}, there was an issue fetching your balance for withdrawal. Please check DMs.`, {chat_id: commandChatId, message_id: originalGroupMessageId, reply_markup:{}}).catch(()=>{});
        return;
    }
    const minTotalNeededForWithdrawal = MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS; 
    if (!linkedWallet) {
        const noWalletText = `💸 **Withdraw SOL** 💸\n\n${playerRef}, to withdraw funds, you first need to link your personal Solana wallet address\\. You can do this by replying here with \`/setwallet YOUR_SOLANA_ADDRESS\` or using the button in the \`/wallet\` menu\\.`;
        await safeSendMessage(userId, noWalletText, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"💳 Go to Wallet Menu", callback_data:"menu:wallet"}]]}});
        if (originalGroupMessageId) await bot.editMessageText(`${playerRef}, please link a withdrawal wallet first. I've sent instructions to your DM: @${escapeMarkdownV2(botUsername)}`, {chat_id: commandChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        return;
    }
    if (balanceLamports < minTotalNeededForWithdrawal) {
        const neededDisplay = await formatBalanceForDisplay(minTotalNeededForWithdrawal, 'USD');
        const currentDisplay = await formatBalanceForDisplay(balanceLamports, 'USD');
        const lowBalanceText = `💸 **Withdraw SOL** 💸\n\n${playerRef}, your balance of approx\\. *${escapeMarkdownV2(currentDisplay)}* is too low to cover the minimum withdrawal amount plus fees \\(approx\\. *${escapeMarkdownV2(neededDisplay)}* required\\)\\.\n\nConsider playing a few more games or making a deposit\\!`;
        await safeSendMessage(userId, lowBalanceText, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"💰 Deposit SOL", callback_data:"menu:deposit"},{text:"💳 Back to Wallet", callback_data:"menu:wallet"}]]}});
        if (originalGroupMessageId) await bot.editMessageText(`${playerRef}, your balance is a bit low for a withdrawal. I've sent details to your DM: @${escapeMarkdownV2(botUsername)}`, {chat_id: commandChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        return;
    }
    const minWithdrawDisplay = await formatBalanceForDisplay(MIN_WITHDRAWAL_LAMPORTS, 'SOL');
    const feeDisplay = await formatBalanceForDisplay(WITHDRAWAL_FEE_LAMPORTS, 'SOL');
    const balanceDisplay = await formatBalanceForDisplay(balanceLamports, 'SOL');
    const promptText = `💸 **Initiate SOL Withdrawal** 💸\n\n` +
                       `Your linked withdrawal address: \`${escapeMarkdownV2(linkedWallet)}\`\n` +
                       `Your current balance: *${escapeMarkdownV2(balanceDisplay)}*\n\n` +
                       `Minimum withdrawal: *${escapeMarkdownV2(minWithdrawDisplay)}*\n` +
                       `Withdrawal fee: *${escapeMarkdownV2(feeDisplay)}*\n\n` +
                       `➡️ Please reply with the amount of *SOL* you wish to withdraw \\(e\\.g\\., \`0.5\` or \`10\`\\)\\.`;
    const sentPromptMsg = await safeSendMessage(userId, promptText, {
        parse_mode: 'MarkdownV2', 
        reply_markup: { inline_keyboard: [[{ text: '❌ Cancel Withdrawal', callback_data: 'menu:wallet' }]] }
    });
    if (sentPromptMsg?.message_id) {
        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_amount',
            chatId: userId, 
            messageId: sentPromptMsg.message_id, 
            data: { linkedWallet, currentBalanceLamportsStr: balanceLamports.toString(), originalGroupChatId: (chatType !== 'private' ? commandChatId : null), originalGroupMessageId },
            timestamp: Date.now()
        });
        if (originalGroupMessageId) { 
            await bot.editMessageText(`${playerRef}, please check your DMs (@${escapeMarkdownV2(botUsername)}) to specify your withdrawal amount. 💸`, {chat_id: commandChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        }
    } else {
        await safeSendMessage(userId, "⚙️ Could not start withdrawal process due to an error sending prompt. Please try again.", {parse_mode:'MarkdownV2'});
        if (originalGroupMessageId) await bot.editMessageText(`${playerRef}, an error occurred initiating withdrawal. Please try \`/withdraw\` again in DMs.`, {chat_id: commandChatId, message_id: originalGroupMessageId, reply_markup:{}}).catch(()=>{});
    }
}
console.log("[UI Handler] handleWithdrawCommand defined.");

async function handleReferralCommand(msg) { 
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) return;
    const playerRef = getPlayerDisplayReference(user);
    const botUsername = (await bot.getMe()).username;
    let referralCode = user.referral_code;
    if (!referralCode) { 
        referralCode = generateReferralCode();
        await queryDatabase("UPDATE users SET referral_code = $1 WHERE telegram_id = $2", [referralCode, userId]);
    }
    const referralLink = `https://t.me/${botUsername}?start=ref_${referralCode}`;
    let messageText = `🤝 *Your Referral Zone, ${playerRef}!*\n\n` +
                      `Invite friends to ${escapeMarkdownV2(BOT_NAME)} and earn rewards\\!\n\n` +
                      `🔗 Your Unique Referral Link:\n\`${escapeMarkdownV2(referralLink)}\`\n_(Tap to copy or share)_ \n\n` +
                      `Share this link with friends\\. When they join and play, you could earn commissions\\!`;
    const earnings = await getTotalReferralEarningsDB(userId); 
    const totalEarnedDisplay = await formatBalanceForDisplay(earnings.total_earned_lamports, 'USD');
    const pendingDisplay = await formatBalanceForDisplay(earnings.total_pending_lamports, 'USD');
    messageText += `\n\n*Your Referral Stats:*\n` +
                   `▫️ Total Earned & Paid: *${escapeMarkdownV2(totalEarnedDisplay)}*\n` +
                   `▫️ Pending Payouts: *${escapeMarkdownV2(pendingDisplay)}*\n\n` +
                   `_(Payouts are processed automatically to your linked wallet once they meet a minimum threshold or periodically)_`;
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
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) return;
    const playerRef = getPlayerDisplayReference(user);
    const botUsername = (await bot.getMe()).username;
    if (chatType !== 'private') {
        if(msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(()=>{});
        await safeSendMessage(commandChatId, `${playerRef}, your transaction history has been sent to our private chat: @${escapeMarkdownV2(botUsername)} 📜`, { parse_mode: 'MarkdownV2' });
    }
    const loadingDmMsg = await safeSendMessage(userId, "Fetching your transaction history... ⏳ This might take a moment.", {parse_mode:'MarkdownV2'});
    const loadingDmMsgId = loadingDmMsg?.message_id;
    try {
        const historyEntries = await getBetHistoryDB(userId, 15); 
        let historyText = `📜 *Your Recent Casino Activity, ${playerRef}:*\n\n`;
        if (historyEntries.length === 0) {
            historyText += "You have no recorded transactions yet\\. Time to make some moves\\!";
        } else {
            for (const entry of historyEntries) {
                const date = new Date(entry.created_at).toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false });
                const amountDisplay = await formatBalanceForDisplay(entry.amount_lamports, 'SOL'); 
                const typeDisplay = escapeMarkdownV2(entry.transaction_type.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
                const sign = BigInt(entry.amount_lamports) >= 0n ? '+' : '';
                historyText += `🗓️ \`${escapeMarkdownV2(date)}\` \\| ${typeDisplay}\n` +
                               `   Amount: *${sign}${escapeMarkdownV2(amountDisplay)}*\n` +
                               `${entry.notes ? `   Notes: _${escapeMarkdownV2(entry.notes.substring(0,50))}${entry.notes.length > 50 ? '...' : ''}_\n` : ''}` +
                               `   Balance After: *${escapeMarkdownV2(await formatBalanceForDisplay(entry.balance_after_lamports, 'USD'))}*\n\n`;
            }
        }
        historyText += `\n_Displaying up to 15 most recent transactions\\._`;
        const keyboard = {inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]};
        if(loadingDmMsgId) await bot.editMessageText(historyText, {chat_id: userId, message_id: loadingDmMsgId, parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview:true});
        else await safeSendMessage(userId, historyText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview:true });
    } catch (error) {
        console.error(`[HistoryCmd UID:${userId}] Error fetching history: ${error.message}`);
        const errText = "⚙️ Sorry, we couldn't fetch your transaction history right now. Please try again later.";
        if(loadingDmMsgId) await bot.editMessageText(errText, {chat_id: userId, message_id: loadingDmMsgId, parse_mode: 'MarkdownV2'});
        else await safeSendMessage(userId, errText, {parse_mode: 'MarkdownV2'});
    }
}
console.log("[UI Handler] handleHistoryCommand defined.");

async function handleLeaderboardsCommand(msg, args) { 
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id); 
    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) return;
    const playerRef = getPlayerDisplayReference(user);
    const type = args[0] || 'overall_wagered'; 
    const leaderboardMessage = `🏆 **Casino Leaderboards** 🏆 \\- Coming Soon\\!\n\nHey ${playerRef}, our high-score tables for categories like *${escapeMarkdownV2(type.replace("_"," "))}* are currently under construction\\. Check back soon to see who's ruling the casino floor\\!`;
    await safeSendMessage(chatId, leaderboardMessage, { parse_mode: 'MarkdownV2' });
}
console.log("[UI Handler] handleLeaderboardsCommand (placeholder) defined.");

async function handleMenuAction(userId, originalChatId, originalMessageId, menuType, params = [], isFromCallback = true, originalChatType = 'private') {
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const logPrefix = `[MenuAction UID:${userId} Type:${menuType} OrigChat:${originalChatId}]`;
    const userObject = await getOrCreateUser(userId); 
    if(!userObject) { await safeSendMessage(originalChatId, "Could not fetch your profile for menu action.", {}); return; }
    const botUsername = (await bot.getMe()).username;
    let targetChatIdForAction = userId; 
    let messageIdForEditing = null;    
    let isGroupActionRedirect = false;
    const sensitiveMenuTypes = ['deposit', 'quick_deposit', 'withdraw', 'history', 'link_wallet_prompt', 'referral'];
    if ((originalChatType === 'group' || originalChatType === 'supergroup') && sensitiveMenuTypes.includes(menuType)) {
        isGroupActionRedirect = true;
        if (originalMessageId && bot) {
            await bot.editMessageText(
                `${getPlayerDisplayReference(userObject)}, for your privacy, please continue this action in our direct message. I've sent you a prompt there: @${escapeMarkdownV2(botUsername)}`,
                { chat_id: originalChatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {
                     inline_keyboard: [[{text: `📬 Open DM with @${botUsername}`, url: `https://t.me/${botUsername}?start=menu_${menuType}`}]]
                }}
            ).catch(e => {if(!e.message.includes("message is not modified")) console.warn(`${logPrefix} Failed to edit group msg for redirect: ${e.message}`)});
        }
    } else if (originalChatType === 'private') {
        targetChatIdForAction = originalChatId; 
        messageIdForEditing = originalMessageId; 
    }
    const actionMsgContext = {
        from: userObject, 
        chat: { id: targetChatIdForAction, type: 'private' }, 
        message_id: messageIdForEditing, 
        originalGroupChatInfo: isGroupActionRedirect ? { chatId: originalChatId, messageId: originalMessageId } : null
    };
    switch(menuType) {
        case 'wallet': await handleWalletCommand(actionMsgContext); break; 
        case 'deposit': case 'quick_deposit': await handleDepositCommand(actionMsgContext); break;
        case 'withdraw': await handleWithdrawCommand(actionMsgContext); break;
        case 'referral': await handleReferralCommand(actionMsgContext); break;
        case 'history': await handleHistoryCommand(actionMsgContext); break;
        case 'leaderboards': 
            actionMsgContext.chat.id = originalChatId; 
            actionMsgContext.chat.type = originalChatType;
            actionMsgContext.message_id = originalMessageId; 
            await handleLeaderboardsCommand(actionMsgContext, params); 
            break;
        case 'link_wallet_prompt': 
            clearUserState(userId); 
            const promptText = `🔗 *Link/Update Your Withdrawal Wallet*\n\nPlease reply to this message with your personal Solana wallet address where you'd like to receive withdrawals\\. Ensure it's correct as transactions are irreversible\\.\n\nExample: \`SoLmaNqerT3ZpPT1qS9j2kKx2o5x94s2f8u5aA3bCgD\``;
            const kbd = { inline_keyboard: [ [{ text: '❌ Cancel & Back to Wallet', callback_data: 'menu:wallet' }] ] };
            const sentDmPrompt = await safeSendMessage(userId, promptText, { parse_mode: 'MarkdownV2', reply_markup: kbd });
            if (sentDmPrompt?.message_id) {
                userStateCache.set(userId, {
                    state: 'awaiting_withdrawal_address', 
                    chatId: userId, 
                    messageId: sentDmPrompt.message_id, 
                    data: { 
                        breadcrumb: "Link Solana Wallet", 
                        originalPromptMessageId: sentDmPrompt.message_id,
                        originalGroupChatId: isGroupActionRedirect ? originalChatId : null,
                        originalGroupMessageId: isGroupActionRedirect ? originalMessageId : null
                    },
                    timestamp: Date.now()
                });
            }
            break;
        case 'main': 
            actionMsgContext.chat.id = targetChatIdForAction; 
            actionMsgContext.message_id = messageIdForEditing;
            if (messageIdForEditing && targetChatIdForAction === userId) await bot.deleteMessage(targetChatIdForAction, messageIdForEditing).catch(()=>{}); // Delete previous menu in DM
            await handleHelpCommand(actionMsgContext); // handleHelpCommand needs to be adapted to take actionMsgContext
            break;
        default: 
            await safeSendMessage(userId, `❓ Unrecognized menu option: \`${escapeMarkdownV2(menuType)}\`\\. Please try again or use \`/help\`\\.`, {parse_mode:'MarkdownV2'});
    }
}
console.log("[UI Handler] handleMenuAction (with privacy awareness) defined.");

async function handleWithdrawalConfirmation(userId, dmChatId, confirmationMessageIdInDm, recipientAddress, amountLamportsStr) {
    // ... (Implementation from previous Part P3, remains unchanged) ...
    const logPrefix = `[WithdrawConfirm UID:${userId}]`;
    const currentState = userStateCache.get(userId); 
    const originalGroupChatId = currentState?.data?.originalGroupChatId;
    const originalGroupMessageId = currentState?.data?.originalGroupMessageId;
    clearUserState(userId); 
    const amountLamports = BigInt(amountLamportsStr);
    const feeLamports = WITHDRAWAL_FEE_LAMPORTS; 
    const totalDeduction = amountLamports + feeLamports;
    const userObjForNotif = await getOrCreateUser(userId);
    const playerRef = getPlayerDisplayReference(userObjForNotif); 
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const userDetails = await getPaymentSystemUserDetails(userId, client); 
        if (!userDetails || BigInt(userDetails.balance) < totalDeduction) {
            throw new Error(`Insufficient balance. Current: ${userDetails ? formatCurrency(BigInt(userDetails.balance), 'SOL') : 'N/A'}, Needed: ${formatCurrency(totalDeduction, 'SOL')}.`);
        }
        const wdReq = await createWithdrawalRequestDB(client, userId, amountLamports, feeLamports, recipientAddress); 
        if (!wdReq.success || !wdReq.withdrawalId) {
            throw new Error(wdReq.error || "Failed to create database withdrawal request.");
        }
        const balUpdate = await updateUserBalanceAndLedger( 
            client, userId, -totalDeduction, 
            'withdrawal_request', 
            { withdrawal_id: wdReq.withdrawalId }, 
            `Withdrawal to ${recipientAddress.slice(0,6)}...${recipientAddress.slice(-4)}`
        );
        if (!balUpdate.success) {
            throw new Error(balUpdate.error || "Failed to deduct balance for withdrawal.");
        }
        await client.query('COMMIT');
        if (typeof addPayoutJob === 'function') { 
            await addPayoutJob({ type: 'payout_withdrawal', withdrawalId: wdReq.withdrawalId, userId });
            const successMsgDm = `✅ *Withdrawal Queued!* Your request to withdraw *${escapeMarkdownV2(formatCurrency(amountLamports, 'SOL'))}* to \`${escapeMarkdownV2(recipientAddress)}\` is now in the payout queue\\. You'll be notified once it's processed\\.`;
            await bot.editMessageText(successMsgDm, {chat_id: dmChatId, message_id: confirmationMessageIdInDm, parse_mode:'MarkdownV2', reply_markup:{}});
            if (originalGroupChatId && originalGroupMessageId && bot) {
                 await bot.editMessageText(`${playerRef}'s withdrawal request has been queued successfully. Details in DM.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
            }
        } else {
            throw new Error("Payout processing system is unavailable. Please contact support.");
        }
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error: ${rbErr.message}`));
        console.error(`${logPrefix} ❌ Error processing withdrawal confirmation: ${e.message}`, e.stack);
        const errorMsgDm = `⚠️ *Withdrawal Failed:*\n${escapeMarkdownV2(e.message)}\n\nPlease try again or contact support if the issue persists\\.`;
        if(confirmationMessageIdInDm && bot) await bot.editMessageText(errorMsgDm, {chat_id: dmChatId, message_id: confirmationMessageIdInDm, parse_mode:'MarkdownV2', reply_markup:{ inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]}}).catch(()=>{});
        else await safeSendMessage(dmChatId, errorMsgDm, {parse_mode:'MarkdownV2', reply_markup:{ inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]}});
        if (originalGroupChatId && originalGroupMessageId && bot) {
            await bot.editMessageText(`${playerRef}, there was an error processing your withdrawal. Please check your DMs.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        }
    } finally {
        if (client) client.release();
    }
}
console.log("[UI Handler] handleWithdrawalConfirmation defined.");

async function placeBet(userId, chatId, gameKey, betDetails, betAmountLamports) {
    console.log(`[placeBet Placeholder] User: ${userId}, Game: ${gameKey}, Amount: ${betAmountLamports}.`);
    return { success: false, error: "Generic placeBet not fully implemented; game handlers manage bets." };
}
console.log("[UI Handler] placeBet (conceptual placeholder) defined.");


// --- Webhook Setup Function ---
/**
 * Configures the Express app to handle incoming payment webhooks.
 * The actual processing logic (parsing, DB ops, queueing) is mostly in Part P4 (processDepositTransaction).
 * @param {import('express').Application} expressAppInstance - The Express application.
 */
function setupPaymentWebhook(expressAppInstance) {
    const logPrefix = '[SetupWebhook]';
    if (!expressAppInstance) {
        console.error(`${logPrefix} 🚨 Express app instance not provided. Cannot set up webhook routes.`);
        return;
    }

    const paymentWebhookPath = process.env.PAYMENT_WEBHOOK_PATH || '/webhook/solana-payments'; // From Part 1 env
    const PAYMENT_WEBHOOK_SECRET = process.env.PAYMENT_WEBHOOK_SECRET; // From Part 1 env for signature validation

    console.log(`${logPrefix} 📡 Configuring webhook endpoint at ${paymentWebhookPath}`);

    expressAppInstance.post(paymentWebhookPath, async (req, res) => {
        const webhookLogPrefix = `[PaymentWebhook ${paymentWebhookPath}]`;
        const signatureFromHeader = req.headers['x-signature'] || req.headers['X-Signature'] || req.headers['helius-signature']; // Common headers

        if (PAYMENT_WEBHOOK_SECRET) {
            // Implement robust signature validation here using req.rawBody (available due to middleware in Part 1)
            // This is a conceptual placeholder. Your actual validation depends on the webhook provider.
            // Example using a simple HMAC SHA256 (replace with provider's method):
            // try {
            //     const calculatedSignature = crypto.createHmac('sha256', PAYMENT_WEBHOOK_SECRET)
            //                                     .update(req.rawBody) // req.rawBody needs to be enabled via express.json middleware
            //                                     .digest('hex');
            //     if (calculatedSignature !== signatureFromHeader) {
            //        console.warn(`${webhookLogPrefix} ⚠️ Invalid webhook signature. Header: ${signatureFromHeader}. Calc: ${calculatedSignature}`);
            //        return res.status(401).send('Unauthorized: Invalid signature');
            //     }
            //     console.log(`${webhookLogPrefix} ✅ Webhook signature validated.`);
            // } catch (sigError) {
            //     console.error(`${webhookLogPrefix} ❌ Error during signature validation: ${sigError.message}`);
            //     return res.status(500).send('Error during signature validation');
            // }
            if(!signatureFromHeader) console.warn(`${webhookLogPrefix} Webhook secret is set, but no signature header found. For production, this should be an error.`);
            else console.log(`${webhookLogPrefix} Received signature: ${signatureFromHeader}. Implement validation for production.`);
        }

        console.log(`${webhookLogPrefix} Received POST. Body (preview): ${JSON.stringify(req.body).substring(0,200)}...`);
        
        try {
            const payload = req.body; 
            let relevantTransactions = [];

            // --- Adapt this payload parsing based on your ACTUAL webhook provider (Helius, Shyft, QuickNode, etc.) ---
            if (Array.isArray(payload)) { // Example: Helius often sends an array of events
                payload.forEach(event => {
                    if (event.type === "TRANSFER" && event.transaction?.signature && event.tokenTransfers) {
                        event.tokenTransfers.forEach(transfer => {
                            // Check for native SOL transfer (mint address for SOL)
                            if (transfer.toUserAccount && transfer.mint === "So11111111111111111111111111111111111111112") { 
                                console.log(`${webhookLogPrefix} Helius-style SOL transfer found: To ${transfer.toUserAccount}, Amt: ${transfer.tokenAmount}`);
                                relevantTransactions.push({
                                    signature: event.transaction.signature,
                                    depositToAddress: transfer.toUserAccount,
                                    // Amount in webhook might be in SOL, convert to lamports
                                    // Assuming 'tokenAmount' is in SOL for this example. Adjust if it's lamports.
                                    // amountLamports: BigInt(Math.floor(parseFloat(transfer.tokenAmount) * LAMPORTS_PER_SOL)) 
                                    // For now, we let processDepositTransaction determine amount from chain.
                                });
                            }
                        });
                    } else if (event.signature && event.type === "NATIVE_TRANSFER" && event.accountData) { // Another possible Helius format
                         event.accountData.forEach(acc => {
                            if(acc.account === MAIN_BOT_KEYPAIR.publicKey.toBase58()) { // Or if it's to a known deposit address
                                // This structure might require more complex parsing to find the deposit.
                                // For now, focusing on simple tokenTransfers for SOL.
                            }
                         });
                    }
                });
            } else if (payload.txHash && payload.to && payload.value && payload.tokenSymbol === 'SOL') { // Example for a different provider
                 console.log(`${webhookLogPrefix} Generic SOL transfer found: To ${payload.to}, Value: ${payload.value}`);
                 relevantTransactions.push({
                    signature: payload.txHash,
                    depositToAddress: payload.to,
                    // amountLamports: BigInt(payload.value) // Assuming value is in lamports
                });
            }
            // --- End of provider-specific payload parsing ---


            if (relevantTransactions.length === 0) {
                 console.log(`${webhookLogPrefix} No relevant SOL transfer transactions identified in webhook payload.`);
                 return res.status(200).send('Webhook received; no actionable SOL transfer data.');
            }

            for (const txInfo of relevantTransactions) {
                const { signature, depositToAddress } = txInfo;
                if (!signature || !depositToAddress) {
                    console.warn(`${webhookLogPrefix} Webhook tx info missing signature or depositToAddress. Skipping.`);
                    continue; 
                }

                // Use cache utility from Part P1 (or main Part 1 if global)
                if (!hasProcessedTxSignatureInCache(signature)) { 
                    const addrInfo = await findDepositAddressInfoDB(depositToAddress); // From Part P2
                    // Ensure addrInfo.isActive also checks expiry implicitly or explicitly
                    if (addrInfo && addrInfo.isActive) { 
                        console.log(`${webhookLogPrefix} ✅ Valid webhook for active address ${depositToAddress}. Queuing TX: ${signature} for User: ${addrInfo.userId}`);
                        // processDepositTransaction from Part P4, depositProcessorQueue from Part 1
                        depositProcessorQueue.add(() => processDepositTransaction(signature, depositToAddress, addrInfo.walletId, addrInfo.userId));
                        addProcessedTxSignatureToCache(signature); // Add to cache
                    } else {
                        console.warn(`${webhookLogPrefix} ⚠️ Webhook for inactive/expired/unknown address ${depositToAddress}. TX ${signature}. AddrInfo:`, stringifyWithBigInt(addrInfo));
                    }
                } else {
                    console.log(`${webhookLogPrefix} ℹ️ TX ${signature} already processed or seen (via cache). Ignoring webhook notification.`);
                }
            }
            res.status(200).send('Webhook data queued for processing');
        } catch (error) {
            console.error(`❌ ${webhookLogPrefix} Error processing webhook payload:`, error);
            res.status(500).send('Internal Server Error during webhook processing');
        }
    });

    console.log(`${logPrefix} ✅ Webhook endpoint ${paymentWebhookPath} configured successfully on Express app.`);
}
// Make sure to export setupPaymentWebhook if Part 6 needs to import it.
// If in same file scope (monolith), no export/import needed if called after definition.
console.log("[UI Handler] setupPaymentWebhook function defined.");


console.log("Part P3: Payment System UI Handlers, Stateful Logic & Webhook Setup - Complete.");
// --- End of Part P3 ---
// --- Start of Part P4 ---
// index.js - Part P4: Payment System Background Tasks & Webhook Handling
//---------------------------------------------------------------------------
console.log("Loading Part P4: Payment System Background Tasks & Webhook Handling...");

// Assumes global constants (DEPOSIT_MONITOR_INTERVAL_MS, SWEEP_INTERVAL_MS, etc.),
// Solana connection (`solanaConnection`), DB pool (`pool`), processing queues (`depositProcessorQueue`, `payoutProcessorQueue`),
// and various utility/DB functions from Part P1, P2, P3 are available.
// Keypairs like MAIN_BOT_KEYPAIR, REFERRAL_PAYOUT_KEYPAIR are from Part 1.
// Functions like notifyAdmin, safeSendMessage, escapeMarkdownV2, formatCurrency, stringifyWithBigInt,
// hasProcessedDepositTx, addProcessedDepositTx, analyzeTransactionAmounts, getKeypairFromPath, sendSol (modified),
// recordConfirmedDepositDB, markDepositAddressInactiveDB, updateUserBalanceAndLedger, findDepositAddressInfoDB,
// getWithdrawalDetailsDB, updateWithdrawalStatusDB, getReferralPayoutDetailsDB, updateReferralPayoutStatusDB,
// getPaymentSystemUserDetails, recordSweepTransactionDB are assumed available.

// --- Global State for Background Task Control ---
let depositMonitorIntervalId = null;
let sweepIntervalId = null;
// let leaderboardManagerIntervalId = null; // If leaderboard updates are periodic

monitorDepositsPolling.isRunning = false; // Prevent overlapping runs
sweepDepositAddresses.isRunning = false; // Prevent overlapping runs


// --- Deposit Monitoring Logic ---

function startDepositMonitoring() {
    let intervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs < 5000) { // Minimum 5 seconds
        intervalMs = 20000; // Default from PAYMENT_ENV_DEFAULTS (Part 1)
        console.warn(`[DepositMonitor] Invalid DEPOSIT_MONITOR_INTERVAL_MS, using default ${intervalMs}ms.`);
    }
    
    if (depositMonitorIntervalId) { // Clear existing interval if any
        clearInterval(depositMonitorIntervalId);
        console.log('🔄 [DepositMonitor] Restarting deposit monitor...');
    } else {
        console.log(`⚙️ [DepositMonitor] Starting Deposit Monitor (Polling Interval: ${intervalMs / 1000}s)...`);
    }
    
    const initialDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 3000;
    console.log(`[DepositMonitor] Scheduling first monitor run in ${initialDelay/1000}s...`);

    setTimeout(() => { // Initial run after a delay
        console.log(`[DepositMonitor] Executing first monitor run...`);
        monitorDepositsPolling().catch(err => console.error("❌ [Initial Deposit Monitor Run] Error:", err.message, err.stack));
        
        // Set up recurring interval
        depositMonitorIntervalId = setInterval(monitorDepositsPolling, intervalMs);
        if (depositMonitorIntervalId.unref) depositMonitorIntervalId.unref(); // Allow process to exit if only interval remains
        console.log(`✅ [DepositMonitor] Recurring monitor interval (ID: ${depositMonitorIntervalId}) set.`);
    }, initialDelay);
}

function stopDepositMonitoring() {
    if (depositMonitorIntervalId) {
        clearInterval(depositMonitorIntervalId);
        depositMonitorIntervalId = null;
        monitorDepositsPolling.isRunning = false; // Ensure flag is reset
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
        const batchSize = parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE, 10);
        const sigFetchLimit = parseInt(process.env.DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT, 10);

        const pendingAddressesRes = await queryDatabase( // queryDatabase from Part 1
            `SELECT wallet_id, public_key, user_telegram_id, derivation_path, expires_at
             FROM user_deposit_wallets
             WHERE is_active = TRUE AND expires_at > NOW() 
             ORDER BY created_at ASC 
             LIMIT $1`,
            [batchSize]
        );

        if (pendingAddressesRes.rowCount === 0) {
            // console.log(`${logPrefix} No active deposit addresses found to monitor in this cycle.`);
            monitorDepositsPolling.isRunning = false;
            return;
        }
        console.log(`${logPrefix} Found ${pendingAddressesRes.rowCount} active address(es) to check this cycle.`);

        for (const row of pendingAddressesRes.rows) {
            if (isShuttingDown) { console.log(`${logPrefix} Shutdown initiated during address check, aborting cycle.`); break; }
            
            const depositAddress = row.public_key;
            const userDepositWalletId = row.wallet_id;
            const userId = row.user_telegram_id;
            const addrLogPrefix = `[Monitor Addr:${depositAddress.slice(0, 6)}.. WalletID:${userDepositWalletId} User:${userId}]`;

            try {
                const pubKey = new PublicKey(depositAddress); // PublicKey from Part 1
                const signatures = await solanaConnection.getSignaturesForAddress( // solanaConnection from Part 1
                    pubKey, { limit: sigFetchLimit }, DEPOSIT_CONFIRMATION_LEVEL // DEPOSIT_CONFIRMATION_LEVEL from Part 1
                );

                if (signatures && signatures.length > 0) {
                    console.log(`${addrLogPrefix} Found ${signatures.length} potential new signature(s).`);
                    for (const sigInfo of signatures.reverse()) { // Process oldest first
                        if (sigInfo?.signature && !hasProcessedDepositTx(sigInfo.signature)) { // hasProcessedDepositTx from Part P1
                            const isConfirmed = sigInfo.confirmationStatus === DEPOSIT_CONFIRMATION_LEVEL || sigInfo.confirmationStatus === 'finalized';
                            if (!sigInfo.err && isConfirmed) {
                                console.log(`${addrLogPrefix} ✅ New confirmed TX: ${sigInfo.signature}. Queuing for processing.`);
                                // depositProcessorQueue from Part 1
                                depositProcessorQueue.add(() => processDepositTransaction(sigInfo.signature, depositAddress, userDepositWalletId, userId))
                                    .catch(queueError => console.error(`❌ ${addrLogPrefix} Error adding TX ${sigInfo.signature} to deposit queue: ${queueError.message}`));
                                addProcessedDepositTx(sigInfo.signature); // Add to cache (Part P1) to prevent re-queueing immediately
                            } else if (sigInfo.err) {
                                console.warn(`${addrLogPrefix} ⚠️ TX ${sigInfo.signature} has an error on-chain: ${JSON.stringify(sigInfo.err)}. Marking as processed.`);
                                addProcessedDepositTx(sigInfo.signature); 
                            } else {
                                // console.log(`${addrLogPrefix} TX ${sigInfo.signature} not yet confirmed to '${DEPOSIT_CONFIRMATION_LEVEL}'. Status: ${sigInfo.confirmationStatus}`);
                            }
                        }
                    }
                }
            } catch (error) {
                console.error(`❌ ${addrLogPrefix} Error checking signatures: ${error.message}`);
                if (typeof isRetryableSolanaError === 'function' && isRetryableSolanaError(error) && (error?.status === 429 || String(error?.message).toLowerCase().includes('rate limit'))) {
                    console.warn(`${addrLogPrefix} Rate limit hit. Pausing before next address.`);
                    await sleep(3000 + Math.random() * 2000); // sleep from Part 1
                }
            }
            await sleep(200); // Small delay between checking each address
        }
    } catch (error) {
        console.error(`❌ ${logPrefix} Critical error in main polling loop: ${error.message}`, error.stack);
        if (typeof notifyAdmin === 'function') await notifyAdmin(`🚨 *ERROR in Deposit Monitor Loop* 🚨\n\n\`${escapeMarkdownV2(String(error.message || error))}\`\nCheck logs for details\\.`, {parse_mode: 'MarkdownV2'});
    } finally {
        monitorDepositsPolling.isRunning = false;
        console.log(`🔍 ${logPrefix} Polling cycle finished.`);
    }
}

async function processDepositTransaction(txSignature, depositAddress, userDepositWalletId, userId) {
    const logPrefix = `[ProcessDeposit TX:${txSignature.slice(0, 6)}.. Addr:${depositAddress.slice(0,6)}.. WalletID:${userDepositWalletId} User:${userId}]`;
    console.log(`${logPrefix} Processing deposit transaction...`);
    let client = null; // DB client

    try {
        const tx = await solanaConnection.getTransaction(txSignature, {
            maxSupportedTransactionVersion: 0, commitment: DEPOSIT_CONFIRMATION_LEVEL
        });

        if (!tx || tx.meta?.err) {
            console.warn(`ℹ️ ${logPrefix} TX ${txSignature} failed on-chain or details not found. Error: ${JSON.stringify(tx?.meta?.err)}. Marking as processed.`);
            addProcessedDepositTx(txSignature); 
            return;
        }

        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, depositAddress); // from Part P1

        if (transferAmount <= 0n) {
            console.log(`ℹ️ ${logPrefix} No positive SOL transfer to ${depositAddress} found in TX ${txSignature}. Ignoring.`);
            addProcessedDepositTx(txSignature);
            return;
        }
        const depositAmountSOLDisplay = await formatBalanceForDisplay(transferAmount, 'SOL'); // For messages
        console.log(`✅ ${logPrefix} Valid deposit identified: ${depositAmountSOLDisplay} from ${payerAddress || 'unknown source'}.`);

        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');

        const depositRecordResult = await recordConfirmedDepositDB(client, userId, userDepositWalletId, depositAddress, txSignature, transferAmount, payerAddress, tx.blockTime); // from Part P2
        if (depositRecordResult.alreadyProcessed) {
            console.warn(`⚠️ ${logPrefix} TX ${txSignature} already processed in DB (ID: ${depositRecordResult.depositId}). This indicates a cache miss or race. Rolling back current attempt.`);
            await client.query('ROLLBACK'); 
            addProcessedDepositTx(txSignature); // Ensure it's in cache
            return;
        }
        if (!depositRecordResult.success || !depositRecordResult.depositId) {
            throw new Error(`Failed to record deposit in DB for ${txSignature}: ${depositRecordResult.error || "Unknown DB error during deposit recording."}`);
        }
        const depositId = depositRecordResult.depositId;

        // Mark deposit address as inactive (it has served its purpose for this deposit)
        const markedInactive = await markDepositAddressInactiveDB(client, userDepositWalletId); // from Part P2
        if (markedInactive) {
             console.log(`${logPrefix} Marked deposit address Wallet ID ${userDepositWalletId} as inactive.`);
        } else {
            console.warn(`${logPrefix} ⚠️ Could not mark deposit address Wallet ID ${userDepositWalletId} as inactive. It might have been already or an error occurred.`);
        }


        const ledgerNote = `Deposit from ${payerAddress ? payerAddress.slice(0,6)+'..' : 'Unknown'} to ${depositAddress.slice(0,6)}... TX:${txSignature.slice(0,6)}..`;
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, transferAmount, 'deposit', { deposit_id: depositId }, ledgerNote); // from Part P2
        if (!balanceUpdateResult.success || typeof balanceUpdateResult.newBalanceLamports === 'undefined') {
            throw new Error(`Failed to update user ${userId} balance/ledger for deposit: ${balanceUpdateResult.error || "Unknown DB error during balance update."}`);
        }
        
        // Referral linking logic could go here if a deposit triggers it
        // e.g., check if user has a pending referral, if this is first deposit, etc.

        await client.query('COMMIT');
        console.log(`✅ ${logPrefix} DB operations committed. User ${userId} credited.`);
        addProcessedDepositTx(txSignature); // Confirm in cache after DB commit

        const newBalanceUSDDisplay = await formatBalanceForDisplay(balanceUpdateResult.newBalanceLamports, 'USD');
        const userForNotif = await getOrCreateUser(userId); // getOrCreateUser from Part 2
        const playerRefForNotif = getPlayerDisplayReference(userForNotif); // from Part 3
        
        await safeSendMessage(userId, // safeSendMessage from Part 1
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
        addProcessedDepositTx(txSignature); // Add to cache even on error to prevent retrying a problematic TX indefinitely
        if (typeof notifyAdmin === 'function') {
            await notifyAdmin(`🚨 *CRITICAL Error Processing Deposit* 🚨\nTX: \`${escapeMarkdownV2(txSignature)}\`\nAddr: \`${escapeMarkdownV2(depositAddress)}\`\nUser: \`${escapeMarkdownV2(String(userId))}\`\n*Error:*\n\`${escapeMarkdownV2(String(error.message || error))}\`\nManual investigation required\\.`, {parse_mode:'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}


// --- Deposit Address Sweeping Logic ---

function startSweepingProcess() {
    let intervalMs = parseInt(process.env.SWEEP_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs <= 0) {
        console.warn("🧹 [Sweeper] Fund sweeping is disabled (SWEEP_INTERVAL_MS not set or zero).");
        return;
    }
    if (intervalMs < 60000) { // Minimum 1 minute for sweeping
        intervalMs = 60000;
        console.warn(`🧹 [Sweeper] SWEEP_INTERVAL_MS too low, enforcing minimum ${intervalMs}ms.`);
    }
    
    if (sweepIntervalId) { clearInterval(sweepIntervalId); console.log('🔄 [Sweeper] Restarting fund sweeper...'); }
    else { console.log(`⚙️ [Sweeper] Starting Fund Sweeper (Interval: ${intervalMs / 1000 / 60} minutes)...`); }
    
    const initialDelay = (parseInt(process.env.INIT_DELAY_MS, 10) || 5000) + 15000; // Stagger after other startups
    console.log(`[Sweeper] Scheduling first sweep run in ${initialDelay/1000}s...`);

    setTimeout(() => {
        console.log(`[Sweeper] Executing first sweep run...`);
        sweepDepositAddresses().catch(err => console.error("❌ [Initial Sweep Run] Error:", err.message, err.stack));
        sweepIntervalId = setInterval(sweepDepositAddresses, intervalMs);
        if (sweepIntervalId.unref) sweepIntervalId.unref();
        console.log(`✅ [Sweeper] Recurring sweep interval (ID: ${sweepIntervalId}) set.`);
    }, initialDelay);
}

function stopSweepingProcess() {
    if (sweepIntervalId) {
        clearInterval(sweepIntervalId);
        sweepIntervalId = null;
        sweepDepositAddresses.isRunning = false; // Reset flag
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
    const sweepAddressDelayMs = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10) || 1000;
    const sweepFeeBuffer = BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS || 15000);
    const minBalanceToSweep = sweepFeeBuffer + 5000n; // Must be more than fee buffer + a bit for tx fee
    const sweepTargetAddress = MAIN_BOT_KEYPAIR.publicKey.toBase58(); // MAIN_BOT_KEYPAIR from Part 1

    let client = null;
    try {
        // Find addresses that are inactive (e.g., after a deposit) AND have not been swept, OR active but expired and have a balance
        // For simplicity, let's primarily target inactive, unswept addresses with a balance.
        const addressesToConsiderRes = await queryDatabase(
            `SELECT wallet_id, public_key, derivation_path 
             FROM user_deposit_wallets 
             WHERE swept_at IS NULL 
             AND (is_active = FALSE OR expires_at < NOW()) -- Either explicitly inactive or expired
             ORDER BY created_at ASC 
             LIMIT $1`,
            [sweepBatchSize]
        );

        if (addressesToConsiderRes.rowCount === 0) {
            // console.log(`${logPrefix} No addresses found requiring a sweep in this cycle.`);
            sweepDepositAddresses.isRunning = false;
            return;
        }
        console.log(`${logPrefix} Found ${addressesToConsiderRes.rowCount} potential addresses to check for sweeping.`);

        client = await pool.connect();

        for (const addrData of addressesToConsiderRes.rows) {
            if (isShuttingDown) { console.log(`${logPrefix} Shutdown initiated during sweep, aborting current cycle.`); break; }
            
            const addrLogPrefix = `[Sweep Addr:${addrData.public_key.slice(0,6)}.. WalletID:${addrData.wallet_id}]`;
            let depositKeypair;
            try {
                depositKeypair = deriveSolanaKeypair(DEPOSIT_MASTER_SEED_PHRASE, addrData.derivation_path); // deriveSolanaKeypair from Part P1
                if (!depositKeypair || depositKeypair.publicKey.toBase58() !== addrData.public_key) {
                    console.error(`${addrLogPrefix} ❌ Key derivation mismatch or failure for path ${addrData.derivation_path}. Marking as unsweepable (error).`);
                    // Mark as error or requires investigation in DB (not implemented here, could be a note)
                    await queryDatabase("UPDATE user_deposit_wallets SET swept_at = NOW(), notes = COALESCE(notes, '') || 'Sweep Error: Key derivation failed.' WHERE wallet_id = $1", [addrData.wallet_id]);
                    continue;
                }
            } catch (derivError) {
                console.error(`${addrLogPrefix} ❌ Critical error deriving key for sweep: ${derivError.message}. Skipping this address.`);
                await queryDatabase("UPDATE user_deposit_wallets SET swept_at = NOW(), notes = COALESCE(notes, '') || 'Sweep Error: Key derivation exception.' WHERE wallet_id = $1", [addrData.wallet_id]);
                continue;
            }

            const balanceLamports = await getSolBalance(addrData.public_key); // getSolBalance from Part P1
            if (balanceLamports === null) {
                console.warn(`${addrLogPrefix} Could not fetch balance. Skipping.`);
                continue;
            }

            if (balanceLamports >= minBalanceToSweep) {
                const amountToSweep = balanceLamports - sweepFeeBuffer; // Leave buffer for tx fee from this address
                console.log(`${addrLogPrefix} Balance: ${balanceLamports}. Attempting to sweep ${amountToSweep} to ${sweepTargetAddress.slice(0,6)}..`);

                await client.query('BEGIN'); // Start transaction for DB updates post-sweep
                
                const sweepPriorityFee = parseInt(process.env.SWEEP_PRIORITY_FEE_MICROLAMPORTS, 10);
                const sweepComputeUnits = parseInt(process.env.SWEEP_COMPUTE_UNIT_LIMIT, 10);

                // sendSol from Part P1 (now accepts payerKeypair)
                const sendResult = await sendSol(depositKeypair, sweepTargetAddress, amountToSweep, `Sweep from ${addrData.public_key.slice(0,4)}`, sweepPriorityFee, sweepComputeUnits);

                if (sendResult.success && sendResult.signature) {
                    totalSweptLamports += amountToSweep;
                    addressesProcessed++;
                    console.log(`${addrLogPrefix} ✅ Sweep successful! TX: ${sendResult.signature}. Amount: ${amountToSweep}`);
                    await recordSweepTransactionDB(client, addrData.public_key, sweepTargetAddress, amountToSweep, sendResult.signature); // from Part P2
                    await markDepositAddressInactiveDB(client, addrData.wallet_id, true, balanceLamports); // from Part P2 (marks swept and inactive)
                    await client.query('COMMIT');
                } else {
                    await client.query('ROLLBACK');
                    console.error(`${addrLogPrefix} ❌ Sweep failed: ${sendResult.error}. Error Type: ${sendResult.errorType}`);
                    if (sendResult.errorType === "InsufficientFundsError") { // Address might be empty after all, or can't cover its own fee
                         await queryDatabase("UPDATE user_deposit_wallets SET swept_at = NOW(), notes = COALESCE(notes, '') || 'Sweep Attempted: Insufficient for fee.' WHERE wallet_id = $1", [addrData.wallet_id]);
                    }
                    // If not retryable, might mark as "sweep_failed" in DB to avoid retrying constantly.
                }
            } else if (balanceLamports > 0n) { // Has some dust, but not enough to sweep
                console.log(`${addrLogPrefix} Balance ${balanceLamports} is below sweep threshold (${minBalanceToSweep}). Marking as swept (dust).`);
                // Mark as swept to prevent re-checking if balance is just dust.
                await queryDatabase("UPDATE user_deposit_wallets SET swept_at = NOW(), balance_at_sweep = $1, is_active = FALSE WHERE wallet_id = $2", [balanceLamports.toString(), addrData.wallet_id]);
            } else { // Zero balance, just ensure it's marked inactive and swept_at is set if not already.
                 await queryDatabase("UPDATE user_deposit_wallets SET swept_at = NOW(), balance_at_sweep = 0, is_active = FALSE WHERE wallet_id = $1 AND swept_at IS NULL", [addrData.wallet_id]);
            }
            await sleep(sweepAddressDelayMs); // Delay between processing each address
        }

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error on main sweep loop: ${rbErr.message}`));
        console.error(`❌ ${logPrefix} Critical error during sweep cycle: ${error.message}`, error.stack);
        if (typeof notifyAdmin === 'function') await notifyAdmin(`🚨 *ERROR in Fund Sweeping Cycle* 🚨\n\n\`${escapeMarkdownV2(String(error.message || error))}\`\nCheck logs for details\\. Sweeping may be impaired\\.`, {parse_mode: 'MarkdownV2'});
    } finally {
        if (client) client.release();
        sweepDepositAddresses.isRunning = false;
        if (addressesProcessed > 0) {
            console.log(`🧹 ${logPrefix} Sweep cycle finished. Processed ${addressesProcessed} addresses, swept total of ${formatCurrency(totalSweptLamports, 'SOL')}.`);
        } else {
            // console.log(`🧹 ${logPrefix} Sweep cycle finished. No funds swept in this cycle.`);
        }
    }
}


// --- Payout Job Processing Logic ---
// (addPayoutJob, handleWithdrawalPayoutJob, handleReferralPayoutJob - these are complex and have been drafted in previous iterations.
//  They rely on DB ops from P2 and sendSol from P1. Ensure they use MAIN_BOT_KEYPAIR or REFERRAL_PAYOUT_KEYPAIR correctly)

async function addPayoutJob(jobData) {
    const jobType = jobData?.type || 'unknown_payout_job';
    const jobId = jobData?.withdrawalId || jobData?.payoutId || 'N/A_ID';
    const logPrefix = `[AddPayoutJob Type:${jobType} ID:${jobId}]`;
    console.log(`⚙️ ${logPrefix} Adding job to payout queue for user ${jobData.userId || 'N/A'}.`);

    if (typeof payoutProcessorQueue === 'undefined' || typeof sleep === 'undefined' || typeof notifyAdmin === 'undefined' || typeof escapeMarkdownV2 === 'undefined') {
        console.error(`${logPrefix} 🚨 CRITICAL: Payout queue or essential utilities missing. Cannot add job.`);
        if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL Error: Cannot add payout job ${escapeMarkdownV2(jobType)}:${escapeMarkdownV2(String(jobId))}. Payout queue/utilities missing.`);
        return;
    }

    payoutProcessorQueue.add(async () => { // payoutProcessorQueue from Part 1
        let attempts = 0;
        const maxAttempts = (parseInt(process.env.PAYOUT_JOB_RETRIES, 10) || 3) + 1; // +1 for initial try
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
                return; // Success, exit retry loop
            } catch(error) {
                console.warn(`⚠️ ${attemptLogPrefix} Attempt failed: ${error.message}`);
                const isRetryableFlag = error.isRetryable === true || (typeof isRetryableSolanaError === 'function' && isRetryableSolanaError(error));

                if (!isRetryableFlag || attempts >= maxAttempts) {
                    console.error(`❌ ${attemptLogPrefix} Job failed permanently after ${attempts} attempts. Error: ${error.message}`);
                    if (typeof notifyAdmin === "function") {
                        notifyAdmin(`🚨 *PAYOUT JOB FAILED (Permanent)* 🚨\nType: \`${escapeMarkdownV2(jobType)}\`\nID: \`${escapeMarkdownV2(String(jobId))}\`\nAttempts: ${attempts}\n*Error:* \`${escapeMarkdownV2(String(error.message || error))}\`\nManual intervention may be required\\.`, {parse_mode:'MarkdownV2'}).catch(()=>{});
                    }
                    return; 
                }
                const delayWithJitter = baseDelayMs * Math.pow(2, attempts - 1) * (0.8 + Math.random() * 0.4);
                const actualDelay = Math.min(delayWithJitter, parseInt(process.env.RPC_RETRY_MAX_DELAY, 10) || 60000); // Cap delay
                console.log(`⏳ ${attemptLogPrefix} Retrying in ~${Math.round(actualDelay / 1000)}s...`);
                await sleep(actualDelay);
            }
        }
    }).catch(queueError => { // Catch errors from adding to queue itself or unhandled promise rejections from the task
        console.error(`❌ ${logPrefix} CRITICAL Error in Payout Queue execution or adding job: ${queueError.message}`, queueError.stack);
        if (typeof notifyAdmin === "function") {
            notifyAdmin(`🚨 *CRITICAL Payout Queue Error* 🚨\nJob Type: \`${escapeMarkdownV2(jobType)}\`\nID: \`${escapeMarkdownV2(String(jobId))}\`\n*Error:* \`${escapeMarkdownV2(String(queueError.message || queueError))}\`\nQueue functionality may be compromised\\.`, {parse_mode:'MarkdownV2'}).catch(()=>{});
        }
    });
}
console.log("[Payout Jobs] addPayoutJob defined.");


async function handleWithdrawalPayoutJob(withdrawalId) {
    const logPrefix = `[WithdrawJob ID:${withdrawalId}]`;
    console.log(`⚙️ ${logPrefix} Processing withdrawal payout job...`);
    let clientForDb = null; // For DB operations within a transaction if needed for refunds
    let sendSolResult = { success: false, error: "Send SOL not initiated", isRetryable: false };

    const details = await getWithdrawalDetailsDB(withdrawalId); // From Part P2
    if (!details) {
        const error = new Error(`Withdrawal details not found for ID ${withdrawalId}. Job cannot proceed.`);
        error.isRetryable = false; throw error; // Non-retryable if details are gone
    }

    if (details.status === 'completed' || details.status === 'confirmed') { // 'confirmed' might be used if tx is on-chain but not yet notified fully
        console.log(`ℹ️ ${logPrefix} Job skipped, withdrawal ID ${withdrawalId} already marked '${details.status}'.`);
        return;
    }
    if (details.status === 'failed') {
         console.log(`ℹ️ ${logPrefix} Job skipped, withdrawal ID ${withdrawalId} already marked 'failed'. No retry from queue.`);
        return;
    }

    const userId = details.user_telegram_id;
    const recipient = details.destination_address;
    const amountToActuallySend = BigInt(details.amount_lamports);
    const feeApplied = BigInt(details.fee_lamports);
    const totalAmountDebitedFromBalance = amountToActuallySend + feeApplied;
    const userForNotif = await getOrCreateUser(userId);
    const playerRefForNotif = getPlayerDisplayReference(userForNotif);

    try {
        clientForDb = await pool.connect(); // For potential refund transaction
        await clientForDb.query('BEGIN'); // Start transaction for status updates, may not be needed if single update call

        await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'processing'); // Part P2
        await clientForDb.query('COMMIT'); // Commit status update before sending
        
        console.log(`${logPrefix} Status updated to 'processing'. Attempting to send ${formatCurrency(amountToActuallySend, 'SOL')} to ${recipient}.`);

        sendSolResult = await sendSol(MAIN_BOT_KEYPAIR, recipient, amountToActuallySend, `Withdrawal ID ${withdrawalId}`); // MAIN_BOT_KEYPAIR from Part 1

        await clientForDb.query('BEGIN'); // New transaction for final status update
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
            console.error(`❌ ${logPrefix} sendSol FAILED for withdrawal ID ${withdrawalId}. Reason: ${sendErrorMsg}. Attempting to mark 'failed' and refund user.`);
            await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'failed', null, sendErrorMsg.substring(0, 250)); // Store concise error
            
            // Refund logic using updateUserBalanceAndLedger
            const refundNotes = `Refund for failed withdrawal ID ${withdrawalId}. Original send error: ${sendErrorMsg.substring(0,100)}`;
            const refundUpdateResult = await updateUserBalanceAndLedger(
                clientForDb, userId, totalAmountDebitedFromBalance, // Credit back the full amount
                'withdrawal_refund', { withdrawal_id: withdrawalId }, refundNotes
            );

            if (refundUpdateResult.success) {
                await clientForDb.query('COMMIT');
                console.log(`✅ ${logPrefix} Successfully refunded ${formatCurrency(totalAmountDebitedFromBalance, 'SOL')} to user ${userId} for failed withdrawal.`);
                await safeSendMessage(userId,
                    `⚠️ *Withdrawal Failed* ⚠️\n\n${playerRefForNotif}, your withdrawal of *${escapeMarkdownV2(formatCurrency(amountToActuallySend, 'SOL'))}* could not be processed at this time \\(Reason: \`${escapeMarkdownV2(sendErrorMsg)}\`\\).\n` +
                    `The full amount of *${escapeMarkdownV2(formatCurrency(totalAmountDebitedFromBalance, 'SOL'))}* \\(including fee\\) has been refunded to your casino balance\\.`,
                    {parse_mode: 'MarkdownV2'}
                );
            } else {
                await clientForDb.query('ROLLBACK'); // Rollback if refund fails
                console.error(`❌ CRITICAL ${logPrefix} FAILED TO REFUND USER ${userId} for withdrawal ${withdrawalId}. Amount: ${formatCurrency(totalAmountDebitedFromBalance, 'SOL')}. Refund DB Error: ${refundUpdateResult.error}`);
                if (typeof notifyAdmin === 'function') {
                    notifyAdmin(`🚨🚨 *CRITICAL: FAILED WITHDRAWAL REFUND* 🚨🚨\nUser: ${playerRefForNotif} (\`${escapeMarkdownV2(String(userId))}\`)\nWD ID: \`${withdrawalId}\`\nAmount Due: \`${escapeMarkdownV2(formatCurrency(totalAmountDebitedFromBalance, 'SOL'))}\`\nSend Error: \`${escapeMarkdownV2(sendErrorMsg)}\`\nRefund DB Error: \`${escapeMarkdownV2(refundUpdateResult.error || 'Unknown')}\`\nMANUAL INTERVENTION REQUIRED\\.`, {parse_mode:'MarkdownV2'});
                }
            }
            
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true;
            throw errorToThrowForRetry; // Throw to trigger queue retry if applicable
        }
    } catch (jobError) {
        if (clientForDb) await clientForDb.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Final rollback error: ${rbErr.message}`)); // Rollback on any job error if TX was open
        console.error(`❌ ${logPrefix} Error during withdrawal job ID ${withdrawalId}: ${jobError.message}`, jobError.stack);
        // Ensure isRetryable is set on the thrown error for the queue
        if (jobError.isRetryable === undefined) {
            jobError.isRetryable = (typeof isRetryableSolanaError === 'function' && isRetryableSolanaError(jobError)) || sendSolResult.isRetryable === true;
        }
        // Ensure status is 'failed' if not already terminal and jobError not retryable by queue
        if (!jobError.isRetryable) {
            const currentDetailsAfterJobError = await getWithdrawalDetailsDB(withdrawalId); // Check status outside transaction
            if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'completed' && currentDetailsAfterJobError.status !== 'failed') {
                const updateClient = await pool.connect(); // New client for isolated update
                await updateWithdrawalStatusDB(updateClient, withdrawalId, 'failed', null, `Job error (non-retryable): ${jobError.message}`.substring(0,250));
                updateClient.release();
            }
        }
        throw jobError; // Re-throw for queue to handle retries
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
    // Payout keypair: use specific referral payout key if available, else main bot key
    const payer = REFERRAL_PAYOUT_KEYPAIR || MAIN_BOT_KEYPAIR; // REFERRAL_PAYOUT_KEYPAIR from Part 1

    const details = await getReferralPayoutDetailsDB(payoutId); // From Part P2 (currently a stub)
    if (!details) {
        const error = new Error(`Referral payout details not found for ID ${payoutId}.`); error.isRetryable = false; throw error;
    }
    if (details.status === 'paid' || details.status === 'failed') {
        console.log(`ℹ️ ${logPrefix} Job skipped, referral payout ID ${payoutId} already '${details.status}'.`); return;
    }

    const referrerUserId = details.referrer_telegram_id;
    const amountToPay = BigInt(details.commission_amount_lamports);
    const userForNotif = await getOrCreateUser(referrerUserId);
    const playerRefForNotif = getPlayerDisplayReference(userForNotif);

    try {
        clientForDb = await pool.connect();
        await clientForDb.query('BEGIN');

        const referrerWalletDetails = await getPaymentSystemUserDetails(referrerUserId, clientForDb); // From Part P2
        if (!referrerWalletDetails?.solana_wallet_address) {
            const noWalletMsg = `Referrer ${playerRefForNotif} (\`${escapeMarkdownV2(String(referrerUserId))}\`) has no linked SOL wallet for referral payout ID ${payoutId}.`;
            console.error(`❌ ${logPrefix} ${noWalletMsg}`);
            await updateReferralPayoutStatusDB(clientForDb, payoutId, 'failed', null, noWalletMsg.substring(0, 250));
            await clientForDb.query('COMMIT');
            const error = new Error(noWalletMsg); error.isRetryable = false; throw error;
        }
        const recipientAddress = referrerWalletDetails.solana_wallet_address;

        await updateReferralPayoutStatusDB(clientForDb, payoutId, 'processing');
        await clientForDb.query('COMMIT'); // Commit status before sending

        console.log(`${logPrefix} Status to 'processing'. Sending ${formatCurrency(amountToPay, 'SOL')} to ${recipientAddress} using ${payer.publicKey.toBase58().slice(0,6)}.. key.`);
        sendSolResult = await sendSol(payer, recipientAddress, amountToPay, `Referral Payout ID ${payoutId}`);

        await clientForDb.query('BEGIN'); // New transaction for final status
        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful for referral ID ${payoutId}. TX: ${sendSolResult.signature}.`);
            await updateReferralPayoutStatusDB(clientForDb, payoutId, 'paid', sendSolResult.signature);
            await clientForDb.query('COMMIT');

            await safeSendMessage(referrerUserId,
                `🎁 *Referral Bonus Paid, ${playerRefForNotif}!* 🎁\n\n` +
                `Your referral commission of *${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}* has been sent to your linked wallet: \`${escapeMarkdownV2(recipientAddress)}\`\\.\n` +
                `🧾 Transaction ID: \`${escapeMarkdownV2(sendSolResult.signature)}\`\n\nThanks for spreading the word about ${escapeMarkdownV2(BOT_NAME)}\\!`,
                { parse_mode: 'MarkdownV2' }
            );
            return;
        } else { // sendSol failed
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure for referral payout.';
            console.error(`❌ ${logPrefix} sendSol FAILED for referral payout ID ${payoutId}. Reason: ${sendErrorMsg}`);
            await updateReferralPayoutStatusDB(clientForDb, payoutId, 'failed', null, sendErrorMsg.substring(0, 250));
            await clientForDb.query('COMMIT'); // Commit failure status

            await safeSendMessage(referrerUserId,
                `⚠️ *Referral Payout Issue* ⚠️\n\n${playerRefForNotif}, we encountered an issue sending your referral reward of *${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}* \\(Details: \`${escapeMarkdownV2(sendErrorMsg)}\`\\)\\. Please ensure your linked wallet is correct or contact support\\.`,
                {parse_mode: 'MarkdownV2'}
            );
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 *REFERRAL PAYOUT FAILED* 🚨\nReferrer: ${playerRefForNotif} (\`${escapeMarkdownV2(String(referrerUserId))}\`)\nPayout ID: \`${payoutId}\`\nAmount: \`${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}\`\n*Error:* \`${escapeMarkdownV2(sendErrorMsg)}\`\\.`, {parse_mode:'MarkdownV2'});
            }
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true;
            throw errorToThrowForRetry;
        }
    } catch (jobError) {
        if(clientForDb) await clientForDb.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Final rollback error: ${rbErr.message}`));
        console.error(`❌ ${logPrefix} Error during referral payout job ID ${payoutId}: ${jobError.message}`, jobError.stack);
        if (jobError.isRetryable === undefined) {
            jobError.isRetryable = (typeof isRetryableSolanaError === 'function' && isRetryableSolanaError(jobError)) || sendSolResult.isRetryable === true;
        }
        if (!jobError.isRetryable) { // Mark as failed if not retryable by queue
            const updateClient = await pool.connect();
            await updateReferralPayoutStatusDB(updateClient, payoutId, 'failed', null, `Job error (non-retryable): ${jobError.message}`.substring(0,250));
            updateClient.release();
        }
        throw jobError;
    } finally {
        if (clientForDb) clientForDb.release();
    }
}
console.log("[Payout Jobs] handleReferralPayoutJob defined.");


// --- Webhook Handling (if ENABLE_PAYMENT_WEBHOOKS === 'true' in Part 1) ---
if (process.env.ENABLE_PAYMENT_WEBHOOKS === 'true') {
    const PAYMENT_WEBHOOK_SECRET = process.env.PAYMENT_WEBHOOK_SECRET; // For signature validation

    if (app) { // app is the express instance from Part 1
        // Ensure rawBody is available for signature validation if using express.json()
        app.use(express.json({
            verify: (req, res, buf) => {
                req.rawBody = buf; // Store raw body buffer for signature check
            }
        }));

        const paymentWebhookPath = process.env.PAYMENT_WEBHOOK_PATH || '/webhook/solana-payment';
        app.post(paymentWebhookPath, async (req, res) => {
            const webhookLogPrefix = `[PaymentWebhook ${paymentWebhookPath}]`;
            const signatureFromHeader = req.headers['x-signature'] || req.headers['X-Signature']; // Example header

            if (PAYMENT_WEBHOOK_SECRET) {
                // Implement robust signature validation here using req.rawBody and PAYMENT_WEBHOOK_SECRET
                // This is a placeholder for actual signature validation logic.
                // const calculatedSignature = crypto.createHmac('sha256', PAYMENT_WEBHOOK_SECRET).update(req.rawBody).digest('hex');
                // if (calculatedSignature !== signatureFromHeader) {
                //    console.warn(`${webhookLogPrefix} ⚠️ Invalid webhook signature. Header: ${signatureFromHeader}`);
                //    return res.status(401).send('Unauthorized: Invalid signature');
                // }
                // console.log(`${webhookLogPrefix} ✅ Webhook signature validated (conceptual).`);
                if(!signatureFromHeader) console.warn(`${webhookLogPrefix} Webhook secret is set, but no signature header found. Proceeding without validation (dev only).`);

            }

            console.log(`${webhookLogPrefix} Received POST request. Body:`, stringifyWithBigInt(req.body));
            
            try {
                // Adapt payload structure based on your webhook provider (e.g., Helius, Shyft, QuickNode)
                // This is a generic example assuming a common payload structure.
                const payload = req.body; 
                let relevantTransactions = [];

                // Example for Helius webhook structure (array of TransactionEvent)
                if (Array.isArray(payload)) {
                    payload.forEach(event => {
                        if (event.type === "TRANSFER" && event.transaction?.signature && event.tokenTransfers) {
                            event.tokenTransfers.forEach(transfer => {
                                if (transfer.toUserAccount && transfer.mint === "So11111111111111111111111111111111111111112") { // Check for SOL transfer
                                    relevantTransactions.push({
                                        signature: event.transaction.signature,
                                        depositToAddress: transfer.toUserAccount,
                                        amount: BigInt(transfer.tokenAmount) * BigInt(10 ** SOL_DECIMALS) // Assuming amount is in SOL
                                    });
                                }
                            });
                        } else if (event.signature && event.description && event.description.includes("funded")) { // More generic
                             // Try to parse out address and amount from description or other fields
                        }
                    });
                } else if (payload.signature && payload.events?.transfer?.to) { // Simpler structure
                     relevantTransactions.push({
                        signature: payload.signature,
                        depositToAddress: payload.events.transfer.to,
                        amount: BigInt(payload.events.transfer.amount) // Assuming lamports
                    });
                }
                 // Add other payload structure checks as needed for your specific webhook provider

                if (relevantTransactions.length === 0) {
                     console.warn(`${webhookLogPrefix} No relevant SOL transfer transactions found in webhook payload.`);
                     return res.status(200).send('Webhook received, no actionable data.'); // Ack but no action
                }

                for (const txInfo of relevantTransactions) {
                    const { signature, depositToAddress } = txInfo;
                    if (!signature || !depositToAddress) {
                        console.warn(`${webhookLogPrefix} Webhook transaction info missing critical data (signature or depositToAddress).`);
                        continue; // Skip this entry
                    }

                    if (!hasProcessedDepositTx(signature)) { // from Part P1
                        const addrInfo = await findDepositAddressInfoDB(depositToAddress); // from Part P2
                        if (addrInfo && addrInfo.isActive && new Date(addrInfo.expiresAt).getTime() > Date.now()) {
                            console.log(`${webhookLogPrefix} ✅ Valid webhook for active address ${depositToAddress}. Queuing TX: ${signature} for User: ${addrInfo.userId}`);
                            depositProcessorQueue.add(() => processDepositTransaction(signature, depositToAddress, addrInfo.walletId, addrInfo.userId))
                                .catch(queueError => console.error(`❌ ${webhookLogPrefix} Error adding TX ${signature} to deposit queue from webhook: ${queueError.message}`));
                            addProcessedDepositTx(signature); // Add to cache immediately to prevent re-queue from polling
                        } else {
                            console.warn(`${webhookLogPrefix} ⚠️ Webhook for inactive/expired/unknown address ${depositToAddress}. TX ${signature}. AddrInfo:`, addrInfo);
                        }
                    } else {
                        console.log(`${webhookLogPrefix} ℹ️ TX ${signature} already processed or seen (via cache). Ignoring webhook notification.`);
                    }
                }
                res.status(200).send('Webhook processed successfully');
            } catch (error) {
                console.error(`❌ ${webhookLogPrefix} Error processing webhook payload:`, error);
                res.status(500).send('Internal Server Error');
            }
        });
        // The app.listen call will be in Part 6 (main startup), or if this script is the main server itself.
        console.log(`✅ Payment webhook route ${paymentWebhookPath} configured (Express server to be started in Part 6 or managed externally).`);
    } else {
        console.warn("⚠️ ENABLE_PAYMENT_WEBHOOKS is true, but Express app instance ('app') is not available. Webhook endpoint not set up.");
    }
} else {
    console.log("ℹ️ Payment webhooks are disabled (ENABLE_PAYMENT_WEBHOOKS is not 'true').");
}

console.log("Part P4: Payment System Background Tasks & Webhook Handling - Complete.");
// --- End of Part P4 ---
