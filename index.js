// --- Start of Part 1 (REVISED - For New Dice Escalator Game IDs and Constants) ---
// index.js - Part 1: Core Imports, Basic Setup, Global State & Utilities
//---------------------------------------------------------------------------

import 'dotenv/config';
import TelegramBot from 'node-telegram-bot-api';
import { Pool } from 'pg';
import express from 'express';
import {
    Connection,
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram,
    SendTransactionError,
    TransactionExpiredBlockheightExceededError
} from '@solana/web3.js';
import bs58 from 'bs58';
import * as crypto from 'crypto'; // For createHash and randomBytes
import { createHash } from 'crypto';
import PQueue from 'p-queue';
import { Buffer } from 'buffer';
import bip39 from 'bip39';
import { derivePath } from 'ed25519-hd-key';
import nacl from 'tweetnacl';
import axios from 'axios';

// Assuming this path is correct relative to your project structure
import RateLimitedConnection from './lib/solana-connection.js';

// Helper function to stringify objects with BigInts and Functions for logging
function stringifyWithBigInt(obj) {
  return JSON.stringify(obj, (key, value) => {
    if (typeof value === 'bigint') {
      return value.toString() + 'n';
    }
    if (typeof value === 'function') {
      return `[Function: ${value.name || 'anonymous'}]`;
    }
    if (value === undefined) {
      return 'undefined_value'; // Represent undefined explicitly
    }
    return value;
  }, 2);
}

// --- Environment Variable Defaults ---
const CASINO_ENV_DEFAULTS = {
  'DB_POOL_MAX': '25',
  'DB_POOL_MIN': '5',
  'DB_IDLE_TIMEOUT': '30000',
  'DB_CONN_TIMEOUT': '5000',
  'DB_SSL': 'true',
  'DB_REJECT_UNAUTHORIZED': 'true',
  'SHUTDOWN_FAIL_TIMEOUT_MS': '10000',
  'JACKPOT_CONTRIBUTION_PERCENT': '0.01', // 1% (For Dice Escalator PvB)
  'MIN_BET_AMOUNT_LAMPORTS': '5000000',  // 0.005 SOL
  'MAX_BET_AMOUNT_LAMPORTS': '1000000000', // 1 SOL
  'COMMAND_COOLDOWN_MS': '1500',
  'JOIN_GAME_TIMEOUT_MS': '120000', // 2 minutes (for PvP offers and unified offers)
  'DEFAULT_STARTING_BALANCE_LAMPORTS': '10000000', // 0.01 SOL
  'TARGET_JACKPOT_SCORE': '100', // For Dice Escalator PvB Jackpot
  'DICE_ESCALATOR_BUST_ON': '1', // For player rolls in Dice Escalator (PvB & PvP)
  'DICE_21_TARGET_SCORE': '21',
  'DICE_21_BOT_STAND_SCORE': '17',
  'OU7_DICE_COUNT': '2',
  'OU7_PAYOUT_NORMAL': '1',
  'OU7_PAYOUT_SEVEN': '4',
  'DUEL_DICE_COUNT': '2',
  'LADDER_ROLL_COUNT': '5',
  'LADDER_BUST_ON': '1',
  'RULES_CALLBACK_PREFIX': 'rules_game_',
  'DEPOSIT_CALLBACK_ACTION': 'deposit_action',
  'WITHDRAW_CALLBACK_ACTION': 'withdraw_action',
  'QUICK_DEPOSIT_CALLBACK_ACTION': 'quick_deposit_action',
  'MAX_RETRY_POLLING_DELAY': '60000',
  'INITIAL_RETRY_POLLING_DELAY': '5000',
  'BOT_NAME': 'Solana Casino Royale',
  'DICE_ROLL_POLL_INTERVAL_MS': '2500',
  'DICE_ROLL_POLL_ATTEMPTS': '24',
  // --- MINES GAME DEFAULTS (NEW) ---
  'MINES_DEFAULT_ROWS': '5',
  'MINES_DEFAULT_COLS': '5',
  'MINES_FALLBACK_DEFAULT_MINES': '3', // Used if difficulty custom mines is not chosen
  'MINES_MIN_MINES': '1',         // Absolute minimum mines
  'MINES_MAX_MINES_PERCENT': '0.6', // Max 60% of cells can be mines (for validation)
};

const PAYMENT_ENV_DEFAULTS = {
  'SOLANA_RPC_URL': 'https://api.mainnet-beta.solana.com/',
  'RPC_URLS': '',
  'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60',
  'DEPOSIT_CONFIRMATIONS': 'confirmed',
  'WITHDRAWAL_FEE_LAMPORTS': '10000',
  'MIN_WITHDRAWAL_LAMPORTS': '10000000',
  'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '10000',
  'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000',
  'PAYOUT_COMPUTE_UNIT_LIMIT': '30000',
  'PAYOUT_JOB_RETRIES': '3',
  'PAYOUT_JOB_RETRY_DELAY_MS': '7000',
  'SWEEP_INTERVAL_MS': '300000',
  'SWEEP_BATCH_SIZE': '15',
  'SWEEP_FEE_BUFFER_LAMPORTS': '50000',
  'SWEEP_COMPUTE_UNIT_LIMIT': '30000',
  'SWEEP_PRIORITY_FEE_MICROLAMPORTS': '5000',
  'SWEEP_ADDRESS_DELAY_MS': '1500',
  'SWEEP_RETRY_ATTEMPTS': '2',
  'SWEEP_RETRY_DELAY_MS': '10000',
  'RPC_MAX_CONCURRENT': '10',
  'RPC_RETRY_BASE_DELAY': '750',
  'RPC_MAX_RETRIES': '4',
  'RPC_RATE_LIMIT_COOLOFF': '3000',
  'RPC_RETRY_MAX_DELAY': '25000',
  'RPC_RETRY_JITTER': '0.3',
  'RPC_COMMITMENT': 'confirmed',
  'PAYOUT_QUEUE_CONCURRENCY': '4',
  'PAYOUT_QUEUE_TIMEOUT_MS': '90000',
  'DEPOSIT_PROCESS_QUEUE_CONCURRENCY': '5',
  'DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS': '45000',
  'TELEGRAM_SEND_QUEUE_CONCURRENCY': '1',
  'TELEGRAM_SEND_QUEUE_INTERVAL_MS': '1050',
  'TELEGRAM_SEND_QUEUE_INTERVAL_CAP': '1',
  'DEPOSIT_MONITOR_INTERVAL_MS': '15000',
  'DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE': '75',
  'DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT': '15',
  'WALLET_CACHE_TTL_MS': (15 * 60 * 1000).toString(),
  'DEPOSIT_ADDR_CACHE_TTL_MS': (parseInt(CASINO_ENV_DEFAULTS.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10) * 60 * 1000 + 5 * 60 * 1000).toString(),
  'MAX_PROCESSED_TX_CACHE': '10000',
  'INIT_DELAY_MS': '7000',
  'ENABLE_PAYMENT_WEBHOOKS': 'false',
  'PAYMENT_WEBHOOK_PORT': '3000',
  'PAYMENT_WEBHOOK_PATH': '/webhook/solana-payments',
  'SOL_PRICE_API_URL': 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
  'SOL_USD_PRICE_CACHE_TTL_MS': (3 * 60 * 1000).toString(),
  'MIN_BET_USD': '0.50',
  'MAX_BET_USD': '100.00',
};

const OPTIONAL_ENV_DEFAULTS = { ...CASINO_ENV_DEFAULTS, ...PAYMENT_ENV_DEFAULTS };

Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
  if (process.env[key] === undefined) {
    process.env[key] = defaultValue;
  }
});

// --- Core Configuration Constants ---
const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_USER_ID = process.env.ADMIN_USER_ID;
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_NAME = process.env.BOT_NAME; // From ENV_DEFAULTS

// Payment System Keys & Seeds
const DEPOSIT_MASTER_SEED_PHRASE = process.env.DEPOSIT_MASTER_SEED_PHRASE;
const MAIN_BOT_PRIVATE_KEY_BS58 = process.env.MAIN_BOT_PRIVATE_KEY;
const REFERRAL_PAYOUT_PRIVATE_KEY_BS58 = process.env.REFERRAL_PAYOUT_PRIVATE_KEY;

// --- GAME_IDS Constant ---
const GAME_IDS = {
    COINFLIP: 'coinflip',
    RPS: 'rps',
    DICE_ESCALATOR_UNIFIED_OFFER: 'dice_escalator_unified_offer',
    DICE_ESCALATOR_PVB: 'dice_escalator_pvb',
    DICE_ESCALATOR_PVP: 'dice_escalator_pvp',
    DICE_21_UNIFIED_OFFER: 'dice21_unified_offer',
    DICE_21: 'dice21', 
    DICE_21_PVP: 'dice21_pvp',
    OVER_UNDER_7: 'ou7',
    DUEL_UNIFIED_OFFER: 'duel_unified_offer',
    DUEL_PVB: 'duel_pvb',
    DUEL_PVP: 'duel_pvp',
    LADDER: 'ladder',
    SEVEN_OUT: 'sevenout',
    SLOT_FRENZY: 'slotfrenzy',
    MINES: 'mines', 
    MINES_OFFER: 'mines_offer', 
};

// Game Specific Constants
const DICE_21_TARGET_SCORE = parseInt(process.env.DICE_21_TARGET_SCORE, 10);
const DICE_21_BOT_STAND_SCORE = parseInt(process.env.DICE_21_BOT_STAND_SCORE, 10);
const DICE_ROLL_POLLING_MAX_ATTEMPTS = parseInt(process.env.DICE_ROLL_POLL_ATTEMPTS, 10);
const DICE_ROLL_POLLING_INTERVAL_MS = parseInt(process.env.DICE_ROLL_POLL_INTERVAL_MS, 10);
const JOIN_GAME_TIMEOUT_MS = parseInt(process.env.JOIN_GAME_TIMEOUT_MS, 10);
const OU7_DICE_COUNT = parseInt(process.env.OU7_DICE_COUNT, 10);
const OU7_PAYOUT_NORMAL = parseFloat(process.env.OU7_PAYOUT_NORMAL);
const OU7_PAYOUT_SEVEN = parseFloat(process.env.OU7_PAYOUT_SEVEN);
const DUEL_DICE_COUNT = parseInt(process.env.DUEL_DICE_COUNT, 10);
const LADDER_ROLL_COUNT = parseInt(process.env.LADDER_ROLL_COUNT, 10);
const LADDER_BUST_ON = parseInt(process.env.LADDER_BUST_ON, 10);
const DICE_ESCALATOR_BUST_ON = parseInt(process.env.DICE_ESCALATOR_BUST_ON, 10);

const LADDER_PAYOUTS = [
    { min: 10, max: 14, multiplier: 1, label: "Nice Climb!" },
    { min: 15, max: 19, multiplier: 2, label: "High Rungs!" },
    { min: 20, max: 24, multiplier: 5, label: "Peak Performer!" },
    { min: 25, max: 29, multiplier: 10, label: "Sky High Roller!" },
    { min: 30, max: 30, multiplier: 25, label: "Ladder Legend!" }
];

// --- MINES GAME CONSTANTS (REVISED FOR DIFFICULTY) ---
const MINES_DEFAULT_ROWS = parseInt(process.env.MINES_DEFAULT_ROWS, 10);
const MINES_DEFAULT_COLS = parseInt(process.env.MINES_DEFAULT_COLS, 10);
const MINES_FALLBACK_DEFAULT_MINES = parseInt(process.env.MINES_FALLBACK_DEFAULT_MINES, 10); 
const MINES_MIN_MINES = parseInt(process.env.MINES_MIN_MINES, 10);
const MINES_MAX_MINES_PERCENT = parseFloat(process.env.MINES_MAX_MINES_PERCENT);

const MINES_DIFFICULTY_CONFIG = {
    easy: { 
        rows: 5, cols: 5, mines: 3, label: "Easy (5x5, 3 Mines)",
        // Total cells: 25, Safe cells: 22
        // Multipliers are TOTAL PAYOUT (stake included). Index = gems found. multipliers[0] is unused.
        multipliers: [ 0, 1.08, 1.18, 1.29, 1.42, 1.55, 1.70, 1.88, 2.08, 2.30, 2.55, 
                       2.85, 3.20, 3.60, 4.05, 4.50, 5.00, 6.00, 7.50, 10.00, 15.00, 25.00, 50.00 ]
    },
    medium: { 
        rows: 5, cols: 5, mines: 5, label: "Medium (5x5, 5 Mines)",
        // Total cells: 25, Safe cells: 20
        multipliers: [ 0, 1.12, 1.28, 1.47, 1.70, 1.98, 2.30, 2.70, 3.15, 3.70, 4.35,
                       5.10, 6.00, 7.10, 8.50, 10.50, 13.00, 16.50, 22.00, 30.00, 75.00 ]
    },
    hard:   { 
        rows: 5, cols: 5, mines: 7, label: "Hard (5x5, 7 Mines)",
        // Total cells: 25, Safe cells: 18
        multipliers: [ 0, 1.18, 1.40, 1.68, 2.00, 2.40, 2.90, 3.50, 4.20, 5.10, 6.20,
                       7.50, 9.20, 11.50, 14.50, 18.00, 23.00, 30.00, 100.00 ]
    },
};
// --- END OF MINES GAME CONSTANTS ---

// Keypair Initializations
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
        REFERRAL_PAYOUT_KEYPAIR = null;
    }
} else {
    console.log("ℹ️ INFO: REFERRAL_PAYOUT_PRIVATE_KEY not set. Main bot wallet will be used for referral payouts.");
}
if (!REFERRAL_PAYOUT_KEYPAIR) { // Fallback if not set or invalid
    REFERRAL_PAYOUT_KEYPAIR = MAIN_BOT_KEYPAIR;
}


// RPC Endpoint Configuration
const RPC_URLS_LIST_FROM_ENV = (process.env.RPC_URLS || '')
    .split(',')
    .map(u => u.trim())
    .filter(u => u && (u.startsWith('http://') || u.startsWith('https://')));

const SINGLE_MAINNET_RPC_FROM_ENV = process.env.SOLANA_RPC_URL || null;

let combinedRpcEndpointsForConnection = [...RPC_URLS_LIST_FROM_ENV];
if (SINGLE_MAINNET_RPC_FROM_ENV && !combinedRpcEndpointsForConnection.some(url => url.startsWith(SINGLE_MAINNET_RPC_FROM_ENV.split('?')[0]))) {
    combinedRpcEndpointsForConnection.push(SINGLE_MAINNET_RPC_FROM_ENV);
}
if (combinedRpcEndpointsForConnection.length === 0) { // Absolute fallback if nothing is provided
    console.warn("⚠️ WARNING: No RPC URLs provided (RPC_URLS, SOLANA_RPC_URL). Using default public Solana RPC as a last resort.");
    combinedRpcEndpointsForConnection.push('https://api.mainnet-beta.solana.com/');
}


// More Constants derived from ENV
const SHUTDOWN_FAIL_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
const MAX_RETRY_POLLING_DELAY = parseInt(process.env.MAX_RETRY_POLLING_DELAY, 10);
const INITIAL_RETRY_POLLING_DELAY = parseInt(process.env.INITIAL_RETRY_POLLING_DELAY, 10);
const JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.JACKPOT_CONTRIBUTION_PERCENT); 
const MAIN_JACKPOT_ID = 'dice_escalator_main_pvb'; 
const TARGET_JACKPOT_SCORE = parseInt(process.env.TARGET_JACKPOT_SCORE, 10); 

const MIN_BET_AMOUNT_LAMPORTS_config = BigInt(process.env.MIN_BET_AMOUNT_LAMPORTS);
const MAX_BET_AMOUNT_LAMPORTS_config = BigInt(process.env.MAX_BET_AMOUNT_LAMPORTS);
const MIN_BET_USD_val = parseFloat(process.env.MIN_BET_USD);
const MAX_BET_USD_val = parseFloat(process.env.MAX_BET_USD);

const COMMAND_COOLDOWN_MS = parseInt(process.env.COMMAND_COOLDOWN_MS, 10);
const DEFAULT_STARTING_BALANCE_LAMPORTS = BigInt(process.env.DEFAULT_STARTING_BALANCE_LAMPORTS);
const RULES_CALLBACK_PREFIX = process.env.RULES_CALLBACK_PREFIX;
const DEPOSIT_CALLBACK_ACTION = process.env.DEPOSIT_CALLBACK_ACTION;
const WITHDRAW_CALLBACK_ACTION = process.env.WITHDRAW_CALLBACK_ACTION;
const QUICK_DEPOSIT_CALLBACK_ACTION = process.env.QUICK_DEPOSIT_CALLBACK_ACTION;

const SOL_DECIMALS = 9; 
const DEPOSIT_ADDRESS_EXPIRY_MINUTES = parseInt(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10);
const DEPOSIT_ADDRESS_EXPIRY_MS = DEPOSIT_ADDRESS_EXPIRY_MINUTES * 60 * 1000;
const DEPOSIT_CONFIRMATION_LEVEL = process.env.DEPOSIT_CONFIRMATIONS?.toLowerCase() || 'confirmed';
const WITHDRAWAL_FEE_LAMPORTS = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS);
const MIN_WITHDRAWAL_LAMPORTS = BigInt(process.env.MIN_WITHDRAWAL_LAMPORTS);


// Critical Configuration Validations
if (!BOT_TOKEN) { console.error("🚨 FATAL ERROR: BOT_TOKEN is not defined. Bot cannot start."); process.exit(1); }
if (!DATABASE_URL) { console.error("🚨 FATAL ERROR: DATABASE_URL is not defined. Cannot connect to PostgreSQL."); process.exit(1); }
if (!DEPOSIT_MASTER_SEED_PHRASE) { console.error("🚨 FATAL ERROR: DEPOSIT_MASTER_SEED_PHRASE is not defined. Payment system cannot generate deposit addresses."); process.exit(1); }

const criticalGameScoresCheck = { TARGET_JACKPOT_SCORE, DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE, OU7_DICE_COUNT, DUEL_DICE_COUNT, LADDER_ROLL_COUNT, LADDER_BUST_ON, DICE_ESCALATOR_BUST_ON };
for (const [key, value] of Object.entries(criticalGameScoresCheck)) {
    if (isNaN(value) || value <=0) {
        console.error(`🚨 FATAL ERROR: Game score/parameter '${key}' ('${value}') is not a valid positive number.`);
        process.exit(1);
    }
}
if (isNaN(MIN_BET_USD_val) || MIN_BET_USD_val <= 0) {
    console.error(`🚨 FATAL ERROR: MIN_BET_USD ('${process.env.MIN_BET_USD}') must be a positive number.`);
    process.exit(1);
}
if (isNaN(MAX_BET_USD_val) || MAX_BET_USD_val < MIN_BET_USD_val) {
    console.error(`🚨 FATAL ERROR: MAX_BET_USD ('${process.env.MAX_BET_USD}') must be >= MIN_BET_USD and be a number.`);
    process.exit(1);
}
if (MIN_BET_AMOUNT_LAMPORTS_config < 1n || isNaN(Number(MIN_BET_AMOUNT_LAMPORTS_config))) {
    console.error(`🚨 FATAL ERROR: MIN_BET_AMOUNT_LAMPORTS ('${MIN_BET_AMOUNT_LAMPORTS_config}') must be a positive number.`);
    process.exit(1);
}
if (MAX_BET_AMOUNT_LAMPORTS_config < MIN_BET_AMOUNT_LAMPORTS_config || isNaN(Number(MAX_BET_AMOUNT_LAMPORTS_config))) {
    console.error(`🚨 FATAL ERROR: MAX_BET_AMOUNT_LAMPORTS ('${MAX_BET_AMOUNT_LAMPORTS_config}') must be >= MIN_BET_AMOUNT_LAMPORTS and be a number.`);
    process.exit(1);
}
if (isNaN(JACKPOT_CONTRIBUTION_PERCENT) || JACKPOT_CONTRIBUTION_PERCENT < 0 || JACKPOT_CONTRIBUTION_PERCENT >= 1) {
    console.error(`🚨 FATAL ERROR: JACKPOT_CONTRIBUTION_PERCENT ('${process.env.JACKPOT_CONTRIBUTION_PERCENT}') must be a number between 0 (inclusive) and 1 (exclusive).`);
    process.exit(1);
}
if (isNaN(OU7_PAYOUT_NORMAL) || OU7_PAYOUT_NORMAL < 0) {
    console.error(`🚨 FATAL ERROR: OU7_PAYOUT_NORMAL must be a non-negative number.`); process.exit(1);
}
if (isNaN(OU7_PAYOUT_SEVEN) || OU7_PAYOUT_SEVEN < 0) {
    console.error(`🚨 FATAL ERROR: OU7_PAYOUT_SEVEN must be a non-negative number.`); process.exit(1);
}

if (isNaN(MINES_DEFAULT_ROWS) || MINES_DEFAULT_ROWS < 3 || MINES_DEFAULT_ROWS > 8) { console.error("FATAL: MINES_DEFAULT_ROWS must be a number between 3-8 for reasonable button display."); process.exit(1); }
if (isNaN(MINES_DEFAULT_COLS) || MINES_DEFAULT_COLS < 3 || MINES_DEFAULT_COLS > 8) { console.error("FATAL: MINES_DEFAULT_COLS must be a number between 3-8."); process.exit(1); }
if (isNaN(MINES_FALLBACK_DEFAULT_MINES) || MINES_FALLBACK_DEFAULT_MINES < 1) { console.error("FATAL: MINES_FALLBACK_DEFAULT_MINES must be at least 1."); process.exit(1); }
if (MINES_FALLBACK_DEFAULT_MINES >= MINES_DEFAULT_ROWS * MINES_DEFAULT_COLS) { console.error("FATAL: MINES_FALLBACK_DEFAULT_MINES must be less than total cells."); process.exit(1); }
if (isNaN(MINES_MIN_MINES) || MINES_MIN_MINES < 1) { console.error("FATAL: MINES_MIN_MINES must be at least 1."); process.exit(1); }
if (isNaN(MINES_MAX_MINES_PERCENT) || MINES_MAX_MINES_PERCENT <= 0 || MINES_MAX_MINES_PERCENT >= 0.8) { console.error("FATAL: MINES_MAX_MINES_PERCENT must be between 0 (exclusive) and 0.8 (exclusive for playability)."); process.exit(1); }

for (const key in MINES_DIFFICULTY_CONFIG) {
    const config = MINES_DIFFICULTY_CONFIG[key];
    if (isNaN(config.rows) || config.rows < 2 || config.rows > 8) { console.error(`FATAL: MINES_DIFFICULTY_CONFIG.${key}.rows must be 2-8.`); process.exit(1); }
    if (isNaN(config.cols) || config.cols < 2 || config.cols > 8) { console.error(`FATAL: MINES_DIFFICULTY_CONFIG.${key}.cols must be 2-8.`); process.exit(1); }
    if (isNaN(config.mines) || config.mines < MINES_MIN_MINES) { console.error(`FATAL: MINES_DIFFICULTY_CONFIG.${key}.mines must be >= MINES_MIN_MINES.`); process.exit(1); }
    if (config.mines >= config.rows * config.cols) { console.error(`FATAL: MINES_DIFFICULTY_CONFIG.${key}.mines must be less than total cells.`); process.exit(1); }
    if (config.mines > Math.floor((config.rows * config.cols) * MINES_MAX_MINES_PERCENT)) {console.error(`FATAL: MINES_DIFFICULTY_CONFIG.${key}.mines exceeds MINES_MAX_MINES_PERCENT for its grid size.`); process.exit(1); }
    if (!Array.isArray(config.multipliers) || config.multipliers.length !== (config.rows * config.cols - config.mines + 1)) { console.error(`FATAL: MINES_DIFFICULTY_CONFIG.${key}.multipliers array is missing or has incorrect length. Expected ${config.rows * config.cols - config.mines + 1} entries (0 gems + N gems).`); process.exit(1); }
}


if (ADMIN_USER_ID) console.log(`ℹ️ Admin User ID: ${ADMIN_USER_ID} loaded.`);

function formatLamportsToSolStringForLog(lamports) {
    if (typeof lamports !== 'bigint' && typeof lamports !== 'number') { 
        try { lamports = BigInt(lamports); }
        catch (e) { return 'Invalid_Lamports_Input'; }
    } else if (typeof lamports === 'number') {
        lamports = BigInt(lamports);
    }
    if (lamports === undefined || lamports === null) return 'N/A_Lamports';
    return (Number(lamports) / Number(LAMPORTS_PER_SOL)).toFixed(SOL_DECIMALS);
}

console.log(`--- ⚙️ Key Game & Bot Configurations Loaded ---
  Dice Escalator (PvB): Target Jackpot Score: ${TARGET_JACKPOT_SCORE}, Player Bust On: ${DICE_ESCALATOR_BUST_ON}, Jackpot Fee: ${JACKPOT_CONTRIBUTION_PERCENT * 100}%
  Dice 21 (Blackjack): Target Score: ${DICE_21_TARGET_SCORE}, Bot Stand: ${DICE_21_BOT_STAND_SCORE}
  Mines Config (Example 'easy'): Grid ${MINES_DIFFICULTY_CONFIG.easy.rows}x${MINES_DIFFICULTY_CONFIG.easy.cols}, Mines: ${MINES_DIFFICULTY_CONFIG.easy.mines}
  Bet Limits (USD): $${MIN_BET_USD_val.toFixed(2)} - $${MAX_BET_USD_val.toFixed(2)} (Lamports Ref: ${formatLamportsToSolStringForLog(MIN_BET_AMOUNT_LAMPORTS_config)} SOL - ${formatLamportsToSolStringForLog(MAX_BET_AMOUNT_LAMPORTS_config)} SOL)
  Default Starting Credits: ${formatLamportsToSolStringForLog(DEFAULT_STARTING_BALANCE_LAMPORTS)} SOL
  Command Cooldown: ${COMMAND_COOLDOWN_MS / 1000}s, Game Join Timeout (Offers): ${JOIN_GAME_TIMEOUT_MS / 1000 / 60}min
  Min Withdrawal: ${formatLamportsToSolStringForLog(MIN_WITHDRAWAL_LAMPORTS)} SOL, Fee: ${formatLamportsToSolStringForLog(WITHDRAWAL_FEE_LAMPORTS)} SOL
  Deposit Address Expiry: ${DEPOSIT_ADDRESS_EXPIRY_MINUTES} minutes
  SOL/USD Price API: ${process.env.SOL_PRICE_API_URL}
  Dice Roll Polling (Helper Bot System): Interval ${DICE_ROLL_POLLING_INTERVAL_MS}ms, Max Attempts ${DICE_ROLL_POLLING_MAX_ATTEMPTS}
  Sweep Fee Buffer (for TX cost): ${formatLamportsToSolStringForLog(BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS))} SOL
-------------------------------------------------`);

const useSsl = process.env.DB_SSL === 'true';
const rejectUnauthorizedSsl = process.env.DB_REJECT_UNAUTHORIZED === 'true';

const pool = new Pool({
  connectionString: DATABASE_URL,
  max: parseInt(process.env.DB_POOL_MAX, 10),
  min: parseInt(process.env.DB_POOL_MIN, 10),
  idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT, 10),
  connectionTimeoutMillis: parseInt(process.env.DB_CONN_TIMEOUT, 10),
  ssl: useSsl ? { rejectUnauthorized: rejectUnauthorizedSsl } : false,
});

pool.on('error', (err, client) => {
  console.error('❌ Unexpected error on idle PostgreSQL client', err);
  if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
    const adminMessage = `🚨 *DATABASE POOL ERROR* 🚨\nAn unexpected error occurred with an idle PostgreSQL client:\n\n*Error Message:*\n\`${escapeMarkdownV2(String(err.message || err))}\`\n\nPlease check the server logs for more details.`;
    safeSendMessage(ADMIN_USER_ID, adminMessage, { parse_mode: 'MarkdownV2' })
      .catch(notifyErr => console.error("Failed to notify admin about DB pool error:", notifyErr));
  } else {
    console.error(`[Admin Alert Failure] DB Pool Error (Idle Client): ${err.message || String(err)} (safeSendMessage, escapeMarkdownV2, or ADMIN_USER_ID unavailable)`);
  }
});

async function queryDatabase(sql, params = [], dbClient = pool) {
    const logPrefix = '[DB_Query]';
    try {
        const result = await dbClient.query(sql, params);
        return result;
    } catch (error) {
        const sqlPreviewOnError = sql.length > 200 ? `${sql.substring(0, 197)}...` : sql;
        const paramsPreviewOnError = params.map(p => (typeof p === 'string' && p.length > 50) ? `${p.substring(0, 47)}...` : ((typeof p === 'bigint') ? p.toString() + 'n' : p) );

        console.error(`${logPrefix} ❌ Error executing query.`);
        console.error(`${logPrefix} SQL that failed (Preview): [${sqlPreviewOnError}]`);
        console.error(`${logPrefix} PARAMS for failed SQL: [${paramsPreviewOnError.join(', ')}]`);
        console.error(`${logPrefix} Error Details: Message: ${error.message}, Code: ${error.code || 'N/A'}, Position: ${error.position || 'N/A'}`);
        if (error.stack) {
            console.error(`${logPrefix} Stack (Partial): ${error.stack.substring(0,500)}...`);
        }
        throw error; 
    }
}

const connectionOptions = {
    commitment: process.env.RPC_COMMITMENT, 
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
};

const solanaConnection = new RateLimitedConnection(
    combinedRpcEndpointsForConnection,
    connectionOptions
);

const bot = new TelegramBot(BOT_TOKEN, { polling: true });

let app = null; 
if (process.env.ENABLE_PAYMENT_WEBHOOKS === 'true') {
    app = express();
    app.use(express.json({
        verify: (req, res, buf) => {
            req.rawBody = buf; 
        }
    }));
}

const BOT_VERSION = process.env.BOT_VERSION || '3.4.0-de-rewrite'; 
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;
let isShuttingDown = false;
let activeGames = new Map(); 
let userCooldowns = new Map(); 
let groupGameSessions = new Map(); 
const walletCache = new Map(); 
const activeDepositAddresses = new Map(); 
const processedDepositTxSignatures = new Set(); 
const PENDING_REFERRAL_TTL_MS = 24 * 60 * 60 * 1000; 
const pendingReferrals = new Map(); 
const userStateCache = new Map(); 
const SOL_PRICE_CACHE_KEY = 'sol_usd_price_cache';
const solPriceCache = new Map(); 

const escapeMarkdownV2 = (text) => {
  if (text === null || typeof text === 'undefined') return '';
  return String(text).replace(/([_*\[\]~`>#+\-=|{}!'\\])/g, '\\$1');
};

async function safeSendMessage(chatId, text, options = {}) {
    const LOG_PREFIX_SSM = `[SafeSend CH:${chatId}]`;
    if (!chatId || typeof text !== 'string') {
        console.error(`${LOG_PREFIX_SSM} Invalid input: ChatID ${chatId}, Text type ${typeof text}. Preview: ${String(text).substring(0, 100)}`);
        return undefined;
    }

    let messageToSend = text;
    let finalOptions = { ...options }; 

    if (finalOptions.parse_mode === 'MarkdownV2' && messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsisBase = ` \\.\\.\\. (_message truncated by ${escapeMarkdownV2(BOT_NAME)}_)`; 
        const truncateAt = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsisBase.length);
        messageToSend = messageToSend.substring(0, truncateAt) + ellipsisBase;
    } else if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) { 
        const ellipsisPlain = `... (message truncated by ${BOT_NAME})`;
        const truncateAt = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsisPlain.length);
        messageToSend = messageToSend.substring(0, truncateAt) + ellipsisPlain;
    }

    if (!bot || typeof bot.sendMessage !== 'function') {
        console.error(`${LOG_PREFIX_SSM} ⚠️ Error: Telegram 'bot' instance or sendMessage function not available.`);
        return undefined;
    }

    try {
        const sentMessage = await bot.sendMessage(chatId, messageToSend, finalOptions);
        return sentMessage;
    } catch (error) {
        console.error(`${LOG_PREFIX_SSM} ❌ Failed to send. Code: ${error.code || 'N/A'}, Msg: ${error.message}`);
        if (error.response && error.response.body && error.response.body.description) {
            const errorDescription = error.response.body.description.toLowerCase();
            if (finalOptions.parse_mode === 'MarkdownV2' && (errorDescription.includes("can't parse entities") || errorDescription.includes("bad request"))) {
                console.warn(`${LOG_PREFIX_SSM} MarkdownV2 parse error detected by API: "${error.response.body.description}". Attempting plain text fallback for original text.`);
                try {
                    let plainTextFallbackOptions = { ...options }; 
                    delete plainTextFallbackOptions.parse_mode;

                    let plainTextForFallback = text; 
                    if (plainTextForFallback.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
                        const ellipsisPlainFallback = `... (message truncated by ${BOT_NAME}, original was Markdown error)`;
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
        return undefined; 
    }
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function notifyAdmin(message, options = {}) {
    if (ADMIN_USER_ID) {
        const adminAlertMessage = `🔔 *ADMIN ALERT* (${escapeMarkdownV2(BOT_NAME)}) 🔔\n\n${message}`; 
        return safeSendMessage(ADMIN_USER_ID, adminAlertMessage, { parse_mode: 'MarkdownV2', ...options });
    } else {
        return null;
    }
}

async function fetchSolUsdPriceFromAPI() {
    const apiUrl = process.env.SOL_PRICE_API_URL;
    const logPrefix = '[PriceFeed API]';
    try {
        const response = await axios.get(apiUrl, { timeout: 8000 }); 
        if (response.data && response.data.solana && typeof response.data.solana.usd === 'number') {
            const price = parseFloat(response.data.solana.usd);
            if (isNaN(price) || price <= 0) {
                throw new Error('Invalid or non-positive price data from API.');
            }
            return price;
        } else {
            console.error(`${logPrefix} ⚠️ SOL price not found or invalid structure in API response:`, stringifyWithBigInt(response.data).substring(0,300));
            throw new Error('SOL price not found or invalid structure in API response.');
        }
    } catch (error) {
        const errMsg = error.isAxiosError ? error.message : String(error);
        console.error(`${logPrefix} ❌ Error fetching SOL/USD price: ${errMsg}`);
        if (error.response) {
            console.error(`${logPrefix} API Response Status: ${error.response.status}, Data:`, stringifyWithBigInt(error.response.data).substring(0,300));
        }
        throw new Error(`Failed to fetch SOL/USD price: ${errMsg}`); 
    }
}

async function getSolUsdPrice() {
    const logPrefix = '[GetSolUsdPrice]';
    const cacheTtl = parseInt(process.env.SOL_USD_PRICE_CACHE_TTL_MS, 10);
    const cachedEntry = solPriceCache.get(SOL_PRICE_CACHE_KEY);

    if (cachedEntry && (Date.now() - cachedEntry.timestamp < cacheTtl)) {
        return cachedEntry.price;
    }
    try {
        const price = await fetchSolUsdPriceFromAPI();
        solPriceCache.set(SOL_PRICE_CACHE_KEY, { price, timestamp: Date.now() });
        return price;
    } catch (error) {
        if (cachedEntry) { 
            console.warn(`${logPrefix} ⚠️ API fetch failed ('${error.message}'), using stale cached SOL/USD price: $${cachedEntry.price.toFixed(2)}`);
            return cachedEntry.price;
        }
        const criticalErrorMessage = `🚨 *CRITICAL PRICE FEED FAILURE* (${escapeMarkdownV2(BOT_NAME)}) 🚨\n\nUnable to fetch SOL/USD price and no cache available. USD conversions will be severely impacted.\n*Error:* \`${escapeMarkdownV2(error.message)}\``;
        console.error(`${logPrefix} ❌ CRITICAL: ${criticalErrorMessage.replace(/\n/g, ' ')}`); 
        if (typeof notifyAdmin === 'function') { 
            await notifyAdmin(criticalErrorMessage); 
        }
        throw new Error(`Critical: Could not retrieve SOL/USD price. Error: ${error.message}`); 
    }
}

function convertLamportsToUSDString(lamports, solUsdPrice, displayDecimals = 2) {
    if (typeof solUsdPrice !== 'number' || solUsdPrice <= 0) {
        return '⚠️ Price N/A';
    }
    let lamportsBigInt;
    try {
        lamportsBigInt = BigInt(lamports);
    } catch (e) {
        return '⚠️ Amount Error';
    }

    const solAmount = Number(lamportsBigInt) / Number(LAMPORTS_PER_SOL);
    const usdValue = solAmount * solUsdPrice;
    return `$${usdValue.toLocaleString('en-US', { minimumFractionDigits: displayDecimals, maximumFractionDigits: displayDecimals })}`;
}

function convertUSDToLamports(usdAmount, solUsdPrice) {
    if (typeof solUsdPrice !== 'number' || solUsdPrice <= 0) {
        throw new Error("SOL/USD price must be a positive number for USD to Lamports conversion.");
    }
    const parsedUsdAmount = parseFloat(String(usdAmount).replace(/[^0-9.-]+/g,""));
    if (isNaN(parsedUsdAmount)) {
        throw new Error("Invalid USD amount for conversion.");
    }
    const solAmount = parsedUsdAmount / solUsdPrice;
    return BigInt(Math.floor(solAmount * Number(LAMPORTS_PER_SOL))); 
}

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

const SLOT_PAYOUTS = {
    64: { multiplier: 100, symbols: "💎💎💎", label: "MEGA JACKPOT!" }, 
    1:  { multiplier: 20,  symbols: "7️⃣7️⃣7️⃣", label: "TRIPLE SEVEN!" },  
    22: { multiplier: 10,  symbols: "🍋🍋🍋", label: "Triple Lemon!" }, 
    43: { multiplier: 5,   symbols: "🔔🔔🔔", label: "Triple Bell!" },  
};
const SLOT_DEFAULT_LOSS_MULTIPLIER = -1; 
// --- End of Part 1 ---
// --- Start of Part 2 (Modified for dice_roll_requests table) ---
// index.js - Part 2: Database Schema Initialization & Core User Management
//---------------------------------------------------------------------------
// Assumed necessary functions and constants from Part 1 are available.
// Specifically: pool, DEFAULT_STARTING_BALANCE_LAMPORTS, escapeMarkdownV2,
// MAIN_JACKPOT_ID, PublicKey (from @solana/web3.js), walletCache,
// activeGames, userCooldowns, groupGameSessions, activeDepositAddresses,
// pendingReferrals, userStateCache, GAME_IDS (if used for activeGames clearing).
// notifyAdmin and ADMIN_USER_ID are used for critical error reporting.

// --- Helper function for referral code generation ---
const generateReferralCode = (length = 8) => {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
};

//---------------------------------------------------------------------------
// Database Schema Initialization
//---------------------------------------------------------------------------
// Replace your entire existing initializeDatabaseSchema function with this:
async function initializeDatabaseSchema() {
    console.log("⚙️ V9 FINAL CLEAN WHITESPACE: Initializing FULL database schema (All Tables & Triggers)..."); // Changed log slightly for clarity
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        console.log("DEBUG V9 FINAL: BEGIN executed.");

        // Users Table
        console.log("DEBUG V9 FINAL: Creating Users table...");
        await client.query(`CREATE TABLE IF NOT EXISTS users (
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
);`);
        console.log("DEBUG V9 FINAL: Users table processed.");

        // Jackpots Table
        console.log("DEBUG V9 FINAL: Creating Jackpots table...");
        await client.query(`CREATE TABLE IF NOT EXISTS jackpots (
    jackpot_id VARCHAR(255) PRIMARY KEY,
    current_amount BIGINT DEFAULT 0,
    last_won_by_telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE SET NULL,
    last_won_timestamp TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);`);
        await client.query(
            `INSERT INTO jackpots (jackpot_id, current_amount) VALUES ($1, 0) ON CONFLICT (jackpot_id) DO NOTHING;`,
            [MAIN_JACKPOT_ID]
        );
        console.log("DEBUG V9 FINAL: Jackpots table processed.");

        // Games Table (Game Log)
        console.log("DEBUG V9 FINAL: Creating Games table...");
        await client.query(`CREATE TABLE IF NOT EXISTS games (
    game_log_id SERIAL PRIMARY KEY,
    game_type VARCHAR(50) NOT NULL,
    chat_id BIGINT,
    initiator_telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE SET NULL,
    participants_ids BIGINT[],
    bet_amount_lamports BIGINT,
    outcome TEXT,
    jackpot_contribution_lamports BIGINT,
    game_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);`);
        console.log("DEBUG V9 FINAL: Games table processed.");

        // User Deposit Wallets Table
        console.log("DEBUG V9 FINAL: Creating User Deposit Wallets table...");
        await client.query(`CREATE TABLE IF NOT EXISTS user_deposit_wallets (
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
);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_user_id ON user_deposit_wallets(user_telegram_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_public_key ON user_deposit_wallets(public_key);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_user_deposit_wallets_is_active_expires_at ON user_deposit_wallets(is_active, expires_at);`);
        console.log("DEBUG V9 FINAL: User Deposit Wallets table processed.");

        // Deposits Table
        console.log("DEBUG V9 FINAL: Creating Deposits table...");
        await client.query(`CREATE TABLE IF NOT EXISTS deposits (
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
);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits(user_telegram_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_deposits_transaction_signature ON deposits(transaction_signature);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_deposits_deposit_address ON deposits(deposit_address);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_deposits_status_created_at ON deposits(confirmation_status, created_at);`);
        console.log("DEBUG V9 FINAL: Deposits table processed.");

        // Withdrawals Table
        console.log("DEBUG V9 FINAL: Creating Withdrawals table...");
        await client.query(`CREATE TABLE IF NOT EXISTS withdrawals (
    withdrawal_id SERIAL PRIMARY KEY,
    user_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    destination_address VARCHAR(44) NOT NULL,
    amount_lamports BIGINT NOT NULL,
    fee_lamports BIGINT NOT NULL,
    transaction_signature VARCHAR(88) UNIQUE,
    status VARCHAR(30) DEFAULT 'pending_verification',
    error_message TEXT,
    requested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    block_time BIGINT,
    priority_fee_microlamports INT,
    compute_unit_price_microlamports INT,
    compute_unit_limit INT,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_withdrawals_user_id ON withdrawals(user_telegram_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_withdrawals_status_requested_at ON withdrawals(status, requested_at);`);
        console.log("DEBUG V9 FINAL: Withdrawals table processed.");

        // Referrals Table
        console.log("DEBUG V9 FINAL: Creating Referrals table...");
        await client.query(`CREATE TABLE IF NOT EXISTS referrals (
    referral_id SERIAL PRIMARY KEY,
    referrer_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    referred_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE UNIQUE,
    commission_type VARCHAR(20),
    commission_amount_lamports BIGINT,
    transaction_signature VARCHAR(88),
    status VARCHAR(20) DEFAULT 'pending_criteria',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_referral_pair UNIQUE (referrer_telegram_id, referred_telegram_id)
);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_referrals_referrer_id ON referrals(referrer_telegram_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_referrals_referred_id ON referrals(referred_telegram_id);`);
        console.log("DEBUG V9 FINAL: Referrals table processed.");

        // Processed Sweeps Table
        console.log("DEBUG V9 FINAL: Creating Processed Sweeps table...");
        await client.query(`CREATE TABLE IF NOT EXISTS processed_sweeps (
    sweep_id SERIAL PRIMARY KEY,
    source_deposit_address VARCHAR(44) NOT NULL,
    destination_main_address VARCHAR(44) NOT NULL,
    amount_lamports BIGINT NOT NULL,
    transaction_signature VARCHAR(88) UNIQUE NOT NULL,
    swept_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_processed_sweeps_source_address ON processed_sweeps(source_deposit_address);`);
        console.log("DEBUG V9 FINAL: Processed Sweeps table processed.");

        // Ledger Table
        console.log("DEBUG V9 FINAL: Creating Ledger table...");
        await client.query(`CREATE TABLE IF NOT EXISTS ledger (
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
);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ledger_user_id ON ledger(user_telegram_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ledger_transaction_type ON ledger(transaction_type);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON ledger(created_at);`);
        console.log("DEBUG V9 FINAL: Ledger table processed.");

        // Dice Roll Requests Table
        console.log("DEBUG V9 FINAL: Creating Dice Roll Requests table...");
        await client.query(`CREATE TABLE IF NOT EXISTS dice_roll_requests (
    request_id SERIAL PRIMARY KEY,
    game_id VARCHAR(255) NULL,
    chat_id BIGINT NOT NULL,
    user_id BIGINT NULL,
    emoji_type VARCHAR(50) DEFAULT '🎲',
    status VARCHAR(50) DEFAULT 'pending',
    roll_value INTEGER NULL,
    requested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ NULL,
    notes TEXT NULL
);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_status_requested ON dice_roll_requests(status, requested_at);`);
        console.log("DEBUG V9 FINAL: Dice Roll Requests table processed.");

        // Update function for 'updated_at' columns
        console.log("DEBUG V9 FINAL: Creating/Ensuring trigger function trigger_set_timestamp...");
        await client.query(`
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;`);
        console.log("DEBUG V9 FINAL: Trigger function processed. Applying triggers to tables...");
        const tablesWithUpdatedAt = ['users', 'jackpots', 'user_deposit_wallets', 'deposits', 'withdrawals', 'referrals'];
        for (const tableName of tablesWithUpdatedAt) {
            console.log(`DEBUG V9 FINAL: Checking/Setting trigger for ${tableName}...`);
            const triggerExistsQuery = `SELECT 1 FROM pg_trigger WHERE tgname = 'set_timestamp' AND tgrelid = $1::regclass;`;
            const triggerExistsRes = await client.query(triggerExistsQuery, [tableName]);

            if (triggerExistsRes.rowCount === 0) {
                const createTriggerQuery = `
CREATE TRIGGER set_timestamp
BEFORE UPDATE ON ${tableName}
FOR EACH ROW
EXECUTE FUNCTION trigger_set_timestamp();`;
                await client.query(createTriggerQuery).catch(err => console.warn(`[DB Schema] Could not set update trigger for ${tableName}: ${err.message}`));
            }
        }
        console.log("DEBUG V9 FINAL: Triggers processed.");

        await client.query('COMMIT');
        console.log("✅ V9 FINAL CLEAN WHITESPACE: Database schema initialization complete (ALL TABLES & TRIGGERS).");

    } catch (e) {
        try {
            console.log("DEBUG V9 FINAL: Error caught, attempting ROLLBACK...");
            await client.query('ROLLBACK');
            console.log("DEBUG V9 FINAL: ROLLBACK executed.");
        } catch (rbError) {
            console.error("DEBUG V9 FINAL: Error during ROLLBACK attempt:", rbError);
        }
        console.error('❌ V9 FINAL CLEAN WHITESPACE: Error during database schema initialization:', e);
        throw e; // Re-throw the original error to be caught by the main init error handler
    } finally {
        client.release();
    }
}

//---------------------------------------------------------------------------
// Core User Management Functions
//---------------------------------------------------------------------------

// Replace your entire existing getOrCreateUser function (in Part 2) with this:
async function getOrCreateUser(telegramId, username = '', firstName = '', lastName = '', referrerIdInput = null) {
    const LOG_PREFIX_GOCU_DEBUG = `[DEBUG getOrCreateUser ENTER]`; // Keep this for entry logging
    console.log(`${LOG_PREFIX_GOCU_DEBUG} Received telegramId: ${telegramId} (type: ${typeof telegramId}), username: "${username}", firstName: "${firstName}", lastName: "${lastName}", referrerIdInput: ${referrerIdInput}`);
    try {
        const argsArray = Array.from(arguments);
        console.log(`${LOG_PREFIX_GOCU_DEBUG} All arguments received as array: ${JSON.stringify(argsArray)}`);
    } catch (e) {
        console.log(`${LOG_PREFIX_GOCU_DEBUG} Could not stringify arguments array: ${e.message}`);
    }

    if (typeof telegramId === 'undefined' || telegramId === null || String(telegramId).trim() === "" || String(telegramId).toLowerCase() === "undefined") {
        console.error(`[GetCreateUser CRITICAL] Invalid telegramId: '${telegramId}'. Aborting.`);
        console.trace("Trace for undefined telegramId call");
        if (typeof notifyAdmin === 'function' && ADMIN_USER_ID) {
            notifyAdmin(`🚨 CRITICAL: getOrCreateUser called with invalid telegramId: ${telegramId}\\. Username hint: ${username}, Name hint: ${firstName}. Check trace in logs.`)
                .catch(err => console.error("Failed to notify admin about invalid telegramId in getOrCreateUser:", err));
        }
        return null;
    }

    const stringTelegramId = String(telegramId).trim(); // Added trim here too
    const LOG_PREFIX_GOCU = `[GetCreateUser TG:${stringTelegramId}]`;

    // Simple sanitization: replace non-printable ASCII, some problematic chars, and trim.
    // Allow letters, numbers, spaces, common punctuation like .,!?-#@_
    // This is a basic sanitizer; for extreme cases, a more robust library might be needed.
    const sanitizeString = (str) => {
        if (typeof str !== 'string') return null; // Or return an empty string like ''
        let cleaned = str.replace(/[^\w\s.,!?\-#@_]/g, '').trim(); // Remove characters not in the allowed set
        return cleaned.substring(0, 255); // Ensure it fits VARCHAR(255)
    };

    const sUsername = username ? sanitizeString(username) : null;
    const sFirstName = firstName ? sanitizeString(firstName) : null;
    // For lastName "#2", sanitizeString should keep it as "#2" if # is in the allowed set.
    // If # was causing issues, we could explicitly remove or replace it.
    // Let's be a bit more aggressive for testing, then can refine the regex.
    const sLastName = lastName ? sanitizeString(lastName) : null;

    console.log(`${LOG_PREFIX_GOCU} Sanitized inputs - Username: "${sUsername}", FirstName: "${sFirstName}", LastName: "${sLastName}"`);


    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        let referrerId = null;
        if (referrerIdInput !== null && referrerIdInput !== undefined) {
            try {
                referrerId = BigInt(referrerIdInput);
            } catch (parseError) {
                referrerId = null;
            }
        }

        let result = await client.query('SELECT * FROM users WHERE telegram_id = $1', [stringTelegramId]);
        if (result.rows.length > 0) {
            const user = result.rows[0];
            user.balance = BigInt(user.balance);
            user.total_deposited_lamports = BigInt(user.total_deposited_lamports || '0');
            user.total_withdrawn_lamports = BigInt(user.total_withdrawn_lamports || '0');
            user.total_wagered_lamports = BigInt(user.total_wagered_lamports || '0');
            user.total_won_lamports = BigInt(user.total_won_lamports || '0');
            if (user.referrer_telegram_id) user.referrer_telegram_id = String(user.referrer_telegram_id);

            let detailsChanged = false;
            const currentUsername = user.username || '';
            const currentFirstName = user.first_name || '';
            const currentLastName = user.last_name || '';

            // Use sanitized values for comparison and update
            if (sUsername && currentUsername !== sUsername) detailsChanged = true;
            if (sFirstName && currentFirstName !== sFirstName) detailsChanged = true;
            if (sLastName && currentLastName !== sLastName) detailsChanged = true;
            if (!currentUsername && sUsername) detailsChanged = true;
            if (!currentFirstName && sFirstName) detailsChanged = true;
            if (!currentLastName && sLastName && sLastName !== '') detailsChanged = true;

            if (detailsChanged) {
                await client.query(
                    'UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP, username = $2, first_name = $3, last_name = $4, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $1',
                    [stringTelegramId, sUsername || user.username, sFirstName || user.first_name, sLastName || user.last_name]
                );
            } else {
                await client.query('UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP WHERE telegram_id = $1', [stringTelegramId]);
            }
            await client.query('COMMIT');
            const updatedUserRow = await client.query('SELECT * FROM users WHERE telegram_id = $1', [stringTelegramId]);
            const finalUser = updatedUserRow.rows[0];
            finalUser.balance = BigInt(finalUser.balance);
            finalUser.total_deposited_lamports = BigInt(finalUser.total_deposited_lamports || '0');
            finalUser.total_withdrawn_lamports = BigInt(finalUser.total_withdrawn_lamports || '0');
            finalUser.total_wagered_lamports = BigInt(finalUser.total_wagered_lamports || '0');
            finalUser.total_won_lamports = BigInt(finalUser.total_won_lamports || '0');
            if (finalUser.referrer_telegram_id) finalUser.referrer_telegram_id = String(finalUser.referrer_telegram_id);
            return finalUser;
        } else {
            console.log(`${LOG_PREFIX_GOCU} User not found. Creating new user with sanitized details.`);
            const newReferralCode = generateReferralCode();
            const insertQuery = `
                INSERT INTO users (telegram_id, username, first_name, last_name, balance, referral_code, referrer_telegram_id, last_active_timestamp, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING *;
            `;
            // Use sanitized values for insert
            const values = [stringTelegramId, sUsername, sFirstName, sLastName, DEFAULT_STARTING_BALANCE_LAMPORTS.toString(), newReferralCode, referrerId];
            result = await client.query(insertQuery, values);
            const newUser = result.rows[0];

            newUser.balance = BigInt(newUser.balance);
            newUser.total_deposited_lamports = BigInt(newUser.total_deposited_lamports || '0');
            newUser.total_withdrawn_lamports = BigInt(newUser.total_withdrawn_lamports || '0');
            newUser.total_wagered_lamports = BigInt(newUser.total_wagered_lamports || '0');
            newUser.total_won_lamports = BigInt(newUser.total_won_lamports || '0');
            if (newUser.referrer_telegram_id) newUser.referrer_telegram_id = String(newUser.referrer_telegram_id);

            console.log(`${LOG_PREFIX_GOCU} New user created: ${newUser.telegram_id}, Bal: ${newUser.balance}, RefCode: ${newUser.referral_code}.`);

            if (referrerId) {
                try {
                    await client.query(
                        `INSERT INTO referrals (referrer_telegram_id, referred_telegram_id, created_at, status, updated_at) 
                         VALUES ($1, $2, CURRENT_TIMESTAMP, 'pending_criteria', CURRENT_TIMESTAMP) 
                         ON CONFLICT (referrer_telegram_id, referred_telegram_id) DO NOTHING
                         ON CONFLICT ON CONSTRAINT referrals_referred_telegram_id_key DO NOTHING;`,
                        [referrerId, newUser.telegram_id]
                    );
                } catch (referralError) {
                   console.error(`${LOG_PREFIX_GOCU} Failed to record referral for ${referrerId} -> ${newUser.telegram_id}:`, referralError);
                }
            }
            await client.query('COMMIT');
            return newUser;
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_GOCU} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_GOCU} Error in getOrCreateUser for telegramId ${stringTelegramId}: ${error.message} (SQL State: ${error.code})`, error.stack?.substring(0,700));
        return null;
    } finally {
        client.release();
    }
}

async function updateUserActivity(telegramId) {
    const stringTelegramId = String(telegramId);
    try {
        await pool.query('UPDATE users SET last_active_timestamp = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $1', [stringTelegramId]);
    } catch (error) {
        // console.error(`[UpdateUserActivity TG:${stringTelegramId}] Error updating last active timestamp:`, error); // Reduced: log only if it's a persistent problem
    }
}

async function getUserBalance(telegramId) {
    const stringTelegramId = String(telegramId);
    try {
        const result = await pool.query('SELECT balance FROM users WHERE telegram_id = $1', [stringTelegramId]);
        if (result.rows.length > 0) {
            return BigInt(result.rows[0].balance);
        }
        return null;
    } catch (error) {
        console.error(`[GetUserBalance TG:${stringTelegramId}] Error retrieving balance:`, error);
        return null;
    }
}

// This updateUserBalance is a direct DB update without ledger, use with extreme caution (admin corrections only).
// For regular balance changes, use updateUserBalanceAndLedger (from Part P2).
async function updateUserBalance(telegramId, newBalanceLamports, client = pool) {
    const stringTelegramId = String(telegramId);
    const LOG_PREFIX_UUB = `[UpdateUserBal TG:${stringTelegramId}]`;
    try {
        if (typeof newBalanceLamports !== 'bigint') {
            console.error(`${LOG_PREFIX_UUB} Invalid newBalanceLamports type: ${typeof newBalanceLamports}. Must be BigInt.`);
            return false;
        }
        
        if (newBalanceLamports < 0n) {
            console.warn(`${LOG_PREFIX_UUB} 🚨 CAUTION: Attempt to set negative balance (${newBalanceLamports.toString()}). This bypasses ledger and is for admin corrections ONLY.`);
        }

        const result = await client.query(
            'UPDATE users SET balance = $1, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $2',
            [newBalanceLamports.toString(), stringTelegramId]
        );
        if (result.rowCount > 0) {
            console.warn(`${LOG_PREFIX_UUB} ⚠️ Balance directly set to ${newBalanceLamports.toString()} lamports. LEDGER NOT UPDATED. Admin use ONLY.`);
            return true;
        } else {
            return false;
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_UUB} Error updating balance for ${stringTelegramId} to ${newBalanceLamports.toString()}:`, error);
        return false;
    }
}

async function linkUserWallet(telegramId, solanaAddress) {
    const stringTelegramId = String(telegramId);
    const LOG_PREFIX_LUW = `[LinkUserWallet TG:${stringTelegramId}]`;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        try {
            new PublicKey(solanaAddress); 
        } catch (e) {
            await client.query('ROLLBACK');
            // Escaped period in user-facing message
            return { success: false, error: "Invalid Solana address format\\. Please provide a valid Base58 encoded public key\\." };
        }

        const existingLink = await client.query('SELECT telegram_id FROM users WHERE solana_wallet_address = $1 AND telegram_id != $2', [solanaAddress, stringTelegramId]);
        if (existingLink.rows.length > 0) {
            const linkedToExistingUserId = existingLink.rows[0].telegram_id;
            console.warn(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} already linked to user ID ${linkedToExistingUserId}.`);
            await client.query('ROLLBACK');
            // Escaped period in user-facing message
            return { success: false, error: `This wallet address is already associated with another player (ID ending with ${String(linkedToExistingUserId).slice(-4)})\\. Please use a different address\\.` };
        }

        const result = await client.query(
            'UPDATE users SET solana_wallet_address = $1, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = $2 RETURNING solana_wallet_address',
            [solanaAddress, stringTelegramId]
        );

        if (result.rowCount > 0) {
            await client.query('COMMIT');
            if (walletCache) walletCache.set(stringTelegramId, { solanaAddress, timestamp: Date.now() }); 
            // Escaped !, .
            return { success: true, message: `Your Solana wallet \`${escapeMarkdownV2(solanaAddress)}\` has been successfully linked\\!` }; 
        } else {
            const currentUserState = await client.query('SELECT solana_wallet_address FROM users WHERE telegram_id = $1', [stringTelegramId]);
            await client.query('ROLLBACK'); 
            if (currentUserState.rowCount === 0) {
                console.error(`${LOG_PREFIX_LUW} User ${stringTelegramId} not found. Cannot link wallet.`);
                // Escaped period
                return { success: false, error: "Your player profile was not found\\. Please try \`/start\` again\\." };
            }
            if (currentUserState.rows[0].solana_wallet_address === solanaAddress) {
                if (walletCache) walletCache.set(stringTelegramId, { solanaAddress, timestamp: Date.now() });
                // Escaped period
                return { success: true, message: `Your wallet \`${escapeMarkdownV2(solanaAddress)}\` was already linked to your account\\.` };
            }
            console.warn(`${LOG_PREFIX_LUW} User ${stringTelegramId} found, but wallet not updated. DB wallet: ${currentUserState.rows[0].solana_wallet_address}, Attempted: ${solanaAddress}.`);
            // Escaped period
            return { success: false, error: "Failed to update wallet in DB\\. It might be the same, or an unknown issue occurred\\." };
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_LUW} Rollback error: ${rbErr.message}`));
        if (error.code === '23505') { 
            console.warn(`${LOG_PREFIX_LUW} Wallet ${solanaAddress} already linked to another user (unique constraint).`);
            // Escaped period
            return { success: false, error: "This wallet address is already in use by another player\\. Please choose a different one\\." };
        }
        console.error(`${LOG_PREFIX_LUW} Error linking wallet ${solanaAddress}:`, error);
        // Escaped period
        return { success: false, error: escapeMarkdownV2(error.message || "An unexpected server error occurred while linking your wallet\\.") };
    } finally {
        client.release();
    }
}

async function getUserLinkedWallet(telegramId) {
    const stringTelegramId = String(telegramId);
    const cacheTTL = parseInt(process.env.WALLET_CACHE_TTL_MS || (15 * 60 * 1000).toString(), 10);
    
    if (walletCache) { 
        const cachedData = walletCache.get(stringTelegramId);
        if (cachedData && cachedData.solanaAddress && (Date.now() - (cachedData.timestamp || 0) < cacheTTL)) {
            return cachedData.solanaAddress;
        }
    }

    try {
        const result = await pool.query('SELECT solana_wallet_address FROM users WHERE telegram_id = $1', [stringTelegramId]);
        if (result.rows.length > 0 && result.rows[0].solana_wallet_address) {
            if (walletCache) walletCache.set(stringTelegramId, { solanaAddress: result.rows[0].solana_wallet_address, timestamp: Date.now() });
            return result.rows[0].solana_wallet_address;
        }
        return null; 
    } catch (error) {
        console.error(`[GetUserWallet TG:${stringTelegramId}] Error getting linked wallet:`, error);
        return null;
    }
}

// --- Start of new findRecipientUser function ---
/**
 * Finds a user by their Telegram ID or username.
 * @param {string} identifier - The user's Telegram ID or @username.
 * @param {import('pg').PoolClient} [dbClient=pool] - Optional database client.
 * @returns {Promise<object|null>} User object from getOrCreateUser if found, otherwise null.
 */
async function findRecipientUser(identifier, dbClient = pool) {
    const logPrefix = `[FindRecipientUser Ident:${identifier}]`;
    let recipientUser = null;

    if (!identifier || typeof identifier !== 'string') {
        console.warn(`${logPrefix} Invalid identifier provided.`);
        return null;
    }

    try {
        if (identifier.startsWith('@')) {
            const usernameToFind = identifier.substring(1);
            if (!usernameToFind) {
                console.warn(`${logPrefix} Empty username provided after @ symbol.`);
                return null;
            }
            // Ensure queryDatabase is available in this scope
            const userRes = await queryDatabase('SELECT telegram_id, username, first_name, last_name FROM users WHERE LOWER(username) = LOWER($1)', [usernameToFind], dbClient);
            if (userRes.rows.length > 0) {
                // Ensure getOrCreateUser is available in this scope
                recipientUser = await getOrCreateUser(userRes.rows[0].telegram_id, userRes.rows[0].username, userRes.rows[0].first_name, userRes.rows[0].last_name);
            } else {
                console.log(`${logPrefix} User with username "${usernameToFind}" not found.`);
            }
        } else if (/^\d+$/.test(identifier)) {
            // Ensure getOrCreateUser is available in this scope
            // Attempt to fetch the user. If they don't exist, getOrCreateUser will return null if it can't create based on ID alone (which is fine for tipping, recipient should exist).
            recipientUser = await getOrCreateUser(identifier); // Pass only ID, other params are for creation if needed
            if (!recipientUser) {
                 console.log(`${logPrefix} User with ID "${identifier}" not found (getOrCreateUser returned null).`);
            }
        } else {
            console.warn(`${logPrefix} Identifier "${identifier}" is not a valid Telegram ID or @username format.`);
        }
    } catch (error) {
        console.error(`${logPrefix} Error finding recipient user: ${error.message}`, error.stack?.substring(0, 500));
        return null; // Return null on error
    }

    if (!recipientUser) {
         console.log(`${logPrefix} No recipient user found for identifier: ${identifier}`);
    }
    return recipientUser;
}
// --- End of new findRecipientUser function ---

async function getNextAddressIndexForUserDB(userId, dbClient = pool) {
    const stringUserId = String(userId);
    const LOG_PREFIX_GNAI = `[NextAddrIdx TG:${stringUserId}]`;
    try {
        const query = `
            SELECT derivation_path
            FROM user_deposit_wallets
            WHERE user_telegram_id = $1
            ORDER BY created_at DESC; 
        `;
        // queryDatabase is from Part 1, assuming it's available and correct.
        // If queryDatabase is the one you provided earlier, it handles its own errors.
        const res = await queryDatabase(query, [stringUserId], dbClient);
        let maxIndex = -1;

        if (res.rows.length > 0) {
            for (const row of res.rows) {
                const path = row.derivation_path;
                const parts = path.split('/');
                if (parts.length >= 6) { 
                    const lastPart = parts[parts.length - 1];
                    if (lastPart.endsWith("'")) {
                        const indexStr = lastPart.substring(0, lastPart.length - 1);
                        const currentIndex = parseInt(indexStr, 10);
                        if (!isNaN(currentIndex) && currentIndex > maxIndex) {
                            maxIndex = currentIndex;
                        }
                    }
                }
            }
        }
        const nextIndex = maxIndex + 1;
        return nextIndex;
    } catch (error) {
        console.error(`${LOG_PREFIX_GNAI} Error calculating next address index: ${error.message}`, error.stack?.substring(0,300));
        throw error; 
    }
}

async function deleteUserAccount(telegramId) {
    const stringTelegramId = String(telegramId);
    const LOG_PREFIX_DUA = `[DeleteUser TG:${stringTelegramId}]`;
    console.warn(`${LOG_PREFIX_DUA} CRITICAL ACTION: Attempting to delete user account and associated data for Telegram ID: ${stringTelegramId}.`);
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        await client.query('UPDATE jackpots SET last_won_by_telegram_id = NULL WHERE last_won_by_telegram_id = $1', [stringTelegramId]);
        await client.query('UPDATE games SET initiator_telegram_id = NULL WHERE initiator_telegram_id = $1', [stringTelegramId]);
        
        console.log(`${LOG_PREFIX_DUA} Preparing to delete user from 'users' table (CASCADE to related tables).`);
        const result = await client.query('DELETE FROM users WHERE telegram_id = $1', [stringTelegramId]);
        await client.query('COMMIT');

        if (result.rowCount > 0) {
            console.log(`${LOG_PREFIX_DUA} User account ${stringTelegramId} and cascaded data deleted successfully from database.`);
            
            // Clear in-memory caches - ensure GAME_IDS is defined/imported if used here
            const GAME_IDS_INTERNAL = typeof GAME_IDS !== 'undefined' ? GAME_IDS : { DICE_ESCALATOR: 'dice_escalator', DICE_21: 'dice21' }; // Fallback if GAME_IDS not in scope

            if (activeGames && activeGames instanceof Map) {
                activeGames.forEach((game, gameId) => {
                    if (game && game.participants && Array.isArray(game.participants)) {
                        game.participants = game.participants.filter(p => String(p.userId) !== stringTelegramId);
                        // Check GAME_IDS_INTERNAL definition if this part is critical
                        if (game.participants.length === 0 && game.type !== GAME_IDS_INTERNAL.DICE_ESCALATOR && game.type !== GAME_IDS_INTERNAL.DICE_21) {
                            activeGames.delete(gameId);
                        }
                    }
                    if (game && String(game.initiatorId) === stringTelegramId) activeGames.delete(gameId);
                    if (game && String(game.userId) === stringTelegramId) activeGames.delete(gameId); // For single player games
                });
            }
            if (userCooldowns && userCooldowns instanceof Map) userCooldowns.delete(stringTelegramId);
            if (groupGameSessions && groupGameSessions instanceof Map) {
                groupGameSessions.forEach((session, chatId) => {
                    if (session.players && session.players[stringTelegramId]) delete session.players[stringTelegramId];
                    if (session.initiator === stringTelegramId && Object.keys(session.players || {}).length === 0) groupGameSessions.delete(chatId);
                });
            }
            if (walletCache && walletCache instanceof Map) walletCache.delete(stringTelegramId);
            if (activeDepositAddresses && activeDepositAddresses instanceof Map) {
                activeDepositAddresses.forEach((value, key) => {
                    if (String(value.userId) === stringTelegramId) activeDepositAddresses.delete(key);
                });
            }
            if (pendingReferrals && pendingReferrals instanceof Map) { 
                pendingReferrals.forEach((value, key) => {
                    if (String(key) === stringTelegramId) pendingReferrals.delete(key); 
                    if (value && String(value.referrerId) === stringTelegramId) pendingReferrals.delete(key); 
                });
            }
            if (userStateCache && userStateCache instanceof Map) userStateCache.delete(stringTelegramId);
            
            console.log(`${LOG_PREFIX_DUA} Relevant in-memory caches cleared for user ${stringTelegramId}.`);
            return true;
        } else {
            console.log(`${LOG_PREFIX_DUA} User ${stringTelegramId} not found in 'users' table, no account deleted.`);
            return false;
        }
    } catch (error) {
        await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_DUA} Rollback error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_DUA} Error deleting user account ${stringTelegramId}:`, error);
        if(typeof notifyAdmin === 'function' && ADMIN_USER_ID) {
            notifyAdmin(`🚨 User Account Deletion FAILED for ${stringTelegramId} 🚨\nError: ${escapeMarkdownV2(error.message)}`, {parse_mode:'MarkdownV2'});
        }
        return false;
    } finally {
        client.release();
    }
}

// --- End of Part 2 ---
// --- Start of Part 3 ---
// index.js - Part 3: Telegram Helpers, Currency Formatting & Basic Game Utilities
//---------------------------------------------------------------------------
// Assumed escapeMarkdownV2, LAMPORTS_PER_SOL, SOL_DECIMALS, getSolUsdPrice,
// convertLamportsToUSDString, crypto (module), BOT_NAME are available from Part 1.

// --- Telegram Specific Helper Functions ---

/**
 * Gets a display name from a user object and escapes it for MarkdownV2.
 * @param {object} userObject - msg.from or a fetched user object.
 * @returns {string} MarkdownV2 escaped display name.
 */
function getEscapedUserDisplayName(userObject) {
    if (!userObject) return escapeMarkdownV2("Valued Player");

    const firstName = userObject.first_name || userObject.firstName; 
    const username = userObject.username;
    const id = userObject.id || userObject.telegram_id; 

    let name = "Player";
    if (firstName) {
        name = firstName;
    } else if (username) {
        name = `@${username}`; 
    } else if (id) {
        name = `Player ${String(id).slice(-4)}`; 
    } else {
        name = "Valued Player"; 
    }
    return escapeMarkdownV2(name);
}

/**
 * Creates a MarkdownV2 mention link for a user object.
 * @param {object} userObject - msg.from or a fetched user object.
 * @returns {string} MarkdownV2 mention string.
 */
function createUserMention(userObject) {
    if (!userObject) return escapeMarkdownV2("Esteemed Guest");

    const id = userObject.id || userObject.telegram_id;
    if (!id) return escapeMarkdownV2("Unknown Player"); 

    const simpleName = userObject.first_name || userObject.firstName || userObject.username || `Player ${String(id).slice(-4)}`;
    return `[${escapeMarkdownV2(simpleName)}](tg://user?id=${id})`;
}

/**
 * Gets a player's display reference, preferring @username, falls back to name. Escapes for MarkdownV2.
 * @param {object} userObject - msg.from or a fetched user object.
 * @param {boolean} [preferUsernameTag=true] - Whether to prefer @username.
 * @returns {string} MarkdownV2 escaped player reference.
 */
function getPlayerDisplayReference(userObject, preferUsernameTag = true) {
    if (!userObject) return escapeMarkdownV2("Mystery Player"); 

    const username = userObject.username;
    if (preferUsernameTag && username) {
        return `@${escapeMarkdownV2(username)}`;
    }
    return getEscapedUserDisplayName(userObject);
}

// --- General Utility Functions ---

/**
 * Formats a BigInt lamports amount into a SOL string representation or raw lamports.
 * @param {bigint|string|number} amountLamports - The amount in lamports.
 * @param {string} [currencyName='SOL'] - The currency to display ('SOL' or 'lamports').
 * @param {boolean} [displayRawLamportsOverride=false] - If true, forces raw lamports.
 * @param {number} [solDecimals=SOL_DECIMALS] - Decimal places for SOL.
 * @returns {string} Formatted currency string.
 */
function formatCurrency(amountLamports, currencyName = 'SOL', displayRawLamportsOverride = false, solDecimals = SOL_DECIMALS) {
    let lamportsAsBigInt;
    try {
        lamportsAsBigInt = BigInt(amountLamports);
    } catch (e) {
        // console.warn(`[formatCurrency] Invalid amount: '${amountLamports}'. Error: ${e.message}`); // Reduced log
        return '⚠️ Amount Invalid';
    }

    if (displayRawLamportsOverride || String(currencyName).toLowerCase() === 'lamports') {
        return `${lamportsAsBigInt.toLocaleString('en-US')} lamports`;
    }

    if (typeof LAMPORTS_PER_SOL === 'undefined' || Number(LAMPORTS_PER_SOL) <= 0) {
        console.error("[formatCurrency] LAMPORTS_PER_SOL not defined or invalid. Cannot format SOL.");
        return `${lamportsAsBigInt.toLocaleString('en-US')} lamports (⚠️ SOL Config Err)`;
    }

    const solValue = Number(lamportsAsBigInt) / Number(LAMPORTS_PER_SOL);
    let effectiveDecimals = solDecimals;

    if (solValue === Math.floor(solValue)) { 
        effectiveDecimals = 0;
    } else {
        const stringValue = solValue.toString();
        const decimalPart = stringValue.split('.')[1];
        if (decimalPart) {
            effectiveDecimals = Math.min(decimalPart.length, solDecimals);
        } else { 
            effectiveDecimals = 0;
        }
    }
    if (effectiveDecimals > 0 && effectiveDecimals < 2 && solDecimals >= 2) {
        effectiveDecimals = 2;
    }
    if (effectiveDecimals > 0 && solDecimals < 2) {
        effectiveDecimals = solDecimals;
    }

    try {
        return `${solValue.toLocaleString('en-US', {
            minimumFractionDigits: effectiveDecimals, 
            maximumFractionDigits: effectiveDecimals
        })} SOL`;
    } catch (e) {
        console.error(`[formatCurrency] Error formatting SOL for ${lamportsAsBigInt} lamports: ${e.message}`);
        return `${lamportsAsBigInt.toLocaleString('en-US')} lamports (⚠️ Format Err)`;
    }
}

/**
 * Formats a BigInt lamports amount for display, defaulting to USD, with fallbacks.
 * @param {bigint|string|number} lamports - The amount in lamports.
 * @param {string} [targetCurrency='USD'] - Target currency ('USD', 'SOL', or 'lamports').
 * @returns {Promise<string>} Formatted currency string.
 */
async function formatBalanceForDisplay(lamports, targetCurrency = 'USD') {
    let lamportsAsBigInt;
    try {
        lamportsAsBigInt = BigInt(lamports);
    } catch (e) {
        // console.warn(`[formatBalanceForDisplay] Invalid lamport amount: '${lamports}'. Error: ${e.message}`); // Reduced log
        return '⚠️ Amount Invalid';
    }

    const upperTargetCurrency = String(targetCurrency).toUpperCase();

    if (upperTargetCurrency === 'USD') {
        try {
            if (typeof getSolUsdPrice !== 'function' || typeof convertLamportsToUSDString !== 'function') {
                console.error("[formatBalanceForDisplay] Price conversion functions not available. Falling back to SOL.");
                return formatCurrency(lamportsAsBigInt, 'SOL'); 
            }
            const price = await getSolUsdPrice();
            return convertLamportsToUSDString(lamportsAsBigInt, price);
        } catch (e) {
            console.error(`[formatBalanceForDisplay] Failed to get SOL/USD price for USD display: ${e.message}. Falling back to SOL.`);
            return formatCurrency(lamportsAsBigInt, 'SOL'); 
        }
    } else if (upperTargetCurrency === 'LAMPORTS') {
        return formatCurrency(lamportsAsBigInt, 'lamports', true); 
    }
    // Default to SOL
    return formatCurrency(lamportsAsBigInt, 'SOL');
}

/**
 * Generates a unique-ish ID for game instances.
 * @param {string} [prefix="game"] - Prefix for the game ID.
 * @returns {string} A game ID.
 */
function generateGameId(prefix = "game") {
  const timestamp = Date.now().toString(36); 
  const randomSuffix = Math.random().toString(36).substring(2, 10); 
  return `${prefix}_${timestamp}_${randomSuffix}`;
}

// --- Dice Display Utilities ---

/**
 * Formats an array of dice roll numbers into a string with emoji and number.
 * @param {number[]} rollsArray - Array of dice roll numbers.
 * @param {string} [diceEmoji='🎲'] - Emoji to use for dice.
 * @returns {string} Formatted dice rolls string.
 */
function formatDiceRolls(rollsArray, diceEmoji = '🎲') {
  if (!Array.isArray(rollsArray) || rollsArray.length === 0) return '';
  const diceVisuals = rollsArray.map(roll => {
      const rollValue = Number(roll); 
      return `${diceEmoji} ${isNaN(rollValue) ? '?' : rollValue}`;
  });
  return diceVisuals.join(' \u00A0 '); // Non-breaking spaces for better layout
}

/**
 * Generates an internal dice roll.
 * @param {number} [sides=6] - Number of sides on the die.
 * @returns {number} Result of the die roll.
 */
function rollDie(sides = 6) {
  sides = Number.isInteger(sides) && sides > 1 ? sides : 6;
  return Math.floor(Math.random() * sides) + 1;
}

// --- Payment Transaction ID Generation (Optional Utility) ---
/**
 * Generates a unique transaction ID for internal tracking of payments/ledger entries.
 * @param {string} type - Type of payment/ledger entry.
 * @param {string} [userId='system'] - Optional user ID.
 * @returns {string} A unique-ish transaction ID.
 */
function generateInternalPaymentTxId(type, userId = 'system') {
    const now = Date.now().toString(36);
    let randomPart;
    if (typeof crypto !== 'undefined' && typeof crypto.randomBytes === 'function') {
        randomPart = crypto.randomBytes(4).toString('hex'); 
    } else {
        // console.warn('[GenInternalTxId] Crypto module not available for random part. Using Math.random.'); // Reduced log
        randomPart = Math.random().toString(36).substring(2, 10); 
    }
    
    const userPartCleaned = String(userId).replace(/[^a-zA-Z0-9_]/g, '').slice(0, 10) || 'sys'; 
    let prefix = String(type).toLowerCase().substring(0, 6).replace(/[^a-z0-9_]/g, '') || 'gen'; 

    return `${prefix}_${userPartCleaned}_${now}_${randomPart}`;
}

// --- End of Part 3 ---
// --- Start of Part 4 ---
// index.js - Part 4: Simplified Game Logic (Enhanced)
//---------------------------------------------------------------------------
// Assumes rollDie (from Part 3) is available.

// --- Coinflip Logic ---
/**
 * Determines the outcome of a coin flip.
 * @returns {object} Object with outcome ('heads'/'tails'), outcomeString ("Heads"/"Tails"), and emoji.
 */
function determineCoinFlipOutcome() {
  const isHeads = Math.random() < 0.5; 
  return isHeads
    ? { outcome: 'heads', outcomeString: "Heads", emoji: '🪙' } 
    : { outcome: 'tails', outcomeString: "Tails", emoji: '🪙' };
}

// --- Dice Logic (Internal for Bot's Turn or Fallback) ---
/**
 * Determines the outcome for an internal die roll.
 * Uses the `rollDie` function.
 * @param {number} [sides=6] - Number of sides for the die.
 * @returns {object} Object with the roll result and emoji.
 */
function determineDieRollOutcome(sides = 6) {
  if (typeof rollDie !== 'function') {
     console.error("[DetermineDieRollOutcome] CRITICAL Error: rollDie function is not defined. Fallback to 1.");
     return { roll: 1, emoji: '🎲' }; 
  }
  sides = Number.isInteger(sides) && sides > 1 ? sides : 6; 
  const roll = rollDie(sides); 

  return { roll: roll, emoji: '🎲' }; 
}

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

/**
 * Gets a random RPS choice for the bot or an opponent.
 * @returns {object} Object with the choice key and emoji.
 */
function getRandomRPSChoice() {
  const choicesArray = Object.values(RPS_CHOICES);
  const randomChoiceKey = choicesArray[Math.floor(Math.random() * choicesArray.length)];
  return { choice: randomChoiceKey, emoji: RPS_EMOJIS[randomChoiceKey] };
}

/**
 * Determines the outcome of an RPS match given two choices.
 * @param {string} player1ChoiceKey - Player 1's choice (e.g., RPS_CHOICES.ROCK).
 * @param {string} player2ChoiceKey - Player 2's choice.
 * @returns {object} A detailed result object including:
 * - result: 'win_player1', 'win_player2', 'draw', or 'error'.
 * - description: A string pre-formatted for MarkdownV2. **Do NOT escape this again.**
 * - player1: { choice, emoji, choiceFormatted }.
 * - player2: { choice, emoji, choiceFormatted }.
 */
function determineRPSOutcome(player1ChoiceKey, player2ChoiceKey) {
  const LOG_PREFIX_RPS_OUTCOME = "[RPS_Outcome]"; // Shortened
  
  const p1c = String(player1ChoiceKey).toLowerCase();
  const p2c = String(player2ChoiceKey).toLowerCase();

  if (!Object.values(RPS_CHOICES).includes(p1c) || !Object.values(RPS_CHOICES).includes(p2c)) {
    console.warn(`${LOG_PREFIX_RPS_OUTCOME} Invalid choices: P1='${player1ChoiceKey}', P2='${player2ChoiceKey}'.`);
    return {
        result: 'error',
        description: "An internal error occurred due to invalid RPS choices\\. Please try again\\.", // User-friendly generic error
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
    resultDescription = `${p1Emoji} ${p1ChoiceFormatted} *${RPS_RULES[p1c].verb}* ${p2Emoji} ${p2ChoiceFormatted}\\! Player 1 *claims victory*\\!`;
  } else { // Player 2 wins
    outcome = 'win_player2';
    resultDescription = `${p2Emoji} ${p2ChoiceFormatted} *${RPS_RULES[p2c]?.verb || 'outplays'}* ${p1Emoji} ${p1ChoiceFormatted}\\! Player 2 *is the winner*\\!`;
  }

  return {
    result: outcome,
    description: resultDescription, // This string is already MarkdownV2 formatted.
    player1: { choice: p1c, emoji: p1Emoji, choiceFormatted: p1ChoiceFormatted },
    player2: { choice: p2c, emoji: p2Emoji, choiceFormatted: p2ChoiceFormatted }
  };
}

// --- End of Part 4 ---
// --- Start of Part 5a, Section 3 (NEW): Group Game Handlers (Coinflip & RPS) ---
// --- Start of Part 5a, Section 3 (NEW): Group Game Handlers (Coinflip & RPS) ---
// index.js - Part 5a, Section 3: Coinflip & Rock Paper Scissors Game Logic
//----------------------------------------------------------------------------------
// Assumed dependencies from previous Parts:
// Part 1: MIN_BET_USD_val, LAMPORTS_PER_SOL, formatCurrency, getPlayerDisplayReference,
//         escapeMarkdownV2, generateGameId, safeSendMessage, activeGames, groupGameSessions (Map),
//         JOIN_GAME_TIMEOUT_MS, QUICK_DEPOSIT_CALLBACK_ACTION, GAME_IDS (defined in 5a-S1 New), pool, bot,
//         stringifyWithBigInt, notifyAdmin
// Part 2: getOrCreateUser
// Part 3: formatBalanceForDisplay
// Part 4: determineCoinFlipOutcome, RPS_EMOJIS, RPS_CHOICES, determineRPSOutcome
// Part 5a-S4 (NEW): createPostGameKeyboard
// Part P2: updateUserBalanceAndLedger

// --- Group Game Session Management Helpers ---
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
        // console.log(`[GroupSession] New session for chat ID: ${stringChatId} ('${chatTitleIfNew}')`); // Reduced log
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
    // console.log(`[GroupSession] Updated group ${stringChatId}: GameID ${gameId || 'None'}, Type ${gameType || 'None'}, Bet ${session.currentBetAmount || 'N/A'}`); // Reduced log
}
// console.log("[Group Game Utils] getGroupSession and updateGroupGameDetails defined for Part 5a-S3."); // Removed loading log

// --- Coinflip Game Command & Callbacks ---

async function handleStartGroupCoinFlipCommand(chatId, initiatorUserObj, betAmountLamports, commandMessageId, chatType) {
    const LOG_PREFIX_CF_START = `[CF_Start UID:${initiatorUserObj.telegram_id} CH:${chatId}]`; // Shortened
    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_CF_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🪙 Oops! There was an issue with the bet amount for Coinflip\\. Please try again with a valid bet\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_CF_START} Initiating Coinflip. Bet: ${betAmountLamports} lamports.`);

    const initiatorId = String(initiatorUserObj.telegram_id);
    const initiatorMention = getPlayerDisplayReference(initiatorUserObj);
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (chatType === 'private') {
        await safeSendMessage(chatId, `${initiatorMention}, 🪙 **Coinflip** is a thrilling two\\-player game! Please start it in a group chat where a worthy opponent can join your challenge\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    
    let chatInfo = null;
    try {
        if (bot && typeof bot.getChat === 'function') chatInfo = await bot.getChat(chatId);
    } catch (e) { /* console.warn(`${LOG_PREFIX_CF_START} Could not fetch chat info for ${chatId}: ${e.message}`); */ } // Reduced log
    const chatTitleEscaped = chatInfo?.title ? escapeMarkdownV2(chatInfo.title) : `this group`;

    const gameSession = await getGroupSession(chatId, chatInfo?.title || `Group ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        const singlePlayerGames = [GAME_IDS.DICE_ESCALATOR, GAME_IDS.DICE_21, GAME_IDS.OVER_UNDER_7, GAME_IDS.DUEL, GAME_IDS.LADDER, GAME_IDS.SEVEN_OUT, GAME_IDS.SLOT_FRENZY];
        if (!singlePlayerGames.includes(existingGame.type)) {
            const activeGameTypeDisplay = escapeMarkdownV2(existingGame.type.replace(/_/g, " "));
            await safeSendMessage(chatId, `⏳ Hold your horses, ${initiatorMention}! A game of \`${activeGameTypeDisplay}\` is already underway in ${chatTitleEscaped}\\. Please wait for it to conclude before starting a new Coinflip\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${initiatorMention}, your war chest is a bit light for a *${betDisplay}* Coinflip showdown! You need approximately *${neededDisplay}* more\\. Top up?`, {
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
            client, initiatorId, BigInt(-betAmountLamports), 
            'bet_placed_coinflip', { game_id_custom_field: gameId }, 
            `Bet for Coinflip game ${gameId} by initiator ${initiatorMention}`
        );

        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            await safeSendMessage(chatId, `${initiatorMention}, your Coinflip wager of *${betDisplay}* couldn't be placed due to a temporary glitch: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        // console.log(`${LOG_PREFIX_CF_START} Initiator's bet ${betAmountLamports} for Coinflip ${gameId} placed. New bal: ${balanceUpdateResult.newBalanceLamports}`); // Reduced log
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports; 

    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_CF_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_CF_START} Database error during Coinflip bet placement: ${dbError.message}`, dbError.stack?.substring(0,500));
        await safeSendMessage(chatId, "⚙️ A database gremlin interfered while starting the Coinflip game\\. Please try again in a moment\\.", { parse_mode: 'MarkdownV2' });
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
        console.error(`${LOG_PREFIX_CF_START} Failed to send Coinflip setup for ${gameId} or game removed. Refunding initiator.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, initiatorId, betAmountLamports, 'refund_coinflip_setup_fail', {}, `Refund for Coinflip game ${gameId} (setup message failure).`);
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
            console.log(`[CF_Timeout GID:${gameId}] Coinflip expired waiting for opponent.`); // Shortened
            let timeoutRefundClient;
            try {
                timeoutRefundClient = await pool.connect();
                await timeoutRefundClient.query('BEGIN');
                await updateUserBalanceAndLedger(timeoutRefundClient, gdCF_timeout.initiatorId, gdCF_timeout.betAmount, 'refund_coinflip_timeout', {}, `Refund for timed-out Coinflip game ${gameId}.`);
                await timeoutRefundClient.query('COMMIT');
            } catch (err) {
                if (timeoutRefundClient) await timeoutRefundClient.query('ROLLBACK');
                console.error(`[CF_Timeout GID:${gameId}] CRITICAL: Failed to refund initiator for timed-out Coinflip: ${err.message}`);
            } finally {
                if (timeoutRefundClient) timeoutRefundClient.release();
            }
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);

            const timeoutBetDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gdCF_timeout.betAmount, 'USD'));
            const timeoutMsgTextCF = `⏳ *Coinflip Challenge Unanswered* ⏳\nThe Coinflip game initiated by ${gdCF_timeout.initiatorMention} for *${timeoutBetDisplay}* in ${chatTitleEscaped} has expired as no challenger emerged\\. The wager has been refunded\\. Better luck next time!`;
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
    const LOG_PREFIX_RPS_START = `[RPS_Start UID:${initiatorUserObj.telegram_id} CH:${chatId}]`; // Shortened
    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_RPS_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "✂️ Oops! There was an issue with the bet amount for Rock Paper Scissors\\. Please try again with a valid bet\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_RPS_START} Initiating RPS. Bet: ${betAmountLamports} lamports.`);

    const initiatorId = String(initiatorUserObj.telegram_id);
    const initiatorMention = getPlayerDisplayReference(initiatorUserObj);
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (chatType === 'private') {
        await safeSendMessage(chatId, `${initiatorMention}, 🪨📄✂️ **Rock Paper Scissors** is a classic duel for two! Please start it in a group chat where a challenger can accept your gauntlet\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let chatInfo = null;
    try { if (bot) chatInfo = await bot.getChat(chatId); } catch (e) { /* console.warn(`${LOG_PREFIX_RPS_START} Could not fetch chat info for ${chatId}: ${e.message}`); */ } // Reduced log
    const chatTitleEscaped = chatInfo?.title ? escapeMarkdownV2(chatInfo.title) : `this group`;

    const gameSession = await getGroupSession(chatId, chatInfo?.title || `Group ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
         const existingGame = activeGames.get(gameSession.currentGameId);
        const singlePlayerGames = [GAME_IDS.DICE_ESCALATOR, GAME_IDS.DICE_21, GAME_IDS.OVER_UNDER_7, GAME_IDS.DUEL, GAME_IDS.LADDER, GAME_IDS.SEVEN_OUT, GAME_IDS.SLOT_FRENZY];
        if (!singlePlayerGames.includes(existingGame.type)) {
            const activeGameTypeDisplay = escapeMarkdownV2(existingGame.type.replace(/_/g, " "));
            await safeSendMessage(chatId, `⏳ Easy there, ${initiatorMention}! A strategic game of \`${activeGameTypeDisplay}\` is currently in progress in ${chatTitleEscaped}\\. Let it conclude before starting a new RPS battle\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${initiatorMention}, your funds are a little short for an RPS duel of *${betDisplay}*! You need about *${neededDisplay}* more\\. Ready to reload?`, {
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
            await safeSendMessage(chatId, `${initiatorMention}, your RPS wager of *${betDisplay}* hit a snag: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet issue")}\`\\. Please try once more\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        // console.log(`${LOG_PREFIX_RPS_START} Initiator's bet ${betAmountLamports} for RPS ${gameId} placed. New bal: ${balanceUpdateResult.newBalanceLamports}`); // Reduced log
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports;

    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_RPS_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_RPS_START} Database error during RPS bet placement: ${dbError.message}`, dbError.stack?.substring(0,500));
        await safeSendMessage(chatId, "⚙️ Our database gnomes are causing mischief! Failed to start the RPS game\\. Please try again shortly\\.", { parse_mode: 'MarkdownV2' });
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
        console.error(`${LOG_PREFIX_RPS_START} Failed to send RPS setup for ${gameId} or game removed. Refunding initiator.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, initiatorId, betAmountLamports, 'refund_rps_setup_fail', {}, `Refund for RPS game ${gameId} (setup message failure).`);
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
            console.log(`[RPS_Timeout GID:${gameId}] RPS game expired waiting for opponent.`); // Shortened
            let timeoutRefundClient;
            try {
                timeoutRefundClient = await pool.connect();
                await timeoutRefundClient.query('BEGIN');
                await updateUserBalanceAndLedger(timeoutRefundClient, gdRPS_timeout.initiatorId, gdRPS_timeout.betAmount, 'refund_rps_timeout', {}, `Refund for timed-out RPS game ${gameId}.`);
                await timeoutRefundClient.query('COMMIT');
            } catch (err) {
                if (timeoutRefundClient) await timeoutRefundClient.query('ROLLBACK');
                console.error(`[RPS_Timeout GID:${gameId}] CRITICAL: Failed to refund initiator for timed-out RPS: ${err.message}`);
            } finally {
                if (timeoutRefundClient) timeoutRefundClient.release();
            }
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);

            const timeoutBetDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gdRPS_timeout.betAmount, 'USD'));
            const timeoutMsgTextRPS = `⏳ *RPS Duel Unanswered* ⏳\nThe Rock Paper Scissors challenge by ${gdRPS_timeout.initiatorMention} for *${timeoutBetDisplay}* in ${chatTitleEscaped} has expired without an opponent\\. The wager has been bravely refunded\\.`;
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
    const LOG_PREFIX_JOIN = `[JoinGame_CB UID:${joinerUserObj.telegram_id} GID:${gameId}]`; // Shortened
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
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ This game is not currently accepting new challengers.", show_alert: true });
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
        // console.log(`${LOG_PREFIX_JOIN} Joiner's bet ${gameData.betAmount} for ${gameId} placed. New bal: ${balanceUpdateResult.newBalanceLamports}`); // Reduced log
        joinerUserObj.balance = balanceUpdateResult.newBalanceLamports;

    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_JOIN} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_JOIN} Database error during joiner bet placement: ${dbError.message}`, dbError.stack?.substring(0,500));
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚙️ A database hiccup occurred while joining. Please try again.", show_alert: true });
        return;
    } finally {
        if (client) client.release();
    }

    await bot.answerCallbackQuery(callbackQueryId, { text: `✅ You're in! You've joined the ${gameData.type} game for ${betDisplay}!` });

    gameData.participants.push({ userId: joinerId, choice: null, mention: joinerMention, betPlaced: true, userObj: joinerUserObj });
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId); 

    // --- COINFLIP RESOLUTION ---
    if (gameData.type === GAME_IDS.COINFLIP && gameData.participants.length === 2) {
        gameData.status = 'resolving';
        activeGames.set(gameId, gameData); 

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        p1.choice = 'heads'; 
        p2.choice = 'tails';

        const cfResult = determineCoinFlipOutcome(); 
        let winnerParticipant = (cfResult.outcome === p1.choice) ? p1 : p2;
        let loserParticipant = (winnerParticipant === p1) ? p2 : p1;

        const totalPot = gameData.betAmount * 2n; 
        const profitForWinner = gameData.betAmount; 
        let gameOutcomeClient;
        let winnerUpdateSuccess = false;
        let winnerNewBalanceLamports = BigInt(winnerParticipant.userObj.balance); 

        try {
            gameOutcomeClient = await pool.connect();
            await gameOutcomeClient.query('BEGIN');

            const winnerUpdateResult = await updateUserBalanceAndLedger(
                gameOutcomeClient, winnerParticipant.userId, totalPot, 
                'win_coinflip', { game_id_custom_field: gameId },
                `Won Coinflip game ${gameId} vs ${loserParticipant.mention}. Pot: ${totalPot}`
            );
            if (!winnerUpdateResult.success) {
                throw new Error(`Failed to credit Coinflip winner ${winnerParticipant.userId}: ${winnerUpdateResult.error}`);
            }
            winnerNewBalanceLamports = winnerUpdateResult.newBalanceLamports; 
            
            await updateUserBalanceAndLedger(
                gameOutcomeClient, loserParticipant.userId, 0n, 
                'loss_coinflip', { game_id_custom_field: gameId },
                `Lost Coinflip game ${gameId} vs ${winnerParticipant.mention}`
            );
            winnerUpdateSuccess = true;
            await gameOutcomeClient.query('COMMIT');
        } catch (err) {
            if (gameOutcomeClient) await gameOutcomeClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_JOIN} CRITICAL: Error processing Coinflip payout for game ${gameId}. Winner: ${winnerParticipant.userId}. Error: ${err.message}`, err.stack?.substring(0,500));
            winnerUpdateSuccess = false;
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 CRITICAL Coinflip Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nWinner: ${winnerParticipant.mention} (\`${escapeMarkdownV2(winnerParticipant.userId)}\`)\nAmount Due (Pot): \`${escapeMarkdownV2(formatCurrency(totalPot))}\`\nError: DB Update Failed\\. Manual credit/check required\\.`, { parse_mode: 'MarkdownV2' });
            }
        } finally {
            if (gameOutcomeClient) gameOutcomeClient.release();
        }

        let resMsg = `🪙 **Coinflip Resolved! The Coin Spirals\\.\\.\\.** 🪙\nBet Amount: *${betDisplay}*\n\n`;
        resMsg += `${p1.mention} called *Heads*! ${p2.mention} called *Tails*!\n\n`;
        resMsg += `The coin glints, tumbles\\.\\.\\. and lands on **${escapeMarkdownV2(cfResult.outcomeString)}** ${cfResult.emoji}!\n\n`;
        
        const profitDisplay = escapeMarkdownV2(await formatBalanceForDisplay(profitForWinner, 'USD'));
        resMsg += `🎉 Magnificent! Congratulations, ${winnerParticipant.mention}! You've masterfully claimed the pot, securing a *${profitDisplay}* profit! 🎉`;

        if (winnerUpdateSuccess) {
            const winnerNewBalanceDisplay = escapeMarkdownV2(await formatBalanceForDisplay(winnerNewBalanceLamports, 'USD'));
            resMsg += `\n\n${winnerParticipant.mention}'s new balance: *${winnerNewBalanceDisplay}*\\.`;
        } else {
            resMsg += `\n\n⚠️ A mystical force (technical issue) interfered while crediting ${winnerParticipant.mention}'s winnings\\. Our casino wizards have been notified to investigate\\.`;
        }
        
        const postGameKeyboard = createPostGameKeyboard(GAME_IDS.COINFLIP, gameData.betAmount);
        if (messageToEditId && bot) {
            bot.editMessageText(resMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard })
                .catch(async (e) => { 
                    console.warn(`${LOG_PREFIX_JOIN} Failed to edit Coinflip result (ID: ${messageToEditId}), sending new: ${e.message}`);
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
        activeGames.set(gameId, gameData); 

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];

        const rpsPrompt = `🪨📄✂️ **Rock Paper Scissors \\- The Duel is Set!** 🪨📄✂️\n\n${p1.mention} vs ${p2.mention} for a grand prize of *${betDisplay}*!\n\nWarriors, the arena awaits your command! Both players, please *secretly* select your move using the buttons below\\. Your choice will be confirmed privately by me in DM\\. Choose wisely!`;
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
                    console.warn(`${LOG_PREFIX_JOIN} Failed to edit RPS prompt (ID: ${messageToEditId}), sending new: ${e.message}`);
                    const newMsg = await safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); 
                    if (newMsg && newMsg.message_id && activeGames.has(gameId)) {
                        activeGames.get(gameId).gameSetupMessageId = newMsg.message_id; 
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
                let timeoutMessage = `⏳ *RPS Stalemate!* ⏳\nThe duel between ${p1_timeout.mention} and ${p2_timeout.mention} for *${betDisplay}* timed out as not all choices were made\\.`;

                timeoutMessage += "\nAll wagers have been refunded due to timeout\\.";
                let refundP1 = true; let refundP2 = true; 

                let timeoutDbClient;
                try {
                    timeoutDbClient = await pool.connect();
                    await timeoutDbClient.query('BEGIN');
                    if (refundP1) await updateUserBalanceAndLedger(timeoutDbClient, p1_timeout.userId, gdRPS_choiceTimeout.betAmount, 'refund_rps_choice_timeout', {}, `Refund for RPS game ${gameId} - P1 choice timeout`);
                    if (refundP2) await updateUserBalanceAndLedger(timeoutDbClient, p2_timeout.userId, gdRPS_choiceTimeout.betAmount, 'refund_rps_choice_timeout', {}, `Refund for RPS game ${gameId} - P2 choice timeout`); 
                    await timeoutDbClient.query('COMMIT');
                } catch (err) {
                    if (timeoutDbClient) await timeoutDbClient.query('ROLLBACK');
                     console.error(`[RPS_ChoiceTimeout GID:${gameId}] CRITICAL: Failed to refund players for timed-out RPS: ${err.message}`);
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
        }, JOIN_GAME_TIMEOUT_MS * 1.5); 
    }
}

async function handleCancelGameCallback(chatId, cancellerUserObj, gameId, interactionMessageId, callbackQueryId, chatType) {
    const LOG_PREFIX_CANCEL = `[CancelGame_CB UID:${cancellerUserObj.telegram_id} GID:${gameId}]`; // Shortened
    const gameData = activeGames.get(gameId);

    if (!gameData) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This game has already concluded or vanished!", show_alert: true }); // Simplified message
        if (interactionMessageId && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        }
        return;
    }

    if (gameData.initiatorId !== String(cancellerUserObj.telegram_id)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Hold on! Only the game's initiator can cancel.", show_alert: true }); // Simplified
        return;
    }

    if (gameData.status !== 'waiting_opponent' && !(gameData.type === GAME_IDS.RPS && gameData.status === 'waiting_choices' && !gameData.participants.find(p=>p.userId !== gameData.initiatorId)?.choice)) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ The game is too far along to be withdrawn.", show_alert: true }); // Simplified
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: "✅ Game cancellation in progress..." });

    // console.log(`${LOG_PREFIX_CANCEL} Game ${gameId} cancellation requested. Refunding ${gameData.participants.length} participant(s).`); // Reduced log
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        for (const p of gameData.participants) {
            if (p.betPlaced && p.userId && gameData.betAmount > 0n) {
                const refundResult = await updateUserBalanceAndLedger(
                    client, p.userId, gameData.betAmount, 
                    `refund_${gameData.type}_cancelled`, { game_id_custom_field: gameId },
                    `Refund for cancelled ${gameData.type} game ${gameId}`
                );
                if (!refundResult.success) {
                    console.error(`${LOG_PREFIX_CANCEL} CRITICAL: Failed to refund UserID: ${p.userId} for cancelled game ${gameId}. Error: ${refundResult.error}`);
                    if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL FAILED REFUND (Cancellation) 🚨\nGame: ${gameData.type} ID: ${gameId}\nUser: ${p.mention} (${p.userId})\nAmount: ${formatCurrency(gameData.betAmount)}\nReason: Cancellation refund failed DB update\\. MANUAL REFUND REQUIRED\\.`, {parse_mode:'MarkdownV2'});
                }
            }
        }
        await client.query('COMMIT');
    } catch (dbError) {
        if (client) await client.query('ROLLBACK');
        console.error(`${LOG_PREFIX_CANCEL} Database error during cancellation refunds for ${gameId}: ${dbError.message}`, dbError.stack?.substring(0,500));
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL: Systemic DB error during cancellation refunds for Game ID: ${gameId}\\. Error: ${dbError.message}\\. Some refunds may have failed\\.`, {parse_mode:'MarkdownV2'});
    } finally {
        if (client) client.release();
    }

    const gameTypeDisplay = escapeMarkdownV2(gameData.type.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    const cancellationMessage = `🚫 **Challenge Retracted by Initiator!** 🚫\n\nThe ${gameTypeDisplay} game for *${betDisplay}*, started by ${gameData.initiatorMention}, has been cancelled\\. All wagers have been gallantly returned to the participants' treasuries\\.`;

    const msgToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (msgToEdit && bot) {
        bot.editMessageText(cancellationMessage, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: {} })
            .catch(async (e) => { 
                // console.warn(`${LOG_PREFIX_CANCEL} Failed to edit cancel message (ID: ${msgToEdit}), sending new: ${e.message}`); // Reduced log
                await safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' }); 
            });
    } else {
        await safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
    }

    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
    // console.log(`${LOG_PREFIX_CANCEL} Game ${gameId} cancelled and removed.`); // Reduced log
}

async function handleRPSChoiceCallback(chatId, userChoiceObj, gameId, choiceKey, interactionMessageId, callbackQueryId, chatType) {
    const LOG_PREFIX_RPS_CHOICE = `[RPS_Choice_CB UID:${userChoiceObj.telegram_id} GID:${gameId}]`; // Shortened
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.RPS || gameData.status !== 'waiting_choices') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This RPS game isn't active or it's not time to choose!", show_alert: true }); // Simplified
        return;
    }

    const participant = gameData.participants.find(p => p.userId === String(userChoiceObj.telegram_id));
    if (!participant) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "🤔 You don't seem to be a duelist in this battle.", show_alert: true }); // Simplified
        return;
    }
    if (participant.choice) {
        const existingChoiceEmoji = RPS_EMOJIS[participant.choice] || '❓';
        await bot.answerCallbackQuery(callbackQueryId, { text: `🛡️ You've already locked in ${existingChoiceEmoji}! Waiting for opponent.`, show_alert: false }); // Simplified
        return;
    }

    participant.choice = choiceKey.toLowerCase();
    const choiceEmoji = RPS_EMOJIS[participant.choice] || '❓';
    const choiceFormatted = participant.choice.charAt(0).toUpperCase() + participant.choice.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, { text: `🎯 Your choice: ${choiceEmoji} ${choiceFormatted} is set!`, show_alert: false }); // Simplified

    const p1 = gameData.participants[0];
    const p2 = gameData.participants[1];
    const allChosen = p1 && p1.choice && p2 && p2.choice;
    const msgToEditId = Number(gameData.gameSetupMessageId || interactionMessageId); 

    // --- RPS RESOLUTION ---
    if (allChosen) {
        gameData.status = 'resolving'; 
        activeGames.set(gameId, gameData); 

        const rpsOutcome = determineRPSOutcome(p1.choice, p2.choice); 
        const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
        const p1ChoiceFormatted = p1.choice.charAt(0).toUpperCase() + p1.choice.slice(1);
        const p2ChoiceFormatted = p2.choice.charAt(0).toUpperCase() + p2.choice.slice(1);


        let resultText = `🪨📄✂️ **Rock Paper Scissors \\- The Dust Settles!** 🪨📄✂️\nBet Amount: *${betDisplay}*\n\n`;
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
                const winnings = gameData.betAmount * 2n; 
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
            } else { 
                console.error(`${LOG_PREFIX_RPS_CHOICE} RPS outcome determination error: ${rpsOutcome.description}`);
                resultText += `⚙️ An unexpected internal error occurred determining the winner\\. Bets may be refunded if an issue is confirmed\\.`;
                outcomeProcessedSuccessfully = false; 
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
            console.error(`${LOG_PREFIX_RPS_CHOICE} CRITICAL: DB error during RPS game ${gameId} outcome: ${dbError.message}`, dbError.stack?.substring(0,500));
            resultText += `\n\n⚠️ A critical database error occurred finalizing this game\\. Our casino staff has been notified\\. Your balance may reflect the pre\\-game state\\.`;
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 CRITICAL RPS Outcome DB Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nError: ${dbError.message}\\. Balances might be incorrect\\. MANUAL CHECK REQUIRED\\.`,{parse_mode:'MarkdownV2'});
            }
        } finally {
            if (clientGameOutcome) clientGameOutcome.release();
        }

        resultText += finalBalancesText;
        const postGameKeyboard = createPostGameKeyboard(GAME_IDS.RPS, gameData.betAmount);

        if (msgToEditId && bot) {
            bot.editMessageText(resultText, { chatId: String(chatId), message_id: msgToEditId, parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard })
                .catch(async (e) => { 
                    console.warn(`${LOG_PREFIX_RPS_CHOICE} Failed to edit RPS result (ID: ${msgToEditId}), sending new: ${e.message}`);
                    await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard }); 
                });
        } else {
            await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
        }
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);

    // --- RPS - WAITING FOR OTHER PLAYER ---
    } else { 
        const p1Status = p1.choice ? `✅ ${p1.mention} has chosen their destiny!` : `⏳ ${p1.mention} is pondering their next move\\.\\.\\.`;
        const p2Status = p2?.choice ? `✅ ${p2.mention} has made their strategic selection!` : `⏳ ${p2?.mention || 'The Challenger'} is calculating their options\\.\\.\\.`;
        
        const waitingText = `🪨📄✂️ **RPS Battle \\- Moves Pending!** 🪨📄✂️\nBet: *${betDisplay}*\n\n${p1Status}\n${p2Status}\n\nThe air crackles with anticipation! Waiting for all warriors to commit to their action\\. Use the buttons below if you haven't chosen\\.`;
        if (msgToEditId && bot) {
            try {
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
                    // console.warn(`${LOG_PREFIX_RPS_CHOICE} Failed to edit RPS waiting message (ID: ${msgToEditId}): ${e.message}`); // Reduced log
                }
            }
        }
    }
}

// console.log("Part 5a, Section 3 (NEW): Group Game Handlers (Coinflip & RPS) - Complete."); // Removed loading log
// --- End of Part 5a, Section 3 (NEW) ---
// --- Start of Part 5b, Section 1 (COMPLETE NEW DICE ESCALATOR LOGIC - Refined Jackpot Run UI) ---
// index.js - Part 5b, Section 1: Dice Escalator Game Logic & Handlers (New Unified Offer, PvB, PvP Structure)
// (This entire block is placed after Original Part 5a, Section 3 in the new order)
//----------------------------------------------------------------------------------------------
// console.log("Loading Part 5b, Section 1 (Refined Jackpot Run UI)...");

// Assumed dependencies from previous Parts:
// Part 1: bot, safeSendMessage, escapeMarkdownV2, getPlayerDisplayReference, formatCurrency, LAMPORTS_PER_SOL,
//         GAME_IDS (with DICE_ESCALATOR_UNIFIED_OFFER, DICE_ESCALATOR_PVB, DICE_ESCALATOR_PVP),
//         TARGET_JACKPOT_SCORE, DICE_ESCALATOR_BUST_ON, JACKPOT_CONTRIBUTION_PERCENT, MAIN_JACKPOT_ID,
//         activeGames, groupGameSessions, JOIN_GAME_TIMEOUT_MS, queryDatabase, pool, stringifyWithBigInt,
//         DICE_ROLL_POLLING_MAX_ATTEMPTS, DICE_ROLL_POLLING_INTERVAL_MS, sleep, // Ensure sleep is available
//         crypto (for randomBytes)
// Part 2: getOrCreateUser, getUserBalance, updateUserBalanceAndLedger
// Part 5a-S4: createPostGameKeyboard
// Payment System: getSolUsdPrice, convertLamportsToUSDString, convertUSDToLamports (for jackpot display)

// --- Constants specific to New Dice Escalator ---
const DE_PVB_BOT_ROLL_COUNT = 3; // Bot rolls 3 dice in PvB
const BUST_MESSAGE_DELAY_MS = 2000; // Delay for bust message in milliseconds

// --- Helper Function for DE Game Message Formatting ---
async function formatDiceEscalatorGameMessage_New(gameData) {
    let messageText = "";
    let jackpotDisplay = "";
    const LOG_PREFIX_FORMAT_DE = `[FormatDE_Msg GID:${gameData.gameId}]`;

    if (gameData.type === GAME_IDS.DICE_ESCALATOR_PVB) {
        try {
            const jackpotResult = await queryDatabase('SELECT current_amount FROM jackpots WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]);
            if (jackpotResult.rows.length > 0 && jackpotResult.rows[0].current_amount) {
                const jackpotAmountLamports = BigInt(jackpotResult.rows[0].current_amount);
                if (jackpotAmountLamports > 0n) {
                    jackpotDisplay = `💎 *Super Jackpot Alert\\!* 💎\nCurrently: *${escapeMarkdownV2(await formatBalanceForDisplay(jackpotAmountLamports, 'USD'))}* / *${escapeMarkdownV2(formatCurrency(jackpotAmountLamports, 'SOL'))}* 💰\n\n`;
                }
            }
        } catch (error) {
            console.error(`${LOG_PREFIX_FORMAT_DE} Error fetching jackpot amount for display: ${error.message}`);
            jackpotDisplay = "⚠️ *Jackpot status currently unavailable\\.* Please check again soon\\.\n\n";
        }
    }

    const betDisplaySOL = escapeMarkdownV2(formatCurrency(gameData.betAmount, 'SOL'));
    const betUsdDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    if (gameData.type === GAME_IDS.DICE_ESCALATOR_PVB) {
        const player = gameData.player;
        messageText = `🎲 *Dice Escalator vs\\. Bot Dealer* 🎲\n\n` +
                      `${jackpotDisplay}` +
                      `👤 Player: ${player.displayName}\n` +
                      `💸 Wager: *${betDisplaySOL}* \\(${betUsdDisplay}\\)\n\n` +
                      `🎯 Your Current Score: *${player.score}*\n`;
        if (player.rolls && player.rolls.length > 0) {
            messageText += `🎲 Your Rolls: ${player.rolls.map(r => `\`${r}\``).join(' ⋅ ')}\n`;
        }

        if (gameData.status === 'player_score_18_plus_awaiting_choice') {
            messageText += `\n\n⚠️ *High Score Alert \\- Decision Time\\!* Your score is *${player.score}*\\. ⚠️\n` +
                           `You're in striking distance\\! You can:\n` +
                           `	1️⃣ *Stand Firm:* Lock in your current score and see what the Bot rolls\\.\n` +
                           `	2️⃣ *Go for the Jackpot\\!:* Keep rolling towards the *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}* point Super Jackpot\\! If you choose this, you *cannot stand later* \\- it's jackpot or bust\\! Each roll will be a quick update without the main board refreshing until the end\\.\n\n` +
                           `What's your strategy, ${player.displayName}?`;
        } else if (gameData.status === 'player_turn_awaiting_emoji') {
            if (player.isGoingForJackpot) { // This message is now mostly for the keyboard logic in updateDiceEscalatorPvBMessage_New
                messageText += `\n\n🔥 *Jackpot Run Active\\!* Score: *${player.score}* (Target: *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}*)\n` +
                               `No standing now\\! Send 🎲 to roll again\\. Aim high or go bust trying\\!`;
            } else {
                 messageText += `\n👉 ${player.displayName}, it's *your turn*\\! Send a 🎲 emoji to roll\\. Keep rolling to boost your score, or select "Stand Firm" to lock it in\\!`;
            }
        } else if (gameData.status === 'player_stood') {
            messageText += `\n✅ You've stood with a magnificent score of *${player.score}*\\!\n🤖 The Bot Dealer is now making its move, rolling ${DE_PVB_BOT_ROLL_COUNT} dice\\.\\.\\. Hold tight\\!`;
        } else if (gameData.status === 'bot_turn_complete') {
            messageText += `\n🤖 Bot Dealer's Hand: ${(gameData.botRolls || []).map(r => `\`${r}\``).join(' ⋅ ')}\n` +
                           `🤖 Bot Dealer's Final Score: *${gameData.botScore || 0}*\n\n` +
                           `⏳ Calculating the thrilling final results\\! One moment\\.\\.\\.`;
        } else if (gameData.status === 'player_busted') {
            messageText += `\n💥 *Oh no, a Bust\\!* Your roll of \`${gameData.lastPlayerRoll}\` \\(bust on \`${DICE_ESCALATOR_BUST_ON}\`\\) ends your turn\\. The Bot Dealer wins by default this round\\.`;
        }
    } else if (gameData.type === GAME_IDS.DICE_ESCALATOR_PVP) {
        // PvP formatting remains the same as before
        const p1 = gameData.initiator;
        const p2 = gameData.opponent;
        const totalPotSolDisplay = escapeMarkdownV2(formatCurrency(gameData.betAmount * 2n, 'SOL'));
        messageText = `⚔️ *Dice Escalator PvP: ${p1.displayName} vs\\. ${p2.displayName}* ⚔️\n\n` +
                      `💸 Wager Each: *${betDisplaySOL}* \\(${betUsdDisplay}\\)\n`+
                      `💰 Total Pot: *${totalPotSolDisplay}*\n\n` +
                      `--- *Current Standings* ---\n`;
        messageText += `👤 **${p1.displayName} (P1):** Score *${p1.score}* ${p1.rolls.length > 0 ? `🎲 \\(Rolls: ${p1.rolls.map(r => `\`${r}\``).join(' ⋅ ')}\\)` : ''}${p1.busted ? " 💥 \\(Busted\\!\\)" : (p1.stood ? " ✅ \\(Stood\\)" : "")}\n`;
        messageText += `👤 **${p2.displayName} (P2):** Score *${p2.score}* ${p2.rolls.length > 0 ? `🎲 \\(Rolls: ${p2.rolls.map(r => `\`${r}\``).join(' ⋅ ')}\\)` : ''}${p2.busted ? " 💥 \\(Busted\\!\\)" : (p2.stood ? " ✅ \\(Stood\\)" : "")}\n\n`;
        let actionPrompt = "";
        if (gameData.initiator.isTurn && (gameData.status === 'p1_awaiting_roll1_emoji' || gameData.status === 'p1_awaiting_roll2_emoji')) {
            actionPrompt = `👉 ${p1.displayName} (P1), it's *your moment*\\! Send a 🎲 to roll again, or "Stand" to lock in your score\\.`;
        } else if (gameData.opponent.isTurn && (gameData.status === 'p2_awaiting_roll1_emoji' || gameData.status === 'p2_awaiting_roll2_emoji')) {
            actionPrompt = `👉 ${p2.displayName} (P2), the gauntlet is thrown\\! ${p1.displayName} (P1) has *${p1.score}* points\\. Send a 🎲 to roll again, or "Stand" to make your play\\.`;
        } else if (gameData.status === 'p1_stood') {
            actionPrompt = `✅ ${p1.displayName} (P1) stands tall with *${p1.score}*\\!\n${p2.displayName} (P2), it's your turn to escalate\\! Can you beat that score? Send 🎲 to roll\\!`;
        } else if (gameData.status.startsWith('game_over') || gameData.status === 'p1_busted' || gameData.status === 'p2_busted' || gameData.status === 'p2_wins_by_crossing_score') {
            actionPrompt = "🏁 *The Dice Have Settled\\!* Calculating the final scores\\.\\.\\. ⏳";
        } else {
            actionPrompt = "⏳ Awaiting player action or the game's thrilling resolution\\.\\.\\.";
        }
        messageText += actionPrompt;
    }
    return messageText.trim();
}

async function getThreeDiceRollsViaHelper_DE_New(gameIdForLog, chatIdForLogContext) {
    const LOG_PREFIX_HELPER = `[DE_HelperBotRolls Game:${gameIdForLog}]`;
    const rolls = [];
    let overallHelperError = null;
    for (let i = 0; i < DE_PVB_BOT_ROLL_COUNT; i++) {
        if (isShuttingDown) { overallHelperError = "Shutdown during DE PvB bot roll requests."; break; }
        let client = null;
        let requestId = null;
        let currentSingleRollError = null;
        try {
            client = await pool.connect();
            const insertResult = await client.query(
                'INSERT INTO dice_roll_requests (game_id, chat_id, user_id, status, emoji_type, notes) VALUES ($1, $2, $3, $4, $5, $6) RETURNING request_id',
                [gameIdForLog, String(chatIdForLogContext), null, 'pending', '🎲', `DE PvB Bot Roll ${i+1}`]
            );
            if (!insertResult.rows[0] || !insertResult.rows[0].request_id) {
                throw new Error("Failed to insert roll request or retrieve request_id for bot.");
            }
            requestId = insertResult.rows[0].request_id;
            client.release(); client = null;
            let attempts = 0;
            let rollValue = null;
            let rollStatus = 'pending';
            let rollNotes = null;
            while (attempts < DICE_ROLL_POLLING_MAX_ATTEMPTS && rollStatus === 'pending') {
                await sleep(DICE_ROLL_POLLING_INTERVAL_MS);
                if (isShuttingDown) { currentSingleRollError = "Shutdown during DE PvB bot roll poll."; break; }
                client = await pool.connect();
                const res = await client.query('SELECT roll_value, status, notes FROM dice_roll_requests WHERE request_id = $1', [requestId]);
                client.release(); client = null;
                if (res.rows.length > 0) {
                    rollValue = res.rows[0].roll_value;
                    rollStatus = res.rows[0].status;
                    rollNotes = res.rows[0].notes;
                    if (rollStatus === 'completed' && rollValue !== null && rollValue >= 1 && rollValue <= 6) {
                        rolls.push(rollValue);
                        break;
                    } else if (rollStatus === 'error') {
                        currentSingleRollError = rollNotes || `Helper Bot reported error for DE PvB Bot roll ${i+1} (Req ID: ${requestId}).`;
                        break;
                    }
                }
                attempts++;
            }
            if (currentSingleRollError) { throw new Error(currentSingleRollError); }
            if (rollStatus !== 'completed' || rollValue === null) {
                currentSingleRollError = `Timeout polling for DE PvB Bot roll ${i+1} (Req ID: ${requestId}). Max attempts reached.`;
                client = await pool.connect();
                await client.query("UPDATE dice_roll_requests SET status='timeout', notes=$1 WHERE request_id=$2 AND status='pending'", [String(currentSingleRollError).substring(0,250), requestId]).catch(()=>{});
                client.release(); client = null;
                throw new Error(currentSingleRollError);
            }
        } catch (dbOrPollError) {
            if (client) client.release();
            console.error(`${LOG_PREFIX_HELPER} Error during bot roll ${i+1} (Req ID: ${requestId || 'N/A'}): ${dbOrPollError.message}. Using fallback.`);
            rolls.push(Math.floor(Math.random() * 6) + 1);
            if (!overallHelperError) overallHelperError = dbOrPollError.message;
        }
    }
    if (overallHelperError) { console.warn(`${LOG_PREFIX_HELPER} One or more bot rolls encountered errors. Final rolls (may include fallbacks): ${rolls.join(', ')}. First error: ${overallHelperError}`); }
    return rolls;
}

async function handleStartDiceEscalatorUnifiedOfferCommand_New(msg, betAmountLamports) {
    // ***** CORRECTED LINE FOR USER ID EXTRACTION *****
    const userId = String(msg.from.id || msg.from.telegram_id); 
    // ***** END OF CORRECTION *****
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_DE_OFFER = `[DE_Offer UID:${userId} CH:${chatId}]`; // Use the extracted userId

    // Ensure initiatorUserObj is created *after* userId is correctly extracted
    let initiatorUserObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!initiatorUserObj) {
        console.warn(`${LOG_PREFIX_DE_OFFER} Failed to get/create user object for ID: ${userId}. Cannot start game offer.`);
        await safeSendMessage(chatId, "Apologies, your player profile couldn't be accessed right now to create a Dice Escalator offer. Please use the `/start` command with me first, and then try again.", { parse_mode: 'MarkdownV2' });
        return;
    }

    if (chatType === 'private') {
        await safeSendMessage(chatId, `📣 *Group Action Required, ${getPlayerDisplayReference(initiatorUserObj)}\\!*\n\nDice Escalator is a social game designed for groups\\! Please use the \`/de <bet>\` command in a group chat to challenge others or the Bot Dealer\\. 🎲`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const balance = await getUserBalance(initiatorUserObj.telegram_id); // Use telegram_id from the fetched userObj
    if (balance === null || balance < betAmountLamports) {
        const balanceDisplay = balance === null ? "⚠️ Error fetching balance" : escapeMarkdownV2(formatCurrency(balance, 'SOL'));
        const neededDisplay = escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL'));
        await safeSendMessage(chatId, `💰 *Funds Alert for ${getPlayerDisplayReference(initiatorUserObj)}\\!*\n\nTo make a Dice Escalator offer of *${neededDisplay}*, your treasury needs a little boost\\!\nYour Current Balance: ${balanceDisplay}\n\nPlease top up and try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const offerId = `deo_${Date.now().toString(36)}_${crypto.randomBytes(3).toString('hex')}`;
    const offerData = {
        gameId: offerId, type: GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER,
        initiator: { userId: initiatorUserObj.telegram_id, username: initiatorUserObj.username, firstName: initiatorUserObj.first_name, displayName: getPlayerDisplayReference(initiatorUserObj) },
        betAmount: betAmountLamports, chatId: chatId, chatType: chatType, status: 'pending_offer', createdAt: Date.now(), offerMessageId: null
    };
    const betDisplaySol = escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL'));
    const betUsdDisplay = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));
    const offerText = `✨🎲 *A New Dice Escalator Challenge Has Been Issued\\!* 🎲✨\n\n` +
                      `Challenger: ${offerData.initiator.displayName}\n` +
                      `💸 Wager: *${betDisplaySol}* \\(${betUsdDisplay}\\) per player\n\n` +
                      `*Who will dare to escalate the stakes?*\n` +
                      `	▫️ The Initiator can choose to battle the *🤖 Bot Dealer*\\!\n` +
                      `	▫️ Or another brave player can accept for an epic *⚔️ PvP Duel*\\!\n\n` +
                      `⏳ This electrifying challenge vanishes in *${JOIN_GAME_TIMEOUT_MS / 1000 / 60} minutes*\\! Choose your fate\\.`;
    const keyboard = {
        inline_keyboard: [
            [{ text: "🤖 Play vs Bot Dealer", callback_data: `de_accept_bot_game:${offerId}` }, { text: "⚔️ Accept PvP Challenge", callback_data: `de_accept_pvp_challenge:${offerId}` }],
            [{ text: "❌ Cancel Offer", callback_data: `de_cancel_unified_offer:${offerId}` }]
        ]
    };
    const sentMessage = await safeSendMessage(chatId, offerText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
    if (sentMessage && sentMessage.message_id) {
        offerData.offerMessageId = String(sentMessage.message_id);
        activeGames.set(offerId, offerData);
        console.log(`${LOG_PREFIX_DE_OFFER} Dice Escalator unified offer ${offerId} created by ${initiatorUserObj.telegram_id}. Message ID: ${sentMessage.message_id}`);
        setTimeout(async () => {
            const currentOffer = activeGames.get(offerId);
            if (currentOffer && currentOffer.status === 'pending_offer') {
                console.log(`${LOG_PREFIX_DE_OFFER} Dice Escalator offer ${offerId} expired.`);
                activeGames.delete(offerId);
                if (currentOffer.offerMessageId && bot) {
                    await bot.editMessageText(`⏳ *Offer Expired* ⏳\nThe Dice Escalator challenge from ${currentOffer.initiator.displayName} for *${betDisplaySol}* has timed out\\. No heroes stepped forward this round\\!`, {
                        chat_id: currentOffer.chatId, message_id: Number(currentOffer.offerMessageId), parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [] }
                    }).catch(e => console.warn(`${LOG_PREFIX_DE_OFFER} Failed to edit expired DE offer message: ${e.message}`));
                }
            }
        }, JOIN_GAME_TIMEOUT_MS);
    } else {
        console.error(`${LOG_PREFIX_DE_OFFER} Failed to send Dice Escalator offer message.`);
        await safeSendMessage(chatId, "⚙️ *Oops\\!* We encountered a hiccup creating your Dice Escalator offer\\. Please try the command again in a moment\\. 🙏", { parse_mode: 'MarkdownV2' });
    }
}

async function handleDiceEscalatorAcceptBotGame_New(offerId, userWhoClicked, originalMessageId, originalChatId, originalChatType, callbackQueryIdPassed = null) {
    const LOG_PREFIX_DE_ACCEPT_BOT = `[DE_AcceptBot UID:${userWhoClicked.telegram_id} Offer:${offerId}]`;
    const offerData = activeGames.get(offerId);
    const callbackQueryId = callbackQueryIdPassed;
    if (!offerData || offerData.type !== GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER || offerData.status !== 'pending_offer') {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Dice Escalator offer is no longer valid or has expired.", show_alert: true }).catch(()=>{});
        else if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, {chat_id: originalChatId, message_id: Number(originalMessageId)}).catch(()=>{});
        return;
    }
    if (offerData.initiator.userId !== userWhoClicked.telegram_id) {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Hold your horses! Only the offer initiator can start a game vs. the Bot.", show_alert: true }).catch(()=>{});
        return;
    }
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "Summoning the Bot Dealer... Game starting!"}).catch(()=>{});
    console.log(`${LOG_PREFIX_DE_ACCEPT_BOT} Initiator chose to play vs Bot for offer ${offerId}.`);
    // Pass the full user object from the offer data, which should be correctly populated
    const initiatorObjFull = offerData.initiatorUserObj || await getOrCreateUser(offerData.initiator.userId, offerData.initiator.username, offerData.initiator.firstName);
    if (!initiatorObjFull) {
        console.error(`${LOG_PREFIX_DE_ACCEPT_BOT} Could not get full user object for initiator ${offerData.initiator.userId}`);
        await safeSendMessage(originalChatId, "😕 *Profile Problem\\!* We couldn't fetch your profile to start the Dice Escalator game\\. Please try again or contact support if this persists\\.", {parse_mode: 'MarkdownV2'});
        activeGames.delete(offerId);
        if (offerData.offerMessageId && bot) bot.editMessageText("⚙️ Offer Cancelled: An internal snag prevented this game from starting\\. The offer has been removed\\.", { chat_id: originalChatId, message_id: Number(offerData.offerMessageId), reply_markup:{}}).catch(()=>{});
        return;
    }
    activeGames.delete(offerId);
    await startDiceEscalatorPvBGame_New({ id: originalChatId, type: originalChatType }, initiatorObjFull, offerData.betAmount, offerData.offerMessageId, false);
}

async function handleDiceEscalatorAcceptPvPChallenge_New(offerId, userWhoClicked, originalMessageId, originalChatId, originalChatType, callbackQueryIdPassed = null) {
    const LOG_PREFIX_DE_ACCEPT_PVP = `[DE_AcceptPvP UID:${userWhoClicked.telegram_id} Offer:${offerId}]`;
    const offerData = activeGames.get(offerId);
    const callbackQueryId = callbackQueryIdPassed;
    if (!offerData || offerData.type !== GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER || offerData.status !== 'pending_offer') {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Dice Escalator offer has already been taken or expired.", show_alert: true }).catch(()=>{});
        else if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, {chat_id: originalChatId, message_id: Number(originalMessageId)}).catch(()=>{});
        return;
    }
    if (offerData.initiator.userId === userWhoClicked.telegram_id) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "You can't battle yourself... or can you? Not in this game!", show_alert: true }).catch(()=>{});
        return;
    }
    const opponentUserObjFull = await getOrCreateUser(userWhoClicked.telegram_id, userWhoClicked.username, userWhoClicked.first_name);
    if(!opponentUserObjFull){
        console.error(`${LOG_PREFIX_DE_ACCEPT_PVP} Could not get/create opponent user object for ${userWhoClicked.telegram_id}`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "Error fetching your profile to join. Please try again.", show_alert:true}).catch(()=>{});
        return;
    }
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "You've accepted the challenge! Preparing for PvP..."}).catch(()=>{});
    const opponentBalance = opponentUserObjFull.balance;
    if (opponentBalance < offerData.betAmount) {
        const neededDisplay = escapeMarkdownV2(formatCurrency(offerData.betAmount, 'SOL'));
        const currentBalanceDisplay = escapeMarkdownV2(formatCurrency(opponentBalance, 'SOL'));
        await safeSendMessage(originalChatId, `💰 *Funds Check for ${getPlayerDisplayReference(opponentUserObjFull)}*\\!\n\nTo accept this Dice Escalator PvP challenge for *${neededDisplay}*, your balance needs a little more fuel\\!\nYour Balance: ${currentBalanceDisplay}`, { parse_mode: 'MarkdownV2' });
        return;
    }
    // Use the initiatorUserObj from the offerData if available and valid
    const initiatorUserObjFull = offerData.initiatorUserObj || await getOrCreateUser(offerData.initiator.userId, offerData.initiator.username, offerData.initiator.firstName);
    if(!initiatorUserObjFull){
        console.error(`${LOG_PREFIX_DE_ACCEPT_PVP} Could not get full user object for initiator ${offerData.initiator.userId}`);
        await safeSendMessage(originalChatId, "⚙️ *Initiator Profile Issue* ⚙️\nWe couldn't fetch the initiator's details\\. The Dice Escalator PvP game cannot start right now\\. The offer has been cancelled\\.", {parse_mode: 'MarkdownV2'});
        activeGames.delete(offerId);
        if (offerData.offerMessageId && bot) bot.editMessageText("⚙️ Offer Cancelled: An issue with the initiator's profile prevented the game from starting\\.", { chat_id: originalChatId, message_id: Number(offerData.offerMessageId), reply_markup:{}}).catch(()=>{});
        return;
    }
    const initiatorBalance = initiatorUserObjFull.balance;
    if (initiatorBalance < offerData.betAmount) {
        console.warn(`${LOG_PREFIX_DE_ACCEPT_PVP} Initiator ${offerData.initiator.displayName} no longer has sufficient funds for this wager. Offer ${offerId} is being cancelled.`);
        if (offerData.offerMessageId && bot) {
            await bot.editMessageText(`⚠️ *Offer Auto\\-Cancelled* ⚠️\nThe Dice Escalator offer by ${offerData.initiator.displayName} was automatically cancelled because their balance is no longer sufficient for the wager\\. Our apologies for any inconvenience\\!`, {
                chat_id: originalChatId, message_id: Number(offerData.offerMessageId), parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard:[]}
            }).catch(()=>{});
        }
        activeGames.delete(offerId);
        return;
    }
    console.log(`${LOG_PREFIX_DE_ACCEPT_PVP} User ${opponentUserObjFull.telegram_id} has accepted the PvP challenge for Dice Escalator offer ${offerId}.`);
    activeGames.delete(offerId);
    await startDiceEscalatorPvPGame_New(offerData, opponentUserObjFull, offerData.offerMessageId);
}

async function handleDiceEscalatorCancelUnifiedOffer_New(offerId, userWhoClicked, originalMessageId, originalChatId, callbackQueryIdPassed = null) {
    const LOG_PREFIX_DE_CANCEL_OFFER = `[DE_CancelOffer UID:${userWhoClicked.telegram_id} OfferID:${offerId}]`;
    const offerData = activeGames.get(offerId);
    const callbackQueryId = callbackQueryIdPassed;
    if (!offerData || offerData.type !== GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This offer has already concluded or vanished into the ether!", show_alert: false }).catch(()=>{});
        return;
    }
    if (offerData.initiator.userId !== userWhoClicked.telegram_id && String(userWhoClicked.telegram_id) !== ADMIN_USER_ID) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Patience, warrior! Only the one who made the offer can retract it.", show_alert: true }).catch(()=>{});
        return;
    }
    if (offerData.status !== 'pending_offer') {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This offer has already been decided or expired. It cannot be cancelled now.", show_alert: false }).catch(()=>{});
        return;
    }
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "Retracting the challenge..."}).catch(()=>{});
    activeGames.delete(offerId);
    console.log(`${LOG_PREFIX_DE_CANCEL_OFFER} Dice Escalator offer ${offerId} has been cancelled by ${userWhoClicked.telegram_id}.`);
    if (originalMessageId && bot) {
        const betDisplay = escapeMarkdownV2(formatCurrency(offerData.betAmount, 'SOL'));
        await bot.editMessageText(`❌ *Offer Retracted by Initiator\\!* ❌\nThe Dice Escalator challenge from ${offerData.initiator.displayName} \\(wager: *${betDisplay}*\\) has been cancelled\\. Perhaps another time the dice will roll\\!`, {
            chat_id: originalChatId, message_id: Number(originalMessageId), parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [] }
        }).catch(e => console.warn(`${LOG_PREFIX_DE_CANCEL_OFFER} Failed to edit the cancelled Dice Escalator offer message: ${e.message}`));
    }
}

// --- Dice Escalator Player vs. Bot (PvB) Game Logic ---
async function startDiceEscalatorPvBGame_New(chat, initiatorUserObj, betAmountLamports, originalOfferMessageIdToDelete = null, isPlayAgain = false) {
    const chatId = String(chat.id);
    const chatType = chat.type;
    const LOG_PREFIX_DE_PVB_START = `[DE_PvB_Start UID:${initiatorUserObj.telegram_id} CH:${chatId}]`;

    // Ensure initiatorUserObj is the full object from DB if not already
    const currentPlayerState = initiatorUserObj.balance !== undefined ? initiatorUserObj : await getOrCreateUser(initiatorUserObj.telegram_id, initiatorUserObj.username, initiatorUserObj.first_name);

    if(!currentPlayerState){
        console.error(`${LOG_PREFIX_DE_PVB_START} Could not get updated initiator profile for PvB game.`);
        await safeSendMessage(chatId, "😕 *Profile Snag\\!* An error occurred fetching your profile\\. The Dice Escalator game cannot start right now\\. Please try again later\\.", {parse_mode:"MarkdownV2"});
        return;
    }

    if (BigInt(currentPlayerState.balance) < betAmountLamports) { // Check against current balance
        await safeSendMessage(chatId, `💰 *Balance Too Low, ${getPlayerDisplayReference(currentPlayerState)}*\\!\nTo challenge the Bot in Dice Escalator for *${escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL'))}*, your current funds are a bit short\\. Please top up your casino wallet\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const gameId = `de_pvb_${chatId}_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
    const gameData = {
        gameId: gameId, type: GAME_IDS.DICE_ESCALATOR_PVB,
        player: {
            userId: currentPlayerState.telegram_id, username: currentPlayerState.username, firstName: currentPlayerState.first_name,
            displayName: getPlayerDisplayReference(currentPlayerState), score: 0, rolls: [],
            isGoingForJackpot: false, // Initialize player choice
            busted: false
        },
        betAmount: betAmountLamports, chatId: chatId, chatType: chatType,
        status: 'player_turn_awaiting_emoji',
        createdAt: Date.now(), gameMessageId: null, jackpotContribution: 0n, lastPlayerRoll: null, botRolls: [], botScore: 0
    };

    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const betTxNotes = `Dice Escalator PvB bet. Game ID: ${gameId}. Player: ${currentPlayerState.telegram_id}.`;
        const betResult = await updateUserBalanceAndLedger(client, currentPlayerState.telegram_id, -betAmountLamports, 'game_bet_de_pvb', { game_id_custom_field: gameId }, betTxNotes);
        if (!betResult.success) {
            await client.query('ROLLBACK');
            throw new Error(`Bet placement failed for Dice Escalator PvB: ${betResult.error}`);
        }
        gameData.jackpotContribution = BigInt(Math.floor(Number(betAmountLamports) * JACKPOT_CONTRIBUTION_PERCENT));
        if (gameData.jackpotContribution > 0n) {
            await client.query('UPDATE jackpots SET current_amount = current_amount + $1, updated_at = NOW() WHERE jackpot_id = $2', [gameData.jackpotContribution.toString(), MAIN_JACKPOT_ID]);
        }
        await client.query('COMMIT');
    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_DE_PVB_START} Database error: ${error.message}`);
        await safeSendMessage(chatId, "⚙️ *Database Hiccup\\!* ⚙️ Error starting game\\. Wager not processed\\. Try again\\.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }

    activeGames.set(gameId, gameData);
    if (originalOfferMessageIdToDelete && bot) {
        await bot.deleteMessage(chatId, Number(originalOfferMessageIdToDelete)).catch(e => {});
    }
    await updateDiceEscalatorPvBMessage_New(gameData); // Send initial game board
}

async function handleDEGoForJackpot(gameId, userWhoClicked, originalMessageId, callbackQueryId, chatData) {
    const LOG_PREFIX_DE_GO_JACKPOT = `[DE_GoJackpot UID:${userWhoClicked.telegram_id} Game:${gameId}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.DICE_ESCALATOR_PVB || gameData.status !== 'player_score_18_plus_awaiting_choice') {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This option is no longer available or the game state is incorrect.", show_alert: true }).catch(()=>{});
        return;
    }
    if (gameData.player.userId !== userWhoClicked.telegram_id) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This is not your decision to make.", show_alert: true }).catch(()=>{});
        return;
    }

    console.log(`${LOG_PREFIX_DE_GO_JACKPOT} Player ${userWhoClicked.telegram_id} chose to go for the jackpot for game ${gameId}.`);
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId).catch(()=>{});

    gameData.player.isGoingForJackpot = true;
    gameData.status = 'player_turn_awaiting_emoji'; // Ready for next roll
    activeGames.set(gameData.gameId, gameData);

    await safeSendMessage(gameData.chatId, `🔥 ${getPlayerDisplayReference(userWhoClicked)} is going for the Super Jackpot\\! No turning back now\\. Each roll will be a quick update\\. Send 🎲 to continue your perilous journey\\!`, { parse_mode: 'MarkdownV2' });
}

async function processDiceEscalatorPvBRollByEmoji_New(gameData, diceValue) {
    const LOG_PREFIX_DE_PVB_ROLL = `[DE_PvB_Roll UID:${gameData.player.userId} Game:${gameData.gameId}]`;

    if (gameData.status !== 'player_turn_awaiting_emoji' && gameData.status !== 'player_score_18_plus_awaiting_choice') {
        console.warn(`${LOG_PREFIX_DE_PVB_ROLL} Roll received but game status is '${gameData.status}'. Expected 'player_turn_awaiting_emoji' or 'player_score_18_plus_awaiting_choice'. Ignoring roll ${diceValue}.`);
        return;
    }
    if (gameData.status === 'player_score_18_plus_awaiting_choice') {
        gameData.status = 'player_turn_awaiting_emoji'; 
    }

    const player = gameData.player;
    player.rolls.push(diceValue);
    gameData.lastPlayerRoll = diceValue;

    const rollScoreValue = (diceValue === DICE_ESCALATOR_BUST_ON ? 0 : diceValue);
    const announcedScore = player.score + rollScoreValue;

    const rollAnnounceText = `🎲 ${player.displayName} rolled a\\.\\.\\. *${diceValue}*\\! ✨\n🎯 Their score climbs to: *${announcedScore}*`;
    await safeSendMessage(gameData.chatId, rollAnnounceText, { parse_mode: 'MarkdownV2' });

    if (diceValue === DICE_ESCALATOR_BUST_ON) {
        console.log(`${LOG_PREFIX_DE_PVB_ROLL} Player ${player.displayName} rolled a ${diceValue} (BUST VALUE)! Player busts.`);
        player.busted = true;
        gameData.status = 'player_busted';
        activeGames.set(gameData.gameId, gameData);
        console.log(`${LOG_PREFIX_DE_PVB_ROLL} Bust detected. Delaying final bust message by ${BUST_MESSAGE_DELAY_MS}ms.`);
        await sleep(BUST_MESSAGE_DELAY_MS);
        await finalizeDiceEscalatorPvBGame_New(gameData, 0);
        return;
    }

    player.score += diceValue; // Add actual dice value if not bust

    if (player.score >= TARGET_JACKPOT_SCORE) {
        console.log(`Player ${player.userId} reached jackpot target score ${player.score} in DE PvB!`);
        player.stood = true; // Effectively stood by hitting jackpot score
        gameData.status = 'player_stood';
        activeGames.set(gameData.gameId, gameData);
        await updateDiceEscalatorPvBMessage_New(gameData, true); 
        await sleep(1000);
        await processDiceEscalatorBotTurnPvB_New(gameData);
        return;
    }

    if (player.score >= 18 && !player.isGoingForJackpot && gameData.status !== 'player_score_18_plus_awaiting_choice') {
        gameData.status = 'player_score_18_plus_awaiting_choice';
        activeGames.set(gameData.gameId, gameData);
        await updateDiceEscalatorPvBMessage_New(gameData); 
        return; 
    }

    if (player.isGoingForJackpot) {
        gameData.status = 'player_turn_awaiting_emoji'; 
        activeGames.set(gameData.gameId, gameData);
    } else {
        gameData.status = 'player_turn_awaiting_emoji';
        activeGames.set(gameData.gameId, gameData);
        await updateDiceEscalatorPvBMessage_New(gameData); 
    }
}

async function updateDiceEscalatorPvBMessage_New(gameData, isStanding = false) {
    const LOG_PREFIX_DE_PVB_UPDATE = `[DE_PvB_Update Game:${gameData.gameId} Standing:${isStanding}]`;
    if (!gameData || !bot) {
        console.warn(`${LOG_PREFIX_DE_PVB_UPDATE} GameData or Bot missing. Cannot update.`);
        return;
    }
    if (gameData.gameMessageId) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.gameMessageId)).catch(e => {});
    }

    if (isStanding) {
        gameData.status = 'player_stood'; // Ensure status reflects standing if called with this flag
    }
    activeGames.set(gameData.gameId, gameData); // Update activeGames before formatting message

    const messageText = await formatDiceEscalatorGameMessage_New(gameData);
    let keyboard = { inline_keyboard: [] };

    if (gameData.status === 'player_score_18_plus_awaiting_choice') {
        keyboard = {
            inline_keyboard: [
                [{ text: "✋ Stand Firm!", callback_data: `de_stand_pvb:${gameData.gameId}` }],
                [{ text: "💎 Go for Jackpot!", callback_data: `de_pvb_go_for_jackpot:${gameData.gameId}` }]
            ]
        };
    } else if (gameData.status === 'player_turn_awaiting_emoji' && !isStanding && !gameData.player.busted) {
        if (!gameData.player.isGoingForJackpot) { 
            keyboard.inline_keyboard.push([{ text: "✋ Stand Firm!", callback_data: `de_stand_pvb:${gameData.gameId}` }]);
        }
        keyboard.inline_keyboard.push([{ text: "📖 Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER}` }]);
    } else if (gameData.status === 'player_stood' || gameData.status === 'bot_turn_complete') {
        keyboard = { inline_keyboard: [ [{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER}` }] ] };
    }
    const replyMarkup = (keyboard.inline_keyboard && keyboard.inline_keyboard.length > 0) ? keyboard : {};

    const sentMessage = await safeSendMessage(gameData.chatId, messageText, {
        parse_mode: 'MarkdownV2',
        reply_markup: replyMarkup
    });

    if (sentMessage && sentMessage.message_id) {
        gameData.gameMessageId = String(sentMessage.message_id);
        activeGames.set(gameData.gameId, gameData); // Ensure gameData with new messageId is saved
    } else {
        console.error(`${LOG_PREFIX_DE_PVB_UPDATE} CRITICAL: Failed to send/update PvB game message for ${gameData.gameId}.`);
    }
}

async function handleDiceEscalatorPvBStand_New(gameId, userWhoClicked, originalMessageId, callbackQueryId, chatData) {
    const LOG_PREFIX_DE_PVB_STAND = `[DE_PvB_Stand UID:${userWhoClicked.telegram_id} Game:${gameId}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.DICE_ESCALATOR_PVB) { 
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This game is no longer active.", show_alert: true }).catch(()=>{});
        return; 
    }
    if (gameData.player.userId !== userWhoClicked.telegram_id) { 
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This isn't your game to stand in!", show_alert: true }).catch(()=>{});
        return; 
    }
    
    if (gameData.player.isGoingForJackpot) { 
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "You've committed to the Jackpot Run! No standing allowed.", show_alert: true }).catch(()=>{});
        return;
    }

    if (gameData.status !== 'player_turn_awaiting_emoji' && gameData.status !== 'player_score_18_plus_awaiting_choice') {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "You can only stand when it's your active turn or when making the 18+ choice.", show_alert: false }).catch(()=>{});
        return;
    }

    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: `You stand strong with ${gameData.player.score} points! Bot's turn...`}).catch(()=>{});
    console.log(`${LOG_PREFIX_DE_PVB_STAND} Player ${gameData.player.displayName} stands with a score of ${gameData.player.score}.`);
    
    gameData.player.stood = true; // Mark player as stood
    activeGames.set(gameData.gameId, gameData); // Save this change
    await updateDiceEscalatorPvBMessage_New(gameData, true); // Pass true for isStanding, which will also set status to 'player_stood'
    await sleep(1000);
    await processDiceEscalatorBotTurnPvB_New(gameData);
}

async function processDiceEscalatorBotTurnPvB_New(gameData) {
    const LOG_PREFIX_DE_PVB_BOT = `[DE_PvB_BotTurn Game:${gameData.gameId}]`;
    if (!gameData || gameData.status !== 'player_stood') { // Ensure player has stood
        console.warn(`${LOG_PREFIX_DE_PVB_BOT} Bot turn was called, but game status is not 'player_stood'. Current status: ${gameData?.status}. Aborting bot turn.`);
        return;
    }
    console.log(`${LOG_PREFIX_DE_PVB_BOT} Bot Dealer is commencing its ${DE_PVB_BOT_ROLL_COUNT} rolls. Player ${gameData.player.displayName} stands with score: ${gameData.player.score}.`);
    await sleep(1000);
    gameData.botRolls = await getThreeDiceRollsViaHelper_DE_New(gameData.gameId, gameData.chatId);
    gameData.botScore = gameData.botRolls.reduce((sum, roll) => sum + roll, 0);
    gameData.status = 'bot_turn_complete';
    activeGames.set(gameData.gameId, gameData);
    console.log(`${LOG_PREFIX_DE_PVB_BOT} Bot's rolls: ${gameData.botRolls.join(', ')}. Bot's total score: ${gameData.botScore}.`);
    await updateDiceEscalatorPvBMessage_New(gameData); // Update message to show bot's rolls/score
    await sleep(1500);
    await finalizeDiceEscalatorPvBGame_New(gameData, gameData.botScore);
}

async function finalizeDiceEscalatorPvBGame_New(gameData, botScore) { // botScore passed for clarity, already in gameData
    const LOG_PREFIX_DE_PVB_FINAL = `[DE_PvB_Final Game:${gameData.gameId}]`;
    const { gameId, chatId, player, betAmount } = gameData;
    activeGames.delete(gameId);
    let resultText = "";
    let titleEmoji = "🏁";
    let payoutLamports = 0n;
    let playerWins = false;
    let jackpotWon = false;
    let jackpotAmountClaimed = 0n;
    const fetchedPlayerBalance = await getUserBalance(player.userId);
    let finalPlayerBalance = fetchedPlayerBalance !== null ? fetchedPlayerBalance : BigInt(0);
    const wagerDisplay = escapeMarkdownV2(formatCurrency(betAmount, 'SOL'));

    if (player.busted) {
        titleEmoji = "💥";
        resultText = `*Oh No, You Busted\\!* 💥\n${player.displayName}, your roll of \`${gameData.lastPlayerRoll}\` \\(bust on \`${DICE_ESCALATOR_BUST_ON}\`\\) means the Bot Dealer claims the wager of *${wagerDisplay}* this round\\. Tough luck\\!`;
        const clientLoss = await pool.connect();
        try {
            await clientLoss.query('BEGIN');
            await updateUserBalanceAndLedger(clientLoss, player.userId, 0n, 'loss_de_pvb_bust', { game_id_custom_field: gameId, final_player_score: player.score }, `DE PvB Bust. GID:${gameId}.`);
            await clientLoss.query('COMMIT');
        } catch (e) { if(clientLoss) await clientLoss.query('ROLLBACK'); console.error(`${LOG_PREFIX_DE_PVB_FINAL} Error logging PvB bust for ledger: ${e.message}`);}
        finally { if(clientLoss) clientLoss.release(); }
        finalPlayerBalance = fetchedPlayerBalance !== null ? fetchedPlayerBalance : BigInt(0); // Re-affirm from DB if possible or use pre-deduction state
    } else if (player.score > gameData.botScore) { // Use gameData.botScore which is now set
        titleEmoji = "🎉";
        playerWins = true;
        payoutLamports = betAmount * 2n;
        resultText = `*VICTORY for ${player.displayName}\\!* 🎉\nYour outstanding score of *${player.score}* has crushed the Bot Dealer's *${gameData.botScore}*\\!\n💰 You've won a handsome profit of *${escapeMarkdownV2(formatCurrency(payoutLamports - betAmount, 'SOL'))}*\\!`;
        if (player.score >= TARGET_JACKPOT_SCORE) {
            const client = await pool.connect();
            try {
                await client.query('BEGIN');
                const jackpotRes = await client.query('SELECT current_amount FROM jackpots WHERE jackpot_id = $1 FOR UPDATE', [MAIN_JACKPOT_ID]);
                if (jackpotRes.rows.length > 0 && BigInt(jackpotRes.rows[0].current_amount || '0') > 0n) {
                    jackpotAmountClaimed = BigInt(jackpotRes.rows[0].current_amount);
                    await client.query('UPDATE jackpots SET current_amount = 0, last_won_by_telegram_id = $1, updated_at = NOW() WHERE jackpot_id = $2', [player.userId, MAIN_JACKPOT_ID]);
                    payoutLamports += jackpotAmountClaimed;
                    jackpotWon = true;
                    titleEmoji = "💎";
                    resultText += `\n\n✨🎊 *~~ SUPER JACKPOT CLAIMED\\!\\!\\! ~~* 🎊✨\nINCREDIBLE, ${player.displayName}\\! You've also smashed the Super Jackpot, winning an additional *${escapeMarkdownV2(formatCurrency(jackpotAmountClaimed, 'SOL'))}*\\! Absolutely legendary\\! 🥳🥳🥳`;
                    console.log(`${LOG_PREFIX_DE_PVB_FINAL} JACKPOT WON! User: ${player.userId}, Amount: ${jackpotAmountClaimed}`);
                }
                await client.query('COMMIT');
            } catch (jackpotError) {
                if(client) await client.query('ROLLBACK').catch(()=>{});
                console.error(`${LOG_PREFIX_DE_PVB_FINAL} Error processing DE PvB jackpot win: ${jackpotError.message}`);
                resultText += `\n\n_(Psst\\! There was a tiny hiccup with the jackpot confirmation, but your main winnings are secure\\! Our gnomes are checking it\\.)_`;
            } finally {
                if(client) client.release();
            }
        }
    } else if (player.score === gameData.botScore) {
        titleEmoji = "⚖️";
        resultText = `*It's a Push\\!* ⚖️\n${player.displayName} \\(Score: *${player.score}*\\) and the Bot Dealer \\(Score: *${gameData.botScore}*\\) are perfectly matched\\! Your wager of *${wagerDisplay}* has been returned\\. Close call\\!`;
        payoutLamports = betAmount;
    } else { // Bot score is higher
        titleEmoji = "🤖";
        resultText = `*The Bot Dealer Prevails\\!* 🤖\nA valiant effort, ${player.displayName}, but the Bot Dealer's score of *${gameData.botScore}* edged out your *${player.score}*\\. Your wager of *${wagerDisplay}* goes to the house this time\\. Better luck on the next thrilling roll\\! 💪`;
        const clientLossScore = await pool.connect();
        try {
            await clientLossScore.query('BEGIN');
            await updateUserBalanceAndLedger(clientLossScore, player.userId, 0n, 'loss_de_pvb_score', { game_id_custom_field: gameId, final_player_score: player.score, bot_score: gameData.botScore }, `DE PvB Loss by score. GID:${gameId}.`);
            await clientLossScore.query('COMMIT');
        } catch (e) { if(clientLossScore) await clientLossScore.query('ROLLBACK'); console.error(`${LOG_PREFIX_DE_PVB_FINAL} Error logging PvB loss by score for ledger: ${e.message}`);}
        finally { if(clientLossScore) clientLossScore.release(); }
        finalPlayerBalance = fetchedPlayerBalance !== null ? fetchedPlayerBalance : BigInt(0); // Re-affirm from DB
    }

    if (payoutLamports > 0n) {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');
            const transactionType = playerWins ? (jackpotWon ? 'win_de_pvb_jackpot' : 'win_de_pvb') : 'push_de_pvb';
            const notes = `DE PvB Result: ${playerWins ? (jackpotWon ? 'Jackpot!' : 'Win') : 'Push'}. GID:${gameId}. Player Score:${player.score}, Bot Score:${gameData.botScore}. Jackpot Amount:${formatCurrency(jackpotAmountClaimed, 'SOL')}`;
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, player.userId, payoutLamports, transactionType, { game_id_custom_field: gameId, jackpot_amount_custom_field: jackpotAmountClaimed.toString() }, notes);
            if (balanceUpdateResult.success) {
                await client.query('COMMIT');
                finalPlayerBalance = balanceUpdateResult.newBalanceLamports;
            } else {
                await client.query('ROLLBACK');
                resultText += `\n\n⚙️ *Balance Update Issue* ⚙️\nThere was a hiccup updating your balance: \`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown database error")}\`\\. Our support team is on the case\\!`;
                console.error(`${LOG_PREFIX_DE_PVB_FINAL} Failed to update balance for user ${player.userId} post-game. Error: ${balanceUpdateResult.error}`);
            }
        } catch (dbError) {
            if(client) await client.query('ROLLBACK').catch(()=>{});
            console.error(`${LOG_PREFIX_DE_PVB_FINAL} Database transaction error during DE PvB finalization: ${dbError.message}`);
            resultText += `\n\n🚨 *Critical DB Error* 🚨\nA serious database issue occurred during game finalization\\. Your balance may not be correctly updated\\. Please contact support with Game ID: \`${escapeMarkdownV2(gameId)}\`\\.`;
        } finally {
            if(client) client.release();
        }
    }
    const fullResultMessage = `${titleEmoji} *Dice Escalator vs\\. Bot \\- Game Over\\!* ${titleEmoji}\n\n` +
                              `--- *Final Scores* ---\n` +
                              `👤 ${player.displayName}: *${player.score}* ${player.busted ? "💥 \\(Busted\\)" : ""}\n` +
                              `🤖 Bot Dealer: *${gameData.botScore}*\n\n` + // Use gameData.botScore
                              `${resultText}\n\n` +
                              `💳 Your new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalPlayerBalance, 'USD'))}* / *${escapeMarkdownV2(formatCurrency(finalPlayerBalance, 'SOL'))}*\\.`;
    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR_PVB, betAmount);
    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(fullResultMessage, {
            chat_id: gameData.chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: finalKeyboard
        }).catch(async e => {
            console.warn(`${LOG_PREFIX_DE_PVB_FINAL} Failed to edit final PvB message, sending new: ${e.message}`);
            await safeSendMessage(gameData.chatId, fullResultMessage, {parse_mode: 'MarkdownV2', reply_markup: finalKeyboard});
        });
    } else {
        await safeSendMessage(gameData.chatId, fullResultMessage, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
    }
    console.log(`${LOG_PREFIX_DE_PVB_FINAL} Game ${gameId} finalized. Player ${player.displayName} ${playerWins ? 'won' : 'did not win'}. Jackpot claimed: ${jackpotWon}.`);
}

// --- Dice Escalator Player vs. Player (PvP) Game Logic (NEW - Quick Update Flow) ---
async function startDiceEscalatorPvPGame_New(offerData, opponentUserObj, originalOfferMessageIdToDelete) {
    const chatId = offerData.chatId;
    const LOG_PREFIX_DE_PVP_START = `[DE_PvP_Start Offer:${offerData.gameId} CH:${chatId}]`;
    const initiatorUserObjFull = offerData.initiatorUserObj || await getOrCreateUser(offerData.initiator.userId, offerData.initiator.username, offerData.initiator.firstName);
    const opponentUserObjFull = await getOrCreateUser(opponentUserObj.telegram_id, opponentUserObj.username, opponentUserObj.first_name);


    if (!initiatorUserObjFull || !opponentUserObjFull) { 
        console.error(`${LOG_PREFIX_DE_PVP_START} Failed to get full user objects for PvP. Initiator: ${!!initiatorUserObjFull}, Opponent: ${!!opponentUserObjFull}`);
        await safeSendMessage(chatId, "A profile error occurred. Dice Escalator PvP cannot start.", {parse_mode: 'MarkdownV2'});
        return; 
    }
    const betAmountLamports = offerData.betAmount;
    const client = await pool.connect();
    try { 
        await client.query('BEGIN');
        // Deduct bet from initiator
        const initiatorBetResult = await updateUserBalanceAndLedger(
            client, initiatorUserObjFull.telegram_id, -betAmountLamports,
            'bet_placed_de_pvp_init', { game_id_custom_field: `temp_offer_${offerData.gameId}_to_pvp`, opponent_id_custom_field: opponentUserObjFull.telegram_id },
            `Initiator bet for DE PvP from offer ${offerData.gameId} vs ${opponentUserObjFull.telegram_id}`
        );
        if (!initiatorBetResult.success) throw new Error(`Initiator bet placement failed: ${initiatorBetResult.error}`);
        initiatorUserObjFull.balance = initiatorBetResult.newBalanceLamports;

        // Deduct bet from opponent
        const opponentBetResult = await updateUserBalanceAndLedger(
            client, opponentUserObjFull.telegram_id, -betAmountLamports,
            'bet_placed_de_pvp_join', { game_id_custom_field: `temp_offer_${offerData.gameId}_to_pvp`, opponent_id_custom_field: initiatorUserObjFull.telegram_id },
            `Opponent bet for DE PvP from offer ${offerData.gameId} vs ${initiatorUserObjFull.telegram_id}`
        );
        if (!opponentBetResult.success) throw new Error(`Opponent bet placement failed: ${opponentBetResult.error}`);
        opponentUserObjFull.balance = opponentBetResult.newBalanceLamports;

        await client.query('COMMIT');
    } catch (error) { 
        if (client) await client.query('ROLLBACK');
        console.error(`${LOG_PREFIX_DE_PVP_START} DB error placing PvP bets: ${error.message}`);
        await safeSendMessage(chatId, `A database error occurred while trying to place bets for the Dice Escalator PvP game. The game cannot start. Please try making a new offer.`, {parse_mode: 'MarkdownV2'});
        // No activeGames.delete(offerId) here as it's already deleted by caller
        return; 
    } finally { if(client) client.release(); }

    const gameId = `de_pvp_${chatId}_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
    const gameData = {
        gameId: gameId, type: GAME_IDS.DICE_ESCALATOR_PVP,
        initiator: { userId: initiatorUserObjFull.telegram_id, displayName: getPlayerDisplayReference(initiatorUserObjFull), score: 0, rolls: [], isTurn: true, busted: false, stood: false },
        opponent: { userId: opponentUserObjFull.telegram_id, displayName: getPlayerDisplayReference(opponentUserObjFull), score: 0, rolls: [], isTurn: false, busted: false, stood: false },
        betAmount: betAmountLamports, chatId: chatId, chatType: offerData.chatType,
        status: 'p1_awaiting_roll1_emoji', // Initiator (P1) rolls first
        createdAt: Date.now(), gameMessageId: null, lastRollValue: null,
    };
    activeGames.set(gameId, gameData);
    if (originalOfferMessageIdToDelete && bot) { 
        await bot.deleteMessage(chatId, Number(originalOfferMessageIdToDelete)).catch(e => {});
    }
    await updateDiceEscalatorPvPMessage_New(gameData);
}

async function processDiceEscalatorPvPRollByEmoji_New(gameData, diceValue, userIdWhoRolled) {
    const LOG_PREFIX_DE_PVP_ROLL = `[DE_PvP_Roll UID:${userIdWhoRolled} Game:${gameData.gameId}]`;
    let currentPlayer, otherPlayer, playerKeyForStatus;
    let isP1Rolling = false;
    if (gameData.initiator.userId === userIdWhoRolled && gameData.initiator.isTurn) {
        if (gameData.status !== 'p1_awaiting_roll1_emoji' && gameData.status !== 'p1_awaiting_roll2_emoji') { console.warn(`${LOG_PREFIX_DE_PVP_ROLL} P1 roll in wrong status: ${gameData.status}`); return; }
        currentPlayer = gameData.initiator; otherPlayer = gameData.opponent; playerKeyForStatus = 'p1'; isP1Rolling = true;
    } else if (gameData.opponent.userId === userIdWhoRolled && gameData.opponent.isTurn) {
        if (gameData.status !== 'p2_awaiting_roll1_emoji' && gameData.status !== 'p2_awaiting_roll2_emoji') { console.warn(`${LOG_PREFIX_DE_PVP_ROLL} P2 roll in wrong status: ${gameData.status}`); return; }
        currentPlayer = gameData.opponent; otherPlayer = gameData.initiator; playerKeyForStatus = 'p2';
    } else { console.warn(`${LOG_PREFIX_DE_PVP_ROLL} Roll from non-active or wrong turn.`); return; }

    currentPlayer.rolls.push(diceValue);
    gameData.lastRollValue = diceValue;
    const rollScoreValue = (diceValue === DICE_ESCALATOR_BUST_ON ? 0 : diceValue);
    const announcedScore = currentPlayer.score + rollScoreValue;
    const rollAnnounceText = `🎲 ${currentPlayer.displayName} rolls a\\.\\.\\. *${diceValue}*\\! ✨\n🎯 Their score this round is now: *${announcedScore}*`;
    await safeSendMessage(gameData.chatId, rollAnnounceText, { parse_mode: 'MarkdownV2' });

    if (diceValue === DICE_ESCALATOR_BUST_ON) {
        currentPlayer.busted = true; gameData.status = isP1Rolling ? 'p1_busted' : 'p2_busted';
        activeGames.set(gameData.gameId, gameData);
        await updateDiceEscalatorPvPMessage_New(gameData);
        await sleep(BUST_MESSAGE_DELAY_MS);
        await resolveDiceEscalatorPvPGame_New(gameData, currentPlayer.userId);
        return;
    }
    currentPlayer.score += diceValue;
    activeGames.set(gameData.gameId, gameData);
    if (!isP1Rolling && !currentPlayer.busted && otherPlayer.stood && currentPlayer.score > otherPlayer.score) {
        gameData.status = 'p2_wins_by_crossing_score'; activeGames.set(gameData.gameId, gameData);
        await updateDiceEscalatorPvPMessage_New(gameData);
        await sleep(1000);
        await resolveDiceEscalatorPvPGame_New(gameData);
        return;
    }
    if (!currentPlayer.busted && !currentPlayer.stood) {
         gameData.status = `${playerKeyForStatus}_awaiting_roll1_emoji`; // Always expect another roll or stand
    }
    activeGames.set(gameData.gameId, gameData);
    await updateDiceEscalatorPvPMessage_New(gameData);
}

async function updateDiceEscalatorPvPMessage_New(gameData) {
    const LOG_PREFIX_DE_PVP_UPDATE = `[DE_PvP_Update Game:${gameData.gameId} Status:${gameData.status}]`;
    if (!gameData || !bot) { console.warn(`${LOG_PREFIX_DE_PVP_UPDATE} GameData or Bot missing.`); return; }
    if (gameData.gameMessageId) { await bot.deleteMessage(gameData.chatId, Number(gameData.gameMessageId)).catch(e => {}); }
    const messageText = await formatDiceEscalatorGameMessage_New(gameData);
    let keyboard = { inline_keyboard: [] };
    let currentTurnPlayer = null;
    if (gameData.initiator.isTurn) currentTurnPlayer = gameData.initiator;
    else if (gameData.opponent.isTurn) currentTurnPlayer = gameData.opponent;
    if (currentTurnPlayer && !currentTurnPlayer.stood && !currentTurnPlayer.busted &&
        (gameData.status === `${currentTurnPlayer === gameData.initiator ? 'p1' : 'p2'}_awaiting_roll1_emoji` ||
         gameData.status === `${currentTurnPlayer === gameData.initiator ? 'p1' : 'p2'}_awaiting_roll2_emoji`)) {
        keyboard.inline_keyboard.push([{ text: `✋ ${currentTurnPlayer.displayName}, Stand!`, callback_data: `de_stand_pvp:${gameData.gameId}` }]);
    }
    keyboard.inline_keyboard.push([{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER}` }]);
    const sentMessage = await safeSendMessage(gameData.chatId, messageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
    if (sentMessage && sentMessage.message_id) {
        gameData.gameMessageId = String(sentMessage.message_id);
        activeGames.set(gameData.gameId, gameData);
    } else {
        console.error(`${LOG_PREFIX_DE_PVP_UPDATE} CRITICAL: Failed to send/update PvP game message for ${gameData.gameId}.`);
        await safeSendMessage(gameData.chatId, `🚨 *Display Glitch\\!* 🚨\nA display error occurred in your PvP Dice Escalator game\\. The game state might be desynced\\. Please contact support if issues persist, quoting Game ID: \`${escapeMarkdownV2(gameData.gameId)}\``, {parse_mode: 'MarkdownV2'});
    }
}

async function handleDiceEscalatorPvPStand_New(gameId, userWhoClicked, originalMessageId, callbackQueryId, chatData) {
    const LOG_PREFIX_DE_PVP_STAND = `[DE_PvP_Stand UID:${userWhoClicked.telegram_id} Game:${gameId}]`;
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DICE_ESCALATOR_PVP) { if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Dice Escalator PvP duel seems to have vanished!", show_alert: true }).catch(()=>{}); return; }
    let playerStanding, otherPlayer; let isP1Standing = false;
    if (gameData.initiator.userId === userWhoClicked.telegram_id && gameData.initiator.isTurn) { playerStanding = gameData.initiator; otherPlayer = gameData.opponent; isP1Standing = true; }
    else if (gameData.opponent.userId === userWhoClicked.telegram_id && gameData.opponent.isTurn) { playerStanding = gameData.opponent; otherPlayer = gameData.initiator; }
    else { if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Hold up! It's not your turn or this isn't your game.", show_alert: true }).catch(()=>{}); return; }
    if (playerStanding.stood || playerStanding.busted) { if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "You've already made your move this round (stood or busted).", show_alert: true }).catch(()=>{}); return; }
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: `You stand strong with ${playerStanding.score} points! Let's see if it's enough...`}).catch(()=>{});
    console.log(`${LOG_PREFIX_DE_PVP_STAND} Player ${playerStanding.displayName} stands with score ${playerStanding.score}.`);
    playerStanding.stood = true; playerStanding.isTurn = false;
    activeGames.set(gameData.gameId, gameData);
    const standAnnounceText = `✋ ${playerStanding.displayName} takes a stand with a confident score of *${escapeMarkdownV2(String(playerStanding.score))}*\\! Let's see if it holds against the challenge\\! 💪`;
    await safeSendMessage(gameData.chatId, standAnnounceText, { parse_mode: 'MarkdownV2' });
    await sleep(750);
    if (isP1Standing) {
        gameData.status = 'p1_stood';
        if (otherPlayer.busted || otherPlayer.stood) { gameData.status = 'game_over_pvp_resolved'; activeGames.set(gameData.gameId, gameData); await updateDiceEscalatorPvPMessage_New(gameData); await sleep(1000); await resolveDiceEscalatorPvPGame_New(gameData); }
        else { otherPlayer.isTurn = true; gameData.status = 'p2_awaiting_roll1_emoji'; activeGames.set(gameData.gameId, gameData); await updateDiceEscalatorPvPMessage_New(gameData); }
    } else { gameData.status = 'p2_stood'; gameData.status = 'game_over_pvp_resolved'; activeGames.set(gameData.gameId, gameData); await updateDiceEscalatorPvPMessage_New(gameData); await sleep(1000); await resolveDiceEscalatorPvPGame_New(gameData); }
}

async function resolveDiceEscalatorPvPGame_New(gameData, playerWhoBustedId = null) {
    const LOG_PREFIX_DE_PVP_RESOLVE = `[DE_PvP_Resolve Game:${gameData.gameId}]`;
    activeGames.delete(gameData.gameId);
    const p1 = gameData.initiator; const p2 = gameData.opponent;
    let winner = null, loser = null, isPush = false, resultOutcomeText = "", titleEmoji = "🏁";
    let totalPotLamports = gameData.betAmount * 2n;
    let p1Payout = 0n; let p2Payout = 0n;
    let p1Ledger = 'loss_de_pvp'; let p2Ledger = 'loss_de_pvp';
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    const wagerLine = `💸 Wager: *${betDisplayUSD}* each`;

    if (playerWhoBustedId === p1.userId || p1.busted) { titleEmoji = "💥"; winner = p2; loser = p1; p1.busted = true; p2Payout = totalPotLamports; p2Ledger = 'win_de_pvp_opponent_bust'; resultOutcomeText = `${p1.displayName} hit a snag and *BUSTED*\\! 💥\n🎉 ${winner.displayName} seizes victory and the pot\\!`; }
    else if (playerWhoBustedId === p2.userId || p2.busted) { titleEmoji = "💥"; winner = p1; loser = p2; p2.busted = true; p1Payout = totalPotLamports; p1Ledger = 'win_de_pvp_opponent_bust'; resultOutcomeText = `${p2.displayName} took a risk and *BUSTED*\\! 💥\n🎉 ${winner.displayName} masterfully claims the win\\!`; }
    else if (gameData.status === 'p2_wins_by_crossing_score') { titleEmoji = "🏆"; winner = p2; loser = p1; p2Payout = totalPotLamports; p2Ledger = 'win_de_pvp_score_cross'; resultOutcomeText = `Incredible move\\! ${winner.displayName} escalates past ${loser.displayName}'s score \\(${p2.score} vs\\. ${p1.score}\\) and *WINS THE DUEL*\\! 🥳`; }
    else if (p1.stood && p2.stood) {
        if (p1.score > p2.score) { titleEmoji = "🏆"; winner = p1; loser = p2; p1Payout = totalPotLamports; p1Ledger = 'win_de_pvp_score'; resultOutcomeText = `What a performance\\! ${winner.displayName} defends their high score \\(${p1.score} vs\\. ${p2.score}\\) and *CLAIMS THE POT*\\!`; }
        else if (p2.score > p1.score) { titleEmoji = "🏆"; winner = p2; loser = p1; p2Payout = totalPotLamports; p2Ledger = 'win_de_pvp_score'; resultOutcomeText = `Strategic genius\\! ${winner.displayName} outmaneuvers ${loser.displayName} \\(${p2.score} vs\\. ${p1.score}\\) and *TAKES THE WIN*\\!`; }
        else { titleEmoji = "⚖️"; isPush = true; resultOutcomeText = `*A Perfect Tie\\!* ⚖️\nRemarkable\\! Both ${p1.displayName} and ${p2.displayName} scored *${p1.score}*\\. All bets are returned\\. An honorable draw\\!`; p1Payout = gameData.betAmount; p2Payout = gameData.betAmount; p1Ledger = 'push_de_pvp'; p2Ledger = 'push_de_pvp'; }
    } else { titleEmoji = "⚙️"; isPush = true; resultOutcomeText = `*Unexpected Finish* ⚙️\nThe game concluded in a peculiar way due to an unforeseen state\\. To ensure fairness, all bets are being refunded\\. Our apologies for this rare anomaly\\!`; p1Payout = gameData.betAmount; p2Payout = gameData.betAmount; p1Ledger = 'refund_de_pvp_error'; p2Ledger = 'refund_de_pvp_error'; console.error(`${LOG_PREFIX_DE_PVP_RESOLVE} Undetermined PvP outcome for game ${gameData.gameId}. P1: ${p1.score} (stood:${p1.stood},busted:${p1.busted}), P2: ${p2.score} (stood:${p2.stood},busted:${p2.busted}). Refunding both.`); }
    let finalMessageText = `${titleEmoji} *Dice Escalator PvP - The Grand Finale\\!* ${titleEmoji}\n\n${wagerLine}\n\n--- *Final Scores* ---\n👤 **${p1.displayName}** \\(P1\\) \\- Score: *${p1.score}* ${p1.busted ? "💥 BUSTED" : (p1.stood ? "✅ STOOD" : "🤔")}\n👤 **${p2.displayName}** \\(P2\\) \\- Score: *${p2.score}* ${p2.busted ? "💥 BUSTED" : (p2.stood ? "✅ STOOD" : "🤔")}\n\n------------------------------------\n${resultOutcomeText}`;
    let p1InitialBalance = await getUserBalance(p1.userId); let p2InitialBalance = await getUserBalance(p2.userId);
    let p1FinalBal = p1InitialBalance !== null ? p1InitialBalance : BigInt(0); let p2FinalBal = p2InitialBalance !== null ? p2InitialBalance : BigInt(0);
    const client = await pool.connect();
    try { 
        await client.query('BEGIN');
        const p1UpdateResult = await updateUserBalanceAndLedger(client, p1.userId, p1Payout, p1Ledger, { game_id_custom_field: gameData.gameId, opponent_id_custom_field: p2.userId }, `DE PvP Result vs ${p2.displayName}`);
        if(!p1UpdateResult.success) throw new Error(`P1 Balance Update Failed: ${p1UpdateResult.error}`);
        p1FinalBal = p1UpdateResult.newBalanceLamports;

        const p2UpdateResult = await updateUserBalanceAndLedger(client, p2.userId, p2Payout, p2Ledger, { game_id_custom_field: gameData.gameId, opponent_id_custom_field: p1.userId }, `DE PvP Result vs ${p1.displayName}`);
        if(!p2UpdateResult.success) throw new Error(`P2 Balance Update Failed: ${p2UpdateResult.error}`);
        p2FinalBal = p2UpdateResult.newBalanceLamports;
        await client.query('COMMIT');
    } catch (e) { 
        if(client) await client.query('ROLLBACK');
        console.error(`${LOG_PREFIX_DE_PVP_RESOLVE} CRITICAL DB Error Finalizing DE PvP ${gameData.gameId}: ${e.message}`);
        finalMessageText += `\n\n⚠️ **CRITICAL SYSTEM ERROR**: Failed to update player balances correctly. Administrators have been notified. Please contact support with Game ID: \`${escapeMarkdownV2(gameData.gameId)}\``;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL DE PvP Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameData.gameId)}\`\nError: ${e.message}. MANUAL CHECK OF BALANCES FOR ${p1.displayName} & ${p2.displayName} IS URGENTLY REQUIRED.`);
    } finally { if (client) client.release(); }
    finalMessageText += `\n\n--- *Updated Balances* ---\n💰 ${p1.displayName}: *${escapeMarkdownV2(await formatBalanceForDisplay(p1FinalBal, 'USD'))}*\n💰 ${p2.displayName}: *${escapeMarkdownV2(await formatBalanceForDisplay(p2FinalBal, 'USD'))}*`;
    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR_PVP, gameData.betAmount);
    if (gameData.gameMessageId && bot) { 
        await bot.editMessageText(finalMessageText, { chatId: String(gameData.chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: finalKeyboard })
        .catch(async (e) => { 
            console.warn(`${LOG_PREFIX_DE_PVP_RESOLVE} Failed to edit DE PvP final message (ID: ${gameData.gameMessageId}), sending new: ${e.message}`);
            await safeSendMessage(gameData.chatId, finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard }); 
        });
    } else { 
        await safeSendMessage(gameData.chatId, finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard }); 
    }
    console.log(`${LOG_PREFIX_DE_PVP_RESOLVE} Game ${gameData.gameId} finalized. Winner: ${winner ? winner.displayName : (isPush ? 'Push' : 'Error/Unknown')}.`);
}

// console.log("Part 5b, Section 1 (Refined Jackpot Run UI) - Complete.");
// --- End of Part 5b, Section 1 ---
// --- Start of Part 5b, Section 2 (COMPLETE REWRITE V5.1 - Casino Standard, Correct Flow) ---
// index.js - Part 5b, Section 2 (Dice 21 / Blackjack-style game logic)
// SEGMENT 1 of 2
//-------------------------------------------------------------------------------------------------
// This section contains the initial parts of Dice 21 (Blackjack-style) game logic.
// Assumed constants (GAME_IDS, DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE, JOIN_GAME_TIMEOUT_MS, etc.)
// and utility functions (getOrCreateUser, getPlayerDisplayReference, formatBalanceForDisplay,
// updateUserBalanceAndLedger, generateGameId, safeSendMessage, createPostGameKeyboard,
// escapeMarkdownV2, pool, activeGames, groupGameSessions, updateGroupGameDetails,
// DICE_ROLL_POLLING_MAX_ATTEMPTS, DICE_ROLL_POLLING_INTERVAL_MS, sleep,
// QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, formatDiceRolls, notifyAdmin,
// insertDiceRollRequest, getDiceRollRequestResult)
// from other parts of your single-file code are available in this scope.

// --- Helper function for a single dice roll via Helper Bot ---
// Returns an object: { roll: number, error: false } on success, 
// or { error: true, message: string, isTimeout: boolean } on failure.
async function getSingleDiceRollViaHelper(gameId, chatIdForLog, userIdForRoll, rollPurposeNote) {
    const logPrefix = `[GetSingleDiceRollHelper GID:${gameId} Purpose:"${rollPurposeNote}" UID:${userIdForRoll || 'BOT_INTERNAL'}]`;
    console.log(`${logPrefix} Requesting die from Helper Bot.`);
    let client = null;
    let requestId = null;
    let specificErrorMessage = `Failed to obtain dice roll for "${rollPurposeNote}" via Helper Bot.`; // Default detailed error
    let isTimeoutErrorFlag = false;

    try {
        client = await pool.connect();
        const requestResult = await insertDiceRollRequest(client, gameId, String(chatIdForLog), userIdForRoll, '🎲', rollPurposeNote);
        if (!requestResult.success || !requestResult.requestId) {
            specificErrorMessage = requestResult.error || `Database error when creating roll request for "${rollPurposeNote}".`;
            console.error(`${logPrefix} ${specificErrorMessage}`);
            throw new Error(specificErrorMessage);
        }
        requestId = requestResult.requestId;
        console.log(`${logPrefix} Roll request successfully created in DB (Request ID: ${requestId}).`);
        client.release(); client = null;

        let attempts = 0;
        console.log(`${logPrefix} Starting to poll for roll result (Request ID: ${requestId}). Max attempts: ${DICE_ROLL_POLLING_MAX_ATTEMPTS}. Interval: ${DICE_ROLL_POLLING_INTERVAL_MS}ms.`);
        while (attempts < DICE_ROLL_POLLING_MAX_ATTEMPTS) {
            await sleep(DICE_ROLL_POLLING_INTERVAL_MS);
            if (isShuttingDown) {
                specificErrorMessage = "System shutdown initiated while waiting for Helper Bot dice roll response.";
                console.warn(`${logPrefix} ${specificErrorMessage}`);
                throw new Error(specificErrorMessage);
            }

            client = await pool.connect();
            const statusResult = await getDiceRollRequestResult(client, requestId);
            client.release(); client = null;

            if (statusResult.success && statusResult.status === 'completed') {
                if (typeof statusResult.roll_value === 'number' && statusResult.roll_value >= 1 && statusResult.roll_value <= 6) {
                    console.log(`${logPrefix} Successfully received roll value from Helper Bot: ${statusResult.roll_value} (Request ID: ${requestId}).`);
                    return { roll: statusResult.roll_value, error: false }; // SUCCESS
                } else {
                    specificErrorMessage = `Helper Bot returned a completed roll for "${rollPurposeNote}" (Request ID: ${requestId}), but the dice value was invalid: '${statusResult.roll_value}'.`;
                    console.error(`${logPrefix} ${specificErrorMessage}`);
                    throw new Error(specificErrorMessage);
                }
            } else if (statusResult.success && statusResult.status === 'error') {
                specificErrorMessage = statusResult.notes || `Helper Bot explicitly reported an error for "${rollPurposeNote}" (Request ID: ${requestId}).`;
                console.error(`${logPrefix} ${specificErrorMessage}`);
                throw new Error(specificErrorMessage);
            }
            attempts++;
            if (attempts % 5 === 0 || attempts === DICE_ROLL_POLLING_MAX_ATTEMPTS -1 || attempts === 1 ) {
                 console.log(`${logPrefix} Polling attempt ${attempts} of ${DICE_ROLL_POLLING_MAX_ATTEMPTS} for roll request ID ${requestId}... DB Status: ${statusResult.status || 'pending/unknown'}`);
            }
        }
        
        isTimeoutErrorFlag = true; // Set flag for timeout
        specificErrorMessage = `Timeout after ${attempts} attempts waiting for Helper Bot response for dice roll: "${rollPurposeNote}" (Request ID: ${requestId}).`;
        throw new Error(specificErrorMessage); // Timeout error

    } catch (error) {
        if (client) client.release();
        
        const finalErrorMessageForReturn = error.message || specificErrorMessage;
        console.error(`${logPrefix} Final error state in getSingleDiceRollViaHelper: ${finalErrorMessageForReturn}`);
        
        if (requestId) { 
            let markErrorClient = null;
            try {
                markErrorClient = await pool.connect();
                const statusToUpdate = isTimeoutErrorFlag ? 'timeout' : 'error';
                console.log(`${logPrefix} Attempting to mark roll request ${requestId} as '${statusToUpdate}' in DB due to error: ${finalErrorMessageForReturn.substring(0,100)}`);
                await markErrorClient.query("UPDATE dice_roll_requests SET status=$1, notes=$2 WHERE request_id=$3 AND status = 'pending'", 
                    [statusToUpdate, String(finalErrorMessageForReturn).substring(0,250), requestId]); 
            } catch (dbMarkError) {
                console.error(`${logPrefix} CRITICAL: Failed to mark roll request ${requestId} as failed in DB: ${dbMarkError.message}`);
            } finally {
                if (markErrorClient) markErrorClient.release();
            }
        }
        return { error: true, message: finalErrorMessageForReturn, isTimeout: isTimeoutErrorFlag }; 
    }
}

// --- Dice 21 Main Command Handler (Handles /d21, creates Unified Offer) ---
// Name: handleStartDice21Command
async function handleStartDice21Command(msg, betAmountLamports, gameModeArg = null) { // gameModeArg retained for signature consistency
    // ***** CORRECTED LINE FOR USER ID EXTRACTION *****
    const userId = String(msg.from.id || msg.from.telegram_id); 
    // ***** END OF CORRECTION *****
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type; 
    const logPrefix = `[D21_HandleStartCmd UID:${userId} CH:${chatId} Type:${chatType}]`; // Now uses corrected userId
    console.log(`${logPrefix} Command /d21 received. Raw bet input: ${betAmountLamports}, GameModeArg (if any): ${gameModeArg}`);

    let initiatorUserObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!initiatorUserObj) {
        console.warn(`${logPrefix} Failed to get/create user object for ID: ${userId}. Cannot start game.`);
        await safeSendMessage(chatId, "Apologies, your player profile couldn't be accessed right now\\. Please use the `/start` command with me first, and then try initiating the Dice 21 game again\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(initiatorUserObj); // Pre-escaped

    // --- Game only playable in group chats ---
    if (chatType === 'private') {
        console.log(`${logPrefix} Dice 21 command used in private chat by ${playerRef}. Informing user game is group-only.`);
        await safeSendMessage(chatId, `🎲 Greetings, ${playerRef}\\!\n\nThe high-stakes Dice 21 game is exclusively available in our designated casino group chats\\. This allows for exciting Player vs Player action or challenging our Bot Dealer in a shared environment\\. \n\nTo start a game, please use the \\\`/d21 <bet>\` command within one of your casino groups\\. Good luck when you do\\!`, { parse_mode: 'MarkdownV2' });
        return; 
    }

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.log(`${logPrefix} Invalid or zero bet amount detected: ${betAmountLamports}. Informing user.`);
        await safeSendMessage(chatId, `🃏 Salutations, ${playerRef}\\! To begin a game of Dice 21, please specify a valid positive bet amount using USD or SOL\\. For example: \\\`/d21 10\\\` (for a ~$10 USD wager) or \\\`/d21 0.2 sol\\\`\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD')); // Pre-escaped
    console.log(`${logPrefix} Initiator: ${playerRef}, Bet (USD Display): ${betDisplayUSD}, Bet (Lamports): ${betAmountLamports}`);

    // Logic below is for Group Chats (Unified Offer Creation)
    const gameSession = await getGroupSession(chatId, msg.chat.title || `Group Chat ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        if ( ([GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER, GAME_IDS.DICE_21_UNIFIED_OFFER, GAME_IDS.DUEL_UNIFIED_OFFER, GAME_IDS.COINFLIP, GAME_IDS.RPS].includes(existingGame.type) && // Added DUEL_UNIFIED_OFFER
               (existingGame.status === 'pending_offer' || existingGame.status === 'waiting_opponent' || existingGame.status === 'waiting_for_choice' || existingGame.status === 'waiting_choices')) ||
             ((existingGame.type === GAME_IDS.DICE_21_PVP || existingGame.type === GAME_IDS.DICE_ESCALATOR_PVP || existingGame.type === GAME_IDS.DUEL_PVP) && !existingGame.status.startsWith('game_over_')) 
           ) {
            console.log(`${logPrefix} Another interactive game offer or active PvP game (ID: ${gameSession.currentGameId}, Type: ${existingGame.type}, Status: ${existingGame.status}) is already active.`);
            await safeSendMessage(chatId, `⏳ Please hold on, ${playerRef}\\! Another game offer (like \`${escapeMarkdownV2(existingGame.type.replace(/_/g, ' '))}\`) or an active Player vs Player match is currently underway in this group\\. Kindly wait for it to conclude before initiating a new Dice 21 challenge\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        console.log(`${logPrefix} Initiator ${playerRef} has insufficient balance for ${betDisplayUSD} bet. Needs ${needed} more lamports.`);
        await safeSendMessage(chatId, `${playerRef}, your casino balance is currently too low for a *${betDisplayUSD}* Dice 21 game\\. You require approximately *${escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'))}* more for this particular wager\\.`, {
            parse_mode: 'MarkdownV2', 
            reply_markup: { inline_keyboard: [[{ text: "💰 Top Up Balance (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const offerId = generateGameId(GAME_IDS.DICE_21_UNIFIED_OFFER);
    const offerMessageText = 
        `🎲 **Dice 21 Challenge by ${playerRef}\\!** 🎲\n\n` +
        `${playerRef} has thrown down the gauntlet for a thrilling game of Dice 21, with a hefty wager of *${betDisplayUSD}* on the line\\!\n\n` +
        `Will any brave challengers step up for a Player vs Player showdown\\?\n` +
        `Alternatively, ${playerRef} can choose to battle wits with our expert Bot Dealer\\. The choice is yours\\!`;
        
    const offerKeyboard = {
        inline_keyboard: [
            // Button text is PLAIN TEXT
            [{ text: "⚔️ Accept PvP Challenge!", callback_data: `d21_accept_pvp_challenge:${offerId}` }],
            [{ text: "🤖 Play Against the Bot Dealer", callback_data: `d21_accept_bot_game:${offerId}` }],
            [{ text: "🚫 Cancel This Offer (Initiator Only)", callback_data: `d21_cancel_unified_offer:${offerId}` }]
        ]
    };

    const offerData = {
        type: GAME_IDS.DICE_21_UNIFIED_OFFER, 
        gameId: offerId, 
        chatId: String(chatId), 
        chatType, 
        initiatorId: userId, 
        initiatorMention: playerRef, 
        initiatorUserObj, 
        betAmount: betAmountLamports,
        status: 'waiting_for_choice', 
        creationTime: Date.now(), 
        gameSetupMessageId: null 
    };
    activeGames.set(offerId, offerData);
    await updateGroupGameDetails(chatId, offerId, GAME_IDS.DICE_21_UNIFIED_OFFER, betAmountLamports);

    console.log(`${logPrefix} Sending Dice 21 unified offer (ID: ${offerId}) to chat ${chatId}.`);
    const sentOfferMessage = await safeSendMessage(chatId, offerMessageText, { parse_mode: 'MarkdownV2', reply_markup: offerKeyboard });
    
    if (sentOfferMessage?.message_id) {
        const offerInMap = activeGames.get(offerId);
        if(offerInMap) {
            offerInMap.gameSetupMessageId = sentOfferMessage.message_id;
            activeGames.set(offerId, offerInMap);
            console.log(`${logPrefix} Unified offer message successfully sent (Msg ID: ${sentOfferMessage.message_id}). Offer ID: ${offerId}.`);
        } else {
            console.warn(`${logPrefix} Offer ${offerId} vanished from activeGames immediately after message ID was set. Orphaned offer message ${sentOfferMessage.message_id} might exist in chat.`);
            if (bot) await bot.deleteMessage(chatId, sentOfferMessage.message_id).catch(delErr => console.warn(`${logPrefix} Could not delete potentially orphaned offer message ${sentOfferMessage.message_id}: ${delErr.message}`));
        }
    } else {
        console.error(`${logPrefix} CRITICAL: Failed to send Dice 21 unified offer message for offer ID ${offerId}. Cleaning up this offer attempt.`);
        activeGames.delete(offerId); 
        await updateGroupGameDetails(chatId, null, null, null); 
        await safeSendMessage(chatId, `An unexpected technical difficulty prevented the Dice 21 game offer by ${playerRef} from being created\\. Please attempt the command again\\. If the issue continues, our support team is here to help\\.`, {parse_mode: 'MarkdownV2'});
        return;
    }

    setTimeout(async () => {
        const currentOfferData = activeGames.get(offerId);
        if (currentOfferData && currentOfferData.status === 'waiting_for_choice') {
            console.log(`[D21_OfferTimeout OfferID:${offerId}] Unified Dice 21 offer has expired due to inactivity.`);
            activeGames.delete(offerId);
            await updateGroupGameDetails(chatId, null, null, null);
            
            if (currentOfferData.gameSetupMessageId && bot) {
                const expiredOfferBetDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(currentOfferData.betAmount, 'USD'));
                const offerExpiredMessageText = `⏳ The Dice 21 game offer initiated by ${currentOfferData.initiatorMention} for *${expiredOfferBetDisplayUSD}* has timed out as no option was selected\\. This offer is now closed\\.`;
                
                console.log(`${logPrefix} Editing expired offer message (ID: ${currentOfferData.gameSetupMessageId}) for offer ${offerId} due to timeout.`);
                await bot.editMessageText(offerExpiredMessageText, {
                    chat_id: String(chatId), 
                    message_id: Number(currentOfferData.gameSetupMessageId), 
                    parse_mode: 'MarkdownV2', 
                    reply_markup: {} 
                }).catch(e => console.error(`${logPrefix} Error editing message for expired D21 unified offer (ID: ${currentOfferData.gameSetupMessageId}): ${e.message}. Message was: "${offerExpiredMessageText}"`));
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

// --- Callback Handlers for Unified Dice 21 Offer ---
// Preserving original name: handleDice21AcceptBotGame
async function handleDice21AcceptBotGame(offerId, initiatorUserObjFromCb, originalOfferMessageId, originalChatId, originalChatTypeFromRouter) {
    const initiatorId = String(initiatorUserObjFromCb.id || initiatorUserObjFromCb.telegram_id);
    const logPrefix = `[D21_AcceptBotCallback GID:${offerId} UID:${initiatorId}]`;
    const offerData = activeGames.get(offerId);
    const initiatorRef = getPlayerDisplayReference(initiatorUserObjFromCb);

    if (!offerData || offerData.type !== GAME_IDS.DICE_21_UNIFIED_OFFER) {
        console.warn(`${logPrefix} ${initiatorRef} tried to accept a PvB game for an invalid or non-existent offer (ID: ${offerId}). Offer data present: ${!!offerData}`);
        const msgIdToClear = offerData?.gameSetupMessageId || originalOfferMessageId;
        if (msgIdToClear && bot) {
            await bot.editMessageText("This Dice 21 offer is no longer active or has already expired\\. Please ask for a new one if you wish to play\\.", {
                chat_id: originalChatId, message_id: Number(msgIdToClear),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(e => console.warn(`${logPrefix} Failed to edit outdated offer message ${msgIdToClear}: ${e.message}`));
        }
        // answerCallbackQuery is handled by the main callback router (Part 5a S1)
        return;
    }
    if (offerData.initiatorId !== initiatorId) {
        console.warn(`${logPrefix} User ${initiatorRef} (ID: ${initiatorId}) tried to accept PvB game for an offer made by ${offerData.initiatorMention} (ID: ${offerData.initiatorId}). This action is restricted to the offer initiator.`);
        // answerCallbackQuery in main router
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
        console.warn(`${logPrefix} Offer ${offerId} (initiator: ${offerData.initiatorMention}) is not in 'waiting_for_choice' state. Current status: ${offerData.status}. Cannot start PvB game now.`);
        if (bot && offerData.gameSetupMessageId) { 
            await bot.editMessageText(`This Dice 21 offer by ${offerData.initiatorMention} has already been actioned or has timed out\\. A new game cannot be started from this offer\\.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(e => console.warn(`${logPrefix} Minor error editing (status not waiting for choice) offer message ${offerData.gameSetupMessageId}: ${e.message}`));
        }
        // answerCallbackQuery in main router
        return;
    }

    console.log(`${logPrefix} Initiator ${offerData.initiatorMention} has selected to play against the Bot Dealer from offer ${offerId}. Proceeding to start PvB game.`);
    await startDice21PvBGame(
        originalChatId, 
        offerData.initiatorUserObj, 
        offerData.betAmount, 
        Number(offerData.gameSetupMessageId || originalOfferMessageId), 
        false, 
        offerId, 
        originalChatTypeFromRouter 
    );
}

// Preserving original name: handleDice21AcceptPvPChallenge
async function handleDice21AcceptPvPChallenge(offerId, joinerUserObjFromCb, originalOfferMessageId, originalChatId, originalChatType) {
    const joinerId = String(joinerUserObjFromCb.id || joinerUserObjFromCb.telegram_id);
    const logPrefix = `[D21_AcceptPvPCallback GID:${offerId} JoinerID:${joinerId}]`;
    let offerData = activeGames.get(offerId); 
    const joinerRef = getPlayerDisplayReference(joinerUserObjFromCb); // Pre-escaped

    if (!offerData || offerData.type !== GAME_IDS.DICE_21_UNIFIED_OFFER) {
        console.warn(`${logPrefix} PvP challenge acceptance by ${joinerRef} for an invalid or non-existent offer (ID: ${offerId}). Offer data present: ${!!offerData}`);
        const msgIdToClearOrUpdate = offerData?.gameSetupMessageId || originalOfferMessageId;
        if (msgIdToClearOrUpdate && bot) {
           await bot.editMessageText("This Dice 21 game offer has expired or is no longer available for a PvP match\\. Please ask for a new offer if you wish to play\\.", { 
                chat_id: originalChatId, message_id: Number(msgIdToClearOrUpdate), 
                parse_mode: 'MarkdownV2', reply_markup: {} 
            }).catch(e => console.warn(`${logPrefix} Failed to edit/clear old PvP offer message (ID: ${msgIdToClearOrUpdate}): ${e.message}`));
        }
        return;
    }

    if (offerData.initiatorId === joinerId) {
        console.warn(`${logPrefix} Initiator ${joinerRef} attempted to accept their own PvP challenge for offer ${offerId}. Action denied.`);
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
        console.warn(`${logPrefix} Offer ${offerId} (initiator: ${offerData.initiatorMention}) is not in 'waiting_for_choice' state (current: ${offerData.status}). ${joinerRef} cannot join for PvP now.`);
        if (offerData.gameSetupMessageId && bot) { 
            await bot.editMessageText(`This Dice 21 offer by ${offerData.initiatorMention} is no longer available to be joined for a PvP match\\. It may have been started as a Bot game, cancelled, or has already been accepted by another player\\.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(e => console.warn(`${logPrefix} Minor error editing (not waiting for choice) PvP offer message ${offerData.gameSetupMessageId}: ${e.message}`));
        }
        return;
    }

    console.log(`${logPrefix} Player ${joinerRef} is accepting the PvP challenge from ${offerData.initiatorMention} (Offer ID: ${offerId}). Verifying funds for both players.`);
    const betAmount = offerData.betAmount;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD')); // Pre-escaped

    let currentJoinerUserObj = await getOrCreateUser(joinerId, joinerUserObjFromCb.username, joinerUserObjFromCb.first_name, joinerUserObjFromCb.last_name);
    if (!currentJoinerUserObj) {
        console.warn(`${logPrefix} Could not get/create user object for joiner ${joinerRef}.`);
        await safeSendMessage(originalChatId, `An error occurred fetching the player profile for ${joinerRef} to join the Dice 21 PvP game\\. Please ensure they have used the \`/start\` command with me at least once before trying to join games\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    if (BigInt(currentJoinerUserObj.balance) < betAmount) {
        console.log(`${logPrefix} Joiner ${joinerRef} has insufficient balance for PvP game. Needs ${betAmount}, has ${currentJoinerUserObj.balance}.`);
        await safeSendMessage(originalChatId, `${joinerRef}, your current casino balance is insufficient to join this *${betDisplayUSD}* Dice 21 PvP game\\. Please top up your funds if you wish to accept the challenge\\.`, { 
            parse_mode: 'MarkdownV2', 
            reply_markup: { inline_keyboard: [[{ text: "💰 Top Up Balance (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] } 
        });
        return;
    }

    let currentInitiatorUserObj = await getOrCreateUser(offerData.initiatorId); 
    if (!currentInitiatorUserObj || BigInt(currentInitiatorUserObj.balance) < betAmount) {
        console.warn(`${logPrefix} Initiator ${offerData.initiatorMention} no longer has sufficient funds for the PvP wager (${betAmount}). Cancelling offer ${offerId}.`);
        const offerMessageIdToUpdateOrDelete = offerData.gameSetupMessageId || originalOfferMessageId;
        const cancelTextDueToInitiatorFunds = `The Dice 21 PvP offer from ${offerData.initiatorMention} (for *${betDisplayUSD}*) has been automatically cancelled\\. The initiator, ${offerData.initiatorMention}, no longer has sufficient funds to cover the wager\\. A new offer can be made once funds are available\\.`;
        
        if (offerMessageIdToUpdateOrDelete && bot) {
           await bot.editMessageText(cancelTextDueToInitiatorFunds, { 
                chat_id: originalChatId, message_id: Number(offerMessageIdToUpdateOrDelete), 
                parse_mode: 'MarkdownV2', reply_markup: {} 
            }).catch(async (e) => { 
                console.warn(`${logPrefix} Failed to edit PvP offer cancellation msg (ID: ${offerMessageIdToUpdateOrDelete}) for initiator funds: ${e.message}. Sending new message.`);
                await safeSendMessage(originalChatId, cancelTextDueToInitiatorFunds, {parse_mode: 'MarkdownV2'});
            });
        } else { 
             await safeSendMessage(originalChatId, cancelTextDueToInitiatorFunds, { parse_mode: 'MarkdownV2'});
        }
        activeGames.delete(offerId); 
        await updateGroupGameDetails(originalChatId, null, null, null); 
        return;
    }

    const offerMessageIdToDelete = offerData.gameSetupMessageId || originalOfferMessageId;
    if (offerMessageIdToDelete && bot) {
        console.log(`${logPrefix} Deleting original offer message (ID: ${offerMessageIdToDelete}) as PvP game (new GID pending) is starting.`);
        await bot.deleteMessage(originalChatId, Number(offerMessageIdToDelete))
            .catch(e => console.warn(`${logPrefix} Non-critical: Could not delete unified offer message (ID: ${offerMessageIdToDelete}) when starting PvP game: ${e.message}`));
    }
    
    let client;
    const pvpGameId = generateGameId(GAME_IDS.DICE_21_PVP); 

    try {
        client = await pool.connect(); await client.query('BEGIN');
        console.log(`${logPrefix} Deducting bet ${betAmount} from initiator ${offerData.initiatorMention} for PvP game ${pvpGameId}.`);
        const initBetRes = await updateUserBalanceAndLedger(client, offerData.initiatorId, BigInt(-betAmount), 
            'bet_placed_dice21_pvp_init', { game_id_custom_field: pvpGameId, opponent_id: joinerId }, 
            `Initiator bet for PvP Dice 21 game ${pvpGameId} against ${joinerRef}`);
        if (!initBetRes.success) throw new Error(`Initiator (${offerData.initiatorMention}) bet placement failed: ${escapeMarkdownV2(initBetRes.error || 'Database transaction error during bet placement')}`);
        currentInitiatorUserObj.balance = initBetRes.newBalanceLamports;

        console.log(`${logPrefix} Deducting bet ${betAmount} from joiner ${joinerRef} for PvP game ${pvpGameId}.`);
        const joinBetRes = await updateUserBalanceAndLedger(client, joinerId, BigInt(-betAmount), 
            'bet_placed_dice21_pvp_join', { game_id_custom_field: pvpGameId, opponent_id: offerData.initiatorId }, 
            `Joiner bet for PvP Dice 21 game ${pvpGameId} against ${offerData.initiatorMention}`);
        if (!joinBetRes.success) throw new Error(`Joiner (${joinerRef}) bet placement failed: ${escapeMarkdownV2(joinBetRes.error || 'Database transaction error during bet placement')}`);
        currentJoinerUserObj.balance = joinBetRes.newBalanceLamports;
        
        await client.query('COMMIT');
        console.log(`${logPrefix} Bets successfully placed for PvP game ${pvpGameId}. Initiator bal: ${currentInitiatorUserObj.balance}, Joiner bal: ${currentJoinerUserObj.balance}.`);

        const pvpGameData = {
            type: GAME_IDS.DICE_21_PVP, 
            gameId: pvpGameId, 
            chatId: String(offerData.chatId), 
            chatType: offerData.chatType, 
            betAmount: offerData.betAmount,
            initiator: { 
                userId: offerData.initiatorId, mention: offerData.initiatorMention, userObj: currentInitiatorUserObj, 
                hand: [], score: 0, status: 'waiting_for_hand', isTurn: false 
            },
            opponent: { 
                userId: joinerId, mention: joinerRef, userObj: currentJoinerUserObj, 
                hand: [], score: 0, status: 'waiting_for_hand', isTurn: false 
            },
            status: 'dealing_initial_hands', 
            creationTime: Date.now(), 
            currentMessageId: null 
        };
        activeGames.set(pvpGameId, pvpGameData);      
        activeGames.delete(offerId);                       
        await updateGroupGameDetails(originalChatId, pvpGameId, GAME_IDS.DICE_21_PVP, betAmount);

        console.log(`${logPrefix} PvP Dice 21 game ${pvpGameId} data object created. Initiating initial deal sequence.`);
        await startDice21PvPInitialDeal(pvpGameId); 

    } catch (e) { 
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} PvP Accept DB Rollback Exception (outer catch): ${rbErr.message}`));
        console.error(`${logPrefix} CRITICAL error creating PvP game ${pvpGameId} from offer ${offerId}: ${e.message}. Stack: ${e.stack ? e.stack.substring(0,600) : 'No stack available'}`);
        
        const displayErrorForUser = e.message.includes('\\') ? e.message : escapeMarkdownV2(e.message);
        await safeSendMessage(originalChatId, `A critical server error occurred while starting the Dice 21 PvP game: \`${displayErrorForUser}\`\\. The game could not be created\\. Please try creating a new offer if you wish to play\\.`, { parse_mode: 'MarkdownV2'});
        
        activeGames.delete(offerId); 
        if (activeGames.has(pvpGameId)) activeGames.delete(pvpGameId);
        await updateGroupGameDetails(originalChatId, null, null, null); 
        
        if (typeof notifyAdmin === 'function') {
            const adminErrorNotification = 
                `🚨 D21 PvP Game Start Failure - POTENTIAL FUNDING ISSUE 🚨\n` +
                `Attempted Game ID: \`${pvpGameId}\` (from Offer ID: \`${offerId}\`)\n` +
                `Error: \`${displayErrorForUser}\`\n` +
                `Players: ${offerData.initiatorMention} & ${joinerRef}\\.\n` +
                `This error occurred after bet placement was attempted\\. Bets might have been deducted without the game starting\\. ` +
                `MANUAL VERIFICATION OF BALANCES AND POTENTIAL REFUNDS REQUIRED for both players\\.`;
            notifyAdmin(adminErrorNotification, {parse_mode:'MarkdownV2'});
        }
        return;
    } finally { 
        if (client) client.release(); 
    }
}

// Preserving original name: handleDice21CancelUnifiedOffer
async function handleDice21CancelUnifiedOffer(offerId, initiatorUserObjFromCb, originalOfferMessageId, originalChatId) {
    const initiatorId = String(initiatorUserObjFromCb.id || initiatorUserObjFromCb.telegram_id);
    const logPrefix = `[D21_CancelOfferCallback GID:${offerId} UID:${initiatorId}]`;
    const offerData = activeGames.get(offerId);
    const initiatorRef = getPlayerDisplayReference(initiatorUserObjFromCb); 

    if (!offerData || offerData.type !== GAME_IDS.DICE_21_UNIFIED_OFFER) {
        console.warn(`${logPrefix} ${initiatorRef} attempted to cancel an invalid or non-existent offer (ID: ${offerId}). Current offer data:`, stringifyWithBigInt(offerData).substring(0,200));
        const msgToClear = offerData?.gameSetupMessageId || originalOfferMessageId;
        if (msgToClear && bot) { 
            await bot.editMessageReplyMarkup({}, {chat_id: originalChatId, message_id: Number(msgToClear)})
                .catch(e => console.warn(`${logPrefix} Failed to remove buttons from old/invalid offer message ID ${msgToClear} on cancel attempt: ${e.message}`));
        }
        // Main callback router handles answering the callbackQuery.
        return;
    }
    if (offerData.initiatorId !== initiatorId) {
        console.warn(`${logPrefix} User ${initiatorRef} (ID: ${initiatorId}) wrongfully tried to cancel an offer made by ${offerData.initiatorMention} (ID: ${offerData.initiatorId}). Action denied.`);
        // Main callback router handles answering the callbackQuery.
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
        console.warn(`${logPrefix} Offer ${offerId} by ${offerData.initiatorMention} is no longer 'waiting_for_choice' (current: ${offerData.status}). It cannot be cancelled by the initiator now.`);
        if (offerData.gameSetupMessageId && bot) { 
            await bot.editMessageText(`This Dice 21 offer by ${offerData.initiatorMention} is no longer pending cancellation as it has likely been actioned or has timed out\\.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(e => console.warn(`${logPrefix} Minor error editing (status not waiting) offer message ${offerData.gameSetupMessageId} during cancel attempt: ${e.message}`));
        }
        // Main callback router handles answering the callbackQuery.
        return;
    }

    console.log(`${logPrefix} Initiator ${offerData.initiatorMention} is successfully cancelling their Dice 21 offer (ID: ${offerId}).`);
    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null); 

    const msgIdToEdit = offerData.gameSetupMessageId || originalOfferMessageId; 
    if (msgIdToEdit && bot) {
        const cancelMessageText = `🚫 The Dice 21 game offer initiated by ${offerData.initiatorMention} has been cancelled by the initiator\\. No game will be played from this offer\\.`;
        console.log(`${logPrefix} Editing offer message (ID: ${msgIdToEdit}) to show cancellation text.`);
        await bot.editMessageText(cancelMessageText, {
            chat_id: String(originalChatId), 
            message_id: Number(msgIdToEdit), 
            parse_mode: 'MarkdownV2', 
            reply_markup: {} 
        }).catch(e => console.error(`${logPrefix} Error editing message for cancelled D21 unified offer (ID: ${msgIdToEdit}): ${e.message}. Message was: "${cancelMessageText}"`));
    } else {
        console.warn(`${logPrefix} No valid message ID found to edit for cancelled offer ${offerId}. Initiator: ${offerData.initiatorMention}. A new message was not sent to avoid potential chat clutter.`);
    }
}


// --- Player vs. Bot (PvB) Dice 21 Game Logic ---
// Preserving original name: startDice21PvBGame
// This version incorporates the "delete and resend" for its main UI messages, "small box" announcements for initial dice, and uses chatTypeFromCaller.
async function startDice21PvBGame(chatId, initiatorUserObj, betAmountLamports, originalCmdOrOfferMsgId, isPrivateChatStart = false, unifiedOfferIdIfAny = null, chatTypeFromCaller) {
    const userId = String(initiatorUserObj.telegram_id); // Ensure using telegram_id from DB object
    const currentChatType = chatTypeFromCaller || (isPrivateChatStart ? 'private' : 'group'); 
    const logPrefix = `[D21_PvB_Start UID:${userId} CH:${chatId} ActualChatType:${currentChatType}]`;
    
    const playerRef = getPlayerDisplayReference(initiatorUserObj); 
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    console.log(`${logPrefix} Initiating PvB game. playerRef="${playerRef}", betDisplayUSD="${betDisplayUSD}", isPrivate=${isPrivateChatStart}, offerId=${unifiedOfferIdIfAny}, originalMsgIdToHandle=${originalCmdOrOfferMsgId}, currentChatType=${currentChatType}`);

    if (unifiedOfferIdIfAny && originalCmdOrOfferMsgId && bot && currentChatType !== 'private') { 
        console.log(`${logPrefix} Deleting unified offer message (ID: ${originalCmdOrOfferMsgId}) as PvB game starts from group offer.`);
        await bot.deleteMessage(chatId, Number(originalCmdOrOfferMsgId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Could not delete unified offer message ${originalCmdOrOfferMsgId} for new PvB game: ${e.message}`));
    } else if (isPrivateChatStart && originalCmdOrOfferMsgId && bot) {
        console.log(`${logPrefix} PvB started in private chat. User's command (ID: ${originalCmdOrOfferMsgId}) is typically not deleted by the bot.`);
    }

    let initialMainGameMessageText = 
        `🃏 ${playerRef} is starting a Dice 21 game vs the Bot Dealer for *${betDisplayUSD}*\\!\n\n` +
        `The Helper Bot is now dealing your initial two dice\\. Please wait a moment for the reveal\\.\\.\\. Good luck\\! ⏳\n\n` +
        `_(After your turn is complete, the Bot Dealer will play its hand following standard casino rules)_`;
    
    console.log(`${logPrefix} Constructing initial PvB main game message (loadingText). Snippet: "${initialMainGameMessageText.substring(0,150)}..."`);

    const sentMainGameMessage = await safeSendMessage(chatId, initialMainGameMessageText, { parse_mode: 'MarkdownV2' });
    const gameUiMessageId = sentMainGameMessage?.message_id;

    if (!gameUiMessageId) {
        console.error(`${logPrefix} CRITICAL: Failed to send initial main game message for PvB game. Game cannot proceed.`);
        if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) { 
            activeGames.delete(unifiedOfferIdIfAny);
        }
        await safeSendMessage(chatId, `Sorry, ${playerRef}, a display error occurred while attempting to start your Dice 21 game\\. Please try the command again in a few moments\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${logPrefix} Initial PvB main game message sent (Message ID: ${gameUiMessageId}). Storing this ID for edits during deal, then will be deleted.`);

    let client = null;
    const gameIdForActivePvB = generateGameId(GAME_IDS.DICE_21); 

    const gameDataPvB = {
        type: GAME_IDS.DICE_21, gameId: gameIdForActivePvB, chatId: String(chatId), 
        chatType: currentChatType, 
        playerId: userId, playerRef, userObj: initiatorUserObj, betAmount: betAmountLamports,
        playerScore: 0, botScore: 0, playerHandRolls: [], botHandRolls: [],
        status: 'player_dealing_initial_hand', 
        gameMessageId: gameUiMessageId, 
        lastInteractionTime: Date.now()
    };
    activeGames.set(gameIdForActivePvB, gameDataPvB); 
    console.log(`${logPrefix} PvB game ${gameIdForActivePvB} initialized with message ID ${gameUiMessageId}.`);

    try {
        client = await pool.connect(); await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, BigInt(-betAmountLamports), 
            'bet_placed_dice21_pvb', 
            { game_id_custom_field: gameIdForActivePvB }, 
            `Bet placed for PvB Dice 21 game ${gameIdForActivePvB}`
        );
        
        if (!balanceUpdateResult.success) {
            throw new Error(balanceUpdateResult.error || "Database error: PvB Dice 21 wager placement failed.");
        }
        
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports; 
        gameDataPvB.userObj = initiatorUserObj; 
        activeGames.set(gameIdForActivePvB, gameDataPvB); 
        await client.query('COMMIT');
        console.log(`${logPrefix} Bet of ${betAmountLamports} successfully placed for ${playerRef}. New balance: ${initiatorUserObj.balance}.`);

        if (currentChatType !== 'private') { 
            await updateGroupGameDetails(chatId, gameIdForActivePvB, GAME_IDS.DICE_21, betAmountLamports);
        }
        
        if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) { 
            console.log(`${logPrefix} Deleting original unified offer (ID: ${unifiedOfferIdIfAny}) as PvB game ${gameIdForActivePvB} has started and bet is placed.`);
            activeGames.delete(unifiedOfferIdIfAny);
        }

        let initialPlayerHandDealt = [];
        let initialPlayerScoreCalculated = 0;
        let playerDealErrorReason = null; 

        let dealingInProgressMessageText = 
            `🃏 ${playerRef} vs Bot - Bet: *${betDisplayUSD}*\n\n` +
            `The Helper Bot is dealing your first die now\\.\\.\\.`;
        if (bot) await bot.editMessageText(dealingInProgressMessageText, {chat_id:String(chatId), message_id:Number(gameUiMessageId), parse_mode:'MarkdownV2'})
            .catch(e=>console.warn(`${logPrefix} Minor edit fail for PvB deal message update (first die): ${e.message}. Content: "${dealingInProgressMessageText.substring(0,100)}..."`));

        for (let i = 0; i < 2; i++) { 
            console.log(`${logPrefix} Requesting Die ${i+1}/2 for player's initial hand from Helper Bot for game ${gameIdForActivePvB}.`);
            const dieRollResultObject = await getSingleDiceRollViaHelper(gameIdForActivePvB, chatId, userId, `Player Initial D21 PvB Die ${i + 1}`);
            
            if (dieRollResultObject.error) { 
                playerDealErrorReason = dieRollResultObject.message || `Failed to get die ${i + 1} for your initial hand from the Helper Bot\\.`;
                console.error(`${logPrefix} Helper Bot error for Die ${i+1}: ${playerDealErrorReason}`);
                break; 
            }
            
            const rollValue = dieRollResultObject.roll;
            initialPlayerHandDealt.push(rollValue);
            initialPlayerScoreCalculated += rollValue;
            
            const dieMessageText = `🎲 ${playerRef} received a *${escapeMarkdownV2(String(rollValue))}* for Dice 21 (Die ${i+1}/2 from the Helper Bot)\\.`;
            await safeSendMessage(chatId, dieMessageText, {parse_mode: 'MarkdownV2'}); 
            console.log(`${logPrefix} Sent "small box" announcement for player's Die ${i+1} (Value: ${rollValue}).`);
            
            if (i < 1) { 
                dealingInProgressMessageText = 
                    `🃏 ${playerRef} vs Bot - Bet: *${betDisplayUSD}*\n\n` +
                    `Your first die from the Helper Bot: ${formatDiceRolls([initialPlayerHandDealt[0]])}\n`+
                    `Now dealing your second die\\.\\.\\.`;
                if (bot) await bot.editMessageText(dealingInProgressMessageText, {chat_id:String(chatId), message_id:Number(gameUiMessageId), parse_mode:'MarkdownV2'})
                    .catch(e=>console.warn(`${logPrefix} Minor edit fail showing first die dealt: ${e.message}. Content: "${dealingInProgressMessageText.substring(0,100)}..."`));
                await sleep(1200); 
            } else {
                await sleep(750); 
            }
        }
        
        if (playerDealErrorReason) {
            throw new Error(playerDealErrorReason); 
        }

        gameDataPvB.playerHandRolls = initialPlayerHandDealt;
        gameDataPvB.playerScore = initialPlayerScoreCalculated;
        gameDataPvB.lastInteractionTime = Date.now();
        activeGames.set(gameIdForActivePvB, gameDataPvB); 
        console.log(`${logPrefix} Player's initial hand successfully dealt for GID ${gameIdForActivePvB}. Hand: [${initialPlayerHandDealt.join(', ')}], Score: ${initialPlayerScoreCalculated}.`);

        if (gameUiMessageId && bot) {
            console.log(`${logPrefix} Deleting 'Dealing...' message (ID: ${gameUiMessageId}) before sending player's turn message.`);
            await bot.deleteMessage(chatId, Number(gameUiMessageId))
                .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete 'Dealing...' message ${gameUiMessageId} before sending player's turn UI: ${e.message}`));
        }
        
        let newMainMessage_Header = `🃏 **Dice 21 vs Bot**\nPlayer: ${playerRef}\nBet: *${betDisplayUSD}*\n\n`;
        let newMainMessage_HandDetails = `Your Initial Hand: ${formatDiceRolls(gameDataPvB.playerHandRolls)}\nYour Score: *${escapeMarkdownV2(String(gameDataPvB.playerScore))}*\n\n`;
        let newMainMessage_ActionPrompt = "";
        let finalPlayerTurnMessageText = "";
        let keyboardForPlayerTurn = {}; 

        if (gameDataPvB.playerScore > DICE_21_TARGET_SCORE) {
            newMainMessage_ActionPrompt = `💥 Oh no\\! You BUSTED on the deal with *${escapeMarkdownV2(String(gameDataPvB.playerScore))}* (target is ${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))})\\.\nThe Bot Dealer wins this round\\.`;
            gameDataPvB.status = 'game_over_player_bust';
        } else if (gameDataPvB.playerScore === DICE_21_TARGET_SCORE) {
            newMainMessage_ActionPrompt = `✨ Blackjack\\! A perfect *${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}* on your initial hand\\! You automatically stand\\.\n\nThe Bot Dealer will now play its turn\\.\\.\\.`;
            gameDataPvB.status = 'bot_turn';
        } else {
            newMainMessage_ActionPrompt = `It's your turn, ${playerRef}\\! To Hit for another die, please send a 🎲 emoji\\. To Stand with your current score, tap the button below\\.`;
            gameDataPvB.status = 'player_turn_awaiting_emoji';
            keyboardForPlayerTurn = {
                inline_keyboard: [
                    [{ text: `✅ Stand (Score: ${gameDataPvB.playerScore})`, callback_data: `d21_stand:${gameIdForActivePvB}` }],
                    [{ text: "🚫 Forfeit Game", callback_data: `d21_pvb_cancel:${gameIdForActivePvB}` }],
                    [{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` }]
                ]
            };
        }
        
        finalPlayerTurnMessageText = newMainMessage_Header + newMainMessage_HandDetails + newMainMessage_ActionPrompt;
        activeGames.set(gameIdForActivePvB, gameDataPvB); 

        console.log(`${logPrefix} Sending new main game message for player's turn. Status: ${gameDataPvB.status}. Text snippet: "${finalPlayerTurnMessageText.substring(0,100)}..."`);
        const sentNewPlayerTurnMessage = await safeSendMessage(chatId, finalPlayerTurnMessageText, { 
            parse_mode: 'MarkdownV2', 
            reply_markup: Object.keys(keyboardForPlayerTurn).length > 0 ? keyboardForPlayerTurn : {} 
        });

        if (sentNewPlayerTurnMessage?.message_id) {
            gameDataPvB.gameMessageId = sentNewPlayerTurnMessage.message_id; 
            activeGames.set(gameIdForActivePvB, gameDataPvB);
            console.log(`${logPrefix} Player's turn message sent successfully (New Main Msg ID: ${gameDataPvB.gameMessageId}).`);
        } else {
            console.error(`${logPrefix} CRITICAL: Failed to send player's turn message for GID ${gameIdForActivePvB}. Game UI is broken.`);
            gameDataPvB.status = 'game_over_error_ui_update'; 
            activeGames.set(gameIdForActivePvB, gameDataPvB);
            await finalizeDice21PvBGame(gameDataPvB); 
            return;
        }

        if (gameDataPvB.status === 'game_over_player_bust') {
            console.log(`${logPrefix} Player busted on deal for GID ${gameDataPvB.gameId}. Finalizing game.`);
            await finalizeDice21PvBGame(gameDataPvB);
        } else if (gameDataPvB.status === 'bot_turn') {
            console.log(`${logPrefix} Player got Blackjack/21 on deal for GID ${gameDataPvB.gameId}. Proceeding to Bot's turn.`);
            await sleep(2000); 
            await processDice21BotTurn(gameDataPvB); 
        }

    } catch (error) { 
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} PvB Start DB Rollback Exception (Main Catch Block): ${rbErr.message}`));
        
        const rawErrorMessageFromCaughtError = error.message || "An unknown error occurred while starting your Dice 21 game.";
        console.error(`${logPrefix} Main catch block in startDice21PvBGame for GID ${gameIdForActivePvB || 'UNKNOWN_GID'}. Error: ${rawErrorMessageFromCaughtError}. Stack: ${error.stack ? error.stack.substring(0, 800) : 'N/A'}`);
        
        const displayableErrorContentForUser = "An unexpected issue occurred with the game server operations\\! Our technical team has been alerted\\. Please try again later\\."; 
        
        let finalUserErrorMessageText = `⚙️ **Game Initialization Error**\n\n${playerRef}, we encountered a problem while setting up your Dice 21 game for *${betDisplayUSD}*\\.\n\nDetails: ${displayableErrorContentForUser}`;

        let refundAttemptedInCatch = false;
        let refundSuccessfulInCatch = false;
        let refundSpecificErrorDetail = null; 

        if (gameIdForActivePvB) { 
            refundAttemptedInCatch = true;
            let refundCatchClient = null;
            try {
                refundCatchClient = await pool.connect(); await refundCatchClient.query('BEGIN');
                console.log(`${logPrefix} Attempting refund of ${betAmountLamports} to ${userId} due to critical error in game start: ${rawErrorMessageFromCaughtError.substring(0,100)}`);
                const refundOpDetails = await updateUserBalanceAndLedger(
                    refundCatchClient, userId, betAmountLamports, 
                    'refund_d21_pvb_start_error_critical', 
                    { game_id_custom_field: gameIdForActivePvB || 'N/A_GID_CRIT_REFUND_MAIN' }, 
                    `Critical refund for failed PvB D21 start. Error: ${rawErrorMessageFromCaughtError.substring(0,100)}`
                );
                if (refundOpDetails.success) {
                    await refundCatchClient.query('COMMIT');
                    refundSuccessfulInCatch = true;
                    console.log(`${logPrefix} Refund successful for ${userId} in main catch block.`);
                } else {
                    await refundCatchClient.query('ROLLBACK');
                    refundSpecificErrorDetail = refundOpDetails.error || "Unknown refund ledger update error during critical error handling.";
                    console.error(`${logPrefix} Refund ledger op FAILED for user ${userId} in main catch block: ${refundSpecificErrorDetail}`);
                }
            } catch (dbExceptionDuringCatchRefund) {
                if (refundCatchClient) await refundCatchClient.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Refund DB Rollback Exception in CATCH (Main): ${rbErr.message}`));
                refundSpecificErrorDetail = dbExceptionDuringCatchRefund.message || "DB exception during critical refund attempt in main catch.";
                console.error(`${logPrefix} CRITICAL: DB Exception during REFUND ATTEMPT in main catch for user ${userId}: ${refundSpecificErrorDetail}`);
            } finally {
                if (refundCatchClient) refundCatchClient.release();
            }
        }

        if (refundAttemptedInCatch) {
            if (refundSuccessfulInCatch) {
                finalUserErrorMessageText += `\n\nYour wager of *${betDisplayUSD}* has been successfully refunded to your account due to this issue\\. We recommend trying to start a new game shortly\\.`;
            } else {
                const escapedRefundErrorForUserDisplay = escapeMarkdownV2(refundSpecificErrorDetail || "The refund could not be processed automatically by the system at this time."); 
                finalUserErrorMessageText += `\n\nYour wager of *${betDisplayUSD}* may have been taken\\. An issue occurred during the automated refund process: \`${escapedRefundErrorForUserDisplay}\` Please contact our support team immediately for assistance with Game ID details if possible, or by describing this error\\.`;
            }
        } else { 
            finalUserErrorMessageText += `\n\nPlease try starting a new game in a few moments\\. If this issue continues, please reach out to our support team\\.`;
        }
        
        if (refundAttemptedInCatch && !refundSuccessfulInCatch) {
            if(typeof notifyAdmin === 'function') {
                const adminDisplayableError = escapeMarkdownV2(rawErrorMessageFromCaughtError); 
                const adminDisplayableRefundError = escapeMarkdownV2(refundSpecificErrorDetail || "N/A");
                const adminAlertMessage = 
                    `🚨 D21 PvB Start Critical Error \\+ REFUND FAILED 🚨\n` +
                    `User: ${playerRef} (\`${escapeMarkdownV2(String(userId))}\`), Bet: ${betDisplayUSD}\n` +
                    `Game ID Attempted: \`${gameIdForActivePvB || 'Unknown_GID'}\`\n` +
                    `Initial Error: ${adminDisplayableError}\n` +
                    `Refund Attempt Error: \`${adminDisplayableRefundError}\`\n` +
                    `MANUAL VERIFICATION OF BALANCE AND REFUND IS URGENTLY REQUIRED FOR THIS USER\\.`;
                notifyAdmin(adminAlertMessage, {parse_mode:'MarkdownV2'});
            }
        }

        console.log(`${logPrefix} Debug Catch (Main): Final error message to send to user = "${finalUserErrorMessageText.substring(0,200)}..."`);
        const replyMarkupForErrorMsg = { inline_keyboard: [[{ text: "Dismiss Message", callback_data: "noop_ok" }]] }; 
        
        const messageIdToDeleteBeforeErrorMsg = gameDataPvB?.gameMessageId || gameUiMessageId; 
        if (messageIdToDeleteBeforeErrorMsg && bot) {
            console.log(`${logPrefix} Attempting to delete message (ID: ${messageIdToDeleteBeforeErrorMsg}) before sending final error details.`);
            await bot.deleteMessage(chatId, Number(messageIdToDeleteBeforeErrorMsg))
                .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete message ${messageIdToDeleteBeforeErrorMsg} before sending error: ${e.message}`));
        }
        
        await safeSendMessage(chatId, finalUserErrorMessageText, { parse_mode: 'MarkdownV2', reply_markup: replyMarkupForErrorMsg });
        
        if (gameIdForActivePvB && activeGames.has(gameIdForActivePvB)) {
            activeGames.delete(gameIdForActivePvB);
            console.log(`${logPrefix} Removed active game ${gameIdForActivePvB} after error in startDice21PvBGame.`);
        }
        if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) { 
            activeGames.delete(unifiedOfferIdIfAny);
            console.log(`${logPrefix} Removed related unified offer ${unifiedOfferIdIfAny} after error in startDice21PvBGame.`);
        }
        await updateGroupGameDetails(chatId, null, null, null); 
        return; 
    } finally { 
        if (client) client.release();
    }
}

// END OF SEGMENT 1 of 2 for "Part 5b, Section 2"
// --- Start of Part 5b, Section 2 (COMPLETE REWRITE V5.1 - Streamlined Bot Turn & Score Display) ---
// index.js - Part 5b, Section 2 (Dice 21 / Blackjack-style game logic)
// SEGMENT 2 of 2
//-------------------------------------------------------------------------------------------------
// (Continues from Segment 1 of 2)
// Assumed constants and utility functions from other parts of your code are available.

// --- Player vs. Bot (PvB) Dice 21 Gameplay Logic (Continued) ---

// Preserving original name: handleDice21PvBCancel
async function handleDice21PvBCancel(gameId, userObj, originalMessageId, callbackQueryId, chatData) {
    const playerId = String(userObj.id || userObj.telegram_id);
    const chatId = String(chatData.id);
    const gameData = activeGames.get(gameId);
    const logPrefix = `[D21_PvBCancel GID:${gameId} UID:${playerId}]`;
    const playerRef = getPlayerDisplayReference(userObj); // Pre-escaped

    if (!gameData || gameData.type !== GAME_IDS.DICE_21 || gameData.playerId !== playerId) {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ This Dice 21 game is not currently active or it doesn't belong to you.", show_alert: true }).catch(()=>{});
        console.warn(`${logPrefix} Invalid forfeit attempt by ${playerRef}. GameData found: ${!!gameData}, Type: ${gameData?.type}, Expected Player: ${gameData?.playerId}, Actual Player: ${playerId}`);
        return;
    }

    if (gameData.status.startsWith('game_over') || gameData.status === 'bot_rolling' || gameData.status === 'bot_turn' || gameData.status === 'finalizing') {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Too late to forfeit! The game is already resolving or the Bot Dealer is in the middle of its turn.", show_alert: true }).catch(()=>{});
        console.log(`${logPrefix} Forfeit attempt by ${playerRef} denied. Game status: ${gameData.status}`);
        return;
    }
    
    if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "Processing your game forfeit. One moment, please..."}).catch(()=>{});

    console.log(`${logPrefix} Player ${playerRef} is forfeiting PvB Dice 21 game ${gameId}. Current player score (if any): ${gameData.playerScore}.`);
    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const forfeitLogResult = await updateUserBalanceAndLedger(client, playerId, 0n, 
            'loss_dice21_pvb_forfeit', 
            { game_id_custom_field: gameId, final_player_score_before_forfeit: gameData.playerScore, bet_amount: gameData.betAmount }, 
            `Player ${playerRef} forfeited PvB Dice 21 game ${gameId}. Wager of ${gameData.betAmount} lamports is lost.`
        );
        if (!forfeitLogResult.success) {
             console.error(`${logPrefix} DB Error occurred while logging forfeit outcome (no balance change was made by this call): ${forfeitLogResult.error}. This is an accounting/logging concern.`);
        }
        await client.query('COMMIT');
        console.log(`${logPrefix} Forfeit successfully logged in the database for game ${gameId}.`);
        
        const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD')); 
        const forfeitUserMessageText = 
            `🚫 ${playerRef} has chosen to forfeit the Dice 21 game against the Bot Dealer\\. \n\n` +
            `Your wager of *${betDisplayUSD}* has been collected by the house\\. Better luck next time around\\!`;
        
        const oldGameMessageIdToDelete = gameData.gameMessageId;
        if (oldGameMessageIdToDelete && bot) {
            console.log(`${logPrefix} Deleting old main game message (ID: ${oldGameMessageIdToDelete}) on forfeit.`);
            await bot.deleteMessage(chatId, Number(oldGameMessageIdToDelete))
                .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete old game message ${oldGameMessageIdToDelete} on forfeit: ${e.message}`));
        }

        console.log(`${logPrefix} Sending forfeit confirmation message to chat ${chatId}.`);
        // Ensure createPostGameKeyboard uses PLAIN TEXT for button labels.
        const finalMsg = await safeSendMessage(chatId, forfeitUserMessageText, { 
            parse_mode: 'MarkdownV2', 
            reply_markup: createPostGameKeyboard(GAME_IDS.DICE_21, gameData.betAmount) 
        });
        if (!finalMsg?.message_id) {
            console.error(`${logPrefix} Failed to send forfeit confirmation message for GID ${gameId}.`);
        } else {
             console.log(`${logPrefix} Forfeit confirmation message sent (ID: ${finalMsg.message_id}).`);
        }
        
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch((rbErr) => {console.error(`${logPrefix} DB Rollback Exception on forfeit: ${rbErr.message}`)});
        console.error(`${logPrefix} CRITICAL DB Error processing forfeit for game ${gameId}: ${e.message}. Stack: ${e.stack ? e.stack.substring(0,500) : 'N/A'}`);
        await safeSendMessage(playerId, `An unexpected server error occurred while processing your forfeit for the Dice 21 game, ${playerRef}\\. Please contact support if you notice any issues with your balance or game history\\. We apologize for the inconvenience\\.`, { parse_mode: 'MarkdownV2' });
        if(typeof notifyAdmin === 'function') {
            notifyAdmin(`🚨 D21 PvB Forfeit Processing DB Error 🚨\nGame ID: \`${gameId}\`, User: ${playerRef} (\`${escapeMarkdownV2(String(playerId))}\`)\nError: \`${escapeMarkdownV2(e.message)}\`\\. Manual check of game state and ledger may be needed\\.`, {parse_mode:'MarkdownV2'});
        }
    } finally { if (client) client.release(); }

    activeGames.delete(gameId); 
    if (gameData.chatType !== 'private' && chatId) { 
        await updateGroupGameDetails(chatId, null, null, null);
    }
    console.log(`${logPrefix} Game ${gameId} (forfeited by player) removed from active games and group session (if applicable).`);
}

// Preserving original name: processDice21PvBRollByEmoji
async function processDice21PvBRollByEmoji(gameData, diceValueRolledByPlayer) { 
    const logPrefix = `[D21_PvB_Hit GID:${gameData.gameId} UID:${gameData.playerId}]`;

    if (!gameData || gameData.status !== 'player_turn_awaiting_emoji') {
        console.warn(`${logPrefix} Hit roll received but gameData missing or status is '${gameData?.status || 'Unknown'}'. Expected 'player_turn_awaiting_emoji'. Player's roll value: ${diceValueRolledByPlayer}. Ignoring.`); 
        return;
    }
    if (gameData.playerHandRolls.length < 2) { 
        console.warn(`${logPrefix} Player ${gameData.playerRef} attempted to hit before their initial hand was fully dealt (Hand size: ${gameData.playerHandRolls.length}). This may indicate a flow issue. Ignoring roll: ${diceValueRolledByPlayer}.`); 
        return; 
    }

    console.log(`${logPrefix} Player ${gameData.playerRef} hits. Helper Bot assigned dice value: ${diceValueRolledByPlayer}. Current Score Before Hit: ${gameData.playerScore}`);
    gameData.playerHandRolls.push(diceValueRolledByPlayer); 
    gameData.playerScore += diceValueRolledByPlayer;
    gameData.lastInteractionTime = Date.now();

    const hitAnnouncementText = `🎲 ${gameData.playerRef} hits and the Helper Bot reveals a *${escapeMarkdownV2(String(diceValueRolledByPlayer))}*\\!\nYour new score is now: *${escapeMarkdownV2(String(gameData.playerScore))}*`;
    await safeSendMessage(gameData.chatId, hitAnnouncementText, { parse_mode: 'MarkdownV2' });
    console.log(`${logPrefix} Sent "small box" hit announcement for ${gameData.playerRef}.`);
    await sleep(750); 

    const oldGameMessageId = gameData.gameMessageId;
    if (oldGameMessageId && bot) {
        console.log(`${logPrefix} Deleting old main game message (ID: ${oldGameMessageId}) after player hit.`);
        await bot.deleteMessage(gameData.chatId, Number(oldGameMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete old game message ${oldGameMessageId} after hit: ${e.message}. A new message will still be sent.`));
    }
    
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    let newMainMessage_Header = `🃏 **Dice 21 vs Bot**\nPlayer: ${gameData.playerRef}\nBet: *${betDisplayUSD}*\n\n`;
    let newMainMessage_HandDetails = `Your Hand: ${formatDiceRolls(gameData.playerHandRolls)}\nYour Current Score: *${escapeMarkdownV2(String(gameData.playerScore))}*\n\n`;
    let newMainMessage_ActionPrompt = "";
    let finalNewMainMessageText = "";
    let keyboardForNewMainMessage = {}; 
    let triggerBotTurnNext = false; 
    let gameIsOverNow = false;

    if (gameData.playerScore > DICE_21_TARGET_SCORE) {
        newMainMessage_ActionPrompt = `💥 BUSTED\\! Your score of *${escapeMarkdownV2(String(gameData.playerScore))}* has exceeded the target of ${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}\\. Unfortunately, the Bot wins this round\\.`;
        gameData.status = 'game_over_player_bust'; 
        gameIsOverNow = true; 
    } else if (gameData.playerScore === DICE_21_TARGET_SCORE) {
        newMainMessage_ActionPrompt = `✨ Perfect *${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}*\\! An excellent score\\! You automatically stand\\.\nThe Bot Dealer will now play its turn\\.\\.\\.`;
        gameData.status = 'bot_turn'; 
        triggerBotTurnNext = true; 
    } else { 
        newMainMessage_ActionPrompt = `It's still your turn, ${gameData.playerRef}\\. Send another 🎲 emoji to Hit for one more die, or tap the 'Stand' button to keep your current score\\.`;
        gameData.status = 'player_turn_awaiting_emoji'; 
        keyboardForNewMainMessage = {
            inline_keyboard: [
                [{ text: `✅ Stand (Score: ${gameData.playerScore})`, callback_data: `d21_stand:${gameData.gameId}` }],
                [{ text: "🚫 Forfeit Game", callback_data: `d21_pvb_cancel:${gameData.gameId}` }],
                [{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` }]
            ]
        };
    }
    
    finalNewMainMessageText = newMainMessage_Header + newMainMessage_HandDetails + newMainMessage_ActionPrompt;
    
    console.log(`${logPrefix} Sending new main game message after hit. Text snippet: "${finalNewMainMessageText.substring(0,100)}..."`);
    const sentNewMainMessage = await safeSendMessage(gameData.chatId, finalNewMainMessageText, {
        parse_mode: 'MarkdownV2',
        reply_markup: Object.keys(keyboardForNewMainMessage).length > 0 && keyboardForNewMainMessage.inline_keyboard?.length > 0 ? keyboardForNewMainMessage : {}
    });

    if (sentNewMainMessage?.message_id) {
        gameData.gameMessageId = sentNewMainMessage.message_id; 
        console.log(`${logPrefix} New main game message sent (ID: ${gameData.gameMessageId}).`);
    } else {
        console.error(`${logPrefix} CRITICAL: Failed to send new main game message after player hit for GID ${gameData.gameId}. Game UI will be stale and unplayable.`);
        gameIsOverNow = true; 
        gameData.status = 'game_over_error_ui_update'; 
    }
    activeGames.set(gameData.gameId, gameData); 

    if (gameIsOverNow) {
        console.log(`${logPrefix} Game is over (status: ${gameData.status}) after player hit. Proceeding to finalize.`);
        await finalizeDice21PvBGame(gameData); 
    } else if (triggerBotTurnNext) {
        console.log(`${logPrefix} Player hit 21 or stood (auto after hit). Triggering bot's turn for GID ${gameData.gameId}.`);
        await sleep(2000); 
        const freshGameDataForBot = activeGames.get(gameData.gameId); 
        if (freshGameDataForBot && freshGameDataForBot.status === 'bot_turn') {
            await processDice21BotTurn(freshGameDataForBot);
        } else {
            console.warn(`${logPrefix} Bot turn was triggered for GID ${gameData.gameId}, but game status changed unexpectedly. Current status: ${freshGameDataForBot?.status} or game data not found.`);
        }
    }
}

// Preserving original name: handleDice21PvBStand
async function handleDice21PvBStand(gameId, userObject, originalMessageId, callbackQueryId, chatData) {
    const playerId = String(userObject.id || userObject.telegram_id);
    const gameData = activeGames.get(gameId);
    const logPrefix = `[D21_Stand_PvB GID:${gameId} UID:${playerId}]`;

    if (!gameData || gameData.type !== GAME_IDS.DICE_21 || gameData.playerId !== playerId) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ This Dice 21 game action is invalid or this game is not yours.", show_alert: true }).catch(()=>{}); 
        console.warn(`${logPrefix} Invalid stand attempt. GameData: ${!!gameData}, Type: ${gameData?.type}, ExpectedPlayer: ${gameData?.playerId}, ActualPlayer: ${playerId}`);
        return;
    }
    if (gameData.status !== 'player_turn_awaiting_emoji' || gameData.playerHandRolls.length < 2) {
        const alertMsg = gameData.playerHandRolls.length < 2 ? "Your initial hand must be dealt before you can stand." : "It's not currently your turn to stand or the action is invalid.";
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: alertMsg, show_alert: true }).catch(()=>{}); 
        console.log(`${logPrefix} Stand attempt denied by ${gameData.playerRef}. Game status: ${gameData.status}, Hand size: ${gameData.playerHandRolls.length}.`);
        return;
    }
    
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: `You have chosen to Stand with your score of ${gameData.playerScore}. Preparing for the Bot Dealer's turn...`}).catch(()=>{});

    console.log(`${logPrefix} Player ${gameData.playerRef} stands with score ${gameData.playerScore}. Bot's turn is next.`);
    gameData.status = 'bot_turn'; 
    gameData.lastInteractionTime = Date.now();
    
    const oldGameMessageId = gameData.gameMessageId;
    if (oldGameMessageId && bot) {
        console.log(`${logPrefix} Deleting old main game message (ID: ${oldGameMessageId}) as player stands.`);
        await bot.deleteMessage(gameData.chatId, Number(oldGameMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete old game message ${oldGameMessageId} on stand: ${e.message}`));
    }

    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    let standAndBotTurnMessage = 
        `🃏 **Dice 21 vs Bot**\nPlayer: ${gameData.playerRef}\nBet: *${betDisplayUSD}*\n\n` +
        `Your Final Hand: ${formatDiceRolls(gameData.playerHandRolls)}\n` + 
        `Your Final Score: *${escapeMarkdownV2(String(gameData.playerScore))}*\n\n` +
        `${gameData.playerRef} has chosen to STAND with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\.\n\n` +
        `The Bot Dealer is now taking its turn\\. Please wait\\! 🤖`;

    console.log(`${logPrefix} Sending new main message for bot's turn. Text snippet: "${standAndBotTurnMessage.substring(0,100)}..."`);
    const sentNewMainMessage = await safeSendMessage(gameData.chatId, standAndBotTurnMessage, { 
        parse_mode: 'MarkdownV2', 
        reply_markup: {} 
    });

    if (sentNewMainMessage?.message_id) {
        gameData.gameMessageId = sentNewMainMessage.message_id; 
        console.log(`${logPrefix} New main game message for bot turn sent (ID: ${gameData.gameMessageId}).`);
    } else {
        console.error(`${logPrefix} CRITICAL: Failed to send new main game message after player stand for GID ${gameId}. UI will be stale for bot's turn.`);
    }
    activeGames.set(gameId, gameData); 

    await sleep(2000); 
    const freshGameDataForBotPlay = activeGames.get(gameId); 
    if (freshGameDataForBotPlay && freshGameDataForBotPlay.status === 'bot_turn') {
        await processDice21BotTurn(freshGameDataForBotPlay);
    } else {
        console.warn(`${logPrefix} Bot turn was expected for GID ${gameId}, but game status is now ${freshGameDataForBotPlay?.status} or game not found.`);
    }
}

// Preserving original name: processDice21BotTurn (Streamlined Bot Announcements - V5.1)
async function processDice21BotTurn(gameData) { 
    const logPrefix = `[D21_BotTurn GID:${gameData.gameId} Rev5.1Streamlined]`;
    if (!gameData || isShuttingDown || gameData.status !== 'bot_turn') {
        if (gameData) console.warn(`${logPrefix} Bot turn sequence aborted. Game status: '${gameData.status}'. System shutting down: ${isShuttingDown}.`);
        else console.warn(`${logPrefix} Bot turn sequence aborted, no game data for GID ${gameData?.gameId}.`);
        return;
    }
    console.log(`${logPrefix} Bot Dealer's turn begins. Player ${gameData.playerRef} stands with: *${gameData.playerScore}*.`);

    gameData.status = 'bot_rolling'; 
    gameData.botScore = 0; 
    gameData.botHandRolls = [];
    activeGames.set(gameData.gameId, gameData);

    let initialBotActionMessage = `🤖 The Bot Dealer is playing against ${gameData.playerRef}'s score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\.\n` +
                                  `Helper Bot is rolling the Bot Dealer's initial two dice\\!`;
    await safeSendMessage(gameData.chatId, initialBotActionMessage, {parse_mode: 'MarkdownV2'});
    console.log(`${logPrefix} Initial bot turn announcement sent for GID ${gameData.gameId}.`);
    await sleep(1500); 

    let botFaultedInTurn = false;
    for (let i = 0; i < 2; i++) { // Bot's initial two dice
        if (isShuttingDown) { 
            botFaultedInTurn = true; 
            await safeSendMessage(gameData.chatId, `⚙️ Bot Dealer's turn was interrupted by system maintenance\\.`, {parse_mode: 'MarkdownV2'});
            break; 
        }
        console.log(`${logPrefix} Bot requesting Die ${i+1}/2 for its initial hand from Helper Bot.`);
        const dieRollResult = await getSingleDiceRollViaHelper(gameData.gameId, gameData.chatId, null, `Bot D21 PvB Initial Die ${i + 1}`);
        if (dieRollResult.error) {
            const helperErrorMsg = dieRollResult.message || "an unspecified Helper Bot error";
            console.error(`${logPrefix} Bot failed to get initial die ${i+1} from Helper Bot: ${helperErrorMsg}.`);
            botFaultedInTurn = true;
            await safeSendMessage(gameData.chatId, `⚙️ The Bot Dealer encountered an issue receiving its initial dice: \`${escapeMarkdownV2(helperErrorMsg.substring(0,100))}\`\\.`, {parse_mode: 'MarkdownV2'});
            break;
        }
        gameData.botHandRolls.push(dieRollResult.roll);
        gameData.botScore += dieRollResult.roll;
        await sleep(500); 
    }
    activeGames.set(gameData.gameId, gameData);

    if (botFaultedInTurn) {
        gameData.status = 'game_over_bot_error';
        activeGames.set(gameData.gameId, gameData);
        await finalizeDice21PvBGame(gameData);
        return;
    }

    let botScoreUpdateMessage = 
        `*Player Score:* ${escapeMarkdownV2(String(gameData.playerScore))}\n` +
        `*Bot Dealer Initial Score:* ${escapeMarkdownV2(String(gameData.botScore))} (Hand: ${formatDiceRolls(gameData.botHandRolls)})\n\n`;
    console.log(`${logPrefix} Bot initial 2 dice rolled. Score: ${gameData.botScore}. Hand: ${gameData.botHandRolls.join(',')}`);
    
    const botStandScoreThreshold = DICE_21_BOT_STAND_SCORE; 
    const targetScoreD21 = DICE_21_TARGET_SCORE;

    while (!botFaultedInTurn && gameData.botScore < botStandScoreThreshold && gameData.botScore <= targetScoreD21) {
        if (isShuttingDown) { botFaultedInTurn = true; break; }

        botScoreUpdateMessage += `Bot Dealer (Score: *${escapeMarkdownV2(String(gameData.botScore))}*) will roll again\\.`;
        await safeSendMessage(gameData.chatId, botScoreUpdateMessage, {parse_mode: 'MarkdownV2'});
        console.log(`${logPrefix} Bot score ${gameData.botScore} < ${botStandScoreThreshold}, rolling again.`);
        await sleep(2000); 

        const dieRollResult = await getSingleDiceRollViaHelper(gameData.gameId, gameData.chatId, null, `Bot D21 PvB Hit (Current Score: ${gameData.botScore})`);
        if (dieRollResult.error) {
            const helperErrorMsg = dieRollResult.message || "an unspecified Helper Bot error";
            console.error(`${logPrefix} Bot failed to get subsequent roll from Helper Bot: ${helperErrorMsg}.`);
            botFaultedInTurn = true;
            await safeSendMessage(gameData.chatId, `⚙️ The Bot Dealer encountered an issue rolling an additional die: \`${escapeMarkdownV2(helperErrorMsg.substring(0,100))}\`\\.`, {parse_mode: 'MarkdownV2'});
            break;
        }
        const rollValue = dieRollResult.roll;
        gameData.botHandRolls.push(rollValue);
        gameData.botScore += rollValue;
        activeGames.set(gameData.gameId, gameData);

        botScoreUpdateMessage = 
            `*Player Score:* ${escapeMarkdownV2(String(gameData.playerScore))}\n` +
            `*Bot Dealer rolled a ${escapeMarkdownV2(String(rollValue))}*\\. New Score: *${escapeMarkdownV2(String(gameData.botScore))}* (Hand: ${formatDiceRolls(gameData.botHandRolls)})\n\n`;
        console.log(`${logPrefix} Bot hit, rolled ${rollValue}. New bot score: ${gameData.botScore}.`);

        if (gameData.botScore > targetScoreD21) {
            botScoreUpdateMessage += `💥 Bot BUSTED with *${escapeMarkdownV2(String(gameData.botScore))}*\\!`;
            await safeSendMessage(gameData.chatId, botScoreUpdateMessage, {parse_mode: 'MarkdownV2'});
            await sleep(750);
            break; 
        }
    }
    
    if (!botFaultedInTurn) {
        if (gameData.botScore >= botStandScoreThreshold && gameData.botScore <= targetScoreD21) {
            const finalBotStatusMsg = `*Player Score:* ${escapeMarkdownV2(String(gameData.playerScore))}\n` +
                                   `*Bot Dealer Score:* ${escapeMarkdownV2(String(gameData.botScore))}\n\n` +
                                   `🤖 Bot Dealer stands with *${escapeMarkdownV2(String(gameData.botScore))}*\\.`;
            await safeSendMessage(gameData.chatId, finalBotStatusMsg, {parse_mode: 'MarkdownV2'});
            console.log(`${logPrefix} Bot stands with score ${gameData.botScore}.`);
            await sleep(750);
        } else if (gameData.botScore < botStandScoreThreshold && gameData.botScore <= targetScoreD21) { 
            const finalBotScoreOnlyMsg = `*Player Score:* ${escapeMarkdownV2(String(gameData.playerScore))}\n` +
                                       `*Bot Dealer final score:* ${escapeMarkdownV2(String(gameData.botScore))} (Hand: ${formatDiceRolls(gameData.botHandRolls)})\\.`;
            await safeSendMessage(gameData.chatId, finalBotScoreOnlyMsg, {parse_mode: 'MarkdownV2'});
            console.warn(`${logPrefix} Bot turn ended with score ${gameData.botScore}, below stand threshold and not busted/error. This is unusual unless other logic stopped it.`);
        }
    }
    
    gameData.status = botFaultedInTurn ? 'game_over_bot_error' : 'game_over_bot_played';
    activeGames.set(gameData.gameId, gameData); 
    
    console.log(`${logPrefix} Bot Dealer's turn finished. Status: ${gameData.status}. Player score: ${gameData.playerScore}, Bot score: ${gameData.botScore}. Finalizing game ${gameData.gameId}.`);
    await sleep(1000); 
    await finalizeDice21PvBGame(gameData); 
}

// Preserving original name: finalizeDice21PvBGame (Streamlined - V5.1)
async function finalizeDice21PvBGame(gameData) {
    const logPrefix = `[D21_PvB_Finalize GID:${gameData.gameId} Rev5.1Streamlined]`;

    if (!gameData) {
        console.error(`${logPrefix} Finalize called but gameData is missing for GID ${gameData?.gameId || 'Unknown_GID_Finalize'}. Cannot proceed.`);
        return;
    }
    
    const finalStatus = gameData.status; 
    console.log(`${logPrefix} Finalizing game. Player: ${gameData.playerRef}, Player Score: ${gameData.playerScore}, Bot Score: ${gameData.botScore}, Final Game Status: ${finalStatus}`);
    activeGames.delete(gameData.gameId);
    
    if (gameData.chatType !== 'private') { 
        await updateGroupGameDetails(gameData.chatId, null, null, null);
    }

    let titleEmoji = "🏁";
    let resultText = "";
    let payoutLamports = 0n; 
    let playerWins = false;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    const targetScoreD21 = DICE_21_TARGET_SCORE;

    if (finalStatus === 'game_over_player_bust') {
        titleEmoji = "💥";
        resultText = `${gameData.playerRef}, you BUSTED with *${escapeMarkdownV2(String(gameData.playerScore))}*\\! The Bot Dealer claims your wager of *${betDisplayUSD}* this round\\. Better luck next time\\!`;
    } else if (finalStatus === 'game_over_bot_error') {
        titleEmoji = "⚙️";
        resultText = `The Bot Dealer encountered a technical issue during its turn\\. To ensure fairness, your wager of *${betDisplayUSD}* is being refunded\\. We apologize for the interruption\\.`;
        payoutLamports = gameData.betAmount; 
    } else if (finalStatus === 'game_over_error_ui_update') {
        titleEmoji = "🛠️";
        resultText = `A critical display error occurred while processing this game\\. Your wager of *${betDisplayUSD}* will be refunded\\. Our team is investigating this issue\\.`;
        payoutLamports = gameData.betAmount; 
    } else if (finalStatus === 'game_over_bot_played') {
        if (gameData.playerScore > targetScoreD21) { 
            titleEmoji = "💥"; resultText = `Unexpected state: Your score *${escapeMarkdownV2(String(gameData.playerScore))}* is over ${targetScoreD21}\\. Bot wins\\.`;
        } else if (gameData.botScore > targetScoreD21) {
            titleEmoji = "🎉"; resultText = `The Bot Dealer BUSTED with *${escapeMarkdownV2(String(gameData.botScore))}*\\! ${gameData.playerRef}, you WIN *${betDisplayUSD}* with your score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\!`;
            playerWins = true; payoutLamports = gameData.betAmount * 2n;
        } else if (gameData.playerScore > gameData.botScore) {
            titleEmoji = "🎉"; resultText = `Victory\\! Your score of *${escapeMarkdownV2(String(gameData.playerScore))}* beats the Bot Dealer's *${escapeMarkdownV2(String(gameData.botScore))}*\\. ${gameData.playerRef}, you WIN *${betDisplayUSD}*\\!`;
            playerWins = true; payoutLamports = gameData.betAmount * 2n;
        } else if (gameData.botScore > gameData.playerScore) {
            titleEmoji = "🤖"; resultText = `The Bot Dealer's score of *${escapeMarkdownV2(String(gameData.botScore))}* bests your *${escapeMarkdownV2(String(gameData.playerScore))}*\\. The house takes the wager of *${betDisplayUSD}* this time\\. A valiant effort\\!`;
        } else { 
            titleEmoji = "⚖️"; resultText = `It's a PUSH\\! Both you (${gameData.playerRef}) and the Bot Dealer have a score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\. Your wager of *${betDisplayUSD}* is returned\\.`;
            payoutLamports = gameData.betAmount; 
        }
    } else { 
        titleEmoji = "❓";
        resultText = `The game concluded with an unexpected status: \`${escapeMarkdownV2(String(finalStatus))}\`\\. For safety, your wager of *${betDisplayUSD}* will be refunded\\. Please contact support if you believe this is incorrect\\.`;
        payoutLamports = gameData.betAmount; 
        console.warn(`${logPrefix} Game ended with unhandled status '${finalStatus}'. Refunding player ${gameData.playerRef}.`);
    }

    const oldGameMessageId = gameData.gameMessageId;
    if (oldGameMessageId && bot) {
        console.log(`${logPrefix} Deleting previous main game message (ID: ${oldGameMessageId}) before sending final result.`);
        await bot.deleteMessage(gameData.chatId, Number(oldGameMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete old game message ${oldGameMessageId} on finalize: ${e.message}`));
    }
    
    let finalUserBalance = await getUserBalance(gameData.playerId) || 0n; 
    let finalBalanceText = "";
    let dbErrorDuringPayout = false;

    if (payoutLamports >= 0n) { // Changed to >= 0n to include refunds and 0 payout losses for logging
        let client = null;
        try {
            client = await pool.connect(); await client.query('BEGIN');
            let transactionType = 'loss_dice21_pvb'; // Default
            if (playerWins) transactionType = 'win_dice21_pvb';
            else if (payoutLamports === gameData.betAmount) transactionType = 'refund_dice21_pvb'; // Covers push and error refunds
            else if (finalStatus === 'game_over_player_bust') transactionType = 'loss_dice21_pvb_player_bust';
            else if (finalStatus === 'game_over_bot_error') transactionType = 'win_dice21_pvb_bot_helper_error'; // This is a win for player (refund type)
            
            const notes = `Dice 21 PvB Game ${gameData.gameId} Result. Player: ${gameData.playerRef}. Status: ${finalStatus}. Payout: ${payoutLamports}`;
            
            const balanceUpdateResult = await updateUserBalanceAndLedger(
                client, gameData.playerId, payoutLamports, 
                transactionType, { game_id_custom_field: gameData.gameId, game_status_custom_field: finalStatus }, notes
            );
            
            if (balanceUpdateResult.success) {
                await client.query('COMMIT');
                finalUserBalance = balanceUpdateResult.newBalanceLamports;
                console.log(`${logPrefix} Successfully updated balance for ${gameData.playerRef}. Payout: ${payoutLamports}, New Bal: ${finalUserBalance}`);
            } else {
                await client.query('ROLLBACK');
                dbErrorDuringPayout = true;
                finalBalanceText = `\n\n⚠️ *Balance Update Error:* An issue occurred crediting your account (\`${escapeMarkdownV2(balanceUpdateResult.error || "Unknown DB error")}\`). Our support team has been notified\\.`;
                console.error(`${logPrefix} FAILED to update balance for ${gameData.playerRef} after game. Payout: ${payoutLamports}. Error: ${balanceUpdateResult.error}`);
            }
        } catch (e) {
            if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Finalize DB Rollback Exception: ${rbErr.message}`));
            dbErrorDuringPayout = true;
            finalBalanceText = `\n\n🚨 *Critical Database Error:* A server issue prevented your balance from updating\\. Please contact support immediately with Game ID: \`${gameData.gameId}\``;
            console.error(`${logPrefix} CRITICAL DB error during finalization for GID ${gameData.gameId}: ${e.message}. Stack: ${e.stack ? e.stack.substring(0,500) : 'N/A'}`);
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 D21 PvB Finalize Payout DB Failure 🚨\nGame ID: \`${escapeMarkdownV2(String(gameData.gameId))}\`, Player: ${gameData.playerRef}\nError: ${escapeMarkdownV2(e.message)}\\. MANUAL BALANCE CHECK/CREDIT REQUIRED for ${payoutLamports} lamports\\.`, {parse_mode: 'MarkdownV2'});
            }
        } finally {
            if (client) client.release();
        }
    } else { 
         finalUserBalance = await getUserBalance(gameData.playerId) || 0n; 
        console.log(`${logPrefix} No payout for ${gameData.playerRef} (likely a loss). Final balance from DB: ${finalUserBalance}`);
    }

    if (!dbErrorDuringPayout) {
        finalBalanceText = `\n\n💳 Your new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalance, 'USD'))}* / *${escapeMarkdownV2(formatCurrency(finalUserBalance, 'SOL'))}*\\.`;
    }
    
    const fullResultMessage = 
        `${titleEmoji} **Dice 21 vs Bot \\- Game Over\\!** ${titleEmoji}\n\n` +
        `--- *Final Scores* ---\n` +
        `👤 ${gameData.playerRef}: *${escapeMarkdownV2(String(gameData.playerScore))}* ${gameData.status === 'game_over_player_bust' ? "💥 (Busted)" : ""}\n` +
        `🤖 Bot Dealer: *${escapeMarkdownV2(String(gameData.botScore))}* ${gameData.botScore > DICE_21_TARGET_SCORE ? "💥 (Busted)" : ""}\n\n` +
        `${resultText}` +
        `${finalBalanceText}`;

    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_21, gameData.betAmount); 
    console.log(`${logPrefix} Sending final game result message for GID ${gameData.gameId}. Snippet: "${fullResultMessage.substring(0,100)}..."`);
    await safeSendMessage(gameData.chatId, fullResultMessage, { 
        parse_mode: 'MarkdownV2', 
        reply_markup: finalKeyboard 
    });
    console.log(`${logPrefix} Finalization complete for game ${gameData.gameId}.`);
}


// --- Player vs. Player (PvP) Dice 21 Specific Logic ---
// Preserving original name: startDice21PvPInitialDeal
async function startDice21PvPInitialDeal(gameId) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DICE_21_PVP || gameData.status !== 'dealing_initial_hands') {
        console.warn(`[D21_PvP_InitialDeal GID:${gameId}] Start deal called for invalid game state or type. Status: ${gameData?.status}, Type: ${gameData?.type}.`);
        return;
    }
    const logPrefix = `[D21_PvP_InitialDeal GID:${gameId}]`;
    console.log(`${logPrefix} Starting initial deal for PvP game. Initiator: ${gameData.initiator.mention}, Opponent: ${gameData.opponent.mention}.`);
    
    const initialMessageText = `⚔️ **Dice 21 PvP: ${gameData.initiator.mention} vs ${gameData.opponent.mention}** ⚔️\n` +
                             `Bet: *${escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'))}* each\\.\n\n` +
                             `The Helper Bot is now dealing the initial two dice to each player\\. Please wait for the reveal\\! ⏳`;

    const sentInitialMessage = await safeSendMessage(gameData.chatId, initialMessageText, { parse_mode: 'MarkdownV2' });
    if (!sentInitialMessage?.message_id) {
        console.error(`${logPrefix} CRITICAL: Failed to send initial dealing message for PvP game ${gameId}. Game cannot proceed with UI.`);
        activeGames.delete(gameId); 
        await updateGroupGameDetails(gameData.chatId, null, null, null); 
        await safeSendMessage(gameData.chatId, `A critical display error occurred starting the Dice 21 PvP game\\. Game abandoned\\. If bets were placed, please contact support for review with Game ID approx time: ${new Date().toISOString()}`, {parse_mode:'MarkdownV2'});
        return;
    }
    gameData.currentMessageId = sentInitialMessage.message_id;
    activeGames.set(gameId, gameData);
    console.log(`${logPrefix} Initial PvP dealing message sent (ID: ${gameData.currentMessageId}).`);
    
    // Deal to initiator
    let initiatorFaulted = false;
    for (let i = 0; i < 2; i++) {
        console.log(`${logPrefix} Dealing Die ${i+1}/2 to Initiator (${gameData.initiator.mention}) via Helper Bot.`);
        const rollResult = await getSingleDiceRollViaHelper(gameId, gameData.chatId, gameData.initiator.userId, `Initiator D21 PvP Die ${i+1}`);
        if (rollResult.error) {
            console.error(`${logPrefix} Helper Bot error for Initiator Die ${i+1}: ${rollResult.message}`);
            initiatorFaulted = true; break;
        }
        gameData.initiator.hand.push(rollResult.roll);
        gameData.initiator.score += rollResult.roll;
        await safeSendMessage(gameData.chatId, `🎲 ${gameData.initiator.mention} received a *${escapeMarkdownV2(String(rollResult.roll))}* (Die ${i+1}/2 from Helper Bot)`, {parse_mode:'MarkdownV2'});
        if (i < 1) await sleep(1000); // Pause between dice for same player
    }
    activeGames.set(gameData.gameId, gameData);
    if (initiatorFaulted) {
        gameData.status = 'game_over_error_deal_initiator';
        await finalizeDice21PvPGame(gameData); return;
    }
    console.log(`${logPrefix} Initiator's hand dealt: [${gameData.initiator.hand.join(', ')}], Score: ${gameData.initiator.score}.`);
    await sleep(1500); // Pause before dealing to opponent

    // Deal to opponent
    let opponentFaulted = false;
    for (let i = 0; i < 2; i++) {
        console.log(`${logPrefix} Dealing Die ${i+1}/2 to Opponent (${gameData.opponent.mention}) via Helper Bot.`);
        const rollResult = await getSingleDiceRollViaHelper(gameId, gameData.chatId, gameData.opponent.userId, `Opponent D21 PvP Die ${i+1}`);
        if (rollResult.error) {
            console.error(`${logPrefix} Helper Bot error for Opponent Die ${i+1}: ${rollResult.message}`);
            opponentFaulted = true; break;
        }
        gameData.opponent.hand.push(rollResult.roll);
        gameData.opponent.score += rollResult.roll;
        await safeSendMessage(gameData.chatId, `🎲 ${gameData.opponent.mention} received a *${escapeMarkdownV2(String(rollResult.roll))}* (Die ${i+1}/2 from Helper Bot)`, {parse_mode:'MarkdownV2'});
        if (i < 1) await sleep(1000);
    }
    activeGames.set(gameData.gameId, gameData);
    if (opponentFaulted) {
        gameData.status = 'game_over_error_deal_opponent';
        await finalizeDice21PvPGame(gameData); return;
    }
    console.log(`${logPrefix} Opponent's hand dealt: [${gameData.opponent.hand.join(', ')}], Score: ${gameData.opponent.score}.`);
    await sleep(1500); 
    
    // Determine who goes first (initiator usually, unless one got Blackjack and other didn't, etc.)
    // Standard flow: Initiator goes first. If initiator has BJ and opponent doesn't, game over. If both BJ, push.
    const p1Score = gameData.initiator.score; const p2Score = gameData.opponent.score;
    const p1HasBJ = p1Score === DICE_21_TARGET_SCORE; const p2HasBJ = p2Score === DICE_21_TARGET_SCORE;

    if (p1HasBJ && p2HasBJ) {
        gameData.status = 'game_over_push_both_blackjack';
        gameData.initiator.status = 'blackjack'; gameData.opponent.status = 'blackjack';
    } else if (p1HasBJ) {
        gameData.status = 'game_over_initiator_blackjack';
        gameData.initiator.status = 'blackjack'; gameData.opponent.status = 'lost_to_blackjack';
    } else if (p2HasBJ) {
        gameData.status = 'game_over_opponent_blackjack';
        gameData.opponent.status = 'blackjack'; gameData.initiator.status = 'lost_to_blackjack';
    } else {
        // Neither has Blackjack on deal. Initiator's turn.
        gameData.initiator.isTurn = true; gameData.opponent.isTurn = false;
        gameData.initiator.status = 'playing_turn'; gameData.opponent.status = 'waiting_turn'; // Explicit player status
        gameData.status = 'initiator_turn'; // Game status reflects current player
    }
    activeGames.set(gameData.gameId, gameData);
    console.log(`${logPrefix} Initial deal complete. Game status: ${gameData.status}. P1 Score: ${p1Score}, P2 Score: ${p2Score}.`);
    
    if (gameData.status.startsWith('game_over')) {
        await finalizeDice21PvPGame(gameData);
    } else {
        await updateDice21PvPGameMessage(gameData); // Show initial hands and current player's turn
    }
}

// Preserving original name: updateDice21PvPGameMessage (V5.1 using delete & resend)
async function updateDice21PvPGameMessage(gameData, isFinal = false, customMessageContent = null) {
    if (!gameData || !bot) {
        console.warn(`[D21_PvP_UpdateMsg GID:${gameData?.gameId || 'Unknown_GID'}] Update requested but gameData or bot is missing. Cannot update.`);
        return;
    }
    const logPrefix = `[D21_PvP_UpdateMsg GID:${gameData.gameId} Final:${isFinal}]`;
    const currentMainMessageId = gameData.currentMessageId;

    if (currentMainMessageId && bot) {
        console.log(`${logPrefix} Deleting previous main PvP game message (ID: ${currentMainMessageId}).`);
        await bot.deleteMessage(gameData.chatId, Number(currentMainMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete message ${currentMainMessageId} before update: ${e.message}`));
    }
    
    let messageText;
    let keyboard = {};
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    if (customMessageContent) {
        messageText = customMessageContent;
        if (!isFinal) { // Keep rules button for non-final custom messages if appropriate
             keyboard = { inline_keyboard: [[{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` }]] };
        }
    } else {
        const p1 = gameData.initiator; const p2 = gameData.opponent;
        const p1StatusIcon = p1.status === 'stood' ? "✅" : (p1.status === 'bust' ? "💥" : (p1.status === 'blackjack' ? "✨" : ""));
        const p2StatusIcon = p2.status === 'stood' ? "✅" : (p2.status === 'bust' ? "💥" : (p2.status === 'blackjack' ? "✨" : ""));

        messageText = `⚔️ **Dice 21 PvP: ${p1.mention} vs ${p2.mention}** ⚔️\n` +
                             `Bet: *${betDisplayUSD}* each\\.\n\n` +
                             `Player 1: ${p1.mention}\n` +
                             `Hand: ${formatDiceRolls(p1.hand)} Score: *${escapeMarkdownV2(String(p1.score))}* ${p1StatusIcon}\n\n` +
                             `Player 2: ${p2.mention}\n` +
                             `Hand: ${formatDiceRolls(p2.hand)} Score: *${escapeMarkdownV2(String(p2.score))}* ${p2StatusIcon}\n\n`;

        let currentActionPrompt = "";
        const buttonsForKeyboard = [];
        const currentPlayerWhoseTurnItIs = p1.isTurn ? p1 : (p2.isTurn ? p2 : null);

        if (gameData.status === 'game_over_pvp') { // Generic game over before finalize message
            currentActionPrompt = "🏁 The game has concluded\\! The final scores are being tallied by the croupier\\.\\.\\.";
        } else if (gameData.status.startsWith('game_over_')) { // More specific game over states
             currentActionPrompt = "🏁 This intense duel has reached its conclusion! Calculating final results...";
        } else if (gameData.status === 'dealing_initial_hands') { 
            currentActionPrompt = "⏳ Initial hands are currently being dealt by the Helper Bot for both players\\. Please wait for the dice to settle\\.";
        } else if (currentPlayerWhoseTurnItIs && currentPlayerWhoseTurnItIs.status === 'playing_turn') { // Check explicit player status
            currentActionPrompt = `It's YOUR turn to act, ${currentPlayerWhoseTurnItIs.mention}\\! Send a 🎲 emoji to **Hit** for another die, or tap the **Stand** button below to keep your current score of *${escapeMarkdownV2(String(currentPlayerWhoseTurnItIs.score))}*\\.`;
            buttonsForKeyboard.push([{ text: `✅ Stand (Score: ${currentPlayerWhoseTurnItIs.score})`, callback_data: `d21_pvp_stand:${gameData.gameId}:${currentPlayerWhoseTurnItIs.userId}` }]);
        } else if (currentPlayerWhoseTurnItIs) { 
            const otherPlayerInGame = (currentPlayerWhoseTurnItIs === p1) ? p2 : p1;
            if (otherPlayerInGame.isTurn && otherPlayerInGame.status === 'playing_turn') {
                 currentActionPrompt = `Waiting for ${otherPlayerInGame.mention} to make their move\\.\\.\\.`;
            } else if (otherPlayerInGame.status !== 'playing_turn' && currentPlayerWhoseTurnItIs.status !== 'playing_turn') { 
                 currentActionPrompt = `All players have completed their turns\\. Calculating the results now\\!`;
                 if (gameData.status !== 'game_over_pvp' && !gameData.status.startsWith('game_over_')) { 
                    console.log(`${logPrefix} Both players done playing, explicitly setting game status to game_over_pvp from update message logic.`);
                    gameData.status = 'game_over_pvp'; 
                 }
            } else { 
                currentActionPrompt = `Waiting for ${otherPlayerInGame.mention}'s turn or game resolution\\.\\.\\.`;
            }
        } else { 
            currentActionPrompt = "Determining the next turn or final game resolution\\.\\.\\.";
            console.warn(`${logPrefix} UI update called in an intermediate PvP state with no clear current player turn. Status: ${gameData.status}. P1 Turn: ${p1.isTurn}, P2 Turn: ${p2.isTurn}. Check turn logic.`);
        }
        messageText += currentActionPrompt;

        if (!gameData.status.startsWith('game_over_') && gameData.status !== 'dealing_initial_hands') {
            buttonsForKeyboard.push([{ text: "📖 Game Rules (Dice 21)", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` }]);
        }
        if (buttonsForKeyboard.length > 0) keyboard = { inline_keyboard: buttonsForKeyboard };
    }
    
    console.log(`${logPrefix} Sending PvP main game message update. Text snippet: "${messageText.substring(0,100)}..."`);
    const sentNewMainMessage = await safeSendMessage(gameData.chatId, messageText, { 
        parse_mode: 'MarkdownV2', 
        reply_markup: Object.keys(keyboard).length > 0 && keyboard.inline_keyboard?.length > 0 ? keyboard : {}
    });

    if (sentNewMainMessage?.message_id) {
        gameData.currentMessageId = sentNewMainMessage.message_id; 
        activeGames.set(gameData.gameId, gameData);
        console.log(`${logPrefix} PvP main game message sent/updated successfully (New ID: ${gameData.currentMessageId}).`);
    } else {
        console.error(`${logPrefix} CRITICAL: Failed to send new PvP main game message for GID ${gameData.gameId}. UI will be broken for players.`);
        await safeSendMessage(gameData.chatId, 
            `🚨 A critical display error occurred in your PvP Dice 21 game (\`${escapeMarkdownV2(gameData.gameId.slice(-6))}\`)\\. The game interface could not be updated correctly and the game must be cancelled\\. ` +
            `All bets will be refunded\\. We sincerely apologize for this interruption\\.`, 
            {parse_mode: 'MarkdownV2'}
        );
        gameData.status = 'game_over_error_ui_update'; 
        activeGames.set(gameData.gameId, gameData);
        if (!isFinal) await finalizeDice21PvPGame(gameData); 
    }
}


// Preserving original name: processDice21PvPRollByEmoji (V5.1 using new update method)
async function processDice21PvPRollByEmoji(gameData, diceValueRolled, userIdWhoRolled) { 
    const logPrefix = `[D21_PvP_Hit GID:${gameData.gameId} UID:${userIdWhoRolled} Rev5.1]`;
    let currentPlayer, otherPlayer, playerKey;

    if (gameData.initiator.userId === userIdWhoRolled && gameData.initiator.isTurn) { 
        currentPlayer = gameData.initiator; otherPlayer = gameData.opponent; playerKey = 'initiator';
    } else if (gameData.opponent.userId === userIdWhoRolled && gameData.opponent.isTurn) { 
        currentPlayer = gameData.opponent; otherPlayer = gameData.initiator; playerKey = 'opponent';
    } else { 
        console.warn(`${logPrefix} Roll received from ${userIdWhoRolled}, but it's not their turn or they are not in this game. Current initiator turn: ${gameData.initiator.isTurn}, Opponent turn: ${gameData.opponent.isTurn}. Ignoring roll: ${diceValueRolled}.`);
        return; 
    }
    
    if (gameData.status !== `${playerKey}_turn` || currentPlayer.status !== 'playing_turn') { // Added check for player status
        console.warn(`${logPrefix} Player ${currentPlayer.mention} attempted to hit, but game status is '${gameData.status}' or player status is '${currentPlayer.status}'. Expected Game Status: '${playerKey}_turn' & Player Status: 'playing_turn'. Ignoring roll: ${diceValueRolled}.`); 
        return;
    }
    if (currentPlayer.hand.length < 2 && currentPlayer.status === 'playing_turn') { 
        console.warn(`${logPrefix} Player ${currentPlayer.mention} attempted to hit before their initial PvP hand was fully dealt or hand is empty. Hand size: ${currentPlayer.hand.length}. This indicates a flow issue. Ignoring roll: ${diceValueRolled}.`); 
        return; 
    }

    console.log(`${logPrefix} Player ${currentPlayer.mention} hits. Helper Bot assigned value: ${diceValueRolled}. Current score before hit: ${currentPlayer.score}.`);
    currentPlayer.hand.push(diceValueRolled); 
    currentPlayer.score += diceValueRolled; 
    gameData.lastInteractionTime = Date.now();

    const hitAnnouncementText = `🎲 ${currentPlayer.mention} (PvP) hits and the Helper Bot deals a *${escapeMarkdownV2(String(diceValueRolled))}*\\!\nTheir new score is now *${escapeMarkdownV2(String(currentPlayer.score))}*\\.`;
    await safeSendMessage(gameData.chatId, hitAnnouncementText, {parse_mode: 'MarkdownV2'});
    console.log(`${logPrefix} Sent "small box" hit announcement for ${currentPlayer.mention}.`);
    await sleep(1000); // Pause for user to read announcement

    if (currentPlayer.score > DICE_21_TARGET_SCORE) { 
        console.log(`${logPrefix} Player ${currentPlayer.mention} BUSTED with score ${currentPlayer.score}.`);
        currentPlayer.status = 'bust'; 
        currentPlayer.isTurn = false; 
        gameData.status = playerKey === 'initiator' ? 'game_over_initiator_bust_during_turn' : 'game_over_opponent_bust_during_turn';
        activeGames.set(gameData.gameId, gameData);
        await finalizeDice21PvPGame(gameData);
    } else if (currentPlayer.score === DICE_21_TARGET_SCORE) { 
        console.log(`${logPrefix} Player ${currentPlayer.mention} hit 21. They automatically stand.`);
        currentPlayer.status = 'stood'; 
        currentPlayer.isTurn = false;
        // Transition to other player's turn or finalize
        if (playerKey === 'initiator') {
            // If opponent already played (stood/busted/blackjack), game over
            if (otherPlayer.status === 'stood' || otherPlayer.status === 'bust' || otherPlayer.status === 'blackjack' || otherPlayer.status === 'lost_to_blackjack') {
                gameData.status = 'game_over_both_played_final';
            } else {
                otherPlayer.isTurn = true; otherPlayer.status = 'playing_turn';
                gameData.status = 'opponent_turn';
            }
        } else { // Opponent hit 21
            gameData.status = 'game_over_both_played_final'; // Both players have completed their turns
        }
        activeGames.set(gameData.gameId, gameData);
        if (gameData.status.startsWith('game_over')) {
            await finalizeDice21PvPGame(gameData);
        } else {
            await updateDice21PvPGameMessage(gameData);
        }
    } else {
        // Player hit and score is still valid, turn continues for them
        gameData.status = `${playerKey}_turn`; // Remains their turn, player status is still 'playing_turn'
        activeGames.set(gameData.gameId, gameData);
        await updateDice21PvPGameMessage(gameData);
    }
}

// Preserving original name: handleDice21PvPStand (V5.1 Logic - was handleDice21PvPStandAction in one version)
async function handleDice21PvPStand(gameId, userIdWhoStood, originalMessageId, callbackQueryId, chatData) { 
    const gameData = activeGames.get(gameId);
    const logPrefix = `[D21_Stand_PvP GID:${gameId} UID:${userIdWhoStood} Rev5.1]`;

    if (!gameData || gameData.type !== GAME_IDS.DICE_21_PVP) { 
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Dice 21 PvP game isn't active or has ended.", show_alert: true }).catch(()=>{});
        console.warn(`${logPrefix} Stand attempt for invalid game or type. GID: ${gameId}, Type: ${gameData?.type}.`); 
        return; 
    }
    if (gameData.status.startsWith('game_over_')) {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "The game is already over.", show_alert: false }).catch(()=>{});
        console.log(`${logPrefix} Stand action received but game GID ${gameId} is already over (Status: ${gameData.status}).`);
        return;
    }
    
    let playerStanding, otherPlayer, playerKeyStanding;
    if (gameData.initiator.userId === userIdWhoStood && gameData.initiator.isTurn) {
        playerStanding = gameData.initiator; otherPlayer = gameData.opponent; playerKeyStanding = 'initiator';
    } else if (gameData.opponent.userId === userIdWhoStood && gameData.opponent.isTurn) {
        playerStanding = gameData.opponent; otherPlayer = gameData.initiator; playerKeyStanding = 'opponent';
    } else {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "It's not your turn to stand or this isn't your game.", show_alert: true }).catch(()=>{});
        console.warn(`${logPrefix} Stand attempt by ${userIdWhoStood}, but not their turn or not in game. Current Game Status: ${gameData.status}, P1 Turn: ${gameData.initiator.isTurn}, P2 Turn: ${gameData.opponent.isTurn}.`);
        return;
    }
    
    if (playerStanding.status !== 'playing_turn') { // Check explicit player status
         if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "You can only stand when it's your active turn.", show_alert: true }).catch(()=>{});
         console.warn(`${logPrefix} Player ${playerStanding.mention} tried to stand but their status is ${playerStanding.status}, not 'playing_turn'. Game status: ${gameData.status}`);
         return;
    }
    if (playerStanding.hand.length < 2) { 
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "You must have at least two dice to stand.", show_alert: true }).catch(()=>{});
        console.warn(`${logPrefix} Player ${playerStanding.mention} tried to stand with only ${playerStanding.hand.length} dice. Flow issue?`);
        return; 
    }

    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: `You stand with a score of ${playerStanding.score}. Waiting for opponent...`}).catch(()=>{});
    console.log(`${logPrefix} Player ${playerStanding.mention} stands with score ${playerStanding.score}.`);

    playerStanding.status = 'stood';
    playerStanding.isTurn = false;
    gameData.lastInteractionTime = Date.now();
    
    const standAnnouncementText = `✋ ${playerStanding.mention} stands tall with a score of *${escapeMarkdownV2(String(playerStanding.score))}*\\!`;
    await safeSendMessage(gameData.chatId, standAnnouncementText, {parse_mode: 'MarkdownV2'});
    await sleep(1000);

    if (playerKeyStanding === 'initiator') {
        // Initiator stood, now opponent's turn if they haven't played or busted
        if (otherPlayer.status === 'bust' || otherPlayer.status === 'stood' || otherPlayer.status === 'blackjack' || otherPlayer.status === 'lost_to_blackjack') {
            gameData.status = 'game_over_both_played_final'; // Opponent already finished
        } else { // Opponent still needs to play
            otherPlayer.isTurn = true; otherPlayer.status = 'playing_turn';
            gameData.status = 'opponent_turn';
        }
    } else { // Opponent stood
        gameData.status = 'game_over_both_played_final'; // Game ends as opponent was the second player to complete their turn
    }
    
    activeGames.set(gameData.gameId, gameData);
    
    if (gameData.status.startsWith('game_over')) {
        console.log(`${logPrefix} Game is over (status: ${gameData.status}) after ${playerStanding.mention} stood. Finalizing.`);
        await finalizeDice21PvPGame(gameData);
    } else {
        console.log(`${logPrefix} After ${playerStanding.mention} stood, it's now ${otherPlayer.mention}'s turn (status: ${gameData.status}). Updating game message.`);
        await updateDice21PvPGameMessage(gameData);
    }
}

// Preserving original name: finalizeDice21PvPGame (V5.1 Logic)
async function finalizeDice21PvPGame(gameData) {
    const logPrefix = `[D21_PvP_Finalize GID:${gameData.gameId} Rev5.1]`;
    if (!gameData) {
        console.error(`${logPrefix} Finalize called but gameData is missing for GID ${gameData?.gameId || 'Unknown_GID_FinalizePvP'}.`);
        return;
    }

    const finalStatus = gameData.status; 
    console.log(`${logPrefix} Finalizing PvP game. P1(${gameData.initiator.mention}): Score ${gameData.initiator.score} (Status: ${gameData.initiator.status}). P2(${gameData.opponent.mention}): Score ${gameData.opponent.score} (Status: ${gameData.opponent.status}). Overall Game Status: ${finalStatus}`);
    activeGames.delete(gameData.gameId);
    await updateGroupGameDetails(gameData.chatId, null, null, null);

    let titleEmoji = "🏁";
    let resultText = "";
    let p1_payout = 0n; let p2_payout = 0n;
    const p1 = gameData.initiator; const p2 = gameData.opponent;
    const target = DICE_21_TARGET_SCORE;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    // Determine Winner
    if (finalStatus === 'game_over_error_deal_initiator' || finalStatus === 'game_over_error_deal_opponent' || finalStatus === 'game_over_error_ui_update' || finalStatus === 'game_over_error_helper_bot') {
        titleEmoji = "⚙️";
        resultText = `A technical error occurred during the game\\. All bets (${betDisplayUSD} each) have been refunded to both players\\. We apologize for the inconvenience\\.`;
        p1_payout = gameData.betAmount; p2_payout = gameData.betAmount;
    } else if (finalStatus === 'game_over_push_both_blackjack') {
        titleEmoji = "✨⚖️✨";
        resultText = `An electrifying DOUBLE BLACKJACK\\! Both ${p1.mention} and ${p2.mention} hit *${target}* on the deal\\! It's a PUSH\\. All bets (${betDisplayUSD} each) are returned\\.`;
        p1_payout = gameData.betAmount; p2_payout = gameData.betAmount;
    } else if (finalStatus === 'game_over_initiator_blackjack') {
        titleEmoji = "✨🏆";
        resultText = `${p1.mention} scores a natural BLACKJACK (*${target}*) on the deal\\! ${p1.mention} WINS *${betDisplayUSD}* profit\\!`;
        p1_payout = gameData.betAmount * 2n;
    } else if (finalStatus === 'game_over_opponent_blackjack') {
        titleEmoji = "✨🏆";
        resultText = `${p2.mention} scores a natural BLACKJACK (*${target}*) on the deal\\! ${p2.mention} WINS *${betDisplayUSD}* profit\\!`;
        p2_payout = gameData.betAmount * 2n;
    } else if (finalStatus === 'game_over_initiator_bust_during_turn' || p1.status === 'bust') {
        titleEmoji = "💥🏆";
        resultText = `${p1.mention} BUSTED with *${escapeMarkdownV2(String(p1.score))}*\\! ${p2.mention} WINS *${betDisplayUSD}* profit by default\\!`;
        p2_payout = gameData.betAmount * 2n;
    } else if (finalStatus === 'game_over_opponent_bust_during_turn' || p2.status === 'bust') {
        titleEmoji = "💥🏆";
        resultText = `${p2.mention} BUSTED with *${escapeMarkdownV2(String(p2.score))}*\\! ${p1.mention} WINS *${betDisplayUSD}* profit by default\\!`;
        p1_payout = gameData.betAmount * 2n;
    } else { // Both players likely stood, or one stood and other didn't bust to beat them
        const p1_finalScore = (p1.status === 'bust') ? -1 : p1.score; // Use -1 for bust to simplify comparison
        const p2_finalScore = (p2.status === 'bust') ? -1 : p2.score;

        if (p1_finalScore === -1 && p2_finalScore === -1) { // Should be caught by individual bust statuses, but defensive
             titleEmoji = "💥💥"; resultText = `A rare DOUBLE BUST outside of turn processing! Bets likely returned. Please check balance.`; p1_payout = gameData.betAmount; p2_payout = gameData.betAmount;
        } else if (p1_finalScore === -1) { // P1 busted, P2 wins (should have been caught)
             titleEmoji = "🏆"; resultText = `${p1.mention} BUSTED. ${p2.mention} wins!`; p2_payout = gameData.betAmount * 2n;
        } else if (p2_finalScore === -1) { // P2 busted, P1 wins
             titleEmoji = "🏆"; resultText = `${p2.mention} BUSTED. ${p1.mention} wins!`; p1_payout = gameData.betAmount * 2n;
        } else if (p1_finalScore > p2_finalScore) {
            titleEmoji = "🏆"; resultText = `${p1.mention} WINS with a superior score of *${escapeMarkdownV2(String(p1.score))}* against ${p2.mention}'s *${escapeMarkdownV2(String(p2.score))}*\\! Congratulations, ${p1.mention}\\!`;
            p1_payout = gameData.betAmount * 2n;
        } else if (p2_finalScore > p1_finalScore) {
            titleEmoji = "🏆"; resultText = `${p2.mention} is victorious with a stunning score of *${escapeMarkdownV2(String(p2.score))}* against ${p1.mention}'s *${escapeMarkdownV2(String(p1.score))}*\\! Well played, ${p2.mention}\\!`;
            p2_payout = gameData.betAmount * 2n;
        } else { // Scores are equal
            titleEmoji = "⚖️"; resultText = `It's a PUSH\\! Both ${p1.mention} and ${p2.mention} ended with a score of *${escapeMarkdownV2(String(p1.score))}*\\. All bets (${betDisplayUSD} each) are returned\\.`;
            p1_payout = gameData.betAmount; p2_payout = gameData.betAmount;
        }
    }
    
    const oldGameMessageId = gameData.currentMessageId;
    if (oldGameMessageId && bot) {
        console.log(`${logPrefix} Deleting previous PvP main game message (ID: ${oldGameMessageId}) before sending final result.`);
        await bot.deleteMessage(gameData.chatId, Number(oldGameMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete old PvP game message ${oldGameMessageId} on finalize: ${e.message}`));
    }

    let p1_finalBalance = await getUserBalance(p1.userId) || 0n;
    let p2_finalBalance = await getUserBalance(p2.userId) || 0n;
    let dbErrorTextForUser = ""; let criticalDbErrorForAdmin = false;

    let client = null;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const determineLedgerType = (payout, bet, isPush) => isPush ? 'refund_dice21_pvp' : (payout > bet ? 'win_dice21_pvp' : (payout === 0n ? 'loss_dice21_pvp' : 'unknown_dice21_pvp'));
        
        const p1_is_push = (p1.score === p2.score && p1.status !== 'bust' && p2.status !== 'bust') || finalStatus.includes('_error_') || finalStatus.includes('_push_');
        const p2_is_push = p1_is_push;

        const p1Update = await updateUserBalanceAndLedger(client, p1.userId, p1_payout, determineLedgerType(p1_payout, gameData.betAmount, p1_is_push), {game_id_custom_field: gameData.gameId, opponent_id: p2.userId, player_score: p1.score, opponent_score: p2.score}, `Dice 21 PvP result vs ${p2.mention}`);
        if (p1Update.success) p1_finalBalance = p1Update.newBalanceLamports; else throw new Error(`P1 (${p1.mention}) balance update failed: ${p1Update.error}`);
        
        const p2Update = await updateUserBalanceAndLedger(client, p2.userId, p2_payout, determineLedgerType(p2_payout, gameData.betAmount, p2_is_push), {game_id_custom_field: gameData.gameId, opponent_id: p1.userId, player_score: p2.score, opponent_score: p1.score}, `Dice 21 PvP result vs ${p1.mention}`);
        if (p2Update.success) p2_finalBalance = p2Update.newBalanceLamports; else throw new Error(`P2 (${p2.mention}) balance update failed: ${p2Update.error}`);
        
        await client.query('COMMIT');
        console.log(`${logPrefix} PvP balances updated successfully. P1 payout: ${p1_payout}, P2 payout: ${p2_payout}.`);
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} PvP Finalize DB Rollback Exception: ${rbErr.message}`)); 
        criticalDbErrorForAdmin = true;
        dbErrorTextForUser = `\n\n⚠️ *Critical Balance Update Error:* A server issue prevented balances from updating correctly (\`${escapeMarkdownV2(e.message || "Unknown DB error")}\`). Please contact support with Game ID: \`${escapeMarkdownV2(String(gameData.gameId))}\`\\.`;
        console.error(`${logPrefix} CRITICAL DB error during PvP finalization for GID ${gameData.gameId}: ${e.message}. Stack: ${e.stack ? e.stack.substring(0,600) : 'N/A'}`);
    } finally {
        if (client) client.release();
    }

    if (criticalDbErrorForAdmin && typeof notifyAdmin === 'function') {
        notifyAdmin(`🚨 D21 PvP Finalize Payout DB Failure 🚨\nGame ID: \`${escapeMarkdownV2(String(gameData.gameId))}\`\nPlayers: ${p1.mention} & ${p2.mention}\nError: ${dbErrorTextForUser}\\. MANUAL BALANCE CHECK/CREDIT REQUIRED for players based on payouts P1: ${p1_payout}, P2: ${p2_payout} lamports\\.`, {parse_mode:'MarkdownV2'});
    }
    
    const p1StatusIcon = p1.status === 'bust' ? "💥 (Busted)" : (p1.status === 'blackjack' ? "✨ (Blackjack!)" : (p1.status === 'stood' ? `(Stood at ${escapeMarkdownV2(String(p1.score))})` : `(Score: ${escapeMarkdownV2(String(p1.score))})`));
    const p2StatusIcon = p2.status === 'bust' ? "💥 (Busted)" : (p2.status === 'blackjack' ? "✨ (Blackjack!)" : (p2.status === 'stood' ? `(Stood at ${escapeMarkdownV2(String(p2.score))})` : `(Score: ${escapeMarkdownV2(String(p2.score))})`));

    const fullResultMessage = 
        `${titleEmoji} **Dice 21 PvP \\- Game Over\\!** ${titleEmoji}\n\n` +
        `--- *Final Standings* ---\n` +
        `👤 ${p1.mention}: *${escapeMarkdownV2(String(p1.score))}* ${p1StatusIcon}\n` +
        `👤 ${p2.mention}: *${escapeMarkdownV2(String(p2.score))}* ${p2StatusIcon}\n\n` +
        `${resultText}` +
        `${dbErrorTextForUser}` + // Will be empty if no DB error
        `\n\n--- *Updated Balances* ---\n` +
        `💰 ${p1.mention}: *${escapeMarkdownV2(await formatBalanceForDisplay(p1_finalBalance, 'USD'))}*\n` +
        `💰 ${p2.mention}: *${escapeMarkdownV2(await formatBalanceForDisplay(p2_finalBalance, 'USD'))}*`;

    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_21_PVP, gameData.betAmount); 
    console.log(`${logPrefix} Sending final PvP game result message for GID ${gameData.gameId}. Snippet: "${fullResultMessage.substring(0,100)}..."`);
    await safeSendMessage(gameData.chatId, fullResultMessage, { 
        parse_mode: 'MarkdownV2', 
        reply_markup: finalKeyboard 
    });
    console.log(`${logPrefix} PvP finalization complete for game ${gameData.gameId}.`);
}
// --- End of Part 5b, Section 2 ---
// --- Start of Part 5c, Section 1 (FULLY UPDATED FOR HELPER BOT DICE ROLLS) ---
// index.js - Part 5c, Section 1: Over/Under 7 Game Logic & Handlers
// (This entire block is placed after Original Part 5b, Section 2 in the new order)
//-------------------------------------------------------------------------------------------------
// Assumed dependencies from previous Parts

// --- Over/Under 7 Game Logic ---

async function handleStartOverUnder7Command(msg, betAmountLamports) {
    // ***** CORRECTED LINE FOR USER ID EXTRACTION *****
    const userId = String(msg.from.id || msg.from.telegram_id);
    // ***** END OF CORRECTION *****
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_OU7_START = `[OU7_Start UID:${userId} CH:${chatId}]`; // Now uses corrected userId

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_OU7_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🎲 Oops! There was an issue with the bet amount for Over/Under 7\\. Please try starting the game again with a valid bet\\.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Apologies! We couldn't fetch your player profile to start Over/Under 7\\. Please try \`/start\` again\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_OU7_START} Initiating Over/Under 7. Bet: ${betAmountLamports}`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino funds are a bit shy for an Over/Under 7 game at *${betDisplayUSD}*\\. You'd need approximately *${neededDisplay}* more\\. Care to top up?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.OVER_UNDER_7);
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_ou7', { game_id_custom_field: gameId },
            `Bet for Over/Under 7 game ${gameId}` 
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_OU7_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Over/Under 7 wager of *${betDisplayUSD}* couldn't be placed due to a hiccup: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports; 
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_OU7_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_OU7_START} Database error during Over/Under 7 bet placement: ${dbError.message}`, dbError.stack?.substring(0,500));
        await safeSendMessage(chatId, "⚙️ A database disturbance prevented the start of your Over/Under 7 game\\. Please try again in a moment\\.", { parse_mode: 'MarkdownV2' });
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
        activeGames.set(gameId, gameData); 
    } else {
        console.error(`${LOG_PREFIX_OU7_START} Failed to send Over/Under 7 game message for ${gameId}. Refunding wager.`);
        let refundClient = null;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_ou7_setup_fail', {}, `Refund OU7 game ${gameId} (message send fail)`);
            await refundClient.query('COMMIT');
        } catch (err) {
            if (refundClient) await refundClient.query('ROLLBACK').catch(rbErr => console.error("Rollback error on OU7 setup refund:", rbErr));
            console.error(`${LOG_PREFIX_OU7_START} CRITICAL: Failed to refund user for OU7 setup fail ${gameId}: ${err.message}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL OU7 REFUND FAILURE 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\` User: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nReason: Failed to send game message AND failed to refund\\. Manual intervention required\\.`, {parse_mode:'MarkdownV2'});
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
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Over/Under 7 game action is outdated or not yours.", show_alert: true }); 
        if (originalMessageIdFromCallback && bot && gameData && gameData.chatId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
            bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(() => {});
        }
        return;
    }
    
    const choiceTextDisplay = choice.charAt(0).toUpperCase() + choice.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, { text: `🎯 Locked In: ${escapeMarkdownV2(choiceTextDisplay)} 7! Requesting dice from Helper Bot...` }).catch(() => {});

    gameData.playerChoice = choice;
    gameData.status = 'rolling_dice_waiting_helper';
    activeGames.set(gameId, gameData); 

    const { chatId, playerRef, betAmount } = gameData;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    
    const titleRolling = createStandardTitle("Over/Under 7 - Dice Rolling via Helper!", "🎲");
    let rollingMessageText = `${titleRolling}\n\n${playerRef} bets *${betDisplayUSD}* on the sum being *${escapeMarkdownV2(choiceTextDisplay)} 7*\\.\nThe Helper Bot is now rolling the dice on the casino network\\.\\.\\. This may take a moment! 🤞`;

    let currentMessageId = gameData.gameMessageId; 
    if (currentMessageId && bot) {
        try {
            await bot.editMessageText(rollingMessageText, { chat_id: String(chatId), message_id: Number(currentMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }); 
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                const newMsg = await safeSendMessage(String(chatId), rollingMessageText, { parse_mode: 'MarkdownV2' });
                if (newMsg?.message_id && activeGames.has(gameId)) { 
                    activeGames.get(gameId).gameMessageId = newMsg.message_id;
                    currentMessageId = newMsg.message_id;
                }
            }
        }
    } else { 
        const newMsg = await safeSendMessage(String(chatId), rollingMessageText, { parse_mode: 'MarkdownV2' });
        if (newMsg?.message_id && activeGames.has(gameId)) {
            activeGames.get(gameId).gameMessageId = newMsg.message_id;
            currentMessageId = newMsg.message_id;
        }
    }

    let diceRolls = [];
    let diceSum = 0;
    let helperBotError = null;

    for (let i = 0; i < OU7_DICE_COUNT; i++) {
        if (isShuttingDown) { helperBotError = "Shutdown during OU7 dice requests."; break; }
        let client = null;
        let requestId = null;
        let currentRollValue = null;

        try {
            client = await pool.connect();
            const requestResult = await insertDiceRollRequest(client, gameId, chatId, userId, '🎲', `OU7 Roll ${i+1}`);
            if (!requestResult.success || !requestResult.requestId) {
                throw new Error(requestResult.error || `Failed to create OU7 roll request ${i+1}.`);
            }
            requestId = requestResult.requestId;
            client.release(); client = null;

            let attempts = 0;
            while (attempts < DICE_ROLL_POLLING_MAX_ATTEMPTS) {
                await sleep(DICE_ROLL_POLLING_INTERVAL_MS);
                if (isShuttingDown) { helperBotError = "Shutdown during OU7 roll poll."; break; }
                client = await pool.connect();
                const statusResult = await getDiceRollRequestResult(client, requestId);
                client.release(); client = null;

                if (statusResult.success && statusResult.status === 'completed') {
                    currentRollValue = statusResult.roll_value; break;
                } else if (statusResult.success && statusResult.status === 'error') {
                    helperBotError = statusResult.notes || `Helper Bot error on OU7 roll ${i+1}.`; break;
                }
                attempts++;
            }
            if (currentRollValue === null && !helperBotError) {
                helperBotError = `Timeout for OU7 roll ${i+1}.`;
                client = await pool.connect();
                await client.query("UPDATE dice_roll_requests SET status='timeout', notes=$1 WHERE request_id=$2", [helperBotError.substring(0,250), requestId]).catch(e => console.error("Failed to mark OU7 request as timeout:", e));
                client.release(); client = null;
            }
            if (helperBotError) throw new Error(helperBotError);
            if (typeof currentRollValue !== 'number') throw new Error(`Invalid roll value from helper for OU7 roll ${i+1}.`);

            diceRolls.push(currentRollValue);
            diceSum += currentRollValue;

        } catch (e) {
            if (client) client.release();
            console.error(`${LOG_PREFIX_OU7_CHOICE} Error getting OU7 roll ${i+1}: ${e.message}`);
            helperBotError = e.message; 
            break; 
        }
        if (helperBotError) break;
    }

    if (helperBotError) {
        const errorMsgToUser = `⚠️ ${playerRef}, there was an issue rolling the dice via the Helper Bot for your Over/Under 7 game: \`${escapeMarkdownV2(String(helperBotError).substring(0,150))}\`\nYour bet of *${betDisplayUSD}* has been refunded\\.`;
        const errorKeyboard = createPostGameKeyboard(GAME_IDS.OVER_UNDER_7, betAmount);
        if (currentMessageId && bot) {
            await bot.editMessageText(errorMsgToUser, { chat_id: String(chatId), message_id: Number(currentMessageId), parse_mode: 'MarkdownV2', reply_markup: errorKeyboard }).catch(async () => {
                 await safeSendMessage(String(chatId), errorMsgToUser, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
            });
        } else {
            await safeSendMessage(String(chatId), errorMsgToUser, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        }
        let refundClient = null;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmount, 'refund_ou7_helper_fail', {game_id_custom_field: gameId}, `Refund OU7 game ${gameId} - Helper Bot error: ${String(helperBotError).substring(0,100)}`);
            await refundClient.query('COMMIT');
        } catch (dbErr) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_OU7_CHOICE} CRITICAL: Failed to refund after OU7 helper error for game ${gameId}: ${dbErr.message}`);
            notifyAdmin(`🚨 CRITICAL OU7 REFUND FAILURE (Helper Error) 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`, User: ${userId}\nError: ${escapeMarkdownV2(helperBotError)}\nDB Refund Error: ${escapeMarkdownV2(dbErr.message)}. MANUAL REFUND REQUIRED.`);
        } finally {
            if (refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
        return;
    }
    
    gameData.diceRolls = diceRolls;
    gameData.diceSum = BigInt(diceSum);
    gameData.status = 'game_over';
    activeGames.set(gameId, gameData); 

    let win = false;
    let profitMultiplier = 0; 
    if (choice === 'under' && diceSum < 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
    else if (choice === 'over' && diceSum > 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
    else if (choice === 'seven' && diceSum === 7) { win = true; profitMultiplier = OU7_PAYOUT_SEVEN; }

    let payoutAmountLamports = 0n; 
    let outcomeReasonLog = `loss_ou7_${choice}_sum${diceSum}`; // Default to loss
    let resultTextPart = "";
    const profitAmountLamports = win ? betAmount * BigInt(Math.floor(profitMultiplier)) : 0n; 

    if (win) {
        payoutAmountLamports = betAmount + profitAmountLamports; 
        outcomeReasonLog = `win_ou7_${choice}_sum${diceSum}`; 
        const winEmoji = choice === 'seven' ? "🎯 JACKPOT!" : "🎉 WINNER!";
        resultTextPart = `${winEmoji} Your prediction of *${escapeMarkdownV2(choiceTextDisplay)} 7* was spot on! You've won a handsome *${escapeMarkdownV2(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}* in profit!`;
    } else {
        payoutAmountLamports = 0n; 
        resultTextPart = `💔 *So Close!* The dice didn't favor your prediction of *${escapeMarkdownV2(choiceTextDisplay)} 7* this round\\. Better luck next time!`;
    }

    let finalUserBalanceLamports = BigInt(userObj.balance); 
    let clientOutcome = null;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, 
            userId, 
            payoutAmountLamports, 
            outcomeReasonLog, 
            { 
                game_id_custom_field: gameId, 
                dice_rolls_info: diceRolls.join(','), 
                player_choice_info: choice 
            }, 
            `Outcome of OU7 game ${gameId}. Player chose ${choice}, sum was ${diceSum}. Rolls: ${diceRolls.join(',')}.`
        );
        
        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
        } else {
            await clientOutcome.query('ROLLBACK');
            resultTextPart += `\n\n⚠️ A critical error occurred settling your bet: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Our casino staff has been alerted\\.`;
            console.error(`${LOG_PREFIX_OU7_CHOICE} Failed to update balance for OU7 game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL OU7 Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\`\nUser: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmountLamports))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_OU7_CHOICE} DB error during OU7 outcome processing for ${gameId}: ${dbError.message}`, dbError.stack?.substring(0,500));
        resultTextPart += `\n\n⚠️ A severe database error occurred\\. Our casino staff has been notified\\.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    const titleResult = createStandardTitle("Over/Under 7 - Result!", "🏁");
    let finalMessageText = `${titleResult}\n\nYour Bet: *${betDisplayUSD}* on *${escapeMarkdownV2(choiceTextDisplay)} 7*\\.\n\n`;
    finalMessageText += `The Helper Bot rolled: ${formatDiceRolls(diceRolls)} for a grand total of *${escapeMarkdownV2(String(diceSum))}*!${'\\!'}\n\n${resultTextPart}`; 
    finalMessageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*\\.`; 

    const postGameKeyboardOU7 = createPostGameKeyboard(GAME_IDS.OVER_UNDER_7, betAmount);

    if (currentMessageId && bot) { 
        try {
            await bot.editMessageText(finalMessageText, { chat_id: String(chatId), message_id: Number(currentMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
            }
        }
    } else { 
        await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
    }
    activeGames.delete(gameId); 
}

// --- End of Part 5c, Section 1 (NEW) ---
// --- Start of Part 5c, Section 2 (COMPLETE REWRITE FOR NEW DUEL GAME LOGIC) ---
// index.js - Part 5c, Section 2: Duel Game Logic & Handlers (PvP/PvB Unified Offer Style)
// (This entire block is placed after Original Part 5c, Section 1 in the new order)
//-------------------------------------------------------------------------------------
// Assumed dependencies: GAME_IDS, DUEL_DICE_COUNT (should be 2), JOIN_GAME_TIMEOUT_MS,
// getOrCreateUser, getPlayerDisplayReference, formatBalanceForDisplay, parseBetAmount,
// updateUserBalanceAndLedger, generateGameId, safeSendMessage, createPostGameKeyboard,
// escapeMarkdownV2, pool, activeGames, groupGameSessions, updateGroupGameDetails,
// DICE_ROLL_POLLING_MAX_ATTEMPTS, DICE_ROLL_POLLING_INTERVAL_MS, sleep,
// QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, formatDiceRolls, notifyAdmin,
// insertDiceRollRequest, getDiceRollRequestResult (or a similar helper like getSingleDiceRollViaHelperDuel)
// BOT_NAME, LAMPORTS_PER_SOL.

// --- Helper function for a single dice roll via Helper Bot for Duel ---
// Similar to the one in D21, adapted for Duel specific logging/notes if needed.
async function getSingleDiceRollViaHelperDuel(gameId, chatIdForLog, userIdForRoll, rollPurposeNote) {
    const logPrefix = `[Duel_GetSingleRoll GID:${gameId} Purpose:"${rollPurposeNote}" UID:${userIdForRoll || 'BOT_INTERNAL'}]`;
    // console.log(`${logPrefix} Requesting die from Helper Bot.`); // Reduced log
    let client = null;
    let requestId = null;
    let specificErrorMessage = `Failed to obtain dice roll for "${rollPurposeNote}" via Helper Bot.`;
    let isTimeoutErrorFlag = false;

    try {
        client = await pool.connect();
        const requestResult = await insertDiceRollRequest(client, gameId, String(chatIdForLog), userIdForRoll, '🎲', rollPurposeNote);
        if (!requestResult.success || !requestResult.requestId) {
            specificErrorMessage = requestResult.error || `Database error when creating roll request for "${rollPurposeNote}".`;
            console.error(`${logPrefix} ${specificErrorMessage}`);
            throw new Error(specificErrorMessage);
        }
        requestId = requestResult.requestId;
        // console.log(`${logPrefix} Roll request successfully created in DB (Request ID: ${requestId}).`); // Reduced log
        client.release(); client = null;

        let attempts = 0;
        // console.log(`${logPrefix} Starting to poll for roll result (Request ID: ${requestId}). Max attempts: ${DICE_ROLL_POLLING_MAX_ATTEMPTS}. Interval: ${DICE_ROLL_POLLING_INTERVAL_MS}ms.`); // Reduced log
        while (attempts < DICE_ROLL_POLLING_MAX_ATTEMPTS) {
            await sleep(DICE_ROLL_POLLING_INTERVAL_MS);
            if (isShuttingDown) {
                specificErrorMessage = "System shutdown initiated while waiting for Helper Bot dice roll response.";
                console.warn(`${logPrefix} ${specificErrorMessage}`);
                throw new Error(specificErrorMessage);
            }

            client = await pool.connect();
            const statusResult = await getDiceRollRequestResult(client, requestId);
            client.release(); client = null;

            if (statusResult.success && statusResult.status === 'completed') {
                if (typeof statusResult.roll_value === 'number' && statusResult.roll_value >= 1 && statusResult.roll_value <= 6) {
                    // console.log(`${logPrefix} Successfully received roll value from Helper Bot: ${statusResult.roll_value} (Request ID: ${requestId}).`); // Reduced log
                    return { roll: statusResult.roll_value, error: false }; // SUCCESS
                } else {
                    specificErrorMessage = `Helper Bot returned a completed roll for "${rollPurposeNote}" (Request ID: ${requestId}), but the dice value was invalid: '${statusResult.roll_value}'.`;
                    console.error(`${logPrefix} ${specificErrorMessage}`);
                    throw new Error(specificErrorMessage);
                }
            } else if (statusResult.success && statusResult.status === 'error') {
                specificErrorMessage = statusResult.notes || `Helper Bot explicitly reported an error for "${rollPurposeNote}" (Request ID: ${requestId}).`;
                console.error(`${logPrefix} ${specificErrorMessage}`);
                throw new Error(specificErrorMessage);
            }
            attempts++;
        }
        
        isTimeoutErrorFlag = true; 
        specificErrorMessage = `Timeout after ${attempts} attempts waiting for Helper Bot response for dice roll: "${rollPurposeNote}" (Request ID: ${requestId}).`;
        throw new Error(specificErrorMessage); 

    } catch (error) {
        if (client) client.release();
        
        const finalErrorMessageForReturn = error.message || specificErrorMessage;
        console.error(`${logPrefix} Final error state in getSingleDiceRollViaHelperDuel: ${finalErrorMessageForReturn}`);
        
        if (requestId) { 
            let markErrorClient = null;
            try {
                markErrorClient = await pool.connect();
                const statusToUpdate = isTimeoutErrorFlag ? 'timeout' : 'error';
                // console.log(`${logPrefix} Attempting to mark roll request ${requestId} as '${statusToUpdate}' in DB due to error: ${finalErrorMessageForReturn.substring(0,100)}`); // Reduced log
                await markErrorClient.query("UPDATE dice_roll_requests SET status=$1, notes=$2 WHERE request_id=$3 AND status = 'pending'", 
                    [statusToUpdate, String(finalErrorMessageForReturn).substring(0,250), requestId]); 
            } catch (dbMarkError) {
                console.error(`${logPrefix} CRITICAL: Failed to mark roll request ${requestId} as failed in DB: ${dbMarkError.message}`);
            } finally {
                if (markErrorClient) markErrorClient.release();
            }
        }
        return { error: true, message: finalErrorMessageForReturn, isTimeout: isTimeoutErrorFlag }; 
    }
}


// --- Duel Main Command Handler (Creates Unified Offer in Group Chat) ---
async function handleStartDuelUnifiedOfferCommand(msg, betAmountLamports) {
    // ***** CORRECTED LINE FOR USER ID EXTRACTION *****
    const userId = String(msg.from.id || msg.from.telegram_id);
    // ***** END OF CORRECTION *****
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const logPrefix = `[Duel_StartOffer UID:${userId} CH:${chatId} Type:${chatType}]`; // Use corrected userId

    if (chatType === 'private') {
        console.log(`${logPrefix} Duel command used in private chat. Informing user game is group-only.`);
        await safeSendMessage(chatId, `⚔️ Greetings, Duelist! The High Roller Duel game, with its thrilling PvP and PvB options, can only be initiated in a **group chat**. Please use the \`/duel <bet>\` command there to lay down your challenge!`, { parse_mode: 'MarkdownV2' });
        return;
    }

    console.log(`${logPrefix} Command /duel received. Bet: ${betAmountLamports}`);

    // Ensure initiatorUserObj is created *after* userId is correctly extracted
    let initiatorUserObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!initiatorUserObj) {
        console.warn(`${logPrefix} Failed to get/create user object for ID: ${userId}.`);
        await safeSendMessage(chatId, "Apologies, your player profile couldn't be accessed. Please use \`/start\` with me first, then try \`/duel\` again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(initiatorUserObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    const gameSession = await getGroupSession(chatId, msg.chat.title || `Group Chat ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        // Check for any non-single player game offer or active PvP game
        if ( ([GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER, GAME_IDS.DICE_21_UNIFIED_OFFER, GAME_IDS.DUEL_UNIFIED_OFFER, GAME_IDS.COINFLIP, GAME_IDS.RPS].includes(existingGame.type) && 
             (existingGame.status === 'pending_offer' || existingGame.status === 'waiting_opponent' || existingGame.status === 'waiting_for_choice' || existingGame.status === 'waiting_choices')) ||
             ((existingGame.type === GAME_IDS.DICE_21_PVP || existingGame.type === GAME_IDS.DICE_ESCALATOR_PVP || existingGame.type === GAME_IDS.DUEL_PVP) && !existingGame.status.startsWith('game_over_'))
           ) {
            console.log(`${logPrefix} Another interactive game offer or active PvP game (ID: ${gameSession.currentGameId}, Type: ${existingGame.type}) is already active.`);
            await safeSendMessage(chatId, `⏳ Hold your weapons, ${playerRef}! Another game offer or an active Player vs Player match is currently underway in this group. Please wait for it to conclude before initiating a new Duel.`, { parse_mode: 'MarkdownV2' });
            return;
        }
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        console.log(`${logPrefix} Initiator ${playerRef} has insufficient balance for ${betDisplayUSD} bet.`);
        await safeSendMessage(chatId, `${playerRef}, your treasury is too light for a *${betDisplayUSD}* Duel! You need about *${escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'))}* more.`, {
            parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: "💰 Top Up Balance (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const offerId = generateGameId(GAME_IDS.DUEL_UNIFIED_OFFER);
    const offerMessageText = 
        `⚔️ **A High Roller Duel Challenge!** ⚔️\n\n` +
        `${playerRef} has thrown down the gauntlet for a Duel, staking *${betDisplayUSD}*!\n\n` +
        `Will another duelist accept the challenge for a Player vs. Player showdown?\n` +
        `Or will ${playerRef} face the casino's own Bot Dealer?`;
        
    const offerKeyboard = {
        inline_keyboard: [
            [{ text: "🤝 Accept PvP Duel!", callback_data: `duel_accept_pvp_challenge:${offerId}` }],
            [{ text: "🤖 Challenge the Bot Dealer", callback_data: `duel_accept_bot_game:${offerId}` }],
            [{ text: "🚫 Withdraw My Challenge (Initiator)", callback_data: `duel_cancel_unified_offer:${offerId}` }]
        ]
    };

    const offerData = {
        type: GAME_IDS.DUEL_UNIFIED_OFFER, gameId: offerId, chatId: String(chatId), chatType,
        initiatorId: userId, initiatorMention: playerRef, initiatorUserObj, // Store the fetched initiatorUserObj
        betAmount: betAmountLamports, status: 'waiting_for_choice', 
        creationTime: Date.now(), gameSetupMessageId: null 
    };
    activeGames.set(offerId, offerData);
    await updateGroupGameDetails(chatId, offerId, GAME_IDS.DUEL_UNIFIED_OFFER, betAmountLamports);

    console.log(`${logPrefix} Sending Duel unified offer (ID: ${offerId}) to chat ${chatId}.`);
    const sentOfferMessage = await safeSendMessage(chatId, offerMessageText, { parse_mode: 'MarkdownV2', reply_markup: offerKeyboard });
    
    if (sentOfferMessage?.message_id) {
        const offerInMap = activeGames.get(offerId);
        if(offerInMap) {
            offerInMap.gameSetupMessageId = sentOfferMessage.message_id;
            activeGames.set(offerId, offerInMap);
        } else { // Should not happen if set right before
             console.warn(`${logPrefix} Offer ${offerId} vanished from activeGames immediately after message ID set.`);
             if (bot) await bot.deleteMessage(chatId, sentOfferMessage.message_id).catch(()=>{});
        }
    } else {
        console.error(`${logPrefix} CRITICAL: Failed to send Duel unified offer message for offer ID ${offerId}. Cleaning up.`);
        activeGames.delete(offerId); 
        await updateGroupGameDetails(chatId, null, null, null); 
        await safeSendMessage(chatId, `An error prevented the Duel offer by ${playerRef} from being created. Please try again.`, {parse_mode: 'MarkdownV2'});
        return;
    }

    setTimeout(async () => {
        const currentOfferData = activeGames.get(offerId);
        if (currentOfferData && currentOfferData.status === 'waiting_for_choice') {
            console.log(`[Duel_OfferTimeout OfferID:${offerId}] Unified Duel offer expired.`);
            activeGames.delete(offerId);
            await updateGroupGameDetails(chatId, null, null, null);
            
            if (currentOfferData.gameSetupMessageId && bot) {
                const expiredOfferBetDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(currentOfferData.betAmount, 'USD'));
                const offerExpiredMessageText = `⏳ The Duel challenge by ${currentOfferData.initiatorMention} for *${expiredOfferBetDisplayUSD}* has timed out. This challenge is now closed.`;
                
                await bot.editMessageText(offerExpiredMessageText, {
                    chat_id: String(chatId), message_id: Number(currentOfferData.gameSetupMessageId), 
                    parse_mode: 'MarkdownV2', reply_markup: {} 
                }).catch(e => console.error(`${logPrefix} Error editing message for expired Duel offer (ID: ${currentOfferData.gameSetupMessageId}): ${e.message}`));
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

// --- Callback Handlers for Duel Unified Offer ---
async function handleDuelAcceptBotGameCallback(offerId, initiatorUserObjFromCb, originalOfferMessageId, originalChatId, originalChatType, callbackQueryIdPassed = null) { // Added callbackQueryIdPassed
    const initiatorId = String(initiatorUserObjFromCb.id || initiatorUserObjFromCb.telegram_id);
    const logPrefix = `[Duel_AcceptBotCB GID:${offerId} UID:${initiatorId}]`;
    const offerData = activeGames.get(offerId);
    const callbackQueryId = callbackQueryIdPassed; // Use the passed ID

    if (!offerData || offerData.type !== GAME_IDS.DUEL_UNIFIED_OFFER) {
        console.warn(`${logPrefix} Invalid or non-existent Duel offer (ID: ${offerId}).`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Duel offer seems to have expired or is invalid.", show_alert: true }).catch(()=>{});
        else if (offerData?.gameSetupMessageId && bot) await bot.editMessageReplyMarkup({}, {chat_id:originalChatId, message_id:Number(offerData.gameSetupMessageId)}).catch(()=>{});
        return;
    }
    if (offerData.initiatorId !== initiatorId) {
        console.warn(`${logPrefix} User ${initiatorId} tried to accept PvB for offer by ${offerData.initiatorId}. Denied.`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Only the challenger can start this Duel against the Bot!", show_alert: true }).catch(()=>{});
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
        console.warn(`${logPrefix} Duel offer ${offerId} not in 'waiting_for_choice' status (is ${offerData.status}).`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Duel offer has already been actioned or timed out.", show_alert: true }).catch(()=>{});
         if (bot && offerData.gameSetupMessageId) {
            await bot.editMessageText(`This Duel offer by ${offerData.initiatorMention} has already been actioned or timed out.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(()=>{});
        }
        return;
    }
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Initiating Duel vs Bot Dealer..."}).catch(()=>{});

    console.log(`${logPrefix} Initiator ${offerData.initiatorMention} selected PvB Duel from offer ${offerId}.`);
    await startDuelPvBGame(
        originalChatId, 
        offerData.initiatorUserObj, // Pass the full user object stored in offerData
        offerData.betAmount, 
        Number(offerData.gameSetupMessageId || originalOfferMessageId), 
        offerId 
    );
}

async function handleDuelAcceptPvPChallengeCallback(offerId, joinerUserObjFromCb, originalOfferMessageId, originalChatId, originalChatType, callbackQueryIdPassed = null) { // Added callbackQueryIdPassed
    const joinerId = String(joinerUserObjFromCb.id || joinerUserObjFromCb.telegram_id);
    const logPrefix = `[Duel_AcceptPvPCB GID:${offerId} JoinerID:${joinerId}]`;
    let offerData = activeGames.get(offerId);
    const joinerRef = getPlayerDisplayReference(joinerUserObjFromCb);
    const callbackQueryId = callbackQueryIdPassed; // Use the passed ID

    if (!offerData || offerData.type !== GAME_IDS.DUEL_UNIFIED_OFFER) {
        console.warn(`${logPrefix} PvP accept for invalid/non-existent Duel offer (ID: ${offerId}).`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Duel offer has expired or is invalid.", show_alert: true }).catch(()=>{});
        else if (offerData?.gameSetupMessageId && bot) await bot.editMessageReplyMarkup({}, {chat_id:originalChatId, message_id:Number(offerData.gameSetupMessageId)}).catch(()=>{});
        return;
    }
    if (offerData.initiatorId === joinerId) {
        console.warn(`${logPrefix} Initiator ${joinerRef} attempted to accept own PvP Duel challenge. Denied.`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "You can't duel yourself in this manner!", show_alert: true }).catch(()=>{});
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
        console.warn(`${logPrefix} Duel offer ${offerId} not 'waiting_for_choice' (is ${offerData.status}). ${joinerRef} cannot join.`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Duel offer isn't available to join right now.", show_alert: true }).catch(()=>{});
        if (bot && offerData.gameSetupMessageId) {
            await bot.editMessageText(`This Duel offer by ${offerData.initiatorMention} is no longer available to be joined for PvP.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(()=>{});
        }
        return;
    }

    const betAmount = offerData.betAmount;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));

    let currentJoinerUserObj = await getOrCreateUser(joinerId, joinerUserObjFromCb.username, joinerUserObjFromCb.first_name, joinerUserObjFromCb.last_name);
    if (!currentJoinerUserObj || BigInt(currentJoinerUserObj.balance) < betAmount) {
        console.log(`${logPrefix} Joiner ${joinerRef} has insufficient balance for PvP Duel.`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: `Not enough funds for this ${betDisplayUSD} duel. Top up!`, show_alert: true }).catch(()=>{});
        await safeSendMessage(originalChatId, `${joinerRef}, your balance is too low for this *${betDisplayUSD}* Duel. Top up your funds!`, {
            parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    let currentInitiatorUserObj = offerData.initiatorUserObj || await getOrCreateUser(offerData.initiatorId); // Use stored obj if available
    if (!currentInitiatorUserObj || BigInt(currentInitiatorUserObj.balance) < betAmount) {
        console.warn(`${logPrefix} Initiator ${offerData.initiatorMention} no longer has funds for PvP Duel. Cancelling offer ${offerId}.`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Initiator can't cover the bet. Offer cancelled.", show_alert: true }).catch(()=>{});
        if (offerData.gameSetupMessageId && bot) {
            await bot.editMessageText(`The Duel offer from ${offerData.initiatorMention} for *${betDisplayUSD}* was cancelled as they no longer have sufficient funds.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(()=>{});
        }
        activeGames.delete(offerId); 
        await updateGroupGameDetails(originalChatId, null, null, null);
        return;
    }
    
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "Challenge Accepted! Setting up PvP Duel..."}).catch(()=>{});

    if (offerData.gameSetupMessageId && bot) {
        await bot.deleteMessage(originalChatId, Number(offerData.gameSetupMessageId)).catch(e => console.warn(`${logPrefix} Non-critical: Could not delete unified Duel offer message (ID: ${offerData.gameSetupMessageId})`));
    }
    
    let client;
    const pvpGameId = generateGameId(GAME_IDS.DUEL_PVP);

    try {
        client = await pool.connect(); await client.query('BEGIN');
        const initBetRes = await updateUserBalanceAndLedger(client, offerData.initiatorId, BigInt(-betAmount), 
            'bet_placed_duel_pvp_init', { game_id_custom_field: pvpGameId, opponent_id: joinerId }, 
            `Initiator bet for PvP Duel ${pvpGameId} vs ${joinerRef}`);
        if (!initBetRes.success) throw new Error(`Initiator bet failed: ${initBetRes.error}`);
        currentInitiatorUserObj.balance = initBetRes.newBalanceLamports;

        const joinBetRes = await updateUserBalanceAndLedger(client, joinerId, BigInt(-betAmount), 
            'bet_placed_duel_pvp_join', { game_id_custom_field: pvpGameId, opponent_id: offerData.initiatorId }, 
            `Joiner bet for PvP Duel ${pvpGameId} vs ${offerData.initiatorMention}`);
        if (!joinBetRes.success) throw new Error(`Joiner bet failed: ${joinBetRes.error}`);
        currentJoinerUserObj.balance = joinBetRes.newBalanceLamports;
        
        await client.query('COMMIT');

        const pvpGameData = {
            type: GAME_IDS.DUEL_PVP, gameId: pvpGameId, chatId: String(offerData.chatId), chatType: offerData.chatType,
            betAmount: offerData.betAmount,
            initiator: { 
                userId: offerData.initiatorId, mention: offerData.initiatorMention, userObj: currentInitiatorUserObj, 
                rolls: [], score: 0, isTurn: false, status: 'waiting_turn' 
            },
            opponent: { 
                userId: joinerId, mention: joinerRef, userObj: currentJoinerUserObj, 
                rolls: [], score: 0, isTurn: false, status: 'waiting_turn'
            },
            status: 'p1_awaiting_roll1_emoji', 
            creationTime: Date.now(), currentMessageId: null, lastInteractionTime: Date.now()
        };
        pvpGameData.initiator.isTurn = true; 

        activeGames.set(pvpGameId, pvpGameData);      
        activeGames.delete(offerId);                     
        await updateGroupGameDetails(originalChatId, pvpGameId, GAME_IDS.DUEL_PVP, betAmount);

        console.log(`${logPrefix} PvP Duel game ${pvpGameId} created. Initiating player turns.`);
        await updateDuelPvPMessage(pvpGameId, true); 

    } catch (e) { 
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} CRITICAL error creating PvP Duel game ${pvpGameId} from offer ${offerId}: ${e.message}`);
        await safeSendMessage(originalChatId, `A critical server error occurred starting the PvP Duel: \`${escapeMarkdownV2(e.message)}\`. Bets might have been affected. Admins notified.`, { parse_mode: 'MarkdownV2'});
        activeGames.delete(offerId); 
        if(activeGames.has(pvpGameId)) activeGames.delete(pvpGameId);
        await updateGroupGameDetails(originalChatId, null, null, null); 
        if (typeof notifyAdmin === 'function') {
            notifyAdmin(`🚨 CRITICAL Duel PvP Start Failure 🚨\nGame ID: \`${pvpGameId}\` (Offer: \`${offerId}\`)\nError: ${e.message}. Bets might be taken. MANUAL CHECK/REFUND REQUIRED.`);
        }
        return;
    } finally { 
        if (client) client.release(); 
    }
}

async function handleDuelCancelUnifiedOfferCallback(offerId, initiatorUserObjFromCb, originalOfferMessageId, originalChatId, callbackQueryIdPassed = null) { // Added callbackQueryIdPassed
    const initiatorId = String(initiatorUserObjFromCb.id || initiatorUserObjFromCb.telegram_id);
    const logPrefix = `[Duel_CancelOfferCB GID:${offerId} UID:${initiatorId}]`;
    const offerData = activeGames.get(offerId);
    const callbackQueryId = callbackQueryIdPassed; // Use the passed ID

    if (!offerData || offerData.type !== GAME_IDS.DUEL_UNIFIED_OFFER) {
        console.warn(`${logPrefix} Attempt to cancel invalid/non-existent Duel offer (ID: ${offerId}).`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This offer is no longer valid.", show_alert: false }).catch(()=>{});
        else if (offerData?.gameSetupMessageId && bot) await bot.editMessageReplyMarkup({}, {chat_id:originalChatId, message_id:Number(offerData.gameSetupMessageId)}).catch(()=>{});
        return;
    }
    if (offerData.initiatorId !== initiatorId) {
        console.warn(`${logPrefix} User ${initiatorId} tried to cancel offer by ${offerData.initiatorId}. Denied.`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Only the challenger can withdraw this offer.", show_alert: true }).catch(()=>{});
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
        console.warn(`${logPrefix} Duel offer ${offerId} not 'waiting_for_choice' (is ${offerData.status}). Cannot cancel now.`);
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This offer has already been actioned or expired.", show_alert: false }).catch(()=>{});
        if (bot && offerData.gameSetupMessageId) {
            await bot.editMessageText(`This Duel offer by ${offerData.initiatorMention} is no longer pending cancellation.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(()=>{});
        }
        return;
    }
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Duel offer cancelled."}).catch(()=>{});

    console.log(`${logPrefix} Initiator ${offerData.initiatorMention} cancelling Duel offer (ID: ${offerId}).`);
    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null); 

    const msgIdToEdit = offerData.gameSetupMessageId || originalOfferMessageId; 
    if (msgIdToEdit && bot) {
        const cancelMessageText = `🚫 The Duel challenge by ${offerData.initiatorMention} has been withdrawn by the initiator.`;
        await bot.editMessageText(cancelMessageText, {
            chat_id: String(originalChatId), message_id: Number(msgIdToEdit), 
            parse_mode: 'MarkdownV2', reply_markup: {} 
        }).catch(e => console.error(`${logPrefix} Error editing message for cancelled Duel offer (ID: ${msgIdToEdit}): ${e.message}`));
    }
}

// --- Player vs. Bot (PvB) Duel Game Logic ---
async function startDuelPvBGame(chatId, initiatorUserObj, betAmountLamports, originalOfferMessageIdToDelete, unifiedOfferIdIfAny) {
    const userId = String(initiatorUserObj.telegram_id);
    const logPrefix = `[Duel_PvB_Start UID:${userId} CH:${chatId}]`;
    const playerRef = getPlayerDisplayReference(initiatorUserObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    console.log(`${logPrefix} Starting PvB Duel. Bet: ${betDisplayUSD}. Offer ID to remove: ${unifiedOfferIdIfAny}`);

    if (unifiedOfferIdIfAny && originalOfferMessageIdToDelete && bot) {
        await bot.deleteMessage(chatId, Number(originalOfferMessageIdToDelete))
            .catch(e => console.warn(`${logPrefix} Non-critical: Could not delete unified Duel offer message ${originalOfferMessageIdToDelete}: ${e.message}`));
    }
    
    let client;
    const gameId = generateGameId(GAME_IDS.DUEL_PVB);
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const currentBalance = await getUserBalance(userId); 
        if (BigInt(currentBalance) < betAmountLamports) {
             throw new Error (`User ${userId} balance ${currentBalance} insufficient for bet ${betAmountLamports} at actual PvB start.`);
        }

        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, BigInt(-betAmountLamports), 
            'bet_placed_duel_pvb', { game_id_custom_field: gameId }, 
            `Bet placed for PvB Duel game ${gameId}`);
        if (!balanceUpdateResult.success) {
            throw new Error(balanceUpdateResult.error || "DB error: PvB Duel wager placement failed.");
        }
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports;
        await client.query('COMMIT');
        console.log(`${logPrefix} Bet of ${betAmountLamports} confirmed/re-validated for ${playerRef}. New balance: ${initiatorUserObj.balance}.`);

    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} Error starting PvB Duel game (bet placement or check): ${e.message}`);
        await safeSendMessage(chatId, `${playerRef}, a critical error occurred starting your Duel vs Bot: \`${escapeMarkdownV2(e.message)}\`. Please try again.`, {parse_mode: 'MarkdownV2'});
        if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) activeGames.delete(unifiedOfferIdIfAny);
        activeGames.delete(gameId); // Clean up if gameId was generated
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    } finally {
        if (client) client.release();
    }

    if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) {
        activeGames.delete(unifiedOfferIdIfAny);
    }
    
    const gameData = {
        type: GAME_IDS.DUEL_PVB, gameId, chatId: String(chatId), chatType: 'group', 
        playerId: userId, playerRef, userObj: initiatorUserObj, betAmount: betAmountLamports,
        playerRolls: [], playerScore: 0, botRolls: [], botScore: 0,
        status: 'player_awaiting_roll1_emoji', 
        gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);
    await updateGroupGameDetails(chatId, gameId, GAME_IDS.DUEL_PVB, betAmountLamports);

    const initialMessageText = 
        `⚔️ **Duel vs. Bot Dealer!** ⚔️\n\n` +
        `${playerRef}, your wager: *${betDisplayUSD}*.\n` +
        `It's your turn to roll! Please send **two separate** 🎲 dice emojis to the chat.`;
        
    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2' });
    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
    } else {
        console.error(`${logPrefix} Failed to send initial PvB Duel message for ${gameId}. Refunding.`);
        await refundDuelPvBBetsGeneric(gameId, userId, betAmountLamports, "PvB Duel setup message fail", logPrefix);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
}

// Called after player sends their second emoji in PvB
async function processDuelPlayerRollsCompletePvB(gameData, firstRoll, secondRoll) {
    const { gameId, chatId, playerRef, betAmount, userObj } = gameData;
    const logPrefix = `[Duel_PvB_PlayerDone GID:${gameId} UID:${userObj.telegram_id}]`;
    
    gameData.playerRolls = [firstRoll, secondRoll];
    gameData.playerScore = firstRoll + secondRoll;
    gameData.status = 'bot_rolling_internal'; 
    gameData.lastInteractionTime = Date.now();
    activeGames.set(gameId, gameData);

    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    const playerRollsDisplay = formatDiceRolls(gameData.playerRolls);
    
    if (gameData.gameMessageId && bot) {
        await bot.deleteMessage(chatId, Number(gameData.gameMessageId)).catch(e => console.warn(`${logPrefix} Non-critical fail to delete old msg ${gameData.gameMessageId}: ${e.message}`));
    }

    let messageText = 
        `⚔️ **Duel vs. Bot Dealer!** ⚔️\n\n` +
        `Wager: *${betDisplayUSD}*\n\n` +
        `${playerRef} rolled: ${playerRollsDisplay} for a total of *${escapeMarkdownV2(String(gameData.playerScore))}*!\n\n` +
        `Now, the Bot Dealer takes its turn. Requesting two dice from the Helper Bot... 🤖🎲🎲`;

    const sentMessage = await safeSendMessage(chatId, messageText, { parse_mode: 'MarkdownV2' });
    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
    } else {
        console.error(`${logPrefix} Failed to send player rolls complete / bot turn message. Game ${gameId} might be stuck.`);
        await refundDuelPvBBetsGeneric(gameId, userObj.telegram_id, betAmount, "PvB UI fail before bot turn", logPrefix);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }
    
    await sleep(1500); 
    await processDuelBotTurnPvB(gameData);
}

async function processDuelBotTurnPvB(gameData) {
    const { gameId, chatId, playerRef, betAmount, playerScore, userObj } = gameData;
    const logPrefix = `[Duel_PvB_BotTurn GID:${gameId}]`;
    console.log(`${logPrefix} Bot's turn. Player score: ${playerScore}`);

    const botRollsResult = await getTwoDiceRollsViaHelperDuel(gameId, chatId, null, "Duel PvB Bot Roll");

    if (botRollsResult.error) {
        console.error(`${logPrefix} Bot failed to get its rolls: ${botRollsResult.message}. Refunding player.`);
        if (gameData.gameMessageId && bot) {
            await bot.deleteMessage(chatId, Number(gameData.gameMessageId)).catch(e => console.warn(`${logPrefix} Non-critical fail to delete old msg ${gameData.gameMessageId} on bot roll error: ${e.message}`));
        }
        const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
        const errorMsg = `${playerRef}, the Bot Dealer encountered an issue getting its dice: \`${escapeMarkdownV2(botRollsResult.message.substring(0,100))}\`. Your *${betDisplayUSD}* wager is refunded.`;
        await safeSendMessage(chatId, errorMsg, {parse_mode: 'MarkdownV2', reply_markup: createPostGameKeyboard(GAME_IDS.DUEL_PVB, betAmount)});
        
        await refundDuelPvBBetsGeneric(gameId, userObj.telegram_id, betAmount, "PvB Bot roll helper error", logPrefix);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }

    gameData.botRolls = botRollsResult.rolls;
    gameData.botScore = botRollsResult.rolls.reduce((sum, val) => sum + val, 0);
    gameData.status = 'game_over_pvb_resolved';
    activeGames.set(gameId, gameData);

    await finalizeDuelPvBGame(gameData);
}

async function finalizeDuelPvBGame(gameData) {
    const { gameId, chatId, playerId, playerRef, playerScore, botScore, betAmount, userObj, gameMessageId, playerRolls, botRolls } = gameData;
    const logPrefix = `[Duel_PvB_Finalize GID:${gameId} UID:${playerId}]`;

    let resultTextPart = "";
    let payoutAmountLamports = 0n;
    let ledgerOutcomeCode = "";

    if (playerScore > botScore) {
        resultTextPart = `🎉 **VICTORY, ${playerRef}!** 🎉\nYour score of *${playerScore}* triumphs over the Bot's *${botScore}*!`;
        payoutAmountLamports = betAmount * 2n; 
        ledgerOutcomeCode = 'win_duel_pvb';
    } else if (botScore > playerScore) {
        resultTextPart = `💔 **Defeat this time, ${playerRef}.** 💔\nThe Bot Dealer's *${botScore}* bests your *${playerScore}*.`;
        payoutAmountLamports = 0n; 
        ledgerOutcomeCode = 'loss_duel_pvb';
    } else {
        resultTextPart = `🛡️ **A Draw!** 🛡️\nBoth you and the Bot Dealer scored *${playerScore}*! Your wager is returned.`;
        payoutAmountLamports = betAmount; 
        ledgerOutcomeCode = 'push_duel_pvb';
    }

    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
    let finalMessageText = 
        `⚔️ **Duel vs. Bot Dealer - Result!** ⚔️\n\n` +
        `Wager: *${betDisplayUSD}*\n\n` +
        `${playerRef} rolled: ${formatDiceRolls(playerRolls)} (Total: *${escapeMarkdownV2(String(playerScore))}*)\n` +
        `Bot Dealer rolled: ${formatDiceRolls(botRolls)} (Total: *${escapeMarkdownV2(String(botScore))}*)\n\n` +
        `------------------------------------\n${resultTextPart}`;

    let finalUserBalanceLamports = BigInt(userObj.balance);
    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const balanceUpdate = await updateUserBalanceAndLedger(client, playerId, payoutAmountLamports, 
            ledgerOutcomeCode, { game_id_custom_field: gameId, player_score_val: playerScore, bot_score_val: botScore }, 
            `PvB Duel game ${gameId} result`);
        
        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await client.query('COMMIT');
            if (payoutAmountLamports > betAmount && ledgerOutcomeCode === 'win_duel_pvb') {
                const profit = payoutAmountLamports - betAmount;
                finalMessageText += `\nYou win *${escapeMarkdownV2(await formatBalanceForDisplay(profit, 'USD'))}* profit!`;
            }
        } else {
            await client.query('ROLLBACK');
            throw new Error(balanceUpdate.error || "DB Error during PvB Duel payout.");
        }
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} CRITICAL DB error finalizing PvB Duel ${gameId}: ${e.message}`);
        finalMessageText += `\n\n⚠️ Critical error settling wager: \`${escapeMarkdownV2(e.message)}\`. Admin notified.`;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Duel PvB Payout Failure 🚨\nGame ID: \`${gameId}\` User: ${playerRef} \nAmount: ${payoutAmountLamports}\nDB Error: ${e.message}. MANUAL CHECK REQUIRED.`);
    } finally {
        if (client) client.release();
    }

    finalMessageText += `\n\nYour new balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*\\.`;
    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.DUEL_PVB, betAmount); 

    if (gameMessageId && bot) {
        await bot.deleteMessage(chatId, Number(gameMessageId)).catch(e => console.warn(`${logPrefix} Non-critical fail to delete old msg ${gameMessageId} before final result: ${e.message}`));
    }
    await safeSendMessage(chatId, finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
    
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
    console.log(`${logPrefix} PvB Duel game ${gameId} finalized.`);
}

// Generic refund helper for PvB Duel if needed before full finalization
async function refundDuelPvBBetsGeneric(gameId, userId, betAmount, reason, logPrefix = "[Duel_PvB_Refund]") {
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        await updateUserBalanceAndLedger(client, userId, betAmount, 
            'refund_duel_pvb_error', 
            { game_id_custom_field: gameId, error_reason: reason.substring(0,100) }, 
            `Refund for errored PvB Duel game ${gameId}. Reason: ${reason}`
        );
        await client.query('COMMIT');
        console.log(`${logPrefix} Refund processed for game ${gameId}, user ${userId}, amount ${betAmount}. Reason: ${reason}`);
    } catch (e) {
        if (client) await client.query('ROLLBACK');
        console.error(`${logPrefix} CRITICAL: Failed to process refund for game ${gameId}, user ${userId}: ${e.message}`);
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Duel PvB REFUND Failure 🚨\nGame ID: \`${gameId}\`, User: ${userId}\nReason: ${reason}\nDB Error: ${e.message}. MANUAL REFUND REQUIRED.`);
    } finally {
        if (client) client.release();
    }
}


// --- Player vs. Player (PvP) Duel Game Logic ---
async function startDuelPvPGameSequence(pvpGameId) { 
    const gameData = activeGames.get(pvpGameId);
    if (!gameData || gameData.type !== GAME_IDS.DUEL_PVP) {
        console.error(`[Duel_PvP_StartSeq GID:${pvpGameId}] Invalid game data or type.`);
        return;
    }
    const logPrefix = `[Duel_PvP_StartSeq GID:${pvpGameId}]`;
    console.log(`${logPrefix} Starting turn sequence. P1 (${gameData.initiator.mention}) to roll first.`);

    gameData.initiator.isTurn = true;
    gameData.opponent.isTurn = false;
    gameData.status = 'p1_awaiting_roll1_emoji';
    activeGames.set(pvpGameId, gameData);
    await updateDuelPvPMessage(pvpGameId, true); 
}

async function processDuelPlayerRollsCompletePvP(gameData, firstRoll, secondRoll, actingPlayerId) {
    const { gameId, chatId, initiator, opponent, betAmount } = gameData;
    const logPrefix = `[Duel_PvP_PlayerDone GID:${gameId} Actor:${actingPlayerId}]`;

    let currentPlayer = (initiator.userId === actingPlayerId) ? initiator : opponent;
    let otherPlayer = (initiator.userId === actingPlayerId) ? opponent : initiator;
    let playerIdentifier = (initiator.userId === actingPlayerId) ? "Initiator" : "Opponent";
    let nextStatusForOtherPlayer = (otherPlayer === initiator) ? 'p1_awaiting_roll1_emoji' : 'p2_awaiting_roll1_emoji';

    currentPlayer.rolls = [firstRoll, secondRoll];
    currentPlayer.score = firstRoll + secondRoll;
    currentPlayer.isTurn = false;
    currentPlayer.status = 'rolls_complete';
    gameData.lastInteractionTime = Date.now();
    
    console.log(`${logPrefix} ${playerIdentifier} ${currentPlayer.mention} completed rolls: [${currentPlayer.rolls.join(',')}] Total: ${currentPlayer.score}`);

    if (otherPlayer.status === 'waiting_turn') { 
        otherPlayer.isTurn = true;
        gameData.status = nextStatusForOtherPlayer;
        console.log(`${logPrefix} Switching turn to ${otherPlayer.mention}. New game status: ${gameData.status}`);
    } else if (otherPlayer.status === 'rolls_complete') { 
        gameData.status = 'game_over_pvp_resolved';
        console.log(`${logPrefix} Both players completed rolls. Game over, proceeding to resolve.`);
    } else {
         console.warn(`${logPrefix} Unexpected other player status: ${otherPlayer.status}. Forcing game over for safety.`);
         gameData.status = 'game_over_pvp_resolved'; 
    }

    activeGames.set(gameId, gameData);
    await updateDuelPvPMessage(gameId);

    if (gameData.status === 'game_over_pvp_resolved') {
        await sleep(1500); 
        await resolveDuelPvPGame(gameId);
    }
}

async function updateDuelPvPMessage(gameId, isInitialTurnMessage = false) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DUEL_PVP) {
        console.error(`[Duel_PvP_UpdateMsg GID:${gameId}] Invalid game data. Cannot update UI.`);
        return;
    }
    const logPrefix = `[Duel_PvP_UpdateMsg GID:${gameId} Status:'${gameData.status}']`;
    
    if (gameData.currentMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.currentMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical fail to delete old PvP msg ${gameData.currentMessageId}: ${e.message}`));
    }

    const p1 = gameData.initiator;
    const p2 = gameData.opponent;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    let messageText = `⚔️ **Duel: ${p1.mention} vs ${p2.mention}** ⚔️\n` +
                      `Wager: *${betDisplayUSD}* each\n\n` +
                      `**${p1.mention}**: ${p1.status === 'rolls_complete' ? `Rolled ${formatDiceRolls(p1.rolls)} (Total: *${p1.score}*)` : (p1.status === 'waiting_turn' && !p1.isTurn ? 'Waiting...' : (p1.isTurn ? 'Rolling...' : 'Waiting...'))}\n` +
                      `**${p2.mention}**: ${p2.status === 'rolls_complete' ? `Rolled ${formatDiceRolls(p2.rolls)} (Total: *${p2.score}*)` : (p2.status === 'waiting_turn' && !p2.isTurn ? 'Waiting...' : (p2.isTurn ? 'Rolling...' : 'Waiting...'))}\n\n`;

    let actionPrompt = "";
    if (gameData.status === 'p1_awaiting_roll1_emoji' || gameData.status === 'p1_awaiting_roll2_emoji') {
        actionPrompt = `${p1.mention}, it's your turn! Please send **two separate** 🎲 dice emojis.`;
         if (gameData.status === 'p1_awaiting_roll2_emoji') actionPrompt = `${p1.mention}, please send your **second** 🎲 dice emoji.`;
    } else if (gameData.status === 'p2_awaiting_roll1_emoji' || gameData.status === 'p2_awaiting_roll2_emoji') {
        actionPrompt = `${p2.mention}, it's your turn! Please send **two separate** 🎲 dice emojis.`;
        if (gameData.status === 'p2_awaiting_roll2_emoji') actionPrompt = `${p2.mention}, please send your **second** 🎲 dice emoji.`;
    } else if (gameData.status === 'game_over_pvp_resolved') {
        actionPrompt = "Both duelists have rolled! Calculating the victor...";
    } else {
        actionPrompt = "Waiting for player action or game resolution...";
    }
    messageText += actionPrompt;

    const sentMessage = await safeSendMessage(gameData.chatId, messageText, { parse_mode: 'MarkdownV2' });
    if (sentMessage?.message_id) {
        gameData.currentMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
    } else {
        console.error(`${logPrefix} CRITICAL: Failed to send/update PvP Duel message for GID ${gameId}.`);
        await safeSendMessage(gameData.chatId, `🚨 Display error in Duel game \`${gameId.slice(-5)}\`. Game cancelled, bets refunded.`, {parse_mode:'MarkdownV2'});
        await refundDuelPvPBets(gameId, "PvP UI update critical failure");
    }
}

async function resolveDuelPvPGame(gameId) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DUEL_PVP || gameData.status !== 'game_over_pvp_resolved') {
        console.error(`[Duel_PvP_Resolve GID:${gameId}] Invalid call. GameData: ${!!gameData}, Type: ${gameData?.type}, Status: ${gameData?.status}`);
        return;
    }
    const logPrefix = `[Duel_PvP_Resolve GID:${gameId}]`;
    const p1 = gameData.initiator;
    const p2 = gameData.opponent;

    console.log(`${logPrefix} Resolving PvP Duel. P1 (${p1.mention}) Score: ${p1.score}, P2 (${p2.mention}) Score: ${p2.score}`);

    let resultTextPart = "";
    let p1CreditChange = 0n; let p2CreditChange = 0n;
    let p1LedgerCode = 'loss_duel_pvp'; let p2LedgerCode = 'loss_duel_pvp';

    if (p1.score > p2.score) {
        resultTextPart = `🏆 ${p1.mention} triumphs with *${p1.score}* over ${p2.mention}'s *${p2.score}*!`;
        p1CreditChange = gameData.betAmount * 2n; 
        p1LedgerCode = 'win_duel_pvp';
    } else if (p2.score > p1.score) {
        resultTextPart = `🏆 ${p2.mention} is victorious with *${p2.score}* over ${p1.mention}'s *${p1.score}*!`;
        p2CreditChange = gameData.betAmount * 2n;
        p2LedgerCode = 'win_duel_pvp';
    } else {
        resultTextPart = `🛡️ A Draw! Both ${p1.mention} and ${p2.mention} scored *${p1.score}*! Wagers returned.`;
        p1CreditChange = gameData.betAmount; p2CreditChange = gameData.betAmount;
        p1LedgerCode = 'push_duel_pvp'; p2LedgerCode = 'push_duel_pvp';
    }

    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    let finalMessageText = 
        `⚔️ **Duel PvP - Final Result!** ⚔️\n` +
        `Wager: *${betDisplayUSD}* each\n\n` +
        `**${p1.mention}** rolled: ${formatDiceRolls(p1.rolls)} (Total: *${escapeMarkdownV2(String(p1.score))}*)\n` +
        `**${p2.mention}** rolled: ${formatDiceRolls(p2.rolls)} (Total: *${escapeMarkdownV2(String(p2.score))}*)\n\n` +
        `------------------------------------\n${resultTextPart}`;

    let p1FinalBal = BigInt(p1.userObj.balance); let p2FinalBal = BigInt(p2.userObj.balance);
    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const p1Update = await updateUserBalanceAndLedger(client, p1.userId, p1CreditChange, p1LedgerCode, 
            { game_id_custom_field: gameId, opponent_id: p2.userId, player_score: p1.score, opponent_score: p2.score }, 
            `PvP Duel ${gameId} result vs ${p2.mention}`);
        if (!p1Update.success) throw new Error(`P1 (${p1.mention}) update fail: ${p1Update.error}`);
        p1FinalBal = p1Update.newBalanceLamports;

        const p2Update = await updateUserBalanceAndLedger(client, p2.userId, p2CreditChange, p2LedgerCode, 
            { game_id_custom_field: gameId, opponent_id: p1.userId, player_score: p2.score, opponent_score: p1.score }, 
            `PvP Duel ${gameId} result vs ${p1.mention}`);
        if (!p2Update.success) throw new Error(`P2 (${p2.mention}) update fail: ${p2Update.error}`);
        p2FinalBal = p2Update.newBalanceLamports;
        
        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} CRITICAL DB error finalizing PvP Duel ${gameId}: ${e.message}`);
        finalMessageText += `\n\n⚠️ Critical error settling wagers: \`${escapeMarkdownV2(e.message)}\`. Admins notified.`;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Duel PvP Payout Failure 🚨\nGame ID: \`${gameId}\`\nError: ${e.message}. MANUAL CHECK REQUIRED.`);
    } finally {
        if (client) client.release();
    }

    finalMessageText += `\n\nBalances: ${p1.mention} *${escapeMarkdownV2(await formatBalanceForDisplay(p1FinalBal, 'USD'))}* | ${p2.mention} *${escapeMarkdownV2(await formatBalanceForDisplay(p2FinalBal, 'USD'))}*`;
    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.DUEL_PVP, gameData.betAmount); 

    if (gameData.currentMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.currentMessageId)).catch(e => console.warn(`${logPrefix} Non-critical: fail to delete old msg ${gameData.currentMessageId}: ${e.message}`));
    }
    await safeSendMessage(gameData.chatId, finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
    
    activeGames.delete(gameId);
    await updateGroupGameDetails(gameData.chatId, null, null, null);
    console.log(`${logPrefix} PvP Duel game ${gameId} finalized.`);
}

async function refundDuelPvPBets(gameId, reason) {
    const gameData = activeGames.get(gameId);
    if (!gameData || (gameData.type !== GAME_IDS.DUEL_PVP && gameData.type !== GAME_IDS.DUEL_UNIFIED_OFFER)) { 
        console.error(`[Duel_RefundPvP GID:${gameId}] Invalid game data or type for refund. Type: ${gameData?.type}`);
        return;
    }
    const logPrefix = `[Duel_RefundPvP GID:${gameId}]`;
    console.log(`${logPrefix} Refunding PvP bets. Reason: ${reason}`);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const betAmount = gameData.betAmount;

        if (gameData.initiator && gameData.initiator.userId && betAmount > 0n) {
            await updateUserBalanceAndLedger(client, gameData.initiator.userId, betAmount, 
                'refund_duel_pvp_error', { game_id_custom_field: gameId, error_reason: reason }, 
                `Refund PvP Duel ${gameId} for initiator. Reason: ${reason}`);
            console.log(`${logPrefix} Initiator ${gameData.initiator.userId} refunded ${betAmount}`);
        }
        if (gameData.opponent && gameData.opponent.userId && betAmount > 0n) {
            await updateUserBalanceAndLedger(client, gameData.opponent.userId, betAmount, 
                'refund_duel_pvp_error', { game_id_custom_field: gameId, error_reason: reason }, 
                `Refund PvP Duel ${gameId} for opponent. Reason: ${reason}`);
            console.log(`${logPrefix} Opponent ${gameData.opponent.userId} refunded ${betAmount}`);
        }
        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK');
        console.error(`${logPrefix} CRITICAL error during PvP Duel refund for game ${gameId}: ${e.message}`);
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Duel PvP REFUND Failure 🚨\nGame ID: \`${gameId}\`\nReason: ${reason}\nDB Error: ${e.message}. MANUAL REFUNDS REQUIRED.`);
    } finally {
        if (client) client.release();
        activeGames.delete(gameId);
        if (gameData.chatId) await updateGroupGameDetails(gameData.chatId, null, null, null);
    }
}

async function getTwoDiceRollsViaHelperDuel(gameId, chatIdForLog, userIdForRoll, rollPurposeNotePrefix) {
    const rolls = [];
    let anErrorOccurred = null;

    for (let i = 0; i < 2; i++) { // DUEL_DICE_COUNT is 2
        const rollResult = await getSingleDiceRollViaHelperDuel(gameId, chatIdForLog, userIdForRoll, `${rollPurposeNotePrefix} - Die ${i + 1}`);
        if (rollResult.error) {
            anErrorOccurred = rollResult.message || `Failed to get Die ${i + 1} for ${rollPurposeNotePrefix}`;
            break;
        }
        rolls.push(rollResult.roll);
    }
    return { rolls, error: anErrorOccurred };
}

async function processDuelPvBRollByEmoji(gameData, diceValue) {
    const logPrefix = `[Duel_PvB_EmojiProc GID:${gameData.gameId}]`;
    console.log(`${logPrefix} Player emoji roll received: ${diceValue}. Current status: ${gameData.status}`);
    if (gameData.status === 'player_awaiting_roll1_emoji') {
        gameData._internalTempRoll1 = diceValue; 
        gameData.status = 'player_awaiting_roll2_emoji';
        activeGames.set(gameData.gameId, gameData);
        console.log(`${logPrefix} First roll ${diceValue} received. Waiting for second.`);
    } else if (gameData.status === 'player_awaiting_roll2_emoji') {
        const firstRoll = gameData._internalTempRoll1;
        delete gameData._internalTempRoll1;
        console.log(`${logPrefix} Second roll ${diceValue} received. First was ${firstRoll}. Processing.`);
        await processDuelPlayerRollsCompletePvB(gameData, firstRoll, diceValue);
    } else {
        console.warn(`${logPrefix} Emoji roll received in unexpected PvB status: ${gameData.status}`);
    }
}

async function processDuelPvPRollByEmoji(gameData, diceValue, rollerUserId) {
    const logPrefix = `[Duel_PvP_EmojiProc GID:${gameData.gameId} Roller:${rollerUserId}]`;
    console.log(`${logPrefix} PvP emoji roll received: ${diceValue}. Current status: ${gameData.status}`);
    
    let currentPlayer, playerKey;
    if (gameData.initiator.userId === rollerUserId && gameData.status === 'p1_awaiting_roll1_emoji') {
        currentPlayer = gameData.initiator; playerKey = 'p1';
        currentPlayer._internalTempRoll1 = diceValue;
        gameData.status = 'p1_awaiting_roll2_emoji';
        console.log(`${logPrefix} P1 first roll ${diceValue} received. Waiting for second.`);
    } else if (gameData.initiator.userId === rollerUserId && gameData.status === 'p1_awaiting_roll2_emoji') {
        currentPlayer = gameData.initiator; playerKey = 'p1';
        const firstRoll = currentPlayer._internalTempRoll1;
        delete currentPlayer._internalTempRoll1;
        console.log(`${logPrefix} P1 second roll ${diceValue} received. First was ${firstRoll}. Processing.`);
        await processDuelPlayerRollsCompletePvP(gameData, firstRoll, diceValue, rollerUserId);
    } else if (gameData.opponent.userId === rollerUserId && gameData.status === 'p2_awaiting_roll1_emoji') {
        currentPlayer = gameData.opponent; playerKey = 'p2';
        currentPlayer._internalTempRoll1 = diceValue;
        gameData.status = 'p2_awaiting_roll2_emoji';
        console.log(`${logPrefix} P2 first roll ${diceValue} received. Waiting for second.`);
    } else if (gameData.opponent.userId === rollerUserId && gameData.status === 'p2_awaiting_roll2_emoji') {
        currentPlayer = gameData.opponent; playerKey = 'p2';
        const firstRoll = currentPlayer._internalTempRoll1;
        delete currentPlayer._internalTempRoll1;
        console.log(`${logPrefix} P2 second roll ${diceValue} received. First was ${firstRoll}. Processing.`);
        await processDuelPlayerRollsCompletePvP(gameData, firstRoll, diceValue, rollerUserId);
    } else {
        console.warn(`${logPrefix} Emoji roll received in unexpected PvP status or from wrong player: ${gameData.status}`);
        return; 
    }
    activeGames.set(gameData.gameId, gameData);
}

// --- End of Part 5c, Section 2 (COMPLETE REWRITE FOR NEW DUEL GAME LOGIC) ---
// --- Start of Part 5c, Section 3 (NEW) - Segment 1 & 2 (FULLY UPDATED FOR HELPER BOT DICE ROLLS for Ladder, Animated for SevenOut) ---
// index.js - Part 5c, Section 3: Greed's Ladder & Sevens Out (Simplified Craps) Game Logic & Handlers
// (This entire block is placed after Original Part 5c, Section 2 in the new order)
//----------------------------------------------------------------------------------------------------
// Assumed dependencies from previous Parts

// --- Greed's Ladder Game Logic ---

async function handleStartLadderCommand(msg, betAmountLamports) {
    // ***** CORRECTED LINE FOR USER ID EXTRACTION *****
    const userId = String(msg.from.id || msg.from.telegram_id);
    // ***** END OF CORRECTION *****
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_LADDER_START = `[Ladder_Start UID:${userId} CH:${chatId}]`; // Now uses corrected userId

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_LADDER_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🪜 Oh dear! The wager for Greed's Ladder seems incorrect\\. Please try again with a valid amount\\.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Greetings, climber! We couldn't find your adventurer profile for Greed's Ladder\\. Please try \`/start\` again\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_LADDER_START} Initiating Greed's Ladder. Bet: ${betAmountLamports}`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your treasure chest is a bit light for the *${betDisplayUSD}* climb on Greed's Ladder! You'll need about *${neededDisplay}* more\\. Fortify your reserves?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.LADDER);
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_ladder', { game_id_custom_field: gameId },
            `Bet for Greed's Ladder game ${gameId}` // Simplified note
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_LADDER_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Greed's Ladder wager of *${betDisplayUSD}* failed to post: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports; 
        // console.log(`${LOG_PREFIX_LADDER_START} Wager ${betAmountLamports} placed. New balance for ${userId}: ${userObj.balance}`); // Reduced log
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_LADDER_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_LADDER_START} Database error during Greed's Ladder bet: ${dbError.message}`, dbError.stack?.substring(0,500));
        await safeSendMessage(chatId, "⚙️ The Ladder's foundations seem shaky (database error)! Failed to start\\. Please try again\\.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }
    
    const gameData = { 
        type: GAME_IDS.LADDER, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, rolls: [], sum: 0n, status: 'rolling_waiting_helper', gameMessageId: null 
    };
    activeGames.set(gameId, gameData); 

    const titleRolling = createStandardTitle("Greed's Ladder - The Climb Begins!", "🪜");
    let messageText = `${titleRolling}\n\n${playerRef} wagers *${betDisplayUSD}* and steps onto Greed's Ladder!\nRequesting *${escapeMarkdownV2(String(LADDER_ROLL_COUNT))} dice* from the Helper Bot... This may take a moment! 🎲⏳`;
    
    const sentRollingMsg = await safeSendMessage(chatId, messageText, {parse_mode: 'MarkdownV2'});
    if (sentRollingMsg?.message_id) {
        gameData.gameMessageId = sentRollingMsg.message_id;
        activeGames.set(gameId, gameData); 
    } else {
        console.error(`${LOG_PREFIX_LADDER_START} Failed to send initial Ladder game message for ${gameId}. Refunding wager.`);
        let refundClient = null;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_ladder_setup_fail', {game_id_custom_field: gameId}, `Refund Ladder game ${gameId} (message send fail)`);
            await refundClient.query('COMMIT');
        } catch (dbErr) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_LADDER_START} CRITICAL: Refund failed after Ladder setup fail for game ${gameId}: ${dbErr.message}`);
        } finally { if (refundClient) refundClient.release(); }
        activeGames.delete(gameId);
        return;
    }

    // Using getSingleDiceRollViaHelper, assuming it's defined and accessible (e.g. from Part 5b-S2 or a common utility section)
    let diceRolls = [];
    let helperBotError = null;

    for (let i = 0; i < LADDER_ROLL_COUNT; i++) {
        if (isShuttingDown) { helperBotError = "Shutdown during Ladder dice requests."; break; }
        const rollResult = await getSingleDiceRollViaHelper(gameId, chatId, userId, `Ladder Roll ${i+1}`); 
        if (rollResult.error) {
            helperBotError = rollResult.message || `Failed to get Ladder Roll ${i+1}`;
            break;
        }
        diceRolls.push(rollResult.roll);
    }


    if (helperBotError || diceRolls.length !== LADDER_ROLL_COUNT) {
        const errorMsgToUser = `⚠️ ${playerRef}, there was an issue getting your dice rolls for Greed's Ladder: \`${escapeMarkdownV2(String(helperBotError || "Incomplete rolls from helper").substring(0,150))}\`\nYour bet of *${betDisplayUSD}* has been refunded.`;
        if (gameData.gameMessageId && bot) {
            await bot.editMessageText(errorMsgToUser, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: createPostGameKeyboard(GAME_IDS.LADDER, betAmountLamports) }).catch(async () => {
                 await safeSendMessage(String(chatId), errorMsgToUser, { parse_mode: 'MarkdownV2', reply_markup: createPostGameKeyboard(GAME_IDS.LADDER, betAmountLamports) });
            });
        } else {
            await safeSendMessage(String(chatId), errorMsgToUser, { parse_mode: 'MarkdownV2', reply_markup: createPostGameKeyboard(GAME_IDS.LADDER, betAmountLamports) });
        }
        let refundClient = null;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_ladder_helper_fail', {game_id_custom_field: gameId}, `Refund Ladder game ${gameId} - Helper Bot error`);
            await refundClient.query('COMMIT');
        } catch (dbErr) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_LADDER_START} CRITICAL: Refund failed after Ladder helper error for game ${gameId}: ${dbErr.message}`);
        } finally { if (refundClient) refundClient.release(); }
        activeGames.delete(gameId); 
        return;
    }

    gameData.rolls = diceRolls;
    gameData.sum = BigInt(diceRolls.reduce((sum, val) => sum + val, 0));
    let isBust = gameData.rolls.includes(LADDER_BUST_ON);

    let payoutAmountLamports = 0n;
    let outcomeReasonLog = "";
    let resultTextPart = "";
    let finalUserBalanceLamports = userObj.balance; 

    const titleResult = createStandardTitle("Greed's Ladder - The Outcome!", "🏁");
    messageText = `${titleResult}\n\n${playerRef}'s wager: *${betDisplayUSD}*\nThe Helper Bot delivered dice: ${formatDiceRolls(gameData.rolls)}\nTotal Sum: *${escapeMarkdownV2(String(gameData.sum))}*\n\n`;

    if (isBust) {
        outcomeReasonLog = `loss_ladder_bust_roll${LADDER_BUST_ON}`;
        resultTextPart = `💥 *CRASH! A ${escapeMarkdownV2(String(LADDER_BUST_ON))} appeared!* 💥\nYou've tumbled off Greed's Ladder! Your wager is lost\\.`;
        gameData.status = 'game_over_player_bust';
    } else {
        let foundPayout = false;
        for (const payoutTier of LADDER_PAYOUTS) {
            if (gameData.sum >= payoutTier.min && gameData.sum <= payoutTier.max) {
                const profitLamports = betAmountLamports * BigInt(payoutTier.multiplier);
                payoutAmountLamports = betAmountLamports + profitLamports; 
                outcomeReasonLog = `win_ladder_sum${gameData.sum}_mult${payoutTier.multiplier}`;
                resultTextPart = `${escapeMarkdownV2(payoutTier.label)} You've reached a high rung and won *${escapeMarkdownV2(await formatBalanceForDisplay(profitLamports, 'USD'))}* in profit!`;
                foundPayout = true;
                break;
            }
        }
        if (!foundPayout) { 
            outcomeReasonLog = 'loss_ladder_no_payout_tier';
            resultTextPart = "😐 A cautious climb\\.\\.\\. but not high enough for a prize this time\\. Your wager is lost\\.";
        }
        gameData.status = 'game_over_resolved';
    }
    messageText += resultTextPart;
    
    let clientOutcome = null;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const ledgerReason = `${outcomeReasonLog} (Game ID: ${gameId})`;
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
            messageText += `\n\n⚠️ A critical error occurred settling your Ladder game: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Casino staff notified\\.`;
            console.error(`${LOG_PREFIX_LADDER_START} Failed to update balance for Ladder game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL LADDER Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\` User: ${playerRef}\nAmount: \`${formatCurrency(payoutAmountLamports)}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check needed\\.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_LADDER_START} DB error during Ladder outcome for ${gameId}: ${dbError.message}`, dbError.stack?.substring(0,500));
        messageText += `\n\n⚠️ A severe database error occurred resolving your climb\\. Casino staff notified\\.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    messageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*\\.`;
    const postGameKeyboardLadder = createPostGameKeyboard(GAME_IDS.LADDER, betAmountLamports);

    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(messageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder })
            .catch(async (e) => {
                // console.warn(`${LOG_PREFIX_LADDER_START} Failed to edit final Ladder message (ID: ${gameData.gameMessageId}), sending new: ${e.message}`); // Reduced log
                await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder });
            });
    } else {
        await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder });
    }
    activeGames.delete(gameId);
}

// --- Sevens Out (Simplified Craps) Game Logic ---

async function handleStartSevenOutCommand(msg, betAmountLamports) {
    // ***** CORRECTED LINE FOR USER ID EXTRACTION *****
    const userId = String(msg.from.id || msg.from.telegram_id);
    // ***** END OF CORRECTION *****
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_S7_START = `[S7_Start UID:${userId} CH:${chatId}]`; // Now uses corrected userId

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_S7_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🎲 Seven's a charm, but not with that bet! Please try again with a valid wager for Sevens Out\\.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Greetings, roller! We couldn't find your player profile for Sevens Out\\. Please try \`/start\` again\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_S7_START} Initiating Sevens Out. Bet: ${betAmountLamports}`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino wallet is a bit light for a *${betDisplayUSD}* game of Sevens Out! You'll need about *${neededDisplay}* more\\. Ready to reload?`, {
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
            `Bet for Sevens Out game ${gameId}` // Simplified note
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_S7_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Sevens Out wager of *${betDisplayUSD}* hit a snag: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`\\. Please try once more\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports;
        // console.log(`${LOG_PREFIX_S7_START} Wager ${betAmountLamports} placed. New balance for ${userId}: ${userObj.balance}`); // Reduced log
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_S7_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_S7_START} Database error during Sevens Out bet: ${dbError.message}`, dbError.stack?.substring(0,500));
        await safeSendMessage(chatId, "⚙️ The dice table seems to be under maintenance (database error)! Failed to start\\. Please try again\\.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }
    
    const gameData = { 
        type: GAME_IDS.SEVEN_OUT, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, pointValue: null, rolls: [], currentSum: 0n,
        status: 'come_out_roll_pending', 
        gameMessageId: null, lastInteractionTime: Date.now() 
    };
    activeGames.set(gameId, gameData);

    const title = createStandardTitle("Sevens Out - Come Out Roll!", "🎲");
    const initialMessageText = `${title}\n\n${playerRef}, your wager of *${betDisplayUSD}* is locked in for Sevens Out! Stepping up for the crucial **Come Out Roll**\\.\\.\\.\n\nI'll roll the first set of dice for you! Good luck! 🍀`;
    
    const sentMessage = await safeSendMessage(chatId, initialMessageText, {parse_mode: 'MarkdownV2'});
    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData); 
        
        const mockMsgContextForFirstRoll = {
            from: userObj,
            chat: { id: chatId, type: msg.chat.type }, 
            message_id: sentMessage.message_id 
        };
        await processSevenOutRoll(gameId, userObj, sentMessage.message_id, null, mockMsgContextForFirstRoll);

    } else {
        console.error(`${LOG_PREFIX_S7_START} Failed to send initial Sevens Out message for ${gameId}. Refunding wager.`);
        let refundClient;
        try {
            refundClient = await pool.connect();
            await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_s7_setup_fail', {}, `Refund for S7 game ${gameId} (message send fail)`);
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
    const currentMainMessageId = gameData.gameMessageId; 

    let rollingText = isComeOutRoll ? 
        `${playerRef} is making the crucial **Come Out Roll** (Wager: *${betDisplayUSD}*)\\.\\.\\.` :
        `${playerRef} rolls for their Point of *${escapeMarkdownV2(String(gameData.pointValue))}* (Wager: *${betDisplayUSD}*)\\.\\.\\.`;
    rollingText += "\n\nDice are flying! 🌪️🎲";
    
    if (currentMainMessageId && bot) {
        try {
            await bot.editMessageText(rollingText, { chat_id: String(chatId), message_id: Number(currentMainMessageId), parse_mode: 'MarkdownV2', reply_markup: {} });
        } catch(e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                // console.warn(`${LOG_PREFIX_S7_ROLL} Failed to edit rolling message (ID:${currentMainMessageId}). Error: ${e.message}`); // Reduced log
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
            await sleep(2200); // Wait for dice animation
        } catch (e) {
            console.warn(`${LOG_PREFIX_S7_ROLL} Failed to send animated dice for S7 (Roll ${i+1}), using internal roll. Error: ${e.message}`);
            const internalRollVal = rollDie(); // rollDie is from Part 3
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
        messageToPlayer += `Your Point to hit is: *${escapeMarkdownV2(String(gameData.pointValue))}*\\.\n`;
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
            payoutAmountLamports = betAmount * 2n; 
            outcomeReasonLog = `win_s7_natural_${currentSum}`;
        } else if (currentSum === 2 || currentSum === 3 || currentSum === 12) { // Craps Loss
            gameEndsNow = true; gameData.status = 'game_over_loss_craps';
            resultTextPart = `💔 **Craps!** A ${currentSum} on the Come Out means the house wins this round\\.`;
            payoutAmountLamports = 0n; 
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
            resultTextPart = `💔 **Seven Out!** You rolled a 7 before hitting your Point of ${escapeMarkdownV2(String(gameData.pointValue))}\\. House wins\\.`;
            payoutAmountLamports = 0n;
            outcomeReasonLog = `loss_s7_seven_out_point_${gameData.pointValue}`;
        } else { // Neither Point nor 7 - Roll Again
            gameData.status = 'point_phase_waiting_roll'; 
            resultTextPart = `🎲 Keep rolling! Your Point is still *${escapeMarkdownV2(String(gameData.pointValue))}*\\. Avoid that 7!`;
            nextKeyboard = { inline_keyboard: [[{ text: `🎲 Roll Again for Point (${escapeMarkdownV2(String(gameData.pointValue))})!`, callback_data: `s7_roll:${gameId}` }],[{text: `📖 Rules`, callback_data:`${RULES_CALLBACK_PREFIX}${GAME_IDS.SEVEN_OUT}`}]] };
        }
    }
    
    messageToPlayer += resultTextPart;
    activeGames.set(gameId, gameData); 

    if (gameEndsNow) {
        await finalizeSevenOutGame(gameData, messageToPlayer, payoutAmountLamports, outcomeReasonLog, currentMainMessageId);
    } else {
        if (currentMainMessageId && bot) {
            await bot.editMessageText(messageToPlayer, { chat_id: String(chatId), message_id: Number(currentMainMessageId), parse_mode: 'MarkdownV2', reply_markup: nextKeyboard })
            .catch(async (e) => {
                if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                    // console.warn(`${LOG_PREFIX_S7_ROLL} Failed to edit S7 mid-game message (ID:${currentMainMessageId}), sending new. Error: ${e.message}`); // Reduced log
                    const newMsg = await safeSendMessage(String(chatId), messageToPlayer, { parse_mode: 'MarkdownV2', reply_markup: nextKeyboard });
                    if (newMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = newMsg.message_id;
                }
            });
        } else { 
            const newMsg = await safeSendMessage(String(chatId), messageToPlayer, { parse_mode: 'MarkdownV2', reply_markup: nextKeyboard });
            if (newMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = newMsg.message_id;
        }
    }
}

async function finalizeSevenOutGame(gameData, initialResultMessage, payoutAmountLamports, outcomeReasonLog, gameUIMessageId) {
    const { gameId, chatId, userId, playerRef, betAmount, userObj } = gameData;
    const LOG_PREFIX_S7_FINALIZE = `[S7_Finalize GID:${gameId} UID:${userId}]`;
    let finalUserBalanceLamports = BigInt(userObj.balance); 
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
                initialResultMessage += `\nYou pocket a neat *${escapeMarkdownV2(await formatBalanceForDisplay(profit, 'USD'))}* in profit\\!`;
            }
        } else {
            await clientOutcome.query('ROLLBACK');
            initialResultMessage += `\n\n⚠️ A critical casino vault error occurred settling your Sevens Out game: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Our pit boss has been alerted for manual review\\.`;
            console.error(`${LOG_PREFIX_S7_FINALIZE} Failed to update balance for Sevens Out game ${gameId}. Error: ${balanceUpdate.error}`);
             if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL S7 Payout/Refund Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\` User: ${playerRef} (\`${escapeMarkdownV2(userId)}\`)\nAmount Due: \`${escapeMarkdownV2(formatCurrency(payoutAmountLamports))}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check required\\.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_S7_FINALIZE} DB error during S7 outcome for ${gameId}: ${dbError.message}`, dbError.stack?.substring(0,500));
        initialResultMessage += `\n\n⚠️ A major dice table malfunction (database error) occurred\\. Our pit boss has been notified\\.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }
    
    initialResultMessage += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*\\.`;
    const postGameKeyboardS7 = createPostGameKeyboard(GAME_IDS.SEVEN_OUT, betAmount);

    if (gameUIMessageId && bot) {
        await bot.editMessageText(initialResultMessage, { chat_id: String(chatId), message_id: Number(gameUIMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardS7 })
            .catch(async (e) => {
                // console.warn(`${LOG_PREFIX_S7_FINALIZE} Failed to edit final S7 message (ID: ${gameUIMessageId}), sending new: ${e.message}`); // Reduced log
                await safeSendMessage(String(chatId), initialResultMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardS7 });
            });
    } else {
        await safeSendMessage(String(chatId), initialResultMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardS7 });
    }
    activeGames.delete(gameId);
}

// --- End of Part 5c, Section 3 (NEW) ---
// --- Start of Part 5c, Section 4 (FULLY UPDATED FOR HELPER BOT DICE ROLLS) ---
// index.js - Part 5c, Section 4: Slot Frenzy Game Logic & Callback Router for Part 5c Games
// (This entire block is placed after Original Part 5c, Section 3 in the new order)
//----------------------------------------------------------------------------------------------------
// Assumed dependencies from previous Parts

// --- Slot Frenzy Game Logic ---

async function handleStartSlotCommand(msg, betAmountLamports) {
    // ***** CORRECTED LINE FOR USER ID EXTRACTION *****
    const userId = String(msg.from.id || msg.from.telegram_id);
    // ***** END OF CORRECTION *****
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_SLOT_START = `[Slot_Start UID:${userId} CH:${chatId}]`; // Now uses corrected userId

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_SLOT_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🎰 Hold your horses! That bet amount for Slot Frenzy doesn't look quite right\\. Please try again with a valid wager\\.", { parse_mode: 'MarkdownV2' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Hey spinner! We couldn't find your player profile for Slot Frenzy\\. Please hit \`/start\` first\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX_SLOT_START} Initiating Slot Frenzy. Bet: ${betAmountLamports}`);

    const playerRef = getPlayerDisplayReference(userObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your casino wallet needs a bit more sparkle for a *${betDisplayUSD}* spin on Slot Frenzy! You're short by about *${neededDisplay}*\\. Time to reload?`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.SLOT_FRENZY); 
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_slot', { game_id_custom_field: gameId },
            `Bet for Slot Frenzy game ${gameId}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_SLOT_START} Wager placement failed: ${balanceUpdateResult.error}`);
            await safeSendMessage(chatId, `${playerRef}, your Slot Frenzy wager of *${betDisplayUSD}* jammed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`\\. Please try spinning again\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports;
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_SLOT_START} DB Rollback Error: ${dbError.message}`));
        console.error(`${LOG_PREFIX_SLOT_START} Database error during Slot Frenzy bet: ${dbError.message}`, dbError.stack?.substring(0,500));
        await safeSendMessage(chatId, "⚙️ The slot machine's gears are stuck (database error)! Failed to start\\. Please try again\\.", { parse_mode: 'MarkdownV2' });
        return;
    } finally {
        if (client) client.release();
    }

    const gameData = { 
        type: GAME_IDS.SLOT_FRENZY, gameId, chatId, userId, playerRef, userObj,
        betAmount: betAmountLamports, diceValue: null, payoutInfo: null,
        status: 'spinning_waiting_helper', gameMessageId: null 
    };
    activeGames.set(gameId, gameData); 

    const titleSpinning = createStandardTitle("Slot Frenzy - Reels are Spinning!", "🎰");
    let messageText = `${titleSpinning}\n\n${playerRef}, you've placed a bet of *${betDisplayUSD}* on the magnificent Slot Frenzy machine!\nRequesting a spin from the Helper Bot... This may take a moment! Good luck! 🌟✨`;
    
    const sentSpinningMsg = await safeSendMessage(chatId, messageText, {parse_mode: 'MarkdownV2'});
    if (sentSpinningMsg?.message_id) {
        gameData.gameMessageId = sentSpinningMsg.message_id;
        activeGames.set(gameId, gameData);
    } else {
        console.error(`${LOG_PREFIX_SLOT_START} Failed to send initial Slot game message for ${gameId}. Refunding wager.`);
        let refundClient = null;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_slot_setup_fail', {game_id_custom_field: gameId}, `Refund Slot game ${gameId} (message send fail)`);
            await refundClient.query('COMMIT');
        } catch (dbErr) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_SLOT_START} CRITICAL: Refund failed after Slot setup fail for game ${gameId}: ${dbErr.message}`);
        } finally { if (refundClient) refundClient.release(); }
        activeGames.delete(gameId);
        return;
    }
    
    let diceRollValue = null;
    let helperBotError = null;
    let requestId = null;
    let dbPollClient = null;

    try {
        dbPollClient = await pool.connect();
        const requestResult = await insertDiceRollRequest(dbPollClient, gameId, chatId, userId, '🎰', 'Slot Frenzy Spin');
        if (!requestResult.success || !requestResult.requestId) {
            throw new Error(requestResult.error || "Failed to create slot spin request in DB.");
        }
        requestId = requestResult.requestId;
        dbPollClient.release(); dbPollClient = null; 

        let attempts = 0;
        while(attempts < DICE_ROLL_POLLING_MAX_ATTEMPTS) {
            await sleep(DICE_ROLL_POLLING_INTERVAL_MS);
            if (isShuttingDown) { helperBotError = "Shutdown during slot poll."; break; }
            dbPollClient = await pool.connect();
            const statusResult = await getDiceRollRequestResult(dbPollClient, requestId);
            dbPollClient.release(); dbPollClient = null;

            if (statusResult.success && statusResult.status === 'completed') {
                diceRollValue = statusResult.roll_value; break;
            } else if (statusResult.success && statusResult.status === 'error') {
                helperBotError = statusResult.notes || "Helper Bot reported an error with the slot spin."; break;
            }
            attempts++;
        }
        if (diceRollValue === null && !helperBotError) {
            helperBotError = "Timeout waiting for Helper Bot slot spin result.";
            dbPollClient = await pool.connect();
            await dbPollClient.query("UPDATE dice_roll_requests SET status='timeout', notes=$1 WHERE request_id=$2", [helperBotError.substring(0,250), requestId]).catch(e => console.error("Failed to mark slot request as timeout:", e));
            dbPollClient.release(); dbPollClient = null;
        }
        if (helperBotError) throw new Error(helperBotError);
        if (typeof diceRollValue !== 'number') throw new Error ("Invalid slot roll value from helper.");

    } catch (e) {
        if (dbPollClient) dbPollClient.release();
        console.warn(`${LOG_PREFIX_SLOT_START} Failed to get slot result from Helper Bot: ${e.message}`);
        helperBotError = e.message; 
    }
    
    if (helperBotError || diceRollValue === null) {
        const errorMsgToUser = `⚠️ ${playerRef}, there was an issue with your Slot Frenzy spin via the Helper Bot: \`${escapeMarkdownV2(String(helperBotError || "No result from helper").substring(0,150))}\`\nYour bet of *${betDisplayUSD}* has been refunded\\.`;
        const errorKeyboard = createPostGameKeyboard(GAME_IDS.SLOT_FRENZY, betAmountLamports);
        if (gameData.gameMessageId && bot) {
            await bot.editMessageText(errorMsgToUser, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: errorKeyboard }).catch(async () => {
                 await safeSendMessage(String(chatId), errorMsgToUser, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
            });
        } else {
            await safeSendMessage(String(chatId), errorMsgToUser, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        }
        let refundClient = null;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_slot_helper_fail', {game_id_custom_field: gameId}, `Refund Slot game ${gameId} - Helper Bot error`);
            await refundClient.query('COMMIT');
        } catch (dbErr) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_SLOT_START} CRITICAL: Refund failed after Slot helper error for game ${gameId}: ${dbErr.message}`);
        } finally { if (refundClient) refundClient.release(); }
        activeGames.delete(gameId);
        return;
    }

    gameData.diceValue = diceRollValue;
    const payoutInfo = SLOT_PAYOUTS[diceRollValue]; 
    gameData.payoutInfo = payoutInfo;
    let payoutAmountLamports = 0n;
    let profitAmountLamports = 0n;
    let outcomeReasonLog = `loss_slot_val${diceRollValue}`; // Default to loss
    let resultTextPart = "";

    const titleResult = createStandardTitle("Slot Frenzy - The Result!", "🎉");
    messageText = `${titleResult}\n\n${playerRef}'s wager: *${betDisplayUSD}*\nThe Helper Bot spun the reels to: Value *${escapeMarkdownV2(String(diceRollValue))}*\n\n`;

    if (payoutInfo) {
        profitAmountLamports = betAmountLamports * BigInt(payoutInfo.multiplier);
        payoutAmountLamports = betAmountLamports + profitAmountLamports; 
        outcomeReasonLog = `win_slot_val${diceRollValue}_mult${payoutInfo.multiplier}`; 
        resultTextPart = `🌟 **${escapeMarkdownV2(payoutInfo.label)}** ${escapeMarkdownV2(payoutInfo.symbols)} 🌟\nCongratulations! You've won a dazzling *${escapeMarkdownV2(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}* in profit!`;
        gameData.status = 'game_over_win';
    } else {
        payoutAmountLamports = 0n; 
        resultTextPart = `💔 Reel mismatch this time\\.\\.\\. The machine keeps your wager\\. Better luck on the next spin!`;
        gameData.status = 'game_over_loss';
    }
    messageText += resultTextPart;
    
    let finalUserBalanceLamports = userObj.balance; 
    let clientOutcome = null;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, 
            userId, 
            payoutAmountLamports, 
            outcomeReasonLog, 
            { 
                game_id_custom_field: gameId, 
                slot_dice_value: diceRollValue 
            }, 
            `Outcome of Slot Frenzy game ${gameId}. Slot value: ${diceRollValue}.`
        );
        
        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
        } else {
            await clientOutcome.query('ROLLBACK');
            messageText += `\n\n⚠️ A critical error occurred paying out your Slot winnings: \`${escapeMarkdownV2(balanceUpdate.error || "DB Error")}\`\\. Casino staff notified\\.`;
            console.error(`${LOG_PREFIX_SLOT_START} Failed to update balance for Slot game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL SLOT Payout Failure 🚨\nGame ID: \`${escapeMarkdownV2(gameId)}\` User: ${playerRef}\nAmount: \`${formatCurrency(payoutAmountLamports)}\`\nDB Error: \`${escapeMarkdownV2(balanceUpdate.error || "N/A")}\`\\. Manual check needed\\.`, {parse_mode:'MarkdownV2'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_SLOT_START} DB error during Slot outcome for ${gameId}: ${dbError.message}`, dbError.stack?.substring(0,500));
        messageText += `\n\n⚠️ A severe database malfunction occurred with the Slot machine\\. Casino staff notified\\.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    messageText += `\n\nYour new casino balance: *${escapeMarkdownV2(await formatBalanceForDisplay(finalUserBalanceLamports, 'USD'))}*\\.`;
    const postGameKeyboardSlot = createPostGameKeyboard(GAME_IDS.SLOT_FRENZY, betAmountLamports);

    if (gameData.gameMessageId && bot) { 
        await bot.editMessageText(messageText, { chat_id: String(chatId), message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot })
            .catch(async (e) => {
                await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot });
            });
    } else { 
        await safeSendMessage(String(chatId), messageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot });
    }
    activeGames.delete(gameId); 
}

// --- End of Part 5c, Section 4 (NEW) ---
// --- Start of Part 5d (NEW - Full Mines Implementation): Mines Game Logic Handlers ---
// index.js - Part 5d: Mines Game Logic & Callback Handlers
//----------------------------------------------------------------------------------------------------
// Assumed dependencies:
// Part 1: GAME_IDS, MINES_DIFFICULTY_CONFIG, bot, safeSendMessage, escapeMarkdownV2,
//         formatBalanceForDisplay, LAMPORTS_PER_SOL, activeGames, pool, getPlayerDisplayReference,
//         createPostGameKeyboard, generateGameId, MAIN_BOT_KEYPAIR (for logging/errors if needed)
// Part 2: getOrCreateUser, updateUserBalanceAndLedger
// Part 3: createStandardTitle (if you have it, otherwise adapt title creation)
// Part 5a, Section 2: parseBetAmount (used by handleStartMinesCommand, bet is passed to handlers)

const TILE_EMOJI_HIDDEN = '❓'; 
const TILE_EMOJI_GEM = '💎';
const TILE_EMOJI_MINE = '💣';
const TILE_EMOJI_FLAG = '🚩'; // Optional: If you add flagging later
const TILE_EMOJI_EMPTY = ' '; // For revealed empty cells with 0 adjacent mines

// --- Mines Grid Generation ---
async function generateMinesGrid(rows, cols, numMines) {
    const logPrefix = `[GenerateMinesGrid ${rows}x${cols}-${numMines}m]`;
    console.log(`${logPrefix} Starting grid generation.`);

    let grid = Array(rows).fill(null).map(() =>
        Array(cols).fill(null).map(() => ({
            isMine: false,
            isRevealed: false,
            isFlagged: false, // Optional: For flagging
            adjacentMines: 0, 
            display: TILE_EMOJI_HIDDEN 
        }))
    );

    let minesPlaced = 0;
    const mineLocations = [];
    if (numMines >= rows * cols) {
        console.error(`${logPrefix} Number of mines (${numMines}) is too high for grid size ${rows}x${cols}. Setting to max possible minus one.`);
        numMines = rows * cols - 1; // Ensure at least one safe spot
    }

    while (minesPlaced < numMines) {
        const r = Math.floor(Math.random() * rows);
        const c = Math.floor(Math.random() * cols);
        if (!grid[r][c].isMine) {
            grid[r][c].isMine = true;
            mineLocations.push([r, c]);
            minesPlaced++;
        }
    }
    console.log(`${logPrefix} ${minesPlaced} mines placed.`);

    for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
            if (!grid[r][c].isMine) {
                let mineCount = 0;
                for (let dr = -1; dr <= 1; dr++) {
                    for (let dc = -1; dc <= 1; dc++) {
                        if (dr === 0 && dc === 0) continue;
                        const nr = r + dr;
                        const nc = c + dc;
                        if (nr >= 0 && nr < rows && nc >= 0 && nc < cols && grid[nr][nc].isMine) {
                            mineCount++;
                        }
                    }
                }
                grid[r][c].adjacentMines = mineCount;
            }
        }
    }
    console.log(`${logPrefix} Adjacent mine counts calculated.`);
    return { grid, mineLocations };
}


// --- MODIFIED Update Mines Game Message (Renders the Board - Delete & Resend Strategy) ---
async function updateMinesGameMessage(gameData) {
    const logPrefix = `[UpdateMinesMsg GID:${gameData.gameId}]`;
    console.log(`${logPrefix} Updating message (Delete & Resend). Status: ${gameData.status}, Gems: ${gameData.gemsFound}`);

    // Delete the previous game message if it exists
    if (gameData.gameMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.gameMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete old game message ID ${gameData.gameMessageId}: ${e.message}`));
        gameData.gameMessageId = null; // Clear it as it's deleted
    }

    let titleText = `Mines - ${gameData.difficultyLabel || 'Custom'}`;
    try {
        if (typeof createStandardTitle === 'function') titleText = createStandardTitle(titleText, TILE_EMOJI_MINE);
        else titleText = `${TILE_EMOJI_MINE} **${escapeMarkdownV2(titleText)}** ${TILE_EMOJI_MINE}`;
    } catch (e) { titleText = `${TILE_EMOJI_MINE} **${escapeMarkdownV2(titleText)}** ${TILE_EMOJI_MINE}`; }


    const betDisplay = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    let text = `${titleText}\nPlayer: ${gameData.playerRef}, Bet: *${betDisplay}*\n`;
    text += `Mines: ${gameData.numMines}, Grid: ${gameData.rows}x${gameData.cols}\n`;
    
    const currentMultiplier = (gameData.status === 'player_turn' && gameData.gemsFound > 0 && gameData.difficultyKey && MINES_DIFFICULTY_CONFIG[gameData.difficultyKey]?.multipliers[gameData.gemsFound]) 
        ? MINES_DIFFICULTY_CONFIG[gameData.difficultyKey].multipliers[gameData.gemsFound] 
        : (gameData.finalMultiplier || (gameData.gemsFound === 0 ? 0 : 1.0)); // Default to 0x if 0 gems, or 1x if game just started with no gems found

    const potentialPayout = (gameData.status === 'player_turn' && gameData.gemsFound > 0) 
        ? BigInt(Math.floor(Number(gameData.betAmount) * currentMultiplier))
        : (gameData.finalPayout || 0n);

    gameData.currentMultiplier = currentMultiplier; // Store for cashout
    gameData.potentialPayout = potentialPayout;   // Store for cashout

    text += `Gems Found: *${gameData.gemsFound}* ${TILE_EMOJI_GEM}\n`;
    text += `Multiplier: *x${escapeMarkdownV2(currentMultiplier.toFixed(2))}*\n`;
    text += `Potential Payout: *${escapeMarkdownV2(await formatBalanceForDisplay(potentialPayout, 'USD'))}*\n\n`;

    const keyboardRows = [];

    for (let r = 0; r < gameData.rows; r++) {
        const rowButtons = [];
        for (let c = 0; c < gameData.cols; c++) {
            const cell = gameData.grid[r][c];
            let buttonText = TILE_EMOJI_HIDDEN;
            let callbackData = `mines_tile:${gameData.gameId}:${r}:${c}`;

            if (cell.isRevealed) {
                if (cell.isMine) {
                    buttonText = TILE_EMOJI_MINE;
                } else {
                    buttonText = cell.adjacentMines > 0 ? String(cell.adjacentMines) : TILE_EMOJI_EMPTY;
                }
                callbackData = `noop_revealed:${r}:${c}`; 
            } else if (gameData.status !== 'player_turn' && gameData.status !== 'awaiting_difficulty') { 
                 if (cell.isMine) buttonText = TILE_EMOJI_MINE;
                 callbackData = `noop_gameover:${r}:${c}`;
            }
            
            if (buttonText === TILE_EMOJI_EMPTY && cell.isRevealed && !cell.isMine) buttonText = " "; 

            rowButtons.push({ text: buttonText, callback_data: callbackData });
        }
        keyboardRows.push(rowButtons);
    }

    if (gameData.status === 'player_turn') {
        if (gameData.gemsFound > 0) { // Only allow cashout if at least one gem is found
            keyboardRows.push([{ text: `💰 Cash Out (${escapeMarkdownV2(await formatBalanceForDisplay(gameData.potentialPayout, 'USD'))})`, callback_data: `mines_cashout:${gameData.gameId}` }]);
        }
    } else if (gameData.status === 'game_over_mine_hit') {
        text += `💥 *BOOM!* You hit a mine! Game Over.\nYour bet of *${betDisplay}* is lost.`;
        // CreatePostGameKeyboard needs to be defined or adapted
        if(typeof createPostGameKeyboard === 'function') keyboardRows.push(...createPostGameKeyboard(GAME_IDS.MINES, gameData.betAmount).inline_keyboard);
        else keyboardRows.push([{ text: "Play Again?", callback_data: `play_again_mines:${gameData.betAmount.toString()}` }]);
    } else if (gameData.status === 'game_over_cashed_out') {
        text += `🎉 *CASHED OUT!* You safely secured *${escapeMarkdownV2(await formatBalanceForDisplay(gameData.finalPayout, 'USD'))}*!`;
        if(typeof createPostGameKeyboard === 'function') keyboardRows.push(...createPostGameKeyboard(GAME_IDS.MINES, gameData.betAmount).inline_keyboard);
        else keyboardRows.push([{ text: "Play Again?", callback_data: `play_again_mines:${gameData.betAmount.toString()}` }]);
    } else if (gameData.status === 'game_over_all_gems_found') {
        text += `🌟 *PERFECT CLEAR!* You found all *${gameData.gemsFound}* gems and won *${escapeMarkdownV2(await formatBalanceForDisplay(gameData.finalPayout, 'USD'))}*! Incredible!`;
        if(typeof createPostGameKeyboard === 'function') keyboardRows.push(...createPostGameKeyboard(GAME_IDS.MINES, gameData.betAmount).inline_keyboard);
        else keyboardRows.push([{ text: "Play Again?", callback_data: `play_again_mines:${gameData.betAmount.toString()}` }]);
    }
    
    if (gameData.status !== 'player_turn' && keyboardRows.length === gameData.rows) { // if only grid buttons are present after game over
         keyboardRows.push([{ text: "💳 Wallet", callback_data: "menu:wallet" }]);
    } else if (gameData.status === 'player_turn' && keyboardRows.length === gameData.rows) { // No cashout button yet (0 gems)
         keyboardRows.push([{ text: "📖 Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.MINES}`}]);
    }


    const messageOptions = {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: keyboardRows }
    };
    
    const newMsg = await safeSendMessage(gameData.chatId, text, messageOptions);
    if (newMsg?.message_id) {
        gameData.gameMessageId = newMsg.message_id; // Update with the new message ID
        console.log(`${logPrefix} New message sent (ID: ${gameData.gameMessageId}).`);
    } else {
        console.error(`${logPrefix} Failed to send new Mines game message for game ${gameData.gameId}.`);
        // If sending fails, the game might become unplayable. Consider error handling.
    }

    if(activeGames.has(gameData.gameId)) activeGames.set(gameData.gameId, gameData);
}


// --- Handle Mines Difficulty Selection & Game Start ---
async function handleMinesDifficultySelectionCallback(offerId, userObject, difficultyKey, callbackQueryId, originalMessageId, originalChatId, originalChatType) {
    const userId = String(userObject.telegram_id);
    const logPrefix = `[MinesDiffSelect OfferID:${offerId} UID:${userId} Diff:${difficultyKey}]`;
    console.log(`${logPrefix} Processing difficulty selection.`);

    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.MINES_OFFER || offerData.status !== 'awaiting_difficulty') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Mines offer is no longer valid or has expired.", show_alert: true }).catch(()=>{});
        if (originalMessageId && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalMessageId) }).catch(() => {});
        }
        return;
    }

    if (offerData.initiatorId !== userId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the player who started the offer can choose the difficulty.", show_alert: true }).catch(()=>{});
        return;
    }

    const difficultyConfig = MINES_DIFFICULTY_CONFIG[difficultyKey];
    if (!difficultyConfig) {
        console.error(`${logPrefix} Invalid difficulty key: ${difficultyKey}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid difficulty selected. Please try again.", show_alert: true }).catch(()=>{});
        return;
    }

    await bot.answerCallbackQuery(callbackQueryId, { text: `Selected ${difficultyConfig.label}. Starting game...`}).catch(()=>{});

    const betAmountLamports = offerData.betAmount;
    const playerRef = offerData.initiatorMention; 
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));
    const actualGameId = generateGameId(GAME_IDS.MINES);

    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_mines', 
            { game_id_custom_field: actualGameId, difficulty_custom: difficultyKey, mines_custom: difficultyConfig.mines },
            `Bet for Mines game ${actualGameId} (${difficultyConfig.label})`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${logPrefix} Wager placement failed for user ${userId}: ${balanceUpdateResult.error}`);
            const failText = `${playerRef}, your Mines wager of *${betDisplayUSD}* failed: \`${escapeMarkdownV2(balanceUpdateResult.error || "Wallet error")}\`.`;
            if (originalMessageId && bot) { // Edit the difficulty selection message to show error
                 await bot.editMessageText(failText, { chat_id: originalChatId, message_id: Number(originalMessageId), parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{text: "Try Again", callback_data: `play_again_mines:${betAmountLamports.toString()}`}]]}}).catch(()=>{});
            } else {
                await safeSendMessage(originalChatId, failText, { parse_mode: 'MarkdownV2' });
            }
            activeGames.delete(offerId); 
            await updateGroupGameDetails(originalChatId, null, null, null);
            return;
        }
        await client.query('COMMIT');
        
        const updatedUserObjAfterBet = await getOrCreateUser(userId); 

        const { grid, mineLocations } = await generateMinesGrid(difficultyConfig.rows, difficultyConfig.cols, difficultyConfig.mines);

        const initialMultiplier = MINES_DIFFICULTY_CONFIG[difficultyKey].multipliers[0] || 0; // Multiplier for 0 gems revealed
        const initialPotentialPayout = BigInt(Math.floor(Number(betAmountLamports) * initialMultiplier));


        const gameData = {
            type: GAME_IDS.MINES,
            gameId: actualGameId,
            chatId: offerData.chatId,
            userId: userId,
            playerRef: playerRef,
            userObj: updatedUserObjAfterBet, 
            betAmount: betAmountLamports,
            rows: difficultyConfig.rows,
            cols: difficultyConfig.cols,
            numMines: difficultyConfig.mines,
            difficultyKey: difficultyKey, 
            difficultyLabel: difficultyConfig.label,
            grid: grid, 
            mineLocations: mineLocations,
            revealedTiles: [], 
            gemsFound: 0,
            currentMultiplier: initialMultiplier,
            potentialPayout: initialPotentialPayout, 
            status: 'player_turn', 
            gameMessageId: originalMessageId, 
            lastInteractionTime: Date.now()
        };
        
        activeGames.set(actualGameId, gameData);
        activeGames.delete(offerId); 
        await updateGroupGameDetails(originalChatId, actualGameId, GAME_IDS.MINES, betAmountLamports);

        console.log(`${logPrefix} Mines game ${actualGameId} started by ${userId}. Grid: ${gameData.rows}x${gameData.cols}, Mines: ${gameData.numMines}. Bet: ${betAmountLamports}`);
        await updateMinesGameMessage(gameData); 

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} DB Rollback Error: ${rbErr.message}`));
        console.error(`${logPrefix} Error starting Mines game after difficulty selection: ${error.message}`, error.stack?.substring(0,700));
        const errorText = `⚙️ Oops! A critical error occurred while starting your Mines game: \`${escapeMarkdownV2(error.message)}\`.`;
        if (originalMessageId && bot) { // Edit original difficulty message
            await bot.editMessageText(errorText, { chat_id: originalChatId, message_id: Number(originalMessageId), parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{text: "Dismiss", callback_data:"noop_ok"}]] } }).catch(()=>{});
        } else {
            await safeSendMessage(originalChatId, errorText, { parse_mode: 'MarkdownV2'});
        }
        activeGames.delete(offerId);
        activeGames.delete(actualGameId); 
        await updateGroupGameDetails(originalChatId, null, null, null);
    } finally {
        if (client) client.release();
    }
}


// --- Handle Tile Click in Mines Game ---
async function handleMinesTileClickCallback(gameId, userObject, r, c, callbackQueryId, originalMessageId, originalChatId) {
    const userId = String(userObject.telegram_id);
    const logPrefix = `[MinesTileClick GID:${gameId} UID:${userId} Tile:${r},${c}]`;
    console.log(`${logPrefix} Processing tile click.`);

    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.MINES) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Mines game is no longer active.", show_alert: true }).catch(()=>{});
        return;
    }
    if (gameData.userId !== userId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This is not your Mines game.", show_alert: true }).catch(()=>{});
        return;
    }
    if (gameData.status !== 'player_turn') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "The game is over or not your turn.", show_alert: false }).catch(()=>{});
        return;
    }
    if (r < 0 || r >= gameData.rows || c < 0 || c >= gameData.cols) {
        console.error(`${logPrefix} Invalid tile coordinates.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid tile selected.", show_alert: true }).catch(()=>{});
        return;
    }

    const cell = gameData.grid[r][c];
    if (cell.isRevealed) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This tile is already revealed.", show_alert: false }).catch(()=>{});
        return;
    }

    await bot.answerCallbackQuery(callbackQueryId).catch(()=>{}); 

    cell.isRevealed = true;
    gameData.lastInteractionTime = Date.now();

    if (cell.isMine) {
        console.log(`${logPrefix} Player hit a mine at ${r},${c}. Game Over.`);
        gameData.status = 'game_over_mine_hit';
        gameData.finalPayout = 0n; 
        gameData.finalMultiplier = 0;
        
        for (let i = 0; i < gameData.rows; i++) {
            for (let j = 0; j < gameData.cols; j++) {
                if (gameData.grid[i][j].isMine) {
                    gameData.grid[i][j].isRevealed = true;
                }
            }
        }
        await updateMinesGameMessage(gameData);
        activeGames.delete(gameId); 
        await updateGroupGameDetails(originalChatId, null, null, null);
    } else {
        gameData.gemsFound++;
        if (!gameData.revealedTiles.find(tile => tile[0] === r && tile[1] === c)) {
            gameData.revealedTiles.push([r,c]);
        }
        console.log(`${logPrefix} Player found a gem at ${r},${c}. Total gems: ${gameData.gemsFound}`);

        if (cell.adjacentMines === 0) {
            const queue = [[r, c]];
            const visitedForFloodFill = new Set([`${r}-${c}`]); // Keep track of visited cells in this flood fill
            
            while (queue.length > 0) {
                const [currR, currC] = queue.shift();
                for (let dr = -1; dr <= 1; dr++) {
                    for (let dc = -1; dc <= 1; dc++) {
                        const nr = currR + dr;
                        const nc = currC + dc;

                        if (nr >= 0 && nr < gameData.rows && nc >= 0 && nc < gameData.cols && !visitedForFloodFill.has(`${nr}-${nc}`)) {
                            const nextCell = gameData.grid[nr][nc];
                            if (!nextCell.isRevealed && !nextCell.isMine) {
                                nextCell.isRevealed = true;
                                visitedForFloodFill.add(`${nr}-${nc}`);
                                if (!gameData.revealedTiles.find(tile => tile[0] === nr && tile[1] === nc)) {
                                    gameData.revealedTiles.push([nr,nc]);
                                    gameData.gemsFound++;
                                }
                                if (nextCell.adjacentMines === 0) {
                                    queue.push([nr, nc]);
                                }
                            }
                        }
                    }
                }
            }
            console.log(`${logPrefix} Flood fill complete. Total gems now: ${gameData.gemsFound}`);
        }

        const multipliers = MINES_DIFFICULTY_CONFIG[gameData.difficultyKey]?.multipliers;
        if (multipliers && gameData.gemsFound < multipliers.length) {
            gameData.currentMultiplier = multipliers[gameData.gemsFound];
        } else if (multipliers && multipliers.length > 0) {
            gameData.currentMultiplier = multipliers[multipliers.length - 1]; // Max multiplier if exceeded
             console.warn(`${logPrefix} Gems found (${gameData.gemsFound}) exceeds defined multipliers length. Using max multiplier.`);
        } else {
            console.warn(`${logPrefix} Multiplier array not found for difficulty ${gameData.difficultyKey} or is empty. Defaulting multiplier.`);
            gameData.currentMultiplier = 1.0 + (0.1 * gameData.gemsFound); // Basic fallback
        }
        gameData.potentialPayout = BigInt(Math.floor(Number(gameData.betAmount) * gameData.currentMultiplier));

        const totalNonMineCells = (gameData.rows * gameData.cols) - gameData.numMines;
        if (gameData.gemsFound >= totalNonMineCells) { // >= just in case flood fill overcounts slightly due to complex reveal path
            console.log(`${logPrefix} Player found all ${totalNonMineCells} gems! Max Win!`);
            gameData.status = 'game_over_all_gems_found';
            gameData.finalPayout = gameData.potentialPayout;
            gameData.finalMultiplier = gameData.currentMultiplier;
            
            let client = null;
            try {
                client = await pool.connect();
                await client.query('BEGIN');
                const winLedgerResult = await updateUserBalanceAndLedger(
                    client, userId, gameData.finalPayout,
                    'game_win_mines_all_gems', 
                    { game_id_custom_field: gameId, difficulty_custom: gameData.difficultyKey, gems_custom: gameData.gemsFound, payout_multiplier_custom: gameData.finalMultiplier.toFixed(4) },
                    `Mines win (${gameData.difficultyLabel}) - All gems. Payout: ${formatCurrency(gameData.finalPayout, 'SOL')}`
                );
                if (!winLedgerResult.success) throw new Error(winLedgerResult.error || "Failed to record Mines max win to ledger.");
                await client.query('COMMIT');
                gameData.userObj.balance = winLedgerResult.newBalanceLamports; 

            } catch (dbError) {
                if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} DB Rollback Error on max win: ${rbErr.message}`));
                console.error(`${logPrefix} DB Error processing Mines max win: ${dbError.message}`);
                await safeSendMessage(userId, "Error processing your max win payout. Please contact support.", {parse_mode:'MarkdownV2'});
            } finally {
                if (client) client.release();
            }
            await updateMinesGameMessage(gameData); // Show final board before deleting
            activeGames.delete(gameId); 
            await updateGroupGameDetails(originalChatId, null, null, null);
            return; // Game ended
        }
        await updateMinesGameMessage(gameData);
    }
    // If game didn't end, re-save the updated gameData
    if (activeGames.has(gameId)) {
        activeGames.set(gameId, gameData); 
    }
}


// --- Handle Cash Out in Mines Game ---
async function handleMinesCashOutCallback(gameId, userObject, callbackQueryId, originalMessageId, originalChatId) {
    const userId = String(userObject.telegram_id);
    const logPrefix = `[MinesCashOut GID:${gameId} UID:${userId}]`;
    console.log(`${logPrefix} Processing cash out.`);

    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.MINES) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Mines game is no longer active.", show_alert: true }).catch(()=>{});
        return;
    }
    if (gameData.userId !== userId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This is not your Mines game.", show_alert: true }).catch(()=>{});
        return;
    }
    if (gameData.status !== 'player_turn') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "You can only cash out during your turn.", show_alert: false }).catch(()=>{});
        return;
    }
    if (gameData.gemsFound === 0) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "You need to find at least one gem to cash out!", show_alert: true }).catch(()=>{});
        return;
    }

    await bot.answerCallbackQuery(callbackQueryId, { text: "Cashing out..." }).catch(()=>{});

    gameData.status = 'game_over_cashed_out';
    // potentialPayout should be the total amount won (bet * multiplier)
    gameData.finalPayout = gameData.potentialPayout; 
    gameData.finalMultiplier = gameData.currentMultiplier;
    gameData.lastInteractionTime = Date.now();

    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        // The bet was already deducted. So, credit the finalPayout.
        const cashoutResult = await updateUserBalanceAndLedger(
            client, userId, gameData.finalPayout, 
            'game_win_mines_cashout',
            { game_id_custom_field: gameId, difficulty_custom: gameData.difficultyKey, gems_custom: gameData.gemsFound, payout_multiplier_custom: gameData.finalMultiplier.toFixed(4) },
            `Mines cash out (${gameData.difficultyLabel}). Payout: ${formatCurrency(gameData.finalPayout, 'SOL')}`
        );

        if (!cashoutResult.success) {
            throw new Error(cashoutResult.error || "Failed to update balance on cash out.");
        }
        await client.query('COMMIT');
        gameData.userObj.balance = cashoutResult.newBalanceLamports; 

        console.log(`${logPrefix} Player cashed out ${gameData.finalPayout} lamports. New balance: ${cashoutResult.newBalanceLamports}`);

    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} DB Rollback Error: ${rbErr.message}`));
        console.error(`${logPrefix} DB Error processing Mines cash out: ${dbError.message}`);
        gameData.status = 'player_turn'; 
        gameData.finalPayout = 0n;       
        gameData.finalMultiplier = 0;
        await safeSendMessage(userId, "⚙️ Error processing your cash out. Your game is still active. Please try cashing out again or contact support.", { parse_mode: 'MarkdownV2' });
        activeGames.set(gameId, gameData); 
        await updateMinesGameMessage(gameData); 
        return; 
    } finally {
        if (client) client.release();
    }
    
    await updateMinesGameMessage(gameData); // Show final cashed-out state
    activeGames.delete(gameId); 
    await updateGroupGameDetails(originalChatId, null, null, null);
}


// --- Handle Cancel Mines Offer ---
async function handleMinesCancelOfferCallback(offerId, userObject, originalMessageId, originalChatId, callbackQueryId) {
    const userId = String(userObject.telegram_id);
    const logPrefix = `[MinesCancelOffer OfferID:${offerId} UID:${userId}]`;
    console.log(`${logPrefix} Processing offer cancellation.`);

    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.MINES_OFFER || offerData.status !== 'awaiting_difficulty') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Mines offer is no longer valid or has expired.", show_alert: false }).catch(()=>{});
        if (originalMessageId && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalMessageId) }).catch(() => {});
        }
        return;
    }

    if (offerData.initiatorId !== userId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the player who started this offer can cancel it.", show_alert: true }).catch(()=>{});
        return;
    }

    await bot.answerCallbackQuery(callbackQueryId, { text: "Mines game offer cancelled." }).catch(()=>{});
    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null); 

    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(offerData.betAmount, 'USD'));
    const cancelText = `🚫 The Mines game offer by ${offerData.initiatorMention} for *${betDisplayUSD}* has been cancelled.`;
    if (originalMessageId && bot) {
        await bot.editMessageText(cancelText, {
            chat_id: originalChatId,
            message_id: Number(originalMessageId),
            parse_mode: 'MarkdownV2',
            reply_markup: {} 
        }).catch(e => console.warn(`${logPrefix} Error editing cancelled mines offer message: ${e.message}`));
    }
}
// --- End of Part 5d (NEW - Full Mines Implementation) ---
// --- Start of Part 5a, Section 2 (REVISED for New Dice Escalator Rules): General Command Handler Implementations ---
// index.js - Part 5a, Section 2: General Casino Bot Command Implementations
//----------------------------------------------------------------------------------
// Assumed dependencies from previous Parts:
// Part 1: safeSendMessage, escapeMarkdownV2, bot, BOT_NAME, BOT_VERSION, ADMIN_USER_ID, pool,
//         MIN_BET_USD_val, MAX_BET_USD_val, MIN_BET_AMOUNT_LAMPORTS_config, MAX_BET_AMOUNT_LAMPORTS_config,
//         TARGET_JACKPOT_SCORE (for DE PvB), DICE_ESCALATOR_BUST_ON (for DE Player),
//         DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE, MAIN_JACKPOT_ID (for DE PvB), GAME_IDS,
//         OU7_PAYOUT_NORMAL, OU7_PAYOUT_SEVEN, OU7_DICE_COUNT, DUEL_DICE_COUNT,
//         LADDER_ROLL_COUNT, LADDER_BUST_ON, LADDER_PAYOUTS, SLOT_PAYOUTS,
//         RULES_CALLBACK_PREFIX, QUICK_DEPOSIT_CALLBACK_ACTION, WITHDRAW_CALLBACK_ACTION, LAMPORTS_PER_SOL,
//         getSolUsdPrice, convertUSDToLamports, convertLamportsToUSDString, userStateCache,
//         MINES_DIFFICULTY_CONFIG, MINES_MIN_MINES, MINES_MAX_MINES_PERCENT, MINES_DEFAULT_ROWS, MINES_DEFAULT_COLS, MINES_FALLBACK_DEFAULT_MINES, JOIN_GAME_TIMEOUT_MS 
// Part 2: getOrCreateUser, getUserBalance, queryDatabase, getUserByReferralCode, generateReferralCode, findRecipientUser
// Part 3: getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay, generateGameId
// Part 5a-S4 (Shared UI): createPostGameKeyboard
// Part P2: updateUserBalanceAndLedger
// Part P3: clearUserState

// --- Command Handler Functions (General Casino Bot Commands) ---

async function handleStartCommand(msg, args) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_START = `[StartCmd UID:${userId} CH:${chatId}]`;

    if (typeof clearUserState === 'function') {
        clearUserState(userId);
    } else {
        userStateCache.delete(userId);
    }

    let userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) {
        await safeSendMessage(chatId, "😕 Error fetching your player profile. Please try typing `/start` again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(userObject);
    let botUsername = BOT_NAME || "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { console.error(`${LOG_PREFIX_START} Could not fetch bot username: ${e.message}`); }

    if (args && args[0]) {
        const deepLinkParam = args[0];
        console.log(`${LOG_PREFIX_START} Processing deep link parameter: ${deepLinkParam}`);

        if (deepLinkParam.startsWith('ref_')) {
            const refCode = deepLinkParam.substring(4);
            const referrerUserRecord = await getUserByReferralCode(refCode);
            let refByDisplay = "a fellow player";

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
                        userObject = await getOrCreateUser(userId); 
                        console.log(`${LOG_PREFIX_START} User ${userId} successfully linked to referrer ${referrerUserRecord.telegram_id} via ref_code ${refCode}.`);
                    } catch (refError) {
                        await client.query('ROLLBACK');
                        console.error(`${LOG_PREFIX_START} Error linking referral for user ${userId} via code ${refCode}:`, refError);
                    } finally {
                        client.release();
                    }
                } else if (String(userObject.referrer_telegram_id) === String(referrerUserRecord.telegram_id)) {
                    // Already referred by this person
                } else {
                    const existingReferrer = await getOrCreateUser(userObject.referrer_telegram_id);
                    if(existingReferrer) refByDisplay = getPlayerDisplayReference(existingReferrer) + " (your original referrer)";
                    else refByDisplay = "your original referrer";
                }
            } else if (referrerUserRecord && String(referrerUserRecord.telegram_id) === userId) {
                refByDisplay = "yourself (clever try! 😉)";
            }

            const referralMsg = `👋 Welcome, ${playerRef}! You joined via ${refByDisplay}. Explore the casino with \`/help\`!`;
            if (chatType !== 'private') {
                if(msg.message_id) await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
                await safeSendMessage(chatId, `${playerRef}, welcome! I've sent more info to our private chat: @${escapeMarkdownV2(botUsername)} 📬`, { parse_mode: 'MarkdownV2' });
                await safeSendMessage(userId, referralMsg, { parse_mode: 'MarkdownV2' });
            } else {
                await safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2' });
            }
            await handleHelpCommand({ ...msg, from: { ...msg.from, id: userId, username: userObject.username, first_name: userObject.first_name }, chat: { id: userId, type: 'private' }});
            return;
        } else if (deepLinkParam.startsWith('cb_') || deepLinkParam.startsWith('menu_')) { 
            const actionDetails = deepLinkParam.startsWith('cb_') ? deepLinkParam.substring(3) : deepLinkParam.substring(5);
            const [actionName, ...actionParams] = actionDetails.split('_');
            console.log(`${LOG_PREFIX_START} Deep link for menu/callback action: ${actionName}, Params: ${actionParams.join(',')}`);
            
            const userGuidanceText = `👋 Welcome back, ${playerRef}!\nTaking you to the requested section. You can always type \`/help\` for main options.`;
            await safeSendMessage(userId, userGuidanceText, {parse_mode: 'MarkdownV2'});
            
            if (typeof handleMenuAction === 'function') {
                 await handleMenuAction(userId, userId, null, actionName, actionParams, false, 'private');
            } else {
                 await handleHelpCommand({ ...msg, from: { ...msg.from, id: userId, username: userObject.username, first_name: userObject.first_name }, chat: { id: userId, type: 'private' }});
            }
            return;
        }
    }

    if (chatType !== 'private') {
        if(msg.message_id && chatId !== userId) await bot.deleteMessage(chatId, msg.message_id).catch(() => {});
        await safeSendMessage(chatId, `👋 Welcome, ${playerRef}! For commands & casino actions, please DM me: @${escapeMarkdownV2(botUsername)} 📬`, { parse_mode: 'MarkdownV2' });
        await handleHelpCommand({ ...msg, from: { ...msg.from, id: userId, username: userObject.username, first_name: userObject.first_name }, chat: { id: userId, type: 'private' }});
    } else { 
        await safeSendMessage(userId, `🎉 Welcome to **${escapeMarkdownV2(BOT_NAME)}**, ${playerRef}! Type \`/help\` for a list of commands and features.`, { parse_mode: 'MarkdownV2' });
        await handleHelpCommand(msg);
    }
}

async function handleHelpCommand(originalMessageObject) {
    let userId = String(originalMessageObject.from?.id || originalMessageObject.from?.telegram_id);
    let userFirstName = originalMessageObject.from?.first_name;
    let userUsername = originalMessageObject.from?.username;
    let userLastName = originalMessageObject.from?.last_name;

    if (!userId || userId === "undefined") {
        console.error("[HelpCmd] CRITICAL: Could not determine userId for HelpCommand.");
        const errorChatId = originalMessageObject.chat?.id || ADMIN_USER_ID || 'unknown_chat';
        if (errorChatId) {
            await safeSendMessage(errorChatId, "Sorry, an error occurred fetching help. Try `/start`.", { parse_mode: 'MarkdownV2' });
        }
        return;
    }

    const chatId = String(originalMessageObject.chat.id);
    const userObj = await getOrCreateUser(userId, userUsername, userFirstName, userLastName);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Error fetching your profile for help. Please try `/start` again.", { parse_mode: 'MarkdownV2' });
        return;
    }

    const playerMention = getPlayerDisplayReference(userObj);
    const botNameEscaped = escapeMarkdownV2(BOT_NAME);
    let botUsername = BOT_NAME;
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { /* Reduced log */ }

    const minBetUsdDisplay = `$${MIN_BET_USD_val.toFixed(2)}`;
    let referenceMinSol = "";
    try {
        const solPrice = await getSolUsdPrice();
        const minBetLamportsDynamic = convertUSDToLamports(MIN_BET_USD_val, solPrice);
        referenceMinSol = ` (${escapeMarkdownV2(formatCurrency(minBetLamportsDynamic, 'SOL'))} approx.)`;
    } catch (priceErr) { /* Fallback to no SOL equiv if price error */ }


    const helpTextParts = [
        `🌟 Welcome to **${botNameEscaped}**, ${playerMention}! Here are your commands:`,
        `\n*👤 Account & Wallet:*`,
        `▫️ \`/balance\` - Check your funds.`,
        `▫️ \`/wallet\` - Manage deposits, withdrawals & linked SOL address (DM for details).`,
        `▫️ \`/deposit\` - Get deposit address (DM for details).`,
        `▫️ \`/withdraw\` - Withdraw SOL (DM for details).`,
        `▫️ \`/setwallet <address>\` - Link/update your SOL wallet (DM for privacy).`,
        `▫️ \`/history\` - View recent activity (DM for details).`,
        `▫️ \`/referral\` - Get your referral link (DM for details).`,
        `▫️ \`/tip <@user_or_id> <amount_usd> [msg]\` - Tip another player.`,
        `\n*🎲 Games (Play in Groups):*`,
        `Use \`<bet>\` in USD (e.g. \`5\`) or SOL (e.g. \`0.1 sol\`). Min bet: *${escapeMarkdownV2(minBetUsdDisplay)}*${referenceMinSol}`,
        `▫️ \`/coinflip <bet>\` - 🪙 Heads or Tails.`,
        `▫️ \`/rps <bet>\` - 🪨📄✂️ Rock Paper Scissors.`,
        `▫️ \`/de <bet>\` - 🎲 Dice Escalator (PvP/PvB - Jackpot!).`,
        `▫️ \`/d21 <bet>\` - 🃏 Dice Blackjack (PvP/PvB).`,
        `▫️ \`/duel <bet>\` - ⚔️ High Roller Dice Duel (PvP/PvB).`,
        `▫️ \`/ou7 <bet>\` - 🎲 Over/Under 7 (vs Bot).`,
        `▫️ \`/ladder <bet>\` - 🪜 Greed's Ladder (vs Bot).`,
        `▫️ \`/s7 <bet>\` - 🎲 Sevens Out / Fast Craps (vs Bot).`,
        `▫️ \`/slot <bet>\` - 🎰 Slot Frenzy (vs Bot).`,
        `▫️ \`/mines <bet>\` - 💣 Minesweeper - Choose difficulty via buttons (vs Bot).`, // MODIFIED
        `\n*📖 Info:*`,
        `▫️ \`/rules\` - Detailed game rules (DM for full menu).`,
        `▫️ \`/jackpot\` - Check Dice Escalator PvB Jackpot.`,
        `\n💡 Tip: For wallet actions, please DM me: @${escapeMarkdownV2(botUsername)}`
    ];

    const helpMessage = helpTextParts.filter(Boolean).join('\n');
    const helpKeyboard = {
        inline_keyboard: [
            [{ text: "💳 My Wallet", callback_data: "menu:wallet" }, { text: "📖 Game Rules", callback_data: "show_rules_menu" }],
            [{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]
        ]
    };
    await safeSendMessage(chatId, helpMessage, { parse_mode: 'MarkdownV2', reply_markup: helpKeyboard, disable_web_page_preview: true });
}

// --- MODIFIED handleStartMinesCommand function ---
async function handleStartMinesCommand(msg, args, userObj) {
    const userId = String(userObj.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_MINES_START = `[Mines_StartOffer UID:${userId} CH:${chatId}]`;

    const playerRef = getPlayerDisplayReference(userObj);
    let betAmountLamports;

    if (chatType === 'private') {
        await safeSendMessage(chatId, `${playerRef}, the Mines game is initiated in a group chat. Please use \`/mines <bet>\` there to choose a difficulty and play!`, { parse_mode: 'MarkdownV2' });
        return;
    }
    
    try {
        betAmountLamports = await parseBetAmount(args[0], chatId, msg.chat.type, userId);
        if (!betAmountLamports || betAmountLamports <= 0n) {
            await safeSendMessage(chatId, `${playerRef}, please specify a valid positive bet amount for Mines. Example: \`/mines 10\` or \`/mines 0.1 sol\``, { parse_mode: 'MarkdownV2' });
            return;
        }
    } catch (e) {
        console.error(`${LOG_PREFIX_MINES_START} Error parsing bet amount: ${e.message}`);
        await safeSendMessage(chatId, `${playerRef}, there was an issue with your bet amount. Please use USD (e.g., \`5\`) or SOL (e.g., \`0.1 sol\`).`, { parse_mode: 'MarkdownV2' });
        return;
    }

    console.log(`${LOG_PREFIX_MINES_START} Initiating Mines offer. Bet: ${betAmountLamports} lamports.`);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    const currentUserDetails = await getOrCreateUser(userId); 
    if (!currentUserDetails || BigInt(currentUserDetails.balance) < betAmountLamports) {
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports - BigInt(currentUserDetails?.balance || 0), 'USD'));
        await safeSendMessage(chatId, `${playerRef}, your balance is too low for a *${betDisplayUSD}* Mines game. You need about *${neededDisplay}* more.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const offerId = generateGameId(GAME_IDS.MINES_OFFER); 

    const offerData = {
        type: GAME_IDS.MINES_OFFER,
        gameId: offerId, 
        chatId: chatId,
        initiatorId: userId,
        initiatorMention: playerRef,
        initiatorUserObj: currentUserDetails, 
        betAmount: betAmountLamports,
        status: 'awaiting_difficulty', 
        creationTime: Date.now(),
        offerMessageId: null 
    };
    activeGames.set(offerId, offerData);
    await updateGroupGameDetails(chatId, offerId, GAME_IDS.MINES_OFFER, betAmountLamports); // Track this offer in the group session

    let difficultyButtons = [];
    for (const diffKey in MINES_DIFFICULTY_CONFIG) { // MINES_DIFFICULTY_CONFIG from Part 1
        const diffConfig = MINES_DIFFICULTY_CONFIG[diffKey];
        difficultyButtons.push({ text: diffConfig.label, callback_data: `mines_difficulty_select:${offerId}:${diffKey}` });
    }
    
    const difficultyKeyboardRows = [];
    for (let i = 0; i < difficultyButtons.length; i += 2) { // Arrange buttons in rows of 2 max
        difficultyKeyboardRows.push(difficultyButtons.slice(i, i + 2));
    }
    difficultyKeyboardRows.push([{ text: "❌ Cancel Offer", callback_data: `mines_cancel_offer:${offerId}` }]);


    const offerMessageText = `💣 **Mines Challenge by ${playerRef}!** 💣\n\nWager: *${betDisplayUSD}*\n\n${playerRef}, please select your desired difficulty level below to start the game:`;
    
    const sentMessage = await safeSendMessage(chatId, offerMessageText, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: difficultyKeyboardRows }
    });

    if (sentMessage?.message_id) {
        const currentOffer = activeGames.get(offerId);
        if (currentOffer) {
            currentOffer.offerMessageId = sentMessage.message_id;
            activeGames.set(offerId, currentOffer);
        }
        
        setTimeout(async () => {
            const timedOutOffer = activeGames.get(offerId);
            if (timedOutOffer && timedOutOffer.status === 'awaiting_difficulty') {
                console.log(`${LOG_PREFIX_MINES_START} Mines offer ${offerId} timed out waiting for difficulty selection.`);
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null); // Clear game from group session
                if (timedOutOffer.offerMessageId && bot) {
                    await bot.editMessageText(
                        `⏳ The Mines game offer by ${timedOutOffer.initiatorMention} for *${betDisplayUSD}* expired as no difficulty was chosen.`,
                        { chat_id: String(chatId), message_id: Number(timedOutOffer.offerMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }
                    ).catch(e => console.warn(`${LOG_PREFIX_MINES_START} Error editing timed out mines offer msg: ${e.message}`));
                }
            }
        }, JOIN_GAME_TIMEOUT_MS); // Use existing timeout constant
    } else {
        console.error(`${LOG_PREFIX_MINES_START} Failed to send Mines difficulty selection message.`);
        activeGames.delete(offerId); 
        await updateGroupGameDetails(chatId, null, null, null);
        await safeSendMessage(chatId, "⚙️ Oops! Couldn't start the Mines game offer. Please try again.", { parse_mode: 'MarkdownV2' });
    }
}
// --- End of modified handleStartMinesCommand function ---

async function handleBalanceCommand(msg) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_BAL = `[BalanceCmd UID:${userId} CH:${commandChatId}]`;

    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) {
        await safeSendMessage(commandChatId, "😕 Apologies! We couldn't fetch your profile to show your balance. Please try \`/start\` again.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(user);
    let botUsername = BOT_NAME || "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { /* console.error(`${LOG_PREFIX_BAL} Could not fetch bot username: ${e.message}`); */ } 

    const balanceLamports = await getUserBalance(userId);
    if (balanceLamports === null) {
        const errorMsgDm = "🏦 Oops! We couldn't retrieve your balance right now. This is unusual. Please try again in a moment, or contact support if this issue persists.";
        await safeSendMessage(userId, errorMsgDm, { parse_mode: 'MarkdownV2' }); 
        if (chatType !== 'private') {
            if (msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(() => {});
            await safeSendMessage(commandChatId, `${playerRef}, there was a hiccup fetching your balance. I've sent details to your DMs with @${escapeMarkdownV2(botUsername)}.`, { parse_mode: 'MarkdownV2' });
        }
        return;
    }

    const balanceUSDShort = await formatBalanceForDisplay(balanceLamports, 'USD');
    const balanceSOLShort = formatCurrency(balanceLamports, 'SOL');

    if (chatType !== 'private') {
        if (msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(() => {});
        const groupBalanceMessage = `${playerRef}, your current war chest holds approx. *${escapeMarkdownV2(balanceUSDShort)}* / *${escapeMarkdownV2(balanceSOLShort)}*. 💰\nFor a detailed breakdown and wallet actions, please check your DMs with me: @${escapeMarkdownV2(botUsername)} 📬`;
        await safeSendMessage(commandChatId, groupBalanceMessage, { parse_mode: 'MarkdownV2' });
    }
    
    const balanceMessageDm = `🏦 **Your Casino Royale Account Statement** 🏦\n\n` +
        `Player: ${playerRef}\n` +
        `-------------------------------\n` + // MarkdownV2 safe divider
        `💰 Approx. Total Value: *${escapeMarkdownV2(balanceUSDShort)}*\n` +
        `🪙 SOL Balance: *${escapeMarkdownV2(balanceSOLShort)}*\n` +
        `⚙️ Lamports: \`${escapeMarkdownV2(String(balanceLamports))}\`\n` +
        `-------------------------------\n\n` + // MarkdownV2 safe divider
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


async function handleTipCommand(msg, args, tipperUserObj) {
    const chatId = String(msg.chat.id);
    const tipperId = String(tipperUserObj.telegram_id);
    const logPrefix = `[TipCmd UID:${tipperId} CH:${chatId}]`;

    console.log(`${logPrefix} Initiated. Tipper: ${tipperUserObj.username || tipperId}, Args: [${args.join(', ')}]`);

    if (args.length < 2) {
        await safeSendMessage(chatId, "💡 Usage: `/tip <@username_or_id> <amount_usd> [message]`\nExample: `/tip @LuckyWinner 5 Great game!`", { parse_mode: 'MarkdownV2' });
        return;
    }

    const recipientIdentifier = args[0];
    const amountUSDStr = args[1];
    const tipMessage = args.slice(2).join(' ').trim() || null;

    const recipientUserObj = await findRecipientUser(recipientIdentifier);

    if (!recipientUserObj) {
        await safeSendMessage(chatId, `😕 Player "${escapeMarkdownV2(recipientIdentifier)}" not found. Please check the username or Telegram ID and ensure they have interacted with the bot before.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const recipientId = String(recipientUserObj.telegram_id);

    if (tipperId === recipientId) {
        await safeSendMessage(chatId, "😜 You can't tip yourself, generous soul!", { parse_mode: 'MarkdownV2' });
        return;
    }

    let tipAmountUSD;
    try {
        tipAmountUSD = parseFloat(amountUSDStr);
        if (isNaN(tipAmountUSD) || tipAmountUSD <= 0) {
            throw new Error("Tip amount must be a positive number.");
        }
    } catch (e) {
        await safeSendMessage(chatId, `⚠️ Invalid tip amount: "${escapeMarkdownV2(amountUSDStr)}". Please specify a valid USD amount (e.g., \`5\` or \`2.50\`).`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let tipAmountLamports;
    let solPrice;
    try {
        solPrice = await getSolUsdPrice();
        tipAmountLamports = convertUSDToLamports(tipAmountUSD, solPrice);
    } catch (priceError) {
        console.error(`${logPrefix} Error getting SOL price or converting tip to lamports: ${priceError.message}`);
        await safeSendMessage(chatId, "⚙️ Apologies, there was an issue fetching the current SOL price to process your tip. Please try again in a moment.", { parse_mode: 'MarkdownV2' });
        return;
    }

    if (tipAmountLamports <= 0n) {
        await safeSendMessage(chatId, `⚠️ Tip amount is too small after conversion. Please try a slightly larger USD amount.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const currentTipperDetails = await getOrCreateUser(tipperId);
    if (!currentTipperDetails) {
         await safeSendMessage(chatId, `⚙️ Error fetching your profile for tipping. Please try \`/start\` and then tip again.`, { parse_mode: 'MarkdownV2' });
         return;
    }
    const tipperCurrentBalance = BigInt(currentTipperDetails.balance);

    if (tipperCurrentBalance < tipAmountLamports) {
        const neededDisplay = escapeMarkdownV2(await formatBalanceForDisplay(tipAmountLamports - tipperCurrentBalance, 'USD', solPrice));
        await safeSendMessage(chatId, `💰 Oops! Your balance is too low to send a *${escapeMarkdownV2(tipAmountUSD.toFixed(2))} USD* tip. You need about *${neededDisplay}* more.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const tipperName = getPlayerDisplayReference(currentTipperDetails);
        const recipientName = getPlayerDisplayReference(recipientUserObj);
        const ledgerNoteTipper = `Tip sent to ${recipientName}${tipMessage ? ` (Msg: ${tipMessage.substring(0, 50)})` : ''}`;
        const ledgerNoteRecipient = `Tip received from ${tipperName}${tipMessage ? ` (Msg: ${tipMessage.substring(0, 50)})` : ''}`;

        const debitResult = await updateUserBalanceAndLedger(
            client,
            tipperId,
            -tipAmountLamports,
            'tip_sent',
            { }, 
            ledgerNoteTipper
        );

        if (!debitResult.success) {
            throw new Error(debitResult.error || "Failed to debit your balance for the tip.");
        }

        const creditResult = await updateUserBalanceAndLedger(
            client,
            recipientId,
            tipAmountLamports,
            'tip_received',
            { }, 
            ledgerNoteRecipient
        );

        if (!creditResult.success) {
            console.error(`${logPrefix} CRITICAL: Debited tipper ${tipperId} but failed to credit recipient ${recipientId}. Amount: ${tipAmountLamports}. Error: ${creditResult.error}`);
            throw new Error(creditResult.error || "Failed to credit recipient's balance after debiting yours. The transaction has been reversed.");
        }

        await client.query('COMMIT');

        const tipAmountDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(tipAmountLamports, 'USD', solPrice));
        const tipperNewBalanceDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(debitResult.newBalanceLamports, 'USD', solPrice));
        const recipientNewBalanceDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(creditResult.newBalanceLamports, 'USD', solPrice));

        await safeSendMessage(chatId, `✅ Success! You tipped *${tipAmountDisplayUSD}* to ${recipientName}. Your new balance is approx. *${tipperNewBalanceDisplayUSD}*.`, { parse_mode: 'MarkdownV2' });

        let recipientNotification = `🎁 You've received a tip of *${tipAmountDisplayUSD}* from ${tipperName}!`;
        if (tipMessage) {
            recipientNotification += `\nMessage: "_${escapeMarkdownV2(tipMessage)}_"`;
        }
        recipientNotification += `\nYour new balance is approx. *${recipientNewBalanceDisplayUSD}*.`;
        await safeSendMessage(recipientId, recipientNotification, { parse_mode: 'MarkdownV2' });

    } catch (error) {
        if (client) {
            await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error: ${rbErr.message}`));
        }
        console.error(`${logPrefix} Error processing tip: ${error.message}`, error.stack?.substring(0, 700));
        await safeSendMessage(chatId, `⚙️ An error occurred while processing your tip: \`${escapeMarkdownV2(error.message)}\`. Please try again.`, { parse_mode: 'MarkdownV2' });

        if (error.message.includes("Failed to credit recipient")) {
             if(typeof notifyAdmin === 'function' && ADMIN_USER_ID) { 
                notifyAdmin(
                    `🚨 CRITICAL TIP FAILURE 🚨\nTipper: ${tipperId} (${tipperUserObj.username || 'N/A'})\nRecipient: ${recipientId} (${recipientUserObj.username || 'N/A'})\nAmount: ${tipAmountLamports} lamports.\nTipper was likely debited but recipient NOT credited. MANUAL VERIFICATION & CORRECTION REQUIRED.\nError: ${escapeMarkdownV2(error.message)}`,
                    {parse_mode: 'MarkdownV2'}
                ).catch(err => console.error("Failed to notify admin about critical tip failure:", err));
             }
        }
    } finally {
        if (client) {
            client.release();
        }
    }
}

async function handleRulesCommand(chatId, userObj, messageIdToEdit = null, isEdit = false, chatType = 'private') {
    const LOG_PREFIX_RULES = `[RulesCmd UID:${userObj.telegram_id} Chat:${chatId}]`;
    const userMention = getPlayerDisplayReference(userObj);
    let botUsername = BOT_NAME || "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { /* console.error(`${LOG_PREFIX_RULES} Could not fetch bot username:`, e.message); */ }

    let targetChatId = String(userObj.telegram_id);
    let targetMessageId = null;
    let targetIsEdit = false;

    if (chatType !== 'private') {
        if (messageIdToEdit && chatId !== String(userObj.telegram_id)) await bot.deleteMessage(chatId, messageIdToEdit).catch(() => {});
        await safeSendMessage(chatId, `${userMention}, I've summoned the Gamepedia to our private chat: @${escapeMarkdownV2(botUsername)} 📖 Dive in!`, { parse_mode: 'MarkdownV2' });
    } else {
        targetChatId = chatId;
        targetMessageId = messageIdToEdit;
        targetIsEdit = isEdit;
        if (!isEdit && messageIdToEdit) { 
            await bot.deleteMessage(chatId, messageIdToEdit).catch(() => {});
            targetMessageId = null; 
        }
    }

    const rulesIntroText = `📚 **${escapeMarkdownV2(BOT_NAME)} Gamepedia Central** 📚\n\nHey ${userMention}, welcome to our casino's hall of knowledge! Select any game below to learn its rules, strategies, and payout secrets. Master them all! 👇`; // Removed backslash from period

    const gameRuleButtons = Object.values(GAME_IDS)
        .filter(gameCode =>
            ![
                GAME_IDS.DICE_21_PVP, 
                GAME_IDS.DUEL_PVB,    
                GAME_IDS.DUEL_PVP,    
                GAME_IDS.DICE_ESCALATOR_PVB, 
                GAME_IDS.DICE_ESCALATOR_PVP, 
                GAME_IDS.MINES_OFFER 
            ].includes(gameCode)
        )
        .map(gameCode => {
            let gameName = gameCode.replace(/_/g, ' ').replace(' Unified Offer', '').replace(/\b\w/g, l => l.toUpperCase());
            let ruleCallbackKey = gameCode; 

            if (gameCode === GAME_IDS.DICE_21_UNIFIED_OFFER) { gameName = "Dice 21 (Blackjack)"; ruleCallbackKey = GAME_IDS.DICE_21_UNIFIED_OFFER; }
            if (gameCode === GAME_IDS.DUEL_UNIFIED_OFFER) { gameName = "Duel / Highroller"; ruleCallbackKey = GAME_IDS.DUEL_UNIFIED_OFFER; } 
            if (gameCode === GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER) { gameName = "Dice Escalator"; ruleCallbackKey = GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER; }
            if (gameCode === GAME_IDS.MINES) { gameName = "Mines"; ruleCallbackKey = GAME_IDS.MINES; }


            let emoji = '❓';
            switch (ruleCallbackKey) { 
                case GAME_IDS.COINFLIP: emoji = '🪙'; break;
                case GAME_IDS.RPS: emoji = '✂️'; break;
                case GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER: emoji = '🎲'; break; 
                case GAME_IDS.DICE_21_UNIFIED_OFFER: emoji = '🃏'; break; 
                case GAME_IDS.DUEL_UNIFIED_OFFER: emoji = '⚔️'; break; 
                case GAME_IDS.OVER_UNDER_7: emoji = '🎲'; break;
                case GAME_IDS.LADDER: emoji = '🪜'; break;
                case GAME_IDS.SEVEN_OUT: emoji = '🎲'; break;
                case GAME_IDS.SLOT_FRENZY: emoji = '🎰'; break;
                case GAME_IDS.MINES: emoji = '💣'; break; 
            }
            return { text: `${emoji} ${escapeMarkdownV2(gameName)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${ruleCallbackKey}` };
        }).filter((button, index, self) => 
            index === self.findIndex((b) => b.text === button.text)
        );


    const rows = [];
    for (let i = 0; i < gameRuleButtons.length; i += 2) {
        rows.push(gameRuleButtons.slice(i, i + 2));
    }
    rows.push([{ text: '🏛️ Back to Main Help', callback_data: 'menu:main' }]);
    rows.push([{ text: '💳 Wallet Dashboard', callback_data: 'menu:wallet' }]);

    const keyboard = { inline_keyboard: rows };
    const options = { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true };

    if (targetIsEdit && targetMessageId) {
        try {
            await bot.editMessageText(rulesIntroText, { chat_id: targetChatId, message_id: Number(targetMessageId), ...options });
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES} Failed to edit rules menu (ID: ${targetMessageId}), sending new. Error: ${e.message}`);
                await safeSendMessage(targetChatId, rulesIntroText, options);
            }
        }
    } else {
        await safeSendMessage(targetChatId, rulesIntroText, options);
    }
}

async function handleDisplayGameRules(chatId, originalMessageId, gameCode, userObj, chatType = 'private') {
    const LOG_PREFIX_RULES_DISP = `[RulesDisplay UID:${userObj.telegram_id} Game:${gameCode}]`;
    const playerRef = getPlayerDisplayReference(userObj);
    let botUsername = BOT_NAME || "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { /* Reduced log */ }

    let targetChatId = String(userObj.telegram_id);
    let messageToEditInDm = null;

    if (chatType !== 'private' && originalMessageId && chatId !== targetChatId) {
        const gameNameDisplayUpper = gameCode.replace(/_/g, ' ').replace(' Unified Offer', '').replace(/\b\w/g, l => l.toUpperCase());
        const redirectText = `${playerRef}, I've sent the detailed rules for *${escapeMarkdownV2(gameNameDisplayUpper)}* to our private chat: @${escapeMarkdownV2(botUsername)} 📖 Check your DMs!`;
        await bot.editMessageText(redirectText, {
            chat_id: chatId, message_id: Number(originalMessageId), parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=showRules_${gameCode}` }]] }
        }).catch(e => {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES_DISP} Failed to edit group msg for rule redirect: ${e.message}.`);
            }
        });
    } else if (chatType === 'private' && originalMessageId) {
        messageToEditInDm = Number(originalMessageId);
    }

    let rulesTitle = gameCode.replace(/_/g, ' ').replace(' Unified Offer', '').replace(/\b\w/g, l => l.toUpperCase());
    if (gameCode === GAME_IDS.DICE_21_UNIFIED_OFFER) rulesTitle = "Dice 21 (Blackjack)"; 

    let gameEmoji = '📜';
    let rulesText = "";

    let solPrice = 100; try { solPrice = await getSolUsdPrice(); } catch (priceErr) { /* Reduced log */ }
    const minBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(convertUSDToLamports(MIN_BET_USD_val, solPrice), solPrice));
    const maxBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(convertUSDToLamports(MAX_BET_USD_val, solPrice), solPrice));
    const defaultBetDisplay = minBetDisplay;
    const generalBettingInfo = `*💰 General Betting Info:*\n` +
        `▫️ Place bets in USD (e.g., \`5\`, \`10.50\`) or SOL (e.g., \`0.1 sol\`, \`0.05\`).\n`+ 
        `▫️ Current Limits (USD Equiv.): *${minBetDisplay}* to *${maxBetDisplay}*.\n` + 
        `▫️ No bet specified? Defaults to *${defaultBetDisplay}* USD approx.\n\n`; 

    // Game-specific rules
    switch (gameCode) {
        case GAME_IDS.COINFLIP: gameEmoji = '🪙'; rulesTitle = "Coinflip Challenge"; break; 
        case GAME_IDS.RPS: gameEmoji = '✂️'; rulesTitle = "Rock Paper Scissors Showdown"; break; 
        case GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER:
            gameEmoji = '🎲'; rulesTitle = "Dice Escalator";
            rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
            rulesText += `Hey ${playerRef}! Ready to master *${escapeMarkdownV2(rulesTitle)}*? This is a strategic dice scoring game available in two modes after an initial offer in a group chat:\n\n`;
            rulesText += generalBettingInfo;
            rulesText += `*🎯 Objective:*\n` +
                         ` ▫️ *Player vs. Bot (PvB):* Achieve a higher score than the Bot Dealer without busting. Win the jackpot by achieving a score of *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}* or higher and beating the Bot!\n` + 
                         ` ▫️ *Player vs. Player (PvP):* Achieve a higher score than your opponent without busting. The player with the highest score wins the pot.\n\n` + 
                         `*🎮 How to Play (General):*\n` +
                         ` 1. Start with \`/de <bet>\` in a group to make an offer. You can then choose to play vs. the Bot, or another player can accept your challenge for PvP.\n` + 
                         ` 2. **Player's Turn (PvB & PvP):** When it's your turn, you will be prompted to roll dice by sending the 🎲 emoji to the chat. The bot will read the value of your dice roll. You can typically roll multiple times to accumulate your score.\n` + 
                         ` 3. **UI Updates:** The main game message will update to show your current score and hand, and provide a "Stand" button if applicable.\n` + 
                         ` 4. **Busting (Player):** Rolling a *${escapeMarkdownV2(String(DICE_ESCALATOR_BUST_ON))}* means that die scores 0 for that roll. It doesn't necessarily end your turn immediately unless specific game conditions are met.\n`+ 
                         ` 5. **Standing:** When you are satisfied with your score, you can press the "Stand" button. This locks in your score for the round.\n\n` + 
                         `*🤖 Player vs. Bot (PvB) Specifics:*\n` +
                         ` ▫️ After you stand, the Bot Dealer (via the Helper Bot) will roll exactly **three dice**. The sum of these three dice is the Bot's score.\n` + 
                         ` ▫️ *Winning PvB:* You win if your score is higher than the Bot's score AND you did not bust (by game rules). If you bust, the Bot wins. Ties are handled by game logic.\n`+ 
                         ` ▫️ *PvB Jackpot:* If you win against the Bot AND your score is *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}* or higher, you also win the current Super Jackpot! A portion of each PvB bet contributes (\`${escapeMarkdownV2(String(JACKPOT_CONTRIBUTION_PERCENT * 100))}%\`)!\n` + 
                        ` ▫️ *Jackpot Run (PvB):* If your score reaches 18 or more, you can choose to "Go for Jackpot". If you do, you can no longer "Stand" and must continue rolling until you hit the Jackpot Target or Bust (roll a ${escapeMarkdownV2(String(DICE_ESCALATOR_BUST_ON))}).\n\n` +
                         `*⚔️ Player vs. Player (PvP) Specifics:*\n` +
                         ` ▫️ Player 1 (Initiator) rolls first, accumulating a score and then stands or busts.\n` + 
                         ` ▫️ Then, Player 2 rolls, trying to beat Player 1's score.\n` + 
                         ` ▫️ The player with the higher score (and not busted) wins. Specific win conditions are handled by game logic.\n` + 
                         ` ▫️ PvP games do not contribute to or win the Super Jackpot.\n\n` + 
                         `*🗑️ Deleting Dice Emojis:*\n` +
                         ` ▫️ Your dice emojis sent to the chat will be automatically deleted by the bot after their value is read.`; 
            break; 
        case GAME_IDS.DICE_21_UNIFIED_OFFER: 
            gameEmoji = '🃏'; rulesTitle = "Dice 21 (Casino Blackjack)"; 
            rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
            rulesText += `Hey ${playerRef}! Ready to master *${escapeMarkdownV2(rulesTitle)}*? Here’s the lowdown:\n\n`;
            rulesText += generalBettingInfo;
            rulesText += `*🎯 Objective:* Get your dice sum closer to *${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}* than your opponent (Bot or another Player), without busting (> ${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}).\n` + 
                         `*🎮 How to Play (General):*\n` +
                         ` ▫️ Use \`/d21 <bet>\` in a group chat to create an offer. You can then choose to play vs. the Bot or wait for a PvP challenger.\n` + 
                         ` ▫️ Players (and Bot in PvB) receive two initial dice via the Helper Bot (player rolls by sending 🎲 emoji when prompted).\n` + 
                         ` ▫️ "Hit" (send 🎲 emoji) for more dice, or "Stand" to keep your score.\n` + 
                         `*🤖 Bot Dealer (PvB):* Stands on *${escapeMarkdownV2(String(DICE_21_BOT_STAND_SCORE))}* or more.\n`+ 
                         `*🏆 Payouts:* Win: 2x bet. Blackjack (target on first 2 dice): 2.5x bet. Push (tie): Bet returned.`; 
            break;
        case GAME_IDS.DUEL_UNIFIED_OFFER:
            gameEmoji = '⚔️'; rulesTitle = "Duel / Highroller"; 
            rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
            rulesText += `Hey ${playerRef}! Ready to master *${escapeMarkdownV2(rulesTitle)}*? Here’s the lowdown:\n\n`;
            rulesText += generalBettingInfo;
            rulesText += `*🎯 Objective:* Achieve a higher sum with two dice rolls than your opponent (another Player or the Bot Dealer).\n` + 
                         `*🎮 How to Play:*\n` +
                         ` ▫️ Start by typing \`/duel <bet>\` in a **group chat**. This creates an offer.\n` + 
                         ` ▫️ From the offer, you (the initiator) can choose to play against the Bot Dealer (PvB), or another player can accept your challenge for a Player vs. Player (PvP) match.\n` + 
                         ` ▫️ **Player's Turn:** When instructed, you must send two separate 🎲 dice emojis to the chat. The Helper Bot will determine the value for each emoji roll.\n` + 
                         ` ▫️ **Bot Dealer's Turn (PvB):** After you've rolled twice, the Bot Dealer will also have two dice rolled for it by the Helper Bot.\n` + 
                         ` ▫️ **PvP Turns:** The first player rolls two dice (via two emojis). Their total is announced. Then, the second player rolls their two dice (via two emojis).\n` + 
                         `*🏆 Winning:* The player (or Bot) with the highest sum from their two dice wins the round and takes the pot (2x the bet). If scores are tied, it's a Push, and bets are returned.\n` + 
                         `*🎲 Dice:*\n All dice values are provided by the Helper Bot to ensure fairness and transparency. Each player (and the bot in PvB) rolls exactly two dice per game.`; 
            break;
        case GAME_IDS.OVER_UNDER_7:
            gameEmoji = '🎲'; rulesTitle = "Over Under 7 Thrills"; 
            rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
            rulesText += `Hey ${playerRef}! Ready to master *${escapeMarkdownV2(rulesTitle)}*? Here’s the lowdown:\n\n`;
            rulesText += generalBettingInfo;
            rulesText += `*🎯 Objective:* Predict if *${escapeMarkdownV2(String(OU7_DICE_COUNT))} dice* sum (rolled by Helper Bot) is Over 7, Under 7, or Exactly 7.\n` + 
                         `*🎮 How to Play:* Use \`/ou7 <bet>\`. Choose your prediction via buttons.\n` + 
                         `*🏆 Payouts:* Under 7 (2-6) or Over 7 (8-12): *${escapeMarkdownV2(String(OU7_PAYOUT_NORMAL + 1))}x* bet. Exactly 7: *${escapeMarkdownV2(String(OU7_PAYOUT_SEVEN + 1))}x* bet! (Payouts include stake back)`; 
            break;
        case GAME_IDS.LADDER:
            gameEmoji = '🪜'; rulesTitle = "Greed's Ladder Challenge"; 
            rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
            rulesText += `Hey ${playerRef}! Ready to master *${escapeMarkdownV2(rulesTitle)}*? Here’s the lowdown:\n\n`;
            rulesText += generalBettingInfo;
            rulesText += `*🎯 Objective:* Get a high sum with *${escapeMarkdownV2(String(LADDER_ROLL_COUNT))} dice* (rolled by Helper Bot). Rolling a *${escapeMarkdownV2(String(LADDER_BUST_ON))}* on ANY die means you bust!\n` + 
                         `*🎮 How to Play:* Use \`/ladder <bet>\`. All dice rolled at once by the Helper Bot.\n` + 
                         `*🏆 Payouts (Based on Sum, No Bust - Payouts include stake back):*\n`;
            LADDER_PAYOUTS.forEach(p => { rulesText += `   ▫️ Sum *${escapeMarkdownV2(String(p.min))}-${escapeMarkdownV2(String(p.max))}*: *${escapeMarkdownV2(String(p.multiplier + 1))}x* bet (${escapeMarkdownV2(p.label)})\n`; }); 
            break;
        case GAME_IDS.SEVEN_OUT:
            gameEmoji = '🎲'; rulesTitle = "Sevens Out (Fast Craps)"; 
            rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
            rulesText += `Hey ${playerRef}! Ready to master *${escapeMarkdownV2(rulesTitle)}*? Here’s the lowdown:\n\n`;
            rulesText += generalBettingInfo;
            rulesText += `*🎯 Objective:* Simplified Craps. Win on Come Out (7/11), or roll Point before a 7. Lose on Come Out (2/3/12) or rolling 7 before Point. Uses 2 dice (rolled via animated dice/Helper Bot). \n` + 
                         `*🎲 Come Out Roll:* Auto-rolled after \`/s7 <bet>\`. Win on 7/11 (2x bet). Lose on 2/3/12. Other sums (4,5,6,8,9,10) become your "Point".\n` + 
                         `*🎲 Point Phase:* Click "Roll for Point". Win if you roll Point (2x bet). Lose if you roll 7 ("Seven Out").`; 
            break;
        case GAME_IDS.SLOT_FRENZY:
            gameEmoji = '🎰'; rulesTitle = "Slot Fruit Frenzy Spins"; 
            rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
            rulesText += `Hey ${playerRef}! Ready to master *${escapeMarkdownV2(rulesTitle)}*? Here’s the lowdown:\n\n`;
            rulesText += generalBettingInfo;
            rulesText += `*🎯 Objective:* Match symbols on Telegram's animated slot machine (value 1-64, provided by Helper Bot).\n` + 
                         `*🎮 How to Play:* Use \`/slot <bet>\`. Helper Bot determines the slot outcome.\n` + 
                         `*🏆 Payouts (based on dice value from slot animation - Payouts include stake back):\n`;
            for (const key in SLOT_PAYOUTS) { if (SLOT_PAYOUTS[key].multiplier >= 1) { rulesText += `   ▫️ ${SLOT_PAYOUTS[key].symbols} (${escapeMarkdownV2(SLOT_PAYOUTS[key].label)}): *${escapeMarkdownV2(String(SLOT_PAYOUTS[key].multiplier + 1))}x* bet (Value: ${key})\n`;}} 
            rulesText += `   ▫️ Other rolls may result in a loss.`; 
            break;
        case GAME_IDS.MINES: 
            gameEmoji = '💣'; rulesTitle = "Mines Field";
            rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
            rulesText += `Hey ${playerRef}! Navigate the treacherous *${escapeMarkdownV2(rulesTitle)}* and uncover riches!\n\n`; 
            rulesText += generalBettingInfo;
            rulesText += `*🎯 Objective:*\n` +
                         `Reveal safe tiles (gems 💎) while avoiding hidden mines 💣. The more gems you find before hitting a mine or cashing out, the higher your payout multiplier!\n\n` + 
                         `*🎮 How to Play:*\n` +
                         ` 1. Start a game with \`/mines <bet_amount>\`.\n` + 
                         ` 2. You will then be prompted to select a difficulty (e.g., Easy, Medium, Hard), which determines grid size and number of mines.\n` + 
                         ` 3. Click on the grid buttons to reveal tiles.\n` + 
                         ` 4. If you reveal a Mine 💣, the game ends, and you lose your bet.\n` + 
                         ` 5. If you reveal a Gem 💎, your potential winnings increase!\n` + 
                         ` 6. You can choose to **"Cash Out"** your current winnings at any point after finding at least one gem.\n\n` + 
                         `*💰 Payouts:*\n` +
                         ` ▫️ Payouts increase with each gem found. The specific multiplier depends on the chosen difficulty (grid size, number of mines), and gems uncovered.\n` + 
                         ` ▫️ The "Cash Out" button will display your current potential winnings.\n\n` + 
                         `*⚠️ Warning:*\n` +
                         ` ▫️ The more gems you try to find, the higher the risk of hitting a mine, but also the greater the potential reward! Play strategically!`; 
            break;
        default:
            if (!rulesText) { 
                rulesText = `${gameEmoji} *Welcome to the Rules of ${escapeMarkdownV2(rulesTitle)}* ${gameEmoji}\n\n`;
                rulesText += `Hey ${playerRef}! Ready to master *${escapeMarkdownV2(rulesTitle)}*? Here’s the lowdown:\n\n`;
                rulesText += generalBettingInfo;
                rulesText += `📜 Rules for *"${escapeMarkdownV2(rulesTitle)}"* are currently being polished by our game masters. Check back soon!`; 
            }
    }
    rulesText += `\n\nPlay smart, play responsibly, and may the odds be ever in your favor! 🍀`; 

    const keyboard = { inline_keyboard: [[{ text: "📚 Back to Games List", callback_data: "show_rules_menu" }]] };

    if (messageToEditInDm) {
        try {
            await bot.editMessageText(rulesText, { chat_id: targetChatId, message_id: Number(messageToEditInDm), parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES_DISP} Failed to edit rules display in DM for ${gameCode}, sending new. Error: ${e.message}`);
                await safeSendMessage(targetChatId, rulesText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
            }
        }
    } else {
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
        const jackpotSOL = formatCurrency(jackpotAmountLamports, 'SOL');
        const jackpotTargetScoreDisplay = escapeMarkdownV2(String(TARGET_JACKPOT_SCORE)); 

        const jackpotMessage = `🏆 **Dice Escalator (PvB) Super Jackpot Alert!** 🏆\n\n` +
            `Hey ${playerRef}, the current Super Jackpot for the Player vs Bot Dice Escalator game is a shimmering mountain of riches:\n\n` +
            `💰 Approx. Value: *${escapeMarkdownV2(jackpotUSD)}*\n` + 
            `🪙 SOL Amount: *${escapeMarkdownV2(jackpotSOL)}*\n\n` +
            `To claim this colossal prize, you must win a round of Dice Escalator (PvB Mode) with a score of *${jackpotTargetScoreDisplay} or higher* AND beat the Bot Dealer! Do you have what it takes? ✨\n\nType \`/de <bet>\` to try your luck in a group chat!`; 

        await safeSendMessage(chatId, jackpotMessage, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });

    } catch (error) {
        console.error(`${LOG_PREFIX_JACKPOT} Error fetching jackpot: ${error.message}`);
        await safeSendMessage(chatId, "⚙️ Apologies, there was a momentary glitch fetching the current Jackpot amount. Please try \`/jackpot\` again soon.", { parse_mode: 'MarkdownV2' }); 
    }
}

async function handleLeaderboardsCommand(msg, args) {
    const userId = String(msg.from.id || msg.from.telegram_id); 
    const chatId = String(msg.chat.id);
    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) {
         await safeSendMessage(chatId, "Error fetching your profile. Please try \`/start\`.", {}); 
         return;
    }
    const playerRef = getPlayerDisplayReference(user);
    const typeArg = args[0] || 'overall_wagered';
    const typeDisplay = escapeMarkdownV2(typeArg.replace(/_/g, " ").replace(/\b\w/g, l => l.toUpperCase()));

    const leaderboardMessage = `🏆 **${escapeMarkdownV2(BOT_NAME)} Hall of Fame** 🏆 - _Coming Soon!_\n\n` + 
        `Greetings, ${playerRef}! Our legendary leaderboards for categories like *${typeDisplay}* are currently under meticulous construction by our top casino architects. 🏗️\n\n` + 
        `Soon, you'll be able to see who's dominating the casino floor, raking in the biggest wins, and making the boldest wagers!\n\n` + 
        `Keep playing, sharpen your skills, and prepare to etch your name in ${escapeMarkdownV2(BOT_NAME)} history! Check back soon for the grand unveiling! ✨`; 
    await safeSendMessage(chatId, leaderboardMessage, { parse_mode: 'MarkdownV2' });
}

async function handleGrantCommand(msg, args, adminUserObj) {
    const LOG_PREFIX_GRANT = `[GrantCmd UID:${adminUserObj.telegram_id}]`;
    const chatId = String(msg.chat.id);
    const adminUserIdStr = String(adminUserObj.telegram_id);

    if (!ADMIN_USER_ID || adminUserIdStr !== ADMIN_USER_ID) {
        return; 
    }

    if (args.length < 2) {
        await safeSendMessage(chatId, "⚙️ **Admin Grant Usage:** `/grant <target_user_id_or_@username> <amount_SOL_or_Lamports> [Optional: reason]`\n*Examples:*\n`/grant @LuckyPlayer 10 SOL Welcome Bonus`\n`/grant 123456789 50000000 lamports Correction`\n`/grant @RiskTaker -2 SOL BetSettleFix`", { parse_mode: 'MarkdownV2' }); 
        return;
    }

    const targetUserIdentifier = args[0];
    const amountArg = args[1];
    const reason = args.slice(2).join(' ') || `Admin grant by ${adminUserObj.username || adminUserIdStr}`;
    let amountToGrantLamports;
    let targetUser;

    try {
        if (targetUserIdentifier.startsWith('@')) {
            const usernameToFind = targetUserIdentifier.substring(1);
            const userRes = await queryDatabase('SELECT telegram_id, username, first_name FROM users WHERE LOWER(username) = LOWER($1)', [usernameToFind]);
            if (userRes.rowCount === 0) throw new Error(`User not found: \`${escapeMarkdownV2(targetUserIdentifier)}\`.`);
            targetUser = await getOrCreateUser(userRes.rows[0].telegram_id, userRes.rows[0].username, userRes.rows[0].first_name);
        } else if (/^\d+$/.test(targetUserIdentifier)) {
            targetUser = await getOrCreateUser(targetUserIdentifier);
        } else {
            throw new Error(`Invalid target: \`${escapeMarkdownV2(targetUserIdentifier)}\`. Use Telegram ID or @username.`);
        }
        if (!targetUser) throw new Error(`Could not find or create target user \`${escapeMarkdownV2(targetUserIdentifier)}\`.`);
        
        const amountArgLower = String(amountArg).toLowerCase();
        let parsedAmount;
        let isNegative = String(amountArg).startsWith('-');

        if (amountArgLower.endsWith('sol')) {
            parsedAmount = parseFloat(amountArgLower.replace('sol', '').trim());
            if (isNaN(parsedAmount)) throw new Error("Invalid SOL amount.");
            amountToGrantLamports = BigInt(Math.floor(parsedAmount * Number(LAMPORTS_PER_SOL)));
        } else if (amountArgLower.endsWith('lamports')) {
            parsedAmount = amountArgLower.replace('lamports','').trim();
            amountToGrantLamports = BigInt(parsedAmount);
        } else if (String(amountArg).includes('.')) { 
            parsedAmount = parseFloat(amountArg);
            if (isNaN(parsedAmount)) throw new Error("Invalid SOL amount (decimal).");
            amountToGrantLamports = BigInt(Math.floor(parsedAmount * Number(LAMPORTS_PER_SOL)));
        } else { 
            parsedAmount = BigInt(amountArg);
            if (parsedAmount !== 0n && (Math.abs(Number(parsedAmount)) < 100000 || isNegative && Math.abs(Number(parsedAmount)) < 100000)) { 
                amountToGrantLamports = BigInt(Math.floor(Number(parsedAmount) * Number(LAMPORTS_PER_SOL)));
            } else { 
                amountToGrantLamports = parsedAmount;
            }
        }
        if (isNaN(Number(amountToGrantLamports))) throw new Error("Could not parse grant amount.");
        if (amountToGrantLamports === 0n && String(amountArg) !== "0") throw new Error("Grant amount resolved to zero incorrectly. Be explicit: `0 sol` or `0 lamports`.");

    } catch (e) {
        await safeSendMessage(chatId, `⚠️ **Grant Parameter Error:**\n${escapeMarkdownV2(e.message)}`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let grantClient = null;
    try {
        grantClient = await pool.connect();
        await grantClient.query('BEGIN');

        if (typeof updateUserBalanceAndLedger !== 'function') {
            console.error(`${LOG_PREFIX_GRANT} FATAL: updateUserBalanceAndLedger is undefined for grant.`);
            await safeSendMessage(chatId, "🛠️ **Internal System Error:** Grant functionality is offline. Core balance function missing.", { parse_mode: 'MarkdownV2' }); 
            await grantClient.query('ROLLBACK'); return;
        }
        const transactionType = amountToGrantLamports >= 0n ? 'admin_grant_credit' : 'admin_grant_debit';
        const grantNotes = `Admin Action: ${reason}. By: ${adminUserObj.username || adminUserIdStr} (${adminUserIdStr}). To: ${targetUser.username || targetUser.telegram_id} (${targetUser.telegram_id}). Amount: ${formatCurrency(amountToGrantLamports, 'SOL')}`;
        
        const grantResult = await updateUserBalanceAndLedger(
            grantClient, targetUser.telegram_id, amountToGrantLamports, transactionType, {}, grantNotes
        );

        if (grantResult.success) {
            await grantClient.query('COMMIT');
            const grantAmountDisplay = escapeMarkdownV2(formatCurrency(amountToGrantLamports, 'SOL'));
            const newBalanceDisplay = escapeMarkdownV2(await formatBalanceForDisplay(grantResult.newBalanceLamports, 'USD'));
            const targetUserDisplay = getPlayerDisplayReference(targetUser);
            const verb = amountToGrantLamports >= 0n ? "credited to" : "debited from";

            await safeSendMessage(chatId, `✅ **Admin Action Successful!**\n*${grantAmountDisplay}* has been ${verb} ${targetUserDisplay} (ID: \`${targetUser.telegram_id}\`).\nNew balance for user: *${newBalanceDisplay}*.`, { parse_mode: 'MarkdownV2' }); 
            
            const userNotifText = amountToGrantLamports >= 0n
                ? `🎉 Good news! You have received an admin credit of *${grantAmountDisplay}* from the Casino Royale team! Your new balance is *${newBalanceDisplay}*. Reason: _${escapeMarkdownV2(reason)}_`
                : `⚖️ Admin Adjustment: Your account has been debited by *${grantAmountDisplay}* by the Casino Royale team. Your new balance is *${newBalanceDisplay}*. Reason: _${escapeMarkdownV2(reason)}_`;
            await safeSendMessage(targetUser.telegram_id, userNotifText, { parse_mode: 'MarkdownV2' });
        } else {
            await grantClient.query('ROLLBACK');
            await safeSendMessage(chatId, `❌ **Admin Action Failed:** Failed to ${amountToGrantLamports > 0n ? 'credit' : 'debit'} funds. Reason: \`${escapeMarkdownV2(grantResult.error || "Unknown balance update error.")}\``, { parse_mode: 'MarkdownV2' }); 
        }
    } catch (grantError) {
        if (grantClient) await grantClient.query('ROLLBACK').catch(() => {});
        console.error(`${LOG_PREFIX_GRANT} Admin Grant DB Transaction Error: ${grantError.message}`, grantError.stack?.substring(0,500));
        await safeSendMessage(chatId, `❌ **Database Error During Grant:** \`${escapeMarkdownV2(grantError.message)}\`. The action was not completed.`, { parse_mode: 'MarkdownV2' }); 
    } finally {
        if (grantClient) grantClient.release();
    }
}

// console.log("Part 5a, Section 2 (REVISED for New Dice Escalator Rules) - Complete.");
// --- End of Part 5a, Section 2 ---
// --- Start of Part 5a, Section 4 (REVISED for New Dice Escalator UI & Simplified Post-Game Keyboard) ---
// --- Start of Part 5a, Section 4 (REVISED for New Dice Escalator UI & Simplified Post-Game Keyboard) ---
// index.js - Part 5a, Section 4: UI Helpers and Shared Utilities for General Commands & Simple Group Games
//----------------------------------------------------------------------------------------------------
// Assumed dependencies from previous Parts:
// Part 1: GAME_IDS (with new DE IDs), QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, escapeMarkdownV2
// Part 3: formatCurrency

// Note: `parseBetAmount` is a critical shared utility but is defined in Part 5a, Section 1.
// (Self-correction: parseBetAmount was actually in Original Part 5a, Section 1, which is being moved later.
// However, it's primarily used by the message command router which is also in Original Part 5a, Section 1.
// If any functions *before* Original Part 5a, Section 1 (in the new order) needed parseBetAmount, that would be an issue.
// A quick scan shows game start handlers receive betAmountLamports directly. parseBetAmount is used when commands like `/de <bet>` are parsed.
// So its placement within the later "Original Part 5a, Section 1" should be fine.)

/**
 * Creates a standardized inline keyboard for post-game actions with "Repeat Bet" and "Wallet" options.
 * @param {string} gameCode - The game identifier (e.g., GAME_IDS.COINFLIP, GAME_IDS.DICE_ESCALATOR_PVB).
 * @param {bigint} betAmountLamports - The bet amount for the "Repeat Bet" button.
 * @returns {object} Telegram InlineKeyboardMarkup object.
 */
function createPostGameKeyboard(gameCode, betAmountLamports) {
    // Assuming formatCurrency and escapeMarkdownV2 are available and correctly defined
    const playAgainBetDisplaySOL = escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL'));

    let playAgainCallbackActionPrefix = gameCode.toLowerCase(); // Default action prefix based on gameCode

    // Adjustments for specific games or game families if their "play_again" callback data
    // needs a different prefix than the direct gameCode.
    // This logic should align with how play_again callbacks are handled in Part 5a, Section 1.
    switch (gameCode) {
        case GAME_IDS.COINFLIP:
            playAgainCallbackActionPrefix = 'coinflip'; // Results in 'play_again_coinflip:bet'
            break;
        case GAME_IDS.RPS:
            playAgainCallbackActionPrefix = 'rps'; // Results in 'play_again_rps:bet'
            break;
        case GAME_IDS.DICE_ESCALATOR_PVB:
            playAgainCallbackActionPrefix = 'de_pvb'; // Results in 'play_again_de_pvb:bet'
            break;
        case GAME_IDS.DICE_ESCALATOR_PVP:
            playAgainCallbackActionPrefix = 'de_pvp'; // Results in 'play_again_de_pvp:bet'
            break;
        case GAME_IDS.DICE_21: // PvB Dice 21
            playAgainCallbackActionPrefix = 'd21'; // Results in 'play_again_d21:bet'
            break;
        case GAME_IDS.DICE_21_PVP:
            playAgainCallbackActionPrefix = 'd21_pvp'; // Results in 'play_again_d21_pvp:bet'
            break;
        case GAME_IDS.DUEL_PVB: // Both Duel PvB and PvP might restart the unified offer
        case GAME_IDS.DUEL_PVP:
        case GAME_IDS.DUEL_UNIFIED_OFFER: // If resolving to this specific code
            playAgainCallbackActionPrefix = 'duel'; // Results in 'play_again_duel:bet'
            break;
        case GAME_IDS.OVER_UNDER_7:
            playAgainCallbackActionPrefix = 'ou7'; // Results in 'play_again_ou7:bet'
            break;
        case GAME_IDS.LADDER:
            playAgainCallbackActionPrefix = 'ladder'; // Results in 'play_again_ladder:bet'
            break;
        case GAME_IDS.SEVEN_OUT:
            playAgainCallbackActionPrefix = 's7'; // Results in 'play_again_s7:bet'
            break;
        case GAME_IDS.SLOT_FRENZY:
            playAgainCallbackActionPrefix = 'slot'; // Results in 'play_again_slot:bet'
            break;
        default:
            // For any other gameCode, use it directly.
            // Ensure that a 'play_again_[gameCode.toLowerCase()]' callback is handled.
            console.warn(`[CreatePostGameKB] Using default playAgainCallbackActionPrefix for unhandled gameCode '${gameCode}': '${playAgainCallbackActionPrefix}'`);
            break;
    }

    const playAgainCallbackData = `play_again_${playAgainCallbackActionPrefix}:${betAmountLamports.toString()}`;

    const keyboardRows = [
        [
            { text: `🔁 Repeat Bet (${playAgainBetDisplaySOL})`, callback_data: playAgainCallbackData },
            { text: "💳 Wallet", callback_data: "menu:wallet" }
        ]
    ];

    return { inline_keyboard: keyboardRows };
}

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

/**
 * Generates a standardized title string for game messages or UI sections.
 * @param {string} titleText The main text of the title.
 * @param {string} [emoji='✨'] Optional leading/trailing emoji.
 * @returns {string} MarkdownV2 formatted title string.
 */
function createStandardTitle(titleText, emoji = '✨') {
    return `${emoji} *${escapeMarkdownV2(titleText)}* ${emoji}`;
}

// console.log("Part 5a, Section 4 (REVISED for Simplified Post-Game Keyboard) - Complete.");
// --- End of Part 5a, Section 4 ---
// --- Start of Part 5a, Section 1 (REVISED for New Dice Escalator & Full Routing for Jackpot Choice) ---
// index.js - Part 5a, Section 1: Core Listeners Setup (Message & Callback) and Populated Routers
// (This entire block should be placed LATE in your index.js, AFTER all game logic, general commands, and UI helpers, but BEFORE Part 6)
//----------------------------------------------------------------------------------------------
// console.log("Loading Part 5a, Section 1 (REVISED for New Dice Escalator & Full Routing for Jackpot Choice)...");

// Dependencies from previous Parts (assumed to be globally available or correctly imported)
// Part 1: isShuttingDown, userStateCache, COMMAND_COOLDOWN_MS, bot, getPlayerDisplayReference,
//         safeSendMessage, escapeMarkdownV2, MIN_BET_USD_val, MAX_BET_USD_val, LAMPORTS_PER_SOL,
//         getSolUsdPrice, convertUSDToLamports, convertLamportsToUSDString, ADMIN_USER_ID, BOT_NAME,
//         MIN_BET_AMOUNT_LAMPORTS_config, MAX_BET_AMOUNT_LAMPORTS_config, stringifyWithBigInt,
//         RULES_CALLBACK_PREFIX, DEPOSIT_CALLBACK_ACTION, WITHDRAW_CALLBACK_ACTION, QUICK_DEPOSIT_CALLBACK_ACTION,
//         userCooldowns, pool, activeGames, groupGameSessions, GAME_IDS (with new DE IDs and MINES_OFFER)
// Part 2: getOrCreateUser, findRecipientUser
// Part 3: createUserMention, formatCurrency
// Part P3: clearUserState, routeStatefulInput, handleMenuAction, handleWithdrawalConfirmation
// Game Logic Parts (e.g., Part 5b-S1 for Dice Escalator): Game logic functions like handleDEGoForJackpot,
// processDiceEscalatorPvBRollByEmoji_New, handleStartDiceEscalatorUnifiedOfferCommand_New, startDiceEscalatorPvBGame_New, etc., are now defined *before* this section.
// Mines handlers like handleStartMinesCommand (from Part 5a, Section 2, which we just modified) are defined before this.


// --- Helper to parse bet amount for game commands (USD primary) ---
const parseBetAmount = async (arg, commandInitiationChatId, commandInitiationChatType, userIdForLog = 'N/A') => {
    const LOG_PREFIX_PBA = `[ParseBet CH:${commandInitiationChatId} UID:${userIdForLog}]`;
    let betAmountLamports;
    let minBetLamports, maxBetLamports;
    let minBetDisplay, maxBetDisplay;
    let defaultBetDisplay;

    try {
        const solPrice = await getSolUsdPrice();

        minBetLamports = convertUSDToLamports(MIN_BET_USD_val, solPrice);
        maxBetLamports = convertUSDToLamports(MAX_BET_USD_val, solPrice);

        minBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(minBetLamports, solPrice));
        maxBetDisplay = escapeMarkdownV2(convertLamportsToUSDString(maxBetLamports, solPrice));
        defaultBetDisplay = minBetDisplay;

        if (!arg || String(arg).trim() === "") {
            betAmountLamports = minBetLamports;
            return betAmountLamports;
        }

        const argStr = String(arg).trim().toLowerCase();
        let potentialUsdAmount = parseFloat(argStr.replace(/[^0-9.]/g, ''));

        if (!isNaN(potentialUsdAmount) && potentialUsdAmount > 0 && !argStr.endsWith('sol') && !argStr.endsWith('lamports')) {
            betAmountLamports = convertUSDToLamports(potentialUsdAmount, solPrice);
            const betUsdDisplay = escapeMarkdownV2(potentialUsdAmount.toFixed(2));

            if (potentialUsdAmount < MIN_BET_USD_val || potentialUsdAmount > MAX_BET_USD_val) {
                const message = `⚠️ Your bet of *${betUsdDisplay} USD* is outside the allowed limits: *${minBetDisplay}* - *${maxBetDisplay}*. Your bet has been adjusted to the minimum: *${defaultBetDisplay}*.`;
                await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                return minBetLamports;
            }
            return betAmountLamports;
        } else {
            let parsedLamportsDirectly;
            try {
                if (argStr.endsWith('sol')) {
                    const solVal = parseFloat(argStr.replace('sol', '').trim());
                    if (!isNaN(solVal) && solVal > 0) {
                        parsedLamportsDirectly = BigInt(Math.floor(solVal * Number(LAMPORTS_PER_SOL)));
                    } else throw new Error("Invalid SOL format with 'sol' suffix.");
                } else if (argStr.includes('.') && !isNaN(parseFloat(argStr))) {
                    const solVal = parseFloat(argStr);
                    if (!isNaN(solVal) && solVal > 0) {
                        parsedLamportsDirectly = BigInt(Math.floor(solVal * Number(LAMPORTS_PER_SOL)));
                    } else throw new Error("Invalid SOL float format.");
                } else if (argStr.endsWith('lamports')) {
                    const lampVal = BigInt(argStr.replace('lamports','').trim());
                    if (lampVal > 0n) parsedLamportsDirectly = lampVal;
                    else throw new Error("Invalid lamports amount.");
                }
                else {
                    const intVal = BigInt(argStr);
                    if (intVal > 0n && intVal < 10000n && !argStr.endsWith('000000')) { 
                        parsedLamportsDirectly = BigInt(Math.floor(Number(intVal) * Number(LAMPORTS_PER_SOL)));
                    } else if (intVal > 0n) { 
                        parsedLamportsDirectly = intVal;
                    } else {
                        throw new Error("Bet amount (SOL/Lamports) must be positive.");
                    }
                }

                if (parsedLamportsDirectly <= 0n) throw new Error("Bet amount (SOL/Lamports) must be positive.");

                const equivalentUsdValue = Number(parsedLamportsDirectly) / Number(LAMPORTS_PER_SOL) * solPrice;

                if (equivalentUsdValue < MIN_BET_USD_val || equivalentUsdValue > MAX_BET_USD_val) {
                    const betInSOLDisplayDynamic = escapeMarkdownV2(formatCurrency(parsedLamportsDirectly, 'SOL'));
                    const message = `⚠️ Your bet of *${betInSOLDisplayDynamic}* (approx. ${escapeMarkdownV2(convertLamportsToUSDString(parsedLamportsDirectly, solPrice))}) is outside current USD limits (*${minBetDisplay}* - *${maxBetDisplay}*). Your bet is set to the minimum: *${defaultBetDisplay}*.`;
                    await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                    return minBetLamports;
                }
                return parsedLamportsDirectly;
            } catch (directParseError) {
                const message = `🤔 Hmmm, your bet amount \`${escapeMarkdownV2(String(arg))}\` seems a bit off. Please use USD (e.g., \`5\` or \`10.50\`), or SOL (e.g. \`0.1 sol\`, \`0.05\`). Your bet is set to the minimum: *${defaultBetDisplay}*.`;
                await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                return minBetLamports;
            }
        }
    } catch (priceError) {
        console.error(`${LOG_PREFIX_PBA} CRITICAL error getting SOL price for bet parsing: ${priceError.message}`);
        const minLamportsFallbackDisplay = escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT_LAMPORTS_config, 'SOL'));
        const message = `⚙️ Apologies, we couldn't determine current bet limits due to a price feed issue. Using internal default lamport limits for now. Your bet has been set to the internal minimum of *${minLamportsFallbackDisplay}*.`;
        await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });

        try {
            if (!arg || String(arg).trim() === "") return MIN_BET_AMOUNT_LAMPORTS_config;
            let fallbackAmountLamports;
            const argStrFB = String(arg).trim().toLowerCase();
            if (argStrFB.endsWith('sol') || argStrFB.includes('.')) {
                const solValFB = parseFloat(argStrFB.replace('sol', '').replace('lamports','').trim());
                if (isNaN(solValFB) || solValFB <=0) return MIN_BET_AMOUNT_LAMPORTS_config;
                fallbackAmountLamports = BigInt(Math.floor(solValFB * Number(LAMPORTS_PER_SOL)));
            } else if (argStrFB.endsWith('lamports')) {
                const lampValFB = BigInt(argStrFB.replace('lamports','').trim());
                if (lampValFB <= 0n) return MIN_BET_AMOUNT_LAMPORTS_config;
                fallbackAmountLamports = lampValFB;
            } else {
                const intValFB = BigInt(argStrFB);
                if (intValFB <= 0n) return MIN_BET_AMOUNT_LAMPORTS_config;
                if (intValFB > 0n && intValFB < 10000n && !argStrFB.endsWith('000000')) {
                    fallbackAmountLamports = BigInt(Math.floor(Number(intValFB) * Number(LAMPORTS_PER_SOL)));
                } else {
                    fallbackAmountLamports = intValFB;
                }
            }

            if (fallbackAmountLamports < MIN_BET_AMOUNT_LAMPORTS_config || fallbackAmountLamports > MAX_BET_AMOUNT_LAMPORTS_config) {
                return MIN_BET_AMOUNT_LAMPORTS_config;
            }
            return fallbackAmountLamports;
        } catch {
            return MIN_BET_AMOUNT_LAMPORTS_config;
        }
    }
};


// --- Main Message Handler (`bot.on('message')`) ---
bot.on('message', async (msg) => {
    const LOG_PREFIX_MSG_HANDLER = `[MsgHandler TID:${msg.message_id || 'N/A'} OriginUID:${msg.from?.id || 'N/A'} ChatID:${msg.chat?.id || 'N/A'}]`;
    console.log(`${LOG_PREFIX_MSG_HANDLER} Received message: ${msg.text ? `Text: "${msg.text}"` : (msg.dice ? `Dice: Value ${msg.dice.value}` : "Non-text/dice message")}`);


    if (isShuttingDown) {
        console.log(`${LOG_PREFIX_MSG_HANDLER} Bot is shutting down. Ignoring message.`);
        return;
    }
    if (!msg || !msg.from || !msg.chat || !msg.date) {
        console.warn(`${LOG_PREFIX_MSG_HANDLER} Ignoring malformed/incomplete message.`);
        return;
    }

    if (msg.from.is_bot) {
        try {
            const selfBotInfo = await bot.getMe();
            if (String(msg.from.id) !== String(selfBotInfo.id)) {
                console.log(`${LOG_PREFIX_MSG_HANDLER} Ignoring message from other bot: ${msg.from.username || msg.from.id}`);
                return;
            }
            if (!msg.dice) { 
                console.log(`${LOG_PREFIX_MSG_HANDLER} Ignoring self-sent non-dice message.`);
                return;
            }
            console.log(`${LOG_PREFIX_MSG_HANDLER} Processing self-sent dice message (value: ${msg.dice.value}).`);
        } catch (getMeError) {
            console.error(`${LOG_PREFIX_MSG_HANDLER} Error in getMe self-check: ${getMeError.message}. Ignoring message.`);
            return;
        }
    }

    const userId = String(msg.from.id || msg.from.telegram_id); 
    const chatId = String(msg.chat.id);
    const text = msg.text || "";
    const chatType = msg.chat.type;

    if (msg.dice && msg.from && !msg.from.is_bot) { 
        const diceValue = msg.dice.value;
        const rollerId = String(msg.from.id || msg.from.telegram_id); 
        console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] User ${rollerId} sent 🎲 (value: ${diceValue}) in chat ${chatId}. ActiveGames size: ${activeGames.size}`);

        let gameIdForDiceRoll = null;
        let gameDataForDiceRoll = null;
        let isDiceEscalatorEmoji = false;
        let isDice21Emoji = false;
        let isDuelGameEmoji = false;

        let iterationCount = 0;
        for (const [gId, gData] of activeGames.entries()) {
            iterationCount++;
            if (String(gData.chatId) === chatId) { 
                const isDEPvBRollExpected = gData.status === 'player_turn_awaiting_emoji';
                if (gData.type === GAME_IDS.DICE_ESCALATOR_PVB && gData.player?.userId === rollerId && isDEPvBRollExpected) {
                    gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDiceEscalatorEmoji = true; break;
                }
                if (gData.type === GAME_IDS.DICE_ESCALATOR_PVP) {
                    const isInitiatorDE_PvP = (gData.initiator && gData.initiator.userId === rollerId && gData.initiator.isTurn && (gData.status === 'p1_awaiting_roll1_emoji' || gData.status === 'p1_awaiting_roll2_emoji'));
                    const isOpponentDE_PvP = (gData.opponent && gData.opponent.userId === rollerId && gData.opponent.isTurn && (gData.status === 'p2_awaiting_roll1_emoji' || gData.status === 'p2_awaiting_roll2_emoji'));
                    if (isInitiatorDE_PvP || isOpponentDE_PvP) {
                        gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDiceEscalatorEmoji = true; break;
                    }
                }
                if (gData.type === GAME_IDS.DICE_21 && gData.playerId === rollerId && gData.status === 'player_turn_awaiting_emoji') {
                    gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDice21Emoji = true; break;
                }
                if (gData.type === GAME_IDS.DICE_21_PVP) {
                    const isInitiatorD21_PvP = (gData.initiator && gData.initiator.userId === rollerId && gData.initiator.isTurn && gData.status === 'initiator_turn' && gData.initiator.status === 'playing_turn');
                    const isOpponentD21_PvP = (gData.opponent && gData.opponent.userId === rollerId && gData.opponent.isTurn && gData.status === 'opponent_turn' && gData.opponent.status === 'playing_turn');
                    if (isInitiatorD21_PvP || isOpponentD21_PvP) {
                        gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDice21Emoji = true; break;
                    }
                }
                if (gData.type === GAME_IDS.DUEL_PVB && gData.playerId === rollerId && (gData.status === 'player_awaiting_roll1_emoji' || gData.status === 'player_awaiting_roll2_emoji')) {
                    gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDuelGameEmoji = true; break;
                }
                if (gData.type === GAME_IDS.DUEL_PVP) {
                    const isInitiatorDuel_PvP = (gData.initiator?.userId === rollerId && gData.initiator?.isTurn && (gData.status === 'p1_awaiting_roll1_emoji' || gData.status === 'p1_awaiting_roll2_emoji'));
                    const isOpponentDuel_PvP = (gData.opponent?.userId === rollerId && gData.opponent?.isTurn && (gData.status === 'p2_awaiting_roll1_emoji' || gData.status === 'p2_awaiting_roll2_emoji'));
                    if (isInitiatorDuel_PvP || isOpponentDuel_PvP) {
                        gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDuelGameEmoji = true; break;
                    }
                }
            }
        }

        if (gameIdForDiceRoll && gameDataForDiceRoll) {
            console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Processing dice for game ${gameIdForDiceRoll} (Type: ${gameDataForDiceRoll.type}). Deleting user dice message ${msg.message_id}.`);
            bot.deleteMessage(chatId, msg.message_id).catch(() => { console.warn(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Failed to delete user's dice emoji message ${msg.message_id}`); });

            if (isDiceEscalatorEmoji) {
                console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Routing to Dice Escalator processor.`);
                if (gameDataForDiceRoll.type === GAME_IDS.DICE_ESCALATOR_PVB) {
                    if (typeof processDiceEscalatorPvBRollByEmoji_New === 'function') {
                        await processDiceEscalatorPvBRollByEmoji_New(gameDataForDiceRoll, diceValue);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: processDiceEscalatorPvBRollByEmoji_New`);
                } else if (gameDataForDiceRoll.type === GAME_IDS.DICE_ESCALATOR_PVP) {
                    if (typeof processDiceEscalatorPvPRollByEmoji_New === 'function') {
                        await processDiceEscalatorPvPRollByEmoji_New(gameDataForDiceRoll, diceValue, rollerId); 
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: processDiceEscalatorPvPRollByEmoji_New`);
                }
            } else if (isDice21Emoji) {
                console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Routing to Dice 21 processor.`);
                if (gameDataForDiceRoll.type === GAME_IDS.DICE_21) { 
                    if (typeof processDice21PvBRollByEmoji === 'function') await processDice21PvBRollByEmoji(gameDataForDiceRoll, diceValue);
                    else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: processDice21PvBRollByEmoji for PvB`);
                } else if (gameDataForDiceRoll.type === GAME_IDS.DICE_21_PVP) { 
                    if (typeof processDice21PvPRollByEmoji === 'function') await processDice21PvPRollByEmoji(gameDataForDiceRoll, diceValue, rollerId); 
                    else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: processDice21PvPRollByEmoji for PvP`);
                }
            } else if (isDuelGameEmoji) {
                console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Routing to Duel processor.`);
                if (gameDataForDiceRoll.type === GAME_IDS.DUEL_PVB) {
                    if (typeof processDuelPvBRollByEmoji === 'function') await processDuelPvBRollByEmoji(gameDataForDiceRoll, diceValue);
                    else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: processDuelPvBRollByEmoji`);
                } else if (gameDataForDiceRoll.type === GAME_IDS.DUEL_PVP) {
                    if (typeof processDuelPvPRollByEmoji === 'function') await processDuelPvPRollByEmoji(gameDataForDiceRoll, diceValue, rollerId); 
                    else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: processDuelPvPRollByEmoji`);
                }
            }
            return; 
        } else {
            console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] No active game found matching criteria for dice roll from user ${rollerId} in chat ${chatId}. Dice value: ${diceValue}.`);
        }
    }

    if (userStateCache.has(userId) && !text.startsWith('/')) {
        const currentState = userStateCache.get(userId);
        console.log(`${LOG_PREFIX_MSG_HANDLER} User ${userId} has pending state: ${currentState.state}. Routing to stateful input.`);
        if (typeof routeStatefulInput === 'function') {
            await routeStatefulInput(msg, currentState);
            return;
        } else {
            console.warn(`${LOG_PREFIX_MSG_HANDLER} routeStatefulInput function not available. Cleared user state for ${userId} as a fallback.`);
            if (typeof clearUserState === 'function') clearUserState(userId); else userStateCache.delete(userId);
        }
    }

    if (text.startsWith('/')) {
        if (!userId || userId === "undefined") {
            console.error(`${LOG_PREFIX_MSG_HANDLER} CRITICAL: User ID undefined for command. Msg: ${stringifyWithBigInt(msg).substring(0,200)}`);
            await safeSendMessage(chatId, "⚠️ Error with your user session. Please try the `/start` command again.", {}); return;
        }
        let userForCommandProcessing = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
        if (!userForCommandProcessing) {
            console.warn(`${LOG_PREFIX_MSG_HANDLER} Could not get/create user for command processing. User ID: ${userId}`);
            await safeSendMessage(chatId, "😕 I couldn't access your player profile just now. Please try the `/start` command and then your desired action.", {}); return;
        }

        const now = Date.now();
        if (userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
            console.log(`${LOG_PREFIX_MSG_HANDLER} Command cooldown active for user ${userId}. Ignoring command: ${text}`);
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
                } else if (chatType === 'group' || chatType === 'supergroup') {
                    console.log(`${LOG_PREFIX_MSG_HANDLER} Command /${commandName} directed at another bot in group. Ignoring.`);
                    return;
                } else {
                    commandName = commandName.split('@')[0];
                }
            } catch (getMeErr) { console.error(`${LOG_PREFIX_MSG_HANDLER} Error stripping @botname from command /${commandName}: ${getMeErr.message}`); }
        }

        console.log(`${LOG_PREFIX_MSG_HANDLER} CMD: /${commandName}, Args: [${commandArgs.join(', ')}] from User ${userId} (${userForCommandProcessing.username || 'NoUsername'}) in Chat ${chatId} (${chatType})`);

        try {
            switch (commandName) {
                case 'start': await handleStartCommand(msg, commandArgs); break;
                case 'help': await handleHelpCommand(msg); break;
                case 'balance': case 'bal': await handleBalanceCommand(msg); break;
                case 'rules': case 'info': await handleRulesCommand(chatId, userForCommandProcessing, originalMessageId, false, chatType); break;
                case 'jackpot': await handleJackpotCommand(chatId, userForCommandProcessing, chatType); break;
                case 'leaderboards': await handleLeaderboardsCommand(msg, commandArgs); break;
                case 'wallet': if (typeof handleWalletCommand === 'function') await handleWalletCommand(msg); else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleWalletCommand`); break;
                case 'deposit': if (typeof handleDepositCommand === 'function') await handleDepositCommand(msg, commandArgs, userId); else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleDepositCommand`); break;
                case 'withdraw': if (typeof handleWithdrawCommand === 'function') await handleWithdrawCommand(msg, commandArgs, userId); else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleWithdrawCommand`); break;
                case 'referral': if (typeof handleReferralCommand === 'function') await handleReferralCommand(msg); else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleReferralCommand`); break;
                case 'history': if (typeof handleHistoryCommand === 'function') await handleHistoryCommand(msg); else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleHistoryCommand`); break;
                case 'setwallet': if (typeof handleSetWalletCommand === 'function') await handleSetWalletCommand(msg, commandArgs); else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleSetWalletCommand`); break;
                case 'grant': await handleGrantCommand(msg, commandArgs, userForCommandProcessing); break;
                case 'tip':
                    if (typeof handleTipCommand === 'function') {
                        await handleTipCommand(msg, commandArgs, userForCommandProcessing);
                    } else {
                        console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleTipCommand`);
                        await safeSendMessage(chatId, "The tipping feature is currently under maintenance.", {});
                    }
                    break;
                case 'coinflip': case 'startcoinflip':
                    if (typeof handleStartGroupCoinFlipCommand === 'function') {
                        const betCF = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betCF) await handleStartGroupCoinFlipCommand(chatId, userForCommandProcessing, betCF, originalMessageId, chatType);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartGroupCoinFlipCommand`); break;
                case 'rps': case 'startrps':
                    if (typeof handleStartGroupRPSCommand === 'function') {
                        const betRPS = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betRPS) await handleStartGroupRPSCommand(chatId, userForCommandProcessing, betRPS, originalMessageId, chatType);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartGroupRPSCommand`); break;
                case 'de': case 'diceescalator':
                    if (chatType === 'private') {
                        await safeSendMessage(chatId, `🎲 The Dice Escalator game, offering both Player vs Bot and Player vs Player modes, must be initiated in a **group chat**. Please use the \`/de <bet>\` command there to start the action!`, { parse_mode: 'MarkdownV2' });
                        break;
                    }
                    if (typeof handleStartDiceEscalatorUnifiedOfferCommand_New === 'function') {
                        const betDE = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betDE) await handleStartDiceEscalatorUnifiedOfferCommand_New(msg, betDE);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartDiceEscalatorUnifiedOfferCommand_New`); break;
                case 'd21': case 'blackjack':
                    if (typeof handleStartDice21Command === 'function') {
                        const betD21 = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betD21) await handleStartDice21Command(msg, betD21, commandArgs[1]);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartDice21Command`); break;
                case 'duel': case 'highroller':
                    if (chatType === 'private') {
                        await safeSendMessage(chatId, `⚔️ The High Roller Duel game can only be started in a group chat. Please use the command there to challenge others or the Bot Dealer!`, { parse_mode: 'MarkdownV2' });
                        break;
                    }
                    if (typeof handleStartDuelUnifiedOfferCommand === 'function') {
                        const betDuel = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betDuel) await handleStartDuelUnifiedOfferCommand(msg, betDuel);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartDuelUnifiedOfferCommand`); break;
                case 'ou7': case 'overunder7':
                    if (typeof handleStartOverUnder7Command === 'function') {
                        const betOU7 = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betOU7) await handleStartOverUnder7Command(msg, betOU7);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartOverUnder7Command`); break;
                case 'ladder': case 'greedsladder':
                    if (typeof handleStartLadderCommand === 'function') {
                        const betLadder = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betLadder) await handleStartLadderCommand(msg, betLadder);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartLadderCommand`); break;
                case 's7': case 'sevenout': case 'craps':
                    if (typeof handleStartSevenOutCommand === 'function') {
                        const betS7 = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betS7) await handleStartSevenOutCommand(msg, betS7);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartSevenOutCommand`); break;
                case 'slot': case 'slots': case 'slotfrenzy':
                    if (typeof handleStartSlotCommand === 'function') {
                        const betSlot = await parseBetAmount(commandArgs[0], chatId, chatType, userId);
                        if(betSlot) await handleStartSlotCommand(msg, betSlot);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartSlotCommand`); break;
                case 'mines': 
                    if (typeof handleStartMinesCommand === 'function') {
                        // handleStartMinesCommand from Part 5a, S2 handles its own args parsing
                        await handleStartMinesCommand(msg, commandArgs, userForCommandProcessing);
                    } else {
                        console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartMinesCommand`);
                        await safeSendMessage(chatId, "The Mines game is currently under construction.", {});
                    }
                    break;

                default:
                    const selfBotInfoDefault = await bot.getMe();
                    if (chatType === 'private' || text.includes(`@${selfBotInfoDefault.username}`)) {
                        console.log(`${LOG_PREFIX_MSG_HANDLER} Unknown command: /${commandName}`);
                        await safeSendMessage(chatId, `🤔 Hmmm, I don't recognize the command \`/${escapeMarkdownV2(commandName || "")}\`. Try \`/help\` for a list of my amazing games and features!!`, { parse_mode: 'MarkdownV2' });
                    }
                    break;
            }
        } catch (commandError) {
            console.error(`${LOG_PREFIX_MSG_HANDLER} 🚨 UNHANDLED ERROR IN COMMAND ROUTER for /${commandName}: ${commandError.message}`, commandError.stack?.substring(0, 700));
            await safeSendMessage(chatId, `⚙️ Oops! A critical error occurred while I was processing your command \`/${escapeMarkdownV2(commandName || "")}\`. My apologies! Please try again in a moment.`, { parse_mode: 'MarkdownV2' });
            if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 Command Router Error for /${escapeMarkdownV2(commandName)} from User: ${userId} in Chat: ${chatId}. Error: \`${escapeMarkdownV2(commandError.message)}\``);
        }
    }
});


// --- Callback Query Handler (`bot.on('callback_query')`) ---
bot.on('callback_query', async (callbackQuery) => {
    const LOG_PREFIX_CBQ = `[CBQ ID:${callbackQuery.id} User:${callbackQuery.from.id} Chat:${callbackQuery.message?.chat?.id || 'N/A'}]`;
    console.log(`${LOG_PREFIX_CBQ} Received callback_query. Data: ${callbackQuery.data}`);

    if (isShuttingDown) {
        console.log(`${LOG_PREFIX_CBQ} Bot is shutting down. Answering callback and returning.`);
        await bot.answerCallbackQuery(callbackQuery.id, { text: "The bot is currently shutting down. Please try again later." }).catch(() => {});
        return;
    }

    const msg = callbackQuery.message;
    const userFromCb = callbackQuery.from;
    const callbackQueryId = callbackQuery.id;
    const data = callbackQuery.data;

    if (!msg || !userFromCb || !data) {
        console.warn(`${LOG_PREFIX_CBQ} Ignoring malformed/incomplete callback query. Data: ${data}`);
        await bot.answerCallbackQuery(callbackQueryId).catch(() => {});
        return;
    }

    const userId = String(userFromCb.id || userFromCb.telegram_id); 
    if (!userId || userId === "undefined") {
        console.error(`${LOG_PREFIX_CBQ} CRITICAL: User ID undefined for callback. Callback Data: ${data}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ An error occurred identifying your session. Please try the action again.", show_alert: true }).catch(() => {});
        return;
    }

    const originalChatId = String(msg.chat.id);
    const originalChatType = msg.chat.type;
    const originalMessageId = String(msg.message_id);

    await bot.answerCallbackQuery(callbackQueryId).catch(()=>{ console.warn(`${LOG_PREFIX_CBQ} Initial answerCallbackQuery failed for ID ${callbackQueryId}. Action: ${data.split(':')[0]}`); });

    let userObjectForCallback = await getOrCreateUser(userId, userFromCb.username, userFromCb.first_name, userFromCb.last_name);
    if (!userObjectForCallback) {
        console.error(`${LOG_PREFIX_CBQ} Failed to get/create user for callback processing. User ID: ${userId}. Callback Data: ${data}`);
        return;
    }

    const [action, ...params] = data.split(':');
    console.log(`${LOG_PREFIX_CBQ} Parsed Action: "${action}", Params: [${params.join(', ')}] (Chat: ${originalChatId}, Type: ${originalChatType}, MsgID: ${originalMessageId})`);

    if (action === 'menu' && (params[0] === 'main' || params[0] === 'wallet' || params[0] === 'game_selection' || params[0] === 'rules')) {
        console.log(`${LOG_PREFIX_CBQ} Clearing user state for ${userId} due to menu navigation: ${action}:${params[0]}`);
        if (typeof clearUserState === 'function') clearUserState(userId); else userStateCache.delete(userId);
    }

    let isCallbackRedirectedToDm = false;
    const sensitiveActions = [
        DEPOSIT_CALLBACK_ACTION, QUICK_DEPOSIT_CALLBACK_ACTION, 'quick_deposit',
        WITHDRAW_CALLBACK_ACTION, 'menu:wallet', 'menu:history', 'menu:link_wallet_prompt',
        'menu:referral', 'process_withdrawal_confirm'
    ];
    const fullCallbackActionForSensitivityCheck = action === 'menu' ? `${action}:${params[0]}` : action;

    if ((originalChatType === 'group' || originalChatType === 'supergroup') && sensitiveActions.includes(fullCallbackActionForSensitivityCheck)) {
        console.log(`${LOG_PREFIX_CBQ} Sensitive action "${fullCallbackActionForSensitivityCheck}" in group. Redirecting user ${userId} to DM.`);
        isCallbackRedirectedToDm = true;
        let botUsernameForRedirect = BOT_NAME || "our bot";
        try { const selfInfo = await bot.getMe(); if (selfInfo.username) botUsernameForRedirect = selfInfo.username; } catch (e) { console.warn(`${LOG_PREFIX_CBQ} Could not fetch bot username for DM redirect message: ${e.message}`); }

        const redirectText = `${getPlayerDisplayReference(userObjectForCallback)}, for your privacy and security, please continue this action in our direct message. I've sent you a prompt there: @${escapeMarkdownV2(botUsernameForRedirect)}`;
        const callbackParamsForUrl = params && params.length > 0 ? `_${params.join('_')}` : '';
        const menuActionForUrl = action === 'menu' ? `${action}_${params[0]}` : action;

        if (originalMessageId && bot) {
            await bot.editMessageText(redirectText, {
                chat_id: originalChatId, message_id: Number(originalMessageId), parse_mode: 'MarkdownV2',
                reply_markup: { inline_keyboard: [[{text: `📬 Open DM with @${escapeMarkdownV2(botUsernameForRedirect)}`, url: `https://t.me/${botUsernameForRedirect}?start=cb_${menuActionForUrl}${callbackParamsForUrl}`}]] }
            }).catch(e => {
                if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                    console.warn(`${LOG_PREFIX_CBQ} Failed to edit redirect message in group: ${e.message}. Sending new message as fallback.`);
                    safeSendMessage(originalChatId, redirectText, {parse_mode: 'MarkdownV2'});
                }
            });
        } else {
            console.warn(`${LOG_PREFIX_CBQ} Original message ID missing for DM redirect. Sending new message.`);
            await safeSendMessage(originalChatId, redirectText, {parse_mode: 'MarkdownV2'});
        }
    }
    const mockMsgObjectForHandler = {
        from: userObjectForCallback, 
        chat: { id: isCallbackRedirectedToDm ? userId : originalChatId, type: isCallbackRedirectedToDm ? 'private' : originalChatType },
        message_id: isCallbackRedirectedToDm ? null : originalMessageId,
        isCallbackRedirect: isCallbackRedirectedToDm,
        originalChatInfo: isCallbackRedirectedToDm ? { id: originalChatId, type: originalChatType, messageId: originalMessageId } : null,
    };

    try {
        if (action.startsWith(RULES_CALLBACK_PREFIX)) {
            const gameCodeForRule = action.substring(RULES_CALLBACK_PREFIX.length);
            console.log(`${LOG_PREFIX_CBQ} Routing to handleDisplayGameRules for game code: ${gameCodeForRule}`);
            if (typeof handleDisplayGameRules === 'function') {
                await handleDisplayGameRules(mockMsgObjectForHandler.chat.id, mockMsgObjectForHandler.message_id, gameCodeForRule, userObjectForCallback, mockMsgObjectForHandler.chat.type);
            } else console.error(`${LOG_PREFIX_CBQ} Missing handler: handleDisplayGameRules`);
        } else {
            console.log(`${LOG_PREFIX_CBQ} Routing callback action "${action}" via main switch statement.`);
            switch (action) {
                case 'show_rules_menu':
                    if (typeof handleRulesCommand === 'function') await handleRulesCommand(mockMsgObjectForHandler.chat.id, userObjectForCallback, mockMsgObjectForHandler.message_id, true, mockMsgObjectForHandler.chat.type);
                    else console.error(`${LOG_PREFIX_CBQ} Missing handler: handleRulesCommand`);
                    break;
                case DEPOSIT_CALLBACK_ACTION:
                case QUICK_DEPOSIT_CALLBACK_ACTION:
                case 'quick_deposit':
                    if (typeof handleDepositCommand === 'function') await handleDepositCommand(mockMsgObjectForHandler, params, userId);
                    else console.error(`${LOG_PREFIX_CBQ} Missing handler: handleDepositCommand`);
                    break;
                case WITHDRAW_CALLBACK_ACTION:
                    if (typeof handleWithdrawCommand === 'function') await handleWithdrawCommand(mockMsgObjectForHandler, params, userId);
                    else console.error(`${LOG_PREFIX_CBQ} Missing handler: handleWithdrawCommand`);
                    break;
                case 'menu':
                    console.log(`${LOG_PREFIX_CBQ} Routing to handleMenuAction for action: ${action}:${params[0]}`);
                    if (typeof handleMenuAction === 'function') await handleMenuAction(userId, originalChatId, originalMessageId, params[0], params.slice(1), true, mockMsgObjectForHandler.chat.type, isCallbackRedirectedToDm, mockMsgObjectForHandler.originalChatInfo);
                    else console.error(`${LOG_PREFIX_CBQ} Missing handler: handleMenuAction`);
                    break;
                case 'process_withdrawal_confirm':
                    if(typeof processWithdrawalConfirmation === 'function') {
                        const decision = params[0];
                        const currentState = userStateCache.get(userId);
                        if (decision === 'yes' && currentState && currentState.state === 'awaiting_withdrawal_confirmation' && currentState.chatId === userId) {
                            await processWithdrawalConfirmation(userId, currentState.chatId, currentState.messageId, currentState.data.linkedWallet, currentState.data.amountLamportsStr, currentState.data.feeLamportsStr, currentState.data.originalGroupChatId, currentState.data.originalGroupMessageId);
                        } else if (decision === 'no') {
                            if (bot && originalMessageId) bot.editMessageText("Withdrawal cancelled by user.", {chat_id: originalChatId, message_id: Number(originalMessageId), reply_markup: {inline_keyboard:[[{text:"💳 Back to Wallet", callback_data:"menu:wallet"}]]}}).catch(()=>{});
                            clearUserState(userId);
                        } else {
                            if (bot && originalMessageId) bot.editMessageText("Invalid confirmation or state expired. Withdrawal cancelled.", {chat_id: originalChatId, message_id: Number(originalMessageId), reply_markup: {inline_keyboard:[[{text:"💳 Back to Wallet", callback_data:"menu:wallet"}]]}}).catch(()=>{});
                            clearUserState(userId);
                        }
                    } else console.error(`${LOG_PREFIX_CBQ} Missing handler: processWithdrawalConfirmation`);
                    break;

                case 'join_game':
                case 'cancel_game':
                case 'rps_choose':
                case 'play_again_coinflip': 
                case 'play_again_rps':    
                    console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardGameCallback for action: ${action}`);
                    if (typeof forwardGameCallback === 'function') await forwardGameCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    else console.warn(`${LOG_PREFIX_CBQ} forwardGameCallback not defined for ${action}`);
                    break;

                case 'de_accept_bot_game':
                case 'de_accept_pvp_challenge':
                case 'de_cancel_unified_offer':
                case 'de_stand_pvb':
                case 'de_stand_pvp':
                case 'play_again_de_pvb':
                case 'play_again_de_pvp':
                case 'de_pvb_go_for_jackpot': 
                    console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardDiceEscalatorCallback_New for action: ${action}`);
                    if (typeof forwardDiceEscalatorCallback_New === 'function') {
                        await forwardDiceEscalatorCallback_New(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else console.error(`${LOG_PREFIX_CBQ} Missing handler: forwardDiceEscalatorCallback_New for Dice Escalator action: ${action}`);
                    break;

                case 'd21_accept_bot_game': case 'd21_accept_pvp_challenge': case 'd21_cancel_unified_offer':
                case 'd21_stand': case 'play_again_d21': case 'd21_pvb_cancel':
                case 'd21_pvp_stand': case 'play_again_d21_pvp':
                    console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardDice21Callback for action: ${action}`);
                    if (typeof forwardDice21Callback === 'function') {
                        await forwardDice21Callback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else console.warn(`${LOG_PREFIX_CBQ} forwardDice21Callback not defined for D21 action: ${action}`);
                    break;

                case 'duel_accept_bot_game': case 'duel_accept_pvp_challenge': case 'duel_cancel_unified_offer':
                case 'play_again_duel':
                    console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardDuelCallback for action: ${action}`);
                    if (typeof forwardDuelCallback === 'function') {
                        await forwardDuelCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else console.warn(`${LOG_PREFIX_CBQ} forwardDuelCallback not defined for Duel action: ${action}`);
                    break;

                case 'mines_difficulty_select': 
                case 'mines_cancel_offer':      
                case 'mines_tile':             // NEW: For clicking a game tile
                case 'mines_cashout':          // NEW: For cashing out
                case 'play_again_mines':
                    console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardMinesCallback for action: ${action}`);
                    if (typeof forwardMinesCallback === 'function') { 
                        await forwardMinesCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else {
                        console.warn(`${LOG_PREFIX_CBQ} forwardMinesCallback not defined or direct handler missing for Mines action: ${action}`);
                        await bot.answerCallbackQuery(callbackQueryId, { text: "Mines game action processing...", show_alert: false }).catch(() => {});
                    }
                    break;

                case 'ou7_choice':     case 'play_again_ou7':
                case 'ladder_roll':    case 'play_again_ladder': 
                case 's7_roll':        case 'play_again_s7': 
                case 'play_again_slot':
                case 'jackpot_display_noop': 
                    if (action === 'jackpot_display_noop') {
                        console.log(`${LOG_PREFIX_CBQ} Jackpot display no-op handled.`);
                    } else if (typeof forwardAdditionalGamesCallback === 'function') {
                        console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardAdditionalGamesCallback for action: ${action}`);
                        await forwardAdditionalGamesCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else {
                        console.warn(`${LOG_PREFIX_CBQ} forwardAdditionalGamesCallback not defined or direct handler missing for general game action: ${action}`);
                    }
                    break;

                default:
                    console.warn(`${LOG_PREFIX_CBQ} Unknown callback action encountered in main switch: "${action}" with params: [${params.join(', ')}]`);
                    break;
            }
        }
    } catch (callbackError) {
        console.error(`${LOG_PREFIX_CBQ} 🚨 UNHANDLED ERROR IN CALLBACK ROUTER for action ${action}: ${callbackError.message}`, callbackError.stack?.substring(0, 700));
        await safeSendMessage(userId, `⚙️ Oops! A critical error occurred while processing your action (\`${escapeMarkdownV2(action)}\`). My apologies! Please try again. If the problem persists, contacting support might be necessary.`, { parse_mode: 'MarkdownV2' });
        if (typeof notifyAdmin === 'function') {
            notifyAdmin(`🚨 Callback Router System Error 🚨\nAction: \`${escapeMarkdownV2(action)}\`\nUser: ${userId} (${userFromCb.username || 'N/A'})\nError: \`${escapeMarkdownV2(String(callbackError.message || callbackError))}\`\nThis was an unhandled exception in the main callback router.`, {parse_mode:'MarkdownV2'}).catch(()=>{});
        }
    }
});


// --- Helper function to forward game callbacks for Coinflip/RPS ---
async function forwardGameCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_GAME_CB_FWD = `[GameCB_Fwd UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_GAME_CB_FWD} Processing action. Params: ${params.join(',')}`);
    const gameIdOrBetAmountStr = params[0];

    switch (action) {
        case 'join_game':
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId for join_game.`); return; }
            if (typeof handleJoinGameCallback === 'function') await handleJoinGameCallback(originalChatId, userObject, gameIdOrBetAmountStr, originalMessageId, callbackQueryId, originalChatType);
            else console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing handler: handleJoinGameCallback`);
            break;
        case 'cancel_game':
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId for cancel_game.`); return; }
            if (typeof handleCancelGameCallback === 'function') await handleCancelGameCallback(originalChatId, userObject, gameIdOrBetAmountStr, originalMessageId, callbackQueryId, originalChatType);
            else console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing handler: handleCancelGameCallback`);
            break;
        case 'rps_choose':
            const gameIdRPS = params[0];
            const choice = params[1];
            if (!gameIdRPS || !choice) { console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId or choice for rps_choose.`); return; }
            if (typeof handleRPSChoiceCallback === 'function') await handleRPSChoiceCallback(originalChatId, userObject, gameIdRPS, choice, originalMessageId, callbackQueryId, originalChatType);
            else console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing handler: handleRPSChoiceCallback`);
            break;
        case 'play_again_coinflip':
        case 'play_again_rps':
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing bet amount for ${action}.`); return; }
            try {
                const betAmountCF_RPS = BigInt(gameIdOrBetAmountStr);
                const mockMsgCF_RPS = { from: userObject, chat: { id: originalChatId, type: originalChatType }, message_id: originalMessageId };
                if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});

                if (action === 'play_again_coinflip' && typeof handleStartGroupCoinFlipCommand === 'function') {
                    await handleStartGroupCoinFlipCommand(originalChatId, userObject, betAmountCF_RPS, null, originalChatType);
                } else if (action === 'play_again_rps' && typeof handleStartGroupRPSCommand === 'function') {
                    await handleStartGroupRPSCommand(originalChatId, userObject, betAmountCF_RPS, null, originalChatType);
                } else {
                    console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing start command handler for ${action}`);
                }
            } catch (e) { console.error(`${LOG_PREFIX_GAME_CB_FWD} Invalid bet amount for ${action}: '${gameIdOrBetAmountStr}'`); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true }); }
            break;
        default:
            console.warn(`${LOG_PREFIX_GAME_CB_FWD} Unhandled action in forwardGameCallback: ${action}`);
    }
}

// --- Helper function to forward Dice 21 callbacks ---
async function forwardDice21Callback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_D21_CB_FWD = `[D21_CB_Fwd UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_D21_CB_FWD} Processing action. Params: ${params.join(',')}`);
    const gameIdOrBetAmountStr = params[0];
    const chatDataForHandler = { id: originalChatId, type: originalChatType };

    switch (action) {
        case 'd21_accept_bot_game':
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_D21_CB_FWD} Missing offerId for d21_accept_bot_game.`); return; }
            if (typeof handleDice21AcceptBotGame === 'function') await handleDice21AcceptBotGame(gameIdOrBetAmountStr, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId);
            else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleDice21AcceptBotGame`);
            break;
        case 'd21_accept_pvp_challenge':
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_D21_CB_FWD} Missing offerId for d21_accept_pvp_challenge.`); return; }
            if (typeof handleDice21AcceptPvPChallenge === 'function') await handleDice21AcceptPvPChallenge(gameIdOrBetAmountStr, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId);
            else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleDice21AcceptPvPChallenge`);
            break;
        case 'd21_cancel_unified_offer':
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_D21_CB_FWD} Missing offerId for d21_cancel_unified_offer.`); return; }
            if (typeof handleDice21CancelUnifiedOffer === 'function') await handleDice21CancelUnifiedOffer(gameIdOrBetAmountStr, userObject, originalMessageId, originalChatId, callbackQueryId);
            else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleDice21CancelUnifiedOffer`);
            break;
        case 'd21_stand': 
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_D21_CB_FWD} Missing gameId for d21_stand.`); return; }
            if (typeof handleDice21PvBStand === 'function') await handleDice21PvBStand(gameIdOrBetAmountStr, userObject, originalMessageId, callbackQueryId, chatDataForHandler);
            else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleDice21PvBStand`);
            break;
        case 'play_again_d21': 
        case 'play_again_d21_pvp': 
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_D21_CB_FWD} Missing bet amount for ${action}.`); return; }
            try {
                const betAmountD21Replay = BigInt(gameIdOrBetAmountStr);
                const mockMsgForD21Replay = { from: userObject, chat: { id: originalChatId, type: originalChatType }, message_id: originalMessageId };
                if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
                if (typeof handleStartDice21Command === 'function') {
                    await handleStartDice21Command(mockMsgForD21Replay, betAmountD21Replay);
                } else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleStartDice21Command for ${action} replay`);
            } catch (e) { console.error(`${LOG_PREFIX_D21_CB_FWD} Invalid bet amount for ${action}: '${gameIdOrBetAmountStr}'`); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true }); }
            break;
        case 'd21_pvb_cancel': 
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_D21_CB_FWD} Missing gameId for d21_pvb_cancel.`); return; }
            if (typeof handleDice21PvBCancel === 'function') await handleDice21PvBCancel(gameIdOrBetAmountStr, userObject, originalMessageId, callbackQueryId, chatDataForHandler);
            else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleDice21PvBCancel`);
            break;
        case 'd21_pvp_stand': 
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_D21_CB_FWD} Missing gameId for d21_pvp_stand.`); return; }
            if (typeof handleDice21PvPStand === 'function') await handleDice21PvPStand(gameIdOrBetAmountStr, userObject.id || userObject.telegram_id, originalMessageId, callbackQueryId, chatDataForHandler);
            else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleDice21PvPStand (or handleDice21PvPStandAction)`);
            break;
        default:
            console.warn(`${LOG_PREFIX_D21_CB_FWD} Unhandled Dice 21 action in forwarder: ${action}`);
            break;
    }
}

// --- Helper function to forward Duel callbacks ---
async function forwardDuelCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_DUEL_CB_FWD = `[Duel_CB_Fwd UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_DUEL_CB_FWD} Processing action. Params: ${params.join(',')}`);
    const offerIdOrBetAmountStr = params[0];
    const mockMsgForHandler = { from: userObject, chat: { id: originalChatId, type: originalChatType }, message_id: originalMessageId };

    switch (action) {
        case 'duel_accept_bot_game':
            if (!offerIdOrBetAmountStr) { console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing offerId for duel_accept_bot_game.`); return; }
            if (typeof handleDuelAcceptBotGameCallback === 'function') await handleDuelAcceptBotGameCallback(offerIdOrBetAmountStr, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId); 
            else console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing handler: handleDuelAcceptBotGameCallback`);
            break;
        case 'duel_accept_pvp_challenge':
            if (!offerIdOrBetAmountStr) { console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing offerId for duel_accept_pvp_challenge.`); return; }
            if (typeof handleDuelAcceptPvPChallengeCallback === 'function') await handleDuelAcceptPvPChallengeCallback(offerIdOrBetAmountStr, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId); 
            else console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing handler: handleDuelAcceptPvPChallengeCallback`);
            break;
        case 'duel_cancel_unified_offer':
            if (!offerIdOrBetAmountStr) { console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing offerId for duel_cancel_unified_offer.`); return; }
            if (typeof handleDuelCancelUnifiedOfferCallback === 'function') await handleDuelCancelUnifiedOfferCallback(offerIdOrBetAmountStr, userObject, originalMessageId, originalChatId, callbackQueryId); 
            else console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing handler: handleDuelCancelUnifiedOfferCallback`);
            break;
        case 'play_again_duel':
            if (!offerIdOrBetAmountStr) { console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing bet amount for play_again_duel.`); return; }
            try {
                const betAmountDuelReplay = BigInt(offerIdOrBetAmountStr);
                if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
                if (typeof handleStartDuelUnifiedOfferCommand === 'function') {
                    await handleStartDuelUnifiedOfferCommand(mockMsgForHandler, betAmountDuelReplay);
                } else console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing handler: handleStartDuelUnifiedOfferCommand for Duel replay`);
            } catch (e) { console.error(`${LOG_PREFIX_DUEL_CB_FWD} Invalid bet amount for play_again_duel: '${offerIdOrBetAmountStr}'`); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true });}
            break;
        default:
            console.warn(`${LOG_PREFIX_DUEL_CB_FWD} Unhandled Duel action in forwarder: ${action}`);
            break;
    }
}

// --- Helper function to forward Dice Escalator callbacks (New Structure with Jackpot Choice) ---
async function forwardDiceEscalatorCallback_New(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_DE_CB_FWD_NEW = `[DE_CB_Fwd_New UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_DE_CB_FWD_NEW} Processing action. Action: ${action}, Params: ${params.join(',')}`);
    const gameIdOrOfferIdOrBet = params[0]; 
    const mockMsgForPlayAgain = { from: userObject, chat: { id: originalChatId, type: originalChatType }, message_id: originalMessageId };

    switch (action) {
        case 'de_accept_bot_game':
            if (!gameIdOrOfferIdOrBet) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing offerId for de_accept_bot_game.`); return; }
            if (typeof handleDiceEscalatorAcceptBotGame_New === 'function') {
                await handleDiceEscalatorAcceptBotGame_New(gameIdOrOfferIdOrBet, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId);
            } else console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: handleDiceEscalatorAcceptBotGame_New`);
            break;
        case 'de_accept_pvp_challenge':
            if (!gameIdOrOfferIdOrBet) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing offerId for de_accept_pvp_challenge.`); return; }
            if (typeof handleDiceEscalatorAcceptPvPChallenge_New === 'function') {
                await handleDiceEscalatorAcceptPvPChallenge_New(gameIdOrOfferIdOrBet, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId);
            } else console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: handleDiceEscalatorAcceptPvPChallenge_New`);
            break;
        case 'de_cancel_unified_offer':
            if (!gameIdOrOfferIdOrBet) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing offerId for de_cancel_unified_offer.`); return; }
            if (typeof handleDiceEscalatorCancelUnifiedOffer_New === 'function') {
                await handleDiceEscalatorCancelUnifiedOffer_New(gameIdOrOfferIdOrBet, userObject, originalMessageId, originalChatId, callbackQueryId);
            } else console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: handleDiceEscalatorCancelUnifiedOffer_New`);
            break;
        case 'de_pvb_go_for_jackpot': 
            if (!gameIdOrOfferIdOrBet) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing gameId for de_pvb_go_for_jackpot.`); return; }
            if (typeof handleDEGoForJackpot === 'function') { 
                await handleDEGoForJackpot(gameIdOrOfferIdOrBet, userObject, originalMessageId, callbackQueryId, { id: originalChatId, type: originalChatType });
            } else console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: handleDEGoForJackpot`);
            break;
        case 'de_stand_pvb':
            if (!gameIdOrOfferIdOrBet) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing gameId for de_stand_pvb.`); return; }
            if (typeof handleDiceEscalatorPvBStand_New === 'function') {
                await handleDiceEscalatorPvBStand_New(gameIdOrOfferIdOrBet, userObject, originalMessageId, callbackQueryId, { id: originalChatId, type: originalChatType });
            } else console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: handleDiceEscalatorPvBStand_New`);
            break;
        case 'play_again_de_pvb':
            const betAmountPvBStr_DE_Corrected = gameIdOrOfferIdOrBet; 
            if (!betAmountPvBStr_DE_Corrected) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing bet for play_again_de_pvb.`); return; }
            try {
                const betAmountPvB_DE_Corrected = BigInt(betAmountPvBStr_DE_Corrected);
                if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
                
                if (typeof startDiceEscalatorPvBGame_New === 'function') { 
                    await startDiceEscalatorPvBGame_New(mockMsgForPlayAgain.chat, userObject, betAmountPvB_DE_Corrected, null, true);
                } else {
                    console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: startDiceEscalatorPvBGame_New`); 
                }
            } catch (e) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Invalid bet amount for play_again_de_pvb: '${betAmountPvBStr_DE_Corrected}'`, e); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true }); }
            break;
        case 'de_stand_pvp':
            if (!gameIdOrOfferIdOrBet) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing gameId for de_stand_pvp.`); return; }
            if (typeof handleDiceEscalatorPvPStand_New === 'function') {
                await handleDiceEscalatorPvPStand_New(gameIdOrOfferIdOrBet, userObject, originalMessageId, callbackQueryId, { id: originalChatId, type: originalChatType });
            } else console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: handleDiceEscalatorPvPStand_New`);
            break;
        case 'play_again_de_pvp':
            const betAmountPvPStr_DE = gameIdOrOfferIdOrBet;
            if (!betAmountPvPStr_DE) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing bet for play_again_de_pvp.`); return; }
            try {
                const betAmountPvP_DE = BigInt(betAmountPvPStr_DE);
                if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
                if (typeof handleStartDiceEscalatorUnifiedOfferCommand_New === 'function') {
                    await handleStartDiceEscalatorUnifiedOfferCommand_New(mockMsgForPlayAgain, betAmountPvP_DE);
                } else console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: handleStartDiceEscalatorUnifiedOfferCommand_New for DE PvP replay`);
            } catch (e) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Invalid bet amount for play_again_de_pvp: '${betAmountPvPStr_DE}'`); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true });}
            break;
        default:
            console.warn(`${LOG_PREFIX_DE_CB_FWD_NEW} Unhandled Dice Escalator action in forwarder: ${action}`);
            break;
    }
}

// --- UPDATED Helper function to forward Mines game callbacks ---
async function forwardMinesCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_MINES_CB_FWD = `[MinesCB_Fwd UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_MINES_CB_FWD} Processing action. Params: ${params.join(',')}`);
    const gameIdOrOfferId = params[0]; // Can be offer ID or game ID
    const mockMsgForReplay = { from: userObject, chat: { id: originalChatId, type: originalChatType }, message_id: originalMessageId };

    switch(action) {
        case 'mines_difficulty_select': // params: [offerId, difficultyKey]
            const difficultyKey = params[1];
            if (!gameIdOrOfferId || !difficultyKey) {
                console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing offerId or difficultyKey for mines_difficulty_select.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Error selecting difficulty.", show_alert: true});
                return;
            }
            if (typeof handleMinesDifficultySelectionCallback === 'function') { 
                await handleMinesDifficultySelectionCallback(gameIdOrOfferId, userObject, difficultyKey, callbackQueryId, originalMessageId, originalChatId, originalChatType);
            } else {
                console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing handler: handleMinesDifficultySelectionCallback`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Mines difficulty selection is under construction!", show_alert: false});
            }
            break;
        case 'mines_cancel_offer': // params: [offerId]
             if (!gameIdOrOfferId) { console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing offerId for mines_cancel_offer.`); return; }
             if (typeof handleMinesCancelOfferCallback === 'function') { 
                await handleMinesCancelOfferCallback(gameIdOrOfferId, userObject, originalMessageId, originalChatId, callbackQueryId);
            } else {
                console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing handler: handleMinesCancelOfferCallback`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Mines offer cancellation is under construction!", show_alert: false});
            }
            break;
        case 'mines_tile': // params: [gameId, row, col]
            const row = params[1];
            const col = params[2];
            if (!gameIdOrOfferId || row === undefined || col === undefined) {
                console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing gameId, row, or col for mines_tile.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Error revealing tile.", show_alert: true});
                return;
            }
            if (typeof handleMinesTileClickCallback === 'function') {
                await handleMinesTileClickCallback(gameIdOrOfferId, userObject, parseInt(row), parseInt(col), callbackQueryId, originalMessageId, originalChatId);
            } else {
                console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing handler: handleMinesTileClickCallback`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Mines tile clicking is under construction!", show_alert: false});
            }
            break;
        case 'mines_cashout': // params: [gameId]
            if (!gameIdOrOfferId) {
                console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing gameId for mines_cashout.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Error cashing out.", show_alert: true});
                return;
            }
            if (typeof handleMinesCashOutCallback === 'function') {
                await handleMinesCashOutCallback(gameIdOrOfferId, userObject, callbackQueryId, originalMessageId, originalChatId);
            } else {
                console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing handler: handleMinesCashOutCallback`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Mines cash out is under construction!", show_alert: false});
            }
            break;
        case 'play_again_mines': // params: [betAmountLamports]
            const betAmountStr = gameIdOrOfferId; // In this case, the first param is the bet amount
            if (!betAmountStr) { console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing bet amount for play_again_mines.`); return; }
            try {
                const betAmountMinesReplay = BigInt(betAmountStr);
                if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
                if (typeof handleStartMinesCommand === 'function') { 
                    await handleStartMinesCommand(mockMsgForReplay, [betAmountMinesReplay.toString()], userObject);
                } else console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing handler: handleStartMinesCommand for Mines replay`);
            } catch (e) { console.error(`${LOG_PREFIX_MINES_CB_FWD} Invalid bet amount for play_again_mines: '${betAmountStr}'`); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true });}
            break;
        default:
            console.warn(`${LOG_PREFIX_MINES_CB_FWD} Unhandled Mines action in forwarder: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: "That Mines action isn't ready yet!", show_alert: false }).catch(() => {});
            break;
    }
}
// --- END OF UPDATED forwardMinesCallback function ---


// --- Helper function to forward other game callbacks ---
async function forwardAdditionalGamesCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_ADD_GAME_CB = `[AddGameCB_Fwd UID:${userObject.telegram_id || userObject.id} Act:${action}]`; 
    console.log(`${LOG_PREFIX_ADD_GAME_CB} Processing action. Params: ${params.join(',')}`);
    const gameIdOrBetAmountStr = params[0];
    const mockMsgForReplay = { from: userObject, chat: { id: originalChatId, type: originalChatType }, message_id: originalMessageId };

    try {
        const betAmount = (action.startsWith('play_again_') && gameIdOrBetAmountStr) ? BigInt(gameIdOrBetAmountStr) : null;

        if (action.startsWith('play_again_')) {
            if (!betAmount) {
                console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing or invalid bet amount for ${action}: '${gameIdOrBetAmountStr}'.`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Missing or invalid bet amount for replay.", show_alert: true });
                return;
            }
            if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});
        }

        switch (action) {
            case 'ou7_choice':
                const choiceOU7 = params[1]; 
                if (!gameIdOrBetAmountStr || !choiceOU7) { console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing params for ou7_choice.`); return; }
                if (typeof handleOverUnder7Choice === 'function') await handleOverUnder7Choice(gameIdOrBetAmountStr, userObject, choiceOU7, originalMessageId, callbackQueryId, {id: originalChatId, type: originalChatType});
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleOverUnder7Choice`);
                break;
            case 'play_again_ou7':
                if (typeof handleStartOverUnder7Command === 'function') await handleStartOverUnder7Command(mockMsgForReplay, betAmount);
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleStartOverUnder7Command`);
                break;
            case 'play_again_ladder':
                if (typeof handleStartLadderCommand === 'function') await handleStartLadderCommand(mockMsgForReplay, betAmount);
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleStartLadderCommand`);
                break;
            case 'play_again_s7':
                if (typeof handleStartSevenOutCommand === 'function') await handleStartSevenOutCommand(mockMsgForReplay, betAmount);
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleStartSevenOutCommand`);
                break;
            case 'play_again_slot':
                if (typeof handleStartSlotCommand === 'function') await handleStartSlotCommand(mockMsgForReplay, betAmount);
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleStartSlotCommand`);
                break;
            default:
                console.warn(`${LOG_PREFIX_ADD_GAME_CB} Unhandled action in forwardAdditionalGamesCallback: ${action}`);
        }
    } catch (e) {
        console.error(`${LOG_PREFIX_ADD_GAME_CB} Error processing ${action} for param '${gameIdOrBetAmountStr}': ${e.message}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Error processing action.", show_alert: true });
    }
}
// --- End of Part 5a, Section 1 (REVISED for New Dice Escalator & Full Routing for Jackpot Choice) ---
// --- Start of Part 6 ---
// index.js - Part 6: Main Application Logic (Initialization, Error Handling, Graceful Shutdown)
//---------------------------------------------------------------------------
// Assumed all necessary functions from previous parts are loaded and available.

// --- Global Error Handlers ---

process.on('uncaughtException', async (error, origin) => {
    console.error(`🚨 UNCAUGHT EXCEPTION! Origin: ${origin}`);
    console.error(error); // Log the full error object
    const errorMessage = error.message || 'No specific message';
    const errorStack = error.stack || 'No stack trace available';
    const adminMessage = `🚨 *CRITICAL: Uncaught Exception* (${escapeMarkdownV2(BOT_NAME)}) 🚨\n\nBot encountered a critical error and will attempt to shut down\\. \n\n*Origin:* \`${escapeMarkdownV2(String(origin))}\`\n*Error:* \`${escapeMarkdownV2(errorMessage)}\`\n*Stack (Partial):*\n\`\`\`\n${escapeMarkdownV2(errorStack.substring(0, 700))}\n\`\`\`\nPlease check server logs immediately for full details\\.`;

    if (!isShuttingDown) { 
        console.log("Initiating shutdown due to uncaught exception...");
        if (typeof notifyAdmin === 'function') {
            await notifyAdmin(adminMessage).catch(err => console.error("Failed to notify admin about uncaught exception:", err.message));
        }
        await gracefulShutdown('uncaught_exception'); 
        setTimeout(() => {
            console.error("Forcing exit after uncaught exception shutdown attempt timeout.");
            process.exit(1);
        }, SHUTDOWN_FAIL_TIMEOUT_MS + 5000); 
    } else {
        console.log("Uncaught exception during ongoing shutdown. Forcing exit.");
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
let expressServerInstance = null; 

async function gracefulShutdown(signal = 'SIGINT') {
    if (isShuttingDown) { 
        console.log("Graceful shutdown already in progress...");
        return;
    }
    isShuttingDown = true; 

    console.log(`\n🛑 Received signal: ${signal}. Initiating graceful shutdown for ${BOT_NAME} v${BOT_VERSION}...`);
    const adminShutdownMessage = `🔌 *Bot Shutdown Initiated* 🔌\n\n${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} is now shutting down due to signal: \`${escapeMarkdownV2(signal)}\`\\. Finalizing operations\\.\\.\\.`;
    if (typeof notifyAdmin === 'function' && signal !== 'test_mode_exit' && signal !== 'initialization_error') {
        await notifyAdmin(adminShutdownMessage).catch(err => console.error("Failed to send admin shutdown initiation notification:", err.message));
    }

    console.log("  ⏳ Stopping Telegram bot polling...");
    if (bot && typeof bot.stopPolling === 'function' && typeof bot.isPolling === 'function' && bot.isPolling()) {
        try {
            await bot.stopPolling({ cancel: true }); 
            console.log("  ✅ Telegram bot polling stopped.");
        } catch (e) {
            console.error("  ❌ Error stopping Telegram bot polling:", e.message);
        }
    } else {
        // console.log("  ℹ️ Telegram bot polling was not active or stopPolling not available/needed."); // Reduced log
    }

    if (typeof stopDepositMonitoring === 'function') {
        console.log("  ⏳ Stopping deposit monitoring...");
        try { await stopDepositMonitoring(); console.log("  ✅ Deposit monitoring stopped."); }
        catch(e) { console.error("  ❌ Error stopping deposit monitoring:", e.message); }
    } else { console.warn("  ⚠️ stopDepositMonitoring function not defined.");}

    if (typeof stopSweepingProcess === 'function') {
        console.log("  ⏳ Stopping sweeping process...");
        try { await stopSweepingProcess(); console.log("  ✅ Sweeping process stopped."); }
        catch(e) { console.error("  ❌ Error stopping sweeping process:", e.message); }
    } else { console.warn("  ⚠️ stopSweepingProcess function not defined.");}
    
    const queuesToStop = { payoutProcessorQueue, depositProcessorQueue }; 
    for (const [queueName, queueInstance] of Object.entries(queuesToStop)) {
        if (queueInstance && typeof queueInstance.onIdle === 'function' && typeof queueInstance.clear === 'function') {
            console.log(`  ⏳ Waiting for ${queueName} (Size: ${queueInstance.size}, Pending: ${queueInstance.pending}) to idle...`);
            try {
                if (queueInstance.size > 0 || queueInstance.pending > 0) {
                    await Promise.race([queueInstance.onIdle(), sleep(15000)]); // Max 15s wait
                }
                queueInstance.clear(); 
                console.log(`  ✅ ${queueName} is idle and cleared.`);
            } catch (qError) {
                console.warn(`  ⚠️ Error or timeout waiting for ${queueName} to idle: ${qError.message}. Clearing queue.`);
                queueInstance.clear();
            }
        } else {
            // console.log(`  ⚠️ Queue ${queueName} not defined or does not support onIdle/clear.`); // Reduced log
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
         // console.log("  ℹ️ Express server not running or not managed by this shutdown process."); // Reduced log
    }

    console.log("  ⏳ Closing PostgreSQL pool...");
    if (pool && typeof pool.end === 'function') {
        try {
            await pool.end();
            console.log("  ✅ PostgreSQL pool closed.");
        } catch (e) {
            console.error("  ❌ Error closing PostgreSQL pool:", e.message);
        }
    } else {
        // console.log("  ⚠️ PostgreSQL pool not active or .end() not available."); // Reduced log
    }

    console.log(`🏁 ${BOT_NAME} shutdown sequence complete. Exiting now.`);
    const finalAdminMessage = `✅ *Bot Shutdown Complete* ✅\n\n${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} has successfully shut down\\.`;
    if (typeof notifyAdmin === 'function' && signal !== 'test_mode_exit' && signal !== 'initialization_error') {
        notifyAdmin(finalAdminMessage).catch(err => console.error("Failed to send final admin shutdown notification:", err.message));
    }
    
    await sleep(500); 
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
            throw new Error("FATAL: initializeDatabaseSchema function is not defined!");
        }
        await initializeDatabaseSchema();
        console.log("✅ Database schema initialized successfully.");

        console.log("⚙️ Step 2: Connecting to Telegram & Starting Bot...");
        if (!bot || typeof bot.getMe !== 'function') {
            throw new Error("FATAL: Telegram bot instance is not correctly configured.");
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
        bot.on('webhook_error', async (error) => { 
            console.error(`[Telegram Webhook Error] Code: ${error.code || 'N/A'}, Message: ${error.message || String(error)}`);
            const adminMsg = `📡 *Telegram Webhook Error* (${escapeMarkdownV2(BOT_NAME)}) 📡\n\nError: \`${escapeMarkdownV2(String(error.message || error))}\`\\.\nBot message receiving may be affected\\.`;
            if (typeof notifyAdmin === 'function' && !isShuttingDown) {
                await notifyAdmin(adminMsg).catch(err => console.error("Failed to notify admin about webhook error:", err.message));
            }
        });
        console.log("✅ Telegram Bot is online and polling for messages (or webhook configured).");
        
        if (ADMIN_USER_ID && typeof safeSendMessage === 'function') {
            const currentTime = new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' } || 'UTC'); // Fallback to UTC
            await safeSendMessage(ADMIN_USER_ID, `🚀 *${escapeMarkdownV2(BOT_NAME)} v${escapeMarkdownV2(BOT_VERSION)} Started Successfully* 🚀\nBot is online and operational\\. Current time: ${currentTime}`, {parse_mode: 'MarkdownV2'});
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
            console.warn("⚠️ getSolUsdPrice function not defined. Price features will be unavailable.");
        }

        console.log("⚙️ Step 4: Starting Background Payment Processes...");
        if (typeof startDepositMonitoring === 'function') {
            startDepositMonitoring(); 
            // console.log("  ▶️ Deposit monitoring process initiated."); // Reduced log
        } else {
            console.warn("⚠️ Deposit monitoring (startDepositMonitoring) function not defined.");
        }

        if (typeof startSweepingProcess === 'function') {
            startSweepingProcess(); 
            // console.log("  ▶️ Address sweeping process initiated."); // Reduced log
        } else {
            console.warn("⚠️ Address sweeping (startSweepingProcess) function not defined.");
        }
        
        if (process.env.ENABLE_PAYMENT_WEBHOOKS === 'true') {
            console.log("⚙️ Step 5: Setting up and starting Payment Webhook Server...");
            if (typeof setupPaymentWebhook === 'function' && app) { 
                const port = parseInt(process.env.PAYMENT_WEBHOOK_PORT, 10) || 3000;
                try {
                    setupPaymentWebhook(app); 
                    
                    expressServerInstance = app.listen(port, () => {
                        console.log(`  ✅ Payment webhook server listening on port ${port} at path ${process.env.PAYMENT_WEBHOOK_PATH || '/webhook/solana-payments'}`);
                    });

                    expressServerInstance.on('error', (serverErr) => {
                        console.error(`  ❌ Express server error: ${serverErr.message}`, serverErr);
                        if (serverErr.code === 'EADDRINUSE') {
                            console.error(`  🚨 FATAL: Port ${port} is already in use for webhooks. Webhook server cannot start.`);
                            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 Webhook Server Failed to Start 🚨\nPort \`${port}\` is already in use\\. Payment webhooks will not function\\.`, {parse_mode:'MarkdownV2'});
                        }
                    });

                } catch (webhookError) {
                    console.error(`  ❌ Failed to set up or start payment webhook server: ${webhookError.message}`);
                }
            } else {
                console.warn("  ⚠️ Payment webhooks enabled, but setupPaymentWebhook function or Express app instance not available.");
            }
        } else {
            // console.log("ℹ️ Payment webhooks are disabled (ENABLE_PAYMENT_WEBHOOKS is not 'true')."); // Reduced log
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
        setTimeout(() => process.exit(1), SHUTDOWN_FAIL_TIMEOUT_MS + 2000); 
    }
}

// --- Run the main application ---
main();

// console.log("End of index.js script. Bot startup process initiated from main()."); // Removed log
// --- End of Part 6 ---
// --- Start of Part P1 ---
// index.js - Part P1: Solana Payment System - Core Utilities & Wallet Generation
//---------------------------------------------------------------------------
// Assumed DEPOSIT_MASTER_SEED_PHRASE, bip39, derivePath, nacl,
// Keypair, PublicKey, LAMPORTS_PER_SOL, SystemProgram, Transaction, sendAndConfirmTransaction,
// ComputeBudgetProgram, TransactionExpiredBlockheightExceededError, SendTransactionError,
// solanaConnection, queryDatabase, pool,
// escapeMarkdownV2, stringifyWithBigInt,
// PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, PAYOUT_COMPUTE_UNIT_LIMIT,
// INITIAL_RETRY_POLLING_DELAY, MAX_RETRY_POLLING_DELAY, DEPOSIT_ADDRESS_EXPIRY_MS,
// RPC_MAX_RETRIES, RPC_COMMITMENT,
// notifyAdmin, sleep, createHash,
// getNextAddressIndexForUserDB,
// activeDepositAddresses cache are available.

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
        console.error("[CreateUserIndex] CRITICAL: createHash (from crypto) is not available. Using insecure fallback. THIS IS NOT PRODUCTION SAFE.");
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
        throw new Error("CRITICAL Dependency missing for deriveSolanaKeypair (bip39, ed25519-hd-key/derivePath, tweetnacl, or @solana/web3.js/Keypair).");
    }
    try {
        const seed = bip39.mnemonicToSeedSync(seedPhrase);
        const derivedSeedForKeypair = derivePath(derivationPath, seed.toString('hex')).key;
        const naclKeypair = nacl.sign.keyPair.fromSeed(derivedSeedForKeypair.slice(0, 32));
        const keypair = Keypair.fromSeed(naclKeypair.secretKey.slice(0, 32));
        return keypair;
    } catch (error) {
        console.error(`[DeriveSolKeypair Path:${derivationPath}] Error: ${error.message}`, error.stack?.substring(0,300)); // Shortened log
        throw new Error(`Keypair derivation failed for path ${derivationPath}: ${error.message}`);
    }
}

/**
 * Generates a new, unique deposit address for a user and stores its record.
 * This function performs a direct DB insert and should be called within a transaction
 * managed by the caller if atomicity with other user updates is required.
 * @param {string|number} userId - The user's Telegram ID.
 * @param {import('pg').PoolClient} [dbClient=pool] - Optional database client.
 * @returns {Promise<string|null>} The public key string of the generated deposit address, or null on failure.
 */
async function generateUniqueDepositAddress(userId, dbClient = pool) {
    const stringUserId = String(userId);
    const LOG_PREFIX_GUDA = `[GenDepositAddr UID:${stringUserId}]`;

    if (!DEPOSIT_MASTER_SEED_PHRASE) {
        console.error(`${LOG_PREFIX_GUDA} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is not set.`);
        if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL: DEPOSIT_MASTER_SEED_PHRASE not set. Deposit address generation failing for user ${stringUserId}.`);
        return null;
    }
    if (typeof getNextAddressIndexForUserDB !== 'function') { 
        console.error(`${LOG_PREFIX_GUDA} CRITICAL: getNextAddressIndexForUserDB function not defined.`);
        return null;
    }

    try {
        const safeUserAccountIndex = createSafeUserSpecificIndex(stringUserId);
        const addressIndex = await getNextAddressIndexForUserDB(stringUserId, dbClient); 

        const derivationPath = `m/44'/501'/${safeUserAccountIndex}'/0'/${addressIndex}'`; 
        
        const depositKeypair = deriveSolanaKeypair(DEPOSIT_MASTER_SEED_PHRASE, derivationPath);
        const depositAddress = depositKeypair.publicKey.toBase58();

        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS);

        const insertQuery = `
            INSERT INTO user_deposit_wallets (user_telegram_id, public_key, derivation_path, expires_at, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, TRUE, NOW(), NOW())
            RETURNING wallet_id, public_key;
        `;
        const result = await queryDatabase(insertQuery, [stringUserId, depositAddress, derivationPath, expiresAt], dbClient);

        if (result.rows.length > 0) {
            console.log(`${LOG_PREFIX_GUDA} ✅ New deposit address: ${depositAddress} (Path: ${derivationPath}, Expires: ${expiresAt.toISOString()})`);
            if (typeof activeDepositAddresses !== 'undefined' && activeDepositAddresses instanceof Map) {
                activeDepositAddresses.set(depositAddress, { userId: stringUserId, expiresAt: expiresAt.getTime() });
            }
            return depositAddress;
        } else {
            console.error(`${LOG_PREFIX_GUDA} ❌ Failed to store generated deposit address ${depositAddress} in DB.`);
            throw new Error("Failed to insert deposit address into database and get ID back.");
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_GUDA} ❌ Error generating unique deposit address for user ${stringUserId}: ${error.message}`, error.stack?.substring(0,500));
        if (error.code === '23505') { 
            console.error(`${LOG_PREFIX_GUDA} Unique constraint violation. Path: ${error.detail?.includes('derivation_path') ? error.detail : 'N/A'}`);
             if (typeof notifyAdmin === 'function') notifyAdmin(`⚠️ Error generating deposit address (Unique Constraint) for user \`${escapeMarkdownV2(stringUserId)}\`: \`${escapeMarkdownV2(error.message)}\`. Possible race condition or index issue.`, {parse_mode:'MarkdownV2'});
        } else if (typeof notifyAdmin === 'function') {
            notifyAdmin(`⚠️ Error generating deposit address for user \`${escapeMarkdownV2(stringUserId)}\`: \`${escapeMarkdownV2(error.message)}\`. Check logs.`, {parse_mode:'MarkdownV2'});
        }
        return null;
    }
}

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
        return false; 
    }
}

/**
 * Gets the SOL balance of a given Solana public key.
 * @param {string} publicKeyString - The public key string.
 * @returns {Promise<bigint|null>} The balance in lamports, or null on error/if address not found.
 */
async function getSolBalance(publicKeyString) {
    const LOG_PREFIX_GSB = `[GetSolBalance PK:${publicKeyString ? publicKeyString.slice(0,10) : 'N/A'}...]`;
    if (!isValidSolanaAddress(publicKeyString)) {
        // console.warn(`${LOG_PREFIX_GSB} Invalid public key provided: ${publicKeyString}`); // Reduced log
        return null;
    }
    try {
        const balance = await solanaConnection.getBalance(new PublicKey(publicKeyString), process.env.RPC_COMMITMENT || 'confirmed');
        return BigInt(balance);
    } catch (error) {
        if (error.message && (error.message.includes("Account does not exist") || error.message.includes("could not find account"))) {
            return 0n; 
        }
        console.error(`${LOG_PREFIX_GSB} Error fetching balance for ${publicKeyString}: ${error.message}`);
        return null; 
    }
}

/**
 * Sends SOL from a payer to a recipient.
 * @param {import('@solana/web3.js').Keypair} payerKeypair - The keypair of the account sending SOL.
 * @param {string} recipientPublicKeyString - The public key string of the recipient.
 * @param {bigint} amountLamports - The amount of SOL to send, in lamports.
 * @param {string} [memoText] - Optional memo text. For production, use @solana/spl-memo.
 * @param {number} [priorityFeeMicroLamportsOverride] - Optional override for priority fee.
 * @param {number} [computeUnitsOverride] - Optional override for compute units.
 * @returns {Promise<{success: boolean, signature?: string, error?: string, errorType?: string, blockTime?: number, feeLamports?: bigint, isRetryable?: boolean}>}
 */
async function sendSol(payerKeypair, recipientPublicKeyString, amountLamports, memoText = null, priorityFeeMicroLamportsOverride = null, computeUnitsOverride = null) {
    const LOG_PREFIX_SENDSOL = `[SendSol From:${payerKeypair.publicKey.toBase58().slice(0,6)} To:${recipientPublicKeyString.slice(0,6)} Amt:${amountLamports}]`;
    
    if (!payerKeypair || typeof payerKeypair.publicKey === 'undefined' || typeof payerKeypair.secretKey === 'undefined') {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid payerKeypair.`);
        return { success: false, error: "Invalid payer keypair.", errorType: "InvalidInputError", isRetryable: false };
    }
    if (!isValidSolanaAddress(recipientPublicKeyString)) {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid recipient public key: ${recipientPublicKeyString}`);
        return { success: false, error: "Invalid recipient address.", errorType: "InvalidInputError", isRetryable: false };
    }
    if (typeof amountLamports !== 'bigint' || amountLamports <= 0n) {
        console.error(`${LOG_PREFIX_SENDSOL} Invalid amount: ${amountLamports}. Must be > 0.`);
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
        // console.log(`${LOG_PREFIX_SENDSOL} Conceptual Memo: "${memoText.trim()}". Use @solana/spl-memo.`); // Reduced log
        // Example: instructions.push(createMemoInstruction(memoText.trim(), [payerKeypair.publicKey]));
    }
    
    transaction.add(...instructions);

    let signature = null;
    let retries = 0;
    const maxRetriesConfig = parseInt(process.env.RPC_MAX_RETRIES, 10); 
    const sendAndConfirmMaxRetries = 3; 
    let retryDelayMs = parseInt(process.env.INITIAL_RETRY_POLLING_DELAY, 10);
    const maxRetryDelayMs = parseInt(process.env.MAX_RETRY_POLLING_DELAY, 10);
    const rpcCommitment = process.env.RPC_COMMITMENT || 'confirmed';

    while (retries < maxRetriesConfig) {
        try {
            // console.log(`${LOG_PREFIX_SENDSOL} Attempt ${retries + 1}/${maxRetriesConfig}: Sending transaction...`); // Reduced log
            
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
            
            console.log(`${LOG_PREFIX_SENDSOL} ✅ TX successful! Sig: ${signature}. Commit: ${rpcCommitment}.`);
            
            let blockTime = null;
            let feeLamports = null;
            try {
                const confirmedTx = await solanaConnection.getTransaction(signature, {commitment: rpcCommitment, maxSupportedTransactionVersion: 0 });
                if (confirmedTx && confirmedTx.blockTime && confirmedTx.meta) {
                    blockTime = confirmedTx.blockTime;
                    feeLamports = BigInt(confirmedTx.meta.fee);
                    // console.log(`${LOG_PREFIX_SENDSOL} Tx details: BlockTime: ${blockTime}, Fee: ${feeLamports}`); // Reduced log
                } else {
                    // console.warn(`${LOG_PREFIX_SENDSOL} Could not fetch full tx details for ${signature}.`); // Reduced log
                }
            } catch (fetchErr) {
                console.warn(`${LOG_PREFIX_SENDSOL} Could not fetch confirmed tx details for ${signature}: ${fetchErr.message}`);
            }
            
            return { success: true, signature, blockTime, feeLamports };

        } catch (error) {
            retries++;
            const errorMessage = error.message || String(error);
            let isRetryableError = false; 
            console.error(`${LOG_PREFIX_SENDSOL} ❌ Attempt ${retries}/${maxRetriesConfig} failed: ${errorMessage}`);
            if (error.stack && retries === maxRetriesConfig) console.error(error.stack.substring(0, 500)); 

            if (error instanceof TransactionExpiredBlockheightExceededError) {
                // console.warn(`${LOG_PREFIX_SENDSOL} Transaction expired. Retrying with new blockhash.`); // Reduced log
                isRetryableError = true;
            } else if (error instanceof SendTransactionError) {
                const transactionLogs = error.logs;
                if (transactionLogs) {
                    // console.error(`${LOG_PREFIX_SENDSOL} Tx logs from SendTransactionError:\n${transactionLogs.join('\n')}`); // Too verbose for regular retry
                    if (transactionLogs.some(log => log.toLowerCase().includes("insufficient lamports"))) {
                        return { success: false, error: "Insufficient SOL to cover transaction fee or amount.", errorType: "InsufficientFundsError", isRetryable: false };
                    }
                    if (transactionLogs.some(log => log.toLowerCase().includes("custom program error") || log.toLowerCase().includes("error processing instruction"))) {
                        return { success: false, error: `Transaction failed: Program error. See logs.`, errorType: "ProgramError", isRetryable: false };
                    }
                }
                isRetryableError = true; 
            } else if (errorMessage.includes("signers") && errorMessage.includes("Transaction was not signed by all")) {
                console.error(`${LOG_PREFIX_SENDSOL} Signing error (code issue).`);
                return {success: false, error: "Transaction signing failed.", errorType: "SigningError", isRetryable: false};
            } else if (errorMessage.toLowerCase().includes("blockhash not found") || errorMessage.toLowerCase().includes("timeout")) {
                isRetryableError = true; 
            }

            if (!isRetryableError || retries >= maxRetriesConfig) {
                console.error(`${LOG_PREFIX_SENDSOL} Max retries or non-retryable error. TX failed permanently.`);
                return { success: false, error: `Transaction failed after ${retries} attempts: ${errorMessage}`, errorType: error.constructor?.name || "UnknownError", isRetryable: false };
            }

            // console.log(`${LOG_PREFIX_SENDSOL} Retrying in ${retryDelayMs / 1000}s...`); // Reduced log
            await sleep(retryDelayMs);
            retryDelayMs = Math.min(retryDelayMs * 2, maxRetryDelayMs); 
        }
    }
    return { success: false, error: "Transaction failed after all attempts.", errorType: "MaxRetriesReached", isRetryable: false };
}

// --- End of Part P1 ---
// --- Start of Part P2 ---
// index.js - Part P2: Payment System Database Operations
//---------------------------------------------------------------------------
// Assumed global `pool`, `queryDatabase`,
// `escapeMarkdownV2`, `formatCurrency`, `stringifyWithBigInt`,
// `generateReferralCode`, `getNextAddressIndexForUserDB`,
// `activeDepositAddresses` cache map, `walletCache` map are available.
// Constants like SOL_DECIMALS, LAMPORTS_PER_SOL are assumed available.

// --- Unified User/Wallet Operations ---

/**
 * Fetches payment-system relevant details for a user.
 * @param {string|number} telegramId The user's Telegram ID.
 * @param {import('pg').PoolClient} [client=pool] Optional database client.
 * @returns {Promise<object|null>} User details with BigInt conversions or null if not found/error.
 */
async function getPaymentSystemUserDetails(telegramId, client = pool) {
    const stringUserId = String(telegramId);
    const LOG_PREFIX_GPSUD = `[GetUserPayDetails TG:${stringUserId}]`; // Shortened
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
            details.telegram_id = String(details.telegram_id); 
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
        // console.warn(`${LOG_PREFIX_GPSUD} User not found.`); // Can be noisy, remove if not essential for normal ops
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_GPSUD} ❌ Error fetching user details: ${err.message}`, err.stack?.substring(0,500));
        return null;
    }
}

/**
 * Finds a user by their referral code.
 * @param {string} refCode The referral code.
 * @param {import('pg').PoolClient} [client=pool] Optional database client.
 * @returns {Promise<{telegram_id: string, username?:string, first_name?:string} | null>} User ID (as string) and basic info or null.
 */
async function getUserByReferralCode(refCode, client = pool) {
    const LOG_PREFIX_GUBRC = `[GetUserByRefCode Code:${refCode}]`;
    if (!refCode || typeof refCode !== 'string' || refCode.trim() === "") {
        // console.warn(`${LOG_PREFIX_GUBRC} Invalid or empty referral code provided.`); // Reduced log
        return null;
    }
    try {
        const result = await queryDatabase('SELECT telegram_id, username, first_name FROM users WHERE referral_code = $1', [refCode.trim()], client);
        if (result.rows.length > 0) {
            const userFound = result.rows[0];
            userFound.telegram_id = String(userFound.telegram_id); 
            return userFound;
        }
        return null;
    } catch (err) {
        console.error(`${LOG_PREFIX_GUBRC} ❌ Error finding user by referral code: ${err.message}`, err.stack?.substring(0,500));
        return null;
    }
}

// --- Unified Balance & Ledger Operations ---

/**
 * Atomically updates a user's balance and records the change in the ledger table.
 * This is the PRIMARY function for all financial transactions affecting user balance.
 * MUST be called within an active DB transaction if part of a larger multi-step operation.
 * The `dbClient` parameter MUST be an active client from `pool.connect()`.
 *
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {string|number} telegramId - The user's Telegram ID.
 * @param {bigint} changeAmountLamports - Positive for credit, negative for debit.
 * @param {string} transactionType - Type for the ledger.
 * @param {object} [relatedIds={}] Optional related IDs.
 * @param {string|null} [notes=null] Optional notes for the ledger entry.
 * @returns {Promise<{success: boolean, newBalanceLamports?: bigint, oldBalanceLamports?: bigint, ledgerId?: number, error?: string, errorCode?: string}>}
 */
async function updateUserBalanceAndLedger(dbClient, telegramId, changeAmountLamports, transactionType, relatedIds = {}, notes = null) {
    const stringUserId = String(telegramId);
    const changeAmount = BigInt(changeAmountLamports);
    const logPrefix = `[UpdateBalLedger UID:${stringUserId} Type:${transactionType} Amt:${changeAmount}]`;

    if (!dbClient || typeof dbClient.query !== 'function') {
        console.error(`${logPrefix} 🚨 CRITICAL: dbClient is not a valid database client.`);
        return { success: false, error: 'Invalid database client provided to updateUserBalanceAndLedger.', errorCode: 'INVALID_DB_CLIENT' };
    }

    const relDepositId = (relatedIds?.deposit_id && Number.isInteger(relatedIds.deposit_id)) ? relatedIds.deposit_id : null;
    const relWithdrawalId = (relatedIds?.withdrawal_id && Number.isInteger(relatedIds.withdrawal_id)) ? relatedIds.withdrawal_id : null;
    const relGameLogId = (relatedIds?.game_log_id && Number.isInteger(relatedIds.game_log_id)) ? relatedIds.game_log_id : null;
    const relReferralId = (relatedIds?.referral_id && Number.isInteger(relatedIds.referral_id)) ? relatedIds.referral_id : null;
    const relSweepId = (relatedIds?.related_sweep_id && Number.isInteger(relatedIds.related_sweep_id)) ? relatedIds.related_sweep_id : null;
    let oldBalanceLamports; 

    try {
        const selectUserSQL = `SELECT balance, total_deposited_lamports, total_withdrawn_lamports, total_wagered_lamports, total_won_lamports FROM users WHERE telegram_id = $1 FOR UPDATE`;
        const balanceRes = await dbClient.query(selectUserSQL, [stringUserId]);
        
        if (balanceRes.rowCount === 0) {
            console.error(`${logPrefix} ❌ User balance record not found for ID ${stringUserId}.`);
            return { success: false, error: 'User profile not found for balance update.', errorCode: 'USER_NOT_FOUND' };
        }
        const userData = balanceRes.rows[0];
        oldBalanceLamports = BigInt(userData.balance); 
        const balanceAfter = oldBalanceLamports + changeAmount;

        // Allow admin grants to make balance negative, but normal operations should not.
        if (balanceAfter < 0n && transactionType !== 'admin_grant' && transactionType !== 'admin_adjustment_debit' && transactionType !== 'admin_grant_debit') { 
            console.warn(`${logPrefix} ⚠️ Insufficient balance. Current: ${oldBalanceLamports}, Change: ${changeAmount}, Would be: ${balanceAfter}.`);
            return { success: false, error: 'Insufficient balance for this transaction.', oldBalanceLamports: oldBalanceLamports, newBalanceLamportsWouldBe: balanceAfter, errorCode: 'INSUFFICIENT_FUNDS' };
        }

        let newTotalDeposited = BigInt(userData.total_deposited_lamports || '0');
        let newTotalWithdrawn = BigInt(userData.total_withdrawn_lamports || '0');
        let newTotalWagered = BigInt(userData.total_wagered_lamports || '0');
        let newTotalWon = BigInt(userData.total_won_lamports || '0'); 

        if (transactionType === 'deposit' && changeAmount > 0n) {
            newTotalDeposited += changeAmount;
        } else if ((transactionType.startsWith('withdrawal_request') || transactionType.startsWith('withdrawal_fee') || transactionType === 'withdrawal_confirmed') && changeAmount < 0n) { 
            newTotalWithdrawn -= changeAmount; 
        } else if (transactionType.startsWith('bet_placed') && changeAmount < 0n) {
            newTotalWagered -= changeAmount; 
        } else if ((transactionType.startsWith('win_') || transactionType.startsWith('jackpot_win_') || transactionType.startsWith('push_')) && changeAmount > 0n) { 
            newTotalWon += changeAmount;
        } else if (transactionType === 'referral_commission_credit' && changeAmount > 0n) { 
           newTotalWon += changeAmount; // Or a new category like total_referral_credits
        }

        const updateUserQuery = `UPDATE users SET balance = $1, total_deposited_lamports = $2, total_withdrawn_lamports = $3, total_wagered_lamports = $4, total_won_lamports = $5, updated_at = NOW() WHERE telegram_id = $6;`;
        const updateUserParams = [
            balanceAfter.toString(), 
            newTotalDeposited.toString(),
            newTotalWithdrawn.toString(),
            newTotalWagered.toString(),
            newTotalWon.toString(),
            stringUserId
        ];
        const updateRes = await dbClient.query(updateUserQuery, updateUserParams);

        if (updateRes.rowCount === 0) {
            console.error(`${logPrefix} ❌ Failed to update user balance row after lock for user ${stringUserId}. This should not happen.`);
            throw new Error('Failed to update user balance row after lock.');
        }

        const ledgerQuery = `INSERT INTO ledger (user_telegram_id, transaction_type, amount_lamports, balance_before_lamports, balance_after_lamports, deposit_id, withdrawal_id, game_log_id, referral_id, related_sweep_id, notes, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW()) RETURNING ledger_id;`;
        const ledgerParams = [
            stringUserId, transactionType, changeAmount.toString(), oldBalanceLamports.toString(), balanceAfter.toString(),
            relDepositId, relWithdrawalId, relGameLogId, relReferralId, relSweepId, notes
        ];
        const ledgerRes = await dbClient.query(ledgerQuery, ledgerParams);
        
        const ledgerId = ledgerRes.rows[0]?.ledger_id;
        console.log(`${logPrefix} ✅ Balance updated: ${oldBalanceLamports} -> ${balanceAfter}. Ledger: ${ledgerId}.`);
        return { success: true, newBalanceLamports: balanceAfter, oldBalanceLamports: oldBalanceLamports, ledgerId };

    } catch (err) {
        console.error(`${logPrefix} ❌ Error in updateUserBalanceAndLedger: ${err.message} (Code: ${err.code || 'N/A'})`, err.stack?.substring(0,500));
        // console.error(`${logPrefix} [DEBUG_PARAMS_FAILURE] Called with: telegramId=${telegramId}, changeAmountLamports=${changeAmountLamports}, transactionType=${transactionType}, relatedIds=${JSON.stringify(relatedIds)}, notes=${notes}`); // Optionally re-enable for deep debug
        let errMsg = `Database error during balance/ledger update (Code: ${err.code || 'N/A'})`;
        if (err.message && err.message.toLowerCase().includes('violates check constraint') && err.message.toLowerCase().includes('balance')) {
            errMsg = 'Insufficient balance (check constraint violation).';
        }
        return { success: false, error: errMsg, errorCode: err.code, oldBalanceLamports };
    }
}

// --- Deposit Address & Deposit Operations ---

/**
 * Finds user ID and other details for a given deposit address. Checks cache first.
 * @param {string} depositAddress The deposit address (public key).
 * @returns {Promise<{userId: string, walletId: number, expiresAt: Date, derivationPath: string, isActive:boolean } | null>}
 */
async function findDepositAddressInfoDB(depositAddress) {
    const LOG_PREFIX_FDAI = `[FindDepositAddr Addr:${depositAddress ? depositAddress.slice(0,6) : 'N/A'}...]`; // Shortened
    if (!depositAddress) {
        // console.warn(`${LOG_PREFIX_FDAI} Called with null or undefined depositAddress.`); // Reduced log
        return null;
    }

    if (typeof activeDepositAddresses !== 'undefined' && activeDepositAddresses instanceof Map) {
        const cached = activeDepositAddresses.get(depositAddress);
        if (cached && Date.now() < cached.expiresAt) {
            // Cache hit is useful, but we often need full details from DB.
        }
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
        console.error(`${LOG_PREFIX_FDAI} ❌ Error finding deposit address info: ${err.message}`, err.stack?.substring(0,500));
        return null;
    }
}

/**
 * Marks a deposit address as inactive and optionally as swept.
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {number} userDepositWalletId - The ID of the `user_deposit_wallets` record.
 * @param {boolean} [swept=false] - If true, also sets swept_at.
 * @param {bigint|null} [balanceAtSweep=null] - Optional balance at time of sweep.
 * @returns {Promise<boolean>} True if updated successfully.
 */
async function markDepositAddressInactiveDB(dbClient, userDepositWalletId, swept = false, balanceAtSweep = null) {
    const LOG_PREFIX_MDAI = `[MarkDepositAddrInactive WID:${userDepositWalletId} Swept:${swept}]`; // Shortened
    try {
        let query = 'UPDATE user_deposit_wallets SET is_active = FALSE, updated_at = NOW()';
        const params = [];
        let paramIndex = 1;

        if (swept) {
            query += `, swept_at = NOW()`;
            if (balanceAtSweep !== null && typeof balanceAtSweep === 'bigint') {
                query += `, balance_at_sweep = $${paramIndex++}`;
                params.push(balanceAtSweep.toString());
            } else if (balanceAtSweep === null && swept) { 
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
            }
            // console.log(`${LOG_PREFIX_MDAI} ✅ Marked wallet ID ${userDepositWalletId} (Addr: ${updatedWallet.public_key.slice(0,6)}) as inactive/swept. Active: ${updatedWallet.is_active}`); // Reduced log
            return true;
        }
        console.warn(`${LOG_PREFIX_MDAI} ⚠️ Wallet ID ${userDepositWalletId} not found or no change made.`);
        return false;
    } catch (err) {
        console.error(`${LOG_PREFIX_MDAI} ❌ Error marking deposit address inactive: ${err.message}`, err.stack?.substring(0,500));
        return false;
    }
}

/**
 * Records a confirmed deposit transaction. Must be called within a transaction using dbClient.
 * @param {import('pg').PoolClient} dbClient
 * @param {string|number} userId
 * @param {number} userDepositWalletId
 * @param {string} depositAddress
 * @param {string} txSignature
 * @param {bigint} amountLamports
 * @param {string|null} [sourceAddress=null]
 * @param {number|null} [blockTime=null]
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
            // console.log(`${LOG_PREFIX_RCD} ✅ Deposit recorded successfully. DB ID: ${res.rows[0].deposit_id}`); // Reduced log
            return { success: true, depositId: res.rows[0].deposit_id };
        }
        const existing = await dbClient.query('SELECT deposit_id FROM deposits WHERE transaction_signature = $1', [txSignature]);
        if (existing.rowCount > 0) {
            console.warn(`${LOG_PREFIX_RCD} ⚠️ Deposit TX ${txSignature} already processed (DB ID: ${existing.rows[0].deposit_id}).`);
            return { success: false, error: 'Deposit already processed.', alreadyProcessed: true, depositId: existing.rows[0].deposit_id };
        }
        console.error(`${LOG_PREFIX_RCD} ❌ Failed to record deposit (not duplicate) for TX ${txSignature}.`);
        return { success: false, error: 'Failed to record deposit (unknown issue after conflict check).' };
    } catch(err) {
        console.error(`${LOG_PREFIX_RCD} ❌ Error recording deposit: ${err.message} (Code: ${err.code})`, err.stack?.substring(0,500));
        return { success: false, error: err.message, errorCode: err.code };
    }
}

// --- Sweep Operations ---
/**
 * Records a successful sweep transaction. Must be called within a transaction using dbClient.
 * @param {import('pg').PoolClient} dbClient
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
    `; 
    try {
        const res = await dbClient.query(query, [sourceDepositAddress, destinationMainAddress, amountLamports.toString(), transactionSignature]);
        if (res.rowCount > 0 && res.rows[0].sweep_id) {
            // console.log(`${LOG_PREFIX_RST} ✅ Sweep transaction recorded. DB ID: ${res.rows[0].sweep_id}`); // Reduced log
            return { success: true, sweepId: res.rows[0].sweep_id };
        }
        console.error(`${LOG_PREFIX_RST} ❌ Failed to record sweep transaction or get ID back for TX ${transactionSignature}.`);
        return { success: false, error: 'Failed to record sweep transaction or retrieve ID.' };
    } catch (err) {
        console.error(`${LOG_PREFIX_RST} ❌ Error recording sweep TX: ${err.message} (Code: ${err.code})`, err.stack?.substring(0,500));
        return { success: false, error: err.message, errorCode: err.code };
    }
}

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
            // console.log(`${LOG_PREFIX_CWR} ✅ Withdrawal request created. DB ID: ${res.rows[0].withdrawal_id}`); // Reduced log
            return { success: true, withdrawalId: res.rows[0].withdrawal_id };
        }
        throw new Error("Withdrawal request creation failed to return ID.");
    } catch (err) {
        console.error(`${LOG_PREFIX_CWR} ❌ Error creating withdrawal request: ${err.message}`, err.stack?.substring(0,500));
        return { success: false, error: err.message, errorCode: err.code };
    }
}

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
            // console.log(`${LOG_PREFIX_UWS} ✅ Withdrawal status updated successfully.`); // Reduced log
            return { success: true, withdrawalId: res.rows[0].withdrawal_id };
        }
        console.warn(`${LOG_PREFIX_UWS} ⚠️ Withdrawal ID ${withdrawalId} not found or status not updated.`);
        return { success: false, error: "Withdrawal record not found or no update made." };
    } catch (err) {
        console.error(`${LOG_PREFIX_UWS} ❌ Error updating withdrawal status: ${err.message}`, err.stack?.substring(0,500));
        return { success: false, error: err.message, errorCode: err.code };
    }
}

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
        console.error(`${LOG_PREFIX_GWD} ❌ Error fetching withdrawal details: ${err.message}`, err.stack?.substring(0,500));
        return null;
    }
}

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
        // console.warn(`${LOG_PREFIX_RRCE} No eligible 'pending_criteria' referral found or already processed.`); // Can be noisy
        return { success: false, error: "No eligible pending referral found or already processed." };
    } catch (err) {
        console.error(`${LOG_PREFIX_RRCE} ❌ Error recording referral commission earned: ${err.message}`, err.stack?.substring(0,500));
        return { success: false, error: err.message, errorCode: err.code };
    }
}

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
        WHERE referral_id = $4 AND status != 'paid_out' 
        RETURNING referral_id;
    `;
    try {
        const res = await dbClient.query(query, [status, transactionSignature, errorMessage, referralId]);
        if (res.rowCount > 0) {
            // console.log(`${LOG_PREFIX_URPS} ✅ Referral payout status updated.`); // Reduced log
            return { success: true };
        }
        // console.warn(`${LOG_PREFIX_URPS} Referral ID ${referralId} not found or already paid out/no status change needed.`); // Can be noisy
        return { success: false, error: "Referral not found or no update made." };
    } catch (err) {
        console.error(`${LOG_PREFIX_URPS} ❌ Error updating referral payout status: ${err.message}`, err.stack?.substring(0,500));
        return { success: false, error: err.message, errorCode: err.code };
    }
}

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
        console.error(`${LOG_PREFIX_GRD} ❌ Error fetching referral details: ${err.message}`, err.stack?.substring(0,500));
        return null;
    }
}

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
        console.error(`${LOG_PREFIX_GTRE} ❌ Error fetching total referral earnings: ${err.message}`, err.stack?.substring(0,500));
        return { total_earned_paid_lamports: 0n, total_pending_payout_lamports: 0n };
    }
}

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
            SELECT l.ledger_id, l.transaction_type, l.amount_lamports, l.balance_after_lamports, l.notes, l.created_at,
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
        console.error(`${LOG_PREFIX_GBH} ❌ Error fetching ledger history: ${err.message}`, err.stack?.substring(0,500));
        return [];
    }
}

async function getLeaderboardDataDB(type = 'total_wagered', period = 'all_time', limit = 10) {
    const LOG_PREFIX_GLD = `[GetLeaderboard Type:${type} Period:${period}]`;
    // console.log(`${LOG_PREFIX_GLD} Fetching leaderboard data...`); // Reduced log
    let orderByField = 'total_wagered_lamports'; 
    if (type === 'total_won') {
        orderByField = 'total_won_lamports';
    } else if (type === 'net_profit') {
        orderByField = '(total_won_lamports - total_wagered_lamports)'; // Actual calculation for net profit
        // console.warn(`${LOG_PREFIX_GLD} 'net_profit' leaderboard type selected.`); // Informative, can be kept or removed
    }

    if (period !== 'all_time') {
        console.warn(`${LOG_PREFIX_GLD} Period '${period}' not yet implemented. Defaulting to 'all_time'.`);
        // Future: Add date range filtering to the WHERE clause based on `period`.
    }

    const query = `
        SELECT telegram_id, username, first_name, ${orderByField} AS stat_value_ordered
        FROM users
        WHERE is_banned = FALSE
        ORDER BY stat_value_ordered DESC, updated_at DESC
        LIMIT $1;
    `;
    try {
        const res = await queryDatabase(query, [limit]);
        return res.rows.map(row => ({
            telegram_id: String(row.telegram_id),
            username: row.username,
            first_name: row.first_name,
            stat_value: BigInt(row.stat_value_ordered) 
        }));
    } catch (err) {
        console.error(`${LOG_PREFIX_GLD} ❌ Error fetching leaderboard data: ${err.message}`, err.stack?.substring(0,500));
        return [];
    }
}

// --- Dice Roll Request Database Operations ---

/**
 * Inserts a new dice roll request into the database for the Helper Bot to process.
 * MUST be called within an active DB transaction if atomicity with other operations is required.
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {string} gameId - Identifier for the game requesting the roll.
 * @param {string|number} chatId - The chat ID where the dice should be sent.
 * @param {string|number} [userId=null] - The user ID associated with this roll, if applicable.
 * @param {string} [emojiType='🎲'] - The emoji type for bot.sendDice.
 * @param {string|null} [notes=null] - Optional notes for the request.
 * @returns {Promise<{success: boolean, requestId?: number, error?: string}>}
 */
async function insertDiceRollRequest(dbClient, gameId, chatId, userId = null, emojiType = '🎲', notes = null) {
    const stringChatId = String(chatId);
    const stringUserId = userId ? String(userId) : null;
    const logPrefix = `[InsertDiceReq GID:${gameId} UID:${stringUserId || 'Bot'}]`; // Clarified Bot roll

    if (!dbClient || typeof dbClient.query !== 'function') {
        console.error(`${logPrefix} 🚨 CRITICAL: dbClient is not a valid database client.`);
        return { success: false, error: 'Invalid database client for insertDiceRollRequest.' };
    }
    const query = `
        INSERT INTO dice_roll_requests (game_id, chat_id, user_id, emoji_type, status, notes, requested_at)
        VALUES ($1, $2, $3, $4, 'pending', $5, NOW())
        RETURNING request_id;
    `;
    try {
        const params = [gameId, stringChatId, stringUserId, emojiType, notes];
        const res = await dbClient.query(query, params);
        if (res.rows.length > 0 && res.rows[0].request_id) {
            // console.log(`${logPrefix} ✅ Dice roll request created. DB ID: ${res.rows[0].request_id}`); // Can be noisy, removed for now
            return { success: true, requestId: res.rows[0].request_id };
        }
        throw new Error("Dice roll request creation failed to return ID.");
    } catch (err) {
        console.error(`${logPrefix} ❌ Error creating dice roll request: ${err.message}`, err.stack?.substring(0,500));
        return { success: false, error: err.message, errorCode: err.code };
    }
}

/**
 * Retrieves the status and result of a specific dice roll request.
 * @param {import('pg').PoolClient} dbClient - The active database client.
 * @param {number} requestId - The ID of the dice_roll_requests record.
 * @returns {Promise<{success: boolean, status?: string, roll_value?: number, notes?: string, error?: string}>}
 */
async function getDiceRollRequestResult(dbClient, requestId) {
    const logPrefix = `[GetDiceReqResult RID:${requestId}]`;

    if (!dbClient || typeof dbClient.query !== 'function') {
        console.error(`${logPrefix} 🚨 CRITICAL: dbClient is not a valid database client.`);
        return { success: false, error: 'Invalid database client for getDiceRollRequestResult.' };
    }
    const query = `
        SELECT status, roll_value, notes
        FROM dice_roll_requests
        WHERE request_id = $1;
    `;
    try {
        const res = await dbClient.query(query, [requestId]);
        if (res.rows.length > 0) {
            const data = res.rows[0];
            // console.log(`${logPrefix} ✅ Fetched status: ${data.status}, value: ${data.roll_value}`); // Reduced log for polling
            return {
                success: true,
                status: data.status,
                roll_value: data.roll_value, 
                notes: data.notes
            };
        }
        // console.warn(`${logPrefix} ⚠️ Dice roll request ID ${requestId} not found.`); // Can be noisy during initial polling
        return { success: false, error: 'Request ID not found.' };
    } catch (err) {
        console.error(`${logPrefix} ❌ Error fetching dice roll request result: ${err.message}`, err.stack?.substring(0,500));
        return { success: false, error: err.message, errorCode: err.code };
    }
}

// --- End of Part P2 ---
// --- Start of Part P3 ---
// index.js - Part P3: Payment System UI Handlers, Stateful Logic & Webhook Setup
//---------------------------------------------------------------------------
// Assumed global utilities, constants, DB ops, Solana utils, cache utils,
// and processing queues from previous parts are available.

// --- User State Management ---
function clearUserState(userId) {
    const stringUserId = String(userId);
    const state = userStateCache.get(stringUserId);
    if (state) {
        if (state.data?.timeoutId) clearTimeout(state.data.timeoutId);
        userStateCache.delete(stringUserId);
    }
}

async function routeStatefulInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text || '';
    const stateName = currentState.state || currentState.action;
    const logPrefix = `[StatefulInput UID:${userId} State:${stateName}]`;

    if (currentState.chatId && String(currentState.chatId) !== chatId) {
        console.warn(`${logPrefix} Stateful input in wrong chat (${chatId}) vs expected (${currentState.chatId})\\.`);
        await safeSendMessage(chatId, "Please respond to my previous question in our direct message chat\\. 💬", {parse_mode: 'MarkdownV2'});
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
            console.warn(`${logPrefix} Unknown or unhandled state: ${stateName}\\. Clearing state\\.`);
            clearUserState(userId);
            await safeSendMessage(chatId, "Your previous action seems to have expired or was unclear\\. Please try again using a command from the main menu\\. 🤔", { parse_mode: 'MarkdownV2' });
    }
}

async function handleWalletAddressInput(msg, currentState) {
    const userId = String(msg.from.id);
    const dmChatId = String(msg.chat.id);
    const potentialNewAddress = msg.text ? msg.text.trim() : '';
    const logPrefix = `[WalletAddrInput UID:${userId}]`;

    if (!currentState || !currentState.data || currentState.state !== 'awaiting_withdrawal_address' || dmChatId !== userId) {
        console.error(`${logPrefix} Invalid state or context for wallet address input\\. State ChatID: ${currentState?.chatId}, Msg ChatID: ${dmChatId}, State: ${currentState?.state}`);
        clearUserState(userId);
        await safeSendMessage(dmChatId, "⚙️ There was an issue processing your address input\\. Please try linking your wallet again via the \`/wallet\` menu or \`/setwallet\` command\\.", { parse_mode: 'MarkdownV2' });
        return;
    }

    const { originalPromptMessageId, originalGroupChatId, originalGroupMessageId } = currentState.data;
    if (originalPromptMessageId && bot) { await bot.deleteMessage(dmChatId, originalPromptMessageId).catch(() => {}); }
    clearUserState(userId);

    const linkingMsgText = `🔗 Validating and attempting to link wallet: \`${escapeMarkdownV2(potentialNewAddress)}\`\\.\\.\\. Please hold on a moment\\.`; // Escaped period and ellipsis

    const linkingMsg = await safeSendMessage(dmChatId, linkingMsgText, { parse_mode: 'MarkdownV2' });
    const displayMsgIdInDm = linkingMsg ? linkingMsg.message_id : null;

    try {
        if (!isValidSolanaAddress(potentialNewAddress)) {
            throw new Error("The provided address has an invalid Solana address format\\. Please double\\-check and try again\\.");
        }

        const linkResult = await linkUserWallet(userId, potentialNewAddress); // Ensure linkUserWallet returns MarkdownV2 safe messages
        let feedbackText;
        const finalKeyboard = { inline_keyboard: [[{ text: '💳 Back to Wallet Menu', callback_data: 'menu:wallet' }]] };

        if (linkResult.success) {
            // Assuming linkUserWallet's message is already MarkdownV2 safe (e.g., "linked\\!")
            feedbackText = linkResult.message || `✅ Success\\! Wallet \`${escapeMarkdownV2(potentialNewAddress)}\` has been successfully linked to your account\\.`;
            if (originalGroupChatId && originalGroupMessageId && bot) {
                const userForGroupMsg = await getOrCreateUser(userId); // Get fresh details
                await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || msg.from)} has successfully updated their linked wallet\\.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(()=>{});
            }
        } else {
            feedbackText = `⚠️ Wallet Link Failed for \`${escapeMarkdownV2(potentialNewAddress)}\`\\.\n*Reason:* ${escapeMarkdownV2(linkResult.error || "Please ensure the address is valid and not already in use\\.")}`;
            if (originalGroupChatId && originalGroupMessageId && bot) {
                const userForGroupMsg = await getOrCreateUser(userId);
                await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || msg.from)}, there was an issue linking your wallet\\. Please check my DM for details and try again\\.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(()=>{});
            }
        }

        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(feedbackText, { chat_id: dmChatId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        } else {
            await safeSendMessage(dmChatId, feedbackText, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        }
    } catch (e) {
        console.error(`${logPrefix} Error linking wallet ${potentialNewAddress}: ${e.message}`);
        const errorTextToDisplay = `⚠️ Error with wallet address: \`${escapeMarkdownV2(potentialNewAddress)}\`\\.\n*Details:* ${escapeMarkdownV2(String(e.message || "An unexpected error occurred\\."))}\nPlease ensure it's a valid Solana public key and try again\\.`;
        const errorKeyboard = { inline_keyboard: [[{ text: '💳 Try Again (Wallet Menu)', callback_data: 'menu:wallet' }]] };
        
        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(errorTextToDisplay, { chat_id: dmChatId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        } else {
            await safeSendMessage(dmChatId, errorTextToDisplay, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        }
        if (originalGroupChatId && originalGroupMessageId && bot) {
            const userForGroupMsg = await getOrCreateUser(userId);
            await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || msg.from)}, there was an error processing your wallet address\\. Please check my DM\\.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(()=>{});
        }
    }
}

async function handleWithdrawalAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const dmChatId = String(msg.chat.id);
    const textAmount = msg.text ? msg.text.trim() : '';
    const logPrefix = `[WithdrawAmountInput UID:${userId}]`;

    if (!currentState || !currentState.data || currentState.state !== 'awaiting_withdrawal_amount' || dmChatId !== userId ||
        !currentState.data.linkedWallet || typeof currentState.data.currentBalanceLamportsStr !== 'string') {
        console.error(`${logPrefix} Invalid state or data for withdrawal amount. State: ${stringifyWithBigInt(currentState).substring(0,300)}`);
        clearUserState(userId);
        await safeSendMessage(dmChatId, "⚙️ Error: Withdrawal context lost or invalid\\. Please restart the withdrawal process from the \`/wallet\` menu\\.", { parse_mode: 'MarkdownV2' });
        return;
    }

    const { linkedWallet, originalPromptMessageId, currentBalanceLamportsStr, originalGroupChatId, originalGroupMessageId } = currentState.data;
    const currentBalanceLamports = BigInt(currentBalanceLamportsStr);
    if (originalPromptMessageId && bot) { await bot.deleteMessage(dmChatId, originalPromptMessageId).catch(() => {}); }
    clearUserState(userId);

    try {
        let amountSOL;
        if (textAmount.toLowerCase() === 'max') {
            const availableToWithdraw = currentBalanceLamports - WITHDRAWAL_FEE_LAMPORTS;
            if (availableToWithdraw < MIN_WITHDRAWAL_LAMPORTS) {
                 throw new Error(`Your balance is too low to withdraw the maximum after fees\\. You need at least *${escapeMarkdownV2(await formatBalanceForDisplay(MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS, 'SOL'))}* total to cover minimum withdrawal and fee\\.`);
            }
            amountSOL = parseFloat( (Number(availableToWithdraw) / Number(LAMPORTS_PER_SOL)).toFixed(SOL_DECIMALS) );
        } else if (textAmount.toLowerCase().endsWith('sol')) {
            amountSOL = parseFloat(textAmount.toLowerCase().replace('sol', '').trim());
        } else {
            amountSOL = parseFloat(String(textAmount).replace(/[^0-9.]/g, ''));
        }

        if (isNaN(amountSOL) || amountSOL <= 0) throw new Error("Invalid number format or non\\-positive amount\\. Please enter a value like \`0.5\` or \`10\` or \`0.1 sol\`\\.");

        const amountLamports = BigInt(Math.floor(amountSOL * Number(LAMPORTS_PER_SOL)));
        const feeLamports = WITHDRAWAL_FEE_LAMPORTS;
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
                                 `⚠️ Double\\-check the recipient address\\! Transactions are irreversible\\. Proceed?`; // Escaped !

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
                const userForGroupMsg = await getOrCreateUser(userId);
                await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || {id: userId, first_name: "Player"})}, please check your DMs to confirm your withdrawal request\\.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
            }
        } else {
            throw new Error("Failed to send withdrawal confirmation message\\. Please try again\\.");
        }
    } catch (e) {
        console.error(`${logPrefix} Error processing withdrawal amount: ${e.message}`);
        const errorText = `⚠️ *Withdrawal Error:*\n${escapeMarkdownV2(e.message)}\n\nPlease restart the withdrawal process from the \`/wallet\` menu\\.`;
        await safeSendMessage(dmChatId, errorText, {
            parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]] }
        });
        if (originalGroupChatId && originalGroupMessageId && bot) {
            const userForGroupMsg = await getOrCreateUser(userId);
            await bot.editMessageText(`${getPlayerDisplayReference(userForGroupMsg || {id: userId, first_name: "Player"})}, there was an error with your withdrawal amount\\. Please check my DM\\.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        }
    }
}


// --- UI Command Handler Implementations ---

// Fully REVISED handleWalletCommand
async function handleWalletCommand(receivedMsgObject) {
    const isFromMenuAction = receivedMsgObject && receivedMsgObject.originalChatInfo !== undefined;

    const actualFromObject = receivedMsgObject.from;
    const actualChatObject = receivedMsgObject.chat;

    let userIdFromInput;
    if (actualFromObject && actualFromObject.telegram_id) { // If it's our DB user object from actionMsgContext.from
        userIdFromInput = String(actualFromObject.telegram_id);
    } else if (actualFromObject && actualFromObject.id) { // If it's a Telegram `from` object from a direct msg
        userIdFromInput = String(actualFromObject.id);
    } else {
        const tempChatIdError = actualChatObject?.id || ADMIN_USER_ID || 'unknown_chat';
        console.error(`[WalletCmd] CRITICAL: Could not determine userId from receivedMsgObject.from: ${JSON.stringify(actualFromObject)}`);
        await safeSendMessage(tempChatIdError, "An internal error occurred (User ID missing for Wallet)\\. Please try `/start`\\.", { parse_mode: 'MarkdownV2' });
        return;
    }

    const userId = userIdFromInput;
    const commandChatId = String(actualChatObject.id); // The chat where /wallet or originating callback happened

    let userObject = await getOrCreateUser(userId, actualFromObject?.username, actualFromObject?.first_name, actualFromObject?.last_name);
    if (!userObject) {
        const tempPlayerRef = getPlayerDisplayReference(actualFromObject);
        const errorMessage = `Error fetching your player profile, ${tempPlayerRef}\\. Please try \`/start\` again\\.`;
        const errorChatTarget = (commandChatId === userId) ? commandChatId : userId;
        await safeSendMessage(errorChatTarget, errorMessage, { parse_mode: 'MarkdownV2' });
        if (commandChatId !== userId) {
             await safeSendMessage(commandChatId, `${tempPlayerRef}, there was an error accessing your wallet\\. Please check DMs or try \`/start\`\\.`, {parse_mode: 'MarkdownV2'});
        }
        return;
    }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId);

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) { /* Reduced log */ }

    const targetDmChatId = userId;
    let messageIdToEditOrDeleteInDm = null;

    if (commandChatId !== targetDmChatId && !isFromMenuAction) {
        if (receivedMsgObject.message_id) await bot.deleteMessage(commandChatId, receivedMsgObject.message_id).catch(() => {});
        await safeSendMessage(commandChatId, `${playerRef}, I've sent your Wallet Dashboard to our private chat: @${escapeMarkdownV2(botUsername)} 💳 For your security, all wallet actions are handled there\\.`, { parse_mode: 'MarkdownV2' });
    } else if (commandChatId === targetDmChatId && receivedMsgObject.message_id) {
        messageIdToEditOrDeleteInDm = receivedMsgObject.message_id;
        if (!isFromMenuAction) { // /wallet typed in DM
             await bot.deleteMessage(targetDmChatId, messageIdToEditOrDeleteInDm).catch(()=>{});
             messageIdToEditOrDeleteInDm = null;
        }
    }
    
    const loadingDmMsgText = "Loading your Wallet Dashboard... ⏳"; // Plain text
    let workingMessageId = messageIdToEditOrDeleteInDm;

    if (workingMessageId) {
        try {
            await bot.editMessageText(loadingDmMsgText, { chat_id: targetDmChatId, message_id: workingMessageId, reply_markup: {inline_keyboard: []} }); // No parse_mode
        } catch (editError) {
            if (!editError.message?.includes("message is not modified")) {
                const tempMsg = await safeSendMessage(targetDmChatId, loadingDmMsgText);
                workingMessageId = tempMsg?.message_id;
            }
        }
    } else {
        const tempMsg = await safeSendMessage(targetDmChatId, loadingDmMsgText);
        workingMessageId = tempMsg?.message_id;
    }
    
    if (!workingMessageId) {
        console.error(`[WalletCmd UID:${userId}] Failed to establish message context (workingMessageId) for wallet display in DM.`);
        return;
    }
    messageIdToEditOrDeleteInDm = workingMessageId;
    
    try {
        const userDetails = await getPaymentSystemUserDetails(userId); 
        if (!userDetails) {
            const noUserText = "😕 Could not retrieve your player profile\\. Please try sending \`/start\` to the bot first\\.";
            await bot.editMessageText(noUserText, {chat_id: targetDmChatId, message_id: messageIdToEditOrDeleteInDm, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "Go to /start", callback_data:"menu:main"}]]}});
            return;
        }

        const balanceLamports = BigInt(userDetails.balance || '0');
        const linkedAddress = userDetails.solana_wallet_address;
        const balanceDisplayUSD_raw = await formatBalanceForDisplay(balanceLamports, 'USD'); 
        const balanceDisplaySOL_raw = await formatBalanceForDisplay(balanceLamports, 'SOL'); 
        const linkedAddress_display_raw = linkedAddress ? linkedAddress : "_Not Set_";

        let text = `⚜️ **${escapeMarkdownV2(BOT_NAME)} Wallet Dashboard** ⚜️\n\n` +
                   `👤 Player: ${playerRef}\n\n` +
                   `💰 Current Balance:\n   Approx\\. *${escapeMarkdownV2(balanceDisplayUSD_raw)}*\n   SOL: *${escapeMarkdownV2(balanceDisplaySOL_raw)}*\n\n` +
                   `🔗 Linked Withdrawal Address:\n   \`${escapeMarkdownV2(linkedAddress_display_raw)}\`\n\n`;
        
        if (!linkedAddress) {
            text += `💡 You can link a wallet using the button below or by typing \`/setwallet YOUR_ADDRESS\` in this chat\\.\n\n`; // Escaped period
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

        await bot.editMessageText(text, { chat_id: targetDmChatId, message_id: messageIdToEditOrDeleteInDm, parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });

    } catch (error) {
        console.error(`[WalletCmd UID:${userId}] ❌ Error displaying wallet: ${error.message}`, error.stack?.substring(0,500));
        const errorTextForUser = `⚙️ Apologies, we encountered an issue displaying your wallet information\\. (${escapeMarkdownV2(error.message)})\\. You can try \`/start\`\\.`;
        await bot.editMessageText(errorTextForUser, {
            chat_id: targetDmChatId, 
            message_id: messageIdToEditOrDeleteInDm, 
            parse_mode: 'MarkdownV2', 
            reply_markup: {inline_keyboard: [[{text: "Try /start", callback_data:"menu:main"}]]}
        }).catch(async (editFallbackError) => {
            console.warn(`[WalletCmd UID:${userId}] Failed to edit error message, sending new. Edit fallback error: ${editFallbackError.message}`);
            await safeSendMessage(targetDmChatId, errorTextForUser, {parse_mode: 'MarkdownV2'}); 
        });
    }
}

// REVISED handleSetWalletCommand
async function handleSetWalletCommand(msg, args) {
    const userId = String(msg.from.id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;

    let userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) { 
        await safeSendMessage(commandChatId, "Error fetching your player profile\\. Please try \`/start\`\\.", {parse_mode: 'MarkdownV2'});
        return; 
    }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId);

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) { /* Reduced log */ }

    if (chatType !== 'private') {
        if(msg.message_id && commandChatId !== userId) await bot.deleteMessage(commandChatId, msg.message_id).catch(() => {});
        const dmPrompt = `${playerRef}, for your security, please set your wallet address by sending the command \`/setwallet YOUR_ADDRESS\` directly to me in our private chat: @${escapeMarkdownV2(botUsername)} 💳`;
        await safeSendMessage(commandChatId, dmPrompt, { parse_mode: 'MarkdownV2' });
        await safeSendMessage(userId, `Hi ${playerRef}, to set or update your withdrawal wallet, please reply here with the command: \`/setwallet YOUR_SOLANA_ADDRESS\`\nExample: \`/setwallet YourSoLaddressHere\\.\\.\\.\``, {parse_mode: 'MarkdownV2'}); // Escaped ellipsis
        return;
    }

    if (args.length < 1 || !args[0].trim()) {
        await safeSendMessage(userId, `💡 To link your Solana wallet for withdrawals, please use the format: \`/setwallet YOUR_SOLANA_ADDRESS\`\nExample: \`/setwallet SoLmaNqerT3ZpPT1qS9j2kKx2o5x94s2f8u5aA3bCgD\``, { parse_mode: 'MarkdownV2' });
        return;
    }
    const potentialNewAddress = args[0].trim();

    if(msg.message_id) await bot.deleteMessage(userId, msg.message_id).catch(() => {});

    const linkingMsgText = `🔗 Validating and attempting to link wallet: \`${escapeMarkdownV2(potentialNewAddress)}\`\\.\\.\\. Please hold on\\.`; // Escaped punctuation
    const linkingMsg = await safeSendMessage(userId, linkingMsgText, { parse_mode: 'MarkdownV2' });
    const displayMsgIdInDm = linkingMsg ? linkingMsg.message_id : null;

    try {
        if (!isValidSolanaAddress(potentialNewAddress)) {
            throw new Error("The provided address has an invalid Solana address format\\.");
        }
        const linkResult = await linkUserWallet(userId, potentialNewAddress); // Ensure linkUserWallet returns MarkdownV2 safe messages
        let feedbackText;
        const finalKeyboard = { inline_keyboard: [[{ text: '💳 Back to Wallet Menu', callback_data: 'menu:wallet' }]] };

        if (linkResult.success) {
            // linkUserWallet messages should already be safe (e.g. "linked\\!")
            feedbackText = linkResult.message || `✅ Success\\! Wallet \`${escapeMarkdownV2(potentialNewAddress)}\` is now linked\\.`;
        } else {
            feedbackText = `⚠️ Wallet Link Failed for \`${escapeMarkdownV2(potentialNewAddress)}\`\\.\n*Reason:* ${escapeMarkdownV2(linkResult.error || "Please check the address and try again\\.")}`;
        }

        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(feedbackText, { chat_id: userId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        } else {
            await safeSendMessage(userId, feedbackText, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        }
    } catch (e) {
        console.error(`[SetWalletCmd UID:${userId}] Error linking wallet ${potentialNewAddress}: ${e.message}`);
        const errorTextToDisplay = `⚠️ Error with wallet address: \`${escapeMarkdownV2(potentialNewAddress)}\`\\.\n*Details:* ${escapeMarkdownV2(e.message || "An unexpected error occurred\\.")}\nPlease ensure it's a valid Solana public key\\.`;
        const errorKeyboard = { inline_keyboard: [[{ text: '💳 Try Again (Wallet Menu)', callback_data: 'menu:wallet' }]] };
        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(errorTextToDisplay, { chat_id: userId, message_id: displayMsgIdInDm, parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        } else {
            await safeSendMessage(userId, errorTextToDisplay, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        }
    }
}


// REVISED handleDepositCommand
async function handleDepositCommand(msgOrCbMsg, args = [], correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const targetDmChatId = userId;
    const originalCommandChatId = String(msgOrCbMsg.chat.id);
    const originalMessageId = msgOrCbMsg.message_id;
    const isCallbackRedirect = msgOrCbMsg.isCallbackRedirect || false;

    const logPrefix = `[DepositCmd UID:${userId} OrigChat:${originalCommandChatId}]`;

    let userObject = await getOrCreateUser(userId, msgOrCbMsg.from?.username, msgOrCbMsg.from?.first_name, msgOrCbMsg.from?.last_name);
    if (!userObject) {
        await safeSendMessage(targetDmChatId, "Error fetching your player profile\\. Please try \`/start\` again\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId);

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if (selfInfo.username) botUsername = selfInfo.username; } catch (e) { /* Reduced log */ }

    if (originalCommandChatId !== targetDmChatId && !isCallbackRedirect) {
        if (originalMessageId) await bot.deleteMessage(originalCommandChatId, originalMessageId).catch(() => {});
        await safeSendMessage(originalCommandChatId, `${playerRef}, for your security and convenience, I've sent your unique deposit address to our private chat: @${escapeMarkdownV2(botUsername)} 📬 Please check your DMs\\.`, { parse_mode: 'MarkdownV2' });
    }
    
    const generatingText = "⏳ Generating your deposit address... Please wait..."; // Plain text
    let workingMessageId;

    if (originalCommandChatId === targetDmChatId && originalMessageId && !isCallbackRedirect && !msgOrCbMsg.isCallbackRedirect) { // Command typed in DM
        await bot.deleteMessage(targetDmChatId, originalMessageId).catch(()=>{});
        const tempMsg = await safeSendMessage(targetDmChatId, generatingText);
        workingMessageId = tempMsg?.message_id;
    } else if (isCallbackRedirect || (originalCommandChatId === targetDmChatId && (msgOrCbMsg.isCallbackRedirect !== undefined || !!correctUserIdFromCb) )) { 
        // Callback redirected to DM, or callback was already in DM from a menu
        if (originalMessageId && originalCommandChatId === targetDmChatId) { // If callback was in DM, edit that message
            try {
                await bot.editMessageText(generatingText, { chat_id: targetDmChatId, message_id: originalMessageId, reply_markup: { inline_keyboard: [] } });
                workingMessageId = originalMessageId;
            } catch (editError) {
                if (!editError.message?.includes("message is not modified")) {
                    const tempMsg = await safeSendMessage(targetDmChatId, generatingText);
                    workingMessageId = tempMsg?.message_id;
                } else {
                    workingMessageId = originalMessageId;
                }
            }
        } else { // Was a group callback, just send new to DM
            const tempMsg = await safeSendMessage(targetDmChatId, generatingText);
            workingMessageId = tempMsg?.message_id;
        }
    } else { 
         const tempMsg = await safeSendMessage(targetDmChatId, generatingText);
         workingMessageId = tempMsg?.message_id;
    }

    if (!workingMessageId) {
        console.error(`${logPrefix} Failed to establish message context (workingMessageId) for deposit address display in DM.`);
        return;
    }

    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const confirmationLevelEscaped = escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL || 'confirmed');
        const existingAddressesRes = await client.query(
            "SELECT public_key, expires_at FROM user_deposit_wallets WHERE user_telegram_id = $1 AND is_active = TRUE AND expires_at > NOW() ORDER BY created_at DESC LIMIT 1",
            [userId]
        );

        let depositAddress;
        let expiresAtDate;
        let newAddressGenerated = false;

        if (existingAddressesRes.rows.length > 0) {
            depositAddress = existingAddressesRes.rows[0].public_key;
            expiresAtDate = new Date(existingAddressesRes.rows[0].expires_at);
        } else {
            const newAddressStr = await generateUniqueDepositAddress(userId, client);
            if (!newAddressStr) { throw new Error("Failed to generate a new deposit address\\. Please try again or contact support\\."); }
            depositAddress = newAddressStr;
            newAddressGenerated = true;
            const newAddrDetails = await client.query("SELECT expires_at FROM user_deposit_wallets WHERE public_key = $1 AND user_telegram_id = $2", [depositAddress, userId]);
            expiresAtDate = newAddrDetails.rows.length > 0 ? new Date(newAddrDetails.rows[0].expires_at) : new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS);
        }

        if (newAddressGenerated || (userObject.last_deposit_address !== depositAddress)) {
            await client.query(
                `UPDATE users SET last_deposit_address = $1, last_deposit_address_generated_at = $2, updated_at = NOW() WHERE telegram_id = $3`,
                [depositAddress, expiresAtDate, userId]
            );
        }
        await client.query('COMMIT');

        const timeRemainingMs = expiresAtDate.getTime() - Date.now();
        const timeRemainingMinutes = Math.max(1, Math.ceil(timeRemainingMs / (60 * 1000)));
        const expiryDateTimeString = expiresAtDate.toLocaleString('en-GB', { hour: '2-digit', minute: '2-digit', day: '2-digit', month: 'short', timeZone: 'UTC' }) + " UTC";

        const escapedAddress = escapeMarkdownV2(depositAddress);
        const timeRemainingMinutesEscaped = escapeMarkdownV2(String(timeRemainingMinutes));
        const expiryDateTimeStringEscaped = escapeMarkdownV2(expiryDateTimeString);

        const message = `💰 *Your ${newAddressGenerated ? 'New' : 'Active'} Deposit Address*\n\n` +
                        `Hi ${playerRef}, please send SOL to your unique deposit address below:\n\n` +
                        `\`${escapedAddress}\`\n` +
                        `_\\(Tap the address above to copy\\)_\\n\n` +
                        `This address is valid for approximately *${timeRemainingMinutesEscaped} minutes* \\(expires around ${expiryDateTimeStringEscaped}\\)\\. __Do not use after expiry\\.__\n\n` +
                        `Funds require *${confirmationLevelEscaped}* network confirmations to be credited\\.\n\n` +
                        `⚠️ *Important Information:*\n` +
                        `* Send only SOL to this address\\. Do not send NFTs or other tokens\\.\n` +
                        `* Exchange deposits may take longer to confirm\\.\n` +
                        `* Address is unique to you for this deposit session\\. Do not share it\\.\n` +
                        `* To generate a new address later, please use the \`/deposit\` command or "Deposit SOL" option in your \`/wallet\` menu\\.`;

        const solanaPayUrl = `solana:${depositAddress}?label=${encodeURIComponent(BOT_NAME + " Deposit")}&message=${encodeURIComponent("Casino Deposit for " + playerRef)}`;
        const qrCodeUrl = `https://api.qrserver.com/v1/create-qr-code/?size=250x250&data=${encodeURIComponent(solanaPayUrl)}`;

        const depositKeyboard = [
            [{ text: "🔍 View on Solscan", url: `https://solscan.io/account/${depositAddress}` }],
            [{ text: "📱 Scan QR Code", url: qrCodeUrl }],
            [{ text: "💳 Back to Wallet", callback_data: "menu:wallet" }]
        ];
        const options = { parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: depositKeyboard}, disable_web_page_preview: true };

        await bot.editMessageText(message, {chat_id: targetDmChatId, message_id: workingMessageId, ...options}).catch(async (e) => {
            if (e.message && (e.message.toLowerCase().includes("can't parse entities") || e.message.toLowerCase().includes("bad request"))) {
                console.error(`❌ ${logPrefix} PARSE ERROR displaying deposit address! Attempting plain text. Original error: ${e.message}`);
                const plainMessage = `Your ${newAddressGenerated ? 'New' : 'Active'} Deposit Address (Tap to copy):\n${depositAddress}\n\nExpires in approx. ${timeRemainingMinutes} minutes (around ${expiryDateTimeString}). Confirmations: ${DEPOSIT_CONFIRMATION_LEVEL || 'confirmed'}.\nImportant: Valid for this session only. Do not use after expiry. Use /deposit for new address. Send SOL only.`;
                const plainKeyboard = {inline_keyboard: [[{ text: 'Back to Wallet', callback_data: 'menu:wallet' }]]};
                await safeSendMessage(targetDmChatId, plainMessage, { reply_markup: plainKeyboard, disable_web_page_preview: true });
                if (workingMessageId) await bot.deleteMessage(targetDmChatId, workingMessageId).catch(()=>{});
            } else if (!e.message?.includes("message is not modified")) {
                console.warn(`${logPrefix} Failed to edit message ${workingMessageId} with deposit address, sending new. Error: ${e.message}`);
                await safeSendMessage(targetDmChatId, message, options);
                if (workingMessageId) await bot.deleteMessage(targetDmChatId, workingMessageId).catch(()=>{});
            }
        });

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error: ${rbErr.message}`));
        console.error(`${logPrefix} ❌ Error handling deposit command: ${error.message}`, error.stack?.substring(0, 500));
        const errorText = `⚙️ Apologies, ${playerRef}, we couldn't generate a deposit address for you at this moment: \`${escapeMarkdownV2(error.message)}\`\\. Please try again shortly or contact support\\.`;
        const errorKeyboardButtons = [[{ text: "Try Again", callback_data: DEPOSIT_CALLBACK_ACTION }, { text: "💳 Wallet", callback_data: "menu:wallet" }]];
        const errorOptions = { parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: errorKeyboardButtons} };
        if (workingMessageId) {
            await bot.editMessageText(errorText, { chat_id: targetDmChatId, message_id: workingMessageId, ...errorOptions })
                .catch(async () => { await safeSendMessage(targetDmChatId, errorText, errorOptions); });
        } else {
            await safeSendMessage(targetDmChatId, errorText, errorOptions);
        }
    } finally {
        if (client) client.release();
    }
}

// Newly ADDED and REVISED handleWithdrawCommand
async function handleWithdrawCommand(msgOrCbMsg, args = [], correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const targetDmChatId = userId; // Withdraw actions are handled in DM
    const originalCommandChatId = String(msgOrCbMsg.chat.id);
    const originalMessageId = msgOrCbMsg.message_id;
    const isFromMenuActionOrRedirect = msgOrCbMsg.isCallbackRedirect !== undefined || !!correctUserIdFromCb;

    const logPrefix = `[WithdrawCmd UID:${userId} OrigChat:${originalCommandChatId}]`;

    let userObject = await getOrCreateUser(userId, msgOrCbMsg.from?.username, msgOrCbMsg.from?.first_name, msgOrCbMsg.from?.last_name);
    if (!userObject) {
        await safeSendMessage(targetDmChatId, "Error fetching your player profile for withdrawal\\. Please try \`/start\` again\\.", { parse_mode: 'MarkdownV2' });
        return;
    }
    const playerRef = getPlayerDisplayReference(userObject);
    clearUserState(userId);

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if (selfInfo.username) botUsername = selfInfo.username; } catch (e) { /* Reduced log */ }

    if (originalCommandChatId !== targetDmChatId && !isFromMenuActionOrRedirect && !msgOrCbMsg.isCallbackRedirect) {
        if (originalMessageId) await bot.deleteMessage(originalCommandChatId, originalMessageId).catch(() => {});
        await safeSendMessage(originalCommandChatId, `${playerRef}, for your security, withdrawal requests are handled in our private chat: @${escapeMarkdownV2(botUsername)} 📬 Please check your DMs\\.`, { parse_mode: 'MarkdownV2' });
    }
    
    if (originalCommandChatId === targetDmChatId && originalMessageId && (isFromMenuActionOrRedirect || !msgOrCbMsg.isCallbackRedirect) ) {
        // If it's a callback from a menu in DM, or a command typed in DM, delete the initiating message
        await bot.deleteMessage(targetDmChatId, originalMessageId).catch(()=>{});
    }

    const loadingDmMsgText = "Preparing your withdrawal request... ⏳";
    const loadingDmMsg = await safeSendMessage(targetDmChatId, loadingDmMsgText);
    const workingMessageId = loadingDmMsg?.message_id;

    if (!workingMessageId) {
        console.error(`${logPrefix} Failed to send 'Preparing withdrawal' message to DM.`);
        return;
    }

    try {
        const linkedWallet = await getUserLinkedWallet(userId);
        const currentBalanceLamports = await getUserBalance(userId);

        if (currentBalanceLamports === null) {
            throw new Error("Could not retrieve your current balance\\. Please try again shortly\\.");
        }

        if (!linkedWallet) {
            const noWalletText = `⚠️ ${playerRef}, you don't have a Solana withdrawal wallet linked to your account yet\\!\n\n` +
                                 `Please link a wallet first using the button below, or by typing \`/setwallet YOUR_SOL_ADDRESS\` in this chat\\.`;
            const noWalletKeyboard = { inline_keyboard: [
                [{ text: "🔗 Link Withdrawal Wallet Now", callback_data: "menu:link_wallet_prompt" }],
                [{ text: "💳 Back to Wallet Menu", callback_data: "menu:wallet" }]
            ]};
            await bot.editMessageText(noWalletText, { chat_id: targetDmChatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: noWalletKeyboard });
            return;
        }

        const minWithdrawalDisplay = await formatBalanceForDisplay(MIN_WITHDRAWAL_LAMPORTS, 'SOL');
        const feeDisplay = await formatBalanceForDisplay(WITHDRAWAL_FEE_LAMPORTS, 'SOL');
        const currentBalanceDisplay = await formatBalanceForDisplay(currentBalanceLamports, 'SOL');

        const promptText = `💸 *Initiate Withdrawal*\n\n` +
                           `Player: ${playerRef}\n` +
                           `Linked Wallet: \`${escapeMarkdownV2(linkedWallet)}\`\n` +
                           `Available Balance: *${escapeMarkdownV2(currentBalanceDisplay)}*\n\n` +
                           `Minimum Withdrawal: *${escapeMarkdownV2(minWithdrawalDisplay)}*\n` +
                           `Withdrawal Fee: *${escapeMarkdownV2(feeDisplay)}* \\(deducted from withdrawal amount\\)\n\n` + // Escaped ()
                           `Please reply with the amount of SOL you wish to withdraw \\(e\\.g\\., \`0.5\` or \`10 sol\` or type \`max\` to withdraw your maximum available balance after fees\\)\\.`; // Escaped ()

        const promptKeyboard = { inline_keyboard: [[{ text: "❌ Cancel & Back to Wallet", callback_data: "menu:wallet" }]] };
        await bot.editMessageText(promptText, { chat_id: targetDmChatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: promptKeyboard, disable_web_page_preview: true });

        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_amount',
            chatId: targetDmChatId,
            messageId: workingMessageId,
            data: {
                linkedWallet: linkedWallet,
                currentBalanceLamportsStr: currentBalanceLamports.toString(),
                originalPromptMessageId: workingMessageId, 
                originalGroupChatId: (originalCommandChatId !== targetDmChatId && !isFromMenuActionOrRedirect && !msgOrCbMsg.isCallbackRedirect) ? originalCommandChatId : (msgOrCbMsg.isCallbackRedirect ? msgOrCbMsg.originalChatInfo?.id : null),
                originalGroupMessageId: (originalCommandChatId !== targetDmChatId && !isFromMenuActionOrRedirect && !msgOrCbMsg.isCallbackRedirect) ? null : (msgOrCbMsg.isCallbackRedirect ? msgOrCbMsg.originalChatInfo?.messageId : null)
            },
            timestamp: Date.now()
        });

    } catch (error) {
        console.error(`${logPrefix} Error preparing withdrawal: ${error.message}`, error.stack?.substring(0,500));
        const errorText = `⚙️ Apologies, ${playerRef}, an error occurred while preparing your withdrawal request: \`${escapeMarkdownV2(error.message)}\`\\. Please try again from the wallet menu\\.`;
        const errorKeyboard = { inline_keyboard: [[{ text: "💳 Back to Wallet Menu", callback_data: "menu:wallet" }]] };
        if (workingMessageId) {
            await bot.editMessageText(errorText, { chat_id: targetDmChatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: errorKeyboard })
                .catch(async () => {
                    await safeSendMessage(targetDmChatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
                });
        } else {
            await safeSendMessage(targetDmChatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: errorKeyboard });
        }
    }
}


async function handleReferralCommand(msgOrCbMsg) {
    const userId = String(msgOrCbMsg.from.id);
    const commandChatId = String(msgOrCbMsg.chat.id);
    const originalMessageId = msgOrCbMsg.message_id;
    const isFromMenuAction = msgOrCbMsg.isCallbackRedirect !== undefined || !!(correctUserIdFromCb && correctUserIdFromCb === userId); // A way to check if it's from menu

    let user = await getOrCreateUser(userId, msgOrCbMsg.from?.username, msgOrCbMsg.from?.first_name, msgOrCbMsg.from?.last_name);
    if (!user) {
        await safeSendMessage(commandChatId === userId ? userId : commandChatId, "Error fetching your profile for referral info\\. Please try \`/start\`\\.", {parse_mode: 'MarkdownV2'});
        return;
    }
    const playerRef = getPlayerDisplayReference(user);
    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) { /* Reduced log */ }

    clearUserState(userId);
    const targetDmChatId = userId;

    if (commandChatId !== targetDmChatId) {
        if (originalMessageId) await bot.deleteMessage(commandChatId, originalMessageId).catch(() => {});
        await safeSendMessage(commandChatId, `${playerRef}, I've sent your referral details and earnings to our private chat: @${escapeMarkdownV2(botUsername)} 🤝`, { parse_mode: 'MarkdownV2' });
    }
    
    if (commandChatId === targetDmChatId && originalMessageId && (isFromMenuAction || !msgOrCbMsg.isCallbackRedirect) ) { // If it's from menu in DM OR typed command in DM
        await bot.deleteMessage(targetDmChatId, originalMessageId).catch(()=>{});
    }

    let referralCode = user.referral_code;
    if (!referralCode) {
        referralCode = generateReferralCode();
        try {
            await queryDatabase("UPDATE users SET referral_code = $1 WHERE telegram_id = $2", [referralCode, userId]);
            user.referral_code = referralCode;
        } catch (dbErr) {
            console.error(`[ReferralCmd] Failed to save new referral code for user ${userId}: ${dbErr.message}`);
            referralCode = "ErrorGenerating";
        }
    }
    const referralLink = `https://t.me/${botUsername}?start=ref_${referralCode}`;

    let messageText = `🤝 *Your Referral Zone, ${playerRef}\\!*\n\n` +
                      `Invite friends to ${escapeMarkdownV2(BOT_NAME)} and earn rewards\\!\n\n` +
                      `🔗 Your Unique Referral Link:\n\`${escapeMarkdownV2(referralLink)}\`\n` +
                      `_\\(Tap to copy or share\\)_\\n\n` +
                      `Share this link with friends\\. When they join using your link and meet criteria \\(e\\.g\\., make a deposit or play games\\), you could earn commissions\\! Details of the current referral program can be found on our official channel/group\\.`;

    const earnings = await getTotalReferralEarningsDB(userId);
    const totalEarnedPaidDisplay = await formatBalanceForDisplay(earnings.total_earned_paid_lamports, 'USD');
    const pendingPayoutDisplay = await formatBalanceForDisplay(earnings.total_pending_payout_lamports, 'USD');

    messageText += `\n\n*Your Referral Stats:*\n` +
                   `▫️ Total Earned & Paid Out: *${escapeMarkdownV2(totalEarnedPaidDisplay)}*\n` +
                   `▫️ Commissions Earned \\(Pending Payout\\): *${escapeMarkdownV2(pendingPayoutDisplay)}*\n\n` +
                   `_\\(Payouts are processed periodically to your linked wallet once they meet a minimum threshold or per program rules\\)_`;

    const keyboard = {inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]};
    await safeSendMessage(targetDmChatId, messageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
}

async function handleHistoryCommand(msgOrCbMsg) {
    const userId = String(msgOrCbMsg.from.id);
    const commandChatId = String(msgOrCbMsg.chat.id);
    const originalMessageId = msgOrCbMsg.message_id;
    const isFromMenuAction = msgOrCbMsg.isCallbackRedirect !== undefined || !!(correctUserIdFromCb && correctUserIdFromCb === userId);

    let user = await getOrCreateUser(userId, msgOrCbMsg.from?.username, msgOrCbMsg.from?.first_name, msgOrCbMsg.from?.last_name);
    if (!user) {
        await safeSendMessage(commandChatId === userId ? userId : commandChatId, "Error fetching your profile for history\\. Please try \`/start\`\\.", {parse_mode: 'MarkdownV2'});
        return;
    }
    const playerRef = getPlayerDisplayReference(user);
    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) { /* Reduced log */ }

    clearUserState(userId);
    const targetDmChatId = userId;

    if (commandChatId !== targetDmChatId) {
        if (originalMessageId) await bot.deleteMessage(commandChatId, originalMessageId).catch(()=>{});
        await safeSendMessage(commandChatId, `${playerRef}, your transaction history has been sent to our private chat: @${escapeMarkdownV2(botUsername)} 📜`, { parse_mode: 'MarkdownV2' });
    }

    let workingMessageId = null;
    if (commandChatId === targetDmChatId && originalMessageId && (isFromMenuAction || !msgOrCbMsg.isCallbackRedirect)) { // From a menu in DM OR typed command in DM
        await bot.deleteMessage(targetDmChatId, originalMessageId).catch(()=>{});
    }
    
    const loadingDmMsgText = "Fetching your transaction history... ⏳"; // Plain text
    const tempMsg = await safeSendMessage(targetDmChatId, loadingDmMsgText);
    workingMessageId = tempMsg?.message_id;

    if (!workingMessageId) {
        console.error(`[HistoryCmd UID:${userId}] Failed to establish message context for history display.`);
        return;
    }

    try {
        const historyEntries = await getBetHistoryDB(userId, 15);
        let historyText = `📜 *Your Recent Casino Activity, ${playerRef}:*\n\n`;

        if (historyEntries.length === 0) {
            historyText += "You have no recorded transactions yet\\. Time to make some moves\\!";
        } else {
            for (const entry of historyEntries) {
                const date = new Date(entry.created_at).toLocaleString('en-GB', { day: '2-digit', month: 'short', year: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
                const amountDisplay = await formatBalanceForDisplay(entry.amount_lamports, 'SOL');
                const typeDisplay = escapeMarkdownV2(entry.transaction_type.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
                const sign = BigInt(entry.amount_lamports) >= 0n ? '+' : '';
                const txSig = entry.deposit_tx || entry.withdrawal_tx;

                historyText += `🗓️ \`${escapeMarkdownV2(date)}\` \\| ${typeDisplay}\n` +
                               `   Amount: *${sign}${escapeMarkdownV2(amountDisplay)}*\n`;
                if (txSig) {
                    historyText += `   Tx: \`${escapeMarkdownV2(txSig.substring(0, 10))}...${escapeMarkdownV2(txSig.substring(txSig.length - 4))}\`\n`;
                }
                if (entry.game_log_type) {
                    historyText += `   Game: ${escapeMarkdownV2(entry.game_log_type)} ${entry.game_log_outcome ? `\\(${escapeMarkdownV2(entry.game_log_outcome)}\\)` : ''}\n`;
                }
                if (entry.notes) {
                    historyText += `   Notes: _${escapeMarkdownV2(entry.notes.substring(0,50))}${entry.notes.length > 50 ? '\\.\\.\\.':''}_\n`; // Escaped ellipsis
                }
                historyText += `   Balance After: *${escapeMarkdownV2(await formatBalanceForDisplay(entry.balance_after_lamports, 'USD'))}*\n\n`;
            }
        }
        historyText += `\n_Displaying up to 15 most recent transactions\\._`; // Escaped period
        const keyboard = {inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]]};

        await bot.editMessageText(historyText, {chat_id: targetDmChatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview:true});

    } catch (error) {
        console.error(`[HistoryCmd UID:${userId}] Error fetching history: ${error.message}`);
        const errText = "⚙️ Sorry, we couldn't fetch your transaction history right now\\. Please try again later\\."; // Escaped period
        await bot.editMessageText(errText, {chat_id: targetDmChatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"Try /start", callback_data:"menu:main"}]]}});
    }
}

// REVISED handleMenuAction with DEBUG logs (essential for Issue 1 diagnosis)
async function handleMenuAction(userId, originalChatId, originalMessageId, menuType, params = [], isFromCallback = true, originalChatType = 'private') {
    // --- BEGIN DEBUG LOGS for handleMenuAction ---
    console.log(`[DEBUG handleMenuAction ENTER] RAW userId param: ${userId} (type: ${typeof userId})`);
    const stringUserId = String(userId);
    console.log(`[DEBUG handleMenuAction AFTER String()] stringUserId: ${stringUserId} (type: ${typeof stringUserId})`);
    // --- END DEBUG LOGS ---

    const logPrefix = `[MenuAction UID:${stringUserId} Type:${menuType}]`;

    if (stringUserId === "undefined" || stringUserId === "") {
        console.error(`[DEBUG handleMenuAction] stringUserId is problematic before calling getOrCreateUser: '${stringUserId}'`);
    }
    console.log(`[DEBUG handleMenuAction] About to CALL getOrCreateUser with stringUserId: '${stringUserId}' (type: ${typeof stringUserId})`);
    // This call is crucial for Issue 1 diagnosis.
    // It now only passes the ID. getOrCreateUser defaults other params if it's creating a new user.
    // If user exists, it fetches them. The userObject should have telegram_id, username, first_name etc.
    let userObject = await getOrCreateUser(stringUserId);

    if(!userObject) {
        // This log is critical if "Error fetching profile" appears
        console.error(`${logPrefix} Could not fetch user profile for menu action (userObject is null after getOrCreateUser). CHECK getOrCreateUser logs for DB errors for ID: ${stringUserId}`);
        await safeSendMessage(originalChatId, "Error fetching your player profile\\. Please try \`/start\` again\\.", {parse_mode:'MarkdownV2'});
        return;
    }

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) { /* Reduced log */ }

    let targetChatIdForAction = stringUserId;
    let messageToEdit = isFromCallback ? originalMessageId : null;
    let isGroupActionRedirect = false;
    const sensitiveMenuTypes = ['deposit', 'quick_deposit', 'withdraw', 'history', 'link_wallet_prompt', 'referral'];

    if ((originalChatType === 'group' || originalChatType === 'supergroup') && sensitiveMenuTypes.includes(menuType)) {
        isGroupActionRedirect = true;
        if (originalMessageId && bot) {
            const playerRefForRedirect = getPlayerDisplayReference(userObject); // Use fetched userObject
            const redirectText = `${playerRefForRedirect}, for your privacy, please continue this action in our direct message\\. I've sent you a prompt there: @${escapeMarkdownV2(botUsername)}`;
            const callbackParamsForUrl = params && params.length > 0 ? `_${params.join('_')}` : '';
            await bot.editMessageText(redirectText, {
                chat_id: originalChatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                reply_markup: { inline_keyboard: [[{text: `📬 Open DM with @${escapeMarkdownV2(botUsername)}`, url: `https://t.me/${botUsername}?start=menu_${menuType}${callbackParamsForUrl}`}]] }
            }).catch(e => { /* Reduced log for "message not modified" */ });
        }
         messageToEdit = null; // Action will be a new message in DM
    } else if (originalChatType === 'private') {
        targetChatIdForAction = originalChatId;
    }

    // actionMsgContext.from now correctly contains the userObject from DB
    const actionMsgContext = {
        from: userObject,
        chat: { id: targetChatIdForAction, type: 'private' },
        message_id: messageToEdit,
        isCallbackRedirect: isGroupActionRedirect,
        originalChatInfo: isGroupActionRedirect ? { id: originalChatId, type: originalChatType, messageId: originalMessageId } : null
    };
    
    const actionsSendingNewMessagesInDm = ['deposit', 'quick_deposit', 'withdraw', 'referral', 'history', 'link_wallet_prompt', 'main'];
    if (targetChatIdForAction === stringUserId && messageToEdit && actionsSendingNewMessagesInDm.includes(menuType)) {
        await bot.deleteMessage(targetChatIdForAction, messageToEdit).catch(()=>{});
        actionMsgContext.message_id = null;
    }

    switch(menuType) {
        case 'wallet':
            await handleWalletCommand(actionMsgContext); // handleWalletCommand now expects this context
            break;
        case 'deposit': case 'quick_deposit':
            await handleDepositCommand(actionMsgContext, [], stringUserId);
            break;
        case 'withdraw':
            await handleWithdrawCommand(actionMsgContext, [], stringUserId); // CALLS THE NEWLY ADDED FUNCTION
            break;
        case 'referral':
            await handleReferralCommand(actionMsgContext);
            break;
        case 'history':
            await handleHistoryCommand(actionMsgContext);
            break;
        case 'leaderboards':
            const leaderboardsContext = isGroupActionRedirect ?
                {...actionMsgContext, chat: {id: stringUserId, type: 'private'}, message_id: null } :
                {...actionMsgContext, chat: {id: originalChatId, type: originalChatType}, message_id: originalMessageId};
            await handleLeaderboardsCommand(leaderboardsContext, params);
            break;
        case 'link_wallet_prompt':
            clearUserState(stringUserId);
            if (actionMsgContext.message_id && targetChatIdForAction === stringUserId) {
                await bot.deleteMessage(targetChatIdForAction, actionMsgContext.message_id).catch(()=>{});
            }
            const promptText = `🔗 *Link/Update Your Withdrawal Wallet*\n\nPlease reply to this message with your personal Solana wallet address where you'd like to receive withdrawals\\. Ensure it's correct as transactions are irreversible\\.\n\nExample: \`SoLmaNqerT3ZpPT1qS9j2kKx2o5x94s2f8u5aA3bCgD\``;
            const kbd = { inline_keyboard: [ [{ text: '❌ Cancel & Back to Wallet', callback_data: 'menu:wallet' }] ] };
            const sentDmPrompt = await safeSendMessage(stringUserId, promptText, { parse_mode: 'MarkdownV2', reply_markup: kbd });

            if (sentDmPrompt?.message_id) {
                userStateCache.set(stringUserId, {
                    state: 'awaiting_withdrawal_address', chatId: stringUserId, messageId: sentDmPrompt.message_id,
                    data: {
                        originalPromptMessageId: sentDmPrompt.message_id,
                        originalGroupChatId: isGroupActionRedirect ? originalChatId : null,
                        originalGroupMessageId: isGroupActionRedirect ? originalMessageId : null
                    },
                    timestamp: Date.now()
                });
            } else {
                await safeSendMessage(stringUserId, "Failed to send the wallet address prompt\\. Please try again from the Wallet menu\\.", {parse_mode: 'MarkdownV2'});
            }
            break;
        case 'main':
            await handleHelpCommand(actionMsgContext);
            break;
        default:
            console.warn(`${logPrefix} Unrecognized menu type: ${menuType}`);
            await safeSendMessage(stringUserId, `❓ Unrecognized menu option: \`${escapeMarkdownV2(menuType)}\`\\. Please try again or use \`/help\`\\.`, {parse_mode:'MarkdownV2'});
    }
}


async function handleWithdrawalConfirmation(userId, dmChatId, confirmationMessageIdInDm, recipientAddress, amountLamportsStr) {
    const stringUserId = String(userId);
    const logPrefix = `[WithdrawConfirm UID:${stringUserId}]`;
    const currentState = userStateCache.get(stringUserId);

    const amountLamports = BigInt(amountLamportsStr);
    const feeLamports = WITHDRAWAL_FEE_LAMPORTS;
    const totalDeduction = amountLamports + feeLamports;
    const userObjForNotif = await getOrCreateUser(stringUserId); 
    const playerRef = getPlayerDisplayReference(userObjForNotif || { id: stringUserId, first_name: "Player" });
    let client = null;

    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const userDetailsCheck = await client.query('SELECT balance FROM users WHERE telegram_id = $1 FOR UPDATE', [stringUserId]);
        if (userDetailsCheck.rowCount === 0) {
            throw new Error("User profile not found during withdrawal confirmation\\.");
        }
        const currentBalanceOnConfirm = BigInt(userDetailsCheck.rows[0].balance);
        if (currentBalanceOnConfirm < totalDeduction) {
            throw new Error(`Insufficient balance at time of confirmation\\. Current: ${escapeMarkdownV2(formatCurrency(currentBalanceOnConfirm, 'SOL'))}, Needed: ${escapeMarkdownV2(formatCurrency(totalDeduction, 'SOL'))}\\. Withdrawal cancelled\\.`);
        }

        const wdReq = await createWithdrawalRequestDB(client, stringUserId, amountLamports, feeLamports, recipientAddress);
        if (!wdReq.success || !wdReq.withdrawalId) {
            throw new Error(wdReq.error || "Failed to create database withdrawal request record\\.");
        }

        const balUpdate = await updateUserBalanceAndLedger(
            client, stringUserId, BigInt(-totalDeduction),
            'withdrawal_request_confirmed',
            { withdrawal_id: wdReq.withdrawalId },
            `Withdrawal confirmed to ${recipientAddress.slice(0,6)}...${recipientAddress.slice(-4)}`
        );
        if (!balUpdate.success) {
            throw new Error(balUpdate.error || "Failed to deduct balance for withdrawal\\. Withdrawal not queued\\.");
        }

        await client.query('COMMIT');

        if (typeof addPayoutJob === 'function') {
            await addPayoutJob({ type: 'payout_withdrawal', withdrawalId: wdReq.withdrawalId, userId: stringUserId });
            const successMsgDm = `✅ *Withdrawal Queued\\!* Your request to withdraw *${escapeMarkdownV2(formatCurrency(amountLamports, 'SOL'))}* to \`${escapeMarkdownV2(recipientAddress)}\` is now in the payout queue\\. You'll be notified by DM once it's processed\\.`;
            if (confirmationMessageIdInDm && bot) {
                await bot.editMessageText(successMsgDm, {chat_id: dmChatId, message_id: confirmationMessageIdInDm, parse_mode:'MarkdownV2', reply_markup:{}});
            } else {
                await safeSendMessage(dmChatId, successMsgDm, {parse_mode:'MarkdownV2'});
            }
            
            if (currentState?.data?.originalGroupChatId && currentState?.data?.originalGroupMessageId && bot) {
                 await bot.editMessageText(`${playerRef}'s withdrawal request for *${escapeMarkdownV2(formatCurrency(amountLamports, 'SOL'))}* has been queued successfully\\. Details in DM\\.`, {chat_id: currentState.data.originalGroupChatId, message_id: currentState.data.originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
            }
        } else {
            console.error(`${logPrefix} 🚨 CRITICAL: addPayoutJob function is not defined! Cannot queue withdrawal ${wdReq.withdrawalId}.`);
            await notifyAdmin(`🚨 CRITICAL: Withdrawal ${wdReq.withdrawalId} for user ${stringUserId} had balance deducted BUT FAILED TO QUEUE for payout (addPayoutJob missing)\\. MANUAL INTERVENTION REQUIRED TO REFUND OR PROCESS\\.`, {parse_mode:'MarkdownV2'});
            throw new Error("Payout processing system is unavailable\\. Your funds were deducted but the payout could not be queued\\. Please contact support immediately\\.");
        }
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error: ${rbErr.message}`));
        console.error(`${logPrefix} ❌ Error processing withdrawal confirmation: ${e.message}`, e.stack?.substring(0,500));
        const errorMsgDm = `⚠️ *Withdrawal Failed:*\n${escapeMarkdownV2(e.message)}\n\nPlease try again or contact support if the issue persists\\.`;
        const errorKeyboard = { inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]] };
        if(confirmationMessageIdInDm && bot) {
            await bot.editMessageText(errorMsgDm, {chat_id: dmChatId, message_id: confirmationMessageIdInDm, parse_mode:'MarkdownV2', reply_markup: errorKeyboard}).catch(async ()=>{
                 await safeSendMessage(dmChatId, errorMsgDm, {parse_mode:'MarkdownV2', reply_markup: errorKeyboard});
            });
        } else {
            await safeSendMessage(dmChatId, errorMsgDm, {parse_mode:'MarkdownV2', reply_markup: errorKeyboard});
        }
        
        if (currentState?.data?.originalGroupChatId && currentState?.data?.originalGroupMessageId && bot) {
            await bot.editMessageText(`${playerRef}, there was an error processing your withdrawal confirmation\\. Please check your DMs\\.`, {chat_id: currentState.data.originalGroupChatId, message_id: currentState.data.originalGroupMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{});
        }
    } finally {
        if (client) client.release();
    }
}


// --- Webhook Setup Function ---
function setupPaymentWebhook(expressAppInstance) {
    const logPrefix = '[SetupWebhook]';
    if (!expressAppInstance) {
        console.error(`${logPrefix} 🚨 Express app instance not provided. Cannot set up webhook routes.`);
        return;
    }

    const paymentWebhookPath = process.env.PAYMENT_WEBHOOK_PATH || '/webhook/solana-payments';
    const PAYMENT_WEBHOOK_SECRET = process.env.PAYMENT_WEBHOOK_SECRET;

    console.log(`${logPrefix} 📡 Configuring webhook endpoint at ${paymentWebhookPath}`);

    expressAppInstance.post(paymentWebhookPath, async (req, res) => {
        const webhookLogPrefix = `[PaymentWebhook ${paymentWebhookPath}]`;
        const signatureFromHeader = req.headers['x-signature'] || req.headers['X-Signature'] || req.headers['helius-signature'] || req.headers['shyft-signature']; 

        if (PAYMENT_WEBHOOK_SECRET) {
            if(!signatureFromHeader) console.warn(`${webhookLogPrefix} Webhook secret is SET, but NO signature header found. Processing insecurely (NOT FOR PRODUCTION).`);
            else console.log(`${webhookLogPrefix} Received signature. Implement provider-specific validation for PAYMENT_WEBHOOK_SECRET.`);
        } else {
            console.warn(`${webhookLogPrefix} PAYMENT_WEBHOOK_SECRET NOT SET. Processing insecurely (NOT FOR PRODUCTION).`);
        }

        try {
            const payload = req.body; 
            let relevantTransactions = []; 

            if (Array.isArray(payload)) { 
                payload.forEach(event => {
                    if (event.type === "TRANSFER" && event.transaction?.signature && Array.isArray(event.nativeTransfers)) { // Example for Helius native SOL
                        event.nativeTransfers.forEach(transfer => {
                            if (transfer.toUserAccount && 
                                (transfer.mint === "So11111111111111111111111111111111111111112" || !transfer.mint) && 
                                transfer.amount && transfer.amount > 0) { 
                                relevantTransactions.push({
                                    signature: event.transaction.signature,
                                    depositToAddress: transfer.toUserAccount,
                                });
                            }
                        });
                    }
                    // Add other 'else if' blocks here for different webhook providers or event types
                });
            } else {
                // Handle non-array payloads if your provider sends them differently
                 console.warn(`${webhookLogPrefix} Received non-array payload: ${typeof payload}. Adapt parsing logic.`);
            }

            if (relevantTransactions.length === 0) {
                return res.status(200).send('Webhook received; no actionable SOL transfer data identified.');
            }

            console.log(`${webhookLogPrefix} Identified ${relevantTransactions.length} potential deposit(s) from webhook.`);

            for (const txInfo of relevantTransactions) {
                const { signature, depositToAddress } = txInfo;
                if (!signature || !depositToAddress) {
                    console.warn(`${webhookLogPrefix} Webhook tx info missing signature or depositToAddress. Skipping.`);
                    continue; 
                }

                if (!processedDepositTxSignatures.has(signature)) { 
                    const addrInfo = await findDepositAddressInfoDB(depositToAddress); 
                    if (addrInfo && addrInfo.isActive) { 
                        console.log(`${webhookLogPrefix} ✅ Valid webhook for active address ${depositToAddress}. Queuing TX: ${signature} for User: ${addrInfo.userId}`);
                        depositProcessorQueue.add(() => processDepositTransaction(signature, depositToAddress, addrInfo.walletId, addrInfo.userId));
                        processedDepositTxSignatures.add(signature); 
                        if(processedDepositTxSignatures.size > (parseInt(process.env.MAX_PROCESSED_TX_CACHE, 10) || 10000) * 1.2) {
                           const oldSigs = Array.from(processedDepositTxSignatures).slice(0, processedDepositTxSignatures.size - (parseInt(process.env.MAX_PROCESSED_TX_CACHE, 10) || 10000));
                           oldSigs.forEach(s => processedDepositTxSignatures.delete(s));
                        }
                    } else {
                        console.warn(`${webhookLogPrefix} ⚠️ Webhook for inactive/expired/unknown address ${depositToAddress}. TX ${signature}. AddrInfo found: ${!!addrInfo}`);
                        if(addrInfo) processedDepositTxSignatures.add(signature);
                    }
                }
            }
            res.status(200).send('Webhook data queued for processing where applicable');
        } catch (error) {
            console.error(`❌ ${webhookLogPrefix} Error processing webhook payload: ${error.message}`, error.stack?.substring(0,500));
            res.status(500).send('Internal Server Error during webhook processing');
        }
    });

    console.log(`${logPrefix} ✅ Webhook endpoint ${paymentWebhookPath} configured on Express app.`);
}

// --- End of Part P3 ---
// --- Start of Part P4 ---
// index.js - Part P4: Payment System Background Tasks & Webhook Handling
//---------------------------------------------------------------------------
// Assumed global constants, Solana connection, DB pool, processing queues,
// Keypairs, Utilities, DB Ops, Solana Utils, Cache, and global flags are available.

// --- Helper Function to Analyze Transaction for Deposits ---
/**
 * Analyzes a fetched Solana transaction to find the amount transferred to a specific deposit address.
 * @param {import('@solana/web3.js').VersionedTransactionResponse | import('@solana/web3.js').TransactionResponse | null} txResponse
 * @param {string} depositAddress
 * @returns {{transferAmount: bigint, payerAddress: string | null}}
 */
function analyzeTransactionAmounts(txResponse, depositAddress) {
    let transferAmount = 0n;
    let payerAddress = null;

    if (!txResponse || !txResponse.meta || !txResponse.transaction) {
        return { transferAmount, payerAddress };
    }

    // Attempt to get payer from the first signer if message is available
    if (txResponse.transaction.message && txResponse.transaction.message.accountKeys && txResponse.transaction.message.accountKeys.length > 0) {
        // The first account is usually the fee payer and often the source for simple transfers
        payerAddress = txResponse.transaction.message.accountKeys[0].toBase58();
    }

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
    // More robust way to find the sender for the specific transfer to depositAddress
    // This is complex as it requires iterating through instructions and matching with account keys.
    // For now, the balance change method is a common approach for simple inbound transfers.
    // If specific instructions need to be parsed (e.g. for SPL tokens or more complex interactions), this would need expansion.
    // Also, if payerAddress is still null and there are innerInstructions, one could look there.

    return { transferAmount, payerAddress };
}

// --- Global State for Background Task Control ---
let depositMonitorIntervalId = null;
let sweepIntervalId = null;

// Add static properties to functions to track running state, ensure these are defined before use.
// These functions are defined below in this Part.
// It's better to define these where the functions themselves are defined or manage state differently.
// For now, assuming they might be forward-declared or this is a pattern in use.
// Consider managing `isRunning` state more robustly if issues arise.
// if (typeof monitorDepositsPolling === 'function' && typeof monitorDepositsPolling.isRunning === 'undefined') {
//     monitorDepositsPolling.isRunning = false;
// }
// if (typeof sweepDepositAddresses === 'function' && typeof sweepDepositAddresses.isRunning === 'undefined') {
//     sweepDepositAddresses.isRunning = false;
// }


// --- Deposit Monitoring Logic ---

function startDepositMonitoring() {
    let intervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs < 5000) { // Minimum 5 seconds
        intervalMs = 15000; // Default to 15 seconds
        console.warn(`[DepositMonitor] Invalid DEPOSIT_MONITOR_INTERVAL_MS, using default ${intervalMs}ms.`);
    }
    
    if (depositMonitorIntervalId) {
        clearInterval(depositMonitorIntervalId);
    } else {
        console.log(`⚙️ [DepositMonitor] Starting Deposit Monitor (Interval: ${intervalMs / 1000}s)...`);
    }
    
    // Ensure monitorDepositsPolling has the isRunning property initialized
    if (typeof monitorDepositsPolling.isRunning === 'undefined') {
        monitorDepositsPolling.isRunning = false;
    }

    const initialDelay = (parseInt(process.env.INIT_DELAY_MS, 10) || 7000) + 2000; // Stagger start
    setTimeout(() => {
        if (isShuttingDown) return;
        monitorDepositsPolling().catch(err => console.error("❌ [Initial Deposit Monitor Run] Error:", err.message, err.stack?.substring(0,500)));
        
        depositMonitorIntervalId = setInterval(() => {
            monitorDepositsPolling().catch(err => console.error("❌ [Recurring Deposit Monitor Run] Error:", err.message, err.stack?.substring(0,500)));
        }, intervalMs);

        if (depositMonitorIntervalId && typeof depositMonitorIntervalId.unref === 'function') {
            depositMonitorIntervalId.unref(); // Allow Node.js to exit if this is the only timer
        }
    }, initialDelay);
}

function stopDepositMonitoring() {
    if (depositMonitorIntervalId) {
        clearInterval(depositMonitorIntervalId);
        depositMonitorIntervalId = null;
        if (typeof monitorDepositsPolling === 'function') monitorDepositsPolling.isRunning = false; // Reset flag
        console.log("🛑 [DepositMonitor] Deposit monitoring stopped.");
    }
}

async function monitorDepositsPolling() {
    const logPrefix = '[DepositMonitor Polling]';
    if (isShuttingDown) { return; }
    if (monitorDepositsPolling.isRunning) {
        return;
    }
    monitorDepositsPolling.isRunning = true;

    try {
        const batchSize = parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE, 10) || 75;
        const sigFetchLimit = parseInt(process.env.DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT, 10) || 15;

        const pendingAddressesRes = await queryDatabase(
            `SELECT wallet_id, public_key, user_telegram_id, derivation_path, expires_at
             FROM user_deposit_wallets
             WHERE is_active = TRUE AND expires_at > NOW()
             ORDER BY created_at ASC
             LIMIT $1`,
            [batchSize]
        );

        if (pendingAddressesRes.rowCount > 0) {
            console.log(`${logPrefix} Found ${pendingAddressesRes.rowCount} active address(es) to check.`);
        }

        for (const row of pendingAddressesRes.rows) {
            if (isShuttingDown) { console.log(`${logPrefix} Shutdown during address check.`); break; }
            
            const depositAddress = row.public_key;
            const userDepositWalletId = row.wallet_id;
            const userId = String(row.user_telegram_id);
            const addrLogPrefix = `[Monitor Addr:${depositAddress.slice(0, 6)} WID:${userDepositWalletId}]`;

            try {
                const pubKey = new PublicKey(depositAddress);
                const signatures = await solanaConnection.getSignaturesForAddress(
                    pubKey, { limit: sigFetchLimit }, DEPOSIT_CONFIRMATION_LEVEL
                );

                if (signatures && signatures.length > 0) {
                    for (const sigInfo of signatures.reverse()) { // Process oldest first
                        if (sigInfo?.signature && !processedDepositTxSignatures.has(sigInfo.signature)) {
                            const isConfirmedByLevel = sigInfo.confirmationStatus === DEPOSIT_CONFIRMATION_LEVEL || sigInfo.confirmationStatus === 'finalized';
                            if (!sigInfo.err && isConfirmedByLevel) {
                                console.log(`${addrLogPrefix} ✅ New confirmed TX: ${sigInfo.signature}. Queuing for processing.`);
                                depositProcessorQueue.add(() => processDepositTransaction(sigInfo.signature, depositAddress, userDepositWalletId, userId))
                                    .catch(queueError => console.error(`❌ ${addrLogPrefix} Error adding TX ${sigInfo.signature} to deposit queue: ${queueError.message}`));
                                processedDepositTxSignatures.add(sigInfo.signature);
                                // Clean up old signatures from the Set to prevent memory leak
                                if(processedDepositTxSignatures.size > (parseInt(process.env.MAX_PROCESSED_TX_CACHE, 10) || 10000) * 1.2) {
                                   const oldSigs = Array.from(processedDepositTxSignatures).slice(0, processedDepositTxSignatures.size - (parseInt(process.env.MAX_PROCESSED_TX_CACHE, 10) || 10000));
                                   oldSigs.forEach(s => processedDepositTxSignatures.delete(s));
                                }
                            } else if (sigInfo.err) {
                                console.warn(`${addrLogPrefix} ⚠️ TX ${sigInfo.signature} has error: ${JSON.stringify(sigInfo.err)}. Marking processed.`);
                                processedDepositTxSignatures.add(sigInfo.signature);
                            }
                        }
                    }
                }
            } catch (error) {
                console.error(`❌ ${addrLogPrefix} Error checking signatures for ${depositAddress}: ${error.message}`);
                if (error?.status === 429 || String(error?.message).toLowerCase().includes('rate limit')) {
                    console.warn(`${addrLogPrefix} Rate limit hit during signature check. Pausing briefly.`);
                    await sleep(5000 + Math.random() * 3000); 
                }
            }
            await sleep(parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_DELAY_MS, 10) || 300); // Delay between checking each address
        }
    } catch (error) {
        console.error(`❌ ${logPrefix} Critical error in main polling loop: ${error.message}`, error.stack?.substring(0,500));
        if (typeof notifyAdmin === 'function') await notifyAdmin(`🚨 *ERROR in Deposit Monitor Loop* 🚨\n\n\`${escapeMarkdownV2(String(error.message || error))}\`\nCheck logs\\.`, {parse_mode: 'MarkdownV2'});
    } finally {
        monitorDepositsPolling.isRunning = false;
    }
}

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
            console.warn(`ℹ️ ${logPrefix} TX ${txSignature} failed on-chain or details not found. Error: ${JSON.stringify(txResponse?.meta?.err)}. Marking processed locally.`);
            processedDepositTxSignatures.add(txSignature);
            return;
        }

        const { transferAmount, payerAddress } = analyzeTransactionAmounts(txResponse, depositAddress);

        if (transferAmount <= 0n) {
            processedDepositTxSignatures.add(txSignature);
            return;
        }
        const depositAmountSOLDisplay = await formatBalanceForDisplay(transferAmount, 'SOL'); // formatBalanceForDisplay from Part 3
        console.log(`✅ ${logPrefix} Valid deposit: ${depositAmountSOLDisplay} from ${payerAddress || 'unknown source'}.`);

        client = await pool.connect();
        await client.query('BEGIN');

        const depositRecordResult = await recordConfirmedDepositDB(client, stringUserId, userDepositWalletId, depositAddress, txSignature, transferAmount, payerAddress, txResponse.blockTime);
        if (depositRecordResult.alreadyProcessed) {
            await client.query('ROLLBACK'); 
            processedDepositTxSignatures.add(txSignature); // Ensure it's marked if DB said already processed
            return;
        }
        if (!depositRecordResult.success || !depositRecordResult.depositId) {
            throw new Error(`Failed to record deposit in DB for ${txSignature}: ${depositRecordResult.error || "Unknown DB error."}`);
        }
        const depositId = depositRecordResult.depositId;

        // Mark address as inactive (but not necessarily swept yet by this function)
        // Sweeping is a separate process. This just deactivates it for new deposits.
        const markedInactive = await markDepositAddressInactiveDB(client, userDepositWalletId, false, null); // false for swept
        if (!markedInactive) {
            // This is not critical enough to fail the whole deposit crediting
            console.warn(`${logPrefix} ⚠️ Could not mark deposit address WID ${userDepositWalletId} as inactive after deposit.`);
        }

        const ledgerNote = `Deposit from ${payerAddress ? payerAddress.slice(0,6)+'...'+payerAddress.slice(-4) : 'Unknown'} to ${depositAddress.slice(0,6)}... TX:${txSignature.slice(0,6)}...`;
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, stringUserId, transferAmount, 'deposit', { deposit_id: depositId }, ledgerNote); // updateUserBalanceAndLedger from Part P2
        if (!balanceUpdateResult.success || typeof balanceUpdateResult.newBalanceLamports === 'undefined') {
            throw new Error(`Failed to update user ${stringUserId} balance/ledger for deposit: ${balanceUpdateResult.error || "Unknown DB error."}`);
        }
        
        await client.query('COMMIT');
        processedDepositTxSignatures.add(txSignature);

        const newBalanceUSDDisplay = await formatBalanceForDisplay(balanceUpdateResult.newBalanceLamports, 'USD');
        const userForNotif = await getOrCreateUser(stringUserId); 
        const playerRefForNotif = getPlayerDisplayReference(userForNotif || { id: stringUserId, first_name: "Player" }); // getPlayerDisplayReference from Part 3
        
        await safeSendMessage(stringUserId, // safeSendMessage from Part 1
            `🎉 *Deposit Confirmed, ${playerRefForNotif}!* 🎉\n\n` +
            `Your deposit of *${escapeMarkdownV2(depositAmountSOLDisplay)}* has been successfully credited to your casino account.\n\n` + // Removed \\. as it's end of sentence.
            `💰 Your New Balance: Approx. *${escapeMarkdownV2(newBalanceUSDDisplay)}*\n` + // Escaped period in Approx.
            `🧾 Transaction ID: \`${escapeMarkdownV2(txSignature)}\`\n\n` +
            `Time to hit the tables! Good luck! 🎰`, // Escaped !
            { parse_mode: 'MarkdownV2' }
        );
        
    } catch (error) {
        console.error(`❌ ${logPrefix} CRITICAL ERROR processing deposit TX ${txSignature}: ${error.message}`, error.stack?.substring(0,500));
        if (client) { await client.query('ROLLBACK').catch(rbErr => console.error(`❌ ${logPrefix} Rollback failed:`, rbErr)); }
        processedDepositTxSignatures.add(txSignature); // Mark as processed to avoid retries on this erroring TX
        if (typeof notifyAdmin === 'function') { // notifyAdmin from Part 1
            await notifyAdmin(`🚨 *CRITICAL Error Processing Deposit* 🚨\nTX: \`${escapeMarkdownV2(txSignature)}\`\nAddr: \`${escapeMarkdownV2(depositAddress)}\`\nUser: \`${escapeMarkdownV2(stringUserId)}\`\n*Error:*\n\`${escapeMarkdownV2(String(error.message || error))}\`\nManual investigation required.`, {parse_mode:'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}


// --- Deposit Address Sweeping Logic (Updated with Rent Exemption) ---
function startSweepingProcess() {
    let intervalMs = parseInt(process.env.SWEEP_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs <= 0) {
        return; // Sweeping disabled
    }
    if (intervalMs < 60000) { 
        intervalMs = 60000;
        console.warn(`🧹 [Sweeper] SWEEP_INTERVAL_MS too low, enforcing minimum ${intervalMs / 1000}s.`);
    }
    
    if (sweepIntervalId) {
        clearInterval(sweepIntervalId);
    } else {
        console.log(`⚙️ [Sweeper] Starting Fund Sweeper (Interval: ${intervalMs / 1000 / 60} minutes)...`);
    }

    if (typeof sweepDepositAddresses.isRunning === 'undefined') {
        sweepDepositAddresses.isRunning = false;
    }

    const initialDelay = (parseInt(process.env.INIT_DELAY_MS, 10) || 7000) + 15000; // Stagger after other initializations
    setTimeout(() => {
        if (isShuttingDown) return;
        sweepDepositAddresses().catch(err => console.error("❌ [Initial Sweep Run] Error:", err.message, err.stack?.substring(0,500)));
        
        sweepIntervalId = setInterval(() => {
            sweepDepositAddresses().catch(err => console.error("❌ [Recurring Sweep Run] Error:", err.message, err.stack?.substring(0,500)));
        }, intervalMs);

        if (sweepIntervalId && typeof sweepIntervalId.unref === 'function') {
            sweepIntervalId.unref();
        }
    }, initialDelay);
}

function stopSweepingProcess() {
    if (sweepIntervalId) {
        clearInterval(sweepIntervalId);
        sweepIntervalId = null;
        if (typeof sweepDepositAddresses === 'function') sweepDepositAddresses.isRunning = false; 
        console.log("🛑 [Sweeper] Fund sweeping stopped.");
    }
}

// This is the updated sweepDepositAddresses function
async function sweepDepositAddresses() {
    const logPrefix = '[SweepAddresses]';
    if (isShuttingDown) { return; }
    if (sweepDepositAddresses.isRunning) {
        return;
    }
    sweepDepositAddresses.isRunning = true;
    console.log(`🧹 ${logPrefix} Starting new sweep cycle...`);

    let addressesProcessedThisCycle = 0;
    let totalSweptThisCycle = 0n;
    const sweepBatchSize = parseInt(process.env.SWEEP_BATCH_SIZE, 10) || 15; // From PAYMENT_ENV_DEFAULTS
    const sweepAddressDelayMs = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10) || 1500; // From PAYMENT_ENV_DEFAULTS
    
    if (!MAIN_BOT_KEYPAIR || !DEPOSIT_MASTER_SEED_PHRASE) {
        console.error(`❌ ${logPrefix} MAIN_BOT_KEYPAIR or DEPOSIT_MASTER_SEED_PHRASE not available. Cannot sweep.`);
        sweepDepositAddresses.isRunning = false;
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 [SweepAddresses] Critical configuration missing (Main Bot Keypair or Master Seed)\\. Sweeping aborted\\.`, {parse_mode: 'MarkdownV2'});
        return;
    }
    const sweepTargetAddress = MAIN_BOT_KEYPAIR.publicKey.toBase58();

    let rentLamports;
    try {
        rentLamports = BigInt(await solanaConnection.getMinimumBalanceForRentExemption(0)); // For a 0-data account
    } catch (rentError) {
        console.error(`❌ ${logPrefix} Failed to get minimum balance for rent exemption: ${rentError.message}. Using fallback.`);
        rentLamports = BigInt(890880); // Approx 0.00089 SOL
        if (typeof notifyAdmin === "function") await notifyAdmin(`⚠️ [SweepAddresses] Failed to fetch rent exemption, using fallback: ${rentLamports}\\. Error: ${escapeMarkdownV2(rentError.message)}`, {parse_mode: 'MarkdownV2'});
    }

    // SWEEP_FEE_BUFFER_LAMPORTS is the amount for the transaction fee ITSELF for the sweep.
    const feeForSweepTxItself = BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS || '20000'); // Default to 20k lamports (0.00002 SOL)
    const minimumLamportsToLeave = rentLamports + feeForSweepTxItself; // Total to leave for rent + this sweep's tx fee

    let addressesToConsiderRes = null;

    try {
        const addressesQuery = `SELECT wallet_id, public_key, derivation_path, user_telegram_id
            FROM user_deposit_wallets 
            WHERE swept_at IS NULL 
            AND (is_active = FALSE OR expires_at < NOW() - INTERVAL '5 minutes')
            ORDER BY created_at ASC 
            LIMIT $1`;
        addressesToConsiderRes = await queryDatabase(addressesQuery, [sweepBatchSize]);

        if (!addressesToConsiderRes || !addressesToConsiderRes.rows) {
            sweepDepositAddresses.isRunning = false;
            return;
        }

        if (addressesToConsiderRes.rowCount > 0) {
            console.log(`${logPrefix} Found ${addressesToConsiderRes.rowCount} potential addresses to check. Min balance to leave in source account after sweep: ${formatCurrency(minimumLamportsToLeave, 'SOL')}`);
        }

        for (const addrData of addressesToConsiderRes.rows) {
            if (isShuttingDown) { console.log(`${logPrefix} Shutdown during address processing.`); break; }
            
            const addrLogPrefix = `[Sweep Addr:${addrData.public_key.slice(0, 6)} WID:${addrData.wallet_id}]`;
            let depositKeypair;
            let clientForThisAddress = null;

            try {
                clientForThisAddress = await pool.connect();
                await clientForThisAddress.query('BEGIN');

                try {
                    depositKeypair = deriveSolanaKeypair(DEPOSIT_MASTER_SEED_PHRASE, addrData.derivation_path);
                    if (!depositKeypair || depositKeypair.publicKey.toBase58() !== addrData.public_key) {
                        throw new Error(`Derived public key mismatch! DB: ${addrData.public_key}, Derived: ${depositKeypair?.publicKey?.toBase58()}`);
                    }
                } catch (derivError) {
                    console.error(`❌ ${addrLogPrefix} Critical error deriving key: ${derivError.message}. Marking as sweep_error.`);
                    await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, true, null); 
                    await clientForThisAddress.query("UPDATE user_deposit_wallets SET notes = COALESCE(notes, '') || ' Sweep Error: Key derivation exception.' WHERE wallet_id = $1", [addrData.wallet_id]);
                    await clientForThisAddress.query('COMMIT');
                    continue;
                }

                const balanceLamports = await getSolBalance(addrData.public_key);
                if (balanceLamports === null) {
                    console.warn(`${addrLogPrefix} Could not fetch balance. Skipping.`);
                    await clientForThisAddress.query('ROLLBACK'); 
                    continue;
                }

                if (balanceLamports <= minimumLamportsToLeave) {
                    // console.log(`${addrLogPrefix} Balance ${formatCurrency(balanceLamports, 'SOL')} is too low to sweep (Min required to leave: ${formatCurrency(minimumLamportsToLeave, 'SOL')}). Marking 'swept_low_balance'.`);
                    await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, true, balanceLamports);
                    await clientForThisAddress.query('COMMIT');
                    continue;
                }

                const amountToSweep = balanceLamports - minimumLamportsToLeave;
                
                if (amountToSweep <= 0n) {
                    console.warn(`${addrLogPrefix} Calculated amountToSweep is not positive (${amountToSweep}). Balance: ${balanceLamports}, MinToLeave: ${minimumLamportsToLeave}. Marking low balance.`);
                    await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, true, balanceLamports);
                    await clientForThisAddress.query('COMMIT');
                    continue;
                }

                const sweepPriorityFee = parseInt(process.env.SWEEP_PRIORITY_FEE_MICROLAMPORTS, 10) || 5000; // From PAYMENT_ENV_DEFAULTS
                const sweepComputeUnits = parseInt(process.env.SWEEP_COMPUTE_UNIT_LIMIT, 10) || 30000; // From PAYMENT_ENV_DEFAULTS
                
                const sendResult = await sendSol(depositKeypair, sweepTargetAddress, amountToSweep, `Sweep from ${addrData.public_key.slice(0,4)}..${addrData.public_key.slice(-4)}`, sweepPriorityFee, sweepComputeUnits);

                if (sendResult.success && sendResult.signature) {
                    totalSweptThisCycle += amountToSweep;
                    addressesProcessedThisCycle++;
                    console.log(`✅ ${addrLogPrefix} Sweep successful! TX: ${sendResult.signature}. Amount: ${formatCurrency(amountToSweep, 'SOL')}`);
                    await recordSweepTransactionDB(clientForThisAddress, addrData.public_key, sweepTargetAddress, amountToSweep, sendResult.signature);
                    // Pass original balanceLamports to record what it had *before* this successful sweep
                    await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, true, balanceLamports); 
                } else {
                    console.error(`❌ ${addrLogPrefix} Sweep failed: ${sendResult.error}. Type: ${sendResult.errorType}. Retryable: ${sendResult.isRetryable}`);
                    if (sendResult.errorType === "InsufficientFundsError" || sendResult.isRetryable === false) {
                        // If error indicates funds issue (e.g. rent after fee, which this logic tries to prevent) or non-retryable, mark as attempted.
                        // We don't mark as 'swept' but as inactive with a note.
                        await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, false, balanceLamports); 
                        await clientForThisAddress.query("UPDATE user_deposit_wallets SET notes = COALESCE(notes, '') || ' Sweep Attempt Failed: " + escapeMarkdownV2(String(sendResult.error || '')).substring(0,100) + "' WHERE wallet_id = $1", [addrData.wallet_id]);
                    }
                }
                await clientForThisAddress.query('COMMIT');
            } catch (addrError) {
                if (clientForThisAddress) await clientForThisAddress.query('ROLLBACK').catch(rbErr => console.error(`${addrLogPrefix} Rollback error: ${rbErr.message}`));
                console.error(`❌ ${addrLogPrefix} Error processing address ${addrData.public_key}: ${addrError.message}`, addrError.stack?.substring(0,500));
                 if (clientForThisAddress) { // Try to mark as error if client is still available
                    try {
                        await clientForThisAddress.query('BEGIN'); // New transaction for this update
                        await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, false, null); // Not swept, attempt to make inactive
                        await clientForThisAddress.query("UPDATE user_deposit_wallets SET notes = COALESCE(notes, '') || ' Sweep Error: Processing exception.' WHERE wallet_id = $1", [addrData.wallet_id]);
                        await clientForThisAddress.query('COMMIT');
                    } catch (finalErr) {
                        if (clientForThisAddress) await clientForThisAddress.query('ROLLBACK');
                        console.error(`❌ ${addrLogPrefix} Error marking address after processing error: ${finalErr.message}`);
                    }
                }
            } finally {
                if (clientForThisAddress) clientForThisAddress.release();
            }
            await sleep(sweepAddressDelayMs);
        }
    } catch (cycleError) {
        console.error(`❌ ${logPrefix} Critical error in sweep cycle setup or main query: ${cycleError.message}`, cycleError.stack?.substring(0,500));
        if (typeof notifyAdmin === 'function') await notifyAdmin(`🚨 *ERROR in Fund Sweeping Cycle Setup* 🚨\n\n\`${escapeMarkdownV2(String(cycleError.message || cycleError))}\`\nCheck logs\\. Sweeping aborted this cycle\\.`, {parse_mode: 'MarkdownV2'});
    } finally {
        sweepDepositAddresses.isRunning = false;
        if (addressesProcessedThisCycle > 0) {
            const sweptAmountFormatted = formatCurrency(totalSweptThisCycle, 'SOL');
            console.log(`🧹 ${logPrefix} Sweep cycle finished. Swept ~${sweptAmountFormatted} from ${addressesProcessedThisCycle} addresses that had sufficient balance beyond rent & fees.`);
            if(typeof notifyAdmin === 'function') await notifyAdmin(`🧹 Sweep Report: Swept approx\\. ${escapeMarkdownV2(sweptAmountFormatted)} from ${addressesProcessedThisCycle} deposit addresses\\.`, {parse_mode: 'MarkdownV2'});
        } else if (addressesToConsiderRes && addressesToConsiderRes.rowCount > 0) {
            // console.log(`🧹 ${logPrefix} Sweep finished. No funds swept from ${addressesToConsiderRes.rowCount} considered addresses (likely due to low balance or errors).`);
        } else {
             // console.log(`🧹 ${logPrefix} Sweep cycle finished. No addresses found needing a sweep.`);
        }
    }
}

// --- Payout Job Processing Logic ---
// (addPayoutJob, handleWithdrawalPayoutJob, handleReferralPayoutJob functions from original Part P4 remain here)
async function addPayoutJob(jobData) {
    const jobType = jobData?.type || 'unknown_payout';
    const jobId = jobData?.withdrawalId || jobData?.payoutId || jobData?.referralId || 'N/A_JobId';
    const userIdForLog = jobData?.userId || jobData?.referrerUserId || 'N/A_User';
    const logPrefix = `[AddPayoutJob Type:${jobType} ID:${jobId} ForUser:${userIdForLog}]`;

    if (typeof payoutProcessorQueue === 'undefined' || typeof sleep === 'undefined' || typeof notifyAdmin === 'undefined' || typeof escapeMarkdownV2 === 'undefined') {
        console.error(`${logPrefix} 🚨 CRITICAL: Payout queue or essential utilities missing. Cannot add job.`);
        if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL Error: Cannot add payout job ${escapeMarkdownV2(jobType)}:${escapeMarkdownV2(String(jobId))}. Payout system compromised.`, {parse_mode: 'MarkdownV2'});
        return;
    }

    payoutProcessorQueue.add(async () => {
        let attempts = 0;
        const maxAttempts = (parseInt(process.env.PAYOUT_JOB_RETRIES, 10) || 3) + 1; // +1 for initial attempt
        const baseDelayMs = parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10) || 7000;

        while(attempts < maxAttempts) {
            attempts++;
            const attemptLogPrefix = `[PayoutJob Att:${attempts}/${maxAttempts} ${jobType} ID:${jobId}]`;
            try {
                if (jobData.type === 'payout_withdrawal' && typeof handleWithdrawalPayoutJob === 'function') {
                    await handleWithdrawalPayoutJob(jobData.withdrawalId);
                } else if (jobData.type === 'payout_referral' && typeof handleReferralPayoutJob === 'function') {
                    await handleReferralPayoutJob(jobData.referralId); // Assuming jobData has referralId for this type
                } else {
                    throw new Error(`Unknown or unavailable payout job type handler: ${jobData.type}`);
                }
                console.log(`✅ ${attemptLogPrefix} Job completed successfully.`);
                return; 
            } catch(error) {
                console.warn(`⚠️ ${attemptLogPrefix} Attempt failed: ${error.message}`);
                const isRetryableFlag = error.isRetryable === true; // Job specific errors should set this

                if (!isRetryableFlag || attempts >= maxAttempts) {
                    console.error(`❌ ${attemptLogPrefix} Job failed permanently after ${attempts} attempts. Error: ${error.message}`);
                    if (typeof notifyAdmin === "function") {
                        notifyAdmin(`🚨 *PAYOUT JOB FAILED (Permanent)* 🚨\nType: \`${escapeMarkdownV2(jobType)}\`\nID: \`${escapeMarkdownV2(String(jobId))}\`\nUser: \`${escapeMarkdownV2(String(userIdForLog))}\`\nError: \`${escapeMarkdownV2(String(error.message || error))}\`\nManual intervention may be required.`, {parse_mode:'MarkdownV2'}).catch(()=>{});
                    }
                    return; 
                }
                const delayWithJitter = baseDelayMs * Math.pow(1.5, attempts - 1) * (0.8 + Math.random() * 0.4); // Exponential backoff with jitter
                const actualDelay = Math.min(delayWithJitter, parseInt(process.env.RPC_RETRY_MAX_DELAY, 10) || 90000); // Max delay from env or 90s
                await sleep(actualDelay);
            }
        }
    }).catch(queueError => { // Catch errors from adding to queue or if the promise from add itself rejects (e.g. from PQueue timeout)
        console.error(`❌ ${logPrefix} CRITICAL Error in Payout Queue execution or job addition: ${queueError.message}`, queueError.stack?.substring(0,500));
        if (typeof notifyAdmin === "function") {
            notifyAdmin(`🚨 *CRITICAL Payout Queue Error* 🚨\nJob: \`${escapeMarkdownV2(jobType)}:${escapeMarkdownV2(String(jobId))}\`\nError: \`${escapeMarkdownV2(String(queueError.message || queueError))}\`\nQueue potentially compromised.`, {parse_mode:'MarkdownV2'}).catch(()=>{});
        }
    });
}

async function handleWithdrawalPayoutJob(withdrawalId) {
    const logPrefix = `[WithdrawJob ID:${withdrawalId}]`;
    console.log(`⚙️ ${logPrefix} Processing withdrawal payout...`);
    let clientForDb = null;
    let sendSolResult = { success: false, error: "Send SOL not initiated", isRetryable: false }; 

    const details = await getWithdrawalDetailsDB(withdrawalId); // From Part P2
    if (!details) {
        const error = new Error(`Withdrawal details not found for ID ${withdrawalId}. Job cannot proceed.`);
        error.isRetryable = false; throw error;
    }

    if (details.status === 'completed' || details.status === 'confirmed' || details.status === 'sent') {
        return; 
    }
    if (details.status === 'failed' && !sendSolResult.isRetryable) { 
        // If previously failed non-retryably, don't try again unless sendSolResult indicates it is now retryable (e.g. new error)
        // This logic might need refinement based on how isRetryable is set by sendSol.
        // For now, if DB says failed, and we haven't just had a retryable failure, we stop.
        return;
    }

    const userId = String(details.user_telegram_id);
    const recipient = details.destination_address;
    const amountToActuallySend = BigInt(details.amount_lamports);
    const feeApplied = BigInt(details.fee_lamports);
    const totalAmountDebitedFromUser = amountToActuallySend + feeApplied;
    const userForNotif = await getOrCreateUser(userId); 
    const playerRefForNotif = getPlayerDisplayReference(userForNotif || {id:userId, first_name:"Player"}); // getPlayerDisplayReference from Part 3

    try {
        clientForDb = await pool.connect();
        await clientForDb.query('BEGIN');
        await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'processing'); // From Part P2
        await clientForDb.query('COMMIT');
    } catch (dbError) {
        if (clientForDb) await clientForDb.query('ROLLBACK').catch(()=>{});
        console.error(`❌ ${logPrefix} DB error setting status to 'processing': ${dbError.message}`);
        jobError.isRetryable = true; // DB errors are often retryable
        throw dbError; // Re-throw to trigger retry if applicable
    } finally {
        if (clientForDb) clientForDb.release();
        clientForDb = null; // Ensure it's reset
    }

    try {
        sendSolResult = await sendSol(MAIN_BOT_KEYPAIR, recipient, amountToActuallySend, `Withdrawal ID ${withdrawalId} from ${BOT_NAME}`, details.priority_fee_microlamports, details.compute_unit_limit); // sendSol from Part P1

        clientForDb = await pool.connect(); 
        await clientForDb.query('BEGIN');

        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful. TX: ${sendSolResult.signature}. Marking 'completed'.`);
            await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'completed', sendSolResult.signature, null, sendSolResult.blockTime);
            await clientForDb.query('COMMIT');
            
            await safeSendMessage(userId, // safeSendMessage from Part 1
                `💸 *Withdrawal Sent Successfully, ${playerRefForNotif}!* 💸\n\n` +
                `Your withdrawal of *${escapeMarkdownV2(formatCurrency(amountToActuallySend, 'SOL'))}* to wallet \`${escapeMarkdownV2(recipient)}\` has been processed.\n` + // Escaped .
                `🧾 Transaction ID: \`${escapeMarkdownV2(sendSolResult.signature)}\`\n\n` +
                `Funds should arrive shortly depending on network confirmations. Thank you for playing at ${escapeMarkdownV2(BOT_NAME)}!`, // Escaped !
                { parse_mode: 'MarkdownV2' }
            );
            return; 
        } else { 
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure.';
            console.error(`❌ ${logPrefix} sendSol FAILED for withdrawal ID ${withdrawalId}. Reason: ${sendErrorMsg}. Type: ${sendSolResult.errorType}. Retryable: ${sendSolResult.isRetryable}`);
            await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'failed', null, sendErrorMsg.substring(0, 250));
            
            const refundNotes = `Refund for failed withdrawal ID ${withdrawalId}. Send Error: ${sendErrorMsg.substring(0,100)}`;
            const refundUpdateResult = await updateUserBalanceAndLedger( // from Part P2
                clientForDb, userId, totalAmountDebitedFromUser, 
                'withdrawal_refund', { withdrawal_id: withdrawalId }, refundNotes
            );

            if (refundUpdateResult.success) {
                await clientForDb.query('COMMIT');
                console.log(`✅ ${logPrefix} Successfully refunded ${formatCurrency(totalAmountDebitedFromUser, 'SOL')} to user ${userId}.`);
                await safeSendMessage(userId,
                    `⚠️ *Withdrawal Failed* ⚠️\n\n${playerRefForNotif}, your withdrawal of *${escapeMarkdownV2(formatCurrency(amountToActuallySend, 'SOL'))}* could not be processed at this time (Reason: \`${escapeMarkdownV2(sendErrorMsg)}\`).\n` + // Escaped ()
                    `The full amount of *${escapeMarkdownV2(formatCurrency(totalAmountDebitedFromUser, 'SOL'))}* (including fee) has been refunded to your casino balance.`, // Escaped . and ()
                    {parse_mode: 'MarkdownV2'}
                );
            } else {
                await clientForDb.query('ROLLBACK');
                console.error(`❌ CRITICAL ${logPrefix} FAILED TO REFUND USER ${userId} for withdrawal ${withdrawalId}. Amount: ${formatCurrency(totalAmountDebitedFromUser, 'SOL')}. Refund DB Error: ${refundUpdateResult.error}`);
                if (typeof notifyAdmin === 'function') {
                    notifyAdmin(`🚨🚨 *CRITICAL: FAILED WITHDRAWAL REFUND* 🚨🚨\nUser: ${playerRefForNotif} (\`${escapeMarkdownV2(String(userId))}\`)\nWD ID: \`${withdrawalId}\`\nAmount Due (Refund): \`${escapeMarkdownV2(formatCurrency(totalAmountDebitedFromUser, 'SOL'))}\`\nSend Error: \`${escapeMarkdownV2(sendErrorMsg)}\`\nRefund DB Error: \`${escapeMarkdownV2(refundUpdateResult.error || 'Unknown')}\`\nMANUAL INTERVENTION REQUIRED.`, {parse_mode:'MarkdownV2'});
                }
            }
            
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true; 
            throw errorToThrowForRetry;
        }
    } catch (jobError) { // Catches errors from sendSol or DB ops after sendSol
        if (clientForDb && clientForDb.release) { // Check if clientForDb was connected and not yet released
             try { await clientForDb.query('ROLLBACK'); } catch(rbErr) { console.error(`${logPrefix} Final rollback error on jobError: ${rbErr.message}`);}
        }
        console.error(`❌ ${logPrefix} Error during withdrawal job ID ${withdrawalId}: ${jobError.message}`, jobError.stack?.substring(0,500));
        
        // Ensure status is 'failed' if not already completed.
        const updateClient = await pool.connect();
        try {
            const currentDetailsAfterJobError = await getWithdrawalDetailsDB(withdrawalId, updateClient); 
            if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'completed' && currentDetailsAfterJobError.status !== 'failed') {
                await updateWithdrawalStatusDB(updateClient, withdrawalId, 'failed', null, `Job error: ${String(jobError.message || jobError)}`.substring(0,250));
            }
        } catch (finalStatusUpdateError) {
            console.error(`${logPrefix} Failed to update status to 'failed' after job error: ${finalStatusUpdateError.message}`);
        } finally {
            updateClient.release();
        }
        
        if (jobError.isRetryable === undefined) { // Ensure isRetryable is set
             jobError.isRetryable = sendSolResult.isRetryable || false; 
        }
        throw jobError; // Re-throw for retry mechanism in addPayoutJob
    } finally {
        if (clientForDb && clientForDb.release) clientForDb.release(); // Ensure release if it was connected
    }
}


async function handleReferralPayoutJob(referralId) { // Changed payoutId to referralId for clarity
    const logPrefix = `[ReferralJob ID:${referralId}]`;
    console.log(`⚙️ ${logPrefix} Processing referral payout...`);
    let clientForDb = null;
    let sendSolResult = { success: false, error: "Send SOL not initiated for referral", isRetryable: false };
    const payerKeypair = REFERRAL_PAYOUT_KEYPAIR || MAIN_BOT_KEYPAIR; 

    const details = await getReferralDetailsDB(referralId); // from Part P2
    if (!details) {
        const error = new Error(`Referral payout details not found for ID ${referralId}.`); error.isRetryable = false; throw error;
    }
    if (details.status === 'paid_out') { return; }
    if (details.status === 'failed') { return; } // Don't retry if manually marked failed or permanently failed previously
    if (details.status !== 'earned') {
        console.warn(`ℹ️ ${logPrefix} Referral payout ID ${referralId} not 'earned' (current: ${details.status}). Skipping for now.`);
        const error = new Error(`Referral payout ID ${referralId} not in 'earned' state.`); error.isRetryable = false; throw error;
    }

    const referrerUserId = String(details.referrer_telegram_id);
    const amountToPay = BigInt(details.commission_amount_lamports || '0');
    if (amountToPay <= 0n) {
        const zeroErr = `Referral commission for ID ${referralId} is zero or less.`;
        console.warn(`${logPrefix} ${zeroErr}`);
        const zeroClient = await pool.connect();
        try { await updateReferralPayoutStatusDB(zeroClient, referralId, 'failed', null, zeroErr.substring(0,250)); } // from Part P2
        catch(e){ console.error(`${logPrefix} DB error marking zero commission as failed: ${e.message}`);}
        finally { zeroClient.release(); }
        const error = new Error(zeroErr); error.isRetryable = false; throw error;
    }

    const userForNotif = await getOrCreateUser(referrerUserId);
    const playerRefForNotif = getPlayerDisplayReference(userForNotif || {id: referrerUserId, first_name:"Referrer"});

    try {
        clientForDb = await pool.connect();
        await clientForDb.query('BEGIN');

        const referrerDetails = await getPaymentSystemUserDetails(referrerUserId, clientForDb); // from Part P2
        if (!referrerDetails?.solana_wallet_address) {
            const noWalletMsg = `Referrer ${playerRefForNotif} (${escapeMarkdownV2(referrerUserId)}) has no linked SOL wallet for referral payout ID ${referralId}.`;
            console.error(`❌ ${logPrefix} ${noWalletMsg}`);
            await updateReferralPayoutStatusDB(clientForDb, referralId, 'failed', null, noWalletMsg.substring(0, 250));
            await clientForDb.query('COMMIT');
            const error = new Error(noWalletMsg); error.isRetryable = false; throw error;
        }
        const recipientAddress = referrerDetails.solana_wallet_address;

        await updateReferralPayoutStatusDB(clientForDb, referralId, 'processing');
        await clientForDb.query('COMMIT');
    } catch(dbProcError) {
        if(clientForDb) await clientForDb.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} DB error setting status to 'processing': ${dbProcError.message}`);
        jobError.isRetryable = true; // DB errors often retryable
        throw dbProcError;
    }
    finally {
        if (clientForDb) clientForDb.release();
        clientForDb = null;
    }


    try {
        sendSolResult = await sendSol(payerKeypair, recipientAddress, amountToPay, `Referral Commission - ${BOT_NAME} - ID ${referralId}`);

        clientForDb = await pool.connect(); 
        await clientForDb.query('BEGIN');
        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful for referral ID ${referralId}. TX: ${sendSolResult.signature}.`);
            await updateReferralPayoutStatusDB(clientForDb, referralId, 'paid_out', sendSolResult.signature);
            await clientForDb.query('COMMIT');

            await safeSendMessage(referrerUserId,
                `🎁 *Referral Bonus Paid, ${playerRefForNotif}!* 🎁\n\n` +
                `Your referral commission of *${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}* has been sent to your linked wallet: \`${escapeMarkdownV2(recipientAddress)}\`.\n` + // Escaped .
                `🧾 Transaction ID: \`${escapeMarkdownV2(sendSolResult.signature)}\`\n\nThanks for spreading the word about ${escapeMarkdownV2(BOT_NAME)}!`, // Escaped !
                { parse_mode: 'MarkdownV2' }
            );
            return; 
        } else {
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure for referral payout.';
            console.error(`❌ ${logPrefix} sendSol FAILED for referral payout ID ${referralId}. Reason: ${sendErrorMsg}`);
            await updateReferralPayoutStatusDB(clientForDb, referralId, 'failed', null, sendErrorMsg.substring(0, 250));
            await clientForDb.query('COMMIT'); // Commit the 'failed' status

            await safeSendMessage(referrerUserId,
                `⚠️ *Referral Payout Issue* ⚠️\n\n${playerRefForNotif}, we encountered an issue sending your referral reward of *${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}* (Details: \`${escapeMarkdownV2(sendErrorMsg)}\`). Please ensure your linked wallet is correct or contact support. This payout will be re-attempted if possible, or an admin will review.`, // Escaped . and ()
                {parse_mode: 'MarkdownV2'}
            );
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 *REFERRAL PAYOUT FAILED* 🚨\nReferrer: ${playerRefForNotif} (\`${escapeMarkdownV2(referrerUserId)}\`)\nPayout ID: \`${referralId}\`\nAmount: \`${escapeMarkdownV2(formatCurrency(amountToPay, 'SOL'))}\`\n*Error:* \`${escapeMarkdownV2(sendErrorMsg)}\`.`, {parse_mode:'MarkdownV2'});
            }
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true;
            throw errorToThrowForRetry;
        }
    } catch (jobError) { // Catches errors from sendSol or DB ops after sendSol
        if(clientForDb && clientForDb.release) { // If it was connected for the second DB op
            try { await clientForDb.query('ROLLBACK');} catch(rbErr) {console.error(`${logPrefix} Final rollback error on jobError: ${rbErr.message}`);}
        } else if (!clientForDb) { // If sendSol failed and we didn't even get to re-connect clientForDb
            // Ensure status is marked as failed if not already done.
             const updateClient = await pool.connect();
             try {
                 const currentDetailsAfterJobError = await getReferralDetailsDB(referralId, updateClient);
                 if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'paid_out' && currentDetailsAfterJobError.status !== 'failed') {
                     await updateReferralPayoutStatusDB(updateClient, referralId, 'failed', null, `Job error (non-retryable): ${String(jobError.message || jobError)}`.substring(0,250));
                 }
             } catch (finalStatusUpdateError) { console.error(`${logPrefix} Failed to mark referral as 'failed': ${finalStatusUpdateError.message}`);}
             finally { updateClient.release(); }
        }

        console.error(`❌ ${logPrefix} Error during referral payout job ID ${referralId}: ${jobError.message}`, jobError.stack?.substring(0,500));
        if (jobError.isRetryable === undefined) jobError.isRetryable = sendSolResult.isRetryable || false;
        throw jobError; 
    } finally {
        if (clientForDb && clientForDb.release) clientForDb.release();
    }
}

// --- End of Part P4 ---
