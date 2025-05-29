// --- Start of Part 1 (REVISED - For New Dice Escalator Game IDs and Constants & PVP_TURN_TIMEOUT_MS & MIN_WITHDRAWAL_USD) ---
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
  'PVP_TURN_TIMEOUT_MS': '60000', // 1 minute for a player's turn in PvP games
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
  'MINES_MIN_MINES': '1',
  'MINES_MAX_MINES_PERCENT': '0.6', // Max 60% of cells can be mines (for validation)
  'MINES_EDIT_THROTTLE_MS': '1200',
};

const PAYMENT_ENV_DEFAULTS = {
  'SOLANA_RPC_URL': 'https://api.mainnet-beta.solana.com/',
  'RPC_URLS': '',
  'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60',
  'DEPOSIT_CONFIRMATIONS': 'confirmed',
  'WITHDRAWAL_FEE_LAMPORTS': '10000',
  // 'MIN_WITHDRAWAL_LAMPORTS': '10000000', // This fixed SOL value is no longer the primary determinant for min withdrawal.
                                            // The system will now primarily use MIN_WITHDRAWAL_USD.
                                            // If you keep this in your .env, it will still be loaded into process.env below.
  'MIN_WITHDRAWAL_USD': '50.00',         // NEW: Minimum withdrawal based on USD value.
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

// Ensure MIN_WITHDRAWAL_LAMPORTS is still part of OPTIONAL_ENV_DEFAULTS if users might have it in their .env
// It just won't be used by the primary withdrawal minimum logic.
// If it's NOT in OPTIONAL_ENV_DEFAULTS and also not in .env, process.env.MIN_WITHDRAWAL_LAMPORTS will be undefined.
const tempOptionalDefaults = { ...CASINO_ENV_DEFAULTS, ...PAYMENT_ENV_DEFAULTS };
if (!tempOptionalDefaults.hasOwnProperty('MIN_WITHDRAWAL_LAMPORTS')) { // If it was removed from PAYMENT_ENV_DEFAULTS strictly
    tempOptionalDefaults['MIN_WITHDRAWAL_LAMPORTS'] = '0'; // Provide a base default if it was fully removed
}
const OPTIONAL_ENV_DEFAULTS = tempOptionalDefaults;


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

// --- GAME_IDS Constant (UPDATED for Coinflip & RPS) ---
const GAME_IDS = {
    COINFLIP: 'coinflip', 
    COINFLIP_UNIFIED_OFFER: 'coinflip_unified_offer',
    COINFLIP_PVB: 'coinflip_pvb',
    COINFLIP_PVP: 'coinflip_pvp',
    RPS: 'rps', 
    RPS_UNIFIED_OFFER: 'rps_unified_offer',
    RPS_PVB: 'rps_pvb',
    RPS_PVP: 'rps_pvp',
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
    DIRECT_PVP_CHALLENGE: 'direct_pvp_challenge',
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
const MINES_EDIT_THROTTLE_MS = parseInt(process.env.MINES_EDIT_THROTTLE_MS, 10) || 1200;

// MINES GAME CONSTANTS (<<<<< ADD/ENSURE THESE ARE HERE AND CORRECT >>>>>)
const TILE_EMOJI_HIDDEN = '❓';
const TILE_EMOJI_GEM = '💎';
const TILE_EMOJI_MINE = '💣';
const TILE_EMOJI_EXPLOSION = '💥'; // For when a mine is hit


const MINES_DIFFICULTY_CONFIG = {
    easy: {
        rows: 5, cols: 5, mines: 3, label: "Easy (5x5, 3 Mines)",
        multipliers: [ 0, 1.08, 1.18, 1.29, 1.42, 1.55, 1.70, 1.88, 2.08, 2.30, 2.55,
                       2.85, 3.20, 3.60, 4.05, 4.50, 5.00, 6.00, 7.50, 10.00, 15.00, 25.00, 50.00 ]
    },
    medium: {
        rows: 5, cols: 5, mines: 5, label: "Medium (5x5, 5 Mines)",
        multipliers: [ 0, 1.12, 1.28, 1.47, 1.70, 1.98, 2.30, 2.70, 3.15, 3.70, 4.35,
                       5.10, 6.00, 7.10, 8.50, 10.50, 13.00, 16.50, 22.00, 30.00, 75.00 ]
    },
    hard:   {
        rows: 5, cols: 5, mines: 7, label: "Hard (5x5, 7 Mines)",
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
if (!REFERRAL_PAYOUT_KEYPAIR) { 
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
if (combinedRpcEndpointsForConnection.length === 0) { 
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
const PVP_TURN_TIMEOUT_MS = parseInt(process.env.PVP_TURN_TIMEOUT_MS, 10); 
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

// MIN_WITHDRAWAL_LAMPORTS_LEGACY_REFERENCE is for reference or non-critical uses if `process.env.MIN_WITHDRAWAL_LAMPORTS` is still set.
// The primary withdrawal logic will use MIN_WITHDRAWAL_USD_val.
const MIN_WITHDRAWAL_LAMPORTS_LEGACY_REFERENCE = BigInt(process.env.MIN_WITHDRAWAL_LAMPORTS || '0'); // Default to '0' if not set to avoid BigInt errors
const MIN_WITHDRAWAL_USD_val = parseFloat(process.env.MIN_WITHDRAWAL_USD); // Parsed from new env var


// Critical Configuration Validations
if (!BOT_TOKEN) { console.error("🚨 FATAL ERROR: BOT_TOKEN is not defined. Bot cannot start."); process.exit(1); }
if (!DATABASE_URL) { console.error("🚨 FATAL ERROR: DATABASE_URL is not defined. Cannot connect to PostgreSQL."); process.exit(1); }
if (!DEPOSIT_MASTER_SEED_PHRASE) { console.error("🚨 FATAL ERROR: DEPOSIT_MASTER_SEED_PHRASE is not defined. Payment system cannot generate deposit addresses."); process.exit(1); }

if (isNaN(PVP_TURN_TIMEOUT_MS) || PVP_TURN_TIMEOUT_MS <= 0) {
    console.warn(`⚠️ WARNING: PVP_TURN_TIMEOUT_MS ('${process.env.PVP_TURN_TIMEOUT_MS}') is not a valid positive number. PvP turn timeouts may not function as expected (will be very short or default).`);
}

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
// NEW VALIDATION FOR MIN_WITHDRAWAL_USD_val
if (isNaN(MIN_WITHDRAWAL_USD_val) || MIN_WITHDRAWAL_USD_val <= 0) {
    console.error(`🚨 FATAL ERROR: MIN_WITHDRAWAL_USD ('${process.env.MIN_WITHDRAWAL_USD}') must be a positive number. Bot cannot start.`);
    process.exit(1);
}
// Optional: Warning if legacy MIN_WITHDRAWAL_LAMPORTS is problematic but not fatal for this specific check
if (MIN_WITHDRAWAL_LAMPORTS_LEGACY_REFERENCE < 0n) {
    console.warn(`⚠️ WARNING: MIN_WITHDRAWAL_LAMPORTS ('${process.env.MIN_WITHDRAWAL_LAMPORTS}') is negative. This value is legacy for withdrawal minimums but should ideally be non-negative if set.`);
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

// --- Log Key Configurations (PVP_TURN_TIMEOUT_MS added, Min Withdrawal line UPDATED) ---
console.log(`--- ⚙️ Key Game & Bot Configurations Loaded ---
  Dice Escalator (PvB): Target Jackpot Score: ${TARGET_JACKPOT_SCORE}, Player Bust On: ${DICE_ESCALATOR_BUST_ON}, Jackpot Fee: ${JACKPOT_CONTRIBUTION_PERCENT * 100}%
  Dice 21 (Blackjack): Target Score: ${DICE_21_TARGET_SCORE}, Bot Stand: ${DICE_21_BOT_STAND_SCORE}
  Mines Config (Example 'easy'): Grid ${MINES_DIFFICULTY_CONFIG.easy.rows}x${MINES_DIFFICULTY_CONFIG.easy.cols}, Mines: ${MINES_DIFFICULTY_CONFIG.easy.mines}
  Bet Limits (USD): $${MIN_BET_USD_val.toFixed(2)} - $${MAX_BET_USD_val.toFixed(2)} (Lamports Ref: ${formatLamportsToSolStringForLog(MIN_BET_AMOUNT_LAMPORTS_config)} SOL - ${formatLamportsToSolStringForLog(MAX_BET_AMOUNT_LAMPORTS_config)} SOL)
  Default Starting Credits: ${formatLamportsToSolStringForLog(DEFAULT_STARTING_BALANCE_LAMPORTS)} SOL
  Command Cooldown: ${COMMAND_COOLDOWN_MS / 1000}s, Game Join Timeout (Offers): ${JOIN_GAME_TIMEOUT_MS / 1000 / 60}min
  PvP Turn Timeout: ${PVP_TURN_TIMEOUT_MS / 1000}s
  Min Withdrawal: Approx. $${MIN_WITHDRAWAL_USD_val.toFixed(2)} USD (actual SOL equivalent varies based on current price), Fee: ${formatLamportsToSolStringForLog(WITHDRAWAL_FEE_LAMPORTS)} SOL
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
  return String(text).replace(/([_*\[\]()~`>#+\-=|{}\\])/g, '\\$1');
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
    } else if (finalOptions.parse_mode !== 'HTML' && messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) { 
        const ellipsisPlain = `... (message truncated by ${BOT_NAME})`;
        const truncateAt = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsisPlain.length); 
        messageToSend = messageToSend.substring(0, truncateAt) + ellipsisPlain;
    } else if (finalOptions.parse_mode === 'HTML' && messageToSend.length > 4096) { 
        const ellipsisBase = ` ... (<i>message truncated by ${escapeHTML(BOT_NAME)}</i>)`; // Assuming escapeHTML is defined
        const truncateAt = Math.max(0, 4096 - ellipsisBase.length);
        messageToSend = messageToSend.substring(0, truncateAt) + ellipsisBase;
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
            if ((finalOptions.parse_mode === 'MarkdownV2' || finalOptions.parse_mode === 'HTML') && (errorDescription.includes("can't parse entities") || errorDescription.includes("bad request"))) {
                console.warn(`${LOG_PREFIX_SSM} ${finalOptions.parse_mode} parse error detected by API: "${error.response.body.description}". Attempting plain text fallback for original text.`);
                try {
                    let plainTextFallbackOptions = { ...options };
                    delete plainTextFallbackOptions.parse_mode;

                    let plainTextForFallback = text; 
                    if (plainTextForFallback.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) { 
                        const ellipsisPlainFallback = `... (message truncated by ${BOT_NAME}, original was parse error)`;
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

const SLOT_PAYOUTS = { // This is the version they last finalized
    64: { multiplier: 25, symbols: "7️⃣7️⃣7️⃣", label: "TRIPLE SEVEN!" },
    1:  { multiplier: 15, symbols: "BAR-BAR-BAR", label: "Triple Bar!" },
    22: { multiplier: 10, symbols: "🍋🍋🍋", label: "Triple Lemon!" },
    43: { multiplier: 5, symbols: "🍒🍒🍒", label: "Triple Cherry!" } // Key 43 (visual Bell) pays as Cherry
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

// From Part 2: Database Schema Initialization & Core User Management

async function getNextAddressIndexForUserDB(userId, dbClient = pool) {
    const stringUserId = String(userId);
    const LOG_PREFIX_GNAI = `[NextAddrIdx TG:${stringUserId}]`;
    try {
        // SQL query (ensure this is also clean from previous fix)
        const query = `SELECT derivation_path FROM user_deposit_wallets WHERE user_telegram_id = $1 ORDER BY created_at DESC;`;

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
        // CORRECTED LINE: Ensure this uses backticks (`) for the template literal
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
// index.js - Part 3: Telegram Helpers, Currency Formatting & Basic Game Utilities (with Group Session Management)
//---------------------------------------------------------------------------
// Assumed escapeMarkdownV2, LAMPORTS_PER_SOL, SOL_DECIMALS, getSolUsdPrice,
// convertLamportsToUSDString, crypto (module), BOT_NAME are available from Part 1.
// Assumed groupGameSessions and activeGames (Maps) are defined globally (e.g., in Part 1).

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

// Helper function to escape characters for HTML content
function escapeHTML(text) {
    if (text === null || typeof text === 'undefined') return '';
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
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

// --- NEW Group Game Session Management Functions ---

/**
 * Retrieves or creates a game session for a specific group chat.
 * Interacts with the global `groupGameSessions` Map.
 * @param {string} chatId - The ID of the Telegram group chat.
 * @param {string | null} chatTitle - The title of the group chat (can be null if just updating).
 * @returns {Promise<object>} The group session object.
 */
async function getGroupSession(chatId, chatTitle = null) {
    const stringChatId = String(chatId);
    if (groupGameSessions.has(stringChatId)) {
        const session = groupGameSessions.get(stringChatId);
        if (chatTitle && !session.title) { // Update title if it was missing
            session.title = chatTitle;
            groupGameSessions.set(stringChatId, session);
        }
        return session;
    } else {
        const newSession = {
            id: stringChatId,
            title: chatTitle || `Group Chat ${stringChatId}`, // Default title if not provided
            currentGameId: null,         // ID of the currently active game or offer in this chat
            activeGameType: null,      // Type of the active game/offer (e.g., GAME_IDS.COINFLIP_UNIFIED_OFFER)
            activeBetAmount: 0n,       // Bet amount of the current game/offer
            lastActivity: Date.now(),  // Timestamp of the last relevant activity in this session
            activePlayers: {},         // Object to store players involved in the current game if needed (e.g., { userId: playerData })
            // You might add other session-specific fields here if needed later
        };
        groupGameSessions.set(stringChatId, newSession);
        console.log(`[GetGroupSession] New session created for chat ID: ${stringChatId}`);
        return newSession;
    }
}

/**
 * Updates the details of the active game in a group session.
 * @param {string} chatId - The ID of the Telegram group chat.
 * @param {string | null} gameId - The ID of the new active game/offer, or null to clear.
 * @param {string | null} gameType - The type of the new active game/offer, or null to clear.
 * @param {bigint | number | null} betAmount - The bet amount for the new game/offer, or null to clear.
 */
async function updateGroupGameDetails(chatId, gameId, gameType, betAmount) {
    const stringChatId = String(chatId);
    const session = await getGroupSession(stringChatId, null); // Title isn't strictly necessary for update

    session.currentGameId = gameId;
    session.activeGameType = gameType;
    session.activeBetAmount = gameId && betAmount !== null ? BigInt(betAmount) : 0n; // Store as BigInt if game is active
    session.lastActivity = Date.now();

    // If gameId is null, it implies the game is over, so clear active players for this session.
    if (!gameId) {
        session.activePlayers = {};
    }

    groupGameSessions.set(stringChatId, session);
    console.log(`[UpdateGroupGameDetails] Updated session for chat ${stringChatId}: GameID=${gameId}, Type=${gameType}, Bet=${session.activeBetAmount}`);
}

// --- End of Part 3 ---
// --- Start of Part 4 --- (Ensure this is placed correctly in your file structure)
// index.js - Part 4: Simplified Game Logic (Enhanced)
//---------------------------------------------------------------------------
// Assumes rollDie (from Part 3) is available.
// Assumes escapeHTML (from Part 3) is available if dynamic names needed further escaping,
// but player1Name/player2Name are expected to be pre-escaped HTML.

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
const RPS_EMOJIS = { // Emojis are generally safe for HTML/MarkdownV2
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
 * @param {string} [player1NameHtml="Player 1"] - HTML-safe name of player 1.
 * @param {string} [player2NameHtml="Player 2"] - HTML-safe name of player 2.
 * @returns {object} A detailed result object including:
 * - result: 'win_player1', 'win_player2', 'draw', or 'error'.
 * - description: An HTML-formatted string.
 * - player1: { choice, emoji, choiceFormatted }.
 * - player2: { choice, emoji, choiceFormatted }.
 */
function determineRPSOutcome(player1ChoiceKey, player2ChoiceKey, player1NameHtml = "Player 1", player2NameHtml = "Player 2") {
  const LOG_PREFIX_RPS_OUTCOME = "[RPS_Outcome_V3_HTML]";

  const p1c = String(player1ChoiceKey).toLowerCase();
  const p2c = String(player2ChoiceKey).toLowerCase();

  if (!Object.values(RPS_CHOICES).includes(p1c) || !Object.values(RPS_CHOICES).includes(p2c)) {
    console.warn(`${LOG_PREFIX_RPS_OUTCOME} Invalid choices: P1='${player1ChoiceKey}', P2='${player2ChoiceKey}'.`);
    return {
        result: 'error',
        description: "An internal error occurred due to invalid RPS choices. Please try again.",
        player1: { choice: player1ChoiceKey, emoji: '❓', choiceFormatted: 'Invalid' },
        player2: { choice: player2ChoiceKey, emoji: '❓', choiceFormatted: 'Invalid' }
    };
  }

  const p1Emoji = RPS_EMOJIS[p1c];
  const p2Emoji = RPS_EMOJIS[p2c];
  const p1ChoiceFormatted = escapeHTML(p1c.charAt(0).toUpperCase() + p1c.slice(1)); // Escape for safety if used in HTML
  const p2ChoiceFormatted = escapeHTML(p2c.charAt(0).toUpperCase() + p2c.slice(1)); // Escape for safety

  let resultDescription;
  let outcome;

  if (p1c === p2c) {
    outcome = 'draw';
    resultDescription = `${p1Emoji} ${p1ChoiceFormatted} clashes with ${p2Emoji} ${p2ChoiceFormatted}! It's a <b>Draw</b>!`;
  } else if (RPS_RULES[p1c]?.beats === p2c) {
    outcome = 'win_player1';
    // player1NameHtml is expected to be already HTML-safe (e.g., from getPlayerDisplayReference + escapeHTML)
    resultDescription = `${p1Emoji} ${p1ChoiceFormatted} <b>${escapeHTML(RPS_RULES[p1c].verb)}</b> ${p2Emoji} ${p2ChoiceFormatted}! ${player1NameHtml} <b>claims victory</b>!`;
  } else {
    outcome = 'win_player2';
    // player2NameHtml is expected to be already HTML-safe
    resultDescription = `${p2Emoji} ${p2ChoiceFormatted} <b>${escapeHTML(RPS_RULES[p2c]?.verb || 'outplays')}</b> ${p1Emoji} ${p1ChoiceFormatted}! ${player2NameHtml} <b>is the winner</b>!`;
  }

  return {
    result: outcome,
    description: resultDescription, // HTML formatted description
    player1: { choice: p1c, emoji: p1Emoji, choiceFormatted: p1ChoiceFormatted },
    player2: { choice: p2c, emoji: p2Emoji, choiceFormatted: p2ChoiceFormatted }
  };
}

// --- End of Part 4 ---
// --- Start of REVISED Part 5a, Section 3: Upgraded Coinflip & RPS Game Logic ---
// This section contains the complete, new game logic for Coinflip and RPS,
// intended to replace any previous Coinflip/RPS logic in your project.

// --- Start of REVISED Coinflip Game Logic & Handlers (Unified Offer, HTML, New Mechanics) ---

// --- Constants for Coinflip ---
const COINFLIP_CHOICE_HEADS = 'heads';
const COINFLIP_CHOICE_TAILS = 'tails';
const COIN_EMOJI_DISPLAY = '🪙'; // Primary coin emoji for display
const COIN_FLIP_ANIMATION_FRAMES = ['🌕', '🌖', '🌗', '🌘', '🌑', '🌒', '🌓', '🌔']; // Simple animation frames
const COIN_FLIP_ANIMATION_INTERVAL_MS = 250;
const COIN_FLIP_ANIMATION_DURATION_MS = 2000; // How long the animation message stays before revealing result
const COIN_FLIP_ANIMATION_STEPS = Math.floor(COIN_FLIP_ANIMATION_DURATION_MS / COIN_FLIP_ANIMATION_INTERVAL_MS);

// --- Coinflip Unified Offer Command ---
async function handleStartCoinflipUnifiedOfferCommand(msg, betAmountLamports, targetUsernameRaw = null) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const logPrefix = `[CF_OfferOrDirect UID:${userId} CH:${chatId}]`; 

    if (chatType === 'private') {
        await safeSendMessage(chatId, `🪙 The Coinflip arena awaits in <b>group chats</b>! Please use <code>/coinflip &lt;bet&gt; [@username]</code> there to challenge others or the bot.`, { parse_mode: 'HTML' });
        return;
    }

    let initiatorUserObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!initiatorUserObj) {
        await safeSendMessage(chatId, "⚠️ Error fetching your player profile. Please try <code>/start</code> again with me first.", { parse_mode: 'HTML' });
        return;
    }
    const initiatorPlayerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        await safeSendMessage(chatId, `💰 ${initiatorPlayerRefHTML}, your balance is too low for a <b>${betDisplayUSD_HTML}</b> Coinflip! You need approx. <b>${escapeHTML(await formatBalanceForDisplay(needed, 'USD'))}</b> more. Top up?`, {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: "💸 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameSession = await getGroupSession(chatId, msg.chat.title || `Group Chat ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        if ( (existingGame.type.includes('_offer') && existingGame.status === 'pending_offer') || 
             (existingGame.type === GAME_IDS.DIRECT_PVP_CHALLENGE && existingGame.status === 'pending_direct_challenge_response') ||
             (existingGame.type.includes('_pvp') && !existingGame.status?.startsWith('game_over_')) ||
             (existingGame.type === GAME_IDS.MINES && existingGame.status !== 'game_over_mine_hit' && existingGame.status !== 'game_over_cashed_out')
           ) {
            await safeSendMessage(chatId, `⏳ Hold your coins, ${initiatorPlayerRefHTML}! An interactive game or challenge (<code>${escapeHTML(existingGame.type.replace(/_/g, " "))}</code>) is already active. Please wait.`, { parse_mode: 'HTML' });
            return;
        }
    }

    let targetUserObject = null;
    if (targetUsernameRaw) {
        targetUserObject = await findRecipientUser(targetUsernameRaw);
        if (!targetUserObject || !targetUserObject.telegram_id) { 
            await safeSendMessage(chatId, `😕 Player ${escapeHTML(targetUsernameRaw)} not found or has an invalid ID. Cannot create a Coinflip challenge. Please ensure they have started a chat with me first.`, { parse_mode: 'HTML' });
            return; 
        }
        if (String(targetUserObject.telegram_id) === userId) {
            await safeSendMessage(chatId, `😅 You can't challenge yourself to a Coinflip duel, ${initiatorPlayerRefHTML}!`, { parse_mode: 'HTML' });
            return;
        }
    }

    if (targetUserObject && targetUserObject.telegram_id) { 
        // --- DIRECT PvP CHALLENGE FLOW for Coinflip ---
        console.log(`${logPrefix} Initiating DIRECT Coinflip challenge to User ID: ${targetUserObject.telegram_id} (@${targetUserObject.username || 'N/A'})`);
        const targetPlayerRefHTML = escapeHTML(getPlayerDisplayReference(targetUserObject));
        const offerId = generateGameId(`dcf_${userId.slice(-3)}_${String(targetUserObject.telegram_id).slice(-3)}`); 

        const groupChallengeTextHTML = `Hey ${targetPlayerRefHTML}❗\n\n${initiatorPlayerRefHTML} has challenged you to a <b>Coinflip</b> duel for <b>${betDisplayUSD_HTML}</b>!`;
        
        const groupChallengeKeyboard = { 
            inline_keyboard: [
                [{ text: "✅ Accept Challenge", callback_data: `dir_chal_acc:${offerId}` }],
                [{ text: "❌ Decline Challenge", callback_data: `dir_chal_dec:${offerId}` }],
                [{ text: "🚫 Withdraw My Challenge", callback_data: `dir_chal_can:${offerId}` }]
            ]
        };

        const sentGroupMessage = await safeSendMessage(chatId, groupChallengeTextHTML, { parse_mode: 'HTML', reply_markup: groupChallengeKeyboard });

        if (!sentGroupMessage || !sentGroupMessage.message_id) {
            console.error(`${logPrefix} Failed to send direct Coinflip challenge message for offer ${offerId}.`);
            await safeSendMessage(chatId, `⚙️ Oops! Couldn't send your Coinflip challenge to ${targetPlayerRefHTML}. Please try again.`, { parse_mode: 'HTML' });
            return;
        }
        const offerMessageIdInGroup = String(sentGroupMessage.message_id);
        const groupNameHTML = escapeHTML(msg.chat.title || "the group");

        const dmNotificationTextHTML = `🔔 Challenge Alert!\n\nHi ${targetPlayerRefHTML},\n${initiatorPlayerRefHTML} has challenged you to a <b>Coinflip</b> game for <b>${betDisplayUSD_HTML}</b> in the group "<b>${groupNameHTML}</b>".\n\nPlease head to that group to accept or decline the challenge.`;
        
        const dmSent = await safeSendMessage(targetUserObject.telegram_id, dmNotificationTextHTML, { parse_mode: 'HTML' });
        if (!dmSent) {
            console.warn(`${logPrefix} Failed to send DM notification for direct challenge to target ${targetUserObject.telegram_id}. Offer still posted in group.`);
            await safeSendMessage(chatId, `ℹ️ ${initiatorPlayerRefHTML}, your challenge to ${targetPlayerRefHTML} is posted! Note: They might not receive a DM if they haven't interacted with me before.`, { parse_mode: 'HTML'});
        }

        const directOfferData = {
            type: GAME_IDS.DIRECT_PVP_CHALLENGE, 
            offerId: offerId, 
            gameId: offerId, 
            initiatorId: userId,
            initiatorUserObj: initiatorUserObj, 
            initiatorMentionHTML: initiatorPlayerRefHTML,
            targetUserId: String(targetUserObject.telegram_id),
            targetUserObj: targetUserObject, 
            targetUserMentionHTML: targetPlayerRefHTML,
            betAmount: betAmountLamports,
            originalGroupId: chatId,
            offerMessageIdInGroup: offerMessageIdInGroup,
            chatTitle: msg.chat.title || `Group Chat ${chatId}`, 
            status: 'pending_direct_challenge_response',
            gameToStart: GAME_IDS.COINFLIP_PVP, 
            creationTime: Date.now()
        };
        activeGames.set(offerId, directOfferData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.DIRECT_PVP_CHALLENGE, betAmountLamports);
        console.log(`${logPrefix} Direct Coinflip challenge offer ${offerId} created and stored.`);

        setTimeout(async () => {
            const timedOutOffer = activeGames.get(offerId);
            if (timedOutOffer && timedOutOffer.status === 'pending_direct_challenge_response' && timedOutOffer.type === GAME_IDS.DIRECT_PVP_CHALLENGE) {
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null);
                const gameNameForTimeout = "Coinflip"; 
                const timeoutBetDisplay = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                const timeoutMsgHTML = `⏳ The ${gameNameForTimeout} challenge from ${timedOutOffer.initiatorMentionHTML} to ${timedOutOffer.targetUserMentionHTML} for <b>${timeoutBetDisplay}</b> has expired unanswered.`;
                if (bot && timedOutOffer.offerMessageIdInGroup) {
                    await bot.editMessageText(timeoutMsgHTML, {
                        chat_id: timedOutOffer.originalGroupId, message_id: Number(timedOutOffer.offerMessageIdInGroup),
                        parse_mode: 'HTML', reply_markup: {} 
                    }).catch(e => { 
                        console.warn(`${logPrefix} Failed to edit expired direct Coinflip challenge message ${timedOutOffer.offerMessageIdInGroup}: ${e.message}. Sending new.`);
                        safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                    });
                } else { 
                    safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                }
                await safeSendMessage(timedOutOffer.initiatorId, `⏳ Your Coinflip challenge to ${timedOutOffer.targetUserMentionHTML} in group "${escapeHTML(timedOutOffer.chatTitle)}" has expired.`, { parse_mode: 'HTML' });
            }
        }, JOIN_GAME_TIMEOUT_MS);

    } else {
        // --- EXISTING COINFLIP UNIFIED OFFER FLOW (No valid targetUserObject specified) ---
        console.log(`${logPrefix} Initiating UNIFIED Coinflip offer (no target user or target was invalid).`);
        const offerId = generateGameId(GAME_IDS.COINFLIP_UNIFIED_OFFER); 
        const offerData = { 
            type: GAME_IDS.COINFLIP_UNIFIED_OFFER,
            gameId: offerId, 
            chatId: chatId,
            chatType: chatType,
            initiatorId: userId,
            initiatorMentionHTML: initiatorPlayerRefHTML, 
            initiatorUserObj: initiatorUserObj,
            betAmount: betAmountLamports,
            status: 'pending_offer', 
            creationTime: Date.now(),
            offerMessageId: null 
        };
        activeGames.set(offerId, offerData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.COINFLIP_UNIFIED_OFFER, betAmountLamports);

        const offerMessageTextHTML = `👑 ${COIN_EMOJI_DISPLAY} <b>A Coinflip Challenge Has Been Issued!</b> ${COIN_EMOJI_DISPLAY} 👑\n\n` +
            `High roller ${initiatorPlayerRefHTML} has bravely wagered <b>${betDisplayUSD_HTML}</b> on the toss of a coin!\n\n` +
            `<b>Will you face the Bot Dealer, or will another player accept the PvP challenge?</b>\n\n` +
            `<i>This electrifying offer expires in ${JOIN_GAME_TIMEOUT_MS / 1000 / 60} minutes! Choose wisely!</i>`;

        const offerKeyboard = { 
            inline_keyboard: [
                [{ text: "🤖 Challenge Bot Dealer", callback_data: `cf_accept_bot:${offerId}` }],
                [{ text: "⚔️ Accept PvP Challenge", callback_data: `cf_accept_pvp:${offerId}` }],
                [{ text: "🚫 Withdraw My Challenge", callback_data: `cf_cancel_offer:${offerId}` }]
            ]
        };

        const sentMessage = await safeSendMessage(chatId, offerMessageTextHTML, { parse_mode: 'HTML', reply_markup: offerKeyboard });

        if (sentMessage?.message_id) {
            const currentOffer = activeGames.get(offerId);
            if(currentOffer) {
                currentOffer.offerMessageId = String(sentMessage.message_id);
                activeGames.set(offerId, currentOffer); 
            } else {
                 if (bot) await bot.deleteMessage(chatId, sentMessage.message_id).catch(()=>{});
            }

            setTimeout(async () => {
                const timedOutOffer = activeGames.get(offerId);
                if (timedOutOffer && timedOutOffer.status === 'pending_offer' && timedOutOffer.type === GAME_IDS.COINFLIP_UNIFIED_OFFER) {
                    activeGames.delete(offerId);
                    await updateGroupGameDetails(chatId, null, null, null);
                    if (timedOutOffer.offerMessageId && bot) {
                        const timeoutBetDisplayUnified = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                        await bot.editMessageText(`⏳ The Coinflip offer by ${timedOutOffer.initiatorMentionHTML} for <b>${timeoutBetDisplayUnified}</b> has expired unanswered.`, {
                            chat_id: timedOutOffer.chatId, message_id: Number(timedOutOffer.offerMessageId), parse_mode: 'HTML', reply_markup: {}
                        }).catch(e => console.warn(`${logPrefix} Failed to edit expired CF unified offer: ${e.message}`));
                    }
                }
            }, JOIN_GAME_TIMEOUT_MS);
        } else {
            console.error(`${logPrefix} Failed to send Coinflip unified offer message for ${offerId}.`);
            activeGames.delete(offerId);
            await updateGroupGameDetails(chatId, null, null, null);
            await safeSendMessage(chatId, `⚙️ Oops! Couldn't create your Coinflip offer. Please try again.`, { parse_mode: 'HTML' });
        }
    }
}

// --- Coinflip Offer Callback Handlers ---
async function handleCoinflipAcceptBotGameCallback(offerId, userWhoClicked, originalOfferMessageId, originalChatId, originalChatType, callbackQueryId) {
    const userId = String(userWhoClicked.id || userWhoClicked.telegram_id);
    const logPrefix = `[CF_AcceptBotCB OfferID:${offerId} UID:${userId}]`;
    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.COINFLIP_UNIFIED_OFFER || offerData.status !== 'pending_offer') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Coinflip offer is no longer valid.", show_alert: true }).catch(() => {});
        if (originalOfferMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalOfferMessageId) }).catch(() => {});
        return;
    }
    if (offerData.initiatorId !== userId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the one who made the offer can play against the Bot!", show_alert: true }).catch(() => {});
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: `🪙 ${COIN_EMOJI_DISPLAY} Starting your Coinflip duel with the Bot Dealer...` }).catch(() => {});
    await startCoinflipPvBGame(originalChatId, offerData.initiatorUserObj, offerData.betAmount, offerData.offerMessageId, offerId);
}

async function handleCoinflipAcceptPvPChallengeCallback(offerId, joinerUserObjFull, originalOfferMessageId, originalChatId, originalChatType, callbackQueryId) {
    const joinerId = String(joinerUserObjFull.id || joinerUserObjFull.telegram_id);
    const logPrefix = `[CF_AcceptPvPCB OfferID:${offerId} JoinerID:${joinerId}]`;
    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.COINFLIP_UNIFIED_OFFER || offerData.status !== 'pending_offer') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Coinflip PvP offer has vanished!", show_alert: true }).catch(() => {});
        if (originalOfferMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalOfferMessageId) }).catch(() => {});
        return;
    }
    if (offerData.initiatorId === joinerId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "You can't accept your own Coinflip challenge for PvP!", show_alert: true }).catch(() => {});
        return;
    }
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount, 'USD'));
    if (BigInt(joinerUserObjFull.balance) < offerData.betAmount) {
        await bot.answerCallbackQuery(callbackQueryId, { text: `Your funds are too low for this ${betDisplayHTML} duel!`, show_alert: true }).catch(() => {});
        const needed = offerData.betAmount - BigInt(joinerUserObjFull.balance);
        await safeSendMessage(originalChatId, `💰 Oops, ${escapeHTML(getPlayerDisplayReference(joinerUserObjFull))}! Your balance is short by ~<b>${escapeHTML(await formatBalanceForDisplay(needed, 'USD'))}</b> to join this <b>${betDisplayHTML}</b> Coinflip.`, {
             parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: "💸 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }
     const currentInitiatorUserObj = await getOrCreateUser(offerData.initiatorId);
    if (!currentInitiatorUserObj || BigInt(currentInitiatorUserObj.balance) < offerData.betAmount) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Initiator can't cover the bet. Offer cancelled.", show_alert:true}).catch(()=>{});
        if (offerData.offerMessageId && bot) {
             await bot.editMessageText(`⚠️ <b>Offer Auto-Cancelled</b><br>The Coinflip offer by ${offerData.initiatorMentionHTML} for <b>${betDisplayHTML}</b> was cancelled as their balance is no longer sufficient.`, {
                 chat_id: originalChatId, message_id: Number(offerData.offerMessageId), parse_mode: 'HTML', reply_markup: {inline_keyboard:[]}
             }).catch(()=>{});
        }
        activeGames.delete(offerId);
        await updateGroupGameDetails(originalChatId, null, null, null);
        return;
    }

    await bot.answerCallbackQuery(callbackQueryId, { text: "⚔️ PvP Challenge Accepted! The coin is ready to be flipped..." }).catch(() => {});
    await startCoinflipPvPGame(offerData, joinerUserObjFull, offerData.offerMessageId);
}

async function handleCoinflipCancelOfferCallback(offerId, userWhoClicked, originalOfferMessageId, originalChatId, callbackQueryId) {
    const userId = String(userWhoClicked.id || userWhoClicked.telegram_id);
    const logPrefix = `[CF_CancelOfferCB OfferID:${offerId} UID:${userId}]`;
    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.COINFLIP_UNIFIED_OFFER || offerData.status !== 'pending_offer') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Offer already gone or actioned!", show_alert: false }).catch(() => {});
        if (originalOfferMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalOfferMessageId) }).catch(() => {});
        return;
    }
    if (offerData.initiatorId !== userId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the offer initiator can cancel.", show_alert: true }).catch(() => {});
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: "Coinflip offer withdrawn." }).catch(() => {});
    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null);
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount, 'USD'));
    if (originalOfferMessageId && bot) {
        await bot.editMessageText(`🚫 <b>Offer Retracted!</b>\nThe Coinflip challenge by ${offerData.initiatorMentionHTML} (wager: <b>${betDisplayHTML}</b>) has been cancelled.`, {
            chat_id: originalChatId, message_id: Number(originalOfferMessageId), parse_mode: 'HTML', reply_markup: {}
        }).catch(async (e) => { // Fallback if edit fails
            await safeSendMessage(originalChatId, `🚫 Coinflip Offer by ${offerData.initiatorMentionHTML} for <b>${betDisplayHTML}</b> withdrawn.`, { parse_mode: 'HTML' });
        });
    } else { // If no original message ID to edit, send new
        await safeSendMessage(originalChatId, `🚫 Coinflip Offer by ${offerData.initiatorMentionHTML} for <b>${betDisplayHTML}</b> withdrawn.`, { parse_mode: 'HTML' });
    }
}

// --- Coinflip Player vs. Bot (PvB) Logic ---
async function startCoinflipPvBGame(chatId, initiatorUserObj, betAmountLamports, originalOfferMessageId, offerIdToDelete) {
    const userId = String(initiatorUserObj.id || initiatorUserObj.telegram_id);
    const logPrefix = `[CF_PvB_Start UID:${userId} CH:${chatId}]`;
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (originalOfferMessageId && bot) {
        await bot.deleteMessage(chatId, Number(originalOfferMessageId)).catch(() => {});
    }
    if (offerIdToDelete) activeGames.delete(offerIdToDelete);

    let client;
    const pvbGameId = generateGameId(GAME_IDS.COINFLIP_PVB);
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, BigInt(-betAmountLamports), 'bet_placed_coinflip_pvb', { game_id_custom_field: pvbGameId }, `PvB Coinflip bet by ${playerRefHTML}`);
        if (!balanceUpdateResult.success) throw new Error(balanceUpdateResult.error || "PvB Coinflip wager placement failed.");
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports;
        await client.query('COMMIT');
    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(() => {});
        console.error(`${logPrefix} DB error starting PvB Coinflip: ${error.message}`);
        await safeSendMessage(chatId, `⚙️ Database error for ${playerRefHTML} starting Coinflip vs Bot. Wager not processed. Try again.`, { parse_mode: 'HTML' });
        await updateGroupGameDetails(chatId, null, null, null); return;
    } finally { if (client) client.release(); }

    const gameDataPvB = {
        type: GAME_IDS.COINFLIP_PVB, gameId: pvbGameId, chatId, userId,
        playerRefHTML, userObj: initiatorUserObj, betAmount: betAmountLamports,
        playerChoice: null, result: null, status: 'pvb_waiting_choice',
        gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(pvbGameId, gameDataPvB);
    await updateGroupGameDetails(chatId, pvbGameId, GAME_IDS.COINFLIP_PVB, betAmountLamports);

    const titleHTML = `🤖${COIN_EMOJI_DISPLAY} <b>Coinflip: ${playerRefHTML} vs. Bot Dealer!</b> ${COIN_EMOJI_DISPLAY}🤖`;
    const initialMessageTextHTML = `${titleHTML}\n\nWager: <b>${betDisplayHTML}</b>\n\n` +
        `The Bot Dealer polishes a shimmering virtual coin! ${playerRefHTML}, make your call: Heads or Tails?`;
    const keyboard = {
        inline_keyboard: [[
            { text: `${COIN_EMOJI_DISPLAY} Heads`, callback_data: `cf_pvb_choice:${pvbGameId}:${COINFLIP_CHOICE_HEADS}` },
            { text: `${COIN_EMOJI_DISPLAY} Tails`, callback_data: `cf_pvb_choice:${pvbGameId}:${COINFLIP_CHOICE_TAILS}` }
        ],[{ text: "📖 Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.COINFLIP_UNIFIED_OFFER}` }]]
    };
    const sentMessage = await safeSendMessage(chatId, initialMessageTextHTML, { parse_mode: 'HTML', reply_markup: keyboard });
    if (sentMessage?.message_id) {
        gameDataPvB.gameMessageId = String(sentMessage.message_id);
        activeGames.set(pvbGameId, gameDataPvB);
    } else { 
        console.error(`${logPrefix} Failed to send Coinflip PvB game message for ${pvbGameId}. Refunding.`);
        let refundClient = null;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_coinflip_pvb_setup_fail', {}, `Refund CF PvB game ${pvbGameId}`);
            await refundClient.query('COMMIT');
        } catch (dbErr) { if (refundClient) await refundClient.query('ROLLBACK'); console.error(`${logPrefix} CRITICAL: Refund failed after CF PvB setup fail for ${pvbGameId}: ${dbErr.message}`);
        } finally { if (refundClient) refundClient.release(); }
        activeGames.delete(pvbGameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
}

async function handleCoinflipPvBChoiceCallback(gameId, playerChoice, userObj, originalMessageId, callbackQueryId) {
    const userId = String(userObj.id || userObj.telegram_id);
    const logPrefix = `[CF_PvBChoiceCB GID:${gameId} UID:${userId} Choice:${playerChoice}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.COINFLIP_PVB || gameData.userId !== userId || gameData.status !== 'pvb_waiting_choice') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Coinflip game action is outdated or not yours.", show_alert: true }).catch(() => {});
        if (originalMessageId && bot && gameData && String(gameData.gameMessageId) !== String(originalMessageId)) {
            bot.editMessageReplyMarkup({}, { chat_id: gameData.chatId, message_id: Number(originalMessageId) }).catch(() => {});
        }
        return;
    }
    const choiceDisplay = playerChoice === COINFLIP_CHOICE_HEADS ? "Heads" : "Tails";
    await bot.answerCallbackQuery(callbackQueryId, { text: `You called ${choiceDisplay}! Bot is flipping...` }).catch(() => {});

    gameData.playerChoice = playerChoice;
    gameData.status = 'pvb_flipping';
    activeGames.set(gameId, gameData);

    // Simulate "Helper Bot announcing result" by main bot doing the flip and animating
    const actualFlipOutcome = Math.random() < 0.5 ? COINFLIP_CHOICE_HEADS : COINFLIP_CHOICE_TAILS;
    gameData.result = actualFlipOutcome;

    const titleFlippingHTML = `💫 ${COIN_EMOJI_DISPLAY} <b>Coin in the Air!</b> ${COIN_EMOJI_DISPLAY} 💫`;
    let flippingMessageText = `${titleFlippingHTML}\n\n${gameData.playerRefHTML} called <b>${escapeHTML(choiceDisplay)}</b>!\n` +
                              `The Bot Dealer flips the coin... it's spinning wildly!\n\n`;

    if (gameData.gameMessageId && bot) {
        for (let i = 0; i < COIN_FLIP_ANIMATION_STEPS; i++) {
            const frame = COIN_FLIP_ANIMATION_FRAMES[i % COIN_FLIP_ANIMATION_FRAMES.length];
            try {
                await bot.editMessageText(flippingMessageText + `<b>${frame}</b>`, { chat_id: gameData.chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'HTML', reply_markup: {} });
            } catch (e) { if(!e.message?.includes("message is not modified")) console.warn(`${logPrefix} Animation edit fail step ${i}`); break; }
            await sleep(COIN_FLIP_ANIMATION_INTERVAL_MS);
        }
    } else { // Fallback if no message to edit
        await safeSendMessage(gameData.chatId, flippingMessageText + "<i>Flip in progress!</i>", {parse_mode: "HTML"});
        await sleep(COIN_FLIP_ANIMATION_DURATION_MS);
    }
    
    await finalizeCoinflipPvBGame(gameData);
}

// In Part 5a, Section 3 (ensure all dependencies like pool, updateUserBalanceAndLedger, formatBalanceForDisplay, etc. are available)

async function finalizeCoinflipPvBGame(gameData) {
    const { gameId, chatId, userId, playerRefHTML, betAmount, playerChoice, result, userObj } = gameData;
    const logPrefix = `[CF_PvB_Finalize GID:${gameId}]`;
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);

    const playerWins = playerChoice === result;
    let payoutAmountLamports = playerWins ? betAmount * 2n : 0n;
    let ledgerOutcomeCode = playerWins ? `win_coinflip_pvb_${playerChoice}` : `loss_coinflip_pvb_${playerChoice}_vs_${result}`;
    let finalUserBalance = BigInt(userObj.balance); // This will be updated but not displayed

    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const balanceUpdate = await updateUserBalanceAndLedger(client, userId, payoutAmountLamports, ledgerOutcomeCode, { game_id_custom_field: gameId }, `PvB Coinflip: ${playerChoice} vs Bot ${result}`);
        if (!balanceUpdate.success) throw new Error(balanceUpdate.error || "DB Error during Coinflip PvB payout.");
        finalUserBalance = balanceUpdate.newBalanceLamports; // Update for internal tracking
        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(() => {});
        console.error(`${logPrefix} CRITICAL DB error: ${e.message}`);
        // resultOutcomeText += `\n\n⚠️ Critical error settling wager. Admin notified.`; // This variable needs to be defined before appending (if used)
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Coinflip PvB Payout Failure 🚨\nGame ID: <code>${escapeHTML(gameId)}</code>\nError: ${escapeHTML(e.message)}. Manual check needed.`, { parse_mode: 'HTML'});
    } finally { if (client) client.release(); }

    const resultDisplay = result === COINFLIP_CHOICE_HEADS ? "Heads" : "Tails";
    const titleResultHTML = playerWins ? `🎉🏆 <b>YOU WIN, ${playerRefHTML}!</b> 🏆🎉` : `💔😥 <b>Better Luck Next Time, ${playerRefHTML}!</b> 😥💔`;
    const resultMessageHTML = `${titleResultHTML}\n\n` +
        `You called: <b>${escapeHTML(playerChoice === COINFLIP_CHOICE_HEADS ? "Heads" : "Tails")}</b>\n` +
        `The coin landed on... ✨ <b>${COIN_EMOJI_DISPLAY} ${escapeHTML(resultDisplay)}!</b> ✨\n\n` +
        (playerWins ? `Congratulations! You won <b>${escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'))}</b> in profit (total payout: <b>${escapeHTML(await formatBalanceForDisplay(payoutAmountLamports, 'USD'))}</b>)!` : `The Bot Dealer claims the pot of <b>${escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'))}</b>.`);
        // Balance display line removed

    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.COINFLIP_PVB, betAmount);
    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(resultMessageHTML, { chat_id: chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'HTML', reply_markup: postGameKeyboard }).catch(async (e)=>{
             if (!e.message?.includes("message is not modified")) await safeSendMessage(chatId, resultMessageHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
        });
    } else {
        await safeSendMessage(chatId, resultMessageHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
    }
}

// --- Coinflip Player vs. Player (PvP) Logic ---
async function startCoinflipPvPGame(
    initiatorData, // Object containing initiator's details and game context
    joinerUserObjFull, // Full user object of the player who joined/accepted
    originalOfferMessageId, // Message ID of the original unified offer (if applicable)
    betsAlreadyDeducted = false // New flag
) {
    const logPrefix = `[CF_PvP_Start_NoDoubleBet Init:${initiatorData.initiatorUserObj?.telegram_id || initiatorData.initiatorId} Join:${joinerUserObjFull.id || joinerUserObjFull.telegram_id}]`;

    const chatId = initiatorData.chatId;
    const betAmount = BigInt(initiatorData.betAmount);
    
    let initiatorUserObj = initiatorData.initiatorUserObj; 
    const initiatorId = String(initiatorUserObj.id || initiatorUserObj.telegram_id);
    const initiatorMentionHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));

    const joinerId = String(joinerUserObjFull.id || joinerUserObjFull.telegram_id);
    const joinerMentionHTML = escapeHTML(getPlayerDisplayReference(joinerUserObjFull));

    if (originalOfferMessageId && bot) {
        await bot.deleteMessage(chatId, Number(originalOfferMessageId)).catch(() => {});
    }
    
    if (initiatorData.gameId && initiatorData.type === GAME_IDS.COINFLIP_UNIFIED_OFFER && activeGames.has(initiatorData.gameId)) {
        activeGames.delete(initiatorData.gameId); 
        console.log(`${logPrefix} Deleted unified offer ${initiatorData.gameId} as Coinflip PvP game starts.`);
    }

    let client;
    const pvpGameId = generateGameId(GAME_IDS.COINFLIP_PVP);

    if (!betsAlreadyDeducted) {
        console.log(`${logPrefix} Bets were not pre-deducted. Processing bet deductions for Coinflip PvP game ${pvpGameId}.`);
        try {
            client = await pool.connect(); await client.query('BEGIN');
            
            const currentInitiatorForBetDeduction = await getOrCreateUser(initiatorId); 
            const currentJoinerForBetDeduction = await getOrCreateUser(joinerId); // Re-fetch for joiner too for safety

            if (!currentInitiatorForBetDeduction || BigInt(currentInitiatorForBetDeduction.balance) < betAmount) {
                 throw new Error(`Initiator ${initiatorMentionHTML} has insufficient funds (${BigInt(currentInitiatorForBetDeduction.balance)}) for Coinflip PvP bet of ${betAmount}.`);
            }
            // Use joinerUserObjFull directly if betsAlreadyDeducted, otherwise currentJoinerForBetDeduction
            const joinerBalanceToCheck = betsAlreadyDeducted ? BigInt(joinerUserObjFull.balance) : BigInt(currentJoinerForBetDeduction.balance);
            if (joinerBalanceToCheck < betAmount) { 
                 throw new Error(`Joiner ${joinerMentionHTML} has insufficient funds (${joinerBalanceToCheck}) for Coinflip PvP bet of ${betAmount}.`);
            }

            const initBetRes = await updateUserBalanceAndLedger(client, initiatorId, BigInt(-betAmount), 'bet_placed_coinflip_pvp_init', { game_id_custom_field: pvpGameId, opponent_id_custom_field: joinerId }, `PvP Coinflip bet vs ${joinerMentionHTML}`);
            if (!initBetRes.success) throw new Error(`Initiator bet failed: ${initBetRes.error}`);
            initiatorUserObj.balance = initBetRes.newBalanceLamports; 

            const joinBetRes = await updateUserBalanceAndLedger(client, joinerId, BigInt(-betAmount), 'bet_placed_coinflip_pvp_join', { game_id_custom_field: pvpGameId, opponent_id_custom_field: initiatorId }, `PvP Coinflip bet vs ${initiatorMentionHTML}`);
            if (!joinBetRes.success) throw new Error(`Joiner bet failed: ${joinBetRes.error}`);
            joinerUserObjFull.balance = joinBetRes.newBalanceLamports; 
            
            await client.query('COMMIT');
        } catch (error) {
            if (client) await client.query('ROLLBACK').catch(() => {});
            console.error(`${logPrefix} DB error placing PvP Coinflip bets: ${error.message}`);
            await safeSendMessage(chatId, `⚙️ Database error placing bets for PvP Coinflip between ${initiatorMentionHTML} and ${joinerMentionHTML}. Game cannot start. Bets have *not* been processed. Please try the offer again.`, { parse_mode: 'HTML' });
            return; 
        } finally {
            if (client) client.release();
        }
    } else {
        console.log(`${logPrefix} Bets already deducted for Coinflip PvP game ${pvpGameId}. Using provided player objects with updated balances.`);
    }

    const p1IsCaller = Math.random() < 0.5;
    const callerId = p1IsCaller ? initiatorId : joinerId;
    const callerMentionHTML = p1IsCaller ? initiatorMentionHTML : joinerMentionHTML;

    const gameDataPvP = {
        type: GAME_IDS.COINFLIP_PVP, gameId: pvpGameId, chatId, betAmount,
        p1: { userId: initiatorId, mentionHTML: initiatorMentionHTML, userObj: initiatorUserObj },
        p2: { userId: joinerId, mentionHTML: joinerMentionHTML, userObj: joinerUserObjFull },
        callerId: callerId,
        callerChoice: null, result: null,
        status: 'pvp_waiting_caller_choice', 
        gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(pvpGameId, gameDataPvP);
    await updateGroupGameDetails(chatId, pvpGameId, GAME_IDS.COINFLIP_PVP, betAmount); 
    await promptCoinflipPvPCaller(gameDataPvP, callerMentionHTML); 
}

async function promptCoinflipPvPCaller(gameData, callerMentionHTML) { // Pass gameData object
    const titleHTML = `✨⚔️ <b>Coinflip PvP: ${gameData.p1.mentionHTML} vs ${gameData.p2.mentionHTML}!</b> ⚔️✨`;
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    const messageTextHTML = `${titleHTML}\nWager: <b>${betDisplayHTML}</b> each.\n\n` +
        `The virtual coin is launched high into the digital sky! 🌪️${COIN_EMOJI_DISPLAY}🌪️\n\n` +
        `Fate has decreed that <b>${callerMentionHTML}</b> shall make the fateful call!\n` +
        `What is your prediction: Heads or Tails? Click your destiny below!`;
    const keyboard = {
        inline_keyboard: [[
            { text: `${COIN_EMOJI_DISPLAY} Heads It Is!`, callback_data: `cf_pvp_call:${gameData.gameId}:${gameData.callerId}:${COINFLIP_CHOICE_HEADS}` },
            { text: `${COIN_EMOJI_DISPLAY} Tails, No Fails!`, callback_data: `cf_pvp_call:${gameData.gameId}:${gameData.callerId}:${COINFLIP_CHOICE_TAILS}` }
        ]]
    };
    const sentMessage = await safeSendMessage(gameData.chatId, messageTextHTML, { parse_mode: 'HTML', reply_markup: keyboard });
    if (sentMessage?.message_id) {
        const currentGameData = activeGames.get(gameData.gameId); // Re-fetch to avoid stale data if needed
        if (currentGameData) {
            currentGameData.gameMessageId = String(sentMessage.message_id);
            activeGames.set(gameData.gameId, currentGameData); // Update with message ID
        }
    } else {
        console.error(`[CF_PvP_PromptCaller GID:${gameData.gameId}] Failed to send caller prompt message.`);
        // Consider ending the game and refunding if this critical message fails
    }
}

async function handleCoinflipPvPCallCallback(gameId, callerIdCheck, callChoice, userObj, originalMessageId, callbackQueryId) {
    const userId = String(userObj.id || userObj.telegram_id);
    const logPrefix = `[CF_PvPCallCB GID:${gameId} UID:${userId} Call:${callChoice}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.COINFLIP_PVP || gameData.callerId !== userId || String(gameData.callerId) !== String(callerIdCheck) || gameData.status !== 'pvp_waiting_caller_choice') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Coinflip call is not for you or has expired.", show_alert: true }).catch(() => {});
        if (originalMessageId && bot && gameData && String(gameData.gameMessageId) !== String(originalMessageId)) {
            bot.editMessageReplyMarkup({}, { chat_id: gameData.chatId, message_id: Number(originalMessageId) }).catch(() => {});
        }
        return;
    }
    const callDisplay = callChoice === COINFLIP_CHOICE_HEADS ? "Heads" : "Tails";
    await bot.answerCallbackQuery(callbackQueryId, { text: `You've boldly called ${callDisplay}! The coin descends...` }).catch(() => {});

    gameData.callerChoice = callChoice;
    gameData.status = 'pvp_flipping'; 
    activeGames.set(gameId, gameData);

    const actualFlipOutcome = Math.random() < 0.5 ? COINFLIP_CHOICE_HEADS : COINFLIP_CHOICE_TAILS;
    gameData.result = actualFlipOutcome;

    const callerPlayerObj = gameData.callerId === gameData.p1.userId ? gameData.p1 : gameData.p2;
    const titleFlippingHTML = `💥 ${COIN_EMOJI_DISPLAY} <b>The Decisive Flip! The Moment of Truth!</b> ${COIN_EMOJI_DISPLAY} 💥`;
    let flippingMessageText = `${titleFlippingHTML}\n\n${callerPlayerObj.mentionHTML} made the call: <b>${escapeHTML(callDisplay)}</b>!\n` +
                              `The coin tumbles through the air, secrets held tight... and finally lands!\n\n`;

    if (gameData.gameMessageId && bot) {
        for (let i = 0; i < COIN_FLIP_ANIMATION_STEPS; i++) {
            const frame = COIN_FLIP_ANIMATION_FRAMES[i % COIN_FLIP_ANIMATION_FRAMES.length];
             try {
                await bot.editMessageText(flippingMessageText + `<b>${frame}</b>`, { chat_id: gameData.chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'HTML', reply_markup: {} });
            } catch (e) { if(!e.message?.includes("message is not modified")) console.warn(`${logPrefix} PvP Animation edit fail step ${i}`); break; }
            await sleep(COIN_FLIP_ANIMATION_INTERVAL_MS);
        }
    } else {
        await safeSendMessage(gameData.chatId, flippingMessageText + "<i>The result is IN!</i>", {parse_mode: "HTML"});
        await sleep(COIN_FLIP_ANIMATION_DURATION_MS);
    }

    await finalizeCoinflipPvPGame(gameData);
}

async function finalizeCoinflipPvPGame(gameData) {
    const { gameId, chatId, betAmount, p1, p2, callerId, callerChoice, result } = gameData;
    const logPrefix = `[CF_PvP_Finalize GID:${gameId}]`;
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);

    const callerWon = callerChoice === result;
    const winnerObj = callerWon ? (callerId === p1.userId ? p1 : p2) : (callerId === p1.userId ? p2 : p1);
    const loserObj = callerWon ? (callerId === p1.userId ? p2 : p1) : (callerId === p1.userId ? p1 : p2);

    let payoutAmountLamports = betAmount * 2n; // Winner gets the full pot
    let ledgerOutcomeCodeWinner = `win_coinflip_pvp_result`;
    let ledgerOutcomeCodeLoser = `loss_coinflip_pvp_result`;
    let finalWinnerBalance = BigInt(winnerObj.userObj.balance); // For internal tracking
    let finalLoserBalance = BigInt(loserObj.userObj.balance);  // For internal tracking

    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const winnerUpdate = await updateUserBalanceAndLedger(client, winnerObj.userId, payoutAmountLamports, ledgerOutcomeCodeWinner, { game_id_custom_field: gameId, opponent_id_custom_field: loserObj.userId }, `PvP Coinflip WIN. Caller: ${callerId===winnerObj.userId ? 'Self' : 'Opponent'}, Call: ${callerChoice}, Result: ${result}`);
        if (!winnerUpdate.success) throw new Error(`Winner payout failed: ${winnerUpdate.error}`);
        finalWinnerBalance = winnerUpdate.newBalanceLamports;

        const loserUpdate = await updateUserBalanceAndLedger(client, loserObj.userId, 0n, ledgerOutcomeCodeLoser, { game_id_custom_field: gameId, opponent_id_custom_field: winnerObj.userId }, `PvP Coinflip LOSS. Caller: ${callerId===loserObj.userId ? 'Self' : 'Opponent'}, Call: ${callerChoice}, Result: ${result}`);
        if (!loserUpdate.success && loserUpdate.errorCode !== 'INSUFFICIENT_FUNDS') {
             console.warn(`${logPrefix} Non-critical error updating loser's ledger (0n change): ${loserUpdate.error}`);
        }
        finalLoserBalance = loserUpdate.newBalanceLamports;

        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(() => {});
        console.error(`${logPrefix} CRITICAL DB error during PvP Coinflip payout: ${e.message}`);
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Coinflip PvP Payout Failure 🚨\nGame ID: <code>${escapeHTML(gameId)}</code>\nWinner: ${winnerObj.mentionHTML}\nLoser: ${loserObj.mentionHTML}\nError: ${escapeHTML(e.message)}. Manual balance check required.`, { parse_mode: 'HTML'});
    } finally { if (client) client.release(); }

    const callerActualMentionHTML = (callerId === p1.userId ? p1.mentionHTML : p2.mentionHTML);
    const resultDisplay = result === COINFLIP_CHOICE_HEADS ? "Heads" : "Tails";
    const callDisplay = callerChoice === COINFLIP_CHOICE_HEADS ? "Heads" : "Tails";

    const titleResultHTML = `🎊 ${COIN_EMOJI_DISPLAY} <b>Coinflip PvP - The Outcome is Revealed!</b> ${COIN_EMOJI_DISPLAY} 🎊`;
    const resultMessageHTML = `${titleResultHTML}\n\n` +
        `The epic duel between ${p1.mentionHTML} and ${p2.mentionHTML} (wager: <b>${escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'))}</b> each) has concluded!\n\n` +
        `<b>${callerActualMentionHTML}</b> was chosen to make the call and predicted: <b>${escapeHTML(callDisplay)}</b>!\n` +
        `The coin majestically landed on... ✨ <b>${COIN_EMOJI_DISPLAY} ${escapeHTML(resultDisplay)}!</b> ✨\n\n` +
        `And thus, the champion of this fateful flip is... 🥳🏆 <b>${winnerObj.mentionHTML}</b>! You seize the glorious pot of <b>${escapeHTML(await formatBalanceForDisplay(payoutAmountLamports, 'USD'))}</b>!\n\n` +
        `Commiserations, ${loserObj.mentionHTML}! Better luck on the next toss.`;
        // Balance display lines removed

    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.COINFLIP_PVP, betAmount);
    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(resultMessageHTML, { chat_id: chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'HTML', reply_markup: postGameKeyboard }).catch(async (e)=>{
             if (!e.message?.includes("message is not modified")) await safeSendMessage(chatId, resultMessageHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
        });
    } else {
        await safeSendMessage(chatId, resultMessageHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
    }
}
// --- End of REVISED Coinflip Game Logic & Handlers ---

// --- Start of REVISED Rock Paper Scissors (RPS) Game Logic & Handlers (Unified Offer, HTML, New Mechanics) ---

// RPS_CHOICES, RPS_EMOJIS, RPS_RULES are assumed to be defined (from Part 4)

// --- RPS Unified Offer Command ---
async function handleStartRPSUnifiedOfferCommand(msg, betAmountLamports, targetUsernameRaw = null) { 
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const logPrefix = `[RPS_OfferOrDirect UID:${userId} CH:${chatId}]`; 

    if (chatType === 'private') {
        await safeSendMessage(chatId, `🪨📄✂️ The Rock Paper Scissors arena is best experienced in <b>group chats</b>! Please use <code>/rps &lt;bet&gt; [@username]</code> there to challenge opponents or the bot.`, { parse_mode: 'HTML' });
        return;
    }

    let initiatorUserObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!initiatorUserObj) {
        await safeSendMessage(chatId, "⚠️ Error fetching your player profile for RPS. Please try <code>/start</code> again with me first.", { parse_mode: 'HTML' });
        return;
    }
    const initiatorPlayerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        await safeSendMessage(chatId, `💰 ${initiatorPlayerRefHTML}, your war chest is a bit light for a <b>${betDisplayUSD_HTML}</b> RPS duel! You'll need approximately <b>${escapeHTML(await formatBalanceForDisplay(needed, 'USD'))}</b> more. Ready to strategize with more funds?`, {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: "💸 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }
    
    const gameSession = await getGroupSession(chatId, msg.chat.title || `Group Chat ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        if ( (existingGame.type.includes('_offer') && existingGame.status === 'pending_offer') || 
             (existingGame.type === GAME_IDS.DIRECT_PVP_CHALLENGE && existingGame.status === 'pending_direct_challenge_response') ||
             (existingGame.type.includes('_pvp') && !existingGame.status?.startsWith('game_over_')) ||
             (existingGame.type === GAME_IDS.MINES && existingGame.status !== 'game_over_mine_hit' && existingGame.status !== 'game_over_cashed_out')
           ) {
            await safeSendMessage(chatId, `⏳ Hold your hands, ${initiatorPlayerRefHTML}! An interactive game or challenge (<code>${escapeHTML(existingGame.type.replace(/_/g, " "))}</code>) is already active. Please wait.`, { parse_mode: 'HTML' });
            return;
        }
    }

    let targetUserObject = null;
    if (targetUsernameRaw) {
        targetUserObject = await findRecipientUser(targetUsernameRaw);
        if (!targetUserObject || !targetUserObject.telegram_id) { 
            await safeSendMessage(chatId, `😕 Player ${escapeHTML(targetUsernameRaw)} not found or has an invalid ID. Cannot create an RPS challenge. Please ensure they have started a chat with me first.`, { parse_mode: 'HTML' });
            return; 
        }
        if (String(targetUserObject.telegram_id) === userId) {
            await safeSendMessage(chatId, `😅 You can't challenge yourself to an RPS duel, ${initiatorPlayerRefHTML}!`, { parse_mode: 'HTML' });
            return;
        }
    }

    if (targetUserObject && targetUserObject.telegram_id) { 
        // --- DIRECT PvP CHALLENGE FLOW for RPS ---
        console.log(`${logPrefix} Initiating DIRECT RPS challenge to User ID: ${targetUserObject.telegram_id} (@${targetUserObject.username || 'N/A'})`);
        const targetPlayerRefHTML = escapeHTML(getPlayerDisplayReference(targetUserObject));
        const offerId = generateGameId(`drps_${userId.slice(-3)}_${String(targetUserObject.telegram_id).slice(-3)}`); 

        const groupChallengeTextHTML = `Hey ${targetPlayerRefHTML}❗\n\n${initiatorPlayerRefHTML} has challenged you to a game of <b>Rock Paper Scissors</b> for <b>${betDisplayUSD_HTML}</b>!`;
        
        const groupChallengeKeyboard = { 
            inline_keyboard: [
                [{ text: "✅ Accept Challenge", callback_data: `dir_chal_acc:${offerId}` }],
                [{ text: "❌ Decline Challenge", callback_data: `dir_chal_dec:${offerId}` }],
                [{ text: "🚫 Withdraw My Challenge", callback_data: `dir_chal_can:${offerId}` }]
            ]
        };

        const sentGroupMessage = await safeSendMessage(chatId, groupChallengeTextHTML, { parse_mode: 'HTML', reply_markup: groupChallengeKeyboard });

        if (!sentGroupMessage || !sentGroupMessage.message_id) {
            console.error(`${logPrefix} Failed to send direct RPS challenge message for offer ${offerId}.`);
            await safeSendMessage(chatId, `⚙️ Oops! Couldn't send your RPS challenge to ${targetPlayerRefHTML}. Please try again.`, { parse_mode: 'HTML' });
            return;
        }
        const offerMessageIdInGroup = String(sentGroupMessage.message_id);
        const groupNameHTML = escapeHTML(msg.chat.title || "the group");

        const dmNotificationTextHTML = `🔔 Challenge Alert!\n\nHi ${targetPlayerRefHTML},\n${initiatorPlayerRefHTML} has challenged you to a game of <b>Rock Paper Scissors</b> for <b>${betDisplayUSD_HTML}</b> in the group "<b>${groupNameHTML}</b>".\n\nPlease head to that group to accept or decline the challenge.`;
        const dmSent = await safeSendMessage(targetUserObject.telegram_id, dmNotificationTextHTML, { parse_mode: 'HTML' });
        if (!dmSent) {
            console.warn(`${logPrefix} Failed to send DM notification for direct RPS challenge to target ${targetUserObject.telegram_id}. Offer still posted in group.`);
            await safeSendMessage(chatId, `ℹ️ ${initiatorPlayerRefHTML}, your challenge to ${targetPlayerRefHTML} is posted! Note: They might not receive a DM if they haven't interacted with me before.`, { parse_mode: 'HTML'});
        }

        const directOfferData = {
            type: GAME_IDS.DIRECT_PVP_CHALLENGE, 
            offerId: offerId, 
            gameId: offerId, 
            initiatorId: userId,
            initiatorUserObj: initiatorUserObj, 
            initiatorMentionHTML: initiatorPlayerRefHTML,
            targetUserId: String(targetUserObject.telegram_id),
            targetUserObj: targetUserObject, 
            targetUserMentionHTML: targetPlayerRefHTML,
            betAmount: betAmountLamports,
            originalGroupId: chatId,
            offerMessageIdInGroup: offerMessageIdInGroup,
            chatTitle: msg.chat.title || `Group Chat ${chatId}`, 
            status: 'pending_direct_challenge_response',
            gameToStart: GAME_IDS.RPS_PVP, 
            creationTime: Date.now()
        };
        activeGames.set(offerId, directOfferData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.DIRECT_PVP_CHALLENGE, betAmountLamports);
        console.log(`${logPrefix} Direct RPS challenge offer ${offerId} created and stored.`);

        setTimeout(async () => {
            const timedOutOffer = activeGames.get(offerId);
            if (timedOutOffer && timedOutOffer.status === 'pending_direct_challenge_response' && timedOutOffer.type === GAME_IDS.DIRECT_PVP_CHALLENGE) {
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null);
                const gameNameForTimeout = "Rock Paper Scissors"; 
                const timeoutBetDisplay = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                const timeoutMsgHTML = `⏳ The ${gameNameForTimeout} challenge from ${timedOutOffer.initiatorMentionHTML} to ${timedOutOffer.targetUserMentionHTML} for <b>${timeoutBetDisplay}</b> has expired unanswered.`;
                if (bot && timedOutOffer.offerMessageIdInGroup) {
                    await bot.editMessageText(timeoutMsgHTML, {
                        chat_id: timedOutOffer.originalGroupId, message_id: Number(timedOutOffer.offerMessageIdInGroup),
                        parse_mode: 'HTML', reply_markup: {} 
                    }).catch(e => { 
                        console.warn(`${logPrefix} Failed to edit expired direct RPS challenge message ${timedOutOffer.offerMessageIdInGroup}: ${e.message}. Sending new.`);
                        safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                    });
                } else { 
                    safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                }
                await safeSendMessage(timedOutOffer.initiatorId, `⏳ Your RPS challenge to ${timedOutOffer.targetUserMentionHTML} in group "${escapeHTML(timedOutOffer.chatTitle)}" has expired.`, { parse_mode: 'HTML' });
            }
        }, JOIN_GAME_TIMEOUT_MS);

    } else {
        // --- EXISTING RPS UNIFIED OFFER FLOW (No valid targetUserObject specified) ---
        console.log(`${logPrefix} Initiating UNIFIED RPS offer (no target user or target was invalid).`);
        const offerId = generateGameId(GAME_IDS.RPS_UNIFIED_OFFER); 
        const offerData = { 
            type: GAME_IDS.RPS_UNIFIED_OFFER,
            gameId: offerId, chatId: chatId, chatType: chatType,
            initiatorId: userId, initiatorMentionHTML: initiatorPlayerRefHTML, initiatorUserObj: initiatorUserObj,
            betAmount: betAmountLamports, status: 'pending_offer', creationTime: Date.now(), offerMessageId: null
        };
        activeGames.set(offerId, offerData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.RPS_UNIFIED_OFFER, betAmountLamports);

        const offerMessageTextHTML = `✨🪨📄✂️ <b>A Battle of Wits! RPS Challenge!</b> ✂️📄🪨✨\n\n` +
            `${initiatorPlayerRefHTML} issues an RPS challenge for <b>${betDisplayUSD_HTML}</b>!\n\n` +
            `Face the cunning Bot Dealer or await a worthy PvP opponent!\n\n` +
            `<i>This strategic offer will vanish in ${JOIN_GAME_TIMEOUT_MS / 1000 / 60} minutes! Make your move!</i>`;

        const offerKeyboard = { 
            inline_keyboard: [
                [{ text: "🤖 Challenge Bot", callback_data: `rps_accept_bot:${offerId}` }],
                [{ text: "👤 Accept PvP Duel", callback_data: `rps_accept_pvp:${offerId}` }],
                [{ text: "🚫 Withdraw Challenge", callback_data: `rps_cancel_offer:${offerId}` }]
            ]
        };

        const sentMessage = await safeSendMessage(chatId, offerMessageTextHTML, { parse_mode: 'HTML', reply_markup: offerKeyboard });
        if (sentMessage?.message_id) {
            const currentOffer = activeGames.get(offerId);
            if(currentOffer) {
                currentOffer.offerMessageId = String(sentMessage.message_id);
                activeGames.set(offerId, currentOffer);
            } else {
                 if (bot) await bot.deleteMessage(chatId, sentMessage.message_id).catch(()=>{});
            }
            setTimeout(async () => { 
                const timedOutOffer = activeGames.get(offerId);
                if (timedOutOffer && timedOutOffer.status === 'pending_offer' && timedOutOffer.type === GAME_IDS.RPS_UNIFIED_OFFER) {
                    activeGames.delete(offerId);
                    await updateGroupGameDetails(chatId, null, null, null);
                    if (timedOutOffer.offerMessageId && bot) {
                        const timeoutBetDisplayUnifiedRPS = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                        await bot.editMessageText(`⏳ The RPS offer by ${timedOutOffer.initiatorMentionHTML} for <b>${timeoutBetDisplayUnifiedRPS}</b> has expired. No strategists answered the call.`, {
                            chat_id: timedOutOffer.chatId, message_id: Number(timedOutOffer.offerMessageId), parse_mode: 'HTML', reply_markup: {}
                        }).catch(e => {}); 
                    }
                }
            }, JOIN_GAME_TIMEOUT_MS);
        } else {
            console.error(`${logPrefix} Failed to send RPS unified offer message for ${offerId}.`);
            activeGames.delete(offerId);
            await updateGroupGameDetails(chatId, null, null, null);
            await safeSendMessage(chatId, `⚙️ Oops! Couldn't issue your RPS challenge. Try again.`, { parse_mode: 'HTML' });
        }
    }
}

// --- RPS Offer Callback Handlers ---
async function handleRPSAcceptBotGameCallback(offerId, userWhoClicked, originalOfferMessageId, originalChatId, originalChatType, callbackQueryId) {
    const userId = String(userWhoClicked.id || userWhoClicked.telegram_id);
    const logPrefix = `[RPS_AcceptBotCB OfferID:${offerId} UID:${userId}]`;
    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.RPS_UNIFIED_OFFER || offerData.status !== 'pending_offer') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This RPS offer is no longer valid.", show_alert: true }).catch(() => {});
        if (originalOfferMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalOfferMessageId) }).catch(() => {});
        return;
    }
    if (offerData.initiatorId !== userId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the one who laid down the gauntlet can face the Bot!", show_alert: true }).catch(() => {});
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: "🤖 Preparing your RPS duel with the Bot Dealer..." }).catch(() => {});
    await startRPSPvBGame(originalChatId, offerData.initiatorUserObj, offerData.betAmount, offerData.offerMessageId, offerId);
}

async function handleRPSAcceptPvPChallengeCallback(offerId, joinerUserObjFull, originalOfferMessageId, originalChatId, originalChatType, callbackQueryId) {
    const joinerId = String(joinerUserObjFull.id || joinerUserObjFull.telegram_id);
    const logPrefix = `[RPS_AcceptPvPCB OfferID:${offerId} JoinerID:${joinerId}]`;
    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.RPS_UNIFIED_OFFER || offerData.status !== 'pending_offer') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This RPS PvP challenge has already been met or has expired!", show_alert: true }).catch(() => {});
        if (originalOfferMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalOfferMessageId) }).catch(() => {});
        return;
    }
    if (offerData.initiatorId === joinerId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "A duel with oneself? An interesting strategy, but not for this game!", show_alert: true }).catch(() => {});
        return;
    }
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount, 'USD'));
    if (BigInt(joinerUserObjFull.balance) < offerData.betAmount) {
        await bot.answerCallbackQuery(callbackQueryId, { text: `Your funds are insufficient for this ${betDisplayHTML} RPS battle!`, show_alert: true }).catch(() => {});
        const needed = offerData.betAmount - BigInt(joinerUserObjFull.balance);
        await safeSendMessage(originalChatId, `💰 ${escapeHTML(getPlayerDisplayReference(joinerUserObjFull))}, your war chest is short by ~<b>${escapeHTML(await formatBalanceForDisplay(needed, 'USD'))}</b> for this intense <b>${betDisplayHTML}</b> RPS duel.`, {
             parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: "💸 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }
    const currentInitiatorUserObj = await getOrCreateUser(offerData.initiatorId);
    if (!currentInitiatorUserObj || BigInt(currentInitiatorUserObj.balance) < offerData.betAmount) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Initiator can't cover the bet. Offer cancelled.", show_alert:true}).catch(()=>{});
        if (offerData.offerMessageId && bot) {
             await bot.editMessageText(`⚠️ <b>Offer Auto-Cancelled</b><br>The RPS challenge by ${offerData.initiatorMentionHTML} for <b>${betDisplayHTML}</b> was cancelled as their balance is no longer sufficient.`, {
                 chat_id: originalChatId, message_id: Number(offerData.offerMessageId), parse_mode: 'HTML', reply_markup: {inline_keyboard:[]}
             }).catch(()=>{});
        }
        activeGames.delete(offerId);
        await updateGroupGameDetails(originalChatId, null, null, null);
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: "⚔️ RPS PvP Duel Accepted! Prepare your minds..." }).catch(() => {});
    await startRPSPvPGame(offerData, joinerUserObjFull, offerData.offerMessageId);
}

async function handleRPSCancelOfferCallback(offerId, userWhoClicked, originalOfferMessageId, originalChatId, callbackQueryId) {
    const userId = String(userWhoClicked.id || userWhoClicked.telegram_id);
    const logPrefix = `[RPS_CancelOfferCB OfferID:${offerId} UID:${userId}]`;
    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.RPS_UNIFIED_OFFER || offerData.status !== 'pending_offer') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Offer already gone or actioned!", show_alert: false }).catch(() => {});
        if (originalOfferMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalOfferMessageId) }).catch(() => {});
        return;
    }
    if (offerData.initiatorId !== userId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the initiator can cancel this RPS challenge.", show_alert: true }).catch(() => {});
        return;
    }
    await bot.answerCallbackQuery(callbackQueryId, { text: "RPS challenge withdrawn." }).catch(() => {});
    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null);
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount, 'USD'));
    if (originalOfferMessageId && bot) {
        await bot.editMessageText(`🚫 <b>Challenge Retracted!</b>\nThe RPS duel by ${offerData.initiatorMentionHTML} (wager: <b>${betDisplayHTML}</b>) has been cancelled.`, {
            chat_id: originalChatId, message_id: Number(originalOfferMessageId), parse_mode: 'HTML', reply_markup: {}
        }).catch(async (e) => {
            await safeSendMessage(originalChatId, `🚫 RPS challenge by ${offerData.initiatorMentionHTML} for <b>${betDisplayHTML}</b> withdrawn.`, { parse_mode: 'HTML' });
        });
    } else {
        await safeSendMessage(originalChatId, `🚫 RPS challenge by ${offerData.initiatorMentionHTML} for <b>${betDisplayHTML}</b> withdrawn.`, { parse_mode: 'HTML' });
    }
}

// --- RPS Player vs. Bot (PvB) Logic ---
async function startRPSPvBGame(chatId, initiatorUserObj, betAmountLamports, originalOfferMessageId, offerIdToDelete) {
    const userId = String(initiatorUserObj.id || initiatorUserObj.telegram_id);
    const logPrefix = `[RPS_PvB_Start UID:${userId} CH:${chatId}]`;
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (originalOfferMessageId && bot) await bot.deleteMessage(chatId, Number(originalOfferMessageId)).catch(() => {});
    if (offerIdToDelete) activeGames.delete(offerIdToDelete);

    let client;
    const pvbGameId = generateGameId(GAME_IDS.RPS_PVB);
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, BigInt(-betAmountLamports), 'bet_placed_rps_pvb', { game_id_custom_field: pvbGameId }, `PvB RPS bet by ${playerRefHTML}`);
        if (!balanceUpdateResult.success) throw new Error(balanceUpdateResult.error || "PvB RPS wager placement failed.");
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports;
        await client.query('COMMIT');
    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(() => {});
        console.error(`${logPrefix} DB error starting PvB RPS: ${error.message}`);
        await safeSendMessage(chatId, `⚙️ Database error for ${playerRefHTML} starting RPS vs Bot. Wager not processed. Try again.`, { parse_mode: 'HTML' });
        await updateGroupGameDetails(chatId, null, null, null); return;
    } finally { if (client) client.release(); }

    const gameDataPvB = {
        type: GAME_IDS.RPS_PVB, gameId: pvbGameId, chatId, userId,
        playerRefHTML, userObj: initiatorUserObj, betAmount: betAmountLamports,
        playerChoice: null, botChoice: null, result: null, status: 'pvb_waiting_player_choice',
        gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(pvbGameId, gameDataPvB);
    await updateGroupGameDetails(chatId, pvbGameId, GAME_IDS.RPS_PVB, betAmountLamports);

    const titleHTML = `🤖🪨📄✂️ <b>RPS: ${playerRefHTML} vs. The Bot Brain!</b> ✂️📄🪨🤖`;
    const initialMessageTextHTML = `${titleHTML}\n\nWager: <b>${betDisplayHTML}</b>\n\n` +
        `The Bot Dealer cracks its digital knuckles! ${playerRefHTML}, make your move! Choose your weapon: Rock, Paper, or Scissors?`;
    const keyboard = {
        inline_keyboard: [[
            { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_pvb_choice:${pvbGameId}:${RPS_CHOICES.ROCK}` },
            { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_pvb_choice:${pvbGameId}:${RPS_CHOICES.PAPER}` },
            { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_pvb_choice:${pvbGameId}:${RPS_CHOICES.SCISSORS}` }
        ],[{ text: "📖 Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.RPS_UNIFIED_OFFER}` }]] // Ensure GAME_IDS.RPS_UNIFIED_OFFER exists
    };
    const sentMessage = await safeSendMessage(chatId, initialMessageTextHTML, { parse_mode: 'HTML', reply_markup: keyboard });
    if (sentMessage?.message_id) {
        gameDataPvB.gameMessageId = String(sentMessage.message_id);
        activeGames.set(pvbGameId, gameDataPvB);
    } else {
        console.error(`${logPrefix} Failed to send RPS PvB game message for ${pvbGameId}. Refunding.`);
        let refundClient = null;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_rps_pvb_setup_fail', {}, `Refund RPS PvB game ${pvbGameId}`);
            await refundClient.query('COMMIT');
        } catch (dbErr) { if (refundClient) await refundClient.query('ROLLBACK'); console.error(`${logPrefix} CRITICAL: Refund failed after RPS PvB setup fail for ${pvbGameId}: ${dbErr.message}`);
        } finally { if (refundClient) refundClient.release(); }
        activeGames.delete(pvbGameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
}

async function handleRPSPvBChoiceCallback(gameId, playerChoiceKey, userObj, originalMessageId, callbackQueryId) {
    const userId = String(userObj.id || userObj.telegram_id);
    const logPrefix = `[RPS_PvBChoiceCB GID:${gameId} UID:${userId} Choice:${playerChoiceKey}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.RPS_PVB || gameData.userId !== userId || gameData.status !== 'pvb_waiting_player_choice') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This RPS game action is outdated or not yours.", show_alert: true }).catch(() => {});
        if (originalMessageId && bot && gameData && String(gameData.gameMessageId) !== String(originalMessageId)) {
            bot.editMessageReplyMarkup({}, { chat_id: gameData.chatId, message_id: Number(originalMessageId) }).catch(() => {});
        }
        return;
    }
    const playerChoiceDisplay = playerChoiceKey.charAt(0).toUpperCase() + playerChoiceKey.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, { text: `You chose ${RPS_EMOJIS[playerChoiceKey]} ${playerChoiceDisplay}! Bot is making its move...` }).catch(() => {});

    gameData.playerChoice = playerChoiceKey;
    const botRPSChoice = getRandomRPSChoice(); // from Part 4
    gameData.botChoice = botRPSChoice.choice;
    gameData.status = 'pvb_resolving';
    activeGames.set(gameId, gameData);

    const playerChoiceEmoji = RPS_EMOJIS[gameData.playerChoice];
    const titleResolvingHTML = `💥 <b>RPS Showdown Unfolds!</b> 💥`;
    let resolvingText = `${titleResolvingHTML}\n\n${gameData.playerRefHTML} throws: ${playerChoiceEmoji}\n` +
                        `Bot Dealer counters with: Thinking... 🤔\n\n<i>The tension mounts!</i>`;
    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(resolvingText, { chat_id: gameData.chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'HTML', reply_markup: {} });
        } catch(e) {
            if (!e.message?.includes("message is not modified")) {
                const newMsg = await safeSendMessage(gameData.chatId, resolvingText, {parse_mode: 'HTML'});
                if(newMsg?.message_id && activeGames.has(gameId)) activeGames.get(gameId).gameMessageId = String(newMsg.message_id);
            }
        }
    }
    await sleep(2500); // Dramatic pause for the "explosion"

    await finalizeRPSPvBGame(gameData);
}

async function finalizeRPSPvBGame(gameData) {
    const { gameId, chatId, userId, playerRefHTML, betAmount, playerChoice, botChoice, userObj } = gameData;
    const logPrefix = `[RPS_PvB_Finalize_V3 GID:${gameId}]`;
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);

    // playerRefHTML is already HTML-escaped. "Bot Dealer" is a safe string.
    const rpsOutcome = determineRPSOutcome(playerChoice, botChoice, playerRefHTML, "Bot Dealer");
    let payoutAmountLamports = 0n;
    let ledgerOutcomeCode = `loss_rps_pvb_${playerChoice}_vs_${botChoice}`;
    let finalUserBalance = BigInt(userObj.balance); // This will be updated but not displayed

    let financialOutcomeText = ""; // To state winnings/losses clearly

    if (rpsOutcome.result === 'win_player1') { // Player 1 (the user) wins
        payoutAmountLamports = betAmount * 2n; // Total payout (stake + profit)
        const profitAmount = betAmount; // Actual profit is 1x bet
        financialOutcomeText = `Congratulations! You won <b>${escapeHTML(await formatBalanceForDisplay(profitAmount, 'USD'))}</b> in profit (total payout: ${escapeHTML(await formatBalanceForDisplay(payoutAmountLamports, 'USD'))})!`;
        ledgerOutcomeCode = `win_rps_pvb_${playerChoice}_vs_${botChoice}`;
    } else if (rpsOutcome.result === 'draw') {
        payoutAmountLamports = betAmount; // Bet returned
        financialOutcomeText = `Your wager of <b>${escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'))}</b> has been returned.`;
        ledgerOutcomeCode = `draw_rps_pvb_${playerChoice}_vs_${botChoice}`;
    } else { // Bot wins (rpsOutcome.result === 'win_player2')
        // payoutAmountLamports remains 0n (bet already deducted)
        financialOutcomeText = `The Bot Dealer claims your wager of <b>${escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'))}</b>.`;
        // ledgerOutcomeCode is already set to loss
    }

    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const balanceUpdate = await updateUserBalanceAndLedger(client, userId, payoutAmountLamports, ledgerOutcomeCode, { game_id_custom_field: gameId }, `RPS PvB: ${playerChoice} vs Bot ${botChoice}`);
        if (!balanceUpdate.success) throw new Error(balanceUpdate.error || "DB Error during RPS PvB payout.");
        finalUserBalance = balanceUpdate.newBalanceLamports; // Update for internal tracking, but won't be displayed
        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(() => {});
        console.error(`${logPrefix} CRITICAL DB error: ${e.message}`);
        rpsOutcome.description = (rpsOutcome.description || "") + `<br><br>⚠️ Critical error settling wager. Admin notified.`;
        if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL RPS PvB Payout Failure 🚨\nGame ID: <code>${escapeHTML(gameId)}</code>\nError: ${escapeHTML(e.message)}. Manual check needed.`, { parse_mode: 'HTML'});
    } finally { if (client) client.release(); }

    const titleResultHTML = `⚡️ <b>RPS PvB - The Dust Settles!</b> ⚡️`;
    const resultMessageHTML = `${titleResultHTML}\n\n${playerRefHTML} wagered <b>${escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'))}</b>.\n\n` +
        `Your masterful choice: ${RPS_EMOJIS[playerChoice]} <b>${escapeHTML(rpsOutcome.player1.choiceFormatted)}</b>\n` +
        `The Bot Dealer's cunning play: ${RPS_EMOJIS[botChoice]} <b>${escapeHTML(rpsOutcome.player2.choiceFormatted)}</b>\n\n` +
        `<i>${rpsOutcome.description}</i>\n\n` + // Contains "Player X is the winner" with specific names
        `${financialOutcomeText}`; // Clear statement of winnings/loss/draw
        // Balance display line removed

    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.RPS_PVB, betAmount);
    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(resultMessageHTML, { chat_id: chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'HTML', reply_markup: postGameKeyboard }).catch(async (e)=>{
             if (!e.message?.includes("message is not modified")) await safeSendMessage(chatId, resultMessageHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
        });
    } else {
        await safeSendMessage(chatId, resultMessageHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
    }
}


// --- RPS Player vs. Player (PvP) Logic (Secret Choices) ---
async function startRPSPvPGame(
    initiatorData, // Can be original unified offerData or an object with { initiatorUserObj, betAmount, chatId, chatType }
    joinerUserObjFull, 
    originalOfferMessageId, // Message ID of the unified offer (if applicable)
    betsAlreadyDeducted = false // New flag
) {
    const logPrefix = `[RPS_PvP_Start_NoDoubleBet Init:${initiatorData.initiatorUserObj?.telegram_id || initiatorData.initiatorId} Join:${joinerUserObjFull.id || joinerUserObjFull.telegram_id}]`;

    const chatId = initiatorData.chatId;
    const betAmount = BigInt(initiatorData.betAmount);
    let initiatorUserObj = initiatorData.initiatorUserObj; // Should have updated balance if betsAlreadyDeducted is true
    const initiatorId = String(initiatorUserObj.id || initiatorUserObj.telegram_id);
    const initiatorMentionHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));
    const joinerId = String(joinerUserObjFull.id || joinerUserObjFull.telegram_id);
    const joinerMentionHTML = escapeHTML(getPlayerDisplayReference(joinerUserObjFull));

    if (originalOfferMessageId && bot) {
        await bot.deleteMessage(chatId, Number(originalOfferMessageId)).catch(() => {});
    }
    if (initiatorData.gameId && initiatorData.type === GAME_IDS.RPS_UNIFIED_OFFER && activeGames.has(initiatorData.gameId)) {
        activeGames.delete(initiatorData.gameId);
        console.log(`${logPrefix} Deleted unified offer ${initiatorData.gameId} as RPS PvP game starts.`);
    }

    let client;
    const pvpGameId = generateGameId(GAME_IDS.RPS_PVP);

    if (!betsAlreadyDeducted) {
        console.log(`${logPrefix} Bets not yet deducted for RPS PvP game ${pvpGameId}.`);
        try {
            client = await pool.connect(); await client.query('BEGIN');

            const currentInitiatorForBetDeduction = await getOrCreateUser(initiatorId);
            const currentJoinerForBetDeduction = await getOrCreateUser(joinerId);

            if (!currentInitiatorForBetDeduction || BigInt(currentInitiatorForBetDeduction.balance) < betAmount) {
                 throw new Error(`Initiator ${initiatorMentionHTML} has insufficient funds for RPS PvP bet.`);
            }
            const joinerBalanceToCheck = betsAlreadyDeducted ? BigInt(joinerUserObjFull.balance) : BigInt(currentJoinerForBetDeduction.balance);
            if (joinerBalanceToCheck < betAmount) { 
                 throw new Error(`Joiner ${joinerMentionHTML} has insufficient funds for RPS PvP bet.`);
            }

            const initBetRes = await updateUserBalanceAndLedger(client, initiatorId, BigInt(-betAmount), 'bet_placed_rps_pvp_init', { game_id_custom_field: pvpGameId, opponent_id_custom_field: joinerId }, `PvP RPS bet vs ${joinerMentionHTML}`);
            if (!initBetRes.success) throw new Error(`Initiator bet failed: ${initBetRes.error}`);
            initiatorUserObj.balance = initBetRes.newBalanceLamports; 

            const joinBetRes = await updateUserBalanceAndLedger(client, joinerId, BigInt(-betAmount), 'bet_placed_rps_pvp_join', { game_id_custom_field: pvpGameId, opponent_id_custom_field: initiatorId }, `PvP RPS bet vs ${initiatorMentionHTML}`);
            if (!joinBetRes.success) throw new Error(`Joiner bet failed: ${joinBetRes.error}`);
            joinerUserObjFull.balance = joinBetRes.newBalanceLamports; 
            await client.query('COMMIT');
        } catch (error) {
            if (client) await client.query('ROLLBACK').catch(() => {});
            console.error(`${logPrefix} DB error placing PvP RPS bets: ${error.message}`);
            await safeSendMessage(chatId, `⚙️ Database error placing bets for PvP RPS. Game cannot start. Bets have *not* been processed. Please try the offer again.`, { parse_mode: 'HTML' });
            return;
        } finally { 
            if (client) client.release(); 
        }
    } else {
        console.log(`${logPrefix} Bets already deducted for RPS PvP game ${pvpGameId}. Using provided player objects.`);
    }

    const gameDataPvP = {
        type: GAME_IDS.RPS_PVP, gameId: pvpGameId, chatId, betAmount,
        p1: { userId: initiatorId, mentionHTML: initiatorMentionHTML, userObj: initiatorUserObj, choice: null, hasChosen: false },
        p2: { userId: joinerId, mentionHTML: joinerMentionHTML, userObj: joinerUserObjFull, choice: null, hasChosen: false },
        status: 'pvp_p1_choosing', 
        gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(pvpGameId, gameDataPvP);
    await updateGroupGameDetails(chatId, pvpGameId, GAME_IDS.RPS_PVP, betAmount);
    await updateRPSPvPGameMessage(gameDataPvP); 
}

async function updateRPSPvPGameMessage(gameData) {
    const logPrefix = `[RPS_PvP_UpdateMsg GID:${gameData.gameId}]`;
    if (gameData.gameMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.gameMessageId)).catch(()=>{/*ignore error if already deleted*/});
        gameData.gameMessageId = null;
    }

    const p1 = gameData.p1; const p2 = gameData.p2;
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    let titleHTML = `🌌✨ <b>High Stakes RPS Duel: ${p1.mentionHTML} vs ${p2.mentionHTML}!</b> ✨🌌`;
    let textHTML = `${titleHTML}\nWager of Doom: <b>${betDisplayHTML}</b> each!\n\n`;
    let keyboard = null;

    // Player Status Display
    textHTML += `<b>${p1.mentionHTML} (P1):</b> ${p1.hasChosen ? "✅ Choice Locked!" : "🤔 Strategizing..."}\n`;
    textHTML += `<b>${p2.mentionHTML} (P2):</b> ${p2.hasChosen ? "✅ Choice Locked!" : (p1.hasChosen ? "🤔 Strategizing..." : "⏳ Waiting for P1...")}\n\n`;

    if (gameData.status === 'pvp_p1_choosing') {
        textHTML += `🔥 ${p1.mentionHTML}, the arena awaits your command! Click your SECRET choice below. Your move will be hidden until both players have chosen!`;
        keyboard = { inline_keyboard: [[
            { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_pvp_choice:${gameData.gameId}:${p1.userId}:${RPS_CHOICES.ROCK}` },
            { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_pvp_choice:${gameData.gameId}:${p1.userId}:${RPS_CHOICES.PAPER}` },
            { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_pvp_choice:${gameData.gameId}:${p1.userId}:${RPS_CHOICES.SCISSORS}` }
        ]]};
    } else if (gameData.status === 'pvp_p2_choosing') {
        textHTML += `⚡️ ${p1.mentionHTML} has committed their strategy! Now, ${p2.mentionHTML}, it's your turn to make your SECRET choice. May your wisdom prevail!`;
        keyboard = { inline_keyboard: [[
            { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_pvp_choice:${gameData.gameId}:${p2.userId}:${RPS_CHOICES.ROCK}` },
            { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_pvp_choice:${gameData.gameId}:${p2.userId}:${RPS_CHOICES.PAPER}` },
            { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_pvp_choice:${gameData.gameId}:${p2.userId}:${RPS_CHOICES.SCISSORS}` }
        ]]};
    } else if (gameData.status === 'pvp_reveal') {
        textHTML += `Decision time is over! Both warriors have made their move. The moment of truth arrives... Unveiling the clash! 💥`;
        // No keyboard here, as it will quickly transition to finalize
    }
     if(keyboard && keyboard.inline_keyboard) { // Add rules button if there are other buttons
        keyboard.inline_keyboard.push([{ text: "📖 Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.RPS_UNIFIED_OFFER}` }]);
    }


    const sentMessage = await safeSendMessage(gameData.chatId, textHTML, { parse_mode: 'HTML', reply_markup: keyboard });
    if (sentMessage?.message_id) {
        const currentGameData = activeGames.get(gameData.gameId); // Re-fetch, gameData might be stale if an async operation happened elsewhere
        if(currentGameData){
            currentGameData.gameMessageId = String(sentMessage.message_id);
            activeGames.set(gameData.gameId, currentGameData); // Update with new message ID
        }
    } else {
        console.error(`${logPrefix} Failed to send/update RPS PvP game message for ${gameData.gameId}.`);
    }
}

async function handleRPSPvPChoiceCallback(gameId, chooserId, choiceKey, userObj, originalMessageId, callbackQueryId) {
    const userIdMakingChoice = String(userObj.id || userObj.telegram_id);
    const logPrefix = `[RPS_PvPChoiceCB GID:${gameId} UID:${userIdMakingChoice} ChosenID:${chooserId} Choice:${choiceKey}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.RPS_PVP) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This RPS PvP game is no longer active.", show_alert: true }).catch(() => {});
        return;
    }
    if (userIdMakingChoice !== chooserId) { 
        await bot.answerCallbackQuery(callbackQueryId, { text: "This is not your turn to choose.", show_alert: true }).catch(() => {});
        return;
    }

    let playerObj;
    if (gameData.status === 'pvp_p1_choosing' && gameData.p1.userId === chooserId && !gameData.p1.hasChosen) {
        playerObj = gameData.p1;
    } else if (gameData.status === 'pvp_p2_choosing' && gameData.p2.userId === chooserId && !gameData.p2.hasChosen) {
        playerObj = gameData.p2;
    } else {
        await bot.answerCallbackQuery(callbackQueryId, { text: "It's not the right time or you've already chosen.", show_alert: false }).catch(() => {});
        return;
    }

    playerObj.choice = choiceKey;
    playerObj.hasChosen = true;
    const choiceDisplay = choiceKey.charAt(0).toUpperCase() + choiceKey.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, { text: `Your choice ${RPS_EMOJIS[choiceKey]} ${choiceDisplay} is locked in secretly!` }).catch(() => {});
    gameData.lastInteractionTime = Date.now();

    if (gameData.p1.hasChosen && gameData.p2.hasChosen) {
        gameData.status = 'pvp_reveal';
        activeGames.set(gameData.gameId, gameData);
        await updateRPSPvPGameMessage(gameData); 
        await sleep(3000); 
        await finalizeRPSPvPGame(gameData);
    } else if (gameData.p1.hasChosen && !gameData.p2.hasChosen) {
        gameData.status = 'pvp_p2_choosing';
        activeGames.set(gameData.gameId, gameData);
        await updateRPSPvPGameMessage(gameData); 
    } else {
        // Should not happen if P1 must choose first
        activeGames.set(gameData.gameId, gameData);
        await updateRPSPvPGameMessage(gameData);
    }
}

async function finalizeRPSPvPGame(gameData) {
    const { gameId, chatId, betAmount, p1, p2 } = gameData;
    const logPrefix = `[RPS_PvP_Finalize_V3 GID:${gameId}]`;
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);

    if (!p1.choice || !p2.choice) {
        console.error(`${logPrefix} Finalize called but one or both choices are missing! P1: ${p1.choice}, P2: ${p2.choice}. Refunding.`);
        let clientRefund;
        try {
            clientRefund = await pool.connect(); await clientRefund.query('BEGIN');
            await updateUserBalanceAndLedger(clientRefund, p1.userId, betAmount, 'refund_rps_pvp_incomplete', { game_id_custom_field: gameId }, `PvP RPS refund due to incomplete choices.`);
            await updateUserBalanceAndLedger(clientRefund, p2.userId, betAmount, 'refund_rps_pvp_incomplete', { game_id_custom_field: gameId }, `PvP RPS refund due to incomplete choices.`);
            await clientRefund.query('COMMIT');
        } catch (e) { if (clientRefund) await clientRefund.query('ROLLBACK'); console.error(`${logPrefix} Error refunding incomplete PvP RPS: ${e.message}`); }
        finally { if (clientRefund) clientRefund.release(); }
        await safeSendMessage(chatId, "⚙️ RPS PvP game ended prematurely due to missing choices. Bets have been refunded.", {parse_mode: "HTML"});
        return;
    }

    const rpsOutcome = determineRPSOutcome(p1.choice, p2.choice, p1.mentionHTML, p2.mentionHTML);
    let p1Payout = 0n; let p2Payout = 0n;
    let p1Ledger = `loss_rps_pvp_${p1.choice}_vs_${p2.choice}`;
    let p2Ledger = `loss_rps_pvp_${p2.choice}_vs_${p1.choice}`;
    let finalP1Balance = BigInt(p1.userObj.balance); // For internal tracking, not display
    let finalP2Balance = BigInt(p2.userObj.balance); // For internal tracking, not display

    let financialOutcomeTextPvP = "";
    const totalPotDisplay = escapeHTML(await formatBalanceForDisplay(betAmount * 2n, 'USD'));
    const singleBetDisplay = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'));

    if (rpsOutcome.result === 'win_player1') { // p1 wins
        p1Payout = betAmount * 2n;
        financialOutcomeTextPvP = `${p1.mentionHTML} wins the pot of <b>${totalPotDisplay}</b>!`;
        p1Ledger = `win_rps_pvp_${p1.choice}_vs_${p2.choice}`;
    } else if (rpsOutcome.result === 'win_player2') { // p2 wins
        p2Payout = betAmount * 2n;
        financialOutcomeTextPvP = `${p2.mentionHTML} wins the pot of <b>${totalPotDisplay}</b>!`;
        p2Ledger = `win_rps_pvp_${p2.choice}_vs_${p1.choice}`;
    } else if (rpsOutcome.result === 'draw') {
        p1Payout = betAmount; p2Payout = betAmount;
        financialOutcomeTextPvP = `It's a draw! Bets of <b>${singleBetDisplay}</b> each are returned.`;
        p1Ledger = `draw_rps_pvp_${p1.choice}_vs_${p2.choice}`;
        p2Ledger = `draw_rps_pvp_${p2.choice}_vs_${p1.choice}`;
    }

    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const p1Update = await updateUserBalanceAndLedger(client, p1.userId, p1Payout, p1Ledger, { game_id_custom_field: gameId, opponent_id_custom_field: p2.userId }, `PvP RPS result: P1(${p1.choice}) vs P2(${p2.choice})`);
        if (!p1Update.success) throw new Error(`P1 balance update failed: ${p1Update.error}`);
        finalP1Balance = p1Update.newBalanceLamports;

        const p2Update = await updateUserBalanceAndLedger(client, p2.userId, p2Payout, p2Ledger, { game_id_custom_field: gameId, opponent_id_custom_field: p1.userId }, `PvP RPS result: P2(${p2.choice}) vs P1(${p1.choice})`);
        if (!p2Update.success) throw new Error(`P2 balance update failed: ${p2Update.error}`);
        finalP2Balance = p2Update.newBalanceLamports;
        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(() => {});
        console.error(`${logPrefix} CRITICAL DB error: ${e.message}`);
        rpsOutcome.description = (rpsOutcome.description || "") + `<br><br>⚠️ Critical error settling wagers. Admin notified.`;
        financialOutcomeTextPvP = `⚠️ Critical error settling wagers. Admin notified.`; // Overwrite financial outcome on DB error
        if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL RPS PvP Payout Failure 🚨\nGame ID: <code>${escapeHTML(gameId)}</code>\nError: ${escapeHTML(e.message)}. Manual check needed for P1:${p1.userId}, P2:${p2.userId}.`, { parse_mode: 'HTML'});
    } finally { if (client) client.release(); }

    const titleResultHTML = `💥✨ <b>RPS PvP - The Reckoning!</b> ✨💥`;
    const resultMessageHTML = `${titleResultHTML}\n\n` +
        `The dust settles between ${p1.mentionHTML} and ${p2.mentionHTML} (Wager: <b>${escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'))}</b> each)!\n\n` +
        `<b>${p1.mentionHTML} (P1) secretly chose:</b> ${RPS_EMOJIS[p1.choice]} <b>${escapeHTML(rpsOutcome.player1.choiceFormatted)}</b>\n` +
        `<b>${p2.mentionHTML} (P2) secretly chose:</b> ${RPS_EMOJIS[p2.choice]} <b>${escapeHTML(rpsOutcome.player2.choiceFormatted)}</b>\n\n` +
        `<i>${rpsOutcome.description}</i>\n\n` + // Contains "Player X is the winner" with actual names
        `${financialOutcomeTextPvP}`; // Clear statement of who won what, or draw outcome
        // Balance display lines removed

    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.RPS_PVP, betAmount);
    if (gameData.gameMessageId && bot) {
        await bot.editMessageText(resultMessageHTML, { chat_id: chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'HTML', reply_markup: postGameKeyboard }).catch(async (e)=>{
             if (!e.message?.includes("message is not modified")) await safeSendMessage(chatId, resultMessageHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
        });
    } else {
        await safeSendMessage(chatId, resultMessageHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
    }
}
// --- End of REVISED Rock Paper Scissors (RPS) Game Logic & Handlers ---

// --- End of REVISED Part 5a, Section 3 ---
// --- Start of Part 5b, Section 1 (COMPLETE DICE ESCALATOR LOGIC - HTML Revamp V4 - PvP State Alignment) ---
// index.js - Part 5b, Section 1: Dice Escalator Game Logic & Handlers
//----------------------------------------------------------------------------------------------

// Assumed dependencies from previous Parts are available

// --- Constants specific to New Dice Escalator ---
const DE_PVB_BOT_ROLL_COUNT = 3;
const BUST_MESSAGE_DELAY_MS = 1500;

// --- Helper Function for DE Game Message Formatting (RETURNS HTML) ---
async function formatDiceEscalatorGameMessage_New(gameData) {
    let messageTextHTML = "";
    let jackpotDisplayHTML = "";
    const LOG_PREFIX_FORMAT_DE_HTML_V4 = `[FormatDE_Msg_HTML_V4 GID:${gameData.gameId}]`;

    if (gameData.type === GAME_IDS.DICE_ESCALATOR_PVB) {
        try {
            const jackpotResult = await queryDatabase('SELECT current_amount FROM jackpots WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]);
            if (jackpotResult.rows.length > 0 && jackpotResult.rows[0].current_amount) {
                const jackpotAmountLamports = BigInt(jackpotResult.rows[0].current_amount);
                if (jackpotAmountLamports > 0n) {
                    const jackpotUSD_HTML = escapeHTML(await formatBalanceForDisplay(jackpotAmountLamports, 'USD'));
                    const jackpotSOL_HTML = escapeHTML(formatCurrency(jackpotAmountLamports, 'SOL'));
                    jackpotDisplayHTML = `\n\n<pre>🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇\n` +
                                       `  💎   SUPER JACKPOT ALERT!  💎\n` +
                                       `🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇🎇</pre>\n` +
                                       `Current Prize: 🔥<b>${jackpotUSD_HTML}</b>🔥\n` +
                                       `(<i>Approx. ${jackpotSOL_HTML}</i>)\n` +
                                       `Score <b>${escapeHTML(String(TARGET_JACKPOT_SCORE))}+</b> & beat the Bot to WIN IT ALL!\n`;
                } else {
                     jackpotDisplayHTML = `\n\n💎 The Super Jackpot is currently <b>${escapeHTML(await formatBalanceForDisplay(0n, 'USD'))}</b>. Be the first to build it up!\n`;
                }
            }
        } catch (error) {
            console.error(`${LOG_PREFIX_FORMAT_DE_HTML_V4} Error fetching jackpot for display: ${error.message}`);
            jackpotDisplayHTML = "\n\n⚠️ <i>Jackpot status temporarily unavailable.</i>\n";
        }
    }

    const betDisplaySOL_HTML = escapeHTML(formatCurrency(gameData.betAmount, 'SOL'));
    const betUsdDisplay_HTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    let playerRefHTML = "Player";
    if (gameData.type === GAME_IDS.DICE_ESCALATOR_PVB && gameData.player && gameData.player.displayName) {
        playerRefHTML = escapeHTML(gameData.player.displayName);
    } else if (gameData.initiator && gameData.initiator.displayName) {
        playerRefHTML = escapeHTML(gameData.initiator.displayName);
    }


    if (gameData.type === GAME_IDS.DICE_ESCALATOR_PVB) {
        const player = gameData.player;
        messageTextHTML = `🎲 💎 <b>Dice Escalator vs. Bot Dealer</b> 💎 🎲\n\n` +
                          `<b>Player</b>: ${playerRefHTML}\n` +
                          `<b>Wager</b>: <b>${betUsdDisplay_HTML}</b> (<i>${betDisplaySOL_HTML}</i>)\n` +
                          `${jackpotDisplayHTML}\n`+
                          `Your Score: <b>${player.score}</b>\n`;
        if (player.rolls && player.rolls.length > 0) {
            messageTextHTML += `Your Rolls: ${formatDiceRolls(player.rolls)}\n`;
        }

        if (gameData.status === 'player_score_18_plus_awaiting_choice') {
            messageTextHTML += `\n\n⚠️ <b>DECISION TIME!</b> Score: <b>${player.score}</b> ⚠️\n` +
                               `You can:\n` +
                               `1. ✋ <b>Stand Firm:</b> Lock in your score.\n` +
                               `2. 🔥 <b>Go for Jackpot!:</b> Roll for <b>${escapeHTML(String(TARGET_JACKPOT_SCORE))}+</b>! (<i>No standing after this choice!</i>)\n\n` +
                               `What's your strategy, <b>${playerRefHTML}</b>?`;
        } else if (gameData.status === 'player_turn_awaiting_emoji') {
            if (player.isGoingForJackpot) {
                messageTextHTML += `\n\n🔥🔥 <b>JACKPOT RUN!</b> 🔥🔥\nScore: <b>${player.score}</b> (Target: ${escapeHTML(String(TARGET_JACKPOT_SCORE))}+)\n` +
                                   `<i>No turning back! Send 🎲 to roll again!</i>`;
            } else {
                messageTextHTML += `\n👉 <b>${playerRefHTML}</b>, it's your turn! Send a 🎲 emoji to roll.\n` +
                                   `<i>Or press "Stand Firm" below.</i>`;
            }
        } else if (gameData.status === 'player_stood') {
            messageTextHTML += `\n✅ You stood with <b>${player.score}</b> points!\n`+
                               `<i>🤖 The Bot Dealer is now making its move...</i>`;
        } else if (gameData.status === 'bot_turn_complete') {
            messageTextHTML += `\n🤖 Bot Dealer's Rolls: ${formatDiceRolls(gameData.botRolls || [])}\n` +
                               `Bot's Final Score: <b>${gameData.botScore || 0}</b>\n\n` +
                               `<i>⏳ Calculating results...</i>`;
        } else if (gameData.status === 'player_busted') {
            messageTextHTML += `\n💥 <b>Oh no, a BUST!</b> You rolled a <code>${escapeHTML(String(gameData.lastPlayerRoll))}</code> (bust on <code>${escapeHTML(String(DICE_ESCALATOR_BUST_ON))}</code>).\n<i>Bot Dealer wins this round.</i>`;
        }
    } else if (gameData.type === GAME_IDS.DICE_ESCALATOR_PVP) {
        const p1 = gameData.initiator;
        const p2 = gameData.opponent;
        const p1MentionHTML = escapeHTML(p1.displayName);
        const p2MentionHTML = escapeHTML(p2.displayName);
        const totalPotUsdDisplay_HTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount * 2n, 'USD'));

        messageTextHTML = `⚔️ <b>Dice Escalator PvP Challenge!</b> ⚔️\n` +
                          `<i>${p1MentionHTML} vs ${p2MentionHTML}</i>\n\n` +
                          `<b>Wager</b>: ${betUsdDisplay_HTML} each\n`+
                          `<b>Total Pot</b>: <b>${totalPotUsdDisplay_HTML}</b>\n\n` +
                          `--- <b>Current Scores</b> ---\n`;
        messageTextHTML += `👤 <b>${p1MentionHTML}</b> (P1): ${formatDiceRolls(p1.rolls)} Score: <b>${p1.score}</b> ${p1.busted ? "💥 BUSTED!" : (p1.stood ? "✅ Stood" : (p1.status === 'awaiting_roll_emoji' ? "🎲 Rolling..." : ""))}\n`;
        messageTextHTML += `👤 <b>${p2MentionHTML}</b> (P2): ${formatDiceRolls(p2.rolls)} Score: <b>${p2.score}</b> ${p2.busted ? "💥 BUSTED!" : (p2.stood ? "✅ Stood" : (p2.status === 'awaiting_roll_emoji' ? "🎲 Rolling..." : (p2.status === 'waiting_turn' ? "<i>Waiting...</i>" : "")) )}\n\n`;
        
        let actionPromptHTML = "<i>Waiting for player action...</i>";
        
        if (p1.isTurn && p1.status === 'awaiting_roll_emoji') {
            actionPromptHTML = `👉 <b>${p1MentionHTML}</b> (P1), it's your turn! Send 🎲 to roll, or use "Stand" below.`;
        } else if (p2.isTurn && p2.status === 'awaiting_roll_emoji') {
            actionPromptHTML = `👉 <b>${p2MentionHTML}</b> (P2), ${p1MentionHTML} (P1) ${p1.stood ? `stands at <b>${p1.score}</b>` : `busted`}. Your turn! Send 🎲 or "Stand".`;
        } else if (gameData.status === 'p1_stood' && p2.status === 'waiting_turn') { // P1 stood, explicitly prompt P2
            actionPromptHTML = `✅ <b>${p1MentionHTML}</b> (P1) stands with <b>${p1.score}</b>!\n<b>${p2MentionHTML}</b> (P2), your turn to conquer! Send 🎲 to roll or "Stand"!`;
        } else if (gameData.status.startsWith('game_over')) {
            actionPromptHTML = "<i>🏁 Game Over! Calculating final results...</i> ⏳";
        }
        messageTextHTML += actionPromptHTML;
    }
    return messageTextHTML.trim();
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

async function handleStartDiceEscalatorUnifiedOfferCommand_New(msg, betAmountLamports, targetUsernameRaw = null) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const logPrefix = `[DE_OfferOrDirect_V2 UID:${userId} CH:${chatId}]`;

    console.log(`${logPrefix} Initiating with targetUsernameRaw: '${targetUsernameRaw}'`);

    if (chatType === 'private') {
        await safeSendMessage(chatId, `🎲 The Dice Escalator arena is in <b>group chats</b>! Use <code>/de &lt;bet&gt; [@username]</code> there.`, { parse_mode: 'HTML' });
        return;
    }

    let initiatorUserObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!initiatorUserObj) {
        await safeSendMessage(chatId, "⚠️ Error fetching your player profile. Please try <code>/start</code> with me first.", { parse_mode: 'HTML' });
        return;
    }
    const initiatorPlayerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        await safeSendMessage(chatId, `💰 ${initiatorPlayerRefHTML}, your balance is too low for a <b>${betDisplayUSD_HTML}</b> Dice Escalator game! You need approx. <b>${escapeHTML(await formatBalanceForDisplay(needed, 'USD'))}</b> more.`, {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: "💸 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameSession = await getGroupSession(chatId, msg.chat.title || `Group Chat ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        if ( (existingGame.type.includes('_offer') && existingGame.status === 'pending_offer') || 
             (existingGame.type === GAME_IDS.DIRECT_PVP_CHALLENGE && existingGame.status === 'pending_direct_challenge_response') ||
             (existingGame.type.includes('_pvp') && !existingGame.status?.startsWith('game_over_')) ||
             (existingGame.type === GAME_IDS.MINES && existingGame.status !== 'game_over_mine_hit' && existingGame.status !== 'game_over_cashed_out')
           ) {
            await safeSendMessage(chatId, `⏳ Hold your dice, ${initiatorPlayerRefHTML}! An interactive game or challenge (<code>${escapeHTML(existingGame.type.replace(/_/g, " "))}</code>) is already active. Please wait.`, { parse_mode: 'HTML' });
            return;
        }
    }

    let targetUserObjectForChallenge = null;
    if (targetUsernameRaw) {
        console.log(`${logPrefix} targetUsernameRaw is '${targetUsernameRaw}'. Attempting findRecipientUser.`);
        targetUserObjectForChallenge = await findRecipientUser(targetUsernameRaw);
        console.log(`${logPrefix} findRecipientUser returned: ${targetUserObjectForChallenge ? `User ID: ${targetUserObjectForChallenge.telegram_id}` : 'null'}`);
        
        if (!targetUserObjectForChallenge || !targetUserObjectForChallenge.telegram_id) { 
            await safeSendMessage(chatId, `😕 Player ${escapeHTML(targetUsernameRaw)} not found or has an invalid ID. Cannot create a Dice Escalator direct challenge. Creating a general offer instead.`, { parse_mode: 'HTML' });
            targetUserObjectForChallenge = null; 
            console.log(`${logPrefix} Target invalid or no telegram_id. Forcing unified offer.`);
        } else if (String(targetUserObjectForChallenge.telegram_id) === userId) {
            await safeSendMessage(chatId, `😅 You can't directly challenge yourself to Dice Escalator, ${initiatorPlayerRefHTML}! Creating a general offer.`, { parse_mode: 'HTML' });
            targetUserObjectForChallenge = null; 
            console.log(`${logPrefix} Self-challenge attempted. Forcing unified offer.`);
        } else {
            console.log(`${logPrefix} Valid target found for direct challenge: ${targetUserObjectForChallenge.telegram_id}`);
        }
    } else {
        console.log(`${logPrefix} No targetUsernameRaw provided. Will proceed to unified offer if not already returned.`);
    }

    if (targetUserObjectForChallenge && targetUserObjectForChallenge.telegram_id) { 
        // --- DIRECT PvP CHALLENGE FLOW for Dice Escalator ---
        console.log(`${logPrefix} Entering DIRECT PvP Challenge Flow for target User ID: ${targetUserObjectForChallenge.telegram_id}`);
        const targetPlayerRefHTML = escapeHTML(getPlayerDisplayReference(targetUserObjectForChallenge));
        const offerId = generateGameId(`dco_${userId.slice(-3)}_${String(targetUserObjectForChallenge.telegram_id).slice(-3)}`); 

        const groupChallengeTextHTML = `Hey ${targetPlayerRefHTML}❗\n\n${initiatorPlayerRefHTML} has challenged you to a <b>Dice Escalator</b> duel for <b>${betDisplayUSD_HTML}</b>!`;
        
        const groupChallengeKeyboard = { 
            inline_keyboard: [
                [{ text: "✅ Accept Challenge", callback_data: `dir_chal_acc:${offerId}` }],
                [{ text: "❌ Decline Challenge", callback_data: `dir_chal_dec:${offerId}` }],
                [{ text: "🚫 Withdraw My Challenge", callback_data: `dir_chal_can:${offerId}` }]
            ]
        };

        const sentGroupMessage = await safeSendMessage(chatId, groupChallengeTextHTML, { parse_mode: 'HTML', reply_markup: groupChallengeKeyboard });

        if (!sentGroupMessage || !sentGroupMessage.message_id) {
            console.error(`${logPrefix} Failed to send direct Dice Escalator challenge message for offer ${offerId}.`);
            await safeSendMessage(chatId, `⚙️ Oops! Couldn't send your Dice Escalator challenge to ${targetPlayerRefHTML}. Please try again.`, { parse_mode: 'HTML' });
            return;
        }
        const offerMessageIdInGroup = String(sentGroupMessage.message_id);
        const groupNameHTML = escapeHTML(msg.chat.title || "the group");

        const dmNotificationTextHTML = `🔔 Challenge Alert!\n\nHi ${targetPlayerRefHTML},\n${initiatorPlayerRefHTML} has challenged you to a <b>Dice Escalator</b> game for <b>${betDisplayUSD_HTML}</b> in the group "<b>${groupNameHTML}</b>".\n\nPlease head to that group to accept or decline the challenge.`;
        
        const dmSent = await safeSendMessage(targetUserObjectForChallenge.telegram_id, dmNotificationTextHTML, { parse_mode: 'HTML' });
        if (!dmSent) {
            console.warn(`${logPrefix} Failed to send DM notification for direct Dice Escalator challenge to target ${targetUserObjectForChallenge.telegram_id}. Offer still posted in group.`);
            await safeSendMessage(chatId, `ℹ️ ${initiatorPlayerRefHTML}, your challenge to ${targetPlayerRefHTML} is posted! Note: They might not receive a DM if they haven't interacted with me before.`, { parse_mode: 'HTML'});
        }

        const directOfferData = {
            type: GAME_IDS.DIRECT_PVP_CHALLENGE, 
            offerId: offerId, 
            gameId: offerId, 
            initiatorId: userId,
            initiatorUserObj: initiatorUserObj, 
            initiatorMentionHTML: initiatorPlayerRefHTML,
            targetUserId: String(targetUserObjectForChallenge.telegram_id),
            targetUserObj: targetUserObjectForChallenge, 
            targetUserMentionHTML: targetPlayerRefHTML,
            betAmount: betAmountLamports,
            originalGroupId: chatId,
            offerMessageIdInGroup: offerMessageIdInGroup,
            chatTitle: msg.chat.title || `Group Chat ${chatId}`, 
            status: 'pending_direct_challenge_response',
            gameToStart: GAME_IDS.DICE_ESCALATOR_PVP, 
            creationTime: Date.now()
        };
        activeGames.set(offerId, directOfferData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.DIRECT_PVP_CHALLENGE, betAmountLamports);
        console.log(`${logPrefix} Direct Dice Escalator challenge offer ${offerId} created and stored.`);

        setTimeout(async () => {
            const timedOutOffer = activeGames.get(offerId);
            const currentTimeForTimeout = new Date().toISOString();
            console.log(`[DE_DirectChallengeTimeout @ ${currentTimeForTimeout}] Timeout for Offer ID: ${offerId}. JOIN_GAME_TIMEOUT_MS: ${JOIN_GAME_TIMEOUT_MS}.`);
            console.log(`[DE_DirectChallengeTimeout] Offer found in activeGames: ${!!timedOutOffer}.`);
            if (timedOutOffer) {
                console.log(`[DE_DirectChallengeTimeout] Offer Details: type=${timedOutOffer.type}, status=${timedOutOffer.status}`);
            }

            if (timedOutOffer && timedOutOffer.status === 'pending_direct_challenge_response' && timedOutOffer.type === GAME_IDS.DIRECT_PVP_CHALLENGE) {
                console.log(`[DE_DirectChallengeTimeout OfferID:${offerId}] Conditions MET. Deleting expired direct challenge.`);
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null);
                const gameNameForTimeout = "Dice Escalator"; 
                const timeoutBetDisplay = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                const timeoutMsgHTML = `⏳ The ${gameNameForTimeout} challenge from ${timedOutOffer.initiatorMentionHTML} to ${timedOutOffer.targetUserMentionHTML} for <b>${timeoutBetDisplay}</b> has expired unanswered.`;
                if (bot && timedOutOffer.offerMessageIdInGroup) {
                    await bot.editMessageText(timeoutMsgHTML, {
                        chat_id: timedOutOffer.originalGroupId, message_id: Number(timedOutOffer.offerMessageIdInGroup),
                        parse_mode: 'HTML', reply_markup: {} 
                    }).catch(e => { 
                        console.warn(`${logPrefix} Failed to edit expired direct DE challenge message ${timedOutOffer.offerMessageIdInGroup}: ${e.message}. Sending new.`);
                        safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                    });
                } else { 
                    safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                }
                if (timedOutOffer.initiatorId) { 
                    await safeSendMessage(timedOutOffer.initiatorId, `⏳ Your Dice Escalator challenge to ${timedOutOffer.targetUserMentionHTML} in group "${escapeHTML(timedOutOffer.chatTitle)}" has expired.`, { parse_mode: 'HTML' });
                }
            } else if (timedOutOffer) {
                console.log(`[DE_DirectChallengeTimeout OfferID:${offerId}] Conditions NOT MET for deletion. Status: ${timedOutOffer.status}, Type: ${timedOutOffer.type}`);
            } else {
                console.log(`[DE_DirectChallengeTimeout OfferID:${offerId}] Timeout fired but offer no longer in activeGames.`);
            }
        }, JOIN_GAME_TIMEOUT_MS);

    } else {
        // --- DICE ESCALATOR UNIFIED OFFER FLOW ---
        console.log(`${logPrefix} Initiating UNIFIED Dice Escalator offer.`);
        const offerId = generateGameId("de_uo"); 
        const offerMessageTextHTML =
            `🎲 <b>Dice Escalator Challenge by ${initiatorPlayerRefHTML}!</b> 🎲\n\n` +
            `Wager: <b>${betDisplayUSD_HTML}</b>\n\n` +
            `Do you want to challenge the Bot Dealer or another player? This offer expires in ${JOIN_GAME_TIMEOUT_MS / 1000 / 60} minutes.`;

        const offerKeyboard = {
            inline_keyboard: [
                [{ text: `⚔️ Accept PvP Challenge!`, callback_data: `de_accept_pvp_challenge:${offerId}` }],
                [{ text: `🤖 Challenge Bot Dealer`, callback_data: `de_accept_bot_game:${offerId}` }],
                [{ text: "🚫 Cancel My Offer", callback_data: `de_cancel_unified_offer:${offerId}` }]
            ]
        };

        const offerData = {
            type: GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER,
            gameId: offerId,
            chatId: String(chatId),
            chatType: chatType,
            initiatorId: userId,
            initiatorMentionHTML: initiatorPlayerRefHTML,
            initiatorUserObj: initiatorUserObj,
            betAmount: betAmountLamports,
            status: 'pending_unified_offer', 
            creationTime: Date.now(),
            gameSetupMessageId: null, 
        };
        activeGames.set(offerId, offerData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER, betAmountLamports);
        console.log(`${logPrefix} Unified Dice Escalator offer ${offerId} created and stored.`);

        const sentOfferMessage = await safeSendMessage(chatId, offerMessageTextHTML, { parse_mode: 'HTML', reply_markup: offerKeyboard });
        if (sentOfferMessage?.message_id) {
            const currentOffer = activeGames.get(offerId);
            if (currentOffer) {
                currentOffer.gameSetupMessageId = String(sentOfferMessage.message_id);
                activeGames.set(offerId, currentOffer);
                console.log(`${logPrefix} Unified offer message ID ${currentOffer.gameSetupMessageId} stored for offer ${offerId}. Setting timeout. JOIN_GAME_TIMEOUT_MS: ${JOIN_GAME_TIMEOUT_MS}`);
            } else {
                console.warn(`${logPrefix} Unified offer ${offerId} was not found in activeGames immediately after setting and sending message. Message: ${sentOfferMessage.message_id}`);
                if(bot && sentOfferMessage.message_id) await bot.deleteMessage(chatId, sentOfferMessage.message_id).catch(()=>{});
                return; 
            }

            setTimeout(async () => {
                const timedOutOffer = activeGames.get(offerId);
                const currentTimeForTimeout = new Date().toISOString();
                console.log(`[DE_UNIFIED_TIMEOUT @ ${currentTimeForTimeout}] Timeout for Offer ID: ${offerId}. JOIN_GAME_TIMEOUT_MS: ${JOIN_GAME_TIMEOUT_MS}.`);
                console.log(`[DE_UNIFIED_TIMEOUT] Offer found in activeGames: ${!!timedOutOffer}.`);
                if (timedOutOffer) {
                    console.log(`[DE_UNIFIED_TIMEOUT] Offer Details: type=${timedOutOffer.type}, status=${timedOutOffer.status}, initiatorId=${timedOutOffer.initiatorId}`);
                }
                
                if (timedOutOffer && timedOutOffer.status === 'pending_unified_offer' && timedOutOffer.type === GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER) {
                    console.log(`[DE_UNIFIED_TIMEOUT OfferID:${offerId}] Conditions MET. Deleting offer.`);
                    activeGames.delete(offerId);
                    await updateGroupGameDetails(chatId, null, null, null);
                    if (timedOutOffer.gameSetupMessageId && bot) {
                        const expiredOfferBetDisplayUSD = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                        await bot.editMessageText(
                            `⏳ The Dice Escalator offer by ${timedOutOffer.initiatorMentionHTML} for <b>${expiredOfferBetDisplayUSD}</b> has expired unanswered.`,
                            { chat_id: String(chatId), message_id: Number(timedOutOffer.gameSetupMessageId), parse_mode: 'HTML', reply_markup: {} }
                        ).catch(e => { console.warn(`${logPrefix} Failed to edit expired unified DE offer ${timedOutOffer.gameSetupMessageId}: ${e.message}`); });
                    }
                } else if (timedOutOffer) {
                    console.log(`[DE_UNIFIED_TIMEOUT OfferID:${offerId}] Conditions NOT MET for deletion. Offer Status: ${timedOutOffer.status}, Offer Type: ${timedOutOffer.type}`);
                } else {
                    console.log(`[DE_UNIFIED_TIMEOUT OfferID:${offerId}] Timeout fired but offer no longer in activeGames.`);
                }
            }, JOIN_GAME_TIMEOUT_MS);
        } else {
            console.error(`${logPrefix} Failed to send Dice Escalator unified offer message for ${offerId}. Cleaning up offer.`);
            activeGames.delete(offerId);
            await updateGroupGameDetails(chatId, null, null, null);
            await safeSendMessage(chatId, `⚙️ Oops! There was an issue creating the Dice Escalator offer by ${initiatorPlayerRefHTML}. Please try again.`, { parse_mode: 'HTML' });
            return;
        }
    }
}

async function handleDiceEscalatorAcceptBotGame_New(offerId, userWhoClicked, originalOfferMessageId, originalChatId, originalChatType, callbackQueryId) {
    const logPrefix = `[DE_AcceptBot UID:${userWhoClicked.telegram_id} OfferID:"${offerId}" CH:${originalChatId}]`;
    console.log(`${logPrefix} User attempting to accept Dice Escalator PvB game from unified offer.`);

    const offerData = activeGames.get(offerId);

    console.log(`${logPrefix} Trying to get offerId "${offerId}" from activeGames. activeGames keys (sample): ${JSON.stringify(Array.from(activeGames.keys()).slice(0, 5))}... (Total: ${activeGames.size})`);
    if (offerData) {
        console.log(`${logPrefix} Details of found offerData: type="${offerData.type}", status="${offerData.status}", gameId="${offerData.gameId}", initiatorId="${offerData.initiatorId}"`);
        console.log(`${logPrefix} Comparing offerData.type ("${offerData.type}") with GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER ("${GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER}")`);
        console.log(`${logPrefix} Comparing offerData.status ("${offerData.status}") with 'pending_unified_offer'`);
    } else {
        console.log(`${logPrefix} OfferData NOT FOUND in activeGames for offerId "${offerId}"`);
    }

    if (!offerData || offerData.type !== GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER || offerData.status !== 'pending_unified_offer') {
        console.warn(`${logPrefix} Offer ${offerId} not found, not a unified DE offer, or not pending. OfferData was: ${offerData ? `Type: ${offerData.type}, Status: ${offerData.status}` : 'Not Found'}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Dice Escalator offer has expired, is not valid, or has already been actioned.", show_alert: true });
        const messageIdToEdit = originalOfferMessageId || offerData?.gameSetupMessageId;
        if (bot && messageIdToEdit) {
            bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(messageIdToEdit) }).catch(() => {});
        }
        return;
    }

    // Only the initiator of the offer can choose to play against the bot from their own unified offer
    if (String(userWhoClicked.telegram_id) !== String(offerData.initiatorId)) {
        console.log(`${logPrefix} Clicker ${userWhoClicked.id} is not initiator ${offerData.initiatorId}. Ignoring PvB accept.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the player who made the offer can choose to play against the bot.", show_alert: true });
        return;
    }

    // Use the initiatorUserObj stored in the offerData
    const initiatorUserObj = offerData.initiatorUserObj; 
    if (!initiatorUserObj) {
        console.error(`${logPrefix} CRITICAL: initiatorUserObj missing from offerData for ${offerId}.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Error: Offer data is incomplete.", show_alert: true });
        return;
    }

    // Re-fetch initiator's balance to ensure it's current
    const currentInitiatorDetails = await getOrCreateUser(initiatorUserObj.telegram_id);
    if (!currentInitiatorDetails || BigInt(currentInitiatorDetails.balance) < offerData.betAmount) {
        const betDisplay = await formatBalanceForDisplay(offerData.betAmount, 'USD');
        console.warn(`${logPrefix} Initiator ${initiatorUserObj.telegram_id} now has insufficient funds for ${betDisplay}.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: `Your balance is too low for the ${betDisplay} bet. Offer cancelled.`, show_alert: true });
        activeGames.delete(offerId);
        await updateGroupGameDetails(originalChatId, null, null, null);
        const messageIdToEdit = originalOfferMessageId || offerData.gameSetupMessageId;
        if (bot && messageIdToEdit) {
             await bot.editMessageText(`🎲 Offer by ${offerData.initiatorMentionHTML} for <b>${betDisplay}</b> was cancelled due to insufficient funds.`, {
                chat_id: originalChatId, message_id: Number(messageIdToEdit), parse_mode: 'HTML', reply_markup: {}
            }).catch(() => {});
        }
        return;
    }

    // Update the offer status to prevent multiple acceptances
    offerData.status = 'bot_game_accepted';
    activeGames.set(offerId, offerData); // Update the offer in activeGames

    await bot.answerCallbackQuery(callbackQueryId, { text: "Starting your Dice Escalator game against the Bot Dealer..." });
    
    // Call startDiceEscalatorPvBGame_New - this function handles its own bet deduction for the initiator
    if (typeof startDiceEscalatorPvBGame_New === 'function') {
        await startDiceEscalatorPvBGame_New(
            { id: originalChatId, type: originalChatType }, // chat object
            currentInitiatorDetails, // User object with current balance
            offerData.betAmount,
            originalOfferMessageId || offerData.gameSetupMessageId // Message ID of the offer to delete/edit
        );
    } else {
        console.error(`${logPrefix} CRITICAL: startDiceEscalatorPvBGame_New function not found!`);
        // Attempt to refund or mark error, this is a critical failure
        // For now, just log and inform admin if possible
        if(typeof notifyAdmin === 'function') notifyAdmin(`CRITICAL Error in DE_AcceptBot: startDiceEscalatorPvBGame_New not found for offer ${offerId}`);
    }
    // The original unified offer (type DICE_ESCALATOR_UNIFIED_OFFER) should be deleted by startDiceEscalatorPvBGame_New
    // or we can delete it here if startDiceEscalatorPvBGame_New creates a new game ID
    // Based on previous structure, startDiceEscalatorPvBGame_New will create a new gameData of type DICE_ESCALATOR_PVB
    // So, the original unified offer should be cleaned up.
    activeGames.delete(offerId); // Delete the unified offer now that PvB game is starting
    // updateGroupGameDetails will be handled by startDiceEscalatorPvBGame_New for the new PvB game
}

async function handleDiceEscalatorAcceptPvPChallenge_New(offerId, joinerUserObjFromCallback, originalOfferMessageId, originalChatId, originalChatType, callbackQueryId) {
    // Use telegram_id consistently for the clicker/joiner
    const joinerId = String(joinerUserObjFromCallback.telegram_id || joinerUserObjFromCallback.id); 
    const logPrefix = `[DE_AcceptPvP UID:${joinerId} OfferID:"${offerId}" CH:${originalChatId}]`;
    console.log(`${logPrefix} User attempting to accept Dice Escalator PvP challenge from unified offer.`);

    if (!joinerId || joinerId === "undefined") {
        console.error(`${logPrefix} CRITICAL: Joiner ID is undefined. Cannot proceed.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Error: Could not identify you to accept the challenge.", show_alert: true });
        return;
    }

    const offerData = activeGames.get(offerId);

    // Add detailed logging for offerData retrieval
    console.log(`${logPrefix} Trying to get offerId "${offerId}" from activeGames.`);
    if (offerData) {
        console.log(`${logPrefix} Details of found offerData: type="${offerData.type}", status="${offerData.status}", initiatorId="${offerData.initiatorId}"`);
    } else {
        console.log(`${logPrefix} OfferData NOT FOUND in activeGames for offerId "${offerId}"`);
    }
    
    if (!offerData || offerData.type !== GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER || offerData.status !== 'pending_unified_offer') {
        console.warn(`${logPrefix} Offer ${offerId} not found, not a unified DE offer, or not pending. OfferData was: ${offerData ? `Type: ${offerData.type}, Status: ${offerData.status}` : 'Not Found'}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Dice Escalator offer has expired, is not valid, or has already been actioned.", show_alert: true });
        const messageIdToEdit = originalOfferMessageId || offerData?.gameSetupMessageId;
        if (bot && messageIdToEdit) {
            bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(messageIdToEdit) }).catch(() => {});
        }
        return;
    }

    const initiatorIdFromOffer = String(offerData.initiatorId);
    if (joinerId === initiatorIdFromOffer) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "You cannot accept your own Dice Escalator challenge for PvP via this button. Ask another player or play vs Bot.", show_alert: true });
        return;
    }

    // Fetch current details for BOTH players for balance check and bet deduction
    // offerData.initiatorUserObj should contain the initiator's details as of offer creation.
    // We re-fetch to get the latest balance.
    let currentInitiatorUserObj = await getOrCreateUser(initiatorIdFromOffer); 
    // joinerUserObjFromCallback is the user who clicked, get their latest details too.
    let currentJoinerUserObj = await getOrCreateUser(joinerId, joinerUserObjFromCallback.username, joinerUserObjFromCallback.first_name, joinerUserObjFromCallback.last_name); 

    console.log(`${logPrefix} Fetched currentInitiatorUserObj: ${currentInitiatorUserObj ? `ID: ${currentInitiatorUserObj.telegram_id}` : 'null'}`);
    console.log(`${logPrefix} Fetched currentJoinerUserObj: ${currentJoinerUserObj ? `ID: ${currentJoinerUserObj.telegram_id}` : 'null'}`);

    if (!currentInitiatorUserObj || !currentInitiatorUserObj.telegram_id || !currentJoinerUserObj || !currentJoinerUserObj.telegram_id) {
        console.error(`${logPrefix} Failed to fetch full user details for initiator (${initiatorIdFromOffer}) or joiner (${joinerId}).`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Error fetching player details. Cannot start game.", show_alert: true });
        return; 
    }

    const betAmount = BigInt(offerData.betAmount);
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD')); // Use HTML for consistency

    if (BigInt(currentInitiatorUserObj.balance) < betAmount) {
        await bot.answerCallbackQuery(callbackQueryId, { text: `The offer initiator, ${offerData.initiatorMentionHTML}, no longer has sufficient funds for this ${betDisplayHTML} bet. Offer cancelled.`, show_alert: true });
        activeGames.delete(offerId);
        await updateGroupGameDetails(originalChatId, null, null, null);
        const messageIdToEdit = originalOfferMessageId || offerData.gameSetupMessageId;
        if (bot && messageIdToEdit) {
            await bot.editMessageText(`🎲 Offer by ${offerData.initiatorMentionHTML} for <b>${betDisplayHTML}</b> was cancelled. Initiator has insufficient funds.`, {
                chat_id: originalChatId, message_id: Number(messageIdToEdit), parse_mode: 'HTML', reply_markup: {}
            }).catch(() => {});
        }
        return;
    }

    if (BigInt(currentJoinerUserObj.balance) < betAmount) {
        await bot.answerCallbackQuery(callbackQueryId, { text: `Your balance is too low to accept this ${betDisplayHTML} challenge.`, show_alert: true });
        return;
    }

    // All checks passed, proceed to deduct bets and start game
    offerData.status = 'pvp_accepted'; // Mark unified offer as accepted to prevent further interactions
    activeGames.set(offerId, offerData); 

    await bot.answerCallbackQuery(callbackQueryId, { text: `Joining Dice Escalator PvP game against ${offerData.initiatorMentionHTML}... Deducting bets...`});

    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const ledgerNoteInitiator = `Unified DE PvP bet (initiated) vs ${escapeHTML(getPlayerDisplayReference(currentJoinerUserObj))}`;
        const initBetRes = await updateUserBalanceAndLedger(client, currentInitiatorUserObj.telegram_id, BigInt(-betAmount), 'bet_placed_dice_escalator_pvp_init', {game_id_custom_field: offerId, opponent_id_custom_field: currentJoinerUserObj.telegram_id}, ledgerNoteInitiator);
        if (!initBetRes.success) throw new Error(`Failed to debit initiator ${currentInitiatorUserObj.telegram_id}: ${initBetRes.error}`);
        currentInitiatorUserObj.balance = initBetRes.newBalanceLamports; 

        const ledgerNoteJoiner = `Unified DE PvP bet (joined) vs ${escapeHTML(getPlayerDisplayReference(currentInitiatorUserObj))}`;
        const joinBetRes = await updateUserBalanceAndLedger(client, currentJoinerUserObj.telegram_id, BigInt(-betAmount), 'bet_placed_dice_escalator_pvp_join', {game_id_custom_field: offerId, opponent_id_custom_field: currentInitiatorUserObj.telegram_id}, ledgerNoteJoiner);
        if (!joinBetRes.success) throw new Error(`Failed to debit joiner ${currentJoinerUserObj.telegram_id}: ${joinBetRes.error}`);
        currentJoinerUserObj.balance = joinBetRes.newBalanceLamports; 
        
        await client.query('COMMIT');
        console.log(`${logPrefix} Bets deducted successfully for initiator ${currentInitiatorUserObj.telegram_id} and joiner ${currentJoinerUserObj.telegram_id}.`);

        // Bets are now deducted, player objects are updated. Call the PvP starter.
        // startDiceEscalatorPvPGame_New does NOT do bet deductions itself.
        if (typeof startDiceEscalatorPvPGame_New === 'function') {
            await startDiceEscalatorPvPGame_New(
                currentInitiatorUserObj, // Has updated balance
                currentJoinerUserObj,    // Has updated balance
                betAmount, 
                originalChatId, 
                originalChatType, 
                originalOfferMessageId || offerData.gameSetupMessageId // Pass message ID to delete the unified offer message
            );
        } else {
            throw new Error("startDiceEscalatorPvPGame_New function is missing, cannot start PvP game.");
        }
        activeGames.delete(offerId); // Delete the original unified offer as the PvP game has been created

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error: ${rbErr.message}`));
        console.error(`${logPrefix} Error processing PvP accept or starting game: ${error.message}`, error);
        await safeSendMessage(originalChatId, `⚙️ An error occurred setting up the Dice Escalator PvP game: ${escapeHTML(error.message)}. The offer may have been cancelled.`, { parse_mode: 'HTML' });
        
        // Revert offer status if it was set to pvp_accepted but game failed to start due to error after bet deduction
        const offerToRevert = activeGames.get(offerId);
        if (offerToRevert && offerToRevert.status === 'pvp_accepted') {
            offerToRevert.status = 'pending_unified_offer'; 
            activeGames.set(offerId, offerToRevert);
            console.log(`${logPrefix} Reverted offer ${offerId} status to 'pending_unified_offer' due to game start error.`);
        } else if (!offerToRevert && error.message.includes("Failed to debit")) { 
             // If bet deduction failed and offer might have been deleted by other means, ensure group details are clear
             await updateGroupGameDetails(originalChatId, null, null, null);
        }
    } finally {
        if (client) client.release();
    }
}

async function handleDiceEscalatorCancelUnifiedOffer_New(offerId, userWhoClicked, originalOfferMessageId, originalChatId, callbackQueryId) {
    const logPrefix = `[DE_CancelUnified UID:${userWhoClicked.telegram_id} OfferID:"${offerId}" CH:${originalChatId}]`;
    console.log(`${logPrefix} User attempting to cancel Dice Escalator unified offer.`);
    
    const offerData = activeGames.get(offerId);
    
    console.log(`${logPrefix} Trying to get offerId "${offerId}" from activeGames. activeGames keys (sample): ${JSON.stringify(Array.from(activeGames.keys()).slice(0, 5))}... (Total: ${activeGames.size})`); // Debug
    if (offerData) {
        console.log(`${logPrefix} Details of found offerData: type="${offerData.type}", status="${offerData.status}", gameId="${offerData.gameId}", initiatorId="${offerData.initiatorId}"`);
        console.log(`${logPrefix} Comparing offerData.type ("${offerData.type}") with GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER ("${GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER}")`);
        console.log(`${logPrefix} Comparing offerData.status ("${offerData.status}") with 'pending_unified_offer'`);
    } else {
        console.log(`${logPrefix} OfferData NOT FOUND in activeGames for offerId "${offerId}"`);
    }

    if (!offerData || offerData.type !== GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER || offerData.status !== 'pending_unified_offer') {
        console.warn(`${logPrefix} Dice Escalator unified offer ${offerId} not found, not a unified offer, or not pending. Current OfferData in map if found: ${offerData ? `Type: ${offerData.type}, Status: ${offerData.status}` : 'Not Found'}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Dice Escalator offer has expired, is not valid, or already actioned.", show_alert: true });
        // Use originalOfferMessageId (the parameter) if it's valid, otherwise use offerData.gameSetupMessageId if available
        const messageIdToEdit = originalOfferMessageId || offerData?.gameSetupMessageId;
        if (bot && messageIdToEdit) { 
            bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(messageIdToEdit) }).catch(() => {});
        }
        return;
    }

    if (String(userWhoClicked.telegram_id) !== String(offerData.initiatorId)) {
        console.log(`${logPrefix} Clicker ${userWhoClicked.telegram_id} is not initiator ${offerData.initiatorId}.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the player who made the offer can cancel it.", show_alert: true });
        return;
    }

    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null); 
    console.log(`${logPrefix} Offer ${offerId} cancelled by initiator and removed from activeGames.`);

    await bot.answerCallbackQuery(callbackQueryId, { text: "Your Dice Escalator offer has been successfully cancelled." });

    const initiatorMentionHTML = offerData.initiatorMentionHTML || escapeHTML(getPlayerDisplayReference(offerData.initiatorUserObj));
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount, 'USD'));
    const cancelledMessage = `🚫 ${initiatorMentionHTML} has cancelled their Dice Escalator offer for <b>${betDisplayUSD_HTML}</b>.`;

    // Use originalOfferMessageId (the parameter which is the ID of the message with the buttons)
    // or fallback to offerData.gameSetupMessageId if for some reason originalOfferMessageId was not correctly passed/available.
    const messageIdToEdit = originalOfferMessageId || offerData.gameSetupMessageId;

    if (bot && messageIdToEdit) {
        await bot.editMessageText(cancelledMessage, {
            chat_id: originalChatId,
            message_id: Number(messageIdToEdit),
            parse_mode: 'HTML',
            reply_markup: {} 
        }).catch(e => {
            console.warn(`${logPrefix} Failed to edit original offer message ${messageIdToEdit}: ${e.message}. Sending new message.`);
            safeSendMessage(originalChatId, cancelledMessage, { parse_mode: 'HTML' });
        });
    } else {
        console.warn(`${logPrefix} No messageId found to edit for cancelled offer ${offerId}. Sending new message.`);
        safeSendMessage(originalChatId, cancelledMessage, { parse_mode: 'HTML' });
    }
}

// --- Dice Escalator Player vs. Bot (PvB) Game Logic ---
// THIS IS THE VERSION WITH THE jackpotContribution FIX
async function startDiceEscalatorPvBGame_New(chat, initiatorUserObj, betAmountLamports, originalOfferMessageIdToDelete = null, isPlayAgain = false) {
    const chatId = String(chat.id);
    const logPrefix = `[DE_PvB_Start_HTML_V3_JackpotFix UID:${initiatorUserObj.telegram_id} CH:${chatId}]`;
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));

    if (originalOfferMessageIdToDelete && bot) {
        await bot.deleteMessage(chatId, Number(originalOfferMessageIdToDelete)).catch(e => {});
    }

    let client = null;
    const gameId = `de_pvb_${chatId}_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
    let jackpotContribution = 0n; // Correctly declared and initialized

    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const currentPlayerState = await getPaymentSystemUserDetails(initiatorUserObj.telegram_id, client);
        if(!currentPlayerState || BigInt(currentPlayerState.balance) < betAmountLamports) {
            await client.query('ROLLBACK'); 
            const neededDisplay = escapeHTML(await formatBalanceForDisplay(betAmountLamports - BigInt(currentPlayerState?.balance || 0), 'USD'));
            await safeSendMessage(chatId, `💰 <b>Balance Too Low, ${playerRefHTML}</b>!<br>To start Dice Escalator for <b>${escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'))}</b>, your funds are short. You need ~<b>${neededDisplay}</b> more.`, { parse_mode: 'HTML' });
            return;
        }

        const betTxNotes = `Dice Escalator PvB bet. Game ID: ${gameId}. Player: ${initiatorUserObj.telegram_id}.`;
        const betResult = await updateUserBalanceAndLedger(client, initiatorUserObj.telegram_id, BigInt(-betAmountLamports), 'game_bet_de_pvb', { game_id_custom_field: gameId }, betTxNotes);
        if (!betResult.success) {
            await client.query('ROLLBACK');
            throw new Error(`Bet placement failed for DE PvB: ${betResult.error}`);
        }
        
        jackpotContribution = BigInt(Math.floor(Number(betAmountLamports) * JACKPOT_CONTRIBUTION_PERCENT)); // Calculated here
        if (jackpotContribution > 0n) {
            await client.query('UPDATE jackpots SET current_amount = current_amount + $1, updated_at = NOW() WHERE jackpot_id = $2', [jackpotContribution.toString(), MAIN_JACKPOT_ID]);
        }
        await client.query('COMMIT');
        initiatorUserObj.balance = betResult.newBalanceLamports; 
    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} Database error starting PvB: ${error.message}`);
        await safeSendMessage(chatId, "⚙️ <b>Database Hiccup!</b> Error starting game. Wager not processed. Try again.", { parse_mode: 'HTML' });
        activeGames.delete(gameId); 
        await updateGroupGameDetails(chatId, null, null, null); 
        return;
    } finally {
        if (client) client.release();
    }
    
    const gameData = {
        gameId: gameId, type: GAME_IDS.DICE_ESCALATOR_PVB,
        player: {
            userId: initiatorUserObj.telegram_id, username: initiatorUserObj.username, firstName: initiatorUserObj.first_name,
            displayName: getPlayerDisplayReference(initiatorUserObj), 
            score: 0, rolls: [], isGoingForJackpot: false, busted: false
        },
        betAmount: betAmountLamports, chatId: chatId, chatType: chat.type,
        status: 'player_turn_awaiting_emoji',
        createdAt: Date.now(), 
        gameMessageId: null, 
        jackpotContribution: jackpotContribution, // Correctly used here
        lastPlayerRoll: null, botRolls: [], botScore: 0
    };
    activeGames.set(gameId, gameData);
    if (chat.type !== 'private') {
        await updateGroupGameDetails(chatId, gameId, GAME_IDS.DICE_ESCALATOR_PVB, betAmountLamports);
    }
    await updateDiceEscalatorPvBMessage_New(gameData); 
}

async function handleDEGoForJackpot(gameId, userWhoClicked, originalMessageId, callbackQueryId, chatData) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DICE_ESCALATOR_PVB || gameData.status !== 'player_score_18_plus_awaiting_choice' || gameData.player.userId !== userWhoClicked.telegram_id) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Action not available.", show_alert: true }).catch(()=>{});
        return;
    }
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "Going for GOLD! Jackpot run active!"}).catch(()=>{});
    gameData.player.isGoingForJackpot = true;
    gameData.status = 'player_turn_awaiting_emoji';
    activeGames.set(gameData.gameId, gameData);
    await updateDiceEscalatorPvBMessage_New(gameData); // This will delete the old message and send new state
}

async function processDiceEscalatorPvBRollByEmoji_New(gameData, diceValue) {
    const logPrefix = `[DE_PvB_Roll_HTML_V3_Fix UID:${gameData.player.userId} Game:${gameData.gameId}]`; // Added Fix to log prefix
    // Basic validation (already done in main message handler usually)
    if (gameData.status !== 'player_turn_awaiting_emoji' && gameData.status !== 'player_score_18_plus_awaiting_choice') {
        console.warn(`${logPrefix} Roll received but game status is '${gameData.status}'. Ignoring.`);
        return;
    }
    if (gameData.status === 'player_score_18_plus_awaiting_choice' && !gameData.player.isGoingForJackpot) {
        // If they had the 18+ choice and just sent a dice, it means they chose to roll again without pressing "Go for Jackpot"
    }
    gameData.status = 'player_turn_awaiting_emoji'; // Ensure correct status for processing

    const player = gameData.player;
    const playerRefHTML = escapeHTML(player.displayName);

    // Send temporary roll announcement (HTML)
    const rollAnnounceTextHTML = `🎲 ${playerRefHTML} rolled a <b>${escapeHTML(String(diceValue))}</b>!`;
    const tempRollMsg = await safeSendMessage(gameData.chatId, rollAnnounceTextHTML, { parse_mode: 'HTML' }); // Corrected variable name here
    
    await sleep(BUST_MESSAGE_DELAY_MS > 1000 ? 1200 : BUST_MESSAGE_DELAY_MS / 1.5); 
    // CORRECTED TYPO: tempMsg to tempRollMsg
    if (tempRollMsg?.message_id && bot) { 
        await bot.deleteMessage(gameData.chatId, tempRollMsg.message_id).catch(()=>{});
    }

    player.rolls.push(diceValue);
    gameData.lastPlayerRoll = diceValue;

    if (diceValue === DICE_ESCALATOR_BUST_ON) {
        player.busted = true;
        gameData.status = 'player_busted';
        activeGames.set(gameData.gameId, gameData);
        await updateDiceEscalatorPvBMessage_New(gameData); // Show bust state
        await sleep(BUST_MESSAGE_DELAY_MS); // Pause for bust message
        await finalizeDiceEscalatorPvBGame_New(gameData, 0); // Bot wins by default
        return;
    }

    player.score += diceValue;

    if (player.score >= TARGET_JACKPOT_SCORE) {
        player.stood = true; 
        gameData.status = 'player_stood'; // Will trigger bot turn next
        activeGames.set(gameData.gameId, gameData);
        await updateDiceEscalatorPvBMessage_New(gameData);
        await sleep(1000);
        await processDiceEscalatorBotTurnPvB_New(gameData);
        return;
    }

    if (player.score >= 18 && !player.isGoingForJackpot && gameData.status !== 'player_score_18_plus_awaiting_choice') {
        gameData.status = 'player_score_18_plus_awaiting_choice';
    } else {
        gameData.status = 'player_turn_awaiting_emoji'; // Continue player's turn
    }
    activeGames.set(gameData.gameId, gameData);
    await updateDiceEscalatorPvBMessage_New(gameData); // Update with new score/status
}

async function updateDiceEscalatorPvBMessage_New(gameData, isStanding = false) {
    if (!gameData || !bot) return;
    if (gameData.gameMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.gameMessageId)).catch(e => {});
        gameData.gameMessageId = null;
    }

    if (isStanding) gameData.status = 'player_stood';
    activeGames.set(gameData.gameId, gameData);

    const messageTextHTML = await formatDiceEscalatorGameMessage_New(gameData);
    let keyboard = { inline_keyboard: [] };

    if (gameData.status === 'player_score_18_plus_awaiting_choice') {
        keyboard.inline_keyboard.push(
            [{ text: "✋ Stand Firm!", callback_data: `de_stand_pvb:${gameData.gameId}` }],
            [{ text: "🔥 Go for Jackpot!", callback_data: `de_pvb_go_for_jackpot:${gameData.gameId}` }]
        );
    } else if (gameData.status === 'player_turn_awaiting_emoji' && !isStanding && !gameData.player.busted && !gameData.player.isGoingForJackpot) {
        keyboard.inline_keyboard.push([{ text: "✋ Stand Firm!", callback_data: `de_stand_pvb:${gameData.gameId}` }]);
    }
    
    if (keyboard.inline_keyboard.length > 0 || 
        gameData.status === 'player_stood' || 
        gameData.status === 'bot_turn_complete' || 
        gameData.status.startsWith('game_over_')) {
        if (keyboard.inline_keyboard.length > 0 && !keyboard.inline_keyboard[keyboard.inline_keyboard.length-1].find(b => b.text === "📖 Game Rules")) {
            keyboard.inline_keyboard.push([{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER}` }]);
        } else if (keyboard.inline_keyboard.length === 0) {
             keyboard.inline_keyboard.push([{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER}` }]);
        }
    }

    const sentMessage = await safeSendMessage(gameData.chatId, messageTextHTML, {
        parse_mode: 'HTML',
        reply_markup: (keyboard.inline_keyboard && keyboard.inline_keyboard.length > 0) ? keyboard : {}
    });

    if (sentMessage?.message_id) {
        gameData.gameMessageId = String(sentMessage.message_id);
    } else {
        console.error(`[UpdateDE_PvB_HTML_V3] CRITICAL: Failed to send/update PvB game message for ${gameData.gameId}.`);
    }
    if(activeGames.has(gameData.gameId)) activeGames.set(gameData.gameId, gameData);
}

async function handleDiceEscalatorPvBStand_New(gameId, userWhoClicked, originalMessageId, callbackQueryId, chatData) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DICE_ESCALATOR_PVB || gameData.player.userId !== userWhoClicked.telegram_id || gameData.player.isGoingForJackpot || (gameData.status !== 'player_turn_awaiting_emoji' && gameData.status !== 'player_score_18_plus_awaiting_choice') ) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Cannot stand now.", show_alert: true }).catch(()=>{});
        return;
    }
    
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: `You stand with ${gameData.player.score} points! Bot's turn...`}).catch(()=>{});
    
    gameData.player.stood = true;
    await updateDiceEscalatorPvBMessage_New(gameData, true); // isStanding = true
    await sleep(1000);
    await processDiceEscalatorBotTurnPvB_New(gameData);
}

async function processDiceEscalatorBotTurnPvB_New(gameData) {
    if (!gameData || gameData.status !== 'player_stood') { return; }
    
    gameData.botRolls = await getThreeDiceRollsViaHelper_DE_New(gameData.gameId, gameData.chatId);
    gameData.botScore = gameData.botRolls.reduce((sum, roll) => sum + roll, 0);
    gameData.status = 'bot_turn_complete';
    activeGames.set(gameData.gameId, gameData);

    await updateDiceEscalatorPvBMessage_New(gameData); 
    await sleep(1500);
    await finalizeDiceEscalatorPvBGame_New(gameData, gameData.botScore);
}

async function finalizeDiceEscalatorPvBGame_New(gameData, botScoreArgument) {
    const { gameId, chatId, player, betAmount } = gameData;
    activeGames.delete(gameId); // Clean up active game state
    await updateGroupGameDetails(chatId, null, null, null); // Clear group state

    let resultTextOutcomeHTML = ""; // This will be the main description of what happened
    let titleEmoji = "🏁";
    let payoutLamports = 0n;
    let ledgerOutcomeCode = 'loss_de_pvb'; // Default
    let jackpotWon = false;
    let jackpotAmountClaimed = 0n;
    const playerRefHTML = escapeHTML(player.displayName); // Player name, HTML escaped
    const wagerDisplayHTML = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD')); // Wager, HTML escaped string
    let finalTitle = `Dice Escalator - Result!`; // Base title
    const botFinalScore = gameData.botScore || botScoreArgument || 0;

    // Construct the outcome description using HTML
    if (player.busted) {
        titleEmoji = "💥"; 
        finalTitle = `BUSTED, ${playerRefHTML}!`;
        const lastRollDisplay = escapeHTML(String(gameData.lastPlayerRoll));
        const bustOnDisplay = escapeHTML(String(DICE_ESCALATOR_BUST_ON));
        resultTextOutcomeHTML = `Your roll of <b>${lastRollDisplay}</b> (bust on <b>${bustOnDisplay}</b>) ended your climb.\nThe Bot Dealer wins <b>${wagerDisplayHTML}</b>.`;
        ledgerOutcomeCode = 'loss_de_pvb_bust';
    } else if (player.score > botFinalScore) {
        titleEmoji = "🎉"; 
        finalTitle = `VICTORY, ${playerRefHTML}!`;
        payoutLamports = betAmount * 2n;
        ledgerOutcomeCode = 'win_de_pvb';
        const potWonHTML = escapeHTML(await formatBalanceForDisplay(payoutLamports, 'USD'));
        resultTextOutcomeHTML = `Your score of <b>${player.score}</b> conquers the Bot Dealer's <i>${botFinalScore}</i>!\nYou win the pot of <b>${potWonHTML}</b>!`;
        
        if (player.score >= TARGET_JACKPOT_SCORE) {
            // Jackpot Logic (ensure messages here are also HTML)
            const client = await pool.connect();
            try {
                await client.query('BEGIN');
                const jackpotRes = await client.query('SELECT current_amount FROM jackpots WHERE jackpot_id = $1 FOR UPDATE', [MAIN_JACKPOT_ID]);
                if (jackpotRes.rows.length > 0 && BigInt(jackpotRes.rows[0].current_amount || '0') > 0n) {
                    jackpotAmountClaimed = BigInt(jackpotRes.rows[0].current_amount);
                    await client.query('UPDATE jackpots SET current_amount = 0, last_won_by_telegram_id = $1, last_won_timestamp = NOW(), updated_at = NOW() WHERE jackpot_id = $2', [player.userId, MAIN_JACKPOT_ID]);
                    payoutLamports += jackpotAmountClaimed;
                    jackpotWon = true; titleEmoji = "🎆";
                    finalTitle = `SUPER JACKPOT WIN, ${playerRefHTML}!!`;
                    const jackpotAmountUSD_HTML = escapeHTML(await formatBalanceForDisplay(jackpotAmountClaimed, 'USD'));
                    const totalPayoutUSD_HTML = escapeHTML(await formatBalanceForDisplay(payoutLamports, 'USD'));
                    
                    resultTextOutcomeHTML = `<pre>🎇✨✨✨✨✨✨✨✨✨✨✨✨🎇\n`+
                                     `  💎💎   MEGA JACKPOT HIT!  💎💎\n`+
                                     `🎇✨✨✨✨✨✨✨✨✨✨✨✨🎇</pre>\n` +
                                   `<b>INCREDIBLE, ${playerRefHTML}!</b>\nYour score of <b>${player.score}</b> beat the Bot's <i>${botFinalScore}</i>, AND you've smashed the Super Jackpot, claiming an additional astounding:\n\n`+
                                   `💰💰💰🔥 <b>${jackpotAmountUSD_HTML}</b> 🔥💰💰💰\n\n` +
                                   `Total Payout: An unbelievable <b>${totalPayoutUSD_HTML}</b>!\n` +
                                   `Truly a legendary performance! 🥳🎉`;
                    ledgerOutcomeCode = 'win_de_pvb_jackpot';
                } else {
                     resultTextOutcomeHTML += `\n\n<i>(The Super Jackpot was already claimed or empty for this win.)</i>`;
                }
                await client.query('COMMIT');
            } catch (e) { 
                if (client) await client.query('ROLLBACK');
                console.error(`[FinalizeDE_PvB_HTML_V11] Error processing jackpot: ${e.message}`);
                resultTextOutcomeHTML += `\n\n⚠️ <i>A small issue occurred with jackpot confirmation. Your base winnings are secure.</i>`;
            } 
            finally { if (client) client.release(); }
        }
    } else if (player.score === botFinalScore) {
        titleEmoji = "⚖️"; 
        finalTitle = `A Close Call - It's a Push!`;
        payoutLamports = betAmount; 
        ledgerOutcomeCode = 'push_de_pvb';
        resultTextOutcomeHTML = `You and the Bot Dealer both scored <b>${player.score}</b>.\nYour wager of <b>${wagerDisplayHTML}</b> is returned.`;
    } else { // Bot wins
        titleEmoji = "🤖"; 
        finalTitle = `The Bot Dealer Wins This Round!`;
        resultTextOutcomeHTML = `The Bot Dealer's score of <b>${botFinalScore}</b> narrowly beat your <i>${player.score}</i>. Better luck next time!`;
        ledgerOutcomeCode = 'loss_de_pvb_score'; // payoutLamports remains 0n
    }
    
    let clientPayout = null;
    let dbErrorText = ""; // To store potential DB error messages for display
    try {
        clientPayout = await pool.connect(); await clientPayout.query('BEGIN');
        const notes = `DE PvB Result. Player: ${player.score}, Bot: ${botFinalScore}. Jackpot: ${jackpotAmountClaimed > 0n ? formatCurrency(jackpotAmountClaimed) : '0'}. GameID: ${gameId}`;
        const balanceUpdate = await updateUserBalanceAndLedger(clientPayout, player.userId, payoutLamports, ledgerOutcomeCode, { game_id_custom_field: gameId, jackpot_amount_custom_field: jackpotAmountClaimed.toString() }, notes);
        if (!balanceUpdate.success) {
            await clientPayout.query('ROLLBACK'); 
            throw new Error(balanceUpdate.error || "DB Error during DE PvB payout/ledger update.");
        }
        await clientPayout.query('COMMIT');
    } catch (e) {
        if (clientPayout) await clientPayout.query('ROLLBACK').catch(()=>{});
        console.error(`[FinalizeDE_PvB_HTML_V11] CRITICAL DB error during payout: ${e.message}`);
        dbErrorText = `\n\n⚠️ <i>Critical error settling wager: ${escapeHTML(e.message)}. Admin has been notified.</i>`;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL DE PvB Payout Failure 🚨\nGame ID: <code>${gameId}</code> User: ${playerRefHTML}\nAmount due: ${payoutLamports}\nDB Error: ${escapeHTML(e.message)}. MANUAL CHECK REQUIRED.`, {parse_mode: 'HTML'});
    } finally {
        if (clientPayout) clientPayout.release();
    }

    // Assuming formatDiceRolls returns HTML-safe text (e.g., "🎲 6  🎲 2" or "🎲 <code>6</code> 🎲 <code>2</code>")
    const playerRollsDisplay = formatDiceRolls(player.rolls); 
    const botRollsDisplay = formatDiceRolls(gameData.botRolls);

    // Construct the final message string using HTML entities where needed, and \n for line breaks.
    // escapeHTML is used for all dynamic content that will be part of the text.
    let fullResultMessageHTML = 
        `${titleEmoji} <b>${escapeHTML(finalTitle)}</b> ${titleEmoji}\n\n` +
        `Player: ${playerRefHTML}\n` + // playerRefHTML is already escaped
        `Wager: <b>${wagerDisplayHTML}</b>\n\n` + // wagerDisplayHTML is already escaped
        `Your Rolls: ${playerRollsDisplay} ➠ Score: <b>${player.score}</b> ${player.busted ? "💥 BUSTED!" : ""}\n` +
        `Bot's Rolls: ${botRollsDisplay} ➠ Score: <b>${botFinalScore}</b>\n\n` +
        `------------------------------------\n` +
        `${resultTextOutcomeHTML}` + // This part already contains HTML and escaped dynamic content
        `${dbErrorText}\n\n` + // dbErrorText will be empty if no error, or contain an escaped error message
        `<i>Thanks for playing Dice Escalator!</i>`;
    
    // Delete the last game state message before sending the final result
    if (gameData.gameMessageId && bot) { 
        await bot.deleteMessage(chatId, Number(gameData.gameMessageId)).catch(() => {});
    }

    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR_PVB, betAmount);
    await safeSendMessage(chatId, fullResultMessageHTML, { 
        parse_mode: 'HTML', // Crucial: Set parse_mode to HTML
        reply_markup: finalKeyboard 
    });
}


// --- Dice Escalator Player vs. Player (PvP) Game Logic (HTML Revamp) ---
async function startDiceEscalatorPvPGame_New(
    initiatorUserObj, // User object for the initiator (balance assumed to be already updated by caller)
    opponentUserObj,  // User object for the opponent (balance assumed to be already updated by caller)
    betAmountLamports,
    groupChatId,      // String: ID of the group chat where the game will be played
    groupChatType,    // String: Type of the group chat (e.g., 'group', 'supergroup')
    messageIdToDeleteAfterAccept = null // Optional: ID of a previous message (e.g., "Challenge Accepted!") to delete
) {
    const logPrefix = `[DE_PvP_Start_V2 UID1:${initiatorUserObj.telegram_id} UID2:${opponentUserObj.telegram_id} CH:${groupChatId}]`;
    console.log(`${logPrefix} Starting new DE PvP game. Bet: ${betAmountLamports}. ChatType: ${groupChatType}. Message to delete (optional): ${messageIdToDeleteAfterAccept}`);

    // 1. Optional: Delete the "Challenge Accepted!" message if its ID was passed,
    //    as the game will now post its own initial board message.
    if (messageIdToDeleteAfterAccept && bot) {
        await bot.deleteMessage(groupChatId, Number(messageIdToDeleteAfterAccept))
            .catch(e => console.warn(`${logPrefix} Non-critical: Could not delete previous message ${messageIdToDeleteAfterAccept}: ${e.message}`));
    }

    // 2. Generate new PvP Game ID for this specific game instance
    const pvpGameId = generateGameId(GAME_IDS.DICE_ESCALATOR_PVP);
    console.log(`${logPrefix} Generated new PvP Game ID: ${pvpGameId}`);
    
    // 3. Prepare player data for the game state using the passed-in user objects
    //    getPlayerDisplayReference should return plain text or text that escapeHTML can handle if not already HTML safe.
    //    For consistency, ensure the displayName stored is what updateDiceEscalatorPvPMessage_New expects (likely HTML-escaped).
    const initiatorPlayerDisplayName = escapeHTML(getPlayerDisplayReference(initiatorUserObj));
    const opponentPlayerDisplayName = escapeHTML(getPlayerDisplayReference(opponentUserObj));
    
    const initiatorPlayerData = { 
        userId: String(initiatorUserObj.telegram_id), 
        displayName: initiatorPlayerDisplayName,
        userObj: initiatorUserObj, // Store the full object if other functions need more than just ID/display name
        score: 0, rolls: [], 
        isTurn: true, // Typically, initiator (Player 1) starts
        busted: false, stood: false, 
        status: 'awaiting_roll_emoji' // Player needs to send dice emoji
    };
    const opponentPlayerData = { 
        userId: String(opponentUserObj.telegram_id), 
        displayName: opponentPlayerDisplayName,
        userObj: opponentUserObj,
        score: 0, rolls: [], 
        isTurn: false, 
        busted: false, stood: false, 
        status: 'waiting_turn' 
    };
    
    const gameData = {
        gameId: pvpGameId, 
        type: GAME_IDS.DICE_ESCALATOR_PVP, 
        chatId: String(groupChatId), 
        chatType: groupChatType,
        // The updateDiceEscalatorPvPMessage_New function expects gameData.initiator and gameData.opponent
        initiator: initiatorPlayerData, // Player 1
        opponent: opponentPlayerData,   // Player 2
        betAmount: betAmountLamports, 
        status: 'p1_awaiting_roll_emoji', // Game status reflects initiator's (P1) turn
        currentMessageId: null, // For the new game board message that will be sent
        createdAt: Date.now(), 
        lastRollValue: null, // To store the value of the most recent dice roll
    };
    activeGames.set(pvpGameId, gameData);
    console.log(`${logPrefix} New DE PvP game object (${pvpGameId}) created and stored in activeGames. Initial Game Status: '${gameData.status}', P1 turn: ${gameData.initiator.isTurn}`);

    // Update the group game session to reflect this new active PvP game
    await updateGroupGameDetails(groupChatId, pvpGameId, GAME_IDS.DICE_ESCALATOR_PVP, betAmountLamports);
    console.log(`${logPrefix} Group game details updated for chat ${groupChatId} to DE PvP game ${pvpGameId}.`);
    
    // 4. Call the function to send/update the initial game board message
    //    updateDiceEscalatorPvPMessage_New is designed to handle sending/editing the game UI
    if (typeof updateDiceEscalatorPvPMessage_New === 'function') {
        await updateDiceEscalatorPvPMessage_New(gameData); 
        console.log(`${logPrefix} Initial DE PvP game message/board potentially sent/updated for GID: ${pvpGameId}.`);
    } else {
        console.error(`${LOG_PREFIX_DE_OFFER_V5_SHORTCB} CRITICAL ERROR: updateDiceEscalatorPvPMessage_New function is not defined! Cannot display game board for ${pvpGameId}.`);
        await safeSendMessage(groupChatId, "⚙️ Critical error: Could not display the game board. Please contact support.", {parse_mode: 'HTML'});
        // If game board can't be displayed, this game instance is problematic.
        // Depending on desired behavior, might want to clean up activeGames and groupGameSessions here
        // or attempt to refund if bets were confirmed to be taken by a higher level function that proves problematic.
        // For now, assuming if this function is called, bets WERE successfully handled by the caller.
    }
    console.log(`${logPrefix} Dice Escalator PvP game ${pvpGameId} setup process complete.`);
}

async function processDiceEscalatorPvPRollByEmoji_New(gameData, diceValue, userIdWhoRolled) {
    const logPrefix = `[DE_PvP_Roll_HTML_V4_StateFix UID:${userIdWhoRolled} Game:${gameData.gameId}]`;
    let currentPlayer, otherPlayer;

    if (gameData.initiator.userId === userIdWhoRolled && gameData.initiator.isTurn) {
        currentPlayer = gameData.initiator; otherPlayer = gameData.opponent;
    } else if (gameData.opponent.userId === userIdWhoRolled && gameData.opponent.isTurn) {
        currentPlayer = gameData.opponent; otherPlayer = gameData.initiator;
    } else { console.warn(`${logPrefix} Roll from non-active player or out of turn.`); return; }
    
    if(currentPlayer.status !== 'awaiting_roll_emoji'){ 
        console.warn(`${logPrefix} Player ${currentPlayer.displayName} status is '${currentPlayer.status}'. Not expecting a roll now.`); 
        return; 
    }

    const playerRefHTML = escapeHTML(currentPlayer.displayName);
    const tempRollMsg = await safeSendMessage(gameData.chatId, `🎲 ${playerRefHTML} rolled a <b>${escapeHTML(String(diceValue))}</b>! Calculating score...`, { parse_mode: 'HTML' });
    await sleep(1500);
    // *** CORRECTED LINE (Original fix for tempMsg typo) ***
    if (tempRollMsg?.message_id && bot) await bot.deleteMessage(gameData.chatId, tempRollMsg.message_id).catch(()=>{});
    // *** END OF CORRECTION ***

    currentPlayer.rolls.push(diceValue);
    gameData.lastRollValue = diceValue;

    // *** MODIFIED BUST LOGIC FOR PVP ***
    if (diceValue === DICE_ESCALATOR_BUST_ON) {
        currentPlayer.busted = true; 
        // currentPlayer.score might or might not be reset; game ends anyway. Current logic doesn't reset on bust for display.
        currentPlayer.status = 'bust'; // Mark player as busted
        currentPlayer.isTurn = false;   // Their turn is over

        const bustedPlayerName = (currentPlayer === gameData.initiator) ? "Player 1 (Initiator)" : "Player 2 (Opponent)";
        console.log(`${logPrefix} ${bustedPlayerName} BUSTED with a ${diceValue}. Game over.`);

        // Game ends immediately upon any player busting in PvP
        gameData.status = 'game_over_pvp_resolved'; 
    } else { // Not a bust
        currentPlayer.score += diceValue;
        // Player remains in 'awaiting_roll_emoji' and isTurn = true, so they can roll again or stand.
        // Main game status reflects this player's turn
        gameData.status = (currentPlayer === gameData.initiator) ? 'p1_awaiting_roll_emoji' : 'p2_awaiting_roll_emoji';
    }
    // *** END OF MODIFIED BUST LOGIC FOR PVP ***
    
    activeGames.set(gameData.gameId, gameData);

    if (gameData.status.startsWith('game_over')) {
        await updateDiceEscalatorPvPMessage_New(gameData); 
        await sleep(1000); // Allow time for the "bust" message to be seen
        await resolveDiceEscalatorPvPGame_New(gameData, currentPlayer.busted ? currentPlayer.userId : null);
    } else {
        await updateDiceEscalatorPvPMessage_New(gameData); 
    }
}

async function updateDiceEscalatorPvPMessage_New(gameData) {
    if (!gameData || !bot) return;
    // Always delete old and send new
    if (gameData.currentMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.currentMessageId)).catch(e => {});
        gameData.currentMessageId = null;
    }

    const messageTextHTML = await formatDiceEscalatorGameMessage_New(gameData);
    let keyboard = { inline_keyboard: [] };
    const activePlayer = gameData.initiator.isTurn ? gameData.initiator : (gameData.opponent.isTurn ? gameData.opponent : null);
    
    if (activePlayer && !activePlayer.stood && !activePlayer.busted && activePlayer.status === 'awaiting_roll_emoji') {
        keyboard.inline_keyboard.push([{ text: `✋ ${escapeHTML(activePlayer.displayName)}, Stand!`, callback_data: `de_stand_pvp:${gameData.gameId}` }]);
    }
    keyboard.inline_keyboard.push([{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER}` }]);
    
    const sentMessage = await safeSendMessage(gameData.chatId, messageTextHTML, { parse_mode: 'HTML', reply_markup: keyboard });
    if (sentMessage?.message_id) gameData.currentMessageId = String(sentMessage.message_id);
    if(activeGames.has(gameData.gameId)) activeGames.set(gameData.gameId, gameData);
}

async function handleDiceEscalatorPvPStand_New(gameId, userWhoClicked, originalMessageId, callbackQueryId, chatData) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DICE_ESCALATOR_PVP) { /* ... */ return; }

    let playerStanding, otherPlayer;
    if (gameData.initiator.userId === userWhoClicked.telegram_id && gameData.initiator.isTurn) {
        playerStanding = gameData.initiator; otherPlayer = gameData.opponent;
    } else if (gameData.opponent.userId === userWhoClicked.telegram_id && gameData.opponent.isTurn) {
        playerStanding = gameData.opponent; otherPlayer = gameData.initiator;
    } else { if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text:"Not your turn!", show_alert:true}); return; }

    if (playerStanding.stood || playerStanding.busted || playerStanding.status !== 'awaiting_roll_emoji') { if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text:"You've already acted or cannot stand now.", show_alert:true}); return; }
    
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: `You stand with ${playerStanding.score} points!`}).catch(()=>{});

    playerStanding.stood = true;
    playerStanding.isTurn = false;
    playerStanding.status = 'stood'; 
    
    const playerStandingMentionHTML = escapeHTML(playerStanding.displayName);
    const tempStandMsg = await safeSendMessage(gameData.chatId, `✋ <b>${playerStandingMentionHTML}</b> stands with a score of <b>${escapeHTML(String(playerStanding.score))}</b>!`, {parse_mode:'HTML'});
    await sleep(1500);
    if (tempStandMsg?.message_id && bot) await bot.deleteMessage(gameData.chatId, tempStandMsg.message_id).catch(()=>{});

    if (playerStanding === gameData.initiator) { // P1 stood
        if (otherPlayer.busted || otherPlayer.stood) { 
            gameData.status = 'game_over_pvp_resolved';
        } else { 
            otherPlayer.isTurn = true;
            otherPlayer.status = 'awaiting_roll_emoji';
            gameData.status = 'p2_awaiting_roll_emoji'; 
        }
    } else { // P2 stood, P1 must have already stood or busted
        gameData.status = 'game_over_pvp_resolved';
    }
    activeGames.set(gameData.gameId, gameData);

    if (gameData.status.startsWith('game_over')) {
        await updateDiceEscalatorPvPMessage_New(gameData); 
        await sleep(1000);
        await resolveDiceEscalatorPvPGame_New(gameData);
    } else {
        await updateDiceEscalatorPvPMessage_New(gameData); 
    }
}

async function resolveDiceEscalatorPvPGame_New(gameData, playerWhoBustedId = null) {
    activeGames.delete(gameData.gameId);
    await updateGroupGameDetails(gameData.chatId, null, null, null);
    const p1 = gameData.initiator; const p2 = gameData.opponent;
    const p1MentionHTML = escapeHTML(p1.displayName); const p2MentionHTML = escapeHTML(p2.displayName);
    const betAmount = BigInt(gameData.betAmount);
    let winner = null, isPush = false, titleEmoji = "⚔️", resultHeaderHTML = "", resultDetailsHTML = "", winningsFooterHTML = "";
    const totalPotLamports = betAmount * 2n;
    let p1Payout = 0n; let p2Payout = 0n;
    let p1LedgerCode = 'loss_de_pvp'; let p2LedgerCode = 'loss_de_pvp';
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'));

    if (p1.busted || playerWhoBustedId === p1.userId) {
        titleEmoji = "💥"; winner = p2; p1.busted = true; // Ensure busted is marked
        p2Payout = totalPotLamports; p2LedgerCode = 'win_de_pvp_opponent_bust';
        resultHeaderHTML = `💣 <b>${p1MentionHTML} BUSTED!</b>`;
        resultDetailsHTML = `${p2MentionHTML} wins by default!`;
    } else if (p2.busted || playerWhoBustedId === p2.userId) {
        titleEmoji = "💥"; winner = p1; p2.busted = true; // Ensure busted is marked
        p1Payout = totalPotLamports; p1LedgerCode = 'win_de_pvp_opponent_bust';
        resultHeaderHTML = `💣 <b>${p2MentionHTML} BUSTED!</b>`;
        resultDetailsHTML = `${p1MentionHTML} wins by default!`;
    } else if (p1.stood && p2.stood) {
        if (p1.score > p2.score) {
            titleEmoji = "🏆"; winner = p1; p1Payout = totalPotLamports; p1LedgerCode = 'win_de_pvp_score';
            resultHeaderHTML = `🏆 <b>${p1MentionHTML} WINS!</b>`;
            resultDetailsHTML = `Their score of <b>${p1.score}</b> beats ${p2MentionHTML}'s <i>${p2.score}</i>.`;
        } else if (p2.score > p1.score) {
            titleEmoji = "🏆"; winner = p2; p2Payout = totalPotLamports; p2LedgerCode = 'win_de_pvp_score';
            resultHeaderHTML = `🏆 <b>${p2MentionHTML} WINS!</b>`;
            resultDetailsHTML = `Their score of <b>${p2.score}</b> beats ${p1MentionHTML}'s <i>${p1.score}</i>.`;
        } else { 
            titleEmoji = "⚖️"; isPush = true;
            resultHeaderHTML = `⚖️ <b>IT'S A DRAW!</b>`;
            resultDetailsHTML = `Both players stood with <b>${p1.score}</b> points.`;
            p1Payout = betAmount; p2Payout = betAmount;
            p1LedgerCode = 'push_de_pvp'; p2LedgerCode = 'push_de_pvp';
        }
    } else if (gameData.status === 'game_over_p2_wins_by_crossing_score' && p2.score > p1.score && p1.stood) {
         titleEmoji = "🏆"; winner = p2; p2Payout = totalPotLamports; p2LedgerCode = 'win_de_pvp_score';
         resultHeaderHTML = `🏆 <b>${p2MentionHTML} WINS!</b>`;
         resultDetailsHTML = `Crossed ${p1MentionHTML}'s score of <i>${p1.score}</i> with a final score of <b>${p2.score}</b>!`;
    } else { 
        titleEmoji = "⚙️"; isPush = true;
        resultHeaderHTML = `⚙️ <b>Unexpected Game End</b>`;
        resultDetailsHTML = `The game concluded unexpectedly. Bets are refunded.`;
        p1Payout = betAmount; p2Payout = betAmount;
        p1LedgerCode = 'refund_de_pvp_error'; p2LedgerCode = 'refund_de_pvp_error';
        console.error(`[ResolveDE_PvP_HTML_V3] Undetermined outcome for GID ${gameData.gameId}. Refunding.`);
    }

    if (winner) {
        winningsFooterHTML = `🎉 <b>${escapeHTML(winner.displayName)}</b> wins the pot of <b>${escapeHTML(await formatBalanceForDisplay(totalPotLamports, 'USD'))}</b>!`;
    } else if (isPush) {
        winningsFooterHTML = `💰 Wagers of <b>${betDisplayUSD_HTML}</b> each are returned.`;
    }

    const finalMessageHTML = `${titleEmoji} <b>Dice Escalator PvP - Result!</b> ${titleEmoji}\n\n` +
                             `Wager: <b>${betDisplayUSD_HTML}</b> each\n\n` +
                             `--- <b>Final Scores</b> ---\n` +
                             `👤 ${p1MentionHTML} (P1): ${formatDiceRolls(p1.rolls)} Score: <b>${p1.score}</b> ${p1.busted ? "💥 BUSTED" : (p1.stood ? "✅ Stood" : "")}\n` +
                             `👤 ${p2MentionHTML} (P2): ${formatDiceRolls(p2.rolls)} Score: <b>${p2.score}</b> ${p2.busted ? "💥 BUSTED" : (p2.stood ? "✅ Stood" : "")}\n\n` +
                             `------------------------------------\n` +
                             `${resultHeaderHTML}\n${resultDetailsHTML}\n\n${winningsFooterHTML}`;

    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        // ... (DB logic for p1Update and p2Update as before) ...
        const p1Update = await updateUserBalanceAndLedger(client, p1.userId, p1Payout, p1LedgerCode, { game_id_custom_field: gameData.gameId, opponent_id_custom_field: p2.userId, player_score: p1.score, opponent_score: p2.score }, `DE PvP Result vs ${p2.mention}`);
        if (!p1Update.success) throw new Error(`P1 (${p1MentionHTML}) update fail: ${p1Update.error}`);
        const p2Update = await updateUserBalanceAndLedger(client, p2.userId, p2Payout, p2LedgerCode, { game_id_custom_field: gameData.gameId, opponent_id_custom_field: p1.userId, player_score: p2.score, opponent_score: p1.score }, `DE PvP Result vs ${p1.mention}`);
        if (!p2Update.success) throw new Error(`P2 (${p2MentionHTML}) update fail: ${p2Update.error}`);
        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK');
        console.error(`[ResolveDE_PvP_HTML_V3] CRITICAL DB Error: ${e.message}`);
        finalMessageHTML += `\n\n⚠️ <i>Critical error settling wagers. Admin notified.</i>`;
        if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL DE PvP Payout Failure 🚨\nGame ID: <code>${escapeHTML(gameData.gameId)}</code>\nError: ${escapeHTML(e.message)}. MANUAL CHECK REQUIRED.`, {parse_mode: 'HTML'});
    } finally {
        if (client) client.release();
    }

    if (gameData.currentMessageId && bot) { // currentMessageId from PvP context
        await bot.deleteMessage(String(gameData.chatId), Number(gameData.currentMessageId)).catch(() => {});
    }
    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR_PVP, betAmount);
    await safeSendMessage(gameData.chatId, finalMessageHTML, { parse_mode: 'HTML', reply_markup: finalKeyboard });
}
// --- End of Part 5b, Section 1 (COMPLETE DICE ESCALATOR LOGIC - HTML Revamp V3 - JackpotContribution Fix) ---
// index.js - Part 5b, Section 2 (Dice 21 / Blackjack-style game logic)
// SEGMENT 1 of 2 (Message Handling V7: Immediate Deletion of Player's Hit/Stand Prompt on Hit)
//-------------------------------------------------------------------------------------------------
// Assumed dependencies as before.

// --- Helper function for a single dice roll via Helper Bot (Unchanged) ---
async function getSingleDiceRollViaHelper(gameId, chatIdForLog, userIdForRoll, rollPurposeNote) {
    const logPrefix = `[GetSingleDiceRollHelper GID:${gameId} Purpose:"${rollPurposeNote}" UID:${userIdForRoll || 'BOT_INTERNAL'}]`;
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
        client.release(); client = null;
        let attempts = 0;
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
                    return { roll: statusResult.roll_value, error: false };
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
        console.error(`${logPrefix} Final error state in getSingleDiceRollViaHelper: ${finalErrorMessageForReturn}`);
        if (requestId) {
            let markErrorClient = null;
            try {
                markErrorClient = await pool.connect();
                const statusToUpdate = isTimeoutErrorFlag ? 'timeout' : 'error';
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

// --- Dice 21 Main Command Handler (Unchanged) ---
async function handleStartDice21Command(msg, betAmountLamports, targetUsernameRaw = null, gameModeArg = null) { 
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_D21_START_HTML = `[D21_OfferOrDirect UID:${userId} CH:${chatId} Type:${chatType}]`; 

    console.log(`${LOG_PREFIX_D21_START_HTML} Command /d21 received. Bet: ${betAmountLamports}, TargetUser: ${targetUsernameRaw || 'None'}, GameModeArg: ${gameModeArg || 'None'}`);

    let initiatorUserObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!initiatorUserObj) {
        await safeSendMessage(chatId, "Apologies, your player profile couldn't be accessed right now.<br>Please use the <code>/start</code> command with me first, and then try initiating the Dice 21 game again.", { parse_mode: 'HTML' });
        return;
    }
    const initiatorPlayerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj));

    if (chatType === 'private') {
        await safeSendMessage(chatId, `🎲 Greetings, ${initiatorPlayerRefHTML}!<br><br>The Dice 21 game, including direct challenges, must be initiated in a <b>group chat</b>.<br>Please use <code>/d21 &lt;bet&gt; [@username]</code> there to start the action.`, { parse_mode: 'HTML' });
        return;
    }

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        await safeSendMessage(chatId, `🃏 Salutations, ${initiatorPlayerRefHTML}! To begin a game of Dice 21, please specify a valid positive bet amount using USD or SOL.<br>For example: <code>/d21 10</code> or <code>/d21 0.2 sol</code>.`, { parse_mode: 'HTML' });
        return;
    }
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    const gameSession = await getGroupSession(chatId, msg.chat.title || `Group Chat ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        if ( (existingGame.type.includes('_offer') && existingGame.status === 'pending_offer') || 
             (existingGame.type === GAME_IDS.DIRECT_PVP_CHALLENGE && existingGame.status === 'pending_direct_challenge_response') ||
             (existingGame.type.includes('_pvp') && !existingGame.status?.startsWith('game_over_')) ||
             (existingGame.type === GAME_IDS.MINES && existingGame.status !== 'game_over_mine_hit' && existingGame.status !== 'game_over_cashed_out')
           ) {
            await safeSendMessage(chatId, `⏳ Please hold on, ${initiatorPlayerRefHTML}! Another game offer (like <code>${escapeHTML(existingGame.type.replace(/_/g, " "))}</code>) or an active Player vs Player match is currently underway in this group.<br>Kindly wait for it to conclude before initiating a new Dice 21 challenge.`, { parse_mode: 'HTML' });
            return;
        }
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        await safeSendMessage(chatId, `${initiatorPlayerRefHTML}, your casino balance is currently too low for a <b>${betDisplayUSD_HTML}</b> Dice 21 game.<br>You require approximately <b>${escapeHTML(await formatBalanceForDisplay(needed, 'USD'))}</b> more for this particular wager.`, {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: "💰 Top Up Balance (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    let targetUserObject = null;
    if (targetUsernameRaw) {
        targetUserObject = await findRecipientUser(targetUsernameRaw);
        if (!targetUserObject || !targetUserObject.telegram_id) { 
            await safeSendMessage(chatId, `😕 Player ${escapeHTML(targetUsernameRaw)} not found or has an invalid ID. Cannot create a Dice 21 challenge. Please ensure they have started a chat with me first.`, { parse_mode: 'HTML' });
            targetUserObject = null; // Ensure it's null to fall through to unified offer
        } else if (String(targetUserObject.telegram_id) === userId) {
            await safeSendMessage(chatId, `😅 You can't challenge yourself to a Dice 21 duel, ${initiatorPlayerRefHTML}! Creating a general offer instead.`, { parse_mode: 'HTML' });
            targetUserObject = null; // Force unified offer
        }
    }

    if (targetUserObject && targetUserObject.telegram_id) { // Ensure telegram_id is valid before direct challenge
        // --- DIRECT PvP CHALLENGE FLOW for Dice 21 ---
        console.log(`${LOG_PREFIX_D21_START_HTML} Initiating DIRECT Dice 21 challenge to User ID: ${targetUserObject.telegram_id} (@${targetUserObject.username || 'N/A'})`);
        const targetPlayerRefHTML = escapeHTML(getPlayerDisplayReference(targetUserObject));
        const offerId = generateGameId(`dd21_${userId.slice(-3)}_${String(targetUserObject.telegram_id).slice(-3)}`); 

        const groupChallengeTextHTML = `Hey ${targetPlayerRefHTML}❗\n\n${initiatorPlayerRefHTML} has challenged you to a game of <b>Dice 21 (Blackjack)</b> for <b>${betDisplayUSD_HTML}</b>!`;
        
        const groupChallengeKeyboard = { 
            inline_keyboard: [
                [{ text: "✅ Accept Challenge", callback_data: `dir_chal_acc:${offerId}` }],
                [{ text: "❌ Decline Challenge", callback_data: `dir_chal_dec:${offerId}` }],
                [{ text: "🚫 Withdraw My Challenge", callback_data: `dir_chal_can:${offerId}` }]
            ]
        };

        const sentGroupMessage = await safeSendMessage(chatId, groupChallengeTextHTML, { parse_mode: 'HTML', reply_markup: groupChallengeKeyboard });

        if (!sentGroupMessage || !sentGroupMessage.message_id) {
            console.error(`${LOG_PREFIX_D21_START_HTML} Failed to send direct Dice 21 challenge message for offer ${offerId}.`);
            await safeSendMessage(chatId, `⚙️ Oops! Couldn't send your Dice 21 challenge to ${targetPlayerRefHTML}. Please try again.`, { parse_mode: 'HTML' });
            return;
        }
        const offerMessageIdInGroup = String(sentGroupMessage.message_id);
        const groupNameHTML = escapeHTML(msg.chat.title || "the group");

        const dmNotificationTextHTML = `🔔 Challenge Alert!\n\nHi ${targetPlayerRefHTML},\n${initiatorPlayerRefHTML} has challenged you to a game of <b>Dice 21 (Blackjack)</b> for <b>${betDisplayUSD_HTML}</b> in the group "<b>${groupNameHTML}</b>".\n\nPlease head to that group to accept or decline the challenge.`;
        const dmSent = await safeSendMessage(targetUserObject.telegram_id, dmNotificationTextHTML, { parse_mode: 'HTML' });
        if(!dmSent){
            console.warn(`${LOG_PREFIX_D21_START_HTML} Failed to send DM notification for direct Dice 21 challenge to target ${targetUserObject.telegram_id}. Offer still posted.`);
            await safeSendMessage(chatId, `ℹ️ ${initiatorPlayerRefHTML}, your challenge to ${targetPlayerRefHTML} is posted! Note: They might not receive a DM if they haven't interacted with me before.`, { parse_mode: 'HTML'});
        }

        const directOfferData = {
            type: GAME_IDS.DIRECT_PVP_CHALLENGE, 
            offerId: offerId, 
            gameId: offerId,
            initiatorId: userId,
            initiatorUserObj: initiatorUserObj, 
            initiatorMentionHTML: initiatorPlayerRefHTML,
            targetUserId: String(targetUserObject.telegram_id), // Ensured to be valid
            targetUserObj: targetUserObject, 
            targetUserMentionHTML: targetPlayerRefHTML,
            betAmount: betAmountLamports,
            originalGroupId: chatId,
            offerMessageIdInGroup: offerMessageIdInGroup,
            chatTitle: msg.chat.title || `Group Chat ${chatId}`, 
            status: 'pending_direct_challenge_response',
            gameToStart: GAME_IDS.DICE_21_PVP, 
            creationTime: Date.now()
        };
        activeGames.set(offerId, directOfferData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.DIRECT_PVP_CHALLENGE, betAmountLamports);
        console.log(`${LOG_PREFIX_D21_START_HTML} Direct Dice 21 challenge offer ${offerId} created and stored.`);

        setTimeout(async () => {
            const timedOutOffer = activeGames.get(offerId);
            if (timedOutOffer && timedOutOffer.status === 'pending_direct_challenge_response' && timedOutOffer.type === GAME_IDS.DIRECT_PVP_CHALLENGE) {
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null);
                const gameNameForTimeout = "Dice 21";
                const timeoutBetDisplay = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                const timeoutMsgHTML = `⏳ The ${gameNameForTimeout} challenge from ${timedOutOffer.initiatorMentionHTML} to ${timedOutOffer.targetUserMentionHTML} for <b>${timeoutBetDisplay}</b> has expired unanswered.`;
                if (bot && timedOutOffer.offerMessageIdInGroup) {
                    await bot.editMessageText(timeoutMsgHTML, {
                        chat_id: timedOutOffer.originalGroupId, message_id: Number(timedOutOffer.offerMessageIdInGroup),
                        parse_mode: 'HTML', reply_markup: {} 
                    }).catch(e => { 
                        console.warn(`${LOG_PREFIX_D21_START_HTML} Failed to edit expired direct Dice 21 challenge msg ${timedOutOffer.offerMessageIdInGroup}: ${e.message}. Sending new.`);
                        safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                    });
                } else { 
                    safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                }
                await safeSendMessage(timedOutOffer.initiatorId, `⏳ Your Dice 21 challenge to ${timedOutOffer.targetUserMentionHTML} in group "${escapeHTML(timedOutOffer.chatTitle)}" has expired.`, { parse_mode: 'HTML' });
            }
        }, JOIN_GAME_TIMEOUT_MS);

    } else {
        // --- EXISTING DICE 21 UNIFIED OFFER FLOW ---
        console.log(`${LOG_PREFIX_D21_START_HTML} Initiating UNIFIED Dice 21 offer (no target user or target was invalid).`);
        const offerId = generateGameId(GAME_IDS.DICE_21_UNIFIED_OFFER);
        const offerMessageTextHTML =
            `🎲 <b>Dice 21 Challenge by ${initiatorPlayerRefHTML}!</b> 🎲\n\n` +
            `${initiatorPlayerRefHTML} has thrown down the gauntlet for a thrilling game of Dice 21, with a hefty wager of <b>${betDisplayUSD_HTML}</b> on the line!\n\n` +
            `Will any brave challengers step up for a Player vs Player showdown?\n` +
            `Alternatively, ${initiatorPlayerRefHTML} can choose to battle wits with our expert Bot Dealer. The choice is yours!`;

        const offerKeyboard = {
            inline_keyboard: [
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
            initiatorMention: initiatorPlayerRefHTML, 
            initiatorUserObj,
            betAmount: betAmountLamports,
            status: 'waiting_for_choice', 
            creationTime: Date.now(),
            gameSetupMessageId: null 
        };
        activeGames.set(offerId, offerData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.DICE_21_UNIFIED_OFFER, betAmountLamports);

        console.log(`${LOG_PREFIX_D21_START_HTML} Sending Dice 21 unified offer (ID: ${offerId}) to chat ${chatId}.`);
        const sentOfferMessage = await safeSendMessage(chatId, offerMessageTextHTML, { parse_mode: 'HTML', reply_markup: offerKeyboard });

        if (sentOfferMessage?.message_id) {
            const offerInMap = activeGames.get(offerId);
            if(offerInMap) {
                offerInMap.gameSetupMessageId = String(sentOfferMessage.message_id); 
                activeGames.set(offerId, offerInMap);
            } else {
                if (bot) await bot.deleteMessage(chatId, sentOfferMessage.message_id).catch(delErr => console.warn(`${LOG_PREFIX_D21_START_HTML} Could not delete potentially orphaned D21 unified offer message ${sentOfferMessage.message_id}: ${delErr.message}`));
            }
        } else {
            console.error(`${LOG_PREFIX_D21_START_HTML} CRITICAL: Failed to send Dice 21 unified offer message for offer ID ${offerId}. Cleaning up this offer attempt.`);
            activeGames.delete(offerId);
            await updateGroupGameDetails(chatId, null, null, null);
            await safeSendMessage(chatId, `An unexpected technical difficulty prevented the Dice 21 game offer by ${initiatorPlayerRefHTML} from being created. Please attempt the command again. If the issue continues, our support team is here to help.`, {parse_mode:'HTML'});
            return;
        }

        setTimeout(async () => {
            const currentOfferData = activeGames.get(offerId);
            if (currentOfferData && currentOfferData.status === 'waiting_for_choice' && currentOfferData.type === GAME_IDS.DICE_21_UNIFIED_OFFER) {
                console.log(`[D21_OfferTimeout OfferID:${offerId}] Unified Dice 21 offer has expired due to inactivity.`);
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null);

                if (currentOfferData.gameSetupMessageId && bot) {
                    const expiredOfferBetDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(currentOfferData.betAmount, 'USD'));
                    const offerExpiredMessageTextHTML = `⏳ The Dice 21 game offer initiated by ${currentOfferData.initiatorMention} for <b>${expiredOfferBetDisplayUSD_HTML}</b> has timed out as no option was selected. This offer is now closed.`;
                    await bot.editMessageText(offerExpiredMessageTextHTML, {
                        chat_id: String(chatId),
                        message_id: Number(currentOfferData.gameSetupMessageId),
                        parse_mode: 'HTML',
                        reply_markup: {}
                    }).catch(e => console.error(`${LOG_PREFIX_D21_START_HTML} Error editing message for expired D21 unified offer (ID: ${currentOfferData.gameSetupMessageId}): ${e.message}.`));
                }
            }
        }, JOIN_GAME_TIMEOUT_MS);
    }
}

// --- Callback Handlers for Unified Dice 21 Offer (Unchanged logic, PvB calls revised start) ---
async function handleDice21AcceptBotGame(offerId, initiatorUserObjFromCb, originalOfferMessageId, originalChatId, originalChatTypeFromRouter, callbackQueryId = null) {
    const initiatorId = String(initiatorUserObjFromCb.id || initiatorUserObjFromCb.telegram_id);
    const logPrefix = `[D21_AcceptBotCallback GID:${offerId} UID:${initiatorId}]`;
    const offerData = activeGames.get(offerId);
    const initiatorRef = getPlayerDisplayReference(initiatorUserObjFromCb);

    if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId).catch(() => {});


    if (!offerData || offerData.type !== GAME_IDS.DICE_21_UNIFIED_OFFER) {
        console.warn(`${logPrefix} ${initiatorRef} tried to accept a PvB game for an invalid or non-existent offer (ID: ${offerId}). Offer data present: ${!!offerData}`);
        const msgIdToClear = offerData?.gameSetupMessageId || originalOfferMessageId;
        if (msgIdToClear && bot) {
            await bot.editMessageText("This Dice 21 offer is no longer active or has already expired. Please ask for a new one if you wish to play.", {
                chat_id: originalChatId, message_id: Number(msgIdToClear),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(e => console.warn(`${logPrefix} Failed to edit outdated offer message ${msgIdToClear}: ${e.message}`));
        }
        return;
    }
    if (offerData.initiatorId !== initiatorId) {
        console.warn(`${logPrefix} User ${initiatorRef} (ID: ${initiatorId}) tried to accept PvB game for an offer made by ${offerData.initiatorMention} (ID: ${offerData.initiatorId}). This action is restricted to the offer initiator.`);
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
        console.warn(`${logPrefix} Offer ${offerId} (initiator: ${offerData.initiatorMention}) is not in 'waiting_for_choice' state. Current status: ${offerData.status}. Cannot start PvB game now.`);
        if (bot && offerData.gameSetupMessageId) {
            await bot.editMessageText(`This Dice 21 offer by ${offerData.initiatorMention} has already been actioned or has timed out. A new game cannot be started from this offer.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(e => console.warn(`${logPrefix} Minor error editing (status not waiting for choice) offer message ${offerData.gameSetupMessageId}: ${e.message}`));
        }
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

async function handleDice21AcceptPvPChallenge(offerId, joinerUserObjFromCb, originalOfferMessageId, originalChatId, originalChatType, callbackQueryId = null) {
    // This PvP handler remains unchanged.
    const joinerId = String(joinerUserObjFromCb.id || joinerUserObjFromCb.telegram_id);
    const logPrefix = `[D21_AcceptPvPCallback GID:${offerId} JoinerID:${joinerId}]`;
    let offerData = activeGames.get(offerId);
    const joinerRef = getPlayerDisplayReference(joinerUserObjFromCb);

    if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId).catch(() => {});

    if (!offerData || offerData.type !== GAME_IDS.DICE_21_UNIFIED_OFFER) {
        console.warn(`${logPrefix} PvP challenge acceptance by ${joinerRef} for an invalid or non-existent offer (ID: ${offerId}). Offer data present: ${!!offerData}`);
        const msgIdToClearOrUpdate = offerData?.gameSetupMessageId || originalOfferMessageId;
        if (msgIdToClearOrUpdate && bot) {
           await bot.editMessageText("This Dice 21 game offer has expired or is no longer available for a PvP match. Please ask for a new offer if you wish to play.", {
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
            await bot.editMessageText(`This Dice 21 offer by ${offerData.initiatorMention} is no longer available to be joined for a PvP match. It may have been started as a Bot game, cancelled, or has already been accepted by another player.`, {
                chat_id: originalChatId, message_id: Number(offerData.gameSetupMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(e => console.warn(`${logPrefix} Minor error editing (not waiting for choice) PvP offer message ${offerData.gameSetupMessageId}: ${e.message}`));
        }
        return;
    }

    console.log(`${logPrefix} Player ${joinerRef} is accepting the PvP challenge from ${offerData.initiatorMention} (Offer ID: ${offerId}). Verifying funds for both players.`);
    const betAmount = offerData.betAmount;
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));

    let currentJoinerUserObj = await getOrCreateUser(joinerId, joinerUserObjFromCb.username, joinerUserObjFromCb.first_name, joinerUserObjFromCb.last_name);
    if (!currentJoinerUserObj) {
        console.warn(`${logPrefix} Could not get/create user object for joiner ${joinerRef}.`);
        await safeSendMessage(originalChatId, `An error occurred fetching the player profile for ${joinerRef} to join the Dice 21 PvP game. Please ensure they have used the \`/start\` command with me at least once before trying to join games.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    if (BigInt(currentJoinerUserObj.balance) < betAmount) {
        console.log(`${logPrefix} Joiner ${joinerRef} has insufficient balance for PvP game. Needs ${betAmount}, has ${currentJoinerUserObj.balance}.`);
        await safeSendMessage(originalChatId, `${joinerRef}, your current casino balance is insufficient to join this *${betDisplayUSD}* Dice 21 PvP game. Please top up your funds if you wish to accept the challenge.`, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: "💰 Top Up Balance (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    let currentInitiatorUserObj = await getOrCreateUser(offerData.initiatorId);
    if (!currentInitiatorUserObj || BigInt(currentInitiatorUserObj.balance) < betAmount) {
        console.warn(`${logPrefix} Initiator ${offerData.initiatorMention} no longer has sufficient funds for the PvP wager (${betAmount}). Cancelling offer ${offerId}.`);
        const offerMessageIdToUpdateOrDelete = offerData.gameSetupMessageId || originalOfferMessageId;
        const cancelTextDueToInitiatorFunds = `The Dice 21 PvP offer from ${offerData.initiatorMention} (for *${betDisplayUSD}*) has been automatically cancelled. The initiator, ${offerData.initiatorMention}, no longer has sufficient funds to cover the wager. A new offer can be made once funds are available.`;

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
        await safeSendMessage(originalChatId, `A critical server error occurred while starting the Dice 21 PvP game: \`${displayErrorForUser}\`. The game could not be created. Please try creating a new offer if you wish to play.`, { parse_mode: 'MarkdownV2'});

        activeGames.delete(offerId);
        if (activeGames.has(pvpGameId)) activeGames.delete(pvpGameId);
        await updateGroupGameDetails(originalChatId, null, null, null);

        if (typeof notifyAdmin === 'function') {
            const adminErrorNotification =
                `🚨 D21 PvP Game Start Failure - POTENTIAL FUNDING ISSUE 🚨\n` +
                `Attempted Game ID: \`${pvpGameId}\` (from Offer ID: \`${offerId}\`)\n` +
                `Error: \`${displayErrorForUser}\`\n` +
                `Players: ${offerData.initiatorMention} & ${joinerRef}.\n` +
                `This error occurred after bet placement was attempted. Bets might have been deducted without the game starting. ` +
                `MANUAL VERIFICATION OF BALANCES AND POTENTIAL REFUNDS REQUIRED for both players.`;
            notifyAdmin(adminErrorNotification, {parse_mode:'MarkdownV2'});
        }
        return;
    } finally {
        if (client) client.release();
    }
}

async function handleDice21CancelUnifiedOffer(offerId, initiatorUserObjFromCb, originalOfferMessageId, originalChatId, callbackQueryId = null) {
    const initiatorId = String(initiatorUserObjFromCb.id || initiatorUserObjFromCb.telegram_id);
    const LOG_PREFIX_D21_CANCEL_OFFER = `[D21_CancelOffer_V2 GID:${offerId} UID:${initiatorId}]`; // Added V2
    const offerData = activeGames.get(offerId);
    const initiatorRef = getPlayerDisplayReference(initiatorUserObjFromCb); // Ensure getPlayerDisplayReference is available

    if (!offerData || offerData.type !== GAME_IDS.DICE_21_UNIFIED_OFFER) {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This offer has already concluded or vanished!", show_alert: false }).catch(() => {});
        if (originalOfferMessageId && bot) { // Try to remove buttons from the stale message
            bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalOfferMessageId) }).catch(() => {});
        }
        return;
    }

    if (offerData.initiatorId !== initiatorId) {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Only the one who made the offer can retract it.", show_alert: true }).catch(() => {});
        return;
    }

    // Unified Dice 21 offer status should be 'waiting_for_choice' to be cancellable
    if (offerData.status !== 'waiting_for_choice') {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This offer has already been actioned or expired.", show_alert: false }).catch(() => {});
        return;
    }

    if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Cancelling Dice 21 offer..." }).catch(() => {});

    // No refund logic needed here if bet is only taken when game starts (PvB or PvP chosen)

    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null); // Clear from group session

    console.log(`${LOG_PREFIX_D21_CANCEL_OFFER} Dice 21 offer ${offerId} has been cancelled by ${initiatorRef}.`);

    // Delete the original offer message
    const messageIdToDelete = Number(originalOfferMessageId || offerData.gameSetupMessageId);
    if (messageIdToDelete && bot) {
        await bot.deleteMessage(String(originalChatId), messageIdToDelete)
            .catch(e => console.warn(`${LOG_PREFIX_D21_CANCEL_OFFER} Failed to delete cancelled Dice 21 offer message ${messageIdToDelete}: ${e.message}`));
    }

    // Send a new confirmation message
    // Ensure formatBalanceForDisplay and escapeMarkdownV2 are available
    const betDisplayUSD = typeof formatBalanceForDisplay === 'function' ? escapeMarkdownV2(await formatBalanceForDisplay(offerData.betAmount, 'USD')) : `${offerData.betAmount / LAMPORTS_PER_SOL} SOL`;
    const confirmationMessage = `🚫 Offer Cancelled!\nThe Dice 21 challenge by ${offerData.initiatorMention} for *${betDisplayUSD}* has been withdrawn.`;
    
    await safeSendMessage(originalChatId, confirmationMessage, { parse_mode: 'MarkdownV2' });
}

// --- REVISED Player vs. Bot (PvB) Dice 21 Game Logic (V7: Refined Player Hit & Bot Turn Messaging) ---
async function startDice21PvBGame(chatId, initiatorUserObj, betAmountLamports, originalCmdOrOfferMsgId, isPrivateChatStart = false, unifiedOfferIdIfAny = null, chatTypeFromCaller) {
    const userId = String(initiatorUserObj.telegram_id);
    const currentChatType = chatTypeFromCaller || (isPrivateChatStart ? 'private' : 'group'); // Determine chat type
    const logPrefix = `[D21_PvB_Start_HTML_V8 UID:${userId} CH:${chatId}]`;

    const playerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj)); // HTML Safe
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    // Delete original offer message if applicable
    if (unifiedOfferIdIfAny && originalCmdOrOfferMsgId && bot && currentChatType !== 'private') {
        await bot.deleteMessage(chatId, Number(originalCmdOrOfferMsgId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Could not delete unified D21 offer message ${originalCmdOrOfferMsgId}: ${e.message}`));
    } else if (originalCmdOrOfferMsgId && bot && (isPrivateChatStart || chatId === userId) ) { // If /d21 command was in DM
        await bot.deleteMessage(chatId, Number(originalCmdOrOfferMsgId)).catch(()=>{});
    }

    let client = null;
    const gameIdForActivePvB = generateGameId(GAME_IDS.DICE_21);

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
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports; // Update local object
        await client.query('COMMIT');

        const gameDataPvB = {
            type: GAME_IDS.DICE_21, gameId: gameIdForActivePvB, chatId: String(chatId),
            chatType: currentChatType,
            playerId: userId, playerRef: playerRefHTML, // Store HTML safe version
            userObj: initiatorUserObj, betAmount: betAmountLamports,
            playerScore: 0, botScore: 0, playerHandRolls: [], botHandRolls: [],
            status: 'player_initial_roll_1_prompted', // Player needs to send first die
            gameMessageId: null,
            intermediateMessageIds: [], // To store IDs of messages to delete later
            lastInteractionTime: Date.now()
        };
        activeGames.set(gameIdForActivePvB, gameDataPvB);

        if (currentChatType !== 'private') {
            await updateGroupGameDetails(chatId, gameIdForActivePvB, GAME_IDS.DICE_21, betAmountLamports);
        }
        if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) {
            activeGames.delete(unifiedOfferIdIfAny);
        }

        // Send the initial game message using HTML
        const titleHTML = `🎲 <b>Dice 21 vs. Bot Dealer</b> 🎲`;
        const initialPromptTextHTML = `${titleHTML}\n\n` +
                                     `Player: ${playerRefHTML}\n` +
                                     `Wager: <b>${betDisplayUSD_HTML}</b>\n\n` +
                                     `It's your turn, ${playerRefHTML}!\n` +
                                     `Please send your <b>first</b> 🎲 dice emoji to roll.`;

        const initialPromptMsg = await safeSendMessage(chatId, initialPromptTextHTML, { parse_mode: 'HTML' });
        if (initialPromptMsg?.message_id) {
            const gd = activeGames.get(gameIdForActivePvB);
            if(gd) gd.gameMessageId = initialPromptMsg.message_id; // This is the main game message now
        } else {
            throw new Error("Failed to send initial prompt message for Dice 21. Cannot track game UI.");
        }
        // console.log(`${logPrefix} Game ${gameIdForActivePvB} started. Player prompted for 1st die. Main prompt ID: ${gameDataPvB.gameMessageId}`);

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} Error starting Dice 21 PvB game: ${error.message}`);
        const finalUserErrorMessageText = `⚙️ <b>Game Setup Error</b>\n${playerRefHTML}, we hit a snag setting up your Dice 21 game for <b>${betDisplayUSD_HTML}</b>.\nDetails: <code>${escapeHTML(error.message || "Unknown setup error")}</code>\nYour bet might not have been placed. Please try again or check your balance.`;
        const targetErrorChatId = (currentChatType === 'private') ? chatId : userId; // Send error to DM if possible
        await safeSendMessage(targetErrorChatId, finalUserErrorMessageText, { parse_mode: 'HTML' });

        // Cleanup game state if error occurred after creation
        activeGames.delete(gameIdForActivePvB);
        if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) activeGames.delete(unifiedOfferIdIfAny);
        if (currentChatType !== 'private') await updateGroupGameDetails(chatId, null, null, null);
    } finally {
        if (client) client.release();
    }
}
// END OF SEGMENT 1 of 2 for "Part 5b, Section 2"
// index.js - Part 5b, Section 2 (Dice 21 / Blackjack-style game logic)
// SEGMENT 2 of 2 (Message Handling V7: Player "Send New Prompt/Collect Old", Bot "Edit Single Msg", Definitive Stand, Final Deletion)
//-------------------------------------------------------------------------------------------------
// (Continues from Segment 1 of 2)

// --- Player vs. Bot (PvB) Dice 21 Gameplay Logic (Continued) ---

// REVISED handleDice21PvBCancel (collects gameMessageId for final deletion)
async function handleDice21PvBCancel(gameId, userObj, originalMessageId, callbackQueryId, chatData) {
    const playerId = String(userObj.id || userObj.telegram_id);
    const chatId = String(chatData.id);
    let gameData = activeGames.get(gameId); // Get fresh gameData
    const logPrefix = `[D21_PvBCancel GID:${gameId} UID:${playerId} V7]`;
    const playerRef = getPlayerDisplayReference(userObj);

    if (!gameData || gameData.type !== GAME_IDS.DICE_21 || gameData.playerId !== playerId) {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ This Dice 21 game is not currently active or it doesn't belong to you.", show_alert: true }).catch(()=>{});
        return;
    }

    if (gameData.status.startsWith('game_over_') || gameData.status === 'bot_rolling' || gameData.status === 'bot_turn_pending_rolls' || gameData.status === 'finalizing' || gameData.status === 'player_blackjack' || gameData.status === 'player_action_processing_stand') {
        if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Too late to cancel! The game is already resolving or past that point.", show_alert: true }).catch(()=>{});
        return;
    }

    if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "Processing your game forfeit..."}).catch(()=>{});

    if (gameData.gameMessageId && bot) { // Delete the current prompt immediately
        await bot.deleteMessage(chatId, Number(gameData.gameMessageId)).catch(() => {console.warn(`${logPrefix} Could not delete prompt message ${gameData.gameMessageId} on forfeit.`);});
        gameData.gameMessageId = null;
    }
    console.log(`${logPrefix} Player ${playerRef} is forfeiting PvB Dice 21 game ${gameId}.`);
    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const forfeitLogResult = await updateUserBalanceAndLedger(client, playerId, 0n,
            'loss_dice21_pvb_forfeit',
            { game_id_custom_field: gameId, final_player_score_before_forfeit: gameData.playerScore, bet_amount: gameData.betAmount },
            `Player ${playerRef} forfeited PvB Dice 21 game ${gameId}. Wager lost.`
        );
        if (!forfeitLogResult.success) {
             console.error(`${logPrefix} DB Error logging forfeit: ${forfeitLogResult.error}.`);
        }
        await client.query('COMMIT');

        gameData.status = 'game_over_player_forfeit';
        activeGames.set(gameId, gameData); // Update status before finalize

        await finalizeDice21PvBGame(gameData);

    } catch (e) {
        if (client) await client.query('ROLLBACK').catch((rbErr) => {console.error(`${logPrefix} DB Rollback Exception on forfeit: ${rbErr.message}`)});
        console.error(`${logPrefix} CRITICAL DB Error processing forfeit for game ${gameId}: ${e.message}.`);
        await safeSendMessage(playerId, `An unexpected server error occurred while processing your forfeit for the Dice 21 game, ${playerRef}. Please contact support.`, { parse_mode: 'MarkdownV2' });
        if(typeof notifyAdmin === 'function') {
            notifyAdmin(`🚨 D21 PvB Forfeit Processing DB Error 🚨\nGame ID: \`${gameId}\`, User: ${playerRef} (\`${escapeMarkdownV2(String(playerId))}\`)\nError: \`${escapeMarkdownV2(e.message)}\`.`, {parse_mode:'MarkdownV2'});
        }
        // Attempt to finalize even on error to clean up state and inform user
        let gdOnError = activeGames.get(gameId); // Re-fetch as it might have been modified
        if (gdOnError) {
            gdOnError.status = 'game_over_error_ui_update';
            await finalizeDice21PvBGame(gdOnError);
        } else { // If gameData was deleted due to another error path
            activeGames.delete(gameId); // Ensure it's cleaned up
        }
    } finally { if (client) client.release(); }

    if (gameData.chatType !== 'private' && chatId) {
        await updateGroupGameDetails(chatId, null, null, null);
    }
}


// --- From Part 5b, Section 2 (Dice 21 / Blackjack-style game logic) - SEGMENT 2 of 2 ---

// REVISED processDice21PvBRollByEmoji (V7 - HTML for player prompts)
async function processDice21PvBRollByEmoji(gameDataInput, diceValueRolledByPlayer, msgContext) {
    let gameData = activeGames.get(gameDataInput.gameId); 
    if (!gameData) {
        // User's dice emoji (msgContext.message_id) is already deleted by the main message handler
        return;
    }

    const logPrefix = `[D21_PvB_Roll_HTML_V9_Consolidated GID:${gameData.gameId} UID:${gameData.playerId} Val:${diceValueRolledByPlayer}]`;

    if (gameData.status !== 'player_initial_roll_1_prompted' &&
        gameData.status !== 'player_initial_roll_2_prompted' &&
        gameData.status !== 'player_turn_hit_stand_prompt') {
        // User's dice emoji already deleted by main message handler. Game not in correct state.
        return;
    }

    gameData.lastInteractionTime = Date.now();
    const playerRefHTML = gameData.playerRef; // Assumed HTML escaped from startDice21PvBGame
    const chatId = gameData.chatId;
    const originalStatusBeforeRoll = gameData.status;
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    // Delete the previous main game prompt message
    if (gameData.gameMessageId && bot) {
        await bot.deleteMessage(chatId, Number(gameData.gameMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete previous game prompt ${gameData.gameMessageId}: ${e.message}`));
        gameData.gameMessageId = null;
    }
    // Clear any other intermediate messages (should be empty with this flow)
    for (const mid of gameData.intermediateMessageIds || []) {
        if (bot) await bot.deleteMessage(chatId, mid).catch(() => {});
    }
    gameData.intermediateMessageIds = [];


    gameData.playerHandRolls.push(diceValueRolledByPlayer);
    gameData.playerScore += diceValueRolledByPlayer;

    // --- Construct the new single, consolidated main game message ---
    const titleHTML = `🎲 <b>Dice 21 vs. Bot Dealer</b> 🎲`;
    let rollConfirmationText = `You rolled a <b>${escapeHTML(String(diceValueRolledByPlayer))}</b>.`;
    let currentHandDisplay = `Your Hand: ${formatDiceRolls(gameData.playerHandRolls)} ➠ Score: <b>${escapeHTML(String(gameData.playerScore))}</b>`;
    
    let nextActionPrompt = "";
    let nextKeyboard = null;

    if (originalStatusBeforeRoll === 'player_initial_roll_1_prompted') {
        gameData.status = 'player_initial_roll_2_prompted';
        nextActionPrompt = `Please send your <b>second</b> 🎲 dice emoji.`;
    } else if (originalStatusBeforeRoll === 'player_initial_roll_2_prompted' || originalStatusBeforeRoll === 'player_turn_hit_stand_prompt') {
        if (gameData.playerScore > DICE_21_TARGET_SCORE) {
            gameData.status = 'game_over_player_bust';
            rollConfirmationText = `You rolled a <b>${escapeHTML(String(diceValueRolledByPlayer))}</b>!`; // Update confirmation for bust
            nextActionPrompt = `💥 <b>BUST!</b> Your score is over ${DICE_21_TARGET_SCORE}.\n<i>Game result incoming...</i>`;
        } else if (gameData.playerScore === DICE_21_TARGET_SCORE) {
            gameData.status = 'player_blackjack'; 
            const blackjackBonus = (gameData.playerHandRolls.length === 2);
            rollConfirmationText = `You rolled a <b>${escapeHTML(String(diceValueRolledByPlayer))}</b>!`; // Update confirmation
            nextActionPrompt = `${blackjackBonus ? '✨ <b>BLACKJACK!</b>' : '🎯 <b>Perfect 21!</b>'}\nYou automatically stand. <i>Bot's turn...</i> 🤖`;
        } else { // Score is < 21
            gameData.status = 'player_turn_hit_stand_prompt';
            nextActionPrompt = `Send another 🎲 to <b>Hit</b>, or click <b>Stand</b>.`;
            nextKeyboard = { inline_keyboard: [[{ text: `✅ Stand (${gameData.playerScore})`, callback_data: `d21_stand:${gameData.gameId}` }, { text: `🚫 Forfeit Game`, callback_data: `d21_pvb_cancel:${gameData.gameId}` }]] };
        }
    }

    let newMainMessageHTML = `${titleHTML}\n\n` +
                             `Player: ${playerRefHTML}\n` +
                             `Wager: <b>${betDisplayUSD_HTML}</b>\n\n` +
                             `${rollConfirmationText}\n${currentHandDisplay}\n\n` + // Combined roll info and hand display
                             `${nextActionPrompt}`;

    const newMainGameMsg = await safeSendMessage(chatId, newMainMessageHTML, { parse_mode: 'HTML', reply_markup: nextKeyboard });
    if (newMainGameMsg?.message_id) {
        gameData.gameMessageId = newMainGameMsg.message_id;
    } else if (gameData.status !== 'game_over_player_bust' && gameData.status !== 'player_blackjack') {
        // If sending the prompt failed and it's not game over, this is a problem
        console.error(`${logPrefix} Failed to send next prompt message for GID ${gameData.gameId}. Setting game to error state.`);
        gameData.status = 'game_over_error_ui_update'; 
    }
    activeGames.set(gameData.gameId, gameData); 

    // Handle game end conditions triggered by player's roll
    if (gameData.status === 'game_over_player_bust' || gameData.status === 'game_over_error_ui_update') {
        await sleep(1000); 
        await finalizeDice21PvBGame(gameData);
    } else if (gameData.status === 'player_blackjack') {
        await sleep(1500); 
        const freshDataForBot = activeGames.get(gameData.gameId); 
        if (freshDataForBot && freshDataForBot.status === 'player_blackjack') {
            freshDataForBot.status = 'bot_turn_pending_rolls'; 
            activeGames.set(gameData.gameId, freshDataForBot);
            await processDice21BotTurn(freshDataForBot);
        }
    }
}


// REVISED handleDice21PvBStand (Immediate Deletion of Hit/Stand Prompt)
async function handleDice21PvBStand(gameId, userObject, originalMessageId, callbackQueryId, chatData) {
    const playerId = String(userObject.id || userObject.telegram_id);
    let gameData = activeGames.get(gameId); // Get fresh gameData
    const logPrefix = `[D21_Stand_PvB_HTML_V8 GID:${gameId} UID:${playerId}]`;

    if (!gameData || gameData.type !== GAME_IDS.DICE_21 || gameData.playerId !== playerId) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "⚠️ Invalid game action.", show_alert: true }).catch(()=>{});
        return;
    }
    if (gameData.status !== 'player_turn_hit_stand_prompt') {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "It's not your turn to stand or the action expired.", show_alert: true }).catch(()=>{});
        return;
    }

    gameData.status = 'player_action_processing_stand'; // Intermediate status
    activeGames.set(gameId, gameData);

    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: `You chose to stand with ${gameData.playerScore}. Processing...`}).catch(()=>{});

    // Delete the Hit/Stand prompt message
    if (originalMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(originalMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete Hit/Stand prompt ${originalMessageId}: ${e.message}`));
        if (String(gameData.gameMessageId) === String(originalMessageId)) {
            gameData.gameMessageId = null;
        }
    }
    // Clear other intermediate messages too
    for (const mid of gameData.intermediateMessageIds) {
        if (bot) await bot.deleteMessage(gameData.chatId, mid).catch(() => {});
    }
    gameData.intermediateMessageIds = [];

    const playerRefHTML = gameData.playerRef; // Already HTML escaped
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    const titleHTML = `🎲 <b>Dice 21 vs. Bot Dealer</b> 🎲`;
    
    let standMessageTextHTML = `${titleHTML}\n\n` +
                               `Player: ${playerRefHTML}\n` +
                               `Wager: <b>${betDisplayUSD_HTML}</b>\n\n` +
                               `Your Hand: ${formatDiceRolls(gameData.playerHandRolls)} ➠ Score: <b>${escapeHTML(String(gameData.playerScore))}</b>\n\n` +
                               `✋ ${playerRefHTML} stands with <b>${escapeHTML(String(gameData.playerScore))}</b>.\n<i>Bot's turn is next...</i> 🤖`;

    const standMsg = await safeSendMessage(gameData.chatId, standMessageTextHTML, { parse_mode: 'HTML'});
    if (standMsg?.message_id) {
        gameData.gameMessageId = standMsg.message_id; // This "Player stands..." message is the new main game message
    }

    gameData.status = 'bot_turn_pending_rolls'; // Final status before bot turn
    gameData.lastInteractionTime = Date.now();
    activeGames.set(gameId, gameData);

    await sleep(1500); // Pause for player to read
    const freshGameDataForBotPlay = activeGames.get(gameId);
    if (freshGameDataForBotPlay && freshGameDataForBotPlay.status === 'bot_turn_pending_rolls') {
        await processDice21BotTurn(freshGameDataForBotPlay);
    } else {
        // Handle unexpected state, e.g., game was already resolved or status changed
        console.warn(`${logPrefix} Bot turn was expected, but game status is now ${freshGameDataForBotPlay?.status}. Game GID: ${gameId}`);
        if (freshGameDataForBotPlay && !freshGameDataForBotPlay.status.startsWith('game_over_')) {
             freshGameDataForBotPlay.status = 'game_over_error_ui_update';
             activeGames.set(gameId, freshGameDataForBotPlay);
             await finalizeDice21PvBGame(freshGameDataForBotPlay);
        }
    }
}

// --- From Part 5b, Section 2 (Dice 21 / Blackjack-style game logic) - SEGMENT 2 of 2 ---

// REVISED processDice21BotTurn with fix for ReferenceError and correct stand logic
async function processDice21BotTurn(gameData) {
    const logPrefix = `[D21_BotTurn_HTML_V16_ShowDice GID:${gameData.gameId}]`;
    if (!gameData || isShuttingDown || gameData.status !== 'bot_turn_pending_rolls') {
        return;
    }

    gameData.status = 'bot_rolling';
    gameData.botScore = 0;
    gameData.botHandRolls = [];
    // gameData.gameMessageId will store the ID of the LATEST bot status message (not the animated dice)
    // gameData.intermediateMessageIds will store IDs of the animated dice messages + "preparing to roll" message for later cleanup

    const playerRefHTML = gameData.playerRef; // Assumed HTML escaped
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
    const titleHTML = `🎲 <b>Dice 21 vs. Bot Dealer</b> 🎲`;

    // Delete the previous main game message (e.g., "Player stands...")
    if (gameData.gameMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.gameMessageId)).catch(()=>{/* ignore if already deleted */});
        gameData.gameMessageId = null;
    }
    // Ensure intermediateMessageIds is initialized if not present
    if (!gameData.intermediateMessageIds) {
        gameData.intermediateMessageIds = [];
    }


    // Send an initial message for the bot's turn
    let initialBotTurnMessageHTML = `${titleHTML}\n\n` +
                                    `Player: ${playerRefHTML}\n` +
                                    `Wager: <b>${betDisplayUSD_HTML}</b>\n\n` +
                                    `Your Hand: ${formatDiceRolls(gameData.playerHandRolls)} ➠ Score: <b>${escapeHTML(String(gameData.playerScore))}</b> (Stood)\n\n` +
                                    `🤖 <b>Bot Dealer is preparing to roll...</b>`;

    const prepMsg = await safeSendMessage(gameData.chatId, initialBotTurnMessageHTML, { parse_mode: 'HTML' });
    if (prepMsg?.message_id) {
        // This "preparing" message is an intermediate message that will be cleaned up by finalize.
        gameData.intermediateMessageIds.push(prepMsg.message_id);
    } else {
        console.error(`${logPrefix} CRITICAL: Failed to send initial 'preparing to roll' message. Aborting bot turn for GID ${gameData.gameId}.`);
        gameData.status = 'game_over_error_ui_update';
        activeGames.set(gameData.gameId, gameData);
        await finalizeDice21PvBGame(gameData);
        return;
    }
    activeGames.set(gameData.gameId, gameData);

    await sleep(1200); // Pause after "Bot is preparing to roll..."

    let botFaultedInTurn = false;
    let rollsThisTurn = 0;
    const MAX_BOT_ROLLS_SAFETY = 10;

    while (gameData.botScore < DICE_21_BOT_STAND_SCORE && !botFaultedInTurn && rollsThisTurn < MAX_BOT_ROLLS_SAFETY) {
        if (isShuttingDown) break;
        rollsThisTurn++;
        let rollVal;
        let sentDiceAnimationMsgId = null;

        try {
            const diceMsg = await bot.sendDice(gameData.chatId, { emoji: '🎲' }); // Bot sends animated dice
            rollVal = diceMsg.dice.value;
            sentDiceAnimationMsgId = diceMsg.message_id;
            if (sentDiceAnimationMsgId) {
                gameData.intermediateMessageIds.push(sentDiceAnimationMsgId); // Store for later cleanup by finalize
            }
            // **NO DELETION of animated dice here - Let it play out**
            await sleep(2500); // Wait for animation to mostly finish and for message to be seen
        } catch (sendDiceError) {
            console.error(`${logPrefix} Failed to send animated dice for bot roll ${rollsThisTurn} (GID ${gameData.gameId}): ${sendDiceError.message}. Using internal fallback.`);
            rollVal = Math.floor(Math.random() * 6) + 1;
            const fallbackRollMsg = await safeSendMessage(gameData.chatId, `⚙️ Bot (internal roll ${rollsThisTurn}): <b>${rollVal}</b>`, {parse_mode: 'HTML'});
            if(fallbackRollMsg?.message_id) {
                gameData.intermediateMessageIds.push(fallbackRollMsg.message_id); // Store for later cleanup
            }
            await sleep(1500); // Pause after fallback roll message
        }

        gameData.botHandRolls.push(rollVal);
        gameData.botScore += rollVal;

        // Construct the NEW status message for this roll
        let botRollStatusMessageHTML = `🤖 Bot rolled a <b>${rollVal}</b>.\n` + // More concise roll announcement
                                     `Current Hand: ${formatDiceRolls(gameData.botHandRolls)} ➠ Score: <b>${escapeHTML(String(gameData.botScore))}</b>`;

        if (gameData.botScore > DICE_21_TARGET_SCORE) {
            botRollStatusMessageHTML += `\n💥 Bot BUSTS with <b>${escapeHTML(String(gameData.botScore))}</b>!`;
        } else if (gameData.botScore >= DICE_21_BOT_STAND_SCORE) {
            botRollStatusMessageHTML += `\n✋ Bot STANDS with <b>${escapeHTML(String(gameData.botScore))}</b>.`;
        } else {
            botRollStatusMessageHTML += `\n<i>Bot rolling again...</i>`;
        }

        const newBotStatusMsg = await safeSendMessage(gameData.chatId, botRollStatusMessageHTML, { parse_mode: 'HTML' });
        if (newBotStatusMsg?.message_id) {
            gameData.gameMessageId = newBotStatusMsg.message_id; // This is the latest status message
            gameData.intermediateMessageIds.push(newBotStatusMsg.message_id); // Also add to intermediate for cleanup
        } else {
            console.error(`${logPrefix} CRITICAL: Failed to send new bot turn status message for roll ${rollsThisTurn} (GID ${gameData.gameId}).`);
            botFaultedInTurn = true;
            break;
        }
        activeGames.set(gameData.gameId, gameData);

        if (gameData.botScore > DICE_21_TARGET_SCORE || gameData.botScore >= DICE_21_BOT_STAND_SCORE) {
            break;
        }
        await sleep(1000); // Pause before next potential bot roll
    }

    // If bot hit max rolls safety, send one final status update message
    if (!botFaultedInTurn && rollsThisTurn >= MAX_BOT_ROLLS_SAFETY && gameData.botScore < DICE_21_BOT_STAND_SCORE && gameData.botScore <= DICE_21_TARGET_SCORE) {
        let safetyStandMessageHTML = `🤖 Bot Dealer's Hand: ${formatDiceRolls(gameData.botHandRolls)} ➠ Score: <b>${escapeHTML(String(gameData.botScore))}</b>\n\n` +
                                     `✋ Bot reached roll limit. Standing with <b>${escapeHTML(String(gameData.botScore))}</b>.`;
        const newSafetyMsg = await safeSendMessage(gameData.chatId, safetyStandMessageHTML, { parse_mode: 'HTML' });
        if (newSafetyMsg?.message_id) {
            gameData.gameMessageId = newSafetyMsg.message_id; // This is now the latest status
            gameData.intermediateMessageIds.push(newSafetyMsg.message_id);
        }
        activeGames.set(gameData.gameId, gameData);
    }

    if (botFaultedInTurn) {
        gameData.status = 'game_over_bot_error';
    } else {
        gameData.status = 'game_over_bot_played';
    }
    activeGames.set(gameData.gameId, gameData);

    await sleep(1500);
    // finalizeDice21PvBGame will use gameData.intermediateMessageIds (which now includes all bot turn messages
    // and animated dice messages) to clean up, and then send the final result as a new message.
    // It will also delete gameData.gameMessageId (the very last bot status message).
    await finalizeDice21PvBGame(gameData);
}

// REVISED finalizeDice21PvBGame (to use HTML parse_mode for the final message)
async function finalizeDice21PvBGame(gameData) {
    const logPrefix = `[D21_PvB_Finalize GID:${gameData.gameId} V7_HTML_Winnings_Fix]`; 

    if (!gameData) {
        console.error(`${logPrefix} Finalize called but gameData is missing. Cannot proceed.`);
        return;
    }

    const finalStatus = gameData.status;
    // console.log(`${logPrefix} Finalizing game. Player: ${gameData.playerRef}, PScore: ${gameData.playerScore}, BScore: ${gameData.botScore}, Status: ${finalStatus}`);

    if (gameData.gameMessageId) {
        gameData.intermediateMessageIds.push(gameData.gameMessageId);
    }
    const messagesToDelete = [...gameData.intermediateMessageIds];
    activeGames.delete(gameData.gameId);

    if (gameData.chatType !== 'private') {
        await updateGroupGameDetails(gameData.chatId, null, null, null);
    }

    let resultTitle = "🏁 Dice 21 Result 🏁";
    let resultOutcomeText = "";
    let payoutLamports = 0n; // Correctly declared and initialized
    let playerWins = false;
    let playerBlackjack = (gameData.playerScore === DICE_21_TARGET_SCORE && gameData.playerHandRolls.length === 2);
    const betAmount = gameData.betAmount; // This is a BigInt
    const betDisplayUSDShort = await formatBalanceForDisplay(betAmount, 'USD', 2);

    if (finalStatus === 'game_over_player_bust') {
        resultTitle = "💥 Player Busts!";
        resultOutcomeText = `Your score: <b>${escapeHTML(String(gameData.playerScore))}</b>. Bot wins <b>${escapeHTML(betDisplayUSDShort)}</b>.`;
        // payoutLamports remains 0n for loss
    } else if (finalStatus === 'game_over_bot_error' || finalStatus === 'game_over_error_ui_update') {
        resultTitle = "⚙️ Game Error";
        resultOutcomeText = `Technical issue. Bet <b>${escapeHTML(betDisplayUSDShort)}</b> refunded.`;
        payoutLamports = betAmount; // Refund original bet
    } else if (finalStatus === 'game_over_player_forfeit') {
        resultTitle = "🚫 Game Forfeited";
        resultOutcomeText = `You forfeited. Bot wins <b>${escapeHTML(betDisplayUSDShort)}</b>.`;
        // payoutLamports remains 0n for loss
    } else if (finalStatus === 'game_over_bot_played' || finalStatus === 'player_blackjack') {
        if (playerBlackjack && (gameData.botScore !== DICE_21_TARGET_SCORE || gameData.botHandRolls.length > 2)) {
            resultTitle = "✨🎉 BLACKJACK!";
            const profitBlackjack = betAmount * 15n / 10n; 
            playerWins = true;
            payoutLamports = betAmount + profitBlackjack; // Total payout is 2.5x bet
            resultOutcomeText = `Natural 21! You win <b>${escapeHTML(await formatBalanceForDisplay(payoutLamports, 'USD', 2))}</b>!`;
        } else if (gameData.botScore > DICE_21_TARGET_SCORE) {
            resultTitle = "🎉 Player Wins!";
            playerWins = true; payoutLamports = betAmount * 2n;
            resultOutcomeText = `Bot BUSTED (<b>${escapeHTML(String(gameData.botScore))}</b>)! You win <b>${escapeHTML(await formatBalanceForDisplay(payoutLamports, 'USD', 2))}</b>!`;
        } else if (gameData.playerScore > gameData.botScore) {
            resultTitle = "🎉 Player Wins!";
            playerWins = true; payoutLamports = betAmount * 2n;
            resultOutcomeText = `Your <b>${escapeHTML(String(gameData.playerScore))}</b> beats Bot's <b>${escapeHTML(String(gameData.botScore))}</b>. You win <b>${escapeHTML(await formatBalanceForDisplay(payoutLamports, 'USD', 2))}</b>!`;
        } else if (gameData.botScore > gameData.playerScore) {
            resultTitle = "🤖 Bot Wins";
            resultOutcomeText = `Bot's <b>${escapeHTML(String(gameData.botScore))}</b> beats your <b>${escapeHTML(String(gameData.playerScore))}</b>. You lost <b>${escapeHTML(betDisplayUSDShort)}</b>.`;
            // payoutLamports remains 0n for loss
        } else { // Push
            resultTitle = "⚖️ Push!";
            resultOutcomeText = `Scores tied at <b>${escapeHTML(String(gameData.playerScore))}</b>. Bet <b>${escapeHTML(betDisplayUSDShort)}</b> returned.`;
            payoutLamports = betAmount; // Refund original bet
        }
    } else { // Unknown status
        resultTitle = "❓ Game Undetermined";
        resultOutcomeText = `Unexpected status: <code>${escapeHTML(String(finalStatus))}</code>. Bet <b>${escapeHTML(betDisplayUSDShort)}</b> refunded.`;
        payoutLamports = betAmount; // Refund original bet
    }

    let dbErrorDuringPayoutText = ""; 

    // This condition ensures we attempt DB update for wins, losses (0 payout), pushes, and refunds.
    if (payoutLamports >= 0n || finalStatus === 'game_over_player_bust' || finalStatus === 'game_over_player_forfeit') {
        let client = null;
        try {
            client = await pool.connect(); await client.query('BEGIN');
            let transactionType = 'loss_dice21_pvb'; // Default
            if (finalStatus === 'game_over_player_bust') transactionType = 'loss_dice21_pvb_player_bust';
            else if (finalStatus === 'game_over_player_forfeit') transactionType = 'loss_dice21_pvb_forfeit';
            else if (playerWins) transactionType = playerBlackjack ? 'win_dice21_pvb_blackjack' : 'win_dice21_pvb';
            else if (payoutLamports === betAmount) transactionType = 'refund_dice21_pvb'; 
            else if (finalStatus === 'game_over_bot_error' || finalStatus === 'game_over_error_ui_update') transactionType = 'refund_dice21_pvb_error';

            const notes = `Dice 21 PvB Result: ${finalStatus}. Payout: ${payoutLamports}. Player Hand: ${gameData.playerHandRolls.join(',')}. Bot Hand: ${gameData.botHandRolls.join(',')}.`;
            
            // Ensure payoutLamports (with 's') is used here
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, gameData.playerId, payoutLamports, transactionType, { game_id_custom_field: gameData.gameId }, notes);

            if (balanceUpdateResult.success) {
                await client.query('COMMIT');
            } else {
                await client.query('ROLLBACK');
                dbErrorDuringPayoutText = `\n\n⚠️ Balance Update Error. Staff notified.`; 
                console.error(`${logPrefix} FAILED to update balance after game. Error: ${balanceUpdateResult.error}`);
                // This is where the original error 'e' for the catch block below would be balanceUpdateResult.error
            }
        } catch (e) { // This 'e' is for DB connection or other errors within the try block
            if (client) await client.query('ROLLBACK').catch(()=>{});
            dbErrorDuringPayoutText = `\n\n🚨 Critical DB Error. Staff notified.`;
            console.error(`${logPrefix} CRITICAL DB error during finalization: ${e.message}`);
            if (typeof notifyAdmin === 'function') {
                // Here, e.message is the actual DB error.
                // payoutLamports (with 's') is from the outer scope and should be defined.
                notifyAdmin(`🚨 D21 PvB Finalize Payout DB Failure 🚨\nGame ID: <code>${escapeHTML(String(gameData.gameId))}</code>\nError: ${escapeHTML(e.message)}. MANUAL BALANCE CHECK/CREDIT REQUIRED for ${payoutLamports} for user ${gameData.playerId}.`, {parse_mode:'HTML'});
            }
        } finally {
            if (client) client.release();
        }
    } else {
      // This else block should ideally not be reached if payoutLamports is always initialized.
      // Added for safety if payoutLamports somehow ended up < 0 without being a loss.
       console.warn(`${logPrefix} payoutLamports was unexpectedly negative and not a loss: ${payoutLamports}. No DB update performed.`);
    }

    const playerRefHTML = escapeHTML(gameData.playerRef);
    let conciseFinalMessageHTML = `<b>${escapeHTML(resultTitle)}</b>\n` +
                                  `You (${playerRefHTML}): <b>${escapeHTML(String(gameData.playerScore))}</b> ${formatDiceRolls(gameData.playerHandRolls)}\n` +
                                  `Bot: <b>${escapeHTML(String(gameData.botScore))}</b> ${formatDiceRolls(gameData.botHandRolls)}\n` +
                                  `${resultOutcomeText}` + 
                                  `${escapeHTML(dbErrorDuringPayoutText)}`;
                                  // Balance display removed

    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_21, gameData.betAmount);
    await safeSendMessage(gameData.chatId, conciseFinalMessageHTML, { parse_mode: 'HTML', reply_markup: finalKeyboard });

    if (messagesToDelete && messagesToDelete.length > 0) {
        for (const msgId of messagesToDelete) {
            if (bot && msgId) {
                await bot.deleteMessage(gameData.chatId, Number(msgId)).catch(e => {
                    if (e.message && !e.message.toLowerCase().includes("message to delete not found") && !e.message.toLowerCase().includes("message can't be deleted")) {
                        // console.warn(...); // Log removed for brevity
                    }
                });
            }
        }
    }
}

// index.js - Part 5b, Section 2 (Dice 21 / Blackjack-style game logic)
// SEGMENT 2 of 2 (PvP Functions MODIFIED FOR HTML DISPLAY, TIMEOUTS, & NO BALANCE IN FINAL MSG)
//-------------------------------------------------------------------------------------------------

// --- Player vs. Player (PvP) Dice 21 Specific Logic (MODIFIED FOR HTML & TIMEOUTS) ---

async function startDice21PvPInitialDeal(gameId) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DICE_21_PVP || gameData.status !== 'dealing_initial_hands') {
        console.warn(`[D21_PvP_InitialDeal GID:${gameId}] Start deal called for invalid game state or type. Status: ${gameData?.status}, Type: ${gameData?.type}.`);
        return;
    }
    const logPrefix = `[D21_PvP_InitialDeal GID:${gameId}_HTML_Timeout]`;
    console.log(`${logPrefix} Starting initial deal for PvP game. Initiator: ${gameData.initiator.mention}, Opponent: ${gameData.opponent.mention}.`);

    const initiatorMentionHTML = escapeHTML(gameData.initiator.mention);
    const opponentMentionHTML = escapeHTML(gameData.opponent.mention);
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    const initialMessageTextHTML = `⚔️ <b>Dice 21 PvP: ${initiatorMentionHTML} vs ${opponentMentionHTML}</b> ⚔️\n` +
                                `Bet: <b>${betDisplayHTML}</b> each.\n\n` +
                                `The Helper Bot is now dealing the initial two dice to each player. Please wait for the reveal! ⏳`;

    const sentInitialMessage = await safeSendMessage(gameData.chatId, initialMessageTextHTML, { parse_mode: 'HTML' });
    if (!sentInitialMessage?.message_id) {
        console.error(`${logPrefix} CRITICAL: Failed to send initial dealing message for PvP game ${gameId}. Game cannot proceed with UI.`);
        activeGames.delete(gameId);
        await updateGroupGameDetails(gameData.chatId, null, null, null);
        await safeSendMessage(gameData.chatId, `A critical display error occurred starting the Dice 21 PvP game. Game abandoned. If bets were placed, please contact support for review with Game ID approx time: ${new Date().toISOString()}`, {parse_mode:'HTML'});
        return;
    }
    gameData.currentMessageId = sentInitialMessage.message_id;
    activeGames.set(gameId, gameData);
    console.log(`${logPrefix} Initial PvP dealing message sent (ID: ${gameData.currentMessageId}).`);

    let initiatorFaulted = false;
    for (let i = 0; i < 2; i++) {
        const currentInitiatorMentionHTML = escapeHTML(gameData.initiator.mention); 
        console.log(`${logPrefix} Dealing Die ${i+1}/2 to Initiator (${currentInitiatorMentionHTML}) via Helper Bot.`);
        const rollResult = await getSingleDiceRollViaHelper(gameId, gameData.chatId, gameData.initiator.userId, `Initiator D21 PvP Die ${i+1}`);
        if (rollResult.error) {
            console.error(`${logPrefix} Helper Bot error for Initiator Die ${i+1}: ${rollResult.message}`);
            initiatorFaulted = true; break;
        }
        gameData.initiator.hand.push(rollResult.roll);
        gameData.initiator.score += rollResult.roll;
        await safeSendMessage(gameData.chatId, `🎲 ${currentInitiatorMentionHTML} received a <b>${escapeHTML(String(rollResult.roll))}</b> (Die ${i+1}/2 from Helper Bot)`, {parse_mode:'HTML'});
        if (i < 1) await sleep(1000);
    }
    activeGames.set(gameData.gameId, gameData);
    if (initiatorFaulted) {
        gameData.status = 'game_over_error_deal_initiator';
        await finalizeDice21PvPGame(gameData); return;
    }
    console.log(`${logPrefix} Initiator's hand dealt: [${gameData.initiator.hand.join(', ')}], Score: ${gameData.initiator.score}.`);
    await sleep(1500);

    let opponentFaulted = false;
    for (let i = 0; i < 2; i++) {
        const currentOpponentMentionHTML = escapeHTML(gameData.opponent.mention);
        console.log(`${logPrefix} Dealing Die ${i+1}/2 to Opponent (${currentOpponentMentionHTML}) via Helper Bot.`);
        const rollResult = await getSingleDiceRollViaHelper(gameId, gameData.chatId, gameData.opponent.userId, `Opponent D21 PvP Die ${i+1}`);
        if (rollResult.error) {
            console.error(`${logPrefix} Helper Bot error for Opponent Die ${i+1}: ${rollResult.message}`);
            opponentFaulted = true; break;
        }
        gameData.opponent.hand.push(rollResult.roll);
        gameData.opponent.score += rollResult.roll;
        await safeSendMessage(gameData.chatId, `🎲 ${currentOpponentMentionHTML} received a <b>${escapeHTML(String(rollResult.roll))}</b> (Die ${i+1}/2 from Helper Bot)`, {parse_mode:'HTML'});
        if (i < 1) await sleep(1000);
    }
    activeGames.set(gameData.gameId, gameData);
    if (opponentFaulted) {
        gameData.status = 'game_over_error_deal_opponent';
        await finalizeDice21PvPGame(gameData); return;
    }
    console.log(`${logPrefix} Opponent's hand dealt: [${gameData.opponent.hand.join(', ')}], Score: ${gameData.opponent.score}.`);
    await sleep(1500);

    const p1Score = gameData.initiator.score; const p2Score = gameData.opponent.score;
    const p1HasBJ = p1Score === DICE_21_TARGET_SCORE && gameData.initiator.hand.length === 2;
    const p2HasBJ = p2Score === DICE_21_TARGET_SCORE && gameData.opponent.hand.length === 2;

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
        gameData.initiator.isTurn = true; gameData.opponent.isTurn = false;
        gameData.initiator.status = 'playing_turn'; gameData.opponent.status = 'waiting_turn';
        gameData.status = 'initiator_turn';
    }
    activeGames.set(gameData.gameId, gameData);
    console.log(`${logPrefix} Initial deal complete. Game status: ${gameData.status}. P1 Score: ${p1Score}, P2 Score: ${p2Score}.`);

    if (gameData.status.startsWith('game_over')) {
        await finalizeDice21PvPGame(gameData);
    } else {
        await updateDice21PvPGameMessage(gameData); 
    }
}

// `updateDice21PvPGameMessage` (Revised for HTML and to initiate turn timeouts)
async function updateDice21PvPGameMessage(gameData, isFinal = false, customMessageContent = null) {
    if (!gameData || !bot) {
        console.warn(`[D21_PvP_UpdateMsg GID:${gameData?.gameId || 'Unknown_GID'}] Update requested but gameData or bot is missing. Cannot update.`);
        return;
    }
    const logPrefix = `[D21_PvP_UpdateMsg GID:${gameData.gameId} Final:${isFinal}_HTML_Timeout_Newline]`;
    const currentMainMessageId = gameData.currentMessageId;

    if (gameData.currentTurnTimeoutId) {
        clearTimeout(gameData.currentTurnTimeoutId);
        gameData.currentTurnTimeoutId = null;
    }

    if (currentMainMessageId && bot) {
        await bot.deleteMessage(gameData.chatId, Number(currentMainMessageId))
            .catch(e => {
                if (!e.message || !e.message.toLowerCase().includes("message to delete not found")) {
                    console.warn(`${logPrefix} Non-critical: Failed to delete message ${currentMainMessageId} before update: ${e.message}`)
                }
            });
        const gd = activeGames.get(gameData.gameId);
        if (gd) gd.currentMessageId = null;
    }

    let messageTextHTML;
    let keyboard = {};
    const p1MentionHTML = escapeHTML(gameData.initiator.mention);
    const p2MentionHTML = escapeHTML(gameData.opponent.mention);
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    if (customMessageContent) {
        messageTextHTML = customMessageContent; 
        if (!isFinal) {
             keyboard = { inline_keyboard: [[{ text: "📖 Game Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` }]] };
        }
    } else {
        const p1 = gameData.initiator; const p2 = gameData.opponent;
        const p1StatusIcon = p1.status === 'stood' ? "✅" : (p1.status === 'bust' ? "💥" : (p1.status === 'blackjack' ? "✨" : ""));
        const p2StatusIcon = p2.status === 'stood' ? "✅" : (p2.status === 'bust' ? "💥" : (p2.status === 'blackjack' ? "✨" : ""));

        messageTextHTML = `⚔️ <b>Dice 21 PvP: ${p1MentionHTML} vs ${p2MentionHTML}</b> ⚔️\n` +
                         `Bet: <b>${betDisplayHTML}</b> each.\n\n` +
                         `Player 1: ${p1MentionHTML}\n` +
                         `Hand: ${formatDiceRolls(p1.hand)} Score: <b>${escapeHTML(String(p1.score))}</b> ${p1StatusIcon}\n\n` +
                         `Player 2: ${p2MentionHTML}\n` +
                         `Hand: ${formatDiceRolls(p2.hand)} Score: <b>${escapeHTML(String(p2.score))}</b> ${p2StatusIcon}\n\n`;

        let currentActionPromptHTML = "";
        const buttonsForKeyboard = [];
        const currentPlayerWhoseTurnItIs = p1.isTurn ? p1 : (p2.isTurn ? p2 : null);
        
        if (gameData.status === 'game_over_pvp') {
            currentActionPromptHTML = "🏁 The game has concluded! The final scores are being tallied...";
        } else if (gameData.status.startsWith('game_over_')) {
             currentActionPromptHTML = "🏁 This intense duel has reached its conclusion! Calculating final results...";
        } else if (gameData.status === 'dealing_initial_hands') {
            currentActionPromptHTML = "⏳ Initial hands are being dealt. Please wait...";
        } else if (currentPlayerWhoseTurnItIs && currentPlayerWhoseTurnItIs.status === 'playing_turn') {
            const currentPlayerMentionHTMLSafe = escapeHTML(currentPlayerWhoseTurnItIs.mention);
            currentActionPromptHTML = `It's YOUR turn to act, ${currentPlayerMentionHTMLSafe}! Send a 🎲 emoji to <b>Hit</b> for another die, or tap the <b>Stand</b> button below to keep your current score of <b>${escapeHTML(String(currentPlayerWhoseTurnItIs.score))}</b>.`;
            buttonsForKeyboard.push([{ text: `✅ Stand (Score: ${currentPlayerWhoseTurnItIs.score})`, callback_data: `d21_pvp_stand:${gameData.gameId}:${currentPlayerWhoseTurnItIs.userId}` }]);
        } else if (currentPlayerWhoseTurnItIs) { 
            const otherPlayerInGame = (currentPlayerWhoseTurnItIs === p1) ? p2 : p1;
            const otherPlayerMentionHTMLSafe = escapeHTML(otherPlayerInGame.mention);
            if (otherPlayerInGame.isTurn && otherPlayerInGame.status === 'playing_turn') {
                 currentActionPromptHTML = `Waiting for ${otherPlayerMentionHTMLSafe} to make their move...`;
            } else if (otherPlayerInGame.status !== 'playing_turn' && currentPlayerWhoseTurnItIs.status !== 'playing_turn') {
                 currentActionPromptHTML = `All players have completed their turns. Calculating the results now!`;
                 if (gameData.status !== 'game_over_pvp' && !gameData.status.startsWith('game_over_')) {
                     gameData.status = 'game_over_pvp';
                 }
            } else {
                 currentActionPromptHTML = `Waiting for ${otherPlayerMentionHTMLSafe}'s turn or game resolution...`;
            }
        } else {
            currentActionPromptHTML = "Determining the next turn or final game resolution...";
        }
        messageTextHTML += currentActionPromptHTML; 

        if (!gameData.status.startsWith('game_over_') && gameData.status !== 'dealing_initial_hands') {
            buttonsForKeyboard.push([{ text: "📖 Game Rules (Dice 21)", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` }]);
        }
        if (buttonsForKeyboard.length > 0) keyboard = { inline_keyboard: buttonsForKeyboard };
    }

    const activePlayerForTimeout = gameData.initiator.isTurn ? gameData.initiator : (gameData.opponent.isTurn ? gameData.opponent : null);
    if (!isFinal && activePlayerForTimeout && activePlayerForTimeout.status === 'playing_turn' &&
        !gameData.status.startsWith('game_over_') && gameData.status !== 'dealing_initial_hands') {
        
        const timedOutPlayerId = activePlayerForTimeout.userId;
        console.log(`${logPrefix} Setting turn timeout for player ${timedOutPlayerId} for ${PVP_TURN_TIMEOUT_MS}ms.`);
        gameData.currentTurnTimeoutId = setTimeout(async () => {
            const currentGDataOnTimeout = activeGames.get(gameData.gameId); // Re-fetch to ensure latest state
            if (currentGDataOnTimeout && 
                !currentGDataOnTimeout.status.startsWith('game_over_') && // Check if game is still active
                ((currentGDataOnTimeout.initiator.isTurn && currentGDataOnTimeout.initiator.userId === timedOutPlayerId && currentGDataOnTimeout.initiator.status === 'playing_turn') ||
                 (currentGDataOnTimeout.opponent.isTurn && currentGDataOnTimeout.opponent.userId === timedOutPlayerId && currentGDataOnTimeout.opponent.status === 'playing_turn'))) {
                
                console.log(`[D21_PvP_Timeout GID:${gameData.gameId}] Player ${timedOutPlayerId} timed out.`);
                if (typeof handleDice21PvPTurnTimeout === 'function') {
                    await handleDice21PvPTurnTimeout(gameData.gameId, timedOutPlayerId);
                } else {
                    console.error(`[D21_PvP_Timeout GID:${gameData.gameId}] CRITICAL: handleDice21PvPTurnTimeout function not defined!`);
                }
            } else {
                console.log(`[D21_PvP_Timeout GID:${gameData.gameId}] Timeout for player ${timedOutPlayerId} fired, but game state changed, player already acted, or not their turn. Current status: ${currentGDataOnTimeout?.status}. Ignoring.`);
            }
        }, PVP_TURN_TIMEOUT_MS); 
    }

    const sentNewMainMessage = await safeSendMessage(gameData.chatId, messageTextHTML, {
        parse_mode: 'HTML',
        reply_markup: Object.keys(keyboard).length > 0 && keyboard.inline_keyboard?.length > 0 ? keyboard : {}
    });

    if (sentNewMainMessage?.message_id) {
        const gd = activeGames.get(gameData.gameId); 
        if(gd) {
            gd.currentMessageId = sentNewMainMessage.message_id;
            activeGames.set(gameData.gameId, gd); 
        }
    } else {
        console.error(`${logPrefix} CRITICAL: Failed to send new PvP main game message for GID ${gameData.gameId}.`);
        await safeSendMessage(gameData.chatId,
            `🚨 A critical display error occurred in your PvP Dice 21 game (<code>${escapeHTML(gameData.gameId.slice(-6))}</code>). Game cancelled. Bets refunded.`,
            {parse_mode: 'HTML'}
        );
        const gdOnError = activeGames.get(gameData.gameId);
        if(gdOnError) {
            gdOnError.status = 'game_over_error_ui_update';
            if (!isFinal) await finalizeDice21PvPGame(gdOnError);
        }
    }
}

async function processDice21PvPRollByEmoji(gameData, diceValueRolled, userIdWhoRolled) {
    const logPrefix = `[D21_PvP_Hit GID:${gameData.gameId} UID:${userIdWhoRolled} Rev5.1_HTML_Timeout_Newline]`;
    let currentPlayer, otherPlayer, playerKey;

    if (gameData.initiator.userId === userIdWhoRolled && gameData.initiator.isTurn) {
        currentPlayer = gameData.initiator; otherPlayer = gameData.opponent; playerKey = 'initiator';
    } else if (gameData.opponent.userId === userIdWhoRolled && gameData.opponent.isTurn) {
        currentPlayer = gameData.opponent; otherPlayer = gameData.initiator; playerKey = 'opponent';
    } else {
        console.warn(`${logPrefix} Roll received from ${userIdWhoRolled}, but it's not their turn or they are not in this game. Ignoring roll: ${diceValueRolled}.`);
        return;
    }

    if (gameData.currentTurnTimeoutId) {
        clearTimeout(gameData.currentTurnTimeoutId);
        gameData.currentTurnTimeoutId = null;
    }

    if (gameData.status !== `${playerKey}_turn` || currentPlayer.status !== 'playing_turn') {
        console.warn(`${logPrefix} Player ${escapeHTML(currentPlayer.mention)} attempted to hit, but game status is '${gameData.status}' or player status is '${currentPlayer.status}'. Ignoring roll: ${diceValueRolled}.`);
        return;
    }
    if (currentPlayer.hand.length < 2 && currentPlayer.status === 'playing_turn') {
        console.warn(`${logPrefix} Player ${escapeHTML(currentPlayer.mention)} attempted to hit before their initial PvP hand was fully dealt. Ignoring roll: ${diceValueRolled}.`);
        return;
    }

    console.log(`${logPrefix} Player ${escapeHTML(currentPlayer.mention)} hits. Helper Bot value: ${diceValueRolled}. Current score: ${currentPlayer.score}.`);
    currentPlayer.hand.push(diceValueRolled);
    currentPlayer.score += diceValueRolled;
    gameData.lastInteractionTime = Date.now();

    const currentPlayerMentionHTML = escapeHTML(currentPlayer.mention);
    const hitAnnouncementTextHTML = `🎲 ${currentPlayerMentionHTML} (PvP) hits and the Helper Bot deals a <b>${escapeHTML(String(diceValueRolled))}</b>!\nTheir new score is now <b>${escapeHTML(String(currentPlayer.score))}</b>.`;
    await safeSendMessage(gameData.chatId, hitAnnouncementTextHTML, {parse_mode:'HTML'});
    console.log(`${logPrefix} Sent HTML hit announcement for ${currentPlayerMentionHTML}.`);
    await sleep(1000);

    if (currentPlayer.score > DICE_21_TARGET_SCORE) {
        currentPlayer.status = 'bust';
        currentPlayer.isTurn = false;
        gameData.status = playerKey === 'initiator' ? 'game_over_initiator_bust_during_turn' : 'game_over_opponent_bust_during_turn';
        activeGames.set(gameData.gameId, gameData);
        await finalizeDice21PvPGame(gameData);
    } else if (currentPlayer.score === DICE_21_TARGET_SCORE) {
        currentPlayer.status = 'stood';
        currentPlayer.isTurn = false;
        if (playerKey === 'initiator') {
            if (otherPlayer.status === 'stood' || otherPlayer.status === 'bust' || otherPlayer.status === 'blackjack' || otherPlayer.status === 'lost_to_blackjack') {
                gameData.status = 'game_over_both_played_final';
            } else {
                otherPlayer.isTurn = true; otherPlayer.status = 'playing_turn';
                gameData.status = 'opponent_turn';
            }
        } else { 
            gameData.status = 'game_over_both_played_final';
        }
        activeGames.set(gameData.gameId, gameData);
        if (gameData.status.startsWith('game_over')) {
            await finalizeDice21PvPGame(gameData);
        } else {
            await updateDice21PvPGameMessage(gameData); 
        }
    } else { 
        gameData.status = `${playerKey}_turn`; 
        activeGames.set(gameData.gameId, gameData);
        await updateDice21PvPGameMessage(gameData); 
    }
}

async function handleDice21PvPStand(gameId, userIdWhoStood, originalMessageId, callbackQueryId, chatData) {
    const gameData = activeGames.get(gameId);
    const logPrefix = `[D21_Stand_PvP GID:${gameId} UID:${userIdWhoStood} Rev5.1_HTML_Timeout]`;

    if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId).catch(() => {});

    if (!gameData || gameData.type !== GAME_IDS.DICE_21_PVP) {
         console.warn(`${logPrefix} Stand called for invalid or non-existent game.`); return;
    }
    if (gameData.currentTurnTimeoutId) {
        clearTimeout(gameData.currentTurnTimeoutId);
        gameData.currentTurnTimeoutId = null;
    }

    if (gameData.status.startsWith('game_over_')) {
        return;
    }
    let playerStanding, otherPlayer, playerKeyStanding;
    if (gameData.initiator.userId === userIdWhoStood && gameData.initiator.isTurn) {
        playerStanding = gameData.initiator; otherPlayer = gameData.opponent; playerKeyStanding = 'initiator';
    } else if (gameData.opponent.userId === userIdWhoStood && gameData.opponent.isTurn) {
        playerStanding = gameData.opponent; otherPlayer = gameData.initiator; playerKeyStanding = 'opponent';
    } else {
        console.warn(`${logPrefix} Stand called by ${userIdWhoStood}, but not their turn or not in game.`);
        return;
    }

    if (playerStanding.status !== 'playing_turn') {
         console.warn(`${logPrefix} Player ${escapeHTML(playerStanding.mention)} attempted to stand, but not their active turn or status is ${playerStanding.status}.`);
         return;
    }
    if (playerStanding.hand.length < 2) {
        console.warn(`${logPrefix} Player ${escapeHTML(playerStanding.mention)} attempted to stand with less than 2 dice.`);
        return;
    }

    console.log(`${logPrefix} Player ${escapeHTML(playerStanding.mention)} stands with score ${playerStanding.score}.`);
    playerStanding.status = 'stood';
    playerStanding.isTurn = false;
    gameData.lastInteractionTime = Date.now();

    const playerStandingMentionHTML = escapeHTML(playerStanding.mention);
    const standAnnouncementTextHTML = `✋ ${playerStandingMentionHTML} stands tall with a score of <b>${escapeHTML(String(playerStanding.score))}</b>!`;
    await safeSendMessage(gameData.chatId, standAnnouncementTextHTML, {parse_mode: 'HTML'});
    await sleep(1000);

    if (playerKeyStanding === 'initiator') {
        if (otherPlayer.status === 'bust' || otherPlayer.status === 'stood' || otherPlayer.status === 'blackjack' || otherPlayer.status === 'lost_to_blackjack') {
            gameData.status = 'game_over_both_played_final';
        } else { 
            otherPlayer.isTurn = true; otherPlayer.status = 'playing_turn';
            gameData.status = 'opponent_turn';
        }
    } else { 
        gameData.status = 'game_over_both_played_final';
    }

    activeGames.set(gameData.gameId, gameData);

    if (gameData.status.startsWith('game_over')) {
        console.log(`${logPrefix} Game is over (status: ${gameData.status}) after ${playerStandingMentionHTML} stood. Finalizing.`);
        await finalizeDice21PvPGame(gameData);
    } else {
        console.log(`${logPrefix} After ${playerStandingMentionHTML} stood, it's now ${escapeHTML(otherPlayer.mention)}'s turn (status: ${gameData.status}). Updating game message.`);
        await updateDice21PvPGameMessage(gameData); 
    }
}

async function finalizeDice21PvPGame(gameData) {
    const logPrefix = `[D21_PvP_Finalize GID:${gameData.gameId} HTML_Winnings_NoBal]`; // Updated log prefix

    if (!gameData) {
        console.error(`${logPrefix} Finalize called but gameData is missing. Aborting.`);
        return;
    }

    if (gameData.currentTurnTimeoutId) {
        clearTimeout(gameData.currentTurnTimeoutId);
        gameData.currentTurnTimeoutId = null;
    }

    const finalStatus = gameData.status;
    const p1 = gameData.initiator;
    const p2 = gameData.opponent;
    const p1MentionHTML = escapeHTML(p1.mention);
    const p2MentionHTML = escapeHTML(p2.mention);

    // console.log(`${logPrefix} Finalizing PvP game. P1(${p1MentionHTML}): Score ${p1.score} (Status: ${p1.status}). P2(${p2MentionHTML}): Score ${p2.score} (Status: ${p2.status}). Game Status: ${finalStatus}`); // Log removed
    activeGames.delete(gameData.gameId);
    await updateGroupGameDetails(gameData.chatId, null, null, null);

    let titleEmoji = "🏁";
    let resultTextHTML = "";
    let p1_payout = 0n; let p2_payout = 0n;
    let winnerObj = null; // To store the winner object if there is one
    const target = DICE_21_TARGET_SCORE;
    const betAmount = gameData.betAmount;
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD')); // For push/loss messages

    if (finalStatus === 'game_over_error_deal_initiator' || finalStatus === 'game_over_error_deal_opponent' || finalStatus === 'game_over_error_ui_update' || finalStatus === 'game_over_error_helper_bot' || finalStatus === 'game_over_error_timeout_logic') {
        titleEmoji = "⚙️";
        resultTextHTML = `A technical error occurred. All bets (<b>${betDisplayHTML}</b> each) refunded.`;
        p1_payout = betAmount; p2_payout = betAmount;
    } else if (finalStatus === 'game_over_initiator_timeout_forfeit') {
        titleEmoji = "⏳🏆"; winnerObj = p2;
        resultTextHTML = `${p1MentionHTML} timed out! ${p2MentionHTML} wins <b>${escapeHTML(await formatBalanceForDisplay(betAmount * 2n, 'USD'))}</b> by default!`;
        p2_payout = gameData.betAmount * 2n;
    } else if (finalStatus === 'game_over_opponent_timeout_forfeit') {
        titleEmoji = "⏳🏆"; winnerObj = p1;
        resultTextHTML = `${p2MentionHTML} timed out! ${p1MentionHTML} wins <b>${escapeHTML(await formatBalanceForDisplay(betAmount * 2n, 'USD'))}</b> by default!`;
        p1_payout = gameData.betAmount * 2n;
    } else if (finalStatus === 'game_over_push_both_blackjack') {
        titleEmoji = "✨⚖️✨";
        resultTextHTML = `DOUBLE BLACKJACK! Both ${p1MentionHTML} & ${p2MentionHTML} hit <b>${target}</b>! It's a PUSH. Bets (<b>${betDisplayHTML}</b> each) returned.`;
        p1_payout = betAmount; p2_payout = betAmount;
    } else if (finalStatus === 'game_over_initiator_blackjack') {
        titleEmoji = "✨🏆"; winnerObj = p1;
        const blackjackProfitLamports = betAmount * 15n / 10n;
        p1_payout = betAmount + blackjackProfitLamports; // Total 2.5x
        resultTextHTML = `${p1MentionHTML} hits a natural BLACKJACK! ${p1MentionHTML} wins <b>${escapeHTML(await formatBalanceForDisplay(p1_payout, 'USD'))}</b>!`;
    } else if (finalStatus === 'game_over_opponent_blackjack') {
        titleEmoji = "✨🏆"; winnerObj = p2;
        const blackjackProfitLamports = betAmount * 15n / 10n;
        p2_payout = betAmount + blackjackProfitLamports; // Total 2.5x
        resultTextHTML = `${p2MentionHTML} hits a natural BLACKJACK! ${p2MentionHTML} wins <b>${escapeHTML(await formatBalanceForDisplay(p2_payout, 'USD'))}</b>!`;
    } else if (finalStatus === 'game_over_initiator_bust_during_turn' || p1.status === 'bust') {
        titleEmoji = "💥🏆"; winnerObj = p2;
        p2_payout = betAmount * 2n;
        resultTextHTML = `${p1MentionHTML} BUSTED with <b>${escapeHTML(String(p1.score))}</b>! ${p2MentionHTML} wins <b>${escapeHTML(await formatBalanceForDisplay(p2_payout, 'USD'))}</b>!`;
    } else if (finalStatus === 'game_over_opponent_bust_during_turn' || p2.status === 'bust') {
        titleEmoji = "💥🏆"; winnerObj = p1;
        p1_payout = betAmount * 2n;
        resultTextHTML = `${p2MentionHTML} BUSTED with <b>${escapeHTML(String(p2.score))}</b>! ${p1MentionHTML} wins <b>${escapeHTML(await formatBalanceForDisplay(p1_payout, 'USD'))}</b>!`;
    } else {
        const p1_finalScore = p1.score;
        const p2_finalScore = p2.score;

        if (p1_finalScore > p2_finalScore) {
            titleEmoji = "🏆"; winnerObj = p1; p1_payout = betAmount * 2n;
            resultTextHTML = `${p1MentionHTML} WINS with <b>${escapeHTML(String(p1.score))}</b> vs ${p2MentionHTML}'s <b>${escapeHTML(String(p2.score))}</b>! Wins <b>${escapeHTML(await formatBalanceForDisplay(p1_payout, 'USD'))}</b>!`;
        } else if (p2_finalScore > p1_finalScore) {
            titleEmoji = "🏆"; winnerObj = p2; p2_payout = betAmount * 2n;
            resultTextHTML = `${p2MentionHTML} WINS with <b>${escapeHTML(String(p2.score))}</b> vs ${p1MentionHTML}'s <b>${escapeHTML(String(p1.score))}</b>! Wins <b>${escapeHTML(await formatBalanceForDisplay(p2_payout, 'USD'))}</b>!`;
        } else {
            titleEmoji = "⚖️";
            resultTextHTML = `PUSH! Both players tied with <b>${escapeHTML(String(p1.score))}</b>! Bets (<b>${betDisplayHTML}</b> each) returned.`;
            p1_payout = betAmount; p2_payout = betAmount;
        }
    }

    let dbErrorTextForUserHTML = "";
    let criticalDbErrorForAdmin = false;
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const determineLedgerType = (payout, bet, isPushOrError, isBlackjackWin = false, isWinByForfeit = false) => {
            if (isWinByForfeit) return 'win_dice21_pvp_forfeit';
            if (isPushOrError) return 'refund_dice21_pvp'; // Generic refund for push or error
            if (isBlackjackWin) return 'win_dice21_pvp_blackjack';
            return payout > bet ? 'win_dice21_pvp' : (payout === 0n ? 'loss_dice21_pvp' : 'unknown_dice21_pvp_outcome');
        };

        const p1_is_push_or_error = (finalStatus.includes('_error_') || finalStatus.includes('_push_') || (p1.score === p2.score && p1.status !== 'bust' && p2.status !== 'bust' && !finalStatus.includes('timeout_forfeit') && !finalStatus.includes('blackjack')));
        const p2_is_push_or_error = p1_is_push_or_error;
        const p1_is_bj_win = (finalStatus === 'game_over_initiator_blackjack');
        const p2_is_bj_win = (finalStatus === 'game_over_opponent_blackjack');
        const p1_won_by_forfeit = (finalStatus === 'game_over_opponent_timeout_forfeit');
        const p2_won_by_forfeit = (finalStatus === 'game_over_initiator_timeout_forfeit');

        const p1Update = await updateUserBalanceAndLedger(client, p1.userId, p1_payout, determineLedgerType(p1_payout, betAmount, p1_is_push_or_error, p1_is_bj_win, p1_won_by_forfeit), {game_id_custom_field: gameData.gameId, opponent_id: p2.userId, player_score: p1.score, opponent_score: p2.score}, `Dice 21 PvP result vs ${p2.mention}`);
        if (!p1Update.success) throw new Error(`P1 (${p1MentionHTML}) balance update failed: ${p1Update.error}`);

        const p2Update = await updateUserBalanceAndLedger(client, p2.userId, p2_payout, determineLedgerType(p2_payout, betAmount, p2_is_push_or_error, p2_is_bj_win, p2_won_by_forfeit), {game_id_custom_field: gameData.gameId, opponent_id: p1.userId, player_score: p2.score, opponent_score: p1.score}, `Dice 21 PvP result vs ${p1.mention}`);
        if (!p2Update.success) throw new Error(`P2 (${p2MentionHTML}) balance update failed: ${p2Update.error}`);

        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        criticalDbErrorForAdmin = true;
        dbErrorTextForUserHTML = `\n\n⚠️ <b>Critical Balance Update Error:</b> A server issue prevented balances from updating correctly (<code>${escapeHTML(e.message || "DB Error")}</code>). Please contact support with Game ID: <code>${escapeHTML(String(gameData.gameId))}</code>`;
        console.error(`${logPrefix} CRITICAL DB error finalizing PvP Dice 21 ${gameData.gameId}: ${e.message}`);
    } finally {
        if (client) client.release();
    }

    if (criticalDbErrorForAdmin && typeof notifyAdmin === 'function') {
        notifyAdmin(`🚨 D21 PvP Finalize Payout DB Failure 🚨\nGame ID: <code>${escapeHTML(String(gameData.gameId))}</code>\nError: (Check console logs for full error). MANUAL BALANCE CHECK/CREDIT REQUIRED for players.`, {parse_mode:'HTML'});
    }

    const p1StatusIconDisplay = p1.status === 'bust' ? "💥 (Busted)" : (p1.status === 'blackjack' ? "✨ (Blackjack!)" : (p1.status === 'stood' ? `(Stood at ${escapeHTML(String(p1.score))})` : (p1.status === 'timeout_forfeit' ? '⏳ (Timed Out)' : `(Score: ${escapeHTML(String(p1.score))})`)));
    const p2StatusIconDisplay = p2.status === 'bust' ? "💥 (Busted)" : (p2.status === 'blackjack' ? "✨ (Blackjack!)" : (p2.status === 'stood' ? `(Stood at ${escapeHTML(String(p2.score))})` : (p2.status === 'timeout_forfeit' ? '⏳ (Timed Out)' : `(Score: ${escapeHTML(String(p2.score))})`)));

    const fullResultMessageHTML =
        `${titleEmoji} <b>Dice 21 PvP - Game Over!</b> ${titleEmoji}\n\n` +
        `Player 1: ${p1MentionHTML} - Score: <b>${escapeHTML(String(p1.score))}</b> ${formatDiceRolls(p1.hand)} ${p1StatusIconDisplay}\n` +
        `Player 2: ${p2MentionHTML} - Score: <b>${escapeHTML(String(p2.score))}</b> ${formatDiceRolls(p2.hand)} ${p2StatusIconDisplay}\n\n` +
        `------------------------------------\n${resultTextHTML}` +
        `${dbErrorTextForUserHTML}`;
        // Balance display lines removed

    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_21_PVP, gameData.betAmount);
    // The updateDice21PvPGameMessage will handle sending/editing the final message.
    // It also handles deleting the previous game message.
    await updateDice21PvPGameMessage(gameData, true, fullResultMessageHTML);
    // console.log(`${logPrefix} PvP finalization complete for game ${gameData.gameId}.`); // Log removed
}

// New function: handleDice21PvPTurnTimeout
async function handleDice21PvPTurnTimeout(gameId, timedOutPlayerId) {
    const logPrefix = `[D21_PvP_TimeoutHdlr GID:${gameId} TimedOutUID:${timedOutPlayerId}]`;
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.DICE_21_PVP || gameData.status.startsWith('game_over_')) {
        console.log(`${logPrefix} Game already ended, not found, or invalid type. Timeout action aborted.`);
        if (gameData && gameData.currentTurnTimeoutId) { // Check if gameData exists before accessing property
            clearTimeout(gameData.currentTurnTimeoutId);
            gameData.currentTurnTimeoutId = null;
        }
        return;
    }

    const activePlayer = gameData.initiator.isTurn ? gameData.initiator : (gameData.opponent.isTurn ? gameData.opponent : null);
    if (!activePlayer || activePlayer.userId !== timedOutPlayerId) {
        console.log(`${logPrefix} Timeout fired for player ${timedOutPlayerId}, but it's not their active turn or player mismatch. Current active: ${activePlayer?.userId}. Status: ${gameData.status}. Ignoring.`);
        // Do not clear gameData.currentTurnTimeoutId here as it might belong to the other player's active timeout
        return; 
    }

    console.log(`${logPrefix} Processing turn timeout for player ${timedOutPlayerId}.`);

    if (gameData.currentTurnTimeoutId) { // This specific timeout should be cleared
        clearTimeout(gameData.currentTurnTimeoutId);
        gameData.currentTurnTimeoutId = null;
    }
    
    let timedOutPlayerMentionHTML, opponentMentionHTML;

    if (gameData.initiator.userId === timedOutPlayerId) {
        gameData.initiator.status = 'timeout_forfeit'; 
        gameData.opponent.status = 'win_by_forfeit';
        gameData.status = 'game_over_initiator_timeout_forfeit';
        timedOutPlayerMentionHTML = escapeHTML(gameData.initiator.mention);
        opponentMentionHTML = escapeHTML(gameData.opponent.mention);
    } else if (gameData.opponent.userId === timedOutPlayerId) {
        gameData.opponent.status = 'timeout_forfeit';
        gameData.initiator.status = 'win_by_forfeit';
        gameData.status = 'game_over_opponent_timeout_forfeit';
        timedOutPlayerMentionHTML = escapeHTML(gameData.opponent.mention);
        opponentMentionHTML = escapeHTML(gameData.initiator.mention);
    } else {
        console.error(`${logPrefix} Timed out player ID ${timedOutPlayerId} does not match active players. Aborting.`);
        gameData.status = 'game_over_error_timeout_logic'; 
        activeGames.set(gameId, gameData);
        await finalizeDice21PvPGame(gameData); 
        return;
    }
    gameData.initiator.isTurn = false; 
    gameData.opponent.isTurn = false;

    activeGames.set(gameId, gameData);

    if (gameData.currentMessageId && bot) { 
        await bot.deleteMessage(gameData.chatId, gameData.currentMessageId).catch(()=>{});
        gameData.currentMessageId = null; 
    }
    
    const timeoutMessageToChatHTML = `⏳ Player ${timedOutPlayerMentionHTML} ran out of time to make a move!<br>${opponentMentionHTML} wins by default.`;
    const sentMsg = await safeSendMessage(gameData.chatId, timeoutMessageToChatHTML, { parse_mode: 'HTML' });
    
    if(sentMsg?.message_id) { 
        const gd = activeGames.get(gameId); 
        if(gd) gd.currentMessageId = sentMsg.message_id; 
    }
    
    await sleep(1000); 
    await finalizeDice21PvPGame(gameData); 
}
// --- end of 5b section 2 ---
// --- Start of Part 5c, Section 1 (FULLY UPDATED FOR HELPER BOT DICE ROLLS & HTML MESSAGING + DEBUG LOGS) ---
// index.js - Part 5c, Section 1: Over/Under 7 Game Logic & Handlers
// (This entire block is placed after Original Part 5b, Section 2 in the new order)
//-------------------------------------------------------------------------------------------------
// Assumed dependencies from previous Parts (ensure escapeHTML is defined elsewhere)

// --- Over/Under 7 Game Logic ---

async function handleStartOverUnder7Command(msg, betAmountLamports) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_OU7_START = `[OU7_Start_DEBUG UID:${userId} CH:${chatId}_HTML]`;
    console.log(`${LOG_PREFIX_OU7_START} Entered function. Bet amount received: ${betAmountLamports}`);

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_OU7_START} Invalid betAmountLamports: ${betAmountLamports}. Sending error message.`);
        await safeSendMessage(chatId, "🎲 Oops! There was an issue with the bet amount for Over/Under 7. Please try starting the game again with a valid bet.", { parse_mode: 'HTML' });
        return;
    }

    console.log(`${LOG_PREFIX_OU7_START} Bet amount is valid. Fetching/creating user.`);
    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        console.warn(`${LOG_PREFIX_OU7_START} Failed to get/create user. Sending error message.`);
        await safeSendMessage(chatId, "😕 Apologies! We couldn't fetch your player profile to start Over/Under 7. Please try <code>/start</code> again.", { parse_mode: 'HTML' });
        return;
    }
    console.log(`${LOG_PREFIX_OU7_START} User object obtained/created: ${userObj.username || userId}. Current balance: ${userObj.balance}`);

    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObj));
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));
    console.log(`${LOG_PREFIX_OU7_START} Player ref: ${playerRefHTML}, Bet display USD: ${betDisplayUSD_HTML}`);

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplayHTML = escapeHTML(await formatBalanceForDisplay(needed, 'USD'));
        console.warn(`${LOG_PREFIX_OU7_START} Insufficient balance. Needs ${neededDisplayHTML} more. Sending message.`);
        await safeSendMessage(chatId, `${playerRefHTML}, your casino funds are a bit shy for an Over/Under 7 game at <b>${betDisplayUSD_HTML}</b>. You'd need approximately <b>${neededDisplayHTML}</b> more. Care to top up?`, {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }
    console.log(`${LOG_PREFIX_OU7_START} User has sufficient balance.`);

    const gameId = generateGameId(GAME_IDS.OVER_UNDER_7);
    console.log(`${LOG_PREFIX_OU7_START} Generated gameId: ${gameId}. Attempting to place bet and update ledger.`);
    let client = null;
    try {
        client = await pool.connect();
        console.log(`${LOG_PREFIX_OU7_START} DB client connected. Beginning transaction.`);
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_ou7', { game_id_custom_field: gameId },
            `Bet for Over/Under 7 game ${gameId}`
        );
        console.log(`${LOG_PREFIX_OU7_START} updateUserBalanceAndLedger result: ${stringifyWithBigInt(balanceUpdateResult)}`);

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_OU7_START} Wager placement failed: ${balanceUpdateResult?.error || 'Unknown error'}. Transaction rolled back.`);
            await safeSendMessage(chatId, `${playerRefHTML}, your Over/Under 7 wager of <b>${betDisplayUSD_HTML}</b> couldn't be placed due to a hiccup: <code>${escapeHTML(balanceUpdateResult?.error || "Wallet error")}</code>. Please try again.`, { parse_mode: 'HTML' });
            return;
        }
        await client.query('COMMIT');
        console.log(`${LOG_PREFIX_OU7_START} Bet placed and transaction committed. New balance: ${balanceUpdateResult.newBalanceLamports}`);
        userObj.balance = balanceUpdateResult.newBalanceLamports;
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_OU7_START} DB Rollback Error during bet placement: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_OU7_START} Database error during Over/Under 7 bet placement: ${dbError.message}`, dbError.stack?.substring(0, 500));
        await safeSendMessage(chatId, "⚙️ A database disturbance prevented the start of your Over/Under 7 game. Please try again in a moment.", { parse_mode: 'HTML' });
        return;
    } finally {
        if (client) {
            client.release();
            console.log(`${LOG_PREFIX_OU7_START} DB client released.`);
        }
    }

    const gameData = {
        type: GAME_IDS.OVER_UNDER_7, gameId, chatId, userId,
        playerRef: playerRefHTML,
        userObj,
        betAmount: betAmountLamports, playerChoice: null, diceRolls: [], diceSum: null,
        status: 'waiting_player_choice', gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);
    console.log(`${LOG_PREFIX_OU7_START} Game data created and stored in activeGames. Status: ${gameData.status}`);

    const titleHTML = `🎲 <b>Over/Under 7 Showdown</b> 🎲`;
    const initialMessageTextHTML = `${titleHTML}\n\n${playerRefHTML}, you've courageously wagered <b>${betDisplayUSD_HTML}</b>. The dice are polished and ready for action!\n\nPredict the total sum of <b>${escapeHTML(String(OU7_DICE_COUNT))} dice</b>: Will it be Under 7, Exactly 7, or Over 7? Make your fateful choice below! 👇`;
    const keyboard = {
        inline_keyboard: [
            [{ text: "📉 Under 7 (Sum 2-6)", callback_data: `ou7_choice:${gameId}:under` }],
            [{ text: "🎯 Exactly 7 (BIG PAYOUT!)", callback_data: `ou7_choice:${gameId}:seven` }],
            [{ text: "📈 Over 7 (Sum 8-12)", callback_data: `ou7_choice:${gameId}:over` }],
            [{ text: `📖 Game Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.OVER_UNDER_7}` }, { text: '💳 Wallet', callback_data: 'menu:wallet' }]
        ]
    };
    console.log(`${LOG_PREFIX_OU7_START} Sending initial game message with choices.`);
    const sentMessage = await safeSendMessage(chatId, initialMessageTextHTML, { parse_mode: 'HTML', reply_markup: keyboard });

    if (sentMessage?.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
        console.log(`${LOG_PREFIX_OU7_START} Initial game message sent successfully. Message ID: ${gameData.gameMessageId}`);
    } else {
        console.error(`${LOG_PREFIX_OU7_START} Failed to send Over/Under 7 game message for ${gameId}. Attempting to refund wager.`);
        let refundClient = null;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_ou7_setup_fail', { game_id_custom_field: gameId }, `Refund OU7 game ${gameId} (message send fail)`);
            await refundClient.query('COMMIT');
            console.log(`${LOG_PREFIX_OU7_START} Refund processed due to message send failure.`);
        } catch (err) {
            if (refundClient) await refundClient.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_OU7_START} Rollback error on OU7 setup refund: ${rbErr.message}`));
            console.error(`${LOG_PREFIX_OU7_START} CRITICAL: Failed to refund user for OU7 setup fail ${gameId}: ${err.message}`);
            if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL OU7 REFUND FAILURE 🚨\nGame ID: <code>${escapeHTML(gameId)}</code> User: ${playerRefHTML} (<code>${escapeHTML(userId)}</code>)\nReason: Failed to send game message AND failed to refund. Manual intervention required.`, { parse_mode: 'HTML' });
        } finally {
            if (refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
    }
    console.log(`${LOG_PREFIX_OU7_START} Exiting function.`);
}

async function handleOverUnder7Choice(gameId, choice, userObj, originalMessageIdFromCallback, callbackQueryId, msgContext) {
    const userId = String(userObj.telegram_id);
    // Most console.logs removed for production clarity

    const gameData = activeGames.get(gameId);

    if (!gameData) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Over/Under 7 game action is outdated or not yours.", show_alert: true });
        return;
    }

    if (gameData.userId !== userId || gameData.status !== 'waiting_player_choice' || (gameData.gameMessageId && Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback))) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "⏳ This Over/Under 7 game action is outdated or not yours.", show_alert: true });
        if (originalMessageIdFromCallback && bot && gameData && gameData.chatId && (!gameData.gameMessageId || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback))) {
            bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(() => {});
        }
        return;
    }

    const choiceTextDisplay = choice.charAt(0).toUpperCase() + choice.slice(1);
    await bot.answerCallbackQuery(callbackQueryId, { text: `🎯 Locked In: ${choiceTextDisplay} 7! Requesting dice...` }).catch(() => {});

    gameData.playerChoice = choice;
    gameData.status = 'rolling_dice_waiting_helper';
    activeGames.set(gameId, gameData);

    const { chatId, playerRef, betAmount } = gameData;
    const playerRefHTML = escapeHTML(playerRef); // playerRefHTML is the display name
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'));

    const titleRollingHTML = `🎲 <b>Over/Under 7 - Dice Rolling via Helper!</b> 🎲`;
    let rollingMessageTextHTML = `${titleRollingHTML}\n\n${playerRefHTML} bets <b>${betDisplayUSD_HTML}</b> on the sum being <b>${escapeHTML(choiceTextDisplay)} 7</b>.\nThe Helper Bot is now rolling the dice on the casino network... This may take a moment! 🤞`;

    let currentMessageId = gameData.gameMessageId;
    if (currentMessageId && bot) {
        try {
            await bot.editMessageText(rollingMessageTextHTML, { chat_id: String(chatId), message_id: Number(currentMessageId), parse_mode: 'HTML', reply_markup: {} });
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                const newMsg = await safeSendMessage(String(chatId), rollingMessageTextHTML, { parse_mode: 'HTML' });
                if (newMsg?.message_id && activeGames.has(gameId)) {
                    activeGames.get(gameId).gameMessageId = newMsg.message_id;
                    currentMessageId = newMsg.message_id;
                }
            }
        }
    } else {
        const newMsg = await safeSendMessage(String(chatId), rollingMessageTextHTML, { parse_mode: 'HTML' });
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
        const rollResult = await getSingleDiceRollViaHelper(gameId, chatId, userId, `OU7 Roll ${i + 1}`);
        if (rollResult.error) {
            helperBotError = rollResult.message || `Failed to get OU7 Roll ${i + 1}`;
            break;
        }
        if (typeof rollResult.roll !== 'number' || rollResult.roll < 1 || rollResult.roll > 6) {
            helperBotError = `Invalid roll value from helper for OU7 roll ${i + 1}: ${rollResult.roll}`;
            break;
        }
        diceRolls.push(rollResult.roll);
        diceSum += rollResult.roll;
    }

    const messageIdToDeleteBeforeFinalResult = currentMessageId;

    if (helperBotError || diceRolls.length !== OU7_DICE_COUNT) {
        const errorMsgToUserHTML = `⚠️ ${playerRefHTML}, there was an issue rolling the dice via the Helper Bot for your Over/Under 7 game: <code>${escapeHTML(String(helperBotError || "Incomplete rolls from helper").substring(0, 150))}</code>\nYour bet of <b>${betDisplayUSD_HTML}</b> has been refunded.`;
        const errorKeyboard = createPostGameKeyboard(GAME_IDS.OVER_UNDER_7, betAmount);

        if (messageIdToDeleteBeforeFinalResult && bot) {
            await bot.deleteMessage(String(chatId), Number(messageIdToDeleteBeforeFinalResult)).catch(e => console.warn(`[OU7_Choice_Cleanup] Failed to delete message ${messageIdToDeleteBeforeFinalResult} before sending error result: ${e.message}`));
        }
        await safeSendMessage(String(chatId), errorMsgToUserHTML, { parse_mode: 'HTML', reply_markup: errorKeyboard });

        let refundClient = null;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmount, 'refund_ou7_helper_fail', { game_id_custom_field: gameId }, `Refund OU7 game ${gameId} - Helper Bot error: ${String(helperBotError).substring(0, 100)}`);
            await refundClient.query('COMMIT');
        } catch (dbErr) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`[OU7_Choice_Cleanup] CRITICAL: Failed to refund after OU7 helper error for game ${gameId}: ${dbErr.message}`);
            notifyAdmin(`🚨 CRITICAL OU7 REFUND FAILURE (Helper Error) 🚨\nGame ID: <code>${escapeHTML(gameId)}</code>, User: ${userId}\nError: ${escapeHTML(helperBotError || "")}\nDB Refund Error: ${escapeHTML(dbErr.message)}. MANUAL REFUND REQUIRED.`, { parse_mode: 'HTML' });
        } finally {
            if (refundClient) refundClient.release();
        }
        activeGames.delete(gameId);
        return;
    }

    gameData.diceRolls = diceRolls;
    gameData.diceSum = BigInt(diceSum);
    gameData.status = 'game_over';

    let win = false;
    let profitMultiplier = 0;
    if (choice === 'under' && diceSum < 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
    else if (choice === 'over' && diceSum > 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
    else if (choice === 'seven' && diceSum === 7) { win = true; profitMultiplier = OU7_PAYOUT_SEVEN; }

    let payoutAmountLamports = 0n;
    let outcomeReasonLog = `loss_ou7_${choice}_sum${diceSum}`;
    let resultTextPartHTML = "";
    const profitAmountLamports = win ? betAmount * BigInt(Math.floor(profitMultiplier)) : 0n;

    if (win) {
        payoutAmountLamports = betAmount + profitAmountLamports;
        outcomeReasonLog = `win_ou7_${choice}_sum${diceSum}`;
        const winEmoji = choice === 'seven' ? "🎯 JACKPOT!" : "🎉 WINNER!";
        resultTextPartHTML = `${winEmoji} Your prediction of <b>${escapeHTML(choiceTextDisplay)} 7</b> was spot on! You've won a handsome <b>${escapeHTML(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}</b> in profit!`;
    } else {
        payoutAmountLamports = 0n;
        resultTextPartHTML = `💔 So Close! The dice didn't favor your prediction of <b>${escapeHTML(choiceTextDisplay)} 7</b> this round. Better luck next time!`;
    }

    let clientOutcome = null;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, userId, payoutAmountLamports, outcomeReasonLog,
            { game_id_custom_field: gameId, dice_rolls_info: diceRolls.join(','), player_choice_info: choice },
            `Outcome of OU7 game ${gameId}. Player chose ${choice}, sum was ${diceSum}. Rolls: ${diceRolls.join(',')}.`
        );

        if (balanceUpdate.success) {
            await clientOutcome.query('COMMIT');
        } else {
            await clientOutcome.query('ROLLBACK');
            resultTextPartHTML += `\n\n⚠️ A critical error occurred settling your bet: <code>${escapeHTML(balanceUpdate.error || "DB Error")}</code>. Casino staff alerted.`;
            console.error(`[OU7_Choice_Cleanup] Failed to update balance for OU7 game ${gameId}. Error: ${balanceUpdate.error}.`);
            if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL OU7 Payout/Refund Failure 🚨\nGame ID: <code>${escapeHTML(gameId)}</code> User: ${playerRefHTML} (<code>${escapeHTML(userId)}</code>)\nAmount Due: <code>${escapeHTML(formatCurrency(payoutAmountLamports))}</code>\nDB Error: <code>${escapeHTML(balanceUpdate.error || "N/A")}</code>. Manual check required.`, { parse_mode: 'HTML' });
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(() => {});
        console.error(`[OU7_Choice_Cleanup] DB error during OU7 outcome processing for ${gameId}: ${dbError.message}`);
        resultTextPartHTML += `\n\n⚠️ A severe database error occurred. Casino staff notified.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    const titleResultHTML = `🏁 <b>Over/Under 7 - Result!</b> 🏁`;
    // MODIFIED: Added playerRefHTML to the beginning of the player-specific line
    let finalMessageTextHTML = `${titleResultHTML}\n\nPlayer: ${playerRefHTML}\nBet: <b>${betDisplayUSD_HTML}</b> on <b>${escapeHTML(choiceTextDisplay)} 7</b>.\n\n`;
    finalMessageTextHTML += `The Helper Bot rolled: ${formatDiceRolls(diceRolls)} for a grand total of <b>${escapeHTML(String(diceSum))}</b>!\n\n${resultTextPartHTML}`;
    // Balance display already removed as per previous request

    const postGameKeyboardOU7 = createPostGameKeyboard(GAME_IDS.OVER_UNDER_7, betAmount);

    if (messageIdToDeleteBeforeFinalResult && bot) {
        await bot.deleteMessage(String(chatId), Number(messageIdToDeleteBeforeFinalResult)).catch(e => {
            console.warn(`[OU7_Choice_Cleanup] Non-critical: Failed to delete message ${messageIdToDeleteBeforeFinalResult}. Error: ${e.message}`);
        });
    }

    await safeSendMessage(String(chatId), finalMessageTextHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboardOU7 });

    activeGames.delete(gameId);
}

// --- End of Part 5c, Section 1 (NEW + DEBUG LOGS) ---
// --- Start of Part 5c, Section 2 (COMPLETE REWRITE FOR NEW DUEL GAME LOGIC - CONSOLIDATED UPDATES) ---
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
async function getSingleDiceRollViaHelperDuel(gameId, chatIdForLog, userIdForRoll, rollPurposeNote) {
    const logPrefix = `[Duel_GetSingleRoll GID:${gameId} Purpose:"${rollPurposeNote}" UID:${userIdForRoll || 'BOT_INTERNAL'}]`;
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
        client.release(); client = null;

        let attempts = 0;
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
                    return { roll: statusResult.roll_value, error: false };
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
async function handleStartDuelUnifiedOfferCommand(msg, betAmountLamports, targetUsernameRaw = null) { 
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const logPrefix = `[Duel_OfferOrDirect UID:${userId} CH:${chatId} Type:${chatType}]`;

    if (chatType === 'private') {
        await safeSendMessage(chatId, `⚔️ Greetings, Duelist! The High Roller Duel game, with its thrilling PvP and PvB options, can only be initiated in a <b>group chat</b>. Please use the <code>/duel &lt;bet&gt; [@username]</code> command there to lay down your challenge!`, { parse_mode: 'HTML' });
        return;
    }

    let initiatorUserObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!initiatorUserObj) {
        await safeSendMessage(chatId, "Apologies, your player profile couldn't be accessed. Please use <code>/start</code> with me first, then try <code>/duel</code> again.", { parse_mode: 'HTML' });
        return;
    }
    const initiatorPlayerRefHTML = escapeHTML(getPlayerDisplayReference(initiatorUserObj)); 
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    const gameSession = await getGroupSession(chatId, msg.chat.title || `Group Chat ${chatId}`);
    if (gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
        const existingGame = activeGames.get(gameSession.currentGameId);
        if ( (existingGame.type.includes('_offer') && existingGame.status === 'pending_offer') || 
             (existingGame.type === GAME_IDS.DIRECT_PVP_CHALLENGE && existingGame.status === 'pending_direct_challenge_response') ||
             (existingGame.type.includes('_pvp') && !existingGame.status?.startsWith('game_over_')) ||
             (existingGame.type === GAME_IDS.MINES && existingGame.status !== 'game_over_mine_hit' && existingGame.status !== 'game_over_cashed_out')
           ) {
            await safeSendMessage(chatId, `⏳ Hold your weapons, ${initiatorPlayerRefHTML}! Another game offer or an active Player vs Player match (<code>${escapeHTML(existingGame.type.replace(/_/g," "))}</code>) is currently underway in this group. Please wait for it to conclude before initiating a new Duel.`, { parse_mode: 'HTML' });
            return;
        }
    }

    if (BigInt(initiatorUserObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(initiatorUserObj.balance);
        await safeSendMessage(chatId, `${initiatorPlayerRefHTML}, your treasury is too light for a <b>${betDisplayUSD_HTML}</b> Duel! You need about <b>${escapeHTML(await formatBalanceForDisplay(needed, 'USD'))}</b> more.`, {
            parse_mode: 'HTML', 
            reply_markup: { inline_keyboard: [[{ text: "💰 Top Up Balance (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    let targetUserObject = null;
    if (targetUsernameRaw) {
        targetUserObject = await findRecipientUser(targetUsernameRaw);
        if (!targetUserObject || !targetUserObject.telegram_id) { 
            await safeSendMessage(chatId, `😕 Player ${escapeHTML(targetUsernameRaw)} not found or has an invalid ID. Cannot create a Duel challenge. Please ensure they have started a chat with me first.`, { parse_mode: 'HTML' });
            return; 
        }
        if (String(targetUserObject.telegram_id) === userId) {
            await safeSendMessage(chatId, `😅 You can't challenge yourself to a Duel, ${initiatorPlayerRefHTML}!`, { parse_mode: 'HTML' });
            return;
        }
    }

    if (targetUserObject && targetUserObject.telegram_id) { 
        // --- DIRECT PvP CHALLENGE FLOW for Duel ---
        console.log(`${logPrefix} Initiating DIRECT Duel challenge to User ID: ${targetUserObject.telegram_id} (@${targetUserObject.username || 'N/A'})`);
        const targetPlayerRefHTML = escapeHTML(getPlayerDisplayReference(targetUserObject));
        const offerId = generateGameId(`dduel_${userId.slice(-3)}_${String(targetUserObject.telegram_id).slice(-3)}`); 

        const groupChallengeTextHTML = `Hey ${targetPlayerRefHTML}❗\n\n${initiatorPlayerRefHTML} has thrown down the gauntlet, challenging you to a <b>Duel</b> for <b>${betDisplayUSD_HTML}</b>!`;
        
        const groupChallengeKeyboard = { 
            inline_keyboard: [
                [{ text: "✅ Accept Challenge", callback_data: `dir_chal_acc:${offerId}` }],
                [{ text: "❌ Decline Challenge", callback_data: `dir_chal_dec:${offerId}` }],
                [{ text: "🚫 Withdraw My Challenge", callback_data: `dir_chal_can:${offerId}` }]
            ]
        };

        const sentGroupMessage = await safeSendMessage(chatId, groupChallengeTextHTML, { parse_mode: 'HTML', reply_markup: groupChallengeKeyboard });

        if (!sentGroupMessage || !sentGroupMessage.message_id) {
            console.error(`${logPrefix} Failed to send direct Duel challenge message for offer ${offerId}.`);
            await safeSendMessage(chatId, `⚙️ Oops! Couldn't send your Duel challenge to ${targetPlayerRefHTML}. Please try again.`, { parse_mode: 'HTML' });
            return;
        }
        const offerMessageIdInGroup = String(sentGroupMessage.message_id);
        const groupNameHTML = escapeHTML(msg.chat.title || "the group");

        const dmNotificationTextHTML = `🔔 Challenge Alert!\n\nHi ${targetPlayerRefHTML},\n${initiatorPlayerRefHTML} has challenged you to a <b>Duel</b> for <b>${betDisplayUSD_HTML}</b> in the group "<b>${groupNameHTML}</b>".\n\nPlease head to that group to accept or decline the challenge.`;
        const dmSent = await safeSendMessage(targetUserObject.telegram_id, dmNotificationTextHTML, { parse_mode: 'HTML' });
        if (!dmSent) {
            console.warn(`${logPrefix} Failed to send DM notification for direct Duel challenge to target ${targetUserObject.telegram_id}. Offer still posted in group.`);
            await safeSendMessage(chatId, `ℹ️ ${initiatorPlayerRefHTML}, your challenge to ${targetPlayerRefHTML} is posted! Note: They might not receive a DM if they haven't interacted with me before.`, { parse_mode: 'HTML'});
        }

        const directOfferData = {
            type: GAME_IDS.DIRECT_PVP_CHALLENGE, 
            offerId: offerId, 
            gameId: offerId,
            initiatorId: userId,
            initiatorUserObj: initiatorUserObj, 
            initiatorMentionHTML: initiatorPlayerRefHTML,
            targetUserId: String(targetUserObject.telegram_id),
            targetUserObj: targetUserObject, 
            targetUserMentionHTML: targetPlayerRefHTML,
            betAmount: betAmountLamports,
            originalGroupId: chatId,
            offerMessageIdInGroup: offerMessageIdInGroup,
            chatTitle: msg.chat.title || `Group Chat ${chatId}`, 
            status: 'pending_direct_challenge_response',
            gameToStart: GAME_IDS.DUEL_PVP, 
            creationTime: Date.now()
        };
        activeGames.set(offerId, directOfferData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.DIRECT_PVP_CHALLENGE, betAmountLamports);
        console.log(`${logPrefix} Direct Duel challenge offer ${offerId} created and stored.`);

        setTimeout(async () => {
            const timedOutOffer = activeGames.get(offerId);
            if (timedOutOffer && timedOutOffer.status === 'pending_direct_challenge_response' && timedOutOffer.type === GAME_IDS.DIRECT_PVP_CHALLENGE) {
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null);
                const gameNameForTimeout = "Duel"; 
                const timeoutBetDisplay = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                const timeoutMsgHTML = `⏳ The ${gameNameForTimeout} challenge from ${timedOutOffer.initiatorMentionHTML} to ${timedOutOffer.targetUserMentionHTML} for <b>${timeoutBetDisplay}</b> has expired unanswered.`;
                if (bot && timedOutOffer.offerMessageIdInGroup) {
                    await bot.editMessageText(timeoutMsgHTML, {
                        chat_id: timedOutOffer.originalGroupId, message_id: Number(timedOutOffer.offerMessageIdInGroup),
                        parse_mode: 'HTML', reply_markup: {} 
                    }).catch(e => { 
                        console.warn(`${logPrefix} Failed to edit expired direct Duel challenge message ${timedOutOffer.offerMessageIdInGroup}: ${e.message}. Sending new.`);
                        safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                    });
                } else { 
                    safeSendMessage(timedOutOffer.originalGroupId, timeoutMsgHTML, { parse_mode: 'HTML' });
                }
                await safeSendMessage(timedOutOffer.initiatorId, `⏳ Your Duel challenge to ${timedOutOffer.targetUserMentionHTML} in group "${escapeHTML(timedOutOffer.chatTitle)}" has expired.`, { parse_mode: 'HTML' });
            }
        }, JOIN_GAME_TIMEOUT_MS);

    } else {
        // --- EXISTING DUEL UNIFIED OFFER FLOW (No valid targetUserObject specified) ---
        console.log(`${logPrefix} Initiating UNIFIED Duel offer (no target user or target was invalid).`);
        const offerId = generateGameId(GAME_IDS.DUEL_UNIFIED_OFFER); 
        const offerData = { 
            type: GAME_IDS.DUEL_UNIFIED_OFFER, 
            gameId: offerId, 
            chatId: String(chatId), 
            chatType,
            initiatorId: userId, 
            initiatorMention: initiatorPlayerRefHTML, 
            initiatorUserObj,
            betAmount: betAmountLamports, 
            status: 'waiting_for_choice', 
            creationTime: Date.now(), 
            gameSetupMessageId: null 
        };
        activeGames.set(offerId, offerData);
        await updateGroupGameDetails(chatId, offerId, GAME_IDS.DUEL_UNIFIED_OFFER, betAmountLamports);

        const offerMessageTextHTML = 
            `⚔️ <b>A High Roller Duel Challenge!</b> ⚔️\n\n` +
            `${initiatorPlayerRefHTML} has thrown down the gauntlet for a Duel, staking <b>${betDisplayUSD_HTML}</b>!\n\n` +
            `Will another duelist accept the challenge for a Player vs. Player showdown?\n` +
            `Or will ${initiatorPlayerRefHTML} face the casino's own Bot Dealer?`;

        const offerKeyboard = { 
            inline_keyboard: [
                [{ text: "🤝 Accept PvP Duel!", callback_data: `duel_accept_pvp_challenge:${offerId}` }],
                [{ text: "🤖 Challenge the Bot Dealer", callback_data: `duel_accept_bot_game:${offerId}` }],
                [{ text: "🚫 Withdraw My Challenge (Initiator)", callback_data: `duel_cancel_unified_offer:${offerId}` }]
            ]
        };

        const sentOfferMessage = await safeSendMessage(chatId, offerMessageTextHTML, { parse_mode: 'HTML', reply_markup: offerKeyboard });

        if (sentOfferMessage?.message_id) {
            const offerInMap = activeGames.get(offerId);
            if(offerInMap) {
                offerInMap.gameSetupMessageId = String(sentOfferMessage.message_id);
                activeGames.set(offerId, offerInMap);
            } else {
                 if (bot) await bot.deleteMessage(chatId, sentOfferMessage.message_id).catch(()=>{});
            }
        } else {
            console.error(`${logPrefix} CRITICAL: Failed to send Duel unified offer message for offer ID ${offerId}. Cleaning up.`);
            activeGames.delete(offerId);
            await updateGroupGameDetails(chatId, null, null, null);
            await safeSendMessage(chatId, `An error prevented the Duel offer by ${initiatorPlayerRefHTML} from being created. Please try again.`, {parse_mode: 'HTML'});
            return;
        }

        setTimeout(async () => {
            const currentOfferData = activeGames.get(offerId);
            if (currentOfferData && currentOfferData.status === 'waiting_for_choice' && currentOfferData.type === GAME_IDS.DUEL_UNIFIED_OFFER) {
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null);
                if (currentOfferData.gameSetupMessageId && bot) {
                    const expiredOfferBetDisplayUSD = escapeHTML(await formatBalanceForDisplay(currentOfferData.betAmount, 'USD'));
                    const offerExpiredMessageText = `⏳ The Duel challenge by ${currentOfferData.initiatorMention} for <b>${expiredOfferBetDisplayUSD}</b> has timed out. This challenge is now closed.`;
                    await bot.editMessageText(offerExpiredMessageText, {
                        chat_id: String(chatId), message_id: Number(currentOfferData.gameSetupMessageId),
                        parse_mode: 'HTML', 
                        reply_markup: {}
                    }).catch(e => console.warn(`${logPrefix} Error editing message for expired Duel offer (ID: ${currentOfferData.gameSetupMessageId}): ${e.message}`));
                }
            }
        }, JOIN_GAME_TIMEOUT_MS);
    }
}

// --- Callback Handlers for Duel Unified Offer ---
async function handleDuelAcceptBotGameCallback(offerId, initiatorUserObjFromCb, originalOfferMessageId, originalChatId, originalChatType, callbackQueryIdPassed = null) {
    const initiatorId = String(initiatorUserObjFromCb.id || initiatorUserObjFromCb.telegram_id);
    const logPrefix = `[Duel_AcceptBotCB GID:${offerId} UID:${initiatorId}]`;
    const offerData = activeGames.get(offerId);
    const callbackQueryId = callbackQueryIdPassed;

    if (!offerData || offerData.type !== GAME_IDS.DUEL_UNIFIED_OFFER) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Duel offer seems to have expired or is invalid.", show_alert: true }).catch(()=>{});
        else if (offerData?.gameSetupMessageId && bot) await bot.editMessageReplyMarkup({}, {chat_id:originalChatId, message_id:Number(offerData.gameSetupMessageId)}).catch(()=>{});
        return;
    }
    if (offerData.initiatorId !== initiatorId) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Only the challenger can start this Duel against the Bot!", show_alert: true }).catch(()=>{});
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
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

    await startDuelPvBGame(
        originalChatId,
        offerData.initiatorUserObj,
        offerData.betAmount,
        Number(offerData.gameSetupMessageId || originalOfferMessageId),
        offerId
    );
}

async function handleDuelAcceptPvPChallengeCallback(offerId, joinerUserObjFromCb, originalOfferMessageId, originalChatId, originalChatType, callbackQueryIdPassed = null) {
    const joinerId = String(joinerUserObjFromCb.id || joinerUserObjFromCb.telegram_id);
    const logPrefix = `[Duel_AcceptPvPCB GID:${offerId} JoinerID:${joinerId}]`;
    let offerData = activeGames.get(offerId);
    const joinerRef = getPlayerDisplayReference(joinerUserObjFromCb);
    const callbackQueryId = callbackQueryIdPassed;

    if (!offerData || offerData.type !== GAME_IDS.DUEL_UNIFIED_OFFER) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This Duel offer has expired or is invalid.", show_alert: true }).catch(()=>{});
        else if (offerData?.gameSetupMessageId && bot) await bot.editMessageReplyMarkup({}, {chat_id:originalChatId, message_id:Number(offerData.gameSetupMessageId)}).catch(()=>{});
        return;
    }
    if (offerData.initiatorId === joinerId) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "You can't duel yourself in this manner!", show_alert: true }).catch(()=>{});
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
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
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: `Not enough funds for this ${betDisplayUSD} duel. Top up!`, show_alert: true }).catch(()=>{});
        await safeSendMessage(originalChatId, `${joinerRef}, your balance is too low for this *${betDisplayUSD}* Duel. Top up your funds!`, {
            parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    let currentInitiatorUserObj = offerData.initiatorUserObj || await getOrCreateUser(offerData.initiatorId);
    if (!currentInitiatorUserObj || BigInt(currentInitiatorUserObj.balance) < betAmount) {
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
                rolls: [], score: 0, isTurn: false, status: 'waiting_turn' // Initial status
            },
            opponent: {
                userId: joinerId, mention: joinerRef, userObj: currentJoinerUserObj,
                rolls: [], score: 0, isTurn: false, status: 'waiting_turn' // Initial status
            },
            status: 'p1_awaiting_roll1_emoji', // This will be updated by startDuelPvPGameSequence
            creationTime: Date.now(), currentMessageId: null, lastInteractionTime: Date.now()
        };
        activeGames.set(pvpGameId, pvpGameData);
        activeGames.delete(offerId);
        await updateGroupGameDetails(originalChatId, pvpGameId, GAME_IDS.DUEL_PVP, betAmount);

        await startDuelPvPGameSequence(pvpGameId); // Call this to set initial turns and send first message

    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} CRITICAL error creating PvP Duel game ${pvpGameId}: ${e.message}`);
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

async function handleDuelCancelUnifiedOfferCallback(offerId, initiatorUserObjFromCb, originalOfferMessageId, originalChatId, callbackQueryIdPassed = null) {
    const initiatorId = String(initiatorUserObjFromCb.id || initiatorUserObjFromCb.telegram_id);
    const LOG_PREFIX_DUEL_CANCEL_OFFER = `[Duel_CancelOffer_V3 UID:${initiatorId} OfferID:${offerId}]`;
    const offerData = activeGames.get(offerId);
    const callbackQueryId = callbackQueryIdPassed;

    if (!offerData || offerData.type !== GAME_IDS.DUEL_UNIFIED_OFFER) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This offer is no longer valid.", show_alert: false }).catch(()=>{});
        if (originalOfferMessageId && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalOfferMessageId) }).catch(() => {});
        }
        return;
    }
    if (offerData.initiatorId !== initiatorId) {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Only the challenger can withdraw this offer.", show_alert: true }).catch(()=>{});
        return;
    }
    if (offerData.status !== 'waiting_for_choice') {
        if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "This offer has already been actioned or expired.", show_alert: false }).catch(()=>{});
        return;
    }
    if(callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, {text: "Duel offer cancelled."}).catch(()=>{});

    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null);

    const messageIdToDelete = Number(originalOfferMessageId || offerData.gameSetupMessageId);
    if (messageIdToDelete && bot) {
        await bot.deleteMessage(String(originalChatId), messageIdToDelete)
            .catch(e => console.warn(`${LOG_PREFIX_DUEL_CANCEL_OFFER} Failed to delete cancelled Duel offer message ${messageIdToDelete}: ${e.message}`));
    }
    
    const betDisplayUSD = typeof formatBalanceForDisplay === 'function' ? escapeMarkdownV2(await formatBalanceForDisplay(offerData.betAmount, 'USD')) : `${offerData.betAmount / LAMPORTS_PER_SOL} SOL`;
    const confirmationMessage = `🚫 Offer Cancelled!\nThe Duel challenge by ${offerData.initiatorMention} for *${betDisplayUSD}* has been withdrawn.`;
    
    await safeSendMessage(originalChatId, confirmationMessage, { parse_mode: 'MarkdownV2' });
}

// --- Player vs. Bot (PvB) Duel Game Logic ---
async function startDuelPvBGame(chatId, initiatorUserObj, betAmountLamports, originalOfferMessageIdToDelete, unifiedOfferIdIfAny) {
    const userId = String(initiatorUserObj.telegram_id);
    const logPrefix = `[Duel_PvB_Start UID:${userId} CH:${chatId}]`;
    const playerRef = getPlayerDisplayReference(initiatorUserObj);
    const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (unifiedOfferIdIfAny && originalOfferMessageIdToDelete && bot) {
        await bot.deleteMessage(chatId, Number(originalOfferMessageIdToDelete))
            .catch(e => console.warn(`${logPrefix} Non-critical: Could not delete unified Duel offer message ${originalOfferMessageIdToDelete}: ${e.message}`));
    }
    
    let client;
    const gameId = generateGameId(GAME_IDS.DUEL_PVB);
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const currentBalance = await getUserBalance(userId);
        if (currentBalance === null || BigInt(currentBalance) < betAmountLamports) { // Added null check for currentBalance
             throw new Error (`User ${userId} balance ${currentBalance === null ? 'N/A' : currentBalance} insufficient for bet ${betAmountLamports} at actual PvB start.`);
        }

        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, BigInt(-betAmountLamports),
            'bet_placed_duel_pvb', { game_id_custom_field: gameId },
            `Bet placed for PvB Duel game ${gameId}`);
        if (!balanceUpdateResult.success) {
            throw new Error(balanceUpdateResult.error || "DB error: PvB Duel wager placement failed.");
        }
        initiatorUserObj.balance = balanceUpdateResult.newBalanceLamports;
        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`${logPrefix} Error starting PvB Duel game: ${e.message}`);
        await safeSendMessage(chatId, `${playerRef}, a critical error occurred starting your Duel vs Bot: \`${escapeMarkdownV2(e.message)}\`. Please try again.`, {parse_mode: 'MarkdownV2'});
        if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) activeGames.delete(unifiedOfferIdIfAny);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    } finally {
        if (client) client.release();
    }

    if (unifiedOfferIdIfAny && activeGames.has(unifiedOfferIdIfAny)) {
        activeGames.delete(unifiedOfferIdIfAny);
    }
    
    const gameData = {
        type: GAME_IDS.DUEL_PVB, gameId, chatId: String(chatId), chatType: 'group', // Assuming PvB is always group
        playerId: userId, playerRef, userObj: initiatorUserObj, betAmount: betAmountLamports,
        playerRolls: [], playerScore: 0, botRolls: [], botScore: 0,
        status: 'player_awaiting_roll1_emoji', // Player rolls first die
        gameMessageId: null, lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);
    await updateGroupGameDetails(chatId, gameId, GAME_IDS.DUEL_PVB, betAmountLamports);

    const initialMessageText =
        `⚔️ **Duel vs. Bot Dealer!** ⚔️\n\n` +
        `${playerRef}, your wager: *${betDisplayUSD}*.\n\n` +
        `It's your turn to roll! Please send your **first** 🎲 dice emoji.`; // Prompt for first die
        
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

async function processDuelPlayerRollsCompletePvB(gameData, firstRoll, secondRoll) { // This is for PvB
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
        await bot.deleteMessage(chatId, Number(gameData.gameMessageId)).catch(()=>{});
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

    const botRollsResult = await getTwoDiceRollsViaHelperDuel(gameId, chatId, null, "Duel PvB Bot Roll");

    if (botRollsResult.error) {
        console.error(`${logPrefix} Bot failed to get its rolls: ${botRollsResult.message}. Refunding player.`);
        if (gameData.gameMessageId && bot) {
            await bot.deleteMessage(chatId, Number(gameData.gameMessageId)).catch(()=>{});
        }
        const betDisplayUSDOnError = escapeMarkdownV2(await formatBalanceForDisplay(betAmount, 'USD'));
        const errorMsg = `${playerRef}, the Bot Dealer encountered an issue getting its dice: \`${escapeMarkdownV2(botRollsResult.message.substring(0,100))}\`. Your *${betDisplayUSDOnError}* wager is refunded.`;
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

// REVERTED finalizeDuelPvBGame (Simpler style, "wins pot" wording)
async function finalizeDuelPvBGame(gameData) {
    const { gameId, chatId, playerId, playerRef, playerScore, botScore, betAmount, userObj, gameMessageId, playerRolls, botRolls } = gameData;

    let outcomeHeader = ""; 
    let outcomeDetails = "";
    let winningsText = "";   
    let titleEmoji = "⚔️";
    let payoutAmountLamports = 0n;
    let ledgerOutcomeCode = "";
    const totalPotForDisplay = betAmount * 2n; 

    const playerRefHTML = escapeHTML(playerRef);
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'));

    if (playerScore > botScore) {
        titleEmoji = "🏆";
        outcomeHeader = `🎉 <b>VICTORY, ${playerRefHTML}!</b>`;
        outcomeDetails = `Your score of <b>${playerScore}</b> triumphs over the Bot Dealer's <i>${botScore}</i>.`;
        payoutAmountLamports = betAmount * 2n;
        ledgerOutcomeCode = 'win_duel_pvb';
        winningsText = `You win the pot of <b>${escapeHTML(await formatBalanceForDisplay(totalPotForDisplay, 'USD'))}</b>!`;
    } else if (botScore > playerScore) {
        titleEmoji = "🤖";
        outcomeHeader = `💔 <b>The Bot Prevails, ${playerRefHTML}.</b>`;
        outcomeDetails = `The Bot Dealer's score of <b>${botScore}</b> edges out your <i>${playerScore}</i>.`;
        payoutAmountLamports = 0n;
        ledgerOutcomeCode = 'loss_duel_pvb';
        winningsText = `Better luck next time!`;
    } else {
        titleEmoji = "⚖️";
        outcomeHeader = `⚖️ <b>A DRAW, ${playerRefHTML}!</b>`;
        outcomeDetails = `Both you and the Bot Dealer scored <b>${playerScore}</b>!`;
        payoutAmountLamports = betAmount;
        ledgerOutcomeCode = 'push_duel_pvb';
        winningsText = `💰 Your wager of <b>${betDisplayUSD_HTML}</b> is returned.`;
    }

    let finalMessageTextHTML =
        `${titleEmoji} <b>Duel Result: You vs Bot Dealer</b> ${titleEmoji}\n\n` +
        `Player: ${playerRefHTML}\n` +
        `Wager: <b>${betDisplayUSD_HTML}</b>\n` +
        `------------------------------------\n` +
        `<b>Player Rolls:</b>\n` +
        `👤 You: ${formatDiceRolls(playerRolls)} ➠ Score: <b>${playerScore}</b>\n` +
        `🤖 Bot Dealer: ${formatDiceRolls(botRolls)} ➠ Score: <b>${botScore}</b>\n` +
        `------------------------------------\n` +
        `${outcomeHeader}\n` +
        `${outcomeDetails}\n\n` +
        `${winningsText}`;

    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const balanceUpdate = await updateUserBalanceAndLedger(client, playerId, payoutAmountLamports,
            ledgerOutcomeCode, { game_id_custom_field: gameId, player_score_val: playerScore, bot_score_val: botScore },
            `PvB Duel game ${gameId} result`);

        if (!balanceUpdate.success) {
            await client.query('ROLLBACK'); 
            throw new Error(balanceUpdate.error || "DB Error during PvB Duel payout.");
        }
        await client.query('COMMIT');
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(()=>{});
        console.error(`[Duel_PvB_Finalize_HTML_V10] CRITICAL DB error: ${e.message}`);
        finalMessageTextHTML += `\n\n⚠️ <i>Critical error settling wager: ${escapeHTML(e.message)}. Admin notified.</i>`;
        if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Duel PvB Payout Failure 🚨\nGame ID: <code>${gameId}</code> User: ${playerRefHTML}\nAmount: ${payoutAmountLamports}\nDB Error: ${escapeHTML(e.message)}. MANUAL CHECK REQUIRED.`, {parse_mode: 'HTML'});
    } finally {
        if (client) client.release();
    }

    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.DUEL_PVB, betAmount);

    if (gameMessageId && bot) {
        await bot.deleteMessage(chatId, Number(gameMessageId)).catch(()=>{});
    }
    await safeSendMessage(chatId, finalMessageTextHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboard });
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
}

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
        console.error(`[Duel_PvP_StartSeq_Debug GID:${pvpGameId}] Invalid game data or type. Cannot start sequence.`);
        return;
    }
    const logPrefix = `[Duel_PvP_StartSeq_Debug GID:${pvpGameId}]`;
    console.log(`${logPrefix} Starting turn sequence. P1 (${gameData.initiator.mention}) to roll first.`);

    gameData.initiator.isTurn = true;
    gameData.initiator.status = 'awaiting_roll_emoji'; // Player is awaiting emoji
    gameData.initiator.rolls = []; // Ensure rolls are reset
    gameData.initiator.score = 0;  // Ensure score is reset

    gameData.opponent.isTurn = false;
    gameData.opponent.status = 'waiting_turn';
    gameData.opponent.rolls = []; // Ensure rolls are reset
    gameData.opponent.score = 0;  // Ensure score is reset

    gameData.status = 'p1_awaiting_roll1_emoji'; // Game state indicates P1 needs first die of DUEL_DICE_COUNT
    activeGames.set(pvpGameId, gameData);
    console.log(`${logPrefix} Initial statuses set. P1 turn. Game status: ${gameData.status}. P1 status: ${gameData.initiator.status}. Calling updateDuelPvPMessage.`);
    await updateDuelPvPMessage(pvpGameId, true); // Pass true for isInitialTurnMessage
}

async function processDuelPlayerRollsCompletePvP(gameData, firstRoll, secondRoll, actingPlayerId) {
    const { gameId, chatId, initiator, opponent, betAmount } = gameData; // Destructuring gameData
    const LOG_PREFIX_PVP_PLAYERDONE_DEBUG = `[Duel_PvP_PlayerDone_Debug GID:${gameId} Actor:${actingPlayerId}]`;

    console.log(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} Entered. Rolls: [${firstRoll}, ${secondRoll}].`);

    let currentPlayer = (initiator.userId === actingPlayerId) ? initiator : opponent;
    let otherPlayer = (initiator.userId === actingPlayerId) ? opponent : initiator;

    currentPlayer.rolls = [firstRoll, secondRoll]; // Set the final two rolls
    currentPlayer.score = firstRoll + secondRoll;  // Calculate score from these two rolls
    currentPlayer.isTurn = false;
    currentPlayer.status = 'rolls_complete'; // Mark current player as done with their rolls for the turn
    gameData.lastInteractionTime = Date.now();

    console.log(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} ${currentPlayer.mention} completed rolls. Score: ${currentPlayer.score}. Status: ${currentPlayer.status}.`);

    // Determine the game's next state
    if (otherPlayer.status === 'waiting_turn') {
        // This means the other player hasn't rolled at all yet. It's now their turn.
        otherPlayer.isTurn = true;
        otherPlayer.status = 'awaiting_roll_emoji'; // Set them to expect rolls
        gameData.status = (otherPlayer === initiator) ? 'p1_awaiting_roll1_emoji' : 'p2_awaiting_roll1_emoji'; // Other player needs their first die
        console.log(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} Switching turn to ${otherPlayer.mention}. New game status: ${gameData.status}. Other player status: ${otherPlayer.status}.`);
    } else if (otherPlayer.status === 'rolls_complete') {
        // This means the other player had ALREADY completed their rolls. So now both are done.
        gameData.status = 'game_over_pvp_resolved';
        console.log(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} Both players have completed rolls. Game status set to: ${gameData.status}.`);
    } else {
        // Fallback for any other unexpected status of the otherPlayer
        console.warn(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} Unexpected 'otherPlayer' status: ${otherPlayer.status}. Forcing resolution.`);
        gameData.status = 'game_over_pvp_resolved';
    }

    activeGames.set(gameId, gameData); // Save updated gameData
    console.log(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} Calling updateDuelPvPMessage for game status: ${gameData.status}.`);
    await updateDuelPvPMessage(gameId); // Update message to reflect current scores and next turn prompt

    if (gameData.status === 'game_over_pvp_resolved') {
        console.log(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} Status is 'game_over_pvp_resolved'. Will call resolveDuelPvPGame.`);
        await sleep(1500); // Give a moment for players to see the "concluding" message
        try {
            await resolveDuelPvPGame(gameData); // Ensure this is awaited and gameData object is passed
            console.log(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} resolveDuelPvPGame completed successfully.`);
        } catch (error) {
            console.error(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} CRITICAL ERROR calling or during resolveDuelPvPGame: ${error.message}`, error.stack);
            await safeSendMessage(gameData.chatId, `⚙️ A critical error occurred while finalizing the Duel game results. Admin has been notified. Game ID: ${gameId}`, { parse_mode: 'HTML' });
            if (typeof notifyAdmin === 'function') {
                notifyAdmin(`🚨 CRITICAL ERROR in Duel PvP GID: ${gameData.gameId}\nFunction resolveDuelPvPGame failed or error within it.\nError: ${escapeHTML(error.message)}`, {parse_mode: 'HTML'});
            }
            activeGames.delete(gameData.gameId); // Clean up game state
            await updateGroupGameDetails(gameData.chatId, null, null, null);
        }
    } else {
        console.log(`${LOG_PREFIX_PVP_PLAYERDONE_DEBUG} Status is '${gameData.status}', not 'game_over_pvp_resolved'. Not calling resolveDuelPvPGame yet.`);
    }
}

async function resolveDuelPvPGame(gameDataOrId, playerWhoBustedId = null) {
    let gameData;
    let gameId_internal;

    if (typeof gameDataOrId === 'string') {
        gameId_internal = gameDataOrId;
        gameData = activeGames.get(gameId_internal);
    } else {
        gameData = gameDataOrId;
        gameId_internal = gameData?.gameId;
    }

    if (!gameData) { 
        console.error(`[Duel_PvP_Resolve_HTML_V10] CRITICAL: No game data for ID: ${gameId_internal || 'N/A'}.`); 
        if (gameId_internal) activeGames.delete(gameId_internal); 
        return; 
    }
    if (typeof gameData.betAmount === 'undefined' || gameData.betAmount === null) { 
        console.error(`[Duel_PvP_Resolve_HTML_V10] FATAL: gameData.betAmount undefined for GID ${gameData.gameId}.`); 
        activeGames.delete(gameData.gameId);
        const refundErrorMessage = `⚙️ A critical internal error occurred (Bet amount missing). Game cannot be resolved. Please contact support with Game ID: ${gameData.gameId}`;
        await safeSendMessage(gameData.chatId, refundErrorMessage, { parse_mode: 'HTML' });
        await updateGroupGameDetails(gameData.chatId, null, null, null);
        if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL: gameData.betAmount is UNDEFINED in resolveDuelPvPGame for GID ${gameData.gameId}.`, { parse_mode: 'HTML' });
        return; 
    }

    activeGames.delete(gameData.gameId);
    const p1 = gameData.initiator;
    const p2 = gameData.opponent;
    const currentBetAmountBigInt = BigInt(gameData.betAmount);

    const p1MentionHTML = escapeHTML(p1.mention);
    const p2MentionHTML = escapeHTML(p2.mention);

    let winner = null; 
    let isPush = false;
    let outcomeHeader = "";    // e.g., 🏆 <b>@Player1 WINS!</b>
    let outcomeDetails = "";   // e.g., Score <b>X</b> beats Y's <i>Z</i>.
    let financialOutcome = ""; // e.g., @Player1 wins the pot of <b>$AMOUNT</b>!
    let titleEmoji = "⚔️";
    let totalPotLamports = currentBetAmountBigInt * 2n;
    let p1Payout = 0n; let p2Payout = 0n;
    let p1LedgerCode = 'loss_duel_pvp'; let p2LedgerCode = 'loss_duel_pvp';
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(currentBetAmountBigInt, 'USD'));

    if (playerWhoBustedId === p1.userId || (p1.status === 'bust' || p1.busted) ) {
        titleEmoji = "💥"; winner = p2; p1.busted = true;
        p2Payout = totalPotLamports; p2LedgerCode = 'win_duel_pvp_opponent_bust';
        outcomeHeader = `💣 <b>${p1MentionHTML} BUSTED!</b>`;
        outcomeDetails = `${escapeHTML(winner.mention)} seizes victory!`;
        financialOutcome = `🎉 <b>${escapeHTML(winner.mention)}</b> wins the pot of <b>${escapeHTML(await formatBalanceForDisplay(totalPotLamports, 'USD'))}</b>!`;
    } else if (playerWhoBustedId === p2.userId || (p2.status === 'bust' || p2.busted) ) {
        titleEmoji = "💥"; winner = p1; p2.busted = true;
        p1Payout = totalPotLamports; p1LedgerCode = 'win_duel_pvp_opponent_bust';
        outcomeHeader = `💣 <b>${p2MentionHTML} BUSTED!</b>`;
        outcomeDetails = `${escapeHTML(winner.mention)} masterfully claims the win!`;
        financialOutcome = `🎉 <b>${escapeHTML(winner.mention)}</b> wins the pot of <b>${escapeHTML(await formatBalanceForDisplay(totalPotLamports, 'USD'))}</b>!`;
    } else if (p1.status === 'rolls_complete' && p2.status === 'rolls_complete') {
        if (p1.score > p2.score) {
            titleEmoji = "🏆"; winner = p1; p1Payout = totalPotLamports; p1LedgerCode = 'win_duel_pvp_score';
            outcomeHeader = `🏆 <b>${p1MentionHTML} WINS!</b>`;
            outcomeDetails = `Their score of <b>${p1.score}</b> beats ${p2MentionHTML}'s <i>${p2.score}</i>.`;
            financialOutcome = `🎉 <b>${p1MentionHTML}</b> wins the pot of <b>${escapeHTML(await formatBalanceForDisplay(totalPotLamports, 'USD'))}</b>!`;
        } else if (p2.score > p1.score) {
            titleEmoji = "🏆"; winner = p2; p2Payout = totalPotLamports; p2LedgerCode = 'win_duel_pvp_score';
            outcomeHeader = `🏆 <b>${p2MentionHTML} WINS!</b>`;
            outcomeDetails = `Their score of <b>${p2.score}</b> beats ${p1MentionHTML}'s <i>${p1.score}</i>.`;
            financialOutcome = `🎉 <b>${p2MentionHTML}</b> wins the pot of <b>${escapeHTML(await formatBalanceForDisplay(totalPotLamports, 'USD'))}</b>!`;
        } else { 
            titleEmoji = "⚖️"; isPush = true;
            outcomeHeader = `⚖️  <b>IT'S A DRAW!</b>`;
            outcomeDetails = `Both ${p1MentionHTML} and ${p2MentionHTML} scored <b>${p1.score}</b>.`;
            p1Payout = currentBetAmountBigInt; p2Payout = currentBetAmountBigInt;
            p1LedgerCode = 'push_duel_pvp'; p2LedgerCode = 'push_duel_pvp';
            financialOutcome = `💰 Wagers of <b>${betDisplayUSD_HTML}</b> each are returned.`;
        }
    } else { 
        titleEmoji = "⚙️"; isPush = true;
        outcomeHeader = `⚙️ <b>Unexpected Duel Finish!</b>`;
        outcomeDetails = `The game concluded unusually. Bets refunded for fairness.`;
        p1Payout = currentBetAmountBigInt; p2Payout = currentBetAmountBigInt;
        p1LedgerCode = 'refund_duel_pvp_error'; p2LedgerCode = 'refund_duel_pvp_error';
        console.error(`[Duel_PvP_Resolve_HTML_V10] Undetermined Duel PvP outcome for GID ${gameData.gameId}. Refunding.`);
    }

    let finalMessageTextHTML =
        `${titleEmoji} <b>Duel PvP Result</b> ${titleEmoji}\n` +
        `<i>${p1MentionHTML} vs ${p2MentionHTML}</i>\n\n` +
        `<b>Wager</b>: ${betDisplayUSD_HTML} each\n` +
        `------------------------------------\n` +
        `<b>Player Rolls:</b>\n` +
        `👤 ${p1MentionHTML} (P1): ${formatDiceRolls(p1.rolls)} ➠ Score: <b>${p1.score}</b>${p1.busted ? " 💥 BUSTED" : ""}\n` +
        `👤 ${p2MentionHTML} (P2): ${formatDiceRolls(p2.rolls)} ➠ Score: <b>${p2.score}</b>${p2.busted ? " 💥 BUSTED" : ""}\n` +
        `------------------------------------\n` +
        `${outcomeHeader}\n` +
        `${outcomeDetails}\n\n` +
        `${financialOutcome}`;

    let client;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        // ... (Database logic for updateUserBalanceAndLedger remains the same as the last correct version)
        const p1Update = await updateUserBalanceAndLedger(client, p1.userId, p1Payout, p1LedgerCode, { game_id_custom_field: gameData.gameId, opponent_id_custom_field: p2.userId, player_score: p1.score, opponent_score: p2.score }, `Duel PvP Result vs ${p2.mention}`);
        if (!p1Update.success) throw new Error(`P1 (${p1MentionHTML}) update fail: ${p1Update.error}`);
        const p2Update = await updateUserBalanceAndLedger(client, p2.userId, p2Payout, p2LedgerCode, { game_id_custom_field: gameData.gameId, opponent_id_custom_field: p1.userId, player_score: p2.score, opponent_score: p1.score }, `Duel PvP Result vs ${p1.mention}`);
        if (!p2Update.success) throw new Error(`P2 (${p2MentionHTML}) update fail: ${p2Update.error}`);
        await client.query('COMMIT');

    } catch (e) {
        if (client) await client.query('ROLLBACK');
        console.error(`[Duel_PvP_Resolve_HTML_V10] CRITICAL DB Error Finalizing Duel PvP ${gameData.gameId}: ${e.message}`);
        // Append DB error to the message only if it's not already including a result.
        // The financialOutcome might be misleading if DB fails.
        finalMessageTextHTML += `\n\n⚠️ <i>Critical error settling wagers: ${escapeHTML(e.message)}. Support notified.</i>`;
        if (typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL Duel PvP Payout Failure 🚨\nGame ID: <code>${escapeHTML(gameData.gameId)}</code>\nError: ${escapeHTML(e.message)}. MANUAL CHECK REQUIRED.`, {parse_mode: 'HTML'});
    } finally {
        if (client) client.release();
    }

    const finalKeyboard = createPostGameKeyboard(GAME_IDS.DUEL_PVP, currentBetAmountBigInt);
    if (gameData.currentMessageId && bot) {
        await bot.deleteMessage(String(gameData.chatId), Number(gameData.currentMessageId)).catch(()=>{});
    }
    await safeSendMessage(gameData.chatId, finalMessageTextHTML, { parse_mode: 'HTML', reply_markup: finalKeyboard });
    await updateGroupGameDetails(gameData.chatId, null, null, null);
}

// MODIFIED updateDuelPvPMessage (Restored Instructions & Reverted Visuals)
async function updateDuelPvPMessage(gameId, isInitialTurnMessage = false) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.type !== GAME_IDS.DUEL_PVP) {
        console.warn(`[UpdateDuelPvPMsg_HTML_V9] Update for invalid/missing game ${gameId}`);
        return;
    }

    const p1 = gameData.initiator;
    const p2 = gameData.opponent;
    const p1MentionHTML = escapeHTML(p1.mention); // Ensure player mentions are HTML-safe
    const p2MentionHTML = escapeHTML(p2.mention);
    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(gameData.betAmount, 'USD'));

    let titleTextHTML = `⚔️ <b>High Stakes Duel</b> ⚔️`;
    let messageTextHTML = `${titleTextHTML}\n`;
    messageTextHTML += `<i>${p1MentionHTML} vs ${p2MentionHTML}</i>\n`;
    messageTextHTML += `<b>Wager</b>: ${betDisplayHTML} each\n\n`;

    // Player 1 Status Display
    messageTextHTML += `👤 <b>${p1MentionHTML}</b> (P1):\n   `;
    if (p1.status === 'rolls_complete') {
        messageTextHTML += `Rolled: ${formatDiceRolls(p1.rolls)} ➠ Total: <b>${p1.score}</b> ✅`;
    } else if (p1.status === 'awaiting_roll_emoji') { // This status is set on the player object
        if (p1.rolls.length === 0) {
            messageTextHTML += `<i>Awaiting ${DUEL_DICE_COUNT} dice...</i>`;
        } else if (p1.rolls.length < DUEL_DICE_COUNT) {
            messageTextHTML += `Rolled: ${formatDiceRolls(p1.rolls)} 🎲\n   <i>Awaiting ${DUEL_DICE_COUNT - p1.rolls.length} more ${DUEL_DICE_COUNT - p1.rolls.length === 1 ? "die" : "dice"}...</i>`;
        } else { // Should not happen if rolls_complete is set correctly
            messageTextHTML += `<i>Processing rolls...</i>`;
        }
    } else if (p1.status === 'waiting_turn') {
        messageTextHTML += `<i>Waiting for turn...</i>`;
    } else if (p1.busted) { // If Duel had a bust concept
        messageTextHTML += `Rolled: ${formatDiceRolls(p1.rolls)} ➠ Total: <b>${p1.score}</b> 💥 BUSTED`;
    } else {
         messageTextHTML += `<i>${escapeHTML(String(p1.status || 'Thinking...').replace(/_/g, ' '))}</i>`;
    }
    messageTextHTML += `\n`;

    // Player 2 Status Display
    messageTextHTML += `👤 <b>${p2MentionHTML}</b> (P2):\n   `;
    if (p2.status === 'rolls_complete') {
        messageTextHTML += `Rolled: ${formatDiceRolls(p2.rolls)} ➠ Total: <b>${p2.score}</b> ✅`;
    } else if (p2.status === 'awaiting_roll_emoji') {
        if (p2.rolls.length === 0) {
            messageTextHTML += `<i>Awaiting ${DUEL_DICE_COUNT} dice...</i>`;
        } else if (p2.rolls.length < DUEL_DICE_COUNT) {
            messageTextHTML += `Rolled: ${formatDiceRolls(p2.rolls)} 🎲\n   <i>Awaiting ${DUEL_DICE_COUNT - p2.rolls.length} more ${DUEL_DICE_COUNT - p2.rolls.length === 1 ? "die" : "dice"}...</i>`;
        } else {
            messageTextHTML += `<i>Processing rolls...</i>`;
        }
    } else if (p2.status === 'waiting_turn') {
        messageTextHTML += `<i>Waiting for turn...</i>`;
    } else if (p2.busted) {
        messageTextHTML += `Rolled: ${formatDiceRolls(p2.rolls)} ➠ Total: <b>${p2.score}</b> 💥 BUSTED`;
    } else {
        messageTextHTML += `<i>${escapeHTML(String(p2.status || 'Thinking...').replace(/_/g, ' '))}</i>`;
    }
    messageTextHTML += `\n\n`;

    // Turn Prompt Logic - Relies on gameData.status and player.isTurn being correctly set
    let actionPromptHTML = "";
    const activePlayer = p1.isTurn ? p1 : (p2.isTurn ? p2 : null);
    const activePlayerMentionHTML = activePlayer ? escapeHTML(activePlayer.mention) : "";

    if (activePlayer && activePlayer.status === 'awaiting_roll_emoji') {
        const diceRolledCount = activePlayer.rolls.length;
        const diceRemaining = DUEL_DICE_COUNT - diceRolledCount;

        if (gameData.status === 'p1_awaiting_roll1_emoji' && p1.isTurn && diceRolledCount === 0) {
            actionPromptHTML = `👉 <b>${p1MentionHTML}</b>, it's your turn! Please send your <b>first</b> of ${DUEL_DICE_COUNT} 🎲 dice emojis.`;
        } else if (gameData.status === 'p1_awaiting_roll2_emoji' && p1.isTurn && diceRolledCount === 1) {
            actionPromptHTML = `👉 <b>${p1MentionHTML}</b>, please send your <b>second</b> 🎲 dice emoji.`;
        } else if (gameData.status === 'p2_awaiting_roll1_emoji' && p2.isTurn && diceRolledCount === 0) {
            actionPromptHTML = `👉 <b>${p2MentionHTML}</b>, it's your turn! Please send your <b>first</b> of ${DUEL_DICE_COUNT} 🎲 dice emojis.`;
        } else if (gameData.status === 'p2_awaiting_roll2_emoji' && p2.isTurn && diceRolledCount === 1) {
            actionPromptHTML = `👉 <b>${p2MentionHTML}</b>, please send your <b>second</b> 🎲 dice emoji.`;
        } else if (diceRemaining > 0) { // Generic fallback prompt if main game status isn't perfectly aligned
            actionPromptHTML = `👉 <b>${activePlayerMentionHTML}</b>, please send <b>${diceRemaining} more</b> 🎲 dice emoji${diceRemaining > 1 ? 's' : ''}.`;
        }
    } else if (gameData.status.startsWith('game_over_')) {
        actionPromptHTML = `<i>🏁 Game concluding... Final results incoming!</i>`;
    } else if (!p1.isTurn && !p2.isTurn && p1.status === 'rolls_complete' && p2.status === 'waiting_turn') {
         actionPromptHTML = `<i>Waiting for ${p2MentionHTML} to begin their turn...</i>`;
    } else if (!p1.isTurn && !p2.isTurn && p2.status === 'rolls_complete' && p1.status === 'waiting_turn') {
         actionPromptHTML = `<i>Waiting for ${p1MentionHTML} to begin their turn...</i>`;
    } else if (!p1.isTurn && !p2.isTurn && p1.status !== 'rolls_complete' && p2.status !== 'rolls_complete') {
        // This case could happen if something went wrong with turn setting
        actionPromptHTML = `<i>Waiting for players to roll...</i>`;
    }


    if (actionPromptHTML) {
        messageTextHTML += `${actionPromptHTML}`;
    }

    const options = {
        chat_id: gameData.chatId,
        parse_mode: 'HTML',
        reply_markup: { inline_keyboard: [] } // No buttons during emoji input phase for Duel PvP
    };

    let sentMessage;
    // Always delete old and send new for simplicity and to ensure it's at the bottom
    if (gameData.currentMessageId) {
        await bot.deleteMessage(gameData.chatId, Number(gameData.currentMessageId)).catch(()=>{});
        gameData.currentMessageId = null;
    }
    
    sentMessage = await safeSendMessage(gameData.chatId, messageTextHTML, options);

    if (sentMessage?.message_id) {
        gameData.currentMessageId = String(sentMessage.message_id);
        if(activeGames.has(gameId)) activeGames.set(gameId, gameData);
    } else {
        console.error(`[UpdateDuelPvPMsg_HTML_V9] Failed to send/update Duel PvP message for GID ${gameId}.`);
    }
}

async function getTwoDiceRollsViaHelperDuel(gameId, chatIdForLog, userIdForRoll, rollPurposeNotePrefix) {
    const rolls = [];
    let anErrorOccurred = null;
    for (let i = 0; i < DUEL_DICE_COUNT; i++) { // DUEL_DICE_COUNT is expected to be 2
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
    if (gameData.status === 'player_awaiting_roll1_emoji') {
        gameData._internalTempRoll1 = diceValue;
        gameData.status = 'player_awaiting_roll2_emoji';
        activeGames.set(gameData.gameId, gameData);
        // Update message to prompt for second die
        const playerRef = getPlayerDisplayReference(gameData.userObj);
        const betDisplayUSD = escapeMarkdownV2(await formatBalanceForDisplay(gameData.betAmount, 'USD'));
        let messageUpdate = `⚔️ **Duel vs. Bot Dealer!** ⚔️\n\n` +
                            `${playerRef}, your wager: *${betDisplayUSD}*.\n` +
                            `You rolled your first die: ${formatDiceRolls([diceValue])}\n` +
                            `Please send your **second** 🎲 dice emoji.`;
        if (gameData.gameMessageId && bot) {
            bot.editMessageText(messageUpdate, { chat_id: gameData.chatId, message_id: Number(gameData.gameMessageId), parse_mode: 'MarkdownV2' }).catch(()=>{});
        } else {
            const newMsg = await safeSendMessage(gameData.chatId, messageUpdate, {parse_mode: 'MarkdownV2'});
            if(newMsg?.message_id) gameData.gameMessageId = newMsg.message_id;
        }

    } else if (gameData.status === 'player_awaiting_roll2_emoji') {
        const firstRoll = gameData._internalTempRoll1;
        delete gameData._internalTempRoll1;
        await processDuelPlayerRollsCompletePvB(gameData, firstRoll, diceValue);
    } else {
        console.warn(`${logPrefix} Emoji roll received in unexpected PvB status: ${gameData.status}`);
    }
}

async function processDuelPvPRollByEmoji(gameData, diceValue, rollerUserId) {
    const logPrefix = `[Duel_PvP_EmojiProc GID:${gameData.gameId} Roller:${rollerUserId}]`;
    let currentPlayer, playerKey, otherPlayerStatusCheck;

    if (gameData.initiator.userId === rollerUserId && gameData.initiator.isTurn) {
        currentPlayer = gameData.initiator; playerKey = 'p1'; otherPlayerStatusCheck = gameData.opponent;
    } else if (gameData.opponent.userId === rollerUserId && gameData.opponent.isTurn) {
        currentPlayer = gameData.opponent; playerKey = 'p2'; otherPlayerStatusCheck = gameData.initiator;
    } else {
        console.warn(`${logPrefix} Roll from non-active player or wrong turn.`);
        return;
    }

    if (currentPlayer.status !== 'awaiting_roll_emoji') {
        console.warn(`${logPrefix} Player ${currentPlayer.mention} not in 'awaiting_roll_emoji' state (is ${currentPlayer.status}). Ignoring dice.`);
        return;
    }

    currentPlayer.rolls.push(diceValue);
    gameData.lastInteractionTime = Date.now(); // Update interaction time

    if (currentPlayer.rolls.length < DUEL_DICE_COUNT) {
        gameData.status = `${playerKey}_awaiting_roll${currentPlayer.rolls.length + 1}_emoji`; // e.g. p1_awaiting_roll2_emoji
        // player.status remains 'awaiting_roll_emoji'
        console.log(`${logPrefix} Player ${currentPlayer.mention} rolled ${diceValue} (${currentPlayer.rolls.length}/${DUEL_DICE_COUNT}). Awaiting next die.`);
        await updateDuelPvPMessage(gameData.gameId); // Update message to show first roll and prompt for second
    } else { // Both dice for the current player have been rolled
        console.log(`${logPrefix} Player ${currentPlayer.mention} rolled their second die: ${diceValue}. Processing turn completion.`);
        await processDuelPlayerRollsCompletePvP(gameData, currentPlayer.rolls[0], currentPlayer.rolls[1], rollerUserId);
    }
    activeGames.set(gameData.gameId, gameData);
}
// --- End of Part 5c, Section 2 (COMPLETE REWRITE FOR NEW DUEL GAME LOGIC - CONSOLIDATED UPDATES) ---
// --- Start of Part 5c, Section 3 (NEW) - Segment 1 & 2 (FULLY UPDATED FOR HELPER BOT DICE ROLLS for Ladder, Animated for SevenOut - SEVENOUT REPLACED WITH LUCKY SUM - FIXES APPLIED) ---
// index.js - Part 5c, Section 3: Greed's Ladder & Sevens Out (Lucky Sum Variant) Game Logic & Handlers
//----------------------------------------------------------------------------------------------------
// Assumed dependencies from previous Parts

// --- Greed's Ladder Game Logic (UNCHANGED) ---

async function handleStartLadderCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_LADDER_START = `[Ladder_Start_HTML_V4_DeleteSend UID:${userId} CH:${chatId}]`; // Updated log prefix

    console.log(`${LOG_PREFIX_LADDER_START} Function called. Bet: ${betAmountLamports}`); 

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_LADDER_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🪜 Oh dear! The wager for Greed's Ladder seems incorrect.<br>Please try again with a valid amount.", { parse_mode: 'HTML' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        console.error(`${LOG_PREFIX_LADDER_START} Failed to get/create user.`); 
        await safeSendMessage(chatId, "😕 Greetings, climber! We couldn't find your adventurer profile for Greed's Ladder.<br>Please try <code>/start</code> again.", { parse_mode: 'HTML' });
        return;
    }
    console.log(`${LOG_PREFIX_LADDER_START} User obtained. Initiating Greed's Ladder. Bet: ${betAmountLamports}`); 

    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObj));
    let betDisplayUSD_HTML; 
    try {
        betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));
        console.log(`${LOG_PREFIX_LADDER_START} betDisplayUSD_HTML constructed: ${betDisplayUSD_HTML}`); 
    } catch (e) {
        console.error(`${LOG_PREFIX_LADDER_START} CRITICAL Error constructing betDisplayUSD_HTML: ${e.message}`, e); 
        await safeSendMessage(chatId, "⚙️ Error preparing game display (price feed issue?). Please try again.", { parse_mode: 'HTML' });
        return;
    }

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplayHTML = escapeHTML(await formatBalanceForDisplay(needed, 'USD')); 
        console.log(`${LOG_PREFIX_LADDER_START} Insufficient balance.`); 
        await safeSendMessage(chatId, `${playerRefHTML}, your treasure chest is a bit light for the <b>${betDisplayUSD_HTML}</b> climb on Greed's Ladder! You'll need about <b>${neededDisplayHTML}</b> more. Fortify your reserves?`, {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: "💰 Add Funds (DM)", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]] }
        });
        return;
    }

    const gameId = generateGameId(GAME_IDS.LADDER);
    let client = null;
    try {
        console.log(`${LOG_PREFIX_LADDER_START} Attempting to place bet in DB. GameID: ${gameId}`); 
        client = await pool.connect();
        await client.query('BEGIN');
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, BigInt(-betAmountLamports),
            'bet_placed_ladder', { game_id_custom_field: gameId },
            `Bet for Greed's Ladder game ${gameId}`
        );

        if (!balanceUpdateResult || !balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${LOG_PREFIX_LADDER_START} Wager placement failed in DB: ${balanceUpdateResult.error}`); 
            await safeSendMessage(chatId, `${playerRefHTML}, your Greed's Ladder wager of <b>${betDisplayUSD_HTML}</b> failed to post: <code>${escapeHTML(balanceUpdateResult.error || "Wallet error")}</code>. Please try again.`, { parse_mode: 'HTML' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports;
        console.log(`${LOG_PREFIX_LADDER_START} Bet placed successfully.`); 
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_LADDER_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_LADDER_START} Database error during Greed's Ladder bet processing: ${dbError.message}`, dbError.stack?.substring(0,500)); 
        await safeSendMessage(chatId, "⚙️ The Ladder's foundations seem shaky (database error)! Failed to start. Please try again.", { parse_mode: 'HTML' });
        return;
    } finally {
        if (client) client.release();
    }

    const gameData = {
        type: GAME_IDS.LADDER, gameId, chatId, userId, playerRef: playerRefHTML,
        userObj, betAmount: betAmountLamports, rolls: [], sum: 0n, status: 'rolling_waiting_helper', gameMessageId: null
    };
    activeGames.set(gameId, gameData);
    console.log(`${LOG_PREFIX_LADDER_START} Game data set in activeGames. Status: ${gameData.status}`); 

    const titleSpinningHTML = `🪜 <b>Greed's Ladder - The Climb Begins!</b> 🪜`;
    let messageTextHTML_Spinning = `${titleSpinningHTML}\n\n${playerRefHTML} wagers <b>${betDisplayUSD_HTML}</b> and steps onto Greed's Ladder!\nRequesting <b>${escapeHTML(String(LADDER_ROLL_COUNT))} dice</b> from the Helper Bot... This may take a moment! 🎲⏳`;
    console.log(`${LOG_PREFIX_LADDER_START} Attempting to send 'Climb Begins' message.`); 

    const sentRollingMsg = await safeSendMessage(chatId, messageTextHTML_Spinning, {parse_mode: 'HTML'});
    if (sentRollingMsg?.message_id) {
        gameData.gameMessageId = sentRollingMsg.message_id; // This is the ID of the "Climb Begins!" message
        activeGames.set(gameId, gameData);
        console.log(`${LOG_PREFIX_LADDER_START} 'Climb Begins' message sent. Msg ID: ${gameData.gameMessageId}`); 
    } else {
        console.error(`${LOG_PREFIX_LADDER_START} CRITICAL: Failed to send initial 'Climb Begins!' message for ${gameId}. Refunding wager.`); 
        let refundClient = null; // Refund logic...
        try { /* ... */ } finally { /* ... */ }
        activeGames.delete(gameId);
        return;
    }

    let diceRolls = [];
    let helperBotError = null;
    let isBust = false; 
    console.log(`${LOG_PREFIX_LADDER_START} Starting dice roll loop for ${LADDER_ROLL_COUNT} rolls.`); 

    for (let i = 0; i < LADDER_ROLL_COUNT; i++) {
        // ... (dice rolling logic with instant bust as previously provided) ...
        console.log(`${LOG_PREFIX_LADDER_START} Requesting roll ${i + 1}/${LADDER_ROLL_COUNT}.`); 
        if (isShuttingDown) { helperBotError = "Shutdown during Ladder dice requests."; console.log(`${LOG_PREFIX_LADDER_START} Shutdown detected during dice roll loop.`); break; } 
        const rollResult = await getSingleDiceRollViaHelper(gameId, chatId, userId, `Ladder Roll ${i+1}`);
        if (rollResult.error) {
            helperBotError = rollResult.message || `Failed to get Ladder Roll ${i+1}`;
            console.error(`${LOG_PREFIX_LADDER_START} Helper Bot error on roll ${i + 1}: ${helperBotError}`); 
            break; 
        }
        diceRolls.push(rollResult.roll);
        console.log(`${LOG_PREFIX_LADDER_START} Roll ${i + 1} received: ${rollResult.roll}`); 

        if (rollResult.roll === LADDER_BUST_ON) {
            console.log(`${LOG_PREFIX_LADDER_START} Bust roll of ${LADDER_BUST_ON} detected on roll ${i + 1}. Game ends now.`); 
            isBust = true; 
            break; 
        }
    }
    gameData.rolls = diceRolls; 
    gameData.sum = BigInt(diceRolls.reduce((sum, val) => sum + val, 0));

    if (helperBotError || (!isBust && gameData.rolls.length !== LADDER_ROLL_COUNT)) {
        // ... (error handling for helper bot failure as previously provided, including refund) ...
        console.error(`${LOG_PREFIX_LADDER_START} Helper Bot error or incorrect roll count (and not a player bust). Error: ${helperBotError}, Rolls: ${gameData.rolls.length}`); 
        const errorMsgToUserHTML = `⚠️ ${playerRefHTML}, there was an issue getting your dice rolls for Greed's Ladder: <code>${escapeHTML(String(helperBotError || "Incomplete rolls from helper").substring(0,150))}</code><br>Your bet of <b>${betDisplayUSD_HTML}</b> has been refunded.`;
        // Try to delete the "rolling..." message if it exists
        if (gameData.gameMessageId && bot) {
            await bot.deleteMessage(String(chatId), Number(gameData.gameMessageId)).catch(e => console.warn(`${LOG_PREFIX_LADDER_START} Non-critical: Failed to delete old message ${gameData.gameMessageId} on helper error. Error: ${e.message}`));
        }
        await safeSendMessage(String(chatId), errorMsgToUserHTML, { parse_mode: 'HTML', reply_markup: createPostGameKeyboard(GAME_IDS.LADDER, betAmountLamports) });
        // ... (actual refund DB logic) ...
        let refundClient = null; 
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_ladder_helper_fail', {game_id_custom_field: gameId}, `Refund Ladder game ${gameId} - Helper Bot error`);
            await refundClient.query('COMMIT');
            console.log(`${LOG_PREFIX_LADDER_START} Refund processed for helper bot error.`);
        } catch (dbErr) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_LADDER_START} CRITICAL: Refund FAILED after helper error for game ${gameId}: ${dbErr.message}`);
        } finally { if (refundClient) refundClient.release(); }
        activeGames.delete(gameId);
        return;
    }

    console.log(`${LOG_PREFIX_LADDER_START} All dice rolls processed (or bust occurred). Final rolls: ${gameData.rolls.join(', ')}, Sum: ${gameData.sum}, isBust: ${isBust}`); 

    let payoutAmountLamports = 0n;
    let outcomeReasonLog = "";
    let resultTextPartHTML = "";

    const titleResultHTML = `🏁 <b>Greed's Ladder - The Outcome!</b> 🏁`;
    let finalMessageTextHTML; 
    try {
        finalMessageTextHTML = `${titleResultHTML}\n\n${playerRefHTML}'s wager: <b>${betDisplayUSD_HTML}</b>\nThe Helper Bot delivered dice: ${formatDiceRolls(gameData.rolls)}\nTotal Sum: <b>${escapeHTML(String(gameData.sum))}</b>\n\n`;
        console.log(`${LOG_PREFIX_LADDER_START} Initial part of final message constructed.`); 
    } catch (e) {
        console.error(`${LOG_PREFIX_LADDER_START} ERROR constructing initial part of finalMessageTextHTML: ${e.message}`, e); 
        if (gameData.gameMessageId && bot) await bot.deleteMessage(String(chatId), Number(gameData.gameMessageId)).catch(()=>{});
        await safeSendMessage(chatId, "⚙️ Critical error preparing Greed's Ladder result display. Please contact support.", {parse_mode: 'HTML'});
        activeGames.delete(gameId);
        return;
    }

    if (isBust) { 
        outcomeReasonLog = `loss_ladder_bust_r${LADDER_BUST_ON}`;
        resultTextPartHTML = `💥 <b>CRASH! A ${escapeHTML(String(LADDER_BUST_ON))} appeared!</b> 💥\nYou've tumbled off Greed's Ladder! Your wager is lost.`;
        gameData.status = 'game_over_player_bust';
    } else { 
        // ... (payout tier logic as previously provided) ...
        let foundPayout = false;
        for (const payoutTier of LADDER_PAYOUTS) {
            if (gameData.sum >= payoutTier.min && gameData.sum <= payoutTier.max) {
                const profitLamports = betAmountLamports * BigInt(payoutTier.multiplier);
                payoutAmountLamports = betAmountLamports + profitLamports;
                outcomeReasonLog = `win_ladder_s${gameData.sum}_m${payoutTier.multiplier}`;
                try {
                    resultTextPartHTML = `${escapeHTML(payoutTier.label)} You've reached a high rung and won <b>${escapeHTML(await formatBalanceForDisplay(profitLamports, 'USD'))}</b> in profit!`;
                    console.log(`${LOG_PREFIX_LADDER_START} Payout tier found: ${payoutTier.label}`); 
                } catch (e) {
                    console.error(`${LOG_PREFIX_LADDER_START} ERROR in formatBalanceForDisplay for profit: ${e.message}`, e); 
                    resultTextPartHTML = `${escapeHTML(payoutTier.label)} You've reached a high rung! (Error displaying profit in USD, SOL value: ${escapeHTML(formatCurrency(profitLamports, 'SOL'))})`;
                }
                foundPayout = true;
                break;
            }
        }
        if (!foundPayout) {
            outcomeReasonLog = 'loss_ladder_no_tier';
            resultTextPartHTML = "😐 A cautious climb... but not high enough for a prize this time. Your wager is lost.";
            console.log(`${LOG_PREFIX_LADDER_START} No payout tier met.`);
        }
        gameData.status = 'game_over_resolved';
    }
    finalMessageTextHTML += resultTextPartHTML;
    console.log(`${LOG_PREFIX_LADDER_START} Final message content constructed (before DB): ${finalMessageTextHTML.substring(0, 250)}...`); 

    let clientOutcome = null;
    // ... (DB update logic as previously provided) ...
    try {
        console.log(`${LOG_PREFIX_LADDER_START} Starting DB update for game outcome.`); 
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const ledgerNotes = `Greed's Ladder: Sum ${gameData.sum}, Rolls ${gameData.rolls.join(',')}. Outcome: ${outcomeReasonLog}. GameID: ${gameId}`;
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, userId, payoutAmountLamports,
            outcomeReasonLog, 
            { game_id_custom_field: gameId }, 
            ledgerNotes
        );

        if (balanceUpdate.success) {
            await clientOutcome.query('COMMIT');
            console.log(`${LOG_PREFIX_LADDER_START} DB update successful.`);
        } else {
            await clientOutcome.query('ROLLBACK');
            const dbFailText = `\n\n⚠️ A critical error occurred settling your Ladder game: <code>${escapeHTML(balanceUpdate.error || "DB Error")}</code>. Casino staff notified.`;
            finalMessageTextHTML += dbFailText;
            console.error(`${LOG_PREFIX_LADDER_START} Failed to update balance for Ladder game ${gameId}. DB Error: ${balanceUpdate.error}`); 
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL LADDER Payout Failure 🚨\nGame ID: <code>${escapeHTML(gameId)}</code> User: ${playerRefHTML}\nAmount: <code>${formatCurrency(payoutAmountLamports)}</code>\nDB Error: <code>${escapeHTML(balanceUpdate.error || "N/A")}</code>. Manual check needed.`, {parse_mode:'HTML'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        const dbCatchFailText = `\n\n⚠️ A severe database error occurred resolving your climb. Casino staff notified.`;
        finalMessageTextHTML += dbCatchFailText;
        console.error(`${LOG_PREFIX_LADDER_START} DB CATCH block error during Ladder outcome for ${gameId}: ${dbError.message}`, dbError.stack?.substring(0,500));
    } finally {
        if (clientOutcome) clientOutcome.release();
    }
    console.log(`${LOG_PREFIX_LADDER_START} DB operations complete. Preparing to display final result.`); // Changed log slightly

    const postGameKeyboardLadder = createPostGameKeyboard(GAME_IDS.LADDER, betAmountLamports);

    // --- NEW FINAL MESSAGE SENDING STRATEGY ---
    // 1. Delete the old "Climb Begins!" message if its ID exists
    if (gameData.gameMessageId && bot) {
        console.log(`${LOG_PREFIX_LADDER_START} Attempting to DELETE old message ID: ${gameData.gameMessageId}`);
        await bot.deleteMessage(String(chatId), Number(gameData.gameMessageId))
            .catch(e => console.warn(`${LOG_PREFIX_LADDER_START} Non-critical: Failed to delete old message ${gameData.gameMessageId}. Error: ${e.message}`));
    } else {
        console.log(`${LOG_PREFIX_LADDER_START} No old gameMessageId to delete, or bot instance missing.`);
    }

    // 2. Always send the final result as a new message
    console.log(`${LOG_PREFIX_LADDER_START} Attempting to SEND final result as new message.`);
    const sentFinalResult = await safeSendMessage(String(chatId), finalMessageTextHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboardLadder });

    if (sentFinalResult && sentFinalResult.message_id) {
        console.log(`${LOG_PREFIX_LADDER_START} Successfully SENT final result. New Msg ID: ${sentFinalResult.message_id}`);
    } else {
        console.error(`${LOG_PREFIX_LADDER_START} CRITICAL: Failed to SEND final result message after all processing.`);
        if(typeof notifyAdmin === 'function') {
            notifyAdmin(`🚨 CRITICAL LADDER - FINAL MESSAGE SEND FAILED 🚨\nGame ID: ${gameId}\nUser: ${playerRefHTML}\nIntended message (start): ${finalMessageTextHTML.substring(0, 200)}...`, {parse_mode: 'HTML'});
        }
    }
    // --- END OF NEW FINAL MESSAGE SENDING STRATEGY ---

    activeGames.delete(gameId);
    console.log(`${LOG_PREFIX_LADDER_START} Game ${gameId} removed from activeGames. Function end.`);
}


// --- Sevens Out (Lucky Sum Variant) Game Logic ---

const LUCKY_SUM_PAYOUTS = {
    2: { multiplier: 2, label: "Snake Eyes! 🐍" },
    12: { multiplier: 2, label: "Boxcars! 🚂" },
    3: { multiplier: 1, label: "Easy Three!" },
    4: { multiplier: 1, label: "Fever Four!" },
    9: { multiplier: 1, label: "Nina Nine!" },
    10: { multiplier: 1, label: "Big Ten!" },
    11: { multiplier: 1, label: "Yo Eleven!" },
};
const LUCKY_SUM_LOSING_NUMBERS = [5, 6, 7, 8];

async function handleStartSevenOutCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_S7_LUCKY_SUM_START = `[S7_LuckySum_Start_HTML_Fix UID:${userId} CH:${chatId}]`;

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        await safeSendMessage(chatId, "🎲 Invalid bet for Lucky Sum! Please use a valid amount.", { parse_mode: 'HTML' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Couldn't find your player profile for Lucky Sum. Try <code>/start</code>.", { parse_mode: 'HTML' });
        return;
    }

    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObj));
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        await safeSendMessage(chatId, `${playerRefHTML}, your balance is too low for a <b>${betDisplayUSD_HTML}</b> Lucky Sum game. You need ~<b>${escapeHTML(await formatBalanceForDisplay(needed, 'USD'))}</b> more.`, {
            parse_mode: 'HTML',
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
            'bet_placed_s7_luckysum', { game_id_custom_field: gameId },
            `Bet for Lucky Sum (S7) game ${gameId}`
        );

        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            await safeSendMessage(chatId, `${playerRefHTML}, your Lucky Sum wager of <b>${betDisplayUSD_HTML}</b> failed: <code>${escapeHTML(balanceUpdateResult.error || "DB Error")}</code>.`, { parse_mode: 'HTML' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports;
    } catch (dbError) {
        if (client) await client.query('ROLLBACK');
        console.error(`${LOG_PREFIX_S7_LUCKY_SUM_START} DB error: ${dbError.message}`);
        await safeSendMessage(chatId, "⚙️ Database error starting Lucky Sum. Please try again.", { parse_mode: 'HTML' });
        return;
    } finally {
        if (client) client.release();
    }

    const gameData = {
        type: GAME_IDS.SEVEN_OUT, gameId, chatId, userId, playerRef: playerRefHTML, userObj,
        betAmount: betAmountLamports, rolls: [], currentSum: 0n,
        status: 'awaiting_single_roll',
        gameMessageIdToDelete: null,
        lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);

    const titleHTML = `🎲 <b>Lucky Sum Roll!</b> 🎲`;
    const initialMessageTextHTML = `${titleHTML}\n\n${playerRefHTML}, your bet of <b>${betDisplayUSD_HTML}</b> is on the line for a game of Lucky Sum!\n\nI'll roll two dice for you now... Good luck! 🍀`;

    const sentInitialMsg = await safeSendMessage(chatId, initialMessageTextHTML, {parse_mode: 'HTML'});
    if (sentInitialMsg?.message_id) {
        gameData.gameMessageIdToDelete = sentInitialMsg.message_id;
        activeGames.set(gameId, gameData);
        await processSevenOutRoll(gameData);
    } else {
        console.error(`${LOG_PREFIX_S7_LUCKY_SUM_START} Failed to send initial Lucky Sum message. Refunding.`);
        let refundClient;
        try {
            refundClient = await pool.connect(); await refundClient.query('BEGIN');
            await updateUserBalanceAndLedger(refundClient, userId, betAmountLamports, 'refund_s7_luckysum_setup_fail', {}, `Refund S7_LuckySum ${gameId}`);
            await refundClient.query('COMMIT');
        } catch (err) {
            if (refundClient) await refundClient.query('ROLLBACK');
            console.error(`${LOG_PREFIX_S7_LUCKY_SUM_START} CRITICAL: Refund failed for ${gameId}: ${err.message}`);
        } finally { if (refundClient) refundClient.release(); }
        activeGames.delete(gameId);
    }
}

async function processSevenOutRoll(gameData) {
    const { gameId, userId, chatId, playerRef, betAmount } = gameData;
    const LOG_PREFIX_S7_LUCKY_ROLL = `[S7_LuckySum_Roll_V4_Fix GID:${gameId} UID:${userId}]`;

    if (gameData.status !== 'awaiting_single_roll') {
        console.warn(`${LOG_PREFIX_S7_LUCKY_ROLL} Invalid game state: ${gameData.status}. Expected 'awaiting_single_roll'.`);
        return;
    }

    gameData.status = 'processing_roll';
    activeGames.set(gameId, gameData);

    if (gameData.gameMessageIdToDelete && bot) {
        await bot.deleteMessage(chatId, Number(gameData.gameMessageIdToDelete)).catch(e => {
            console.warn(`${LOG_PREFIX_S7_LUCKY_ROLL} Non-critical: Could not delete previous message ID ${gameData.gameMessageIdToDelete}: ${e.message}`);
        });
        gameData.gameMessageIdToDelete = null;
    }
    
    const rollingMessageHTML = `🎲 ${playerRef} rolling for Lucky Sum... The dice are tumbling! 🌪️`;
    const tempRollingMsg = await safeSendMessage(chatId, rollingMessageHTML, { parse_mode: 'HTML' });
    await sleep(1000); 

    let currentRolls = [];
    let currentSum = 0;
    let animatedDiceMessageIds = [];

    for (let i = 0; i < 2; i++) {
        try {
            const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
            currentRolls.push(diceMsg.dice.value);
            currentSum += diceMsg.dice.value;
            if (diceMsg.message_id) animatedDiceMessageIds.push(diceMsg.message_id);
            await sleep(2200);
        } catch (e) {
            console.warn(`${LOG_PREFIX_S7_LUCKY_ROLL} Failed to send animated dice (Roll ${i+1}), using internal roll. Error: ${e.message}`);
            const internalRollVal = rollDie();
            currentRolls.push(internalRollVal);
            currentSum += internalRollVal;
            const fallbackMsg = await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Casino's Internal Dice Roll ${i + 1}): A <b>${internalRollVal}</b> 🎲 appears!`, { parse_mode: 'HTML' });
            if (fallbackMsg?.message_id) animatedDiceMessageIds.push(fallbackMsg.message_id);
            await sleep(500);
        }
    }

    if (tempRollingMsg?.message_id && bot) {
        await bot.deleteMessage(chatId, tempRollingMsg.message_id).catch(() => {});
    }
    for (const id of animatedDiceMessageIds) {
        if (bot) await bot.deleteMessage(String(chatId), id).catch(() => {});
    }
    
    gameData.rolls = currentRolls;
    gameData.currentSum = BigInt(currentSum);

    let win = false;
    let payoutRule = LUCKY_SUM_PAYOUTS[currentSum];
    let profitMultiplier = 0;
    let outcomeReasonLog = `loss_s7_luckysum_sum${currentSum}`;
    let resultTitleHTML = "";
    let resultDetailsHTML = ""; 
    let financialOutcomeHTML = "";

    let profitAmountLamports = 0n; // Initialize here
    let payoutAmountLamports = 0n; // Initialize here

    const betDisplayUSD_HTML_Result = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'));

    if (payoutRule) { 
        win = true;
        profitMultiplier = payoutRule.multiplier;
        outcomeReasonLog = `win_s7_luckysum_sum${currentSum}_mult${profitMultiplier}`;
        resultTitleHTML = `🎲✨ <b>Lucky Sum - YOU WIN!</b> ✨🎲`;
        resultDetailsHTML = `Your roll sum of <b>${currentSum}</b> hit a lucky spot: ${escapeHTML(payoutRule.label)}`;
        
        profitAmountLamports = betAmount * BigInt(profitMultiplier); 
        payoutAmountLamports = betAmount + profitAmountLamports;    
        
        financialOutcomeHTML = `You won <b>${escapeHTML(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}</b> in profit!\n(Total Payout: <b>${escapeHTML(await formatBalanceForDisplay(payoutAmountLamports, 'USD'))}</b>)`;
    } else { 
        win = false; 
        // payoutAmountLamports and profitAmountLamports remain 0n
        resultTitleHTML = `🎲😥 <b>Lucky Sum - Not This Time!</b> 😥🎲`;
        if (LUCKY_SUM_LOSING_NUMBERS.includes(currentSum)) {
            resultDetailsHTML = `A sum of <b>${currentSum}</b> means the house takes this round.`;
        } else {
            console.error(`${LOG_PREFIX_S7_LUCKY_ROLL} Sum ${currentSum} not in defined win/loss outcomes! Defaulting to loss.`);
            resultDetailsHTML = `Your roll of <b>${currentSum}</b> didn't hit a winning number.`;
        }
        financialOutcomeHTML = `Your wager of <b>${betDisplayUSD_HTML_Result}</b> is lost. Better luck next time! 🍀`;
    }
    
    gameData.status = 'game_over';
    activeGames.set(gameId, gameData); 

    let messageToPlayerHTML = `${resultTitleHTML}\n<pre>==============================</pre>\n` +
                             `Player: <b>${playerRef}</b>\nWager: <b>${betDisplayUSD_HTML_Result}</b>\n` +
                             `<pre>------------------------------</pre>\n` +
                             `Dice Rolled: ${formatDiceRolls(currentRolls)} ➠ Sum: <b>${currentSum}</b>!\n` +
                             `<pre>==============================</pre>\n` +
                             `${resultDetailsHTML}\n${financialOutcomeHTML}`;

    await finalizeSevenOutGame(gameData, messageToPlayerHTML, payoutAmountLamports, outcomeReasonLog);
}


async function finalizeSevenOutGame(gameData, resultMessageHTML, payoutAmountLamports, outcomeReasonLog) {
    const { gameId, chatId, userId, playerRef, betAmount, userObj } = gameData;
    const LOG_PREFIX_S7_FINALIZE_LUCKY = `[S7_LuckySum_Finalize_V3_Fix GID:${gameId} UID:${userId}]`; // V3 for fix
    let finalUserBalanceLamports = BigInt(userObj.balance);
    let clientOutcome;
    let dbErrorText = "";

    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        // --- FIX: Shortened ledgerReason for transaction_type column ---
        const ledgerReasonForTransactionType = outcomeReasonLog.length > 50 ? outcomeReasonLog.substring(0, 47) + "..." : outcomeReasonLog;
        const fullNotesForLedger = `${outcomeReasonLog} (Game ID: ${gameId})`; // Keep full detail in notes
        // --- END FIX ---

        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, userId, payoutAmountLamports,
            ledgerReasonForTransactionType, // Use shortened version here
            { game_id_custom_field: gameId, dice_rolls_s7_luckysum: gameData.rolls.join(','), sum_s7_luckysum: gameData.currentSum.toString() },
            fullNotesForLedger // Full details in notes
        );

        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
        } else {
            await clientOutcome.query('ROLLBACK');
            // Use \n for newlines in HTML context, Telegram converts to <br>
            dbErrorText = `\n\n⚠️ A critical casino vault error occurred settling your game: <code>${escapeHTML(balanceUpdate.error || "DB Error")}</code>. Our pit boss has been alerted.`;
            console.error(`${LOG_PREFIX_S7_FINALIZE_LUCKY} Failed to update balance for S7 Lucky Sum game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL S7 LuckySum Payout/Refund Failure 🚨\nGame ID: <code>${escapeHTML(gameId)}</code> User: ${playerRef} (<code>${escapeHTML(userId)}</code>)\nAmount Due: <code>${escapeHTML(formatCurrency(payoutAmountLamports))}</code>\nDB Error: <code>${escapeHTML(balanceUpdate.error || "N/A")}</code>. Manual check required.`, {parse_mode:'HTML'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_S7_FINALIZE_LUCKY} DB error during S7 Lucky Sum outcome for ${gameId}: ${dbError.message}`, dbError.stack?.substring(0,500));
        // Use \n for newlines in HTML context
        dbErrorText = `\n\n⚠️ A major dice table malfunction (database error) occurred. Our pit boss has been notified.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    let finalMessageWithDbStatusHTML = resultMessageHTML + dbErrorText;

    const postGameKeyboardS7 = createPostGameKeyboard(GAME_IDS.SEVEN_OUT, betAmount);
    await safeSendMessage(String(chatId), finalMessageWithDbStatusHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboardS7 });
    activeGames.delete(gameId);
}

// --- End of Part 5c, Section 3 (Ladder UNCHANGED, Sevens Out REPLACED with Lucky Sum - FIXES APPLIED) ---
// index.js - Part 5c, Section 4: Slot Frenzy Game Logic & Callback Router for Part 5c Games
//----------------------------------------------------------------------------------------------------
// Assumed dependencies from previous Parts

// --- Slot Frenzy Game Logic ---

async function handleStartSlotCommand(msg, betAmountLamports) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const LOG_PREFIX_SLOT_START = `[Slot_Start_HTML_NewMsg UID:${userId} CH:${chatId}]`;

    if (typeof betAmountLamports !== 'bigint' || betAmountLamports <= 0n) {
        console.error(`${LOG_PREFIX_SLOT_START} Invalid betAmountLamports: ${betAmountLamports}.`);
        await safeSendMessage(chatId, "🎰 Oh dear! That bet amount for Slot Frenzy doesn't look quite right.<br>Please try again with a valid wager.", { parse_mode: 'HTML' });
        return;
    }

    let userObj = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObj) {
        await safeSendMessage(chatId, "😕 Hey spinner! We couldn't find your player profile for Slot Frenzy.<br>Please hit <code>/start</code> first.", { parse_mode: 'HTML' });
        return;
    }
    // console.log(`${LOG_PREFIX_SLOT_START} Initiating Slot Frenzy. Bet: ${betAmountLamports}`); // Reduced log

    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObj));
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    if (BigInt(userObj.balance) < betAmountLamports) {
        const needed = betAmountLamports - BigInt(userObj.balance);
        const neededDisplayHTML = escapeHTML(await formatBalanceForDisplay(needed, 'USD'));
        await safeSendMessage(chatId, `${playerRefHTML}, your casino wallet needs a bit more sparkle for a <b>${betDisplayUSD_HTML}</b> spin on Slot Frenzy! You're short by about <b>${neededDisplayHTML}</b>. Time to reload?`, {
            parse_mode: 'HTML',
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
            await safeSendMessage(chatId, `${playerRefHTML}, your Slot Frenzy wager of <b>${betDisplayUSD_HTML}</b> jammed: <code>${escapeHTML(balanceUpdateResult.error || "Wallet error")}</code>. Please try spinning again.`, { parse_mode: 'HTML' });
            return;
        }
        await client.query('COMMIT');
        userObj.balance = balanceUpdateResult.newBalanceLamports;
    } catch (dbError) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${LOG_PREFIX_SLOT_START} DB Rollback Error: ${rbErr.message}`));
        console.error(`${LOG_PREFIX_SLOT_START} Database error during Slot Frenzy bet: ${dbError.message}`, dbError.stack?.substring(0,500));
        await safeSendMessage(chatId, "⚙️ The slot machine's gears are stuck (database error)! Failed to start. Please try again.", { parse_mode: 'HTML' });
        return;
    } finally {
        if (client) client.release();
    }

    const gameData = {
        type: GAME_IDS.SLOT_FRENZY, gameId, chatId, userId, playerRef: playerRefHTML,
        userObj, betAmount: betAmountLamports, diceValue: null, payoutInfo: null,
        status: 'spinning_waiting_helper', gameMessageId: null
    };
    activeGames.set(gameId, gameData);

    const titleSpinningHTML = `🎰 <b>Slot Frenzy - Reels in Motion!</b> 🎰`;
    let initialMessageTextHTML = `${titleSpinningHTML}\n\n` +
                                 `Player: <b>${playerRefHTML}</b>\nBet: <b>${betDisplayUSD_HTML}</b>\n\n` +
                                 `Hold tight! The Helper Bot is revving up the Slot Machine! 💨\n`+
                                 `✨ May fortune favor your spin! ✨`;

    const sentSpinningMsg = await safeSendMessage(chatId, initialMessageTextHTML, {parse_mode: 'HTML'});
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

    if (gameData.gameMessageId && bot) {
        await bot.deleteMessage(chatId, Number(gameData.gameMessageId)).catch(e => {
            // console.warn(`${LOG_PREFIX_SLOT_START} Non-critical: Could not delete spinning message ID ${gameData.gameMessageId}: ${e.message}`); // Reduced log
        });
        gameData.gameMessageId = null;
    }

    if (helperBotError || diceRollValue === null) {
        const errorMsgToUserHTML = `💣 <b>Slot Spin Malfunction!</b> 💣\n\n` +
                                 `Oh no, ${playerRefHTML}! It seems the Slot Machine had a hiccup: <pre>${escapeHTML(String(helperBotError || "No result from helper").substring(0,150))}</pre>\n\n` +
                                 `✅ Your bet of <b>${betDisplayUSD_HTML}</b> has been fully refunded.`;
        const errorKeyboard = createPostGameKeyboard(GAME_IDS.SLOT_FRENZY, betAmountLamports);
        await safeSendMessage(String(chatId), errorMsgToUserHTML, { parse_mode: 'HTML', reply_markup: errorKeyboard });

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
    const payoutInfo = SLOT_PAYOUTS[diceRollValue]; // Uses the updated SLOT_PAYOUTS
    gameData.payoutInfo = payoutInfo;
    let payoutAmountLamports = 0n;
    let profitAmountLamports = 0n;
    let outcomeReasonLog = `loss_slot_val${diceRollValue}`;
    let resultTextPartHTML = "";
    let finalTitleHTML = "";
    let finalUserBalanceLamports = userObj.balance;

    if (payoutInfo) {
        profitAmountLamports = betAmountLamports * BigInt(payoutInfo.multiplier);
        payoutAmountLamports = betAmountLamports + profitAmountLamports; // Total returned to player (original bet + profit)
        outcomeReasonLog = `win_slot_val${diceRollValue}_mult${payoutInfo.multiplier}`;
        finalTitleHTML = `🎉🎉 <b>${escapeHTML(payoutInfo.label)}</b> 🎉🎉`;
        resultTextPartHTML = `✨ <b>AMAZING HIT!</b> ✨\n<b>${escapeHTML(payoutInfo.symbols)}</b>\n\n` +
                             `Congratulations! You've won a dazzling <b>${escapeHTML(await formatBalanceForDisplay(profitAmountLamports, 'USD'))}</b> in profit!\n` +
                             `(Total Payout: <b>${escapeHTML(await formatBalanceForDisplay(payoutAmountLamports, 'USD'))}</b>)`;
        gameData.status = 'game_over_win';
    } else {
        payoutAmountLamports = 0n; // Bet is lost
        finalTitleHTML = `😕 <b>Slot Frenzy - No Win This Time</b> 😕`;
        resultTextPartHTML = `Reel Result: <i>Not a winning combination.</i>\n\n` +
                             `The machine keeps your wager of <b>${betDisplayUSD_HTML}</b>.\nBetter luck on the next spin! 🍀`;
        gameData.status = 'game_over_loss';
    }

    // MODIFIED: Removed the "Spin Value (from Helper)" line
    let finalMessageTextHTML = `${finalTitleHTML}\n\n` +
                                 `Player: <b>${playerRefHTML}</b>\nWager: <b>${betDisplayUSD_HTML}</b>\n\n` +
                                 `${resultTextPartHTML}`;

    let clientOutcome = null;
    try {
        clientOutcome = await pool.connect();
        await clientOutcome.query('BEGIN');
        const balanceUpdate = await updateUserBalanceAndLedger(
            clientOutcome, userId, payoutAmountLamports, outcomeReasonLog,
            { game_id_custom_field: gameId, slot_dice_value: diceRollValue },
            `Outcome of Slot Frenzy game ${gameId}. Slot value: ${diceRollValue}.`
        );

        if (balanceUpdate.success) {
            finalUserBalanceLamports = balanceUpdate.newBalanceLamports;
            await clientOutcome.query('COMMIT');
        } else {
            await clientOutcome.query('ROLLBACK');
            finalMessageTextHTML += `\n\n⚠️ A critical error occurred paying out your Slot winnings: <code>${escapeHTML(balanceUpdate.error || "DB Error")}</code>. Casino staff notified.`;
            console.error(`${LOG_PREFIX_SLOT_START} Failed to update balance for Slot game ${gameId}. Error: ${balanceUpdate.error}`);
            if(typeof notifyAdmin === 'function') notifyAdmin(`🚨 CRITICAL SLOT Payout Failure 🚨\nGame ID: <code>${escapeHTML(gameId)}</code> User: ${playerRefHTML}\nAmount: <code>${formatCurrency(payoutAmountLamports)}</code>\nDB Error: <code>${escapeHTML(balanceUpdate.error || "N/A")}</code>. Manual check needed.`, {parse_mode:'HTML'});
        }
    } catch (dbError) {
        if (clientOutcome) await clientOutcome.query('ROLLBACK').catch(()=>{});
        console.error(`${LOG_PREFIX_SLOT_START} DB error during Slot outcome for ${gameId}: ${dbError.message}`, dbError.stack?.substring(0,500));
        finalMessageTextHTML += `\n\n⚠️ A severe database malfunction occurred with the Slot machine. Casino staff notified.`;
    } finally {
        if (clientOutcome) clientOutcome.release();
    }

    const postGameKeyboardSlot = createPostGameKeyboard(GAME_IDS.SLOT_FRENZY, betAmountLamports);
    await safeSendMessage(String(chatId), finalMessageTextHTML, { parse_mode: 'HTML', reply_markup: postGameKeyboardSlot });
    activeGames.delete(gameId);
}

// --- End of Part 5c, Section 4 ---
// --- Start of Part 5d (Mines Game - Delete & Resend Strategy - Full & Corrected for Variable Definitions) ---
// index.js - Part 5d: Mines Game Logic & Callback Handlers
//----------------------------------------------------------------------------------------------------

// ASSUMED GLOBAL DEPENDENCIES (defined in other Parts of your index.js):
// - Constants: GAME_IDS, MINES_DIFFICULTY_CONFIG, MINES_MIN_MINES, TILE_EMOJI_HIDDEN, TILE_EMOJI_GEM, TILE_EMOJI_MINE, TILE_EMOJI_EXPLOSION, RULES_CALLBACK_PREFIX
// - Bot Instance: bot (from node-telegram-bot-api)
// - Database: pool (pg.Pool instance), queryDatabase (if you have a custom wrapper)
// - Utility Functions: safeSendMessage, escapeHTML, formatBalanceForDisplay, formatCurrency, 
//                      getPlayerDisplayReference, generateGameId, createPostGameKeyboard,
//                      getOrCreateUser, updateUserBalanceAndLedger, updateGroupGameDetails
// - Global State: activeGames (Map)

// --- Mines Grid Generation ---
async function generateMinesGridAndData(rows, cols, numMines) {
    const logPrefix = `[GenerateMinesGridAndData ${rows}x${cols}-${numMines}m]`;
    console.log(`${logPrefix} Starting grid generation.`);

    let grid = Array(rows).fill(null).map(() =>
        Array(cols).fill(null).map(() => ({
            isMine: false,
            isRevealed: false, 
            display: TILE_EMOJI_HIDDEN 
        }))
    );

    let minesPlaced = 0;
    const mineLocations = [];
    if (numMines >= rows * cols) {
        console.warn(`${logPrefix} Number of mines (${numMines}) too high for grid ${rows}x${cols}. Clamping.`);
        numMines = Math.max(0, rows * cols - 1); 
    }
    const effectiveMinMines = (typeof MINES_MIN_MINES !== 'undefined') ? MINES_MIN_MINES : 1;
    if (numMines < effectiveMinMines && rows * cols > 0) { 
        console.warn(`${logPrefix} Number of mines (${numMines}) is less than minimum (${effectiveMinMines}). Adjusting.`);
        numMines = Math.min(effectiveMinMines, Math.max(0, rows * cols - 1));
    }

    while (minesPlaced < numMines) {
        const r_coord = Math.floor(Math.random() * rows);
        const c_coord = Math.floor(Math.random() * cols);
        if (!grid[r_coord][c_coord].isMine) {
            grid[r_coord][c_coord].isMine = true;
            mineLocations.push([r_coord, c_coord]);
            minesPlaced++;
        }
    }
    console.log(`${logPrefix} ${minesPlaced} mines placed.`);
    return { grid, mineLocations }; 
}

// --- Definition of calculateMinesMultiplier ---
function calculateMinesMultiplier(gameData, revealedGemsCount) { // Renamed param to revealedGemsCount for clarity
    const { difficultyKey, gameId } = gameData; 
    const difficultyConfig = MINES_DIFFICULTY_CONFIG[difficultyKey]; 

    if (!difficultyConfig || !difficultyConfig.multipliers) {
        console.error(`[MinesCalcMult GID:${gameId || 'N/A'}] Multipliers not found for difficulty ${difficultyKey}`);
        return 0.0; 
    }
    
    if (revealedGemsCount >= 0 && revealedGemsCount < difficultyConfig.multipliers.length) { 
        return difficultyConfig.multipliers[revealedGemsCount] || 0.0;
    } else if (revealedGemsCount >= difficultyConfig.multipliers.length && difficultyConfig.multipliers.length > 0) {
        return difficultyConfig.multipliers[difficultyConfig.multipliers.length - 1] || 0.0; 
    }
    console.warn(`[MinesCalcMult GID:${gameId || 'N/A'}] Invalid revealedGemsCount: ${revealedGemsCount} for difficulty ${difficultyKey}. Returning 0.`);
    return 0.0; 
}

// --- Mines Game Message & Keyboard Generation (Helper) ---
// CORRECTED formatAndGenerateMinesMessageComponents
async function formatAndGenerateMinesMessageComponents(gameData, isForFinalSummary = false) {
    const logPrefix = `[FormatMinesMsgComponents GID:${gameData.gameId}]`;

    // Destructure ALL properties from gameData that are used directly as variables
    const { 
        betAmount, difficultyKey, difficultyLabel, rows, cols, numMines, 
        gemsFound, // Correctly destructure gemsFound
        status, grid, revealedTiles, gameId, playerRef, 
        initiatorMentionHTML, initiatorUserObj, userId, 
        currentMultiplier, potentialPayout, 
        finalPayout, finalMultiplier 
    } = gameData;

    const actualPlayerRef = playerRef || initiatorMentionHTML || escapeHTML(getPlayerDisplayReference(initiatorUserObj || {telegram_id: userId, first_name: "Player"}));

    let titleTextStr = `Mines - ${escapeHTML(difficultyLabel || 'Custom Game')}`;
    let titleEmoji = TILE_EMOJI_MINE;

    if (status === 'game_over_mine_hit') titleEmoji = TILE_EMOJI_EXPLOSION;
    else if (status === 'game_over_cashed_out' || status === 'game_over_all_gems_found') titleEmoji = '🎉';
    
    const titleText = `${titleEmoji} <b>${escapeHTML(titleTextStr)}</b> ${titleEmoji}`;

    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(betAmount, 'USD'));
    let messageTextHTML = `${titleText}\n`;
    messageTextHTML += `Player: ${actualPlayerRef}\n`;
    messageTextHTML += `Wager: <b>${betDisplayHTML}</b> | Difficulty: <b>${escapeHTML(difficultyLabel || 'N/A')}</b> (${escapeHTML(String(numMines))} ${TILE_EMOJI_MINE})\n`;
    
    const totalSafeTiles = (rows * cols) - numMines;
    // Use destructured gemsFound
    if (status === 'player_turn' || isForFinalSummary) {
         messageTextHTML += `${TILE_EMOJI_GEM} Gems Found: <b>${escapeHTML(String(gemsFound))} / ${escapeHTML(String(totalSafeTiles))}</b>\n`;
    }

    let outcomeAndPayoutLine = "";
    if (status === 'game_over_mine_hit') {
        outcomeAndPayoutLine = `<b>Outcome:</b> You hit a mine! 😥 Bet of <b>${betDisplayHTML}</b> lost.`;
    } else if (status === 'game_over_cashed_out') {
        const finalPayoutDisplay = escapeHTML(await formatBalanceForDisplay(finalPayout || 0n, 'USD'));
        const displayMultiplier = escapeHTML((finalMultiplier || currentMultiplier || 0).toFixed(2));
        // Use destructured gemsFound
        outcomeAndPayoutLine = `<b>Outcome:</b> Cashed out with <b>${escapeHTML(String(gemsFound))}</b> ${TILE_EMOJI_GEM}!\nFinal Payout: <b>${finalPayoutDisplay}</b> (x${displayMultiplier})`;
    } else if (status === 'game_over_all_gems_found') {
        const finalPayoutDisplay = escapeHTML(await formatBalanceForDisplay(finalPayout || 0n, 'USD'));
        const displayMultiplier = escapeHTML((finalMultiplier || currentMultiplier || 0).toFixed(2));
        // Use destructured gemsFound
        outcomeAndPayoutLine = `<b>Outcome:</b> Found all <b>${escapeHTML(String(gemsFound))}</b> ${TILE_EMOJI_GEM}!\nMax Payout: <b>${finalPayoutDisplay}</b> (x${displayMultiplier})`;
    }
    
    if (status === 'player_turn' && !isForFinalSummary) {
        // Use destructured gemsFound
        if (gemsFound > 0) {
            const currentCalcMultiplier = calculateMinesMultiplier(gameData, gemsFound); 
            const currentCalcPotentialPayout = BigInt(Math.floor(Number(betAmount) * currentCalcMultiplier));
            messageTextHTML += `Current Multiplier: <b>x${escapeHTML(currentCalcMultiplier.toFixed(2))}</b>\n`;
            messageTextHTML += `Cash Out Value: <b>${escapeHTML(await formatBalanceForDisplay(currentCalcPotentialPayout, 'USD'))}</b>\n`;
        } else {
            messageTextHTML += `Current Payout: Find gems to increase! ✨\n`;
        }
        // Use destructured gemsFound
        if (gemsFound < totalSafeTiles) {
             const nextGemCalcMultiplier = calculateMinesMultiplier(gameData, gemsFound + 1); 
             const nextCalcPayout = BigInt(Math.floor(Number(betAmount) * nextGemCalcMultiplier));
             messageTextHTML += `Next ${TILE_EMOJI_GEM} Prize: <b>x${escapeHTML(nextGemCalcMultiplier.toFixed(2))}</b> (${escapeHTML(await formatBalanceForDisplay(nextCalcPayout, 'USD'))})\n`;
        }
    } else if (isForFinalSummary && outcomeAndPayoutLine) { 
        messageTextHTML += `\n${outcomeAndPayoutLine}\n`; 
    }
    
    messageTextHTML += "\n"; 

    if (status === 'player_turn' && !isForFinalSummary) {
        messageTextHTML += `👇 Click a tile to reveal it. Good luck!`;
    }

    const keyboardRows = [];
    for (let r_idx = 0; r_idx < rows; r_idx++) { 
        const rowButtons = [];
        for (let c_idx = 0; c_idx < cols; c_idx++) {
            const cell = grid[r_idx][c_idx]; 
            let buttonText = TILE_EMOJI_HIDDEN;
            let callbackData = `mines_tile:${gameId}:${r_idx}:${c_idx}`; 

            const cellIsRevealed = revealedTiles[r_idx][c_idx]; 

            if (status === 'player_turn' && !isForFinalSummary) {
                buttonText = cellIsRevealed ? (cell.isMine ? TILE_EMOJI_EXPLOSION : TILE_EMOJI_GEM) : TILE_EMOJI_HIDDEN;
            } else { 
                if (cellIsRevealed) {
                    buttonText = cell.isMine ? TILE_EMOJI_EXPLOSION : TILE_EMOJI_GEM;
                } else { 
                    buttonText = cell.isMine ? TILE_EMOJI_MINE : TILE_EMOJI_GEM; 
                }
            }
                        
            rowButtons.push({
                text: buttonText,
                callback_data: (status === 'player_turn' && !isForFinalSummary && !cellIsRevealed) ? `mines_tile:${gameId}:${r_idx}:${c_idx}` : `mines_noop:${gameId}:${r_idx}:${c_idx}`
            });
        }
        keyboardRows.push(rowButtons);
    }

    if (status === 'player_turn' && !isForFinalSummary) {
        // Use destructured gemsFound
        if (gemsFound > 0) {
            const currentCalcMultiplier = calculateMinesMultiplier(gameData, gemsFound); 
            const currentCalcPotentialPayout = BigInt(Math.floor(Number(betAmount) * currentCalcMultiplier));
            keyboardRows.push([{ text: `💰 Cash Out (${escapeHTML(await formatBalanceForDisplay(currentCalcPotentialPayout, 'USD'))})`, callback_data: `mines_cashout:${gameId}` }]);
        }
    }
    
    // Use destructured gemsFound
    if (status === 'player_turn' && !isForFinalSummary && gemsFound === 0 && keyboardRows.length === rows) {
        keyboardRows.push([{ text: "📖 Rules", callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.MINES}`}]);
    }

    return { messageTextHTML, keyboard: { inline_keyboard: keyboardRows } };
}

async function updateMinesGameMessage(gameData, deleteOldMessage = true, isFinalSummary = false) {
    const logPrefix = `[UpdateMinesMsg GID:${gameData.gameId} DeleteNSend Fin:${isFinalSummary}]`;
    console.log(`${logPrefix} Updating. Status: ${gameData.status}, Gems: ${gameData.gemsFound}, OldMsgId: ${gameData.gameMessageId}`);

    if (deleteOldMessage && gameData.gameMessageId && bot) {
        await bot.deleteMessage(String(gameData.chatId), Number(gameData.gameMessageId))
            .catch(e => console.warn(`${logPrefix} Non-critical: Failed to delete old game message ID ${gameData.gameMessageId}: ${e.message}`));
        gameData.gameMessageId = null; 
    }

    const { messageTextHTML, keyboard: gridKeyboard } = await formatAndGenerateMinesMessageComponents(gameData, isFinalSummary);
    
    let finalReplyMarkup = gridKeyboard;

    if (isFinalSummary) {
        const postGameActionButtons = createPostGameKeyboard(GAME_IDS.MINES, gameData.betAmount);
        finalReplyMarkup = {
            inline_keyboard: [
                ...(gridKeyboard.inline_keyboard || []), 
                ...(postGameActionButtons.inline_keyboard || []) 
            ]
        };
    }
    
    const messageOptions = {
        parse_mode: 'HTML', 
        reply_markup: finalReplyMarkup,
        disable_web_page_preview: true
    };
    // console.log(`${logPrefix} Sending new message. ParseMode: HTML. Keyboard total rows: ${finalReplyMarkup.inline_keyboard.length}. ChatID: ${gameData.chatId}`);
    // console.log(`${logPrefix} Message Text (start): ${messageTextHTML.substring(0, 200)}...`); 
    
    const newMsg = await safeSendMessage(String(gameData.chatId), messageTextHTML, messageOptions);
    if (newMsg?.message_id) {
        if (!isFinalSummary && gameData.status === 'player_turn') {
            gameData.gameMessageId = String(newMsg.message_id);
        } 
        console.log(`${logPrefix} New message sent (ID: ${newMsg.message_id}).`);
    } else {
        console.error(`${logPrefix} Failed to send new Mines game message for game ${gameData.gameId}.`);
    }

    if(!isFinalSummary && activeGames.has(gameData.gameId)) {
        activeGames.set(gameData.gameId, gameData); 
    }
}

// REVISED handleMinesDifficultySelectionCallback (to ensure single client release)
async function handleMinesDifficultySelectionCallback(offerId, userWhoClicked, difficultyKey, callbackQueryId, originalMessageId, originalChatId, originalChatType) {
    const clickerId = String(userWhoClicked.telegram_id || userWhoClicked.id); 
    const logPrefix = `[MinesDiffSelect_DeleteNSend_ReleaseFix_OrderFix_Full UID:${clickerId} OfferID:${offerId} Diff:${difficultyKey}]`;
    console.log(`${logPrefix} Processing difficulty selection.`);

    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.MINES_OFFER || offerData.status !== 'awaiting_difficulty') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Mines offer is no longer valid.", show_alert: true }).catch(()=>{});
        if (originalMessageId && bot) bot.editMessageReplyMarkup({}, { chat_id: originalChatId, message_id: Number(originalMessageId) }).catch(() => {});
        return;
    }
    if (String(offerData.initiatorId) !== clickerId) { 
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the offer initiator can select difficulty.", show_alert: true });
        return; 
    }

    const difficultyConfig = MINES_DIFFICULTY_CONFIG[difficultyKey];
    if (!difficultyConfig) { 
        console.error(`${logPrefix} Invalid difficulty key: ${difficultyKey}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid difficulty selected.", show_alert: true });
        return; 
    }

    let client = null; 
    let actualGameId; 
    try {
        client = await pool.connect(); 
        await client.query('BEGIN');

        const currentUserForBet = await getOrCreateUser(clickerId, userWhoClicked.username, userWhoClicked.first_name, userWhoClicked.last_name, client); 
        
        if (!currentUserForBet || BigInt(currentUserForBet.balance) < offerData.betAmount) {
            await client.query('ROLLBACK'); 
            const betDisplayErrorHTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount, 'USD'));
            const neededErrorDisplayHTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount - BigInt(currentUserForBet?.balance || 0), 'USD'));
            await bot.answerCallbackQuery(callbackQueryId, { text: `Your balance is too low for a ${betDisplayErrorHTML} game. Need ${neededErrorDisplayHTML} more.`, show_alert: true });
            
            if (bot && offerData.offerMessageId) {
                 await bot.editMessageText( `💣 Offer by ${offerData.initiatorMentionHTML} for <b>${betDisplayErrorHTML}</b> was cancelled. Insufficient funds to start game.`, { chat_id: originalChatId, message_id: Number(offerData.offerMessageId), parse_mode: 'HTML', reply_markup: {} } ).catch(()=>{});
            }
            activeGames.delete(offerId);
            await updateGroupGameDetails(originalChatId, null, null, null);
            return; // client released in finally
        }

        const balanceUpdateResult = await updateUserBalanceAndLedger( 
            client, 
            clickerId, 
            BigInt(-offerData.betAmount), 
            'bet_placed_mines', 
            { game_id_custom_field: offerId }, // Using offerId as a temporary game_id for this bet log
            `Mines game started (${difficultyKey}). Bet: ${await formatBalanceForDisplay(offerData.betAmount, 'SOL')}`
        );

        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK'); 
            console.error(`${logPrefix} Failed to deduct bet for Mines game ${offerId}: ${balanceUpdateResult.error}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: "Error placing your bet. Please try again.", show_alert: true });
            return; // client released in finally
        }
        await client.query('COMMIT');
        console.log(`${logPrefix} Bet of ${offerData.betAmount} lamports successfully deducted for user ${clickerId}.`);
        
        const updatedUserObjAfterBet = await getOrCreateUser(clickerId); // Re-fetch to get the latest balance

        await bot.answerCallbackQuery(callbackQueryId, { text: `Selected ${difficultyConfig.label}. Starting game...`}).catch(()=>{});

        actualGameId = generateGameId(GAME_IDS.MINES);
        const { grid, mineLocations } = await generateMinesGridAndData(difficultyConfig.rows, difficultyConfig.cols, difficultyConfig.mines);
        const initialMultiplier = MINES_DIFFICULTY_CONFIG[difficultyKey].multipliers[0] || 0; 
        const initialPotentialPayout = BigInt(Math.floor(Number(offerData.betAmount) * initialMultiplier));

        const gameData = {
            type: GAME_IDS.MINES, gameId: actualGameId, chatId: offerData.chatId,
            userId: clickerId, playerRef: offerData.initiatorMentionHTML, 
            initiatorId: clickerId, 
            initiatorMentionHTML: offerData.initiatorMentionHTML,
            initiatorUserObj: updatedUserObjAfterBet, 
            betAmount: offerData.betAmount,
            rows: difficultyConfig.rows, cols: difficultyConfig.cols, numMines: difficultyConfig.mines,
            difficultyKey: difficultyKey, difficultyLabel: difficultyConfig.label,
            grid: grid, mineLocations: mineLocations, 
            revealedTiles: Array(difficultyConfig.rows).fill(null).map(() => Array(difficultyConfig.cols).fill(false)),
            gemsFound: 0, currentMultiplier: initialMultiplier, potentialPayout: initialPotentialPayout, 
            status: 'player_turn', 
            gameMessageId: offerData.offerMessageId, 
            lastInteractionTime: Date.now()
        };
            
        activeGames.set(actualGameId, gameData);
        activeGames.delete(offerId); 
        await updateGroupGameDetails(originalChatId, actualGameId, GAME_IDS.MINES, offerData.betAmount);

        console.log(`${logPrefix} Mines game ${actualGameId} started. Grid: ${gameData.rows}x${gameData.cols}, Mines: ${gameData.numMines}. Bet: ${offerData.betAmount}`);
        await updateMinesGameMessage(gameData, true, false); 

    } catch (error) {
        if (client) { 
           try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} DB Rollback Error: ${rbErr.message}`); }
        }
        console.error(`${logPrefix} Error starting Mines game after difficulty selection: ${error.message}`, error.stack?.substring(0,700));
        const errorText = `⚙️ Oops! A critical error occurred while starting your Mines game: \`${escapeHTML(error.message)}\`.`;
        if (originalMessageId && bot && offerData.offerMessageId) { 
            await bot.editMessageText(errorText, { chat_id: originalChatId, message_id: Number(offerData.offerMessageId), parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{text: "Dismiss", callback_data:"noop_ok"}]] } }).catch(async () => {
                await safeSendMessage(originalChatId, errorText, { parse_mode: 'HTML'}); 
            });
        } else {
            await safeSendMessage(originalChatId, errorText, { parse_mode: 'HTML'});
        }
        activeGames.delete(offerId); 
        if (typeof actualGameId === 'string' && activeGames.has(actualGameId)) { // Ensure actualGameId is defined and string before using in activeGames.has
            activeGames.delete(actualGameId);
        }
        await updateGroupGameDetails(originalChatId, null, null, null);
    } finally {
        if (client) { 
            console.log(`${logPrefix} Releasing DB client in finally block.`);
            client.release();
        }
    }
}

// CORRECTED handleMinesTileClickCallback (ensures `c` and `gameData.cols` are used, not `col`)
async function handleMinesTileClickCallback(gameId, userWhoClicked, r_str, c_str, callbackQueryId, originalMessageId, originalChatId) {
    const userId = String(userWhoClicked.telegram_id || userWhoClicked.id);
    const r = parseInt(r_str, 10); // parameter r
    const c = parseInt(c_str, 10); // parameter c (this was the focus of the 'col is not defined' fix)
    const logPrefix = `[MinesTileClick GID:${gameId} UID:${userId} Tile:${r},${c} DeleteNSendFix]`;
    console.log(`${logPrefix} Processing tile click.`);

    let gameData = activeGames.get(gameId); 

    // Use `c` (local const from param) and `gameData.cols` (property)
    if (!gameData || gameData.type !== GAME_IDS.MINES || gameData.userId !== userId || gameData.status !== 'player_turn' ||
        isNaN(r) || isNaN(c) || r < 0 || r >= gameData.rows || c < 0 || c >= gameData.cols || gameData.grid[r][c].isRevealed) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid action or tile.", show_alert: true }).catch(()=>{});
        return;
    }

    const cell = gameData.grid[r][c]; // Correctly uses r and c
    cell.isRevealed = true;
    gameData.lastInteractionTime = Date.now();
    let statusMessageForAnswerCallback = ''; 
    let gameOver = false;
    let client;

    if (cell.isMine) {
        console.log(`${logPrefix} Player hit a mine at ${r},${c}. Game Over.`);
        gameData.status = 'game_over_mine_hit';
        statusMessageForAnswerCallback = `${TILE_EMOJI_EXPLOSION} BOOM! Mine at (${r + 1},${c + 1})!`;
        gameOver = true;
        gameData.finalPayout = 0n; gameData.finalMultiplier = 0;
        
        gameData.mineLocations.forEach(([mr, mc_loc]) => { 
            if (gameData.grid[mr] && gameData.grid[mr][mc_loc]) { 
                gameData.grid[mr][mc_loc].isRevealed = true; 
            }
        });
        if (gameData.grid[r] && gameData.grid[r][c]) { 
            gameData.grid[r][c].display = TILE_EMOJI_EXPLOSION; // Ensure clicked mine shows explosion for final grid
        }

        try { 
            client = await pool.connect(); await client.query('BEGIN');
            const lossLedgerDetails = { game_id_custom_field: gameId, outcome: 'loss_mine_hit', difficulty: gameData.difficultyKey, mines_total: gameData.numMines, gems_found: gameData.gemsFound };
            const lossLedgerNotes = `Mines: Hit mine. Bet ${await formatBalanceForDisplay(gameData.betAmount, 'SOL')}. Gems found: ${gameData.gemsFound}.`;
            await updateUserBalanceAndLedger(client, userId, 0n, 'loss_mines_hit', lossLedgerDetails, lossLedgerNotes);
            await client.query('COMMIT');
        } catch (e) { if (client) await client.query('ROLLBACK'); console.error(`${logPrefix} DB Error logging mine hit loss for ${gameId}: ${e.message}`); statusMessageForAnswerCallback += " (DB Log Error)";} 
        finally { if (client) client.release(); }
    } else { 
        gameData.gemsFound++;
        console.log(`${logPrefix} Player found a gem at ${r},${c}. Total gems: ${gameData.gemsFound}`);
        
        gameData.currentMultiplier = calculateMinesMultiplier(gameData, gameData.gemsFound);
        gameData.potentialPayout = BigInt(Math.floor(Number(gameData.betAmount) * gameData.currentMultiplier));
        statusMessageForAnswerCallback = `${TILE_EMOJI_GEM} Gem! x${gameData.currentMultiplier.toFixed(2)}`;

        // Use gameData.rows and gameData.cols
        const totalNonMineCells = (gameData.rows * gameData.cols) - gameData.numMines;
        if (gameData.gemsFound >= totalNonMineCells) { 
            console.log(`${logPrefix} Player found all ${totalNonMineCells} gems! Max Win!`);
            gameData.status = 'game_over_all_gems_found';
            gameData.finalPayout = gameData.potentialPayout; 
            gameData.finalMultiplier = gameData.currentMultiplier;
            gameOver = true;
            const profitLamportsAllGems = gameData.finalPayout - gameData.betAmount; 
            
            try { 
                client = await pool.connect(); await client.query('BEGIN');
                const winLedgerDetails = { game_id_custom_field: gameId, outcome: 'win_all_gems', difficulty: gameData.difficultyKey, mines_total: gameData.numMines, gems_found: gameData.gemsFound, payout_multiplier_custom: gameData.finalMultiplier.toFixed(4) };
                const winLedgerNotes = `Mines win (${gameData.difficultyLabel}) - All gems. Payout: ${await formatBalanceForDisplay(gameData.finalPayout, 'SOL')}`;
                await updateUserBalanceAndLedger(client, userId, profitLamportsAllGems, 'win_mines_all_gems', winLedgerDetails, winLedgerNotes); 
                await client.query('COMMIT');
            } catch (dbError) { if (client) await client.query('ROLLBACK'); console.error(`${logPrefix} DB Error processing Mines max win: ${dbError.message}`); statusMessageForAnswerCallback = "Error processing max win payout.";} 
            finally { if (client) client.release(); }
        }
    }
    
    if(activeGames.has(gameId)) activeGames.set(gameId, gameData); 

    const cbAnswerText = statusMessageForAnswerCallback.length > 190 ? statusMessageForAnswerCallback.substring(0,190) + "..." : statusMessageForAnswerCallback;
    await bot.answerCallbackQuery(callbackQueryId, {text: cbAnswerText }).catch(() => {});

    await updateMinesGameMessage(gameData, true, gameOver); 

    if (gameOver) {
        activeGames.delete(gameId); 
        await updateGroupGameDetails(originalChatId, null, null, null);
        console.log(`${logPrefix} Game ${gameId} (Game Over) fully finalized and cleaned up.`);
    }
}

async function handleMinesCashOutCallback(gameId, userObject, callbackQueryId, originalMessageId, originalChatId) {
    const userId = String(userObject.telegram_id);
    const logPrefix = `[MinesCashOut GID:${gameId} UID:${userId} DeleteNSendFix]`;
    console.log(`${logPrefix} Processing cash out.`);

    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.type !== GAME_IDS.MINES || gameData.userId !== userId || 
        gameData.status !== 'player_turn' || gameData.gemsFound === 0) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Cannot cash out now or invalid game.", show_alert: true }).catch(()=>{});
        return;
    }

    await bot.answerCallbackQuery(callbackQueryId, { text: "Cashing out..." }).catch(()=>{});

    gameData.status = 'game_over_cashed_out';
    gameData.currentMultiplier = calculateMinesMultiplier(gameData, gameData.gemsFound);
    gameData.finalPayout = BigInt(Math.floor(Number(gameData.betAmount) * gameData.currentMultiplier));
    gameData.finalMultiplier = gameData.currentMultiplier;
    const profitLamports = gameData.finalPayout - gameData.betAmount;
    gameData.lastInteractionTime = Date.now();

    for(let r_idx=0; r_idx<gameData.rows; r_idx++) { for(let c_idx=0; c_idx<gameData.cols; c_idx++) { gameData.grid[r_idx][c_idx].isRevealed = true; } }

    let client = null;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const cashoutResult = await updateUserBalanceAndLedger(
            client, userId, profitLamports, 
            'win_mines_cashout',
            { game_id_custom_field: gameId, difficulty_custom: gameData.difficultyKey, gems_custom: gameData.gemsFound, payout_multiplier_custom: gameData.finalMultiplier.toFixed(4) },
            `Mines cash out (${gameData.difficultyLabel}). Profit: ${await formatBalanceForDisplay(profitLamports, 'SOL')}`
        );
        if (!cashoutResult.success) throw new Error(cashoutResult.error || "Failed to update balance on cash out.");
        await client.query('COMMIT');
        console.log(`${logPrefix} Player cashed out. Profit: ${profitLamports}. New balance: ${cashoutResult.newBalanceLamports}`);
    } catch (dbError) {
        if (client) await client.query('ROLLBACK');
        console.error(`${logPrefix} DB Error processing Mines cash out: ${dbError.message}`);
        gameData.status = 'player_turn'; 
        gameData.finalPayout = 0n; gameData.finalMultiplier = 0;
        // Revert revealed state based on what player actually clicked, which is stored in gameData.revealedTiles (which is an array of objects)
        // If gameData.grid[r][c].isRevealed was the source of truth, need to reset it.
        // For simplicity on error, we'll just let the next update show currently known gems.
        // To be more precise, you'd need to reconstruct which tiles were *player-revealed* vs *game-over-revealed*.
        // Let's assume the user should retry the cashout.
        gameData.grid.forEach((rowItem, r_idx) => rowItem.forEach((cellItem, c_idx) => {
            // This logic for reverting is tricky if isRevealed was directly set.
            // The simplest is just to re-render with current gemsFound.
            // For now, we just make sure status is back to player_turn.
            if (gameData.grid[r_idx][c_idx].isMine) gameData.grid[r_idx][c_idx].isRevealed = false; // Hide mines again
            // But keep gems revealed by player
            let wasPlayerRevealedGem = false;
            if(gameData.grid[r_idx][c_idx].isRevealed && !gameData.grid[r_idx][c_idx].isMine) wasPlayerRevealedGem = true;
            gameData.grid[r_idx][c_idx].isRevealed = wasPlayerRevealedGem;

        }));


        await safeSendMessage(userId, "⚙️ Error processing your cash out. Your game state with found gems has been preserved. Please try cashing out again or contact support.", { parse_mode: 'HTML' });
        activeGames.set(gameId, gameData); 
        await updateMinesGameMessage(gameData, true, false); 
        if(client) client.release();
        return; 
    } finally {
        if (client) client.release();
    }
    
    if(activeGames.has(gameId)) activeGames.set(gameId, gameData); 
    await updateMinesGameMessage(gameData, true, true); 
    activeGames.delete(gameId); 
    await updateGroupGameDetails(originalChatId, null, null, null);
}

async function handleMinesCancelOfferCallback(offerId, userWhoClicked, originalMessageId, originalChatId, callbackQueryId) {
    const clickerId = String(userWhoClicked.telegram_id || userWhoClicked.id);
    const logPrefix = `[MinesCancelOffer_DeleteNSend UID:${clickerId} OfferID:${offerId}]`;
    const offerData = activeGames.get(offerId);

    if (!offerData || offerData.type !== GAME_IDS.MINES_OFFER || offerData.status !== 'awaiting_difficulty') {
        await bot.answerCallbackQuery(callbackQueryId, { text: "This Mines offer is no longer valid or already started.", show_alert: true });
        return;
    }
    if (offerData.initiatorId !== clickerId) {
        await bot.answerCallbackQuery(callbackQueryId, { text: "Only the offer initiator can cancel.", show_alert: true });
        return;
    }

    await bot.answerCallbackQuery(callbackQueryId, { text: "Mines offer cancelled." });
    activeGames.delete(offerId);
    await updateGroupGameDetails(originalChatId, null, null, null);

    const betDisplayHTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount, 'USD'));
    const messageTextHTML = `💣 Offer by ${offerData.initiatorMentionHTML} for <b>${betDisplayHTML}</b> (Mines) has been cancelled.`;
    
    console.log(`${logPrefix} Deleting original offer message ${originalMessageId} and sending cancellation notice.`);
    if (originalMessageId && bot) {
        await bot.deleteMessage(String(originalChatId), Number(originalMessageId)).catch(e => 
            console.warn(`${logPrefix} Failed to delete cancelled Mines offer message: ${e.message}`)
        );
    }
    await safeSendMessage(String(originalChatId), messageTextHTML, { parse_mode: 'HTML' });
}
// --- End of Part 5d (Mines Game - Casino Style - Delete & Resend with All Fixes) ---
// --- Start of Part 5a, Section 2 (REVISED for DM-only Help/Rules Menus & New Dice Escalator Rules): General Command Handler Implementations ---
// index.js - Part 5a, Section 2: General Casino Bot Command Implementations
//----------------------------------------------------------------------------------
// Assumed dependencies from previous Parts:
// Part 1: safeSendMessage, escapeMarkdownV2, bot, BOT_NAME, BOT_VERSION, ADMIN_USER_ID, pool,
//         MIN_BET_USD_val, MAX_BET_USD_val, MIN_BET_AMOUNT_LAMPORTS_config, MAX_BET_AMOUNT_LAMPORTS_config,
//         TARGET_JACKPOT_SCORE (for DE PvB), DICE_ESCALATOR_BUST_ON (for DE Player),
//         DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE, MAIN_JACKPOT_ID (for DE PvB), GAME_IDS,
//         OU7_PAYOUT_NORMAL, OU7_PAYOUT_SEVEN, OU7_DICE_COUNT, DUEL_DICE_COUNT,
//         LADDER_ROLL_COUNT, LADDER_BUST_ON, LADDER_PAYOUTS, SLOT_PAYOUTS,
//         RULES_CALLBACK_PREFIX, QUICK_DEPOSIT_CALLBACK_ACTION, WITHDRAW_CALLBACK_ACTION, LAMPORTS_PER_SOL,
//         getSolUsdPrice, convertUSDToLamports, convertLamportsToUSDString, userStateCache,
//         MINES_DIFFICULTY_CONFIG, MINES_MIN_MINES, MINES_MAX_MINES_PERCENT, MINES_DEFAULT_ROWS, MINES_DEFAULT_COLS, MINES_FALLBACK_DEFAULT_MINES, JOIN_GAME_TIMEOUT_MS 
// Part 2: getOrCreateUser, getUserBalance, queryDatabase, getUserByReferralCode, generateReferralCode, findRecipientUser
// Part 3: getPlayerDisplayReference, formatCurrency, formatBalanceForDisplay, generateGameId
// Part 5a-S4 (Shared UI): createPostGameKeyboard
// Part P2: updateUserBalanceAndLedger
// Part P3: clearUserState, routeStatefulInput, handleMenuAction, handleWithdrawalConfirmation

// --- Command Handler Functions (General Casino Bot Commands) ---

async function handleStartCommand(msg, args) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_START_V2 = `[StartCmd_V2 UID:${userId} CH:${chatId}]`; // V2 for new version

    console.log(`${LOG_PREFIX_START_V2} /start command received. ChatType: ${chatType}, Args: ${args.join(', ')}`);

    // Get user object first for any path
    let userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) {
        // This error message will be HTML
        await safeSendMessage(chatId, "😕 Error fetching your player profile. Please try typing <code>/start</code> again.", { parse_mode: 'HTML' });
        return;
    }
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObject)); // HTML safe version
    let botUsername = BOT_NAME || "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { console.error(`${LOG_PREFIX_START_V2} Could not fetch bot username: ${e.message}`); }
    const botUsernameHTML = escapeHTML(botUsername);


    // 1. Handle deep link arguments first
    if (args && args[0]) {
        const deepLinkParam = args[0];
        console.log(`${LOG_PREFIX_START_V2} Processing deep link parameter: ${deepLinkParam}`);

        if (deepLinkParam.startsWith('ref_')) {
            const refCode = deepLinkParam.substring(4);
            // (Referral logic as in your original document - ensuring messages are HTML and sent to DM)
            const referrerUserRecord = await getUserByReferralCode(refCode);
            let refByDisplayHTML = "a fellow player";

            if (referrerUserRecord && String(referrerUserRecord.telegram_id) !== userId) {
                const referrerFullObj = await getOrCreateUser(referrerUserRecord.telegram_id, referrerUserRecord.username, referrerUserRecord.first_name);
                if (referrerFullObj) refByDisplayHTML = escapeHTML(getPlayerDisplayReference(referrerFullObj));

                if (!userObject.referrer_telegram_id) {
                    // ... (DB logic to link referral - unchanged) ...
                    let clientRefLink = null;
                    try {
                        clientRefLink = await pool.connect();
                        await clientRefLink.query('BEGIN');
                        await clientRefLink.query('UPDATE users SET referrer_telegram_id = $1 WHERE telegram_id = $2 AND referrer_telegram_id IS NULL', [referrerUserRecord.telegram_id, userId]);
                        await clientRefLink.query(
                            `INSERT INTO referrals (referrer_telegram_id, referred_telegram_id, status, created_at, updated_at)
                             VALUES ($1, $2, 'pending_criteria', NOW(), NOW())
                             ON CONFLICT (referrer_telegram_id, referred_telegram_id) DO NOTHING
                             ON CONFLICT ON CONSTRAINT referrals_referred_telegram_id_key DO NOTHING;`,
                            [referrerUserRecord.telegram_id, userId]
                        );
                        await clientRefLink.query('COMMIT');
                        userObject = await getOrCreateUser(userId); // Re-fetch userObject
                        console.log(`${LOG_PREFIX_START_V2} User ${userId} successfully linked to referrer ${referrerUserRecord.telegram_id}`);
                    } catch (refError) {
                        if(clientRefLink) await clientRefLink.query('ROLLBACK');
                        console.error(`${LOG_PREFIX_START_V2} Error linking referral for user ${userId} via code ${refCode}:`, refError);
                    } finally {
                        if(clientRefLink) clientRefLink.release();
                    }
                } else { /* User already has a referrer */ }
            } else if (referrerUserRecord && String(referrerUserRecord.telegram_id) === userId) {
                refByDisplayHTML = "yourself (clever try! 😉)";
            }
            
            const referralMsgHTML = `👋 Welcome, ${playerRefHTML}! You joined via ${refByDisplayHTML}.<br>Explore the casino using the menu I've just displayed!`;
            
            if (chatType !== 'private') {
                if(msg.message_id) await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
                await safeSendMessage(chatId, `${playerRefHTML}, welcome! I've sent the main menu to our private chat: @${botUsernameHTML} 📬`, { parse_mode: 'HTML' });
            }
            // Send referral welcome and main menu to DM
            await safeSendMessage(userId, referralMsgHTML, { parse_mode: 'HTML' });
            const dmMsgContext = { // Simulate msg object for DM context
                from: userObject, 
                chat: { id: userId, type: 'private' }, 
                message_id: null // To ensure handleHelpCommand sends a new message
            };
            await handleHelpCommand(dmMsgContext);
            return;
        } else if (deepLinkParam.startsWith('cb_') || deepLinkParam.startsWith('menu_')) {
            const actionDetails = deepLinkParam.startsWith('cb_') ? deepLinkParam.substring(3) : deepLinkParam.substring(5);
            const [actionName, ...actionParams] = actionDetails.split('_');
            console.log(`${LOG_PREFIX_START_V2} Deep link for menu/callback action: ${actionName}, Params: ${actionParams.join(',')}`);
            
            // Inform user in DM and then route action
            if (chatType !== 'private' && msg.message_id) await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
            const userGuidanceTextHTML = `👋 Welcome back, ${playerRefHTML}!<br>Taking you to the requested section.`;
            await safeSendMessage(userId, userGuidanceTextHTML, {parse_mode: 'HTML'});
            
            if (typeof handleMenuAction === 'function') {
                // handleMenuAction should primarily operate in DMs or handle its own redirection message editing.
                // We ensure it's called with DM context if it's a deep link.
                await handleMenuAction(userId, userId, null, actionName, actionParams, true, 'private');
            } else {
                console.error(`${LOG_PREFIX_START_V2} handleMenuAction not defined. Falling back to main help.`);
                const dmMsgContext = { from: userObject, chat: { id: userId, type: 'private' }, message_id: null };
                await handleHelpCommand(dmMsgContext);
            }
            return;
        }
    }

    // 2. No deep link args, standard /start command
    if (typeof clearUserState === 'function') {
        clearUserState(userId);
    } else {
        userStateCache.delete(userId);
    }

    if (chatType === 'group' || chatType === 'supergroup') {
        if (msg.message_id && chatId !== userId) { // Delete /start command from group
            await bot.deleteMessage(chatId, msg.message_id).catch(() => {});
        }
        // Message in group guiding to DM
        await safeSendMessage(chatId, `Hi ${playerRefHTML}! 👋 For commands & our main menu, please check our private chat: @${botUsernameHTML} 📬 I've sent it to you there!`, { parse_mode: 'HTML' });
        
        // Send the actual help menu to the user's DM
        const dmMsgContext = { 
            from: userObject, // Pass the full user object
            chat: { id: userId, type: 'private' }, 
            message_id: null // This ensures handleHelpCommand sends a new message
        };
        await handleHelpCommand(dmMsgContext);
    } else { // Private chat
        if (msg.message_id) { // Delete user's /start command in DM
            await bot.deleteMessage(userId, msg.message_id).catch(() => {});
        }
        // Directly call handleHelpCommand to show the main menu in the DM
        // msg here already has chat.id = userId and chat.type = 'private'
        // We pass null for message_id to ensure a new menu is sent, not an edit attempt of user's /start
        const privateStartMsgContext = { ...msg, message_id: null, from: userObject }; 
        await handleHelpCommand(privateStartMsgContext);
    }
}

async function handleHelpCommand(msg) {
    // msg.from and msg.chat are expected to be set correctly for the DM context here.
    // msg.message_id might be the ID of a previous bot message (if called via "Back to Menu" button)
    // or null/user's message ID (if called after /start).
    const userId = String(msg.from.id || msg.from.telegram_id);
    const dmChatId = String(msg.chat.id); // Should be same as userId
    const originalMessageIdToEdit = (msg.chat.type === 'private' && msg.message_id && msg.from.is_bot === undefined) ? null : msg.message_id; // Edit only if msg.message_id is from a bot's own message button
                                                                                                                                          // If msg.from.is_bot exists and is false, it's user's /start command.
                                                                                                                                          // Better: handleHelpCommand should always send new after deleting user's cmd if it's /help in DM.
                                                                                                                                          // For menu callbacks, msg.message.message_id is the one to edit.

    const LOG_PREFIX_HELP_V2 = `[HelpCmd_V2 UID:${userId} Chat:${dmChatId}]`;
    console.log(`${LOG_PREFIX_HELP_V2} Displaying main help menu. MessageID to potentially edit: ${originalMessageIdToEdit}`);

    // Ensure userObject has latest details, especially if msg.from is minimal from a callback
    let userObject = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!userObject) {
        await safeSendMessage(dmChatId, "😕 Error fetching your player profile. Please try <code>/start</code> again.", { parse_mode: 'HTML' });
        return;
    }
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObject));
    let botUsername = BOT_NAME || "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { /* Reduced log */ }
    
    const helpMessageHTML = 
        `🎉 Welcome to <b>${escapeHTML(BOT_NAME)}</b>, ${playerRefHTML}!\n\n` +
        `Your casino adventure starts here. What would you like to do?`;

    const helpKeyboard = {
        inline_keyboard: [
            [{ text: "💰 My Wallet & Funds", callback_data: "menu:wallet" }],
            [{ text: "🎲 Play Games", callback_data: "menu:games_overview" }], 
            [{ text: "📖 Game Rules", callback_data: "menu:rules_list" }],    
            [{ text: "🤝 Referral Program", callback_data: "menu:referral" }],
            // [{ text: "🏆 Leaderboards", callback_data: "menu:leaderboards" }], // Optional
            // [{ text: "💬 Support/Community", url: "YOUR_SUPPORT_LINK_HERE" }] // Optional
        ]
    };

    // If this was triggered by a user typing /help in DM, their message was already deleted by the router
    // If this was triggered by a menu button (isFromCallback=true for handleMenuAction), 
    // handleMenuAction passes originalMessageId to edit.
    // If called by handleStartCommand for a DM, originalMessageId is passed as null to send new.

    if (originalMessageIdToEdit && msg.message?.from?.is_bot) { // Only edit if originalMessageId is from one of the bot's own messages with buttons
        try {
            await bot.editMessageText(helpMessageHTML, {
                chat_id: dmChatId,
                message_id: Number(originalMessageIdToEdit),
                parse_mode: 'HTML',
                reply_markup: helpKeyboard,
                disable_web_page_preview: true
            });
            console.log(`${LOG_PREFIX_HELP_V2} Help menu edited successfully on message ${originalMessageIdToEdit}.`);
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_HELP_V2} Failed to edit help message ${originalMessageIdToEdit}, sending new. Error: ${e.message}`);
                await safeSendMessage(dmChatId, helpMessageHTML, { parse_mode: 'HTML', reply_markup: helpKeyboard, disable_web_page_preview: true });
            } else {
                 console.log(`${LOG_PREFIX_HELP_V2} Help message content was not modified.`);
            }
        }
    } else {
        // If originalMessageId was from user (like their typed /start or /help), it should have been deleted by handleStartCommand or the message router.
        // Send as a new message.
        console.log(`${LOG_PREFIX_HELP_V2} Sending new help menu.`);
        await safeSendMessage(dmChatId, helpMessageHTML, { parse_mode: 'HTML', reply_markup: helpKeyboard, disable_web_page_preview: true });
    }
}

async function handleRulesCommand(invokedInChatIdStr, userObj, msgIdInInvokedChatStr = null, isEditAttempt = false, invokedChatType = 'private') {
    const invokedInChatId = String(invokedInChatIdStr);
    const msgIdInInvokedChat = msgIdInInvokedChatStr ? Number(msgIdInInvokedChatStr) : null;
    const userIdAsDmChatId = String(userObj.telegram_id); // Ensures we're using the string ID for DM
    const LOG_PREFIX_RULES_V2 = `[RulesCmd_V2 UID:${userIdAsDmChatId} InvokedInChat:${invokedInChatId}]`; // V2 for HTML update
    
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObj)); // Use HTML version
    let botUsername = BOT_NAME || "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { console.error(`${LOG_PREFIX_RULES_V2} Could not fetch bot username: ${e.message}`);}

    // If command was in group, guide to DM and send the rules menu there.
    if (invokedChatType !== 'private') {
        const redirectMsgHTML = `${playerRefHTML}, I've sent the Game Rules menu to our private chat: @${escapeHTML(botUsername)} 📖 Please check your DMs.`;
        if (isEditAttempt && msgIdInInvokedChat) { 
            try {
                await bot.editMessageText(redirectMsgHTML, {
                    chat_id: invokedInChatId,
                    message_id: msgIdInInvokedChat,
                    parse_mode: 'HTML', // Changed to HTML
                    reply_markup: { inline_keyboard: [[{ text: `📬 Open DM @${escapeHTML(botUsername)}`, url: `https://t.me/${botUsername}?start=menu_rules_list` }]]} 
                });
            } catch (e) { 
                if (!e.message?.toLowerCase().includes("message is not modified")) {
                    console.warn(`${LOG_PREFIX_RULES_V2} Failed to edit group msg for rules redirect (ID: ${msgIdInInvokedChat}): ${e.message}. Sending new.`);
                    await safeSendMessage(invokedInChatId, redirectMsgHTML, { parse_mode: 'HTML' });
                }
            }
        } else { 
            if(msgIdInInvokedChat) await bot.deleteMessage(invokedInChatId, msgIdInInvokedChat).catch(()=>{}); // Delete original /rules command
            await safeSendMessage(invokedInChatId, redirectMsgHTML, { parse_mode: 'HTML' });
        }
    }

    // Construct and send/edit the rules menu in DM
    const rulesIntroTextHTML = `📚 <b>${escapeHTML(BOT_NAME)} Gamepedia Central</b> 📚\n\n` +
                             `Hey ${playerRefHTML}, welcome to our casino's hall of knowledge! Select any game below to learn its rules, strategies, and payout secrets. 👇`;
    
    const gameRuleButtons = Object.values(GAME_IDS)
        .filter(gameCode => // Filter to show primary game rules, not every single variant if covered by a unified rule
            ![
                GAME_IDS.DICE_21_PVP, // Covered by DICE_21_UNIFIED_OFFER rules
                GAME_IDS.DUEL_PVB,    // Covered by DUEL_UNIFIED_OFFER rules
                GAME_IDS.DUEL_PVP,    // Covered by DUEL_UNIFIED_OFFER rules
                GAME_IDS.DICE_ESCALATOR_PVB, // Covered by DICE_ESCALATOR_UNIFIED_OFFER rules
                GAME_IDS.DICE_ESCALATOR_PVP, // Covered by DICE_ESCALATOR_UNIFIED_OFFER rules
                GAME_IDS.COINFLIP_PVB, // Covered by COINFLIP (or COINFLIP_UNIFIED_OFFER) rules
                GAME_IDS.COINFLIP_PVP, // Covered by COINFLIP (or COINFLIP_UNIFIED_OFFER) rules
                GAME_IDS.RPS_PVB,      // Covered by RPS (or RPS_UNIFIED_OFFER) rules
                GAME_IDS.RPS_PVP,      // Covered by RPS (or RPS_UNIFIED_OFFER) rules
                GAME_IDS.MINES_OFFER  // Actual game is MINES
            ].includes(gameCode) && 
            // Only include base or unified offer IDs for the menu
            (gameCode === GAME_IDS.COINFLIP || gameCode === GAME_IDS.COINFLIP_UNIFIED_OFFER ||
             gameCode === GAME_IDS.RPS || gameCode === GAME_IDS.RPS_UNIFIED_OFFER ||
             gameCode === GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER ||
             gameCode === GAME_IDS.DICE_21 || gameCode === GAME_IDS.DICE_21_UNIFIED_OFFER ||
             gameCode === GAME_IDS.OVER_UNDER_7 ||
             gameCode === GAME_IDS.DUEL_UNIFIED_OFFER ||
             gameCode === GAME_IDS.LADDER ||
             gameCode === GAME_IDS.SEVEN_OUT ||
             gameCode === GAME_IDS.SLOT_FRENZY ||
             gameCode === GAME_IDS.MINES 
            )
        )
        .map(gameCode => {
            let gameName = gameCode.replace(/_/g, ' ').replace(' Unified Offer', '').replace(/\b\w/g, l => l.toUpperCase());
            let ruleCallbackKey = gameCode; 
            let emoji = '❓';

            // Standardize names and emojis for buttons
            if (gameCode === GAME_IDS.COINFLIP || gameCode === GAME_IDS.COINFLIP_UNIFIED_OFFER) { gameName = "Coinflip"; emoji = '🪙'; ruleCallbackKey = GAME_IDS.COINFLIP; } // Point to one rule key
            else if (gameCode === GAME_IDS.RPS || gameCode === GAME_IDS.RPS_UNIFIED_OFFER) { gameName = "Rock Paper Scissors"; emoji = '✂️'; ruleCallbackKey = GAME_IDS.RPS; }
            else if (gameCode === GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER) { gameName = "Dice Escalator"; emoji = '🎲'; }
            else if (gameCode === GAME_IDS.DICE_21 || gameCode === GAME_IDS.DICE_21_UNIFIED_OFFER) { gameName = "Dice 21 (Blackjack)"; emoji = '🃏'; ruleCallbackKey = GAME_IDS.DICE_21; }
            else if (gameCode === GAME_IDS.OVER_UNDER_7) { gameName = "Over/Under 7"; emoji = '🎲'; }
            else if (gameCode === GAME_IDS.DUEL_UNIFIED_OFFER) { gameName = "Duel / Highroller"; emoji = '⚔️'; }
            else if (gameCode === GAME_IDS.LADDER) { gameName = "Greed's Ladder"; emoji = '🪜'; }
            else if (gameCode === GAME_IDS.SEVEN_OUT) { gameName = "Lucky Sum (Sevens Out)"; emoji = '🎲'; }
            else if (gameCode === GAME_IDS.SLOT_FRENZY) { gameName = "Slot Frenzy"; emoji = '🎰'; }
            else if (gameCode === GAME_IDS.MINES) { gameName = "Mines"; emoji = '💣'; }
            
            return { text: `${emoji} ${escapeHTML(gameName)}`, callback_data: `${RULES_CALLBACK_PREFIX}${ruleCallbackKey}` };
        })
        .filter((button, index, self) => index === self.findIndex((b) => b.callback_data === button.callback_data)); // Ensure unique callback_data for buttons

    const rows = [];
    for (let i = 0; i < gameRuleButtons.length; i += 2) { // Max 2 buttons per row
        rows.push(gameRuleButtons.slice(i, i + 2));
    }
    rows.push([{ text: '⬅️ Back to Main Menu', callback_data: 'menu:main' }]);
    
    const keyboard = { inline_keyboard: rows };
    const options = { parse_mode: 'HTML', reply_markup: keyboard, disable_web_page_preview: true }; // Changed to HTML

    // Determine if we are editing an existing message in DM or sending a new one
    let messageIdToOperateOn = null;
    if (invokedChatType === 'private' && isEditAttempt && msgIdInInvokedChat) {
        messageIdToOperateOn = msgIdInInvokedChat;
    } else if (invokedChatType === 'private' && !isEditAttempt && msgIdInInvokedChat) {
        // This means /rules was typed in DM, delete user's command and send new menu
        await bot.deleteMessage(userIdAsDmChatId, msgIdInInvokedChat).catch(()=>{});
    }
    // If redirected from group, msgIdInInvokedChat was for the group message, so we send new to DM.

    if (messageIdToOperateOn) {
        try {
            await bot.editMessageText(rulesIntroTextHTML, { chat_id: userIdAsDmChatId, message_id: messageIdToOperateOn, ...options });
            console.log(`${LOG_PREFIX_RULES_V2} Rules menu edited successfully on message ${messageIdToOperateOn}.`);
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES_V2} Failed to edit rules menu message ${messageIdToOperateOn}, sending new. Error: ${e.message}`);
                await safeSendMessage(userIdAsDmChatId, rulesIntroTextHTML, options); 
            } else {
                console.log(`${LOG_PREFIX_RULES_V2} Rules menu message content was not modified.`);
            }
        }
    } else { 
        console.log(`${LOG_PREFIX_RULES_V2} Sending new rules menu to DM for user ${userIdAsDmChatId}.`);
        await safeSendMessage(userIdAsDmChatId, rulesIntroTextHTML, options);
    }
}

async function handleGamesOverviewMenu(msg) { // msg here is the actionMsgContext from handleMenuAction
    const userId = String(msg.from.id || msg.from.telegram_id);
    const dmChatId = String(msg.chat.id); // This will be the user's DM ID
    const messageIdToEdit = msg.message_id; // ID of the message that had the "Play Games" button
    const LOG_PREFIX_GAMES_OVERVIEW = `[GamesOverviewMenu UID:${userId}]`;

    console.log(`${LOG_PREFIX_GAMES_OVERVIEW} Displaying games overview menu. Message to edit: ${messageIdToEdit}`);

    // User object is already enriched in actionMsgContext.from
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(msg.from)); 

    const messageTextHTML = `<b>Choose Your Challenge, ${playerRefHTML}!</b> 🎯\n\n` +
                          `Select a game category or explore all our exciting offerings below. Good luck!`;
    
    const keyboard = {
        inline_keyboard: [
            // [{ text: "⚔️ Player vs. Player (PvP)", callback_data: "menu:games_pvp_list" }], // Placeholder for future
            // [{ text: "🤖 Player vs. Bot (PvE)", callback_data: "menu:games_pve_list" }], // Placeholder for future
            [{ text: "🎰 View All Games & Rules", callback_data: "menu:rules_list" }], // Directs to the rules list which shows all games
            [{ text: "⬅️ Back to Main Menu", callback_data: "menu:main" }]
        ]
    };
    // Simplified for now: "Play Games" directly takes to "menu:rules_list" which acts as an "All Games" list
    // If you want separate PvP/PvE lists, you'll need to create menu:games_pvp_list etc. and corresponding handlers.
    // For now, let's reuse menu:rules_list for showing all games which then link to their individual rules.
    // OR, if you want a different message before showing the rules list:
    // const messageTextHTML = `<b>Explore Our Games, ${playerRefHTML}!</b> 🎲\n\nBrowse all available games and their rules:`;
    // const keyboard = {
    // inline_keyboard: [
    // [{ text: "📜 Show All Games & Rules", callback_data: "menu:rules_list" }],
    // [{ text: "⬅️ Back to Main Menu", callback_data: "menu:main" }]
    // ]
    // };


    if (messageIdToEdit) {
        try {
            await bot.editMessageText(messageTextHTML, {
                chat_id: dmChatId,
                message_id: Number(messageIdToEdit),
                parse_mode: 'HTML',
                reply_markup: keyboard,
                disable_web_page_preview: true
            });
            console.log(`${LOG_PREFIX_GAMES_OVERVIEW} Games overview menu edited successfully on message ${messageIdToEdit}.`);
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_GAMES_OVERVIEW} Failed to edit message ${messageIdToEdit} for games overview, sending new. Error: ${e.message}`);
                await safeSendMessage(dmChatId, messageTextHTML, { parse_mode: 'HTML', reply_markup: keyboard, disable_web_page_preview: true });
            } else {
                console.log(`${LOG_PREFIX_GAMES_OVERVIEW} Games overview message content was not modified.`);
            }
        }
    } else {
        console.log(`${LOG_PREFIX_GAMES_OVERVIEW} No messageIdToEdit, sending new games overview menu.`);
        await safeSendMessage(dmChatId, messageTextHTML, { parse_mode: 'HTML', reply_markup: keyboard, disable_web_page_preview: true });
    }
}

async function handleDisplayGameRules(originalInvokedChatIdStr, originalMessageIdStr, gameCode, userObj, originalInvokedChatType = 'private') {
    const originalInvokedChatId = String(originalInvokedChatIdStr);
    const originalMessageId = originalMessageIdStr ? Number(originalMessageIdStr) : null;
    const userIdAsDmChatId = String(userObj.telegram_id);
    const LOG_PREFIX_RULES_DISP = `[RulesDisplay_V3_EmojiFix UID:${userIdAsDmChatId} Game:${gameCode} InvokedInChat:${originalInvokedChatId}]`; // V3 for emoji fix

    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObj));
    let botUsername = BOT_NAME || "our bot";
    try {
        const selfInfo = await bot.getMe();
        if (selfInfo.username) botUsername = selfInfo.username;
    } catch (e) { console.error(`${LOG_PREFIX_RULES_DISP} Could not fetch bot username: ${e.message}`); }

    const targetDmChatId = userIdAsDmChatId;
    let messageIdToEditInDm = (originalInvokedChatType === 'private' && originalMessageId) ? originalMessageId : null;

    let generalBettingInfoHTML = "<i>General betting information is currently unavailable.</i>\n\n";
    try {
        const solPrice = await getSolUsdPrice(); 
        const minBetInLamports = convertUSDToLamports(MIN_BET_USD_val, solPrice);
        const minBetDisplayHTML = escapeHTML(await formatBalanceForDisplay(minBetInLamports, 'USD'));
        const maxBetDisplayHTML = escapeHTML(`$${MAX_BET_USD_val.toFixed(2)}`); 
        const defaultBetDisplayHTML = minBetDisplayHTML;

        generalBettingInfoHTML = `<b>💰 General Betting Info:</b>\n` +
            ` • Place bets in USD (e.g., <code>5</code>, <code>10.50</code>).\n` +
            ` • Current Bet Limits (USD Equivalent): <b>${minBetDisplayHTML}</b> to <b>${maxBetDisplayHTML}</b>.\n` +
            ` • If no bet amount is specified when starting a game, it often defaults to the minimum bet (approx. <b>${defaultBetDisplayHTML}</b>).\n\n`;
    } catch (priceError) {
        console.error(`${LOG_PREFIX_RULES_DISP} Error fetching SOL price for generalBettingInfoHTML: ${priceError.message}`);
        generalBettingInfoHTML = "Error loading current bet limit information. Please assume standard casino limits.\n\n";
    }

    let rulesTitle = gameCode.replace(/_/g, ' ').replace(' Unified Offer', '').replace(/\b\w/g, l => l.toUpperCase());
    let gameEmoji = '📜'; // Default emoji
    let rulesTextHTML = "";

    switch (gameCode) {
        case GAME_IDS.COINFLIP: 
            gameEmoji = '🪙'; rulesTitle = "Coinflip Challenge";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}!\n\n` +
                `<b>Objective:</b>\n<i>Correctly predict the outcome of a coin flip (Heads or Tails) to win against an opponent or the Bot Dealer.</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Start with <code>/coinflip &lt;bet&gt;</code> in a group chat to make an offer.\n` +
                `• You can then choose to play against the Bot Dealer, or another player can accept your challenge for a PvP match.\n` +
                `• A choice (Heads/Tails) is made by the designated caller (you in PvB, or a randomly chosen player in PvP).\n` +
                `• Our secure system (via the Helper Bot) flips the coin.\n\n` +
                `<b>Winning:</b>\n` +
                `• If the call matches the coin flip outcome, the caller (or their side) wins!\n\n` +
                `<b>Payouts:</b>\n` +
                `• Winning typically pays 2x your bet (your stake + an equal amount in profit).\n\n` +
                generalBettingInfoHTML;
            break;

        case GAME_IDS.RPS: 
            gameEmoji = '✂️'; rulesTitle = "Rock Paper Scissors";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}!\n\n` +
                `<b>Objective:</b>\n<i>Outsmart your opponent by choosing Rock, Paper, or Scissors. Rock crushes Scissors, Scissors cuts Paper, and Paper covers Rock.</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Initiate with <code>/rps &lt;bet&gt;</code> in a group chat.\n` +
                `• You can play vs. the Bot or wait for a PvP opponent to accept.\n` +
                `• In PvP, both players secretly submit their choice (Rock, Paper, or Scissors) via DM with me.\n` +
                `• In PvB, you make your choice, and the Bot makes its move.\n` +
                `• Choices are revealed, and the winner is determined!\n\n` +
                `<b>Winning:</b>\n` +
                `• Defeat your opponent's choice based on the classic rules.\n\n` +
                `<b>Payouts:</b>\n` +
                `• Winner takes the pot (2x your bet in PvP, or 2x bet from Bot in PvB).\n` +
                `• A draw (both choose the same) results in bets being returned (Push).\n\n`+
                generalBettingInfoHTML;
            break;

        case GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER: 
            gameEmoji = '🎲'; rulesTitle = "Dice Escalator";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}! Ready for a strategic dice scoring game?\n\n` +
                `<b>Objective:</b>\n` +
                `<i>Achieve a higher score by rolling dice than your opponent (Bot Dealer or another Player) without busting.</i>\n\n` +
                `<b>How to Play (General):</b>\n` +
                `• Start with <code>/de &lt;bet&gt;</code> in a group chat. You can then choose to play vs. the Bot or await a PvP challenger.\n` +
                `• When it's your turn, send the 🎲 emoji to roll a die (value from Helper Bot).\n` +
                `• Accumulate your score over multiple rolls.\n` +
                `• <b>Busting (Player):</b> Rolling a <b>${escapeHTML(String(DICE_ESCALATOR_BUST_ON))}</b> on any die means that specific die scores 0. In PvP, a bust roll ends your turn. In PvB, it might end your turn or game depending on context.\n` +
                `• <b>Standing:</b> If satisfied with your score, press the "Stand Firm!" button (PvB/PvP when available).\n\n` +
                `<b>Player vs. Bot (PvB) Specifics:</b>\n` +
                `• After you stand, the Bot Dealer rolls exactly three dice; their sum is the Bot's score.\n` +
                `• If your score is higher, you win 2x your bet.\n` +
                `• If scores are tied, it's a Push (bet returned).\n` +
                `• 💎 <b>Jackpot:</b> Win against the Bot with a score of <b>${escapeHTML(String(TARGET_JACKPOT_SCORE))}+</b> to also win the current Super Jackpot! (${escapeHTML(String(JACKPOT_CONTRIBUTION_PERCENT * 100))}% of PvB bets contribute).\n` +
                `• 🔥 <b>Jackpot Run (PvB):</b> If your score reaches 18+, you can "Go for Jackpot!" You must then keep rolling (no standing) until you hit ${escapeHTML(String(TARGET_JACKPOT_SCORE))}+ or bust.\n\n` +
                `<b>Player vs. Player (PvP) Specifics:</b>\n` +
                `• Player 1 rolls, then stands or busts. Player 2 then rolls to beat P1's score.\n` +
                `• Highest score wins the pot (2x their bet). Ties are a Push.\n` +
                `• PvP does not involve the Super Jackpot.\n\n` +
                generalBettingInfoHTML;
            break;

        case GAME_IDS.DICE_21: 
            gameEmoji = '🃏'; rulesTitle = "Dice 21 (Blackjack)";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}!\n\n` +
                `<b>Objective:</b>\n<i>Get your dice sum closer to <b>${escapeHTML(String(DICE_21_TARGET_SCORE))}</b> than your opponent (Bot or Player) without going over (busting).</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Start with <code>/d21 &lt;bet&gt;</code> in a group. Choose to play vs. Bot or await a PvP challenger.\n` +
                `• Players receive two initial dice (you send 🎲 emoji when prompted for each).\n` +
                `• <b>Hit:</b> Send another 🎲 emoji to get an additional die.\n` +
                `• <b>Stand:</b> Click "Stand" to keep your current score and end your turn.\n` +
                `• <b>Bust:</b> If your score exceeds ${escapeHTML(String(DICE_21_TARGET_SCORE))}, you bust and lose.\n\n` +
                `<b>Bot Dealer (PvB):</b>\n` +
                `• The Bot Dealer will typically stand on a score of <b>${escapeHTML(String(DICE_21_BOT_STAND_SCORE))}</b> or more.\n\n` +
                `<b>Payouts:</b>\n` +
                `• Win (higher score than opponent, no bust): 2x bet (stake + profit).\n` +
                `• Blackjack (score of ${escapeHTML(String(DICE_21_TARGET_SCORE))} on your first two dice): Typically pays 2.5x bet.\n` +
                `• Push (tie with opponent, no bust): Bet returned.\n\n` +
                generalBettingInfoHTML;
            break;

        case GAME_IDS.OVER_UNDER_7:
            gameEmoji = '🎲'; rulesTitle = "Over/Under 7";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}!\n\n` +
                `<b>Objective:</b>\n<i>Predict if the sum of <b>${escapeHTML(String(OU7_DICE_COUNT))} dice</b> (rolled by the Helper Bot) will be Over 7, Under 7, or Exactly 7.</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Start with <code>/ou7 &lt;bet&gt;</code>.\n` +
                `• Choose your prediction: Under 7 (sums 2-6), Exactly 7, or Over 7 (sums 8-12) using the buttons.\n\n` +
                `<b>Payouts (Total Return, includes your stake):</b>\n` +
                `• Under 7 or Over 7: <b>${escapeHTML(String(OU7_PAYOUT_NORMAL + 1))}x</b> your bet.\n` +
                `• Exactly 7: A whopping <b>${escapeHTML(String(OU7_PAYOUT_SEVEN + 1))}x</b> your bet!\n\n` +
                generalBettingInfoHTML;
            break;

        case GAME_IDS.DUEL_UNIFIED_OFFER: 
            gameEmoji = '⚔️'; rulesTitle = "Duel / Highroller";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}!\n\n` +
                `<b>Objective:</b>\n<i>Achieve a higher sum with <b>${escapeHTML(String(DUEL_DICE_COUNT))} dice</b> rolls than your opponent (another Player or the Bot Dealer).</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Start with <code>/duel &lt;bet&gt;</code> in a group chat to make an offer.\n` +
                `• You can then play vs. Bot, or another player can accept for PvP.\n` +
                `• <b>Your Turn:</b> When prompted, send 🎲 emoji for each of your ${escapeHTML(String(DUEL_DICE_COUNT))} dice.\n` +
                `• <b>Bot's Turn (PvB):</b> After your rolls, the Bot Dealer gets ${escapeHTML(String(DUEL_DICE_COUNT))} dice.\n` +
                `• <b>PvP:</b> Player 1 rolls ${escapeHTML(String(DUEL_DICE_COUNT))} dice, then Player 2 rolls ${escapeHTML(String(DUEL_DICE_COUNT))} dice.\n\n` +
                `<b>Winning:</b>\n` +
                `• The player with the highest sum from their ${escapeHTML(String(DUEL_DICE_COUNT))} dice wins.\n\n` +
                `<b>Payouts:</b>\n` +
                `• Winner receives 2x their bet (stake + profit).\n` +
                `• Ties are a Push (bet returned).\n\n` +
                generalBettingInfoHTML;
            break;

        case GAME_IDS.LADDER:
            gameEmoji = '🪜'; rulesTitle = "Greed's Ladder";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}!\n\n` +
                `<b>Objective:</b>\n<i>Climb the ladder by achieving a high sum with <b>${escapeHTML(String(LADDER_ROLL_COUNT))} dice</b> (rolled by Helper Bot). But beware the bust!</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Start with <code>/ladder &lt;bet&gt;</code>. All dice are rolled at once by the Helper Bot.\n` +
                `• <b>Busting:</b> Rolling a <b>${escapeHTML(String(LADDER_BUST_ON))}</b> on ANY of the ${escapeHTML(String(LADDER_ROLL_COUNT))} dice means you bust and lose your wager immediately!\n\n` +
                `<b>Payouts (Based on Total Sum if NO Bust - Payouts are total return including stake):</b>\n`;
            LADDER_PAYOUTS.forEach(p => {
                rulesTextHTML += ` • Sum <b>${escapeHTML(String(p.min))}-${escapeHTML(String(p.max))}</b>: <b>${escapeHTML(String(p.multiplier + 1))}x</b> bet <i>(${escapeHTML(p.label)})</i>\n`;
            });
            rulesTextHTML += `\n` + generalBettingInfoHTML;
            break;

        case GAME_IDS.SEVEN_OUT: 
            gameEmoji = '🎲'; rulesTitle = "Lucky Sum (Fast Sevens)"; // Updated title
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}!\n\n` +
                `<b>Objective:</b>\n<i>Roll two dice. Certain sums win, others lose instantly! It's a quick thrill.</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Start with <code>/s7 &lt;bet&gt;</code> (or <code>/sevenout</code>, <code>/craps</code>).\n` +
                `• Two dice are rolled for you by the casino (via animated dice sent by the bot).\n\n` + // Clarified who sends animated dice
                `<b>Outcomes & Payouts (Total Return, includes your stake):</b>\n`;
            for (const sumKey in LUCKY_SUM_PAYOUTS) { 
                const payoutInfo = LUCKY_SUM_PAYOUTS[sumKey];
                rulesTextHTML += ` • Roll a sum of <b>${escapeHTML(sumKey)}</b> (${escapeHTML(payoutInfo.label)}): <b>${escapeHTML(String(payoutInfo.multiplier + 1))}x</b> bet\n`;
            }
            rulesTextHTML += ` • Rolling a sum of <b>${LUCKY_SUM_LOSING_NUMBERS.map(n => escapeHTML(String(n))).join(', ')}</b> results in a loss.\n\n` +
                generalBettingInfoHTML;
            break;

        case GAME_IDS.SLOT_FRENZY:
            gameEmoji = '🎰'; rulesTitle = "Slot Frenzy";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}!\n\n` +
                `<b>Objective:</b>\n<i>Spin the reels and match symbols for big wins! The outcome is determined by a single value (1-64) from our Helper Bot, mapped to slot combinations.</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Start with <code>/slot &lt;bet&gt;</code>.\n` +
                `• The Helper Bot provides a spin value, and your win is determined by the payout table.\n\n` +
                `<b>Payouts (Based on current configuration; total return including stake):</b>\n`;
            for (const key in SLOT_PAYOUTS) { 
                if (SLOT_PAYOUTS[key].multiplier >= 0) { 
                    rulesTextHTML += ` • ${escapeHTML(SLOT_PAYOUTS[key].symbols)} (${escapeHTML(SLOT_PAYOUTS[key].label)}): <b>${escapeHTML(String(SLOT_PAYOUTS[key].multiplier + 1))}x</b> bet\n`;
                }
            }
            rulesTextHTML += ` • Other combinations result in a loss.\n\n` +
                generalBettingInfoHTML;
            break;
            
        case GAME_IDS.MINES:
            gameEmoji = '💣'; rulesTitle = "Mines Field Sweeper";
            rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} - Rules & How to Play</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                `Hey ${playerRefHTML}! Navigate the treacherous Mines Field and uncover hidden gems!\n\n`+
                `<b>Objective:</b>\n<i>Reveal safe tiles (gems 💎) while avoiding hidden mines 💣. Each gem found increases your potential payout multiplier. Cash out at any time after finding at least one gem, or try to find all gems for the max prize!</i>\n\n` +
                `<b>How to Play:</b>\n` +
                `• Start with <code>/mines &lt;bet&gt;</code> in a group chat.\n` +
                `• You'll be prompted to select a difficulty (e.g., Easy, Medium, Hard), which sets the grid size and number of mines.\n` +
                `• Click on the grid buttons to reveal tiles.\n` +
                `• If you reveal a Mine 💣, the game ends, and your bet is lost.\n` +
                `• If you reveal a Gem 💎, your potential winnings increase based on the multiplier for that number of gems at your chosen difficulty.\n` +
                `• You can <b>"Cash Out"</b> your current winnings at any point after finding at least one gem by clicking the button.\n\n` +
                `<b>Payouts:</b>\n` +
                `• Multipliers increase with each gem found and vary by difficulty. Check the game screen for current multiplier and potential payout.\n` +
                `• Max Payout: Find all gems without hitting a mine!\n\n` +
                `<b>Example Difficulty (Easy - ${escapeHTML(MINES_DIFFICULTY_CONFIG.easy.rows + "x" + MINES_DIFFICULTY_CONFIG.easy.cols)}, ${escapeHTML(String(MINES_DIFFICULTY_CONFIG.easy.mines))} Mines):</b>\n`+
                ` • 1 Gem: x${escapeHTML(MINES_DIFFICULTY_CONFIG.easy.multipliers[1].toFixed(2))}\n` +
                ` • 5 Gems: x${escapeHTML(MINES_DIFFICULTY_CONFIG.easy.multipliers[5].toFixed(2))}\n` +
                ` • 10 Gems: x${escapeHTML(MINES_DIFFICULTY_CONFIG.easy.multipliers[10].toFixed(2))}\n` +
                `   (...multipliers continue to increase...)\n\n` +
                `<b>Warning:</b> The more gems you uncover, the higher the risk! Play strategically!\n\n` +
                generalBettingInfoHTML;
            break;

        default:
            // Default case uses gameEmoji which is initialized to '📜'
            if (!rulesTextHTML) { 
                rulesTitle = "Unknown Game";
                rulesTextHTML = `${gameEmoji} <b>${escapeHTML(rulesTitle)} Rules</b> ${gameEmoji}\n\n` + // Corrected to gameEmoji
                                `Hey ${playerRefHTML}!\n\n` +
                                `📜 Rules for "<code>${escapeHTML(gameCode)}</code>" are currently under construction or this is not a primary game entry.\n` +
                                `Please select a game from the main rules list.`;
            }
    }
    rulesTextHTML += `\n\nPlay smart, play responsibly, and may fortune favor your spin! 🍀`; 

    const keyboard = { inline_keyboard: [[{ text: "📚 Back to Rules List", callback_data: "menu:rules_list" }]] }; 
    const options = { parse_mode: 'HTML', reply_markup: keyboard, disable_web_page_preview: true };

    if (messageIdToEditInDm) {
        try {
            await bot.editMessageText(rulesTextHTML, { chat_id: userIdAsDmChatId, message_id: Number(messageIdToEditInDm), ...options });
            console.log(`${LOG_PREFIX_RULES_DISP} Rules for ${gameCode} edited successfully on message ${messageIdToEditInDm}.`);
        } catch (e) {
            if (!e.message || !e.message.toLowerCase().includes("message is not modified")) {
                console.warn(`${LOG_PREFIX_RULES_DISP} Failed to edit rules message ${messageIdToEditInDm} for ${gameCode}, sending new. Error: ${e.message}`);
                await safeSendMessage(userIdAsDmChatId, rulesTextHTML, options); 
            } else {
                console.log(`${LOG_PREFIX_RULES_DISP} Rules message content for ${gameCode} was not modified.`);
            }
        }
    } else { 
        console.log(`${LOG_PREFIX_RULES_DISP} Sending new rules message to DM for ${gameCode} for user ${userIdAsDmChatId}.`);
        await safeSendMessage(userIdAsDmChatId, rulesTextHTML, options);
    }
}

// --- Other command handlers from Part 5a, Section 2 (handleStartMinesCommand, handleBalanceCommand, etc.) ---
// --- These are assumed to be here and are unchanged from your last full version of this part unless specified. ---

// This function is in "Part 5a, Section 2"
async function handleStartMinesCommand(msg, args, userObj) {
    const userId = String(userObj.telegram_id);
    const chatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_MINES_START = `[Mines_StartOffer_HTML UID:${userId} CH:${chatId}]`;

    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObj)); 
    let betAmountLamports;

    if (chatType === 'private') {
        await safeSendMessage(chatId, `💣 Hey ${playerRefHTML}, the Mines game is best played in a <b>group chat</b>. Please use <code>/mines &lt;bet&gt;</code> there to choose a difficulty and start your treasure hunt!`, { parse_mode: 'HTML' });
        return;
    }
    
    try {
        betAmountLamports = await parseBetAmount(args[0], chatId, msg.chat.type, userId);
        if (!betAmountLamports || betAmountLamports <= 0n) { 
            await safeSendMessage(chatId, `${playerRefHTML}, please specify a valid positive bet amount for Mines.<br>Example: <code>/mines 10</code> or <code>/mines 0.1 sol</code>`, { parse_mode: 'HTML' });
            return;
        }
    } catch (e) { 
        console.error(`${LOG_PREFIX_MINES_START} Error parsing bet amount: ${e.message}`);
        await safeSendMessage(chatId, `${playerRefHTML}, there was an issue with your bet amount. Please use USD (e.g., <code>5</code>) or SOL (e.g., <code>0.1 sol</code>).`, { parse_mode: 'HTML' });
        return;
    }

    console.log(`${LOG_PREFIX_MINES_START} Initiating Mines offer. Bet: ${betAmountLamports} lamports.`);
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports, 'USD'));

    const currentUserDetails = await getOrCreateUser(userId); 
    if (!currentUserDetails || BigInt(currentUserDetails.balance) < betAmountLamports) {
        const neededDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(betAmountLamports - BigInt(currentUserDetails?.balance || 0), 'USD'));
        await safeSendMessage(chatId, `${playerRefHTML}, your balance is too low for a <b>${betDisplayUSD_HTML}</b> Mines game.<br>You need about <b>${neededDisplayUSD_HTML}</b> more.`, {
            parse_mode: 'HTML',
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
        initiatorMentionHTML: playerRefHTML, 
        initiatorUserObj: currentUserDetails, 
        betAmount: betAmountLamports,
        status: 'awaiting_difficulty', 
        creationTime: Date.now(),
        offerMessageId: null 
    };
    activeGames.set(offerId, offerData);
    await updateGroupGameDetails(chatId, offerId, GAME_IDS.MINES_OFFER, betAmountLamports);

    let difficultyButtons = [];
    for (const diffKey in MINES_DIFFICULTY_CONFIG) { 
        const diffConfig = MINES_DIFFICULTY_CONFIG[diffKey];
        // MODIFIED LINE: Use TILE_EMOJI_MINE as default if diffConfig.emoji is not set
        difficultyButtons.push({ 
            text: `${diffConfig.emoji || TILE_EMOJI_MINE} ${escapeHTML(diffConfig.label)}`, // Use TILE_EMOJI_MINE
            callback_data: `mines_difficulty_select:${offerId}:${diffKey}` 
        });
    }
    
    const difficultyKeyboardRows = [];
    for (let i = 0; i < difficultyButtons.length; i += 2) { 
        difficultyKeyboardRows.push(difficultyButtons.slice(i, i + 2));
    }
    difficultyKeyboardRows.push([{ text: "❌ Cancel Offer", callback_data: `mines_cancel_offer:${offerId}` }]);

    const offerMessageTextHTML = `💣 <b>Mines Challenge by ${playerRefHTML}!</b> 💣\n\n` +
                                 `Wager: <b>${betDisplayUSD_HTML}</b>\n\n` +
                                 `${playerRefHTML}, please select your desired difficulty level below to start your treasure hunt:`;
    
    const sentMessage = await safeSendMessage(chatId, offerMessageTextHTML, {
        parse_mode: 'HTML',
        reply_markup: { inline_keyboard: difficultyKeyboardRows }
    });

    if (sentMessage?.message_id) {
        const currentOffer = activeGames.get(offerId);
        if (currentOffer) {
            currentOffer.offerMessageId = String(sentMessage.message_id); 
            activeGames.set(offerId, currentOffer);
        }
        
        setTimeout(async () => {
            const timedOutOffer = activeGames.get(offerId);
            if (timedOutOffer && timedOutOffer.status === 'awaiting_difficulty') {
                console.log(`${LOG_PREFIX_MINES_START} Mines offer ${offerId} timed out waiting for difficulty selection.`);
                activeGames.delete(offerId);
                await updateGroupGameDetails(chatId, null, null, null); 
                if (timedOutOffer.offerMessageId && bot) {
                    const timeoutBetDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(timedOutOffer.betAmount, 'USD'));
                    // Ensure initiatorMentionHTML is used from the timedOutOffer data
                    const initiatorMentionHTML_Timeout = timedOutOffer.initiatorMentionHTML || escapeHTML(getPlayerDisplayReference(timedOutOffer.initiatorUserObj || {telegram_id: timedOutOffer.initiatorId, first_name: "Player"}));
                    await bot.editMessageText(
                        `⏳ The Mines game offer by ${initiatorMentionHTML_Timeout} for <b>${timeoutBetDisplayUSD_HTML}</b> expired as no difficulty was chosen.`,
                        { chat_id: String(chatId), message_id: Number(timedOutOffer.offerMessageId), parse_mode: 'HTML', reply_markup: {} }
                    ).catch(e => console.warn(`${LOG_PREFIX_MINES_START} Error editing timed out mines offer msg: ${e.message}`));
                }
            }
        }, JOIN_GAME_TIMEOUT_MS); 
    } else {
        console.error(`${LOG_PREFIX_MINES_START} Failed to send Mines difficulty selection message.`);
        activeGames.delete(offerId); 
        await updateGroupGameDetails(chatId, null, null, null);
        await safeSendMessage(chatId, "⚙️ Oops! Couldn't start the Mines game offer. Please try again.", { parse_mode: 'HTML' });
    }
}

async function handleBalanceCommand(msg) {
    const userId = String(msg.from.id || msg.from.telegram_id);
    const commandChatId = String(msg.chat.id);
    const chatType = msg.chat.type;
    const LOG_PREFIX_BAL = `[BalanceCmd UID:${userId} CH:${commandChatId}]`;

    const user = await getOrCreateUser(userId, msg.from.username, msg.from.first_name, msg.from.last_name);
    if (!user) {
        await safeSendMessage(commandChatId, "😕 Apologies! We couldn't fetch your player profile to show your balance. Please try \`/start\` again.", { parse_mode: 'MarkdownV2' });
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
        `-------------------------------\n` + 
        `💰 Approx. Total Value: *${escapeMarkdownV2(balanceUSDShort)}*\n` +
        `🪙 SOL Balance: *${escapeMarkdownV2(balanceSOLShort)}*\n` +
        `⚙️ Lamports: \`${escapeMarkdownV2(String(balanceLamports))}\`\n` +
        `-------------------------------\n\n` + 
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

        const debitResult = await updateUserBalanceAndLedger(client,tipperId,-tipAmountLamports,'tip_sent',{},ledgerNoteTipper);
        if (!debitResult.success) throw new Error(debitResult.error || "Failed to debit your balance for the tip.");
        const creditResult = await updateUserBalanceAndLedger(client,recipientId,tipAmountLamports,'tip_received',{},ledgerNoteRecipient);
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
        if (tipMessage) { recipientNotification += `\nMessage: "_${escapeMarkdownV2(tipMessage)}_"`;}
        recipientNotification += `\nYour new balance is approx. *${recipientNewBalanceDisplayUSD}*.`;
        await safeSendMessage(recipientId, recipientNotification, { parse_mode: 'MarkdownV2' });
    } catch (error) {
        if (client) { await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error: ${rbErr.message}`));}
        console.error(`${logPrefix} Error processing tip: ${error.message}`, error.stack?.substring(0, 700));
        await safeSendMessage(chatId, `⚙️ An error occurred while processing your tip: \`${escapeMarkdownV2(error.message)}\`. Please try again.`, { parse_mode: 'MarkdownV2' });
        if (error.message.includes("Failed to credit recipient")) {
             if(typeof notifyAdmin === 'function' && ADMIN_USER_ID) { 
                notifyAdmin(`🚨 CRITICAL TIP FAILURE 🚨\nTipper: ${tipperId} (${tipperUserObj.username || 'N/A'})\nRecipient: ${recipientId} (${recipientUserObj.username || 'N/A'})\nAmount: ${tipAmountLamports} lamports.\nTipper was likely debited but recipient NOT credited. MANUAL VERIFICATION & CORRECTION REQUIRED.\nError: ${escapeMarkdownV2(error.message)}`,{parse_mode: 'MarkdownV2'}).catch(err => console.error("Failed to notify admin about critical tip failure:", err));
             }
        }
    } finally {
        if (client) { client.release(); }
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
// --- End of Part 5a, Section 2 ---
// --- Start of Part 5a, Section 4 (REVISED for New Dice Escalator UI & Simplified Post-Game Keyboard) ---
// index.js - Part 5a, Section 4: UI Helpers and Shared Utilities for General Commands & Simple Group Games
//----------------------------------------------------------------------------------------------------
// Assumed dependencies from previous Parts:
// Part 1: GAME_IDS (with new DE, CF, RPS IDs), QUICK_DEPOSIT_CALLBACK_ACTION, RULES_CALLBACK_PREFIX, escapeMarkdownV2
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
        case GAME_IDS.COINFLIP: // Base Coinflip ID
        case GAME_IDS.COINFLIP_PVB:
        case GAME_IDS.COINFLIP_PVP:
        case GAME_IDS.COINFLIP_UNIFIED_OFFER: // In case it's ever used directly here
            playAgainCallbackActionPrefix = 'coinflip'; // Results in 'play_again_coinflip:bet'
            break;
        case GAME_IDS.RPS: // Base RPS ID
        case GAME_IDS.RPS_PVB:
        case GAME_IDS.RPS_PVP:
        case GAME_IDS.RPS_UNIFIED_OFFER: // In case it's ever used directly here
            playAgainCallbackActionPrefix = 'rps'; // Results in 'play_again_rps:bet'
            break;
        case GAME_IDS.DICE_ESCALATOR_PVB:
            playAgainCallbackActionPrefix = 'de_pvb'; // Results in 'play_again_de_pvb:bet'
            break;
        case GAME_IDS.DICE_ESCALATOR_PVP:
        case GAME_IDS.DICE_ESCALATOR_UNIFIED_OFFER: // If unified offer result needs play again
            playAgainCallbackActionPrefix = 'de_pvp'; // Or simply 'de' if PvP restarts unified offer
            break;
        case GAME_IDS.DICE_21: // PvB Dice 21
        case GAME_IDS.DICE_21_UNIFIED_OFFER:
            playAgainCallbackActionPrefix = 'd21'; // Results in 'play_again_d21:bet'
            break;
        case GAME_IDS.DICE_21_PVP:
            playAgainCallbackActionPrefix = 'd21_pvp'; // Results in 'play_again_d21_pvp:bet'
            break;
        case GAME_IDS.DUEL_PVB:
        case GAME_IDS.DUEL_PVP:
        case GAME_IDS.DUEL_UNIFIED_OFFER:
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
        case GAME_IDS.MINES: // If Mines has a play again that directly starts new game
        case GAME_IDS.MINES_OFFER: // If unified offer for Mines has play again
            playAgainCallbackActionPrefix = 'mines'; // Results in 'play_again_mines:bet'
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
// --- End of Part 5a, Section // --- Start of Part 5a, Section 1 (REVISED for New Coinflip/RPS, Dice Escalator Jackpot Choice & OU7 // --- Start of Part 5a, Section 1 (REVISED for New Coinflip/RPS, Dice Escalator Jackpot Choice & OU7 // --- Start of Part 5a, Section 1 (REVISED for New Coinflip/RPS, Dice Escalator Jackpot Choice & OU7 Fix) ---
// --- Start of Part 5a, Section 1 (REVISED for New Coinflip/RPS, Dice Escalator Jackpot Choice & OU7 Fix) ---
// index.js - Part 5a, Section 1: Core Listeners Setup (Message & Callback) and Populated Routers
// (This entire block should be placed LATE in your index.js, AFTER all game logic, general commands, and UI helpers, but BEFORE Part 6)
//----------------------------------------------------------------------------------------------
// console.log("Loading Part 5a, Section 1 (REVISED for New Coinflip/RPS, Dice Escalator Jackpot Choice & OU7 Fix)...");

// Dependencies from previous Parts (assumed to be globally available or correctly imported)
// Part 1: isShuttingDown, userStateCache, COMMAND_COOLDOWN_MS, bot, getPlayerDisplayReference,
//         safeSendMessage, escapeMarkdownV2, MIN_BET_USD_val, MAX_BET_USD_val, LAMPORTS_PER_SOL,
//         getSolUsdPrice, convertUSDToLamports, convertLamportsToUSDString, ADMIN_USER_ID, BOT_NAME,
//         MIN_BET_AMOUNT_LAMPORTS_config, MAX_BET_AMOUNT_LAMPORTS_config, stringifyWithBigInt,
//         RULES_CALLBACK_PREFIX, DEPOSIT_CALLBACK_ACTION, WITHDRAW_CALLBACK_ACTION, QUICK_DEPOSIT_CALLBACK_ACTION,
//         userCooldowns, pool, activeGames, groupGameSessions, GAME_IDS (with new DE, CF, RPS IDs and MINES_OFFER)
// Part 2: getOrCreateUser, findRecipientUser
// Part 3: createUserMention, formatCurrency
// Part P3: clearUserState, routeStatefulInput, handleMenuAction, handleWithdrawalConfirmation
// Game Logic Parts (e.g., Part 5a-S3 for Coinflip/RPS, Part 5b-S1 for Dice Escalator): Game logic functions are now defined *before* this section.
// Mines handlers like handleStartMinesCommand (from Part 5a, Section 2) are defined before this.


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
        } else {
            const argStr = String(arg).trim().toLowerCase();
            let isExplicitSol = argStr.endsWith('sol');
            let isExplicitLamports = argStr.endsWith('lamports');
            let potentialNumberPart = argStr.replace('sol', '').replace('lamports', '').trim();
            let parsedValueFloat = parseFloat(potentialNumberPart);
            let parsedValueBigInt = null;
            try { parsedValueBigInt = BigInt(potentialNumberPart); } catch {}

            if (isExplicitSol) {
                if (isNaN(parsedValueFloat) || parsedValueFloat <= 0) throw new Error("Invalid amount for 'sol' suffix.");
                betAmountLamports = BigInt(Math.floor(parsedValueFloat * Number(LAMPORTS_PER_SOL)));
                const equivalentUsdValue = parsedValueFloat * solPrice;
                if (equivalentUsdValue < MIN_BET_USD_val || equivalentUsdValue > MAX_BET_USD_val) {
                    const betInSOLDisplayDynamic = escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL'));
                    const message = `⚠️ Your bet of *${betInSOLDisplayDynamic}* (approx. ${escapeMarkdownV2(convertLamportsToUSDString(betAmountLamports, solPrice))}) is outside current USD limits (*${minBetDisplay}* - *${maxBetDisplay}*). Your bet is set to the minimum: *${defaultBetDisplay}*.`;
                    await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                    betAmountLamports = minBetLamports;
                }
            } else if (isExplicitLamports) {
                if (parsedValueBigInt === null || parsedValueBigInt <= 0n) throw new Error("Invalid amount for 'lamports' suffix.");
                betAmountLamports = parsedValueBigInt;
                const equivalentUsdValue = Number(betAmountLamports) / Number(LAMPORTS_PER_SOL) * solPrice;
                if (equivalentUsdValue < MIN_BET_USD_val || equivalentUsdValue > MAX_BET_USD_val) {
                    const betInLamportsDisplay = escapeMarkdownV2(formatCurrency(betAmountLamports, 'lamports', true));
                    const message = `⚠️ Your bet of *${betInLamportsDisplay}* (approx. ${escapeMarkdownV2(convertLamportsToUSDString(betAmountLamports, solPrice))}) is outside current USD limits (*${minBetDisplay}* - *${maxBetDisplay}*). Your bet is set to the minimum: *${defaultBetDisplay}*.`;
                    await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                    betAmountLamports = minBetLamports;
                }
            } else {
                if (!isNaN(parsedValueFloat) && parsedValueFloat > 0) {
                    const usdThreshold = MAX_BET_USD_val * 1.5;

                    if ( (argStr.includes('.')) || (!argStr.includes('.') && parsedValueFloat <= usdThreshold && parsedValueFloat >= MIN_BET_USD_val) ) {
                        let usdAmountToConvert = parsedValueFloat;
                        betAmountLamports = convertUSDToLamports(usdAmountToConvert, solPrice);

                        if (usdAmountToConvert < MIN_BET_USD_val || usdAmountToConvert > MAX_BET_USD_val) {
                            const betUsdDisplay = escapeMarkdownV2(usdAmountToConvert.toFixed(2));
                            const message = `⚠️ Your bet of *${betUsdDisplay} USD* is outside the allowed limits: *${minBetDisplay}* - *${maxBetDisplay}*. Your bet has been adjusted to the minimum: *${defaultBetDisplay}*.`;
                            await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });
                            betAmountLamports = minBetLamports;
                        }
                    } else {
                        if (parsedValueBigInt !== null && parsedValueBigInt > 0n) {
                            betAmountLamports = parsedValueBigInt;
                            const equivalentUsdValue = Number(betAmountLamports) / Number(LAMPORTS_PER_SOL) * solPrice;

                            if (betAmountLamports === minBetLamports) {
                            } else if (betAmountLamports === maxBetLamports) {
                            } else if (equivalentUsdValue < MIN_BET_USD_val || equivalentUsdValue > MAX_BET_USD_val) {
                                const betInSOLDisplayDynamic = escapeMarkdownV2(formatCurrency(betAmountLamports, 'SOL'));
                                let adjustmentMessage;
                                if (equivalentUsdValue < MIN_BET_USD_val) {
                                    adjustmentMessage = `⚠️ Your bet of *${betInSOLDisplayDynamic}* (approx. ${escapeMarkdownV2(convertLamportsToUSDString(betAmountLamports, solPrice))}) is below the minimum limit of *${minBetDisplay}*. Adjusted to minimum.`;
                                    betAmountLamports = minBetLamports;
                                } else {
                                    adjustmentMessage = `⚠️ Your bet of *${betInSOLDisplayDynamic}* (approx. ${escapeMarkdownV2(convertLamportsToUSDString(betAmountLamports, solPrice))}) exceeds the maximum limit of *${maxBetDisplay}*. Adjusted to maximum.`;
                                    betAmountLamports = maxBetLamports;
                                }
                                await safeSendMessage(commandInitiationChatId, adjustmentMessage, { parse_mode: 'MarkdownV2' });
                            }
                        } else {
                            throw new Error("Invalid numeric bet value provided (large integer path).");
                        }
                    }
                } else {
                    throw new Error("Could not parse bet amount. Use numbers, or 'sol'/'lamports' suffix.");
                }
            }
        }

        const effectiveMinLamportsSystem = MIN_BET_AMOUNT_LAMPORTS_config;
        const effectiveMaxLamportsSystem = MAX_BET_AMOUNT_LAMPORTS_config;

        if (betAmountLamports === minBetLamports) { 
            if (betAmountLamports > effectiveMaxLamportsSystem) { 
                const adjustedMaxDisplaySystem = await formatBalanceForDisplay(effectiveMaxLamportsSystem, 'USD', solPrice);
                console.warn(`${LOG_PREFIX_PBA} minBetLamports (${formatCurrency(betAmountLamports)}) somehow exceeds effectiveMaxLamportsSystem (${formatCurrency(effectiveMaxLamportsSystem)}). Clamping to max.`);
                await safeSendMessage(commandInitiationChatId, `ℹ️ Your $${MIN_BET_USD_val.toFixed(2)} bet (converted to lamports) unusually exceeded the system's absolute maximum. Adjusted to *${escapeMarkdownV2(adjustedMaxDisplaySystem)}*.`, { parse_mode: 'MarkdownV2' });
                return effectiveMaxLamportsSystem;
            }
            if (betAmountLamports < effectiveMinLamportsSystem) {
                console.warn(`${LOG_PREFIX_PBA} minBetLamports derived from USD (${formatCurrency(betAmountLamports)}) is less than absolute system min (${formatCurrency(effectiveMinLamportsSystem)}). Using system min.`);
                return effectiveMinLamportsSystem;
            }
            return betAmountLamports;
        }

        if (betAmountLamports < effectiveMinLamportsSystem) {
            const adjustedMinDisplaySystem = await formatBalanceForDisplay(effectiveMinLamportsSystem, 'USD', solPrice);
            console.warn(`${LOG_PREFIX_PBA} Bet ${formatCurrency(betAmountLamports)} is BELOW absolute system lamport limit ${formatCurrency(effectiveMinLamportsSystem)}. Adjusting to ${escapeMarkdownV2(adjustedMinDisplaySystem)}.`);
            await safeSendMessage(commandInitiationChatId, `ℹ️ Your specified bet was below the system's absolute minimum value and has been adjusted to *${escapeMarkdownV2(adjustedMinDisplaySystem)}*.`, { parse_mode: 'MarkdownV2' });
            return effectiveMinLamportsSystem;
        }
        if (betAmountLamports > effectiveMaxLamportsSystem) {
            if (betAmountLamports === maxBetLamports) { 
                if (betAmountLamports > effectiveMaxLamportsSystem) { 
                    console.warn(`${LOG_PREFIX_PBA} maxBetLamports derived from USD (${formatCurrency(betAmountLamports)}) is greater than absolute system max (${formatCurrency(effectiveMaxLamportsSystem)}). Using system max.`);
                    return effectiveMaxLamportsSystem;
                }
            } else {
                const adjustedMaxDisplaySystem = await formatBalanceForDisplay(effectiveMaxLamportsSystem, 'USD', solPrice);
                console.warn(`${LOG_PREFIX_PBA} Bet ${formatCurrency(betAmountLamports)} is ABOVE absolute system lamport limit ${formatCurrency(effectiveMaxLamportsSystem)}. Adjusting to ${escapeMarkdownV2(adjustedMaxDisplaySystem)}.`);
                await safeSendMessage(commandInitiationChatId, `ℹ️ Your specified bet exceeded the system's absolute maximum value and has been adjusted to *${escapeMarkdownV2(adjustedMaxDisplaySystem)}*.`, { parse_mode: 'MarkdownV2' });
                return effectiveMaxLamportsSystem;
            }
        }
        return betAmountLamports;

    } catch (priceError) {
        console.error(`${LOG_PREFIX_PBA} CRITICAL error during bet parsing (e.g. SOL price unavailable): ${priceError.message}`);
        const minLamportsFallbackDisplay = escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT_LAMPORTS_config, 'SOL'));
        const message = `⚙️ Apologies, we couldn't determine current bet limits due to a price feed issue. Your bet has been set to the internal minimum of *${minLamportsFallbackDisplay}*.`;
        await safeSendMessage(commandInitiationChatId, message, { parse_mode: 'MarkdownV2' });

        try { 
            if (!arg || String(arg).trim() === "") return MIN_BET_AMOUNT_LAMPORTS_config;
            let fallbackAmountLamports;
            const argStrFB = String(arg).trim().toLowerCase();
            if (argStrFB.endsWith('sol')) {
                const solValFB = parseFloat(argStrFB.replace('sol', '').trim());
                if (isNaN(solValFB) || solValFB <=0) return MIN_BET_AMOUNT_LAMPORTS_config;
                fallbackAmountLamports = BigInt(Math.floor(solValFB * Number(LAMPORTS_PER_SOL)));
            } else if (argStrFB.endsWith('lamports')) {
                const lampValFB = BigInt(argStrFB.replace('lamports','').trim());
                if (lampValFB <= 0n) return MIN_BET_AMOUNT_LAMPORTS_config;
                fallbackAmountLamports = lampValFB;
            } else { 
                try {
                    const numValFB = BigInt(argStrFB);
                    if (numValFB <=0n) return MIN_BET_AMOUNT_LAMPORTS_config;
                    fallbackAmountLamports = numValFB;
                } catch (e) { 
                    return MIN_BET_AMOUNT_LAMPORTS_config; 
                }
            }

            if (fallbackAmountLamports < MIN_BET_AMOUNT_LAMPORTS_config) return MIN_BET_AMOUNT_LAMPORTS_config;
            if (fallbackAmountLamports > MAX_BET_AMOUNT_LAMPORTS_config) return MAX_BET_AMOUNT_LAMPORTS_config;
            return fallbackAmountLamports;
        } catch {
            return MIN_BET_AMOUNT_LAMPORTS_config;
        }
    }
};


// --- Main Message Handler (`bot.on('message')`) ---
bot.on('message', async (msg) => {
    const LOG_PREFIX_MSG_HANDLER = `[MsgHandler TID:${msg.message_id || 'N/A'} OriginUID:${msg.from?.id || 'N/A'} ChatID:${msg.chat?.id || 'N/A'}]`;
    // console.log(`${LOG_PREFIX_MSG_HANDLER} Received message: ${msg.text ? `Text: "${msg.text}"` : (msg.dice ? `Dice: Value ${msg.dice.value}` : "Non-text/dice message")}`); // Reduced this log for brevity

    if (isShuttingDown) {
        // console.log(`${LOG_PREFIX_MSG_HANDLER} Bot is shutting down. Ignoring message.`); // Reduced log
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
                // console.log(`${LOG_PREFIX_MSG_HANDLER} Ignoring message from other bot: ${msg.from.username || msg.from.id}`); // Reduced log
                return;
            }
            if (!msg.dice) { // Only process self-sent dice messages (for Helper Bot system)
                // console.log(`${LOG_PREFIX_MSG_HANDLER} Ignoring self-sent non-dice message.`); // Reduced log
                return;
            }
            console.log(`${LOG_PREFIX_MSG_HANDLER} Processing self-sent dice message (value: ${msg.dice.value}).`); // Keep this for Helper Bot debug
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
        // console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] User ${rollerId} sent 🎲 (value: ${diceValue}) in chat ${chatId}. ActiveGames size: ${activeGames.size}`); // Can be noisy

        let gameIdForDiceRoll = null;
        let gameDataForDiceRoll = null;
        let isDiceEscalatorEmoji = false;
        let isDice21Emoji = false;
        let isDuelGameEmoji = false;

        for (const [gId, gData] of activeGames.entries()) { 
            if (String(gData.chatId) === chatId) {
                if (gData.type === GAME_IDS.DICE_ESCALATOR_PVB && gData.player?.userId === rollerId && 
                    (gData.status === 'player_turn_awaiting_emoji' || gData.status === 'player_score_18_plus_awaiting_choice')) { 
                    gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDiceEscalatorEmoji = true; break;
                }
                if (gData.type === GAME_IDS.DICE_ESCALATOR_PVP) {
                    const isPlayerTurnInDEPvP = (gData.initiator?.userId === rollerId && gData.initiator?.isTurn && gData.initiator?.status === 'awaiting_roll_emoji') ||
                                                (gData.opponent?.userId === rollerId && gData.opponent?.isTurn && gData.opponent?.status === 'awaiting_roll_emoji');
                    if (isPlayerTurnInDEPvP) {
                        gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDiceEscalatorEmoji = true; break;
                    }
                }
                if (gData.type === GAME_IDS.DICE_21 && gData.playerId === rollerId &&
                    (gData.status === 'player_turn_hit_stand_prompt' || 
                     gData.status === 'player_initial_roll_1_prompted' ||
                     gData.status === 'player_initial_roll_2_prompted')) {
                    gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDice21Emoji = true; break;
                }
                if (gData.type === GAME_IDS.DICE_21_PVP) {
                    const isPlayerTurnInD21PvP = (gData.initiator?.userId === rollerId && gData.initiator?.isTurn && gData.initiator?.status === 'playing_turn') ||
                                               (gData.opponent?.userId === rollerId && gData.opponent?.isTurn && gData.opponent?.status === 'playing_turn');
                    if (isPlayerTurnInD21PvP) {
                        gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDice21Emoji = true; break;
                    }
                }
                if (gData.type === GAME_IDS.DUEL_PVB && gData.playerId === rollerId && 
                    (gData.status === 'player_awaiting_roll1_emoji' || gData.status === 'player_awaiting_roll2_emoji')) {
                    gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDuelGameEmoji = true; break;
                }
                if (gData.type === GAME_IDS.DUEL_PVP) {
                    const isPlayerTurnInDuelPvP = (gData.initiator?.userId === rollerId && gData.initiator?.isTurn && gData.initiator?.status === 'awaiting_roll_emoji') ||
                                                  (gData.opponent?.userId === rollerId && gData.opponent?.isTurn && gData.opponent?.status === 'awaiting_roll_emoji');
                    if (isPlayerTurnInDuelPvP) {
                        gameIdForDiceRoll = gId; gameDataForDiceRoll = gData; isDuelGameEmoji = true; break;
                    }
                }
            }
        }

        if (gameIdForDiceRoll && gameDataForDiceRoll) {
            // console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Processing dice for game ${gameIdForDiceRoll} (Type: ${gameDataForDiceRoll.type}). Deleting user dice message ${msg.message_id}.`); // Reduced log
            bot.deleteMessage(chatId, msg.message_id).catch(() => { /* console.warn(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Failed to delete user's dice emoji message ${msg.message_id}`); */ }); // Reduced log

            if (isDiceEscalatorEmoji) {
                // console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Routing to Dice Escalator processor.`); // Reduced log
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
                // console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Routing to Dice 21 processor.`); // Reduced log
                if (gameDataForDiceRoll.type === GAME_IDS.DICE_21) { 
                    if (typeof processDice21PvBRollByEmoji === 'function') await processDice21PvBRollByEmoji(gameDataForDiceRoll, diceValue, msg); 
                    else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: processDice21PvBRollByEmoji for PvB`);
                } else if (gameDataForDiceRoll.type === GAME_IDS.DICE_21_PVP) { 
                    if (typeof processDice21PvPRollByEmoji === 'function') await processDice21PvPRollByEmoji(gameDataForDiceRoll, diceValue, rollerId); 
                    else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: processDice21PvPRollByEmoji for PvP`);
                }
            } else if (isDuelGameEmoji) {
                // console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] Routing to Duel processor.`); // Reduced log
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
            // console.log(`${LOG_PREFIX_MSG_HANDLER} [DiceEmoji] No active game found matching criteria for dice roll from user ${rollerId} in chat ${chatId}. Dice value: ${diceValue}.`); // Reduced log
        }
    }

    if (userStateCache.has(userId) && !text.startsWith('/')) {
        const currentState = userStateCache.get(userId);
        // console.log(`${LOG_PREFIX_MSG_HANDLER} User ${userId} has pending state: ${currentState.state}. Routing to stateful input.`); // Reduced log
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
            // console.log(`${LOG_PREFIX_MSG_HANDLER} Command cooldown active for user ${userId}. Ignoring command: ${text}`); // Reduced log
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
                    // console.log(`${LOG_PREFIX_MSG_HANDLER} Command /${commandName} directed at another bot in group. Ignoring.`); // Reduced log
                    return;
                } else {
                    commandName = commandName.split('@')[0];
                }
            } catch (getMeErr) { console.error(`${LOG_PREFIX_MSG_HANDLER} Error stripping @botname from command /${commandName}: ${getMeErr.message}`); }
        }

        console.log(`${LOG_PREFIX_MSG_HANDLER} CMD: /${commandName}, Args: [${commandArgs.join(', ')}] from User ${userId} (${userForCommandProcessing.username || 'NoUsername'}) in Chat ${chatId} (${chatType})`);

        // --- REVISED ARGUMENT PARSING HELPER ---
        const parseGameArgs = (args) => {
            let betArg = null;
            let targetRaw = null;
            let otherArgs = [];

            if (args.length === 0) {
                // No args, bet defaults to min, no target
            } else if (args.length === 1) {
                const arg0 = args[0];
                // If it's clearly a username, it's a target. Bet will be default.
                if (arg0.startsWith('@')) {
                    targetRaw = arg0;
                } 
                // If it's purely numeric AND long (typical ID length), assume it's a target ID. Bet will be default.
                // Shorter numbers, or numbers with decimals/suffixes, are bets.
                else if (/^\d+$/.test(arg0) && arg0.length >= 7 && arg0.length <= 12) { 
                    targetRaw = arg0;
                } else { // Otherwise, it's a bet.
                    betArg = arg0;
                }
            } else { // args.length >= 2
                const arg0IsUserLike = args[0].startsWith('@') || (/^\d+$/.test(args[0]) && args[0].length >= 7);
                const arg1IsUserLike = args.length > 1 && (args[1].startsWith('@') || (/^\d+$/.test(args[1]) && args[1].length >= 7));

                if (!arg0IsUserLike && arg1IsUserLike) { // Format: <bet> @user_or_id [otherArgs...]
                    betArg = args[0];
                    targetRaw = args[1];
                    otherArgs = args.slice(2);
                } else if (arg0IsUserLike && !arg1IsUserLike) { // Format: @user_or_id <bet> [otherArgs...]
                    targetRaw = args[0];
                    betArg = args[1];
                    otherArgs = args.slice(2);
                } else if (!arg0IsUserLike && !arg1IsUserLike) { // Format: <bet> <non_user_arg> [otherArgs...]
                    betArg = args[0]; 
                    otherArgs = args.slice(1); 
                } else { // Both args look like users (e.g., /game @user1 @user2) or other ambiguous.
                    // This case is tricky. Defaulting to first as bet if it's not user-like, otherwise error or more specific parsing per command.
                    // For now, assume if this complex case is hit, it might be an invalid format for direct challenge.
                    // The game-specific handlers should manage this if `targetRaw` ends up being a bet or vice-versa.
                    // A simple approach: if unclear, assume first is bet, and no target.
                    betArg = args[0]; // Or prioritize based on what is NOT user-like.
                    targetRaw = null; // Or try to find a user in later args if needed.
                    otherArgs = args.slice(1);
                    console.warn(`${LOG_PREFIX_MSG_HANDLER} Ambiguous arguments for game command with two user-like or two bet-like args: ${args.join(' ')}. Defaulting to first as bet.`);
                }
            }
            return { betArg, targetRaw, otherArgs };
        };
        // --- END OF REVISED parseGameArgs DEFINITION ---

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
                    
                    // --- REVISED GAME COMMAND ROUTING WITH CORRECTED parseGameArgs ---
                case 'coinflip': case 'cf': {
                    if (chatType === 'private') {
                        await safeSendMessage(chatId, `🪙 The Coinflip arena awaits in <b>group chats</b>! Please use <code>/coinflip &lt;bet&gt; [@username]</code> there.`, { parse_mode: 'HTML' });
                        break;
                    }
                    if (typeof handleStartCoinflipUnifiedOfferCommand === 'function') {
                        const { betArg, targetRaw } = parseGameArgs(commandArgs);
                        const betCF = await parseBetAmount(betArg, chatId, chatType, userId);
                        if(betCF) await handleStartCoinflipUnifiedOfferCommand(msg, betCF, targetRaw);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartCoinflipUnifiedOfferCommand`);
                    break;
                    }
                case 'rps': {
                    if (chatType === 'private') {
                        await safeSendMessage(chatId, `🪨📄✂️ The Rock Paper Scissors arena is best experienced in <b>group chats</b>! Please use <code>/rps &lt;bet&gt; [@username]</code> there.`, { parse_mode: 'HTML' });
                        break;
                    }
                    if (typeof handleStartRPSUnifiedOfferCommand === 'function') {
                        const { betArg, targetRaw } = parseGameArgs(commandArgs);
                        const betRPS = await parseBetAmount(betArg, chatId, chatType, userId);
                        if(betRPS) await handleStartRPSUnifiedOfferCommand(msg, betRPS, targetRaw);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartRPSUnifiedOfferCommand`);
                    break;
                    }
                case 'de': 
                case 'diceescalator': {
                    if (chatType === 'private') {
                        const playerRefForDMBlock_DE = escapeHTML(getPlayerDisplayReference(userForCommandProcessing || msg.from));
                        await safeSendMessage(chatId, `🎲 Greetings, ${playerRefForDMBlock_DE}!<br><br>The Dice Escalator game, including direct challenges, must be initiated in a <b>group chat</b>.<br>Please use <code>/de &lt;bet&gt; [@username]</code> there to start the action!`, { parse_mode: 'HTML' });
                        break; 
                    }
                    if (typeof handleStartDiceEscalatorUnifiedOfferCommand_New === 'function') {
                        const { betArg, targetRaw } = parseGameArgs(commandArgs);
                        const betDE = await parseBetAmount(betArg, chatId, chatType, userId);
                        if (betDE) { // betDE will be min bet if betArg was null (e.g. /de @user)
                            await handleStartDiceEscalatorUnifiedOfferCommand_New(msg, betDE, targetRaw); 
                        } else { // Should not happen if parseBetAmount always returns a value or throws
                                console.error(`${LOG_PREFIX_MSG_HANDLER} parseBetAmount returned invalid value for Dice Escalator.`);
                            }
                    } else { 
                            console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartDiceEscalatorUnifiedOfferCommand_New`);
                            await safeSendMessage(chatId, "⚙️ The Dice Escalator game is temporarily unavailable. Please try again later.", { parse_mode: 'HTML' });
                        }
                    break;
                    }
                case 'd21': case 'blackjack': {
                    if (chatType === 'private') { 
                        const playerRefForDMBlock_D21 = escapeHTML(getPlayerDisplayReference(userForCommandProcessing || msg.from));
                        await safeSendMessage(chatId, `🎲 Greetings, ${playerRefForDMBlock_D21}!<br><br>The Dice 21 game must be initiated in a <b>group chat</b>.<br>Please use <code>/d21 &lt;bet&gt; [@username]</code> there.`, { parse_mode: 'HTML' });
                        break;
                    }
                    if (typeof handleStartDice21Command === 'function') {
                        const { betArg, targetRaw, otherArgs } = parseGameArgs(commandArgs);
                            const gameModeArgD21 = otherArgs[0] || null; 
                        const betD21 = await parseBetAmount(betArg, chatId, chatType, userId);
                        if(betD21) await handleStartDice21Command(msg, betD21, targetRaw, gameModeArgD21);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartDice21Command`); 
                    break;
                    }
                case 'duel': case 'highroller': {
                    if (chatType === 'private') { 
                        await safeSendMessage(chatId, `⚔️ The High Roller Duel game can only be started in a <b>group chat</b>. Please use <code>/duel &lt;bet&gt; [@username]</code> there!`, { parse_mode: 'HTML' });
                        break; 
                    }
                    if (typeof handleStartDuelUnifiedOfferCommand === 'function') {
                        const { betArg, targetRaw } = parseGameArgs(commandArgs);
                        const betDuel = await parseBetAmount(betArg, chatId, chatType, userId);
                        if(betDuel) await handleStartDuelUnifiedOfferCommand(msg, betDuel, targetRaw);
                    } else console.error(`${LOG_PREFIX_MSG_HANDLER} Missing handler: handleStartDuelUnifiedOfferCommand`); 
                    break;
                    }
                    // --- END OF REVISED GAME COMMAND ROUTING ---

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

    let userObjectForCallback = await getOrCreateUser(userId, userFromCb.username, userFromCb.first_name, userFromCb.last_name);
    if (!userObjectForCallback) {
        console.error(`${LOG_PREFIX_CBQ} Failed to get/create user for callback processing. User ID: ${userId}. Callback Data: ${data}`);
        await bot.answerCallbackQuery(callbackQueryId, {text: "Error fetching your profile.", show_alert: true}).catch(()=>{});
        return; 
    }

    const [action, ...params] = data.split(':');
    console.log(`${LOG_PREFIX_CBQ} Parsed Action: "${action}", Params: [${params.join(', ')}] (Chat: ${originalChatId}, Type: ${originalChatType}, MsgID: ${originalMessageId})`);

    // --- DEBUGGING LINES (kept from previous steps, can be removed if issue is resolved) ---
    console.log(`[DEBUG_SWITCH] Action String for Switch: "${action}"`);
    console.log(`[DEBUG_SWITCH] Action Length: ${action.length}`);
    let charCodes = [];
    for (let i = 0; i < action.length; i++) {
        charCodes.push(action.charCodeAt(i));
    }
    console.log(`[DEBUG_SWITCH] Action Character Codes: [${charCodes.join(', ')}]`);
    // --- DEBUGGING LINES END ---

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
            const effectiveChatId = isCallbackRedirectedToDm ? userId : originalChatId;
            const effectiveMessageId = isCallbackRedirectedToDm ? null : originalMessageId;

            switch (action) {
                case 'show_rules_menu':
                    if (typeof handleRulesCommand === 'function') await handleRulesCommand(effectiveChatId, userObjectForCallback, effectiveMessageId, true, isCallbackRedirectedToDm ? 'private' : originalChatType);
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
                    if (typeof handleMenuAction === 'function') await handleMenuAction(userId, originalChatId, originalMessageId, params[0], params.slice(1), true, originalChatType); 
                    else console.error(`${LOG_PREFIX_CBQ} Missing handler: handleMenuAction`);
                    break;
                case 'process_withdrawal_confirm':
                    console.log(`${LOG_PREFIX_CBQ} Entered 'process_withdrawal_confirm' case. Raw Params: [${params.join(',')}]`);
                    if (typeof handleWithdrawalConfirmation === 'function') { 
                        const decision = params[0];
                        const currentState = userStateCache.get(userId); 
                        console.log(`${LOG_PREFIX_CBQ} Retrieved state for 'process_withdrawal_confirm'. Decision: '${decision}'. State content: ${stringifyWithBigInt(currentState)}`);
                        const messageIdToEditInDm = currentState?.messageId || originalMessageId; 
                        const effectiveDmChatIdForWithdraw = currentState?.chatId || userId; 
                        if (decision === 'yes' && currentState && currentState.state === 'awaiting_withdrawal_confirmation' && currentState.chatId === String(userId)) {
                            console.log(`${LOG_PREFIX_CBQ} 'yes' decision, state is valid and for correct user/chat. Calling handleWithdrawalConfirmation. Message ID to edit/use: ${currentState.messageId}`);
                            await handleWithdrawalConfirmation(userId, currentState.chatId, currentState.messageId, currentState.data.linkedWallet, currentState.data.amountLamportsStr, currentState.data.feeLamportsStr, currentState.data.originalGroupChatId, currentState.data.originalGroupMessageId);
                        } else if (decision === 'no') {
                            console.log(`${LOG_PREFIX_CBQ} 'no' decision. Cancelling withdrawal. State was: ${currentState?.state}`);
                            if (bot && messageIdToEditInDm) {
                                await bot.editMessageText("Withdrawal cancelled by user.", { chat_id: effectiveDmChatIdForWithdraw, message_id: Number(messageIdToEditInDm), parse_mode:'HTML', reply_markup: {inline_keyboard:[[{text:"💳 Back to Wallet", callback_data:"menu:wallet"}]]} }).catch(e => console.error(`${LOG_PREFIX_CBQ} Error editing 'no' decision message: ${e.message}`));
                            }
                            clearUserState(userId);
                        } else {
                            console.warn(`${LOG_PREFIX_CBQ} Invalid confirmation decision ('${decision}') or state invalid/expired. Current state name: '${currentState?.state}'. Current state chatId: '${currentState?.chatId}' vs userId: '${String(userId)}'.`);
                            if (bot && messageIdToEditInDm) {
                                await bot.editMessageText("Invalid confirmation or your session expired. Withdrawal cancelled.", { chat_id: effectiveDmChatIdForWithdraw, message_id: Number(messageIdToEditInDm), parse_mode:'HTML', reply_markup: {inline_keyboard:[[{text:"💳 Back to Wallet", callback_data:"menu:wallet"}]]} }).catch(e => console.error(`${LOG_PREFIX_CBQ} Error editing 'invalid state' message: ${e.message}`));
                            }
                            clearUserState(userId);
                        }
                    } else {
                        console.error(`${LOG_PREFIX_CBQ} Missing handler: handleWithdrawalConfirmation. Cannot process confirmation.`);
                        await bot.answerCallbackQuery(callbackQueryId, {text: "Error processing confirmation: Handler missing.", show_alert: true}).catch(()=>{});
                    }
                    break;
                // --- DIRECT CHALLENGE ROUTING (Restored from test, ensure case literals are perfect) ---
                case 'dir_chal_acc': 
                case 'dir_chal_dec': 
                case 'dir_chal_can': 
                    console.log(`${LOG_PREFIX_CBQ} Routing to handleDirectChallengeResponse for action: ${action}. Params: ${params.join(',')}`);
                    if (typeof handleDirectChallengeResponse === 'function') {
                        const offerIdFromParams = params[0];
                        await handleDirectChallengeResponse(
                            action, 
                            offerIdFromParams,
                            userObjectForCallback,
                            originalMessageId,
                            originalChatId,
                            originalChatType,
                            callbackQueryId
                        );
                    } else {
                        console.error(`${LOG_PREFIX_CBQ} CRITICAL_ERROR: Missing handler function: handleDirectChallengeResponse for action: ${action}`);
                        await bot.answerCallbackQuery(callbackQueryId, {text: "Error: This challenge action is currently unavailable.", show_alert: true}).catch(()=>{});
                    }
                    break;
                // --- END OF DIRECT CHALLENGE ROUTING ---
                case 'cf_accept_bot': case 'cf_accept_pvp': case 'cf_cancel_offer':
                case 'cf_pvb_choice': case 'cf_pvp_call':
                case 'rps_accept_bot': case 'rps_accept_pvp': case 'rps_cancel_offer':
                case 'rps_pvb_choice': case 'rps_pvp_choice':
                    console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardCoinflipRPSCallback for action: ${action}`);
                    if (typeof forwardCoinflipRPSCallback === 'function') {
                        await forwardCoinflipRPSCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else {
                        console.error(`${LOG_PREFIX_CBQ} Missing handler: forwardCoinflipRPSCallback for Coinflip/RPS action: ${action}`);
                        await bot.answerCallbackQuery(callbackQueryId, {text: "Action handler not found.", show_alert: true}).catch(()=>{});
                    }
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
                    console.log(`${LOG_PREFIX_CBQ} Routing to forwardDiceEscalatorCallback_New for action: ${action}`);
                    if (typeof forwardDiceEscalatorCallback_New === 'function') {
                        await forwardDiceEscalatorCallback_New(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else { 
                        console.error(`${LOG_PREFIX_CBQ} Missing handler: forwardDiceEscalatorCallback_New for DE action: ${action}`);
                        await bot.answerCallbackQuery(callbackQueryId, {text: "Error: This Dice Escalator action is currently unavailable.", show_alert: true}).catch(()=>{});
                    }
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
                case 'mines_tile':
                case 'mines_cashout':
                case 'play_again_mines':
                    console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardMinesCallback for action: ${action}`);
                    if (typeof forwardMinesCallback === 'function') {
                        await forwardMinesCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else {
                        console.warn(`${LOG_PREFIX_CBQ} forwardMinesCallback not defined or direct handler missing for Mines action: ${action}`);
                         await bot.answerCallbackQuery(callbackQueryId, {text: "Mines action handler not found.", show_alert: true}).catch(()=>{});
                    }
                    break;
                case 'ou7_choice': case 'play_again_ou7':
                case 'ladder_roll': case 'play_again_ladder':
                case 's7_roll': case 'play_again_s7':
                case 'play_again_slot':
                case 'jackpot_display_noop': 
                    if (action === 'jackpot_display_noop') {
                        console.log(`${LOG_PREFIX_CBQ} Jackpot display no-op handled.`);
                        await bot.answerCallbackQuery(callbackQueryId).catch(()=>{}); 
                    } else if (typeof forwardAdditionalGamesCallback === 'function') {
                        console.log(`${LOG_PREFIX_CBQ} Forwarding to forwardAdditionalGamesCallback for action: ${action}`);
                        await forwardAdditionalGamesCallback(action, params, userObjectForCallback, originalMessageId, originalChatId, originalChatType, callbackQueryId);
                    } else {
                        console.warn(`${LOG_PREFIX_CBQ} forwardAdditionalGamesCallback not defined or direct handler missing for general game action: ${action}`);
                        await bot.answerCallbackQuery(callbackQueryId, {text: "Game action handler not found.", show_alert: true}).catch(()=>{});
                    }
                    break;
                case 'noop_ok': case 'noop':
                    console.log(`${LOG_PREFIX_CBQ} No-op action '${action}' handled. Deleting message if it exists and is not from group (or if it's a specific 'OK' type button).`);
                    if (originalMessageId && bot && (originalChatType === 'private' || action === 'noop_ok')) {
                         await bot.deleteMessage(originalChatId, Number(originalMessageId)).catch(() => {});
                    }
                    await bot.answerCallbackQuery(callbackQueryId).catch(()=>{}); 
                    break;
                default:
                        console.warn(`${LOG_PREFIX_CBQ} Unknown callback action encountered in main switch (original default path): "${action}" with params: [${params.join(', ')}]`);
                        await bot.answerCallbackQuery(callbackQueryId, {text: "Unknown action.", show_alert: false}).catch(()=>{});
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


// --- Helper function to forward game callbacks for Coinflip/RPS (NEW) ---
async function forwardCoinflipRPSCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_CF_RPS_CB_FWD = `[CF_RPS_CB_Fwd UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_CF_RPS_CB_FWD} Processing Coinflip/RPS action. Params: ${params.join(',')}`);

    const offerIdOrGameId = params[0];
    const choiceOrPlayerId = params[1]; 
    const actualChoice = params[2];   

    const chatDataForHandler = { id: originalChatId, type: originalChatType }; 

    switch (action) {
        // Coinflip Callbacks
        case 'cf_accept_bot':
            if (!offerIdOrGameId) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing offerId for cf_accept_bot.`); return; }
            if (typeof handleCoinflipAcceptBotGameCallback === 'function') await handleCoinflipAcceptBotGameCallback(offerIdOrGameId, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleCoinflipAcceptBotGameCallback`);
            break;
        case 'cf_accept_pvp':
            if (!offerIdOrGameId) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing offerId for cf_accept_pvp.`); return; }
            if (typeof handleCoinflipAcceptPvPChallengeCallback === 'function') await handleCoinflipAcceptPvPChallengeCallback(offerIdOrGameId, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleCoinflipAcceptPvPChallengeCallback`);
            break;
        case 'cf_cancel_offer':
            if (!offerIdOrGameId) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing offerId for cf_cancel_offer.`); return; }
            if (typeof handleCoinflipCancelOfferCallback === 'function') await handleCoinflipCancelOfferCallback(offerIdOrGameId, userObject, originalMessageId, originalChatId, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleCoinflipCancelOfferCallback`);
            break;
        case 'cf_pvb_choice': 
            const playerChoiceCF = choiceOrPlayerId; 
            if (!offerIdOrGameId || !playerChoiceCF) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing gameId or choice for cf_pvb_choice.`); return; }
            if (typeof handleCoinflipPvBChoiceCallback === 'function') await handleCoinflipPvBChoiceCallback(offerIdOrGameId, playerChoiceCF, userObject, originalMessageId, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleCoinflipPvBChoiceCallback`);
            break;
        case 'cf_pvp_call': 
            const callerIdCheckCF = choiceOrPlayerId; 
            const callChoiceCF = actualChoice;      
            if (!offerIdOrGameId || !callerIdCheckCF || !callChoiceCF) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing params for cf_pvp_call.`); return; }
            if (typeof handleCoinflipPvPCallCallback === 'function') await handleCoinflipPvPCallCallback(offerIdOrGameId, callerIdCheckCF, callChoiceCF, userObject, originalMessageId, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleCoinflipPvPCallCallback`);
            break;

        // RPS Callbacks
        case 'rps_accept_bot':
            if (!offerIdOrGameId) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing offerId for rps_accept_bot.`); return; }
            if (typeof handleRPSAcceptBotGameCallback === 'function') await handleRPSAcceptBotGameCallback(offerIdOrGameId, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleRPSAcceptBotGameCallback`);
            break;
        case 'rps_accept_pvp':
            if (!offerIdOrGameId) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing offerId for rps_accept_pvp.`); return; }
            if (typeof handleRPSAcceptPvPChallengeCallback === 'function') await handleRPSAcceptPvPChallengeCallback(offerIdOrGameId, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleRPSAcceptPvPChallengeCallback`);
            break;
        case 'rps_cancel_offer':
            if (!offerIdOrGameId) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing offerId for rps_cancel_offer.`); return; }
            if (typeof handleRPSCancelOfferCallback === 'function') await handleRPSCancelOfferCallback(offerIdOrGameId, userObject, originalMessageId, originalChatId, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleRPSCancelOfferCallback`);
            break;
        case 'rps_pvb_choice': 
            const playerChoiceRPS = choiceOrPlayerId; 
            if (!offerIdOrGameId || !playerChoiceRPS) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing gameId or choiceKey for rps_pvb_choice.`); return; }
            if (typeof handleRPSPvBChoiceCallback === 'function') await handleRPSPvBChoiceCallback(offerIdOrGameId, playerChoiceRPS, userObject, originalMessageId, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleRPSPvBChoiceCallback`);
            break;
        case 'rps_pvp_choice': 
            const chooserIdRPS = choiceOrPlayerId; 
            const choiceKeyRPS = actualChoice;     
            if (!offerIdOrGameId || !chooserIdRPS || !choiceKeyRPS) { console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing params for rps_pvp_choice.`); return; }
            if (typeof handleRPSPvPChoiceCallback === 'function') await handleRPSPvPChoiceCallback(offerIdOrGameId, chooserIdRPS, choiceKeyRPS, userObject, originalMessageId, callbackQueryId);
            else console.error(`${LOG_PREFIX_CF_RPS_CB_FWD} Missing handler: handleRPSPvPChoiceCallback`);
            break;
        default:
            console.warn(`${LOG_PREFIX_CF_RPS_CB_FWD} Unhandled Coinflip/RPS action in forwarder: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, {text: "Unknown game action.", show_alert: false}).catch(()=>{});
            break;
    }
}


// --- Helper function to forward game callbacks for Coinflip/RPS (OLD, to be deprecated or merged if any unique logic remains) ---
async function forwardGameCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_GAME_CB_FWD = `[GameCB_Fwd UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_GAME_CB_FWD} Processing action. Params: ${params.join(',')}`);
    const gameIdOrBetAmountStr = params[0];
    const mockMsgForReplay = { from: userObject, chat: { id: originalChatId, type: originalChatType }, message_id: originalMessageId }; // For Play Again

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
            const gameIdRPS_old = params[0];
            const choice_old = params[1];
            console.warn(`${LOG_PREFIX_GAME_CB_FWD} Old 'rps_choose' callback received. GameID: ${gameIdRPS_old}, Choice: ${choice_old}. This path should be deprecated.`);
            if (!gameIdRPS_old || !choice_old) { console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing gameId or choice for old rps_choose.`); return; }
            if (typeof handleRPSChoiceCallback === 'function') await handleRPSChoiceCallback(originalChatId, userObject, gameIdRPS_old, choice_old, originalMessageId, callbackQueryId, originalChatType);
            else console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing handler: handleRPSChoiceCallback (for old rps_choose path)`);
            break;
        case 'play_again_coinflip':
        case 'play_again_rps':
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing bet amount for ${action}.`); return; }
            try {
                const betAmountReplay = BigInt(gameIdOrBetAmountStr);
                if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(originalChatId), message_id: Number(originalMessageId) }).catch(() => {});

                if (action === 'play_again_coinflip' && typeof handleStartCoinflipUnifiedOfferCommand === 'function') {
                    await handleStartCoinflipUnifiedOfferCommand(mockMsgForReplay, betAmountReplay, null); 
                } else if (action === 'play_again_rps' && typeof handleStartRPSUnifiedOfferCommand === 'function') {
                    await handleStartRPSUnifiedOfferCommand(mockMsgForReplay, betAmountReplay, null); 
                } else {
                    console.error(`${LOG_PREFIX_GAME_CB_FWD} Missing NEW UNIFIED start command handler for ${action}`);
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
                    await handleStartDice21Command(mockMsgForD21Replay, betAmountD21Replay, null, null); 
                } else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleStartDice21Command for ${action} replay`);
            } catch (e) { console.error(`${LOG_PREFIX_D21_CB_FWD} Invalid bet amount for ${action}: '${gameIdOrBetAmountStr}'`); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true }); }
            break;
        case 'd21_pvb_cancel':
            if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_D21_CB_FWD} Missing gameId for d21_pvb_cancel.`); return; }
            if (typeof handleDice21PvBCancel === 'function') await handleDice21PvBCancel(gameIdOrBetAmountStr, userObject, originalMessageId, callbackQueryId, chatDataForHandler);
            else console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleDice21PvBCancel`);
            break;
        case 'd21_pvp_stand':
            if (!gameIdOrBetAmountStr) {
                console.error(`${LOG_PREFIX_D21_CB_FWD} Missing gameId for d21_pvp_stand.`);
                if (callbackQueryId) await bot.answerCallbackQuery(callbackQueryId, { text: "Error: Missing game ID for stand action.", show_alert: true }).catch(() => {});
                return;
            }
            const userIdStringForStand = String(userObject.id || userObject.telegram_id); 
            if (typeof handleDice21PvPStand === 'function') {
                await handleDice21PvPStand(gameIdOrBetAmountStr, userIdStringForStand, originalMessageId, callbackQueryId, chatDataForHandler);
            } else {
                console.error(`${LOG_PREFIX_D21_CB_FWD} Missing handler: handleDice21PvPStand`);
            }
            break;
        default:
            console.warn(`${LOG_PREFIX_D21_CB_FWD} Unhandled D21 action in forwarder: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: "Unknown Dice 21 action.", show_alert: false }).catch(() => {});
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
                    await handleStartDuelUnifiedOfferCommand(mockMsgForHandler, betAmountDuelReplay, null); 
                } else console.error(`${LOG_PREFIX_DUEL_CB_FWD} Missing handler: handleStartDuelUnifiedOfferCommand for Duel replay`);
            } catch (e) { console.error(`${LOG_PREFIX_DUEL_CB_FWD} Invalid bet amount for play_again_duel: '${offerIdOrBetAmountStr}'`); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true });}
            break;
        default:
            console.warn(`${LOG_PREFIX_DUEL_CB_FWD} Unhandled Duel action in forwarder: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, { text: "Unknown Duel action.", show_alert: false }).catch(() => {});
            break;
    }
}

// --- Helper function to forward Dice Escalator callbacks (New Structure with Jackpot Choice) ---
async function forwardDiceEscalatorCallback_New(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_DE_CB_FWD_NEW = `[DE_CB_Fwd_New UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    const offerIdFromParams = params[0]; // This is the offerId extracted from the callback_data

    // --- REVISED DIAGNOSTIC LOGS ---
    console.log(`${LOG_PREFIX_DE_CB_FWD_NEW} Entry. Action: "${action}"`);
    // The raw 'data' string that was split into 'action' and 'params' is available from callbackQuery.data in the main handler.
    // Here, we already have 'action' and 'params' separately.
    // We can reconstruct the essence if needed, or rely on logs from the main router for the full raw data.
    // For debugging the offerId specifically:
    console.log(`${LOG_PREFIX_DE_CB_FWD_NEW} Full params array (includes offerId as first element): [${params.join(',')}]`);
    console.log(`${LOG_PREFIX_DE_CB_FWD_NEW} Extracted offerIdFromParams (which is params[0]): "${offerIdFromParams}" (Type: ${typeof offerIdFromParams})`);
    console.log(`${LOG_PREFIX_DE_CB_FWD_NEW} Current activeGames map size: ${activeGames.size}`);
    if (activeGames.size > 0 && activeGames.size < 20) { // Increased limit slightly for better debug
        console.log(`${LOG_PREFIX_DE_CB_FWD_NEW} Some keys in activeGames: ${JSON.stringify(Array.from(activeGames.keys()))}`);
    }
    // --- END OF REVISED DIAGNOSTIC LOGS ---
    console.log(`${LOG_PREFIX_DE_CB_FWD_NEW} Processing action. Action: ${action}, Params: ${params.join(',')}`); // Your existing log
    const gameIdOrOfferIdOrBet = offerIdFromParams; // Use the clearly extracted variable
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
                    await handleStartDiceEscalatorUnifiedOfferCommand_New(mockMsgForPlayAgain, betAmountPvP_DE, null); 
                } else console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Missing handler: handleStartDiceEscalatorUnifiedOfferCommand_New for DE PvP replay`);
            } catch (e) { console.error(`${LOG_PREFIX_DE_CB_FWD_NEW} Invalid bet amount for play_again_de_pvp: '${betAmountPvPStr_DE}'`); await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid bet amount for replay.", show_alert: true });}
            break;
        default:
            console.warn(`${LOG_PREFIX_DE_CB_FWD_NEW} Unhandled Dice Escalator action in forwarder: ${action}`);
            await bot.answerCallbackQuery(callbackQueryId, {text: "Unknown Dice Escalator action.", show_alert: false}).catch(()=>{});
            break;
    }
}

// --- UPDATED Helper function to forward Mines game callbacks ---
async function forwardMinesCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_MINES_CB_FWD = `[MinesCB_Fwd UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_MINES_CB_FWD} Processing action. Params: ${params.join(',')}`);
    const gameIdOrOfferId = params[0]; 
    const mockMsgForReplay = { from: userObject, chat: { id: originalChatId, type: originalChatType }, message_id: originalMessageId };

    switch(action) {
        case 'mines_difficulty_select': 
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
        case 'mines_cancel_offer': 
             if (!gameIdOrOfferId) { console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing offerId for mines_cancel_offer.`); return; }
             if (typeof handleMinesCancelOfferCallback === 'function') {
                await handleMinesCancelOfferCallback(gameIdOrOfferId, userObject, originalMessageId, originalChatId, callbackQueryId);
            } else {
                console.error(`${LOG_PREFIX_MINES_CB_FWD} Missing handler: handleMinesCancelOfferCallback`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Mines offer cancellation is under construction!", show_alert: false});
            }
            break;
        case 'mines_tile': 
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
        case 'mines_cashout': 
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
        case 'play_again_mines': 
            const betAmountStr = gameIdOrOfferId; 
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


// --- Helper function to forward other game callbacks (Corrected) ---
async function forwardAdditionalGamesCallback(action, params, userObject, originalMessageId, originalChatId, originalChatType, callbackQueryId) {
    const LOG_PREFIX_ADD_GAME_CB = `[AddGameCB_Fwd_CORRECTED UID:${userObject.telegram_id || userObject.id} Act:${action}]`;
    console.log(`${LOG_PREFIX_ADD_GAME_CB} Processing action. Params: ${params.join(',')}`);
    const gameIdOrBetAmountStr = params[0];
    const msgContext = { chatId: originalChatId, chatType: originalChatType, messageId: originalMessageId };
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
                if (typeof handleOverUnder7Choice === 'function') {
                    await handleOverUnder7Choice(gameIdOrBetAmountStr, choiceOU7, userObject, originalMessageId, callbackQueryId, msgContext);
                } else {
                    console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleOverUnder7Choice`);
                }
                break;
            case 'play_again_ou7':
                if (typeof handleStartOverUnder7Command === 'function') await handleStartOverUnder7Command(mockMsgForReplay, betAmount);
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleStartOverUnder7Command`);
                break;
            case 'play_again_ladder':
                if (typeof handleStartLadderCommand === 'function') await handleStartLadderCommand(mockMsgForReplay, betAmount);
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleStartLadderCommand`);
                break;
            case 's7_roll': 
                if (!gameIdOrBetAmountStr) { console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing gameId for s7_roll.`); return; }
                if (typeof processSevenOutRoll === 'function') { 
                     const gameDataS7 = activeGames.get(gameIdOrBetAmountStr);
                     if(gameDataS7) await processSevenOutRoll(gameDataS7); 
                          else console.error(`${LOG_PREFIX_ADD_GAME_CB} GameData not found for s7_roll ID: ${gameIdOrBetAmountStr}`);
                } else {
                    console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: processSevenOutRoll`);
                }
                break;
            case 'play_again_s7':
                if (typeof handleStartSevenOutCommand === 'function') await handleStartSevenOutCommand(mockMsgForReplay, betAmount);
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleStartSevenOutCommand`);
                break;
            case 'play_again_slot':
                if (typeof handleStartSlotCommand === 'function') await handleStartSlotCommand(mockMsgForReplay, betAmount);
                else console.error(`${LOG_PREFIX_ADD_GAME_CB} Missing handler: handleStartSlotCommand`);
                break;
            case 'jackpot_display_noop':
                 console.log(`${LOG_PREFIX_ADD_GAME_CB} Jackpot display no-op handled.`);
                 await bot.answerCallbackQuery(callbackQueryId).catch(()=>{});
                 break;
            default:
                console.warn(`${LOG_PREFIX_ADD_GAME_CB} Unhandled action in forwardAdditionalGamesCallback: ${action}`);
        }
    } catch (e) {
        console.error(`${LOG_PREFIX_ADD_GAME_CB} Error processing ${action} for param '${gameIdOrBetAmountStr}': ${e.message}`, e.stack);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Error processing action.", show_alert: true });
    }
}

// --- MODIFIED handleDirectChallengeResponse to include all PvP game types ---
async function handleDirectChallengeResponse(actionName, offerId, clickerUserObj, originalMessageIdInGroupStr, originalChatIdFromGroupStr, originalChatTypeFromGroup, callbackQueryId) {
    const clickerId = String(clickerUserObj.id || clickerUserObj.telegram_id);
    const originalMessageIdInGroup = Number(originalMessageIdInGroupStr);
    const originalChatIdFromGroup = String(originalChatIdFromGroupStr);

    const logPrefix = `[DirectChallengeResp GID:${offerId} Clicker:${clickerId} Act:${actionName} Chat:${originalChatIdFromGroup}]`;
    console.log(`${logPrefix} Processing direct challenge response.`);

    const offerData = activeGames.get(offerId);

    if (!offerData) {
        console.warn(`${logPrefix} Offer ID ${offerId} not found in activeGames.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "This challenge has expired or is no longer valid.", show_alert: true }).catch(() => {});
        if (originalMessageIdInGroup && bot) {
            bot.editMessageReplyMarkup({}, { chat_id: originalChatIdFromGroup, message_id: originalMessageIdInGroup }).catch(() => {});
        }
        return;
    }

    if (offerData.type !== GAME_IDS.DIRECT_PVP_CHALLENGE || offerData.status !== 'pending_direct_challenge_response') {
        console.warn(`${logPrefix} Offer ${offerId} is not a pending direct PvP challenge. Status: ${offerData.status}, Type: ${offerData.type}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "This challenge is not in a valid state to respond to.", show_alert: true }).catch(() => {});
        return;
    }

    const initiatorUserObjFull = offerData.initiatorUserObj; 
    const targetUserObjFull = offerData.targetUserObj; 
    const initiatorMentionHTML = offerData.initiatorMentionHTML || escapeHTML(getPlayerDisplayReference(initiatorUserObjFull));
    const targetMentionHTML = offerData.targetUserMentionHTML || escapeHTML(getPlayerDisplayReference(targetUserObjFull));
    const betDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(offerData.betAmount, 'USD'));

    // Robust check for targetUserObjFull and its telegram_id
    if (!initiatorUserObjFull || !targetUserObjFull || !targetUserObjFull.telegram_id || targetUserObjFull.telegram_id === "undefined") { 
        console.error(`${logPrefix} Critical: Missing/invalid initiator or target user object (or target telegram_id) in offerData for ${offerId}. Target User Object: ${stringifyWithBigInt(targetUserObjFull)}`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "Error: Player details missing or invalid for challenge.", show_alert: true }).catch(()=>{});
        activeGames.delete(offerId);
        await updateGroupGameDetails(originalChatIdFromGroup, null, null, null);
         if (bot && offerData.offerMessageIdInGroup) {
             bot.editMessageText("Error processing challenge: Player details missing or invalid from offer.", { chat_id: originalChatIdFromGroup, message_id: Number(offerData.offerMessageIdInGroup), parse_mode: 'HTML', reply_markup: {} }).catch(() => {});
        }
        return;
    }

    switch (actionName) { 
        case 'dir_chal_acc': 
            if (clickerId !== String(offerData.targetUserId)) {
                await bot.answerCallbackQuery(callbackQueryId, { text: "This challenge was not addressed to you.", show_alert: true }).catch(() => {});
                return;
            }

            await bot.answerCallbackQuery(callbackQueryId, { text: `Accepting challenge from ${initiatorMentionHTML}... Verifying details...` }).catch(() => {});

            const freshInitiator = await getOrCreateUser(offerData.initiatorId);
            const freshTarget = await getOrCreateUser(offerData.targetUserId); 

            if (!freshInitiator || !freshTarget || !freshTarget.telegram_id) { 
                 console.error(`${logPrefix} Failed to fetch fresh user details or target has invalid ID for balance check on accept.`);
                 await safeSendMessage(originalChatIdFromGroup, `⚙️ An error occurred fetching player details. Challenge cannot proceed.`, { parse_mode: 'HTML' });
                 if (bot && offerData.offerMessageIdInGroup) {
                    bot.editMessageReplyMarkup({}, { chat_id: originalChatIdFromGroup, message_id: Number(offerData.offerMessageIdInGroup) }).catch(() => {});
                 }
                 activeGames.delete(offerId); 
                 await updateGroupGameDetails(originalChatIdFromGroup, null, null, null);
                 return;
            }

            if (BigInt(freshTarget.balance) < offerData.betAmount) {
                const declineMsgTargetNoFunds = `⚠️ ${targetMentionHTML}, your balance is too low (needs <b>${betDisplayUSD_HTML}</b>) to accept the challenge from ${initiatorMentionHTML}. Challenge cancelled.`;
                if (bot && offerData.offerMessageIdInGroup) await bot.editMessageText(declineMsgTargetNoFunds, { chat_id: originalChatIdFromGroup, message_id: Number(offerData.offerMessageIdInGroup), parse_mode: 'HTML', reply_markup: {} }).catch(()=>{ safeSendMessage(originalChatIdFromGroup, declineMsgTargetNoFunds, {parse_mode: 'HTML'})});
                else await safeSendMessage(originalChatIdFromGroup, declineMsgTargetNoFunds, {parse_mode: 'HTML'});
                activeGames.delete(offerId);
                await updateGroupGameDetails(originalChatIdFromGroup, null, null, null);
                return;
            }
            if (BigInt(freshInitiator.balance) < offerData.betAmount) {
                const declineMsgInitiatorNoFunds = `⚠️ Challenge from ${initiatorMentionHTML} to ${targetMentionHTML} (<b>${betDisplayUSD_HTML}</b>) is void. ${initiatorMentionHTML} has insufficient funds.`;
                if (bot && offerData.offerMessageIdInGroup) await bot.editMessageText(declineMsgInitiatorNoFunds, { chat_id: originalChatIdFromGroup, message_id: Number(offerData.offerMessageIdInGroup), parse_mode: 'HTML', reply_markup: {} }).catch(()=>{ safeSendMessage(originalChatIdFromGroup, declineMsgInitiatorNoFunds, {parse_mode: 'HTML'})});
                else await safeSendMessage(originalChatIdFromGroup, declineMsgInitiatorNoFunds, {parse_mode: 'HTML'});
                activeGames.delete(offerId);
                await updateGroupGameDetails(originalChatIdFromGroup, null, null, null);
                return;
            }

            console.log(`${logPrefix} Challenge accepted by ${targetMentionHTML}. Balances OK. Proceeding to deduct bets and start ${offerData.gameToStart}.`);
            
            let client = null;
            let pvpGameSetupData; 
            try {
                client = await pool.connect(); 
                await client.query('BEGIN');
                
                const gameNameForLedger = offerData.gameToStart.replace('_PVP','').toLowerCase();
                const initBetRes = await updateUserBalanceAndLedger(client, offerData.initiatorId, BigInt(-offerData.betAmount), 
                                                                  `bet_placed_${gameNameForLedger}_direct_init`, 
                                                                  { game_id_custom_field: offerId, opponent_id_custom_field: offerData.targetUserId }, 
                                                                  `Direct PvP bet vs ${targetMentionHTML} for ${offerData.gameToStart}`);
                if (!initBetRes.success) throw new Error(`Initiator bet placement failed: ${initBetRes.error}`);
                freshInitiator.balance = initBetRes.newBalanceLamports; 
                
                const targetBetRes = await updateUserBalanceAndLedger(client, offerData.targetUserId, BigInt(-offerData.betAmount), 
                                                                    `bet_placed_${gameNameForLedger}_direct_join`, 
                                                                    { game_id_custom_field: offerId, opponent_id_custom_field: offerData.initiatorId }, 
                                                                    `Direct PvP bet vs ${initiatorMentionHTML} for ${offerData.gameToStart}`);
                if (!targetBetRes.success) throw new Error(`Target player bet placement failed: ${targetBetRes.error}`);
                freshTarget.balance = targetBetRes.newBalanceLamports; 
                
                await client.query('COMMIT');
                console.log(`${logPrefix} Bets successfully deducted for both players for ${offerData.gameToStart}.`);

                pvpGameSetupData = {
                    chatId: offerData.originalGroupId, 
                    chatType: originalChatTypeFromGroup, 
                    betAmount: offerData.betAmount,
                    initiatorUserObj: freshInitiator, 
                    opponentUserObj: freshTarget,   
                    offerMessageIdInGroup: offerData.offerMessageIdInGroup 
                };

            } catch (e) {
                if (client) await client.query('ROLLBACK').catch(()=>{});
                console.error(`${logPrefix} DB error during bet deductions for direct challenge ${offerId}: ${e.message}`);
                const dbErrorMsg = `⚙️ A database error occurred while processing bets for the challenge between ${initiatorMentionHTML} and ${targetMentionHTML}. The game cannot start. Please try again.`;
                if (bot && offerData.offerMessageIdInGroup) {
                    await bot.editMessageText(dbErrorMsg, { chat_id: originalChatIdFromGroup, message_id: Number(offerData.offerMessageIdInGroup), parse_mode: 'HTML', reply_markup: {} }).catch(()=>{ safeSendMessage(originalChatIdFromGroup, dbErrorMsg, {parse_mode: 'HTML'})});
                } else {
                    await safeSendMessage(originalChatIdFromGroup, dbErrorMsg, {parse_mode: 'HTML'});
                }
                activeGames.delete(offerId); 
                await updateGroupGameDetails(originalChatIdFromGroup, null, null, null);
                return;
            } finally {
                if (client) client.release();
            }
            
            activeGames.delete(offerId); 
            await updateGroupGameDetails(originalChatIdFromGroup, null, null, null); 

            const gameDisplayName = escapeHTML(offerData.gameToStart.replace('_PVP','').replace(/_/g,' '));
            const acceptedMsgHTML = `✅ Challenge Accepted by ${targetMentionHTML}!\n\nA <b>${gameDisplayName}</b> duel between ${initiatorMentionHTML} and ${targetMentionHTML} for <b>${betDisplayUSD_HTML}</b> is starting now...`;
            if (bot && pvpGameSetupData.offerMessageIdInGroup) { 
                await bot.editMessageText(acceptedMsgHTML, {
                    chat_id: pvpGameSetupData.chatId, 
                    message_id: Number(pvpGameSetupData.offerMessageIdInGroup),
                    parse_mode: 'HTML', reply_markup: {}
                }).catch(e => console.warn(`${logPrefix} Failed to edit group message for challenge accepted: ${e.message}`));
            } else { 
                await safeSendMessage(pvpGameSetupData.chatId, acceptedMsgHTML, { parse_mode: 'HTML' });
            }

            // --- MODIFIED: Switch to call appropriate PvP game starters ---
            switch (offerData.gameToStart) {
                case GAME_IDS.DICE_ESCALATOR_PVP:
                    if (typeof startDiceEscalatorPvPGame_New === 'function') {
                        await startDiceEscalatorPvPGame_New(
                            pvpGameSetupData.initiatorUserObj, 
                            pvpGameSetupData.opponentUserObj, 
                            pvpGameSetupData.betAmount, 
                            pvpGameSetupData.chatId,
                            pvpGameSetupData.chatType, 
                            null 
                        );
                    } else { 
                        console.error(`${logPrefix} Missing handler: startDiceEscalatorPvPGame_New`);
                        await safeSendMessage(originalChatIdFromGroup, "⚙️ Error starting Dice Escalator PvP: Handler missing.", {parse_mode: 'HTML'});
                    }
                    break;
                    case GAME_IDS.COINFLIP_PVP:
                        if (typeof startCoinflipPvPGame === 'function') {
                            const coinflipInitiatorData = {
                                initiatorUserObj: pvpGameSetupData.initiatorUserObj, // Already has updated balance
                                betAmount: pvpGameSetupData.betAmount,
                                chatId: pvpGameSetupData.chatId,
                                chatType: pvpGameSetupData.chatType,
                                initiatorId: pvpGameSetupData.initiatorUserObj.telegram_id, // For compatibility if startCoinflipPvPGame expects initiatorId
                                initiatorMentionHTML: escapeHTML(getPlayerDisplayReference(pvpGameSetupData.initiatorUserObj)) // For compatibility
                            };
                            await startCoinflipPvPGame(
                                coinflipInitiatorData,
                                pvpGameSetupData.opponentUserObj, // Has updated balance
                                null, // No unified offer message ID from this direct flow
                                true  // Signal that bets are already deducted
                            );
                        } else { 
                            console.error(`${logPrefix} Missing handler: startCoinflipPvPGame`);
                            await safeSendMessage(originalChatIdFromGroup, "⚙️ Error starting Coinflip PvP: Handler missing.", {parse_mode: 'HTML'});
                        }
                        break;
                    case GAME_IDS.RPS_PVP:
                        if (typeof startRPSPvPGame === 'function') {
                             const rpsInitiatorData = { 
                                initiatorUserObj: pvpGameSetupData.initiatorUserObj, // Has updated balance
                                betAmount: pvpGameSetupData.betAmount,
                                chatId: pvpGameSetupData.chatId,
                                chatType: pvpGameSetupData.chatType,
                                initiatorId: pvpGameSetupData.initiatorUserObj.telegram_id,
                                initiatorMentionHTML: escapeHTML(getPlayerDisplayReference(pvpGameSetupData.initiatorUserObj))
                            };
                            await startRPSPvPGame(
                                rpsInitiatorData,
                                pvpGameSetupData.opponentUserObj, // Has updated balance
                                null, 
                                true // Signal that bets are already deducted
                            );
                        } else { 
                            console.error(`${logPrefix} Missing handler: startRPSPvPGame`);
                            await safeSendMessage(originalChatIdFromGroup, "⚙️ Error starting RPS PvP: Handler missing.", {parse_mode: 'HTML'});
                        }
                        break;
                    case GAME_IDS.DICE_21_PVP:
                        if (typeof startDice21PvPInitialDeal === 'function') {
                            const pvpGameIdD21 = generateGameId(GAME_IDS.DICE_21_PVP);
                            const pvpGameDataD21 = {
                                type: GAME_IDS.DICE_21_PVP, gameId: pvpGameIdD21,
                                chatId: pvpGameSetupData.chatId, chatType: pvpGameSetupData.chatType,
                                betAmount: pvpGameSetupData.betAmount,
                                initiator: { 
                                    userId: pvpGameSetupData.initiatorUserObj.telegram_id, 
                                    mention: getPlayerDisplayReference(pvpGameSetupData.initiatorUserObj), 
                                    userObj: pvpGameSetupData.initiatorUserObj, // Contains updated balance
                                    hand: [], score: 0, status: 'waiting_for_hand', isTurn: false 
                                },
                                opponent: { 
                                    userId: pvpGameSetupData.opponentUserObj.telegram_id, 
                                    mention: getPlayerDisplayReference(pvpGameSetupData.opponentUserObj), 
                                    userObj: pvpGameSetupData.opponentUserObj, // Contains updated balance
                                    hand: [], score: 0, status: 'waiting_for_hand', isTurn: false 
                                },
                                status: 'dealing_initial_hands', creationTime: Date.now(), currentMessageId: null
                            };
                            activeGames.set(pvpGameIdD21, pvpGameDataD21);
                            await updateGroupGameDetails(pvpGameSetupData.chatId, pvpGameIdD21, GAME_IDS.DICE_21_PVP, pvpGameSetupData.betAmount);
                            await startDice21PvPInitialDeal(pvpGameIdD21); // Expects gameId, game data is in activeGames
                        } else { 
                            console.error(`${logPrefix} Missing handler: startDice21PvPInitialDeal`);
                            await safeSendMessage(originalChatIdFromGroup, "⚙️ Error starting Dice 21 PvP: Handler missing.", {parse_mode: 'HTML'});
                        }
                        break;
                    case GAME_IDS.DUEL_PVP:
                        if (typeof startDuelPvPGameSequence === 'function') {
                            const pvpGameIdDuel = generateGameId(GAME_IDS.DUEL_PVP);
                            const pvpGameDataDuel = {
                                type: GAME_IDS.DUEL_PVP, gameId: pvpGameIdDuel,
                                chatId: pvpGameSetupData.chatId, chatType: pvpGameSetupData.chatType,
                                betAmount: pvpGameSetupData.betAmount,
                                initiator: { 
                                    userId: pvpGameSetupData.initiatorUserObj.telegram_id, 
                                    displayName: getPlayerDisplayReference(pvpGameSetupData.initiatorUserObj),
                                    mention: getPlayerDisplayReference(pvpGameSetupData.initiatorUserObj),
                                    userObj: pvpGameSetupData.initiatorUserObj, // Contains updated balance
                                    rolls: [], score: 0, isTurn: false, status: 'waiting_turn' 
                                },
                                opponent: { 
                                    userId: pvpGameSetupData.opponentUserObj.telegram_id, 
                                    displayName: getPlayerDisplayReference(pvpGameSetupData.opponentUserObj),
                                    mention: getPlayerDisplayReference(pvpGameSetupData.opponentUserObj),
                                    userObj: pvpGameSetupData.opponentUserObj, // Contains updated balance
                                    rolls: [], score: 0, isTurn: false, status: 'waiting_turn' 
                                },
                                status: 'p1_awaiting_roll1_emoji', // Will be set by startDuelPvPGameSequence
                                currentMessageId: null, createdAt: Date.now(), lastRollValue: null
                            };
                            activeGames.set(pvpGameIdDuel, pvpGameDataDuel);
                            await updateGroupGameDetails(pvpGameSetupData.chatId, pvpGameIdDuel, GAME_IDS.DUEL_PVP, pvpGameSetupData.betAmount);
                            await startDuelPvPGameSequence(pvpGameIdDuel); // Expects gameId, game data is in activeGames
                        } else { 
                            console.error(`${logPrefix} Missing handler: startDuelPvPGameSequence`);
                            await safeSendMessage(originalChatIdFromGroup, "⚙️ Error starting Duel PvP: Handler missing.", {parse_mode: 'HTML'});
                        }
                        break;
                default:
                    console.error(`${logPrefix} Unknown gameTypeForAccept in offerData: ${offerData.gameToStart}. Cannot start game.`);
                    await safeSendMessage(originalChatIdFromGroup, `⚙️ Error: Cannot start game type "<code>${escapeHTML(offerData.gameToStart)}</code>". Bets were deducted. Admin notified.`, {parse_mode: 'HTML'});
                    if (typeof notifyAdmin === 'function') {
                        notifyAdmin(`🚨 CRITICAL: Direct Challenge for ${offerData.gameToStart} accepted, bets deducted, but no game starter function found. OfferID: ${offerId}. Players ${offerData.initiatorId} & ${offerData.targetUserId} may need manual game start or refund.`, {parse_mode: 'HTML'});
                    }
            }
            break;

        case 'dir_chal_dec': 
            if (clickerId !== String(offerData.targetUserId)) {
                await bot.answerCallbackQuery(callbackQueryId, { text: "This challenge was not addressed to you to decline.", show_alert: true }).catch(() => {});
                return;
            }
            await bot.answerCallbackQuery(callbackQueryId, { text: "Challenge declined." }).catch(() => {});
            console.log(`${logPrefix} Challenge declined by ${targetMentionHTML}.`);
            const gameNameForDecline = escapeHTML(offerData.gameToStart.replace('_PVP','').replace(/_/g,' '));
            const declineMsgHTML = `❌ ${targetMentionHTML} has declined the ${gameNameForDecline} challenge from ${initiatorMentionHTML} for <b>${betDisplayUSD_HTML}</b>.`;
            if (bot && offerData.offerMessageIdInGroup) await bot.editMessageText(declineMsgHTML, { chat_id: originalChatIdFromGroup, message_id: Number(offerData.offerMessageIdInGroup), parse_mode: 'HTML', reply_markup: {} }).catch(()=>{ safeSendMessage(originalChatIdFromGroup, declineMsgHTML, {parse_mode: 'HTML'})});
            else await safeSendMessage(originalChatIdFromGroup, declineMsgHTML, {parse_mode: 'HTML'});
            
            if (offerData.targetUserId && offerData.targetUserId !== "undefined") { // Send DM only if targetUserId is valid
                await safeSendMessage(offerData.initiatorId, `${targetMentionHTML} has declined your ${gameNameForDecline} challenge for <b>${betDisplayUSD_HTML}</b> in the group "${escapeHTML(offerData.chatTitle)}".`, { parse_mode: 'HTML' });
            }
            
            activeGames.delete(offerId);
            await updateGroupGameDetails(originalChatIdFromGroup, null, null, null);
            break;

        case 'dir_chal_can': 
            if (clickerId !== String(offerData.initiatorId)) {
                await bot.answerCallbackQuery(callbackQueryId, { text: "Only the initiator can withdraw this challenge.", show_alert: true }).catch(() => {});
                return;
            }
            await bot.answerCallbackQuery(callbackQueryId, { text: "Challenge withdrawn." }).catch(() => {});
            console.log(`${logPrefix} Challenge withdrawn by initiator ${initiatorMentionHTML}.`);
            const gameNameForWithdraw = escapeHTML(offerData.gameToStart.replace('_PVP','').replace(/_/g,' '));
            const withdrawMsgHTML = `🚫 ${initiatorMentionHTML} has withdrawn their ${gameNameForWithdraw} challenge to ${targetMentionHTML}.`;
             if (bot && offerData.offerMessageIdInGroup) await bot.editMessageText(withdrawMsgHTML, { chat_id: originalChatIdFromGroup, message_id: Number(offerData.offerMessageIdInGroup), parse_mode: 'HTML', reply_markup: {} }).catch(()=>{ safeSendMessage(originalChatIdFromGroup, withdrawMsgHTML, {parse_mode: 'HTML'})});
             else await safeSendMessage(originalChatIdFromGroup, withdrawMsgHTML, {parse_mode: 'HTML'});
            
            if (offerData.status === 'pending_direct_challenge_response' && offerData.targetUserId && offerData.targetUserId !== "undefined") { 
                 await safeSendMessage(offerData.targetUserId, `${initiatorMentionHTML} has withdrawn their ${gameNameForWithdraw} challenge to you in the group "${escapeHTML(offerData.chatTitle)}".`, { parse_mode: 'HTML' });
            }
            
            activeGames.delete(offerId);
            await updateGroupGameDetails(originalChatIdFromGroup, null, null, null);
            break;

        default: 
            console.warn(`${logPrefix} Unknown action in handleDirectChallengeResponse: ${actionName}`); 
            await bot.answerCallbackQuery(callbackQueryId, { text: "Unknown challenge action details.", show_alert: false }).catch(() => {}); 
    }
}
// --- End of Part 5a, Section 1 (REVISED for New Coinflip/RPS, Dice Escalator & Full Routing for Jackpot Choice + OU7 Fix) ---
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
    // MODIFIED SQL Query: Added ::VARCHAR to $1
    const query = `
        UPDATE withdrawals 
        SET status = $1::VARCHAR,  -- Explicit cast for status
            transaction_signature = $2, 
            error_message = $3, 
            block_time = $4,
            processed_at = CASE WHEN $1::VARCHAR IN ('completed', 'failed', 'confirmed', 'sent') THEN NOW() ELSE processed_at END, -- Explicit cast here too
            updated_at = NOW()
        WHERE withdrawal_id = $5
        RETURNING withdrawal_id;
    `;
    try {
        // Parameters: [status, signature, errorMessage, blockTime, withdrawalId]
        const res = await dbClient.query(query, [status, signature, errorMessage, blockTime, withdrawalId]);
        if (res.rowCount > 0) {
            // console.log(`${LOG_PREFIX_UWS} ✅ Withdrawal status updated successfully.`); // Reduced log
            return { success: true, withdrawalId: res.rows[0].withdrawal_id };
        }
        console.warn(`${LOG_PREFIX_UWS} ⚠️ Withdrawal ID ${withdrawalId} not found or no status update made (rowCount 0).`);
        return { success: false, error: "Withdrawal record not found or no update made." };
    } catch (err) {
        // This console.error is what you're seeing in your logs for this specific error
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
    const dmChatId = String(msg.chat.id); // Should be same as userId for DM states
    const potentialNewAddress = msg.text ? msg.text.trim() : '';
    const logPrefix = `[WalletAddrInput UID:${userId}]`;

    if (!currentState || !currentState.data || currentState.state !== 'awaiting_withdrawal_address' || dmChatId !== userId) {
        console.error(`${logPrefix} Invalid state or context for wallet address input. State ChatID: ${currentState?.chatId}, Msg ChatID: ${dmChatId}, State: ${currentState?.state}`);
        clearUserState(userId);
        await safeSendMessage(dmChatId, "⚙️ There was an issue processing your address input.<br>Please try linking your wallet again via the <code>/wallet</code> menu or <code>/setwallet</code> command.", { parse_mode: 'HTML' });
        return;
    }

    const { originalPromptMessageId, originalGroupChatId, originalGroupMessageId } = currentState.data;
    if (originalPromptMessageId && bot) { await bot.deleteMessage(dmChatId, originalPromptMessageId).catch(() => {}); }
    clearUserState(userId);

    const linkingMsgText = `🔗 Validating and attempting to link wallet: <code>${escapeHTML(potentialNewAddress)}</code>...<br>Please hold on a moment.`;

    const linkingMsg = await safeSendMessage(dmChatId, linkingMsgText, { parse_mode: 'HTML' });
    const displayMsgIdInDm = linkingMsg ? linkingMsg.message_id : null;

    try {
        if (!isValidSolanaAddress(potentialNewAddress)) {
            throw new Error("The provided address has an invalid Solana address format.<br>Please double-check and try again.");
        }

        const linkResult = await linkUserWallet(userId, potentialNewAddress); 
        let feedbackText;
        const finalKeyboard = { inline_keyboard: [[{ text: '💳 Back to Wallet Menu', callback_data: 'menu:wallet' }]] };

        if (linkResult.success) {
            feedbackText = linkResult.message || `✅ Success! Wallet <code>${escapeHTML(potentialNewAddress)}</code> has been successfully linked to your account.`;
            if (originalGroupChatId && originalGroupMessageId && bot) {
                const userForGroupMsg = await getOrCreateUser(userId); 
                await bot.editMessageText(`${escapeHTML(getPlayerDisplayReference(userForGroupMsg || msg.from))} has successfully updated their linked wallet.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'HTML', reply_markup: {}}).catch(()=>{});
            }
        } else {
            feedbackText = `⚠️ Wallet Link Failed for <code>${escapeHTML(potentialNewAddress)}</code>.<br><b>Reason:</b> ${escapeHTML(linkResult.error || "Please ensure the address is valid and not already in use.")}`;
            if (originalGroupChatId && originalGroupMessageId && bot) {
                const userForGroupMsg = await getOrCreateUser(userId);
                await bot.editMessageText(`${escapeHTML(getPlayerDisplayReference(userForGroupMsg || msg.from))}, there was an issue linking your wallet. Please check my DM for details and try again.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'HTML', reply_markup: {}}).catch(()=>{});
            }
        }

        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(feedbackText, { chat_id: dmChatId, message_id: displayMsgIdInDm, parse_mode: 'HTML', reply_markup: finalKeyboard });
        } else {
            await safeSendMessage(dmChatId, feedbackText, { parse_mode: 'HTML', reply_markup: finalKeyboard });
        }
    } catch (e) {
        console.error(`${logPrefix} Error linking wallet ${potentialNewAddress}: ${e.message}`);
        const errorTextToDisplay = `⚠️ Error with wallet address: <code>${escapeHTML(potentialNewAddress)}</code>.<br><b>Details:</b> ${escapeHTML(String(e.message || "An unexpected error occurred."))}<br>Please ensure it's a valid Solana public key and try again.`;
        const errorKeyboard = { inline_keyboard: [[{ text: '💳 Try Again (Wallet Menu)', callback_data: 'menu:wallet' }]] };
        
        if (displayMsgIdInDm && bot) {
            await bot.editMessageText(errorTextToDisplay, { chat_id: dmChatId, message_id: displayMsgIdInDm, parse_mode: 'HTML', reply_markup: errorKeyboard });
        } else {
            await safeSendMessage(dmChatId, errorTextToDisplay, { parse_mode: 'HTML', reply_markup: errorKeyboard });
        }
        if (originalGroupChatId && originalGroupMessageId && bot) {
            const userForGroupMsg = await getOrCreateUser(userId);
            await bot.editMessageText(`${escapeHTML(getPlayerDisplayReference(userForGroupMsg || msg.from))}, there was an error processing your wallet address. Please check my DM.`, {chat_id: originalGroupChatId, message_id: originalGroupMessageId, parse_mode: 'HTML', reply_markup: {}}).catch(()=>{});
        }
    }
}

async function handleWithdrawalAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const dmChatId = String(msg.chat.id); 
    const textAmount = msg.text ? msg.text.trim() : '';
    const logPrefix = `[WithdrawAmountInput_HTML_V3_Newline UID:${userId}]`; 

    // This initial check should now pass given previous fixes, but the log inside it is important if it ever triggers again.
    if (!currentState || !currentState.data || currentState.state !== 'awaiting_withdrawal_amount' || dmChatId !== userId ||
        !currentState.data.linkedWallet || typeof currentState.data.currentBalanceLamportsStr !== 'string') {
        console.error(`${logPrefix} Invalid state or data at START of handleWithdrawalAmountInput. State: ${stringifyWithBigInt(currentState).substring(0,300)}`);
        clearUserState(userId);
        await safeSendMessage(dmChatId, "⚙️ Error: Withdrawal context lost or invalid (Initial Check).<br>Please restart the withdrawal process from the <code>/wallet</code> menu.", { parse_mode: 'HTML' });
        return;
    }

    const { linkedWallet, originalPromptMessageId, currentBalanceLamportsStr, originalGroupChatId, originalGroupMessageId } = currentState.data;
    const currentBalanceLamports = BigInt(currentBalanceLamportsStr);
    if (originalPromptMessageId && bot) { await bot.deleteMessage(dmChatId, originalPromptMessageId).catch(() => {}); }
    clearUserState(userId); // Clear the 'awaiting_withdrawal_amount' state

    try {
        const solPrice = await getSolUsdPrice(); 
        const effectiveMinWithdrawalLamports = convertUSDToLamports(MIN_WITHDRAWAL_USD_val, solPrice);
        const minWithdrawDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(effectiveMinWithdrawalLamports, 'USD'));

        let amountUSD;
        let amountLamports;

        if (textAmount.toLowerCase() === 'max') {
            const availableToWithdrawAfterFee = currentBalanceLamports - WITHDRAWAL_FEE_LAMPORTS;
            if (availableToWithdrawAfterFee < effectiveMinWithdrawalLamports) {
                 throw new Error(`Your balance is too low to withdraw the maximum after fees.<br>You need at least <b>${minWithdrawDisplayUSD_HTML}</b> (plus fee) to make a withdrawal.`);
            }
            amountLamports = availableToWithdrawAfterFee; 
            amountUSD = parseFloat(Number(amountLamports) / Number(LAMPORTS_PER_SOL) * solPrice);
        } else {
            amountUSD = parseFloat(String(textAmount).replace(/[^0-9.]/g, ''));
            if (isNaN(amountUSD) || amountUSD <= 0) {
                throw new Error("Invalid number format or non-positive amount.<br>Please enter a value like <code>50</code> or <code>75.50</code>, or type <code>max</code>.");
            }
            amountLamports = convertUSDToLamports(amountUSD, solPrice); 
        }
        
        const feeLamports = WITHDRAWAL_FEE_LAMPORTS; 
        const totalDeductionLamports = amountLamports + feeLamports;
        
        const currentBalanceDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(currentBalanceLamports, 'USD'));
        const amountToWithdrawDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(amountLamports, 'USD')); 
        const feeDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(feeLamports, 'USD'));
        const totalDeductionDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(totalDeductionLamports, 'USD')); 

        if (amountLamports < effectiveMinWithdrawalLamports) {
            throw new Error(`Withdrawal amount of <b>${amountToWithdrawDisplayUSD_HTML}</b> is less than the minimum of <b>${minWithdrawDisplayUSD_HTML}</b> (approx. $${escapeHTML(MIN_WITHDRAWAL_USD_val.toFixed(2))}).`);
        }
        if (currentBalanceLamports < totalDeductionLamports) {
            throw new Error(`Insufficient balance. You need <b>${totalDeductionDisplayUSD_HTML}</b> (amount to receive + fee) to withdraw <b>${amountToWithdrawDisplayUSD_HTML}</b>.\nYour current balance is approx. <b>${currentBalanceDisplayUSD_HTML}</b>.`);
        }

        const confirmationTextHTML = `⚜️ <b>Withdrawal Confirmation</b> ⚜️\n\n` +
                                 `Please review and confirm your withdrawal:\n\n` +
                                 `🔹 Amount You Will Receive: <b>${amountToWithdrawDisplayUSD_HTML}</b>\n` +
                                 `🔹 Withdrawal Fee: <b>${feeDisplayUSD_HTML}</b>\n` +
                                 `🔹 Total Deducted From Your Balance: <b>${totalDeductionDisplayUSD_HTML}</b>\n` +
                                 `🔹 Recipient Wallet: <code>${escapeHTML(linkedWallet)}</code>\n\n` +
                                 `⚠️ Double-check the recipient address! Transactions are irreversible. Proceed?`;

        const sentConfirmMsg = await safeSendMessage(dmChatId, confirmationTextHTML, {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [
                [{ text: '✅ Yes, Confirm Withdrawal', callback_data: `process_withdrawal_confirm:yes` }],
                [{ text: '❌ No, Cancel', callback_data: `process_withdrawal_confirm:no` }]
            ]}
        });

        if (sentConfirmMsg?.message_id) {
            const stateToSetForConfirmation = {
                state: 'awaiting_withdrawal_confirmation',
                chatId: dmChatId, // Should be the DM chat ID (same as userId)
                messageId: sentConfirmMsg.message_id, // ID of the Yes/No prompt message
                data: { 
                    linkedWallet: linkedWallet, 
                    amountLamportsStr: amountLamports.toString(), // Amount user receives
                    feeLamportsStr: feeLamports.toString(),      // Fee
                    originalGroupChatId, 
                    originalGroupMessageId 
                },
                timestamp: Date.now()
            };
            userStateCache.set(userId, stateToSetForConfirmation);
            // --- ADDED DIAGNOSTIC LOG ---
            console.log(`${logPrefix} State SET for awaiting_withdrawal_confirmation. Content: ${stringifyWithBigInt(userStateCache.get(userId))}`);
            // --- END OF ADDED DIAGNOSTIC LOG ---
            
            if (originalGroupChatId && originalGroupMessageId && bot) { 
                const userForGroupMsg = await getOrCreateUser(userId);
                await bot.editMessageText(`${escapeHTML(getPlayerDisplayReference(userForGroupMsg || {id: userId, first_name: "Player"}))}, please check your DMs to confirm your withdrawal request.`, {chat_id: originalGroupChatId, message_id: Number(originalGroupMessageId), parse_mode:'HTML', reply_markup:{}}).catch(()=>{});
            }
        } else {
            throw new Error("Failed to send withdrawal confirmation message.\nPlease try again.");
        }
    } catch (e) {
        console.error(`${logPrefix} Error processing withdrawal amount: ${e.message}`);
        const errorText = `⚠️ <b>Withdrawal Amount Error:</b>\n${e.message}\n\nPlease restart the withdrawal process from the <code>/wallet</code> menu.`;
        await safeSendMessage(dmChatId, errorText, {
            parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]] }
        });
        if (originalGroupChatId && originalGroupMessageId && bot) { 
            const userForGroupMsg = await getOrCreateUser(userId);
            await bot.editMessageText(`${escapeHTML(getPlayerDisplayReference(userForGroupMsg || {id: userId, first_name: "Player"}))}, there was an error with your withdrawal amount. Please check my DM.`, {chat_id: originalGroupChatId, message_id: Number(originalGroupMessageId), parse_mode:'HTML', reply_markup:{}}).catch(()=>{});
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
    if (actualFromObject && actualFromObject.telegram_id) {
        userIdFromInput = String(actualFromObject.telegram_id);
    } else if (actualFromObject && actualFromObject.id) {
        userIdFromInput = String(actualFromObject.id);
    } else {
        const tempChatIdError = actualChatObject?.id || ADMIN_USER_ID || 'unknown_chat';
        console.error(`[WalletCmd] CRITICAL: Could not determine userId from receivedMsgObject.from: ${JSON.stringify(actualFromObject)}`);
        await safeSendMessage(tempChatIdError, "An internal error occurred (User ID missing for Wallet). Please try <code>/start</code>.", { parse_mode: 'HTML' });
        return;
    }

    const userId = userIdFromInput;
    const commandChatId = String(actualChatObject.id);

    let userObject = await getOrCreateUser(userId, actualFromObject?.username, actualFromObject?.first_name, actualFromObject?.last_name);
    if (!userObject) {
        const tempPlayerRef = getPlayerDisplayReference(actualFromObject); // getPlayerDisplayReference uses escapeMarkdownV2, for HTML this should use escapeHTML if used directly
        const errorMessage = `Error fetching your player profile, ${escapeHTML(tempPlayerRef)}. Please try <code>/start</code> again.`;
        const errorChatTarget = (commandChatId === userId) ? commandChatId : userId;
        await safeSendMessage(errorChatTarget, errorMessage, { parse_mode: 'HTML' });
        if (commandChatId !== userId) {
             await safeSendMessage(commandChatId, `${escapeHTML(tempPlayerRef)}, there was an error accessing your wallet. Please check DMs or try <code>/start</code>.`, {parse_mode: 'HTML'});
        }
        return;
    }
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObject)); // Ensure playerRef is HTML safe
    clearUserState(userId);

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) { /* Reduced log */ }
    const botUsernameHTML = escapeHTML(botUsername);

    const targetDmChatId = userId;
    let messageIdToEditOrDeleteInDm = null;

    if (commandChatId !== targetDmChatId && !isFromMenuAction) {
        if (receivedMsgObject.message_id) await bot.deleteMessage(commandChatId, receivedMsgObject.message_id).catch(() => {});
        await safeSendMessage(commandChatId, `${playerRefHTML}, I've sent your Wallet Dashboard to our private chat: @${botUsernameHTML} 💳 For your security, all wallet actions are handled there.`, { parse_mode: 'HTML' });
    } else if (commandChatId === targetDmChatId && receivedMsgObject.message_id) {
        messageIdToEditOrDeleteInDm = receivedMsgObject.message_id;
        if (!isFromMenuAction) { 
             await bot.deleteMessage(targetDmChatId, messageIdToEditOrDeleteInDm).catch(()=>{});
             messageIdToEditOrDeleteInDm = null;
        }
    }
    
    const loadingDmMsgText = "Loading your Wallet Dashboard... ⏳";
    let workingMessageId = messageIdToEditOrDeleteInDm;

    if (workingMessageId) {
        try {
            await bot.editMessageText(loadingDmMsgText, { chat_id: targetDmChatId, message_id: workingMessageId, reply_markup: {inline_keyboard: []} }); // No parse_mode for simple text
        } catch (editError) {
            if (!editError.message?.includes("message is not modified")) {
                const tempMsg = await safeSendMessage(targetDmChatId, loadingDmMsgText); // No parse_mode
                workingMessageId = tempMsg?.message_id;
            }
        }
    } else {
        const tempMsg = await safeSendMessage(targetDmChatId, loadingDmMsgText); // No parse_mode
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
            const noUserText = `😕 Could not retrieve your player profile. Please try <code>/start</code> to the bot first.`;
            await bot.editMessageText(noUserText, {chat_id: targetDmChatId, message_id: messageIdToEditOrDeleteInDm, parse_mode: 'HTML', reply_markup: {inline_keyboard: [[{text: "Go to /start", callback_data:"menu:main"}]]}});
            return;
        }

        const balanceLamports = BigInt(userDetails.balance || '0');
        const linkedAddress = userDetails.solana_wallet_address;
        const balanceDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(balanceLamports, 'USD')); 
        const balanceDisplaySOL_HTML = escapeHTML(await formatBalanceForDisplay(balanceLamports, 'SOL')); 
        const linkedAddress_display_HTML = linkedAddress ? escapeHTML(linkedAddress) : "<i>Not Set</i>";

        let textHTML = `⚜️ <b>${escapeHTML(BOT_NAME)} Wallet Dashboard</b> ⚜️\n\n` +
                   `👤 Player: ${playerRefHTML}\n\n` + // Already HTML escaped
                   `💰 Current Balance:\n   Approx. <b>${balanceDisplayUSD_HTML}</b>\n   SOL: <b>${balanceDisplaySOL_HTML}</b>\n\n` +
                   `🔗 Linked Withdrawal Address:\n   <code>${linkedAddress_display_HTML}</code>\n\n`;
        
        if (!linkedAddress) {
            textHTML += `💡 You can link a wallet using the button below or by typing <code>/setwallet YOUR_ADDRESS</code> in this chat.\n\n`;
        }
        textHTML += `What would you like to do?`;

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

        await bot.editMessageText(textHTML, { chat_id: targetDmChatId, message_id: messageIdToEditOrDeleteInDm, parse_mode: 'HTML', reply_markup: keyboard, disable_web_page_preview: true });

    } catch (error) {
        console.error(`[WalletCmd UID:${userId}] ❌ Error displaying wallet: ${error.message}`, error.stack?.substring(0,500));
        const errorTextForUserHTML = `⚙️ Apologies, we encountered an issue displaying your wallet information. (<code>${escapeHTML(error.message)}</code>). You can try <code>/start</code>.`;
        await bot.editMessageText(errorTextForUserHTML, {
            chat_id: targetDmChatId, 
            message_id: messageIdToEditOrDeleteInDm, 
            parse_mode: 'HTML', 
            reply_markup: {inline_keyboard: [[{text: "Try /start", callback_data:"menu:main"}]]}
        }).catch(async (editFallbackError) => {
            console.warn(`[WalletCmd UID:${userId}] Failed to edit error message, sending new. Edit fallback error: ${editFallbackError.message}`);
            await safeSendMessage(targetDmChatId, errorTextForUserHTML, {parse_mode: 'HTML'}); 
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
    const targetDmChatId = userId; 
    const originalCommandChatId = String(msgOrCbMsg.chat.id);
    const originalMessageId = msgOrCbMsg.message_id;
    const isFromMenuActionOrRedirect = msgOrCbMsg.isCallbackRedirect !== undefined || !!(correctUserIdFromCb && correctUserIdFromCb === userId);

    const logPrefix = `[WithdrawCmd_HTML_V3_Newline UID:${userId} OrigChat:${originalCommandChatId}]`; 

    let userObject = await getOrCreateUser(userId, msgOrCbMsg.from?.username, msgOrCbMsg.from?.first_name, msgOrCbMsg.from?.last_name);
    if (!userObject) {
        await safeSendMessage(targetDmChatId, "Error fetching your player profile for withdrawal.<br>Please try <code>/start</code> again.", { parse_mode: 'HTML' });
        return;
    }
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObject));
    clearUserState(userId); // Clears previous states before setting a new one for withdrawal

    let botUsername = "our bot";
    try { const selfInfo = await bot.getMe(); if (selfInfo.username) botUsername = selfInfo.username; } catch (e) { /* Reduced log */ }

    if (originalCommandChatId !== targetDmChatId && !isFromMenuActionOrRedirect && !msgOrCbMsg.isCallbackRedirect) {
        if (originalMessageId) await bot.deleteMessage(originalCommandChatId, originalMessageId).catch(() => {});
        await safeSendMessage(originalCommandChatId, `${playerRefHTML}, for your security, withdrawal requests are handled in our private chat: @${escapeHTML(botUsername)} 📬 Please check your DMs.`, { parse_mode: 'HTML' });
    }
    
    if (originalCommandChatId === targetDmChatId && originalMessageId && (isFromMenuActionOrRedirect || !msgOrCbMsg.isCallbackRedirect) ) {
        await bot.deleteMessage(targetDmChatId, originalMessageId).catch(()=>{});
    }

    const loadingDmMsgText = "Preparing your withdrawal request... ⏳";
    const loadingDmMsg = await safeSendMessage(targetDmChatId, loadingDmMsgText, {parse_mode: 'HTML'});
    const workingMessageId = loadingDmMsg?.message_id;

    if (!workingMessageId) {
        console.error(`${logPrefix} Failed to send 'Preparing withdrawal' message to DM.`);
        return;
    }

    try {
        const linkedWallet = await getUserLinkedWallet(userId);
        const currentBalanceLamports = await getUserBalance(userId);

        if (currentBalanceLamports === null) {
            throw new Error("Could not retrieve your current balance.<br>Please try again shortly.");
        }

        if (!linkedWallet) {
            // ... (noWalletText and keyboard as before) ...
            const noWalletText = `⚠️ ${playerRefHTML}, you don't have a Solana withdrawal wallet linked to your account yet!<br><br>` +
                                 `Please link a wallet first using the button below, or by typing <code>/setwallet YOUR_SOL_ADDRESS</code> in this chat.`;
            const noWalletKeyboard = { inline_keyboard: [
                [{ text: "🔗 Link Withdrawal Wallet Now", callback_data: "menu:link_wallet_prompt" }],
                [{ text: "💳 Back to Wallet Menu", callback_data: "menu:wallet" }]
            ]};
            await bot.editMessageText(noWalletText, { chat_id: targetDmChatId, message_id: workingMessageId, parse_mode: 'HTML', reply_markup: noWalletKeyboard });
            return;
        }

        const solPrice = await getSolUsdPrice();
        const effectiveMinWithdrawalLamports = convertUSDToLamports(MIN_WITHDRAWAL_USD_val, solPrice);
        const minWithdrawalDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(effectiveMinWithdrawalLamports, 'USD')); 
        const feeDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(WITHDRAWAL_FEE_LAMPORTS, 'USD')); 
        const currentBalanceDisplayUSD_HTML = escapeHTML(await formatBalanceForDisplay(currentBalanceLamports, 'USD')); 

        const promptTextHTML = `💸 <b>Initiate Withdrawal</b>\n\n` +
                             `Player: ${playerRefHTML}\n` +
                             `Linked Wallet: <code>${escapeHTML(linkedWallet)}</code>\n` +
                             `Available Balance: <b>${currentBalanceDisplayUSD_HTML}</b>\n\n` +
                             `Minimum Withdrawal: <b>${minWithdrawalDisplayUSD_HTML}</b> (approx. $${escapeHTML(MIN_WITHDRAWAL_USD_val.toFixed(2))})\n` +
                             `Withdrawal Fee: <b>${feeDisplayUSD_HTML}</b> (this will be deducted from the amount you withdraw)\n\n` +
                             `Please reply with the amount you wish to withdraw in USD (e.g., <code>50</code> or <code>75.50</code>) or type <code>max</code> to withdraw your maximum available balance (after fees).`;

        const promptKeyboard = { inline_keyboard: [[{ text: "❌ Cancel & Back to Wallet", callback_data: "menu:wallet" }]] };
        await bot.editMessageText(promptTextHTML, { chat_id: targetDmChatId, message_id: workingMessageId, parse_mode: 'HTML', reply_markup: promptKeyboard, disable_web_page_preview: true });

        const stateToSet = {
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
        };
        userStateCache.set(userId, stateToSet);
        // --- ADDED DIAGNOSTIC LOG ---
        console.log(`${logPrefix} State SET for awaiting_withdrawal_amount. Content: ${stringifyWithBigInt(userStateCache.get(userId))}`);
        // --- END OF ADDED DIAGNOSTIC LOG ---

    } catch (error) {
        console.error(`${logPrefix} Error preparing withdrawal: ${error.message}`, error.stack?.substring(0,500));
        const errorText = `⚙️ Apologies, ${playerRefHTML}, an error occurred while preparing your withdrawal request: <code>${escapeHTML(error.message)}</code>.<br>Please try again from the wallet menu.`;
        const errorKeyboard = { inline_keyboard: [[{ text: "💳 Back to Wallet Menu", callback_data: "menu:wallet" }]] };
        if (workingMessageId) {
            await bot.editMessageText(errorText, { chat_id: targetDmChatId, message_id: workingMessageId, parse_mode: 'HTML', reply_markup: errorKeyboard })
                .catch(async () => {
                    await safeSendMessage(targetDmChatId, errorText, { parse_mode: 'HTML', reply_markup: errorKeyboard });
                });
        } else {
            await safeSendMessage(targetDmChatId, errorText, { parse_mode: 'HTML', reply_markup: errorKeyboard });
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
    // console.log(`[DEBUG handleMenuAction ENTER] RAW userId param: ${userId} (type: ${typeof userId})`); // Kept for reference, can be removed if not debugging this part
    const stringUserId = String(userId);
    // console.log(`[DEBUG handleMenuAction AFTER String()] stringUserId: ${stringUserId} (type: ${typeof stringUserId})`);
    // --- END DEBUG LOGS ---

    const logPrefix = `[MenuAction UID:${stringUserId} Type:${menuType} OrigChat:${originalChatId}]`;
    console.log(`${logPrefix} Processing menu action. Params: [${params.join(',')}]`);


    if (stringUserId === "undefined" || stringUserId === "") {
        console.error(`${logPrefix} CRITICAL: stringUserId is problematic before calling getOrCreateUser: '${stringUserId}'`);
        // Attempt to answer callback if possible, then return
        if (isFromCallback && callbackQueryId) { // callbackQueryId would need to be passed into handleMenuAction if used here
             bot.answerCallbackQuery(callbackQueryId, {text: "User ID error.", show_alert: true}).catch(()=>{});
        }
        return;
    }
    // console.log(`${logPrefix} About to CALL getOrCreateUser with stringUserId: '${stringUserId}' (type: ${typeof stringUserId})`);
    let userObject = await getOrCreateUser(stringUserId); // Basic fetch, specific handlers re-fetch if they need more fields not in default getOrCreateUser return for just ID

    if(!userObject) {
        console.error(`${logPrefix} Could not fetch user profile for menu action (userObject is null after getOrCreateUser). User ID: ${stringUserId}`);
        if (isFromCallback && callbackQueryId) {
             bot.answerCallbackQuery(callbackQueryId, {text: "Error fetching profile.", show_alert: true}).catch(()=>{});
        } else if (originalChatId) { // If not from callback, originalChatId is where the problem might be reported
             safeSendMessage(originalChatId, "Error fetching your player profile. Please try <code>/start</code> again.", {parse_mode:'HTML'});
        }
        return;
    }

    let botUsername = BOT_NAME || "our bot"; // Assuming BOT_NAME is globally available
    try { const selfInfo = await bot.getMe(); if(selfInfo.username) botUsername = selfInfo.username; } catch(e) { /* Reduced log */ }

    let targetChatIdForAction = stringUserId; // Default to DM
    let messageIdToEdit = (isFromCallback && originalChatType === 'private') ? originalMessageId : null; // Only allow editing if callback was in DM
    let isGroupActionRedirect = false;
    
    const sensitiveMenuTypes = ['deposit', 'quick_deposit', 'withdraw', 'history', 'link_wallet_prompt', 'referral', 'wallet'];
    // Add new menu types that should also primarily be in DM
    const dmPreferredMenuTypes = [...sensitiveMenuTypes, 'rules_list', 'games_overview', 'games_pvp_list', 'games_pve_list', 'games_all_list'];


    if ((originalChatType === 'group' || originalChatType === 'supergroup') && dmPreferredMenuTypes.includes(menuType)) {
        console.log(`${logPrefix} Sensitive/DM-preferred menu action '${menuType}' in group. Redirecting user ${stringUserId} to DM.`);
        isGroupActionRedirect = true;
        const playerRefForRedirect = escapeHTML(getPlayerDisplayReference(userObject)); // Use HTML escape
        const redirectText = `${playerRefForRedirect}, for the best experience and privacy, please continue this action in our direct message. I've sent you a prompt there: @${escapeHTML(botUsername)}`;
        const callbackParamsForUrl = params && params.length > 0 ? `_${params.join('_')}` : '';
        
        if (originalMessageId && bot) {
            try {
                await bot.editMessageText(redirectText, {
                    chat_id: originalChatId, message_id: Number(originalMessageId), parse_mode: 'HTML',
                    reply_markup: { inline_keyboard: [[{text: `📬 Open DM with @${escapeHTML(botUsername)}`, url: `https://t.me/${botUsername}?start=menu_${menuType}${callbackParamsForUrl}`}]] }
                });
            } catch (e) {
                if (!e.message?.toLowerCase().includes("message is not modified")) {
                    console.warn(`${logPrefix} Failed to edit redirect message in group: ${e.message}. Sending new message as fallback.`);
                    await safeSendMessage(originalChatId, redirectText, {parse_mode: 'HTML'});
                }
            }
        } else {
            await safeSendMessage(originalChatId, redirectText, {parse_mode: 'HTML'});
        }
        messageIdToEdit = null; // Action will be a new message in DM if not already there
    } else if (originalChatType === 'private') {
        targetChatIdForAction = originalChatId; // Action happens in the current DM
        // messageIdToEdit is already set if isFromCallback and originalMessageId was provided
    }
    
    // If a menu navigation is happening, clear any pending input state
    if (menuType !== 'link_wallet_prompt_confirm_address' && menuType !== 'withdraw_amount_confirm') { // Example states to preserve
        console.log(`${logPrefix} Clearing user state for ${stringUserId} due to menu navigation: ${menuType}`);
        if (typeof clearUserState === 'function') clearUserState(stringUserId); else userStateCache.delete(stringUserId);
    }

    const actionMsgContext = {
        from: userObject, // Contains full user details from getOrCreateUser
        chat: { id: targetChatIdForAction, type: 'private' }, // Assume actions are now in DM
        message_id: messageIdToEdit, // Will be null if new message needed in DM, or if original was group
        isCallbackRedirect: isGroupActionRedirect, // True if we just redirected from a group
        originalChatInfo: isGroupActionRedirect ? { id: originalChatId, type: originalChatType, messageId: originalMessageId } : null
    };
    
    // For actions that always send a new message in DM, ensure message_id is null
    const alwaysNewMessageInDM = ['deposit', 'quick_deposit', 'withdraw', 'referral', 'history', 'link_wallet_prompt', 'main', 'rules_list', 'games_overview'];
    if (targetChatIdForAction === stringUserId && actionMsgContext.message_id && alwaysNewMessageInDM.includes(menuType)) {
        // If an old bot message exists in DM and we want to send a fresh menu, delete old first
        await bot.deleteMessage(targetChatIdForAction, Number(actionMsgContext.message_id)).catch(()=>{});
        actionMsgContext.message_id = null; // Force sending a new message
    }

    switch(menuType) {
        case 'wallet':
            if (typeof handleWalletCommand === 'function') await handleWalletCommand(actionMsgContext);
            else console.error(`${logPrefix} Missing handler: handleWalletCommand`);
            break;
        case 'deposit': case 'quick_deposit':
            if (typeof handleDepositCommand === 'function') await handleDepositCommand(actionMsgContext, [], stringUserId);
            else console.error(`${logPrefix} Missing handler: handleDepositCommand`);
            break;
        case 'withdraw':
            if (typeof handleWithdrawCommand === 'function') await handleWithdrawCommand(actionMsgContext, [], stringUserId);
            else console.error(`${logPrefix} Missing handler: handleWithdrawCommand`);
            break;
        case 'referral':
            if (typeof handleReferralCommand === 'function') await handleReferralCommand(actionMsgContext);
            else console.error(`${logPrefix} Missing handler: handleReferralCommand`);
            break;
        case 'history':
            if (typeof handleHistoryCommand === 'function') await handleHistoryCommand(actionMsgContext);
            else console.error(`${logPrefix} Missing handler: handleHistoryCommand`);
            break;
        case 'leaderboards':
            // Leaderboards can often be shown in group or DM, let's assume handleLeaderboardsCommand handles context
            const leaderboardsContext = isGroupActionRedirect ?
                {...actionMsgContext, chat: {id: stringUserId, type: 'private'}, message_id: null } :
                {...actionMsgContext, chat: {id: originalChatId, type: originalChatType}, message_id: originalMessageId}; // Pass original context if not redirected
            if (typeof handleLeaderboardsCommand === 'function') await handleLeaderboardsCommand(leaderboardsContext, params);
            else console.error(`${logPrefix} Missing handler: handleLeaderboardsCommand`);
            break;
        case 'link_wallet_prompt':
            // This action specifically sets up a state, ensure it's in DM
            if (typeof handleSetWalletCommand === 'function') { // Assuming handleSetWalletCommand can also just show prompt if no args
                // Simplified: Call a dedicated prompter or ensure handleSetWalletCommand handles no-args case by prompting
                // For now, directly implementing the prompt logic here as it's a menu action.
                clearUserState(stringUserId);
                if (actionMsgContext.message_id && targetChatIdForAction === stringUserId) {
                    await bot.deleteMessage(targetChatIdForAction, Number(actionMsgContext.message_id)).catch(()=>{});
                }
                const promptText = `🔗 <b>Link/Update Your Withdrawal Wallet</b>\n\nPlease reply to this message with your personal Solana wallet address where you'd like to receive withdrawals.\nEnsure it's correct as transactions are irreversible.\n\nExample: <code>SoLmaNqerT3ZpPT1qS9j2kKx2o5x94s2f8u5aA3bCgD</code>`;
                const kbd = { inline_keyboard: [ [{ text: '❌ Cancel & Back to Wallet', callback_data: 'menu:wallet' }] ] };
                const sentDmPrompt = await safeSendMessage(stringUserId, promptText, { parse_mode: 'HTML', reply_markup: kbd });

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
                    await safeSendMessage(stringUserId, "Failed to send the wallet address prompt. Please try again from the Wallet menu.", {parse_mode: 'HTML'});
                }
            } else console.error(`${logPrefix} Missing handler or logic for: link_wallet_prompt`);
            break;
        case 'main': // For "Back to Main Menu" buttons
            if (typeof handleHelpCommand === 'function') await handleHelpCommand(actionMsgContext); // handleHelpCommand now shows the main menu
            else console.error(`${logPrefix} Missing handler: handleHelpCommand`);
            break;
        
        // --- NEW CASES ---
        case 'rules_list': // From "📖 Game Rules" button on main menu
            if (typeof handleRulesCommand === 'function') {
                // handleRulesCommand expects: (invokedInChatIdStr, userObj, msgIdInInvokedChatStr, isEditAttempt, invokedChatType)
                // actionMsgContext.chat.id is DM ID, actionMsgContext.from is userObj, 
                // actionMsgContext.message_id is the ID of the main menu message (so we edit it)
                await handleRulesCommand(actionMsgContext.chat.id, actionMsgContext.from, actionMsgContext.message_id, true, 'private');
            } else {
                console.error(`${logPrefix} Missing handler: handleRulesCommand for menu:rules_list`);
                await safeSendMessage(actionMsgContext.chat.id, "The Game Rules section is currently unavailable.", { parse_mode: 'HTML', reply_markup: createBackToMenuKeyboard('menu:main', '⬅️ Back to Main Menu') });
            }
            break;
        case 'games_overview': // From "🎲 Play Games" button on main menu
            if (typeof handleGamesOverviewMenu === 'function') {
                await handleGamesOverviewMenu(actionMsgContext); // Pass the context, it will edit or send new
            } else {
                console.error(`${logPrefix} Missing handler: handleGamesOverviewMenu for menu:games_overview`);
                await safeSendMessage(actionMsgContext.chat.id, "The Game Selection menu is currently unavailable.", { parse_mode: 'HTML', reply_markup: createBackToMenuKeyboard('menu:main', '⬅️ Back to Main Menu') });
            }
            break;
        // --- END OF NEW CASES ---

        default:
            console.warn(`${logPrefix} Unrecognized menu type in handleMenuAction: ${menuType}`);
            await safeSendMessage(stringUserId, `❓ Unrecognized menu option: <code>${escapeHTML(menuType)}</code>.<br>Please try again or use <code>/help</code>.`, {parse_mode:'HTML', reply_markup: createBackToMenuKeyboard('menu:main', '⬅️ Back to Main Menu')});
    }
}


async function handleWithdrawalConfirmation(userId, dmChatId, confirmationMessageIdInDm, recipientAddress, amountLamportsStr, feeLamportsStr, originalGroupChatIdForNotif, originalGroupMessageIdForNotif) {
    const stringUserId = String(userId);
    const logPrefix = `[WithdrawConfirm_HTML_V3_Newline UID:${stringUserId}]`;

    // --- ADDED DIAGNOSTIC LOGS ---
    console.log(`${logPrefix} ENTERED handleWithdrawalConfirmation.`);
    console.log(`${logPrefix} Received amountLamportsStr: '${amountLamportsStr}' (type: ${typeof amountLamportsStr})`);
    console.log(`${logPrefix} Received feeLamportsStr: '${feeLamportsStr}' (type: ${typeof feeLamportsStr})`);
    // --- END OF ADDED DIAGNOSTIC LOGS ---
    
    const amountLamports = BigInt(amountLamportsStr); 
    const feeLamports = BigInt(feeLamportsStr);       
    const totalDeduction = amountLamports + feeLamports; 

    const userObjForNotif = await getOrCreateUser(stringUserId); 
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userObjForNotif || { id: stringUserId, first_name: "Player" }));
    let client = null;

    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const userDetailsCheck = await client.query('SELECT balance FROM users WHERE telegram_id = $1 FOR UPDATE', [stringUserId]);
        if (userDetailsCheck.rowCount === 0) {
            throw new Error("User profile not found during withdrawal confirmation.");
        }
        const currentBalanceOnConfirm = BigInt(userDetailsCheck.rows[0].balance);
        const solPrice = await getSolUsdPrice(); 
        if (currentBalanceOnConfirm < totalDeduction) {
            throw new Error(`Insufficient balance at time of confirmation.\nCurrent: <b>${escapeHTML(await formatBalanceForDisplay(currentBalanceOnConfirm, 'USD', solPrice))}</b>, Needed: <b>${escapeHTML(await formatBalanceForDisplay(totalDeduction, 'USD', solPrice))}</b>.\nWithdrawal cancelled.`);
        }

        const wdReq = await createWithdrawalRequestDB(client, stringUserId, amountLamports, feeLamports, recipientAddress);
        if (!wdReq.success || !wdReq.withdrawalId) {
            throw new Error(wdReq.error || "Failed to create database withdrawal request record.");
        }

        // This is line approx 12848 from your previous error stack for this function if it calls updateUserBalanceAndLedger
        const balUpdate = await updateUserBalanceAndLedger(
            client, stringUserId, BigInt(-totalDeduction), 
            'withdrawal_request_confirmed',
            { withdrawal_id: wdReq.withdrawalId },
            `Withdrawal confirmed to ${recipientAddress.slice(0,6)}...${recipientAddress.slice(-4)}`
        );
        if (!balUpdate.success) {
            throw new Error(balUpdate.error || "Failed to deduct balance for withdrawal. Withdrawal not queued.");
        }

        await client.query('COMMIT');

        if (typeof addPayoutJob === 'function') {
            await addPayoutJob({ type: 'payout_withdrawal', withdrawalId: wdReq.withdrawalId, userId: stringUserId });
            const amountToReceiveDisplayUSD = escapeHTML(await formatBalanceForDisplay(amountLamports, 'USD')); 
            const successMsgDmHTML = `✅ <b>Withdrawal Queued!</b>\nYour request to withdraw <b>${amountToReceiveDisplayUSD}</b> to <code>${escapeHTML(recipientAddress)}</code> is now in the payout queue.\nYou'll be notified by DM once it's processed.`;
            
            const currentState = userStateCache.get(stringUserId); // Re-fetch for originalGroup IDs
            if (confirmationMessageIdInDm && bot) {
                await bot.editMessageText(successMsgDmHTML, {chat_id: dmChatId, message_id: Number(confirmationMessageIdInDm), parse_mode:'HTML', reply_markup:{}});
            } else {
                await safeSendMessage(dmChatId, successMsgDmHTML, {parse_mode:'HTML'});
            }
            
            if (currentState?.data?.originalGroupChatId && currentState?.data?.originalGroupMessageId && bot) {
                 await bot.editMessageText(`${playerRefHTML}'s withdrawal request for <b>${amountToReceiveDisplayUSD}</b> has been queued successfully. Details in DM.`, {chat_id: currentState.data.originalGroupChatId, message_id: Number(currentState.data.originalGroupMessageId), parse_mode:'HTML', reply_markup:{}}).catch(()=>{});
            }
        } else {
            console.error(`${logPrefix} 🚨 CRITICAL: addPayoutJob function is not defined! Cannot queue withdrawal ${wdReq.withdrawalId}.`);
            await notifyAdmin(`🚨 CRITICAL: Withdrawal ${wdReq.withdrawalId} for user ${stringUserId} had balance deducted BUT FAILED TO QUEUE for payout (addPayoutJob missing). MANUAL INTERVENTION REQUIRED TO REFUND OR PROCESS.`, {parse_mode:'HTML'});
            throw new Error("Payout processing system is unavailable.\nYour funds were deducted but the payout could not be queued.\nPlease contact support immediately.");
        }
    } catch (e) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback error: ${rbErr.message}`));
        console.error(`❌ ${logPrefix} Error processing withdrawal confirmation: ${e.message}`, e.stack?.substring(0,500)); // This is where your error was logged
        const errorMsgDmHTML = `⚠️ <b>Withdrawal Failed:</b>\n${e.message}\n\nPlease try again or contact support if the issue persists.`;
        const errorKeyboard = { inline_keyboard: [[{ text: '💳 Back to Wallet', callback_data: 'menu:wallet' }]] };
        if(confirmationMessageIdInDm && bot) {
            await bot.editMessageText(errorMsgDmHTML, {chat_id: dmChatId, message_id: Number(confirmationMessageIdInDm), parse_mode:'HTML', reply_markup: errorKeyboard}).catch(async ()=>{
                 await safeSendMessage(dmChatId, errorMsgDmHTML, {parse_mode:'HTML', reply_markup: errorKeyboard});
            });
        } else {
            await safeSendMessage(dmChatId, errorMsgDmHTML, {parse_mode:'HTML', reply_markup: errorKeyboard});
        }
        
        const currentState = userStateCache.get(stringUserId); 
        if (currentState?.data?.originalGroupChatId && currentState?.data?.originalGroupMessageId && bot) {
            await bot.editMessageText(`${playerRefHTML}, there was an error processing your withdrawal confirmation. Please check your DMs.`, {chat_id: currentState.data.originalGroupChatId, message_id: Number(currentState.data.originalGroupMessageId), parse_mode:'HTML', reply_markup:{}}).catch(()=>{});
        }
    } finally {
        if (client) client.release();
        clearUserState(stringUserId); 
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

    if (txResponse.transaction.message && txResponse.transaction.message.accountKeys && txResponse.transaction.message.accountKeys.length > 0) {
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
            console.log(`${logPrefix} No positive transfer amount found for ${depositAddress} in TX ${txSignature}. Amount: ${transferAmount}. Marking processed.`);
            processedDepositTxSignatures.add(txSignature);
            return;
        }
        // Prepare display amounts (these will be further escaped before inserting into HTML)
        const depositAmountSOLDisplay = await formatBalanceForDisplay(transferAmount, 'SOL');
        console.log(`✅ ${logPrefix} Valid deposit: ${depositAmountSOLDisplay} from ${payerAddress || 'unknown source'}.`);

        client = await pool.connect();
        await client.query('BEGIN');

        const depositRecordResult = await recordConfirmedDepositDB(client, stringUserId, userDepositWalletId, depositAddress, txSignature, transferAmount, payerAddress, txResponse.blockTime);
        if (depositRecordResult.alreadyProcessed) {
            await client.query('ROLLBACK'); 
            processedDepositTxSignatures.add(txSignature);
            return;
        }
        if (!depositRecordResult.success || !depositRecordResult.depositId) {
            throw new Error(`Failed to record deposit in DB for ${txSignature}: ${depositRecordResult.error || "Unknown DB error."}`);
        }
        const depositId = depositRecordResult.depositId;

        const markedInactive = await markDepositAddressInactiveDB(client, userDepositWalletId, false, null); 
        if (!markedInactive) {
            console.warn(`${logPrefix} ⚠️ Could not mark deposit address WID ${userDepositWalletId} as inactive after deposit.`);
        }

        const ledgerNote = `Deposit from ${payerAddress ? payerAddress.slice(0,6)+'...'+payerAddress.slice(-4) : 'Unknown'} to ${depositAddress.slice(0,6)}... TX:${txSignature.slice(0,6)}...`;
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, stringUserId, transferAmount, 'deposit', { deposit_id: depositId }, ledgerNote); 
        if (!balanceUpdateResult.success || typeof balanceUpdateResult.newBalanceLamports === 'undefined') {
            throw new Error(`Failed to update user ${stringUserId} balance/ledger for deposit: ${balanceUpdateResult.error || "Unknown DB error."}`);
        }
        
        await client.query('COMMIT');
        processedDepositTxSignatures.add(txSignature);

        // Prepare variables for the HTML message
        const newBalanceUSDDisplay = await formatBalanceForDisplay(balanceUpdateResult.newBalanceLamports, 'USD');
        const userForNotif = await getOrCreateUser(stringUserId); 
        // getPlayerDisplayReference output should be HTML-escaped for safety in HTML context
        const playerRefHTML = escapeHTML(getPlayerDisplayReference(userForNotif || { id: stringUserId, first_name: "Player" }));
        
        // Construct the HTML message
        const confirmationMessageHTML = 
            `🎉 <b>Deposit Confirmed, ${playerRefHTML}!</b> 🎉\n\n` +
            `Your deposit of <b>${escapeHTML(depositAmountSOLDisplay)}</b> has been successfully credited to your casino account.\n\n` +
            `💰 Your New Balance: Approx. <b>${escapeHTML(newBalanceUSDDisplay)}</b>\n` +
            `🧾 Transaction ID: <code>${escapeHTML(txSignature)}</code>\n\n` +
            `Time to hit the tables! Good luck! 🎰`;

        await safeSendMessage(stringUserId, 
            confirmationMessageHTML,
            { parse_mode: 'HTML' } // Changed to HTML
        );
        
    } catch (error) {
        console.error(`❌ ${logPrefix} CRITICAL ERROR processing deposit TX ${txSignature}: ${error.message}`, error.stack?.substring(0,500));
        if (client) { await client.query('ROLLBACK').catch(rbErr => console.error(`❌ ${logPrefix} Rollback failed:`, rbErr)); }
        processedDepositTxSignatures.add(txSignature); 
        if (typeof notifyAdmin === 'function') { 
            await notifyAdmin(`🚨 *CRITICAL Error Processing Deposit* 🚨\nTX: <code>${escapeHTML(txSignature)}</code>\nAddr: <code>${escapeHTML(depositAddress)}</code>\nUser: <code>${escapeHTML(stringUserId)}</code>\n*Error:*\n<pre>${escapeHTML(String(error.message || error))}</pre>\nManual investigation required.`, {parse_mode:'HTML'});
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
    const logPrefix = '[SweepAddresses_V2]'; // Added V2 for new logging
    if (isShuttingDown) { 
        console.log(`${logPrefix} Shutdown detected. Aborting sweep cycle.`);
        return; 
    }
    if (sweepDepositAddresses.isRunning) {
        console.log(`${logPrefix} Sweep cycle already running. Skipping.`);
        return;
    }
    sweepDepositAddresses.isRunning = true;
    console.log(`🧹 ${logPrefix} Starting new sweep cycle...`);

    let addressesProcessedThisCycle = 0;
    let totalSweptThisCycle = 0n;
    const sweepBatchSize = parseInt(process.env.SWEEP_BATCH_SIZE, 10) || 15;
    const sweepAddressDelayMs = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10) || 1500;
    
    if (!MAIN_BOT_KEYPAIR || !DEPOSIT_MASTER_SEED_PHRASE) {
        console.error(`❌ ${logPrefix} CRITICAL: MAIN_BOT_KEYPAIR or DEPOSIT_MASTER_SEED_PHRASE not available. Cannot sweep.`);
        sweepDepositAddresses.isRunning = false;
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 ${logPrefix} Critical configuration missing (Main Bot Keypair or Master Seed). Sweeping aborted.`, {parse_mode: 'MarkdownV2'});
        return;
    }
    const sweepTargetAddress = MAIN_BOT_KEYPAIR.publicKey.toBase58();
    console.log(`${logPrefix} Target sweep address: ${sweepTargetAddress}`);

    let rentLamports;
    try {
        rentLamports = BigInt(await solanaConnection.getMinimumBalanceForRentExemption(0));
        console.log(`${logPrefix} Current rent exemption for 0-data account: ${rentLamports} lamports.`);
    } catch (rentError) {
        console.error(`❌ ${logPrefix} Failed to get minimum balance for rent exemption: ${rentError.message}. Using fallback.`);
        rentLamports = BigInt(890880); // Approx 0.00089 SOL
        if (typeof notifyAdmin === "function") await notifyAdmin(`⚠️ ${logPrefix} Failed to fetch rent exemption, using fallback: ${rentLamports}. Error: ${escapeMarkdownV2(rentError.message)}`, {parse_mode: 'MarkdownV2'});
    }

    const feeForSweepTxItself = BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS || '20000');
    const minimumLamportsToLeave = rentLamports + feeForSweepTxItself;
    console.log(`${logPrefix} Minimum lamports to leave in source account (rent + fee buffer): ${minimumLamportsToLeave} (${formatCurrency(minimumLamportsToLeave, 'SOL')})`);

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
            console.log(`${logPrefix} No addresses found meeting initial sweep criteria in this batch or query failed.`);
            sweepDepositAddresses.isRunning = false;
            return;
        }

        if (addressesToConsiderRes.rowCount > 0) {
            console.log(`${logPrefix} Found ${addressesToConsiderRes.rowCount} potential addresses in this batch to check for sweeping.`);
        } else {
            console.log(`${logPrefix} No addresses currently meet the criteria for sweeping.`);
            sweepDepositAddresses.isRunning = false;
            return;
        }

        for (const addrData of addressesToConsiderRes.rows) {
            if (isShuttingDown) { console.log(`${logPrefix} Shutdown detected during address processing for WID ${addrData.wallet_id}.`); break; }
            
            const addrLogPrefix = `[Sweep Addr:${addrData.public_key.slice(0, 6)} WID:${addrData.wallet_id}]`;
            let depositKeypair;
            let clientForThisAddress = null;

            try {
                clientForThisAddress = await pool.connect();
                await clientForThisAddress.query('BEGIN');
                console.log(`${addrLogPrefix} Processing address. Path: ${addrData.derivation_path}`);

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
                    continue; // Move to next address
                }
                console.log(`${addrLogPrefix} Keypair derived successfully.`);

                const balanceLamports = await getSolBalance(addrData.public_key);
                if (balanceLamports === null) {
                    console.warn(`${addrLogPrefix} Could not fetch balance. Skipping.`);
                    await clientForThisAddress.query('ROLLBACK'); 
                    continue;
                }
                console.log(`${addrLogPrefix} On-chain balance: ${balanceLamports} (${formatCurrency(balanceLamports, 'SOL')})`);

                if (balanceLamports <= minimumLamportsToLeave) {
                    console.log(`${addrLogPrefix} Balance ${balanceLamports} is <= minimum to leave ${minimumLamportsToLeave}. Marking 'swept_low_balance'.`);
                    await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, true, balanceLamports);
                    await clientForThisAddress.query('COMMIT');
                    continue;
                }

                const amountToSweep = balanceLamports - minimumLamportsToLeave;
                console.log(`${addrLogPrefix} Calculated amountToSweep: ${amountToSweep} (${formatCurrency(amountToSweep, 'SOL')})`);
                
                if (amountToSweep <= 0n) {
                    console.warn(`${addrLogPrefix} Calculated amountToSweep is not positive (${amountToSweep}). Balance: ${balanceLamports}, MinToLeave: ${minimumLamportsToLeave}. Marking low balance.`);
                    await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, true, balanceLamports);
                    await clientForThisAddress.query('COMMIT');
                    continue;
                }

                const sweepPriorityFee = parseInt(process.env.SWEEP_PRIORITY_FEE_MICROLAMPORTS, 10) || 5000;
                const sweepComputeUnits = parseInt(process.env.SWEEP_COMPUTE_UNIT_LIMIT, 10) || 30000;
                console.log(`${addrLogPrefix} Attempting sendSol. Amount: ${amountToSweep}, PrioFee: ${sweepPriorityFee}, ComputeUnits: ${sweepComputeUnits}`);
                
                const sendResult = await sendSol(depositKeypair, sweepTargetAddress, amountToSweep, `Sweep from ${addrData.public_key.slice(0,4)}..${addrData.public_key.slice(-4)}`, sweepPriorityFee, sweepComputeUnits);

                if (sendResult.success && sendResult.signature) {
                    totalSweptThisCycle += amountToSweep;
                    addressesProcessedThisCycle++;
                    console.log(`✅ ${addrLogPrefix} Sweep successful! TX: ${sendResult.signature}. Amount: ${formatCurrency(amountToSweep, 'SOL')}`);
                    await recordSweepTransactionDB(clientForThisAddress, addrData.public_key, sweepTargetAddress, amountToSweep, sendResult.signature);
                    await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, true, balanceLamports); 
                } else {
                    console.error(`❌ ${addrLogPrefix} Sweep sendSol FAILED: ${sendResult.error}. Type: ${sendResult.errorType}. Retryable: ${sendResult.isRetryable}`);
                    if (sendResult.errorType === "InsufficientFundsError" || sendResult.isRetryable === false) {
                        await markDepositAddressInactiveDB(clientForThisAddress, addrData.wallet_id, false, balanceLamports); 
                        await clientForThisAddress.query("UPDATE user_deposit_wallets SET notes = COALESCE(notes, '') || ' Sweep Attempt Failed (sendSol): " + escapeMarkdownV2(String(sendResult.error || '')).substring(0,100) + "' WHERE wallet_id = $1", [addrData.wallet_id]);
                    }
                }
                await clientForThisAddress.query('COMMIT');
            } catch (addrError) {
                if (clientForThisAddress) await clientForThisAddress.query('ROLLBACK').catch(rbErr => console.error(`${addrLogPrefix} Rollback error: ${rbErr.message}`));
                console.error(`❌ ${addrLogPrefix} Error processing address ${addrData.public_key}: ${addrError.message}`, addrError.stack?.substring(0,500));
                // Mark as error in DB if possible
                if (clientForThisAddress) { 
                    try { /* ... (error marking logic as before) ... */ } catch {}
                }
            } finally {
                if (clientForThisAddress) clientForThisAddress.release();
            }
            await sleep(sweepAddressDelayMs);
        }
    } catch (cycleError) {
        console.error(`❌ ${logPrefix} Critical error in sweep cycle setup or main query: ${cycleError.message}`, cycleError.stack?.substring(0,500));
        if (typeof notifyAdmin === 'function') await notifyAdmin(`🚨 *ERROR in Fund Sweeping Cycle Setup* 🚨\n\n\`${escapeMarkdownV2(String(cycleError.message || cycleError))}\`\nCheck logs. Sweeping aborted this cycle.`, {parse_mode: 'MarkdownV2'});
    } finally {
        sweepDepositAddresses.isRunning = false;
        if (addressesProcessedThisCycle > 0) {
            const sweptAmountFormatted = formatCurrency(totalSweptThisCycle, 'SOL');
            console.log(`🧹 ${logPrefix} Sweep cycle finished. Swept ~${sweptAmountFormatted} from ${addressesProcessedThisCycle} addresses.`);
            if(typeof notifyAdmin === 'function') await notifyAdmin(`🧹 Sweep Report: Swept approx. ${escapeMarkdownV2(sweptAmountFormatted)} from ${addressesProcessedThisCycle} deposit addresses.`, {parse_mode: 'MarkdownV2'});
        } else if (addressesToConsiderRes && addressesToConsiderRes.rowCount > 0) {
            console.log(`🧹 ${logPrefix} Sweep finished. No funds swept from ${addressesToConsiderRes.rowCount} considered addresses (likely due to low balance or errors during processing).`);
        } else {
            console.log(`🧹 ${logPrefix} Sweep cycle finished. No addresses found needing a sweep in this batch.`);
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
    const logPrefix = `[WithdrawJob_HTML ID:${withdrawalId}]`; // Added HTML to log prefix
    console.log(`⚙️ ${logPrefix} Processing withdrawal payout...`);
    let clientForDb = null;
    // Initialize sendSolResult to ensure error.isRetryable can be checked even if sendSol is not reached
    let sendSolResult = { success: false, error: "Send SOL not initiated", isRetryable: false, signature: null, blockTime: null }; 
    const payerKeypair = MAIN_BOT_KEYPAIR; 

    const details = await getWithdrawalDetailsDB(withdrawalId);
    if (!details) {
        const error = new Error(`Withdrawal details not found for ID ${withdrawalId}. Job cannot proceed.`);
        error.isRetryable = false; // This error is not typically retryable for the job
        console.error(`${logPrefix} ${error.message}`);
        // Optionally, update DB to a specific error state if details are missing, though this job might not run again if error is thrown.
        // This depends on how your job queue handles thrown errors vs. explicit retry flags.
        throw error;
    }

    // If already in a final state, don't re-process.
    if (details.status === 'completed' || details.status === 'confirmed' || details.status === 'sent') {
        console.log(`ℹ️ ${logPrefix} Withdrawal already in a final state (${details.status}). Skipping.`);
        return; 
    }
    // If it previously failed and the failure was marked as non-retryable by sendSol, don't try again.
    // The isRetryable flag on jobError (thrown at the end) controls queue retries.
    if (details.status === 'failed' /* && specific non-retryable error was logged previously */) {
        // This check might be too simple. The job queue's retry mechanism based on thrown error.isRetryable is better.
        // For now, if it's 'failed', we might only proceed if a retry is explicitly desired or if the error type was transient.
        // The current job queue retries based on thrown errors. If it gets here, it's a new attempt.
        console.log(`ℹ️ ${logPrefix} Withdrawal previously marked 'failed'. This is attempt #${(jobData?.attempts || 0) + 1}. Proceeding.`);
    }

    const userId = String(details.user_telegram_id);
    const recipient = details.destination_address;
    const amountToActuallySend = BigInt(details.amount_lamports); // Amount user receives
    const feeApplied = BigInt(details.fee_lamports); // Fee deducted from user's balance initially
    const totalAmountDebitedFromUser = amountToActuallySend + feeApplied; // This was already taken from user balance

    const userForNotif = await getOrCreateUser(userId); 
    // Ensure playerRefHTML is HTML-escaped for use in HTML messages
    const playerRefHTML = escapeHTML(getPlayerDisplayReference(userForNotif || {id:userId, first_name:"Player"}));

    // Phase 1: Mark as 'processing'
    try {
        clientForDb = await pool.connect();
        await clientForDb.query('BEGIN');
        await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'processing', null, null, null); // No sig/err/blocktime yet
        await clientForDb.query('COMMIT');
        console.log(`${logPrefix} Marked withdrawal as 'processing' in DB.`);
    } catch (dbError) {
        if (clientForDb) await clientForDb.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} DB Rollback Error setting 'processing': ${rbErr.message}`));
        console.error(`❌ ${logPrefix} DB error setting status to 'processing': ${dbError.message}`);
        const jobError = new Error(`DB error pre-send: ${dbError.message}`); 
        jobError.isRetryable = true; // DB errors are often retryable
        throw jobError; 
    } finally {
        if (clientForDb) clientForDb.release();
        clientForDb = null; 
    }

    // Phase 2: Attempt SOL transfer
    try {
        sendSolResult = await sendSol(
            payerKeypair, 
            recipient, 
            amountToActuallySend, 
            `Withdrawal ID ${withdrawalId} from ${BOT_NAME}`, 
            details.priority_fee_microlamports, 
            details.compute_unit_limit
        ); 

        clientForDb = await pool.connect(); 
        await clientForDb.query('BEGIN');

        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful. TX: ${sendSolResult.signature}. Marking 'completed'.`);
            
            const finalSignature = sendSolResult.signature; // String
            const finalBlockTime = (typeof sendSolResult.blockTime === 'number') ? sendSolResult.blockTime : null;
            const finalErrorMessage = null; 

            await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'completed', finalSignature, finalErrorMessage, finalBlockTime);
            
            await clientForDb.query('COMMIT');
            console.log(`✅ ${logPrefix} Withdrawal ID ${withdrawalId} successfully marked 'completed' in DB.`);
            
            const successMsgHTML = `💸 <b>Withdrawal Sent Successfully, ${playerRefHTML}!</b> 💸\n\n` +
                                 `Your withdrawal of <b>${escapeHTML(formatCurrency(amountToActuallySend, 'SOL'))}</b> to wallet <code>${escapeHTML(recipient)}</code> has been processed.\n` +
                                 `🧾 Transaction ID: <a href="https://solscan.io/tx/${escapeHTML(sendSolResult.signature)}">${escapeHTML(sendSolResult.signature)}</a>\n\n` +
                                 `Funds should arrive shortly depending on network confirmations. Thank you for playing at ${escapeHTML(BOT_NAME)}!`;
            await safeSendMessage(userId, successMsgHTML, { parse_mode: 'HTML', disable_web_page_preview: true });
            return; // Successful completion of the job
        } else { 
            // sendSol failed
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure.';
            console.error(`❌ ${logPrefix} sendSol FAILED for withdrawal ID ${withdrawalId}. Reason: ${sendErrorMsg}. Type: ${sendSolResult.errorType}. Retryable: ${sendSolResult.isRetryable}`);
            
            // Mark as failed in DB
            await updateWithdrawalStatusDB(clientForDb, withdrawalId, 'failed', null, sendErrorMsg.substring(0, 250), null);
            
            // Refund the user (since balance was already debited when request was confirmed)
            console.log(`${logPrefix} sendSol failed. Attempting to refund ${totalAmountDebitedFromUser} lamports to user ${userId}.`);
            const refundNotes = `Refund for failed withdrawal ID ${withdrawalId}. Send Error: ${sendErrorMsg.substring(0,100)}`;
            const refundUpdateResult = await updateUserBalanceAndLedger( 
                clientForDb, userId, totalAmountDebitedFromUser, // Credit back the full amount
                'withdrawal_refund', { withdrawal_id: withdrawalId }, refundNotes
            );

            if (refundUpdateResult.success) {
                await clientForDb.query('COMMIT');
                console.log(`✅ ${logPrefix} Successfully refunded ${formatCurrency(totalAmountDebitedFromUser, 'SOL')} to user ${userId}.`);
                const failureMsgHTML = `⚠️ <b>Withdrawal Failed</b> ⚠️\n\n${playerRefHTML}, your withdrawal of <b>${escapeHTML(formatCurrency(amountToActuallySend, 'SOL'))}</b> could not be processed at this time (Reason: <code>${escapeHTML(sendErrorMsg)}</code>).\n` +
                                     `The full amount of <b>${escapeHTML(formatCurrency(totalAmountDebitedFromUser, 'SOL'))}</b> (which was reserved for this withdrawal, including fees) has been refunded to your casino balance.`;
                await safeSendMessage(userId, failureMsgHTML, {parse_mode: 'HTML'});
            } else {
                await clientForDb.query('ROLLBACK');
                console.error(`❌ CRITICAL ${logPrefix} FAILED TO REFUND USER ${userId} for withdrawal ${withdrawalId}. Amount: ${formatCurrency(totalAmountDebitedFromUser, 'SOL')}. Refund DB Error: ${refundUpdateResult.error}`);
                if (typeof notifyAdmin === 'function') {
                    notifyAdmin(`🚨🚨 *CRITICAL: FAILED WITHDRAWAL REFUND* 🚨🚨\nUser: ${playerRefHTML} (<code>${escapeHTML(String(userId))}</code>)\nWD ID: <code>${withdrawalId}</code>\nAmount Due (Refund): <code>${escapeHTML(formatCurrency(totalAmountDebitedFromUser, 'SOL'))}</code>\nSend Error: <code>${escapeHTML(sendErrorMsg)}</code>\nRefund DB Error: <code>${escapeHTML(refundUpdateResult.error || 'Unknown')}</code>\nMANUAL INTERVENTION REQUIRED.`, {parse_mode:'HTML'});
                }
            }
            
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true; 
            throw errorToThrowForRetry; // This will make the job queue retry if isRetryable is true
        }
    } catch (jobError) { 
        // This catch block handles errors from sendSol OR subsequent DB operations if sendSol succeeded but DB failed
        if (clientForDb && clientForDb.release) { 
             try { await clientForDb.query('ROLLBACK'); } catch(rbErr) { console.error(`${logPrefix} Final rollback error on jobError: ${rbErr.message}`);}
        }
        console.error(`❌ ${logPrefix} Error during withdrawal job ID ${withdrawalId} (Phase 2 - Send/Post-Send DB): ${jobError.message}`, jobError.stack?.substring(0,500));
        
        // Attempt to mark as 'failed' if not already completed, as a last resort.
        const updateClientFinal = await pool.connect();
        try {
            const currentDetailsAfterJobError = await getWithdrawalDetailsDB(withdrawalId, updateClientFinal); 
            if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'completed' && currentDetailsAfterJobError.status !== 'failed') {
                await updateWithdrawalStatusDB(updateClientFinal, withdrawalId, 'failed', null, `Job error: ${String(jobError.message || jobError)}`.substring(0,250), null);
            }
        } catch (finalStatusUpdateError) {
            console.error(`${logPrefix} Failed to update status to 'failed' after job error (Phase 2): ${finalStatusUpdateError.message}`);
        } finally {
            updateClientFinal.release();
        }
        
        // Ensure the error re-thrown to the job queue has the correct retry flag
        if (jobError.isRetryable === undefined) { 
            jobError.isRetryable = sendSolResult?.isRetryable || false; // Default to sendSolResult's retryable status or false
        }
        throw jobError; // Re-throw for retry mechanism in addPayoutJob
    } finally {
        if (clientForDb && clientForDb.release) clientForDb.release(); 
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
