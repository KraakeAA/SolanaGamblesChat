// --- Start of Part 1 ---
// index.js - Part 1: Core Imports, Basic Setup, Global State & Utilities
//---------------------------------------------------------------------------

// ESM-style imports
import 'dotenv/config'; // Ensure this is at the very top to load your .env file
import TelegramBot from 'node-telegram-bot-api';
import { Pool } from 'pg'; // For PostgreSQL

console.log("Loading Part 1: Core Imports, Basic Setup, Global State & Utilities...");

// --- Helper function for JSON.stringify to handle BigInts (Defined ONCE Globally) ---
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
  }, 2); // Pretty print
}
console.log("[Global Utils] stringifyWithBigInt helper function defined.");

//---------------------------------------------------------------------------
// index.js - Part 1: Core Imports & Basic Setup
//---------------------------------------------------------------------------

// --- Environment Variable Validation & Defaults ---
const OPTIONAL_ENV_DEFAULTS = {
    'DB_POOL_MAX': '25',
    'DB_POOL_MIN': '5',
    'DB_IDLE_TIMEOUT': '30000',
    'DB_CONN_TIMEOUT': '5000',
    'DB_SSL': 'true',
    'DB_REJECT_UNAUTHORIZED': 'false',
    'SHUTDOWN_FAIL_TIMEOUT_MS': '10000',
    'JACKPOT_CONTRIBUTION_PERCENT': '0.01',
    'MIN_BET_AMOUNT': '5',
    'MAX_BET_AMOUNT': '1000',
    'COMMAND_COOLDOWN_MS': '2000',
    'JOIN_GAME_TIMEOUT_MS': '60000',
    'DEFAULT_STARTING_BALANCE_LAMPORTS': '100000000',
    'TARGET_JACKPOT_SCORE': '100', // Default for Dice Escalator Jackpot
    'BOT_STAND_SCORE_DICE_ESCALATOR': '10', // Default for Dice Escalator Bot stand score
    'DICE_21_TARGET_SCORE': '21', // Default for Dice 21 target
    'DICE_21_BOT_STAND_SCORE': '17' // Default for Dice 21 Bot stand score
};

// Apply defaults if not set in process.env
Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
    if (process.env[key] === undefined) {
        console.log(`[ENV_DEFAULT] Setting default for ${key}: ${defaultValue}`);
        process.env[key] = defaultValue;
    }
});

// --- Core Configuration Constants ---
const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_USER_ID = process.env.ADMIN_USER_ID;
const DATABASE_URL = process.env.DATABASE_URL;

// --- Crucial Game Play Constants & Settings ---
// Shutdown and System Behavior
const SHUTDOWN_FAIL_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);

// Jackpot Mechanics (Mainly for Dice Escalator)
const JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.JACKPOT_CONTRIBUTION_PERCENT);
const MAIN_JACKPOT_ID = 'dice_escalator_main'; // ID for the primary Dice Escalator jackpot
const TARGET_JACKPOT_SCORE = parseInt(process.env.TARGET_JACKPOT_SCORE, 10);

// Bot Behavior Scores for Games
const BOT_STAND_SCORE_DICE_ESCALATOR = parseInt(process.env.BOT_STAND_SCORE_DICE_ESCALATOR, 10);
const DICE_21_TARGET_SCORE = parseInt(process.env.DICE_21_TARGET_SCORE, 10);
const DICE_21_BOT_STAND_SCORE = parseInt(process.env.DICE_21_BOT_STAND_SCORE, 10);

// General Game Limits & Timings
const MIN_BET_AMOUNT = parseInt(process.env.MIN_BET_AMOUNT, 10);
const MAX_BET_AMOUNT = parseInt(process.env.MAX_BET_AMOUNT, 10);
const COMMAND_COOLDOWN_MS = parseInt(process.env.COMMAND_COOLDOWN_MS, 10);
const JOIN_GAME_TIMEOUT_MS = parseInt(process.env.JOIN_GAME_TIMEOUT_MS, 10);
const DEFAULT_STARTING_BALANCE_LAMPORTS = BigInt(process.env.DEFAULT_STARTING_BALANCE_LAMPORTS);


// --- Validations and Logging for Critical Variables ---
if (!BOT_TOKEN) {
    console.error("FATAL ERROR: BOT_TOKEN is not defined. Bot cannot start.");
    process.exit(1);
}
if (!DATABASE_URL) {
    console.error("FATAL ERROR: DATABASE_URL is not defined. Cannot connect to PostgreSQL.");
    process.exit(1);
}
if (isNaN(TARGET_JACKPOT_SCORE)) {
    console.error(`FATAL ERROR: TARGET_JACKPOT_SCORE is not a valid number. Value from env: '${process.env.TARGET_JACKPOT_SCORE}'. Check .env file or defaults.`);
    process.exit(1);
}
if (isNaN(BOT_STAND_SCORE_DICE_ESCALATOR)) {
    console.error(`FATAL ERROR: BOT_STAND_SCORE_DICE_ESCALATOR is not a valid number. Value from env: '${process.env.BOT_STAND_SCORE_DICE_ESCALATOR}'. Check .env file or defaults.`);
    process.exit(1);
}
if (isNaN(DICE_21_TARGET_SCORE)) {
    console.error(`FATAL ERROR: DICE_21_TARGET_SCORE is not a valid number. Value from env: '${process.env.DICE_21_TARGET_SCORE}'. Check .env file or defaults.`);
    process.exit(1);
}
if (isNaN(DICE_21_BOT_STAND_SCORE)) {
    console.error(`FATAL ERROR: DICE_21_BOT_STAND_SCORE is not a valid number. Value from env: '${process.env.DICE_21_BOT_STAND_SCORE}'. Check .env file or defaults.`);
    process.exit(1);
}


console.log("BOT_TOKEN loaded successfully.");
if (ADMIN_USER_ID) console.log(`Admin User ID: ${ADMIN_USER_ID} loaded.`);
else console.log("INFO: No ADMIN_USER_ID set (optional).");

console.log("--- Game Settings Loaded ---");
console.log(`Dice Escalator - Target Jackpot Score: ${TARGET_JACKPOT_SCORE}`);
console.log(`Dice Escalator - Bot Stand Score: ${BOT_STAND_SCORE_DICE_ESCALATOR}`);
console.log(`Dice Escalator - Jackpot Contribution: ${JACKPOT_CONTRIBUTION_PERCENT * 100}%`);
console.log(`Dice 21 - Target Score: ${DICE_21_TARGET_SCORE}`);
console.log(`Dice 21 - Bot Stand Score: ${DICE_21_BOT_STAND_SCORE}`);
console.log(`Bet Limits: ${MIN_BET_AMOUNT} - ${MAX_BET_AMOUNT} credits`);
console.log(`Default Starting Balance: ${DEFAULT_STARTING_BALANCE_LAMPORTS} lamports`);
console.log(`Command Cooldown: ${COMMAND_COOLDOWN_MS}ms`);
console.log(`Join Game Timeout: ${JOIN_GAME_TIMEOUT_MS}ms`);
console.log("-----------------------------");


// --- PostgreSQL Pool Initialization ---
console.log("⚙️ Setting up PostgreSQL Pool...");
console.log(`DB_SSL is: '${process.env.DB_SSL}' (type: ${typeof process.env.DB_SSL})`);
console.log(`DB_REJECT_UNAUTHORIZED is: '${process.env.DB_REJECT_UNAUTHORIZED}' (type: ${typeof process.env.DB_REJECT_UNAUTHORIZED})`);

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

pool.on('connect', client => {
    console.log('ℹ️ [DB Pool] Client connected to PostgreSQL.');
});
pool.on('error', (err, client) => {
    console.error('❌ Unexpected error on idle PostgreSQL client', err);
    if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
        safeSendMessage(ADMIN_USER_ID, `🚨 DATABASE POOL ERROR (Idle Client): ${escapeMarkdownV2(err.message || String(err))}`)
            .catch(notifyErr => console.error("Failed to notify admin about DB pool error:", notifyErr));
    } else {
        console.error(`[ADMIN ALERT during DB Pool Error (Idle Client)] ${err.message || String(err)} (safeSendMessage, escapeMarkdownV2, or ADMIN_USER_ID might not be defined yet)`);
    }
});
console.log("✅ PostgreSQL Pool created.");

// --- Telegram Bot Instance ---
const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("Telegram Bot instance created and configured for polling.");

const BOT_VERSION = '2.4.0-multi-game-casino-feel'; // Updated version marker
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;

// --- Global State Variables for Shutdown & Operation ---
let isShuttingDown = false; // Flag to prevent multiple shutdown sequences

// --- In-memory stores ---
let activeGames = new Map(); // For active game state
let userCooldowns = new Map(); // For command cooldowns

console.log(`Group Chat Casino Bot v${BOT_VERSION} initializing...`);
console.log(`Current system time: ${new Date().toISOString()}`);
console.log(`Node.js Version: ${process.version}`);

// --- Core Utility Functions ---

// Escapes text for Telegram MarkdownV2 mode
const escapeMarkdownV2 = (text) => {
    if (text === null || typeof text === 'undefined') return '';
    return String(text).replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};

// Safely sends a message, handling potential errors and length limits
async function safeSendMessage(chatId, text, options = {}) {
    const LOG_PREFIX_SSM = "[safeSendMessage]";
    if (!chatId || typeof text !== 'string') {
        console.error(`${LOG_PREFIX_SSM} Invalid input: ChatID is ${chatId}, Text type is ${typeof text}. Preview: ${String(text).substring(0, 50)}`);
        return undefined;
    }

    let messageToSend = text;
    let finalOptions = { ...options };

    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsis = "... (message truncated)";
        const truncateAt = MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsis.length;
        messageToSend = (truncateAt > 0) ? messageToSend.substring(0, truncateAt) + ellipsis : messageToSend.substring(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH);
        console.warn(`${LOG_PREFIX_SSM} Message for chat ${chatId} was truncated before potential escaping.`);
    }

    if (finalOptions.parse_mode === 'MarkdownV2') {
        // It's generally better to escape the original full text if possible, then truncate if still too long after escaping.
        // However, escaping can increase length. Current approach: truncate then escape.
        messageToSend = escapeMarkdownV2(messageToSend);
        if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
             console.warn(`${LOG_PREFIX_SSM} Message for chat ${chatId} (MarkdownV2) still exceeds length limit AFTER escaping and initial truncation. Sending anyway.`);
             // Potentially truncate again, or send as multiple messages if critical. For now, send as is.
        }
    }

    if (!bot) {
        console.error(`${LOG_PREFIX_SSM} Error: Telegram 'bot' instance is not available. Cannot send to chat ${chatId}.`);
        return undefined;
    }

    try {
        if (typeof bot.sendMessage !== 'function') {
            throw new Error("'bot.sendMessage' is not a function. Bot instance might be improperly initialized.");
        }
        const sentMessage = await bot.sendMessage(chatId, messageToSend, finalOptions);
        // console.log(`${LOG_PREFIX_SSM} Message sent successfully to chat ${chatId}. Msg ID: ${sentMessage ? sentMessage.message_id : 'N/A'}`);
        return sentMessage;
    } catch (error) {
        console.error(`${LOG_PREFIX_SSM} Failed to send message to chat ${chatId}. Code: ${error.code || 'N/A'}, Msg: ${error.message}`);
        if (error.response?.body) {
            console.error(`${LOG_PREFIX_SSM} API Response Body: ${JSON.stringify(error.response.body)}`);
        }
        return undefined;
    }
}

// Simple asynchronous sleep function
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

console.log("Part 1: Core Imports, Basic Setup, Global State & Utilities - Complete.");
// --- End of Part 1 ---
//---------------------------------------------------------------------------
// --- Start of Part 2 ---
// index.js - Part 2: Database Operations & Data Management (DATABASE BACKED) (Updated)
//---------------------------------------------------------------------------
console.log("Loading Part 2: Database Operations & Data Management (DATABASE BACKED)...");

// In-memory stores for non-critical/session data
const groupGameSessions = new Map(); // Tracks active game per group (can be re-evaluated if needed)
// userDatabase (in-memory map for balances) is NOW OBSOLETE. Balances are in PostgreSQL.
console.log("In-memory data stores (groupGameSessions) initialized.");


// --- queryDatabase Helper Function ---
// (Ensure 'pool' is defined in Part 1 and globally accessible)
async function queryDatabase(sql, params = [], dbClient = pool) { // 'pool' is expected from Part 1
    if (!dbClient) {
        const poolError = new Error("Database pool/client is not available for queryDatabase. 'pool' might not be correctly defined or accessible from Part 1.");
        console.error("❌ CRITICAL: queryDatabase called but dbClient (pool) is invalid!", poolError.stack);
        throw poolError;
    }
    if (typeof sql !== 'string' || sql.trim().length === 0) {
        const sqlError = new TypeError(`queryDatabase received invalid SQL query (type: ${typeof sql}, value: ${sql})`);
        console.error(`❌ DB Query Error:`, sqlError.message);
        throw sqlError;
    }
    try {
        // console.log(`[DB_QUERY] Executing SQL: ${sql.substring(0, 100)}${sql.length > 100 ? '...' : ''}`, params);
        const result = await dbClient.query(sql, params);
        // console.log(`[DB_QUERY] Success. Rows: ${result.rowCount}`);
        return result;
    } catch (error) {
        console.error(`❌ DB Query Error Encountered:`);
        console.error(`   SQL (truncated): ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`);
        // Use stringifyWithBigInt if available globally, otherwise basic JSON.stringify
        const safeParamsString = typeof stringifyWithBigInt === 'function'
            ? stringifyWithBigInt(params)
            : JSON.stringify(params, (key, value) => typeof value === 'bigint' ? value.toString() + 'n' : value);
        console.error(`   Params: ${safeParamsString}`);
        console.error(`   Error Code: ${error.code || 'N/A'}`);
        console.error(`   Error Message: ${error.message}`);
        if (error.constraint) { console.error(`   Constraint Violated: ${error.constraint}`); }
        throw error;
    }
}

// --- User and Balance Functions (DATABASE BACKED) ---
// Constants like DEFAULT_STARTING_BALANCE_LAMPORTS, JACKPOT_CONTRIBUTION_PERCENT, MAIN_JACKPOT_ID
// are now expected to be defined globally in Part 1.

// Gets user data (from DB), creates if doesn't exist.
// Returns user object including balance.
// Does NOT expect telegram_username or telegram_first_name columns in wallets table.
async function getUser(userId, username = null, firstName = null) {
    const LOG_PREFIX_GETUSER = "[DB_GetUser]";
    const userIdStr = String(userId);
    let client; // Declare client here to ensure it's in scope for finally block

    console.log(`${LOG_PREFIX_GETUSER} Attempting to get/create user: ${userIdStr}, Username: ${username}, FirstName: ${firstName}`);

    try {
        if (!pool) { // Check if pool is available
            console.error(`${LOG_PREFIX_GETUSER} CRITICAL: Database pool is not defined or accessible!`);
            throw new Error("Database pool is not initialized.");
        }
        client = await pool.connect();
        console.log(`${LOG_PREFIX_GETUSER} DB client connected for user ${userIdStr}. Starting transaction.`);
        await client.query('BEGIN');

        // Try to get wallet/user first
        let userResult = await client.query(
            'SELECT user_id, referral_code, last_used_at FROM wallets WHERE user_id = $1',
            [userIdStr]
        );
        console.log(`${LOG_PREFIX_GETUSER} Wallet query result for ${userIdStr}: ${userResult.rows.length} rows.`);

        let userWalletData;
        let isNewUser = false;
        let actualBalanceLamports;

        // Ensure DEFAULT_STARTING_BALANCE_LAMPORTS is accessible
        if (typeof DEFAULT_STARTING_BALANCE_LAMPORTS === 'undefined') {
            console.error(`${LOG_PREFIX_GETUSER} CRITICAL: DEFAULT_STARTING_BALANCE_LAMPORTS is undefined! Cannot proceed with user creation/balance check.`);
            throw new Error("System configuration error: Default starting balance not set.");
        }


        if (userResult.rows.length === 0) {
            console.log(`${LOG_PREFIX_GETUSER} User ${userIdStr} not found. Creating new wallet and balance entry.`);
            isNewUser = true;
            const newReferralCode = `ref${Date.now().toString(36)}${Math.random().toString(36).substring(2, 5)}`;
            const insertWalletQuery = `
                INSERT INTO wallets (user_id, referral_code, created_at, last_used_at) 
                VALUES ($1, $2, NOW(), NOW())
                RETURNING user_id, referral_code, last_used_at; 
            `;
            userResult = await client.query(insertWalletQuery, [userIdStr, newReferralCode]);
            userWalletData = userResult.rows[0];
            console.log(`${LOG_PREFIX_GETUSER} New wallet created for ${userIdStr} with referral code ${newReferralCode}.`);

            const insertBalanceQuery = `
                INSERT INTO user_balances (user_id, balance_lamports, updated_at)
                VALUES ($1, $2, NOW());
            `;
            await client.query(insertBalanceQuery, [userIdStr, DEFAULT_STARTING_BALANCE_LAMPORTS.toString()]);
            actualBalanceLamports = DEFAULT_STARTING_BALANCE_LAMPORTS;
            console.log(`${LOG_PREFIX_GETUSER} Initial balance of ${actualBalanceLamports} set for new user ${userIdStr}.`);
        } else {
            userWalletData = userResult.rows[0];
            console.log(`${LOG_PREFIX_GETUSER} User ${userIdStr} found. Updating last_used_at.`);
            await client.query('UPDATE wallets SET last_used_at = NOW() WHERE user_id = $1', [userIdStr]);

            const balanceResult = await client.query('SELECT balance_lamports FROM user_balances WHERE user_id = $1', [userIdStr]);
            console.log(`${LOG_PREFIX_GETUSER} Balance query result for existing user ${userIdStr}: ${balanceResult.rows.length} rows.`);
            if (balanceResult.rows.length === 0) {
                console.warn(`${LOG_PREFIX_GETUSER} Wallet exists for ${userIdStr} but no balance record! Creating with default balance.`);
                await client.query('INSERT INTO user_balances (user_id, balance_lamports, updated_at) VALUES ($1, $2, NOW())', [userIdStr, DEFAULT_STARTING_BALANCE_LAMPORTS.toString()]);
                actualBalanceLamports = DEFAULT_STARTING_BALANCE_LAMPORTS;
            } else {
                actualBalanceLamports = BigInt(balanceResult.rows[0].balance_lamports);
                console.log(`${LOG_PREFIX_GETUSER} Existing balance for ${userIdStr} is ${actualBalanceLamports}.`);
            }
        }

        await client.query('COMMIT');
        console.log(`${LOG_PREFIX_GETUSER} Transaction committed for user ${userIdStr}.`);

        return {
            userId: userWalletData.user_id,
            username: username || `User_${userIdStr}`,
            firstName: firstName,
            balance: Number(actualBalanceLamports), // For convenience in some game logic expecting Number
            balanceLamports: actualBalanceLamports, // For precise operations
            isNew: isNewUser,
            referral_code: userWalletData.referral_code,
            groupStats: new Map(), // Placeholder
        };

    } catch (error) {
        console.error(`${LOG_PREFIX_GETUSER} Error fetching/creating user ${userIdStr}: ${error.message}`, error);
        if (client) {
            try {
                await client.query('ROLLBACK');
                console.log(`${LOG_PREFIX_GETUSER} Transaction rolled back for user ${userIdStr} due to error.`);
            } catch (rbErr) {
                console.error(`${LOG_PREFIX_GETUSER} Rollback failed for user ${userIdStr}: ${rbErr.message}`, rbErr);
            }
        }
        throw error; // Re-throw the error to be handled by the caller
    } finally {
        if (client) {
            client.release();
            console.log(`${LOG_PREFIX_GETUSER} DB client released for user ${userIdStr}.`);
        }
    }
}

// Updates user balance IN THE DATABASE transactionally.
async function updateUserBalance(userId, amountChangeLamports, reason = "unknown_transaction", client_ = null, associatedGameId = null, chatIdForLog = null) {
    const LOG_PREFIX_UPDATEBAL = "[DB_UpdateBalance]";
    const userIdStr = String(userId);
    const operationClient = client_ || await pool.connect(); // Uses global 'pool' from Part 1

    console.log(`${LOG_PREFIX_UPDATEBAL} Attempting to update balance for UserID: ${userIdStr}, Change: ${amountChangeLamports}, Reason: ${reason}, GameID: ${associatedGameId}, ChatID: ${chatIdForLog}, Using Provided Client: ${!!client_}`);

    try {
        if (!client_) {
            console.log(`${LOG_PREFIX_UPDATEBAL} Starting new transaction for UserID: ${userIdStr}.`);
            await operationClient.query('BEGIN');
        } else {
            console.log(`${LOG_PREFIX_UPDATEBAL} Using existing transaction for UserID: ${userIdStr}.`);
        }

        const balanceSelectRes = await operationClient.query(
            'SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE',
            [userIdStr]
        );
        console.log(`${LOG_PREFIX_UPDATEBAL} Fetched current balance for ${userIdStr}. Rows: ${balanceSelectRes.rows.length}`);

        if (balanceSelectRes.rows.length === 0) {
            if (!client_) await operationClient.query('ROLLBACK');
            console.warn(`${LOG_PREFIX_UPDATEBAL} Update balance called for non-existent user balance record: ${userIdStr}. User should be created via getUser first.`);
            return { success: false, error: "User balance record not found. Ensure user exists." };
        }

        const currentBalanceLamports = BigInt(balanceSelectRes.rows[0].balance_lamports);
        const change = BigInt(amountChangeLamports);
        let proposedBalanceLamports = currentBalanceLamports + change;
        console.log(`${LOG_PREFIX_UPDATEBAL} User ${userIdStr}: CurrentBal=${currentBalanceLamports}, Change=${change}, ProposedBal=${proposedBalanceLamports}`);

        // Ensure JACKPOT_CONTRIBUTION_PERCENT and MAIN_JACKPOT_ID are accessible globally from Part 1
        if (typeof JACKPOT_CONTRIBUTION_PERCENT === 'undefined' || typeof MAIN_JACKPOT_ID === 'undefined') {
             console.warn(`${LOG_PREFIX_UPDATEBAL} WARNING: JACKPOT_CONTRIBUTION_PERCENT or MAIN_JACKPOT_ID is undefined. Jackpot contributions will be skipped.`);
        }

        let jackpotContribution = 0n;
        if (reason.startsWith('bet_placed_dice_escalator') && change < 0n && associatedGameId && typeof JACKPOT_CONTRIBUTION_PERCENT === 'number' && MAIN_JACKPOT_ID) {
            const betAmount = -change; // Absolute bet amount
            jackpotContribution = BigInt(Math.floor(Number(betAmount) * JACKPOT_CONTRIBUTION_PERCENT)); // Ensure JACKPOT_CONTRIBUTION_PERCENT is a fraction e.g. 0.01 for 1%
            // Using Math.floor to ensure whole lamports. Adjust precision as needed.
            // For higher precision: jackpotContribution = betAmount * BigInt(Math.round(JACKPOT_CONTRIBUTION_PERCENT * 10000)) / 10000n;


            if (jackpotContribution > 0n) {
                console.log(`${LOG_PREFIX_UPDATEBAL} Calculating jackpot contribution for Dice Escalator. Bet: ${betAmount}, Percent: ${JACKPOT_CONTRIBUTION_PERCENT}, Contribution: ${jackpotContribution}. Game: ${associatedGameId}`);
                await operationClient.query(
                    'UPDATE jackpot_status SET current_amount_lamports = current_amount_lamports + $1, updated_at = NOW(), last_contributed_game_id = $2 WHERE jackpot_id = $3',
                    [jackpotContribution.toString(), associatedGameId, MAIN_JACKPOT_ID]
                );
                console.log(`${LOG_PREFIX_UPDATEBAL} [JACKPOT] Contributed ${jackpotContribution} to ${MAIN_JACKPOT_ID} from game ${associatedGameId}.`);
            }
        }

        if (proposedBalanceLamports < 0n) {
            if (!client_) await operationClient.query('ROLLBACK');
            console.log(`${LOG_PREFIX_UPDATEBAL} User ${userIdStr} insufficient balance (${currentBalanceLamports}) for change of ${change}. Reason: ${reason}. Transaction rolling back (if new).`);
            return { success: false, error: "Insufficient balance.", currentBalance: Number(currentBalanceLamports), currentBalanceLamports: currentBalanceLamports };
        }

        await operationClient.query(
            'UPDATE user_balances SET balance_lamports = $1, updated_at = NOW() WHERE user_id = $2',
            [proposedBalanceLamports.toString(), userIdStr]
        );
        console.log(`${LOG_PREFIX_UPDATEBAL} User ${userIdStr} balance successfully updated in DB to: ${proposedBalanceLamports}`);

        // Bet Logging Logic
        const betDetails = { game_id: associatedGameId };
        let wagerAmountForLog = 0n;
        if (change < 0n) wagerAmountForLog = -change; // Log wager as positive

        if (reason.startsWith('bet_placed_') && wagerAmountForLog > 0n && associatedGameId && chatIdForLog) {
            const gameTypeFromReason = reason.substring('bet_placed_'.length).split(':')[0]; // e.g. "dice_escalator" or "group_coinflip_init"
            if (jackpotContribution > 0n) {
                betDetails.jackpot_contribution = jackpotContribution.toString();
            }
            console.log(`${LOG_PREFIX_UPDATEBAL} Logging 'bet_placed' event for user ${userIdStr}, game ${gameTypeFromReason}, wager ${wagerAmountForLog}, details: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(betDetails) : JSON.stringify(betDetails)}.`);
            await operationClient.query(
                `INSERT INTO bets (user_id, chat_id, game_type, wager_amount_lamports, bet_details, status, reason_tx, created_at, processed_at)
                 VALUES ($1, $2, $3, $4, $5, 'active', $6, NOW(), NOW())`,
                [userIdStr, String(chatIdForLog), gameTypeFromReason, wagerAmountForLog.toString(), betDetails, reason]
            );
        } else if (associatedGameId && (reason.startsWith('won_') || reason.startsWith('lost_') || reason.startsWith('push_') || reason.startsWith('refund_') || reason.startsWith('jackpot_win'))) {
            let statusForLog = reason.split(':')[0]; // e.g., "won", "lost", "push", "jackpot_win"
            let payoutAmountForLog = change; // For wins/refunds/pushes, 'change' is positive; for losses this needs adjustment.

            if (statusForLog === "lost") { // Handle specific 'lost' status without amount detail
                payoutAmountForLog = 0n; // Payout is 0 for a loss
            }
            // For 'won', 'push', 'refund', 'jackpot_win', change already represents the amount credited (profit + bet back, or just bet_back)

            // If it's a jackpot win payout specifically, log it as a separate "jackpot" type bet for clarity
            if (reason.startsWith('jackpot_win')) {
                betDetails.jackpot_won = change.toString();
                console.log(`${LOG_PREFIX_UPDATEBAL} Logging 'jackpot_win' as a special bet record for user ${userIdStr}, amount ${payoutAmountForLog}.`);
                await operationClient.query(
                    `INSERT INTO bets (user_id, chat_id, game_type, wager_amount_lamports, payout_amount_lamports, bet_details, status, reason_tx, created_at, processed_at)
                     VALUES ($1, $2, 'jackpot_payout', '0', $3, $4, 'processed', $5, NOW(), NOW())`,
                    [userIdStr, String(chatIdForLog || 'N/A'), payoutAmountForLog.toString(), betDetails, reason]
                );
            } else { // For regular game outcomes, update the existing 'active' bet record
                console.log(`${LOG_PREFIX_UPDATEBAL} Updating existing bet record for user ${userIdStr}, gameId ${associatedGameId}. New status: ${statusForLog}, Payout: ${payoutAmountForLog}.`);
                const updateBetResult = await operationClient.query(
                    `UPDATE bets SET status = $1, payout_amount_lamports = $2, reason_tx = $3, processed_at = NOW() 
                     WHERE user_id = $4 AND bet_details->>'game_id' = $5 AND status = 'active'`,
                    [statusForLog, payoutAmountForLog.toString(), reason, userIdStr, associatedGameId]
                );
                if (updateBetResult.rowCount === 0) {
                    console.warn(`${LOG_PREFIX_UPDATEBAL} WARN: No active bet found to update for game outcome. User: ${userIdStr}, GameID: ${associatedGameId}, Reason: ${reason}. This might happen if bet logging failed or game was already processed.`);
                }
            }
        }

        if (!client_) {
            await operationClient.query('COMMIT');
            console.log(`${LOG_PREFIX_UPDATEBAL} Transaction committed for UserID: ${userIdStr}.`);
        }
        console.log(`${LOG_PREFIX_UPDATEBAL} User ${userIdStr} balance updated to: ${proposedBalanceLamports}. Change: ${change}, Reason: ${reason}, Game: ${associatedGameId || 'N/A'}`);

        return {
            success: true,
            newBalance: Number(proposedBalanceLamports),
            newBalanceLamports: proposedBalanceLamports
        };

    } catch (error) {
        if (!client_) {
            try {
                await operationClient.query('ROLLBACK');
                console.log(`${LOG_PREFIX_UPDATEBAL} Transaction rolled back for UserID: ${userIdStr} due to error.`);
            } catch (rbErr) {
                console.error(`${LOG_PREFIX_UPDATEBAL} Rollback failed for UserID: ${userIdStr}: ${rbErr.message}`, rbErr);
            }
        }
        console.error(`${LOG_PREFIX_UPDATEBAL} Error updating balance for user ${userIdStr} (Reason: ${reason}, Game: ${associatedGameId || 'N/A'}): ${error.message}`, error);
        return { success: false, error: `Database error: ${error.message}`, currentBalance: Number(currentBalanceLamports || 0), currentBalanceLamports: (currentBalanceLamports || 0n) };
    } finally {
        if (!client_ && operationClient) {
            operationClient.release();
            console.log(`${LOG_PREFIX_UPDATEBAL} DB client released for UserID: ${userIdStr}.`);
        }
    }
}


// --- Group Session Functions (In-Memory) ---
async function getGroupSession(chatId, chatTitle) {
    const LOG_PREFIX_GS = "[GroupSession_Get]";
    const chatIdStr = String(chatId);
    console.log(`${LOG_PREFIX_GS} Requesting session for ChatID: ${chatIdStr}, Title: ${chatTitle}`);

    if (!groupGameSessions.has(chatIdStr)) {
        const newSession = {
            chatId: chatIdStr,
            chatTitle: chatTitle || `Group_${chatIdStr}`,
            currentGameId: null,
            currentGameType: null,
            currentBetAmount: null,
            lastActivity: new Date(),
        };
        groupGameSessions.set(chatIdStr, newSession);
        console.log(`${LOG_PREFIX_GS} New group session created for ${chatIdStr} ('${newSession.chatTitle}'). Session data: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(newSession) : JSON.stringify(newSession)}`);
        return { ...newSession }; // Return a copy
    }

    const session = groupGameSessions.get(chatIdStr);
    if (chatTitle && session.chatTitle !== chatTitle) {
        session.chatTitle = chatTitle;
        console.log(`${LOG_PREFIX_GS} Updated title for session ${chatIdStr} to "${chatTitle}"`);
    }
    session.lastActivity = new Date();
    // groupGameSessions.set(chatIdStr, session); // Update the map with new lastActivity and potentially title
    console.log(`${LOG_PREFIX_GS} Returning existing session for ${chatIdStr}. Last activity updated. Session data: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(session) : JSON.stringify(session)}`);
    return { ...session }; // Return a copy
}

async function updateGroupGameDetails(chatId, gameId, gameType, betAmount) {
    const LOG_PREFIX_UGS = "[GroupSession_UpdateDetails]";
    const chatIdStr = String(chatId);
    console.log(`${LOG_PREFIX_UGS} Updating game details for ChatID: ${chatIdStr}. GameID: ${gameId}, Type: ${gameType}, Bet: ${betAmount}`);

    // getGroupSession handles creation and returns a copy; we need to operate on the actual map entry or replace it.
    let session = groupGameSessions.get(chatIdStr);
    if (!session) {
        console.log(`${LOG_PREFIX_UGS} No existing session for ${chatIdStr}, creating one implicitly via getGroupSession.`);
        // This will create it, but we need to fetch it again or ensure we modify the one in the map.
        // A bit redundant, ideally getGroupSession would return a reference or a clear way to update.
        // For now, let's fetch it, modify, then set it back.
        // OR, modify getGroupSession to return the actual object if we want to mutate it directly (careful with shared refs).
        // Sticking to immutable pattern:
        const tempSession = await getGroupSession(chatIdStr, null); // Title might be null if not starting a new game that provides it.
        session = { ...tempSession }; // Start with a fresh copy or a newly created one
    } else {
        session = { ...session }; // Work on a copy to avoid direct mutation issues if shared
    }


    // Logic from original code: Only update certain fields for non-multi-game types or when clearing.
    // This logic seems specific to single-instance games like Coinflip/RPS.
    // Multi-instance games like DiceEscalator shouldn't overwrite these session-level "current game" details.
    if ((gameType && gameType !== 'DiceEscalator' && gameType !== 'Dice21' && gameType !== 'OverUnder7' && gameType !== 'Duel' && gameType !== 'Ladder' && gameType !== 'SevenOut' && gameType !== 'Slot') || gameId === null) {
        session.currentGameId = gameId;
        session.currentGameType = gameType;
        session.currentBetAmount = betAmount;
        console.log(`${LOG_PREFIX_UGS} Session details updated for single-instance game type or clearing. GameID: ${session.currentGameId}, Type: ${session.currentGameType}, Bet: ${session.currentBetAmount}`);
    } else {
        console.log(`${LOG_PREFIX_UGS} Not updating session's primary game details for multi-instance game type: ${gameType} or gameId not null.`);
    }

    session.lastActivity = new Date();
    groupGameSessions.set(chatIdStr, session); // Put the modified copy back into the map

    // Ensure formatCurrency is available (expected from Part 3)
    const betDisplay = (betAmount !== null && betAmount !== undefined && typeof formatCurrency === 'function')
        ? formatCurrency(betAmount)
        : (betAmount !== null && betAmount !== undefined ? `${betAmount} units` : 'N/A');

    console.log(`${LOG_PREFIX_UGS} Group ${chatIdStr} details updated. Current single-game slot (Coinflip/RPS): ID: ${session.currentGameId || 'None'}, Type: ${session.currentGameType || 'None'}, Bet: ${betDisplay}. Full session: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(session) : JSON.stringify(session)}`);
    return true;
}

console.log("Part 2: Database Operations & Data Management (DATABASE BACKED) (Updated) - Complete.");
// --- End of Part 2 ---
// --- Start of Part 3 ---
// index.js - Part 3: Telegram Helpers & Basic Game Utilities
//---------------------------------------------------------------------------
console.log("Loading Part 3: Telegram Helpers & Basic Game Utilities...");

// --- Telegram Specific Helper Functions ---

// Gets a display name from a user object and escapes it for MarkdownV2
function getEscapedUserDisplayName(userObject) {
    if (!userObject) return escapeMarkdownV2("Anonymous User");
    // Prefer first name, fallback to username, fallback to generic User ID
    const name = userObject.first_name || userObject.username || `User ${userObject.id}`;
    return escapeMarkdownV2(name);
}

// Creates a MarkdownV2 mention link for a user object
function createUserMention(userObject) {
    if (!userObject || !userObject.id) return escapeMarkdownV2("Unknown User");
    // Use first name if available, otherwise username, fallback to generic ID
    const displayName = userObject.first_name || userObject.username || `User ${userObject.id}`;
    // Format: [Link Text](tg://user?id=USER_ID)
    return `[${escapeMarkdownV2(displayName)}](tg://user?id=${userObject.id})`;
}

// Gets a player's display reference, preferring @username
function getPlayerDisplayReference(userObject) {
    if (!userObject) return escapeMarkdownV2("Unknown Player"); // Fallback for unknown user
    if (userObject.username) {
        return `@${escapeMarkdownV2(userObject.username)}`; // Ideal: @username
    }
    // Fallback to first name if username is not available
    const name = userObject.first_name || `Player ${userObject.id}`; // Or Player ID if no first name
    return escapeMarkdownV2(name);
}

// --- General Utility Functions ---

// Formats a number as currency (e.g., "1,000 credits")
function formatCurrency(amount, currencyName = "credits") {
    let num = Number(amount);
    // Handle non-numeric input gracefully
    if (isNaN(num)) {
        console.warn(`[formatCurrency] Received non-numeric amount: ${amount}`);
        num = 0;
    }
    // Format with commas and appropriate decimal places (0 for integers, up to 2 for fractions)
    return `${num.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: (num % 1 === 0 ? 0 : 2)
    })} ${currencyName}`;
}

// Generates a unique-ish ID for game instances
function generateGameId() {
    const timestamp = Date.now();
    // Add a random component to reduce collision likelihood
    const randomSuffix = Math.random().toString(36).substring(2, 9); // 7 random alphanumeric chars
    return `game_${timestamp}_${randomSuffix}`;
}

// --- Dice Display Utilities ---

// *** The 'diceEmojis' constant has been removed. ***

// Formats an array of dice roll numbers into a string with a generic dice emoji and the number
// This function is kept as it provides the "🎲 5" style formatting.
function formatDiceRolls(rolls) {
    if (!Array.isArray(rolls) || rolls.length === 0) return '';
    // Using generic dice emoji + number for each roll
    const diceVisuals = rolls.map(roll => `🎲 ${roll}`); // e.g., "🎲 1", "🎲 5"
    return diceVisuals.join(' '); // Join with space if multiple rolls are ever passed (e.g. for Yahtzee type games)
                                  // For Dice Escalator, 'rolls' will usually be a single element array.
}

// Generates an internal dice roll for the BOT's turn in games like Dice Escalator
// This is SEPARATE from the rolls generated by the helper bot for the player.
function rollDie(sides = 6) {
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6; // Default to 6 sides
    return Math.floor(Math.random() * sides) + 1;
}

console.log("Part 3: Telegram Helpers & Basic Game Utilities - Complete.");
// --- End of Part 3 ---
// --- Start of Part 4 ---
// index.js - Part 4: Simplified Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 4: Simplified Game Logic...");

// --- Coinflip Logic ---
function determineCoinFlipOutcome() {
    const isHeads = Math.random() < 0.5; // 50% chance for heads
    return isHeads
            ? { outcome: 'heads', outcomeString: "Heads", emoji: '🪙' }
            : { outcome: 'tails', outcomeString: "Tails", emoji: '🪙' };
}

// --- Dice Logic (Internal for Bot's Turn) ---
// Determines the outcome for the BOT's internal rolls (e.g., in Dice Escalator)
// Uses the internal rollDie function defined in Part 3.
function determineDieRollOutcome(sides = 6) {
    // Ensure rollDie function is defined (expected in Part 3)
    if (typeof rollDie !== 'function') {
         console.error("[determineDieRollOutcome] Error: rollDie function is not defined.");
         // Return a default or throw an error
         return { roll: 1 }; // Default fallback roll
    }
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6; // Ensure valid sides
    const roll = rollDie(sides); // Use the internal function
    // --- THIS LINE IS UPDATED ---
    return { roll: roll }; // <<< Updated line: Just return the roll number
                           // The emoji part was removed as diceEmojis constant was deleted.
                           // Display formatting is handled by formatDiceRolls in Part 3/5b.
}

// Constant defining the losing roll in Dice Escalator
const DICE_ESCALATOR_BUST_ON = 1;

// --- Rock Paper Scissors (RPS) Logic ---
const RPS_CHOICES = { ROCK: 'rock', PAPER: 'paper', SCISSORS: 'scissors' };
const RPS_EMOJIS = {
    [RPS_CHOICES.ROCK]: '🪨',
    [RPS_CHOICES.PAPER]: '📄',
    [RPS_CHOICES.SCISSORS]: '✂️'
};
const RPS_RULES = {
    [RPS_CHOICES.ROCK]: { beats: RPS_CHOICES.SCISSORS, verb: "crushes" },
    [RPS_CHOICES.PAPER]: { beats: RPS_CHOICES.ROCK, verb: "covers" },
    [RPS_CHOICES.SCISSORS]: { beats: RPS_CHOICES.PAPER, verb: "cuts" }
};

// Gets a random RPS choice for the bot or opponent if needed
function getRandomRPSChoice() {
    const choices = Object.values(RPS_CHOICES);
    const randomChoice = choices[Math.floor(Math.random() * choices.length)];
    return { choice: randomChoice, emoji: RPS_EMOJIS[randomChoice] };
}

// Determines the outcome of an RPS match given two choices
function determineRPSOutcome(choice1, choice2) {
    // Normalize choices to handle potential case issues or minor variations if needed
    const c1_key = String(choice1).toLowerCase();
    const c2_key = String(choice2).toLowerCase();

    // Validate inputs against defined choices
    if (!RPS_CHOICES[c1_key.toUpperCase()] || !RPS_CHOICES[c2_key.toUpperCase()]) {
        console.warn(`[RPS_WARN] Invalid choices in determineRPSOutcome: P1='${choice1}', P2='${choice2}'`);
        // Return an error object or handle invalid state appropriately
        return { result: 'error', description: "Invalid choices were made.", choice1, choice1Emoji: '❔', choice2, choice2Emoji: '❔' };
    }

    const c1 = RPS_CHOICES[c1_key.toUpperCase()]; // Get the canonical choice name
    const c2 = RPS_CHOICES[c2_key.toUpperCase()];
    const c1E = RPS_EMOJIS[c1]; // Get corresponding emoji
    const c2E = RPS_EMOJIS[c2];

    // Determine winner
    if (c1 === c2) { // Draw case
        return { result: 'draw', description: `${c1E} ${c1} vs ${c2E} ${c2}. It's a Draw!`, choice1:c1, choice1Emoji:c1E, choice2:c2, choice2Emoji:c2E };
    } else if (RPS_RULES[c1].beats === c2) { // Player 1 wins
        return { result: 'win1', description: `${c1E} ${c1} ${RPS_RULES[c1].verb} ${c2E} ${c2}. Player 1 wins!`, choice1:c1, choice1Emoji:c1E, choice2:c2, choice2Emoji:c2E };
    } else { // Player 2 wins (since it's not a draw and P1 didn't win)
        return { result: 'win2', description: `${c2E} ${c2} ${RPS_RULES[c2].verb} ${c1E} ${c1}. Player 2 wins!`, choice1:c1, choice1Emoji:c1E, choice2:c2, choice2Emoji:c2E };
    }
}
console.log("Part 4: Simplified Game Logic - Complete.");

// --- End of Part 4 ---
//---------------------------------------------------------------------------
// --- Start of Part 5a (Segment 1 of 2) ---
// index.js - Part 5a: Message & Callback Handling (Core Listeners, All Games)
//---------------------------------------------------------------------------
console.log("Loading Part 5a: Message & Callback Handling, All Game Flows...");

// --- Game Constants & Configuration ---
// Basic constants like COMMAND_COOLDOWN_MS, MIN_BET_AMOUNT, MAX_BET_AMOUNT, JOIN_GAME_TIMEOUT_MS are expected from Part 1
// TARGET_JACKPOT_SCORE for Dice Escalator is expected from Part 5b or Part 1

// --- CONSTANTS FOR DICE 21 ---
const DICE_21_TARGET_SCORE = 21;
const DICE_21_BOT_STAND_SCORE = 17;

// --- CONSTANTS FOR OVER/UNDER 7 ---
const OU7_PAYOUT_NORMAL = 1; // 1:1 profit (bet back + 1x bet)
const OU7_PAYOUT_SEVEN = 4;  // 4:1 profit (bet back + 4x bet)

// --- CONSTANTS FOR HIGH ROLLER DUEL ---
const DUEL_DICE_COUNT = 2; // Number of dice each participant rolls

// --- CONSTANTS FOR GREED'S LADDER ---
const LADDER_ROLL_COUNT = 3; // Fixed number of rolls for the player
const LADDER_BUST_ON = 1;
// Example Payout Tiers for Greed's Ladder (sum of LADDER_ROLL_COUNT dice):
const LADDER_PAYOUTS = [ // [minScore, maxScore, profitMultiplier (0 = push, -1 = lose)]
    { min: (LADDER_ROLL_COUNT * 5 + 1), max: (LADDER_ROLL_COUNT * 6), multiplier: 5, label: "Excellent!" },     // e.g., 3 rolls: 16-18
    { min: (LADDER_ROLL_COUNT * 4 + 1), max: (LADDER_ROLL_COUNT * 5), multiplier: 3, label: "Great!" },        // e.g., 3 rolls: 13-15
    { min: (LADDER_ROLL_COUNT * 3 + 1), max: (LADDER_ROLL_COUNT * 4), multiplier: 1, label: "Good." },         // e.g., 3 rolls: 10-12
    { min: (LADDER_ROLL_COUNT * 2 + 1), max: (LADDER_ROLL_COUNT * 3), multiplier: 0, label: "Okay." },         // e.g., 3 rolls: 7-9 (Push)
    { min: (LADDER_ROLL_COUNT * 1), max: (LADDER_ROLL_COUNT * 2), multiplier: -1, label: "Unlucky."}       // e.g., 3 rolls: 3-6 (Lose)
];

// --- CONSTANTS FOR SEVENS OUT ---
// No specific constants here other than dice outcomes

// --- CONSTANTS FOR SLOT FRUIT FRENZY ---
// Value 64 is usually jackpot (e.g., 777)
// Other values map to combinations. This is a simplified example.
const SLOT_PAYOUTS = {
    64: { multiplier: 50, symbols: "💎💎💎 JACKPOT!" }, // Triple Diamond (Example for value 64)
    1:  { multiplier: 20, symbols: "🔔🔔🔔" },        // Triple Bell (Example for value 1)
    22: { multiplier: 10, symbols: "🍊🍊🍊" },        // Triple Orange (Example for value 22)
    43: { multiplier: 5,  symbols: "🍋🍋🍋" },        // Triple Lemon (Example for value 43)
    // Add more mappings as needed. Most of the 64 values will be losses.
};
const SLOT_DEFAULT_LOSS_MULTIPLIER = -1;


// --- Main Message Handler (`bot.on('message')`) ---
bot.on('message', async (msg) => {
    if (isShuttingDown) { console.log("[MSG_HANDLER] Shutdown, ignoring message."); return; }
    if (msg && msg.from && msg.chat) { /* console.log raw msg */ } else { return; }
    if (!msg.from) { return; }
    if (msg.from.is_bot) {
        try {
            if (!bot || typeof bot.getMe !== 'function') { return; }
            const selfBotInfo = await bot.getMe();
            if (String(msg.from.id) !== String(selfBotInfo.id)) { return; }
        } catch (getMeError) { return; }
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text || "";
    const chatType = msg.chat.type;
    const messageId = msg.message_id;
    let userForCommandProcessing;

    if (text.startsWith('/')) {
        try {
            userForCommandProcessing = await getUser(userId, msg.from.username, msg.from.first_name);
            if (!userForCommandProcessing) {
                await safeSendMessage(chatId, "Sorry, error accessing your user data."); return;
            }
        } catch (e) {
            await safeSendMessage(chatId, "Sorry, problem accessing user data. Try again."); return;
        }
        const now = Date.now();
        if (userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
            console.log(`[COOLDOWN] User ${userId} command ("${text}") ignored.`); return;
        }
        userCooldowns.set(userId, now);
    }

    if (text.startsWith('/') && !msg.from.is_bot) {
        const commandArgs = text.substring(1).split(' ');
        const commandName = commandArgs.shift()?.toLowerCase();
        console.log(`[CMD RCV] Chat: ${chatId}, User: ${userId}, Cmd: /${commandName}, Args: [${commandArgs.join(', ')}]`);

        switch (commandName) {
            case 'start':
            case 'help':
                await handleHelpCommand(chatId, msg.from);
                break;
            case 'balance':
            case 'bal':
                await handleBalanceCommand(chatId, msg.from);
                break;
            case 'startcoinflip':
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    await handleStartGroupCoinFlipCommand(chatId, userForCommandProcessing, bet, messageId);
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            case 'startrps':
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    await handleStartGroupRPSCommand(chatId, userForCommandProcessing, bet, messageId);
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            case 'startdice': // Dice Escalator
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    if (typeof handleStartDiceEscalatorCommand === 'function') await handleStartDiceEscalatorCommand(chatId, userForCommandProcessing, bet, messageId);
                    else { await safeSendMessage(chatId, "Dice Escalator unavailable."); }
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            case 'jackpot':
                await handleJackpotCommand(chatId);
                break;
            case 'dice21':
            case 'd21':
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    if (typeof handleStartDice21Command === 'function') await handleStartDice21Command(chatId, userForCommandProcessing, bet, messageId);
                    else { await safeSendMessage(chatId, "Dice 21 unavailable."); }
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            case 'ou7':
            case 'overunder7':
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    if (typeof handleStartOverUnder7Command === 'function') await handleStartOverUnder7Command(chatId, userForCommandProcessing, bet, messageId);
                    else { await safeSendMessage(chatId, "Over/Under 7 unavailable."); }
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            case 'duel':
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    if (typeof handleStartDuelCommand === 'function') await handleStartDuelCommand(chatId, userForCommandProcessing, bet, messageId);
                    else { await safeSendMessage(chatId, "Duel game unavailable."); }
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            case 'ladder':
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    if (typeof handleStartLadderCommand === 'function') await handleStartLadderCommand(chatId, userForCommandProcessing, bet, messageId);
                    else { await safeSendMessage(chatId, "Ladder game unavailable."); }
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            case 'sevenout':
            case 's7':
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    if (typeof handleStartSevenOutCommand === 'function') await handleStartSevenOutCommand(chatId, userForCommandProcessing, bet, messageId);
                    else { await safeSendMessage(chatId, "Sevens Out unavailable."); }
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            case 'slot':
            case 'slots':
                if (chatType !== 'private') {
                    let bet = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(bet) || bet < MIN_BET_AMOUNT || bet > MAX_BET_AMOUNT) { /* ... */ return; }
                    if (typeof handleStartSlotCommand === 'function') await handleStartSlotCommand(chatId, userForCommandProcessing, bet, messageId);
                    else { await safeSendMessage(chatId, "Slots game unavailable."); }
                } else { await safeSendMessage(chatId, "Group chats only.", {}); }
                break;
            default:
                if (chatType === 'private' || text.startsWith('/')) {
                    await safeSendMessage(chatId, "Unknown command. Try /help.", {});
                }
        }
    }
});

// --- Callback Query Handler (`bot.on('callback_query')`) ---
bot.on('callback_query', async (callbackQuery) => {
    if (isShuttingDown) { /* ... */ return; }
    const msg = callbackQuery.message;
    const userFromCb = callbackQuery.from;
    const callbackQueryId = callbackQuery.id;
    const data = callbackQuery.data;
    if (!msg || !userFromCb || !data) { /* ... */ return; }
    const userId = String(userFromCb.id);
    const chatId = String(msg.chat.id);
    const originalMessageId = msg.message_id;
    console.log(`[CBQ RCV] Chat: ${chatId}, User: ${userId}, Data: "${data}"`);
    if (bot) bot.answerCallbackQuery(callbackQueryId).catch(()=>{});

    let userObjectForCallback;
    try {
        userObjectForCallback = await getUser(userId, userFromCb.username, userFromCb.first_name);
        if (!userObjectForCallback) throw new Error("User data N/A for callback.");
    } catch(e) { /* ... error handling ... */ return; }

    const [action, ...params] = data.split(':');

    try {
        switch (action) {
            case 'join_game': /* Coinflip/RPS */
                if (!params[0]) throw new Error("Missing gameId for join_game.");
                await handleJoinGameCallback(chatId, userObjectForCallback, params[0], originalMessageId);
                break;
            case 'cancel_game': /* Coinflip/RPS */
                if (!params[0]) throw new Error("Missing gameId for cancel_game.");
                await handleCancelGameCallback(chatId, userObjectForCallback, params[0], originalMessageId);
                break;
            case 'rps_choose':
                if (params.length < 2) throw new Error("Missing params for rps_choose.");
                await handleRPSChoiceCallback(chatId, userObjectForCallback, params[0], params[1], originalMessageId);
                break;
            case 'de_roll_prompt': /* Dice Escalator */
                if (!params[0]) throw new Error("Missing gameId for de_roll_prompt.");
                if (typeof handleDiceEscalatorPlayerAction === 'function') await handleDiceEscalatorPlayerAction(params[0], userId, action, originalMessageId, chatId);
                else throw new Error("Dice Escalator action handler N/A.");
                break;
            case 'de_cashout': /* Dice Escalator Stand */
                if (!params[0]) throw new Error("Missing gameId for de_cashout.");
                if (typeof handleDiceEscalatorPlayerAction === 'function') await handleDiceEscalatorPlayerAction(params[0], userId, action, originalMessageId, chatId);
                else throw new Error("Dice Escalator action handler N/A.");
                break;
            case 'play_again_de': /* Dice Escalator */
                if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_de.");
                if (bot) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartDiceEscalatorCommand === 'function') await handleStartDiceEscalatorCommand(chatId, userObjectForCallback, parseInt(params[0], 10), originalMessageId);
                else throw new Error("Dice Escalator start handler N/A.");
                break;
            case 'd21_hit': /* Dice 21 */
                if (!params[0]) throw new Error("Missing gameId for d21_hit.");
                if (typeof handleDice21Hit === 'function') await handleDice21Hit(params[0], userObjectForCallback, originalMessageId);
                else throw new Error("Dice 21 hit handler N/A.");
                break;
            case 'd21_stand': /* Dice 21 */
                if (!params[0]) throw new Error("Missing gameId for d21_stand.");
                if (typeof handleDice21Stand === 'function') await handleDice21Stand(params[0], userObjectForCallback, originalMessageId);
                else throw new Error("Dice 21 stand handler N/A.");
                break;
            case 'play_again_d21': /* Dice 21 */
                if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_d21.");
                if (bot) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartDice21Command === 'function') await handleStartDice21Command(chatId, userObjectForCallback, parseInt(params[0], 10), originalMessageId);
                else throw new Error("Dice 21 start handler N/A.");
                break;
            case 'ou7_choice': /* Over/Under 7 */
                if (params.length < 2) throw new Error("Missing params for ou7_choice (gameId:choice)"); // gameId is params[0], choice is params[1]
                if (typeof handleOverUnder7Choice === 'function') await handleOverUnder7Choice(params[0], params[1], userObjectForCallback, originalMessageId);
                else throw new Error("Over/Under 7 choice handler N/A.");
                break;
            case 'play_again_ou7': /* Over/Under 7 */
                if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_ou7.");
                if (bot) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartOverUnder7Command === 'function') await handleStartOverUnder7Command(chatId, userObjectForCallback, parseInt(params[0], 10), originalMessageId);
                else throw new Error("Over/Under 7 start handler N/A.");
                break;
            case 'duel_roll': /* High Roller Duel */
                 if (!params[0]) throw new Error("Missing gameId for duel_roll.");
                 if (typeof handleDuelRoll === 'function') await handleDuelRoll(params[0], userObjectForCallback, originalMessageId);
                 else throw new Error("Duel roll handler N/A.");
                 break;
            case 'play_again_duel': /* High Roller Duel */
                if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_duel.");
                if (bot) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartDuelCommand === 'function') await handleStartDuelCommand(chatId, userObjectForCallback, parseInt(params[0], 10), originalMessageId);
                else throw new Error("Duel start handler N/A.");
                break;
            case 'play_again_ladder': /* Greed's Ladder */
                if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_ladder.");
                if (bot) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartLadderCommand === 'function') await handleStartLadderCommand(chatId, userObjectForCallback, parseInt(params[0], 10), originalMessageId);
                else throw new Error("Ladder start handler N/A.");
                break;
            case 's7_roll': /* Sevens Out */
                if (!params[0]) throw new Error("Missing gameId for s7_roll.");
                if (typeof handleSevenOutRoll === 'function') await handleSevenOutRoll(params[0], userObjectForCallback, originalMessageId);
                else throw new Error("Sevens Out roll handler N/A.");
                break;
            case 'play_again_s7': /* Sevens Out */
                if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_s7.");
                if (bot) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartSevenOutCommand === 'function') await handleStartSevenOutCommand(chatId, userObjectForCallback, parseInt(params[0], 10), originalMessageId);
                else throw new Error("Sevens Out start handler N/A.");
                break;
            case 'play_again_slot': /* Slot Fruit Frenzy */
                if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_slot.");
                if (bot) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartSlotCommand === 'function') await handleStartSlotCommand(chatId, userObjectForCallback, parseInt(params[0], 10), originalMessageId);
                else throw new Error("Slot start handler N/A.");
                break;
            default:
                console.log(`[CBQ_INFO] Unknown callback: "${action}" Data: ${data}`);
        }
    } catch (error) {
        console.error(`[CBQ_ERROR] Processing callback "${data}" for ${userId} in ${chatId}:`, error);
        await safeSendMessage(userId, "Sorry, an error occurred. Please try again.", {}).catch(() => {});
    }
});

// --- Command Handler Functions ---

async function handleHelpCommand(chatId, userFromMessage) {
    const userMention = typeof createUserMention === 'function' ? createUserMention(userFromMessage) : (userFromMessage.first_name || `User ${userFromMessage.id}`);
    const jackpotScoreInfo = typeof TARGET_JACKPOT_SCORE !== 'undefined' ? TARGET_JACKPOT_SCORE : 'a high';
    const helpTextParts = [
        `👋 Hello ${userMention}\\! Welcome to the Casino Bot v${BOT_VERSION}\\.`,
        `\n*Available commands:*`,
        `▫️ \`/help\` \\- This help message\\.`,
        `▫️ \`/balance\` or \`/bal\` \\- Check your credits\\.`,
        `▫️ \`/startcoinflip <bet>\` \\- Coinflip game\\. Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/startrps <bet>\` \\- Rock Paper Scissors\\. Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/startdice <bet>\` \\- Dice Escalator vs Bot\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/dice21 <bet>\` or \`/d21 <bet>\` \\- Dice 21 (Blackjack) vs Bot\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/ou7 <bet>\` or \`/overunder7 <bet>\` \\- Over/Under 7 (2 dice)\\. Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/duel <bet>\` \\- High Roller Dice Duel vs Bot\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/ladder <bet>\` \\- Greed's Ladder (3 rolls)\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/sevenout <bet>\` or \`/s7 <bet>\` \\- Sevens Out (Craps style)\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/slot <bet>\` or \`/slots <bet>\` \\- Slot Machine\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/jackpot\` \\- Current Dice Escalator jackpot\\. Win by standing with score *${jackpotScoreInfo}+* & beating Bot in Dice Escalator\\!`,
        `\n*Game Notes:*`,
        `➡️ Coinflip/RPS: Needs an opponent to join\\.`,
        `➡️ Dice Escalator: Roll until you Stand or Bust\\. Bot plays if you Stand\\.`,
        `➡️ Dice 21: Try to get up to 21\\. Bot plays after you Stand\\.`,
        `\nHave fun and play responsibly\\!`
    ];
    await safeSendMessage(chatId, helpTextParts.filter(Boolean).join('\n'), { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

async function handleBalanceCommand(chatId, userFromMessage) {
    const user = await getUser(String(userFromMessage.id), userFromMessage.username, userFromMessage.first_name);
    if (!user) { await safeSendMessage(chatId, "Could not retrieve your balance.", {}); return; }
    const balanceMessage = `${createUserMention(userFromMessage)}, your balance is: *${formatCurrency(user.balance)}*\\.`;
    await safeSendMessage(chatId, balanceMessage, { parse_mode: 'MarkdownV2' });
}

async function handleJackpotCommand(chatId) {
    try {
        if (typeof queryDatabase !== 'function' || typeof MAIN_JACKPOT_ID === 'undefined' || typeof TARGET_JACKPOT_SCORE === 'undefined' || typeof formatCurrency !== 'function' || typeof escapeMarkdownV2 !== 'function') {
            await safeSendMessage(chatId, "Jackpot system unavailable (config error).", {}); return;
        }
        const result = await queryDatabase('SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]);
        if (result.rows.length > 0) {
            const jackpotAmountLamports = BigInt(result.rows[0].current_amount_lamports);
            const jackpotDisplay = formatCurrency(Number(jackpotAmountLamports));
            await safeSendMessage(chatId, `💰 Current Dice Escalator Jackpot: *${escapeMarkdownV2(jackpotDisplay)}*!\nWin by Standing with score *${TARGET_JACKPOT_SCORE}+* & beating Bot!`, { parse_mode: 'MarkdownV2' });
        } else { await safeSendMessage(chatId, "Jackpot info unavailable.", {}); }
    } catch (error) { await safeSendMessage(chatId, "Sorry, error fetching jackpot.", {}); }
}

// --- Existing Group Game Flow Functions (Coinflip, RPS - FULLY INTEGRATED) ---
// (Includes updates to use fetched user objects and proper updateUserBalance calls)

async function handleStartGroupCoinFlipCommand(chatId, initiatorUserObj, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUserObj.id);
    let chatInfo = null; try { if (bot) chatInfo = await bot.getChat(chatId); } catch (e) { console.warn(`[COINFLIP_START_WARN] No chat info ${chatId}: ${e.message}`); }
    const chatTitle = chatInfo?.title;
    const gameSession = await getGroupSession(chatId, chatTitle || "Group Chat"); 
    if (gameSession.currentGameId && !['DiceEscalator', 'Dice21', 'OverUnder7', 'Duel', 'Ladder', 'SevenOut', 'Slot'].includes(gameSession.currentGameType) ) { 
        await safeSendMessage(chatId, `A ${escapeMarkdownV2(gameSession.currentGameType || 'game')} is active. Wait.`, { parse_mode: 'MarkdownV2' }); return;
    }
    const initiator = initiatorUserObj; 
    if (initiator.balance < betAmount) { 
        await safeSendMessage(chatId, `${createUserMention(initiatorUserObj)}, bal (${formatCurrency(initiator.balance)}) too low for ${formatCurrency(betAmount)} bet.`, { parse_mode: 'MarkdownV2' }); return;
    }
    const gameId = generateGameId(); 
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_coinflip_init:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) { /* ... */ return; }
    const gameDataCF = {
        type: 'coinflip', gameId, chatId: String(chatId), initiatorId,
        initiatorMention: createUserMention(initiatorUserObj), betAmount,
        participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUserObj), betPlaced: true }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
    };
    activeGames.set(gameId, gameDataCF); 
    await updateGroupGameDetails(chatId, gameId, 'CoinFlip', betAmount); 
    const joinMsgCF = `${createUserMention(initiatorUserObj)} started *Coin Flip* for ${escapeMarkdownV2(formatCurrency(betAmount))}! Join!`;
    const kbCF = { inline_keyboard: [[{ text: "🪙 Join Coinflip!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]] };
    const setupMsgCF = await safeSendMessage(chatId, joinMsgCF, { parse_mode: 'MarkdownV2', reply_markup: kbCF });
    if (setupMsgCF && activeGames.has(gameId)) { activeGames.get(gameId).gameSetupMessageId = setupMsgCF.message_id; } 
    else { /* ... refund ... */ 
        await updateUserBalance(initiatorId, betAmount, `refund_coinflip_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null); return;
    }
    setTimeout(async () => { /* ... timeout logic ... */ 
        const gdCF = activeGames.get(gameId);
        if (gdCF && gdCF.status === 'waiting_opponent') {
            await updateUserBalance(gdCF.initiatorId, gdCF.betAmount, `refund_coinflip_timeout:${gameId}`, null, gameId, String(chatId));
            /* ... delete game, update message ... */
            activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgTextCF = `Coinflip by ${gdCF.initiatorMention} expired. Bet refunded.`;
            if (gdCF.gameSetupMessageId && bot) { bot.editMessageText(timeoutMsgTextCF, { chatId: String(chatId), message_id: Number(gdCF.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>{safeSendMessage(chatId, timeoutMsgTextCF, {parse_mode: 'MarkdownV2'});}); }
            else { safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' }); }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleStartGroupRPSCommand(chatId, initiatorUserObj, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUserObj.id);
    let chatInfo = null; try { if (bot) chatInfo = await bot.getChat(chatId); } catch (e) { /* ... */ }
    const chatTitle = chatInfo?.title;
    const gameSession = await getGroupSession(chatId, chatTitle || "Group Chat");
    if (gameSession.currentGameId && !['DiceEscalator', 'Dice21', 'OverUnder7', 'Duel', 'Ladder', 'SevenOut', 'Slot'].includes(gameSession.currentGameType) ) { /* ... */ return; }
    const initiator = initiatorUserObj;
    if (initiator.balance < betAmount) { /* ... */ return; }
    const gameId = generateGameId();
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_rps_init:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) { /* ... */ return; }
    const gameDataRPS = { 
        type: 'rps', gameId, chatId: String(chatId), initiatorId, initiatorMention: createUserMention(initiatorUserObj),
        betAmount, participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUserObj), betPlaced: true }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
    };
    activeGames.set(gameId, gameDataRPS);
    await updateGroupGameDetails(chatId, gameId, 'RockPaperScissors', betAmount);
    const joinMsgRPS = `${createUserMention(initiatorUserObj)} challenges for *RPS* (${escapeMarkdownV2(formatCurrency(betAmount))})! Join?`;
    const kbRPS = { inline_keyboard: [[{ text: "🪨📄✂️ Join RPS!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]] };
    const setupMsgRPS = await safeSendMessage(chatId, joinMsgRPS, { parse_mode: 'MarkdownV2', reply_markup: kbRPS });
    if (setupMsgRPS && activeGames.has(gameId)) { activeGames.get(gameId).gameSetupMessageId = setupMsgRPS.message_id; }
    else { /* ... refund ... */ 
        await updateUserBalance(initiatorId, betAmount, `refund_rps_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null); return;
    }
    setTimeout(async () => { /* ... timeout logic ... */ 
        const gdRPS = activeGames.get(gameId);
        if (gdRPS && gdRPS.status === 'waiting_opponent') {
            await updateUserBalance(gdRPS.initiatorId, gdRPS.betAmount, `refund_rps_timeout:${gameId}`, null, gameId, String(chatId));
            activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgTextRPS = `RPS by ${gdRPS.initiatorMention} expired. Refunded.`;
            if (gdRPS.gameSetupMessageId && bot) { bot.editMessageText(timeoutMsgTextRPS, { chatId: String(chatId), message_id: Number(gdRPS.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>{safeSendMessage(chatId, timeoutMsgTextRPS, {parse_mode:'MarkdownV2'});}); }
            else { safeSendMessage(chatId, timeoutMsgTextRPS, { parse_mode: 'MarkdownV2' }); }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleJoinGameCallback(chatId, joinerUserObj, gameId, interactionMessageId) {
    const joinerId = String(joinerUserObj.id);
    const gameData = activeGames.get(gameId);
    if (!gameData) { await safeSendMessage(joinerId, "Game not available.", {}); if (interactionMessageId && bot) bot.editMessageReplyMarkup({}, {chat_id:chatId, message_id:interactionMessageId}).catch(()=>{}); return; }
    if (gameData.chatId !== String(chatId)) { await safeSendMessage(joinerId, "Join error (chat mismatch).", {}); return; }
    if (gameData.initiatorId === joinerId) { await safeSendMessage(joinerId, "Can't join own game.", {}); return; }
    if (gameData.status !== 'waiting_opponent') { await safeSendMessage(joinerId, "Game not waiting.", {}); if (interactionMessageId && bot) bot.editMessageReplyMarkup({}, {chat_id:chatId, message_id:interactionMessageId}).catch(()=>{}); return; }
    if (gameData.participants.length >= 2 && (gameData.type === 'coinflip' || gameData.type === 'rps')) { await safeSendMessage(joinerId, "Game full.", {}); if (interactionMessageId && bot) bot.editMessageReplyMarkup({}, {chat_id:chatId, message_id:interactionMessageId}).catch(()=>{}); return; }

    if (joinerUserObj.balance < gameData.betAmount) { /* ... insufficient balance ... */ return; }
    const balanceUpdateResult = await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_group_${gameData.type}_join:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) { /* ... handle bet placement error ... */ return; }
    gameData.participants.push({ userId: joinerId, choice: null, mention: createUserMention(joinerUserObj), betPlaced: true });
    activeGames.set(gameId, gameData);
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);

    if (gameData.type === 'coinflip' && gameData.participants.length === 2) {
        gameData.status = 'resolving'; const p1 = gameData.participants[0]; const p2 = gameData.participants[1]; p1.choice = 'heads'; p2.choice = 'tails';
        const cfResult = determineCoinFlipOutcome(); let winner = (cfResult.outcome === p1.choice) ? p1 : p2;
        const winningsToCredit = BigInt(gameData.betAmount) + BigInt(gameData.betAmount); 
        await updateUserBalance(winner.userId, winningsToCredit, `won_group_coinflip:${gameId}`, null, gameId, String(chatId));
        const resMsg = `*CoinFlip Resolved!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n${p1.mention} (H) vs ${p2.mention} (T)\nLanded: *${escapeMarkdownV2(cfResult.outcomeString)}* ${cfResult.emoji}!\n🎉 ${winner.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;
        if (messageToEditId && bot) { bot.editMessageText(resMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(() => { safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' }); });}
        else { safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' }); }
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
    } else if (gameData.type === 'rps' && gameData.participants.length === 2) {
        gameData.status = 'waiting_choices';
        const rpsPrompt = `${gameData.participants[0].mention} & ${gameData.participants[1].mention}, RPS for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} is set! Make your choice:`;
        const rpsKeyboard = { inline_keyboard: [[ /* ... RPS buttons ... */ 
            { text: `${RPS_EMOJIS.ROCK} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
            { text: `${RPS_EMOJIS.PAPER} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
            { text: `${RPS_EMOJIS.SCISSORS} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
        ]] };
        if (messageToEditId && bot) { bot.editMessageText(rpsPrompt, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }).catch(() => { safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); });}
        else { safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); }
        activeGames.set(gameId, gameData);
    }
}

async function handleCancelGameCallback(chatId, cancellerUserObj, gameId, interactionMessageId) {
    const cancellerId = String(cancellerUserObj.id); 
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== String(chatId) || gameData.initiatorId !== cancellerId ) { /* ... */ return; }
    const cancellableStatuses = ['waiting_opponent', 'waiting_choices']; if (!cancellableStatuses.includes(gameData.status)) { /* ... */ return; }
    for (const p of gameData.participants) { if (p.betPlaced) { await updateUserBalance(p.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, null, gameId, String(chatId)); }}
    const gameTypeDisplay = gameData.type.charAt(0).toUpperCase() + gameData.type.slice(1);
    const cancellationMessage = `${gameData.initiatorMention} cancelled ${escapeMarkdownV2(gameTypeDisplay)} (Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}). Bets refunded.`;
    const msgToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (msgToEdit && bot) { bot.editMessageText(cancellationMessage, {chatId: String(chatId), message_id: msgToEdit, parse_mode:'MarkdownV2', reply_markup:{}}).catch(()=>{safeSendMessage(chatId, cancellationMessage, {parse_mode:'MarkdownV2'});});}
    else { safeSendMessage(chatId, cancellationMessage, {parse_mode:'MarkdownV2'});}
    activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
}

async function handleRPSChoiceCallback(chatId, userChoiceObj, gameId, choice, interactionMessageId) {
    const userId = String(userChoiceObj.id); 
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== String(chatId) || gameData.type !== 'rps' || gameData.status !== 'waiting_choices') { /* ... */ return; }
    const participant = gameData.participants.find(p => p.userId === userId); if (!participant || participant.choice) { /* ... */ return; }
    const normalizedChoiceKey = String(choice).toUpperCase(); if (!RPS_CHOICES[normalizedChoiceKey]) { /* ... */ return; }
    participant.choice = RPS_CHOICES[normalizedChoiceKey];
    await safeSendMessage(userId, `You chose ${RPS_EMOJIS[participant.choice]}! Waiting...`, { parse_mode: 'MarkdownV2' });
    activeGames.set(gameId, gameData);
    const otherPlayer = gameData.participants.find(p => p.userId !== userId);
    let groupUpdateMsg = `${participant.mention} made their choice!`;
    let kbUpd = {}; 
    if (otherPlayer && !otherPlayer.choice) { groupUpdateMsg += ` Waiting for ${otherPlayer.mention}...`; kbUpd = { inline_keyboard: [[ /* RPS Buttons */ 
        { text: `${RPS_EMOJIS.ROCK} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
        { text: `${RPS_EMOJIS.PAPER} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
        { text: `${RPS_EMOJIS.SCISSORS} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
    ]] }; }
    const msgToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (msgToEdit && bot) { bot.editMessageText(groupUpdateMsg, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: kbUpd }).catch(()=>{}); }

    const allChosen = gameData.participants.length === 2 && gameData.participants.every(p => p.choice);
    if (allChosen) { 
        gameData.status = 'game_over';
        const p1 = gameData.participants[0]; const p2 = gameData.participants[1]; const rpsRes = determineRPSOutcome(p1.choice, p2.choice);
        let winnerP = null; let resTxt = `*RPS Result!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n${p1.mention}: ${RPS_EMOJIS[p1.choice]} vs ${p2.mention}: ${RPS_EMOJIS[p2.choice]}\n${escapeMarkdownV2(rpsRes.description)}\n`;
        if (rpsRes.result === 'win1') winnerP = p1; else if (rpsRes.result === 'win2') winnerP = p2;
        if (winnerP) { const wins = BigInt(gameData.betAmount) + BigInt(gameData.betAmount); await updateUserBalance(winnerP.userId, wins, `won_rps:${gameId}`, null, gameId, String(chatId)); resTxt += `🎉 ${winnerP.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`; }
        else if (rpsRes.result === 'draw') { await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, null, gameId, String(chatId)); await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, null, gameId, String(chatId)); resTxt += `Draw! Bets refunded.`; }
        else { /* error */ await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_error:${gameId}`, null, gameId, String(chatId)); await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_error:${gameId}`, null, gameId, String(chatId)); resTxt = `Error. Bets refunded.`; }
        if (msgToEdit && bot) { bot.editMessageText(resTxt, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(() => { safeSendMessage(chatId, resTxt, { parse_mode: 'MarkdownV2' }); });}
        else { safeSendMessage(chatId, resTxt, { parse_mode: 'MarkdownV2' }); }
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
    }
}

// --- DICE 21 GAME LOGIC (Full Implementation with Casino Feel) ---
// This section is intended to replace/update your existing Dice 21 handlers in Part 5a.
// Ensure `stringifyWithBigInt` is defined ONCE globally in your project (e.g., in Part 1).

// Ensure these constants are defined, typically in Part 1 or a global constants file
// const DICE_21_TARGET_SCORE = 21;
// const DICE_21_BOT_STAND_SCORE = 17; // Bot stands on 17 or more

console.log("Initializing Dice 21 Game Logic with Casino Feel (ensure stringifyWithBigInt is globally defined once)...");

async function handleStartDice21Command(chatId, userObj, betAmount, commandMessageId) {
    const LOG_PREFIX = "[Dice21_Start]";
    console.log(`${LOG_PREFIX} Initializing new game. ChatID: ${chatId}, User: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(userObj) : JSON.stringify(userObj)}, Bet: ${betAmount}, CmdMsgID: ${commandMessageId}`);

    if (!userObj || typeof userObj.userId === 'undefined') {
        console.error(`${LOG_PREFIX} CRITICAL: User object or userId is undefined. userObj: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(userObj) : JSON.stringify(userObj)}`);
        await safeSendMessage(chatId, "There was an issue accessing your player profile. Please try again.");
        return;
    }

    const userId = String(userObj.userId);
    const playerRef = getPlayerDisplayReference(userObj);
    const gameId = generateGameId();

    console.log(`${LOG_PREFIX} User ${userId} (${playerRef}) starting Dice 21. Wager: ${betAmount}, GameID: ${gameId}`);

    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_dice21:${gameId}`, null, gameId, String(chatId));
    console.log(`${LOG_PREFIX} Wager placement result for ${userId}: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(balanceUpdateResult) : JSON.stringify(balanceUpdateResult)}`);

    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        const errMsg = balanceUpdateResult ? balanceUpdateResult.error : "could not be processed";
        console.error(`${LOG_PREFIX} Wager placement failed for ${userId}. Reason: ${errMsg}`);
        await safeSendMessage(chatId, `${playerRef}, your wager of ${escapeMarkdownV2(formatCurrency(betAmount))} for Dice 21 ${escapeMarkdownV2(errMsg)}. Please check your balance.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX} Wager ${formatCurrency(betAmount)} accepted for ${userId}. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

    await safeSendMessage(chatId, `🎲 Welcome to the Dice 21 table, ${playerRef}! 🎲\nYour wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* has been placed. Let's deal the initial dice...`, { parse_mode: 'MarkdownV2' });
    await sleep(1500);

    let initialRollsValues = [];
    let playerScore = 0n;
    const diceToDeal = 2;

    console.log(`${LOG_PREFIX} Dealing ${diceToDeal} dice to player ${userId} for game ${gameId}.`);
    for (let i = 0; i < diceToDeal; i++) {
        try {
            console.log(`${LOG_PREFIX} Sending die ${i + 1}/${diceToDeal} for game ${gameId}.`);
            const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
            if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') {
                throw new Error("Invalid dice message from Telegram API.");
            }
            initialRollsValues.push(diceMsg.dice.value);
            playerScore += BigInt(diceMsg.dice.value);
            console.log(`${LOG_PREFIX} Die ${i + 1} for game ${gameId} was ${diceMsg.dice.value}. Player score now ${playerScore}. Pausing.`);
            await sleep(2000); // Pause for animation
        } catch (diceError) {
            console.error(`${LOG_PREFIX} Error sending animated die ${i + 1} for game ${gameId}: ${diceError.message}. Using internal roll.`, diceError);
            const internalRoll = rollDie();
            initialRollsValues.push(internalRoll);
            playerScore += BigInt(internalRoll);
            await safeSendMessage(chatId, `${playerRef}, an internal roll was used for one of your dice: it's a *${internalRoll}*!`);
            console.log(`${LOG_PREFIX} Internal die ${i + 1} for game ${gameId} was ${internalRoll}. Player score now ${playerScore}. Pausing.`);
            await sleep(1000);
        }
    }

    const gameData = {
        type: 'dice21', gameId, chatId: String(chatId), userId, playerRef,
        betAmount: BigInt(betAmount), playerScore, botScore: 0n, playerInitialDeal: [...initialRollsValues], playerHits: [],
        status: 'player_turn', gameMessageId: null
    };
    console.log(`${LOG_PREFIX} Initial deal complete for game ${gameId}. Player score: ${playerScore}. Rolls: ${initialRollsValues.join(', ')}`);

    let messageText = `${playerRef}, your initial hand: ${formatDiceRolls(initialRollsValues)}, totaling *${escapeMarkdownV2(String(playerScore))}*.`;
    let buttons = [];
    let gameMessageOptions = { parse_mode: 'MarkdownV2' };

    // Ensure DICE_21_TARGET_SCORE is defined and accessible here
    // const targetScore = typeof DICE_21_TARGET_SCORE !== 'undefined' ? BigInt(DICE_21_TARGET_SCORE) : 21n;


    if (playerScore > targetScore) {
        messageText += `\n\n💥 Oh dear, that's a BUST! Over ${targetScore}. The house takes the wager of ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}.`;
        gameData.status = 'game_over_player_bust';
        console.log(`${LOG_PREFIX} Player ${userId} busted on initial deal for game ${gameId}.`);
        await updateUserBalance(userId, 0n, `lost_dice21_deal_bust:${gameId}`, null, gameId, String(chatId)); // Log loss
        buttons.push({ text: `🎲 Play Dice 21 Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_d21:${betAmount}` });
    } else if (playerScore === targetScore) {
        messageText += `\n\n✨ Blackjack! A perfect ${targetScore}! You automatically stand. Now, let's see the Bot Dealer's hand...`;
        gameData.status = 'bot_turn'; // Player stands on Blackjack
        console.log(`${LOG_PREFIX} Player ${userId} hit Blackjack on initial deal for game ${gameId}.`);
    } else {
        messageText += `\n\nYour move, ${playerRef}. Will you "Hit" for another die or "Stand"?`;
        buttons.push({ text: "⤵️ Hit", callback_data: `d21_hit:${gameId}` });
        buttons.push({ text: `✅ Stand`, callback_data: `d21_stand:${gameId}` });
    }

    if (buttons.length > 0) {
        gameMessageOptions.reply_markup = { inline_keyboard: [buttons] };
    }
    console.log(`${LOG_PREFIX} Composed initial game message for game ${gameId}: "${messageText}", Options: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(gameMessageOptions) : JSON.stringify(gameMessageOptions)}`);

    const sentMsg = await safeSendMessage(chatId, messageText, gameMessageOptions);
    if (sentMsg && sentMsg.message_id) {
        gameData.gameMessageId = sentMsg.message_id;
        console.log(`${LOG_PREFIX} Game message sent for ${gameId}, msgID: ${gameData.gameMessageId}.`);
    } else {
        console.error(`${LOG_PREFIX} Failed to send initial game message for ${gameId}. Attempting to refund.`);
        await updateUserBalance(userId, betAmount, `refund_dice21_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
        return;
    }

    activeGames.set(gameId, gameData);
    console.log(`${LOG_PREFIX} Game ${gameId} data stored: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(gameData) : JSON.stringify(gameData)}`);

    if (gameData.status === 'bot_turn') {
        console.log(`${LOG_PREFIX} Player Blackjack, proceeding to bot's turn for game ${gameId}.`);
        await sleep(2000);
        await processDice21BotTurn(gameData, gameData.gameMessageId);
    } else if (gameData.status.startsWith('game_over')) {
        console.log(`${LOG_PREFIX} Game ${gameId} ended on deal (e.g., player bust). Cleaning up.`);
        activeGames.delete(gameId);
    }
    console.log(`${LOG_PREFIX} Exiting handleStartDice21Command for game ${gameId}.`);
}

async function handleDice21Hit(gameId, userObj, originalMessageId) {
    const LOG_PREFIX = "[Dice21_Hit]";
    console.log(`${LOG_PREFIX} Player action 'Hit'. GameID: ${gameId}, User: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(userObj) : JSON.stringify(userObj)}, OrigMsgID: ${originalMessageId}`);

    const gameData = activeGames.get(gameId);
    if (!userObj || typeof userObj.userId === 'undefined') {
        console.error(`${LOG_PREFIX} CRITICAL: User object or userId is undefined. userObj: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(userObj) : JSON.stringify(userObj)}`);
        await safeSendMessage(originalMessageId && gameData ? gameData.chatId : (userObj ? userObj.id : null), "Error processing your action due to user profile issue.");
        return;
    }
    const userId = String(userObj.userId);

    if (!gameData) {
        console.warn(`${LOG_PREFIX} Game ${gameId} not found for user ${userId}.`);
        await safeSendMessage(userId, "That Dice 21 game could not be found. It might have timed out.", {});
        if (originalMessageId && bot && gameData && gameData.chatId) bot.editMessageReplyMarkup({}, { chat_id: gameData.chatId, message_id: originalMessageId }).catch(() => {});
        else if (originalMessageId && bot && userObj && userObj.chat && userObj.chat.id) bot.editMessageReplyMarkup({}, { chat_id: String(userObj.chat.id), message_id: originalMessageId }).catch(() => {}); // Fallback if gameData missing chatId
        return;
    }
    console.log(`${LOG_PREFIX} Game ${gameId} found for user ${userId}. Current status: ${gameData.status}`);

    if (gameData.userId !== userId || gameData.status !== 'player_turn') {
        console.warn(`${LOG_PREFIX} Invalid action for game ${gameId}. User ${userId} (expected ${gameData.userId}) or status ${gameData.status} (expected player_turn).`);
        await safeSendMessage(userId, "It's not your turn or this game is not active for a hit.", {});
        return;
    }
    if (Number(gameData.gameMessageId) !== Number(originalMessageId)) {
        console.warn(`${LOG_PREFIX} Hit action on stale message for game ${gameId}. GameMsgID: ${gameData.gameMessageId}, ActionMsgID: ${originalMessageId}.`);
        return;
    }

    const chatId = gameData.chatId;
    console.log(`${LOG_PREFIX} Player ${userId} hits in game ${gameId}. Current score: ${gameData.playerScore}.`);

    await bot.editMessageText(`${gameData.playerRef} takes another card (die)... a moment of suspense!\nYour current score: *${escapeMarkdownV2(String(gameData.playerScore))}*`, {
        chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {}
    }).catch(e => console.error(`${LOG_PREFIX} Error editing message to 'hitting': ${e.message}`, e));
    await sleep(1000);

    let newRoll;
    try {
        console.log(`${LOG_PREFIX} Sending die for hit in game ${gameId}.`);
        const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
         if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') {
            throw new Error("Invalid dice message from Telegram API on hit.");
        }
        newRoll = BigInt(diceMsg.dice.value);
        gameData.playerHits.push(newRoll);
        console.log(`${LOG_PREFIX} Player ${userId} hit a ${newRoll} (animated) for game ${gameId}. Pausing.`);
        await sleep(2000);
    } catch (e) {
        console.error(`${LOG_PREFIX} sendDice failed for hit in game ${gameId}: ${e.message}. Using internal roll.`, e);
        newRoll = BigInt(rollDie());
        gameData.playerHits.push(newRoll);
        await safeSendMessage(chatId, `${gameData.playerRef}, your internal roll for the hit is a *${newRoll}*!`);
        console.log(`${LOG_PREFIX} Player ${userId} hit a ${newRoll} (internal) for game ${gameId}. Pausing.`);
        await sleep(1000);
    }

    gameData.playerScore += newRoll;
    console.log(`${LOG_PREFIX} Player ${userId} new score in game ${gameId}: ${gameData.playerScore}.`);

    let messageText = `${gameData.playerRef}, you drew a ${formatDiceRolls([Number(newRoll)])}. Your hand is now ${formatDiceRolls([...gameData.playerInitialDeal, ...gameData.playerHits.map(Number)])}, totaling *${escapeMarkdownV2(String(gameData.playerScore))}*.`;
    let buttons = [];
    let gameEndedThisTurn = false;

    // Ensure DICE_21_TARGET_SCORE is defined and accessible here
    // const targetScore = typeof DICE_21_TARGET_SCORE !== 'undefined' ? BigInt(DICE_21_TARGET_SCORE) : 21n;

    if (gameData.playerScore > targetScore) {
        messageText += `\n\n💥 BUST! Your score is over ${targetScore}. The house claims your ${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount)))} wager.`;
        gameData.status = 'game_over_player_bust';
        gameEndedThisTurn = true;
        console.log(`${LOG_PREFIX} Player ${userId} busted after hit in game ${gameId}.`);
        await updateUserBalance(userId, 0n, `lost_dice21_hit_bust:${gameId}`, null, gameId, String(chatId));
        buttons.push({ text: `🎲 Play Dice 21 Again (${formatCurrency(Number(gameData.betAmount))})`, callback_data: `play_again_d21:${gameData.betAmount}` });
    } else if (gameData.playerScore === targetScore) {
        messageText += `\n\n✨ A perfect ${targetScore}! You stand. The Bot Dealer will now play.`;
        gameData.status = 'bot_turn';
        gameEndedThisTurn = true;
        console.log(`${LOG_PREFIX} Player ${userId} reached ${targetScore} after hit in game ${gameId}.`);
    } else {
        messageText += `\n\nWhat's your next move, ${gameData.playerRef}? "Hit" or "Stand"?`;
        buttons.push({ text: "⤵️ Hit", callback_data: `d21_hit:${gameId}` });
        buttons.push({ text: `✅ Stand`, callback_data: `d21_stand:${gameId}` });
    }

    const gameMessageOptions = { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2' };
    if (buttons.length > 0) {
        gameMessageOptions.reply_markup = { inline_keyboard: [buttons] };
    }
    console.log(`${LOG_PREFIX} Composed message after hit for game ${gameId}: "${messageText}", Options: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(gameMessageOptions) : JSON.stringify(gameMessageOptions)}`);
    await bot.editMessageText(messageText, gameMessageOptions)
             .catch(e => console.error(`${LOG_PREFIX} Failed to edit message after hit for game ${gameId}: ${e.message}`, e));

    activeGames.set(gameId, gameData);

    if (gameEndedThisTurn) {
        if (gameData.status === 'bot_turn') {
            console.log(`${LOG_PREFIX} Player reached ${targetScore}, proceeding to bot's turn for game ${gameId}.`);
            await sleep(2000);
            await processDice21BotTurn(gameData, originalMessageId);
        } else if (gameData.status.startsWith('game_over')) {
            console.log(`${LOG_PREFIX} Game ${gameId} ended after hit (e.g. player bust). Cleaning up.`);
            activeGames.delete(gameId);
        }
    }
    console.log(`${LOG_PREFIX} Exiting handleDice21Hit for game ${gameId}.`);
}

async function handleDice21Stand(gameId, userObj, originalMessageId) {
    const LOG_PREFIX = "[Dice21_Stand]";
    console.log(`${LOG_PREFIX} Player action 'Stand'. GameID: ${gameId}, User: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(userObj) : JSON.stringify(userObj)}, OrigMsgID: ${originalMessageId}`);

    const gameData = activeGames.get(gameId);
     if (!userObj || typeof userObj.userId === 'undefined') {
        console.error(`${LOG_PREFIX} CRITICAL: User object or userId is undefined.`);
        return;
    }
    const userId = String(userObj.userId);

    if (!gameData) {
        console.warn(`${LOG_PREFIX} Game ${gameId} not found for user ${userId}.`);
        await safeSendMessage(userId, "That Dice 21 game seems to have concluded or timed out.", {});
        if (originalMessageId && bot && gameData && gameData.chatId) bot.editMessageReplyMarkup({}, { chat_id: gameData.chatId, message_id: originalMessageId }).catch(() => {});
        else if (originalMessageId && bot && userObj && userObj.chat && userObj.chat.id) bot.editMessageReplyMarkup({}, { chat_id: String(userObj.chat.id), message_id: originalMessageId }).catch(() => {});
        return;
    }
    console.log(`${LOG_PREFIX} Game ${gameId} found for user ${userId}. Current status: ${gameData.status}`);

    if (gameData.userId !== userId || gameData.status !== 'player_turn') {
        console.warn(`${LOG_PREFIX} Invalid action for game ${gameId}. User ${userId} or status ${gameData.status}.`);
        await safeSendMessage(userId, "You cannot stand in this game at this moment.", {});
        return;
    }
     if (Number(gameData.gameMessageId) !== Number(originalMessageId)) {
        console.warn(`${LOG_PREFIX} Stand action on stale message for game ${gameId}. GameMsgID: ${gameData.gameMessageId}, ActionMsgID: ${originalMessageId}.`);
        return;
    }

    gameData.status = 'bot_turn';
    activeGames.set(gameId, gameData);
    console.log(`${LOG_PREFIX} Player ${userId} stands in game ${gameId} with score ${gameData.playerScore}. Status -> 'bot_turn'.`);

    const messageText = `${gameData.playerRef} stands with a formidable score of *${escapeMarkdownV2(String(gameData.playerScore))}*.\nThe tension mounts! The Bot Dealer will now reveal its hand and play... 🤖`;
    console.log(`${LOG_PREFIX} Composed 'stand' message for game ${gameId}: "${messageText}"`);
    await bot.editMessageText(messageText, {
        chat_id: gameData.chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {}
    }).catch(e => console.error(`${LOG_PREFIX} Failed to edit message on stand for game ${gameId}: ${e.message}`, e));

    console.log(`${LOG_PREFIX} Pausing 2s before bot's turn for game ${gameId}.`);
    await sleep(2000);
    await processDice21BotTurn(gameData, originalMessageId);
    console.log(`${LOG_PREFIX} Exiting handleDice21Stand for game ${gameId}.`);
}

async function processDice21BotTurn(gameData, messageIdToUpdate) {
    const LOG_PREFIX = "[Dice21_BotTurn]";
    console.log(`${LOG_PREFIX} Bot's turn for game ${gameData.gameId}. Player score: ${gameData.playerScore}. GameData: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(gameData) : JSON.stringify(gameData)}`);

    const { gameId, chatId, userId, playerRef, playerScore, betAmount } = gameData;
    let botScore = 0n;
    let botBusted = false;
    let botHandRolls = [];
    let botMessageAccumulator = `Player ${playerRef} stands at *${escapeMarkdownV2(String(playerScore))}*.\nBot Dealer's turn. Revealing hand...\n`;

    console.log(`${LOG_PREFIX} Initial bot turn message for game ${gameId}: "${botMessageAccumulator}"`);
    if (messageIdToUpdate && bot) {
        await bot.editMessageText(botMessageAccumulator + "\n_The dealer begins to draw..._ 🎲", {
            chat_id: chatId, message_id: messageIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: {}
        }).catch(e => console.warn(`${LOG_PREFIX} Initial editMessageText failed for game ${gameId}: ${e.message}`, e));
    }
    await sleep(1500);

    // Ensure DICE_21_BOT_STAND_SCORE is defined and accessible
    const botStandScoreThreshold = typeof DICE_21_BOT_STAND_SCORE !== 'undefined' ? BigInt(DICE_21_BOT_STAND_SCORE) : 17n;
    const targetScore = typeof DICE_21_TARGET_SCORE !== 'undefined' ? BigInt(DICE_21_TARGET_SCORE) : 21n;


    const maxBotRolls = 7;
    for (let i = 0; i < maxBotRolls && botScore < botStandScoreThreshold && !botBusted; i++) {
        console.log(`${LOG_PREFIX} Bot rolling die ${i + 1} for game ${gameId}. Current bot score: ${botScore}.`);
        let currentRollValue;
        try {
            console.log(`${LOG_PREFIX} Sending die for bot in game ${gameId}.`);
            const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
            if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') {
                throw new Error("Invalid dice message from Telegram API for bot's roll.");
            }
            currentRollValue = BigInt(diceMsg.dice.value);
            botHandRolls.push(currentRollValue);
            console.log(`${LOG_PREFIX} Bot drew a ${currentRollValue} (animated) for game ${gameId}. Pausing.`);
            await sleep(2000);
        } catch (e) {
            console.error(`${LOG_PREFIX} sendDice failed for bot in game ${gameId}: ${e.message}. Using internal roll.`, e);
            currentRollValue = BigInt(rollDie());
            botHandRolls.push(currentRollValue);
            await safeSendMessage(chatId, `Bot Dealer (internal roll): reveals a *${currentRollValue}*!`);
            console.log(`${LOG_PREFIX} Bot drew a ${currentRollValue} (internal) for game ${gameId}. Pausing.`);
            await sleep(1000);
        }
        botScore += currentRollValue;
        botMessageAccumulator += `Bot Dealer draws ${formatDiceRolls([Number(currentRollValue)])} → Bot's hand totals *${escapeMarkdownV2(String(botScore))}*.\n`;
        console.log(`${LOG_PREFIX} Bot score for game ${gameId} is now ${botScore}. Full hand: ${botHandRolls.join(', ')}.`);

        let nextActionMsg = "";
        if (botScore > targetScore) {
            botBusted = true;
            botMessageAccumulator += `💥 BOT BUSTED! Over ${targetScore}!\n`;
            console.log(`${LOG_PREFIX} Bot busted in game ${gameId}.`);
            nextActionMsg = "";
        } else if (botScore >= botStandScoreThreshold) {
            botMessageAccumulator += `Bot Dealer stands with *${escapeMarkdownV2(String(botScore))}*.\n`;
            console.log(`${LOG_PREFIX} Bot stands in game ${gameId} with score ${botScore}.`);
            nextActionMsg = "";
        } else {
            nextActionMsg = "\n_The dealer draws another..._ 🎲";
        }

        if (messageIdToUpdate && bot) {
            console.log(`${LOG_PREFIX} Updating message for game ${gameId} with bot's roll. Next action: "${nextActionMsg}"`);
            await bot.editMessageText(botMessageAccumulator + nextActionMsg, {
                chat_id: chatId, message_id: messageIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: {}
            }).catch(e => console.warn(`${LOG_PREFIX} editMessageText during bot roll ${i + 1} failed for game ${gameId}: ${e.message}`, e));
        }
        if (botBusted || botScore >= botStandScoreThreshold) break;
        await sleep(2500);
    }
    await sleep(1500);

    let resultTextEnd = "";
    let payoutAmount = 0n;
    let outcomeReasonLog = "";

    console.log(`${LOG_PREFIX} Determining final outcome for game ${gameId}. Player score: ${playerScore}, Bot score: ${botScore}, Bot busted: ${botBusted}.`);
    if (botBusted) {
        resultTextEnd = `🎉 ${playerRef} WINS! The Bot Dealer busted. A fine victory!`;
        payoutAmount = betAmount + betAmount;
        outcomeReasonLog = `won_dice21_bot_bust:${gameId}`;
    } else if (playerScore > botScore) {
        resultTextEnd = `🎉 ${playerRef} WINS with a superior hand (*${escapeMarkdownV2(String(playerScore))}* vs. Bot's *${escapeMarkdownV2(String(botScore))}*)!`;
        payoutAmount = betAmount + betAmount;
        outcomeReasonLog = `won_dice21_score:${gameId}`;
    } else if (botScore > playerScore) {
        resultTextEnd = `💀 The Bot Dealer takes the round (*${escapeMarkdownV2(String(botScore))}* vs. your *${escapeMarkdownV2(String(playerScore))}*). Better luck next time!`;
        payoutAmount = 0n;
        outcomeReasonLog = `lost_dice21_score:${gameId}`;
    } else {
        resultTextEnd = `😐 A PUSH! Both player and Bot Dealer have *${escapeMarkdownV2(String(playerScore))}*. Your wager of ${escapeMarkdownV2(formatCurrency(Number(betAmount)))} is returned.`;
        payoutAmount = betAmount;
        outcomeReasonLog = `push_dice21:${gameId}`;
    }
    console.log(`${LOG_PREFIX} Game ${gameId} outcome: ${resultTextEnd}. PayoutAmount: ${payoutAmount}, Reason: ${outcomeReasonLog}.`);

    resultTextEnd += `\nYour original wager: ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}.`;

    if (payoutAmount > 0n) {
        const balanceUpdateOutcome = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
        console.log(`${LOG_PREFIX} Balance update for payout in game ${gameId}: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(balanceUpdateOutcome) : JSON.stringify(balanceUpdateOutcome)}`);
        if (balanceUpdateOutcome.success) {
            resultTextEnd += `\nYour payout of *${escapeMarkdownV2(formatCurrency(Number(payoutAmount)))}* has been credited.`;
        } else {
            resultTextEnd += `\nThere was an issue processing your payout. Admin has been notified.`;
            console.error(`${LOG_PREFIX} Failed to credit payout for game ${gameId}: ${balanceUpdateOutcome.error}`);
        }
    } else if (outcomeReasonLog.startsWith('lost_')) {
        await updateUserBalance(userId, 0n, outcomeReasonLog, null, gameId, String(chatId));
        console.log(`${LOG_PREFIX} Loss logged for game ${gameId}.`);
    }

    const finalMessageText = botMessageAccumulator + "\n" + resultTextEnd;
    const playAgainKeyboardD21 = { inline_keyboard: [[{ text: `🎲 Play Dice 21 Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_d21:${betAmount}` }]] };
    console.log(`${LOG_PREFIX} Composed final game message for ${gameId}: "${finalMessageText}", Keyboard: ${typeof stringifyWithBigInt === 'function' ? stringifyWithBigInt(playAgainKeyboardD21) : JSON.stringify(playAgainKeyboardD21)}`);

    if (messageIdToUpdate && bot) {
        await bot.editMessageText(finalMessageText, {
            chat_id: chatId, message_id: messageIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboardD21
        }).catch(async (e) => {
            console.error(`${LOG_PREFIX} Failed to edit final message for game ${gameId}: ${e.message}. Sending new.`, e);
            await safeSendMessage(chatId, finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboardD21 });
        });
    } else {
        console.warn(`${LOG_PREFIX} No messageIdToUpdate or bot instance for game ${gameId}. Sending new final message.`);
        await safeSendMessage(chatId, finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboardD21 });
    }

    activeGames.delete(gameId);
    console.log(`${LOG_PREFIX} Game ${gameId} concluded and removed from activeGames. Player: ${playerScore}, Bot: ${botScore}, BotBusted: ${botBusted}.`);
}

console.log("Dice 21 Game Logic with Casino Feel (no duplicate stringifyWithBigInt) fully loaded and ready.");

// --- END OF DICE 21 GAME LOGIC ---

// --- OVER/UNDER 7 GAME HANDLER FUNCTIONS ---
async function handleStartOverUnder7Command(chatId, userObj, betAmount, commandMessageId) {
    const userId = String(userObj.id);
    const playerRef = getPlayerDisplayReference(userObj);
    const gameId = generateGameId();

    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_ou7:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${playerRef}, bet failed for Over/Under 7: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const messageText = `${playerRef}, you bet ${formatCurrency(betAmount)} on Over/Under 7.\nChoose your prediction:`;
    const keyboard = {
        inline_keyboard: [[
            { text: "📉 Under 7 (Roll 2-6)", callback_data: `ou7_choice:${gameId}:under` },
            { text: "🍀 Exactly 7", callback_data: `ou7_choice:${gameId}:exact` },
            { text: "📈 Over 7 (Roll 8-12)", callback_data: `ou7_choice:${gameId}:over` }
        ]]
    };
    const sentMsg = await safeSendMessage(chatId, messageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    if (sentMsg) {
        activeGames.set(gameId, {
            type: 'overunder7', gameId, chatId, userId, playerRef,
            betAmount: BigInt(betAmount),
            status: 'waiting_choice',
            gameMessageId: sentMsg.message_id
        });
    } else { // Failed to send message, refund bet
        await updateUserBalance(userId, betAmount, `refund_ou7_setup_fail:${gameId}`, null, gameId, String(chatId));
    }
}

async function handleOverUnder7Choice(gameId, choice, userObj, originalMessageId) {
    const gameData = activeGames.get(gameId);
    const userId = String(userObj.id);

    if (!gameData || gameData.userId !== userId || gameData.status !== 'waiting_choice') {
        await safeSendMessage(userId, "Not your Over/Under 7 game or choice already made.", {});
        return;
    }

    gameData.playerChoice = choice;
    gameData.status = 'rolling';
    activeGames.set(gameId, gameData);

    const choiceText = choice === 'exact' ? 'Exactly 7' : (choice === 'under' ? 'Under 7' : 'Over 7');
    await bot.editMessageText(`${gameData.playerRef} chose *${choiceText}* for ${formatCurrency(Number(gameData.betAmount))}. Rolling dice... 🎲🎲`, {
        chat_id: gameData.chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {}
    });
    await sleep(1000);

    let roll1, roll2;
    try {
        const diceMsg1 = await bot.sendDice(gameData.chatId, { emoji: '🎲' });
        roll1 = diceMsg1.dice.value;
        await sleep(2000);
        const diceMsg2 = await bot.sendDice(gameData.chatId, { emoji: '🎲' });
        roll2 = diceMsg2.dice.value;
        await sleep(2000);
    } catch (e) {
        console.error("[OU7_ERR] sendDice failed, using internal rolls:", e.message);
        roll1 = rollDie(); roll2 = rollDie();
        await safeSendMessage(gameData.chatId, `Bot (internal rolls): ${formatDiceRolls([roll1, roll2])}`);
    }
    
    const total = roll1 + roll2;
    let resultText = `${gameData.playerRef} chose *${choiceText}*.\nDice rolled: ${formatDiceRolls([roll1, roll2])} (Total: *${total}*).\n\n`;
    let payoutAmount = 0n;
    let outcomeReasonLog = "";
    let playerWon = false;

    if (choice === 'under' && total < 7) { playerWon = true; payoutAmount = gameData.betAmount + (gameData.betAmount * BigInt(OU7_PAYOUT_NORMAL)); }
    else if (choice === 'over' && total > 7) { playerWon = true; payoutAmount = gameData.betAmount + (gameData.betAmount * BigInt(OU7_PAYOUT_NORMAL)); }
    else if (choice === 'exact' && total === 7) { playerWon = true; payoutAmount = gameData.betAmount + (gameData.betAmount * BigInt(OU7_PAYOUT_SEVEN)); }

    if (playerWon) {
        resultText += `🎉 Congratulations, you WIN! You receive ${formatCurrency(Number(payoutAmount))}.`;
        outcomeReasonLog = `won_ou7_${choice}:${gameId}`;
        await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(gameData.chatId));
    } else {
        resultText += `💀 Sorry, you lost your ${formatCurrency(Number(gameData.betAmount))} bet.`;
        outcomeReasonLog = `lost_ou7_${choice}:${gameId}`;
        // Bet already deducted, log the loss
        await queryDatabase(
            `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() 
             WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
             [outcomeReasonLog, userId, gameId]
        ).catch(e => console.error("[OU7_CHOICE_ERR] Error logging loss:", e));
    }

    const playAgainKeyboardOU7 = { inline_keyboard: [[{ text: `🎲 Play Over/Under 7 Again (${formatCurrency(Number(gameData.betAmount))})`, callback_data: `play_again_ou7:${gameData.betAmount}` }]] };
    await bot.editMessageText(resultText, { chat_id: gameData.chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboardOU7 });
    
    activeGames.delete(gameId);
    console.log(`[OU7_END] Game ${gameId} ended. Choice: ${choice}, Total: ${total}, Win: ${playerWon}`);
}


// --- END OF SEGMENT 1 for Part 5a -------
// --- Start of Part 5a (Segment 2 of 2) ---
// (This code directly follows the end of Part 5a, Segment 1)

// --- HIGH ROLLER DUEL GAME HANDLER FUNCTIONS ---
async function handleStartDuelCommand(chatId, userObj, betAmount, commandMessageId) {
    const userId = String(userObj.id);
    const playerRef = getPlayerDisplayReference(userObj);
    const gameId = generateGameId();

    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_duel:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${playerRef}, bet failed for Duel: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const messageText = `⚔️ ${playerRef} starts a Dice Duel with a ${formatCurrency(betAmount)} bet against the Bot!\nClick below to roll your dice!`;
    const keyboard = { inline_keyboard: [[{ text: `🎲 Roll ${DUEL_DICE_COUNT} Dice!`, callback_data: `duel_roll:${gameId}` }]] };
    
    const sentMsg = await safeSendMessage(chatId, messageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    if (sentMsg) {
        activeGames.set(gameId, {
            type: 'duel', gameId, chatId, userId, playerRef,
            betAmount: BigInt(betAmount),
            playerScore: 0, // Will be sum of DUEL_DICE_COUNT dice
            botScore: 0,    // Will be sum of DUEL_DICE_COUNT dice
            status: 'player_turn_to_roll', // Player needs to click to roll
            gameMessageId: sentMsg.message_id
        });
    } else {
        await updateUserBalance(userId, betAmount, `refund_duel_setup_fail:${gameId}`, null, gameId, String(chatId)); // Refund
    }
}

async function handleDuelRoll(gameId, userObj, originalMessageId) {
    const gameData = activeGames.get(gameId);
    const userId = String(userObj.id);

    if (!gameData || gameData.userId !== userId || gameData.status !== 'player_turn_to_roll') {
        await safeSendMessage(userId, "Not your Duel game or not time to roll.", {});
        return;
    }

    const chatId = gameData.chatId;
    await bot.editMessageText(`${gameData.playerRef} is rolling ${DUEL_DICE_COUNT} dice...`, {chat_id: chatId, message_id: originalMessageId, parse_mode:'MarkdownV2', reply_markup:{}});
    
    let playerRolls = [];
    let playerScore = 0;
    for (let i = 0; i < DUEL_DICE_COUNT; i++) {
        try {
            const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
            playerRolls.push(diceMsg.dice.value);
            playerScore += diceMsg.dice.value;
            await sleep(2000); 
        } catch (e) {
            console.error(`[DUEL_ERR] sendDice failed for player, using internal: ${e.message}`);
            const internalRoll = rollDie();
            playerRolls.push(internalRoll);
            playerScore += internalRoll;
            await safeSendMessage(chatId, `${gameData.playerRef} (internal roll ${i+1}/${DUEL_DICE_COUNT}): ${formatDiceRolls([internalRoll])}`);
            await sleep(1000);
        }
    }
    gameData.playerScore = BigInt(playerScore);
    
    let messageText = `${gameData.playerRef} rolled ${formatDiceRolls(playerRolls)} for a total of *${playerScore}*!\n\nNow the Bot rolls...`;
    await bot.editMessageText(messageText, {chat_id: chatId, message_id: originalMessageId, parse_mode:'MarkdownV2', reply_markup:{}});
    await sleep(1500);

    let botRolls = [];
    let botScore = 0;
    for (let i = 0; i < DUEL_DICE_COUNT; i++) {
         try {
            const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
            botRolls.push(diceMsg.dice.value);
            botScore += diceMsg.dice.value;
            await sleep(2000);
        } catch (e) {
            console.error(`[DUEL_ERR] sendDice failed for bot, using internal: ${e.message}`);
            const internalRoll = rollDie();
            botRolls.push(internalRoll);
            botScore += internalRoll;
            await safeSendMessage(chatId, `Bot (internal roll ${i+1}/${DUEL_DICE_COUNT}): ${formatDiceRolls([internalRoll])}`);
            await sleep(1000);
        }
    }
    gameData.botScore = BigInt(botScore);
    gameData.status = 'game_over';

    messageText += `\nBot rolled ${formatDiceRolls(botRolls)} for a total of *${botScore}*.\n\n`;

    let payoutAmount = 0n;
    let outcomeReasonLog = "";

    if (gameData.playerScore > gameData.botScore) {
        messageText += `🎉 ${gameData.playerRef} WINS the duel!`;
        payoutAmount = gameData.betAmount + gameData.betAmount; // Bet back + 1x bet profit
        outcomeReasonLog = `won_duel:${gameId}`;
    } else if (gameData.botScore > gameData.playerScore) {
        messageText += `💀 The Bot WINS the duel! ${gameData.playerRef} loses the bet.`;
        payoutAmount = 0n; // Bet already deducted
        outcomeReasonLog = `lost_duel:${gameId}`;
    } else {
        messageText += `😐 It's a TIE! Bet returned.`;
        payoutAmount = gameData.betAmount; // Bet back
        outcomeReasonLog = `push_duel:${gameId}`;
    }

    if (payoutAmount > 0n) {
        await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
        messageText += `\nYou receive ${formatCurrency(Number(payoutAmount))}.`;
    } else if (outcomeReasonLog.startsWith('lost_')) {
         await queryDatabase(
            `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() 
             WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
             [outcomeReasonLog, userId, gameId]
        ).catch(e => console.error("[DUEL_ERR] Error logging player loss:", e));
    }
    
    const playAgainKeyboard = { inline_keyboard: [[{ text: `⚔️ Play Duel Again (${formatCurrency(Number(gameData.betAmount))})`, callback_data: `play_again_duel:${gameData.betAmount}` }]] };
    await bot.editMessageText(messageText, {chat_id: chatId, message_id: originalMessageId, parse_mode:'MarkdownV2', reply_markup: playAgainKeyboard});
    
    activeGames.delete(gameId);
}


// --- GREED'S LADDER GAME HANDLER FUNCTION ---
async function handleStartLadderCommand(chatId, userObj, betAmount, commandMessageId) {
    const userId = String(userObj.id);
    const playerRef = getPlayerDisplayReference(userObj);
    const gameId = generateGameId();

    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_ladder:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${playerRef}, bet failed for Greed's Ladder: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let mainMessage = await safeSendMessage(chatId, `🪜 ${playerRef} starts Greed's Ladder with a ${formatCurrency(betAmount)} bet!\nBot will roll ${LADDER_ROLL_COUNT} dice for you. Bust on a ${LADDER_BUST_ON}! Good luck!`, { parse_mode: 'MarkdownV2' });
    const gameMessageId = mainMessage ? mainMessage.message_id : null;
    
    activeGames.set(gameId, { // Store minimal state needed if errors occur before full processing
        type: 'ladder', gameId, chatId, userId, playerRef,
        betAmount: BigInt(betAmount), status: 'rolling', gameMessageId
    });

    await sleep(1500);

    let rolls = [];
    let currentSum = 0;
    let busted = false;

    for (let i = 0; i < LADDER_ROLL_COUNT; i++) {
        await safeSendMessage(chatId, `${playerRef}, Roll ${i + 1}/${LADDER_ROLL_COUNT}...`, {});
        let rollValue;
        try {
            const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
            rollValue = diceMsg.dice.value;
            await sleep(2000);
        } catch (e) {
            console.error(`[LADDER_ERR] sendDice failed for roll ${i+1}, using internal: ${e.message}`);
            rollValue = rollDie();
            await safeSendMessage(chatId, `${playerRef} (internal roll ${i+1}): ${formatDiceRolls([rollValue])}`);
            await sleep(1000);
        }

        rolls.push(rollValue);
        if (rollValue === LADDER_BUST_ON) {
            busted = true;
            break; 
        }
        currentSum += rollValue;
    }

    let resultText = `${playerRef}'s rolls for Greed's Ladder: ${formatDiceRolls(rolls)}.\n`;
    let payoutAmount = 0n;
    let outcomeReasonLog = "";

    if (busted) {
        resultText += `💥 Oh no, a ${LADDER_BUST_ON}! You BUSTED and lose your ${formatCurrency(Number(betAmount))} bet.`;
        outcomeReasonLog = `lost_ladder_bust:${gameId}`;
        // Bet already deducted
        await queryDatabase(
            `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() 
             WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
             [outcomeReasonLog, userId, gameId]
        ).catch(e => console.error("[LADDER_ERR] Error logging bust:", e));
    } else {
        resultText += `Total score: *${currentSum}*.\n`;
        let foundTier = false;
        for (const tier of LADDER_PAYOUTS) {
            if (currentSum >= tier.min && currentSum <= tier.max) {
                resultText += `${tier.label} `;
                if (tier.multiplier > 0) {
                    payoutAmount = gameData.betAmount + (gameData.betAmount * BigInt(tier.multiplier));
                    resultText += `You win! Payout: ${formatCurrency(Number(payoutAmount))}.`;
                    outcomeReasonLog = `won_ladder_score_${currentSum}:${gameId}`;
                } else if (tier.multiplier === 0) {
                    payoutAmount = gameData.betAmount; // Push
                    resultText += `Bet returned.`;
                    outcomeReasonLog = `push_ladder_score_${currentSum}:${gameId}`;
                } else { // Lose
                    resultText += `You lose your bet.`;
                    outcomeReasonLog = `lost_ladder_score_${currentSum}:${gameId}`;
                     await queryDatabase(
                        `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() 
                         WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
                         [outcomeReasonLog, userId, gameId]
                    ).catch(e => console.error("[LADDER_ERR] Error logging loss:", e));
                }
                foundTier = true;
                break;
            }
        }
        if (!foundTier) { // Should not happen if tiers cover all sums
            resultText += `No payout tier matched for score ${currentSum}. Bet lost.`;
            outcomeReasonLog = `lost_ladder_unknown_tier:${gameId}`;
             await queryDatabase(
                `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() 
                 WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
                 [outcomeReasonLog, userId, gameId]
            ).catch(e => console.error("[LADDER_ERR] Error logging unknown tier loss:", e));
        }
    }

    if (payoutAmount > 0n) {
        await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    }
    
    const playAgainKeyboard = { inline_keyboard: [[{ text: `🪜 Play Ladder Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_ladder:${betAmount}` }]] };
    
    if (gameMessageId && bot) {
        await bot.editMessageText(resultText, {chat_id: chatId, message_id: gameMessageId, parse_mode:'MarkdownV2', reply_markup: playAgainKeyboard});
    } else {
        await safeSendMessage(chatId, resultText, {parse_mode:'MarkdownV2', reply_markup: playAgainKeyboard});
    }
    activeGames.delete(gameId);
}


// --- SEVENS OUT GAME HANDLER FUNCTIONS ---
async function handleStartSevenOutCommand(chatId, userObj, betAmount, commandMessageId) {
    const userId = String(userObj.id);
    const playerRef = getPlayerDisplayReference(userObj);
    const gameId = generateGameId();

    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_s7:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${playerRef}, bet failed for Sevens Out: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let initialMessage = await safeSendMessage(chatId, `🎲 ${playerRef} starts Sevens Out with ${formatCurrency(betAmount)}!\nCome Out Roll happening now...`, { parse_mode: 'MarkdownV2' });
    const gameMessageId = initialMessage ? initialMessage.message_id : null;

    const gameData = {
        type: 'sevenout', gameId, chatId, userId, playerRef,
        betAmount: BigInt(betAmount),
        point: null, status: 'come_out_roll', gameMessageId
    };
    activeGames.set(gameId, gameData);
    await sleep(1500);

    // Perform Come Out Roll
    await processSevenOutRoll(gameData, gameMessageId);
}

async function handleSevenOutRoll(gameId, userObj, originalMessageId) { // Called when player clicks "Roll for Point"
    const gameData = activeGames.get(gameId);
    const userId = String(userObj.id);

    if (!gameData || gameData.userId !== userId || gameData.status !== 'point_phase') {
        await safeSendMessage(userId, "Not your Sevens Out game or not in point phase.", {});
        return;
    }
    // Edit message to show rolling for point
    await bot.editMessageText(`${gameData.playerRef} is rolling for Point *${gameData.point}*...`, {
        chat_id: gameData.chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {}
    });
    await sleep(1000);
    await processSevenOutRoll(gameData, originalMessageId); // Process the actual roll
}

async function processSevenOutRoll(gameData, messageId) { // Handles both Come Out and Point Phase rolls
    const { gameId, chatId, userId, playerRef, betAmount, status, point } = gameData;
    let roll1, roll2;
    try {
        const diceMsg1 = await bot.sendDice(chatId, { emoji: '🎲' }); roll1 = diceMsg1.dice.value; await sleep(2000);
        const diceMsg2 = await bot.sendDice(chatId, { emoji: '🎲' }); roll2 = diceMsg2.dice.value; await sleep(2000);
    } catch(e) { roll1 = rollDie(); roll2 = rollDie(); await safeSendMessage(chatId, `${playerRef} (internal rolls): ${formatDiceRolls([roll1, roll2])}`); }
    
    const total = roll1 + roll2;
    let messageText = `${playerRef} rolled ${formatDiceRolls([roll1, roll2])} = *${total}*.\n`;
    let payoutAmount = 0n;
    let outcomeReasonLog = "";
    let gameEnded = false;
    let newStatus = status;
    let newPoint = point;
    let buttons = [];

    if (status === 'come_out_roll') {
        if (total === 7 || total === 11) { // Win on Come Out
            messageText += `🎉 WINNER! ${total} on the come out roll! You win ${formatCurrency(Number(betAmount))} profit.`;
            payoutAmount = betAmount + betAmount; // Bet back + profit
            outcomeReasonLog = `won_s7_comeout_${total}:${gameId}`;
            gameEnded = true;
        } else if (total === 2 || total === 3 || total === 12) { // Lose on Come Out (Craps)
            messageText += `💀 CRAPS! ${total} on the come out roll. You lose your bet.`;
            outcomeReasonLog = `lost_s7_comeout_${total}:${gameId}`;
            gameEnded = true;
        } else { // Point Established
            newPoint = total;
            newStatus = 'point_phase';
            messageText += `POINT IS SET: *${total}*.\nRoll again to hit your Point before a 7!`;
            buttons.push({ text: `🎲 Roll for Point ${total}`, callback_data: `s7_roll:${gameId}` });
        }
    } else if (status === 'point_phase') {
        if (total === newPoint) { // Made Point
            messageText += `🎉 POINT HIT! You rolled your Point *${newPoint}*! You win ${formatCurrency(Number(betAmount))} profit.`;
            payoutAmount = betAmount + betAmount;
            outcomeReasonLog = `won_s7_hit_point_${total}:${gameId}`;
            gameEnded = true;
        } else if (total === 7) { // Seven Out - Lose
            messageText += `💀 SEVEN OUT! You rolled a 7 before your Point *${newPoint}*. You lose your bet.`;
            outcomeReasonLog = `lost_s7_seven_out:${gameId}`;
            gameEnded = true;
        } else { // Keep rolling for Point
            messageText += `You rolled *${total}*. Still aiming for Point *${newPoint}*. Roll again!`;
            buttons.push({ text: `🎲 Roll for Point ${newPoint}`, callback_data: `s7_roll:${gameId}` });
        }
    }

    if (gameEnded) {
        buttons.push({ text: `🎲 Play Sevens Out Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_s7:${betAmount}` });
        if (payoutAmount > 0n) {
            await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
        } else { // Log loss
             await queryDatabase(
                `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() 
                 WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
                 [outcomeReasonLog, userId, gameId]
            ).catch(e => console.error("[S7_ERR] Error logging loss:", e));
        }
        activeGames.delete(gameId);
    } else {
        gameData.status = newStatus;
        gameData.point = newPoint;
        activeGames.set(gameId, gameData);
    }

    const keyboard = { inline_keyboard: [buttons] };
    if (messageId && bot) await bot.editMessageText(messageText, {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboard : {}) });
    else await safeSendMessage(chatId, messageText, {parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboard : {}) });
}


// --- SLOT FRUIT FRENZY GAME HANDLER FUNCTION ---
async function handleStartSlotCommand(chatId, userObj, betAmount, commandMessageId) {
    const userId = String(userObj.id);
    const playerRef = getPlayerDisplayReference(userObj);
    const gameId = generateGameId();

    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_slot:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${playerRef}, bet failed for Slots: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let initialMessage = await safeSendMessage(chatId, `🎰 ${playerRef} spins the Slot Machine for ${formatCurrency(betAmount)}! Good luck...`, { parse_mode: 'MarkdownV2' });
    const gameMessageId = initialMessage ? initialMessage.message_id : null;
    
    await sleep(1000);

    let slotResultValue;
    try {
        const diceMsg = await bot.sendDice(chatId, { emoji: '🎰' });
        slotResultValue = diceMsg.dice.value; // Value 1-64
        await sleep(3000); // Time for animation
    } catch (e) {
        console.error(`[SLOT_ERR] sendDice for slot machine failed: ${e.message}. Assigning random loss.`);
        // Fallback to a non-winning value if sendDice fails, for graceful error handling
        // Find a value not in SLOT_PAYOUTS keys
        let randomLossVal = Math.floor(Math.random() * 64) + 1;
        while(SLOT_PAYOUTS[randomLossVal]) { randomLossVal = Math.floor(Math.random() * 64) + 1; }
        slotResultValue = randomLossVal;
        await safeSendMessage(chatId, `${playerRef}, slot machine had a hiccup. Spinning internally...`);
        await sleep(1000);
    }

    const outcome = SLOT_PAYOUTS[slotResultValue];
    let resultText = `${playerRef}, the slot machine result value is ${slotResultValue}.\n`;
    let payoutAmount = 0n;
    let outcomeReasonLog = "";

    if (outcome) {
        resultText += `Symbols: ${escapeMarkdownV2(outcome.symbols)}\n🎉 YOU WIN! 🎉`;
        payoutAmount = BigInt(betAmount) + (BigInt(betAmount) * BigInt(outcome.multiplier));
        outcomeReasonLog = `won_slot_${slotResultValue}:${gameId}`;
        await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
        resultText += `\nPayout: ${formatCurrency(Number(payoutAmount))}.`;
    } else {
        resultText += `Sorry, not a winning spin this time. You lose your ${formatCurrency(Number(betAmount))} bet.`;
        outcomeReasonLog = `lost_slot_${slotResultValue}:${gameId}`;
        // Bet already deducted, log loss
         await queryDatabase(
            `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() 
             WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
             [outcomeReasonLog, userId, gameId]
        ).catch(e => console.error("[SLOT_ERR] Error logging loss:", e));
    }

    const playAgainKeyboard = { inline_keyboard: [[{ text: `🎰 Spin Slots Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_slot:${betAmount}` }]] };
    
    if (gameMessageId && bot) {
        await bot.editMessageText(resultText, {chat_id: chatId, message_id: gameMessageId, parse_mode:'MarkdownV2', reply_markup: playAgainKeyboard});
    } else {
        await safeSendMessage(chatId, resultText, {parse_mode:'MarkdownV2', reply_markup: playAgainKeyboard});
    }
    activeGames.delete(gameId); // Slots is a one-shot game per spin
}


console.log("Part 5a: Message & Callback Handling (All Games & Features) - Complete.");
// --- End of Part 5a ---
// --- Start of Part 5b ---
// index.js - Part 5b: Dice Escalator Game Logic (Professional Casino Feel, Updated Stand Button, Jackpot Tip & Enhanced Logging)
//---------------------------------------------------------------------------
console.log("Loading Part 5b: Dice Escalator Game Logic (Professional Casino Feel, Updated Stand Button, Jackpot Tip, Jackpot Button, UI enhancements, jackpot display, and DETAILED LOGGING)...");

// Helper function for JSON.stringify to handle BigInts, undefined, and other types for comprehensive logging.
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

// --- Helper Function to get Jackpot Text for the Button ---
async function getJackpotButtonText() {
    const LOG_PREFIX = "[getJackpotButtonText]";
    console.log(`${LOG_PREFIX} Attempting to fetch jackpot amount for button text.`);
    let jackpotAmountString = "🎰 Jackpot: N/A";

    try {
        console.log(`${LOG_PREFIX} Checking for dependencies: queryDatabase, MAIN_JACKPOT_ID, formatCurrency.`);
        if (typeof queryDatabase !== 'function' || typeof MAIN_JACKPOT_ID === 'undefined' || typeof formatCurrency !== 'function') {
            console.warn(`${LOG_PREFIX} Missing dependencies for jackpot button text. queryDatabase: ${typeof queryDatabase}, MAIN_JACKPOT_ID: ${MAIN_JACKPOT_ID}, formatCurrency: ${typeof formatCurrency}. Using default text.`);
            return jackpotAmountString;
        }
        console.log(`${LOG_PREFIX} Dependencies seem to be present.`);
        const sqlQuery = 'SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1';
        const queryParams = [MAIN_JACKPOT_ID];
        const result = await queryDatabase(sqlQuery, queryParams);

        if (result.rows.length > 0) {
            const jackpotAmountLamports = BigInt(result.rows[0].current_amount_lamports);
            console.log(`${LOG_PREFIX} Jackpot amount found: ${jackpotAmountLamports} lamports.`);
            const jackpotDisplayAmount = formatCurrency(Number(jackpotAmountLamports));
            jackpotAmountString = `✨ Jackpot: ${jackpotDisplayAmount} ✨`; // Enhanced visual
            console.log(`${LOG_PREFIX} Successfully generated jackpot button text: "${jackpotAmountString}"`);
        } else {
            console.log(`${LOG_PREFIX} No jackpot record found for jackpot_id: ${MAIN_JACKPOT_ID}. Using default text.`);
        }
    } catch (error) {
        console.error(`${LOG_PREFIX} Error fetching jackpot for button: ${error.message}`, error);
        jackpotAmountString = "🎰 Jackpot: Error";
    }
    return jackpotAmountString;
}

// --- Dice Escalator Constants ---
// These are expected to be globally available (e.g., from Part 1, loaded from process.env).
// const BOT_STAND_SCORE_DICE_ESCALATOR = BigInt(10);
// const TARGET_JACKPOT_SCORE = BigInt(process.env.TARGET_JACKPOT_SCORE || '100'); // User mentioned 100
// const MAIN_JACKPOT_ID = 'dice_escalator_main';

// --- Game Handler Functions for Dice Escalator ---

async function handleStartDiceEscalatorCommand(chatId, userObj, betAmount, originalCommandMessageId) {
    const LOG_PREFIX = "[handleStartDiceEscalatorCommand]";
    console.log(`${LOG_PREFIX} Entered. ChatID: ${chatId}, UserObj: ${stringifyWithBigInt(userObj)}, BetAmount: ${betAmount}, OriginalCmdMsgID: ${originalCommandMessageId}`);

    if (!userObj || typeof userObj.userId === 'undefined') {
        console.error(`${LOG_PREFIX} CRITICAL ERROR: userObj is invalid or userObj.userId is undefined. UserObj: ${stringifyWithBigInt(userObj)}.`);
        if (typeof safeSendMessage === 'function') {
            await safeSendMessage(chatId, `An internal error occurred with your user profile. Please try again or contact support.`, {});
        }
        return;
    }
    const userId = String(userObj.userId);
    const playerRef = typeof getPlayerDisplayReference === 'function' ? getPlayerDisplayReference(userObj) : `Player ${userId}`;
    const gameId = typeof generateGameId === 'function' ? generateGameId() : `game_${Date.now()}`;

    console.log(`${LOG_PREFIX} User ${userId} (${playerRef}) initiated Dice Escalator. Bet: ${betAmount}, GameID: ${gameId}`);

    if (!userId || userId === "undefined_value" || userId === "undefined") {
        console.error(`${LOG_PREFIX} Critical error: UserID is invalid. UserID: ${userId}. UserObj: ${stringifyWithBigInt(userObj)}. Cannot start game.`);
        await safeSendMessage(chatId, `${playerRef}, a technical difficulty occurred processing your ID. Please try again.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    console.log(`${LOG_PREFIX} Placing wager for UserID: ${userId}, Amount: ${-betAmount}, GameID: ${gameId}`);
    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_dice_escalator:${gameId}`, null, gameId, String(chatId));
    console.log(`${LOG_PREFIX} Wager placement result: ${stringifyWithBigInt(balanceUpdateResult)}`);

    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        const errorMsg = balanceUpdateResult ? balanceUpdateResult.error : "transaction issue";
        console.error(`${LOG_PREFIX} Wager placement failed for ${userId}. Error: ${errorMsg}`);
        await safeSendMessage(chatId, `${playerRef}, your wager of ${escapeMarkdownV2(formatCurrency(betAmount))} could not be processed due to a ${escapeMarkdownV2(errorMsg)}. Please check your balance or try again.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX} Wager of ${formatCurrency(betAmount)} accepted for ${userId}. New balance: ${balanceUpdateResult.newBalanceLamports !== undefined ? formatCurrency(Number(balanceUpdateResult.newBalanceLamports)) : 'N/A'}`);

    const gameData = {
        type: 'DiceEscalator', gameId, chatId: String(chatId), userId, playerRef,
        betAmount: BigInt(betAmount), playerScore: 0n, playerRollCount: 0, botScore: 0n,
        status: 'waiting_player_roll', gameMessageId: null, commandMessageId: originalCommandMessageId,
        lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData);
    console.log(`${LOG_PREFIX} Game ${gameId} initiated. Data: ${stringifyWithBigInt(gameData)}`);

    const jackpotButtonText = await getJackpotButtonText();
    console.log(`${LOG_PREFIX} Jackpot button text: "${jackpotButtonText}"`);

    let targetJackpotScoreDisplay = "the house's target score";
    if (typeof TARGET_JACKPOT_SCORE !== 'undefined' && !isNaN(parseInt(TARGET_JACKPOT_SCORE))) {
        targetJackpotScoreDisplay = escapeMarkdownV2(String(TARGET_JACKPOT_SCORE));
         console.log(`${LOG_PREFIX} TARGET_JACKPOT_SCORE is defined as: ${TARGET_JACKPOT_SCORE}`);
    } else {
        console.warn(`${LOG_PREFIX} WARNING: TARGET_JACKPOT_SCORE is undefined or not a valid number. Jackpot tip message will be generic. Value: ${TARGET_JACKPOT_SCORE}`);
    }
    const jackpotTip = `\n\n👑 *How to Win the Jackpot:*\nTo claim the grand prize, you must stand with a score of *${targetJackpotScoreDisplay} or higher* AND emerge victorious against the Bot Dealer in this round!`;

    const initialMessageText = `🎲 Welcome to the Dice Escalator, ${playerRef}! 🎲\n\nYou've placed a wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* against our esteemed Bot Dealer 🤖.${jackpotTip}\n\nThe table is yours. Current score: *0*. Fortune awaits your first roll!\n👇 Press "Roll Dice" to begin.`;
    const keyboard = {
        inline_keyboard: [
            [{ text: jackpotButtonText, callback_data: 'jackpot_display_noop' }],
            [{ text: "🎲 Roll Dice", callback_data: `de_roll_prompt:${gameId}` }],
        ]
    };
    console.log(`${LOG_PREFIX} Initial game message composed. Length: ${initialMessageText.length}. Keyboard: ${stringifyWithBigInt(keyboard)}`);

    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    if (sentMessage && sentMessage.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData);
        console.log(`${LOG_PREFIX} Game ${gameId} message sent. MessageID: ${sentMessage.message_id}.`);
    } else {
        console.error(`${LOG_PREFIX} Failed to send game message for ${gameId}. Refunding wager for ${userId}.`);
        await updateUserBalance(userId, betAmount, `refund_dice_escalator_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
        console.log(`${LOG_PREFIX} Wager refunded and game ${gameId} removed.`);
    }
    console.log(`${LOG_PREFIX} Exiting handleStartDiceEscalatorCommand.`);
}


async function handleDiceEscalatorPlayerAction(gameId, userIdFromCallback, action, originalMessageId, chatIdFromCallback) {
    const LOG_PREFIX = "[handleDiceEscalatorPlayerAction]";
    console.log(`${LOG_PREFIX} Entered. GameID: ${gameId}, UserID_CB: ${userIdFromCallback}, Action: ${action}, OrigMsgID: ${originalMessageId}, ChatID_CB: ${chatIdFromCallback}`);

    const gameData = activeGames.get(gameId);
    const now = Date.now();

    if (!gameData) {
        console.warn(`${LOG_PREFIX} Game ${gameId} not found. Action: ${action}. Attempting to clear buttons on msg ${originalMessageId}.`);
        if (bot && originalMessageId && chatIdFromCallback && typeof bot.editMessageReplyMarkup === 'function') {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatIdFromCallback), message_id: Number(originalMessageId) })
                .catch(e => console.error(`${LOG_PREFIX} Failed to clear buttons for expired game ${gameId}: ${e.message}`, e));
        }
        await safeSendMessage(String(chatIdFromCallback), "This Dice Escalator game has concluded or expired. Please start a new game.", {});
        console.log(`${LOG_PREFIX} Exiting: Game not found.`);
        return;
    }
    console.log(`${LOG_PREFIX} Game ${gameId} found. Data: ${stringifyWithBigInt(gameData)}`);

    if (action === 'jackpot_display_noop') {
        console.log(`${LOG_PREFIX} Action 'jackpot_display_noop' for game ${gameId}. No operation.`);
        // Optionally answer callback query if callbackQueryId is available: await bot.answerCallbackQuery(callbackQueryId);
        return;
    }

    if (String(gameData.userId) !== String(userIdFromCallback)) {
        console.warn(`${LOG_PREFIX} User mismatch. Game ${gameId} belongs to ${gameData.userId}, action by ${userIdFromCallback}.`);
        // await safeSendMessage(String(userIdFromCallback), "This isn't your active game.", {}); // Optional
        console.log(`${LOG_PREFIX} Exiting: User mismatch.`);
        return;
    }

    if (gameData.gameMessageId && Number(gameData.gameMessageId) !== Number(originalMessageId)) {
        console.warn(`${LOG_PREFIX} Action on stale message for game ${gameId}. GameMsgID: ${gameData.gameMessageId}, ActionMsgID: ${originalMessageId}.`);
        // await safeSendMessage(String(chatIdFromCallback), "You're interacting with an outdated game message. Please use the latest one.", {}); // Optional
        console.log(`${LOG_PREFIX} Exiting: Stale message interaction.`);
        return;
    }

    console.log(`${LOG_PREFIX} Updating lastInteractionTime for game ${gameId}.`);
    gameData.lastInteractionTime = now;
    activeGames.set(gameId, gameData);

    console.log(`${LOG_PREFIX} Processing action '${action}' for game ${gameId}. Status: ${gameData.status}.`);
    const jackpotButtonText = await getJackpotButtonText();

    switch (action) {
        case 'de_roll_prompt':
            console.log(`${LOG_PREFIX} Action 'de_roll_prompt'. Validating status.`);
            if (gameData.status !== 'waiting_player_roll' && gameData.status !== 'player_turn_prompt_action') {
                console.warn(`${LOG_PREFIX} Roll attempt in invalid state '${gameData.status}' for game ${gameId}.`);
                if (gameData.status.startsWith('game_over') || gameData.status === 'bot_turn' || gameData.status === 'bot_rolling') {
                    let playAgainButtonRow = [];
                    if (gameData.status.startsWith("game_over")) { // Simplified condition
                         playAgainButtonRow = [{ text: `🎲 Play Again (${formatCurrency(Number(gameData.betAmount))})`, callback_data: `play_again_de:${gameData.betAmount}` }];
                    }
                    const keyboardForEndedGame = { inline_keyboard: [
                        [{ text: jackpotButtonText, callback_data: 'jackpot_display_noop' }],
                        playAgainButtonRow
                    ].filter(row => row.length > 0 && row.some(button => button !== undefined)) };
                    const endedMsgText = `This round of Dice Escalator (ID: ${gameId}) has already concluded, or the Bot is in play. Your final score this round was: ${escapeMarkdownV2(String(gameData.playerScore))}.`;
                    if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
                        await bot.editMessageText(endedMsgText, {
                            chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
                            parse_mode: 'MarkdownV2', reply_markup: keyboardForEndedGame
                        }).catch(e => console.error(`${LOG_PREFIX} Failed to update message for invalid roll state: ${e.message}`, e));
                    } else {
                        await safeSendMessage(String(gameData.chatId), endedMsgText, { parse_mode: 'MarkdownV2', reply_markup: keyboardForEndedGame });
                    }
                }
                console.log(`${LOG_PREFIX} Exiting 'de_roll_prompt': Invalid status.`);
                return;
            }
            console.log(`${LOG_PREFIX} Status valid for roll. Proceeding for game ${gameId}.`);
            await processDiceEscalatorPlayerRoll(gameData, jackpotButtonText);
            break;

        case 'de_cashout': // Player stands
            console.log(`${LOG_PREFIX} Action 'de_cashout'. Validating status.`);
            if (gameData.status !== 'player_turn_prompt_action') {
                console.warn(`${LOG_PREFIX} Stand attempt in invalid state '${gameData.status}' for game ${gameId}.`);
                console.log(`${LOG_PREFIX} Exiting 'de_cashout': Invalid status.`);
                return;
            }
            if (gameData.playerScore <= 0n) {
                console.warn(`${LOG_PREFIX} Stand attempt with score ${gameData.playerScore} for game ${gameId}. Not permitted.`);
                await safeSendMessage(String(gameData.chatId), `${gameData.playerRef}, you must have a score greater than zero to stand. Your current score is ${escapeMarkdownV2(String(gameData.playerScore))}.`, { parse_mode: 'MarkdownV2' });
                console.log(`${LOG_PREFIX} Exiting 'de_cashout': Non-positive score.`);
                return;
            }

            console.log(`${LOG_PREFIX} Player ${gameData.userId} stands with score ${gameData.playerScore} in game ${gameId}. Status -> 'bot_turn'.`);
            gameData.status = 'bot_turn';
            activeGames.set(gameId, gameData);

            const cashoutMessage = `${gameData.playerRef} stands firm with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*! An excellent decision.\nThe wager: ${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount)))}\n\nAll eyes on the Bot Dealer now... 🤖`;
            const cashoutKeyboard = { inline_keyboard: [
                [{ text: jackpotButtonText, callback_data: 'jackpot_display_noop' }]
            ]};
            console.log(`${LOG_PREFIX} Composed stand message for game ${gameId}.`);

            if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
                await bot.editMessageText(cashoutMessage, {
                    chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
                    parse_mode: 'MarkdownV2', reply_markup: cashoutKeyboard
                }).catch(e => {
                    console.error(`${LOG_PREFIX} Failed to edit message for stand on game ${gameId}: ${e.message}. Sending new.`, e);
                    safeSendMessage(String(gameData.chatId), cashoutMessage, { parse_mode: 'MarkdownV2', reply_markup: cashoutKeyboard });
                });
            } else {
                await safeSendMessage(String(gameData.chatId), cashoutMessage, { parse_mode: 'MarkdownV2', reply_markup: cashoutKeyboard });
            }

            console.log(`${LOG_PREFIX} Pausing 1.5s before Bot's turn for game ${gameId}.`);
            await sleep(1500);
            console.log(`${LOG_PREFIX} Initiating Bot's turn for game ${gameId}.`);
            await processDiceEscalatorBotTurn(gameData);
            break;

        default:
            console.error(`${LOG_PREFIX} Unknown action '${action}' for game ${gameId}.`);
    }
    console.log(`${LOG_PREFIX} Exiting handleDiceEscalatorPlayerAction. GameID: ${gameId}, Action: ${action}`);
}

async function processDiceEscalatorPlayerRoll(gameData, currentJackpotButtonText) {
    const LOG_PREFIX = "[processDiceEscalatorPlayerRoll]";
    console.log(`${LOG_PREFIX} Entered. GameID: ${gameData.gameId}. JackpotBtnTxt: ${currentJackpotButtonText}. Data: ${stringifyWithBigInt(gameData)}`);

    const { gameId, chatId, userId, playerRef, betAmount } = gameData;
    gameData.status = 'player_rolling';
    activeGames.set(gameId, gameData);
    console.log(`${LOG_PREFIX} Game ${gameId} status set to 'player_rolling'.`);

    const jackpotButtonTextForRoll = currentJackpotButtonText || await getJackpotButtonText();
    const rollingMessage = `The dice are tumbling for ${playerRef}! Wager: ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}\nSpinning for glory... 🎲 Current Score: *${escapeMarkdownV2(String(gameData.playerScore))}*`;
    const rollingKeyboard = { inline_keyboard: [
        [{ text: jackpotButtonTextForRoll, callback_data: 'jackpot_display_noop' }]
    ]};
    console.log(`${LOG_PREFIX} Composed 'rolling...' message for game ${gameId}.`);

    if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
        await bot.editMessageText(rollingMessage, {
            chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
            parse_mode: 'MarkdownV2', reply_markup: rollingKeyboard
        }).catch(e => console.warn(`${LOG_PREFIX} Failed to update 'rolling...' message for game ${gameId}: ${e.message}.`, e));
    } else {
        console.warn(`${LOG_PREFIX} Cannot edit 'rolling...' message (no gameMessageId or bot issue). GameID: ${gameId}`);
    }

    await sleep(500);
    let playerRollValue;
    console.log(`${LOG_PREFIX} Attempting animated dice roll for player in game ${gameId}.`);
    try {
        if (!bot || typeof bot.sendDice !== 'function') throw new Error("bot.sendDice is unavailable.");
        const diceMessage = await bot.sendDice(chatId, { emoji: '🎲' });
        if (!diceMessage || !diceMessage.dice || typeof diceMessage.dice.value === 'undefined') throw new Error("Invalid dice message from API.");
        playerRollValue = BigInt(diceMessage.dice.value);
        console.log(`${LOG_PREFIX} Player ${userId} rolled ${playerRollValue} (animated) in game ${gameId}. Pausing 2s.`);
        await sleep(2000);
    } catch (diceError) {
        console.error(`${LOG_PREFIX} Animated dice failed for game ${gameId}: ${diceError.message}. Using internal roll.`, diceError);
        playerRollValue = BigInt(rollDie());
        console.log(`${LOG_PREFIX} Player ${userId} rolled ${playerRollValue} (internal) in game ${gameId}.`);
        await safeSendMessage(chatId, `${playerRef} (internal roll): You rolled a *${escapeMarkdownV2(String(playerRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' });
        await sleep(1000);
    }

    gameData.playerRollCount += 1;
    console.log(`${LOG_PREFIX} Player roll count for game ${gameId} now ${gameData.playerRollCount}.`);

    const bustValue = typeof DICE_ESCALATOR_BUST_ON !== 'undefined' ? BigInt(DICE_ESCALATOR_BUST_ON) : 1n;
    const latestJackpotButtonText = await getJackpotButtonText();

    if (playerRollValue === bustValue) {
        console.log(`${LOG_PREFIX} Player ${userId} BUSTED in game ${gameId} with roll ${playerRollValue}.`);
        gameData.playerScore = 0n;
        gameData.status = 'game_over_player_bust';
        activeGames.set(gameId, gameData);

        await updateUserBalance(userId, 0n, `lost_dice_escalator_bust:${gameId}`, null, gameId, String(chatId));
        console.log(`${LOG_PREFIX} Loss logged for bust in game ${gameId}.`);

        const bustMessage = `A roll of *${escapeMarkdownV2(String(playerRollValue))}* for ${playerRef}... Oh, the fickle hand of fate! BUST! 💥\nYour score crumbles to 0, and the house claims your wager of ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}. Better luck next time! 😥`;
        const bustKeyboard = { inline_keyboard: [
            [{ text: latestJackpotButtonText, callback_data: 'jackpot_display_noop' }],
            [{ text: `🎲 Play Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_de:${betAmount}` }]
        ]};
        console.log(`${LOG_PREFIX} Composed bust message for game ${gameId}.`);

        if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
            await bot.editMessageText(bustMessage, {
                chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: bustKeyboard
            }).catch(async e => {
                console.error(`${LOG_PREFIX} Failed to edit bust message for game ${gameId}: ${e.message}. Sending new.`, e);
                await safeSendMessage(chatId, bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
            });
        } else {
            await safeSendMessage(chatId, bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
        }
        activeGames.delete(gameId);
        console.log(`${LOG_PREFIX} Game ${gameId} deleted due to player bust.`);
    } else {
        gameData.playerScore += playerRollValue;
        gameData.status = 'player_turn_prompt_action';
        activeGames.set(gameId, gameData);
        console.log(`${LOG_PREFIX} Successful roll ${playerRollValue} for ${userId} in game ${gameId}. New Score: ${gameData.playerScore}. Status: ${gameData.status}.`);

        const successMessage = `Superb roll, ${playerRef}! A *${escapeMarkdownV2(String(playerRollValue))}* lands! 🎉\nYour score climbs to an impressive *${escapeMarkdownV2(String(gameData.playerScore))}*.\nWager: ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}\n\nThe choice is yours: Tempt fortune with another "Roll Again", or "Stand" with your current score? 🤔`;
        const successKeyboard = {
            inline_keyboard: [
                [{ text: latestJackpotButtonText, callback_data: 'jackpot_display_noop' }],
                [{ text: "🎲 Roll Again", callback_data: `de_roll_prompt:${gameId}` },
                 { text: "💰 Stand", callback_data: `de_cashout:${gameId}` }] // STAND BUTTON TEXT UPDATED
            ]
        };
        console.log(`${LOG_PREFIX} Composed success roll message for game ${gameId}.`);

        if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
            await bot.editMessageText(successMessage, {
                chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: successKeyboard
            }).catch(async e => {
                console.error(`${LOG_PREFIX} Failed to edit success message for game ${gameId}: ${e.message}. Sending new.`, e);
                const newMsg = await safeSendMessage(chatId, successMessage, { parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
                if (newMsg && newMsg.message_id) {
                     gameData.gameMessageId = newMsg.message_id; activeGames.set(gameId, gameData);
                     console.log(`${LOG_PREFIX} Fallback message sent for success, new msgID ${newMsg.message_id}. GameID: ${gameId}`);
                }
            });
        } else {
            const newMsg = await safeSendMessage(chatId, successMessage, { parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
            if (newMsg && newMsg.message_id) {
                gameData.gameMessageId = newMsg.message_id; activeGames.set(gameId, gameData);
                console.log(`${LOG_PREFIX} Fallback message sent (no prior msgID) for success, new msgID ${newMsg.message_id}. GameID: ${gameId}`);
            }
        }
    }
    console.log(`${LOG_PREFIX} Exiting processDiceEscalatorPlayerRoll. GameID: ${gameId}. Status: ${gameData.status}, Score: ${gameData.playerScore}.`);
}

async function processDiceEscalatorBotTurn(gameData) {
    const LOG_PREFIX = "[processDiceEscalatorBotTurn]";
    console.log(`${LOG_PREFIX} Entered. GameID: ${gameData.gameId}. Data: ${stringifyWithBigInt(gameData)}`);

    const { gameId, chatId, userId, playerRef, playerScore, betAmount } = gameData;
    gameData.status = 'bot_rolling';
    gameData.botScore = 0n;
    activeGames.set(gameId, gameData);
    console.log(`${LOG_PREFIX} Game ${gameId} status 'bot_rolling', bot score reset.`);

    let jackpotButtonTextBotTurn = await getJackpotButtonText();
    let botMessageAccumulator = `${playerRef} stands with *${escapeMarkdownV2(String(playerScore))}*.\nWager: ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}\n\nThe Bot Dealer contemplates its strategy... 🤔\n`;
    const initialBotTurnKeyboard = { inline_keyboard: [
        [{ text: jackpotButtonTextBotTurn, callback_data: 'jackpot_display_noop' }]
    ]};
    console.log(`${LOG_PREFIX} Composed initial bot turn message for game ${gameId}.`);

    if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
        await bot.editMessageText(botMessageAccumulator, { // Initial message only shows thinking
            chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
            parse_mode: 'MarkdownV2', reply_markup: initialBotTurnKeyboard
        }).catch(e => console.warn(`${LOG_PREFIX} Failed to update 'bot thinking' message for game ${gameId}: ${e.message}.`, e));
    } else {
        const newMsg = await safeSendMessage(chatId, botMessageAccumulator, {parse_mode: 'MarkdownV2', reply_markup: initialBotTurnKeyboard});
        if (newMsg && newMsg.message_id) gameData.gameMessageId = newMsg.message_id; activeGames.set(gameId, gameData);
        console.warn(`${LOG_PREFIX} Sent new 'bot thinking' message (no prior msgID or edit fail). GameID: ${gameId}`);
    }
    await sleep(1500); // Pause for "thinking"

    let botRollValue;
    const botStandScore = typeof BOT_STAND_SCORE_DICE_ESCALATOR !== 'undefined' ? BigInt(BOT_STAND_SCORE_DICE_ESCALATOR) : 10n;
    const bustValueBot = typeof DICE_ESCALATOR_BUST_ON !== 'undefined' ? BigInt(DICE_ESCALATOR_BUST_ON) : 1n;
    console.log(`${LOG_PREFIX} Bot parameters for game ${gameId}: StandScore=${botStandScore}, BustValue=${bustValueBot}.`);

    do {
        jackpotButtonTextBotTurn = await getJackpotButtonText();
        const currentLoopKeyboard = { inline_keyboard: [
            [{ text: jackpotButtonTextBotTurn, callback_data: 'jackpot_display_noop' }]
        ]};
        console.log(`${LOG_PREFIX} Bot rolling for game ${gameId}. Current bot score: ${gameData.botScore}.`);
        try {
            if (!bot || typeof bot.sendDice !== 'function') throw new Error("bot.sendDice unavailable.");
            const diceMessage = await bot.sendDice(chatId, { emoji: '🎲' });
            if (!diceMessage || !diceMessage.dice || typeof diceMessage.dice.value === 'undefined') throw new Error("Invalid dice message from API.");
            botRollValue = BigInt(diceMessage.dice.value);
            console.log(`${LOG_PREFIX} Bot rolled ${botRollValue} (animated) for game ${gameId}. Pausing 2s.`);
            await sleep(2000);
        } catch (diceError) {
            console.error(`${LOG_PREFIX} Bot animated dice failed for game ${gameId}: ${diceError.message}. Using internal.`, diceError);
            botRollValue = BigInt(rollDie());
            console.log(`${LOG_PREFIX} Bot rolled ${botRollValue} (internal) for game ${gameId}.`);
            await safeSendMessage(chatId, `🤖 Bot Dealer (internal roll): A *${escapeMarkdownV2(String(botRollValue))}* appears! 🎲`, { parse_mode: 'MarkdownV2' });
            await sleep(1000);
        }

        if (botRollValue === bustValueBot) {
            gameData.botScore = 0n;
            botMessageAccumulator += `The Bot Dealer rolls a *${escapeMarkdownV2(String(botRollValue))}*... and BUSTS! 💥 The house advantage crumbles!\n`;
            console.log(`${LOG_PREFIX} Bot BUSTED with ${botRollValue} in game ${gameId}.`);
            break;
        } else {
            gameData.botScore += botRollValue;
            botMessageAccumulator += `The Bot Dealer rolls... a *${escapeMarkdownV2(String(botRollValue))}*! 🎲 Bot's score now *${escapeMarkdownV2(String(gameData.botScore))}*.\n`;
            console.log(`${LOG_PREFIX} Bot roll ${botRollValue} successful. New Bot score: ${gameData.botScore}. Game ${gameId}.`);
        }
        activeGames.set(gameId, gameData);

        if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
            let botActionFeedback = "";
            if (gameData.botScore > 0n && gameData.botScore < botStandScore) botActionFeedback = "_The Bot Dealer rolls again..._ 🎲";
            else if (gameData.botScore >= botStandScore) botActionFeedback = "_The Bot Dealer decides to stand._";
            else botActionFeedback = "_The Bot Dealer has busted!_";
            await bot.editMessageText(botMessageAccumulator + botActionFeedback, {
                chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: currentLoopKeyboard
            }).catch(e => console.warn(`${LOG_PREFIX} Failed to update bot rolling message for ${gameId}: ${e.message}.`, e));
        }
        await sleep(1500);
    } while (gameData.botScore > 0n && gameData.botScore < botStandScore);

    console.log(`${LOG_PREFIX} Bot finished rolling. Game ${gameId}. Final bot score: ${gameData.botScore}. Player score: ${playerScore}.`);

    let resultMessage;
    let payoutAmount = 0n;
    let outcomeReasonLog = "";
    let jackpotWon = false;
    const targetJackpotScoreValue = typeof TARGET_JACKPOT_SCORE !== 'undefined' && !isNaN(parseInt(TARGET_JACKPOT_SCORE)) ? BigInt(TARGET_JACKPOT_SCORE) : 99999n;
    console.log(`${LOG_PREFIX} Determining outcome. Player: ${playerScore}, Bot: ${gameData.botScore}, JackpotTarget: ${targetJackpotScoreValue}. GameID: ${gameId}`);

    if (gameData.botScore === 0n) { // Bot busted
        resultMessage = `🎉 Magnificent play, ${playerRef}! The Bot Dealer has busted. Victory is yours!`;
        payoutAmount = betAmount + betAmount;
        outcomeReasonLog = `won_dice_escalator_bot_bust:${gameId}`;
        if (playerScore >= targetJackpotScoreValue) {
            console.log(`${LOG_PREFIX} Jackpot condition met (bot bust & player score ${playerScore} >= ${targetJackpotScoreValue}). GameID: ${gameId}`);
            jackpotWon = true;
        }
    } else if (playerScore > gameData.botScore) {
        resultMessage = `🎉 Magnificent play, ${playerRef}! You WIN with a score of *${escapeMarkdownV2(String(playerScore))}* against the Bot's *${escapeMarkdownV2(String(gameData.botScore))}*!`;
        payoutAmount = betAmount + betAmount;
        outcomeReasonLog = `won_dice_escalator_score:${gameId}`;
        if (playerScore >= targetJackpotScoreValue) {
            console.log(`${LOG_PREFIX} Jackpot condition met (player score ${playerScore} > bot ${gameData.botScore} AND player score >= ${targetJackpotScoreValue}). GameID: ${gameId}`);
            jackpotWon = true;
        }
    } else if (playerScore < gameData.botScore) {
        resultMessage = `💀 The Bot Dealer prevails with *${escapeMarkdownV2(String(gameData.botScore))}* against your *${escapeMarkdownV2(String(playerScore))}*. The house takes this round.`;
        payoutAmount = 0n;
        outcomeReasonLog = `lost_dice_escalator_score:${gameId}`;
    } else { // Draw
        resultMessage = `😐 A tense standoff! Both ${playerRef} and the Bot Dealer hold a score of *${escapeMarkdownV2(String(playerScore))}*. A Push! Your wager is returned.`;
        payoutAmount = betAmount;
        outcomeReasonLog = `push_dice_escalator:${gameId}`;
    }
    console.log(`${LOG_PREFIX} Outcome for game ${gameId}: ${resultMessage}. Payout: ${payoutAmount}. Reason: ${outcomeReasonLog}. JackpotWon: ${jackpotWon}.`);

    botMessageAccumulator += `\n${resultMessage}\n`;
    gameData.status = `game_over_final_${outcomeReasonLog.split(':')[0]}`;
    activeGames.set(gameId, gameData);

    if (payoutAmount > 0n) {
        const payoutUpdateResult = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
        console.log(`${LOG_PREFIX} Payout update result for game ${gameId}: ${stringifyWithBigInt(payoutUpdateResult)}.`);
        if (payoutUpdateResult && payoutUpdateResult.success) {
             botMessageAccumulator += `Your winnings of ${escapeMarkdownV2(formatCurrency(Number(payoutAmount)))} are credited to your account! 💸`;
        } else {
            botMessageAccumulator += `There was an issue crediting your winnings of ${escapeMarkdownV2(formatCurrency(Number(payoutAmount)))}. Support has been alerted.`;
            console.error(`${LOG_PREFIX} Failed to credit winnings for user ${userId}, game ${gameId}. Error: ${payoutUpdateResult ? payoutUpdateResult.error : 'N/A'}`);
        }
    } else if (outcomeReasonLog.startsWith('lost_')) {
        await updateUserBalance(userId, 0n, outcomeReasonLog, null, gameId, String(chatId));
        botMessageAccumulator += `Your wager of ${escapeMarkdownV2(formatCurrency(Number(betAmount)))} remains with the house this time. 😥`;
        console.log(`${LOG_PREFIX} Loss processed for player ${userId}, game ${gameId}.`);
    }

    if (jackpotWon) {
        console.log(`${LOG_PREFIX} JACKPOT WIN - Processing. GameID: ${gameId}, User: ${userId}.`);
        if (!pool || typeof pool.connect !== 'function') {
            console.error(`${LOG_PREFIX} CRITICAL DB POOL ERROR during jackpot processing! GameID: ${gameId}`);
            botMessageAccumulator += `\n\n⚠️ A system error occurred with the Jackpot payout. Admin has been alerted. Your standard win is secure.`;
        } else {
            const client = await pool.connect();
            console.log(`${LOG_PREFIX} Jackpot - DB client acquired. GameID: ${gameId}`);
            try {
                await client.query('BEGIN');
                console.log(`${LOG_PREFIX} Jackpot - DB transaction started. GameID: ${gameId}`);
                const jackpotResult = await client.query('SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1 FOR UPDATE', [MAIN_JACKPOT_ID]);
                console.log(`${LOG_PREFIX} Jackpot - Select query result: ${stringifyWithBigInt(jackpotResult)}. GameID: ${gameId}`);

                if (jackpotResult.rows.length > 0) {
                    const jackpotTotalLamports = BigInt(jackpotResult.rows[0].current_amount_lamports);
                    console.log(`${LOG_PREFIX} Jackpot - Current amount: ${jackpotTotalLamports}. GameID: ${gameId}`);
                    if (jackpotTotalLamports > 0n) {
                        const jackpotPayoutResult = await updateUserBalance(userId, jackpotTotalLamports, `jackpot_win_dice_escalator:${gameId}`, client, gameId, String(chatId));
                        console.log(`${LOG_PREFIX} Jackpot - Payout (updateUserBalance) result: ${stringifyWithBigInt(jackpotPayoutResult)}. GameID: ${gameId}`);
                        if (jackpotPayoutResult.success) {
                            await client.query('UPDATE jackpot_status SET current_amount_lamports = $1, last_won_at = NOW(), last_won_by_user_id = $2, last_won_game_id = $3, updated_at = NOW() WHERE jackpot_id = $4', ['0', userId, gameId, MAIN_JACKPOT_ID]);
                            await client.query('COMMIT');
                            const jackpotWinMsgText = `\n\n👑🌟 JACKPOT VICTORY! 🌟👑\nAgainst all odds, ${playerRef}, you've hit the DICE ESCALATOR JACKPOT of *${escapeMarkdownV2(formatCurrency(Number(jackpotTotalLamports)))}*! An absolutely legendary win! Congratulations!`;
                            botMessageAccumulator += jackpotWinMsgText;
                            console.log(`${LOG_PREFIX} JACKPOT WIN SUCCESS! User ${userId} PAID ${jackpotTotalLamports}. Game ${gameId}. Jackpot reset.`);
                        } else {
                            await client.query('ROLLBACK');
                            console.error(`${LOG_PREFIX} Jackpot - Payout failed: ${jackpotPayoutResult.error}. Rolled back. GameID: ${gameId}`);
                            botMessageAccumulator += `\n\n⚠️ An issue occurred with the Jackpot payout. Support is investigating.`;
                        }
                    } else {
                        await client.query('COMMIT'); // Commit even if jackpot was 0 to release lock
                        console.log(`${LOG_PREFIX} Jackpot - Condition met, but jackpot was 0. GameID: ${gameId}`);
                    }
                } else {
                    await client.query('ROLLBACK');
                    console.error(`${LOG_PREFIX} Jackpot - ID ${MAIN_JACKPOT_ID} not found. Rolled back. GameID: ${gameId}`);
                    botMessageAccumulator += `\n\n⚠️ Jackpot system configuration error. Admin notified.`;
                }
            } catch (error) {
                console.error(`${LOG_PREFIX} Jackpot - DB error: ${error.message}. GameID: ${gameId}`, error);
                try { await client.query('ROLLBACK'); console.log(`${LOG_PREFIX} Jackpot - Rolled back due to DB error. GameID: ${gameId}`); }
                catch (rbErr) { console.error(`${LOG_PREFIX} Jackpot - Rollback failed: ${rbErr.message}`, rbErr); }
                botMessageAccumulator += `\n\n⚠️ A database error affected the Jackpot payout. Admin notified.`;
            } finally {
                if (client) client.release();
                console.log(`${LOG_PREFIX} Jackpot - DB client released. GameID: ${gameId}`);
            }
        }
    }

    const finalJackpotButtonText = await getJackpotButtonText();
    const finalKeyboard = { inline_keyboard: [
        [{ text: finalJackpotButtonText, callback_data: 'jackpot_display_noop' }],
        [{ text: `🎲 Play Dice Escalator Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_de:${betAmount}` }]
    ]};
    console.log(`${LOG_PREFIX} Composed final message for game ${gameId}. Keyboard: ${stringifyWithBigInt(finalKeyboard)}`);

    if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
        await bot.editMessageText(botMessageAccumulator, {
            chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
            parse_mode: 'MarkdownV2', reply_markup: finalKeyboard
        }).catch(async e => {
            console.error(`${LOG_PREFIX} Failed to edit final message for ${gameId}: ${e.message}. Sending new.`, e);
            await safeSendMessage(chatId, botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        });
    } else {
        await safeSendMessage(chatId, botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
    }

    activeGames.delete(gameId);
    console.log(`${LOG_PREFIX} Exiting processDiceEscalatorBotTurn. Game ${gameId} concluded & deleted. Player: ${playerScore}, Bot: ${gameData.botScore}. JackpotWon: ${jackpotWon}.`);
}

console.log("Part 5b: Dice Escalator Game Logic (Professional Casino Feel, Updated Stand Button, Jackpot Tip & DETAILED LOGGING) - Loaded and Ready.");
// --- End of Part 5b ---
// --- Start of Part 6 ---
// index.js - Part 6: Database Initialization, Startup, Shutdown, and Enhanced Error Handling
//---------------------------------------------------------------------------
console.log("Loading Part 6: Startup, Shutdown, DB Init, Enhanced Error Handling...");

// --- Database Initialization Function ---
async function initializeDatabase() {
    console.log('⚙️ [DB Init] Initializing Database Schema (if necessary)...');
    let client = null; // Use a dedicated client for the transaction
    try {
        client = await pool.connect(); // Get a client from the pool
        await client.query('BEGIN');   // Start a transaction
        console.log('⚙️ [DB Init] Transaction started for schema setup.');

        // Wallets Table
        console.log('⚙️ [DB Init] Ensuring "wallets" table exists (referral_code as VARCHAR(20))...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id VARCHAR(255) PRIMARY KEY,
                external_withdrawal_address VARCHAR(44),
                linked_at TIMESTAMPTZ,
                last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                referral_code VARCHAR(20) UNIQUE, -- Accommodates longer generated codes
                referred_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
                referral_count INTEGER NOT NULL DEFAULT 0,
                total_wagered BIGINT NOT NULL DEFAULT 0,
                last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0,
                last_bet_amounts JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                -- telegram_username and telegram_first_name columns were kept out
                -- as per user preference to not alter existing schema if these weren't there.
            );
        `);
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_total_wagered ON wallets (total_wagered DESC);');

        // User Balances Table
        console.log('⚙️ [DB Init] Ensuring "user_balances" table exists...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
                balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Bets Table (Transaction Log)
        console.log('⚙️ [DB Init] Ensuring "bets" table exists...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                chat_id VARCHAR(255) NOT NULL,
                game_type VARCHAR(50) NOT NULL,
                bet_details JSONB,
                wager_amount_lamports BIGINT NOT NULL CHECK (wager_amount_lamports >= 0),
                payout_amount_lamports BIGINT,
                status VARCHAR(50) NOT NULL DEFAULT 'active',
                reason_tx TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_user_id_game_type ON bets (user_id, game_type);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_status_created_at ON bets (status, created_at);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_bet_details_game_id ON bets ((bet_details->>\'game_id\'));');

        // DICE ROLL REQUESTS TABLE
        console.log('⚙️ [DB Init] Ensuring "dice_roll_requests" table exists with emoji_type...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS dice_roll_requests (
                request_id SERIAL PRIMARY KEY,
                game_id VARCHAR(255) NOT NULL UNIQUE,
                chat_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                roll_value INTEGER,
                emoji_type VARCHAR(10), -- <<< NEW COLUMN for specified emoji (e.g., '🎲', '🎰')
                requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring "dice_roll_requests" indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_status_requested_at ON dice_roll_requests (status, requested_at);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_emoji_type ON dice_roll_requests (emoji_type);'); // Optional index

        // JACKPOT STATUS TABLE
        console.log('⚙️ [DB Init] Ensuring "jackpot_status" table exists...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS jackpot_status (
                jackpot_id VARCHAR(50) PRIMARY KEY,
                current_amount_lamports BIGINT NOT NULL DEFAULT 0 CHECK (current_amount_lamports >= 0),
                last_won_at TIMESTAMPTZ,
                last_won_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
                last_contributed_game_id VARCHAR(255),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        // MAIN_JACKPOT_ID should be defined in Part 1
        await client.query(`
            INSERT INTO jackpot_status (jackpot_id, current_amount_lamports, updated_at)
            VALUES ($1, 0, NOW())
            ON CONFLICT (jackpot_id) DO NOTHING;
        `, [MAIN_JACKPOT_ID]);
        console.log(`⚙️ [DB Init] "jackpot_status" table ensured and initialized for ID: ${MAIN_JACKPOT_ID}.`);

        await client.query('COMMIT');
        console.log('✅ [DB Init] Database schema initialized/verified successfully.');
    } catch (err) {
        console.error('❌ CRITICAL DATABASE INITIALIZATION ERROR:', err);
        if (client) {
            try {
                await client.query('ROLLBACK');
                console.log('⚙️ [DB Init] Transaction rolled back due to schema setup error.');
            } catch (rbErr) {
                console.error('[DB Init_ERR] Rollback failed:', rbErr);
            }
        }
        if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
            safeSendMessage(ADMIN_USER_ID, `🚨 CRITICAL DB INIT FAILED: ${escapeMarkdownV2(String(err.message || err))}. Bot cannot start. Check logs.`, {parse_mode:'MarkdownV2'}).catch(() => {});
        }
        process.exit(2);
    } finally {
        if (client) {
            client.release();
            console.log('⚙️ [DB Init] Database client released.');
        }
    }
}

// --- Optional Periodic Background Tasks ---
let backgroundTaskInterval = null;
async function runPeriodicBackgroundTasks() {
    console.log(`[BACKGROUND_TASK] [${new Date().toISOString()}] Running periodic background tasks...`);
    const now = Date.now();
    const JOIN_GAME_TIMEOUT_MS_parsed = parseInt(process.env.JOIN_GAME_TIMEOUT_MS || '60000', 10); // From Part 1
    const GAME_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS_parsed * 10;
    let cleanedGames = 0;

    try {
        for (const [gameId, gameData] of activeGames.entries()) { // activeGames from Part 1
            if (!gameData || !gameData.creationTime || !gameData.status || !gameData.type || !gameData.chatId) {
                console.warn(`[BACKGROUND_TASK] Skipping corrupt game entry ID: ${gameId}`);
                activeGames.delete(gameId);
                continue;
            }
            const staleStatuses = ['waiting_opponent', 'waiting_choices', 'waiting_db_roll', 'player_turn_to_roll', 'come_out_roll', 'point_phase']; // Added more game statuses
            if ((now - gameData.creationTime > GAME_CLEANUP_THRESHOLD_MS) && staleStatuses.includes(gameData.status)) {
                console.warn(`[BACKGROUND_TASK] Cleaning stale game ${gameId} (${gameData.type}) in chat ${gameData.chatId}. Status: ${gameData.status}`);
                let refundReason = `refund_stale_${gameData.type}_timeout:${gameId}`;
                const initiatorDisp = gameData.playerReference || gameData.initiatorMention || (gameData.userId ? `User ${gameData.userId}` : (gameData.initiatorId ? `User ${gameData.initiatorId}`: 'Unknown Initiator'));
                let staleMsgText = `Game \\(ID: \`${escapeMarkdownV2(gameId)}\`\\) by ${escapeMarkdownV2(initiatorDisp)} was cleared due to inactivity`;

                // Determine the primary user ID for refund (could be initiatorId or userId)
                const primaryUserIdForRefund = gameData.initiatorId || gameData.userId;

                if (gameData.type === 'dice_escalator' && gameData.status === 'waiting_db_roll') {
                    staleMsgText += " \\(roll not processed\\)\\.";
                    if (gameData.playerScore === 0n && primaryUserIdForRefund && gameData.betAmount) {
                        await updateUserBalance(primaryUserIdForRefund, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId));
                        staleMsgText += " Bet refunded\\.";
                    } else {
                        staleMsgText += ` Last score: ${gameData.playerScore}\\.`;
                    }
                    if (typeof queryDatabase === 'function') {
                         queryDatabase("DELETE FROM dice_roll_requests WHERE game_id = $1 AND status = 'pending'", [gameId]).catch(e => console.error(`[BACKGROUND_TASK_ERR] Failed delete stale dice_roll_request ${gameId}: ${e.message}`));
                    }
                } else if (['coinflip', 'rps'].includes(gameData.type) && gameData.status === 'waiting_opponent') {
                    if (primaryUserIdForRefund && gameData.betAmount) {
                         await updateUserBalance(primaryUserIdForRefund, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId));
                         staleMsgText += "\\. Bet refunded\\.";
                    }
                } else if (gameData.type === 'rps' && gameData.status === 'waiting_choices') {
                     staleMsgText += " during choice phase\\. Bets refunded\\.";
                     if (gameData.participants && gameData.betAmount) {
                          for (const p of gameData.participants) {
                              if (p.betPlaced && p.userId) {
                                   await updateUserBalance(p.userId, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId));
                              }
                          }
                     }
                } else if (['dice21', 'overunder7', 'duel', 'sevenout', 'slot'].includes(gameData.type) && (gameData.status !== 'game_over' && gameData.status !== 'game_over_player_bust')) {
                    // For other incomplete solo games or games waiting for player action
                    staleMsgText += ` \\(game incomplete\\)\\. Bet refunded\\.`;
                    if (primaryUserIdForRefund && gameData.betAmount) {
                        await updateUserBalance(primaryUserIdForRefund, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId));
                    }
                }


                if (bot && typeof safeSendMessage === 'function' && gameData.chatId) {
                   if (gameData.gameSetupMessageId || gameData.gameMessageId) { // Check both potential message ID holders
                        const messageIdToEdit = gameData.gameSetupMessageId || gameData.gameMessageId;
                        bot.editMessageText(staleMsgText, { chatId: String(gameData.chatId), message_id: Number(messageIdToEdit), parse_mode: 'MarkdownV2', reply_markup: {} })
                          .catch(() => { safeSendMessage(String(gameData.chatId), staleMsgText, { parse_mode: 'MarkdownV2' }); });
                   } else {
                        safeSendMessage(String(gameData.chatId), staleMsgText, { parse_mode: 'MarkdownV2' });
                   }
                }
                activeGames.delete(gameId);
                cleanedGames++;
            }
        }
    } catch (loopError) {
         console.error("[BACKGROUND_TASK_ERR] Error during stale game cleanup loop:", loopError);
    }
    if (cleanedGames > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedGames} stale game(s).`);

    const SESSION_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS_parsed * 30;
    let cleanedSessions = 0;
    try {
        for (const [chatId, sessionData] of groupGameSessions.entries()) { // groupGameSessions from Part 2
            if (sessionData && !sessionData.currentGameId && sessionData.lastActivity instanceof Date && (now - sessionData.lastActivity.getTime()) > SESSION_CLEANUP_THRESHOLD_MS) {
                console.log(`[BACKGROUND_TASK] Cleaning inactive group session for chat ${chatId}.`);
                groupGameSessions.delete(chatId);
                cleanedSessions++;
            }
        }
    } catch (loopError) {
        console.error("[BACKGROUND_TASK_ERR] Error during inactive session cleanup loop:", loopError);
    }
    if (cleanedSessions > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedSessions} inactive group session entries.`);
    console.log(`[BACKGROUND_TASK] Finished. Active games: ${activeGames.size}, Group sessions: ${groupGameSessions.size}.`);
}


// --- Telegram Polling Retry Logic ---
let isRetryingPolling = false;
let retryPollingDelay = 5000; // From Part 1 or defined here
const MAX_RETRY_POLLING_DELAY = 60000; // From Part 1 or defined here
let pollingRetryTimeoutId = null;

async function attemptRestartPolling(error) {
    if (isShuttingDown) { /* ... */ return; } // isShuttingDown from Part 1
    if (isRetryingPolling) { /* ... */ return; }
    isRetryingPolling = true;
    clearTimeout(pollingRetryTimeoutId);
    console.warn(`[POLLING_RETRY] Polling error: ${error.code || 'N/A'} - ${error.message}. Restarting in ${retryPollingDelay / 1000}s...`);
    try { if (bot?.isPolling?.()) await bot.stopPolling({ cancel: true }); } catch (stopErr) { /* ... */ }

    pollingRetryTimeoutId = setTimeout(async () => {
         if (isShuttingDown) { /* ... */ isRetryingPolling = false; return; }
         if (!bot || typeof bot.startPolling !== 'function') { /* ... */ isRetryingPolling = false; return; }
        try {
            await bot.startPolling();
            console.log('✅ [POLLING_RETRY] Polling successfully restarted!');
            retryPollingDelay = 5000; isRetryingPolling = false;
        } catch (startErr) {
            console.error(`❌ [POLLING_RETRY] Failed to restart polling: ${startErr.code || 'N/A'} - ${startErr.message}`);
            retryPollingDelay = Math.min(retryPollingDelay * 2, MAX_RETRY_POLLING_DELAY);
            isRetryingPolling = false;
            console.warn(`[POLLING_RETRY] Next retry: ${retryPollingDelay / 1000}s on next 'polling_error'.`);
            if (ADMIN_USER_ID && retryPollingDelay >= MAX_RETRY_POLLING_DELAY && typeof safeSendMessage === "function") {
                safeSendMessage(ADMIN_USER_ID, `🚨 BOT ALERT: Failed to restart polling repeatedly. Last: ${escapeMarkdownV2(startErr.message)}. Intervention needed.`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
            }
             if (typeof shutdown === "function" && (String(startErr.message).includes('409') || String(startErr.code).includes('EFATAL'))) {
                  console.error("FATAL error during polling restart. Shutting down.");
                  if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
                       safeSendMessage(ADMIN_USER_ID, `🚨 BOT SHUTDOWN: Fatal error during polling restart. Error: ${escapeMarkdownV2(startErr.message)}.`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
                  }
                  shutdown('POLLING_RESTART_FATAL').catch(() => process.exit(1));
             }
        }
    }, retryPollingDelay);
    if(pollingRetryTimeoutId?.unref) pollingRetryTimeoutId.unref();
}

// --- The Core Shutdown Function (Enhanced Version) ---
async function shutdown(signal) {
    if (isShuttingDown) { /* ... */ return; } // isShuttingDown from Part 1
    isShuttingDown = true;
    console.warn(`\n🚦 Received signal: ${signal}. Initiating graceful shutdown... (PID: ${process.pid})`);
    clearTimeout(pollingRetryTimeoutId); isRetryingPolling = false;

    if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function" && typeof BOT_VERSION !== 'undefined') {
        await safeSendMessage(ADMIN_USER_ID, `ℹ️ Bot v${BOT_VERSION} shutting down (Signal: ${escapeMarkdownV2(String(signal))})...`, { parse_mode: 'MarkdownV2' }).catch(()=>{});
    }

    console.log("🚦 [Shutdown] Stopping Telegram updates..."); // bot from Part 1
    if (bot?.isPolling?.()) { await bot.stopPolling({ cancel: true }).then(() => console.log("✅ [Shutdown] Polling stopped.")).catch(e => console.error("❌ Error stopping polling:", e.message)); }
    else if (bot && typeof bot.deleteWebHook === 'function' && !bot.options.polling) { await bot.deleteWebHook({ drop_pending_updates: false }).then(() => console.log("✅ Webhook deleted.")).catch(e => console.warn(`⚠️ Error deleting webhook: ${e.message}`));}
    else { console.log("ℹ️ [Shutdown] Telegram bot N/A or not polling/webhook."); }

    console.log("🚦 [Shutdown] Stopping background intervals...");
    if (backgroundTaskInterval) clearInterval(backgroundTaskInterval); backgroundTaskInterval = null;
    console.log("✅ [Shutdown] Background intervals cleared.");

    console.log("ℹ️ [Shutdown] No dedicated message queues to wait for.");
    // await sleep(1000); // sleep from Part 1

    console.log("🚦 [Shutdown] Closing Database pool..."); // pool from Part 1
    if (pool && typeof pool.end === 'function') { await pool.end().then(() => console.log("✅ DB pool closed.")).catch(e => console.error("❌ Error closing DB pool:", e.message));}
    else { console.log("ℹ️ [Shutdown] DB pool N/A or closed."); }

    console.log(`🏁 [Shutdown] Graceful shutdown complete (Signal: ${signal}). Exiting.`);
    const exitCode = (signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
    process.exit(exitCode);
}

// Watchdog timer to force exit if shutdown hangs
function startShutdownWatchdog(signal) {
     // SHUTDOWN_FAIL_TIMEOUT_MS from Part 1
     const timeoutMs = typeof SHUTDOWN_FAIL_TIMEOUT_MS === 'number' ? SHUTDOWN_FAIL_TIMEOUT_MS : 10000;
     const timerId = setTimeout(() => {
          console.error(`🚨 Forcing exit after ${timeoutMs}ms due to hanging shutdown (Signal: ${signal}).`);
          process.exit(1);
     }, timeoutMs);
     if (timerId?.unref) timerId.unref();
}

// --- Main Startup Function ---
async function main() {
    // BOT_VERSION from Part 1
    console.log(`\n🚀🚀🚀 Initializing Group Chat Casino Bot v${BOT_VERSION} 🚀🚀🚀`);
    console.log(`Timestamp: ${new Date().toISOString()}`);
    console.log(`PID: ${process.pid}`);

    console.log("⚙️ [Startup] Setting up process signal & error handlers...");
    process.on('SIGINT', () => { if (!isShuttingDown) startShutdownWatchdog('SIGINT'); shutdown('SIGINT'); });
    process.on('SIGTERM', () => { if (!isShuttingDown) startShutdownWatchdog('SIGTERM'); shutdown('SIGTERM'); });
    process.on('uncaughtException', async (error, origin) => {
        console.error(`\n🚨🚨🚨 UNCAUGHT EXCEPTION [Origin: ${origin}] 🚨🚨🚨\n`, error);
        if (!isShuttingDown) {
            if (ADMIN_USER_ID && typeof safeSendMessage === "function") await safeSendMessage(ADMIN_USER_ID, `🚨🚨 UNCAUGHT EXCEPTION (${escapeMarkdownV2(String(origin))})\n${escapeMarkdownV2(String(error.message || error))}\nAttempting shutdown...`, { parse_mode: 'MarkdownV2' }).catch(()=>{});
            startShutdownWatchdog('uncaughtException'); shutdown('uncaughtException').catch(() => process.exit(1));
        } else console.warn("Uncaught exception during shutdown. Forcing exit soon.");
    });
    process.on('unhandledRejection', async (reason, promise) => {
        console.error('\n🔥🔥🔥 UNHANDLED PROMISE REJECTION 🔥🔥🔥\nReason:', reason);
        if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
            const reasonMsg = reason instanceof Error ? reason.message : String(reason);
            const stack = reason instanceof Error ? `\nStack: ${reason.stack}` : '';
            await safeSendMessage(ADMIN_USER_ID, `🔥🔥 UNHANDLED REJECTION\nReason: ${escapeMarkdownV2(reasonMsg)}${escapeMarkdownV2(stack)}`, { parse_mode: 'MarkdownV2' }).catch(()=>{});
        }
    });
    console.log("✅ [Startup] Process handlers set up.");

    await initializeDatabase();
    console.log("Main Bot: Database initialization sequence completed.");

    try {
        console.log("Main Bot: Connecting to Telegram and setting up listeners...");
        if (bot && typeof bot.getMe === 'function') {
            bot.on('polling_error', async (error) => {
                 console.error(`\n🚫 TG POLLING ERROR 🚫 Code: ${error.code || 'N/A'} | Msg: ${error.message}`);
                 if (String(error.message).includes('409 Conflict')) {
                      console.error("FATAL: 409 Conflict. Another instance? Shutting down THIS instance.");
                       if (ADMIN_USER_ID && typeof safeSendMessage === "function") await safeSendMessage(ADMIN_USER_ID, `🚨 BOT CONFLICT (409) on ${process.env.HOSTNAME || 'local'}. Shutting down. Ensure only one instance. Error: ${escapeMarkdownV2(String(error.message || error))}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
                       if (!isShuttingDown) { startShutdownWatchdog('POLLING_409_ERROR'); shutdown('POLLING_409_ERROR').catch(() => process.exit(1));}
                 } else if (String(error.code).includes('EFATAL')) {
                      console.error("FATAL POLLING ERROR (EFATAL). Shutting down.", error);
                       if (ADMIN_USER_ID && typeof safeSendMessage === "function") await safeSendMessage(ADMIN_USER_ID, `🚨 BOT FATAL ERROR (EFATAL). Shutting down. Error: ${escapeMarkdownV2(error.message)}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
                       if (!isShuttingDown) { startShutdownWatchdog('POLLING_FATAL_ERROR'); shutdown('POLLING_FATAL_ERROR').catch(() => process.exit(1));}
                 } else { if (typeof attemptRestartPolling === 'function') attemptRestartPolling(error); }
            });
            bot.on('error', async (error) => {
                 console.error('\n🔥 TG LIBRARY ERROR EVENT 🔥:', error);
                  if (ADMIN_USER_ID && typeof safeSendMessage === "function") await safeSendMessage(ADMIN_USER_ID, `⚠️ BOT LIBRARY ERROR\n${escapeMarkdownV2(error.message || String(error))}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
            });
            console.log("✅ [Startup] Telegram event listeners attached.");

            const me = await bot.getMe();
            console.log(`✅ Connected to Telegram! Bot: @${me.username} (ID: ${me.id})`);
            if (ADMIN_USER_ID && typeof safeSendMessage === 'function') {
                await safeSendMessage(ADMIN_USER_ID, `🎉 Bot v${BOT_VERSION} started! PID: ${process.pid}. Host: ${process.env.HOSTNAME || 'local'}. Polling active.`, { parse_mode: 'MarkdownV2' });
            }
            console.log(`\n🎉 Main Bot operational! Waiting for messages...`);
            // Optional: Start periodic background tasks
            // if (typeof runPeriodicBackgroundTasks === 'function') {
            //     backgroundTaskInterval = setInterval(runPeriodicBackgroundTasks, 15 * 60 * 1000);
            //     console.log("ℹ️ [Startup] Periodic background tasks scheduled.");
            // }
        } else {
             throw new Error("Telegram bot instance ('bot') or 'bot.getMe' failed to initialize.");
        }
    } catch (error) {
        console.error("❌ CRITICAL STARTUP ERROR (Main Bot: Connection/Listener Setup):", error);
        if (ADMIN_USER_ID && BOT_TOKEN && typeof escapeMarkdownV2 === 'function' && typeof TelegramBot !== 'undefined') {
            try {
                const tempBot = new TelegramBot(BOT_TOKEN, {});
                await tempBot.sendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE Bot v${BOT_VERSION}:\n${escapeMarkdownV2(error.message)}\nBot is exiting. Check logs.`, {parse_mode:'MarkdownV2'}).catch(() => {});
            } catch (tempBotError) { console.error("Main Bot: Failed temp bot notification:", tempBotError); }
        }
        if (!isShuttingDown && typeof startShutdownWatchdog === 'function') {
            startShutdownWatchdog('STARTUP_FAILURE'); process.exit(1);
        } else if (!isShuttingDown) { console.error("STARTUP_FAILURE & watchdog N/A. Forcing exit."); process.exit(1); }
    }
}

// --- Final Execution: Start the Bot ---
main().catch(error => {
    console.error("❌ MAIN ASYNC FUNCTION UNHANDLED ERROR:", error);
    if(typeof startShutdownWatchdog === 'function') startShutdownWatchdog('MAIN_CATCH');
    else console.error("Watchdog N/A, forcing exit from main catch.");
    process.exit(1);
});

console.log("Main Bot: End of index.js script. Bot startup process initiated.");
// --- End of Part 6 ---
