// --- Start of Part 1 ---
// index.js - Part 1: Core Imports, Basic Setup, Global State & Utilities
//---------------------------------------------------------------------------

// ESM-style imports
import 'dotenv/config';
import TelegramBot from 'node-telegram-bot-api';
import { Pool } from 'pg'; // For PostgreSQL

console.log("Loading Part 1: Core Imports, Basic Setup, Global State & Utilities...");

//---------------------------------------------------------------------------
// index.js - Part 1: Core Imports & Basic Setup
//---------------------------------------------------------------------------

// --- Environment Variable Validation & Defaults ---
const OPTIONAL_ENV_DEFAULTS = {
    'DB_POOL_MAX': '25',
    'DB_POOL_MIN': '5',
    'DB_IDLE_TIMEOUT': '30000', // in milliseconds
    'DB_CONN_TIMEOUT': '5000', // in milliseconds
    'DB_SSL': 'true', // Default to SSL enabled
    'DB_REJECT_UNAUTHORIZED': 'false', // Default to false
    'SHUTDOWN_FAIL_TIMEOUT_MS': '10000', // Timeout to force exit if shutdown hangs (e.g., 10s)
    'JACKPOT_CONTRIBUTION_PERCENT': '0.01', // Default 1% contribution to jackpot from bets
    'MIN_BET_AMOUNT': '5', // Default min bet for games
    'MAX_BET_AMOUNT': '1000', // Default max bet for games
    'COMMAND_COOLDOWN_MS': '2000', // Default command cooldown for users
    'JOIN_GAME_TIMEOUT_MS': '60000', // Default timeout for games waiting for players
    'DEFAULT_STARTING_BALANCE_LAMPORTS': '100000000' // Default starting balance for new users
};

// Apply defaults if not set in process.env
Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
    if (process.env[key] === undefined) {
        console.log(`[ENV_DEFAULT] Setting default for ${key}: ${defaultValue}`);
        process.env[key] = defaultValue;
    }
});

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_USER_ID = process.env.ADMIN_USER_ID;
const DATABASE_URL = process.env.DATABASE_URL;
const SHUTDOWN_FAIL_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
const JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.JACKPOT_CONTRIBUTION_PERCENT);
const MAIN_JACKPOT_ID = 'dice_escalator_main'; // ID for the primary Dice Escalator jackpot

// --- Crucial Game Play Constants ---
const MIN_BET_AMOUNT = parseInt(process.env.MIN_BET_AMOUNT, 10);
const MAX_BET_AMOUNT = parseInt(process.env.MAX_BET_AMOUNT, 10);
const COMMAND_COOLDOWN_MS = parseInt(process.env.COMMAND_COOLDOWN_MS, 10);
const JOIN_GAME_TIMEOUT_MS = parseInt(process.env.JOIN_GAME_TIMEOUT_MS, 10);
// DEFAULT_STARTING_BALANCE_LAMPORTS is defined in Part 2 where it's first used,
// or can be defined here if needed more globally earlier.
// For consistency with Part 2's usage, let's ensure it's parsed here too.
// const DEFAULT_STARTING_BALANCE_LAMPORTS = BigInt(process.env.DEFAULT_STARTING_BALANCE_LAMPORTS);
// This BigInt version is used in Part 2. For Part 1, if any non-BigInt use, keep as Number or ensure context.
// Since no direct use in Part 1, the definition in Part 2 with BigInt() is fine.

if (!BOT_TOKEN) {
    console.error("FATAL ERROR: BOT_TOKEN is not defined.");
    process.exit(1);
}
if (!DATABASE_URL) {
    console.error("FATAL ERROR: DATABASE_URL is not defined. Cannot connect to PostgreSQL.");
    process.exit(1);
}

console.log("BOT_TOKEN loaded successfully.");
if (ADMIN_USER_ID) console.log(`Admin User ID: ${ADMIN_USER_ID} loaded.`);
else console.log("INFO: No ADMIN_USER_ID set (optional).");
console.log(`Jackpot Contribution Percent: ${JACKPOT_CONTRIBUTION_PERCENT * 100}%`);
console.log(`Bet Limits: ${MIN_BET_AMOUNT} - ${MAX_BET_AMOUNT}`);
console.log(`Command Cooldown: ${COMMAND_COOLDOWN_MS}ms`);
console.log(`Join Game Timeout: ${JOIN_GAME_TIMEOUT_MS}ms`);


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
pool.on('acquire', client => {
    // console.log('ℹ️ [DB Pool] Client acquired from pool.'); // Can be verbose
});
pool.on('remove', client => {
    // console.log('ℹ️ [DB Pool] Client removed from pool.'); // Can be verbose
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

const BOT_VERSION = '2.3.0-multi-game-final'; // Updated version marker
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
    if (!chatId || typeof text !== 'string') {
        console.error("[safeSendMessage] Invalid input:", { chatId, textPreview: String(text).substring(0, 50) });
        return undefined;
    }

    let messageToSend = text;
    let finalOptions = { ...options };

    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsis = "... (message truncated)";
        const truncateAt = MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsis.length;
        messageToSend = (truncateAt > 0) ? messageToSend.substring(0, truncateAt) + ellipsis : messageToSend.substring(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH);
        console.warn(`[safeSendMessage] Message for chat ${chatId} was truncated before potential escaping.`);
    }

    if (finalOptions.parse_mode === 'MarkdownV2') {
        messageToSend = escapeMarkdownV2(messageToSend);
        if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
             console.warn(`[safeSendMessage] Message for chat ${chatId} might still exceed length limit after escaping. Sending anyway.`);
        }
    }

    if (!bot) {
         console.error("[safeSendMessage] Error: Telegram 'bot' instance is not available.");
         return undefined;
    }

    try {
        if (typeof bot.sendMessage !== 'function') {
             throw new Error("bot.sendMessage is not available.");
        }
        const sentMessage = await bot.sendMessage(chatId, messageToSend, finalOptions);
        return sentMessage;
    } catch (error) {
        console.error(`[safeSendMessage] Failed to send to chat ${chatId}. Code: ${error.code || 'N/A'}, Msg: ${error.message}`);
        if (error.response?.body) {
            console.error(`[safeSendMessage] API Response: ${JSON.stringify(error.response.body)}`);
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
// index.js - Part 2: Database Operations & Data Management (DATABASE BACKED)
//---------------------------------------------------------------------------
console.log("Loading Part 2: Database Operations & Data Management (DATABASE BACKED)...");

// In-memory stores for non-critical/session data
const groupGameSessions = new Map(); // Tracks active game per group (can be re-evaluated if needed)
// userDatabase (in-memory map for balances) is NOW OBSOLETE. Balances are in PostgreSQL.
console.log("In-memory data stores (groupGameSessions) initialized.");


// --- queryDatabase Helper Function ---
// (Ensure 'pool' is defined in Part 1)
async function queryDatabase(sql, params = [], dbClient = pool) {
    if (!dbClient) {
        const poolError = new Error("Database pool/client is not available for queryDatabase");
        console.error("❌ CRITICAL: queryDatabase called but dbClient is invalid!", poolError.stack);
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
        const safeParamsString = JSON.stringify(params, (key, value) =>
            typeof value === 'bigint' ? value.toString() + 'n' : value
        );
        console.error(`   Params: ${safeParamsString}`);
        console.error(`   Error Code: ${error.code || 'N/A'}`);
        console.error(`   Error Message: ${error.message}`);
        if (error.constraint) { console.error(`   Constraint Violated: ${error.constraint}`); }
        throw error;
    }
}

// --- User and Balance Functions (DATABASE BACKED) ---

// Constants for new user default balance (ensure it's defined, e.g., in Part 1 or here)
// This should be in the smallest unit of your currency (e.g., Lamports for SOL).
const DEFAULT_STARTING_BALANCE_LAMPORTS = BigInt(process.env.DEFAULT_STARTING_BALANCE_LAMPORTS || '100000000'); // Example: 100,000,000 units

// Constants for jackpot (ensure they are defined, e.g., in Part 1)
// const JACKPOT_CONTRIBUTION_PERCENT = 0.01; // Example: 1%
// const MAIN_JACKPOT_ID = 'dice_escalator_main';


// Gets user data (from DB), creates if doesn't exist.
// Returns user object including balance.
// Does NOT expect telegram_username or telegram_first_name columns in wallets table.
async function getUser(userId, username = null, firstName = null) {
    const userIdStr = String(userId);
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // Try to get wallet/user first (select only columns that exist)
        let userResult = await client.query(
            'SELECT user_id, referral_code, last_used_at FROM wallets WHERE user_id = $1', // Removed telegram_username, telegram_first_name
            [userIdStr]
        );
        let userWalletData; // Will hold data from the 'wallets' table
        let isNewUser = false;
        let actualBalanceLamports;

        if (userResult.rows.length === 0) {
            // User does not exist in wallets, create them
            const newReferralCode = `ref${Date.now().toString(36)}${Math.random().toString(36).substring(2, 5)}`;
            const insertWalletQuery = `
                INSERT INTO wallets (user_id, referral_code, created_at, last_used_at) 
                VALUES ($1, $2, NOW(), NOW())
                RETURNING user_id, referral_code, last_used_at; 
            `; // Does not insert telegram_username/first_name
            userResult = await client.query(insertWalletQuery, [userIdStr, newReferralCode]);
            userWalletData = userResult.rows[0];
            isNewUser = true;
            console.log(`[DB_USER] New user wallet created for ${userIdStr}.`);

            const insertBalanceQuery = `
                INSERT INTO user_balances (user_id, balance_lamports, updated_at)
                VALUES ($1, $2, NOW());
            `;
            await client.query(insertBalanceQuery, [userIdStr, DEFAULT_STARTING_BALANCE_LAMPORTS.toString()]);
            console.log(`[DB_USER] Initial balance set for new user ${userIdStr}.`);
            actualBalanceLamports = DEFAULT_STARTING_BALANCE_LAMPORTS;
        } else {
            userWalletData = userResult.rows[0];
            // Update last_used_at. No other details to update in wallets table based on this function's inputs.
            await client.query('UPDATE wallets SET last_used_at = NOW() WHERE user_id = $1', [userIdStr]);

            const balanceResult = await client.query('SELECT balance_lamports FROM user_balances WHERE user_id = $1', [userIdStr]);
            if (balanceResult.rows.length === 0) {
                console.warn(`[DB_USER_WARN] Wallet exists for ${userIdStr} but no balance record. Creating with default.`);
                await client.query('INSERT INTO user_balances (user_id, balance_lamports, updated_at) VALUES ($1, $2, NOW())', [userIdStr, DEFAULT_STARTING_BALANCE_LAMPORTS.toString()]);
                actualBalanceLamports = DEFAULT_STARTING_BALANCE_LAMPORTS;
            } else {
                actualBalanceLamports = BigInt(balanceResult.rows[0].balance_lamports);
            }
        }

        await client.query('COMMIT');

        return {
            userId: userWalletData.user_id,
            username: username || `User_${userIdStr}`, // Use provided username from Telegram, or fallback
            firstName: firstName, // Use provided first name from Telegram (can be null)
            balance: Number(actualBalanceLamports), // For game logic convenience
            balanceLamports: actualBalanceLamports, // For precise operations
            isNew: isNewUser,
            referral_code: userWalletData.referral_code,
            groupStats: new Map(), // Placeholder for now
        };

    } catch (error) {
        await client.query('ROLLBACK');
        console.error(`[DB_GET_USER_ERR] Error fetching/creating user ${userIdStr}:`, error);
        throw error;
    } finally {
        client.release();
    }
}

// Updates user balance IN THE DATABASE transactionally.
async function updateUserBalance(userId, amountChangeLamports, reason = "unknown_transaction", client_ = null, associatedGameId = null, chatIdForLog = null) {
    const userIdStr = String(userId);
    const operationClient = client_ || await pool.connect();

    try {
        if (!client_) await operationClient.query('BEGIN');

        const balanceSelectRes = await operationClient.query(
            'SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE',
            [userIdStr]
        );

        if (balanceSelectRes.rows.length === 0) {
            if (!client_) await operationClient.query('ROLLBACK');
            console.warn(`[DB_BALANCE_ERR] Update balance called for non-existent user balance record: ${userIdStr}.`);
            // It's better if getUser is always called before any operation that assumes a user exists.
            // Trying to create the user here can complicate transaction management.
            return { success: false, error: "User balance record not found. Ensure user exists via getUser first." };
        }

        const currentBalanceLamports = BigInt(balanceSelectRes.rows[0].balance_lamports);
        const change = BigInt(amountChangeLamports);
        let proposedBalanceLamports = currentBalanceLamports + change;

        let jackpotContribution = 0n; // BigInt for consistency
        // Ensure JACKPOT_CONTRIBUTION_PERCENT and MAIN_JACKPOT_ID are defined (e.g., in Part 1)
        if (reason.startsWith('bet_placed_dice_escalator') && change < 0n && associatedGameId && typeof JACKPOT_CONTRIBUTION_PERCENT === 'number' && MAIN_JACKPOT_ID) {
            const betAmount = -change;
            jackpotContribution = betAmount * BigInt(Math.round(JACKPOT_CONTRIBUTION_PERCENT * 10000)) / 10000n; // More precision for percent

            if (jackpotContribution > 0n) {
                await operationClient.query(
                    'UPDATE jackpot_status SET current_amount_lamports = current_amount_lamports + $1, updated_at = NOW(), last_contributed_game_id = $2 WHERE jackpot_id = $3',
                    [jackpotContribution.toString(), associatedGameId, MAIN_JACKPOT_ID]
                );
                console.log(`[JACKPOT] Contributed ${jackpotContribution} to ${MAIN_JACKPOT_ID} from game ${associatedGameId}.`);
            }
        }

        if (proposedBalanceLamports < 0n) {
            if (!client_) await operationClient.query('ROLLBACK');
            console.log(`[DB_BALANCE] User ${userIdStr} insufficient balance (${currentBalanceLamports}) for change of ${change}. Reason: ${reason}`);
            return { success: false, error: "Insufficient balance.", currentBalance: Number(currentBalanceLamports) };
        }

        await operationClient.query(
            'UPDATE user_balances SET balance_lamports = $1, updated_at = NOW() WHERE user_id = $2',
            [proposedBalanceLamports.toString(), userIdStr]
        );
        
        const betDetails = { game_id: associatedGameId };
        let wagerAmountForLog = 0n;
        if (change < 0n) wagerAmountForLog = -change; // Log wager as positive

        if (reason.startsWith('bet_placed_') && wagerAmountForLog > 0n && associatedGameId && chatIdForLog) {
            const gameTypeFromReason = reason.substring('bet_placed_'.length).split(':')[0];
            if (jackpotContribution > 0n) {
                betDetails.jackpot_contribution = jackpotContribution.toString();
            }
            await operationClient.query(
                `INSERT INTO bets (user_id, chat_id, game_type, wager_amount_lamports, bet_details, status, reason_tx, created_at, processed_at)
                 VALUES ($1, $2, $3, $4, $5, 'active', $6, NOW(), NOW())`,
                [userIdStr, String(chatIdForLog), gameTypeFromReason, wagerAmountForLog.toString(), betDetails, reason]
            );
        } else if (associatedGameId && (reason.startsWith('won_') || reason.startsWith('lost_') || reason.startsWith('push_') || reason.startsWith('cashout_') || reason.startsWith('refund_') || reason.startsWith('jackpot_win'))) {
            let statusForLog = reason.split(':')[0]; 
            let payoutAmountForLog = change; 
            
            if (statusForLog.startsWith("lost_")) {
                payoutAmountForLog = 0n; 
            } else if (statusForLog.startsWith("push_")){
                // For push, 'change' is the bet amount returned.
            } else if (statusForLog.startsWith("won_") || statusForLog.startsWith("cashout_")) {
                // For wins/cashouts, 'change' is the total amount credited (bet_back + profit)
            }

            if (statusForLog === "jackpot_win_de"){ // Specific reason for jackpot direct payout
                 betDetails.jackpot_won = change.toString(); 
                 statusForLog = 'jackpot_won_direct'; // Log as a direct jackpot payout type
                 // This assumes jackpot payout is a separate transaction in bets table.
                 // Or, it could update the original bet record.
                 await operationClient.query(
                     `INSERT INTO bets (user_id, chat_id, game_type, wager_amount_lamports, payout_amount_lamports, bet_details, status, reason_tx, created_at, processed_at)
                      VALUES ($1, $2, 'jackpot', '0', $3, $4, 'processed', $5, NOW(), NOW())`,
                     [userIdStr, String(chatIdForLog || 'N/A'), payoutAmountForLog.toString(), betDetails, statusForLog, reason]
                 );
            } else { // For game outcomes updating an existing bet record
                 await operationClient.query(
                     `UPDATE bets SET status = $1, payout_amount_lamports = $2, reason_tx = $3, processed_at = NOW() 
                      WHERE user_id = $4 AND bet_details->>'game_id' = $5 AND status = 'active'`,
                      [statusForLog, payoutAmountForLog.toString(), reason, userIdStr, associatedGameId]
                 );
            }
        }

        if (!client_) await operationClient.query('COMMIT');
        console.log(`[DB_BALANCE] User ${userIdStr} balance updated to: ${proposedBalanceLamports} (Change: ${change}, Reason: ${reason}, Game: ${associatedGameId || 'N/A'})`);
        
        return { 
            success: true, 
            newBalance: Number(proposedBalanceLamports),
            newBalanceLamports: proposedBalanceLamports 
        };

    } catch (error) {
        if (!client_) {
            try { await operationClient.query('ROLLBACK'); } catch (rbErr) { console.error("[DB_BALANCE_ERR] Rollback failed:", rbErr); }
        }
        console.error(`[DB_BALANCE_ERR] Error updating balance for user ${userIdStr} (Reason: ${reason}, Game: ${associatedGameId || 'N/A'}):`, error);
        return { success: false, error: `Database error: ${error.message}` };
    } finally {
        if (!client_ && operationClient) operationClient.release();
    }
}


// --- Group Session Functions (In-Memory) ---
async function getGroupSession(chatId, chatTitle) {
    const chatIdStr = String(chatId);
    if (!groupGameSessions.has(chatIdStr)) {
        const newSession = {
            chatId: chatIdStr,
            chatTitle: chatTitle || `Group_${chatIdStr}`,
            // These fields are mainly for single-instance-per-chat games like Coinflip/RPS
            currentGameId: null, 
            currentGameType: null, 
            currentBetAmount: null, 
            lastActivity: new Date(), 
        };
        groupGameSessions.set(chatIdStr, newSession);
        console.log(`[IN_MEM_SESS] New group session created: ${chatIdStr} (${newSession.chatTitle}).`);
        return { ...newSession };
    }
    const session = groupGameSessions.get(chatIdStr);
    if (chatTitle && session.chatTitle !== chatTitle) {
           session.chatTitle = chatTitle;
           console.log(`[IN_MEM_SESS] Updated title for session ${chatIdStr} to "${chatTitle}"`);
    }
    session.lastActivity = new Date();
    return { ...session };
}

async function updateGroupGameDetails(chatId, gameId, gameType, betAmount) {
    const chatIdStr = String(chatId);
    // Ensure session exists, creating if necessary
    const session = await getGroupSession(chatIdStr, null); // getGroupSession handles creation

    if (!session) { // Should not happen if getGroupSession works correctly
       console.error(`[IN_MEM_SESS_ERROR] Failed to get/create session for ${chatIdStr} during game details update.`);
       return false;
    }

    // Only update if gameType is not DiceEscalator (as DiceEscalator allows multiple games)
    // Or if clearing details (gameId is null)
    if (gameType !== 'DiceEscalator' || gameId === null) {
        session.currentGameId = gameId;
        session.currentGameType = gameType;
        session.currentBetAmount = betAmount;
         // Persist the change back to the map (since getGroupSession returns a copy)
         groupGameSessions.set(chatIdStr, { ...session });
    }
    // Update lastActivity regardless
    session.lastActivity = new Date();
    groupGameSessions.set(chatIdStr, { ...session });


    // Ensure formatCurrency exists (Part 3) for logging
    const betDisplay = (betAmount !== null && betAmount !== undefined && typeof formatCurrency === 'function') 
                       ? formatCurrency(betAmount) 
                       : (betAmount !== null && betAmount !== undefined ? `${betAmount} credits` : 'N/A');

    console.log(`[IN_MEM_SESS] Group ${chatIdStr} details updated. Active Game for Coinflip/RPS: ID: ${session.currentGameId || 'None'}, Type: ${session.currentGameType || 'None'}, Bet: ${betDisplay}`);
    return true;
}

console.log("Part 2: Database Operations & Data Management (DATABASE BACKED) - Complete.");
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

// --- DICE 21 GAME HANDLER FUNCTIONS ---
async function handleStartDice21Command(chatId, userObj, betAmount, commandMessageId) {
    const userId = String(userObj.id);
    const playerRef = getPlayerDisplayReference(userObj);
    const gameId = generateGameId();

    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_dice21:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${playerRef}, bet failed for Dice 21: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    await safeSendMessage(chatId, `🎲 ${playerRef} starts Dice 21! Bet: ${formatCurrency(betAmount)}. Dealing first two dice...`, { parse_mode: 'MarkdownV2' });
    await sleep(1000); 

    let initialRollsValues = [];
    let playerScore = 0n;
    for (let i = 0; i < 2; i++) {
        try {
            const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
            initialRollsValues.push(diceMsg.dice.value);
            playerScore += BigInt(diceMsg.dice.value);
            await sleep(2000); 
        } catch (diceError) {
            console.error(`[D21_START_ERR] Error sending dice animation, using internal roll: ${diceError.message}`);
            const internalRoll = rollDie(); // from Part 3
            initialRollsValues.push(internalRoll);
            playerScore += BigInt(internalRoll);
            await safeSendMessage(chatId, `${playerRef} (internal roll ${i+1}): ${formatDiceRolls([internalRoll])}`);
            await sleep(1000);
        }
    }

    const gameData = {
        type: 'dice21', gameId, chatId, userId, playerRef,
        betAmount: BigInt(betAmount), playerScore, botScore: 0n,
        status: 'player_turn', gameMessageId: null 
    };

    let messageText = `${playerRef}, your rolls ${formatDiceRolls(initialRollsValues)} total: *${playerScore}*.`;
    let buttons = [];

    if (playerScore > BigInt(DICE_21_TARGET_SCORE)) {
        messageText += `\n\n💥 BUST! Over ${DICE_21_TARGET_SCORE}. You lose ${formatCurrency(Number(betAmount))}.`;
        gameData.status = 'game_over_player_bust';
        await queryDatabase(
            `UPDATE bets SET status = 'lost', processed_at = NOW() 
             WHERE user_id = $1 AND bet_details->>'game_id' = $2 AND status = 'active'`,
             [userId, gameId]
        ).catch(e => console.error("[D21_START_ERR] Error logging player bust on deal:", e));
        buttons.push({ text: `🎲 Play Dice 21 Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_d21:${betAmount}` });
    } else if (playerScore === BigInt(DICE_21_TARGET_SCORE)) {
        messageText += `\n\n✨ Perfect ${DICE_21_TARGET_SCORE}! You Stand. Bot's turn...`;
        gameData.status = 'bot_turn';
    } else {
        messageText += `\n\nHit or Stand?`;
        buttons.push({ text: "⤵️ Hit (Roll Again)", callback_data: `d21_hit:${gameId}` });
        buttons.push({ text: `✅ Stand (Score: ${playerScore})`, callback_data: `d21_stand:${gameId}` });
    }
    
    const keyboard = { inline_keyboard: [buttons] };
    const sentMsg = await safeSendMessage(chatId, messageText, { parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboard : {}) });
    
    if (sentMsg) gameData.gameMessageId = sentMsg.message_id;
    activeGames.set(gameId, gameData);

    if (gameData.status === 'bot_turn') {
        await sleep(1500);
        if (typeof processDice21BotTurn === 'function') await processDice21BotTurn(gameData, gameData.gameMessageId);
        else console.error("processDice21BotTurn not defined!");
    } else if (gameData.status.startsWith('game_over')) {
        activeGames.delete(gameId);
    }
}

async function handleDice21Hit(gameId, userObj, originalMessageId) {
    const gameData = activeGames.get(gameId);
    const userId = String(userObj.id);

    if (!gameData || gameData.userId !== userId || gameData.status !== 'player_turn') {
        await safeSendMessage(userId, "Not your Dice 21 game or turn.", {}); return;
    }
    const chatId = gameData.chatId;
    await safeSendMessage(chatId, `${gameData.playerRef} hits! Rolling...`, {parse_mode: 'MarkdownV2'});
    let newRoll;
    try {
        const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
        newRoll = BigInt(diceMsg.dice.value);
    } catch (e) {
        console.error("[D21_HIT_ERR] sendDice failed, using internal roll:", e.message);
        newRoll = BigInt(rollDie());
        await safeSendMessage(chatId, `${gameData.playerRef} (internal roll): ${formatDiceRolls([Number(newRoll)])}`);
    }
    gameData.playerScore += newRoll;
    await sleep(2000); 

    let messageText = `${gameData.playerRef} rolled ${formatDiceRolls([Number(newRoll)])}. Score: *${gameData.playerScore}*.`;
    let buttons = [];

    if (gameData.playerScore > BigInt(DICE_21_TARGET_SCORE)) {
        messageText += `\n\n💥 BUST! Over ${DICE_21_TARGET_SCORE}. You lose ${formatCurrency(Number(gameData.betAmount))}.`;
        gameData.status = 'game_over_player_bust';
        buttons.push({ text: `🎲 Play Dice 21 Again (${formatCurrency(Number(gameData.betAmount))})`, callback_data: `play_again_d21:${gameData.betAmount}` });
        await queryDatabase(
            `UPDATE bets SET status = 'lost', processed_at = NOW() 
             WHERE user_id = $1 AND bet_details->>'game_id' = $2 AND status = 'active'`,
             [userId, gameId]
        ).catch(e => console.error("[D21_HIT_ERR] Error logging player bust:", e));
    } else if (gameData.playerScore === BigInt(DICE_21_TARGET_SCORE)) {
        messageText += `\n\n✨ Perfect ${DICE_21_TARGET_SCORE}! You Stand. Bot's turn...`;
        gameData.status = 'bot_turn';
    } else {
        messageText += `\n\nHit or Stand?`;
        buttons.push({ text: "⤵️ Hit (Roll Again)", callback_data: `d21_hit:${gameId}` });
        buttons.push({ text: `✅ Stand (Score: ${gameData.playerScore})`, callback_data: `d21_stand:${gameId}` });
    }
    
    const keyboard = { inline_keyboard: [buttons] };
    await bot.editMessageText(messageText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboard : {}) });
    
    if (gameData.status.startsWith('game_over')) {
        activeGames.delete(gameId);
    } else {
        activeGames.set(gameId, gameData);
        if (gameData.status === 'bot_turn') {
             await sleep(1500);
             if (typeof processDice21BotTurn === 'function') await processDice21BotTurn(gameData, originalMessageId);
             else console.error("processDice21BotTurn not defined!");
        }
    }
}

async function handleDice21Stand(gameId, userObj, originalMessageId) {
    const gameData = activeGames.get(gameId);
    const userId = String(userObj.id);
    if (!gameData || gameData.userId !== userId || gameData.status !== 'player_turn') { /* ... */ return; }
    gameData.status = 'bot_turn';
    activeGames.set(gameId, gameData);
    const messageText = `${gameData.playerRef} stands with *${gameData.playerScore}*.\n\n🤖 Bot's turn...`;
    await bot.editMessageText(messageText, { chat_id: gameData.chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {} });
    await sleep(1500);
    if (typeof processDice21BotTurn === 'function') await processDice21BotTurn(gameData, originalMessageId);
    else console.error("processDice21BotTurn not defined!");
}

async function processDice21BotTurn(gameData, messageId) {
    const { gameId, chatId, userId, playerRef, playerScore, betAmount } = gameData;
    let botScore = 0n;
    let botBusted = false;
    let botRollsDisplayAccumulator = `${playerRef}'s score: *${playerScore}*\nBot is playing...\n`;

    await bot.editMessageText(botRollsDisplayAccumulator + "\n_Bot is rolling..._ 🎲", {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(e => console.warn(`[D21_BOT] Initial editMessageText failed: ${e.message}`));
    await sleep(1000);

    for (let i=0; i < 10 && botScore < BigInt(DICE_21_BOT_STAND_SCORE) && !botBusted; i++) { // Bot takes max 10 rolls to prevent infinite loop
        let currentRollValue;
        try {
            const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
            currentRollValue = BigInt(diceMsg.dice.value);
        } catch (e) {
            console.error("[D21_BOT_TURN_ERR] sendDice failed for bot, using internal:", e.message);
            currentRollValue = BigInt(rollDie());
            await safeSendMessage(chatId, `Bot (internal roll): ${formatDiceRolls([Number(currentRollValue)])}`);
        }
        botScore += currentRollValue;
        botRollsDisplayAccumulator += `Bot rolled ${formatDiceRolls([Number(currentRollValue)])} → Bot score: *${botScore}*\n`;
        
        let nextActionMsg = (botScore < BigInt(DICE_21_BOT_STAND_SCORE) && botScore <= BigInt(DICE_21_TARGET_SCORE)) ? "\n_Bot hits..._ 🎲" : "";
        if (botScore > BigInt(DICE_21_TARGET_SCORE)) {
            botBusted = true;
            botRollsDisplayAccumulator += "💥 Bot BUSTED!\n";
            nextActionMsg = ""; // No next action if busted
        } else if (botScore >= BigInt(DICE_21_BOT_STAND_SCORE)) {
            botRollsDisplayAccumulator += `Bot stands with *${botScore}*.\n`;
            nextActionMsg = ""; // No next action if standing
        }
        
        await bot.editMessageText(botRollsDisplayAccumulator + nextActionMsg, {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: {}}).catch(e => console.warn(`[D21_BOT] editMessageText during roll ${i+1} failed: ${e.message}`));
        await sleep(2500);
        if (botBusted || botScore >= BigInt(DICE_21_BOT_STAND_SCORE)) break;
    }
    await sleep(1500);

    let resultTextEnd = "";
    let payoutAmount = 0n;
    let outcomeReasonLog = "";

    if (botBusted) { resultTextEnd = `🎉 ${playerRef} wins! Bot busted.`; payoutAmount = betAmount + betAmount; outcomeReasonLog = `won_dice21_bot_bust:${gameId}`; }
    else if (playerScore > botScore) { resultTextEnd = `🎉 ${playerRef} wins (*${playerScore}* vs *${botScore}*)!`; payoutAmount = betAmount + betAmount; outcomeReasonLog = `won_dice21_score:${gameId}`; }
    else if (botScore > playerScore) { resultTextEnd = `💀 Bot wins (*${botScore}* vs *${playerScore}*). Bet lost.`; payoutAmount = 0n; outcomeReasonLog = `lost_dice21_score:${gameId}`; }
    else { resultTextEnd = `😐 Push! Both *${playerScore}*. Bet returned.`; payoutAmount = betAmount; outcomeReasonLog = `push_dice21:${gameId}`; }
    
    resultTextEnd += `\nBet: ${formatCurrency(Number(betAmount))}.`;

    if (payoutAmount > 0n) {
        await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
        resultTextEnd += ` You receive ${formatCurrency(Number(payoutAmount))}.`;
    } else if (outcomeReasonLog.startsWith('lost_')) {
        await queryDatabase(
            `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
            [outcomeReasonLog, userId, gameId]
        ).catch(e => console.error("[D21_BOT_TURN_ERR] Error logging player loss:", e));
    }
    
    const finalMessageText = botRollsDisplayAccumulator + "\n" + resultTextEnd;
    const playAgainKeyboardD21 = { inline_keyboard: [[{ text: `🎲 Play Dice 21 Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_d21:${betAmount}` }]] };
    
    if (messageId && bot) await bot.editMessageText(finalMessageText, {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboardD21 }).catch(() => safeSendMessage(chatId, finalMessageText, {parse_mode:'MarkdownV2', reply_markup: playAgainKeyboardD21}));
    else await safeSendMessage(chatId, finalMessageText, {parse_mode:'MarkdownV2', reply_markup: playAgainKeyboardD21});

    activeGames.delete(gameId);
    console.log(`[DICE21_END] Game ${gameId}. Player: ${playerScore}, Bot: ${botScore}, BotBusted: ${botBusted}`);
}

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
// index.js - Part 5b: Dice Escalator Game Logic (with Enhanced Logging)
//---------------------------------------------------------------------------
console.log("Loading Part 5b: Dice Escalator Game Logic (with UI enhancements, jackpot display, and DETAILED LOGGING)...");

// Helper function for JSON.stringify to handle BigInts, undefined, and other types for comprehensive logging.
// This is included here to make logging comprehensive as requested.
function stringifyWithBigInt(obj) {
  return JSON.stringify(obj, (key, value) => {
    if (typeof value === 'bigint') {
      return value.toString() + 'n'; // Append 'n' to denote BigInt in logs
    }
    if (typeof value === 'function') {
      return `[Function: ${value.name || 'anonymous'}]`;
    }
    if (value === undefined) {
      return 'undefined_value'; // Clearly mark undefined values
    }
    // For other complex objects, you might want to add more specific serialization
    // but for now, this covers BigInt and undefined clearly.
    return value;
  }, 2); // The '2' argument pretty-prints the JSON string
}


// --- Helper Function to get Jackpot Display Segment ---
async function getJackpotDisplaySegment() {
    const LOG_PREFIX = "[getJackpotDisplaySegment]";
    console.log(`${LOG_PREFIX} Attempting to fetch jackpot display segment.`);

    try {
        console.log(`${LOG_PREFIX} Checking for dependencies: queryDatabase, MAIN_JACKPOT_ID, formatCurrency, escapeMarkdownV2.`);
        if (typeof queryDatabase !== 'function' || typeof MAIN_JACKPOT_ID === 'undefined' || typeof formatCurrency !== 'function' || typeof escapeMarkdownV2 !== 'function') {
            console.warn(`${LOG_PREFIX} Missing dependencies for jackpot display. queryDatabase: ${typeof queryDatabase}, MAIN_JACKPOT_ID: ${MAIN_JACKPOT_ID}, formatCurrency: ${typeof formatCurrency}, escapeMarkdownV2: ${typeof escapeMarkdownV2}. Returning empty string.`);
            return "";
        }
        console.log(`${LOG_PREFIX} Dependencies seem to be present.`);

        const sqlQuery = 'SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1';
        const queryParams = [MAIN_JACKPOT_ID];
        console.log(`${LOG_PREFIX} Executing database query: ${sqlQuery} with params: ${stringifyWithBigInt(queryParams)}`);
        const result = await queryDatabase(sqlQuery, queryParams);
        console.log(`${LOG_PREFIX} Database query executed. Result: ${stringifyWithBigInt(result)}`);

        if (result.rows.length > 0) {
            const jackpotAmountLamports = BigInt(result.rows[0].current_amount_lamports);
            console.log(`${LOG_PREFIX} Jackpot amount found: ${jackpotAmountLamports} lamports.`);
            const jackpotDisplayAmount = formatCurrency(Number(jackpotAmountLamports));
            const escapedDisplayAmount = escapeMarkdownV2(jackpotDisplayAmount);
            const displaySegment = `\n\n🎰 Current Jackpot: *${escapedDisplayAmount}*`;
            console.log(`${LOG_PREFIX} Successfully generated jackpot display segment: "${displaySegment}"`);
            return displaySegment;
        } else {
            console.log(`${LOG_PREFIX} No jackpot record found for jackpot_id: ${MAIN_JACKPOT_ID}. Returning empty string.`);
        }
    } catch (error) {
        console.error(`${LOG_PREFIX} Error fetching jackpot for display: ${error.message}`, error);
    }
    console.log(`${LOG_PREFIX} Returning empty string due to error or no record.`);
    return "";
}

// --- Dice Escalator Constants ---
// IMPORTANT: Ensure these constants are declared ONLY ONCE in your entire index.js file.
// If they are also in Part 1 or elsewhere, remove the duplicate declarations.

// const BOT_STAND_SCORE_DICE_ESCALATOR = BigInt(10); // Expected from Part 1 or globally
// const TARGET_JACKPOT_SCORE = BigInt(process.env.TARGET_JACKPOT_SCORE || '16'); // Expected from Part 1 or globally
// const MAIN_JACKPOT_ID = 'dice_escalator_main'; // Expected from Part 1
// const JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.JACKPOT_CONTRIBUTION_PERCENT); // Expected from Part 1


// --- Game Handler Functions for Dice Escalator ---

/**
 * Starts a new Dice Escalator game.
 * - Deducts bet from user.
 * - Adds game to activeGames.
 * - Sends initial message with "Roll" button.
 */
async function handleStartDiceEscalatorCommand(chatId, userObj, betAmount, originalCommandMessageId) {
    const LOG_PREFIX = "[handleStartDiceEscalatorCommand]";
    console.log(`${LOG_PREFIX} Entered. ChatID: ${chatId}, UserObj: ${stringifyWithBigInt(userObj)}, BetAmount: ${betAmount}, OriginalCmdMsgID: ${originalCommandMessageId}`);

    if (!userObj || typeof userObj.userId === 'undefined') { // userId was userObj.id in Part 5a, ensure consistency
        console.error(`${LOG_PREFIX} CRITICAL ERROR: userObj is invalid or userObj.userId is undefined. UserObj: ${stringifyWithBigInt(userObj)}.`);
        // Assuming safeSendMessage and escapeMarkdownV2 are available globally
        if (typeof safeSendMessage === 'function' && typeof escapeMarkdownV2 === 'function') {
            await safeSendMessage(chatId, `Internal error: Your user data could not be processed correctly. Please try again or contact admin if this persists.`, {});
        }
        return;
    }
    const userId = String(userObj.userId); // Corrected from original Part 5b to use userObj.userId if that's the structure
    const playerRef = typeof getPlayerDisplayReference === 'function' ? getPlayerDisplayReference(userObj) : `User_${userId}`;
    const gameId = typeof generateGameId === 'function' ? generateGameId() : `game_${Date.now()}`;

    console.log(`${LOG_PREFIX} User ${userId} (${playerRef}) initiated Dice Escalator. Bet: ${betAmount}, Chat: ${chatId}, GameID: ${gameId}`);

    if (!userId || userId === "undefined_value" || userId === "undefined") {
        console.error(`${LOG_PREFIX} Critical error: userId is invalid after processing userObj. UserID: ${userId}. UserObj: ${stringifyWithBigInt(userObj)}. Cannot start game.`);
        await safeSendMessage(chatId, `${playerRef}, there was an internal error getting your User ID. Please try again. If the problem persists, contact an admin.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX} Proceeding with UserID: ${userId}.`);

    console.log(`${LOG_PREFIX} Attempting to update user balance for bet placement. UserID: ${userId}, Amount: ${-betAmount}, GameID: ${gameId}`);
    const balanceUpdateResult = await updateUserBalance(
        userId,
        -betAmount,
        `bet_placed_dice_escalator:${gameId}`,
        null, // client_
        gameId,
        String(chatId)
    );
    console.log(`${LOG_PREFIX} Balance update result: ${stringifyWithBigInt(balanceUpdateResult)}`);

    if (!balanceUpdateResult || !balanceUpdateResult.success) {
        const errorMsg = balanceUpdateResult ? balanceUpdateResult.error : "Unknown balance update error";
        console.error(`${LOG_PREFIX} Bet placement failed for ${userId}. Error: ${errorMsg}`);
        await safeSendMessage(chatId, `${playerRef}, your bet of ${escapeMarkdownV2(formatCurrency(betAmount))} for Dice Escalator failed. Reason: ${escapeMarkdownV2(errorMsg)}.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${LOG_PREFIX} Bet ${formatCurrency(betAmount)} placed successfully for ${userId}. New balance lamports: ${balanceUpdateResult.newBalanceLamports !== undefined ? balanceUpdateResult.newBalanceLamports : 'N/A'}`);

    const gameData = {
        type: 'DiceEscalator',
        gameId,
        chatId: String(chatId),
        userId,
        playerRef, // Storing for easy access
        betAmount: BigInt(betAmount),
        playerScore: 0n,
        playerRollCount: 0,
        botScore: 0n,
        status: 'waiting_player_roll', // Initial status
        gameMessageId: null, // Will be set after sending message
        commandMessageId: originalCommandMessageId,
        lastInteractionTime: Date.now()
    };
    activeGames.set(gameId, gameData); // activeGames map expected to be global
    console.log(`${LOG_PREFIX} Game ${gameId} created and stored in activeGames. Data: ${stringifyWithBigInt(gameData)}`);

    console.log(`${LOG_PREFIX} Fetching jackpot display segment for initial message.`);
    const jackpotDisplay = await getJackpotDisplaySegment();
    console.log(`${LOG_PREFIX} Jackpot display segment: "${jackpotDisplay}"`);

    const initialMessageText = `🎲 *Dice Escalator Challenge!* 🎲\n\n${playerRef} vs. The Bot 🤖!\nBet: *${escapeMarkdownV2(formatCurrency(betAmount))}*${jackpotDisplay}\n\nIt's your turn, ${playerRef}. Your current score: *0*.\n👇 Roll the dice!`;
    const keyboard = {
        inline_keyboard: [
            [{ text: "🎲 Roll Dice", callback_data: `de_roll_prompt:${gameId}` }],
        ]
    };
    console.log(`${LOG_PREFIX} Prepared initial message text and keyboard. Text length: ${initialMessageText.length}. Keyboard: ${stringifyWithBigInt(keyboard)}`);

    console.log(`${LOG_PREFIX} Attempting to send initial game message to chat ${chatId}.`);
    const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    if (sentMessage && sentMessage.message_id) {
        gameData.gameMessageId = sentMessage.message_id;
        activeGames.set(gameId, gameData); // Update gameData with message_id
        console.log(`${LOG_PREFIX} Initial game message sent successfully for ${gameId}. MessageID: ${sentMessage.message_id}. Updated gameData: ${stringifyWithBigInt(gameData)}`);
    } else {
        console.error(`${LOG_PREFIX} Failed to send initial game message for ${gameId}. Refunding user ${userId} for amount ${betAmount}.`);
        await updateUserBalance(userId, betAmount, `refund_dice_escalator_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
        console.log(`${LOG_PREFIX} User refunded and game ${gameId} deleted from activeGames.`);
    }
    console.log(`${LOG_PREFIX} Exiting.`);
}

/**
 * Handles player actions from inline buttons (Roll or Cash Out/Stand).
 */
async function handleDiceEscalatorPlayerAction(gameId, userIdFromCallback, action, originalMessageId, chatIdFromCallback) {
    const LOG_PREFIX = "[handleDiceEscalatorPlayerAction]";
    console.log(`${LOG_PREFIX} Entered. GameID: ${gameId}, UserID from CB: ${userIdFromCallback}, Action: ${action}, OriginalMsgID: ${originalMessageId}, ChatID from CB: ${chatIdFromCallback}`);

    const gameData = activeGames.get(gameId);
    const now = Date.now();

    if (!gameData) {
        console.warn(`${LOG_PREFIX} Game ${gameId} not found in activeGames. Action: ${action}. Clearing buttons on msg ${originalMessageId} in chat ${chatIdFromCallback}.`);
        if (bot && originalMessageId && chatIdFromCallback && typeof bot.editMessageReplyMarkup === 'function') {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatIdFromCallback), message_id: Number(originalMessageId) })
                .then(() => console.log(`${LOG_PREFIX} Successfully cleared buttons for non-existent game ${gameId} on msg ${originalMessageId}.`))
                .catch(e => console.error(`${LOG_PREFIX} Failed to clear buttons for non-existent game ${gameId} on msg ${originalMessageId}: ${e.message}`, e));
        }
        await safeSendMessage(String(chatIdFromCallback), "This Dice Escalator game session has expired or could not be found. 🙁 Please start a new game.", {});
        console.log(`${LOG_PREFIX} Exiting due to game not found.`);
        return;
    }
    console.log(`${LOG_PREFIX} Game ${gameId} found. Current gameData: ${stringifyWithBigInt(gameData)}`);

    if (String(gameData.userId) !== String(userIdFromCallback)) {
        console.warn(`${LOG_PREFIX} User ${userIdFromCallback} pressed button for game ${gameId} which belongs to user ${gameData.userId}. Ignoring.`);
        // Optionally, send a message to userIdFromCallback if they are not the player
        // await safeSendMessage(String(userIdFromCallback), "This is not your game.", {});
        console.log(`${LOG_PREFIX} Exiting due to user mismatch.`);
        return;
    }

    if (gameData.gameMessageId && Number(gameData.gameMessageId) !== Number(originalMessageId)) {
        console.warn(`${LOG_PREFIX} Action on an old/stale message for game ${gameId}. Game's active message ID: ${gameData.gameMessageId}, Action's message ID: ${originalMessageId}. Ignoring action.`);
        // It might be useful to inform the user that they are interacting with an old message
        // await safeSendMessage(String(chatIdFromCallback), "You're clicking buttons on an older game message. Please use the latest game message.", {});
        console.log(`${LOG_PREFIX} Exiting due to action on stale message.`);
        return;
    }

    console.log(`${LOG_PREFIX} Updating lastInteractionTime for game ${gameId} to ${now}.`);
    gameData.lastInteractionTime = now;
    activeGames.set(gameId, gameData); // Save updated interaction time

    console.log(`${LOG_PREFIX} Processing action '${action}' for game ${gameId}. Current status: ${gameData.status}.`);

    switch (action) {
        case 'de_roll_prompt':
            console.log(`${LOG_PREFIX} Action is 'de_roll_prompt'. Validating game status.`);
            if (gameData.status !== 'waiting_player_roll' && gameData.status !== 'player_turn_prompt_action') {
                console.warn(`${LOG_PREFIX} Roll action received but game ${gameId} is in an invalid state: '${gameData.status}'.`);
                if (gameData.status.startsWith('game_over') || gameData.status === 'bot_turn' || gameData.status === 'bot_rolling') {
                    console.log(`${LOG_PREFIX} Game is already over or bot's turn. Attempting to update message for game ${gameId}.`);
                    const jackpotDisplayEnded = await getJackpotDisplaySegment();
                    let playAgainButton = {};
                    if (gameData.status.startsWith("game_over_player") || gameData.status.startsWith("game_over_final")) {
                         playAgainButton = { inline_keyboard: [[{ text: `🎲 Play Again (${formatCurrency(Number(gameData.betAmount))})`, callback_data: `play_again_de:${gameData.betAmount}` }]]};
                    }
                    const msgText = `Game ${gameId} is already concluded or it's the Bot's turn. Your score was: ${escapeMarkdownV2(String(gameData.playerScore))}.${jackpotDisplayEnded}`;
                     if(gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
                        await bot.editMessageText(msgText, {
                            chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId || originalMessageId),
                            parse_mode: 'MarkdownV2',
                            reply_markup: playAgainButton
                        }).catch(e => console.error(`${LOG_PREFIX} Failed to update message for invalid roll state: ${e.message}`, e));
                    } else {
                        console.warn(`${LOG_PREFIX} Cannot edit message for invalid roll state as gameMessageId or bot is not available.`);
                        await safeSendMessage(String(gameData.chatId), msgText, { parse_mode: 'MarkdownV2', reply_markup: playAgainButton });
                    }
                }
                console.log(`${LOG_PREFIX} Exiting 'de_roll_prompt' due to invalid status.`);
                return;
            }
            console.log(`${LOG_PREFIX} Game status is valid for roll. Proceeding to process player roll for game ${gameId}.`);
            await processDiceEscalatorPlayerRoll(gameData);
            break;

        case 'de_cashout': // This means player chooses to Stand
            console.log(`${LOG_PREFIX} Action is 'de_cashout'. Validating game status.`);
            if (gameData.status !== 'player_turn_prompt_action') {
                console.warn(`${LOG_PREFIX} Cashout action received but game ${gameId} is in an invalid state: '${gameData.status}'. Ignoring.`);
                console.log(`${LOG_PREFIX} Exiting 'de_cashout' due to invalid status.`);
                return;
            }
            if (gameData.playerScore <= 0n) {
                console.warn(`${LOG_PREFIX} Cashout attempted with score ${gameData.playerScore} for game ${gameId}. This is not allowed. Player must have a positive score.`);
                await safeSendMessage(String(gameData.chatId), `${gameData.playerRef}, you cannot cash out with a score of ${escapeMarkdownV2(String(gameData.playerScore))}. You need to roll at least once successfully and have a score greater than 0.`, { parse_mode: 'MarkdownV2' });
                console.log(`${LOG_PREFIX} Exiting 'de_cashout' due to zero or negative score.`);
                return;
            }

            console.log(`${LOG_PREFIX} Player ${gameData.userId} stands (Cashout) with score ${gameData.playerScore} in game ${gameId}. Setting status to 'bot_turn'.`);
            gameData.status = 'bot_turn';
            activeGames.set(gameId, gameData); // Persist status change

            const jackpotDisplayCashout = await getJackpotDisplaySegment();
            const cashoutMessage = `${gameData.playerRef} stands with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*! ✅\nBet: ${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount)))}.${jackpotDisplayCashout}\n\n🤖 Bot is now playing... 🤔`;
            console.log(`${LOG_PREFIX} Prepared cashout message for game ${gameId}: "${cashoutMessage}"`);

            if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
                console.log(`${LOG_PREFIX} Attempting to edit message ${gameData.gameMessageId} for cashout.`);
                await bot.editMessageText(
                    cashoutMessage,
                    {
                        chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId || originalMessageId),
                        parse_mode: 'MarkdownV2', reply_markup: {} // Clear buttons
                    }
                ).catch(e => {
                    console.error(`${LOG_PREFIX} Failed to edit message for cashout on game ${gameId}: ${e.message}. Sending new message.`, e);
                    safeSendMessage(String(gameData.chatId), cashoutMessage, { parse_mode: 'MarkdownV2' }); // Fallback
                });
            } else {
                console.warn(`${LOG_PREFIX} Cannot edit message for cashout as gameMessageId or bot is not available. Sending new message. GameID: ${gameId}`);
                await safeSendMessage(String(gameData.chatId), cashoutMessage, { parse_mode: 'MarkdownV2' });
            }

            console.log(`${LOG_PREFIX} Pausing for 1.5s before bot turn for game ${gameId}.`);
            await sleep(1500); // Give user time to read the "Bot is playing" message

            console.log(`${LOG_PREFIX} Proceeding to process bot turn for game ${gameId}.`);
            await processDiceEscalatorBotTurn(gameData);
            break;

        default:
            console.error(`${LOG_PREFIX} Unknown action '${action}' received for game ${gameId}. Full callback data might be more complex.`);
    }
    console.log(`${LOG_PREFIX} Exiting. GameID: ${gameId}, Action: ${action}`);
}

/**
 * Processes a player's dice roll.
 */
async function processDiceEscalatorPlayerRoll(gameData) {
    const LOG_PREFIX = "[processDiceEscalatorPlayerRoll]";
    console.log(`${LOG_PREFIX} Entered. GameID: ${gameData.gameId}. Initial gameData: ${stringifyWithBigInt(gameData)}`);

    const { gameId, chatId, userId, playerRef, betAmount } = gameData; // Destructure after logging initial gameData

    console.log(`${LOG_PREFIX} Setting game ${gameId} status to 'player_rolling'.`);
    gameData.status = 'player_rolling';
    activeGames.set(gameId, gameData);

    const jackpotDisplayRolling = await getJackpotDisplaySegment();
    const rollingMessage = `${playerRef} (Bet: ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}) is rolling... 🎲${jackpotDisplayRolling}\nCurrent Score: *${escapeMarkdownV2(String(gameData.playerScore))}*`;
    console.log(`${LOG_PREFIX} Prepared 'rolling...' message for game ${gameId}: "${rollingMessage}"`);

    if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
        console.log(`${LOG_PREFIX} Attempting to edit message ${gameData.gameMessageId} to 'rolling...'.`);
        await bot.editMessageText(
            rollingMessage, {
            chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
            parse_mode: 'MarkdownV2', reply_markup: {} // Clear buttons while rolling
        }).catch(e => console.warn(`${LOG_PREFIX} Failed to update message to 'rolling...' for game ${gameId}: ${e.message}. Player might see old buttons briefly.`, e));
    } else {
        console.warn(`${LOG_PREFIX} Cannot edit message to 'rolling...' as gameMessageId or bot is not available. GameID: ${gameId}`);
        // Might send a new message if edit fails or not possible, but could be confusing.
        // For now, just log it. The dice roll message will follow.
    }

    console.log(`${LOG_PREFIX} Pausing for 0.5s before sending dice animation for game ${gameId}.`);
    await sleep(500); // Brief pause

    let playerRollValue;
    console.log(`${LOG_PREFIX} Attempting to send animated dice for player roll in game ${gameId}, chat ${chatId}.`);
    try {
        if (!bot || typeof bot.sendDice !== 'function') {
            throw new Error("bot.sendDice function is not available.");
        }
        const diceMessage = await bot.sendDice(chatId, { emoji: '🎲' });
        if (!diceMessage || !diceMessage.dice || typeof diceMessage.dice.value === 'undefined') {
            throw new Error("Invalid dice message received from bot.sendDice.");
        }
        playerRollValue = BigInt(diceMessage.dice.value);
        console.log(`${LOG_PREFIX} Player ${userId} rolled ${playerRollValue} (animated) for game ${gameId}.`);
        console.log(`${LOG_PREFIX} Pausing for 2s for dice animation to complete for game ${gameId}.`);
        await sleep(2000); // Allow time for animation
    } catch (diceError) {
        console.error(`${LOG_PREFIX} Failed to send animated dice for game ${gameId}: ${diceError.message}. Using internal roll.`, diceError);
        playerRollValue = BigInt(rollDie()); // rollDie is from Part 3
        console.log(`${LOG_PREFIX} Player ${userId} rolled ${playerRollValue} (internal fallback) for game ${gameId}.`);
        await safeSendMessage(chatId, `${playerRef} (internal roll): You rolled a *${escapeMarkdownV2(String(playerRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' });
        console.log(`${LOG_PREFIX} Pausing for 1s after internal roll message for game ${gameId}.`);
        await sleep(1000);
    }

    gameData.playerRollCount += 1;
    console.log(`${LOG_PREFIX} Player roll count for game ${gameId} is now ${gameData.playerRollCount}.`);

    // DICE_ESCALATOR_BUST_ON is expected from Part 4, e.g., const DICE_ESCALATOR_BUST_ON = 1;
    const bustValue = typeof DICE_ESCALATOR_BUST_ON !== 'undefined' ? BigInt(DICE_ESCALATOR_BUST_ON) : 1n; // Default to 1 if not defined
    if (playerRollValue === bustValue) {
        console.log(`${LOG_PREFIX} Player ${userId} BUSTED in game ${gameId} by rolling ${playerRollValue} (bust value is ${bustValue}).`);
        gameData.playerScore = 0n; // Score resets on bust
        gameData.status = 'game_over_player_bust';
        activeGames.set(gameId, gameData); // Update status

        console.log(`${LOG_PREFIX} Updating user balance for bust (loss) in game ${gameId}. UserID: ${userId}`);
        // For a bust, the bet is lost. `updateUserBalance` with 0n change and a 'lost' reason will handle logging the loss correctly.
        // The bet was already deducted at the start of the game.
        // This call ensures the bet record in 'bets' table is updated to 'lost'.
        await updateUserBalance(userId, 0n, `lost_dice_escalator_bust:${gameId}`, null, gameId, String(chatId));
        console.log(`${LOG_PREFIX} Balance update call for loss completed for game ${gameId}.`);

        const jackpotDisplayBust = await getJackpotDisplaySegment();
        const bustMessage = `${playerRef} rolled a *${escapeMarkdownV2(String(playerRollValue))}*... BUST! 💥\nYour score is reset to 0. You lose your bet of ${escapeMarkdownV2(formatCurrency(Number(betAmount)))} 😥.${jackpotDisplayBust}`;
        const bustKeyboard = { inline_keyboard: [[{ text: `🎲 Play Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_de:${betAmount}` }]] };
        console.log(`${LOG_PREFIX} Prepared bust message for game ${gameId}: "${bustMessage}"`);

        if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
            console.log(`${LOG_PREFIX} Attempting to edit message ${gameData.gameMessageId} with bust outcome.`);
            await bot.editMessageText(bustMessage, {
                chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: bustKeyboard
            }).catch(async e => {
                console.error(`${LOG_PREFIX} Failed to edit message for bust on game ${gameId}: ${e.message}. Sending new message.`, e);
                await safeSendMessage(chatId, bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
            });
        } else {
            console.warn(`${LOG_PREFIX} Cannot edit message for bust as gameMessageId or bot is not available. Sending new message. GameID: ${gameId}`);
            await safeSendMessage(chatId, bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
        }
        console.log(`${LOG_PREFIX} Deleting game ${gameId} from activeGames due to player bust.`);
        activeGames.delete(gameId);
    } else {
        gameData.playerScore += playerRollValue;
        gameData.status = 'player_turn_prompt_action'; // Player can roll again or stand
        activeGames.set(gameId, gameData); // Persist new score and status
        console.log(`${LOG_PREFIX} Player ${userId} roll ${playerRollValue} successful in game ${gameId}. New Score: ${gameData.playerScore}. Status: ${gameData.status}.`);

        const jackpotDisplaySuccess = await getJackpotDisplaySegment();
        const successMessage = `${playerRef}, you rolled a *${escapeMarkdownV2(String(playerRollValue))}*! 🎉\nYour new score: *${escapeMarkdownV2(String(gameData.playerScore))}*.\nBet: ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}.${jackpotDisplaySuccess}\n\nIt's still your turn 🤔: Roll again or Stand?`;
        const successKeyboard = {
            inline_keyboard: [
                [{ text: "🎲 Roll Again", callback_data: `de_roll_prompt:${gameId}` },
                 { text: `💰 Stand (Score: ${escapeMarkdownV2(String(gameData.playerScore))})`, callback_data: `de_cashout:${gameId}` }]
            ]
        };
        console.log(`${LOG_PREFIX} Prepared success roll message for game ${gameId}: "${successMessage}"`);

        if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
            console.log(`${LOG_PREFIX} Attempting to edit message ${gameData.gameMessageId} with roll success outcome.`);
            await bot.editMessageText(successMessage, {
                chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: successKeyboard
            }).catch(async e => {
                console.error(`${LOG_PREFIX} Failed to edit message for roll success on game ${gameId}: ${e.message}. Sending new message.`, e);
                const newMsg = await safeSendMessage(chatId, successMessage, { parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
                if (newMsg && newMsg.message_id) {
                     console.log(`${LOG_PREFIX} New message sent successfully, updating gameMessageId for game ${gameId} to ${newMsg.message_id}.`);
                     gameData.gameMessageId = newMsg.message_id;
                     activeGames.set(gameId, gameData); // Persist new message ID
                } else {
                    console.error(`${LOG_PREFIX} Failed to send new message or get message_id for roll success fallback. GameID: ${gameId}`);
                }
            });
        } else {
            console.warn(`${LOG_PREFIX} Cannot edit message for roll success as gameMessageId or bot is not available. Sending new message. GameID: ${gameId}`);
            const newMsg = await safeSendMessage(chatId, successMessage, { parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
            if (newMsg && newMsg.message_id) {
                console.log(`${LOG_PREFIX} New message sent successfully, updating gameMessageId for game ${gameId} to ${newMsg.message_id}.`);
                gameData.gameMessageId = newMsg.message_id;
                activeGames.set(gameId, gameData); // Persist new message ID
            } else {
                 console.error(`${LOG_PREFIX} Failed to send new message or get message_id for roll success fallback (no prior gameMessageId). GameID: ${gameId}`);
            }
        }
    }
    console.log(`${LOG_PREFIX} Exiting. GameID: ${gameId}. Final status for this roll: ${gameData.status}, player score: ${gameData.playerScore}. Game data in activeGames: ${stringifyWithBigInt(activeGames.get(gameId))}`);
}


/**
 * Processes the Bot's turn after a player stands.
 */
async function processDiceEscalatorBotTurn(gameData) {
    const LOG_PREFIX = "[processDiceEscalatorBotTurn]";
    console.log(`${LOG_PREFIX} Entered. GameID: ${gameData.gameId}. Initial gameData: ${stringifyWithBigInt(gameData)}`);

    const { gameId, chatId, userId, playerRef, playerScore, betAmount } = gameData;

    console.log(`${LOG_PREFIX} Setting game ${gameId} status to 'bot_rolling'.`);
    gameData.status = 'bot_rolling';
    gameData.botScore = 0n; // Initialize bot score for this turn
    activeGames.set(gameId, gameData);

    const initialJackpotDisplayBot = await getJackpotDisplaySegment();
    let botMessageAccumulator = `${playerRef} stood with *${escapeMarkdownV2(String(playerScore))}*.\nBet: ${escapeMarkdownV2(formatCurrency(Number(betAmount)))}.${initialJackpotDisplayBot}\n\n🤖 Bot's turn:\n`;
    const thinkingMessage = "_Bot is thinking..._ 🤔";
    console.log(`${LOG_PREFIX} Prepared initial bot turn message for game ${gameId}. Accumulator: "${botMessageAccumulator}", Thinking: "${thinkingMessage}"`);

    if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
        console.log(`${LOG_PREFIX} Attempting to edit message ${gameData.gameMessageId} to 'bot thinking...'.`);
        await bot.editMessageText(botMessageAccumulator + thinkingMessage, {
            chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
            parse_mode: 'MarkdownV2', reply_markup: {} // Clear buttons
        }).catch(e => {
            console.warn(`${LOG_PREFIX} Failed to update 'bot thinking' message for game ${gameId}: ${e.message}. Sending new message as fallback.`, e);
            // If edit fails, try to send a new message, though this might clutter the chat.
            // Storing the new message ID would be complex here as it deviates from main flow.
            safeSendMessage(chatId, botMessageAccumulator + thinkingMessage, { parse_mode: 'MarkdownV2' });
        });
    } else {
        console.warn(`${LOG_PREFIX} Cannot edit message for 'bot thinking' as gameMessageId or bot is not available. Sending new message. GameID: ${gameId}`);
        const newMsg = await safeSendMessage(chatId, botMessageAccumulator + thinkingMessage, { parse_mode: 'MarkdownV2' });
        if (newMsg && newMsg.message_id) {
            console.log(`${LOG_PREFIX} New 'bot thinking' message sent, gameMessageId for game ${gameId} is now ${newMsg.message_id}.`);
            gameData.gameMessageId = newMsg.message_id; // Update if a new message had to be sent
            activeGames.set(gameId, gameData);
        } else {
            console.error(`${LOG_PREFIX} Failed to send new 'bot thinking' message or get its ID. GameID: ${gameId}`);
        }
    }

    console.log(`${LOG_PREFIX} Pausing for 1s while bot 'thinks' for game ${gameId}.`);
    await sleep(1000);

    let botRollValue;
    const botStandScore = typeof BOT_STAND_SCORE_DICE_ESCALATOR !== 'undefined' ? BigInt(BOT_STAND_SCORE_DICE_ESCALATOR) : 10n; // Default if not defined
    const bustValueBot = typeof DICE_ESCALATOR_BUST_ON !== 'undefined' ? BigInt(DICE_ESCALATOR_BUST_ON) : 1n;

    console.log(`${LOG_PREFIX} Bot will stand on score: ${botStandScore}. Bust value: ${bustValueBot}. GameID: ${gameId}`);

    do {
        console.log(`${LOG_PREFIX} Bot rolling dice for game ${gameId}. Current bot score: ${gameData.botScore}.`);
        try {
            if (!bot || typeof bot.sendDice !== 'function') {
                throw new Error("bot.sendDice function is not available for bot's roll.");
            }
            const diceMessage = await bot.sendDice(chatId, { emoji: '🎲' });
             if (!diceMessage || !diceMessage.dice || typeof diceMessage.dice.value === 'undefined') {
                throw new Error("Invalid dice message received from bot.sendDice for bot's roll.");
            }
            botRollValue = BigInt(diceMessage.dice.value);
            console.log(`${LOG_PREFIX} Bot rolled ${botRollValue} (animated) for game ${gameId}.`);
            console.log(`${LOG_PREFIX} Pausing for 2s for bot dice animation to complete for game ${gameId}.`);
            await sleep(2000);
        } catch (diceError) {
            console.error(`${LOG_PREFIX} Bot failed animated dice for game ${gameId}: ${diceError.message}. Using internal roll.`, diceError);
            botRollValue = BigInt(rollDie()); // rollDie from Part 3
            console.log(`${LOG_PREFIX} Bot rolled ${botRollValue} (internal fallback) for game ${gameId}.`);
            await safeSendMessage(chatId, `🤖 Bot (internal roll): Rolled a *${escapeMarkdownV2(String(botRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' });
            console.log(`${LOG_PREFIX} Pausing for 1s after bot internal roll message for game ${gameId}.`);
            await sleep(1000);
        }

        if (botRollValue === bustValueBot) {
            gameData.botScore = 0n; // Bot busts, score resets to 0 for win/loss calculation
            botMessageAccumulator += `Bot rolled a *${escapeMarkdownV2(String(botRollValue))}*... BUST! 💥\n`;
            console.log(`${LOG_PREFIX} Bot BUSTED (rolled ${botRollValue}) in game ${gameId}.`);
            break; // Exit roll loop
        } else {
            gameData.botScore += botRollValue;
            botMessageAccumulator += `Bot rolled a *${escapeMarkdownV2(String(botRollValue))}* 🎲, new Bot score: *${escapeMarkdownV2(String(gameData.botScore))}*.\n`;
            console.log(`${LOG_PREFIX} Bot roll ${botRollValue} successful. New Bot score: ${gameData.botScore}. Game ${gameId}.`);
        }
        activeGames.set(gameId, gameData); // Update bot score in active game data

        if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
            let botActionMessage = "";
            if (gameData.botScore > 0n && gameData.botScore < botStandScore) { // Check > 0n as bust sets to 0n
                botActionMessage = "_Bot rolls again..._ 🎲";
            } else if (gameData.botScore >= botStandScore) {
                botActionMessage = "_Bot stands._";
            } else { // Bot busted (botScore is 0n)
                 botActionMessage = "_Bot busted!_";
            }
            console.log(`${LOG_PREFIX} Attempting to edit message ${gameData.gameMessageId} with bot's current roll status. Action: "${botActionMessage}"`);
            await bot.editMessageText(botMessageAccumulator + botActionMessage, {
                chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2'
            }).catch(e => console.warn(`${LOG_PREFIX} Failed to update bot rolling message for ${gameId}: ${e.message}. Game continues.`, e));
        } else {
            console.warn(`${LOG_PREFIX} Cannot update bot rolling message text as gameMessageId or bot is not available. GameID: ${gameId}`);
        }
        console.log(`${LOG_PREFIX} Pausing for 1.5s after bot roll update for game ${gameId}.`);
        await sleep(1500);

    } while (gameData.botScore > 0n && gameData.botScore < botStandScore); // Continue if score is positive and less than stand score

    console.log(`${LOG_PREFIX} Bot finished rolling for game ${gameId}. Final bot score: ${gameData.botScore}. Player score: ${playerScore}.`);

    let resultMessage;
    let payoutAmount = 0n;
    let outcomeReasonLog = "";
    let jackpotWon = false;
    const targetJackpotScoreValue = typeof TARGET_JACKPOT_SCORE !== 'undefined' ? BigInt(TARGET_JACKPOT_SCORE) : 99999n; // Default high if not set

    console.log(`${LOG_PREFIX} Determining game outcome. Player: ${playerScore}, Bot: ${gameData.botScore}, Jackpot Target: ${targetJackpotScoreValue}. GameID: ${gameId}`);

    if (gameData.botScore === 0n) { // Bot busted
        resultMessage = `🎉 ${playerRef} WINS! Bot busted.`;
        payoutAmount = betAmount + betAmount; // Player wins original bet + equivalent amount
        outcomeReasonLog = `won_dice_escalator_bot_bust:${gameId}`;
        if (playerScore >= targetJackpotScoreValue) {
            console.log(`${LOG_PREFIX} Jackpot condition met: Player score ${playerScore} >= ${targetJackpotScoreValue} AND bot busted. GameID: ${gameId}`);
            jackpotWon = true;
        }
    } else if (playerScore > gameData.botScore) {
        resultMessage = `🎉 ${playerRef} WINS with *${escapeMarkdownV2(String(playerScore))}* vs Bot's *${escapeMarkdownV2(String(gameData.botScore))}*!`;
        payoutAmount = betAmount + betAmount;
        outcomeReasonLog = `won_dice_escalator_score:${gameId}`;
        if (playerScore >= targetJackpotScoreValue) {
            console.log(`${LOG_PREFIX} Jackpot condition met: Player score ${playerScore} > Bot score ${gameData.botScore} AND Player score ${playerScore} >= ${targetJackpotScoreValue}. GameID: ${gameId}`);
            jackpotWon = true;
        }
    } else if (playerScore < gameData.botScore) {
        resultMessage = `💀 Bot WINS with *${escapeMarkdownV2(String(gameData.botScore))}* vs Player's *${escapeMarkdownV2(String(playerScore))}*.`;
        payoutAmount = 0n; // Player loses their bet (already deducted)
        outcomeReasonLog = `lost_dice_escalator_score:${gameId}`;
    } else { // Draw
        resultMessage = `😐 It's a DRAW! Both Player and Bot have *${escapeMarkdownV2(String(playerScore))}*.`;
        payoutAmount = betAmount; // Player gets their bet back
        outcomeReasonLog = `push_dice_escalator:${gameId}`;
    }
    console.log(`${LOG_PREFIX} Game outcome: ${resultMessage}. PayoutAmount: ${payoutAmount}. Reason: ${outcomeReasonLog}. JackpotWon: ${jackpotWon}. GameID: ${gameId}`);

    botMessageAccumulator += `\n${resultMessage}\n`;
    gameData.status = `game_over_final_${outcomeReasonLog.split(':')[0]}`; // Update final status
    activeGames.set(gameId, gameData); // Save status

    if (payoutAmount > 0n) {
        console.log(`${LOG_PREFIX} Player ${userId} has a payout of ${payoutAmount}. Updating balance for game ${gameId}. Reason: ${outcomeReasonLog}`);
        const payoutUpdateResult = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
        console.log(`${LOG_PREFIX} Payout balance update result: ${stringifyWithBigInt(payoutUpdateResult)} for game ${gameId}.`);
        if (payoutUpdateResult && payoutUpdateResult.success) {
             botMessageAccumulator += `You receive ${escapeMarkdownV2(formatCurrency(Number(payoutAmount)))} 💸.`;
        } else {
            botMessageAccumulator += `There was an issue processing your payout of ${escapeMarkdownV2(formatCurrency(Number(payoutAmount)))}. Admin has been notified.`;
            console.error(`${LOG_PREFIX} Failed to process payout for user ${userId}, game ${gameId}. Error: ${payoutUpdateResult ? payoutUpdateResult.error : 'Unknown error'}`);
        }
    } else if (outcomeReasonLog.startsWith('lost_')) {
        // Bet was already deducted. updateUserBalance is called here to log the 'lost' status in the 'bets' table.
        console.log(`${LOG_PREFIX} Player ${userId} lost. Logging loss for game ${gameId}. Reason: ${outcomeReasonLog}`);
        await updateUserBalance(userId, 0n, outcomeReasonLog, null, gameId, String(chatId)); // 0n change, just to log status
        botMessageAccumulator += `You lost your bet of ${escapeMarkdownV2(formatCurrency(Number(betAmount)))} 😥.`;
        console.log(`${LOG_PREFIX} Loss logged for player ${userId}, game ${gameId}.`);
    } else { // This case covers a push where payoutAmount is equal to betAmount.
         console.log(`${LOG_PREFIX} Game ${gameId} was a push. Balance already updated to reflect bet return.`);
         // updateUserBalance would have been called with payoutAmount = betAmount
    }


    if (jackpotWon) {
        console.log(`${LOG_PREFIX} JACKPOT WIN PROCESSING for game ${gameId}. User: ${userId}, Player Score: ${playerScore}.`);
        // `pool` is expected from Part 1
        if (!pool || typeof pool.connect !== 'function') {
            console.error(`${LOG_PREFIX} CRITICAL: Database pool is not available for jackpot processing! GameID: ${gameId}`);
            botMessageAccumulator += `\n\n⚠️ Critical error: DB Pool not found for jackpot. Admin notified.`;
        } else {
            const client = await pool.connect();
            console.log(`${LOG_PREFIX} Acquired DB client for jackpot transaction. GameID: ${gameId}`);
            try {
                await client.query('BEGIN');
                console.log(`${LOG_PREFIX} Started DB transaction for jackpot. GameID: ${gameId}`);

                const jackpotSelectQuery = 'SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1 FOR UPDATE';
                const jackpotSelectParams = [MAIN_JACKPOT_ID]; // MAIN_JACKPOT_ID from Part 1
                console.log(`${LOG_PREFIX} Selecting jackpot amount. Query: ${jackpotSelectQuery}, Params: ${stringifyWithBigInt(jackpotSelectParams)}. GameID: ${gameId}`);
                const jackpotResult = await client.query(jackpotSelectQuery, jackpotSelectParams);
                console.log(`${LOG_PREFIX} Jackpot select query result: ${stringifyWithBigInt(jackpotResult)}. GameID: ${gameId}`);

                if (jackpotResult.rows.length > 0) {
                    const jackpotTotalLamports = BigInt(jackpotResult.rows[0].current_amount_lamports);
                    console.log(`${LOG_PREFIX} Current jackpot amount is ${jackpotTotalLamports} lamports. GameID: ${gameId}`);
                    if (jackpotTotalLamports > 0n) {
                        console.log(`${LOG_PREFIX} Attempting to pay jackpot of ${jackpotTotalLamports} to user ${userId}. GameID: ${gameId}`);
                        const jackpotPayoutResult = await updateUserBalance(userId, jackpotTotalLamports, `jackpot_win_dice_escalator:${gameId}`, client, gameId, String(chatId));
                        console.log(`${LOG_PREFIX} Jackpot payout (updateUserBalance) result: ${stringifyWithBigInt(jackpotPayoutResult)}. GameID: ${gameId}`);

                        if (jackpotPayoutResult.success) {
                            const jackpotUpdateQuery = 'UPDATE jackpot_status SET current_amount_lamports = $1, last_won_at = NOW(), last_won_by_user_id = $2, last_won_game_id = $3, updated_at = NOW() WHERE jackpot_id = $4';
                            const jackpotUpdateParams = ['0', userId, gameId, MAIN_JACKPOT_ID];
                            console.log(`${LOG_PREFIX} Resetting jackpot. Query: ${jackpotUpdateQuery}, Params: ${stringifyWithBigInt(jackpotUpdateParams)}. GameID: ${gameId}`);
                            await client.query(jackpotUpdateQuery, jackpotUpdateParams);
                            console.log(`${LOG_PREFIX} Jackpot reset in DB. Committing transaction. GameID: ${gameId}`);
                            await client.query('COMMIT');

                            const jackpotWinMessage = `\n\n🏆🎉 CONGRATULATIONS ${playerRef}! You also won the Dice Escalator JACKPOT of *${escapeMarkdownV2(formatCurrency(Number(jackpotTotalLamports)))}*! 🎉🏆`;
                            botMessageAccumulator += jackpotWinMessage;
                            console.log(`${LOG_PREFIX} JACKPOT WIN SUCCESS! User ${userId} PAID jackpot of ${jackpotTotalLamports}. Game ${gameId}. Jackpot reset.`);
                        } else {
                            console.error(`${LOG_PREFIX} Failed to pay jackpot to ${userId} via updateUserBalance. Error: ${jackpotPayoutResult.error}. Rolling back. GameID: ${gameId}`);
                            await client.query('ROLLBACK');
                            botMessageAccumulator += `\n\n⚠️ Issue processing jackpot payout. Admin notified. Your game win is processed.`;
                        }
                    } else {
                        console.log(`${LOG_PREFIX} Jackpot condition met, but jackpot amount was 0. No payout. Committing transaction. GameID: ${gameId}`);
                        await client.query('COMMIT');
                        // No specific message needed if jackpot was 0, main win message is enough.
                    }
                } else {
                    console.error(`${LOG_PREFIX} Jackpot ID ${MAIN_JACKPOT_ID} not found in jackpot_status table. Rolling back. GameID: ${gameId}`);
                    await client.query('ROLLBACK');
                    botMessageAccumulator += `\n\n⚠️ Jackpot system error (ID not found). Admin notified. Your game win is processed.`;
                }
            } catch (error) {
                console.error(`${LOG_PREFIX} DB error during jackpot processing for game ${gameId}: ${error.message}`, error);
                try {
                    await client.query('ROLLBACK');
                    console.log(`${LOG_PREFIX} Transaction rolled back due to DB error in jackpot processing. GameID: ${gameId}`);
                } catch (rbErr) {
                    console.error(`${LOG_PREFIX} Failed to rollback jackpot transaction: ${rbErr.message}`, rbErr);
                }
                botMessageAccumulator += `\n\n⚠️ DB error with jackpot processing. Admin notified. Your game win is processed.`;
            } finally {
                if (client) {
                    client.release();
                    console.log(`${LOG_PREFIX} Released DB client for jackpot transaction. GameID: ${gameId}`);
                }
            }
        }
    }

    const finalJackpotDisplay = await getJackpotDisplaySegment();
    console.log(`${LOG_PREFIX} Fetched final jackpot display for end message: "${finalJackpotDisplay}". GameID: ${gameId}`);
    if (finalJackpotDisplay) {
        let trimmedJackpotDisplay = finalJackpotDisplay;
        if (botMessageAccumulator.endsWith('\n') && trimmedJackpotDisplay.startsWith('\n\n')) {
            trimmedJackpotDisplay = trimmedJackpotDisplay.substring(1);
        } else if (!botMessageAccumulator.endsWith('\n') && !trimmedJackpotDisplay.startsWith('\n')) {
            trimmedJackpotDisplay = '\n' + trimmedJackpotDisplay;
        }
        botMessageAccumulator += trimmedJackpotDisplay;
    }

    const finalKeyboard = { inline_keyboard: [[{ text: `🎲 Play Dice Escalator Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_de:${betAmount}` }]] };
    console.log(`${LOG_PREFIX} Prepared final message for game ${gameId}. Message: "${botMessageAccumulator}", Keyboard: ${stringifyWithBigInt(finalKeyboard)}`);

    if (gameData.gameMessageId && bot && typeof bot.editMessageText === 'function') {
        console.log(`${LOG_PREFIX} Attempting to edit final message ${gameData.gameMessageId} for game ${gameId}.`);
        await bot.editMessageText(botMessageAccumulator, {
            chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
            parse_mode: 'MarkdownV2', reply_markup: finalKeyboard
        }).catch(async e => {
            console.error(`${LOG_PREFIX} Failed to edit final message for game ${gameId}: ${e.message}. Sending new message.`, e);
            await safeSendMessage(chatId, botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
        });
    } else {
        console.warn(`${LOG_PREFIX} Cannot edit final message as gameMessageId or bot is not available. Sending new message. GameID: ${gameId}`);
        await safeSendMessage(chatId, botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
    }

    console.log(`${LOG_PREFIX} Deleting game ${gameId} from activeGames as it has concluded.`);
    activeGames.delete(gameId);
    console.log(`${LOG_PREFIX} Exiting. GameID: ${gameId} concluded. Player score: ${playerScore}, Bot score: ${gameData.botScore}. Jackpot won: ${jackpotWon}.`);
}

console.log("Part 5b: Dice Escalator Game Logic (with UI enhancements, jackpot display, and DETAILED LOGGING) - Loaded and Ready.");
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
