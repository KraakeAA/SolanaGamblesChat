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
    'JACKPOT_CONTRIBUTION_PERCENT': '0.01' // Default 1% contribution to jackpot from bets
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
const DATABASE_URL = process.env.DATABASE_URL; // Crucial for DB connection
const SHUTDOWN_FAIL_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
const JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.JACKPOT_CONTRIBUTION_PERCENT);
const MAIN_JACKPOT_ID = 'dice_escalator_main'; // ID for the primary jackpot

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

const BOT_VERSION = '2.2.0-jackpot'; // Updated version marker
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;

// --- Global State Variables for Shutdown & Operation ---
let isShuttingDown = false; // Flag to prevent multiple shutdown sequences

// --- In-memory stores ---
let activeGames = new Map(); // For active game state (Dice Escalator, Coinflip, RPS)
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
// --- Start of Part 5a ---
// index.js - Part 5a: Message & Callback Handling (Core Listeners, All Games)
//---------------------------------------------------------------------------
console.log("Loading Part 5a: Message & Callback Handling, All Game Flows...");

// --- Game Constants & Configuration ---
// These are typically defined in Part 1 or at the top of the script
// const COMMAND_COOLDOWN_MS = 2000; (from Part 1)
// const JOIN_GAME_TIMEOUT_MS = parseInt(process.env.JOIN_GAME_TIMEOUT_MS || '60000', 10); (from Part 1)
// const MIN_BET_AMOUNT = parseInt(process.env.MIN_BET_AMOUNT || '5', 10); (from Part 1)
// const MAX_BET_AMOUNT = parseInt(process.env.MAX_BET_AMOUNT || '1000', 10); (from Part 1)
// Assuming TARGET_JACKPOT_SCORE for Dice Escalator is defined (e.g., in Part 5b or Part 1, e.g., const TARGET_JACKPOT_SCORE = 120;)

// --- CONSTANTS FOR DICE 21 ---
const DICE_21_TARGET_SCORE = 21;
const DICE_21_BOT_STAND_SCORE = 17;
// --- END DICE 21 CONSTANTS ---

// --- Main Message Handler (`bot.on('message')`) ---
bot.on('message', async (msg) => {
    if (isShuttingDown) {
        console.log("[MSG_HANDLER] Shutdown in progress, ignoring new message.");
        return;
    }

    if (msg && msg.from && msg.chat) {
        // console.log(`[RAW_MSG_LOG] Text: "${msg.text || 'N/A'}", FromID: ${msg.from.id}, User: @${msg.from.username || 'N/A'}, IsBot: ${msg.from.is_bot}, ChatID: ${msg.chat.id}, MsgID: ${msg.message_id}`);
    } else {
        console.log(`[RAW_MSG_LOG] Received incomplete message object. Msg JSON (partial):`, JSON.stringify(msg).substring(0, 200));
        return;
    }

    if (!msg.from) {
        console.warn("[MSG_HANDLER_WARN] Message received without 'from' field. Ignoring.", msg);
        return;
    }

    if (msg.from.is_bot) {
        try {
            if (!bot || typeof bot.getMe !== 'function') {
                console.error("[MSG_HANDLER_ERROR] Bot instance not available for self-check.");
                return;
            }
            const selfBotInfo = await bot.getMe();
            if (String(msg.from.id) !== String(selfBotInfo.id)) {
                return;
            }
        } catch (getMeError) {
            console.error("[MSG_HANDLER_ERROR] Failed to get self bot info to check message source:", getMeError.message);
            return;
        }
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text || "";
    const chatType = msg.chat.type;
    const messageId = msg.message_id;

    let userForCommandProcessing;
    if (text.startsWith('/')) {
        try {
            userForCommandProcessing = await getUser(userId, msg.from.username, msg.from.first_name); // DB-backed
            if (!userForCommandProcessing) {
                console.error(`[MSG_HANDLER_ERR] getUser returned null/undefined for ${userId}`);
                await safeSendMessage(chatId, "Sorry, there was an issue accessing your user data.");
                return;
            }
        } catch (e) {
            console.error(`[MSG_HANDLER_ERR] Failed to ensure user ${userId} exists for command processing: ${e.message}`);
            await safeSendMessage(chatId, "Sorry, there was a problem accessing your user data. Please try again.");
            return;
        }
    }

    if (!msg.from.is_bot && text.startsWith('/')) {
        const now = Date.now();
        if (userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
            console.log(`[COOLDOWN] User ${userId} command ("${text}") ignored due to cooldown.`);
            return;
        }
        userCooldowns.set(userId, now);
    }

    if (text.startsWith('/') && !msg.from.is_bot) {
        const commandArgs = text.substring(1).split(' ');
        const commandName = commandArgs.shift()?.toLowerCase();

        console.log(`[CMD RCV] Chat: ${chatId}, User: ${userId} (@${msg.from.username || 'N/A'}), Cmd: /${commandName}, Args: [${commandArgs.join(', ')}]`);

        // userForCommandProcessing is already fetched if it was a command

        switch (commandName) {
            case 'start':
            case 'help':
                await handleHelpCommand(chatId, msg.from); // msg.from is okay for display details
                break;
            case 'balance':
            case 'bal':
                await handleBalanceCommand(chatId, msg.from); // msg.from to pass to getUser
                break;
            case 'startcoinflip':
                if (chatType !== 'private') {
                    let betAmountCF = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(betAmountCF) || betAmountCF < MIN_BET_AMOUNT || betAmountCF > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet. ${MIN_BET_AMOUNT}-${MAX_BET_AMOUNT}. Usage: \`/startcoinflip <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupCoinFlipCommand(chatId, userForCommandProcessing, betAmountCF, messageId);
                } else {
                    await safeSendMessage(chatId, "This Coinflip game is for group chats only.", {});
                }
                break;
            case 'startrps':
                if (chatType !== 'private') {
                    let betAmountRPS = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(betAmountRPS) || betAmountRPS < MIN_BET_AMOUNT || betAmountRPS > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet. ${MIN_BET_AMOUNT}-${MAX_BET_AMOUNT}. Usage: \`/startrps <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupRPSCommand(chatId, userForCommandProcessing, betAmountRPS, messageId);
                } else {
                    await safeSendMessage(chatId, "This Rock Paper Scissors game is for group chats only.", {});
                }
                break;
            case 'startdice':
                if (chatType !== 'private') {
                    let betAmountDE = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(betAmountDE) || betAmountDE < MIN_BET_AMOUNT || betAmountDE > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet. ${MIN_BET_AMOUNT}-${MAX_BET_AMOUNT}. Usage: \`/startdice <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    if (typeof handleStartDiceEscalatorCommand === 'function') {
                        await handleStartDiceEscalatorCommand(chatId, userForCommandProcessing, betAmountDE, messageId);
                    } else {
                        console.error("[CMD_ERR] handleStartDiceEscalatorCommand function not found!");
                        await safeSendMessage(chatId, "Error: Dice Escalator game not loaded.");
                    }
                } else {
                    await safeSendMessage(chatId, "The Dice Escalator game is for group chats only.", {});
                }
                break;
            case 'jackpot':
                await handleJackpotCommand(chatId);
                break;
            case 'dice21':
            case 'd21':
                if (chatType !== 'private') {
                    let betAmountD21 = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(betAmountD21) || betAmountD21 < MIN_BET_AMOUNT || betAmountD21 > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet for Dice 21. ${MIN_BET_AMOUNT}-${MAX_BET_AMOUNT}. Usage: \`/dice21 <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    if (typeof handleStartDice21Command === 'function') {
                        await handleStartDice21Command(chatId, userForCommandProcessing, betAmountD21, messageId);
                    } else {
                        console.error("[CMD_ERR] handleStartDice21Command function not found!");
                        await safeSendMessage(chatId, "Error: Dice 21 game not loaded.");
                    }
                } else {
                    await safeSendMessage(chatId, "Dice 21 is for group chats only.", {});
                }
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
    if (isShuttingDown) {
        console.log("[CBQ_HANDLER] Shutdown in progress, ignoring callback.");
        if (bot && typeof bot.answerCallbackQuery === 'function') {
            bot.answerCallbackQuery(callbackQuery.id, { text: "Bot is shutting down, please try again later." }).catch(() => {});
        }
        return;
    }

    const msg = callbackQuery.message;
    const userFromCb = callbackQuery.from;
    const callbackQueryId = callbackQuery.id;
    const data = callbackQuery.data;

    if (!msg || !userFromCb || !data) {
        console.warn(`[CBQ_WARN] Incomplete callback query. ID: ${callbackQueryId}, Data: ${data}`);
        if (bot && typeof bot.answerCallbackQuery === 'function') {
            bot.answerCallbackQuery(callbackQueryId, { text: "Error: Invalid callback data." }).catch(() => {});
        }
        return;
    }

    const userId = String(userFromCb.id);
    const chatId = String(msg.chat.id);
    const originalMessageId = msg.message_id;

    console.log(`[CBQ RCV] Chat: ${chatId}, User: ${userId} (@${userFromCb.username || 'N/A'}), Data: "${data}", OriginalMsgID: ${originalMessageId}`);

    if (bot && typeof bot.answerCallbackQuery === 'function') {
        bot.answerCallbackQuery(callbackQueryId).catch((err) => {
            console.error(`[CBQ_ERROR] Failed to answer callback query ${callbackQueryId}: ${err.message}`);
        });
    }

    let userObjectForCallback;
    try {
        userObjectForCallback = await getUser(userId, userFromCb.username, userFromCb.first_name); // DB-backed
        if (!userObjectForCallback) {
             throw new Error("User data N/A for callback action.");
        }
    } catch(e) {
        console.error(`[CBQ_ERR] Failed to ensure user ${userId} for callback: ${e.message}`);
        safeSendMessage(chatId, "Sorry, an issue processing your action. Try again.");
        return;
    }

    const [action, ...params] = data.split(':');

    try {
        switch (action) {
            case 'join_game':
                if (!params[0]) throw new Error("Missing gameId for join_game.");
                await handleJoinGameCallback(chatId, userObjectForCallback, params[0], originalMessageId);
                break;
            case 'cancel_game':
                if (!params[0]) throw new Error("Missing gameId for cancel_game.");
                await handleCancelGameCallback(chatId, userObjectForCallback, params[0], originalMessageId);
                break;
            case 'rps_choose':
                if (params.length < 2) throw new Error("Missing params for rps_choose.");
                await handleRPSChoiceCallback(chatId, userObjectForCallback, params[0], params[1], originalMessageId);
                break;
            case 'de_roll_prompt': 
                if (!params[0]) throw new Error("Missing gameId for de_roll_prompt.");
                if (typeof handleDiceEscalatorPlayerAction === 'function') {
                    await handleDiceEscalatorPlayerAction(params[0], userId, action, originalMessageId, chatId);
                } else { throw new Error("Dice Escalator action handler N/A."); }
                break;
            case 'de_cashout': 
                if (!params[0]) throw new Error("Missing gameId for de_cashout.");
                if (typeof handleDiceEscalatorPlayerAction === 'function') {
                    await handleDiceEscalatorPlayerAction(params[0], userId, action, originalMessageId, chatId);
                } else { throw new Error("Dice Escalator action handler N/A."); }
                break;
            case 'play_again_de':
                if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid betAmount for play_again_de.");
                const originalBetAmountDE = parseInt(params[0], 10);
                console.log(`[CBQ_PLAY_AGAIN_DE] User ${userId} (${userObjectForCallback.username || userObjectForCallback.firstName || 'N/A'}) starting Dice Escalator again. Bet: ${originalBetAmountDE}`);
                if (bot && typeof bot.editMessageReplyMarkup === 'function') await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartDiceEscalatorCommand === 'function') {
                    await handleStartDiceEscalatorCommand(chatId, userObjectForCallback, originalBetAmountDE, originalMessageId);
                } else { throw new Error("Dice Escalator start handler N/A."); }
                break;
            case 'd21_hit':
                if (!params[0]) throw new Error("Missing gameId for d21_hit.");
                await handleDice21Hit(params[0], userObjectForCallback, originalMessageId);
                break;
            case 'd21_stand':
                if (!params[0]) throw new Error("Missing gameId for d21_stand.");
                await handleDice21Stand(params[0], userObjectForCallback, originalMessageId);
                break;
            case 'play_again_d21':
                 if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid betAmount for play_again_d21.");
                const originalBetAmountD21 = parseInt(params[0], 10);
                console.log(`[CBQ_PLAY_AGAIN_D21] User ${userId} starting Dice 21 again. Bet: ${originalBetAmountD21}`);
                if (bot && typeof bot.editMessageReplyMarkup === 'function') await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
                if (typeof handleStartDice21Command === 'function') {
                    await handleStartDice21Command(chatId, userObjectForCallback, originalBetAmountD21, originalMessageId);
                } else { throw new Error("Dice 21 start handler N/A."); }
                break;
            default:
                console.log(`[CBQ_INFO] Unknown callback: "${action}" Data: ${data}`);
        }
    } catch (error) {
        console.error(`[CBQ_ERROR] Processing callback "${data}" for user ${userId} in ${chatId}:`, error);
        await safeSendMessage(userId, "Sorry, an error occurred. Please try again.", {}).catch(() => {});
    }
});

// --- Command Handler Functions ---

async function handleHelpCommand(chatId, userFromMessage) {
    const userMention = typeof createUserMention === 'function' ? createUserMention(userFromMessage) : (userFromMessage.first_name || `User ${userFromMessage.id}`);
    const jackpotScoreInfo = typeof TARGET_JACKPOT_SCORE !== 'undefined' ? TARGET_JACKPOT_SCORE : 'a high'; // TARGET_JACKPOT_SCORE from Part 5b/global
    const helpTextParts = [
        `👋 Hello ${userMention}\\! Welcome to the Casino Bot v${BOT_VERSION}\\.`, // BOT_VERSION from Part 1
        `\n*Available commands:*`,
        `▫️ \`/help\` \\- This help message\\.`,
        `▫️ \`/balance\` or \`/bal\` \\- Check your credits\\.`,
        `▫️ \`/startcoinflip <bet>\` \\- Coinflip game\\. Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`, // MIN/MAX_BET_AMOUNT from Part 1
        `▫️ \`/startrps <bet>\` \\- Rock Paper Scissors\\. Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/startdice <bet>\` \\- Dice Escalator vs Bot\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/dice21 <bet>\` or \`/d21 <bet>\` \\- Play Dice 21 (Blackjack) vs Bot\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\.`,
        `▫️ \`/jackpot\` \\- Current Dice Escalator jackpot\\. Win by standing with score *${jackpotScoreInfo}+* & beating Bot in Dice Escalator\\!`,
        `\n*Game Notes:*`,
        `➡️ Coinflip/RPS: Click 'Join Game'\\.`,
        `➡️ Dice Escalator: Roll until you Stand or Bust\\.`,
        `➡️ Dice 21: Get up to 21\\. Bot plays after you Stand\\.`,
        `\nHave fun and play responsibly\\!`
    ];
    await safeSendMessage(chatId, helpTextParts.filter(Boolean).join('\n'), { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

async function handleBalanceCommand(chatId, userFromMessage) {
    const user = await getUser(String(userFromMessage.id), userFromMessage.username, userFromMessage.first_name); // DB-backed
    if (!user) {
         await safeSendMessage(chatId, `${typeof createUserMention === 'function' ? createUserMention(userFromMessage) : userFromMessage.first_name}, could not retrieve your balance.`, { parse_mode: 'MarkdownV2' });
         return;
    }
    const balanceMessage = `${typeof createUserMention === 'function' ? createUserMention(userFromMessage) : userFromMessage.first_name}, your balance is: *${formatCurrency(user.balance)}*\\.`;
    await safeSendMessage(chatId, balanceMessage, { parse_mode: 'MarkdownV2' });
}

async function handleJackpotCommand(chatId) {
    try {
        if (typeof queryDatabase !== 'function' || typeof MAIN_JACKPOT_ID === 'undefined' || typeof TARGET_JACKPOT_SCORE === 'undefined' || typeof formatCurrency !== 'function' || typeof escapeMarkdownV2 !== 'function') {
            console.error("[JACKPOT_CMD_ERR] Missing functions/constants for /jackpot.");
            await safeSendMessage(chatId, "Jackpot system unavailable (config error).", {});
            return;
        }
        const result = await queryDatabase('SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]); // MAIN_JACKPOT_ID from Part 1
        if (result.rows.length > 0) {
            const jackpotAmountLamports = BigInt(result.rows[0].current_amount_lamports);
            const jackpotDisplay = formatCurrency(Number(jackpotAmountLamports)); // formatCurrency from Part 3
            await safeSendMessage(chatId, `💰 Current Dice Escalator Jackpot: *${escapeMarkdownV2(jackpotDisplay)}*!\nWin by Standing with score *${TARGET_JACKPOT_SCORE}+* & beating Bot!`, { parse_mode: 'MarkdownV2' });
        } else {
            await safeSendMessage(chatId, "Jackpot info unavailable (not initialized?).", {});
        }
    } catch (error) {
        console.error("[JACKPOT_CMD_ERR] Error fetching jackpot:", error);
        await safeSendMessage(chatId, "Sorry, error fetching jackpot amount.", {});
    }
}

// --- Group Game Flow Functions (Coinflip, RPS - FULLY INTEGRATED) ---

async function handleStartGroupCoinFlipCommand(chatId, initiatorUserObj, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUserObj.id); // initiatorUserObj is full object from getUser
    let chatInfo = null; try { if (bot) chatInfo = await bot.getChat(chatId); } catch (e) { console.warn(`[COINFLIP_START_WARN] No chat info for ${chatId}: ${e.message}`); }
    const chatTitle = chatInfo?.title;
    const gameSession = await getGroupSession(chatId, chatTitle || "Group Chat"); // From Part 2

    if (gameSession.currentGameId && gameSession.currentGameType !== 'DiceEscalator' && gameSession.currentGameType !== 'Dice21') { 
        await safeSendMessage(chatId, `A ${escapeMarkdownV2(gameSession.currentGameType || 'game')} is active. Please wait.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const initiator = initiatorUserObj; 
    if (initiator.balance < betAmount) { 
        await safeSendMessage(chatId, `${createUserMention(initiatorUserObj)}, balance (${formatCurrency(initiator.balance)}) too low for ${formatCurrency(betAmount)} bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const gameId = generateGameId(); 
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_coinflip_init:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `Error starting Coinflip: Bet failed. (${escapeMarkdownV2(balanceUpdateResult.error || '')})`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const gameDataCF = {
        type: 'coinflip', gameId, chatId: String(chatId), initiatorId,
        initiatorMention: createUserMention(initiatorUserObj), betAmount,
        participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUserObj), betPlaced: true }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
    };
    activeGames.set(gameId, gameDataCF); 
    await updateGroupGameDetails(chatId, gameId, 'CoinFlip', betAmount); 

    const joinMsgCF = `${createUserMention(initiatorUserObj)} started *Coin Flip* for ${escapeMarkdownV2(formatCurrency(betAmount))}! Click "Join Game"!`;
    const kbCF = { inline_keyboard: [[{ text: "🪙 Join Coinflip!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]] };
    const setupMsgCF = await safeSendMessage(chatId, joinMsgCF, { parse_mode: 'MarkdownV2', reply_markup: kbCF });

    if (setupMsgCF && activeGames.has(gameId)) {
        activeGames.get(gameId).gameSetupMessageId = setupMsgCF.message_id;
    } else { 
        console.error(`[COINFLIP_START_ERR] Failed to send setup message for ${gameId}. Refunding.`);
        await updateUserBalance(initiatorId, betAmount, `refund_coinflip_setup_fail:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }

    setTimeout(async () => { 
        const gdCF = activeGames.get(gameId);
        if (gdCF && gdCF.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] Coinflip game ${gameId} timed out.`);
            await updateUserBalance(gdCF.initiatorId, gdCF.betAmount, `refund_coinflip_timeout:${gameId}`, null, gameId, String(chatId));
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgTextCF = `Coinflip by ${gdCF.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdCF.betAmount))} expired. Bet refunded.`;
            if (gdCF.gameSetupMessageId && bot) { 
                bot.editMessageText(timeoutMsgTextCF, { chatId: String(chatId), message_id: Number(gdCF.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                    .catch(() => { safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' }); });
            } else { safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' }); }
        }
    }, JOIN_GAME_TIMEOUT_MS); // JOIN_GAME_TIMEOUT_MS from Part 1
}

async function handleStartGroupRPSCommand(chatId, initiatorUserObj, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUserObj.id);
    let chatInfo = null; try { if (bot) chatInfo = await bot.getChat(chatId); } catch (e) { /* ... */ }
    const chatTitle = chatInfo?.title;
    const gameSession = await getGroupSession(chatId, chatTitle || "Group Chat");
    if (gameSession.currentGameId && gameSession.currentGameType !== 'DiceEscalator' && gameSession.currentGameType !== 'Dice21') { /* ... */ return; }
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
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }
    setTimeout(async () => { /* ... timeout logic ... */ 
        const gdRPS = activeGames.get(gameId);
        if (gdRPS && gdRPS.status === 'waiting_opponent') {
            await updateUserBalance(gdRPS.initiatorId, gdRPS.betAmount, `refund_rps_timeout:${gameId}`, null, gameId, String(chatId));
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgTextRPS = `RPS by ${gdRPS.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdRPS.betAmount))} expired. Refunded.`;
            if (gdRPS.gameSetupMessageId && bot) { /* ... edit/send message ... */ }
            else { safeSendMessage(chatId, timeoutMsgTextRPS, { parse_mode: 'MarkdownV2' }); }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleJoinGameCallback(chatId, joinerUserObj, gameId, interactionMessageId) {
    const joinerId = String(joinerUserObj.id); // joinerUserObj is the full user object from getUser
    const gameData = activeGames.get(gameId);

    if (!gameData) { await safeSendMessage(joinerId, "Game not available.", {}); if (interactionMessageId && bot) bot.editMessageReplyMarkup({}, {chat_id:chatId, message_id:interactionMessageId}).catch(()=>{}); return; }
    if (gameData.chatId !== String(chatId)) { await safeSendMessage(joinerId, "Join error (chat mismatch).", {}); return; }
    if (gameData.initiatorId === joinerId) { await safeSendMessage(joinerId, "Can't join your own game.", {}); return; }
    if (gameData.status !== 'waiting_opponent') { await safeSendMessage(joinerId, "Game not waiting for opponent.", {}); if (interactionMessageId && bot) bot.editMessageReplyMarkup({}, {chat_id:chatId, message_id:interactionMessageId}).catch(()=>{}); return; }
    if (gameData.participants.length >= 2 && (gameData.type === 'coinflip' || gameData.type === 'rps')) { await safeSendMessage(joinerId, "Game already full.", {}); if (interactionMessageId && bot) bot.editMessageReplyMarkup({}, {chat_id:chatId, message_id:interactionMessageId}).catch(()=>{}); return; }

    // joinerUserObj is the result of getUser, so its balance is current
    if (joinerUserObj.balance < gameData.betAmount) {
        await safeSendMessage(joinerId, `Balance (${formatCurrency(joinerUserObj.balance)}) too low for this ${formatCurrency(gameData.betAmount)} game.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const balanceUpdateResult = await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_group_${gameData.type}_join:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(joinerId, `Join error: Bet failed. (${escapeMarkdownV2(balanceUpdateResult.error || '')})`, { parse_mode: 'MarkdownV2' });
        return;
    }

    gameData.participants.push({ userId: joinerId, choice: null, mention: createUserMention(joinerUserObj), betPlaced: true });
    activeGames.set(gameId, gameData);
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);

    if (gameData.type === 'coinflip' && gameData.participants.length === 2) {
        gameData.status = 'resolving';
        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        p1.choice = 'heads'; p2.choice = 'tails';

        const cfResult = determineCoinFlipOutcome(); // from Part 4
        let winner = (cfResult.outcome === p1.choice) ? p1 : p2;
        const winningsToCredit = gameData.betAmount + gameData.betAmount; // Bet back + opponent's bet

        await updateUserBalance(winner.userId, winningsToCredit, `won_group_coinflip:${gameId}`, null, gameId, String(chatId));

        const resMsg = `*CoinFlip Resolved!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n` +
                       `${p1.mention} (Heads) vs ${p2.mention} (Tails)\n\n` +
                       `Landed on: *${escapeMarkdownV2(cfResult.outcomeString)}* ${cfResult.emoji}!\n\n` +
                       `🎉 ${winner.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;

        if (messageToEditId && bot) {
            bot.editMessageText(resMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {} })
                .catch(() => { safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' }); });
        } else { safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' }); }
        
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);

    } else if (gameData.type === 'rps' && gameData.participants.length === 2) {
        gameData.status = 'waiting_choices';
        const rpsPrompt = `${gameData.participants[0].mention} & ${gameData.participants[1].mention}, your RPS match for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} is set!\nMake your choice:`;
        const rpsKeyboard = { inline_keyboard: [[
            { text: `${RPS_EMOJIS.ROCK} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
            { text: `${RPS_EMOJIS.PAPER} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
            { text: `${RPS_EMOJIS.SCISSORS} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
        ]] }; // RPS_EMOJIS, RPS_CHOICES from Part 4
        if (messageToEditId && bot) {
            bot.editMessageText(rpsPrompt, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard })
                .catch(() => { safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); });
        } else { safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); }
        activeGames.set(gameId, gameData);
    }
}

async function handleCancelGameCallback(chatId, cancellerUserObj, gameId, interactionMessageId) {
    const cancellerId = String(cancellerUserObj.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== String(chatId)) { await safeSendMessage(cancellerId, "Game not found.", {}); return; }
    if (gameData.initiatorId !== cancellerId) { await safeSendMessage(cancellerId, `Only ${gameData.initiatorMention} can cancel.`, { parse_mode: 'MarkdownV2' }); return; }
    const cancellableStatuses = ['waiting_opponent', 'waiting_choices'];
    if (!cancellableStatuses.includes(gameData.status)) { await safeSendMessage(cancellerId, `Game cannot be cancelled (Status: ${gameData.status}).`, { parse_mode: 'MarkdownV2' }); return; }

    console.log(`[GAME_CANCEL] Game ${gameId} cancelled by ${cancellerId}.`);
    for (const participant of gameData.participants) {
        if (participant.betPlaced) {
            await updateUserBalance(participant.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, null, gameId, String(chatId));
        }
    }

    const gameTypeDisplay = gameData.type.charAt(0).toUpperCase() + gameData.type.slice(1);
    const cancellationMessage = `${gameData.initiatorMention} cancelled ${escapeMarkdownV2(gameTypeDisplay)} (Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}). Bets refunded.`;
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (messageToEditId && bot) {
        bot.editMessageText(cancellationMessage, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {} })
            .catch(() => { safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' }); });
    } else { safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' }); }

    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
}

async function handleRPSChoiceCallback(chatId, userChoiceObj, gameId, choice, interactionMessageId) {
    const userId = String(userChoiceObj.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== String(chatId) || gameData.type !== 'rps' || gameData.status !== 'waiting_choices') { await safeSendMessage(userId, "RPS game not found/ended or not waiting for choices.", {}); return; }
    const participant = gameData.participants.find(p => p.userId === userId);
    if (!participant) { await safeSendMessage(userId, "You're not in this RPS game.", {}); return; }
    if (participant.choice) { await safeSendMessage(userId, `You already chose ${RPS_EMOJIS[participant.choice]}. Waiting.`, { parse_mode: 'MarkdownV2' }); return; }
    
    const normalizedChoiceKey = String(choice).toUpperCase();
    if (!RPS_CHOICES[normalizedChoiceKey]) { await safeSendMessage(userId, "Invalid RPS choice.", {}); return; }

    participant.choice = RPS_CHOICES[normalizedChoiceKey];
    await safeSendMessage(userId, `You chose ${RPS_EMOJIS[participant.choice]}! Waiting for opponent...`, { parse_mode: 'MarkdownV2' });
    activeGames.set(gameId, gameData);

    const otherPlayer = gameData.participants.find(p => p.userId !== userId);
    let groupUpdateMsg = `${participant.mention} has made their choice!`;
    let keyboardForUpdate = {}; 

    if (otherPlayer && !otherPlayer.choice) {
        groupUpdateMsg += ` Waiting for ${otherPlayer.mention}...`;
        keyboardForUpdate = { inline_keyboard: [[ /* ... RPS buttons ... */ 
            { text: `${RPS_EMOJIS.ROCK} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
            { text: `${RPS_EMOJIS.PAPER} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
            { text: `${RPS_EMOJIS.SCISSORS} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
        ]] };
    }

    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (messageToEditId && bot) {
        bot.editMessageText(groupUpdateMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: keyboardForUpdate })
            .catch(()=>{}); // Fail silently or log
    }

    const allChosen = gameData.participants.length === 2 && gameData.participants.every(p => p.choice);
    if (allChosen) {
        gameData.status = 'game_over';
        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        const rpsRes = determineRPSOutcome(p1.choice, p2.choice); // From Part 4

        let winnerParticipant = null;
        let resultText = `*RPS Result!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n` +
                         `${p1.mention}: ${RPS_EMOJIS[p1.choice]} vs ${p2.mention}: ${RPS_EMOJIS[p2.choice]}\n\n` +
                         `${escapeMarkdownV2(rpsRes.description)}\n\n`;

        if (rpsRes.result === 'win1') winnerParticipant = p1;
        else if (rpsRes.result === 'win2') winnerParticipant = p2;

        if (winnerParticipant) {
            const winningsToCredit = gameData.betAmount + gameData.betAmount;
            await updateUserBalance(winnerParticipant.userId, winningsToCredit, `won_rps:${gameId}`, null, gameId, String(chatId));
            resultText += `🎉 ${winnerParticipant.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;
        } else if (rpsRes.result === 'draw') {
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, null, gameId, String(chatId));
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, null, gameId, String(chatId));
            resultText += `Draw! Bets refunded.`;
        } else { 
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_error:${gameId}`, null, gameId, String(chatId));
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_error:${gameId}`, null, gameId, String(chatId));
            resultText = `Error resolving RPS. Bets refunded.`;
        }

        const finalMsgIdToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
        if (finalMsgIdToEdit && bot) {
            bot.editMessageText(resultText, { chatId: String(chatId), message_id: finalMsgIdToEdit, parse_mode: 'MarkdownV2', reply_markup: {} })
                .catch(() => { safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2' }); });
        } else { safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2' }); }

        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
}

// --- NEW DICE 21 GAME HANDLER FUNCTIONS ---

async function handleStartDice21Command(chatId, userObj, betAmount, commandMessageId) {
    const userId = String(userObj.id);
    const playerRef = getPlayerDisplayReference(userObj); // From Part 3
    const gameId = generateGameId(); // From Part 3

    const balanceUpdateResult = await updateUserBalance(userId, -betAmount, `bet_placed_dice21:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${playerRef}, bet failed for Dice 21: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown")}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    await safeSendMessage(chatId, `🎲 ${playerRef} starts Dice 21! Bet: ${formatCurrency(betAmount)}. Dealing first two dice...`, { parse_mode: 'MarkdownV2' });
    await sleep(1000); 

    let initialRollsValues = [];
    let playerScore = 0n; // Use BigInt for scores
    for (let i = 0; i < 2; i++) {
        const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
        initialRollsValues.push(diceMsg.dice.value);
        playerScore += BigInt(diceMsg.dice.value);
        await sleep(2000); // Time to see dice animation + result from Telegram
    }

    const gameData = {
        type: 'dice21', gameId, chatId, userId, playerRef,
        betAmount: BigInt(betAmount), playerScore, botScore: 0n,
        status: 'player_turn', gameMessageId: null 
    };

    let messageText = `${playerRef}, your rolls ${formatDiceRolls(initialRollsValues)} total: *${playerScore}*.`;
    let buttons = []; // Array for the row of buttons

    if (playerScore > BigInt(DICE_21_TARGET_SCORE)) {
        messageText += `\n\n💥 BUST! Over ${DICE_21_TARGET_SCORE}. You lose ${formatCurrency(betAmount)}.`;
        gameData.status = 'game_over_player_bust';
        // Log loss in bets table
        await queryDatabase(
            `UPDATE bets SET status = 'lost', processed_at = NOW() 
             WHERE user_id = $1 AND bet_details->>'game_id' = $2 AND status = 'active'`,
             [userId, gameId]
        ).catch(e => console.error("[D21_START_ERR] Error logging bust on deal:", e));
        buttons.push({ text: `🎲 Play Dice 21 Again (${formatCurrency(betAmount)})`, callback_data: `play_again_d21:${betAmount}` });
    } else if (playerScore === BigInt(DICE_21_TARGET_SCORE)) {
        messageText += `\n\n✨ Perfect ${DICE_21_TARGET_SCORE}! You Stand. Bot's turn...`;
        gameData.status = 'bot_turn';
        // No buttons here, proceed to bot turn
    } else {
        messageText += `\n\nHit or Stand?`;
        buttons.push({ text: " Hit (Roll Again)", callback_data: `d21_hit:${gameId}` });
        buttons.push({ text: `✅ Stand (Score: ${playerScore})`, callback_data: `d21_stand:${gameId}` });
    }
    
    const keyboard = { inline_keyboard: [buttons] }; // Buttons in a single row
    const sentMsg = await safeSendMessage(chatId, messageText, { parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboard : {}) });
    
    if (sentMsg) gameData.gameMessageId = sentMsg.message_id;
    activeGames.set(gameId, gameData);

    if (gameData.status === 'bot_turn') { // Auto-stand on natural 21
        await sleep(1500);
        await processDice21BotTurn(gameData, gameData.gameMessageId);
    } else if (gameData.status.startsWith('game_over')) {
        activeGames.delete(gameId); // Clean up if busted on deal
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
    const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
    const newRoll = BigInt(diceMsg.dice.value);
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
        buttons.push({ text: " Hit (Roll Again)", callback_data: `d21_hit:${gameId}` });
        buttons.push({ text: `✅ Stand (Score: ${gameData.playerScore})`, callback_data: `d21_stand:${gameId}` });
    }
    
    const keyboard = { inline_keyboard: [buttons] };
    await bot.editMessageText(messageText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboard : {}) });
    
    if (gameData.status.startsWith('game_over')) {
        activeGames.delete(gameId);
    } else {
        activeGames.set(gameId, gameData);
        if (gameData.status === 'bot_turn') { // Auto-stand on 21
             await sleep(1500);
             await processDice21BotTurn(gameData, originalMessageId);
        }
    }
}

async function handleDice21Stand(gameId, userObj, originalMessageId) {
    const gameData = activeGames.get(gameId);
    const userId = String(userObj.id);

    if (!gameData || gameData.userId !== userId || gameData.status !== 'player_turn') {
        await safeSendMessage(userId, "Not your Dice 21 game or turn to stand.", {}); return;
    }

    gameData.status = 'bot_turn';
    activeGames.set(gameId, gameData);

    const messageText = `${gameData.playerRef} stands with *${gameData.playerScore}*.\n\n🤖 Bot's turn...`;
    await bot.editMessageText(messageText, { chat_id: gameData.chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {} });
    await sleep(1500);
    await processDice21BotTurn(gameData, originalMessageId);
}

async function processDice21BotTurn(gameData, messageId) {
    const { gameId, chatId, userId, playerRef, playerScore, betAmount } = gameData; // playerScore is BigInt, betAmount is BigInt
    let botScore = 0n;
    let botBusted = false;
    let botRollsDisplayAccumulator = `${playerRef}'s score: *${playerScore}*\nBot is playing...\n`;

    await bot.editMessageText(botRollsDisplayAccumulator + "\n_Bot is rolling..._ 🎲", {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: {}});
    await sleep(1000);

    while (botScore < BigInt(DICE_21_BOT_STAND_SCORE)) {
        const diceMsg = await bot.sendDice(chatId, { emoji: '🎲' });
        const roll = BigInt(diceMsg.dice.value);
        botScore += roll;
        botRollsDisplayAccumulator += `Bot rolled ${formatDiceRolls([Number(roll)])} → Bot score: *${botScore}*\n`;
        
        // Edit the main message with each bot roll update.
        await bot.editMessageText(botRollsDisplayAccumulator + (botScore < BigInt(DICE_21_BOT_STAND_SCORE) && botScore <= BigInt(DICE_21_TARGET_SCORE) ? "\n_Bot hits..._ 🎲" : ""), {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: {}});
        await sleep(2500); // Time to see dice and message

        if (botScore > BigInt(DICE_21_TARGET_SCORE)) {
            botBusted = true;
            botRollsDisplayAccumulator += "💥 Bot BUSTED!\n";
            await bot.editMessageText(botRollsDisplayAccumulator, {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: {}});
            break;
        }
    }
    if (!botBusted && botScore >= BigInt(DICE_21_BOT_STAND_SCORE)) {
        botRollsDisplayAccumulator += `Bot stands with *${botScore}*.\n`;
        await bot.editMessageText(botRollsDisplayAccumulator, {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: {}});
    }
    await sleep(1500);

    let resultTextEnd = "";
    let payoutAmount = 0n; // This is the amount to credit back (bet_return + profit, or just bet_return)
    let outcomeReasonLog = "";

    if (botBusted) {
        resultTextEnd = `🎉 ${playerRef} wins! The Bot busted.`;
        payoutAmount = betAmount + betAmount; // Bet back + winnings equal to bet
        outcomeReasonLog = `won_dice21_bot_bust:${gameId}`;
    } else if (playerScore > botScore) {
        resultTextEnd = `🎉 ${playerRef} wins with *${playerScore}* vs Bot's *${botScore}*!`;
        payoutAmount = betAmount + betAmount;
        outcomeReasonLog = `won_dice21_score:${gameId}`;
    } else if (botScore > playerScore) {
        resultTextEnd = `💀 The Bot wins with *${botScore}* vs ${playerRef}'s *${playerScore}*. You lose your bet.`;
        payoutAmount = 0n; // Player loses bet (already deducted)
        outcomeReasonLog = `lost_dice21_score:${gameId}`;
    } else { // Push
        resultTextEnd = `😐 Push! Both scored *${playerScore}*. Your bet is returned.`;
        payoutAmount = betAmount; // Bet returned
        outcomeReasonLog = `push_dice21:${gameId}`;
    }
    
    if (payoutAmount > 0n) {
        await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    } else if (outcomeReasonLog.startsWith('lost_')) {
        // Log the loss status if no payout (bet already deducted)
        await queryDatabase(
            `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', reason_tx = $1, processed_at = NOW() 
             WHERE user_id = $2 AND bet_details->>'game_id' = $3 AND status = 'active'`,
             [outcomeReasonLog, userId, gameId]
        ).catch(e => console.error("[D21_BOT_TURN_ERR] Error logging player loss:", e));
    }
    
    const finalMessageText = botRollsDisplayAccumulator + "\n" + resultTextEnd;
    const playAgainKeyboardD21 = { inline_keyboard: [[{ text: `🎲 Play Dice 21 Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_d21:${betAmount}` }]] };
    
    await bot.editMessageText(finalMessageText, {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboardD21 });

    activeGames.delete(gameId);
    console.log(`[DICE21_END] Game ${gameId} ended. Player: ${playerScore}, Bot: ${botScore}, BotBusted: ${botBusted}`);
}

console.log("Part 5a: Message & Callback Handling (Core & Basic Games) - Complete.");
// --- End of Part 5a ---
//---------------------------------------------------------------------------
// --- Start of Part 5b ---
// index.js - Part 5b: Dice Escalator Game Logic (Revised "Stand and Challenge" with Animated Bot Rolls)
//---------------------------------------------------------------------------
console.log("Loading Part 5b: Dice Escalator Game Logic (Revised)...");

// Constants for Dice Escalator DB Polling (Player's first roll)
const DB_POLLING_INTERVAL_MS = 2000; // Check DB every 2 seconds
const DB_POLLING_TIMEOUT_MS = 30000; // Give up after 30 seconds
const activeDbPolls = new Map(); // Tracks active polling for player's first roll

// Game Constants for Dice Escalator
const DICE_ESCALATOR_BOT_ROLLS = 3; // Max rolls for the bot
// DICE_ESCALATOR_BUST_ON is defined in Part 4 (should be 1)
const TARGET_JACKPOT_SCORE = 120;    // Player must stand with at least this score AND win vs bot for Jackpot

// Assumed to be defined (e.g., in Part 1 or top of Part 2) for jackpot contribution:
// const JACKPOT_CONTRIBUTION_PERCENT = 0.01; // 1%
// const MAIN_JACKPOT_ID = 'dice_escalator_main';


// --- Definition of the DB Polling Function (for player's first roll) ---
async function startPollingForDbResult(gameId, userId, chatId, messageIdToUpdate) {
    if (activeDbPolls.has(gameId)) {
        console.log(`[DB_POLL_WARN] Polling already active for game ${gameId}. Ignoring duplicate start request.`);
        return;
    }
    console.log(`[DB_POLL_START] Starting DB poll for player's roll (game ${gameId}).`);
    const pollingStartTime = Date.now();
    const pollIntervalId = setInterval(async () => {
        if (Date.now() - pollingStartTime > DB_POLLING_TIMEOUT_MS) {
            console.error(`[DB_POLL_ERR] Timeout polling DB for game ${gameId}.`);
            clearInterval(pollIntervalId);
            activeDbPolls.delete(gameId);
            const gameData = activeGames.get(gameId);
            if (gameData) {
                const playerRef = gameData.playerReference || `User ${userId}`;
                const timeoutMsg = `⏳ Oh dear, ${playerRef}! We waited, but the dice roll result didn't arrive for game \`${escapeMarkdownV2(gameId)}\`. The game has been cancelled.`;
                // This is likely for the very first roll if polling times out.
                // Bet was already placed. Refund it.
                if (gameData.betAmount && typeof updateUserBalance === 'function') {
                    await updateUserBalance(userId, gameData.betAmount, `refund_de_db_poll_timeout:${gameId}`, null, gameId, chatId)
                        .catch(e => console.error(`[DB_POLL_ERR] Failed refund on poll timeout for ${gameId}: ${e.message}`));
                    safeSendMessage(chatId, timeoutMsg + " Your bet has been refunded.", { parse_mode: 'MarkdownV2' });
                } else {
                    safeSendMessage(chatId, timeoutMsg, { parse_mode: 'MarkdownV2' });
                }
                activeGames.delete(gameId);
                if (typeof queryDatabase === 'function') {
                    queryDatabase("DELETE FROM dice_roll_requests WHERE game_id = $1 AND status = 'pending'", [gameId]).catch(() => {});
                }
            }
            return;
        }
        const currentGameData = activeGames.get(gameId);
        if (!currentGameData || currentGameData.status !== 'waiting_db_roll') {
            console.log(`[DB_POLL_STOP] Game ${gameId} no longer waiting for DB roll (Status: ${currentGameData?.status}). Stopping poll.`);
            clearInterval(pollIntervalId);
            activeDbPolls.delete(gameId);
            return;
        }
        try {
            const dbResult = await queryDatabase('SELECT roll_value, status FROM dice_roll_requests WHERE game_id = $1', [gameId]);
            if (dbResult.rows.length > 0) {
                const { roll_value, status } = dbResult.rows[0];
                if (status === 'completed' && roll_value !== null && Number.isInteger(roll_value)) {
                    console.log(`[DB_POLL_SUCCESS] Received roll ${roll_value} for game ${gameId}.`);
                    clearInterval(pollIntervalId);
                    activeDbPolls.delete(gameId);
                    const finalGameData = activeGames.get(gameId);
                    if (finalGameData && finalGameData.status === 'waiting_db_roll') {
                        await processDiceEscalatorPlayerRoll(finalGameData, roll_value, messageIdToUpdate);
                    } else {
                        console.warn(`[DB_POLL_WARN] Game ${gameId} status changed before processing roll ${roll_value}. Status: ${finalGameData?.status}`);
                    }
                } else if (status === 'error') {
                    console.error(`[DB_POLL_ERR] DB request for game ${gameId} has status 'error'.`);
                    clearInterval(pollIntervalId);
                    activeDbPolls.delete(gameId);
                    const gameData = activeGames.get(gameId);
                    if (gameData) {
                        const playerRef = gameData.playerReference || `User ${userId}`;
                        const errorMsg = `⚙️ Uh oh, ${playerRef}! Issue generating dice roll for game \`${escapeMarkdownV2(gameId)}\`. Cancelled.`;
                        if (gameData.betAmount) {
                            await updateUserBalance(userId, gameData.betAmount, `refund_de_db_roll_error:${gameId}`, null, gameId, chatId)
                                .catch(e => console.error(`[DB_POLL_ERR] Failed refund on DB error for ${gameId}: ${e.message}`));
                            safeSendMessage(chatId, errorMsg + " Your bet has been refunded.", { parse_mode: 'MarkdownV2' });
                        } else {
                            safeSendMessage(chatId, errorMsg, { parse_mode: 'MarkdownV2' });
                        }
                        activeGames.delete(gameId);
                    }
                }
            } else { // No row found - critical error if insert was confirmed
                console.error(`[DB_POLL_ERR] No row found for game_id ${gameId} during polling. Critical error.`);
                clearInterval(pollIntervalId);
                activeDbPolls.delete(gameId);
                const gameData = activeGames.get(gameId);
                if (gameData) {
                    const playerRef = gameData.playerReference || `User ${userId}`;
                    safeSendMessage(chatId, `⚠️ System error for game ${escapeMarkdownV2(gameId)}, ${playerRef}. Please try again.`, { parse_mode: 'MarkdownV2' });
                    if (gameData.betAmount) await updateUserBalance(userId, gameData.betAmount, `refund_de_db_poll_critical:${gameId}`, null, gameId, chatId);
                    activeGames.delete(gameId);
                }
            }
        } catch (dbError) {
            console.error(`[DB_POLL_ERR] DB query error during poll for game ${gameId}:`, dbError.message);
        }
    }, DB_POLLING_INTERVAL_MS);
    activeDbPolls.set(gameId, pollIntervalId);
}

// --- Dice Escalator Command Handler (/startdice) ---
async function handleStartDiceEscalatorCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    const initiatorUsername = initiatorUser.username;
    const initiatorFirstName = initiatorUser.first_name;

    console.log(`[DE_START_INFO] Initiating Dice Escalator for ${initiatorId} in chat ${chatId}. Bet: ${betAmount}. Multiple games allowed.`);

    const gameId = generateGameId();
    const playerRef = getPlayerDisplayReference(initiatorUser);
    const betFormatted = formatCurrency(betAmount); // Use non-escaped for logs, escape for messages

    const initiator = await getUser(initiatorId, initiatorUsername, initiatorFirstName); // Ensures user exists in DB
    if (!initiator) {
         await safeSendMessage(chatId, `${playerRef}, error retrieving your user data. Please try again.`, {parse_mode: 'MarkdownV2'});
         return;
    }
    // Using DB-backed balance check
    if (initiator.balance < betAmount) { // Note: getUser returns balance as Number
        await safeSendMessage(chatId, `${playerRef}, your balance of ${formatCurrency(initiator.balance)} is too low for a *${escapeMarkdownV2(betFormatted)}* bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Using DB-backed updateUserBalance. Pass gameId and chatId for logging/jackpot.
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_dice_escalator:${gameId}`, null, gameId, String(chatId));
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `${playerRef}, couldn't place your bet: ${escapeMarkdownV2(balanceUpdateResult.error || 'Unknown reason')}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    let currentJackpotDisplay = "";
    try {
        const jackpotResult = await queryDatabase('SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]);
        if (jackpotResult.rows.length > 0) {
            const jackpotAmount = BigInt(jackpotResult.rows[0].current_amount_lamports);
            if (jackpotAmount > 0) {
                 currentJackpotDisplay = `\n\n💰 Current Jackpot: *${escapeMarkdownV2(formatCurrency(Number(jackpotAmount)))}*!`;
            }
        }
    } catch (e) { console.warn("[JACKPOT_DISPLAY_ERR] Could not fetch jackpot for game start message:", e.message); }

    const initialPlaceholderText = `🔥 *Dice Escalator: The Challenge Begins!* 🔥\n${playerRef} bets *${escapeMarkdownV2(betFormatted)}* against The House!${currentJackpotDisplay}\n\n_The dice are poised... summoning the first roll!_ ⏳`;
    const sentPlaceholderMsg = await safeSendMessage(chatId, initialPlaceholderText, { parse_mode: 'MarkdownV2' });

    if (!sentPlaceholderMsg) {
        console.error(`[DE_START_ERR] Failed to send initial Dice Escalator message for ${gameId}. Refunding bet.`);
        await updateUserBalance(initiatorId, betAmount, `refund_de_setup_fail:${gameId}`, null, gameId, String(chatId)); // Refund
        return;
    }

    const gameData = {
        type: 'dice_escalator', gameId, chatId: String(chatId), initiatorId,
        initiatorUsername, initiatorFirstName, playerReference: playerRef,
        betAmount: BigInt(betAmount), playerScore: 0n, botScore: 0n, // Use BigInt for scores too
        status: 'waiting_db_roll', currentPlayerId: initiatorId,
        bustValue: DICE_ESCALATOR_BUST_ON, creationTime: Date.now(),
        gameSetupMessageId: sentPlaceholderMsg.message_id
    };
    activeGames.set(gameId, gameData);

    try {
        console.log(`[DB_ROLL_REQUEST] Inserting FIRST roll request for DE game ${gameId}`);
        await queryDatabase(
            'INSERT INTO dice_roll_requests (game_id, chat_id, user_id, status, requested_at) VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (game_id) DO UPDATE SET status = EXCLUDED.status, requested_at = EXCLUDED.requested_at, roll_value = NULL, processed_at = NULL',
            [gameId, String(chatId), initiatorId, 'pending']
        );
        startPollingForDbResult(gameId, initiatorId, String(chatId), sentPlaceholderMsg.message_id);
    } catch (error) {
        console.error(`[DE_START_ERR] DB interaction error for FIRST roll ${gameId}:`, error.message);
        const errorText = `🚧 Game Start Glitch, ${playerRef}. Your bet of *${escapeMarkdownV2(betFormatted)}* refunded. Please try again!`;
        if (sentPlaceholderMsg) {
            bot.editMessageText(errorText, { chatId: String(chatId), message_id: sentPlaceholderMsg.message_id, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>safeSendMessage(String(chatId), errorText, {parse_mode:'MarkdownV2'}));
        } else {
            safeSendMessage(String(chatId), errorText, {parse_mode:'MarkdownV2'});
        }
        await updateUserBalance(initiatorId, betAmount, `refund_de_first_roll_dberr:${gameId}`, null, gameId, String(chatId));
        activeGames.delete(gameId);
    }
}

// --- Processes the player's roll in Dice Escalator ---
async function processDiceEscalatorPlayerRoll(gameData, playerRollValue, messageIdToUpdate) {
    const playerRoll = BigInt(playerRollValue); // Ensure roll is BigInt
    const { gameId, chatId, bustValue, initiatorId, initiatorUsername, initiatorFirstName } = gameData;
    const playerRef = gameData.playerReference || getPlayerDisplayReference({ id: initiatorId, username: initiatorUsername, first_name: initiatorFirstName });
    const betFormatted = formatCurrency(Number(gameData.betAmount));
    const diceFormatted = formatDiceRolls([Number(playerRoll)]);
    const bustDiceFormatted = formatDiceRolls([bustValue]);

    if (playerRoll <= 0n || playerRoll > 6n) { // Invalid roll from helper
        console.error(`[ROLL_INVALID] Invalid roll value ${playerRoll} from DB for game ${gameId}.`);
        gameData.status = 'player_turn_prompt_action';
        activeGames.set(gameId, gameData);
        let invalidRollMsg = `🤔 Odd Roll Data, ${playerRef}!\nReceived an invalid roll (${playerRoll}). Your score is *${gameData.playerScore}*. Try again or Stand.`;
        const kbInvalid = { inline_keyboard: [[{ text: `🎲 Roll Again! (Score: ${gameData.playerScore})`, callback_data: `de_roll_prompt:${gameId}` }], [{ text: `✅ Stand (Score: ${gameData.playerScore})`, callback_data: `de_cashout:${gameId}` }]] };
        if (messageIdToUpdate) bot.editMessageText(invalidRollMsg, { chat_id: String(chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: kbInvalid }).catch(() => safeSendMessage(String(chatId), invalidRollMsg, { parse_mode: 'MarkdownV2', reply_markup: kbInvalid }));
        else safeSendMessage(String(chatId), invalidRollMsg, { parse_mode: 'MarkdownV2', reply_markup: kbInvalid });
        return;
    }

    const msgId = Number(messageIdToUpdate || gameData.gameSetupMessageId);

    if (playerRoll === BigInt(bustValue)) { // Player BUSTS
        gameData.status = 'game_over_player_bust';
        gameData.playerScore = 0n; // Score resets
        const turnResMsg = `*Your Roll: ${diceFormatted}... Oh No!* 😱\n\n` +
                            `💥 *BUSTED!* 💥\n` +
                            `Ouch, ${playerRef}! Rolling a ${bustDiceFormatted} means your score crumbles to zero! The House claims your *${escapeMarkdownV2(betFormatted)}* bet.\n\n` +
                            `_Better luck next time!_`;
        activeGames.delete(gameId);
        // Bet already deducted, player loses it. Log the loss.
        await queryDatabase(
            `UPDATE bets SET status = 'lost', processed_at = NOW() 
             WHERE user_id = $1 AND bet_details->>'game_id' = $2 AND status = 'active'`,
             [initiatorId, gameId]
        ).catch(e => console.error("Error logging player bust to bets table:", e));

        const playAgainKeyboard = { inline_keyboard: [[{ text: `🎲 Play Again (${betFormatted})`, callback_data: `play_again_de:${gameData.betAmount}` }]] };
        if (msgId) bot.editMessageText(turnResMsg, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboard }).catch(() => safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboard }));
        else safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboard });
    } else { // Player does NOT bust
        gameData.playerScore += playerRoll;
        gameData.status = 'player_turn_prompt_action';
        activeGames.set(gameId, gameData);

        const scoreFormatted = escapeMarkdownV2(String(gameData.playerScore));
        const turnResMsg = `🎲 *Roll Result: ${diceFormatted}!* 🎲\n` +
                             `Nice roll, ${playerRef}! Your score is now *${scoreFormatted}*.\n\n` +
                             `💰 Current Score to Stand on: *${scoreFormatted}*\n` +
                             `🔥 _Remember, a ${bustDiceFormatted} means BUST!_ 💥\n\n` +
                             `What's your move?`;
        const standButtonText = `✅ Stand (Score: ${gameData.playerScore})`;
        const kb = { inline_keyboard: [
             [{ text: `🎲 Roll Again! (Score: ${gameData.playerScore})`, callback_data: `de_roll_prompt:${gameId}` }],
             [{ text: standButtonText, callback_data: `de_cashout:${gameId}` }] // "de_cashout" action now means "Stand"
        ]};
        if (msgId) bot.editMessageText(turnResMsg, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: kb }).catch(() => safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2', reply_markup: kb }));
        else safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2', reply_markup: kb });
    }
    console.log(`[ROLL_PROCESS_COMPLETE] Game ${gameId}: status=${gameData.status}, score=${gameData.playerScore}`);
}

// --- Dice Escalator Callback Handler (for "Roll Again" or "Stand") ---
async function handleDiceEscalatorPlayerAction(gameId, userId, actionType, interactionMessageId, chatId) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== String(chatId) || gameData.type !== 'dice_escalator' || gameData.initiatorId !== String(userId)) {
        await safeSendMessage(userId, "This game isn't available or it's not your turn.", {});
        if (interactionMessageId && bot) bot.editMessageReplyMarkup({}, {chat_id:String(chatId), message_id:Number(interactionMessageId)}).catch(()=>{});
        return;
    }
    const playerRef = gameData.playerReference || getPlayerDisplayReference({id: userId, username: gameData.initiatorUsername, first_name: gameData.initiatorFirstName});
    const msgIdToUpdate = Number(interactionMessageId || gameData.gameSetupMessageId);

    if (gameData.status !== 'player_turn_prompt_action') {
        await safeSendMessage(userId, `You can't ${actionType === 'de_roll_prompt' ? 'roll again' : 'stand'} right now. Game status: ${escapeMarkdownV2(gameData.status)}`, {parse_mode:'MarkdownV2'});
        return;
    }

    if (actionType === 'de_roll_prompt') { // Player wants to Roll Again
        gameData.status = 'waiting_db_roll';
        activeGames.set(gameId, gameData);
        const promptMessageText = `🎲 _Rolling again for ${playerRef}! Fetching that next roll..._ ⏳`;
        if (msgIdToUpdate) bot.editMessageText(promptMessageText, { chat_id: String(chatId), message_id: msgIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>safeSendMessage(chatId, promptMessageText, {parse_mode:'MarkdownV2'}));
        else safeSendMessage(chatId, promptMessageText, {parse_mode:'MarkdownV2'});

        try {
            await queryDatabase('INSERT INTO dice_roll_requests (game_id, chat_id, user_id, status, requested_at) VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (game_id) DO UPDATE SET status = EXCLUDED.status, requested_at = EXCLUDED.requested_at, roll_value = NULL, processed_at = NULL', [gameId, String(chatId), userId, 'pending']);
            startPollingForDbResult(gameId, userId, String(chatId), msgIdToUpdate);
        } catch (error) {
            console.error(`[DE_ACTION_ERR] DB insert/poll for subsequent roll ${gameId}:`, error.message);
            gameData.status = 'player_turn_prompt_action'; // Revert status
            activeGames.set(gameId, gameData);
            const errorRestoreMsg = `⚙️ Oops, ${playerRef}! Couldn't process that roll request. Your score is *${gameData.playerScore}*. Try again or Stand.`;
            const standButtonTextErr = `✅ Stand (Score: ${gameData.playerScore})`;
            const kbError = { inline_keyboard:[[{text:`🎲 Roll Again! (Score: ${gameData.playerScore})`,callback_data:`de_roll_prompt:${gameId}`}], [{text:standButtonTextErr, callback_data:`de_cashout:${gameId}`}]] };
            if (msgIdToUpdate) bot.editMessageText(errorRestoreMsg, {chat_id:String(chatId), message_id:msgIdToUpdate, parse_mode:'MarkdownV2', reply_markup:kbError}).catch(()=>safeSendMessage(chatId, errorRestoreMsg, {parse_mode:'MarkdownV2', reply_markup:kbError}));
            else safeSendMessage(chatId, errorRestoreMsg, {parse_mode:'MarkdownV2', reply_markup:kbError});
        }
    } else if (actionType === 'de_cashout') { // Player wants to Stand
        if (gameData.playerScore <= 0n) { // Cannot stand with zero or negative score
             await safeSendMessage(userId, "You need a score greater than 0 to Stand!", {});
             return;
        }
        gameData.status = 'player_stood_waiting_bot';
        activeGames.set(gameId, gameData);
        const standMsgText = `✅ ${playerRef} stands with a score of *${gameData.playerScore}*!\n\n` +
                             `🤖 *Now, The House Responds!* 🤖\nCan the Bot beat this score? Let's see...`;
        if (msgIdToUpdate) bot.editMessageText(standMsgText, { chatId: String(chatId), message_id: msgIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>safeSendMessage(chatId, standMsgText, {parse_mode:'MarkdownV2'}));
        else safeSendMessage(chatId, standMsgText, {parse_mode:'MarkdownV2'});

        await sleep(1500); // Brief pause
        await processDiceEscalatorBotTurn(gameData, msgIdToUpdate);
    }
}

// --- Processes the Bot's turn (ANIMATED ROLLS & REVISED PAYOUTS) ---
async function processDiceEscalatorBotTurn(gameData, messageIdToUpdate) {
    const { gameId, chatId, bustValue, initiatorId, initiatorUsername, initiatorFirstName, betAmount } = gameData;
    const playerRef = gameData.playerReference || getPlayerDisplayReference({ id: initiatorId, username: initiatorUsername, first_name: initiatorFirstName });
    const playerScoreStood = gameData.playerScore; // Player's final score they stood on

    console.log(`[DE_BOT_TURN] Bot starting turn for game ${gameId}. Player stood with ${playerScoreStood}. Bet: ${betAmount}`);

    let botScore = 0n;
    let botBusted = false;
    let rollsMadeByBot = 0;
    let botRollsLog = []; // To accumulate text of bot rolls

    // Initial message update that bot's turn started (if not already done by handleDiceEscalatorPlayerAction)
    // The message currently being edited (messageIdToUpdate) should already show player stood.
    // We will send new messages for each bot dice animation, then update the main game message.

    await sleep(1000); // Pause before bot's first roll

    while (rollsMadeByBot < DICE_ESCALATOR_BOT_ROLLS && botScore <= playerScoreStood && !botBusted) {
        rollsMadeByBot++;
        let botRollAnnounce = `Bot Roll ${rollsMadeByBot}/${DICE_ESCALATOR_BOT_ROLLS}...`;
        await safeSendMessage(chatId, botRollAnnounce, {}); // Announce roll attempt

        let currentRollValue = 0;
        try {
            const diceMessage = await bot.sendDice(chatId, { emoji: '🎲' });
            currentRollValue = BigInt(diceMessage.dice.value);
            await sleep(2500); // Wait for dice animation to be seen
        } catch (e) {
            console.error(`[DE_BOT_TURN_ERR] Failed to send animated dice for bot roll ${rollsMadeByBot}, game ${gameId}. Using internal roll. Error: ${e.message}`);
            // Fallback to internal roll if sendDice fails
            const internalRollResult = determineDieRollOutcome(); // From Part 4, returns {roll: number}
            currentRollValue = BigInt(internalRollResult.roll);
            await safeSendMessage(chatId, `Bot (internal roll ${rollsMadeByBot}): ${formatDiceRolls([Number(currentRollValue)])}`);
            await sleep(1000);
        }

        botRollsLog.push(`Roll ${rollsMadeByBot}: ${formatDiceRolls([Number(currentRollValue)])}`);

        if (currentRollValue === BigInt(bustValue)) {
            botBusted = true;
            botScore = 0n;
            botRollsLog[botRollsLog.length-1] += " → 💥 BUSTED!";
            break; 
        } else {
            botScore += currentRollValue;
            botRollsLog[botRollsLog.length-1] += ` → Total: ${botScore}`;
        }
        // Update main game message with cumulative log
        let tempBotTurnStatus = `🤖 *Bot's Turn Ongoing...*\nPlayer Stood Score: *${playerScoreStood}*\n\n${botRollsLog.join('\n')}`;
        if (messageIdToUpdate) bot.editMessageText(tempBotTurnStatus, { chatId, message_id: Number(messageIdToUpdate), parse_mode:'MarkdownV2', reply_markup:{} }).catch(()=>{/*ignore edit error during rolls*/});

        if (botScore > playerScoreStood) break; // Bot wins, no need for more rolls
        if (rollsMadeByBot < DICE_ESCALATOR_BOT_ROLLS) await sleep(1000); // Pause if more rolls coming
    }
    // End of bot rolling loop

    await sleep(1500); // Final pause before result

    // --- Determine Winner & Payout ---
    let finalResultSection = "";
    let payoutAmount = 0n; // What gets credited back to player
    let finalGameStatusForLog = "lost"; // Default to loss for logging
    let jackpotActuallyWonAmount = 0n;

    if (botBusted || botScore < playerScoreStood) { // PLAYER WINS ROUND
        finalGameStatusForLog = "won";
        payoutAmount = betAmount + playerScoreStood; // Bet back + score as profit
        finalResultSection = `\n\n--- 🎉 *YOU WIN!* 🎉 ---\n${playerRef}, The Bot ${botBusted ? "BUSTED 💥" : `only scored *${botScore}*`}. You beat The House with your score of *${playerScoreStood}*!\n` +
                             `You receive *${formatCurrency(Number(payoutAmount))}* (your bet + winnings).`;

        // JACKPOT CHECK (Only if player won the round)
        if (playerScoreStood >= BigInt(TARGET_JACKPOT_SCORE)) {
            const client = await pool.connect();
            try {
                await client.query('BEGIN');
                const jackpotRes = await client.query('SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1 FOR UPDATE', [MAIN_JACKPOT_ID]);
                if (jackpotRes.rows.length > 0 && BigInt(jackpotRes.rows[0].current_amount_lamports) > 0n) {
                    const currentJackpot = BigInt(jackpotRes.rows[0].current_amount_lamports);
                    jackpotActuallyWonAmount = currentJackpot; // Store the actual amount won

                    // Add jackpot to player's balance (this is separate from game winnings for clarity, but part of same transaction)
                    const jackpotPayoutReason = `jackpot_win_de:${gameId}`;
                    const jackpotPayoutResult = await updateUserBalance(initiatorId, jackpotActuallyWonAmount, jackpotPayoutReason, client, gameId, chatId);

                    if (jackpotPayoutResult.success) {
                        await client.query('UPDATE jackpot_status SET current_amount_lamports = 0, last_won_at = NOW(), last_won_by_user_id = $1, updated_at = NOW() WHERE jackpot_id = $2', [initiatorId, MAIN_JACKPOT_ID]);
                        finalResultSection += `\n\n🎉🎉🎉 *JACKPOT HIT!* 🎉🎉🎉\nCongratulations! You also won the Jackpot of *${formatCurrency(Number(jackpotActuallyWonAmount))}*!`;
                        console.log(`[JACKPOT_WIN] User ${initiatorId} won jackpot of ${jackpotActuallyWonAmount}. Game ${gameId}.`);
                        // Log jackpot payout specifically, or ensure updateUserBalance log is clear
                         await client.query(
                             `UPDATE bets SET status = $1, payout_amount_lamports = COALESCE(payout_amount_lamports, 0) + $2, bet_details = bet_details || $3::jsonb, processed_at = NOW() 
                              WHERE user_id = $4 AND bet_details->>'game_id' = $5 AND status = 'active'`, // Note: status might be 'won' already if game payout logged first
                              ['jackpot_won_addon', jackpotActuallyWonAmount.toString(), JSON.stringify({jackpot_won: jackpotActuallyWonAmount.toString()}), initiatorId, gameId]
                         );
                    } else {
                        console.error(`[JACKPOT_ERR] Failed to payout jackpot ${jackpotActuallyWonAmount} to ${initiatorId}. Error: ${jackpotPayoutResult.error}. Jackpot NOT reset.`);
                        jackpotActuallyWonAmount = 0n; // Reset for messaging if payout failed
                    }
                }
                await client.query('COMMIT');
            } catch (error) {
                await client.query('ROLLBACK');
                console.error(`[JACKPOT_ERR] DB error during jackpot win for ${gameId}:`, error);
                jackpotActuallyWonAmount = 0n;
            } finally {
                client.release();
            }
        }
    } else if (botScore === playerScoreStood && !botBusted) { // PUSH
        finalGameStatusForLog = "push";
        payoutAmount = betAmount; // Bet returned
        finalResultSection = `\n\n--- 😐 *PUSH!* 😐 ---\n${playerRef}, The Bot also scored *${botScore}*! Your bet of *${formatCurrency(Number(betAmount))}* is returned.`;
    } else { // BOT WINS ROUND (botScore > playerScoreStood and bot not busted)
        finalGameStatusForLog = "lost"; // Player loses initial bet (already deducted)
        payoutAmount = 0n;
        finalResultSection = `\n\n--- 💀 *THE HOUSE WINS!* 💀 ---\n${playerRef}, The Bot scored *${botScore}*, beating your score of *${playerScoreStood}*.\nBetter luck next time!`;
    }

    // Update player's balance for the main game outcome (bet back / bet back + score)
    // Jackpot amount is handled separately above and already added to balance if won.
    if (payoutAmount > 0n) {
        // The reason here should reflect the game outcome excluding jackpot for clarity in balance logs
        const gamePayoutReason = `${finalGameStatusForLog}_de_vs_house:${gameId}`;
        await updateUserBalance(initiatorId, payoutAmount, gamePayoutReason, null, gameId, chatId);
    } else if (finalGameStatusForLog === "lost") {
        // Bet already deducted, just log the loss status in 'bets' table
        await queryDatabase(
            `UPDATE bets SET status = 'lost', payout_amount_lamports = '0', processed_at = NOW() 
             WHERE user_id = $1 AND bet_details->>'game_id' = $2 AND status = 'active'`,
             [initiatorId, gameId]
        ).catch(e => console.error("Error logging loss to bets table:", e));
    }

    // Construct final message
    const botPlaysMsgBase = `🤖 *Bot's Turn Results for ${playerRef}* 🤖\nPlayer Stood Score: *${playerScoreStood}*\n\n${botRollsLog.join('\n')}`;
    let finalStandings = `\n\n*Final Standings:*\nYou (Stood): *${playerScoreStood}* ${ (botBusted || botScore < playerScoreStood) ? '🏆' : '' }\nBot: *${botScore}* ${ (!botBusted && botScore > playerScoreStood) ? '🏆' : (botBusted ? '💥' : '') }`;
    const finalMsg = botPlaysMsgBase + finalResultSection + finalStandings; // finalResultSection already includes jackpot if won

    const playAgainKeyboard = { inline_keyboard: [[{ text: `🎲 Play Again (${formatCurrency(Number(betAmount))})`, callback_data: `play_again_de:${betAmount}` }]] };

    if (messageIdToUpdate) bot.editMessageText(finalMsg, { chatId: String(chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboard }).catch(() => safeSendMessage(String(chatId), finalMsg, { parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboard }));
    else safeSendMessage(String(chatId), finalMsg, { parse_mode: 'MarkdownV2', reply_markup: playAgainKeyboard });

    activeGames.delete(gameId);
    console.log(`[DE_BOT_TURN_COMPLETE] Game ${gameId}. Player Stood: ${playerScoreStood}, Bot Score: ${botScore}, Bot Busted: ${botBusted}, Jackpot Won: ${jackpotActuallyWonAmount}`);
}

console.log("Part 5b: Dice Escalator Game Logic (Revised) - Complete.");
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
                referral_code VARCHAR(20) UNIQUE, -- <<< MODIFIED TO VARCHAR(20)
                referred_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
                referral_count INTEGER NOT NULL DEFAULT 0,
                total_wagered BIGINT NOT NULL DEFAULT 0,
                last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0,
                last_bet_amounts JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                -- telegram_username and telegram_first_name columns were previously removed from this definition
                -- as per your preference to not add them to your existing schema.
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
        console.log('⚙️ [DB Init] Ensuring "dice_roll_requests" table exists...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS dice_roll_requests (
                request_id SERIAL PRIMARY KEY,
                game_id VARCHAR(255) NOT NULL UNIQUE,
                chat_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL, 
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                roll_value INTEGER,
                requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring "dice_roll_requests" indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_status_requested_at ON dice_roll_requests (status, requested_at);');

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
    const JOIN_GAME_TIMEOUT_MS_parsed = parseInt(process.env.JOIN_GAME_TIMEOUT_MS || '60000', 10);
    const GAME_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS_parsed * 10; 
    let cleanedGames = 0;

    try {
        for (const [gameId, gameData] of activeGames.entries()) {
            if (!gameData || !gameData.creationTime || !gameData.status || !gameData.type || !gameData.chatId) {
                console.warn(`[BACKGROUND_TASK] Skipping potentially corrupt game entry with ID: ${gameId}`);
                activeGames.delete(gameId);
                continue;
            }
            const staleStatuses = ['waiting_opponent', 'waiting_choices', 'waiting_db_roll'];
            if ((now - gameData.creationTime > GAME_CLEANUP_THRESHOLD_MS) && staleStatuses.includes(gameData.status)) {
                console.warn(`[BACKGROUND_TASK] Cleaning stale game ${gameId} (${gameData.type}) in chat ${gameData.chatId}. Status: ${gameData.status}`);
                let refundReason = `refund_stale_${gameData.type}_timeout:${gameId}`;
                const initiatorDisp = gameData.playerReference || gameData.initiatorMention || (gameData.initiatorId ? `User ${gameData.initiatorId}` : 'Unknown Initiator');
                let staleMsgText = `Game \\(ID: \`${escapeMarkdownV2(gameId)}\`\\) by ${initiatorDisp} was cleared due to inactivity`;

                if (gameData.type === 'dice_escalator' && gameData.status === 'waiting_db_roll') {
                    staleMsgText += " \\(roll not processed\\)\\.";
                    if (gameData.playerScore === 0n && gameData.initiatorId && gameData.betAmount) { 
                        await updateUserBalance(gameData.initiatorId, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId))
                            .catch(e => console.error(`[BACKGROUND_TASK_ERR] Failed refund for stale dice game ${gameId}: ${e.message}`));
                        staleMsgText += " Bet refunded\\.";
                    } else {
                        staleMsgText += ` Last score: ${gameData.playerScore}\\.`;
                    }
                    if (typeof queryDatabase === 'function') {
                         queryDatabase("DELETE FROM dice_roll_requests WHERE game_id = $1 AND status = 'pending'", [gameId]).catch(e => console.error(`[BACKGROUND_TASK_ERR] Failed to delete stale dice_roll_request for ${gameId}: ${e.message}`));
                    }
                } else if ((gameData.type === 'coinflip' || gameData.type === 'rps') && gameData.status === 'waiting_opponent') {
                    if (gameData.initiatorId && gameData.betAmount) {
                         await updateUserBalance(gameData.initiatorId, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId))
                            .catch(e => console.error(`[BACKGROUND_TASK_ERR] Failed refund for stale ${gameData.type} game ${gameId}: ${e.message}`));
                         staleMsgText += "\\. Bet refunded\\.";
                    }
                } else if (gameData.type === 'rps' && gameData.status === 'waiting_choices') {
                     staleMsgText += " during choice phase\\. Bets refunded to all participants\\.";
                     if (gameData.participants && gameData.betAmount) {
                          for (const p of gameData.participants) { 
                              if (p.betPlaced && p.userId) { 
                                   await updateUserBalance(p.userId, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId))
                                     .catch(e => console.error(`[BACKGROUND_TASK_ERR] Failed refund for stale RPS participant ${p.userId}, game ${gameId}: ${e.message}`));
                              }
                          }
                     }
                }

                if (bot && typeof safeSendMessage === 'function' && gameData.chatId) {
                   if (gameData.gameSetupMessageId) {
                        bot.editMessageText(staleMsgText, { chatId: String(gameData.chatId), message_id: Number(gameData.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
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
        for (const [chatId, sessionData] of groupGameSessions.entries()) {
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
let retryPollingDelay = 5000;
const MAX_RETRY_POLLING_DELAY = 60000;
let pollingRetryTimeoutId = null;

async function attemptRestartPolling(error) {
    if (isShuttingDown) {
         console.log('[POLLING_RETRY] Shutdown in progress, skipping polling restart attempt.');
         return;
    }
    if (isRetryingPolling) {
        console.log('[POLLING_RETRY] Already attempting to restart polling. Skipping.');
        return;
    }
    isRetryingPolling = true;
    clearTimeout(pollingRetryTimeoutId);
    console.warn(`[POLLING_RETRY] Polling error: ${error.code || 'N/A'} - ${error.message}. Restarting in ${retryPollingDelay / 1000}s...`);

    try {
        if (bot?.isPolling?.()) { 
            await bot.stopPolling({ cancel: true });
            console.log('[POLLING_RETRY] Explicitly stopped polling before retry.');
        }
    } catch (stopErr) {
        console.error('[POLLING_RETRY] Error stopping polling before retry:', stopErr.message);
    }

    pollingRetryTimeoutId = setTimeout(async () => {
         if (isShuttingDown) {
              console.log('[POLLING_RETRY] Shutdown initiated before polling restart could occur.');
              isRetryingPolling = false;
              return;
         }
         if (!bot || typeof bot.startPolling !== 'function') {
              console.error("[POLLING_RETRY] Cannot restart polling, bot instance or startPolling method is not available.");
              isRetryingPolling = false;
              return;
         }
        try {
            console.log('[POLLING_RETRY] Attempting bot.startPolling()...');
            await bot.startPolling();
            console.log('✅ [POLLING_RETRY] Polling successfully restarted!');
            retryPollingDelay = 5000;
            isRetryingPolling = false;
        } catch (startErr) {
            console.error(`❌ [POLLING_RETRY] Failed to restart polling: ${startErr.code || 'N/A'} - ${startErr.message}`);
            retryPollingDelay = Math.min(retryPollingDelay * 2, MAX_RETRY_POLLING_DELAY);
            isRetryingPolling = false;
            console.warn(`[POLLING_RETRY] Next retry attempt on next 'polling_error' after ${retryPollingDelay / 1000}s.`);
            if (ADMIN_USER_ID && retryPollingDelay >= MAX_RETRY_POLLING_DELAY && typeof safeSendMessage === "function") {
                safeSendMessage(ADMIN_USER_ID, `🚨 BOT ALERT: Failed to restart polling repeatedly. Last error: ${escapeMarkdownV2(startErr.message)}. Manual intervention needed.`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
            }
             if (typeof shutdown === "function" && (String(startErr.message).includes('409') || String(startErr.code).includes('EFATAL'))) {
                  console.error("FATAL error during polling restart attempt. Initiating shutdown.");
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
    if (isShuttingDown) {
        console.warn("🚦 Shutdown already in progress, ignoring duplicate signal:", signal);
        return;
    }
    isShuttingDown = true;
    console.warn(`\n🚦 Received signal: ${signal}. Initiating graceful shutdown... (PID: ${process.pid})`);

    clearTimeout(pollingRetryTimeoutId);
    isRetryingPolling = false;

    if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function" && typeof BOT_VERSION !== 'undefined') {
        await safeSendMessage(ADMIN_USER_ID, `ℹ️ Bot v${BOT_VERSION} shutting down (Signal: ${escapeMarkdownV2(String(signal))})...`, { parse_mode: 'MarkdownV2' }).catch(e => console.error("Admin notify fail (shutdown start):", e));
    }

    console.log("🚦 [Shutdown] Stopping Telegram updates...");
    if (bot?.isPolling?.()) {
        await bot.stopPolling({ cancel: true })
            .then(() => console.log("✅ [Shutdown] Polling stopped."))
            .catch(e => console.error("❌ [Shutdown] Error stopping polling:", e.message));
    } else if (bot && typeof bot.deleteWebHook === 'function' && !bot.options.polling) { 
         console.log("ℹ️ [Shutdown] In webhook mode. Attempting to delete webhook...");
         await bot.deleteWebHook({ drop_pending_updates: false }) 
              .then(() => console.log("✅ [Shutdown] Webhook deleted."))
              .catch(e => console.warn(`⚠️ [Shutdown] Non-critical error deleting webhook: ${e.message}`));
    } else {
        console.log("ℹ️ [Shutdown] Telegram bot instance not available or polling/webhook already off.");
    }

    console.log("🚦 [Shutdown] Stopping background intervals...");
    if (backgroundTaskInterval) clearInterval(backgroundTaskInterval); backgroundTaskInterval = null;
    console.log("✅ [Shutdown] Background intervals cleared.");

    console.log("ℹ️ [Shutdown] Skipping explicit queue wait (no dedicated queues like BullMQ implemented).");
    // await sleep(1000); // Optional short delay for in-flight async operations

    console.log("🚦 [Shutdown] Closing Database pool...");
    if (pool && typeof pool.end === 'function') {
        await pool.end()
            .then(() => console.log("✅ [Shutdown] Database pool closed."))
            .catch(e => console.error("❌ [Shutdown] Error closing Database pool:", e.message));
    } else {
        console.log("ℹ️ [Shutdown] Database pool not available or already closed.");
    }

    console.log(`🏁 [Shutdown] Graceful shutdown complete (Signal: ${signal}). Exiting.`);
    const exitCode = (signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
    process.exit(exitCode);
}

// Watchdog timer to force exit if shutdown hangs
function startShutdownWatchdog(signal) {
     // SHUTDOWN_FAIL_TIMEOUT_MS is defined in Part 1
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
    process.on('SIGINT', () => {
        console.log("Received SIGINT.");
        if (!isShuttingDown) startShutdownWatchdog('SIGINT');
        shutdown('SIGINT');
    });
    process.on('SIGTERM', () => {
        console.log("Received SIGTERM.");
         if (!isShuttingDown) startShutdownWatchdog('SIGTERM');
        shutdown('SIGTERM');
    });

    process.on('uncaughtException', async (error, origin) => {
        console.error(`\n🚨🚨🚨 UNCAUGHT EXCEPTION [Origin: ${origin}] 🚨🚨🚨\n`, error);
        if (!isShuttingDown) {
            console.error("Initiating emergency shutdown due to uncaught exception...");
            if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
                await safeSendMessage(ADMIN_USER_ID, `🚨🚨 UNCAUGHT EXCEPTION (${escapeMarkdownV2(String(origin))})\n${escapeMarkdownV2(String(error.message || error))}\nAttempting shutdown...`, { parse_mode: 'MarkdownV2' }).catch(e => console.error("Admin notify fail (uncaught):", e));
            }
             startShutdownWatchdog('uncaughtException');
            shutdown('uncaughtException').catch(() => process.exit(1));
        } else {
            console.warn("Uncaught exception occurred during an ongoing shutdown sequence. Forcing exit soon via watchdog.");
        }
    });

    process.on('unhandledRejection', async (reason, promise) => {
        console.error('\n🔥🔥🔥 UNHANDLED PROMISE REJECTION 🔥🔥🔥');
        console.error('Reason:', reason); 
        if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
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
                 console.error(`\n🚫 MAIN BOT TELEGRAM POLLING ERROR 🚫 Code: ${error.code || 'N/A'} | Message: ${error.message}`);
                 if (String(error.message).includes('409 Conflict')) {
                      console.error("FATAL: 409 Conflict. Another instance running. Shutting down THIS instance.");
                       if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
                           await safeSendMessage(ADMIN_USER_ID, `🚨 BOT CONFLICT (409): Instance on host ${process.env.HOSTNAME || 'local'} shutting down. Ensure only one token instance runs. Error: ${escapeMarkdownV2(String(error.message || error))}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
                       }
                       if (!isShuttingDown) {
                            startShutdownWatchdog('POLLING_409_ERROR');
                            shutdown('POLLING_409_ERROR').catch(() => process.exit(1));
                       }
                 } else if (String(error.code).includes('EFATAL')) {
                      console.error("FATAL POLLING ERROR (EFATAL). Shutting down.", error);
                       if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
                           await safeSendMessage(ADMIN_USER_ID, `🚨 BOT FATAL ERROR (EFATAL): Polling stopped. Shutting down. Error: ${escapeMarkdownV2(error.message)}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
                       }
                       if (!isShuttingDown) {
                           startShutdownWatchdog('POLLING_FATAL_ERROR');
                           shutdown('POLLING_FATAL_ERROR').catch(() => process.exit(1));
                       }
                 } else {
                      if (typeof attemptRestartPolling === 'function') attemptRestartPolling(error);
                 }
            });

            bot.on('error', async (error) => { 
                 console.error('\n🔥 MAIN BOT GENERAL TELEGRAM LIBRARY ERROR EVENT 🔥:', error);
                  if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
                     await safeSendMessage(ADMIN_USER_ID, `⚠️ BOT LIBRARY ERROR\n${escapeMarkdownV2(error.message || String(error))}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
                 }
            });
            console.log("✅ [Startup] Telegram event listeners attached.");

            const me = await bot.getMe();
            console.log(`✅ Successfully connected to Telegram! Bot Name: @${me.username}, Bot ID: ${me.id}`);

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
                await tempBot.sendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE Main Bot v${BOT_VERSION}:\n${escapeMarkdownV2(error.message)}\nBot is exiting. Check logs.`, {parse_mode:'MarkdownV2'}).catch(() => {});
            } catch (tempBotError) {
                console.error("Main Bot: Failed create temp bot for failure notification:", tempBotError);
            }
        }
        if (!isShuttingDown && typeof startShutdownWatchdog === 'function') { 
            startShutdownWatchdog('STARTUP_FAILURE');
            process.exit(1);
        } else if (!isShuttingDown) {
            console.error("STARTUP_FAILURE, watchdog not defined. Forcing exit.");
            process.exit(1);
        }
    }
}

// --- Final Execution: Start the Bot ---
main().catch(error => {
    console.error("❌ MAIN ASYNC FUNCTION UNHANDLED ERROR (Should not happen if main() has good try/catch):", error);
    if(typeof startShutdownWatchdog === 'function') startShutdownWatchdog('MAIN_CATCH');
    else console.error("Watchdog not defined, forcing exit immediately from main catch.");
    process.exit(1);
});

console.log("Main Bot: End of index.js script. Bot startup process initiated.");
// --- End of Part 6 ---
