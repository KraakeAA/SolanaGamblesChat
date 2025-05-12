// index.js - Main Solana Casino Bot (Updated for Automatic First Roll in Dice Escalator)
// Version 2.1.0-auto-first-roll: Incorporates DB polling for dice rolls provided by a helper bot.

import 'dotenv/config';
import TelegramBot from 'node-telegram-bot-api';
import { Pool } from 'pg'; // For PostgreSQL

// --- Start of actual code execution after imports ---
console.log("Loading Part 1: Core Imports & Basic Setup...");

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
    // Add other non-DB defaults here if needed
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
    // Ensure safeSendMessage and escapeMarkdownV2 are defined before use if called early
    if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
        safeSendMessage(ADMIN_USER_ID, `🚨 DATABASE POOL ERROR (Idle Client): ${escapeMarkdownV2(err.message || String(err))}`)
            .catch(notifyErr => console.error("Failed to notify admin about DB pool error:", notifyErr));
    } else {
        console.error(`[ADMIN ALERT during DB Pool Error (Idle Client)] ${err.message || String(err)} (safeSendMessage or escapeMarkdownV2 might not be defined yet)`);
    }
});
console.log("✅ PostgreSQL Pool created.");


const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("Telegram Bot instance created and configured for polling.");

const BOT_VERSION = '2.1.0-auto-first-roll'; // Updated version marker
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;

// In-memory stores (consider replacing with DB for production balances/state)
let activeGames = new Map(); // For active game state (Dice Escalator, Coinflip, RPS)
let userCooldowns = new Map(); // For command cooldowns

console.log(`Group Chat Casino Bot v${BOT_VERSION} initializing...`);
console.log(`Current system time: ${new Date().toISOString()}`);

// --- Core Utility Functions ---

// Escapes text for Telegram MarkdownV2 mode
const escapeMarkdownV2 = (text) => {
    if (text === null || typeof text === 'undefined') return '';
    // Escape characters specifically required by Telegram MarkdownV2
    return String(text).replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};

// Safely sends a message, handling potential errors and length limits
async function safeSendMessage(chatId, text, options = {}) {
    if (!chatId || typeof text !== 'string') {
        console.error("[safeSendMessage] Invalid input:", { chatId, textPreview: String(text).substring(0, 50) });
        return undefined; // Indicate failure
    }

    let messageToSend = text;
    let finalOptions = { ...options }; // Clone options

    // Handle potential length issues BEFORE escaping for MarkdownV2
    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsis = "... (message truncated)";
        // Calculate truncation point based on raw text length
        const truncateAt = MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsis.length;
        messageToSend = (truncateAt > 0) ? messageToSend.substring(0, truncateAt) + ellipsis : messageToSend.substring(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH);
        console.warn(`[safeSendMessage] Message for chat ${chatId} was truncated before potential escaping.`);
    }

    // Apply escaping ONLY if MarkdownV2 is specified
    if (finalOptions.parse_mode === 'MarkdownV2') {
        messageToSend = escapeMarkdownV2(messageToSend);
        // Re-check length *after* escaping, as escaping adds characters. This is tricky.
        // A simple re-truncation might break Markdown. Better to truncate original text more aggressively above.
        if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
             console.warn(`[safeSendMessage] Message for chat ${chatId} might still exceed length limit after escaping. Sending anyway.`);
             // Optionally truncate again, but risk breaking formatting:
             // messageToSend = messageToSend.substring(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH);
        }
    }

    try {
        // Attempt to send the message
        const sentMessage = await bot.sendMessage(chatId, messageToSend, finalOptions);
        return sentMessage; // Return the message object on success
    } catch (error) {
        console.error(`[safeSendMessage] Failed to send to chat ${chatId}. Code: ${error.code || 'N/A'}, Msg: ${error.message}`);
        // Log more details if available
        if (error.response && error.response.body) {
            console.error(`[safeSendMessage] API Response: ${JSON.stringify(error.response.body)}`);
        }
        // Specific handling for common errors can be added here (e.g., blocked, chat not found)
        return undefined; // Indicate failure
    }
}
console.log("Part 1: Core Imports & Basic Setup - Complete.");

// --- End of Part 1 ---
//---------------------------------------------------------------------------
// index.js - Part 2: Database Operations & Data Management
//---------------------------------------------------------------------------
console.log("Loading Part 2: Database Operations & Data Management...");

// In-memory stores for quick access data (like active games, user stats not requiring DB persistence yet)
// NOTE: For a production bot, especially handling real value (like Solana),
// user balances *must* be stored and managed reliably in the PostgreSQL database,
// likely within the 'user_balances' table and using transactionally safe updates (e.g., SELECT FOR UPDATE).
// The 'userDatabase' Map here is a simplified placeholder from earlier versions.
const userDatabase = new Map(); // Placeholder for simple in-memory balance/stats
const groupGameSessions = new Map(); // Tracks active game per group
console.log("In-memory data stores (userDatabase, groupGameSessions) initialized.");


// --- queryDatabase Helper Function ---
// Provides a consistent way to interact with the database pool and includes basic error logging.
async function queryDatabase(sql, params = [], dbClient = pool) {
    // Validate input
    if (!dbClient) {
        const poolError = new Error("Database pool/client is not available for queryDatabase");
        console.error("❌ CRITICAL: queryDatabase called but dbClient is invalid!", poolError.stack);
        throw poolError; // Cannot proceed without a client/pool
    }
    if (typeof sql !== 'string' || sql.trim().length === 0) {
        const sqlError = new TypeError(`queryDatabase received invalid SQL query (type: ${typeof sql}, value: ${sql})`);
        console.error(`❌ DB Query Error:`, sqlError.message);
        // Avoid logging potentially sensitive params directly in generic error if possible
        // console.error(`   Params: ${JSON.stringify(params)}`); // Be cautious with logging params
        throw sqlError;
    }

    // Execute query
    try {
        // console.log(`[DB_QUERY] Executing SQL: ${sql.substring(0, 100)}${sql.length > 100 ? '...' : ''}`, params); // Optional: Log query execution
        const result = await dbClient.query(sql, params);
        // console.log(`[DB_QUERY] Success. Rows: ${result.rowCount}`); // Optional: Log success
        return result; // Return the full result object (includes rows, rowCount, etc.)
    } catch (error) {
        // Log detailed error information
        console.error(`❌ DB Query Error Encountered:`);
        console.error(`   SQL (truncated): ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`);
        // Safely stringify params, handling potential BigInts if they were ever used
        const safeParamsString = JSON.stringify(params, (key, value) =>
            typeof value === 'bigint' ? value.toString() + 'n' : value // Handle BigInt for logging if needed
        );
        console.error(`   Params: ${safeParamsString}`);
        console.error(`   Error Code: ${error.code || 'N/A'}`);
        console.error(`   Error Message: ${error.message}`);
        if (error.constraint) { console.error(`   Constraint Violated: ${error.constraint}`); }
        // Consider logging stack trace for deeper debugging if needed: console.error(error.stack);

        // Re-throw the error so the calling function knows the operation failed
        // and can handle it appropriately (e.g., rollback transaction, notify user).
        throw error;
    }
}


// --- User and Group Session Functions (Using simplified in-memory store) ---
// NOTE: These are placeholders. Real balance logic should use the database.

// Gets user data (from memory map), creates if doesn't exist
async function getUser(userId, username) {
    const userIdStr = String(userId);
    if (!userDatabase.has(userIdStr)) {
        // Create a new user entry in the map
        const newUser = {
            userId: userIdStr,
            username: username || `User_${userIdStr}`, // Use provided username or generate one
            balance: 1000, // Default starting balance (for testing/placeholder)
            lastPlayed: null,
            groupStats: new Map(), // For tracking stats per group chat
            isNew: true, // Flag indicating this user was just created in memory
        };
        userDatabase.set(userIdStr, newUser);
        console.log(`[IN_MEM_DB] New user added (in-memory): ${userIdStr} (@${newUser.username}), Bal: ${newUser.balance}`);
        return { ...newUser }; // Return a copy to prevent direct modification
    }

    // Retrieve existing user
    const user = userDatabase.get(userIdStr);
    // Update username if it has changed since last seen
    if (username && user.username !== username) {
        user.username = username;
        console.log(`[IN_MEM_DB] Updated username for ${userIdStr} to @${username}`);
    }
    user.isNew = false; // Mark as not new
    // Return a deep copy to avoid modifying the original map entry unintentionally
    // For groupStats, ensure the map itself is copied
    return { ...user, groupStats: new Map(user.groupStats) };
}

// Updates user balance (in memory map only) - VERY SIMPLISTIC PLACEHOLDER
async function updateUserBalance(userId, amountChange, reason = "unknown_transaction", chatId) {
    const userIdStr = String(userId);
    if (!userDatabase.has(userIdStr)) {
        console.warn(`[IN_MEM_DB_ERROR] Update balance called for non-existent user: ${userIdStr}`);
        return { success: false, error: "User not found in memory." };
    }

    const user = userDatabase.get(userIdStr);
    const proposedBalance = user.balance + amountChange;

    // Basic check for insufficient funds
    if (proposedBalance < 0) {
        console.log(`[IN_MEM_DB] User ${userIdStr} insufficient balance (${user.balance}) for deduction of ${Math.abs(amountChange)} (Reason: ${reason}).`);
        return { success: false, error: "Insufficient balance." };
    }

    // Update balance and last activity timestamp
    user.balance = proposedBalance;
    user.lastPlayed = new Date();
    console.log(`[IN_MEM_DB] User ${userIdStr} balance updated to: ${user.balance} (Change: ${amountChange}, Reason: ${reason}, Chat: ${chatId || 'N/A'})`);

    // --- Placeholder Stat Tracking (In-Memory) ---
    // This section just updates stats within the user's in-memory record.
    // A real system would likely update stats tables in the database.
    if (chatId) {
        const chatIdStr = String(chatId);
        // Initialize stats for this group if not present
        if (!user.groupStats.has(chatIdStr)) {
            user.groupStats.set(chatIdStr, { gamesPlayed: 0, totalWagered: 0, netWinLoss: 0 });
        }
        const stats = user.groupStats.get(chatIdStr);

        // Update stats based on the reason string (very basic parsing)
        const reasonLower = reason.toLowerCase();
        if (reasonLower.includes("bet_")) {
            const wager = Math.abs(amountChange);
            stats.totalWagered += wager;
            stats.netWinLoss -= wager; // Deduct wager from net
        } else if (reasonLower.includes("won_") || reasonLower.includes("payout_") || reasonLower.includes("cashout")) {
            stats.netWinLoss += amountChange; // Add winnings/cashout to net
             // Increment games played only on a win resolution, not cashout/refund
            if (!reasonLower.includes("refund_") && !reasonLower.includes("cashout") && amountChange > 0) {
                 stats.gamesPlayed += 1;
            }
        } else if (reasonLower.includes("lost_")) {
            // Player lost their bet, net was already reduced by the bet placement.
            // Increment games played unless it was a non-game loss (e.g., bust)
            if (!reasonLower.includes("bust")) { // Don't count bust as a "played game" in same way? Adjust as needed.
                stats.gamesPlayed += 1;
            }
        } else if (reasonLower.includes("refund_")) {
            // Refund reverses the net change of the bet placement
            stats.netWinLoss += amountChange; // Add back the refunded amount
        }
        // console.log(`[IN_MEM_STATS] User ${userIdStr} stats for chat ${chatIdStr}:`, stats); // Optional: Log stats update
    }
    // --- End Placeholder Stat Tracking ---

    return { success: true, newBalance: user.balance };
}

// Gets group session data (from memory map), creates if doesn't exist
async function getGroupSession(chatId, chatTitle) {
    const chatIdStr = String(chatId);
    if (!groupGameSessions.has(chatIdStr)) {
        const newSession = {
            chatId: chatIdStr,
            chatTitle: chatTitle || `Group_${chatIdStr}`, // Use provided title or generate one
            currentGameId: null, // ID of the currently active game in this chat
            currentGameType: null, // Type of game (e.g., 'CoinFlip', 'RPS', 'DiceEscalator')
            currentBetAmount: null, // Bet amount for the current game
            lastActivity: new Date(), // Timestamp of last known activity in this session
        };
        groupGameSessions.set(chatIdStr, newSession);
        console.log(`[IN_MEM_SESS] New group session created: ${chatIdStr} (${newSession.chatTitle}).`);
        return { ...newSession }; // Return a copy
    }

    // Retrieve existing session
    const session = groupGameSessions.get(chatIdStr);
    // Update title if it has changed (Telegram groups can be renamed)
    if (chatTitle && session.chatTitle !== chatTitle) {
         session.chatTitle = chatTitle;
         console.log(`[IN_MEM_SESS] Updated title for session ${chatIdStr} to "${chatTitle}"`);
    }
    session.lastActivity = new Date(); // Update last activity timestamp
    return { ...session }; // Return a copy
}

// Updates details about the current game in a group session (in memory map)
async function updateGroupGameDetails(chatId, gameId, gameType, betAmount) {
    const chatIdStr = String(chatId);
    // Ensure session exists before trying to update it
    if (!groupGameSessions.has(chatIdStr)) {
        // This might happen if a game update is called before getGroupSession,
        // or if the session was cleared due to inactivity. Attempt to create it.
        console.warn(`[IN_MEM_SESS_WARN] Group session ${chatIdStr} not found for update. Attempting creation.`);
        // Pass null for title if unknown, getGroupSession will handle default
        await getGroupSession(chatIdStr, null);
    }

    const session = groupGameSessions.get(chatIdStr);
    // If session *still* doesn't exist after attempt to create, log error and fail
    if (!session) {
       console.error(`[IN_MEM_SESS_ERROR] Failed to get/create session for ${chatIdStr} during game details update.`);
       return false; // Indicate failure
    }

    // Update session details
    session.currentGameId = gameId; // null to clear game
    session.currentGameType = gameType; // null to clear game
    session.currentBetAmount = betAmount; // null to clear bet
    session.lastActivity = new Date(); // Update activity timestamp

    // Use helper function for currency formatting if amount exists
    const betDisplay = (betAmount !== null && betAmount !== undefined) ? formatCurrency(betAmount) : 'N/A';
    console.log(`[IN_MEM_SESS] Group ${chatIdStr} game details updated -> GameID: ${gameId || 'None'}, Type: ${gameType || 'None'}, Bet: ${betDisplay}`);
    return true; // Indicate success
}
console.log("Part 2: Database Operations & Data Management - Complete.");

// --- End of Part 2 ---
//---------------------------------------------------------------------------
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

// Simple asynchronous sleep function
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- Dice Display Utilities ---

// Map of dice roll values to their static Unicode emoji representation (can be kept for internal logic or other display needs)
const diceEmojis = {
    1: '⚀', // U+2680
    2: '⚁', // U+2681
    3: '⚂', // U+2682
    4: '⚃', // U+2683
    5: '⚄', // U+2684
    6: '⚅'  // U+2685
};

// Formats an array of dice roll numbers into a string with a generic dice emoji and the number
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
//---------------------------------------------------------------------------
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
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6; // Ensure valid sides
    const roll = rollDie(sides); // Use the internal function
    // Use the diceEmojis map defined in Part 3 for display
    return { roll: roll, emoji: diceEmojis[roll] || `🎲(${roll})` };
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
// index.js - Part 5a: Message & Callback Handling (Core Listeners & Basic Games)
//---------------------------------------------------------------------------
console.log("Loading Part 5a: Message & Callback Handling, Basic Game Flow...");

// --- Game Constants & Configuration ---
const COMMAND_COOLDOWN_MS = 2000; // 2 seconds between commands for a user
const JOIN_GAME_TIMEOUT_MS = 60000; // 60 seconds for someone to join a game
const MIN_BET_AMOUNT = 5;
const MAX_BET_AMOUNT = 1000;
// DICE_ESCALATOR specific constants are defined later if needed

// --- Main Message Handler (`bot.on('message')`) ---
// This listener processes incoming text messages.
bot.on('message', async (msg) => {
    // Basic logging for all received messages (can be verbose)
    // Optional: Filter logs based on message type or content if needed
    if (msg && msg.from && msg.chat) {
        console.log(`[RAW_MSG_LOG] Text: "${msg.text || 'N/A'}", FromID: ${msg.from.id}, User: @${msg.from.username || 'N/A'}, IsBot: ${msg.from.is_bot}, ChatID: ${msg.chat.id}, MsgID: ${msg.message_id}`);
    } else {
        console.log(`[RAW_MSG_LOG] Received incomplete message object. Msg JSON (partial):`, JSON.stringify(msg).substring(0, 200));
        return; // Ignore if essential parts like 'from' or 'chat' are missing
    }

    // --- Message Validation and Filtering ---
    // Ignore messages without a sender (should be rare)
    if (!msg.from) {
        console.warn("[MSG_HANDLER_WARN] Message received without 'from' field. Ignoring.", msg);
        return;
    }

    // Ignore messages from other bots to prevent loops or unintended interactions.
    // Allow messages from the bot itself if needed for some advanced flows (not currently used).
    if (msg.from.is_bot) {
        try {
            const selfBotInfo = await bot.getMe(); // Get own bot info
            if (String(msg.from.id) !== String(selfBotInfo.id)) {
                // Message is from a bot, and it's NOT this bot itself
                // console.log(`[MSG_IGNORE] Ignoring message from other bot (ID: ${msg.from.id}, User: @${msg.from.username || 'N/A'}).`);
                return; // Stop processing
            }
            // Optional: Add logic here if the bot needs to react to its own messages
        } catch (getMeError) {
            console.error("[MSG_HANDLER_ERROR] Failed to get self bot info to check message source:", getMeError.message);
            // Decide whether to ignore all bot messages if self-check fails
            return; // Safer to ignore if unsure
        }
    }

    // --- Process Commands from Non-Bot Users ---
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text || ""; // Ensure text is a string, default to empty
    const chatType = msg.chat.type; // 'private', 'group', 'supergroup', 'channel'
    const messageId = msg.message_id;

    // Apply command cooldown
    if (!msg.from.is_bot) { // Only apply cooldown to user messages
        const now = Date.now();
        // Check if the message is a command and if the user is in cooldown
        if (text.startsWith('/') && userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
            console.log(`[COOLDOWN] User ${userId} command ("${text}") ignored due to cooldown.`);
            // Optionally notify user, but be mindful of spam potential
            // await safeSendMessage(chatId, `${createUserMention(msg.from)}, please wait a moment before the next command.`, {parse_mode: 'MarkdownV2'});
            return; // Stop processing this command
        }
        // If it's a command, update the user's last command timestamp
        if (text.startsWith('/')) {
            userCooldowns.set(userId, now);
        }
    }

    // --- Command Dispatcher ---
    // Only process messages starting with '/' as commands from non-bots
    if (text.startsWith('/') && !msg.from.is_bot) {
        const commandArgs = text.substring(1).split(' '); // Split command and arguments
        const commandName = commandArgs.shift()?.toLowerCase(); // Get command name, handle potential empty string

        // Log command execution attempt
        console.log(`[CMD RCV] Chat: ${chatId}, User: ${userId} (@${msg.from.username || 'N/A'}), Cmd: /${commandName}, Args: [${commandArgs.join(', ')}]`);

        // Ensure user exists in our (in-memory) system before processing command
        await getUser(userId, msg.from.username || msg.from.first_name);

        // Route command to appropriate handler
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
                if (chatType !== 'private') { // Group/Supergroup only
                    let betAmountCF = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10; // Default bet 10
                    if (isNaN(betAmountCF) || betAmountCF < MIN_BET_AMOUNT || betAmountCF > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet amount\\. Must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}\\. Usage: \`/startcoinflip <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupCoinFlipCommand(chatId, msg.from, betAmountCF, messageId);
                } else {
                    await safeSendMessage(chatId, "This Coinflip game is for group chats only.", {});
                }
                break;
            case 'startrps':
                if (chatType !== 'private') { // Group/Supergroup only
                    let betAmountRPS = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10; // Default bet 10
                    if (isNaN(betAmountRPS) || betAmountRPS < MIN_BET_AMOUNT || betAmountRPS > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet amount\\. Must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}\\. Usage: \`/startrps <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupRPSCommand(chatId, msg.from, betAmountRPS, messageId);
                } else {
                    await safeSendMessage(chatId, "This Rock Paper Scissors game is for group chats only.", {});
                }
                break;

            // --- MODIFIED COMMAND FOR DICE ESCALATOR ---
            case 'startdice': // Changed from 'startdiceescalator'
                if (chatType !== 'private') { // Group/Supergroup only
                    let betAmountDE = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10; // Default bet 10 (changed example)
                    if (isNaN(betAmountDE) || betAmountDE < MIN_BET_AMOUNT || betAmountDE > MAX_BET_AMOUNT) {
                        // Update the usage example in the error message
                        await safeSendMessage(chatId, `Invalid bet amount\\. Must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}\\. Usage: \`/startdice <amount>\``, { parse_mode: 'MarkdownV2' }); // Updated usage example
                        return;
                    }
                    // The function call remains the same (it's defined in Part 5b)
                    await handleStartDiceEscalatorCommand(chatId, msg.from, betAmountDE, messageId);
                } else {
                    await safeSendMessage(chatId, "This Dice game is for group chats only.", {});
                }
                break;
            // --- END OF MODIFIED COMMAND ---

            default:
                // Avoid replying to every non-command message in groups
                if (chatType === 'private' || text.startsWith('/')) { // Only reply if it looks like a command attempt
                    await safeSendMessage(chatId, "Unknown command. Try /help to see available commands.", {});
                }
        }
    }
    // Ignore non-command messages in groups, process them in private chats if needed (no logic here currently)
}); // End bot.on('message')

// --- Callback Query Handler (`bot.on('callback_query')`) ---
// This listener processes interactions with inline keyboard buttons.
bot.on('callback_query', async (callbackQuery) => {
    const msg = callbackQuery.message; // The message the button was attached to
    const user = callbackQuery.from; // The user who clicked the button
    const callbackQueryId = callbackQuery.id; // ID to answer the query
    const data = callbackQuery.data; // Data string associated with the button (e.g., "join_game:game_123")

    // Validate essential callback data
    if (!msg || !user || !data) {
        console.warn(`[CBQ_WARN] Received incomplete callback query. ID: ${callbackQueryId}, Data: ${data}`);
        // Try to answer anyway to remove loading state, but show error
        bot.answerCallbackQuery(callbackQueryId, { text: "Error: Invalid callback data received." }).catch(() => {});
        return;
    }

    const userId = String(user.id);
    const chatId = String(msg.chat.id);
    const originalMessageId = msg.message_id; // ID of the message with the button

    console.log(`[CBQ RCV] Chat: ${chatId}, User: ${userId} (@${user.username || 'N/A'}), Data: "${data}", OriginalMsgID: ${originalMessageId}`);

    // --- Answer Callback Immediately ---
    // It's crucial to answer the callback query quickly to remove the "loading" state on the button user-side.
    // We answer with no text here, just acknowledging the press. Error/success feedback is sent via separate messages or edits.
    bot.answerCallbackQuery(callbackQueryId).catch((err) => {
        // Log if answering fails, but don't stop processing the action
        console.error(`[CBQ_ERROR] Failed to answer callback query ${callbackQueryId}: ${err.message}`);
    });

    // Ensure user exists in our (in-memory) system
    await getUser(userId, user.username || user.first_name);

    // --- Callback Action Dispatcher ---
    // Parse the callback data (simple format: action:param1:param2...)
    const [action, ...params] = data.split(':');

    try {
        // Route action to appropriate handler
        switch (action) {
            case 'join_game': // Handles joining Coinflip or RPS
                if (!params[0]) throw new Error("Missing gameId for join_game action.");
                await handleJoinGameCallback(chatId, user, params[0], originalMessageId);
                break;
            case 'cancel_game': // Handles cancelling Coinflip or RPS
                 if (!params[0]) throw new Error("Missing gameId for cancel_game action.");
                await handleCancelGameCallback(chatId, user, params[0], originalMessageId);
                break;
            case 'rps_choose': // Handles RPS choice selection
                if (params.length < 2) throw new Error("Missing parameters for rps_choose action (expected gameId:choice).");
                await handleRPSChoiceCallback(chatId, user, params[0], params[1], originalMessageId);
                break;

            // --- ADDED CASES FOR DICE ESCALATOR ---
            case 'de_roll_prompt': // Player wants to roll again in Dice Escalator
                if (!params[0]) throw new Error("Missing gameId for de_roll_prompt action.");
                // Call the handler function defined in Part 5b
                // Parameters: gameId, userId, actionType, interactionMessageId, chatId
                await handleDiceEscalatorPlayerAction(params[0], userId, action, originalMessageId, chatId);
                break;
            case 'de_cashout': // Player wants to cash out in Dice Escalator
                 if (!params[0]) throw new Error("Missing gameId for de_cashout action.");
                 // Call the handler function defined in Part 5b
                 // Parameters: gameId, userId, actionType, interactionMessageId, chatId
                 await handleDiceEscalatorPlayerAction(params[0], userId, action, originalMessageId, chatId);
                break;
            // --- END OF ADDED CASES ---

            default:
                console.log(`[CBQ_INFO] Unknown or unhandled callback query action received: "${action}"`);
                // Optionally notify the user directly if the action is unexpected
                await safeSendMessage(userId, "Sorry, that button action is not recognized or may have expired.", {}).catch(() => {});
        }
    } catch (error) {
        // Generic error handling for issues within the callback handlers
        console.error(`[CBQ_ERROR] Error processing callback query "${data}" for user ${userId} in chat ${chatId}:`, error);
        // Notify the user an error occurred
        await safeSendMessage(userId, "Sorry, an error occurred while processing your action. Please try again or contact an admin if it persists.", {}).catch(() => {});
    }
}); // End bot.on('callback_query')


// --- Command Handler Functions (Help, Balance) ---
async function handleHelpCommand(chatId, userObject) {
    const userMention = createUserMention(userObject);
    // Updated help text for clarity and includes Dice Escalator flow with new command
    const helpTextParts = [
        `👋 Hello ${userMention}\\! Welcome to the Group Casino Bot v${BOT_VERSION}\\.`,
        `This bot allows you to play games in group chats\\.`,
        `\n*Available commands:*`,
        `▫️ \`/help\` \\- Shows this help message\\.`,
        `▫️ \`/balance\` or \`/bal\` \\- Check your current game credits\\.`,
        `▫️ \`/startcoinflip <bet\\_amount>\` \\- Starts a Coinflip game for one opponent\\. Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\. Example: \`/startcoinflip 10\``,
        `▫️ \`/startrps <bet\\_amount>\` \\- Starts a Rock Paper Scissors game for one opponent\\. Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\. Example: \`/startrps 5\``,
        // --- MODIFIED HELP TEXT FOR DICE ESCALATOR ---
        `▫️ \`/startdice <bet\\_amount>\` \\- Play Dice Escalator against the Bot\\! Bet: ${MIN_BET_AMOUNT}\\-${MAX_BET_AMOUNT}\\. Example: \`/startdice 10\``, // Changed command and example
        // --- END OF MODIFIED HELP TEXT ---
        `\n*Game Notes:*`,
        `➡️ For Coinflip or RPS, click 'Join Game' when someone starts one\\!`,
        `➡️ For Dice Escalator, the first roll is automatic\\. After that, click 'Request Roll' or 'Cashout'\\.`,
        `\nHave fun and play responsibly\\!`
    ];
    // Send the help message using MarkdownV2, disable web page preview
    await safeSendMessage(chatId, helpTextParts.filter(Boolean).join('\n'), { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

async function handleBalanceCommand(chatId, userObject) {
    // Fetch latest in-memory user data (replace with DB fetch for production)
    const user = await getUser(String(userObject.id));
    // Format the balance and send the message
    const balanceMessage = `${createUserMention(userObject)}, your current balance is: *${formatCurrency(user.balance)}*\\.`;
    await safeSendMessage(chatId, balanceMessage, { parse_mode: 'MarkdownV2' });
}

// --- Group Game Flow Functions (Coinflip, RPS - Start, Join, Cancel, Choose) ---

async function handleStartGroupCoinFlipCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    // Fetch chat info (needed for session title)
    let chatInfo = null; try { chatInfo = await bot.getChat(chatId); } catch (e) { console.warn(`[COINFLIP_START_WARN] Could not fetch chat info for chat ${chatId}: ${e.message}`); }
    const chatTitle = chatInfo?.title;
    const gameSession = await getGroupSession(chatId, chatTitle || "Group Chat"); // Get/create session

    // Check if a game is already running in this chat
    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active in this chat: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}* \\(ID: \`${escapeMarkdownV2(gameSession.currentGameId)}\`\\)\\. Please wait for it to finish\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Check initiator balance (using in-memory function)
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance \\(${escapeMarkdownV2(formatCurrency(initiator.balance))}\\) is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Generate game ID *before* deducting bet
    const gameId = generateGameId();

    // Deduct bet amount (using in-memory function)
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_coinflip_init:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        console.error(`[COINFLIP_START_ERR] Failed to deduct balance for initiator ${initiatorId} (Reason: ${balanceUpdateResult.error})`);
        await safeSendMessage(chatId, `Error starting game: Could not place your bet\\. \\(${escapeMarkdownV2(balanceUpdateResult.error)}\\)`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Create game data structure
    const gameDataCF = {
        type: 'coinflip', gameId, chatId: String(chatId), initiatorId,
        initiatorMention: createUserMention(initiatorUser), betAmount,
        participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser), betPlaced: true }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
    };
    activeGames.set(gameId, gameDataCF); // Store game state in memory
    await updateGroupGameDetails(chatId, gameId, 'CoinFlip', betAmount); // Update session info

    // Send message to chat prompting for opponent
    const joinMsgCF = `${createUserMention(initiatorUser)} has started a *Coin Flip Challenge* for ${escapeMarkdownV2(formatCurrency(betAmount))}\\!\nAn opponent is needed\\. Click "Join Game" to accept\\!`;
    const kbCF = { inline_keyboard: [[{ text: "🪙 Join Coinflip!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]] };

    const setupMsgCF = await safeSendMessage(chatId, joinMsgCF, { parse_mode: 'MarkdownV2', reply_markup: kbCF });

    // Handle failure to send setup message (refund, cleanup)
    if (setupMsgCF) {
        const gameToUpdate = activeGames.get(gameId);
        if (gameToUpdate) gameToUpdate.gameSetupMessageId = setupMsgCF.message_id; // Store message ID for edits
    } else {
        console.error(`[COINFLIP_START_ERR] Failed to send setup message for game ${gameId}. Refunding bet for initiator ${initiatorId}.`);
        await updateUserBalance(initiatorId, betAmount, `refund_coinflip_setup_fail:${gameId}`, chatId); // Refund bet
        activeGames.delete(gameId); // Remove game from active map
        await updateGroupGameDetails(chatId, null, null, null); // Clear game from session
        return;
    }

    // Set timeout to automatically cancel game if no opponent joins
    setTimeout(async () => {
        const gdCF = activeGames.get(gameId);
        // Check if game still exists and is waiting for opponent
        if (gdCF && gdCF.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] Coinflip game ${gameId} in chat ${chatId} timed out waiting for an opponent.`);
            await updateUserBalance(gdCF.initiatorId, gdCF.betAmount, `refund_coinflip_timeout:${gameId}`, chatId); // Refund initiator
            activeGames.delete(gameId); // Clean up game state
            await updateGroupGameDetails(chatId, null, null, null); // Clean up session state
            const timeoutMsgTextCF = `The Coin Flip game \\(ID: \`${escapeMarkdownV2(gameId)}\`\\) started by ${gdCF.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdCF.betAmount))} has expired without an opponent\\. Bet refunded\\.`;
            // Try to edit the original setup message, fallback to sending new message
            if (gdCF.gameSetupMessageId) {
                bot.editMessageText(timeoutMsgTextCF, { chatId: String(chatId), message_id: Number(gdCF.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }) // Remove buttons
                    .catch(e => {
                        console.warn(`[GAME_TIMEOUT_EDIT_ERR] Coinflip ${gameId}: ${e.message}. Sending new timeout message.`);
                        safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' });
                    });
            } else {
                safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' });
            }
        }
    }, JOIN_GAME_TIMEOUT_MS); // Use configured timeout
}


async function handleStartGroupRPSCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    let chatInfo = null; try { chatInfo = await bot.getChat(chatId); } catch (e) { console.warn(`[RPS_START_WARN] Could not fetch chat info for chat ${chatId}: ${e.message}`); }
    const chatTitle = chatInfo?.title;
    const gameSession = await getGroupSession(chatId, chatTitle || "Group Chat");

    // Check if game already active
    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}*\\. Please wait\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    // Check balance
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance \\(${escapeMarkdownV2(formatCurrency(initiator.balance))}\\) is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Generate game ID *before* deducting bet
    const gameId = generateGameId();

    // Deduct bet
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_rps_init:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        console.error(`[RPS_START_ERR] Failed to deduct balance for initiator ${initiatorId} (Reason: ${balanceUpdateResult.error})`);
        await safeSendMessage(chatId, `Error starting game: Could not place your bet\\. \\(${escapeMarkdownV2(balanceUpdateResult.error)}\\)`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Create game data
    const gameDataRPS = {
        type: 'rps', gameId, chatId: String(chatId), initiatorId, initiatorMention: createUserMention(initiatorUser),
        betAmount, participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser), betPlaced: true }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
    };
    activeGames.set(gameId, gameDataRPS);
    await updateGroupGameDetails(chatId, gameId, 'RockPaperScissors', betAmount);

    // Send join prompt message
    const joinMsgRPS = `${createUserMention(initiatorUser)} challenges someone to *Rock Paper Scissors* for ${escapeMarkdownV2(formatCurrency(betAmount))}\\!\nClick "Join Game" to play\\!`;
    const kbRPS = { inline_keyboard: [[{ text: "🪨📄✂️ Join RPS!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]] };

    const setupMsgRPS = await safeSendMessage(chatId, joinMsgRPS, { parse_mode: 'MarkdownV2', reply_markup: kbRPS });

    // Handle failure to send setup message
    if (setupMsgRPS) {
        const gameToUpdate = activeGames.get(gameId);
        if (gameToUpdate) gameToUpdate.gameSetupMessageId = setupMsgRPS.message_id;
    } else {
        console.error(`[RPS_START_ERR] Failed to send setup message for game ${gameId}. Refunding bet for initiator ${initiatorId}.`);
        await updateUserBalance(initiatorId, betAmount, `refund_rps_setup_fail:${gameId}`, chatId);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }

    // Set timeout for opponent join
    setTimeout(async () => {
        const gdRPS = activeGames.get(gameId);
        if (gdRPS && gdRPS.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] RPS game ${gameId} in chat ${chatId} timed out waiting for an opponent.`);
            await updateUserBalance(gdRPS.initiatorId, gdRPS.betAmount, `refund_rps_timeout:${gameId}`, chatId); // Refund
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgTextRPS = `The RPS game \\(ID: \`${escapeMarkdownV2(gameId)}\`\\) by ${gdRPS.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdRPS.betAmount))} expired\\. Bet refunded\\.`;
            if (gdRPS.gameSetupMessageId) {
                bot.editMessageText(timeoutMsgTextRPS, { chatId: String(chatId), message_id: Number(gdRPS.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                    .catch(e => {
                        console.warn(`[GAME_TIMEOUT_EDIT_ERR] RPS ${gameId}: ${e.message}. Sending new timeout message.`);
                        safeSendMessage(chatId, timeoutMsgTextRPS, { parse_mode: 'MarkdownV2' });
                    });
            } else {
                safeSendMessage(chatId, timeoutMsgTextRPS, { parse_mode: 'MarkdownV2' });
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}


// --- Callback Handler Functions (Join, Cancel, RPS Choice) ---

async function handleJoinGameCallback(chatId, joinerUser, gameId, interactionMessageId) {
    const joinerId = String(joinerUser.id);
    const gameData = activeGames.get(gameId);

    // --- Validations ---
    if (!gameData) {
        await safeSendMessage(joinerId, "This game is no longer available or has expired.", {});
        // Try to remove buttons from the original message if possible
        if (interactionMessageId && chatId) bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        return;
    }
    // Ensure the join attempt is from the correct chat
    if (gameData.chatId !== String(chatId)) {
        console.warn(`[JOIN_ERR] Game ${gameId} chat mismatch. Expected: ${gameData.chatId}, Got: ${chatId}`);
        await safeSendMessage(joinerId, "Error joining game (chat mismatch).", {});
        return;
    }
    // Prevent initiator from joining their own game
    if (gameData.initiatorId === joinerId) {
        await safeSendMessage(joinerId, "You cannot join a game you started.", {});
        return;
    }
    // Check if game is still waiting for an opponent
    if (gameData.status !== 'waiting_opponent') {
        await safeSendMessage(joinerId, "This game is no longer waiting for an opponent.", {});
        // Remove buttons if game status changed definitively
        if (interactionMessageId && chatId && gameData.status !== 'waiting_opponent') {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        }
        return;
    }
    // Check if game is already full (for 2-player games like CF/RPS)
    if (gameData.participants.length >= 2 && (gameData.type === 'coinflip' || gameData.type === 'rps')) {
        await safeSendMessage(joinerId, "Sorry, this game already has enough players.", {});
        // Remove buttons as game is full
        bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        return;
    }

    // Check joiner's balance
    const joiner = await getUser(joinerId);
    if (joiner.balance < gameData.betAmount) {
        await safeSendMessage(joinerId, `Your balance \\(${escapeMarkdownV2(formatCurrency(joiner.balance))}\\) is too low to join this ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} game\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    // Deduct bet from joiner
    const balanceUpdateResult = await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_group_${gameData.type}_join:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        console.error(`[JOIN_ERR] Failed to deduct balance for joiner ${joinerId}: ${balanceUpdateResult.error}`);
        await safeSendMessage(joinerId, `Error joining game: Could not place bet\\. \\(${escapeMarkdownV2(balanceUpdateResult.error)}\\)`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Add joiner to participants
    gameData.participants.push({ userId: joinerId, choice: null, mention: createUserMention(joinerUser), betPlaced: true });
    activeGames.set(gameId, gameData); // Update game state

    // --- Proceed Based on Game Type ---
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId); // Prefer interaction ID

    if (gameData.type === 'coinflip' && gameData.participants.length === 2) {
        // Coinflip: Resolve immediately
        gameData.status = 'playing'; // Mark as playing briefly
        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        p1.choice = 'heads'; p2.choice = 'tails'; // Assign sides arbitrarily

        const cfResult = determineCoinFlipOutcome(); // Get random outcome
        let winner = (cfResult.outcome === p1.choice) ? p1 : p2;
        let loser = (winner === p1) ? p2 : p1;
        const winnings = gameData.betAmount * 2; // Winner gets total pot (their bet + loser's bet)

        await updateUserBalance(winner.userId, winnings, `won_group_coinflip:${gameId}`, chatId); // Pay winner

        // Construct result message
        const resMsg = `*CoinFlip Resolved\\!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n` +
                       `${p1.mention} \\(Heads\\) vs ${p2.mention} \\(Tails\\)\n\n` +
                       `Landed on: *${escapeMarkdownV2(cfResult.outcomeString)}* ${cfResult.emoji}\\!\n\n` +
                       `🎉 ${winner.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\\!`; // Win amount is opponent's bet

        // Edit original message or send new one
        if (messageToEditId) {
            bot.editMessageText(resMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {} }) // Remove buttons
                .catch(e => { console.warn(`[COINFLIP_EDIT_ERR] ${e.message}`); safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' }); });
        } else {
            safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' });
        }
        // Clean up game state
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);

    } else if (gameData.type === 'rps' && gameData.participants.length === 2) {
        // RPS: Prompt players to make choices
        gameData.status = 'waiting_choices';
        const rpsPrompt = `${gameData.participants[0].mention} & ${gameData.participants[1].mention}, your RPS match for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} is set\\!\nEach player must click a button below to make their choice:`;
        const rpsKeyboard = { inline_keyboard: [[
            { text: `${RPS_EMOJIS.ROCK} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
            { text: `${RPS_EMOJIS.PAPER} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
            { text: `${RPS_EMOJIS.SCISSORS} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
        ]] };
        // Edit original message or send new one
        if (messageToEditId) {
            bot.editMessageText(rpsPrompt, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard })
                .catch(e => { console.warn(`[RPS_EDIT_ERR] ${e.message}`); safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); });
        } else {
            safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard });
        }
        activeGames.set(gameId, gameData); // Save updated status
    }
    // If another game type needs handling after join, add here
}


async function handleCancelGameCallback(chatId, cancellerUser, gameId, interactionMessageId) {
    const cancellerId = String(cancellerUser.id);
    const gameData = activeGames.get(gameId);

    // --- Validations ---
    if (!gameData || gameData.chatId !== String(chatId)) {
        await safeSendMessage(cancellerId, "Game not found or cannot be cancelled from this chat.", {});
        return;
    }
    // Only initiator can cancel before game starts/resolves
    if (gameData.initiatorId !== cancellerId) {
        await safeSendMessage(cancellerId, `Only the initiator \\(${gameData.initiatorMention}\\) can cancel this game\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    // Define which statuses are cancellable
    const cancellableStatuses = ['waiting_opponent', 'waiting_choices'];
    if (!cancellableStatuses.includes(gameData.status)) {
        await safeSendMessage(cancellerId, `This game cannot be cancelled in its current state \\(${escapeMarkdownV2(gameData.status)}\\)\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // --- Process Cancellation ---
    console.log(`[GAME_CANCEL] Game ${gameId} in chat ${chatId} being cancelled by initiator ${cancellerId}.`);
    // Refund bets for all participants who placed one
    for (const participant of gameData.participants) {
        if (participant.betPlaced) {
            await updateUserBalance(participant.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, chatId);
        }
    }

    // Construct cancellation message
    const gameTypeDisplay = gameData.type.charAt(0).toUpperCase() + gameData.type.slice(1); // Capitalize type
    const cancellationMessage = `${gameData.initiatorMention} has cancelled the ${escapeMarkdownV2(gameTypeDisplay)} game \\(Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\\)\\. All bets have been refunded\\.`;

    // Edit original message or send new one
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (messageToEditId) {
        bot.editMessageText(cancellationMessage, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {} }) // Remove buttons
            .catch(e => { console.warn(`[CANCEL_EDIT_ERR] ${e.message}`); safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' }); });
    } else {
        safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
    }

    // Clean up game state
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
}


async function handleRPSChoiceCallback(chatId, userObject, gameId, choice, interactionMessageId) {
    const userId = String(userObject.id);
    const gameData = activeGames.get(gameId);

    // --- Validations ---
    if (!gameData || gameData.chatId !== String(chatId) || gameData.type !== 'rps') {
        await safeSendMessage(userId, "This RPS game isn't available or has ended.", {}); return;
    }
    if (gameData.status !== 'waiting_choices') {
        await safeSendMessage(userId, "The game is not currently waiting for choices.", {}); return;
    }
    const participant = gameData.participants.find(p => p.userId === userId);
    if (!participant) {
        await safeSendMessage(userId, "You are not playing in this RPS game.", {}); return;
    }
    if (participant.choice) {
        // User already made a choice, notify them gently
        await safeSendMessage(userId, `You have already chosen ${RPS_EMOJIS[participant.choice]}\\. Waiting for opponent\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    // Validate the choice received from the callback data
    const normalizedChoiceKey = String(choice).toUpperCase();
    if (!RPS_CHOICES[normalizedChoiceKey]) {
        console.error(`[RPS_CHOICE_ERR] Invalid choice '${choice}' from user ${userId} for game ${gameId}.`);
        await safeSendMessage(userId, `Invalid choice\\. Please click Rock, Paper, or Scissors\\.`, {parse_mode:'MarkdownV2'});
        return;
    }

    // --- Record Choice ---
    participant.choice = RPS_CHOICES[normalizedChoiceKey];
    await safeSendMessage(userId, `You chose ${RPS_EMOJIS[participant.choice]}\\! Waiting for your opponent\\.\\.\\.`, { parse_mode: 'MarkdownV2' });
    console.log(`[RPS_CHOICE] User ${userId} chose ${participant.choice} for game ${gameId}`);
    activeGames.set(gameId, gameData); // Save the choice to the in-memory game state

    // --- Check if Both Players Have Chosen ---
    const otherPlayer = gameData.participants.find(p => p.userId !== userId);
    let groupUpdateMsg = `${participant.mention} has made their choice\\!`;
    let keyboardForUpdate = {}; // Default to removing keyboard after choice

    // If opponent hasn't chosen yet, update message but keep keyboard
    if (otherPlayer && !otherPlayer.choice) {
        groupUpdateMsg += ` Waiting for ${otherPlayer.mention}\\.\\.\\.`;
        // Keep keyboard if opponent still needs to choose
        keyboardForUpdate = { inline_keyboard: [[
             { text: `${RPS_EMOJIS.ROCK} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
             { text: `${RPS_EMOJIS.PAPER} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
             { text: `${RPS_EMOJIS.SCISSORS} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
        ]] };
    }

    // Update the group message (the one with the RPS buttons)
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (messageToEditId) {
        bot.editMessageText(groupUpdateMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: keyboardForUpdate })
            .catch((e) => {console.warn(`[RPS_EDIT_WARN] Failed to edit message ${messageToEditId} after choice: ${e.message}`)});
    }

    // --- Resolve Game if Both Chosen ---
    const allChosen = gameData.participants.length === 2 && gameData.participants.every(p => p.choice);
    if (allChosen) {
        console.log(`[RPS_RESOLVE] Both players have chosen for game ${gameId}. Determining outcome.`);
        gameData.status = 'game_over';
        activeGames.set(gameId, gameData); // Save status before async operations

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        const rpsRes = determineRPSOutcome(p1.choice, p2.choice); // Use logic function from Part 4

        let winnerParticipant = null;
        let resultText = `*Rock Paper Scissors Result\\!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n` +
                         `${p1.mention}: ${RPS_EMOJIS[p1.choice]} vs ${p2.mention}: ${RPS_EMOJIS[p2.choice]}\n\n` +
                         `${escapeMarkdownV2(rpsRes.description)}\n\n`;

        // Determine winner and handle payouts/refunds
        if (rpsRes.result === 'win1') winnerParticipant = p1;
        else if (rpsRes.result === 'win2') winnerParticipant = p2;

        if (winnerParticipant) { // We have a winner
            const winnings = gameData.betAmount * 2; // Total pot
            await updateUserBalance(winnerParticipant.userId, winnings, `won_rps:${gameId}`, chatId);
            resultText += `🎉 ${winnerParticipant.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\\!`;
        } else if (rpsRes.result === 'draw') { // Draw case
            // Refund both players
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, chatId);
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, chatId);
            resultText += `It's a draw\\! Bets have been refunded\\.`;
        } else { // Error case from determineRPSOutcome
            console.error(`[RPS_RESOLVE_ERR] RPS determination error for game ${gameId}. Desc: ${rpsRes.description}`);
            // Refund both players on error
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_error:${gameId}`, chatId);
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_error:${gameId}`, chatId);
            resultText = `An unexpected error occurred resolving RPS\\. Bets have been refunded\\.`;
        }

        // Update the final message
        const finalMsgIdToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
        if (finalMsgIdToEdit) {
            bot.editMessageText(resultText, { chatId: String(chatId), message_id: finalMsgIdToEdit, parse_mode: 'MarkdownV2', reply_markup: {} }) // Remove buttons
                .catch(e => { console.warn(`[RPS_FINAL_EDIT_ERR] ${e.message}`); safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2' }); });
        } else {
            safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2' });
        }

        // Clean up game state
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
}


console.log("Part 5a: Message & Callback Handling (Core & Basic Games) - Complete.");

// --- End of Part 5a ---
//---------------------------------------------------------------------------
///---------------------------------------------------------------------------
// index.js - Part 5b: Dice Escalator Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 5b: Dice Escalator Game Logic...");

// Constants for Dice Escalator
const DICE_ESCALATOR_BOT_ROLLS = 3; // Max rolls for the bot in Dice Escalator if player cashes out
// NOTE: DICE_ESCALATOR_BUST_ON is defined globally (in Part 4)

// --- Dice Escalator Command Handler (/startdice) ---
async function handleStartDiceEscalatorCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    let chatInfo = null; try { chatInfo = await bot.getChat(chatId); } catch (e) { console.warn(`[DE_START_WARN] Could not fetch chat info for chat ${chatId}: ${e.message}`); }
    const chatTitle = chatInfo?.title;
    const gameSession = await getGroupSession(chatId, chatTitle || "Dice Escalator Group");
    const gameId = generateGameId();

    const playerRef = getPlayerDisplayReference(initiatorUser); // Using new helper
    const betFormatted = escapeMarkdownV2(formatCurrency(betAmount));

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `Hold your horses! A game of *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown')}* is already in progress. Please wait for it to conclude.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const initiator = await getUser(initiatorId); // User object needed for balance check etc.
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${playerRef}, your current balance of ${escapeMarkdownV2(formatCurrency(initiator.balance))} isn't quite enough for a *${betFormatted}* wager. Top up and try again!`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_dice_escalator:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        await safeSendMessage(chatId, `A slight hiccup, ${playerRef}. We couldn't place your bet due to: ${escapeMarkdownV2(balanceUpdateResult.error)}. Please try again.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const initialPlaceholderText = `🔥 *Dice Escalator: The Challenge Begins!* 🔥\n${playerRef} throws down the gauntlet with a daring bet of *${betFormatted}* against The House!\n\n_The dice are poised... summoning the first roll!_ ⏳`;
    const sentPlaceholderMsg = await safeSendMessage(chatId, initialPlaceholderText, { parse_mode: 'MarkdownV2' });

    if (!sentPlaceholderMsg) {
        console.error(`[DE_START_ERR] Failed to send initial Dice Escalator message for game ${gameId}. Refunding bet.`);
        await updateUserBalance(initiatorId, betAmount, `refund_de_setup_fail:${gameId}`, chatId);
        return;
    }

    const gameData = {
        type: 'dice_escalator', gameId, chatId: String(chatId), initiatorId,
        playerReference: playerRef, // Store the chosen display reference
        betAmount, playerScore: 0, botScore: 0,
        status: 'waiting_db_roll', currentPlayerId: initiatorId,
        bustValue: DICE_ESCALATOR_BUST_ON, creationTime: Date.now(), commandMessageId,
        gameSetupMessageId: sentPlaceholderMsg.message_id
    };
    activeGames.set(gameId, gameData);
    await updateGroupGameDetails(chatId, gameId, 'DiceEscalator', betAmount);

    try {
        console.log(`[DB_ROLL_REQUEST] Inserting FIRST roll request for DE game ${gameId}, user ${initiatorId}, chat ${chatId}`);
        await queryDatabase(
            'INSERT INTO dice_roll_requests (game_id, chat_id, user_id, status, requested_at) VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (game_id) DO UPDATE SET status = EXCLUDED.status, requested_at = EXCLUDED.requested_at, roll_value = NULL, processed_at = NULL',
            [gameId, String(chatId), initiatorId, 'pending']
        );
        console.log(`[DB_ROLL_REQUEST] Successfully inserted/updated FIRST roll request for DE game ${gameId}.`);
        startPollingForDbResult(gameId, initiatorId, String(chatId), sentPlaceholderMsg.message_id);
    } catch (error) {
        console.error(`[DE_START_ERR] Failed to insert FIRST roll request into DB for game ${gameId}:`, error.message);
        const errorText = `🚧 Uh Oh! Game Start Glitch 🚧\n${playerRef}, we hit a snag initiating your Dice Escalator game. Your bet of *${betFormatted}* has been safely refunded. Please try again shortly!`;
        if (sentPlaceholderMsg) {
            bot.editMessageText(errorText, { chatId: String(chatId), message_id: sentPlaceholderMsg.message_id, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(()=>{
                safeSendMessage(String(chatId), errorText, {parse_mode:'MarkdownV2'});
            });
        } else {
             safeSendMessage(String(chatId), errorText, {parse_mode:'MarkdownV2'});
        }
        await updateUserBalance(initiatorId, betAmount, `refund_de_first_roll_fail:${gameId}`, chatId);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
}

// Processes the player's roll in Dice Escalator
async function processDiceEscalatorPlayerRoll(gameData, playerRoll, messageIdToUpdate) {
    const playerRef = gameData.playerReference; // Use the stored display reference
    const betFormatted = escapeMarkdownV2(formatCurrency(gameData.betAmount));
    const diceFormatted = formatDiceRolls([playerRoll]); // Will now be "🎲 X"
    const bustDiceFormatted = formatDiceRolls([gameData.bustValue]); // Will now be "🎲 1"
    const playerScoreBeforeThisRollFormatted = escapeMarkdownV2(String(gameData.playerScore)); // Score *before* this roll

    if (typeof playerRoll !== 'number' || !Number.isInteger(playerRoll) || playerRoll < 1 || playerRoll > 6) {
        console.error(`[ROLL_INVALID] Invalid roll value: ${playerRoll} for game ${gameData.gameId}.`);
        gameData.status = 'player_turn_prompt_action'; activeGames.set(gameData.gameId, gameData);
        let invalidRollMsg = `🤔 Odd Roll Data!\nThat wasn't a standard dice result (${escapeMarkdownV2(String(playerRoll))}). Your score remains *${playerScoreBeforeThisRollFormatted}*. Try your luck again or cash out!`;
        const kbInvalid = {inline_keyboard:[[{text:`🎲 Roll Again! (Score: ${gameData.playerScore})`,callback_data:`de_roll_prompt:${gameData.gameId}`}], [{text:`💰 Cashout ${escapeMarkdownV2(formatCurrency(gameData.playerScore))}!`,callback_data:`de_cashout:${gameData.gameId}`}]] };
        if (messageIdToUpdate) bot.editMessageText(invalidRollMsg, {chat_id: String(gameData.chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: kbInvalid}).catch(()=>{ safeSendMessage(String(gameData.chatId), invalidRollMsg, {parse_mode:'MarkdownV2'}); }); else safeSendMessage(String(gameData.chatId), invalidRollMsg, {parse_mode:'MarkdownV2'});
        return;
    }

    const { gameId, chatId, bustValue } = gameData;
    const msgId = Number(messageIdToUpdate || gameData.gameSetupMessageId);

    if (!msgId) {
        console.error(`[DE_PLAYER_ROLL_ERR] No messageId for game ${gameId} display update.`);
        await safeSendMessage(String(chatId), `${playerRef}, roll result: ${diceFormatted} (Display Error)`, { parse_mode: 'MarkdownV2' });
    }

    if (playerRoll === bustValue) {
        gameData.status = 'game_over_player_bust'; gameData.playerScore = 0;
        const turnResMsg = `*Your Roll: ${diceFormatted}... Oh No!* 😱\n\n` +
                           `💥 *BUSTED!* 💥\n` +
                           `Ouch, ${playerRef}! Rolling a ${bustDiceFormatted} means your score crumbles to zero! The House claims your *${betFormatted}* bet this round.\n\n` +
                           `_Better luck next time!_`;
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
        if (msgId) { try { await bot.editMessageText(turnResMsg, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: {} }); } catch (e) { console.error(`[DE_PROCESS_ROLL_EDIT_ERR_BUST] Edit fail ${msgId}: ${e.message}`); await safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2' }); } } else { await safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2' }); }
    } else {
        const scoreBeforeRoll = gameData.playerScore;
        gameData.playerScore += playerRoll; gameData.status = 'player_turn_prompt_action';
        const newScoreFormatted = escapeMarkdownV2(String(gameData.playerScore));
        let turnResMsg = "";

        if (scoreBeforeRoll === 0) {
             turnResMsg = `🎲 *First Roll Revealed: ${diceFormatted}!* 🎲\n` +
                          `Alright, ${playerRef}! You're on the board with a starting score of *${newScoreFormatted}*!\n\n` +
                          `💰 Current Payout (Score): *${newScoreFormatted}*\n` +
                          `📈 _Keep climbing! But roll a ${bustDiceFormatted} and you BUST!_ 💥\n\n` +
                          `What's your move, high roller?`;
        } else {
             turnResMsg = `🎲 *Another One: ${diceFormatted}!* 🎲\n` +
                          `Nice roll, ${playerRef}! Your score escalates to *${newScoreFormatted}*!\n\n` +
                          `💰 Current Payout (Score): *${newScoreFormatted}*\n` +
                          `🔥 _The heat is on! Remember, a ${bustDiceFormatted} means BUST!_ 💥\n\n` +
                          `Feeling brave? Roll again or lock in those winnings?`;
        }

        const kb = { inline_keyboard: [ [{ text: `🎲 Roll Again! (Score: ${gameData.playerScore})`, callback_data: `de_roll_prompt:${gameId}` }], [{ text: `💰 Cashout ${escapeMarkdownV2(formatCurrency(gameData.playerScore))}!`, callback_data: `de_cashout:${gameId}` }] ] };

        console.log(`[DE_EDIT_ATTEMPT_DEBUG] Attempting edit: msgId=${msgId}, chatId=${chatId}, type=${typeof chatId}`);
        console.log(`[DE_EDIT_ATTEMPT_DEBUG] Message Content: ${turnResMsg}`);
        if (msgId) {
            try {
                await bot.editMessageText(turnResMsg, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: kb });
                console.log(`[DE_PROCESS_ROLL_EDIT_OK] Successfully edited message ${msgId}.`);
            } catch (e) {
                console.error(`[DE_PROCESS_ROLL_EDIT_ERR] Failed editing message ${msgId} game ${gameId} (chatId=${chatId}):`, e.message);
                await safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2', reply_markup: kb });
            }
        } else { await safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2', reply_markup: kb }); }
        activeGames.set(gameId, gameData);
    }
    console.log(`[ROLL_PROCESS_COMPLETE] Game ${gameData.gameId}`, { newStatus: gameData.status, playerScore: gameData.playerScore });
}

// --- Dice Escalator Callback Handler ---
async function handleDiceEscalatorPlayerAction(gameId, userId, actionType, interactionMessageId, chatId) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== String(chatId) || gameData.type !== 'dice_escalator') {
        await safeSendMessage(userId, "This Dice Escalator game is not available or has ended.", {});
        if (interactionMessageId) bot.editMessageReplyMarkup({}, {chat_id:String(chatId), message_id:Number(interactionMessageId)}).catch(()=>{});
        return;
    }

    const playerRef = gameData.playerReference; // Use the stored display reference
    const playerScoreFormatted = escapeMarkdownV2(String(gameData.playerScore));

    if (gameData.currentPlayerId !== String(userId)) {
        await safeSendMessage(userId, "It's not your turn to act in this game.", {});
        return;
    }
    if (gameData.status !== 'player_turn_prompt_action') {
        await safeSendMessage(userId, `You cannot perform this action now. Game status: ${escapeMarkdownV2(gameData.status)}`, {parse_mode:'MarkdownV2'});
        if (interactionMessageId && gameData.status !== 'player_turn_prompt_action') {
            bot.editMessageReplyMarkup({}, {chat_id:String(chatId), message_id:Number(interactionMessageId)}).catch(()=>{});
        }
        return;
    }
    const msgIdToUpdate = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (!msgIdToUpdate) {
        console.error(`[DE_ACTION_ERR] No valid messageId for game ${gameId} action ${actionType}.`);
        await safeSendMessage(String(chatId), `Error updating game display. Please try again or contact support if the issue persists.`, {parse_mode:'MarkdownV2'});
        return;
    }

    if (actionType === 'de_roll_prompt') {
        gameData.status = 'waiting_db_roll'; activeGames.set(gameId, gameData);

        const promptMessageText = `🎲 _Rolling for ${playerRef}! Fetching that next roll..._ ⏳`;
        try {
            console.log(`[DE_ACTION_EDIT_DEBUG] Attempting MD edit for roll prompt: msgId=${msgIdToUpdate}, chatId=${chatId}`);
            console.log(`[DE_ACTION_EDIT_DEBUG] MD Prompt Content: ${promptMessageText}`);
            await bot.editMessageText(promptMessageText, {
                chat_id: String(chatId),
                message_id: msgIdToUpdate,
                parse_mode: 'MarkdownV2',
                reply_markup: {}
            });
            console.log(`[DE_ACTION_EDIT_DEBUG] MD edit OK for roll prompt msgId=${msgIdToUpdate}.`);
        } catch (editError) {
            console.error(`[DE_ACTION_ERR] Failed MD edit message ${msgIdToUpdate} for roll prompt (game ${gameId}, chatId=${chatId}):`, editError.message);
            const plainFallbackPrompt = `Requesting next roll for ${playerRef}... Please wait. ⏳`;
            try {
                console.log(`[DE_ACTION_EDIT_DEBUG] Attempting PLAIN TEXT fallback edit: msgId=${msgIdToUpdate}, chatId=${chatId}`);
                await bot.editMessageText(plainFallbackPrompt, { chat_id: String(chatId), message_id: msgIdToUpdate, reply_markup: {} });
                console.log(`[DE_ACTION_EDIT_DEBUG] Plain text fallback edit OK for msgId=${msgIdToUpdate}.`);
            } catch (plainEditError) {
                console.error(`[DE_ACTION_ERR] Failed plain text fallback edit for msgId=${msgIdToUpdate}:`, plainEditError.message);
            }
        }

        try {
            console.log(`[DB_ROLL_REQUEST] Inserting SUBSEQUENT roll req ${gameId}, user ${userId}, chat ${chatId}`);
            await queryDatabase('INSERT INTO dice_roll_requests (game_id, chat_id, user_id, status, requested_at) VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (game_id) DO UPDATE SET status = EXCLUDED.status, requested_at = EXCLUDED.requested_at, roll_value = NULL, processed_at = NULL', [gameId, String(chatId), userId, 'pending']);
            console.log(`[DB_ROLL_REQUEST] Success insert SUBSEQUENT ${gameId}.`);
            startPollingForDbResult(gameId, userId, String(chatId), msgIdToUpdate);
        } catch (error) {
            console.error(`[DE_ACTION_ERR] DB insert fail ${gameId}:`, error.message);
            gameData.status = 'player_turn_prompt_action'; activeGames.set(gameId, gameData);
            let errorRestoreMsg = `⚙️ Oops! Roll Request Failed ⚙️\nCouldn't process your roll request this time, ${playerRef}. Your score remains *${playerScoreFormatted}*. Ready to try again or cash out?`;
            const kbError = { inline_keyboard:[[{text:`🎲 Roll Again! (Score: ${gameData.playerScore})`,callback_data:`de_roll_prompt:${gameId}`}], [{text:`💰 Cashout ${escapeMarkdownV2(formatCurrency(gameData.playerScore))}!`,callback_data:`de_cashout:${gameId}`}]] };
            try { await bot.editMessageText(errorRestoreMsg, {chat_id:String(chatId), message_id:msgIdToUpdate, parse_mode:'MarkdownV2', reply_markup:kbError}); } catch (editErr) { console.error(`[DE_ACTION_ERR] Failed to edit for DB insert error: ${editErr.message}`); safeSendMessage(String(chatId), errorRestoreMsg, {parse_mode:'MarkdownV2'}); }
        }

    } else if (actionType === 'de_cashout') {
        if (gameData.playerScore <= 0) { await safeSendMessage(userId, "You need a score greater than 0 to cash out!", {}); return; }
        const scoreCashedOut = gameData.playerScore;
        const totalReturnToBalance = gameData.betAmount + scoreCashedOut;
        const scoreCashedOutFormatted = escapeMarkdownV2(String(scoreCashedOut));
        const totalReturnFormatted = escapeMarkdownV2(formatCurrency(totalReturnToBalance));
        await updateUserBalance(userId, totalReturnToBalance, `cashout_de_player:${gameId}`, chatId);
        gameData.status = 'player_cashed_out'; activeGames.set(gameId, gameData);

        let cashoutMsgText = `💰 *CHA-CHING! Cash Out Success!* 💰\nSmart move, ${playerRef}! You've cashed out with a score of *${scoreCashedOutFormatted}*!\nA total of *${totalReturnFormatted}* is now safely in your balance.\n\n` +
                             `🤖 *Now, The House Responds!* 🤖\nCan the Bot beat your impressive score of *${scoreCashedOutFormatted}*? Let's see...`;

        try { await bot.editMessageText(cashoutMsgText, { chatId: String(chatId), message_id: msgIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: {} }); } catch (error) { console.error(`[DE_ACTION_ERR] Failed edit for cashout ${msgIdToUpdate}: ${error.message}`); await safeSendMessage(String(chatId), cashoutMsgText, { parse_mode: 'MarkdownV2' });}
        await sleep(2000);
        await processDiceEscalatorBotTurn(gameData, msgIdToUpdate);
    }
}

// Processes the Bot's turn in Dice Escalator
async function processDiceEscalatorBotTurn(gameData, messageIdToUpdate) {
    const { gameId, chatId, bustValue } = gameData; // playerReference is in gameData
    const playerRef = gameData.playerReference;
    const playerCashedScore = gameData.playerScore; // This is actually the cashed out score

    let botScore = 0; let botBusted = false; let rollsMadeByBot = 0;
    const playerCashedOutFormatted = escapeMarkdownV2(String(playerCashedScore));

    let botPlaysMsgBase = `💰 *CHA-CHING! Cash Out Success!* 💰\n${playerRef} cashed out with *${playerCashedOutFormatted}* points.\n_Balance Updated!_\n\n` +
                          `🤖 *Bot's Turn!* 🤖\nThe House must now beat *${playerCashedOutFormatted}*...`;

    const msgId = Number(messageIdToUpdate || gameData.gameSetupMessageId);
    if (!msgId) {
        console.error(`[DE_BOT_TURN_ERR] Invalid messageId for game ${gameId} bot turn.`);
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
        await safeSendMessage(String(chatId), `A display error occurred during the bot's turn for game ${gameId}. Game ended.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    try {
        await bot.editMessageText(botPlaysMsgBase + "\n\n_The Bot calculates its odds... Rolling now!_ 🎲", { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: {} });
    } catch (e) { console.error(`[DE_BOT_TURN_EDIT_ERR] Initial bot rolling edit ${gameId}: ${e.message}`); }
    await sleep(2000);

    let rollLog = "";
    while (rollsMadeByBot < DICE_ESCALATOR_BOT_ROLLS && botScore <= playerCashedScore && !botBusted) {
        rollsMadeByBot++;
        const botRollResult = determineDieRollOutcome(); const botRoll = botRollResult.roll;
        const botDiceFormatted = formatDiceRolls([botRoll]); // Will be "🎲 X"

        if (botRoll === bustValue) {
            botBusted = true; botScore = 0;
            rollLog += `\nBot Roll ${rollsMadeByBot}: ${botDiceFormatted} 🔮 → 💥 *BUSTED!*`;
            break;
        } else {
            botScore += botRoll; const botScoreFormatted = escapeMarkdownV2(String(botScore));
            rollLog += `\nBot Roll ${rollsMadeByBot}: ${botDiceFormatted} 🔮 → Bot Score: *${botScoreFormatted}*`;
        }

        let currentDisplayMsg = botPlaysMsgBase + "\n" + rollLog;
        let nextPrompt = (botScore <= playerCashedScore && rollsMadeByBot < DICE_ESCALATOR_BOT_ROLLS && !botBusted) ? "\n\n🤖 _The Bot pushes its luck... Rolling again!_" : "";
        try { await bot.editMessageText(currentDisplayMsg + nextPrompt, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: {} }); } catch (e) { console.error(`[DE_BOT_TURN_EDIT_ERR] Edit during bot roll ${rollsMadeByBot} game ${gameId}: ${e.message}`); }
        if (nextPrompt) await sleep(2500);
    }
    rollLog = rollLog.replace(/\n\n🤖 _The Bot pushes its luck\.\.\. Rolling again!_$/, '');
    await sleep(1500);

    gameData.botScore = botScore; // Ensure botScore is set on gameData if needed by other logic, though not used further here
    const finalBotScoreFormatted = escapeMarkdownV2(String(botScore));
    let finalResultSection = "";

    if (botBusted || botScore <= playerCashedScore) {
         finalResultSection = `\n\n--- 🎉 *YOU WIN! The House Crumbles!* 🎉 ---\n${playerRef}, The Bot ${botBusted ? "BUSTED spectacularly 💥" : `could only muster a score of *${finalBotScoreFormatted}*`}! It couldn't match your masterful cash-out score of *${playerCashedOutFormatted}*.\n\n` +
                              `*Final Standings:*\nYou (Cashed Out): *${playerCashedOutFormatted}* 🏆\nBot: *${finalBotScoreFormatted}* ${botBusted ? "💥" : "😥"}\n\n` +
                              `_Those winnings are all yours! Play again?_`;
    } else {
         finalResultSection = `\n\n--- 💀 *THE HOUSE WINS!* 💀 ---\nClose call, ${playerRef}! The Bot strategically rolled to a stunning *${finalBotScoreFormatted}*, just edging out your cash-out score of *${playerCashedOutFormatted}*.\n\n` +
                              `*Final Standings:*\nYou (Cashed Out): *${playerCashedOutFormatted}*\nBot: *${finalBotScoreFormatted}* 🏆\n\n` +
                              `_The casino always has an edge! Care for a rematch?_`;
    }

    const finalMsg = botPlaysMsgBase + "\n" + rollLog + finalResultSection;
    try { await bot.editMessageText(finalMsg, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: {} }); } catch (e) { console.error(`[DE_BOT_TURN_FINAL_EDIT_ERR] Final edit fail ${gameId}: ${e.message}`); await safeSendMessage(String(chatId), finalMsg, { parse_mode: 'MarkdownV2' }); }

    activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
    console.log(`[DE_BOT_TURN_COMPLETE] Game ${gameId}. Player: ${playerCashedScore}, Bot: ${botScore}, Busted: ${botBusted}`);
}

console.log("Part 5b: Dice Escalator Game Logic - Complete.");
// --- End of Part 5b ---
//---------------------------------------------------------------------------
// index.js - Part 6: Database Initialization, Startup, Shutdown, and Error Handling
//---------------------------------------------------------------------------
console.log("Loading Part 6: Startup, Shutdown, and Basic Error Handling...");

// --- Database Initialization Function ---
// Creates necessary tables if they don't already exist.
async function initializeDatabase() {
    console.log('⚙️ [DB Init] Initializing Database Schema (if necessary)...');
    let client = null; // Use a dedicated client for the transaction
    try {
        client = await pool.connect(); // Get a client from the pool
        await client.query('BEGIN');   // Start a transaction
        console.log('⚙️ [DB Init] Transaction started for schema setup.');

        // Wallets Table (Example structure, adjust to your actual schema needs)
        console.log('⚙️ [DB Init] Ensuring "wallets" table exists...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id VARCHAR(255) PRIMARY KEY,
                external_withdrawal_address VARCHAR(44),
                linked_at TIMESTAMPTZ,
                last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                referral_code VARCHAR(12) UNIQUE,
                referred_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
                referral_count INTEGER NOT NULL DEFAULT 0,
                total_wagered BIGINT NOT NULL DEFAULT 0,
                last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0,
                last_bet_amounts JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        // Add indexes for wallets table (examples)
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_total_wagered ON wallets (total_wagered DESC);');

        // User Balances Table (Example structure)
        console.log('⚙️ [DB Init] Ensuring "user_balances" table exists...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
                balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Bets Table (Example structure for tracking bets)
        console.log('⚙️ [DB Init] Ensuring "bets" table exists...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                chat_id VARCHAR(255) NOT NULL,
                game_type VARCHAR(50) NOT NULL,
                bet_details JSONB,
                wager_amount_lamports BIGINT NOT NULL CHECK (wager_amount_lamports > 0),
                payout_amount_lamports BIGINT, -- Can be NULL if lost or refunded
                status VARCHAR(30) NOT NULL DEFAULT 'active', -- e.g., active, won, lost, refunded
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_user_id_game_type ON bets (user_id, game_type);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_status_created_at ON bets (status, created_at);');


        // --- DICE ROLL REQUESTS TABLE (Crucial for Helper Bot Interaction) ---
        console.log('⚙️ [DB Init] Ensuring "dice_roll_requests" table exists...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS dice_roll_requests (
                request_id SERIAL PRIMARY KEY,
                game_id VARCHAR(255) NOT NULL UNIQUE,      -- Ensures one active roll request per game_id
                chat_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL,             -- The user who initiated the roll prompt
                status VARCHAR(20) NOT NULL DEFAULT 'pending', -- e.g., 'pending', 'processing', 'completed', 'error'
                roll_value INTEGER,                        -- Will be NULL until processed by helper
                requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        // Indexes for efficient querying by helper (status, requested_at) and main bot (game_id)
        console.log('⚙️ [DB Init] Ensuring "dice_roll_requests" indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_status_requested_at ON dice_roll_requests (status, requested_at);');
        // Note: UNIQUE constraint on game_id already creates an index, but explicit index might be desired by some.
        // await client.query('CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_game_id ON dice_roll_requests (game_id);');
        // --- END DICE ROLL REQUESTS TABLE ---


        // Add other table creations (deposits, withdrawals, ledger, etc.) here as needed for your full application.

        await client.query('COMMIT'); // Commit transaction if all schema setup is successful
        console.log('✅ [DB Init] Database schema initialized/verified successfully.');
    } catch (err) {
        console.error('❌ CRITICAL DATABASE INITIALIZATION ERROR:', err);
        if (client) {
            try {
                await client.query('ROLLBACK'); // Rollback transaction on error
                console.log('⚙️ [DB Init] Transaction rolled back due to schema setup error.');
            } catch (rbErr) {
                console.error('[DB Init_ERR] Rollback failed:', rbErr);
            }
        }
        // Notify admin if configured and possible
        if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
            safeSendMessage(ADMIN_USER_ID, `🚨 CRITICAL DB INIT FAILED: ${escapeMarkdownV2(String(err.message || err))}\\. Bot cannot start\\. Check logs\\.`, {parse_mode:'MarkdownV2'}).catch(() => {});
        }
        process.exit(2); // Exit with a specific code indicating DB init failure
    } finally {
        if (client) {
            client.release(); // Release the client back to the pool
            console.log('⚙️ [DB Init] Database client released.');
        }
    }
}

// --- Optional Periodic Background Tasks ---
// Cleans up stale games or sessions from in-memory stores.
async function runPeriodicBackgroundTasks() {
    console.log(`[BACKGROUND_TASK] [${new Date().toISOString()}] Running periodic background tasks...`);
    const now = Date.now();
    const GAME_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 10; // Example: 10 minutes for a game to be considered stale
    let cleanedGames = 0;

    for (const [gameId, gameData] of activeGames.entries()) {
        // Define statuses that indicate a game might be stale if too old
        const staleStatuses = ['waiting_opponent', 'waiting_choices', 'waiting_db_roll'];
        if ((now - gameData.creationTime > GAME_CLEANUP_THRESHOLD_MS) && staleStatuses.includes(gameData.status)) {
            console.warn(`[BACKGROUND_TASK] Cleaning stale game ${gameId} (${gameData.type}) in chat ${gameData.chatId}. Status: ${gameData.status}`);

            // Handle refund or specific cleanup actions based on game type and status
            let refundReason = `refund_stale_${gameData.type}_timeout:${gameId}`;
            let staleMsgText = `Game \\(ID: \`${escapeMarkdownV2(gameId)}\`\\) by ${gameData.initiatorMention} was cleared due to inactivity`;

            if (gameData.type === 'dice_escalator' && gameData.status === 'waiting_db_roll') {
                staleMsgText += " \\(roll not processed in time\\)\\.";
                 // Only refund if it was the first roll (playerScore still 0)
                 if(gameData.playerScore === 0) {
                    await updateUserBalance(gameData.initiatorId, gameData.betAmount, refundReason, gameData.chatId);
                    staleMsgText += " Bet refunded\\.";
                 } else {
                     staleMsgText += ` Last score was ${gameData.playerScore}\\.`;
                 }
                // Clean up pending request from DB if it's stuck
                try {
                    await queryDatabase("DELETE FROM dice_roll_requests WHERE game_id = $1 AND status = 'pending'", [gameId]);
                    console.log(`[BACKGROUND_TASK] Deleted stale 'pending' dice_roll_request for game ${gameId}.`);
                } catch (dbErr) {
                    console.error(`[BACKGROUND_TASK_ERR] Failed to delete stale dice_roll_request for game ${gameId}:`, dbErr.message);
                }
            } else if ((gameData.type === 'coinflip' || gameData.type === 'rps') && gameData.status === 'waiting_opponent') {
                await updateUserBalance(gameData.initiatorId, gameData.betAmount, refundReason, gameData.chatId);
                staleMsgText += "\\. Bet refunded\\.";
            } else if (gameData.type === 'rps' && gameData.status === 'waiting_choices') {
                staleMsgText += " during choice phase\\. Bets refunded to all participants\\.";
                for (const p of gameData.participants) { if (p.betPlaced) { await updateUserBalance(p.userId, gameData.betAmount, refundReason, gameData.chatId); }}
            }

            // Notify chat and clean up
            if (gameData.gameSetupMessageId) {
                bot.editMessageText(staleMsgText, { chatId: String(gameData.chatId), message_id: Number(gameData.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }).catch(() => { safeSendMessage(String(gameData.chatId), staleMsgText, { parse_mode: 'MarkdownV2' }); });
            } else {
                safeSendMessage(String(gameData.chatId), staleMsgText, { parse_mode: 'MarkdownV2' });
            }

            activeGames.delete(gameId);
            const groupSession = await getGroupSession(gameData.chatId); // Get session to update
            if (groupSession && groupSession.currentGameId === gameId) {
                await updateGroupGameDetails(gameData.chatId, null, null, null); // Clear game from session
            }
            cleanedGames++;
        }
    }
    if (cleanedGames > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedGames} stale game(s).`);

    // Clean up very old, inactive group sessions (no active game)
    const SESSION_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 30; // Example: 30 minutes
    let cleanedSessions = 0;
    for (const [chatId, sessionData] of groupGameSessions.entries()) {
        if (!sessionData.currentGameId && sessionData.lastActivity instanceof Date && (now - sessionData.lastActivity.getTime()) > SESSION_CLEANUP_THRESHOLD_MS) {
            console.log(`[BACKGROUND_TASK] Cleaning inactive group session for chat ${chatId}.`);
            groupGameSessions.delete(chatId);
            cleanedSessions++;
        }
    }
    if (cleanedSessions > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedSessions} inactive group session entries.`);
    console.log(`[BACKGROUND_TASK] Finished. Active games: ${activeGames.size}, Group sessions: ${groupGameSessions.size}.`);
}
// To enable periodic tasks, uncomment the line below:
// const backgroundTaskInterval = setInterval(runPeriodicBackgroundTasks, 15 * 60 * 1000); // e.g., every 15 minutes

// --- Process-level Error Handling ---
process.on('uncaughtException', (error, origin) => {
    console.error(`\n🚨🚨 MAIN BOT UNCAUGHT EXCEPTION AT: ${origin} 🚨🚨`, error);
    if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
        safeSendMessage(ADMIN_USER_ID, `🆘 MAIN BOT UNCAUGHT EXCEPTION:\nOrigin: ${escapeMarkdownV2(origin)}\nError: ${escapeMarkdownV2(error.message)}`, {parse_mode:'MarkdownV2'}).catch(() => {});
    }
    // Consider if bot should exit or attempt to recover. For critical errors, exit is safer.
    // process.exit(1); // Uncomment to exit on uncaught exceptions
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`\n🔥🔥 MAIN BOT UNHANDLED REJECTION 🔥🔥`, reason);
    // console.error("Promise:", promise); // Can be very verbose
    if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
        const reasonMsg = (reason instanceof Error) ? reason.message : String(reason);
        safeSendMessage(ADMIN_USER_ID, `♨️ MAIN BOT UNHANDLED REJECTION:\nReason: ${escapeMarkdownV2(reasonMsg)}`, {parse_mode:'MarkdownV2'}).catch(() => {});
    }
});

// --- Telegram Bot Library Specific Error Handling ---
if (bot) { // Ensure bot object exists before attaching handlers
    bot.on('polling_error', (error) => {
        console.error(`\n🚫 MAIN BOT TELEGRAM POLLING ERROR 🚫 Code: ${error.code || 'N/A'}`);
        console.error(`Message: ${error.message || 'No message'}`);
        // Log full error object for more details, can be verbose
        // console.error(error);
        // Specific handling for common polling errors:
        // E.g., 'EFATAL' often means network issue or token revoked.
        // 'ECONNRESET', 'ETIMEOUT' are network related.
        // Consider if bot should attempt to restart polling or exit based on error.
    });
    bot.on('webhook_error', (error) => { // If using webhooks
        console.error(`\n🚫 MAIN BOT TELEGRAM WEBHOOK ERROR 🚫 Code: ${error.code || 'N/A'}`);
        console.error(`Message: ${error.message || 'No message'}`);
    });
    bot.on('error', (error) => { // General errors from the bot instance itself
        console.error('\n🔥 MAIN BOT GENERAL TELEGRAM LIBRARY ERROR EVENT 🔥:', error);
    });
} else {
    console.error("!!! CRITICAL ERROR: 'bot' instance not defined when trying to attach general error handlers in Part 6 !!!");
}

// --- Shutdown Handling ---
let isShuttingDown = false;
async function shutdown(signal) {
    if (isShuttingDown) {
        console.log("Main Bot: Shutdown already in progress.");
        return;
    }
    isShuttingDown = true;
    console.log(`\n🚦 Received ${signal}. Shutting down Main Bot v${BOT_VERSION} gracefully...`);

    // 1. Stop Telegram Polling (if polling)
    if (bot && typeof bot.stopPolling === 'function' && bot.isPolling()) {
        try {
            await bot.stopPolling({ cancel: true }); // Cancel pending requests
            console.log("Main Bot: Telegram polling stopped.");
        } catch (e) {
            console.error("Main Bot: Error stopping Telegram polling:", e.message);
        }
    }
    // If using webhooks, you might need bot.deleteWebHook();

    // 2. Clear any intervals (like backgroundTaskInterval if it was active)
    // if (backgroundTaskInterval) clearInterval(backgroundTaskInterval);

    // 3. Gracefully close the Database Pool
    if (pool && typeof pool.end === 'function') {
        try {
            await pool.end();
            console.log("Main Bot: PostgreSQL pool has been closed.");
        } catch (e) {
            console.error("Main Bot: Error closing PostgreSQL pool:", e.message);
        }
    }

    // 4. Optional: Notify admin about shutdown
    if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
        // Use a timeout to allow other shutdown tasks to complete before exiting process
        // safeSendMessage might rely on the event loop which is about to be terminated.
        // This is a best-effort notification.
        const notifyAdminPromise = safeSendMessage(ADMIN_USER_ID, `ℹ️ Bot v${BOT_VERSION} shutting down (Signal: ${signal}).`, {}).catch(() => {});
        // Wait for a short period or for the promise to resolve/reject
        await Promise.race([notifyAdminPromise, new Promise(resolve => setTimeout(resolve, 1000))]);
    }

    console.log("✅ Main Bot: Shutdown complete. Exiting.");
    // Exit with appropriate code based on signal or if it was an error-triggered shutdown elsewhere
    process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
}
// Register signal handlers for graceful shutdown
process.on('SIGINT', () => shutdown('SIGINT'));  // CTRL+C from terminal
process.on('SIGTERM', () => shutdown('SIGTERM')); // kill command (default)

// --- Main Startup Function ---
async function main() {
    console.log(`\n🚀🚀🚀 Initializing Group Chat Casino Bot v${BOT_VERSION} 🚀🚀🚀`);
    console.log(`Timestamp: ${new Date().toISOString()}`);

    // 1. Initialize Database Schema (creates tables if they don't exist)
    await initializeDatabase();
    console.log("Main Bot: Database initialization sequence completed.");

    // 2. Connect to Telegram and get Bot Info
    try {
        console.log("Main Bot: Connecting to Telegram...");
        const me = await bot.getMe();
        console.log(`✅ Successfully connected to Telegram! Bot Name: @${me.username}, Bot ID: ${me.id}`);

        // 3. Notify Admin (if configured)
        if (ADMIN_USER_ID) {
            await safeSendMessage(ADMIN_USER_ID, `🎉 Bot v${BOT_VERSION} (DB Dice Roll Mode) started! Polling active. Host: ${process.env.HOSTNAME || 'local'}.`, { parse_mode: 'MarkdownV2' });
        }
        console.log(`\n🎉 Main Bot operational! Waiting for messages...`);

        // 4. Optional: Run background tasks once shortly after startup
        // setTimeout(runPeriodicBackgroundTasks, 15000); // Run 15s after start

    } catch (error) {
        console.error("❌ CRITICAL STARTUP ERROR (Main Bot: getMe or DB Init earlier):", error);
        if (ADMIN_USER_ID && BOT_TOKEN) { // Attempt to notify admin even on critical failure
            // Create a temporary, non-polling bot instance for notification
            try {
                const tempBot = new TelegramBot(BOT_TOKEN, {}); // No polling
                await tempBot.sendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE Main Bot v${BOT_VERSION}:\n${escapeMarkdownV2(error.message)}\nBot is exiting. Check logs.`, {parse_mode:'MarkdownV2'}).catch(() => {});
            } catch (tempBotError) {
                console.error("Main Bot: Failed to create temporary bot for failure notification:", tempBotError);
            }
        }
        // If initializeDatabase failed, process.exit(2) might have already been called.
        // If getMe failed or other critical error, exit with code 1.
        if (process.exitCode === undefined || process.exitCode === 0) {
            process.exit(1); // Ensure exit if not already exiting
        }
    }
} // End main() function

// --- Final Execution: Start the Bot ---
main().catch(error => {
    // This catches errors from the main async function itself if something goes wrong
    // that isn't handled within main's try/catch (e.g., an await outside try/catch).
    console.error("❌ MAIN ASYNC FUNCTION UNHANDLED ERROR (Should not happen if main() has good try/catch):", error);
    process.exit(1); // Exit if the main promise chain fails catastrophically
});

console.log("Main Bot: End of index.js script. Bot startup process initiated.");
// --- END OF index.js ---
