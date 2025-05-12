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
    'DB_REJECT_UNAUTHORIZED': 'false', // <<< CRITICAL CHANGE FOR RAILWAY: Default to false
    // Add other non-DB defaults here if they were in your original snippet
};

// Apply defaults if not set in process.env
// This ensures process.env.DB_SSL and process.env.DB_REJECT_UNAUTHORIZED have values
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
    if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
        safeSendMessage(ADMIN_USER_ID, `🚨 DATABASE POOL ERROR (Idle Client): ${escapeMarkdownV2(err.message || String(err))}`)
            .catch(notifyErr => console.error("Failed to notify admin about DB pool error:", notifyErr));
    } else {
        console.error(`[ADMIN ALERT during DB Pool Error (Idle Client)] ${err.message || String(err)} (safeSendMessage or escapeMarkdownV2 might not be defined yet if error is early)`);
    }
});
console.log("✅ PostgreSQL Pool created.");


const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("Telegram Bot instance created and configured for polling.");

const BOT_VERSION = '2.0.3-db-ssl-fix'; // Updated version marker
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;

let activeGames = new Map(); // For in-memory game state
let userCooldowns = new Map(); // For command cooldowns

console.log(`Group Chat Casino Bot v${BOT_VERSION} initializing...`);
console.log(`Current system time: ${new Date().toISOString()}`);

// escapeMarkdownV2 and safeSendMessage must be defined before they are used by pool.on('error') if admin notifications are desired there.
// Moved them up slightly.
const escapeMarkdownV2 = (text) => {
    if (text === null || typeof text === 'undefined') return '';
    return String(text).replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};

async function safeSendMessage(chatId, text, options = {}) {
    if (!chatId || typeof text !== 'string') {
        console.error("[safeSendMessage] Invalid input:", { chatId, textPreview: String(text).substring(0, 50) });
        return undefined;
    }
    let messageToSend = text;
    let finalOptions = { ...options };

    if (finalOptions.parse_mode === 'MarkdownV2') {
        messageToSend = escapeMarkdownV2(text);
    }

    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsis = "... (message truncated)";
        let escapedEllipsis = (finalOptions.parse_mode === 'MarkdownV2') ? escapeMarkdownV2(ellipsis) : ellipsis;
        const truncateAt = MAX_MARKDOWN_V2_MESSAGE_LENGTH - escapedEllipsis.length;
        messageToSend = (truncateAt > 0) ? messageToSend.substring(0, truncateAt) + escapedEllipsis : messageToSend.substring(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH);
        console.warn(`[safeSendMessage] Message for chat ${chatId} was truncated.`);
    }

    try {
        return await bot.sendMessage(chatId, messageToSend, finalOptions);
    } catch (error) {
        console.error(`[safeSendMessage] Failed to send to chat ${chatId}. Code: ${error.code || 'N/A'}, Msg: ${error.message}`);
        if (error.response?.body) console.error(`[safeSendMessage] API Response: ${JSON.stringify(error.response.body)}`);
        return undefined;
    }
}
console.log("Part 1: Core Imports & Basic Setup - Complete.");
//---------------------------------------------------------------------------
// index.js - Part 2: Database Operations & Data Management
//---------------------------------------------------------------------------
console.log("Loading Part 2: Database Operations & Data Management...");

// In-memory stores for data not persisted or for quick access (user stats, game sessions)
// Note: Actual user balances for a production bot would ideally be stored in and fetched from your 'user_balances' table.
// For this example, we're keeping the simple in-memory balance system to focus on the dice roll DB integration.
const userDatabase = new Map();
const groupGameSessions = new Map();
console.log("In-memory data stores (userDatabase, groupGameSessions) for non-persistent data initialized.");


// --- queryDatabase Helper Function (from your provided code) ---
async function queryDatabase(sql, params = [], dbClient = pool) {
    if (!dbClient) { // Ensure dbClient is valid, default to global pool if necessary
        if (!pool) {
            const poolError = new Error("Database pool not available for queryDatabase");
            console.error("❌ CRITICAL: queryDatabase called but default pool is not initialized!", poolError.stack);
            throw poolError;
        }
        dbClient = pool;
    }
    if (typeof sql !== 'string' || sql.trim().length === 0) {
        const sqlError = new TypeError(`queryDatabase received invalid SQL query (type: ${typeof sql}, value: ${sql})`);
        console.error(`❌ DB Query Error:`, sqlError.message);
        console.error(`   Params: ${JSON.stringify(params, (k, v) => typeof v === 'bigint' ? v.toString() + 'n' : v)}`);
        throw sqlError;
    }

    try {
        return await dbClient.query(sql, params);
    } catch (error) {
        console.error(`❌ DB Query Error:`);
        console.error(`   SQL: ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`);
        const safeParamsString = JSON.stringify(params, (key, value) =>
            typeof value === 'bigint'
                ? value.toString() + 'n' // Indicate BigInt in log
                : value // Return other values unchanged
        );
        console.error(`   Params: ${safeParamsString}`);
        console.error(`   Error Code: ${error.code || 'N/A'}`);
        console.error(`   Error Message: ${error.message}`);
        if (error.constraint) { console.error(`   Constraint: ${error.constraint}`); }
        throw error; // Re-throw to be handled by caller
    }
}


// User and Group Session functions (using simplified in-memory store for this example's game state)
async function getUser(userId, username) {
    const userIdStr = String(userId);
    if (!userDatabase.has(userIdStr)) {
        const newUser = {
            userId: userIdStr, username, balance: 1000, // Default starting balance
            lastPlayed: null,
            groupStats: new Map(), isNew: true,
        };
        userDatabase.set(userIdStr, newUser);
        console.log(`[IN_MEM_DB] New user (in-memory): ${userIdStr} (@${username||'N/A'}), Bal: ${newUser.balance}`);
        return { ...newUser }; // Return a copy
    }
    const user = userDatabase.get(userIdStr);
    if (username && user.username !== username) { // Update username if changed
        user.username = username;
    }
    user.isNew = false;
    return { ...user, groupStats: new Map(user.groupStats) }; // Return a deep copy
}

async function updateUserBalance(userId, amountChange, reason = "unknown_transaction", chatId) {
    // This function updates the IN-MEMORY balance.
    // In a full DB system, this would interact with your 'user_balances' table using queryDatabase.
    const userIdStr = String(userId);
    if (!userDatabase.has(userIdStr)) {
        console.warn(`[IN_MEM_DB_ERROR] Update balance for non-existent user: ${userIdStr}`);
        return { success: false, error: "User not found." };
    }
    const user = userDatabase.get(userIdStr);
    const proposedBalance = user.balance + amountChange;

    if (proposedBalance < 0) {
        console.log(`[IN_MEM_DB] User ${userIdStr} insufficient balance (${user.balance}) for deduction of ${Math.abs(amountChange)} (Reason: ${reason}).`);
        return { success: false, error: "Insufficient balance." };
    }
    user.balance = proposedBalance;
    user.lastPlayed = new Date();
    console.log(`[IN_MEM_DB] User ${userIdStr} balance: ${user.balance} (Change: ${amountChange}, Reason: ${reason}, Chat: ${chatId||'N/A'})`);
    
    // Simplified in-memory stat tracking (as in original code)
    if (chatId) {
        const chatIdStr = String(chatId);
        if (!user.groupStats.has(chatIdStr)) user.groupStats.set(chatIdStr, { gamesPlayed: 0, totalWagered: 0, netWinLoss: 0 });
        const stats = user.groupStats.get(chatIdStr);
        if (reason.toLowerCase().includes("bet_")) {
            const wager = Math.abs(amountChange); stats.totalWagered += wager; stats.netWinLoss -= wager;
        } else if (reason.toLowerCase().includes("won_") || reason.toLowerCase().includes("payout_") || reason.toLowerCase().includes("cashout")) {
            stats.netWinLoss += amountChange;
            if (!reason.toLowerCase().includes("refund_") && !reason.toLowerCase().includes("cashout") && amountChange > 0) stats.gamesPlayed += 1;
        } else if (reason.toLowerCase().includes("lost_")) {
             if (!reason.toLowerCase().includes("bust")) stats.gamesPlayed += 1;
        } else if (reason.toLowerCase().includes("refund_")) {
            stats.netWinLoss += amountChange;
        }
    }
    return { success: true, newBalance: user.balance };
}

async function getGroupSession(chatId, chatTitle) {
    const chatIdStr = String(chatId);
    if (!groupGameSessions.has(chatIdStr)) {
        const newSession = {
            chatId: chatIdStr, chatTitle: chatTitle || "Group Chat", currentGameId: null, currentGameType: null,
            currentBetAmount: null, lastActivity: new Date(),
        };
        groupGameSessions.set(chatIdStr, newSession);
        console.log(`[IN_MEM_SESS] New group session: ${chatIdStr} (${newSession.chatTitle}).`);
        return { ...newSession }; // Return a copy
    }
    const session = groupGameSessions.get(chatIdStr);
    // Update title if provided and different (e.g., if group title changes)
    if (chatTitle && session.chatTitle !== chatTitle) {
         session.chatTitle = chatTitle;
         console.log(`[IN_MEM_SESS] Updated title for session ${chatIdStr} to "${chatTitle}"`);
    }
    session.lastActivity = new Date();
    return { ...session }; // Return a copy
}

async function updateGroupGameDetails(chatId, gameId, gameType, betAmount) {
    const chatIdStr = String(chatId);
    // Ensure session exists before trying to update it
    if (!groupGameSessions.has(chatIdStr)) {
        // This might happen if a game update is called before getGroupSession, or if session was cleared
        console.warn(`[IN_MEM_SESS_WARN] Group session ${chatIdStr} not found while trying to update game details. Creating one.`);
        await getGroupSession(chatIdStr, "Unknown Group (Auto-created during game update)"); // Attempt to create
    }
    const session = groupGameSessions.get(chatIdStr);
    // If session still doesn't exist after attempt to create, log error and return false
    if (!session) {
       console.error(`[IN_MEM_SESS_ERROR] Failed to get/create session for ${chatIdStr} during game details update.`);
       return false;
    }
    session.currentGameId = gameId;
    session.currentGameType = gameType;
    session.currentBetAmount = betAmount;
    session.lastActivity = new Date();
    console.log(`[IN_MEM_SESS] Group ${chatIdStr} game updated -> GameID: ${gameId||'None'}, Type: ${gameType||'None'}, Bet: ${betAmount !== null ? formatCurrency(betAmount) : 'N/A'}`);
    return true;
}
console.log("Part 2: Database Operations & Data Management - Complete.");

//---------------------------------------------------------------------------
// index.js - Part 3: Telegram Helpers & Basic Game Utilities
//---------------------------------------------------------------------------
console.log("Loading Part 3: Telegram Helpers & Basic Game Utilities...");

function getEscapedUserDisplayName(userObject) {
    if (!userObject) return escapeMarkdownV2("Anonymous User");
    const name = userObject.first_name || userObject.username || `User ${userObject.id}`;
    return escapeMarkdownV2(name);
}

function createUserMention(userObject) {
    if (!userObject || !userObject.id) return escapeMarkdownV2("Unknown User");
    const displayName = userObject.first_name || userObject.username || `User ${userObject.id}`;
    return `[${escapeMarkdownV2(displayName)}](tg://user?id=${userObject.id})`;
}

function formatCurrency(amount, currencyName = "credits") {
    let num = Number(amount); if (isNaN(num)) num = 0;
    return `${num.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: (num % 1 === 0 ? 0 : 2) })} ${currencyName}`;
}

function rollDie(sides = 6) { // This is the main bot's internal roll, used for BOT's turn in DE
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6;
    return Math.floor(Math.random() * sides) + 1;
}

function formatDiceRolls(rolls) {
    const diceEmojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"];
    const diceVisuals = rolls.map(roll => (roll >= 1 && roll <= 6) ? diceEmojis[roll - 1] : `🎲${roll}`);
    return `Rolls: ${diceVisuals.join(' ')}`;
}

function generateGameId() {
    const timestamp = Date.now();
    const randomSuffix = Math.random().toString(36).substring(2, 9);
    return `game_${timestamp}_${randomSuffix}`;
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
console.log("Part 3: Telegram Helpers & Basic Game Utilities - Complete.");

//---------------------------------------------------------------------------
// index.js - Part 4: Simplified Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 4: Simplified Game Logic...");

function determineCoinFlipOutcome() {
    const isHeads = Math.random() < 0.5;
    return isHeads ? { outcome: 'heads', outcomeString: "Heads", emoji: '🪙' } : { outcome: 'tails', outcomeString: "Tails", emoji: '🪙' };
}

function determineDieRollOutcome(sides = 6) { // For BOT's internal rolls in Dice Escalator
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6;
    const roll = Math.floor(Math.random() * sides) + 1;
    const emojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"];
    return { roll: roll, emoji: (roll >= 1 && roll <= 6) ? emojis[roll - 1] : `🎲${roll}` };
}

const DICE_ESCALATOR_BUST_ON = 1; // Player busts if they roll a 1

const RPS_CHOICES = { ROCK: 'rock', PAPER: 'paper', SCISSORS: 'scissors' };
const RPS_EMOJIS = { [RPS_CHOICES.ROCK]: '🪨', [RPS_CHOICES.PAPER]: '📄', [RPS_CHOICES.SCISSORS]: '✂️' };
const RPS_RULES = {
    [RPS_CHOICES.ROCK]: { beats: RPS_CHOICES.SCISSORS, verb: "crushes" },
    [RPS_CHOICES.PAPER]: { beats: RPS_CHOICES.ROCK, verb: "covers" },
    [RPS_CHOICES.SCISSORS]: { beats: RPS_CHOICES.PAPER, verb: "cuts" }
};

function getRandomRPSChoice() {
    const choices = Object.values(RPS_CHOICES);
    const randomChoice = choices[Math.floor(Math.random() * choices.length)];
    return { choice: randomChoice, emoji: RPS_EMOJIS[randomChoice] };
}

function determineRPSOutcome(choice1, choice2) {
    const c1_key = String(choice1).toUpperCase(); 
    const c2_key = String(choice2).toUpperCase();

    if (!RPS_CHOICES[c1_key] || !RPS_CHOICES[c2_key]) {
        console.warn(`[RPS_WARN] Invalid choices in determineRPSOutcome: P1='${choice1}', P2='${choice2}'`);
        return { result: 'error', description: "Invalid choices were made.", choice1, choice1Emoji: '❔', choice2, choice2Emoji: '❔' };
    }

    const c1 = RPS_CHOICES[c1_key]; 
    const c2 = RPS_CHOICES[c2_key];
    const c1E = RPS_EMOJIS[c1]; 
    const c2E = RPS_EMOJIS[c2];

    if (c1 === c2) {
        return { result: 'draw', description: `${c1E} ${c1} vs ${c2E} ${c2}. It's a Draw!`, choice1:c1,choice1Emoji:c1E, choice2:c2,choice2Emoji:c2E };
    }
    if (RPS_RULES[c1].beats === c2) {
        return { result: 'win1', description: `${c1E} ${c1} ${RPS_RULES[c1].verb} ${c2E} ${c2}. Player 1 wins!`, choice1:c1,choice1Emoji:c1E, choice2:c2,choice2Emoji:c2E };
    }
    return { result: 'win2', description: `${c2E} ${c2} ${RPS_RULES[c2].verb} ${c1E} ${c1}. Player 2 wins!`, choice1:c1,choice1Emoji:c1E, choice2:c2,choice2Emoji:c2E };
}
console.log("Part 4: Simplified Game Logic - Complete.");

//---------------------------------------------------------------------------
// index.js - Part 5: Message & Callback Handling, Basic Game Flow
//---------------------------------------------------------------------------
console.log("Loading Part 5: Message & Callback Handling, Basic Game Flow...");

const COMMAND_COOLDOWN_MS = 2000; // 2 seconds between commands for a user
const JOIN_GAME_TIMEOUT_MS = 60000; // 60 seconds for someone to join a game
// const DICE_ESCALATOR_TIMEOUT_MS = 120000; // Original, now DB polling has its own timeout
const MIN_BET_AMOUNT = 5;
const MAX_BET_AMOUNT = 1000;
const DICE_ESCALATOR_BOT_ROLLS = 3; // Max rolls for the bot in Dice Escalator

// --- Main Message Handler ---
bot.on('message', async (msg) => {
    // Log every message received by the library
    if (msg && msg.from && msg.chat) {
        console.log(`[ULTRA_RAW_LOG] Text: "${msg.text || 'N/A'}", FromID: ${msg.from.id}, User: @${msg.from.username || 'N/A'}, IsBot: ${msg.from.is_bot}, ChatID: ${msg.chat.id}`);
    } else {
        console.log(`[ULTRA_RAW_LOG] Received incomplete/malformed message object. Msg:`, JSON.stringify(msg).substring(0, 200));
        return; // Exit if essential message parts are missing
    }

    // The ✅✅✅ DIAGNOSTIC check for helper bot ID is removed as dice rolls are now via DB.
    // The main bot no longer directly listens for a Telegram helper bot for dice rolls.

    if (!msg.from) { // Message must have a sender
        console.warn("[MSG_HANDLER_WARN] Message received without 'from' field. Ignoring.", msg);
        return;
    }

    // Ignore messages from other bots (but not from itself, if the bot ever needs to react to its own messages for some reason)
    // The primary dice roll mechanism no longer relies on identifying a specific helper bot via Telegram messages.
    if (msg.from.is_bot) {
        const selfBotInfo = await bot.getMe();
        if (String(msg.from.id) !== String(selfBotInfo.id)) { // If message is from a bot AND it's not this bot itself
            console.log(`[MSG_IGNORE] Ignoring message from other bot (ID: ${msg.from.id}, User: @${msg.from.username || 'N/A'}).`);
            return; // Stop processing messages from other bots
        }
        // If it's a message from self, allow it to proceed (though current logic doesn't use this)
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text || ""; // Ensure text is always a string, default to empty
    const chatType = msg.chat.type;
    const messageId = msg.message_id;

    // Apply command cooldown for non-bot users
    if (!msg.from.is_bot) {
        const now = Date.now();
        if (text.startsWith('/') && userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
            console.log(`[COOLDOWN] User ${userId} command ("${text}") ignored due to cooldown.`);
            // Optionally send a message to the user about cooldown, but can be spammy.
            // await safeSendMessage(chatId, `${createUserMention(msg.from)}, please wait a moment.`, {parse_mode: 'MarkdownV2'});
            return;
        }
        if (text.startsWith('/')) { // If it's a command, update their cooldown timestamp
            userCooldowns.set(userId, now);
        }
    }

    // Process commands if the message starts with '/' and is not from a bot
    if (text.startsWith('/') && !msg.from.is_bot) {
        const commandArgs = text.substring(1).split(' ');
        const commandName = commandArgs.shift().toLowerCase();
        console.log(`[CMD RCV] Chat: ${chatId}, User: ${userId} (@${msg.from.username || 'N/A'}), Cmd: /${commandName}, Args: ${commandArgs.join(' ')}`);

        // Ensure user exists in our in-memory system
        await getUser(userId, msg.from.username);

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
                    let betAmountCF = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10; // Default bet 10
                    if (isNaN(betAmountCF) || betAmountCF < MIN_BET_AMOUNT || betAmountCF > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet amount. Must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startcoinflip <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupCoinFlipCommand(chatId, msg.from, betAmountCF, messageId);
                } else {
                    await safeSendMessage(chatId, "This Coinflip game is for group chats only.", {});
                }
                break;
            case 'startrps':
                if (chatType !== 'private') {
                    let betAmountRPS = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10; // Default bet 10
                    if (isNaN(betAmountRPS) || betAmountRPS < MIN_BET_AMOUNT || betAmountRPS > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet amount. Must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startrps <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupRPSCommand(chatId, msg.from, betAmountRPS, messageId);
                } else {
                    await safeSendMessage(chatId, "This Rock Paper Scissors game is for group chats only.", {});
                }
                break;
            case 'startdiceescalator':
                if (chatType === 'group' || chatType === 'supergroup') {
                    let betAmountDE = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10; // Default bet 10
                    if (isNaN(betAmountDE) || betAmountDE < MIN_BET_AMOUNT || betAmountDE > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet for Dice Escalator. Amount must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startdiceescalator <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartDiceEscalatorCommand(chatId, msg.from, betAmountDE, messageId);
                } else {
                    await safeSendMessage(chatId, "Dice Escalator can only be started in a group or supergroup chat.", {});
                }
                break;
            // Removed 'debughelper' command as direct helper bot interaction is removed for dice rolls
            default:
                // Only reply for unknown commands in private chat or if it's clearly a command attempt
                if (chatType === 'private' || text.startsWith('/')) {
                    await safeSendMessage(chatId, "Unknown command. Try /help to see available commands.", {});
                }
        }
    }
}); // End bot.on('message')

// --- Callback Query Handler ---
bot.on('callback_query', async (callbackQuery) => {
    const msg = callbackQuery.message; // The message the button was attached to
    if (!msg) { // If the original message is somehow gone
        bot.answerCallbackQuery(callbackQuery.id, { text: "Action expired or original message not found." }).catch(() => {});
        console.warn(`[CBQ_WARN] Callback query received without associated message. ID: ${callbackQuery.id}, Data: ${callbackQuery.data}`);
        return;
    }
    const userId = String(callbackQuery.from.id);
    const chatId = String(msg.chat.id);
    const data = callbackQuery.data; // e.g., "join_game:game_123"
    const originalMessageId = msg.message_id;

    console.log(`[CBQ RCV] Chat: ${chatId}, User: ${userId} (@${callbackQuery.from.username || 'N/A'}), Data: "${data}", OriginalMsgID: ${originalMessageId}`);

    // Answer callback query immediately to remove "loading" state on button
    bot.answerCallbackQuery(callbackQuery.id).catch((err) => {
         console.error(`[CBQ_ERROR] Failed to answer callback query ${callbackQuery.id}: ${err.message}`);
    });

    // Ensure user exists in our in-memory system
    await getUser(userId, callbackQuery.from.username);

    const [action, ...params] = data.split(':'); // Basic parsing: action:param1:param2...

    try {
        switch (action) {
            case 'join_game':
                if (!params[0]) throw new Error("Missing gameId for join_game action.");
                await handleJoinGameCallback(chatId, callbackQuery.from, params[0], originalMessageId);
                break;
            case 'cancel_game':
                 if (!params[0]) throw new Error("Missing gameId for cancel_game action.");
                await handleCancelGameCallback(chatId, callbackQuery.from, params[0], originalMessageId);
                break;
            case 'rps_choose':
                if (params.length < 2) throw new Error("Missing parameters for rps_choose action (expected gameId:choice).");
                await handleRPSChoiceCallback(chatId, callbackQuery.from, params[0], params[1], originalMessageId);
                break;
            case 'de_roll_prompt': // For Dice Escalator
                if (!params[0]) throw new Error("Missing gameId for de_roll_prompt action.");
                await handleDiceEscalatorPlayerAction(params[0], userId, 'roll_prompt', originalMessageId, String(chatId));
                break;
            case 'de_cashout': // For Dice Escalator
                if (!params[0]) throw new Error("Missing gameId for de_cashout action.");
                await handleDiceEscalatorPlayerAction(params[0], userId, 'cashout', originalMessageId, String(chatId));
                break;
            default:
                console.log(`[CBQ_INFO] Unknown callback query action received: ${action}`);
                // Optionally, notify user if action is truly unknown or unhandled
                // await safeSendMessage(userId, "Sorry, I didn't understand that button press or it has expired.", {});
        }
    } catch (error) {
        console.error(`[CBQ_ERROR] Error processing callback query "${data}" for user ${userId} in chat ${chatId}:`, error);
        // Notify the user an error occurred
        await safeSendMessage(userId, "Sorry, an error occurred while processing your action. Please try again or contact an admin if it persists.", {}).catch();
    }
}); // End bot.on('callback_query')

// --- Command Handler Functions ---
async function handleHelpCommand(chatId, userObject) {
    const userMention = createUserMention(userObject);
    // Dice Escalator help text updated to reflect automatic roll via DB service
    const helpTextParts = [
        `👋 Hello ${userMention}! Welcome to the Group Casino Bot v${BOT_VERSION}.`,
        `This is a simplified bot for playing games in group chats.`,
        `\n*Available commands:*`,
        `▫️ \`/help\` - Shows this help message.`,
        `▫️ \`/balance\` or \`/bal\` - Check your current game credits.`,
        `▫️ \`/startcoinflip <bet_amount>\` - Starts a Coinflip game for one opponent. Bet: ${MIN_BET_AMOUNT}-${MAX_BET_AMOUNT}. Example: \`/startcoinflip 10\``,
        `▫️ \`/startrps <bet_amount>\` - Starts a Rock Paper Scissors game for one opponent. Bet: ${MIN_BET_AMOUNT}-${MAX_BET_AMOUNT}. Example: \`/startrps 5\``,
        `▫️ \`/startdiceescalator <bet_amount>\` - Play Dice Escalator against the Bot! Bet: ${MIN_BET_AMOUNT}-${MAX_BET_AMOUNT}. Example: \`/startdiceescalator 20\``,
        `\n*Game Notes:*`,
        `➡️ For Dice Escalator, after starting, click 'Request Dice Roll'. The roll is processed automatically by our dice service.`,
        `➡️ For Coinflip or RPS, click 'Join Game' when someone starts one!`,
        `\nHave fun and play responsibly!`
    ];
    await safeSendMessage(chatId, helpTextParts.filter(Boolean).join('\n'), { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

async function handleBalanceCommand(chatId, userObject) {
    const user = await getUser(String(userObject.id)); // Fetch latest in-memory user data
    await safeSendMessage(chatId, `${createUserMention(userObject)}, your current balance is: *${formatCurrency(user.balance)}*.`, { parse_mode: 'MarkdownV2' });
}
// index.js - Part 2 of 2

// --- Group Game Flow Functions (Coinflip, RPS - largely unchanged from original logic) ---
async function handleStartGroupCoinFlipCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    // Fetch chat info to get title if needed, instead of relying on a 'msg' object here
    let chatInfo = null;
    try {
        chatInfo = await bot.getChat(chatId);
    } catch (e) {
        console.warn(`[COINFLIP_START_WARN] Could not fetch chat info for chat ${chatId}: ${e.message}`);
    }
    const chatTitle = chatInfo?.title; // Use optional chaining
    const gameSession = await getGroupSession(chatId, chatTitle || "Group Chat"); // Fallback title
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active in this chat: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}* (ID: \`${gameSession.currentGameId}\`). Please wait for it to finish.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance (${escapeMarkdownV2(formatCurrency(initiator.balance))}) is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_coinflip_init:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        console.error(`[COINFLIP_START_ERR] Failed to deduct balance for initiator ${initiatorId} (Reason: ${balanceUpdateResult.error})`);
        await safeSendMessage(chatId, `Error starting game: Could not place your bet. (${balanceUpdateResult.error})`, {});
        return;
    }

    const gameDataCF = {
        type: 'coinflip', gameId, chatId: String(chatId), initiatorId,
        initiatorMention: createUserMention(initiatorUser), betAmount,
        participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser), betPlaced: true }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
    };
    activeGames.set(gameId, gameDataCF);
    await updateGroupGameDetails(chatId, gameId, 'CoinFlip', betAmount);

    const joinMsgCF = `${createUserMention(initiatorUser)} has started a *Coin Flip Challenge* for ${escapeMarkdownV2(formatCurrency(betAmount))}!\nAn opponent is needed. Click "Join Game" to accept!`;
    const kbCF = { inline_keyboard: [[{ text: "🪙 Join Coinflip!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]] };

    const setupMsgCF = await safeSendMessage(chatId, joinMsgCF, { parse_mode: 'MarkdownV2', reply_markup: kbCF });
    if (setupMsgCF) {
        const gameToUpdate = activeGames.get(gameId);
        if (gameToUpdate) gameToUpdate.gameSetupMessageId = setupMsgCF.message_id;
    } else {
        console.error(`[COINFLIP_START_ERR] Failed to send setup message for game ${gameId}. Refunding bet for initiator ${initiatorId}.`);
        await updateUserBalance(initiatorId, betAmount, `refund_coinflip_setup_fail:${gameId}`, chatId); // Refund
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null); // Clear game from session
        return;
    }

    setTimeout(async () => {
        const gdCF = activeGames.get(gameId);
        if (gdCF && gdCF.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] Coinflip game ${gameId} in chat ${chatId} timed out waiting for an opponent.`);
            await updateUserBalance(gdCF.initiatorId, gdCF.betAmount, `refund_coinflip_timeout:${gameId}`, chatId);
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgTextCF = `The Coin Flip game (ID: \`${gameId}\`) started by ${gdCF.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdCF.betAmount))} has expired without an opponent. Bet refunded.`;
            if (gdCF.gameSetupMessageId) {
                bot.editMessageText(escapeMarkdownV2(timeoutMsgTextCF), { chatId: String(chatId), message_id: Number(gdCF.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                    .catch(e => {
                        console.warn(`[GAME_TIMEOUT_EDIT_ERR] Coinflip ${gameId}: ${e.message}. Sending new timeout message.`);
                        safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgTextCF), { parse_mode: 'MarkdownV2' });
                    });
            } else {
                safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgTextCF), { parse_mode: 'MarkdownV2' });
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleStartGroupRPSCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    let chatInfo = null;
    try {
        chatInfo = await bot.getChat(chatId);
    } catch (e) {
        console.warn(`[RPS_START_WARN] Could not fetch chat info for chat ${chatId}: ${e.message}`);
    }
    const chatTitle = chatInfo?.title;
    const gameSession = await getGroupSession(chatId, chatTitle || "Group Chat");
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}*. Please wait.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance (${escapeMarkdownV2(formatCurrency(initiator.balance))}) is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_rps_init:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        console.error(`[RPS_START_ERR] Failed to deduct balance for initiator ${initiatorId} (Reason: ${balanceUpdateResult.error})`);
        await safeSendMessage(chatId, `Error starting game: Could not place your bet. (${balanceUpdateResult.error})`, {});
        return;
    }

    const gameDataRPS = {
        type: 'rps', gameId, chatId: String(chatId), initiatorId, initiatorMention: createUserMention(initiatorUser),
        betAmount, participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser), betPlaced: true }],
        status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
    };
    activeGames.set(gameId, gameDataRPS);
    await updateGroupGameDetails(chatId, gameId, 'RockPaperScissors', betAmount);

    const joinMsgRPS = `${createUserMention(initiatorUser)} challenges someone to *Rock Paper Scissors* for ${escapeMarkdownV2(formatCurrency(betAmount))}!\nClick "Join Game" to play!`;
    const kbRPS = { inline_keyboard: [[{ text: "🪨📄✂️ Join RPS!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]] };

    const setupMsgRPS = await safeSendMessage(chatId, joinMsgRPS, { parse_mode: 'MarkdownV2', reply_markup: kbRPS });
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

    setTimeout(async () => {
        const gdRPS = activeGames.get(gameId);
        if (gdRPS && gdRPS.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] RPS game ${gameId} in chat ${chatId} timed out waiting for an opponent.`);
            await updateUserBalance(gdRPS.initiatorId, gdRPS.betAmount, `refund_rps_timeout:${gameId}`, chatId);
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgTextRPS = `The RPS game (ID: \`${gameId}\`) by ${gdRPS.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdRPS.betAmount))} expired. Bet refunded.`;
            if (gdRPS.gameSetupMessageId) {
                bot.editMessageText(escapeMarkdownV2(timeoutMsgTextRPS), { chatId: String(chatId), message_id: Number(gdRPS.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                    .catch(e => {
                        console.warn(`[GAME_TIMEOUT_EDIT_ERR] RPS ${gameId}: ${e.message}. Sending new timeout message.`);
                        safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgTextRPS), { parse_mode: 'MarkdownV2' });
                    });
            } else {
                safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgTextRPS), { parse_mode: 'MarkdownV2' });
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleJoinGameCallback(chatId, joinerUser, gameId, interactionMessageId) {
    const joinerId = String(joinerUser.id);
    const gameData = activeGames.get(gameId);

    if (!gameData) {
        await safeSendMessage(joinerId, "This game is no longer available or has expired.", {});
        if (interactionMessageId && chatId) bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        return;
    }
    if (gameData.chatId !== String(chatId)) {
        console.warn(`[JOIN_ERR] Game ${gameId} chat mismatch. Expected: ${gameData.chatId}, Got: ${chatId}`);
        await safeSendMessage(joinerId, "Error joining game (chat mismatch).", {});
        return;
    }
    if (gameData.initiatorId === joinerId) {
        await safeSendMessage(joinerId, "You cannot join a game you started.", {});
        return;
    }
    if (gameData.status !== 'waiting_opponent') {
        await safeSendMessage(joinerId, "This game is no longer waiting for an opponent.", {});
        if (interactionMessageId && chatId && gameData.status !== 'waiting_opponent') {
            bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        }
        return;
    }
    if (gameData.participants.length >= 2 && (gameData.type === 'coinflip' || gameData.type === 'rps')) {
        await safeSendMessage(joinerId, "Sorry, this game already has enough players.", {});
        bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        return;
    }

    const joiner = await getUser(joinerId);
    if (joiner.balance < gameData.betAmount) {
        await safeSendMessage(joinerId, `Your balance (${escapeMarkdownV2(formatCurrency(joiner.balance))}) is too low to join this ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} game.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const balanceUpdateResult = await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_group_${gameData.type}_join:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        console.error(`[JOIN_ERR] Failed to deduct balance for joiner ${joinerId}: ${balanceUpdateResult.error}`);
        await safeSendMessage(joinerId, `Error joining game: Could not place bet. (${balanceUpdateResult.error})`, {});
        return;
    }

    gameData.participants.push({ userId: joinerId, choice: null, mention: createUserMention(joinerUser), betPlaced: true });

    if (gameData.type === 'coinflip' && gameData.participants.length === 2) {
        gameData.status = 'playing';
        activeGames.set(gameId, gameData);
        const p1 = gameData.participants[0], p2 = gameData.participants[1];
        p1.choice = 'heads'; p2.choice = 'tails'; // Assign sides
        const cfResult = determineCoinFlipOutcome();
        let winner = (cfResult.outcome === p1.choice) ? p1 : p2;
        const winnings = gameData.betAmount * 2; // Total pot (winner gets their bet back + opponent's bet)
        await updateUserBalance(winner.userId, winnings, `won_group_coinflip:${gameId}`, chatId);
        const resMsg = `*CoinFlip Resolved!*\nBet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n${p1.mention} (Heads) vs ${p2.mention} (Tails)\n\nLanded on: *${escapeMarkdownV2(cfResult.outcomeString)} ${cfResult.emoji}*!\n\n🎉 ${winner.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;
        if (interactionMessageId) {
            bot.editMessageText(resMsg, { chatId: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                .catch(e => safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' }));
        } else {
            safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' });
        }
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    } else if (gameData.type === 'rps' && gameData.participants.length === 2) {
        gameData.status = 'waiting_choices';
        activeGames.set(gameId, gameData);
        const rpsPrompt = `${gameData.participants[0].mention} & ${gameData.participants[1].mention}, your RPS match for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} is set!\nEach player must click a button below to make their choice:`;
        const rpsKeyboard = { inline_keyboard: [[{ text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` }, { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` }, { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }]] };
        if (interactionMessageId) {
            bot.editMessageText(escapeMarkdownV2(rpsPrompt), { chatId: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard })
                .catch(e => safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt), { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }));
        } else {
            safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt), { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard });
        }
    } else {
        activeGames.set(gameId, gameData);
        console.log(`[JOIN_INFO] User ${joinerId} joined game ${gameId}. Participants: ${gameData.participants.length}`);
    }
}

async function handleCancelGameCallback(chatId, cancellerUser, gameId, interactionMessageId) {
    const cancellerId = String(cancellerUser.id);
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== String(chatId)) {
        await safeSendMessage(cancellerId, "Game unavailable for cancellation.", {});
        return;
    }
    if (gameData.initiatorId !== cancellerId) {
        await safeSendMessage(cancellerId, `Only the initiator (${gameData.initiatorMention}) can cancel this game.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const cancellableStatuses = ['waiting_opponent', 'waiting_choices'];
    if (!cancellableStatuses.includes(gameData.status)) {
        await safeSendMessage(cancellerId, `This game cannot be cancelled in its current state: ${escapeMarkdownV2(gameData.status)}.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    for (const participant of gameData.participants) {
        if (participant.betPlaced) {
            await updateUserBalance(participant.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, chatId);
        }
    }
    const gameTypeDisplay = gameData.type.charAt(0).toUpperCase() + gameData.type.slice(1);
    const cancellationMessage = `${gameData.initiatorMention} has cancelled the ${escapeMarkdownV2(gameTypeDisplay)} game (Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}). All bets have been refunded.`;
    if (interactionMessageId) {
        bot.editMessageText(cancellationMessage, { chatId: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
            .catch(e => safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' }));
    } else {
        safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
    }
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
    console.log(`[GAME_CANCEL] Game ${gameId} in chat ${chatId} cancelled by initiator ${cancellerId}.`);
}

async function handleRPSChoiceCallback(chatId, userObject, gameId, choice, interactionMessageId) {
    const userId = String(userObject.id);
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== String(chatId) || gameData.type !== 'rps') {
        await safeSendMessage(userId, "This RPS game isn't available or has ended.", {});
        return;
    }
    if (gameData.status !== 'waiting_choices') {
        await safeSendMessage(userId, "The game is not currently waiting for choices.", {});
        return;
    }
    const participant = gameData.participants.find(p => p.userId === userId);
    if (!participant) {
        await safeSendMessage(userId, "You are not playing in this RPS game.", {});
        return;
    }
    if (participant.choice) {
        await safeSendMessage(userId, `You have already chosen ${RPS_EMOJIS[participant.choice]}. Waiting for opponent.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const normalizedChoiceKey = String(choice).toUpperCase();
    if (!RPS_CHOICES[normalizedChoiceKey]) {
        console.error(`[RPS_CHOICE_ERR] Invalid choice '${choice}' from user ${userId} for game ${gameId}.`);
        await safeSendMessage(userId, `Invalid choice. Please click Rock, Paper, or Scissors.`, {});
        return;
    }
    participant.choice = RPS_CHOICES[normalizedChoiceKey];
    await safeSendMessage(userId, `You chose ${RPS_EMOJIS[participant.choice]}! Waiting for your opponent...`, { parse_mode: 'MarkdownV2' });
    console.log(`[RPS_CHOICE] User ${userId} chose ${participant.choice} for game ${gameId}`);
    activeGames.set(gameId, gameData); // Save the choice

    const otherPlayer = gameData.participants.find(p => p.userId !== userId);
    let groupUpdateMsg = `${participant.mention} has made their choice!`;
    let keyboardForUpdate = {}; // Default to empty (remove keyboard)

    if (otherPlayer && !otherPlayer.choice) {
        groupUpdateMsg += ` Waiting for ${otherPlayer.mention}...`;
        // Keep keyboard if opponent still needs to choose
        keyboardForUpdate = { inline_keyboard: [[{ text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` }, { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` }, { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }]] };
    }
    // Update the group message (e.g., the one with buttons or previous "joined" message)
    const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);
    if (messageToEditId) {
        bot.editMessageText(escapeMarkdownV2(groupUpdateMsg), { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: keyboardForUpdate })
            .catch(() => {}); // Ignore if edit fails (e.g., message deleted)
    }

    const allChosen = gameData.participants.length === 2 && gameData.participants.every(p => p.choice);
    if (allChosen) {
        gameData.status = 'game_over';
        activeGames.set(gameId, gameData); // Save status before async
        const p1 = gameData.participants[0], p2 = gameData.participants[1];
        const rpsRes = determineRPSOutcome(p1.choice, p2.choice);
        let winnerParticipant = null, resultText = `*Rock Paper Scissors Result!*\nBet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n${p1.mention}: ${RPS_EMOJIS[p1.choice]} vs ${p2.mention}: ${RPS_EMOJIS[p2.choice]}\n\n${escapeMarkdownV2(rpsRes.description)}\n\n`;

        if (rpsRes.result === 'win1') winnerParticipant = p1;
        else if (rpsRes.result === 'win2') winnerParticipant = p2;

        if (winnerParticipant) {
            const winnings = gameData.betAmount * 2;
            await updateUserBalance(winnerParticipant.userId, winnings, `won_rps:${gameId}`, chatId);
            resultText += `🎉 ${winnerParticipant.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;
        } else if (rpsRes.result === 'draw') {
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, chatId);
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, chatId);
            resultText += `It's a draw! Bets have been refunded.`;
        } else { // Error case from determineRPSOutcome
            console.error(`[RPS_RESOLVE_ERR] RPS determination error for game ${gameId}. Description: ${rpsRes.description}`);
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_error:${gameId}`, chatId);
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_error:${gameId}`, chatId);
            resultText = `An unexpected error occurred while determining the RPS winner. Bets have been refunded.`;
        }

        const finalMsgIdToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
        if (finalMsgIdToEdit) {
            bot.editMessageText(resultText, { chatId: String(chatId), message_id: finalMsgIdToEdit, parse_mode: 'MarkdownV2', reply_markup: {} })
                .catch(e => safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2' }));
        } else {
            safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2' });
        }
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
}

// --- Dice Escalator Game Functions (Using Database for Rolls) ---
async function handleStartDiceEscalatorCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    // Fetch chat title if available for getGroupSession
    let chatInfo = null; try { chatInfo = await bot.getChat(chatId); } catch(e) { console.warn(`Could not fetch chat info for DE start: ${e.message}`); }
    const gameSession = await getGroupSession(chatId, chatInfo?.title || "Dice Escalator Group");
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType||'Unknown')}*. Please wait.`, {parse_mode:'MarkdownV2'});
        return;
    }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet. You have ${escapeMarkdownV2(formatCurrency(initiator.balance))}.`, {parse_mode:'MarkdownV2'});
        return;
    }
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_dice_escalator:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
       await safeSendMessage(chatId, `Error starting Dice Escalator: ${balanceUpdateResult.error}`, {});
       return;
    }

    const gameData = {
        type: 'dice_escalator', gameId, chatId:String(chatId), initiatorId,
        initiatorMention:createUserMention(initiatorUser), betAmount, playerScore:0, botScore:0,
        status:'player_turn_prompt_action', currentPlayerId:initiatorId,
        bustValue:DICE_ESCALATOR_BUST_ON, creationTime:Date.now(), commandMessageId, gameSetupMessageId:null
    };
    activeGames.set(gameId, gameData);
    await updateGroupGameDetails(chatId, gameId, 'DiceEscalator', betAmount);

    const initialMsg = `${gameData.initiatorMention} has started *Dice Escalator* versus the Bot for ${escapeMarkdownV2(formatCurrency(betAmount))}!\n\nYour current score: *0*. If you roll a *${gameData.bustValue}*, you bust!`;
    const kb = {
        inline_keyboard:[
            [{text:"🎲 Request Dice Roll",callback_data:`de_roll_prompt:${gameId}`}],
            [{text:`💰 Cashout ${formatCurrency(0)}`,callback_data:`de_cashout:${gameId}`}] // Initial cashout is 0
        ]
    };
    const sentMsg = await safeSendMessage(chatId, initialMsg, {parse_mode:'MarkdownV2', reply_markup:kb});

    if(sentMsg){
        const gtu=activeGames.get(gameId); if(gtu) gtu.gameSetupMessageId=sentMsg.message_id;
    } else {
        console.error(`[DE_START_ERR] Failed to send initial Dice Escalator message for game ${gameId}. Refunding bet.`);
        await updateUserBalance(initiatorId,betAmount,`refund_de_setup_fail:${gameId}`,chatId);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId,null,null,null);
    }
}
// index.js - Part 2 of 2 (Continued)

async function handleDiceEscalatorPlayerAction(gameId, userId, actionType, interactionMessageId, chatId) {
    const gameData = activeGames.get(gameId);

    // Basic validation: game exists, correct type, correct chat, current player's turn
    if (!gameData || 
        gameData.chatId !== String(chatId) || 
        gameData.type !== 'dice_escalator' || 
        gameData.currentPlayerId !== String(userId)) {
        await safeSendMessage(userId, "Cannot perform this action now (game mismatch, not your turn, or game ended).", {});
        // Attempt to remove buttons if the original interaction message ID is known and the game is clearly not in a state for this user to act
        if (interactionMessageId && gameData && (gameData.currentPlayerId !== String(userId) || gameData.status !== 'player_turn_prompt_action')) {
             bot.editMessageReplyMarkup({}, {chat_id:String(chatId), message_id:Number(interactionMessageId)}).catch(()=>{});
        }
        return;
    }
    
    const msgIdToUpdate = Number(interactionMessageId) || Number(gameData.gameSetupMessageId);
    // For 'roll_prompt', we might send a new message if edit fails. For 'cashout', msgIdToUpdate is more critical.
    if (!msgIdToUpdate && actionType === 'cashout') { 
        console.error(`[DE_ACTION_ERR] No valid messageId for game ${gameId} to perform cashout action.`);
        await safeSendMessage(String(chatId),`Error updating game display for ${gameData.initiatorMention}. Action could not be fully processed.`,{parse_mode:'MarkdownV2'});
        return;
    }

    if (actionType === 'roll_prompt') {
        // Player can only request a roll if it's their turn to prompt for action
        if (gameData.status !== 'player_turn_prompt_action') {
            await safeSendMessage(userId, "You cannot request a roll at this time (invalid game state).", {});
            return;
        }

        gameData.status = 'waiting_db_roll'; // New status: waiting for roll from database
        activeGames.set(gameId, gameData); // Save updated status
        
        const promptMessageText = `${gameData.initiatorMention}, your roll is being processed by the dice service... please wait.`;
        
        // Try to edit the existing game message
        let messageUpdatedOrSent = false;
        if (msgIdToUpdate) {
            try {
                await bot.editMessageText(escapeMarkdownV2(promptMessageText), { 
                    chat_id: String(chatId), 
                    message_id: msgIdToUpdate, 
                    parse_mode: 'MarkdownV2', 
                    reply_markup: {} // Remove buttons while processing
                });
                console.log(`[DE_ACTION] Updated message for game ${gameId} (ID: ${msgIdToUpdate}) to show DB roll processing.`);
                messageUpdatedOrSent = true;
            } catch (editError) {
                console.error(`[DE_ACTION_ERR] Failed to edit message ${msgIdToUpdate} for DB roll prompt (game ${gameId}):`, editError.message);
                // If edit fails, we will try to send a new message below
            }
        }
        
        if (!messageUpdatedOrSent) { // If msgIdToUpdate was invalid or edit failed
            const newSentMsg = await safeSendMessage(String(chatId), escapeMarkdownV2(promptMessageText), { parse_mode: 'MarkdownV2'});
            if (newSentMsg) { // If we sent a new message, update gameData with its ID for future edits
                gameData.gameSetupMessageId = newSentMsg.message_id; // Potentially problematic if original buttons were on an older message
                activeGames.set(gameId, gameData);
                console.log(`[DE_ACTION] Sent new message for game ${gameId} (ID: ${newSentMsg.message_id}) to show DB roll processing.`);
            } else {
                console.error(`[DE_ACTION_ERR] Failed to send new prompt message for game ${gameId} after edit failure.`);
                // Revert status if we can't even inform the user
                gameData.status = 'player_turn_prompt_action';
                activeGames.set(gameId, gameData);
                await safeSendMessage(String(chatId), "An error occurred trying to request your roll. Please try again.", {});
                return;
            }
        }

        try {
            console.log(`[DB_ROLL_REQUEST] Inserting roll request for game ${gameId}, user ${userId}, chat ${chatId}`);
            // Insert request into the database, use ON CONFLICT to handle retries for the same game_id
            await queryDatabase(
                'INSERT INTO dice_roll_requests (game_id, chat_id, user_id, status, requested_at) VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (game_id) DO UPDATE SET status = EXCLUDED.status, requested_at = EXCLUDED.requested_at, roll_value = NULL, processed_at = NULL',
                [gameId, String(chatId), userId, 'pending']
            );
            console.log(`[DB_ROLL_REQUEST] Successfully inserted/updated roll request for game ${gameId}.`);

            // Start polling the database for the result
            let attempts = 0;
            const maxAttempts = 30; // Poll for 30 * 2s = 60 seconds max
            const pollInterval = 2000; // Poll every 2 seconds

            const pollForDbResult = async () => {
                const currentGameData = activeGames.get(gameId); // Get fresh game data each poll
                
                // Stop polling if game ended, or status changed, or user is no longer current player
                if (!currentGameData || currentGameData.status !== 'waiting_db_roll' || currentGameData.initiatorId !== userId) {
                    console.log(`[DB_ROLL_POLL_CANCEL] Game ${gameId} no longer active or status changed from 'waiting_db_roll' for user ${userId}. Stopping poll.`);
                    return;
                }

                if (attempts >= maxAttempts) {
                    console.error(`[DB_ROLL_TIMEOUT] Timed out waiting for roll from DB for game ${gameId}`);
                    await safeSendMessage(String(chatId), "Sorry, there was a timeout getting your roll from the dice service. Please try prompting again.", {});
                    currentGameData.status = 'player_turn_prompt_action'; 
                    activeGames.set(gameId, currentGameData);
                    // Try to restore buttons on the game message
                    const messageIdToRestoreButtons = Number(currentGameData.gameSetupMessageId || msgIdToUpdate);
                    if (messageIdToRestoreButtons) {
                        const kb = {inline_keyboard:[[{text:"🎲 Request Roll Again",callback_data:`de_roll_prompt:${gameId}`}], [{text:`💰 Cashout ${formatCurrency(currentGameData.playerScore)}`,callback_data:`de_cashout:${gameId}`}]]};
                        await bot.editMessageText(escapeMarkdownV2(`${currentGameData.initiatorMention}, roll timed out. Current score: ${currentGameData.playerScore}. Choose an action.`), {chat_id: String(chatId), message_id: messageIdToRestoreButtons, parse_mode: 'MarkdownV2', reply_markup: kb}).catch((e)=>{console.error(`[DB_ROLL_TIMEOUT] Failed to edit message ${messageIdToRestoreButtons} to restore buttons: ${e.message}`)});
                    }
                    return;
                }
                attempts++;

                try {
                    const result = await queryDatabase('SELECT roll_value, status FROM dice_roll_requests WHERE game_id = $1', [gameId]);
                    
                    if (result.rows.length > 0 && result.rows[0].status === 'completed' && result.rows[0].roll_value !== null) {
                        const rollValue = result.rows[0].roll_value;
                        console.log(`[DB_ROLL_RESULT] Received roll ${rollValue} for game ${gameId} from DB.`);
                        
                        // Clean up the processed request from the database
                        await queryDatabase('DELETE FROM dice_roll_requests WHERE game_id = $1', [gameId]);
                        
                        await processDiceEscalatorPlayerRoll(currentGameData, rollValue); // Pass current gameData
                    } else if (result.rows.length > 0 && result.rows[0].status === 'error') {
                        console.error(`[DB_ROLL_ERROR] Dice service reported an error for game ${gameId} in DB.`);
                        await safeSendMessage(String(chatId), "The dice service encountered an error processing your roll. Please try again.", {});
                        currentGameData.status = 'player_turn_prompt_action'; activeGames.set(gameId, currentGameData);
                        await queryDatabase('DELETE FROM dice_roll_requests WHERE game_id = $1', [gameId]); // Clean up error entry
                        const messageIdToRestoreButtons = Number(currentGameData.gameSetupMessageId || msgIdToUpdate);
                        if (messageIdToRestoreButtons) {
                            const kb = {inline_keyboard:[[{text:"🎲 Request Roll Again",callback_data:`de_roll_prompt:${gameId}`}], [{text:`💰 Cashout ${formatCurrency(currentGameData.playerScore)}`,callback_data:`de_cashout:${gameId}`}]]};
                            await bot.editMessageText(escapeMarkdownV2(`${currentGameData.initiatorMention}, an error occurred with the roll. Current score: ${currentGameData.playerScore}. Choose action.`), {chat_id: String(chatId), message_id: messageIdToRestoreButtons, parse_mode: 'MarkdownV2', reply_markup: kb}).catch((e)=>{console.error(`[DB_ROLL_ERROR] Failed to edit message ${messageIdToRestoreButtons} to restore buttons: ${e.message}`)});
                        }
                    } else {
                        console.log(`[DB_ROLL_POLL] Roll for game ${gameId} status in DB: '${result.rows[0]?.status || 'not_found_or_pending'}', attempt ${attempts}`);
                        setTimeout(pollForDbResult, pollInterval); // Poll again
                    }
                } catch (dbError) {
                    console.error(`[DB_ROLL_POLL_ERR] Error during DB poll for game ${gameId}:`, dbError.message);
                    await safeSendMessage(String(chatId), "An error occurred while checking for your roll. Please try again.", {});
                    currentGameData.status = 'player_turn_prompt_action'; activeGames.set(gameId, currentGameData);
                    const messageIdToRestoreButtons = Number(currentGameData.gameSetupMessageId || msgIdToUpdate);
                     if (messageIdToRestoreButtons) {
                        const kb = {inline_keyboard:[[{text:"🎲 Request Roll Again",callback_data:`de_roll_prompt:${gameId}`}], [{text:`💰 Cashout ${formatCurrency(currentGameData.playerScore)}`,callback_data:`de_cashout:${gameId}`}]]};
                        await bot.editMessageText(escapeMarkdownV2(`${currentGameData.initiatorMention}, error fetching roll. Current score: ${currentGameData.playerScore}. Choose action.`), {chat_id: String(chatId), message_id: messageIdToRestoreButtons, parse_mode: 'MarkdownV2', reply_markup: kb}).catch((e)=>{console.error(`[DB_ROLL_POLL_ERR] Failed to edit message ${messageIdToRestoreButtons} to restore buttons: ${e.message}`)});
                     }
                }
            };
            setTimeout(pollForDbResult, pollInterval); // Start the first database poll

        } catch (error) {
            console.error(`[DB_ROLL_REQUEST_ERR] Failed to insert roll request into DB for game ${gameId}:`, error.message);
            await safeSendMessage(String(chatId), "There was an error requesting your roll from the dice service. Please try again.", {});
            gameData.status = 'player_turn_prompt_action'; // Reset status
            activeGames.set(gameId, gameData);
            // Try to restore buttons on the game message if possible
             const messageIdToRestoreButtons = Number(gameData.gameSetupMessageId || msgIdToUpdate);
             if (messageIdToRestoreButtons) {
                const kb = {inline_keyboard:[[{text:"🎲 Request Roll Again",callback_data:`de_roll_prompt:${gameId}`}], [{text:`💰 Cashout ${formatCurrency(gameData.playerScore)}`,callback_data:`de_cashout:${gameId}`}]]};
                await bot.editMessageText(escapeMarkdownV2(`${gameData.initiatorMention}, error requesting roll. Current score: ${gameData.playerScore}. Choose action.`), {chat_id: String(chatId), message_id: messageIdToRestoreButtons, parse_mode: 'MarkdownV2', reply_markup: kb}).catch((e)=>{console.error(`[DB_ROLL_REQUEST_ERR] Failed to edit message ${messageIdToRestoreButtons} to restore buttons: ${e.message}`)});
             }
        }

    } else if (actionType === 'cashout') {
        if (gameData.status !== 'player_turn_prompt_action') { // Can only cashout when it's their turn to make a choice
            await safeSendMessage(userId, "You can only cash out when it's your turn to roll or cash out.", {});
            return;
        }
        if (gameData.playerScore <= 0) {
            await safeSendMessage(userId, "You cannot cash out with a score of 0.", {});
            return; 
        }
        const score = gameData.playerScore; 
        const totalReturn = gameData.betAmount + score; // Bet back + winnings
        await updateUserBalance(userId, totalReturn, `cashout_de_player:${gameId}`, chatId); // This updates in-memory balance
        
        gameData.status = 'player_cashed_out'; 
        activeGames.set(gameId, gameData); 
        
        let msgText = `${gameData.initiatorMention} cashed out with a score of *${escapeMarkdownV2(String(score))}*!\nTotal credited: ${escapeMarkdownV2(formatCurrency(totalReturn))}.`;
        msgText += `\n\n🤖 Now it's the Bot's turn. Target: Beat *${escapeMarkdownV2(String(score))}*...`;
        
        // Ensure msgIdToUpdate is valid for cashout message edit
        const currentMsgIdForCashout = Number(gameData.gameSetupMessageId || interactionMessageId);
        if(currentMsgIdForCashout){
            try {
                await bot.editMessageText(msgText, {chat_id:String(chatId),message_id:currentMsgIdForCashout,parse_mode:'MarkdownV2',reply_markup:{}});
            } catch (error) { 
                console.error(`[DE_ACTION_ERR] Failed to edit message ${currentMsgIdForCashout} for cashout game ${gameId}: ${error.message}`);
                await safeSendMessage(String(chatId), msgText, {parse_mode:'MarkdownV2'}); // Send new if edit fails
            }
            await sleep(2000); 
            await processDiceEscalatorBotTurn(gameData, currentMsgIdForCashout);
        } else {
            console.error(`[DE_ACTION_ERR] No valid message ID to update for cashout in game ${gameId}.`);
            await safeSendMessage(String(chatId), msgText + "\n(Error updating game display)", {parse_mode:'MarkdownV2'});
             // Bot turn still needs to happen, but display might be off. This is tricky.
             // For now, we might just end the game if display cannot be updated.
             activeGames.delete(gameId);
             await updateGroupGameDetails(chatId, null, null, null);
             await safeSendMessage(String(chatId), `Game ${gameId} ended due to display update issue after cashout.`, {});
        }
    }
}

async function processDiceEscalatorPlayerRoll(gameData, playerRoll) {
    console.log(`[ROLL_PROCESS_START] Game ${gameData.gameId}`, { status: gameData.status, currentPlayer: gameData.currentPlayerId, rollValue: playerRoll });

    // Validate roll (should be done by helper service too, but good to double check)
    if (typeof playerRoll !== 'number' || !Number.isInteger(playerRoll) || playerRoll < 1 || playerRoll > 6) {
        console.error(`[ROLL_INVALID] Invalid roll value received in processDiceEscalatorPlayerRoll: ${playerRoll} for game ${gameData.gameId}.`);
        // Attempt to notify player and reset state
        await safeSendMessage(gameData.currentPlayerId || gameData.initiatorId, `An invalid roll value (${playerRoll}) was processed from the dice service. Please try requesting a roll again.`, {});
        gameData.status = 'player_turn_prompt_action'; 
        activeGames.set(gameData.gameId, gameData);
        console.log(`[GAME_STATE_RESET] Game ${gameData.gameId} status reset to 'player_turn_prompt_action' due to invalid roll value.`);
        // Try to update the game message with options again
        const msgIdToUpdate = Number(gameData.gameSetupMessageId);
        if (msgIdToUpdate) {
            const kb = {inline_keyboard:[[{text:`🎲 Request Roll Again (Score: ${gameData.playerScore})`,callback_data:`de_roll_prompt:${gameData.gameId}`}], [{text:`💰 Cashout ${escapeMarkdownV2(formatCurrency(gameData.playerScore))}`,callback_data:`de_cashout:${gameData.gameId}`}]]};
            await bot.editMessageText(escapeMarkdownV2(`${gameData.initiatorMention}, invalid roll processed. Score: ${gameData.playerScore}. Choose action.`), {chat_id: String(gameData.chatId), message_id: msgIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: kb}).catch(()=>{});
        }
        return;
    }

    const { gameId, chatId, initiatorMention, betAmount, bustValue, playerScore } = gameData;
    let newScore = playerScore;
    const msgId = Number(gameData.gameSetupMessageId); // Use the stored game setup message ID for updates

    if (!msgId) { 
        console.error(`[DE_PLAYER_ROLL_ERR] No gameSetupMessageId stored for game ${gameId}. Cannot update display consistently.`);
        // If we can't update the display, the game experience is broken.
        // We might need to send a new message and potentially end the game or refund.
        await safeSendMessage(String(chatId), `${initiatorMention}, your roll was ${playerRoll}. Error updating game display.`, {parse_mode:'MarkdownV2'});
        // For now, we'll proceed with logic but display might be off.
    }

    let turnResMsg = `${initiatorMention}, your roll from the dice service: ${formatDiceRolls([playerRoll])}!\n\n`;

    if (playerRoll === bustValue) {
        gameData.status = 'game_over_player_bust';
        turnResMsg += `💥 *BUSTED* by rolling a ${bustValue}! You lost your ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`;
        activeGames.delete(gameId); 
        await updateGroupGameDetails(chatId, null, null, null); // Clear from group session

        if (msgId) {
            try { await bot.editMessageText(turnResMsg, {chatId:String(chatId),message_id:msgId,parse_mode:'MarkdownV2',reply_markup:{}}); }
            catch (e) { await safeSendMessage(String(chatId), turnResMsg, {parse_mode:'MarkdownV2'}); }
        } else {
            await safeSendMessage(String(chatId), turnResMsg, {parse_mode:'MarkdownV2'});
        }
    } else {
        newScore += playerRoll; 
        gameData.playerScore = newScore; 
        gameData.status = 'player_turn_prompt_action'; // Back to player to decide next move
        
        turnResMsg += `Your score is now *${escapeMarkdownV2(String(newScore))}*.\nPotential winnings (if cashed out now): *${escapeMarkdownV2(formatCurrency(newScore))}*.`;
        turnResMsg += `\n\nRoll a *${bustValue}* next time and you bust! What's your next move?`;
        
        const kb = {
            inline_keyboard:[
                [{text:`🎲 Request Roll (Current Score: ${formatCurrency(newScore)})`,callback_data:`de_roll_prompt:${gameId}`}],
                [{text:`💰 Cashout ${escapeMarkdownV2(formatCurrency(newScore))}`,callback_data:`de_cashout:${gameId}`}]
            ]
        };
        
        if (msgId) {
            try { await bot.editMessageText(turnResMsg, {chatId:String(chatId),message_id:msgId,parse_mode:'MarkdownV2',reply_markup:kb}); }
            catch (e) { await safeSendMessage(String(chatId), turnResMsg, {parse_mode:'MarkdownV2', reply_markup:kb}); }
        } else {
             await safeSendMessage(String(chatId), turnResMsg, {parse_mode:'MarkdownV2', reply_markup:kb});
        }
        activeGames.set(gameId, gameData); // Save updated game state
    }
    console.log(`[ROLL_PROCESS_COMPLETE] Game ${gameData.gameId}`, { newStatus: gameData.status, playerScore: gameData.playerScore });
}

async function processDiceEscalatorBotTurn(gameData, messageIdToUpdate) {
    const { gameId, chatId, initiatorMention, betAmount, playerScore: playerCashedScore, bustValue } = gameData;
    let botScore = 0;
    let botBusted = false;
    let rolls = 0;
    let botPlaysMsg = `${initiatorMention} cashed out with a score of *${escapeMarkdownV2(String(playerCashedScore))}*.\n\n🤖 Bot's turn (Target: Beat ${playerCashedScore}):\n`;
    
    const msgId = Number(messageIdToUpdate); // Use the passed messageIdToUpdate
    if (!msgId) { 
        console.error(`[DE_BOT_TURN_ERR] Invalid or missing messageIdToUpdate (${messageIdToUpdate}) for game ${gameId}. Aborting bot turn.`);
        activeGames.delete(gameId); 
        await updateGroupGameDetails(chatId,null,null,null); 
        await safeSendMessage(String(chatId), `A display error occurred during the bot's turn for game ${gameId}. The game has ended.`, {});
        return; 
    }

    try { 
        await bot.editMessageText(escapeMarkdownV2(botPlaysMsg) + "\n_Bot thinking..._", {chatId:String(chatId), message_id:msgId,parse_mode:'MarkdownV2',reply_markup:{}}); 
    } catch(e) { 
        console.error(`[DE_BOT_TURN_EDIT_ERR] Initial edit for bot turn game ${gameId}: ${e.message}`);
        // If initial edit fails, we might just send a new message and proceed, but display could be confusing
        await safeSendMessage(String(chatId), escapeMarkdownV2(botPlaysMsg) + "\n_Bot thinking... (Display Update Error)_", {parse_mode:'MarkdownV2'}); 
    }
    await sleep(2000);

    while(rolls < DICE_ESCALATOR_BOT_ROLLS && botScore <= playerCashedScore && !botBusted){
        rolls++; 
        const botRollRes = determineDieRollOutcome(); // Bot uses its internal roll
        botPlaysMsg += `\nBot roll ${rolls}: ${botRollRes.emoji} (${botRollRes.roll})`;
        
        if(botRollRes.roll === bustValue){ 
            botBusted=true; botScore=0; botPlaysMsg += `\n💥 Bot BUSTED!`; break; 
        }
        botScore += botRollRes.roll; 
        botPlaysMsg += `\nBot score: *${botScore}*`;
        
        let nextPrompt = (botScore <= playerCashedScore && rolls < DICE_ESCALATOR_BOT_ROLLS && !botBusted) ? "\n\n_Bot rolls again..._" : "";
        try { 
            await bot.editMessageText(escapeMarkdownV2(botPlaysMsg) + nextPrompt, {chatId:String(chatId),message_id:msgId,parse_mode:'MarkdownV2',reply_markup:{}}); 
        } catch(e) { 
            console.error(`[DE_BOT_TURN_EDIT_ERR] Edit during bot roll ${rolls} for game ${gameId}: ${e.message}`); 
            // Continue bot logic even if intermediate display update fails
        }
        if(nextPrompt) await sleep(2500);
    }
    await sleep(1500); // Pause before final result

    let finalMsg = botPlaysMsg + "\n\n--- *Game Over* ---"; 
    gameData.status='game_over_bot_played'; gameData.botScore=botScore;
    
    if(botBusted || botScore <= playerCashedScore) {
        finalMsg += `\n🎉 ${initiatorMention}, the Bot ${botBusted?"Busted":`only reached *${escapeMarkdownV2(String(botScore))}*`}! It didn't beat your cashed-out score of *${escapeMarkdownV2(String(playerCashedScore))}*.`;
        finalMsg += `\nYou keep your winnings!`;
    } else {
        finalMsg += `\n😢 ${initiatorMention}, the Bot beat your score, reaching *${escapeMarkdownV2(String(botScore))}*!`;
        finalMsg += `\nSince you already cashed out your winnings, the House wins this round conceptually.`;
    }
    finalMsg += `\n\nFinal Scores -> You (Cashed Out): *${escapeMarkdownV2(String(playerCashedScore))}* | Bot: *${escapeMarkdownV2(String(botScore))}*${botBusted?" (Busted)":""}.`;

    try { 
        await bot.editMessageText(finalMsg, {chatId:String(chatId),message_id:msgId,parse_mode:'MarkdownV2',reply_markup:{}}); 
    } catch(e) { 
        console.error(`[DE_BOT_TURN_EDIT_ERR] Final edit for game ${gameId}: ${e.message}`);
        await safeSendMessage(String(chatId), finalMsg, {parse_mode:'MarkdownV2'}); 
    }
    
    activeGames.delete(gameId); 
    await updateGroupGameDetails(chatId,null,null,null);
    console.log(`[DE_BOT_TURN] Completed Dice Escalator game ${gameId}. Player Cashed Score: ${playerCashedScore}, Bot Score: ${botScore}, Bot Busted: ${botBusted}`);
}
console.log("Part 5: Message & Callback Handling, Basic Game Flow - Complete.");


//---------------------------------------------------------------------------
// index.js - Part 6: Startup, Shutdown, and Basic Error Handling
//---------------------------------------------------------------------------
console.log("Loading Part 6: Startup, Shutdown, and Basic Error Handling...");

// --- Database Initialization Function (from your provided code, with dice_roll_requests added) ---
async function initializeDatabase() {
    console.log('⚙️ [DB Init] Initializing Database Schema...');
    let client = null; // Use PoolClient for transactions
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        console.log('⚙️ [DB Init] Transaction started.');

        // Wallets Table (from your provided schema)
        console.log('⚙️ [DB Init] Ensuring wallets table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id VARCHAR(255) PRIMARY KEY, external_withdrawal_address VARCHAR(44), linked_at TIMESTAMPTZ,
                last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), referral_code VARCHAR(12) UNIQUE,
                referred_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
                referral_count INTEGER NOT NULL DEFAULT 0, total_wagered BIGINT NOT NULL DEFAULT 0,
                last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0,
                last_bet_amounts JSONB DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        console.log('⚙️ [DB Init] Ensuring wallets indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referred_by ON wallets (referred_by_user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_total_wagered ON wallets (total_wagered DESC);');

        // User Balances Table
        console.log('⚙️ [DB Init] Ensuring user_balances table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
                balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        
        // Deposit Addresses Table
        console.log('⚙️ [DB Init] Ensuring deposit_addresses table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposit_addresses (
                id SERIAL PRIMARY KEY, user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address VARCHAR(44) NOT NULL UNIQUE, derivation_path VARCHAR(255) NOT NULL,
                status VARCHAR(30) NOT NULL DEFAULT 'pending', expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), last_checked_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring deposit_addresses indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_user_id ON deposit_addresses (user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_expires ON deposit_addresses (status, expires_at);');
        // ... (other deposit_addresses indexes from your code)

        // Deposits Table
        console.log('⚙️ [DB Init] Ensuring deposits table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposits (
                id SERIAL PRIMARY KEY, user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address_id INTEGER REFERENCES deposit_addresses(id) ON DELETE SET NULL,
                tx_signature VARCHAR(88) NOT NULL UNIQUE, amount_lamports BIGINT NOT NULL CHECK (amount_lamports > 0),
                status VARCHAR(20) NOT NULL DEFAULT 'confirmed', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        console.log('⚙️ [DB Init] Ensuring deposits indexes...');
        // ... (deposit indexes from your code)

        // Bets Table
        console.log('⚙️ [DB Init] Ensuring bets table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY, user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                chat_id VARCHAR(255) NOT NULL, game_type VARCHAR(50) NOT NULL, bet_details JSONB,
                wager_amount_lamports BIGINT NOT NULL CHECK (wager_amount_lamports > 0),
                payout_amount_lamports BIGINT, status VARCHAR(30) NOT NULL DEFAULT 'active',
                priority INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring bets indexes...');
        // ... (bets indexes from your code)

        // Withdrawals Table
        console.log('⚙️ [DB Init] Ensuring withdrawals table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY, user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                requested_amount_lamports BIGINT NOT NULL, fee_lamports BIGINT NOT NULL DEFAULT 0,
                final_send_amount_lamports BIGINT NOT NULL, recipient_address VARCHAR(44) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending', payout_tx_signature VARCHAR(88) UNIQUE,
                error_message TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ, completed_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring withdrawals indexes...');
        // ... (withdrawals indexes from your code)

        // Referral Payouts Table
        console.log('⚙️ [DB Init] Ensuring referral_payouts table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS referral_payouts (
                id SERIAL PRIMARY KEY, referrer_user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                referee_user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                payout_type VARCHAR(20) NOT NULL, payout_amount_lamports BIGINT NOT NULL,
                triggering_bet_id INTEGER REFERENCES bets(id) ON DELETE SET NULL,
                milestone_reached_lamports BIGINT, status VARCHAR(20) NOT NULL DEFAULT 'pending',
                payout_tx_signature VARCHAR(88) UNIQUE, error_message TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ, paid_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring referral_payouts indexes...');
        // ... (referral_payouts indexes from your code)

        // Ledger Table
        console.log('⚙️ [DB Init] Ensuring ledger table...');
        await client.query(`
             CREATE TABLE IF NOT EXISTS ledger (
                id BIGSERIAL PRIMARY KEY, user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                transaction_type VARCHAR(50) NOT NULL, amount_lamports BIGINT NOT NULL,
                balance_before BIGINT NOT NULL, balance_after BIGINT NOT NULL,
                related_bet_id INTEGER REFERENCES bets(id) ON DELETE SET NULL,
                related_deposit_id INTEGER REFERENCES deposits(id) ON DELETE SET NULL,
                related_withdrawal_id INTEGER REFERENCES withdrawals(id) ON DELETE SET NULL,
                related_ref_payout_id INTEGER REFERENCES referral_payouts(id) ON DELETE SET NULL,
                notes TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        console.log('⚙️ [DB Init] Ensuring ledger indexes...');
        // ... (ledger indexes from your code)
        
        // Jackpots Table
        console.log('⚙️ [DB Init] Ensuring jackpots table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS jackpots (
                game_key VARCHAR(50) PRIMARY KEY,
                current_amount_lamports BIGINT NOT NULL DEFAULT 0 CHECK (current_amount_lamports >= 0),
                last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Game Leaderboards Table
        console.log('⚙️ [DB Init] Ensuring game_leaderboards table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS game_leaderboards (
                id SERIAL PRIMARY KEY, game_key VARCHAR(50) NOT NULL,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                score_type VARCHAR(50) NOT NULL, period_type VARCHAR(20) NOT NULL,
                period_identifier VARCHAR(50) NOT NULL, score BIGINT NOT NULL,
                player_display_name VARCHAR(255), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (game_key, user_id, score_type, period_type, period_identifier)
            );
        `);
        console.log('⚙️ [DB Init] Ensuring game_leaderboards indexes...');
        // ... (leaderboard indexes from your code)

        // --- ADDED: dice_roll_requests table for Dice Escalator ---
        console.log('⚙️ [DB Init] Ensuring dice_roll_requests table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS dice_roll_requests (
                request_id SERIAL PRIMARY KEY,
                game_id VARCHAR(255) NOT NULL UNIQUE, -- Ensures one active roll request per game_id
                chat_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL, -- The user who initiated the roll prompt
                status VARCHAR(20) NOT NULL DEFAULT 'pending', -- e.g., 'pending', 'processing', 'completed', 'error'
                roll_value INTEGER, -- Will be NULL until processed
                requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring dice_roll_requests indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_status_requested_at ON dice_roll_requests (status, requested_at);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_game_id ON dice_roll_requests (game_id);'); // For main bot to query by game_id
        // --- END ADDED SECTION ---

        await client.query('COMMIT');
        console.log('✅ [DB Init] Database schema initialized/verified successfully.');
    } catch (err) {
        console.error('❌ CRITICAL DATABASE INITIALIZATION ERROR:', err);
        if (client) { try { await client.query('ROLLBACK'); console.log('⚙️ [DB Init] Transaction rolled back.'); } catch (rbErr) { console.error('Rollback failed:', rbErr); } }
        if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
            safeSendMessage(ADMIN_USER_ID, `🚨 CRITICAL DB INIT FAILED: ${escapeMarkdownV2(String(err.message || err))}. Bot cannot start. Check logs.`).catch(()=>{});
        }
        process.exit(2); // Critical error, exit with a specific code
    } finally {
        if (client) client.release(); // Release client back to pool
    }
}

async function runPeriodicBackgroundTasks() {
    console.log(`[BACKGROUND_TASK] [${new Date().toISOString()}] Running periodic background tasks...`);
    const now = Date.now();
    const GAME_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 5; // e.g., 5 minutes
    let cleanedGames = 0;
    for (const [gameId, gameData] of activeGames.entries()) {
        // Added 'waiting_db_roll' to the list of statuses for cleanup
        if (now - gameData.creationTime > GAME_CLEANUP_THRESHOLD_MS &&
           (gameData.status === 'waiting_opponent' || gameData.status === 'waiting_choices' || gameData.status === 'waiting_db_roll')) {
            
            console.warn(`[BACKGROUND_TASK] Cleaning stale game ${gameId} (${gameData.type}) in chat ${gameData.chatId}. Status: ${gameData.status}`);
            
            // Refund initiator if game is stale in specific states where bet was placed but game didn't conclude
            if (gameData.initiatorId && gameData.betAmount > 0 &&
               (gameData.status === 'waiting_opponent' || // Refund if no opponent joined
               (gameData.type==='dice_escalator' && gameData.status === 'waiting_db_roll' && gameData.playerScore === 0 ))) { // Refund DE if roll from DB never came back
                
                await updateUserBalance(gameData.initiatorId, gameData.betAmount, `refund_stale_${gameData.type}_db_timeout:${gameId}`, gameData.chatId);
                const staleMsg = `Game (ID: \`${gameId}\`) by ${gameData.initiatorMention} cleared due to inactivity (roll not processed). Bet refunded.`;
                if(gameData.gameSetupMessageId) {
                    bot.editMessageText(escapeMarkdownV2(staleMsg), {chat_id: String(gameData.chatId), message_id: Number(gameData.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup:{}}).catch(()=>{safeSendMessage(String(gameData.chatId), escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'})});
                } else {
                    safeSendMessage(String(gameData.chatId), escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'});
                }
                 // Also clean up from dice_roll_requests if it was a Dice Escalator game stuck waiting for DB
                 if (gameData.type === 'dice_escalator' && gameData.status === 'waiting_db_roll') {
                    try {
                        await queryDatabase("DELETE FROM dice_roll_requests WHERE game_id = $1 AND status = 'pending'", [gameId]);
                        console.log(`[BACKGROUND_TASK] Deleted stale 'pending' dice_roll_request for game ${gameId}.`);
                    } catch (dbErr) {
                        console.error(`[BACKGROUND_TASK_ERR] Failed to delete stale dice_roll_request for game ${gameId}:`, dbErr.message);
                    }
                }
            } else if (gameData.status === 'waiting_choices' && gameData.type ==='rps' && gameData.participants.length > 1) {
                 // Handle stale RPS where choices weren't made
                 console.log(`[BACKGROUND_TASK] Stale RPS game ${gameId} found in waiting_choices. Refunding players.`);
                 for (const p of gameData.participants) { if (p.betPlaced) { await updateUserBalance(p.userId, gameData.betAmount, `refund_stale_rps_nochoice:${gameId}`, gameData.chatId); } }
                 const staleMsg = `RPS Game (ID: \`${gameId}\`) by ${gameData.initiatorMention} cleared due to inactivity during choice phase. Bets refunded.`;
                 if(gameData.gameSetupMessageId) bot.editMessageText(escapeMarkdownV2(staleMsg), {chat_id: String(gameData.chatId), message_id: Number(gameData.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup:{}}).catch(()=>{safeSendMessage(String(gameData.chatId), escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'})});
                 else safeSendMessage(String(gameData.chatId), escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'});
            }

            activeGames.delete(gameId);
            const groupSession = await getGroupSession(gameData.chatId); // Ensure it uses the in-memory one
            if (groupSession && groupSession.currentGameId === gameId) await updateGroupGameDetails(gameData.chatId, null, null, null);
            cleanedGames++;
        }
    }
    if (cleanedGames > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedGames} stale game(s).`);

    const SESSION_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 20; 
    let cleanedSessions = 0;
    for (const [chatId, sessionData] of groupGameSessions.entries()) {
        if (!sessionData.currentGameId && sessionData.lastActivity instanceof Date && (now - sessionData.lastActivity.getTime()) > SESSION_CLEANUP_THRESHOLD_MS) {
            console.log(`[BACKGROUND_TASK] Cleaning inactive group session for chat ${chatId}.`); 
            groupGameSessions.delete(chatId); 
            cleanedSessions++;
        } else if (!sessionData.lastActivity && !sessionData.currentGameId) { // If lastActivity is somehow missing
             console.warn(`[BACKGROUND_TASK] Group session for chat ${chatId} missing lastActivity timestamp and no active game. Removing as potentially stale.`);
             groupGameSessions.delete(chatId);
             cleanedSessions++;
        }
    }
    if (cleanedSessions > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedSessions} inactive group session entries.`);
    console.log(`[BACKGROUND_TASK] Finished. Active games: ${activeGames.size}, Group sessions: ${groupGameSessions.size}.`);
}
// const backgroundTaskInterval = setInterval(runPeriodicBackgroundTasks, 15 * 60 * 1000); // Keep commented unless specifically needed

// --- Process-level Error Handling ---
process.on('uncaughtException', (error, origin) => {
    console.error(`\n🚨🚨 UNCAUGHT EXCEPTION AT: ${origin} 🚨🚨`, error);
    if(ADMIN_USER_ID) safeSendMessage(ADMIN_USER_ID, `🆘 UNCAUGHT EXCEPTION:\nOrigin: ${origin}\nError: ${error.message}`, {}).catch(()=>{});
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`\n🔥🔥 UNHANDLED REJECTION 🔥🔥`, reason, promise);
    if(ADMIN_USER_ID) safeSendMessage(ADMIN_USER_ID, `♨️ UNHANDLED REJECTION:\nReason: ${escapeMarkdownV2(String(reason instanceof Error ? reason.message : reason))}`, {parse_mode:'MarkdownV2'}).catch();
});

// --- Telegram Bot Library Specific Error Handling ---
if (bot) { // Ensure bot object exists
    bot.on('polling_error', (error) => {
        console.error(`\n🚫 POLLING ERROR 🚫 Code: ${error.code}`); 
        console.error(`Message: ${error.message}`); 
        console.error(error); // Log full error object
    });
    bot.on('error', (error) => { // General errors from the bot instance
        console.error('\n🔥 BOT GENERAL ERROR EVENT 🔥:', error);
    });
} else {
    console.error("!!! CRITICAL ERROR: 'bot' instance not defined when trying to attach general error handlers in Part 6 !!!");
}

// --- Shutdown Handling ---
let isShuttingDown = false;
async function shutdown(signal) {
    if (isShuttingDown) return; 
    isShuttingDown = true;
    console.log(`\n🚦 Received ${signal}. Shutting down Bot v${BOT_VERSION}...`);
    if (bot && bot.isPolling()) { 
        try { 
            await bot.stopPolling({cancel:true}); 
            console.log("Polling stopped."); 
        } catch(e){
            console.error("Error stopping polling:", e.message);
        } 
    }
    // if (backgroundTaskInterval) clearInterval(backgroundTaskInterval); // Clear if using setInterval
    if (pool) { // Gracefully close DB pool
        try {
            await pool.end();
            console.log("PostgreSQL pool has been closed.");
        } catch (e) {
            console.error("Error closing PostgreSQL pool:", e.message);
        }
    }
    if (ADMIN_USER_ID) { 
        await safeSendMessage(ADMIN_USER_ID, `ℹ️ Bot v${BOT_VERSION} shutting down (Signal: ${signal}).`).catch(()=>{}); 
    }
    console.log("✅ Shutdown complete. Exiting.");
    process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
}
process.on('SIGINT', () => shutdown('SIGINT')); 
process.on('SIGTERM', () => shutdown('SIGTERM'));

// --- Main Startup Function ---
async function main() {
    console.log(`\n🚀🚀🚀 Initializing Group Chat Casino Bot v${BOT_VERSION} 🚀🚀🚀`);
    console.log(`Timestamp: ${new Date().toISOString()}`);

    // Initialize Database Schema (creates tables if they don't exist)
    await initializeDatabase(); 
    console.log("Database initialization sequence completed.");

    // Helper bot ID variables are no longer used from environment for game logic,
    // as dice rolls are now mediated by the database.
    // DICES_HELPER_BOT_ID and DICES_HELPER_BOT_USERNAME are effectively deprecated for this approach.
    console.log("[INFO] Dice Escalator rolls are now handled via database, not direct Telegram helper bot messages.");

    try {
        const me = await bot.getMe();
        console.log(`✅ Successfully connected to Telegram! Bot Name: @${me.username}, Bot ID: ${me.id}`);
        if (ADMIN_USER_ID) {
            await safeSendMessage(ADMIN_USER_ID, `🎉 Bot v${BOT_VERSION} (DB Dice Roll Mode) started! Polling active. Host: ${process.env.HOSTNAME || 'local'}`, {parse_mode:'MarkdownV2'});
        }
        console.log(`\n🎉 Bot operational! Waiting for messages...`);
        // Run background tasks once shortly after startup, then they should be on an interval if uncommented
        setTimeout(runPeriodicBackgroundTasks, 15000); 
    } catch (error) {
        console.error("❌ CRITICAL STARTUP ERROR (getMe or DB Init earlier):", error);
        if (ADMIN_USER_ID && BOT_TOKEN) { // Attempt to notify admin even on critical failure
            try {
                const tempBot = new TelegramBot(BOT_TOKEN, {}); // Don't poll with temp bot
                await tempBot.sendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE v${BOT_VERSION}:\n${escapeMarkdownV2(error.message)}\nBot is exiting.`).catch(()=>{});
            } catch (tempBotError) { 
                console.error("Failed to create temporary bot for failure notification:", tempBotError); 
            }
        }
        // If DB init failed, process.exit(2) might have already been called.
        // If getMe failed, exit with code 1.
        if (process.exitCode === undefined || process.exitCode === 0) { // Check if not already exiting with specific code
            process.exit(1);
        }
    }
} // End main() function

// --- Final Execution ---
main().catch(error => {
    console.error("❌ MAIN ASYNC FUNCTION UNHANDLED ERROR:", error);
    process.exit(1); // Exit if main promise chain fails
});

console.log("End of index.js script. Bot startup process initiated.");
// --- END OF index.js ---
