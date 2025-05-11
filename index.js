// index.js - Part 1: Core Imports & Basic Setup
// --- VERSION: 1.0.0-group ---

// --- Core Imports ---
import 'dotenv/config'; // For managing environment variables (e.g., BOT_TOKEN)
import TelegramBot from 'node-telegram-bot-api';

// --- Environment Variable Check (Simplified) ---
const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_USER_ID = process.env.ADMIN_USER_ID; // Example: for simple admin actions

if (!BOT_TOKEN) {
    console.error("FATAL ERROR: BOT_TOKEN is not defined in your .env file or environment variables.");
    process.exit(1); // Exit if the bot token is missing
}
console.log("BOT_TOKEN loaded successfully.");
if (ADMIN_USER_ID) {
    console.log(`Admin User ID: ${ADMIN_USER_ID} loaded from environment.`);
} else {
    console.log("INFO: No ADMIN_USER_ID set (this is optional).");
}

// --- Basic Bot Initialization ---
// We'll use polling for simplicity in this initial version.
// Webhooks can be configured later if needed for production environments.
const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("Telegram Bot instance created and configured for polling.");

// --- Global Constants & Simple State (Examples) ---
const BOT_VERSION = '1.0.0-group'; // Matches package.json
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096; // Telegram API limit

/**
 * @type {Map<string, object>}
 * Tracks active games in different group chats.
 * - key: gameId (string, uniquely identifies a game instance)
 * - value: Object containing game details (e.g., type, chatId, initiatorId, participants, betAmount, status, creationTime)
 */
let activeGames = new Map();

/**
 * @type {Map<string, number>}
 * Tracks user command timestamps for simple cooldown/spam prevention.
 * - key: userId (string)
 * - value: lastCommandTimestamp (number, Date.now())
 */
let userCooldowns = new Map();

console.log(`Solana Group Chat Casino Bot v${BOT_VERSION} initializing...`);
console.log(`Current system time: ${new Date().toISOString()}`);


// --- Basic Utility Functions ---

/**
 * Escapes characters for Telegram MarkdownV2 parse mode.
 * This is a common requirement for formatting messages correctly.
 * Handles null/undefined inputs by returning an empty string.
 * Characters to escape: _ * [ ] ( ) ~ \` > # + - = | { } . !
 * @param {string | number | bigint | null | undefined} text Input text to escape.
 * @returns {string} Escaped text, safe for MarkdownV2.
 */
const escapeMarkdownV2 = (text) => {
    if (text === null || typeof text === 'undefined') {
        return '';
    }
    const textString = String(text);
    // Important: The backslash itself must be escaped first if it's part of the replacement,
    // but here we are adding a backslash before each special character.
    // The order of replacements in the regex usually doesn't matter for this approach.
    return textString.replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};

/**
 * Safely sends a Telegram message, handling potential errors and message length limits.
 * Ensures text is properly escaped if MarkdownV2 is used.
 * @param {string|number} chatId Target chat ID.
 * @param {string} text Message text. If options.parse_mode is MarkdownV2, this text should NOT be pre-escaped by the caller.
 * @param {TelegramBot.SendMessageOptions} [options={}] Additional Telegram SendMessageOptions (e.g., parse_mode, reply_markup).
 * @returns {Promise<TelegramBot.Message | undefined>} The sent message object or undefined on failure.
 */
async function safeSendMessage(chatId, text, options = {}) {
    if (!chatId || typeof text !== 'string') {
        console.error("[safeSendMessage] Invalid input: chatId or text missing/invalid.", { chatId, textPreview: String(text).substring(0, 50) });
        return undefined;
    }

    let messageToSend = text;
    let finalOptions = {...options}; // Clone options

    // If MarkdownV2 is used, escape the text here.
    // Callers should pass raw text if they specify MarkdownV2.
    if (finalOptions.parse_mode === 'MarkdownV2') {
        messageToSend = escapeMarkdownV2(text);
    }

    // Truncate if necessary
    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsis = "... (message truncated)";
        let escapedEllipsis = ellipsis;
        if (finalOptions.parse_mode === 'MarkdownV2') {
            escapedEllipsis = escapeMarkdownV2(ellipsis);
        }
        const truncateAt = MAX_MARKDOWN_V2_MESSAGE_LENGTH - escapedEllipsis.length;
        messageToSend = messageToSend.substring(0, truncateAt) + escapedEllipsis;
        console.warn(`[safeSendMessage] Message for chat ${chatId} was truncated to ${MAX_MARKDOWN_V2_MESSAGE_LENGTH} characters.`);
    }

    try {
        const sentMessage = await bot.sendMessage(chatId, messageToSend, finalOptions);
        return sentMessage;
    } catch (error) {
        console.error(`[safeSendMessage] Failed to send message to chat ${chatId}. Error Code: ${error.code || 'N/A'}, Message: ${error.message}`);
        if (error.response?.body) {
            console.error(`[safeSendMessage] Telegram API Response Body: ${JSON.stringify(error.response.body)}`);
            const errorCode = error.response.body.error_code;
            const description = error.response.body.description?.toLowerCase() || '';
            if (errorCode === 403 || description.includes('blocked by the user') || description.includes('user is deactivated') || description.includes('bot was kicked') || description.includes('chat not found')) {
                console.warn(`[safeSendMessage] Bot interaction issue with chat ${chatId}: ${description}. Further messages might fail.`);
                // Consider adding logic here to mark the chat as inactive to prevent further send attempts.
            } else if (errorCode === 400 && (description.includes('parse error') || description.includes('can\'t parse entities'))) {
                console.error(`[safeSendMessage] Telegram PARSE ERROR in chat ${chatId}. Original text (first 100 chars): "${String(text).substring(0,100)}". Sent text (first 100 chars): "${messageToSend.substring(0,100)}". Options: ${JSON.stringify(options)}`);
                // Consider sending a plain text fallback to the user or admin notification.
            }
        }
        return undefined; // Indicate failure
    }
}

console.log("Part 1: Core Imports & Basic Setup - Complete.");
// End of Part 1
// index.js - Part 2: Simulated Database Operations & Data Management
// --- VERSION: 1.0.0-group ---

// (Code from Part 1 is assumed to be above this)
// ...

console.log("Loading Part 2: Simulated Database Operations & Data Management...");

// --- In-Memory Data Stores (Simulating a Database) ---

/**
 * @typedef {Object} UserGroupStats
 * @property {number} gamesPlayed - Total games played in this group.
 * @property {number} totalWagered - Total amount wagered in this group.
 * @property {number} netWinLoss - Net win/loss amount in this group.
 */

/**
 * @typedef {Object} UserData
 * @property {string} userId - The user's Telegram ID.
 * @property {string} [username] - The user's Telegram username (if available).
 * @property {number} balance - User's current balance (e.g., game credits).
 * @property {Date | null} lastPlayed - Timestamp of the last game played.
 * @property {Map<string, UserGroupStats>} groupStats - Stats per group chat (key: chatId).
 * @property {boolean} [isNew] - Flag to indicate if the user was newly created in this session.
 */

/**
 * @type {Map<string, UserData>}
 * Simulates a 'users' table.
 * - key: Telegram User ID (string)
 */
const userDatabase = new Map();

/**
 * @typedef {Object} GroupGameSession
 * @property {string} chatId - Telegram Chat ID.
 * @property {string} [chatTitle] - Title of the group chat.
 * @property {string | null} currentGameId - Unique ID of the current game instance in this chat, or null.
 * @property {string | null} currentGameType - String identifying the type of game (e.g., 'coinflip_group'), or null.
 * @property {number | null} currentBetAmount - The agreed bet amount for the current game.
 * @property {Date} lastActivity - Timestamp of the last activity in this group's game session.
 */

/**
 * @type {Map<string, GroupGameSession>}
 * Simulates a 'group_sessions' or 'active_group_contexts' table.
 * - key: Telegram Chat ID (string)
 */
const groupGameSessions = new Map();

console.log("In-memory data stores (userDatabase, groupGameSessions) initialized.");

// --- Simulated Database Functions ---

/**
 * Gets or creates a user in our simulated database.
 * Populates with a default starting balance if new.
 * @param {string} userId The user's Telegram ID.
 * @param {string} [username] The user's Telegram username (optional, can be updated).
 * @returns {Promise<UserData>} The user object.
 */
async function getUser(userId, username) {
    // Ensure userId is a string
    const userIdStr = String(userId);

    if (!userDatabase.has(userIdStr)) {
        const newUser = {
            userId: userIdStr,
            username: username,
            balance: 1000, // Default starting balance (e.g., 1000 game credits)
            lastPlayed: null,
            groupStats: new Map(),
            isNew: true, // Mark as new for this session
        };
        userDatabase.set(userIdStr, newUser);
        console.log(`[DB_SIM] New user created: ${userIdStr} (@${username || 'N/A'}), Balance: ${newUser.balance}`);
        return { ...newUser }; // Return a copy
    }

    const user = userDatabase.get(userIdStr);
    // Update username if it has changed or was missing
    if (username && user.username !== username) {
        user.username = username;
        console.log(`[DB_SIM] Username updated for user ${userIdStr} to @${username}`);
    }
    user.isNew = false; // No longer new for this session if retrieved
    return { ...user, groupStats: new Map(user.groupStats) }; // Return a copy, including a copy of groupStats
}

/**
 * Updates a user's balance.
 * @param {string} userId The user's Telegram ID.
 * @param {number} amountChange The amount to change the balance by (positive for credit, negative for debit).
 * @param {string} reason For logging purposes (e.g., "won_coinflip", "bet_placed_roulette").
 * @param {string} [chatId] Optional: The chat ID where this balance change occurred, for group-specific stats.
 * @returns {Promise<{success: boolean, newBalance?: number, error?: string}>} Result of the operation.
 */
async function updateUserBalance(userId, amountChange, reason = "unknown_transaction", chatId) {
    const userIdStr = String(userId);
    if (!userDatabase.has(userIdStr)) {
        console.warn(`[DB_SIM_ERROR] Attempted to update balance for non-existent user: ${userIdStr}`);
        return { success: false, error: "User not found." };
    }

    const user = userDatabase.get(userIdStr);
    const proposedBalance = user.balance + amountChange;

    if (proposedBalance < 0) {
        console.log(`[DB_SIM] User ${userIdStr} has insufficient balance (${user.balance}) for deduction of ${Math.abs(amountChange)} (Reason: ${reason}).`);
        return { success: false, error: "Insufficient balance." };
    }

    user.balance = proposedBalance;
    user.lastPlayed = new Date(); // Update last activity timestamp for the user
    console.log(`[DB_SIM] User ${userIdStr} balance updated by ${amountChange}. New balance: ${user.balance}. Reason: ${reason}. Chat: ${chatId || 'N/A'}.`);

    // Update group-specific stats if chatId is provided
    if (chatId) {
        const chatIdStr = String(chatId);
        if (!user.groupStats.has(chatIdStr)) {
            user.groupStats.set(chatIdStr, { gamesPlayed: 0, totalWagered: 0, netWinLoss: 0 });
        }
        const stats = user.groupStats.get(chatIdStr);

        if (reason.toLowerCase().includes("bet_placed")) {
            const wagerAmount = Math.abs(amountChange); // amountChange is negative for bets
            stats.totalWagered += wagerAmount;
            // NetWinLoss is affected when the bet resolves, not just when placed,
            // or you can consider a bet placed as an immediate negative to netWinLoss.
            // For simplicity, let's assume bet placed means that amount is "at risk".
            stats.netWinLoss -= wagerAmount;
        } else if (reason.toLowerCase().includes("won_") || reason.toLowerCase().includes("payout_") || reason.toLowerCase().includes("refund_")) {
            // amountChange is positive for wins/payouts/refunds
            stats.netWinLoss += amountChange;
            if (!reason.toLowerCase().includes("refund_")) { // Don't count refunds as a game played
                 stats.gamesPlayed += 1;
            }
        } else if (reason.toLowerCase().includes("lost_")) {
            // If using the "bet_placed" to already reduce netWinLoss, a "lost" event means no further change to netWinLoss.
            // The wager was already accounted for. Just increment games played.
            stats.gamesPlayed += 1;
        }
    }

    return { success: true, newBalance: user.balance };
}

/**
 * Gets or creates a game session tracker for a group chat.
 * @param {string} chatId The group chat's Telegram ID.
 * @param {string} [chatTitle] The group chat's title (optional, can be updated).
 * @returns {Promise<GroupGameSession>} The group session object.
 */
async function getGroupSession(chatId, chatTitle) {
    const chatIdStr = String(chatId);
    if (!groupGameSessions.has(chatIdStr)) {
        const newSession = {
            chatId: chatIdStr,
            chatTitle: chatTitle,
            currentGameId: null,
            currentGameType: null,
            currentBetAmount: null,
            lastActivity: new Date(),
        };
        groupGameSessions.set(chatIdStr, newSession);
        console.log(`[DB_SIM] New group session created for chat ${chatIdStr} (${chatTitle || 'Untitled Group'}).`);
        return { ...newSession }; // Return a copy
    }

    const session = groupGameSessions.get(chatIdStr);
    // Update title if it has changed or was missing
    if (chatTitle && session.chatTitle !== chatTitle) {
        session.chatTitle = chatTitle;
        console.log(`[DB_SIM] Chat title updated for ${chatIdStr} to "${chatTitle}".`);
    }
    session.lastActivity = new Date(); // Update activity timestamp on access
    return { ...session }; // Return a copy
}

/**
 * Updates the current game details for a group chat session.
 * @param {string} chatId The group chat's Telegram ID.
 * @param {string | null} gameId Unique ID of the game instance being set, or null to clear.
 * @param {string | null} gameType The type of the game being set, or null to clear.
 * @param {number | null} betAmount The bet amount for the game, or null.
 * @returns {Promise<boolean>} True if successful.
 */
async function updateGroupGameDetails(chatId, gameId, gameType, betAmount) {
    const chatIdStr = String(chatId);
    if (!groupGameSessions.has(chatIdStr)) {
        // Attempt to create session if it doesn't exist, as updating implies it should.
        console.warn(`[DB_SIM] Attempted to update game for non-existent group session: ${chatIdStr}. Creating session.`);
        await getGroupSession(chatIdStr, "Unknown Group (Auto-created)");
    }

    const session = groupGameSessions.get(chatIdStr);
    session.currentGameId = gameId;
    session.currentGameType = gameType;
    session.currentBetAmount = betAmount;
    session.lastActivity = new Date();

    console.log(`[DB_SIM] Group ${chatIdStr} game details updated -> Game ID: ${gameId || 'None'}, Type: ${gameType || 'None'}, Bet: ${betAmount !== null ? formatCurrency(betAmount) : 'N/A'}`);
    return true;
}

console.log("Part 2: Simulated Database Operations & Data Management - Complete.");
// End of Part 2
// index.js - Part 3: Telegram Helpers & Basic Game Utilities
// --- VERSION: 1.0.0-group ---

// (Code from Parts 1 & 2 is assumed to be above this)
// ...

console.log("Loading Part 3: Telegram Helpers & Basic Game Utilities...");

// --- Telegram Specific Helper Functions ---

/**
 * Gets a user's display name, prioritizing their first name, then username, then a generic ID.
 * This is useful for constructing messages that address users.
 * The name is escaped for MarkdownV2.
 * @param {TelegramBot.User} userObject The user object from a Telegram message or callback.
 * @returns {string} An escaped, displayable name for the user.
 */
function getEscapedUserDisplayName(userObject) {
    if (!userObject) {
        return escapeMarkdownV2("Anonymous User"); // escapeMarkdownV2 from Part 1
    }
    const name = userObject.first_name || userObject.username || `User ID ${userObject.id}`;
    return escapeMarkdownV2(name);
}

/**
 * Creates a MarkdownV2 mention link for a user. This allows users to tap the name to open a chat with the user.
 * @param {TelegramBot.User} userObject The user object from a Telegram message or callback.
 * It must contain at least `id` and preferably `first_name` or `username`.
 * @returns {string} A MarkdownV2 mention string, e.g., "[John Doe](tg://user?id=123456789)"
 */
function createUserMention(userObject) {
    if (!userObject || !userObject.id) {
        console.warn("[createUserMention] Called with invalid userObject:", userObject);
        return escapeMarkdownV2("Unknown User");
    }
    // Use first_name if available, otherwise username, otherwise a generic placeholder.
    const displayName = userObject.first_name || userObject.username || `User ${userObject.id}`;
    // The display text within the brackets also needs to be MarkdownV2 escaped.
    return `[${escapeMarkdownV2(displayName)}](tg://user?id=${userObject.id})`;
}


// --- Basic Game & Currency Utilities ---

/**
 * Formats a numeric amount into a currency string.
 * For this simplified version, it assumes a generic "credits" or "points".
 * Can be adapted later for specific cryptocurrencies like SOL (e.g., by handling lamports).
 * @param {number} amount The amount to format.
 * @param {string} [currencyName="credits"] The name of the currency unit.
 * @returns {string} Formatted currency string (e.g., "100 credits", "0.50 points").
 */
function formatCurrency(amount, currencyName = "credits") {
    // Using toLocaleString for basic number formatting (e.g., thousands separators)
    // and a fixed number of decimal places if dealing with fractional units.
    let formattedAmount;
    if (Number.isInteger(amount)) {
        formattedAmount = amount.toLocaleString();
    } else {
        formattedAmount = amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }
    return `${formattedAmount} ${currencyName}`;
}

/**
 * Simulates rolling a standard N-sided die.
 * @param {number} [sides=6] Number of sides on the die. Must be a positive integer.
 * @returns {number} The result of the die roll (an integer between 1 and sides).
 */
function rollDie(sides = 6) {
    if (sides <= 0 || !Number.isInteger(sides)) {
        console.warn(`[rollDie] Invalid number of sides: ${sides}. Defaulting to 6.`);
        sides = 6;
    }
    return Math.floor(Math.random() * sides) + 1;
}

/**
 * Generates a visual string representation of dice rolls using Unicode dice characters for 1-6.
 * For rolls outside this range, or if diceEmojis are not preferred, it shows "🎲 Number".
 * @param {number[]} rolls An array of dice roll results.
 * @returns {string} A string like "Dice: ⚂ ⚄" or "Dice: 🎲 3 🎲 8"
 */
function formatDiceRolls(rolls) {
    const diceEmojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"]; // Standard Unicode dice for 1-6

    const diceVisuals = rolls.map(roll => {
        if (roll >= 1 && roll <= 6) {
            return diceEmojis[roll - 1]; // Array is 0-indexed
        }
        return `🎲 ${roll}`; // Fallback for other numbers or if emojis are not desired for them
    });

    return `Rolls: ${diceVisuals.join(' ')}`;
}

/**
 * Generates a simple unique ID, useful for game instances or other temporary tracking.
 * This is NOT cryptographically secure or guaranteed globally unique for large scale systems.
 * @returns {string} A string like "game_timestamp_randomsuffix".
 */
function generateGameId() {
    const timestamp = Date.now();
    const randomSuffix = Math.random().toString(36).substring(2, 9); // 7 random alphanumeric chars
    return `game_${timestamp}_${randomSuffix}`;
}

console.log("Part 3: Telegram Helpers & Basic Game Utilities - Complete.");
// End of Part 3
// index.js - Part 4: Simplified Game Logic
// --- VERSION: 1.0.0-group ---

// (Code from Parts 1, 2 & 3 is assumed to be above this)
// ...

console.log("Loading Part 4: Simplified Game Logic...");

// --- Game Logic Functions ---

/**
 * Simulates a coin flip.
 * @returns {{outcome: 'heads' | 'tails', outcomeString: string, emoji: string}} The result of the flip, including a display string and emoji.
 */
function determineCoinFlipOutcome() {
    const isHeads = Math.random() < 0.5;
    if (isHeads) {
        return { outcome: 'heads', outcomeString: "Heads", emoji: '🪙' };
    } else {
        return { outcome: 'tails', outcomeString: "Tails", emoji: '🪙' }; // Using the same emoji for both sides, text differs
    }
}

/**
 * Simulates rolling a single die.
 * This can be used as a basis for various dice games in a group.
 * @param {number} [sides=6] Number of sides on the die. Must be a positive integer.
 * @returns {{roll: number, emoji: string}} The result of the roll and a corresponding emoji.
 */
function determineDieRollOutcome(sides = 6) {
    if (sides <= 0 || !Number.isInteger(sides)) {
        console.warn(`[determineDieRollOutcome] Invalid number of sides: ${sides}. Defaulting to 6.`);
        sides = 6;
    }
    const roll = Math.floor(Math.random() * sides) + 1;
    let emojiRepresentation = `🎲 ${roll}`; // Default visual for dice with many sides or non-standard rolls
    const unicodeDiceEmojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"]; // For 1-6

    if (sides === 6 && roll >= 1 && roll <= 6) {
        emojiRepresentation = unicodeDiceEmojis[roll - 1];
    }
    return { roll: roll, emoji: emojiRepresentation };
}

/**
 * Simulates a "Guess the Number" game where the bot picks a secret number.
 * This function only determines the secret number. The guessing logic would be in Part 5.
 * @param {number} [min=1] The minimum number (inclusive).
 * @param {number} [max=10] The maximum number (inclusive).
 * @returns {{secretNumber: number}} The secret number chosen by the bot.
 */
function determineSecretNumber(min = 1, max = 10) {
    if (min >= max) {
        console.warn(`[determineSecretNumber] Min value ${min} is not less than max value ${max}. Defaulting to 1-10.`);
        min = 1;
        max = 10;
    }
    const secretNumber = Math.floor(Math.random() * (max - min + 1)) + min;
    return { secretNumber: secretNumber };
}

// --- Rock Paper Scissors (RPS) Game Logic ---
const RPS_CHOICES = {
    ROCK: 'rock',
    PAPER: 'paper',
    SCISSORS: 'scissors'
};

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

/**
 * Gets a random RPS choice for the bot or an undecided player.
 * @returns {{choice: string, emoji: string}} The bot's choice and its emoji.
 */
functiongetRandomRPSChoice() {
    const choices = Object.values(RPS_CHOICES);
    const randomChoice = choices[Math.floor(Math.random() * choices.length)];
    return { choice: randomChoice, emoji: RPS_EMOJIS[randomChoice] };
}

/**
 * Determines the winner of a Rock Paper Scissors round between two choices.
 * @param {string} choice1 One of RPS_CHOICES (e.g., 'rock').
 * @param {string} choice2 One of RPS_CHOICES (e.g., 'paper').
 * @returns {{
 * result: 'win1' | 'win2' | 'draw',
 * description: string,
 * choice1: string, choice1Emoji: string,
 * choice2: string, choice2Emoji: string
 * }}
 * Detailed result: 'win1' if choice1 wins, 'win2' if choice2 wins, 'draw'.
 * Description explains the outcome (e.g., "Rock crushes Scissors").
 */
function determineRPSOutcome(choice1, choice2) {
    if (!RPS_CHOICES[choice1.toUpperCase()] || !RPS_CHOICES[choice2.toUpperCase()]) {
        console.error(`[determineRPSOutcome] Invalid choices provided: ${choice1}, ${choice2}`);
        // Fallback or throw error. For now, let's assume a draw if choices are bad.
        return {
            result: 'draw',
            description: "Invalid choices led to a draw.",
            choice1: choice1, choice1Emoji: '❔',
            choice2: choice2, choice2Emoji: '❔'
        };
    }

    const c1 = RPS_CHOICES[choice1.toUpperCase()];
    const c2 = RPS_CHOICES[choice2.toUpperCase()];
    const c1Emoji = RPS_EMOJIS[c1];
    const c2Emoji = RPS_EMOJIS[c2];

    if (c1 === c2) {
        return {
            result: 'draw',
            description: `${c1Emoji} ${c1} draws with ${c2Emoji} ${c2}. It's a tie!`,
            choice1: c1, choice1Emoji: c1Emoji,
            choice2: c2, choice2Emoji: c2Emoji
        };
    }
    if (RPS_RULES[c1].beats === c2) {
        return {
            result: 'win1',
            description: `${c1Emoji} ${c1} ${RPS_RULES[c1].verb} ${c2Emoji} ${c2}. Player 1 wins!`,
            choice1: c1, choice1Emoji: c1Emoji,
            choice2: c2, choice2Emoji: c2Emoji
        };
    }
    // If not a draw and c1 doesn't beat c2, then c2 must beat c1
    return {
        result: 'win2',
        description: `${c2Emoji} ${c2} ${RPS_RULES[c2].verb} ${c1Emoji} ${c1}. Player 2 wins!`,
        choice1: c1, choice1Emoji: c1Emoji,
        choice2: c2, choice2Emoji: c2Emoji
    };
}

console.log("Part 4: Simplified Game Logic - Complete.");
// End of Part 4
// index.js - Part 5: Message & Callback Handling, Basic Game Flow
// --- VERSION: 1.0.0-group ---

// (Code from Parts 1, 2, 3 & 4 is assumed to be above this)
// ...

console.log("Loading Part 5: Message & Callback Handling, Basic Game Flow...");

// --- Constants for Cooldowns & Game Flow ---
const COMMAND_COOLDOWN_MS = 2000; // 2 seconds between commands for a user
const JOIN_GAME_TIMEOUT_MS = 60000; // 60 seconds for users to join a game
const MIN_BET_AMOUNT = 5; // Minimum bet amount for games, e.g., 5 credits
const MAX_BET_AMOUNT = 1000; // Maximum bet amount

// --- Main Message Handler ---
bot.on('message', async (msg) => {
    // Basic validation: ignore messages without text or sender
    if (!msg.text || !msg.from) {
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text;
    const chatType = msg.chat.type; // 'private', 'group', 'supergroup'
    const messageId = msg.message_id;

    console.log(`[MSG RCV] ChatID: ${chatId} (Type: ${chatType}), UserID: ${userId} (@${msg.from.username || 'N/A'}), Text: "${text}"`);

    // Prevent bot from reacting to its own messages or messages from other bots
    if (msg.from.is_bot) {
        return;
    }

    // Cooldown Check (userCooldowns map from Part 1)
    const now = Date.now();
    const lastCommandTime = userCooldowns.get(userId) || 0;
    if (now - lastCommandTime < COMMAND_COOLDOWN_MS) {
        console.log(`[COOLDOWN] User ${userId} command ignored due to cooldown.`);
        // Optionally, send a "please wait" message, but often better to ignore silently
        // await safeSendMessage(chatId, "Please wait a moment before sending another command.", {});
        return;
    }
    // Update last command time only if it's a command or a relevant interaction
    // For now, we'll update it broadly for any processed message that isn't ignored.

    // Simple Command Router (only processes messages starting with '/')
    if (text.startsWith('/')) {
        userCooldowns.set(userId, now); // Update cooldown time for commands
        const args = text.substring(1).split(' ');
        const command = args.shift().toLowerCase(); // Get the command and remove it from args

        // Ensure user exists in our system (getUser from Part 2)
        // This also updates their username if it has changed.
        await getUser(userId, msg.from.username);

        switch (command) {
            case 'start': // Fall-through
            case 'help':
                await handleHelpCommand(chatId, msg.from);
                break;
            case 'balance':
            case 'bal':
                await handleBalanceCommand(chatId, msg.from);
                break;
            case 'startcoinflip':
                if (chatType === 'group' || chatType === 'supergroup') {
                    let betAmount = args[0] ? parseInt(args[0], 10) : 10; // Default bet 10
                    if (isNaN(betAmount) || betAmount < MIN_BET_AMOUNT || betAmount > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet amount. Please use a number between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startcoinflip <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupCoinFlipCommand(chatId, msg.from, betAmount, messageId);
                } else {
                    await safeSendMessage(chatId, "Coin flip games can only be started in a group chat.", {});
                }
                break;
            case 'startrps': // Start Rock Paper Scissors
                if (chatType === 'group' || chatType === 'supergroup') {
                    let rpsBetAmount = args[0] ? parseInt(args[0], 10) : 10; // Default bet 10
                     if (isNaN(rpsBetAmount) || rpsBetAmount < MIN_BET_AMOUNT || rpsBetAmount > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet amount. Please use a number between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startrps <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupRPSCommand(chatId, msg.from, rpsBetAmount, messageId);
                } else {
                    await safeSendMessage(chatId, "Rock Paper Scissors games can only be started in a group chat.", {});
                }
                break;
            // Example for a game that requires an opponent to be tagged:
            // case 'challenge':
            //     if ((chatType === 'group' || chatType === 'supergroup') && msg.entities) {
            //         const mentionEntity = msg.entities.find(entity => entity.type === 'mention' || entity.type === 'text_mention');
            //         if (mentionEntity) {
            //             // Logic to extract opponent's ID and start a challenge
            //         } else {
            //             safeSendMessage(chatId, "Please mention a user to challenge: /challenge @username <game> <bet>", {});
            //         }
            //     }
            //     break;
            default:
                await safeSendMessage(chatId, "Sorry, I didn't understand that command. Try /help.", {});
        }
    }
    // Future: Handle non-command messages if the bot is in a specific state
    // (e.g., waiting for a user's bet choice after a game prompt)
});

// --- Callback Query Handler ---
bot.on('callback_query', async (callbackQuery) => {
    const msg = callbackQuery.message;
    if (!msg) { // Message might be missing if it's too old or deleted
        console.warn("[CBQ_WARN] Callback query received without an associated message:", callbackQuery);
        bot.answerCallbackQuery(callbackQuery.id, { text: "This action has expired." }).catch(e => console.error("Error answering CBQ:", e.message));
        return;
    }

    const userId = String(callbackQuery.from.id); // User who clicked the button
    const chatId = String(msg.chat.id);
    const data = callbackQuery.data; // e.g., 'join_game:game123:coinflip'
    const originalMessageId = msg.message_id;

    console.log(`[CBQ RCV] ChatID: ${chatId}, UserID: ${userId} (@${callbackQuery.from.username || 'N/A'}), Data: "${data}", MsgID: ${originalMessageId}`);

    // Acknowledge the button press immediately to remove the "loading" icon on the button
    bot.answerCallbackQuery(callbackQuery.id).catch(e => console.warn(`[CBQ_WARN] Failed to answer callback query ${callbackQuery.id}: ${e.message}`));

    // Ensure user exists (getUser from Part 2)
    await getUser(userId, callbackQuery.from.username);

    const [action, ...params] = data.split(':');

    try {
        switch (action) {
            case 'join_game': // Params: gameId
                const gameIdToJoin = params[0];
                await handleJoinGameCallback(chatId, callbackQuery.from, gameIdToJoin, originalMessageId);
                break;
            case 'cancel_game': // Params: gameId
                const gameIdToCancel = params[0];
                await handleCancelGameCallback(chatId, callbackQuery.from, gameIdToCancel, originalMessageId);
                break;
            case 'rps_choose': // Params: gameId, choice (rock/paper/scissors)
                const rpsGameId = params[0];
                const rpsChoice = params[1];
                await handleRPSChoiceCallback(chatId, callbackQuery.from, rpsGameId, rpsChoice, originalMessageId);
                break;
            // Add more callback actions for other game interactions as needed
            default:
                console.log(`[CBQ_INFO] Unknown callback action: ${action} from user ${userId}`);
                // Optionally send a message if the action is truly unknown and not just an expired button
                // await safeSendMessage(chatId, "That button seems to be for an old action or is no longer valid.", {});
        }
    } catch (error) {
        console.error(`[CBQ_ERROR] Error processing callback data "${data}" for user ${userId}:`, error);
        // Notify user of a generic error with the button.
        await safeSendMessage(userId, "Sorry, there was an error processing your action. Please try again or start a new game.", {}).catch();
    }
});

// --- Command Handler Functions ---

async function handleHelpCommand(chatId, userObject) {
    const userMention = createUserMention(userObject); // createUserMention from Part 3
    const helpTextParts = [
        `👋 Hello ${userMention}!`,
        `I'm the *Group Chat Casino Bot* v${BOT_VERSION}. Let's play some games!`,
        `Here are some commands:`,
        `▫️ \`/help\` - Shows this help message`,
        `▫️ \`/balance\` or \`/bal\` - Check your game credits`,
        `▫️ \`/startcoinflip <amount>\` - Start a coin flip game in a group (e.g., \`/startcoinflip 10\`)`,
        `▫️ \`/startrps <amount>\` - Start a Rock Paper Scissors game (e.g., \`/startrps 5\`)`,
        `➡️ When a game starts, click the 'Join Game' button to participate!`,
        `More games and features are coming soon!`,
    ];
    // Note: safeSendMessage will handle the MarkdownV2 escaping if specified in options.
    // We pass the raw string here.
    await safeSendMessage(chatId, helpTextParts.join('\n\n'), { parse_mode: 'MarkdownV2' });
}

async function handleBalanceCommand(chatId, userObject) {
    const user = await getUser(String(userObject.id)); // getUser from Part 2
    const userMention = createUserMention(userObject);
    const balanceMessage = `${userMention}, your current balance is: *${formatCurrency(user.balance)}*.`;
    await safeSendMessage(chatId, balanceMessage, { parse_mode: 'MarkdownV2' });
}


// --- Group Game Flow Functions (Example: Coinflip & RPS) ---

/**
 * Initiates a group coin flip game.
 * @param {string} chatId
 * @param {TelegramBot.User} initiatorUser
 * @param {number} betAmount
 * @param {number} commandMessageId - The ID of the user's /startcoinflip command message.
 */
async function handleStartGroupCoinFlipCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, "Group Chat"); // getGroupSession from Part 2
    const gameId = generateGameId(); // generateGameId from Part 3

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game of *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}* (ID: \`${gameSession.currentGameId}\`) is already active or being set up in this group. Please wait for it to finish.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, you don't have enough balance (${escapeMarkdownV2(formatCurrency(initiator.balance))}) to start a game for ${escapeMarkdownV2(formatCurrency(betAmount))}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // "Hold" initiator's bet
    await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_coinflip_init:${gameId}`, chatId); // updateUserBalance from Part 2

    activeGames.set(gameId, { // activeGames Map from Part 1
        type: 'coinflip',
        chatId: chatId,
        initiatorId: initiatorId,
        initiatorMention: createUserMention(initiatorUser),
        betAmount: betAmount,
        participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser) }],
        status: 'waiting_opponent', // Waiting for one opponent
        creationTime: Date.now(),
        commandMessageId: commandMessageId // Store the original command message ID
    });

    await updateGroupGameDetails(chatId, gameId, 'CoinFlip', betAmount); // updateGroupGameDetails from Part 2

    const joinMessage = `${createUserMention(initiatorUser)} has started a *Coin Flip Challenge* for ${escapeMarkdownV2(formatCurrency(betAmount))}!\n\nAn opponent is needed. Click "Join Game" to accept!`;
    const keyboard = { inline_keyboard: [
            [{ text: "🪙 Join Coinflip!", callback_data: `join_game:${gameId}` }],
            [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]
        ]};
    const gameSetupMessage = await safeSendMessage(chatId, joinMessage, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    if (gameSetupMessage) {
        const gameData = activeGames.get(gameId);
        if (gameData) gameData.gameSetupMessageId = gameSetupMessage.message_id; // Store the ID of the "Join Game" message
    }

    // Timeout for game setup
    setTimeout(async () => {
        const currentGameData = activeGames.get(gameId);
        if (currentGameData && currentGameData.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] Coinflip game ${gameId} in chat ${chatId} timed out waiting for an opponent.`);
            await updateUserBalance(currentGameData.initiatorId, currentGameData.betAmount, `refund_group_coinflip_timeout:${gameId}`, chatId);
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsg = `The Coin Flip game (ID: \`${gameId}\`) started by ${currentGameData.initiatorMention} for ${escapeMarkdownV2(formatCurrency(currentGameData.betAmount))} has expired due to no opponent joining. The bet has been refunded.`;
            if (currentGameData.gameSetupMessageId) {
                bot.editMessageText(escapeMarkdownV2(timeoutMsg), { chatId: chatId, message_id: currentGameData.gameSetupMessageId, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(e => safeSendMessage(chatId, escapeMarkdownV2(timeoutMsg), {parse_mode: 'MarkdownV2'}));
            } else {
                safeSendMessage(chatId, escapeMarkdownV2(timeoutMsg), {parse_mode: 'MarkdownV2'});
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

/**
 * Handles a user joining a game via a button click.
 * @param {string} chatId
 * @param {TelegramBot.User} joinerUser
 * @param {string} gameId
 * @param {number} interactionMessageId - The ID of the message with the "Join Game" button.
 */
async function handleJoinGameCallback(chatId, joinerUser, gameId, interactionMessageId) {
    const joinerId = String(joinerUser.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== chatId) {
        await safeSendMessage(joinerId, "This game is no longer available or is not in this chat.", {}); // Notify user privately
        bot.editMessageReplyMarkup({}, {chat_id: chatId, message_id: interactionMessageId}).catch(()=>{}); // Try to remove buttons
        return;
    }

    if (gameData.initiatorId === joinerId) {
        await safeSendMessage(joinerId, "You can't join a game you initiated as an opponent.", {});
        return;
    }

    if (gameData.status !== 'waiting_opponent') {
        await safeSendMessage(joinerId, "This game is already full, has started, or has been cancelled.", {});
        if(gameData.status === 'playing' || gameData.status === 'game_over') { // If game is already on/over, remove buttons
             bot.editMessageReplyMarkup({}, {chat_id: chatId, message_id: interactionMessageId}).catch(()=>{});
        }
        return;
    }

    const joiner = await getUser(joinerId);
    if (joiner.balance < gameData.betAmount) {
        await safeSendMessage(joinerId, `You don't have enough balance (${escapeMarkdownV2(formatCurrency(joiner.balance))}) to join this ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} game.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_group_${gameData.type}_join:${gameId}`, chatId);
    gameData.participants.push({ userId: joinerId, choice: null, mention: createUserMention(joinerUser) });
    gameData.status = 'playing'; // Mark as playing now that opponent joined
    activeGames.set(gameId, gameData);

    // Resolve Coinflip immediately for this 2-player example
    if (gameData.type === 'coinflip' && gameData.participants.length === 2) {
        const initiator = gameData.participants[0];
        const opponent = gameData.participants[1];

        // Assign choices: initiator heads, opponent tails for simplicity
        initiator.choice = 'heads';
        opponent.choice = 'tails'; // Opponent wins if tails

        const coinResult = determineCoinFlipOutcome(); // from Part 4
        let winnerParticipant = (coinResult.outcome === initiator.choice) ? initiator : opponent;
        let loserParticipant = (winnerParticipant === initiator) ? opponent : initiator;

        const winnings = gameData.betAmount * 2; // Total pot
        await updateUserBalance(winnerParticipant.userId, winnings, `won_group_coinflip_resolved:${gameId}`, chatId);

        const resultMessage =
            `*Coin Flip: Match Found & Resolved!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n` +
            `${initiator.mention} (Heads) vs ${opponent.mention} (Tails)\n\n` +
            `The coin landed on: *${escapeMarkdownV2(coinResult.outcomeString)} ${coinResult.emoji}*!\n\n` +
            `🎉 ${winnerParticipant.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} (total ${escapeMarkdownV2(formatCurrency(winnings))} payout)!\n` +
            `😢 ${loserParticipant.mention} lost their bet.`;

        if (interactionMessageId) { // Edit the original "Join Game" message
            bot.editMessageText(escapeMarkdownV2(resultMessage), { chatId: chatId, message_id: interactionMessageId, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(e => safeSendMessage(chatId, escapeMarkdownV2(resultMessage), {parse_mode: 'MarkdownV2'}));
        } else {
            safeSendMessage(chatId, escapeMarkdownV2(resultMessage), {parse_mode: 'MarkdownV2'});
        }
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    } else if (gameData.type === 'rps' && gameData.participants.length === 2) {
        // For RPS, don't resolve immediately. Prompt for choices.
        const rpsPrompt = `${gameData.participants[0].mention} and ${gameData.participants[1].mention}, your Rock Paper Scissors match for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} is ready!\n\nChoose your move privately by clicking a button below.`;
        const rpsKeyboard = { inline_keyboard: [
            [{ text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` }],
            [{ text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` }],
            [{ text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }]
        ]};
         if (interactionMessageId) {
            bot.editMessageText(escapeMarkdownV2(rpsPrompt), {chatId: chatId, message_id: interactionMessageId, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard}).catch(e => safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt), {parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard}));
        } else {
            safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt), {parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard});
        }
        gameData.status = 'waiting_choices';
        activeGames.set(gameId, gameData);
    }
}


/**
 * Handles cancellation of game setup by the initiator.
 * @param {string} chatId
 * @param {TelegramBot.User} cancellerUser
 * @param {string} gameId
 * @param {number} interactionMessageId - The ID of the message with the "Join/Cancel" buttons.
 */
async function handleCancelGameCallback(chatId, cancellerUser, gameId, interactionMessageId) {
    const cancellerId = String(cancellerUser.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== chatId) {
        await safeSendMessage(cancellerId, "This game is no longer available for cancellation.", {});
        return;
    }

    if (gameData.initiatorId !== cancellerId) {
        await safeSendMessage(cancellerId, `Only the game initiator (${gameData.initiatorMention}) can cancel this game setup.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Can only cancel if waiting for opponent or choices (if applicable for more complex games)
    if (gameData.status !== 'waiting_opponent' && gameData.status !== 'waiting_choices') {
        await safeSendMessage(cancellerId, `This game cannot be cancelled at its current stage (${escapeMarkdownV2(gameData.status)}).`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Refund initiator's bet
    await updateUserBalance(gameData.initiatorId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, chatId);
    // If other participants had placed bets (for multi-join games), refund them too.
    // For this 2-player model, only initiator's bet is held before opponent joins.
    if(gameData.participants.length > 1 && gameData.status === 'waiting_choices') { // e.g. RPS if opponent joined but game cancelled before choices
        for(const p of gameData.participants) {
            if(p.userId !== gameData.initiatorId && p.betPlaced) { // Assuming a betPlaced flag
                 await updateUserBalance(p.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, chatId);
            }
        }
    }


    const gameTypeDisplay = gameData.type.charAt(0).toUpperCase() + gameData.type.slice(1);
    const cancellationMessage = `${gameData.initiatorMention} has cancelled the ${escapeMarkdownV2(gameTypeDisplay)} game for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}. Bets have been refunded.`;

    if (interactionMessageId) {
        bot.editMessageText(cancellationMessage, {
            chat_id: chatId, message_id: interactionMessageId, parse_mode: 'MarkdownV2', reply_markup: {} // Remove buttons
        }).catch(e => {
            console.warn(`[CBQ_WARN] Error editing game cancellation message ${interactionMessageId}: ${e.message}`);
            safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
        });
    } else {
        safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
    }

    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
    console.log(`[GAME_CANCEL] Game ${gameId} in chat ${chatId} cancelled by initiator ${cancellerId}.`);
}


async function handleStartGroupRPSCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, "Group Chat");
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game of *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}* (ID: \`${gameSession.currentGameId}\`) is already active. Please wait.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, you need ${escapeMarkdownV2(formatCurrency(betAmount))} to start, but only have ${escapeMarkdownV2(formatCurrency(initiator.balance))}.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_rps_init:${gameId}`, chatId);
    activeGames.set(gameId, {
        type: 'rps',
        chatId: chatId,
        initiatorId: initiatorId,
        initiatorMention: createUserMention(initiatorUser),
        betAmount: betAmount,
        participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser), betPlaced: true }],
        status: 'waiting_opponent',
        creationTime: Date.now(),
        commandMessageId: commandMessageId
    });

    await updateGroupGameDetails(chatId, gameId, 'RockPaperScissors', betAmount);

    const joinMessageRPS = `${createUserMention(initiatorUser)} challenges someone to *Rock Paper Scissors* for ${escapeMarkdownV2(formatCurrency(betAmount))}!\n\nClick "Join Game" to play!`;
    const keyboardRPS = { inline_keyboard: [
            [{ text: "🪨📄✂️ Join RPS!", callback_data: `join_game:${gameId}` }],
            [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]
        ]};
    const gameSetupMessageRPS = await safeSendMessage(chatId, joinMessageRPS, { parse_mode: 'MarkdownV2', reply_markup: keyboardRPS });
     if (gameSetupMessageRPS) {
        const gameData = activeGames.get(gameId);
        if (gameData) gameData.gameSetupMessageId = gameSetupMessageRPS.message_id;
    }


    setTimeout(async () => {
        const currentGameData = activeGames.get(gameId);
        if (currentGameData && currentGameData.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] RPS game ${gameId} in chat ${chatId} timed out.`);
            await updateUserBalance(currentGameData.initiatorId, currentGameData.betAmount, `refund_group_rps_timeout:${gameId}`, chatId);
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsg = `The RPS game (ID: \`${gameId}\`) by ${currentGameData.initiatorMention} for ${escapeMarkdownV2(formatCurrency(currentGameData.betAmount))} expired. Bet refunded.`;
            if (currentGameData.gameSetupMessageId) {
                bot.editMessageText(escapeMarkdownV2(timeoutMsg), { chatId: chatId, message_id: currentGameData.gameSetupMessageId, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(e => safeSendMessage(chatId, escapeMarkdownV2(timeoutMsg), {parse_mode: 'MarkdownV2'}));
            } else {
                safeSendMessage(chatId, escapeMarkdownV2(timeoutMsg), {parse_mode: 'MarkdownV2'});
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleRPSChoiceCallback(chatId, userObject, gameId, choice, interactionMessageId) {
    const userId = String(userObject.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== chatId || gameData.type !== 'rps') {
        await safeSendMessage(userId, "This RPS game is no longer available or your choice is for a different game.", {});
        return;
    }
    if (gameData.status !== 'waiting_choices') {
        await safeSendMessage(userId, "The game is not currently waiting for choices.", {});
        return;
    }

    const participant = gameData.participants.find(p => p.userId === userId);
    if (!participant) {
        await safeSendMessage(userId, "You are not part of this RPS game.", {});
        return;
    }
    if (participant.choice) {
        await safeSendMessage(userId, `You have already chosen ${RPS_EMOJIS[participant.choice]} for this game.`, {parse_mode: 'MarkdownV2'});
        return;
    }

    participant.choice = choice; // choice is 'rock', 'paper', or 'scissors'
    await safeSendMessage(userId, `You chose ${RPS_EMOJIS[choice]}! Waiting for the other player...`, {parse_mode: 'MarkdownV2'}); // Private confirmation
    console.log(`[RPS_CHOICE] User ${userId} chose ${choice} for game ${gameId}`);

    // Check if all participants have made a choice
    const allChosen = gameData.participants.every(p => p.choice !== null);
    if (allChosen && gameData.participants.length === 2) {
        gameData.status = 'game_over';
        activeGames.set(gameId, gameData); // Save before async operations

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        const rpsResult = determineRPSOutcome(p1.choice, p2.choice); // from Part 4

        let winnerParticipant, loserParticipant;
        if (rpsResult.result === 'win1') {
            winnerParticipant = p1;
            loserParticipant = p2;
        } else if (rpsResult.result === 'win2') {
            winnerParticipant = p2;
            loserParticipant = p1;
        }

        let resultMessage = `*Rock Paper Scissors: Result!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n` +
                            `${p1.mention} chose ${RPS_EMOJIS[p1.choice]}\n` +
                            `${p2.mention} chose ${RPS_EMOJIS[p2.choice]}\n\n` +
                            `${escapeMarkdownV2(rpsResult.description)}\n\n`;

        if (winnerParticipant) {
            const winnings = gameData.betAmount * 2;
            await updateUserBalance(winnerParticipant.userId, winnings, `won_group_rps_resolved:${gameId}`, chatId);
            resultMessage += `🎉 ${winnerParticipant.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} (total ${escapeMarkdownV2(formatCurrency(winnings))} payout)!\n`;
            resultMessage += `😢 ${loserParticipant.mention} lost their bet.`;
        } else { // Draw
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_group_rps_draw:${gameId}`, chatId);
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_group_rps_draw:${gameId}`, chatId);
            resultMessage += `It's a draw! Bets are refunded.`;
        }

        if (interactionMessageId) { // Edit the "Choose your move" message
            bot.editMessageText(resultMessage, { chatId: chatId, message_id: interactionMessageId, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(e => safeSendMessage(chatId, resultMessage, {parse_mode: 'MarkdownV2'}));
        } else if (gameData.gameSetupMessageId) { // Fallback to editing game setup message
             bot.editMessageText(resultMessage, { chatId: chatId, message_id: gameData.gameSetupMessageId, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(e => safeSendMessage(chatId, resultMessage, {parse_mode: 'MarkdownV2'}));
        }
        else {
            safeSendMessage(chatId, resultMessage, {parse_mode: 'MarkdownV2'});
        }

        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    } else {
        // Still waiting for other player(s)
        const waitingMsg = `${participant.mention} has made their choice. Still waiting for other players.`;
        if (interactionMessageId) { // Edit the "Choose your move" message to update its content slightly or just acknowledge
             bot.editMessageText(escapeMarkdownV2(waitingMsg) + "\n\n_Waiting for opponent..._", {chatId: chatId, message_id: interactionMessageId, parse_mode: 'MarkdownV2'}).catch(e => {/*ignore if edit fails, user got PM*/});
        }
        activeGames.set(gameId, gameData); // Save updated participant choice
    }
}


console.log("Part 5: Message & Callback Handling, Basic Game Flow - Complete.");
// End of Part 5
// index.js - Part 6: Startup, Shutdown, and Basic Error Handling
// --- VERSION: 1.0.0-group ---

// (Code from Parts 1, 2, 3, 4 & 5 is assumed to be above this)
// ...

console.log("Loading Part 6: Startup, Shutdown, and Basic Error Handling...");

// --- Placeholder for Background Tasks ---
/**
 * Example: A function that could be called periodically for cleanup or other maintenance.
 * In a more advanced bot, this might run on a setInterval.
 */
async function runPeriodicBackgroundTasks() {
    console.log(`[BACKGROUND_TASK] [${new Date().toISOString()}] Running periodic background tasks...`);

    const now = Date.now();
    const GAME_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 5; // e.g., 5 times the join timeout for very old games

    // Clean up very old or stuck games from activeGames
    let cleanedGames = 0;
    for (const [gameId, gameData] of activeGames.entries()) {
        if (now - gameData.creationTime > GAME_CLEANUP_THRESHOLD_MS) {
            console.warn(`[BACKGROUND_TASK] Cleaning up very old/stuck game ${gameId} in chat ${gameData.chatId} (Status: ${gameData.status}).`);
            // Try to refund initiator if game was still waiting for an opponent
            if (gameData.status === 'waiting_opponent' && gameData.initiatorId && gameData.betAmount > 0) {
                await updateUserBalance(gameData.initiatorId, gameData.betAmount, `refund_stale_game_cleanup:${gameId}`, gameData.chatId);
                console.log(`[BACKGROUND_TASK] Refunded ${formatCurrency(gameData.betAmount)} to initiator ${gameData.initiatorId} for stale game ${gameId}.`);
                // Notify in chat if possible (might be too old, or message ID not stored robustly here)
                if(gameData.gameSetupMessageId) {
                    const staleGameMsg = `The game (ID: \`${gameId}\`) initiated by ${gameData.initiatorMention} was found to be very old and has been cleared. The initial bet has been refunded.`;
                    bot.editMessageText(escapeMarkdownV2(staleGameMsg), {chat_id: gameData.chatId, message_id: gameData.gameSetupMessageId, parse_mode: 'MarkdownV2', reply_markup:{}}).catch(()=>{});
                } else {
                    safeSendMessage(gameData.chatId, `An old game (ID: \`${gameId}\`) initiated by ${gameData.initiatorMention} was cleared due to inactivity. The initial bet has been refunded.`, {parse_mode: 'MarkdownV2'});
                }
            }
            activeGames.delete(gameId);
            // Clear from groupGameSessions if this was the active one
            const groupSession = await getGroupSession(gameData.chatId); // Use await here
            if (groupSession && groupSession.currentGameId === gameId) {
                await updateGroupGameDetails(gameData.chatId, null, null, null);
            }
            cleanedGames++;
        }
    }
    if (cleanedGames > 0) {
        console.log(`[BACKGROUND_TASK] Cleaned up ${cleanedGames} stale game(s).`);
    }

    // Example: Clean up very old group sessions that have no active game and no recent activity
    const SESSION_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 20; // Much longer threshold for inactive group sessions
    let cleanedSessions = 0;
    for (const [chatId, sessionData] of groupGameSessions.entries()) {
        if (!sessionData.currentGameId && (now - sessionData.lastActivity.getTime()) > SESSION_CLEANUP_THRESHOLD_MS) {
            console.log(`[BACKGROUND_TASK] Cleaning up inactive group session entry for chat ${chatId}. Last activity: ${sessionData.lastActivity.toISOString()}`);
            groupGameSessions.delete(chatId);
            cleanedSessions++;
        }
    }
     if (cleanedSessions > 0) {
        console.log(`[BACKGROUND_TASK] Cleaned up ${cleanedSessions} inactive group session entries.`);
    }

    console.log(`[BACKGROUND_TASK] Periodic background tasks finished. Active games: ${activeGames.size}, Group sessions tracked: ${groupGameSessions.size}.`);
}

// To run background tasks periodically (e.g., every 15 minutes):
// const backgroundTaskInterval = setInterval(runPeriodicBackgroundTasks, 15 * 60 * 1000);
// console.log("Periodic background tasks scheduled.");

// --- Process-Level Error Handling ---
process.on('uncaughtException', (error, origin) => {
    console.error(`\n🚨🚨 UNCAUGHT EXCEPTION AT: ${origin} 🚨🚨`);
    console.error(error);
    // In a production environment, you'd want to log this to a file or external service
    // and potentially notify an administrator immediately.
    if (ADMIN_USER_ID) {
        safeSendMessage(ADMIN_USER_ID, `🆘 UNCAUGHT EXCEPTION on GroupCasinoBot:\nOrigin: ${origin}\nError: ${error.message}\n\nBot might be unstable or need a restart.`, {}).catch();
    }
    // Exiting on an uncaught exception is generally safer to avoid an inconsistent state.
    // However, for development, you might want to keep it running.
    // For production, a process manager (like PM2) should restart the bot.
    // process.exit(1); // Uncomment for production to force exit and allow restart by process manager
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`\n🔥🔥 UNHANDLED PROMISE REJECTION 🔥🔥`);
    console.error('Reason:', reason); // This could be an Error object or any other value
    console.error('Promise:', promise);
    if (ADMIN_USER_ID) {
        const reasonMsg = reason instanceof Error ? reason.message : String(reason);
        safeSendMessage(ADMIN_USER_ID, `♨️ UNHANDLED REJECTION on GroupCasinoBot:\nReason: ${escapeMarkdownV2(reasonMsg)}\n\nReview logs.`, {parse_mode: 'MarkdownV2'}).catch();
    }
});

// --- Graceful Shutdown Handling ---
let isShuttingDown = false;
async function shutdown(signal) {
    if (isShuttingDown) {
        console.log("[SHUTDOWN] Already in progress...");
        return;
    }
    isShuttingDown = true;
    console.log(`\n🚦 Received signal: ${signal}. Initiating graceful shutdown of Group Casino Bot v${BOT_VERSION}...`);

    // 1. Stop Telegram Polling to prevent new incoming updates
    if (bot && typeof bot.stopPolling === 'function' && bot.isPolling()) {
        try {
            await bot.stopPolling({ cancel: true }); // cancel any pending getUpdates requests
            console.log("[SHUTDOWN] Telegram polling successfully stopped.");
        } catch (error) {
            console.error("[SHUTDOWN_ERROR] Error stopping Telegram polling:", error.message);
        }
    } else {
        console.log("[SHUTDOWN] Telegram polling was not active or bot instance not available.");
    }

    // 2. Clear any running intervals (like background tasks if they were active)
    // if (backgroundTaskInterval) clearInterval(backgroundTaskInterval);
    // console.log("[SHUTDOWN] Background task interval cleared (if was running).");


    // 3. Save any critical in-memory data if needed (not applicable for current simple state)
    // For example, if activeGames needed to be persisted:
    // console.log("[SHUTDOWN] Attempting to save 'activeGames' state (simulated)...");
    // In a real scenario, this would write to a file or database.

    console.log("[SHUTDOWN] In-memory data (userDatabase, groupGameSessions, activeGames) will be lost.");

    // 4. Notify admin (optional)
    if (ADMIN_USER_ID) {
        await safeSendMessage(ADMIN_USER_ID, `ℹ️ Group Casino Bot v${BOT_VERSION} is shutting down (Signal: ${signal}).`).catch(e => console.error("[SHUTDOWN_ERROR] Failed to send shutdown notification to admin:", e.message));
    }

    console.log(`✅ [SHUTDOWN] Graceful shutdown complete for Group Casino Bot v${BOT_VERSION}. Exiting process.`);
    // Determine exit code based on signal
    const exitCode = (signal === 'SIGINT' || signal === 'SIGTERM') ? 0 : 1;
    process.exit(exitCode);
}

// Listen for shutdown signals
process.on('SIGINT', () => shutdown('SIGINT'));   // Ctrl+C
process.on('SIGTERM', () => shutdown('SIGTERM')); // `kill` command (default)

// --- Main Application Startup Function ---
async function main() {
    console.log(`\n🚀🚀🚀 Initializing Solana Group Chat Casino Bot v${BOT_VERSION} (Simplified Mode) 🚀🚀🚀`);
    console.log(`Timestamp: ${new Date().toISOString()}`);
    console.log("Attempting to connect to Telegram API...");

    try {
        const me = await bot.getMe(); // Verifies the BOT_TOKEN and connection
        console.log(`✅ Successfully connected to Telegram! Bot Name: @${me.username}, Bot ID: ${me.id}`);

        // Notify admin that the bot has started (optional)
        if (ADMIN_USER_ID) {
            await safeSendMessage(ADMIN_USER_ID, `🎉 Group Casino Bot v${BOT_VERSION} (Simplified) has started successfully and is now polling for messages. Host: ${process.env.HOSTNAME || 'local'}`, { parse_mode: 'MarkdownV2' });
        }

        // Start any other initial processes here if needed (e.g., load data from a file if not using DB)
        console.log(`\n🎉 Bot is now fully operational and polling for messages.`);
        console.log(`Press Ctrl+C to stop the bot.`);

        // Example: Run background tasks once shortly after startup
        setTimeout(runPeriodicBackgroundTasks, 10000); // Run 10 seconds after startup

    } catch (error) {
        console.error("❌ CRITICAL ERROR DURING BOT STARTUP (bot.getMe() failed):");
        console.error(error);
        console.error("Please check your BOT_TOKEN, network connection, and Telegram API status.");
        // Notify admin about critical startup failure if possible
        if (ADMIN_USER_ID && BOT_TOKEN) { // Check BOT_TOKEN too, as a faulty one could cause getMe to fail
            const tempBotForError = new TelegramBot(BOT_TOKEN, {}); // Temporary instance without polling
            tempBotForError.sendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE for GroupCasinoBot v${BOT_VERSION}:\n${escapeMarkdownV2(error.message)}\nBot is exiting.`).catch(e => console.error("Failed to send critical startup error admin notification:", e));
        }
        process.exit(1); // Exit with an error code
    }
}

// --- Start the Bot ---
main().catch(error => {
    // This catch is for any unhandled errors specifically from the main() function itself,
    // though most errors within main() should be caught and handled there.
    console.error("❌ UNHANDLED CRITICAL ERROR IN MAIN ASYNC FUNCTION:", error);
    process.exit(1); // Exit if main startup logic itself has an unhandled rejection.
});

console.log("Part 6: Startup, Shutdown, and Basic Error Handling - Complete.");
// End of Part 6
// --- END OF index.js ---
