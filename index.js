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
console.log("BOT_TOKEN loaded.");
if (ADMIN_USER_ID) {
    console.log(`Admin User ID: ${ADMIN_USER_ID} loaded.`);
} else {
    console.log("No ADMIN_USER_ID set (optional).");
}

// --- Basic Bot Initialization ---
// We'll use polling for simplicity in this initial version.
// Webhooks can be configured later if needed.
const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("Telegram Bot instance created with polling.");

// --- Global Constants & Simple State (Examples) ---
const BOT_VERSION = '1.0.0-group';
let activeGames = new Map(); // Example: To track games active in different group chats { chatId: gameDetails }
let userCooldowns = new Map(); // Example: { userId: lastCommandTimestamp } for simple spam prevention

console.log(`Solana Group Chat Casino Bot v${BOT_VERSION} initializing...`);

// --- Basic Utility Functions (Examples) ---

/**
 * Escapes characters for Telegram MarkdownV2 parse mode.
 * A simplified version. For production, a more robust one is needed.
 * @param {string | number | null | undefined} text Input text to escape.
 * @returns {string} Escaped text.
 */
const escapeMarkdownV2 = (text) => {
    if (text === null || typeof text === 'undefined') {
        return '';
    }
    const textString = String(text);
    // Basic escapes - extend as needed
    return textString.replace(/[_*[\]()~`>#+\-=|{}.!\\]/g, '\\$&');
};

/**
 * Simple function to send a message with error handling.
 * @param {string|number} chatId Target chat ID.
 * @param {string} text Message text (should be pre-escaped if using Markdown).
 * @param {object} [options={}] Telegram SendMessageOptions.
 */
async function safeSendMessage(chatId, text, options = {}) {
    try {
        await bot.sendMessage(chatId, text, options);
    } catch (error) {
        console.error(`Failed to send message to chat ${chatId}: ${error.message}`);
        // Basic error handling: if bot is blocked by user or chat not found
        if (error.response && (error.response.statusCode === 403 || error.response.statusCode === 400)) {
            console.warn(`Bot may be blocked or chat not found for ID: ${chatId}. Specific error: ${error.message}`);
            // Consider removing chat from active interactions if such errors persist
        }
    }
}

// End of Part 1
// index.js - Part 2: Simulated Database Operations & Data Management
// --- VERSION: 1.0.0-group ---

// (Code from Part 1 is assumed to be above this)
// ...

// --- In-Memory Data Stores (Simulating a Database) ---

/**
 * @type {Map<string, {userId: string, username?: string, balance: number, lastPlayed?: Date, groupStats: Map<string, { gamesPlayed: number, totalWagered: number, netWinLoss: number }> }>}
 * Simulates a 'users' table.
 * - key: Telegram User ID (string)
 * - value: Object containing user details:
 * - userId: Telegram User ID
 * - username: Telegram username (optional)
 * - balance: User's current balance (e.g., in SOL, or a game currency unit)
 * - lastPlayed: Timestamp of the last game played
 * - groupStats: Map where key is chatId, value is stats for that group
 */
const userDatabase = new Map();

/**
 * @type {Map<string, {chatId: string, chatTitle?: string, currentGame: string | null, currentBetAmount: number | null, participants: string[], lastActivity: Date}>}
 * Simulates a 'group_sessions' or 'active_games_in_groups' table.
 * - key: Telegram Chat ID (string)
 * - value: Object containing group game session details:
 * - chatId: Telegram Chat ID
 * - chatTitle: Title of the group chat (optional)
 * - currentGame: String identifying the current game being played (e.g., 'coinflip_group'), or null if none.
 * - currentBetAmount: The agreed bet amount for the current game.
 * - participants: Array of userIds participating in the current game.
 * - lastActivity: Timestamp of the last activity in this group's game session.
 */
const groupGameSessions = new Map();

console.log("In-memory data stores initialized (simulating database).");

// --- Simulated Database Functions ---

/**
 * Gets or creates a user in our simulated database.
 * @param {string} userId The user's Telegram ID.
 * @param {string} [username] The user's Telegram username (optional).
 * @returns {Promise<{userId: string, username?: string, balance: number, isNew: boolean, groupStats: Map<string, any>}>} The user object.
 */
async function getUser(userId, username) {
    if (!userDatabase.has(userId)) {
        const newUser = {
            userId,
            username,
            balance: 100, // Starting balance for new users (e.g., 100 game credits or 0.1 SOL if representing real SOL)
            lastPlayed: null,
            groupStats: new Map(),
            isNew: true,
        };
        userDatabase.set(userId, newUser);
        console.log(`New user created in simulated DB: ${userId}, Balance: ${newUser.balance}`);
        return newUser;
    }
    const user = userDatabase.get(userId);
    if (username && user.username !== username) { // Update username if changed
        user.username = username;
    }
    user.isNew = false;
    return user;
}

/**
 * Updates a user's balance.
 * @param {string} userId The user's Telegram ID.
 * @param {number} amountChange The amount to change the balance by (can be negative).
 * @param {string} reason For logging purposes (e.g., "won_coinflip", "bet_placed_roulette").
 * @param {string} [chatId] Optional: The chat ID where this balance change occurred, for group stats.
 * @returns {Promise<{success: boolean, newBalance?: number, error?: string}>}
 */
async function updateUserBalance(userId, amountChange, reason = "unknown", chatId) {
    if (!userDatabase.has(userId)) {
        console.warn(`Attempted to update balance for non-existent user: ${userId}`);
        return { success: false, error: "User not found." };
    }
    const user = userDatabase.get(userId);
    if (user.balance + amountChange < 0) {
        console.log(`User ${userId} insufficient balance for ${amountChange} (Reason: ${reason}). Has ${user.balance}`);
        return { success: false, error: "Insufficient balance." };
    }
    user.balance += amountChange;
    console.log(`User ${userId} balance updated by ${amountChange} to ${user.balance} (Reason: ${reason}, Chat: ${chatId || 'N/A'})`);

    // Update group-specific stats if chatId is provided
    if (chatId) {
        if (!user.groupStats.has(chatId)) {
            user.groupStats.set(chatId, { gamesPlayed: 0, totalWagered: 0, netWinLoss: 0 });
        }
        const stats = user.groupStats.get(chatId);
        if (reason.startsWith("bet_placed_")) {
            const wagerAmount = Math.abs(amountChange); // Assuming amountChange is negative for bets placed
            stats.totalWagered += wagerAmount;
            stats.netWinLoss -= wagerAmount; // Bet placed is a loss until resolved
        } else if (reason.startsWith("won_") || reason.startsWith("payout_")) {
            // For wins/payouts, amountChange is positive and includes stake + profit
            // We need to track the *profit* part for netWinLoss if the stake was already deducted
            // This simplified example just adds the positive amountChange to netWinLoss
            // A more robust system would differentiate between stake return and profit.
            stats.netWinLoss += amountChange;
            stats.gamesPlayed += 1; // Increment games played on resolution
        } else if (reason.startsWith("lost_")) {
            // If it's a "lost" reason, the stake was already deducted as a "bet_placed".
            // No further change to netWinLoss here if bet_placed correctly handled it.
            // But we do increment gamesPlayed.
            stats.gamesPlayed += 1;
        }
    }

    return { success: true, newBalance: user.balance };
}

/**
 * Gets or creates a game session for a group chat.
 * @param {string} chatId The group chat's Telegram ID.
 * @param {string} [chatTitle] The group chat's title (optional).
 * @returns {Promise<{chatId: string, chatTitle?: string, currentGame: string | null, currentBetAmount: number | null, participants: string[], lastActivity: Date}>} The group session object.
 */
async function getGroupSession(chatId, chatTitle) {
    if (!groupGameSessions.has(chatId)) {
        const newSession = {
            chatId,
            chatTitle,
            currentGame: null,
            currentBetAmount: null,
            participants: [],
            lastActivity: new Date(),
        };
        groupGameSessions.set(chatId, newSession);
        console.log(`New group session created for chat ${chatId} (${chatTitle || 'No Title'}).`);
        return newSession;
    }
    const session = groupGameSessions.get(chatId);
    if (chatTitle && session.chatTitle !== chatTitle) { // Update title if changed
        session.chatTitle = chatTitle;
    }
    session.lastActivity = new Date(); // Update activity timestamp on access
    return session;
}

/**
 * Updates the current game state for a group chat.
 * @param {string} chatId The group chat's Telegram ID.
 * @param {string | null} gameName The name of the game being started, or null to clear.
 * @param {number | null} betAmount The bet amount for the game, or null.
 * @param {string[]} [participants=[]] Array of user IDs participating.
 * @returns {Promise<boolean>} True if successful.
 */
async function updateGroupGame(chatId, gameName, betAmount, participants = []) {
    if (!groupGameSessions.has(chatId)) {
        // Optionally create if not exists, or require getGroupSession to be called first.
        // For now, let's assume it should exist.
        console.warn(`Attempted to update game for non-existent group session: ${chatId}`);
        await getGroupSession(chatId, "Unknown Group"); // Create it if it doesn't exist
    }
    const session = groupGameSessions.get(chatId);
    session.currentGame = gameName;
    session.currentBetAmount = betAmount;
    session.participants = participants;
    session.lastActivity = new Date();
    console.log(`Group ${chatId} game updated to ${gameName || 'None'}, Bet: ${betAmount}, Participants: ${participants.length}`);
    return true;
}

// End of Part 2
// index.js - Part 3: Telegram Helpers & Basic Game Utilities
// --- VERSION: 1.0.0-group ---

// (Code from Parts 1 & 2 is assumed to be above this)
// ...

// --- Telegram Specific Helper Functions ---

/**
 * Gets a user's display name, prioritizing their first name, then username.
 * For group chats, this helps in addressing users directly.
 * NOTE: This is a simplified version. msg.from contains sender info.
 * For getting member info within a specific group, bot.getChatMember(chatId, userId) would be used.
 * @param {TelegramBot.User} userObject The user object from a Telegram message or callback.
 * @returns {string} A display name for the user.
 */
function getUserDisplayMention(userObject) {
    if (!userObject) {
        return "Anonymous User";
    }
    // For direct mentions in MarkdownV2, use user ID.
    // Name is for display, actual tagging for notification uses the [text](tg://user?id=USER_ID) format.
    const name = userObject.first_name || userObject.username || `User ${userObject.id}`;
    return escapeMarkdownV2(name); // escapeMarkdownV2 from Part 1
}

/**
 * Creates a MarkdownV2 mention link for a user.
 * @param {TelegramBot.User} userObject The user object from a Telegram message or callback.
 * @returns {string} A MarkdownV2 mention string, e.g., "[John Doe](tg://user?id=123456789)"
 */
function createMention(userObject) {
    if (!userObject || !userObject.id) {
        return "Unknown User";
    }
    const displayName = userObject.first_name || userObject.username || `User ${userObject.id}`;
    return `[${escapeMarkdownV2(displayName)}](tg://user?id=${userObject.id})`;
}


// --- Basic Game & Currency Utilities (Placeholders/Simplified) ---

/**
 * Formats a numeric amount into a currency string.
 * Placeholder: Assumes 'credits' for now. Can be adapted for SOL later.
 * @param {number} amount The amount to format.
 * @param {string} [currencySymbol="credits"] The currency symbol or name.
 * @returns {string} Formatted currency string (e.g., "100 credits", "0.5 SOL").
 */
function formatCurrency(amount, currencySymbol = "credits") {
    // For actual SOL, you'd convert lamports to SOL and format precisely.
    // Example: const SOL_DECIMALS = 9; return (amount / Math.pow(10, SOL_DECIMALS)).toFixed(SOL_DECIMALS) + " SOL";
    return `${amount.toLocaleString()} ${currencySymbol}`;
}

/**
 * Simulates rolling a standard N-sided die.
 * @param {number} sides Number of sides on the die (default 6).
 * @returns {number} The result of the die roll.
 */
function rollDie(sides = 6) {
    return Math.floor(Math.random() * sides) + 1;
}

/**
 * Generates a visual representation of dice rolls (text-based).
 * @param {number[]} rolls An array of dice roll results.
 * @returns {string} A string like "Dice: 🎲 3  🎲 5"
 */
function formatDiceRolls(rolls) {
    const diceEmojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"]; // Unicode dice
    // Fallback if using numbers directly or for dice with > 6 sides
    // const diceVisual = rolls.map(roll => `🎲 ${roll}`).join('  ');
    const diceVisual = rolls.map(roll => (roll >= 1 && roll <= 6) ? diceEmojis[roll - 1] : `🎲 ${roll}`).join(' ');
    return `Rolls: ${diceVisual}`;
}

/**
 * Placeholder for generating a unique game ID.
 * @returns {string} A simple unique ID.
 */
function generateGameId() {
    return `game_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
}

console.log("Telegram helper and basic game utilities loaded.");

// End of Part 3
// index.js - Part 4: Simplified Game Logic
// --- VERSION: 1.0.0-group ---

// (Code from Parts 1, 2 & 3 is assumed to be above this)
// ...

// --- Game Logic Functions ---

/**
 * Simulates a coin flip.
 * @returns {{outcome: 'heads' | 'tails', emoji: string}} The result of the flip.
 */
function playCoinFlip() {
    const isHeads = Math.random() < 0.5;
    if (isHeads) {
        return { outcome: 'heads', emoji: '🪙 Heads' };
    } else {
        return { outcome: 'tails', emoji: '🪙 Tails' };
    }
}

/**
 * Simulates rolling a single die (default 6-sided).
 * This can be used as a basis for a simple "highest roll" game in a group.
 * @param {number} [sides=6] Number of sides on the die.
 * @returns {{roll: number, emoji: string}} The result of the roll.
 */
function rollSingleDie(sides = 6) {
    const roll = Math.floor(Math.random() * sides) + 1;
    let emoji = `🎲 ${roll}`; // Default visual
    const diceEmojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"]; // Unicode dice from Part 3
    if (sides === 6 && roll >= 1 && roll <= 6) {
        emoji = diceEmojis[roll - 1];
    }
    return { roll, emoji };
}

/**
 * Simulates a simple "Guess the Number" game round.
 * Bot picks a number, players would guess. This function just picks the number.
 * @param {number} [min=1] The minimum number (inclusive).
 * @param {number} [max=10] The maximum number (inclusive).
 * @returns {{secretNumber: number}} The secret number chosen by the bot.
 */
function pickSecretNumber(min = 1, max = 10) {
    const secretNumber = Math.floor(Math.random() * (max - min + 1)) + min;
    return { secretNumber };
}

// --- Placeholder for more complex game logic ---
// For example, a simple Rock Paper Scissors
const RPS_CHOICES = ['rock', 'paper', 'scissors'];
const RPS_EMOJIS = { rock: '🪨', paper: '📄', scissors: '✂️' };
const RPS_RULES = {
    rock: { beats: 'scissors' },
    paper: { beats: 'rock' },
    scissors: { beats: 'paper' },
};

/**
 * Determines the winner of a Rock Paper Scissors round between two players.
 * @param {string} player1Choice ('rock', 'paper', or 'scissors')
 * @param {string} player2Choice ('rock', 'paper', or 'scissors')
 * @returns {{winner: 'player1' | 'player2' | 'draw', p1Move: string, p2Move: string, p1Emoji: string, p2Emoji: string}}
 */
function playRockPaperScissors(player1Choice, player2Choice) {
    if (!RPS_CHOICES.includes(player1Choice) || !RPS_CHOICES.includes(player2Choice)) {
        throw new Error("Invalid choice for Rock Paper Scissors.");
    }

    let winner;
    if (player1Choice === player2Choice) {
        winner = 'draw';
    } else if (RPS_RULES[player1Choice].beats === player2Choice) {
        winner = 'player1';
    } else {
        winner = 'player2';
    }

    return {
        winner,
        p1Move: player1Choice,
        p2Move: player2Choice,
        p1Emoji: RPS_EMOJIS[player1Choice],
        p2Emoji: RPS_EMOJIS[player2Choice],
    };
}


console.log("Simplified game logic functions loaded.");

// End of Part 4
// index.js - Part 5: Message & Callback Handling, Basic Game Flow
// --- VERSION: 1.0.0-group ---

// (Code from Parts 1, 2, 3 & 4 is assumed to be above this)
// ...

// --- Constants for Cooldowns & Game Flow (Example) ---
const COMMAND_COOLDOWN_MS = 2000; // 2 seconds
const JOIN_GAME_TIMEOUT_MS = 60000; // 60 seconds for users to join a game

console.log("Interaction handlers and game flow logic loading...");

// --- Main Message Handler ---
bot.on('message', async (msg) => {
    // Ignore non-text messages or messages without a sender for this basic version
    if (!msg.text || !msg.from) {
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text;
    const chatType = msg.chat.type; // 'private', 'group', 'supergroup'

    console.log(`[MSG] Chat: ${chatId} (${chatType}), User: ${userId}, Text: "${text}"`);

    // Basic Cooldown Check (userCooldowns from Part 1)
    const now = Date.now();
    if (userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
        console.log(`[COOLDOWN] User ${userId} command ignored due to cooldown.`);
        // Optionally send a "please wait" message, but for simplicity, we'll ignore.
        return;
    }
    userCooldowns.set(userId, now);

    // Simple Command Router
    if (text.startsWith('/')) {
        const [command, ...args] = text.substring(1).split(' ');
        const commandLower = command.toLowerCase();

        // Ensure user exists (getUser from Part 2)
        await getUser(userId, msg.from.username);

        switch (commandLower) {
            case 'start':
            case 'help':
                await handleHelpCommand(chatId, userId, msg.from);
                break;
            case 'balance':
                await handleBalanceCommand(chatId, userId, msg.from);
                break;
            case 'joingame': // Example command to join a game (could also be a button)
                if (chatType === 'group' || chatType === 'supergroup') {
                    await handleJoinGameCommand(chatId, msg.from);
                } else {
                    safeSendMessage(chatId, "You can only join games in a group chat.", { parse_mode: 'MarkdownV2' });
                }
                break;
            case 'startcoinflip': // Example: /startcoinflip <bet_amount>
                if (chatType === 'group' || chatType === 'supergroup') {
                    const betAmount = args[0] ? parseInt(args[0], 10) : 10; // Default bet 10 credits
                    if (isNaN(betAmount) || betAmount <= 0) {
                        safeSendMessage(chatId, "Invalid bet amount. Please specify a positive number.", { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupCoinFlipCommand(chatId, msg.from, betAmount);
                } else {
                    safeSendMessage(chatId, "Group games can only be started in a group chat.", { parse_mode: 'MarkdownV2' });
                }
                break;
            // Admin commands (very basic example)
            case 'adminbroadcast':
                if (ADMIN_USER_ID && userId === ADMIN_USER_ID) {
                    const broadcastMessage = args.join(' ');
                    if (broadcastMessage) {
                        // In a real scenario, you'd iterate over all known chatIds or userIds
                        safeSendMessage(chatId, `Admin Broadcast (simulated): ${escapeMarkdownV2(broadcastMessage)}`, { parse_mode: 'MarkdownV2' });
                        console.log(`Admin ${userId} broadcasted: ${broadcastMessage}`);
                    } else {
                        safeSendMessage(chatId, "Usage: /adminbroadcast <message>", { parse_mode: 'MarkdownV2' });
                    }
                } else {
                    safeSendMessage(chatId, "You are not authorized for this command.", { parse_mode: 'MarkdownV2' });
                }
                break;
            default:
                safeSendMessage(chatId, "Unknown command. Try /help.", { parse_mode: 'MarkdownV2' });
        }
    }
    // Could add non-command message handling here if needed (e.g., for stateful interactions)
});

// --- Callback Query Handler ---
bot.on('callback_query', async (callbackQuery) => {
    const msg = callbackQuery.message;
    const userId = String(callbackQuery.from.id); // User who clicked the button
    const chatId = String(msg.chat.id);
    const data = callbackQuery.data; // e.g., 'join_coinflip:game123'

    console.log(`[CBQ] Chat: ${chatId}, User: ${userId}, Data: "${data}"`);
    bot.answerCallbackQuery(callbackQuery.id); // Acknowledge the button press

    // Ensure user exists (getUser from Part 2)
    await getUser(userId, callbackQuery.from.username);

    const [action, ...params] = data.split(':');

    switch (action) {
        case 'join_group_game':
            const gameIdToJoin = params[0]; // This would be an ID for the specific game instance
            await handleJoinGameCallback(chatId, callbackQuery.from, gameIdToJoin);
            break;
        case 'cancel_game_creation':
            const gameIdToCancel = params[0];
            await handleCancelGameCreationCallback(chatId, callbackQuery.from, gameIdToCancel, msg.message_id);
            break;
        // Add more callback actions as needed (e.g., player choices within a game)
        default:
            console.log(`Unknown callback action: ${action}`);
            safeSendMessage(chatId, "Sorry, that button action is not recognized or has expired.", { parse_mode: 'MarkdownV2' });
    }
});

// --- Command Handler Functions ---

async function handleHelpCommand(chatId, userId, userObject) {
    const userMention = createMention(userObject); // createMention from Part 3
    const helpText =
        `👋 Hello ${userMention}\\!\n\n` +
        `I'm the *Solana Group Chat Casino Bot* v${BOT_VERSION}\\.\n\n` + // BOT_VERSION from Part 1
        `Here are some commands you can use:\n` +
        `▫️ /help \\- Shows this help message\n` +
        `▫️ /balance \\- Check your current game balance\n` +
        `▫️ /startcoinflip <amount> \\- Start a coin flip game in a group \\(e\\.g\\., \`/startcoinflip 10\`\\)\n` +
        `▫️ /joingame \\- Join an active game in this group \\(if one is waiting for players\\)\n\n` +
        `More games and features coming soon\\!`;
    safeSendMessage(chatId, escapeMarkdownV2(helpText), { parse_mode: 'MarkdownV2' }); // safeSendMessage from Part 1
}

async function handleBalanceCommand(chatId, userId, userObject) {
    const user = await getUser(userId, userObject.username); // getUser from Part 2
    const userMention = createMention(userObject);
    const balanceMessage = `${userMention}, your current balance is: *${escapeMarkdownV2(formatCurrency(user.balance))}*\\.`; // formatCurrency from Part 3
    safeSendMessage(chatId, balanceMessage, { parse_mode: 'MarkdownV2' });
}

// --- Group Game Flow Functions (Example: Coinflip) ---

/**
 * Initiates a group coin flip game.
 * @param {string} chatId
 * @param {TelegramBot.User} initiatorUser
 * @param {number} betAmount
 */
async function handleStartGroupCoinFlipCommand(chatId, initiatorUser, betAmount) {
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, initiatorUser.username ? "" : "Unknown Group"); // getGroupSession from Part 2
    const gameId = generateGameId(); // generateGameId from Part 3

    if (gameSession.currentGame) {
        safeSendMessage(chatId, `A game of *${escapeMarkdownV2(gameSession.currentGame)}* is already active in this group\\. Please wait for it to finish or be cancelled\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Check initiator's balance
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        safeSendMessage(chatId, `${createMention(initiatorUser)}, you don't have enough balance \\(${escapeMarkdownV2(formatCurrency(initiator.balance))}\\) to start a game for ${escapeMarkdownV2(formatCurrency(betAmount))}\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Deduct initiator's bet (or "hold" it)
    await updateUserBalance(initiatorId, -betAmount, `bet_placed_groupcoinflip_init`, chatId); // updateUserBalance from Part 2
    activeGames.set(gameId, { // activeGames from Part 1
        type: 'coinflip',
        chatId: chatId,
        initiatorId: initiatorId,
        initiatorMention: createMention(initiatorUser),
        betAmount: betAmount,
        participants: [{ userId: initiatorId, choice: null, mention: createMention(initiatorUser) }], // choice can be 'heads' or 'tails'
        status: 'waiting_opponent', // or 'waiting_choices' if multiple players can pick sides
        creationTime: Date.now()
    });

    // Update group session to show a game is starting
    await updateGroupGame(chatId, `Coinflip (${formatCurrency(betAmount)})`, betAmount, [initiatorId]); // updateGroupGame from Part 2

    const joinMessage =
        `${createMention(initiatorUser)} has started a *Coin Flip Game* for ${escapeMarkdownV2(formatCurrency(betAmount))}\\!\n\n` +
        `Who wants to play against them\\?\n` +
        `Click "Join Game" to accept the challenge\\.`;

    const keyboard = {
        inline_keyboard: [
            [{ text: "🪙 Join Game!", callback_data: `join_group_game:${gameId}` }],
            [{ text: "❌ Cancel Game Setup", callback_data: `cancel_game_creation:${gameId}` }]
        ]
    };
    await safeSendMessage(chatId, escapeMarkdownV2(joinMessage), { parse_mode: 'MarkdownV2', reply_markup: keyboard });

    // Set a timeout to auto-cancel the game if no one joins
    setTimeout(async () => {
        const gameData = activeGames.get(gameId);
        if (gameData && gameData.status === 'waiting_opponent') {
            console.log(`Game ${gameId} in chat ${chatId} timed out waiting for opponent.`);
            // Refund initiator
            await updateUserBalance(gameData.initiatorId, gameData.betAmount, "refund_groupcoinflip_timeout", chatId);
            activeGames.delete(gameId);
            await updateGroupGame(chatId, null, null, []); // Clear game from group session
            safeSendMessage(chatId, `The Coin Flip game started by ${gameData.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} has expired due to no one joining\\. The bet has been refunded\\.`, { parse_mode: 'MarkdownV2' });
        }
    }, JOIN_GAME_TIMEOUT_MS);
}


/**
 * Handles a user joining a group game via a button.
 * @param {string} chatId
 * @param {TelegramBot.User} joinerUser
 * @param {string} gameId
 */
async function handleJoinGameCallback(chatId, joinerUser, gameId) {
    const joinerId = String(joinerUser.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== chatId) {
        safeSendMessage(chatId, `${createMention(joinerUser)}, this game is no longer available or not for this chat\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    if (gameData.status !== 'waiting_opponent') {
        safeSendMessage(chatId, `${createMention(joinerUser)}, this game is already full or has started\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    if (gameData.initiatorId === joinerId) {
        safeSendMessage(chatId, `${createMention(joinerUser)}, you can't join your own game as an opponent\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Check joiner's balance
    const joiner = await getUser(joinerId);
    if (joiner.balance < gameData.betAmount) {
        safeSendMessage(chatId, `${createMention(joinerUser)}, you don't have enough balance \\(${escapeMarkdownV2(formatCurrency(joiner.balance))}\\) to join this ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} game\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Deduct joiner's bet
    await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_groupcoinflip_join`, chatId);

    gameData.participants.push({ userId: joinerId, choice: null, mention: createMention(joinerUser) });
    gameData.status = 'playing'; // Or 'waiting_choices' if they need to pick sides
    activeGames.set(gameId, gameData);

    // --- For a simple 2-player coinflip, we can resolve it immediately ---
    const initiator = gameData.participants[0];
    const opponent = gameData.participants[1];

    // Let's assign choices (e.g., initiator is heads, opponent is tails) or ask them
    // For simplicity here, let's make initiator always 'heads' for this auto-resolve
    initiator.choice = 'heads';
    opponent.choice = 'tails'; // This means opponent wins if it's tails

    const coinResult = playCoinFlip(); // from Part 4
    let winnerParticipant;
    let loserParticipant;

    if (coinResult.outcome === initiator.choice) {
        winnerParticipant = initiator;
        loserParticipant = opponent;
    } else {
        winnerParticipant = opponent;
        loserParticipant = initiator;
    }

    const winnings = gameData.betAmount * 2; // Total pot
    await updateUserBalance(winnerParticipant.userId, winnings, `won_groupcoinflip_resolved`, chatId);

    const resultMessage =
        `*Coin Flip Resolved\\!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n` +
        `${gameData.initiatorMention} (Initiator) vs ${opponent.mention} (Opponent)\n` +
        `The coin landed on: *${escapeMarkdownV2(coinResult.emoji)}*\\!\n\n` +
        `🎉 ${winnerParticipant.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} (total ${escapeMarkdownV2(formatCurrency(winnings))} payout)\\!\n` +
        `😢 ${loserParticipant.mention} lost their bet\\.`;

    await safeSendMessage(chatId, resultMessage, { parse_mode: 'MarkdownV2' });

    // Clean up
    activeGames.delete(gameId);
    await updateGroupGame(chatId, null, null, []);
}

/**
 * Handles cancellation of game creation by the initiator.
 * @param {string} chatId
 * @param {TelegramBot.User} cancellerUser
 * @param {string} gameId
 * @param {number} originalMessageId The ID of the message with the "Join/Cancel" buttons.
 */
async function handleCancelGameCreationCallback(chatId, cancellerUser, gameId, originalMessageId) {
    const cancellerId = String(cancellerUser.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== chatId) {
        safeSendMessage(chatId, `This game is no longer available for cancellation\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    if (gameData.initiatorId !== cancellerId) {
        safeSendMessage(chatId, `${createMention(cancellerUser)}, only the game initiator (${gameData.initiatorMention}) can cancel the game setup\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    if (gameData.status !== 'waiting_opponent') {
        safeSendMessage(chatId, `This game cannot be cancelled at this stage \\(current status: ${escapeMarkdownV2(gameData.status)}\\)\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Refund initiator's bet
    await updateUserBalance(gameData.initiatorId, gameData.betAmount, "refund_groupcoinflip_cancelled", chatId);

    activeGames.delete(gameId);
    await updateGroupGame(chatId, null, null, []); // Clear game from group session

    const cancellationMessage = `${gameData.initiatorMention} has cancelled the Coin Flip game setup for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\\. The bet has been refunded\\.`;
    if (originalMessageId) {
        bot.editMessageText(escapeMarkdownV2(cancellationMessage), {
            chat_id: chatId,
            message_id: originalMessageId,
            parse_mode: 'MarkdownV2',
            reply_markup: {} // Remove buttons
        }).catch(e => {
            console.warn(`Error editing game cancellation message: ${e.message}`);
            safeSendMessage(chatId, escapeMarkdownV2(cancellationMessage), { parse_mode: 'MarkdownV2' });
        });
    } else {
        safeSendMessage(chatId, escapeMarkdownV2(cancellationMessage), { parse_mode: 'MarkdownV2' });
    }
    console.log(`Game ${gameId} in chat ${chatId} cancelled by initiator ${cancellerId}.`);
}


console.log("Message and callback handlers loaded.");

// End of Part 5
// index.js - Part 6: Startup, Shutdown, and Basic Error Handling
// --- VERSION: 1.0.0-group ---

// (Code from Parts 1, 2, 3, 4 & 5 is assumed to be above this)
// ...

console.log("Loading Part 6: Startup, Shutdown, and Error Handling...");

// --- Placeholder for Background Tasks ---
/**
 * Example: A function that could be called periodically for cleanup.
 */
async function runBackgroundTasks() {
    console.log("[BACKGROUND] Running periodic background tasks...");

    // Example: Clean up old games from activeGames or groupGameSessions
    const now = Date.now();
    const gameTimeout = JOIN_GAME_TIMEOUT_MS * 2; // e.g., double the join timeout

    for (const [gameId, gameData] of activeGames.entries()) {
        if (now - gameData.creationTime > gameTimeout && gameData.status !== 'playing') {
            console.log(`[BACKGROUND] Cleaning up stale game ${gameId} in chat ${gameData.chatId} (status: ${gameData.status}).`);
            if (gameData.status === 'waiting_opponent' && gameData.initiatorId) {
                // Refund initiator if game was waiting and timed out (though timeout in handleStartGroupCoinFlipCommand should cover this)
                await updateUserBalance(gameData.initiatorId, gameData.betAmount, "refund_stale_game_cleanup", gameData.chatId);
                safeSendMessage(gameData.chatId, `The game initiated by ${gameData.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} was automatically cleared due to inactivity\\. The bet has been refunded\\.`, { parse_mode: 'MarkdownV2' });
            }
            activeGames.delete(gameId);
            // Also clear from groupGameSessions if it was the active one
            const groupSession = await getGroupSession(gameData.chatId);
            if (groupSession.currentGame && groupSession.participants.includes(gameData.initiatorId)) { // Basic check
                await updateGroupGame(gameData.chatId, null, null, []);
            }
        }
    }

    // Example: Clean up very old group sessions that have no active game
    for (const [chatId, sessionData] of groupGameSessions.entries()) {
        if (!sessionData.currentGame && (now - sessionData.lastActivity.getTime()) > (JOIN_GAME_TIMEOUT_MS * 10)) {
            console.log(`[BACKGROUND] Cleaning up inactive group session for chat ${chatId}.`);
            groupGameSessions.delete(chatId);
        }
    }

    console.log("[BACKGROUND] Background tasks complete.");
}

// We are not starting a setInterval for runBackgroundTasks in this simplified version,
// but this is where you would set it up if needed:
// setInterval(runBackgroundTasks, 30 * 60 * 1000); // e.g., every 30 minutes

// --- Error Handling ---
process.on('uncaughtException', (error, origin) => {
    console.error(`\n🚨🚨 UNCAUGHT EXCEPTION 🚨🚨`);
    console.error(`Origin: ${origin}`);
    console.error(error);
    // For a real bot, you might notify an admin here.
    // safeSendMessage(ADMIN_USER_ID, `URGENT: Uncaught Exception: ${error.message}`).catch();
    // Consider if you want to exit or try to continue (exiting is often safer for unhandled states)
    // process.exit(1); // Uncomment to exit on uncaught exceptions
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`\n🔥🔥 UNHANDLED PROMISE REJECTION 🔥🔥`);
    console.error('Reason:', reason);
    console.error('Promise:', promise);
    // For a real bot, notify admin.
    // const reasonMsg = reason instanceof Error ? reason.message : String(reason);
    // safeSendMessage(ADMIN_USER_ID, `ALERT: Unhandled Rejection: ${reasonMsg}`).catch();
});

// --- Shutdown Handling ---
let isShuttingDown = false;
async function shutdown(signal) {
    if (isShuttingDown) {
        console.log("Shutdown already in progress...");
        return;
    }
    isShuttingDown = true;
    console.log(`\n🚦 Received ${signal}. Starting graceful shutdown...`);

    // 1. Stop receiving new messages/callbacks (polling will be stopped by process.exit)
    if (bot && bot.isPolling()) {
        try {
            await bot.stopPolling({ cancel: true }); // cancel pending getUpdates
            console.log("Telegram polling stopped.");
        } catch (error) {
            console.error("Error stopping Telegram polling:", error.message);
        }
    }

    // 2. Process any ongoing tasks (e.g., wait for activeGames to resolve or save state)
    //    For this simple version, we don't have complex queues to wait for.
    console.log("No complex queues to clear in this version.");

    // 3. Save any critical data (if not using a persistent DB that saves on each transaction)
    //    Our simulated DB is in-memory, so data will be lost. A real DB handles this.
    console.log("Simulated in-memory data will be lost on shutdown.");

    // 4. Notify admin (optional)
    if (ADMIN_USER_ID) { // ADMIN_USER_ID from Part 1
        await safeSendMessage(ADMIN_USER_ID, `Bot v${BOT_VERSION} is shutting down (Signal: ${signal}).`).catch(e => console.error("Failed to send shutdown notification:", e.message));
    }

    console.log("✅ Shutdown complete. Exiting.");
    process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
}

process.on('SIGINT', () => shutdown('SIGINT')); // Ctrl+C
process.on('SIGTERM', () => shutdown('SIGTERM')); // kill command

// --- Main Startup Function ---
async function main() {
    console.log(`\n🚀 Initializing Solana Group Chat Casino Bot v${BOT_VERSION} (Simplified)...`);
    console.log(`Current Time: ${new Date().toISOString()}`);
    console.log("Connecting to Telegram...");

    try {
        const me = await bot.getMe();
        console.log(`✅ Successfully connected to Telegram as @${me.username} (ID: ${me.id})`);

        // Announce bot is ready (e.g., to admin or a specific channel if configured)
        if (ADMIN_USER_ID) {
            await safeSendMessage(ADMIN_USER_ID, `Bot v${BOT_VERSION} (Simplified Group Version) has started successfully! Polling for messages.`, { parse_mode: 'MarkdownV2' });
        }
        console.log(`🎉 Bot is now running and polling for messages! Press Ctrl+C to stop.`);

    } catch (error) {
        console.error("❌ CRITICAL: Failed to connect to Telegram or get bot info.", error);
        process.exit(1);
    }

    // Any other one-time startup tasks would go here.
    // For example, loading initial data if we weren't using a fresh in-memory store.
}

// Start the bot
main().catch(error => {
    console.error("❌ CRITICAL ERROR DURING BOT STARTUP:", error);
    process.exit(1);
});

// End of Part 6
// End of index.js
