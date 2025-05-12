// index.js - Group Chat Casino Bot
// --- VERSION: 1.0.0-group-diceescalator-deepseek-v2 --- (Modified as per user request)

//---------------------------------------------------------------------------
// index.js - Part 1: Core Imports & Basic Setup
//---------------------------------------------------------------------------
console.log("Loading Part 1: Core Imports & Basic Setup...");

import 'dotenv/config';
import TelegramBot from 'node-telegram-bot-api';

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_USER_ID = process.env.ADMIN_USER_ID;

const DICES_HELPER_BOT_USERNAME = process.env.DICES_HELPER_BOT_USERNAME;
// NOTE: DICES_HELPER_BOT_ID might be overwritten by auto-fetch below
let DICES_HELPER_BOT_ID = process.env.DICES_HELPER_BOT_ID;

if (!BOT_TOKEN) {
    console.error("FATAL ERROR: BOT_TOKEN is not defined.");
    process.exit(1);
}
console.log("BOT_TOKEN loaded successfully.");
if (ADMIN_USER_ID) console.log(`Admin User ID: ${ADMIN_USER_ID} loaded.`);
else console.log("INFO: No ADMIN_USER_ID set (optional).");

if (DICES_HELPER_BOT_ID) {
    console.log(`Configured DICES_HELPER_BOT_ID: ${DICES_HELPER_BOT_ID}`);
} else if (DICES_HELPER_BOT_USERNAME) {
    console.log(`Configured DICES_HELPER_BOT_USERNAME: @${DICES_HELPER_BOT_USERNAME}. ID will be fetched if possible.`);
} else {
    console.warn("WARNING: Neither DICES_HELPER_BOT_ID nor DICES_HELPER_BOT_USERNAME is set. Dice Escalator game's helper bot interactions will likely fail.");
}

const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("Telegram Bot instance created");

// --- User Request Step 3: Auto-fetch roller bot ID if only username is provided ---
if (!DICES_HELPER_BOT_ID && DICES_HELPER_BOT_USERNAME && DICES_HELPER_BOT_USERNAME !== "YourDiceHelperBotUsername") {
    // Need to wrap await in an async IIFE or handle promise
    (async () => {
        try {
            console.log(`[AUTO_CONFIG] Attempting to fetch ID for @${DICES_HELPER_BOT_USERNAME}...`);
            const rollerBot = await bot.getChat(`@${DICES_HELPER_BOT_USERNAME}`);
            // Assign to the variable directly, modifying process.env is less standard
            DICES_HELPER_BOT_ID = String(rollerBot.id);
            console.log(`[AUTO_CONFIG] Fetched and set DICES_HELPER_BOT_ID: ${DICES_HELPER_BOT_ID}`);
        } catch (error) {
            console.error(`[AUTO_CONFIG_ERROR] Failed to get ID for @${DICES_HELPER_BOT_USERNAME}:`, error.message);
            // Optionally set DICES_HELPER_BOT_ID to null or undefined explicitly if fetch fails
            DICES_HELPER_BOT_ID = undefined;
        }
    })(); // Immediately invoke the async function
}
// --- End User Request Step 3 ---


console.log("Telegram Bot instance created and configured for polling."); // Original log moved after potential async fetch


const BOT_VERSION = '1.0.0-group-diceescalator-deepseek-v2-mod1'; // Updated version marker
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;

let activeGames = new Map();
let userCooldowns = new Map();

console.log(`Group Chat Casino Bot v${BOT_VERSION} initializing...`);
console.log(`Current system time: ${new Date().toISOString()}`);

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
    // Markdown escaping is handled here ONLY if parse_mode is MarkdownV2
    // Important: If text is already escaped, don't escape again. Assume input text is raw.
    if (finalOptions.parse_mode === 'MarkdownV2') {
       // NOTE: We escape the raw text. If pre-escaped text is passed, this might double-escape.
       // The original function escaped only if parse_mode was MarkdownV2, which is maintained.
       // However, many internal messages are constructed with manual escaping (`escapeMarkdownV2`).
       // This function should ideally handle raw text and escape based on parse_mode.
       // For now, maintaining original behavior: escaping here based on option.
        messageToSend = escapeMarkdownV2(text);
    }

    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsis = "... (message truncated)";
        // Escape the ellipsis itself if needed for the specific parse mode
        let escapedEllipsis = (finalOptions.parse_mode === 'MarkdownV2') ? escapeMarkdownV2(ellipsis) : ellipsis;
        const truncateAt = MAX_MARKDOWN_V2_MESSAGE_LENGTH - escapedEllipsis.length;
        // Ensure truncateAt is not negative
        if (truncateAt > 0) {
           messageToSend = messageToSend.substring(0, truncateAt) + escapedEllipsis;
        } else {
            // If the ellipsis itself is too long, just truncate crudely
            messageToSend = messageToSend.substring(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH);
        }
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
// index.js - Part 2: Simulated Database Operations & Data Management
//---------------------------------------------------------------------------
console.log("Loading Part 2: Simulated Database Operations & Data Management...");
const userDatabase = new Map();
const groupGameSessions = new Map();
console.log("In-memory data stores (userDatabase, groupGameSessions) initialized.");

async function getUser(userId, username) {
    const userIdStr = String(userId);
    if (!userDatabase.has(userIdStr)) {
        const newUser = {
            userId: userIdStr, username, balance: 1000, lastPlayed: null,
            groupStats: new Map(), isNew: true,
        };
        userDatabase.set(userIdStr, newUser);
        console.log(`[DB_SIM] New user: ${userIdStr} (@${username||'N/A'}), Bal: ${newUser.balance}`);
        return { ...newUser }; // Return a copy
    }
    const user = userDatabase.get(userIdStr);
    if (username && user.username !== username) {
        user.username = username;
        // console.log(`[DB_SIM] Username updated for ${userIdStr} to @${username}`);
    }
    user.isNew = false;
    return { ...user, groupStats: new Map(user.groupStats) }; // Return a copy, including a copy of groupStats
}

async function updateUserBalance(userId, amountChange, reason = "unknown_transaction", chatId) {
    const userIdStr = String(userId);
    if (!userDatabase.has(userIdStr)) {
        console.warn(`[DB_SIM_ERROR] Update balance for non-existent user: ${userIdStr}`);
        return { success: false, error: "User not found." };
    }
    const user = userDatabase.get(userIdStr);
    const proposedBalance = user.balance + amountChange;

    if (proposedBalance < 0) {
        console.log(`[DB_SIM] User ${userIdStr} insufficient balance (${user.balance}) for deduction of ${Math.abs(amountChange)} (Reason: ${reason}).`);
        return { success: false, error: "Insufficient balance." };
    }
    user.balance = proposedBalance;
    user.lastPlayed = new Date();
    console.log(`[DB_SIM] User ${userIdStr} balance: ${user.balance} (Change: ${amountChange}, Reason: ${reason}, Chat: ${chatId||'N/A'})`);
    if (chatId) {
        const chatIdStr = String(chatId);
        if (!user.groupStats.has(chatIdStr)) user.groupStats.set(chatIdStr, { gamesPlayed: 0, totalWagered: 0, netWinLoss: 0 });
        const stats = user.groupStats.get(chatIdStr);
        // Simplified stat tracking based on reason keywords
        if (reason.toLowerCase().includes("bet_")) { // Covers init, placed etc.
             const wager = Math.abs(amountChange); stats.totalWagered += wager; stats.netWinLoss -= wager;
        } else if (reason.toLowerCase().includes("won_") || reason.toLowerCase().includes("payout_") || reason.toLowerCase().includes("cashout")) {
             stats.netWinLoss += amountChange;
             // Don't increment gamesPlayed on cashout or refunds from wins
             if (!reason.toLowerCase().includes("refund_") && !reason.toLowerCase().includes("cashout") && amountChange > 0) stats.gamesPlayed += 1;
        } else if (reason.toLowerCase().includes("lost_")) { // Player lost bet (not bust)
             // netWinLoss already adjusted by the bet placement
             if (!reason.toLowerCase().includes("bust")) stats.gamesPlayed += 1;
        } else if (reason.toLowerCase().includes("refund_")) {
             // Refund means adding back the bet, adjusting netWinLoss
             stats.netWinLoss += amountChange;
             // Should potentially decrement totalWagered if refund cancels bet? Assumes refund is separate event for now.
        }
    }
    return { success: true, newBalance: user.balance };
}

async function getGroupSession(chatId, chatTitle) {
    const chatIdStr = String(chatId);
    if (!groupGameSessions.has(chatIdStr)) {
        const newSession = {
            chatId: chatIdStr, chatTitle, currentGameId: null, currentGameType: null,
            currentBetAmount: null, lastActivity: new Date(),
        };
        groupGameSessions.set(chatIdStr, newSession);
        console.log(`[DB_SIM] New group session: ${chatIdStr} (${chatTitle||'Untitled'}).`);
        return { ...newSession };
    }
    const session = groupGameSessions.get(chatIdStr);
    if (chatTitle && session.chatTitle !== chatTitle) session.chatTitle = chatTitle;
    session.lastActivity = new Date();
    return { ...session };
}

async function updateGroupGameDetails(chatId, gameId, gameType, betAmount) {
    const chatIdStr = String(chatId);
    if (!groupGameSessions.has(chatIdStr)) {
        // If session doesn't exist, create it before updating
        console.warn(`[DB_SIM] Group session ${chatIdStr} not found while trying to update game details. Creating one.`);
        await getGroupSession(chatIdStr, "Unknown Group (Auto-created)");
    }
    const session = groupGameSessions.get(chatIdStr);
    // Ensure session exists after potential creation attempt
    if (!session) {
       console.error(`[DB_SIM_ERROR] Failed to get/create session for ${chatIdStr} during game update.`);
       return false;
    }
    session.currentGameId = gameId;
    session.currentGameType = gameType;
    session.currentBetAmount = betAmount;
    session.lastActivity = new Date();
    console.log(`[DB_SIM] Group ${chatIdStr} game updated -> GameID: ${gameId||'None'}, Type: ${gameType||'None'}, Bet: ${betAmount !== null ? formatCurrency(betAmount) : 'N/A'}`);
    return true;
}
console.log("Part 2: Simulated Database Operations & Data Management - Complete.");

//---------------------------------------------------------------------------
// index.js - Part 3: Telegram Helpers & Basic Game Utilities
//---------------------------------------------------------------------------
console.log("Loading Part 3: Telegram Helpers & Basic Game Utilities...");
function getEscapedUserDisplayName(userObject) {
    if (!userObject) return escapeMarkdownV2("Anonymous User");
    // Prefer first name, fallback to username, fallback to User ID
    const name = userObject.first_name || userObject.username || `User ${userObject.id}`;
    return escapeMarkdownV2(name);
}
function createUserMention(userObject) {
    if (!userObject || !userObject.id) return escapeMarkdownV2("Unknown User");
    // Use first name if available, otherwise username, fallback to generic 'User ID'
    const displayName = userObject.first_name || userObject.username || `User ${userObject.id}`;
    // Ensure the display name itself is escaped for MarkdownV2 link text
    return `[${escapeMarkdownV2(displayName)}](tg://user?id=${userObject.id})`;
}
function formatCurrency(amount, currencyName = "credits") {
    let num = Number(amount); if (isNaN(num)) num = 0;
    // Format with commas, show decimals only if needed
    return `${num.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: (num % 1 === 0 ? 0 : 2) })} ${currencyName}`;
}
function rollDie(sides = 6) {
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6;
    return Math.floor(Math.random() * sides) + 1;
}
function formatDiceRolls(rolls) {
    const diceEmojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"]; // Emojis for 1-6
    // Map rolls to emojis if 1-6, otherwise show 🎲 + number
    const diceVisuals = rolls.map(roll => (roll >= 1 && roll <= 6) ? diceEmojis[roll - 1] : `🎲${roll}`);
    return `Rolls: ${diceVisuals.join(' ')}`; // Join with space
}
function generateGameId() {
    const timestamp = Date.now();
    const randomSuffix = Math.random().toString(36).substring(2, 9); // Short random string
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
function determineDieRollOutcome(sides = 6) { // For BOT's internal rolls
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6;
    const roll = Math.floor(Math.random() * sides) + 1;
    const emojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"];
    return { roll: roll, emoji: (roll >= 1 && roll <= 6) ? emojis[roll - 1] : `🎲${roll}` };
}
const DICE_ESCALATOR_BUST_ON = 1; // Roll 1 = Bust
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
    // Ensure choices are valid keys (case-insensitive check)
    const c1_key = String(choice1).toUpperCase();
    const c2_key = String(choice2).toUpperCase();

    if (!RPS_CHOICES[c1_key] || !RPS_CHOICES[c2_key]) {
        console.warn(`[RPS_WARN] Invalid choices detected: P1='${choice1}', P2='${choice2}'`);
        return { result: 'error', description: "Invalid choices were provided.", choice1, choice1Emoji: '❔', choice2, choice2Emoji: '❔' };
    }

    const c1 = RPS_CHOICES[c1_key]; // Get normalized choice value
    const c2 = RPS_CHOICES[c2_key];
    const c1E = RPS_EMOJIS[c1]; // Get corresponding emoji
    const c2E = RPS_EMOJIS[c2];

    if (c1 === c2) {
        return { result: 'draw', description: `${c1E} ${c1} vs ${c2E} ${c2}. It's a Draw!`, choice1: c1, choice1Emoji: c1E, choice2: c2, choice2Emoji: c2E };
    }
    if (RPS_RULES[c1].beats === c2) {
        return { result: 'win1', description: `${c1E} ${c1} ${RPS_RULES[c1].verb} ${c2E} ${c2}. Player 1 wins!`, choice1: c1, choice1Emoji: c1E, choice2: c2, choice2Emoji: c2E };
    }
    // If not draw and p1 doesn't win, p2 must win
    return { result: 'win2', description: `${c2E} ${c2} ${RPS_RULES[c2].verb} ${c1E} ${c1}. Player 2 wins!`, choice1: c1, choice1Emoji: c1E, choice2: c2, choice2Emoji: c2E };
}
console.log("Part 4: Simplified Game Logic - Complete.");

//---------------------------------------------------------------------------
// index.js - Part 5: Message & Callback Handling, Basic Game Flow
//---------------------------------------------------------------------------
console.log("Loading Part 5: Message & Callback Handling, Basic Game Flow...");

const COMMAND_COOLDOWN_MS = 2000; // 2 seconds
const JOIN_GAME_TIMEOUT_MS = 60000; // 60 seconds
const DICE_ESCALATOR_TIMEOUT_MS = 120000; // 120 seconds (for player action? Needs review)
const MIN_BET_AMOUNT = 5;
const MAX_BET_AMOUNT = 1000;
const DICE_ESCALATOR_BOT_ROLLS = 3; // Max rolls for bot in Dice Escalator

// --- Main Message Handler ---
bot.on('message', async (msg) => {
    // --- START EXTREMELY AGGRESSIVE RAW MESSAGE LOG ---
    if (msg && msg.from && msg.chat) {
        console.log(`[ULTRA_RAW_LOG] Text: "${msg.text || 'N/A'}", FromID: ${msg.from.id}, User: @${msg.from.username || 'N/A'}, IsBot: ${msg.from.is_bot}, ChatID: ${msg.chat.id}`);
    } else {
        console.log(`[ULTRA_RAW_LOG] Received incomplete/malformed message object. Msg:`, JSON.stringify(msg).substring(0, 200));
        return; // Exit if essential message parts are missing
    }
    // --- END EXTREMELY AGGRESSIVE RAW MESSAGE LOG ---

    // !!!!! --- DIAGNOSTIC CHECK from previous step (kept as requested) --- !!!!!
    // Check if DICES_HELPER_BOT_ID is loaded and if the incoming message matches, REGARDLESS of game state
    if (DICES_HELPER_BOT_ID && msg.from && String(msg.from.id) === DICES_HELPER_BOT_ID) {
        console.log(`\n✅✅✅ DIAGNOSTIC: Message received from CONFIGURED HELPER BOT ID (${DICES_HELPER_BOT_ID}). Text: "${msg.text || 'N/A'}" ✅✅✅\n`);
    }
    // !!!!! --- END DIAGNOSTIC CHECK --- !!!!!


    if (!msg.from) return; // Basic check: message must have a sender

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text || ""; // Ensure text is always a string
    const chatType = msg.chat.type; // 'private', 'group', 'supergroup', 'channel'
    const messageId = msg.message_id;

    // --- User Request Step 2: Modified Helper Bot Detection Code ---
    // --- HELPER BOT MESSAGE IDENTIFICATION AND PROCESSING ---
    let isFromHelperBot = false;
    if (msg.from.is_bot) {
        console.log(`[HELPER_CHECK] Message from bot detected:`, {
            id: msg.from.id,
            username: msg.from.username,
            text: msg.text // Use original msg.text here for logging
        });

        // Priority check by ID if configured
        if (DICES_HELPER_BOT_ID && String(msg.from.id) === String(DICES_HELPER_BOT_ID)) {
            isFromHelperBot = true;
            console.log(`[HELPER_ID_MATCH] Confirmed helper bot by ID: ${msg.from.id}`);
        }
        // Fallback to username check
        else if (DICES_HELPER_BOT_USERNAME && msg.from.username &&
                 msg.from.username.toLowerCase() === DICES_HELPER_BOT_USERNAME.toLowerCase()) {
            isFromHelperBot = true;
            console.log(`[HELPER_USERNAME_MATCH] Confirmed helper bot by username: @${msg.from.username}`);
        }

        // If it's a bot message, but NOT the helper bot AND NOT our own bot, ignore it.
        const botInfo = await bot.getMe(); // Get self bot info
        if (!isFromHelperBot && String(msg.from.id) !== String(botInfo.id)) {
             console.log(`[MSG_IGNORE] Ignoring message from other bot (ID: ${msg.from.id}, User: @${msg.from.username||'N/A'}).`);
             return; // Ignore messages from other bots
        }

        // NOTE: The original code had processing logic here. It was removed as per implicit instruction
        // because Step 5 adds similar logic before the command processing block.
        // The 'return;' that was here previously if isFromHelperBot was true is now handled by Step 5's logic.

        // Logging validation result:
        if (isFromHelperBot) {
             console.log(`[HELPER_MSG_VALIDATED] Identified helper message for potential processing: "${msg.text}"`);
        }
    }
    // --- End User Request Step 2 Modification ---


    // --- Cooldown Check (Apply only to non-bot user commands) ---
    if (!msg.from.is_bot) {
        const now = Date.now();
        if (text.startsWith('/') && userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
            console.log(`[COOLDOWN] User ${userId} command ("${text}") ignored due to cooldown.`);
            // Optionally notify the user they are on cooldown (can be spammy)
            // await safeSendMessage(chatId, `${createUserMention(msg.from)}, please wait a moment before using another command.`, { parse_mode: 'MarkdownV2' });
            return; // Ignore command if user is on cooldown
        }
        // Reset cooldown timestamp if the message is a command
        if (text.startsWith('/')) {
            userCooldowns.set(userId, now);
        }
    }


    // --- User Request Step 5: Critical Fix for Message Handling ---
    // Add this BEFORE the command processing section
    if (isFromHelperBot && msg.text) { // Ensure helper flag is true and message has text
        console.log(`[HELPER_MSG_PROCESSING] Handling helper message in chat ${chatId}: "${msg.text}"`);
        const gameSession = await getGroupSession(chatId); // Fetch current session for the chat

        if (gameSession?.currentGameId) { // Check if there's an active game ID in the session
            const gameData = activeGames.get(gameSession.currentGameId); // Get game data from active games map

            // Check if the active game is Dice Escalator and waiting for the helper's roll
            if (gameData?.type === 'dice_escalator' && gameData.status === 'waiting_player_roll_via_helper') {
                const rollValue = parseInt(msg.text.trim(), 10); // Parse the roll value from helper message text

                if (!isNaN(rollValue) && rollValue >= 1 && rollValue <= 6) { // Validate the parsed roll
                    console.log(`[HELPER_ROLL_VALID] Processing roll ${rollValue} for game ${gameData.gameId}`);
                    // Ensure processDiceEscalatorPlayerRoll is defined and accessible
                    await processDiceEscalatorPlayerRoll(gameData, rollValue);
                    return; // Important: Skip further processing (like commands) if helper message handled
                } else {
                     console.log(`[HELPER_ROLL_INVALID] Helper text "${msg.text}" not parseable or invalid roll. Game: ${gameData.gameId}`);
                     // Optionally inform the player about the issue reading the roll
                     const helperBotName = DICES_HELPER_BOT_ID ? `Helper Bot (ID: ${DICES_HELPER_BOT_ID})` : (DICES_HELPER_BOT_USERNAME ? `@${DICES_HELPER_BOT_USERNAME}` : "the Dice Helper Bot");
                     await safeSendMessage(gameData.currentPlayerId, `There was an issue reading the roll ("${escapeMarkdownV2(msg.text)}") from ${helperBotName}. Please click 'Prompt Roll' again if you wish to retry.`, {parse_mode: 'MarkdownV2'});
                     // Do we need to reset the game status here? Maybe back to 'player_turn_prompt_action'?
                     // gameData.status = 'player_turn_prompt_action'; // Example reset
                     // activeGames.set(gameData.gameId, gameData);
                     // Consider implications before uncommenting status reset
                     return; // Still return to prevent command processing
                }
            } else {
                 console.log(`[HELPER_MSG_IGNORE] Ignored helper message. Game state not ready or not Dice Escalator. Type: ${gameData?.type}, Status: ${gameData?.status}`);
            }
        } else {
             console.log(`[HELPER_MSG_IGNORE] Ignored helper message. No active game ID found in session for chat ${chatId}.`);
        }
    }
    // --- End User Request Step 5 ---


    // --- Command Processing (Only if message is not from helper bot or wasn't handled above) ---
    if (text.startsWith('/') && !msg.from.is_bot) {
        const commandArgs = text.substring(1).split(' ');
        const commandName = commandArgs.shift().toLowerCase();
        console.log(`[CMD RCV] Chat: ${chatId}, User: ${userId}, Cmd: /${commandName}, Args: ${commandArgs.join(' ')}`);

        // Ensure user exists in our system before processing command
        await getUser(userId, msg.from.username); // Pass username for potential updates

        // --- User Request Step 6: Added 'debughelper' command ---
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
                    let betAmountCF = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(betAmountCF) || betAmountCF < MIN_BET_AMOUNT || betAmountCF > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet. Amount must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startcoinflip <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupCoinFlipCommand(chatId, msg.from, betAmountCF, messageId);
                } else {
                    await safeSendMessage(chatId, "This Coinflip game is designed for group chats.", {});
                }
                break;
            case 'startrps':
                if (chatType !== 'private') {
                    let betAmountRPS = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(betAmountRPS) || betAmountRPS < MIN_BET_AMOUNT || betAmountRPS > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet. Amount must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startrps <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupRPSCommand(chatId, msg.from, betAmountRPS, messageId);
                } else {
                    await safeSendMessage(chatId, "This Rock Paper Scissors game is designed for group chats.", {});
                }
                break;
            case 'startdiceescalator':
                if (chatType === 'group' || chatType === 'supergroup') {
                    let betAmountDE = commandArgs[0] ? parseInt(commandArgs[0], 10) : 10;
                    if (isNaN(betAmountDE) || betAmountDE < MIN_BET_AMOUNT || betAmountDE > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet for Dice Escalator. Amount must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startdiceescalator <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartDiceEscalatorCommand(chatId, msg.from, betAmountDE, messageId);
                } else {
                    await safeSendMessage(chatId, "Dice Escalator can only be started in a group or supergroup chat.", {});
                }
                break;
            case 'debughelper': // Added command
                // Check if ADMIN_USER_ID is set and matches the user sending the command
                 if (ADMIN_USER_ID && String(msg.from.id) === ADMIN_USER_ID) {
                    const helperInfo = {
                        configuredId: DICES_HELPER_BOT_ID, // Show currently loaded ID
                        configuredUsername: DICES_HELPER_BOT_USERNAME, // Show currently loaded username
                        // Get active Dice Escalator games (potentially across all chats)
                        activeDiceEscalatorGames: Array.from(activeGames.values()).filter(g => g.type === 'dice_escalator')
                    };
                    // Send the debug info as a JSON string in a code block
                    await safeSendMessage(chatId, `*Helper Bot Debug Info:*\n\`\`\`json\n${JSON.stringify(helperInfo, null, 2)}\n\`\`\``, {parse_mode: 'MarkdownV2'});
                } else {
                     // Optionally inform non-admins they can't use it, or just ignore silently
                     console.log(`[CMD_IGNORE] User ${userId} tried to use /debughelper without admin privileges.`);
                }
                break;
                // --- End User Request Step 6 ---
            default:
                // Only reply for unknown commands in private chat or if they start with /
                // Avoids replying to general group chat messages not meant for the bot.
                if (chatType === 'private' || text.startsWith('/')) {
                    await safeSendMessage(chatId, "Unknown command. Try /help to see available commands.", {});
                }
        }
    }
});

// --- Callback Query Handler ---
bot.on('callback_query', async (callbackQuery) => {
    const msg = callbackQuery.message;
    if (!msg) {
        // Answer the callback to remove the loading state, even if message is gone
        bot.answerCallbackQuery(callbackQuery.id, { text: "This action seems to have expired or the message was deleted." }).catch(() => {});
        console.warn(`[CBQ_WARN] Callback query received without associated message. ID: ${callbackQuery.id}, Data: ${callbackQuery.data}`);
        return;
    }
    const userId = String(callbackQuery.from.id);
    const chatId = String(msg.chat.id);
    const data = callbackQuery.data; // e.g., "join_game:game_123"
    const originalMessageId = msg.message_id;

    console.log(`[CBQ RCV] Chat: ${chatId}, User: ${userId}, Data: "${data}", MsgID: ${originalMessageId}`);

    // Answer callback immediately to provide responsiveness
    bot.answerCallbackQuery(callbackQuery.id).catch((err) => {
         console.error(`[CBQ_ERROR] Failed to answer callback query ${callbackQuery.id}: ${err.message}`);
    });

    // Ensure user exists
    await getUser(userId, callbackQuery.from.username);

    // Basic parsing: action:param1:param2...
    const [action, ...params] = data.split(':');

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
                if (params.length < 2) throw new Error("Missing params for rps_choose action (expected gameId:choice).");
                await handleRPSChoiceCallback(chatId, callbackQuery.from, params[0], params[1], originalMessageId);
                break;
            case 'de_roll_prompt':
                if (!params[0]) throw new Error("Missing gameId for de_roll_prompt action.");
                await handleDiceEscalatorPlayerAction(params[0], userId, 'roll_prompt', originalMessageId, chatId);
                break;
            case 'de_cashout':
                if (!params[0]) throw new Error("Missing gameId for de_cashout action.");
                await handleDiceEscalatorPlayerAction(params[0], userId, 'cashout', originalMessageId, chatId);
                break;
            default:
                console.log(`[CBQ_INFO] Unknown callback query action received: ${action}`);
                // Optionally notify user if the action is unknown
                // await safeSendMessage(userId, "Sorry, I didn't understand that button press.", {});
        }
    } catch (error) {
        console.error(`[CBQ_ERROR] Error processing callback query "${data}" for user ${userId} in chat ${chatId}:`, error);
        // Notify the user an error occurred
        await safeSendMessage(userId, "Sorry, an error occurred while processing your action. Please try again later.", {}).catch();
    }
});

// --- Command Handler Functions ---
async function handleHelpCommand(chatId, userObject) {
    const userMention = createUserMention(userObject);
    // Determine helper bot name for help text dynamically
    const helperBotNameForHelp = DICES_HELPER_BOT_ID ? `Helper Bot (ID: ${DICES_HELPER_BOT_ID})` : (DICES_HELPER_BOT_USERNAME && DICES_HELPER_BOT_USERNAME !== "YourDiceHelperBotUsername" ? `@${DICES_HELPER_BOT_USERNAME}` : "the configured Dice Helper Bot");

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
        `➡️ For Dice Escalator, click 'Prompt Roll', then trigger ${helperBotNameForHelp} (e.g., send 🎲).`,
        `➡️ For Coinflip or RPS, click 'Join Game' when someone starts one!`,
        // Added optional debug command visibility for Admin
        (ADMIN_USER_ID ? `\n*Admin Commands:*\n▫️ \`/debughelper\` - Shows helper bot config.` : ""),
        `\nHave fun and play responsibly!`
    ];
    // Filter out empty strings (like the admin command part if no admin ID)
    await safeSendMessage(chatId, helpTextParts.filter(Boolean).join('\n'), { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}
async function handleBalanceCommand(chatId, userObject) {
    const user = await getUser(String(userObject.id)); // Fetch latest user data
    // Make sure balance is properly formatted
    await safeSendMessage(chatId, `${createUserMention(userObject)}, your current balance is: *${formatCurrency(user.balance)}*.`, { parse_mode: 'MarkdownV2' });
}

// --- Group Game Flow Functions (Coinflip, RPS) ---
async function handleStartGroupCoinFlipCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, msg.chat.title || "Group Chat"); // Use actual chat title if available
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}* (ID: \`${gameSession.currentGameId}\`). Please wait for it to finish.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance (${escapeMarkdownV2(formatCurrency(initiator.balance))}) is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Deduct balance first
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_coinflip_init:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        // Should not happen based on check above, but good practice
        console.error(`[COINFLIP_START_ERR] Failed to deduct balance for ${initiatorId} (Reason: ${balanceUpdateResult.error})`);
        await safeSendMessage(chatId, `Error starting game: Could not place bet. (${balanceUpdateResult.error})`, {});
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
        console.error(`[COINFLIP_START_ERR] Failed to send setup message for game ${gameId}. Refunding bet.`);
        await updateUserBalance(initiatorId, betAmount, `refund_coinflip_setup_fail:${gameId}`, chatId);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return; // Stop if setup message failed
    }

    // Timeout for waiting opponent
    setTimeout(async () => {
        const gdCF = activeGames.get(gameId);
        if (gdCF && gdCF.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] Coinflip game ${gameId} in chat ${chatId} timed out waiting for opponent.`);
            await updateUserBalance(gdCF.initiatorId, gdCF.betAmount, `refund_coinflip_timeout:${gameId}`, chatId);
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null); // Clear game details from session

            const timeoutMsgTextCF = `The Coin Flip game (ID: \`${gameId}\`) started by ${gdCF.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdCF.betAmount))} has expired without an opponent. Bet refunded.`;
            // Try to edit the original setup message, fallback to sending new message
            if (gdCF.gameSetupMessageId) {
                 bot.editMessageText(escapeMarkdownV2(timeoutMsgTextCF), { chatId: String(chatId), message_id: Number(gdCF.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                   .catch(e => {
                        console.warn(`[GAME_TIMEOUT] Failed to edit message ${gdCF.gameSetupMessageId} for timed out coinflip ${gameId}. Sending new message. Error: ${e.message}`);
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
    const gameSession = await getGroupSession(chatId, msg.chat.title || "Group Chat"); // Use actual chat title
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}* (ID: \`${gameSession.currentGameId}\`). Please wait.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance (${escapeMarkdownV2(formatCurrency(initiator.balance))}) is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_rps_init:${gameId}`, chatId);
     if (!balanceUpdateResult.success) {
        console.error(`[RPS_START_ERR] Failed to deduct balance for ${initiatorId} (Reason: ${balanceUpdateResult.error})`);
        await safeSendMessage(chatId, `Error starting game: Could not place bet. (${balanceUpdateResult.error})`, {});
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
         console.error(`[RPS_START_ERR] Failed to send setup message for game ${gameId}. Refunding bet.`);
        await updateUserBalance(initiatorId, betAmount, `refund_rps_setup_fail:${gameId}`, chatId);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        return;
    }

    // Timeout for opponent join
    setTimeout(async () => {
        const gdRPS = activeGames.get(gameId);
        if (gdRPS && gdRPS.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] RPS game ${gameId} in chat ${chatId} timed out waiting for opponent.`);
            await updateUserBalance(gdRPS.initiatorId, gdRPS.betAmount, `refund_rps_timeout:${gameId}`, chatId);
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);

            const timeoutMsgTextRPS = `The Rock Paper Scissors game (ID: \`${gameId}\`) started by ${gdRPS.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdRPS.betAmount))} expired without an opponent. Bet refunded.`;
             if (gdRPS.gameSetupMessageId) {
                 bot.editMessageText(escapeMarkdownV2(timeoutMsgTextRPS), { chatId: String(chatId), message_id: Number(gdRPS.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                    .catch(e => {
                        console.warn(`[GAME_TIMEOUT] Failed to edit message ${gdRPS.gameSetupMessageId} for timed out RPS ${gameId}. Sending new message. Error: ${e.message}`);
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

    // --- Validation Checks ---
    if (!gameData) {
        await safeSendMessage(joinerId, "This game is no longer available or has expired.", {});
        // Attempt to remove buttons from the original message if possible
        if (interactionMessageId && chatId) bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
        return;
    }
     if (gameData.chatId !== String(chatId)) {
        // Should not happen if callback is from the right chat, but safety check
        console.warn(`[JOIN_ERR] Game ${gameId} chat mismatch. Expected: ${gameData.chatId}, Got: ${chatId}`);
        await safeSendMessage(joinerId, "Error joining game (chat mismatch).", {});
        return;
    }
    if (gameData.initiatorId === joinerId) {
        await safeSendMessage(joinerId, "You cannot join a game you started yourself.", {});
        return; // Don't need to answer callback again, already done.
    }
    if (gameData.status !== 'waiting_opponent') {
        await safeSendMessage(joinerId, "This game is no longer waiting for an opponent.", {});
        // If game is already playing or over, remove buttons
         if (interactionMessageId && chatId && gameData.status !== 'waiting_opponent') {
             bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
         }
        return;
    }
     // Check if game already has max participants (e.g., 2 for CF/RPS)
     if (gameData.participants.length >= 2 && (gameData.type === 'coinflip' || gameData.type === 'rps')) {
        await safeSendMessage(joinerId, "Sorry, this game already has enough players.", {});
        bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {}); // Remove buttons as game is full
        return;
    }


    // --- Balance and Bet ---
    const joiner = await getUser(joinerId);
    if (joiner.balance < gameData.betAmount) {
        await safeSendMessage(joinerId, `Your balance (${escapeMarkdownV2(formatCurrency(joiner.balance))}) is too low to join this ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} game.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const balanceUpdateResult = await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_group_${gameData.type}_join:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
        console.error(`[JOIN_ERR] Failed to deduct balance for joiner ${joinerId} (Reason: ${balanceUpdateResult.error})`);
        await safeSendMessage(joinerId, `Error joining game: Could not place bet. (${balanceUpdateResult.error})`, {});
        return;
    }

    // --- Add Participant and Update Game State ---
    gameData.participants.push({ userId: joinerId, choice: null, mention: createUserMention(joinerUser), betPlaced: true });

    // --- Process Game Specific Logic ---
    if (gameData.type === 'coinflip' && gameData.participants.length === 2) {
        gameData.status = 'playing'; // Mark as playing to prevent further joins/cancellations
        activeGames.set(gameId, gameData); // Save state before async operations

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        // Assign sides deterministically (e.g., initiator is heads)
        p1.choice = 'heads';
        p2.choice = 'tails';

        const cfResult = determineCoinFlipOutcome();
        let winner = (cfResult.outcome === p1.choice) ? p1 : p2;
        let loser = (winner === p1) ? p2 : p1; // For potential future use

        const winnings = gameData.betAmount * 2; // Total pot
        await updateUserBalance(winner.userId, winnings, `won_group_coinflip:${gameId}`, chatId);

        const resMsg = `*CoinFlip Resolved!*\nBet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n${p1.mention} (Heads) vs ${p2.mention} (Tails)\n\nLanded on: *${escapeMarkdownV2(cfResult.outcomeString)} ${cfResult.emoji}*!\n\n🎉 ${winner.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;

        // Edit the original message to show result
        if (interactionMessageId) {
            bot.editMessageText(resMsg, { chatId: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
                .catch(e => {
                    console.warn(`[COINFLIP_RESOLVE_ERR] Failed to edit message ${interactionMessageId}. Sending new message. Error: ${e.message}`);
                    safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' });
                });
        } else {
            safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2' });
        }

        activeGames.delete(gameId); // Clean up finished game
        await updateGroupGameDetails(chatId, null, null, null); // Update session

    } else if (gameData.type === 'rps' && gameData.participants.length === 2) {
        gameData.status = 'waiting_choices'; // Now waiting for players to choose
        activeGames.set(gameId, gameData);

        const rpsPrompt = `${gameData.participants[0].mention} & ${gameData.participants[1].mention}, your Rock Paper Scissors match for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} is ready!\n\nEach player must click a button below to make their choice secretly:`;
        const rpsKeyboard = {
            inline_keyboard: [
                [
                    { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
                    { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
                    { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
                ]
                 // Adding cancel might be complex if one player already chose
                 // [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]
            ]
        };

        if (interactionMessageId) {
            bot.editMessageText(escapeMarkdownV2(rpsPrompt), { chatId: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard })
               .catch(e => {
                    console.warn(`[RPS_JOIN_ERR] Failed to edit message ${interactionMessageId} for RPS prompt. Sending new message. Error: ${e.message}`);
                    safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt), { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard });
                });
        } else {
            safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt), { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard });
        }

    } else {
        // Fallback for other game types or if more players needed (not applicable here)
        activeGames.set(gameId, gameData);
        const joinedMessage = `${createUserMention(joinerUser)} has joined the ${escapeMarkdownV2(gameData.type)} game for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}! Waiting for more actions or players.`;
        // Generally better not to edit the message here unless it's the final state
        // safeSendMessage(chatId, joinedMessage, { parse_mode: 'MarkdownV2' });
        console.log(`[JOIN_INFO] User ${joinerId} joined game ${gameId}. Participants: ${gameData.participants.length}`);
    }
}
async function handleCancelGameCallback(chatId, cancellerUser, gameId, interactionMessageId) {
    const cancellerId = String(cancellerUser.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== String(chatId)) {
        await safeSendMessage(cancellerId, "Game unavailable for cancellation or has already finished.", {});
        return;
    }
    // Only initiator can cancel unless game logic allows otherwise
    if (gameData.initiatorId !== cancellerId) {
        await safeSendMessage(cancellerId, `Only the initiator (${gameData.initiatorMention}) can cancel this game.`, { parse_mode: 'MarkdownV2' });
        return;
    }
     // Define statuses where cancellation is allowed
     const cancellableStatuses = ['waiting_opponent', 'waiting_choices']; // Allow cancel during RPS choice phase? Maybe not if one chose? For simplicity, allowing here.
    if (!cancellableStatuses.includes(gameData.status)) {
         await safeSendMessage(cancellerId, `This game cannot be cancelled in its current state (${escapeMarkdownV2(gameData.status)}).`, { parse_mode: 'MarkdownV2' });
         return;
     }


    // Refund bets for all participants who placed one
    let refundCount = 0;
    for (const participant of gameData.participants) {
        if (participant.betPlaced) {
            await updateUserBalance(participant.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, chatId);
            refundCount++;
        }
    }
    console.log(`[GAME_CANCEL] Refunded ${refundCount} bet(s) for cancelled game ${gameId}.`);

    const gameTypeDisplay = gameData.type.charAt(0).toUpperCase() + gameData.type.slice(1); // Capitalize type
    const cancellationMessage = `${gameData.initiatorMention} has cancelled the ${escapeMarkdownV2(gameTypeDisplay)} game (Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}). All bets have been refunded.`;

    if (interactionMessageId) {
        bot.editMessageText(cancellationMessage, { chatId: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2', reply_markup: {} }) // Remove buttons
            .catch(e => {
                console.warn(`[GAME_CANCEL_ERR] Failed to edit message ${interactionMessageId} for cancelled game ${gameId}. Sending new message. Error: ${e.message}`);
                safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
            });
    } else {
        safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
    }

    activeGames.delete(gameId); // Remove from active games
    await updateGroupGameDetails(chatId, null, null, null); // Update session

    console.log(`[GAME_CANCEL] Game ${gameId} in chat ${chatId} cancelled by initiator ${cancellerId}.`);
}
async function handleRPSChoiceCallback(chatId, userObject, gameId, choice, interactionMessageId) {
    const userId = String(userObject.id);
    const gameData = activeGames.get(gameId);

    // Validations
    if (!gameData || gameData.chatId !== String(chatId) || gameData.type !== 'rps') {
        await safeSendMessage(userId, "This Rock Paper Scissors game isn't available or has ended.", {});
        return;
    }
     if (gameData.status !== 'waiting_choices') {
         await safeSendMessage(userId, "The game is not currently waiting for choices.", {});
         return;
     }
    const participant = gameData.participants.find(p => p.userId === userId);
    if (!participant) {
        await safeSendMessage(userId, "You are not currently playing in this RPS game.", {});
        return;
    }
    if (participant.choice) {
        // Using RPS_EMOJIS requires RPS_CHOICES to be available
        const chosenEmoji = RPS_EMOJIS[participant.choice] || '?';
        await safeSendMessage(userId, `You have already chosen ${chosenEmoji}. Waiting for the opponent.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    // Validate the choice itself
    if (!RPS_CHOICES[String(choice).toUpperCase()]) {
         console.error(`[RPS_CHOICE_ERR] Invalid choice '${choice}' received for game ${gameId} user ${userId}.`);
         await safeSendMessage(userId, `Invalid choice received. Please click Rock, Paper, or Scissors.`, {});
         return;
    }


    // Record choice and notify user privately
    participant.choice = RPS_CHOICES[String(choice).toUpperCase()]; // Store normalized choice
    const confirmEmoji = RPS_EMOJIS[participant.choice] || '?';
    await safeSendMessage(userId, `You chose ${confirmEmoji}! Waiting for your opponent...`, { parse_mode: 'MarkdownV2' });
    console.log(`[RPS_CHOICE] User ${userId} chose ${participant.choice} for game ${gameId}`);

    activeGames.set(gameId, gameData); // Update game data with choice

    // Check if opponent has chosen
    const otherPlayer = gameData.participants.find(p => p.userId !== userId);
    let groupUpdateMsg = `${participant.mention} has made their choice!`;

    if (otherPlayer && !otherPlayer.choice) {
        groupUpdateMsg += ` Waiting for ${otherPlayer.mention}...`;
        // Keep keyboard active if opponent hasn't chosen
         if (interactionMessageId) {
             // Re-generate keyboard in case it was removed previously
             const rpsKeyboard = {inline_keyboard: [[{text:`${RPS_EMOJIS.rock} Rock`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.ROCK}`},{text:`${RPS_EMOJIS.paper} Paper`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.PAPER}`},{text:`${RPS_EMOJIS.scissors} Scissors`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}`}]]};
             bot.editMessageText(escapeMarkdownV2(groupUpdateMsg), { chatId: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard })
                 .catch(() => {}); // Ignore errors editing, maybe message deleted
         }
    } else if (otherPlayer && otherPlayer.choice) {
        // Both players have chosen - Resolve the game
        gameData.status = 'game_over'; // Set status before async operations
        activeGames.set(gameId, gameData);

        const p1 = gameData.participants[0];
        const p2 = gameData.participants[1];
        const rpsRes = determineRPSOutcome(p1.choice, p2.choice); // Use normalized choices

        let winnerP = null;
        let resultText = `*Rock Paper Scissors Result!*\nBet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n${p1.mention}: ${RPS_EMOJIS[p1.choice]} vs ${p2.mention}: ${RPS_EMOJIS[p2.choice]}\n\n`;
        resultText += `${escapeMarkdownV2(rpsRes.description)}\n\n`;

        if (rpsRes.result === 'win1') winnerP = p1;
        else if (rpsRes.result === 'win2') winnerP = p2;

        if (winnerP) {
            const winnings = gameData.betAmount * 2; // Total pot
            await updateUserBalance(winnerP.userId, winnings, `won_rps:${gameId}`, chatId);
            resultText += `🎉 ${winnerP.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;
        } else if (rpsRes.result === 'draw') {
            // Refund both players on draw
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, chatId);
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, chatId);
            resultText += `It's a draw! Bets refunded.`;
        } else {
            // Handle potential 'error' result from determineRPSOutcome
             console.error(`[RPS_RESOLVE_ERR] RPS determination resulted in error for game ${gameId}. Refunding bets.`);
             await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_error:${gameId}`, chatId);
             await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_error:${gameId}`, chatId);
             resultText = `An error occurred resolving the RPS game. Bets refunded.`;
        }

        // Edit the message with the final result and remove keyboard
        const finalMsgId = Number(interactionMessageId || gameData.gameSetupMessageId); // Try original interaction ID first
        if (finalMsgId) {
             bot.editMessageText(resultText, { chatId: String(chatId), message_id: finalMsgId, parse_mode: 'MarkdownV2', reply_markup: {} })
                 .catch(e => {
                    console.warn(`[RPS_RESOLVE_ERR] Failed to edit message ${finalMsgId} with RPS result. Sending new message. Error: ${e.message}`);
                    safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2' });
                });
        } else {
            safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2' });
        }

        activeGames.delete(gameId); // Clean up finished game
        await updateGroupGameDetails(chatId, null, null, null); // Update session
    }
    // If only one player or other player hasn't chosen, state is already saved. No further action needed here.
}


// --- Dice Escalator Game Functions ---
async function handleStartDiceEscalatorCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, "Dice Escalator Group");
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown')}*. Please wait.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance (${escapeMarkdownV2(formatCurrency(initiator.balance))}) is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const balanceUpdateResult = await updateUserBalance(initiatorId, -betAmount, `bet_placed_dice_escalator:${gameId}`, chatId);
    if (!balanceUpdateResult.success) {
       console.error(`[DE_START_ERR] Failed to deduct balance for ${initiatorId} (Reason: ${balanceUpdateResult.error})`);
       await safeSendMessage(chatId, `Error starting game: Could not place bet. (${balanceUpdateResult.error})`, {});
       return;
    }

    // Define game data
    const gameData = {
        type: 'dice_escalator', gameId, chatId: String(chatId), initiatorId,
        initiatorMention: createUserMention(initiatorUser), betAmount,
        playerScore: 0, botScore: 0, status: 'player_turn_prompt_action', // Initial status
        currentPlayerId: initiatorId, // Player starts
        bustValue: DICE_ESCALATOR_BUST_ON, creationTime: Date.now(), commandMessageId,
        gameSetupMessageId: null
    };
    activeGames.set(gameId, gameData);
    await updateGroupGameDetails(chatId, gameId, 'DiceEscalator', betAmount);

    // Initial message and keyboard
    const initialMsg = `${gameData.initiatorMention} started *Dice Escalator* vs the Bot for ${escapeMarkdownV2(formatCurrency(betAmount))}!\n\nYour current score: *0*. Roll a *${gameData.bustValue}* and you bust!\n\nWhat do you want to do?`;
    const kb = {
        inline_keyboard: [
            [{ text: "🎲 Prompt User to Roll Dice", callback_data: `de_roll_prompt:${gameId}` }],
             // Disable cashout when score is 0
            [{ text: `💰 Cashout ${formatCurrency(0)}`, callback_data: `de_cashout:${gameId}` }]
        ]
    };

    const sentMsg = await safeSendMessage(chatId, initialMsg, { parse_mode: 'MarkdownV2', reply_markup: kb });

    if (sentMsg) {
        const gtu = activeGames.get(gameId);
        if (gtu) gtu.gameSetupMessageId = sentMsg.message_id;
    } else {
        console.error(`[DE_START_ERR] Failed to send initial game message for ${gameId}. Refunding bet.`);
        await updateUserBalance(initiatorId, betAmount, `refund_de_setup_fail:${gameId}`, chatId);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
     // Add timeout specific to Dice Escalator player action?
     // setTimeout(() => handleDiceEscalatorTimeout(gameId), DICE_ESCALATOR_TIMEOUT_MS);
}
async function handleDiceEscalatorPlayerAction(gameId, userId, actionType, interactionMessageId, chatId) {
    const gameData = activeGames.get(gameId);

    // Robust checks for game state and user
    if (!gameData) {
        await safeSendMessage(userId, "This Dice Escalator game seems to have ended or expired.", {});
        if (interactionMessageId && chatId) bot.editMessageReplyMarkup({}, {chat_id:String(chatId), message_id:Number(interactionMessageId)}).catch(()=>{});
        return;
    }
    if (gameData.chatId !== String(chatId) || gameData.type !== 'dice_escalator') {
        console.warn(`[DE_ACTION_WARN] Action '${actionType}' received for wrong game type or chat. Game ${gameId}, Type ${gameData.type}, Expected Chat ${gameData.chatId}, Got ${chatId}`);
        await safeSendMessage(userId, "There was an issue processing your action (game mismatch).", {});
        return;
    }
     // Only the current player (initiator in this version) can act
    if (gameData.currentPlayerId !== String(userId)) {
        await safeSendMessage(userId, "It's not your turn to perform this action.", {});
        return;
    }
    // Check if game is in the correct status for player action
    if (gameData.status !== 'player_turn_prompt_action') {
         await safeSendMessage(userId, `You cannot ${actionType} right now (current status: ${gameData.status}).`, {});
         // Optionally remove buttons if action is invalid for current state
         if (interactionMessageId && gameData.status !== 'player_turn_prompt_action') bot.editMessageReplyMarkup({}, {chat_id:String(chatId), message_id:Number(interactionMessageId)}).catch(()=>{});
         return;
    }


    const msgIdToUpdate = Number(interactionMessageId) || Number(gameData.gameSetupMessageId);
    if (!msgIdToUpdate) {
        console.error(`[DE_ACTION_ERR] No messageId found to update display for game ${gameId}`);
        // Send a new message if update fails, inform player
        await safeSendMessage(String(chatId), `Error updating game display for ${gameData.initiatorMention}. Please try the action again if applicable.`, { parse_mode: 'MarkdownV2' });
        return;
    }


    if (actionType === 'roll_prompt') {
        gameData.status = 'waiting_player_roll_via_helper'; // Update status: now waiting for helper
        activeGames.set(gameId, gameData);

        // Determine helper name/ID for message
        const helperName = DICES_HELPER_BOT_ID ? `Helper Bot (ID: ${DICES_HELPER_BOT_ID})` : (DICES_HELPER_BOT_USERNAME && DICES_HELPER_BOT_USERNAME !== "YourDiceHelperBotUsername" ? `@${DICES_HELPER_BOT_USERNAME}` : "the configured Dice Helper");
        const promptMsg = `${gameData.initiatorMention}, please send the 🎲 emoji now to have ${helperName} determine your roll.`;

        // Edit the message to show the prompt and remove buttons
        const editOpts = { chat_id: String(chatId), message_id: msgIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: {} }; // Remove keyboard
        console.log('[DEBUG_DE_ROLL_PROMPT] Preparing to edit message. ChatID value:', editOpts.chat_id, 'interactionMessageId (messageIdToUpdate) value:', editOpts.message_id); // Enhanced log
        console.log('[DEBUG_DE_ROLL_PROMPT] editOptions:', JSON.stringify(editOpts)); // Keep for debugging structure

        try {
            await bot.editMessageText(escapeMarkdownV2(promptMsg) + "\n\n_Waiting for roll from helper..._", editOpts);
            console.log(`[DE_ACTION] Prompted user ${userId} for roll via helper for game ${gameId}.`);
        } catch (error) {
             console.error(`[DE_ACTION_ERR] Failed to edit message ${msgIdToUpdate} to prompt roll for game ${gameId}: ${error.message}`);
             // Attempt to send a new message if edit fails
             await safeSendMessage(String(chatId), escapeMarkdownV2(promptMsg) + "\n\n_Waiting for roll from helper... (Error updating original message)_", { parse_mode: 'MarkdownV2'});
             // Should we revert status? Maybe not, player was prompted.
        }

         // Add timeout for helper roll? If helper doesn't respond in X seconds, cancel/refund?
         // setTimeout(() => handleDiceEscalatorHelperTimeout(gameId), DICE_ESCALATOR_TIMEOUT_MS);


    } else if (actionType === 'cashout') {
        // Check if score is > 0 for cashout
        if (gameData.playerScore <= 0) {
            await safeSendMessage(userId, "You cannot cash out with a score of 0.", {});
            return; // Don't proceed
        }

        const score = gameData.playerScore;
        const totalReturn = gameData.betAmount + score; // Original bet back + winnings

        await updateUserBalance(userId, totalReturn, `cashout_de_player:${gameId}`, chatId);
        gameData.status = 'player_cashed_out'; // Update status
        activeGames.set(gameId, gameData); // Save state


        let msg = `${gameData.initiatorMention} cashed out with a score of *${escapeMarkdownV2(String(score))}*!\nTotal credited: ${escapeMarkdownV2(formatCurrency(totalReturn))}.`;
        msg += `\n\n🤖 Now it's the Bot's turn. Target: Beat *${escapeMarkdownV2(String(score))}*...`;

        // Edit message to show cashout and bot's turn start
        try {
             await bot.editMessageText(msg, { chat_id: String(chatId), message_id: msgIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: {} }); // Remove buttons
        } catch (error) {
              console.error(`[DE_ACTION_ERR] Failed to edit message ${msgIdToUpdate} for cashout ${gameId}: ${error.message}`);
             await safeSendMessage(String(chatId), msg, { parse_mode: 'MarkdownV2'}); // Send new if edit fails
        }


        await sleep(2000); // Pause before bot plays
        await processDiceEscalatorBotTurn(gameData, msgIdToUpdate); // Start bot's turn
    }
}

// --- User Request Step 4: Enhanced processDiceEscalatorPlayerRoll ---
async function processDiceEscalatorPlayerRoll(gameData, playerRoll) {
    // Enhanced logging at start
    console.log(`[ROLL_PROCESS_START] Game ${gameData.gameId}`, {
        status: gameData.status, // Should be 'waiting_player_roll_via_helper'
        currentPlayer: gameData.currentPlayerId,
        rollValue: playerRoll
    });

     // Input validation
     if (typeof playerRoll !== 'number' || playerRoll < 1 || playerRoll > 6) {
        console.error(`[ROLL_INVALID] Invalid roll value received in processDiceEscalatorPlayerRoll: ${playerRoll}. Type: ${typeof playerRoll}`);
        // Should potentially inform the user or reset state here, but need careful thought
        // For now, just log and return to prevent errors.
        // Consider sending a message back to user via gameData.currentPlayerId
         await safeSendMessage(gameData.currentPlayerId, `An invalid roll value (${playerRoll}) was processed. Please try prompting again.`, {});
         // Reset status?
         // gameData.status = 'player_turn_prompt_action';
         // activeGames.set(gameData.gameId, gameData);
        return;
    }

    // Destructure needed game data
    const { gameId, chatId, initiatorMention, betAmount, bustValue, gameSetupMessageId, playerScore } = gameData;
    let newScore = playerScore; // Start with current score
    const msgId = Number(gameSetupMessageId); // Ensure we have the message ID to update

    if (!msgId) {
        console.error(`[DE_PLAYER_ROLL_ERR] No gameSetupMessageId found for game ${gameId}. Cannot update display.`);
        // Maybe send a new message if display update fails
        // await safeSendMessage(String(chatId), `Error processing roll for ${initiatorMention} (cannot update display).`, {parse_mode: 'MarkdownV2'});
        // Game state might be inconsistent if we proceed without ability to update UI. Consider returning.
        return;
    }

    // Determine helper name/ID for message
    const helperName = DICES_HELPER_BOT_ID ? `Helper Bot (ID: ${DICES_HELPER_BOT_ID})` : (DICES_HELPER_BOT_USERNAME && DICES_HELPER_BOT_USERNAME !== "YourDiceHelperBotUsername" ? `@${DICES_HELPER_BOT_USERNAME}` : "the configured Dice Helper");
    let turnResMsg = `${initiatorMention}, ${helperName} reported roll: ${formatDiceRolls([playerRoll])}!\n\n`; // Add newline

    if (playerRoll === bustValue) {
        gameData.status = 'game_over_player_bust'; // Update status: Player busted
        // No balance change here, bet was already deducted. Loss is implicit.
        // Update stats via updateUserBalance if needed (e.g., record loss explicitly)
        // await updateUserBalance(gameData.initiatorId, 0, `lost_de_bust:${gameId}`, chatId); // Example: 0 change, just for stats

        turnResMsg += `💥 *BUSTED* by rolling a ${bustValue}! You lost your ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`;

        activeGames.delete(gameId); // Remove completed game
        await updateGroupGameDetails(chatId, null, null, null); // Clear session game details

        // Update message, remove keyboard
        try {
            await bot.editMessageText(turnResMsg, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: {} });
        } catch (error) {
            console.error(`[DE_PLAYER_ROLL_ERR] Failed to edit message ${msgId} for player bust ${gameId}: ${error.message}`);
            await safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2' }); // Send new if edit fails
        }

    } else {
        newScore += playerRoll; // Add roll to score
        gameData.playerScore = newScore; // Update score in game data
        gameData.status = 'player_turn_prompt_action'; // Set status back to allow next action

        turnResMsg += `Your score is now *${escapeMarkdownV2(String(newScore))}*.\nPotential winnings (if cashed out): *${escapeMarkdownV2(formatCurrency(newScore))}*.`;
        turnResMsg += `\n\nRoll a *${bustValue}* next and you bust! Choose your next action:`;

        // Update keyboard for next action
        const kb = {
            inline_keyboard: [
                [{ text: `🎲 Prompt Roll (Current: ${formatCurrency(newScore)})`, callback_data: `de_roll_prompt:${gameId}` }],
                [{ text: `💰 Cashout ${escapeMarkdownV2(formatCurrency(newScore))}`, callback_data: `de_cashout:${gameId}` }]
            ]
        };

         // Update message with new score and keyboard
         try {
             await bot.editMessageText(turnResMsg, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: kb });
         } catch (error) {
             console.error(`[DE_PLAYER_ROLL_ERR] Failed to edit message ${msgId} after player roll ${gameId}: ${error.message}`);
             await safeSendMessage(String(chatId), turnResMsg, { parse_mode: 'MarkdownV2', reply_markup: kb }); // Send new if edit fails
         }


        // Update active games map only if game didn't bust
        activeGames.set(gameId, gameData);
    }

     // Enhanced logging at end
     console.log(`[ROLL_PROCESS_COMPLETE] Game ${gameData.gameId}`, {
         newStatus: gameData.status, // Log final status after processing
         playerScore: gameData.playerScore // Log final score
     });
}
// --- End User Request Step 4 ---

async function processDiceEscalatorBotTurn(gameData, messageIdToUpdate) {
    const { gameId, chatId, initiatorMention, betAmount, playerScore: playerCashedScore, bustValue } = gameData;
    let botScore = 0;
    let botBusted = false;
    let rolls = 0;
    let botPlaysMsg = `${initiatorMention} cashed out at *${escapeMarkdownV2(String(playerCashedScore))}*.\n\n🤖 Bot's turn (Target: Beat ${playerCashedScore}):\n`;

    // Ensure messageIdToUpdate is valid
    const msgId = Number(messageIdToUpdate);
    if (!msgId) {
        console.error(`[DE_BOT_TURN_ERR] Invalid or missing messageIdToUpdate (${messageIdToUpdate}) for game ${gameId}. Aborting bot turn.`);
        // Clean up game state as bot turn cannot proceed / display
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
        await safeSendMessage(String(chatId), `Error occurred during bot's turn (display issue). Game ${gameId} ended.`, {});
        return;
    }

    // Initial update: Bot is thinking
    try {
        await bot.editMessageText(escapeMarkdownV2(botPlaysMsg) + "\n_Bot thinking..._", { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: {} });
    } catch (editError) {
         console.error(`[DE_BOT_TURN_ERR] Failed initial edit message ${msgId} for bot turn ${gameId}: ${editError.message}`);
         // Attempt to send new message if edit fails, but bot turn might be desynced from UI
         await safeSendMessage(String(chatId), escapeMarkdownV2(botPlaysMsg) + "\n_Bot thinking... (Display Error)_", { parse_mode: 'MarkdownV2' });
         // Proceed with bot logic, but UI might be stuck
    }

    await sleep(2000); // Pause for thinking effect

    // Bot rolling logic loop
    while (rolls < DICE_ESCALATOR_BOT_ROLLS && botScore <= playerCashedScore && !botBusted) {
        rolls++;
        const botRollResult = determineDieRollOutcome(); // Use internal roll function
        const botRoll = botRollResult.roll;
        botPlaysMsg += `\nBot roll ${rolls}: ${botRollResult.emoji} (${botRoll})`;

        if (botRoll === bustValue) {
            botBusted = true;
            botScore = 0; // Score reset on bust
            botPlaysMsg += `\n💥 Bot BUSTED!`;
            break; // Exit loop on bust
        }

        botScore += botRoll;
        botPlaysMsg += `\nBot score: *${botScore}*`;

        // Decide if bot needs to roll again
        let nextPrompt = "";
        if (botScore <= playerCashedScore && rolls < DICE_ESCALATOR_BOT_ROLLS && !botBusted) {
            nextPrompt = "\n\n_Bot rolls again..._";
        }

        // Update message with current roll and score
        try {
            await bot.editMessageText(escapeMarkdownV2(botPlaysMsg) + nextPrompt, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: {} });
        } catch (editError) {
             console.error(`[DE_BOT_TURN_ERR] Failed to edit message ${msgId} during bot roll ${rolls} for game ${gameId}: ${editError.message}`);
             // Log error but continue bot logic if possible
        }


        if (nextPrompt) await sleep(2500); // Pause between bot rolls
    }

    await sleep(1500); // Pause before final result

    // Determine final outcome
    let finalMsg = botPlaysMsg;
    gameData.status = 'game_over_bot_played'; // Final status
    gameData.botScore = botScore; // Record final bot score

    finalMsg += "\n\n--- *Game Over* ---";
    if (botBusted || botScore <= playerCashedScore) {
        // Player wins (conceptually, as they already cashed out)
        finalMsg += `\n🎉 ${initiatorMention}, the Bot ${botBusted ? "Busted" : `only reached *${escapeMarkdownV2(String(botScore))}*`}! It didn't beat your score of *${escapeMarkdownV2(String(playerCashedScore))}*.`;
        finalMsg += `\nYou keep your cashed-out winnings!`;
    } else {
        // Bot wins (conceptually)
        finalMsg += `\n😢 ${initiatorMention}, the Bot beat your score, reaching *${escapeMarkdownV2(String(botScore))}*!`;
        finalMsg += `\nSince you already cashed out, the House wins this round conceptually.`;
    }

    finalMsg += `\n\nFinal Scores: You (Cashed Out): *${escapeMarkdownV2(String(playerCashedScore))}* | Bot: *${escapeMarkdownV2(String(botScore))}*${botBusted ? " (Busted)" : ""}.`;

    // Final message update
    try {
         await bot.editMessageText(finalMsg, { chatId: String(chatId), message_id: msgId, parse_mode: 'MarkdownV2', reply_markup: {} });
    } catch (editError) {
        console.error(`[DE_BOT_TURN_ERR] Failed to edit final message ${msgId} for game ${gameId}: ${editError.message}`);
        await safeSendMessage(String(chatId), finalMsg, { parse_mode: 'MarkdownV2' }); // Send new if edit fails
    }


    activeGames.delete(gameId); // Clean up finished game
    await updateGroupGameDetails(chatId, null, null, null); // Update session

    console.log(`[DE_BOT_TURN] Bot turn completed for game ${gameId}. Player Score: ${playerCashedScore}, Bot Score: ${botScore}, Bot Busted: ${botBusted}`);
}


console.log("Part 5: Message & Callback Handling, Basic Game Flow - Complete.");


//---------------------------------------------------------------------------
// index.js - Part 6: Startup, Shutdown, and Basic Error Handling
//---------------------------------------------------------------------------
console.log("Loading Part 6: Startup, Shutdown, and Basic Error Handling...");

async function runPeriodicBackgroundTasks() {
    console.log(`[BACKGROUND_TASK] [${new Date().toISOString()}] Running periodic background tasks...`);
    const now = Date.now();
    const GAME_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 5; // Using constant from Part 5
    let cleanedGames = 0;
    for (const [gameId, gameData] of activeGames.entries()) {
        if (now - gameData.creationTime > GAME_CLEANUP_THRESHOLD_MS && (gameData.status === 'waiting_opponent' || gameData.status === 'waiting_choices' || gameData.status === 'waiting_player_roll_via_helper')) {
            console.warn(`[BACKGROUND_TASK] Cleaning stale game ${gameId} (${gameData.type}) in chat ${gameData.chatId}. Status: ${gameData.status}`);
            // Refund initiator if game is stale in specific states where bet was placed but game didn't conclude
            if (gameData.initiatorId && gameData.betAmount > 0 &&
               (gameData.status === 'waiting_opponent' || // Refund if no opponent joined
               (gameData.type==='dice_escalator' && gameData.status === 'waiting_player_roll_via_helper' && gameData.playerScore === 0 ))) // Refund DE if helper never responded after prompt
            {
                // Ensure updateUserBalance and other functions/constants are accessible here
                await updateUserBalance(gameData.initiatorId, gameData.betAmount, `refund_stale_${gameData.type}:${gameId}`, gameData.chatId);
                const staleMsg = `Game (ID: \`${gameId}\`) by ${gameData.initiatorMention} cleared due to inactivity. Bet refunded.`;
                if(gameData.gameSetupMessageId) {
                    // Assuming 'bot' and 'escapeMarkdownV2' are accessible
                    bot.editMessageText(escapeMarkdownV2(staleMsg), {chat_id: String(gameData.chatId), message_id: Number(gameData.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup:{}}).catch(()=>{safeSendMessage(String(gameData.chatId), escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'})});
                } else {
                    safeSendMessage(String(gameData.chatId), escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'});
                }
            } else if (gameData.status === 'waiting_choices' && gameData.participants.length > 1) {
                 // Handle stale RPS where choices weren't made
                 console.log(`[BACKGROUND_TASK] Stale RPS game ${gameId} found in waiting_choices. Refunding players.`);
                 for (const p of gameData.participants) {
                      if (p.betPlaced) {
                           await updateUserBalance(p.userId, gameData.betAmount, `refund_stale_rps_nochoice:${gameId}`, gameData.chatId);
                      }
                 }
                 const staleMsg = `RPS Game (ID: \`${gameId}\`) by ${gameData.initiatorMention} cleared due to inactivity during choice phase. Bets refunded.`;
                  if(gameData.gameSetupMessageId) bot.editMessageText(escapeMarkdownV2(staleMsg), {chat_id: String(gameData.chatId), message_id: Number(gameData.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup:{}}).catch(()=>{safeSendMessage(String(gameData.chatId), escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'})});
                  else safeSendMessage(String(gameData.chatId), escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'});
            }

            activeGames.delete(gameId);
            // Ensure getGroupSession and updateGroupGameDetails are accessible
            const groupSession = await getGroupSession(gameData.chatId);
            if (groupSession && groupSession.currentGameId === gameId) await updateGroupGameDetails(gameData.chatId, null, null, null);
            cleanedGames++;
        }
    }
    if (cleanedGames > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedGames} stale game(s).`);

    const SESSION_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 20; // Using constant from Part 5
    let cleanedSessions = 0;
    for (const [chatId, sessionData] of groupGameSessions.entries()) {
        // Check if lastActivity exists and is a Date object before comparison
        if (!sessionData.currentGameId && sessionData.lastActivity instanceof Date && (now - sessionData.lastActivity.getTime()) > SESSION_CLEANUP_THRESHOLD_MS) {
            console.log(`[BACKGROUND_TASK] Cleaning inactive group session for chat ${chatId}.`);
            groupGameSessions.delete(chatId);
            cleanedSessions++;
        } else if (!sessionData.lastActivity) {
             // Handle sessions that might somehow lack a lastActivity timestamp
             console.warn(`[BACKGROUND_TASK] Group session for chat ${chatId} missing lastActivity timestamp. Removing potentially stale session.`);
             groupGameSessions.delete(chatId);
             cleanedSessions++;
        }
    }
    if (cleanedSessions > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedSessions} inactive group session entries.`);
    console.log(`[BACKGROUND_TASK] Finished. Active games: ${activeGames.size}, Group sessions: ${groupGameSessions.size}.`);
}
// const backgroundTaskInterval = setInterval(runPeriodicBackgroundTasks, 15 * 60 * 1000);

// --- Process-level Error Handling ---
process.on('uncaughtException', (error, origin) => {
    console.error(`\n🚨🚨 UNCAUGHT EXCEPTION AT: ${origin} 🚨🚨`, error);
    // Ensure ADMIN_USER_ID and safeSendMessage are accessible
    if(ADMIN_USER_ID) safeSendMessage(ADMIN_USER_ID, `🆘 UNCAUGHT EXCEPTION:\nOrigin: ${origin}\nError: ${error.message}`, {}).catch(()=>{});
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`\n🔥🔥 UNHANDLED REJECTION 🔥🔥`, reason, promise);
    // Ensure ADMIN_USER_ID, safeSendMessage, and escapeMarkdownV2 are accessible
    if(ADMIN_USER_ID) safeSendMessage(ADMIN_USER_ID, `♨️ UNHANDLED REJECTION:\nReason: ${escapeMarkdownV2(String(reason instanceof Error ? reason.message : reason))}`, {parse_mode:'MarkdownV2'}).catch();
});

// --- Telegram Bot Library Specific Error Handling ---
// Ensure 'bot' instance is accessible here
if (bot) { // Add a check to ensure bot is defined before attaching listeners
    bot.on('polling_error', (error) => {
        console.error(`\n🚫 POLLING ERROR 🚫 Code: ${error.code}`);
        console.error(`Message: ${error.message}`);
        console.error(error); // Log the full error object for detailed diagnostics
    });

    bot.on('error', (error) => {
        console.error('\n🔥 BOT GENERAL ERROR EVENT 🔥:', error);
    });

} else {
    console.error("!!! CRITICAL ERROR: 'bot' instance not defined when trying to attach error handlers in Part 6 !!!");
}


// --- Shutdown Handling ---
let isShuttingDown = false;
async function shutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log(`\n🚦 Received ${signal}. Shutting down Bot v${BOT_VERSION}...`); // Ensure BOT_VERSION is accessible
    if (bot && bot.isPolling()) {
        try {
            await bot.stopPolling({cancel:true});
            console.log("Polling stopped.");
        } catch(e){
            console.error("Error stopping polling:", e.message);
        }
    }
    /* if (backgroundTaskInterval) clearInterval(backgroundTaskInterval); */
    // Ensure ADMIN_USER_ID and safeSendMessage are accessible
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
    console.log(`\n🚀🚀🚀 Initializing Group Chat Casino Bot v${BOT_VERSION} 🚀🚀🚀`); // Ensure BOT_VERSION accessible
    console.log(`Timestamp: ${new Date().toISOString()}`);

    // NOTE: Helper Bot ID/Username logging and fetching logic was moved to Part 1
    // as per user request step 3. Removed the redundant block from here.
    console.log(`[STARTUP_INFO] Helper Bot ID currently set to: ${DICES_HELPER_BOT_ID}`);
    console.log(`[STARTUP_INFO] Helper Bot Username currently set to: ${DICES_HELPER_BOT_USERNAME}`);


    try {
        const me = await bot.getMe();
        console.log(`✅ Successfully connected to Telegram! Bot Name: @${me.username}, Bot ID: ${me.id}`);
        // Ensure ADMIN_USER_ID, safeSendMessage, BOT_VERSION, process.env.HOSTNAME are accessible
        if (ADMIN_USER_ID) {
            await safeSendMessage(ADMIN_USER_ID, `🎉 Bot v${BOT_VERSION} started! Polling active. Host: ${process.env.HOSTNAME || 'local'}`, { parse_mode: 'MarkdownV2' });
        }
        console.log(`\n🎉 Bot operational! Waiting for messages...`);
        // Run background tasks once shortly after startup
        setTimeout(runPeriodicBackgroundTasks, 15000); // Ensure runPeriodicBackgroundTasks is defined
    } catch (error) {
        console.error("❌ CRITICAL STARTUP ERROR (getMe):", error);
        // Ensure ADMIN_USER_ID, BOT_TOKEN, TelegramBot, BOT_VERSION, escapeMarkdownV2 are accessible
        if (ADMIN_USER_ID && BOT_TOKEN) {
            // Use a temporary bot instance ONLY for sending the failure message
            try {
                const tempBot = new TelegramBot(BOT_TOKEN, {}); // No polling for temp bot
                await tempBot.sendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE v${BOT_VERSION}:\n${escapeMarkdownV2(error.message)}\nBot is exiting.`).catch(e => console.error("Failed to send critical startup failure message:", e));
            } catch (tempBotError) {
                console.error("Failed to create temporary bot for failure notification:", tempBotError);
            }
        }
        process.exit(1);
    }
}

// --- Final Execution ---
main().catch(error => {
    console.error("❌ MAIN ASYNC FUNCTION UNHANDLED ERROR:", error);
    process.exit(1);
});

console.log("Part 6: Startup, Shutdown, and Basic Error Handling - Complete.");
// --- END OF index.js ---
