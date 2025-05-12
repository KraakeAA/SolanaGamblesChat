// index.js - Group Chat Casino Bot
// --- VERSION: 1.0.0-group-diceescalator ---

//---------------------------------------------------------------------------
// index.js - Part 1: Core Imports & Basic Setup
//---------------------------------------------------------------------------
console.log("Loading Part 1: Core Imports & Basic Setup...");

import 'dotenv/config';
import TelegramBot from 'node-telegram-bot-api';

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_USER_ID = process.env.ADMIN_USER_ID;
// Placeholder for the username or ID of your DicesHelperBot.
// IMPORTANT: For ID, don't include '@'. For username, also no '@'.
const DICES_HELPER_BOT_USERNAME = process.env.DICES_HELPER_BOT_USERNAME || "YourDiceHelperBotUsername"; // Replace with actual or use ID
const DICES_HELPER_BOT_ID = process.env.DICES_HELPER_BOT_ID; // Alternative: use the helper bot's numerical ID

if (!BOT_TOKEN) {
    console.error("FATAL ERROR: BOT_TOKEN is not defined.");
    process.exit(1);
}
console.log("BOT_TOKEN loaded successfully.");
if (ADMIN_USER_ID) console.log(`Admin User ID: ${ADMIN_USER_ID} loaded.`);
else console.log("INFO: No ADMIN_USER_ID set (optional).");

if (!DICES_HELPER_BOT_ID && DICES_HELPER_BOT_USERNAME === "YourDiceHelperBotUsername") {
    console.warn("WARNING: DICES_HELPER_BOT_USERNAME is using placeholder. Dice Escalator game might not function correctly if it relies on detecting messages from a specific helper bot by username.");
} else if (DICES_HELPER_BOT_ID) {
    console.log(`Dices Helper Bot ID: ${DICES_HELPER_BOT_ID} will be used for roll detection.`);
} else {
    console.log(`Dices Helper Bot Username: @${DICES_HELPER_BOT_USERNAME} will be used for roll detection.`);
}


const bot = new TelegramBot(BOT_TOKEN, { polling: true });
console.log("Telegram Bot instance created and configured for polling.");

const BOT_VERSION = '1.0.0-group-diceescalator';
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096;

let activeGames = new Map(); // Key: gameId, Value: game state object
let userCooldowns = new Map(); // Key: userId, Value: timestamp

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
    let finalOptions = {...options};
    if (finalOptions.parse_mode === 'MarkdownV2') {
        messageToSend = escapeMarkdownV2(text);
    }
    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsis = "... (message truncated)";
        let escapedEllipsis = (finalOptions.parse_mode === 'MarkdownV2') ? escapeMarkdownV2(ellipsis) : ellipsis;
        messageToSend = messageToSend.substring(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - escapedEllipsis.length) + escapedEllipsis;
        console.warn(`[safeSendMessage] Message for chat ${chatId} truncated.`);
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
        return { ...newUser };
    }
    const user = userDatabase.get(userIdStr);
    if (username && user.username !== username) user.username = username;
    user.isNew = false;
    return { ...user, groupStats: new Map(user.groupStats) };
}

async function updateUserBalance(userId, amountChange, reason = "unknown", chatId) {
    const userIdStr = String(userId);
    if (!userDatabase.has(userIdStr)) {
        console.warn(`[DB_SIM_ERROR] Update balance for non-existent user: ${userIdStr}`);
        return { success: false, error: "User not found." };
    }
    const user = userDatabase.get(userIdStr);
    if (user.balance + amountChange < 0) {
        console.log(`[DB_SIM] User ${userIdStr} insufficient balance (${user.balance}) for ${amountChange} (Reason: ${reason}).`);
        return { success: false, error: "Insufficient balance." };
    }
    user.balance += amountChange;
    user.lastPlayed = new Date();
    console.log(`[DB_SIM] User ${userIdStr} balance: ${user.balance} (Change: ${amountChange}, Reason: ${reason}, Chat: ${chatId||'N/A'})`);
    if (chatId) {
        const chatIdStr = String(chatId);
        if (!user.groupStats.has(chatIdStr)) user.groupStats.set(chatIdStr, { gamesPlayed: 0, totalWagered: 0, netWinLoss: 0 });
        const stats = user.groupStats.get(chatIdStr);
        if (reason.toLowerCase().includes("bet_placed")) {
            const wager = Math.abs(amountChange);
            stats.totalWagered += wager;
            stats.netWinLoss -= wager;
        } else if (reason.toLowerCase().includes("won_") || reason.toLowerCase().includes("payout_") || reason.toLowerCase().includes("cashout")) {
            stats.netWinLoss += amountChange; // Assumes amountChange is net profit for wins or total for cashout refund
            if (!reason.toLowerCase().includes("refund_") && !reason.toLowerCase().includes("cashout")) stats.gamesPlayed += 1;
        } else if (reason.toLowerCase().includes("lost_") && !reason.toLowerCase().includes("bust")) { // bust already means bet was lost
            stats.gamesPlayed += 1;
        } else if (reason.toLowerCase().includes("refund_")) {
            // If a bet was placed and then refunded, netWinLoss should be adjusted back if it was initially reduced
            // This part depends on how "bet_placed" affects netWinLoss.
            // If bet_placed reduces netWinLoss by wager, refund should add wager back.
            // Current logic: amountChange is positive refund.
            stats.netWinLoss += amountChange;
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
        await getGroupSession(chatIdStr, "Unknown Group (Auto)");
    }
    const session = groupGameSessions.get(chatIdStr);
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
    if (!userObject) return escapeMarkdownV2("Anon");
    return escapeMarkdownV2(userObject.first_name || userObject.username || `User ${userObject.id}`);
}

function createUserMention(userObject) {
    if (!userObject || !userObject.id) return escapeMarkdownV2("Unknown User");
    const name = userObject.first_name || userObject.username || `User ${userObject.id}`;
    return `[${escapeMarkdownV2(name)}](tg://user?id=${userObject.id})`;
}

function formatCurrency(amount, currencyName = "credits") {
    let num = Number(amount);
    if (isNaN(num)) num = 0;
    return `${num.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: (num % 1 === 0 ? 0 : 2)})} ${currencyName}`;
}

function rollDie(sides = 6) { // Used by bot for its own rolls
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6;
    return Math.floor(Math.random() * sides) + 1;
}

function formatDiceRolls(rolls) {
    const emojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"];
    return `Rolls: ${rolls.map(r => (r >= 1 && r <= 6) ? emojis[r-1] : `🎲 ${r}`).join(' ')}`;
}

function generateGameId() {
    return `game_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
}
console.log("Part 3: Telegram Helpers & Basic Game Utilities - Complete.");

//---------------------------------------------------------------------------
// index.js - Part 4: Simplified Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 4: Simplified Game Logic...");

function determineCoinFlipOutcome() {
    const isHeads = Math.random() < 0.5;
    return isHeads ? { outcome: 'heads', outcomeString: "Heads", emoji: '🪙' } : { outcome: 'tails', outcomeString: "Tails", emoji: '🪙' };
}

function determineDieRollOutcome(sides = 6) { // This is for OUR BOT's internal rolls
    sides = Number.isInteger(sides) && sides > 0 ? sides : 6;
    const roll = Math.floor(Math.random() * sides) + 1;
    const emojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"];
    return { roll, emoji: (roll >= 1 && roll <= 6) ? emojis[roll-1] : `🎲${roll}` };
}

// Dice Escalator Game Specifics
const DICE_ESCALATOR_BUST_ON = 1;

// (Rock Paper Scissors logic from previous Part 4 can remain here if desired)
const RPS_CHOICES = { ROCK: 'rock', PAPER: 'paper', SCISSORS: 'scissors' };
const RPS_EMOJIS = { [RPS_CHOICES.ROCK]: '🪨', [RPS_CHOICES.PAPER]: '📄', [RPS_CHOICES.SCISSORS]: '✂️' };
const RPS_RULES = {
    [RPS_CHOICES.ROCK]: { beats: RPS_CHOICES.SCISSORS, verb: "crushes" },
    [RPS_CHOICES.PAPER]: { beats: RPS_CHOICES.ROCK, verb: "covers" },
    [RPS_CHOICES.SCISSORS]: { beats: RPS_CHOICES.PAPER, verb: "cuts" }
};
function getRandomRPSChoice() { /* ... as before ... */
    const choices = Object.values(RPS_CHOICES);
    const randomChoice = choices[Math.floor(Math.random() * choices.length)];
    return { choice: randomChoice, emoji: RPS_EMOJIS[randomChoice] };
}
function determineRPSOutcome(choice1, choice2) { /* ... as before ... */
    if (!RPS_CHOICES[String(choice1).toUpperCase()] || !RPS_CHOICES[String(choice2).toUpperCase()]) {
        return { result: 'draw', description: "Invalid choices.", choice1, choice1Emoji: '❔', choice2, choice2Emoji: '❔' };
    }
    const c1 = RPS_CHOICES[String(choice1).toUpperCase()], c2 = RPS_CHOICES[String(choice2).toUpperCase()];
    const c1E = RPS_EMOJIS[c1], c2E = RPS_EMOJIS[c2];
    if (c1 === c2) return { result: 'draw', description: `${c1E} ${c1} vs ${c2E} ${c2}. Draw!`, choice1:c1,choice1Emoji:c1E, choice2:c2,choice2Emoji:c2E };
    if (RPS_RULES[c1].beats === c2) return { result: 'win1', description: `${c1E} ${c1} ${RPS_RULES[c1].verb} ${c2E} ${c2}. Player 1 wins!`, choice1:c1,choice1Emoji:c1E, choice2:c2,choice2Emoji:c2E };
    return { result: 'win2', description: `${c2E} ${c2} ${RPS_RULES[c2].verb} ${c1E} ${c1}. Player 2 wins!`, choice1:c1,choice1Emoji:c1E, choice2:c2,choice2Emoji:c2E };
}

console.log("Part 4: Simplified Game Logic - Complete.");

// index.js - Part 5: Message & Callback Handling, Basic Game Flow
// --- VERSION: 1.0.0-group-diceescalator ---

// (Code from Parts 1, 2, 3 & 4 is assumed to be above this)
// ...

console.log("Loading Part 5: Message & Callback Handling, Basic Game Flow...");

// --- Constants for Cooldowns & Game Flow ---
const COMMAND_COOLDOWN_MS = 2000; // 2 seconds between commands for a user
const JOIN_GAME_TIMEOUT_MS = 60000; // 60 seconds for users to join a game for Coinflip/RPS
const DICE_ESCALATOR_TIMEOUT_MS = 120000; // 2 minutes for the Dice Escalator game session
const MIN_BET_AMOUNT = 5; // Minimum bet amount for games, e.g., 5 credits
const MAX_BET_AMOUNT = 1000; // Maximum bet amount
const DICE_ESCALATOR_BOT_ROLLS = 3; // How many times bot will attempt to roll in Dice Escalator

// --- Main Message Handler ---
bot.on('message', async (msg) => {
    // --- START TEMPORARY RAW MESSAGE LOG ---
    if (msg && msg.from) {
        console.log(`[RAW_MSG_INPUT] Received message. Text: "${msg.text || 'N/A'}", From ID: ${msg.from.id}, From Username: @${msg.from.username || 'N/A'}, Is Bot: ${msg.from.is_bot}`);
    } else if (msg) {
        console.log(`[RAW_MSG_INPUT] Received message with no 'from' field:`, JSON.stringify(msg).substring(0, 200));
    } else {
        console.log("[RAW_MSG_INPUT] Received an undefined 'msg' object in message handler.");
        return; // Can't do anything with an undefined message
    }
    // --- END TEMPORARY RAW MESSAGE LOG ---


    // Initial guard: We need 'msg.from' to identify the sender.
    if (!msg.from) {
        return;
    }

    // If the message is from any bot (including self, or the helper bot)
    if (msg.from.is_bot) {
        const isFromConfiguredHelperBotById = DICES_HELPER_BOT_ID && String(msg.from.id) === DICES_HELPER_BOT_ID;
        const isFromConfiguredHelperBotByUsername = !DICES_HELPER_BOT_ID && DICES_HELPER_BOT_USERNAME && msg.from.username === DICES_HELPER_BOT_USERNAME;

        if (!(isFromConfiguredHelperBotById || isFromConfiguredHelperBotByUsername)) {
            // console.log(`[MSG_IGNORE] Ignoring message from other bot: @${msg.from.username || msg.from.id}`);
            return;
        }
        if (!msg.text) { // Helper bot message MUST have text (the number)
            console.log(`[MSG_IGNORE] Message from Helper Bot (@${msg.from.username || msg.from.id}) ignored: No text content.`);
            return;
        }
    } else { // From a human
        if (!msg.text) {
            // console.log(`[MSG_IGNORE] Message from human user ${msg.from.id} ignored: No text content.`);
            return;
        }
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text || ""; // Ensure text is always a string
    const chatType = msg.chat.type;
    const messageId = msg.message_id;

    // Log user messages here. Helper messages logged in their specific block.
    if (!msg.from.is_bot) {
        console.log(`[MSG RCV] Chat: ${chatId} (Type: ${chatType}), User: ${userId} (@${msg.from.username || 'N/A'}), Text: "${text}"`);
    }

    // Cooldown Check (only for user-initiated commands starting with '/')
    const now = Date.now();
    if (!msg.from.is_bot && text.startsWith('/') && userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
        console.log(`[COOLDOWN] User ${userId} command ("${text}") ignored due to cooldown.`);
        return;
    }

    // --- Check if this message is specifically from our configured DicesHelperBot ---
    const isFromHelperBotById = DICES_HELPER_BOT_ID && String(msg.from.id) === DICES_HELPER_BOT_ID;
    const isFromHelperBotByUsername = !DICES_HELPER_BOT_ID && DICES_HELPER_BOT_USERNAME && msg.from.username === DICES_HELPER_BOT_USERNAME;

    if (isFromHelperBotById || isFromHelperBotByUsername) {
        console.log(`[HELPER_MSG_DETECTED] Message from DicesHelperBot (User ID: ${msg.from.id}, Username: @${msg.from.username || 'N/A'}) in chat ${chatId}. Text: "${text}"`);

        const gameSession = await getGroupSession(chatId); // From Part 2
        console.log(`[HELPER_MSG_DETECTED] Current gameSession for chat ${chatId}:`, gameSession ? `GameID: ${gameSession.currentGameId}, Type: ${gameSession.currentGameType}` : "No active gameSession object for this chat.");

        if (gameSession && gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
            const gameData = activeGames.get(gameSession.currentGameId);
            console.log(`[HELPER_MSG_DETECTED] GameData for ${gameSession.currentGameId}: Type: ${gameData.type}, Status: ${gameData.status}, CurrentPlayer (human): ${gameData.currentPlayerId}`);

            if (gameData.type === 'dice_escalator' &&
                gameData.status === 'waiting_player_roll_via_helper' &&
                gameData.currentPlayerId) { // currentPlayerId is the ID of the HUMAN player who was prompted

                const rollValue = parseInt(text.trim(), 10);
                if (!isNaN(rollValue) && rollValue >= 1 && rollValue <= 6) { // Assuming D6
                    console.log(`[HELPER_ROLL_VALID] Helper bot rolled ${rollValue}. This roll is for human player ${gameData.currentPlayerId} in game ${gameData.gameId}`);
                    // It's important that processDiceEscalatorPlayerRoll uses gameData.gameSetupMessageId to edit messages
                    await processDiceEscalatorPlayerRoll(gameData, rollValue);
                } else {
                    console.warn(`[HELPER_ROLL_ERR] Could not parse roll value "${text}" from helper bot for game ${gameData.gameId}. Expected a plain number.`);
                    const helperBotName = DICES_HELPER_BOT_ID ? `Helper Bot (ID: ${DICES_HELPER_BOT_ID})` : (DICES_HELPER_BOT_USERNAME ? `@${DICES_HELPER_BOT_USERNAME}` : "the Dice Helper Bot");
                    // Notify the player whose turn it was
                    await safeSendMessage(gameData.currentPlayerId, `There was an issue reading the roll ("${escapeMarkdownV2(text)}") from ${helperBotName}. Please click 'Prompt Roll' again if you wish to retry.`, {parse_mode: 'MarkdownV2'});
                }
            } else {
                 console.log(`[HELPER_MSG_STATE_MISMATCH] Helper message received, but game state is not expecting it for game ${gameSession.currentGameId}. GameType: ${gameData.type}, Status: ${gameData.status}, ExpectedPlayer (human): ${gameData.currentPlayerId}`);
            }
        } else {
            console.log(`[HELPER_MSG_NO_GAME] Helper message received, but no active game session for chat ${chatId} or gameId ${gameSession ? gameSession.currentGameId : 'N/A'} not found in activeGames map.`);
        }
        return; // IMPORTANT: Stop further processing for helper bot messages
    }


    // --- Process User Commands (if not from helper bot and starts with '/') ---
    if (text.startsWith('/')) {
        userCooldowns.set(userId, now); // Update cooldown *after* passing helper check and confirming it's a command
        const args = text.substring(1).split(' ');
        const command = args.shift().toLowerCase();
        console.log(`[CMD RCV] Chat: ${chatId}, User: ${userId}, Command: /${command}, Args: ${args.join(' ')}`);
        await getUser(userId, msg.from.username); // Ensure user exists in our system

        switch (command) {
            case 'start':
            case 'help':
                await handleHelpCommand(chatId, msg.from);
                break;
            case 'balance':
            case 'bal':
                await handleBalanceCommand(chatId, msg.from);
                break;
            case 'startcoinflip':
                 if (chatType !== 'private') { // Ensure it's a group or supergroup
                    let betAmountCF = args[0] ? parseInt(args[0],10) : 10; // Default bet
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
                 if (chatType !== 'private') { // Ensure it's a group or supergroup
                    let betAmountRPS = args[0] ? parseInt(args[0],10) : 10; // Default bet
                    if (isNaN(betAmountRPS) || betAmountRPS < MIN_BET_AMOUNT || betAmountRPS > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet. Amount must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startrps <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartGroupRPSCommand(chatId, msg.from, betAmountRPS, messageId);
                 } else {
                    await safeSendMessage(chatId, "This Rock Paper Scissors game is designed for group chats.", {});
                 }
                break;
            case 'startdiceescalator': // Ensure command is one word
                if (chatType === 'group' || chatType === 'supergroup') {
                    let betAmountDE = args[0] ? parseInt(args[0], 10) : 10;
                    if (isNaN(betAmountDE) || betAmountDE < MIN_BET_AMOUNT || betAmountDE > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet for Dice Escalator. Amount must be between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT}. Usage: \`/startdiceescalator <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartDiceEscalatorCommand(chatId, msg.from, betAmountDE, messageId);
                } else {
                    await safeSendMessage(chatId, "Dice Escalator can only be started in a group chat.", {});
                }
                break;
            default:
                // Only send "Unknown command" if it's a group where the bot might be expected to have more commands,
                // or if it's a private chat. For general group messages not starting with a known command,
                // it's better to stay silent unless the bot is explicitly mentioned or involved in an active game.
                if (chatType === 'private' || ((chatType === 'group' || chatType === 'supergroup') && text.startsWith('/'))) {
                    await safeSendMessage(chatId, "Unknown command. Try /help.", {});
                }
        }
    }
    // Non-command messages from users are ignored by this point unless specific stateful input handling is added later.
});

// --- Callback Query Handler ---
bot.on('callback_query', async (callbackQuery) => {
    const msg = callbackQuery.message;
    if (!msg) {
        console.warn("[CBQ_WARN] Callback query received without an associated message:", callbackQuery.id);
        bot.answerCallbackQuery(callbackQuery.id, { text: "This action may have expired or the original message is unavailable." }).catch(e => console.error("Error answering CBQ (no msg):", e.message));
        return;
    }

    const userId = String(callbackQuery.from.id);
    const chatId = String(msg.chat.id); // Use chat ID from the message associated with the callback
    const data = callbackQuery.data;
    const originalMessageId = msg.message_id;

    console.log(`[CBQ RCV] Chat: ${chatId}, User: ${userId} (@${callbackQuery.from.username||'N/A'}), Data: "${data}", MsgID: ${originalMessageId}`);
    bot.answerCallbackQuery(callbackQuery.id).catch(e => console.warn(`[CBQ_WARN] Failed to answer callback query ${callbackQuery.id}: ${e.message}`)); // Answer immediately

    await getUser(userId, callbackQuery.from.username); // Ensure user exists

    const [action, ...params] = data.split(':');

    try {
        switch (action) {
            case 'join_game': // Params: gameId
                await handleJoinGameCallback(chatId, callbackQuery.from, params[0], originalMessageId);
                break;
            case 'cancel_game': // Params: gameId
                await handleCancelGameCallback(chatId, callbackQuery.from, params[0], originalMessageId);
                break;
            case 'rps_choose': // Params: gameId, choice (rock/paper/scissors)
                await handleRPSChoiceCallback(chatId, callbackQuery.from, params[0], params[1], originalMessageId);
                break;
            case 'de_roll_prompt': // dice_escalator_roll_prompt, params: gameId
                await handleDiceEscalatorPlayerAction(params[0], userId, 'roll_prompt', originalMessageId, chatId);
                break;
            case 'de_cashout': // dice_escalator_cashout, params: gameId
                await handleDiceEscalatorPlayerAction(params[0], userId, 'cashout', originalMessageId, chatId);
                break;
            default:
                console.log(`[CBQ_INFO] Unknown callback action: ${action} from user ${userId} for message ${originalMessageId}`);
                // Optionally, edit the message to say "Action expired or unknown" if appropriate
                // For now, just log it. The user already got an answerCallbackQuery.
        }
    } catch (error) {
        console.error(`[CBQ_ERROR] Error processing CBQ data "${data}" for user ${userId} (MsgID: ${originalMessageId}):`, error);
        // Send a private message to the user who clicked the button if an error occurs
        await safeSendMessage(userId, "Sorry, there was an error processing your action. Please try again or start a new game if the problem persists.", {}).catch();
    }
});

// --- Command Handler Functions (Help, Balance) ---
async function handleHelpCommand(chatId, userObject) {
    const userMention = createUserMention(userObject); // From Part 3
    const helperBotNameForHelp = DICES_HELPER_BOT_ID
        ? `the configured Helper Bot (ID: ${DICES_HELPER_BOT_ID})`
        : (DICES_HELPER_BOT_USERNAME && DICES_HELPER_BOT_USERNAME !== "YourDiceHelperBotUsername" ? `@${DICES_HELPER_BOT_USERNAME}` : "the configured Dice Helper Bot");

    const helpTextParts = [
        `👋 Hello ${userMention}! Welcome to the Group Casino Bot v${BOT_VERSION}.`, // BOT_VERSION from Part 1
        `This is a simplified bot for playing games in group chats.`,
        `Available commands:`,
        `▫️ \`/help\` - Shows this help message.`,
        `▫️ \`/balance\` or \`/bal\` - Check your current game credits.`,
        `▫️ \`/startcoinflip <bet_amount>\` - Starts a Coinflip game for one opponent. Example: \`/startcoinflip 10\``,
        `▫️ \`/startrps <bet_amount>\` - Starts a Rock Paper Scissors game for one opponent. Example: \`/startrps 5\``,
        `▫️ \`/startdiceescalator <bet_amount>\` - Play Dice Escalator against the Bot! Example: \`/startdiceescalator 20\``,
        `➡️ For Dice Escalator, after clicking 'Prompt Roll', send the 🎲 emoji to have ${helperBotNameForHelp} determine your roll.`,
        `➡️ For other games like Coinflip or RPS, click the 'Join Game' button when someone starts one!`,
        `\nGame bets are between ${MIN_BET_AMOUNT} and ${MAX_BET_AMOUNT} credits.`,
        `Have fun and play responsibly!`
    ];
    // safeSendMessage (from Part 1) will handle MarkdownV2 escaping internally if specified in options.
    // We pass the raw string here, and options will have parse_mode.
    await safeSendMessage(chatId, helpTextParts.join('\n\n'), { parse_mode: 'MarkdownV2' });
}

async function handleBalanceCommand(chatId, userObject) {
    const user = await getUser(String(userObject.id)); // from Part 2
    const userMention = createUserMention(userObject);
    const balanceMessage = `${userMention}, your current balance is: *${formatCurrency(user.balance)}*.`; // formatCurrency from Part 3
    await safeSendMessage(chatId, balanceMessage, { parse_mode: 'MarkdownV2' });
}


// --- Group Game Flow Functions (Coinflip, RPS - ensure they use gameId and interactionMessageId correctly) ---
async function handleStartGroupCoinFlipCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, "Group Chat");
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}* (ID: \`${gameSession.currentGameId}\`). Please wait for it to finish or be cancelled.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance is too low (${escapeMarkdownV2(formatCurrency(initiator.balance))}) for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_coinflip_init:${gameId}`, chatId);
    const gameDataCF = { type: 'coinflip', gameId, chatId: String(chatId), initiatorId, initiatorMention: createUserMention(initiatorUser), betAmount, participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser), betPlaced: true }], status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null };
    activeGames.set(gameId, gameDataCF);
    await updateGroupGameDetails(chatId, gameId, 'CoinFlip', betAmount);
    const joinMsgCF = `${createUserMention(initiatorUser)} has started a *Coin Flip Challenge* for ${escapeMarkdownV2(formatCurrency(betAmount))}!\nAn opponent is needed. Click "Join Game" to accept!`;
    const kbCF = { inline_keyboard: [[{ text: "🪙 Join Coinflip!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]]};
    const setupMsgCF = await safeSendMessage(chatId, joinMsgCF, { parse_mode: 'MarkdownV2', reply_markup: kbCF });
    if (setupMsgCF) {
        const gameToUpdate = activeGames.get(gameId);
        if (gameToUpdate) gameToUpdate.gameSetupMessageId = setupMsgCF.message_id;
    }

    setTimeout(async () => {
        const gdCF = activeGames.get(gameId);
        if (gdCF && gdCF.status === 'waiting_opponent') {
            console.log(`[GAME_TIMEOUT] Coinflip game ${gameId} in chat ${chatId} timed out.`);
            await updateUserBalance(gdCF.initiatorId, gdCF.betAmount, `refund_coinflip_timeout:${gameId}`, chatId);
            activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgTextCF = `The Coin Flip game (ID: \`${gameId}\`) started by ${gdCF.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdCF.betAmount))} has expired due to no opponent joining. The bet has been refunded.`;
            if (gdCF.gameSetupMessageId) {
                 bot.editMessageText(escapeMarkdownV2(timeoutMsgTextCF), {chatId: String(chatId), message_id: Number(gdCF.gameSetupMessageId), parse_mode:'MarkdownV2',reply_markup:{}}).catch(e=>safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgTextCF),{parse_mode:'MarkdownV2'}));
            } else {
                safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgTextCF),{parse_mode:'MarkdownV2'});
            }
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleStartGroupRPSCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, "Group Chat");
    const gameId = generateGameId();
    if (gameSession.currentGameId) { await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}*. Please wait.`, {parse_mode:'MarkdownV2'}); return; }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) { await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance (${escapeMarkdownV2(formatCurrency(initiator.balance))}) is too low for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`, {parse_mode:'MarkdownV2'}); return; }
    await updateUserBalance(initiatorId, -betAmount, `bet_rps_init:${gameId}`, chatId);
    const gameDataRPS = { type: 'rps', gameId, chatId: String(chatId), initiatorId, initiatorMention: createUserMention(initiatorUser), betAmount, participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser), betPlaced: true }], status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null };
    activeGames.set(gameId, gameDataRPS);
    await updateGroupGameDetails(chatId, gameId, 'RockPaperScissors', betAmount);
    const joinMsgRPS = `${createUserMention(initiatorUser)} challenges someone to *Rock Paper Scissors* for ${escapeMarkdownV2(formatCurrency(betAmount))}!\nClick "Join Game" to play!`;
    const kbRPS = { inline_keyboard: [[{ text: "🪨📄✂️ Join RPS!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]]};
    const setupMsgRPS = await safeSendMessage(chatId, joinMsgRPS, { parse_mode: 'MarkdownV2', reply_markup: kbRPS });
     if (setupMsgRPS) {
        const gameToUpdate = activeGames.get(gameId);
        if (gameToUpdate) gameToUpdate.gameSetupMessageId = setupMsgRPS.message_id;
    }

    setTimeout(async () => {
        const gdRPS = activeGames.get(gameId);
        if (gdRPS && gdRPS.status === 'waiting_opponent') {
             console.log(`[GAME_TIMEOUT] RPS game ${gameId} in chat ${chatId} timed out.`);
             await updateUserBalance(gdRPS.initiatorId, gdRPS.betAmount, `refund_rps_timeout:${gameId}`, chatId);
             activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
             const timeoutMsgTextRPS = `The RPS game (ID: \`${gameId}\`) by ${gdRPS.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gdRPS.betAmount))} expired. Bet refunded.`;
            if (gdRPS.gameSetupMessageId) bot.editMessageText(escapeMarkdownV2(timeoutMsgTextRPS), {chatId:String(chatId), message_id:Number(gdRPS.gameSetupMessageId), parse_mode:'MarkdownV2',reply_markup:{}}).catch(e=>safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgTextRPS),{parse_mode:'MarkdownV2'}));
            else safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgTextRPS),{parse_mode:'MarkdownV2'});
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleJoinGameCallback(chatId, joinerUser, gameId, interactionMessageId) {
    const joinerId = String(joinerUser.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== String(chatId)) {
        await safeSendMessage(joinerId, "This game is no longer available or is not in this chat.", {});
        if(interactionMessageId) bot.editMessageReplyMarkup({}, {chat_id:String(chatId), message_id:Number(interactionMessageId)}).catch(()=>{});
        return;
    }
    if (gameData.initiatorId === joinerId) {
        await safeSendMessage(joinerId, "You can't join a game you initiated as an opponent.", {});
        return;
    }
    if (gameData.status !== 'waiting_opponent') {
        await safeSendMessage(joinerId, "This game is already full, has started, or has been cancelled.", {});
        if(interactionMessageId && (gameData.status === 'playing' || gameData.status === 'game_over' || gameData.status === 'waiting_choices')) {
             bot.editMessageReplyMarkup({}, {chat_id:String(chatId), message_id:Number(interactionMessageId)}).catch(()=>{});
        }
        return;
    }

    const joiner = await getUser(joinerId);
    if (joiner.balance < gameData.betAmount) {
        await safeSendMessage(joinerId, `You don't have enough balance (${escapeMarkdownV2(formatCurrency(joiner.balance))}) to join this ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} game.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    await updateUserBalance(joinerId, -gameData.betAmount, `bet_placed_group_${gameData.type}_join:${gameId}`, chatId);
    gameData.participants.push({ userId: joinerId, choice: null, mention: createUserMention(joinerUser), betPlaced: true });

    if (gameData.type === 'coinflip' && gameData.participants.length === 2) {
        gameData.status = 'playing'; // Or resolve immediately
        activeGames.set(gameId, gameData); // Save before async
        const p1 = gameData.participants[0], p2 = gameData.participants[1];
        p1.choice = 'heads'; p2.choice = 'tails'; // Simplification: initiator is heads
        const cfResult = determineCoinFlipOutcome(); // From Part 4
        let winner = (cfResult.outcome === p1.choice) ? p1 : p2;
        const winnings = gameData.betAmount * 2; // Total pot
        await updateUserBalance(winner.userId, winnings, `won_group_coinflip:${gameId}`, chatId);
        const resMsg = `*CoinFlip Resolved!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n${p1.mention} (Heads) vs ${p2.mention} (Tails)\nLanded on: *${escapeMarkdownV2(cfResult.outcomeString)} ${cfResult.emoji}*!\n🎉 ${winner.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} (total ${escapeMarkdownV2(formatCurrency(winnings))} payout)!`;
        if(interactionMessageId) bot.editMessageText(resMsg, {chatId:String(chatId), message_id:Number(interactionMessageId), parse_mode:'MarkdownV2', reply_markup:{}}).catch(e=>safeSendMessage(chatId,resMsg,{parse_mode:'MarkdownV2'})); else safeSendMessage(chatId,resMsg,{parse_mode:'MarkdownV2'});
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);

    } else if (gameData.type === 'rps' && gameData.participants.length === 2) {
        gameData.status = 'waiting_choices'; activeGames.set(gameId, gameData);
        const rpsPrompt = `${gameData.participants[0].mention} & ${gameData.participants[1].mention}, your RPS match for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} is set!\nEach player click a button below to make your choice:`;
        const rpsKeyboard = {inline_keyboard: [[{text:`${RPS_EMOJIS.rock} Rock`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.ROCK}`},{text:`${RPS_EMOJIS.paper} Paper`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.PAPER}`},{text:`${RPS_EMOJIS.scissors} Scissors`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}`}]]};
        if(interactionMessageId) bot.editMessageText(escapeMarkdownV2(rpsPrompt), {chatId:String(chatId), message_id:Number(interactionMessageId), parse_mode:'MarkdownV2',reply_markup:rpsKeyboard}).catch(e=>safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt),{parse_mode:'MarkdownV2',reply_markup:rpsKeyboard})); else safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt),{parse_mode:'MarkdownV2',reply_markup:rpsKeyboard});
    } else {
        // Game type not immediately resolvable or needs more players (if we add >2 player games)
        activeGames.set(gameId, gameData); // Save participant update
        const joinedMessage = `${createUserMention(joinerUser)} has joined the ${escapeMarkdownV2(gameData.type)} game for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}! Waiting for more actions or players.`;
        if(interactionMessageId) bot.editMessageText(joinedMessage, {chatId:String(chatId), message_id:Number(interactionMessageId), parse_mode:'MarkdownV2'}).catch(e=>safeSendMessage(chatId, joinedMessage, {parse_mode:'MarkdownV2'})); else safeSendMessage(chatId, joinedMessage, {parse_mode:'MarkdownV2'});
    }
}

async function handleCancelGameCallback(chatId, cancellerUser, gameId, interactionMessageId) {
    const cancellerId = String(cancellerUser.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== String(chatId)) { await safeSendMessage(cancellerId, "This game is no longer available for cancellation.", {}); return; }
    if (gameData.initiatorId !== cancellerId) { await safeSendMessage(cancellerId, `Only the game initiator (${gameData.initiatorMention}) can cancel this game setup.`, { parse_mode: 'MarkdownV2' }); return; }
    if (gameData.status !== 'waiting_opponent' && gameData.status !== 'waiting_choices') { // Check both statuses
        await safeSendMessage(cancellerId, `This game cannot be cancelled at its current stage (${escapeMarkdownV2(gameData.status)}).`, { parse_mode: 'MarkdownV2' }); return;
    }

    // Refund all participants who had placed a bet
    for (const participant of gameData.participants) {
        if (participant.betPlaced) { // Check if they actually placed a bet
            await updateUserBalance(participant.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, chatId);
        }
    }

    const gameTypeDisplay = gameData.type.charAt(0).toUpperCase() + gameData.type.slice(1);
    const cancellationMessage = `${gameData.initiatorMention} has cancelled the ${escapeMarkdownV2(gameTypeDisplay)} game for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}. All bets have been refunded.`;

    if (interactionMessageId) {
        bot.editMessageText(cancellationMessage, {
            chat_id: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2', reply_markup: {}
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

async function handleRPSChoiceCallback(chatId, userObject, gameId, choice, interactionMessageId) {
    const userId = String(userObject.id);
    const gameData = activeGames.get(gameId);

    if (!gameData || gameData.chatId !== String(chatId) || gameData.type !== 'rps') { await safeSendMessage(userId, "This Rock Paper Scissors game is no longer available or is not for this chat.", {}); return; }
    if (gameData.status !== 'waiting_choices') { await safeSendMessage(userId, "The game is not currently waiting for choices.", {}); return; }

    const participant = gameData.participants.find(p => p.userId === userId);
    if (!participant) { await safeSendMessage(userId, "You are not a participant in this RPS game.", {}); return; }
    if (participant.choice) { await safeSendMessage(userId, `You have already chosen ${RPS_EMOJIS[participant.choice]} for this game. Waiting for opponent.`, {parse_mode:'MarkdownV2'}); return; }

    participant.choice = choice; // choice should be 'rock', 'paper', or 'scissors' from RPS_CHOICES
    await safeSendMessage(userId, `You chose ${RPS_EMOJIS[choice]}! Waiting for the other player...`, {parse_mode:'MarkdownV2'}); // Private confirmation
    console.log(`[RPS_CHOICE] User ${userId} chose ${choice} for game ${gameId}`);

    // Update the group message to show one player has chosen, but keep buttons for other player
    const otherPlayer = gameData.participants.find(p => p.userId !== userId);
    let groupUpdateMsg = `${participant.mention} has made their choice!`;
    if (otherPlayer && !otherPlayer.choice) {
        groupUpdateMsg += ` Still waiting for ${otherPlayer.mention}...`;
    }
    if (interactionMessageId) {
        bot.editMessageText(escapeMarkdownV2(groupUpdateMsg), {
            chatId: String(chatId), message_id: Number(interactionMessageId), parse_mode: 'MarkdownV2',
            reply_markup: (otherPlayer && !otherPlayer.choice) ? {inline_keyboard: [[{text:`${RPS_EMOJIS.rock} Rock`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.ROCK}`},{text:`${RPS_EMOJIS.paper} Paper`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.PAPER}`},{text:`${RPS_EMOJIS.scissors} Scissors`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}`}]]} : {}
        }).catch(e => console.warn(`[RPS_CHOICE_EDIT_WARN] Failed to edit message ${interactionMessageId}: ${e.message}`));
    }


    const allChosen = gameData.participants.length === 2 && gameData.participants.every(p => p.choice !== null);
    if (allChosen) {
        gameData.status = 'game_over'; activeGames.set(gameId, gameData); // Save before async
        const p1 = gameData.participants[0], p2 = gameData.participants[1];
        const rpsResult = determineRPSOutcome(p1.choice, p2.choice); // from Part 4

        let winnerParticipant, loserParticipant;
        if (rpsResult.result === 'win1') { winnerParticipant = p1; loserParticipant = p2; }
        else if (rpsResult.result === 'win2') { winnerParticipant = p2; loserParticipant = p1; }

        let resultMessage = `*Rock Paper Scissors: Result!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n\n` +
                            `${p1.mention} chose ${RPS_EMOJIS[p1.choice]}\n` +
                            `${p2.mention} chose ${RPS_EMOJIS[p2.choice]}\n\n` +
                            `${escapeMarkdownV2(rpsResult.description)}\n\n`;

        if (winnerParticipant) {
            const winnings = gameData.betAmount * 2; // Winner gets the full pot
            await updateUserBalance(winnerParticipant.userId, winnings, `won_group_rps_resolved:${gameId}`, chatId);
            resultMessage += `🎉 ${winnerParticipant.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} (total ${escapeMarkdownV2(formatCurrency(winnings))} payout)!\n`;
            // Loser's balance was already debited.
        } else { // Draw
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_group_rps_draw:${gameId}`, chatId); // Refund p1
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_group_rps_draw:${gameId}`, chatId); // Refund p2
            resultMessage += `It's a draw! Bets of ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} are refunded to both players.`;
        }

        // Edit the original game setup message (interactionMessageId should be gameData.gameSetupMessageId if available)
        const finalMsgId = Number(gameData.gameSetupMessageId || interactionMessageId);
        if(finalMsgId) bot.editMessageText(resultMessage, { chatId: String(chatId), message_id: finalMsgId, parse_mode: 'MarkdownV2', reply_markup: {} }).catch(e => safeSendMessage(chatId, resultMessage, {parse_mode:'MarkdownV2'}));
        else safeSendMessage(chatId, resultMessage, {parse_mode:'MarkdownV2'});

        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    } else {
        activeGames.set(gameId, gameData); // Save updated participant choice if still waiting for others
    }
}

// --- Dice Escalator Game Functions ---
async function handleStartDiceEscalatorCommand(chatId, initiatorUser, betAmount, commandMessageId) {
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, "Group Chat");
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown')}* (ID: \`${gameSession.currentGameId}\`). Please wait.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, your balance is too low (${escapeMarkdownV2(formatCurrency(initiator.balance))}) for a ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    await updateUserBalance(initiatorId, -betAmount, `bet_placed_dice_escalator:${gameId}`, chatId);
    const gameData = {
        type: 'dice_escalator', gameId, chatId: String(chatId), initiatorId, initiatorMention: createUserMention(initiatorUser),
        betAmount, playerScore: 0, botScore: 0,
        status: 'player_turn_prompt_action',
        currentPlayerId: initiatorId,
        bustValue: DICE_ESCALATOR_BUST_ON,
        creationTime: Date.now(), commandMessageId,
        gameSetupMessageId: null // This will be the ID of the message with the Roll/Cashout buttons
    };
    activeGames.set(gameId, gameData);
    await updateGroupGameDetails(chatId, gameId, 'DiceEscalator', betAmount);

    const initialMessage = `${gameData.initiatorMention} started *Dice Escalator* vs the Bot for ${escapeMarkdownV2(formatCurrency(betAmount))}!\n\nYour current potential winnings (score this round): *${escapeMarkdownV2(formatCurrency(0))}*. Roll a *${gameData.bustValue}* and you bust!`;
    const keyboard = { inline_keyboard: [
        [{ text: "🎲 Prompt User to Roll Dice", callback_data: `de_roll_prompt:${gameId}` }],
        [{ text: `💰 Cashout ${escapeMarkdownV2(formatCurrency(0))}`, callback_data: `de_cashout:${gameId}` }]
    ]};
    const sentMsg = await safeSendMessage(chatId, initialMessage, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
    if (sentMsg) {
        const gameToUpdate = activeGames.get(gameId); // Re-fetch to ensure we have the latest if any async happened
        if(gameToUpdate) gameToUpdate.gameSetupMessageId = sentMsg.message_id;
    } else {
        console.error(`[DE_START_ERR] Failed to send initial game message for game ${gameId}. Refunding and cleaning up.`);
        await updateUserBalance(initiatorId, betAmount, `refund_dice_escalator_setup_fail:${gameId}`, chatId);
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    }
}

async function handleDiceEscalatorPlayerAction(gameId, userId, actionType, interactionMessageId, chatId) {
    const gameData = activeGames.get(gameId);

    if (!gameData ||
        gameData.chatId !== String(chatId) ||
        gameData.type !== 'dice_escalator' ||
        gameData.currentPlayerId !== String(userId) ||
        gameData.status !== 'player_turn_prompt_action') {
        await safeSendMessage(userId, "It's not your turn, this game isn't active, or that action isn't available right now.", {});
        if (interactionMessageId && gameData && gameData.chatId === String(chatId)) { // Check gameData exists before accessing its chatId
             bot.editMessageReplyMarkup({}, {chat_id: String(chatId), message_id: Number(interactionMessageId)}).catch(()=>{});
        }
        return;
    }

    const messageIdToUpdate = Number(interactionMessageId) || Number(gameData.gameSetupMessageId);
    if (!messageIdToUpdate) {
        console.error(`[DE_ACTION_ERR] No messageId available to update for game ${gameId}, action ${actionType}. ChatID: ${chatId}`);
        await safeSendMessage(String(chatId), `Error: Could not update game display for ${gameData.initiatorMention}. Please try starting a new game.`, {parse_mode: 'MarkdownV2'});
        return;
    }

    if (actionType === 'roll_prompt') {
        gameData.status = 'waiting_player_roll_via_helper';
        activeGames.set(gameId, gameData);

        const helperBotNameToMention = DICES_HELPER_BOT_ID
            ? `the Helper Bot (ID: ${DICES_HELPER_BOT_ID})`
            : (DICES_HELPER_BOT_USERNAME && DICES_HELPER_BOT_USERNAME !== "YourDiceHelperBotUsername" ? `@${DICES_HELPER_BOT_USERNAME}` : "the Dice Helper Bot");

        const promptMsg = `${gameData.initiatorMention}, please send the 🎲 emoji now to have ${helperBotNameToMention} determine your roll.\n\n_Waiting for roll from helper..._`;

        console.log(`[DEBUG_DE_ROLL_PROMPT] Preparing to edit message. ChatID value: '${chatId}', InteractionMessageId (messageIdToUpdate) value: '${messageIdToUpdate}'`);
        const editOptions = {
            chat_id: String(chatId),
            message_id: Number(messageIdToUpdate),
            parse_mode: 'MarkdownV2',
            reply_markup: {}
        };
        console.log('[DEBUG_DE_ROLL_PROMPT] editOptions:', JSON.stringify(editOptions));
        await bot.editMessageText(escapeMarkdownV2(promptMsg), editOptions);

    } else if (actionType === 'cashout') {
        const cashedOutScore = gameData.playerScore;
        const totalReturnToPlayer = gameData.betAmount + cashedOutScore;

        await updateUserBalance(userId, totalReturnToPlayer, `cashout_dice_escalator_player:${gameId}`, chatId);

        let cashoutMessage = `${gameData.initiatorMention} cashed out with a score of *${escapeMarkdownV2(String(cashedOutScore))}* credits! Your original bet of ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} plus ${escapeMarkdownV2(formatCurrency(cashedOutScore))} winnings have been credited.`;
        gameData.status = 'player_cashed_out';
        activeGames.set(gameId, gameData);

        cashoutMessage += `\n\n🤖 Now it's the Bot's turn to try and beat your score of *${escapeMarkdownV2(String(cashedOutScore))}*!`;
        await bot.editMessageText(cashoutMessage, { chatId: String(chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: {} });
        await sleep(2000);
        await processDiceEscalatorBotTurn(gameData, Number(messageIdToUpdate));
    } else {
        console.warn(`[DE_ACTION_ERR] Unknown actionType: ${actionType} for game ${gameId}`);
        await safeSendMessage(userId, "Unknown action selected.", {});
    }
}

async function processDiceEscalatorPlayerRoll(gameData, playerRoll) {
    const { gameId, chatId, initiatorMention, betAmount, bustValue, gameSetupMessageId, commandMessageId } = gameData;
    let playerBusted = false;
    let newPlayerScore = gameData.playerScore;

    // Prioritize gameSetupMessageId (the message with buttons) for edits
    const messageIdToUpdate = Number(gameSetupMessageId) || Number(commandMessageId);

    if (!messageIdToUpdate) {
        console.error(`[DE_PLAYER_ROLL_ERR] No messageIdToUpdate for game ${gameId}. Cannot display roll result to chat ${chatId}.`);
        const playerMessage = `${initiatorMention}, your roll of ${playerRoll} was processed. Current score: ${newPlayerScore + (playerRoll === bustValue ? 0 : playerRoll)}. (Display error occurred)`;
        await safeSendMessage(chatId, playerMessage, {}); // Fallback to sending a new message

        // Update game state even if message fails
        if (playerRoll === bustValue) {
            gameData.status = 'game_over_player_bust';
        } else {
            gameData.playerScore += playerRoll;
            gameData.status = 'player_turn_prompt_action';
        }
        activeGames.set(gameId, gameData);
        if(playerRoll === bustValue) {
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
        }
        return;
    }
    const helperBotName = DICES_HELPER_BOT_ID ? `Helper Bot (ID: ${DICES_HELPER_BOT_ID})` : (DICES_HELPER_BOT_USERNAME && DICES_HELPER_BOT_USERNAME !== "YourDiceHelperBotUsername" ? `@${DICES_HELPER_BOT_USERNAME}` : "the Dice Helper Bot");
    let turnResultMessage = `${initiatorMention}, ${helperBotName} reported your roll: ${formatDiceRolls([playerRoll])}!\n`;

    if (playerRoll === bustValue) {
        playerBusted = true;
        gameData.status = 'game_over_player_bust';
        // Player loses initial bet, accumulated score is wiped. Balance was already debited for bet.
        turnResultMessage += `💥 Oh no! You rolled a *${bustValue}* and BUSTED! You lose your ${escapeMarkdownV2(formatCurrency(betAmount))} bet and any accumulated score for this round.`;
        await bot.editMessageText(turnResultMessage, { chatId: String(chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: {} });
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    } else {
        newPlayerScore += playerRoll;
        gameData.status = 'player_turn_prompt_action';
        turnResultMessage += `Your current potential winnings (score this round): *${escapeMarkdownV2(formatCurrency(newPlayerScore))}*.`;
        const keyboard = {
            inline_keyboard: [
                [{ text: `🎲 Prompt Roll Again (Current: ${formatCurrency(newPlayerScore)})`, callback_data: `de_roll_prompt:${gameId}` }],
                [{ text: `💰 Cashout ${escapeMarkdownV2(formatCurrency(newPlayerScore))}`, callback_data: `de_cashout:${gameId}` }]
            ]
        };
        await bot.editMessageText(turnResultMessage, { chatId: String(chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: keyboard });
    }
    gameData.playerScore = newPlayerScore; // Update score if not busted
    activeGames.set(gameId, gameData);
}

async function processDiceEscalatorBotTurn(gameData, messageIdToUpdate) {
    const { gameId, chatId, initiatorMention, betAmount, playerScore: playerScoreToBeat, bustValue } = gameData;
    let botCurrentScore = 0;
    let botBusted = false;
    let botRollsMade = 0;
    let botPlaysMessage = `${initiatorMention} cashed out at *${escapeMarkdownV2(String(playerScoreToBeat))}* credits.\n\n🤖 Bot's turn! Target to beat: *${playerScoreToBeat}*.\n`;

    if (!messageIdToUpdate) {
        console.error(`[DE_BOT_TURN_ERR] No messageIdToUpdate for game ${gameId}. Cannot display bot's turn in chat ${chatId}.`);
        messageIdToUpdate = (await safeSendMessage(String(chatId), botPlaysMessage + "_Bot is thinking..._", { parse_mode: 'MarkdownV2' }))?.message_id;
        if (!messageIdToUpdate) {
            console.error(`[DE_BOT_TURN_ERR] Critical: Failed to send new message for bot's turn display. Aborting turn for game ${gameId}.`);
            activeGames.delete(gameId);
            await updateGroupGameDetails(chatId, null, null, null);
            return;
        }
    } else {
        await bot.editMessageText(escapeMarkdownV2(botPlaysMessage) + "_Bot is thinking..._", { chatId: String(chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: {} });
    }
    await sleep(2000);

    while (botRollsMade < DICE_ESCALATOR_BOT_ROLLS && botCurrentScore <= playerScoreToBeat && !botBusted) {
        botRollsMade++;
        const botRollResult = determineDieRollOutcome(); // Bot uses its internal roller
        botPlaysMessage += `Bot roll ${botRollsMade}: ${botRollResult.emoji} (Rolled a *${botRollResult.roll}*)\n`;

        if (botRollResult.roll === bustValue) {
            botBusted = true;
            botCurrentScore = 0;
            botPlaysMessage += `💥 Bot BUSTED by rolling a *${bustValue}*!\n`;
            break;
        }
        botCurrentScore += botRollResult.roll;
        botPlaysMessage += `Bot score is now: *${botCurrentScore}*\n`;
        let nextPromptForBot = (botCurrentScore <= playerScoreToBeat && botRollsMade < DICE_ESCALATOR_BOT_ROLLS && !botBusted) ? "\n_Bot rolls again..._" : "";
        await bot.editMessageText(escapeMarkdownV2(botPlaysMessage) + nextPromptForBot, { chatId: String(chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: {} });
        if (nextPromptForBot) {
            await sleep(2500); // Slower sleep for bot rolls
        }
    }
    await sleep(1500);

    let finalMessage = botPlaysMessage;
    gameData.status = 'game_over_bot_played';
    gameData.botScore = botCurrentScore;

    if (botBusted || botCurrentScore <= playerScoreToBeat) {
        finalMessage += `\n🎉 Congratulations, ${initiatorMention}! The Bot (Score: ${botCurrentScore}${botBusted ? ", Busted" : ""}) didn't beat your cashed-out score of *${escapeMarkdownV2(String(playerScoreToBeat))}*. You secured your win!`;
        // Player balance was already updated positively when they cashed out. No further action needed.
    } else { // Bot wins
        finalMessage += `\n😢 Tough luck, ${initiatorMention}! The Bot beat your score, reaching *${escapeMarkdownV2(String(botCurrentScore))}*! The house takes this conceptual win.`;
        // Player keeps what they cashed out. The original bet amount is effectively lost to "the house".
        // No further negative adjustment to player balance is made here; their cashout was their final transaction for this game.
    }

    finalMessage += `\n\nGame Over. Final Scores -> You: *${escapeMarkdownV2(String(playerScoreToBeat))}* (Cashed Out) | Bot: *${escapeMarkdownV2(String(botCurrentScore))}*${botBusted ? " (Busted)" : ""}.`;
    finalMessage += `\nYour balance was updated when you cashed out.`;

    await bot.editMessageText(finalMessage, { chatId: String(chatId), message_id: Number(messageIdToUpdate), parse_mode: 'MarkdownV2', reply_markup: {} });
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
}


console.log("Part 5: Message & Callback Handling, Basic Game Flow - Complete.");
// End of Part 5
//---------------------------------------------------------------------------
// index.js - Part 6: Startup, Shutdown, and Basic Error Handling
//---------------------------------------------------------------------------
console.log("Loading Part 6: Startup, Shutdown, and Basic Error Handling...");

async function runPeriodicBackgroundTasks() { /* ... as before, maybe add dice_escalator to cleanup logic ... */
    console.log(`[BACKGROUND_TASK] [${new Date().toISOString()}] Running periodic background tasks...`);
    const now = Date.now();
    const GAME_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS * 5;
    let cleanedGames = 0;
    for (const [gameId, gameData] of activeGames.entries()) {
        if (now - gameData.creationTime > GAME_CLEANUP_THRESHOLD_MS &&
            (gameData.status === 'waiting_opponent' || gameData.status === 'waiting_choices' || gameData.status === 'waiting_player_roll_via_helper')) {
            console.warn(`[BACKGROUND_TASK] Cleaning up stale game ${gameId} (${gameData.type}) in chat ${gameData.chatId}. Status: ${gameData.status}`);
            if (gameData.initiatorId && gameData.betAmount > 0 && (gameData.status === 'waiting_opponent' || (gameData.type==='dice_escalator' && gameData.currentPlayerId === gameData.initiatorId && gameData.playerScore ===0 ))) {
                await updateUserBalance(gameData.initiatorId, gameData.betAmount, `refund_stale_${gameData.type}:${gameId}`, gameData.chatId);
                const staleMsg = `Game (ID: \`${gameId}\`) by ${gameData.initiatorMention} cleared due to inactivity. Bet refunded.`;
                 if(gameData.gameSetupMessageId) bot.editMessageText(escapeMarkdownV2(staleMsg), {chat_id: gameData.chatId, message_id: gameData.gameSetupMessageId, parse_mode: 'MarkdownV2', reply_markup:{}}).catch(()=>{safeSendMessage(gameData.chatId, escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'})});
                 else safeSendMessage(gameData.chatId, escapeMarkdownV2(staleMsg), {parse_mode:'MarkdownV2'});
            }
            activeGames.delete(gameId);
            const groupSession = await getGroupSession(gameData.chatId);
            if (groupSession && groupSession.currentGameId === gameId) await updateGroupGameDetails(gameData.chatId, null, null, null);
            cleanedGames++;
        }
    }
    if (cleanedGames > 0) console.log(`[BACKGROUND_TASK] Cleaned ${cleanedGames} stale game(s).`);
    /* ... rest of cleanup as before ... */
}
// const backgroundTaskInterval = setInterval(runPeriodicBackgroundTasks, 15 * 60 * 1000);


process.on('uncaughtException', (error, origin) => { /* ... as before ... */
    console.error(`\n🚨🚨 UNCAUGHT EXCEPTION AT: ${origin} 🚨🚨`, error);
    if(ADMIN_USER_ID) safeSendMessage(ADMIN_USER_ID, `🆘 UNCAUGHT EXCEPTION:\nOrigin: ${origin}\nError: ${error.message}\nBot might be unstable.`, {}).catch();
});
process.on('unhandledRejection', (reason, promise) => { /* ... as before ... */
    console.error(`\n🔥🔥 UNHANDLED REJECTION 🔥🔥`, reason, promise);
    if(ADMIN_USER_ID) safeSendMessage(ADMIN_USER_ID, `♨️ UNHANDLED REJECTION:\nReason: ${escapeMarkdownV2(String(reason instanceof Error ? reason.message : reason))}`, {parse_mode:'MarkdownV2'}).catch();
});

let isShuttingDown = false;
async function shutdown(signal) { /* ... as before ... */
    if (isShuttingDown) return; isShuttingDown = true;
    console.log(`\n🚦 ${signal}. Shutting down Group Casino Bot v${BOT_VERSION}...`);
    if (bot && bot.isPolling()) { try { await bot.stopPolling({cancel:true}); console.log("Polling stopped."); } catch(e){console.error("Err stopping poll:",e.message);} }
    // if (backgroundTaskInterval) clearInterval(backgroundTaskInterval);
    if (ADMIN_USER_ID) await safeSendMessage(ADMIN_USER_ID, `ℹ️ Bot v${BOT_VERSION} shutting down (Signal: ${signal}).`).catch(()=>{});
    console.log("✅ Shutdown complete. Exiting.");
    process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// --- Main Application Startup Function ---
async function main() {
    console.log(`\n🚀🚀🚀 Initializing Solana Group Chat Casino Bot v${BOT_VERSION} (Simplified Mode) 🚀🚀🚀`);
    console.log(`Timestamp: ${new Date().toISOString()}`);
    console.log("Attempting to connect to Telegram API...");

    // TEMPORARY DEBUGGING LOG FOR RAILWAY:
    const tokenForDebug = process.env.BOT_TOKEN;
    if (tokenForDebug) {
        console.log(`[DEBUG] Token being used by bot.getMe(): First 5 chars: '${tokenForDebug.substring(0, 5)}', Last 5 chars: '${tokenForDebug.substring(tokenForDebug.length - 5)}', Length: ${tokenForDebug.length}`);
    } else {
        console.log("[DEBUG] Token being used by bot.getMe() is UNDEFINED OR EMPTY at this point!");
    }
    // END TEMPORARY DEBUGGING LOG

    try {
        const me = await bot.getMe(); // Verifies the BOT_TOKEN and connection
        console.log(`✅ Successfully connected to Telegram! Bot Name: @${me.username}, Bot ID: ${me.id}`);
        // ... rest of main function
    } catch (error) {
        console.error("❌ CRITICAL STARTUP ERROR (bot.getMe() failed):");
        // TEMPORARY DEBUGGING LOG FOR RAILWAY (in case of error too):
        const tokenInError = process.env.BOT_TOKEN;
         if (tokenInError) {
            console.log(`[DEBUG_ERROR] Token at time of error: First 5: '${tokenInError.substring(0, 5)}', Last 5: '${tokenInError.substring(tokenInError.length - 5)}', Length: ${tokenInError.length}`);
        } else {
            console.log("[DEBUG_ERROR] Token at time of error was UNDEFINED OR EMPTY!");
        }
        // END TEMPORARY DEBUGGING LOG
        console.error(error); // This will print the ETELEGRAM 404 error
        console.error("Please check your BOT_TOKEN on Railway, network connection, and Telegram API status.");
        if (ADMIN_USER_ID && BOT_TOKEN) { // Check BOT_TOKEN here too
            const tempBotForError = new TelegramBot(BOT_TOKEN, {});
            tempBotForError.sendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE v${BOT_VERSION} on Railway:\n${escapeMarkdownV2(error.message)}\nToken (ends): ...${tokenInError ? tokenInError.substring(tokenInError.length - 5) : 'N/A'}\nExiting.`).catch(e => console.error("Failed to send critical startup error admin notification:", e));
        }
        process.exit(1);
    }
}

main().catch(error => {
    console.error("❌ MAIN ASYNC UNHANDLED ERROR:", error);
    process.exit(1);
});

console.log("Part 6: Startup, Shutdown, and Basic Error Handling - Complete.");
// --- END OF index.js ---
