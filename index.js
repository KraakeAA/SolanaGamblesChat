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

//---------------------------------------------------------------------------
// index.js - Part 5: Message & Callback Handling, Basic Game Flow
//---------------------------------------------------------------------------
console.log("Loading Part 5: Message & Callback Handling, Basic Game Flow...");

const COMMAND_COOLDOWN_MS = 2000;
const JOIN_GAME_TIMEOUT_MS = 60000; // General join timeout
const DICE_ESCALATOR_TIMEOUT_MS = 120000; // Longer timeout for the whole game session
const MIN_BET_AMOUNT = 5;
const MAX_BET_AMOUNT = 1000;
const DICE_ESCALATOR_BOT_ROLLS = 3; // How many times bot will attempt to roll in Dice Escalator

// Main Message Handler
bot.on('message', async (msg) => {
    if (!msg.text || !msg.from || msg.from.is_bot) return;

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text;
    const chatType = msg.chat.type;
    const messageId = msg.message_id;

    console.log(`[MSG RCV] Chat: ${chatId} (${chatType}), User: ${userId} (@${msg.from.username||'N/A'}), Text: "${text}"`);

    // Cooldown
    const now = Date.now();
    if (userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS && text.startsWith('/')) {
        console.log(`[COOLDOWN] User ${userId} command ignored.`);
        return;
    }

    // Check if this message is a dice roll from the DicesHelperBot
    // IMPORTANT: Adjust DICES_HELPER_BOT_ID or DICES_HELPER_BOT_USERNAME check as needed
    const isFromHelperBotById = DICES_HELPER_BOT_ID && userId === DICES_HELPER_BOT_ID;
    const isFromHelperBotByUsername = !DICES_HELPER_BOT_ID && msg.from.username === DICES_HELPER_BOT_USERNAME;

    if (isFromHelperBotById || isFromHelperBotByUsername) {
        console.log(`[HELPER_MSG] Message from DicesHelperBot in chat ${chatId}: "${text}"`);
        const gameSession = await getGroupSession(chatId);
        if (gameSession && gameSession.currentGameId && activeGames.has(gameSession.currentGameId)) {
            const gameData = activeGames.get(gameSession.currentGameId);
            if (gameData.type === 'dice_escalator' && gameData.status === 'waiting_player_roll_via_helper' && gameData.currentPlayerId) {
                // Assume helper bot's message text is just the number
                const rollValue = parseInt(text.trim(), 10);
                if (!isNaN(rollValue) && rollValue >= 1 && rollValue <= 6) { // Assuming D6
                    console.log(`[HELPER_ROLL] Helper bot rolled ${rollValue} for player ${gameData.currentPlayerId} in game ${gameData.gameId}`);
                    // Process this roll for the player in Dice Escalator
                    await processDiceEscalatorPlayerRoll(gameData, rollValue);
                } else {
                    console.warn(`[HELPER_ROLL_ERR] Could not parse roll value "${text}" from helper bot for game ${gameData.gameId}`);
                    // Maybe re-prompt the player or notify them of helper bot error
                    await safeSendMessage(gameData.currentPlayerId, `There was an issue reading the roll from ${DICES_HELPER_BOT_USERNAME ? '@'+DICES_HELPER_BOT_USERNAME : 'the helper bot'}. Please try asking it to roll again if you were prompted.`, {});
                }
            } else {
                 console.log(`[HELPER_MSG_IGNORE] Helper bot message, but no active game waiting for its roll, or wrong game type/status. Game: ${gameData ? gameData.type : 'N/A'}, Status: ${gameData ? gameData.status : 'N/A'}`);
            }
        } else {
            console.log(`[HELPER_MSG_IGNORE] Helper bot message, but no active game session for this chat ${chatId} or gameId ${gameSession ? gameSession.currentGameId : 'N/A'} not in activeGames.`);
        }
        return; // Don't process helper bot messages as user commands
    }


    // User Commands
    if (text.startsWith('/')) {
        userCooldowns.set(userId, now);
        const args = text.substring(1).split(' ');
        const command = args.shift().toLowerCase();
        await getUser(userId, msg.from.username); // Ensure user exists

        switch (command) {
            case 'start': case 'help':
                await handleHelpCommand(chatId, msg.from);
                break;
            case 'balance': case 'bal':
                await handleBalanceCommand(chatId, msg.from);
                break;
            case 'startcoinflip': /* ... (as in previous Part 5, can be adapted for group vs group or PvP) ... */
                 if (chatType !== 'private') await handleStartGroupCoinFlipCommand(chatId, msg.from, args[0] ? parseInt(args[0],10) : 10, messageId);
                 else await safeSendMessage(chatId, "This Coinflip is for groups!");
                break;
            case 'startrps': /* ... (as in previous Part 5) ... */
                 if (chatType !== 'private') await handleStartGroupRPSCommand(chatId, msg.from, args[0] ? parseInt(args[0],10) : 10, messageId);
                 else await safeSendMessage(chatId, "This RPS game is for groups!");
                break;
            case 'startdiceescalator': // NEW GAME COMMAND
                if (chatType === 'group' || chatType === 'supergroup') {
                    let betAmount = args[0] ? parseInt(args[0], 10) : 10; // Default bet 10
                    if (isNaN(betAmount) || betAmount < MIN_BET_AMOUNT || betAmount > MAX_BET_AMOUNT) {
                        await safeSendMessage(chatId, `Invalid bet for Dice Escalator. Amount: ${MIN_BET_AMOUNT}-${MAX_BET_AMOUNT}. Usage: \`/startdiceescalator <amount>\``, { parse_mode: 'MarkdownV2' });
                        return;
                    }
                    await handleStartDiceEscalatorCommand(chatId, msg.from, betAmount, messageId);
                } else {
                    await safeSendMessage(chatId, "Dice Escalator can only be started in a group chat.", {});
                }
                break;
            default:
                await safeSendMessage(chatId, "Unknown command. Try /help.", {});
        }
    }
});

// Callback Query Handler
bot.on('callback_query', async (callbackQuery) => {
    const msg = callbackQuery.message;
    if (!msg) {
        bot.answerCallbackQuery(callbackQuery.id, { text: "Action expired." }).catch(e => console.error("Error answering CBQ:", e.message));
        return;
    }
    const userId = String(callbackQuery.from.id);
    const chatId = String(msg.chat.id);
    const data = callbackQuery.data;
    const originalMessageId = msg.message_id;

    console.log(`[CBQ RCV] Chat: ${chatId}, User: ${userId} (@${callbackQuery.from.username||'N/A'}), Data: "${data}", MsgID: ${originalMessageId}`);
    bot.answerCallbackQuery(callbackQuery.id).catch(e => console.warn(`[CBQ_WARN] Answer CBQ ${callbackQuery.id} failed: ${e.message}`));
    await getUser(userId, callbackQuery.from.username);

    const [action, ...params] = data.split(':');

    try {
        switch (action) {
            case 'join_game': // For Coinflip, RPS
                await handleJoinGameCallback(chatId, callbackQuery.from, params[0], originalMessageId);
                break;
            case 'cancel_game': // For Coinflip, RPS
                await handleCancelGameCallback(chatId, callbackQuery.from, params[0], originalMessageId);
                break;
            case 'rps_choose': // For RPS
                await handleRPSChoiceCallback(chatId, callbackQuery.from, params[0], params[1], originalMessageId);
                break;
            // NEW DICE ESCALATOR CALLBACKS
            case 'de_roll_prompt': // dice_escalator_roll_prompt, params: gameId
                await handleDiceEscalatorPlayerAction(params[0], userId, 'roll_prompt', originalMessageId, chatId);
                break;
            case 'de_cashout': // dice_escalator_cashout, params: gameId
                await handleDiceEscalatorPlayerAction(params[0], userId, 'cashout', originalMessageId, chatId);
                break;
            default:
                console.log(`[CBQ_INFO] Unknown callback action: ${action}`);
        }
    } catch (error) {
        console.error(`[CBQ_ERROR] Processing CBQ "${data}" for user ${userId}:`, error);
        await safeSendMessage(userId, "Error processing action. Please try again or start a new game.", {}).catch();
    }
});

// Command Handlers (Help, Balance)
async function handleHelpCommand(chatId, userObject) { /* ... as before ... */
    const userMention = createUserMention(userObject);
    const helpText = [
        `👋 Hello ${userMention}! Bot v${BOT_VERSION}.`,
        `Commands:`,
        `▫️ \`/help\` - This message`,
        `▫️ \`/balance\` or \`/bal\` - Your credits`,
        `▫️ \`/startcoinflip <bet>\` - Group Coinflip`,
        `▫️ \`/startrps <bet>\` - Group Rock Paper Scissors`,
        `▫️ \`/startdiceescalator <bet>\` - Player vs Bot Dice Escalator!`,
        `➡️ Click 'Join Game' or follow prompts! Assumed helper bot for dice games: ${DICES_HELPER_BOT_ID ? `ID ${DICES_HELPER_BOT_ID}`: `@${DICES_HELPER_BOT_USERNAME}`}`
    ].join('\n\n');
    await safeSendMessage(chatId, helpText, { parse_mode: 'MarkdownV2' });
}
async function handleBalanceCommand(chatId, userObject) { /* ... as before ... */
    const user = await getUser(String(userObject.id));
    await safeSendMessage(chatId, `${createUserMention(userObject)}, your balance: *${formatCurrency(user.balance)}*.`, { parse_mode: 'MarkdownV2' });
}

// Group Game Flow Functions (Coinflip, RPS - largely as before, with minor adjustments for consistency)
async function handleStartGroupCoinFlipCommand(chatId, initiatorUser, betAmount, commandMessageId) { /* ... as before ... */
    // Ensure it uses updateGroupGameDetails appropriately
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, "Group Chat");
    const gameId = generateGameId();

    if (gameSession.currentGameId) {
        await safeSendMessage(chatId, `A game is already active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown')}* (ID: \`${gameSession.currentGameId}\`). Wait or cancel.`, { parse_mode: 'MarkdownV2' }); return;
    }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) {
        await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, balance too low for ${escapeMarkdownV2(formatCurrency(betAmount))}. You have ${escapeMarkdownV2(formatCurrency(initiator.balance))}.`, { parse_mode: 'MarkdownV2' }); return;
    }
    await updateUserBalance(initiatorId, -betAmount, `bet_placed_group_coinflip_init:${gameId}`, chatId);
    activeGames.set(gameId, { type: 'coinflip', gameId, chatId, initiatorId, initiatorMention: createUserMention(initiatorUser), betAmount, participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser) }], status: 'waiting_opponent', creationTime: Date.now(), commandMessageId });
    await updateGroupGameDetails(chatId, gameId, 'CoinFlip', betAmount);
    const joinMsg = `${createUserMention(initiatorUser)} started *Coin Flip* for ${escapeMarkdownV2(formatCurrency(betAmount))}! Opponent needed.`;
    const kb = { inline_keyboard: [[{ text: "🪙 Join Coinflip!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]]};
    const setupMsg = await safeSendMessage(chatId, joinMsg, { parse_mode: 'MarkdownV2', reply_markup: kb });
    if (setupMsg) activeGames.get(gameId).gameSetupMessageId = setupMsg.message_id;

    setTimeout(async () => { /* ... timeout logic as before ... */
        const gd = activeGames.get(gameId);
        if (gd && gd.status === 'waiting_opponent') {
            await updateUserBalance(gd.initiatorId, gd.betAmount, `refund_coinflip_timeout:${gameId}`, chatId);
            activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
            const timeoutMsgText = `Coin Flip by ${gd.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gd.betAmount))} expired. Bet refunded.`;
            if (gd.gameSetupMessageId) bot.editMessageText(escapeMarkdownV2(timeoutMsgText), {chatId, message_id:gd.gameSetupMessageId, parse_mode:'MarkdownV2',reply_markup:{}}).catch(e=>safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgText),{parse_mode:'MarkdownV2'}));
            else safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgText),{parse_mode:'MarkdownV2'});
        }
    }, JOIN_GAME_TIMEOUT_MS);
}
async function handleStartGroupRPSCommand(chatId, initiatorUser, betAmount, commandMessageId) { /* ... as before ... */
    // Ensure it uses updateGroupGameDetails appropriately
    const initiatorId = String(initiatorUser.id);
    const gameSession = await getGroupSession(chatId, "Group Chat");
    const gameId = generateGameId();
    if (gameSession.currentGameId) { /* ... game active message ... */ await safeSendMessage(chatId, `Game active: *${escapeMarkdownV2(gameSession.currentGameType || 'Unknown')}*. Wait.`, {parse_mode:'MarkdownV2'}); return; }
    const initiator = await getUser(initiatorId);
    if (initiator.balance < betAmount) { /* ... low balance message ... */ await safeSendMessage(chatId, `${createUserMention(initiatorUser)}, bal too low for ${escapeMarkdownV2(formatCurrency(betAmount))}.`, {parse_mode:'MarkdownV2'}); return; }
    await updateUserBalance(initiatorId, -betAmount, `bet_rps_init:${gameId}`, chatId);
    activeGames.set(gameId, { type: 'rps', gameId, chatId, initiatorId, initiatorMention: createUserMention(initiatorUser), betAmount, participants: [{ userId: initiatorId, choice: null, mention: createUserMention(initiatorUser), betPlaced: true }], status: 'waiting_opponent', creationTime: Date.now(), commandMessageId });
    await updateGroupGameDetails(chatId, gameId, 'RockPaperScissors', betAmount);
    const joinMsg = `${createUserMention(initiatorUser)} started *RPS* for ${escapeMarkdownV2(formatCurrency(betAmount))}! Opponent needed.`;
    const kb = { inline_keyboard: [[{ text: "🪨📄✂️ Join RPS!", callback_data: `join_game:${gameId}` }], [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]]};
    const setupMsg = await safeSendMessage(chatId, joinMsg, { parse_mode: 'MarkdownV2', reply_markup: kb });
     if (setupMsg) activeGames.get(gameId).gameSetupMessageId = setupMsg.message_id;
    setTimeout(async () => { /* ... timeout logic as before ... */
        const gd = activeGames.get(gameId);
        if (gd && gd.status === 'waiting_opponent') { /* ... refund and cleanup ... */
             await updateUserBalance(gd.initiatorId, gd.betAmount, `refund_rps_timeout:${gameId}`, chatId);
             activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
             const timeoutMsgText = `RPS by ${gd.initiatorMention} for ${escapeMarkdownV2(formatCurrency(gd.betAmount))} expired. Bet refunded.`;
            if (gd.gameSetupMessageId) bot.editMessageText(escapeMarkdownV2(timeoutMsgText), {chatId, message_id:gd.gameSetupMessageId, parse_mode:'MarkdownV2',reply_markup:{}}).catch(e=>safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgText),{parse_mode:'MarkdownV2'}));
            else safeSendMessage(chatId, escapeMarkdownV2(timeoutMsgText),{parse_mode:'MarkdownV2'});
        }
    }, JOIN_GAME_TIMEOUT_MS);
}

async function handleJoinGameCallback(chatId, joinerUser, gameId, interactionMessageId) { /* ... as before, ensuring it calls the correct next step for RPS ... */
    const joinerId = String(joinerUser.id);
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== chatId) { /* ... game not available ... */ await safeSendMessage(joinerId, "Game unavailable.", {}); bot.editMessageReplyMarkup({}, {chat_id:chatId, message_id:interactionMessageId}).catch(()=>{}); return; }
    if (gameData.initiatorId === joinerId) { /* ... can't join own game ... */ await safeSendMessage(joinerId, "Can't join own game.", {}); return; }
    if (gameData.status !== 'waiting_opponent') { /* ... game full/started ... */ await safeSendMessage(joinerId, "Game not waiting for opponent.", {}); if(gameData.status !== 'waiting_opponent') bot.editMessageReplyMarkup({}, {chat_id:chatId, message_id:interactionMessageId}).catch(()=>{}); return;}
    const joiner = await getUser(joinerId);
    if (joiner.balance < gameData.betAmount) { /* ... low balance for joiner ... */ await safeSendMessage(joinerId, `Bal too low for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}.`, {parse_mode:'MarkdownV2'}); return; }
    await updateUserBalance(joinerId, -gameData.betAmount, `bet_${gameData.type}_join:${gameId}`, chatId);
    gameData.participants.push({ userId: joinerId, choice: null, mention: createUserMention(joinerUser), betPlaced: true }); // Mark bet as placed for joiner

    if (gameData.type === 'coinflip' && gameData.participants.length === 2) {
        gameData.status = 'playing'; activeGames.set(gameId, gameData);
        const p1 = gameData.participants[0], p2 = gameData.participants[1];
        p1.choice = 'heads'; p2.choice = 'tails'; // Simplification
        const cfResult = determineCoinFlipOutcome();
        let winner = (cfResult.outcome === p1.choice) ? p1 : p2;
        let loser = (winner === p1) ? p2 : p1;
        const winnings = gameData.betAmount * 2;
        await updateUserBalance(winner.userId, winnings, `won_coinflip:${gameId}`, chatId);
        const resMsg = `*CoinFlip Resolved!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n${p1.mention} (Heads) vs ${p2.mention} (Tails)\nLanded: *${escapeMarkdownV2(cfResult.outcomeString)} ${cfResult.emoji}*!\n🎉 ${winner.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;
        if(interactionMessageId) bot.editMessageText(resMsg, {chatId, message_id:interactionMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(e=>safeSendMessage(chatId,resMsg,{parse_mode:'MarkdownV2'})); else safeSendMessage(chatId,resMsg,{parse_mode:'MarkdownV2'});
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
    } else if (gameData.type === 'rps' && gameData.participants.length === 2) {
        gameData.status = 'waiting_choices'; activeGames.set(gameId, gameData);
        const rpsPrompt = `${gameData.participants[0].mention} & ${gameData.participants[1].mention}, your RPS match for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))} is set!\nMake your choice (privately replied to you if possible, or via buttons if all in group):`;
        const rpsKeyboard = {inline_keyboard: [[{text:`${RPS_EMOJIS.rock} Rock`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.ROCK}`},{text:`${RPS_EMOJIS.paper} Paper`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.PAPER}`},{text:`${RPS_EMOJIS.scissors} Scissors`,callback_data:`rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}`}]]};
        if(interactionMessageId) bot.editMessageText(escapeMarkdownV2(rpsPrompt), {chatId, message_id:interactionMessageId, parse_mode:'MarkdownV2',reply_markup:rpsKeyboard}).catch(e=>safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt),{parse_mode:'MarkdownV2',reply_markup:rpsKeyboard})); else safeSendMessage(chatId, escapeMarkdownV2(rpsPrompt),{parse_mode:'MarkdownV2',reply_markup:rpsKeyboard});
    }
}
async function handleCancelGameCallback(chatId, cancellerUser, gameId, interactionMessageId) { /* ... as before ... */
    const cancellerId = String(cancellerUser.id);
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== chatId) { await safeSendMessage(cancellerId, "Game unavailable.", {}); return; }
    if (gameData.initiatorId !== cancellerId) { await safeSendMessage(cancellerId, `Only initiator (${gameData.initiatorMention}) can cancel.`, {parse_mode:'MarkdownV2'}); return; }
    if (gameData.status !== 'waiting_opponent' && gameData.status !== 'waiting_choices') { await safeSendMessage(cancellerId, `Can't cancel at status: ${escapeMarkdownV2(gameData.status)}.`,{parse_mode:'MarkdownV2'}); return; }
    await updateUserBalance(gameData.initiatorId, gameData.betAmount, `refund_${gameData.type}_cancel:${gameId}`, chatId);
    // If other participants joined and placed bets (e.g. in RPS waiting_choices)
    if(gameData.participants.length > 1) {
        for(const p of gameData.participants) {
            if(p.userId !== gameData.initiatorId && p.betPlaced) { // check betPlaced flag
                 await updateUserBalance(p.userId, gameData.betAmount, `refund_${gameData.type}_cancel:${gameId}`, chatId);
            }
        }
    }
    const cancelMsg = `${gameData.initiatorMention} cancelled ${escapeMarkdownV2(gameData.type)} for ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}. Bets refunded.`;
    if(interactionMessageId) bot.editMessageText(cancelMsg, {chatId, message_id:interactionMessageId, parse_mode:'MarkdownV2', reply_markup:{}}).catch(e=>safeSendMessage(chatId,cancelMsg,{parse_mode:'MarkdownV2'})); else safeSendMessage(chatId,cancelMsg,{parse_mode:'MarkdownV2'});
    activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
}
async function handleRPSChoiceCallback(chatId, userObject, gameId, choice, interactionMessageId) { /* ... as before ... */
    const userId = String(userObject.id);
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== chatId || gameData.type !== 'rps') { await safeSendMessage(userId, "RPS game unavailable.", {}); return; }
    if (gameData.status !== 'waiting_choices') { await safeSendMessage(userId, "Not waiting for choices.", {}); return; }
    const p = gameData.participants.find(p => p.userId === userId);
    if (!p) { await safeSendMessage(userId, "Not in this RPS game.", {}); return; }
    if (p.choice) { await safeSendMessage(userId, `You already chose ${RPS_EMOJIS[p.choice]}.`, {parse_mode:'MarkdownV2'}); return; }
    p.choice = choice;
    await safeSendMessage(userId, `You chose ${RPS_EMOJIS[choice]}! Waiting for opponent...`, {parse_mode:'MarkdownV2'});

    const allChosen = gameData.participants.every(par => par.choice);
    if (allChosen && gameData.participants.length === 2) {
        gameData.status = 'game_over'; activeGames.set(gameId, gameData);
        const p1 = gameData.participants[0], p2 = gameData.participants[1];
        const rpsRes = determineRPSOutcome(p1.choice, p2.choice);
        let winnerP, loserP, resText = `*RPS Result!* Bet: ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}\n${p1.mention}: ${RPS_EMOJIS[p1.choice]} vs ${p2.mention}: ${RPS_EMOJIS[p2.choice]}\n${escapeMarkdownV2(rpsRes.description)}\n`;
        if (rpsRes.result === 'win1') winnerP = p1; else if (rpsRes.result === 'win2') winnerP = p2;
        if (winnerP) {
            const winnings = gameData.betAmount * 2;
            await updateUserBalance(winnerP.userId, winnings, `won_rps:${gameId}`, chatId);
            resText += `🎉 ${winnerP.mention} wins ${escapeMarkdownV2(formatCurrency(gameData.betAmount))}!`;
        } else {
            await updateUserBalance(p1.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, chatId);
            await updateUserBalance(p2.userId, gameData.betAmount, `refund_rps_draw:${gameId}`, chatId);
            resText += `Draw! Bets refunded.`;
        }
        if(interactionMessageId) bot.editMessageText(resText, {chatId, message_id:interactionMessageId, parse_mode:'MarkdownV2',reply_markup:{}}).catch(e=>safeSendMessage(chatId,resText,{parse_mode:'MarkdownV2'})); else safeSendMessage(chatId,resText,{parse_mode:'MarkdownV2'});
        activeGames.delete(gameId); await updateGroupGameDetails(chatId, null, null, null);
    } else {
        activeGames.set(gameId, gameData); // Save updated choice
        // Update the group message to show one player has chosen
        const waitingMsgText = `${p.mention} has made their choice! Still waiting for other player(s)...`;
        if (interactionMessageId) bot.editMessageText(escapeMarkdownV2(waitingMsgText), {chatId: chatId, message_id: interactionMessageId, parse_mode:'MarkdownV2'}).catch(()=>{});
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
        type: 'dice_escalator', gameId, chatId, initiatorId, initiatorMention: createUserMention(initiatorUser),
        betAmount, playerScore: 0, botScore: 0,
        status: 'player_turn_prompt_action', // Player goes first
        currentPlayerId: initiatorId, // Human player starts
        bustValue: DICE_ESCALATOR_BUST_ON,
        creationTime: Date.now(), commandMessageId,
        gameSetupMessageId: null // Will be set after sending the initial game message
    };
    activeGames.set(gameId, gameData);
    await updateGroupGameDetails(chatId, gameId, 'DiceEscalator', betAmount);

    const initialMessage = `${createUserMention(initiatorUser)} started *Dice Escalator* vs the Bot for ${escapeMarkdownV2(formatCurrency(betAmount))}!\n\nYour current score: *0*. Roll a *${DICE_ESCALATOR_BUST_ON}* and you bust!`;
    const keyboard = { inline_keyboard: [
        [{ text: "🎲 Prompt Roll (via Helper)", callback_data: `de_roll_prompt:${gameId}` }],
        [{ text: "💰 Cashout 0 credits", callback_data: `de_cashout:${gameId}` }]
    ]};
    const sentMsg = await safeSendMessage(chatId, initialMessage, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
    if (sentMsg) gameData.gameSetupMessageId = sentMsg.message_id; // Store message ID for future edits
}

async function handleDiceEscalatorPlayerAction(gameId, userId, actionType, interactionMessageId, chatId) {
    const gameData = activeGames.get(gameId);
    if (!gameData || gameData.chatId !== chatId || gameData.type !== 'dice_escalator' || gameData.currentPlayerId !== userId || gameData.status !== 'player_turn_prompt_action') {
        await safeSendMessage(userId, "It's not your turn, this game is not active, or that action isn't available right now.", {});
        return;
    }

    if (actionType === 'roll_prompt') {
        gameData.status = 'waiting_player_roll_via_helper';
        activeGames.set(gameId, gameData);
        const helperUsernameToMention = DICES_HELPER_BOT_ID ? `the Helper Bot (ID: ${DICES_HELPER_BOT_ID})` : (DICES_HELPER_BOT_USERNAME ? `@${DICES_HELPER_BOT_USERNAME}` : 'the Helper Bot');
        const promptMsg = `${gameData.initiatorMention}, please trigger ${helperUsernameToMention} to roll your dice (e.g., by typing \`/roll\` if that's its command).\n\n_Waiting for roll..._`;

        // --- START DEBUG LOGS ---
        console.log(`[DEBUG_DE_ROLL] Preparing to edit message. ChatID type: ${typeof chatId}, ChatID value: '${chatId}'`);
        console.log(`[DEBUG_DE_ROLL] InteractionMessageId type: ${typeof interactionMessageId}, Value: '${interactionMessageId}'`);
        console.log(`[DEBUG_DE_ROLL] GameData initiatorMention: ${gameData.initiatorMention}`);
        // --- END DEBUG LOGS ---

        await bot.editMessageText(escapeMarkdownV2(promptMsg), { 
            chatId: String(chatId), // Explicitly ensure it's a string, though it should be from CB
            message_id: Number(interactionMessageId), // Ensure it's a number
            parse_mode: 'MarkdownV2', 
            reply_markup: {} 
        });
    } 
    } else if (actionType === 'cashout') {
        const cashedOutAmount = gameData.playerScore;
        const netProfit = cashedOutAmount; // In this version, playerScore IS the net profit above the initial bet.
        const totalReturn = gameData.betAmount + netProfit;

        await updateUserBalance(userId, totalReturn, `cashout_dice_escalator:${gameId}`, chatId);
        let cashoutMessage = `${gameData.initiatorMention} cashed out with a score of *${escapeMarkdownV2(String(cashedOutAmount))}* credits, winning ${escapeMarkdownV2(formatCurrency(netProfit))} (+ bet back)!`;
        activeGames.get(gameId).status = 'player_cashed_out'; // Intermediate status

        // Now bot plays
        cashoutMessage += `\n\n🤖 Now it's the Bot's turn to try and beat your score of ${escapeMarkdownV2(String(cashedOutAmount))}!`;
        await bot.editMessageText(cashoutMessage, { chatId, message_id: interactionMessageId, parse_mode: 'MarkdownV2', reply_markup: {} });
        await sleep(1500); // Pause before bot plays
        await processDiceEscalatorBotTurn(gameData, interactionMessageId);
    }
}

async function processDiceEscalatorPlayerRoll(gameData, playerRoll) {
    const { gameId, chatId, initiatorMention, betAmount, bustValue } = gameData;
    let playerBusted = false;
    let newPlayerScore = gameData.playerScore;

    if (playerRoll === bustValue) {
        playerBusted = true;
        newPlayerScore = 0; // Reset score on bust
        gameData.status = 'game_over_player_bust';
    } else {
        newPlayerScore += playerRoll; // Add roll value to score
        gameData.status = 'player_turn_prompt_action'; // Back to prompting player
    }
    gameData.playerScore = newPlayerScore;
    activeGames.set(gameId, gameData);

    let turnResultMessage = `${initiatorMention} (Player) rolled: ${formatDiceRolls([playerRoll])}!\n`; // formatDiceRolls from Part 3

    if (playerBusted) {
        turnResultMessage += `💥 Oh no! You rolled a ${bustValue} and BUSTED! You lose your ${escapeMarkdownV2(formatCurrency(betAmount))} bet.`;
        // Balance already deducted, so loss is implicit.
        await bot.editMessageText(turnResultMessage, { chatId, message_id: gameData.gameSetupMessageId, parse_mode: 'MarkdownV2', reply_markup: {} });
        activeGames.delete(gameId);
        await updateGroupGameDetails(chatId, null, null, null);
    } else {
        turnResultMessage += `Your current score this round: *${escapeMarkdownV2(String(newPlayerScore))}*.`;
        const keyboard = { inline_keyboard: [
            [{ text: `🎲 Prompt Roll Again (via Helper)`, callback_data: `de_roll_prompt:${gameId}` }],
            [{ text: `💰 Cashout ${escapeMarkdownV2(formatCurrency(newPlayerScore))} credits`, callback_data: `de_cashout:${gameId}` }]
        ]};
        await bot.editMessageText(turnResultMessage, { chatId, message_id: gameData.gameSetupMessageId, parse_mode: 'MarkdownV2', reply_markup: keyboard });
    }
}

async function processDiceEscalatorBotTurn(gameData, messageIdToUpdate) {
    const { gameId, chatId, initiatorMention, betAmount, playerScore: playerScoreToBeat, bustValue } = gameData;
    let botCurrentScore = 0;
    let botBusted = false;
    let botRollsMade = 0;
    let botDecisionMessage = `\n🤖 Bot's turn (Target: ${playerScoreToBeat}):\n`;

    await bot.editMessageText(escapeMarkdownV2(botDecisionMessage) + "_Bot is thinking..._", {chatId, message_id: messageIdToUpdate, parse_mode:'MarkdownV2', reply_markup:{}});
    await sleep(1000);

    // Simple bot strategy: try to beat player's score, roll up to DICE_ESCALATOR_BOT_ROLLS times
    while (botRollsMade < DICE_ESCALATOR_BOT_ROLLS && botCurrentScore <= playerScoreToBeat && !botBusted) {
        botRollsMade++;
        const botRollResult = determineDieRollOutcome(); // Bot uses its internal roller (Part 4)
        botDecisionMessage += `Bot roll ${botRollsMade}: ${botRollResult.emoji} (${botRollResult.roll})\n`;

        if (botRollResult.roll === bustValue) {
            botBusted = true;
            botCurrentScore = 0; // Bust
            botDecisionMessage += `💥 Bot BUSTED by rolling a ${bustValue}!\n`;
            break;
        }
        botCurrentScore += botRollResult.roll;
        botDecisionMessage += `Bot score is now: ${botCurrentScore}\n`;
        await bot.editMessageText(escapeMarkdownV2(botDecisionMessage) + "_Bot is rolling..._", {chatId, message_id: messageIdToUpdate, parse_mode:'MarkdownV2', reply_markup:{}});
        await sleep(1500);
    }

    if (!botBusted && botCurrentScore <= playerScoreToBeat) {
        botDecisionMessage += `Bot couldn't beat your score of ${playerScoreToBeat} after ${botRollsMade} rolls (Bot score: ${botCurrentScore}).\n`;
    } else if (!botBusted && botCurrentScore > playerScoreToBeat) {
         botDecisionMessage += `Bot's final score: ${botCurrentScore}. Bot will cashout.\n`;
    }


    let finalMessage = botDecisionMessage;
    gameData.status = 'game_over';

    if (botBusted || botCurrentScore <= playerScoreToBeat) {
        // Player already cashed out and won their amount. Bot loses or fails to beat.
        finalMessage += `\n🎉 ${initiatorMention}, you successfully cashed out and the Bot didn't beat your score! You keep your winnings of ${escapeMarkdownV2(formatCurrency(playerScoreToBeat))} (plus your bet back).`;
        // User balance already updated on their cashout.
    } else { // Bot wins (beat player's cashed out score and didn't bust)
        finalMessage += `\n😢 ${initiatorMention}, the Bot beat your cashed-out score of ${playerScoreToBeat} with a score of ${botCurrentScore}! The house wins this time.`;
        // Player already got their cashed out amount. This means the initial bet effectively becomes profit for the house.
        // No further balance change for the player needed here as they already cashed out.
        // The net effect is player won `playerScoreToBeat`, bot won nothing from player beyond the initial interaction.
        // If the rule was "player loses bet if bot beats cashout", then we'd deduct `gameData.betAmount` again.
        // For now, let's assume cashout is final for player. The "loss" is just that the bot did better.
    }

    finalMessage += `\n\nGame Over.`;
    await bot.editMessageText(finalMessage, { chatId, message_id: messageIdToUpdate, parse_mode: 'MarkdownV2', reply_markup: {} });
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
}


console.log("Part 5: Message & Callback Handling, Basic Game Flow - Complete.");

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
