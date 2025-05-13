// --- Start of Part 1 ---
// index.js - Part 1: Core Imports, Basic Setup, Global State & Utilities
//---------------------------------------------------------------------------

// ESM-style imports
import 'dotenv/config'; // Ensure this is at the very top to load your .env file
import TelegramBot from 'node-telegram-bot-api';
import { Pool } from 'pg'; // For PostgreSQL

console.log("Loading Part 1: Core Imports, Basic Setup, Global State & Utilities (Enhanced)...");

// --- Helper function for JSON.stringify to handle BigInts (Defined ONCE Globally) ---
function stringifyWithBigInt(obj) {
  return JSON.stringify(obj, (key, value) => {
    if (typeof value === 'bigint') {
      return value.toString() + 'n'; // Suffix 'n' to clearly indicate BigInt in logs
    }
    if (typeof value === 'function') {
      return `[Function: ${value.name || 'anonymous'}]`;
    }
    if (value === undefined) {
      return 'undefined_value'; // More explicit than null for debugging
    }
    return value;
  }, 2); // Pretty print with 2-space indentation
}
console.log("[Global Utils] stringifyWithBigInt helper function defined.");

//---------------------------------------------------------------------------
// index.js - Part 1: Core Imports & Basic Setup
//---------------------------------------------------------------------------

// --- Environment Variable Validation & Defaults ---
const OPTIONAL_ENV_DEFAULTS = {
  'DB_POOL_MAX': '25',
  'DB_POOL_MIN': '5',
  'DB_IDLE_TIMEOUT': '30000',       // 30 seconds
  'DB_CONN_TIMEOUT': '5000',        // 5 seconds
  'DB_SSL': 'true',                 // Default to true for production environments
  'DB_REJECT_UNAUTHORIZED': 'true', // Default to true for security with SSL
  'SHUTDOWN_FAIL_TIMEOUT_MS': '10000', // 10 seconds
  'JACKPOT_CONTRIBUTION_PERCENT': '0.01', // 1%
  'MIN_BET_AMOUNT': '5',
  'MAX_BET_AMOUNT': '1000',
  'COMMAND_COOLDOWN_MS': '1500',    // 1.5 seconds
  'JOIN_GAME_TIMEOUT_MS': '120000', // 2 minutes
  'DEFAULT_STARTING_BALANCE_LAMPORTS': '100000000', // 100 (assuming 1 unit = 1,000,000 lamports for display)
  'TARGET_JACKPOT_SCORE': '100',     // Default for Dice Escalator Jackpot
  'BOT_STAND_SCORE_DICE_ESCALATOR': '10',// Default for Dice Escalator Bot stand score
  'DICE_21_TARGET_SCORE': '21',      // Default for Dice 21 target
  'DICE_21_BOT_STAND_SCORE': '17',   // Default for Dice 21 Bot stand score
  'RULES_CALLBACK_PREFIX': 'rules_game_',
  'DEPOSIT_CALLBACK_ACTION': 'deposit_action',
  'WITHDRAW_CALLBACK_ACTION': 'withdraw_action',
  'QUICK_DEPOSIT_CALLBACK_ACTION': 'quick_deposit_action',
  'MAX_RETRY_POLLING_DELAY': '60000', // 1 minute
  'INITIAL_RETRY_POLLING_DELAY': '5000', // 5 seconds
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
const ADMIN_USER_ID = process.env.ADMIN_USER_ID; // Optional, for admin notifications
const DATABASE_URL = process.env.DATABASE_URL;

// --- Crucial Game Play Constants & Settings ---
// Shutdown and System Behavior
const SHUTDOWN_FAIL_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
const MAX_RETRY_POLLING_DELAY = parseInt(process.env.MAX_RETRY_POLLING_DELAY, 10);
const INITIAL_RETRY_POLLING_DELAY = parseInt(process.env.INITIAL_RETRY_POLLING_DELAY, 10);


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

// Callback Action Prefixes/Strings (New)
const RULES_CALLBACK_PREFIX = process.env.RULES_CALLBACK_PREFIX;
const DEPOSIT_CALLBACK_ACTION = process.env.DEPOSIT_CALLBACK_ACTION;
const WITHDRAW_CALLBACK_ACTION = process.env.WITHDRAW_CALLBACK_ACTION;
const QUICK_DEPOSIT_CALLBACK_ACTION = process.env.QUICK_DEPOSIT_CALLBACK_ACTION;


// --- Validations and Logging for Critical Variables ---
if (!BOT_TOKEN) {
  console.error("FATAL ERROR: BOT_TOKEN is not defined. Bot cannot start.");
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error("FATAL ERROR: DATABASE_URL is not defined. Cannot connect to PostgreSQL.");
  process.exit(1);
}
const criticalGameScores = {
    TARGET_JACKPOT_SCORE, BOT_STAND_SCORE_DICE_ESCALATOR,
    DICE_21_TARGET_SCORE, DICE_21_BOT_STAND_SCORE
};
for (const [key, value] of Object.entries(criticalGameScores)) {
    if (isNaN(value)) {
        console.error(`FATAL ERROR: ${key} is not a valid number. Value from env: '${process.env[key]}'. Check .env file or defaults.`);
        process.exit(1);
    }
}
if (MIN_BET_AMOUNT < 1 || isNaN(MIN_BET_AMOUNT)) {
    console.error(`FATAL ERROR: MIN_BET_AMOUNT (${MIN_BET_AMOUNT}) must be a positive number.`);
    process.exit(1);
}
if (MAX_BET_AMOUNT < MIN_BET_AMOUNT || isNaN(MAX_BET_AMOUNT)) {
    console.error(`FATAL ERROR: MAX_BET_AMOUNT (${MAX_BET_AMOUNT}) must be greater than or equal to MIN_BET_AMOUNT and be a number.`);
    process.exit(1);
}


console.log("BOT_TOKEN loaded successfully.");
if (ADMIN_USER_ID) console.log(`Admin User ID: ${ADMIN_USER_ID} loaded.`);
else console.log("INFO: No ADMIN_USER_ID set (optional, for admin alerts).");

console.log("--- Game Settings Loaded ---");
console.log(`Dice Escalator - Target Jackpot Score: ${TARGET_JACKPOT_SCORE}, Bot Stand Score: ${BOT_STAND_SCORE_DICE_ESCALATOR}, Jackpot Contribution: ${JACKPOT_CONTRIBUTION_PERCENT * 100}%`);
console.log(`Dice 21 - Target Score: ${DICE_21_TARGET_SCORE}, Bot Stand Score: ${DICE_21_BOT_STAND_SCORE}`);
console.log(`Bet Limits: ${MIN_BET_AMOUNT} - ${MAX_BET_AMOUNT} credits`);
console.log(`Default Starting Balance: ${DEFAULT_STARTING_BALANCE_LAMPORTS} lamports`);
console.log(`Command Cooldown: ${COMMAND_COOLDOWN_MS}ms`);
console.log(`Join Game Timeout: ${JOIN_GAME_TIMEOUT_MS}ms`);
console.log("-----------------------------");


// --- PostgreSQL Pool Initialization ---
console.log("⚙️ Setting up PostgreSQL Pool...");
console.log(`DB_SSL configuration: '${process.env.DB_SSL}', rejectUnauthorized: '${process.env.DB_REJECT_UNAUTHORIZED}'`);

const useSsl = process.env.DB_SSL === 'true';
const rejectUnauthorizedSsl = process.env.DB_REJECT_UNAUTHORIZED === 'true'; // Important for security

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
  // Optional: Set timezone for new connections if necessary, e.g., client.query("SET TIME ZONE 'UTC'");
});
pool.on('error', (err, client) => {
  console.error('❌ Unexpected error on idle PostgreSQL client', err);
  // safeSendMessage and escapeMarkdownV2 might not be defined when this part of code is executed first.
  // So, we ensure admin notification is attempted carefully.
  if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
    safeSendMessage(ADMIN_USER_ID, `🚨 DATABASE POOL ERROR (Idle Client): ${escapeMarkdownV2(err.message || String(err))}`)
      .catch(notifyErr => console.error("Failed to notify admin about DB pool error:", notifyErr));
  } else {
    console.error(`[Admin Alert Failure] DB Pool Error (Idle Client): ${err.message || String(err)} (safeSendMessage, escapeMarkdownV2, or ADMIN_USER_ID might not be available yet)`);
  }
});
console.log("✅ PostgreSQL Pool created.");

// --- Telegram Bot Instance ---
const bot = new TelegramBot(BOT_TOKEN, {
    polling: true,
    // Optional: Configure request timeout for polling if needed
    // request: { timeout: 30000 } // e.g., 30 seconds timeout for polling requests
});
console.log("Telegram Bot instance created and configured for polling.");

const BOT_VERSION = '3.0.0-pro-casino-ux'; // Updated version marker
const MAX_MARKDOWN_V2_MESSAGE_LENGTH = 4096; // Telegram API limit

// --- Global State Variables for Shutdown & Operation ---
let isShuttingDown = false; // Flag to prevent multiple shutdown sequences

// --- In-memory stores ---
let activeGames = new Map(); // For active game state <gameId, gameDataObject>
let userCooldowns = new Map(); // For command cooldowns <userId, timestamp>
// groupGameSessions will be initialized in Part 2

console.log(`Initializing Group Chat Casino Bot v${BOT_VERSION}...`);
console.log(`Current system time: ${new Date().toISOString()}`);
console.log(`Node.js Version: ${process.version}`);

// --- Core Utility Functions ---

// Escapes text for Telegram MarkdownV2 mode
const escapeMarkdownV2 = (text) => {
  if (text === null || typeof text === 'undefined') return '';
  // Order of replacements can matter for complex escapes, but for Telegram's limited set, this is usually fine.
  // Specific characters to escape: _ * [ ] ( ) ~ ` > # + - = | { } . ! \
  return String(text).replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};

// Safely sends a message, handling potential errors and length limits
async function safeSendMessage(chatId, text, options = {}) {
  const LOG_PREFIX_SSM = `[safeSendMessage CH:${chatId}]`;
  if (!chatId || typeof text !== 'string') {
    console.error(`${LOG_PREFIX_SSM} Invalid input: ChatID is ${chatId}, Text type is ${typeof text}. Preview: ${String(text).substring(0, 100)}`);
    return undefined; // Return undefined for consistency on failure
  }

  let messageToSend = text;
  let finalOptions = { ...options }; // Clone options to avoid modifying original

  // Truncation logic (pre-escape, then re-check post-escape if Markdown)
  if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
    const ellipsis = "... (message truncated)";
    // Ensure truncateAt is positive and gives space for ellipsis
    const truncateAt = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsis.length);
    messageToSend = messageToSend.substring(0, truncateAt) + ellipsis;
    console.warn(`${LOG_PREFIX_SSM} Message was pre-truncated due to length > ${MAX_MARKDOWN_V2_MESSAGE_LENGTH}.`);
  }

  if (finalOptions.parse_mode === 'MarkdownV2') {
    // Escape the (potentially pre-truncated) message
    messageToSend = escapeMarkdownV2(messageToSend);
    // After escaping, the message might become longer. Re-truncate if necessary.
    if (messageToSend.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
        const ellipsisMarkdown = escapeMarkdownV2("... (message re-truncated)");
        const truncateAtMarkdown = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsisMarkdown.length);
        messageToSend = messageToSend.substring(0, truncateAtMarkdown) + ellipsisMarkdown;
        console.warn(`${LOG_PREFIX_SSM} Message (MarkdownV2) re-truncated AFTER escaping due to length > ${MAX_MARKDOWN_V2_MESSAGE_LENGTH}.`);
    }
  }

  if (!bot) {
    console.error(`${LOG_PREFIX_SSM} Error: Telegram 'bot' instance is not available. Cannot send message.`);
    return undefined;
  }

  try {
    if (typeof bot.sendMessage !== 'function') {
      throw new Error("'bot.sendMessage' is not a function. Bot instance might be improperly initialized.");
    }
    // console.debug(`${LOG_PREFIX_SSM} Sending: "${messageToSend.substring(0,100)}${messageToSend.length > 100 ? '...' : ''}" Opts: ${stringifyWithBigInt(finalOptions)}`);
    const sentMessage = await bot.sendMessage(chatId, messageToSend, finalOptions);
    // console.debug(`${LOG_PREFIX_SSM} Message sent successfully. Msg ID: ${sentMessage ? sentMessage.message_id : 'N/A'}`);
    return sentMessage;
  } catch (error) {
    console.error(`${LOG_PREFIX_SSM} Failed to send message. Code: ${error.code || 'N/A'}, Msg: ${error.message}`);
    if (error.response && error.response.body) {
      console.error(`${LOG_PREFIX_SSM} API Response Body: ${stringifyWithBigInt(error.response.body)}`);
      // Specific check for common Markdown parsing errors
      if (finalOptions.parse_mode === 'MarkdownV2' && error.response.body.description && error.response.body.description.includes("can't parse entities")) {
          console.error(`${LOG_PREFIX_SSM} MarkdownV2 parsing error. Original unescaped text (approx first 200 chars): "${text.substring(0,200)}"`);
          // Fallback: try sending as plain text if Markdown fails
          console.warn(`${LOG_PREFIX_SSM} Attempting to send message as plain text due to MarkdownV2 parse error.`);
          try {
              delete finalOptions.parse_mode; // Remove parse_mode
              // Re-truncate original text if it was long, as escaping is removed
              let plainText = text;
              if (plainText.length > MAX_MARKDOWN_V2_MESSAGE_LENGTH) {
                const ellipsis = "... (message truncated)";
                const truncateAt = Math.max(0, MAX_MARKDOWN_V2_MESSAGE_LENGTH - ellipsis.length);
                plainText = plainText.substring(0, truncateAt) + ellipsis;
              }
              return await bot.sendMessage(chatId, plainText, finalOptions);
          } catch (fallbackError) {
              console.error(`${LOG_PREFIX_SSM} Failed to send message as plain text fallback. Code: ${fallbackError.code || 'N/A'}, Msg: ${fallbackError.message}`);
              return undefined;
          }
      }
    }
    return undefined;
  }
}

// Simple asynchronous sleep function
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

console.log("Part 1: Core Imports, Basic Setup, Global State & Utilities (Enhanced) - Complete.");
// --- End of Part 1 ---
// --- Start of Part 2 ---
// index.js - Part 2: Database Operations & Data Management (DATABASE BACKED) (Enhanced)
//---------------------------------------------------------------------------
console.log("Loading Part 2: Database Operations & Data Management (DATABASE BACKED) (Enhanced)...");

// In-memory stores for non-critical/session data
const groupGameSessions = new Map(); // Tracks active game per group (e.g., for Coinflip/RPS) <chatId, sessionObject>
// userDatabase (in-memory map for balances) was OBSOLETE and remains so. Balances are in PostgreSQL.
console.log("In-memory data stores (groupGameSessions) initialized.");


// --- queryDatabase Helper Function ---
// (Ensure 'pool' is defined in Part 1 and globally accessible)
async function queryDatabase(sql, params = [], dbClient = pool) { // 'pool' is expected from Part 1
  const LOG_PREFIX_DB_QUERY = "[DB_Query]";
  if (!dbClient) {
    const poolError = new Error("Database pool/client is not available for queryDatabase. 'pool' might not be correctly defined or accessible from Part 1.");
    console.error(`❌ CRITICAL: ${LOG_PREFIX_DB_QUERY} called but dbClient (pool) is invalid!`, poolError.stack);
    // In a real production system, you might want to trigger an alert here
    throw poolError;
  }
  if (typeof sql !== 'string' || sql.trim().length === 0) {
    const sqlError = new TypeError(`queryDatabase received invalid SQL query (type: ${typeof sql}, value: "${String(sql).substring(0,100)}")`);
    console.error(`❌ ${LOG_PREFIX_DB_QUERY} Error:`, sqlError.message);
    throw sqlError;
  }

  // Basic logging for query execution, can be expanded for performance monitoring
  // console.debug(`${LOG_PREFIX_DB_QUERY} Executing SQL (first 100 chars): ${sql.substring(0, 100)}${sql.length > 100 ? '...' : ''}`, params);

  try {
    const result = await dbClient.query(sql, params);
    // console.debug(`${LOG_PREFIX_DB_QUERY} Success. Rows affected/returned: ${result.rowCount !== null ? result.rowCount : 'N/A'}`);
    return result;
  } catch (error) {
    console.error(`❌ ${LOG_PREFIX_DB_QUERY} Error Encountered:`);
    console.error(`   SQL (truncated for safety): ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`);
    // Use stringifyWithBigInt if available globally, otherwise basic JSON.stringify for params
    const safeParamsString = typeof stringifyWithBigInt === 'function'
      ? stringifyWithBigInt(params)
      : JSON.stringify(params, (key, value) => typeof value === 'bigint' ? value.toString() + 'n' : value);
    console.error(`   Params: ${safeParamsString}`);
    console.error(`   Error Code: ${error.code || 'N/A'}`);
    console.error(`   Error Message: ${error.message}`);
    if (error.constraint) { console.error(`   Constraint Violated: ${error.constraint}`); }
    // Consider more detailed error classification or alerting for specific error codes (e.g., connection issues vs. syntax errors)
    throw error; // Re-throw the error to be handled by the caller
  }
}

// --- User and Balance Functions (DATABASE BACKED) ---
// Constants like DEFAULT_STARTING_BALANCE_LAMPORTS, JACKPOT_CONTRIBUTION_PERCENT, MAIN_JACKPOT_ID
// are expected to be defined globally in Part 1.

// Gets user data (from DB), creates if doesn't exist.
// Returns user object including balance.
async function getUser(userId, username = null, firstName = null) {
  const LOG_PREFIX_GETUSER = `[DB_GetUser UID:${userId}]`;
  const userIdStr = String(userId);
  let client; // Declare client here to ensure it's in scope for finally block

  // console.log(`${LOG_PREFIX_GETUSER} Attempting to get/create user. Username: ${username}, FirstName: ${firstName}`);

  if (!userIdStr || userIdStr === 'undefined' || userIdStr === 'null') {
      console.error(`${LOG_PREFIX_GETUSER} CRITICAL: Invalid userId provided: ${userIdStr}`);
      throw new Error(`Invalid user ID: ${userIdStr}. Cannot fetch or create user.`);
  }

  try {
    if (!pool) {
      console.error(`${LOG_PREFIX_GETUSER} CRITICAL: Database pool is not defined or accessible!`);
      throw new Error("Database pool is not initialized. Cannot perform user operations.");
    }
    client = await pool.connect();
    // console.log(`${LOG_PREFIX_GETUSER} DB client connected. Starting transaction.`);
    await client.query('BEGIN');

    // Try to get wallet/user first
    let userResult = await client.query(
      'SELECT user_id, referral_code, last_used_at, created_at FROM wallets WHERE user_id = $1',
      [userIdStr]
    );
    // console.log(`${LOG_PREFIX_GETUSER} Wallet query result: ${userResult.rows.length} rows.`);

    let userWalletData;
    let isNewUser = false;
    let actualBalanceLamports;
    let userCreatedAt;

    if (typeof DEFAULT_STARTING_BALANCE_LAMPORTS === 'undefined') {
      console.error(`${LOG_PREFIX_GETUSER} CRITICAL: DEFAULT_STARTING_BALANCE_LAMPORTS is undefined! Cannot proceed.`);
      await client.query('ROLLBACK'); // Rollback before throwing
      throw new Error("System configuration error: Default starting balance not set.");
    }

    if (userResult.rows.length === 0) {
      // console.log(`${LOG_PREFIX_GETUSER} User not found. Creating new wallet and balance entry.`);
      isNewUser = true;
      // More robust referral code generation
      const timestampPart = Date.now().toString(36).slice(-4); // Use last 4 chars of timestamp
      const randomPart = Math.random().toString(36).substring(2, 7); // 5 random alphanumeric chars
      const newReferralCode = `ref${timestampPart}${randomPart}`.slice(0, 20); // Ensure it fits VARCHAR(20)

      const insertWalletQuery = `
        INSERT INTO wallets (user_id, referral_code, created_at, last_used_at)
        VALUES ($1, $2, NOW(), NOW())
        RETURNING user_id, referral_code, last_used_at, created_at;
      `;
      userResult = await client.query(insertWalletQuery, [userIdStr, newReferralCode]);
      userWalletData = userResult.rows[0];
      userCreatedAt = userWalletData.created_at;
      // console.log(`${LOG_PREFIX_GETUSER} New wallet created with referral code ${newReferralCode}.`);

      const insertBalanceQuery = `
        INSERT INTO user_balances (user_id, balance_lamports, updated_at)
        VALUES ($1, $2, NOW());
      `;
      await client.query(insertBalanceQuery, [userIdStr, DEFAULT_STARTING_BALANCE_LAMPORTS.toString()]);
      actualBalanceLamports = DEFAULT_STARTING_BALANCE_LAMPORTS;
      // console.log(`${LOG_PREFIX_GETUSER} Initial balance of ${actualBalanceLamports} set for new user.`);
    } else {
      userWalletData = userResult.rows[0];
      userCreatedAt = userWalletData.created_at;
      // console.log(`${LOG_PREFIX_GETUSER} User found. Updating last_used_at.`);
      await client.query('UPDATE wallets SET last_used_at = NOW() WHERE user_id = $1', [userIdStr]);

      const balanceResult = await client.query('SELECT balance_lamports FROM user_balances WHERE user_id = $1', [userIdStr]);
      // console.log(`${LOG_PREFIX_GETUSER} Balance query result for existing user: ${balanceResult.rows.length} rows.`);
      if (balanceResult.rows.length === 0) {
        console.warn(`${LOG_PREFIX_GETUSER} Wallet exists but no balance record! This indicates a potential data integrity issue. Creating with default balance.`);
        await client.query('INSERT INTO user_balances (user_id, balance_lamports, updated_at) VALUES ($1, $2, NOW())', [userIdStr, DEFAULT_STARTING_BALANCE_LAMPORTS.toString()]);
        actualBalanceLamports = DEFAULT_STARTING_BALANCE_LAMPORTS;
        // Consider logging this anomaly more formally for investigation
      } else {
        actualBalanceLamports = BigInt(balanceResult.rows[0].balance_lamports);
        // console.log(`${LOG_PREFIX_GETUSER} Existing balance is ${actualBalanceLamports}.`);
      }
    }

    await client.query('COMMIT');
    // console.log(`${LOG_PREFIX_GETUSER} Transaction committed.`);

    return {
      userId: userWalletData.user_id,
      username: username || `User_${userIdStr.slice(-6)}`, // Use last 6 digits of ID if no username for some privacy
      firstName: firstName, // Store it if provided, can be useful for personalized messages
      balance: Number(actualBalanceLamports), // For convenience in some game logic expecting Number
      balanceLamports: actualBalanceLamports, // For precise operations (BigInt)
      isNew: isNewUser,
      referralCode: userWalletData.referral_code,
      createdAt: userCreatedAt, // Include creation date
      // groupStats: new Map(), // Placeholder for potential future group-specific stats (if needed)
    };

  } catch (error) {
    console.error(`${LOG_PREFIX_GETUSER} Error fetching/creating user: ${error.message}`, error.stack);
    if (client) {
      try {
        await client.query('ROLLBACK');
        // console.log(`${LOG_PREFIX_GETUSER} Transaction rolled back due to error.`);
      } catch (rbErr) {
        console.error(`${LOG_PREFIX_GETUSER} Rollback failed: ${rbErr.message}`, rbErr.stack);
      }
    }
    throw error; // Re-throw the error to be handled by the caller (e.g., command handler)
  } finally {
    if (client) {
      client.release();
      // console.log(`${LOG_PREFIX_GETUSER} DB client released.`);
    }
  }
}

// Updates user balance IN THE DATABASE transactionally.
// Reason examples: 'bet_placed_dice_escalator:game123', 'won_dice_escalator_score:game123', 'refund_stale_game:game456', 'jackpot_win_dice_escalator:game123', 'admin_grant:tx789'
async function updateUserBalance(userId, amountChangeLamports, reason = "unknown_transaction", client_ = null, associatedGameId = null, chatIdForLog = null) {
  const LOG_PREFIX_UPDATEBAL = `[DB_UpdateBalance UID:${userId} GameID:${associatedGameId || 'N/A'}]`;
  const userIdStr = String(userId);
  const operationClient = client_ || await pool.connect(); // Uses global 'pool' from Part 1

  // console.log(`${LOG_PREFIX_UPDATEBAL} Attempting update. Change: ${amountChangeLamports}, Reason: ${reason}, ChatID: ${chatIdForLog}, Using Provided Client: ${!!client_}`);

  if (!userIdStr || userIdStr === 'undefined' || userIdStr === 'null') {
    console.error(`${LOG_PREFIX_UPDATEBAL} CRITICAL: Invalid userId provided: ${userIdStr}`);
    if (!client_ && operationClient) operationClient.release(); // Release if we acquired it
    return { success: false, error: `Invalid user ID: ${userIdStr}. Balance update failed.` };
  }

  let currentBalanceLamportsFromDB; // To store balance fetched for error reporting

  try {
    if (!client_) {
      // console.log(`${LOG_PREFIX_UPDATEBAL} Starting new transaction.`);
      await operationClient.query('BEGIN');
    } else {
      // console.log(`${LOG_PREFIX_UPDATEBAL} Using existing transaction.`);
    }

    const balanceSelectRes = await operationClient.query(
      'SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE',
      [userIdStr]
    );
    // console.log(`${LOG_PREFIX_UPDATEBAL} Fetched current balance. Rows: ${balanceSelectRes.rows.length}`);

    if (balanceSelectRes.rows.length === 0) {
      if (!client_) await operationClient.query('ROLLBACK');
      console.warn(`${LOG_PREFIX_UPDATEBAL} Update balance called for non-existent user balance record. User should be created via getUser first.`);
      // This scenario should ideally be prevented by ensuring getUser is always called first.
      return { success: false, error: "User balance record not found. Ensure user exists before updating balance." };
    }

    currentBalanceLamportsFromDB = BigInt(balanceSelectRes.rows[0].balance_lamports);
    const change = BigInt(amountChangeLamports); // Ensure amountChangeLamports is correctly a BigInt or convertible
    let proposedBalanceLamports = currentBalanceLamportsFromDB + change;
    // console.log(`${LOG_PREFIX_UPDATEBAL} CurrentBal=${currentBalanceLamportsFromDB}, Change=${change}, ProposedBal=${proposedBalanceLamports}`);

    // Jackpot Contribution Logic (Dice Escalator Specific)
    // Ensure JACKPOT_CONTRIBUTION_PERCENT and MAIN_JACKPOT_ID are accessible globally from Part 1
    if (typeof JACKPOT_CONTRIBUTION_PERCENT === 'undefined' || typeof MAIN_JACKPOT_ID === 'undefined') {
        console.warn(`${LOG_PREFIX_UPDATEBAL} WARNING: JACKPOT_CONTRIBUTION_PERCENT or MAIN_JACKPOT_ID is undefined. Jackpot contributions will be skipped if applicable.`);
    }

    let jackpotContribution = 0n;
    if (reason.startsWith('bet_placed_dice_escalator') && change < 0n && associatedGameId && typeof JACKPOT_CONTRIBUTION_PERCENT === 'number' && MAIN_JACKPOT_ID) {
      const betAmount = -change; // Absolute bet amount
      // Ensure JACKPOT_CONTRIBUTION_PERCENT is a fraction e.g. 0.01 for 1%
      // Using Math.floor to ensure whole lamports.
      jackpotContribution = BigInt(Math.floor(Number(betAmount) * JACKPOT_CONTRIBUTION_PERCENT));

      if (jackpotContribution > 0n) {
        // console.log(`${LOG_PREFIX_UPDATEBAL} Calculating jackpot contribution. Bet: ${betAmount}, Percent: ${JACKPOT_CONTRIBUTION_PERCENT}, Contribution: ${jackpotContribution}.`);
        const updateJackpotResult = await operationClient.query(
          'UPDATE jackpot_status SET current_amount_lamports = current_amount_lamports + $1, updated_at = NOW(), last_contributed_game_id = $2 WHERE jackpot_id = $3',
          [jackpotContribution.toString(), associatedGameId, MAIN_JACKPOT_ID]
        );
        if (updateJackpotResult.rowCount > 0) {
            // console.log(`${LOG_PREFIX_UPDATEBAL} [JACKPOT] Contributed ${jackpotContribution} to ${MAIN_JACKPOT_ID}.`);
        } else {
            console.warn(`${LOG_PREFIX_UPDATEBAL} [JACKPOT] FAILED to contribute to ${MAIN_JACKPOT_ID}. Jackpot ID might not exist in table. Contribution: ${jackpotContribution}`);
            // Decide if this should be a fatal error for the transaction or just a warning. For now, warning.
        }
      }
    }

    if (proposedBalanceLamports < 0n) {
      if (!client_) await operationClient.query('ROLLBACK');
      console.log(`${LOG_PREFIX_UPDATEBAL} Insufficient balance (${currentBalanceLamportsFromDB}) for change of ${change}. Reason: ${reason}. Transaction rolling back (if new).`);
      return {
          success: false,
          error: "Insufficient balance",
          currentBalance: Number(currentBalanceLamportsFromDB),
          currentBalanceLamports: currentBalanceLamportsFromDB
        };
    }

    await operationClient.query(
      'UPDATE user_balances SET balance_lamports = $1, updated_at = NOW() WHERE user_id = $2',
      [proposedBalanceLamports.toString(), userIdStr]
    );
    // console.log(`${LOG_PREFIX_UPDATEBAL} Balance successfully updated in DB to: ${proposedBalanceLamports}`);

    // Bet Logging Logic (critical for audit trail)
    const betDetails = { game_id: associatedGameId }; // Ensure associatedGameId is always passed if relevant
    let wagerAmountForLog = 0n;
    if (change < 0n) wagerAmountForLog = -change; // Log wager as positive if it's a debit

    if (reason.startsWith('bet_placed_') && wagerAmountForLog > 0n && associatedGameId && chatIdForLog) {
      const gameTypeFromReason = reason.substring('bet_placed_'.length).split(':')[0];
      if (jackpotContribution > 0n && gameTypeFromReason === 'dice_escalator') { // only add jackpot_contrib if it's for DE
        betDetails.jackpot_contribution_lamports = jackpotContribution.toString();
      }
      // console.log(`${LOG_PREFIX_UPDATEBAL} Logging 'bet_placed' event. GameType: ${gameTypeFromReason}, Wager: ${wagerAmountForLog}, Details: ${stringifyWithBigInt(betDetails)}.`);
      await operationClient.query(
        `INSERT INTO bets (user_id, chat_id, game_type, wager_amount_lamports, bet_details, status, reason_tx, created_at, processed_at)
         VALUES ($1, $2, $3, $4, $5, 'active', $6, NOW(), NOW())`,
        [userIdStr, String(chatIdForLog), gameTypeFromReason, wagerAmountForLog.toString(), betDetails, reason]
      );
    } else if (associatedGameId && (reason.startsWith('won_') || reason.startsWith('lost_') || reason.startsWith('push_') || reason.startsWith('refund_'))) {
      let statusForLog = reason.split('_')[0]; // e.g., "won", "lost", "push", "refund"
      let payoutAmountForLog = (statusForLog === 'lost') ? 0n : change; // For wins/pushes/refunds, 'change' is positive. For losses, payout is 0.

      // console.log(`${LOG_PREFIX_UPDATEBAL} Updating existing bet record. Status: ${statusForLog}, Payout: ${payoutAmountForLog}.`);
      const updateBetResult = await operationClient.query(
        `UPDATE bets SET status = $1, payout_amount_lamports = $2, reason_tx = $3, processed_at = NOW()
         WHERE user_id = $4 AND bet_details->>'game_id' = $5 AND status = 'active'`,
        [statusForLog, payoutAmountForLog.toString(), reason, userIdStr, associatedGameId]
      );
      if (updateBetResult.rowCount === 0) {
        console.warn(`${LOG_PREFIX_UPDATEBAL} WARN: No 'active' bet found to update for game outcome. User: ${userIdStr}, GameID: ${associatedGameId}, Reason: ${reason}. This might happen if bet logging failed or game was already processed.`);
        // This could indicate a logic error or a duplicate processing attempt.
      }
    } else if (reason.startsWith('jackpot_win')) { // Special handling for jackpot payout logging
        // console.log(`${LOG_PREFIX_UPDATEBAL} Logging 'jackpot_win' as a special bet record. Amount: ${change}.`);
        betDetails.jackpot_won_lamports = change.toString();
        await operationClient.query(
            `INSERT INTO bets (user_id, chat_id, game_type, wager_amount_lamports, payout_amount_lamports, bet_details, status, reason_tx, created_at, processed_at)
             VALUES ($1, $2, 'jackpot_payout', '0', $3, $4, 'processed', $5, NOW(), NOW())`,
            [userIdStr, String(chatIdForLog || 'N/A'), change.toString(), betDetails, reason]
        );
    } else if (reason.startsWith('admin_') || reason.startsWith('deposit_') || reason.startsWith('withdraw_')) {
        // console.log(`${LOG_PREFIX_UPDATEBAL} Logging administrative or payment transaction: ${reason}. Amount: ${change}.`);
        // For direct balance adjustments not tied to a specific game bet, create a general transaction log.
        // This might be a separate table in a full system, or a specific 'game_type' here.
        await operationClient.query(
            `INSERT INTO bets (user_id, chat_id, game_type, wager_amount_lamports, payout_amount_lamports, bet_details, status, reason_tx, created_at, processed_at)
             VALUES ($1, $2, $3, $4, $5, $6, 'processed', $7, NOW(), NOW())`,
            [
                userIdStr,
                String(chatIdForLog || 'N/A_admin_tx'), // Chat ID might not always be relevant
                reason.split(':')[0], // e.g., 'admin_grant', 'deposit_manual'
                (change < 0n ? -change : 0n).toString(), // Wager if debit
                (change > 0n ? change : 0n).toString(),  // Payout if credit
                { "transaction_type": reason, "details": `Amount: ${change.toString()}` }, // Basic details
                reason
            ]
        );
    }


    if (!client_) {
      await operationClient.query('COMMIT');
      // console.log(`${LOG_PREFIX_UPDATEBAL} Transaction committed.`);
    }
    // console.log(`${LOG_PREFIX_UPDATEBAL} Balance updated to: ${proposedBalanceLamports}. Change: ${change}, Reason: ${reason}`);

    return {
      success: true,
      newBalance: Number(proposedBalanceLamports),
      newBalanceLamports: proposedBalanceLamports
    };

  } catch (error) {
    if (!client_) {
      try {
        await operationClient.query('ROLLBACK');
        // console.log(`${LOG_PREFIX_UPDATEBAL} Transaction rolled back due to error.`);
      } catch (rbErr) {
        console.error(`${LOG_PREFIX_UPDATEBAL} Rollback failed: ${rbErr.message}`, rbErr.stack);
      }
    }
    console.error(`${LOG_PREFIX_UPDATEBAL} Error updating balance (Reason: ${reason}): ${error.message}`, error.stack);
    return {
        success: false,
        error: `Database error during balance update: ${error.message}`,
        currentBalance: Number(currentBalanceLamportsFromDB || 0), // Use fetched balance if available
        currentBalanceLamports: (currentBalanceLamportsFromDB || 0n)
    };
  } finally {
    if (!client_ && operationClient) { // Only release if this function acquired the client
      operationClient.release();
      // console.log(`${LOG_PREFIX_UPDATEBAL} DB client released.`);
    }
  }
}


// --- Group Session Functions (In-Memory) ---
// These manage short-lived state for games like Coinflip/RPS in a specific group chat.
async function getGroupSession(chatId, chatTitleFromContext = null) {
  const LOG_PREFIX_GS = `[GroupSession_Get CH:${chatId}]`;
  const chatIdStr = String(chatId);
  // console.log(`${LOG_PREFIX_GS} Requesting session. Title from context: ${chatTitleFromContext}`);

  if (!groupGameSessions.has(chatIdStr)) {
    const newSession = {
      chatId: chatIdStr,
      chatTitle: chatTitleFromContext || `Group_${chatIdStr.slice(-5)}`, // Default title if not provided
      currentGameId: null,        // For games like Coinflip/RPS that occupy the chat
      currentGameType: null,
      currentBetAmount: null,
      lastActivity: new Date(),
    };
    groupGameSessions.set(chatIdStr, newSession);
    // console.log(`${LOG_PREFIX_GS} New group session created. Data: ${stringifyWithBigInt(newSession)}`);
    return { ...newSession }; // Return a copy for safety
  }

  const session = groupGameSessions.get(chatIdStr);
  // Update title if a more current one is provided and differs
  if (chatTitleFromContext && session.chatTitle !== chatTitleFromContext) {
    session.chatTitle = chatTitleFromContext;
    // console.log(`${LOG_PREFIX_GS} Updated title for session to "${chatTitleFromContext}"`);
  }
  session.lastActivity = new Date(); // Always update last activity on access
  // groupGameSessions.set(chatIdStr, session); // Not strictly necessary if 'session' is a direct reference, but good practice if copying.
                                            // Since we return a copy, modifying the original map entry is fine.
  // console.log(`${LOG_PREFIX_GS} Returning existing session. Last activity updated. Data: ${stringifyWithBigInt(session)}`);
  return { ...session }; // Return a copy
}

async function updateGroupGameDetails(chatId, gameId, gameType, betAmount) {
  const LOG_PREFIX_UGS = `[GroupSession_Update CH:${chatId}]`;
  const chatIdStr = String(chatId);
  // console.log(`${LOG_PREFIX_UGS} Updating game details. GameID: ${gameId}, Type: ${gameType}, Bet: ${betAmount}`);

  let session = groupGameSessions.get(chatIdStr);
  if (!session) {
    // console.log(`${LOG_PREFIX_UGS} No existing session, creating one implicitly (title might be generic).`);
    // This will create it with a generic title if not available from context here.
    // Call getGroupSession to ensure creation and get a base structure.
    session = await getGroupSession(chatIdStr, `Group_${chatIdStr.slice(-5)}`); // Use generic title
  } else {
    // If session exists, we want to modify the actual object in the map, not a copy yet.
  }

  // This logic is specific to single-instance games like Coinflip/RPS.
  // Multi-instance games (DiceEscalator, Dice21, etc.) don't use this group-level "current game" slot.
  const singleInstanceGameTypes = ['CoinFlip', 'RockPaperScissors']; // Canonical game type names

  if ((gameType && singleInstanceGameTypes.includes(gameType)) || gameId === null) {
    // Update only if it's a recognized single-instance game type or if we are clearing the game details (gameId is null)
    session.currentGameId = gameId;
    session.currentGameType = gameType;
    session.currentBetAmount = betAmount; // Store as number or BigInt consistently if needed
    // console.log(`${LOG_PREFIX_UGS} Session details updated for single-instance game type or clearing. GameID: ${session.currentGameId}, Type: ${session.currentGameType}, Bet: ${session.currentBetAmount}`);
  } else if (gameType && !singleInstanceGameTypes.includes(gameType)) {
    // console.log(`${LOG_PREFIX_UGS} Not updating group session's primary game slot for multi-instance game type: ${gameType}. Game ID: ${gameId}`);
  }


  session.lastActivity = new Date();
  groupGameSessions.set(chatIdStr, session); // Ensure the potentially modified session is set back into the map

  const betDisplay = (betAmount !== null && betAmount !== undefined && typeof formatCurrency === 'function')
    ? formatCurrency(Number(betAmount)) // formatCurrency expects a number
    : (betAmount !== null && betAmount !== undefined ? `${betAmount} units` : 'N/A');

  // console.log(`${LOG_PREFIX_UGS} Group session details processed. Current single-game slot: ID: ${session.currentGameId || 'None'}, Type: ${session.currentGameType || 'None'}, Bet: ${betDisplay}. Full session: ${stringifyWithBigInt(session)}`);
  return true;
}

console.log("Part 2: Database Operations & Data Management (DATABASE BACKED) (Enhanced) - Complete.");
// --- End of Part 2 ---
// --- Start of Part 3 ---
// index.js - Part 3: Telegram Helpers & Basic Game Utilities (Enhanced)
//---------------------------------------------------------------------------
console.log("Loading Part 3: Telegram Helpers & Basic Game Utilities (Enhanced)...");

// --- Telegram Specific Helper Functions ---

// Gets a display name from a user object (msg.from or a fetched user object) and escapes it for MarkdownV2
function getEscapedUserDisplayName(userObject) {
  if (!userObject) return escapeMarkdownV2("Valued Player"); // More engaging default

  // User objects from getUser will have .firstName, .username, .userId
  // User objects from msg.from will have .first_name, .username, .id
  const firstName = userObject.first_name || userObject.firstName;
  const username = userObject.username;
  const id = userObject.id || userObject.userId;

  // Prefer first name, fallback to username, fallback to a generic User ID display
  const name = firstName || username || `Player ${String(id).slice(-4)}`; // Show last 4 of ID for some uniqueness
  return escapeMarkdownV2(name);
}

// Creates a MarkdownV2 mention link for a user object
function createUserMention(userObject) {
  if (!userObject) return escapeMarkdownV2("Esteemed Guest");

  const id = userObject.id || userObject.userId;
  if (!id) return escapeMarkdownV2("Unknown Player"); // Should not happen if userObject is valid

  // Use the more comprehensive getEscapedUserDisplayName for the link text part
  const displayName = getEscapedUserDisplayName(userObject); // This is already escaped

  // Format: [Link Text](tg://user?id=USER_ID)
  // Note: The displayName from getEscapedUserDisplayName is already escaped.
  // However, Telegram links with MarkdownV2 require the link text to be unescaped if it contains Markdown characters.
  // Since getEscapedUserDisplayName escapes everything, we need to be careful.
  // For mentions, it's often safer to use a simpler, non-Markdown name in the [] part if issues arise,
  // or ensure the display name chosen doesn't conflict.
  // Let's use a simpler, directly escaped name for the mention text for robustness.
  const simpleName = userObject.first_name || userObject.firstName || userObject.username || `Player ${String(id).slice(-4)}`;

  return `[${escapeMarkdownV2(simpleName)}](tg://user?id=${id})`;
}

// Gets a player's display reference, preferring @username for easy clicking in some contexts,
// but falls back to a non-@ name for general display. Escapes for MarkdownV2.
function getPlayerDisplayReference(userObject, preferUsernameTag = false) {
  if (!userObject) return escapeMarkdownV2("Mystery Player");

  const username = userObject.username;
  if (preferUsernameTag && username) {
    return `@${escapeMarkdownV2(username)}`; // Ideal for direct tagging if user has a username
  }

  // Fallback to the standard display name for general use
  return getEscapedUserDisplayName(userObject);
}

// --- General Utility Functions ---

// Formats a number or BigInt as currency (e.g., "1,000 credits")
// Now explicitly handles BigInt and allows custom currency names.
function formatCurrency(amount, currencyName = "credits", displayLamportsAsFull = false) {
  let num;
  let originalAmountIsBigInt = (typeof amount === 'bigint');

  if (originalAmountIsBigInt) {
    if (displayLamportsAsFull || currencyName.toLowerCase() === 'lamports') {
        num = amount; // Keep as BigInt for toLocaleString if displaying raw lamports
    } else {
        // Assuming standard conversion where 1 credit = 1,000,000 lamports (or similar, defined by DEFAULT_STARTING_BALANCE_LAMPORTS logic)
        // This needs to be consistent with how DEFAULT_STARTING_BALANCE_LAMPORTS is interpreted.
        // If 100,000,000 lamports = 100 credits, then 1 credit = 1,000,000 lamports.
        // For simplicity, let's assume 1,000,000 lamports = 1 credit for display purposes.
        // THIS IS A CRITICAL ASSUMPTION. Adjust divisor if your unit economics are different.
        const divisor = 1000000n; // Example: 1M lamports = 1 credit for display
        if (amount >= divisor || amount <= -divisor || amount === 0n ) { // Only divide if it makes sense or is zero
             num = Number(amount / divisor); // Convert to Number for credit display
        } else {
            // If lamports are less than 1 unit of 'credits' (but not zero), display as fraction of credit or raw lamports
            // For now, let's show raw lamports if it's a small, non-zero amount when 'credits' is requested.
            // This avoids "0 credits" for small non-zero lamport amounts.
            // Or, always show credits, which might be 0 for small lamport values.
            // Let's stick to showing "0 credits" if it's less than 1 credit unit after division.
            num = Number(amount / divisor);
            if (Number.isFinite(num) && Math.abs(num) < 0.01 && num !== 0) { // if it's a tiny fraction like 0.005
                 return `${amount.toString()} lamports`; // show raw lamports instead of "0.00 credits"
            }
        }

    }
  } else {
    num = Number(amount);
  }

  if (isNaN(num) && !originalAmountIsBigInt) { // If it wasn't a BigInt and still NaN
    console.warn(`[formatCurrency] Received non-numeric amount that's not BigInt: ${amount}`);
    num = 0; // Default to 0 if input is truly unparseable
  }


  // For BigInts representing whole lamports, no decimal places.
  // For Numbers (credits), allow up to 2 decimal places if they exist.
  const fractionDigits = (originalAmountIsBigInt && (displayLamportsAsFull || currencyName.toLowerCase() === 'lamports'))
                            ? 0
                            : (Number.isInteger(num) ? 0 : 2);

  try {
    return `${num.toLocaleString(undefined, { // Use user's locale for formatting
      minimumFractionDigits: 0, // Always show at least 0
      maximumFractionDigits: fractionDigits
    })} ${currencyName}`;
  } catch (e) {
    // Fallback for very large BigInts that toLocaleString on Number might struggle with
    if (originalAmountIsBigInt) {
        return `${amount.toString()} ${currencyName}`;
    }
    console.error(`[formatCurrency] Error formatting ${num}: ${e.message}`);
    return `${String(amount)} ${currencyName}`; // Raw amount as last resort
  }
}


// Generates a unique-ish ID for game instances
function generateGameId(prefix = "game") {
  const timestamp = Date.now().toString(36); // Base36 for shorter string
  // Add a more substantial random component to significantly reduce collision likelihood
  const randomSuffix = Math.random().toString(36).substring(2, 10); // 8 random alphanumeric chars
  return `${prefix}_${timestamp}_${randomSuffix}`;
}

// --- Dice Display Utilities ---

// Formats an array of dice roll numbers into a string with a generic dice emoji and the number
// Example: [5, 2] -> "🎲 5  🎲 2" (using double space for slight separation)
function formatDiceRolls(rollsArray, diceEmoji = '🎲') {
  if (!Array.isArray(rollsArray) || rollsArray.length === 0) return '';
  // Using generic dice emoji + number for each roll
  const diceVisuals = rollsArray.map(roll => `${diceEmoji} ${roll}`);
  return diceVisuals.join('  '); // Join with double space if multiple rolls
}

// Generates an internal dice roll for the BOT's turn in games or when Telegram's sendDice fails
// This is SEPARATE from the rolls generated by `bot.sendDice()`.
function rollDie(sides = 6) {
  sides = Number.isInteger(sides) && sides > 1 ? sides : 6; // Default to 6 sides, ensure sides > 1
  return Math.floor(Math.random() * sides) + 1;
}

// --- Placeholder for Future Payment System Utilities ---
// Example: Function to generate a unique transaction ID for deposits/withdrawals
// function generatePaymentTransactionId(userId) {
//   const now = Date.now().toString(36);
//   const userPart = String(userId).slice(-4);
//   const randomPart = Math.random().toString(36).substring(2, 8);
//   return `txn_${userPart}_${now}_${randomPart}`;
// }
// This would be used when actual payment processing is added.

console.log("Part 3: Telegram Helpers & Basic Game Utilities (Enhanced) - Complete.");
// --- End of Part 3 ---
// --- Start of Part 4 ---
// index.js - Part 4: Simplified Game Logic (Enhanced)
//---------------------------------------------------------------------------
console.log("Loading Part 4: Simplified Game Logic (Enhanced)...");

// --- Coinflip Logic ---
// Returns an object with the outcome, a display string, and an emoji.
function determineCoinFlipOutcome() {
  const isHeads = Math.random() < 0.5; // 50% chance for heads
  return isHeads
    ? { outcome: 'heads', outcomeString: "Heads", emoji: '🪙' } // Using a standard coin emoji
    : { outcome: 'tails', outcomeString: "Tails", emoji: '🪙' };
}

// --- Dice Logic (Internal for Bot's Turn or Fallback) ---
// This determines the outcome for the BOT's internal rolls or when `bot.sendDice` fails.
// It uses the internal `rollDie` function defined in Part 3.
function determineDieRollOutcome(sides = 6) {
  // Ensure rollDie function is defined (expected in Part 3)
  if (typeof rollDie !== 'function') {
     console.error("[determineDieRollOutcome] CRITICAL Error: rollDie function is not defined from Part 3.");
     // Fallback to a predictable, safe roll if rollDie is missing, though this indicates a serious issue.
     return { roll: 1, emoji: '🎲' }; // Default fallback roll
  }
  sides = Number.isInteger(sides) && sides > 1 ? sides : 6; // Ensure valid sides (at least 2)
  const roll = rollDie(sides); // Use the internal function

  // The emoji here is for potential direct use if not formatting via formatDiceRolls,
  // but generally, formatDiceRolls will handle the display.
  return { roll: roll, emoji: '🎲' }; // Return the roll number and a generic dice emoji
                                      // Display formatting (e.g., "🎲 5") is best handled by formatDiceRolls.
}

// Constant defining the losing roll in Dice Escalator (from Part 1, but good to acknowledge its use here)
// const DICE_ESCALATOR_BUST_ON = 1; (This is defined and used in Part 5b game logic)

// --- Rock Paper Scissors (RPS) Logic ---
const RPS_CHOICES = {
  ROCK: 'rock',
  PAPER: 'paper',
  SCISSORS: 'scissors'
};
const RPS_EMOJIS = {
  [RPS_CHOICES.ROCK]: '🪨',    // Rock emoji
  [RPS_CHOICES.PAPER]: '📄',   // Paper emoji
  [RPS_CHOICES.SCISSORS]: '✂️' // Scissors emoji
};
// Defines what each choice beats and the verb for the action.
const RPS_RULES = {
  [RPS_CHOICES.ROCK]: { beats: RPS_CHOICES.SCISSORS, verb: "crushes" },
  [RPS_CHOICES.PAPER]: { beats: RPS_CHOICES.ROCK, verb: "covers" },
  [RPS_CHOICES.SCISSORS]: { beats: RPS_CHOICES.PAPER, verb: "cuts" }
};

// Gets a random RPS choice for the bot or an opponent if needed.
function getRandomRPSChoice() {
  const choicesArray = Object.values(RPS_CHOICES);
  const randomChoiceKey = choicesArray[Math.floor(Math.random() * choicesArray.length)];
  return { choice: randomChoiceKey, emoji: RPS_EMOJIS[randomChoiceKey] };
}

// Determines the outcome of an RPS match given two choices (e.g., RPS_CHOICES.ROCK).
// Returns a detailed result object.
function determineRPSOutcome(player1ChoiceKey, player2ChoiceKey) {
  const LOG_PREFIX_RPS_OUTCOME = "[RPS_Outcome]";
  // Validate inputs against defined choices (case-insensitive for input flexibility, but uses canonical keys internally)
  const p1c = String(player1ChoiceKey).toLowerCase();
  const p2c = String(player2ChoiceKey).toLowerCase();

  if (!Object.values(RPS_CHOICES).includes(p1c) || !Object.values(RPS_CHOICES).includes(p2c)) {
    console.warn(`${LOG_PREFIX_RPS_OUTCOME} Invalid choices: P1='${player1ChoiceKey}', P2='${player2ChoiceKey}'`);
    return {
        result: 'error',
        description: "Invalid choices were made.",
        player1: { choice: player1ChoiceKey, emoji: '❓' },
        player2: { choice: player2ChoiceKey, emoji: '❓' }
    };
  }

  const p1Emoji = RPS_EMOJIS[p1c];
  const p2Emoji = RPS_EMOJIS[p2c];

  let resultDescription;
  let outcome; // 'win_player1', 'win_player2', 'draw'

  if (p1c === p2c) { // Draw case
    outcome = 'draw';
    resultDescription = `${p1Emoji} ${p1c.charAt(0).toUpperCase() + p1c.slice(1)} vs ${p2Emoji} ${p2c.charAt(0).toUpperCase() + p2c.slice(1)}. It's a Draw!`;
  } else if (RPS_RULES[p1c]?.beats === p2c) { // Player 1 wins
    outcome = 'win_player1';
    resultDescription = `${p1Emoji} ${p1c.charAt(0).toUpperCase() + p1c.slice(1)} ${RPS_RULES[p1c].verb} ${p2Emoji} ${p2c.charAt(0).toUpperCase() + p2c.slice(1)}. Player 1 wins!`;
  } else { // Player 2 wins (since it's not a draw and P1 didn't win, implies P2's choice beats P1's)
    outcome = 'win_player2';
    // It's good practice to ensure player2's winning rule is also defined, even if logically implied here
    resultDescription = `${p2Emoji} ${p2c.charAt(0).toUpperCase() + p2c.slice(1)} ${RPS_RULES[p2c]?.verb || 'beats'} ${p1Emoji} ${p1c.charAt(0).toUpperCase() + p1c.slice(1)}. Player 2 wins!`;
  }

  return {
    result: outcome,
    description: resultDescription, // A full sentence describing the outcome
    player1: { choice: p1c, emoji: p1Emoji },
    player2: { choice: p2c, emoji: p2Emoji }
  };
}
console.log("Part 4: Simplified Game Logic (Enhanced) - Complete.");

// --- End of Part 4 ---
// --- Start of Part 5a (Segment 1 of 2) ---
// index.js - Part 5a: Message & Callback Handling (Core Listeners, General Commands, UX Enhancements)
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 1): Message & Callback Handling, General Commands, UX Enhancements...");

// --- Game Constants & Configuration ---
// Basic constants like COMMAND_COOLDOWN_MS, MIN_BET_AMOUNT, MAX_BET_AMOUNT, JOIN_GAME_TIMEOUT_MS are from Part 1
// TARGET_JACKPOT_SCORE for Dice Escalator is from Part 1

// --- Game Identifiers (used for callback data, rules, etc.) ---
const GAME_IDS = {
  COINFLIP: 'coinflip',
  RPS: 'rps',
  DICE_ESCALATOR: 'dice_escalator',
  DICE_21: 'dice21',
  OVER_UNDER_7: 'ou7',
  DUEL: 'duel',
  LADDER: 'ladder',
  SEVEN_OUT: 'sevenout',
  SLOT_FRENZY: 'slotfrenzy',
};

// --- CONSTANTS FOR OVER/UNDER 7 ---
const OU7_PAYOUT_NORMAL = 1; // 1:1 profit (total 2x bet returned)
const OU7_PAYOUT_SEVEN = 4;  // 4:1 profit (total 5x bet returned)
const OU7_DICE_COUNT = 2;

// --- CONSTANTS FOR HIGH ROLLER DUEL ---
const DUEL_DICE_COUNT = 2; // Number of dice each participant rolls

// --- CONSTANTS FOR GREED'S LADDER ---
const LADDER_ROLL_COUNT = 3; // Fixed number of rolls for the player
const LADDER_BUST_ON = 1;    // Rolling a 1 busts the player
// Example Payout Tiers for Greed's Ladder (sum of LADDER_ROLL_COUNT dice):
const LADDER_PAYOUTS = [ // [minScore, maxScore, profitMultiplier (0 = push, -1 = lose bet)]
  // Tiers are for the sum of LADDER_ROLL_COUNT dice, assuming no bust.
  // Example for 3 rolls (max sum 18, min sum 3 (if no bust), or 2 if a 1 isn't bust)
  // Max sum if no 1s: 3*6 = 18. Min sum if no 1s: 3*2 = 6 (as 1 is bust)
  // Let's adjust for LADDER_BUST_ON = 1; min score if no bust is LADDER_ROLL_COUNT * 2
  { min: (LADDER_ROLL_COUNT * 5 + 1), max: (LADDER_ROLL_COUNT * 6), multiplier: 5, label: "🌟 Excellent Climb!" }, // e.g., 3 rolls: 16-18
  { min: (LADDER_ROLL_COUNT * 4 + 1), max: (LADDER_ROLL_COUNT * 5), multiplier: 3, label: "🎉 Great Ascent!" },   // e.g., 3 rolls: 13-15
  { min: (LADDER_ROLL_COUNT * 3 + 1), max: (LADDER_ROLL_COUNT * 4), multiplier: 1, label: "👍 Good Progress!" },   // e.g., 3 rolls: 10-12
  { min: (LADDER_ROLL_COUNT * 2),     max: (LADDER_ROLL_COUNT * 3), multiplier: 0, label: "😐 Steady Steps." },    // e.g., 3 rolls: 6-9 (Push, min score 2*3=6)
  // Anything below LADDER_ROLL_COUNT * 2 (and not a bust) would be a loss if not covered by push.
  // This structure implies scores below LADDER_ROLL_COUNT * 2 are losses.
  // Or define an explicit loss tier:
  // { min: LADDER_ROLL_COUNT * 1 (if 1 not bust) or LADDER_ROLL_COUNT*2, max: (LADDER_ROLL_COUNT * 2)-1, multiplier: -1, label: "Unlucky Slip."}
];


// --- CONSTANTS FOR SEVENS OUT ---
// Uses standard two 6-sided dice. Win/loss conditions handled in game logic.

// --- CONSTANTS FOR SLOT FRUIT FRENZY ---
// Values 1-64 from Telegram's 🎰 emoji.
// This is a simplified example; a real slot would have more complex reel mapping.
const SLOT_PAYOUTS = {
  // Value: { multiplier (profit), symbols, label }
  64: { multiplier: 100, symbols: "💎💎💎", label: "MEGA JACKPOT!" }, // Triple Diamond (Telegram value 64)
  1:  { multiplier: 20,  symbols: "🔔🔔🔔", label: "Triple Bell!" },    // Triple Bell (Telegram value 1)
  22: { multiplier: 10,  symbols: "🍊🍊🍊", label: "Triple Orange!" },  // Triple Orange (Telegram value 22)
  43: { multiplier: 5,   symbols: "🍋🍋🍋", label: "Triple Lemon!" },   // Triple Lemon (Telegram value 43)
  // Adding a few more mid-tier wins
  16: { multiplier: 2,   symbols: "🍒🍒ANY", label: "Two Cherries!" }, // Example for BAR or other symbol (value 16) - simplified
  32: { multiplier: 3,   symbols: "🍉🍉ANY", label: "Two Watermelons!"}, // Example (value 32)
  // Most of the 64 values (1-64) will result in a loss.
};
const SLOT_DEFAULT_LOSS_MULTIPLIER = -1; // Indicates loss of bet amount

// --- Main Message Handler (`bot.on('message')`) ---
bot.on('message', async (msg) => {
  const LOG_PREFIX_MSG = `[MSG_Handler TID:${msg.message_id}]`;

  if (isShuttingDown) {
    // console.log(`${LOG_PREFIX_MSG} Shutdown in progress. Ignoring message from UserID: ${msg.from?.id} in ChatID: ${msg.chat?.id}.`);
    return;
  }

  // Basic validation of message structure
  if (!msg || !msg.from || !msg.chat || !msg.date) {
    // console.log(`${LOG_PREFIX_MSG} Ignoring malformed or incomplete message object: ${stringifyWithBigInt(msg)}`);
    return;
  }
   // console.log(`${LOG_PREFIX_MSG} Received message from UserID: ${msg.from.id} (@${msg.from.username || 'N/A'}) in ChatID: ${msg.chat.id} ('${msg.chat.title || 'Private'}'). Text: "${msg.text || msg.caption || msg.dice?.emoji || '[NoText/Media]'}"`);


  // Ignore messages from other bots, but allow from self if necessary (though generally not for commands)
  if (msg.from.is_bot) {
    try {
      if (!bot || typeof bot.getMe !== 'function') { // Ensure bot.getMe is available
          // console.log(`${LOG_PREFIX_MSG} bot.getMe not available, ignoring potential self-message without ID check.`);
          return; // Cannot determine if it's a self-message reliably
      }
      const selfBotInfo = await bot.getMe();
      if (String(msg.from.id) !== String(selfBotInfo.id)) {
        // console.log(`${LOG_PREFIX_MSG} Ignoring message from other bot: ${msg.from.id}`);
        return;
      }
      // If it's a message from self, allow it to pass for now, might be results of sendDice etc.
      // console.log(`${LOG_PREFIX_MSG} Processing message from self (BotID: ${selfBotInfo.id}).`);
    } catch (getMeError) {
      console.error(`${LOG_PREFIX_MSG} Error in getMe self-check: ${getMeError.message}. Ignoring bot message.`);
      return;
    }
  }

  const userId = String(msg.from.id);
  const chatId = String(msg.chat.id);
  const text = msg.text || ""; // Ensure text is a string, even if undefined
  const chatType = msg.chat.type; // 'private', 'group', 'supergroup', 'channel'
  const messageId = msg.message_id;
  let userForCommandProcessing; // Will be populated by getUser

  // Command processing logic
  if (text.startsWith('/')) {
    // console.log(`${LOG_PREFIX_MSG} Command received: "${text}" from UserID: ${userId} in ChatID: ${chatId}.`);
    try {
      userForCommandProcessing = await getUser(userId, msg.from.username, msg.from.first_name);
      if (!userForCommandProcessing) {
        await safeSendMessage(chatId, "😕 Sorry, there was an issue accessing your player profile. Please try again shortly.", {});
        return;
      }
      // console.log(`${LOG_PREFIX_MSG} User profile fetched for command processing: ${getPlayerDisplayReference(userForCommandProcessing)}`);
    } catch (e) {
      console.error(`${LOG_PREFIX_MSG} Error fetching user for command: ${e.message}`, e.stack);
      await safeSendMessage(chatId, "🛠️ Apologies, a technical hiccup occurred while fetching your details. Please try again.", {});
      return;
    }

    // Command Cooldown Check
    const now = Date.now();
    if (userCooldowns.has(userId) && (now - userCooldowns.get(userId)) < COMMAND_COOLDOWN_MS) {
      // console.log(`${LOG_PREFIX_MSG} User ${userId} command ("${text}") ignored due to cooldown.`);
      // Optionally send a silent notification or just ignore
      // await bot.answerCallbackQuery() can be used for button clicks, but not direct messages.
      // For messages, usually just ignoring or a subtle hint if it becomes a problem.
      return;
    }
    userCooldowns.set(userId, now); // Update last command timestamp

    const commandArgs = text.substring(1).split(/\s+/); // Split by one or more spaces
    const commandName = commandArgs.shift()?.toLowerCase(); // Get command and remove from args

    // console.log(`${LOG_PREFIX_MSG} Parsed Command: /${commandName}, Args: [${commandArgs.join(', ')}]`);

    // Master switch for all commands
    switch (commandName) {
      case 'start':
      case 'help':
        await handleHelpCommand(chatId, userForCommandProcessing); // Pass user object for personalized greeting
        break;
      case 'balance':
      case 'bal':
        await handleBalanceCommand(chatId, userForCommandProcessing);
        break;
      case 'rules':
      case 'info': // Alias for rules
        await handleRulesCommand(chatId, userForCommandProcessing);
        break;
      case 'startcoinflip':
      case 'coinflip':
        if (chatType === 'private') {
          await safeSendMessage(chatId, "🪙 Coinflip is a group game! Please try this command in a group chat.", {});
          break;
        }
        let betCF = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT; // Default to min bet
        if (isNaN(betCF) || betCF < MIN_BET_AMOUNT || betCF > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, please enter a valid bet for Coinflip: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        await handleStartGroupCoinFlipCommand(chatId, userForCommandProcessing, betCF, messageId);
        break;
      case 'startrps':
      case 'rps':
        if (chatType === 'private') {
          await safeSendMessage(chatId, "🪨📄✂️ Rock Paper Scissors is a group game! Try it in a group chat.", {});
          break;
        }
        let betRPS = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT;
        if (isNaN(betRPS) || betRPS < MIN_BET_AMOUNT || betRPS > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, please enter a valid bet for RPS: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        await handleStartGroupRPSCommand(chatId, userForCommandProcessing, betRPS, messageId);
        break;
      case 'startdice': // Dice Escalator (alias)
      case 'diceescalator':
        if (chatType === 'private') {
          await safeSendMessage(chatId, "🎲 Dice Escalator is best enjoyed in group play! Please use this command in a group.", {});
          break;
        }
        let betDE = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT;
        if (isNaN(betDE) || betDE < MIN_BET_AMOUNT || betDE > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, valid bet for Dice Escalator: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        if (typeof handleStartDiceEscalatorCommand === 'function') {
          await handleStartDiceEscalatorCommand(chatId, userForCommandProcessing, betDE, messageId);
        } else {
          console.error(`${LOG_PREFIX_MSG} Dice Escalator command called, but handler is not defined!`);
          await safeSendMessage(chatId, "🎲 Dice Escalator is currently under maintenance. Please try again later.", {});
        }
        break;
      case 'jackpot':
        await handleJackpotCommand(chatId, userForCommandProcessing);
        break;
      case 'dice21':
      case 'd21':
      case 'blackjack': // Alias
         if (chatType === 'private') {
          await safeSendMessage(chatId, "🃏 Dice 21 (Blackjack style) is designed for group tables! Please use this command in a group.", {});
          break;
        }
        let betD21 = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT;
        if (isNaN(betD21) || betD21 < MIN_BET_AMOUNT || betD21 > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, valid bet for Dice 21: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        if (typeof handleStartDice21Command === 'function') {
            await handleStartDice21Command(chatId, userForCommandProcessing, betD21, messageId);
        } else {
            console.error(`${LOG_PREFIX_MSG} Dice 21 command called, but handler is not defined!`);
            await safeSendMessage(chatId, "🃏 Dice 21 is polishing its dice. Please try again later.", {});
        }
        break;
      case 'ou7':
      case 'overunder7':
        if (chatType === 'private') {
          await safeSendMessage(chatId, "🎲 Over/Under 7 is a group dice game! Try it in a group.", {});
          break;
        }
        let betOU7 = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT;
        if (isNaN(betOU7) || betOU7 < MIN_BET_AMOUNT || betOU7 > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, valid bet for Over/Under 7: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        if (typeof handleStartOverUnder7Command === 'function') {
            await handleStartOverUnder7Command(chatId, userForCommandProcessing, betOU7, messageId);
        } else {
            console.error(`${LOG_PREFIX_MSG} Over/Under 7 command called, but handler is not defined!`);
            await safeSendMessage(chatId, "🎲 Over/Under 7 is currently unavailable. Check back soon!", {});
        }
        break;
      case 'duel':
      case 'highroller': // Alias
        if (chatType === 'private') {
          await safeSendMessage(chatId, "⚔️ High Roller Duel is a group game! Challenge the bot in a group chat.", {});
          break;
        }
        let betDuel = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT;
        if (isNaN(betDuel) || betDuel < MIN_BET_AMOUNT || betDuel > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, valid bet for Duel: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        if (typeof handleStartDuelCommand === 'function') {
            await handleStartDuelCommand(chatId, userForCommandProcessing, betDuel, messageId);
        } else {
            console.error(`${LOG_PREFIX_MSG} Duel command called, but handler is not defined!`);
            await safeSendMessage(chatId, "⚔️ The Duel arena is being prepared. Please try again later.", {});
        }
        break;
      case 'ladder':
      case 'greedsladder':
        if (chatType === 'private') {
          await safeSendMessage(chatId, "🪜 Greed's Ladder is a group dice game! Try it in a group.", {});
          break;
        }
        let betLadder = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT;
        if (isNaN(betLadder) || betLadder < MIN_BET_AMOUNT || betLadder > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, valid bet for Greed's Ladder: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        if (typeof handleStartLadderCommand === 'function') {
            await handleStartLadderCommand(chatId, userForCommandProcessing, betLadder, messageId);
        } else {
            console.error(`${LOG_PREFIX_MSG} Ladder command called, but handler is not defined!`);
            await safeSendMessage(chatId, "🪜 Greed's Ladder is currently under construction. Check back soon!", {});
        }
        break;
      case 'sevenout':
      case 's7':
      case 'craps': // Alias
        if (chatType === 'private') {
          await safeSendMessage(chatId, "🎲 Sevens Out (Craps style) is a group game! Roll the dice in a group chat.", {});
          break;
        }
        let betS7 = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT;
        if (isNaN(betS7) || betS7 < MIN_BET_AMOUNT || betS7 > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, valid bet for Sevens Out: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        if (typeof handleStartSevenOutCommand === 'function') {
            await handleStartSevenOutCommand(chatId, userForCommandProcessing, betS7, messageId);
        } else {
            console.error(`${LOG_PREFIX_MSG} Sevens Out command called, but handler is not defined!`);
            await safeSendMessage(chatId, "🎲 Sevens Out is currently unavailable. Please try again later.", {});
        }
        break;
      case 'slot':
      case 'slots':
      case 'slotfrenzy':
        if (chatType === 'private') {
          await safeSendMessage(chatId, "🎰 Slot Frenzy is a group game! Spin the reels in a group chat.", {});
          break;
        }
        let betSlot = commandArgs[0] ? parseInt(commandArgs[0], 10) : MIN_BET_AMOUNT;
        if (isNaN(betSlot) || betSlot < MIN_BET_AMOUNT || betSlot > MAX_BET_AMOUNT) {
          await safeSendMessage(chatId, `${getPlayerDisplayReference(userForCommandProcessing)}, valid bet for Slots: ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))} - ${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}${escapeMarkdownV2(".")}`, { parse_mode: 'MarkdownV2' });
          break;
        }
        if (typeof handleStartSlotCommand === 'function') {
            await handleStartSlotCommand(chatId, userForCommandProcessing, betSlot, messageId);
        } else {
            console.error(`${LOG_PREFIX_MSG} Slot command called, but handler is not defined!`);
            await safeSendMessage(chatId, "🎰 The Slot Machine is being polished. Check back soon!", {});
        }
        break;
      // ADMIN COMMANDS (Example - ensure ADMIN_USER_ID is set and checked)
      case 'grant':
        if (ADMIN_USER_ID && userId === ADMIN_USER_ID) {
            const amountToGrant = commandArgs[0] ? BigInt(commandArgs[0]) : null;
            const targetUserId = commandArgs[1] || userId; // Grant to self if no target user
            if (amountToGrant === null || amountToGrant <= 0n) {
                await safeSendMessage(chatId, "Usage: /grant <amount_lamports> [target_user_id]", {});
                break;
            }
            // Ensure target user exists
            let targetUser;
            try {
                targetUser = await getUser(targetUserId);
                if (!targetUser) throw new Error("Target user not found by getUser.");
            } catch (grantGetUserError) {
                console.error(`${LOG_PREFIX_MSG} Admin Grant: Error fetching target user ${targetUserId}: ${grantGetUserError.message}`);
                await safeSendMessage(chatId, `Could not find or create target user ${targetUserId} for grant.`, {});
                break;
            }

            const grantResult = await updateUserBalance(targetUserId, amountToGrant, `admin_grant:${userId}_to_${targetUserId}`, null, null, chatId);
            if (grantResult.success) {
                await safeSendMessage(chatId, `✅ Successfully granted ${escapeMarkdownV2(formatCurrency(Number(amountToGrant), "lamports", true))} to ${getPlayerDisplayReference(targetUser)}. New balance: ${escapeMarkdownV2(formatCurrency(grantResult.newBalance, "credits"))}.`, { parse_mode: 'MarkdownV2' });
            } else {
                await safeSendMessage(chatId, `❌ Failed to grant credits: ${escapeMarkdownV2(grantResult.error || "Unknown error")}`, { parse_mode: 'MarkdownV2'});
            }
        } else {
            await safeSendMessage(chatId, "🤔 This command seems to be for administrators only.", {});
        }
        break;

      default:
        // Only send "Unknown command" in private chats or if it clearly looks like a command attempt
        if (chatType === 'private' || text.startsWith('/')) {
          await safeSendMessage(chatId, `❓ Unknown command: \`/${escapeMarkdownV2(commandName || "")}\`\nType \`/help\` for a list of available commands.`, { parse_mode: 'MarkdownV2' });
        }
    }
  } // End of command processing
}); // End of bot.on('message')

// --- Callback Query Handler (`bot.on('callback_query')`) ---
bot.on('callback_query', async (callbackQuery) => {
  const LOG_PREFIX_CBQ = `[CBQ_Handler ID:${callbackQuery.id}]`;
  // console.log(`${LOG_PREFIX_CBQ} Received CBQ from UserID: ${callbackQuery.from.id} in ChatID: ${callbackQuery.message?.chat?.id}. Data: "${callbackQuery.data}"`);

  if (isShuttingDown) {
    // console.log(`${LOG_PREFIX_CBQ} Shutdown in progress. Ignoring CBQ.`);
    try { await bot.answerCallbackQuery(callbackQuery.id); } catch(e) {/* ignore */}
    return;
  }

  const msg = callbackQuery.message;
  const userFromCb = callbackQuery.from; // User who pressed the button
  const callbackQueryId = callbackQuery.id;
  const data = callbackQuery.data; // e.g., "action:param1:param2"

  if (!msg || !userFromCb || !data) {
    console.error(`${LOG_PREFIX_CBQ} Ignoring malformed or incomplete callback query object.`);
    try { await bot.answerCallbackQuery(callbackQueryId, { text: "Error: Invalid query." }); } catch(e) {/* ignore */}
    return;
  }

  const userId = String(userFromCb.id);
  const chatId = String(msg.chat.id);
  const originalMessageId = msg.message_id; // ID of the message with the button

  // Always answer the callback query to remove the "loading" spinner on the button
  // Specific messages can be overridden in handlers if needed.
  // Default answer is empty, just to acknowledge.
  try { await bot.answerCallbackQuery(callbackQueryId); } catch(e) {
      // console.warn(`${LOG_PREFIX_CBQ} Non-critical: Failed to answer CBQ (already answered or other issue): ${e.message}`);
  }

  let userObjectForCallback;
  try {
    userObjectForCallback = await getUser(userId, userFromCb.username, userFromCb.first_name);
    if (!userObjectForCallback) {
      throw new Error("User data could not be fetched for callback processing.");
    }
    // console.log(`${LOG_PREFIX_CBQ} User profile fetched for CBQ processing: ${getPlayerDisplayReference(userObjectForCallback)}`);
  } catch(e) {
    console.error(`${LOG_PREFIX_CBQ} Error fetching user for callback: ${e.message}`, e.stack);
    await safeSendMessage(chatId, "🛠️ Apologies, a technical hiccup occurred while fetching your details for this action.", {});
    return;
  }

  const [action, ...params] = data.split(':');
  // console.log(`${LOG_PREFIX_CBQ} Parsed Action: "${action}", Params: [${params.join(', ')}]`);

  try {
    switch (action) {
      // --- General Actions (Rules, Deposit, Withdraw) ---
      case RULES_CALLBACK_PREFIX.slice(0, -1): // e.g. if prefix is "rules_game_", action is "rules_game"
        const gameCodeForRule = params[0];
        if (!gameCodeForRule) throw new Error("Missing game_code for rules display.");
        await handleDisplayGameRules(chatId, originalMessageId, gameCodeForRule, userObjectForCallback);
        break;
      case DEPOSIT_CALLBACK_ACTION:
        await handleGenericCallbackMessage(chatId, "💰 **Real Money Deposits** 💰\n\nThis feature is currently under development and will be available soon!\n\nOur team is working hard to provide you with a seamless and secure deposit experience. Stay tuned for updates!\n\nFor now, enjoy playing with your complimentary credits. If you are an admin or tester, you might have access to test grant commands.", {}, originalMessageId, true);
        break;
      case WITHDRAW_CALLBACK_ACTION:
        await handleGenericCallbackMessage(chatId, "💸 **Real Money Withdrawals** 💸\n\nCashing out your winnings will be available soon!\n\nWe are implementing a secure and efficient withdrawal system. We appreciate your patience!\n\nKeep an eye out for announcements regarding this feature.", {}, originalMessageId, true);
        break;
      case QUICK_DEPOSIT_CALLBACK_ACTION: // Often shown after games
         await handleGenericCallbackMessage(chatId, "💰 **Quick Deposit** 💰\n\nNeed more credits to continue the fun? The full deposit system is coming soon!\n\nThank you for your enthusiasm!", {}, originalMessageId, true); // Edit the message if originalMessageId exists
        break;

      // --- Coinflip & RPS Callbacks (Group Games) ---
      case 'join_game': // Used by Coinflip, RPS
        if (!params[0]) throw new Error("Missing gameId for join_game action.");
        await handleJoinGameCallback(chatId, userObjectForCallback, params[0], originalMessageId);
        break;
      case 'cancel_game': // Used by Coinflip, RPS
        if (!params[0]) throw new Error("Missing gameId for cancel_game action.");
        await handleCancelGameCallback(chatId, userObjectForCallback, params[0], originalMessageId);
        break;
      case 'rps_choose':
        if (params.length < 2) throw new Error("Missing gameId or choice for rps_choose action."); // gameId, choice
        await handleRPSChoiceCallback(chatId, userObjectForCallback, params[0], params[1], originalMessageId);
        break;

      // --- Dice Escalator Callbacks ---
      case 'de_roll_prompt':
      case 'de_cashout':
      case 'jackpot_display_noop': // Handles clicks on the jackpot display button
        if (action === 'jackpot_display_noop') {
             // console.log(`${LOG_PREFIX_CBQ} Jackpot display button clicked (no-op). GameID param (if any): ${params[0]}`);
             // No actual game logic, just acknowledge. If it had a gameId, it's just for context of which game's jackpot button was pressed.
        } else {
            if (!params[0]) throw new Error(`Missing gameId for Dice Escalator action: ${action}.`);
            if (typeof handleDiceEscalatorPlayerAction === 'function') {
                await handleDiceEscalatorPlayerAction(params[0], userId, action, originalMessageId, chatId);
            } else {
                throw new Error("Dice Escalator action handler (handleDiceEscalatorPlayerAction) is not available.");
            }
        }
        break;
      case 'play_again_de':
        if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing or invalid bet amount for play_again_de.");
        const betAmountDE = parseInt(params[0], 10);
        // Attempt to remove buttons from the message that had the "Play Again"
        if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
        if (typeof handleStartDiceEscalatorCommand === 'function') {
            await handleStartDiceEscalatorCommand(chatId, userObjectForCallback, betAmountDE, null /* no original command msg id for play again */);
        } else {
            throw new Error("Dice Escalator start handler (handleStartDiceEscalatorCommand) is not available for Play Again.");
        }
        break;

      // --- Dice 21 Callbacks ---
      case 'd21_hit':
      case 'd21_stand':
        if (!params[0]) throw new Error(`Missing gameId for Dice 21 action: ${action}.`);
        const gameIdD21Action = params[0];
        if (action === 'd21_hit' && typeof handleDice21Hit === 'function') {
            await handleDice21Hit(gameIdD21Action, userObjectForCallback, originalMessageId);
        } else if (action === 'd21_stand' && typeof handleDice21Stand === 'function') {
            await handleDice21Stand(gameIdD21Action, userObjectForCallback, originalMessageId);
        } else {
            throw new Error(`Handler for ${action} is not available.`);
        }
        break;
      case 'play_again_d21':
        if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing or invalid bet amount for play_again_d21.");
        const betAmountD21 = parseInt(params[0], 10);
        if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
        if (typeof handleStartDice21Command === 'function') {
            await handleStartDice21Command(chatId, userObjectForCallback, betAmountD21, null);
        } else {
            throw new Error("Dice 21 start handler not available for Play Again.");
        }
        break;

      // --- Over/Under 7 Callbacks ---
      case 'ou7_choice':
        // params[0] = gameId, params[1] = choice ('over', 'under', 'seven')
        if (params.length < 2) throw new Error("Missing gameId or choice for ou7_choice.");
        if (typeof handleOverUnder7Choice === 'function') {
            await handleOverUnder7Choice(params[0], params[1], userObjectForCallback, originalMessageId);
        } else {
            throw new Error("Over/Under 7 choice handler not available.");
        }
        break;
      case 'play_again_ou7':
        if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing or invalid bet for play_again_ou7.");
        const betAmountOU7 = parseInt(params[0], 10);
        if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
        if (typeof handleStartOverUnder7Command === 'function') {
            await handleStartOverUnder7Command(chatId, userObjectForCallback, betAmountOU7, null);
        } else {
            throw new Error("Over/Under 7 start handler not available for Play Again.");
        }
        break;

      // --- Duel Callbacks ---
      case 'duel_roll':
        if (!params[0]) throw new Error("Missing gameId for duel_roll.");
        if (typeof handleDuelRoll === 'function') {
            await handleDuelRoll(params[0], userObjectForCallback, originalMessageId);
        } else {
            throw new Error("Duel roll handler not available.");
        }
        break;
      case 'play_again_duel':
        if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_duel.");
        const betAmountDuel = parseInt(params[0], 10);
        if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
        if (typeof handleStartDuelCommand === 'function') {
            await handleStartDuelCommand(chatId, userObjectForCallback, betAmountDuel, null);
        } else {
            throw new Error("Duel start handler N/A for Play Again.");
        }
        break;

      // --- Greed's Ladder Callbacks ---
      case 'play_again_ladder':
        if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_ladder.");
        const betAmountLadder = parseInt(params[0], 10);
        if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
        if (typeof handleStartLadderCommand === 'function') {
            await handleStartLadderCommand(chatId, userObjectForCallback, betAmountLadder, null);
        } else {
            throw new Error("Ladder start handler N/A for Play Again.");
        }
        break;

      // --- Sevens Out Callbacks ---
      case 's7_roll':
        if (!params[0]) throw new Error("Missing gameId for s7_roll.");
        if (typeof handleSevenOutRoll === 'function') {
            await handleSevenOutRoll(params[0], userObjectForCallback, originalMessageId);
        } else {
            throw new Error("Sevens Out roll handler N/A.");
        }
        break;
      case 'play_again_s7':
        if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_s7.");
        const betAmountS7 = parseInt(params[0], 10);
        if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
        if (typeof handleStartSevenOutCommand === 'function') {
            await handleStartSevenOutCommand(chatId, userObjectForCallback, betAmountS7, null);
        } else {
            throw new Error("Sevens Out start handler N/A for Play Again.");
        }
        break;

      // --- Slot Fruit Frenzy Callbacks ---
      case 'play_again_slot':
        if (!params[0] || isNaN(parseInt(params[0], 10))) throw new Error("Missing/invalid bet for play_again_slot.");
        const betAmountSlot = parseInt(params[0], 10);
        if (bot && originalMessageId) await bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(originalMessageId) }).catch(()=>{});
        if (typeof handleStartSlotCommand === 'function') {
            await handleStartSlotCommand(chatId, userObjectForCallback, betAmountSlot, null);
        } else {
            throw new Error("Slot start handler N/A for Play Again.");
        }
        break;

      default:
        console.log(`${LOG_PREFIX_CBQ} INFO: Unhandled callback action: "${action}" with params: [${params.join(', ')}]`);
        // Optionally provide feedback to the user for unhandled actions if they are not expected to be silent
        // await bot.answerCallbackQuery(callbackQueryId, { text: "Action not recognized.", show_alert: false });
    }
  } catch (error) {
    console.error(`${LOG_PREFIX_CBQ} CRITICAL ERROR processing callback action "${action}" for UserID ${userId} in ChatID ${chatId}: ${error.message}`, error.stack);
    // Send a generic error message to the user via a new message, as editing the original might not be appropriate
    // or the original message context is lost.
    await safeSendMessage(userId, "😕 Oops! Something went wrong while processing your action. Please try again or use a command.", {}).catch(() => {}); // Send to user's private chat
    // If the error is very specific and an alert on the button is better:
    // try { await bot.answerCallbackQuery(callbackQueryId, { text: "An error occurred. Try again.", show_alert: true }); } catch(e) {/* ignore */}
  }
}); // End of bot.on('callback_query')

// --- Command Handler Functions (General) ---

async function handleHelpCommand(chatId, userObj) {
  const userMention = getPlayerDisplayReference(userObj); // Use the enhanced display reference
  const jackpotScoreInfo = (typeof TARGET_JACKPOT_SCORE !== 'undefined' && !isNaN(TARGET_JACKPOT_SCORE)) ? TARGET_JACKPOT_SCORE : 'a high';

  // Enhanced help text with better formatting and more professional tone
  const helpTextParts = [
    `👋 Hello ${userMention}\\! Welcome to the **${escapeMarkdownV2(BOT_NAME || "Grand Casino Bot")} v${BOT_VERSION}**\\.`,
    `\nHere's a quick guide to our commands and games:`,
    `\n*Core Commands:*`,
    `▫️ \`/help\` \\- Shows this help message\\.`,
    `▫️ \`/balance\` or \`/bal\` \\- Check your current credit balance and access deposit/withdrawal options\\.`,
    `▫️ \`/rules\` or \`/info\` \\- View detailed rules for all our exciting games\\.`,
    `▫️ \`/jackpot\` \\- View the current Dice Escalator jackpot total\\.`,
    `\n*Available Games (Group Play Recommended):*`,
    `▫️ \`/coinflip <bet>\` \\- Classic coin toss against another player\\.`,
    `▫️ \`/rps <bet>\` \\- Rock Paper Scissors duel\\.`,
    `▫️ \`/diceescalator <bet>\` \\- Climb the score ladder against the Bot\\. Hit the Jackpot\\!`,
    `▫️ \`/dice21 <bet>\` \\(or \`/d21\`, \`/blackjack\`\\) \\- Dice version of Blackjack vs\\. the Bot\\.`,
    `▫️ \`/ou7 <bet>\` \\(or \`/overunder7\`\\) \\- Bet on the sum of two dice: Over 7, Under 7, or Exactly 7\\.`,
    `▫️ \`/duel <bet>\` \\(or \`/highroller\`\\) \\- High-stakes dice duel against the Bot\\.`,
    `▫️ \`/ladder <bet>\` \\(or \`/greedsladder\`\\) \\- Risk it all in Greed's Ladder with 3 dice rolls\\.`,
    `▫️ \`/sevenout <bet>\` \\(or \`/s7\`, \`/craps\`\\) \\- Simplified Craps\\-style dice game\\.`,
    `▫️ \`/slot <bet>\` \\(or \`/slots\`, \`/slotfrenzy\`\\) \\- Spin the Slot Machine for big wins\\!`,
    `\n*Betting:*`,
    `Bets range from *${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT, ''))}* to *${escapeMarkdownV2(formatCurrency(MAX_BET_AMOUNT))}* credits\\. Specify your bet after the game command, e\\.g\\., \`/d21 ${MIN_BET_AMOUNT}\`\\. If no bet is specified, it defaults to ${escapeMarkdownV2(formatCurrency(MIN_BET_AMOUNT))}\\.`,
    `\n*Dice Escalator Jackpot:*`,
    `🏆 Win by standing with a score of *${escapeMarkdownV2(String(jackpotScoreInfo))}\\+* AND beating the Bot Dealer in Dice Escalator\\!`,
    `\nRemember to play responsibly and have fun\\! 🎉`,
    `For any issues, please contact an administrator\\. (Bot Admin ID: ${ADMIN_USER_ID ? escapeMarkdownV2(ADMIN_USER_ID) : '_Not Set_'})`
  ];
  // BOT_NAME constant should be defined in Part 1 if you want a specific name, e.g. const BOT_NAME = "Your Bot Name";
  // Otherwise, it will use a generic term or be undefined.
  // For now, I'll assume BOT_NAME might be defined or add a fallback.
  const BOT_NAME = process.env.BOT_NAME || "The Casino Bot"; // Example of defining it if not already

  await safeSendMessage(chatId, helpTextParts.filter(Boolean).join('\n'), { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

async function handleBalanceCommand(chatId, userObj) {
  const LOG_PREFIX_BAL = `[BalanceCmd UID:${userObj.userId}]`;
  // console.log(`${LOG_PREFIX_BAL} Handling balance command.`);
  // User object is already fetched by the main message handler
  const userMention = getPlayerDisplayReference(userObj);

  // Ensure balanceLamports is available and is a BigInt for formatCurrency
  const currentBalanceLamports = BigInt(userObj.balanceLamports || 0n);
  const balanceMessage = `${userMention}, your current account balance is:\n💰 *${escapeMarkdownV2(formatCurrency(currentBalanceLamports, "credits"))}*`;
  // If you want to show lamports as well:
  // ` (${escapeMarkdownV2(formatCurrency(currentBalanceLamports, "lamports", true))})`

  const keyboard = {
    inline_keyboard: [
      [
        { text: "💰 Deposit Credits", callback_data: DEPOSIT_CALLBACK_ACTION },
        { text: "💸 Withdraw Credits", callback_data: WITHDRAW_CALLBACK_ACTION }
      ],
      // Optionally, add a button for transaction history if that feature exists/is planned
      // [ { text: "📜 Transaction History", callback_data: "history_action" } ]
    ]
  };

  await safeSendMessage(chatId, balanceMessage, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
}


async function handleJackpotCommand(chatId, userObj) {
    const LOG_PREFIX_JACKPOT = `[JackpotCmd UID:${userObj.userId}]`;
    // console.log(`${LOG_PREFIX_JACKPOT} Handling jackpot command.`);
    const userMention = getPlayerDisplayReference(userObj);

    try {
        if (typeof queryDatabase !== 'function' || typeof MAIN_JACKPOT_ID === 'undefined' || typeof TARGET_JACKPOT_SCORE === 'undefined' || typeof formatCurrency !== 'function' || typeof escapeMarkdownV2 !== 'function') {
            console.error(`${LOG_PREFIX_JACKPOT} CRITICAL: Missing dependencies for jackpot display.`);
            await safeSendMessage(chatId, "🎰 The jackpot display seems to be temporarily unavailable due to a configuration issue. Please try again later.", {});
            return;
        }
        const result = await queryDatabase('SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1', [MAIN_JACKPOT_ID]);
        let jackpotMessage;
        if (result.rows.length > 0) {
            const jackpotAmountLamports = BigInt(result.rows[0].current_amount_lamports);
            const jackpotDisplay = formatCurrency(jackpotAmountLamports, "credits"); // Display as credits
            jackpotMessage = `Hey ${userMention}!\n\nThe current Dice Escalator Super Jackpot is a whopping:\n💎 *${escapeMarkdownV2(jackpotDisplay)}* 💎\n\nTo win it, you need to achieve a score of *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}\\+* in Dice Escalator and successfully stand while also beating the Bot Dealer's score\\! Good luck\\! 🍀`;
        } else {
            console.warn(`${LOG_PREFIX_JACKPOT} No jackpot record found for ID: ${MAIN_JACKPOT_ID}. This is unusual.`);
            jackpotMessage = `${userMention}, the Dice Escalator Jackpot information is currently unavailable. It might be resetting or under maintenance.`;
        }
        await safeSendMessage(chatId, jackpotMessage, { parse_mode: 'MarkdownV2' });
    } catch (error) {
        console.error(`${LOG_PREFIX_JACKPOT} Error fetching jackpot: ${error.message}`, error.stack);
        await safeSendMessage(chatId, "😕 Sorry, there was an error fetching the current jackpot amount. Please try again in a moment.", {});
    }
}

// --- Generic Handler for Placeholder Messages (Deposit/Withdraw/Rules Intro) ---
async function handleGenericCallbackMessage(chatId, text, options = {}, originalMessageId = null, editOriginal = false) {
    if (editOriginal && originalMessageId && bot) {
        try {
            await bot.editMessageText(text, {
                chat_id: String(chatId),
                message_id: Number(originalMessageId),
                parse_mode: 'MarkdownV2', // Assume MarkdownV2 for these messages
                reply_markup: (options.reply_markup ? options.reply_markup : {}), // Clear buttons if not provided
                ...options // Spread other options like disable_web_page_preview
            });
        } catch (e) {
            // console.warn(`[GenericCallbackMsg] Failed to edit original message ${originalMessageId}, sending new: ${e.message}`);
            // Fallback to sending a new message if editing fails (e.g., message too old)
            await safeSendMessage(chatId, text, { parse_mode: 'MarkdownV2', ...options });
        }
    } else {
        await safeSendMessage(chatId, text, { parse_mode: 'MarkdownV2', ...options });
    }
}

console.log("Part 5a (Segment 1): Message & Callback Handling (Core Listeners, General Commands, UX Enhancements) - Complete.");
// --- End of Part 5a (Segment 1 of 2) ---
// --- Start of Part 5a (Segment 2 of 2) ---
// index.js - Part 5a: Rules Implementation, Coinflip/RPS Enhancements, Game Handler Stubs
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 2): Rules Implementation, Coinflip/RPS Enhancements, Game Handler Stubs...");

// --- Rules Command Handler Functions ---

async function handleRulesCommand(chatId, userObj) {
  const LOG_PREFIX_RULES = `[RulesCmd UID:${userObj.userId}]`;
  // console.log(`${LOG_PREFIX_RULES} Handling /rules command.`);
  const userMention = getPlayerDisplayReference(userObj);

  const rulesIntroText = `${userMention}, welcome to the Casino Knowledge Base! 📚\n\nSelect a game from the options below to learn its rules, payouts, and how to play:`;

  // Dynamically generate buttons based on GAME_IDS
  const gameRuleButtons = Object.entries(GAME_IDS).map(([key, gameCode]) => {
    // Format game names for buttons (e.g., DICE_ESCALATOR -> Dice Escalator)
    const gameName = key.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
    let emoji = '❓'; // Default emoji
    if (gameCode === GAME_IDS.COINFLIP) emoji = '🪙';
    else if (gameCode === GAME_IDS.RPS) emoji = '✂️';
    else if (gameCode === GAME_IDS.DICE_ESCALATOR) emoji = '🎲';
    else if (gameCode === GAME_IDS.DICE_21) emoji = '🃏';
    else if (gameCode === GAME_IDS.OVER_UNDER_7) emoji = '🎲';
    else if (gameCode === GAME_IDS.DUEL) emoji = '⚔️';
    else if (gameCode === GAME_IDS.LADDER) emoji = '🪜';
    else if (gameCode === GAME_IDS.SEVEN_OUT) emoji = '🎲';
    else if (gameCode === GAME_IDS.SLOT_FRENZY) emoji = '🎰';

    return { text: `${emoji} ${gameName}`, callback_data: `${RULES_CALLBACK_PREFIX}${gameCode}` };
  });

  // Structure buttons into rows of 2 for better layout
  const rows = [];
  for (let i = 0; i < gameRuleButtons.length; i += 2) {
    rows.push(gameRuleButtons.slice(i, i + 2));
  }

  const keyboard = { inline_keyboard: rows };
  await safeSendMessage(chatId, rulesIntroText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
}

async function handleDisplayGameRules(chatId, originalMessageId, gameCode, userObj) {
  const LOG_PREFIX_RULES_DISP = `[RulesDisplay UID:${userObj.userId} Game:${gameCode}]`;
  // console.log(`${LOG_PREFIX_RULES_DISP} Displaying rules.`);
  let gameName = "Selected Game";
  let rulesText = `📜 **Rules for ${escapeMarkdownV2(gameName)}** 📜\n\n`;

  // Find game name from GAME_IDS for better display
  for (const [key, val] of Object.entries(GAME_IDS)) {
    if (val === gameCode) {
      gameName = key.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
      rulesText = `📜 **Rules for ${escapeMarkdownV2(gameName)}** 📜\n\n`;
      break;
    }
  }

  switch (gameCode) {
    case GAME_IDS.COINFLIP:
      rulesText += `🪙 *${escapeMarkdownV2(gameName)}*\n\n`;
      rulesText += `*Objective:* Correctly guess the outcome of a coin flip\\.\n\n`;
      rulesText += `*How to Play:*\n`;
      rulesText += `1\\. Use \`/coinflip <bet>\` to start a game in a group chat\\.\n`;
      rulesText += `2\\. Another player must join your game by clicking the "Join Coinflip" button\\.\n`;
      rulesText += `3\\. Once an opponent joins, the coin is flipped automatically\\. The initiator is assigned 'Heads', the joiner 'Tails'\\.\n`;
      rulesText += `4\\. The player whose side matches the coin flip wins the pot (their bet \\+ opponent's bet)\\.\n\n`;
      rulesText += `*Payout:* Winner takes all (1:1 profit on their bet)\\.`;
      break;
    case GAME_IDS.RPS:
      rulesText += `🪨📄✂️ *${escapeMarkdownV2(gameName)} (Rock Paper Scissors)*\n\n`;
      rulesText += `*Objective:* Choose an option that beats your opponent's choice based on standard RPS rules\\.\n\n`;
      rulesText += `*How to Play:*\n`;
      rulesText += `1\\. Use \`/rps <bet>\` to start a game in a group chat\\.\n`;
      rulesText += `2\\. Another player must join your game\\.\n`;
      rulesText += `3\\. Both players will then be prompted to secretly choose Rock, Paper, or Scissors via inline buttons\\.\n`;
      rulesText += `4\\. Choices are revealed simultaneously\\. Wins are determined as follows:\n`;
      rulesText += `   \\- Rock crushes Scissors\n`;
      rulesText += `   \\- Paper covers Rock\n`;
      rulesText += `   \\- Scissors cuts Paper\n\n`;
      rulesText += `*Outcome:*\n`;
      rulesText += `▫️ *Win:* Winner takes the pot (1:1 profit on their bet)\\.\n`;
      rulesText += `▫️ *Draw:* If both players choose the same, bets are refunded\\.`;
      break;
    case GAME_IDS.DICE_ESCALATOR:
      rulesText += `🎲 *${escapeMarkdownV2(gameName)}*\n\n`;
      rulesText += `*Objective:* Accumulate a higher score than the Bot Dealer without busting, or have the Bot Dealer bust\\.\n\n`;
      rulesText += `*How to Play:*\n`;
      rulesText += `1\\. Use \`/diceescalator <bet>\` (or \`/startdice\`) to begin a game against the Bot Dealer\\.\n`;
      rulesText += `2\\. Press "Roll Dice" to roll a standard 6\\-sided die\\. The result is added to your score\\.\n`;
      rulesText += `3\\. *Bust:* If you roll a *${escapeMarkdownV2(String(DICE_ESCALATOR_BUST_ON))}*, your score becomes 0, and you lose your bet immediately\\.\n`;
      rulesText += `4\\. *Roll Again or Stand:* If you don't bust, you can choose to "Roll Again" to increase your score or "Stand" to keep your current score and end your turn\\.\n`;
      rulesText += `5\\. *Bot's Turn:* If you stand, the Bot Dealer will roll dice until its score is *${escapeMarkdownV2(String(BOT_STAND_SCORE_DICE_ESCALATOR))}* or higher, or until it busts by rolling a *${escapeMarkdownV2(String(DICE_ESCALATOR_BUST_ON))}*\\.\n\n`;
      rulesText += `*Winning & Payouts:*\n`;
      rulesText += `▫️ You win (1:1 profit) if: Your score is higher than the Bot's, OR the Bot busts\\.\n`;
      rulesText += `▫️ You lose your bet if: The Bot's score is higher than yours (and Bot didn't bust), OR you bust\\.\n`;
      rulesText += `▫️ *Push (Tie):* If your score equals the Bot's score (and neither busted), your bet is refunded\\.\n\n`;
      rulesText += `🏆 *Jackpot:* Win the current Jackpot if you stand with a score of *${escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))}\\+* AND win the round against the Bot\\!`;
      break;
    case GAME_IDS.DICE_21:
      rulesText += `🃏 *${escapeMarkdownV2(gameName)} (Blackjack Style)*\n\n`;
      rulesText += `*Objective:* Get a score as close to *${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}* as possible with two 6\\-sided dice per "hit", without going over\\. Beat the Bot Dealer's score\\.\n\n`;
      rulesText += `*How to Play:*\n`;
      rulesText += `1\\. Use \`/dice21 <bet>\` (or \`/d21\`) to start\\. You are dealt two initial dice rolls\\.\n`;
      rulesText += `2\\. *Hit:* Request another die roll to increase your score\\. You can hit multiple times\\.\n`;
      rulesText += `3\\. *Stand:* Keep your current score and end your turn\\.\n`;
      rulesText += `4\\. *Bust:* If your score exceeds *${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}*, you bust and lose your bet immediately\\.\n`;
      rulesText += `5\\. *Blackjack:* If your initial two dice total *${escapeMarkdownV2(String(DICE_21_TARGET_SCORE))}*, you have Blackjack\\! You automatically stand\\. Standard Blackjack usually pays 3:2, but here it means you stand strong\\.\n`; // Clarify if Blackjack has special payout
      rulesText += `6\\. *Bot's Turn:* After you stand (or get Blackjack), the Bot Dealer plays, aiming to reach at least *${escapeMarkdownV2(String(DICE_21_BOT_STAND_SCORE))}* without busting\\.\n\n`;
      rulesText += `*Winning & Payouts:*\n`;
      rulesText += `▫️ You win (1:1 profit) if: Your score is higher than the Bot's (and neither busted), OR the Bot busts and you did not\\.\n`;
      rulesText += `▫️ You lose if: You bust, OR the Bot's score is higher than yours (and Bot didn't bust)\\.\n`;
      rulesText += `▫️ *Push (Tie):* If scores are equal (and no busts), your bet is refunded\\.`;
      break;
    case GAME_IDS.OVER_UNDER_7:
      rulesText += `🎲 *${escapeMarkdownV2(gameName)}*\n\n`;
      rulesText += `*Objective:* Predict the sum of two 6\\-sided dice\\.\n\n`;
      rulesText += `*How to Play:*\n`;
      rulesText += `1\\. Use \`/ou7 <bet>\` to start\\. You'll be prompted to choose your prediction:\n`;
      rulesText += `   \\- *Under 7:* The sum will be 2, 3, 4, 5, or 6\\.\n`;
      rulesText += `   \\- *Exactly 7:* The sum will be exactly 7\\.\n`;
      rulesText += `   \\- *Over 7:* The sum will be 8, 9, 10, 11, or 12\\.\n`;
      rulesText += `2\\. Two dice are rolled, and the sum is revealed\\.\n\n`;
      rulesText += `*Payouts (Profit on Bet):*\n`;
      rulesText += `▫️ *Under 7 or Over 7:* Win *${escapeMarkdownV2(String(OU7_PAYOUT_NORMAL))}:1* (e\\.g\\., bet 10, win 10 profit, get 20 back)\\.\n`;
      rulesText += `▫️ *Exactly 7:* Win *${escapeMarkdownV2(String(OU7_PAYOUT_SEVEN))}:1* (e\\.g\\., bet 10, win 40 profit, get 50 back)\\.\n`;
      rulesText += `▫️ If your prediction is incorrect, you lose your bet\\.`;
      break;
    // Add stubs for other games, to be filled in detail later
    case GAME_IDS.DUEL:
        rulesText += `⚔️ *${escapeMarkdownV2(gameName)} (High Roller Duel)*\n\n`;
        rulesText += `*Objective:* Roll a higher total sum with *${escapeMarkdownV2(String(DUEL_DICE_COUNT))}* dice than the Bot Dealer\\.\n\n`;
        rulesText += `*How to Play:*\n`;
        rulesText += `1\\. Use \`/duel <bet>\` to start\\. \n`;
        rulesText += `2\\. You will be prompted to roll your ${escapeMarkdownV2(String(DUEL_DICE_COUNT))} dice\\. Their sum is your score\\.\n`;
        rulesText += `3\\. The Bot Dealer then rolls ${escapeMarkdownV2(String(DUEL_DICE_COUNT))} dice\\. Their sum is the Bot's score\\.\n\n`;
        rulesText += `*Winning & Payouts:*\n`;
        rulesText += `▫️ *Win (1:1 profit):* If your total score is higher than the Bot's\\.\n`;
        rulesText += `▫️ *Lose:* If the Bot's total score is higher than yours\\.\n`;
        rulesText += `▫️ *Push (Tie):* If scores are equal, your bet is refunded\\.`;
        break;
    case GAME_IDS.LADDER:
        rulesText += `🪜 *${escapeMarkdownV2(gameName)}*\n\n`;
        rulesText += `*Objective:* Achieve a high total score from *${escapeMarkdownV2(String(LADDER_ROLL_COUNT))}* dice rolls without rolling a *${escapeMarkdownV2(String(LADDER_BUST_ON))}*\\.\n\n`;
        rulesText += `*How to Play:*\n`;
        rulesText += `1\\. Use \`/ladder <bet>\` to start\\. The bot will automatically roll ${escapeMarkdownV2(String(LADDER_ROLL_COUNT))} dice for you one by one\\.\n`;
        rulesText += `2\\. *Bust:* If any roll is a *${escapeMarkdownV2(String(LADDER_BUST_ON))}*, you bust and lose your bet immediately\\.\n`;
        rulesText += `3\\. *Score & Payout:* If you don't bust, the sum of your ${escapeMarkdownV2(String(LADDER_ROLL_COUNT))} rolls determines your payout based on score tiers\\. Higher scores yield better payouts\\.\n\n`;
        rulesText += `*Example Payout Tiers (for ${escapeMarkdownV2(String(LADDER_ROLL_COUNT))} rolls, specific tiers vary):*\n`;
        LADDER_PAYOUTS.forEach(tier => {
            rulesText += `▫️ Score *${escapeMarkdownV2(String(tier.min))}\\-${escapeMarkdownV2(String(tier.max))}*: ${escapeMarkdownV2(tier.label)} (Profit: ${tier.multiplier === -1 ? 'Lose Bet' : (tier.multiplier === 0 ? 'Push' : `${tier.multiplier}x Bet` ) })\\.\n`;
        });
        break;
    case GAME_IDS.SEVEN_OUT:
        rulesText += `🎲 *${escapeMarkdownV2(gameName)} (Craps Style)*\n\n`;
        rulesText += `*Objective:* Based on the outcome of rolling two 6\\-sided dice over one or more rounds\\.\n\n`;
        rulesText += `*How to Play:*\n`;
        rulesText += `1\\. *Come Out Roll:* Use \`/sevenout <bet>\` to start\\. The first roll is the "Come Out" roll\\.\n`;
        rulesText += `   \\- *Natural Win (7 or 11):* You win immediately (1:1 profit)\\.\n`;
        rulesText += `   \\- *Craps (2, 3, or 12):* You lose immediately\\.\n`;
        rulesText += `   \\- *Point Established (4, 5, 6, 8, 9, or 10):* This sum becomes your "Point"\\.\n`;
        rulesText += `2\\. *Point Phase (if a Point is established):*\n`;
        rulesText += `   \\- You continue rolling the dice by clicking "Roll for Point"\\.\n`;
        rulesText += `   \\- *Win:* If you roll your Point number again before rolling a 7, you win (1:1 profit)\\.\n`;
        rulesText += `   \\- *Seven Out (Lose):* If you roll a 7 before hitting your Point, you lose your bet\\.\n`;
        rulesText += `   \\- Other rolls: You continue rolling until you hit your Point or a 7\\.\n\n`;
        rulesText += `*Payout:* All wins are typically 1:1 profit on your bet\\.`;
        break;
    case GAME_IDS.SLOT_FRENZY:
        rulesText += `🎰 *${escapeMarkdownV2(gameName)}*\n\n`;
        rulesText += `*Objective:* Match symbols on a virtual slot machine to win payouts based on your bet\\.\n\n`;
        rulesText += `*How to Play:*\n`;
        rulesText += `1\\. Use \`/slot <bet>\` to spin the reels\\.\n`;
        rulesText += `2\\. The bot will send an animated slot machine emoji (🎰)\\. The outcome (a value from 1 to 64) determines your win or loss based on a predefined paytable\\.\n\n`;
        rulesText += `*Payouts (Example - Multipliers are for PROFIT on bet):*\n`;
        for(const val in SLOT_PAYOUTS){
            rulesText += `▫️ *${escapeMarkdownV2(SLOT_PAYOUTS[val].symbols)}* \\(${escapeMarkdownV2(SLOT_PAYOUTS[val].label)}\\): Wins *${escapeMarkdownV2(String(SLOT_PAYOUTS[val].multiplier))}x* your bet\\.\n`;
        }
        rulesText += `▫️ Other combinations result in a loss of the bet amount\\.\n\n`;
        rulesText += `*Note:* This is a simplified slot simulation\\. Enjoy the thrill\\!`;
        break;
    default:
      rulesText += `Rules for "${escapeMarkdownV2(gameCode)}" are not yet documented or this is an invalid game code\\. Please check back later or select another game\\.`;
  }

  rulesText += `\n\nGood luck, and may fortune favor you\\! ✨`;

  const backButton = { text: "↩️ Back to Rules Menu", callback_data: "show_rules_menu" };
  // If we are editing a message, it's good to offer a way back.
  // If originalMessageId is not null, it implies this is a callback from the rules menu.
  const keyboard = originalMessageId ? { inline_keyboard: [[backButton]] } : {};


  if (originalMessageId && bot) {
      // Edit the existing message (which was the rules menu)
      try {
        await bot.editMessageText(rulesText, {
            chat_id: chatId,
            message_id: Number(originalMessageId),
            parse_mode: 'MarkdownV2',
            reply_markup: keyboard, // Show back button
            disable_web_page_preview: true
        });
      } catch (e) {
        // console.warn(`${LOG_PREFIX_RULES_DISP} Failed to edit rules message, sending new: ${e.message}`);
        await safeSendMessage(chatId, rulesText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
      }
  } else {
      // Send as a new message (e.g., if /rules <game_code> was a direct command, though not implemented this way currently)
      await safeSendMessage(chatId, rulesText, { parse_mode: 'MarkdownV2', reply_markup: keyboard, disable_web_page_preview: true });
  }
}


// --- Existing Group Game Flow Functions (Coinflip, RPS - ENHANCED UX) ---

// Helper to create common post-game keyboard
function createPostGameKeyboard(gameCode, betAmount, newGameId = null) {
    // newGameId is optional, could be used if "Play Again" starts the *exact same game instance ID*
    // For most casino games, Play Again means a new game instance of the same type/bet.
    let playAgainCallback = `play_again_${gameCode}:${betAmount}`;
    if (newGameId) playAgainCallback += `:${newGameId}`;

    return {
        inline_keyboard: [
            [{ text: `🔁 Play Again (${formatCurrency(Number(betAmount))})`, callback_data: playAgainCallback }],
            [{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }
             // { text: "📜 Rules", callback_data: `${RULES_CALLBACK_PREFIX}${gameCode}` } // Optional: Rules button
            ]
            // [{ text: "↩️ Main Menu", callback_data: "main_menu" }] // Optional: Main menu button
        ]
    };
}


async function handleStartGroupCoinFlipCommand(chatId, initiatorUserObj, betAmount, commandMessageId) {
  const LOG_PREFIX_CF_START = `[Coinflip_Start UID:${initiatorUserObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_CF_START} Initiating Coinflip. Bet: ${betAmount}`);

  const initiatorId = String(initiatorUserObj.userId);
  const initiatorMention = getPlayerDisplayReference(initiatorUserObj); // Enhanced display

  let chatInfo = null;
  try {
    if (bot && typeof bot.getChat === 'function') chatInfo = await bot.getChat(chatId);
  } catch (e) { console.warn(`${LOG_PREFIX_CF_START} Could not fetch chat info for ${chatId}: ${e.message}`); }
  const chatTitle = chatInfo?.title;

  const gameSession = await getGroupSession(chatId, chatTitle || `Group Chat ${chatId}`);
  if (gameSession.currentGameId && !['DiceEscalator', 'Dice21', 'OverUnder7', 'Duel', 'Ladder', 'SevenOut', 'Slot'].includes(gameSession.currentGameType) ) {
    await safeSendMessage(chatId, `⏳ A game of \`${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}\` is already active in this chat\\. Please wait for it to conclude\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }

  if (initiatorUserObj.balanceLamports < BigInt(betAmount)) {
    const needed = formatCurrency(Number(betAmount) - initiatorUserObj.balance, "credits");
    await safeSendMessage(chatId, `${initiatorMention}, your balance of ${escapeMarkdownV2(formatCurrency(initiatorUserObj.balance, "credits"))} is too low for a ${escapeMarkdownV2(formatCurrency(betAmount, "credits"))} Coinflip bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const gameId = generateGameId(GAME_IDS.COINFLIP);
  const balanceUpdateResult = await updateUserBalance(initiatorId, -BigInt(betAmount), `bet_placed_group_coinflip_init:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult.success) {
    await safeSendMessage(chatId, `${initiatorMention}, your Coinflip wager of ${escapeMarkdownV2(formatCurrency(betAmount))} failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }
  // console.log(`${LOG_PREFIX_CF_START} Initiator's bet of ${betAmount} placed for game ${gameId}. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  const gameDataCF = {
    type: GAME_IDS.COINFLIP, gameId, chatId: String(chatId), initiatorId,
    initiatorMention: initiatorMention, // Store the formatted mention
    betAmount: BigInt(betAmount),
    participants: [{ userId: initiatorId, choice: null, mention: initiatorMention, betPlaced: true, userObj: initiatorUserObj }],
    status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
  };
  activeGames.set(gameId, gameDataCF);
  await updateGroupGameDetails(chatId, gameId, GAME_IDS.COINFLIP, betAmount); // Update group session

  const joinMsgCF = `🪙 *Coinflip Challenge!* 🪙\n\n${initiatorMention} has started a Coinflip game for *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}*\\!\n\nWho will accept the challenge\\?`;
  const kbCF = {
    inline_keyboard: [
      [{ text: "🪙 Join Coinflip!", callback_data: `join_game:${gameId}` }],
      [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]
    ]
  };
  const setupMsgCF = await safeSendMessage(chatId, joinMsgCF, { parse_mode: 'MarkdownV2', reply_markup: kbCF });

  if (setupMsgCF && activeGames.has(gameId)) {
    activeGames.get(gameId).gameSetupMessageId = setupMsgCF.message_id;
  } else {
    console.error(`${LOG_PREFIX_CF_START} Failed to send Coinflip setup message for game ${gameId} or game was removed.`);
    await updateUserBalance(initiatorId, BigInt(betAmount), `refund_coinflip_setup_fail:${gameId}`, null, gameId, String(chatId)); // Refund
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null); // Clear from group session
    return;
  }

  // Timeout for game waiting for opponent
  setTimeout(async () => {
    const gdCF_timeout = activeGames.get(gameId);
    if (gdCF_timeout && gdCF_timeout.status === 'waiting_opponent') {
      // console.log(`[Coinflip_Timeout GID:${gameId}] Game expired waiting for opponent.`);
      await updateUserBalance(gdCF_timeout.initiatorId, gdCF_timeout.betAmount, `refund_coinflip_timeout:${gameId}`, null, gameId, String(chatId));
      activeGames.delete(gameId);
      await updateGroupGameDetails(chatId, null, null, null);

      const timeoutMsgTextCF = `🪙 Coinflip game by ${gdCF_timeout.initiatorMention} (Bet: ${escapeMarkdownV2(formatCurrency(Number(gdCF_timeout.betAmount)))}) has expired due to no opponent joining\\. The bet has been refunded\\.`;
      if (gdCF_timeout.gameSetupMessageId && bot) {
        bot.editMessageText(timeoutMsgTextCF, { chatId: String(chatId), message_id: Number(gdCF_timeout.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
          .catch(() => { safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' }); });
      } else {
        safeSendMessage(chatId, timeoutMsgTextCF, { parse_mode: 'MarkdownV2' });
      }
    }
  }, JOIN_GAME_TIMEOUT_MS);
}


async function handleStartGroupRPSCommand(chatId, initiatorUserObj, betAmount, commandMessageId) {
  const LOG_PREFIX_RPS_START = `[RPS_Start UID:${initiatorUserObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_RPS_START} Initiating RPS. Bet: ${betAmount}`);

  const initiatorId = String(initiatorUserObj.userId);
  const initiatorMention = getPlayerDisplayReference(initiatorUserObj);

  let chatInfo = null;
  try { if (bot) chatInfo = await bot.getChat(chatId); } catch (e) { /* ignore */ }
  const chatTitle = chatInfo?.title;

  const gameSession = await getGroupSession(chatId, chatTitle || `Group Chat ${chatId}`);
   if (gameSession.currentGameId && !['DiceEscalator', 'Dice21', 'OverUnder7', 'Duel', 'Ladder', 'SevenOut', 'Slot'].includes(gameSession.currentGameType) ) {
    await safeSendMessage(chatId, `⏳ A game of \`${escapeMarkdownV2(gameSession.currentGameType || 'Unknown Game')}\` is already active in this chat\\. Please wait\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }

  if (initiatorUserObj.balanceLamports < BigInt(betAmount)) {
     const needed = formatCurrency(Number(betAmount) - initiatorUserObj.balance, "credits");
    await safeSendMessage(chatId, `${initiatorMention}, your balance of ${escapeMarkdownV2(formatCurrency(initiatorUserObj.balance, "credits"))} is too low for a ${escapeMarkdownV2(formatCurrency(betAmount, "credits"))} RPS bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const gameId = generateGameId(GAME_IDS.RPS);
  const balanceUpdateResult = await updateUserBalance(initiatorId, -BigInt(betAmount), `bet_placed_group_rps_init:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult.success) {
    await safeSendMessage(chatId, `${initiatorMention}, your RPS wager of ${escapeMarkdownV2(formatCurrency(betAmount))} failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }

  const gameDataRPS = {
    type: GAME_IDS.RPS, gameId, chatId: String(chatId), initiatorId,
    initiatorMention: initiatorMention,
    betAmount: BigInt(betAmount),
    participants: [{ userId: initiatorId, choice: null, mention: initiatorMention, betPlaced: true, userObj: initiatorUserObj }],
    status: 'waiting_opponent', creationTime: Date.now(), commandMessageId, gameSetupMessageId: null
  };
  activeGames.set(gameId, gameDataRPS);
  await updateGroupGameDetails(chatId, gameId, GAME_IDS.RPS, betAmount);

  const joinMsgRPS = `🪨📄✂️ *Rock Paper Scissors Battle!* 🪨📄✂️\n\n${initiatorMention} has laid down the gauntlet for *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}*\\!\n\nAre you brave enough to face them\\?`;
  const kbRPS = {
    inline_keyboard: [
      [{ text: "✨ Join RPS Battle!", callback_data: `join_game:${gameId}` }],
      [{ text: "❌ Cancel Game", callback_data: `cancel_game:${gameId}` }]
    ]
  };
  const setupMsgRPS = await safeSendMessage(chatId, joinMsgRPS, { parse_mode: 'MarkdownV2', reply_markup: kbRPS });

  if (setupMsgRPS && activeGames.has(gameId)) {
    activeGames.get(gameId).gameSetupMessageId = setupMsgRPS.message_id;
  } else {
    console.error(`${LOG_PREFIX_RPS_START} Failed to send RPS setup message for game ${gameId} or game was removed.`);
    await updateUserBalance(initiatorId, BigInt(betAmount), `refund_rps_setup_fail:${gameId}`, null, gameId, String(chatId));
    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);
    return;
  }

  setTimeout(async () => {
    const gdRPS_timeout = activeGames.get(gameId);
    if (gdRPS_timeout && gdRPS_timeout.status === 'waiting_opponent') {
      // console.log(`[RPS_Timeout GID:${gameId}] Game expired waiting for opponent.`);
      await updateUserBalance(gdRPS_timeout.initiatorId, gdRPS_timeout.betAmount, `refund_rps_timeout:${gameId}`, null, gameId, String(chatId));
      activeGames.delete(gameId);
      await updateGroupGameDetails(chatId, null, null, null);

      const timeoutMsgTextRPS = `🪨📄✂️ RPS game by ${gdRPS_timeout.initiatorMention} (Bet: ${escapeMarkdownV2(formatCurrency(Number(gdRPS_timeout.betAmount)))}) has expired\\. No challenger appeared\\. Bet refunded\\.`;
      if (gdRPS_timeout.gameSetupMessageId && bot) {
        bot.editMessageText(timeoutMsgTextRPS, { chatId: String(chatId), message_id: Number(gdRPS_timeout.gameSetupMessageId), parse_mode: 'MarkdownV2', reply_markup: {} })
          .catch(() => { safeSendMessage(chatId, timeoutMsgTextRPS, { parse_mode: 'MarkdownV2' }); });
      } else {
        safeSendMessage(chatId, timeoutMsgTextRPS, { parse_mode: 'MarkdownV2' });
      }
    }
  }, JOIN_GAME_TIMEOUT_MS);
}


// --- Stubs for other game start handlers (to be fully implemented in later segments) ---
async function handleStartDiceEscalatorCommand(chatId, userObj, betAmount, commandMessageId) {
    await safeSendMessage(chatId, "🎲 Dice Escalator handler is under construction in Part 5b!", {});
}
async function handleStartDice21Command(chatId, userObj, betAmount, commandMessageId) {
    await safeSendMessage(chatId, "🃏 Dice 21 handler is under construction!", {});
}
async function handleStartOverUnder7Command(chatId, userObj, betAmount, commandMessageId) {
    await safeSendMessage(chatId, "🎲 Over/Under 7 handler is under construction!", {});
}
async function handleStartDuelCommand(chatId, userObj, betAmount, commandMessageId) {
    await safeSendMessage(chatId, "⚔️ Duel handler is under construction!", {});
}
async function handleStartLadderCommand(chatId, userObj, betAmount, commandMessageId) {
    await safeSendMessage(chatId, "🪜 Ladder handler is under construction!", {});
}
async function handleStartSevenOutCommand(chatId, userObj, betAmount, commandMessageId) {
    await safeSendMessage(chatId, "🎲 Sevens Out handler is under construction!", {});
}
async function handleStartSlotCommand(chatId, userObj, betAmount, commandMessageId) {
    await safeSendMessage(chatId, "🎰 Slot Frenzy handler is under construction!", {});
}


console.log("Part 5a (Segment 2): Rules Implementation, Coinflip/RPS Enhancements, Game Handler Stubs - Complete.");
// --- End of Part 5a (Segment 2 of 2) ---
// --- Start of Part 5a (Segment 3 of N) ---
// index.js - Part 5a: Coinflip & RPS Callback Handlers, Post-Game UX
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 3): Coinflip & RPS Callback Handlers, Post-Game UX...");

// --- Coinflip & RPS Callback Handlers (Group Games) ---

async function handleJoinGameCallback(chatId, joinerUserObj, gameId, interactionMessageId) {
  const LOG_PREFIX_JOIN = `[JoinGame_CB UID:${joinerUserObj.userId} GID:${gameId}]`;
  // console.log(`${LOG_PREFIX_JOIN} Attempting to join game.`);

  const gameData = activeGames.get(gameId);

  if (!gameData) {
    // console.warn(`${LOG_PREFIX_JOIN} Game not found or already concluded.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "This game is no longer available.", show_alert: true });
    // Attempt to clear buttons on the message clicked, if it still exists
    if (interactionMessageId && bot) {
        bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
    }
    return;
  }

  if (gameData.chatId !== String(chatId)) {
    // console.warn(`${LOG_PREFIX_JOIN} Chat ID mismatch. Game is in ${gameData.chatId}.`);
    // This should not happen if Telegram works as expected with message-bound callbacks.
    await bot.answerCallbackQuery(callbackQueryId, { text: "Error: Game context mismatch.", show_alert: true });
    return;
  }

  if (gameData.initiatorId === String(joinerUserObj.userId)) {
    // console.log(`${LOG_PREFIX_JOIN} Initiator cannot join their own game as opponent.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "You can't join your own game as an opponent!", show_alert: false });
    return;
  }

  if (gameData.status !== 'waiting_opponent') {
    // console.log(`${LOG_PREFIX_JOIN} Game is not in 'waiting_opponent' status. Current status: ${gameData.status}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "This game is not currently accepting new players.", show_alert: true });
    if (interactionMessageId && bot) { // Clear buttons if game is full or started
        bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
    }
    return;
  }

  // Check if game is already full (should be max 2 participants for these games)
  if (gameData.participants.length >= 2 && (gameData.type === GAME_IDS.COINFLIP || gameData.type === GAME_IDS.RPS)) {
    // console.log(`${LOG_PREFIX_JOIN} Game is full.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Sorry, this game is already full!", show_alert: true });
    if (interactionMessageId && bot) {
        bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
    }
    return;
  }

  if (joinerUserObj.balanceLamports < gameData.betAmount) {
    // console.log(`${LOG_PREFIX_JOIN} Joiner has insufficient balance.`);
    const needed = formatCurrency(Number(gameData.betAmount) - joinerUserObj.balance, "credits");
    await bot.answerCallbackQuery(callbackQueryId, { text: `Your balance is too low. You need ${needed} more.`, show_alert: true });
    // Send a follow-up message with deposit option
    await safeSendMessage(chatId, `${getPlayerDisplayReference(joinerUserObj)}, you need ${escapeMarkdownV2(needed)} more credits to join this ${escapeMarkdownV2(gameData.type)} game for ${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"))}\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const joinerId = String(joinerUserObj.userId);
  const joinerMention = getPlayerDisplayReference(joinerUserObj);
  const balanceUpdateResult = await updateUserBalance(joinerId, -BigInt(gameData.betAmount), `bet_placed_group_${gameData.type}_join:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult.success) {
    console.error(`${LOG_PREFIX_JOIN} Bet placement failed for joiner: ${balanceUpdateResult.error}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: `Wager failed: ${balanceUpdateResult.error || "Unknown issue"}.`, show_alert: true });
    return;
  }
  // console.log(`${LOG_PREFIX_JOIN} Joiner's bet of ${gameData.betAmount} placed. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  gameData.participants.push({ userId: joinerId, choice: null, mention: joinerMention, betPlaced: true, userObj: joinerUserObj });
  // No need to activeGames.set(gameId, gameData) yet, will be set after status change.

  const messageToEditId = Number(interactionMessageId || gameData.gameSetupMessageId);
  const betDisplay = escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"));

  if (gameData.type === GAME_IDS.COINFLIP && gameData.participants.length === 2) {
    gameData.status = 'resolving';
    activeGames.set(gameId, gameData); // Update game state before async operations

    const p1 = gameData.participants[0]; // Initiator
    const p2 = gameData.participants[1]; // Joiner

    // Assign sides: Initiator is Heads, Joiner is Tails (example assignment)
    p1.choice = 'heads';
    p2.choice = 'tails';

    const cfResult = determineCoinFlipOutcome(); // From Part 4
    let winnerParticipant = (cfResult.outcome === p1.choice) ? p1 : p2;
    let loserParticipant = (winnerParticipant === p1) ? p2 : p1;

    const winningsToCredit = gameData.betAmount + gameData.betAmount; // Total pot

    const winnerUpdateResult = await updateUserBalance(winnerParticipant.userId, winningsToCredit, `won_group_coinflip:${gameId}`, null, gameId, String(chatId));
    // Loser's bet is already deducted. Log the loss transaction.
    await updateUserBalance(loserParticipant.userId, 0n, `lost_group_coinflip:${gameId}`, null, gameId, String(chatId));


    let resMsg = `🪙 *Coinflip Resolved!* 🪙\nBet Amount: *${betDisplay}*\n\n`;
    resMsg += `${p1.mention} chose *Heads*\n`;
    resMsg += `${p2.mention} chose *Tails*\n\n`;
    resMsg += `The coin spins through the air and lands on... **${escapeMarkdownV2(cfResult.outcomeString)}** ${cfResult.emoji}\\!\n\n`;
    resMsg += `🎉 ${winnerParticipant.mention} wins the pot of *${escapeMarkdownV2(formatCurrency(Number(winningsToCredit - gameData.betAmount), "credits"))}* profit\\! 🎉`;

    if (winnerUpdateResult.success) {
        resMsg += `\n\n${winnerParticipant.mention}'s new balance: *${escapeMarkdownV2(formatCurrency(Number(winnerUpdateResult.newBalanceLamports), "credits"))}*\\.`;
    } else {
        resMsg += `\n\n⚠️ There was an issue crediting ${winnerParticipant.mention}'s winnings\\. Admin has been notified\\.`;
        console.error(`${LOG_PREFIX_JOIN} CRITICAL: Failed to credit Coinflip winner ${winnerParticipant.userId} for game ${gameId}.`);
    }

    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.COINFLIP, Number(gameData.betAmount));
    if (messageToEditId && bot) {
      bot.editMessageText(resMsg, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard })
        .catch(() => { safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard }); });
    } else {
      safeSendMessage(chatId, resMsg, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
    }

    activeGames.delete(gameId); // Game concluded
    await updateGroupGameDetails(chatId, null, null, null); // Clear from group session

  } else if (gameData.type === GAME_IDS.RPS && gameData.participants.length === 2) {
    gameData.status = 'waiting_choices';
    activeGames.set(gameId, gameData); // Update game state

    const p1 = gameData.participants[0];
    const p2 = gameData.participants[1];

    const rpsPrompt = `🪨📄✂️ *RPS Battle Joined!* 🪨📄✂️\n\n${p1.mention} vs ${p2.mention} for *${betDisplay}*\\!\n\nBoth players, please make your choice secretly using the buttons below\\. You will receive a private confirmation\\.`;
    const rpsKeyboard = {
      inline_keyboard: [[
        { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
        { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
        { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
      ],[
        { text: "❌ Cancel Game (Initiator Only)", callback_data: `cancel_game:${gameId}` }
      ]]
    };

    if (messageToEditId && bot) {
      bot.editMessageText(rpsPrompt, { chatId: String(chatId), message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard })
        .catch(() => { safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard }); });
    } else {
      const newMsg = await safeSendMessage(chatId, rpsPrompt, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboard });
      if (newMsg) gameData.gameSetupMessageId = newMsg.message_id; // Update if new message was sent
    }
    activeGames.set(gameId, gameData); // Save updated gameSetupMessageId if it changed
  }
}

async function handleCancelGameCallback(chatId, cancellerUserObj, gameId, interactionMessageId) {
  const LOG_PREFIX_CANCEL = `[CancelGame_CB UID:${cancellerUserObj.userId} GID:${gameId}]`;
  // console.log(`${LOG_PREFIX_CANCEL} Attempting to cancel game.`);

  const gameData = activeGames.get(gameId);

  if (!gameData) {
    // console.warn(`${LOG_PREFIX_CANCEL} Game not found or already cancelled/concluded.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "This game is no longer active.", show_alert: true });
    if (interactionMessageId && bot) {
        bot.editMessageReplyMarkup({}, { chat_id: String(chatId), message_id: Number(interactionMessageId) }).catch(() => {});
    }
    return;
  }

  if (gameData.chatId !== String(chatId)) {
    // console.warn(`${LOG_PREFIX_CANCEL} Chat ID mismatch.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Error: Game context mismatch.", show_alert: true });
    return;
  }

  // Only initiator can cancel, or admin (future enhancement)
  // Also, allow cancellation if in 'waiting_choices' for RPS if only one person has chosen (or timeout logic for choices)
  const canCancelAsInitiator = (gameData.initiatorId === String(cancellerUserObj.userId));
  const cancellableStatuses = ['waiting_opponent', 'waiting_choices']; // Can cancel if waiting for opponent, or if RPS choices are pending

  if (!canCancelAsInitiator) {
    // console.log(`${LOG_PREFIX_CANCEL} Non-initiator cannot cancel.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Only the game initiator can cancel this game.", show_alert: false });
    return;
  }

  if (!cancellableStatuses.includes(gameData.status)) {
    // console.log(`${LOG_PREFIX_CANCEL} Game is not in a cancellable state. Status: ${gameData.status}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "This game has already started or cannot be cancelled now.", show_alert: true });
    return;
  }

  // Refund bets for all participants who placed one
  // console.log(`${LOG_PREFIX_CANCEL} Refunding bets for participants.`);
  for (const p of gameData.participants) {
    if (p.betPlaced && p.userId && gameData.betAmount > 0n) {
      await updateUserBalance(p.userId, gameData.betAmount, `refund_group_${gameData.type}_cancelled:${gameId}`, null, gameId, String(chatId));
      // console.log(`${LOG_PREFIX_CANCEL} Refunded ${gameData.betAmount} to UserID: ${p.userId}`);
    }
  }

  const gameTypeDisplay = gameData.type.charAt(0).toUpperCase() + gameData.type.slice(1);
  const betDisplay = escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"));
  const cancellationMessage = `🚫 Game Cancelled 🚫\n\nThe ${escapeMarkdownV2(gameTypeDisplay)} game for *${betDisplay}*, started by ${gameData.initiatorMention}, has been cancelled\\. All bets have been refunded\\.`;

  const msgToEdit = Number(interactionMessageId || gameData.gameSetupMessageId);
  if (msgToEdit && bot) {
    bot.editMessageText(cancellationMessage, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: {} })
      .catch(() => { safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' }); });
  } else {
    safeSendMessage(chatId, cancellationMessage, { parse_mode: 'MarkdownV2' });
  }

  activeGames.delete(gameId);
  await updateGroupGameDetails(chatId, null, null, null); // Clear from group session
  // console.log(`${LOG_PREFIX_CANCEL} Game ${gameId} cancelled and removed.`);
}

async function handleRPSChoiceCallback(chatId, userChoiceObj, gameId, choiceKey, interactionMessageId) {
  const LOG_PREFIX_RPS_CHOICE = `[RPS_Choice_CB UID:${userChoiceObj.userId} GID:${gameId} Choice:${choiceKey}]`;
  // console.log(`${LOG_PREFIX_RPS_CHOICE} Handling RPS choice.`);

  const gameData = activeGames.get(gameId);

  if (!gameData || gameData.type !== GAME_IDS.RPS) {
    // console.warn(`${LOG_PREFIX_RPS_CHOICE} Game not found, not RPS, or already concluded.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "This RPS game is not active or invalid.", show_alert: true });
    return;
  }

  if (gameData.status !== 'waiting_choices') {
    // console.warn(`${LOG_PREFIX_RPS_CHOICE} RPS game not in 'waiting_choices' status. Status: ${gameData.status}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "It's not time to make a choice in this game.", show_alert: true });
    return;
  }

  const participant = gameData.participants.find(p => p.userId === String(userChoiceObj.userId));
  if (!participant) {
    // console.warn(`${LOG_PREFIX_RPS_CHOICE} User is not a participant in this game.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "You are not part of this RPS game.", show_alert: true });
    return;
  }

  if (participant.choice) {
    // console.log(`${LOG_PREFIX_RPS_CHOICE} Participant has already chosen: ${participant.choice}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: `You have already chosen ${RPS_EMOJIS[participant.choice]}. Waiting for opponent.`, show_alert: false });
    return;
  }

  // Validate the choiceKey against RPS_CHOICES
  const validChoice = Object.values(RPS_CHOICES).includes(choiceKey.toLowerCase());
  if (!validChoice) {
    console.error(`${LOG_PREFIX_RPS_CHOICE} Invalid choice key received: ${choiceKey}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid choice selected.", show_alert: true });
    return;
  }
  participant.choice = choiceKey.toLowerCase(); // Store the canonical choice
  const choiceEmoji = RPS_EMOJIS[participant.choice];

  await bot.answerCallbackQuery(callbackQueryId, { text: `You chose ${choiceEmoji} ${participant.choice}! Waiting for opponent...`, show_alert: false });
  // No need to activeGames.set here yet, will be set after checking if all chosen.

  // Check if both players have made their choices
  const allChosen = gameData.participants.length === 2 && gameData.participants.every(p => p.choice);
  const msgToEdit = Number(gameData.gameSetupMessageId || interactionMessageId); // Prefer gameSetupMessageId if available

  if (allChosen) {
    gameData.status = 'game_over'; // Mark as resolving
    activeGames.set(gameId, gameData);

    const p1 = gameData.participants[0];
    const p2 = gameData.participants[1];
    const rpsOutcome = determineRPSOutcome(p1.choice, p2.choice); // From Part 4

    let resultText = `🪨📄✂️ *RPS Battle Concluded!* 🪨📄✂️\nBet Amount: *${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"))}*\n\n`;
    resultText += `${p1.mention} chose: ${RPS_EMOJIS[p1.choice]} ${escapeMarkdownV2(p1.choice)}\n`;
    resultText += `${p2.mention} chose: ${RPS_EMOJIS[p2.choice]} ${escapeMarkdownV2(p2.choice)}\n\n`;
    resultText += `*Result:* ${escapeMarkdownV2(rpsOutcome.description)}\n\n`;

    let winnerParticipant = null;
    let finalBalancesText = "";

    if (rpsOutcome.result === 'win_player1') {
      winnerParticipant = p1;
      const winnings = gameData.betAmount + gameData.betAmount;
      const winUpdate = await updateUserBalance(p1.userId, winnings, `won_group_rps:${gameId}`, null, gameId, String(chatId));
      await updateUserBalance(p2.userId, 0n, `lost_group_rps:${gameId}`, null, gameId, String(chatId)); // Log P2 loss
      resultText += `🎉 ${p1.mention} is the victor!`;
      if (winUpdate.success) finalBalancesText += `\n${p1.mention}'s new balance: *${escapeMarkdownV2(formatCurrency(Number(winUpdate.newBalanceLamports), "credits"))}*\\.`;
    } else if (rpsOutcome.result === 'win_player2') {
      winnerParticipant = p2;
      const winnings = gameData.betAmount + gameData.betAmount;
      const winUpdate = await updateUserBalance(p2.userId, winnings, `won_group_rps:${gameId}`, null, gameId, String(chatId));
      await updateUserBalance(p1.userId, 0n, `lost_group_rps:${gameId}`, null, gameId, String(chatId)); // Log P1 loss
      resultText += `🎉 ${p2.mention} is the victor!`;
      if (winUpdate.success) finalBalancesText += `\n${p2.mention}'s new balance: *${escapeMarkdownV2(formatCurrency(Number(winUpdate.newBalanceLamports), "credits"))}*\\.`;
    } else if (rpsOutcome.result === 'draw') {
      resultText += `🤝 It's a Draw! All bets are refunded.`;
      const refund1 = await updateUserBalance(p1.userId, gameData.betAmount, `refund_group_rps_draw:${gameId}`, null, gameId, String(chatId));
      const refund2 = await updateUserBalance(p2.userId, gameData.betAmount, `refund_group_rps_draw:${gameId}`, null, gameId, String(chatId));
      if (refund1.success) finalBalancesText += `\n${p1.mention}'s new balance: *${escapeMarkdownV2(formatCurrency(Number(refund1.newBalanceLamports), "credits"))}*\\.`;
      if (refund2.success) finalBalancesText += `\n${p2.mention}'s new balance: *${escapeMarkdownV2(formatCurrency(Number(refund2.newBalanceLamports), "credits"))}*\\.`;
    } else { // Error case from determineRPSOutcome
      resultText += `⚙️ An error occurred determining the outcome. Bets are refunded as a precaution.`;
      const refund1Err = await updateUserBalance(p1.userId, gameData.betAmount, `refund_group_rps_error:${gameId}`, null, gameId, String(chatId));
      const refund2Err = await updateUserBalance(p2.userId, gameData.betAmount, `refund_group_rps_error:${gameId}`, null, gameId, String(chatId));
       if (refund1Err.success) finalBalancesText += `\n${p1.mention}'s new balance: *${escapeMarkdownV2(formatCurrency(Number(refund1Err.newBalanceLamports), "credits"))}*\\.`;
      if (refund2Err.success) finalBalancesText += `\n${p2.mention}'s new balance: *${escapeMarkdownV2(formatCurrency(Number(refund2Err.newBalanceLamports), "credits"))}*\\.`;
    }

    resultText += finalBalancesText;
    const postGameKeyboard = createPostGameKeyboard(GAME_IDS.RPS, Number(gameData.betAmount));

    if (msgToEdit && bot) {
      bot.editMessageText(resultText, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard })
        .catch(() => { safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard }); });
    } else {
      safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboard });
    }

    activeGames.delete(gameId);
    await updateGroupGameDetails(chatId, null, null, null);

  } else {
    // Only one player has chosen, update the message to reflect this
    activeGames.set(gameId, gameData); // Save the choice that was made
    let waitingForPlayerMention = "the other player";
    const otherPlayer = gameData.participants.find(p => p.userId !== String(userChoiceObj.userId));
    if (otherPlayer) waitingForPlayerMention = otherPlayer.mention;

    const updateMsgText = `${participant.mention} has made their choice ${choiceEmoji}\\!\nWaiting for ${waitingForPlayerMention} to make their move\\.\\.\\.`;
    const rpsKeyboardStillWaiting = { // Keep buttons for the player who hasn't chosen
      inline_keyboard: [[
        { text: `${RPS_EMOJIS.rock} Rock`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.ROCK}` },
        { text: `${RPS_EMOJIS.paper} Paper`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.PAPER}` },
        { text: `${RPS_EMOJIS.scissors} Scissors`, callback_data: `rps_choose:${gameId}:${RPS_CHOICES.SCISSORS}` }
      ],[
        { text: "❌ Cancel Game (Initiator Only)", callback_data: `cancel_game:${gameId}` }
      ]]
    };

    if (msgToEdit && bot) {
      bot.editMessageText(updateMsgText, { chatId: String(chatId), message_id: msgToEdit, parse_mode: 'MarkdownV2', reply_markup: rpsKeyboardStillWaiting })
        .catch((e) => {
            // console.warn(`${LOG_PREFIX_RPS_CHOICE} Failed to edit RPS message after one choice: ${e.message}. Sending new message.`);
            // safeSendMessage(chatId, updateMsgText, { parse_mode: 'MarkdownV2', reply_markup: rpsKeyboardStillWaiting });
            // Decided against sending a new message here to avoid chat spam if edits fail often.
        });
    }
  }
}


console.log("Part 5a (Segment 3): Coinflip & RPS Callback Handlers, Post-Game UX - Complete.");
// --- End of Part 5a (Segment 3 of N) ---
// --- Start of Part 5a (Segment 4 of N) ---
// index.js - Part 5a: Dice Escalator Game Logic (Start, Player Roll)
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 4): Dice Escalator Game Logic (Start, Player Roll)...");

// --- Helper Function to get Jackpot Text for the Dice Escalator Button ---
// This function was previously in Part 5b, moved here for Dice Escalator game logic cohesion.
async function getJackpotButtonText() {
  const LOG_PREFIX_JACKPOT_BTN = "[getJackpotButtonText]";
  // console.log(`${LOG_PREFIX_JACKPOT_BTN} Attempting to fetch jackpot amount for button text.`);
  let jackpotAmountString = "🎰 Jackpot: Fetching..."; // Default while loading

  try {
    if (typeof queryDatabase !== 'function' || typeof MAIN_JACKPOT_ID === 'undefined' || typeof formatCurrency !== 'function') {
      console.warn(`${LOG_PREFIX_JACKPOT_BTN} Missing dependencies. queryDatabase: ${typeof queryDatabase}, MAIN_JACKPOT_ID: ${MAIN_JACKPOT_ID}, formatCurrency: ${typeof formatCurrency}. Using default.`);
      return "🎰 Jackpot: N/A";
    }

    const sqlQuery = 'SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1';
    const queryParams = [MAIN_JACKPOT_ID];
    const result = await queryDatabase(sqlQuery, queryParams);

    if (result.rows.length > 0) {
      const jackpotAmountLamports = BigInt(result.rows[0].current_amount_lamports);
      // console.log(`${LOG_PREFIX_JACKPOT_BTN} Jackpot amount found: ${jackpotAmountLamports} lamports.`);
      const jackpotDisplayAmount = formatCurrency(jackpotAmountLamports, "credits"); // Display as credits
      jackpotAmountString = `💎 Jackpot: ${escapeMarkdownV2(jackpotDisplayAmount)} 💎`; // Enhanced visual
      // console.log(`${LOG_PREFIX_JACKPOT_BTN} Successfully generated jackpot button text: "${jackpotAmountString}"`);
    } else {
      // console.log(`${LOG_PREFIX_JACKPOT_BTN} No jackpot record found for ID: ${MAIN_JACKPOT_ID}. Using 'N/A'.`);
      jackpotAmountString = "💎 Jackpot: N/A 💎";
    }
  } catch (error) {
    console.error(`${LOG_PREFIX_JACKPOT_BTN} Error fetching jackpot for button: ${error.message}`, error.stack);
    jackpotAmountString = "💎 Jackpot: Error 💎";
  }
  return jackpotAmountString;
}


// --- Dice Escalator Game Handler Functions ---

async function handleStartDiceEscalatorCommand(chatId, userObj, betAmount, originalCommandMessageId) {
  const LOG_PREFIX_DE_START = `[DE_Start UID:${userObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_DE_START} Initiating Dice Escalator. Bet: ${betAmount}`);

  if (!userObj || typeof userObj.userId === 'undefined') {
    console.error(`${LOG_PREFIX_DE_START} CRITICAL: User object or userId undefined.`);
    await safeSendMessage(chatId, "An internal error occurred with your player profile. Please try again.", {});
    return;
  }
  const userId = String(userObj.userId);
  const playerRef = getPlayerDisplayReference(userObj);
  const gameId = generateGameId(GAME_IDS.DICE_ESCALATOR);

  if (userObj.balanceLamports < BigInt(betAmount)) {
    const needed = formatCurrency(Number(betAmount) - userObj.balance, "credits");
    await safeSendMessage(chatId, `${playerRef}, your balance of ${escapeMarkdownV2(formatCurrency(userObj.balance, "credits"))} is too low for a *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* Dice Escalator bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const balanceUpdateResult = await updateUserBalance(userId, -BigInt(betAmount), `bet_placed_dice_escalator:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult || !balanceUpdateResult.success) {
    console.error(`${LOG_PREFIX_DE_START} Wager placement failed: ${balanceUpdateResult.error}`);
    await safeSendMessage(chatId, `${playerRef}, your Dice Escalator wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Please check your balance or try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }
  // console.log(`${LOG_PREFIX_DE_START} Wager ${betAmount} accepted. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  const gameData = {
    type: GAME_IDS.DICE_ESCALATOR, gameId, chatId: String(chatId), userId, playerRef, userObj, // Store full userObj
    betAmount: BigInt(betAmount), playerScore: 0n, playerRollCount: 0, botScore: 0n,
    status: 'waiting_player_roll', // Initial status: player needs to make the first roll
    gameMessageId: null, commandMessageId: originalCommandMessageId,
    lastInteractionTime: Date.now()
  };
  activeGames.set(gameId, gameData);
  // console.log(`${LOG_PREFIX_DE_START} Game ${gameId} initiated. Data: ${stringifyWithBigInt(gameData)}`);

  const jackpotButtonText = await getJackpotButtonText();
  let targetJackpotScoreDisplay = (typeof TARGET_JACKPOT_SCORE !== 'undefined' && !isNaN(TARGET_JACKPOT_SCORE))
                                ? escapeMarkdownV2(String(TARGET_JACKPOT_SCORE))
                                : escapeMarkdownV2("the house's target");

  const jackpotTip = `\n\n👑 *Jackpot Alert!* To claim the grand Dice Escalator Jackpot, you must bravely *Stand* with a score of *${targetJackpotScoreDisplay}\\+* AND emerge victorious against the Bot Dealer in this round\\!`;

  const initialMessageText = `🎲 Welcome to the electrifying **Dice Escalator**, ${playerRef}\\! 🎲\n\nYou've wagered *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* against our formidable Bot Dealer 🤖\\.${jackpotTip}\n\nThe dice await your command\\. Your current score: *0*\\. Press "Roll Dice" to start your ascent\\! 👇`;
  const keyboard = {
    inline_keyboard: [
      [{ text: jackpotButtonText, callback_data: `jackpot_display_noop:${gameId}` }], // gameId for context if needed
      [{ text: "🎲 Roll Dice", callback_data: `de_roll_prompt:${gameId}` }],
      [{ text: `📜 ${escapeMarkdownV2(gameData.type.replace('_',' '))} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR}` }]
    ]
  };

  const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

  if (sentMessage && sentMessage.message_id) {
    gameData.gameMessageId = sentMessage.message_id;
    activeGames.set(gameId, gameData); // Update gameData with messageId
    // console.log(`${LOG_PREFIX_DE_START} Game ${gameId} initial message sent. MessageID: ${sentMessage.message_id}.`);
  } else {
    console.error(`${LOG_PREFIX_DE_START} Failed to send Dice Escalator game message for ${gameId}. Refunding wager.`);
    await updateUserBalance(userId, BigInt(betAmount), `refund_dice_escalator_setup_fail:${gameId}`, null, gameId, String(chatId));
    activeGames.delete(gameId);
    // console.log(`${LOG_PREFIX_DE_START} Wager refunded and game ${gameId} removed.`);
  }
  // console.log(`${LOG_PREFIX_DE_START} Exiting command handler.`);
}

async function handleDiceEscalatorPlayerAction(gameId, userIdFromCallback, action, originalMessageId, chatIdFromCallback) {
  const LOG_PREFIX_DE_ACTION = `[DE_Action GID:${gameId} UID:${userIdFromCallback} Act:${action}]`;
  // console.log(`${LOG_PREFIX_DE_ACTION} Handling player action.`);

  const gameData = activeGames.get(gameId);
  const now = Date.now();

  if (!gameData) {
    // console.warn(`${LOG_PREFIX_DE_ACTION} Game not found. Attempting to clear buttons on msg ${originalMessageId}.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "This Dice Escalator game has ended.", show_alert: true });
    if (bot && originalMessageId && chatIdFromCallback) {
      bot.editMessageReplyMarkup({}, { chat_id: String(chatIdFromCallback), message_id: Number(originalMessageId) }).catch(() => {});
    }
    return;
  }
  // console.log(`${LOG_PREFIX_DE_ACTION} Game found. Current status: ${gameData.status}`);

  if (action === `jackpot_display_noop:${gameId}` || action === 'jackpot_display_noop') {
    // console.log(`${LOG_PREFIX_DE_ACTION} Jackpot display button clicked (no-op).`);
    await bot.answerCallbackQuery(callbackQueryId); // Acknowledge silently
    return;
  }

  if (String(gameData.userId) !== String(userIdFromCallback)) {
    // console.warn(`${LOG_PREFIX_DE_ACTION} User mismatch. Game belongs to ${gameData.userId}.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "This isn't your active game turn.", show_alert: false });
    return;
  }

  // Crucial check against stale messages, especially if using delete & resend or frequent edits
  if (gameData.gameMessageId && Number(gameData.gameMessageId) !== Number(originalMessageId)) {
    // console.warn(`${LOG_PREFIX_DE_ACTION} Action on stale message. GameMsgID: ${gameData.gameMessageId}, ActionMsgID: ${originalMessageId}.`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Please use the latest game message buttons.", show_alert: true });
    return;
  }

  gameData.lastInteractionTime = now;
  activeGames.set(gameId, gameData); // Update interaction time

  const jackpotButtonText = await getJackpotButtonText(); // Get fresh text for any message updates

  switch (action) {
    case `de_roll_prompt:${gameId}`: // Ensure gameId matches if it's part of action string
    case 'de_roll_prompt': // Generic action part if gameId is passed separately
      // console.log(`${LOG_PREFIX_DE_ACTION} Validating status for roll. Current: ${gameData.status}`);
      // Valid statuses for rolling: 'waiting_player_roll' (first roll) or 'player_turn_prompt_action' (subsequent rolls)
      if (gameData.status !== 'waiting_player_roll' && gameData.status !== 'player_turn_prompt_action') {
        // console.warn(`${LOG_PREFIX_DE_ACTION} Roll attempt in invalid state '${gameData.status}'.`);
        let endedMsgText = `This round of Dice Escalator (ID: \`${escapeMarkdownV2(gameId)}\`) has already concluded or it's not your turn\\.`;
        let replyMarkup = {};
        if (gameData.status.startsWith("game_over")) {
            endedMsgText += ` Your final score was: *${escapeMarkdownV2(String(gameData.playerScore))}*\\.`;
            replyMarkup = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR, Number(gameData.betAmount));
        } else if (gameData.status === 'bot_turn' || gameData.status === 'bot_rolling') {
            endedMsgText = `The Bot Dealer is currently playing for game ID \`${escapeMarkdownV2(gameId)}\`\\. Please wait for the results\\.`;
            replyMarkup = { inline_keyboard: [[{ text: jackpotButtonText, callback_data: `jackpot_display_noop:${gameId}` }]] };
        }

        await bot.answerCallbackQuery(callbackQueryId, { text: "Not your turn or game ended.", show_alert: true });
        if (gameData.gameMessageId && bot) {
          bot.editMessageText(endedMsgText, {
            chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
            parse_mode: 'MarkdownV2', reply_markup: replyMarkup
          }).catch(() => {}); // Ignore error if message already deleted
        }
        return;
      }
      // console.log(`${LOG_PREFIX_DE_ACTION} Status valid for roll. Processing player roll.`);
      await processDiceEscalatorPlayerRoll(gameData, jackpotButtonText); // Pass fresh jackpot button text
      break;

    case `de_cashout:${gameId}`: // Player stands
    case 'de_cashout':
      // console.log(`${LOG_PREFIX_DE_ACTION} Validating status for cashout/stand. Current: ${gameData.status}`);
      if (gameData.status !== 'player_turn_prompt_action') {
        // console.warn(`${LOG_PREFIX_DE_ACTION} Stand attempt in invalid state '${gameData.status}'.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "You can only stand after rolling at least once.", show_alert: true });
        return;
      }
      if (gameData.playerScore <= 0n && gameData.playerRollCount > 0) { // Can't stand if busted (score would be 0) or haven't rolled
         // This check is mostly redundant if bust makes score 0 and player can't stand on 0.
        // console.warn(`${LOG_PREFIX_DE_ACTION} Stand attempt with non-positive score ${gameData.playerScore}.`);
        await bot.answerCallbackQuery(callbackQueryId, { text: "You must have a score greater than zero to stand.", show_alert: true });
        return;
      }
      // console.log(`${LOG_PREFIX_DE_ACTION} Player stands with score ${gameData.playerScore}. Processing bot turn.`);
      // Full implementation of bot turn will be in the next segment.
      await processDiceEscalatorStandAction(gameData, jackpotButtonText); // New function for clarity
      break;

    default:
      console.error(`${LOG_PREFIX_DE_ACTION} Unknown Dice Escalator action: '${action}'.`);
      await bot.answerCallbackQuery(callbackQueryId, { text: "Unknown game action.", show_alert: true });
  }
  // console.log(`${LOG_PREFIX_DE_ACTION} Exiting action handler.`);
}

async function processDiceEscalatorPlayerRoll(gameData, currentJackpotButtonText) {
  const LOG_PREFIX_DE_PLAYER_ROLL = `[DE_PlayerRoll GID:${gameData.gameId} UID:${gameData.userId}]`;
  // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Processing player roll. Current score: ${gameData.playerScore}`);

  gameData.status = 'player_rolling'; // Mark that player is in the process of rolling
  activeGames.set(gameData.gameId, gameData);

  const rollingMessage = `${gameData.playerRef} is shaking the dice cup\\! 🎲\nWager: *${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"))}*\nCurrent Score: *${escapeMarkdownV2(String(gameData.playerScore))}*\nRolling for glory\\!`;
  const rollingKeyboard = { inline_keyboard: [
      [{ text: currentJackpotButtonText, callback_data: `jackpot_display_noop:${gameData.gameId}` }]
  ]};

  if (gameData.gameMessageId && bot) {
    try {
        await bot.editMessageText(rollingMessage, {
        chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: rollingKeyboard
        });
    } catch (editError) {
        // console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to edit message to 'rolling...': ${editError.message}. Game message might have been deleted.`);
        // If edit fails, the dice animation will still appear as a new message.
    }
  }
  await sleep(700); // Short pause for "shaking dice" effect

  let playerRollValue;
  let animatedDiceMessageId = null;
  // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Attempting animated dice roll.`);
  try {
    if (!bot || typeof bot.sendDice !== 'function') throw new Error("bot.sendDice is unavailable.");
    const diceMessage = await bot.sendDice(String(gameData.chatId), { emoji: '🎲' }); // Send to game chat
    if (!diceMessage || !diceMessage.dice || typeof diceMessage.dice.value === 'undefined') {
      throw new Error("Invalid dice message response from Telegram API.");
    }
    playerRollValue = BigInt(diceMessage.dice.value);
    animatedDiceMessageId = diceMessage.message_id; // Store to delete later
    // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Player rolled ${playerRollValue} (animated dice msg ID: ${animatedDiceMessageId}). Pausing 2s.`);
    await sleep(2000); // Let user see the animated die
  } catch (diceError) {
    console.error(`${LOG_PREFIX_DE_PLAYER_ROLL} Animated dice roll failed: ${diceError.message}. Using internal roll.`, diceError.stack);
    playerRollValue = BigInt(rollDie()); // rollDie is from Part 3
    // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Player rolled ${playerRollValue} (internal roll).`);
    // Send a message indicating internal roll as animated one failed
    await safeSendMessage(String(gameData.chatId), `⚙️ ${gameData.playerRef} (Internal Roll): You rolled a *${escapeMarkdownV2(String(playerRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' });
    await sleep(1000); // Brief pause for the internal roll message
  }

  // Attempt to delete the animated dice message IF it was sent successfully to reduce chat clutter
  if (animatedDiceMessageId && bot) {
      bot.deleteMessage(String(gameData.chatId), animatedDiceMessageId).catch(delErr => {
          // console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Non-critical: Failed to delete animated dice message ${animatedDiceMessageId}: ${delErr.message}`);
      });
  }

  gameData.playerRollCount += 1;
  // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Player roll count now ${gameData.playerRollCount}.`);

  // DICE_ESCALATOR_BUST_ON should be defined in Part 1, parsed as int.
  const bustValue = BigInt(DICE_ESCALATOR_BUST_ON); // Ensure comparison with BigInt
  const latestJackpotButtonText = await getJackpotButtonText(); // Get fresh text

  if (playerRollValue === bustValue) {
    // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Player BUSTED with roll ${playerRollValue}!`);
    const originalScoreBeforeBust = gameData.playerScore; // For display, if needed
    gameData.playerScore = 0n; // Score resets on bust
    gameData.status = 'game_over_player_bust';
    activeGames.set(gameData.gameId, gameData);

    // Log the loss in the bets table; bet was already deducted.
    // The 0n indicates no further change to balance for this specific transaction (loss is implicit)
    await updateUserBalance(gameData.userId, 0n, `lost_dice_escalator_bust:${gameData.gameId}`, null, gameData.gameId, String(gameData.chatId));

    const userForBalanceDisplay = await getUser(gameData.userId); // Get updated user data for balance
    const newBalanceDisplay = userForBalanceDisplay ? escapeMarkdownV2(formatCurrency(userForBalanceDisplay.balanceLamports, "credits")) : "N/A";

    const bustMessage = `💥 Oh no, ${gameData.playerRef}\\! A roll of *${escapeMarkdownV2(String(playerRollValue))}* means you've BUSTED\\!\nYour score of *${escapeMarkdownV2(String(originalScoreBeforeBust))}* crumbles to dust\\. The house claims your wager of *${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"))}*\\.\n\nBetter luck next time\\! 😥\nYour new balance: *${newBalanceDisplay}*\\.`;
    const bustKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR, Number(gameData.betAmount));
    // Add jackpot button back to bust keyboard
    bustKeyboard.inline_keyboard.unshift([{ text: latestJackpotButtonText, callback_data: `jackpot_display_noop:${gameData.gameId}` }]);


    if (gameData.gameMessageId && bot) {
      try {
          await bot.editMessageText(bustMessage, {
          chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
          parse_mode: 'MarkdownV2', reply_markup: bustKeyboard
          });
      } catch (e) {
          // console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to edit bust message, sending new: ${e.message}`);
          await safeSendMessage(String(gameData.chatId), bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
      }
    } else { // Should not happen if game start was successful
      await safeSendMessage(String(gameData.chatId), bustMessage, { parse_mode: 'MarkdownV2', reply_markup: bustKeyboard });
    }
    activeGames.delete(gameData.gameId); // Game over
    // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Game ${gameData.gameId} deleted due to player bust.`);

  } else {
    gameData.playerScore += playerRollValue;
    gameData.status = 'player_turn_prompt_action'; // Player can choose to roll again or stand
    activeGames.set(gameData.gameId, gameData);
    // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Successful roll ${playerRollValue}. New Score: ${gameData.playerScore}. Status: ${gameData.status}.`);

    const successMessage = `🎯 Excellent roll, ${gameData.playerRef}\\! You rolled a *${escapeMarkdownV2(String(playerRollValue))}*\\!\nYour score escalates to an impressive: *${escapeMarkdownV2(String(gameData.playerScore))}*\\.\nWager: *${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"))}*\n\nThe choice is yours: Tempt fate with another "Roll Again", or strategically "Stand" with your current score\\? 🤔`;
    const successKeyboard = {
      inline_keyboard: [
        [{ text: latestJackpotButtonText, callback_data: `jackpot_display_noop:${gameData.gameId}` }],
        [
          { text: "🎲 Roll Again", callback_data: `de_roll_prompt:${gameData.gameId}` },
          { text: "💰 Stand & Challenge Bot", callback_data: `de_cashout:${gameData.gameId}` }
        ],
        [{ text: `📜 ${escapeMarkdownV2(gameData.type.replace('_',' '))} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR}` }]
      ]
    };

    if (gameData.gameMessageId && bot) {
      try {
        await bot.editMessageText(successMessage, {
          chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
          parse_mode: 'MarkdownV2', reply_markup: successKeyboard
        });
      } catch (e) {
        // console.warn(`${LOG_PREFIX_DE_PLAYER_ROLL} Failed to edit success message, sending new: ${e.message}`);
        const newMsg = await safeSendMessage(String(gameData.chatId), successMessage, { parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
        if (newMsg && newMsg.message_id) { // If sending new was successful, update gameMessageId
            gameData.gameMessageId = newMsg.message_id;
            activeGames.set(gameData.gameId, gameData);
        }
      }
    } else { // Should not happen if game start was successful
      const newMsg = await safeSendMessage(String(gameData.chatId), successMessage, { parse_mode: 'MarkdownV2', reply_markup: successKeyboard });
      if (newMsg && newMsg.message_id) {
            gameData.gameMessageId = newMsg.message_id;
            activeGames.set(gameData.gameId, gameData);
        }
    }
  }
  // console.log(`${LOG_PREFIX_DE_PLAYER_ROLL} Exiting player roll process.`);
}


console.log("Part 5a (Segment 4): Dice Escalator Game Logic (Start, Player Roll) - Complete.");
// --- End of Part 5a (Segment 4 of N) ---
// --- Start of Part 5a (Segment 5 of N) ---
// index.js - Part 5a: Dice Escalator Game Logic (Stand, Bot Turn, Jackpot)
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 5): Dice Escalator Game Logic (Stand, Bot Turn, Jackpot)...");

// This function is called when a player chooses to stand in Dice Escalator
async function processDiceEscalatorStandAction(gameData, currentJackpotButtonText) {
    const LOG_PREFIX_DE_STAND = `[DE_Stand GID:${gameData.gameId} UID:${gameData.userId}]`;
    // console.log(`${LOG_PREFIX_DE_STAND} Player stands with score ${gameData.playerScore}. Preparing for bot turn.`);

    gameData.status = 'bot_turn_pending'; // Intermediate status before bot actually starts rolling
    activeGames.set(gameData.gameId, gameData);

    const standMessage = `${gameData.playerRef} bravely stands with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\! 🏦\nThe wager: *${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"))}*\n\nAll eyes turn to the Bot Dealer 🤖\\. The tension is palpable\\! Will your score hold up\\?`;
    const standKeyboard = {
        inline_keyboard: [
            [{ text: currentJackpotButtonText, callback_data: `jackpot_display_noop:${gameData.gameId}` }]
            // No other actions for player at this point
        ]
    };

    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(standMessage, {
                chat_id: String(gameData.chatId),
                message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2',
                reply_markup: standKeyboard
            });
        } catch (e) {
            // console.warn(`${LOG_PREFIX_DE_STAND} Failed to edit message for stand, sending new: ${e.message}`);
            const newMsg = await safeSendMessage(String(gameData.chatId), standMessage, { parse_mode: 'MarkdownV2', reply_markup: standKeyboard });
            if (newMsg && newMsg.message_id) gameData.gameMessageId = newMsg.message_id; // Update game message ID
        }
    } else {
        const newMsg = await safeSendMessage(String(gameData.chatId), standMessage, { parse_mode: 'MarkdownV2', reply_markup: standKeyboard });
        if (newMsg && newMsg.message_id) gameData.gameMessageId = newMsg.message_id;
    }
    activeGames.set(gameData.gameId, gameData); // Save updated gameMessageId if it changed

    // console.log(`${LOG_PREFIX_DE_STAND} Pausing briefly before Bot's turn begins.`);
    await sleep(2000); // Dramatic pause

    await processDiceEscalatorBotTurn(gameData); // Initiate the bot's turn
}


async function processDiceEscalatorBotTurn(gameData) {
  const LOG_PREFIX_DE_BOT_TURN = `[DE_BotTurn GID:${gameData.gameId}]`;
  // console.log(`${LOG_PREFIX_DE_BOT_TURN} Bot's turn begins. Player score: ${gameData.playerScore}.`);

  const { gameId, chatId, userId, playerRef, playerScore, betAmount, userObj } = gameData; // Destructure userObj as well for final balance display
  gameData.status = 'bot_rolling';
  gameData.botScore = 0n; // Reset bot score for its turn
  activeGames.set(gameId, gameData);

  let jackpotButtonTextBotTurn = await getJackpotButtonText();
  let botMessageAccumulator = `${playerRef} stands at *${escapeMarkdownV2(String(playerScore))}*\\. Wager: *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}*\\.\n\nThe Bot Dealer 🤖 takes a deep breath and begins to roll\\.\\.\\.\n`;
  let animatedDiceMessageId = null; // To store ID of bot's animated dice message for deletion

  const updateBotTurnMessage = async (text, showRollingIndicator = false) => {
    let fullText = botMessageAccumulator + text;
    if (showRollingIndicator) {
        fullText += "\n_The Bot Dealer rolls again\\.\\.\\. 🎲_";
    }
    const keyboard = { inline_keyboard: [[{ text: jackpotButtonTextBotTurn, callback_data: `jackpot_display_noop:${gameId}` }]] };
    if (gameData.gameMessageId && bot) {
        try {
            await bot.editMessageText(fullText, {
                chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: keyboard
            });
        } catch (e) {
            // console.warn(`${LOG_PREFIX_DE_BOT_TURN} Failed to edit bot turn message: ${e.message}. A new message might be sent by other logic if unrecoverable.`);
        }
    } else { // Should not happen if stand message was successful
        // console.warn(`${LOG_PREFIX_DE_BOT_TURN} No gameMessageId to edit for bot turn. This is unexpected.`);
    }
  };

  await updateBotTurnMessage(""); // Initial message: "Bot begins to roll..."

  await sleep(1500);

  let botRollValue;
  const botStandScore = BigInt(BOT_STAND_SCORE_DICE_ESCALATOR); // From Part 1
  const bustValueBot = BigInt(DICE_ESCALATOR_BUST_ON);     // From Part 1

  // console.log(`${LOG_PREFIX_DE_BOT_TURN} Bot parameters: StandScore=${botStandScore}, BustValue=${bustValueBot}.`);

  do {
    jackpotButtonTextBotTurn = await getJackpotButtonText(); // Refresh in case jackpot changes mid-roll by another game (unlikely but good practice)
    // console.log(`${LOG_PREFIX_DE_BOT_TURN} Bot rolling. Current bot score: ${gameData.botScore}.`);

    // Delete previous animated dice if it exists
    if (animatedDiceMessageId && bot) {
        bot.deleteMessage(String(chatId), animatedDiceMessageId).catch(() => {});
        animatedDiceMessageId = null;
    }

    try {
      if (!bot || typeof bot.sendDice !== 'function') throw new Error("bot.sendDice unavailable.");
      const diceMessage = await bot.sendDice(String(chatId), { emoji: '🎲' });
      if (!diceMessage || !diceMessage.dice || typeof diceMessage.dice.value === 'undefined') {
        throw new Error("Invalid dice message from API for bot.");
      }
      botRollValue = BigInt(diceMessage.dice.value);
      animatedDiceMessageId = diceMessage.message_id; // Store for deletion
      // console.log(`${LOG_PREFIX_DE_BOT_TURN} Bot rolled ${botRollValue} (animated dice ID: ${animatedDiceMessageId}). Pausing.`);
      await sleep(2000); // Let user see the animated die
    } catch (diceError) {
      console.error(`${LOG_PREFIX_DE_BOT_TURN} Bot animated dice failed: ${diceError.message}. Using internal.`, diceError.stack);
      botRollValue = BigInt(rollDie()); // Part 3
      // console.log(`${LOG_PREFIX_DE_BOT_TURN} Bot rolled ${botRollValue} (internal).`);
      await safeSendMessage(String(chatId), `⚙️ Bot Dealer (Internal Roll): A *${escapeMarkdownV2(String(botRollValue))}* is revealed\\! 🎲`, { parse_mode: 'MarkdownV2' });
      await sleep(1000);
    }

    if (botRollValue === bustValueBot) {
      gameData.botScore = 0n; // Bot busts, score is 0
      botMessageAccumulator += `\nBot rolls a *${escapeMarkdownV2(String(botRollValue))}*\\! 💥 **BUSTED!** The Bot Dealer's score crumbles\\!\n`;
      // console.log(`${LOG_PREFIX_DE_BOT_TURN} Bot BUSTED with ${botRollValue}.`);
      await updateBotTurnMessage(""); // Update with bust message
      break; // Exit rolling loop
    } else {
      gameData.botScore += botRollValue;
      botMessageAccumulator += `\nBot rolls a *${escapeMarkdownV2(String(botRollValue))}*\\. Bot's score is now *${escapeMarkdownV2(String(gameData.botScore))}*\\.\n`;
      // console.log(`${LOG_PREFIX_DE_BOT_TURN} Bot roll ${botRollValue} successful. New Bot score: ${gameData.botScore}.`);
    }
    activeGames.set(gameId, gameData); // Persist bot's score progress

    // Update message, indicate if bot rolls again or stands
    const willRollAgain = gameData.botScore > 0n && gameData.botScore < botStandScore;
    await updateBotTurnMessage("", willRollAgain);
    if (willRollAgain) await sleep(2200); // Pause if bot is rolling again

  } while (gameData.botScore > 0n && gameData.botScore < botStandScore); // Continue if not busted and below stand score

  // After loop, one final update if bot stood without rolling again message
  if (gameData.botScore >= botStandScore && gameData.botScore > 0n) { // Bot stands
      botMessageAccumulator += `\n🤖 The Bot Dealer stands with *${escapeMarkdownV2(String(gameData.botScore))}*\\.\n`;
      // console.log(`${LOG_PREFIX_DE_BOT_TURN} Bot stands with ${gameData.botScore}.`);
      await updateBotTurnMessage(""); // Final score display before result
  }

  // Delete final animated dice message
  if (animatedDiceMessageId && bot) {
      bot.deleteMessage(String(chatId), animatedDiceMessageId).catch(() => {});
  }

  await sleep(1500); // Dramatic pause before final result

  // --- Determine Outcome & Jackpot ---
  let resultMessageText;
  let payoutAmount = 0n;
  let outcomeReasonLog = "";
  let jackpotWon = false;
  const targetJackpotScoreValue = BigInt(TARGET_JACKPOT_SCORE); // From Part 1

  // console.log(`${LOG_PREFIX_DE_BOT_TURN} Determining outcome. Player: ${playerScore}, Bot: ${gameData.botScore}, JackpotTarget: ${targetJackpotScoreValue}.`);

  if (gameData.botScore === 0n) { // Bot busted
    resultMessageText = `🎉 **YOU WIN!** ${playerRef}, the Bot Dealer has busted\\! Your score of *${escapeMarkdownV2(String(playerScore))}* stands tall\\!`;
    payoutAmount = betAmount + betAmount; // Bet back + 1x profit
    outcomeReasonLog = `won_dice_escalator_bot_bust:${gameId}`;
    if (playerScore >= targetJackpotScoreValue) {
      // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot condition met (bot bust & player score ${playerScore} >= ${targetJackpotScoreValue}).`);
      jackpotWon = true;
    }
  } else if (playerScore > gameData.botScore) {
    resultMessageText = `🎉 **YOU WIN!** ${playerRef}, your superior score of *${escapeMarkdownV2(String(playerScore))}* triumphs over the Bot's *${escapeMarkdownV2(String(gameData.botScore))}*\\!`;
    payoutAmount = betAmount + betAmount;
    outcomeReasonLog = `won_dice_escalator_score:${gameId}`;
    if (playerScore >= targetJackpotScoreValue) {
      // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot condition met (player score ${playerScore} > bot ${gameData.botScore} AND player score >= ${targetJackpotScoreValue}).`);
      jackpotWon = true;
    }
  } else if (playerScore < gameData.botScore) {
    resultMessageText = `💀 **Bot Wins.** The Bot Dealer's score of *${escapeMarkdownV2(String(gameData.botScore))}* edges out your *${escapeMarkdownV2(String(playerScore))}*\\. Better luck next time\\!`;
    payoutAmount = 0n; // Bet is lost (already deducted)
    outcomeReasonLog = `lost_dice_escalator_score:${gameId}`;
  } else { // Draw
    resultMessageText = `😐 **PUSH!** A tense standoff ends in a draw\\! Both you and the Bot Dealer scored *${escapeMarkdownV2(String(playerScore))}*\\. Your wager is returned\\.`;
    payoutAmount = betAmount; // Bet returned
    outcomeReasonLog = `push_dice_escalator:${gameId}`;
  }
  // console.log(`${LOG_PREFIX_DE_BOT_TURN} Outcome: ${resultMessageText}. Payout: ${payoutAmount}. Reason: ${outcomeReasonLog}. JackpotWon: ${jackpotWon}.`);

  botMessageAccumulator += `\n------------------------------------\n${resultMessageText}\n`;
  gameData.status = `game_over_final_${outcomeReasonLog.split(':')[0]}`; // e.g., game_over_final_won
  // activeGames.set(gameId, gameData); // Game will be deleted soon, status update mostly for logs

  let finalUserBalanceForDisplay = userObj.balanceLamports; // Start with balance before this game's outcome

  if (payoutAmount > 0n || outcomeReasonLog.startsWith('lost_')) { // lost means 0 payout, but still a transaction
    const effectiveChangeForBalance = (payoutAmount > 0n && !outcomeReasonLog.startsWith('push_')) ? (payoutAmount - betAmount) : (outcomeReasonLog.startsWith('push_') ? 0n : -betAmount);
    // The above is complex. Simpler: if payoutAmount includes original bet (win/push), use payoutAmount. If loss, it's 0.
    // `updateUserBalance` expects the total amount to credit. If it's a win, credit winnings + original bet. If push, credit original bet. If loss, credit 0.
    const balanceUpdateResult = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    // console.log(`${LOG_PREFIX_DE_BOT_TURN} Balance update result: ${stringifyWithBigInt(balanceUpdateResult)}.`);
    if (balanceUpdateResult.success) {
      finalUserBalanceForDisplay = balanceUpdateResult.newBalanceLamports;
      if (payoutAmount > betAmount && !outcomeReasonLog.startsWith('push_')) { // Actual win
        botMessageAccumulator += `\nYou won *${escapeMarkdownV2(formatCurrency(Number(payoutAmount - betAmount), "credits"))}* profit\\!`;
      } else if (outcomeReasonLog.startsWith('push_')) {
         // message already states wager returned
      } else if (payoutAmount === 0n) {
         // message already states loss
      }
    } else {
      botMessageAccumulator += `\n⚠️ There was an issue settling your bet: ${escapeMarkdownV2(balanceUpdateResult.error || "N/A")}\\. Admin notified\\.`;
      console.error(`${LOG_PREFIX_DE_BOT_TURN} Failed to update balance for ${userId}, game ${gameId}. Error: ${balanceUpdateResult.error}`);
    }
  }

  // --- Jackpot Payout Logic ---
  if (jackpotWon) {
    // console.log(`${LOG_PREFIX_DE_BOT_TURN} JACKPOT WIN - Processing. GameID: ${gameId}, User: ${userId}.`);
    let jackpotPayoutSuccessful = false;
    if (!pool || typeof pool.connect !== 'function') {
      console.error(`${LOG_PREFIX_DE_BOT_TURN} CRITICAL DB POOL ERROR during jackpot processing! GameID: ${gameId}`);
      botMessageAccumulator += `\n\n⚠️ A system error occurred with the Jackpot payout (DB Pool). Admin has been alerted. Your standard win is secure.`;
    } else {
      const client = await pool.connect();
      // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - DB client acquired.`);
      try {
        await client.query('BEGIN');
        // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - DB transaction started.`);
        const jackpotSelectResult = await client.query('SELECT current_amount_lamports FROM jackpot_status WHERE jackpot_id = $1 FOR UPDATE', [MAIN_JACKPOT_ID]);
        // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - Select query result: ${stringifyWithBigInt(jackpotSelectResult)}.`);

        if (jackpotSelectResult.rows.length > 0) {
          const jackpotTotalLamports = BigInt(jackpotSelectResult.rows[0].current_amount_lamports);
          // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - Current amount from DB: ${jackpotTotalLamports}.`);
          if (jackpotTotalLamports > 0n) {
            const jackpotPayoutUpdate = await updateUserBalance(userId, jackpotTotalLamports, `jackpot_win_dice_escalator:${gameId}`, client, gameId, String(chatId));
            // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - Payout (updateUserBalance) result: ${stringifyWithBigInt(jackpotPayoutUpdate)}.`);
            if (jackpotPayoutUpdate.success) {
              await client.query(
                'UPDATE jackpot_status SET current_amount_lamports = $1, last_won_at = NOW(), last_won_by_user_id = $2, last_won_game_id = $3, updated_at = NOW() WHERE jackpot_id = $4',
                ['0', userId, gameId, MAIN_JACKPOT_ID] // Reset jackpot to 0
              );
              await client.query('COMMIT');
              const jackpotWinMsgText = `\n\n👑🌟 **!!CONGRATULATIONS, ${playerRef}!!** 🌟👑\nAgainst all odds, you've hit the **DICE ESCALATOR JACKPOT** of\n💎 *${escapeMarkdownV2(formatCurrency(jackpotTotalLamports, "credits"))}* 💎\nAn absolutely legendary win! This epic moment will be remembered!`;
              botMessageAccumulator += jackpotWinMsgText;
              finalUserBalanceForDisplay = jackpotPayoutUpdate.newBalanceLamports; // Update with jackpot winnings
              jackpotPayoutSuccessful = true;
              // console.log(`${LOG_PREFIX_DE_BOT_TURN} JACKPOT WIN SUCCESS! User ${userId} PAID ${jackpotTotalLamports}. Jackpot reset.`);
            } else {
              await client.query('ROLLBACK');
              console.error(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - Payout failed via updateUserBalance: ${jackpotPayoutUpdate.error}. Rolled back.`);
              botMessageAccumulator += `\n\n⚠️ An issue occurred with the Jackpot payout (balance update failed). Support is investigating. Your standard win is secure.`;
            }
          } else { // Jackpot was 0, even if conditions met
            await client.query('COMMIT'); // Commit to release lock
            // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - Condition met, but jackpot amount was 0. No payout.`);
            botMessageAccumulator += `\n\n✨ You met the Jackpot conditions, but the Jackpot was already claimed or empty this time! Still, an amazing play!`;
          }
        } else { // MAIN_JACKPOT_ID not found
          await client.query('ROLLBACK');
          console.error(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - ID ${MAIN_JACKPOT_ID} not found in jackpot_status table. Rolled back.`);
          botMessageAccumulator += `\n\n⚠️ Jackpot system configuration error (ID not found). Admin notified. Your standard win is secure.`;
        }
      } catch (error) {
        console.error(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - DB transaction error: ${error.message}.`, error.stack);
        try { await client.query('ROLLBACK'); /* console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - Rolled back due to DB error.`); */ }
        catch (rbErr) { console.error(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - Rollback failed: ${rbErr.message}`, rbErr.stack); }
        botMessageAccumulator += `\n\n⚠️ A database error affected the Jackpot payout. Admin notified. Your standard win is secure.`;
      } finally {
        if (client) client.release();
        // console.log(`${LOG_PREFIX_DE_BOT_TURN} Jackpot - DB client released.`);
      }
    }
     // Update jackpotButtonText for the final message AFTER a potential win
    jackpotButtonTextBotTurn = await getJackpotButtonText();
  }
  // Always show final balance
  botMessageAccumulator += `\n\n${playerRef}'s new balance: *${escapeMarkdownV2(formatCurrency(finalUserBalanceForDisplay, "credits"))}*\\.`;


  const finalKeyboard = createPostGameKeyboard(GAME_IDS.DICE_ESCALATOR, Number(betAmount));
  // Add jackpot button to final keyboard
  finalKeyboard.inline_keyboard.unshift([{ text: jackpotButtonTextBotTurn, callback_data: `jackpot_display_noop:${gameId}` }]);
  finalKeyboard.inline_keyboard.push([{ text: `📜 ${escapeMarkdownV2(gameData.type.replace('_',' '))} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_ESCALATOR}` }])


  // console.log(`${LOG_PREFIX_DE_BOT_TURN} Composed final message for game ${gameId}.`);

  if (gameData.gameMessageId && bot) {
    try {
        await bot.editMessageText(botMessageAccumulator, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: finalKeyboard
        });
    } catch (e) {
        // console.warn(`${LOG_PREFIX_DE_BOT_TURN} Failed to edit final message, sending new: ${e.message}`);
        await safeSendMessage(String(chatId), botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
    }
  } else { // Should not happen if previous messages were successful
    await safeSendMessage(String(chatId), botMessageAccumulator, { parse_mode: 'MarkdownV2', reply_markup: finalKeyboard });
  }

  activeGames.delete(gameId);
  // console.log(`${LOG_PREFIX_DE_BOT_TURN} Game ${gameId} concluded & deleted from activeGames.`);
}


console.log("Part 5a (Segment 5): Dice Escalator Game Logic (Stand, Bot Turn, Jackpot) - Complete.");
// --- End of Part 5a (Segment 5 of N) ---
// --- Start of Part 5a (Segment 6 of N) ---
// index.js - Part 5a: Dice 21 (Blackjack Style) Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 6): Dice 21 (Blackjack Style) Game Logic...");

// --- DICE 21 GAME LOGIC (Full Implementation with Delete-and-Resend Strategy, Casino Feel & Meticulous Escaping) ---

async function handleStartDice21Command(chatId, userObj, betAmount, originalCommandMessageId) {
  const LOG_PREFIX_D21_START = `[D21_Start UID:${userObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_D21_START} Initiating Dice 21. Bet: ${betAmount}`);

  if (!userObj || typeof userObj.userId === 'undefined') {
    console.error(`${LOG_PREFIX_D21_START} CRITICAL: User object or userId undefined.`);
    await safeSendMessage(chatId, "An internal error occurred with your player profile. Please try again.", {});
    return;
  }
  const userId = String(userObj.userId);
  const playerRef = getPlayerDisplayReference(userObj);
  const gameId = generateGameId(GAME_IDS.DICE_21);

  if (userObj.balanceLamports < BigInt(betAmount)) {
    const needed = formatCurrency(Number(betAmount) - userObj.balance, "credits");
    await safeSendMessage(chatId, `${playerRef}, your balance of ${escapeMarkdownV2(formatCurrency(userObj.balance, "credits"))} is too low for a *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* Dice 21 bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const balanceUpdateResult = await updateUserBalance(userId, -BigInt(betAmount), `bet_placed_dice21:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult || !balanceUpdateResult.success) {
    console.error(`${LOG_PREFIX_D21_START} Wager placement failed: ${balanceUpdateResult.error}`);
    await safeSendMessage(chatId, `${playerRef}, your Dice 21 wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Please check your balance or try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }
  // console.log(`${LOG_PREFIX_D21_START} Wager ${betAmount} accepted. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  // Initial "Dealing" message, to be deleted/edited shortly
  let dealingMsg = await safeSendMessage(chatId, `🃏 Welcome to the **Dice 21** table, ${playerRef}\\! 🃏\nYour wager: *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}*\\.\nShuffling the dice and dealing your initial hand\\.\\.\\. please wait\\.`, { parse_mode: 'MarkdownV2' });
  await sleep(1500);

  let initialPlayerRollsValues = [];
  let playerScore = 0n;
  const diceToDeal = 2;
  let animatedDiceMessageIds = []; // To store IDs of animated dice for deletion

  for (let i = 0; i < diceToDeal; i++) {
    try {
      if (!bot || typeof bot.sendDice !== 'function') throw new Error("bot.sendDice unavailable.");
      const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
      if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') {
        throw new Error("Invalid dice message response from API for player initial deal.");
      }
      initialPlayerRollsValues.push(diceMsg.dice.value);
      playerScore += BigInt(diceMsg.dice.value);
      animatedDiceMessageIds.push(diceMsg.message_id);
      // console.log(`${LOG_PREFIX_D21_START} Player initial die ${i + 1} was ${diceMsg.dice.value}. Current score: ${playerScore}. Pausing.`);
      await sleep(2000); // Pause to let user see each die
    } catch (e) {
      console.error(`${LOG_PREFIX_D21_START} sendDice error for player initial die ${i + 1}: ${e.message}. Using internal roll.`, e.stack);
      const internalRoll = rollDie(); // From Part 3
      initialPlayerRollsValues.push(internalRoll);
      playerScore += BigInt(internalRoll);
      await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Internal Roll for initial die ${i+1}): You received a *${escapeMarkdownV2(String(internalRoll))}* 🎲`, { parse_mode: 'MarkdownV2' });
      await sleep(1000);
    }
  }
  // console.log(`${LOG_PREFIX_D21_START} Player initial dice dealt. Hand: [${initialPlayerRollsValues.join(', ')}], Score: ${playerScore}.`);

  // Delete the initial "Dealing..." message and animated dice messages
  if (dealingMsg && dealingMsg.message_id && bot) {
    bot.deleteMessage(String(chatId), dealingMsg.message_id).catch(() => {});
  }
  animatedDiceMessageIds.forEach(id => {
      if (bot) bot.deleteMessage(String(chatId), id).catch(() => {});
  });

  const gameData = {
    type: GAME_IDS.DICE_21, gameId, chatId: String(chatId), userId, playerRef, userObj, // Store full userObj
    betAmount: BigInt(betAmount), playerScore, botScore: 0n,
    playerHandRolls: [...initialPlayerRollsValues], // Store all player rolls (initial + hits)
    botHandRolls: [],
    status: 'player_turn', // Player's turn to hit or stand
    gameMessageId: null, // This will be set by the first interactive game status message
    lastInteractionTime: Date.now()
  };

  let messageText = `🃏 **Dice 21 Table** vs\\. Bot Dealer 🤖\n${playerRef}, your wager: *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}*\n\n`;
  messageText += `Your initial hand: ${formatDiceRolls(initialPlayerRollsValues)} totaling *${escapeMarkdownV2(String(playerScore))}*\\.\n`;

  let buttons = [];
  const targetScoreD21 = BigInt(DICE_21_TARGET_SCORE); // From Part 1

  if (playerScore > targetScoreD21) {
    messageText += `\n💥 **BUST!** Your score of *${escapeMarkdownV2(String(playerScore))}* is over ${escapeMarkdownV2(String(targetScoreD21))}\\. The house claims your wager\\.`;
    gameData.status = 'game_over_player_bust';
    await updateUserBalance(userId, 0n, `lost_dice21_deal_bust:${gameId}`, null, gameId, String(chatId)); // Log loss
    const userForBalanceDisplay = await getUser(userId);
    messageText += `\n\nYour new balance: *${escapeMarkdownV2(formatCurrency(userForBalanceDisplay.balanceLamports, "credits"))}*\\.`;
    buttons = createPostGameKeyboard(GAME_IDS.DICE_21, Number(betAmount)).inline_keyboard[0]; // Get only the first row of buttons
  } else if (playerScore === targetScoreD21) {
    messageText += `\n✨ **BLACKJACK!** A perfect *${escapeMarkdownV2(String(targetScoreD21))}* on the deal\\! You automatically stand\\. The Bot Dealer will now play\\.\\.\\.`;
    gameData.status = 'bot_turn_pending'; // Bot will play next
    // No buttons here, will proceed to bot turn
  } else {
    messageText += `\nYour move, ${playerRef}\\: "Hit" for another die, or "Stand"\\?`;
    buttons.push({ text: "⤵️ Hit", callback_data: `d21_hit:${gameId}` });
    buttons.push({ text: `✅ Stand at ${playerScore}`, callback_data: `d21_stand:${gameId}` });
  }
   buttons.push({ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` });


  const gameMessageOptions = { parse_mode: 'MarkdownV2', reply_markup: buttons.length > 0 ? { inline_keyboard: [buttons] } : {} };
  const sentGameMsg = await safeSendMessage(chatId, messageText, gameMessageOptions);

  if (sentGameMsg && sentGameMsg.message_id) {
    gameData.gameMessageId = sentGameMsg.message_id;
    // console.log(`${LOG_PREFIX_D21_START} First interactive game message ${gameData.gameMessageId} sent for ${gameId}.`);
  } else {
    console.error(`${LOG_PREFIX_D21_START} Failed to send first interactive game message for ${gameId}. Refunding.`);
    await updateUserBalance(userId, BigInt(betAmount), `refund_dice21_setup_msg_fail:${gameId}`, null, gameId, String(chatId));
    activeGames.delete(gameId); // Clean up
    return;
  }
  activeGames.set(gameId, gameData); // Save game state

  if (gameData.status === 'bot_turn_pending') {
    await sleep(2000); // Pause before bot turn
    await processDice21BotTurn(gameData, gameData.gameMessageId); // Pass current message ID
  } else if (gameData.status.startsWith('game_over')) {
    activeGames.delete(gameId); // Game ended on deal (bust)
  }
  // console.log(`${LOG_PREFIX_D21_START} Exiting start command handler.`);
}


async function handleDice21Hit(gameId, userObj, originalMessageIdFromCallback) {
  const LOG_PREFIX_D21_HIT = `[D21_Hit GID:${gameId} UID:${userObj.userId}]`;
  // console.log(`${LOG_PREFIX_D21_HIT} Player 'Hit'. ClickedMsgID: ${originalMessageIdFromCallback}`);

  const gameData = activeGames.get(gameId);
  if (!gameData || gameData.userId !== String(userObj.userId) || gameData.status !== 'player_turn' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
    // console.warn(`${LOG_PREFIX_D21_HIT} Invalid hit: game not found, wrong user/status, or stale message. Status: ${gameData?.status}, GameMsgID: ${gameData?.gameMessageId}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "It's not your turn or this game action is outdated.", show_alert: true });
    if (originalMessageIdFromCallback && bot && gameData && gameData.chatId) { // Attempt to clear buttons on the stale message clicked
        bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
    }
    return;
  }

  const chatId = gameData.chatId;
  const previousGameMessageId = gameData.gameMessageId; // ID of the message we will delete

  // Update previous message to "Rolling..." before deleting
  if (previousGameMessageId && bot) {
    try {
      await bot.editMessageText(`${gameData.playerRef} is drawing another die\\.\\.\\. 🎲\nCurrent score: *${escapeMarkdownV2(String(gameData.playerScore))}*`, {
          chat_id: String(chatId), message_id: Number(previousGameMessageId), parse_mode: 'MarkdownV2', reply_markup: {} // Clear buttons
      });
    } catch (editError) { /* console.warn(`${LOG_PREFIX_D21_HIT} Non-fatal: Failed to edit prev msg ${previousGameMessageId} to 'rolling...': ${editError.message}`); */ }
  }
  await sleep(700); // Short pause

  let newRollValue;
  let animatedDiceMessageIdHit = null;
  try {
    const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
    if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') throw new Error("Invalid dice API response for player hit.");
    newRollValue = BigInt(diceMsg.dice.value);
    animatedDiceMessageIdHit = diceMsg.message_id;
    // console.log(`${LOG_PREFIX_D21_HIT} Player hit ${newRollValue} (animated). Pausing.`);
    await sleep(2000);
  } catch (e) {
    console.error(`${LOG_PREFIX_D21_HIT} sendDice error for hit: ${e.message}. Using internal roll.`, e.stack);
    newRollValue = BigInt(rollDie());
    await safeSendMessage(String(chatId), `⚙️ ${gameData.playerRef} (Internal Roll for hit): You drew a *${escapeMarkdownV2(String(newRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' });
    await sleep(1000);
  }

  // Delete the "Rolling..." message and the animated dice message
  if (previousGameMessageId && bot) {
    bot.deleteMessage(String(chatId), Number(previousGameMessageId)).catch(() => {});
  }
  if (animatedDiceMessageIdHit && bot) {
    bot.deleteMessage(String(chatId), animatedDiceMessageIdHit).catch(() => {});
  }

  gameData.playerHandRolls.push(Number(newRollValue)); // Store as number for formatDiceRolls
  gameData.playerScore += newRollValue;
  // console.log(`${LOG_PREFIX_D21_HIT} Player new score: ${gameData.playerScore}. Hand: [${gameData.playerHandRolls.join(', ')}]`);

  let newMainMessageText = `🃏 **Dice 21 Table** vs\\. Bot Dealer 🤖\n${gameData.playerRef}, your wager: *${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"))}*\n\n`;
  newMainMessageText += `You drew a ${formatDiceRolls([Number(newRollValue)])}\\.\nYour hand is now: ${formatDiceRolls(gameData.playerHandRolls)} totaling *${escapeMarkdownV2(String(gameData.playerScore))}*\\.\n`;
  let buttons = [];
  let gameEndedThisTurn = false;
  const targetScoreD21 = BigInt(DICE_21_TARGET_SCORE);

  if (gameData.playerScore > targetScoreD21) {
    newMainMessageText += `\n💥 **BUST!** Score over ${escapeMarkdownV2(String(targetScoreD21))}\\. The house claims your wager\\.`;
    gameData.status = 'game_over_player_bust'; gameEndedThisTurn = true;
    await updateUserBalance(gameData.userId, 0n, `lost_dice21_hit_bust:${gameId}`, null, gameId, String(chatId)); // Log loss
    const userForBalanceDisplay = await getUser(gameData.userId);
    newMainMessageText += `\n\nYour new balance: *${escapeMarkdownV2(formatCurrency(userForBalanceDisplay.balanceLamports, "credits"))}*\\.`;
    buttons = createPostGameKeyboard(GAME_IDS.DICE_21, Number(gameData.betAmount)).inline_keyboard[0];
  } else if (gameData.playerScore === targetScoreD21) {
    newMainMessageText += `\n✨ **PERFECT ${escapeMarkdownV2(String(targetScoreD21))}!** You automatically stand\\. The Bot Dealer plays next\\.\\.\\.`;
    gameData.status = 'bot_turn_pending'; gameEndedThisTurn = true;
  } else { // Player turn continues
    newMainMessageText += `\nYour move, ${gameData.playerRef}\\: "Hit" or "Stand"\\?`;
    buttons.push({ text: "⤵️ Hit", callback_data: `d21_hit:${gameId}` });
    buttons.push({ text: `✅ Stand at ${gameData.playerScore}`, callback_data: `d21_stand:${gameId}` });
  }
   buttons.push({ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` });


  const newGameMessageOptions = { parse_mode: 'MarkdownV2', reply_markup: buttons.length > 0 ? { inline_keyboard: [buttons] } : {} };
  const sentNewMsg = await safeSendMessage(chatId, newMainMessageText, newGameMessageOptions);

  if (sentNewMsg && sentNewMsg.message_id) {
    gameData.gameMessageId = sentNewMsg.message_id; // Update to the new message ID
    // console.log(`${LOG_PREFIX_D21_HIT} NEW game message ${gameData.gameMessageId} sent.`);
  } else {
    console.error(`${LOG_PREFIX_D21_HIT} CRITICAL: Failed to send NEW game message for ${gameId} after hit. Game may be stuck.`);
    await safeSendMessage(String(chatId), "A display error occurred. Please try starting a new game if issues persist.", {});
    activeGames.delete(gameId); return; // Avoid further processing
  }
  activeGames.set(gameId, gameData);

  if (gameEndedThisTurn) {
    if (gameData.status === 'bot_turn_pending') {
      await sleep(2000);
      await processDice21BotTurn(gameData, gameData.gameMessageId);
    } else if (gameData.status.startsWith('game_over')) {
      activeGames.delete(gameId); // Game ended due to bust
    }
  }
  // console.log(`${LOG_PREFIX_D21_HIT} Exiting hit handler.`);
}


async function handleDice21Stand(gameId, userObj, originalMessageIdFromCallback) {
  const LOG_PREFIX_D21_STAND = `[D21_Stand GID:${gameId} UID:${userObj.userId}]`;
  // console.log(`${LOG_PREFIX_D21_STAND} Player 'Stand'. ClickedMsgID: ${originalMessageIdFromCallback}`);

  const gameData = activeGames.get(gameId);
  if (!gameData || gameData.userId !== String(userObj.userId) || gameData.status !== 'player_turn' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
    // console.warn(`${LOG_PREFIX_D21_STAND} Invalid stand: game/user/status mismatch or stale message. Status: ${gameData?.status}, GameMsgID: ${gameData?.gameMessageId}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Not your turn or this game action is outdated.", show_alert: true });
     if (originalMessageIdFromCallback && bot && gameData && gameData.chatId) {
        bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
    }
    return;
  }

  const chatId = gameData.chatId;
  const previousGameMessageId = gameData.gameMessageId;

  gameData.status = 'bot_turn_pending'; // Mark status before async ops
  activeGames.set(gameId, gameData); // Update activeGames with new status
  // console.log(`${LOG_PREFIX_D21_STAND} Player stands with score ${gameData.playerScore}. Status -> 'bot_turn_pending'.`);

  // Delete the previous game message (with Hit/Stand buttons)
  if (previousGameMessageId && bot) {
    bot.deleteMessage(String(chatId), Number(previousGameMessageId)).catch(() => {});
  }

  const standMessageText = `🃏 **Dice 21 Table** 🃏\n${gameData.playerRef} stands with a score of *${escapeMarkdownV2(String(gameData.playerScore))}*\\.\n\nThe Bot Dealer 🤖 takes a moment, then prepares to play its hand\\.\\.\\.`;
  const sentNewStandMsg = await safeSendMessage(chatId, standMessageText, { parse_mode: 'MarkdownV2' }); // No buttons needed here

  if (sentNewStandMsg && sentNewStandMsg.message_id) {
    gameData.gameMessageId = sentNewStandMsg.message_id; // Update with the NEW message ID
    activeGames.set(gameId, gameData); // Save again with new message ID
    // console.log(`${LOG_PREFIX_D21_STAND} NEW 'stand notification' message ${gameData.gameMessageId} sent.`);
  } else {
    console.error(`${LOG_PREFIX_D21_STAND} CRITICAL: Failed to send NEW 'stand notification' message for ${gameId}. Game may be stuck.`);
    await safeSendMessage(String(chatId), "A display error occurred transitioning to the bot's turn. Please try starting a new game.", {});
    activeGames.delete(gameId); return;
  }

  await sleep(2000); // Dramatic pause
  await processDice21BotTurn(gameData, gameData.gameMessageId); // Pass the new message ID for the bot turn to manage
  // console.log(`${LOG_PREFIX_D21_STAND} Exiting stand handler.`);
}

async function processDice21BotTurn(gameData, initialBotTurnMessageId) {
  const LOG_PREFIX_D21_BOT = `[D21_BotTurn GID:${gameData.gameId}]`;
  // console.log(`${LOG_PREFIX_D21_BOT} Bot's turn begins. Player Score: ${gameData.playerScore}. Initial MsgID for bot's turn: ${initialBotTurnMessageId}.`);

  const { gameId, chatId, userId, playerRef, playerScore, betAmount, userObj } = gameData;
  gameData.status = 'bot_rolling';
  // gameData.botScore is already 0n from gameData initialization or reset if this function is re-entrant (it's not currently)
  let botBusted = false;
  // gameData.botHandRolls is already []

  // This is the ID of the message currently displaying the bot's turn progress.
  // It starts as the message passed from handleDice21Stand or initial Blackjack deal.
  let currentBotStatusMessageId = Number(initialBotTurnMessageId);

  let initialBotTurnMessageText = `🃏 **Dice 21 Table** 🃏\n${playerRef} stands at *${escapeMarkdownV2(String(playerScore))}*\\.\n\nBot Dealer's turn\\! 🤖 Preparing to draw its first dice\\.\\.\\.`;

  if (currentBotStatusMessageId && bot) {
    try {
      await bot.editMessageText(initialBotTurnMessageText, {
        chat_id: String(chatId), message_id: currentBotStatusMessageId, parse_mode: 'MarkdownV2', reply_markup: {} // No buttons during bot's active play
      });
      // console.log(`${LOG_PREFIX_D21_BOT} Edited message ${currentBotStatusMessageId} to show bot is starting its turn.`);
    } catch (e) {
      // console.warn(`${LOG_PREFIX_D21_BOT} Failed to edit initial bot turn message ${currentBotStatusMessageId}: ${e.message}. It might have been deleted. Sending new one.`);
      const newMsg = await safeSendMessage(String(chatId), initialBotTurnMessageText, { parse_mode: 'MarkdownV2' });
      if (newMsg && newMsg.message_id) currentBotStatusMessageId = newMsg.message_id;
      else {
        console.error(`${LOG_PREFIX_D21_BOT} CRITICAL: Could not send/edit initial bot turn message. Aborting bot turn.`);
        await safeSendMessage(String(chatId), `A display error occurred during the bot's turn. Please start a new game.`);
        activeGames.delete(gameId); return;
      }
    }
  } else { // Should only happen if no messageId was passed correctly
    const newMsg = await safeSendMessage(String(chatId), initialBotTurnMessageText, { parse_mode: 'MarkdownV2' });
    if (newMsg && newMsg.message_id) currentBotStatusMessageId = newMsg.message_id;
    else {
      console.error(`${LOG_PREFIX_D21_BOT} CRITICAL: Could not send initial bot turn message. Aborting bot turn.`);
      activeGames.delete(gameId); return;
    }
  }
  gameData.gameMessageId = currentBotStatusMessageId; // Store the latest main message ID
  activeGames.set(gameId, gameData);
  await sleep(1500); // Pause for "preparing to draw"

  const botStandScoreThreshold = BigInt(DICE_21_BOT_STAND_SCORE); // From Part 1
  const targetScoreD21 = BigInt(DICE_21_TARGET_SCORE);       // From Part 1
  const maxBotRolls = 7; // Safety break for bot rolls to prevent infinite loops in weird edge cases
  let animatedDiceMsgIdBot = null; // For bot's animated dice

  for (let i = 0; i < maxBotRolls && gameData.botScore < botStandScoreThreshold && !botBusted; i++) {
    // console.log(`${LOG_PREFIX_D21_BOT} Bot rolling die ${i + 1}. Bot score: ${gameData.botScore}.`);

    // Delete previous animated dice and status message
    if (currentBotStatusMessageId && bot) {
        bot.deleteMessage(String(chatId), currentBotStatusMessageId).catch(() => {});
    }
    if (animatedDiceMsgIdBot && bot) {
        bot.deleteMessage(String(chatId), animatedDiceMsgIdBot).catch(() => {});
        animatedDiceMsgIdBot = null;
    }

    // Send "Bot is rolling..." message before sending the actual die
    let tempRollingMsg = await safeSendMessage(String(chatId), `Bot Dealer is rolling die #${i+1}${ escapeMarkdownV2("...")}`, {parse_mode: 'MarkdownV2'});
    await sleep(500);


    let currentRollValue;
    try {
      const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
      if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') throw new Error("Invalid dice API response for bot.");
      currentRollValue = BigInt(diceMsg.dice.value);
      animatedDiceMsgIdBot = diceMsg.message_id;
      // console.log(`${LOG_PREFIX_D21_BOT} Bot drew ${currentRollValue} (animated). Pausing.`);
      await sleep(2000); // Let user see the die
    } catch (e) {
      console.error(`${LOG_PREFIX_D21_BOT} sendDice error for bot roll ${i+1}: ${e.message}. Checking for rate limit or using internal.`, e.stack);
      if (e.message && e.message.includes("429 Too Many Requests")) {
        const retryAfterMatch = e.message.match(/retry after (\d+)/i);
        const retryAfter = retryAfterMatch ? parseInt(retryAfterMatch[1], 10) : 5; // Default 5s
        console.warn(`${LOG_PREFIX_D21_BOT} Rate limit hit. Retrying sendDice for bot after ${retryAfter} seconds.`);
        if (tempRollingMsg && tempRollingMsg.message_id && bot) { // Edit the "Bot is rolling..." message
            bot.editMessageText(`Bot Dealer is taking a moment due to high table activity (will retry in ${retryAfter}s)${escapeMarkdownV2("...")}`, {chat_id: String(chatId), message_id: tempRollingMsg.message_id, parse_mode:'MarkdownV2'}).catch(()=>{});
        }
        await sleep((retryAfter + 1) * 1000); // Wait an extra second
        i--; // Decrement i to retry the current roll
        if (tempRollingMsg && tempRollingMsg.message_id && bot) bot.deleteMessage(String(chatId), tempRollingMsg.message_id).catch(()=>{}); // clean up temp msg
        continue; // Skip to next iteration to retry sendDice
      }
      // Fallback to internal roll if not a rate limit error or if retry also fails (though retry logic isn't set up for multi-attempt here)
      currentRollValue = BigInt(rollDie());
      gameData.botHandRolls.push(Number(currentRollValue)); // Add to hand for final display
      await safeSendMessage(String(chatId), `⚙️ Bot Dealer (Internal Roll): Reveals a *${escapeMarkdownV2(String(currentRollValue))}* 🎲`, { parse_mode: 'MarkdownV2' });
      await sleep(1000);
    }

    if (tempRollingMsg && tempRollingMsg.message_id && bot) bot.deleteMessage(String(chatId), tempRollingMsg.message_id).catch(()=>{}); // clean up temp msg

    gameData.botScore += currentRollValue;
    gameData.botHandRolls.push(Number(currentRollValue)); // Add even if animated for final hand display
    activeGames.set(gameId, gameData); // Save score progression

    // Compose NEW bot status message for this roll
    let botTurnInProgressMessage = `🃏 **Dice 21 Table** 🃏\n${playerRef}'s score: *${escapeMarkdownV2(String(playerScore))}*\\.\n\n`;
    botTurnInProgressMessage += `Bot Dealer's hand: ${formatDiceRolls(gameData.botHandRolls)} (Total: *${escapeMarkdownV2(String(gameData.botScore))}*)${escapeMarkdownV2(".")}\n`;
    // console.log(`${LOG_PREFIX_D21_BOT} Bot score now ${gameData.botScore}. Hand: [${gameData.botHandRolls.join(', ')}].`);

    let nextActionFeedback = "";
    if (gameData.botScore > targetScoreD21) {
      botBusted = true;
      nextActionFeedback = `\n💥 **BOT BUSTED!** Score over ${escapeMarkdownV2(String(targetScoreD21))}\\!`;
    } else if (gameData.botScore >= botStandScoreThreshold) {
      nextActionFeedback = `\nBot Dealer stands with *${escapeMarkdownV2(String(gameData.botScore))}*\\.`;
    } else if (i < maxBotRolls -1){ // Only show "draws another" if not the last possible roll in loop
      nextActionFeedback = `\n_The dealer draws another die\\.\\._ 🎲`;
    }
    botTurnInProgressMessage += nextActionFeedback;

    const sentNewBotStatusMsg = await safeSendMessage(String(chatId), botTurnInProgressMessage, { parse_mode: 'MarkdownV2' });
    if (sentNewBotStatusMsg && sentNewBotStatusMsg.message_id) {
      currentBotStatusMessageId = sentNewBotStatusMsg.message_id;
      gameData.gameMessageId = currentBotStatusMessageId;
      activeGames.set(gameId, gameData); // Save new message ID
      // console.log(`${LOG_PREFIX_D21_BOT} NEW bot status message ${currentBotStatusMessageId} sent.`);
    } else {
      console.error(`${LOG_PREFIX_D21_BOT} CRITICAL: Failed to send NEW bot status message. Bot turn might be visually stuck.`);
      // Attempt to recover by sending final result directly, though game state might be partially shown.
      break; // Exit loop and proceed to result calculation
    }

    if (botBusted || gameData.botScore >= botStandScoreThreshold) break; // Exit main rolling loop
    await sleep(2500); // Pause between bot actions/rolls if it continues
  }
  await sleep(1500); // Final pause before results

  // Delete the last bot turn progress message and any lingering animated dice
  if (currentBotStatusMessageId && bot) {
    bot.deleteMessage(String(chatId), currentBotStatusMessageId).catch(() => {});
  }
   if (animatedDiceMsgIdBot && bot) {
    bot.deleteMessage(String(chatId), animatedDiceMsgIdBot).catch(() => {});
  }


  // --- Final Result Calculation and Message ---
  let resultTextEnd = "";
  let payoutAmount = 0n;
  let outcomeReasonLog = "";

  if (botBusted) {
    resultTextEnd = `🎉 ${playerRef} **WINS!** The Bot Dealer busted\\. A fine victory\\!`;
    payoutAmount = betAmount + betAmount; // Bet back + 1x profit
    outcomeReasonLog = `won_dice21_bot_bust:${gameId}`;
  } else if (playerScore > gameData.botScore) {
    resultTextEnd = `🎉 ${playerRef} **WINS** with a superior hand (*${escapeMarkdownV2(String(playerScore))}* vs\\. Bot's *${escapeMarkdownV2(String(gameData.botScore))}*)\\!`;
    payoutAmount = betAmount + betAmount;
    outcomeReasonLog = `won_dice21_score:${gameId}`;
  } else if (gameData.botScore > playerScore) {
    resultTextEnd = `💀 The **Bot Dealer wins** this round (*${escapeMarkdownV2(String(gameData.botScore))}* vs\\. your *${escapeMarkdownV2(String(playerScore))}*)\\. Better luck next time\\!`;
    payoutAmount = 0n; // Bet lost
    outcomeReasonLog = `lost_dice21_score:${gameId}`;
  } else { // Push (Tie)
    resultTextEnd = `😐 A **PUSH!** Both players have *${escapeMarkdownV2(String(playerScore))}*\\. Your wager of *${escapeMarkdownV2(formatCurrency(Number(betAmount)))}* is returned\\.`;
    payoutAmount = betAmount; // Bet back
    outcomeReasonLog = `push_dice21:${gameId}`;
  }
  // console.log(`${LOG_PREFIX_D21_BOT} Game outcome: ${resultTextEnd}. Payout: ${payoutAmount}, Reason: ${outcomeReasonLog}.`);

  let finalSummaryMessage = `🃏 **Dice 21 - Final Result** 🃏\nOriginal Wager: *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}*\n\n`;
  finalSummaryMessage += `${playerRef}'s final hand: ${formatDiceRolls(gameData.playerHandRolls)} (Score: *${escapeMarkdownV2(String(playerScore))}*)\n`;
  finalSummaryMessage += `Bot Dealer's final hand: ${formatDiceRolls(gameData.botHandRolls)} (Score: *${escapeMarkdownV2(String(gameData.botScore))}*)${escapeMarkdownV2(botBusted ? " - BUSTED!" : ".")}\n\n`;
  finalSummaryMessage += `${resultTextEnd}`;

  let finalUserBalanceForDisplay = userObj.balanceLamports;

  if (payoutAmount > 0n || outcomeReasonLog.startsWith('lost_')) {
    const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    if (balanceUpdate.success) {
      finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports;
      // Profit message already incorporated in resultTextEnd implicitly or explicitly
    } else {
      finalSummaryMessage += `\n\n⚠️ There was an issue settling your bet: ${escapeMarkdownV2(balanceUpdate.error || "N/A")}. Admin has been notified.`;
      console.error(`${LOG_PREFIX_D21_BOT} Failed to update balance for D21 outcome for ${userId}. Error: ${balanceUpdate.error}`);
    }
  }
  finalSummaryMessage += `\n\n${playerRef}'s new balance: *${escapeMarkdownV2(formatCurrency(finalUserBalanceForDisplay, "credits"))}*\\.`;

  const postGameKeyboardD21 = createPostGameKeyboard(GAME_IDS.DICE_21, Number(gameData.betAmount));
   postGameKeyboardD21.inline_keyboard.push([{ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DICE_21}` }])


  const finalSentMsg = await safeSendMessage(String(chatId), finalSummaryMessage, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardD21 });
  if (finalSentMsg && finalSentMsg.message_id) {
    gameData.gameMessageId = finalSentMsg.message_id; // Update with final message ID
    // console.log(`${LOG_PREFIX_D21_BOT} Final result message sent, ID: ${finalSentMsg.message_id}.`);
  } else {
    console.error(`${LOG_PREFIX_D21_BOT} CRITICAL: Failed to send final D21 result message.`);
  }

  activeGames.delete(gameId); // Game concluded
  // console.log(`${LOG_PREFIX_D21_BOT} Game ${gameId} concluded and removed. Player: ${playerScore}, Bot: ${gameData.botScore}, BotBusted: ${botBusted}.`);
}


console.log("Part 5a (Segment 6): Dice 21 (Blackjack Style) Game Logic - Complete.");
// --- End of Part 5a (Segment 6 of N) ---
// --- Start of Part 5a (Segment 7 of N) ---
// index.js - Part 5a: Over/Under 7 Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 7): Over/Under 7 Game Logic...");

// --- OVER/UNDER 7 GAME LOGIC ---

async function handleStartOverUnder7Command(chatId, userObj, betAmount, originalCommandMessageId) {
  const LOG_PREFIX_OU7_START = `[OU7_Start UID:${userObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_OU7_START} Initiating Over/Under 7. Bet: ${betAmount}`);

  if (!userObj || typeof userObj.userId === 'undefined') {
    console.error(`${LOG_PREFIX_OU7_START} CRITICAL: User object or userId undefined.`);
    await safeSendMessage(chatId, "An internal error occurred with your player profile. Please try again.", {});
    return;
  }
  const userId = String(userObj.userId);
  const playerRef = getPlayerDisplayReference(userObj);
  const gameId = generateGameId(GAME_IDS.OVER_UNDER_7);

  if (userObj.balanceLamports < BigInt(betAmount)) {
    const needed = formatCurrency(Number(betAmount) - userObj.balance, "credits");
    await safeSendMessage(chatId, `${playerRef}, your balance of ${escapeMarkdownV2(formatCurrency(userObj.balance, "credits"))} is too low for an *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* Over/Under 7 bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  // Pre-deduct bet, will be refunded if setup fails or game not played.
  const balanceUpdateResult = await updateUserBalance(userId, -BigInt(betAmount), `bet_placed_ou7_init:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult || !balanceUpdateResult.success) {
    console.error(`${LOG_PREFIX_OU7_START} Wager placement failed: ${balanceUpdateResult.error}`);
    await safeSendMessage(chatId, `${playerRef}, your Over/Under 7 wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Please check your balance or try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }
  // console.log(`${LOG_PREFIX_OU7_START} Wager ${betAmount} accepted. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  const gameData = {
    type: GAME_IDS.OVER_UNDER_7, gameId, chatId: String(chatId), userId, playerRef, userObj,
    betAmount: BigInt(betAmount),
    playerChoice: null, // 'over', 'under', 'seven'
    diceRolls: [],
    diceSum: null,
    status: 'waiting_player_choice', // Player needs to choose their bet type
    gameMessageId: null,
    lastInteractionTime: Date.now()
  };
  activeGames.set(gameId, gameData);
  // console.log(`${LOG_PREFIX_OU7_START} Game ${gameId} initiated. Data: ${stringifyWithBigInt(gameData)}`);

  const initialMessageText = `🎲 **Over/Under 7 Challenge!** 🎲\n\n${playerRef}, you've placed a bet of *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}*\\.\n\nPredict the sum of *${OU7_DICE_COUNT}* dice:`;
  const keyboard = {
    inline_keyboard: [
      [
        { text: "📉 Under 7 (2-6)", callback_data: `ou7_choice:${gameId}:under` },
        { text: "🎯 Exactly 7", callback_data: `ou7_choice:${gameId}:seven` },
        { text: "📈 Over 7 (8-12)", callback_data: `ou7_choice:${gameId}:over` }
      ],
      [{ text: `📜 ${escapeMarkdownV2(gameData.type.replace('_',' '))} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.OVER_UNDER_7}` }]
      // No cancel button for this type of quick game, player commits by choosing.
    ]
  };

  const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

  if (sentMessage && sentMessage.message_id) {
    gameData.gameMessageId = sentMessage.message_id;
    activeGames.set(gameId, gameData); // Update gameData with messageId
    // console.log(`${LOG_PREFIX_OU7_START} Game ${gameId} choice prompt sent. MessageID: ${sentMessage.message_id}.`);
  } else {
    console.error(`${LOG_PREFIX_OU7_START} Failed to send Over/Under 7 game message for ${gameId}. Refunding wager.`);
    // Bet was already deducted, so refund it.
    await updateUserBalance(userId, BigInt(betAmount), `refund_ou7_setup_fail:${gameId}`, null, gameId, String(chatId));
    activeGames.delete(gameId);
    // console.log(`${LOG_PREFIX_OU7_START} Wager refunded and game ${gameId} removed.`);
  }
  // console.log(`${LOG_PREFIX_OU7_START} Exiting start command handler.`);
}

async function handleOverUnder7Choice(gameId, choice, userObj, originalMessageIdFromCallback) {
  const LOG_PREFIX_OU7_CHOICE = `[OU7_Choice GID:${gameId} UID:${userObj.userId} Choice:${choice}]`;
  // console.log(`${LOG_PREFIX_OU7_CHOICE} Player made a choice.`);

  const gameData = activeGames.get(gameId);

  if (!gameData || gameData.userId !== String(userObj.userId) || gameData.status !== 'waiting_player_choice' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
    // console.warn(`${LOG_PREFIX_OU7_CHOICE} Invalid choice: game not found, wrong user/status, or stale message. Status: ${gameData?.status}, GameMsgID: ${gameData?.gameMessageId}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "This Over/Under 7 game action is outdated or not your turn.", show_alert: true });
    if (originalMessageIdFromCallback && bot && gameData && gameData.chatId) {
        bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
    }
    return;
  }

  const validChoices = ['under', 'seven', 'over'];
  if (!validChoices.includes(choice)) {
    console.error(`${LOG_PREFIX_OU7_CHOICE} Invalid choice parameter received: ${choice}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Invalid choice made.", show_alert: true });
    return;
  }

  gameData.playerChoice = choice;
  gameData.status = 'rolling_dice';
  activeGames.set(gameId, gameData);

  const { chatId, playerRef, betAmount } = gameData;

  // Update the message to show choice and "rolling..."
  const choiceTextDisplay = choice.charAt(0).toUpperCase() + choice.slice(1);
  let rollingMessageText = `🎲 **Over/Under 7** 🎲\n${playerRef} bets *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}* on the sum being *${escapeMarkdownV2(choiceTextDisplay)} 7*\\.\n\nRolling the dice now\\! Good luck\\!`;

  if (gameData.gameMessageId && bot) {
    try {
      await bot.editMessageText(rollingMessageText, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: {} // Clear buttons
      });
    } catch (e) { /* console.warn(...); Fall through, dice will appear as new messages */ }
  } else { // If somehow gameMessageId was lost, send a new one
    await safeSendMessage(String(chatId), rollingMessageText, { parse_mode: 'MarkdownV2' });
  }
  await sleep(1000); // Pause for effect

  // Roll the dice
  let diceRolls = [];
  let diceSum = 0;
  let animatedDiceMessageIdsOU7 = [];

  for (let i = 0; i < OU7_DICE_COUNT; i++) {
    try {
      const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
      if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') throw new Error("Invalid dice API response.");
      diceRolls.push(diceMsg.dice.value);
      diceSum += diceMsg.dice.value;
      animatedDiceMessageIdsOU7.push(diceMsg.message_id);
      await sleep(2000); // Pause between dice if multiple, or before result
    } catch (e) {
      console.error(`${LOG_PREFIX_OU7_CHOICE} sendDice error for die ${i + 1}: ${e.message}. Using internal roll.`, e.stack);
      const internalRoll = rollDie();
      diceRolls.push(internalRoll);
      diceSum += internalRoll;
      await safeSendMessage(String(chatId), `⚙️ (Internal Roll for die ${i+1}): A *${escapeMarkdownV2(String(internalRoll))}* 🎲 appears\\!`, { parse_mode: 'MarkdownV2' });
      await sleep(1000);
    }
  }
  gameData.diceRolls = diceRolls;
  gameData.diceSum = diceSum;
  gameData.status = 'game_over';
  activeGames.set(gameId, gameData); // Save rolls and sum

  // Delete animated dice messages
  animatedDiceMessageIdsOU7.forEach(id => {
      if (bot) bot.deleteMessage(String(chatId), id).catch(() => {});
  });

  // Determine outcome
  let win = false;
  let profitMultiplier = 0;
  if (choice === 'under' && diceSum < 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
  else if (choice === 'over' && diceSum > 7) { win = true; profitMultiplier = OU7_PAYOUT_NORMAL; }
  else if (choice === 'seven' && diceSum === 7) { win = true; profitMultiplier = OU7_PAYOUT_SEVEN; }

  let payoutAmount = 0n;
  let outcomeReasonLog = "";
  let resultTextPart = "";

  if (win) {
    payoutAmount = betAmount + (betAmount * BigInt(profitMultiplier)); // Bet back + profit
    outcomeReasonLog = `won_ou7_${choice}_sum${diceSum}:${gameId}`;
    resultTextPart = `🎉 **WINNER!** Your prediction of *${escapeMarkdownV2(choiceTextDisplay)} 7* was correct\\! You win *${escapeMarkdownV2(formatCurrency(Number(betAmount * BigInt(profitMultiplier)), "credits"))}* profit\\.`;
  } else {
    payoutAmount = 0n; // Bet lost (already deducted)
    outcomeReasonLog = `lost_ou7_${choice}_sum${diceSum}:${gameId}`;
    resultTextPart = `💔 **Better luck next time!** Your prediction of *${escapeMarkdownV2(choiceTextDisplay)} 7* was incorrect\\.`;
  }

  let finalUserBalanceForDisplay = userObj.balanceLamports; // Initial pre-bet balance if update fails
  const balanceUpdate = await updateUserBalance(userObj.userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
  if (balanceUpdate.success) {
    finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports;
  } else {
    resultTextPart += `\n\n⚠️ Error settling bet: ${escapeMarkdownV2(balanceUpdate.error || "N/A")}. Admin notified.`;
  }

  let finalMessageText = `🎲 **Over/Under 7 - Result** 🎲\nBet: *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}* on *${escapeMarkdownV2(choiceTextDisplay)} 7*\\.\n\n`;
  finalMessageText += `The dice rolled: ${formatDiceRolls(diceRolls)} = Sum of *${escapeMarkdownV2(String(diceSum))}*\\!\n\n`;
  finalMessageText += `${resultTextPart}`;
  finalMessageText += `\n\n${playerRef}'s new balance: *${escapeMarkdownV2(formatCurrency(finalUserBalanceForDisplay, "credits"))}*\\.`;

  const postGameKeyboardOU7 = createPostGameKeyboard(GAME_IDS.OVER_UNDER_7, Number(betAmount));
  postGameKeyboardOU7.inline_keyboard.push([{ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.OVER_UNDER_7}` }])


  // Edit the original game message (which was showing "rolling...") or send new if ID lost
  if (gameData.gameMessageId && bot) {
    try {
      await bot.editMessageText(finalMessageText, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7
      });
    } catch (e) {
      // console.warn(`${LOG_PREFIX_OU7_CHOICE} Failed to edit OU7 result message, sending new: ${e.message}`);
      await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
    }
  } else {
    await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardOU7 });
  }

  activeGames.delete(gameId); // Game is finished
  // console.log(`${LOG_PREFIX_OU7_CHOICE} Game ${gameId} concluded. Player chose ${choice}, sum was ${diceSum}. Win: ${win}.`);
}

console.log("Part 5a (Segment 7): Over/Under 7 Game Logic - Complete.");
// --- End of Part 5a (Segment 7 of N) ---
// --- Start of Part 5a (Segment 8 of N) ---
// index.js - Part 5a: High Roller Duel Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 8): High Roller Duel Game Logic...");

// --- HIGH ROLLER DUEL GAME LOGIC ---

async function handleStartDuelCommand(chatId, userObj, betAmount, originalCommandMessageId) {
  const LOG_PREFIX_DUEL_START = `[Duel_Start UID:${userObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_DUEL_START} Initiating High Roller Duel. Bet: ${betAmount}`);

  if (!userObj || typeof userObj.userId === 'undefined') {
    console.error(`${LOG_PREFIX_DUEL_START} CRITICAL: User object or userId undefined.`);
    await safeSendMessage(chatId, "An internal error occurred with your player profile. Please try again.", {});
    return;
  }
  const userId = String(userObj.userId);
  const playerRef = getPlayerDisplayReference(userObj);
  const gameId = generateGameId(GAME_IDS.DUEL);

  if (userObj.balanceLamports < BigInt(betAmount)) {
    const needed = formatCurrency(Number(betAmount) - userObj.balance, "credits");
    await safeSendMessage(chatId, `${playerRef}, your balance of ${escapeMarkdownV2(formatCurrency(userObj.balance, "credits"))} is too low for a *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* High Roller Duel bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const balanceUpdateResult = await updateUserBalance(userId, -BigInt(betAmount), `bet_placed_duel:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult || !balanceUpdateResult.success) {
    console.error(`${LOG_PREFIX_DUEL_START} Wager placement failed: ${balanceUpdateResult.error}`);
    await safeSendMessage(chatId, `${playerRef}, your Duel wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Please check your balance or try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }
  // console.log(`${LOG_PREFIX_DUEL_START} Wager ${betAmount} accepted. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  const gameData = {
    type: GAME_IDS.DUEL, gameId, chatId: String(chatId), userId, playerRef, userObj,
    betAmount: BigInt(betAmount),
    playerScore: 0, // Will be sum of DUEL_DICE_COUNT dice
    playerRolls: [],
    botScore: 0,    // Will be sum of DUEL_DICE_COUNT dice
    botRolls: [],
    status: 'player_turn_to_roll', // Player needs to click to roll
    gameMessageId: null,
    lastInteractionTime: Date.now()
  };
  activeGames.set(gameId, gameData);
  // console.log(`${LOG_PREFIX_DUEL_START} Game ${gameId} initiated. Data: ${stringifyWithBigInt(gameData)}`);

  const initialMessageText = `⚔️ **High Roller Duel Accepted!** ⚔️\n\n${playerRef}, you've wagered *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* against the Bot Dealer in a high\\-stakes dice duel\\!\n\nClick the button below to roll your *${escapeMarkdownV2(String(DUEL_DICE_COUNT))}* dice and set your score\\. May fortune favor your roll\\!`;
  const keyboard = {
    inline_keyboard: [
      [{ text: `🎲 Roll ${DUEL_DICE_COUNT} Dice!`, callback_data: `duel_roll:${gameId}` }],
      [{ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DUEL}` }]
    ]
  };

  const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: keyboard });

  if (sentMessage && sentMessage.message_id) {
    gameData.gameMessageId = sentMessage.message_id;
    activeGames.set(gameId, gameData); // Update gameData with messageId
    // console.log(`${LOG_PREFIX_DUEL_START} Game ${gameId} duel prompt sent. MessageID: ${sentMessage.message_id}.`);
  } else {
    console.error(`${LOG_PREFIX_DUEL_START} Failed to send Duel game message for ${gameId}. Refunding wager.`);
    await updateUserBalance(userId, BigInt(betAmount), `refund_duel_setup_fail:${gameId}`, null, gameId, String(chatId));
    activeGames.delete(gameId);
    // console.log(`${LOG_PREFIX_DUEL_START} Wager refunded and game ${gameId} removed.`);
  }
  // console.log(`${LOG_PREFIX_DUEL_START} Exiting start command handler.`);
}

async function handleDuelRoll(gameId, userObj, originalMessageIdFromCallback) {
  const LOG_PREFIX_DUEL_ROLL = `[Duel_Roll GID:${gameId} UID:${userObj.userId}]`;
  // console.log(`${LOG_PREFIX_DUEL_ROLL} Player initiated roll.`);

  const gameData = activeGames.get(gameId);

  if (!gameData || gameData.userId !== String(userObj.userId) || gameData.status !== 'player_turn_to_roll' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
    // console.warn(`${LOG_PREFIX_DUEL_ROLL} Invalid roll attempt: game/user/status mismatch or stale message. Status: ${gameData?.status}, GameMsgID: ${gameData?.gameMessageId}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Not your turn to roll or this Duel action is outdated.", show_alert: true });
    if (originalMessageIdFromCallback && bot && gameData && gameData.chatId) {
        bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
    }
    return;
  }

  gameData.status = 'player_rolling'; // Mark as player actively rolling
  activeGames.set(gameId, gameData);

  const { chatId, playerRef, betAmount } = gameData;

  // Update message to "Player is rolling..."
  let currentMessageText = `⚔️ **High Roller Duel!** ⚔️\n${playerRef} (Bet: *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}*) is rolling *${escapeMarkdownV2(String(DUEL_DICE_COUNT))}* dice\\.\\.\\.`;
  if (gameData.gameMessageId && bot) {
    try {
      await bot.editMessageText(currentMessageText, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: {} // Clear buttons
      });
    } catch (e) { /* console.warn(...); Fall through, dice will appear */ }
  }
  await sleep(1000);

  // Player's rolls
  let playerRolls = [];
  let playerScore = 0;
  let animatedDiceMessageIdsPlayer = [];

  for (let i = 0; i < DUEL_DICE_COUNT; i++) {
    try {
      const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
      if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') throw new Error("Invalid dice API for player.");
      playerRolls.push(diceMsg.dice.value);
      playerScore += diceMsg.dice.value;
      animatedDiceMessageIdsPlayer.push(diceMsg.message_id);
      await sleep(1800); // Pause to see each die
    } catch (e) {
      console.error(`${LOG_PREFIX_DUEL_ROLL} Player sendDice error die ${i + 1}: ${e.message}. Internal.`, e.stack);
      const internalRoll = rollDie();
      playerRolls.push(internalRoll);
      playerScore += internalRoll;
      await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Internal Roll ${i+1}/${DUEL_DICE_COUNT}): A *${escapeMarkdownV2(String(internalRoll))}* 🎲 rolls\\!`, { parse_mode: 'MarkdownV2' });
      await sleep(1000);
    }
  }
  gameData.playerRolls = playerRolls;
  gameData.playerScore = BigInt(playerScore);

  // Delete player's animated dice
  animatedDiceMessageIdsPlayer.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

  currentMessageText += `\n\n${playerRef} rolled: ${formatDiceRolls(playerRolls)} for a total of *${escapeMarkdownV2(String(playerScore))}*\\!`;
  currentMessageText += `\n\nNow, the Bot Dealer 🤖 rolls its *${escapeMarkdownV2(String(DUEL_DICE_COUNT))}* dice\\.\\.\\.`;
  if (gameData.gameMessageId && bot) {
    try {
      await bot.editMessageText(currentMessageText, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: {}
      });
    } catch (e) { /* console.warn(...); */ }
  }
  await sleep(1500);

  // Bot's rolls
  gameData.status = 'bot_rolling';
  activeGames.set(gameId, gameData);
  let botRolls = [];
  let botScore = 0;
  let animatedDiceMessageIdsBot = [];

  for (let i = 0; i < DUEL_DICE_COUNT; i++) {
    try {
      const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
      if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') throw new Error("Invalid dice API for bot.");
      botRolls.push(diceMsg.dice.value);
      botScore += diceMsg.dice.value;
      animatedDiceMessageIdsBot.push(diceMsg.message_id);
      await sleep(1800);
    } catch (e) {
      console.error(`${LOG_PREFIX_DUEL_ROLL} Bot sendDice error die ${i + 1}: ${e.message}. Internal.`, e.stack);
      const internalRoll = rollDie();
      botRolls.push(internalRoll);
      botScore += internalRoll;
      await safeSendMessage(String(chatId), `⚙️ Bot Dealer (Internal Roll ${i+1}/${DUEL_DICE_COUNT}): A *${escapeMarkdownV2(String(internalRoll))}* 🎲 appears\\!`, { parse_mode: 'MarkdownV2' });
      await sleep(1000);
    }
  }
  gameData.botRolls = botRolls;
  gameData.botScore = BigInt(botScore);
  gameData.status = 'game_over';
  activeGames.set(gameId, gameData);

  // Delete bot's animated dice
  animatedDiceMessageIdsBot.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

  currentMessageText += `\nBot Dealer rolled: ${formatDiceRolls(botRolls)} for a total of *${escapeMarkdownV2(String(botScore))}*\\.`;

  // Determine outcome
  let resultTextPart;
  let payoutAmount = 0n;
  let outcomeReasonLog = "";

  if (gameData.playerScore > gameData.botScore) {
    resultTextPart = `🎉 **${playerRef} WINS the duel!** A stunning victory with superior rolls\\!`;
    payoutAmount = betAmount + betAmount; // Bet back + 1x profit
    outcomeReasonLog = `won_duel:${gameId}`;
  } else if (gameData.botScore > gameData.playerScore) {
    resultTextPart = `💀 **The Bot Dealer WINS the duel!** The house prevails this time\\.`;
    payoutAmount = 0n; // Bet lost
    outcomeReasonLog = `lost_duel:${gameId}`;
  } else { // Tie
    resultTextPart = `😐 **It's a TIE!** An exact match in scores\\! Your wager is returned\\.`;
    payoutAmount = betAmount; // Bet back
    outcomeReasonLog = `push_duel:${gameId}`;
  }
  currentMessageText += `\n\n${resultTextPart}`;

  let finalUserBalanceForDisplay = gameData.userObj.balanceLamports;
  const balanceUpdate = await updateUserBalance(gameData.userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
  if (balanceUpdate.success) {
    finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports;
  } else {
    currentMessageText += `\n\n⚠️ Error settling bet: ${escapeMarkdownV2(balanceUpdate.error || "N/A")}. Admin notified.`;
  }
  currentMessageText += `\n\n${playerRef}'s new balance: *${escapeMarkdownV2(formatCurrency(finalUserBalanceForDisplay, "credits"))}*\\.`;

  const postGameKeyboardDuel = createPostGameKeyboard(GAME_IDS.DUEL, Number(betAmount));
  postGameKeyboardDuel.inline_keyboard.push([{ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.DUEL}` }])


  if (gameData.gameMessageId && bot) {
    try {
      await bot.editMessageText(currentMessageText, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel
      });
    } catch (e) {
      // console.warn(`${LOG_PREFIX_DUEL_ROLL} Failed to edit Duel result message, sending new: ${e.message}`);
      await safeSendMessage(String(chatId), currentMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel });
    }
  } else {
    await safeSendMessage(String(chatId), currentMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardDuel });
  }

  activeGames.delete(gameId); // Game is finished
  // console.log(`${LOG_PREFIX_DUEL_ROLL} Game ${gameId} concluded. Player: ${gameData.playerScore}, Bot: ${gameData.botScore}.`);
}

console.log("Part 5a (Segment 8): High Roller Duel Game Logic - Complete.");
// --- End of Part 5a (Segment 8 of N) ---
// --- Start of Part 5a (Segment 9 of N) ---
// index.js - Part 5a: Greed's Ladder Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 9): Greed's Ladder Game Logic...");

// --- GREED'S LADDER GAME LOGIC ---

async function handleStartLadderCommand(chatId, userObj, betAmount, originalCommandMessageId) {
  const LOG_PREFIX_LADDER_START = `[Ladder_Start UID:${userObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_LADDER_START} Initiating Greed's Ladder. Bet: ${betAmount}`);

  if (!userObj || typeof userObj.userId === 'undefined') {
    console.error(`${LOG_PREFIX_LADDER_START} CRITICAL: User object or userId undefined.`);
    await safeSendMessage(chatId, "An internal error occurred with your player profile. Please try again.", {});
    return;
  }
  const userId = String(userObj.userId);
  const playerRef = getPlayerDisplayReference(userObj);
  const gameId = generateGameId(GAME_IDS.LADDER);

  if (userObj.balanceLamports < BigInt(betAmount)) {
    const needed = formatCurrency(Number(betAmount) - userObj.balance, "credits");
    await safeSendMessage(chatId, `${playerRef}, your balance of ${escapeMarkdownV2(formatCurrency(userObj.balance, "credits"))} is too low for a *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* Greed's Ladder bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const balanceUpdateResult = await updateUserBalance(userId, -BigInt(betAmount), `bet_placed_ladder:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult || !balanceUpdateResult.success) {
    console.error(`${LOG_PREFIX_LADDER_START} Wager placement failed: ${balanceUpdateResult.error}`);
    await safeSendMessage(chatId, `${playerRef}, your Greed's Ladder wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Please check your balance or try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }
  // console.log(`${LOG_PREFIX_LADDER_START} Wager ${betAmount} accepted. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  const gameData = { // Store initial state; game resolves quickly
    type: GAME_IDS.LADDER, gameId, chatId: String(chatId), userId, playerRef, userObj,
    betAmount: BigInt(betAmount),
    rolls: [],
    currentSum: 0n,
    busted: false,
    status: 'rolling',
    gameMessageId: null, // Will be the main message displaying progress
    lastInteractionTime: Date.now()
  };
  activeGames.set(gameId, gameData); // Store briefly, will be deleted after resolution
  // console.log(`${LOG_PREFIX_LADDER_START} Game ${gameId} initiated.`);

  let mainMessageText = `🪜 **Greed's Ladder Challenge!** 🪜\n\n${playerRef}, you've bravely wagered *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}*\\.\nThe Bot will now roll *${escapeMarkdownV2(String(LADDER_ROLL_COUNT))}* dice for you\\. Watch out for the dreaded *${escapeMarkdownV2(String(LADDER_BUST_ON))}*\\!\n\nInitializing rolls\\.\\.\\.`;
  const sentMessage = await safeSendMessage(chatId, mainMessageText, { parse_mode: 'MarkdownV2' });
  if (sentMessage && sentMessage.message_id) {
    gameData.gameMessageId = sentMessage.message_id;
  } else {
    console.error(`${LOG_PREFIX_LADDER_START} Failed to send initial Ladder message for ${gameId}. Refunding wager.`);
    await updateUserBalance(userId, BigInt(betAmount), `refund_ladder_setup_fail:${gameId}`, null, gameId, String(chatId));
    activeGames.delete(gameId);
    return;
  }
  await sleep(1500);

  let animatedDiceMessageIdsLadder = [];

  for (let i = 0; i < LADDER_ROLL_COUNT; i++) {
    if (gameData.gameMessageId && bot) { // Update the main message before each roll
        let rollProgressText = `🪜 **Greed's Ladder** - Roll ${i + 1} of ${LADDER_ROLL_COUNT} for ${playerRef}\\.\nBet: *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}*\n`;
        if (gameData.rolls.length > 0) {
            rollProgressText += `Previous rolls: ${formatDiceRolls(gameData.rolls)}\n`;
        }
        rollProgressText += `Current sum: *${escapeMarkdownV2(String(gameData.currentSum))}*\n\nRolling die #${i+1}\\.\\.\\.`;
        try {
            await bot.editMessageText(rollProgressText, {
                chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
                parse_mode: 'MarkdownV2', reply_markup: {}
            });
        } catch (e) { /* console.warn(...) */ }
    }
    await sleep(1000);

    let rollValue;
    try {
      const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎲' });
      if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') throw new Error("Invalid dice API response.");
      rollValue = diceMsg.dice.value;
      animatedDiceMessageIdsLadder.push(diceMsg.message_id);
      await sleep(2000); // See the die
    } catch (e) {
      console.error(`${LOG_PREFIX_LADDER_START} sendDice error for roll ${i + 1}: ${e.message}. Internal.`, e.stack);
      rollValue = rollDie();
      await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Internal Roll ${i+1}/${LADDER_ROLL_COUNT}): A *${escapeMarkdownV2(String(rollValue))}* 🎲 tumbles out\\!`, { parse_mode: 'MarkdownV2' });
      await sleep(1000);
    }

    gameData.rolls.push(rollValue);
    if (rollValue === LADDER_BUST_ON) {
      gameData.busted = true;
      // console.log(`${LOG_PREFIX_LADDER_START} Player BUSTED with a ${LADDER_BUST_ON} on roll ${i+1}.`);
      break; // Exit loop immediately on bust
    }
    gameData.currentSum += BigInt(rollValue);
    activeGames.set(gameId, gameData); // Update state after each roll
  }

  // Delete animated dice messages
  animatedDiceMessageIdsLadder.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

  // Determine outcome
  let resultTextPart = "";
  let payoutAmount = 0n;
  let outcomeReasonLog = "";

  mainMessageText = `🪜 **Greed's Ladder - Result!** 🪜\n${playerRef}'s wager: *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}*\n\nRolls: ${formatDiceRolls(gameData.rolls)}\n`;

  if (gameData.busted) {
    resultTextPart = `💥 **BUST!** You rolled a *${escapeMarkdownV2(String(LADDER_BUST_ON))}*\\! The ladder crumbles, and your bet is lost\\.`;
    payoutAmount = 0n; // Bet already deducted
    outcomeReasonLog = `lost_ladder_bust:${gameId}`;
  } else {
    mainMessageText += `Final Sum: *${escapeMarkdownV2(String(gameData.currentSum))}*\n\n`;
    let foundTier = false;
    for (const tier of LADDER_PAYOUTS) { // LADDER_PAYOUTS from Part 5a, Segment 1
      if (gameData.currentSum >= tier.min && gameData.currentSum <= tier.max) {
        resultTextPart = `${escapeMarkdownV2(tier.label)} `;
        if (tier.multiplier > 0) {
          payoutAmount = betAmount + (betAmount * BigInt(tier.multiplier)); // Bet back + profit
          resultTextPart += `You win *${escapeMarkdownV2(formatCurrency(Number(betAmount * BigInt(tier.multiplier)), "credits"))}* profit\\!`;
          outcomeReasonLog = `won_ladder_score${gameData.currentSum}:${gameId}`;
        } else if (tier.multiplier === 0) {
          payoutAmount = betAmount; // Push - bet returned
          resultTextPart += `Your wager is returned\\.`;
          outcomeReasonLog = `push_ladder_score${gameData.currentSum}:${gameId}`;
        } else { // tier.multiplier === -1 (Loss)
          payoutAmount = 0n;
          resultTextPart += `Unfortunately, this score tier means your bet is lost\\.`;
          outcomeReasonLog = `lost_ladder_score${gameData.currentSum}:${gameId}`;
        }
        foundTier = true;
        break;
      }
    }
    if (!foundTier) { // Fallback if sum doesn't match any tier (shouldn't happen with well-defined tiers)
      console.warn(`${LOG_PREFIX_LADDER_START} Score ${gameData.currentSum} did not match any payout tier for game ${gameId}. Defaulting to loss.`);
      resultTextPart = `Your score of *${escapeMarkdownV2(String(gameData.currentSum))}* didn't reach a winning tier\\. Bet lost\\.`;
      payoutAmount = 0n;
      outcomeReasonLog = `lost_ladder_no_tier:${gameId}`;
    }
  }
  mainMessageText += resultTextPart;

  let finalUserBalanceForDisplay = userObj.balanceLamports; // Pre-bet balance
  const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
  if (balanceUpdate.success) {
    finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports;
  } else {
    mainMessageText += `\n\n⚠️ Error settling bet: ${escapeMarkdownV2(balanceUpdate.error || "N/A")}. Admin notified.`;
  }
  mainMessageText += `\n\n${playerRef}'s new balance: *${escapeMarkdownV2(formatCurrency(finalUserBalanceForDisplay, "credits"))}*\\.`;

  const postGameKeyboardLadder = createPostGameKeyboard(GAME_IDS.LADDER, Number(betAmount));
  postGameKeyboardLadder.inline_keyboard.push([{ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.LADDER}` }])


  if (gameData.gameMessageId && bot) {
    try {
      await bot.editMessageText(mainMessageText, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder
      });
    } catch (e) {
      // console.warn(`${LOG_PREFIX_LADDER_START} Failed to edit Ladder result message, sending new: ${e.message}`);
      await safeSendMessage(String(chatId), mainMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder });
    }
  } else {
    await safeSendMessage(String(chatId), mainMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardLadder });
  }

  activeGames.delete(gameId); // Game is finished
  // console.log(`${LOG_PREFIX_LADDER_START} Game ${gameId} concluded. Busted: ${gameData.busted}, Sum: ${gameData.currentSum}.`);
}

console.log("Part 5a (Segment 9): Greed's Ladder Game Logic - Complete.");
// --- End of Part 5a (Segment 9 of N) ---
// --- Start of Part 5a (Segment 10 of N) ---
// index.js - Part 5a: Sevens Out (Craps Style) Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 10): Sevens Out (Craps Style) Game Logic...");

// --- SEVENS OUT (CRAPS STYLE) GAME LOGIC ---

async function handleStartSevenOutCommand(chatId, userObj, betAmount, originalCommandMessageId) {
  const LOG_PREFIX_S7_START = `[S7_Start UID:${userObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_S7_START} Initiating Sevens Out. Bet: ${betAmount}`);

  if (!userObj || typeof userObj.userId === 'undefined') {
    console.error(`${LOG_PREFIX_S7_START} CRITICAL: User object or userId undefined.`);
    await safeSendMessage(chatId, "An internal error occurred with your player profile. Please try again.", {});
    return;
  }
  const userId = String(userObj.userId);
  const playerRef = getPlayerDisplayReference(userObj);
  const gameId = generateGameId(GAME_IDS.SEVEN_OUT);

  if (userObj.balanceLamports < BigInt(betAmount)) {
    const needed = formatCurrency(Number(betAmount) - userObj.balance, "credits");
    await safeSendMessage(chatId, `${playerRef}, your balance of ${escapeMarkdownV2(formatCurrency(userObj.balance, "credits"))} is too low for a *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* Sevens Out bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const balanceUpdateResult = await updateUserBalance(userId, -BigInt(betAmount), `bet_placed_s7_init:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult || !balanceUpdateResult.success) {
    console.error(`${LOG_PREFIX_S7_START} Wager placement failed: ${balanceUpdateResult.error}`);
    await safeSendMessage(chatId, `${playerRef}, your Sevens Out wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Please check your balance or try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }
  // console.log(`${LOG_PREFIX_S7_START} Wager ${betAmount} accepted. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  const gameData = {
    type: GAME_IDS.SEVEN_OUT, gameId, chatId: String(chatId), userId, playerRef, userObj,
    betAmount: BigInt(betAmount),
    point: null, // The established point number
    status: 'come_out_roll', // Initial phase
    gameMessageId: null,
    lastInteractionTime: Date.now()
  };
  activeGames.set(gameId, gameData);
  // console.log(`${LOG_PREFIX_S7_START} Game ${gameId} initiated. Data: ${stringifyWithBigInt(gameData)}`);

  const initialMessageText = `🎲 **Sevens Out!** 🎲\n\n${playerRef} steps up to the table with a bet of *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}*\\.\nThis is the **Come Out Roll**\\! Let's see what fate the dice hold\\.\\.\\.\n\nRolling the dice\\!`;
  const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2' });

  if (sentMessage && sentMessage.message_id) {
    gameData.gameMessageId = sentMessage.message_id;
    activeGames.set(gameId, gameData); // Update with messageId
    // console.log(`${LOG_PREFIX_S7_START} Game ${gameId} Come Out Roll announcement sent. MessageID: ${sentMessage.message_id}.`);
    await sleep(1500); // Pause before the first roll
    await processSevenOutRoll(gameData); // Pass the gameData object directly
  } else {
    console.error(`${LOG_PREFIX_S7_START} Failed to send Sevens Out initial game message for ${gameId}. Refunding wager.`);
    await updateUserBalance(userId, BigInt(betAmount), `refund_s7_setup_fail:${gameId}`, null, gameId, String(chatId));
    activeGames.delete(gameId);
    // console.log(`${LOG_PREFIX_S7_START} Wager refunded and game ${gameId} removed.`);
  }
  // console.log(`${LOG_PREFIX_S7_START} Exiting start command handler.`);
}


async function handleSevenOutRoll(gameId, userObj, originalMessageIdFromCallback) { // Called by "Roll for Point" button
  const LOG_PREFIX_S7_PLAYER_ROLL = `[S7_PlayerRoll GID:${gameId} UID:${userObj.userId}]`;
  // console.log(`${LOG_PREFIX_S7_PLAYER_ROLL} Player initiated roll for point.`);

  const gameData = activeGames.get(gameId);

  if (!gameData || gameData.userId !== String(userObj.userId) || gameData.status !== 'point_phase' || Number(gameData.gameMessageId) !== Number(originalMessageIdFromCallback)) {
    // console.warn(`${LOG_PREFIX_S7_PLAYER_ROLL} Invalid roll: game/user/status mismatch or stale message. Status: ${gameData?.status}, GameMsgID: ${gameData?.gameMessageId}`);
    await bot.answerCallbackQuery(callbackQueryId, { text: "Not your turn or this Sevens Out action is outdated.", show_alert: true });
    if (originalMessageIdFromCallback && bot && gameData && gameData.chatId) {
        bot.editMessageReplyMarkup({}, { chat_id: String(gameData.chatId), message_id: Number(originalMessageIdFromCallback) }).catch(()=>{});
    }
    return;
  }

  // Update message to show rolling for point
  const rollingForPointText = `🎲 **Sevens Out - Point Phase** 🎲\n${gameData.playerRef} (Bet: *${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount), "credits"))}*)\nYour Point is: *${escapeMarkdownV2(String(gameData.point))}*\\.\nRolling the dice again, trying to hit your Point before a 7\\!`;
  if (gameData.gameMessageId && bot) {
    try {
        await bot.editMessageText(rollingForPointText, {
            chat_id: String(gameData.chatId), message_id: Number(gameData.gameMessageId),
            parse_mode: 'MarkdownV2', reply_markup: {} // Clear "Roll for Point" button
        });
    } catch(e) { /* console.warn(...) */ }
  }
  await sleep(1000);
  await processSevenOutRoll(gameData); // Process the actual roll
}


async function processSevenOutRoll(gameData) { // Handles dice rolling for all phases
  const LOG_PREFIX_S7_PROCESS = `[S7_ProcessRoll GID:${gameData.gameId} UID:${gameData.userId} Phase:${gameData.status}]`;
  // console.log(`${LOG_PREFIX_S7_PROCESS} Processing roll.`);

  const { gameId, chatId, userId, playerRef, betAmount, status, point, userObj } = gameData; // userObj for final balance display
  let animatedDiceMessageIdsS7 = [];

  // Roll two dice
  let roll1, roll2;
  try {
    const diceMsg1 = await bot.sendDice(String(chatId), { emoji: '🎲' });
    if (!diceMsg1 || !diceMsg1.dice) throw new Error("Dice 1 API error");
    roll1 = diceMsg1.dice.value;
    animatedDiceMessageIdsS7.push(diceMsg1.message_id);
    await sleep(1800);

    const diceMsg2 = await bot.sendDice(String(chatId), { emoji: '🎲' });
    if (!diceMsg2 || !diceMsg2.dice) throw new Error("Dice 2 API error");
    roll2 = diceMsg2.dice.value;
    animatedDiceMessageIdsS7.push(diceMsg2.message_id);
    await sleep(1800);
  } catch(e) {
    console.error(`${LOG_PREFIX_S7_PROCESS} sendDice error: ${e.message}. Using internal rolls.`, e.stack);
    roll1 = rollDie(); roll2 = rollDie();
    await safeSendMessage(String(chatId), `⚙️ ${playerRef} (Internal Rolls): Dice show *${escapeMarkdownV2(String(roll1))}* and *${escapeMarkdownV2(String(roll2))}* 🎲🎲`, { parse_mode: 'MarkdownV2' });
    await sleep(1000);
  }

  // Delete animated dice messages
  animatedDiceMessageIdsS7.forEach(id => { if (bot) bot.deleteMessage(String(chatId), id).catch(() => {}); });

  const total = roll1 + roll2;
  let currentMessageText = `🎲 **Sevens Out - Roll Result** 🎲\n${playerRef}'s Bet: *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}*\n\n`;
  currentMessageText += `The dice show: ${formatDiceRolls([roll1, roll2])} for a total sum of *${escapeMarkdownV2(String(total))}*\\!\n\n`;

  let payoutAmount = 0n;
  let outcomeReasonLog = "";
  let gameEnded = false;
  let newStatus = status;
  let newPoint = point;
  let buttons = [];
  let resultTextPart = "";

  if (status === 'come_out_roll') {
    currentMessageText += `*Come Out Roll Result:*\n`;
    if (total === 7 || total === 11) { // Win on Come Out
      resultTextPart = `🎉 **NATURAL WIN!** A *${escapeMarkdownV2(String(total))}* on the come out roll means you win immediately\\!`;
      payoutAmount = betAmount + betAmount; // Bet back + 1x profit
      outcomeReasonLog = `won_s7_comeout_${total}:${gameId}`;
      gameEnded = true;
    } else if (total === 2 || total === 3 || total === 12) { // Lose on Come Out (Craps)
      resultTextPart = `💀 **CRAPS!** A *${escapeMarkdownV2(String(total))}* on the come out roll\\. Unfortunately, that's a loss\\.`;
      payoutAmount = 0n; // Bet lost
      outcomeReasonLog = `lost_s7_comeout_craps_${total}:${gameId}`;
      gameEnded = true;
    } else { // Point Established
      newPoint = total;
      newStatus = 'point_phase';
      resultTextPart = `🎯 **POINT ESTABLISHED: ${escapeMarkdownV2(String(newPoint))}**\\!\nNow, you need to roll a *${escapeMarkdownV2(String(newPoint))}* again before rolling a 7 to win\\.`;
      buttons.push({ text: `🎲 Roll for Point ${newPoint}`, callback_data: `s7_roll:${gameId}` });
    }
  } else if (status === 'point_phase') {
    currentMessageText += `*Point Phase Roll (Your Point: ${escapeMarkdownV2(String(point))}) Result:*\n`;
    if (total === newPoint) { // Made Point
      resultTextPart = `🎉 **POINT HIT!** You rolled your Point *${escapeMarkdownV2(String(newPoint))}* again\\! You win\\!`;
      payoutAmount = betAmount + betAmount; // Bet back + 1x profit
      outcomeReasonLog = `won_s7_hit_point_${total}:${gameId}`;
      gameEnded = true;
    } else if (total === 7) { // Seven Out - Lose
      resultTextPart = `💀 **SEVEN OUT!** You rolled a 7 before hitting your Point (*${escapeMarkdownV2(String(newPoint))}*)\\. The house wins this round\\.`;
      payoutAmount = 0n; // Bet lost
      outcomeReasonLog = `lost_s7_seven_out:${gameId}`;
      gameEnded = true;
    } else { // Keep rolling for Point
      resultTextPart = `You rolled a *${escapeMarkdownV2(String(total))}*\\. Still aiming for your Point: *${escapeMarkdownV2(String(newPoint))}*\\. Roll again\\!`;
      buttons.push({ text: `🎲 Roll for Point ${newPoint}`, callback_data: `s7_roll:${gameId}` });
    }
  }
  currentMessageText += resultTextPart;
  let finalUserBalanceForDisplay = userObj.balanceLamports;

  if (gameEnded) {
    const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
    if (balanceUpdate.success) {
      finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports;
    } else {
      currentMessageText += `\n\n⚠️ Error settling bet: ${escapeMarkdownV2(balanceUpdate.error || "N/A")}. Admin notified.`;
    }
    currentMessageText += `\n\n${playerRef}'s new balance: *${escapeMarkdownV2(formatCurrency(finalUserBalanceForDisplay, "credits"))}*\\.`;
    const postGameKeyboardS7 = createPostGameKeyboard(GAME_IDS.SEVEN_OUT, Number(betAmount));
    postGameKeyboardS7.inline_keyboard.push([{ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.SEVEN_OUT}` }])
    buttons = postGameKeyboardS7.inline_keyboard; // Use the full post-game keyboard
    activeGames.delete(gameId);
  } else {
    gameData.status = newStatus;
    gameData.point = newPoint;
    activeGames.set(gameId, gameData); // Save updated status/point
     buttons.push({ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.SEVEN_OUT}` });
  }

  const keyboardS7 = { inline_keyboard: buttons }; // buttons will be empty if gameEnded and createPostGameKeyboard isn't used or populated for continuation
  if (gameData.gameMessageId && bot) {
    try {
      await bot.editMessageText(currentMessageText, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboardS7 : {})
      });
    } catch (e) {
      // console.warn(`${LOG_PREFIX_S7_PROCESS} Failed to edit S7 message, sending new: ${e.message}`);
      await safeSendMessage(String(chatId), currentMessageText, { parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboardS7 : {}) });
    }
  } else { // Should only happen if initial message failed to send and gameMessageId is null
    await safeSendMessage(String(chatId), currentMessageText, { parse_mode: 'MarkdownV2', reply_markup: (buttons.length > 0 ? keyboardS7 : {}) });
  }
  // console.log(`${LOG_PREFIX_S7_PROCESS} Roll processed. Game ended: ${gameEnded}. New status: ${gameData.status}, Point: ${gameData.point}.`);
}

console.log("Part 5a (Segment 10): Sevens Out (Craps Style) Game Logic - Complete.");
// --- End of Part 5a (Segment 10 of N) ---
// --- Start of Part 5a (Segment 11 of N) ---
// index.js - Part 5a: Slot Fruit Frenzy Game Logic
//---------------------------------------------------------------------------
console.log("Loading Part 5a (Segment 11): Slot Fruit Frenzy Game Logic...");

// --- SLOT FRUIT FRENZY GAME LOGIC ---

async function handleStartSlotCommand(chatId, userObj, betAmount, originalCommandMessageId) {
  const LOG_PREFIX_SLOT_START = `[Slot_Start UID:${userObj.userId} CH:${chatId}]`;
  // console.log(`${LOG_PREFIX_SLOT_START} Initiating Slot Fruit Frenzy. Bet: ${betAmount}`);

  if (!userObj || typeof userObj.userId === 'undefined') {
    console.error(`${LOG_PREFIX_SLOT_START} CRITICAL: User object or userId undefined.`);
    await safeSendMessage(chatId, "An internal error occurred with your player profile. Please try again.", {});
    return;
  }
  const userId = String(userObj.userId);
  const playerRef = getPlayerDisplayReference(userObj);
  const gameId = generateGameId(GAME_IDS.SLOT_FRENZY);

  if (userObj.balanceLamports < BigInt(betAmount)) {
    const needed = formatCurrency(Number(betAmount) - userObj.balance, "credits");
    await safeSendMessage(chatId, `${playerRef}, your balance of ${escapeMarkdownV2(formatCurrency(userObj.balance, "credits"))} is too low for a *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}* Slot Frenzy bet\\. You need ${escapeMarkdownV2(needed)} more\\.`, {
        parse_mode: 'MarkdownV2',
        reply_markup: { inline_keyboard: [[{ text: "💰 Quick Deposit", callback_data: QUICK_DEPOSIT_CALLBACK_ACTION }]]}
    });
    return;
  }

  const balanceUpdateResult = await updateUserBalance(userId, -BigInt(betAmount), `bet_placed_slot:${gameId}`, null, gameId, String(chatId));

  if (!balanceUpdateResult || !balanceUpdateResult.success) {
    console.error(`${LOG_PREFIX_SLOT_START} Wager placement failed: ${balanceUpdateResult.error}`);
    await safeSendMessage(chatId, `${playerRef}, your Slot Frenzy wager of *${escapeMarkdownV2(formatCurrency(betAmount))}* failed: ${escapeMarkdownV2(balanceUpdateResult.error || "Unknown issue")}\\. Please check your balance or try again\\.`, { parse_mode: 'MarkdownV2' });
    return;
  }
  // console.log(`${LOG_PREFIX_SLOT_START} Wager ${betAmount} accepted. New balance: ${formatCurrency(Number(balanceUpdateResult.newBalanceLamports))}`);

  // Game data is minimal as it resolves in one go.
  const gameData = {
    type: GAME_IDS.SLOT_FRENZY, gameId, chatId: String(chatId), userId, playerRef, userObj,
    betAmount: BigInt(betAmount),
    status: 'spinning', // Game immediately goes to spinning
    gameMessageId: null,
    lastInteractionTime: Date.now()
  };
  // activeGames.set(gameId, gameData); // Not strictly necessary to store for long as it resolves quickly.
                                     // However, if refunds or complex logging during spin were needed, store it.
                                     // For simplicity, we'll manage its lifecycle within this function.

  let initialMessageText = `🎰 **Slot Fruit Frenzy!** 🎰\n\n${playerRef} pulls the lever with a bet of *${escapeMarkdownV2(formatCurrency(betAmount, "credits"))}*\\!\nThe reels are a blur\\! Let's see what lands\\.\\.\\. Good luck\\!`;
  const sentMessage = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2' });
  if (sentMessage && sentMessage.message_id) {
    gameData.gameMessageId = sentMessage.message_id; // Store for potential edit
  }
  await sleep(1000); // Pause for "spinning" effect

  let slotResultValue;
  let animatedSlotMessageId = null;
  try {
    const diceMsg = await bot.sendDice(String(chatId), { emoji: '🎰' }); // Use the slot machine emoji
    if (!diceMsg || !diceMsg.dice || typeof diceMsg.dice.value === 'undefined') {
      throw new Error("Invalid slot dice response from Telegram API.");
    }
    slotResultValue = diceMsg.dice.value; // This value is 1-64
    animatedSlotMessageId = diceMsg.message_id;
    // console.log(`${LOG_PREFIX_SLOT_START} Slot machine result (value ${slotResultValue}) received. Animation ID: ${animatedSlotMessageId}. Pausing for animation.`);
    await sleep(3000); // Time for the slot animation to complete visually for the user
  } catch (e) {
    console.error(`${LOG_PREFIX_SLOT_START} sendDice for slot machine failed: ${e.message}. Assigning random loss.`, e.stack);
    // Fallback to a non-winning value if sendDice fails
    let randomLossVal = Math.floor(Math.random() * 64) + 1;
    while(SLOT_PAYOUTS[randomLossVal]) { // Ensure it's a value NOT in the defined SLOT_PAYOUTS keys
        randomLossVal = Math.floor(Math.random() * 64) + 1;
        if (Object.keys(SLOT_PAYOUTS).length >= 64 && SLOT_PAYOUTS[randomLossVal]) { // Highly unlikely all 64 are wins
            console.warn(`${LOG_PREFIX_SLOT_START} All 64 slot values defined as wins in SLOT_PAYOUTS. Fallback might pick a win.`);
            break;
        }
    }
    slotResultValue = randomLossVal;
    await safeSendMessage(String(chatId), `⚙️ The slot machine had a hiccup\\. Spinning internally\\.\\.\\. the result is *${escapeMarkdownV2(String(slotResultValue))}*\\.`, { parse_mode: 'MarkdownV2' });
    await sleep(1000);
  }

  // Delete the animated slot message
  if (animatedSlotMessageId && bot) {
    bot.deleteMessage(String(chatId), animatedSlotMessageId).catch(() => {});
  }

  const outcomeDetails = SLOT_PAYOUTS[slotResultValue]; // SLOT_PAYOUTS from Part 5a, Segment 1
  let resultTextPart = "";
  let payoutAmount = 0n;
  let outcomeReasonLog = "";

  // Construct result message text
  let finalMessageText = `🎰 **Slot Fruit Frenzy - Result!** 🎰\n${playerRef}'s Bet: *${escapeMarkdownV2(formatCurrency(Number(betAmount), "credits"))}*\n\n`;
  finalMessageText += `The reels settle on value: *${escapeMarkdownV2(String(slotResultValue))}*\\.\n`; // Good for debugging or if symbols are complex

  if (outcomeDetails) { // A winning combination!
    finalMessageText += `Symbols: **${escapeMarkdownV2(outcomeDetails.symbols)}**\n\n`;
    resultTextPart = `🎉 **${escapeMarkdownV2(outcomeDetails.label)} YOU WIN!** 🎉\nYou've won *${escapeMarkdownV2(formatCurrency(Number(betAmount * BigInt(outcomeDetails.multiplier)), "credits"))}* profit\\!`;
    payoutAmount = betAmount + (betAmount * BigInt(outcomeDetails.multiplier)); // Bet back + profit
    outcomeReasonLog = `won_slot_${slotResultValue}:${gameId}`;
  } else { // Not a winning combination
    finalMessageText += `Symbols: No winning line this time\\.\n\n`;
    resultTextPart = `💔 **So close!** Not a winning spin this time\\. Better luck on the next pull\\!`;
    payoutAmount = 0n; // Bet lost
    outcomeReasonLog = `lost_slot_${slotResultValue}:${gameId}`;
  }
  finalMessageText += resultTextPart;

  let finalUserBalanceForDisplay = userObj.balanceLamports;
  const balanceUpdate = await updateUserBalance(userId, payoutAmount, outcomeReasonLog, null, gameId, String(chatId));
  if (balanceUpdate.success) {
    finalUserBalanceForDisplay = balanceUpdate.newBalanceLamports;
  } else {
    finalMessageText += `\n\n⚠️ Error settling bet: ${escapeMarkdownV2(balanceUpdate.error || "N/A")}. Admin notified.`;
  }
  finalMessageText += `\n\n${playerRef}'s new balance: *${escapeMarkdownV2(formatCurrency(finalUserBalanceForDisplay, "credits"))}*\\.`;

  const postGameKeyboardSlot = createPostGameKeyboard(GAME_IDS.SLOT_FRENZY, Number(betAmount));
  postGameKeyboardSlot.inline_keyboard.push([{ text: `📜 ${escapeMarkdownV2(gameData.type)} Rules`, callback_data: `${RULES_CALLBACK_PREFIX}${GAME_IDS.SLOT_FRENZY}` }])


  // Edit the initial "spinning" message or send new if ID was lost
  if (gameData.gameMessageId && bot) {
    try {
      await bot.editMessageText(finalMessageText, {
        chat_id: String(chatId), message_id: Number(gameData.gameMessageId),
        parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot
      });
    } catch (e) {
      // console.warn(`${LOG_PREFIX_SLOT_START} Failed to edit Slot result message, sending new: ${e.message}`);
      await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot });
    }
  } else {
    await safeSendMessage(String(chatId), finalMessageText, { parse_mode: 'MarkdownV2', reply_markup: postGameKeyboardSlot });
  }

  // Slots is a one-shot game, no complex state to keep in activeGames usually.
  // activeGames.delete(gameId); // If it was added, delete it.
  // console.log(`${LOG_PREFIX_SLOT_START} Game ${gameId} concluded. Slot value: ${slotResultValue}. Win: ${!!outcomeDetails}.`);
}

console.log("Part 5a (Segment 11): Slot Fruit Frenzy Game Logic - Complete.");
// --- End of Part 5a (Segment 11 of N) ---
// --- Start of Part 6 ---
// index.js - Part 6: Database Initialization, Startup, Shutdown, and Enhanced Error Handling
//---------------------------------------------------------------------------
console.log("Loading Part 6: Database Initialization, Startup, Shutdown, and Enhanced Error Handling...");

// --- Database Initialization Function ---
async function initializeDatabase() {
  const LOG_PREFIX_DB_INIT = '⚙️ [DB Init]';
  console.log(`${LOG_PREFIX_DB_INIT} Initializing Database Schema (if necessary)...`);
  let client = null; // Use a dedicated client for the transaction to ensure atomicity

  try {
    if (!pool) {
        console.error(`${LOG_PREFIX_DB_INIT} CRITICAL: Database pool is not available. Cannot initialize database.`);
        process.exit(2); // Critical failure
    }
    client = await pool.connect();
    await client.query('BEGIN'); // Start a transaction
    console.log(`${LOG_PREFIX_DB_INIT} Transaction started for schema setup.`);

    // Wallets Table: Stores core user information, referral details, and some aggregate stats.
    console.log(`${LOG_PREFIX_DB_INIT} Ensuring "wallets" table exists...`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS wallets (
        user_id VARCHAR(255) PRIMARY KEY, -- Telegram User ID
        external_withdrawal_address VARCHAR(64), -- For crypto withdrawals (e.g., SOL, ETH, BTC address)
        linked_at TIMESTAMPTZ,                 -- When withdrawal address was linked
        last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Last interaction time
        referral_code VARCHAR(20) UNIQUE,      -- Unique code for referring others
        referred_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL, -- Who referred this user
        referral_count INTEGER NOT NULL DEFAULT 0, -- How many users this user has referred
        total_wagered_lamports BIGINT NOT NULL DEFAULT 0, -- Lifetime wagered amount (for rewards/stats)
        last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0, -- Tracks wager milestones for rewards
        last_bet_amounts JSONB DEFAULT '{}'::jsonb, -- Stores last bet amounts per game for "Play Again"
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW() -- User registration time
      );
    `);
    await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_total_wagered ON wallets (total_wagered_lamports DESC);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_created_at ON wallets (created_at);');
    console.log(`${LOG_PREFIX_DB_INIT} "wallets" table schema ensured.`);

    // User Balances Table: Stores current user balances. Separate for frequent updates.
    console.log(`${LOG_PREFIX_DB_INIT} Ensuring "user_balances" table exists...`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS user_balances (
        user_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE, -- Ensures balance record is removed if wallet is
        balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0), -- Current balance, cannot be negative
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() -- Last time balance was updated
      );
    `);
    console.log(`${LOG_PREFIX_DB_INIT} "user_balances" table schema ensured.`);

    // Bets Table (Transaction Log): Records every bet, win, loss, refund for auditing.
    console.log(`${LOG_PREFIX_DB_INIT} Ensuring "bets" table exists...`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS bets (
        id SERIAL PRIMARY KEY,                        -- Auto-incrementing internal ID for the bet record
        user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
        chat_id VARCHAR(255) NOT NULL,                -- Chat where the game/bet occurred
        game_id VARCHAR(255),                         -- Link to a specific game instance (from activeGames map key)
        game_type VARCHAR(50) NOT NULL,               -- e.g., 'dice_escalator', 'coinflip', 'admin_grant'
        bet_details JSONB,                            -- Game-specific details (e.g., dice rolls, choices, jackpot contribution)
        wager_amount_lamports BIGINT NOT NULL DEFAULT 0 CHECK (wager_amount_lamports >= 0), -- Amount wagered
        payout_amount_lamports BIGINT,                -- Amount paid out (includes wager return for wins/pushes)
        status VARCHAR(50) NOT NULL DEFAULT 'active', -- 'active', 'processed', 'refunded', 'lost', 'won', 'push'
        reason_tx TEXT,                               -- Detailed reason for the transaction (e.g., 'won_dice_escalator_score:game123')
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),-- When the bet was placed/transaction initiated
        processed_at TIMESTAMPTZ                     -- When the bet was resolved/transaction completed
      );
    `);
    await client.query('CREATE INDEX IF NOT EXISTS idx_bets_user_id_game_type_status ON bets (user_id, game_type, status);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_bets_game_id ON bets (game_id);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_bets_created_at_status ON bets (created_at DESC, status);');
    console.log(`${LOG_PREFIX_DB_INIT} "bets" table schema ensured.`);

    // DICE ROLL REQUESTS TABLE (Note: Current game logic uses bot.sendDice directly)
    // This table is kept as per "no omissions" instruction.
    // It would be used if a separate helper bot or an internal queuing system for dice rolls was implemented.
    console.log(`${LOG_PREFIX_DB_INIT} Ensuring "dice_roll_requests" table exists (currently unused by direct game logic)...`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS dice_roll_requests (
        request_id SERIAL PRIMARY KEY,
        game_id VARCHAR(255) NOT NULL UNIQUE,      -- ID of the game requesting the roll
        chat_id VARCHAR(255) NOT NULL,             -- Chat context for the roll
        user_id VARCHAR(255) NOT NULL,             -- User for whom the roll is made
        status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'processed', 'failed', 'expired'
        roll_value INTEGER,                        -- The outcome of the roll
        emoji_type VARCHAR(10),                    -- Emoji used for the dice (e.g., '🎲', '🎰')
        requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ
      );
    `);
    await client.query('CREATE INDEX IF NOT EXISTS idx_dice_roll_requests_status_requested_at ON dice_roll_requests (status, requested_at);');
    console.log(`${LOG_PREFIX_DB_INIT} "dice_roll_requests" table schema ensured.`);

    // JACKPOT STATUS TABLE: Manages jackpot amounts for games like Dice Escalator.
    console.log(`${LOG_PREFIX_DB_INIT} Ensuring "jackpot_status" table exists...`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS jackpot_status (
        jackpot_id VARCHAR(50) PRIMARY KEY,          -- Unique ID for the jackpot (e.g., 'dice_escalator_main')
        current_amount_lamports BIGINT NOT NULL DEFAULT 0 CHECK (current_amount_lamports >= 0),
        last_won_at TIMESTAMPTZ,
        last_won_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
        last_won_game_id VARCHAR(255),               -- Game ID that triggered the last jackpot win
        last_contributed_game_id VARCHAR(255),       -- Game ID of the last game that contributed
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    // Ensure the main Dice Escalator jackpot entry exists. MAIN_JACKPOT_ID is from Part 1.
    await client.query(`
      INSERT INTO jackpot_status (jackpot_id, current_amount_lamports, updated_at)
      VALUES ($1, 0, NOW())
      ON CONFLICT (jackpot_id) DO NOTHING;
    `, [MAIN_JACKPOT_ID]);
    console.log(`${LOG_PREFIX_DB_INIT} "jackpot_status" table ensured and main jackpot ID ('${MAIN_JACKPOT_ID}') initialized if new.`);

    await client.query('COMMIT');
    console.log(`✅ ${LOG_PREFIX_DB_INIT} Database schema initialized/verified successfully.`);

  } catch (err) {
    console.error(`❌ CRITICAL DATABASE INITIALIZATION ERROR: ${err.message}`, err.stack);
    if (client) {
      try { await client.query('ROLLBACK'); console.log(`${LOG_PREFIX_DB_INIT} Transaction rolled back due to schema setup error.`); }
      catch (rbErr) { console.error(`${LOG_PREFIX_DB_INIT} Rollback failed: ${rbErr.message}`, rbErr.stack); }
    }
    // Attempt to notify admin before exiting
    if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function") {
      safeSendMessage(ADMIN_USER_ID, `🚨 CRITICAL DB INIT FAILED: ${escapeMarkdownV2(String(err.message || err))}. Bot cannot start. Check logs.`, {parse_mode:'MarkdownV2'})
        .catch(e => console.error("Failed to send admin notification on DB init failure:", e));
    }
    process.exit(2); // Use a distinct exit code for DB init failure
  } finally {
    if (client) {
      client.release();
      // console.log(`${LOG_PREFIX_DB_INIT} Database client released.`);
    }
  }
}

// --- Periodic Background Tasks ---
// Cleans up stale games, sessions, and potentially other background maintenance.
let backgroundTaskIntervalId = null; // Renamed for clarity
const BACKGROUND_TASK_INTERVAL_MS = 15 * 60 * 1000; // Run every 15 minutes (configurable)

async function runPeriodicBackgroundTasks() {
  const LOG_PREFIX_BG = `[BackgroundTask (${new Date().toISOString()})]`;
  // console.log(`${LOG_PREFIX_BG} Starting periodic background tasks...`);
  const now = Date.now();
  const JOIN_GAME_TIMEOUT_MS_parsed = parseInt(process.env.JOIN_GAME_TIMEOUT_MS || '120000', 10);
  const GAME_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS_parsed * 5; // Stale if 5x join timeout
  let cleanedGamesCount = 0;

  try {
    for (const [gameId, gameData] of activeGames.entries()) {
      if (!gameData || typeof gameData.creationTime !== 'number' || typeof gameData.status !== 'string') {
        console.warn(`${LOG_PREFIX_BG} Corrupt game entry ID: ${gameId}. Removing. Details: ${stringifyWithBigInt(gameData)}`);
        activeGames.delete(gameId);
        continue;
      }

      const gameAge = now - (gameData.lastInteractionTime || gameData.creationTime); // Use lastInteractionTime if available

      // Define statuses that indicate a game is "stuck" or waiting for player action indefinitely
      const stalePlayerActionStatuses = [
          'waiting_opponent', 'waiting_choices', // Group games
          'waiting_player_roll', 'player_turn_prompt_action', // Dice Escalator
          'player_turn', // Dice 21
          'waiting_player_choice', // Over/Under 7
          'player_turn_to_roll', // Duel
          'point_phase' // Sevens Out (if player doesn't click roll)
        ];

      if (stalePlayerActionStatuses.includes(gameData.status) && gameAge > GAME_CLEANUP_THRESHOLD_MS) {
        console.warn(`${LOG_PREFIX_BG} Cleaning stale game ${gameId} (${gameData.type}). Status: ${gameData.status}, Age: ${Math.round(gameAge/60000)}m.`);
        const primaryUserId = gameData.initiatorId || gameData.userId;
        const playerRefForMsg = gameData.initiatorMention || gameData.playerRef || (primaryUserId ? `User ${String(primaryUserId).slice(-4)}` : 'Player');
        let staleMsgText = `⏳ Game \`${escapeMarkdownV2(gameId)}\` (${escapeMarkdownV2(gameData.type)}) started by ${playerRefForMsg} was cleared due to extended inactivity\\.`;

        if (primaryUserId && gameData.betAmount > 0n && typeof updateUserBalance === 'function') {
          // Only refund if bet was placed and not yet resolved in a loss (e.g., bust already handled)
          // For most stale games, the bet was deducted but game didn't conclude.
          const refundReason = `refund_stale_game_${gameData.type}:${gameId}`;
          await updateUserBalance(primaryUserId, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId));
          staleMsgText += ` Bet of ${escapeMarkdownV2(formatCurrency(Number(gameData.betAmount)))} refunded\\.`;
          // console.log(`${LOG_PREFIX_BG} Refunded bet for stale game ${gameId} to user ${primaryUserId}.`);

          // If it's a group game with multiple bettors (e.g. RPS waiting for choices from both)
          if (gameData.participants && gameData.participants.length > 1) {
            for (const p of gameData.participants) {
              if (p.userId !== primaryUserId && p.betPlaced) { // Refund other participants
                await updateUserBalance(p.userId, gameData.betAmount, refundReason, null, gameId, String(gameData.chatId));
                // console.log(`${LOG_PREFIX_BG} Refunded bet for stale game ${gameId} to participant ${p.userId}.`);
              }
            }
          }
        }

        if (bot && typeof safeSendMessage === 'function' && gameData.chatId) {
          const messageIdToEdit = gameData.gameMessageId || gameData.gameSetupMessageId;
          if (messageIdToEdit) {
            bot.editMessageText(staleMsgText, { chatId: String(gameData.chatId), message_id: Number(messageIdToEdit), parse_mode: 'MarkdownV2', reply_markup: {} })
              .catch(() => { safeSendMessage(String(gameData.chatId), staleMsgText, { parse_mode: 'MarkdownV2' }); });
          } else {
            safeSendMessage(String(gameData.chatId), staleMsgText, { parse_mode: 'MarkdownV2' });
          }
        }
        activeGames.delete(gameId);
        cleanedGamesCount++;
        if (typeof updateGroupGameDetails === 'function' && (gameData.type === GAME_IDS.COINFLIP || gameData.type === GAME_IDS.RPS)) {
            await updateGroupGameDetails(gameData.chatId, null, null, null); // Clear from group session too
        }
      }
    }
  } catch (loopError) {
    console.error(`${LOG_PREFIX_BG} Error during stale game cleanup loop:`, loopError);
  }
  if (cleanedGamesCount > 0) console.log(`${LOG_PREFIX_BG} Cleaned ${cleanedGamesCount} stale game(s).`);

  // Cleanup for groupGameSessions (if no active game in them for a long time)
  const SESSION_CLEANUP_THRESHOLD_MS = JOIN_GAME_TIMEOUT_MS_parsed * 20; // e.g. 40 mins for 2 min join timeout
  let cleanedSessionsCount = 0;
  try {
    for (const [chatId, sessionData] of groupGameSessions.entries()) {
      if (sessionData && !sessionData.currentGameId && sessionData.lastActivity instanceof Date && (now - sessionData.lastActivity.getTime()) > SESSION_CLEANUP_THRESHOLD_MS) {
        // console.log(`${LOG_PREFIX_BG} Cleaning inactive group session for chat ${chatId}.`);
        groupGameSessions.delete(chatId);
        cleanedSessionsCount++;
      }
    }
  } catch (loopError) {
    console.error(`${LOG_PREFIX_BG} Error during inactive session cleanup loop:`, loopError);
  }
  if (cleanedSessionsCount > 0) console.log(`${LOG_PREFIX_BG} Cleaned ${cleanedSessionsCount} inactive group session entries.`);

  // console.log(`${LOG_PREFIX_BG} Finished. Active games: ${activeGames.size}, Group sessions: ${groupGameSessions.size}.`);
}


// --- Telegram Polling Retry Logic ---
let isRetryingPolling = false;
let currentPollingRetryDelay = INITIAL_RETRY_POLLING_DELAY; // Use constant from Part 1
let pollingRetryTimeoutId = null;

async function attemptRestartPolling(error) {
  const LOG_PREFIX_POLL_RETRY = "[Polling_Retry]";
  if (isShuttingDown) { console.log(`${LOG_PREFIX_POLL_RETRY} Shutdown in progress, not restarting polling.`); return; }
  if (isRetryingPolling) { console.log(`${LOG_PREFIX_POLL_RETRY} Already retrying polling, new request ignored.`); return; }

  isRetryingPolling = true;
  if (pollingRetryTimeoutId) clearTimeout(pollingRetryTimeoutId); // Clear any existing scheduled retry

  console.warn(`${LOG_PREFIX_POLL_RETRY} Polling error encountered: ${error.code || 'N/A'} - ${error.message}. Attempting to restart polling in ${currentPollingRetryDelay / 1000}s...`);

  try {
    if (bot && typeof bot.isPolling === 'function' && bot.isPolling()) {
      // console.log(`${LOG_PREFIX_POLL_RETRY} Stopping current polling instance before restart.`);
      await bot.stopPolling({ cancel: true }); // cancel: true to reject pending getUpdates requests
    }
  } catch (stopErr) {
    console.error(`${LOG_PREFIX_POLL_RETRY} Error while trying to stop polling: ${stopErr.message}`);
    // Continue with restart attempt regardless
  }

  pollingRetryTimeoutId = setTimeout(async () => {
    if (isShuttingDown) { isRetryingPolling = false; console.log(`${LOG_PREFIX_POLL_RETRY} Shutdown initiated during retry wait, aborting.`); return; }
    if (!bot || typeof bot.startPolling !== 'function') {
      console.error(`${LOG_PREFIX_POLL_RETRY} CRITICAL: Bot instance or startPolling function is not available. Cannot restart.`);
      isRetryingPolling = false; return;
    }

    console.log(`${LOG_PREFIX_POLL_RETRY} Attempting to call bot.startPolling().`);
    try {
      await bot.startPolling();
      console.log(`✅ ${LOG_PREFIX_POLL_RETRY} Polling successfully restarted!`);
      currentPollingRetryDelay = INITIAL_RETRY_POLLING_DELAY; // Reset delay on success
      isRetryingPolling = false;
    } catch (startErr) {
      console.error(`❌ ${LOG_PREFIX_POLL_RETRY} Failed to restart polling: ${startErr.code || 'N/A'} - ${startErr.message}`);
      currentPollingRetryDelay = Math.min(currentPollingRetryDelay * 2, MAX_RETRY_POLLING_DELAY); // Exponential backoff
      isRetryingPolling = false; // Allow next error to trigger a new retry attempt
      console.warn(`${LOG_PREFIX_POLL_RETRY} Next polling error will trigger a retry attempt after ${currentPollingRetryDelay / 1000}s.`);

      if (ADMIN_USER_ID && currentPollingRetryDelay >= MAX_RETRY_POLLING_DELAY && typeof safeSendMessage === "function") {
        safeSendMessage(ADMIN_USER_ID, `🚨 BOT ALERT: Failed to restart polling repeatedly. Last error: ${escapeMarkdownV2(startErr.message)}. Manual intervention may be required.`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
      }

      // If a specific fatal error occurs during restart (like 409 conflict again, or EFATAL), trigger shutdown
      if (typeof shutdown === "function" && (String(startErr.message).toLowerCase().includes('conflict') || String(startErr.code).includes('EFATAL'))) {
        console.error(`${LOG_PREFIX_POLL_RETRY} Fatal error (${startErr.message}) during polling restart. Initiating bot shutdown.`);
        if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
          safeSendMessage(ADMIN_USER_ID, `🚨 BOT SHUTDOWN: Fatal error during polling restart. Error: ${escapeMarkdownV2(startErr.message)}.`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
        }
        shutdown('POLLING_RESTART_FATAL').catch(() => process.exit(1)); // Ensure shutdown is called
      }
    }
  }, currentPollingRetryDelay);

  if(pollingRetryTimeoutId?.unref) pollingRetryTimeoutId.unref(); // Allow Node.js to exit if this is the only timer
}

// --- The Core Shutdown Function (Enhanced Version) ---
async function shutdown(signal = 'UNKNOWN') {
  const LOG_PREFIX_SHUTDOWN = "🚦 [Shutdown]";
  if (isShuttingDown) {
    console.warn(`${LOG_PREFIX_SHUTDOWN} Shutdown already in progress. Signal: ${signal} (ignored additional call).`);
    return;
  }
  isShuttingDown = true; // Set flag immediately
  console.warn(`\n${LOG_PREFIX_SHUTDOWN} Received signal: ${signal}. Initiating graceful shutdown... (PID: ${process.pid})`);

  // Clear any pending polling restart attempts
  if (pollingRetryTimeoutId) clearTimeout(pollingRetryTimeoutId);
  isRetryingPolling = false;

  // Notify admin if possible
  if (ADMIN_USER_ID && typeof safeSendMessage === "function" && typeof escapeMarkdownV2 === "function" && typeof BOT_VERSION !== 'undefined') {
    const shutdownMessage = `ℹ️ Bot v${BOT_VERSION} is shutting down. Signal: ${escapeMarkdownV2(String(signal))}. PID: ${process.pid}. Host: ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}.`;
    // console.log(`${LOG_PREFIX_SHUTDOWN} Attempting to notify admin: ${ADMIN_USER_ID}`);
    // Use a timeout for admin notification to prevent hanging shutdown
    const adminNotifyPromise = safeSendMessage(ADMIN_USER_ID, shutdownMessage, { parse_mode: 'MarkdownV2' });
    Promise.race([adminNotifyPromise, sleep(3000)]) // Wait max 3 seconds for notification
        .catch(e => console.warn(`${LOG_PREFIX_SHUTDOWN} Failed to send shutdown notification to admin: ${e.message}`));
  }

  console.log(`${LOG_PREFIX_SHUTDOWN} Stopping Telegram updates...`);
  if (bot && typeof bot.stopPolling === 'function' && typeof bot.isPolling === 'function' && bot.isPolling()) {
    await bot.stopPolling({ cancel: true }) // cancel: true to reject pending getUpdates
      .then(() => console.log(`✅ ${LOG_PREFIX_SHUTDOWN} Telegram polling stopped.`))
      .catch(e => console.error(`❌ ${LOG_PREFIX_SHUTDOWN} Error stopping Telegram polling: ${e.message}`));
  } else if (bot && typeof bot.deleteWebHook === 'function' && bot.options && !bot.options.polling) { // If using webhook
    await bot.deleteWebHook({ drop_pending_updates: false }) // Try not to drop updates if possible during graceful shutdown
      .then(() => console.log(`✅ ${LOG_PREFIX_SHUTDOWN} Telegram webhook deleted.`))
      .catch(e => console.warn(`⚠️ ${LOG_PREFIX_SHUTDOWN} Error deleting Telegram webhook: ${e.message}`));
  } else {
    console.log(`ℹ️ ${LOG_PREFIX_SHUTDOWN} Telegram bot instance not available or not polling/webhook.`);
  }

  console.log(`${LOG_PREFIX_SHUTDOWN} Clearing background task interval...`);
  if (backgroundTaskIntervalId) clearInterval(backgroundTaskIntervalId);
  backgroundTaskIntervalId = null;
  console.log(`✅ ${LOG_PREFIX_SHUTDOWN} Background task interval cleared.`);

  // Add any other cleanup tasks here, e.g., saving active games to DB for recovery (complex)
  console.log(`ℹ️ ${LOG_PREFIX_SHUTDOWN} Processing any final game state settlements (if applicable)...`);
  // For simplicity, we are not implementing game state saving on shutdown here.
  // Active games in memory will be lost. Bets already logged will persist.

  // Short delay to allow any final async operations (like admin notification) to attempt completion
  await sleep(1000);

  console.log(`${LOG_PREFIX_SHUTDOWN} Closing Database pool...`);
  if (pool && typeof pool.end === 'function') {
    await pool.end()
      .then(() => console.log(`✅ ${LOG_PREFIX_SHUTDOWN} Database pool closed.`))
      .catch(e => console.error(`❌ ${LOG_PREFIX_SHUTDOWN} Error closing database pool: ${e.message}`));
  } else {
    console.log(`ℹ️ ${LOG_PREFIX_SHUTDOWN} Database pool not available or already closed.`);
  }

  console.log(`🏁 ${LOG_PREFIX_SHUTDOWN} Graceful shutdown complete (Signal: ${signal}). Bot is exiting.`);
  const exitCode = (String(signal).startsWith('SIG') || signal === 'POLLING_RESTART_FATAL') ? 0 : 1; // Exit 0 for clean signals/handled fatals
  process.exit(exitCode);
}

// Watchdog timer to force exit if graceful shutdown hangs
let shutdownWatchdogTimerId = null;
function startShutdownWatchdog(signal) {
  if (shutdownWatchdogTimerId) clearTimeout(shutdownWatchdogTimerId); // Clear previous watchdog if any
  const timeoutMs = typeof SHUTDOWN_FAIL_TIMEOUT_MS === 'number' ? SHUTDOWN_FAIL_TIMEOUT_MS : 10000; // Default 10s
  shutdownWatchdogTimerId = setTimeout(() => {
    console.error(`🚨🚨 SHUTDOWN TIMEOUT! Forcing exit after ${timeoutMs}ms due to hanging shutdown (Original Signal: ${signal}). 🚨🚨`);
    process.exit(1); // Force exit
  }, timeoutMs);
  if (shutdownWatchdogTimerId?.unref) shutdownWatchdogTimerId.unref(); // Don't let timer keep Node alive
}

// --- Main Startup Function ---
async function main() {
  const LOG_PREFIX_MAIN = "🚀 [Startup]";
  console.log(`\n${LOG_PREFIX_MAIN} Initializing Group Chat Casino Bot v${BOT_VERSION} 🚀🚀🚀`);
  console.log(`${LOG_PREFIX_MAIN} Timestamp: ${new Date().toISOString()}`);
  console.log(`${LOG_PREFIX_MAIN} Process ID (PID): ${process.pid}`);
  console.log(`${LOG_PREFIX_MAIN} Node.js Version: ${process.version}`);
  console.log(`${LOG_PREFIX_MAIN} Hostname: ${process.env.HOSTNAME || 'local/unknown'}`);


  console.log(`${LOG_PREFIX_MAIN} Setting up process signal & global error handlers...`);
  process.on('SIGINT', () => { if (!isShuttingDown) { startShutdownWatchdog('SIGINT'); shutdown('SIGINT'); } });
  process.on('SIGTERM', () => { if (!isShuttingDown) { startShutdownWatchdog('SIGTERM'); shutdown('SIGTERM'); } });

  process.on('uncaughtException', async (error, origin) => {
    console.error(`\n🚨🚨🚨 UNCAUGHT EXCEPTION [Origin: ${origin}] 🚨🚨🚨\nName: ${error.name}\nMessage: ${error.message}\nStack:\n${error.stack}\n`);
    if (!isShuttingDown) { // Only attempt graceful shutdown if not already shutting down
      if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
        await safeSendMessage(ADMIN_USER_ID, `🚨 BOT CRASH (Uncaught Exception on ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}) 🚨\nOrigin: ${escapeMarkdownV2(String(origin))}\nError: ${escapeMarkdownV2(String(error.message || error))}\nAttempting shutdown...`, { parse_mode: 'MarkdownV2' }).catch(()=>{});
      }
      startShutdownWatchdog('uncaughtException'); // Start watchdog before attempting shutdown
      shutdown('uncaughtException').catch(() => process.exit(1)); // If shutdown itself fails, exit
    } else {
      console.warn(`${LOG_PREFIX_MAIN} Uncaught exception occurred *during* shutdown. Forcing exit.`);
      process.exit(1); // Force exit if already shutting down
    }
  });

  process.on('unhandledRejection', async (reason, promise) => {
    console.error('\n🔥🔥🔥 UNHANDLED PROMISE REJECTION 🔥🔥🔥');
    if (reason instanceof Error) {
      console.error(`Reason: ${reason.name} - ${reason.message}\nStack:\n${reason.stack}`);
    } else {
      console.error('Reason:', reason);
    }
    // console.error('Promise:', promise); // Logging the promise can be verbose

    // Avoid sending admin notifications for unhandled rejections during shutdown if they are related to cleanup tasks
    if (!isShuttingDown && ADMIN_USER_ID && typeof safeSendMessage === "function") {
      const reasonMsg = reason instanceof Error ? reason.message : String(reason);
      const stackInfo = reason instanceof Error ? `\nStack (first few lines): ${reason.stack.split('\n').slice(0,3).join('\n')}` : '';
      await safeSendMessage(ADMIN_USER_ID, `🔥🔥 UNHANDLED REJECTION on ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}\nReason: ${escapeMarkdownV2(reasonMsg)}${escapeMarkdownV2(stackInfo)}`, { parse_mode: 'MarkdownV2' }).catch(()=>{});
    }
    // Note: Per Node.js docs, unhandled rejections might terminate the process in future versions.
    // It's best to handle all promise rejections. For now, we log and continue, unless it's during startup.
  });
  console.log(`✅ ${LOG_PREFIX_MAIN} Process signal and global error handlers set up.`);

  await initializeDatabase(); // Ensure DB schema is ready
  console.log(`✅ ${LOG_PREFIX_MAIN} Database initialization sequence completed.`);

  try {
    console.log(`${LOG_PREFIX_MAIN} Connecting to Telegram and setting up bot listeners...`);
    if (!bot || typeof bot.getMe !== 'function') {
      throw new Error("Telegram bot instance ('bot') or 'bot.getMe' failed to initialize prior to this point.");
    }

    bot.on('polling_error', async (error) => {
      console.error(`\n🚫 TELEGRAM POLLING ERROR 🚫 Code: ${error.code || 'N/A'} | Message: ${error.message}`);
      // console.error(error); // Full error object for details
      if (String(error.message).toLowerCase().includes('conflict')) { // Typically 409 Conflict
        console.error("FATAL: 409 Conflict polling error. Another bot instance might be running with the same token. Shutting down THIS instance.");
        if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
          await safeSendMessage(ADMIN_USER_ID, `🚨 BOT CONFLICT (e.g., 409) on ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}. Shutting down this instance. Ensure only one bot instance is running with this token. Error: ${escapeMarkdownV2(String(error.message || error))}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
        }
        if (!isShuttingDown) { startShutdownWatchdog('POLLING_CONFLICT_ERROR'); shutdown('POLLING_CONFLICT_ERROR').catch(() => process.exit(1)); }
      } else if (String(error.code).includes('EFATAL') || String(error.message).toLowerCase().includes('efatal')) {
        console.error("FATAL POLLING ERROR (EFATAL). This is usually unrecoverable. Shutting down.", error);
        if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
          await safeSendMessage(ADMIN_USER_ID, `🚨 BOT FATAL POLLING ERROR (EFATAL) on ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}. Shutting down. Error: ${escapeMarkdownV2(error.message)}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
        }
        if (!isShuttingDown) { startShutdownWatchdog('POLLING_EFATAL_ERROR'); shutdown('POLLING_EFATAL_ERROR').catch(() => process.exit(1)); }
      } else {
        // For other polling errors, attempt to restart polling
        if (typeof attemptRestartPolling === 'function') {
          attemptRestartPolling(error);
        } else {
          console.error("attemptRestartPolling function is not defined. Cannot recover from polling error automatically.");
          // Consider a shutdown if retries are not possible and polling is essential
        }
      }
    });

    bot.on('webhook_error', async (error) => { // If using webhooks instead of polling
        console.error(`\n🚫 TELEGRAM WEBHOOK ERROR 🚫 Code: ${error.code || 'N/A'} | Message: ${error.message}`);
        // console.error(error);
        if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
          await safeSendMessage(ADMIN_USER_ID, `⚠️ BOT WEBHOOK ERROR on ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}\n${escapeMarkdownV2(error.message || String(error))}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
        }
        // Webhook errors might require manual intervention or specific restart logic for the webhook server
    });

    bot.on('error', async (error) => { // General library errors
      console.error('\n🔥 TELEGRAM BOT LIBRARY GENERIC ERROR EVENT 🔥:', error);
      if (ADMIN_USER_ID && typeof safeSendMessage === "function") {
        await safeSendMessage(ADMIN_USER_ID, `⚠️ BOT LIBRARY ERROR on ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}\n${escapeMarkdownV2(error.message || String(error))}`, {parse_mode: 'MarkdownV2'}).catch(()=>{});
      }
    });
    console.log(`✅ ${LOG_PREFIX_MAIN} Core Telegram event listeners (polling_error, error) attached.`);

    const me = await bot.getMe();
    console.log(`✅ ${LOG_PREFIX_MAIN} Successfully connected to Telegram! Bot Name: ${me.first_name}, Username: @${me.username} (ID: ${me.id})`);

    if (ADMIN_USER_ID && typeof safeSendMessage === 'function') {
      await safeSendMessage(ADMIN_USER_ID, `🎉 Bot v${BOT_VERSION} started successfully! 🎉\nPID: ${process.pid}\nHost: ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}\nPolling active. Casino is open! 🎲`, { parse_mode: 'MarkdownV2' });
    }
    console.log(`\n🎉 Casino Bot is now fully operational! Waiting for commands and interactions...`);

    // Start periodic background tasks
    if (typeof runPeriodicBackgroundTasks === 'function') {
      runPeriodicBackgroundTasks(); // Run once on startup
      backgroundTaskIntervalId = setInterval(runPeriodicBackgroundTasks, BACKGROUND_TASK_INTERVAL_MS);
      console.log(`ℹ️ ${LOG_PREFIX_MAIN} Periodic background tasks scheduled to run every ${BACKGROUND_TASK_INTERVAL_MS / 60000} minutes.`);
    }

  } catch (error) {
    console.error(`❌ CRITICAL STARTUP FAILURE (${LOG_PREFIX_MAIN}): ${error.message}`, error.stack);
    // Attempt to notify admin even on startup failure using a temporary bot instance if main one failed
    if (ADMIN_USER_ID && BOT_TOKEN && typeof escapeMarkdownV2 === 'function' && typeof TelegramBot !== 'undefined' && !bot) { // if main 'bot' failed
      try {
        console.warn(`${LOG_PREFIX_MAIN} Main bot instance failed. Attempting emergency admin notification with temporary bot.`);
        const tempBot = new TelegramBot(BOT_TOKEN, {}); // No polling for temp bot
        await tempBot.sendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE Bot v${BOT_VERSION} on ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}:\n${escapeMarkdownV2(error.message)}\nBot is exiting. Check logs immediately.`, {parse_mode:'MarkdownV2'});
      } catch (tempBotError) {
        console.error(`${LOG_PREFIX_MAIN} Emergency admin notification also failed:`, tempBotError.message);
      }
    } else if (ADMIN_USER_ID && typeof safeSendMessage === 'function' && bot) { // if main 'bot' is there but getMe or something else failed
         await safeSendMessage(ADMIN_USER_ID, `🆘 CRITICAL STARTUP FAILURE Bot v${BOT_VERSION} on ${escapeMarkdownV2(process.env.HOSTNAME || 'local')}:\n${escapeMarkdownV2(error.message)}\nBot is exiting. Check logs immediately.`, {parse_mode:'MarkdownV2'}).catch(()=>{});
    }

    if (!isShuttingDown) { // Ensure shutdown is called if not already in process
      startShutdownWatchdog('STARTUP_MAIN_CATCH_FAILURE');
      shutdown('STARTUP_MAIN_CATCH_FAILURE').catch(() => process.exit(1)); // If shutdown fails, force exit
    } else {
      process.exit(1); // If already shutting down (e.g. from an earlier uncaughtException), just exit.
    }
  }
}

// --- Final Execution: Start the Bot ---
// This top-level catch is a last resort. Errors should ideally be caught within main() or by global handlers.
main().catch(finalError => {
  console.error("❌❌❌ UNRECOVERABLE ERROR IN MAIN EXECUTION ❌❌❌:", finalError.message, finalError.stack);
  if (!isShuttingDown) {
    // Attempt a desperate shutdown if not already initiated
    startShutdownWatchdog('MAIN_ASYNC_CATCH');
    shutdown('MAIN_ASYNC_CATCH').catch(() => process.exit(1));
  } else {
      process.exit(1); // Force exit if shutdown was already underway
  }
});

console.log("End of index.js script. Bot startup process has been initiated.");
// --- End of Part 6 ---
