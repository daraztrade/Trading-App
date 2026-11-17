const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');
const { createClient } = require('@libsql/client');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const UPLOADS_DIR = process.env.VERCEL
  ? path.join('/tmp', 'uploads')
  : path.join(__dirname, 'uploads');

const TRADER_DIR = path.join(__dirname, '../frontend/trader');
const ADMIN_DIR = path.join(__dirname, '../frontend/admin');
const SHARED_DIR = path.join(__dirname, '../frontend/shared');

if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

app.use('/shared', express.static(SHARED_DIR));
app.use('/uploads', express.static(UPLOADS_DIR));

const PORTAL_MODE = process.env.PORTAL_MODE || 'ALL';
const staticOpts = { extensions: ['html'] };

if (PORTAL_MODE === 'ADMIN') {
  app.get('/', (req, res) => res.redirect('/login'));
  app.use('/', express.static(ADMIN_DIR, staticOpts));
} else if (PORTAL_MODE === 'TRADER') {
  app.use('/admin', (req, res) => res.status(404).send('Not Found'));
  app.use('/', express.static(TRADER_DIR, staticOpts));
} else {
  app.get('/admin', (req, res) => res.redirect('/admin/login'));
  app.use('/admin', express.static(ADMIN_DIR, staticOpts));
  app.use('/', express.static(TRADER_DIR, staticOpts));
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOADS_DIR),
  filename: (req, file, cb) => {
    const unique = Date.now() + '-' + Math.round(Math.random() * 1e6);
    cb(null, unique + path.extname(file.originalname));
  }
});
const upload = multer({ storage });

// ==========================================================================
// PASSWORD HASHING (BACKWARD-COMPATIBLE)
// ==========================================================================
function hashPassword(plain) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derived = crypto.scryptSync(String(plain), salt, 64).toString('hex');
  return `scrypt:${salt}:${derived}`;
}

function verifyPassword(plain, stored) {
  if (!stored) return false;
  if (stored.startsWith('scrypt:')) {
    const [, salt, hash] = stored.split(':');
    const derived = crypto.scryptSync(String(plain), salt, 64).toString('hex');
    return crypto.timingSafeEqual(Buffer.from(hash, 'hex'), Buffer.from(derived, 'hex'));
  }
  return String(plain) === String(stored);
}

// ==========================================================================
// REAL-TIME SSE BROADCASTER
// ==========================================================================
const sseClients = new Set();

app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  const client = { res };
  sseClients.add(client);
  res.write(`data: ${JSON.stringify({ type: 'CONNECTED', ts: Date.now() })}\n\n`);

  req.on('close', () => sseClients.delete(client));
});

function broadcastEvent(type, payload = {}) {
  const msg = `data: ${JSON.stringify({ type, payload, ts: Date.now() })}\n\n`;
  for (const client of sseClients) {
    client.res.write(msg);
  }
}

// ==========================================================================
// TURSO (LIBSQL) CLOUD DATABASE CLIENT & QUERY HELPERS
// ==========================================================================
const turso = createClient({
  url: process.env.TURSO_DATABASE_URL || `file:${path.join(__dirname, 'daraz_trade_local.db')}`,
  authToken: process.env.TURSO_AUTH_TOKEN
});

async function dbRun(sql, args = []) {
  return await turso.execute({ sql, args });
}

async function dbGet(sql, args = []) {
  const res = await turso.execute({ sql, args });
  return res.rows[0] ? { ...res.rows[0] } : null;
}

async function dbAll(sql, args = []) {
  const res = await turso.execute({ sql, args });
  return res.rows.map(r => ({ ...r }));
}

const defaultSettings = {
  defaultCurrency: 'USDT',
  currencies: [
    { code: 'BTC', symbol: '₿', name: 'Bitcoin', rateToBase: 1 },
    { code: 'ETH', symbol: 'Ξ', name: 'Ethereum', rateToBase: 1 },
    { code: 'BNB', symbol: 'BNB', name: 'BNB', rateToBase: 1 },
    { code: 'SOL', symbol: 'SOL', name: 'Solana', rateToBase: 1 },
    { code: 'BTS', symbol: 'BTS', name: 'BitShares', rateToBase: 1 },
    { code: 'USDC', symbol: 'USDC', name: 'USD Coin', rateToBase: 1 },
    { code: 'DOGE', symbol: 'DOGE', name: 'Dogecoin', rateToBase: 1 },
    { code: 'TRX', symbol: 'TRX', name: 'TRON', rateToBase: 1 },
    { code: 'XRP', symbol: 'XRP', name: 'Ripple', rateToBase: 1 },
    { code: 'ADA', symbol: 'ADA', name: 'Cardano', rateToBase: 1 },
    { code: 'PI', symbol: 'π', name: 'Pi Network', rateToBase: 1 },
    { code: 'TON', symbol: 'TON', name: 'Toncoin', rateToBase: 1 },
    { code: 'AVAX', symbol: 'AVAX', name: 'Avalanche', rateToBase: 1 },
    { code: 'DOT', symbol: 'DOT', name: 'Polkadot', rateToBase: 1 },
    { code: 'LINK', symbol: 'LINK', name: 'Chainlink', rateToBase: 1 },
    { code: 'MATIC', symbol: 'POL', name: 'Polygon', rateToBase: 1 },
    { code: 'LTC', symbol: 'Ł', name: 'Litecoin', rateToBase: 1 },
    { code: 'BCH', symbol: 'BCH', name: 'Bitcoin Cash', rateToBase: 1 },
    { code: 'SHIB', symbol: 'SHIB', name: 'Shiba Inu', rateToBase: 1 },
    { code: 'PEPE', symbol: 'PEPE', name: 'Pepe', rateToBase: 1 },
    { code: 'UNI', symbol: 'UNI', name: 'Uniswap', rateToBase: 1 },
    { code: 'ATOM', symbol: 'ATOM', name: 'Cosmos', rateToBase: 1 },
    { code: 'XLM', symbol: 'XLM', name: 'Stellar', rateToBase: 1 },
    { code: 'NEAR', symbol: 'NEAR', name: 'NEAR Protocol', rateToBase: 1 },
    { code: 'APT', symbol: 'APT', name: 'Aptos', rateToBase: 1 },
    { code: 'SUI', symbol: 'SUI', name: 'Sui', rateToBase: 1 },
    { code: 'FIL', symbol: 'FIL', name: 'Filecoin', rateToBase: 1 },
    { code: 'ETC', symbol: 'ETC', name: 'Ethereum Classic', rateToBase: 1 },
    { code: 'ALGO', symbol: 'ALGO', name: 'Algorand', rateToBase: 1 },
    { code: 'ARB', symbol: 'ARB', name: 'Arbitrum', rateToBase: 1 },
    { code: 'OP', symbol: 'OP', name: 'Optimism', rateToBase: 1 },
    { code: 'INJ', symbol: 'INJ', name: 'Injective', rateToBase: 1 },
    { code: 'TIA', symbol: 'TIA', name: 'Celestia', rateToBase: 1 },
    { code: 'SEI', symbol: 'SEI', name: 'Sei', rateToBase: 1 },
    { code: 'RENDER', symbol: 'RNDR', name: 'Render', rateToBase: 1 },
    { code: 'FET', symbol: 'FET', name: 'Fetch.ai', rateToBase: 1 },
    { code: 'AAVE', symbol: 'AAVE', name: 'Aave', rateToBase: 1 },
    { code: 'MKR', symbol: 'MKR', name: 'Maker', rateToBase: 1 },
    { code: 'VET', symbol: 'VET', name: 'VeChain', rateToBase: 1 },
    { code: 'HBAR', symbol: 'HBAR', name: 'Hedera', rateToBase: 1 },
    { code: 'ICP', symbol: 'ICP', name: 'Internet Computer', rateToBase: 1 },
    { code: 'GRT', symbol: 'GRT', name: 'The Graph', rateToBase: 1 },
    { code: 'SAND', symbol: 'SAND', name: 'The Sandbox', rateToBase: 1 },
    { code: 'MANA', symbol: 'MANA', name: 'Decentraland', rateToBase: 1 },
    { code: 'AXS', symbol: 'AXS', name: 'Axie Infinity', rateToBase: 1 },
    { code: 'THETA', symbol: 'THETA', name: 'Theta Network', rateToBase: 1 },
    { code: 'EGLD', symbol: 'EGLD', name: 'MultiversX', rateToBase: 1 },
    { code: 'XTZ', symbol: 'XTZ', name: 'Tezos', rateToBase: 1 },
    { code: 'EOS', symbol: 'EOS', name: 'EOS', rateToBase: 1 },
    { code: 'KAVA', symbol: 'KAVA', name: 'Kava', rateToBase: 1 }
  ],
  taxRate: 0,
  minCreditScoreForWithdraw: 90,
  tradeOutcomeMode: 'WIN_DEFAULT',
  payoutPercentages: { "60": 25, "80": 30, "120": 50 },
  globalTradeBan: { enabled: false, message: 'Trading is currently paused for system maintenance.' },
  globalWithdrawBan: { enabled: false, message: 'Withdrawals are temporarily disabled. Please try later.' },
  cryptoWallets: {
    "USDT (TRC20)": "TQn9Y2khEsLJW1ChVWFMSMeRDow5KcbLSE",
    "BTC (Bitcoin)": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
    "ETH (ERC20)": "0x71C7656EC7ab88b098defB751B7401B5f6d8976F",
    "BNB (BEP20)": "bnb1grpf0955h0ykzq3ar5nmum7y6gdfl6lxfn46h2"
  }
};

let dbInitPromise = null;
function ensureDbInitialized() {
  if (dbInitPromise) return dbInitPromise;
  dbInitPromise = (async () => {
    await turso.batch([
      `CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'TRADER',
        firstName TEXT DEFAULT '',
        lastName TEXT DEFAULT '',
        country TEXT DEFAULT 'Pakistan',
        mobile TEXT DEFAULT '',
        currency TEXT DEFAULT 'USDT',
        invitationCode TEXT DEFAULT '',
        profileCompleted INTEGER DEFAULT 0,
        status TEXT DEFAULT 'ACTIVE',
        balance REAL DEFAULT 0,
        inOrderBalance REAL DEFAULT 0,
        totalWithdrawn REAL DEFAULT 0,
        creditScore INTEGER DEFAULT 100,
        tradeBanEnabled INTEGER DEFAULT 0,
        tradeBanMessage TEXT DEFAULT 'Trading is restricted on your account.',
        withdrawBanEnabled INTEGER DEFAULT 0,
        withdrawBanMessage TEXT DEFAULT 'Withdrawals are restricted on your account.',
        canApproveWithdrawals INTEGER DEFAULT 1,
        tradeOutcome TEXT DEFAULT 'WIN_DEFAULT',
        address TEXT DEFAULT '',
        city TEXT DEFAULT '',
        state TEXT DEFAULT '',
        zip TEXT DEFAULT '',
        emailVerified INTEGER DEFAULT 1,
        mobileVerified INTEGER DEFAULT 1,
        kycVerified INTEGER DEFAULT 1,
        createdAt TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        userId TEXT NOT NULL,
        username TEXT NOT NULL,
        pair TEXT NOT NULL,
        direction TEXT NOT NULL,
        stake REAL NOT NULL,
        payoutPercent REAL NOT NULL,
        durationSeconds INTEGER NOT NULL,
        entryPrice REAL NOT NULL,
        exitPrice REAL,
        profit REAL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'RUNNING',
        outcomeOverride TEXT DEFAULT '',
        createdAt INTEGER NOT NULL,
        expiresAt INTEGER NOT NULL,
        settledAt TEXT
      )`,
      `CREATE TABLE IF NOT EXISTS deposits (
        id TEXT PRIMARY KEY,
        userId TEXT NOT NULL,
        username TEXT NOT NULL,
        cryptoCurrency TEXT NOT NULL,
        walletAddress TEXT NOT NULL,
        amount REAL NOT NULL,
        txHash TEXT NOT NULL,
        proofUrl TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        reviewedBy TEXT,
        createdAt TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS withdrawals (
        id TEXT PRIMARY KEY,
        userId TEXT NOT NULL,
        username TEXT NOT NULL,
        amount REAL NOT NULL,
        method TEXT NOT NULL,
        accountDetails TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        createdAt TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS transactions (
        trx TEXT PRIMARY KEY,
        userId TEXT NOT NULL,
        username TEXT NOT NULL,
        type TEXT NOT NULL,
        amount REAL NOT NULL,
        postBalance REAL NOT NULL,
        note TEXT NOT NULL,
        timestamp TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS tickets (
        id TEXT PRIMARY KEY,
        userId TEXT NOT NULL,
        username TEXT NOT NULL,
        subject TEXT NOT NULL,
        message TEXT NOT NULL,
        reply TEXT DEFAULT '',
        repliedBy TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'OPEN',
        createdAt TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS custom_errors (
        id TEXT PRIMARY KEY,
        targetUserId TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT NOT NULL,
        active INTEGER DEFAULT 1,
        sentBy TEXT NOT NULL,
        createdAt TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS audit_logs (
        id TEXT PRIMARY KEY,
        actorUsername TEXT NOT NULL,
        actorRole TEXT NOT NULL,
        action TEXT NOT NULL,
        target TEXT NOT NULL,
        details TEXT NOT NULL,
        timestamp TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS login_logs (
        id TEXT PRIMARY KEY,
        userId TEXT NOT NULL,
        username TEXT NOT NULL,
        ip TEXT NOT NULL,
        userAgent TEXT NOT NULL,
        timestamp TEXT NOT NULL
      )`
    ], 'write');

    const safeAlter = [
      "ALTER TABLE users ADD COLUMN tradeOutcome TEXT DEFAULT 'WIN_DEFAULT'",
      "ALTER TABLE users ADD COLUMN address TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN city TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN state TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN zip TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN emailVerified INTEGER DEFAULT 1",
      "ALTER TABLE users ADD COLUMN mobileVerified INTEGER DEFAULT 1",
      "ALTER TABLE users ADD COLUMN kycVerified INTEGER DEFAULT 1",
      "ALTER TABLE trades ADD COLUMN outcomeOverride TEXT DEFAULT ''"
    ];
    for (const sql of safeAlter) {
      try { await dbRun(sql); } catch (_) {}
    }

    const countRow = await dbGet('SELECT COUNT(*) as c FROM settings');
    if (!countRow || Number(countRow.c) === 0) {
      await saveSettings(defaultSettings);
    } else {
      await saveSettings({
        defaultCurrency: 'USDT',
        currencies: defaultSettings.currencies
      });
    }

    const existingAdmin = await dbGet('SELECT id FROM users WHERE username = ?', ['mainadmin']);
    if (!existingAdmin) {
      await dbRun(
        `INSERT INTO users (id, username, email, password, role, firstName, lastName, status, canApproveWithdrawals, createdAt)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        ['admin-1', 'mainadmin', 'admin@daraztrade.online', hashPassword('admin123'), 'MAIN_ADMIN', 'Main', 'Admin', 'ACTIVE', 1, new Date().toISOString()]
      );
    }

    const dummySeeded = await dbGet("SELECT value FROM settings WHERE key = 'dummyUsersSeeded'");
    if (!dummySeeded) {
      const dummyTraders = [
        {
          id: 'usr-demo-1', username: 'hasnainshah', email: 'hasnain.shah@daraztrade.online',
          firstName: 'Hasnain', lastName: 'Shah', country: 'Pakistan', mobile: '+923001122334',
          balance: 125000, totalWithdrawn: 25000, creditScore: 100, status: 'ACTIVE',
          tradeOutcome: 'WIN_DEFAULT', city: 'Islamabad', state: 'ICT', address: 'F-7 Markaz, Islamabad'
        },
        {
          id: 'usr-demo-2', username: 'sanabatool', email: 'sana.batool@daraztrade.online',
          firstName: 'Sana', lastName: 'Batool', country: 'Pakistan', mobile: '+923214455667',
          balance: 340000, totalWithdrawn: 50000, creditScore: 95, status: 'ACTIVE',
          tradeOutcome: 'WIN_DEFAULT', city: 'Lahore', state: 'Punjab', address: 'Gulberg III, Lahore'
        },
        {
          id: 'usr-demo-3', username: 'anasadaf', email: 'ana.sadaf@daraztrade.online',
          firstName: 'Ana', lastName: 'Sadaf', country: 'Pakistan', mobile: '+923337788990',
          balance: 78500, totalWithdrawn: 0, creditScore: 85, status: 'ACTIVE',
          tradeOutcome: 'FORCE_LOSS', city: 'Karachi', state: 'Sindh', address: 'Clifton Block 4, Karachi'
        },
        {
          id: 'usr-demo-4', username: 'usmanahmed', email: 'usman.ahmed@daraztrade.online',
          firstName: 'Usman', lastName: 'Ahmed', country: 'UAE', mobile: '+971501234567',
          balance: 520000, totalWithdrawn: 110000, creditScore: 100, status: 'ACTIVE',
          tradeOutcome: 'FORCE_WIN', city: 'Dubai', state: 'Dubai', address: 'Business Bay, Dubai'
        }
      ];

      for (let i = 0; i < dummyTraders.length; i++) {
        const d = dummyTraders[i];
        const exists = await dbGet('SELECT id FROM users WHERE username = ?', [d.username]);
        if (!exists) {
          const ts = new Date(Date.now() - (i + 1) * 3600 * 1000 * 3).toISOString();
          await dbRun(
            `INSERT INTO users (id, username, email, password, role, firstName, lastName, country, mobile, currency, profileCompleted, status, balance, totalWithdrawn, creditScore, tradeOutcome, address, city, state, createdAt)
             VALUES (?, ?, ?, ?, 'TRADER', ?, ?, ?, ?, 'USDT', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [d.id, d.username, d.email, hashPassword('123456'), d.firstName, d.lastName, d.country, d.mobile, d.status, d.balance, d.totalWithdrawn, d.creditScore, d.tradeOutcome, d.address, d.city, d.state, ts]
          );
          await dbRun(
            `INSERT OR IGNORE INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp)
             VALUES (?, ?, ?, 'DEPOSIT', ?, ?, 'Initial Account Deposit', ?)`,
            [`DEMO${i + 1}TRX01`, d.id, d.username, d.balance, d.balance, ts]
          );
          await dbRun(
            `INSERT OR IGNORE INTO trades (id, userId, username, pair, direction, stake, payoutPercent, durationSeconds, entryPrice, exitPrice, profit, status, createdAt, expiresAt, settledAt)
             VALUES (?, ?, ?, 'BTC_USDT', 'UP', 1000, 25, 60, 89450.00, 89584.18, 250, 'WON', ?, ?, ?)`,
            [`TRD-DEMO-${i + 1}`, d.id, d.username, Date.now() - 7200000, Date.now() - 7140000, ts]
          );
        }
      }
      await saveSettings({ dummyUsersSeeded: true });
    }
  })().catch(err => {
    dbInitPromise = null;
    throw err;
  });
  return dbInitPromise;
}

app.use('/api', async (req, res, next) => {
  try {
    await ensureDbInitialized();
    next();
  } catch (err) {
    console.error('Turso DB Init Error:', err);
    res.status(500).json({ error: 'Database connection error.' });
  }
});

async function getSettings() {
  const rows = await dbAll('SELECT key, value FROM settings');
  const result = { ...defaultSettings };
  for (const r of rows) {
    result[r.key] = JSON.parse(r.value);
  }
  return result;
}

async function saveSettings(obj) {
  const stmts = Object.entries(obj).map(([k, v]) => ({
    sql: 'INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value',
    args: [k, JSON.stringify(v)]
  }));
  if (stmts.length) await turso.batch(stmts, 'write');
}

function generateTrx() {
  return crypto.randomBytes(6).toString('hex').toUpperCase();
}

function formatUserRow(row) {
  if (!row) return null;
  const safe = { ...row };
  delete safe.password;
  return {
    ...safe,
    balance: Number(row.balance || 0),
    inOrderBalance: Number(row.inOrderBalance || 0),
    totalWithdrawn: Number(row.totalWithdrawn || 0),
    creditScore: Number(row.creditScore ?? 100),
    profileCompleted: Boolean(row.profileCompleted),
    emailVerified: row.emailVerified === undefined ? true : Boolean(row.emailVerified),
    mobileVerified: row.mobileVerified === undefined ? true : Boolean(row.mobileVerified),
    kycVerified: row.kycVerified === undefined ? true : Boolean(row.kycVerified),
    tradeOutcome: row.tradeOutcome || 'WIN_DEFAULT',
    tradeBan: { enabled: Boolean(row.tradeBanEnabled), message: row.tradeBanMessage || 'Trading is restricted on your account.' },
    withdrawBan: { enabled: Boolean(row.withdrawBanEnabled), message: row.withdrawBanMessage || 'Withdrawals are restricted on your account.' },
    permissions: { canApproveWithdrawals: Boolean(row.canApproveWithdrawals) }
  };
}

async function logAudit(actor, action, target, details) {
  await dbRun(
    `INSERT INTO audit_logs (id, actorUsername, actorRole, action, target, details, timestamp)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [generateTrx(), actor.username, actor.role, action, target, details, new Date().toISOString()]
  );
}

// ==========================================================================
// LIVE MARKET TICK ENGINE (ALL *_USDT PAIRS) & TRADE SETTLEMENT
// ==========================================================================
const marketPrices = {
  'BTC_USDT': 89450.25,
  'ETH_USDT': 3120.80,
  'BNB_USDT': 612.40,
  'SOL_USDT': 184.65,
  'BTS_USDT': 0.0042,
  'USDC_USDT': 1.00,
  'DOGE_USDT': 0.3840,
  'TRX_USDT': 0.2415,
  'XRP_USDT': 2.48,
  'ADA_USDT': 0.9420,
  'PI_USDT': 48.50,
  'TON_USDT': 5.62,
  'AVAX_USDT': 38.90,
  'DOT_USDT': 7.45,
  'LINK_USDT': 19.80,
  'MATIC_USDT': 0.5420,
  'LTC_USDT': 104.30,
  'BCH_USDT': 468.50,
  'SHIB_USDT': 0.000024,
  'PEPE_USDT': 0.000018,
  'UNI_USDT': 12.60,
  'ATOM_USDT': 6.85,
  'XLM_USDT': 0.4120,
  'NEAR_USDT': 5.34,
  'APT_USDT': 11.20,
  'SUI_USDT': 3.78,
  'FIL_USDT': 5.15,
  'ETC_USDT': 27.40,
  'ALGO_USDT': 0.3480,
  'ARB_USDT': 0.8920,
  'OP_USDT': 1.94,
  'INJ_USDT': 24.60,
  'TIA_USDT': 5.88,
  'SEI_USDT': 0.4850,
  'RENDER_USDT': 7.92,
  'FET_USDT': 1.48,
  'AAVE_USDT': 285.40,
  'MKR_USDT': 1640.00,
  'VET_USDT': 0.0465,
  'HBAR_USDT': 0.2890,
  'ICP_USDT': 10.75,
  'GRT_USDT': 0.2340,
  'SAND_USDT': 0.5820,
  'MANA_USDT': 0.5410,
  'AXS_USDT': 6.95,
  'THETA_USDT': 2.18,
  'EGLD_USDT': 36.40,
  'XTZ_USDT': 1.24,
  'EOS_USDT': 0.8450,
  'KAVA_USDT': 0.5120
};

function getPairPrice(pair) {
  if (marketPrices[pair] !== undefined) return marketPrices[pair];
  if (pair && pair.endsWith('_USDT')) {
    marketPrices[pair] = 12.50;
    return marketPrices[pair];
  }
  return null;
}

function tickMarketPrices() {
  for (const pair in marketPrices) {
    const changePercent = (Math.random() - 0.5) * 0.0024;
    const decimals = marketPrices[pair] < 0.01 ? 6 : marketPrices[pair] < 1 ? 4 : 2;
    marketPrices[pair] = Number((marketPrices[pair] * (1 + changePercent)).toFixed(decimals));
  }
}

setInterval(() => {
  tickMarketPrices();
  settleExpiredTrades().catch(() => {});
}, 1000);

async function settleExpiredTrades() {
  const now = Date.now();
  const expired = await dbAll("SELECT * FROM trades WHERE status = 'RUNNING' AND expiresAt <= ?", [now]);
  if (!expired.length) return;

  const settings = await getSettings();

  for (const trade of expired) {
    const user = await dbGet('SELECT * FROM users WHERE id = ?', [trade.userId]);
    if (!user) continue;

    let exitPrice = getPairPrice(trade.pair) || Number(trade.entryPrice);
    let mode = settings.tradeOutcomeMode || 'WIN_DEFAULT';
    if (user.tradeOutcome === 'FORCE_WIN' || user.tradeOutcome === 'FORCE_LOSS') {
      mode = user.tradeOutcome === 'FORCE_WIN' ? 'WIN_DEFAULT' : 'LOSS_DEFAULT';
    }
    if (trade.outcomeOverride === 'WIN') mode = 'WIN_DEFAULT';
    if (trade.outcomeOverride === 'LOSS') mode = 'LOSS_DEFAULT';

    let won = false;
    const decimals = Number(trade.entryPrice) < 0.01 ? 6 : Number(trade.entryPrice) < 1 ? 4 : 2;
    if (mode === 'WIN_DEFAULT') {
      won = true;
      exitPrice = trade.direction === 'UP'
        ? Number((Number(trade.entryPrice) * 1.0015).toFixed(decimals))
        : Number((Number(trade.entryPrice) * 0.9985).toFixed(decimals));
    } else if (mode === 'LOSS_DEFAULT') {
      won = false;
      exitPrice = trade.direction === 'UP'
        ? Number((Number(trade.entryPrice) * 0.9985).toFixed(decimals))
        : Number((Number(trade.entryPrice) * 1.0015).toFixed(decimals));
    } else {
      won = (trade.direction === 'UP' && exitPrice > Number(trade.entryPrice)) ||
            (trade.direction === 'DOWN' && exitPrice < Number(trade.entryPrice));
    }

    const settledAt = new Date().toISOString();
    const stake = Number(trade.stake);
    const payoutPercent = Number(trade.payoutPercent);
    const newInOrder = Math.max(0, Number((Number(user.inOrderBalance) - stake).toFixed(2)));

    if (won) {
      const profit = Number((stake * (payoutPercent / 100)).toFixed(2));
      const totalReturn = Number((stake + profit).toFixed(2));
      const newBalance = Number((Number(user.balance) + totalReturn).toFixed(2));
      const newScore = Math.min(100, Number(user.creditScore) + 5);

      await turso.batch([
        {
          sql: 'UPDATE users SET balance = ?, inOrderBalance = ?, creditScore = ? WHERE id = ?',
          args: [newBalance, newInOrder, newScore, user.id]
        },
        {
          sql: "UPDATE trades SET status = 'WON', exitPrice = ?, profit = ?, settledAt = ? WHERE id = ?",
          args: [exitPrice, profit, settledAt, trade.id]
        },
        {
          sql: 'INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
          args: [generateTrx(), user.id, user.username, 'TRADE_WIN', totalReturn, newBalance, `Trade Win (${trade.pair} +${payoutPercent}%)`, settledAt]
        }
      ], 'write');
    } else {
      const newScore = Math.max(0, Number(user.creditScore) - 15);
      await turso.batch([
        {
          sql: 'UPDATE users SET inOrderBalance = ?, creditScore = ? WHERE id = ?',
          args: [newInOrder, newScore, user.id]
        },
        {
          sql: "UPDATE trades SET status = 'LOST', exitPrice = ?, profit = ?, settledAt = ? WHERE id = ?",
          args: [exitPrice, -stake, settledAt, trade.id]
        },
        {
          sql: 'INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
          args: [generateTrx(), user.id, user.username, 'TRADE_LOSS', -stake, Number(user.balance), `Trade Loss (${trade.pair})`, settledAt]
        }
      ], 'write');
    }
  }

  broadcastEvent('TRADE_SETTLED');
}

// ==========================================================================
// AUTH MIDDLEWARE & SIGNED TOKENS
// ==========================================================================
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.createHash('sha256').update(process.env.TURSO_AUTH_TOKEN || 'daraz-trade-hmac-secret-2026').digest('hex');

function signSession(user) {
  const payload = Buffer.from(JSON.stringify({
    id: user.id,
    username: user.username,
    role: user.role
  })).toString('base64url');
  const sig = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('base64url');
  return `${payload}.${sig}`;
}

function verifySession(token) {
  if (!token || !token.includes('.')) return null;
  const [payload, sig] = token.split('.');
  const expected = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('base64url');
  if (sig !== expected) return null;
  try {
    return JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
  } catch {
    return null;
  }
}

function requireAuth(roles = []) {
  return async (req, res, next) => {
    try {
      const userId = req.headers['x-user-id'];
      const token = req.headers['x-session-token'];
      let effectiveId = userId;

      if (!effectiveId && token) {
        const claims = verifySession(token);
        if (claims?.id) effectiveId = claims.id;
      }

      if (!effectiveId) return res.status(401).json({ error: 'Unauthorized. Please log in.' });

      const row = await dbGet('SELECT * FROM users WHERE id = ?', [effectiveId]);
      if (!row) return res.status(401).json({ error: 'Unauthorized. Please log in.' });

      const user = formatUserRow(row);
      user._rawPassword = row.password;
      if (user.status === 'BANNED' || user.status === 'FROZEN') {
        return res.status(403).json({ error: `Account is ${user.status}. Contact Help Center.` });
      }
      if (roles.length && !roles.includes(user.role)) {
        return res.status(403).json({ error: 'Access denied.' });
      }

      req.user = user;
      next();
    } catch (err) {
      next(err);
    }
  };
}

// ==========================================================================
// TRADER & PUBLIC ROUTES
// ==========================================================================
app.get('/api/public/config', async (req, res) => {
  const s = await getSettings();
  res.json({
    currencies: s.currencies,
    defaultCurrency: s.defaultCurrency,
    payoutPercentages: s.payoutPercentages
  });
});

const pakistaniNames = [
  'Shamir K.', 'Javeria N.', 'Zainab S.', 'Bilal A.', 'Hamza R.',
  'Ayesha T.', 'Usman M.', 'Fatima Z.', 'Omer F.', 'Hira B.',
  'Tariq M.', 'Sana K.', 'Daniyal H.', 'Mahnoor R.', 'Kamran J.'
];

app.get('/api/public/live-activity', (req, res) => {
  const name = pakistaniNames[Math.floor(Math.random() * pakistaniNames.length)];
  const actions = ['deposited', 'withdrew', 'won'];
  const action = actions[Math.floor(Math.random() * actions.length)];
  const rawAmount = [1250, 2400, 3800, 5200, 7500, 9400, 12000][Math.floor(Math.random() * 7)];
  const minsAgo = Math.floor(Math.random() * 12) + 1;

  res.json({ id: Date.now(), text: `${name} ${action} $${rawAmount.toLocaleString()} USDT`, action, timeAgo: `${minsAgo} mins ago` });
});

app.post('/api/auth/register', async (req, res) => {
  const { username, email, country, mobile, currency, password, invitationCode } = req.body;
  if (!username || !email || !password) return res.status(400).json({ error: 'Username, email, and password are required.' });

  const exists = await dbGet('SELECT id FROM users WHERE username = ? OR email = ?', [username, email]);
  if (exists) return res.status(400).json({ error: 'Username or email already exists.' });

  const id = 'usr-' + Date.now();
  await dbRun(
    `INSERT INTO users (id, username, email, password, role, country, mobile, currency, invitationCode, createdAt)
     VALUES (?, ?, ?, ?, 'TRADER', ?, ?, ?, ?, ?)`,
    [id, username, email, hashPassword(password), country || 'Pakistan', mobile || '', currency || 'USDT', invitationCode || '', new Date().toISOString()]
  );

  const user = formatUserRow(await dbGet('SELECT * FROM users WHERE id = ?', [id]));
  broadcastEvent('NEW_USER', { username });
  res.json({ message: 'Account created', user, token: signSession(user) });
});

app.post('/api/auth/complete-profile', requireAuth(['TRADER']), async (req, res) => {
  const { firstName, lastName } = req.body;
  await dbRun('UPDATE users SET firstName = ?, lastName = ?, profileCompleted = 1 WHERE id = ?', [firstName, lastName, req.user.id]);
  const user = formatUserRow(await dbGet('SELECT * FROM users WHERE id = ?', [req.user.id]));
  res.json({ message: 'Profile completed', user });
});

app.post('/api/auth/login', async (req, res) => {
  const { login, password, portal } = req.body;
  const row = await dbGet('SELECT * FROM users WHERE username = ? OR email = ?', [login, login]);
  if (!row || !verifyPassword(password, row.password)) {
    return res.status(401).json({ error: 'Invalid credentials.' });
  }

  const user = formatUserRow(row);
  if (user.status !== 'ACTIVE') return res.status(403).json({ error: `Account is ${user.status}.` });
  if (portal === 'ADMIN' && user.role === 'TRADER') return res.status(403).json({ error: 'Traders cannot access the Admin Portal.' });
  if (portal === 'TRADER' && user.role !== 'TRADER') return res.status(403).json({ error: 'Staff must log in via the Admin Portal.' });

  const ip = (req.headers['x-forwarded-for'] || req.socket?.remoteAddress || 'Unknown').toString().split(',')[0].trim();
  const ua = (req.headers['user-agent'] || 'Unknown Browser').slice(0, 120);
  await dbRun(
    `INSERT INTO login_logs (id, userId, username, ip, userAgent, timestamp) VALUES (?, ?, ?, ?, ?, ?)`,
    [generateTrx(), user.id, user.username, ip, ua, new Date().toISOString()]
  ).catch(() => {});

  res.json({ user, token: signSession(user) });
});

app.post('/api/auth/change-password', requireAuth(['TRADER', 'SUB_ADMIN', 'MAIN_ADMIN']), async (req, res) => {
  const { oldPassword, newPassword } = req.body;
  if (!verifyPassword(oldPassword, req.user._rawPassword)) {
    return res.status(400).json({ error: 'Current password is incorrect.' });
  }
  await dbRun('UPDATE users SET password = ? WHERE id = ?', [hashPassword(newPassword), req.user.id]);
  res.json({ message: 'Password updated successfully.' });
});

app.get('/api/trader/dashboard', requireAuth(['TRADER']), async (req, res) => {
  tickMarketPrices();
  await settleExpiredTrades();
  const [userRow, settings, trades, transactions, tickets, customErrors] = await Promise.all([
    dbGet('SELECT * FROM users WHERE id = ?', [req.user.id]),
    getSettings(),
    dbAll('SELECT * FROM trades WHERE userId = ? ORDER BY createdAt DESC LIMIT 50', [req.user.id]),
    dbAll('SELECT * FROM transactions WHERE userId = ? ORDER BY timestamp DESC LIMIT 25', [req.user.id]),
    dbAll('SELECT * FROM tickets WHERE userId = ? ORDER BY createdAt DESC', [req.user.id]),
    dbAll("SELECT * FROM custom_errors WHERE active = 1 AND (targetUserId = 'ALL' OR targetUserId = ?) ORDER BY createdAt DESC", [req.user.id])
  ]);

  res.json({
    user: formatUserRow(userRow),
    settings,
    prices: marketPrices,
    trades,
    transactions,
    tickets,
    customErrors
  });
});

app.post('/api/trader/errors/:id/dismiss', requireAuth(['TRADER']), async (req, res) => {
  await dbRun('UPDATE custom_errors SET active = 0 WHERE id = ? AND targetUserId = ?', [req.params.id, req.user.id]);
  res.json({ ok: true });
});

app.post('/api/trades/execute', requireAuth(['TRADER']), async (req, res) => {
  const settings = await getSettings();
  if (settings.globalTradeBan?.enabled) return res.status(403).json({ error: settings.globalTradeBan.message });
  if (req.user.tradeBan?.enabled) return res.status(403).json({ error: req.user.tradeBan.message });

  const { pair, direction, stake, durationSeconds } = req.body;
  const amount = Number(stake);
  const duration = String(durationSeconds || '60');
  const payoutPercent = settings.payoutPercentages[duration] || 25;
  const entryPrice = getPairPrice(pair);

  if (!entryPrice) return res.status(400).json({ error: 'Invalid trading pair.' });
  if (!['UP', 'DOWN'].includes(direction)) return res.status(400).json({ error: 'Invalid direction.' });
  if (isNaN(amount) || amount <= 0 || req.user.balance < amount) return res.status(400).json({ error: 'Insufficient balance.' });

  const newBal = Number((req.user.balance - amount).toFixed(2));
  const newInOrder = Number((req.user.inOrderBalance + amount).toFixed(2));
  const id = 'TRD-' + generateTrx();
  const createdAt = Date.now();
  const expiresAt = createdAt + Number(duration) * 1000;

  await turso.batch([
    {
      sql: 'UPDATE users SET balance = ?, inOrderBalance = ? WHERE id = ?',
      args: [newBal, newInOrder, req.user.id]
    },
    {
      sql: `INSERT INTO trades (id, userId, username, pair, direction, stake, payoutPercent, durationSeconds, entryPrice, status, createdAt, expiresAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?, ?)`,
      args: [id, req.user.id, req.user.username, pair, direction, amount, payoutPercent, Number(duration), entryPrice, createdAt, expiresAt]
    }
  ], 'write');

  broadcastEvent('NEW_TRADE', { id, username: req.user.username, pair });
  res.json({
    message: 'Trade started',
    trade: {
      id,
      pair,
      direction,
      stake: amount,
      payoutPercent,
      durationSeconds: Number(duration),
      entryPrice,
      status: 'RUNNING',
      createdAt,
      expiresAt
    }
  });
});

app.post('/api/wallet/deposit', requireAuth(['TRADER']), upload.single('proof'), async (req, res) => {
  const { cryptoCurrency, amount, txHash } = req.body;
  const numAmount = Number(amount);
  if (!cryptoCurrency || isNaN(numAmount) || numAmount <= 0 || !req.file) {
    return res.status(400).json({ error: 'Currency, valid amount, and screenshot proof are required.' });
  }

  const settings = await getSettings();
  const id = 'DEP-' + generateTrx();
  await dbRun(
    `INSERT INTO deposits (id, userId, username, cryptoCurrency, walletAddress, amount, txHash, proofUrl, status, createdAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)`,
    [id, req.user.id, req.user.username, cryptoCurrency, settings.cryptoWallets[cryptoCurrency] || 'N/A', numAmount, txHash || 'N/A', `/uploads/${req.file.filename}`, new Date().toISOString()]
  );

  broadcastEvent('NEW_DEPOSIT', { id, username: req.user.username, amount: numAmount });
  res.json({ message: 'Deposit proof submitted for Admin verification.' });
});

app.post('/api/wallet/withdraw', requireAuth(['TRADER']), async (req, res) => {
  const settings = await getSettings();
  if (settings.globalWithdrawBan?.enabled) return res.status(403).json({ error: settings.globalWithdrawBan.message });
  if (req.user.withdrawBan?.enabled) return res.status(403).json({ error: req.user.withdrawBan.message });

  const { amount, method, accountDetails } = req.body;
  const numAmount = Number(amount);
  if (isNaN(numAmount) || numAmount <= 0 || numAmount > req.user.balance) {
    return res.status(400).json({ error: 'Invalid withdrawal amount or insufficient balance.' });
  }

  const newBal = Number((req.user.balance - numAmount).toFixed(2));
  const id = 'WDR-' + generateTrx();

  await turso.batch([
    {
      sql: 'UPDATE users SET balance = ? WHERE id = ?',
      args: [newBal, req.user.id]
    },
    {
      sql: `INSERT INTO withdrawals (id, userId, username, amount, method, accountDetails, status, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)`,
      args: [id, req.user.id, req.user.username, numAmount, method || 'Direct Transfer', accountDetails || '', new Date().toISOString()]
    }
  ], 'write');

  broadcastEvent('NEW_WITHDRAWAL', { id, username: req.user.username, amount: numAmount });
  res.json({ message: 'Withdrawal request submitted.' });
});

app.post('/api/help-center/send', requireAuth(['TRADER']), async (req, res) => {
  const { subject, message } = req.body;
  if (!message) return res.status(400).json({ error: 'Message cannot be empty.' });

  const id = 'TKT-' + generateTrx();
  await dbRun(
    `INSERT INTO tickets (id, userId, username, subject, message, status, createdAt)
     VALUES (?, ?, ?, ?, ?, 'OPEN', ?)`,
    [id, req.user.id, req.user.username, subject || 'General Inquiry', message, new Date().toISOString()]
  );

  broadcastEvent('NEW_TICKET', { id, username: req.user.username });
  res.json({ message: 'Message sent to Help Center.' });
});

// ==========================================================================
// ADMIN & STEALTH MODERATOR ROUTES
// ==========================================================================
app.get('/api/admin/overview', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  await settleExpiredTrades();
  const isMain = req.user.role === 'MAIN_ADMIN';

  const [tradersRows, staffRows, totalTradesRow, runningTradesRow, pendingDepRow, pendingWdrRow, openTktRow, deposits, withdrawals, trades, tickets, auditLogs, settings] = await Promise.all([
    dbAll("SELECT * FROM users WHERE role = 'TRADER' ORDER BY createdAt DESC"),
    isMain ? dbAll("SELECT * FROM users WHERE role = 'SUB_ADMIN' ORDER BY createdAt DESC") : Promise.resolve([]),
    dbGet('SELECT COUNT(*) as c FROM trades'),
    dbGet("SELECT COUNT(*) as c FROM trades WHERE status = 'RUNNING'"),
    dbGet("SELECT COUNT(*) as c FROM deposits WHERE status = 'PENDING'"),
    dbGet("SELECT COUNT(*) as c FROM withdrawals WHERE status = 'PENDING'"),
    dbGet("SELECT COUNT(*) as c FROM tickets WHERE status = 'OPEN'"),
    dbAll('SELECT * FROM deposits ORDER BY createdAt DESC LIMIT 60'),
    dbAll('SELECT * FROM withdrawals ORDER BY createdAt DESC LIMIT 60'),
    dbAll('SELECT * FROM trades ORDER BY createdAt DESC LIMIT 120'),
    dbAll('SELECT * FROM tickets ORDER BY createdAt DESC LIMIT 50'),
    isMain
      ? dbAll('SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 60')
      : dbAll("SELECT * FROM audit_logs WHERE actorRole != 'MAIN_ADMIN' ORDER BY timestamp DESC LIMIT 60"),
    getSettings()
  ]);

  const traders = tradersRows.map(formatUserRow);
  const staff = staffRows.map(formatUserRow);

  res.json({
    actor: req.user,
    stats: {
      totalUsers: traders.length,
      activeUsers: traders.filter(u => u.status === 'ACTIVE').length,
      totalTrades: Number(totalTradesRow?.c || 0),
      runningTrades: Number(runningTradesRow?.c || 0),
      pendingDeposits: Number(pendingDepRow?.c || 0),
      pendingWithdrawals: Number(pendingWdrRow?.c || 0),
      openTickets: Number(openTktRow?.c || 0)
    },
    traders,
    staff,
    deposits,
    withdrawals,
    trades,
    tickets,
    auditLogs,
    settings
  });
});

// MAIN_ADMIN ONLY: Create / Delete Moderator Admin Accounts (Stealth from SUB_ADMINs)
app.post('/api/admin/staff', requireAuth(['MAIN_ADMIN']), async (req, res) => {
  const { username, email, password, firstName, lastName } = req.body;
  if (!username || !email || !password) {
    return res.status(400).json({ error: 'Username, email, and password are required.' });
  }
  const exists = await dbGet('SELECT id FROM users WHERE username = ? OR email = ?', [username, email]);
  if (exists) return res.status(400).json({ error: 'Username or email already exists.' });

  const id = 'mod-' + Date.now();
  await dbRun(
    `INSERT INTO users (id, username, email, password, role, firstName, lastName, status, canApproveWithdrawals, createdAt)
     VALUES (?, ?, ?, ?, 'SUB_ADMIN', ?, ?, 'ACTIVE', 1, ?)`,
    [id, username, email, hashPassword(password), firstName || username, lastName || 'Admin', new Date().toISOString()]
  );

  await logAudit(req.user, 'CREATE_ADMIN', username, 'Created Admin account');
  res.json({ message: 'Admin account created.' });
});

app.delete('/api/admin/staff/:id', requireAuth(['MAIN_ADMIN']), async (req, res) => {
  const mod = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'SUB_ADMIN'", [req.params.id]);
  if (!mod) return res.status(404).json({ error: 'Admin not found.' });
  await dbRun('DELETE FROM users WHERE id = ?', [mod.id]);
  await logAudit(req.user, 'DELETE_ADMIN', mod.username, 'Removed Admin account');
  res.json({ message: 'Admin account removed.' });
});

app.get('/api/admin/users/:id/detail', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  await settleExpiredTrades();
  const [row, trades, deposits, withdrawals, transactions, logins, notifications] = await Promise.all([
    dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]),
    dbAll('SELECT * FROM trades WHERE userId = ? ORDER BY createdAt DESC LIMIT 50', [req.params.id]),
    dbAll('SELECT * FROM deposits WHERE userId = ? ORDER BY createdAt DESC LIMIT 50', [req.params.id]),
    dbAll('SELECT * FROM withdrawals WHERE userId = ? ORDER BY createdAt DESC LIMIT 50', [req.params.id]),
    dbAll('SELECT * FROM transactions WHERE userId = ? ORDER BY timestamp DESC LIMIT 50', [req.params.id]),
    dbAll('SELECT * FROM login_logs WHERE userId = ? ORDER BY timestamp DESC LIMIT 25', [req.params.id]),
    dbAll("SELECT * FROM custom_errors WHERE targetUserId = ? OR targetUserId = 'ALL' ORDER BY createdAt DESC LIMIT 25", [req.params.id])
  ]);
  if (!row) return res.status(404).json({ error: 'Trader not found.' });

  res.json({ user: formatUserRow(row), trades, deposits, withdrawals, transactions, logins, notifications });
});

app.post('/api/admin/users/:id/profile', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const row = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]);
  if (!row) return res.status(404).json({ error: 'Trader not found.' });

  const {
    firstName, lastName, email, mobile, country, address, city, state, zip,
    creditScore, status, tradeOutcome, emailVerified, mobileVerified, kycVerified,
    tradeBan, withdrawBan
  } = req.body;

  await dbRun(
    `UPDATE users SET
      firstName = ?, lastName = ?, email = ?, mobile = ?, country = ?,
      address = ?, city = ?, state = ?, zip = ?,
      creditScore = ?, status = ?, tradeOutcome = ?,
      emailVerified = ?, mobileVerified = ?, kycVerified = ?,
      tradeBanEnabled = ?, tradeBanMessage = ?,
      withdrawBanEnabled = ?, withdrawBanMessage = ?
     WHERE id = ?`,
    [
      firstName ?? row.firstName,
      lastName ?? row.lastName,
      email ?? row.email,
      mobile ?? row.mobile,
      country ?? row.country,
      address ?? row.address ?? '',
      city ?? row.city ?? '',
      state ?? row.state ?? '',
      zip ?? row.zip ?? '',
      creditScore !== undefined ? Math.max(0, Math.min(100, Number(creditScore))) : Number(row.creditScore),
      status || row.status,
      tradeOutcome || row.tradeOutcome || 'WIN_DEFAULT',
      emailVerified ? 1 : 0,
      mobileVerified ? 1 : 0,
      kycVerified ? 1 : 0,
      tradeBan?.enabled ? 1 : 0,
      tradeBan?.message ?? row.tradeBanMessage ?? '',
      withdrawBan?.enabled ? 1 : 0,
      withdrawBan?.message ?? row.withdrawBanMessage ?? '',
      row.id
    ]
  );

  await logAudit(req.user, 'UPDATE_USER_DETAIL', row.username, `Outcome: ${tradeOutcome || 'WIN_DEFAULT'}, Status: ${status || row.status}`);
  broadcastEvent('USER_UPDATED');
  res.json({ message: 'User profile & controls saved.' });
});

app.post('/api/admin/trades/:id/outcome', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const { outcome } = req.body;
  const trade = await dbGet('SELECT * FROM trades WHERE id = ?', [req.params.id]);
  if (!trade || trade.status !== 'RUNNING') return res.status(400).json({ error: 'Trade not running.' });

  await dbRun('UPDATE trades SET outcomeOverride = ? WHERE id = ?', [outcome || '', trade.id]);
  await logAudit(req.user, 'SET_TRADE_OUTCOME', trade.username, `${trade.id} -> ${outcome || 'DEFAULT'}`);
  res.json({ message: `Trade outcome set to ${outcome || 'DEFAULT'}.` });
});

app.post('/api/admin/deposits/:id/review', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const { action } = req.body;
  const deposit = await dbGet('SELECT * FROM deposits WHERE id = ?', [req.params.id]);
  if (!deposit || deposit.status !== 'PENDING') return res.status(400).json({ error: 'Invalid deposit.' });

  const status = action === 'APPROVE' ? 'APPROVED' : 'REJECTED';
  await dbRun('UPDATE deposits SET status = ?, reviewedBy = ? WHERE id = ?', [status, req.user.username, deposit.id]);

  if (status === 'APPROVED') {
    const trader = await dbGet('SELECT * FROM users WHERE id = ?', [deposit.userId]);
    if (trader) {
      const newBal = Number((Number(trader.balance) + Number(deposit.amount)).toFixed(2));
      await dbRun('UPDATE users SET balance = ? WHERE id = ?', [newBal, trader.id]);
      await dbRun(
        'INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [generateTrx(), trader.id, trader.username, 'DEPOSIT', Number(deposit.amount), newBal, `Deposit Approved (${deposit.cryptoCurrency})`, new Date().toISOString()]
      );
    }
  }

  await logAudit(req.user, `DEPOSIT_${status}`, deposit.username, `${deposit.id} (${deposit.amount})`);
  broadcastEvent('DEPOSIT_UPDATED');
  res.json({ message: `Deposit ${status}.` });
});

app.post('/api/admin/withdrawals/:id/review', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const { action } = req.body;
  const withdrawal = await dbGet('SELECT * FROM withdrawals WHERE id = ?', [req.params.id]);
  if (!withdrawal || withdrawal.status !== 'PENDING') return res.status(400).json({ error: 'Invalid withdrawal.' });

  const status = action === 'APPROVE' ? 'APPROVED' : 'REJECTED';
  await dbRun('UPDATE withdrawals SET status = ? WHERE id = ?', [status, withdrawal.id]);

  const trader = await dbGet('SELECT * FROM users WHERE id = ?', [withdrawal.userId]);
  if (trader) {
    if (status === 'APPROVED') {
      const newWithdrawn = Number((Number(trader.totalWithdrawn) + Number(withdrawal.amount)).toFixed(2));
      await dbRun('UPDATE users SET totalWithdrawn = ? WHERE id = ?', [newWithdrawn, trader.id]);
      await dbRun(
        'INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [generateTrx(), trader.id, trader.username, 'WITHDRAW', -Number(withdrawal.amount), Number(trader.balance), `Withdrawal Approved (${withdrawal.method}: ${withdrawal.accountDetails})`, new Date().toISOString()]
      );
    } else {
      const refundedBal = Number((Number(trader.balance) + Number(withdrawal.amount)).toFixed(2));
      await dbRun('UPDATE users SET balance = ? WHERE id = ?', [refundedBal, trader.id]);
    }
  }

  await logAudit(req.user, `WITHDRAWAL_${status}`, withdrawal.username, withdrawal.id);
  broadcastEvent('WITHDRAWAL_UPDATED');
  res.json({ message: `Withdrawal ${status}.` });
});

app.post('/api/admin/trades/:id/void', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const trade = await dbGet('SELECT * FROM trades WHERE id = ?', [req.params.id]);
  if (!trade || trade.status !== 'RUNNING') return res.status(400).json({ error: 'Trade not running.' });

  await dbRun("UPDATE trades SET status = 'VOIDED' WHERE id = ?", [trade.id]);
  const trader = await dbGet('SELECT * FROM users WHERE id = ?', [trade.userId]);
  if (trader) {
    const newInOrder = Math.max(0, Number((Number(trader.inOrderBalance) - Number(trade.stake)).toFixed(2)));
    const newBal = Number((Number(trader.balance) + Number(trade.stake)).toFixed(2));
    await dbRun('UPDATE users SET inOrderBalance = ?, balance = ? WHERE id = ?', [newInOrder, newBal, trader.id]);
  }

  await logAudit(req.user, 'VOID_TRADE', trade.username, `Voided ${trade.id}`);
  broadcastEvent('TRADE_SETTLED');
  res.json({ message: 'Trade voided.' });
});

app.post('/api/admin/tickets/:id/reply', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const ticket = await dbGet('SELECT * FROM tickets WHERE id = ?', [req.params.id]);
  if (!ticket) return res.status(404).json({ error: 'Ticket not found.' });

  await dbRun("UPDATE tickets SET reply = ?, repliedBy = ?, status = 'ANSWERED' WHERE id = ?", [req.body.reply, req.user.username, ticket.id]);
  await logAudit(req.user, 'REPLY_TICKET', ticket.username, `Replied to ${ticket.id}`);
  broadcastEvent('TICKET_UPDATED');
  res.json({ message: 'Reply sent.' });
});

app.post('/api/admin/error-sender', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const { targetUserId, title, message } = req.body;
  const id = 'ERR-' + generateTrx();
  await dbRun(
    `INSERT INTO custom_errors (id, targetUserId, title, message, active, sentBy, createdAt)
     VALUES (?, ?, ?, ?, 1, ?, ?)`,
    [id, targetUserId || 'ALL', title || 'System Notice', message, req.user.username, new Date().toISOString()]
  );

  await logAudit(req.user, 'ERROR_SENDER', targetUserId || 'ALL', title);
  broadcastEvent('ERROR_ALERT');
  res.json({ message: 'Alert sent.' });
});

app.post('/api/admin/users/:id/impersonate', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const row = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]);
  if (!row) return res.status(404).json({ error: 'Trader not found.' });
  const trader = formatUserRow(row);
  await logAudit(req.user, 'LOGIN_AS_USER', trader.username, 'Impersonated trader');
  res.json({ user: trader, token: signSession(trader) });
});

app.post('/api/admin/users/:id/reset-password', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const { newPassword } = req.body;
  const trader = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]);
  if (!trader || !newPassword) return res.status(400).json({ error: 'Valid user and password required.' });

  await dbRun('UPDATE users SET password = ? WHERE id = ?', [hashPassword(newPassword), trader.id]);
  await logAudit(req.user, 'RESET_PASSWORD', trader.username, 'Password changed by Admin');
  res.json({ message: `Password updated for ${trader.username}.` });
});

app.post('/api/admin/users/:id/adjust', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const { balanceDelta, creditScore } = req.body;
  const trader = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]);
  if (!trader) return res.status(404).json({ error: 'Trader not found.' });

  let newBal = Number(trader.balance);
  if (balanceDelta !== undefined && Number(balanceDelta) !== 0) {
    const delta = Number(balanceDelta);
    newBal = Math.max(0, Number((newBal + delta).toFixed(2)));
    await dbRun(
      'INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      [generateTrx(), trader.id, trader.username, delta > 0 ? 'ADMIN_CREDIT' : 'ADMIN_DEBIT', delta, newBal, 'Admin Balance Adjustment', new Date().toISOString()]
    );
  }
  const newScore = creditScore !== undefined ? Math.max(0, Math.min(100, Number(creditScore))) : Number(trader.creditScore);

  await dbRun('UPDATE users SET balance = ?, creditScore = ? WHERE id = ?', [newBal, newScore, trader.id]);
  await logAudit(req.user, 'ADJUST_BALANCE_SCORE', trader.username, `Delta: ${balanceDelta || 0}, Score: ${newScore}`);
  broadcastEvent('USER_UPDATED');
  res.json({ message: 'Trader updated.' });
});

app.post('/api/admin/settings', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  await saveSettings(req.body);
  await logAudit(req.user, 'UPDATE_SETTINGS', 'SYSTEM', 'Updated platform settings');
  broadcastEvent('SETTINGS_UPDATED');
  res.json({ message: 'Settings saved.', settings: await getSettings() });
});

if (require.main === module) {
  ensureDbInitialized().then(() => {
    app.listen(PORT, () => {
      console.log(`Server listening on http://localhost:${PORT}`);
    });
  });
}

module.exports = app;