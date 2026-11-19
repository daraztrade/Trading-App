const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env'), override: true });
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const crypto = require('crypto');
const multer = require('multer');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '2mb' }));

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
const staticOpts = {
  extensions: ['html'],
  setHeaders(res, filePath) {
    if (String(filePath).endsWith('.html') || String(filePath).endsWith('.js')) {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    }
  }
};

if (PORTAL_MODE === 'ADMIN') {
  app.get('/', (req, res) => res.redirect('/login'));
  app.use('/', express.static(ADMIN_DIR, staticOpts));
} else if (PORTAL_MODE === 'TRADER') {
  app.use('/admin', (req, res) => res.status(404).send('Not Found'));
  app.use('/', express.static(TRADER_DIR, staticOpts));
} else {
  app.get('/admin', (req, res) => res.redirect('/admin/login'));
  app.get('/login.html', (req, res) => res.redirect(302, '/index.html'));
  app.get('/login', (req, res) => res.redirect(302, '/index.html'));
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
const upload = multer({
  storage,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = ['image/jpeg', 'image/png', 'image/webp'];
    if (!allowed.includes(file.mimetype)) {
      return cb(new Error('Only JPG, PNG, and WEBP images are allowed.'));
    }
    cb(null, true);
  }
});

// ==========================================================================
// VALIDATION HELPERS
// ==========================================================================
const isValidEmail = (email) => /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(String(email || '').trim());
const isValidUsername = (u) => /^[a-zA-Z0-9_]{3,24}$/.test(String(u || '').trim());
const cleanStr = (v, max = 200) => String(v || '').trim().slice(0, max);

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
// SUPABASE (POSTGRES) DATABASE CLIENT & QUERY HELPERS
// ==========================================================================
function resolveDatabaseUrl() {
  let url =
    process.env.DATABASE_URL ||
    process.env.SUPABASE_DB_URL ||
    process.env.SUPABASE_DATABASE_URL ||
    '';
  // Session pooler (:5432) hits max clients quickly; prefer Transaction mode (:6543).
  if (url.includes('.pooler.supabase.com:5432/')) {
    url = url.replace('.pooler.supabase.com:5432/', '.pooler.supabase.com:6543/');
  }
  return url;
}

const DATABASE_URL = resolveDatabaseUrl();

if (!DATABASE_URL) {
  console.error(
    'Missing DATABASE_URL. Set it in Trading-App/.env to your Supabase Postgres connection string.'
  );
}

const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : undefined,
  max: process.env.VERCEL ? 1 : 3,
  idleTimeoutMillis: 5000,
  connectionTimeoutMillis: 15000,
  allowExitOnIdle: true
});
pool.on('error', (err) => {
  console.error('Unexpected Postgres pool error:', err.message);
});

function toPg(sql, args = []) {
  let i = 0;
  const text = String(sql).replace(/\?/g, () => `$${++i}`);
  return { text, values: args };
}

const CAMEL_COLS = {
  firstname: 'firstName',
  lastname: 'lastName',
  invitationcode: 'invitationCode',
  profilecompleted: 'profileCompleted',
  inorderbalance: 'inOrderBalance',
  frozenbalance: 'frozenBalance',
  totalwithdrawn: 'totalWithdrawn',
  creditscore: 'creditScore',
  tradebanenabled: 'tradeBanEnabled',
  tradebanmessage: 'tradeBanMessage',
  withdrawbanenabled: 'withdrawBanEnabled',
  withdrawbanmessage: 'withdrawBanMessage',
  canapprovewithdrawals: 'canApproveWithdrawals',
  tradeoutcome: 'tradeOutcome',
  emailverified: 'emailVerified',
  mobileverified: 'mobileVerified',
  kycverified: 'kycVerified',
  createdat: 'createdAt',
  userid: 'userId',
  cryptocurrency: 'cryptoCurrency',
  walletaddress: 'walletAddress',
  txhash: 'txHash',
  proofurl: 'proofUrl',
  reviewedby: 'reviewedBy',
  accountdetails: 'accountDetails',
  rejectreason: 'rejectReason',
  postbalance: 'postBalance',
  targetuserid: 'targetUserId',
  sentby: 'sentBy',
  actorusername: 'actorUsername',
  actorrole: 'actorRole',
  useragent: 'userAgent',
  payoutpercent: 'payoutPercent',
  durationseconds: 'durationSeconds',
  entryprice: 'entryPrice',
  exitprice: 'exitPrice',
  outcomeoverride: 'outcomeOverride',
  expiresat: 'expiresAt',
  settledat: 'settledAt',
  subadminid: 'subAdminId',
  balancefrozen: 'balanceFrozen',
  balancefreezemessage: 'balanceFreezeMessage',
  repliedby: 'repliedBy',
  createdby: 'createdBy',
  createdbyusername: 'createdByUsername',
  usedbyuserid: 'usedByUserId',
  usedbyusername: 'usedByUsername',
  usedat: 'usedAt',
  maxuses: 'maxUses',
  usecount: 'useCount'
};

function mapRow(row) {
  if (!row) return null;
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    out[CAMEL_COLS[k] || k] = v;
  }
  return out;
}

async function dbRun(sql, args = []) {
  const q = toPg(sql, args);
  return pool.query(q.text, q.values);
}

async function dbGet(sql, args = []) {
  const q = toPg(sql, args);
  const res = await pool.query(q.text, q.values);
  return res.rows[0] ? mapRow(res.rows[0]) : null;
}

async function dbAll(sql, args = []) {
  const q = toPg(sql, args);
  const res = await pool.query(q.text, q.values);
  return res.rows.map(mapRow);
}

async function dbBatch(stmts) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const stmt of stmts) {
      const sql = typeof stmt === 'string' ? stmt : stmt.sql;
      const args = typeof stmt === 'string' ? [] : (stmt.args || []);
      const q = toPg(sql, args);
      await client.query(q.text, q.values);
    }
    await client.query('COMMIT');
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }
}

const defaultSettings = {
  defaultCurrency: 'PKR',
  pkrRate: 278.5,
  currencies: [
    { code: 'PKR', symbol: 'Rs ', name: 'Pakistani Rupee', rateToBase: 278.5 },
    { code: 'USDT', symbol: '$', name: 'Tether USDT', rateToBase: 1 },
    { code: 'BTC', symbol: '₿', name: 'Bitcoin', rateToBase: 0.000011 },
    { code: 'ETH', symbol: 'Ξ', name: 'Ethereum', rateToBase: 0.00032 }
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
    if (!DATABASE_URL) {
      throw new Error('DATABASE_URL is not set. Add your Supabase Postgres URI to Trading-App/.env');
    }
    await dbBatch([
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
        currency TEXT DEFAULT 'PKR',
        invitationCode TEXT DEFAULT '',
        subAdminId TEXT DEFAULT '',
        profileCompleted INTEGER DEFAULT 0,
        status TEXT DEFAULT 'ACTIVE',
        balance REAL DEFAULT 0,
        inOrderBalance REAL DEFAULT 0,
        frozenBalance REAL DEFAULT 0,
        totalWithdrawn REAL DEFAULT 0,
        creditScore INTEGER DEFAULT 100,
        tradeBanEnabled INTEGER DEFAULT 0,
        tradeBanMessage TEXT DEFAULT 'Trading is restricted on your account.',
        withdrawBanEnabled INTEGER DEFAULT 0,
        withdrawBanMessage TEXT DEFAULT 'Withdrawals are restricted on your account.',
        balanceFrozen INTEGER DEFAULT 0,
        balanceFreezeMessage TEXT DEFAULT 'Your balance is frozen. You can see the amount, but withdrawals are not allowed.',
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
      `CREATE TABLE IF NOT EXISTS invite_links (
        code TEXT PRIMARY KEY,
        createdBy TEXT NOT NULL,
        createdByUsername TEXT NOT NULL,
        label TEXT DEFAULT '',
        maxUses INTEGER NOT NULL DEFAULT 1,
        useCount INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'ACTIVE',
        usedByUserId TEXT DEFAULT '',
        usedByUsername TEXT DEFAULT '',
        usedAt TEXT DEFAULT '',
        createdAt TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        userId TEXT NOT NULL,
        username TEXT NOT NULL,
        pair TEXT NOT NULL,
        currency TEXT DEFAULT 'PKR',
        direction TEXT NOT NULL,
        stake REAL NOT NULL,
        payoutPercent REAL NOT NULL,
        durationSeconds INTEGER NOT NULL,
        entryPrice REAL NOT NULL,
        exitPrice REAL,
        profit REAL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'RUNNING',
        outcomeOverride TEXT DEFAULT '',
        createdAt BIGINT NOT NULL,
        expiresAt BIGINT NOT NULL,
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
        currency TEXT DEFAULT 'PKR',
        method TEXT NOT NULL,
        accountDetails TEXT NOT NULL,
        rejectReason TEXT DEFAULT '',
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
    ]);

    const safeAlter = [
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS tradeOutcome TEXT DEFAULT 'WIN_DEFAULT'",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS address TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS city TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS state TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS zip TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS emailVerified INTEGER DEFAULT 1",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS mobileVerified INTEGER DEFAULT 1",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS kycVerified INTEGER DEFAULT 1",
      "ALTER TABLE trades ADD COLUMN IF NOT EXISTS outcomeOverride TEXT DEFAULT ''",
      "ALTER TABLE trades ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'PKR'",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS subAdminId TEXT DEFAULT ''",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS frozenBalance REAL DEFAULT 0",
      "ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS rejectReason TEXT DEFAULT ''",
      "ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'PKR'",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS balanceFrozen INTEGER DEFAULT 0",
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS balanceFreezeMessage TEXT DEFAULT 'Your balance is frozen. You can see the amount, but withdrawals are not allowed.'"
    ];
    for (const sql of safeAlter) {
      try { await dbRun(sql); } catch (_) {}
    }

    const indexQueries = [
      'CREATE INDEX IF NOT EXISTS idx_trades_user_created ON trades (userId, createdAt DESC)',
      'CREATE INDEX IF NOT EXISTS idx_trades_status_expires ON trades (status, expiresAt)',
      'CREATE INDEX IF NOT EXISTS idx_withdrawals_user_created ON withdrawals (userId, createdAt DESC)',
      'CREATE INDEX IF NOT EXISTS idx_deposits_user_created ON deposits (userId, createdAt DESC)',
      'CREATE INDEX IF NOT EXISTS idx_transactions_user_ts ON transactions (userId, timestamp DESC)',
      'CREATE INDEX IF NOT EXISTS idx_tickets_user_created ON tickets (userId, createdAt DESC)'
    ];
    for (const sql of indexQueries) {
      try { await dbRun(sql); } catch (_) {}
    }

    try {
      await dbRun('ALTER TABLE trades ALTER COLUMN createdAt TYPE BIGINT USING "createdat"::bigint');
      await dbRun('ALTER TABLE trades ALTER COLUMN expiresAt TYPE BIGINT USING "expiresat"::bigint');
    } catch (_) {
      try {
        await dbRun('ALTER TABLE trades ALTER COLUMN createdat TYPE BIGINT USING createdat::bigint');
        await dbRun('ALTER TABLE trades ALTER COLUMN expiresat TYPE BIGINT USING expiresat::bigint');
      } catch (__) {}
    }

    const countRow = await dbGet('SELECT COUNT(*) as c FROM settings');
    if (!countRow || Number(countRow.c) === 0) {
      await saveSettings(defaultSettings);
    }

    const existingMain = await dbGet(
      'SELECT id FROM users WHERE LOWER(username) = ? OR LOWER(email) = ?',
      ['mainadmin', 'mainadmin@daraztrade.online']
    );
    if (!existingMain) {
      await dbRun(
        `INSERT INTO users (id, username, email, password, role, firstName, lastName, status, invitationCode, canApproveWithdrawals, createdAt)
         VALUES (?, ?, ?, ?, 'MAIN_ADMIN', 'Main', 'Admin', 'ACTIVE', 'MAINADMIN', 1, ?)`,
        ['admin-main', 'mainadmin', 'mainadmin@daraztrade.online', hashPassword('admin321'), new Date().toISOString()]
      );
    }

    const existingAdmin = await dbGet(
      'SELECT id FROM users WHERE LOWER(username) IN (?, ?) OR LOWER(email) = ?',
      ['admin', 'ajmanzoor', 'admin@daraztrade.online']
    );
    if (!existingAdmin) {
      await dbRun(
        `INSERT INTO users (id, username, email, password, role, firstName, lastName, status, invitationCode, canApproveWithdrawals, createdAt)
         VALUES (?, ?, ?, ?, 'SUB_ADMIN', 'Platform', 'Admin', 'ACTIVE', 'ADMIN', 1, ?)`,
        ['admin-sub', 'admin', 'admin@daraztrade.online', hashPassword('admin123'), new Date().toISOString()]
      );
    }

    const existingDemo = await dbGet('SELECT id FROM users WHERE username = ?', ['demo_trader']);
    if (!existingDemo) {
      await dbRun(
        `INSERT INTO users (id, username, email, password, role, firstName, lastName, country, mobile, currency, profileCompleted, status, balance, creditScore, createdAt)
         VALUES (?, ?, ?, ?, 'TRADER', 'Demo', 'Trader', 'Pakistan', '+923001234567', 'PKR', 1, 'ACTIVE', 50000, 100, ?)`,
        ['usr-demo-1', 'demo_trader', 'trader@daraztrade.online', hashPassword('Trader@123'), new Date().toISOString()]
      );
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
    console.error('Supabase DB Init Error:', err);
    res.status(500).json({ error: 'Database connection error.' });
  }
});

async function getSettings() {
  const rows = await dbAll('SELECT key, value FROM settings');
  const result = { ...defaultSettings };
  for (const r of rows) {
    result[r.key] = JSON.parse(r.value);
  }
  // Preferred/account currencies must always include PKR (and stay usable for signup).
  const defaults = defaultSettings.currencies;
  const incoming = Array.isArray(result.currencies) ? result.currencies : [];
  const byCode = new Map();
  for (const c of [...defaults, ...incoming]) {
    if (!c || !c.code) continue;
    byCode.set(String(c.code).toUpperCase(), {
      code: String(c.code).toUpperCase(),
      symbol: c.symbol || c.code,
      name: c.name || c.code,
      rateToBase: Number(c.rateToBase) || 1
    });
  }
  // Keep core account currencies first: PKR, USDT, BTC, ETH
  const core = ['PKR', 'USDT', 'BTC', 'ETH'];
  const ordered = [];
  for (const code of core) {
    if (byCode.has(code)) {
      ordered.push(byCode.get(code));
      byCode.delete(code);
    }
  }
  for (const c of byCode.values()) ordered.push(c);
  result.currencies = ordered;
  result.defaultCurrency = 'PKR';
  return result;
}

async function saveSettings(obj) {
  const stmts = Object.entries(obj).map(([k, v]) => ({
    sql: 'INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value',
    args: [k, JSON.stringify(v)]
  }));
  if (stmts.length) await dbBatch(stmts);
}

function generateTrx() {
  return crypto.randomBytes(6).toString('hex').toUpperCase();
}

function formatUserRow(row) {
  if (!row) return null;
  const safe = { ...row };
  delete safe.password;

  const totalBalance = Number(row.balance || 0);
  const frozenBalance = Math.max(0, Math.min(totalBalance, Number(row.frozenBalance || 0)));
  // Frozen Balance = Total Balance - Available Balance  =>  Available Balance = Total Balance - Frozen Balance
  const availableBalance = Math.max(0, Number((totalBalance - frozenBalance).toFixed(2)));

  return {
    ...safe,
    totalBalance,
    frozenBalance,
    availableBalance,
    balance: availableBalance,
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
    balanceFreeze: {
      enabled: Boolean(row.balanceFrozen) || frozenBalance > 0,
      frozenAmount: frozenBalance,
      message: row.balanceFreezeMessage || 'Your balance is frozen. You can see the amount, but withdrawals are not allowed.'
    },
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
// LIVE MARKET TICK ENGINE (BTC_PKR DEFAULT + ALL *_USDT PAIRS)
// ==========================================================================
const PKR_PER_USD = 278.5;

const marketPrices = {
  'BTC_PKR': 24911895,
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
  return marketPrices[pair] !== undefined ? marketPrices[pair] : null;
}

let lastRealSync = 0;
let isSyncingPrices = false;
let hasInitialPriceSync = false;

async function syncRealBasePrices() {
  if (isSyncingPrices || Date.now() - lastRealSync < 30000) return;
  isSyncingPrices = true;
  lastRealSync = Date.now();
  try {
    const res = await fetch('https://api.binance.com/api/v3/ticker/price', {
      signal: AbortSignal.timeout(4000)
    });
    if (res.ok) {
      const list = await res.json();
      const map = new Map(list.map(item => [item.symbol, Number(item.price)]));
      for (const pair in marketPrices) {
        if (pair === 'BTC_PKR') continue;
        const binanceSymbol = pair.replace('_', '');
        const realVal = map.get(binanceSymbol);
        if (realVal && Number.isFinite(realVal) && realVal > 0) {
          const decimals = realVal < 0.01 ? 6 : realVal < 1 ? 4 : 2;
          const current = marketPrices[pair];
          const diffRatio = Math.abs(realVal - current) / realVal;
          const next = diffRatio > 0.05 ? realVal : current + (realVal - current) * 0.4;
          marketPrices[pair] = Number(next.toFixed(decimals));
        }
      }
      marketPrices['BTC_PKR'] = Math.round(marketPrices['BTC_USDT'] * PKR_PER_USD);
      hasInitialPriceSync = true;
      return;
    }
  } catch (_) {
  } finally {
    isSyncingPrices = false;
  }

  try {
    const cgRes = await fetch(
      'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,binancecoin,solana,ripple,dogecoin,tron,cardano,the-open-network,avalanche-2,polkadot,chainlink,litecoin,bitcoin-cash,shiba-inu,pepe,uniswap,cosmos,stellar,near,aptos,sui,filecoin,ethereum-classic,algorand,arbitrum,optimism,injective-protocol,celestia,sei-network,render-token,fetch-ai,aave,maker,vechain,hedera-hashgraph,internet-computer,the-graph,the-sandbox,decentraland,axie-infinity,theta-token,elrond-erd-2,tezos,eos,kava&vs_currencies=usd',
      { signal: AbortSignal.timeout(4000) }
    );
    if (cgRes.ok) {
      const cg = await cgRes.json();
      const cgMap = {
        BTC_USDT: cg.bitcoin?.usd,
        ETH_USDT: cg.ethereum?.usd,
        BNB_USDT: cg.binancecoin?.usd,
        SOL_USDT: cg.solana?.usd,
        XRP_USDT: cg.ripple?.usd,
        DOGE_USDT: cg.dogecoin?.usd,
        TRX_USDT: cg.tron?.usd,
        ADA_USDT: cg.cardano?.usd,
        TON_USDT: cg['the-open-network']?.usd,
        AVAX_USDT: cg['avalanche-2']?.usd,
        DOT_USDT: cg.polkadot?.usd,
        LINK_USDT: cg.chainlink?.usd,
        LTC_USDT: cg.litecoin?.usd,
        BCH_USDT: cg['bitcoin-cash']?.usd,
        SHIB_USDT: cg['shiba-inu']?.usd,
        PEPE_USDT: cg.pepe?.usd,
        UNI_USDT: cg.uniswap?.usd,
        ATOM_USDT: cg.cosmos?.usd,
        XLM_USDT: cg.stellar?.usd,
        NEAR_USDT: cg.near?.usd,
        APT_USDT: cg.aptos?.usd,
        SUI_USDT: cg.sui?.usd,
        FIL_USDT: cg.filecoin?.usd,
        ETC_USDT: cg['ethereum-classic']?.usd,
        ALGO_USDT: cg.algorand?.usd,
        ARB_USDT: cg.arbitrum?.usd,
        OP_USDT: cg.optimism?.usd,
        INJ_USDT: cg['injective-protocol']?.usd,
        TIA_USDT: cg.celestia?.usd,
        SEI_USDT: cg['sei-network']?.usd,
        RENDER_USDT: cg['render-token']?.usd,
        FET_USDT: cg['fetch-ai']?.usd,
        AAVE_USDT: cg.aave?.usd,
        MKR_USDT: cg.maker?.usd,
        VET_USDT: cg.vechain?.usd,
        HBAR_USDT: cg['hedera-hashgraph']?.usd,
        ICP_USDT: cg['internet-computer']?.usd,
        GRT_USDT: cg['the-graph']?.usd,
        SAND_USDT: cg['the-sandbox']?.usd,
        MANA_USDT: cg.decentraland?.usd,
        AXS_USDT: cg['axie-infinity']?.usd,
        THETA_USDT: cg['theta-token']?.usd,
        EGLD_USDT: cg['elrond-erd-2']?.usd,
        XTZ_USDT: cg.tezos?.usd,
        EOS_USDT: cg.eos?.usd,
        KAVA_USDT: cg.kava?.usd
      };
      for (const [pair, realVal] of Object.entries(cgMap)) {
        if (realVal && Number.isFinite(realVal) && realVal > 0 && marketPrices[pair] !== undefined) {
          const decimals = realVal < 0.01 ? 6 : realVal < 1 ? 4 : 2;
          marketPrices[pair] = Number(realVal.toFixed(decimals));
        }
      }
      marketPrices['BTC_PKR'] = Math.round(marketPrices['BTC_USDT'] * PKR_PER_USD);
      hasInitialPriceSync = true;
    }
  } catch (_) {}
}

syncRealBasePrices();

function tickMarketPrices() {
  syncRealBasePrices();
  for (const pair in marketPrices) {
    if (pair === 'BTC_PKR') continue;
    const changePercent = (Math.random() - 0.5) * 0.0024;
    const decimals = marketPrices[pair] < 0.01 ? 6 : marketPrices[pair] < 1 ? 4 : 2;
    marketPrices[pair] = Number((marketPrices[pair] * (1 + changePercent)).toFixed(decimals));
  }
  marketPrices['BTC_PKR'] = Math.round(marketPrices['BTC_USDT'] * PKR_PER_USD);
}

setInterval(() => {
  tickMarketPrices();
  settleExpiredTrades().catch(() => {});
}, 1000);

async function settleOneTrade(trade, settings) {
  const user = await dbGet('SELECT * FROM users WHERE id = ?', [trade.userId]);
  if (!user) return null;

  let exitPrice = getPairPrice(trade.pair) || Number(trade.entryPrice);
  let mode = settings.tradeOutcomeMode || 'WIN_DEFAULT';
  if (user.tradeOutcome === 'FORCE_WIN') mode = 'WIN_DEFAULT';
  if (user.tradeOutcome === 'FORCE_LOSS') mode = 'LOSS_DEFAULT';
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
  const tradeCurr = trade.currency || user.currency || 'PKR';
  const newInOrder = Math.max(0, Number((Number(user.inOrderBalance) - stake).toFixed(2)));
  let profit = -stake;

  if (won) {
    profit = Number((stake * (payoutPercent / 100)).toFixed(2));
    const totalReturn = Number((stake + profit).toFixed(2));
    const newBalance = Number((Number(user.balance) + totalReturn).toFixed(2));
    const newScore = Math.min(100, Number(user.creditScore) + 5);

    const claimed = await dbRun(
      "UPDATE trades SET status = 'WON', exitPrice = ?, profit = ?, settledAt = ? WHERE id = ? AND status = 'RUNNING'",
      [exitPrice, profit, settledAt, trade.id]
    );
    if (!claimed.rowCount) return null;

    await dbBatch([
      {
        sql: 'UPDATE users SET balance = ?, inOrderBalance = ?, creditScore = ? WHERE id = ?',
        args: [newBalance, newInOrder, newScore, user.id]
      },
      {
        sql: 'INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        args: [generateTrx(), user.id, user.username, 'TRADE_WIN', totalReturn, newBalance, `Trade Win (${trade.pair} [${tradeCurr}] +${payoutPercent}%)`, settledAt]
      }
    ]);
  } else {
    const newScore = Math.max(0, Number(user.creditScore) - 15);
    const claimed = await dbRun(
      "UPDATE trades SET status = 'LOST', exitPrice = ?, profit = ?, settledAt = ? WHERE id = ? AND status = 'RUNNING'",
      [exitPrice, -stake, settledAt, trade.id]
    );
    if (!claimed.rowCount) return null;

    await dbBatch([
      {
        sql: 'UPDATE users SET inOrderBalance = ?, creditScore = ? WHERE id = ?',
        args: [newInOrder, newScore, user.id]
      },
      {
        sql: 'INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        args: [generateTrx(), user.id, user.username, 'TRADE_LOSS', -stake, Number(user.balance), `Trade Loss (${trade.pair} [${tradeCurr}])`, settledAt]
      }
    ]);
  }

  return {
    ...trade,
    status: won ? 'WON' : 'LOST',
    exitPrice,
    profit,
    settledAt
  };
}

async function settleExpiredTrades() {
  const now = Date.now();
  const expired = await dbAll("SELECT * FROM trades WHERE status = 'RUNNING' AND expiresAt <= ?", [now]);
  if (!expired.length) return;

  const settings = await getSettings();
  for (const trade of expired) {
    await settleOneTrade(trade, settings);
  }
  broadcastEvent('TRADE_SETTLED');
}

// ==========================================================================
// AUTH MIDDLEWARE, RATE LIMITER & SIGNED TOKENS
// ==========================================================================
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.createHash('sha256').update(process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.DATABASE_URL || 'daraz-trade-hmac-secret-2026').digest('hex');

function signSession(user) {
  const payload = Buffer.from(JSON.stringify({
    id: user.id,
    username: user.username,
    role: user.role,
    exp: Date.now() + 7 * 24 * 60 * 60 * 1000
  })).toString('base64url');
  const sig = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('base64url');
  return `${payload}.${sig}`;
}

function verifySession(token) {
  if (!token || !token.includes('.')) return null;
  const [payload, sig] = token.split('.');
  const expected = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('base64url');
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  try {
    const data = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    if (!data.exp || Date.now() > data.exp) return null;
    return data;
  } catch {
    return null;
  }
}

function requireAuth(roles = []) {
  return async (req, res, next) => {
    try {
      const token = req.headers['x-session-token'];
      const claims = verifySession(token);
      if (!claims?.id) {
        return res.status(401).json({ error: 'Unauthorized. Please log in.' });
      }
      const effectiveId = claims.id;

      const row = await dbGet('SELECT * FROM users WHERE id = ?', [effectiveId]);
      if (!row) return res.status(401).json({ error: 'Unauthorized. Please log in.' });

      const user = formatUserRow(row);
      user._rawPassword = row.password;
      if (user.status === 'BANNED') {
        return res.status(403).json({ error: 'Your account is banned. Contact Help Center.' });
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

const authHits = new Map();
function rateLimitAuth(req, res, next) {
  const ip = (req.headers['x-forwarded-for'] || req.socket?.remoteAddress || 'unknown').toString().split(',')[0].trim();
  const now = Date.now();
  const windowMs = 15 * 60 * 1000;
  const maxAttempts = 20;
  const entry = authHits.get(ip);
  if (!entry || now > entry.resetAt) {
    authHits.set(ip, { count: 1, resetAt: now + windowMs });
    return next();
  }
  entry.count += 1;
  if (entry.count > maxAttempts) {
    return res.status(429).json({ error: 'Too many attempts. Please try again in 15 minutes.' });
  }
  next();
}
app.use('/api/auth/login', rateLimitAuth);
app.use('/api/auth/register', rateLimitAuth);

// ==========================================================================
// TRADER & PUBLIC ROUTES
// ==========================================================================
app.get('/api/public/config', async (req, res) => {
  const s = await getSettings();
  // Full currency list (PKR first via getSettings), always default to PKR.
  res.json({
    currencies: (s.currencies && s.currencies.length) ? s.currencies : defaultSettings.currencies,
    defaultCurrency: 'PKR',
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
  const username = cleanStr(req.body.username, 24);
  const email = cleanStr(req.body.email, 120).toLowerCase();
  const country = cleanStr(req.body.country || 'Pakistan', 60);
  const mobile = cleanStr(req.body.mobile, 25);
  const currency = cleanStr(req.body.currency || 'PKR', 10);
  const password = String(req.body.password || '');
  const invitationCode = cleanStr(req.body.invitationCode, 40).toUpperCase();

  if (!isValidUsername(username)) {
    return res.status(400).json({ error: 'Username must be 3–24 characters (letters, numbers, or underscore).' });
  }
  if (!isValidEmail(email)) {
    return res.status(400).json({ error: 'Please enter a valid email address.' });
  }
  if (password.length < 6) {
    return res.status(400).json({ error: 'Password must be at least 6 characters long.' });
  }
  if (!mobile || mobile.replace(/\D/g, '').length < 7) {
    return res.status(400).json({ error: 'Please enter a valid mobile number.' });
  }

  const exists = await dbGet('SELECT id FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(email) = LOWER(?)', [username, email]);
  if (exists) return res.status(400).json({ error: 'Username or email already exists.' });

  let subAdminId = '';
  let usedSingleInvite = null;

  if (invitationCode) {
    // 1. Check single-use unique invitation links first
    const inviteLink = await dbGet('SELECT * FROM invite_links WHERE UPPER(code) = ?', [invitationCode]);
    if (inviteLink) {
      if (inviteLink.status !== 'ACTIVE' || Number(inviteLink.useCount) >= Number(inviteLink.maxUses || 1)) {
        return res.status(400).json({ error: 'This invitation link has already been used or expired (1 user limit).' });
      }
      subAdminId = inviteLink.createdBy;
      usedSingleInvite = inviteLink;
    } else {
      // 2. Fallback to admin static invitation code
      const inviter = await dbGet(
        "SELECT id FROM users WHERE role IN ('MAIN_ADMIN', 'SUB_ADMIN') AND UPPER(invitationCode) = ?",
        [invitationCode]
      );
      if (!inviter) {
        return res.status(400).json({ error: 'Invalid or already-used invitation link.' });
      }
      subAdminId = inviter.id;
    }
  }

  const id = 'usr-' + Date.now();
  const nowIso = new Date().toISOString();

  if (usedSingleInvite) {
    const claimedLink = await dbRun(
      `UPDATE invite_links
       SET useCount = useCount + 1, status = 'USED', usedByUserId = ?, usedByUsername = ?, usedAt = ?
       WHERE code = ? AND status = 'ACTIVE' AND useCount < maxUses`,
      [id, username, nowIso, usedSingleInvite.code]
    );
    if (!claimedLink.rowCount) {
      return res.status(400).json({ error: 'This invitation link has already been claimed by another user.' });
    }
  }

  await dbRun(
    `INSERT INTO users (id, username, email, password, role, country, mobile, currency, invitationCode, subAdminId, createdAt)
     VALUES (?, ?, ?, ?, 'TRADER', ?, ?, ?, ?, ?, ?)`,
    [id, username, email, hashPassword(password), country, mobile, currency, invitationCode, subAdminId, nowIso]
  );

  const user = formatUserRow(await dbGet('SELECT * FROM users WHERE id = ?', [id]));
  broadcastEvent('NEW_USER', { username });
  res.json({ message: 'Account created', user, token: signSession(user) });
});

app.post('/api/auth/complete-profile', requireAuth(['TRADER']), async (req, res) => {
  const firstName = cleanStr(req.body.firstName, 50);
  const lastName = cleanStr(req.body.lastName, 50);
  if (!firstName || !lastName) {
    return res.status(400).json({ error: 'First name and last name are required.' });
  }
  await dbRun('UPDATE users SET firstName = ?, lastName = ?, profileCompleted = 1 WHERE id = ?', [firstName, lastName, req.user.id]);
  const user = formatUserRow(await dbGet('SELECT * FROM users WHERE id = ?', [req.user.id]));
  res.json({ message: 'Profile completed', user });
});

app.post('/api/auth/login', async (req, res) => {
  const login = cleanStr(req.body.login, 120);
  const password = String(req.body.password || '');
  const portal = cleanStr(req.body.portal, 20);

  if (!login || !password) return res.status(400).json({ error: 'Username/email and password are required.' });

  const row = await dbGet('SELECT * FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(email) = LOWER(?)', [login, login]);
  if (!row || !verifyPassword(password, row.password)) {
    return res.status(401).json({ error: 'Invalid credentials.' });
  }

  const user = formatUserRow(row);
  if (user.status === 'BANNED') return res.status(403).json({ error: 'Your account is banned. Contact Help Center.' });
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
  if (!newPassword || String(newPassword).length < 6) {
    return res.status(400).json({ error: 'New password must be at least 6 characters.' });
  }
  if (!verifyPassword(oldPassword, req.user._rawPassword)) {
    return res.status(400).json({ error: 'Current password is incorrect.' });
  }
  await dbRun('UPDATE users SET password = ? WHERE id = ?', [hashPassword(newPassword), req.user.id]);
  res.json({ message: 'Password updated successfully.' });
});

app.post('/api/trader/currency', requireAuth(['TRADER']), async (req, res) => {
  const currency = cleanStr(req.body.currency, 10).toUpperCase();
  const settings = await getSettings();
  const allowed = new Set((settings.currencies || []).map((c) => String(c.code || '').toUpperCase()));
  ['PKR', 'USDT', 'BTC', 'ETH'].forEach((c) => allowed.add(c));
  if (!allowed.has(currency)) {
    return res.status(400).json({ error: 'Unsupported currency.' });
  }
  await dbRun('UPDATE users SET currency = ? WHERE id = ?', [currency, req.user.id]);
  res.json({ message: 'Currency updated', currency });
});

app.get('/api/trader/dashboard', requireAuth(['TRADER']), async (req, res) => {
  if (!hasInitialPriceSync) {
    await syncRealBasePrices();
  }
  tickMarketPrices();
  await settleExpiredTrades();
  const [userRow, settings, trades, transactions, tickets, customErrors, withdrawals] = await Promise.all([
    dbGet('SELECT * FROM users WHERE id = ?', [req.user.id]),
    getSettings(),
    dbAll('SELECT * FROM trades WHERE userId = ? ORDER BY createdAt DESC LIMIT 50', [req.user.id]),
    dbAll('SELECT * FROM transactions WHERE userId = ? ORDER BY timestamp DESC LIMIT 25', [req.user.id]),
    dbAll('SELECT * FROM tickets WHERE userId = ? ORDER BY createdAt DESC', [req.user.id]),
    dbAll("SELECT * FROM custom_errors WHERE active = 1 AND (targetUserId = 'ALL' OR targetUserId = ?) ORDER BY createdAt DESC", [req.user.id]),
    dbAll('SELECT * FROM withdrawals WHERE userId = ? ORDER BY createdAt DESC LIMIT 30', [req.user.id])
  ]);

  res.json({
    user: formatUserRow(userRow),
    settings,
    prices: marketPrices,
    trades,
    transactions,
    tickets,
    customErrors,
    withdrawals
  });
});

app.post('/api/trader/errors/:id/dismiss', requireAuth(['TRADER']), async (req, res) => {
  await dbRun('UPDATE custom_errors SET active = 0 WHERE id = ? AND (targetUserId = ? OR targetUserId = ?)', [req.params.id, req.user.id, 'ALL']);
  res.json({ ok: true });
});

// Instant settle when countdown ends (avoids 1s interval + poll delay)
app.post('/api/trades/:id/settle', requireAuth(['TRADER']), async (req, res) => {
  const trade = await dbGet('SELECT * FROM trades WHERE id = ? AND userId = ?', [req.params.id, req.user.id]);
  if (!trade) return res.status(404).json({ error: 'Trade not found.' });

  if (trade.status !== 'RUNNING') {
    return res.json({ trade });
  }

  // Allow small early settle so result is ready as countdown hits 0
  if (Number(trade.expiresAt) > Date.now() + 1500) {
    return res.status(400).json({ error: 'Trade has not expired yet.' });
  }

  const settings = await getSettings();
  const settled = await settleOneTrade(trade, settings);
  if (!settled) {
    const latest = await dbGet('SELECT * FROM trades WHERE id = ?', [trade.id]);
    return res.json({ trade: latest || trade });
  }

  broadcastEvent('TRADE_SETTLED', { id: trade.id });
  res.json({ trade: settled });
});

app.post('/api/trades/execute', requireAuth(['TRADER']), async (req, res) => {
  const settings = await getSettings();
  if (settings.globalTradeBan?.enabled) return res.status(403).json({ error: settings.globalTradeBan.message });
  if (req.user.tradeBan?.enabled) {
    return res.status(403).json({ error: req.user.tradeBan.message || 'Trading is banned on your account. Contact support.' });
  }

  const pair = cleanStr(req.body.pair, 20);
  const currency = cleanStr(req.body.currency || req.user.currency || 'PKR', 10).toUpperCase();
  const direction = cleanStr(req.body.direction, 10);
  const amount = Number(req.body.stake);
  const duration = String(req.body.durationSeconds || '60');
  if (!['60', '80', '120'].includes(duration)) return res.status(400).json({ error: 'Invalid trade duration.' });

  const payoutPercent = settings.payoutPercentages[duration] || 25;
  const entryPrice = getPairPrice(pair);

  if (!entryPrice) return res.status(400).json({ error: 'Invalid trading pair.' });
  if (!['UP', 'DOWN'].includes(direction)) return res.status(400).json({ error: 'Invalid direction.' });
  if (!Number.isFinite(amount) || amount < 10) return res.status(400).json({ error: 'Minimum trade amount is 10.' });

  const id = 'TRD-' + generateTrx();
  const createdAt = Date.now();
  const expiresAt = createdAt + Number(duration) * 1000;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Only deduct from unlocked available balance: (balance - frozenBalance) >= amount
    const balRes = await client.query(
      `UPDATE users
       SET balance = ROUND((balance - $1)::numeric, 2),
           inOrderBalance = ROUND((inOrderBalance + $1)::numeric, 2)
       WHERE id = $2 AND (balance - COALESCE(frozenBalance, 0)) >= $1
       RETURNING balance`,
      [amount, req.user.id]
    );
    if (balRes.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Insufficient available balance (frozen funds cannot be traded).' });
    }
    await client.query(
      `INSERT INTO trades (id, userId, username, pair, currency, direction, stake, payoutPercent, durationSeconds, entryPrice, status, createdAt, expiresAt)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'RUNNING', $11, $12)`,
      [id, req.user.id, req.user.username, pair, currency, direction, amount, payoutPercent, Number(duration), entryPrice, createdAt, expiresAt]
    );
    await client.query('COMMIT');
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }

  broadcastEvent('NEW_TRADE', { id, username: req.user.username, pair, currency, stake: amount, direction });
  res.json({
    message: 'Trade started',
    trade: {
      id,
      pair,
      currency,
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
  const cryptoCurrency = cleanStr(req.body.cryptoCurrency, 40);
  const txHash = cleanStr(req.body.txHash, 120);
  const numAmount = Number(req.body.amount);
  if (!cryptoCurrency || !Number.isFinite(numAmount) || numAmount <= 0 || !req.file) {
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
  if (req.user.balanceFreeze?.enabled && req.user.availableBalance <= 0) {
    return res.status(403).json({ error: req.user.balanceFreeze.message || 'Your balance is frozen. You can see the amount, but withdrawals are not allowed.' });
  }
  if (settings.globalWithdrawBan?.enabled) return res.status(403).json({ error: settings.globalWithdrawBan.message });
  if (req.user.withdrawBan?.enabled) {
    return res.status(403).json({ error: req.user.withdrawBan.message || 'Withdrawals are banned on your account. Contact support.' });
  }

  const numAmount = Number(req.body.amount);
  const currency = cleanStr(req.body.currency || req.user.currency || 'PKR', 10).toUpperCase();
  const method = cleanStr(req.body.method, 40);
  const accountDetails = cleanStr(req.body.accountDetails, 240);

  if (!Number.isFinite(numAmount) || numAmount < 100) {
    return res.status(400).json({ error: 'Minimum withdrawal amount is 100.' });
  }
  if (!['JazzCash', 'EasyPaisa', 'Bank'].includes(method) || accountDetails.length < 8) {
    return res.status(400).json({ error: 'Please enter complete withdrawal account details.' });
  }

  const id = 'WDR-' + generateTrx();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Only allow withdrawing from unlocked available balance: (balance - frozenBalance) >= numAmount
    const balRes = await client.query(
      `UPDATE users
       SET balance = ROUND((balance - $1)::numeric, 2)
       WHERE id = $2 AND (balance - COALESCE(frozenBalance, 0)) >= $1
       RETURNING balance`,
      [numAmount, req.user.id]
    );
    if (balRes.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Insufficient available balance (frozen funds cannot be withdrawn).' });
    }
    await client.query(
      `INSERT INTO withdrawals (id, userId, username, amount, currency, method, accountDetails, status, createdAt)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8)`,
      [id, req.user.id, req.user.username, numAmount, currency, method, accountDetails, new Date().toISOString()]
    );
    await client.query('COMMIT');
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }

  broadcastEvent('NEW_WITHDRAWAL', { id, username: req.user.username, amount: numAmount, currency });
  res.json({ message: 'Your withdrawal request is under review.' });
});

app.post('/api/help-center/send', requireAuth(['TRADER']), async (req, res) => {
  const message = cleanStr(req.body.message, 1000);
  if (message.length < 2) return res.status(400).json({ error: 'Please type a message.' });
  const subject = cleanStr(req.body.subject, 120) || (message.length > 48 ? message.slice(0, 48) + '…' : message);

  const id = 'TKT-' + generateTrx();
  await dbRun(
    `INSERT INTO tickets (id, userId, username, subject, message, status, createdAt)
     VALUES (?, ?, ?, ?, ?, 'OPEN', ?)`,
    [id, req.user.id, req.user.username, subject, message, new Date().toISOString()]
  );

  broadcastEvent('NEW_TICKET', { id, username: req.user.username });
  res.json({ message: 'Message sent to Help Center.', ticket: { id, subject, message, status: 'OPEN' } });
});

// ==========================================================================
// ADMIN & SUB-ADMIN ROUTES
// ==========================================================================
app.get('/api/admin/overview', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  await settleExpiredTrades();
  const isMain = req.user.role === 'MAIN_ADMIN';

  const [
    tradersRows,
    staffRows,
    countsRow,
    deposits,
    withdrawals,
    trades,
    tickets,
    auditLogs,
    inviteLinks,
    settings
  ] = await Promise.all([
    dbAll("SELECT * FROM users WHERE role = 'TRADER' ORDER BY createdAt DESC LIMIT 200"),
    isMain ? dbAll("SELECT * FROM users WHERE role != 'TRADER' ORDER BY createdAt DESC") : Promise.resolve([]),
    dbGet(`SELECT
      (SELECT COUNT(*) FROM trades) AS totaltrades,
      (SELECT COUNT(*) FROM trades WHERE status = 'RUNNING') AS runningtrades,
      (SELECT COUNT(*) FROM deposits WHERE status = 'PENDING') AS pendingdeposits,
      (SELECT COUNT(*) FROM withdrawals WHERE status = 'PENDING') AS pendingwithdrawals,
      (SELECT COUNT(*) FROM tickets WHERE status = 'OPEN') AS opentickets`),
    dbAll('SELECT * FROM deposits ORDER BY createdAt DESC LIMIT 60'),
    dbAll('SELECT * FROM withdrawals ORDER BY createdAt DESC LIMIT 100'),
    dbAll('SELECT * FROM trades ORDER BY createdAt DESC LIMIT 100'),
    dbAll('SELECT * FROM tickets ORDER BY createdAt DESC LIMIT 300'),
    dbAll('SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 60'),
    dbAll('SELECT * FROM invite_links ORDER BY createdAt DESC LIMIT 100'),
    getSettings()
  ]);

  const traders = tradersRows.map(formatUserRow);
  const staff = staffRows.map(formatUserRow);
  const myCode = (req.user.invitationCode || req.user.username || '').toUpperCase();
  const invitedCount = traders.filter(
    t => t.subAdminId === req.user.id || (myCode && String(t.invitationCode || '').toUpperCase() === myCode)
  ).length;

  res.json({
    actor: req.user,
    invitedCount,
    stats: {
      totalUsers: traders.length,
      activeUsers: traders.filter(u => u.status === 'ACTIVE').length,
      totalTrades: Number(countsRow?.totaltrades || 0),
      runningTrades: Number(countsRow?.runningtrades || 0),
      pendingDeposits: Number(countsRow?.pendingdeposits || 0),
      pendingWithdrawals: Number(countsRow?.pendingwithdrawals || 0),
      openTickets: Number(countsRow?.opentickets || 0)
    },
    traders,
    staff,
    deposits,
    withdrawals,
    trades,
    tickets,
    auditLogs,
    inviteLinks,
    settings
  });
});

// Single-Use Invite Links Management (Max 1 user registration per link)
app.post('/api/admin/invite-links', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const label = cleanStr(req.body.label || '', 80);
  const customCode = cleanStr(req.body.code || '', 24).toUpperCase().replace(/[^A-Z0-9_-]/g, '');
  const code = customCode.length >= 4
    ? customCode
    : 'INV-' + crypto.randomBytes(4).toString('hex').toUpperCase();

  const clash = await dbGet('SELECT code FROM invite_links WHERE UPPER(code) = ?', [code]);
  if (clash) return res.status(400).json({ error: 'Invite code already exists. Try another.' });

  await dbRun(
    `INSERT INTO invite_links (code, createdBy, createdByUsername, label, maxUses, useCount, status, createdAt)
     VALUES (?, ?, ?, ?, 1, 0, 'ACTIVE', ?)`,
    [code, req.user.id, req.user.username, label, new Date().toISOString()]
  );

  await logAudit(req.user, 'CREATE_SINGLE_INVITE', code, label || '1-user unique invite link');
  res.json({ message: 'Unique 1-user invite link created.', code });
});

app.delete('/api/admin/invite-links/:code', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const code = cleanStr(req.params.code, 40).toUpperCase();
  await dbRun('DELETE FROM invite_links WHERE UPPER(code) = ?', [code]);
  await logAudit(req.user, 'DELETE_INVITE_LINK', code, 'Removed invite link');
  res.json({ message: 'Invite link deleted.' });
});

app.post('/api/admin/my-invite-code', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const code = cleanStr(req.body.invitationCode, 30).toUpperCase().replace(/[^A-Z0-9_-]/g, '');
  if (code.length < 3) {
    return res.status(400).json({ error: 'Invitation code must be at least 3 letters/numbers.' });
  }
  const clash = await dbGet('SELECT id FROM users WHERE UPPER(invitationCode) = ? AND id != ?', [code, req.user.id]);
  if (clash) {
    return res.status(400).json({ error: 'That invitation code is already in use.' });
  }
  await dbRun('UPDATE users SET invitationCode = ? WHERE id = ?', [code, req.user.id]);
  await logAudit(req.user, 'UPDATE_INVITE_CODE', req.user.username, `Code: ${code}`);
  res.json({ message: 'Invitation link updated.', invitationCode: code });
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

// Freeze / Unfreeze Trader Balance (Frozen Balance = Total Balance - Available Balance)
app.post('/api/admin/users/:id/freeze-balance', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const row = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]);
  if (!row) return res.status(404).json({ error: 'Trader not found.' });

  const totalBal = Number(row.balance || 0);
  const action = cleanStr(req.body.action || 'FREEZE', 20).toUpperCase();
  const message = cleanStr(
    req.body.message || row.balanceFreezeMessage || 'Your balance is frozen. You can see the amount, but withdrawals are not allowed.',
    240
  );

  let nextFrozen = 0;
  let nextFlag = 0;
  if (action === 'UNFREEZE') {
    nextFrozen = 0;
    nextFlag = 0;
  } else {
    const reqAmt = req.body.amount !== undefined && req.body.amount !== '' ? Number(req.body.amount) : totalBal;
    if (!Number.isFinite(reqAmt) || reqAmt < 0) {
      return res.status(400).json({ error: 'Enter a valid freeze amount.' });
    }
    nextFrozen = Math.min(totalBal, Number(reqAmt.toFixed(2)));
    nextFlag = nextFrozen > 0 || req.body.freezeAll ? 1 : 0;
  }

  await dbRun(
    'UPDATE users SET frozenBalance = ?, balanceFrozen = ?, balanceFreezeMessage = ? WHERE id = ?',
    [nextFrozen, nextFlag, message, row.id]
  );

  await logAudit(req.user, action === 'UNFREEZE' ? 'UNFREEZE_BALANCE' : 'FREEZE_BALANCE', row.username, `Frozen: ${nextFrozen} / Total: ${totalBal}`);
  broadcastEvent('USER_UPDATED');
  res.json({
    message: action === 'UNFREEZE' ? 'Trader balance unfrozen.' : `Frozen balance set to ${nextFrozen.toLocaleString()}.`,
    frozenBalance: nextFrozen,
    availableBalance: Math.max(0, Number((totalBal - nextFrozen).toFixed(2)))
  });
});

app.post('/api/admin/users/:id/profile', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const row = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]);
  if (!row) return res.status(404).json({ error: 'Trader not found.' });

  const {
    firstName, lastName, email, mobile, country, address, city, state, zip,
    creditScore, status, tradeOutcome, emailVerified, mobileVerified, kycVerified,
    tradeBan, withdrawBan, balanceFreeze
  } = req.body;

  if (email && !isValidEmail(email)) return res.status(400).json({ error: 'Invalid email address.' });

  let nextStatus = status || row.status;
  if (nextStatus === 'FROZEN') nextStatus = 'ACTIVE';

  const totalBal = Number(row.balance || 0);
  const freezeEnabled = Boolean(balanceFreeze?.enabled);
  const nextFrozen = freezeEnabled
    ? (balanceFreeze?.frozenAmount !== undefined ? Math.min(totalBal, Math.max(0, Number(balanceFreeze.frozenAmount))) : totalBal)
    : 0;

  await dbRun(
    `UPDATE users SET
      firstName = ?, lastName = ?, email = ?, mobile = ?, country = ?,
      address = ?, city = ?, state = ?, zip = ?,
      creditScore = ?, status = ?, tradeOutcome = ?,
      emailVerified = ?, mobileVerified = ?, kycVerified = ?,
      tradeBanEnabled = ?, tradeBanMessage = ?,
      withdrawBanEnabled = ?, withdrawBanMessage = ?,
      frozenBalance = ?, balanceFrozen = ?, balanceFreezeMessage = ?
     WHERE id = ?`,
    [
      cleanStr(firstName ?? row.firstName, 60),
      cleanStr(lastName ?? row.lastName, 60),
      cleanStr(email ?? row.email, 120),
      cleanStr(mobile ?? row.mobile, 30),
      cleanStr(country ?? row.country, 60),
      cleanStr(address ?? row.address ?? '', 180),
      cleanStr(city ?? row.city ?? '', 60),
      cleanStr(state ?? row.state ?? '', 60),
      cleanStr(zip ?? row.zip ?? '', 20),
      creditScore !== undefined ? Math.max(0, Math.min(100, Number(creditScore))) : Number(row.creditScore),
      nextStatus,
      tradeOutcome || row.tradeOutcome || 'WIN_DEFAULT',
      emailVerified ? 1 : 0,
      mobileVerified ? 1 : 0,
      kycVerified ? 1 : 0,
      tradeBan?.enabled ? 1 : 0,
      cleanStr(tradeBan?.message ?? row.tradeBanMessage ?? '', 240),
      withdrawBan?.enabled ? 1 : 0,
      cleanStr(withdrawBan?.message ?? row.withdrawBanMessage ?? '', 240),
      nextFrozen,
      freezeEnabled ? 1 : 0,
      cleanStr(balanceFreeze?.message || row.balanceFreezeMessage || 'Your balance is frozen. You can see the amount, but withdrawals are not allowed.', 240),
      row.id
    ]
  );

  await logAudit(req.user, 'UPDATE_USER_DETAIL', row.username, `Outcome: ${tradeOutcome || 'WIN_DEFAULT'}, Status: ${nextStatus}`);
  broadcastEvent('USER_UPDATED');
  res.json({ message: 'User profile & controls saved.' });
});

app.post('/api/admin/users/:id/trade-mode', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const mode = req.body.tradeOutcome;
  if (!['FORCE_WIN', 'FORCE_LOSS'].includes(mode)) {
    return res.status(400).json({ error: 'Trade mode must be win or lose.' });
  }

  const row = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]);
  if (!row) return res.status(404).json({ error: 'Trader not found.' });

  await dbRun('UPDATE users SET tradeOutcome = ? WHERE id = ?', [mode, row.id]);
  await logAudit(req.user, 'SET_USER_TRADE_MODE', row.username, mode);
  broadcastEvent('USER_UPDATED');
  res.json({ message: mode === 'FORCE_WIN' ? 'User set to win.' : 'User set to lose.', tradeOutcome: mode });
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
  const status = action === 'APPROVE' ? 'APPROVED' : 'REJECTED';

  const claimed = await dbRun(
    "UPDATE deposits SET status = ?, reviewedBy = ? WHERE id = ? AND status = 'PENDING'",
    [status, req.user.username, req.params.id]
  );
  if (!claimed.rowCount) return res.status(400).json({ error: 'Invalid or already reviewed deposit.' });

  const deposit = await dbGet('SELECT * FROM deposits WHERE id = ?', [req.params.id]);
  if (status === 'APPROVED' && deposit) {
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
  const status = action === 'APPROVE' ? 'APPROVED' : 'REJECTED';
  const reason = cleanStr(req.body.reason || '', 300);
  if (status === 'REJECTED' && !reason) {
    return res.status(400).json({ error: 'Write a reason before rejecting this withdrawal.' });
  }

  const claimed = await dbRun(
    "UPDATE withdrawals SET status = ?, rejectReason = ? WHERE id = ? AND status = 'PENDING'",
    [status, status === 'REJECTED' ? reason : '', req.params.id]
  );
  if (!claimed.rowCount) return res.status(400).json({ error: 'Invalid or already reviewed withdrawal.' });

  const withdrawal = await dbGet('SELECT * FROM withdrawals WHERE id = ?', [req.params.id]);
  const trader = withdrawal ? await dbGet('SELECT * FROM users WHERE id = ?', [withdrawal.userId]) : null;
  if (trader) {
    if (status === 'APPROVED') {
      const newWithdrawn = Number((Number(trader.totalWithdrawn) + Number(withdrawal.amount)).toFixed(2));
      await dbRun('UPDATE users SET totalWithdrawn = ? WHERE id = ?', [newWithdrawn, trader.id]);
      await dbRun(
        'INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [generateTrx(), trader.id, trader.username, 'WITHDRAW', -Number(withdrawal.amount), Number(trader.balance), `Withdrawal Approved (${withdrawal.currency || 'PKR'} · ${withdrawal.method}: ${withdrawal.accountDetails})`, new Date().toISOString()]
      );
    } else {
      const refundedBal = Number((Number(trader.balance) + Number(withdrawal.amount)).toFixed(2));
      await dbRun('UPDATE users SET balance = ? WHERE id = ?', [refundedBal, trader.id]);
    }
  }

  await logAudit(req.user, `WITHDRAWAL_${status}`, withdrawal.username, status === 'REJECTED' ? `${withdrawal.id}: ${reason}` : withdrawal.id);
  broadcastEvent(status === 'REJECTED' ? 'WITHDRAWAL_REJECTED' : 'WITHDRAWAL_UPDATED', { id: withdrawal.id, username: withdrawal.username });
  res.json({ message: `Withdrawal ${status}.` });
});

app.post('/api/admin/trades/:id/void', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const claimed = await dbRun("UPDATE trades SET status = 'VOIDED' WHERE id = ? AND status = 'RUNNING'", [req.params.id]);
  if (!claimed.rowCount) return res.status(400).json({ error: 'Trade not running.' });

  const trade = await dbGet('SELECT * FROM trades WHERE id = ?', [req.params.id]);
  const trader = trade ? await dbGet('SELECT * FROM users WHERE id = ?', [trade.userId]) : null;
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
  const reply = cleanStr(req.body.reply, 1000);
  if (!reply) return res.status(400).json({ error: 'Reply cannot be empty.' });

  await dbRun("UPDATE tickets SET reply = ?, repliedBy = ?, status = 'ANSWERED' WHERE id = ?", [reply, req.user.username, ticket.id]);
  await logAudit(req.user, 'REPLY_TICKET', ticket.username, `Replied to ${ticket.id}`);
  broadcastEvent('TICKET_UPDATED');
  res.json({ message: 'Reply sent.' });
});

// Reply inside a user's help chat (latest OPEN ticket, or new admin follow-up)
app.post('/api/admin/help-chats/:userId/reply', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const userId = cleanStr(req.params.userId, 60);
  const reply = cleanStr(req.body.reply, 1000);
  if (!reply) return res.status(400).json({ error: 'Reply cannot be empty.' });

  const user = await dbGet('SELECT * FROM users WHERE id = ?', [userId]);
  if (!user) return res.status(404).json({ error: 'User not found.' });

  const openTicket = await dbGet(
    "SELECT * FROM tickets WHERE userId = ? AND status = 'OPEN' ORDER BY createdAt ASC LIMIT 1",
    [userId]
  );

  if (openTicket) {
    await dbRun(
      "UPDATE tickets SET reply = ?, repliedBy = ?, status = 'ANSWERED' WHERE id = ?",
      [reply, req.user.username, openTicket.id]
    );
    await logAudit(req.user, 'REPLY_TICKET', user.username, `Replied to ${openTicket.id}`);
    broadcastEvent('TICKET_UPDATED');
    return res.json({ message: 'Reply sent.', ticketId: openTicket.id });
  }

  const id = 'TKT-' + generateTrx();
  await dbRun(
    `INSERT INTO tickets (id, userId, username, subject, message, reply, repliedBy, status, createdAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'ANSWERED', ?)`,
    [id, userId, user.username, 'Admin follow-up', '', reply, req.user.username, new Date().toISOString()]
  );
  await logAudit(req.user, 'REPLY_TICKET', user.username, `Admin chat message ${id}`);
  broadcastEvent('TICKET_UPDATED');
  res.json({ message: 'Reply sent.', ticketId: id });
});

app.post('/api/admin/error-sender', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), async (req, res) => {
  const targetUserId = cleanStr(req.body.targetUserId || 'ALL', 60);
  const title = cleanStr(req.body.title || 'System Notice', 120);
  const message = cleanStr(req.body.message, 500);
  if (!message) return res.status(400).json({ error: 'Alert message is required.' });

  const id = 'ERR-' + generateTrx();
  await dbRun(
    `INSERT INTO custom_errors (id, targetUserId, title, message, active, sentBy, createdAt)
     VALUES (?, ?, ?, ?, 1, ?, ?)`,
    [id, targetUserId, title, message, req.user.username, new Date().toISOString()]
  );

  await logAudit(req.user, 'ERROR_SENDER', targetUserId, title);
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
  const newPassword = String(req.body.newPassword || '');
  const trader = await dbGet("SELECT * FROM users WHERE id = ? AND role = 'TRADER'", [req.params.id]);
  if (!trader || newPassword.length < 6) {
    return res.status(400).json({ error: 'Valid user and password (min 6 chars) required.' });
  }

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
    if (!Number.isFinite(delta)) return res.status(400).json({ error: 'Invalid balance amount.' });
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
  const allowedKeys = [
    'defaultCurrency',
    'currencies',
    'taxRate',
    'minCreditScoreForWithdraw',
    'tradeOutcomeMode',
    'payoutPercentages',
    'globalTradeBan',
    'globalWithdrawBan',
    'cryptoWallets'
  ];
  const filtered = {};
  for (const key of allowedKeys) {
    if (req.body[key] !== undefined) {
      filtered[key] = req.body[key];
    }
  }
  await saveSettings(filtered);
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