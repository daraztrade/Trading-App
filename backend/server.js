const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');
const { DatabaseSync } = require('node:sqlite');

const app = express();
const PORT = 3000;

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
app.get('/admin', (req, res) => {
  res.redirect('/admin/login.html');
});

const PORTAL_MODE = process.env.PORTAL_MODE || 'ALL';

if (PORTAL_MODE === 'ADMIN') {
  app.get('/', (req, res) => res.redirect('/login.html'));
  app.use('/', express.static(ADMIN_DIR));
} else if (PORTAL_MODE === 'TRADER') {
  app.use('/admin', (req, res) => res.status(404).send('Not Found'));
  app.use('/', express.static(TRADER_DIR));
} else {
  app.get('/admin', (req, res) => res.redirect('/admin/login.html'));
  app.use('/admin', express.static(ADMIN_DIR));
  app.use('/', express.static(TRADER_DIR));
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
// REAL-TIME SSE BROADCASTER (Instant Sync for Admins, Guards & Traders)
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
// SQLITE RELATIONAL DATABASE INITIALIZATION (daraz_trade.db)
// ==========================================================================
const DB_PATH = process.env.VERCEL
  ? path.join('/tmp', 'daraz_trade.db')
  : path.join(__dirname, 'daraz_trade.db');

const db = new DatabaseSync(DB_PATH);
db.exec('PRAGMA journal_mode = WAL;');
db.exec('PRAGMA foreign_keys = ON;');

db.exec(`
  CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS users (
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
    canApproveWithdrawals INTEGER DEFAULT 0,
    createdAt TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS trades (
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
    createdAt INTEGER NOT NULL,
    expiresAt INTEGER NOT NULL,
    settledAt TEXT
  );

  CREATE TABLE IF NOT EXISTS deposits (
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
  );

  CREATE TABLE IF NOT EXISTS withdrawals (
    id TEXT PRIMARY KEY,
    userId TEXT NOT NULL,
    username TEXT NOT NULL,
    amount REAL NOT NULL,
    method TEXT NOT NULL,
    accountDetails TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    createdAt TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS transactions (
    trx TEXT PRIMARY KEY,
    userId TEXT NOT NULL,
    username TEXT NOT NULL,
    type TEXT NOT NULL,
    amount REAL NOT NULL,
    postBalance REAL NOT NULL,
    note TEXT NOT NULL,
    timestamp TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS tickets (
    id TEXT PRIMARY KEY,
    userId TEXT NOT NULL,
    username TEXT NOT NULL,
    subject TEXT NOT NULL,
    message TEXT NOT NULL,
    reply TEXT DEFAULT '',
    repliedBy TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'OPEN',
    createdAt TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS custom_errors (
    id TEXT PRIMARY KEY,
    targetUserId TEXT NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    active INTEGER DEFAULT 1,
    sentBy TEXT NOT NULL,
    createdAt TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS audit_logs (
    id TEXT PRIMARY KEY,
    actorUsername TEXT NOT NULL,
    actorRole TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    details TEXT NOT NULL,
    timestamp TEXT NOT NULL
  );
`);

// Auto-normalize any legacy 1000-scale scores & 850 withdrawal threshold in existing DBs
db.exec("UPDATE users SET creditScore = 100 WHERE creditScore > 100;");
db.exec("UPDATE settings SET value = '90' WHERE key = 'minCreditScoreForWithdraw' AND value = '850';");

// Settings Helpers
const defaultSettings = {
  defaultCurrency: 'PKR',
currencies: [
    { code: 'INR', symbol: '₹', name: 'Indian Rupee', rateToBase: 0.30 },
    { code: 'BTC', symbol: '₿', name: 'Bitcoin', rateToBase: 0.000000033 },
    { code: 'ETH', symbol: 'Ξ', name: 'Ethereum', rateToBase: 0.00000105 },
    { code: 'BNB', symbol: 'BNB', name: 'BNB', rateToBase: 0.0000055 },
    { code: 'SOL', symbol: 'SOL', name: 'Solana', rateToBase: 0.000018 },
    { code: 'USDC', symbol: 'USDC', name: 'USD Coin', rateToBase: 0.0036 },
    { code: 'BTS', symbol: 'BTS', name: 'BitShares', rateToBase: 0.85 },
    { code: 'DOGE', symbol: 'DOGE', name: 'Dogecoin', rateToBase: 0.015 },
    { code: 'TRX', symbol: 'TRX', name: 'TRON', rateToBase: 0.022 },
    { code: 'PKR', symbol: 'Rs', name: 'Pakistani Rupee', rateToBase: 1 },
    { code: 'XRP', symbol: 'XRP', name: 'Ripple', rateToBase: 0.0065 },
    { code: 'ADA', symbol: 'ADA', name: 'Cardano', rateToBase: 0.0095 },
    { code: 'PI', symbol: 'π', name: 'Pi Network', rateToBase: 0.000085 },
    { code: 'USDT', symbol: '$', name: 'Tether USD', rateToBase: 0.0036 },
    { code: 'BDT', symbol: '৳', name: 'Bangladeshi Taka', rateToBase: 0.43 }
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

function getSettings() {
  const rows = db.prepare('SELECT key, value FROM settings').all();
  const result = { ...defaultSettings };
  for (const r of rows) {
    result[r.key] = JSON.parse(r.value);
  }
  return result;
}

function saveSettings(obj) {
  const upsert = db.prepare('INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value');
  for (const [k, v] of Object.entries(obj)) {
    upsert.run(k, JSON.stringify(v));
  }
}

// Seed default settings & Main Admin if empty
if (db.prepare('SELECT COUNT(*) as c FROM settings').get().c === 0) {
  saveSettings(defaultSettings);
}

const existingAdmin = db.prepare('SELECT id FROM users WHERE username = ?').get('mainadmin');
if (!existingAdmin) {
  db.prepare(`
    INSERT INTO users (id, username, email, password, role, firstName, lastName, status, canApproveWithdrawals, createdAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run('admin-1', 'mainadmin', 'admin@daraztrade.online', 'admin123', 'MAIN_ADMIN', 'Main', 'Admin', 'ACTIVE', 1, new Date().toISOString());
}

function generateTrx() {
  return crypto.randomBytes(6).toString('hex').toUpperCase();
}

function formatUserRow(row) {
  if (!row) return null;
  return {
    ...row,
    profileCompleted: Boolean(row.profileCompleted),
    tradeBan: { enabled: Boolean(row.tradeBanEnabled), message: row.tradeBanMessage },
    withdrawBan: { enabled: Boolean(row.withdrawBanEnabled), message: row.withdrawBanMessage },
    permissions: { canApproveWithdrawals: Boolean(row.canApproveWithdrawals) }
  };
}

function logAudit(actor, action, target, details) {
  db.prepare(`
    INSERT INTO audit_logs (id, actorUsername, actorRole, action, target, details, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(generateTrx(), actor.username, actor.role, action, target, details, new Date().toISOString());
}

// ==========================================================================
// LIVE MARKET TICK ENGINE & TRADE SETTLEMENT
// ==========================================================================
const marketPrices = {
  'BTC/PKR': 29888495.45,
  'ETH/PKR': 945200.00,
  'BNB/PKR': 182450.00,
  'SOL/PKR': 54320.00
};

setInterval(() => {
  for (const pair in marketPrices) {
    const changePercent = (Math.random() - 0.5) * 0.0024;
    marketPrices[pair] = Number((marketPrices[pair] * (1 + changePercent)).toFixed(2));
  }
  settleExpiredTrades();
}, 1000);

function settleExpiredTrades() {
  const now = Date.now();
  const expired = db.prepare("SELECT * FROM trades WHERE status = 'RUNNING' AND expiresAt <= ?").all(now);
  if (!expired.length) return;

  const settings = getSettings();

  for (const trade of expired) {
    const user = db.prepare('SELECT * FROM users WHERE id = ?').get(trade.userId);
    if (!user) continue;

    let exitPrice = marketPrices[trade.pair];
    const mode = settings.tradeOutcomeMode;
    let won = false;

    if (mode === 'WIN_DEFAULT') {
      won = true;
      exitPrice = trade.direction === 'UP'
        ? Number((trade.entryPrice * 1.0015).toFixed(2))
        : Number((trade.entryPrice * 0.9985).toFixed(2));
    } else if (mode === 'LOSS_DEFAULT') {
      won = false;
    } else {
      won = (trade.direction === 'UP' && exitPrice > trade.entryPrice) ||
            (trade.direction === 'DOWN' && exitPrice < trade.entryPrice);
    }

    const settledAt = new Date().toISOString();
    const newInOrder = Math.max(0, Number((user.inOrderBalance - trade.stake).toFixed(2)));

    if (won) {  
      const profit = Number((trade.stake * (trade.payoutPercent / 100)).toFixed(2));
      const totalReturn = Number((trade.stake + profit).toFixed(2));
      const newBalance = Number((user.balance + totalReturn).toFixed(2));
      const newScore = Math.min(100, user.creditScore + 5);

      db.prepare('UPDATE users SET balance = ?, inOrderBalance = ?, creditScore = ? WHERE id = ?')
        .run(newBalance, newInOrder, newScore, user.id);
      db.prepare("UPDATE trades SET status = 'WON', exitPrice = ?, profit = ?, settledAt = ? WHERE id = ?")
        .run(exitPrice, profit, settledAt, trade.id);
      db.prepare('INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
        .run(generateTrx(), user.id, user.username, 'TRADE_WIN', totalReturn, newBalance, `Trade Win (${trade.pair} +${trade.payoutPercent}%)`, settledAt);
    } else {
      const newScore = Math.max(0, user.creditScore - 15);
      db.prepare('UPDATE users SET inOrderBalance = ?, creditScore = ? WHERE id = ?')
        .run(newInOrder, newScore, user.id);
      db.prepare("UPDATE trades SET status = 'LOST', exitPrice = ?, profit = ?, settledAt = ? WHERE id = ?")
        .run(exitPrice, -trade.stake, settledAt, trade.id);
      db.prepare('INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
        .run(generateTrx(), user.id, user.username, 'TRADE_LOSS', -trade.stake, user.balance, `Trade Loss (${trade.pair})`, settledAt);
    }
  }

  broadcastEvent('TRADE_SETTLED');
}

// ==========================================================================
// AUTH MIDDLEWARE
// ==========================================================================
const SESSION_SECRET = process.env.SESSION_SECRET || 'daraz-trade-hmac-secret-2026';

function signSession(user) {
  const payload = Buffer.from(JSON.stringify({
    id: user.id,
    username: user.username,
    email: user.email,
    role: user.role,
    country: user.country,
    currency: user.currency,
    balance: user.balance,
    creditScore: user.creditScore
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
  return (req, res, next) => {
    const userId = req.headers['x-user-id'];
    const token = req.headers['x-session-token'];
    let row = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);

    if (!row && token) {
      const claims = verifySession(token);
      if (claims && claims.id === userId) {
        db.prepare(`
          INSERT OR IGNORE INTO users (id, username, email, password, role, country, currency, status, balance, creditScore, canApproveWithdrawals, createdAt)
          VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?)
        `).run(
          claims.id,
          claims.username,
          claims.email || `${claims.username}@daraztrade.online`,
          'restored',
          claims.role || 'TRADER',
          claims.country || 'Pakistan',
          claims.currency || 'PKR',
          claims.balance ?? 0,
          claims.creditScore ?? 100,
          claims.role === 'MAIN_ADMIN' ? 1 : 0,
          new Date().toISOString()
        );
        row = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);
      }
    }

    if (!row) return res.status(401).json({ error: 'Unauthorized. Please log in.' });

    const user = formatUserRow(row);
    if (user.status === 'BANNED' || user.status === 'FROZEN') {
      return res.status(403).json({ error: `Account is ${user.status}. Contact Help Center.` });
    }
    if (roles.length && !roles.includes(user.role)) {
      return res.status(403).json({ error: 'Access denied.' });
    }

    req.user = user;
    next();
  };
}

// ==========================================================================
// TRADER & PUBLIC ROUTES
// ==========================================================================
app.get('/api/public/config', (req, res) => {
  const s = getSettings();
  res.json({
    currencies: s.currencies,
    defaultCurrency: s.defaultCurrency,
    payoutPercentages: s.payoutPercentages
  });
});

// Live Pakistani Activity Feed (Display Only)
const pakistaniNames = [
  'Shamir K.', 'Javeria N.', 'Zainab S.', 'Bilal A.', 'Hamza R.',
  'Ayesha T.', 'Usman M.', 'Fatima Z.', 'Omer F.', 'Hira B.',
  'Tariq M.', 'Sana K.', 'Daniyal H.', 'Mahnoor R.', 'Kamran J.'
];

app.get('/api/public/live-activity', (req, res) => {
  const name = pakistaniNames[Math.floor(Math.random() * pakistaniNames.length)];
  const actions = ['deposited', 'withdrew', 'won'];
  const action = actions[Math.floor(Math.random() * actions.length)];
  const lacAmounts = [
    115000, 145000, 180000, 210000, 250000,
    295000, 340000, 385000, 420000, 475000,
    530000, 610000, 740000, 850000
  ];
  const rawAmount = lacAmounts[Math.floor(Math.random() * lacAmounts.length)];
  const amount = rawAmount.toLocaleString('en-PK');
  const minsAgo = Math.floor(Math.random() * 12) + 1;

  res.json({ id: Date.now(), text: `${name} ${action} PKR ${amount}`, action, timeAgo: `${minsAgo} mins ago` });
});

app.post('/api/auth/register', (req, res) => {
  const { username, email, country, mobile, currency, password, invitationCode } = req.body;
  if (!username || !email || !password) return res.status(400).json({ error: 'Username, email, and password are required.' });

  const exists = db.prepare('SELECT id FROM users WHERE username = ? OR email = ?').get(username, email);
  if (exists) return res.status(400).json({ error: 'Username or email already exists.' });

  const id = 'usr-' + Date.now();
  db.prepare(`
    INSERT INTO users (id, username, email, password, role, country, mobile, currency, invitationCode, createdAt)
    VALUES (?, ?, ?, ?, 'TRADER', ?, ?, ?, ?, ?)
  `).run(id, username, email, password, country || 'Pakistan', mobile || '', currency || 'PKR', invitationCode || '', new Date().toISOString());

  const user = formatUserRow(db.prepare('SELECT * FROM users WHERE id = ?').get(id));
  broadcastEvent('NEW_USER', { username });
  res.json({ message: 'Account created', user, token: signSession(user) });
});

app.post('/api/auth/complete-profile', requireAuth(['TRADER']), (req, res) => {
  const { firstName, lastName } = req.body;
  db.prepare('UPDATE users SET firstName = ?, lastName = ?, profileCompleted = 1 WHERE id = ?')
    .run(firstName, lastName, req.user.id);
  const user = formatUserRow(db.prepare('SELECT * FROM users WHERE id = ?').get(req.user.id));
  res.json({ message: 'Profile completed', user });
});

app.post('/api/auth/login', (req, res) => {
  const { login, password, portal, savedProfile } = req.body;
  let row = db.prepare('SELECT * FROM users WHERE (username = ? OR email = ?) AND password = ?').get(login, login, password);

  if (!row && savedProfile && (savedProfile.username === login || savedProfile.email === login) && savedProfile.password === password) {
    db.prepare(`
      INSERT OR IGNORE INTO users (id, username, email, password, role, firstName, lastName, country, mobile, currency, status, balance, creditScore, createdAt)
      VALUES (?, ?, ?, ?, 'TRADER', ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?)
    `).run(
      savedProfile.id || ('usr-' + Date.now()),
      savedProfile.username,
      savedProfile.email,
      savedProfile.password,
      savedProfile.firstName || '',
      savedProfile.lastName || '',
      savedProfile.country || 'Pakistan',
      savedProfile.mobile || '',
      savedProfile.currency || 'PKR',
      savedProfile.balance ?? 0,
      savedProfile.creditScore ?? 100,
      savedProfile.createdAt || new Date().toISOString()
    );
    row = db.prepare('SELECT * FROM users WHERE username = ?').get(savedProfile.username);
  }

  if (!row) return res.status(401).json({ error: 'Invalid credentials.' });

  const user = formatUserRow(row);
  if (user.status !== 'ACTIVE') return res.status(403).json({ error: `Account is ${user.status}.` });
  if (portal === 'ADMIN' && user.role === 'TRADER') return res.status(403).json({ error: 'Traders cannot access the Admin Portal.' });
  if (portal === 'TRADER' && user.role !== 'TRADER') return res.status(403).json({ error: 'Staff must log in via the Admin Portal.' });

  res.json({ user, token: signSession(user) });
});

app.post('/api/auth/change-password', requireAuth(['TRADER', 'SUB_ADMIN', 'MAIN_ADMIN']), (req, res) => {
  const { oldPassword, newPassword } = req.body;
  if (req.user.password !== oldPassword) return res.status(400).json({ error: 'Current password is incorrect.' });
  db.prepare('UPDATE users SET password = ? WHERE id = ?').run(newPassword, req.user.id);
  res.json({ message: 'Password updated successfully.' });
});

app.get('/api/trader/dashboard', requireAuth(['TRADER']), (req, res) => {
  settleExpiredTrades();
  const settings = getSettings();
  const trades = db.prepare('SELECT * FROM trades WHERE userId = ? ORDER BY createdAt DESC LIMIT 30').all(req.user.id);
  const transactions = db.prepare('SELECT * FROM transactions WHERE userId = ? ORDER BY timestamp DESC LIMIT 25').all(req.user.id);
  const tickets = db.prepare('SELECT * FROM tickets WHERE userId = ? ORDER BY createdAt DESC').all(req.user.id);
  const customErrors = db.prepare("SELECT * FROM custom_errors WHERE active = 1 AND (targetUserId = 'ALL' OR targetUserId = ?) ORDER BY createdAt DESC").all(req.user.id);

  res.json({
    user: req.user,
    settings,
    prices: marketPrices,
    trades,
    transactions,
    tickets,
    customErrors
  });
});

app.post('/api/trader/errors/:id/dismiss', requireAuth(['TRADER']), (req, res) => {
  db.prepare('UPDATE custom_errors SET active = 0 WHERE id = ? AND targetUserId = ?').run(req.params.id, req.user.id);
  res.json({ ok: true });
});

app.post('/api/trades/execute', requireAuth(['TRADER']), (req, res) => {
  const settings = getSettings();
  if (settings.globalTradeBan?.enabled) return res.status(403).json({ error: settings.globalTradeBan.message });
  if (req.user.tradeBan?.enabled) return res.status(403).json({ error: req.user.tradeBan.message });

  const { pair, direction, stake, durationSeconds } = req.body;
  const amount = Number(stake);
  const duration = String(durationSeconds || '60');
  const payoutPercent = settings.payoutPercentages[duration];

  if (!marketPrices[pair]) return res.status(400).json({ error: 'Invalid trading pair.' });
  if (!['UP', 'DOWN'].includes(direction)) return res.status(400).json({ error: 'Invalid direction.' });
  if (!payoutPercent) return res.status(400).json({ error: 'Invalid duration tier.' });
  if (isNaN(amount) || amount <= 0 || req.user.balance < amount) return res.status(400).json({ error: 'Insufficient balance.' });

  const newBal = Number((req.user.balance - amount).toFixed(2));
  const newInOrder = Number((req.user.inOrderBalance + amount).toFixed(2));
  const id = 'TRD-' + generateTrx();
  const createdAt = Date.now();
  const expiresAt = createdAt + Number(duration) * 1000;

  db.prepare('UPDATE users SET balance = ?, inOrderBalance = ? WHERE id = ?').run(newBal, newInOrder, req.user.id);
  db.prepare(`
    INSERT INTO trades (id, userId, username, pair, direction, stake, payoutPercent, durationSeconds, entryPrice, status, createdAt, expiresAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?, ?)
  `).run(id, req.user.id, req.user.username, pair, direction, amount, payoutPercent, Number(duration), marketPrices[pair], createdAt, expiresAt);

  broadcastEvent('NEW_TRADE', { id, username: req.user.username, pair });
  res.json({ message: 'Trade started', id });
});

app.post('/api/wallet/deposit', requireAuth(['TRADER']), upload.single('proof'), (req, res) => {
  const { cryptoCurrency, amount, txHash } = req.body;
  const numAmount = Number(amount);
  if (!cryptoCurrency || isNaN(numAmount) || numAmount <= 0 || !req.file) {
    return res.status(400).json({ error: 'Currency, valid amount, and screenshot proof are required.' });
  }

  const settings = getSettings();
  const id = 'DEP-' + generateTrx();
  db.prepare(`
    INSERT INTO deposits (id, userId, username, cryptoCurrency, walletAddress, amount, txHash, proofUrl, status, createdAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
  `).run(id, req.user.id, req.user.username, cryptoCurrency, settings.cryptoWallets[cryptoCurrency] || 'N/A', numAmount, txHash || 'N/A', `/uploads/${req.file.filename}`, new Date().toISOString());

  broadcastEvent('NEW_DEPOSIT', { id, username: req.user.username, amount: numAmount });
  res.json({ message: 'Deposit proof submitted for Admin verification.' });
});

app.post('/api/wallet/withdraw', requireAuth(['TRADER']), (req, res) => {
  const settings = getSettings();
  if (settings.globalWithdrawBan?.enabled) return res.status(403).json({ error: settings.globalWithdrawBan.message });
  if (req.user.withdrawBan?.enabled) return res.status(403).json({ error: req.user.withdrawBan.message });

  if (req.user.creditScore < settings.minCreditScoreForWithdraw) {
    return res.status(403).json({
      error: `Credit score too low (${req.user.creditScore}/${settings.minCreditScoreForWithdraw}). Win more trades to unlock withdrawals.`
    });
  }

  const { amount, method, accountDetails } = req.body;
  const numAmount = Number(amount);
  if (isNaN(numAmount) || numAmount <= 0 || numAmount > req.user.balance) {
    return res.status(400).json({ error: 'Invalid withdrawal amount or insufficient balance.' });
  }

  const newBal = Number((req.user.balance - numAmount).toFixed(2));
  const id = 'WDR-' + generateTrx();

  db.prepare('UPDATE users SET balance = ? WHERE id = ?').run(newBal, req.user.id);
  db.prepare(`
    INSERT INTO withdrawals (id, userId, username, amount, method, accountDetails, status, createdAt)
    VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)
  `).run(id, req.user.id, req.user.username, numAmount, method || 'USDT (TRC20)', accountDetails || '', new Date().toISOString());

  broadcastEvent('NEW_WITHDRAWAL', { id, username: req.user.username, amount: numAmount });
  res.json({ message: 'Withdrawal request submitted.' });
});

app.post('/api/help-center/send', requireAuth(['TRADER']), (req, res) => {
  const { subject, message } = req.body;
  if (!message) return res.status(400).json({ error: 'Message cannot be empty.' });

  const id = 'TKT-' + generateTrx();
  db.prepare(`
    INSERT INTO tickets (id, userId, username, subject, message, status, createdAt)
    VALUES (?, ?, ?, ?, ?, 'OPEN', ?)
  `).run(id, req.user.id, req.user.username, subject || 'General Inquiry', message, new Date().toISOString());

  broadcastEvent('NEW_TICKET', { id, username: req.user.username });
  res.json({ message: 'Message sent to Help Center.' });
});

// ==========================================================================
// ADMIN & SUB-ADMIN GUARD ROUTES
// ==========================================================================
app.get('/api/admin/overview', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), (req, res) => {
  settleExpiredTrades();
  const traders = db.prepare("SELECT * FROM users WHERE role = 'TRADER' ORDER BY createdAt DESC").all().map(formatUserRow);
  const staff = req.user.role === 'MAIN_ADMIN'
    ? db.prepare("SELECT * FROM users WHERE role != 'TRADER'").all().map(formatUserRow)
    : [];

  res.json({
    actor: req.user,
    stats: {
      totalUsers: traders.length,
      activeUsers: traders.filter(u => u.status === 'ACTIVE').length,
      totalTrades: db.prepare('SELECT COUNT(*) as c FROM trades').get().c,
      runningTrades: db.prepare("SELECT COUNT(*) as c FROM trades WHERE status = 'RUNNING'").get().c,
      pendingDeposits: db.prepare("SELECT COUNT(*) as c FROM deposits WHERE status = 'PENDING'").get().c,
      pendingWithdrawals: db.prepare("SELECT COUNT(*) as c FROM withdrawals WHERE status = 'PENDING'").get().c,
      openTickets: db.prepare("SELECT COUNT(*) as c FROM tickets WHERE status = 'OPEN'").get().c
    },
    traders,
    staff,
    deposits: db.prepare('SELECT * FROM deposits ORDER BY createdAt DESC LIMIT 60').all(),
    withdrawals: db.prepare('SELECT * FROM withdrawals ORDER BY createdAt DESC LIMIT 60').all(),
    trades: db.prepare('SELECT * FROM trades ORDER BY createdAt DESC LIMIT 60').all(),
    tickets: db.prepare('SELECT * FROM tickets ORDER BY createdAt DESC LIMIT 50').all(),
    auditLogs: db.prepare('SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 60').all(),
    settings: getSettings()
  });
});

app.post('/api/admin/deposits/:id/review', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), (req, res) => {
  const { action } = req.body;
  const deposit = db.prepare('SELECT * FROM deposits WHERE id = ?').get(req.params.id);
  if (!deposit || deposit.status !== 'PENDING') return res.status(400).json({ error: 'Invalid deposit.' });

  const status = action === 'APPROVE' ? 'APPROVED' : 'REJECTED';
  db.prepare('UPDATE deposits SET status = ?, reviewedBy = ? WHERE id = ?').run(status, req.user.username, deposit.id);

  if (status === 'APPROVED') {
    const trader = db.prepare('SELECT * FROM users WHERE id = ?').get(deposit.userId);
    if (trader) {
      const newBal = Number((trader.balance + deposit.amount).toFixed(2));
      db.prepare('UPDATE users SET balance = ? WHERE id = ?').run(newBal, trader.id);
      db.prepare('INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
        .run(generateTrx(), trader.id, trader.username, 'DEPOSIT', deposit.amount, newBal, `Deposit Approved (${deposit.cryptoCurrency})`, new Date().toISOString());
    }
  }

  logAudit(req.user, `DEPOSIT_${status}`, deposit.username, `${deposit.id} (${deposit.amount})`);
  broadcastEvent('DEPOSIT_UPDATED');
  res.json({ message: `Deposit ${status}.` });
});

app.post('/api/admin/withdrawals/:id/edit-account', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), (req, res) => {
  const { accountDetails } = req.body;
  const withdrawal = db.prepare('SELECT * FROM withdrawals WHERE id = ?').get(req.params.id);
  if (!withdrawal) return res.status(404).json({ error: 'Withdrawal not found.' });

  db.prepare('UPDATE withdrawals SET accountDetails = ? WHERE id = ?').run(accountDetails, withdrawal.id);
  logAudit(req.user, 'EDIT_WITHDRAW_ACCOUNT', withdrawal.username, `Updated ${withdrawal.id} to ${accountDetails}`);
  broadcastEvent('WITHDRAWAL_UPDATED');
  res.json({ message: 'Withdrawal account updated.' });
});

app.post('/api/admin/withdrawals/:id/review', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), (req, res) => {
  if (req.user.role === 'SUB_ADMIN' && !req.user.permissions?.canApproveWithdrawals) {
    return res.status(403).json({ error: 'Sub-Admin lacks withdrawal approval permission.' });
  }
  const { action } = req.body;
  const withdrawal = db.prepare('SELECT * FROM withdrawals WHERE id = ?').get(req.params.id);
  if (!withdrawal || withdrawal.status !== 'PENDING') return res.status(400).json({ error: 'Invalid withdrawal.' });

  const status = action === 'APPROVE' ? 'APPROVED' : 'REJECTED';
  db.prepare('UPDATE withdrawals SET status = ? WHERE id = ?').run(status, withdrawal.id);

  const trader = db.prepare('SELECT * FROM users WHERE id = ?').get(withdrawal.userId);
  if (trader) {
    if (status === 'APPROVED') {
      const newWithdrawn = Number((trader.totalWithdrawn + withdrawal.amount).toFixed(2));
      db.prepare('UPDATE users SET totalWithdrawn = ? WHERE id = ?').run(newWithdrawn, trader.id);
      db.prepare('INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
        .run(generateTrx(), trader.id, trader.username, 'WITHDRAW', -withdrawal.amount, trader.balance, `Withdrawal Approved (${withdrawal.method}: ${withdrawal.accountDetails})`, new Date().toISOString());
    } else {
      const refundedBal = Number((trader.balance + withdrawal.amount).toFixed(2));
      db.prepare('UPDATE users SET balance = ? WHERE id = ?').run(refundedBal, trader.id);
    }
  }

  logAudit(req.user, `WITHDRAWAL_${status}`, withdrawal.username, withdrawal.id);
  broadcastEvent('WITHDRAWAL_UPDATED');
  res.json({ message: `Withdrawal ${status}.` });
});

app.post('/api/admin/users/:id/guard', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), (req, res) => {
  const { status, tradeBan, withdrawBan } = req.body;
  const trader = db.prepare("SELECT * FROM users WHERE id = ? AND role = 'TRADER'").get(req.params.id);
  if (!trader) return res.status(404).json({ error: 'Trader not found.' });

  if (status) db.prepare('UPDATE users SET status = ? WHERE id = ?').run(status, trader.id);
  if (tradeBan) {
    db.prepare('UPDATE users SET tradeBanEnabled = ?, tradeBanMessage = ? WHERE id = ?')
      .run(tradeBan.enabled ? 1 : 0, tradeBan.message || '', trader.id);
  }
  if (withdrawBan) {
    db.prepare('UPDATE users SET withdrawBanEnabled = ?, withdrawBanMessage = ? WHERE id = ?')
      .run(withdrawBan.enabled ? 1 : 0, withdrawBan.message || '', trader.id);
  }

  logAudit(req.user, 'GUARD_UPDATE_USER', trader.username, `Updated restrictions`);
  broadcastEvent('USER_UPDATED');
  res.json({ message: 'Trader updated.' });
});

app.post('/api/admin/trades/:id/void', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), (req, res) => {
  const trade = db.prepare('SELECT * FROM trades WHERE id = ?').get(req.params.id);
  if (!trade || trade.status !== 'RUNNING') return res.status(400).json({ error: 'Trade not running.' });

  db.prepare("UPDATE trades SET status = 'VOIDED' WHERE id = ?").run(trade.id);
  const trader = db.prepare('SELECT * FROM users WHERE id = ?').get(trade.userId);
  if (trader) {
    const newInOrder = Math.max(0, Number((trader.inOrderBalance - trade.stake).toFixed(2)));
    const newBal = Number((trader.balance + trade.stake).toFixed(2));
    db.prepare('UPDATE users SET inOrderBalance = ?, balance = ? WHERE id = ?').run(newInOrder, newBal, trader.id);
  }

  logAudit(req.user, 'VOID_TRADE', trade.username, `Voided ${trade.id}`);
  broadcastEvent('TRADE_SETTLED');
  res.json({ message: 'Trade voided.' });
});

app.post('/api/admin/tickets/:id/reply', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), (req, res) => {
  const ticket = db.prepare('SELECT * FROM tickets WHERE id = ?').get(req.params.id);
  if (!ticket) return res.status(404).json({ error: 'Ticket not found.' });

  db.prepare("UPDATE tickets SET reply = ?, repliedBy = ?, status = 'ANSWERED' WHERE id = ?")
    .run(req.body.reply, req.user.username, ticket.id);
  logAudit(req.user, 'REPLY_TICKET', ticket.username, `Replied to ${ticket.id}`);
  broadcastEvent('TICKET_UPDATED');
  res.json({ message: 'Reply sent.' });
});

app.post('/api/admin/error-sender', requireAuth(['MAIN_ADMIN', 'SUB_ADMIN']), (req, res) => {
  const { targetUserId, title, message } = req.body;
  const id = 'ERR-' + generateTrx();
  db.prepare(`
    INSERT INTO custom_errors (id, targetUserId, title, message, active, sentBy, createdAt)
    VALUES (?, ?, ?, ?, 1, ?, ?)
  `).run(id, targetUserId || 'ALL', title || 'System Notice', message, req.user.username, new Date().toISOString());

  logAudit(req.user, 'ERROR_SENDER', targetUserId || 'ALL', title);
  broadcastEvent('ERROR_ALERT');
  res.json({ message: 'Alert sent.' });
});

app.post('/api/admin/users/:id/impersonate', requireAuth(['MAIN_ADMIN']), (req, res) => {
  const row = db.prepare("SELECT * FROM users WHERE id = ? AND role = 'TRADER'").get(req.params.id);
  if (!row) return res.status(404).json({ error: 'Trader not found.' });
  const trader = formatUserRow(row);
  logAudit(req.user, 'LOGIN_AS_USER', trader.username, 'Impersonated trader');
  res.json({ user: trader });
});

app.post('/api/admin/users/:id/reset-password', requireAuth(['MAIN_ADMIN']), (req, res) => {
  const { newPassword } = req.body;
  const trader = db.prepare('SELECT * FROM users WHERE id = ?').get(req.params.id);
  if (!trader || !newPassword) return res.status(400).json({ error: 'Valid user and password required.' });

  db.prepare('UPDATE users SET password = ? WHERE id = ?').run(newPassword, trader.id);
  logAudit(req.user, 'RESET_PASSWORD', trader.username, 'Password changed by Main Admin');
  res.json({ message: `Password updated for ${trader.username}.` });
});

app.post('/api/admin/users/:id/adjust', requireAuth(['MAIN_ADMIN']), (req, res) => {
  const { balanceDelta, creditScore } = req.body;
  const trader = db.prepare("SELECT * FROM users WHERE id = ? AND role = 'TRADER'").get(req.params.id);
  if (!trader) return res.status(404).json({ error: 'Trader not found.' });

  let newBal = trader.balance;
  if (balanceDelta !== undefined && Number(balanceDelta) !== 0) {
    const delta = Number(balanceDelta);
    newBal = Math.max(0, Number((trader.balance + delta).toFixed(2)));
    db.prepare('INSERT INTO transactions (trx, userId, username, type, amount, postBalance, note, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
      .run(generateTrx(), trader.id, trader.username, delta > 0 ? 'ADMIN_CREDIT' : 'ADMIN_DEBIT', delta, newBal, 'Admin Balance Adjustment', new Date().toISOString());
  }
  const newScore = creditScore !== undefined ? Math.max(0, Math.min(100, Number(creditScore))) : trader.creditScore;

  db.prepare('UPDATE users SET balance = ?, creditScore = ? WHERE id = ?').run(newBal, newScore, trader.id);
  logAudit(req.user, 'ADJUST_BALANCE_SCORE', trader.username, `Delta: ${balanceDelta || 0}, Score: ${newScore}`);
  broadcastEvent('USER_UPDATED');
  res.json({ message: 'Trader updated.' });
});

app.post('/api/admin/subadmins', requireAuth(['MAIN_ADMIN']), (req, res) => {
  const { username, email, password, canApproveWithdrawals } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username & password required.' });
  if (db.prepare('SELECT id FROM users WHERE username = ?').get(username)) return res.status(400).json({ error: 'Username exists.' });

  const id = 'sub-' + Date.now();
  db.prepare(`
    INSERT INTO users (id, username, email, password, role, firstName, lastName, status, canApproveWithdrawals, createdAt)
    VALUES (?, ?, ?, ?, 'SUB_ADMIN', ?, 'Guard', 'ACTIVE', ?, ?)
  `).run(id, username, email || `${username}@daraztrade.online`, password, username, canApproveWithdrawals ? 1 : 0, new Date().toISOString());

  logAudit(req.user, 'CREATE_SUB_ADMIN', username, `Withdrawals: ${Boolean(canApproveWithdrawals)}`);
  res.json({ message: 'Sub-Admin Guard created.' });
});

app.post('/api/admin/settings', requireAuth(['MAIN_ADMIN']), (req, res) => {
  saveSettings(req.body);
  logAudit(req.user, 'UPDATE_SETTINGS', 'SYSTEM', 'Updated platform settings');
  broadcastEvent('SETTINGS_UPDATED');
  res.json({ message: 'Settings saved.', settings: getSettings() });
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`\n==================================================`);
    console.log(`SQLite DB Active: backend/daraz_trade.db`);
    console.log(`Trader Portal:    http://localhost:${PORT}/`);
    console.log(`Admin Portal:     http://localhost:${PORT}/admin/login.html`);
    console.log(`Main Admin ->     mainadmin / admin123`);
    console.log(`==================================================\n`);
  });
}

module.exports = app;