/**
 * GoalPulse — Günlük H2H Prefetch
 * ─────────────────────────────────────────────────────────────────
 * Sabah çalışır, bugünün NS (başlamamış) maçları için Mackolik'ten
 * H2H verilerini çekip Supabase'e kaydeder.
 * Worker maç başladığında veriyi hazır bulur, gereksiz istek atmaz.
 *
 * Kullanım:
 *   node prefetch_h2h.js
 *   node prefetch_h2h.js --dry-run        (Supabase'e yazmaz, sadece loglar)
 *   node prefetch_h2h.js --concurrency=3
 *   node prefetch_h2h.js --delay=1200
 *
 * Gerekli env değişkenleri:
 *   SUPABASE_URL
 *   SUPABASE_KEY
 *
 * Cron örneği (her gün 05:00 TR = 02:00 UTC):
 *   0 2 * * * node /app/prefetch_h2h.js >> /var/log/prefetch_h2h.log 2>&1
 */

'use strict';

const https  = require('https');
const zlib   = require('zlib');
const fs     = require('fs');
const path   = require('path');

// ─── LOG ──────────────────────────────────────────────────────────────────────
const logFile = fs.createWriteStream(
  path.join(__dirname, 'prefetch_h2h.log'),
  { flags: 'a' }   // append — her çalışma logları birikir
);

function log(...a) {
  const ts = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
  const m  = `[${ts}] ${a.join(' ')}`;
  console.log(m);
  logFile.write(m + '\n');
}
function logErr(...a) {
  const ts = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
  const m  = `[${ts}] ❌ ${a.join(' ')}`;
  console.error(m);
  logFile.write(m + '\n');
}

process.on('uncaughtException',  e => { logErr('UNCAUGHT:', e.stack || e.message);   logFile.end(() => process.exit(1)); });
process.on('unhandledRejection', e => { logErr('REJECTION:', e?.stack || String(e)); logFile.end(() => process.exit(1)); });

// ─── ARGS ─────────────────────────────────────────────────────────────────────
const args = Object.fromEntries(
  process.argv.slice(2)
    .filter(a => a.startsWith('--'))
    .map(a => {
      const [k, ...v] = a.slice(2).split('=');
      return [k, v.length ? v.join('=') : 'true'];
    })
);
const DRY_RUN     = args['dry-run']    === 'true';
const CONCURRENCY = parseInt(args.concurrency || '2', 10);
const DELAY_MS    = parseInt(args.delay        || '1000', 10);
const EXTRA_DELAY = parseInt(args.extraDelay   || '600',  10);

// ─── ENV ──────────────────────────────────────────────────────────────────────
// .env dosyası varsa yükle (opsiyonel)
try {
  const envPath = path.join(__dirname, '.env');
  if (fs.existsSync(envPath)) {
    fs.readFileSync(envPath, 'utf8')
      .split('\n')
      .forEach(line => {
        const clean = line.trim();
        if (!clean || clean.startsWith('#')) return;
        const eqIdx = clean.indexOf('=');
        if (eqIdx < 1) return;
        const key = clean.slice(0, eqIdx).trim();
        const val = clean.slice(eqIdx + 1).trim().replace(/^["']|["']$/g, '');
        if (!(key in process.env)) process.env[key] = val;
      });
    log('ℹ️  .env yüklendi');
  }
} catch (_) {}

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_KEY || '';

if (!SUPABASE_URL || !SUPABASE_KEY) {
  logErr('SUPABASE_URL ve SUPABASE_KEY env değişkenleri gerekli!');
  process.exit(1);
}

// ─── YARDIMCILAR ──────────────────────────────────────────────────────────────
const sleep    = ms => new Promise(r => setTimeout(r, ms));
const randWait = () => sleep(DELAY_MS + Math.floor(Math.random() * 400));

function getTRToday() {
  return new Date().toLocaleString('en-CA', { timeZone: 'Europe/Istanbul' }).split(',')[0].trim();
}

// ─── UA POOL ──────────────────────────────────────────────────────────────────
const UA_POOL = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
];
const randUA = () => UA_POOL[Math.floor(Math.random() * UA_POOL.length)];

// ─── HTTP ─────────────────────────────────────────────────────────────────────
function httpGet(url, extraHeaders = {}, maxRetry = 3) {
  return new Promise((resolve, reject) => {
    const RETRY_DELAYS = [2000, 5000, 10000];

    const attempt = (tryNum) => {
      const options = {
        headers: {
          'User-Agent':      randUA(),
          'Accept':          'text/html,application/json,*/*',
          'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8',
          'Accept-Encoding': 'gzip, deflate',
          'Connection':      'keep-alive',
          ...extraHeaders,
        }
      };

      https.get(url, options, res => {
        // 429 → retry-after kadar bekle
        if (res.statusCode === 429) {
          const retryAfter = parseInt(res.headers['retry-after'] || '15', 10);
          const delay = Math.max(retryAfter * 1000, RETRY_DELAYS[tryNum - 1] || 15000);
          log(`  ⏳ 429 rate-limit, ${delay}ms bekleniyor...`);
          if (tryNum < maxRetry) { setTimeout(() => attempt(tryNum + 1), delay); return; }
          reject(new Error(`429 rate-limit: ${url}`)); return;
        }

        // 5xx → retry
        if (res.statusCode >= 500) {
          const delay = RETRY_DELAYS[tryNum - 1] || 5000;
          if (tryNum < maxRetry) {
            log(`  🔁 HTTP ${res.statusCode}, ${delay}ms retry ${tryNum}/${maxRetry}...`);
            setTimeout(() => attempt(tryNum + 1), delay);
            return;
          }
          reject(new Error(`HTTP ${res.statusCode}: ${url}`)); return;
        }

        // gzip / deflate decode
        const enc    = res.headers['content-encoding'] || '';
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => {
          const buf    = Buffer.concat(chunks);
          const decode = (err, decoded) => resolve(err ? buf.toString('utf8') : decoded.toString('utf8'));
          if      (enc === 'gzip')    zlib.gunzip(buf, decode);
          else if (enc === 'deflate') zlib.inflate(buf, (e, r) => e ? zlib.inflateRaw(buf, decode) : decode(null, r));
          else if (enc === 'br')      zlib.brotliDecompress(buf, decode);
          else    resolve(buf.toString('utf8'));
        });
        res.on('error', err => reject(err));
      }).on('error', err => {
        const delay = RETRY_DELAYS[tryNum - 1] || 5000;
        if (tryNum < maxRetry) {
          log(`  🔁 Bağlantı hatası (${err.message}), ${delay}ms retry ${tryNum}/${maxRetry}...`);
          setTimeout(() => attempt(tryNum + 1), delay);
        } else {
          reject(err);
        }
      });
    };

    attempt(1);
  });
}

async function httpGetJSON(url, extraHeaders = {}) {
  const raw = await httpGet(url, extraHeaders);
  if (raw.trimStart().startsWith('<')) throw new Error(`HTML döndü: ${raw.slice(0, 80)}`);
  try { return JSON.parse(raw); }
  catch (e) {
    const cleaned = raw.replace(/\\(?!["\\/bfnrtu])/g, '\\\\').replace(/[\x00-\x1F\x7F]/g, ' ');
    return JSON.parse(cleaned);
  }
}

// ─── SUPABASE ─────────────────────────────────────────────────────────────────
async function sbFetch(method, path_, body = null) {
  return new Promise((resolve, reject) => {
    const url     = new URL(SUPABASE_URL);
    const options = {
      hostname: url.hostname,
      port:     443,
      path:     path_,
      method,
      headers: {
        'apikey':        SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type':  'application/json',
        'Prefer':        method === 'POST' ? 'return=minimal' : '',
      },
    };

    const req = require('https').request(options, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode >= 400) {
          reject(new Error(`Supabase ${res.statusCode}: ${text.slice(0, 200)}`));
          return;
        }
        resolve(text ? JSON.parse(text) : null);
      });
    });

    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

/** Bugün başlamamış (NS) maçları Supabase'den çek */
async function fetchTodayMatches() {
  const today = getTRToday();
  log(`📅 Bugün: ${today} | NS maçlar çekiliyor...`);

  // updated_at bugün olan NS maçlar
  const q = `/rest/v1/live_matches?status_short=eq.NS&updated_at=gte.${today}T00:00:00&select=fixture_id,home_team,home_team_id,away_team,away_team_id`;
  const rows = await sbFetch('GET', q);
  log(`   ✅ ${rows.length} NS maç bulundu`);
  return rows || [];
}

/** H2H verisi zaten var mı? */
async function h2hExists(h2hKey) {
  const q = `/rest/v1/match_h2h?h2h_key=eq.${encodeURIComponent(h2hKey)}&select=h2h_key`;
  const rows = await sbFetch('GET', q);
  return Array.isArray(rows) && rows.length > 0;
}

/** H2H'ı Supabase'e kaydet (upsert) */
async function saveH2H(h2hKey, data) {
  if (DRY_RUN) {
    log(`   [DRY-RUN] H2H kaydedilecekti: ${h2hKey}`);
    return;
  }
  const body = {
    h2h_key:    h2hKey,
    data,
    updated_at: new Date().toISOString(),
  };
  // PATCH yerine POST + onConflict=h2h_key (upsert)
  await sbFetch('POST', '/rest/v1/match_h2h?on_conflict=h2h_key', body);
}

// ─── MACKOLİK CACHE ──────────────────────────────────────────────────────────
let _macCache   = [];
let _macCacheTs = null;

async function getMackolikCache() {
  if (_macCache.length > 0 && _macCacheTs && (Date.now() - _macCacheTs) < 60 * 60 * 1000) {
    return _macCache;
  }

  const now   = new Date();
  const tr    = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Istanbul' }));
  const dd    = String(tr.getDate()).padStart(2, '0');
  const mm    = String(tr.getMonth() + 1).padStart(2, '0');
  const yyyy  = tr.getFullYear();
  const macDate = `${dd}/${mm}/${yyyy}`;

  log(`   📡 Mackolik cache yükleniyor: ${macDate}`);
  const url  = `https://vd.mackolik.com/livedata?date=${encodeURIComponent(macDate)}`;
  const data = await httpGetJSON(url, { 'Referer': 'https://arsiv.mackolik.com/' });
  const raw  = (data.m || []);

  _macCache = raw
    .filter(m => Array.isArray(m) && m.length >= 37)
    .filter(m => {
      const li = Array.isArray(m[36]) ? m[36] : [];
      return (parseInt(li[11], 10) || 1) === 1; // sadece futbol
    })
    .map(m => ({
      mackolikId: parseInt(m[0], 10),
      homeTeam:   String(m[2] || '').trim(),
      awayTeam:   String(m[4] || '').trim(),
    }))
    .filter(m => m.mackolikId > 0);

  _macCacheTs = Date.now();
  log(`   ✅ Mackolik cache: ${_macCache.length} maç`);
  return _macCache;
}

// ─── TAKIM İSMİ EŞLEŞTİRME ───────────────────────────────────────────────────
function normalize(name) {
  return (name || '')
    .toLowerCase()
    .replace(/ı/g, 'i').replace(/ğ/g, 'g').replace(/ü/g, 'u')
    .replace(/ş/g, 's').replace(/ö/g, 'o').replace(/ç/g, 'c')
    .replace(/é/g, 'e').replace(/á/g, 'a').replace(/ñ/g, 'n')
    .replace(/[^\w\s]/g, '').replace(/\s+/g, ' ').trim();
}

function teamSimilarity(a, b) {
  const n1 = normalize(a), n2 = normalize(b);
  if (n1 === n2) return 1.0;
  if (n1.includes(n2) || n2.includes(n1)) return 0.9;
  const w1 = new Set(n1.split(' '));
  const w2 = new Set(n2.split(' '));
  const inter = [...w1].filter(w => w2.has(w)).length;
  const union = new Set([...w1, ...w2]).size;
  const jaccard = union === 0 ? 0 : inter / union;
  if (jaccard >= 0.5) return 0.7 + jaccard * 0.2;
  if (n1.length >= 3 && n2.length >= 3 && n1.slice(0, 3) === n2.slice(0, 3)) return 0.6;
  return jaccard * 0.5;
}

async function findMackolikId(homeTeam, awayTeam) {
  const cache = await getMackolikCache();
  let best = null, bestScore = 0;

  for (const mac of cache) {
    const hSim = teamSimilarity(homeTeam, mac.homeTeam);
    const aSim = teamSimilarity(awayTeam, mac.awayTeam);
    const score = (hSim + aSim) / 2;
    if (score > bestScore && hSim >= 0.5 && aSim >= 0.5) {
      bestScore = score;
      best = mac;
    }
  }

  if (best && bestScore >= 0.55) {
    log(`   🔗 Eşleşme: ${homeTeam} vs ${awayTeam} → mackolikId=${best.mackolikId} (%${Math.round(bestScore * 100)})`);
    return best.mackolikId;
  }

  log(`   ⚠️  Eşleşme bulunamadı: ${homeTeam} vs ${awayTeam}`);
  return null;
}

// ─── H2H FETCH + PARSE ────────────────────────────────────────────────────────
async function fetchH2H(mackolikId) {
  const url = `https://arsiv.mackolik.com/Match/Head2Head.aspx?id=${mackolikId}&s=1`;
  try {
    const raw = await httpGet(url, { 'Referer': `https://arsiv.mackolik.com/Mac/${mackolikId}/` });

    if (raw.includes('Object moved') || raw.includes('PageError.htm')) {
      log(`   ⚠️  H2H sayfası yönlendirme hatası: mackolikId=${mackolikId}`);
      return null;
    }

    return parseH2H(raw);
  } catch (e) {
    logErr(`H2H fetch hatası mackolikId=${mackolikId}: ${e.message}`);
    return null;
  }
}

function parseH2H(html) {
  const result = { h2h: [], homeForm: [], awayForm: [], homeScorers: [], awayScorers: [] };
  if (!html || typeof html !== 'string') return result;

  const clean = s => s.replace(/<[^>]+>/g, '').replace(/&nbsp;/g, '').replace(/\s+/g, ' ').trim();

  // ── H2H SON 5 MAÇ ──
  const h2hTableM = html.match(/Aralarındaki Maçlar\s*<\/div>[\s\S]*?<table[^>]*class="md-table3"[^>]*>([\s\S]*?)<\/table>/);
  if (h2hTableM) {
    const rowRe = /<tr class="row alt[12]">([\s\S]*?)<\/tr>/g;
    let row;
    while ((row = rowRe.exec(h2hTableM[1])) !== null) {
      const tds = [...row[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g)].map(t => t[1]);
      if (tds.length < 8) continue;
      const scoreM = tds[6].match(/\/Mac\/(\d+)\/[^>]+><b>\s*(\d+)\s*-\s*(\d+)/);
      if (!scoreM) continue;
      const hg  = parseInt(scoreM[2], 10);
      const ag  = parseInt(scoreM[3], 10);
      const htM = tds[8] ? tds[8].match(/(\d+)\s*-\s*(\d+)/) : null;
      result.h2h.push({
        matchId:    parseInt(scoreM[1], 10),
        date:       clean(tds[2]),
        homeTeam:   clean(tds[5]),
        awayTeam:   clean(tds[7]),
        homeGoals:  hg,
        awayGoals:  ag,
        htHome:     htM ? parseInt(htM[1], 10) : null,
        htAway:     htM ? parseInt(htM[2], 10) : null,
        homeWinner: hg > ag ? true : hg < ag ? false : null,
      });
      if (result.h2h.length >= 5) break;
    }
  }

  // ── FORM TABLOLARI ──
  // Kolon yapısı: [0]=lig [1]=tarih [2]=ev sahibi [3]=skor [4]=deplasman [5]=sonuç
  const parseForm = tableHtml => {
    const rows = [];
    const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    let m;
    while ((m = rowRe.exec(tableHtml)) !== null) {
      const tds = [...m[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map(t => t[1]);
      if (tds.length < 6) continue; // başlık / boş satırlar

      const league   = tds[0].replace(/<[^>]+>/g, '').trim();
      const rawDate  = tds[1].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').trim();
      const dateM    = rawDate.match(/(\d{2}\.\d{2})/);
      const dateStr  = dateM ? dateM[1] : '';
      const homeTeam = tds[2].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();

      const scoreM = tds[3].match(/<b>\s*(\d+)\s*-\s*(\d+)/i);
      if (!scoreM) continue; // oynanmamış (v) satırlar

      const awayTeam = tds[4].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
      const imgM     = tds[5].match(/img5\/(G|B|M)\.png/i);
      const resultV  = imgM ? (imgM[1].toUpperCase() === 'G' ? 'W' : imgM[1].toUpperCase() === 'B' ? 'D' : 'L') : '';

      rows.push({
        league,
        date:      dateStr,
        homeTeam,
        awayTeam,
        homeGoals: parseInt(scoreM[1], 10),
        awayGoals: parseInt(scoreM[2], 10),
        result:    resultV,
      });
      if (rows.length >= 10) break;
    }
    return rows;
  };

  const formTables = [...html.matchAll(/Form Durumu\s*<\/div>\s*<table[^>]*>([\s\S]*?)<\/table>/g)]
    .map(m => m[1]);
  if (formTables[0]) result.homeForm = parseForm(formTables[0]);
  if (formTables[1]) result.awayForm = parseForm(formTables[1]);

  // ── EN GOLCÜLER ──
  const parseScorers = tableHtml => {
    const scorers = [];
    for (const m of tableHtml.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)) {
      const nameM  = m[1].match(/href="[^"]*\/Futbolcu\/\d+\/[^"]*">\s*([^<]+?)\s*<\/a>/);
      const goalsM = m[1].match(/<b>(\d+)<\/b>/);
      if (nameM && goalsM) {
        scorers.push({ name: clean(nameM[1]), goals: parseInt(goalsM[1], 10) });
        if (scorers.length >= 3) break;
      }
    }
    return scorers;
  };

  const scorerTables = [...html.matchAll(/En Golc[üu]ler\s*<\/div>\s*<table[^>]*>([\s\S]*?)<\/table>/g)]
    .map(m => m[1]);
  if (scorerTables[0]) result.homeScorers = parseScorers(scorerTables[0]);
  if (scorerTables[1]) result.awayScorers = parseScorers(scorerTables[1]);

  return result;
}

// ─── TEK MAÇ İŞLE ─────────────────────────────────────────────────────────────
async function processMatch(match) {
  const { fixture_id, home_team, home_team_id, away_team, away_team_id } = match;
  const h2hKey = `${home_team_id}-${away_team_id}`;

  // Zaten var mı?
  const exists = await h2hExists(h2hKey);
  if (exists) {
    log(`   ⏭  ${home_team} vs ${away_team} → H2H zaten mevcut (${h2hKey}), atlandı`);
    return { status: 'skipped' };
  }

  // Mackolik ID bul
  const mackolikId = await findMackolikId(home_team, away_team);
  if (!mackolikId) {
    return { status: 'no_match' };
  }

  await sleep(EXTRA_DELAY + Math.floor(Math.random() * 400));

  // H2H çek
  const h2hData = await fetchH2H(mackolikId);
  if (!h2hData) {
    return { status: 'fetch_failed' };
  }

  const hasData = h2hData.h2h.length > 0 || h2hData.homeForm.length > 0;
  if (!hasData) {
    log(`   ⚠️  H2H boş döndü: ${home_team} vs ${away_team}`);
    return { status: 'empty' };
  }

  // Kaydet
  await saveH2H(h2hKey, h2hData);
  log(`   ✅ H2H kaydedildi: ${home_team} vs ${away_team} | h2h=${h2hData.h2h.length} form=${h2hData.homeForm.length}/${h2hData.awayForm.length}`);
  return { status: 'saved' };
}

// ─── ANA AKIŞ ─────────────────────────────────────────────────────────────────
(async () => {
  const startTime = Date.now();
  log('');
  log('══════════════════════════════════════════════');
  log('  🚀 GoalPulse H2H Prefetch Başlatılıyor');
  log(`  📅 Tarih: ${getTRToday()}`);
  log(`  ⚡ Concurrency: ${CONCURRENCY} | Delay: ${DELAY_MS}ms`);
  log(`  ${DRY_RUN ? '⚠️  DRY-RUN modu — Supabase\'e yazılmayacak' : '💾 Supabase\'e yazılacak'}`);
  log('══════════════════════════════════════════════');

  // 1. Bugünün NS maçlarını çek
  let matches;
  try {
    matches = await fetchTodayMatches();
  } catch (e) {
    logErr('Supabase maç listesi alınamadı:', e.message);
    logFile.end(() => process.exit(1));
    return;
  }

  if (matches.length === 0) {
    log('ℹ️  Bugün NS maç yok, çıkılıyor.');
    logFile.end(() => process.exit(0));
    return;
  }

  log(`\n🔄 ${matches.length} maç işlenecek (concurrency=${CONCURRENCY})\n`);

  // 2. Concurrency ile işle
  const stats = { saved: 0, skipped: 0, no_match: 0, fetch_failed: 0, empty: 0, error: 0 };

  for (let i = 0; i < matches.length; i += CONCURRENCY) {
    const batch = matches.slice(i, i + CONCURRENCY);
    const batchNum = Math.floor(i / CONCURRENCY) + 1;
    const totalBatches = Math.ceil(matches.length / CONCURRENCY);
    log(`📦 Batch ${batchNum}/${totalBatches}`);

    const results = await Promise.all(
      batch.map(async match => {
        try {
          return await processMatch(match);
        } catch (e) {
          logErr(`  processMatch hatası (${match.home_team} vs ${match.away_team}): ${e.message}`);
          return { status: 'error' };
        }
      })
    );

    for (const r of results) {
      stats[r.status] = (stats[r.status] || 0) + 1;
    }

    if (i + CONCURRENCY < matches.length) await randWait();
  }

  // 3. Özet
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log('\n══════════════════════════════════════════════');
  log(`  ✅ Tamamlandı (${elapsed}s)`);
  log(`  💾 Kaydedildi:        ${stats.saved}`);
  log(`  ⏭  Zaten mevcuttu:   ${stats.skipped}`);
  log(`  ❓ Eşleşme yok:       ${stats.no_match}`);
  log(`  📭 Boş döndü:         ${stats.empty}`);
  log(`  ❌ Fetch hatası:       ${stats.fetch_failed}`);
  log(`  💥 İşlem hatası:       ${stats.error}`);
  log('══════════════════════════════════════════════\n');

  logFile.end(() => process.exit(0));
})();
