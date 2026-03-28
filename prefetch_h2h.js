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
 *   1 0 * * * node /app/prefetch_h2h.js >> /var/log/prefetch_h2h.log 2>&1
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
  log(`📅 Bugün: ${today} | Maçlar çekiliyor...`);

  // live_matches'taki NS maçlar
  const q1 = `/rest/v1/live_matches?status_short=eq.NS&select=fixture_id,home_team,home_team_id,away_team,away_team_id`;
  const liveRows = await sbFetch('GET', q1);
  log(`   live_matches NS: ${liveRows.length} maç`);

  // future_matches'taki bugünün maçları
  const q2 = `/rest/v1/future_matches?date=eq.${today}&select=fixture_id,date`;
  const futureRows = await sbFetch('GET', q2);
  log(`   future_matches bugün: ${futureRows.length} maç`);

  // future_matches'tan detay için live_matches'a bak, yoksa raw data'dan çıkar
  const allRows = [...liveRows];
  
  for (const fr of futureRows) {
    if (!allRows.find(r => r.fixture_id === fr.fixture_id)) {
      // future_matches'tan detaylı veriyi çek
      const q3 = `/rest/v1/future_matches?fixture_id=eq.${fr.fixture_id}&select=fixture_id,data`;
      const detail = await sbFetch('GET', q3);
      if (detail?.[0]?.data) {
        const d = detail[0].data;
        allRows.push({
          fixture_id:   fr.fixture_id,
          home_team:    d.teams?.home?.name || '',
          home_team_id: d.teams?.home?.id   || null,
          away_team:    d.teams?.away?.name || '',
          away_team_id: d.teams?.away?.id   || null,
        });
      }
    }
  }

  log(`   ✅ Toplam: ${allRows.length} maç`);
  return allRows.filter(r => r.home_team_id && r.away_team_id);
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
    const result = {
        h2h:         [],
        homeForm:    [],
        awayForm:    [],
        homeScorers: [],
        awayScorers: [],
    };

    if (!html || typeof html !== 'string') return result;

    // ── 1. H2H SON 5 MAÇ (TD SÜTUN İNDEKSİ İLE KUSURSUZ OKUMA) ───────────────
    // "Aralarındaki Maçlar" başlığından sonraki ilk md-table3'ü alır
    const h2hRe = /Aralarındaki Maçlar\s*<\/div>[\s\S]*?<table[^>]*class="md-table3"[^>]*>([\s\S]*?)<\/table>/;
    const h2hMatch = html.match(h2hRe);
    if (h2hMatch) {
        const rowRe = /<tr class="row alt[12]">([\s\S]*?)<\/tr>/g;
        let row;
        while ((row = rowRe.exec(h2hMatch[1])) !== null) {
            // Her satırdaki hücreleri (td) yakalayıp diziye çeviriyoruz
            const tds = [...row[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g)].map(t => t[1]);
            if (tds.length >= 8) {
                const dateRaw = tds[2].replace(/<[^>]+>/g, '').trim();
                // Mackolik'in bozuk HTML'ine (<b> 0-0 </a></b>) özel skor yakalayıcı
                const scoreM = tds[6].match(/\/Mac\/(\d+)\/[^>]+><b>\s*(\d+)\s*-\s*(\d+)/);
                
                if (!scoreM) continue; // "v" yazan (oynanmamış) maçları atla
                
                const matchId_  = parseInt(scoreM[1], 10);
                const homeGoals = parseInt(scoreM[2], 10);
                const awayGoals = parseInt(scoreM[3], 10);
                
                const homeName = tds[5].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, '').trim();
                const awayName = tds[7].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, '').trim();
                
                const htM = tds[8].match(/(\d+)\s*-\s*(\d+)/);
                const htHome = htM ? parseInt(htM[1], 10) : null;
                const htAway = htM ? parseInt(htM[2], 10) : null;
                
                // Kazananı CSS'ten değil skorlardan biz hesaplıyoruz
                const homeWinner = homeGoals > awayGoals ? true : (homeGoals < awayGoals ? false : null);
                
                result.h2h.push({ matchId: matchId_, date: dateRaw, homeTeam: homeName, awayTeam: awayName, homeGoals, awayGoals, htHome, htAway, homeWinner });
                if (result.h2h.length >= 5) break;
            }
        }
    }

    // ── 2. FORM (KUSURSUZ DİREKT EŞLEŞTİRME VE TAKIM İSİMLERİ) ───────────────
    // Sadece Form Durumu başlığının hemen altındaki tabloları hedefler
    const formTables = [];
    const formRe = /Form Durumu\s*<\/div>\s*<table[^>]*>([\s\S]*?)<\/table>/g;
    let mForm;
    while ((mForm = formRe.exec(html)) !== null) {
        formTables.push(mForm[1]);
    }

    const parseFormTable = (tableHtml) => {
        const rows = [];
        const rowRe = /<tr[^>]*class="row alt[12]"[^>]*>([\s\S]*?)<\/tr>/g;
        let m;
        while ((m = rowRe.exec(tableHtml)) !== null) {
            // Sütunları (td) parçalara ayırıyoruz. Böylece kapanmayan </td> etiketleri sorun olmuyor!
            const parts = m[1].split(/<td[^>]*>/i);
            
            // Eğer sütun sayısı eksikse (bozuk HTML) atla
            if (parts.length < 7) continue;

            // 2. Parça: Tarih (Etiketsiz, sadece rakamları alıyoruz)
            let dateStr = parts[2].replace(/<[^>]+>/g, '').trim();
            const dMatch = dateStr.match(/\d{2}\.\d{2}/);
            dateStr = dMatch ? dMatch[0] : '';

            // 3. Parça: Ev Sahibi Takım
            const homeTeam = parts[3].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, '').trim();

            // 4. Parça: Skor
            const scoreM = parts[4].match(/<b>\s*(\d+)\s*-\s*(\d+)/);
            if (!scoreM) continue; // Oynanmamış (v) maçları atla

            // 5. Parça: Deplasman Takımı
            const awayTeam = parts[5].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, '').trim();

            // 6. Parça: Maç Sonucu (G/B/M)
            const imgM = parts[6].match(/img5\/(G|B|M)\.png/);
            const resultVal = imgM ? (imgM[1] === 'G' ? 'W' : imgM[1] === 'B' ? 'D' : 'L') : '';

            rows.push({
                date:      dateStr,
                homeTeam:  homeTeam, // Eklendi!
                awayTeam:  awayTeam, // Eklendi!
                homeGoals: parseInt(scoreM[1], 10),
                awayGoals: parseInt(scoreM[2], 10),
                result:    resultVal,
            });
            if (rows.length >= 10) break;
        }
        return rows;
    };

    if (formTables.length > 0) result.homeForm = parseFormTable(formTables[0]);
    if (formTables.length > 1) result.awayForm = parseFormTable(formTables[1]);

    // ── 3. EN GOLCÜLER (MD-TABLE TESPİTİ) ────────────────────────────────────
    const scorerTables = [];
    const scorerRe = /En Golc[üu]ler\s*<\/div>\s*<table[^>]*>([\s\S]*?)<\/table>/g;
    let mScorer;
    while ((mScorer = scorerRe.exec(html)) !== null) {
        scorerTables.push(mScorer[1]);
    }

    const parseScorerTable = (tableHtml) => {
        const scorers = [];
        const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/g;
        let m;
        while ((m = rowRe.exec(tableHtml)) !== null) {
            const b = m[1];
            const nameM = b.match(/href="[^"]*\/Futbolcu\/\d+\/[^"]*">\s*([^<]+?)\s*<\/a>/);
            const goalsM = b.match(/<b>(\d+)<\/b>/);
            if (nameM && goalsM) {
                scorers.push({ name: nameM[1].trim(), goals: parseInt(goalsM[1], 10) });
                if (scorers.length >= 3) break;
            }
        }
        return scorers;
    };

    if (scorerTables.length > 0) result.homeScorers = parseScorerTable(scorerTables[0]);
    if (scorerTables.length > 1) result.awayScorers = parseScorerTable(scorerTables[1]);

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
