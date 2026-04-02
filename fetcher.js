const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ── Ayarlar ──────────────────────────────────────────────────────────────────
const DATA_DIR      = path.join(__dirname, 'data');
const LOGOS_DIR     = path.join(__dirname, 'logos', 'teams');
const TEAMS_FILE    = path.join(DATA_DIR, 'teams_new.json'); 
const PROGRESS_FILE = path.join(DATA_DIR, 'mackolik_processed_ids.json');

const MACKOLIK_LOGO_URL = (id) => `https://im.mackolik.com/img/logo/buyuk/${id}.gif`;
const MACKOLIK_LIVEDATA = (date) => `https://vd.mackolik.com/livedata?date=${date}&s=1`;

const DAYS_TO_SCAN = 90; // 90 Günlük Tarama

// ── Yardımcı: Tarih üret ──────────────────────────────────────────────────
function generateDates(dayCount) {
    const dates = [];
    const today = new Date();
    for (let i = 0; i < dayCount; i++) {
        const d = new Date(today);
        d.setDate(today.getDate() - i);
        const day   = String(d.getDate()).padStart(2, '0');
        const month = String(d.getMonth() + 1).padStart(2, '0');
        const year  = d.getFullYear();
        dates.push(`${day}/${month}/${year}`);
    }
    return dates;
}

// ── 1. Mackolik Livedata'dan 90 Günlük ID Topla ───────────────────────────
async function collectTeamIdsFromMackolik() {
    const dates = generateDates(DAYS_TO_SCAN);
    const teamMap = {}; 

    console.log(`\n[1/4] Mackolik API'den son ${DAYS_TO_SCAN} günün maçları taranıyor...`);

    for (const date of dates) {
        try {
            const res = await fetch(MACKOLIK_LIVEDATA(date), {
                headers: { 
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Referer': 'https://www.mackolik.com/'
                },
                signal: AbortSignal.timeout(10000)
            });

            if (!res.ok) { await sleep(1000); continue; }

            const data = await res.json();
            const matches = data?.m || [];

            for (const match of matches) {
                if (!match) continue;

                const tournamentInfo = match.find(item => Array.isArray(item));
                
                // KESİN FUTBOL FİLTRESİ: 0. indeks spor dalını belirtir (1 = Futbol)
                if (!tournamentInfo || tournamentInfo[0] !== 1) continue;

                const homeId = match[1];
                const homeName = match[2] || '';
                const awayId = match[3];
                const awayName = match[4] || '';

                if (homeId) teamMap[homeId] = { id: homeId, name: homeName };
                if (awayId) teamMap[awayId] = { id: awayId, name: awayName };
            }

            process.stdout.write(`\r  ${date} → Bulunan Futbol Takımı: ${Object.keys(teamMap).length}  `);
            await sleep(300); 

        } catch (err) {
            // Sessizce atla
        }
    }
    console.log(`\n  Mackolik'ten toplam ${Object.keys(teamMap).length} futbol takım ID'si toplandı.`);
    return teamMap;
}

// ── 2. Tek bir logo indir ─────────────────────────────────────────────────
async function downloadLogo(teamId) {
    const logoUrl   = MACKOLIK_LOGO_URL(teamId);
    const localPath = path.join(LOGOS_DIR, `${teamId}.gif`);

    if (fs.existsSync(localPath) && fs.statSync(localPath).size > 100) return 'skip';

    try {
        const res = await fetch(logoUrl, {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            signal: AbortSignal.timeout(8000)
        });

        if (!res.ok) return 'fail';

        const buffer = Buffer.from(await res.arrayBuffer());
        if (buffer.length < 100) return 'empty'; 

        fs.writeFileSync(localPath, buffer);
        return 'ok';
    } catch {
        return 'fail';
    }
}

// ── 3. teams_new.json güncelle ────────────────────────────────────────────
function updateTeamsJson(downloadedIds) {
    let teams = [];
    if (fs.existsSync(TEAMS_FILE)) {
        const raw = JSON.parse(fs.readFileSync(TEAMS_FILE));
        teams = Array.isArray(raw) ? raw : Object.values(raw);
    }

    let added = 0;

    for (const [idStr, info] of Object.entries(downloadedIds)) {
        const mackolikId = parseInt(idStr); // ID'yi integer yap
        const logoUrl  = MACKOLIK_LOGO_URL(mackolikId);
        const localGif = path.join('logos', 'teams', `${mackolikId}.gif`);
        const exists   = fs.existsSync(path.join(LOGOS_DIR, `${mackolikId}.gif`));

        if (!exists) continue; 

        // Hem yeni format (mackolik_id) hem eski format (id) için kontrol
        const existing = teams.find(t => t.mackolik_id === mackolikId || t.id === mackolikId);
        
        if (existing) {
            existing.name = info.name;
            existing.api_logo = logoUrl;
            existing.logo_local = localGif;
            existing.mackolik_id = mackolikId; // Garantilemek için ekle
            delete existing.id; // Eski "id" anahtarını JSON'dan sil
        } else {
            teams.push({
                mackolik_id: mackolikId, // ARTIK DOĞRUDAN mackolik_id YAZIYORUZ
                name:       info.name,
                api_logo:   logoUrl,
                logo_local: localGif
            });
            added++;
        }
    }

    fs.writeFileSync(TEAMS_FILE, JSON.stringify(teams, null, 2));
    console.log(`  teams_new.json güncellendi → Toplam ${teams.length} takım (${added} yeni eklendi)`);
}

// ── 4. Git push ───────────────────────────────────────────────────────────
function gitPush() {
    console.log('\n[4/4] Değişiklikler GitHub\'a yükleniyor...');
    try {
        try { execSync('git config user.email "bot@mackoliksync.local"'); } catch(e){}
        try { execSync('git config user.name "Data Bot"'); } catch(e){}

        execSync('git add .');
        try { execSync('git commit -m "Otomatik Bot: Logolar ve takımlar güncellendi (mackolik_id formatı)"'); } 
        catch { console.log('  Gönderilecek yeni değişiklik yok.'); return; }
        
        console.log('  GitHub\'dan güncel veriler çekiliyor (Pull)...');
        execSync('git pull --rebase origin main');
        console.log('  GitHub\'a gönderiliyor (Push)...');
        execSync('git push origin main');
        console.log('  ✅ GitHub\'a başarıyla yüklendi!');
    } catch (err) {
        console.error('  ❌ Git hatası:', err.message);
    }
}

// ── Ana akış ─────────────────────────────────────────────────────────────
async function start() {
    console.log('═'.repeat(60));
    console.log('  90 Günlük Mackolik Logo Botu (mackolik_id formatlı)');
    console.log('═'.repeat(60));

    if (!fs.existsSync(DATA_DIR))  fs.mkdirSync(DATA_DIR,  { recursive: true });
    if (!fs.existsSync(LOGOS_DIR)) fs.mkdirSync(LOGOS_DIR, { recursive: true });

    let processedIds = new Set();
    if (fs.existsSync(PROGRESS_FILE)) {
        processedIds = new Set(JSON.parse(fs.readFileSync(PROGRESS_FILE)));
    }

    // 1. 90 Günlük ID ve İsimleri Çek
    const mackolikTeams = await collectTeamIdsFromMackolik();
    
    const allIds = Object.keys(mackolikTeams).map(Number).sort((a, b) => a - b);
    const remaining = allIds.filter(id => !processedIds.has(id));

    console.log(`\n[2/4] İşlem Özeti:`);
    console.log(`  Bulunan Toplam ID : ${allIds.length}`);
    console.log(`  Şimdi İndirilecek : ${remaining.length}`);

    if (remaining.length === 0) {
        updateTeamsJson(mackolikTeams); 
        gitPush();
        return;
    }

    console.log('\n[3/4] Logolar indiriliyor...\n');
    let ok = 0, skip = 0, fail = 0;

    for (let i = 0; i < remaining.length; i++) {
        const id     = remaining[i];
        const info   = mackolikTeams[id];
        const result = await downloadLogo(id);

        if (result === 'ok') { ok++; processedIds.add(id); console.log(`  ✓ İndirildi: ${id} - ${info.name}`); } 
        else if (result === 'skip') { skip++; processedIds.add(id); } 
        else { fail++; processedIds.add(id); }

        if ((i + 1) % 50 === 0) fs.writeFileSync(PROGRESS_FILE, JSON.stringify([...processedIds]));
        await sleep(150);
    }
    
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify([...processedIds]));
    console.log(`\n  Bilanço: ✓ ${ok} İndirildi  |  ⏭ ${skip} Zaten Vardı  |  ❌ ${fail} Hata`);

    updateTeamsJson(mackolikTeams);
    gitPush();
}

start().catch(console.error);
