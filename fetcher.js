const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ── Ayarlar ──────────────────────────────────────────────────────────────────
const DATA_DIR   = path.join(__dirname, 'data');
const TEAMS_FILE = path.join(DATA_DIR, 'teams_new.json'); 

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

    console.log(`\n[1/3] Mackolik API'den son ${DAYS_TO_SCAN} günün maçları taranıyor...`);

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
                
                // DOĞRULANMIŞ KESİN FUTBOL FİLTRESİ: 23. indeks spor dalını belirtir (1 = Futbol)
                if (match[23] !== 1) continue;

                const homeId = match[1];
                const homeName = match[2] || '';
                const awayId = match[3];
                const awayName = match[4] || '';

                if (homeId) teamMap[homeId] = { id: homeId, name: homeName };
                if (awayId) teamMap[awayId] = { id: awayId, name: awayName };
            }

            process.stdout.write(`\r  ${date} → Bulunan Dünya Geneli Futbol Takımı: ${Object.keys(teamMap).length}  `);
            await sleep(300); 

        } catch (err) {
            // Sessizce atla
        }
    }
    console.log(`\n  Mackolik'ten toplam ${Object.keys(teamMap).length} futbol takım ID'si toplandı.`);
    return teamMap;
}

// ── 2. teams_new.json güncelle (İndirme yok, sadece URL kaydı) ────────────
function updateTeamsJson(downloadedIds) {
    console.log(`\n[2/3] JSON dosyası güncelleniyor...`);
    let teams = [];
    if (fs.existsSync(TEAMS_FILE)) {
        const raw = JSON.parse(fs.readFileSync(TEAMS_FILE));
        teams = Array.isArray(raw) ? raw : Object.values(raw);
    }

    let added = 0;

    for (const [idStr, info] of Object.entries(downloadedIds)) {
        const mackolikId = parseInt(idStr); // ID'yi integer yap
        const logoUrl    = MACKOLIK_LOGO_URL(mackolikId);

        // Hem yeni format (mackolik_id) hem eski format (id) için kontrol
        const existing = teams.find(t => t.mackolik_id === mackolikId || t.id === mackolikId);
        
        if (existing) {
            existing.name = info.name;
            existing.api_logo = logoUrl;
            existing.mackolik_id = mackolikId; // Garantilemek için ekle
            delete existing.id; // Eski "id" anahtarını JSON'dan sil
            delete existing.logo_local; // Artık lokal indirme yapmadığımız için bu anahtarı temizle
        } else {
            teams.push({
                mackolik_id: mackolikId, 
                name:       info.name,
                api_logo:   logoUrl
            });
            added++;
        }
    }

    fs.writeFileSync(TEAMS_FILE, JSON.stringify(teams, null, 2));
    console.log(`  teams_new.json güncellendi → Toplam ${teams.length} takım (${added} yeni eklendi)`);
}

// ── 3. Git push ───────────────────────────────────────────────────────────
function gitPush() {
    console.log('\n[3/3] Değişiklikler GitHub\'a yükleniyor...');
    try {
        try { execSync('git config user.email "bot@mackoliksync.local"'); } catch(e){}
        try { execSync('git config user.name "Data Bot"'); } catch(e){}

        execSync('git add .');
        try { execSync('git commit -m "Otomatik Bot: Takım logoları (URL) güncellendi"'); } 
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
    console.log('  90 Günlük Mackolik URL Botu (Hızlı, İndirmesiz & 100% Futbol)');
    console.log('═'.repeat(60));

    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

    // 1. 90 Günlük ID ve İsimleri Çek
    const mackolikTeams = await collectTeamIdsFromMackolik();
    
    if (Object.keys(mackolikTeams).length === 0) {
        console.log('  Hiç takım bulunamadı, işlem sonlandırılıyor.');
        return;
    }

    // 2. JSON'u Güncelle
    updateTeamsJson(mackolikTeams);

    // 3. Git Push
    gitPush();
}

start().catch(console.error);
