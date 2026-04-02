const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ── Ayarlar ──────────────────────────────────────────────────────────────────
const DATA_DIR      = path.join(__dirname, 'data');
const LOGOS_DIR     = path.join(__dirname, 'logos', 'teams');
// Dart kodunun okuduğu dosya adı 'teams.json' olduğu için doğrudan onu güncelliyoruz
const TEAMS_FILE    = path.join(DATA_DIR, 'teams.json'); 
const PROGRESS_FILE = path.join(DATA_DIR, 'bilyoner_processed_ids.json');

const MACKOLIK_LOGO_URL = (id) => `https://im.mackolik.com/img/logo/buyuk/${id}.gif`;

// Bilyoner API Ayarları (Dart kodunla birebir uyumlu)
const BILYONER_BASE = 'https://www.bilyoner.com';
const BILYONER_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'tr',
    'platform-token': '40CAB7292CD83F7EE0631FC35A0AFC75',
    'x-client-app-version': '3.95.2',
    'x-client-browser-version': 'Chrome / v146.0.0.0',
    'x-client-channel': 'WEB',
    'x-device-id': 'C1A34687-8F75-47E8-9FF9-1D231F05782E',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'
};

// ── 1. Bilyoner API'den takım ID'lerini ve İsimlerini topla ────────────────
async function collectTeamIdsFromBilyoner() {
    const teamMap = {}; // { id: { id, name } }
    
    // 1: Canlı (Live), 2: Maç Önü (Prematch), 3: Bitenler (Results - Opsiyonel)
    const bulletinTypes = [1, 2, 3]; 

    console.log(`\n[1/4] Bilyoner API'den güncel bültenler (Canlı & Maç Önü) taranıyor...`);

    for (const bType of bulletinTypes) {
        try {
            const url = `${BILYONER_BASE}/api/v3/mobile/aggregator/gamelist/all/v1?tabType=1&bulletinType=${bType}`;
            const res = await fetch(url, { headers: BILYONER_HEADERS, signal: AbortSignal.timeout(15000) });

            if (!res.ok) {
                console.log(`  ⚠️ BulletinType=${bType} HTTP Hata: ${res.status}`);
                continue;
            }

            const data = await res.json();
            const eventsRaw = data?.events || {};
            
            // Dart kodundaki gibi sadece st === 1 (Futbol) olanları alıyoruz
            const footballMatches = Object.values(eventsRaw).filter(e => e.st === 1);

            for (const ev of footballMatches) {
                const homeId = ev.htpi;
                const awayId = ev.atpi;
                const homeName = ev.htn;
                const awayName = ev.atn;

                if (homeId && homeName) teamMap[homeId] = { id: homeId, name: homeName };
                if (awayId && awayName) teamMap[awayId] = { id: awayId, name: awayName };
            }

            console.log(`  ✓ BulletinType=${bType} tarandı. Toplam bulunan benzersiz takım: ${Object.keys(teamMap).length}`);
            await sleep(1000); // Bilyoner API'ye nazik ol (Rate limit engeli yememek için)

        } catch (err) {
            console.log(`  ⚠️ BulletinType=${bType} Çekilemedi: ${err.message}`);
        }
    }

    console.log(`\n  Bilyoner'den toplam ${Object.keys(teamMap).length} benzersiz futbol takım verisi (isim+id) toplandı.`);
    return teamMap;
}

// ── 2. Mevcut teams.json'dan zaten bilinen takımları al ───────────────────
function collectTeamIdsFromTeamsJson() {
    if (!fs.existsSync(TEAMS_FILE)) return {};

    const teams = JSON.parse(fs.readFileSync(TEAMS_FILE));
    const localTeams = {};

    for (const team of (Array.isArray(teams) ? teams : Object.values(teams))) {
        if (team.id) {
            localTeams[team.id] = { id: team.id, name: team.name || '' };
        }
    }

    console.log(`  teams.json'dan ${Object.keys(localTeams).length} takım bulundu.`);
    return localTeams;
}

// ── 3. Tek bir logo indir (Mackolik CDN'den) ──────────────────────────────
async function downloadLogo(teamId, teamName) {
    const logoUrl   = MACKOLIK_LOGO_URL(teamId);
    const localPath = path.join(LOGOS_DIR, `${teamId}.gif`);

    if (fs.existsSync(localPath) && fs.statSync(localPath).size > 100) {
        return 'skip';
    }

    try {
        const res = await fetch(logoUrl, {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
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

// ── 4. teams.json güncelle ────────────────────────────────────────────────
function updateTeamsJson(downloadedIds) {
    let teams = [];
    if (fs.existsSync(TEAMS_FILE)) {
        const raw = JSON.parse(fs.readFileSync(TEAMS_FILE));
        teams = Array.isArray(raw) ? raw : Object.values(raw);
    }

    let updated = 0;
    let added = 0;

    for (const [idStr, info] of Object.entries(downloadedIds)) {
        const id       = parseInt(idStr);
        const logoUrl  = MACKOLIK_LOGO_URL(id);
        const localGif = path.join('logos', 'teams', `${id}.gif`);
        const exists   = fs.existsSync(path.join(LOGOS_DIR, `${id}.gif`));

        if (!exists) continue; // Logo başarıyla inmediyse JSON'a ekleme

        const existing = teams.find(t => t.id === id);
        if (existing) {
            // İSİM GÜNCELLEMESİ: Mackolik ismini ezip Bilyoner ismini yazıyoruz
            if (existing.name !== info.name) {
                existing.name = info.name; 
                updated++;
            }
            // Dart kodu api_logo beklediği için bu key'i garanti altına alıyoruz
            existing.api_logo = logoUrl;
            existing.logo_local = localGif;
        } else {
            teams.push({
                id,
                name:       info.name || '',
                api_logo:   logoUrl,   // Dart kodun doğrudan bunu okuyacak
                logo_local: localGif,
                country:    '' // İstersen daha sonra geliştirebilirsin
            });
            added++;
        }
    }

    fs.writeFileSync(TEAMS_FILE, JSON.stringify(teams, null, 2));
    console.log(`  teams.json güncellendi → Toplam: ${teams.length} takım (${added} yeni eklendi, ${updated} takımın ismi Bilyoner'e göre güncellendi)`);
}

// ── 5. Git push ───────────────────────────────────────────────────────────
function gitPush() {
    console.log('\n[4/4] Değişiklikler GitHub\'a yükleniyor...');
    try {
        try { execSync('git config user.email "bot@bilyonersync.local"'); } catch(e){}
        try { execSync('git config user.name "Bilyoner Bot"'); } catch(e){}

        execSync('git add .');
        try {
            execSync('git commit -m "Otomatik Bot: Bilyoner isimleri ve logoları senkronize edildi"');
        } catch {
            console.log('  Gönderilecek yeni değişiklik yok.');
            return;
        }
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
    console.log('  Bilyoner-Mackolik Logo ve İsim Senkronizasyon Botu');
    console.log('═'.repeat(60));

    if (!fs.existsSync(DATA_DIR))  fs.mkdirSync(DATA_DIR,  { recursive: true });
    if (!fs.existsSync(LOGOS_DIR)) fs.mkdirSync(LOGOS_DIR, { recursive: true });

    let processedIds = new Set();
    if (fs.existsSync(PROGRESS_FILE)) {
        processedIds = new Set(JSON.parse(fs.readFileSync(PROGRESS_FILE)));
        console.log(`  Daha önce işlenmiş: ${processedIds.size} takım ID'si\n`);
    }

    const fromJson     = collectTeamIdsFromTeamsJson();
    const fromBilyoner = await collectTeamIdsFromBilyoner(); 

    // ÖNCELİK BİLYONER'DE: Bilyoner isimleri Mackolik isimlerini ezecek
    const allTeams = { ...fromJson, ...fromBilyoner };
    const allIds   = Object.keys(allTeams).map(Number).sort((a, b) => a - b);

    const remaining = allIds.filter(id => !processedIds.has(id));

    console.log(`\n[2/4] İşlem Özeti:`);
    console.log(`  Bulunan Toplam ID : ${allIds.length}`);
    console.log(`  Zaten İşlenmiş    : ${processedIds.size}`);
    console.log(`  Şimdi İndirilecek : ${remaining.length}`);

    if (remaining.length === 0 && Object.keys(fromBilyoner).length === 0) {
        console.log('\n  İşlem yapılacak veri bulunamadı.');
        return;
    }

    console.log('\n[3/4] Logolar kontrol ediliyor ve indiriliyor...\n');

    let ok = 0, skip = 0, fail = 0;
    const downloadedMap = {};

    // Kalan logoları indir
    for (let i = 0; i < remaining.length; i++) {
        const id     = remaining[i];
        const info   = allTeams[id] || { name: '' };
        const result = await downloadLogo(id, info.name);

        if (result === 'ok') {
            ok++;
            processedIds.add(id);
            console.log(`  ✓ [${ok + skip}/${remaining.length}] ${id} – ${info.name} (İndirildi)`);
        } else if (result === 'skip') {
            skip++;
            processedIds.add(id);
        } else {
            fail++;
            processedIds.add(id); 
        }

        if ((i + 1) % 50 === 0) {
            fs.writeFileSync(PROGRESS_FILE, JSON.stringify([...processedIds]));
        }
        await sleep(150);
    }
    
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify([...processedIds]));
    
    // json güncellemesi için hem indirilenleri hem de zaten var olup ismi değişmesi gerekenleri gönderiyoruz
    for(const id of processedIds){
        if(allTeams[id]) downloadedMap[id] = allTeams[id];
    }

    console.log(`\n  Bilanço: ✓ ${ok} İndirildi  |  ⏭ ${skip} Zaten Vardı  |  ❌ ${fail} Bulunamadı/Hata`);

    updateTeamsJson(downloadedMap);
    gitPush();
}

start().catch(console.error);
