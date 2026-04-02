const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ── Ayarlar ──────────────────────────────────────────────────────────────────
const DATA_DIR      = path.join(__dirname, 'data');
const LOGOS_DIR     = path.join(__dirname, 'logos', 'teams');
const TEAMS_FILE    = path.join(DATA_DIR, 'teams_new.json');
const PROGRESS_FILE = path.join(DATA_DIR, 'mackolik_processed_ids.json');

const MACKOLIK_LOGO_URL  = (id) => `https://im.mackolik.com/img/logo/buyuk/${id}.gif`;
const MACKOLIK_LIVEDATA  = (date) => `https://vd.mackolik.com/livedata?date=${date}&s=1`;

// Kaç günlük maç verisinden ID toplayalım
const DAYS_TO_SCAN = 90; // Son 90 güne ait maçlardan ID topla

// ── Yardımcı: Tarih üret (DD/MM/YYYY) ───────────────────────────────────────
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

// ── 1. Livedata API'den takım ID'lerini topla ─────────────────────────────
async function collectTeamIdsFromLivedata() {
    const dates = generateDates(DAYS_TO_SCAN);
    const teamMap = {}; // { id: { id, name } }

    console.log(`\n[1/4] Livedata API'den son ${DAYS_TO_SCAN} günün maçları taranıyor...`);

    for (const date of dates) {
        try {
            const url = MACKOLIK_LIVEDATA(date);
            const res = await fetch(url, {
                headers: { 
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Referer': 'https://www.mackolik.com/',
                    'Origin': 'https://www.mackolik.com'
                },
                signal: AbortSignal.timeout(10000)
            });

            if (!res.ok) { await sleep(1000); continue; }

            const data = await res.json();
            const matches = data?.m || [];

            for (const match of matches) {
                if (!match) continue;

                // Futbol filtrelemesi: İçinde lig/turnuva verisi olan diziyi bul
                const tournamentInfo = match.find(item => Array.isArray(item));
                
                // 8. indeks branşı belirtir (1=Futbol). Eğer futbol değilse bu maçı atla.
                if (!tournamentInfo || tournamentInfo[8] !== 1) continue;

                // Index 1: Ev Sahibi ID, Index 2: Ev Sahibi Adı
                // Index 3: Deplasman ID, Index 4: Deplasman Adı
                const homeId = match[1];
                const homeName = match[2] || '';
                const awayId = match[3];
                const awayName = match[4] || '';

                if (homeId) teamMap[homeId] = { id: homeId, name: homeName };
                if (awayId) teamMap[awayId] = { id: awayId, name: awayName };
            }

            process.stdout.write(`\r  ${date} → Toplam Bulunan ID: ${Object.keys(teamMap).length}  `);
            await sleep(300); // API'ye nazik ol

        } catch (err) {
            // Hata olursa sessizce atla ve devam et
        }
    }

    console.log(`\n  Livedata'dan toplam ${Object.keys(teamMap).length} benzersiz futbol takım ID'si toplandı.`);
    return teamMap;
}

// ── 2. Mevcut teams.json'dan zaten bilinen mackolik ID'lerini al ──────────
function collectTeamIdsFromTeamsJson() {
    if (!fs.existsSync(TEAMS_FILE)) return {};

    const teams = JSON.parse(fs.readFileSync(TEAMS_FILE));
    const macTeams = {};

    for (const team of (Array.isArray(teams) ? teams : Object.values(teams))) {
        const logo = team.mackolik_logo || team.logo || team.api_logo || '';
        if (logo.includes('im.mackolik.com')) {
            // Logo URL'sinden ID'yi çek: /buyuk/12345.gif
            const match = logo.match(/\/buyuk\/(\d+)\.gif/);
            const macId = match ? parseInt(match[1]) : team.id;
            if (macId) {
                macTeams[macId] = { id: macId, name: team.name || '' };
            }
        }
    }

    console.log(`  teams_new.json'dan ${Object.keys(macTeams).length} Mackolik ID'li takım bulundu.`);
    return macTeams;
}

// ── 3. Tek bir logo indir ─────────────────────────────────────────────────
async function downloadLogo(teamId, teamName) {
    const logoUrl   = MACKOLIK_LOGO_URL(teamId);
    const localPath = path.join(LOGOS_DIR, `${teamId}.gif`);

    // Zaten var ve boyutu makul ise atla
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
        if (buffer.length < 100) return 'empty'; // geçersiz/boş GIF

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
        // teams.json array ya da object olabilir
        teams = Array.isArray(raw) ? raw : Object.values(raw);
    }

    let added = 0;

    for (const [idStr, info] of Object.entries(downloadedIds)) {
        const id       = parseInt(idStr);
        const logoUrl  = MACKOLIK_LOGO_URL(id);
        const localGif = path.join('logos', 'teams', `${id}.gif`);
        const exists   = fs.existsSync(path.join(LOGOS_DIR, `${id}.gif`));

        if (!exists) continue; // İndirilemeyen logoyu ekleme

        const existing = teams.find(t => t.id === id);
        if (existing) {
            // Sadece logo alanlarını güncelle
            existing.mackolik_logo = logoUrl;
            existing.logo_local    = localGif;
        } else {
            teams.push({
                id,
                name:          info.name || '',
                mackolik_logo: logoUrl,
                logo_local:    localGif,
            });
            added++;
        }
    }

    fs.writeFileSync(TEAMS_FILE, JSON.stringify(teams, null, 2));
    console.log(`  teams_new.json güncellendi → ${teams.length} takım (${added} yeni takım listeye eklendi)`);
}

// ── 5. Git push ───────────────────────────────────────────────────────────
function gitPush() {
    console.log('\n[4/4] Değişiklikler GitHub\'a yükleniyor...');
    try {
        // Sunucu/Action hatasını çözmek için geçici kimlik tanımlaması
        try { execSync('git config user.email "bot@mackoliksync.local"'); } catch(e){}
        try { execSync('git config user.name "Mackolik Bot"'); } catch(e){}

        execSync('git add .');
        try {
            execSync('git commit -m "Otomatik Bot: Mackolik takım logoları güncellendi"');
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
    console.log('  Mackolik Futbol Logo İndirici (Son 90 Gün)');
    console.log('═'.repeat(60));

    // Klasörleri oluştur
    if (!fs.existsSync(DATA_DIR))  fs.mkdirSync(DATA_DIR,  { recursive: true });
    if (!fs.existsSync(LOGOS_DIR)) fs.mkdirSync(LOGOS_DIR, { recursive: true });

    // Daha önce indirilmiş ID'leri yükle
    let processedIds = new Set();
    if (fs.existsSync(PROGRESS_FILE)) {
        processedIds = new Set(JSON.parse(fs.readFileSync(PROGRESS_FILE)));
        console.log(`  Daha önce işlenmiş: ${processedIds.size} takım ID'si\n`);
    }

    // ── ID toplama ──────────────────────────────────────────────────────
    const fromJson     = collectTeamIdsFromTeamsJson();       // JSON'dan
    const fromLivedata = await collectTeamIdsFromLivedata();  // API'den

    // Birleştir
    const allTeams = { ...fromLivedata, ...fromJson };
    const allIds   = Object.keys(allTeams).map(Number).sort((a, b) => a - b);

    // Henüz işlenmemiş ID'ler
    const remaining = allIds.filter(id => !processedIds.has(id));

    console.log(`\n[2/4] İşlem Özeti:`);
    console.log(`  Bulunan Toplam ID : ${allIds.length}`);
    console.log(`  Zaten İşlenmiş    : ${processedIds.size}`);
    console.log(`  Şimdi İndirilecek : ${remaining.length}`);

    if (remaining.length === 0) {
        console.log('\n  Tebrikler! Tüm logolar zaten indirilmiş.');
        gitPush();
        return;
    }

    // ── Logo indirme ─────────────────────────────────────────────────────
    console.log('\n[3/4] Logolar indiriliyor...\n');

    let ok = 0, skip = 0, fail = 0;
    const downloadedMap = {};

    for (let i = 0; i < remaining.length; i++) {
        const id     = remaining[i];
        const info   = allTeams[id] || { name: '' };
        const result = await downloadLogo(id, info.name);

        if (result === 'ok') {
            ok++;
            downloadedMap[id] = info;
            processedIds.add(id);
            console.log(`  ✓ [${ok + skip}/${remaining.length}] ${id} – ${info.name}`);
        } else if (result === 'skip') {
            skip++;
            processedIds.add(id);
            downloadedMap[id] = info;
        } else {
            fail++;
            processedIds.add(id); // Hatalı ID'yi de ekle ki bir dahaki sefer boşuna takılmasın
        }

        // Her 50 logoda bir state (durum) kaydet
        if ((i + 1) % 50 === 0) {
            fs.writeFileSync(PROGRESS_FILE, JSON.stringify([...processedIds]));
        }

        await sleep(150); // Sunucu engelini aşmak için bekleme (Rate limit)
    }

    // Son durumu kaydet
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify([...processedIds]));

    console.log(`\n  Bilanço: ✓ ${ok} İndirildi  |  ⏭ ${skip} Zaten Vardı  |  ❌ ${fail} Bulunamadı/Hata`);

    // ── teams.json güncelle ────────────────────────────────────────────
    updateTeamsJson(downloadedMap);

    // ── Git push ───────────────────────────────────────────────────────
    gitPush();
}

start().catch(console.error);
