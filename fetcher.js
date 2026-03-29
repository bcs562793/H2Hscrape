const fs = require('fs');
const path = require('path');

const API_KEY = process.env.API_KEY;
const SEASON = '2023'; // Güncel sezon

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function start() {
    const headers = { 'x-apisports-key': API_KEY };

    // 1. DÜNYADAKİ TÜM LİGLERİ OTOMATİK BUL (1 İstek harcar)
    console.log("Dünyadaki tüm ligler taranıyor...");
    const leaguesRes = await fetch('https://v3.football.api-sports.io/leagues', { headers });
    const leaguesData = await leaguesRes.json();

    if (!leaguesData.response) {
        console.error("Ligler çekilemedi, API günlük limitiniz dolmuş olabilir.");
        return;
    }

    const allLeagueIds = leaguesData.response.map(l => l.league.id);
    console.log(`Toplam ${allLeagueIds.length} adet lig bulundu!`);

    // 2. HAFIZA SİSTEMİ: Nerede kaldığımızı hatırla
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

    const progressFile = path.join(dataDir, 'processed_leagues.json');
    let processedLeagues = [];
    if (fs.existsSync(progressFile)) {
        processedLeagues = JSON.parse(fs.readFileSync(progressFile));
    }

    const teamsFile = path.join(dataDir, 'teams.json');
    let allTeams = [];
    if (fs.existsSync(teamsFile)) {
        allTeams = JSON.parse(fs.readFileSync(teamsFile));
    }

    // 3. SADECE YENİ (ÇEKİLMEMİŞ) LİGLERİ AYIR
    const remainingLeagues = allLeagueIds.filter(id => !processedLeagues.includes(id));
    console.log(`Geriye çekilecek ${remainingLeagues.length} lig kaldı.`);

    // Günde 100 istek limitimiz var. Her çalışmada 80 lig çekiyoruz (Güvenlik payı)
    const targetLeagues = remainingLeagues.slice(0, 80);

    if (targetLeagues.length === 0) {
        console.log("Tebrikler! Dünyadaki tüm ligler ve takımlar zaten deponuza çekilmiş.");
        return;
    }

    console.log(`Bu oturumda sıradaki ${targetLeagues.length} lig işleniyor...`);

    // 4. SIRADAKİ LİGLERİN TAKIMLARINI VE LOGOLARINI ÇEK
    for (const leagueId of targetLeagues) {
        console.log(`Lig ID ${leagueId} takımları çekiliyor...`);
        try {
            const res = await fetch(`https://v3.football.api-sports.io/teams?league=${leagueId}&season=${SEASON}`, { headers });
            const data = await res.json();
            
            // Eğer limite çarptıysak döngüyü güvenli şekilde kır
            if (!data.response || (data.errors && data.errors.requests)) {
                console.log("API limitine ulaşıldı, işlem durduruluyor...");
                break;
            }

            for (const item of data.response) {
                const team = item.team;
                
                // Takım havuzumuzda yoksa ekle
                if (!allTeams.find(t => t.id === team.id)) {
                    allTeams.push({ 
                        id: team.id, 
                        name: team.name, 
                        country: team.country,
                        api_logo: team.logo 
                    });
                }

                // Logoyu klasöre kaydet
                const dir = path.join(__dirname, 'logos', 'teams');
                if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
                const filePath = path.join(dir, `${team.id}.png`);

                // Sadece daha önce inmediyse indir (hız kazandırır)
                if (!fs.existsSync(filePath) && team.logo) {
                    const imgRes = await fetch(team.logo);
                    const buffer = await imgRes.arrayBuffer();
                    fs.writeFileSync(filePath, Buffer.from(buffer));
                    console.log(`İndirildi: ${team.name} (${team.id}.png)`);
                }
            }
            
            // Başarılı olan ligi hafızaya yaz
            processedLeagues.push(leagueId);

        } catch (err) {
            console.error(`Lig ${leagueId} çekilirken hata:`, err);
        }
        
        // Dakikada 10 istek sınırını aşmamak için her lig arası 7 saniye uyu
        await sleep(7000); 
    }

    // 5. TÜM YENİ VERİLERİ DOSYALARA KAYDET
    fs.writeFileSync(teamsFile, JSON.stringify(allTeams, null, 2));
    fs.writeFileSync(progressFile, JSON.stringify(processedLeagues, null, 2));
    
    console.log("Oturum bitti! Veriler ve yeni logolar GitHub'a kaydedilmeye hazır.");
}

start();
