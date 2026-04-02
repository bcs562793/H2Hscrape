const fs = require('fs');
const path = require('path');

// GitHub Actions ortamında çalışma dizinini (workspace) kök olarak alıyoruz
const WORKSPACE = process.env.GITHUB_WORKSPACE || process.cwd();
const DATA_DIR = path.join(WORKSPACE, 'data');

// Dosya yolları
const TEAMS_FILE = path.join(DATA_DIR, 'teams.json');
const TEAMS_NEW_FILE = path.join(DATA_DIR, 'teams_new.json');
const OUTPUT_FILE = path.join(DATA_DIR, 'teams_updated.json');

// İsimleri eşleştirirken pürüzleri gidermek için temizleme fonksiyonu
function normalizeName(name) {
    if (!name) return '';
    return name.toLowerCase().trim()
        .replace(/ş/g, 's').replace(/ğ/g, 'g').replace(/ü/g, 'u')
        .replace(/ö/g, 'o').replace(/ç/g, 'c').replace(/ı/g, 'i').replace(/i̇/g, 'i')
        .replace(/[^a-z0-9]/g, ''); // Sadece harf ve rakamları bırakır (boşluk ve noktaları siler)
}

function start() {
    console.log('🔄 Takım birleştirme işlemi başlıyor...\n');
    console.log(`Aranan dizin: ${DATA_DIR}`);

    // Dosyaları oku
    if (!fs.existsSync(TEAMS_FILE) || !fs.existsSync(TEAMS_NEW_FILE)) {
        console.error('❌ teams.json veya teams_new.json bulunamadı!');
        console.log(`Beklenen teams.json yolu: ${TEAMS_FILE}`);
        console.log(`Beklenen teams_new.json yolu: ${TEAMS_NEW_FILE}`);
        return;
    }

    const teams = JSON.parse(fs.readFileSync(TEAMS_FILE, 'utf8'));
    const teamsNew = JSON.parse(fs.readFileSync(TEAMS_NEW_FILE, 'utf8'));

    let matchedCount = 0;
    let addedCount = 0;

    // teams_new.json içindeki her bir takımı dön
    for (const newTeam of teamsNew) {
        const normNewName = normalizeName(newTeam.name);

        // teams.json içinde bu takımı bulmaya çalış
        let existingTeam = teams.find(t => normalizeName(t.name) === normNewName);

        if (existingTeam) {
            // 1. DURUM: Takım eşleşti! 
            // id değerini mackolik_id ile değiştir (ama key "id" olarak kalacak)
            existingTeam.id = newTeam.mackolik_id;
            existingTeam.api_logo = newTeam.api_logo; // Logoyu da mackolik logosuyla güncelle
            matchedCount++;
        } else {
            // 2. DURUM: Takım teams.json'da yok!
            // Yeni takım olarak ekle
            teams.push({
                id: newTeam.mackolik_id,  // mackolik_id değerini id anahtarına yaz
                name: newTeam.name,
                country: "", // teams_new'da ülke olmadığı için boş bırakıyoruz
                api_logo: newTeam.api_logo
            });
            addedCount++;
        }
    }

    // Güncellenmiş listeyi yeni bir dosyaya kaydet
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(teams, null, 2));

    // Sonuç Raporu
    console.log(`✅ İşlem tamamlandı! Sonuçlar ${OUTPUT_FILE} dosyasına kaydedildi.`);
    console.log(`--------------------------------------------------`);
    console.log(`Eski listede olup eşleşen ve ID'si güncellenen : ${matchedCount} takım`);
    console.log(`Eski listede olmayıp YENİ eklenen              : ${addedCount} takım`);
    console.log(`--------------------------------------------------`);
    console.log(`Güncel teams.json toplam takım sayısı          : ${teams.length}`);
}

start();
