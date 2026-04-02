"""
build_teams_json.py
===================
CSV dosyalarından (future_matches_rows.csv ve live_matches_rows.csv) tüm
takımları okur, Mackolik CDN'inden logoları indirir ve data/teams.json'a yazar.

Kullanım:
    python build_teams_json.py

Proje yapısı (H2Hscrape):
    data/
        teams.json          ← bu script tarafından oluşturulur / güncellenir
        logos/              ← indirilen .gif dosyaları {team_id}.gif
    future_matches_rows.csv
    live_matches_rows.csv
    build_teams_json.py     ← bu dosya

Veri kaynakları:
    live_matches_rows.csv  → kolonlar: home_team_id, home_team, home_logo,
                                        away_team_id, away_team, away_logo
    future_matches_rows.csv → kolon: data (JSON içinde teams.home / teams.away)

Logo mantığı:
    • logo URL'si im.mackolik.com içeriyorsa → indir, local path yaz
    • logo URL'si api-sports.io içeriyorsa   → URL'yi olduğu gibi bırak
    • live_matches logosu mackolik ise future'daki aynı takımın api-sports
      logosunun üzerine yaz (mackolik her zaman öncelikli)
"""

import csv
import json
import os
import re
import time
import requests

# ── Ayarlar ──────────────────────────────────────────────────────────────────
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
DATA_DIR         = os.path.join(BASE_DIR, "data")
LOGOS_DIR        = os.path.join(DATA_DIR, "logos")
TEAMS_JSON       = os.path.join(DATA_DIR, "teams.json")
LIVE_CSV         = os.path.join(BASE_DIR, "live_matches_rows.csv")
FUTURE_CSV       = os.path.join(BASE_DIR, "future_matches_rows.csv")

MACKOLIK_LOGO    = "https://im.mackolik.com/img/logo/buyuk/{id}.gif"
REQUEST_DELAY    = 0.15   # saniye - sunucuyu yormamak için
REQUEST_TIMEOUT  = 8      # saniye

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

os.makedirs(LOGOS_DIR, exist_ok=True)


# ── 1. CSV'lerden takım verisi topla ─────────────────────────────────────────
def load_teams_from_csvs() -> dict[str, dict]:
    """
    Her iki CSV'yi okuyarak takım sözlüğü döndürür.
    Yapı: { "team_id_str": {"id": int, "name": str, "logo": str} }
    """
    teams: dict[str, dict] = {}

    # --- future_matches_rows.csv ---
    if os.path.exists(FUTURE_CSV):
        with open(FUTURE_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    data = json.loads(row["data"])
                    for side in ("home", "away"):
                        t = data["teams"][side]
                        tid = str(t["id"])
                        teams[tid] = {
                            "id":   t["id"],
                            "name": t["name"],
                            "logo": t.get("logo", ""),
                        }
                except Exception:
                    pass
        print(f"[CSV]  future_matches → {len(teams)} takım")
    else:
        print(f"[UYARI] {FUTURE_CSV} bulunamadı, atlanıyor.")

    # --- live_matches_rows.csv ---
    live_count = 0
    if os.path.exists(LIVE_CSV):
        with open(LIVE_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                for id_col, name_col, logo_col in (
                    ("home_team_id", "home_team", "home_logo"),
                    ("away_team_id", "away_team", "away_logo"),
                ):
                    tid = row.get(id_col, "").strip()
                    if not tid:
                        continue
                    logo = row.get(logo_col, "").strip()
                    name = row.get(name_col, "").strip()

                    # Mackolik logosu varsa future verisinin üstüne yaz
                    if tid not in teams or "im.mackolik.com" in logo:
                        teams[tid] = {"id": int(tid), "name": name, "logo": logo}
                        live_count += 1

        print(f"[CSV]  live_matches   → {live_count} takım eklendi/güncellendi")
    else:
        print(f"[UYARI] {LIVE_CSV} bulunamadı, atlanıyor.")

    print(f"[CSV]  Toplam benzersiz takım: {len(teams)}")
    return teams


# ── 2. Var olan teams.json ile birleştir ─────────────────────────────────────
def merge_with_existing(new_teams: dict[str, dict]) -> dict[str, dict]:
    """Varsa mevcut teams.json'u yükler, yeni verilerle birleştirir."""
    if not os.path.exists(TEAMS_JSON):
        return new_teams

    with open(TEAMS_JSON, encoding="utf-8") as f:
        existing = json.load(f)

    before = len(existing)
    for tid, v in new_teams.items():
        # Mackolik logolu yeni veri her zaman kazanır
        if tid not in existing or "im.mackolik.com" in v.get("logo", ""):
            existing[tid] = v

    print(f"[JSON] Mevcut: {before} → Birleştirilmiş: {len(existing)} takım")
    return existing


# ── 3. Mackolik logolarını indir ─────────────────────────────────────────────
def _mackolik_id_from_url(logo_url: str) -> str | None:
    """Logo URL'sinden mackolik takım ID'sini çıkarır."""
    m = re.search(r"/buyuk/(\d+)\.gif", logo_url)
    return m.group(1) if m else None


def download_logos(teams: dict[str, dict]) -> dict[str, dict]:
    """
    im.mackolik.com'lu logo URL'si olan takımların .gif dosyasını indirir.
    Logo indirildikten sonra 'logo_local' alanına yerel path kaydedilir.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    mackolik_teams = {
        tid: v for tid, v in teams.items()
        if "im.mackolik.com" in v.get("logo", "")
    }

    print(f"\n[İNDİR] {len(mackolik_teams)} takım için Mackolik logosu indiriliyor…")
    ok = skip = fail = 0

    for tid, team in sorted(mackolik_teams.items(), key=lambda x: int(x[0])):
        mac_id    = _mackolik_id_from_url(team["logo"]) or tid
        logo_url  = MACKOLIK_LOGO.format(id=mac_id)
        local_path = os.path.join(LOGOS_DIR, f"{mac_id}.gif")
        rel_path   = f"data/logos/{mac_id}.gif"   # proje köküne göre relative

        # Zaten varsa atla
        if os.path.exists(local_path) and os.path.getsize(local_path) > 100:
            teams[tid]["logo_local"] = rel_path
            skip += 1
            continue

        try:
            r = session.get(logo_url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and len(r.content) > 100:
                with open(local_path, "wb") as f:
                    f.write(r.content)
                teams[tid]["logo_local"] = rel_path
                print(f"  ✓ {mac_id:>8}  {team['name']}")
                ok += 1
            else:
                print(f"  ✗ {mac_id:>8}  {team['name']}  [{r.status_code}]")
                fail += 1
        except Exception as e:
            print(f"  ! {mac_id:>8}  {team['name']}  HATA: {e}")
            fail += 1

        time.sleep(REQUEST_DELAY)

    print(f"\n[İNDİR] Tamamlandı → ✓ {ok} yeni  ⏭ {skip} zaten var  ✗ {fail} başarısız")
    return teams


# ── 4. teams.json yaz ────────────────────────────────────────────────────────
def save_teams_json(teams: dict[str, dict]) -> None:
    """Takım sözlüğünü data/teams.json'a yazar."""
    with open(TEAMS_JSON, "w", encoding="utf-8") as f:
        json.dump(teams, f, ensure_ascii=False, indent=2)
    print(f"\n[JSON] {len(teams)} takım → {TEAMS_JSON}")


# ── 5. Ana akış ──────────────────────────────────────────────────────────────
def main() -> None:
    print("=" * 60)
    print("  H2Hscrape – Teams JSON & Logo Builder")
    print("=" * 60, "\n")

    teams   = load_teams_from_csvs()
    teams   = merge_with_existing(teams)
    teams   = download_logos(teams)
    save_teams_json(teams)

    # Özet
    mac_logos   = sum(1 for v in teams.values() if "im.mackolik.com" in v.get("logo",""))
    local_logos = sum(1 for v in teams.values() if v.get("logo_local"))
    api_logos   = sum(1 for v in teams.values() if "api-sports.io" in v.get("logo",""))
    print(f"\n{'─'*60}")
    print(f"  Toplam takım     : {len(teams)}")
    print(f"  Mackolik logolu  : {mac_logos}  (local: {local_logos})")
    print(f"  API-Sports logolu: {api_logos}")
    print(f"  Logolar klasörü  : {LOGOS_DIR}")
    print(f"  Çıktı JSON       : {TEAMS_JSON}")


if __name__ == "__main__":
    main()
