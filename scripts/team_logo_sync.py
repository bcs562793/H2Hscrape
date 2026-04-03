"""
update_live_logos.py
====================
live_matches tablosundaki home_logo ve away_logo alanlarını
data/teams_updated.json'daki api_logo URL'siyle günceller.

Eşleştirme: önce team_id ile direkt, bulunamazsa odds-update.js v6
ile aynı tokenSim + TEAM_ALIASES fuzzy eşleştirme.

Kullanım:
    SUPABASE_URL=... SUPABASE_KEY=... python update_live_logos.py

Proje yapısı (H2Hscrape):
    data/
        teams_updated.json   ← [{id, name, country, api_logo}]
    update_live_logos.py     ← bu dosya
"""

import json
import os
import re
import sys

from supabase import create_client

# ── Supabase bağlantısı ──────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("[HATA] SUPABASE_URL ve SUPABASE_KEY env değişkenleri gerekli")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── teams_updated.json yolu ──────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # scripts klasöründen bir üst dizine (ana dizine) çık
TEAMS_JSON   = os.path.join(PROJECT_ROOT, "data", "teams_updated.json")

# ── Eşik değerleri (odds-update.js v6 ile aynı) ─────────────────────
THRESHOLD    = 0.40
MIN_PER_TEAM = 0.25


# ═══════════════════════════════════════════════════════════════════════
# EŞLEŞTIRME ARAÇLARI  (odds-update.js v6 Python karşılığı)
# ═══════════════════════════════════════════════════════════════════════

def norm(s: str) -> str:
    s = (s or "").lower()
    for old, new in [("ğ","g"),("ü","u"),("ş","s"),("ı","i"),("ö","o"),("ç","c")]:
        s = s.replace(old, new)
    s = re.sub(r"[^a-z0-9]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


TEAM_ALIASES: dict[str, str] = {
    "seattle s"                  : "seattle sounders",
    "st louis"                   : "s louis city",
    "s san jose"                 : "deportivo saprissa",
    "cs cartagines"              : "cartagines",
    "gabala"                     : "kabala",
    "panaitolikos"               : "paneitolikos",
    "panserraikos"               : "panseraikos",
    "rz pellets wac"             : "wolfsberger",
    "tsv egger glas hartberg"    : "hartberg",
    "fc red bull salzburg"       : "salzburg",
    "ksv 1919"                   : "kapfenberger sv",
    "sk rapid ii"                : "r wien amt",
    "sw bregenz"                 : "schwarz weiss b",
    "sk austria klagenfurt"      : "klagenfurt",
    "skn st polten"              : "st polten",
    "skn st pölten"              : "st polten",
    "fc hertha wels"             : "wsc hertha",
    "b68 toftir"                 : "tofta itrottarfelag b68",
    "ca ferrocarril midland"     : "f midland",
    "gimnasia y esgrima de men"  : "gimnasia y",
    "estudiantes rio cuarto"     : "e rio cuarto",
    "ind medellin"               : "ind medellin",
    "america de cali"            : "america cali",
    "napredak"                   : "fk napredak kru",
    "tsc backa to"               : "tsc backa t",
    "d makhachkala"              : "dyn makhachkala",
    "rfc liege"                  : "rfc liege",
    "raal la louviere"           : "raal la louviere",
    "racing genk b"              : "j krc genk u23",
    "h w welders"                : "harland wolff w",
    "adelaide united fc k"       : "adelaide utd k",
    "canberra utd k"             : "canberra utd k",
    "brisbane roar fc k"         : "brisbane r k",
    "kyzylzhar"                  : "kyzyl zhar sk",
    "d batumi"                   : "dinamo b",
    "algeciras cf"               : "algeciras",
    "ibiza"                      : "i eivissa",
    "gubbio"                     : "as gubbio 1910",
    "pineto"                     : "asd pineto calcio",
    "mont tuscia"                : "monterosi t",
    "ssd casarano calcio"        : "casarano",
    "palermo"                    : "us palermo",
    "avellino"                   : "as avellino 1912",
    "utdofmanch"                 : "utd of manch",
    "sg sonnenhof grossaspach"   : "grossaspach",
    "chengdu"                    : "chengdu ron",
    "qingdao y i"                : "qingdao yth is",
    "bragantino"                 : "rb bragantino",
    "palmeiras"                  : "palmeiras sp",
    "gremio"                     : "gremio p",
    "baltika"                    : "b kaliningrad",
    "velez"                      : "v sarsfield",
    "s shenhua"                  : "shanghai s",
    "tianjin jinmen"             : "tianjin jin",
    "g birligi"                  : "genclerbirligi",
    "1 fc slovacko"              : "slovacko",
    "jagiellonia"                : "j bialystok",
    "ilves"                      : "tampereen i",
    "auvergne"                   : "le puy foot 43",
    "juventud"                   : "ca juventud de las piedras",
    "akademisk bo"               : "ab gladsaxe",
    "lusitania de lourosa"       : "lusitania",
    "stade nyonnais"             : "std nyonnis",
    "fc zurich"                  : "zurih",
    "cordoba cf"                 : "cordoba",
    "deportivo"                  : "dep la coruna",
    "masr"                       : "zed",
    "future fc"                  : "modern sport club",
    "new york rb"                : "ny red bulls",
    "the new saints"             : "tns",
    "vancouver"                  : "v whitecaps",
    "fc hradec kralove"          : "h kralove",
    "fc midtjylland"             : "midtjylland",
    "sonderjyske"                : "sonderjyske",
    "pacos de ferreira"          : "p ferreira",
    "mingachevir"                : "mingecevir",
    "oleksandriya"               : "oleksandriia",
}


def norm_alias(s: str) -> str:
    n = norm(s)
    return TEAM_ALIASES.get(n, n)


def token_sim(a: str, b: str) -> float:
    ta = {t for t in norm(a).split() if len(t) > 1}
    tb = {t for t in norm(b).split() if len(t) > 1}
    if not ta or not tb:
        return 0.0
    hit = 0.0
    for t in ta:
        if t in tb:
            hit += 1
            continue
        for u in tb:
            if t.startswith(u) or u.startswith(t):
                hit += 0.7
                break
    return hit / max(len(ta), len(tb))


def find_logo(
    team_id:     int | None,
    team_name:   str,
    by_id:       dict[int, dict],
    by_norm:     dict[str, dict],
) -> str | None:
    """
    Takım için api_logo URL'sini döndürür.
    Önce ID, bulunamazsa fuzzy isim eşleştirmesi.
    """
    # 1. ID
    if team_id and team_id in by_id:
        return by_id[team_id].get("api_logo") or None

    # 2. Exact norm
    na = norm_alias(team_name)
    if na in by_norm:
        return by_norm[na].get("api_logo") or None

    # 3. Fuzzy
    best_score = 0.0
    best_logo  = None
    for jnorm, jentry in by_norm.items():
        s = token_sim(na, jnorm)
        if s > best_score:
            best_score = s
            best_logo  = jentry.get("api_logo")

    if best_score >= THRESHOLD and best_logo:
        return best_logo

    return None


# ═══════════════════════════════════════════════════════════════════════
# ANA AKIŞ
# ═══════════════════════════════════════════════════════════════════════

def main() -> None:
    print("=" * 60)
    print("  H2Hscrape – Live Matches Logo Updater")
    print("=" * 60, "\n")

    # ── 1. teams_updated.json yükle ─────────────────────────────────
    if not os.path.exists(TEAMS_JSON):
        print(f"[HATA] {TEAMS_JSON} bulunamadı!")
        sys.exit(1)

    with open(TEAMS_JSON, encoding="utf-8") as f:
        raw = json.load(f)

    by_id   = {t["id"]: t for t in raw if "id" in t}
    by_norm = {norm(t["name"]): t for t in raw}
    print(f"[JSON] {len(by_id)} takım yüklendi")

    # ── 2. Supabase'den live_matches çek ────────────────────────────
    print("[DB] live_matches çekiliyor...")
    resp = sb.table("live_matches") \
             .select("fixture_id, home_team_id, away_team_id, home_team, away_team") \
             .execute()

    rows = resp.data or []
    if not rows:
        print("[DB] live_matches tablosu boş.")
        return
    print(f"[DB] {len(rows)} satır alındı")

    # ── 3. Her satır için logo bul ve güncelle ──────────────────────
    updates   = []
    no_home   = []
    no_away   = []

    for row in rows:
        fixture_id = row["fixture_id"]
        home_id    = row.get("home_team_id")
        away_id    = row.get("away_team_id")
        home_name  = row.get("home_team", "")
        away_name  = row.get("away_team", "")

        home_logo = find_logo(home_id, home_name, by_id, by_norm)
        away_logo = find_logo(away_id, away_name, by_id, by_norm)

        if home_logo and away_logo:
            updates.append({
                "fixture_id": fixture_id,
                "home_logo":  home_logo,
                "away_logo":  away_logo,
            })
            print(f"  ✓ [{fixture_id}] {home_name} vs {away_name}")
        else:
            if not home_logo:
                no_home.append((fixture_id, home_name))
                print(f"  ~ [{fixture_id}] home logo YOK: '{home_name}'")
            if not away_logo:
                no_away.append((fixture_id, away_name))
                print(f"  ~ [{fixture_id}] away logo YOK: '{away_name}'")

            # Kısmen de olsa yaz
            patch: dict = {"fixture_id": fixture_id}
            if home_logo: patch["home_logo"] = home_logo
            if away_logo: patch["away_logo"] = away_logo
            if len(patch) > 1:
                updates.append(patch)

    # ── 4. Supabase'e toplu upsert ───────────────────────────────────
    if not updates:
        print("\n[DB] Yazılacak güncelleme yok.")
        return

    print(f"\n[DB] {len(updates)} satır güncelleniyor...")
    BATCH = 100
    total_written = 0
    for i in range(0, len(updates), BATCH):
        batch = updates[i : i + BATCH]
        err_resp = sb.table("live_matches") \
                     .upsert(batch, on_conflict="fixture_id") \
                     .execute()
        total_written += len(batch)
        print(f"  [{i+len(batch)}/{len(updates)}] yazıldı")

    print(f"\n[DB] ✅ {total_written} satır güncellendi")

    # ── 5. Özet ─────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"  Toplam satır      : {len(rows)}")
    print(f"  Güncellenen       : {total_written}")
    print(f"  Home logo YOK     : {len(no_home)}")
    print(f"  Away logo YOK     : {len(no_away)}")

    if no_home or no_away:
        print("\n  Logosu bulunamayan takımlar:")
        for fid, name in no_home:
            print(f"    home [{fid}] '{name}'")
        for fid, name in no_away:
            print(f"    away [{fid}] '{name}'")


if __name__ == "__main__":
    main()
