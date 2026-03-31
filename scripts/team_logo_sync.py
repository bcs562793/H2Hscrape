"""
scripts/team_logo_sync.py
─────────────────────────
GitHub Actions üzerinden her gün TR 06:00'da çalışır.
Akıllı çok katmanlı fuzzy eşleştirme kullanır.

Gerekli repo secret:
    DATABASE_URL  →  postgresql://user:pass@host:5432/dbname
"""

import logging
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
from rapidfuzz import fuzz

# ── Ayarlar ───────────────────────────────────────────────────────────────────

GITHUB_URL = (
    "https://raw.githubusercontent.com/bcs562793/H2Hscrape/main/data/teams.json"
)

DB_DSN = os.environ["DATABASE_URL"]

MIN_SCORE = 72   # Bu skorun altı low_confidence = TRUE

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Normalizasyon ─────────────────────────────────────────────────────────────

TR_CHARS = str.maketrans("şŞğĞüÜöÖçÇıİ", "sSgGuUoOcCiI")

# Eşleştirmeye gürültü katan token'lar — normalize sonrası çıkarılır
NOISE = {
    "(k)", "(w)", "(f)", "w", "kadin", "women", "feminin",
    "ii", "iii", "iv", "reserves", "res", "b",
    "fc", "sc", "cf", "ac", "if", "bk", "sk",
    "afc", "bfc", "cfc", "sfc", "rfc",
    "club", "cp", "cd", "sd", "ud", "rc", "rcd",
    "united", "city", "athletic", "athletics",
    "football", "futbol",
}

# Key'ler ASCII'ye çevrilmiş Türkçe (normalize() sonrası eşleşir)
# "Karadağ" → TR_CHARS → "Karadag" → to_ascii → "karadag"
TR_TO_EN: dict[str, str] = {
    # Milli takımlar / ülkeler
    "kuzey irlanda":    "northern ireland",
    "singapur":         "singapore",
    "bangledes":        "bangladesh",  # ş→s
    "maldivler":        "maldives",
    "tayvan":           "chinese taipei",
    "malezya":          "malaysia",
    "letonya":          "latvia",
    "litvanya":         "lithuania",
    "estonya":          "estonia",
    "belcika":          "belgium",     # ç→c
    "norvec":           "norway",      # ç→c
    "polonya":          "poland",
    "danimarka":        "denmark",
    "fransa":           "france",
    "hirvatistan":      "croatia",     # ı→i
    "romanya":          "romania",
    "avustralya":       "australia",
    "kamerun":          "cameroon",
    "kazakistan":       "kazakhstan",
    "komorlar":         "comoros",
    "cin":              "china",       # ç→c (tek kelime, çin→cin)
    "karadag":          "montenegro",  # ğ→g
    "isvicre":          "switzerland", # ş→s, ç→c
    "lihtenstayn":      "liechtenstein",
    "cekya":            "czech republic", # ç→c
    "arnavutluk":       "albania",
    "avusturya":        "austria",
    "finlandiya":       "finland",
    "hollanda":         "netherlands",
    "ispanya":          "spain",
    "italya":           "italy",
    "isvec":            "sweden",      # ç→c
    "iskocya":          "scotland",
    "japonya":          "japan",
    "guney kore":       "south korea", # ü→u
    "portekiz":         "portugal",
    "yunanistan":       "greece",
    "almanya":          "germany",
    "rusya":            "russia",
    "ukrayna":          "ukraine",
    "macaristan":       "hungary",
    "slovakya":         "slovakia",
    "slovenya":         "slovenia",
    "sirbistan":        "serbia",
    "bulgaristan":      "bulgaria",
    "cezayir":          "algeria",
    "fas":              "morocco",
    "tunus":            "tunisia",
    "misir":            "egypt",
    "nijerya":          "nigeria",
    "gana":             "ghana",
    "fildisi sahili":   "ivory coast",
    "arjantin":         "argentina",
    "brezilya":         "brazil",
    "kolombiya":        "colombia",
    "sili":             "chile",       # ş→s
    "uruguay":          "uruguay",
    "paraguay":         "paraguay",
    "ekvador":          "ecuador",
    "bolivya":          "bolivia",
    "venezuela":        "venezuela",
    "meksika":          "mexico",
    "kanada":           "canada",
    "kosta rika":       "costa rica",
    "jamaika":          "jamaica",
    "iran":             "iran",
    "irak":             "iraq",
    "suudi arabistan":  "saudi arabia",
    "katar":            "qatar",
    "kuveyt":           "kuwait",
    "urdun":            "jordan",      # ü→u
    "suriye":           "syria",
    "lubnan":           "lebanon",     # ü→u
    "israil":           "israel",
    "hindistan":        "india",
    "tayland":          "thailand",
    "endonezya":        "indonesia",
    "filipinler":       "philippines",
    "ozbekistan":       "uzbekistan",  # ö→o
    "azerbaycan":       "azerbaijan",
    "gurcistan":        "georgia",     # ü→u
    "ermenistan":       "armenia",
    "kirgizistan":      "kyrgyzstan",
    "tacikistan":       "tajikistan",
    "turkmenistan":     "turkmenistan",
    "galler":           "wales",
    "ingiltere":        "england",
    "irlanda":          "ireland",
    "kibris":           "cyprus",
    "izlanda":          "iceland",
    "faroe adalari":    "faroe islands",
    "sri lanka":        "sri lanka",
    "vietnam":          "vietnam",
    "curacao":          "curacao",
}


def to_ascii(s: str) -> str:
    """Unicode → ASCII (é→e, ñ→n, ü→u, vb.)"""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def normalize(name: str) -> str:
    """
    Eşleştirme için takım adını normalize eder:
    1. Türkçe harf → Latin (ş→s, ğ→g, ü→u …)
    2. Küçük harf + Unicode → ASCII
    3. Türkçe ülke/milli takım adı → İngilizce
    4. Parantez içleri sil
    5. Noktalama → boşluk
    6. Yaş grupları standartlaştır ("U 19" → "u19")
    7. Gürültü tokenları çıkar (fc, sc, united, city …)
    """
    # Adım 1-2
    s = to_ascii(name.translate(TR_CHARS)).lower()

    # Adım 3 — en uzun eşleşmeden başla (önce "kuzey irlanda", sonra "irlanda")
    for tr, en in sorted(TR_TO_EN.items(), key=lambda x: -len(x[0])):
        if s == tr or s.startswith(tr + " "):
            s = en + s[len(tr):]
            break

    # Adım 4 — parantez içleri
    s = re.sub(r"\([^)]*\)", "", s)

    # Adım 5 — noktalama
    s = re.sub(r"[.\-_/'\\]", " ", s)

    # Adım 6 — "u 19" / "u-21" → "u19"
    s = re.sub(r"\bu\s*(\d{2})\b", r"u\1", s)

    # Adım 7 — gürültü tokenları
    tokens = [t for t in s.split() if t not in NOISE]
    s = " ".join(tokens) if tokens else s.strip()

    return re.sub(r"\s+", " ", s).strip()


def abbreviation_score(query: str, candidate: str) -> float:
    """
    Kısaltma eşleştirmesi:
      "indep medell"  ↔  "independiente medellin"  → 100
      "dep maldonado" ↔  "deportivo maldonado"      → 100
    Her query token'ı, candidate token'larından birinin başlangıcıyla eşleşirse say.
    """
    q_tokens = query.split()
    c_tokens = candidate.split()
    if not q_tokens or not c_tokens:
        return 0.0

    matched = 0
    for qt in q_tokens:
        qt_clean = qt.rstrip(".")
        min_len  = max(3, len(qt_clean))
        for ct in c_tokens:
            if ct.startswith(qt_clean) or qt_clean.startswith(ct[:min_len]):
                matched += 1
                break

    ratio = matched / max(len(q_tokens), len(c_tokens))
    if matched == len(q_tokens):          # query tamamen eşleşti → bonus
        ratio = min(1.0, ratio * 1.1)
    return round(ratio * 100, 1)


def smart_score(query_raw: str, candidate_raw: str) -> float:
    """
    5 farklı stratejinin maksimumunu döner:
    • token_sort_ratio  — token sırası farksız edit distance
    • token_set_ratio   — ortak token seti oranı
    • partial_ratio     — alt-string / kısmi eşleşme
    • ratio             — düz Levenshtein
    • abbreviation_score — kısaltma mantığı
    """
    q = normalize(query_raw)
    c = normalize(candidate_raw)

    if not q or not c:
        return 0.0
    if q == c:
        return 100.0

    return max(
        fuzz.token_sort_ratio(q, c),
        fuzz.token_set_ratio(q, c),
        fuzz.partial_ratio(q, c),
        fuzz.ratio(q, c),
        abbreviation_score(q, c),
    )


# ── GitHub & DB ───────────────────────────────────────────────────────────────


def fetch_github_teams() -> list[dict]:
    log.info("GitHub teams.json çekiliyor...")
    r = requests.get(GITHUB_URL, timeout=30)
    r.raise_for_status()
    teams = r.json()
    log.info("%d takım yüklendi", len(teams))
    return teams


def get_live_teams(cur) -> list[dict]:
    cur.execute("""
        SELECT DISTINCT home_team_id AS team_id, home_team AS team_name
        FROM live_matches
        UNION
        SELECT DISTINCT away_team_id, away_team
        FROM live_matches
    """)
    rows = [{"team_id": r[0], "team_name": r[1]} for r in cur.fetchall()]
    log.info("%d benzersiz takım bulundu", len(rows))
    return rows


def get_overrides(cur) -> dict[int, dict]:
    """team_logo_override'daki manuel düzeltmeleri döner."""
    cur.execute("""
        SELECT live_team_id, gh_team_id, gh_team_name, api_logo
        FROM team_logo_override
    """)
    return {
        r[0]: {"gh_team_id": r[1], "gh_team_name": r[2], "api_logo": r[3]}
        for r in cur.fetchall()
    }


# ── Eşleştirme ────────────────────────────────────────────────────────────────


def match_teams(
    live_teams: list[dict],
    gh_teams: list[dict],
    overrides: dict[int, dict],
) -> list[dict]:
    results  = []
    low_list = []

    for lt in live_teams:
        tid = lt["team_id"]

        # 1. Manuel override var mı?
        if tid in overrides:
            ov = overrides[tid]
            results.append({
                "live_team_id":   tid,
                "live_team_name": lt["team_name"],
                "gh_team_id":     ov["gh_team_id"],
                "gh_team_name":   ov["gh_team_name"],
                "api_logo":       ov["api_logo"],
                "match_score":    100.0,
                "low_confidence": False,
            })
            continue

        # 2. Akıllı fuzzy eşleştirme
        best_team  = None
        best_score = 0.0

        for gh_team in gh_teams:
            s = smart_score(lt["team_name"], gh_team["name"])
            if s > best_score:
                best_score = s
                best_team  = gh_team
            if s == 100.0:
                break   # mükemmel eşleşme bulundu

        low = best_score < MIN_SCORE

        results.append({
            "live_team_id":   tid,
            "live_team_name": lt["team_name"],
            "gh_team_id":     best_team["id"]               if best_team else None,
            "gh_team_name":   best_team["name"]             if best_team else None,
            "api_logo":       best_team.get("api_logo", "") if best_team else "",
            "match_score":    round(best_score, 1),
            "low_confidence": low,
        })

        if low:
            low_list.append(
                f"  {lt['team_name']!r:35s} → "
                f"{best_team['name']!r if best_team else 'YOK':30s} "
                f"({best_score:.0f})"
            )

    high = sum(1 for r in results if not r["low_confidence"])
    log.info(
        "Eşleştirme tamamlandı: %d yüksek güven, %d düşük güven",
        high, len(results) - high,
    )
    if low_list:
        log.warning(
            "Düşük güven (%d) — live_matches'e yazılmadı:\n%s",
            len(low_list), "\n".join(low_list),
        )

    return results


# ── DB yazma ─────────────────────────────────────────────────────────────────


def ensure_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_logo_mapping (
            live_team_id    INTEGER      PRIMARY KEY,
            live_team_name  TEXT         NOT NULL,
            gh_team_id      INTEGER,
            gh_team_name    TEXT,
            api_logo        TEXT,
            match_score     NUMERIC(5,1),
            low_confidence  BOOLEAN      DEFAULT FALSE,
            updated_at      TIMESTAMPTZ  DEFAULT now()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_logo_override (
            live_team_id   INTEGER      PRIMARY KEY,
            live_team_name TEXT,
            gh_team_id     INTEGER,
            gh_team_name   TEXT,
            api_logo       TEXT         NOT NULL,
            note           TEXT,
            created_at     TIMESTAMPTZ  DEFAULT now()
        )
    """)


def upsert_mappings(cur, results: list[dict]):
    rows = [
        (
            r["live_team_id"], r["live_team_name"],
            r["gh_team_id"],   r["gh_team_name"],
            r["api_logo"],     r["match_score"],
            r["low_confidence"], datetime.now(timezone.utc),
        )
        for r in results
    ]
    psycopg2.extras.execute_values(cur, """
        INSERT INTO team_logo_mapping
            (live_team_id, live_team_name, gh_team_id, gh_team_name,
             api_logo, match_score, low_confidence, updated_at)
        VALUES %s
        ON CONFLICT (live_team_id) DO UPDATE SET
            live_team_name = EXCLUDED.live_team_name,
            gh_team_id     = EXCLUDED.gh_team_id,
            gh_team_name   = EXCLUDED.gh_team_name,
            api_logo       = EXCLUDED.api_logo,
            match_score    = EXCLUDED.match_score,
            low_confidence = EXCLUDED.low_confidence,
            updated_at     = EXCLUDED.updated_at
    """, rows)
    log.info("%d satır upsert edildi", len(rows))


def update_live_matches(cur):
    cur.execute("""
        UPDATE live_matches lm
        SET home_logo = m.api_logo
        FROM team_logo_mapping m
        WHERE lm.home_team_id  = m.live_team_id
          AND m.low_confidence = FALSE
          AND m.api_logo IS NOT NULL
          AND m.api_logo <> ''
    """)
    home_n = cur.rowcount

    cur.execute("""
        UPDATE live_matches lm
        SET away_logo = m.api_logo
        FROM team_logo_mapping m
        WHERE lm.away_team_id  = m.live_team_id
          AND m.low_confidence = FALSE
          AND m.api_logo IS NOT NULL
          AND m.api_logo <> ''
    """)
    away_n = cur.rowcount

    log.info("live_matches güncellendi — home: %d, away: %d satır", home_n, away_n)


# ── Ana akış ─────────────────────────────────────────────────────────────────


def main():
    log.info("=== team_logo_sync başladı ===")

    try:
        gh_teams = fetch_github_teams()
    except Exception as e:
        log.error("GitHub hatası: %s", e)
        sys.exit(1)

    try:
        conn = psycopg2.connect(DB_DSN)
        conn.autocommit = False
    except Exception as e:
        log.error("DB bağlantı hatası: %s", e)
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            ensure_tables(cur)
            live_teams = get_live_teams(cur)
            overrides  = get_overrides(cur)
            results    = match_teams(live_teams, gh_teams, overrides)
            upsert_mappings(cur, results)
            update_live_matches(cur)
        conn.commit()
        log.info("=== team_logo_sync tamamlandı ===")
    except Exception as e:
        conn.rollback()
        log.exception("Hata, rollback: %s", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
