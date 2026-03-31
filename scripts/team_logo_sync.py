"""
scripts/team_logo_sync.py
─────────────────────────
GitHub Actions üzerinden her gün TR 06:00'da çalışır.
Supabase REST API kullanır (psycopg2 gerekmez).

Gerekli Actions secrets:
    SUPABASE_URL  →  https://xxxxxxxxxxxx.supabase.co
    SUPABASE_KEY  →  service_role key (Settings > API)
"""

import logging
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone

import requests
from rapidfuzz import fuzz
from supabase import Client, create_client

# ── Ayarlar ───────────────────────────────────────────────────────────────────

GITHUB_URL = (
    "https://raw.githubusercontent.com/bcs562793/H2Hscrape/main/data/teams.json"
)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

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

NOISE = {
    "(k)", "(w)", "(f)", "w", "kadin", "women", "feminin",
    "ii", "iii", "iv", "reserves", "res", "b",
    "fc", "sc", "cf", "ac", "if", "bk", "sk",
    "afc", "bfc", "cfc", "sfc", "rfc",
    "club", "cp", "cd", "sd", "ud", "rc", "rcd",
    "united", "city", "athletic", "athletics",
    "football", "futbol",
}

# Key'ler normalize() sonrası karşılaştırılır (ASCII, küçük harf)
TR_TO_EN: dict[str, str] = {
    "kuzey irlanda":    "northern ireland",
    "suudi arabistan":  "saudi arabia",
    "fildisi sahili":   "ivory coast",
    "faroe adalari":    "faroe islands",
    "guney kore":       "south korea",
    "kosta rika":       "costa rica",
    "sri lanka":        "sri lanka",
    "cin halk cumhuriyeti": "china",
    "singapur":         "singapore",
    "bangledes":        "bangladesh",
    "banglades":        "bangladesh",
    "maldivler":        "maldives",
    "tayvan":           "chinese taipei",
    "malezya":          "malaysia",
    "letonya":          "latvia",
    "litvanya":         "lithuania",
    "estonya":          "estonia",
    "belcika":          "belgium",
    "norvec":           "norway",
    "polonya":          "poland",
    "danimarka":        "denmark",
    "fransa":           "france",
    "hirvatistan":      "croatia",
    "romanya":          "romania",
    "avustralya":       "australia",
    "kamerun":          "cameroon",
    "kazakistan":       "kazakhstan",
    "komorlar":         "comoros",
    "karadag":          "montenegro",
    "isvicre":          "switzerland",
    "lihtenstayn":      "liechtenstein",
    "cekya":            "czech republic",
    "arnavutluk":       "albania",
    "avusturya":        "austria",
    "finlandiya":       "finland",
    "hollanda":         "netherlands",
    "ispanya":          "spain",
    "italya":           "italy",
    "isvec":            "sweden",
    "iskocya":          "scotland",
    "japonya":          "japan",
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
    "tunus":            "tunisia",
    "misir":            "egypt",
    "nijerya":          "nigeria",
    "gana":             "ghana",
    "arjantin":         "argentina",
    "brezilya":         "brazil",
    "kolombiya":        "colombia",
    "sili":             "chile",
    "venezuela":        "venezuela",
    "venezüela":        "venezuela",
    "meksika":          "mexico",
    "kanada":           "canada",
    "jamaika":          "jamaica",
    "urdun":            "jordan",
    "suriye":           "syria",
    "lubnan":           "lebanon",
    "israil":           "israel",
    "hindistan":        "india",
    "tayland":          "thailand",
    "endonezya":        "indonesia",
    "filipinler":       "philippines",
    "ozbekistan":       "uzbekistan",
    "azerbaycan":       "azerbaijan",
    "gurcistan":        "georgia",
    "ermenistan":       "armenia",
    "kirgizistan":      "kyrgyzstan",
    "tacikistan":       "tajikistan",
    "turkmenistan":     "turkmenistan",
    "galler":           "wales",
    "ingiltere":        "england",
    "kibris":           "cyprus",
    "izlanda":          "iceland",
    "fas":              "morocco",
    "cin":              "china",
    "iran":             "iran",
    "irak":             "iraq",
    "katar":            "qatar",
    "kuveyt":           "kuwait",
}


def to_ascii(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def normalize(name: str) -> str:
    s = to_ascii(name.translate(TR_CHARS)).lower()

    for tr, en in sorted(TR_TO_EN.items(), key=lambda x: -len(x[0])):
        if s == tr or s.startswith(tr + " "):
            s = en + s[len(tr):]
            break

    s = re.sub(r"\([^)]*\)", "", s)
    s = re.sub(r"[.\-_/'\\]", " ", s)
    s = re.sub(r"\bu\s*(\d{2})\b", r"u\1", s)

    tokens = [t for t in s.split() if t not in NOISE]
    s = " ".join(tokens) if tokens else s.strip()
    return re.sub(r"\s+", " ", s).strip()


def abbreviation_score(query: str, candidate: str) -> float:
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
    if matched == len(q_tokens):
        ratio = min(1.0, ratio * 1.1)
    return round(ratio * 100, 1)


def smart_score(query_raw: str, candidate_raw: str) -> float:
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


# ── GitHub ────────────────────────────────────────────────────────────────────


def fetch_github_teams() -> list[dict]:
    log.info("GitHub teams.json çekiliyor...")
    r = requests.get(GITHUB_URL, timeout=30)
    r.raise_for_status()
    teams = r.json()
    log.info("%d takım yüklendi", len(teams))
    return teams


# ── Supabase yardımcıları ─────────────────────────────────────────────────────


def get_live_teams(sb: Client) -> list[dict]:
    """live_matches tablosundaki tüm benzersiz takımları döner."""
    # home takımlar
    home = (
        sb.table("live_matches")
        .select("home_team_id, home_team")
        .execute()
        .data
    )
    # away takımlar
    away = (
        sb.table("live_matches")
        .select("away_team_id, away_team")
        .execute()
        .data
    )

    seen = {}
    for r in home:
        tid = r["home_team_id"]
        if tid not in seen:
            seen[tid] = r["home_team"]
    for r in away:
        tid = r["away_team_id"]
        if tid not in seen:
            seen[tid] = r["away_team"]

    rows = [{"team_id": k, "team_name": v} for k, v in seen.items()]
    log.info("%d benzersiz takım bulundu", len(rows))
    return rows


def get_overrides(sb: Client) -> dict[int, dict]:
    """team_logo_override tablosundaki manuel düzeltmeleri döner."""
    try:
        data = sb.table("team_logo_override").select("*").execute().data
        return {
            r["live_team_id"]: {
                "gh_team_id":   r.get("gh_team_id"),
                "gh_team_name": r.get("gh_team_name"),
                "api_logo":     r["api_logo"],
            }
            for r in data
        }
    except Exception:
        log.warning("team_logo_override tablosu bulunamadı, atlanıyor.")
        return {}


# ── Eşleştirme ────────────────────────────────────────────────────────────────


def match_teams(
    live_teams: list[dict],
    gh_teams: list[dict],
    overrides: dict[int, dict],
) -> list[dict]:
    results  = []
    low_list = []
    now      = datetime.now(timezone.utc).isoformat()

    for lt in live_teams:
        tid = lt["team_id"]

        # 1. Manuel override
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
                "updated_at":     now,
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
                break

        low = best_score < MIN_SCORE

        results.append({
            "live_team_id":   tid,
            "live_team_name": lt["team_name"],
            "gh_team_id":     best_team["id"]               if best_team else None,
            "gh_team_name":   best_team["name"]             if best_team else None,
            "api_logo":       best_team.get("api_logo", "") if best_team else "",
            "match_score":    round(best_score, 1),
            "low_confidence": low,
            "updated_at":     now,
        })

        if low:
            live_name  = lt["team_name"]
            best_name  = best_team["name"] if best_team else "YOK"
            low_list.append(
                f"  {live_name!r:35s} → {best_name!r:30s} ({best_score:.0f})"
            )

    high = sum(1 for r in results if not r["low_confidence"])
    log.info("Eşleştirme: %d yüksek güven, %d düşük güven", high, len(results) - high)
    if low_list:
        log.warning("Düşük güven (%d) — live_matches'e yazılmadı:\n%s",
                    len(low_list), "\n".join(low_list))
    return results


# ── Supabase yazma ────────────────────────────────────────────────────────────

BATCH = 500   # Supabase upsert batch boyutu


def upsert_mappings(sb: Client, results: list[dict]):
    """team_logo_mapping tablosuna batch upsert yapar."""
    for i in range(0, len(results), BATCH):
        chunk = results[i : i + BATCH]
        sb.table("team_logo_mapping").upsert(chunk).execute()
        log.info("Upsert: %d / %d", min(i + BATCH, len(results)), len(results))


def update_live_matches(sb: Client, results: list[dict]):
    """
    Yüksek güvenli eşleşmeleri live_matches tablosuna yazar.
    home_logo ve away_logo sütunlarını günceller.
    """
    high = [r for r in results if not r["low_confidence"] and r.get("api_logo")]

    home_updated = away_updated = 0

    for r in high:
        tid  = r["live_team_id"]
        logo = r["api_logo"]

        res = (
            sb.table("live_matches")
            .update({"home_logo": logo})
            .eq("home_team_id", tid)
            .execute()
        )
        home_updated += len(res.data) if res.data else 0

        res = (
            sb.table("live_matches")
            .update({"away_logo": logo})
            .eq("away_team_id", tid)
            .execute()
        )
        away_updated += len(res.data) if res.data else 0

    log.info("live_matches güncellendi — home: %d, away: %d satır",
             home_updated, away_updated)


# ── Ana akış ─────────────────────────────────────────────────────────────────


def main():
    log.info("=== team_logo_sync başladı ===")

    # 1. GitHub'dan takımları çek
    try:
        gh_teams = fetch_github_teams()
    except Exception as e:
        log.error("GitHub hatası: %s", e)
        sys.exit(1)

    # 2. Supabase istemcisi
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        log.error("Supabase bağlantı hatası: %s", e)
        sys.exit(1)

    # 3. Live takımları al
    live_teams = get_live_teams(sb)

    # 4. Manuel override'ları al
    overrides = get_overrides(sb)

    # 5. Eşleştir
    results = match_teams(live_teams, gh_teams, overrides)

    # 6. Mapping tablosuna yaz
    upsert_mappings(sb, results)

    # 7. live_matches logolarını güncelle
    update_live_matches(sb, results)

    log.info("=== team_logo_sync tamamlandı ===")


if __name__ == "__main__":
    main()
