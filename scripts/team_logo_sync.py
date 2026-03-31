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
from rapidfuzz import fuzz, process as fuzz_process
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
    """
    Hız optimizasyonu:
    - GitHub takım adları bir kez normalize edilir ve önbelleğe alınır.
    - rapidfuzz.process.extractOne ile C-hızında token_sort_ratio taraması yapılır.
    - Ardından abbreviation_score ile kısaltma kontrolü eklenir.
    - Böylece 101 × 14069 döngüsü yerine ~50ms'de biter.
    """
    # Normalize edilmiş GitHub adlarını önbelleğe al
    gh_norm_names = [normalize(t["name"]) for t in gh_teams]
    gh_by_norm    = {normalize(t["name"]): t for t in gh_teams}

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

        # 2. Normalize edilmiş sorgu
        q_norm = normalize(lt["team_name"])

        # 3. rapidfuzz ile hızlı token_sort_ratio taraması (C katmanı, ~ms)
        fast_match = fuzz_process.extractOne(
            q_norm,
            gh_norm_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=0,
        )
        fast_name, fast_score, _ = fast_match

        # 4. token_set_ratio ve partial_ratio ile cross-check
        set_score     = fuzz.token_set_ratio(q_norm, fast_name)
        partial_score = fuzz.partial_ratio(q_norm, fast_name)
        abbr_sc       = abbreviation_score(q_norm, fast_name)
        best_score    = max(fast_score, set_score, partial_score, abbr_sc)

        # 5. Eğer skor düşükse abbreviation ile tüm listeyi tara (sadece düşük skorlularda)
        if best_score < MIN_SCORE:
            for gn in gh_norm_names:
                s = abbreviation_score(q_norm, gn)
                if s > best_score:
                    best_score = s
                    fast_name  = gn

        best_team = gh_by_norm.get(fast_name)
        low       = best_score < MIN_SCORE

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
            live_name = lt["team_name"]
            best_name = best_team["name"] if best_team else "YOK"
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
        try:
            sb.table("team_logo_mapping").upsert(chunk).execute()
            log.info("Upsert: %d / %d", min(i + BATCH, len(results)), len(results))
        except Exception as e:
            msg = str(e)
            if "PGRST205" in msg or "schema cache" in msg:
                log.error(
                    "team_logo_mapping tablosu bulunamadi!\n"
                    "Supabase SQL Editor'da scripts/setup.sql dosyasini bir kez calistir:\n"
                    "https://supabase.com/dashboard/project/_/sql/new"
                )
                sys.exit(1)
            raise


def update_live_matches(sb: Client):
    """
    team_logo_mapping tablosunu JOIN'leyerek live_matches logolarını
    tek bir SQL fonksiyonu çağrısıyla günceller.
    setup.sql'deki sync_live_match_logos() fonksiyonunu kullanır.
    """
    sb.rpc("sync_live_match_logos", {}).execute()
    log.info("live_matches logoları güncellendi (sync_live_match_logos RPC)")


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
    update_live_matches(sb)

    log.info("=== team_logo_sync tamamlandı ===")


if __name__ == "__main__":
    main()
