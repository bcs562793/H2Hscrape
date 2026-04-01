"""
scripts/team_logo_sync.py
─────────────────────────
Başlangıçta otomatik tanı (diagnose) çalıştırır:
  • teams.json alan adlarını doğrular
  • NOISE çakışmalarını loglar
  • Sorun varsa erken çıkış yapar

Gerekli Actions secrets:
    SUPABASE_URL  →  https://xxxxxxxxxxxx.supabase.co
    SUPABASE_KEY  →  service_role key (Settings > API)
"""

import logging
import os
import re
import sys
import unicodedata
from collections import defaultdict
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

MIN_SCORE = 82

# teams.json alan adları — tanı aşamasında otomatik algılanır, burada fallback
GH_NAME_FIELD = "name"
GH_LOGO_FIELD = "api_logo"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Normalizasyon ─────────────────────────────────────────────────────────────

TR_CHARS = str.maketrans("şŞğĞüÜöÖçÇıİ", "sSgGuUoOcCiI")

# Sadece gerçek prefix gürültüsü — takım adının parçası olan kelimeler YOK
NOISE = {
    "(k)", "(w)", "(f)", "kadin", "women", "womens", "feminin",
    "reserves", "res",
    "ii", "iii", "iv",
    "fc", "sc", "cf", "ac", "if", "bk", "sk",
    "afc", "bfc", "cfc", "sfc", "rfc",
    "cp", "cd", "sd", "ud", "rc", "rcd",
}

TR_TO_EN: dict[str, str] = {
    "kuzey irlanda":        "northern ireland",
    "suudi arabistan":      "saudi arabia",
    "fildisi sahili":       "ivory coast",
    "faroe adalari":        "faroe islands",
    "guney kore":           "south korea",
    "kosta rika":           "costa rica",
    "sri lanka":            "sri lanka",
    "cin halk cumhuriyeti": "china",
    "singapur":             "singapore",
    "bangledes":            "bangladesh",
    "banglades":            "bangladesh",
    "maldivler":            "maldives",
    "tayvan":               "chinese taipei",
    "malezya":              "malaysia",
    "letonya":              "latvia",
    "litvanya":             "lithuania",
    "estonya":              "estonia",
    "belcika":              "belgium",
    "norvec":               "norway",
    "polonya":              "poland",
    "danimarka":            "denmark",
    "fransa":               "france",
    "hirvatistan":          "croatia",
    "romanya":              "romania",
    "avustralya":           "australia",
    "kamerun":              "cameroon",
    "kazakistan":           "kazakhstan",
    "komorlar":             "comoros",
    "karadag":              "montenegro",
    "isvicre":              "switzerland",
    "lihtenstayn":          "liechtenstein",
    "cekya":                "czech republic",
    "arnavutluk":           "albania",
    "avusturya":            "austria",
    "finlandiya":           "finland",
    "hollanda":             "netherlands",
    "ispanya":              "spain",
    "italya":               "italy",
    "isvec":                "sweden",
    "iskocya":              "scotland",
    "japonya":              "japan",
    "portekiz":             "portugal",
    "yunanistan":           "greece",
    "almanya":              "germany",
    "rusya":                "russia",
    "ukrayna":              "ukraine",
    "macaristan":           "hungary",
    "slovakya":             "slovakia",
    "slovenya":             "slovenia",
    "sirbistan":            "serbia",
    "bulgaristan":          "bulgaria",
    "cezayir":              "algeria",
    "tunus":                "tunisia",
    "misir":                "egypt",
    "nijerya":              "nigeria",
    "gana":                 "ghana",
    "arjantin":             "argentina",
    "brezilya":             "brazil",
    "kolombiya":            "colombia",
    "sili":                 "chile",
    "venezuela":            "venezuela",
    "venezüela":            "venezuela",
    "meksika":              "mexico",
    "kanada":               "canada",
    "jamaika":              "jamaica",
    "urdun":                "jordan",
    "suriye":               "syria",
    "lubnan":               "lebanon",
    "israil":               "israel",
    "hindistan":            "india",
    "tayland":              "thailand",
    "endonezya":            "indonesia",
    "filipinler":           "philippines",
    "ozbekistan":           "uzbekistan",
    "azerbaycan":           "azerbaijan",
    "gurcistan":            "georgia",
    "ermenistan":           "armenia",
    "kirgizistan":          "kyrgyzstan",
    "tacikistan":           "tajikistan",
    "turkmenistan":         "turkmenistan",
    "galler":               "wales",
    "ingiltere":            "england",
    "kibris":               "cyprus",
    "izlanda":              "iceland",
    "fas":                  "morocco",
    "turkiye":              "turkey",
    "turk":                 "turkey",
    "cin":                  "china",
    "iran":                 "iran",
    "irak":                 "iraq",
    "katar":                "qatar",
    "kuveyt":               "kuwait",
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


def token_length_ratio(q: str, c: str) -> float:
    lq, lc = len(q.split()), len(c.split())
    if lq == 0 or lc == 0:
        return 0.0
    return min(lq, lc) / max(lq, lc)


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


# ── TANI (Diagnose) ───────────────────────────────────────────────────────────

def diagnose(gh_teams: list[dict]) -> None:
    """
    teams.json yapısını ve NOISE çakışmalarını kontrol eder.
    Kritik sorun bulursa sys.exit(1) ile durur.
    """
    log.info("── Tanı başlıyor ──")

    if not gh_teams:
        log.error("TANI: teams.json boş geldi, devam edilemiyor.")
        sys.exit(1)

    # ── 1. Alan adlarını algıla ve global'e yaz ───────────────────────────────
    global GH_NAME_FIELD, GH_LOGO_FIELD

    sample = gh_teams[0]
    log.info("TANI: İlk kayıt alanları → %s", list(sample.keys()))

    name_candidates = ("name", "team_name", "title", "Name", "TeamName")
    logo_candidates = ("api_logo", "logo", "logo_url", "image", "crest")

    detected_name = next((f for f in name_candidates if f in sample), None)
    detected_logo = next((f for f in logo_candidates if f in sample), None)

    if not detected_name:
        log.error(
            "TANI: İsim alanı bulunamadı! Mevcut alanlar: %s  "
            "— GH_NAME_FIELD değerini manuel güncelle.",
            list(sample.keys()),
        )
        sys.exit(1)

    if not detected_logo:
        log.warning(
            "TANI: Logo alanı bulunamadı, GH_LOGO_FIELD='%s' fallback kullanılıyor. "
            "Mevcut alanlar: %s",
            GH_LOGO_FIELD, list(sample.keys()),
        )
    else:
        GH_NAME_FIELD = detected_name
        GH_LOGO_FIELD = detected_logo
        log.info("TANI: Alan adları → isim='%s'  logo='%s'", GH_NAME_FIELD, GH_LOGO_FIELD)

    # ── 2. Normalize çakışmalarını bul ────────────────────────────────────────
    collision_map: dict[str, list[str]] = defaultdict(list)
    empty_names: list[str] = []

    for t in gh_teams:
        raw = t.get(GH_NAME_FIELD, "")
        norm = normalize(raw)
        if not norm:
            empty_names.append(raw)
        else:
            collision_map[norm].append(raw)

    collisions = {k: v for k, v in collision_map.items() if len(v) > 1}

    if empty_names:
        log.warning("TANI: Normalize sonrası BOŞ kalan %d takım: %s",
                    len(empty_names), empty_names)

    if collisions:
        lines = "\n".join(
            f"  '{norm}' ← {names}" for norm, names in sorted(collisions.items())
        )
        log.warning(
            "TANI: %d normalize çakışması — bu takımlarda yanlış logo riski var:\n%s",
            len(collisions), lines,
        )
    else:
        log.info("TANI: Normalize çakışması yok ✓")

    log.info("── Tanı tamamlandı ──")


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
    home = sb.table("live_matches").select("home_team_id, home_team").execute().data
    away = sb.table("live_matches").select("away_team_id, away_team").execute().data
    seen: dict = {}
    for r in home:
        if r["home_team_id"] not in seen:
            seen[r["home_team_id"]] = r["home_team"]
    for r in away:
        if r["away_team_id"] not in seen:
            seen[r["away_team_id"]] = r["away_team"]
    rows = [{"team_id": k, "team_name": v} for k, v in seen.items()]
    log.info("%d benzersiz takım bulundu", len(rows))
    return rows


def get_overrides(sb: Client) -> dict[int, dict]:
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


# ── Index ─────────────────────────────────────────────────────────────────────

def build_gh_index(gh_teams: list[dict]) -> tuple[list[str], dict[str, dict]]:
    gh_norm_names: list[str] = []
    gh_by_norm: dict[str, dict] = {}
    for t in gh_teams:
        raw  = t.get(GH_NAME_FIELD, "")
        norm = normalize(raw)
        if not norm:
            continue
        if norm in gh_by_norm:
            # Çakışma zaten diagnose'da loglandı, sessizce atla
            continue
        gh_norm_names.append(norm)
        gh_by_norm[norm] = t
    return gh_norm_names, gh_by_norm


# ── Eşleştirme ────────────────────────────────────────────────────────────────

def match_teams(
    live_teams: list[dict],
    gh_teams: list[dict],
    overrides: dict[int, dict],
) -> list[dict]:

    gh_norm_names, gh_by_norm = build_gh_index(gh_teams)
    results: list[dict] = []
    low_list: list[str] = []
    now = datetime.now(timezone.utc).isoformat()

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

        q_norm = normalize(lt["team_name"])

        # 2. Hızlı token_sort_ratio taraması (partial_ratio YOK)
        fast_name, fast_score, _ = fuzz_process.extractOne(
            q_norm, gh_norm_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=0,
        )

        # 3. Ek skorlar
        lp         = token_length_ratio(q_norm, fast_name)
        set_score  = fuzz.token_set_ratio(q_norm, fast_name) * (0.4 + 0.6 * lp)
        abbr_sc    = abbreviation_score(q_norm, fast_name)
        best_score = max(fast_score, set_score, abbr_sc)
        best_name  = fast_name

        # 4. Skor düşükse kısaltma ile tam tarama
        if best_score < MIN_SCORE:
            for gn in gh_norm_names:
                s = abbreviation_score(q_norm, gn)
                if s > best_score:
                    best_score = s
                    best_name  = gn

        best_team = gh_by_norm.get(best_name)
        low       = best_score < MIN_SCORE

        results.append({
            "live_team_id":   tid,
            "live_team_name": lt["team_name"],
            "gh_team_id":     best_team.get("id")              if best_team else None,
            "gh_team_name":   best_team.get(GH_NAME_FIELD)     if best_team else None,
            "api_logo":       best_team.get(GH_LOGO_FIELD, "") if best_team else "",
            "match_score":    round(best_score, 1),
            "low_confidence": low,
            "updated_at":     now,
        })

        if low:
            best_disp = best_team.get(GH_NAME_FIELD) if best_team else "YOK"
            low_list.append(
                f"  {lt['team_name']!r:35s} → {best_disp!r:30s} ({best_score:.0f})"
            )

    high = sum(1 for r in results if not r["low_confidence"])
    log.info("Eşleştirme: %d yüksek güven, %d düşük güven", high, len(results) - high)
    if low_list:
        log.warning("Düşük güven (%d):\n%s", len(low_list), "\n".join(low_list))
    return results


# ── Supabase yazma ────────────────────────────────────────────────────────────

BATCH = 500


def upsert_mappings(sb: Client, results: list[dict]) -> None:
    for i in range(0, len(results), BATCH):
        chunk = results[i : i + BATCH]
        try:
            sb.table("team_logo_mapping").upsert(chunk).execute()
            log.info("Upsert: %d / %d", min(i + BATCH, len(results)), len(results))
        except Exception as e:
            msg = str(e)
            if "PGRST205" in msg or "schema cache" in msg:
                log.error("team_logo_mapping tablosu bulunamadi! setup.sql'i çalıştır.")
                sys.exit(1)
            raise


def update_logos(sb: Client) -> None:
    sb.rpc("sync_live_match_logos", {}).execute()
    log.info("live_matches logoları güncellendi")
    sb.rpc("sync_future_match_logos", {}).execute()
    log.info("future_matches.data logoları güncellendi")


# ── Ana akış ─────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== team_logo_sync başladı ===")

    # 1. GitHub'dan çek
    try:
        gh_teams = fetch_github_teams()
    except Exception as e:
        log.error("GitHub hatası: %s", e)
        sys.exit(1)

    # 2. Tanı — alan adlarını doğrula, çakışmaları logla
    diagnose(gh_teams)

    # 3. Supabase bağlantısı
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        log.error("Supabase bağlantı hatası: %s", e)
        sys.exit(1)

    # 4. Live takımlar + override'lar
    live_teams = get_live_teams(sb)
    overrides  = get_overrides(sb)

    # 5. Eşleştir
    results = match_teams(live_teams, gh_teams, overrides)

    # 6. Yaz
    upsert_mappings(sb, results)
    update_logos(sb)

    log.info("=== team_logo_sync tamamlandı ===")


if __name__ == "__main__":
    main()
