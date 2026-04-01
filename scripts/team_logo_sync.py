"""
scripts/team_logo_sync.py  (v5 — isim + ülke bazlı eşleştirme)
───────────────────────────────────────────────────────────────
Nasıl çalışır:
  1. teams.json → {(norm_name, country): api_logo} haritası
  2. live_matches → (team_id, team_name, league_country) listesi
  3. Eşleştirme:
       a. Tam isim + ülke eşleşmesi          → skor 100
       b. Tam isim, ülke boş/bilinmiyor       → skor 95
       c. Fuzzy isim (≥90) + ülke eşleşmesi   → skor fuzzy
       d. Fuzzy isim (≥90), ülke yok          → skor fuzzy - 5 ceza
  4. team_logo_mapping'e upsert
  5. sync_live_match_logos + sync_future_match_logos RPC

Gerekli Actions secrets: SUPABASE_URL, SUPABASE_KEY
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

GITHUB_URL   = "https://raw.githubusercontent.com/bcs562793/H2Hscrape/main/data/teams.json"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

MIN_SCORE = 85   # Ülke filtresi yanlış eşleşmeleri azalttığı için 82'den 85'e çıktı

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Normalizasyon ─────────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    TR = str.maketrans("şŞğĞüÜöÖçÇıİ", "sSgGuUoOcCiI")
    s = unicodedata.normalize("NFKD", name.translate(TR)).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[.\-_/'\\()]", " ", s)
    # fc, sc, ac gibi prefix'leri sil
    tokens = [t for t in s.split() if t not in {
        "fc","sc","cf","ac","if","bk","sk","afc","bfc","cfc","sfc","rfc",
        "cp","cd","sd","ud","rc","rcd",
    }]
    s = " ".join(tokens) if tokens else s.strip()
    return re.sub(r"\s+", " ", s).strip()


# ── Lig adından ülke çıkarma (live_matches.league_name Türkçe) ────────────────

_LG_COUNTRY_MAP = {
    "almanya":"Germany","ispanya":"Spain","italya":"Italy","fransa":"France",
    "hollanda":"Netherlands","portekiz":"Portugal","brezilya":"Brazil",
    "arjantin":"Argentina","turkiye":"Turkey","turk":"Turkey",
    "belcika":"Belgium","isvicre":"Switzerland","avustralya":"Australia",
    "japonya":"Japan","danimarka":"Denmark","norvec":"Norway","isvec":"Sweden",
    "finlandiya":"Finland","polonya":"Poland","hirvatistan":"Croatia",
    "slovenya":"Slovenia","slovakya":"Slovakia","cekya":"Czech Republic",
    "macaristan":"Hungary","romanya":"Romania","bulgaristan":"Bulgaria",
    "sirbistan":"Serbia","yunanistan":"Greece","avusturya":"Austria",
    "iskocya":"Scotland","ingiltere":"England","galler":"Wales",
    "kolombiya":"Colombia","meksika":"Mexico","sili":"Chile","misir":"Egypt",
    "fas":"Morocco","cezayir":"Algeria","nijerya":"Nigeria","gana":"Ghana",
    "abd":"USA","kanada":"Canada","arnavutluk":"Albania","karadag":"Montenegro",
    "letonya":"Latvia","litvanya":"Lithuania","estonya":"Estonia",
    "ukrayna":"Ukraine","rusya":"Russia","azerbaycan":"Azerbaijan",
    "gurcistan":"Georgia","ermenistan":"Armenia","honduras":"Honduras",
    "guatemala":"Guatemala","panama":"Panama","paraguay":"Paraguay",
    "uruguay":"Uruguay","bolivya":"Bolivia","peru":"Peru","ekvador":"Ecuador",
    "tanzanya":"Tanzania","kenya":"Kenya","tunus":"Tunisia","irak":"Iraq",
    "suriye":"Syria","iran":"Iran","katar":"Qatar","hindistan":"India",
    "cin":"China","endonezya":"Indonesia","tayland":"Thailand",
    "malezya":"Malaysia","izlanda":"Iceland","kibris":"Cyprus",
    "israil":"Israel","kazakistan":"Kazakhstan","ozbekistan":"Uzbekistan",
    # İki kelimeli — birleşik anahtar
    "guney_afrika":"South Africa","kuzey_irlanda":"Northern Ireland",
    "kosta_rika":"Costa Rica","el_salvador":"El Salvador",
    "suudi_arabistan":"Saudi Arabia","faroe_adalari":"Faroe Islands",
    "guney_kore":"South Korea","yeni_zelanda":"New Zealand",
}

def _norm_for_country(s: str) -> str:
    TR = str.maketrans("şŞğĞüÜöÖçÇıİ", "sSgGuUoOcCiI")
    return unicodedata.normalize("NFKD", s.translate(TR)).encode("ascii","ignore").decode().lower()

def extract_country(league_name: str) -> str:
    if not league_name:
        return ""
    words = league_name.strip().split()
    if not words:
        return ""
    # İki kelimeli deneme
    if len(words) >= 2:
        key2 = f"{_norm_for_country(words[0])}_{_norm_for_country(words[1])}"
        if key2 in _LG_COUNTRY_MAP:
            return _LG_COUNTRY_MAP[key2]
    # Tek kelime
    return _LG_COUNTRY_MAP.get(_norm_for_country(words[0]), "")


# ── GitHub ────────────────────────────────────────────────────────────────────

def fetch_teams() -> list[dict]:
    log.info("teams.json çekiliyor...")
    r = requests.get(GITHUB_URL, timeout=30)
    r.raise_for_status()
    teams = r.json()
    log.info("%d takım yüklendi", len(teams))
    return teams

def build_logo_index(teams: list[dict]) -> tuple[dict, list, list]:
    """
    Üç yapı döner:
      exact_map : {(norm_name, country_lower): api_logo}  — tam eşleşme için
      norm_names: normalize isim listesi (fuzzy için)
      team_list : orijinal team dict listesi (norm_names ile aynı sıra)
    """
    exact_map  : dict[tuple, str] = {}
    norm_names : list[str]        = []
    team_list  : list[dict]       = []

    for t in teams:
        name    = t.get("name", "")
        country = (t.get("country") or "").strip()
        logo    = t.get("api_logo", "")
        if not name or not logo:
            continue
        norm = normalize(name)
        exact_map[(norm, country.lower())] = logo
        exact_map[(norm, "")]              = logo   # ülkesiz fallback
        norm_names.append(norm)
        team_list.append(t)

    log.info("Index: %d kayıt", len(team_list))
    return exact_map, norm_names, team_list


# ── Supabase ──────────────────────────────────────────────────────────────────

def get_live_teams(sb: Client) -> list[dict]:
    """live_matches'ten benzersiz takımları ve lig adını döner."""
    home = sb.table("live_matches").select("home_team_id, home_team, league_name").execute().data
    away = sb.table("live_matches").select("away_team_id, away_team, league_name").execute().data

    seen: dict[int, dict] = {}
    for r in home:
        tid = r["home_team_id"]
        if tid and tid not in seen:
            seen[tid] = {
                "team_id":    tid,
                "team_name":  r["home_team"],
                "league_name": r.get("league_name", ""),
            }
    for r in away:
        tid = r["away_team_id"]
        if tid and tid not in seen:
            seen[tid] = {
                "team_id":    tid,
                "team_name":  r["away_team"],
                "league_name": r.get("league_name", ""),
            }
    rows = list(seen.values())
    log.info("%d benzersiz takım", len(rows))
    return rows


def get_overrides(sb: Client) -> dict[int, dict]:
    try:
        data = sb.table("team_logo_override").select("*").execute().data
        return {
            r["live_team_id"]: {
                "gh_team_name": r.get("gh_team_name"),
                "api_logo":     r["api_logo"],
            }
            for r in data
        }
    except Exception:
        log.warning("team_logo_override bulunamadı, atlanıyor.")
        return {}


# ── Eşleştirme ────────────────────────────────────────────────────────────────

def build_mappings(
    live_teams: list[dict],
    exact_map:  dict,
    norm_names: list[str],
    team_list:  list[dict],
    overrides:  dict[int, dict],
) -> list[dict]:
    now      = datetime.now(timezone.utc).isoformat()
    results  = []
    no_match = []

    for lt in live_teams:
        tid        = lt["team_id"]
        name       = lt["team_name"] or ""
        league_name = lt.get("league_name", "") or ""
        country    = extract_country(league_name)   # "Germany", "Spain" vb.

        # 1. Manuel override
        if tid in overrides:
            ov = overrides[tid]
            results.append({
                "live_team_id":   tid,
                "live_team_name": name,
                "gh_team_name":   ov["gh_team_name"],
                "api_logo":       ov["api_logo"],
                "match_score":    100.0,
                "low_confidence": False,
                "updated_at":     now,
            })
            continue

        q_norm = normalize(name)

        # 2. Tam isim + ülke eşleşmesi
        logo = exact_map.get((q_norm, country.lower()), "")
        if logo:
            results.append({
                "live_team_id":   tid,
                "live_team_name": name,
                "gh_team_name":   name,
                "api_logo":       logo,
                "match_score":    100.0,
                "low_confidence": False,
                "updated_at":     now,
            })
            continue

        # 3. Tam isim, ülke bilinmiyor
        logo = exact_map.get((q_norm, ""), "")
        if logo and not country:
            results.append({
                "live_team_id":   tid,
                "live_team_name": name,
                "gh_team_name":   name,
                "api_logo":       logo,
                "match_score":    95.0,
                "low_confidence": False,
                "updated_at":     now,
            })
            continue

        # 4. Fuzzy eşleşme
        match = fuzz_process.extractOne(
            q_norm, norm_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=0,
        )
        if match:
            _, score, idx = match
            best_team    = team_list[idx]
            best_country = (best_team.get("country") or "").strip()
            best_logo    = best_team.get("api_logo", "")
            best_name    = best_team.get("name", "")

            # Ülke eşleşmesi varsa bonus, yoksa ceza
            if country and best_country:
                country_ok = country.lower() == best_country.lower()
                if not country_ok:
                    score -= 15   # Ülke farklı → büyük ceza
            
            if score >= MIN_SCORE and best_logo:
                results.append({
                    "live_team_id":   tid,
                    "live_team_name": name,
                    "gh_team_name":   best_name,
                    "api_logo":       best_logo,
                    "match_score":    round(score, 1),
                    "low_confidence": False,
                    "updated_at":     now,
                })
                if score < 95:
                    log.info("  🟡 %s → %s (%s) (%.0f)", name, best_name, country, score)
                continue

        # Eşleşme yok
        results.append({
            "live_team_id":   tid,
            "live_team_name": name,
            "gh_team_name":   None,
            "api_logo":       "",
            "match_score":    0.0,
            "low_confidence": True,
            "updated_at":     now,
        })
        no_match.append(f"  {name!r:35s} (ülke: {country or '?'})")

    matched = sum(1 for r in results if not r["low_confidence"])
    log.info("Eşleştirme: %d tam/yakın, %d eşleşmedi", matched, len(no_match))
    if no_match:
        log.warning("Eşleşmeyen %d takım:\n%s", len(no_match), "\n".join(sorted(no_match)))
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
            if "PGRST205" in str(e) or "schema cache" in str(e):
                log.error("team_logo_mapping tablosu bulunamadı!")
                sys.exit(1)
            raise

def update_logos(sb: Client) -> None:
    sb.rpc("sync_live_match_logos", {}).execute()
    log.info("live_matches logoları güncellendi")
    sb.rpc("sync_future_match_logos", {}).execute()
    log.info("future_matches.data logoları güncellendi")


# ── Ana akış ─────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== team_logo_sync başladı (isim + ülke) ===")

    try:
        teams = fetch_teams()
    except Exception as e:
        log.error("GitHub hatası: %s", e)
        sys.exit(1)

    exact_map, norm_names, team_list = build_logo_index(teams)

    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        log.error("Supabase bağlantı hatası: %s", e)
        sys.exit(1)

    live_teams = get_live_teams(sb)
    overrides  = get_overrides(sb)
    results    = build_mappings(live_teams, exact_map, norm_names, team_list, overrides)
    upsert_mappings(sb, results)
    update_logos(sb)

    log.info("=== team_logo_sync tamamlandı ===")


if __name__ == "__main__":
    main()
