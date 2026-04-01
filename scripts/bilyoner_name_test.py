"""
scripts/bilyoner_name_test.py
──────────────────────────────
Bilyoner'den canlı + prematch takım isimlerini çeker,
teams.json ile karşılaştırır.

Kullanım:
    python bilyoner_name_test.py
"""

import json
import re
import unicodedata
import requests
from rapidfuzz import fuzz, process as fuzz_process

BILYONER_URL = (
    "https://www.bilyoner.com/api/v3/mobile/aggregator/gamelist/all/v1"
)
GITHUB_URL = (
    "https://raw.githubusercontent.com/bcs562793/H2Hscrape/main/data/teams.json"
)

HEADERS = {
    "accept":                   "application/json, text/plain, */*",
    "accept-language":          "tr",
    "platform-token":           "40CAB7292CD83F7EE0631FC35A0AFC75",
    "x-client-app-version":     "3.95.2",
    "x-client-browser-version": "Chrome / v146.0.0.0",
    "x-client-channel":         "WEB",
    "x-device-id":              "C1A34687-8F75-47E8-9FF9-1D231F05782E",
    "user-agent":               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
}


def fetch_bilyoner_teams() -> dict[int, str]:
    """Bilyoner'den tüm futbol takımlarını {team_id: team_name} olarak döner."""
    teams: dict[int, str] = {}
    for bulletin in (1, 2):
        params = {"tabType": 1, "bulletinType": bulletin}
        try:
            r = requests.get(BILYONER_URL, headers=HEADERS, params=params, timeout=20)
            r.raise_for_status()
            events = r.json().get("events", {})
            for ev in events.values():
                if not isinstance(ev, dict):
                    continue
                if (ev.get("st") or 0) != 1:  # sadece futbol
                    continue
                htpi = ev.get("htpi")
                atpi = ev.get("atpi")
                htn  = ev.get("htn", "")
                atn  = ev.get("atn", "")
                if htpi and htn:
                    teams[int(htpi)] = htn
                if atpi and atn:
                    teams[int(atpi)] = atn
            print(f"bulletinType={bulletin}: {len(events)} event")
        except Exception as e:
            print(f"⚠️  bulletinType={bulletin} hatası: {e}")
    return teams


def fetch_github_teams() -> list[dict]:
    r = requests.get(GITHUB_URL, timeout=20)
    r.raise_for_status()
    return r.json()


def normalize(name: str) -> str:
    TR = str.maketrans("şŞğĞüÜöÖçÇıİ", "sSgGuUoOcCiI")
    s = unicodedata.normalize("NFKD", name.translate(TR)).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[.\-_/'\\()]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def main():
    print("Bilyoner takımları çekiliyor...")
    bilyoner_teams = fetch_bilyoner_teams()
    print(f"→ {len(bilyoner_teams)} benzersiz takım\n")

    print("teams.json çekiliyor...")
    gh_teams = fetch_github_teams()
    gh_names = [t["name"] for t in gh_teams]
    gh_norm  = [normalize(n) for n in gh_names]
    print(f"→ {len(gh_teams)} takım\n")

    print("=" * 80)
    print(f"{'Bilyoner İsim':40s}  {'En iyi eşleşme (teams.json)':35s}  Skor")
    print("=" * 80)

    exact  = 0
    good   = 0
    bad    = 0

    rows = []
    for tid, bname in sorted(bilyoner_teams.items(), key=lambda x: x[1]):
        bnorm = normalize(bname)
        match = fuzz_process.extractOne(bnorm, gh_norm, scorer=fuzz.token_sort_ratio)
        if not match:
            rows.append((bname, "YOK", 0))
            bad += 1
            continue
        mname, score, idx = match
        gh_original = gh_names[idx]

        if score == 100:
            exact += 1
        elif score >= 82:
            good += 1
        else:
            bad += 1

        rows.append((bname, gh_original, score))

    # Önce kötü olanları göster
    rows.sort(key=lambda x: x[2])
    for bname, gname, score in rows:
        flag = "✅" if score == 100 else ("🟡" if score >= 82 else "❌")
        print(f"{flag} {bname:40s}  {gname:35s}  {score}")

    print("=" * 80)
    print(f"✅ Tam eşleşme : {exact}")
    print(f"🟡 Yakın (≥82) : {good}")
    print(f"❌ Düşük (<82) : {bad}")
    print(f"\nDüzeltilmesi gereken teams.json kaydı: {bad}")


if __name__ == "__main__":
    main()
