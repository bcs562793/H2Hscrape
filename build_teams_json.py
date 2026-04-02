#!/usr/bin/env python3
"""
build_combined_teams.py
───────────────────────
teams.json (api-sports, 14 k takım)  +  teams_new.json (mackolik, 9.6 k takım)
→ combined_teams.json

Her kayıt:
  n  – canonical takım adı
  c  – ülke
  l  – en iyi logo (api-sports.io ≥ mackolik cdn)
  m  – mackolik logo (boş olabilir)

Çalıştır:
  python3 build_combined_teams.py teams.json teams_new.json combined_teams.json
"""

import json, re, os, sys
from collections import defaultdict

# ── Normalize ─────────────────────────────────────────────────────────────────

# Takım adlarında anlamsız kısaltmalar (FC, SC, …)
# "rb" kasıtlı dahil EDİLMEDİ → "RB Leipzig" / "RB Bragantino" ayrıştırılsın diye
NOISE = {
    'fc','sc','cf','ac','bk','sk','fk','afc','bfc','cfc','sfc','rfc',
    'cp','cd','sd','ud','rc','rcd','ss','svv','sv','rv','mv','nv','bv',
    'ov','dv','jv','lv','hsv','bsc','ik','il','is','vv','bss',
}

TR_MAP = [
    ('ş','s'),('ğ','g'),('ü','u'),('ö','o'),('ç','c'),('ı','i'),('İ','i'),
    ('é','e'),('è','e'),('ê','e'),('ë','e'),
    ('á','a'),('à','a'),('â','a'),('ã','a'),('ä','a'),('å','a'),
    ('ó','o'),('ò','o'),('ô','o'),('õ','o'),('ø','o'),
    ('ú','u'),('ù','u'),('û','u'),
    ('í','i'),('ì','i'),('î','i'),
    ('ñ','n'),('ć','c'),('č','c'),('ž','z'),('š','s'),('ý','y'),
    ('ř','r'),('ß','ss'),('ł','l'),('ę','e'),('ą','a'),('ń','n'),
    ('ź','z'),('ż','z'),('ő','o'),('ű','u'),('ě','e'),('ț','t'),('ș','s'),
]

def norm(s: str) -> str:
    s = s.lower().strip()
    for src, dst in TR_MAP:
        s = s.replace(src, dst)
    s = re.sub(r"[.\-_/'\\()\[\]+&]", ' ', s)
    tokens = [t for t in s.split() if t and t not in NOISE]
    return ' '.join(tokens).strip()


# ── Yardımcı: api-sports eşleştirme ──────────────────────────────────────────

def build_api_index(teams_api):
    idx = defaultdict(list)
    for t in teams_api:
        idx[norm(t['name'])].append(t)
    return idx


def best_api(name: str, prefer_country: str, api_idx: dict):
    """
    'name' için api-sports tablosunda en iyi eşleşmeyi döner.
    Strateji:
      1. Tam isim eşleşmesi
      2. Suffix drop  : "Charlton Athletic" → "charlton athletic" → drop "athletic" → "charlton"
      3. Prefix token : "Bragantino" → candidates with "bragantino" token
    Ülke eşleşmesi varsa öncelik verilir.
    """
    n = norm(name)
    candidates = api_idx.get(n, [])

    if not candidates:
        tokens = n.split()
        # Suffix drop  (1..2 kelime)
        for drop in range(1, min(3, len(tokens))):
            partial = ' '.join(tokens[:-drop])
            if len(partial) >= 4 and partial in api_idx:
                candidates = api_idx[partial]
                break

    if not candidates:
        # Prefix: herhangi bir uzun token içinde geçiyor mu?
        tokens = n.split()
        for subtoken in tokens:
            if len(subtoken) >= 6:
                for api_n, api_list in api_idx.items():
                    if subtoken in api_n.split():   # token tam eşleşmesi
                        candidates = api_list
                        break
            if candidates:
                break

    if not candidates:
        return None, ''

    pc = prefer_country.lower()
    for c in candidates:
        if (c.get('country') or '').lower() == pc:
            return c['api_logo'], c.get('country', '')
    return candidates[0]['api_logo'], candidates[0].get('country', '')


# ── Ana işlev ─────────────────────────────────────────────────────────────────

def build_combined(teams_api_path: str, teams_mk_path: str, out_path: str):
    with open(teams_api_path, encoding='utf-8') as f:
        teams_api = json.load(f)
    with open(teams_mk_path, encoding='utf-8') as f:
        teams_mk  = json.load(f)

    api_idx = build_api_index(teams_api)

    # (norm_name, country_lower) → entry dict
    store: dict[tuple, dict] = {}

    def add(name, country, logo_api, logo_mk=''):
        n = norm(name)
        if not n or not (logo_api or logo_mk):
            return
        c = (country or '').strip()
        key = (n, c.lower())
        if key not in store:
            store[key] = {
                'n': name,
                'c': c,
                'l': logo_api or logo_mk,
                'm': logo_mk,
            }
        else:
            e = store[key]
            # api-sports logosu daha iyiyse güncelle
            if logo_api and 'api-sports.io' in logo_api and 'api-sports.io' not in e['l']:
                e['l'] = logo_api
            # mackolik logo ekle
            if logo_mk and not e['m']:
                e['m'] = logo_mk

    # ── 1. Tüm api-sports takımlarını ekle ────────────────────────────────────
    for t in teams_api:
        add(t['name'], t.get('country',''), t['api_logo'])

    # ── 2. teams_new (mackolik) → bağla veya yeni ekle ───────────────────────
    linked = 0; added = 0

    for mk in teams_mk:
        mk_name   = mk['name']
        mk_logo   = mk['api_logo']    # mackolik CDN URL

        api_logo, api_country = best_api(mk_name, '', api_idx)

        if api_logo:
            # teams_new adını da ayrı bir giriş olarak ekle (alias etkisi)
            add(mk_name, api_country, api_logo, mk_logo)
            # Orijinal api-sports girişine de mackolik logoyu bağla
            n_mk = norm(mk_name)
            for cnd in api_idx.get(n_mk, []):
                ck = (norm(cnd['name']), (cnd.get('country') or '').lower())
                if ck in store and not store[ck]['m']:
                    store[ck]['m'] = mk_logo
            linked += 1
        else:
            # Sadece mackolik'te olan takım
            add(mk_name, '', mk_logo, mk_logo)
            added += 1

    result = list(store.values())

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))

    size = os.path.getsize(out_path)
    print(f'✅ {out_path}')
    print(f'   Toplam giriş    : {len(result):,}')
    print(f'   api-sports base : {len(teams_api):,}')
    print(f'   mackolik bağlı  : {linked:,} / {len(teams_mk):,}')
    print(f'   mackolik yeni   : {added:,}')
    print(f'   Dosya boyutu    : {size//1024:,} KB')

    # Hızlı doğrulama
    ni = defaultdict(list)
    for e in result:
        ni[norm(e['n'])].append(e)

    tests = ['Charlton Athletic','Leicester City','Flamengo','Bragantino',
             'RB Bragantino','Galatasaray','Vendsyssel','Al-Jandal',
             'Gençlerbirliği','Fenerbahçe','Preston North End','Real Madrid']
    print('\nDoğrulama:')
    for name in tests:
        found = ni.get(norm(name), [])
        if found:
            e = found[0]
            src = 'api✅' if 'api-sports.io' in e['l'] else 'mk⚠️'
            print(f'  ✅ {name:<25} → {e["n"]} ({e["c"]}) [{src}]')
        else:
            print(f'  ❌ {name:<25} → BULUNAMADI')


if __name__ == '__main__':
    if len(sys.argv) == 4:
        build_combined(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        # Varsayılan yollar
        build_combined('teams.json', 'teams_new.json', 'combined_teams.json')
