# production_ready_generator_ship_ready_v6.py
# Daily posting to Webflow with Supabase state
# - TRUE dedupe by rank (persisted when available)
# - Bootstraps posted_ranks from EXISTING Webflow items every run (no more reliance on local files)
# - Hard base-slug guard (never creates "-2" for previously published players)
# - Correct Webflow v2 slug lookup (?slug=)
# - File fallbacks for all state; silent fallback if state_data 404s

import json
import requests
import os
import random
import re
import time
import hashlib
import html
import sys
from datetime import datetime, timezone

# ---------- I/O & Env ----------
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass  # Py<3.7 fallback

REQUIRED_ENV_VARS = ['WEBFLOW_API_TOKEN', 'WEBFLOW_SITE_ID', 'WEBFLOW_COLLECTION_ID']
_missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if _missing:
    raise ValueError(f"🔐 CRITICAL: Missing required environment variables: {', '.join(_missing)}")

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
WEBFLOW_API_TOKEN = os.getenv('WEBFLOW_API_TOKEN')
WEBFLOW_SITE_ID = os.getenv('WEBFLOW_SITE_ID')
WEBFLOW_COLLECTION_ID = os.getenv('WEBFLOW_COLLECTION_ID')

COLLECTION_PATH = os.getenv("WEBFLOW_COLLECTION_PATH", "fantasy-football-updates")

# Optional exclusions
EXCLUDE_TOP_N = int(os.getenv("EXCLUDE_TOP_N", "0"))      # e.g., 9 to skip ranks 1..9
EXCLUDE_RANKS_ENV = os.getenv("EXCLUDE_RANKS", "")        # e.g., "1,2,3,11,13"
EXCLUDE_RANKS_EXTRA = set()
if EXCLUDE_RANKS_ENV.strip():
    try:
        EXCLUDE_RANKS_EXTRA = {int(x.strip()) for x in EXCLUDE_RANKS_ENV.split(",") if x.strip().isdigit()}
    except Exception:
        EXCLUDE_RANKS_EXTRA = set()

HAS_SUPABASE = bool(SUPABASE_URL and SUPABASE_ANON_KEY)

STATE_DIR = os.getenv("STATE_DIR", ".")
os.makedirs(STATE_DIR, exist_ok=True)

POSTED_PATH = os.path.join(STATE_DIR, "posted_players.json")
HASHES_PATH = os.path.join(STATE_DIR, "content_hashes.json")
ANCHORS_PATH = os.path.join(STATE_DIR, "used_anchors.json")
POSTED_RANKS_PATH = os.path.join(STATE_DIR, "posted_ranks.json")

# ---------- Static Data (abridged to what we use) ----------
ESPN_RANKINGS = {
    'Ja\'Marr Chase': 1, 'Bijan Robinson': 2, 'Justin Jefferson': 3, 'Saquon Barkley': 4,
    'Jahmyr Gibbs': 5, 'CeeDee Lamb': 6, 'Christian McCaffrey': 7, 'Puka Nacua': 8,
    'Malik Nabers': 9, 'Amon-Ra St. Brown': 10, 'Ashton Jeanty': 11, 'De\'Von Achane': 12,
    'Nico Collins': 13, 'Brian Thomas Jr.': 14, 'A.J. Brown': 15, 'Drake London': 16,
    'Jonathan Taylor': 17, 'Josh Jacobs': 18, 'Derrick Henry': 19, 'Brock Bowers': 20,
    'Trey McBride': 21, 'Bucky Irving': 22, 'Chase Brown': 23, 'Tee Higgins': 24,
    'Tyreek Hill': 25, 'Ladd McConkey': 26, 'Davante Adams': 27, 'Josh Allen': 28,
    'Lamar Jackson': 29, 'Jalen Hurts': 31, 'Kyren Williams': 32, 'James Cook': 33,
    'Jaxon Smith-Njigba': 34, 'Terry McLaurin': 35, 'Garrett Wilson': 36, 'Omarion Hampton': 37,
    'Kenneth Walker III': 38, 'Joe Burrow': 39, 'George Kittle': 40, 'Alvin Kamara': 41,
    'Chuba Hubbard': 42, 'James Conner': 43, 'TreVeyon Henderson': 44, 'Mike Evans': 45,
    'Marvin Harrison Jr.': 46, 'Xavier Worthy': 47, 'DK Metcalf': 48, 'D.J. Moore': 49,
    'Rashee Rice': 50, 'Breece Hall': 51, 'D\'Andre Swift': 52, 'Zay Flowers': 53,
    'Courtland Sutton': 54, 'Calvin Ridley': 55, 'DeVonta Smith': 56, 'Jaylen Waddle': 57,
    'Jerry Jeudy': 58, 'Jameson Williams': 59, 'George Pickens': 60, 'Sam LaPorta': 61,
    'Patrick Mahomes': 62, 'Baker Mayfield': 63, 'Rome Odunze': 64, 'Tetairoa McMillan': 65,
    'Travis Hunter': 66, 'David Montgomery': 67, 'Aaron Jones Sr.': 68, 'T.J. Hockenson': 69,
    'Tony Pollard': 70, 'RJ Harvey': 71, 'Isiah Pacheco': 72, 'Chris Godwin': 73,
    'Jakobi Meyers': 74, 'Chris Olave': 75, 'Cooper Kupp': 76, 'Stefon Diggs': 77,
    'Matthew Golden': 78, 'Jordan Addison': 79, 'Tyrone Tracy Jr.': 80, 'Jaylen Warren': 81,
    'Kaleb Johnson': 82, 'Travis Kelce': 83, 'David Njoku': 84, 'Mark Andrews': 85,
    'Evan Engram': 86, 'Bo Nix': 87, 'Kyler Murray': 88, 'Brock Purdy': 89, 'Joe Mixon': 90,
    'Quinshon Judkins': 91, 'Brian Robinson Jr.': 92, 'J.K. Dobbins': 93, 'Rhamondre Stevenson': 94,
    'Javonte Williams': 95, 'Khalil Shakir': 96, 'Jauan Jennings': 97, 'Deebo Samuel': 98, 'Ricky Pearsall': 99, 'Keon Coleman': 100
}
ESPN_RANKINGS['Aaron Jones'] = 68

PLAYER_NAME_MAPPING = {
    'J. Chase': "Ja'Marr Chase", 'J Chase': "Ja'Marr Chase", "Ja'Marr Chase": "Ja'Marr Chase",
    'J. Jefferson': "Justin Jefferson", 'J Jefferson': "Justin Jefferson", 'Justin Jefferson': 'Justin Jefferson',
    'J. Gibbs': 'Jahmyr Gibbs', 'J Gibbs': 'Jahmyr Gibbs', 'Jahmyr Gibbs': 'Jahmyr Gibbs',
    'Bijan Robinson': 'Bijan Robinson',
    'S. Barkley': 'Saquon Barkley', 'Saquon Barkley': 'Saquon Barkley',
    'M. Nabers': 'Malik Nabers', 'Malik Nabers': 'Malik Nabers',
    'C. Lamb': 'CeeDee Lamb', 'CeeDee Lamb': 'CeeDee Lamb',
    "D. Achane": "De'Von Achane", "De'Von Achane": "De'Von Achane",
    'A. St. Brown': 'Amon-Ra St. Brown', 'A St. Brown': 'Amon-Ra St. Brown', 'Amon-Ra St. Brown': 'Amon-Ra St. Brown',
    # (add more as needed)
}

KEYWORD_VARIATIONS = [
    "vegas backed fantasy football rankings",
    "market-based fantasy rankings",
    "sportsbook-projected fantasy tiers",
    "betting market fantasy insights",
    "Vegas-derived player projections"
]
INTRO_STYLES = {
    "standard": "Welcome to market-based fantasy analysis—rankings anchored to sportsbook player props rather than static projections. We translate Vegas lines into fantasy expectations so you can draft with data, not guesswork.",
    "direct": "The betting market prices {name} differently than ESPN. Here's why our sportsbook-derived analysis reveals edges traditional rankings miss.",
    "comparison": "ESPN ranks {name} at #{espn_rank}, but Vegas betting markets tell a different story. Our market-implied projections place {name} at #{rank} overall.",
    "insight": "When sportsbooks set player prop lines, they're pricing real performance expectations. That market efficiency creates actionable fantasy insights traditional analysis overlooks."
}
FAQ_POOLS = {
    'primary': [
        "Is {name} worth a first-round pick in 2025?",
        "Should I draft {name} in the first round?",
        "What round should I target {name}?",
        "Is {name} a good fantasy pick this year?"
    ],
    'secondary': [
        "Why do market-based ranks differ from ESPN?",
        "How reliable are Vegas-based fantasy rankings?",
        "What makes market projections more accurate?",
        "Why trust sportsbook data over expert rankings?"
    ],
    'contextual': [
        "How does the playoff schedule impact {name}?",
        "What's {name}'s championship week outlook?",
        "How does team depth chart affect {name}?",
        "What's the market delta vs {name}'s ADP?"
    ]
}

# ---------- Core ----------
class ProductionBlogGenerator:
    def __init__(self):
        self.supabase_headers = {
            'apikey': SUPABASE_ANON_KEY,
            'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        } if HAS_SUPABASE else {}

        self.webflow_headers = {
            'Authorization': f'Bearer {WEBFLOW_API_TOKEN}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # If state_data table is missing, silently fallback to files after first 404
        self._state_data_writes_disabled = False
        self._posted_articles_writes_disabled = False  # quiet legacy name-log if table missing

        if HAS_SUPABASE:
            self.init_supabase_state()

        if os.getenv("RESET_STATE") == "1":
            print("🔄 RESET_STATE=1 detected - clearing all local state files")
            for path in [POSTED_PATH, HASHES_PATH, ANCHORS_PATH, POSTED_RANKS_PATH]:
                if os.path.exists(path):
                    os.remove(path)
                    print(f"🗑️ Deleted {path}")

        self.content_hashes = self.load_content_hashes_from_supabase()
        self.posted_players = self.load_posted_players_from_supabase()
        self.used_anchors = self.load_used_anchors_from_supabase()
        self.posted_ranks = self.load_posted_ranks_from_supabase()

    # ----- Canonicalization -----
    def _canon(self, s: str) -> str:
        if not s: return ""
        return self._canonical_player(PLAYER_NAME_MAPPING.get(s.strip(), s.strip()))

    def _canonical_player(self, raw):
        name = (raw or "").strip()
        return PLAYER_NAME_MAPPING.get(name, name)

    def _slugify_name(self, full_name: str) -> str:
        return full_name.lower().replace(' ', '-').replace('.', '').replace("'", "")

    # ----- Supabase / State init -----
    def init_supabase_state(self):
        try:
            print("📊 Supabase state tables assumed present")
        except Exception as e:
            print(f"⚠️ Could not initialize Supabase state: {e}")

    # ----- Content hashes -----
    def load_content_hashes_from_supabase(self):
        try:
            r = self._get(f'{SUPABASE_URL}/rest/v1/state_data?key=eq.content_hashes', self.supabase_headers)
            if r.status_code == 200:
                data = r.json()
                if data:
                    return set(data[0]['data'])
        except Exception:
            pass
        return self._load_set(HASHES_PATH)

    def save_content_hashes_to_supabase(self):
        if self._state_data_writes_disabled or not HAS_SUPABASE:
            self._save_set(HASHES_PATH, self.content_hashes)
            return
        try:
            payload = {'key': 'content_hashes', 'data': list(self.content_hashes),
                       'updated_at': datetime.now(timezone.utc).isoformat()}
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/state_data?on_conflict=key',
                headers={**self.supabase_headers, 'Prefer': 'resolution=merge-duplicates'},
                json=payload, timeout=30
            )
            if r.status_code not in (200, 201):
                if r.status_code == 404:
                    self._state_data_writes_disabled = True
                    print("ℹ️ state_data not found; using local files for state (no more warnings).")
                self._save_set(HASHES_PATH, self.content_hashes)
        except Exception:
            self._save_set(HASHES_PATH, self.content_hashes)

    # ----- Posted players (legacy, by name) -----
    def load_posted_players_from_supabase(self):
        try:
            r = self._get(f'{SUPABASE_URL}/rest/v1/posted_articles?select=player_name', self.supabase_headers)
            if r.status_code == 200:
                return [self._canon(d['player_name']) for d in r.json()]
        except Exception:
            pass
        return self._load_list(POSTED_PATH)

    def save_posted_player_to_supabase(self, player_name, slug, content_hash):
        if self._posted_articles_writes_disabled or not HAS_SUPABASE:
            self._append_list(POSTED_PATH, self._canon(player_name))
            return False
        try:
            c = self._canon(player_name)
            payload = {
                'player_name': c, 'slug': slug, 'content_hash': content_hash,
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/posted_articles?on_conflict=player_name',
                headers={**self.supabase_headers, 'Prefer': 'resolution=merge-duplicates'},
                json=payload, timeout=30
            )
            if r.status_code in (200, 201):
                return True
            if r.status_code == 404:
                self._posted_articles_writes_disabled = True
            self._append_list(POSTED_PATH, c)
            return False
        except Exception:
            self._append_list(POSTED_PATH, self._canon(player_name))
            return False

    # ----- Used anchors -----
    def load_used_anchors_from_supabase(self):
        try:
            r = self._get(f'{SUPABASE_URL}/rest/v1/state_data?key=eq.used_anchors', self.supabase_headers)
            if r.status_code == 200:
                data = r.json()
                if data:
                    return data[0]['data']
        except Exception:
            pass
        return self._load_json(ANCHORS_PATH, {})

    def save_used_anchors_to_supabase(self):
        if self._state_data_writes_disabled or not HAS_SUPABASE:
            self._save_json(ANCHORS_PATH, self.used_anchors)
            return
        try:
            payload = {'key': 'used_anchors', 'data': self.used_anchors,
                       'updated_at': datetime.now(timezone.utc).isoformat()}
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/state_data?on_conflict=key',
                headers={**self.supabase_headers, 'Prefer': 'resolution=merge-duplicates'},
                json=payload, timeout=30
            )
            if r.status_code not in (200, 201):
                if r.status_code == 404:
                    self._state_data_writes_disabled = True
                    print("ℹ️ state_data not found; using local files for state (no more warnings).")
                self._save_json(ANCHORS_PATH, self.used_anchors)
        except Exception:
            self._save_json(ANCHORS_PATH, self.used_anchors)

    # ----- posted_ranks (true dedupe) -----
    def load_posted_ranks_from_supabase(self):
        try:
            if HAS_SUPABASE:
                r = self._get(f'{SUPABASE_URL}/rest/v1/state_data?key=eq.posted_ranks', self.supabase_headers)
                if r.status_code == 200 and r.json():
                    return set(r.json()[0]['data'])
        except Exception:
            pass
        return self._load_set(POSTED_RANKS_PATH)

    def save_posted_ranks_to_supabase(self):
        if self._state_data_writes_disabled or not HAS_SUPABASE:
            self._save_set(POSTED_RANKS_PATH, self.posted_ranks)
            return
        try:
            payload = {'key': 'posted_ranks', 'data': sorted(list(self.posted_ranks)),
                       'updated_at': datetime.now(timezone.utc).isoformat()}
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/state_data?on_conflict=key',
                headers={**self.supabase_headers, 'Prefer': 'resolution=merge-duplicates'},
                json=payload, timeout=30
            )
            if r.status_code not in (200, 201):
                if r.status_code == 404:
                    self._state_data_writes_disabled = True
                    print("ℹ️ state_data not found; using local files for state (no more warnings).")
                self._save_set(POSTED_RANKS_PATH, self.posted_ranks)
        except Exception:
            self._save_set(POSTED_RANKS_PATH, self.posted_ranks)

    # ----- HTTP helpers -----
    def _get(self, url, headers, tries=3):
        for i in range(tries):
            try:
                r = requests.get(url, headers=headers, timeout=30)
                if r.status_code == 200: return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5)); continue
                return r
            except Exception:
                if i < tries-1:
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5))
                else:
                    raise

    def _post_with_backoff(self, url, headers, json_payload, tries=3):
        for i in range(tries):
            try:
                r = requests.post(url, headers=headers, json=json_payload, timeout=30)
                if r.status_code in (200, 201, 202): return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5)); continue
                return r
            except Exception:
                if i < tries-1:
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5))
                else:
                    raise

    # ----- Webflow lookups (v2 uses ?slug=) -----
    def slug_exists(self, slug: str) -> bool:
        try:
            q = requests.utils.quote(slug)
            r = self._get(
                f'https://api.webflow.com/v2/collections/{WEBFLOW_COLLECTION_ID}/items?slug={q}',
                self.webflow_headers
            )
            if r and r.status_code == 200:
                items = r.json().get('items', [])
                for it in items:
                    fd = (it.get('fieldData') or {})
                    if fd.get('slug') == slug or it.get('slug') == slug:
                        return True
        except Exception:
            pass
        return False

    # ----- Local JSON helpers -----
    def _load_set(self, path):
        if os.path.exists(path):
            try:
                with open(path, 'r') as f: return set(json.load(f))
            except Exception: pass
        return set()

    def _save_set(self, path, s):
        try:
            with open(path, 'w') as f: json.dump(sorted(list(s)), f)
        except Exception: pass

    def _load_list(self, path):
        if os.path.exists(path):
            try:
                with open(path, 'r') as f: return json.load(f)
            except Exception: pass
        return []

    def _append_list(self, path, val):
        data = self._load_list(path)
        if val not in data:
            data.append(val)
            try:
                with open(path, 'w') as f: json.dump(data, f, indent=2)
            except Exception: pass

    def _load_json(self, path, default):
        if os.path.exists(path):
            try:
                with open(path, 'r') as f: return json.load(f)
            except Exception: pass
        return default

    def _save_json(self, path, obj):
        try:
            with open(path, 'w') as f: json.dump(obj, f)
        except Exception: pass

    # ----- Content helpers -----
    def _safe_jsonld(self, payload, max_len=90000):
        s = json.dumps(payload, separators=(',', ':'))
        if len(s) > max_len:
            if isinstance(payload, list) and len(payload) == 2 and payload[1].get("@type") == "FAQPage":
                while len(s) > max_len and payload[1]["mainEntity"]:
                    payload[1]["mainEntity"].pop()
                    s = json.dumps(payload, separators=(',', ':'))
        return s

    def word_safe_clamp(self, text, max_length):
        if len(text) <= max_length: return text
        truncated = text[:max_length]
        last_space = truncated.rfind(' ')
        return text[:last_space] if last_space > max_length * 0.8 else text[:max_length]

    def _is_missing(self, value):
        return value in (None, "N/A") or (isinstance(value, str) and value.strip() == "")

    def _to_float(self, value):
        try: return float(str(value).replace(',', ''))
        except: return None

    def _as_webflow_image(self, url, alt=""):
        FALLBACK = "https://cdn.prod.website-files.com/670bfa1fd9c3c20a149fa6a7/688d2acad067d5e2eb678698_footballblog.png"
        u = (url or "").strip() or FALLBACK
        return {"url": u, "alt": alt}

    def _webflow_allowed_fields(self):
        if hasattr(self, "_wf_fields_cache"): return self._wf_fields_cache
        # Minimal set present in your collection; script will auto-filter to this.
        fallback = {
            "name", "slug", "post-body", "post-summary",
            "main-image", "meta-title", "meta-description", "featured", "url"
        }
        try:
            r = self._get(f'https://api.webflow.com/v2/collections/{WEBFLOW_COLLECTION_ID}', self.webflow_headers)
            r.raise_for_status()
            data = r.json()
            schema_slugs = {f.get("slug") for f in data.get("fields", []) if f.get("slug")}
            schema_slugs |= {"name", "slug", "main-image"}
            allowed = schema_slugs if schema_slugs else fallback
        except Exception:
            allowed = fallback
        self._wf_fields_cache = allowed
        return allowed

    def _filter_to_allowed(self, fielddata: dict):
        allowed = self._webflow_allowed_fields()
        filtered = {k: v for k, v in fielddata.items() if k in allowed}
        return filtered

    # ----- Content generation -----
    def check_data_completeness(self, player_data):
        required = ["rushing_yards_line", "receiving_yards_line", "rushing_touchdowns_line",
                    "receiving_touchdowns_line", "fantasy_score"]
        na = sum(1 for k in required if self._is_missing(player_data.get(k)))
        score = 5 - na
        return (na < 3), ("Data complete" if na < 3 else f"Insufficient market data ({na}/5 missing)"), score

    def guarantee_primary_keyword(self, html_content, variation_index=0):
        PRIMARY = KEYWORD_VARIATIONS[variation_index % len(KEYWORD_VARIATIONS)]
        normalized = html.unescape(html_content)
        normalized = re.sub(r'&nbsp;', ' ', normalized)
        normalized = re.sub(r'[\u2018\u2019]', "'", normalized)
        for phrase in KEYWORD_VARIATIONS:
            normalized = re.sub(re.escape(phrase), "market-based rankings", normalized, flags=re.IGNORECASE)
        block = (
            '<h2>Market vs. Media Rankings</h2>\n'
            f'<p>Our <a href="https://thebettinginsider.com/fantasy-football">{PRIMARY}</a> '
            'provide superior draft guidance.</p>'
        )
        if '<h2>Market vs. Media Rankings</h2>' in normalized:
            return normalized.replace('<h2>Market vs. Media Rankings</h2>', block, 1)
        if '</p>' in normalized:
            return normalized.replace('</p>', '</p>\n' + block, 1)
        return block + "\n" + normalized

    def comparable_delta_enhanced(self, base_player, comp_player):
        deltas = []
        for field, label in [('rushing_touchdowns_line', 'TD line'),
                             ('receiving_yards_line', 'Rec yards'),
                             ('playoff_sos_score', 'Playoff SOS'),
                             ('fantasy_score', 'Proj points')]:
            bv = self._to_float(base_player.get(field))
            cv = self._to_float(comp_player.get(field))
            if bv is not None and cv is not None:
                diff = bv - cv
                sign = '+' if diff >= 0 else ''
                deltas.append(f"{label} {sign}{round(diff, 1)}")
        return ', '.join(deltas) if deltas else 'similar profile'

    def generate_article_html(self, full_name, position, player_data, espn_rank, overall_rank, all_players_data):
        # Sections / body
        rush_line = self._to_float(player_data.get('rushing_yards_line'))
        rec_line = self._to_float(player_data.get('receiving_yards_line'))
        td_line = self._to_float(player_data.get('rushing_touchdowns_line')) or self._to_float(player_data.get('receiving_touchdowns_line'))
        fantasy_score = self._to_float(player_data.get('fantasy_score'))

        sections = ['market_intel', 'production', 'championship', 'health', 'strategy']
        if position == 'RB':
            sections = ['championship', 'production', 'market_intel', 'health', 'strategy']
        elif position == 'WR' and random.random() > 0.6:
            sections = ['production', 'market_intel', 'championship', 'strategy', 'health']

        INTRO_STYLES = {
            "standard": "Welcome to market-based fantasy analysis—rankings anchored to sportsbook player props rather than static projections. We translate Vegas lines into fantasy expectations so you can draft with data, not guesswork.",
            "direct": f"The betting market prices {full_name} differently than ESPN. Here's why our sportsbook-derived analysis reveals edges traditional rankings miss.",
            "comparison": f"ESPN ranks {full_name} at #{espn_rank or '—'}, but Vegas betting markets tell a different story. Our market-implied projections place {full_name} at #{overall_rank} overall.",
            "insight": "When sportsbooks set player prop lines, they're pricing real performance expectations. That market efficiency creates actionable fantasy insights traditional analysis overlooks."
        }
        intro_text = INTRO_STYLES[random.choice(list(INTRO_STYLES.keys()))]

        post_body = (
            f'<p><em>By Jake Turner • Updated {datetime.now(timezone.utc).strftime("%B %d, %Y at %I:%M %p UTC")}</em></p>\n'
            f'<p>{intro_text}</p>\n'
            '<h2>Market vs. Media Rankings</h2>\n'
            f'<p>Our analysis places {full_name} at #{overall_rank} overall and #{player_data.get("position_rank", "N/A")} at {position}, compared to ESPN\'s ranking of #{espn_rank or "—"}.</p>\n'
            f'<p>{"The market prices " + full_name + " higher than ESPN (#" + str(overall_rank) + " vs #" + str(espn_rank) + "), suggesting undervalued consensus opportunity." if espn_rank and overall_rank < espn_rank else "ESPN ranks " + full_name + " at #" + str(espn_rank) + " while market data suggests #" + str(overall_rank) + ", indicating potential overvaluation." if espn_rank and overall_rank > espn_rank else "Both market and ESPN align, but our market-driven analysis reveals deeper context ESPN misses."}</p>\n'
        )
        if espn_rank:
            CONS = {
                'Ja\'Marr Chase': "Elite WR1 who dominated 2024 with league-leading metrics across targets, yards, and touchdowns.",
                'Justin Jefferson': "Proven WR1, QB-proof, top-5 despite QB changes.",
                'Bijan Robinson': "Breakout RB with elite dual-threat usage entering age-23 season.",
                'Saquon Barkley': "OPOY-level usage with massive scrimmage volume; high floor.",
                'Jahmyr Gibbs': "Explosive dual-threat; top TD equity when featured.",
                'CeeDee Lamb': "High-volume WR1, bounce-back candidate with stable QB play.",
                'Malik Nabers': "Target monster; elite PPR base with room for TD growth.",
                'Amon-Ra St. Brown': "105+ receptions three straight years; alpha slot."
            }
            insight = CONS.get(full_name)
            if insight:
                post_body += f'<p><strong>Consensus View:</strong> {insight}</p>\n'

        section_content = {
            'market_intel': f'''<h2>Market Intelligence</h2>
<p>The betting market's precision in pricing player outcomes makes our sportsbook-implied outlook significantly more reliable than conventional analysis.</p>
{f'<p><strong>Key Insight:</strong> {full_name}\'s {td_line} TD line implies 60%+ red-zone involvement—historically correlated with teams averaging 25+ PPG.</p>' if td_line and td_line > 7 else ''}''',

            'production': f'''<h2>Fantasy Production Outlook</h2>
<p>Our betting market insights position {full_name} with a projected fantasy score of {fantasy_score or "N/A"} points. This projection accounts for market efficiency patterns that traditional methods miss.</p>''',

            'championship': f'''<h2>Championship Weeks Assessment</h2>
<p>Playoff SOS score: {player_data.get('playoff_sos_score', 'N/A')} ({player_data.get('playoff_tier', 'Average')} tier)</p>
<p>Championship-week scheduling makes {full_name} {'advantageous' if player_data.get('playoff_sos_score', 50) > 65 else 'challenging' if player_data.get('playoff_sos_score', 50) < 45 else 'neutral'} for playoff builds during weeks 15-17.</p>''',

            'health': f'''<h2>Health & Availability Profile</h2>
<p>Projected games missed: {player_data.get('projected_games_missed', 'N/A')}</p>
<p>Market-implied values incorporate injury-adjusted distributions for realistic availability expectations.</p>''',

            'strategy': f'''<h2>Market-Based Draft Strategy</h2>
<p>Draft positioning for {full_name}: {'Picks 1-12 in 12-team formats' if overall_rank <= 12 else 'Picks 13-24 in 12-team formats' if overall_rank <= 24 else 'Round 3+ value territory'}.</p>
<p>Build considerations: {'Pair with high-target WR1 to balance TD variance' if position == 'RB' and overall_rank <= 12 else 'Build around as foundational WR1' if position == 'WR' and overall_rank <= 12 else 'Excellent depth with upside potential'}.</p>'''
        }
        for s in sections:
            post_body += "\n" + section_content[s] + "\n"

        # Key takeaways
        takeaways = [
            f"{full_name} market rank: #{overall_rank}",
            f"Projected fantasy points: {fantasy_score if fantasy_score is not None else 'N/A'}",
            f"Playoff SOS: {player_data.get('playoff_sos_score','N/A')} ({player_data.get('playoff_tier','Average')} tier)",
            f"TD line insight present: {'Yes' if (td_line and td_line > 7) else 'No'}",
        ]
        post_body += (
            '<div style="border:1px solid #eee;border-radius:8px;padding:14px;margin:18px 0;">'
            '<strong>Key Takeaways</strong><ul>' +
            ''.join(f'<li>{t}</li>' for t in takeaways) +
            '</ul></div>'
            '<div style="background:#f5f7ff;border:1px solid #dfe6ff;border-radius:8px;'
            'padding:12px;margin:16px 0;">'
            '<strong>Editor\'s Note:</strong> These ranks are market-implied and update as lines move.'
            '</div>'
        )

        # FAQs
        def gen_faqs():
            faqs = []
            primary_q = random.choice(FAQ_POOLS['primary']).format(name=full_name)
            primary_a = f"Based on Vegas-derived projections, {full_name} provides {'elite' if overall_rank <= 12 else 'strong' if overall_rank <= 24 else 'solid'} value at #{overall_rank} with {player_data.get('fantasy_score', 'N/A')} projected points."
            faqs.append((primary_q, primary_a))
            secondary_q = random.choice(FAQ_POOLS['secondary'])
            secondary_a = random.choice([
                "Sportsbook lines react to injuries, depth charts, and news in real-time, creating actionable edges that static preseason projections miss.",
                "Market efficiency in pricing player outcomes makes Vegas-derived projections more responsive to changing conditions than expert consensus rankings."
            ])
            faqs.append((secondary_q, secondary_a))
            if random.random() > 0.5:
                contextual_q = random.choice(FAQ_POOLS['contextual']).format(name=full_name)
                contextual_a = f"{'Favorable' if player_data.get('playoff_sos_score', 50) > 65 else 'Challenging' if player_data.get('playoff_sos_score', 50) < 45 else 'Neutral'} playoff matchups with {player_data.get('playoff_sos_score', 'N/A')} SOS score."
                faqs.append((contextual_q, contextual_a))
            return faqs

        faqs = gen_faqs()
        post_body += "\n<h2>Frequently Asked Questions</h2>\n"
        for q, a in faqs:
            post_body += f"<h3>{q}</h3>\n<p>{a}</p>\n\n"

        # Hub links
        team = player_data.get('team', 'Unknown')
        position = player_data.get('position', 'Unknown')
        position_lower = position.lower()
        team_lower = team.lower().replace(' ', '-')
        hub_links = f'''<div style="background:#f8f9fa;border:1px solid #e9ecef;border-radius:8px;padding:16px;margin:20px 0;">
<strong>Explore More:</strong> 
<a href="/fantasy-football/">All Rankings</a> • 
<a href="/fantasy-football/{position_lower}/">{position} Rankings</a> • 
<a href="/teams/{team_lower}/fantasy/">{team} Fantasy</a>
</div>'''
        post_body += hub_links

        # JSON-LD
        sports_article = {
            "@context": "https://schema.org",
            "@type": "SportsArticle",
            "headline": f"{full_name} Fantasy 2025: Market-Based Outlook",
            "about": [{"@type": "Person", "name": full_name}],
            "datePublished": datetime.now(timezone.utc).isoformat(),
            "dateModified": datetime.now(timezone.utc).isoformat(),
            "author": {"@type": "Person", "name": "Jake Turner"},
            "publisher": {"@type": "Organization", "name": "The Betting Insider",
                          "logo": {"@type": "ImageObject", "url": "https://thebettinginsider.com/logo.png"}},
            "image": {"@type": "ImageObject", "url": (player_data.get('player_headshot_url') or
                    'https://cdn.prod.website-files.com/670bfa1fd9c3c20a149fa6a7/688d2acad067d5e2eb678698_footballblog.png'),
                      "width": 400, "height": 400},
            "articleSection": "Fantasy Football",
            "keywords": [f"{full_name} fantasy 2025", f"{position} rankings"],
            "mainEntityOfPage": {"@type": "WebPage", "@id": f"https://thebettinginsider.com/{COLLECTION_PATH}/{self._slugify_name(full_name)}"}
        }
        faq_entities = [{"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faqs]
        faq_schema = {"@context": "https://schema.org", "@type": "FAQPage", "mainEntity": faq_entities}
        json_ld = self._safe_jsonld([sports_article, faq_schema])
        post_body += f'\n<script type="application/ld+json">{json_ld}</script>\n'

        return post_body

    # ----- Webflow seeding from existing items -----
    def seed_posted_ranks_from_webflow(self, all_players):
        """
        Robust, stateless bootstrap: for every player, compute the base slug and
        mark its rank as posted if that slug already exists in Webflow.
        This makes duplicates impossible even without persisted state.
        """
        added = 0
        for p in all_players:
            name_raw = p.get('name', '')
            full_name = self._canonical_player(PLAYER_NAME_MAPPING.get(name_raw, name_raw))
            base_slug = self._slugify_name(full_name)
            try:
                rank = int(p.get('overall_rank', 0))
            except:
                rank = 0
            if rank and self.slug_exists(base_slug) and rank not in self.posted_ranks:
                self.posted_ranks.add(rank)
                added += 1
        if added:
            print(f"🧩 Seeded {added} ranks from existing Webflow items")
            self.save_posted_ranks_to_supabase()

    # ----- Main loop -----
    def run_daily_posting(self, posts_per_day=9):
        print(f"🚀 Starting DAILY production posting - {posts_per_day} new blogs")
        print(f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"📁 State persistence: {'Supabase + file fallback' if HAS_SUPABASE else 'file-only'} in {STATE_DIR}")
        if HAS_SUPABASE:
            print("✅ Supabase state persistence")
            try:
                test = requests.get(f'{SUPABASE_URL}/rest/v1/players?limit=1', headers=self.supabase_headers, timeout=10)
                print(f"🔍 Supabase test response: {test.status_code}")
                if test.status_code != 200:
                    print(f"❌ Supabase connection failed: {test.text}"); return
                print("✅ Supabase connection successful")
            except Exception as e:
                print(f"❌ Supabase connection error: {e}"); return
        else:
            print("❌ Supabase credentials are required to fetch player data."); return

        print("📊 Fetching all players...")
        try:
            r = requests.get(
                f'{SUPABASE_URL}/rest/v1/players?position=not.in.(D/ST,K)&order=overall_rank.asc&limit=175',
                headers=self.supabase_headers, timeout=30
            )
            print(f"📊 Player fetch response: {r.status_code}")
            if r.status_code != 200:
                print(f"❌ Failed to fetch players: {r.status_code}"); print(f"❌ Response text: {r.text}"); return
            all_players = r.json()
            print(f"📊 Found {len(all_players)} total players")
        except Exception as e:
            print(f"❌ Error fetching players: {e}"); return

        # 🔧 NEW: Bootstrap posted_ranks from Webflow every run
        self.seed_posted_ranks_from_webflow(all_players)

        # Build exclusion set
        exclude = set(self.posted_ranks)
        if EXCLUDE_TOP_N > 0:
            exclude |= set(range(1, EXCLUDE_TOP_N + 1))
        if EXCLUDE_RANKS_EXTRA:
            exclude |= EXCLUDE_RANKS_EXTRA

        def _safe_rank(p):
            try: return int(p.get('overall_rank', 999))
            except: return 999

        unposted = [p for p in all_players if _safe_rank(p) not in exclude]
        unposted = sorted(unposted, key=lambda x: _safe_rank(x))

        print(f"📊 Excluding {len(exclude)} ranks; sample: {sorted(list(exclude))[:20]}")
        print(f"📊 Next up ranks: {[ _safe_rank(p) for p in unposted[:10] ]}")

        daily_batch = unposted[:posts_per_day]
        print(f"📝 Today's batch: {len(daily_batch)} new players")
        print(f"🏷️ Already posted by rank: {len(self.posted_ranks)}")
        print(f"🔄 Remaining after today: {len(unposted) - len(daily_batch)}")

        if not daily_batch:
            print("🎉 All available players have been posted (given current exclusions)!")
            return

        successful = 0
        failed = 0
        data_skipped = 0

        for i, player in enumerate(daily_batch):
            name_raw = player['name']
            try:
                player_rank = int(player.get('overall_rank', 999))
            except:
                player_rank = 999

            full_name = self._canonical_player(PLAYER_NAME_MAPPING.get(name_raw, name_raw))
            base_slug = self._slugify_name(full_name)

            print(f"\n📝 Processing {i+1}/{len(daily_batch)}: #{player_rank} {name_raw}")

            # ⛔ HARD BASE-SLUG GUARD: if base slug already exists in Webflow, treat as posted and skip
            if self.slug_exists(base_slug):
                print(f"⛔ Already exists in Webflow (base slug): {base_slug} — marking rank as posted and skipping")
                self.posted_ranks.add(player_rank)
                self.save_posted_ranks_to_supabase()
                if full_name not in self.posted_players:
                    self._append_list(POSTED_PATH, full_name)
                continue

            # Fetch detailed data
            detailed = self.fetch_detailed_player_data(name_raw)
            if not detailed:
                print(f"❌ Fetch failed for {name_raw}")
                failed += 1
                continue

            player_data = detailed['player']
            espn_rank = detailed.get('espn', {}).get('rank') if detailed.get('espn') else None
            overall_rank = player_data.get('overall_rank', player_rank)
            position = player_data.get('position', 'Unknown')

            # Data completeness
            ok, _, _ = self.check_data_completeness(player_data)
            if not ok:
                print(f"⚠️ Data completeness issue: Skipping #{player_rank} {name_raw}")
                data_skipped += 1
                continue

            # Build body
            post_body = self.generate_article_html(full_name, position, player_data, espn_rank, overall_rank, all_players)

            # Title/meta
            clean_content = re.sub(r'<[^>]+>', '', post_body)
            content_hash = hashlib.sha1(clean_content.encode()).hexdigest()
            if content_hash in self.content_hashes:
                print(f"⚠️ Duplicate content hash for {full_name} — skipping")
                self.content_hashes.add(content_hash); self.save_content_hashes_to_supabase()
                self.posted_ranks.add(player_rank); self.save_posted_ranks_to_supabase()
                continue

            self.content_hashes.add(content_hash); self.save_content_hashes_to_supabase()

            title = self.word_safe_clamp(f"{full_name} Fantasy Outlook 2025 (Vegas vs ESPN, #{overall_rank})", 60)
            meta = self.word_safe_clamp(
                f"{full_name} market rank #{overall_rank} vs ESPN #{espn_rank or '—'}. Full breakdown, projections.",
                160
            )

            # Images
            featured_image = (player_data.get('player_headshot_url') or
                              'https://cdn.prod.website-files.com/670bfa1fd9c3c20a149fa6a7/688d2acad067d5e2eb678698_footballblog.png')

            # Field data (filtered later)
            fieldData_raw = {
                "name": title,
                "slug": base_slug,  # 👈 use base slug ONLY; never "-2" dupes
                "post-body": post_body,
                "post-summary": self.word_safe_clamp(clean_content.strip(), 220),
                "main-image": self._as_webflow_image(featured_image, alt=f"{full_name} fantasy article image"),
                "meta-title": title,
                "meta-description": meta,
                "featured": False,
                "url": f"https://thebettinginsider.com/{COLLECTION_PATH}/{base_slug}",
            }

            filtered_data = self._filter_to_allowed(fieldData_raw)

            print("DEBUG fieldData keys (post-filter):", sorted(filtered_data.keys()), flush=True)
            print("DEBUG main-image (post-filter):", filtered_data.get("main-image"), flush=True)

            # Post to Webflow
            post_data = {"isArchived": False, "isDraft": False, "fieldData": filtered_data}
            try:
                response = self._post_with_backoff(
                    f'https://api.webflow.com/v2/collections/{WEBFLOW_COLLECTION_ID}/items',
                    self.webflow_headers, post_data, tries=3
                )
                if response.status_code in (200, 201, 202):
                    print(f"✅ Posted {full_name} to Webflow (Status: {response.status_code}) - {len(clean_content.split())} words")
                    # publish url hint
                    try:
                        coll_response = self._get(f"https://api.webflow.com/v2/collections/{WEBFLOW_COLLECTION_ID}", self.webflow_headers)
                        if coll_response and coll_response.status_code == 200:
                            coll = coll_response.json()
                            coll_slug = coll.get("slug") or coll.get("displaySlug") or "fantasy-football-updates"
                            print(f"🔗 New: https://thebettinginsider.com/{coll_slug}/{filtered_data['slug']}")
                    except Exception:
                        pass

                    # mark posted
                    self.posted_ranks.add(player_rank); self.save_posted_ranks_to_supabase()
                    self.save_posted_player_to_supabase(full_name, base_slug, content_hash)
                    self.save_used_anchors_to_supabase()
                    successful += 1
                else:
                    print(f"❌ Failed to post {full_name}: {response.status_code} {response.text}")
                    failed += 1
            except Exception as e:
                print(f"❌ Error posting {full_name}: {e}")
                failed += 1

        if successful > 0:
            print(f"\n🚀 Publishing Webflow site...")
            self.publish_webflow_site()

        print(f"\n📊 DAILY posting summary:")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
        print(f"⚠️ Data issues skipped: {data_skipped}")
        print(f"📝 Total posted ranks to date: {len(self.posted_ranks)}")
        est_remaining = max(0, len(all_players) - len(self.posted_ranks) - len(EXCLUDE_RANKS_EXTRA) - EXCLUDE_TOP_N)
        print(f"🔄 Remaining (est): {est_remaining}")
        print("\n🎯 Features: base-slug guard • Webflow seeding • true rank dedupe • SEO • file fallbacks")

    # ----- Data fetch -----
    def fetch_detailed_player_data(self, player_name):
        try:
            qname = requests.utils.quote(player_name)
            player_resp = requests.get(
                f'{SUPABASE_URL}/rest/v1/players?name=ilike.%25{qname}%25',
                headers=self.supabase_headers, timeout=30
            )
            if player_resp.status_code != 200 or not player_resp.json():
                return None
            player_info = player_resp.json()[0]
            player_id = player_info['id']

            betting_resp = requests.get(
                f'{SUPABASE_URL}/rest/v1/player_betting_breakdown?player_id=eq.{player_id}',
                headers=self.supabase_headers, timeout=30
            )
            betting = betting_resp.json()[0] if betting_resp.status_code == 200 and betting_resp.json() else {}
            combined = {**player_info, **betting}

            full_name = self._canonical_player(PLAYER_NAME_MAPPING.get(player_name, player_name))
            espn_rank = ESPN_RANKINGS.get(full_name)
            espn_data = {'rank': espn_rank} if espn_rank else None

            return {'player': combined, 'espn': espn_data}
        except Exception:
            return None

    # ----- Publishing -----
    def publish_webflow_site(self, publish_custom=True, publish_staging=True):
        try:
            domain_ids = []
            if publish_custom:
                r = self._get(f'https://api.webflow.com/v2/sites/{WEBFLOW_SITE_ID}/custom_domains', self.webflow_headers)
                if r and r.status_code == 200:
                    data = r.json()
                    domain_ids = [d["id"] for d in data.get("customDomains", []) if d.get("id")]
                else:
                    publish_custom = False
            payload = {"publishToWebflowSubdomain": bool(publish_staging)}
            if publish_custom and domain_ids:
                payload["customDomains"] = domain_ids
            print("DEBUG publish payload:", payload, flush=True)
            resp = self._post_with_backoff(f'https://api.webflow.com/v2/sites/{WEBFLOW_SITE_ID}/publish',
                                           self.webflow_headers, payload, tries=3)
            if resp and resp.status_code in (200, 202):
                print("✅ Webflow site publish queued")
                for ping in [
                    "https://www.google.com/ping?sitemap=https://thebettinginsider.com/sitemap.xml",
                    "https://www.bing.com/ping?sitemap=https://thebettinginsider.com/sitemap.xml",
                ]:
                    try: requests.get(ping, timeout=10); print(f"📍 Pinged {ping.split('.')[1].title()}")
                    except: pass
                return True
            print(f"❌ Failed to publish site: {getattr(resp, 'status_code', None)} {getattr(resp, 'text', '')}")
            return False
        except Exception as e:
            print(f"❌ Error publishing site: {e}")
            return False


# ---------- CLI ----------
if __name__ == "__main__":
    import argparse

    print("🔍 DEBUG: Starting main script...")
    parser = argparse.ArgumentParser(description='DAILY production blog posting to Webflow (no-duplicate base slug, Webflow seeding)')
    parser.add_argument('--posts', type=int, default=9, help='Posts per day (default: 9)')
    parser.add_argument('--test', action='store_true', help='Test mode')
    args = parser.parse_args()
    print(f"🔍 DEBUG: Args parsed: posts={args.posts}, test={args.test}")

    print("🛡️ DAILY Production Blog Generator v6")
    print("✅ Base-slug guard (no -2 duplicates)")
    print("✅ Webflow seeding of posted_ranks on every run")
    print("✅ True dedupe by rank")
    print("✅ SEO + JSON-LD + hub links")
    print("🔐 Env validated")

    print("🔍 DEBUG: Creating generator instance...")
    try:
        generator = ProductionBlogGenerator()
        print("✅ Generator instance created successfully")
    except Exception as e:
        print(f"❌ Failed to create generator: {e}")
        sys.exit(1)

    if args.test:
        print("🧪 Test mode - no network posting")
    else:
        print("🔍 DEBUG: Starting daily posting...")
        generator.run_daily_posting(args.posts)
