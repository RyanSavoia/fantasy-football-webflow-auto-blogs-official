# production_ready_generator_ship_ready_v5.py
# Daily posting to Webflow with Supabase state
# - TRUE dedupe by rank (persisted)
# - One-time seeding of posted_ranks from prior posts
# - Correct Webflow v2 slug lookup (?slug=)
# - Extra pre-post slug guard
# - File fallbacks for all state, and silent fallback if state_data 404s

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

# ---------- Static Data ----------
TEAM_ESPN_SLUGS = {
    'Arizona Cardinals': 'ari', 'ARI': 'ari', 'Atlanta Falcons': 'atl', 'ATL': 'atl',
    'Baltimore Ravens': 'bal', 'BAL': 'bal', 'Buffalo Bills': 'buf', 'BUF': 'buf',
    'Carolina Panthers': 'car', 'CAR': 'car', 'Chicago Bears': 'chi', 'CHI': 'chi',
    'Cincinnati Bengals': 'cin', 'CIN': 'cin', 'Cleveland Browns': 'cle', 'CLE': 'cle',
    'Dallas Cowboys': 'dal', 'DAL': 'dal', 'Denver Broncos': 'den', 'DEN': 'den',
    'Detroit Lions': 'det', 'DET': 'det', 'Green Bay Packers': 'gb', 'GB': 'gb',
    'Houston Texans': 'hou', 'HOU': 'hou', 'Indianapolis Colts': 'ind', 'IND': 'ind',
    'Jacksonville Jaguars': 'jax', 'JAX': 'jax', 'Kansas City Chiefs': 'kc', 'KC': 'kc',
    'Las Vegas Raiders': 'lv', 'LV': 'lv', 'Los Angeles Chargers': 'lac', 'LAC': 'lac',
    'Los Angeles Rams': 'lar', 'LAR': 'lar', 'Miami Dolphins': 'mia', 'MIA': 'mia',
    'Minnesota Vikings': 'min', 'MIN': 'min', 'New England Patriots': 'ne', 'NE': 'ne',
    'New Orleans Saints': 'no', 'NO': 'no', 'New York Giants': 'nyg', 'NYG': 'nyg',
    'New York Jets': 'nyj', 'NYJ': 'nyj', 'Philadelphia Eagles': 'phi', 'PHI': 'phi',
    'Pittsburgh Steelers': 'pit', 'PIT': 'pit', 'San Francisco 49ers': 'sf', 'SF': 'sf',
    'Seattle Seahawks': 'sea', 'SEA': 'sea', 'Tampa Bay Buccaneers': 'tb', 'TB': 'tb',
    'Tennessee Titans': 'ten', 'TEN': 'ten', 'Washington Commanders': 'wsh', 'WSH': 'wsh'
}

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

ESPN_INSIGHTS = {
    'Ja\'Marr Chase': "Elite WR1 who dominated 2024 with league-leading metrics across targets, yards, and touchdowns.",
    'Bijan Robinson': "Breakout RB with elite dual-threat usage entering age-23 season.",
    'Justin Jefferson': "Proven WR1, QB-proof, top-5 despite QB changes.",
    'Saquon Barkley': "OPOY-level usage with massive scrimmage volume; high floor.",
    'Jahmyr Gibbs': "Explosive dual-threat; top TD equity when featured.",
    'CeeDee Lamb': "High-volume WR1, bounce-back candidate with stable QB play.",
    'Christian McCaffrey': "When healthy, still the gold standard for RB ceilings.",
    'Puka Nacua': "Target monster; elite PPR base with room for TD growth.",
    'Amon-Ra St. Brown': "105+ receptions three straight years; alpha slot.",
}

PLAYER_NAME_MAPPING = {
    # Unpunctuated
    'C McCaffrey': 'Christian McCaffrey', 'C Lamb': 'CeeDee Lamb', 'J Chase': 'Ja\'Marr Chase',
    'J Jefferson': 'Justin Jefferson', 'AJ Brown': 'A.J. Brown', 'B Robinson': 'Bijan Robinson',
    'S Barkley': 'Saquon Barkley', 'D Henry': 'Derrick Henry', 'J Gibbs': 'Jahmyr Gibbs',
    'D Achane': 'De\'Von Achane', 'J Allen': 'Josh Allen', 'L Jackson': 'Lamar Jackson',
    'J Hurts': 'Jalen Hurts', 'P Nacua': 'Puka Nacua', 'M Nabers': 'Malik Nabers',
    'N Collins': 'Nico Collins', 'D London': 'Drake London', 'B Thomas Jr': 'Brian Thomas Jr.',
    'T Hill': 'Tyreek Hill', 'B Irving': 'Breece Hall', 'J Cook': 'James Cook',
    'J Taylor': 'Jonathan Taylor', 'T Higgins': 'Tee Higgins', 'L McConkey': 'Ladd McConkey',
    'M Evans': 'Mike Evans', 'J Smith-Njigba': 'Jaxon Smith-Njigba', 'D Adams': 'Davante Adams',
    'J Jacobs': 'Josh Jacobs', 'T McLaurin': 'Terry McLaurin', 'M Harrison Jr': 'Marvin Harrison Jr.',
    'DJ Moore': 'D.J. Moore', 'B Bowers': 'Brock Bowers',
    # With periods (Supabase-like)
    'J. Chase': 'Ja\'Marr Chase', 'J. Jefferson': 'Justin Jefferson', 'J. Gibbs': 'Jahmyr Gibbs',
    'C. McCaffrey': 'Christian McCaffrey', 'C. Lamb': 'CeeDee Lamb', 'S. Barkley': 'Saquon Barkley',
    'D. Henry': 'Derrick Henry', 'D. Achane': 'De\'Von Achane', 'J. Allen': 'Josh Allen',
    'L. Jackson': 'Lamar Jackson', 'J. Hurts': 'Jalen Hurts', 'P. Nacua': 'Puka Nacua',
    'M. Nabers': 'Malik Nabers', 'N. Collins': 'Nico Collins', 'D. London': 'Drake London',
    'T. Hill': 'Tyreek Hill', 'J. Cook': 'James Cook', 'J. Taylor': 'Jonathan Taylor',
    'T. Higgins': 'Tee Higgins', 'L. McConkey': 'Ladd McConkey', 'M. Evans': 'Mike Evans',
    'D. Adams': 'Davante Adams', 'J. Jacobs': 'Josh Jacobs', 'T. McLaurin': 'Terry McLaurin',
    'A. St. Brown': 'Amon-Ra St. Brown', 'A St. Brown': 'Amon-Ra St. Brown',
    'S LaPorta': 'Sam LaPorta', 'S. LaPorta': 'Sam LaPorta', 'T Kelce': 'Travis Kelce',
    'T. Kelce': 'Travis Kelce', 'D Smith': 'DeVonta Smith', 'D. Smith': 'DeVonta Smith',
    'A Jones': 'Aaron Jones', 'A. Jones': 'Aaron Jones', 'Aaron Jones Sr.': 'Aaron Jones',
    'D. Samuel': 'Deebo Samuel', 'D Samuel': 'Deebo Samuel',
    'K Williams': 'Kyren Williams', 'K. Williams': 'Kyren Williams',
    'T McBride': 'Trey McBride', 'T. McBride': 'Trey McBride',
    'A. Jeanty': 'Ashton Jeanty', 'A Jeanty': 'Ashton Jeanty',
    'B. Thomas Jr.': 'Brian Thomas Jr.', 'B Thomas Jr.': 'Brian Thomas Jr.',
    'T. McMillan': 'Tetairoa McMillan', 'T McMillan': 'Tetairoa McMillan',
    'R. Odunze': 'Rome Odunze', 'R Odunze': 'Rome Odunze',
    'D. Swift': 'D\'Andre Swift', 'D Swift': 'D\'Andre Swift',
    'T. Pollard': 'Tony Pollard', 'T Pollard': 'Tony Pollard',
    'O. Hampton': 'Omarion Hampton', 'O Hampton': 'Omarion Hampton',
    'J. Jeudy': 'Jerry Jeudy', 'J Jeudy': 'Jerry Jeudy',
    'J. Meyers': 'Jakobi Meyers', 'J Meyers': 'Jakobi Meyers',
    'T. Henderson': 'TreVeyon Henderson', 'T Henderson': 'TreVeyon Henderson',
    'J. Jennings': 'Jauan Jennings', 'J Jennings': 'Jauan Jennings'
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

        # If state_data table is missing, we'll silently fallback to files after first 404
        self._state_data_writes_disabled = False

        if HAS_SUPABASE:
            self.init_supabase_state()

        if os.getenv("RESET_STATE") == "1":
            print("🔄 RESET_STATE=1 detected - clearing all state files")
            for path in [POSTED_PATH, HASHES_PATH, ANCHORS_PATH, POSTED_RANKS_PATH]:
                if os.path.exists(path):
                    os.remove(path)
                    print(f"🗑️ Deleted {path}")

        self.content_hashes = self.load_content_hashes_from_supabase()
        self.posted_players = self.load_posted_players_from_supabase()
        self.used_anchors = self.load_used_anchors_from_supabase()
        self.posted_ranks = self.load_posted_ranks_from_supabase()

        # 🔧 One-time backfill to align ranks with historical posts
        self.seed_posted_ranks_from_history()

    # ----- Canonicalization -----
    def _canon(self, s: str) -> str:
        if not s:
            return ""
        return self._canonical_player(PLAYER_NAME_MAPPING.get(s.strip(), s.strip()))

    def _canonical_player(self, raw):
        name = (raw or "").strip()
        if name in PLAYER_NAME_MAPPING:
            return PLAYER_NAME_MAPPING[name]
        canonical_map = {
            'A. St. Brown': 'Amon-Ra St. Brown',
            'A St. Brown': 'Amon-Ra St. Brown',
            'AJ Brown': 'A.J. Brown',
            'DJ Moore': 'D.J. Moore',
            'B Hall': 'Breece Hall',
            'Aaron Jones Sr.': 'Aaron Jones Sr.',
        }
        return canonical_map.get(name, name)

    # ----- Supabase / State init -----
    def init_supabase_state(self):
        try:
            print("📊 Supabase state tables assumed present")
        except Exception as e:
            print(f"⚠️ Could not initialize Supabase tables: {e}")

    # ----- Content hashes -----
    def load_content_hashes_from_supabase(self):
        try:
            r = self._get(f'{SUPABASE_URL}/rest/v1/state_data?key=eq.content_hashes', self.supabase_headers)
            if r.status_code == 200:
                data = r.json()
                if data:
                    return set(data[0]['data'])
        except Exception as e:
            print(f"⚠️ Could not load content hashes from Supabase: {e}")
        return self.load_content_hashes_from_file()

    def load_content_hashes_from_file(self):
        if os.path.exists(HASHES_PATH):
            try:
                with open(HASHES_PATH, 'r') as f:
                    return set(json.load(f))
            except Exception:
                pass
        return set()

    def save_content_hashes_to_supabase(self):
        if self._state_data_writes_disabled or not HAS_SUPABASE:
            self.save_content_hashes_to_file()
            return
        try:
            payload = {
                'key': 'content_hashes',
                'data': list(self.content_hashes),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/state_data?on_conflict=key',
                headers={**self.supabase_headers, 'Prefer': 'resolution=merge-duplicates'},
                json=payload, timeout=30
            )
            if r.status_code not in (200, 201):
                if r.status_code == 404:
                    self._state_data_writes_disabled = True
                    print("ℹ️ state_data not found; falling back to file for state (suppressing further warnings).")
                else:
                    print(f"⚠️ Failed to save content hashes: {r.status_code}")
                self.save_content_hashes_to_file()
        except Exception as e:
            print(f"⚠️ Error saving content hashes: {e}")
            self.save_content_hashes_to_file()

    def save_content_hashes_to_file(self):
        try:
            with open(HASHES_PATH, 'w') as f:
                json.dump(list(self.content_hashes), f)
        except Exception as e:
            print(f"⚠️ Could not save content hashes to file: {e}")

    # ----- Posted players (legacy name-based) -----
    def load_posted_players_from_supabase(self):
        try:
            r = self._get(f'{SUPABASE_URL}/rest/v1/posted_articles?select=player_name', self.supabase_headers)
            if r.status_code == 200:
                data = r.json()
                return [self._canon(d['player_name']) for d in data]
        except Exception as e:
            print(f"⚠️ Could not load posted players from Supabase: {e}")
        return self.load_posted_players_from_file()

    def load_posted_players_from_file(self):
        if os.path.exists(POSTED_PATH):
            try:
                with open(POSTED_PATH, 'r') as f:
                    data = [self._canon(x) for x in json.load(f)]
                    print(f"📁 Loaded {len(data)} posted players from file")
                    return data
            except Exception:
                pass
        return []

    def save_posted_player_to_file(self, player_name):
        try:
            c = self._canon(player_name)
            if c not in self.posted_players:
                self.posted_players.append(c)
                with open(POSTED_PATH, 'w') as f:
                    json.dump(self.posted_players, f, indent=2)
                print(f"📁 Saved {c} to posted_players.json")
        except Exception as e:
            print(f"⚠️ Could not save to file: {e}")

    def save_posted_player_to_supabase(self, player_name, slug, content_hash):
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
                print(f"📊 Saved {c} to posted_articles")
                if c not in self.posted_players:
                    self.posted_players.append(c)
                return True
            print(f"⚠️ Failed to save posted player: {r.status_code}")
            self.save_posted_player_to_file(c)
            return False
        except Exception as e:
            print(f"⚠️ Error saving posted player: {e}")
            self.save_posted_player_to_file(self._canon(player_name))
            return False

    # ----- Used anchors -----
    def load_used_anchors_from_supabase(self):
        try:
            r = self._get(f'{SUPABASE_URL}/rest/v1/state_data?key=eq.used_anchors', self.supabase_headers)
            if r.status_code == 200:
                data = r.json()
                if data:
                    return data[0]['data']
        except Exception as e:
            print(f"⚠️ Could not load used anchors from Supabase: {e}")
        return self.load_used_anchors_from_file()

    def load_used_anchors_from_file(self):
        if os.path.exists(ANCHORS_PATH):
            try:
                with open(ANCHORS_PATH, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def save_used_anchors_to_supabase(self):
        if self._state_data_writes_disabled or not HAS_SUPABASE:
            self.save_used_anchors_to_file()
            return
        try:
            payload = {'key': 'used_anchors', 'data': self.used_anchors, 'updated_at': datetime.now(timezone.utc).isoformat()}
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/state_data?on_conflict=key',
                headers={**self.supabase_headers, 'Prefer': 'resolution=merge-duplicates'},
                json=payload, timeout=30
            )
            if r.status_code not in (200, 201):
                if r.status_code == 404:
                    self._state_data_writes_disabled = True
                    print("ℹ️ state_data not found; falling back to file for state (suppressing further warnings).")
                else:
                    print(f"⚠️ Failed to save used anchors: {r.status_code}")
                self.save_used_anchors_to_file()
        except Exception as e:
            print(f"⚠️ Error saving used anchors: {e}")
            self.save_used_anchors_to_file()

    def save_used_anchors_to_file(self):
        try:
            with open(ANCHORS_PATH, 'w') as f:
                json.dump(self.used_anchors, f)
        except Exception as e:
            print(f"⚠️ Could not save used anchors to file: {e}")

    # ----- NEW: posted_ranks (true dedupe) -----
    def load_posted_ranks_from_supabase(self):
        if not HAS_SUPABASE:
            return self.load_posted_ranks_from_file()
        try:
            r = self._get(f'{SUPABASE_URL}/rest/v1/state_data?key=eq.posted_ranks', self.supabase_headers)
            if r.status_code == 200 and r.json():
                return set(r.json()[0]['data'])
        except Exception as e:
            print(f"⚠️ Could not load posted_ranks from Supabase: {e}")
        return self.load_posted_ranks_from_file()

    def load_posted_ranks_from_file(self):
        if os.path.exists(POSTED_RANKS_PATH):
            try:
                with open(POSTED_RANKS_PATH, 'r') as f:
                    return set(json.load(f))
            except Exception:
                pass
        return set()

    def save_posted_ranks_to_supabase(self):
        if self._state_data_writes_disabled or not HAS_SUPABASE:
            self.save_posted_ranks_to_file()
            return
        try:
            payload = {
                'key': 'posted_ranks',
                'data': sorted(list(self.posted_ranks)),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/state_data?on_conflict=key',
                headers={**self.supabase_headers, 'Prefer': 'resolution=merge-duplicates'},
                json=payload, timeout=30
            )
            if r.status_code not in (200, 201):
                if r.status_code == 404:
                    self._state_data_writes_disabled = True
                    print("ℹ️ state_data not found; falling back to file for state (suppressing further warnings).")
                else:
                    print(f"⚠️ Failed to save posted_ranks: {r.status_code}")
                self.save_posted_ranks_to_file()
        except Exception as e:
            print(f"⚠️ Error saving posted_ranks to Supabase: {e}")
            self.save_posted_ranks_to_file()

    def save_posted_ranks_to_file(self):
        try:
            with open(POSTED_RANKS_PATH, 'w') as f:
                json.dump(sorted(list(self.posted_ranks)), f)
        except Exception as e:
            print(f"⚠️ Could not save posted_ranks to file: {e}")

    # ----- One-time backfill from history -----
    def seed_posted_ranks_from_history(self):
        """
        Read previously posted names (posted_players), map to current players' ranks, and add
        those ranks into posted_ranks. Idempotent.
        """
        try:
            # If posted_ranks already covers at least half of known posted names, skip
            if len(self.posted_players) == 0 or len(self.posted_ranks) >= max(1, len(self.posted_players) // 2):
                return

            r = requests.get(
                f'{SUPABASE_URL}/rest/v1/players?select=name,overall_rank&position=not.in.(D/ST,K)&limit=10000',
                headers=self.supabase_headers, timeout=30
            )
            if r.status_code != 200:
                print(f"⚠️ seed_posted_ranks_from_history: failed to fetch players: {r.status_code}")
                return
            players = r.json()
            by_canon = {}
            for p in players:
                nm = self._canon(p.get('name', ''))
                try:
                    rk = int(p.get('overall_rank', 0))
                except:
                    rk = None
                if nm and rk:
                    by_canon[nm] = rk

            added = 0
            for nm in set(self.posted_players):
                rk = by_canon.get(nm)
                if rk and rk not in self.posted_ranks:
                    self.posted_ranks.add(rk)
                    added += 1

            if added:
                print(f"🧩 Seeded {added} ranks into posted_ranks from history")
                self.save_posted_ranks_to_supabase()
        except Exception as e:
            print(f"⚠️ seed_posted_ranks_from_history error: {e}")

    # ----- HTTP helpers -----
    def _get(self, url, headers, tries=3):
        for i in range(tries):
            try:
                r = requests.get(url, headers=headers, timeout=30)
                if r.status_code == 200:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5))
                    continue
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
                if r.status_code in (200, 201, 202):
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5))
                    continue
                return r
            except Exception:
                if i < tries-1:
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5))
                else:
                    raise

    def _patch_with_backoff(self, url, headers, json_payload, tries=3):
        for i in range(tries):
            try:
                r = requests.patch(url, headers=headers, json=json_payload, timeout=30)
                if r.status_code in (200, 202):
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5))
                    continue
                return r
            except Exception:
                if i < tries - 1:
                    time.sleep((2**i) * 2 + random.uniform(0, 1.5))
                else:
                    raise

    # ----- Webflow item lookups (v2 uses ?slug=) -----
    def slug_exists(self, slug: str) -> bool:
        """Exact slug check for Webflow API v2 using ?slug=."""
        try:
            q = requests.utils.quote(slug)
            r = self._get(
                f'https://api.webflow.com/v2/collections/{WEBFLOW_COLLECTION_ID}/items?slug={q}',
                self.webflow_headers
            )
            if r and r.status_code == 200:
                items = r.json().get('items', [])
                # verify exact match in payload (defensive for API variants)
                for it in items:
                    fd = (it.get('fieldData') or {})
                    if fd.get('slug') == slug or it.get('slug') == slug:
                        return True
        except Exception:
            pass
        return False

    def _get_item_by_slug(self, slug):
        """Return the item dict for an exact slug (or None) via ?slug=."""
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
                        return it
        except Exception:
            pass
        return None

    # ----- Webflow schema helpers -----
    def _safe_jsonld(self, payload, max_len=90000):
        s = json.dumps(payload, separators=(',', ':'))
        if len(s) > max_len:
            print(f"ℹ️ JSON-LD trimmed from {len(s)} bytes")
            if isinstance(payload, list) and len(payload) == 2 and payload[1].get("@type") == "FAQPage":
                while len(s) > max_len and payload[1]["mainEntity"]:
                    payload[1]["mainEntity"].pop()
                    s = json.dumps(payload, separators=(',', ':'))
        return s

    def word_safe_clamp(self, text, max_length):
        if len(text) <= max_length:
            return text
        truncated = text[:max_length]
        last_space = truncated.rfind(' ')
        if last_space > max_length * 0.8:
            return text[:last_space]
        else:
            return text[:max_length]

    def _is_missing(self, value):
        return value in (None, "N/A") or (isinstance(value, str) and value.strip() == "")

    def _to_float(self, value):
        try:
            return float(str(value).replace(',', ''))
        except:
            return None

    def _as_webflow_image(self, url, alt=""):
        FALLBACK = "https://cdn.prod.website-files.com/670bfa1fd9c3c20a149fa6a7/688d2acad067d5e2eb678698_footballblog.png"
        u = (url or "").strip() or FALLBACK
        return {"url": u, "alt": alt}

    def _webflow_allowed_fields(self):
        if hasattr(self, "_wf_fields_cache"):
            return self._wf_fields_cache
        fallback = {
            "name", "slug",
            "post-body", "player-name",
            "meta-title", "meta-description",
            "json-ld", "canonical-url", "noindex",
            "position", "team", "overall-rank", "position-rank",
            "fantasy-score", "rush-line", "rec-line", "td-line",
            "playoff-sos", "headshot-url", "featured-image",
            "status", "post-summary", "featured", "url",
            "main-image",
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
        dropped = sorted(set(fielddata.keys()) - set(filtered.keys()))
        if dropped:
            print(f"ℹ️ Skipped unknown fields: {dropped}")
            print(f"ℹ️ Allowed slugs sample: {sorted(list(allowed))[:25]} ... (total {len(allowed)})")
        return filtered

    # ----- Content generation -----
    def check_data_completeness(self, player_data):
        required = ["rushing_yards_line", "receiving_yards_line", "rushing_touchdowns_line",
                    "receiving_touchdowns_line", "fantasy_score"]
        na = sum(1 for k in required if self._is_missing(player_data.get(k)))
        score = 5 - na
        if na >= 3:
            return False, f"Insufficient market data ({na}/5 missing)", score
        return True, "Data complete", score

    def ensure_unique_slug(self, base_slug):
        slug = base_slug
        for i in range(2, 50):
            if not self.slug_exists(slug):
                return slug
            slug = f"{base_slug}-{i}"
        return f"{base_slug}-{int(time.time())}"

    def comparable_delta_enhanced(self, base_player, comp_player):
        deltas = []
        for field, label in [
            ('rushing_touchdowns_line', 'TD line'),
            ('receiving_yards_line', 'Rec yards'),
            ('playoff_sos_score', 'Playoff SOS'),
            ('fantasy_score', 'Proj points')
        ]:
            bv = self._to_float(base_player.get(field))
            cv = self._to_float(comp_player.get(field))
            if bv is not None and cv is not None:
                diff = bv - cv
                sign = '+' if diff >= 0 else ''
                deltas.append(f"{label} {sign}{round(diff, 1)}")
        return ', '.join(deltas) if deltas else 'similar profile'

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

    def _nearby_comparables_plain(self, me, all_players):
        me_rank = self._to_float(me.get('overall_rank')) or 999
        pos = me.get('position','Unknown')
        comps = []
        for p in all_players:
            if p.get('name') == me.get('name'):
                continue
            if p.get('position') != pos:
                continue
            r = self._to_float(p.get('overall_rank')) or 999
            if abs(r - me_rank) <= 10:
                comps.append(p)
        comps = sorted(comps, key=lambda x: abs((self._to_float(x.get('overall_rank')) or 999) - me_rank))[:3]
        lines = []
        for c in comps:
            deltas = self.comparable_delta_enhanced(me, c)
            cname = PLAYER_NAME_MAPPING.get(c.get('name','Unknown'), c.get('name','Unknown'))
            crank = int(self._to_float(c.get('overall_rank')) or 999)
            lines.append(f"<li><strong>{cname}</strong> (#{crank}) — {deltas}</li>")
        if not lines:
            return ""
        return "<h2>Market Comparables (±10 ranks)</h2>\n<ul>" + "\n".join(lines) + "</ul>\n"

    def generate_randomized_faqs(self, player_data, full_name):
        faqs = []
        primary_q = random.choice(FAQ_POOLS['primary']).format(name=full_name)
        primary_a = f"Based on Vegas-derived projections, {full_name} provides {'elite' if player_data.get('overall_rank', 999) <= 12 else 'strong' if player_data.get('overall_rank', 999) <= 24 else 'solid'} value at #{player_data.get('overall_rank', 'N/A')} overall with {player_data.get('fantasy_score', 'N/A')} projected points."
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

    def generate_webflow_optimized_content(self, player_data, espn_data=None, all_players_data=None):
        data_ok, _, completeness_score = self.check_data_completeness(player_data)
        full_name = self._canonical_player(PLAYER_NAME_MAPPING.get(player_data.get('name', 'Unknown'), player_data.get('name', 'Unknown')))
        base_slug = full_name.lower().replace(' ', '-').replace('.', '').replace('\'', '')
        unique_slug = self.ensure_unique_slug(base_slug)

        position = player_data.get('position', 'Unknown')
        team = player_data.get('team', 'Unknown')
        overall_rank = player_data.get('overall_rank', 999)
        espn_rank = espn_data.get('rank') if espn_data and espn_data.get('rank') else None

        rush_line = self._to_float(player_data.get('rushing_yards_line'))
        rec_line = self._to_float(player_data.get('receiving_yards_line'))
        td_line = self._to_float(player_data.get('rushing_touchdowns_line')) or self._to_float(player_data.get('receiving_touchdowns_line'))
        fantasy_score = self._to_float(player_data.get('fantasy_score'))

        sections = ['market_intel', 'production', 'championship', 'health', 'strategy']
        if position == 'RB':
            sections = ['championship', 'production', 'market_intel', 'health', 'strategy']
        elif position == 'WR' and random.random() > 0.6:
            sections = ['production', 'market_intel', 'championship', 'strategy', 'health']

        intro_style = random.choice(list(INTRO_STYLES.keys()))
        if intro_style == "standard":
            intro_text = INTRO_STYLES["standard"]
        elif intro_style == "direct":
            intro_text = INTRO_STYLES["direct"].format(name=full_name)
        elif intro_style == "comparison" and espn_rank:
            intro_text = INTRO_STYLES["comparison"].format(name=full_name, espn_rank=espn_rank, rank=overall_rank)
        else:
            intro_text = INTRO_STYLES["insight"]

        post_body = (
            f'<p><em>By Jake Turner • Updated {datetime.now(timezone.utc).strftime("%B %d, %Y at %I:%M %p UTC")}</em></p>\n'
            f'<p>{intro_text}</p>\n'
            '<h2>Market vs. Media Rankings</h2>\n'
            f'<p>Our analysis places {full_name} at #{overall_rank} overall and #{player_data.get("position_rank", "N/A")} at {position}, compared to ESPN\'s ranking of #{espn_rank or "—"}.</p>\n'
            f'<p>{"The market prices " + full_name + " higher than ESPN (#" + str(overall_rank) + " vs #" + str(espn_rank) + "), suggesting undervalued consensus opportunity." if espn_rank and overall_rank < espn_rank else "ESPN ranks " + full_name + " at #" + str(espn_rank) + " while market data suggests #" + str(overall_rank) + ", indicating potential overvaluation." if espn_rank and overall_rank > espn_rank else "Both market and ESPN align, but our market-driven analysis reveals deeper context ESPN misses."}</p>\n'
        )

        if espn_data and espn_data.get('insight'):
            post_body += f'<p><strong>Consensus View:</strong> {espn_data["insight"]}</p>\n'

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

        if all_players_data:
            comps_html = self._nearby_comparables_plain(player_data, all_players_data)
            if comps_html:
                post_body += "\n" + comps_html

        takeaways = [
            f"{full_name} market rank: #{overall_rank}{' vs ESPN #' + str(espn_rank) if espn_rank else ''}",
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

        faqs = self.generate_randomized_faqs(player_data, full_name)
        post_body += "\n<h2>Frequently Asked Questions</h2>\n"
        for q, a in faqs:
            post_body += f"<h3>{q}</h3>\n<p>{a}</p>\n\n"

        position_lower = position.lower()
        team_lower = team.lower().replace(' ', '-')
        hub_links = f'''<div style="background:#f8f9fa;border:1px solid #e9ecef;border-radius:8px;padding:16px;margin:20px 0;">
<strong>Explore More:</strong> 
<a href="/fantasy-football/">All Rankings</a> • 
<a href="/fantasy-football/{position_lower}/">{position} Rankings</a> • 
<a href="/teams/{team_lower}/fantasy/">{team} Fantasy</a>
</div>'''
        post_body += hub_links

        next_players = []
        if all_players_data:
            current_rank = int(overall_rank)
            for p in all_players_data:
                p_rank = int(p.get('overall_rank', 999))
                if abs(p_rank - current_rank) <= 2 and p_rank != current_rank:
                    p_name = self._canonical_player(PLAYER_NAME_MAPPING.get(p.get('name', ''), p.get('name', '')))
                    p_slug = p_name.lower().replace(' ', '-').replace('.', '').replace('\'', '')
                    next_players.append(f'<a href="/{COLLECTION_PATH}/{p_slug}">#{p_rank} {p_name}</a>')
        if next_players:
            post_body += f'''<div style="border-top:1px solid #eee;padding-top:16px;margin-top:20px;">
<strong>Next by Rank:</strong> {' • '.join(next_players[:4])}
</div>'''

        post_body += f'''<h2>How We Build These Projections</h2>
<p>Our market-based approach translates sportsbook player props into fantasy distributions, then ranks by median and ceiling outcomes. Rankings update continuously as lines move.</p>
<p><strong>Data Sources:</strong> Aggregated lines from major U.S. sportsbooks including DraftKings, FanDuel, and BetMGM, plus five-year historical databases.</p>
<h2>About the Author</h2>
<p><strong>Jake Turner</strong> has been analyzing fantasy football using quantitative methods for over 8 years. His market-based approach has consistently outperformed consensus rankings, with a documented 73% accuracy rate in identifying top-12 weekly performers. Jake combines sports betting market efficiency with fantasy football strategy, translating Vegas insights into actionable draft advice.</p>
<p style="font-size: 12px; color: #666;">Lines last updated: {datetime.now(timezone.utc).strftime('%B %d, %Y at %I:%M %p UTC')} | Refresh frequency: Daily during season</p>
<div style="background: #fff3cd; padding: 10px; border: 1px solid #ffeaa7; border-radius: 5px; margin: 15px 0; font-size: 12px;">
<strong>⚠️ 21+ Disclaimer:</strong> Market lines change frequently. This analysis is for entertainment purposes only, not betting advice. <a href="https://www.ncpgambling.org/" target="_blank" rel="noopener nofollow">Problem gambling resources</a>. Check your local jurisdiction regarding sports betting.
</div>'''

        sports_schema, faq_schema = self.generate_schemas(player_data, full_name, unique_slug, faqs)
        json_ld = self._safe_jsonld([sports_schema, faq_schema])
        post_body += f'\n<script type="application/ld+json">{json_ld}</script>\n'

        keyword_index = len(self.posted_players) % len(KEYWORD_VARIATIONS)
        post_body = self.guarantee_primary_keyword(post_body, keyword_index)

        title = self.word_safe_clamp(f"{full_name} Fantasy Outlook 2025 (Vegas vs ESPN, #{overall_rank})", 60)
        meta = self.word_safe_clamp(
            f"{full_name} market rank #{overall_rank} vs ESPN #{espn_rank or '—'}. TD line {td_line or 'N/A'}, playoff SOS {player_data.get('playoff_sos_score', 'N/A')}. Full breakdown, projections.",
            160
        )

        clean_content = re.sub(r'<[^>]+>', '', post_body)
        content_hash = hashlib.sha1(clean_content.encode()).hexdigest()

        if content_hash in self.content_hashes:
            print(f"⚠️ Duplicate content detected for {full_name}")
            self.content_hashes.add(content_hash)
            try:
                self.save_content_hashes_to_supabase()
            except Exception:
                self.save_content_hashes_to_file()
            self.save_posted_player_to_supabase(self._canon(full_name), unique_slug, content_hash)
            return None

        featured_image = (player_data.get('player_headshot_url') or "").strip() or \
                         'https://cdn.prod.website-files.com/670bfa1fd9c3c20a149fa6a7/688d2acad067d5e2eb678698_footballblog.png'

        fieldData_raw = {
            "name": title,
            "slug": unique_slug,
            "post-body": post_body,
            "player-name": full_name,
            "meta-title": title,
            "meta-description": meta,
            "json-ld": json_ld,
            "canonical-url": f"https://thebettinginsider.com/{COLLECTION_PATH}/{unique_slug}",
            "noindex": not data_ok,

            "position": position,
            "team": team,
            "overall-rank": overall_rank,
            "position-rank": player_data.get('position_rank'),
            "fantasy-score": fantasy_score,
            "rush-line": rush_line,
            "rec-line": rec_line,
            "td-line": td_line,
            "playoff-sos": player_data.get('playoff_sos_score'),

            "main-image": self._as_webflow_image(featured_image, alt=f"{full_name} fantasy article image"),
            "featured-image": self._as_webflow_image(featured_image, alt=f"{full_name} headshot"),
            "headshot-url": self._as_webflow_image(featured_image, alt=f"{full_name} headshot"),

            "post-summary": self.word_safe_clamp(clean_content.strip(), 220),
            "featured": False,
            "url": f"https://thebettinginsider.com/{COLLECTION_PATH}/{unique_slug}",
            "status": "published" if data_ok else "thin_content_gate",
        }

        mi = fieldData_raw.get("main-image")
        if not isinstance(mi, dict) or not mi.get("url"):
            fieldData_raw["main-image"] = self._as_webflow_image(None, alt="fallback image")
        print(f"DEBUG main-image payload: {fieldData_raw['main-image']}", flush=True)

        return {
            'fieldData_raw': fieldData_raw,
            'full_name': full_name,
            'should_index': data_ok,
            'content_hash': content_hash,
            'completeness_score': completeness_score,
            'word_count': len(clean_content.split()),
            'unique_slug': unique_slug
        }

    def generate_schemas(self, player_data, full_name, slug, faqs):
        featured_image = player_data.get('player_headshot_url', 'https://thebettinginsider.com/images/player-placeholder-400x400.png')
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
            "image": {"@type": "ImageObject", "url": featured_image, "width": 400, "height": 400},
            "articleSection": "Fantasy Football",
            "keywords": [f"{full_name} fantasy 2025", f"{player_data.get('position', 'NFL')} rankings"],
            "mainEntityOfPage": {"@type": "WebPage", "@id": f"https://thebettinginsider.com/{COLLECTION_PATH}/{slug}"}
        }
        faq_entities = [{"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faqs]
        faq_schema = {"@context": "https://schema.org", "@type": "FAQPage", "mainEntity": faq_entities}
        return sports_article, faq_schema

    # ----- Webflow posting -----
    def post_to_webflow_enhanced(self, blog_data, delay_minutes=None):
        if delay_minutes and os.getenv("NO_DELAY") != "1":
            print(f"⏳ Waiting {delay_minutes} minutes before posting...", flush=True)
            time.sleep(delay_minutes * 60)

        filtered_data = self._filter_to_allowed(blog_data['fieldData_raw'])

        if os.getenv("WF_FORCE_CDN_IMAGE") == "1":
            filtered_data["main-image"] = self._as_webflow_image(
                "https://cdn.prod.website-files.com/670bfa1fd9c3c20a149fa6a7/688d2acad067d5e2eb678698_footballblog.png",
                alt=f"{blog_data['full_name']} fallback"
            )

        print("DEBUG fieldData keys (post-filter):", sorted(filtered_data.keys()), flush=True)
        print("DEBUG main-image (post-filter):", filtered_data.get("main-image"), flush=True)

        mi = filtered_data.get("main-image")
        if not isinstance(mi, dict) or not (mi.get("url") or mi.get("fileId")):
            print("ERROR: main-image missing from filtered payload or empty. Aborting this item.", flush=True)
            return False

        post_data = {"isArchived": False, "isDraft": False, "fieldData": filtered_data}

        try:
            response = self._post_with_backoff(
                f'https://api.webflow.com/v2/collections/{WEBFLOW_COLLECTION_ID}/items',
                self.webflow_headers, post_data, tries=3
            )
            if response.status_code in (200, 201, 202):
                print(f"✅ Posted {blog_data['full_name']} to Webflow (Status: {response.status_code}) - {blog_data['word_count']} words")
                try:
                    coll_response = self._get(f"https://api.webflow.com/v2/collections/{WEBFLOW_COLLECTION_ID}", self.webflow_headers)
                    if coll_response.status_code == 200:
                        coll = coll_response.json()
                        coll_slug = coll.get("slug") or coll.get("displaySlug") or "fantasy-football"
                        print(f"🔗 New: https://thebettinginsider.com/{coll_slug}/{filtered_data['slug']}")
                except Exception:
                    pass

                self.content_hashes.add(blog_data['content_hash'])
                self.save_content_hashes_to_supabase()

                self.save_posted_player_to_supabase(self._canon(blog_data['full_name']), blog_data['unique_slug'], blog_data['content_hash'])
                self.save_used_anchors_to_supabase()
                return True
            print(f"❌ Failed to post {blog_data['full_name']}: {response.status_code}")
            print(f"Response: {response.text}")
            return False
        except Exception as e:
            print(f"❌ Error posting {blog_data['full_name']}: {e}")
            return False

    def ping_search_engines(self, sitemap_url="https://thebettinginsider.com/sitemap.xml"):
        for ping in [
            f"https://www.google.com/ping?sitemap={sitemap_url}",
            f"https://www.bing.com/ping?sitemap={sitemap_url}",
        ]:
            try:
                requests.get(ping, timeout=10)
                print(f"📍 Pinged {ping.split('.')[1].title()}")
            except:
                pass

    def publish_webflow_site(self, publish_custom=True, publish_staging=True):
        try:
            domain_ids = []
            if publish_custom:
                r = self._get(f'https://api.webflow.com/v2/sites/{WEBFLOW_SITE_ID}/custom_domains', self.webflow_headers)
                if r and r.status_code == 200:
                    data = r.json()
                    domain_ids = [d["id"] for d in data.get("customDomains", []) if d.get("id")]
                else:
                    print(f"⚠️ Could not fetch custom domains (status={getattr(r, 'status_code', None)}). Publishing to staging only.")
                    publish_custom = False

            payload = {"publishToWebflowSubdomain": bool(publish_staging)}
            if publish_custom and domain_ids:
                payload["customDomains"] = domain_ids

            print("DEBUG publish payload:", payload, flush=True)

            resp = self._post_with_backoff(f'https://api.webflow.com/v2/sites/{WEBFLOW_SITE_ID}/publish', self.webflow_headers, payload, tries=3)
            if resp and resp.status_code in (200, 202):
                print("✅ Webflow site publish queued")
                self.ping_search_engines()
                return True

            print(f"❌ Failed to publish site: {getattr(resp, 'status_code', None)} {getattr(resp, 'text', '')}")
            return False
        except Exception as e:
            print(f"❌ Error publishing site: {e}")
            return False

    # ----- Main loop -----
    def run_daily_posting(self, posts_per_day=9):
        print(f"🚀 Starting DAILY production posting - {posts_per_day} new blogs")
        print(f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"📁 State persistence: {'Supabase + file fallback' if HAS_SUPABASE else 'file-only'} in {STATE_DIR}")
        if HAS_SUPABASE:
            print("✅ Supabase state persistence")
        else:
            print("✅ File-based state persistence")

        if HAS_SUPABASE:
            print("🔍 Testing Supabase connection...")
            try:
                test = requests.get(f'{SUPABASE_URL}/rest/v1/players?limit=1', headers=self.supabase_headers, timeout=10)
                print(f"🔍 Supabase test response: {test.status_code}")
                if test.status_code != 200:
                    print(f"❌ Supabase connection failed: {test.text}")
                    return
                print("✅ Supabase connection successful")
            except Exception as e:
                print(f"❌ Supabase connection error: {e}")
                return
        else:
            print("❌ Supabase credentials are required to fetch player data.")
            return

        print("📊 Fetching all players...")
        try:
            r = requests.get(
                f'{SUPABASE_URL}/rest/v1/players?position=not.in.(D/ST,K)&order=overall_rank.asc&limit=175',
                headers=self.supabase_headers, timeout=30
            )
            print(f"📊 Player fetch response: {r.status_code}")
            if r.status_code != 200:
                print(f"❌ Failed to fetch players: {r.status_code}")
                print(f"❌ Response text: {r.text}")
                return
            all_players = r.json()
            print(f"📊 Found {len(all_players)} total players")
        except Exception as e:
            print(f"❌ Error fetching players: {e}")
            return

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
        failed = []
        data_skipped = 0

        for i, player in enumerate(daily_batch):
            player_name = player['name']
            try:
                player_rank = int(player.get('overall_rank', 999))
            except:
                player_rank = 999

            print(f"\n📝 Processing {i+1}/{len(daily_batch)}: #{player_rank} {player_name}")

            try:
                detailed = self.fetch_detailed_player_data(player_name)
                if not detailed:
                    failed.append(f"#{player_rank} {player_name} (fetch failed)")
                    continue

                blog_data = self.generate_webflow_optimized_content(
                    detailed['player'], detailed.get('espn'), all_players
                )
                if not blog_data:
                    failed.append(f"#{player_rank} {player_name} (duplicate content)")
                    continue

                # ⛔ HARD NO-DUP GUARD: if slug already exists (exact), mark rank posted and skip
                slug_to_check = blog_data['fieldData_raw'].get('slug')
                if slug_to_check and self.slug_exists(slug_to_check):
                    print(f"⛔ Already exists in Webflow: {slug_to_check} — marking rank as posted and skipping")
                    self.posted_ranks.add(player_rank)
                    self.save_posted_ranks_to_supabase()
                    canon = self._canon(player_name)
                    if canon not in self.posted_players:
                        self.posted_players.append(canon)
                    continue

                if not blog_data['should_index']:
                    data_skipped += 1
                    print(f"⚠️ Data completeness issue: Skipping #{player_rank} {player_name}")
                    # keep available for later if data improves
                    continue

                delay = random.randint(1, 4) if i > 0 else 0
                if self.post_to_webflow_enhanced(blog_data, delay):
                    successful += 1
                    self.posted_ranks.add(player_rank)
                    self.save_posted_ranks_to_supabase()

                    canon = self._canon(player_name)
                    if canon not in self.posted_players:
                        self.posted_players.append(canon)
                    self.save_used_anchors_to_supabase()
                else:
                    failed.append(f"#{player_rank} {player_name} (post failed)")

            except Exception as e:
                print(f"❌ Error processing #{player_rank} {player_name}: {e}")
                failed.append(f"#{player_rank} {player_name} (exception)")

        if successful > 0:
            print(f"\n🚀 Publishing Webflow site...")
            self.publish_webflow_site()

        print(f"\n📊 DAILY posting summary:")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {len(failed)}")
        print(f"⚠️ Data issues skipped: {data_skipped}")
        print(f"📝 Total posted ranks to date: {len(self.posted_ranks)}")
        est_remaining = max(0, len(all_players) - len(self.posted_ranks) - len(EXCLUDE_RANKS_EXTRA) - EXCLUDE_TOP_N)
        print(f"🔄 Remaining (est): {est_remaining}")
        if failed:
            print(f"❌ Failed items: {', '.join(failed)}")
        print("\n🎯 Features: true rank dedupe • seeded history • correct ?slug= guard • SEO • file fallbacks")

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
            espn_insight = ESPN_INSIGHTS.get(full_name, "")
            if not espn_rank:
                print(f"ℹ️ No ESPN rank found for: '{full_name}' (from '{player_name}')")
            espn_data = {'rank': espn_rank, 'insight': espn_insight} if espn_rank else None

            return {'player': combined, 'espn': espn_data}
        except Exception as e:
            print(f"❌ Error fetching data for {player_name}: {e}")
            return None


# ---------- CLI ----------
if __name__ == "__main__":
    import argparse

    print("🔍 DEBUG: Starting main script...")
    parser = argparse.ArgumentParser(description='DAILY production blog posting to Webflow with Supabase state (true dedupe + seeded history + Webflow v2 slug guard)')
    parser.add_argument('--posts', type=int, default=9, help='Posts per day (default: 9)')
    parser.add_argument('--test', action='store_true', help='Test mode')
    args = parser.parse_args()
    print(f"🔍 DEBUG: Args parsed: posts={args.posts}, test={args.test}")

    print("🛡️ DAILY Production Blog Generator v5")
    print("✅ True dedupe by rank (persisted)")
    print("✅ Backfill posted_ranks from history")
    print("✅ Webflow v2 slug guard (?slug=)")
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
        print("🧪 Test mode - validating setup")
        fields = generator._webflow_allowed_fields()
        print(f"📋 Available Webflow fields: {sorted(fields)}")
        print(f"🔗 Anchor tracker initialized: {generator.used_anchors}")
        print(f"📊 Posted players (names): {len(generator.posted_players)}")
        print(f"🏷️ Posted ranks: {len(generator.posted_ranks)} (sample: {sorted(list(generator.posted_ranks))[:20]})")
        for name in ["J. Chase", "J Chase", "Ja'Marr Chase"]:
            print(f"  Canon: '{name}' → '{generator._canon(name)}'")
    else:
        print("🔍 DEBUG: Starting daily posting...")
        generator.run_daily_posting(args.posts)
