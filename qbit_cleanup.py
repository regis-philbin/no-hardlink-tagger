import os
import time
import json
import threading
import sqlite3
from datetime import datetime
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from qbittorrent import Client
import requests
import hashlib
import uuid


VERSION = "no-hardlink-tagger v3.0 — cache + two-stage + budget"

# -------------------------
# Config (env)
# -------------------------
QBITTORRENT_URL  = os.environ.get('QBITTORRENT_URL', '').rstrip('/')
QBITTORRENT_USER = os.environ.get('QBITTORRENT_USER')
QBITTORRENT_PASS = os.environ.get('QBITTORRENT_PASS')

ORPHAN_TAG    = os.environ.get('ORPHAN_TAG', 'NoMediaLink')
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')
MEDIA_DIRS    = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]

DEBUG_INTERVAL = int(os.environ.get('DEBUG_INTERVAL', '60'))
BATCH_SIZE     = int(os.environ.get('BATCH_SIZE', '25'))
MAX_TORRENTS   = int(os.environ.get('MAX_TORRENTS', '0'))  # 0 = all

EXT_WHITELIST  = [e.strip().lower() for e in os.environ.get(
    'EXT_WHITELIST', '.mkv,.mp4,.m4v,.mov,.avi,.ts,.m2ts,.mpg,.mpeg,.wmv'
).split(',') if e.strip()]
MIN_SIZE_MB    = int(os.environ.get('MIN_SIZE_MB', '50'))
MEDIA_LINK_MIN_PERCENT = max(0, min(100, int(os.environ.get('MEDIA_LINK_MIN_PERCENT', '1'))))

MEDIA_LINK_TAG_PREFIX = os.environ.get('MEDIA_LINK_TAG_PREFIX', 'MediaLink-')
MEDIA_LINK_TAG_STEPS = []
for _pct in os.environ.get('MEDIA_LINK_TAG_STEPS', '').split(','):
    if not _pct.strip():
        continue
    try:
        val = int(_pct)
        if 0 < val <= 100:
            MEDIA_LINK_TAG_STEPS.append(val)
    except ValueError:
        continue
MEDIA_LINK_TAG_STEPS = sorted(set(MEDIA_LINK_TAG_STEPS))

# Visibility guard
FAILSAFE_ENABLED           = os.environ.get('FAILSAFE_ENABLED', '1') not in ('0','false','False')
FAILSAFE_REQUIRE_DIRS      = os.environ.get('FAILSAFE_REQUIRE_DIRS', 'any').lower()  # 'any' or 'all'
FAILSAFE_MIN_MEDIA_FILES   = int(os.environ.get('FAILSAFE_MIN_MEDIA_FILES', '100'))
FAILSAFE_MAX_INDEX_ERRORS  = int(os.environ.get('FAILSAFE_MAX_INDEX_ERRORS', '200'))

# Activity + age
ACTIVE_GRACE_MINUTES       = int(os.environ.get('ACTIVE_GRACE_MINUTES', '30'))
MIN_COMPLETED_AGE_HOURS    = int(os.environ.get('MIN_COMPLETED_AGE_HOURS', '24'))

# Active-inode shield
ACTIVE_INODE_SHIELD        = os.environ.get('ACTIVE_INODE_SHIELD', '1') not in ('0','false','False')

# NEW: persistent cache + budget + decision TTL
CACHE_DIR                  = os.environ.get('CACHE_DIR', '/cache')
HASH_BUDGET_MB             = int(os.environ.get('HASH_BUDGET_MB', '1024'))  # total MiB to read this run
DECISION_TTL_HOURS         = int(os.environ.get('DECISION_TTL_HOURS', '24')) # reuse result for unchanged torrents

# Logging style
LOG_USE_AMPM               = os.environ.get('LOG_USE_AMPM', '0').lower() in ('1', 'true', 'yes', 'on')
ACTION_LOG_PATH            = os.environ.get('ACTION_LOG_PATH')  # optional override; defaults to CACHE_DIR/actions.log

# Web UI + persistence
WEB_UI_ENABLED             = os.environ.get('WEB_UI_ENABLED', '0').lower() in ('1', 'true', 'yes', 'on')
WEB_UI_BIND                = os.environ.get('WEB_UI_BIND', '0.0.0.0')
WEB_UI_PORT                = int(os.environ.get('WEB_UI_PORT', '8081'))

ASSESSMENTS_DB_PATH        = os.environ.get('ASSESSMENTS_DB_PATH', os.path.join(CACHE_DIR, 'assessments.sqlite'))

_verify_enabled_env        = os.environ.get('VERIFY_ENABLED')
if _verify_enabled_env is None:
    VERIFY_ENABLED = WEB_UI_ENABLED
else:
    VERIFY_ENABLED = _verify_enabled_env.lower() not in ('0', 'false', 'no')
VERIFY_INTERVAL_SECONDS    = int(os.environ.get('VERIFY_INTERVAL_SECONDS', '3600'))

# -------------------------
# Logging
# -------------------------
def _timestamp():
    now = datetime.now()
    if LOG_USE_AMPM:
        return now.strftime("%Y-%m-%d %I:%M:%S %p")
    return now.isoformat(sep=' ', timespec='seconds')


def iso_utcnow():
    return datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'


def human_size(n):
    try:
        n = int(n)
    except Exception:
        return ''
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.1f} {u}"
        size /= 1024


def log(msg):
    print(f"[{_timestamp()}] {msg}", flush=True)

_ACTION_LOG_WARN_INTERVAL = 60  # seconds
_last_action_log_warn = 0

def _action_log_path():
    if ACTION_LOG_PATH:
        path = ACTION_LOG_PATH
    else:
        if not _ensure_dir(CACHE_DIR):
            return None
        path = os.path.join(CACHE_DIR, 'actions.log')
    parent = os.path.dirname(path) or '.'
    return path if _ensure_dir(parent) else None

def log_actions(entries):
    """
    Persist important actions (tag/untag/coverage-tag) to a durable log.
    Entries: list of dicts already containing timestamp strings.
    """
    path = _action_log_path()
    global _last_action_log_warn
    if not path:
        return
    try:
        with open(path, 'a', encoding='utf-8') as f:
            for entry in entries:
                f.write(json.dumps(entry, separators=(',', ':'), ensure_ascii=False))
                f.write('\n')
    except Exception as e:
        now = time.time()
        if now - _last_action_log_warn >= _ACTION_LOG_WARN_INTERVAL:
            log(f"⚠️ action log write failed: {e}")
            _last_action_log_warn = now

# -------------------------
# qBittorrent helpers
# -------------------------
def get_qb_client():
    try:
        qb = Client(QBITTORRENT_URL)
        qb.login(QBITTORRENT_USER, QBITTORRENT_PASS)
        return qb
    except Exception as e:
        log(f"Error connecting to qBittorrent: {e}")
        return None

def _api_session():
    try:
        s = requests.Session()
        s.headers.update({'Referer': f"{QBITTORRENT_URL}/", 'Origin': QBITTORRENT_URL})
        r = s.post(f"{QBITTORRENT_URL}/api/v2/auth/login",
                   data={'username': QBITTORRENT_USER, 'password': QBITTORRENT_PASS},
                   timeout=10)
        if r.status_code != 200:
            return None, f"auth failed: {r.status_code} {r.text}"
        return s, None
    except Exception as e:
        return None, f"auth error: {e}"

def _api_post(path, data):
    s, err = _api_session()
    if err:
        log(f"❌ API session error: {err}")
        return False
    try:
        r = s.post(f"{QBITTORRENT_URL}/api/v2/{path}", data=data, timeout=15)
        if r.status_code == 200:
            log(f"➡ POST {path} 200 OK")
            return True
        log(f"❌ POST {path} -> {r.status_code} {r.text.strip()}")
        return False
    except Exception as e:
        log(f"❌ POST {path} exception: {e}")
        return False

def add_tag_http(hashes, tag):
    if hashes and _api_post('torrents/addTags', {'hashes': '|'.join(hashes), 'tags': tag}):
        log(f"✅ addTags OK: tagged {len(hashes)} torrent(s) with '{tag}'.")
        return len(hashes)
    return 0

def remove_tag_http(hashes, tag):
    if hashes and _api_post('torrents/removeTags', {'hashes': '|'.join(hashes), 'tags': tag}):
        log(f"🗑 removeTags OK: removed '{tag}' from {len(hashes)} torrent(s).")
        return len(hashes)
    return 0

# -------------------------
# Activity gates
# -------------------------
def is_actively_seeding(t):
    # Strict: only these are "active"
    s = (t.get('state') or '').strip().lower()
    return s in {'uploading', 'forcedup', 'checkingup', 'queuedup'}

def is_recently_active(t, minutes):
    if minutes <= 0:
        return False
    now = int(time.time())
    la = t.get('last_activity')
    if isinstance(la, int) and la > 0 and (now - la) < minutes * 60:
        return True
    if (t.get('upspeed') or 0) > 0:
        return True
    if is_actively_seeding(t):
        return True
    return False

def is_too_new(t, hours):
    if hours <= 0:
        return False
    co = t.get('completion_on')
    if not isinstance(co, int) or co <= 0:
        return True
    return (int(time.time()) - co) < hours * 3600

# -------------------------
# Filesystem helpers
# -------------------------
def is_media_candidate(path, size_bytes):
    if size_bytes < MIN_SIZE_MB * 1024 * 1024:
        return False
    ext = os.path.splitext(path)[1].lower()
    return (ext in EXT_WHITELIST) if EXT_WHITELIST else True

def _dir_accessible(p):
    try:
        if not os.path.isdir(p): return False
        if not (os.access(p, os.R_OK) and os.access(p, os.X_OK)): return False
        with os.scandir(p) as it:
            for _ in it: break
        return True
    except Exception:
        return False

def _dir_fingerprint(path, entry_count=None):
    try:
        st = os.stat(path)
        if entry_count is None:
            with os.scandir(path) as it:
                entry_count = sum(1 for _ in it)
        return max(int(st.st_mtime), int(st.st_ino), int(entry_count or 0))
    except Exception:
        return None

# -------------------------
# JSON cache helpers
# -------------------------
def _ensure_dir(p):
    try:
        os.makedirs(p, exist_ok=True)
        return True
    except Exception:
        return False

def _load_json(path, default):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception:
        return default

def _atomic_save_json(path, data):
    tmp = f"{path}.tmp"
    with open(tmp, 'w') as f:
        json.dump(data, f, separators=(',', ':'), ensure_ascii=False)
    os.replace(tmp, path)


# -------------------------
# SQLite persistence
# -------------------------
class AssessmentDB:
    def __init__(self, path):
        self.path = path
        self.ready = False
        if not path:
            return
        parent = os.path.dirname(path) or '.'
        if not _ensure_dir(parent):
            log(f"⚠️ Unable to create database directory: {parent}")
            return
        try:
            conn = self._connect()
            self._init_schema(conn)
            conn.close()
            self.ready = True
        except Exception as e:
            log(f"⚠️ Failed to initialize database at {path}: {e}")

    def _warn(self, msg):
        log(f"⚠️ DB: {msg}")

    def _connect(self):
        conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA foreign_keys=ON;')
        return conn

    def _init_schema(self, conn):
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS torrents (
                    torrent_hash TEXT PRIMARY KEY,
                    name TEXT,
                    size_bytes INTEGER,
                    tracker TEXT,
                    tags TEXT,
                    last_assessed_at TEXT,
                    linked_pct REAL,
                    coverage_tag TEXT,
                    cached INTEGER,
                    exists_in_qbt INTEGER,
                    last_seen_in_qbt_at TEXT,
                    last_missing_in_qbt_at TEXT,
                    removed INTEGER,
                    removed_after_tag INTEGER,
                    last_action TEXT,
                    last_action_tag TEXT,
                    last_action_at TEXT,
                    last_action_success INTEGER,
                    last_action_error TEXT
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_torrents_last_assessed_at ON torrents(last_assessed_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_torrents_exists_in_qbt ON torrents(exists_in_qbt);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_torrents_removed ON torrents(removed);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_torrents_linked_pct ON torrents(linked_pct);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_torrents_tracker ON torrents(tracker);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS assessment_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    assessed_at TEXT,
                    torrent_hash TEXT,
                    name TEXT,
                    size_bytes INTEGER,
                    tracker TEXT,
                    tags TEXT,
                    linked_pct REAL,
                    coverage_tag TEXT,
                    cached INTEGER,
                    exists_in_qbt INTEGER,
                    action_taken TEXT,
                    action_tag TEXT,
                    action_success INTEGER,
                    action_error TEXT
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_history_run_id ON assessment_history(run_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_history_hash ON assessment_history(torrent_hash);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)

    def _execute(self, query, params=None, many=False):
        if not self.ready:
            return
        try:
            conn = self._connect()
            try:
                with conn:
                    if many:
                        conn.executemany(query, params or [])
                    else:
                        conn.execute(query, params or [])
            finally:
                conn.close()
        except Exception as e:
            self._warn(e)

    def upsert_torrents(self, rows):
        if not (self.ready and rows):
            return
        payload = [(
            r.get('torrent_hash'), r.get('name'), r.get('size_bytes'), r.get('tracker'), r.get('tags'),
            r.get('last_assessed_at'), r.get('linked_pct'), r.get('coverage_tag'),
            1 if r.get('cached') else 0,
            1 if r.get('exists_in_qbt') else 0,
            r.get('last_seen_in_qbt_at'), r.get('last_missing_in_qbt_at'),
            1 if r.get('removed') else 0, 1 if r.get('removed_after_tag') else 0,
            r.get('last_action'), r.get('last_action_tag'), r.get('last_action_at'),
            None if r.get('last_action_success') is None else (1 if r.get('last_action_success') else 0),
            r.get('last_action_error')
        ) for r in rows]
        self._execute(
            """
            INSERT INTO torrents (
                torrent_hash, name, size_bytes, tracker, tags, last_assessed_at,
                linked_pct, coverage_tag, cached, exists_in_qbt, last_seen_in_qbt_at,
                last_missing_in_qbt_at, removed, removed_after_tag, last_action,
                last_action_tag, last_action_at, last_action_success, last_action_error
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(torrent_hash) DO UPDATE SET
                name=excluded.name,
                size_bytes=excluded.size_bytes,
                tracker=excluded.tracker,
                tags=excluded.tags,
                last_assessed_at=excluded.last_assessed_at,
                linked_pct=excluded.linked_pct,
                coverage_tag=excluded.coverage_tag,
                cached=excluded.cached,
                exists_in_qbt=excluded.exists_in_qbt,
                last_seen_in_qbt_at=excluded.last_seen_in_qbt_at,
                last_missing_in_qbt_at=CASE WHEN torrents.removed=1 THEN torrents.last_missing_in_qbt_at ELSE excluded.last_missing_in_qbt_at END,
                removed=CASE WHEN torrents.removed=1 THEN 1 ELSE excluded.removed END,
                removed_after_tag=CASE WHEN torrents.removed_after_tag=1 THEN 1 ELSE excluded.removed_after_tag END,
                last_action=COALESCE(excluded.last_action, torrents.last_action),
                last_action_tag=COALESCE(excluded.last_action_tag, torrents.last_action_tag),
                last_action_at=COALESCE(excluded.last_action_at, torrents.last_action_at),
                last_action_success=COALESCE(excluded.last_action_success, torrents.last_action_success),
                last_action_error=COALESCE(excluded.last_action_error, torrents.last_action_error)
            ;
            """,
            payload,
            many=True,
        )

    def append_history(self, rows):
        if not (self.ready and rows):
            return
        payload = [(
            r.get('run_id'), r.get('assessed_at'), r.get('torrent_hash'), r.get('name'), r.get('size_bytes'),
            r.get('tracker'), r.get('tags'), r.get('linked_pct'), r.get('coverage_tag'),
            1 if r.get('cached') else 0,
            1 if r.get('exists_in_qbt') else 0,
            r.get('action_taken'), r.get('action_tag'),
            None if r.get('action_success') is None else (1 if r.get('action_success') else 0),
            r.get('action_error')
        ) for r in rows]
        self._execute(
            """
            INSERT INTO assessment_history (
                run_id, assessed_at, torrent_hash, name, size_bytes, tracker, tags,
                linked_pct, coverage_tag, cached, exists_in_qbt, action_taken, action_tag,
                action_success, action_error
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            );
            """,
            payload,
            many=True,
        )

    def set_meta(self, key, value):
        if not self.ready:
            return
        self._execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
            (key, value),
        )

    def get_meta(self, key):
        if not self.ready:
            return None
        try:
            conn = self._connect()
            try:
                cur = conn.execute("SELECT value FROM meta WHERE key=?;", (key,))
                row = cur.fetchone()
                return row['value'] if row else None
            finally:
                conn.close()
        except Exception as e:
            self._warn(e)
            return None

    def list_hashes(self):
        if not self.ready:
            return set()
        try:
            conn = self._connect()
            try:
                cur = conn.execute("SELECT torrent_hash FROM torrents;")
                return {r['torrent_hash'] for r in cur.fetchall()}
            finally:
                conn.close()
        except Exception as e:
            self._warn(e)
            return set()

    def update_existence(self, exists_hashes, missing_hashes, ts):
        if not self.ready:
            return
        try:
            conn = self._connect()
            try:
                with conn:
                    if exists_hashes:
                        conn.executemany(
                            "UPDATE torrents SET exists_in_qbt=1, last_seen_in_qbt_at=?, last_missing_in_qbt_at=last_missing_in_qbt_at WHERE torrent_hash=?;",
                            [(ts, h) for h in exists_hashes],
                        )
                    if missing_hashes:
                        conn.executemany(
                            """
                            UPDATE torrents
                            SET exists_in_qbt=0,
                                last_missing_in_qbt_at=?,
                                removed=1,
                                removed_after_tag=CASE WHEN removed_after_tag=1 THEN 1 ELSE (CASE WHEN last_action='tag' AND last_action_success=1 THEN 1 ELSE 0 END) END
                            WHERE torrent_hash=?;
                            """,
                            [(ts, h) for h in missing_hashes],
                        )
            finally:
                conn.close()
        except Exception as e:
            self._warn(e)

    def status(self):
        if not self.ready:
            return {
                'last_verified_at': None,
                'total': 0,
                'exists': 0,
                'missing': 0,
                'last_verify_error': None,
            }
        try:
            conn = self._connect()
            try:
                cur = conn.execute("SELECT COUNT(*) AS c FROM torrents;")
                total = cur.fetchone()['c']
                cur = conn.execute("SELECT COUNT(*) AS c FROM torrents WHERE exists_in_qbt=1;")
                exists = cur.fetchone()['c']
                cur = conn.execute("SELECT COUNT(*) AS c FROM torrents WHERE exists_in_qbt=0;")
                missing = cur.fetchone()['c']
                return {
                    'last_verified_at': self.get_meta('last_verified_at'),
                    'total': total,
                    'exists': exists,
                    'missing': missing,
                    'last_verify_error': self.get_meta('last_verify_error'),
                }
            finally:
                conn.close()
        except Exception as e:
            self._warn(e)
            return {
                'last_verified_at': self.get_meta('last_verified_at'),
                'total': 0,
                'exists': 0,
                'missing': 0,
                'last_verify_error': self.get_meta('last_verify_error'),
            }

    def fetch_assessments(self):
        if not self.ready:
            return []
        try:
            conn = self._connect()
            try:
                cur = conn.execute("""
                    SELECT torrent_hash, name, size_bytes, tracker, tags, last_assessed_at, linked_pct,
                           coverage_tag, cached, exists_in_qbt, removed, removed_after_tag,
                           last_seen_in_qbt_at, last_missing_in_qbt_at,
                           last_action, last_action_tag, last_action_at, last_action_success,
                           last_action_error
                    FROM torrents
                    ORDER BY (last_assessed_at IS NULL), last_assessed_at DESC;
                """)
                rows = cur.fetchall()
                results = []
                for r in rows:
                    missing = not bool(r['exists_in_qbt']) if r['exists_in_qbt'] is not None else False
                    missing_after_tag = False
                    if missing and r['last_action'] == 'tag' and (r['last_action_success'] == 1 or r['last_action_success'] is True):
                        if r['last_missing_in_qbt_at'] and r['last_action_at']:
                            try:
                                missing_after_tag = r['last_action_at'] < r['last_missing_in_qbt_at']
                            except Exception:
                                missing_after_tag = True
                        else:
                            missing_after_tag = True
                    results.append({
                        'torrent_hash': r['torrent_hash'],
                        'name': r['name'],
                        'size_bytes': r['size_bytes'],
                        'size_human': human_size(r['size_bytes']),
                        'tracker': r['tracker'],
                        'tags': r['tags'],
                        'last_assessed_at': r['last_assessed_at'],
                        'linked_pct': r['linked_pct'],
                        'cached': bool(r['cached']) if r['cached'] is not None else False,
                        'exists_in_qbt': bool(r['exists_in_qbt']) if r['exists_in_qbt'] is not None else False,
                        'missing': missing,
                        'missing_after_tag': missing_after_tag,
                        'removed': bool(r['removed']) if r['removed'] is not None else False,
                        'removed_after_tag': bool(r['removed_after_tag']) if r['removed_after_tag'] is not None else False,
                        'last_missing_in_qbt_at': r['last_missing_in_qbt_at'],
                        'coverage_tag': r['coverage_tag'],
                        'last_action': r['last_action'],
                        'last_action_tag': r['last_action_tag'],
                        'last_action_at': r['last_action_at'],
                        'last_action_success': r['last_action_success'],
                        'last_action_error': r['last_action_error'],
                    })
                return results
            finally:
                conn.close()
        except Exception as e:
            self._warn(e)
            return []


# -------------------------
# Verification worker
# -------------------------
class VerificationScheduler:
    def __init__(self, db):
        self.db = db
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

    def verify_now(self):
        if not (self.db and self.db.ready):
            return {'last_verified_at': None, 'total': 0, 'exists': 0, 'missing': 0, 'last_verify_error': 'db_unavailable'}
        if not self.lock.acquire(blocking=False):
            return self.db.status()
        start = time.time()
        run_id = uuid.uuid4().hex
        ts = None
        error = None
        try:
            qb = get_qb_client()
            if not qb:
                error = "qBittorrent unavailable"
            else:
                try:
                    torrents = qb.torrents()
                except Exception as e:
                    error = f"Error fetching torrents: {e}"
                else:
                    existing_hashes = {t['hash'] for t in torrents}
                    known_hashes = self.db.list_hashes()
                    missing = known_hashes - existing_hashes
                    present = known_hashes & existing_hashes
                    ts = iso_utcnow()
                    self.db.update_existence(present, missing, ts)
                    duration_ms = int((time.time() - start) * 1000)
                    log(f"🔍 Verification complete: {len(present)} present, {len(missing)} missing in {duration_ms} ms.")
                    self.db.set_meta('last_verify_duration_ms', str(duration_ms))
        finally:
            ts = ts or iso_utcnow()
            self.db.set_meta('last_verified_at', ts)
            self.db.set_meta('last_verified_run_id', run_id)
            self.db.set_meta('last_verify_error', error or '')
            self.lock.release()
        return self.db.status()

    def start_loop(self, interval_seconds):
        def _loop():
            while not self.stop_event.is_set():
                try:
                    self.verify_now()
                except Exception as e:
                    log(f"⚠️ Verification loop error: {e}")
                self.stop_event.wait(interval_seconds)
        thread = threading.Thread(target=_loop, daemon=True)
        thread.start()
        return thread


# -------------------------
# Web UI
# -------------------------
WEB_UI_HTML = """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>No-hardlink-tagger — Assessments</title>
  <link rel=\"stylesheet\" href=\"https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css\" />
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #f6f6f6; }
    h1 { margin-bottom: 8px; }
    .status { margin-bottom: 12px; padding: 12px; background: #fff; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .status span { margin-right: 12px; }
    .filters { margin: 12px 0; padding: 12px; background: #fff; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); display: flex; flex-wrap: wrap; gap: 12px; }
    .filters label { display: flex; flex-direction: column; font-size: 12px; color: #555; }
    table.dataTable tbody tr { background: #fff; }
    .btn { padding: 6px 10px; border: 1px solid #1976d2; background: #2196f3; color: white; border-radius: 4px; cursor: pointer; }
    .btn:disabled { opacity: 0.6; cursor: not-allowed; }
    .error { color: #b00020; margin-left: 8px; }
  </style>
</head>
<body>
  <h1>No-hardlink-tagger assessments</h1>
  <div class=\"status\">
    <div><strong>Last verified at:</strong> <span id=\"last-verified\">never</span> <span id=\"verify-error\" class=\"error\"></span></div>
    <div><span id=\"count-total\">0</span> total &middot; <span id=\"count-exists\">0</span> exist &middot; <span id=\"count-missing\">0</span> missing</div>
    <div style=\"margin-top:8px;\"><button id=\"refresh-btn\" class=\"btn\">Refresh existence now</button></div>
  </div>

  <div class=\"filters\">
    <label>Exists
      <select id=\"filter-exists\">
        <option value=\"all\">All</option>
        <option value=\"exists\">Exists</option>
        <option value=\"missing\">Missing</option>
      </select>
    </label>
    <label>Missing
      <select id=\"filter-missing\">
        <option value=\"all\">All</option>
        <option value=\"yes\">Yes</option>
        <option value=\"no\">No</option>
      </select>
    </label>
    <label>Missing after tag
      <select id=\"filter-missing-after\">
        <option value=\"all\">All</option>
        <option value=\"yes\">Yes</option>
        <option value=\"no\">No</option>
      </select>
    </label>
    <label>Linked % min
      <input id=\"filter-linked-min\" type=\"number\" min=\"0\" max=\"100\" />
    </label>
    <label>Linked % max
      <input id=\"filter-linked-max\" type=\"number\" min=\"0\" max=\"100\" />
    </label>
  </div>

  <table id=\"assessments\" class=\"display\" style=\"width:100%\">
    <thead>
      <tr>
        <th>Name</th>
        <th>Size</th>
        <th>Hash</th>
        <th>Tracker</th>
        <th>Tags</th>
        <th>Last assessed</th>
        <th>% linked</th>
        <th>Cached?</th>
        <th>Exists?</th>
        <th>Missing?</th>
        <th>Missing after tag?</th>
        <th>Coverage tag</th>
        <th>Last action</th>
        <th>Last action tag</th>
        <th>Last action at</th>
        <th>Last action success</th>
        <th>Last action error</th>
      </tr>
    </thead>
  </table>

  <script src=\"https://code.jquery.com/jquery-3.7.1.min.js\"></script>
  <script src=\"https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js\"></script>
  <script>
    let table;

    function fmtBool(v) { return v ? 'Yes' : 'No'; }
    function fmtPct(v) { return (v === null || v === undefined) ? '' : v + '%'; }

    async function loadStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        document.getElementById('last-verified').innerText = data.last_verified_at || 'never';
        document.getElementById('count-total').innerText = data.total;
        document.getElementById('count-exists').innerText = data.exists;
        document.getElementById('count-missing').innerText = data.missing;
        document.getElementById('verify-error').innerText = data.last_verify_error || '';
      } catch (e) {
        document.getElementById('verify-error').innerText = 'Failed to load status';
      }
    }

    async function loadAssessments() {
      const res = await fetch('/api/assessments');
      const data = await res.json();
      table.clear();
      table.rows.add(data);
      table.draw();
    }

    function setupFilters() {
      $.fn.dataTable.ext.search.push(function(settings, data, dataIndex) {
        const row = table.row(dataIndex).data();
        if (!row) { return true; }
        const existsFilter = document.getElementById('filter-exists').value;
        if (existsFilter === 'exists' && !row.exists_in_qbt) return false;
        if (existsFilter === 'missing' && row.exists_in_qbt) return false;

        const missingFilter = document.getElementById('filter-missing').value;
        if (missingFilter === 'yes' && !row.missing) return false;
        if (missingFilter === 'no' && row.missing) return false;

        const missingAfterFilter = document.getElementById('filter-missing-after').value;
        if (missingAfterFilter === 'yes' && !row.missing_after_tag) return false;
        if (missingAfterFilter === 'no' && row.missing_after_tag) return false;

        const minPct = parseFloat(document.getElementById('filter-linked-min').value);
        const maxPct = parseFloat(document.getElementById('filter-linked-max').value);
        if (!Number.isNaN(minPct)) {
          if (row.linked_pct === null || row.linked_pct === undefined || row.linked_pct < minPct) return false;
        }
        if (!Number.isNaN(maxPct)) {
          if (row.linked_pct === null || row.linked_pct === undefined || row.linked_pct > maxPct) return false;
        }
        return true;
      });

      ['filter-exists', 'filter-missing', 'filter-missing-after', 'filter-linked-min', 'filter-linked-max'].forEach(id => {
        const el = document.getElementById(id);
        el.addEventListener('change', () => table.draw());
        el.addEventListener('keyup', () => table.draw());
      });
    }

    document.addEventListener('DOMContentLoaded', () => {
      table = $('#assessments').DataTable({
        data: [],
        columns: [
          { data: 'name' },
          { data: 'size_bytes', render: function(data, type, row) { return row.size_human; }, defaultContent: '' },
          { data: 'torrent_hash' },
          { data: 'tracker', defaultContent: '' },
          { data: 'tags', defaultContent: '' },
          { data: 'last_assessed_at', defaultContent: '' },
          { data: 'linked_pct', render: function(data) { return fmtPct(data); }, defaultContent: '' },
          { data: 'cached', render: function(data) { return fmtBool(data); }, defaultContent: '' },
          { data: 'exists_in_qbt', render: function(data) { return fmtBool(data); }, defaultContent: '' },
          { data: 'missing', render: function(data) { return fmtBool(data); }, defaultContent: '' },
          { data: 'missing_after_tag', render: function(data) { return fmtBool(data); }, defaultContent: '' },
          { data: 'coverage_tag', defaultContent: '' },
          { data: 'last_action', defaultContent: '' },
          { data: 'last_action_tag', defaultContent: '' },
          { data: 'last_action_at', defaultContent: '' },
          { data: 'last_action_success', render: function(data) { return data === null || data === undefined ? '' : fmtBool(data); }, defaultContent: '' },
          { data: 'last_action_error', defaultContent: '' },
        ],
        order: [[5, 'desc']],
        pageLength: 25,
      });

      setupFilters();
      loadStatus();
      loadAssessments();

      document.getElementById('refresh-btn').addEventListener('click', async () => {
        const btn = document.getElementById('refresh-btn');
        btn.disabled = true;
        btn.innerText = 'Refreshing...';
        try {
          await fetch('/api/refresh', { method: 'POST' });
          await loadStatus();
          await loadAssessments();
        } finally {
          btn.disabled = false;
          btn.innerText = 'Refresh existence now';
        }
      });
    });
  </script>
</body>
</html>
"""


class WebUIHandler(BaseHTTPRequestHandler):
    db = None
    verifier = None

    def _send_json(self, obj, status=200):
        payload = json.dumps(obj, separators=(',', ':')).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == '/':
            content = WEB_UI_HTML.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return
        if path == '/api/status':
            status_obj = self.db.status() if self.db else {
                'last_verified_at': None,
                'total': 0,
                'exists': 0,
                'missing': 0,
                'last_verify_error': None,
            }
            if status_obj.get('last_verify_error') == '':
                status_obj['last_verify_error'] = None
            self._send_json(status_obj)
            return
        if path == '/api/assessments':
            data = self.db.fetch_assessments() if self.db else []
            self._send_json(data)
            return
        self._send_json({'error': 'not_found'}, status=404)

    def do_POST(self):
        path = urlparse(self.path).path
        if path == '/api/refresh':
            if not self.verifier:
                self._send_json({'error': 'refresh_unavailable'}, status=503)
                return
            status_obj = self.verifier.verify_now()
            self._send_json(status_obj)
            return
        self._send_json({'error': 'not_found'}, status=404)

    def log_message(self, fmt, *args):
        log("[web] " + fmt % args)


def start_web_ui(db, verifier):
    WebUIHandler.db = db
    WebUIHandler.verifier = verifier
    server = ThreadingHTTPServer((WEB_UI_BIND, WEB_UI_PORT), WebUIHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log(f"🌐 Web UI listening on http://{WEB_UI_BIND}:{WEB_UI_PORT}")
    return server


# -------------------------
# Hash budget
# -------------------------
class Budget:
    def __init__(self, mib):
        self.total = int(mib) * 1024 * 1024
        self.remaining = self.total
        self.exhausted = False
    def need(self, nbytes):
        if self.remaining >= nbytes:
            self.remaining -= nbytes
            return True
        self.exhausted = True
        return False

def quick_hash_budgeted(path, budget, block=1024*1024):
    try:
        sz = os.path.getsize(path)
        need = min(block, sz) + (block if sz > block else 0)
        if not budget.need(need):
            return None
        h = hashlib.sha1()
        with open(path, 'rb') as f:
            data = f.read(min(block, sz)); h.update(data)
            if sz > block:
                try: f.seek(max(0, sz - block))
                except OSError: pass
                data2 = f.read(block); h.update(data2)
        return h.hexdigest()
    except Exception:
        return None

# -------------------------
# Stage 0: visibility guard
# -------------------------
def build_media_visibility_stats():
    files_count = 0
    errors = 0
    dirs_ok = 0
    dirs_missing = []
    start = time.time()
    for media_dir in MEDIA_DIRS:
        if not _dir_accessible(media_dir):
            dirs_missing.append(media_dir)
            continue
        dirs_ok += 1
        try:
            for _, _, files in os.walk(media_dir):
                files_count += len(files)
        except Exception:
            errors += 1
            continue
    secs = time.time() - start
    stats = {
        'files_count': files_count,
        'dirs_ok': dirs_ok,
        'dirs_missing': dirs_missing,
        'errors': errors,
        'elapsed': secs,
    }
    log(f"📁 Library visibility: files~{files_count}, dirs_ok={dirs_ok}/{len(MEDIA_DIRS)}, "
        f"missing={len(dirs_missing)}, errors={errors}, in {secs:.1f}s.")
    if dirs_missing:
        log(f"⚠ Missing/Unreadable media dirs: {', '.join(dirs_missing)}")
    ok = True
    if FAILSAFE_ENABLED:
        if (FAILSAFE_REQUIRE_DIRS == 'all' and dirs_ok < len(MEDIA_DIRS)) or \
           (FAILSAFE_REQUIRE_DIRS == 'any' and dirs_ok == 0) or \
           (files_count < FAILSAFE_MIN_MEDIA_FILES) or \
           (errors > FAILSAFE_MAX_INDEX_ERRORS):
            ok = False
    return ok, stats

# -------------------------
# Stage 1: enumerate torrents, filter & collect wanted sizes
# -------------------------
def build_active_inode_shield(qb, torrents):
    shield = set()
    protected = 0
    for t in torrents:
        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue
        if not (is_actively_seeding(t) or is_recently_active(t, ACTIVE_GRACE_MINUTES)):
            continue
        try:
            files = qb.get_torrent_files(t['hash'])
        except Exception:
            continue
        for fi in files:
            p = os.path.join(t['save_path'], fi['name'])
            try:
                st = os.stat(p)
            except Exception:
                continue
            if is_media_candidate(p, st.st_size):
                shield.add((st.st_dev, st.st_ino)); protected += 1
    if shield:
        log(f"🛡 Active inode shield: {len(shield)} unique files from {protected} media entries.")
    else:
        log("🛡 Active inode shield: empty.")
    return shield

def collect_torrent_candidates(qb, torrents, active_shield):
    """
    Returns:
      wanted_sizes: set of sizes we must index in MEDIA_DIRS
      t_candidates: {hash: [{'path':..., 'size':..., 'dev':..., 'ino':..., 'nlink':...}, ...]}
      meta: counters
    """
    wanted_sizes = set()
    t_candidates = {}
    skipped_active = skipped_recent = skipped_min_age = skipped_shield = 0
    for t in torrents:
        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue
        if is_actively_seeding(t):
            skipped_active += 1; continue
        if is_recently_active(t, ACTIVE_GRACE_MINUTES):
            skipped_recent += 1; continue
        if is_too_new(t, MIN_COMPLETED_AGE_HOURS):
            skipped_min_age += 1; continue

        try:
            files = qb.get_torrent_files(t['hash'])
        except Exception:
            files = []

        cand_list = []
        shield_hit = False
        for fi in files:
            p = os.path.join(t['save_path'], fi['name'])
            try:
                st = os.stat(p)
            except Exception:
                continue
            if not is_media_candidate(p, st.st_size):
                continue
            if ACTIVE_INODE_SHIELD and (st.st_dev, st.st_ino) in active_shield:
                shield_hit = True; break
            if st.st_nlink and st.st_nlink > 1:
                cand_list.append({'path': p, 'size': st.st_size, 'dev': st.st_dev,
                                  'ino': st.st_ino, 'nlink': st.st_nlink})
                wanted_sizes.add(st.st_size)

        if shield_hit:
            skipped_shield += 1
            continue

        if cand_list:
            t_candidates[t['hash']] = cand_list

    meta = dict(skipped_active=skipped_active,
                skipped_recent=skipped_recent,
                skipped_min_age=skipped_min_age,
                skipped_shield=skipped_shield)
    return wanted_sizes, t_candidates, meta

# -------------------------
# Stage 2: build media signature set with persistent cache
# -------------------------
def build_media_signature_set(wanted_sizes, budget):
    """
    Returns:
      sig_set: set of (size, quickhash)
      index_stats: dict with counts/timings + index_complete flag
      cache_updated: bool
    """
    sig_set = set()
    files_seen = set()
    hashed_new = 0
    cached_hits = 0
    errors = 0
    start = time.time()

    cache_ok = _ensure_dir(CACHE_DIR)
    media_cache_path = os.path.join(CACHE_DIR, 'media_hashes.json') if cache_ok else None
    media_cache = _load_json(media_cache_path, {"entries": {}, "dir_fingerprints": {}}) if media_cache_path else {"entries": {}, "dir_fingerprints": {}}

    entries = media_cache.get("entries", {})
    dir_fingerprints = media_cache.get("dir_fingerprints", {})

    # Walk only wanted sizes
    for media_dir in MEDIA_DIRS:
        if not _dir_accessible(media_dir):
            continue
        for root, dirs, files in os.walk(media_dir):
            fp = _dir_fingerprint(root, entry_count=len(files) + len(dirs))
            cached_fp = dir_fingerprints.get(root)
            if fp is not None:
                dir_fingerprints[root] = fp
            if fp is not None and cached_fp is not None and fp == cached_fp and root not in files_seen:
                prefix = root + os.sep
                for path, ent in entries.items():
                    if not path.startswith(prefix):
                        continue
                    sz = ent.get('size')
                    qh = ent.get('qhash')
                    if sz not in wanted_sizes or not qh:
                        continue
                    if not is_media_candidate(path, sz):
                        continue
                    files_seen.add(path)
                    sig_set.add((sz, qh))
                    cached_hits += 1
                files_seen.add(root)
                dirs[:] = []
                continue
            for fn in files:
                path = os.path.join(root, fn)
                try:
                    st = os.stat(path)
                except Exception:
                    errors += 1; continue
                sz = st.st_size
                if sz not in wanted_sizes:
                    continue
                if not is_media_candidate(path, sz):
                    continue

                key = path
                files_seen.add(key)
                ent = entries.get(key)
                # cache valid?
                if ent and ent.get('size') == sz and ent.get('mtime') == int(st.st_mtime) \
                   and ent.get('ino') == st.st_ino and ent.get('dev') == st.st_dev \
                   and ent.get('qhash'):
                    sig_set.add((sz, ent['qhash'])); cached_hits += 1
                    continue

                # compute quickhash (budgeted)
                qh = quick_hash_budgeted(path, budget)
                if not qh:
                    # no hash -> cannot include in signature set
                    continue
                hashed_new += 1
                entries[key] = {
                    'size': sz, 'mtime': int(st.st_mtime),
                    'ino': st.st_ino, 'dev': st.st_dev, 'qhash': qh
                }
                sig_set.add((sz, qh))

    # prune removed files from cache
    removed = 0
    if entries:
        for k in list(entries.keys()):
            if k not in files_seen and entries[k].get('size') in wanted_sizes:
                entries.pop(k, None); removed += 1

    media_cache['entries'] = entries
    media_cache['dir_fingerprints'] = dir_fingerprints
    cache_updated = False
    if media_cache_path:
        try:
            _atomic_save_json(media_cache_path, media_cache)
            cache_updated = True
        except Exception:
            pass

    secs = time.time() - start
    index_complete = not budget.exhausted  # if budget ran out, we might have missed hashes
    index_stats = {
        'sig_count': len(sig_set),
        'cached_hits': cached_hits,
        'hashed_new': hashed_new,
        'cache_pruned': removed,
        'errors': errors,
        'elapsed': secs,
        'index_complete': index_complete,
        'budget_used_mb': (budget.total - budget.remaining) // (1024*1024),
        'budget_total_mb': budget.total // (1024*1024),
    }
    log(f"🔎 Media signatures: sigs={len(sig_set)}, cached={cached_hits}, new_hashes={hashed_new}, "
        f"pruned={removed}, errors={errors}, in {secs:.1f}s, budget={index_stats['budget_used_mb']}/{index_stats['budget_total_mb']} MiB.")
    if not index_complete:
        log("⚠ Signature index incomplete (hash budget exhausted). Will skip tagging where a signature match is required.")
    return sig_set, index_stats, cache_updated

# -------------------------
# Decision cache (per torrent)
# -------------------------
def load_torrent_cache():
    path = os.path.join(CACHE_DIR, 'torrent_state.json') if _ensure_dir(CACHE_DIR) else None
    raw = _load_json(path, {}) if path else {}
    current_cfg = {
        'media_link_min_percent': MEDIA_LINK_MIN_PERCENT,
        'media_link_tag_steps': MEDIA_LINK_TAG_STEPS,
    }

    if isinstance(raw, dict) and 'entries' in raw:
        entries = raw.get('entries') or {}
        cached_cfg = raw.get('config') or {}
    elif isinstance(raw, dict):
        entries = raw
        cached_cfg = {}
    else:
        entries = {}
        cached_cfg = {}

    if cached_cfg != current_cfg and entries:
        log("ℹ️ Torrent cache config changed, invalidating entries.")
        entries = {}

    return path, {'entries': entries, 'config': current_cfg}

def save_torrent_cache(path, data):
    if not path:
        return
    payload = data
    if not isinstance(payload, dict) or 'entries' not in payload:
        payload = {'entries': data}
    payload.setdefault('config', {
        'media_link_min_percent': MEDIA_LINK_MIN_PERCENT,
        'media_link_tag_steps': MEDIA_LINK_TAG_STEPS,
    })
    try:
        _atomic_save_json(path, payload)
    except Exception:
        pass

def can_reuse_decision(t, tstate, existing_tags):
    if DECISION_TTL_HOURS <= 0:
        return False
    entries = tstate.get('entries', tstate)
    entry = entries.get(t.get('hash'))
    if not entry:
        return False
    if entry.get('completion_on') != t.get('completion_on'):
        return False
    if entry.get('save_path') != t.get('save_path'):
        return False
    if int(time.time()) - entry.get('decided_at', 0) > DECISION_TTL_HOURS * 3600:
        return False

    coverage_tags_present = {tag for tag in existing_tags if tag.startswith(MEDIA_LINK_TAG_PREFIX)}
    expected_coverage_tag = entry.get('coverage_tag')
    coverage_ok = coverage_tags_present == ({expected_coverage_tag} if expected_coverage_tag else set())

    decision = entry.get('decision')
    if decision == 'orphan':
        decision_ok = ORPHAN_TAG in existing_tags
    elif decision == 'linked':
        decision_ok = ORPHAN_TAG not in existing_tags
    else:
        decision_ok = False

    return decision_ok and coverage_ok

def remember_decision(t, tstate, decision, coverage_pct=None, coverage_tag=None):
    entries = tstate.setdefault('entries', {})
    entries[t['hash']] = {
        'completion_on': t.get('completion_on'),
        'save_path': t.get('save_path'),
        'decision': decision,  # 'linked' or 'orphan'
        'decided_at': int(time.time()),
        'coverage_pct': coverage_pct,
        'coverage_tag': coverage_tag,
    }

# -------------------------
# Tag verification helpers
# -------------------------
def verify_tag_state(qb, hashes, tag, expect_present):
    successes = []
    failures = []
    for h in hashes:
        try:
            tinfo = qb.get_torrent(h)
        except Exception:
            failures.append((h, 'api_error'))
            continue
        if not tinfo:
            failures.append((h, 'torrent_missing'))
            continue
        tags = set((tinfo.get('tags') or '').split(',')) if tinfo.get('tags') else set()
        has_tag = tag in tags
        if has_tag == expect_present:
            successes.append(h)
        else:
            failures.append((h, 'missing_tag' if not has_tag else 'unexpected_tag'))
    return successes, failures


def _update_assessment_row_for_action(assessment_rows, torrent_hash, action, tag, ts_now, success, error):
    if assessment_rows is None:
        return
    row = assessment_rows.get(torrent_hash)
    if row is None:
        return
    row['last_action'] = action
    row['last_action_tag'] = tag
    row['last_action_at'] = ts_now
    row['last_action_success'] = success
    row['last_action_error'] = error
    row['_action_logged'] = True
    row['_run_action'] = {
        'action_taken': action,
        'action_tag': tag,
        'action_success': success,
        'action_error': error,
    }


def _append_history_from_action(history_rows, assessment_rows, torrent_hash, action, tag, assessed_at, success, error, run_id):
    if history_rows is None or assessment_rows is None:
        return
    row = assessment_rows.get(torrent_hash)
    if row is None:
        return
    history_rows.append({
        'run_id': run_id,
        'assessed_at': assessed_at,
        'torrent_hash': torrent_hash,
        'name': row.get('name'),
        'size_bytes': row.get('size_bytes'),
        'tracker': row.get('tracker'),
        'tags': row.get('tags'),
        'linked_pct': row.get('linked_pct'),
        'coverage_tag': row.get('coverage_tag'),
        'cached': row.get('cached'),
        'exists_in_qbt': True,
        'action_taken': action,
        'action_tag': tag,
        'action_success': success,
        'action_error': error,
    })

def apply_and_log_tag_changes(qb, action, tag, hashes, name_lookup, coverage_info, run_id, seq_ref, http_func,
                              assessment_rows=None, history_rows=None, assessed_at=None):
    if not hashes:
        return 0
    http_ok = http_func(hashes, tag)
    expect_present = action == 'tag'
    successes, failures = verify_tag_state(qb, hashes, tag, expect_present)
    seen = {h for h in successes}
    seen.update(h for h, _ in failures)
    for h in hashes:
        if h not in seen:
            failures.append((h, 'verify_failed' if http_ok else 'http_error'))
    entries = []
    ts_now = iso_utcnow()
    for h in successes:
        seq_ref[0] += 1
        cov = coverage_info.get(h, {}) if coverage_info else {}
        err = None
        entries.append({
            'ts': _timestamp(),
            'run_id': run_id,
            'seq': seq_ref[0],
            'action': action,
            'tag': tag,
            'hash': h,
            'name': name_lookup.get(h, {}).get('name'),
            'coverage_pct': cov.get('coverage_pct'),
            'coverage_tag': cov.get('coverage_tag'),
            'success': True,
            'error': err,
        })
        _update_assessment_row_for_action(assessment_rows, h, action, tag, ts_now, True, err)
        _append_history_from_action(history_rows, assessment_rows, h, action, tag, assessed_at or ts_now, True, err, run_id)
    for h, reason in failures:
        seq_ref[0] += 1
        cov = coverage_info.get(h, {}) if coverage_info else {}
        err = reason or ('http_error' if not http_ok else 'verify_failed')
        entries.append({
            'ts': _timestamp(),
            'run_id': run_id,
            'seq': seq_ref[0],
            'action': action,
            'tag': tag,
            'hash': h,
            'name': name_lookup.get(h, {}).get('name'),
            'coverage_pct': cov.get('coverage_pct'),
            'coverage_tag': cov.get('coverage_tag'),
            'success': False,
            'error': err,
        })
        _update_assessment_row_for_action(assessment_rows, h, action, tag, ts_now, False, err)
        _append_history_from_action(history_rows, assessment_rows, h, action, tag, assessed_at or ts_now, False, err, run_id)
    if entries:
        log_actions(entries)
    return len(successes)

# -------------------------
# Evaluate torrents with sig set
# -------------------------
def evaluate_and_tag(qb, torrents, t_candidates, sig_set, index_complete, tstate, torrent_lookup, run_id, assessment_rows, history_rows, assessed_at):
    orphan_batch, untag_batch = [], []
    total_tagged = total_untagged = 0
    skipped_reuse = skipped_inconclusive = 0
    seq_ref = [0]
    coverage_info = {}

    coverage_add = defaultdict(list)  # tag -> [hashes]
    coverage_remove = defaultdict(list)  # tag -> [hashes]

    for i, t in enumerate(torrents, 1):
        if MAX_TORRENTS > 0 and i > MAX_TORRENTS:
            break
        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue
        h = t['hash']
        cand_files = t_candidates.get(h)
        if not cand_files:
            # no eligible files -> treat as "not linked" only if we choose to; safer to require evidence
            continue

        existing_tags = set((t.get('tags') or '').split(',')) if t.get('tags') else set()
        row = assessment_rows.get(h) if assessment_rows else None

        # decision reuse? only if tags already reflect cached decision/coverage
        if can_reuse_decision(t, tstate, existing_tags):
            if row is not None:
                row['cached'] = 1
                cache_entry = (tstate.get('entries') or {}).get(h) or {}
                row['linked_pct'] = cache_entry.get('coverage_pct')
                row['coverage_tag'] = cache_entry.get('coverage_tag')
            skipped_reuse += 1
            continue

        # If our media signatures are incomplete, be conservative: skip
        if not index_complete:
            skipped_inconclusive += 1
            continue

        # Check each candidate torrent file: (size, qhash) in sig_set ?
        linked_matches = 0
        linked_bytes = 0
        total_bytes = sum(cf['size'] for cf in cand_files)
        inconclusive = False
        for cf in cand_files:
            tqh = quick_hash_budgeted(cf['path'], TORRENT_HASH_BUDGET)
            if not tqh:
                inconclusive = True
                break
            if (cf['size'], tqh) in sig_set:
                linked_matches += 1
                linked_bytes += cf['size']

        if inconclusive:
            skipped_inconclusive += 1
            continue

        coverage_pct = int((linked_bytes / total_bytes) * 100) if total_bytes > 0 else 0
        linked_enough = linked_bytes > 0 and coverage_pct >= MEDIA_LINK_MIN_PERCENT

        # Optional coverage tags (best matching threshold)
        coverage_tag = None
        if MEDIA_LINK_TAG_STEPS:
            for step in MEDIA_LINK_TAG_STEPS:
                if coverage_pct >= step:
                    coverage_tag = f"{MEDIA_LINK_TAG_PREFIX}{step}%"
                else:
                    break

        coverage_tags_present = {tag for tag in existing_tags if tag.startswith(MEDIA_LINK_TAG_PREFIX)}

        if coverage_tag and coverage_tag not in existing_tags:
            coverage_add[coverage_tag].append(h)
        for tag in coverage_tags_present:
            if tag != coverage_tag:
                coverage_remove[tag].append(h)

        if row is not None:
            row['linked_pct'] = coverage_pct
            row['coverage_tag'] = coverage_tag
            row.setdefault('cached', 0)

        has_tag = ORPHAN_TAG in (t.get('tags') or '')
        if not linked_enough:
            orphan_batch.append(h)
            coverage_info[h] = {'coverage_pct': coverage_pct, 'coverage_tag': coverage_tag}
            if len(orphan_batch) >= BATCH_SIZE:
                total_tagged += apply_and_log_tag_changes(qb, 'tag', ORPHAN_TAG, orphan_batch, torrent_lookup, coverage_info, run_id, seq_ref, add_tag_http, assessment_rows, history_rows, assessed_at)
                orphan_batch.clear()
            remember_decision(t, tstate, 'orphan', coverage_pct, coverage_tag)
        else:
            if has_tag:
                untag_batch.append(h)
                coverage_info[h] = {'coverage_pct': coverage_pct, 'coverage_tag': coverage_tag}
                if len(untag_batch) >= BATCH_SIZE:
                    total_untagged += apply_and_log_tag_changes(qb, 'untag', ORPHAN_TAG, untag_batch, torrent_lookup, coverage_info, run_id, seq_ref, remove_tag_http, assessment_rows, history_rows, assessed_at)
                    untag_batch.clear()
            remember_decision(t, tstate, 'linked', coverage_pct, coverage_tag)

    if orphan_batch:
        total_tagged += apply_and_log_tag_changes(qb, 'tag', ORPHAN_TAG, orphan_batch, torrent_lookup, coverage_info, run_id, seq_ref, add_tag_http, assessment_rows, history_rows, assessed_at)
    if untag_batch:
        total_untagged += apply_and_log_tag_changes(qb, 'untag', ORPHAN_TAG, untag_batch, torrent_lookup, coverage_info, run_id, seq_ref, remove_tag_http, assessment_rows, history_rows, assessed_at)

    # Apply coverage tags
    for tag, hashes in coverage_add.items():
        apply_and_log_tag_changes(qb, 'tag', tag, hashes, torrent_lookup, coverage_info, run_id, seq_ref, add_tag_http, assessment_rows, history_rows, assessed_at)
    for tag, hashes in coverage_remove.items():
        apply_and_log_tag_changes(qb, 'untag', tag, hashes, torrent_lookup, coverage_info, run_id, seq_ref, remove_tag_http, assessment_rows, history_rows, assessed_at)

    return {
        'tagged': total_tagged,
        'untagged': total_untagged,
        'skipped_reuse': skipped_reuse,
        'skipped_inconclusive': skipped_inconclusive
    }

# Single budget instance used across run (media + torrents)
TORRENT_HASH_BUDGET = None  # will be set per run

def run_cleanup(db=None):
    global TORRENT_HASH_BUDGET
    if not all([QBITTORRENT_URL, QBITTORRENT_USER, QBITTORRENT_PASS]):
        raise SystemExit("Missing qBittorrent env vars.")
    run_id = uuid.uuid4().hex
    assessed_at = iso_utcnow()
    log(f"{VERSION} — url={QBITTORRENT_URL} — MIN_SIZE_MB={MIN_SIZE_MB} — EXT_WHITELIST={','.join(EXT_WHITELIST)} "
        f"— ACTIVE_GRACE_MINUTES={ACTIVE_GRACE_MINUTES} — MIN_COMPLETED_AGE_HOURS={MIN_COMPLETED_AGE_HOURS} "
        f"— ACTIVE_INODE_SHIELD={int(ACTIVE_INODE_SHIELD)} — HASH_BUDGET_MB={HASH_BUDGET_MB} "
        f"— DECISION_TTL_HOURS={DECISION_TTL_HOURS} — CACHE_DIR={CACHE_DIR} "
        f"— MEDIA_LINK_MIN_PERCENT={MEDIA_LINK_MIN_PERCENT} "
        f"— MEDIA_LINK_TAG_STEPS={MEDIA_LINK_TAG_STEPS if MEDIA_LINK_TAG_STEPS else 'disabled'} "
        f"— MEDIA_LINK_TAG_PREFIX='{MEDIA_LINK_TAG_PREFIX}' "
        f"— LOG_USE_AMPM={int(LOG_USE_AMPM)}")
    log("Starting cleanup cycle...")

    qb = get_qb_client()
    if not qb:
        log("No connection to qBittorrent, skipping.")
        return

    vis_ok, _ = build_media_visibility_stats()
    if not vis_ok:
        log("🛑 FAILSAFE: Library visibility not healthy. **No tag changes this cycle.**")
        return

    try:
        torrents = qb.torrents()
    except Exception as e:
        log(f"Error fetching torrents: {e}")
        return

    if MAX_TORRENTS > 0:
        torrents = torrents[:MAX_TORRENTS]
        log(f"⚙ Limiting to first {MAX_TORRENTS} torrents.")

    torrent_lookup = {t['hash']: {'name': t.get('name'), 'save_path': t.get('save_path')} for t in torrents}
    assessment_rows = {}
    history_rows = []
    for t in torrents:
        h = t['hash']
        assessment_rows[h] = {
            'torrent_hash': h,
            'name': t.get('name'),
            'size_bytes': t.get('total_size') or t.get('size') or 0,
            'tracker': t.get('tracker'),
            'tags': t.get('tags') or '',
            'last_assessed_at': assessed_at,
            'linked_pct': None,
            'coverage_tag': None,
            'cached': 0,
            'exists_in_qbt': 1,
            'last_seen_in_qbt_at': assessed_at,
            'last_missing_in_qbt_at': None,
            'removed': 0,
            'removed_after_tag': 0,
            'last_action': None,
            'last_action_tag': None,
            'last_action_at': None,
            'last_action_success': None,
            'last_action_error': None,
        }

    # Build active inode shield
    active_shield = build_active_inode_shield(qb, torrents) if ACTIVE_INODE_SHIELD else set()

    # Stage 1: filter + collect wanted sizes and candidate files
    wanted_sizes, t_candidates, meta = collect_torrent_candidates(qb, torrents, active_shield)
    log(f"🎯 Stage1: wanted_sizes={len(wanted_sizes)}, candidates={len(t_candidates)} torrents; "
        f"skipped_active={meta['skipped_active']}, skipped_recent={meta['skipped_recent']}, "
        f"skipped_min_age={meta['skipped_min_age']}, shield_skips={meta['skipped_shield']}.")

    # Budget: we split between media and torrents dynamically; start with full, consume as we go
    budget = Budget(HASH_BUDGET_MB)
    # Stage 2: build media signature set only for sizes we actually care about
    sig_set, idx_stats, _ = build_media_signature_set(wanted_sizes, budget)

    # Whatever remains in the budget is available for torrent quickhashes
    TORRENT_HASH_BUDGET = budget  # pass the same budget into torrent hashing

    # Load decision cache
    tcache_path, tcache = load_torrent_cache()

    # Stage 3: evaluate + tag using signature set
    results = evaluate_and_tag(qb, torrents, t_candidates, sig_set, idx_stats['index_complete'], tcache, torrent_lookup, run_id, assessment_rows, history_rows, assessed_at)

    # Save decision cache
    save_torrent_cache(tcache_path, tcache)

    # Add base history rows for torrents without action entries
    for h, row in assessment_rows.items():
        if row.get('_action_logged'):
            continue
        history_rows.append({
            'run_id': run_id,
            'assessed_at': assessed_at,
            'torrent_hash': h,
            'name': row.get('name'),
            'size_bytes': row.get('size_bytes'),
            'tracker': row.get('tracker'),
            'tags': row.get('tags'),
            'linked_pct': row.get('linked_pct'),
            'coverage_tag': row.get('coverage_tag'),
            'cached': row.get('cached'),
            'exists_in_qbt': True,
            'action_taken': None,
            'action_tag': None,
            'action_success': None,
            'action_error': None,
        })

    if db and db.ready:
        db.append_history(history_rows)
        db.upsert_torrents(list(assessment_rows.values()))

    log(f"📊 Summary: tagged={results['tagged']}, untagged={results['untagged']}, "
        f"reuse_skips={results['skipped_reuse']}, inconclusive_skips={results['skipped_inconclusive']}, "
        f"budget_used={idx_stats['budget_used_mb']}/{idx_stats['budget_total_mb']} MiB.")
    log("Cleanup cycle complete.")

if __name__ == "__main__":
    db = AssessmentDB(ASSESSMENTS_DB_PATH)
    verifier = VerificationScheduler(db) if db and db.ready else None
    if VERIFY_ENABLED and verifier:
        verifier.start_loop(VERIFY_INTERVAL_SECONDS)
        log(f"🧭 Verification loop enabled every {VERIFY_INTERVAL_SECONDS}s.")
    elif VERIFY_ENABLED:
        log("⚠️ Verification enabled but database not ready; skipping loop.")

    if WEB_UI_ENABLED:
        if db and db.ready:
            start_web_ui(db, verifier)
        else:
            log("⚠️ Web UI enabled but database not ready; server not started.")

    while True:
        try:
            run_cleanup(db)
        except Exception as e:
            log(f"💥 Unhandled error: {e}")
        log(f"Waiting {DEBUG_INTERVAL} seconds before next run...")
        time.sleep(DEBUG_INTERVAL)
