import os
import time
from datetime import datetime
from qbittorrent import Client
import requests
import hashlib
from collections import defaultdict

VERSION = "no-hardlink-tagger v2.0 — visibility guard + strict seeding"

# --- Config from env ---
QBITTORRENT_URL  = os.environ.get('QBITTORRENT_URL', '').rstrip('/')
QBITTORRENT_USER = os.environ.get('QBITTORRENT_USER')
QBITTORRENT_PASS = os.environ.get('QBITTORRENT_PASS')

ORPHAN_TAG    = os.environ.get('ORPHAN_TAG', 'NoMediaLink')
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')
MEDIA_DIRS    = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]

DEBUG_INTERVAL = int(os.environ.get('DEBUG_INTERVAL', '60'))
BATCH_SIZE     = int(os.environ.get('BATCH_SIZE', '25'))
MAX_TORRENTS   = int(os.environ.get('MAX_TORRENTS', '0'))      # 0 = all

# Only treat these as "media files" for linkage detection
EXT_WHITELIST  = [e.strip().lower() for e in os.environ.get(
    'EXT_WHITELIST',
    '.mkv,.mp4,.m4v,.mov,.avi,.ts,.m2ts,.mpg,.mpeg,.wmv'
).split(',') if e.strip()]
MIN_SIZE_MB    = int(os.environ.get('MIN_SIZE_MB', '50'))      # ignore files smaller than this

# --- Visibility guard knobs ---
FAILSAFE_ENABLED           = os.environ.get('FAILSAFE_ENABLED', '1') not in ('0', 'false', 'False')
FAILSAFE_REQUIRE_DIRS      = os.environ.get('FAILSAFE_REQUIRE_DIRS', 'any').lower()  # 'any' or 'all'
FAILSAFE_MIN_MEDIA_FILES   = int(os.environ.get('FAILSAFE_MIN_MEDIA_FILES', '100'))  # min files indexed
FAILSAFE_MAX_INDEX_ERRORS  = int(os.environ.get('FAILSAFE_MAX_INDEX_ERRORS', '200')) # stat/list errors cap

if not all([QBITTORRENT_URL, QBITTORRENT_USER, QBITTORRENT_PASS]):
    def log(msg): print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")
    log("Error: Missing required qBittorrent environment variables.")
    time.sleep(9999); raise SystemExit(1)

last_checked_completion_time = {}

def log(msg):
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}", flush=True)

# --- qB read (list) ---
def get_qb_client():
    try:
        qb = Client(QBITTORRENT_URL)
        qb.login(QBITTORRENT_USER, QBITTORRENT_PASS)
        return qb
    except Exception as e:
        log(f"Error connecting to qBittorrent: {e}")
        return None

# --- HTTP write (tags) ---
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
    if hashes:
        if _api_post('torrents/addTags', {'hashes': '|'.join(hashes), 'tags': tag}):
            log(f"✅ addTags OK: tagged {len(hashes)} torrent(s) with '{tag}'.")

def remove_tag_http(hashes, tag):
    if hashes:
        if _api_post('torrents/removeTags', {'hashes': '|'.join(hashes), 'tags': tag}):
            log(f"🗑 removeTags OK: removed '{tag}' from {len(hashes)} torrent(s).")

# --- Strict seeding detection ---
def is_actively_seeding(t):
    """
    Active ONLY when state is one of: uploading, forcedUP, checkingUP, queuedUP.
    Everything else (including stalledUP, pausedUP) is NOT active.
    """
    s = (t.get('state') or '').strip().lower()
    return s in {'uploading', 'forcedup', 'checkingup', 'queuedup'}

# --- Helpers for nlink + content ---
def is_media_candidate(path, size_bytes):
    if size_bytes < MIN_SIZE_MB * 1024 * 1024:
        return False
    ext = os.path.splitext(path)[1].lower()
    return (ext in EXT_WHITELIST) if EXT_WHITELIST else True

def quick_hash(path, block=1024*1024):
    """Hash first and last 1MB. Fast, sufficient to disambiguate same-size files."""
    h = hashlib.sha1()
    try:
        sz = os.path.getsize(path)
        with open(path, 'rb') as f:
            data = f.read(block); h.update(data)
            if sz > block:
                try: f.seek(max(0, sz - block))
                except OSError: pass
                data2 = f.read(block); h.update(data2)
        return h.hexdigest()
    except Exception:
        return None

# --- Visibility guard + media index (size -> paths), with stats ---
def _dir_accessible(p):
    try:
        if not os.path.isdir(p): return False
        # Need both read and execute (traverse) perms
        if not (os.access(p, os.R_OK) and os.access(p, os.X_OK)): return False
        # Try listing a single entry to catch FUSE/permission quirks
        with os.scandir(p) as it:
            for _ in it:
                break
        return True
    except Exception:
        return False

def build_media_size_map_with_stats():
    """
    Returns (size_map, stats) where:
      size_map: {size_bytes: [paths,...]} for media candidates
      stats: {
        'files_count': int,
        'dirs_ok': int,
        'dirs_missing': [path,...],
        'errors': int,
        'distinct_sizes': int,
      }
    """
    size_map = defaultdict(list)
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
        # Walk this dir
        try:
            for root, _, files in os.walk(media_dir):
                for fn in files:
                    path = os.path.join(root, fn)
                    try:
                        st = os.stat(path)
                    except FileNotFoundError:
                        continue
                    except Exception:
                        errors += 1
                        continue
                    if not is_media_candidate(path, st.st_size):
                        continue
                    size_map[st.st_size].append(path)
                    files_count += 1
        except Exception:
            errors += 1
            continue

    secs = time.time() - start
    stats = {
        'files_count': files_count,
        'dirs_ok': dirs_ok,
        'dirs_missing': dirs_missing,
        'errors': errors,
        'distinct_sizes': len(size_map),
        'elapsed': secs,
    }
    log(f"📚 Media size index: files={files_count}, sizes={len(size_map)}, "
        f"dirs_ok={dirs_ok}/{len(MEDIA_DIRS)}, missing={len(dirs_missing)}, "
        f"errors={errors}, in {secs:.1f}s.")
    if dirs_missing:
        log(f"⚠ Missing/Unreadable media dirs: {', '.join(dirs_missing)}")
    return size_map, stats

def linked_to_library(qb, t, media_size_map, media_hash_cache):
    """
    Returns True if ANY qualifying torrent file:
      - has st_nlink > 1 (i.e., is hardlinked somewhere), AND
      - content-matches (size + quick hash) at least one file under MEDIA_DIRS.
    """
    try:
        files = qb.get_torrent_files(t['hash'])
    except Exception as e:
        log(f"⚠ Could not fetch files for '{t['name']}': {e}")
        files = []

    for fi in files:
        torrent_path = os.path.join(t['save_path'], fi['name'])
        try:
            st = os.stat(torrent_path)
        except FileNotFoundError:
            continue
        except Exception:
            continue

        if not is_media_candidate(torrent_path, st.st_size):
            continue
        if not st.st_nlink or st.st_nlink <= 1:
            continue

        candidates = media_size_map.get(st.st_size)
        if not candidates:
            continue

        tqh = quick_hash(torrent_path)
        if not tqh:
            continue

        for lib_path in candidates:
            if lib_path in media_hash_cache:
                lqh = media_hash_cache[lib_path]
            else:
                lqh = quick_hash(lib_path)
                media_hash_cache[lib_path] = lqh
            if lqh and lqh == tqh:
                return True  # confirmed match in MEDIA_DIRS

    return False

def _visibility_is_ok(stats):
    """
    Decide if the media library visibility looks healthy enough to allow tag changes.
    """
    if not FAILSAFE_ENABLED:
        return True, "failsafe disabled"

    # Require some directories to be visible
    if FAILSAFE_REQUIRE_DIRS == 'all':
        if stats['dirs_ok'] < len(MEDIA_D]()
