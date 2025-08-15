import os
import time
from datetime import datetime
from qbittorrent import Client
import requests
import hashlib
from collections import defaultdict

VERSION = "no-hardlink-tagger v1.9 — seeding-aware (nlink + content)"

# --- Config from env ---
QBITTORRENT_URL  = os.environ.get('QBITTORRENT_URL', '').rstrip('/')
QBITTORRENT_USER = os.environ.get('QBITTORRENT_USER')
QBITTORRENT_PASS = os.environ.get('QBITTORRENT_PASS')

ORPHAN_TAG    = os.environ.get('ORPHAN_TAG', 'NoMediaLink')
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')
MEDIA_DIRS    = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]

DEBUG_INTERVAL = int(os.environ.get('DEBUG_INTERVAL', '60'))
BATCH_SIZE     = int(os.environ.get('BATCH_SIZE', '25'))
MAX_TORRENTS   = int(os.environ.get('MAX_TORRENTS', '0'))  # 0 = all

# Only treat these as "media files" for linkage detection
EXT_WHITELIST  = [e.strip().lower() for e in os.environ.get(
    'EXT_WHITELIST',
    '.mkv,.mp4,.m4v,.mov,.avi,.ts,.m2ts,.mpg,.mpeg,.wmv'
).split(',') if e.strip()]

MIN_SIZE_MB    = int(os.environ.get('MIN_SIZE_MB', '50'))  # ignore files smaller than this

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

# --- Seeding detection ---
def is_actively_seeding(t):
    """
    Strict seeding detection:
      active ONLY when state is one of:
        uploading, forcedUP, checkingUP, queuedUP, pausedUP
      Everything else (including stalledUP) is NOT active.
    """
    s = (t.get('state') or '').strip()
    if not s:
        return False
    s_norm = s.lower()  # normalize: 'forcedUP' -> 'forcedup'
    active_states = {'uploading', 'forcedup', 'checkingup', 'queuedup', 'pausedup'}
    return s_norm in active_states

# --- Helpers for v1.8 (nlink + content) ---
def is_media_candidate(path, size_bytes):
    if size_bytes < MIN_SIZE_MB * 1024 * 1024:
        return False
    ext = os.path.splitext(path)[1].lower()
    return (ext in EXT_WHITELIST) if EXT_WHITELIST else True

def quick_hash(path, block=1024*1024):
    """Hash first and last 1MB. Fast, good enough to disambiguate same-size files."""
    h = hashlib.sha1()
    try:
        sz = os.path.getsize(path)
        with open(path, 'rb') as f:
            data = f.read(block)
            h.update(data)
            if sz > block:
                try:
                    f.seek(max(0, sz - block))
                except OSError:
                    pass
                data2 = f.read(block)
                h.update(data2)
        return h.hexdigest()
    except Exception:
        return None

def build_media_size_map():
    """Map size -> list(paths) for media files; hashes computed lazily per candidate."""
    size_map = defaultdict(list)
    files_count = 0
    start = time.time()
    for media_dir in MEDIA_DIRS:
        for root, _, files in os.walk(media_dir):
            for fn in files:
                path = os.path.join(root, fn)
                try:
                    st = os.stat(path)
                except FileNotFoundError:
                    continue
                except Exception:
                    continue
                if not is_media_candidate(path, st.st_size):
                    continue
                size_map[st.st_size].append(path)
                files_count += 1
    secs = time.time() - start
    log(f"📚 Media size index built: {files_count} files across {len(MEDIA_DIRS)} dir(s) in {secs:.1f}s "
        f"({len(size_map)} distinct sizes).")
    return size_map

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

        # must be hardlinked *somewhere*
        if not st.st_nlink or st.st_nlink <= 1:
            continue

        # find library candidates by size
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

def run_cleanup():
    log(f"{VERSION} — url={QBITTORRENT_URL} — MIN_SIZE_MB={MIN_SIZE_MB} — EXT_WHITELIST={','.join(EXT_WHITELIST)}")
    log("Starting cleanup cycle...")

    qb = get_qb_client()
    if not qb:
        log("No connection to qBittorrent, skipping this cycle.")
        return

    media_size_map = build_media_size_map()
    media_hash_cache = {}

    try:
        torrents = qb.torrents()
    except Exception as e:
        log(f"Error fetching torrents: {e}")
        return

    if MAX_TORRENTS > 0:
        torrents = torrents[:MAX_TORRENTS]
        log(f"⚙ Limiting to first {MAX_TORRENTS} torrents (MAX_TORRENTS).")

    orphan_batch, untag_batch = [], []

    for i, t in enumerate(torrents, 1):
        if i % 50 == 0:
            log(f"…processed {i}/{len(torrents)} torrents.")

        # Only consider torrents saved under DOWNLOADS_DIR
        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue

        # NEW: exclude active seeders from any tag changes
        if is_actively_seeding(t):
            log(f"⏩ Skipping '{t['name']}' (actively seeding: state={t.get('state')}). Leaving tags unchanged.")
            continue

        has_orphan_tag = ORPHAN_TAG in t.get('tags', '')
        completion_time = t.get('completion_on', 0)

        # Skip unchanged orphans to save time
        if has_orphan_tag and completion_time == last_checked_completion_time.get(t['hash']):
            continue

        linked = linked_to_library(qb, t, media_size_map, media_hash_cache)

        if not linked:
            orphan_batch.append(t['hash'])
            if len(orphan_batch) >= BATCH_SIZE:
                add_tag_http(orphan_batch, ORPHAN_TAG)
                orphan_batch.clear()
        else:
            if has_orphan_tag:
                untag_batch.append(t['hash'])
                if len(untag_batch) >= BATCH_SIZE:
                    remove_tag_http(untag_batch, ORPHAN_TAG)
                    untag_batch.clear()

        last_checked_completion_time[t['hash']] = completion_time

    # Flush remaining batches
    if orphan_batch:
        add_tag_http(orphan_batch, ORPHAN_TAG)
    if untag_batch:
        remove_tag_http(untag_batch, ORPHAN_TAG)

    log("Cleanup cycle complete.")

if __name__ == "__main__":
    while True:
        run_cleanup()
        log(f"Waiting {DEBUG_INTERVAL} seconds before next run...")
        time.sleep(DEBUG_INTERVAL)
