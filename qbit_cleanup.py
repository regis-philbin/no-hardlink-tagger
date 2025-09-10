import os
import time
from datetime import datetime
from qbittorrent import Client
import requests
import hashlib
from collections import defaultdict

VERSION = "no-hardlink-tagger v2.3 — visibility guard + strict seeding + activity window + active-inode shield + min completed age"

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
FAILSAFE_ENABLED           = os.environ.get('FAILSAFE_ENABLED', '1') not in ('0','false','False')
FAILSAFE_REQUIRE_DIRS      = os.environ.get('FAILSAFE_REQUIRE_DIRS', 'any').lower()  # 'any' or 'all'
FAILSAFE_MIN_MEDIA_FILES   = int(os.environ.get('FAILSAFE_MIN_MEDIA_FILES', '100'))
FAILSAFE_MAX_INDEX_ERRORS  = int(os.environ.get('FAILSAFE_MAX_INDEX_ERRORS', '200'))

# --- Activity window ---
ACTIVE_GRACE_MINUTES       = int(os.environ.get('ACTIVE_GRACE_MINUTES', '30'))  # 0 disables

# --- Active Inode Shield toggle ---
ACTIVE_INODE_SHIELD        = os.environ.get('ACTIVE_INODE_SHIELD', '1') not in ('0','false','False')

# --- NEW: minimum completed age (hours). Skip torrents completed more recently than this ---
MIN_COMPLETED_AGE_HOURS    = int(os.environ.get('MIN_COMPLETED_AGE_HOURS', '24'))  # 0 disables

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
    if not hashes:
        return 0
    if _api_post('torrents/addTags', {'hashes': '|'.join(hashes), 'tags': tag}):
        log(f"✅ addTags OK: tagged {len(hashes)} torrent(s) with '{tag}'.")
        return len(hashes)
    return 0

def remove_tag_http(hashes, tag):
    if not hashes:
        return 0
    if _api_post('torrents/removeTags', {'hashes': '|'.join(hashes), 'tags': tag}):
        log(f"🗑 removeTags OK: removed '{tag}' from {len(hashes)} torrent(s).")
        return len(hashes)
    return 0

# --- Strict seeding detection (only these count as active) ---
def is_actively_seeding(t):
    """
    Active ONLY when state is one of: uploading, forcedUP, checkingUP, queuedUP.
    Everything else (including stalledUP, pausedUP) is NOT active.
    """
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

# --- NEW: minimum completed age gate ---
def is_too_new(t, hours):
    """
    Return True if torrent completed less than 'hours' ago.
    If 'completion_on' is missing/0 (not completed / unknown), treat as 'too new' for safety.
    """
    if hours <= 0:
        return False
    co = t.get('completion_on')
    if not isinstance(co, int) or co <= 0:
        return True
    now = int(time.time())
    return (now - co) < hours * 3600

# --- Media candidate + quick hash ---
def is_media_candidate(path, size_bytes):
    if size_bytes < MIN_SIZE_MB * 1024 * 1024:
        return False
    ext = os.path.splitext(path)[1].lower()
    return (ext in EXT_WHITELIST) if EXT_WHITELIST else True

def quick_hash(path, block=1024*1024, retries=2, delay=0.2):
    """Hash first and last 1MB with small retry to tolerate transient read/lock issues."""
    for attempt in range(retries + 1):
        try:
            h = hashlib.sha1()
            sz = os.path.getsize(path)
            with open(path, 'rb') as f:
                data = f.read(block); h.update(data)
                if sz > block:
                    try: f.seek(max(0, sz - block))
                    except OSError: pass
                    data2 = f.read(block); h.update(data2)
            return h.hexdigest()
        except Exception:
            if attempt < retries:
                time.sleep(delay)
            else:
                return None

# --- Visibility guard + media index (size -> paths), with stats ---
def _dir_accessible(p):
    try:
        if not os.path.isdir(p): return False
        if not (os.access(p, os.R_OK) and os.access(p, os.X_OK)): return False
        with os.scandir(p) as it:
            for _ in it: break
        return True
    except Exception:
        return False

def build_media_size_map_with_stats():
    size_map = defaultdict(list)
    files_count = 0
    errors = 0
    dirs_ok = 0
    dirs_missing = []
    start = time.time()

    for media_dir in MEDIA_DIRS:
        if not _dir_accessible(media_dir):
            dirs_missing.append(media_dir); continue
        dirs_ok += 1
        try:
            for root, _, files in os.walk(media_dir):
                for fn in files:
                    path = os.path.join(root, fn)
                    try:
                        st = os.stat(path)
                    except FileNotFoundError:
                        continue
                    except Exception:
                        errors += 1; continue
                    if not is_media_candidate(path, st.st_size):
                        continue
                    size_map[st.st_size].append(path)
                    files_count += 1
        except Exception:
            errors += 1; continue

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

# --- Active Inode Shield ---
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
                shield.add((st.st_dev, st.st_ino))
                protected += 1
    if shield:
        log(f"🛡 Active inode shield: {len(shield)} unique files from {protected} media entries across active/recent torrents.")
    else:
        log("🛡 Active inode shield: empty.")
    return shield

def linked_to_library(qb, t, media_size_map, media_hash_cache):
    """
    Returns True if ANY qualifying torrent file:
      - has st_nlink > 1 (i.e., is hardlinked somewhere), AND
      - content-matches (size + quick hash) at least one file under MEDIA_DIRS.
    Returns False if no match; None if reads were inconclusive.
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
            return None  # inconclusive

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
    if not FAILSAFE_ENABLED:
        return True, "failsafe disabled"
    if FAILSAFE_REQUIRE_DIRS == 'all':
        if stats['dirs_ok'] < len(MEDIA_DIRS):
            return False, f"not all media dirs accessible ({stats['dirs_ok']}/{len(MEDIA_DIRS)})"
    else:
        if stats['dirs_ok'] == 0:
            return False, "no media dirs accessible"
    if stats['files_count'] < FAILSAFE_MIN_MEDIA_FILES:
        return False, f"too few media files indexed ({stats['files_count']} < {FAILSAFE_MIN_MEDIA_FILES})"
    if stats['errors'] > FAILSAFE_MAX_INDEX_ERRORS:
        return False, f"too many indexing errors ({stats['errors']} > {FAILSAFE_MAX_INDEX_ERRORS})"
    return True, "ok"

def run_cleanup():
    log(f"{VERSION} — url={QBITTORRENT_URL} — MIN_SIZE_MB={MIN_SIZE_MB} — EXT_WHITELIST={','.join(EXT_WHITELIST)} — ACTIVE_GRACE_MINUTES={ACTIVE_GRACE_MINUTES} — ACTIVE_INODE_SHIELD={int(ACTIVE_INODE_SHIELD)} — MIN_COMPLETED_AGE_HOURS={MIN_COMPLETED_AGE_HOURS}")
    log("Starting cleanup cycle...")

    qb = get_qb_client()
    if not qb:
        log("No connection to qBittorrent, skipping this cycle.")
        return

    media_size_map, vis_stats = build_media_size_map_with_stats()
    vis_ok, vis_reason = _visibility_is_ok(vis_stats)
    if not vis_ok:
        log(f"🛑 FAILSAFE: Library visibility not healthy ({vis_reason}). **No tag changes will be made this cycle.**")
        return

    try:
        torrents = qb.torrents()
    except Exception as e:
        log(f"Error fetching torrents: {e}")
        return

    if MAX_TORRENTS > 0:
        torrents = torrents[:MAX_TORRENTS]
        log(f"⚙ Limiting to first {MAX_TORRENTS} torrents (MAX_TORRENTS).")

    active_shield = build_active_inode_shield(qb, torrents) if ACTIVE_INODE_SHIELD else set()

    media_hash_cache = {}
    orphan_batch, untag_batch = [], []
    skipped_active = skipped_recent = skipped_shield = skipped_min_age = skipped_inconclusive = 0
    total_tagged = total_untagged = 0

    for i, t in enumerate(torrents, 1):
        if i % 50 == 0:
            log(f"…processed {i}/{len(torrents)} torrents.")

        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue

        # Strict: skip actives and recent
        if is_actively_seeding(t):
            skipped_active += 1
            continue
        if is_recently_active(t, ACTIVE_GRACE_MINUTES):
            skipped_recent += 1
            continue

        # NEW: skip torrents completed too recently
        if is_too_new(t, MIN_COMPLETED_AGE_HOURS):
            skipped_min_age += 1
            continue

        # Shield: if any file shares inode with an active/recent torrent, skip changes
        if active_shield:
            try:
                files = qb.get_torrent_files(t['hash'])
            except Exception:
                files = []
            shield_hit = False
            for fi in files:
                p = os.path.join(t['save_path'], fi['name'])
                try:
                    st = os.stat(p)
                except Exception:
                    continue
                if is_media_candidate(p, st.st_size) and (st.st_dev, st.st_ino) in active_shield:
                    shield_hit = True
                    break
            if shield_hit:
                skipped_shield += 1
                continue

        has_orphan_tag = ORPHAN_TAG in t.get('tags', '')
        completion_time = t.get('completion_on', 0)
        if has_orphan_tag and completion_time == last_checked_completion_time.get(t['hash']):
            continue

        linked = linked_to_library(qb, t, media_size_map, media_hash_cache)
        if linked is None:
            skipped_inconclusive += 1
            continue

        if not linked:
            orphan_batch.append(t['hash'])
            if len(orphan_batch) >= BATCH_SIZE:
                total_tagged += add_tag_http(orphan_batch, ORPHAN_TAG)
                orphan_batch.clear()
        else:
            if has_orphan_tag:
                untag_batch.append(t['hash'])
                if len(untag_batch) >= BATCH_SIZE:
                    total_untagged += remove_tag_http(untag_batch, ORPHAN_TAG)
                    untag_batch.clear()

        last_checked_completion_time[t['hash']] = completion_time

    if orphan_batch:
        total_tagged += add_tag_http(orphan_batch, ORPHAN_TAG)
    if untag_batch:
        total_untagged += remove_tag_http(untag_batch, ORPHAN_TAG)

    log(f"📊 Summary: tagged={total_tagged}, untagged={total_untagged}, "
        f"skipped_active={skipped_active}, skipped_recent={skipped_recent}, "
        f"skipped_min_age={skipped_min_age}, shield_skips={skipped_shield}, "
        f"inconclusive_skips={skipped_inconclusive}")
    log("Cleanup cycle complete.")

if __name__ == "__main__":
    while True:
        run_cleanup()
        log(f"Waiting {DEBUG_INTERVAL} seconds before next run...")
        time.sleep(DEBUG_INTERVAL)
