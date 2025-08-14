import os
import time
from datetime import datetime
from qbittorrent import Client
import requests
from collections import defaultdict

VERSION = "no-hardlink-tagger v1.5 — indexed mode + batch tagging"

# --- Configuration ---
QBITTORRENT_URL = os.environ.get('QBITTORRENT_URL', '').rstrip('/')
QBITTORRENT_USER = os.environ.get('QBITTORRENT_USER')
QBITTORRENT_PASS = os.environ.get('QBITTORRENT_PASS')

ORPHAN_TAG   = os.environ.get('ORPHAN_TAG', 'NoMediaLink')
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')
MEDIA_DIRS    = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]

# Debug knobs
DEBUG_INTERVAL = int(os.environ.get('DEBUG_INTERVAL', '30'))   # seconds between runs
BATCH_SIZE     = int(os.environ.get('BATCH_SIZE', '25'))       # tag this many at a time
MAX_TORRENTS   = int(os.environ.get('MAX_TORRENTS', '0'))      # 0 = no limit; for quick tests set e.g. 50

if not all([QBITTORRENT_URL, QBITTORRENT_USER, QBITTORRENT_PASS]):
    def log(msg): print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")
    log("Error: Missing required qBittorrent environment variables.")
    time.sleep(9999); raise SystemExit(1)

last_checked_completion_time = {}

def log(msg):
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}", flush=True)

# --- read-only client (listing) ---
def get_qb_client():
    try:
        qb = Client(QBITTORRENT_URL)
        qb.login(QBITTORRENT_USER, QBITTORRENT_PASS)
        return qb
    except Exception as e:
        log(f"Error connecting to qBittorrent: {e}")
        return None

# --- HTTP session for write calls (tags) ---
def _api_session():
    try:
        s = requests.Session()
        s.headers.update({
            'Referer': f"{QBITTORRENT_URL}/",
            'Origin': QBITTORRENT_URL,
        })
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
        return False, None, err
    try:
        r = s.post(f"{QBITTORRENT_URL}/api/v2/{path}", data=data, timeout=15)
        ok = (r.status_code == 200)
        if ok:
            log(f"➡ POST {path} 200 OK")
        else:
            log(f"❌ POST {path} -> {r.status_code} {r.text.strip()}")
        return ok, r.status_code, r.text.strip()
    except Exception as e:
        log(f"❌ POST {path} exception: {e}")
        return False, None, str(e)

def add_tag_http(hashes, tag):
    if not hashes:
        return
    ok, code, text = _api_post('torrents/addTags',
                               {'hashes': '|'.join(hashes), 'tags': tag})
    if ok:
        log(f"✅ addTags OK: tagged {len(hashes)} torrent(s) with '{tag}'.")
    else:
        log(f"❌ addTags failed ({code}): {text}")

def remove_tag_http(hashes, tag):
    if not hashes:
        return
    ok, code, text = _api_post('torrents/removeTags',
                               {'hashes': '|'.join(hashes), 'tags': tag})
    if ok:
        log(f"🗑 removeTags OK: removed '{tag}' from {len(hashes)} torrent(s).")
    else:
        log(f"❌ removeTags failed ({code}): {text}")

# --- Build media index once per cycle ---
def build_media_index():
    start = time.time()
    index = defaultdict(set)  # filename -> set of inodes
    files_count = 0
    for media_dir in MEDIA_DIRS:
        for root, _, files in os.walk(media_dir):
            for fn in files:
                path = os.path.join(root, fn)
                try:
                    inode = os.stat(path).st_ino
                except FileNotFoundError:
                    continue
                except Exception as e:
                    log(f"⚠ stat error in media index: {e}")
                    continue
                index[fn].add(inode)
                files_count += 1
    elapsed = time.time() - start
    log(f"📚 Media index built: {files_count} files across {len(MEDIA_DIRS)} dir(s) in {elapsed:.1f}s "
        f"({len(index)} unique basenames).")
    return index

def run_cleanup():
    log(f"{VERSION} — url={QBITTORRENT_URL}")
    log("Starting cleanup cycle...")

    qb = get_qb_client()
    if not qb:
        log("No connection to qBittorrent, skipping this cycle.")
        return

    # Build index once
    media_index = build_media_index()

    try:
        torrents = qb.torrents()
    except Exception as e:
        log(f"Error fetching torrents: {e}")
        return

    if MAX_TORRENTS > 0:
        torrents = torrents[:MAX_TORRENTS]
        log(f"⚙ Limiting to first {MAX_TORRENTS} torrents for this run (MAX_TORRENTS).")

    orphan_batch = []
    untag_batch = []

    processed = 0
    for t in torrents:
        processed += 1
        if processed % 50 == 0:
            log(f"…processed {processed}/{len(torrents)} torrents so far.")

        # Only consider torrents saved under DOWNLOADS_DIR
        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue

        has_orphan_tag = ORPHAN_TAG in t.get('tags', '')
        completion_time = t.get('completion_on', 0)

        # Skip unchanged orphans
        if has_orphan_tag and completion_time == last_checked_completion_time.get(t['hash']):
            continue

        # Fetch file list once
        try:
            files = qb.get_torrent_files(t['hash'])
        except Exception as e:
            log(f"⚠ Could not fetch files for '{t['name']}': {e}")
            files = []

        is_linked_to_media = False
        for fi in files:
            torrent_path = os.path.join(t['save_path'], fi['name'])
            fn = os.path.basename(torrent_path)
            try:
                t_inode = os.stat(torrent_path).st_ino
            except FileNotFoundError:
                continue
            except Exception as e:
                log(f"⚠ stat error on torrent file: {e}")
                continue

            # fast lookup: is this torrent file's inode in the media index for same basename?
            if t_inode in media_index.get(fn, ()):
                is_linked_to_media = True
                break

        if not is_linked_to_media:
            log(f"Torrent '{t['name']}' has no media link. Will tag '{ORPHAN_TAG}'.")
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
