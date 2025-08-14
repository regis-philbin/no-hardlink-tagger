import os
import time
from datetime import datetime
from qbittorrent import Client
import requests

# --- Configuration ---
QBITTORRENT_URL = os.environ.get('QBITTORRENT_URL', '').rstrip('/')
QBITTORRENT_USER = os.environ.get('QBITTORRENT_USER')
QBITTORRENT_PASS = os.environ.get('QBITTORRENT_PASS')

if not all([QBITTORRENT_URL, QBITTORRENT_USER, QBITTORRENT_PASS]):
    def log(msg): print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")
    log("Error: Missing required qBittorrent environment variables.")
    time.sleep(9999); raise SystemExit(1)

ORPHAN_TAG = os.environ.get('ORPHAN_TAG', 'NoMediaLink')
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')
MEDIA_DIRS = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]
DEBUG_INTERVAL = int(os.environ.get('DEBUG_INTERVAL', '3600'))

last_checked_completion_time = {}

def log(msg):  # timestamped logging
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}", flush=True)

# --- read-only client (works fine) ---
def get_qb_client():
    try:
        qb = Client(QBITTORRENT_URL)
        qb.login(QBITTORRENT_USER, QBITTORRENT_PASS)
        return qb
    except Exception as e:
        log(f"Error connecting to qBittorrent: {e}")
        return None

# --- small HTTP helper for write actions (tags) ---
def _api_session():
    """
    Fresh session per call; logs in, sets Referer (needed on newer qB WebUI),
    returns (session, error_text) where error_text is None on success.
    """
    try:
        s = requests.Session()
        s.headers.update({'Referer': f"{QBITTORRENT_URL}/"})  # important for CSRF/host checks
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
        if not ok:
            log(f"❌ POST /api/v2/{path} -> {r.status_code} {r.text.strip()}")
        return ok, r.status_code, r.text.strip()
    except Exception as e:
        log(f"❌ POST /api/v2/{path} exception: {e}")
        return False, None, str(e)

# --- tagging via HTTP API ---
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

# --- media search on host filesystem ---
def find_media_path(torrent_file_path):
    fn = os.path.basename(torrent_file_path)
    for media_dir in MEDIA_DIRS:
        for root, _, files in os.walk(media_dir):
            if fn in files:
                return os.path.join(root, fn)
    return None

def run_cleanup():
    log("Starting cleanup cycle...")
    qb = get_qb_client()
    if not qb:
        log("No connection to qBittorrent, skipping this cycle.")
        return

    try:
        torrents = qb.torrents()
    except Exception as e:
        log(f"Error fetching torrents: {e}")
        return

    orphaned_hashes = []
    linked_hashes_with_tag = []

    for t in torrents:
        # Only consider torrents saved under DOWNLOADS_DIR
        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue

        has_orphan_tag = ORPHAN_TAG in t.get('tags', '')
        completion_time = t.get('completion_on', 0)

        # Skip unchanged orphans
        if has_orphan_tag and completion_time == last_checked_completion_time.get(t['hash']):
            log(f"⏩ Skipping '{t['name']}' (unchanged orphan).")
            continue

        # Inspect files to see if any file is hardlink-equal to media
        is_linked_to_media = False
        try:
            files = qb.get_torrent_files(t['hash'])
        except Exception as e:
            log(f"⚠ Could not fetch files for '{t['name']}': {e}")
            files = []

        for fi in files:
            torrent_path = os.path.join(t['save_path'], fi['name'])
            media_path = find_media_path(torrent_path)
            if not media_path:
                continue
            try:
                if os.stat(torrent_path).st_ino == os.stat(media_path).st_ino:
                    is_linked_to_media = True
                    break
            except FileNotFoundError:
                # Skip missing files
                pass
            except Exception as e:
                log(f"⚠ stat error on '{t['name']}': {e}")

        if not is_linked_to_media:
            log(f"Torrent '{t['name']}' has no media link. Will tag '{ORPHAN_TAG}'.")
            orphaned_hashes.append(t['hash'])
        else:
            log(f"Torrent '{t['name']}' is linked to media.")
            if has_orphan_tag:
                linked_hashes_with_tag.append(t['hash'])

        last_checked_completion_time[t['hash']] = completion_time

    # Apply/remove tags via raw HTTP calls (with status logging)
    if orphaned_hashes:
        add_tag_http(orphaned_hashes, ORPHAN_TAG)
    if linked_hashes_with_tag:
        remove_tag_http(linked_hashes_with_tag, ORPHAN_TAG)

    log(f"📊 Summary: {len(orphaned_hashes)} tagged, {len(linked_hashes_with_tag)} untagged.")
    if not orphaned_hashes and not linked_hashes_with_tag:
        log("No tagging changes needed.")
    log("Cleanup cycle complete.")

if __name__ == "__main__":
    while True:
        run_cleanup()
        log(f"Waiting {DEBUG_INTERVAL} seconds before next run...")
        time.sleep(DEBUG_INTERVAL)
