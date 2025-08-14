import os
import time
from datetime import datetime
from qbittorrent import Client

# --- Configuration ---
QBITTORRENT_URL = os.environ.get('QBITTORRENT_URL')
QBITTORRENT_USER = os.environ.get('QBITTORRENT_USER')
QBITTORRENT_PASS = os.environ.get('QBITTORRENT_PASS')

if not all([QBITTORRENT_URL, QBITTORRENT_USER, QBITTORRENT_PASS]):
    def log(msg): print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")
    log("Error: Missing one or more required qBittorrent environment variables.")
    log("Please check QBITTORRENT_URL, QBITTORRENT_USER, and QBITTORRENT_PASS.")
    time.sleep(9999)  # Keep container alive for inspection
    exit(1)

ORPHAN_TAG = os.environ.get('ORPHAN_TAG', 'orphaned')
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')
MEDIA_DIRS = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]

DEBUG_INTERVAL = int(os.environ.get('DEBUG_INTERVAL', '30'))  # Seconds between runs

# --- Logging helper ---
def log(msg):
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}", flush=True)

# --- qBittorrent connection ---
def get_qb_client():
    try:
        qb = Client(QBITTORRENT_URL)
        qb.login(QBITTORRENT_USER, QBITTORRENT_PASS)
        return qb
    except Exception as e:
        log(f"Error connecting to qBittorrent: {e}")
        return None

# --- Media search ---
def find_media_path(torrent_file_path):
    torrent_filename = os.path.basename(torrent_file_path)
    for media_dir in MEDIA_DIRS:
        for root, dirs, files in os.walk(media_dir):
            if torrent_filename in files:
                return os.path.join(root, torrent_filename)
    return None

# --- Tagging helpers ---
def ensure_tag_exists(qb, tag):
    try:
        existing_tags = qb._get('torrents/tags')
        if isinstance(existing_tags, list):
            tag_names = [t['name'] if isinstance(t, dict) else t for t in existing_tags]
            if tag not in tag_names:
                qb._post('torrents/createTags', data={'tags': tag})
                log(f"🆕 Created tag '{tag}' in qBittorrent.")
        else:
            # If API returns string list instead of dicts
            if tag not in existing_tags:
                qb._post('torrents/createTags', data={'tags': tag})
                log(f"🆕 Created tag '{tag}' in qBittorrent.")
    except Exception as e:
        log(f"⚠ Could not check/create tag '{tag}': {e}")

def tag_torrents(qb, hashes, tag):
    if not hashes:
        return
    ensure_tag_exists(qb, tag)
    try:
        qb._post('torrents/addTags', data={
            'hashes': '|'.join(hashes),
            'tags': tag
        })
        log(f"✅ Successfully tagged {len(hashes)} torrent(s) with '{tag}'.")
    except Exception as e:
        log(f"❌ Error tagging torrents: {e}")

# --- Main cleanup ---
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

    for torrent in torrents:
        if not torrent['save_path'].startswith(DOWNLOADS_DIR):
            continue

        torrent_files = qb.get_torrent_files(torrent['hash'])
        
        is_linked_to_media = False
        for file_info in torrent_files:
            torrent_file_path = os.path.join(torrent['save_path'], file_info['name'])
            media_file_path = find_media_path(torrent_file_path)

            if not media_file_path:
                continue

            try:
                torrent_inode = os.stat(torrent_file_path).st_ino
                media_inode = os.stat(media_file_path).st_ino

                if torrent_inode == media_inode:
                    is_linked_to_media = True
                    break
            except FileNotFoundError:
                pass

        if not is_linked_to_media:
            log(f"Torrent '{torrent['name']}' has no media link. Will tag '{ORPHAN_TAG}'.")
            orphaned_hashes.append(torrent['hash'])
        else:
            log(f"Torrent '{torrent['name']}' is linked to media. No action.")

    if orphaned_hashes:
        tag_torrents(qb, orphaned_hashes, ORPHAN_TAG)
    else:
        log("No orphaned torrents to tag.")

    log("Cleanup cycle complete.")

# --- Loop ---
if __name__ == "__main__":
    while True:
        run_cleanup()
        log(f"Waiting {DEBUG_INTERVAL} seconds before next run...")
        time.sleep(DEBUG_INTERVAL)
