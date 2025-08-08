import os
import time
from qbittorrent import Client

# --- Configuration ---
QBITTORRENT_URL = os.environ.get('QBITTORRENT_URL')
QBITTORRENT_USER = os.environ.get('QBITTORRENT_USER')
QBITTORRENT_PASS = os.environ.get('QBITTORRENT_PASS')

if not all([QBITTORRENT_URL, QBITTORRENT_USER, QBITTORRENT_PASS]):
    print("Error: Missing one or more required qBittorrent environment variables.")
    # Exit with a non-zero status code to indicate failure
    exit(1)

ORPHAN_TAG = os.environ.get('ORPHAN_TAG', 'orphaned')
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')
MEDIA_DIRS = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]

# --- Script Logic ---
def get_qb_client():
    # ... rest of the function remains the same
    try:
        qb = Client(QBITTORRENT_URL)
        qb.login(QBITTORRENT_USER, QBITTORRENT_PASS)
        return qb
    except Exception as e:
        print(f"Error connecting to qBittorrent: {e}")
        return None

def find_media_path(torrent_file_path):
    # ... rest of the function remains the same
    torrent_filename = os.path.basename(torrent_file_path)
    for media_dir in MEDIA_DIRS:
        for root, dirs, files in os.walk(media_dir):
            if torrent_filename in files:
                return os.path.join(root, torrent_filename)
    return None

def run_cleanup():
    # ... rest of the function remains the same
    print("Starting qBittorrent cleanup script...")
    qb = get_qb_client()
    if not qb:
        return

    torrents = qb.torrents()
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
            print(f"Torrent '{torrent['name']}' is not linked to a media file. Tagging as '{ORPHAN_TAG}'.")
            orphaned_hashes.append(torrent['hash'])
        else:
            print(f"Torrent '{torrent['name']}' is linked to media. No action needed.")

    if orphaned_hashes:
        print(f"Tagging {len(orphaned_hashes)} torrents with '{ORPHAN_TAG}'...")
        qb.add_tags(orphaned_hashes, ORPHAN_TAG)
    else:
        print("No orphaned torrents to tag.")
    
    print("Cleanup complete.")

if __name__ == "__main__":
    while True:
        run_cleanup()
        print("Waiting for 1 hour before next run...")
        time.sleep(3600)
