import os
import time
from qbittorrent import Client

# --- Configuration ---
# The script will now expect these environment variables to be set.
QBITTORRENT_URL = os.environ['QBITTORRENT_URL']
QBITTORRENT_USER = os.environ['QBITTORRENT_USER']
QBITTORRENT_PASS = os.environ['QBITTORRENT_PASS']

# Tag to be applied to orphaned torrents
ORPHAN_TAG = os.environ.get('ORPHAN_TAG', 'orphaned')

# The directory where all your torrents are downloaded
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')

# The directories where your final media files are stored
MEDIA_DIRS = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]

# --- Script Logic (rest remains the same) ---
def get_qb_client():
    """Connects to the qBittorrent client."""
    try:
        qb = Client(QBITTORRENT_URL)
        qb.login(QBITTORRENT_USER, QBITTORRENT_PASS)
        return qb
    except Exception as e:
        print(f"Error connecting to qBittorrent: {e}")
        return None

def find_media_path(torrent_file_path):
    """
    Finds the corresponding media path for a torrent file by searching for the
    file name within the media directories.
    """
    torrent_filename = os.path.basename(torrent_file_path)
    for media_dir in MEDIA_DIRS:
        for root, dirs, files in os.walk(media_dir):
            if torrent_filename in files:
                return os.path.join(root, torrent_filename)
    return None

def run_cleanup():
    """Main function to perform the cleanup."""
    print("Starting qBittorrent cleanup script...")
    qb = get_qb_client()
    if not qb:
        return

    torrents = qb.torrents_info()
    orphaned_hashes = []

    for torrent in torrents:
        # We only care about torrents in the downloads directory
        if not torrent.save_path.startswith(DOWNLOADS_DIR):
            continue

        torrent_files = qb.get_torrent_files(torrent.hash)
        
        is_linked_to_media = False
        for file_info in torrent_files:
            torrent_file_path = os.path.join(torrent.save_path, file_info.name)

            # Find the corresponding media file by name search
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
            print(f"Torrent '{torrent.name}' is not linked to a media file. Tagging as '{ORPHAN_TAG}'.")
            orphaned_hashes.append(torrent.hash)
        else:
            print(f"Torrent '{torrent.name}' is linked to media. No action needed.")

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
