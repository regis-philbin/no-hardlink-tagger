# no-hardlink-tagger

Tags torrents in qBittorrent that do not hard link back to your media library. Published as `ghcr.io/<repo-owner>/no-hardlink-tagger:latest` for use with Portainer, Docker Compose, or any OCI-compatible runtime.

## Deploy with Portainer Stack Editor
1. Open **Stacks → Add stack** in Portainer.
2. Paste the compose snippet below (adjust the environment values for your setup).
3. Deploy the stack to automatically pull the GHCR image `ghcr.io/<your-ghcr-namespace>/no-hardlink-tagger:latest`.

```yaml
version: "3.8"
services:
  no-hardlink-tagger:
    image: ghcr.io/<your-ghcr-namespace>/no-hardlink-tagger:latest
    container_name: no-hardlink-tagger
    restart: unless-stopped
    environment:
      QBITTORRENT_URL: "${QBITTORRENT_URL}"
      QBITTORRENT_USER: "${QBITTORRENT_USER}"
      QBITTORRENT_PASS: "${QBITTORRENT_PASS}"
      ORPHAN_TAG: "${ORPHAN_TAG:-NoMediaLink}"
      DOWNLOADS_DIR: "${DOWNLOADS_DIR:-/media/downloads}"
      MEDIA_DIRS: "${MEDIA_DIRS:-/media/movies,/media/tv}"
      DEBUG_INTERVAL: "${DEBUG_INTERVAL:-60}"
      LOG_USE_AMPM: "${LOG_USE_AMPM:-0}"
      BATCH_SIZE: "${BATCH_SIZE:-25}"
      MAX_TORRENTS: "${MAX_TORRENTS:-0}"
      MIN_SIZE_MB: "${MIN_SIZE_MB:-50}"
      EXT_WHITELIST: "${EXT_WHITELIST:-.mkv,.mp4,.m4v,.mov,.avi,.ts,.m2ts,.mpg,.mpeg,.wmv}"
      CACHE_DIR: "${CACHE_DIR:-/cache}"
      HASH_BUDGET_MB: "${HASH_BUDGET_MB:-1024}"
      DECISION_TTL_HOURS: "${DECISION_TTL_HOURS:-24}"
      ACTIVE_GRACE_MINUTES: "${ACTIVE_GRACE_MINUTES:-30}"
      MIN_COMPLETED_AGE_HOURS: "${MIN_COMPLETED_AGE_HOURS:-24}"
      MEDIA_LINK_MIN_PERCENT: "${MEDIA_LINK_MIN_PERCENT:-1}"
      MEDIA_LINK_TAG_STEPS: "${MEDIA_LINK_TAG_STEPS:-}"
      MEDIA_LINK_TAG_PREFIX: "${MEDIA_LINK_TAG_PREFIX:-MediaLink-}"
      FAILSAFE_ENABLED: "${FAILSAFE_ENABLED:-1}"
      FAILSAFE_REQUIRE_DIRS: "${FAILSAFE_REQUIRE_DIRS:-any}"
      FAILSAFE_MIN_MEDIA_FILES: "${FAILSAFE_MIN_MEDIA_FILES:-100}"
      FAILSAFE_MAX_INDEX_ERRORS: "${FAILSAFE_MAX_INDEX_ERRORS:-200}"
      ACTIVE_INODE_SHIELD: "${ACTIVE_INODE_SHIELD:-1}"
    volumes:
      - /media:/media
      - ./cache:/cache
```

## Docker Compose (local example)
Create a `.env` file with your qBittorrent credentials and any overrides, then run:

```bash
docker compose up -d
```

Use the same compose snippet shown in the Portainer section above, saved as `compose.yaml` or `docker-compose.yml`, to pull and run `ghcr.io/<your-ghcr-namespace>/no-hardlink-tagger:latest` locally.

### Published image
The GitHub Actions workflow in `.github/workflows/docker.yml` builds and publishes the image to GitHub Container Registry on pushes to the default branch. By default, images are tagged under `ghcr.io/<repo-owner>/no-hardlink-tagger` with tags for the branch name, version tags, and `latest` on the default branch. Replace `<your-ghcr-namespace>` in the compose snippet with the same namespace to match the published image location.

## Environment configuration
All runtime configuration is provided through environment variables and can be edited directly in the Portainer stack editor or a Compose file.

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBITTORRENT_URL` | _(required)_ | qBittorrent Web UI base URL (e.g., `http://qbittorrent:8080`). |
| `QBITTORRENT_USER` | _(required)_ | qBittorrent username. |
| `QBITTORRENT_PASS` | _(required)_ | qBittorrent password. |
| `ORPHAN_TAG` | `NoMediaLink` | Tag applied when torrents lack hard links to media. |
| `DOWNLOADS_DIR` | `/media/downloads` | Root downloads directory in qBittorrent. |
| `MEDIA_DIRS` | `/media/movies,/media/tv` | Comma-separated media library directories to check for hardlinks. |
| `DEBUG_INTERVAL` | `60` | Seconds between cleanup cycles (log heartbeat). |
| `LOG_USE_AMPM` | `0` | Set to `1` to format log timestamps in 12-hour time with AM/PM. |
| `BATCH_SIZE` | `25` | Torrents scanned per batch. |
| `MAX_TORRENTS` | `0` | Maximum torrents to process per run (`0` = no limit). |
| `MIN_SIZE_MB` | `50` | Minimum file size (MiB) to treat as media. |
| `EXT_WHITELIST` | `.mkv,.mp4,.m4v,.mov,.avi,.ts,.m2ts,.mpg,.mpeg,.wmv` | Allowed media file extensions. |
| `MEDIA_LINK_MIN_PERCENT` | `1` | Minimum percent of torrent data that must hardlink to media to avoid tagging. |
| `MEDIA_LINK_TAG_STEPS` | _(empty)_ | Comma-separated percentages for additional coverage tags. |
| `MEDIA_LINK_TAG_PREFIX` | `MediaLink-` | Prefix used for coverage tags. |
| `FAILSAFE_ENABLED` | `1` | Enable guardrails to avoid tagging when media paths look inaccessible. |
| `FAILSAFE_REQUIRE_DIRS` | `any` | Require `any` or `all` media dirs to be accessible before scanning. |
| `FAILSAFE_MIN_MEDIA_FILES` | `100` | Minimum media files expected to consider paths healthy. |
| `FAILSAFE_MAX_INDEX_ERRORS` | `200` | Maximum filesystem errors tolerated before aborting. |
| `ACTIVE_GRACE_MINUTES` | `30` | Skip tagging torrents active within this many minutes. |
| `MIN_COMPLETED_AGE_HOURS` | `24` | Skip torrents completed within this many hours. |
| `ACTIVE_INODE_SHIELD` | `1` | Extra protection to avoid tagging while files are mutating. |
| `WEB_UI_ENABLED` | `0` | Enable the embedded assessments web UI and SQLite persistence. |
| `WEB_UI_BIND` | `0.0.0.0` | Host/interface to bind the web UI server. |
| `WEB_UI_PORT` | `8081` | Port to expose the web UI server. |
| `ASSESSMENTS_DB_PATH` | `${CACHE_DIR}/assessments.sqlite` | Location of the persistent assessments database. |
| `VERIFY_ENABLED` | `1` when `WEB_UI_ENABLED=1` | Enable background verification of stored torrent existence. |
| `VERIFY_INTERVAL_SECONDS` | `3600` | Seconds between verification passes when enabled. |
| `CACHE_DIR` | `/cache` | Location for persistent cache data. |
| `HASH_BUDGET_MB` | `1024` | MiB budget for hashing per run. |
| `DECISION_TTL_HOURS` | `24` | Reuse previous decisions for unchanged torrents for this many hours. |

## Minimum media-link coverage
You can control how much of a torrent's candidate media size must be hardlinked back to your media folders before it is considered linked. Set `MEDIA_LINK_MIN_PERCENT` (0–100) to the minimum percentage of total candidate **size** that needs to match the media library for the torrent to avoid the `NoMediaLink` tag. For example, `MEDIA_LINK_MIN_PERCENT=20` will still tag a season pack if only one 1 GB file is linked out of a 5 GB season pack.

Optional coverage tags can also be emitted to show the best-matching threshold a torrent met. Configure `MEDIA_LINK_TAG_STEPS` with a comma-separated list of percentages (e.g., `10,20,30`) and the script will apply tags such as `MediaLink-10%`, `MediaLink-20%`, etc., using the prefix from `MEDIA_LINK_TAG_PREFIX`.

## Web UI and assessments database
Set `WEB_UI_ENABLED=1` to start a lightweight HTTP server (default bind: `0.0.0.0:8081`) that serves a DataTables-powered assessments page and JSON endpoints. The UI shows every torrent the script has ever assessed, its most recent assessment results, existence status, and whether a removal was detected after the script successfully tagged it.

### Persistence
Assessment data is stored in SQLite at `${CACHE_DIR}/assessments.sqlite` (override with `ASSESSMENTS_DB_PATH`). Bind-mount `CACHE_DIR` (e.g., `./cache:/cache`) to keep the database and existing JSONL `actions.log` across restarts. The background verification loop revalidates torrent existence using qBittorrent and updates the UI even after torrents are removed from the client.

### Example docker-compose snippet (UI enabled)
```yaml
version: "3.8"
services:
  no-hardlink-tagger:
    image: ghcr.io/<your-ghcr-namespace>/no-hardlink-tagger:latest
    restart: unless-stopped
    environment:
      QBITTORRENT_URL: "${QBITTORRENT_URL}"
      QBITTORRENT_USER: "${QBITTORRENT_USER}"
      QBITTORRENT_PASS: "${QBITTORRENT_PASS}"
      CACHE_DIR: "${CACHE_DIR:-/cache}"
      WEB_UI_ENABLED: "1"
      WEB_UI_PORT: "8081"
      VERIFY_INTERVAL_SECONDS: "3600"
    ports:
      - "8081:8081"
    volumes:
      - /media:/media
      - ./cache:/cache
```

### UI overview
- **Status header:** displays the last verification timestamp (global), total torrent count, how many still exist in qBittorrent, and how many are currently **missing**. A **Refresh existence now** button triggers an immediate verification pass.
- **Table:** sortable/searchable DataTable with filters for existence, current missing state, missing-after-tag state, and linked-percent min/max ranges. Columns include torrent name/hash/tracker/tags, linked percentage, cached flag (whether a cached assessment was reused), existence/missing flags, coverage tag, and last action metadata.

### Missing vs. missing after tag
- **Missing?** reflects the current state: `exists_in_qbt == 0` at the time of the latest verification check.
- **Missing after tag?** is true when the torrent is currently missing and the most recent tag action from this script succeeded (optionally also requiring that tag action to predate the first missing observation).
