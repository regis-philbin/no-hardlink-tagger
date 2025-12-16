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
| `CACHE_DIR` | `/cache` | Location for persistent cache data. |
| `HASH_BUDGET_MB` | `1024` | MiB budget for hashing per run. |
| `DECISION_TTL_HOURS` | `24` | Reuse previous decisions for unchanged torrents for this many hours. |

## Minimum media-link coverage
You can control how much of a torrent's candidate media size must be hardlinked back to your media folders before it is considered linked. Set `MEDIA_LINK_MIN_PERCENT` (0–100) to the minimum percentage of total candidate **size** that needs to match the media library for the torrent to avoid the `NoMediaLink` tag. For example, `MEDIA_LINK_MIN_PERCENT=20` will still tag a season pack if only one 1 GB file is linked out of a 5 GB season pack.

Optional coverage tags can also be emitted to show the best-matching threshold a torrent met. Configure `MEDIA_LINK_TAG_STEPS` with a comma-separated list of percentages (e.g., `10,20,30`) and the script will apply tags such as `MediaLink-10%`, `MediaLink-20%`, etc., using the prefix from `MEDIA_LINK_TAG_PREFIX`.
