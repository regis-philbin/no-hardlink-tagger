# no-hardlink-tagger
Tags torrents in qBittorrent that do not hard link back to your media library.

## New: minimum media-link coverage
You can now control how many hardlinked files must be found in your media folders before a torrent is considered "linked." Set `MEDIA_LINK_MIN_PERCENT` (0–100) to the minimum percentage of candidate torrent files that need to match the media library for the torrent to avoid the `NoMediaLink` tag. Example: `MEDIA_LINK_MIN_PERCENT=20` will still tag a season pack if only 1 of 10 episodes is linked back to the media directory.

Optional coverage tags can also be emitted to show the best-matching threshold a torrent met. Configure `MEDIA_LINK_TAG_STEPS` with a comma-separated list of percentages (e.g., `10,20,30`) and the script will apply tags such as `MediaLink-10%`, `MediaLink-20%`, etc., using the prefix from `MEDIA_LINK_TAG_PREFIX`.

### Related environment variables
- `MEDIA_LINK_MIN_PERCENT` (default `1`): minimum percent of candidate files that must match the media library to avoid `NoMediaLink`.
- `MEDIA_LINK_TAG_STEPS` (default empty): comma-separated percentage thresholds for emitting coverage tags. Leave blank to disable.
- `MEDIA_LINK_TAG_PREFIX` (default `MediaLink-`): prefix used when composing coverage tags.
