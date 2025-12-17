import os
import time
import json
import tempfile
from datetime import datetime
from collections import defaultdict
from qbittorrent import Client
import requests
import hashlib

VERSION = "no-hardlink-tagger v3.0 — cache + two-stage + budget"

# =========================
# Config (env)
# =========================
QBITTORRENT_URL  = os.environ.get('QBITTORRENT_URL', '').rstrip('/')
QBITTORRENT_USER = os.environ.get('QBITTORRENT_USER')
QBITTORRENT_PASS = os.environ.get('QBITTORRENT_PASS')

ORPHAN_TAG    = os.environ.get('ORPHAN_TAG', 'NoMediaLink')
DOWNLOADS_DIR = os.environ.get('DOWNLOADS_DIR', '/media/downloads')
MEDIA_DIRS    = [d.strip() for d in os.environ.get('MEDIA_DIRS', '/media/movies,/media/tv').split(',')]

DEBUG_INTERVAL = int(os.environ.get('DEBUG_INTERVAL', '60'))
BATCH_SIZE     = int(os.environ.get('BATCH_SIZE', '25'))
MAX_TORRENTS   = int(os.environ.get('MAX_TORRENTS', '0'))  # 0 = all

EXT_WHITELIST  = [e.strip().lower() for e in os.environ.get(
    'EXT_WHITELIST', '.mkv,.mp4,.m4v,.mov,.avi,.ts,.m2ts,.mpg,.mpeg,.wmv'
).split(',') if e.strip()]
MIN_SIZE_MB    = int(os.environ.get('MIN_SIZE_MB', '50'))
MEDIA_LINK_MIN_PERCENT = max(0, min(100, int(os.environ.get('MEDIA_LINK_MIN_PERCENT', '1'))))

MEDIA_LINK_TAG_PREFIX = os.environ.get('MEDIA_LINK_TAG_PREFIX', 'MediaLink-')
MEDIA_LINK_TAG_STEPS = []
for _pct in os.environ.get('MEDIA_LINK_TAG_STEPS', '').split(','):
    if not _pct.strip():
        continue
    try:
        val = int(_pct)
        if 0 < val <= 100:
            MEDIA_LINK_TAG_STEPS.append(val)
    except ValueError:
        continue
MEDIA_LINK_TAG_STEPS = sorted(set(MEDIA_LINK_TAG_STEPS))

# Visibility guard
FAILSAFE_ENABLED           = os.environ.get('FAILSAFE_ENABLED', '1') not in ('0','false','False')
FAILSAFE_REQUIRE_DIRS      = os.environ.get('FAILSAFE_REQUIRE_DIRS', 'any').lower()  # 'any' or 'all'
FAILSAFE_MIN_MEDIA_FILES   = int(os.environ.get('FAILSAFE_MIN_MEDIA_FILES', '100'))
FAILSAFE_MAX_INDEX_ERRORS  = int(os.environ.get('FAILSAFE_MAX_INDEX_ERRORS', '200'))

# Activity + age
ACTIVE_GRACE_MINUTES       = int(os.environ.get('ACTIVE_GRACE_MINUTES', '30'))
MIN_COMPLETED_AGE_HOURS    = int(os.environ.get('MIN_COMPLETED_AGE_HOURS', '24'))

# Active-inode shield
ACTIVE_INODE_SHIELD        = os.environ.get('ACTIVE_INODE_SHIELD', '1') not in ('0','false','False')

# NEW: persistent cache + budget + decision TTL
CACHE_DIR                  = os.environ.get('CACHE_DIR', '/cache')
HASH_BUDGET_MB             = int(os.environ.get('HASH_BUDGET_MB', '1024'))  # total MiB to read this run
DECISION_TTL_HOURS         = int(os.environ.get('DECISION_TTL_HOURS', '24')) # reuse result for unchanged torrents

# Logging style
LOG_USE_AMPM               = os.environ.get('LOG_USE_AMPM', '0').lower() in ('1', 'true', 'yes', 'on')

# =========================
# Logging
# =========================
def _timestamp():
    now = datetime.now()
    if LOG_USE_AMPM:
        return now.strftime("%Y-%m-%d %I:%M:%S %p")
    return now.isoformat(sep=' ', timespec='seconds')

def log(msg):
    print(f"[{_timestamp()}] {msg}", flush=True)

# =========================
# qBittorrent helpers
# =========================
def get_qb_client():
    try:
        qb = Client(QBITTORRENT_URL)
        qb.login(QBITTORRENT_USER, QBITTORRENT_PASS)
        return qb
    except Exception as e:
        log(f"Error connecting to qBittorrent: {e}")
        return None

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
    if hashes and _api_post('torrents/addTags', {'hashes': '|'.join(hashes), 'tags': tag}):
        log(f"✅ addTags OK: tagged {len(hashes)} torrent(s) with '{tag}'.")
        return len(hashes)
    return 0

def remove_tag_http(hashes, tag):
    if hashes and _api_post('torrents/removeTags', {'hashes': '|'.join(hashes), 'tags': tag}):
        log(f"🗑 removeTags OK: removed '{tag}' from {len(hashes)} torrent(s).")
        return len(hashes)
    return 0

# =========================
# Activity gates
# =========================
def is_actively_seeding(t):
    # Strict: only these are "active"
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

def is_too_new(t, hours):
    if hours <= 0:
        return False
    co = t.get('completion_on')
    if not isinstance(co, int) or co <= 0:
        return True
    return (int(time.time()) - co) < hours * 3600

# =========================
# Filesystem helpers
# =========================
def is_media_candidate(path, size_bytes):
    if size_bytes < MIN_SIZE_MB * 1024 * 1024:
        return False
    ext = os.path.splitext(path)[1].lower()
    return (ext in EXT_WHITELIST) if EXT_WHITELIST else True

def _dir_accessible(p):
    try:
        if not os.path.isdir(p): return False
        if not (os.access(p, os.R_OK) and os.access(p, os.X_OK)): return False
        with os.scandir(p) as it:
            for _ in it: break
        return True
    except Exception:
        return False

def _dir_fingerprint(path, entry_count=None):
    try:
        st = os.stat(path)
        if entry_count is None:
            with os.scandir(path) as it:
                entry_count = sum(1 for _ in it)
        return max(int(st.st_mtime), int(st.st_ino), int(entry_count or 0))
    except Exception:
        return None

# =========================
# JSON cache helpers
# =========================
def _ensure_dir(p):
    try:
        os.makedirs(p, exist_ok=True)
        return True
    except Exception:
        return False

def _load_json(path, default):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception:
        return default

def _atomic_save_json(path, data):
    tmp = f"{path}.tmp"
    with open(tmp, 'w') as f:
        json.dump(data, f, separators=(',', ':'), ensure_ascii=False)
    os.replace(tmp, path)

# =========================
# Hash budget
# =========================
class Budget:
    def __init__(self, mib):
        self.total = int(mib) * 1024 * 1024
        self.remaining = self.total
        self.exhausted = False
    def need(self, nbytes):
        if self.remaining >= nbytes:
            self.remaining -= nbytes
            return True
        self.exhausted = True
        return False

def quick_hash_budgeted(path, budget, block=1024*1024):
    try:
        sz = os.path.getsize(path)
        need = min(block, sz) + (block if sz > block else 0)
        if not budget.need(need):
            return None
        h = hashlib.sha1()
        with open(path, 'rb') as f:
            data = f.read(min(block, sz)); h.update(data)
            if sz > block:
                try: f.seek(max(0, sz - block))
                except OSError: pass
                data2 = f.read(block); h.update(data2)
        return h.hexdigest()
    except Exception:
        return None

# =========================
# Stage 0: visibility guard
# =========================
def build_media_visibility_stats():
    files_count = 0
    errors = 0
    dirs_ok = 0
    dirs_missing = []
    start = time.time()
    for media_dir in MEDIA_DIRS:
        if not _dir_accessible(media_dir):
            dirs_missing.append(media_dir)
            continue
        dirs_ok += 1
        try:
            for _, _, files in os.walk(media_dir):
                files_count += len(files)
        except Exception:
            errors += 1
            continue
    secs = time.time() - start
    stats = {
        'files_count': files_count,
        'dirs_ok': dirs_ok,
        'dirs_missing': dirs_missing,
        'errors': errors,
        'elapsed': secs,
    }
    log(f"📁 Library visibility: files~{files_count}, dirs_ok={dirs_ok}/{len(MEDIA_DIRS)}, "
        f"missing={len(dirs_missing)}, errors={errors}, in {secs:.1f}s.")
    if dirs_missing:
        log(f"⚠ Missing/Unreadable media dirs: {', '.join(dirs_missing)}")
    ok = True
    if FAILSAFE_ENABLED:
        if (FAILSAFE_REQUIRE_DIRS == 'all' and dirs_ok < len(MEDIA_DIRS)) or \
           (FAILSAFE_REQUIRE_DIRS == 'any' and dirs_ok == 0) or \
           (files_count < FAILSAFE_MIN_MEDIA_FILES) or \
           (errors > FAILSAFE_MAX_INDEX_ERRORS):
            ok = False
    return ok, stats

# =========================
# Stage 1: enumerate torrents, filter & collect wanted sizes
# =========================
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
                shield.add((st.st_dev, st.st_ino)); protected += 1
    if shield:
        log(f"🛡 Active inode shield: {len(shield)} unique files from {protected} media entries.")
    else:
        log("🛡 Active inode shield: empty.")
    return shield

def collect_torrent_candidates(qb, torrents, active_shield):
    """
    Returns:
      wanted_sizes: set of sizes we must index in MEDIA_DIRS
      t_candidates: {hash: [{'path':..., 'size':..., 'dev':..., 'ino':..., 'nlink':...}, ...]}
      meta: counters
    """
    wanted_sizes = set()
    t_candidates = {}
    skipped_active = skipped_recent = skipped_min_age = skipped_shield = 0
    for t in torrents:
        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue
        if is_actively_seeding(t):
            skipped_active += 1; continue
        if is_recently_active(t, ACTIVE_GRACE_MINUTES):
            skipped_recent += 1; continue
        if is_too_new(t, MIN_COMPLETED_AGE_HOURS):
            skipped_min_age += 1; continue

        try:
            files = qb.get_torrent_files(t['hash'])
        except Exception:
            files = []

        cand_list = []
        shield_hit = False
        for fi in files:
            p = os.path.join(t['save_path'], fi['name'])
            try:
                st = os.stat(p)
            except Exception:
                continue
            if not is_media_candidate(p, st.st_size):
                continue
            if ACTIVE_INODE_SHIELD and (st.st_dev, st.st_ino) in active_shield:
                shield_hit = True; break
            if st.st_nlink and st.st_nlink > 1:
                cand_list.append({'path': p, 'size': st.st_size, 'dev': st.st_dev,
                                  'ino': st.st_ino, 'nlink': st.st_nlink})
                wanted_sizes.add(st.st_size)

        if shield_hit:
            skipped_shield += 1
            continue

        if cand_list:
            t_candidates[t['hash']] = cand_list

    meta = dict(skipped_active=skipped_active,
                skipped_recent=skipped_recent,
                skipped_min_age=skipped_min_age,
                skipped_shield=skipped_shield)
    return wanted_sizes, t_candidates, meta

# =========================
# Stage 2: build media signature set with persistent cache
# =========================
def build_media_signature_set(wanted_sizes, budget):
    """
    Returns:
      sig_set: set of (size, quickhash)
      index_stats: dict with counts/timings + index_complete flag
      cache_updated: bool
    """
    sig_set = set()
    files_seen = set()
    hashed_new = 0
    cached_hits = 0
    errors = 0
    start = time.time()

    cache_ok = _ensure_dir(CACHE_DIR)
    media_cache_path = os.path.join(CACHE_DIR, 'media_hashes.json') if cache_ok else None
    media_cache = _load_json(media_cache_path, {"entries": {}, "dir_fingerprints": {}}) if media_cache_path else {"entries": {}, "dir_fingerprints": {}}

    entries = media_cache.get("entries", {})
    dir_fingerprints = media_cache.get("dir_fingerprints", {})

    # Walk only wanted sizes
    for media_dir in MEDIA_DIRS:
        if not _dir_accessible(media_dir):
            continue
        for root, dirs, files in os.walk(media_dir):
            fp = _dir_fingerprint(root, entry_count=len(files) + len(dirs))
            cached_fp = dir_fingerprints.get(root)
            if fp is not None:
                dir_fingerprints[root] = fp
            if fp is not None and cached_fp is not None and fp == cached_fp and root not in files_seen:
                prefix = root + os.sep
                for path, ent in entries.items():
                    if not path.startswith(prefix):
                        continue
                    sz = ent.get('size')
                    qh = ent.get('qhash')
                    if sz not in wanted_sizes or not qh:
                        continue
                    if not is_media_candidate(path, sz):
                        continue
                    files_seen.add(path)
                    sig_set.add((sz, qh))
                    cached_hits += 1
                files_seen.add(root)
                dirs[:] = []
                continue
            for fn in files:
                path = os.path.join(root, fn)
                try:
                    st = os.stat(path)
                except Exception:
                    errors += 1; continue
                sz = st.st_size
                if sz not in wanted_sizes:
                    continue
                if not is_media_candidate(path, sz):
                    continue

                key = path
                files_seen.add(key)
                ent = entries.get(key)
                # cache valid?
                if ent and ent.get('size') == sz and ent.get('mtime') == int(st.st_mtime) \
                   and ent.get('ino') == st.st_ino and ent.get('dev') == st.st_dev \
                   and ent.get('qhash'):
                    sig_set.add((sz, ent['qhash'])); cached_hits += 1
                    continue

                # compute quickhash (budgeted)
                qh = quick_hash_budgeted(path, budget)
                if not qh:
                    # no hash -> cannot include in signature set
                    continue
                hashed_new += 1
                entries[key] = {
                    'size': sz, 'mtime': int(st.st_mtime),
                    'ino': st.st_ino, 'dev': st.st_dev, 'qhash': qh
                }
                sig_set.add((sz, qh))

    # prune removed files from cache
    removed = 0
    if entries:
        for k in list(entries.keys()):
            if k not in files_seen and entries[k].get('size') in wanted_sizes:
                entries.pop(k, None); removed += 1

    media_cache['entries'] = entries
    media_cache['dir_fingerprints'] = dir_fingerprints
    cache_updated = False
    if media_cache_path:
        try:
            _atomic_save_json(media_cache_path, media_cache)
            cache_updated = True
        except Exception:
            pass

    secs = time.time() - start
    index_complete = not budget.exhausted  # if budget ran out, we might have missed hashes
    index_stats = {
        'sig_count': len(sig_set),
        'cached_hits': cached_hits,
        'hashed_new': hashed_new,
        'cache_pruned': removed,
        'errors': errors,
        'elapsed': secs,
        'index_complete': index_complete,
        'budget_used_mb': (budget.total - budget.remaining) // (1024*1024),
        'budget_total_mb': budget.total // (1024*1024),
    }
    log(f"🔎 Media signatures: sigs={len(sig_set)}, cached={cached_hits}, new_hashes={hashed_new}, "
        f"pruned={removed}, errors={errors}, in {secs:.1f}s, budget={index_stats['budget_used_mb']}/{index_stats['budget_total_mb']} MiB.")
    if not index_complete:
        log("⚠ Signature index incomplete (hash budget exhausted). Will skip tagging where a signature match is required.")
    return sig_set, index_stats, cache_updated

# =========================
# Decision cache (per torrent)
# =========================
def load_torrent_cache():
    path = os.path.join(CACHE_DIR, 'torrent_state.json') if _ensure_dir(CACHE_DIR) else None
    raw = _load_json(path, {}) if path else {}
    current_cfg = {
        'media_link_min_percent': MEDIA_LINK_MIN_PERCENT,
        'media_link_tag_steps': MEDIA_LINK_TAG_STEPS,
    }

    if isinstance(raw, dict) and 'entries' in raw:
        entries = raw.get('entries') or {}
        cached_cfg = raw.get('config') or {}
    elif isinstance(raw, dict):
        entries = raw
        cached_cfg = {}
    else:
        entries = {}
        cached_cfg = {}

    if cached_cfg != current_cfg and entries:
        log("ℹ️ Torrent cache config changed, invalidating entries.")
        entries = {}

    return path, {'entries': entries, 'config': current_cfg}

def save_torrent_cache(path, data):
    if not path:
        return
    payload = data
    if not isinstance(payload, dict) or 'entries' not in payload:
        payload = {'entries': data}
    payload.setdefault('config', {
        'media_link_min_percent': MEDIA_LINK_MIN_PERCENT,
        'media_link_tag_steps': MEDIA_LINK_TAG_STEPS,
    })
    try:
        _atomic_save_json(path, payload)
    except Exception:
        pass

def can_reuse_decision(t, tstate, existing_tags):
    if DECISION_TTL_HOURS <= 0:
        return False
    entries = tstate.get('entries', tstate)
    entry = entries.get(t.get('hash'))
    if not entry:
        return False
    if entry.get('completion_on') != t.get('completion_on'):
        return False
    if entry.get('save_path') != t.get('save_path'):
        return False
    if int(time.time()) - entry.get('decided_at', 0) > DECISION_TTL_HOURS * 3600:
        return False

    coverage_tags_present = {tag for tag in existing_tags if tag.startswith(MEDIA_LINK_TAG_PREFIX)}
    expected_coverage_tag = entry.get('coverage_tag')
    coverage_ok = coverage_tags_present == ({expected_coverage_tag} if expected_coverage_tag else set())

    decision = entry.get('decision')
    if decision == 'orphan':
        decision_ok = ORPHAN_TAG in existing_tags
    elif decision == 'linked':
        decision_ok = ORPHAN_TAG not in existing_tags
    else:
        decision_ok = False

    return decision_ok and coverage_ok

def remember_decision(t, tstate, decision, coverage_pct=None, coverage_tag=None):
    entries = tstate.setdefault('entries', {})
    entries[t['hash']] = {
        'completion_on': t.get('completion_on'),
        'save_path': t.get('save_path'),
        'decision': decision,  # 'linked' or 'orphan'
        'decided_at': int(time.time()),
        'coverage_pct': coverage_pct,
        'coverage_tag': coverage_tag,
    }

# =========================
# Evaluate torrents with sig set
# =========================
def evaluate_and_tag(qb, torrents, t_candidates, sig_set, index_complete, tstate):
    orphan_batch, untag_batch = [], []
    total_tagged = total_untagged = 0
    skipped_reuse = skipped_inconclusive = 0

    coverage_add = defaultdict(list)  # tag -> [hashes]
    coverage_remove = defaultdict(list)  # tag -> [hashes]

    for i, t in enumerate(torrents, 1):
        if MAX_TORRENTS > 0 and i > MAX_TORRENTS:
            break
        if not t['save_path'].startswith(DOWNLOADS_DIR):
            continue
        h = t['hash']
        cand_files = t_candidates.get(h)
        if not cand_files:
            # no eligible files -> treat as "not linked" only if we choose to; safer to require evidence
            continue

        existing_tags = set((t.get('tags') or '').split(',')) if t.get('tags') else set()

        # decision reuse? only if tags already reflect cached decision/coverage
        if can_reuse_decision(t, tstate, existing_tags):
            skipped_reuse += 1
            continue

        # If our media signatures are incomplete, be conservative: skip
        if not index_complete:
            skipped_inconclusive += 1
            continue

        # Check each candidate torrent file: (size, qhash) in sig_set ?
        linked_matches = 0
        linked_bytes = 0
        total_bytes = sum(cf['size'] for cf in cand_files)
        inconclusive = False
        for cf in cand_files:
            tqh = quick_hash_budgeted(cf['path'], TORRENT_HASH_BUDGET)
            if not tqh:
                inconclusive = True
                break
            if (cf['size'], tqh) in sig_set:
                linked_matches += 1
                linked_bytes += cf['size']

        if inconclusive:
            skipped_inconclusive += 1
            continue

        coverage_pct = int((linked_bytes / total_bytes) * 100) if total_bytes > 0 else 0
        linked_enough = linked_bytes > 0 and coverage_pct >= MEDIA_LINK_MIN_PERCENT

        # Optional coverage tags (best matching threshold)
        coverage_tag = None
        if MEDIA_LINK_TAG_STEPS:
            for step in MEDIA_LINK_TAG_STEPS:
                if coverage_pct >= step:
                    coverage_tag = f"{MEDIA_LINK_TAG_PREFIX}{step}%"
                else:
                    break

        coverage_tags_present = {tag for tag in existing_tags if tag.startswith(MEDIA_LINK_TAG_PREFIX)}

        if coverage_tag and coverage_tag not in existing_tags:
            coverage_add[coverage_tag].append(h)
        for tag in coverage_tags_present:
            if tag != coverage_tag:
                coverage_remove[tag].append(h)

        has_tag = ORPHAN_TAG in (t.get('tags') or '')
        if not linked_enough:
            orphan_batch.append(h)
            if len(orphan_batch) >= BATCH_SIZE:
                total_tagged += add_tag_http(orphan_batch, ORPHAN_TAG); orphan_batch.clear()
            remember_decision(t, tstate, 'orphan', coverage_pct, coverage_tag)
        else:
            if has_tag:
                untag_batch.append(h)
                if len(untag_batch) >= BATCH_SIZE:
                    total_untagged += remove_tag_http(untag_batch, ORPHAN_TAG); untag_batch.clear()
            remember_decision(t, tstate, 'linked', coverage_pct, coverage_tag)

    if orphan_batch:
        total_tagged += add_tag_http(orphan_batch, ORPHAN_TAG)
    if untag_batch:
        total_untagged += remove_tag_http(untag_batch, ORPHAN_TAG)

    # Apply coverage tags
    for tag, hashes in coverage_add.items():
        add_tag_http(hashes, tag)
    for tag, hashes in coverage_remove.items():
        remove_tag_http(hashes, tag)

    return {
        'tagged': total_tagged,
        'untagged': total_untagged,
        'skipped_reuse': skipped_reuse,
        'skipped_inconclusive': skipped_inconclusive
    }

# Single budget instance used across run (media + torrents)
TORRENT_HASH_BUDGET = None  # will be set per run

def run_cleanup():
    if not all([QBITTORRENT_URL, QBITTORRENT_USER, QBITTORRENT_PASS]):
        raise SystemExit("Missing qBittorrent env vars.")

    global TORRENT_HASH_BUDGET
    log(f"{VERSION} — url={QBITTORRENT_URL} — MIN_SIZE_MB={MIN_SIZE_MB} — EXT_WHITELIST={','.join(EXT_WHITELIST)} "
        f"— ACTIVE_GRACE_MINUTES={ACTIVE_GRACE_MINUTES} — MIN_COMPLETED_AGE_HOURS={MIN_COMPLETED_AGE_HOURS} "
        f"— ACTIVE_INODE_SHIELD={int(ACTIVE_INODE_SHIELD)} — HASH_BUDGET_MB={HASH_BUDGET_MB} "
        f"— DECISION_TTL_HOURS={DECISION_TTL_HOURS} — CACHE_DIR={CACHE_DIR} "
        f"— MEDIA_LINK_MIN_PERCENT={MEDIA_LINK_MIN_PERCENT} "
        f"— MEDIA_LINK_TAG_STEPS={MEDIA_LINK_TAG_STEPS if MEDIA_LINK_TAG_STEPS else 'disabled'} "
        f"— MEDIA_LINK_TAG_PREFIX='{MEDIA_LINK_TAG_PREFIX}' "
        f"— LOG_USE_AMPM={int(LOG_USE_AMPM)}")
    log("Starting cleanup cycle...")

    qb = get_qb_client()
    if not qb:
        log("No connection to qBittorrent, skipping.")
        return

    vis_ok, _ = build_media_visibility_stats()
    if not vis_ok:
        log("🛑 FAILSAFE: Library visibility not healthy. **No tag changes this cycle.**")
        return

    try:
        torrents = qb.torrents()
    except Exception as e:
        log(f"Error fetching torrents: {e}")
        return

    if MAX_TORRENTS > 0:
        torrents = torrents[:MAX_TORRENTS]
        log(f"⚙ Limiting to first {MAX_TORRENTS} torrents.")

    # Build active inode shield
    active_shield = build_active_inode_shield(qb, torrents) if ACTIVE_INODE_SHIELD else set()

    # Stage 1: filter + collect wanted sizes and candidate files
    wanted_sizes, t_candidates, meta = collect_torrent_candidates(qb, torrents, active_shield)
    log(f"🎯 Stage1: wanted_sizes={len(wanted_sizes)}, candidates={len(t_candidates)} torrents; "
        f"skipped_active={meta['skipped_active']}, skipped_recent={meta['skipped_recent']}, "
        f"skipped_min_age={meta['skipped_min_age']}, shield_skips={meta['skipped_shield']}.")

    # Budget: we split between media and torrents dynamically; start with full, consume as we go
    budget = Budget(HASH_BUDGET_MB)
    # Stage 2: build media signature set only for sizes we actually care about
    sig_set, idx_stats, _ = build_media_signature_set(wanted_sizes, budget)

    # Whatever remains in the budget is available for torrent quickhashes
    TORRENT_HASH_BUDGET = budget  # pass the same budget into torrent hashing

    # Load decision cache
    tcache_path, tcache = load_torrent_cache()

    # Stage 3: evaluate + tag using signature set
    results = evaluate_and_tag(qb, torrents, t_candidates, sig_set, idx_stats['index_complete'], tcache)

    # Save decision cache
    save_torrent_cache(tcache_path, tcache)

    log(f"📊 Summary: tagged={results['tagged']}, untagged={results['untagged']}, "
        f"reuse_skips={results['skipped_reuse']}, inconclusive_skips={results['skipped_inconclusive']}, "
        f"budget_used={idx_stats['budget_used_mb']}/{idx_stats['budget_total_mb']} MiB.")
    log("Cleanup cycle complete.")

if __name__ == "__main__":
    while True:
        try:
            run_cleanup()
        except Exception as e:
            log(f"💥 Unhandled error: {e}")
        log(f"Waiting {DEBUG_INTERVAL} seconds before next run...")
        time.sleep(DEBUG_INTERVAL)
