import os
import json
import time
import re
import requests
from sys import stderr
from loguru import logger
from datetime import datetime
from collections import defaultdict

# --- .env support ---
ENV_PATH = os.getenv("ENV_PATH", ".env")
try:
    from dotenv import load_dotenv  # type: ignore
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH, override=False)
except Exception:
    def _load_env_fallback(path: str):
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"\'')
                os.environ.setdefault(k, v)

    _load_env_fallback(ENV_PATH)

try:
    from msvcrt import getch  # Windows
except Exception:
    def getch():
        try:
            input("Press Enter to exit...")
        except Exception:
            pass


API_BASE = "https://discord.com/api/v9"
SOCIALDATA_BASE = "https://api.socialdata.tools/twitter"

CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoint.json")
EXPORT_PATH = os.getenv("EXPORT_PATH", "user_stats.json")

# message exports
MESSAGES_JSONL_PATH = os.getenv("MESSAGES_JSONL_PATH", "messages.jsonl")
MESSAGES_EXPORT_PATH = os.getenv("MESSAGES_EXPORT_PATH", "message.json")
BUILD_MESSAGE_JSON = os.getenv("BUILD_MESSAGE_JSON", "1") == "1"
FETCH_ROLES = os.getenv("FETCH_ROLES", "1") == "1"  # 1=on, 0=off

# twitter exports
POSTS_EXPORT_PATH = os.getenv("POSTS_EXPORT_PATH", "posts.json")
SOCIALDATA_CACHE_PATH = os.getenv("SOCIALDATA_CACHE_PATH", "socialdata_cache.json")
BUILD_TWITTER_STATS = os.getenv("BUILD_TWITTER_STATS", "1") == "1"

TWITTER_LINKS_CHANNEL_ID = str(os.getenv("TWITTER_LINKS_CHANNEL_ID", "1470671171602219008"))

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
GUILD_ID = os.getenv("GUILD_ID", "")
MODE = os.getenv("MODE", "backfill").lower()

# SocialData API key (Bearer)
SOCIALDATA_API_KEY = os.getenv("SOCIALDATA_API_KEY", "")

HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "10"))
LOG_EVERY_PAGES = int(os.getenv("LOG_EVERY_PAGES", "50"))

logger.remove()
logger.add(
    stderr,
    format="<white>{time:HH:mm:ss}</white> | <level>{level: <8}</level> | <white>{message}</white>"
)

if not DISCORD_TOKEN:
    print("ERROR: DISCORD_TOKEN is empty (env DISCORD_TOKEN).")
    getch()
    raise SystemExit

if not GUILD_ID or not str(GUILD_ID).isdigit():
    print("ERROR: GUILD_ID is empty or not numeric (env GUILD_ID).")
    getch()
    raise SystemExit

if BUILD_TWITTER_STATS and not SOCIALDATA_API_KEY:
    logger.warning("SOCIALDATA_API_KEY is empty. Twitter stats will be skipped.")


class NoAccessError(Exception):
    pass


session = requests.Session()
session.headers.update({
    "authorization": DISCORD_TOKEN,
    "user-agent": "Mozilla/5.0",
    "accept-encoding": "gzip, deflate",
})


def get_json(url: str, max_retries: int = 8, timeout: int = 25):
    """Discord GET with retries and rate-limit handling."""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout)

            if r.status_code == 429:
                try:
                    ra = float(r.json().get("retry_after", 1.5))
                except Exception:
                    ra = 1.5
                sleep_s = max(ra, 0.2)
                logger.warning(f"429 rate limit. Sleep {sleep_s:.2f}s")
                time.sleep(sleep_s)
                continue

            if r.status_code == 403:
                try:
                    j = r.json()
                    if j.get("code") == 50001:
                        raise NoAccessError()
                except NoAccessError:
                    raise
                except Exception:
                    pass

            # 404s for missing/left members/users should NOT be retried.
            # Discord error codes:
            # - 10007: Unknown Member (user not in guild)
            # - 10013: Unknown User (user does not exist)
            if r.status_code == 404:
                try:
                    j = r.json()
                    if j.get("code") in (10007, 10013):
                        return None
                except Exception:
                    # If we can't parse JSON, still treat members endpoint 404 as non-retry
                    if "/members/" in url:
                        return None

            if r.status_code >= 400:
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                raise RuntimeError(last_err)

            return r.json()

        except NoAccessError:
            raise
        except Exception as e:
            last_err = str(e)
            if attempt == max_retries:
                logger.error(f"GET failed (attempt {attempt}/{max_retries}): {url} | err={last_err}")
                raise
            sleep_s = min(2 ** attempt, 30)
            logger.warning(f"GET retry (attempt {attempt}/{max_retries}) in {sleep_s}s: {url} | err={last_err}")
            time.sleep(sleep_s)

    raise RuntimeError(last_err or "Unknown error")


def build_avatar_url(author: dict):
    uid = author.get("id")
    ah = author.get("avatar")
    if uid and ah:
        return f"https://cdn.discordapp.com/avatars/{uid}/{ah}.png?size=128"
    return None


def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            cp = json.load(f)
        logger.info(f"Loaded checkpoint: users={len(cp.get('users', {}))} channels={len(cp.get('channels', {}))}")
    else:
        cp = {"meta": {}, "channels": {}, "users": {}, "channel_names": {}}
        logger.info("No checkpoint found. Starting fresh.")

    cp.setdefault("meta", {})
    cp.setdefault("channels", {})
    cp.setdefault("users", {})
    cp.setdefault("channel_names", {})

    # roles & member display names
    cp.setdefault("roles", {})
    cp.setdefault("member_roles", {})
    cp.setdefault("member_display", {})

    # twitter discovery
    cp.setdefault("twitter_handles", {})
    cp.setdefault("twitter_best", {})
    cp.setdefault("twitter_links", {})

    # twitter stats
    cp.setdefault("twitter_stats", {})

    return cp


def save_checkpoint(cp, reason: str = ""):
    cp["meta"]["generated_at"] = datetime.utcnow().isoformat() + "Z"
    tmp = CHECKPOINT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CHECKPOINT_PATH)
    logger.info(f"Checkpoint saved{(' (' + reason + ')') if reason else ''}: {CHECKPOINT_PATH}")


def ensure_user(cp, uid: str, username: str, pfp: str):
    users = cp["users"]
    if uid not in users:
        users[uid] = {
            "id": uid,
            "user_name": username,
            "pfp": pfp,
            "by_channel": {},
        }
    else:
        u = users[uid]
        if username:
            u["user_name"] = username
        if pfp:
            u["pfp"] = pfp


def commit_channel_aggregate(cp, channel_id: str, channel_agg: dict):
    for uid, info in channel_agg.items():
        ensure_user(cp, uid, info.get("user_name"), info.get("pfp"))
        u = cp["users"][uid]
        bc = u["by_channel"]
        bc[channel_id] = int(bc.get(channel_id, 0)) + int(info.get("count", 0))


# ===== guild/channels =====
def ensure_server_name(cp, guild_id: str):
    if cp["meta"].get("guild_name"):
        return
    g = get_json(f"{API_BASE}/guilds/{guild_id}")
    cp["meta"]["guild_id"] = str(guild_id)
    cp["meta"]["guild_name"] = g.get("name")
    logger.success(f"Guild: {cp['meta']['guild_name']} | guild_id={guild_id}")


def list_text_channels(guild_id: str):
    chans = get_json(f"{API_BASE}/guilds/{guild_id}/channels")
    out = []
    for c in chans:
        if c.get("type") == 0:
            out.append({"id": str(c["id"]), "name": c.get("name")})
    logger.info(f"Fetched channels: total={len(chans)} | text={len(out)}")
    return out


# ===== roles + members =====
def fetch_roles(cp, guild_id: str):
    roles = get_json(f"{API_BASE}/guilds/{guild_id}/roles")
    role_map = {str(r.get("id")): r.get("name") for r in roles}
    cp["roles"] = role_map
    logger.success(f"Fetched roles: {len(role_map)}")


def _compute_display_name(member: dict):
    nick = member.get("nick")
    user = member.get("user") or {}
    global_name = user.get("global_name")
    username = user.get("username")
    return nick or global_name or username


def fetch_all_members_roles(cp, guild_id: str):
    """
    Fetch all members using /members?limit=1000&after=...
    Stores:
      - member_roles: user_id -> [role_name,...]
      - member_display: user_id -> display name on server

    NOTE: This endpoint can return 403 depending on token/permissions.
    """
    role_map = cp.get("roles") or {}
    out_roles = {}
    out_display = {}

    after = "0"
    total = 0
    pages = 0

    while True:
        url = f"{API_BASE}/guilds/{guild_id}/members?limit=1000&after={after}"
        data = get_json(url)
        if not data:
            break
        pages += 1

        for m in data:
            u = m.get("user") or {}
            uid = str(u.get("id"))
            if not uid:
                continue

            role_ids = m.get("roles") or []
            role_names = [role_map.get(str(rid), str(rid)) for rid in role_ids]
            out_roles[uid] = role_names
            out_display[uid] = _compute_display_name(m)
            total += 1

        after = str((data[-1].get("user") or {}).get("id") or after)
        if pages % 5 == 0:
            logger.info(f"Members fetched: {total} (pages={pages})")
        time.sleep(0.2)

    cp["member_roles"] = out_roles
    cp["member_display"] = out_display
    logger.success(f"Fetched member roles: members={len(out_roles)}")


def fetch_member_fallback(cp, guild_id: str, uid: str):
    """Fallback: fetch single member roles/display name."""
    role_map = cp.get("roles") or {}
    try:
        m = get_json(f"{API_BASE}/guilds/{guild_id}/members/{uid}")
    except NoAccessError:
        return "no_access"
    except Exception:
        return "error"

    if not m:
        return "missing"

    role_ids = m.get("roles") or []
    role_names = [role_map.get(str(rid), str(rid)) for rid in role_ids]
    cp.setdefault("member_roles", {})[uid] = role_names
    cp.setdefault("member_display", {})[uid] = _compute_display_name(m)
    return "ok"


def run_roles_fallback_for_seen_users(cp, guild_id: str, reason: str = ""):
    """Runs per-user member fetch to populate roles/display for all seen users, with progress logs."""
    uids = list((cp.get("users") or {}).keys())
    total = len(uids)
    if total == 0:
        return

    sleep_s = float(os.getenv("FALLBACK_SLEEP_SEC", "0.15"))
    log_every = int(os.getenv("FALLBACK_LOG_EVERY", "50"))

    processed = ok = missing = no_access = errors = 0
    started = time.time()
    last_log = started
    logger.info(
        f"[ROLES-FB] Start fallback for seen users: total={total} sleep={sleep_s}s "
        f"{('(' + reason + ')') if reason else ''}"
    )

    for uid in uids:
        processed += 1
        st = fetch_member_fallback(cp, guild_id, uid)
        if st == "ok":
            ok += 1
        elif st == "missing":
            missing += 1
        elif st == "no_access":
            no_access += 1
        else:
            errors += 1

        now = time.time()
        if processed % log_every == 0 or (now - last_log) >= HEARTBEAT_SEC:
            elapsed = max(now - started, 1e-6)
            speed = processed / elapsed
            eta_s = (total - processed) / speed if speed > 0 else 0
            logger.info(
                f"[ROLES-FB] {processed}/{total} ok={ok} missing={missing} no_access={no_access} errors={errors} "
                f"speed={speed:.2f} users/s eta={eta_s/60:.1f}m"
            )
            last_log = now

        if sleep_s > 0:
            time.sleep(sleep_s)

    elapsed = max(time.time() - started, 1e-6)
    logger.success(
        f"[ROLES-FB] Done: processed={processed} ok={ok} missing={missing} no_access={no_access} errors={errors} "
        f"avg={processed/elapsed:.2f} users/s time={elapsed/60:.1f}m"
    )


# ===== twitter handle extraction =====
TW_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:x|twitter)\.com/([A-Za-z0-9_]{1,15})(?:/status/(\d+))?",
    re.IGNORECASE,
)


def extract_twitter_links(text: str):
    """Returns list of dicts: {handle, tweet_id (or None), url}."""
    if not text:
        return []
    out = []
    for m in TW_URL_RE.finditer(text):
        handle = m.group(1)
        tid = m.group(2)
        url = m.group(0)
        if handle:
            out.append({"handle": handle, "tweet_id": tid, "url": url})
    return out


def update_twitter_from_message(cp, uid: str, msg_content: str):
    links = extract_twitter_links(msg_content)
    if not links:
        return

    # counts per handle from raw URLs
    th = cp.setdefault("twitter_handles", {})
    per = th.get(uid) or {}
    for x in links:
        h = (x.get("handle") or "").strip()
        if not h:
            continue
        per[h] = int(per.get(h, 0)) + 1
    th[uid] = per

    # store raw links
    tl = cp.setdefault("twitter_links", {})
    arr = tl.get(uid) or []
    arr.extend(links)
    tl[uid] = arr

    # provisional best handle from URLs
    best_h, best_c = None, -1
    for h, c in per.items():
        if c > best_c or (c == best_c and (best_h is None or h < best_h)):
            best_h, best_c = h, c
    if best_h:
        cp.setdefault("twitter_best", {})[uid] = best_h


# ===== message logging =====
def append_message_jsonl(record: dict):
    with open(MESSAGES_JSONL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def log_message(uid: str, user_name: str, msg: dict):
    rec = {
        "user_id": uid,
        "user_name": user_name,
        "timestamp": msg.get("timestamp"),
        "content": msg.get("content") or "",
    }
    append_message_jsonl(rec)


def build_message_json():
    """Builds message.json = { user_id: [ {user_name,timestamp,content}, ... ] }"""
    if not os.path.exists(MESSAGES_JSONL_PATH):
        logger.warning(f"No {MESSAGES_JSONL_PATH} found, skip building {MESSAGES_EXPORT_PATH}")
        return

    grouped = defaultdict(list)
    seen = defaultdict(set)

    total = 0
    with open(MESSAGES_JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            uid = rec.get("user_id")
            ts = rec.get("timestamp") or ""
            content = rec.get("content")
            uname = rec.get("user_name")
            if not uid:
                continue

            key = (ts, content)
            if key in seen[uid]:
                continue
            seen[uid].add(key)

            grouped[uid].append({
                "user_name": uname,
                "timestamp": ts,
                "content": content
            })
            total += 1

            if total % 200000 == 0:
                logger.info(f"Building message.json: loaded={total}")

    for uid, arr in grouped.items():
        arr.sort(key=lambda x: (x.get("timestamp") or ""))

    with open(MESSAGES_EXPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(grouped, f, ensure_ascii=False, indent=2)

    logger.success(f"Message export saved: {MESSAGES_EXPORT_PATH} | users={len(grouped)} | messages={total}")


# ===== SocialData helpers =====
def load_socialdata_cache():
    if os.path.exists(SOCIALDATA_CACHE_PATH):
        try:
            with open(SOCIALDATA_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_socialdata_cache(cache: dict):
    tmp = SOCIALDATA_CACHE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SOCIALDATA_CACHE_PATH)


def socialdata_get_tweet(tweet_id: str, cache: dict, max_retries: int = 6):
    if tweet_id in cache:
        return cache[tweet_id]
    if not SOCIALDATA_API_KEY:
        return None

    url = f"{SOCIALDATA_BASE}/tweets/{tweet_id}"
    headers = {"Authorization": f"Bearer {SOCIALDATA_API_KEY}", "Accept": "application/json"}

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 429:
                sleep_s = min(2 ** attempt, 30)
                logger.warning(f"SocialData 429. Sleep {sleep_s}s")
                time.sleep(sleep_s)
                continue
            if r.status_code == 402:
                logger.error("SocialData: insufficient balance (402).")
                return None
            if r.status_code >= 400:
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                raise RuntimeError(last_err)

            data = r.json()
            cache[tweet_id] = data
            return data
        except Exception as e:
            last_err = str(e)
            if attempt == max_retries:
                logger.warning(f"SocialData GET tweet failed: {tweet_id} | err={last_err}")
                return None
            sleep_s = min(2 ** attempt, 30)
            time.sleep(sleep_s)

    logger.warning(f"SocialData GET tweet failed: {tweet_id} | err={last_err}")
    return None


def tweet_type(sd: dict):
    if sd.get("retweeted_status") is not None:
        return "retweet"
    if sd.get("in_reply_to_status_id") is not None:
        return "reply"
    if sd.get("is_quote_status") is True or sd.get("quoted_status_id") is not None:
        return "quote"
    return "post"


def extract_media_url(sd: dict):
    for key in ("extended_entities", "entities"):
        ent = sd.get(key) or {}
        media = ent.get("media") or []
        for m in media:
            u = m.get("media_url_https") or m.get("media_url")
            if u:
                return u
            u = m.get("url")
            if u and u.startswith("http"):
                return u
    return None


def extract_tweet_author(sd: dict):
    """
    Best-effort extraction of tweet author from SocialData payload.
    Supports several possible payload shapes.
    """
    user = sd.get("user") or sd.get("author") or sd.get("users") or {}

    if isinstance(user, list):
        user = user[0] if user else {}
    if not isinstance(user, dict):
        user = {}

    author_id = (
        user.get("id")
        or user.get("user_id")
        or sd.get("user_id")
        or sd.get("author_id")
    )

    handle = (
        user.get("screen_name")
        or user.get("username")
        or user.get("handle")
        or sd.get("screen_name")
        or sd.get("username")
    )

    name = (
        user.get("name")
        or sd.get("name")
    )

    return {
        "author_id": str(author_id) if author_id is not None else None,
        "handle": handle,
        "name": name,
    }


def build_twitter_stats_and_posts(cp):
    """
    Updated logic:
    - collect ALL tweet_ids ever posted by a Discord user from twitter_links[uid]
    - DO NOT filter by handle from URL
    - fetch each tweet from SocialData
    - determine dominant real author for this Discord user
    - aggregate only tweets belonging to that dominant author
    - "post" = total count of all tweets in filtered
    - field names remain unchanged
    """
    if not SOCIALDATA_API_KEY:
        logger.warning("Skipping Twitter stats: SOCIALDATA_API_KEY missing")
        return

    cache = load_socialdata_cache()

    twitter_links = cp.get("twitter_links", {})
    member_display = cp.get("member_display", {})

    posts_out = []
    stats_out = {}
    best_out = {}

    total_tweets_fetched = 0

    for uid, links in twitter_links.items():
        tweet_ids = []
        for x in links:
            tid = x.get("tweet_id")
            if tid and str(tid).isdigit():
                tweet_ids.append(str(tid))

        tweet_ids = list(dict.fromkeys(tweet_ids))
        if not tweet_ids:
            continue

        loaded = []
        author_counts = {}
        author_handle_by_key = {}

        for tid in tweet_ids:
            sd = socialdata_get_tweet(tid, cache)
            if not sd or sd.get("status") == "error":
                continue

            author = extract_tweet_author(sd)
            author_key = author.get("author_id") or ((author.get("handle") or "").lower() or None)

            if author_key:
                author_counts[author_key] = author_counts.get(author_key, 0) + 1
                if author.get("handle") and author_key not in author_handle_by_key:
                    author_handle_by_key[author_key] = author.get("handle")

            loaded.append((tid, sd, author))

            total_tweets_fetched += 1
            if total_tweets_fetched % 100 == 0:
                logger.info(f"SocialData fetched tweets: {total_tweets_fetched}")

        if not loaded:
            continue

        best_author_key = None
        best_author_count = -1
        for k, c in author_counts.items():
            if c > best_author_count or (c == best_author_count and (best_author_key is None or str(k) < str(best_author_key))):
                best_author_key = k
                best_author_count = c

        filtered = []
        for tid, sd, author in loaded:
            author_key = author.get("author_id") or ((author.get("handle") or "").lower() or None)
            if best_author_key is None or author_key == best_author_key:
                filtered.append((tid, sd, author))

        if not filtered:
            continue

        actual_handle = author_handle_by_key.get(best_author_key)
        if not actual_handle:
            for _, _, author in filtered:
                if author.get("handle"):
                    actual_handle = author["handle"]
                    break

        if actual_handle:
            best_out[uid] = actual_handle

        agg = {
            "post": 0,
            "like": 0,
            "reply": 0,
            "ретвит": 0,
            "цитата": 0,
            "посмотры": 0,
        }

        for tid, sd, author in filtered:
            likes = int(sd.get("favorite_count") or 0)
            replies = int(sd.get("reply_count") or 0)
            retweets = int(sd.get("retweet_count") or 0)
            quotes = int(sd.get("quote_count") or 0)
            views = int(sd.get("views_count") or 0) if sd.get("views_count") is not None else 0

            # post = total count of all found tweets for this user
            agg["post"] += 1
            agg["like"] += likes
            agg["reply"] += replies
            agg["ретвит"] += retweets
            agg["цитата"] += quotes
            agg["посмотры"] += views

            handle = author.get("handle") or actual_handle or "unknown"
            media_url = extract_media_url(sd)
            tweet_url = f"https://x.com/{handle}/status/{tid}"

            posts_out.append({
                "discord_name": member_display.get(uid) or (cp.get("users", {}).get(uid) or {}).get("user_name"),
                "twitter_name": handle,
                "tweet_author_id": author.get("author_id"),
                "tweet_id": tid,
                "tweet_url": tweet_url,
                "published_at": sd.get("tweet_created_at"),
                "type": tweet_type(sd),
                "metrics": {
                    "like": likes,
                    "reply": replies,
                    "ретвит": retweets,
                    "цитата": quotes,
                    "посмотры": views,
                },
                "image": media_url,
                "text": sd.get("full_text") or sd.get("text"),
            })

        stats_out[uid] = agg

    cp["twitter_stats"] = stats_out
    cp["twitter_best"] = best_out

    with open(POSTS_EXPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(posts_out, f, ensure_ascii=False, indent=2)
    logger.success(f"posts.json saved: {POSTS_EXPORT_PATH} | posts={len(posts_out)}")

    save_socialdata_cache(cache)


# ===== BACKFILL =====
def backfill_channel(cp, channel_id: str, channel_name: str):
    logger.info(f"[BACKFILL] Start #{channel_name} ({channel_id})")
    started = time.time()
    last_beat = started

    channel_agg = {}
    pages = 0
    scanned = 0

    try:
        first = get_json(f"{API_BASE}/channels/{channel_id}/messages?limit=100")
    except NoAccessError:
        logger.warning(f"[BACKFILL] SKIP no access #{channel_name} ({channel_id})")
        cp["channels"][channel_id] = {
            "name": channel_name,
            "last_seen_id": None,
            "backfill_done": False,
            "skipped_no_access": True
        }
        return

    if not first:
        logger.warning(f"[BACKFILL] Empty #{channel_name}")
        cp["channels"][channel_id] = {"name": channel_name, "last_seen_id": None, "backfill_done": True}
        return

    newest_id = max(int(m["id"]) for m in first)

    def consume(messages):
        nonlocal scanned
        scanned += len(messages)
        for msg in messages:
            author = msg.get("author") or {}
            uid = author.get("id")
            if not uid:
                continue

            username = author.get("username")
            pfp = build_avatar_url(author)

            if uid not in channel_agg:
                channel_agg[uid] = {"user_name": username, "pfp": pfp, "count": 0}
            channel_agg[uid]["count"] += 1
            if username:
                channel_agg[uid]["user_name"] = username
            if pfp:
                channel_agg[uid]["pfp"] = pfp

            try:
                log_message(str(uid), username, msg)
            except Exception as e:
                logger.warning(f"Failed to log message: {e}")

            if str(channel_id) == TWITTER_LINKS_CHANNEL_ID:
                update_twitter_from_message(cp, str(uid), msg.get("content") or "")

    consume(first)
    pages += 1
    last_before = first[-1]["id"]

    while True:
        now = time.time()
        if now - last_beat >= HEARTBEAT_SEC:
            elapsed = max(now - started, 1e-6)
            logger.info(
                f"[BACKFILL] #{channel_name} pages={pages} scanned={scanned} uniq_users={len(channel_agg)} speed={scanned/elapsed:.2f} msg/s"
            )
            last_beat = now

        try:
            data = get_json(f"{API_BASE}/channels/{channel_id}/messages?limit=100&before={last_before}")
        except NoAccessError:
            logger.warning(f"[BACKFILL] LOST access mid-channel, SKIP #{channel_name}")
            cp["channels"][channel_id] = {
                "name": channel_name,
                "last_seen_id": None,
                "backfill_done": False,
                "skipped_no_access": True
            }
            return

        if not data:
            break

        consume(data)
        pages += 1
        last_before = data[-1]["id"]

        if LOG_EVERY_PAGES and (pages % LOG_EVERY_PAGES == 0):
            elapsed = max(time.time() - started, 1e-6)
            logger.info(
                f"[BACKFILL] #{channel_name} pages={pages} scanned={scanned} uniq_users={len(channel_agg)} speed={scanned/elapsed:.2f} msg/s"
            )

    commit_channel_aggregate(cp, channel_id, channel_agg)
    cp["channels"][channel_id] = {"name": channel_name, "last_seen_id": str(newest_id), "backfill_done": True}

    elapsed = max(time.time() - started, 1e-6)
    logger.success(
        f"[BACKFILL] Done #{channel_name}: scanned={scanned} pages={pages} uniq_users={len(channel_agg)} avg={scanned/elapsed:.2f} msg/s"
    )


# ===== INCREMENTAL =====
def incremental_channel(cp, channel_id: str, channel_name: str):
    st = cp["channels"].get(channel_id) or {"name": channel_name, "last_seen_id": None, "backfill_done": False}
    after_id = st.get("last_seen_id")
    if not after_id:
        return 0

    logger.info(f"[INCR] Start #{channel_name} after={after_id}")

    added = 0
    max_id = int(after_id)
    started = time.time()
    last_beat = started

    while True:
        now = time.time()
        if now - last_beat >= HEARTBEAT_SEC:
            elapsed = max(now - started, 1e-6)
            logger.info(f"[INCR] #{channel_name} added={added} speed={added/elapsed:.2f} msg/s")
            last_beat = now

        try:
            data = get_json(f"{API_BASE}/channels/{channel_id}/messages?limit=100&after={after_id}")
        except NoAccessError:
            logger.warning(f"[INCR] SKIP no access #{channel_name} ({channel_id})")
            cp["channels"][channel_id] = {**st, "skipped_no_access": True}
            break

        if not data:
            break

        for msg in data:
            author = msg.get("author") or {}
            uid = author.get("id")
            if not uid:
                continue
            username = author.get("username")
            pfp = build_avatar_url(author)
            ensure_user(cp, str(uid), username, pfp)
            u = cp["users"][str(uid)]
            bc = u["by_channel"]
            bc[channel_id] = int(bc.get(channel_id, 0)) + 1

            try:
                log_message(str(uid), username, msg)
            except Exception as e:
                logger.warning(f"Failed to log message: {e}")

            if str(channel_id) == TWITTER_LINKS_CHANNEL_ID:
                update_twitter_from_message(cp, str(uid), msg.get("content") or "")

            mid = int(msg["id"])
            if mid > max_id:
                max_id = mid
            added += 1

        after_id = str(max_id)

    cp["channels"][channel_id] = {**st, "last_seen_id": str(max_id), "backfill_done": True}

    logger.success(f"[INCR] Done #{channel_name}: added={added} new_last_seen={max_id}")
    return added


# ===== EXPORT =====
def export_user_stats(cp):
    channel_name_by_id = cp.get("channel_names", {})
    member_roles = cp.get("member_roles", {})
    member_display = cp.get("member_display", {})
    twitter_best = cp.get("twitter_best", {})
    twitter_stats = cp.get("twitter_stats", {})

    out = []
    for uid, u in (cp.get("users") or {}).items():
        mc = {}
        for cid, n in (u.get("by_channel") or {}).items():
            cname = channel_name_by_id.get(cid) or cid
            mc[cname] = int(n)

        display_name = member_display.get(uid) or u.get("user_name")

        out.append({
            "id": u.get("id"),
            "server_name": display_name,
            "user_name": u.get("user_name"),
            "pfp": u.get("pfp"),
            "role": member_roles.get(uid, []),
            "twitter_name": twitter_best.get(uid),
            "twitter_stats": twitter_stats.get(uid),
            "message_count": mc,
        })

    out.sort(key=lambda x: sum((x.get("message_count") or {}).values()), reverse=True)

    with open(EXPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    logger.success(f"Export saved: {EXPORT_PATH} | users={len(out)}")


def main():
    logger.info(f"Boot | MODE={MODE} | guild_id={GUILD_ID} | twitter_channel={TWITTER_LINKS_CHANNEL_ID}")

    cp = load_checkpoint()
    ensure_server_name(cp, GUILD_ID)

    chans = list_text_channels(GUILD_ID)
    for c in chans:
        cp["channel_names"][str(c["id"])] = c.get("name") or str(c["id"])

    roles_ok = not FETCH_ROLES
    if FETCH_ROLES:
        try:
            fetch_roles(cp, GUILD_ID)
            fetch_all_members_roles(cp, GUILD_ID)
            roles_ok = True
        except Exception as e:
            logger.warning(f"Bulk roles fetch failed: {e}")

    if MODE == "backfill":
        for i, c in enumerate(chans, start=1):
            cid = str(c["id"])
            cname = c.get("name") or cid
            st = cp["channels"].get(cid) or {}
            if st.get("backfill_done") is True or st.get("skipped_no_access") is True:
                continue

            backfill_channel(cp, cid, cname)

            if i % 3 == 0:
                save_checkpoint(cp, reason="during backfill")

        if FETCH_ROLES and (not roles_ok) and cp.get("users"):
            run_roles_fallback_for_seen_users(cp, GUILD_ID, reason="bulk failed (backfill)")

        save_checkpoint(cp, reason="after backfill")

        if BUILD_TWITTER_STATS:
            try:
                build_twitter_stats_and_posts(cp)
            except Exception as e:
                logger.warning(f"Twitter stats build failed: {e}")

        export_user_stats(cp)
        if BUILD_MESSAGE_JSON:
            build_message_json()
        logger.success("Backfill complete.")
        return

    total_added = 0
    for c in chans:
        cid = str(c["id"])
        cname = c.get("name") or cid
        total_added += incremental_channel(cp, cid, cname)

    if FETCH_ROLES and (not roles_ok) and cp.get("users"):
        run_roles_fallback_for_seen_users(cp, GUILD_ID, reason="bulk failed (incremental)")

    save_checkpoint(cp, reason="after incremental")

    if BUILD_TWITTER_STATS:
        try:
            build_twitter_stats_and_posts(cp)
        except Exception as e:
            logger.warning(f"Twitter stats build failed: {e}")

    export_user_stats(cp)
    if BUILD_MESSAGE_JSON:
        build_message_json()
    logger.success(f"Incremental complete. added_messages={total_added}")


if __name__ == "__main__":
    main()