import os
import asyncio
import time
import io
import sys
import logging
from datetime import datetime, timezone

from pyrogram import Client, errors
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from dotenv import load_dotenv

# Image handling
try:
    from PIL import Image
    PIL_OK = True
except ImportError:
    PIL_OK = False

# Telegraph
try:
    from telegraph import Telegraph
    TELEGRAPH_OK = True
except ImportError:
    TELEGRAPH_OK = False

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sync")

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
MONGO_URL = os.getenv("MONGO_URL")
TELEGRAPH_TOKEN = os.getenv("TELEGRAPH_ACCESS_TOKEN")

# Channels from env var
CHANNELS_RAW = os.getenv("SYNC_CHANNELS", "")
CHANNELS = []
if CHANNELS_RAW:
    CHANNELS = [int(ch.strip()) for ch in CHANNELS_RAW.split(",") if ch.strip()]
else:
    log.error("SYNC_CHANNELS env var is empty! Set it to comma-separated channel IDs.")
    sys.exit(1)

BATCH_SIZE = 100
CALL_DELAY = 0.5
PAUSE_EVERY_N = 20
PAUSE_DURATION = 5
MAX_RETRIES = 10
THUMB_WIDTH = 400
THUMB_HEIGHT = 600
MAX_EMPTY_PAGES = 10

# Pillow resampling
if PIL_OK:
    try:
        resample = Image.Resampling.LANCZOS
    except AttributeError:
        resample = Image.LANCZOS
else:
    resample = None

# -------------------------------------------------------------------
# SETUP
# -------------------------------------------------------------------
# CHANGED: Replaced global initialization with a placeholder. 
# The client is now instantiated dynamically inside main() using the session string.
app = None 

mongo = AsyncIOMotorClient(MONGO_URL)
db = mongo.nexstream
videos_col = db.videos
sync_state_col = db.sync_state
sessions_col = db.sessions

# -------------------------------------------------------------------
# LOAD SESSION FROM MONGODB (or create a new one)
# -------------------------------------------------------------------
async def load_session_from_db():
    """Try to load an existing session string from MongoDB."""
    session_data = await sessions_col.find_one({"name": "main"})
    if session_data and session_data.get("string"):
        return session_data["string"]
    return None

telegraph = None
if TELEGRAPH_TOKEN and TELEGRAPH_OK:
    telegraph = Telegraph()
    telegraph.access_token = TELEGRAPH_TOKEN
    log.info("Telegraph ready")
else:
    log.warning("Telegraph not available, thumbnails will use file_id only")

# -------------------------------------------------------------------
# GENRE DETECTION (simple keyword matching)
# -------------------------------------------------------------------
GENRE_KEYWORDS = {
    "Action": ["action", "fight", "battle", "war", "mission"],
    "Comedy": ["comedy", "funny", "humor", "laugh"],
    "Thriller": ["thriller", "suspense", "mystery", "crime"],
    "Sci-Fi": ["sci-fi", "science fiction", "space", "alien", "futuristic"],
    "Horror": ["horror", "ghost", "supernatural", "terror"],
    "Drama": ["drama", "romance", "tragedy", "family"],
    "RPG": ["rpg", "role-playing", "fantasy", "quest"],
    "FPS": ["fps", "shooter", "first-person", "gun"],
    "Adventure": ["adventure", "explore", "journey", "treasure"],
}

def detect_genres(title: str, caption: str = "") -> list:
    text = f"{title} {caption}".lower()
    found = []
    for genre, keywords in GENRE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            found.append(genre)
    return found if found else []

# -------------------------------------------------------------------
# INDEXES
# -------------------------------------------------------------------
async def ensure_indexes():
    try:
        await videos_col.create_index("file_unique_id", unique=True, sparse=True)
        await videos_col.create_index([("channel_id", 1), ("message_id", 1)])
        await videos_col.create_index("type")
        await videos_col.create_index([("title", "text")])
        await sessions_col.create_index("name", unique=True)
        log.info("Indexes ensured")
    except Exception as e:
        log.error("Index creation error: %s", e)

# -------------------------------------------------------------------
# SAVE SESSION
# -------------------------------------------------------------------
async def save_session_to_db():
    try:
        session_string = await app.export_session_string()
        await sessions_col.update_one(
            {"name": "main"},
            {"$set": {"string": session_string, "updated_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        log.info("Session saved to MongoDB")
    except Exception as e:
        log.error("Failed to save session: %s", e)

# -------------------------------------------------------------------
# THUMBNAIL UPLOAD (does NOT write to DB – returns URL only)
# -------------------------------------------------------------------
async def download_and_upload_thumb(thumb_file_id, video_unique_id):
    """
    Upload a thumbnail to Telegraph and return the URL.
    Does NOT write to the database – that's done in bulk.
    """
    if not TELEGRAPH_OK or not PIL_OK or not thumb_file_id:
        return None

    # Check if we already have a URL stored (quick read‑only cache)
    existing = await videos_col.find_one(
        {"file_unique_id": video_unique_id}, {"thumb_url": 1}
    )
    if existing and existing.get("thumb_url", "").startswith("http"):
        return existing["thumb_url"]

    try:
        raw = await app.download_media(thumb_file_id, in_memory=True)
        if not raw:
            return None
        image_bytes = raw.read() if hasattr(raw, 'read') else bytes(raw)
        if len(image_bytes) < 100:
            return None

        img = Image.open(io.BytesIO(image_bytes))
        if img.mode != 'RGB':
            img = img.convert('RGB')

        w, h = img.size
        target_ratio = THUMB_WIDTH / THUMB_HEIGHT
        current_ratio = w / h

        if current_ratio > target_ratio:
            new_w = int(h * target_ratio)
            left = (w - new_w) // 2
            img = img.crop((left, 0, left + new_w, h))
        elif current_ratio < target_ratio:
            new_h = int(w / target_ratio)
            top = (h - new_h) // 2
            img = img.crop((0, top, w, top + new_h))

        img = img.resize((THUMB_WIDTH, THUMB_HEIGHT), resample)

        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=70)
        final_bytes = buf.getvalue()
        if len(final_bytes) < 100:
            return None

        result = telegraph.upload_file(io.BytesIO(final_bytes))
        url = "https://telegra.ph" + result[0]["src"]
        return url

    except errors.FloodWait as e:
        log.warning("FloodWait thumbnail: %ds", e.value)
        await asyncio.sleep(e.value + 1)
        return None
    except Exception as e:
        log.error("Thumb error: %s", e)
        return None

# -------------------------------------------------------------------
# FETCH PAGE (loop with retries)
# -------------------------------------------------------------------
async def fetch_page(channel_id, offset_id, limit):
    for attempt in range(MAX_RETRIES):
        try:
            messages = []
            async for msg in app.get_chat_history(channel_id, offset_id=offset_id, limit=limit):
                messages.append(msg)
            return messages
        except errors.FloodWait as e:
            wait = e.value + 1
            log.warning("FloodWait: %ds", wait)
            await asyncio.sleep(wait)
        except Exception as e:
            log.error("Error: %s, retry %d/%d", e, attempt+1, MAX_RETRIES)
            await asyncio.sleep(5)
    return []

# -------------------------------------------------------------------
# SYNC STATE HELPERS
# -------------------------------------------------------------------
async def get_last_synced_id(channel_id):
    state = await sync_state_col.find_one({"channel_id": channel_id})
    return state.get("last_message_id") if state else None

async def save_progress(channel_id, last_id, count):
    await sync_state_col.update_one(
        {"channel_id": channel_id},
        {"$set": {"last_message_id": last_id, "total_synced": count, "updated_at": datetime.now(timezone.utc)}},
        upsert=True
    )

# -------------------------------------------------------------------
# MAIN SYNC FUNCTION
# -------------------------------------------------------------------
async def sync_channel(channel_id):
    log.info("Syncing channel %d", channel_id)
    last_synced = await get_last_synced_id(channel_id)
    if last_synced:
        offset_id = last_synced
        log.info("Resuming from message_id < %d", last_synced)
    else:
        offset_id = 0
        log.info("Fresh sync from beginning")

    count = 0
    thumb_count = 0
    api_calls = 0
    empty_pages = 0
    start_time = time.time()
    db_batch = []

    while True:
        api_calls += 1
        messages = await fetch_page(channel_id, offset_id, BATCH_SIZE)

        if not messages:
            empty_pages += 1
            if empty_pages >= MAX_EMPTY_PAGES:
                log.info("End of channel history")
                break
            await asyncio.sleep(CALL_DELAY)
            continue

        empty_pages = 0
        page_vids = 0
        page_thumbs = 0

        for msg in messages:
            # ALWAYS update offset_id to move backwards through the channel
            if offset_id == 0 or msg.id < offset_id:
                offset_id = msg.id

            if not msg.video:
                continue

            vid = msg.video
            thumb_file_id = None
            if vid.thumbs:
                thumb_file_id = max(vid.thumbs, key=lambda t: t.width or 0).file_id

            title = (msg.text or msg.caption or vid.file_name or "Untitled")[:500]
            genres = detect_genres(title, msg.caption or "")

            doc = {
                "file_unique_id": vid.file_unique_id,
                "file_id": vid.file_id,
                "title": title,
                "desc": (msg.caption or msg.text or "")[:2000],
                "duration": vid.duration or 0,
                "width": vid.width or 0,
                "height": vid.height or 0,
                "file_size": vid.file_size or 0,
                "mime_type": vid.mime_type or "video/mp4",
                "thumb_file_id": thumb_file_id,
                "channel_id": channel_id,
                "message_id": msg.id,
                "views": msg.views or 0,
                "date": msg.date,
                "type": "movie",
                "genres": genres,
                "rating": 0,
                "year": msg.date.year if msg.date else datetime.now(timezone.utc).year,
                "synced_at": datetime.now(timezone.utc),
            }

            # Thumbnail upload (only if possible)
            thumb_url = None
            if thumb_file_id and TELEGRAPH_OK and PIL_OK:
                try:
                    thumb_url = await download_and_upload_thumb(thumb_file_id, vid.file_unique_id)
                    if thumb_url:
                        doc["thumb_url"] = thumb_url
                        page_thumbs += 1
                        thumb_count += 1
                except Exception as e:
                    log.error("Thumb upload failed: %s", e)
                await asyncio.sleep(0.3)

            db_batch.append(doc)
            count += 1
            page_vids += 1

        # Bulk write with error handling – only update progress on success
        if db_batch:
            try:
                operations = [
                    UpdateOne(
                        {"file_unique_id": d["file_unique_id"]},
                        {"$set": d},
                        upsert=True
                    )
                    for d in db_batch
                ]
                await videos_col.bulk_write(operations)
                await save_progress(channel_id, offset_id, count)
                db_batch = []
            except Exception as e:
                log.error("FAILED to write batch to DB! Data might be lost. Error: %s", e)
                raise

        elapsed = time.time() - start_time
        rate = count / elapsed if elapsed > 0 else 0
        log.info(
            "Call #%d: +%d vids, +%d thumbs | %d total, %d thumbs | %.1f/s",
            api_calls, page_vids, page_thumbs, count, thumb_count, rate
        )

        if api_calls % PAUSE_EVERY_N == 0:
            log.info("Pausing %ds...", PAUSE_DURATION)
            await asyncio.sleep(PAUSE_DURATION)
        else:
            await asyncio.sleep(CALL_DELAY)

    total_db = await videos_col.count_documents({"channel_id": channel_id})
    thumbs_db = await videos_col.count_documents({"channel_id": channel_id, "thumb_url": {"$regex": "^http"}})
    elapsed = time.time() - start_time
    log.info(
        "Channel %d done: %d new, %d total in DB, %d thumbs, %.1f min",
        channel_id, count, total_db, thumbs_db, elapsed / 60
    )

# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------
async def main():
    global app  # Explicitly bind the global app placeholder to this scope
    await ensure_indexes()

    # 1. Try environment variable first
    session_str = os.getenv("TELEGRAM_SESSION_STRING")

    # 2. If not set, try loading from MongoDB
    if not session_str:
        session_str = await load_session_from_db()
        if session_str:
            log.info("Loaded session from MongoDB")
        else:
            log.error("No session found in environment or database. "
                      "Run sync.py locally once to save a session.")
            sys.exit(1)
    else:
        log.info("Loaded session from GitHub Environment Variables")

    # 3. Dynamic setup of Pyrogram Client using the session string explicitly
    app = Client(
        "sync_session", 
        api_id=API_ID, 
        api_hash=API_HASH, 
        session_string=session_str
    )

    await app.start()
    await save_session_to_db()

    log.info("Checking channel access...")
    for ch in CHANNELS:
        try:
            chat = await app.get_chat(ch)
            log.info("  %s — %s members", chat.title, chat.members_count or "?")
        except Exception as e:
            log.error("  ERROR accessing %d: %s", ch, e)

    log.info("Syncing %d channel(s)...", len(CHANNELS))
    for ch in CHANNELS:
        try:
            await sync_channel(ch)
        except Exception as e:
            log.error("Failed to sync channel %d: %s", ch, e)
            continue

    total_all = await videos_col.estimated_document_count()
    thumbs_all = await videos_col.count_documents({"thumb_url": {"$regex": "^http"}})
    with_genres = await videos_col.count_documents({"genres.0": {"$exists": True}})

    log.info("ALL DONE!")
    log.info("Videos: %d", total_all)
    log.info("With thumbnails: %d", thumbs_all)
    log.info("With genres: %d", with_genres)

    await app.stop()
    mongo.close()
    log.info("MongoDB connection closed.")
    return
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
