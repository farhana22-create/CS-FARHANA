import os
import sqlite3
import logging
import threading
from datetime import datetime
from typing import Optional, List, Tuple

from flask import Flask, jsonify
from hydrogram import Client, filters
from hydrogram.types import Message

# =========================
# BOT CONFIG
# =========================
BOT_NAME = "CS FARHANA"
BOT_TAGLINE = "Customer Support Inbox Bot"

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
PORT = int(os.getenv("PORT", "8080"))

if not API_ID or not API_HASH or not BOT_TOKEN or not OWNER_ID:
    raise ValueError("Missing env vars: API_ID, API_HASH, BOT_TOKEN, OWNER_ID")

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("cs_farhana_bot")

# =========================
# FLASK APP
# =========================
web_app = Flask(__name__)

BOT_START_TIME = datetime.utcnow()

@web_app.route("/")
def home():
    return f"{BOT_NAME} is alive."

@web_app.route("/health")
def health():
    uptime_seconds = int((datetime.utcnow() - BOT_START_TIME).total_seconds())
    return jsonify({
        "status": "ok",
        "bot_name": BOT_NAME,
        "service": "telegram-support-bot",
        "uptime_seconds": uptime_seconds,
        "time": datetime.utcnow().isoformat() + "Z"
    })

def run_web():
    web_app.run(host="0.0.0.0", port=PORT)

# =========================
# TELEGRAM CLIENT
# =========================
bot = Client(
    "cs_farhana_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# =========================
# DATABASE
# =========================
DB_PATH = "cs_farhana.db"

def db_connect():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with db_connect() as conn:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT DEFAULT '',
                last_name TEXT DEFAULT '',
                username TEXT DEFAULT '',
                is_banned INTEGER DEFAULT 0,
                joined_at TEXT,
                updated_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS reply_map (
                owner_message_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.commit()

def add_or_update_user(user_id: int, first_name: str = "", last_name: str = "", username: str = ""):
    now = datetime.utcnow().isoformat()
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (user_id, first_name, last_name, username, joined_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                username = excluded.username,
                updated_at = excluded.updated_at
        """, (user_id, first_name or "", last_name or "", username or "", now, now))
        conn.commit()

def get_user(user_id: int) -> Optional[Tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, first_name, last_name, username, is_banned, joined_at, updated_at
            FROM users
            WHERE user_id = ?
        """, (user_id,))
        return cur.fetchone()

def get_total_users() -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        row = cur.fetchone()
        return row[0] if row else 0

def get_banned_count() -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
        row = cur.fetchone()
        return row[0] if row else 0

def get_all_active_user_ids() -> List[int]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id FROM users
            WHERE is_banned = 0
            ORDER BY joined_at ASC
        """)
        return [row[0] for row in cur.fetchall()]

def get_recent_users(limit: int = 10) -> List[Tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, first_name, last_name, username, is_banned
            FROM users
            ORDER BY updated_at DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()

def user_exists(user_id: int) -> bool:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE user_id = ? LIMIT 1", (user_id,))
        return cur.fetchone() is not None

def is_banned(user_id: int) -> bool:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT is_banned FROM users WHERE user_id = ? LIMIT 1", (user_id,))
        row = cur.fetchone()
        return bool(row[0]) if row else False

def set_ban_status(user_id: int, status: bool):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE users
            SET is_banned = ?, updated_at = ?
            WHERE user_id = ?
        """, (1 if status else 0, datetime.utcnow().isoformat(), user_id))
        conn.commit()

def save_reply_mapping(owner_message_id: int, user_id: int):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO reply_map (owner_message_id, user_id, created_at)
            VALUES (?, ?, ?)
        """, (owner_message_id, user_id, datetime.utcnow().isoformat()))
        conn.commit()

def get_target_user_id(owner_message_id: int) -> Optional[int]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM reply_map WHERE owner_message_id = ?", (owner_message_id,))
        row = cur.fetchone()
        return row[0] if row else None

def cleanup_old_reply_map(limit_keep: int = 5000):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM reply_map")
        total = cur.fetchone()[0]
        if total > limit_keep:
            to_delete = total - limit_keep
            cur.execute("""
                DELETE FROM reply_map
                WHERE owner_message_id IN (
                    SELECT owner_message_id
                    FROM reply_map
                    ORDER BY created_at ASC
                    LIMIT ?
                )
            """, (to_delete,))
            conn.commit()

# =========================
# HELPERS
# =========================
OWNER_COMMANDS = [
    "/start", "/help", "/users", "/stats",
    "/msg", "/broadcast", "/ban", "/unban", "/id"
]

def is_owner_command(message: Message) -> bool:
    text = (message.text or message.caption or "").strip()
    return any(text.startswith(cmd) for cmd in OWNER_COMMANDS)

def format_name(first_name: str, last_name: str) -> str:
    return f"{first_name or ''} {last_name or ''}".strip() or "Unknown"

def build_user_info(message: Message) -> str:
    user = message.from_user
    if not user:
        return "New message received\n\nUser info unavailable."

    full_name = format_name(user.first_name, user.last_name)
    username = f"@{user.username}" if user.username else "No username"

    return (
        f"New message in {BOT_NAME}\n\n"
        f"Name: {full_name}\n"
        f"Username: {username}\n"
        f"User ID: {user.id}\n"
        f"Chat ID: {message.chat.id}"
    )

async def safe_reply(message: Message, text: str):
    try:
        await message.reply_text(text)
    except Exception as e:
        logger.warning(f"Reply failed: {e}")

def parse_user_id_from_text(text: str) -> Optional[int]:
    parts = (text or "").split(maxsplit=2)
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None

# =========================
# OWNER COMMANDS
# =========================
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    user = message.from_user
    if user:
        add_or_update_user(user.id, user.first_name, user.last_name, user.username)

    if user and user.id == OWNER_ID:
        await message.reply_text(
            f"{BOT_NAME} owner panel ready.\n\n"
            "Commands:\n"
            "/users - show recent users\n"
            "/stats - show bot stats\n"
            "/msg user_id text - send direct text\n"
            "Reply + /msg user_id - send replied content\n"
            "/broadcast text - send to all active users\n"
            "Reply + /broadcast - broadcast replied content\n"
            "/ban user_id - ban a user\n"
            "/unban user_id - unban a user\n"
            "/id - get replied user's id\n\n"
            "Also reply to copied user messages to answer them."
        )
    else:
        if user and is_banned(user.id):
            await message.reply_text("You are blocked from using this bot.")
            return

        await message.reply_text(
            f"Welcome to {BOT_NAME}.\n\n"
            "Send your message here. It will be delivered to support."
        )

@bot.on_message(filters.command("help") & filters.private)
async def help_handler(client: Client, message: Message):
    user = message.from_user
    if user and user.id == OWNER_ID:
        await message.reply_text(
            "Owner help:\n\n"
            "/users\n"
            "/stats\n"
            "/msg user_id your text\n"
            "reply + /msg user_id\n"
            "/broadcast your text\n"
            "reply + /broadcast\n"
            "/ban user_id\n"
            "/unban user_id\n"
            "/id\n\n"
            "Reply directly to a copied user message to answer that user."
        )
    else:
        if user and is_banned(user.id):
            await message.reply_text("You are blocked from using this bot.")
            return

        await message.reply_text(
            f"{BOT_NAME}\n\n"
            "Send your message here. Our team will receive it."
        )

@bot.on_message(filters.command("stats") & filters.private & filters.user(OWNER_ID))
async def stats_handler(client: Client, message: Message):
    total_users = get_total_users()
    banned_users = get_banned_count()
    active_users = max(total_users - banned_users, 0)
    uptime_seconds = int((datetime.utcnow() - BOT_START_TIME).total_seconds())

    await message.reply_text(
        f"{BOT_NAME} Stats\n\n"
        f"Total users: {total_users}\n"
        f"Active users: {active_users}\n"
        f"Banned users: {banned_users}\n"
        f"Uptime: {uptime_seconds} seconds"
    )

@bot.on_message(filters.command("users") & filters.private & filters.user(OWNER_ID))
async def users_handler(client: Client, message: Message):
    users = get_recent_users(15)
    total = get_total_users()

    if not users:
        await message.reply_text("No users found.")
        return

    lines = [f"{BOT_NAME} Users", f"Total users: {total}", ""]
    for idx, row in enumerate(users, start=1):
        user_id, first_name, last_name, username, banned = row
        full_name = format_name(first_name, last_name)
        uname = f"@{username}" if username else "No username"
        status = "BANNED" if banned else "ACTIVE"
        lines.append(f"{idx}. {full_name}")
        lines.append(f"   ID: {user_id}")
        lines.append(f"   Username: {uname}")
        lines.append(f"   Status: {status}")
        lines.append("")

    await message.reply_text("\n".join(lines).strip())

@bot.on_message(filters.command("ban") & filters.private & filters.user(OWNER_ID))
async def ban_handler(client: Client, message: Message):
    user_id = parse_user_id_from_text(message.text or "")
    if not user_id:
        await message.reply_text("Usage:\n/ban user_id")
        return

    if not user_exists(user_id):
        await message.reply_text("User not found in database.")
        return

    set_ban_status(user_id, True)
    await message.reply_text(f"User {user_id} banned.")

@bot.on_message(filters.command("unban") & filters.private & filters.user(OWNER_ID))
async def unban_handler(client: Client, message: Message):
    user_id = parse_user_id_from_text(message.text or "")
    if not user_id:
        await message.reply_text("Usage:\n/unban user_id")
        return

    if not user_exists(user_id):
        await message.reply_text("User not found in database.")
        return

    set_ban_status(user_id, False)
    await message.reply_text(f"User {user_id} unbanned.")

@bot.on_message(filters.command("id") & filters.private & filters.user(OWNER_ID))
async def id_handler(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text("Reply to a copied user message first.")
        return

    target_user_id = get_target_user_id(message.reply_to_message.id)
    if not target_user_id:
        await message.reply_text("User ID not found for this message.")
        return

    await message.reply_text(f"User ID: {target_user_id}")

@bot.on_message(filters.command("msg") & filters.private & filters.user(OWNER_ID))
async def msg_handler(client: Client, message: Message):
    try:
        parts = (message.text or "").split(maxsplit=2)

        if len(parts) < 2:
            await message.reply_text(
                "Usage:\n"
                "/msg user_id your message\n\n"
                "Or reply to any message and use:\n"
                "/msg user_id"
            )
            return

        try:
            target_user_id = int(parts[1])
        except ValueError:
            await message.reply_text("Invalid user_id.")
            return

        if not user_exists(target_user_id):
            await message.reply_text("This user is not in database yet.")
            return

        if is_banned(target_user_id):
            await message.reply_text("This user is banned.")
            return

        if len(parts) >= 3 and parts[2].strip():
            await client.send_message(chat_id=target_user_id, text=parts[2].strip())
            await message.reply_text("Direct message sent.")
            return

        if message.reply_to_message:
            await client.copy_message(
                chat_id=target_user_id,
                from_chat_id=message.chat.id,
                message_id=message.reply_to_message.id
            )
            await message.reply_text("Replied content sent.")
            return

        await message.reply_text("Nothing to send.")

    except Exception as e:
        logger.exception(f"/msg failed: {e}")
        await message.reply_text("Failed to send direct message.")

@bot.on_message(filters.command("broadcast") & filters.private & filters.user(OWNER_ID))
async def broadcast_handler(client: Client, message: Message):
    try:
        user_ids = get_all_active_user_ids()
        if not user_ids:
            await message.reply_text("No active users found.")
            return

        text_payload = None
        replied_payload = None

        parts = (message.text or "").split(maxsplit=1)
        if len(parts) >= 2 and parts[1].strip():
            text_payload = parts[1].strip()
        elif message.reply_to_message:
            replied_payload = message.reply_to_message
        else:
            await message.reply_text(
                "Usage:\n"
                "/broadcast your text\n\n"
                "Or reply to a message and use:\n"
                "/broadcast"
            )
            return

        status_msg = await message.reply_text("Broadcast started...")

        success = 0
        failed = 0

        for user_id in user_ids:
            try:
                if text_payload:
                    await client.send_message(chat_id=user_id, text=text_payload)
                else:
                    await client.copy_message(
                        chat_id=user_id,
                        from_chat_id=message.chat.id,
                        message_id=replied_payload.id
                    )
                success += 1
            except Exception as e:
                failed += 1
                logger.warning(f"Broadcast failed for {user_id}: {e}")

        await status_msg.edit_text(
            f"Broadcast completed.\n\n"
            f"Success: {success}\n"
            f"Failed: {failed}"
        )

    except Exception as e:
        logger.exception(f"/broadcast failed: {e}")
        await message.reply_text("Broadcast failed.")

# =========================
# USER -> OWNER RELAY
# =========================
@bot.on_message(filters.private & ~filters.user(OWNER_ID) & ~filters.command(["start", "help"]))
async def relay_user_to_owner(client: Client, message: Message):
    try:
        user = message.from_user
        if user:
            add_or_update_user(user.id, user.first_name, user.last_name, user.username)

            if is_banned(user.id):
                await safe_reply(message, "You are blocked from using this bot.")
                return

        await client.send_message(
            chat_id=OWNER_ID,
            text=build_user_info(message)
        )

        copied = await client.copy_message(
            chat_id=OWNER_ID,
            from_chat_id=message.chat.id,
            message_id=message.id
        )

        save_reply_mapping(copied.id, message.chat.id)
        cleanup_old_reply_map()

        await safe_reply(message, f"Your message has been sent to {BOT_NAME}.")

    except Exception as e:
        logger.exception(f"Relay failed: {e}")
        await safe_reply(message, "Failed to send your message. Please try again later.")

# =========================
# OWNER -> USER REPLY
# =========================
@bot.on_message(filters.private & filters.user(OWNER_ID) & filters.reply)
async def owner_reply_handler(client: Client, message: Message):
    try:
        if is_owner_command(message):
            return

        replied = message.reply_to_message
        if not replied:
            return

        target_user_id = get_target_user_id(replied.id)
        if not target_user_id:
            return

        if is_banned(target_user_id):
            await message.reply_text("This user is banned.")
            return

        await client.copy_message(
            chat_id=target_user_id,
            from_chat_id=message.chat.id,
            message_id=message.id
        )

        await safe_reply(message, "Reply sent.")

    except Exception as e:
        logger.exception(f"Owner reply failed: {e}")
        await safe_reply(message, "Failed to send reply.")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    init_db()

    web_thread = threading.Thread(target=run_web, daemon=True)
    web_thread.start()

    logger.info(f"Starting {BOT_NAME}...")
    bot.run()