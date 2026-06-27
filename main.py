# -*- coding: utf-8 -*-
"""
Telegram Bot - Optimized untuk 100-500 Concurrent Users
POLLING MODE + OPTIMIZATIONS (FIXED)
✅ Parallel message processing
✅ Better rate limiting
✅ Scalable & reliable
"""

import logging
import logging.handlers
import os
import asyncio
import sqlite3
import random
import string
import shutil
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional, List, Tuple

from fastapi import FastAPI
from uvicorn import Config, Server
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import RetryAfter, Forbidden, BadRequest
from telegram.ext import Application, CommandHandler, MessageHandler, filters

from config import (
    TOKEN, CHANNEL, CHANNEL_ID, BOT_USERNAME, ADMIN_IDS,
    HOST, PORT, DATABASE_PATH,
    BACKUP_CHAT_ID, AUTO_DELETE_TIMEOUT, BATCH_TIMEOUT,
    BACKUP_INTERVAL, BACKUP_DIR, MAX_BACKUPS,
    REPLIT_DOMAIN
)

# ===== LOGGING =====
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            "logs/bot.log",
            maxBytes=5*1024*1024,
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== DATABASE CONNECTION POOL =====

class DatabasePool:
    """Optimized connection pool"""
    def __init__(self, db_path, pool_size=5):
        self.db_path = db_path
        self.pool_size = pool_size
        self.connections = asyncio.Queue(maxsize=pool_size)
        self.write_lock = asyncio.Lock()
        self.read_semaphore = asyncio.Semaphore(pool_size * 2)

    async def init(self):
        """Initialize pool"""
        for _ in range(self.pool_size):
            conn = sqlite3.connect(self.db_path, timeout=20, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")
            conn.execute("PRAGMA temp_store=MEMORY")
            await self.connections.put(conn)
        logger.info(f"✅ Database pool initialized with {self.pool_size} connections")

    async def get(self):
        return await self.connections.get()

    async def put(self, conn):
        await self.connections.put(conn)

    async def execute_write(self, query, params=()):
        async with self.write_lock:
            conn = await self.get()
            try:
                c = conn.cursor()
                c.execute(query, params)
                conn.commit()
                return c.lastrowid
            except Exception as e:
                conn.rollback()
                logger.error(f"DB Write Error: {e}")
                raise
            finally:
                await self.put(conn)

    async def execute_read(self, query, params=(), fetch_one=False):
        async with self.read_semaphore:
            conn = await self.get()
            try:
                c = conn.cursor()
                c.execute(query, params)
                if fetch_one:
                    return c.fetchone()
                return c.fetchall()
            except Exception as e:
                logger.error(f"DB Read Error: {e}")
                return None if fetch_one else []
            finally:
                await self.put(conn)

    async def close(self):
        while not self.connections.empty():
            try:
                conn = self.connections.get_nowait()
                conn.close()
            except:
                pass

db_pool: Optional[DatabasePool] = None

# ===== INIT DATABASE =====

async def init_db():
    """Initialize database"""
    conn = await db_pool.get()
    try:
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS media(
                code TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                type TEXT NOT NULL,
                caption TEXT DEFAULT '',
                click INTEGER DEFAULT 0,
                ready INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Migrate old users table: add missing columns if not exist
        existing_cols = [row[1] for row in c.execute("PRAGMA table_info(users)").fetchall()]
        if "username" not in existing_cols:
            c.execute("ALTER TABLE users ADD COLUMN username TEXT")
            logger.info("✅ Migrated users table: added username column")
        if "joined_at" not in existing_cols:
            c.execute("ALTER TABLE users ADD COLUMN joined_at TIMESTAMP")
            logger.info("✅ Migrated users table: added joined_at column")

        c.execute("""
            CREATE TABLE IF NOT EXISTS backup_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_name TEXT,
                backup_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_size INTEGER
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_broadcasts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                schedule_time TEXT NOT NULL,
                msg_type TEXT NOT NULL,
                file_id TEXT,
                text_content TEXT,
                caption TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS link_access_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Migrate media table: tambah created_at jika belum ada (DB lama)
        media_cols = [row[1] for row in c.execute("PRAGMA table_info(media)").fetchall()]
        if "created_at" not in media_cols:
            c.execute("ALTER TABLE media ADD COLUMN created_at TIMESTAMP")
            logger.info("✅ Migrated media table: added created_at column")

        c.execute("CREATE INDEX IF NOT EXISTS idx_media_code ON media(code)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_media_ready ON media(ready)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_id ON users(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_link_log_code ON link_access_log(code)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_link_log_user ON link_access_log(user_id)")

        conn.commit()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"DB Init Error: {e}")
        conn.rollback()
    finally:
        await db_pool.put(conn)

# ===== HELPER FUNCTIONS =====

def gen_code(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

async def save_user(uid, username=None):
    try:
        await db_pool.execute_write(
            "INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
            (uid, username)
        )
    except Exception as e:
        logger.error(f"Error saving user: {e}")

async def save_media(code, file_id, media_type, caption=""):
    try:
        await db_pool.execute_write(
            "INSERT OR REPLACE INTO media (code, file_id, type, caption, click, ready) VALUES (?, ?, ?, ?, 0, 0)",
            (code, file_id, media_type, caption)
        )
    except Exception as e:
        logger.error(f"Error saving media: {e}")

async def set_ready(code):
    try:
        await db_pool.execute_write(
            "UPDATE media SET ready=1 WHERE code=?",
            (code,)
        )
        logger.info(f"✅ Code {code} marked ready")
    except Exception as e:
        logger.error(f"Error setting ready: {e}")

def is_admin(uid):
    return uid in ADMIN_IDS

async def check_joined(application, uid):
    try:
        member = await application.bot.get_chat_member(CHANNEL, uid)
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.warning(f"Check joined error: {e}")
        return False

async def get_total_users():
    result = await db_pool.execute_read("SELECT COUNT(*) FROM users", fetch_one=True)
    return result[0] if result else 0

async def get_total_media():
    result = await db_pool.execute_read("SELECT COUNT(*) FROM media", fetch_one=True)
    return result[0] if result else 0

async def get_total_clicks():
    result = await db_pool.execute_read("SELECT SUM(click) FROM media", fetch_one=True)
    return result[0] if result else 0

async def get_last_backup():
    return await db_pool.execute_read(
        "SELECT backup_name, file_size, backup_time FROM backup_log ORDER BY backup_time DESC LIMIT 1",
        fetch_one=True
    )

async def get_media_by_code(code) -> List[Tuple]:
    return await db_pool.execute_read(
        "SELECT file_id, type, caption FROM media WHERE code=? ORDER BY rowid",
        (code,)
    )

async def increment_clicks(code):
    try:
        await db_pool.execute_write(
            "UPDATE media SET click=click+1 WHERE code=?",
            (code,)
        )
    except Exception as e:
        logger.error(f"Error incrementing clicks: {e}")

async def log_link_access(code: str, user_id: int, username: str):
    """Catat akses link ke log per-user"""
    try:
        await db_pool.execute_write(
            "INSERT INTO link_access_log (code, user_id, username) VALUES (?,?,?)",
            (code, user_id, username)
        )
    except Exception as e:
        logger.debug(f"log_link_access error: {e}")

# ===== BATCH & ALBUM MANAGEMENT =====

batch_buffer = {}
batch_timers = {}
album_cache = {}
ALBUM_TTL = 300

async def cleanup_expired_caches():
    now = datetime.now()
    expired = [gid for gid, data in album_cache.items() if now > data["expire_at"]]
    for gid in expired:
        album_cache.pop(gid, None)
    if expired:
        logger.info(f"🗑️ Cleaned {len(expired)} expired albums")

async def finalize_batch(application, uid):
    if uid not in batch_buffer:
        return

    code = batch_buffer[uid]["code"]

    try:
        await set_ready(code)
        link = f"https://t.me/{BOT_USERNAME}?start={code}"

        await application.bot.send_message(
            uid,
            f"✅ LINK MEDIA READY\n\n🔗 {link}\n\n📌 Bagikan link ini ke user",
            parse_mode="HTML"
        )
        logger.info(f"✅ Link sent to {uid}: {code}")
    except Exception as e:
        logger.error(f"Error sending link to {uid}: {e}")
    finally:
        batch_buffer.pop(uid, None)
        if uid in batch_timers:
            batch_timers[uid].cancel()

# ===== HANDLERS =====

async def start_command(update, context):
    """Handle /start command"""
    uid = update.effective_user.id
    username = update.effective_user.username or "unknown"

    context.application.create_task(save_user(uid, username))
    logger.info(f"👤 User {username} ({uid}) started bot")

    if not context.args:
        await update.message.reply_text(
            "👋 Halo! Saya bot media sharing.\n\n"
            "Untuk membuka media, gunakan link yang diberikan admin.\n\n"
            f"📢 Join channel: {CHANNEL}"
        )
        return

    code = context.args[0]
    logger.info(f"🔗 User {username} accessing code: {code}")

    joined = await check_joined(context.application, uid)
    logger.info(f"🔍 check_joined for {username} ({uid}): {joined}")
    if not joined:
        btn = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 JOIN CHANNEL", url=f"https://t.me/{CHANNEL[1:]}")],
            [InlineKeyboardButton("🔄 Coba Lagi", url=f"https://t.me/{BOT_USERNAME}?start={code}")]
        ])
        await update.message.reply_text(
            f"⚠️ Wajib join channel {CHANNEL} untuk akses media!",
            reply_markup=btn
        )
        return

    ready_result, media_list = await asyncio.gather(
        db_pool.execute_read("SELECT ready FROM media WHERE code=? LIMIT 1", (code,), fetch_one=True),
        get_media_by_code(code),
        return_exceptions=True
    )

    if not ready_result or ready_result[0] == 0:
        await update.message.reply_text("⏳ Sedang menyiapkan media, coba lagi sebentar...")
        return

    if not media_list:
        await update.message.reply_text("❌ Link tidak valid atau sudah kadaluarsa")
        return

    context.application.create_task(increment_clicks(code))
    context.application.create_task(log_link_access(code, uid, username))

    sent_ids = []
    send_tasks = []

    for file_id, media_type, caption in media_list:
        task = send_media_item(context.bot, uid, file_id, media_type, caption, sent_ids)
        send_tasks.append(task)

    await asyncio.gather(*send_tasks, return_exceptions=True)

    if sent_ids:
        try:
            msg = await update.message.reply_text(
                f"⌛ {len(sent_ids)} media akan terhapus otomatis dalam 1 jam."
            )
            sent_ids.append(msg.message_id)
        except:
            pass

        async def delete_later():
            try:
                await asyncio.sleep(AUTO_DELETE_TIMEOUT)
                for mid in sent_ids:
                    try:
                        await context.bot.delete_message(uid, mid)
                    except:
                        pass
            except:
                pass

        context.application.create_task(delete_later())

async def send_media_item(bot, uid, file_id, media_type, caption, sent_ids):
    """Send single media item (can run in parallel)"""
    try:
        if media_type == "photo":
            msg = await bot.send_photo(uid, file_id, caption=caption or "")
        elif media_type == "video":
            msg = await bot.send_video(uid, file_id, caption=caption or "")
        else:
            return

        sent_ids.append(msg.message_id)
        logger.info(f"✅ Sent {media_type} to {uid}")
    except Exception as e:
        logger.error(f"Error sending {media_type} to {uid}: {e}")

async def upload_handler(update, context):
    """Handle photo/video upload"""
    uid = update.effective_user.id

    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    msg = update.message
    file_id = None
    media_type = None

    if msg.photo:
        file_id = msg.photo[-1].file_id
        media_type = "photo"
        await update.message.reply_text("✅ Foto diterima")
    elif msg.video:
        file_id = msg.video.file_id
        media_type = "video"
        await update.message.reply_text("✅ Video diterima")
    else:
        return

    caption = msg.caption or ""
    gid = msg.media_group_id

    if gid:
        if gid not in album_cache:
            album_cache[gid] = {
                "code": gen_code(),
                "expire_at": datetime.now() + timedelta(seconds=ALBUM_TTL)
            }

        code = album_cache[gid]["code"]
        context.application.create_task(save_media(code, file_id, media_type, caption))

        async def send_album_link():
            try:
                await asyncio.sleep(3)
                await set_ready(code)
                link = f"https://t.me/{BOT_USERNAME}?start={code}"
                media_count = len(await get_media_by_code(code))
                await context.bot.send_message(
                    uid,
                    f"✅ LINK ALBUM READY\n\n🔗 {link}\n\n📌 Total media: {media_count}"
                )
                logger.info(f"✅ Album link sent: {code}")
            except Exception as e:
                logger.error(f"Error sending album link: {e}")
            finally:
                album_cache.pop(gid, None)

        context.application.create_task(send_album_link())
        return

    if uid not in batch_buffer:
        batch_buffer[uid] = {"code": gen_code()}

    code = batch_buffer[uid]["code"]
    context.application.create_task(save_media(code, file_id, media_type, caption))

    if uid in batch_timers:
        batch_timers[uid].cancel()

    async def finalize():
        try:
            await asyncio.sleep(BATCH_TIMEOUT)
            await finalize_batch(context.application, uid)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Finalize error: {e}")

    task = context.application.create_task(finalize())
    batch_timers[uid] = task

# Track active broadcasts per admin — allows /bc_cancel
_active_broadcasts: dict = {}

async def broadcast_command(update, context):
    """Handle /bc — broadcast any message type (text/photo/video/dll) ke semua user"""
    uid = update.effective_user.id

    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    if _active_broadcasts.get(uid):
        await update.message.reply_text(
            "⚠️ Broadcast sedang berjalan.\n"
            "Kirim /bc_cancel untuk membatalkan."
        )
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "❌ Reply ke pesan yang ingin di-broadcast.\n\n"
            "📌 Mendukung semua jenis: teks, foto, video, dokumen, dll."
        )
        return

    msg_to_send = update.message.reply_to_message
    users = await db_pool.execute_read("SELECT user_id FROM users")
    total = len(users)

    if total == 0:
        await update.message.reply_text("ℹ️ Tidak ada user.")
        return

    status_msg = await update.message.reply_text(
        f"📤 <b>Broadcast dimulai...</b>\n"
        f"👥 Target: <b>{total:,}</b> users",
        parse_mode="HTML"
    )

    _active_broadcasts[uid] = True
    sent = 0
    failed_blocked = 0
    failed_other = 0
    loop_start = asyncio.get_event_loop().time()
    last_update_time = loop_start

    try:
        for i, row in enumerate(users):
            user_id = row[0]

            # Cek apakah admin membatalkan
            if not _active_broadcasts.get(uid):
                break

            try:
                await asyncio.wait_for(
                    msg_to_send.copy(user_id),
                    timeout=10.0
                )
                sent += 1
                # Rate limit aman: ~15 pesan/detik
                await asyncio.sleep(0.065)

            except asyncio.TimeoutError:
                failed_other += 1
                logger.debug(f"Broadcast timeout uid={user_id}")

            except RetryAfter as e:
                # Telegram FloodWait — tunggu sesuai instruksi server
                wait_sec = int(e.retry_after) + 2
                logger.warning(f"FloodWait {wait_sec}s saat broadcast")
                await asyncio.sleep(wait_sec)
                # Retry sekali setelah jeda
                try:
                    await asyncio.wait_for(msg_to_send.copy(user_id), timeout=10.0)
                    sent += 1
                except Exception:
                    failed_other += 1

            except (Forbidden, BadRequest) as e:
                # User memblokir bot atau akun tidak aktif
                err = str(e).lower()
                if any(x in err for x in ["blocked", "deactivated", "not found", "chat not found", "kicked"]):
                    failed_blocked += 1
                else:
                    failed_other += 1
                logger.debug(f"Broadcast skip uid={user_id}: {e}")

            except Exception as e:
                failed_other += 1
                logger.debug(f"Broadcast error uid={user_id}: {e}")

            # Update progress setiap 50 user atau setiap 10 detik
            now = asyncio.get_event_loop().time()
            if (i + 1) % 50 == 0 or (now - last_update_time) >= 10:
                last_update_time = now
                pct = ((i + 1) / total) * 100
                bar_filled = int(pct / 5)
                bar = "█" * bar_filled + "░" * (20 - bar_filled)
                try:
                    await status_msg.edit_text(
                        f"📤 <b>Broadcasting...</b>\n"
                        f"<code>[{bar}]</code> {pct:.1f}%\n"
                        f"{'━' * 28}\n"
                        f"📊 Proses: {i + 1:,} / {total:,}\n"
                        f"✅ Terkirim: <b>{sent:,}</b>\n"
                        f"🚫 Diblokir: <b>{failed_blocked:,}</b>\n"
                        f"❌ Error lain: <b>{failed_other:,}</b>",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

    finally:
        cancelled = not _active_broadcasts.pop(uid, True)
        elapsed = asyncio.get_event_loop().time() - loop_start
        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"

        title = "🚫 <b>Broadcast Dibatalkan</b>" if cancelled else "✅ <b>Broadcast Selesai</b>"
        summary = (
            f"{title}\n"
            f"{'━' * 28}\n"
            f"👥 Total Target: <b>{total:,}</b>\n"
            f"✅ Terkirim: <b>{sent:,}</b>\n"
            f"🚫 Diblokir bot: <b>{failed_blocked:,}</b>\n"
            f"❌ Error lain: <b>{failed_other:,}</b>\n"
            f"⏱️ Durasi: <b>{elapsed_str}</b>"
        )
        try:
            await status_msg.edit_text(summary, parse_mode="HTML")
        except Exception:
            try:
                await update.message.reply_text(summary, parse_mode="HTML")
            except Exception:
                pass

async def bc_cancel_command(update, context):
    """Handle /bc_cancel — batalkan broadcast yang sedang berjalan"""
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    if uid in _active_broadcasts:
        _active_broadcasts[uid] = False
        await update.message.reply_text("🛑 Membatalkan broadcast, mohon tunggu...")
    else:
        await update.message.reply_text("ℹ️ Tidak ada broadcast aktif saat ini.")

# ===== SCHEDULED BROADCAST =====

async def _save_schedule(admin_id, schedule_time, msg_type, file_id=None, text_content=None, caption=None):
    await db_pool.execute_write(
        "INSERT INTO scheduled_broadcasts (admin_id, schedule_time, msg_type, file_id, text_content, caption) VALUES (?,?,?,?,?,?)",
        (admin_id, schedule_time, msg_type, file_id, text_content, caption)
    )

async def _get_all_schedules():
    return await db_pool.execute_read(
        "SELECT id, admin_id, schedule_time, msg_type, file_id, text_content, caption FROM scheduled_broadcasts ORDER BY schedule_time"
    )

async def _delete_schedule(schedule_id: int):
    await db_pool.execute_write(
        "DELETE FROM scheduled_broadcasts WHERE id=?",
        (schedule_id,)
    )

async def bc_schedule_command(update, context):
    """Handle /bc_schedule HH:MM — jadwalkan broadcast harian"""
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Sertakan waktu pengiriman.\n\n"
            "📌 Cara pakai:\n"
            "1. Kirim/siapkan pesan yang ingin dijadwalkan\n"
            "2. Reply pesan itu dengan:\n"
            "   <code>/bc_schedule HH:MM</code>\n\n"
            "Contoh: <code>/bc_schedule 08:00</code>\n"
            "Mendukung: teks, foto, video, dokumen, animasi",
            parse_mode="HTML"
        )
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "❌ Reply ke pesan yang ingin dijadwalkan.\n\n"
            "Contoh: <code>/bc_schedule 08:00</code>",
            parse_mode="HTML"
        )
        return

    # Parse dan validasi waktu
    time_str = context.args[0].strip()
    try:
        hour, minute = map(int, time_str.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Out of range")
        schedule_time = f"{hour:02d}:{minute:02d}"
    except Exception:
        await update.message.reply_text(
            "❌ Format waktu salah. Gunakan format 24 jam.\n"
            "Contoh: <code>/bc_schedule 08:00</code>",
            parse_mode="HTML"
        )
        return

    # Ekstrak isi pesan
    msg = update.message.reply_to_message
    msg_type = file_id = text_content = caption = None

    if msg.text:
        msg_type, text_content = "text", msg.text
    elif msg.photo:
        msg_type = "photo"
        file_id = msg.photo[-1].file_id
        caption = msg.caption or ""
    elif msg.video:
        msg_type = "video"
        file_id = msg.video.file_id
        caption = msg.caption or ""
    elif msg.document:
        msg_type = "document"
        file_id = msg.document.file_id
        caption = msg.caption or ""
    elif msg.animation:
        msg_type = "animation"
        file_id = msg.animation.file_id
        caption = msg.caption or ""
    else:
        await update.message.reply_text(
            "❌ Jenis pesan tidak didukung.\n"
            "Mendukung: teks, foto, video, dokumen, animasi/GIF."
        )
        return

    await _save_schedule(uid, schedule_time, msg_type, file_id, text_content, caption)
    logger.info(f"📅 Admin {uid} scheduled {msg_type} broadcast at {schedule_time}")

    await update.message.reply_text(
        f"✅ <b>Broadcast Dijadwalkan!</b>\n"
        f"{'━' * 28}\n"
        f"🕐 Waktu: <b>{schedule_time}</b> (setiap hari)\n"
        f"📩 Jenis pesan: <b>{msg_type}</b>\n\n"
        f"Lihat semua jadwal: /bc_schedules\n"
        f"Hapus jadwal: /bc_unschedule [id]",
        parse_mode="HTML"
    )

async def bc_schedules_command(update, context):
    """Handle /bc_schedules — tampilkan semua jadwal aktif"""
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    rows = await _get_all_schedules()
    if not rows:
        await update.message.reply_text(
            "ℹ️ Tidak ada jadwal broadcast aktif.\n\n"
            "Buat jadwal baru:\n<code>/bc_schedule HH:MM</code>",
            parse_mode="HTML"
        )
        return

    lines = [f"📅 <b>Jadwal Broadcast Aktif ({len(rows)})</b>\n" + "━" * 28]
    for row in rows:
        sched_id, admin_id, sched_time, msg_type, *_ = row
        lines.append(f"• ID <code>{sched_id}</code>  ⏰ <b>{sched_time}</b>  📩 {msg_type}")
    lines.append(f"\n🗑️ Hapus: /bc_unschedule [id]")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def bc_unschedule_command(update, context):
    """Handle /bc_unschedule [id] — hapus jadwal broadcast"""
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Sertakan ID jadwal.\n"
            "Contoh: <code>/bc_unschedule 1</code>\n\n"
            "Lihat daftar ID: /bc_schedules",
            parse_mode="HTML"
        )
        return

    try:
        sched_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID harus berupa angka.")
        return

    await _delete_schedule(sched_id)
    await update.message.reply_text(
        f"✅ Jadwal ID <code>{sched_id}</code> berhasil dihapus.",
        parse_mode="HTML"
    )
    logger.info(f"📅 Admin {uid} deleted schedule ID={sched_id}")

async def _send_one_scheduled(bot, user_id, msg_type, file_id, text_content, caption):
    """Kirim satu pesan terjadwal ke satu user"""
    cap = caption or None
    if msg_type == "text":
        await bot.send_message(user_id, text_content)
    elif msg_type == "photo":
        await bot.send_photo(user_id, file_id, caption=cap)
    elif msg_type == "video":
        await bot.send_video(user_id, file_id, caption=cap)
    elif msg_type == "document":
        await bot.send_document(user_id, file_id, caption=cap)
    elif msg_type == "animation":
        await bot.send_animation(user_id, file_id, caption=cap)

async def _execute_scheduled_broadcast(row):
    """Eksekusi satu jadwal broadcast ke semua user"""
    sched_id, admin_id, schedule_time, msg_type, file_id, text_content, caption = row

    users = await db_pool.execute_read("SELECT user_id FROM users")
    total = len(users)
    if total == 0:
        return

    logger.info(f"📅 Scheduled broadcast ID={sched_id} ({schedule_time}) → {total} users")
    sent = failed_blocked = failed_other = 0

    for row_user in users:
        user_id = row_user[0]
        try:
            await asyncio.wait_for(
                _send_one_scheduled(application.bot, user_id, msg_type, file_id, text_content, caption),
                timeout=10.0
            )
            sent += 1
            await asyncio.sleep(0.065)

        except asyncio.TimeoutError:
            failed_other += 1

        except RetryAfter as e:
            wait_sec = int(e.retry_after) + 2
            logger.warning(f"FloodWait {wait_sec}s dalam scheduled broadcast")
            await asyncio.sleep(wait_sec)
            try:
                await asyncio.wait_for(
                    _send_one_scheduled(application.bot, user_id, msg_type, file_id, text_content, caption),
                    timeout=10.0
                )
                sent += 1
            except Exception:
                failed_other += 1

        except (Forbidden, BadRequest) as e:
            err = str(e).lower()
            if any(x in err for x in ["blocked", "deactivated", "not found", "chat not found", "kicked"]):
                failed_blocked += 1
            else:
                failed_other += 1

        except Exception as e:
            failed_other += 1
            logger.debug(f"Scheduled broadcast error uid={user_id}: {e}")

    summary = (
        f"📅 <b>Broadcast Terjadwal Selesai</b>\n"
        f"{'━' * 28}\n"
        f"⏰ Jadwal: <b>{schedule_time}</b>\n"
        f"📩 Jenis: <b>{msg_type}</b>\n"
        f"👥 Total Target: <b>{total:,}</b>\n"
        f"✅ Terkirim: <b>{sent:,}</b>\n"
        f"🚫 Diblokir: <b>{failed_blocked:,}</b>\n"
        f"❌ Error: <b>{failed_other:,}</b>"
    )
    await notify_admin(application.bot, summary)
    logger.info(f"📅 Scheduled ID={sched_id} done — sent={sent} blocked={failed_blocked} error={failed_other}")

async def schedule_runner_task():
    """Background task: cek setiap 30 detik, jalankan jadwal yang waktunya tiba"""
    logger.info("📅 Schedule runner started")
    fired_today: set = set()  # simpan (sched_id, tanggal) agar tidak double-fire

    while True:
        try:
            await asyncio.sleep(30)

            now = datetime.now()
            current_time = now.strftime("%H:%M")
            today_str = now.strftime("%Y-%m-%d")

            schedules = await _get_all_schedules()
            for row in schedules:
                sched_id = row[0]
                sched_time = row[2]
                fire_key = (sched_id, today_str)

                if sched_time == current_time and fire_key not in fired_today:
                    fired_today.add(fire_key)
                    asyncio.create_task(_execute_scheduled_broadcast(row))
                    logger.info(f"📅 Fired schedule ID={sched_id} at {current_time}")

            # Bersihkan catatan kemarin agar tidak menumpuk
            fired_today = {(sid, d) for sid, d in fired_today if d == today_str}

        except asyncio.CancelledError:
            logger.info("📅 Schedule runner stopped")
            break
        except Exception as e:
            logger.error(f"Schedule runner error: {e}")
            await asyncio.sleep(10)

async def link_stats_command(update, context):
    """Handle /link_stats KODE — statistik akses per link"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Sertakan kode link.\n\n"
            "Contoh: <code>/link_stats qCQoSj7HcV</code>",
            parse_mode="HTML"
        )
        return

    code = context.args[0].strip()

    # Ambil semua data sekaligus secara paralel
    media_info, total_clicks_row, unique_users_row, first_access_row, last_access_row, recent_users = \
        await asyncio.gather(
            db_pool.execute_read(
                "SELECT type, caption, created_at FROM media WHERE code=? LIMIT 1",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT click FROM media WHERE code=?",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT COUNT(DISTINCT user_id) FROM link_access_log WHERE code=?",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT accessed_at FROM link_access_log WHERE code=? ORDER BY accessed_at ASC LIMIT 1",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT accessed_at FROM link_access_log WHERE code=? ORDER BY accessed_at DESC LIMIT 1",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT user_id, username, accessed_at FROM link_access_log "
                "WHERE code=? ORDER BY accessed_at DESC LIMIT 10",
                (code,)
            ),
            return_exceptions=True
        )

    if not media_info:
        await update.message.reply_text(
            f"❌ Kode <code>{code}</code> tidak ditemukan.",
            parse_mode="HTML"
        )
        return

    media_type  = media_info[0] if media_info else "?"
    caption     = (media_info[1] or "")[:40] or "-"
    created_at  = media_info[2] if media_info else "-"
    total_clicks = total_clicks_row[0] if total_clicks_row else 0
    unique_users = unique_users_row[0] if unique_users_row and not isinstance(unique_users_row, Exception) else 0
    first_access = first_access_row[0] if first_access_row and not isinstance(first_access_row, Exception) else "-"
    last_access  = last_access_row[0]  if last_access_row  and not isinstance(last_access_row,  Exception) else "-"

    lines = [
        f"🔗 <b>Link Stats</b>: <code>{code}</code>",
        f"{'━' * 28}",
        f"📩 Jenis: <b>{media_type}</b>",
        f"📝 Caption: {caption}",
        f"📅 Dibuat: {str(created_at)[:16]}",
        f"{'━' * 28}",
        f"👆 Total Klik: <b>{total_clicks:,}</b>",
        f"👥 User Unik: <b>{unique_users:,}</b>",
        f"🕐 Pertama Diakses: {str(first_access)[:16]}",
        f"🕐 Terakhir Diakses: {str(last_access)[:16]}",
    ]

    if recent_users and not isinstance(recent_users, Exception) and len(recent_users) > 0:
        lines.append(f"{'━' * 28}")
        lines.append(f"👤 <b>10 Akses Terakhir:</b>")
        for row in recent_users:
            r_uid, r_uname, r_time = row
            display = f"@{r_uname}" if r_uname and r_uname != "unknown" else f"id:{r_uid}"
            lines.append(f"  • {display}  <i>{str(r_time)[:16]}</i>")
    else:
        lines.append(f"{'━' * 28}")
        lines.append("ℹ️ Belum ada log akses tercatat.")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def top_links_command(update, context):
    """Handle /top_links — tampilkan 10 link dengan klik terbanyak"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    # Ambil top 10 berdasarkan klik + jumlah user unik per link secara paralel
    top_rows, total_media = await asyncio.gather(
        db_pool.execute_read(
            """
            SELECT m.code, m.type,
                   CAST(m.click AS INTEGER) AS click,
                   m.caption,
                   COUNT(DISTINCT l.user_id) AS unique_users
            FROM media m
            LEFT JOIN link_access_log l ON l.code = m.code
            WHERE m.ready = 1
            GROUP BY m.code
            ORDER BY CAST(m.click AS INTEGER) DESC
            LIMIT 10
            """
        ),
        get_total_media(),
        return_exceptions=True
    )

    if not top_rows or isinstance(top_rows, Exception) or len(top_rows) == 0:
        await update.message.reply_text("ℹ️ Belum ada data link.")
        return

    lines = [
        f"🏆 <b>Top 10 Link Terpopuler</b>",
        f"📊 Dari total <b>{total_media:,}</b> link",
        f"{'━' * 28}",
    ]

    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(top_rows):
        code, media_type, clicks, caption, unique_users = row
        medal = medals[i] if i < 3 else f"{i + 1}."
        cap_short = (caption or "")[:25].strip()
        cap_display = f" — {cap_short}" if cap_short else ""
        try:
            clicks_int = int(clicks or 0)
        except (ValueError, TypeError):
            clicks_int = 0
        try:
            unique_int = int(unique_users or 0)
        except (ValueError, TypeError):
            unique_int = 0
        lines.append(
            f"{medal} <code>{code}</code> <i>({media_type})</i>{cap_display}\n"
            f"    👆 {clicks_int:,} klik  👥 {unique_int:,} user unik"
        )

    lines.append(f"{'━' * 28}")
    lines.append("Detail: /link_stats [kode]")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def status_command(update, context):
    """Handle /status — real-time bot health for admins"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    now = datetime.now()

    # Uptime
    if bot_start_time:
        delta = now - bot_start_time
        days = delta.days
        hours, rem = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
    else:
        uptime_str = "Unknown"

    # DB checks in parallel
    total_users, total_media, db_size, today_users = await asyncio.gather(
        get_total_users(),
        get_total_media(),
        _get_db_size(),
        _get_today_users(),
        return_exceptions=True
    )

    polling_alive = (
        application and
        application.updater and
        application.updater.running
    )

    status_text = (
        "🖥️ <b>BOT STATUS</b>\n"
        f"{'━' * 28}\n"
        f"🤖 Bot: @{BOT_USERNAME}\n"
        f"🟢 Polling: {'Active' if polling_alive else '🔴 Stopped'}\n"
        f"⏱️ Uptime: <code>{uptime_str}</code>\n"
        f"🕐 Waktu: <code>{now.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"{'━' * 28}\n"
        f"👥 Total Users: <b>{total_users:,}</b>\n"
        f"📅 Aktif Hari Ini: <b>{today_users:,}</b>\n"
        f"📄 Total Media: <b>{total_media:,}</b>\n"
        f"💾 Database Size: <b>{db_size}</b>\n"
        f"{'━' * 28}\n"
        f"📢 Channel: {CHANNEL}\n"
    )

    await update.message.reply_text(status_text, parse_mode="HTML")

async def _get_db_size() -> str:
    """Get database file size as human-readable string"""
    try:
        size = os.path.getsize(DATABASE_PATH)
        if size < 1024:
            return f"{size} B"
        elif size < 1024 * 1024:
            return f"{size / 1024:.1f} KB"
        else:
            return f"{size / (1024 * 1024):.2f} MB"
    except Exception:
        return "N/A"

async def _get_today_users() -> int:
    """Get count of users who joined today"""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        result = await db_pool.execute_read(
            "SELECT COUNT(*) FROM users WHERE joined_at >= ?",
            (today,),
            fetch_one=True
        )
        return result[0] if result else 0
    except Exception:
        return 0

async def stats_command(update, context):
    """Handle /stats"""
    uid = update.effective_user.id

    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    total_users, total_media, total_clicks, last_backup = await asyncio.gather(
        get_total_users(),
        get_total_media(),
        get_total_clicks(),
        get_last_backup(),
        return_exceptions=True
    )

    stats_text = (
        "📊 BOT STATISTICS\n\n"
        f"👥 Total Users: {total_users:,}\n"
        f"📄 Total Media: {total_media:,}\n"
        f"🔗 Total Clicks: {total_clicks:,}\n\n"
    )

    if last_backup:
        backup_name, file_size, backup_time = last_backup
        stats_text += (
            f"📦 Last Backup:\n"
            f"  💾 {backup_name}\n"
            f"  📊 Size: {file_size / (1024*1024):.2f} MB\n"
            f"  ⏰ {backup_time}\n"
        )
    else:
        stats_text += "📦 No backups yet\n"

    await update.message.reply_text(stats_text)

async def error_handler(update, context):
    logger.error(f"❌ Error: {context.error}", exc_info=True)

# ===== ADMIN NOTIFIER =====

async def notify_admin(bot, message: str):
    """Send notification to all admin IDs"""
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Failed to notify admin {admin_id}: {e}")

# ===== BACKUP =====

async def send_backup_to_admin(application, backup_path, backup_name, file_size):
    try:
        with open(backup_path, "rb") as f:
            await application.bot.send_document(
                BACKUP_CHAT_ID,
                f,
                caption=f"📦 Database Backup\n⏰ {backup_name}\n💾 Size: {file_size / (1024*1024):.2f} MB"
            )
        logger.info("✅ Backup sent to admin")
    except Exception as e:
        logger.error(f"Error sending backup: {e}")

async def create_backup(application):
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_name = f"database_{ts}.db"
        backup_path = os.path.join(BACKUP_DIR, backup_name)

        if os.path.exists(DATABASE_PATH):
            shutil.copy2(DATABASE_PATH, backup_path)
            file_size = os.path.getsize(backup_path)

            logger.info(f"📦 Backup created: {backup_path} ({file_size/(1024*1024):.2f} MB)")

            try:
                await db_pool.execute_write(
                    "INSERT INTO backup_log (backup_name, file_size) VALUES (?, ?)",
                    (backup_name, file_size)
                )
            except Exception as e:
                logger.error(f"Error logging backup: {e}")

            await send_backup_to_admin(application, backup_path, backup_name, file_size)

            try:
                files = sorted(os.listdir(BACKUP_DIR))
                if len(files) > MAX_BACKUPS:
                    for old_file in files[:-MAX_BACKUPS]:
                        try:
                            os.remove(os.path.join(BACKUP_DIR, old_file))
                            logger.info(f"🗑️ Old backup deleted: {old_file}")
                        except:
                            pass
            except Exception as e:
                logger.error(f"Error cleaning backups: {e}")
    except Exception as e:
        logger.error(f"Backup error: {e}")

async def backup_task(application):
    logger.info("📦 Auto backup task started")
    # Kirim backup pertama setelah 5 menit (bukan 6 jam),
    # agar backup tidak hilang saat bot sering restart
    initial_delay = 300  # 5 menit
    logger.info(f"📦 Backup pertama dalam {initial_delay // 60} menit...")
    try:
        await asyncio.sleep(initial_delay)
        await create_backup(application)
    except asyncio.CancelledError:
        logger.info("📦 Backup task stopped (before first backup)")
        return
    except Exception as e:
        logger.error(f"Backup task error (initial): {e}")

    # Setelah itu, ulangi setiap BACKUP_INTERVAL
    while True:
        try:
            await asyncio.sleep(BACKUP_INTERVAL)
            await create_backup(application)
        except asyncio.CancelledError:
            logger.info("📦 Backup task stopped")
            break
        except Exception as e:
            logger.error(f"Backup task error: {e}")
            await asyncio.sleep(60)

async def backup_now_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    msg = await update.message.reply_text("⏳ Membuat backup database, harap tunggu...")
    try:
        await create_backup(context.application)
        await msg.edit_text("✅ Backup berhasil dikirim ke Telegram!")
    except Exception as e:
        logger.error(f"backup_now_command error: {e}")
        await msg.edit_text(f"❌ Gagal membuat backup: {e}")

async def keep_alive_task():
    """Ping URL publik Replit setiap 4 menit agar container tidak tidur"""
    import urllib.request
    import ssl

    # Gunakan URL publik Replit — localhost saja tidak cukup mencegah container tidur
    if REPLIT_DOMAIN:
        ping_url = f"https://{REPLIT_DOMAIN}/health"
    else:
        ping_url = f"http://127.0.0.1:{PORT}/health"

    logger.info(f"💓 Keep-alive task started → {ping_url}")
    await asyncio.sleep(60)  # tunggu server benar-benar ready dulu

    loop = asyncio.get_event_loop()
    ssl_ctx = ssl.create_default_context()

    while True:
        try:
            def _ping():
                req = urllib.request.Request(
                    ping_url,
                    headers={"User-Agent": "TelegramBot-KeepAlive/1.0"},
                )
                with urllib.request.urlopen(req, timeout=15, context=ssl_ctx if ping_url.startswith("https") else None) as resp:
                    return resp.status

            status = await asyncio.wait_for(
                loop.run_in_executor(None, _ping), timeout=20
            )
            logger.info(f"💓 Keep-alive ping OK (HTTP {status})")
        except asyncio.CancelledError:
            logger.info("💓 Keep-alive task stopped")
            break
        except Exception as e:
            logger.warning(f"💓 Keep-alive ping gagal: {e}")
            # Fallback ke localhost jika publik gagal
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection('127.0.0.1', PORT), timeout=10
                )
                writer.write(b'GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n')
                await writer.drain()
                await asyncio.wait_for(reader.read(128), timeout=10)
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                logger.info("💓 Keep-alive fallback (localhost) OK")
            except Exception:
                pass
        await asyncio.sleep(240)  # 4 menit

async def cache_cleanup_task():
    logger.info("🗑️ Cache cleanup task started")
    while True:
        try:
            await asyncio.sleep(60)
            await cleanup_expired_caches()
        except asyncio.CancelledError:
            logger.info("🗑️ Cache cleanup task stopped")
            break
        except Exception as e:
            logger.error(f"Cache cleanup error: {e}")

# ===== FASTAPI =====

application = None
background_tasks = []
bot_start_time: datetime = None

@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    """Lifespan context manager"""
    global application, db_pool, background_tasks, bot_start_time
    bot_start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("🤖 TELEGRAM BOT STARTING (POLLING MODE - OPTIMIZED)")
    logger.info("=" * 70)

    try:
        db_pool = DatabasePool(DATABASE_PATH, pool_size=5)
        await db_pool.init()

        await init_db()

        application = Application.builder().token(TOKEN).build()
        application.add_error_handler(error_handler)

        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("bc", broadcast_command))
        application.add_handler(CommandHandler("bc_cancel", bc_cancel_command))
        application.add_handler(CommandHandler("bc_schedule", bc_schedule_command))
        application.add_handler(CommandHandler("bc_schedules", bc_schedules_command))
        application.add_handler(CommandHandler("bc_unschedule", bc_unschedule_command))
        application.add_handler(CommandHandler("link_stats", link_stats_command))
        application.add_handler(CommandHandler("top_links", top_links_command))
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(CommandHandler("status", status_command))
        application.add_handler(CommandHandler("backup_now", backup_now_command))
        application.add_handler(MessageHandler(filters.PHOTO, upload_handler))
        application.add_handler(MessageHandler(filters.VIDEO, upload_handler))

        await application.initialize()

        logger.info("=" * 70)
        logger.info("🟢 BOT IS RUNNING (POLLING MODE - OPTIMIZED FOR 100-500 USERS)")
        logger.info("=" * 70)

        # Start PTB application once here (not inside polling_loop)
        await application.start()

        backup_task_obj = asyncio.create_task(backup_task(application))
        cleanup_task_obj = asyncio.create_task(cache_cleanup_task())
        polling_task_obj = asyncio.create_task(polling_loop(application))
        schedule_task_obj = asyncio.create_task(schedule_runner_task())
        keepalive_task_obj = asyncio.create_task(keep_alive_task())
        background_tasks = [backup_task_obj, cleanup_task_obj, polling_task_obj, schedule_task_obj, keepalive_task_obj]

        yield

    except Exception as e:
        logger.error(f"❌ Startup error: {e}", exc_info=True)
        raise
    finally:
        logger.info("🛑 Shutting down bot...")

        for task in background_tasks:
            try:
                task.cancel()
                await asyncio.wait_for(task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception:
                pass

        try:
            if application:
                if application.updater and application.updater.running:
                    await application.updater.stop()
                await application.stop()
                await application.shutdown()
        except Exception:
            pass

        try:
            if db_pool:
                await db_pool.close()
        except Exception:
            pass

        logger.info("✅ Bot shutdown complete")

async def polling_loop(application):
    """Polling supervisor — auto-restart with exponential backoff"""
    retry_delay = 5
    max_retry_delay = 120
    attempt = 0
    first_start = True

    logger.info("📡 Polling supervisor started")

    while True:
        try:
            attempt += 1
            logger.info(f"📡 Starting polling (attempt #{attempt})...")

            if not application.updater.running:
                await application.updater.start_polling(
                    allowed_updates=["message", "callback_query"],
                    poll_interval=1.0,
                    timeout=10.0,       # server-side long-poll max wait
                    read_timeout=20.0,  # HARUS > timeout agar tidak timeout duluan
                    write_timeout=15.0,
                    connect_timeout=15.0,
                    pool_timeout=15.0,
                )
                logger.info("✅ Polling started successfully")

                if first_start:
                    first_start = False
                    await notify_admin(
                        application.bot,
                        f"✅ <b>Bot Online</b>\n"
                        f"🤖 @{BOT_USERNAME}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                else:
                    await notify_admin(
                        application.bot,
                        f"🔄 <b>Bot Auto-Restart</b> (attempt #{attempt})\n"
                        f"🤖 @{BOT_USERNAME}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )

                retry_delay = 5  # reset backoff after success

            # Monitor: cek updater setiap 15 detik, ping Telegram setiap 5 menit
            health_tick = 0
            while True:
                await asyncio.sleep(15)
                health_tick += 1

                if not application.updater.running:
                    logger.warning("⚠️ Updater stopped unexpectedly — restarting...")
                    break

                # Setiap 20 × 15s = 5 menit: lakukan ping nyata ke Telegram
                if health_tick % 20 == 0:
                    try:
                        await asyncio.wait_for(application.bot.get_me(), timeout=10.0)
                        logger.info("💓 Health check OK")
                    except Exception as e:
                        logger.warning(f"⚠️ Health check gagal: {e} — memaksa restart polling...")
                        try:
                            await application.updater.stop()
                        except Exception:
                            pass
                        break

        except asyncio.CancelledError:
            logger.info("📡 Polling supervisor stopped (cancelled)")
            try:
                await application.updater.stop()
            except Exception:
                pass
            break
        except Exception as e:
            logger.error(f"❌ Polling error: {e} — restarting in {retry_delay}s...")
            try:
                await notify_admin(
                    application.bot,
                    f"⚠️ <b>Bot Error — Restarting</b>\n"
                    f"❌ <code>{str(e)[:200]}</code>\n"
                    f"🔄 Retry dalam {retry_delay}s (attempt #{attempt})\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            except Exception:
                pass
            try:
                await application.updater.stop()
            except Exception:
                pass
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)

fastapi_app = FastAPI(lifespan=lifespan)

@fastapi_app.get("/health")
async def health():
    try:
        total_users = await get_total_users()
        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "bot": BOT_USERNAME,
            "users": total_users,
            "mode": "polling_optimized",
            "concurrent_capacity": "100-500 users"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@fastapi_app.get("/")
async def root():
    return {
        "name": "Telegram Media Bot (Optimized)",
        "status": "running",
        "bot": BOT_USERNAME,
        "mode": "polling",
        "concurrent_capacity": "100-500 users",
        "version": "3.0-fixed"
    }

# ===== MAIN =====

def main():
    if not TOKEN:
        logger.error("❌ TOKEN not set in .env!")
        return

    logger.info(f"🚀 Bot Configuration (OPTIMIZED):")
    logger.info(f"   Bot: @{BOT_USERNAME}")
    logger.info(f"   Channel: {CHANNEL}")
    logger.info(f"   Mode: POLLING (Optimized for 100-500 users)")
    logger.info(f"   DB Pool Size: 5")
    logger.info(f"   Polling Interval: 1.0 second")

    config = Config(
        app=fastapi_app,
        host=HOST,
        port=PORT,
        log_level="warning",
        workers=1,
        timeout_keep_alive=120,
        timeout_notify=30,
        loop="asyncio",
    )

    server = Server(config)
    # Biarkan exception naik ke __main__ agar restart loop bekerja
    asyncio.run(server.serve())

if __name__ == "__main__":
    import time
    import signal

    _exit_requested = False

    def _handle_sigterm(signum, frame):
        global _exit_requested
        _exit_requested = True
        logger.info("🛑 SIGTERM diterima — bot akan berhenti bersih")

    signal.signal(signal.SIGTERM, _handle_sigterm)

    restart_delay = 10
    while not _exit_requested:
        try:
            main()
            # main() selesai tanpa exception — bisa terjadi saat server shutdown bersih
            if _exit_requested:
                logger.info("🛑 Bot berhenti karena SIGTERM")
                break
            # Jika bukan SIGTERM, restart otomatis
            logger.info(f"🔄 Bot keluar tak terduga, restart dalam {restart_delay}s...")
            time.sleep(restart_delay)
        except KeyboardInterrupt:
            logger.info("⚠️ Bot dihentikan oleh pengguna (Ctrl+C)")
            break
        except Exception as e:
            logger.error(f"❌ Fatal crash: {e}", exc_info=True)
            logger.info(f"🔄 Restart dalam {restart_delay}s...")
            time.sleep(restart_delay)
            restart_delay = min(restart_delay * 2, 120)  # backoff, max 2 menit