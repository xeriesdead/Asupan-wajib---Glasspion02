# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL = os.getenv("CHANNEL", "@Asupan_Wajib")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003512802994"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "Asupan_Wajib_Bot")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "8441460682").split(",")]
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))
WEBHOOK_URL = os.getenv("REPLIT_ORIGIN", "https://yourdomain.com")
WEBHOOK_PATH = "/webhook/telegram"
DATABASE_PATH = os.getenv("DATABASE_PATH", "database.db")
BACKUP_CHAT_ID = int(os.getenv("BACKUP_CHAT_ID", "8441460682"))
REPLIT_DOMAIN = os.getenv("REPLIT_DOMAINS", "").split(",")[0].strip()
AUTO_DELETE_TIMEOUT = int(os.getenv("AUTO_DELETE_TIMEOUT", "3600"))  # 1 jam
BATCH_TIMEOUT = 10  # Tunggu 10 detik sebelum finalize single media
BACKUP_INTERVAL = 21600  # 6 jam
BACKUP_DIR = "backups"
MAX_BACKUPS = 10

if not TOKEN:
    raise ValueError("❌ TELEGRAM_BOT_TOKEN not set in .env!")